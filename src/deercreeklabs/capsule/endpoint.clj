(ns deercreeklabs.capsule.endpoint
  (:require
   [clojure.core.async :as ca]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.capsule.utils :as u]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   [deercreeklabs.tube.connection :as tc]
   [schema.core :as s]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]]))

(deftype ConnInfo [tube-conn decoder subject-id roles])

(defn decode-handshake-req [data]
  (l/deserialize u/handshake-req-schema
                 (l/get-parsing-canonical-form u/handshake-req-schema) data))

(defprotocol IEndpoint
  (get-path [this])
  (on-connect [this tube-conn conn-id])
  (on-rcv [this conn-id tube-conn data])
  (<handle-login-req [this conn-id conn-info msg])
  (<handle-logout-req [this conn-id conn-info msg])
  (<do-schema-negotiation [this conn-id tube-conn data])
  (get-decoder [this client-fp client-pcf])
  (send-event-to-all-conns [this event-name event])
  (send-event-to-conn [this conn-id event-name event])
  (send-event-to-subject-conns [this subject-id event-name event])
  (send-event* [this conn-info event-name event])
  (send-msg-by-schema [this tube-conn msg-schema msg])
  (send-msg-by-name [this tube-conn msg-rec-name msg])
  (close-conn [this conn-id])
  (close-subject-conns [this subject-id]))

(defrecord Endpoint [path <authenticator msg-union-schema
                     msg-record-name->handler event-name->event-msg-record-name
                     *conn-id->conn-info *subject-id->authenticated-conn-ids
                     *decoders]
  IEndpoint
  (get-path [this]
    path)

  (on-connect [this tube-conn conn-id]
    (tc/set-on-rcv tube-conn (partial on-rcv this conn-id))
    (tc/set-on-close tube-conn
                     (fn [tube-conn code reason]
                       (when-let [^ConnInfo conn-info (@*conn-id->conn-info
                                                       conn-id)]
                         (swap! *conn-id->conn-info dissoc conn-id)
                         (when-let [subject-id (.subject-id conn-info)]
                           (swap! *subject-id->authenticated-conn-ids
                                  dissoc subject-id))))))

  (on-rcv [this conn-id tube-conn data]
    (if-let [^ConnInfo conn-info (@*conn-id->conn-info conn-id)]
      (ca/go
        (let [decoder (.decoder conn-info)
              [msg-name msg] (decoder data)
              <handler (msg-record-name->handler msg-name)]
          (when (not <handler)
            (let [data (ba/byte-array->debug-str data)]
              (throw (ex-info (str "No handler is defined for " msg-name)
                              (u/sym-map msg-name msg <handler data)))))
          (try
            (au/<? (<handler this conn-id conn-info msg))
            (catch Exception e
              (errorf "Error in handler for %s.%s" msg-name
                      (lu/get-exception-msg-and-stacktrace e))))))
      (<do-schema-negotiation this conn-id tube-conn data)))

  (<do-schema-negotiation [this conn-id tube-conn data]
    (ca/go
      (try
        (let [req (decode-handshake-req data)
              actual-server-fp (u/long->byte-array
                                (l/get-fingerprint64 msg-union-schema))
              server-pcf (l/get-parsing-canonical-form msg-union-schema)
              {:keys [client-fp client-pcf server-fp]} req
              decoder (get-decoder this client-fp client-pcf)
              server-match? (ba/equivalent-byte-arrays? actual-server-fp
                                                        server-fp)
              match (if decoder
                      (if server-match?
                        :both
                        :client)
                      :none)
              rsp (cond-> {:match match}
                    (not server-match?) (assoc :server-fp actual-server-fp
                                               :server-pcf server-pcf))]
          (tc/send tube-conn (l/serialize u/handshake-rsp-schema rsp))
          (when decoder
            (let [conn-info (->ConnInfo tube-conn decoder nil nil)]
              (swap! *conn-id->conn-info assoc conn-id conn-info))))
        (catch Exception e
          (lu/log-exception e)))))

  (get-decoder [this client-fp client-pcf]
    (let [k (ba/byte-array->b64 client-fp)]
      (or (@*decoders k)
          (when client-pcf
            (let [decoder #(l/deserialize msg-union-schema client-pcf %)]
              (swap! *decoders assoc k decoder)
              decoder)))))

  (<handle-login-req [this conn-id conn-info msg]
    (au/go
      (let [tube-conn (.tube-conn ^ConnInfo conn-info)
            {:keys [subject-id credential]} msg
            roles (au/<? (<authenticator subject-id credential))
            rsp {:was-successful (boolean roles)}]
        (when roles
          (if (set? roles)
            (let [decoder (.decoder ^ConnInfo conn-info)
                  new-conn-info (->ConnInfo tube-conn decoder subject-id roles)]
              (swap! *conn-id->conn-info assoc conn-id new-conn-info)
              (swap! *subject-id->authenticated-conn-ids update subject-id
                     (fn [old-conn-ids]
                       (if old-conn-ids
                         (conj old-conn-ids conn-id)
                         #{conn-id}))))
            (throw (ex-info "Authenticator did not return a set of roles."
                            (u/sym-map roles)))))
        (send-msg-by-schema this tube-conn u/login-rsp-schema rsp))))

  (<handle-logout-req [this conn-id conn-info msg]
    (au/go
      (let [tube-conn (.tube-conn ^ConnInfo conn-info)
            decoder (.decoder ^ConnInfo conn-info)
            subject-id (.subject-id ^ConnInfo conn-info)
            new-conn-info (->ConnInfo tube-conn decoder nil nil)]
        (swap! *conn-id->conn-info assoc conn-id new-conn-info)
        (swap! *subject-id->authenticated-conn-ids update subject-id
               (fn [old-conn-ids]
                 (disj old-conn-ids conn-id)))
        (when (zero? (count (@*subject-id->authenticated-conn-ids subject-id)))
          (swap! *subject-id->authenticated-conn-ids dissoc subject-id))
        (send-msg-by-schema this tube-conn u/logout-rsp-schema
                            {:was-successful true}))))

  (send-event-to-conn [this conn-id event-name event]
    (let [conn-info (@*conn-id->conn-info conn-id)]
      (send-event* this conn-info event-name event)))

  (send-event-to-all-conns [this event-name event]
    (doseq [[conn-id conn-info] @*conn-id->conn-info]
      (send-event* this conn-info event-name event)))

  (send-event-to-subject-conns [this subject-id event-name event]
    (let [conn-ids (@*subject-id->authenticated-conn-ids subject-id)]
      (doseq [conn-id conn-ids]
        (send-event-to-conn this conn-id event-name event))))

  (send-event* [this conn-info event-name event]
    (let [tube-conn (.tube-conn ^ConnInfo conn-info)
          msg-rec-name (event-name->event-msg-record-name event-name)]
      (if msg-rec-name
        (send-msg-by-name this tube-conn msg-rec-name (u/sym-map event))
        (throw (ex-info (str "Event " event-name " is not in the API.")
                        (u/sym-map event-name event))))))

  (send-msg-by-schema [this tube-conn msg-schema msg]
    (tc/send tube-conn (l/serialize msg-union-schema (l/wrap msg-schema msg))))

  (send-msg-by-name [this tube-conn msg-rec-name msg]
    (tc/send tube-conn (l/serialize msg-union-schema [msg-rec-name msg])))

  (close-conn [this conn-id]
    (let [conn-info (@*conn-id->conn-info conn-id)
          tube-conn (.tube-conn ^ConnInfo conn-info)]
      (tc/close tube-conn)))

  (close-subject-conns [this subject-id]
    (let [conn-ids (@*subject-id->authenticated-conn-ids subject-id)]
      (doseq [conn-id conn-ids]
        (close-conn this conn-id)))))

(def default-endpoint-options
  {:path ""
   :<authenticator (fn [subject-id credential]
                     (au/go
                       false))})

(defn make-event-name-map [api]
  (reduce (fn [acc event-name]
            (let [rec-name (u/make-msg-record-name :event event-name)]
              (assoc acc event-name rec-name)))
          {} (keys (:events api))))

(defn permitted? [subject-roles rpc-roles]
  (pos? (count (clojure.set/intersection subject-roles rpc-roles))))

(defn make-rpc-handler [rpc-name rsp-name rpc->roles <handler]
  (let [rpc-roles (rpc->roles rpc-name)]
    (fn [endpoint conn-id ^ConnInfo conn-info msg]
      (ca/go
        (let [{:keys [rpc-id arg]} msg
              tube-conn (.tube-conn conn-info)]
          (try
            (let [roles (or (.roles conn-info) #{:public})]
              (if (permitted? roles rpc-roles)
                (let [subject-id (.subject-id conn-info)
                      metadata (u/sym-map rpc-id roles subject-id)
                      ret (au/<? (<handler arg metadata))
                      rsp (u/sym-map rpc-id ret)]
                  (send-msg-by-name endpoint tube-conn rsp-name rsp))
                (let [rsp {:rpc-id rpc-id
                           :rpc-name (name rpc-name)
                           :rpc-arg "...elided..."
                           :failure-type :unauthorized
                           :error-str "Unauthorized RPC."}]
                  (send-msg-by-schema endpoint tube-conn
                                      u/rpc-failure-rsp-schema rsp))))
            (catch Exception e
              (let [arg-str (str arg)
                    rsp {:rpc-id rpc-id
                         :rpc-name (name rpc-name)
                         :rpc-arg (subs arg-str 0 (min (count arg-str) 500))
                         :failure-type :server-exception
                         :error-str (lu/get-exception-msg e)}]
                (lu/log-exception e)
                (send-msg-by-schema endpoint tube-conn
                                    u/rpc-failure-rsp-schema rsp)))))))))

(defn make-msg-record-name->handler [roles-to-rpcs handlers]
  (let [base {::u/login-req <handle-login-req
              ::u/logout-req <handle-logout-req}]
    (reduce-kv (fn [acc rpc-name outer-handler]
                 (let [req-name (u/make-msg-record-name :rpc-req rpc-name)
                       rsp-name (u/make-msg-record-name
                                 :rpc-success-rsp rpc-name)
                       handler (make-rpc-handler rpc-name rsp-name roles-to-rpcs
                                                 outer-handler)]
                   (assoc acc req-name handler)))
               base (:rpcs handlers))))

(defn make-rpc->roles [roles->rpcs]
  (reduce-kv (fn [acc role rpcs]
               (reduce (fn [acc* rpc]
                         (update acc* rpc (fn [roles*]
                                            (if roles*
                                              (conj roles* role)
                                              #{role}))))
                       acc rpcs))
             {} roles->rpcs))

(s/defn make-endpoint :- (s/protocol IEndpoint)
  ([api :- u/API
    roles->rpcs :- u/RolesToRpcs
    handlers :- u/HandlerMap]
   (make-endpoint api handlers default-endpoint-options))
  ([api :- u/API
    roles->rpcs :- u/RolesToRpcs
    handlers :- u/HandlerMap
    opts :- u/EndpointOptions]
   (let [opts (merge default-endpoint-options opts)
         {:keys [path <authenticator]} opts
         msg-union-schema (u/make-msg-union-schema api)
         event-name->event-msg-record-name (make-event-name-map api)
         rpc->roles (make-rpc->roles roles->rpcs)
         msg-record-name->handler (make-msg-record-name->handler rpc->roles
                                                                 handlers)
         *conn-id->conn-info (atom {})
         *subject-id->authenticated-conn-ids (atom {})
         ;; TODO: Pre-seed *decoders with server-pcf
         *decoders (atom {})]
     ;; Change name of <authenticator? Handle sync or async automatically?
     (->Endpoint path <authenticator msg-union-schema
                 msg-record-name->handler event-name->event-msg-record-name
                 *conn-id->conn-info *subject-id->authenticated-conn-ids
                 *decoders))))
