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

(primitive-math/use-primitive-operators)

(def default-endpoint-options
  {:path ""
   :<authenticator (fn [endpoint subject-id credential]
                     (let [ch (ca/chan)]
                       (ca/put! ch {:was-successful false
                                    :reason "No authenticator"})))})

(deftype ConnInfo [tube-conn decoder subject-id roles])

(defn decode-handshake-req [data]
  (l/deserialize u/handshake-req-schema
                 (l/get-parsing-canonical-form u/handshake-req-schema) data))

(defprotocol IEndpoint
  (get-path [this])
  (get-conn-count [this])
  (get-subject-conn-count [this subject-id])
  (on-connect [this tube-conn])
  (on-rcv [this tube-conn data])
  (<handle-login-req [this conn-info msg])
  (<handle-logout-req [this conn-info msg])
  (<do-schema-negotiation [this tube-conn data])
  (get-decoder [this client-fp client-pcf])
  (send-event-to-all-conns [this event-name event])
  (send-event-to-conn [this conn-id event-name event])
  (send-event-to-subject-conns [this subject-id event-name event])
  (send-event* [this conn-info event-name event])
  (send-msg-by-schema [this tube-conn msg-schema msg])
  (send-msg-by-name [this tube-conn msg-rec-name msg])
  (close-conn [this conn-id])
  (close-subject-conns [this subject-id])
  (on-disconnect* [this conn-id remote-addr tube-conn code reason]))

(defrecord Endpoint [path <authenticator msg-union-schema
                     msg-record-name->handler event-name->event-msg-record-name
                     *conn-id->conn-info *subject-id->authenticated-conn-ids
                     *decoders *conn-count]
  IEndpoint
  (get-path [this]
    path)

  (get-conn-count [this]
    @*conn-count)

  (get-subject-conn-count [this subject-id]
    (count (@*subject-id->authenticated-conn-ids subject-id)))

  (on-connect [this tube-conn]
    (swap! *conn-count #(inc (int %)))
    (let [conn-id (tc/get-conn-id tube-conn)
          remote-addr (tc/get-remote-addr tube-conn)]
      (infof "Opened conn %s on %s from %s. Endpoint conn count: %s"
             conn-id path remote-addr @*conn-count)
      (tc/set-on-rcv tube-conn (fn [conn data]
                                 (on-rcv this conn data)))
      (tc/set-on-disconnect tube-conn
                            (partial on-disconnect* this conn-id remote-addr))))

  (on-rcv [this tube-conn data]
    (let [conn-id (tc/get-conn-id tube-conn)]
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
              (au/<? (<handler this conn-info msg))
              (catch Exception e
                (errorf "Error in handler for %s.%s" msg-name
                        (lu/get-exception-msg-and-stacktrace e))))))
        (<do-schema-negotiation this tube-conn data))))

  (<do-schema-negotiation [this tube-conn data]
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
            (let [conn-info (->ConnInfo tube-conn decoder nil nil)
                  conn-id (tc/get-conn-id tube-conn)]
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

  (<handle-login-req [this conn-info msg]
    (au/go
      (let [tube-conn (.tube-conn ^ConnInfo conn-info)
            conn-id (tc/get-conn-id tube-conn)
            {:keys [op-id arg]} msg
            {:keys [subject-id credential]} arg
            ret (au/<? (<authenticator this subject-id credential))
            {:keys [was-successful roles reason]} ret
            rsp (u/sym-map op-id was-successful reason)]
        (when roles
          (if-not (set? roles)
            (throw (ex-info "Authenticator did not return a set of roles."
                            (u/sym-map roles)))
            (let [decoder (.decoder ^ConnInfo conn-info)
                  new-conn-info (->ConnInfo tube-conn decoder subject-id roles)]
              (swap! *conn-id->conn-info assoc conn-id new-conn-info)
              (swap! *subject-id->authenticated-conn-ids update subject-id
                     (fn [old-conn-ids]
                       (if old-conn-ids
                         (conj old-conn-ids conn-id)
                         #{conn-id}))))))
        (send-msg-by-schema this tube-conn u/login-rsp-schema rsp))))

  (<handle-logout-req [this conn-info msg]
    (au/go
      (let [tube-conn (.tube-conn ^ConnInfo conn-info)
            conn-id (tc/get-conn-id tube-conn)
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
                            {:op-id (:op-id msg)
                             :was-successful true}))))

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
        (close-conn this conn-id))))

  (on-disconnect* [this conn-id remote-addr tube-conn code reason]
    (swap! *conn-count #(dec (int %)))
    (infof (str "Closed conn %s on %s from %s. Endpoint conn count: %s")
           conn-id path remote-addr @*conn-count)
    (when-let [^ConnInfo conn-info (@*conn-id->conn-info conn-id)]
      (swap! *conn-id->conn-info dissoc conn-id)
      (when-let [subject-id (.subject-id conn-info)]
        (swap! *subject-id->authenticated-conn-ids
               (fn [m]
                 (let [conn-ids (m subject-id)
                       new-conn-ids (disj conn-ids conn-id)]
                   (if (pos? (count new-conn-ids))
                     (assoc m subject-id new-conn-ids)
                     (dissoc m subject-id)))))))))

(defn make-event-name-map [api]
  (reduce (fn [acc event-name]
            (let [rec-name (u/make-msg-record-name :event event-name)]
              (assoc acc event-name rec-name)))
          {} (keys (:events api))))

(defn permitted? [subject-roles rpc-roles]
  (pos? (count (clojure.set/intersection subject-roles rpc-roles))))

(defn make-rpc-handler [rpc-name rsp-name rpc->roles <handler]
  (let [rpc-roles (rpc->roles rpc-name)]
    (fn [endpoint ^ConnInfo conn-info msg]
      (ca/go
        (let [{:keys [op-id arg]} msg
              tube-conn (.tube-conn conn-info)]
          (try
            (let [roles (or (.roles conn-info) #{:public})]
              (if (permitted? roles rpc-roles)
                (let [subject-id (.subject-id conn-info)
                      metadata (u/sym-map op-id roles subject-id)
                      handler-ch (<handler arg metadata)
                      _ (when-not (au/channel? handler-ch)
                          (throw
                           (ex-info (str "Handler for `" rpc-name
                                         "` did not return a channel.")
                                    (u/sym-map handler-ch rpc-name))))
                      ret (au/<? handler-ch)
                      rsp (u/sym-map op-id ret)]
                  (send-msg-by-name endpoint tube-conn rsp-name rsp))
                (let [rsp {:op-id op-id
                           :rpc-name (name rpc-name)
                           :rpc-arg "...elided..."
                           :failure-type :unauthorized
                           :error-str "Unauthorized RPC."}]
                  (send-msg-by-schema endpoint tube-conn
                                      u/rpc-failure-rsp-schema rsp))))
            (catch Exception e
              (let [arg-str (str arg)
                    rsp {:op-id op-id
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
         *decoders (atom {})
         *conn-count (atom 0)]
     ;; Change name of <authenticator? Handle sync or async automatically?
     (->Endpoint path <authenticator msg-union-schema
                 msg-record-name->handler event-name->event-msg-record-name
                 *conn-id->conn-info *subject-id->authenticated-conn-ids
                 *decoders *conn-count))))
