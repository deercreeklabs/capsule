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

#?(:clj
   (primitive-math/use-primitive-operators))

(def default-endpoint-options
  {:default-rpc-timeout-ms 10000
   :silence-log? false})

(deftype ConnInfo [tube-conn subject-id client-pcf])

(defprotocol IEndpoint
  (get-path [this])
  (get-conn-count [this])
  (get-subject-conn-count [this subject-id])
  (get-subject-conn-ids [this subject-id])
  (get-all-conn-ids [this subject-id])
  (on-connect [this tube-conn])
  (on-rcv [this tube-conn data])
  (<send-msg
    [this conn-id msg-name-kw arg]
    [this conn-id msg-name-kw arg timeout-ms])
  (send-msg
    [this conn-id msg-name-kw arg]
    [this conn-id msg-name-kw arg timeout-ms]
    [this conn-id msg-name-kw arg success-cb failure-cb]
    [this conn-id msg-name-kw arg success-cb failure-cb timeout-ms])
  (send-msg-to-subject-conns [this subject-id msg-name-kw arg])
  (send-msg-to-all-conns [this msg-name-kw arg])
  (set-handler [this msg-name-kw handler])
  (close-conn [this conn-id])
  (close-subject-conns [this subject-id])
  (shutdown [this]))

(defprotocol IEndpointInternals
  (do-schema-negotiation* [this conn-id tube-conn data])
  (<handle-login-req* [this msg metadata])
  (send-rpc* [this conn-id rpc-name-kw arg success-cb failure-cb timeout-ms])
  (send-msg* [this conn-id msg-name-kw msg timeout-ms])
  (start-gc-loop* [this])
  (on-disconnect* [this conn-id remote-addr tube-conn code reason]))

(defrecord Endpoint [path authenticator rpc-name->req-name msg-name->rec-name
                     msgs-union-schema default-rpc-timeout-ms role peer-role
                     peer-name-maps *conn-id->conn-info *subject-id->conn-ids
                     *conn-count *fp->pcf *rpc-id *rpc-id->rpc-info
                     *shutdown? *msg-rec-name->handler]

  IEndpoint
  (get-path [this]
    path)

  (get-conn-count [this]
    @*conn-count)

  (get-subject-conn-count [this subject-id]
    (count (get-subject-conn-ids this subject-id)))

  (get-subject-conn-ids [this subject-id]
    (@*subject-id->conn-ids subject-id))

  (get-all-conn-ids [this subject-id]
    (keys @*conn-id->conn-info))

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
    (try
      (let [conn-id (tc/get-conn-id tube-conn)
            peer-id (tc/get-remote-addr tube-conn)
            sender (fn [msg-rec-name msg]
                     (tc/send tube-conn (l/serialize msgs-union-schema
                                                     [msg-rec-name msg])))]
        (if-let [^ConnInfo conn-info (@*conn-id->conn-info conn-id)]
          (u/handle-rcv :endpoint conn-id sender (.subject-id conn-info)
                        peer-id data msgs-union-schema (.client-pcf conn-info)
                        *msg-rec-name->handler)
          (do-schema-negotiation* this conn-id tube-conn data)))
      (catch #?(:clj Exception :cljs js/Error) e
        (errorf "Error in on-rcv: %s"
                (lu/get-exception-msg-and-stacktrace e)))))

  (<send-msg [this conn-id msg-name-kw arg]
    (<send-msg this conn-id msg-name-kw arg default-rpc-timeout-ms))

  (<send-msg [this conn-id msg-name-kw arg timeout-ms]
    (let [ch (ca/chan)
          cb #(ca/put! ch %)]
      (send-msg this conn-id msg-name-kw arg cb cb timeout-ms)
      ch))

  (send-msg [this conn-id msg-name-kw arg]
    (send-msg this conn-id msg-name-kw arg default-rpc-timeout-ms))

  (send-msg [this conn-id msg-name-kw arg timeout-ms]
    (send-msg this conn-id msg-name-kw arg nil nil timeout-ms))

  (send-msg [this conn-id msg-name-kw arg success-cb failure-cb]
    (send-msg this conn-id msg-name-kw arg success-cb failure-cb
              default-rpc-timeout-ms))

  (send-msg [this conn-id msg-name-kw arg success-cb failure-cb timeout-ms]
    (when @*shutdown?
      (throw (ex-info "Endpoint is shut down" (u/sym-map path))))
    (cond
      (rpc-name->req-name msg-name-kw)
      (send-rpc* this conn-id msg-name-kw arg success-cb failure-cb timeout-ms)

      (msg-name->rec-name msg-name-kw)
      (send-msg* this conn-id msg-name-kw arg timeout-ms)

      :else
      (throw
       (ex-info (str "Role `" role "` is not a sender for msg `"
                     msg-name-kw "`.")
                (u/sym-map role msg-name-kw arg)))))

  (set-handler [this msg-name-kw handler]
    (u/set-handler* msg-name-kw handler peer-name-maps *msg-rec-name->handler
                    peer-role))

  (send-msg-to-subject-conns [this subject-id msg-name-kw msg]
    (doseq [conn-id (@*subject-id->conn-ids subject-id)]
      (send-msg this conn-id msg-name-kw msg)))

  (send-msg-to-all-conns [this msg-name-kw msg]
    (doseq [[conn-id conn-info] @*conn-id->conn-info]
      (send-msg this conn-id msg-name-kw msg)))

  (close-conn [this conn-id]
    (let [conn-info (@*conn-id->conn-info conn-id)
          tube-conn (.tube-conn ^ConnInfo conn-info)]
      (tc/close tube-conn)))

  (close-subject-conns [this subject-id]
    (let [conn-ids (@*subject-id->conn-ids subject-id)]
      (doseq [conn-id conn-ids]
        (close-conn this conn-id))))

  (shutdown [this]
    (reset! *shutdown? true))

  IEndpointInternals
  (do-schema-negotiation* [this conn-id tube-conn data]
    (let [req (l/deserialize
               u/handshake-req-schema
               (l/get-parsing-canonical-form u/handshake-req-schema)
               data)
          actual-server-fp (l/get-fingerprint64 msgs-union-schema)
          server-pcf (l/get-parsing-canonical-form msgs-union-schema)
          {:keys [client-fp client-pcf server-fp]} req
          client-pcf (if client-pcf
                       (do
                         (swap! *fp->pcf assoc client-fp client-pcf)
                         client-pcf)
                       (@*fp->pcf client-fp))
          server-match? (#?(:clj = :cljs .equals)
                         actual-server-fp
                         server-fp)
          match (if-not client-pcf
                  :none
                  (if server-match?
                    :both
                    :client))
          rsp (cond-> {:match match}
                (not server-match?) (assoc :server-fp actual-server-fp
                                           :server-pcf server-pcf))]
      (tc/send tube-conn (l/serialize u/handshake-rsp-schema rsp))
      (when client-pcf
        (let [conn-info (->ConnInfo tube-conn nil client-pcf)]
          (swap! *conn-id->conn-info assoc conn-id conn-info)))))

  (<handle-login-req* [this msg metadata]
    (ca/go
      (try
        (let [{:keys [subject-id credential]} msg
              {:keys [conn-id sender]} metadata
              conn-info (@*conn-id->conn-info conn-id)
              tube-conn (.tube-conn ^ConnInfo conn-info)
              auth-ret (authenticator subject-id credential metadata)
              was-successful (boolean (if-not (au/channel? auth-ret)
                                        auth-ret
                                        (au/<? auth-ret)))
              rsp (u/sym-map was-successful)]
          (if-not was-successful
            (do
              (sender ::u/login-rsp rsp)
              (tc/close tube-conn))
            (let [new-conn-info (->ConnInfo tube-conn subject-id
                                            (.client-pcf ^ConnInfo conn-info))]
              (swap! *conn-id->conn-info assoc conn-id new-conn-info)
              (swap! *subject-id->conn-ids update subject-id
                     (fn [old-conn-ids]
                       (if old-conn-ids
                         (conj old-conn-ids conn-id)
                         #{conn-id})))
              (sender ::u/login-rsp rsp))))
        (catch #?(:clj Exception :cljs js/Error) e
          (errorf "Error in <handle-login-req*: %s"
                  (lu/get-exception-msg-and-stacktrace e))))))

  (send-rpc* [this conn-id rpc-name-kw arg success-cb failure-cb timeout-ms]
    (let [msg-rec-name (rpc-name->req-name rpc-name-kw)
          rpc-id (u/get-rpc-id* *rpc-id)
          failure-time-ms  (+ (#?(:clj long :cljs identity)
                               (u/get-current-time-ms))
                              (int timeout-ms))
          rpc-info (u/sym-map rpc-name-kw arg rpc-id success-cb
                              failure-cb timeout-ms failure-time-ms)
          msg (u/sym-map rpc-id timeout-ms arg)
          msg-info (u/sym-map msg-rec-name msg failure-time-ms failure-cb)
          ^ConnInfo conn-info (@*conn-id->conn-info conn-id)
          tube-conn (.tube-conn conn-info)]
      (swap! *rpc-id->rpc-info assoc rpc-id rpc-info)
      (tc/send tube-conn (l/serialize msgs-union-schema [msg-rec-name msg]))))

  (send-msg* [this conn-id msg-name-kw msg timeout-ms]
    (let [msg-rec-name (msg-name->rec-name msg-name-kw)
          ^ConnInfo conn-info (@*conn-id->conn-info conn-id)
          tube-conn (.tube-conn conn-info)]
      (tc/send tube-conn (l/serialize msgs-union-schema
                                      [msg-rec-name {:arg msg}]))))

  (on-disconnect* [this conn-id remote-addr tube-conn code reason]
    (swap! *conn-count #(dec (int %)))
    (infof (str "Closed conn %s on %s from %s. Endpoint conn count: %s")
           conn-id path remote-addr @*conn-count)
    (when-let [^ConnInfo conn-info (@*conn-id->conn-info conn-id)]
      (swap! *conn-id->conn-info dissoc conn-id)
      (when-let [subject-id (.subject-id conn-info)]
        (swap! *subject-id->conn-ids
               (fn [m]
                 (let [conn-ids (m subject-id)
                       new-conn-ids (disj conn-ids conn-id)]
                   (if (pos? (count new-conn-ids))
                     (assoc m subject-id new-conn-ids)
                     (dissoc m subject-id))))))))

  (start-gc-loop* [this]
    (let []
      (u/start-gc-loop *shutdown? *rpc-id->rpc-info))))

(s/defn make-endpoint :- (s/protocol IEndpoint)
  ([path :- s/Str
    authenticator :- u/Authenticator
    protocol :- u/Protocol
    role :- u/Role]
   (make-endpoint path authenticator protocol role default-endpoint-options))
  ([path :- s/Str
    authenticator :- u/Authenticator
    protocol :- u/Protocol
    role :- u/Role
    options :- u/EndpointOptions]
   (when-not (string? path)
     (throw (ex-info "`path` parameter must be a string." (u/sym-map path))))
   (when-not (ifn? authenticator)
     (throw (ex-info "`authenticator` parameter must be a function."
                     (u/sym-map authenticator))))
   (u/check-protocol protocol)
   (when-not (keyword? role)
     (throw (ex-info "`role` parameter must be a keyword." (u/sym-map role))))
   (when-not (map? options)
     (throw (ex-info "`options` parameter must be a map."
                     (u/sym-map options))))
   (let [{:keys [default-rpc-timeout-ms
                 silence-log? handlers]} options
         msgs-union-schema (u/make-msgs-union-schema protocol)
         peer-role (u/get-peer-role protocol role)
         my-name-maps (u/make-name-maps protocol role)
         peer-name-maps (u/make-name-maps protocol peer-role)
         {:keys [rpc-name->req-name msg-name->rec-name]} my-name-maps
         *conn-id->conn-info (atom {})
         *subject-id->conn-ids (atom {})
         *conn-count (atom 0)
         *fp->pcf (atom {})
         *rpc-id (atom 0)
         *rpc-id->rpc-info (atom {})
         *shutdown? (atom false)
         *msg-rec-name->handler (atom (u/make-msg-rec-name->handler
                                       my-name-maps peer-name-maps
                                       *rpc-id->rpc-info silence-log?))
         endpoint (->Endpoint
                   path authenticator rpc-name->req-name msg-name->rec-name
                   msgs-union-schema default-rpc-timeout-ms role peer-role
                   peer-name-maps *conn-id->conn-info *subject-id->conn-ids
                   *conn-count *fp->pcf *rpc-id *rpc-id->rpc-info
                   *shutdown? *msg-rec-name->handler)]
     (swap! *msg-rec-name->handler assoc ::u/login-req
            (partial <handle-login-req* endpoint))
     (doseq [[msg-name-kw handler] handlers]
       (set-handler endpoint msg-name-kw handler))
     (start-gc-loop* endpoint)
     endpoint)))
