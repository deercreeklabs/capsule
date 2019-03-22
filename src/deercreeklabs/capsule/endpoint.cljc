(ns deercreeklabs.capsule.endpoint
  (:require
   [clojure.core.async :as ca]
   [clojure.data :as data]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.capsule.logging :as logging
    :refer [debug debug-syms error info]]
   [deercreeklabs.capsule.utils :as u]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.tube.connection :as tc]
   [schema.core :as s]))

#?(:clj
   (primitive-math/use-primitive-operators))

(def default-endpoint-options
  {:default-rpc-timeout-ms 10000
   :silence-log? false})

(deftype ConnInfo [tube-conn subject-id client-schema])

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
                     *conn-count *fp->schema *rpc-id *rpc-id->rpc-info
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
      (info (str "Opened conn " conn-id " on " path " from " remote-addr
                 ". Endpoint conn count: " @*conn-count))
      (tc/set-on-rcv tube-conn (fn [conn data]
                                 (on-rcv this conn data)))
      (tc/set-on-disconnect tube-conn
                            (partial on-disconnect* this conn-id remote-addr))))

  (on-rcv [this tube-conn data]
    (try
      (let [conn-id (tc/get-conn-id tube-conn)
            peer-id (tc/get-remote-addr tube-conn)
            sender (fn [msg]
                     (tc/send tube-conn (l/serialize msgs-union-schema msg)))]
        (if-let [^ConnInfo conn-info (@*conn-id->conn-info conn-id)]
          (u/handle-rcv :endpoint conn-id sender (.subject-id conn-info)
                        peer-id data msgs-union-schema
                        (.client-schema conn-info)
                        *msg-rec-name->handler)
          (do-schema-negotiation* this conn-id tube-conn data)))
      (catch #?(:clj Exception :cljs js/Error) e
        (error (str "Error in on-rcv: " (logging/ex-msg-and-stacktrace e))))))

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
    (when-not (keyword? msg-name-kw)
      (throw (ex-info "msg-name-kw must be a keyword."
                      (u/sym-map msg-name-kw))))
    (u/set-handler msg-name-kw handler peer-name-maps *msg-rec-name->handler
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
    (let [req (l/deserialize-same u/handshake-req-schema data)
          actual-server-fp (l/fingerprint64 msgs-union-schema)
          server-pcf (l/pcf msgs-union-schema)
          {:keys [client-fp client-pcf server-fp]} req
          client-schema (if client-pcf
                          (let [client-schema (l/json->schema client-pcf)]
                            (swap! *fp->schema assoc client-fp client-schema)
                            client-schema)
                          (@*fp->schema client-fp))
          server-match? (#?(:clj = :cljs .equals)
                         actual-server-fp
                         server-fp)
          match (if-not client-schema
                  :match/none
                  (if server-match?
                    :match/both
                    :match/client))
          rsp (cond-> (u/sym-map match)
                (not server-match?)
                (assoc :server-fp actual-server-fp
                       :server-pcf server-pcf))]
      (tc/send tube-conn (l/serialize u/handshake-rsp-schema rsp))
      (when client-schema
        (let [conn-info (->ConnInfo tube-conn nil client-schema)]
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
              rsp (with-meta (u/sym-map was-successful)
                    {:short-name :login-rsp})]
          (if-not was-successful
            (do
              (sender rsp)
              (tc/close tube-conn))
            (let [new-conn-info (->ConnInfo tube-conn subject-id
                                            (.client-schema
                                             ^ConnInfo conn-info))]
              (swap! *conn-id->conn-info assoc conn-id new-conn-info)
              (swap! *subject-id->conn-ids update subject-id
                     (fn [old-conn-ids]
                       (if old-conn-ids
                         (conj old-conn-ids conn-id)
                         #{conn-id})))
              (sender rsp))))
        (catch #?(:clj Exception :cljs js/Error) e
          (error (str "Error in <handle-login-req*: "
                      (logging/ex-msg-and-stacktrace e)))))))

  (send-rpc* [this conn-id rpc-name-kw arg success-cb failure-cb timeout-ms]
    (let [rpc-id (u/get-rpc-id* *rpc-id)
          {:keys [msg-info rpc-info]} (u/rpc-msg-info
                                       rpc-name->req-name rpc-name-kw rpc-id
                                       timeout-ms arg success-cb failure-cb)
          ^ConnInfo conn-info (@*conn-id->conn-info conn-id)
          tube-conn (.tube-conn conn-info)]
      (swap! *rpc-id->rpc-info assoc rpc-id rpc-info)
      (tc/send tube-conn (l/serialize msgs-union-schema (:msg msg-info)))))

  (send-msg* [this conn-id msg-name-kw arg timeout-ms]
    (let [msg-rec-name (msg-name->rec-name msg-name-kw)
          msg (with-meta {:arg arg} {:short-name msg-rec-name})
          ^ConnInfo conn-info (@*conn-id->conn-info conn-id)
          tube-conn (.tube-conn conn-info)]
      (tc/send tube-conn (l/serialize msgs-union-schema msg))))

  (on-disconnect* [this conn-id remote-addr tube-conn code reason]
    (swap! *conn-count #(dec (int %)))
    (info (str "Closed conn " conn-id " on " path " from " remote-addr
               ". Endpoint conn count: "  @*conn-count))
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

(s/defn endpoint :- (s/protocol IEndpoint)
  ([path :- s/Str
    authenticator :- u/Authenticator
    protocol :- u/Protocol
    role :- u/Role]
   (endpoint path authenticator protocol role default-endpoint-options))
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
         msgs-union-schema (u/msgs-union-schema protocol)
         peer-role (u/get-peer-role protocol role)
         my-name-maps (u/name-maps protocol role)
         peer-name-maps (u/name-maps protocol peer-role)
         {:keys [rpc-name->req-name msg-name->rec-name]} my-name-maps
         *conn-id->conn-info (atom {})
         *subject-id->conn-ids (atom {})
         *conn-count (atom 0)
         *fp->schema (atom {})
         *rpc-id (atom 0)
         *rpc-id->rpc-info (atom {})
         *shutdown? (atom false)
         *msg-rec-name->handler (atom (u/msg-rec-name->handler
                                       my-name-maps peer-name-maps
                                       *rpc-id->rpc-info silence-log?))
         endpoint (->Endpoint
                   path authenticator rpc-name->req-name msg-name->rec-name
                   msgs-union-schema default-rpc-timeout-ms role peer-role
                   peer-name-maps *conn-id->conn-info *subject-id->conn-ids
                   *conn-count *fp->schema *rpc-id *rpc-id->rpc-info
                   *shutdown? *msg-rec-name->handler)]
     (swap! *msg-rec-name->handler assoc :login-req
            (partial <handle-login-req* endpoint))
     (doseq [[msg-name-kw handler] handlers]
       (set-handler endpoint msg-name-kw handler))
     (start-gc-loop* endpoint)
     endpoint)))
