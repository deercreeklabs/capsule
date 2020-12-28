(ns deercreeklabs.capsule.client
  (:require
   [clojure.core.async :as ca]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.capsule.logging :as log]
   [deercreeklabs.capsule.utils :as u]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.tube.client :as tc]
   [deercreeklabs.tube.connection :as connection]
   [schema.core :as s]))

(def conn-wait-ms-multiplier 1.5)
(def initial-conn-wait-ms 1000)
(def max-conn-wait-ms 30000)

(def default-client-options
  {:default-rpc-timeout-ms 15000
   :get-credentials-timeout-ms 15000
   :get-url-timeout-ms 15000
   :rcv-queue-size 1000
   :send-queue-size 1000
   :silence-log? false
   :on-connect (fn [capsule-client]
                 (log/info "Client connected.")
                 nil)
   :on-disconnect (fn [capsule-client]
                    (log/info "Client disconnected.")
                    nil)})

(defprotocol ICapsuleClient
  (<send-msg
    [this msg-name-kw arg]
    [this msg-name-kw arg timeout-ms])
  (send-msg
    [this msg-name-kw arg]
    [this msg-name-kw arg timeout-ms]
    [this msg-name-kw arg success-cb failure-cb]
    [this msg-name-kw arg success-cb failure-cb timeout-ms])
  (set-handler [this msg-name-kw handler])
  (shutdown [this]))

(defprotocol ICapsuleClientInternals
  (send-rpc* [this msg-name-kw arg success-cb failure-cb timeout-ms])
  (send-msg* [this msg-name-kw msg failure-cb timeout-ms])
  (<do-login* [this tube-client rcv-chan credentials])
  (<do-auth-w-creds* [this tube-client rcv-chan credentials])
  (<get-credentials* [this])
  (<get-credentials-and-do-auth* [this tube-client rcv-chan])
  (<do-schema-negotiation* [this tube-client rcv-chan url])
  (<do-auth* [this tube-client rcv-chan])
  (<get-url* [this])
  (<connect* [this <ws-client])
  (start-connect-loop* [this <ws-client])
  (start-gc-loop* [this])
  (start-send-loop* [this])
  (start-rcv-loop* [this]))

(defrecord CapsuleClient
    [get-url get-url-timeout-ms get-credentials
     get-credentials-timeout-ms *rcv-chan send-chan reconnect-chan
     msgs-union-schema client-fp client-pcf default-rpc-timeout-ms
     rcv-queue-size send-queue-size silence-log? on-connect
     on-disconnect on-login-result role msgs rpcs peer-role
     peer-msgs peer-rpcs
     *url->server-fp *server-schema *rpc-id *tube-client
     *credentials *shutdown? *rpc-id->rpc-info
     *msg-rec-name->handler]

  ICapsuleClient
  (<send-msg [this msg-name-kw arg]
    (<send-msg this msg-name-kw arg default-rpc-timeout-ms))

  (<send-msg [this msg-name-kw arg timeout-ms]
    (let [ch (ca/chan)
          cb (fn [arg]
               (if (nil? arg)
                 (ca/close! ch)
                 (ca/put! ch arg)))]
      (send-msg this msg-name-kw arg cb cb timeout-ms)
      ch))

  (send-msg [this msg-name-kw arg]
    (send-msg this msg-name-kw arg default-rpc-timeout-ms))

  (send-msg [this msg-name-kw arg timeout-ms]
    (send-msg this msg-name-kw arg nil nil timeout-ms))

  (send-msg [this msg-name-kw arg success-cb failure-cb]
    (send-msg this msg-name-kw arg success-cb failure-cb
              default-rpc-timeout-ms))

  (send-msg [this msg-name-kw arg success-cb failure-cb timeout-ms]
    (when @*shutdown?
      (throw (ex-info "Client is shut down" {})))
    (cond
      (rpcs msg-name-kw)
      (send-rpc* this msg-name-kw arg success-cb failure-cb timeout-ms)

      (msgs msg-name-kw)
      (do
        (send-msg* this msg-name-kw arg failure-cb timeout-ms)
        (when success-cb
          (success-cb nil)))

      :else
      (throw
       (ex-info (str "Role `" role "` is not a sender for msg `"
                     msg-name-kw "`.")
                (u/sym-map role msg-name-kw arg)))))

  (set-handler [this msg-name-kw handler]
    (u/set-handler msg-name-kw handler *msg-rec-name->handler
                   peer-msgs peer-rpcs peer-role))

  (shutdown [this]
    (reset! *shutdown? true)
    (when-let [tube-client @*tube-client]
      (tc/close tube-client)
      (reset! *tube-client nil)))

  ICapsuleClientInternals
  (send-rpc* [this rpc-name-kw arg success-cb failure-cb timeout-ms]
    (let [rpc-id (u/get-rpc-id* *rpc-id)
          {:keys [msg-info rpc-info]} (u/rpc-msg-info rpc-name-kw rpc-id
                                                      timeout-ms arg
                                                      success-cb failure-cb)]
      (if (ca/offer! send-chan msg-info)
        (swap! *rpc-id->rpc-info assoc rpc-id rpc-info)
        (when failure-cb
          (failure-cb (ex-info "RPC cannot be sent. Send queue is full."
                               {:rpc-info msg-info})))))
    nil)

  (send-msg* [this msg-name-kw arg failure-cb timeout-ms]
    (let [msg-rec-name (u/msg-record-name :msg msg-name-kw)
          msg {msg-rec-name {:arg arg}}
          failure-time-ms (+ (u/get-current-time-ms) timeout-ms)
          msg-info (u/sym-map msg failure-time-ms failure-cb)]
      (ca/offer! send-chan msg-info))
    nil)

  (<do-login* [this tube-client rcv-chan credentials]
    (au/go
      (tc/send tube-client (l/serialize msgs-union-schema
                                        {:login-req credentials}))
      (let [data (au/<? rcv-chan)
            msg (l/deserialize msgs-union-schema @*server-schema data)]
        (case msg
          {:login-rsp {:was-successful true}}
          (do
            (when-not silence-log? (log/info "Capsule login succeeded."))
            (when on-login-result
              (on-login-result true))
            true)

          {:login-rsp {:was-successful false}}
          (do
            (when-not silence-log? (log/info "Capsule login failed."))
            (when on-login-result
              (on-login-result false))
            false)

          (log/error (str "Got wrong login rsp msg: " msg))))))

  (<do-auth-w-creds* [this tube-client rcv-chan credentials]
    (au/go
      (let [timeout-ch (ca/timeout default-rpc-timeout-ms)
            login-ch (<do-login* this tube-client rcv-chan credentials)
            [success? ch] (au/alts? [timeout-ch login-ch])]
        (if (= timeout-ch ch)
          (do
            (log/error "Authentication timed out.")
            false)
          (if-not success?
            (do
              (reset! *credentials nil)
              false)
            (do
              (reset! *credentials credentials)
              true))))))

  (<get-credentials* [this]
    (ca/go
      (try
        (let [creds-ret (get-credentials)]
          (if-not (au/channel? creds-ret)
            creds-ret
            (let [timeout-ch (ca/timeout get-credentials-timeout-ms)
                  [credentials ch] (au/alts? [creds-ret timeout-ch])]
              (if (= timeout-ch ch)
                false
                credentials))))
        (catch #?(:clj Exception :cljs js/Error) e
          (log/error (str "Error in <get-credentials: "
                          (log/ex-msg-and-stacktrace e)))
          false))))

  (<get-credentials-and-do-auth* [this tube-client rcv-chan]
    (au/go
      (if-let [credentials (au/<? (<get-credentials* this))]
        (do
          (when (not (map? credentials))
            (throw (ex-info (str "get-credentials did not return a map. Got "
                                 credentials)
                            (u/sym-map credentials))))
          (when (not (:subject-id credentials))
            (throw (ex-info (str "get-credentials returned a map without a "
                                 "valid :subject-id key. Got: " credentials)
                            (u/sym-map credentials))))
          (when (not (:subject-secret credentials))
            (throw (ex-info (str "get-credentials returned a map without a "
                                 "valid :subject-secret key. Got: " credentials)
                            (u/sym-map credentials))))
          (if (au/<? (<do-auth-w-creds* this tube-client rcv-chan
                                        credentials))
            true
            (do
              (log/error "Authentication failed.")
              false)))
        (do
          (log/error "<get-credentials* failed.")
          false))))

  (<do-auth* [this tube-client rcv-chan]
    (au/go
      (if-not @*credentials
        (au/<? (<get-credentials-and-do-auth* this tube-client rcv-chan))
        (or (au/<? (<do-auth-w-creds* this tube-client rcv-chan @*credentials))
            (au/<? (<get-credentials-and-do-auth* this tube-client
                                                  rcv-chan))))))

  (<get-url* [this]
    (ca/go
      (try
        (let [url-ret (get-url)]
          (if-not (au/channel? url-ret)
            url-ret
            (let [timeout-ch (ca/timeout get-url-timeout-ms)
                  [url ch] (au/alts? [url-ret timeout-ch])]
              (if (= timeout-ch ch)
                false
                url))))
        (catch #?(:clj Exception :cljs js/Error) e
          (log/error (str "Error in <get-url: "
                          (log/ex-msg-and-stacktrace e)))
          false))))

  (<connect* [this <ws-client]
    (au/go
      (loop [wait-ms initial-conn-wait-ms]
        (when-not @*shutdown?
          (let [rand-mult (+ 0.5 (rand))
                new-wait-ms (-> (* wait-ms conn-wait-ms-multiplier)
                                (min max-conn-wait-ms)
                                (* rand-mult)
                                (Math/floor)
                                (int))]
            (let [url (ca/<! (<get-url* this))]
              (if-not url
                (do
                  (ca/<! (ca/timeout wait-ms))
                  (recur new-wait-ms))
                (if-not (string? url)
                  (do
                    (log/error
                     (str "<get-url* did not return a string, returned: " url))
                    (ca/<! (ca/timeout wait-ms))
                    (recur new-wait-ms))
                  (let [_ (when-not silence-log?
                            (log/info
                             (str "Got url: " url
                                  ". Attempting websocket connection.")))
                        rcv-chan (ca/chan rcv-queue-size)
                        logger (fn [level msg]
                                 (log/log* {:level level
                                            :ms (u/get-current-time-ms)
                                            :msg (str "TUBE: " msg)}))
                        opts (cond-> {:connect-timeout-ms wait-ms
                                      :logger logger
                                      :on-disconnect
                                      (fn [conn code reason]
                                        (on-disconnect this)
                                        (when-not silence-log?
                                          (log/info
                                           (str "Connection to "
                                                (connection/get-uri conn)
                                                " disconnected: " reason
                                                "(" code ").")))
                                        (when-not @*shutdown?
                                          (ca/put! reconnect-chan true)))
                                      :on-rcv (fn on-rcv [conn data]
                                                (ca/put! rcv-chan data))}
                               <ws-client (assoc :<ws-client
                                                 <ws-client))
                        tube-client (au/<? (tc/<tube-client url opts))]
                    (if-not tube-client
                      (when-not @*shutdown?
                        (ca/<! (ca/timeout wait-ms))
                        (recur new-wait-ms))
                      (if @*shutdown?
                        (do
                          (tc/close tube-client)
                          false)
                        (do
                          (au/<? (<do-schema-negotiation* this tube-client
                                                          rcv-chan url))
                          (if-not (au/<? (<do-auth* this tube-client rcv-chan))
                            (do
                              (tc/close tube-client)
                              (shutdown this)
                              false)
                            (do
                              (reset! *rcv-chan rcv-chan)
                              (reset! *tube-client tube-client)
                              true))))))))))))))

  (start-connect-loop* [this <ws-client]
    (ca/go
      (try
        (when (au/<? (<connect* this <ws-client))
          (on-connect this))
        (while (not @*shutdown?)
          (let [[reconnect? ch] (ca/alts! [reconnect-chan
                                           (ca/timeout initial-conn-wait-ms)])]
            (when (and (= reconnect-chan ch) reconnect?)
              (let [success? (au/<? (<connect* this <ws-client))]
                (if success?
                  (on-connect this)
                  (when-not @*shutdown?
                    (log/error "Client failed to reconnect. Shutting down.")
                    (shutdown this)))))))
        (catch #?(:clj Exception :cljs js/Error) e
          (log/error (str "Unexpected error in connect loop: "
                          (log/ex-msg-and-stacktrace e)))
          (shutdown this)))))

  (<do-schema-negotiation* [this tube-client rcv-chan url]
    (ca/go
      (try
        (loop [retry? false]
          (if @*shutdown?
            (tc/close tube-client)
            (let [known-server-fp (@*url->server-fp url)
                  req (cond-> {:client-fp client-fp
                               :server-fp (or known-server-fp
                                              client-fp)}
                        retry? (assoc :client-pcf client-pcf))
                  _ (tc/send tube-client (l/serialize
                                          u/handshake-req-schema req))
                  rsp (l/deserialize u/handshake-rsp-schema
                                     u/handshake-rsp-schema
                                     (au/<? rcv-chan))
                  {:keys [match server-fp server-pcf]} rsp]
              (case match
                :both
                (do
                  (swap! *url->server-fp assoc url known-server-fp)
                  (when-not known-server-fp
                    (reset! *server-schema (l/json->schema client-pcf)))
                  true)

                :client
                (do
                  (swap! *url->server-fp assoc url server-fp)
                  (reset! *server-schema (l/json->schema server-pcf))
                  true)

                :none
                (do
                  (when-not (nil? server-fp)
                    (swap! *url->server-fp assoc url server-fp)
                    (reset! *server-schema (l/json->schema server-pcf)))
                  (recur true))))))
        (catch #?(:clj Exception :cljs js/Error) e
          (log/error (str "Schema negotiation failed: "
                          (log/ex-msg-and-stacktrace e)))
          false))))

  (start-send-loop* [this]
    (ca/go
      (try
        (while (not @*shutdown?)
          (let [[msg-info ch] (ca/alts! [send-chan (ca/timeout 100)])]
            (when (= send-chan ch)
              (let [{:keys [msg failure-time-ms failure-cb]} msg-info]
                (loop []
                  (when (not @*shutdown?)
                    (if-let [tube-client @*tube-client]
                      (tc/send tube-client (l/serialize msgs-union-schema msg))
                      (do
                        (when failure-time-ms
                          (if (> (u/get-current-time-ms) failure-time-ms)
                            (when failure-cb
                              (failure-cb
                               (ex-info
                                "Send timed out waiting for connection."
                                (u/sym-map msg-info))))
                            (do
                              (ca/<! (ca/timeout 100))
                              (recur))))))))))))
        (catch #?(:clj Exception :cljs js/Error) e
          (log/error (str "Unexpected error in send loop: "
                          (log/ex-msg-and-stacktrace e)))))))

  (start-gc-loop* [this]
    (u/start-gc-loop *shutdown? *rpc-id->rpc-info))

  (start-rcv-loop* [this]
    (ca/go
      (while (not @*shutdown?)
        (try
          (if-let [rcv-chan @*rcv-chan]
            (let [[data ch] (au/alts? [rcv-chan (ca/timeout 1000)])]
              (when (= rcv-chan ch)
                (let [conn-id 0 ;; there is only one connection
                      sender (fn [msg]
                               (when-not (ca/offer! send-chan (u/sym-map msg))
                                 (log/error (str "RPC rsp cannot be sent. "
                                                 "Queue is full."))))]
                  (u/handle-rcv :client conn-id sender (name peer-role)
                                (name peer-role) data
                                msgs-union-schema @*server-schema
                                *msg-rec-name->handler))))
            (ca/<! (ca/timeout 100))) ;; Wait for rcv-chan to be set
          (catch #?(:clj Exception :cljs js/Error) e
            (log/error (str "Unexpected error in rcv-loop: "
                            (log/ex-msg-and-stacktrace e)))
            ;; Rate limit
            (ca/<! (ca/timeout 1000))))))))


(s/defn client :- (s/protocol ICapsuleClient)
  ([get-url :- u/GetURLFn
    get-credentials :- u/GetCredentialsFn
    protocol :- u/Protocol
    role :- u/Role]
   (client get-url get-credentials protocol role {}))
  ([get-url :- u/GetURLFn
    get-credentials :- u/GetCredentialsFn
    protocol :- u/Protocol
    role :- u/Role
    options :- u/ClientOptions]
   (when-not (ifn? get-url)
     (throw (ex-info "`get-url` parameter must be a function."
                     (u/sym-map get-url))))
   (when-not (ifn? get-credentials)
     (throw (ex-info "`get-credentials` parameter must be a function."
                     (u/sym-map get-credentials))))
   (u/check-protocol protocol)
   (when-not (keyword? role)
     (throw (ex-info "`role` parameter must be a keyword." (u/sym-map role))))
   (when-not (map? options)
     (throw (ex-info "`options` parameter must be a map."
                     (u/sym-map options))))
   (let [{:keys [default-rpc-timeout-ms
                 get-credentials-timeout-ms
                 get-url-timeout-ms
                 rcv-queue-size
                 send-queue-size
                 silence-log?
                 on-connect
                 on-disconnect
                 on-login-result
                 handlers
                 <ws-client]} (merge default-client-options options)
         *rcv-chan (atom nil)
         send-chan (ca/chan send-queue-size)
         reconnect-chan (ca/chan)
         peer-role (u/get-peer-role protocol role)
         peer-msgs (u/get-msgs-name-set protocol peer-role)
         peer-rpcs (u/get-rpcs-name-set protocol peer-role)
         msgs (u/get-msgs-name-set protocol role)
         rpcs (u/get-rpcs-name-set protocol role)
         msgs-union-schema (u/msgs-union-schema protocol)
         client-fp (l/fingerprint64 msgs-union-schema)
         client-pcf (l/pcf msgs-union-schema)
         *url->server-fp (atom {})
         *server-schema (atom nil)
         *rpc-id (atom 0)
         *tube-client (atom nil)
         *credentials (atom nil)
         *shutdown? (atom false)
         *rpc-id->rpc-info (atom {})
         handler-map (u/make-rpc-rsp-handler-map
                      protocol role *rpc-id->rpc-info silence-log?)
         *msg-rec-name->handler (atom handler-map)
         client (->CapsuleClient
                 get-url get-url-timeout-ms get-credentials
                 get-credentials-timeout-ms *rcv-chan send-chan reconnect-chan
                 msgs-union-schema client-fp client-pcf default-rpc-timeout-ms
                 rcv-queue-size send-queue-size silence-log? on-connect
                 on-disconnect on-login-result role msgs rpcs peer-role
                 peer-msgs peer-rpcs
                 *url->server-fp *server-schema *rpc-id *tube-client
                 *credentials *shutdown? *rpc-id->rpc-info
                 *msg-rec-name->handler)]
     (doseq [[msg-name-kw handler] handlers]
       (set-handler client msg-name-kw handler))
     (start-connect-loop* client <ws-client)
     (start-gc-loop* client)
     (start-rcv-loop* client)
     (start-send-loop* client)
     client)))
