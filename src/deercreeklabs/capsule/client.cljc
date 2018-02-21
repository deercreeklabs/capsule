(ns deercreeklabs.capsule.client
  (:require
   [clojure.core.async :as ca]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.capsule.utils :as u]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   [deercreeklabs.tube.client :as tc]
   [deercreeklabs.tube.connection :as connection]
   [schema.core :as s]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]]))

(def conn-wait-ms-multiplier 1.5)
(def initial-conn-wait-ms 1000)
(def max-conn-wait-ms 30000)

(def default-client-options
  {:default-rpc-timeout-ms 10000
   :rcv-queue-size 1000
   :send-queue-size 1000
   :silence-log? false
   :<on-reconnect (fn [capsule-client]
                    (ca/go
                      (infof "Client reconnected.")
                      nil))})

(defprotocol ICapsuleClient
  (<send-rpc
    [this rpc-name-kw arg]
    [this rpc-name-kw arg timeout-ms])
  (send-rpc
    [this rpc-name-kw arg success-cb failure-cb]
    [this rpc-name-kw arg success-cb failure-cb timeout-ms])
  (send-msg
    [this msg-name-kw arg]
    [this msg-name-kw arg timeout-ms])
  (set-rpc-handler [this rpc-name-kw handler])
  (set-msg-handler [this msg-name-kw handler])
  (shutdown [this]))

(defprotocol ICapsuleClientInternals
  (<do-login* [this tube-client rcv-chan credentials])
  (<do-auth-w-creds* [this tube-client rcv-chan credentials])
  (<get-credentials* [this])
  (<get-credentials-and-do-auth* [this tube-client rcv-chan])
  (<do-schema-negotiation-and-auth* [this tube-client rcv-chan url])
  (<do-schema-negotiation* [this tube-client rcv-chan url])
  (<get-url* [this wait-ms])
  (<connect* [this])
  (start-connect-loop* [this])
  (start-gc-loop* [this])
  (start-send-loop* [this])
  (start-rcv-loop* [this]))

(defrecord CapsuleClient
    [<get-url <get-credentials *rcv-chan send-chan reconnect-chan
     rpc-name->req-name msg-name->rec-name
     msgs-union-schema client-fp client-pcf default-rpc-timeout-ms
     rcv-queue-size send-queue-size silence-log? <on-reconnect
     role peer-role peer-name-maps
     *url->server-fp *server-pcf *rpc-id *tube-client *credentials
     *shutdown? *rpc-id->rpc-info *msg-rec-name->handler]

  ICapsuleClient
  (<send-rpc [this rpc-name-kw arg]
    (<send-rpc this rpc-name-kw arg default-rpc-timeout-ms))

  (<send-rpc [this rpc-name-kw arg timeout-ms]
    (let [ch (ca/chan)
          cb #(ca/put! ch %)]
      (send-rpc this rpc-name-kw arg cb cb timeout-ms)
      ch))

  (send-rpc [this rpc-name-kw arg success-cb failure-cb]
    (send-rpc this rpc-name-kw arg success-cb failure-cb
              default-rpc-timeout-ms))

  (send-rpc [this rpc-name-kw arg success-cb failure-cb timeout-ms]
    (if @*shutdown?
      (throw (ex-info "Client is shut down" {}))
      (let [msg-rec-name (rpc-name->req-name rpc-name-kw)
            _ (when-not msg-rec-name
                (throw
                 (ex-info (str "Protocol violation. Role `" role
                               "` does not have an RPC named `"
                               rpc-name-kw "`.")
                          (u/sym-map role rpc-name-kw arg))))
            rpc-id (u/get-rpc-id* *rpc-id)
            failure-time-ms (+ (u/get-current-time-ms) timeout-ms)
            rpc-info (u/sym-map rpc-name-kw arg rpc-id success-cb
                                failure-cb timeout-ms failure-time-ms)
            msg (u/sym-map rpc-id timeout-ms arg)
            msg-info (u/sym-map msg-rec-name msg failure-time-ms failure-cb)]
        (if (ca/offer! send-chan msg-info)
          (swap! *rpc-id->rpc-info assoc rpc-id rpc-info)
          (when failure-cb
            (failure-cb (ex-info "RPC cannot be sent. Send queue is full."
                                 {:rpc-info msg-info})))))))

  (send-msg [this msg-name-kw msg]
    (send-msg this msg-name-kw msg default-rpc-timeout-ms))

  (send-msg [this msg-name-kw msg timeout-ms]
    (if @*shutdown?
      (throw (ex-info "Client is shut down" {}))
      (let [msg-rec-name (msg-name->rec-name msg-name-kw)
            _ (when-not msg-rec-name
                (throw
                 (ex-info (str "Protocol violation. Role `" role
                       "` does not have a msg named `"
                       msg-name-kw "`.")
                  (u/sym-map role msg-name-kw msg))))
            msg {:arg msg}
            failure-time-ms (+ (u/get-current-time-ms) timeout-ms)
            msg-info (u/sym-map msg-rec-name msg failure-time-ms)]
        (ca/offer! send-chan msg-info))))

  (set-rpc-handler [this rpc-name-kw handler]
    (u/set-rpc-handler rpc-name-kw handler peer-role peer-name-maps
                       *msg-rec-name->handler))

  (set-msg-handler [this msg-name-kw handler]
    (u/set-msg-handler msg-name-kw handler peer-role peer-name-maps
                       *msg-rec-name->handler))

  (shutdown [this]
    (reset! *shutdown? true)
    (when-let [tube-client @*tube-client]
      (tc/close tube-client)
      (reset! *tube-client nil)))

  ICapsuleClientInternals
  (<do-login* [this tube-client rcv-chan credentials]
    (au/go
      (tc/send tube-client
               (l/serialize msgs-union-schema
                            (l/wrap u/login-req-schema credentials)))
      (let [data (au/<? rcv-chan)
            [msg-name msg] (l/deserialize msgs-union-schema
                                          @*server-pcf data)]
        (if-not (= ::u/login-rsp msg-name)
          (do
            (errorf "Got wrong login rsp msg: %s" msg-name)
            (shutdown this)
            false)
          (let [{:keys [was-successful]} msg]
            (when-not silence-log?
              (if was-successful
                (infof "Login succeeded.")
                (infof "Login failed.")))
            was-successful)))))

  (<do-auth-w-creds* [this tube-client rcv-chan credentials]
    (au/go
      (loop [wait-ms initial-conn-wait-ms]
        (let [timeout-ch (ca/timeout default-rpc-timeout-ms)
              login-ch (<do-login* this tube-client rcv-chan credentials)
              [success? ch] (au/alts? [timeout-ch login-ch])]
          (if (not= login-ch ch)
            (let [rand-mult (+ 0.5 (rand))
                  new-wait-ms (* wait-ms conn-wait-ms-multiplier rand-mult)]
              (if (< new-wait-ms max-conn-wait-ms)
                  (recur new-wait-ms)
                  (do
                    (errorf "Authentication timed out. Shutting down.")
                    (shutdown this))))
            (if-not success?
              (do
                (reset! *credentials nil)
                false)
              (do
                (reset! *credentials credentials)
                true)))))))

  (<get-credentials* [this]
    (ca/go
      (try
        (loop [wait-ms initial-conn-wait-ms]
          (let [credentials-ch (<get-credentials)
                [credentials ch] (au/alts?
                                  [credentials-ch (ca/timeout wait-ms)])]
            (if (= credentials-ch ch)
              credentials
              (let [rand-mult (+ 0.5 (rand))
                    new-wait-ms (* wait-ms conn-wait-ms-multiplier rand-mult)]
                (if (< new-wait-ms max-conn-wait-ms)
                  (recur new-wait-ms)
                  false)))))
        (catch #?(:clj Exception :cljs js/Error) e
          (errorf "Error in <get-credentials: %s"
                  (lu/get-exception-msg-and-stacktrace e))
          false))))

  (<get-credentials-and-do-auth* [this tube-client rcv-chan]
    (au/go
      (if-let [credentials (au/<? (<get-credentials* this))]
        (if (au/<? (<do-auth-w-creds* this tube-client rcv-chan
                                      credentials))
          (do
            (reset! *rcv-chan rcv-chan)
            (reset! *tube-client tube-client)
            true)
          (do
            (errorf "Authentication failed. Shutting down.")
            (shutdown this)))
        (do
          (errorf "<get-credentials failed. Shutting down.")
          (shutdown this)))))

  (<do-schema-negotiation-and-auth* [this tube-client rcv-chan url]
    (au/go
      (if @*shutdown?
        (tc/close tube-client)
        (do
          (au/<? (<do-schema-negotiation* this tube-client rcv-chan url))
          (if-let [credentials @*credentials] ;; Try cached creds first
            (if (au/<? (<do-auth-w-creds* this tube-client rcv-chan
                                          credentials))
              true
              (<get-credentials-and-do-auth* this tube-client rcv-chan))
            (<get-credentials-and-do-auth* this tube-client rcv-chan))))))

  (<get-url* [this wait-ms]
    (ca/go
      (try
        (let [url-ch (<get-url)
              [url ch] (au/alts? [url-ch (ca/timeout wait-ms)])]
          (if (= url-ch ch)
            url
            false))
        (catch #?(:clj Exception :cljs js/Error) e
          (errorf "Error in <get-url: %s"
                  (lu/get-exception-msg-and-stacktrace e))
          false))))

  (<connect* [this]
    (au/go
      (loop [wait-ms initial-conn-wait-ms]
        (when-not @*shutdown?
          (let [rand-mult (+ 0.5 (rand))
                new-wait-ms (-> (* wait-ms conn-wait-ms-multiplier)
                                (min max-conn-wait-ms)
                                (* rand-mult))]
            (let [url (ca/<! (<get-url* this wait-ms))]
              (if-not url
                (do
                  (ca/<! (ca/timeout wait-ms))
                  (recur new-wait-ms))
                (if-not (string? url)
                  (do
                    (errorf "<get-url did not return a string, returned: %s"
                            url)
                    (ca/<! (ca/timeout wait-ms))
                    (recur new-wait-ms))
                  (let [_ (when-not silence-log?
                            (debugf (str "Got url: %s. Attempting "
                                         "websocket connection.") url))
                        rcv-chan (ca/chan rcv-queue-size)
                        opts {:on-disconnect
                              (fn [conn code reason]
                                (when-not silence-log?
                                  (infof
                                   (str "Connection to %s disconnected: "
                                        "%s (%s)")
                                   (connection/get-uri conn) reason code))
                                (when-let [tube-client @*tube-client]
                                  (tc/close tube-client)
                                  (reset! *tube-client nil)
                                  (when-not @*shutdown?
                                    (ca/put! reconnect-chan true))))
                              :on-rcv (fn on-rcv [conn data]
                                        (ca/put! rcv-chan data))}
                        tube-client (au/<? (tc/<make-tube-client
                                            url wait-ms opts))]
                    (if tube-client
                      (au/<? (<do-schema-negotiation-and-auth* this tube-client
                                                               rcv-chan url))
                      (do
                        (when-not silence-log?
                          (debugf "make-tube-client failed."))
                        (ca/<! (ca/timeout wait-ms))
                        (recur new-wait-ms))))))))))))

  (start-connect-loop* [this]
    (ca/go
      (try
        (au/<? (<connect* this))
        (while (not @*shutdown?)
          (let [[reconnect? ch] (ca/alts! [reconnect-chan
                                           (ca/timeout initial-conn-wait-ms)])]
            (when (and (= reconnect-chan ch) reconnect?)
              (let [success? (au/<? (<connect* this))]
                (if success?
                  (<on-reconnect this)
                  (when-not @*shutdown?
                    (errorf "Client failed to reconnect. Shutting down.")
                    (shutdown this)))))))
        (catch #?(:clj Exception :cljs js/Error) e
          (errorf "Unexpected error in connect loop: %s"
                  (lu/get-exception-msg-and-stacktrace e))
          (shutdown this)))))

  (<do-schema-negotiation* [this tube-client rcv-chan url]
    (ca/go
      (try
        (loop [retry? false]
          (if @*shutdown?
            (tc/close tube-client)
            (let [known-server-fp (@*url->server-fp url)
                  req (cond-> {:client-fp client-fp
                               :server-fp (or known-server-fp client-fp)}
                        retry? (assoc :client-pcf client-pcf))
                  _ (tc/send tube-client (l/serialize
                                          u/handshake-req-schema req))
                  rsp (l/deserialize u/handshake-rsp-schema
                                     (l/get-parsing-canonical-form
                                      u/handshake-rsp-schema)
                                     (au/<? rcv-chan))
                  {:keys [match server-fp server-pcf]} rsp]
              (case match
                :both (do
                        (swap! *url->server-fp assoc url known-server-fp)
                        (when-not known-server-fp
                          (reset! *server-pcf client-pcf))
                        true)
                :client (do
                          (swap! *url->server-fp assoc url server-fp)
                          (reset! *server-pcf server-pcf)
                          true)
                :none (do
                        (when-not (nil? server-fp)
                          (swap! *url->server-fp assoc url server-fp)
                          (reset! *server-pcf server-pcf))
                        (recur true))))))
        (catch #?(:clj Exception :cljs js/Error) e
          (errorf "Schema negotiation failed: %s"
                  (lu/get-exception-msg-and-stacktrace e))
          false))))

  (start-send-loop* [this]
    (ca/go
      (try
        (while (not @*shutdown?)
          (let [[msg-info ch] (ca/alts! [send-chan (ca/timeout 100)])]
            (when (= send-chan ch)
              (let [{:keys [msg-rec-name msg
                            failure-time-ms failure-cb]} msg-info]
                (loop []
                  (when (not @*shutdown?)
                    (if-let [tube-client @*tube-client]
                      (do
                        (tc/send tube-client (l/serialize msgs-union-schema
                                                          [msg-rec-name msg])))
                      (when failure-time-ms
                        (if (> (u/get-current-time-ms) failure-time-ms)
                          (when failure-cb
                            (failure-cb
                             (ex-info
                              "Send timed out waiting for connection."
                              (u/sym-map msg-info))))
                          (do
                            (ca/<! (ca/timeout 100))
                            (recur)))))))))))
        (catch #?(:clj Exception :cljs js/Error) e
          (errorf "Unexpected error in send loop: %s"
                  (lu/get-exception-msg-and-stacktrace e))))))

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
                      sender (fn [msg-rec-name msg]
                               (when-not (ca/offer! send-chan
                                                  (u/sym-map msg-rec-name msg))
                                 (errorf
                                  "RPC rsp cannot be sent. Queue is full.")))]
                  (u/handle-rcv :client conn-id sender (name peer-role) data
                                msgs-union-schema @*server-pcf
                                *msg-rec-name->handler))))
            (ca/<! (ca/timeout 100))) ;; Wait for rcv-chan to be set
          (catch #?(:clj Exception :cljs js/Error) e
            (errorf "Unexpected error in rcv-loop: %s"
                    (lu/get-exception-msg-and-stacktrace e))
            ;; Rate limit
            (ca/<! (ca/timeout 1000))))))))


(s/defn make-client :- (s/protocol ICapsuleClient)
  ([<get-url :- u/GetURLFn
    <get-credentials :- u/GetCredentialsFn
    protocol :- u/Protocol
    role :- u/Role
    handlers :- u/HandlerMap]
   (make-client <get-url <get-credentials protocol role handlers {}))
  ([<get-url :- u/GetURLFn
    <get-credentials :- u/GetCredentialsFn
    protocol :- u/Protocol
    role :- u/Role
    handlers :- u/HandlerMap
    opts :- u/ClientOptions]
   (let [opts (merge default-client-options opts)
         {:keys [default-rpc-timeout-ms
                 rcv-queue-size
                 send-queue-size
                 silence-log?
                 <on-reconnect]} opts
         *rcv-chan (atom nil)
         send-chan (ca/chan send-queue-size)
         reconnect-chan (ca/chan)
         peer-role (u/get-peer-role protocol role)
         my-name-maps (u/make-name-maps protocol role)
         peer-name-maps (u/make-name-maps protocol peer-role)
         {:keys [rpc-name->req-name msg-name->rec-name]} my-name-maps
         msgs-union-schema (u/make-msgs-union-schema protocol)
         client-fp (l/get-fingerprint64 msgs-union-schema)
         client-pcf (l/get-parsing-canonical-form msgs-union-schema)
         *url->server-fp (atom {})
         *server-pcf (atom {})
         *rpc-id (atom 0)
         *tube-client (atom nil)
         *credentials (atom nil)
         *shutdown? (atom false)
         *rpc-id->rpc-info (atom {})
         *msg-rec-name->handler (atom (u/make-msg-rec-name->handler
                                       my-name-maps peer-name-maps
                                       *rpc-id->rpc-info handlers silence-log?))
         client (->CapsuleClient
                 <get-url <get-credentials *rcv-chan send-chan reconnect-chan
                 rpc-name->req-name msg-name->rec-name
                 msgs-union-schema client-fp client-pcf default-rpc-timeout-ms
                 rcv-queue-size send-queue-size silence-log? <on-reconnect
                 role peer-role peer-name-maps
                 *url->server-fp *server-pcf *rpc-id *tube-client *credentials
                 *shutdown? *rpc-id->rpc-info *msg-rec-name->handler)]
     (start-connect-loop* client)
     (start-gc-loop* client)
     (start-rcv-loop* client)
     (start-send-loop* client)
     client)))
