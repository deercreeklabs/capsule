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

;; TODO: Measure timings and set these appropriately
(def default-client-options
  {:default-op-timeout-ms 10000
   :max-login-attempts 5
   :max-op-timeout-ms 10000
   :max-ops-per-second 10
   :max-total-op-time-ms 10000
   :op-burst-seconds 3
   :rate-limit? true
   :silence-log? false
   :<get-reconnect-credentials #(ca/go
                                  {:subject-id "" :credential ""})
   :<on-reconnect (fn [capsule-client]
                    (ca/go
                      (infof "Client reconnected.")
                      nil))})

(defn get-op-id* [*op-id]
  (swap! *op-id (fn [op-id]
                  (let [new-op-id (inc op-id)]
                    (if (> new-op-id 2147483647)
                      0
                      new-op-id)))))

(defprotocol ICapsuleClient
  (<send-rpc
    [this rpc-name-kw arg]
    [this rpc-name-kw arg timeout-ms])
  (send-rpc
    [this rpc-name-kw arg]
    [this rpc-name-kw arg success-cb failure-cb]
    [this rpc-name-kw arg success-cb failure-cb timeout-ms])
  (log-in
    [this subject-id credential]
    [this subject-id credential success-cb failure-cb])
  (<log-in [this subject-id credential])
  (log-out
    [this]
    [this success-cb failure-cb])
  (<log-out [this])
  (logged-in? [this])
  (bind-event [this event-name-kw event-handler])
  (shutdown [this])
  (enqueue-op* [this op failure-cb])
  (do-op* [this req-name rsp-name arg success-cb failure-cb timeout-ms])
  (<send-op* [this op])
  (<do-schema-negotiation* [this tube-client rcv-chan])
  (handle-rpc-success-rsp* [this msg])
  (handle-rpc-failure-rsp* [this msg])
  (handle-login-rsp* [this msg])
  (handle-logout-rsp* [this msg])
  (<do-login* [this tube-client rcv-chan subject-id credential])
  (<attempt-reauth* [this tube-client rcv-chan])
  (<connect* [this])
  (start-connect-loop* [this])
  (start-retry-loop* [this])
  (start-send-loop* [this])
  (start-rcv-loop* [this]))

(defrecord CapsuleClient
    [<get-uri *rcv-chan <get-reconnect-credentials
     max-login-attempts msg-union-schema
     client-fp client-pcf *server-fp *server-pcf rpc-name-kws
     event-name-kws op-chan max-op-timeout-ms default-op-timeout-ms
     max-total-op-time-ms max-ops-per-second op-burst-seconds
     rate-limit? silence-log? reconnect-chan <on-reconnect
     *op-id *tube-client *logged-in? *subject-id *credential
     *msg-record-name->handler *shutdown? *op-id->op]

  ICapsuleClient
  (<send-rpc [this rpc-name-kw arg]
    (<send-rpc this rpc-name-kw arg default-op-timeout-ms))

  (<send-rpc [this rpc-name-kw arg timeout-ms]
    (let [ch (ca/chan)
          cb #(ca/put! ch %)]
      (send-rpc this rpc-name-kw arg cb cb timeout-ms)
      ch))

  (send-rpc [this rpc-name-kw arg]
    (send-rpc this rpc-name-kw arg nil nil default-op-timeout-ms))

  (send-rpc [this rpc-name-kw arg success-cb failure-cb]
    (send-rpc this rpc-name-kw arg success-cb failure-cb
              default-op-timeout-ms))

  (send-rpc [this rpc-name-kw arg success-cb failure-cb timeout-ms]
    (if-not (rpc-name-kws rpc-name-kw)
      (failure-cb (ex-info (str "RPC `" rpc-name-kw "` is not in the API.")
                           (u/sym-map rpc-name-kw)))
      (let [req-name (u/make-msg-record-name :rpc-req rpc-name-kw)
            rsp-name (u/make-msg-record-name :rpc-rsp rpc-name-kw)]
        (do-op* this req-name rsp-name arg success-cb failure-cb
                timeout-ms))))

  (<log-in [this subject-id credential]
    (let [ch (ca/chan)
          success-cb #(ca/put! ch [true %])
          failure-cb #(ca/put! ch [false %])]
      (log-in this subject-id credential success-cb failure-cb)
      ch))

  (log-in [this subject-id credential]
    (log-in this subject-id credential nil nil))

  (log-in [this subject-id credential success-cb failure-cb]
    (let [success-cb* #(do
                         (reset! *subject-id subject-id)
                         (reset! *credential credential)
                         (success-cb %))
          arg (u/sym-map subject-id credential)]
      (do-op* this ::u/login-req ::u/login-rsp arg success-cb* failure-cb
              default-op-timeout-ms)))

  (<log-out [this]
    (let [ch (ca/chan)
          cb #(ca/put! ch %)]
      (log-out this cb cb)
      ch))

  (log-out [this]
    (log-out this nil nil))

  (log-out [this success-cb failure-cb]
    (do-op* this ::u/logout-req ::u/logout-rsp nil success-cb failure-cb
            default-op-timeout-ms))

  (logged-in? [this]
    @*logged-in?)

  (bind-event [this event-name-kw event-handler]
    (if-not (event-name-kws event-name-kw)
      (let [error-str (str "Event `" event-name-kw "` is not in the API.")]
        (throw (ex-info error-str
                        {:type :illegal-argument
                         :subtype :unknown-event-name-kw
                         :error-str error-str
                         :event-name-kw event-name-kw})))
      (let [rec-name (u/make-msg-record-name :event event-name-kw)]
        (swap! *msg-record-name->handler assoc rec-name
               (fn [client msg]
                 (event-handler (:event msg)))))))

  (shutdown [this]
    (reset! *shutdown? true)
    (when-let [tube-client @*tube-client]
      (tc/close tube-client)
      (reset! *tube-client nil)))

  (<do-login* [this tube-client rcv-chan subject-id credential]
    (au/go
      (let [op-id 1
            arg (u/sym-map subject-id credential)
            msg (u/sym-map op-id arg)
            _ (tc/send tube-client (l/serialize msg-union-schema
                                                [::u/login-req msg]))
            data (au/<? rcv-chan)
            [msg-name msg] (l/deserialize msg-union-schema
                                          @*server-pcf data)]
        (if-not (= ::u/login-rsp msg-name)
          (do
            (errorf "Got wrong login rsp msg: %s" msg-name)
            (shutdown this)
            false)
          (let [{:keys [was-successful reason]} msg]
            (if was-successful
              (do
                (when-not silence-log?
                  (infof "Reconnect login succeeded."))
                (reset! *logged-in? true)
                (reset! *subject-id subject-id)
                (reset! *credential credential))
              (when-not silence-log?
                (infof "Login failed: %s" reason)))
            was-successful)))))

  (<attempt-reauth* [this tube-client rcv-chan]
    (au/go
      (let [subject-id @*subject-id
            credential @*credential]
        (when (and subject-id credential)
          (debugf "Attempting to reauthenticate.")
          (loop [attempts 0
                 subject-id subject-id
                 credential credential]
            (let [timeout-ch (ca/timeout max-op-timeout-ms)
                  login-ch (<do-login* this tube-client rcv-chan
                                       subject-id credential)
                  [success? ch] (au/alts? [timeout-ch login-ch])]
              (if (not= login-ch ch)
                (do
                  (errorf "Login attempt timed out after %s ms. Shutting down."
                          max-op-timeout-ms)
                  (shutdown this))
                (when-not success?
                  (if (>= (inc attempts) max-login-attempts)
                    (do
                      (errorf "Max reconnect login attempts (%s) reached."
                              max-login-attempts)
                      (shutdown this))
                    (let [timeout-ch (ca/timeout max-op-timeout-ms)
                          creds-ch (<get-reconnect-credentials)
                          [creds ch] (au/alts? [timeout-ch creds-ch])]
                      (if (= timeout-ch ch)
                        (do
                          (errorf (str "<get-reconnect-credentials did not "
                                       "return within %s ms. Shutting down.")
                                  max-op-timeout-ms)
                          (shutdown this))
                        (let [{:keys [subject-id credential]} creds]
                          (recur (inc attempts)
                                 subject-id
                                 credential)))))))))))))

  (<connect* [this]
    (au/go
      (debugf "Entering <connect*")
      (loop [wait-ms initial-conn-wait-ms]
        (when-not @*shutdown?
          (debugf "Top of <connect* loop. wait-ms: %.0f" (float wait-ms))
          (let [rand-mult (+ 0.5 (rand))
                new-wait-ms (-> (* wait-ms conn-wait-ms-multiplier)
                                (min max-conn-wait-ms)
                                (* rand-mult))
                uri-ch (<get-uri)
                [uri ch] (au/alts? [uri-ch (ca/timeout wait-ms)])]
            (if (not= uri-ch ch)
              (do
                (debugf "Timed out waiting for <get-uri.")
                (recur new-wait-ms))
              (do
                (if (not (string? uri))
                  (do
                    (errorf "<get-uri did not return a string, returned: %s"
                            uri)
                    (ca/<! (ca/timeout wait-ms))
                    (recur new-wait-ms))
                  (let [_ (debugf (str "Got F1 uri: %s. Attempting "
                                       "websocket connection.") uri)
                        rcv-chan (ca/chan 1000)
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
                                  (reset! *logged-in? false)
                                  (ca/put! reconnect-chan true)))
                              :on-rcv (fn on-rcv [conn data]
                                        (ca/put! rcv-chan data))}
                        tube-client (au/<? (tc/<make-tube-client
                                            uri wait-ms opts))]
                    (if-not tube-client
                      (do
                        (debugf "make-tube-client failed.")
                        (ca/<! (ca/timeout wait-ms))
                        (recur new-wait-ms))
                      (do
                        (debugf "make-tube-client succeeded.")
                        (if-not (au/<? (<do-schema-negotiation* this tube-client
                                                                rcv-chan))
                          (do
                            (tc/close tube-client)
                            (ca/<! (ca/timeout wait-ms))
                            (recur new-wait-ms))
                          (if @*shutdown?
                            (do
                              (tc/close tube-client)
                              false)
                            (do
                              (debugf "Schema negotiation succeeded.")
                              (au/<? (<attempt-reauth* this tube-client
                                                       rcv-chan))
                              (if @*shutdown?
                                (do
                                  (tc/close tube-client)
                                  false)
                                (do
                                  (debugf "tube-client is connected")
                                  (reset! *rcv-chan rcv-chan)
                                  (reset! *tube-client tube-client)
                                  true))))))))))))))))

  (start-connect-loop* [this]
    (debugf "Entering start-connect-loop*")
    (ca/go
      (try
        (au/<? (<connect* this))
        (while (not @*shutdown?)
          (let [[reconnect? ch] (ca/alts! [reconnect-chan
                                           (ca/timeout initial-conn-wait-ms)])]
            (when (and (= reconnect-chan ch) reconnect?)
              (let [success? (au/<? (<connect* this))]
                (if success?
                  (<on-reconnect this))))))
        (catch #?(:clj Exception :cljs js/Error) e
          (errorf "Unexpected error in connect loop: %s"
                  (lu/get-exception-msg-and-stacktrace e))
          (shutdown this)))))

  (enqueue-op* [this op failure-cb]
    (let [ret (ca/offer! op-chan op)]
      (when-not ret ;; chan is full
        (failure-cb (ex-info (str "This operation cannot be completed "
                                  "within " max-op-timeout-ms " ms.")
                             (u/sym-map max-op-timeout-ms op))))
      nil))

  (do-op* [this req-name rsp-name arg success-cb failure-cb timeout-ms]
    (if @*shutdown?
      (throw (ex-info "Client is shut down" {}))
      (let [timeout-ms (max timeout-ms max-op-timeout-ms)
            op-id (get-op-id* *op-id)
            now (u/get-current-time-ms)
            retry-time-ms (+ now timeout-ms)
            failure-time-ms (+ now max-total-op-time-ms)
            op (u/sym-map req-name rsp-name arg op-id success-cb failure-cb
                          timeout-ms retry-time-ms failure-time-ms)]
        (enqueue-op* this op failure-cb))))

  (<send-op* [this op]
    (let [{:keys [failure-time-ms failure-cb]} op]
      (au/go
        (loop []
          (when (not @*shutdown?)
            (if-let [tube-client @*tube-client]
              (let [{:keys [req-name op-id arg]} op
                    msg (u/sym-map op-id arg)]
                (tc/send tube-client (l/serialize msg-union-schema
                                                  [req-name msg]))
                (swap! *op-id->op assoc op-id op))
              (if (> (u/get-current-time-ms) failure-time-ms)
                (failure-cb (ex-info
                             "Operation timed out waiting for connection."
                             (u/sym-map op)))
                (do
                  (ca/<! (ca/timeout 100))
                  (recur)))))))))

  (<do-schema-negotiation* [this tube-client rcv-chan]
    (ca/go
      (try
        (loop [retry? false]
          (let [known-server-fp @*server-fp
                req (cond-> {:client-fp client-fp
                             :server-fp (or known-server-fp client-fp)}
                      retry? (assoc :client-pcf client-pcf))
                _ (tc/send tube-client (l/serialize u/handshake-req-schema req))
                rsp (l/deserialize u/handshake-rsp-schema
                                   (l/get-parsing-canonical-form
                                    u/handshake-rsp-schema)
                                   (au/<? rcv-chan))
                {:keys [match server-fp server-pcf]} rsp]
            (case match
              :both (do
                      (reset! *server-fp known-server-fp)
                      (when-not known-server-fp
                        (reset! *server-pcf client-pcf))
                      true)
              :client (do
                        (reset! *server-fp server-fp)
                        (reset! *server-pcf server-pcf)
                        true)
              :none (do
                      (when-not (nil? server-fp)
                        (reset! *server-fp server-fp)
                        (reset! *server-pcf server-pcf))
                      (recur true)))))
        (catch #?(:clj Exception :cljs js/Error) e
          (errorf "Schema negotiation failed: %s"
                  (lu/get-exception-msg-and-stacktrace e))
          false))))

  (handle-rpc-success-rsp* [this msg]
    (let [{:keys [op-id ret]} msg
          {:keys [success-cb]} (@*op-id->op op-id)]
      (swap! *op-id->op dissoc op-id)
      (when success-cb
        (success-cb ret))))

  (handle-rpc-failure-rsp* [this msg]
    (let [{:keys [op-id rpc-name rpc-arg failure-type error-str]} msg
          {:keys [failure-cb]} (@*op-id->op op-id)
          error-msg (str "RPC failed.\n  RPC id: " op-id "\n  RPC name: "
                         rpc-name "\n  Argument: "
                         rpc-arg "\n  Fail type: " (name failure-type)
                         "\n  Error msg: " error-str)]
      (swap! *op-id->op dissoc op-id)
      (when-not silence-log?
        (errorf "%s" error-msg))
      (when failure-cb
        (failure-cb (ex-info error-msg msg)))))

  (handle-login-rsp* [this msg]
    (let [{:keys [op-id was-successful reason]} msg
          op (@*op-id->op op-id)
          {:keys [success-cb failure-cb]} op]
      (if was-successful
        (do
          (when-not silence-log?
            (infof "Login succeeded."))
          (reset! *logged-in? true)
          (when success-cb
            (success-cb "Login succeeded")))
        (do
          (when-not silence-log?
            (infof "Login failed: %s" reason))
          (reset! *logged-in? false)
          (when failure-cb
            (failure-cb (or reason "Login failed.")))))))

  (handle-logout-rsp* [this msg]
    (let [{:keys [op-id was-successful]} msg
          {:keys [success-cb failure-cb]} (@*op-id->op op-id)]
      (if was-successful
        (do
          (when-not silence-log?
            (infof "Logout succeeded."))
          (reset! *logged-in? false)
          (when success-cb
            (success-cb "Logout succeeded")))
        (do
          (when-not silence-log?
            (infof "Logout failed."))
          (when failure-cb
            (failure-cb "Logout failed."))))))

  (start-retry-loop* [this]
    (ca/go
      (try
        (while (not @*shutdown?)
          (doseq [[op-id op] @*op-id->op]
            (let [{:keys [retry-time-ms failure-time-ms op-id failure-cb]} op
                  now (u/get-current-time-ms)]
              (cond
                (> now failure-time-ms)
                (do
                  (swap! *op-id->op dissoc op-id)
                  (failure-cb (ex-info
                               "Operation timed out. No response received."
                               (u/sym-map op op-id))))

                (> now retry-time-ms)
                (let [{:keys [timeout-ms failure-cb]} op
                      op-id (get-op-id* *op-id)
                      new-op (assoc op
                                    :op-id op-id
                                    :retry-time-ms (+ now timeout-ms))]
                  (swap! *op-id->op dissoc op-id)
                  (enqueue-op* this op failure-cb)))))
          (ca/<! (ca/timeout 1000)))
        (catch #?(:clj Exception :cljs js/Error) e
          (lu/log-exception e)
          (shutdown this)))))

  (start-send-loop* [this]
    (let [buf-size (* max-ops-per-second op-burst-seconds)
          credit-ch (ca/chan (ca/dropping-buffer buf-size))
          credit-ch-delay-ms (/ 1000 max-ops-per-second)]
      (when rate-limit?
        (dotimes [i buf-size] ;; Preload credits
        (ca/put! credit-ch :one-credit))
        (ca/go
          (while (not @*shutdown?)
            (ca/>! credit-ch :one-credit)
            (ca/<! (ca/timeout credit-ch-delay-ms)))))
      (ca/go
        (try
          (while (not @*shutdown?)
            (let [[op ch] (ca/alts! [op-chan (ca/timeout 100)])]
              (when (= op-chan ch)
                (when rate-limit?
                  (ca/<! credit-ch)) ;; Consume a credit
                (au/<? (<send-op* this op)))))
          (catch #?(:clj Exception :cljs js/Error) e
            (errorf "Unexpected error in send loop: %s"
                    (lu/get-exception-msg-and-stacktrace e)))))))

  (start-rcv-loop* [this]
    (ca/go
      (while (not @*shutdown?)
        (try
          (if-let [rcv-chan @*rcv-chan]
            (let [[data ch] (au/alts? [rcv-chan (ca/timeout 1000)])]
              (when (= rcv-chan ch)
                (let [[msg-name msg] (l/deserialize msg-union-schema
                                                    @*server-pcf data)
                      handler (@*msg-record-name->handler msg-name)]
                  (if handler
                    (try
                      (handler this msg)
                      (catch #?(:clj Exception :cljs js/Error) e
                        (errorf "Exception in %s msg handler: %s" msg-name e)
                        (lu/log-exception e)))
                    (infof "No handler found for msg name: %s" msg-name)))))
            (ca/<! (ca/timeout 100))) ;; Wait for rcv-chan to be set
          (catch #?(:clj Exception :cljs js/Error) e
            (errorf "Unexpected error in rcv-loop: %s"
                    (lu/get-exception-msg-and-stacktrace e))
            ;; Rate limit
            (ca/<! (ca/timeout 1000))))))))

(defn deserialize* [msg-union-schema pcf data]
  (l/deserialize msg-union-schema
                 (l/get-parsing-canonical-form msg-union-schema)
                 data))

(defn ceil [n]
  #?(:clj (int (Math/ceil n))
     :cljs (.ceil js/Math n)))

(defn make-msg-record-name->handler
  [rpc-name-kws *logged-in? *op-id->op]
  (let [base {::u/login-rsp handle-login-rsp*
              ::u/logout-rsp handle-logout-rsp*
              ::u/rpc-failure-rsp handle-rpc-failure-rsp*}]
    (reduce (fn [acc rpc-name]
              (let [rec-name (u/make-msg-record-name :rpc-success-rsp rpc-name)]
                (assoc acc rec-name handle-rpc-success-rsp*)))
            base rpc-name-kws)))

(s/defn make-client :- (s/protocol ICapsuleClient)
  ([api :- u/API
    <get-uri :- u/GetURIFn]
   (make-client api <get-uri {}))
  ([api :- u/API
    <get-uri :- u/GetURIFn
    opts :- u/ClientOptions]
   (let [opts (merge default-client-options opts)
         {:keys [default-op-timeout-ms
                 max-login-attempts
                 max-op-timeout-ms
                 max-ops-per-second
                 max-total-op-time-ms
                 op-burst-seconds
                 rate-limit?
                 silence-log?
                 <get-reconnect-credentials
                 <on-reconnect]} opts
         rpc-name-kws (into #{} (keys (:rpcs api)))
         event-name-kws (into #{} (keys (:events api)))
         op-chan (ca/chan (ceil (* max-ops-per-second op-burst-seconds
                                   (/ max-op-timeout-ms 1000))))
         msg-union-schema (u/make-msg-union-schema api)
         client-pcf (l/get-parsing-canonical-form msg-union-schema)
         client-fp (u/long->byte-array (l/get-fingerprint64 msg-union-schema))
         *rcv-chan (atom nil)
         reconnect-chan (ca/chan)
         *op-id (atom 0)
         *tube-client (atom nil)
         *server-pcf (atom nil)
         *server-fp (atom nil)
         *event-name-kw->handler (atom {})
         *logged-in? (atom false)
         *subject-id (atom nil)
         *credential (atom nil)
         *shutdown? (atom false)
         *op-id->op (atom {})
         *msg-record-name->handler (atom
                                    (make-msg-record-name->handler
                                     rpc-name-kws *logged-in?
                                     *op-id->op))
         client (->CapsuleClient
                 <get-uri *rcv-chan <get-reconnect-credentials
                 max-login-attempts msg-union-schema
                 client-fp client-pcf *server-fp *server-pcf rpc-name-kws
                 event-name-kws op-chan max-op-timeout-ms default-op-timeout-ms
                 max-total-op-time-ms max-ops-per-second op-burst-seconds
                 rate-limit? silence-log? reconnect-chan <on-reconnect
                 *op-id *tube-client *logged-in? *subject-id *credential
                 *msg-record-name->handler *shutdown? *op-id->op)]
     (start-connect-loop* client)
     (start-retry-loop* client)
     (start-rcv-loop* client)
     (start-send-loop* client)
     client)))
