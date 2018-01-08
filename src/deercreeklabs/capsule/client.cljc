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

(def initial-conn-wait-ms 5000)
(def max-conn-wait-ms 60000)

;; TODO: Measure timings and set these appropriately
(def default-client-options
  {:connect-timeout-ms 5000
   :default-op-timeout-ms 3000
   :max-reconnect-wait-ms 2000
   :max-op-timeout-ms 10000
   :max-ops-per-second 10
   :op-burst-seconds 3
   :max-total-op-time-ms 10000
   :on-reconnect-login-failure (constantly nil)
   :silence-log? false})

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
  (<connect* [this])
  (enqueue-op* [this op failure-cb])
  (do-op* [this req-name rsp-name arg success-cb failure-cb timeout-ms])
  (<send-op* [this op])
  (<do-schema-negotiation* [this tube-client rcv-chan])
  (handle-rpc-success-rsp* [this msg])
  (handle-rpc-failure-rsp* [this msg])
  (handle-login-rsp* [this msg])
  (handle-logout-rsp* [this msg])
  (start-retry-loop* [this])
  (start-send-loop* [this])
  (start-rcv-loop* [this]))

(defrecord CapsuleClient
    [<get-uri *rcv-chan on-reconnect-login-failure msg-union-schema
     client-fp client-pcf *server-fp *server-pcf rpc-name-kws
     event-name-kws op-chan max-op-timeout-ms default-op-timeout-ms
     max-total-op-time-ms max-ops-per-second op-burst-seconds
     silence-log? *op-id *tube-client *logged-in? *subject-id
     *credential *msg-record-name->handler *shutdown? *op-id->op]
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

  (<connect* [this]
    (ca/go
      (try
        (loop [wait-ms initial-conn-wait-ms]
          (when-let [tube-client @*tube-client]
            (tc/close tube-client)
            (reset! *tube-client nil)
            (reset! *logged-in? false))
          (when-not @*shutdown?
            (let [uri-ch (<get-uri)
                  [uri ch] (au/alts? [uri-ch (ca/timeout wait-ms)])]
              (if (not= uri-ch ch)
                (recur (min (* 2 wait-ms) max-conn-wait-ms))
                (do
                  (if (not (string? uri))
                    (do
                      (errorf "<get-uri did not return a string, returned: %s"
                              uri)
                      (recur (min (* 2 wait-ms) max-conn-wait-ms)))
                    (let [rcv-chan (ca/chan 1000)
                          opts {:on-disconnect
                                (fn [conn code reason]
                                  (when-not silence-log?
                                    (infof
                                     (str "Connection to %s disconnected: "
                                          "%s (%s)")
                                     (connection/get-uri conn) reason code))
                                  (<connect* this))
                                :on-rcv (fn on-rcv [conn data]
                                          (ca/put! rcv-chan data))}
                          tube-client (au/<? (tc/<make-tube-client
                                              uri wait-ms opts))]
                      (if-not tube-client
                        (recur (min (* 2 wait-ms) max-conn-wait-ms))
                        (do
                          (au/<? (<do-schema-negotiation* this tube-client
                                                          rcv-chan))
                          (if @*shutdown?
                            (tc/close tube-client)
                            (do
                              ;; If we were logged in, log in again
                              (when-let [subject-id @*subject-id]
                                (when-let [credential @*credential]
                                  (let [success-ch (ca/chan)
                                        success-cb #(ca/put! success-ch true)
                                        _ (log-in this subject-id credential
                                                  success-cb
                                                  on-reconnect-login-failure)
                                        timeout-ch (ca/timeout 10000)
                                        [ret ch] (au/alts?
                                                  [success-ch timeout-ch])]
                                    (when (= timeout-ch ch)
                                      (tc/close tube-client)
                                      (on-reconnect-login-failure
                                       "Timed out re-authenticating.")))))
                              (if @*shutdown?
                                (tc/close tube-client)
                                (do
                                  (reset! *rcv-chan rcv-chan)
                                  (reset! *tube-client tube-client))))))))))))))
        (catch #?(:clj Exception :cljs js/Error) e
          (errorf "Unexpected error while (re)connecting: %s"
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
    (au/go
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
                      (reset! *server-pcf client-pcf)))
            :client (do
                      (reset! *server-fp server-fp)
                      (reset! *server-pcf server-pcf))
            :none (do
                    (when-not (nil? server-fp)
                      (reset! *server-fp server-fp)
                      (reset! *server-pcf server-pcf))
                    (recur true)))))))

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
            (infof "Login failed."))
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
      (dotimes [i buf-size] ;; Preload credits
        (ca/put! credit-ch :one-credit))
      (ca/go
        (while (not @*shutdown?)
          (ca/>! credit-ch :one-credit)
          (ca/<! (ca/timeout credit-ch-delay-ms))))
      (ca/go
        (try
          (while (not @*shutdown?)
            (let [[op ch] (ca/alts! [op-chan (ca/timeout 100)])]
              (when (= op-chan ch)
                (ca/<! credit-ch) ;; Consume a credit
                (au/<? (<send-op* this op)))))
          (catch #?(:clj Exception :cljs js/Error) e
            (errorf "Unexpected error in send loop: %s"
                    (lu/get-exception-msg-and-stacktrace e)))))))

  (start-rcv-loop* [this]
    (ca/go
      (while (not @*shutdown?)
        (try
          (if-let [rcv-chan @*rcv-chan]
            (let [[data ch] (ca/alts! [rcv-chan (ca/timeout 1000)])]
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
         {:keys [max-ops-per-second max-op-timeout-ms
                 op-burst-seconds default-op-timeout-ms max-total-op-time-ms
                 connect-timeout-ms max-reconnect-wait-ms
                 on-reconnect-login-failure silence-log?]} opts
         rpc-name-kws (into #{} (keys (:rpcs api)))
         event-name-kws (into #{} (keys (:events api)))
         op-chan (ca/chan (ceil (* max-ops-per-second op-burst-seconds
                                   (/ max-op-timeout-ms 1000))))
         msg-union-schema (u/make-msg-union-schema api)
         client-pcf (l/get-parsing-canonical-form msg-union-schema)
         client-fp (u/long->byte-array (l/get-fingerprint64 msg-union-schema))
         *rcv-chan (atom nil)
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
                 <get-uri *rcv-chan on-reconnect-login-failure msg-union-schema
                 client-fp client-pcf *server-fp *server-pcf rpc-name-kws
                 event-name-kws op-chan max-op-timeout-ms default-op-timeout-ms
                 max-total-op-time-ms max-ops-per-second op-burst-seconds
                 silence-log? *op-id *tube-client *logged-in? *subject-id
                 *credential *msg-record-name->handler *shutdown? *op-id->op)]
     (<connect* client)
     (start-retry-loop* client)
     (start-rcv-loop* client)
     (start-send-loop* client)
     client)))
