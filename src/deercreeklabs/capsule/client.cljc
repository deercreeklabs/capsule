(ns deercreeklabs.capsule.client
  (:require
   [clojure.core.async :as ca]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.capsule.utils :as u]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   [deercreeklabs.tube.client :as tc]
   [schema.core :as s]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]]))

(def connect-timeout-ms 2000)
(def max-reconnect-wait-ms 2000)
(def ^:dynamic **silence-log** false)

(defn call-callback-w-result [callback result]
  (when callback
    (callback {:error nil
               :result result})))

(defn call-callback-w-error [callback error-msg]
  (when callback
    (callback {:error error-msg
               :result nil})))

(s/defn enqueue-rpc :- u/Nil
  [rpc-chan :- au/Channel
   rpc :- u/RpcInfo
   cb :- u/RpcCallback
   max-rpc-timeout-ms :- s/Int]
  (let [ret (ca/offer! rpc-chan rpc)]
    (when-not ret ;; chan is full
      (call-callback-w-error cb (str "This operation cannot be completed "
                                     "within " max-rpc-timeout-ms " ms."))
      nil)))

(defn send-msg* [tube-client msg-union-schema msg-rec-name msg]
  (tc/send tube-client (l/serialize msg-union-schema [msg-rec-name msg])))

(defn send-rpc* [tube-client msg-union-schema rpc *rpc-id->rpc]
  (let [{:keys [rpc-req-msg-record-name rpc-id arg cb]} rpc
        req (u/sym-map rpc-id arg)]
    (do
      (send-msg* tube-client msg-union-schema rpc-req-msg-record-name req)
      (swap! *rpc-id->rpc assoc rpc-id rpc))))

(defn log-in* [tube-client msg-union-schema subject-id credential]
  (let [msg (u/sym-map subject-id credential)]
    (send-msg* tube-client msg-union-schema ::u/login-req msg)))

(defn get-rpc-id* [*rpc-id]
  (swap! *rpc-id (fn [rpc-id]
                   (let [new-rpc-id (inc rpc-id)]
                     (if (> new-rpc-id 2147483647)
                       0
                       new-rpc-id)))))

(defprotocol ICapsuleClient
  (send-rpc
    [this rpc-name-kw arg]
    [this rpc-name-kw arg cb]
    [this rpc-name-kw arg cb timeout-ms])
  (<send-rpc
    [this rpc-name-kw arg]
    [this rpc-name-kw arg timeout-ms])
  (log-in
    [this subject-id credential]
    [this subject-id credential cb])
  (<log-in [this subject-id credential])
  (log-out
    [this]
    [this cb])
  (<log-out [this])
  (bind-event [this event-name-kw event-handler])
  (shutdown [this]))

(defrecord CapsuleClient [msg-union-schema rpc-name-kws event-name-kws rpc-chan
                          max-rpc-timeout-ms
                          default-rpc-timeout-ms
                          max-total-rpc-time-ms *rpc-id *tube-client *login-cb
                          *subject-id *credential *logout-cb
                          *msg-record-name->handler *shutdown? *rpc-id->rpc]
  ICapsuleClient
  (send-rpc [this rpc-name-kw arg]
    (send-rpc this rpc-name-kw arg nil default-rpc-timeout-ms))

  (send-rpc [this rpc-name-kw arg cb]
    (send-rpc this rpc-name-kw arg cb default-rpc-timeout-ms))

  (send-rpc [this rpc-name-kw arg cb timeout-ms]
    (if @*shutdown?
      (throw (ex-info "Client is shut down" {}))
      (if-not (rpc-name-kws rpc-name-kw)
        (call-callback-w-error cb (str "RPC `" rpc-name-kw
                                       "` is not in the API."))
        (let [timeout-ms (max timeout-ms max-rpc-timeout-ms)
              rpc-id (get-rpc-id* *rpc-id)
              rpc-req-msg-record-name (u/make-msg-record-name :rpc-req
                                                              rpc-name-kw)
              rpc-rsp-msg-record-name (u/make-msg-record-name :rpc-rsp
                                                              rpc-name-kw)
              now (u/get-current-time-ms)
              retry-time-ms (+ now timeout-ms)
              failure-time-ms (+ now max-total-rpc-time-ms)
              rpc (u/sym-map rpc-req-msg-record-name
                             rpc-rsp-msg-record-name
                             arg rpc-id cb timeout-ms
                             retry-time-ms failure-time-ms)]
          (enqueue-rpc rpc-chan rpc cb max-rpc-timeout-ms)))))

  (<send-rpc [this rpc-name-kw arg]
    (<send-rpc this rpc-name-kw arg default-rpc-timeout-ms))

  (<send-rpc [this rpc-name-kw arg timeout-ms]
    (let [ch (ca/chan)
          cb #(ca/put! ch %)]
      (send-rpc this rpc-name-kw arg cb timeout-ms)
      ch))

  (<log-in [this subject-id credential]
    (let [ch (ca/chan)
          cb #(ca/put! ch %)]
      (log-in this subject-id credential cb)
      ch))

  (log-in [this subject-id credential]
    (log-in this subject-id credential nil))

  (log-in [this subject-id credential cb]
    (reset! *subject-id subject-id)
    (reset! *credential credential)
    (reset! *login-cb cb)
    (when-let [tube-client @*tube-client]
      (log-in* tube-client msg-union-schema subject-id credential)))

  (log-out [this]
    (log-out this nil))

  (log-out [this cb]
    (reset! *logout-cb cb)
    (when-let [tube-client @*tube-client]
      (send-msg* tube-client msg-union-schema ::u/logout-req nil)))

  (<log-out [this]
    (let [ch (ca/chan)
          cb #(ca/put! ch %)]
      (log-out this cb)
      ch))

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
               #(event-handler (:event %))))))

  (shutdown [this]
    (reset! *shutdown? true)))

(defn <do-schema-negotiation
  [rcv-chan tube-client client-fp client-pcf *server-fp *server-pcf]
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

(defn start-connection-loop
  [<get-uris msg-union-schema client-fp client-pcf *server-fp *server-pcf
   *subject-id *credential *rcv-chan *tube-client *shutdown?]
  (ca/go
    (while (not @*shutdown?)
      (try
        (let [uris (au/<? (<get-uris))
              uri (rand-nth uris)
              reconnect-ch (ca/chan)
              rcv-chan (ca/chan)
              tc-opts {:on-disconnect (fn on-disconnect [code reason]
                                        (reset! *tube-client nil)
                                        (ca/put! reconnect-ch true))
                       :on-rcv (fn on-rcv [conn data]
                                 (ca/put! rcv-chan data))}
              tube-client (au/<? (tc/<make-tube-client uri connect-timeout-ms
                                                       tc-opts))]
          (when tube-client
            (au/<? (<do-schema-negotiation rcv-chan tube-client client-fp
                                           client-pcf *server-fp *server-pcf))
            (when-let [subject-id @*subject-id]
              (when-let [credential @*credential]
                (log-in* tube-client msg-union-schema subject-id credential)))
            (reset! *rcv-chan rcv-chan)
            (reset! *tube-client tube-client)
            (ca/<! reconnect-ch))
          (ca/<! (ca/timeout (rand-int max-reconnect-wait-ms))))
        (catch #?(:clj Exception :cljs js/Error) e
          (lu/log-exception e)
          ;; Rate limit
          (ca/<! (ca/timeout 1000)))))))

(defn <do-rpc [msg-union-schema rpc *tube-client *rpc-id->rpc *shutdown?]
  (let [{:keys [failure-time-ms cb]} rpc]
    (au/go
      (loop []
        (when (not @*shutdown?)
          (if-let [tube-client @*tube-client]
            (send-rpc* tube-client msg-union-schema rpc *rpc-id->rpc)
            (if (> (u/get-current-time-ms) failure-time-ms)
              (call-callback-w-error cb "This operation timed out.")
              (do
                (ca/<! (ca/timeout 100))
                (recur)))))))))

(defn start-rpc-send-loop
  [rpc-chan msg-union-schema max-rpcs-per-second rpc-burst-seconds *tube-client
   *rpc-id->rpc *shutdown?]
  (let [credit-ch (ca/chan (ca/dropping-buffer (* max-rpcs-per-second
                                                  rpc-burst-seconds)))
        credit-ch-delay-ms (/ 1000 max-rpcs-per-second)]
    (ca/go
      (while (not @*shutdown?)
        (ca/>! credit-ch :one-credit)
        (ca/<! (ca/timeout credit-ch-delay-ms))))
    (ca/go
      (while (not @*shutdown?)
        (try
          (let [[rpc ch] (ca/alts! [rpc-chan (ca/timeout 100)])]
            (when (= rpc-chan ch)
              (ca/<! credit-ch) ;; Consume a credit
              (au/<? (<do-rpc msg-union-schema rpc *tube-client
                              *rpc-id->rpc *shutdown?))))
          (catch #?(:clj Exception :cljs js/Error) e
            (errorf "Unexpected error in rpc-loop: %s"
                    (lu/get-exception-msg-and-stacktrace e))))))))

(defn retry-rpc [rpc now rpc-chan max-rpc-timeout-ms *rpc-id]
  (let [{:keys [timeout-ms cb]} rpc
        rpc-id (get-rpc-id* *rpc-id)
        new-rpc (assoc rpc
                       :rpc-id rpc-id
                       :retry-time-ms (+ now timeout-ms))]
    (enqueue-rpc rpc-chan new-rpc max-rpc-timeout-ms cb)))

(defn start-rpc-retry-loop
  [rpc-chan max-rpc-timeout-ms *rpc-id->rpc *rpc-id *shutdown?]
  (ca/go
    (while (not @*shutdown?)
      (try
        (doseq [[rpc-id rpc] @*rpc-id->rpc]
          (let [{:keys [retry-time-ms failure-time-ms rpc-id cb]} rpc
                now (u/get-current-time-ms)]
            (cond
              (> now failure-time-ms)
              (do
                (swap! *rpc-id->rpc dissoc rpc-id)
                (call-callback-w-error cb "This operation timed out."))

              (> now retry-time-ms)
              (do
                (swap! *rpc-id->rpc dissoc rpc-id)
                (retry-rpc rpc now rpc-chan max-rpc-timeout-ms *rpc-id)))))
        (ca/<! (ca/timeout 100))
        (catch #?(:clj Exception :cljs js/Error) e
          (lu/log-exception e))))))

(defn handle-rpc-success-rsp [msg *rpc-id->rpc]
  (let [{:keys [rpc-id ret]} msg
        {:keys [cb]} (@*rpc-id->rpc rpc-id)]
    (swap! *rpc-id->rpc dissoc rpc-id)
    (when cb
      (call-callback-w-result cb ret))))

(defn handle-rpc-failure-rsp [msg *rpc-id->rpc]
  (let [{:keys [rpc-id rpc-name rpc-arg failure-type error-str]} msg
        {:keys [cb]} (@*rpc-id->rpc rpc-id)
        error-msg (str "RPC failed.\n  RPC id: " rpc-id "\n  RPC name: "
                       rpc-name "\n  Argument: "
                       rpc-arg "\n  Fail type: " (name failure-type)
                       "\n  Error msg: " error-str)]
    (swap! *rpc-id->rpc dissoc rpc-id)
    (when-not **silence-log**
      (errorf "%s" error-msg))
    (when cb
      (call-callback-w-error cb error-msg))))

(defn handle-login-rsp [msg *login-cb]
  (let [{:keys [was-successful]} msg]
    (when-not **silence-log**
      (if was-successful
        (infof "Login succeeded.")
        (infof "Login failed.")))
    (call-callback-w-result @*login-cb was-successful)
    (reset! *login-cb nil)))

(defn handle-logout-rsp [msg *logout-cb]
  (let [{:keys [was-successful]} msg]
    (when-not **silence-log**
      (if was-successful
        (infof "Logout succeeded.")
        (infof "Logout failed.")))
    (call-callback-w-result @*logout-cb was-successful)
    (reset! *logout-cb nil)))

(defn deserialize* [msg-union-schema pcf data]
  (l/deserialize msg-union-schema
                 (l/get-parsing-canonical-form msg-union-schema)
                 data))

(defn start-rcv-loop
  [*rcv-chan msg-union-schema *server-schema-pcf *shutdown?
   *msg-record-name->handler]
  (ca/go
    (while (not @*shutdown?)
      (try
        (if-let [rcv-chan @*rcv-chan]
          (let [[data ch] (ca/alts! [rcv-chan (ca/timeout 100)])]
            (when (= rcv-chan ch)
              (let [[msg-name msg] (l/deserialize msg-union-schema
                                                  @*server-schema-pcf data)
                    handler (@*msg-record-name->handler msg-name)]
                (if handler
                  (handler msg)
                  (infof "No handler found for msg name: %s" msg-name)))))
          ;; If no rcv-chan, wait for conn
          (ca/<! (ca/timeout 100)))
        (catch #?(:clj Exception :cljs js/Error) e
          (lu/log-exception e))))))

;; TODO: Measure timings and set these appropriately
(def default-client-options
  {:max-rpcs-per-second 10
   :max-rpc-timeout-ms 10000
   :rpc-burst-seconds 5
   :default-rpc-timeout-ms 3000
   :max-total-rpc-time-ms 10000
   :connect-timeout-ms 5000
   :max-reconnect-wait-ms 2000})

(defn ceil [n]
  #?(:clj (int (Math/ceil n))
     :cljs (.ceil js/Math n)))

(defn make-msg-record-name->handler
  [rpc-name-kws *login-cb *logout-cb *rpc-id->rpc]
  (let [base {::u/login-rsp #(handle-login-rsp % *login-cb)
              ::u/logout-rsp #(handle-logout-rsp % *logout-cb)
              ::u/rpc-failure-rsp #(handle-rpc-failure-rsp % *rpc-id->rpc)}]
    (reduce (fn [acc rpc-name]
              (let [rec-name (u/make-msg-record-name :rpc-success-rsp rpc-name)]
                (assoc acc rec-name #(handle-rpc-success-rsp % *rpc-id->rpc))))
            base rpc-name-kws)))

(s/defn make-client :- (s/protocol ICapsuleClient)
  ([api :- u/API
    <get-uris :- u/GetURIsFn]
   (make-client api <get-uris {}))
  ([api :- u/API
    <get-uris :- u/GetURIsFn
    opts :- u/ClientOptions]
   (let [opts (merge default-client-options opts)
         {:keys [max-rpcs-per-second max-rpc-timeout-ms
                 rpc-burst-seconds default-rpc-timeout-ms max-total-rpc-time-ms
                 connect-timeout-ms max-reconnect-wait-ms]} opts
         rpc-name-kws (into #{} (keys (:rpcs api)))
         event-name-kws (into #{} (keys (:events api)))
         rpc-chan (ca/chan (ceil (* max-rpcs-per-second rpc-burst-seconds
                                    (/ max-rpc-timeout-ms 1000))))
         msg-union-schema (u/make-msg-union-schema api)
         client-pcf (l/get-parsing-canonical-form msg-union-schema)
         client-fp (u/long->byte-array (l/get-fingerprint64 msg-union-schema))
         *rpc-id (atom 0)
         *rcv-chan (atom nil)
         *tube-client (atom nil)
         *server-pcf (atom nil)
         *server-fp (atom nil)
         *event-name-kw->handler (atom {})
         *login-cb (atom nil)
         *subject-id (atom nil)
         *credential (atom nil)
         *logout-cb (atom nil)
         *shutdown? (atom false)
         *rpc-id->rpc (atom {})
         *msg-record-name->handler (atom
                                    (make-msg-record-name->handler
                                     rpc-name-kws *login-cb *logout-cb
                                     *rpc-id->rpc))
         client (->CapsuleClient
                 msg-union-schema rpc-name-kws event-name-kws rpc-chan
                 max-rpc-timeout-ms default-rpc-timeout-ms max-total-rpc-time-ms
                 *rpc-id *tube-client *login-cb *subject-id *credential
                 *logout-cb *msg-record-name->handler *shutdown? *rpc-id->rpc)]
     (start-connection-loop <get-uris msg-union-schema client-fp client-pcf
                            *server-fp *server-pcf *subject-id *credential
                            *rcv-chan *tube-client *shutdown?)
     (start-rpc-retry-loop rpc-chan max-rpc-timeout-ms *rpc-id->rpc *rpc-id
                           *shutdown?)
     (start-rcv-loop *rcv-chan msg-union-schema *server-pcf *shutdown?
                     *msg-record-name->handler)
     (start-rpc-send-loop rpc-chan msg-union-schema max-rpcs-per-second
                          rpc-burst-seconds *tube-client *rpc-id->rpc
                          *shutdown?)
     client)))
