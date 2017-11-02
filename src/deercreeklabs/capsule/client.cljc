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
   [taoensso.timbre :as timbre :refer [debugf errorf infof]])
  #?(:cljs
     (:require-macros
      [cljs.core.async.macros :as ca])))

(def connect-timeout-ms 2000)
(def max-reconnect-wait-ms 2000)

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

(defn send-rpc* [tube-client api rpc *rpc-id-str->rpc]
  (let [{:keys [rpc-name-kw rpc-id rpc-id-str arg cb]} rpc]
    (if-not ((u/get-rpc-name-kws api) rpc-name-kw)
      (call-callback-w-error cb (str "RPC `" rpc-name-kw
                                     "` is not in the API."))
      (do
        (tc/send tube-client (u/encode api {:msg-type :rpc-req
                                            :msg-name rpc-name-kw
                                            :msg-id rpc-id
                                            :msg arg}))
        (swap! *rpc-id-str->rpc assoc rpc-id-str rpc)))))

(defn log-in* [tube-client api subject-id credential]
  (let [msg-id (u/make-msg-id)
        msg (u/sym-map subject-id credential)]
    (tc/send tube-client (u/encode api {:msg-type :login-req
                                        :msg-name :login-req
                                        :msg-id msg-id
                                        :msg msg}))))

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

(defrecord CapsuleClient [api rpc-chan max-rpc-timeout-ms default-rpc-timeout-ms
                          max-total-rpc-time-ms *tube-client *login-cb
                          *subject-id *credential *logout-cb
                          *event-name-kw->handler *shutdown? *rpc-id-str->rpc]
  ICapsuleClient
  (send-rpc [this rpc-name-kw arg]
    (send-rpc this rpc-name-kw arg nil default-rpc-timeout-ms))

  (send-rpc [this rpc-name-kw arg cb]
    (send-rpc this rpc-name-kw arg cb default-rpc-timeout-ms))

  (send-rpc [this rpc-name-kw arg cb timeout-ms]
    (if @*shutdown?
      (throw (ex-info "Client is shut down" {}))
      (let [timeout-ms (max timeout-ms max-rpc-timeout-ms)
            rpc-id (u/make-msg-id)
            rpc-id-str (ba/byte-array->b64 rpc-id)
            now (u/get-current-time-ms)
            retry-time-ms (+ now timeout-ms)
            failure-time-ms (+ now max-total-rpc-time-ms)
            rpc (u/sym-map rpc-name-kw arg rpc-id rpc-id-str cb timeout-ms
                           retry-time-ms failure-time-ms)]
        (enqueue-rpc rpc-chan rpc cb max-rpc-timeout-ms))))

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
      (log-in* tube-client api subject-id credential)))

  (log-out [this]
    (log-out this nil))

  (log-out [this cb]
    (reset! *logout-cb cb)
    (when-let [tube-client @*tube-client]
      (let [msg-id (u/make-msg-id)
            msg (u/sym-map msg-id)]
        (tc/send tube-client (u/encode api {:msg-type :logout-req
                                            :msg-name :logout-req
                                            :msg-id msg-id
                                            :msg nil})))))

  (<log-out [this]
    (let [ch (ca/chan)
          cb #(ca/put! ch %)]
      (log-out this cb)
      ch))

  (bind-event [this event-name-kw event-handler]
    (when-not ((u/get-event-name-kws api) event-name-kw)
      (let [error-str (str "Event `" event-name-kw "` is not in the API.")]
        (throw (ex-info error-str
                        {:type :illegal-argument
                         :subtype :unknown-event-name-kw
                         :error-str error-str
                         :event-name-kw event-name-kw}))))
    (swap! *event-name-kw->handler assoc event-name-kw event-handler))

  (shutdown [this]
    (reset! *shutdown? true)))

(defn send-schema-pcf [tube-client api]
  (->> api
       (u/get-msg-schema)
       (l/get-parsing-canonical-form)
       (l/serialize l/string-schema)
       (tc/send tube-client)))

(defn start-connection-loop
  [<get-uris api *subject-id *credential *rcv-chan *tube-client
   *server-schema-pcf *shutdown?]
  (ca/go
    (while (not @*shutdown?)
      (try
        (let [uris (au/<? (<get-uris))
              uri (rand-nth uris)
              reconnect-ch (ca/chan)
              rcv-chan (ca/chan)
              tc-opts {:on-disconnect (fn on-disconnect [code reason]
                                        (reset! *tube-client nil)
                                        (reset! *server-schema-pcf nil)
                                        (ca/put! reconnect-ch true))
                       :on-rcv (fn on-rcv [conn data]
                                 (ca/put! rcv-chan data))}
              tube-client (au/<? (tc/<make-tube-client uri connect-timeout-ms
                                                       tc-opts))]
          (when tube-client
            (send-schema-pcf tube-client api)
            (reset! *server-schema-pcf
                    (l/deserialize l/string-schema
                                   (l/get-parsing-canonical-form
                                    l/string-schema)
                                   (ca/<! rcv-chan)))
            (when-let [subject-id @*subject-id]
              (when-let [credential @*credential]
                (log-in* tube-client api subject-id credential)))
            (reset! *rcv-chan rcv-chan)
            (reset! *tube-client tube-client)
            (ca/<! reconnect-ch))
          (ca/<! (ca/timeout (rand-int max-reconnect-wait-ms))))
        (catch #?(:clj Exception :cljs js/Error) e
            (lu/log-exception e)
          ;; Rate limit
          (ca/<! (ca/timeout 1000)))))))

(defn <do-rpc [api rpc *tube-client *rpc-id-str->rpc *shutdown?]
  (let [{:keys [failure-time-ms cb]} rpc]
    (au/go
      (loop []
        (when (not @*shutdown?)
          (if-let [tube-client @*tube-client]
            (send-rpc* tube-client api rpc *rpc-id-str->rpc)
            (if (> (u/get-current-time-ms) failure-time-ms)
              (call-callback-w-error cb "This operation timed out.")
              (do
                (ca/<! (ca/timeout 100))
                (recur)))))))))

(defn start-rpc-send-loop
  [rpc-chan api max-rpcs-per-second rpc-burst-seconds *tube-client
   *rpc-id-str->rpc *shutdown?]
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
              (au/<? (<do-rpc api rpc *tube-client
                              *rpc-id-str->rpc *shutdown?))))
          (catch #?(:clj Exception :cljs js/Error) e
              (errorf "Unexpected error in rpc-loop: %s"
                      (lu/get-exception-msg-and-stacktrace e))))))))

(defn retry-rpc [rpc now rpc-chan max-rpc-timeout-ms]
  (let [{:keys [timeout-ms cb]} rpc
        rpc-id (u/make-msg-id)
        new-rpc (assoc rpc
                       :rpc-id rpc-id
                       :rpc-id-str (ba/byte-array->b64 rpc-id)
                       :retry-time-ms (+ now timeout-ms))]
    (enqueue-rpc rpc-chan new-rpc max-rpc-timeout-ms cb)))

(defn start-rpc-retry-loop
  [rpc-chan max-rpc-timeout-ms *rpc-id-str->rpc *shutdown?]
  (ca/go
    (while (not @*shutdown?)
      (try
        (doseq [[rpc-id-str rpc] @*rpc-id-str->rpc]
          (let [{:keys [retry-time-ms failure-time-ms rpc-id-str cb]} rpc
                now (u/get-current-time-ms)]
            (cond
              (> now failure-time-ms)
              (do
                (swap! *rpc-id-str->rpc dissoc rpc-id-str)
                (call-callback-w-error cb "This operation timed out."))

              (> now retry-time-ms)
              (do
                (swap! *rpc-id-str->rpc dissoc rpc-id-str)
                (retry-rpc rpc now rpc-chan max-rpc-timeout-ms)))))
        (ca/<! (ca/timeout 100))
        (catch #?(:clj Exception :cljs js/Error) e
            (lu/log-exception e))))))

(defn handle-rpc-success-rsp [msg-info *rpc-id-str->rpc]
  (let [{:keys [msg-id msg]} msg-info
        rpc-id-str (ba/byte-array->b64 msg-id)
        {:keys [cb]} (@*rpc-id-str->rpc rpc-id-str)]
    (swap! *rpc-id-str->rpc dissoc rpc-id-str)
    (when cb
      (call-callback-w-result cb msg))))

(defn handle-rpc-failure-rsp [msg-info *rpc-id-str->rpc]
  (let [{:keys [msg-id msg]} msg-info
        {:keys [rpc-id-str error-str]} msg
        {:keys [cb]} (@*rpc-id-str->rpc rpc-id-str)]
    (swap! *rpc-id-str->rpc dissoc rpc-id-str)
    (when cb
      (call-callback-w-error cb error-str))))

(defn handle-event [msg-info *event-name-kw->handler]
  (let [{:keys [msg-name msg]} msg-info
        handler (@*event-name-kw->handler msg-name)
        m @*event-name-kw->handler]
    (when handler
      (handler msg))))

(defn handle-login-rsp [msg-info *login-cb]
  (let [was-successful (:msg msg-info)]
    (if was-successful
      (infof "Login succeeded.")
      (infof "Login failed."))
    (call-callback-w-result @*login-cb was-successful)
    (reset! *login-cb nil)))

(defn handle-logout-rsp [msg-info *logout-cb]
  (let [was-successful (:msg msg-info)]
    (if was-successful
      (infof "Logout succeeded.")
      (infof "Logout failed."))
    (call-callback-w-result @*logout-cb was-successful)
    (reset! *logout-cb nil)))

(defn start-rcv-loop
  [*rcv-chan api *server-schema-pcf *login-cb *logout-cb *shutdown?
   *rpc-id-str->rpc *event-name-kw->handler]
  (ca/go
    (while (not @*shutdown?)
      (try
        (if-let [rcv-chan @*rcv-chan]
          (let [[data ch] (ca/alts! [rcv-chan (ca/timeout 100)])]
            (when (= rcv-chan ch)
              (let [msg-info (u/decode api @*server-schema-pcf data)]
                (case (:msg-type msg-info)
                  :login-rsp (handle-login-rsp msg-info *login-cb)
                  :logout-rsp (handle-logout-rsp msg-info *logout-cb)
                  :rpc-success-rsp (handle-rpc-success-rsp msg-info
                                                           *rpc-id-str->rpc)
                  :rpc-failure-rsp (handle-rpc-failure-rsp msg-info
                                                           *rpc-id-str->rpc)
                  :event (handle-event msg-info *event-name-kw->handler)))))
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

(s/defn make-client :- (s/protocol ICapsuleClient)
  ([api :- (s/protocol u/IAPI)
    <get-uris :- u/GetURIsFn]
   (make-client api <get-uris {}))
  ([api :- (s/protocol u/IAPI)
    <get-uris :- u/GetURIsFn
    opts :- u/ClientOptions]
   (let [opts (merge default-client-options opts)
         {:keys [max-rpcs-per-second max-rpc-timeout-ms
                 rpc-burst-seconds default-rpc-timeout-ms max-total-rpc-time-ms
                 connect-timeout-ms max-reconnect-wait-ms]} opts
         rpc-chan (ca/chan (ceil (* max-rpcs-per-second rpc-burst-seconds
                                    (/ max-rpc-timeout-ms 1000))))
         *rcv-chan (atom nil)
         *tube-client (atom nil)
         *server-schema-pcf (atom nil)
         *event-name-kw->handler (atom {})
         *login-cb (atom nil)
         *subject-id (atom nil)
         *credential (atom nil)
         *logout-cb (atom nil)
         *shutdown? (atom false)
         *rpc-id-str->rpc (atom {})
         client (->CapsuleClient
                 api rpc-chan max-rpc-timeout-ms default-rpc-timeout-ms
                 max-total-rpc-time-ms *tube-client *login-cb *subject-id
                 *credential *logout-cb *event-name-kw->handler *shutdown?
                 *rpc-id-str->rpc)]
     (start-connection-loop <get-uris api *subject-id *credential *rcv-chan
                            *tube-client *server-schema-pcf *shutdown?)
     (start-rpc-retry-loop rpc-chan max-rpc-timeout-ms *rpc-id-str->rpc
                           *shutdown?)
     (start-rcv-loop *rcv-chan api *server-schema-pcf *login-cb *logout-cb
                     *shutdown? *rpc-id-str->rpc *event-name-kw->handler)
     (start-rpc-send-loop rpc-chan api max-rpcs-per-second rpc-burst-seconds
                          *tube-client *rpc-id-str->rpc *shutdown?)
     client)))
