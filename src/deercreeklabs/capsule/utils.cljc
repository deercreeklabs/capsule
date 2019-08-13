(ns deercreeklabs.capsule.utils
  (:require
   [clojure.core.async :as ca]
   #?(:cljs [clojure.pprint :as pprint])
   [clojure.set :refer [subset?]]
   [clojure.string :as str]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.capsule.logging :as logging :refer [debug debug-syms error]]
   [deercreeklabs.lancaster :as l]
   #?(:clj [puget.printer :refer [cprint-str]])
   [deercreeklabs.tube.connection :as tc]
   [schema.core :as s])
  #?(:clj
     (:import
      (clojure.lang ExceptionInfo))
     :cljs
     (:require-macros
      [deercreeklabs.capsule.utils :refer [sym-map]])))

#?(:cljs
   (set! *warn-on-infer* true))

(def Nil (s/eq nil))
(def LancasterSchema (s/pred l/schema?))
(def MsgName s/Keyword)
(def Role s/Keyword)
(def Path s/Str)
(def SubjectId s/Str)
(def SubjectSecret s/Str)
(def ConnId s/Int)
(def RpcId s/Int)
(def MsgMetadata
  {(s/required-key :conn-id) ConnId
   (s/required-key :sender) s/Any
   (s/required-key :subject-id) SubjectId
   (s/required-key :peer-id) s/Str
   (s/required-key :msg-name) MsgName
   (s/required-key :encoded-msg) ba/ByteArray
   (s/required-key :writer-pcf) s/Str
   (s/required-key :msgs-union-schema) LancasterSchema
   (s/optional-key :rpc-id) RpcId
   (s/optional-key :timeout-ms) s/Int})
(def Handler (s/=> s/Any s/Any MsgMetadata))
(def HandlerMap {MsgName Handler})
(def Authenticator (s/=> au/Channel SubjectId SubjectSecret))
(def GetURLFn (s/=> au/Channel))
(def GetCredentialsFn (s/=> au/Channel))
(def Protocol
  {:roles [(s/one Role "first-role") (s/one Role "second-role")]
   :msgs {MsgName {(s/required-key :arg) LancasterSchema
                   (s/optional-key :ret) LancasterSchema
                   (s/required-key :sender) Role}}})
(def CapsuleClient s/Any) ;; Avoid circular dependency w/ client.cljc

(def ClientOptions
  {(s/optional-key :default-rpc-timeout-ms) s/Int
   (s/optional-key :get-credentials-timeout-ms) s/Int
   (s/optional-key :get-url-timeout-ms) s/Int
   (s/optional-key :rcv-queue-size) s/Int
   (s/optional-key :send-queue-size) s/Int
   (s/optional-key :silence-log?) s/Bool
   (s/optional-key :on-connect) (s/=> s/Any CapsuleClient)
   (s/optional-key :on-disconnect) (s/=> s/Any CapsuleClient)
   (s/optional-key :handlers) HandlerMap
   (s/optional-key :<ws-client) (s/=> s/Any)})
(def EndpointConnectionInfo
  {(s/required-key :conn-id) ConnId
   (s/required-key :peer-id) s/Str})
(def EndpointOptions
  {(s/optional-key :default-rpc-timeout-ms) s/Int
   (s/optional-key :handlers) HandlerMap
   (s/optional-key :on-connect) (s/=> s/Any EndpointConnectionInfo)
   (s/optional-key :on-disconnect) (s/=> s/Any EndpointConnectionInfo)
   (s/optional-key :silence-log?) s/Bool})

(defmacro sym-map
  "Builds a map from symbols.
   Symbol names are turned into keywords and become the map's keys.
   Symbol values become the map's values.
  (let [a 1
        b 2]
    (sym-map a b))  =>  {:a 1 :b 2}"
  [& syms]
  (zipmap (map keyword syms) syms))

(defn pprint [x]
  #?(:clj (.write *out* (cprint-str x))
     :cljs (pprint/pprint x)))

(defn pprint-str [x]
  #?(:clj (cprint-str x)
     :cljs (with-out-str (pprint/pprint x))))

(defn configure-logging
  ([]
   (configure-logging :debug))
  ([level]
   (logging/add-log-reporter! :println logging/println-reporter)
   (logging/set-log-level! level)))

(s/defn get-current-time-ms :- s/Num
  []
  #?(:clj (System/currentTimeMillis)
     :cljs (.getTime (js/Date.))))

(def rpc-id-schema l/int-schema)

(def fp-schema l/long-schema)

(l/def-enum-schema match-schema
  :both :client :none)

(l/def-record-schema handshake-req-schema
  [:client-fp fp-schema]
  [:client-pcf (l/maybe l/string-schema)]
  [:server-fp fp-schema])

(l/def-record-schema handshake-rsp-schema
  [:match match-schema]
  [:server-fp (l/maybe fp-schema)]
  [:server-pcf (l/maybe l/string-schema)])

(l/def-record-schema login-req-schema
  [:subject-id l/string-schema]
  [:subject-secret l/string-schema])

(l/def-record-schema login-req-wrapper-schema
  [:login-req login-req-schema])

(l/def-record-schema login-rsp-schema
  [:was-successful l/boolean-schema])

(l/def-record-schema login-rsp-wrapper-schema
  [:login-rsp login-rsp-schema])

(l/def-record-schema rpc-failure-rsp-schema
  [:rpc-id rpc-id-schema]
  [:error-str l/string-schema])

(l/def-record-schema rpc-failure-rsp-wrapper-schema
  [:rpc-failure-rsp rpc-failure-rsp-schema])

(defn get-rpcs [protocol role]
  (filter (fn [[msg-name {:keys [ret sender]}]]
            (and ret
                 (#{role :either} sender)))
          (:msgs protocol)))

(defn get-msgs [protocol role]
  (filter (fn [[msg-name {:keys [ret sender]}]]
            (and (not ret)
                 (#{role :either} sender)))
          (:msgs protocol)))

(defn get-rpcs-name-set [protocol role]
  (->> (get-rpcs protocol role)
       (map first)
       (set)))

(defn get-msgs-name-set [protocol role]
  (->> (get-msgs protocol role)
       (map first)
       (set)))

(defn msg-record-name [msg-type msg-name-kw]
  (keyword (str (name msg-name-kw) "-" (name msg-type))))

(defn rpc-req-wrapper-schema [[rpc-name rpc-def]]
  (let [rec-name (msg-record-name :rpc-req rpc-name)
        wrapper-name (msg-record-name :rpc-req-wrapper rpc-name)
        arg-schema (:arg rpc-def)
        rpc-req-schema (l/record-schema rec-name
                                        [[:rpc-id rpc-id-schema]
                                         [:timeout-ms l/int-schema]
                                         [:arg arg-schema]])]
    (l/record-schema wrapper-name
                     [[rec-name rpc-req-schema]])))

(defn rpc-success-rsp-wrapper-schema [[rpc-name rpc-def]]
  (let [rec-name (msg-record-name :rpc-success-rsp rpc-name)
        wrapper-name (msg-record-name :rpc-success-rsp-wrapper rpc-name)
        rpc-success-rsp-schema (l/record-schema rec-name
                                                [[:rpc-id rpc-id-schema]
                                                 [:ret (:ret rpc-def)]])]
    (l/record-schema wrapper-name
                     [[rec-name rpc-success-rsp-schema]])))

(defn msg-wrapper-schema [[msg-name {:keys [arg]}]]
  (let [rec-name (msg-record-name :msg msg-name)
        wrapper-name (msg-record-name :msg-wrapper msg-name)
        msg-schema (l/record-schema rec-name
                                    [[:arg arg]])]
    (l/record-schema wrapper-name
                     [[rec-name msg-schema]])))

(defn role-schemas [protocol role]
  (let [rpcs (get-rpcs protocol role)
        msgs (get-msgs protocol role)
        rpc-req-wrapper-schemas (map rpc-req-wrapper-schema rpcs)
        rpc-success-rsp-wrapper-schemas (map rpc-success-rsp-wrapper-schema rpcs)
        msg-wrapper-schemas (map msg-wrapper-schema msgs)]
    (concat rpc-req-wrapper-schemas rpc-success-rsp-wrapper-schemas
            msg-wrapper-schemas)))

(defn dedupe-schemas [schemas]
  (-> (reduce (fn [acc schema]
                (let [{:keys [unique-schemas pcfs]} acc
                      pcf (l/pcf schema)]
                  (if (pcfs pcf)
                    acc
                    (-> acc
                        (update :unique-schemas conj schema)
                        (update :pcfs conj pcf)))))
              {:unique-schemas [] :pcfs #{}}
              schemas)
      (:unique-schemas)))

(s/defn msgs-union-schema :- LancasterSchema
  [protocol :- Protocol]
  (let [all-role-schemas (mapcat (partial role-schemas protocol)
                                 (:roles protocol))
        unique-role-schemas (dedupe-schemas all-role-schemas)
        schemas (concat unique-role-schemas
                        [login-req-wrapper-schema login-rsp-wrapper-schema
                         rpc-failure-rsp-wrapper-schema])]
    (l/union-schema schemas)))

(defn get-rpc-id* [*rpc-id]
  (swap! *rpc-id (fn [rpc-id]
                   (let [new-rpc-id (inc rpc-id)]
                     (if (= new-rpc-id 2147483647)
                       0
                       new-rpc-id)))))

(defn check-protocol [protocol]
  (let [{:keys [roles msgs]} protocol
        roles-set (set roles)
        roles-set-w-either (conj roles-set :either)]
    (when-not (sequential? roles)
      (throw (ex-info (str "The value of the :roles key must be a sequence of "
                           "exactly two keywords.")
                      (sym-map protocol))))
    (when-not (= 2 (count roles))
      (throw (ex-info (str "The value of the :roles key must be a sequence of "
                           "exactly two keywords.")
                      (sym-map protocol))))
    (when (roles-set :either)
      (throw (ex-info (str "The value of the :roles key must not contain "
                           " the keyword `:either`.")
                      (sym-map roles protocol))))
    (doseq [[msg-name msg-def] msgs]
      (let [{:keys [arg ret sender]} msg-def]
        (when-not (keyword? msg-name)
          (throw (ex-info (str "Msg names must be keywords. Got `"
                               msg-name "`.")
                          (sym-map msg-name msg-def))))
        (when (namespace msg-name)
          (throw (ex-info (str "Msg names must be simple (non-qualified) "
                               "keyords. Got `" msg-name "`.")
                          (sym-map msg-name msg-def))))
        (when-not (contains? msg-def :arg)
          (throw (ex-info "Msg definitions must include an :arg key."
                          (sym-map msg-name msg-def))))
        (when-not (l/schema? arg)
          (throw (ex-info "Msg :arg value must be a LancasterSchema object."
                          (sym-map msg-name msg-def))))
        (when (and ret
                   (not (l/schema? ret)))
          (throw (ex-info (str "If present, :ret value must be a "
                               "LancasterSchema object.")
                          (sym-map msg-name msg-def))))
        (when-not (roles-set-w-either sender)
          (throw (ex-info (str "Msg :sender value must be one of the two "
                               "specified roles or the keyword `:either`.")
                          (sym-map msg-name msg-def)))))))
  nil)

(defn rpc-msg-info
  [rpc-name-kw rpc-id timeout-ms arg success-cb failure-cb]
  (let [msg-rec-name (msg-record-name :rpc-req rpc-name-kw)
        failure-time-ms (+ (#?(:clj long :cljs identity) (get-current-time-ms))
                           (int timeout-ms))
        rpc-info (sym-map rpc-name-kw arg rpc-id success-cb
                          failure-cb timeout-ms failure-time-ms)
        msg {msg-rec-name (sym-map rpc-id timeout-ms arg)}
        msg-info (sym-map msg failure-time-ms failure-cb)]
    (sym-map msg-info rpc-info)))

(defn handle-rcv
  [rcvr-type conn-id sender subject-id peer-id encoded-msg
   msgs-union-schema writer-schema *msg-record-name->handler]
  (let [msg-rec (l/deserialize msgs-union-schema writer-schema encoded-msg)
        [msg-name msg] (first msg-rec)
        _ (when (and (not subject-id)
                     (not (= :login-req msg-name)))
            (throw (ex-info "Subject is not logged in."
                            (sym-map conn-id peer-id msg-name msg))))
        handler (@*msg-record-name->handler msg-name)
        writer-pcf (l/pcf writer-schema)
        metadata (sym-map conn-id sender subject-id peer-id encoded-msg
                          writer-pcf msgs-union-schema msg-name)]
    (when (not handler)
      (let [data (ba/byte-array->debug-str encoded-msg)]
        (throw (ex-info (str "No handler is defined for " msg-name)
                        (sym-map rcvr-type msg-name msg handler encoded-msg)))))
    (try
      (handler msg metadata)
      (catch #?(:clj Exception :cljs js/Error) e
        (error (str "Error in handler for " msg-name ". "
                    (logging/ex-msg-and-stacktrace e)))))))

(defn handle-rpc-success-rsp [*rpc-id->rpc-info]
  (fn handle-rpc-success-rsp [msg metadata]
    (let [{:keys [rpc-id ret]} msg
          {:keys [success-cb]} (@*rpc-id->rpc-info rpc-id)]
      (swap! *rpc-id->rpc-info dissoc rpc-id)
      (when success-cb
        (success-cb ret)))))

(defn handle-rpc-failure-rsp [*rpc-id->rpc-info silence-log?]
  (fn handle-rpc-failure-rsp [msg metadata]
    (let [{:keys [rpc-id error-str]} msg
          {:keys [rpc-name-kw arg failure-cb]} (@*rpc-id->rpc-info rpc-id)
          error-msg (str "RPC failed.\n  RPC id: " rpc-id "\n  RPC name: "
                         rpc-name-kw "\n  Argument: "
                         arg "\n  Error msg: " error-str)]
      (swap! *rpc-id->rpc-info dissoc rpc-id)
      (when-not silence-log?
        (error error-msg))
      (when failure-cb
        (failure-cb (ex-info error-msg msg))))))

(defn throw-cant-serialize [rsp rpc-name e]
  (let [{:keys [orig-e]} (ex-data e)
        orig-msg (logging/ex-msg orig-e)
        return-value (:ret rsp)]
    (throw (ex-info (str "Can't serialize return value for RPC `" rpc-name "`.")
                    (sym-map return-value rpc-name orig-e orig-msg)))))

(defn rpc-req-handler [rpc-name-kw rpc-rsp-name handler]
  (s/fn handle-rpc :- Nil
    [msg :- s/Any
     metadata :- MsgMetadata]
    (let [{:keys [rpc-id arg]} msg
          {:keys [sender]} metadata]
      (try
        (let [handler-ret (handler arg metadata)
              log-e #(error (str "Error in handle-rpc for `" rpc-name-kw
                                 "`. RPC arg: `" arg "`. Error: "
                                 (logging/ex-msg-and-stacktrace %)))
              send-ret (fn [ret]
                         (try
                           (if (instance? #?(:cljs js/Error :clj Throwable) ret)
                             (log-e ret)
                             (let [rsp (sym-map rpc-id ret)]
                               (try
                                 (sender {rpc-rsp-name rsp})
                                 (catch #?(:clj ExceptionInfo :cljs js/Error) e
                                   (if (= :cant-serialize-msg
                                          (:type (ex-data e)))
                                     (throw-cant-serialize rsp rpc-name-kw e)
                                     (throw e))))))
                           (catch #?(:clj Exception :cljs js/Error) e
                             (log-e e))))]
          (if-not (au/channel? handler-ret)
            (send-ret handler-ret)
            (ca/take! handler-ret send-ret)))
        (catch #?(:clj Exception :cljs js/Error) e
          (error (str "Error in handle-rpc for " rpc-name-kw ": "
                      (logging/ex-msg-and-stacktrace e)))
          (let [error-str (logging/ex-msg-and-stacktrace e)]
            (sender {:rpc-failure-rsp (sym-map rpc-id error-str)})))))))

(defn set-rpc-handler
  [rpc-name-kw handler *msg-rec-name->handler]
  (let [req-name (msg-record-name :rpc-req rpc-name-kw)
        rsp-name (msg-record-name :rpc-success-rsp rpc-name-kw)
        rpc-handler (rpc-req-handler rpc-name-kw rsp-name handler)]
    (swap! *msg-rec-name->handler assoc req-name rpc-handler)))

(defn set-msg-handler
  [msg-name-kw handler *msg-rec-name->handler]
  (let [rec-name (msg-record-name :msg msg-name-kw)]
    (swap! *msg-rec-name->handler assoc rec-name
           (fn [msg metadata]
             (handler (:arg msg) metadata)))))

(defn set-handler
  [msg-name-kw handler *msg-rec-name->handler peer-msgs peer-rpcs peer-role]
  (cond
    (peer-msgs msg-name-kw)
    (set-msg-handler msg-name-kw handler *msg-rec-name->handler)

    (peer-rpcs msg-name-kw)
    (set-rpc-handler msg-name-kw handler *msg-rec-name->handler)

    :else
    (throw
     (ex-info (str "Cannot set handler. Peer role `" peer-role
                   "` is not a sender for msg `" msg-name-kw "`.")
              (sym-map peer-role msg-name-kw)))))

(defn get-peer-role [protocol role]
  (-> (:roles protocol)
      (set)
      (disj role)
      (first)))

(defn make-rpc-rsp-handler-map
  [protocol role *rpc-id->rpc-info silence-log?]
  (reduce
   (fn [acc rpc]
     (-> acc
         (assoc (msg-record-name :rpc-success-rsp rpc)
                (handle-rpc-success-rsp *rpc-id->rpc-info))
         (assoc :rpc-failure-rsp
                (handle-rpc-failure-rsp *rpc-id->rpc-info silence-log?))))
   {} (keys (get-rpcs protocol role))))

(defn start-gc-loop [*shutdown? *rpc-id->rpc-info]
  (ca/go
    (try
      (while (not @*shutdown?)
        (doseq [[rpc-id rpc-info] @*rpc-id->rpc-info]
          (when (> (get-current-time-ms) (:failure-time-ms rpc-info))
            (swap! *rpc-id->rpc-info dissoc rpc-id)
            (when-let [failure-cb (:failure-cb rpc-info)]
              (failure-cb
               (ex-info (str "RPC timed out after " (:timeout-ms rpc-info)
                             " milliseconds.")
                        (dissoc rpc-info :success-cb :failure-cb))))))
        (ca/<! (ca/timeout 1000)))
      (catch #?(:clj Exception :cljs js/Error) e
        (error (str "Unexpected error in gc loop: "
                    (logging/ex-msg-and-stacktrace e)))))))
