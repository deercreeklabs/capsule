(ns deercreeklabs.capsule.utils
  (:require
   [clojure.core.async :as ca]
   [clojure.set :refer [subset?]]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   [deercreeklabs.tube.connection :as tc]
   #?(:clj [puget.printer :refer [cprint]])
   [schema.core :as s]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]])
  #?(:cljs
     (:require-macros
      [deercreeklabs.capsule.utils :refer [sym-map]])))

#?(:cljs
   (set! *warn-on-infer* true))

(def Nil (s/eq nil))
(def AvroSchema (s/protocol deercreeklabs.lancaster.utils/IAvroSchema))
(def RpcOrMsgName s/Keyword)
(def Role s/Keyword)
(def Path s/Str)
(def SubjectId s/Str)
(def Credential s/Str)
(def ConnId s/Int)
(def RpcId s/Int)
(def MsgMetadata
  {(s/required-key :conn-id) ConnId
   (s/required-key :sender) s/Any
   (s/required-key :subject-id) SubjectId
   (s/required-key :msg-name) RpcOrMsgName
   (s/required-key :encoded-msg) ba/ByteArray
   (s/required-key :writer-pcf) s/Str
   (s/required-key :msgs-union-schema) AvroSchema
   (s/optional-key :rpc-id) RpcId
   (s/optional-key :timeout-ms) s/Int})
(def Handler (s/=> s/Any s/Any MsgMetadata))
(def HandlerMap {RpcOrMsgName Handler})
(def Authenticator (s/=> au/Channel SubjectId Credential))
(def TubeConn (s/protocol tc/IConnection))
(def GetURLFn (s/=> au/Channel))
(def GetCredentialsFn (s/=> au/Channel))
(def MsgDefs {RpcOrMsgName AvroSchema})
(def RpcDefs {RpcOrMsgName {:arg AvroSchema
                            :ret AvroSchema}})
(def RoleDef
  {(s/optional-key :rpcs) RpcDefs
   (s/optional-key :msgs) MsgDefs})
(def Protocol{Role RoleDef})
(def Callback (s/=> s/Any s/Any))
(def ClientOptions
  {(s/optional-key :default-rpc-timeout-ms) s/Int
   (s/optional-key :rcv-queue-size) s/Int
   (s/optional-key :send-queue-size) s/Int
   (s/optional-key :silence-log?) s/Bool
   (s/optional-key :<on-reconnect) Callback
   (s/optional-key :rpc-handlers) HandlerMap
   (s/optional-key :msg-handlers) HandlerMap})
(def EndpointOptions
  {(s/optional-key :default-rpc-timeout-ms) s/Int
   (s/optional-key :silence-log?) s/Bool
   (s/optional-key :rpc-handlers) HandlerMap
   (s/optional-key :msg-handlers) HandlerMap})

(def RolesToRpcs
  {Role #{RpcOrMsgName}})

(defmacro sym-map
  "Builds a map from symbols.
   Symbol names are turned into keywords and become the map's keys.
   Symbol values become the map's values.
  (let [a 1
        b 2]
    (sym-map a b))  =>  {:a 1 :b 2}"
  [& syms]
  (zipmap (map keyword syms) syms))

(defn make-msg-record-name [msg-type msg-name-kw]
  (let [msg-ns (namespace msg-name-kw)
        msg-name (name msg-name-kw)]
    (keyword msg-ns (str msg-name "-" (name msg-type)))))

(def rpc-id-schema l/int-schema)

(def fp-schema l/long-schema)

(def null-or-string-schema
  (l/make-union-schema [l/null-schema l/string-schema]))

(def null-or-fp-schema
  (l/make-union-schema [l/null-schema fp-schema]))

(l/def-enum-schema match-schema
  :both :client :none)

(l/def-record-schema handshake-req-schema
  [:client-fp fp-schema]
  [:client-pcf null-or-string-schema]
  [:server-fp fp-schema])

(l/def-record-schema handshake-rsp-schema
  [:match match-schema]
  [:server-fp null-or-fp-schema]
  [:server-pcf null-or-string-schema])

(l/def-record-schema login-req-schema
  [:subject-id l/string-schema]
  [:credential l/string-schema])

(l/def-record-schema login-rsp-schema
  [:was-successful l/boolean-schema])

(l/def-record-schema rpc-failure-rsp-schema
  [:rpc-id rpc-id-schema]
  [:error-str l/string-schema])

(defn make-rpc-req-schema [[rpc-name rpc-def]]
  (let [rec-name (make-msg-record-name :rpc-req rpc-name)
        arg-schema (:arg rpc-def)]
    (l/make-record-schema rec-name
                          [[:rpc-id rpc-id-schema]
                           [:timeout-ms l/int-schema]
                           [:arg arg-schema]])))

(defn make-rpc-success-rsp-schema [[rpc-name rpc-def]]
  (let [rec-name (make-msg-record-name :rpc-success-rsp rpc-name)]
    (l/make-record-schema rec-name
                          [[:rpc-id rpc-id-schema]
                           [:ret (:ret rpc-def)]])))

(defn make-msg-schema [[msg-name msg-def]]
  (let [rec-name (make-msg-record-name :msg msg-name)]
    (l/make-record-schema rec-name
                          [[:arg msg-def]])))

(defn make-name-maps [protocol role]
  (let [{:keys [rpcs msgs]} (protocol role)
        rpc-name->req-name (reduce (fn [acc [msg-name msg-def]]
                                     (assoc acc msg-name
                                            (make-msg-record-name
                                             :rpc-req msg-name)))
                                   {} rpcs)
        rpc-name->rsp-name (reduce (fn [acc [msg-name msg-def]]
                                     (assoc acc msg-name
                                            (make-msg-record-name
                                             :rpc-success-rsp msg-name)))
                                   {} rpcs)
        msg-name->rec-name (reduce (fn [acc [msg-name msg-def]]
                                     (assoc acc msg-name
                                            (make-msg-record-name
                                             :msg msg-name)))
                                   {} msgs)]
    (sym-map rpc-name->req-name rpc-name->rsp-name msg-name->rec-name)))

(defn make-role-schemas [protocol role]
  (let [{:keys [rpcs msgs]} (protocol role)
        rpc-req-schemas (map make-rpc-req-schema rpcs)
        rpc-success-rsp-schemas (map make-rpc-success-rsp-schema rpcs)
        msg-schemas (map make-msg-schema msgs)]
    (concat rpc-req-schemas rpc-success-rsp-schemas msg-schemas)))

(s/defn make-msgs-union-schema :- AvroSchema
  [protocol :- Protocol]
  (let [role-schemas (mapcat (partial make-role-schemas protocol)
                             (keys protocol))]
    (l/make-union-schema (concat [login-req-schema login-rsp-schema
                                  rpc-failure-rsp-schema]
                                 role-schemas))))

(defn get-rpc-id* [*rpc-id]
  (swap! *rpc-id (fn [rpc-id]
                   (let [new-rpc-id (inc rpc-id)]
                     (if (= new-rpc-id 2147483647)
                       0
                       new-rpc-id)))))

(defn configure-logging []
  (timbre/merge-config!
   {:level :debug
    :output-fn lu/short-log-output-fn
    :appenders
    {:println {:ns-blacklist ["org.eclipse.jetty.*"]}}}))

(s/defn get-current-time-ms :- s/Num
  []
  #?(:clj (System/currentTimeMillis)
     :cljs (.getTime (js/Date.))))

(defn check-role [role-name role-def]
  (let [{:keys [rpcs msgs]} role-def]
    (doseq [[rpc-name rpc-def] rpcs]
      (let [{:keys [arg ret]} rpc-def]
        (when-not (keyword? rpc-name)
          (throw (ex-info "RPC names must be keywords."
                          (sym-map rpc-name rpc-def role-name role-def))))
        (when-not arg
          (throw (ex-info "RPC definition must include an :arg key."
                          (sym-map rpc-name rpc-def role-name role-def))))
        (when-not (satisfies? deercreeklabs.lancaster.utils/IAvroSchema arg)
          (throw (ex-info "RPC :arg value must be an AvroSchema object."
                          (sym-map rpc-name rpc-def role-name role-def))))
        (when-not ret
          (throw (ex-info "RPC definition must include a :ret key."
                          (sym-map rpc-name rpc-def role-name role-def))))
        (when-not (satisfies? deercreeklabs.lancaster.utils/IAvroSchema ret)
          (throw (ex-info "RPC :ret value must be an AvroSchema object."
                          (sym-map rpc-name rpc-def role-name role-def))))))
    (doseq [[msg-name msg-schema] msgs]
      (when-not (keyword? msg-name)
        (throw (ex-info "Msg names must be keywords."
                        (sym-map msg-name msg-schema))))
      (when-not (satisfies? deercreeklabs.lancaster.utils/IAvroSchema
                            msg-schema)
        (throw (ex-info "Msg values must be AvroSchema objects."
                        (sym-map msg-name msg-schema role-name role-def)))))))

(defn valid-protocol? [protocol]
  (when-not (= 2 (count (keys protocol)))
    (ex-info (str "The protocol must have exactly two keys, which are "
                  "role name keywords.")
             (sym-map protocol)))
  (doseq [[role-name role-def] protocol]
    (check-role role-name role-def))
  true)

(defn handle-rcv
  [rcvr-type conn-id sender subject-id encoded-msg msgs-union-schema writer-pcf
   *msg-record-name->handler]
  (let [[msg-name msg] (l/deserialize msgs-union-schema writer-pcf encoded-msg)
        handler (@*msg-record-name->handler msg-name)
        metadata (sym-map conn-id sender subject-id encoded-msg writer-pcf
                          msgs-union-schema msg-name)]
    (when (not handler)
      (let [data (ba/byte-array->debug-str encoded-msg)]
        (throw (ex-info (str "No handler is defined for " msg-name)
                        (sym-map rcvr-type msg-name msg handler encoded-msg)))))
    (try
      (handler msg metadata)
      (catch #?(:clj Exception :cljs js/Error) e
        (errorf "Error in handler for %s. %s" msg-name
                (lu/get-exception-msg-and-stacktrace e))))))

(defn make-handle-rpc-success-rsp [*rpc-id->rpc-info]
  (fn handle-rpc-success-rsp [msg metadata]
    (let [{:keys [rpc-id ret]} msg
          {:keys [success-cb]} (@*rpc-id->rpc-info rpc-id)]
      (swap! *rpc-id->rpc-info dissoc rpc-id)
      (when success-cb
        (success-cb ret)))))

(defn make-handle-rpc-failure-rsp [*rpc-id->rpc-info silence-log?]
  (fn handle-rpc-failure-rsp [msg metadata]
    (let [{:keys [rpc-id error-str]} msg
          {:keys [rpc-name-kw arg failure-cb]} (@*rpc-id->rpc-info rpc-id)
          error-msg (str "RPC failed.\n  RPC id: " rpc-id "\n  RPC name: "
                         rpc-name-kw "\n  Argument: "
                         arg "\n  Error msg: " error-str)]
      (swap! *rpc-id->rpc-info dissoc rpc-id)
      (when-not silence-log?
        (errorf "%s" error-msg))
      (when failure-cb
        (failure-cb (ex-info error-msg msg))))))

(defn make-rpc-req-handler [rpc-name rpc-rsp-name handler]
  (s/fn handle-rpc :- Nil
    [msg :- s/Any
     metadata :- MsgMetadata]
    (let [{:keys [rpc-id arg]} msg
          {:keys [sender]} metadata]
      (try
        (let [handler-ret (handler arg metadata)
              send-ret (fn [ret]
                         (let [rsp (sym-map rpc-id ret)]
                           (sender rpc-rsp-name rsp)))]
          (if-not (au/channel? handler-ret)
            (send-ret handler-ret)
            (ca/take! handler-ret send-ret)))
        (catch #?(:clj Exception :cljs js/Error) e
          (errorf "Error in handle-rpc for %s: %s" rpc-name
                  (lu/get-exception-msg-and-stacktrace e))
          (let [error-str (lu/get-exception-msg-and-stacktrace e)]
            (sender ::rpc-failure-rsp (sym-map rpc-id error-str))))))))

(defn make-msg-rec-name->handler
  [my-name-maps peer-name-maps *rpc-id->rpc-info silence-log?]
  (let [m {::rpc-failure-rsp (make-handle-rpc-failure-rsp
                              *rpc-id->rpc-info silence-log?)}]
    (reduce-kv (fn [acc rpc-name rsp-name]
                 (assoc acc rsp-name
                        (make-handle-rpc-success-rsp *rpc-id->rpc-info)))
               m (:rpc-name->rsp-name my-name-maps))))

(defn set-rpc-handler
  [rpc-name-kw handler peer-role peer-name-maps *msg-rec-name->handler]
  (let [req-name ((:rpc-name->req-name peer-name-maps) rpc-name-kw)
        _ (when-not req-name
            (throw
             (ex-info (str "Protocol violation. Peer role `" peer-role
                           "` does not have an RPC named `"
                           rpc-name-kw "`.")
                      (sym-map peer-role rpc-name-kw))))
        rsp-name ((:rpc-name->rsp-name peer-name-maps) rpc-name-kw)
        rpc-handler (make-rpc-req-handler rpc-name-kw rsp-name handler)]
    (swap! *msg-rec-name->handler assoc req-name rpc-handler)))

(defn set-msg-handler
  [msg-name-kw handler peer-role peer-name-maps *msg-rec-name->handler]
  (let [rec-name ((:msg-name->rec-name peer-name-maps) msg-name-kw)]
    (when-not rec-name
      (throw
       (ex-info (str "Protocol violation. Peer role `" peer-role
                     "` does not have a msg named `"
                     msg-name-kw "`.")
                (sym-map peer-role msg-name-kw))))
    (swap! *msg-rec-name->handler assoc rec-name
           (fn [msg metadata]
             (handler (:arg msg) metadata)))))

(defn get-peer-role [protocol role]
  (-> (keys protocol)
      (set)
      (disj role)
      (first)))

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
                        rpc-info)))))
        (ca/<! (ca/timeout 1000)))
      (catch #?(:clj Exception :cljs js/Error) e
        (errorf "Unexpected error in gc loop: %s"
                (lu/get-exception-msg-and-stacktrace e))))))
