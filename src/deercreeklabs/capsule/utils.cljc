(ns deercreeklabs.capsule.utils
  (:require
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   [deercreeklabs.tube.connection :as tc]
   #?(:clj [puget.printer :refer [cprint]])
   [schema.core :as s]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]]))

#?(:cljs
   (set! *warn-on-infer* true))

(def Nil (s/eq nil))
(def AvroSchema (s/protocol deercreeklabs.lancaster.utils/IAvroSchema))
(def RpcOrEventName s/Keyword)
(def Path s/Str)
(def SubjectId s/Num)
(def EventDef {RpcOrEventName AvroSchema})
(def Msg s/Any)
(def MsgMetadata
  {:subject-id SubjectId
   :msg-id ba/ByteArray})
(def Handler (s/=> s/Any Msg MsgMetadata))
(def Identifier s/Str)
(def Credential s/Str)
(def Authenticator (s/=> au/Channel SubjectId Credential))
(def TubeConn (s/protocol tc/IConnection))
(def GetURIsFn (s/=> au/Channel))
(def RpcDef
  {RpcOrEventName {(s/required-key :arg-schema) AvroSchema
                   (s/optional-key :ret-schema) AvroSchema
                   (s/optional-key :public?) s/Bool}})
(def ApiInfo
  {(s/optional-key :rpcs) RpcDef
   (s/optional-key :events) EventDef})
(def HandlerMap {(s/optional-key :rpcs) {RpcOrEventName Handler}
                 (s/optional-key :events) {RpcOrEventName Handler}})
(def EndpointOptions
  {(s/optional-key :path) Path
   (s/optional-key :<authenticator) Authenticator})
(def ClientOptions
  {(s/optional-key :subject-id) s/Str
   (s/optional-key :credential) s/Str
   (s/optional-key :default-rpc-timeout-ms) s/Int
   (s/optional-key :max-rpc-timeout-ms) s/Int
   (s/optional-key :connect-timeout-ms) s/Int
   (s/optional-key :max-reconnect-wait-ms) s/Int})
(def RpcCallback (s/=> s/Any s/Any))
(def RPC
  {:rpc-name s/Str
   :arg s/Any
   :rpc-id ba/ByteArray
   :rpc-id-str s/Str
   :cb RpcCallback
   :timeout-ms s/Num
   :retry-time-ms s/Num
   :failure-time-ms s/Num})

(defmacro sym-map
  "Builds a map from symbols.
   Symbol names are turned into keywords and become the map's keys.
   Symbol values become the map's values.
  (let [a 1
        b 2]
    (sym-map a b))  =>  {:a 1 :b 2}"
  [& syms]
  (zipmap (map keyword syms) syms))

(defn make-msg-id []
  (ba/byte-array (take 16 (repeatedly #(rand-int 256)))))

(defn make-dispatch-name [msg-name msg-type]
  (clojure.string/join  "_" ["capsule" (name msg-name) (name msg-type)]))

(defn make-dispatch-sym [msg-name msg-type]
  (symbol (str (make-dispatch-name msg-name msg-type) "-schema")))

(defn dispatch-name->msg-name-and-type [dispatch-name]
  (let [[capsule msg-name msg-type] (clojure.string/split
                                     (name dispatch-name) #"_")]
    [msg-name msg-type]))

(defn encode-msg* [msg-schema msg-info]
  (let [{:keys [msg-type msg-name msg-id msg]} msg-info
        dispatch-kw (keyword (make-dispatch-name msg-name msg-type))]
    (l/serialize msg-schema {dispatch-kw {:msg-id msg-id
                                          :content msg}})))

(defn decode-msg* [msg-schema writer-msg-schema ba]
  (let [[dispatch-name msg] (first (l/deserialize msg-schema writer-msg-schema
                                                  ba))
        [msg-name msg-type] (dispatch-name->msg-name-and-type dispatch-name)]
    {:msg-type (keyword msg-type)
     :msg-name (keyword msg-name)
     :msg-id (:msg-id msg)
     :msg (:content msg)}))

(defprotocol IAPI
  (encode [this msg-info])
  (decode [this writer-msg-schema ba])
  (get-msg-schema [this]))

(defrecord API [msg-schema]
  IAPI
  (encode [this msg-info]
    (encode-msg* msg-schema msg-info))

  (decode [this writer-msg-schema ba]
    (decode-msg* msg-schema writer-msg-schema ba))

  (get-msg-schema [this]
    msg-schema))

(l/def-fixed-schema msg-id-schema 16)

(l/def-record-schema rpc-failure-info-schema
  [:rpc-name l/string-schema]
  [:rpc-id-str l/string-schema]
  [:rpc-arg l/string-schema]
  [:error-str l/string-schema])

(defn send-msg [tube-conn api msg-type msg-name msg-id msg]
  (tc/send tube-conn (encode api (sym-map msg-type msg-name msg-id msg))))

(defn make-rpc-req-schema [[rpc-name rpc-info]]
  (let [rec-sym (make-dispatch-sym rpc-name :rpc-req)]
    `(l/def-record-schema ~rec-sym
       [:msg-id msg-id-schema]
       [:content ~(:arg-schema rpc-info)])))

(defn make-rpc-success-rsp-schema [[rpc-name rpc-info]]
  (let [rec-sym (make-dispatch-sym rpc-name :rpc-success-rsp)]
    `(l/def-record-schema ~rec-sym
       [:msg-id msg-id-schema]
       [:content ~(:ret-schema rpc-info)])))

(defn make-rpc-failure-rsp-schema [[rpc-name rpc-info]]
  (let [rec-sym (make-dispatch-sym rpc-name :rpc-failure-rsp)]
    `(l/def-record-schema ~rec-sym
       [:msg-id msg-id-schema]
       [:content rpc-failure-info-schema])))

(defn make-event-schema [[event-name event-schema]]
  (let [rec-sym (make-dispatch-sym event-name :event)]
    `(l/def-record-schema ~rec-sym
       [:msg-id msg-id-schema]
       [:content ~event-schema])))

(defn make-msg-schema [msg-schema-sym api-info]
  (let [{:keys [rpcs events]} api-info
        rpc-names (keys rpcs)
        event-names (keys events)
        login-req-sym (make-dispatch-sym :auth :login-req)
        login-rsp-sym (make-dispatch-sym :auth :login-rsp)
        rpc-req-syms (map #(make-dispatch-sym % :rpc-req) rpc-names)
        rpc-success-rsp-syms (map #(make-dispatch-sym % :rpc-success-rsp)
                                  rpc-names)
        rpc-failure-rsp-syms (map #(make-dispatch-sym % :rpc-failure-rsp)
                                  rpc-names)
        event-syms (map #(make-dispatch-sym % :event) event-names)
        msg-rec-dispatch-syms (concat [login-req-sym login-rsp-sym]
                                      rpc-req-syms rpc-success-rsp-syms
                                      rpc-failure-rsp-syms event-syms)]
    `(l/def-union-schema ~msg-schema-sym
       ~@msg-rec-dispatch-syms)))

(defmacro def-api [var-name api-info]
  (let [{:keys [rpcs events]} api-info
        login-req-sym (make-dispatch-sym :auth :login-req)
        login-rsp-sym (make-dispatch-sym :auth :login-rsp)
        rpc-req-schemas (map make-rpc-req-schema rpcs)
        rpc-success-rsp-schemas (map make-rpc-success-rsp-schema rpcs)
        rpc-failure-rsp-schemas (map make-rpc-failure-rsp-schema rpcs)
        event-schemas (map make-event-schema events)
        msg-schema-sym (gensym "msg-schema")
        msg-schema (make-msg-schema msg-schema-sym api-info)]
    `(do
       (l/def-record-schema ~login-req-sym
         [:subject-id :string]
         [:credential :string])
       (l/def-record-schema ~login-rsp-sym
         [:was-successful :boolean])
       ~@rpc-req-schemas
       ~@rpc-success-rsp-schemas
       ~@rpc-failure-rsp-schemas
       ~@event-schemas
       ~msg-schema
       (def ~var-name
         (->API ~msg-schema-sym)))))

(defn configure-logging []
  (timbre/merge-config!
   {:level :debug
    :output-fn lu/short-log-output-fn
    :appenders
    {:println {:ns-blacklist
               ["org.apache.*"]}}}))

(s/defn get-current-time-ms :- s/Num
  []
  #?(:clj (System/currentTimeMillis)
     :cljs (.getTime (js/Date.))))
