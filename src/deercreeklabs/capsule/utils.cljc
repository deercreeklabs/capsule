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
(def RpcName s/Keyword)
(def RpcOrEventName s/Keyword)
(def Role s/Keyword)
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
                   (s/required-key :ret-schema) AvroSchema}})
(def ApiInfo
  {(s/optional-key :rpcs) RpcDef
   (s/optional-key :events) EventDef})
(def HandlerMap {(s/optional-key :rpcs) {RpcOrEventName Handler}
                 (s/optional-key :events) {RpcOrEventName Handler}})
(def EndpointOptions
  {(s/optional-key :path) Path
   (s/optional-key :<authenticator) Authenticator})
(def ClientOptions
  {(s/optional-key :default-rpc-timeout-ms) s/Int
   (s/optional-key :max-rpc-timeout-ms) s/Int
   (s/optional-key :connect-timeout-ms) s/Int
   (s/optional-key :max-reconnect-wait-ms) s/Int})
(def RpcCallback (s/=> s/Any s/Any))
(def RpcInfo
  {:rpc-name-kw s/Keyword
   :arg s/Any
   :rpc-id ba/ByteArray
   :rpc-id-str s/Str
   :cb RpcCallback
   :timeout-ms s/Num
   :retry-time-ms s/Num
   :failure-time-ms s/Num})
(def RolesToRpcs
  {Role #{RpcName}})

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

(defn make-record-name [msg-type msg-name]
  (clojure.string/join  "_" ["capsule" (name msg-name) (name msg-type)]))

(defn make-record-name-sym [msg-type msg-name]
  (symbol (str (make-record-name msg-type msg-name) "-schema")))

(defn fq-record-name->msg-name-and-type [fq-record-name]
  (let [name-parts (clojure.string/split fq-record-name #"\.")
        record-ns (clojure.string/join "." (butlast name-parts))
        record-name (last name-parts)
        [capsule msg-name msg-type] (clojure.string/split
                                     (name record-name) #"_")]
    [msg-name msg-type]))

(defn encode-msg* [msg-schema msg-info]
  (let [{:keys [msg-type msg-name msg-id msg]} msg-info
        record-name (make-record-name msg-type msg-name)
        msg-edn-schema (l/get-edn-schema msg-schema)
        msg-ns (:namespace (first msg-edn-schema))
        fq-record-name (str msg-ns "." record-name)]
    (l/serialize msg-schema {fq-record-name {:msg-id msg-id
                                             :content msg}})))

(defn decode-msg* [msg-schema writer-msg-schema ba]
  (let [[fq-record-name msg] (first (l/deserialize msg-schema writer-msg-schema
                                                   ba))
        [msg-name msg-type] (fq-record-name->msg-name-and-type fq-record-name)]
    {:msg-type (keyword msg-type)
     :msg-name (keyword msg-name)
     :msg-id (:msg-id msg)
     :msg (:content msg)}))

(defprotocol IAPI
  (encode [this msg-info])
  (decode [this writer-msg-schema ba])
  (get-msg-schema [this])
  (get-rpc-name-kws [this])
  (get-event-name-kws [this]))

(defrecord API [msg-schema rpc-name-kws event-name-kws]
  IAPI
  (encode [this msg-info]
    (encode-msg* msg-schema msg-info))

  (decode [this writer-msg-schema ba]
    (decode-msg* msg-schema writer-msg-schema ba))

  (get-msg-schema [this]
    msg-schema)

  (get-rpc-name-kws [this]
    rpc-name-kws)

  (get-event-name-kws [this]
    event-name-kws))

(l/def-fixed-schema msg-id-schema 16)

(l/def-enum-schema rpc-failure-type-schema
  :unauthorized :server-exception :client-exception)

(l/def-record-schema rpc-failure-info-schema
  [:rpc-name l/string-schema]
  [:rpc-id-str l/string-schema]
  [:rpc-arg l/string-schema]
  [:failure-type rpc-failure-type-schema]
  [:error-str l/string-schema])

(l/def-record-schema login-info-schema
  [:subject-id :string]
  [:credential :string])

(defn send-msg [tube-conn api msg-type msg-name msg-id msg]
  (tc/send tube-conn (encode api (sym-map msg-type msg-name msg-id msg))))

(defn make-rpc-req-schema [[rpc-name rpc-info]]
  (let [rec-sym (make-record-name-sym :rpc-req rpc-name)]
    `(l/def-record-schema ~rec-sym
       [:msg-id msg-id-schema]
       [:content ~(:arg-schema rpc-info)])))

(defn make-rpc-success-rsp-schema [[rpc-name rpc-info]]
  (let [rec-sym (make-record-name-sym :rpc-success-rsp rpc-name)]
    `(l/def-record-schema ~rec-sym
       [:msg-id msg-id-schema]
       [:content ~(:ret-schema rpc-info)])))

(defn make-event-schema [[event-name event-schema]]
  (let [rec-sym (make-record-name-sym :event event-name)]
    `(l/def-record-schema ~rec-sym
       [:msg-id msg-id-schema]
       [:content ~event-schema])))

(defn make-msg-schema [msg-schema-sym api-info builtin-syms]
  (let [{:keys [rpcs events]} api-info
        rpc-names (keys rpcs)
        event-names (keys events)
        rpc-req-syms (map #(make-record-name-sym :rpc-req %) rpc-names)
        rpc-success-rsp-syms (map #(make-record-name-sym :rpc-success-rsp %)
                                  rpc-names)
        event-syms (map #(make-record-name-sym :event %) event-names)
        msg-rec-syms (concat builtin-syms rpc-req-syms rpc-success-rsp-syms
                             event-syms)]
    `(l/def-union-schema ~msg-schema-sym
       ~@msg-rec-syms)))

(defn configure-logging []
  (timbre/merge-config!
   {:level :debug
    :output-fn lu/short-log-output-fn
    :appenders
    {:println {:ns-blacklist
               ["org.apache.*" "org.eclipse.jetty.*"]}}}))

(s/defn get-current-time-ms :- s/Num
  []
  #?(:clj (System/currentTimeMillis)
     :cljs (.getTime (js/Date.))))
