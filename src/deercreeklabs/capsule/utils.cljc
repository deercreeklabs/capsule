(ns deercreeklabs.capsule.utils
  (:require
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.capsule.api :as capi]
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

(defn send-msg [tube-conn api msg-type msg-name msg-id msg]
  (tc/send tube-conn (capi/encode api (sym-map msg-type msg-name msg-id msg))))

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
