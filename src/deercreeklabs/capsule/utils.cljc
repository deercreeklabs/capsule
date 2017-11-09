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

#?(:cljs (def Long js/Long))

(def Nil (s/eq nil))
(def AvroSchema (s/protocol deercreeklabs.lancaster.schemas/IAvroSchema))
(def RpcName s/Keyword)
(def RpcOrEventName s/Keyword)
(def Role s/Keyword)
(def Path s/Str)
(def SubjectId s/Int)
(def EventDef {RpcOrEventName AvroSchema})
(def Msg s/Any)
(def RpcId s/Int)
(def RPCMetadata
  {:subject-id SubjectId
   :roles #{Role}
   :rpc-id RpcId})
(def Handler (s/=> s/Any s/Any RPCMetadata))
(def Identifier s/Str)
(def Credential s/Str)
(def Authenticator (s/=> au/Channel SubjectId Credential))
(def TubeConn (s/protocol tc/IConnection))
(def GetURIsFn (s/=> au/Channel))
(def RpcDef
  {RpcOrEventName {(s/required-key :arg-schema) AvroSchema
                   (s/required-key :ret-schema) AvroSchema}})
(def API
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
  {:rpc-req-msg-record-name s/Keyword
   :rpc-rsp-msg-record-name s/Keyword
   :arg s/Any
   :rpc-id RpcId
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

(defn make-msg-record-name [msg-type msg-name-kw]
  (let [msg-ns (namespace msg-name-kw)
        msg-name (name msg-name-kw)]
    (keyword msg-ns (str msg-name "-" (name msg-type)))))

(def rpc-id-schema l/int-schema)

(def fp-schema
  (l/make-fixed-schema ::fp 8))

(def null-or-string-schema
  (l/make-union-schema [l/null-schema l/string-schema]))

(def null-or-fp-schema
  (l/make-union-schema [l/null-schema fp-schema]))

(def match-schema
  (l/make-enum-schema ::match
                      [:both :client :none]))

(def handshake-req-schema
  (l/make-record-schema ::handshake-req
                        [[:client-fp fp-schema]
                         [:client-pcf null-or-string-schema]
                         [:server-fp fp-schema]]))

(def handshake-rsp-schema
  (l/make-record-schema ::handshake-rsp
                        [[:match match-schema]
                         [:server-fp null-or-fp-schema]
                         [:server-pcf null-or-string-schema]]))

(def rpc-failure-type-schema
  (l/make-enum-schema ::rpc-failure-type
                      [:unauthorized :server-exception :client-exception]))

(def login-req-schema
  (l/make-record-schema ::login-req
                        [[:subject-id l/string-schema]
                         [:credential l/string-schema]]))
(def login-rsp-schema
  (l/make-record-schema ::login-rsp
                        [[:was-successful l/boolean-schema]]))
(def logout-req-schema
  (l/make-record-schema ::logout-req
                        [[:content l/null-schema]]))
(def logout-rsp-schema
  (l/make-record-schema ::logout-rsp
                        [[:was-successful l/boolean-schema]]))

(def rpc-failure-rsp-schema
  (l/make-record-schema ::rpc-failure-rsp
                        [[:rpc-id rpc-id-schema]
                         [:rpc-name l/string-schema]
                         [:rpc-arg l/string-schema]
                         [:failure-type rpc-failure-type-schema]
                         [:error-str l/string-schema]]))

(defn make-rpc-req-schema [[rpc-name rpc-info]]
  (let [rec-name (make-msg-record-name :rpc-req rpc-name)]
    (l/make-record-schema rec-name
                          [[:rpc-id rpc-id-schema]
                           [:arg (:arg-schema rpc-info)]])))

(defn make-rpc-success-rsp-schema [[rpc-name rpc-info]]
  (let [rec-name (make-msg-record-name :rpc-success-rsp rpc-name)]
    (l/make-record-schema rec-name
                          [[:rpc-id rpc-id-schema]
                           [:ret (:ret-schema rpc-info)]])))

(defn make-event-schema [[event-name event-schema]]
  (let [rec-name (make-msg-record-name :event event-name)]
    (l/make-record-schema rec-name
                          [[:event event-schema]])))

(s/defn make-msg-union-schema :- AvroSchema
  [api :- API]
  (let [{:keys [rpcs events]} api
        builtin-schemas [login-req-schema login-rsp-schema logout-req-schema
                         logout-rsp-schema rpc-failure-rsp-schema]
        rpc-req-schemas (map make-rpc-req-schema rpcs)
        rpc-success-rsp-schemas (map make-rpc-success-rsp-schema rpcs)
        event-schemas (map make-event-schema events)]
    (l/make-union-schema
     (concat builtin-schemas rpc-req-schemas rpc-success-rsp-schemas
             event-schemas))))

(s/defn long->ints :- (s/pair s/Int :high-int
                              s/Int :low-int)
  [l :- Long]
  (let [high (int #?(:clj (bit-shift-right l 32)
                     :cljs (.getHighBits l)))
        low (int #?(:clj (.intValue l)
                    :cljs (.getLowBits l)))]
    [high low]))

(defn long->byte-array [l]
  (let [[high low] (long->ints l)]
    (ba/byte-array
     [(bit-and 0xff (bit-shift-right high 24))
      (bit-and 0xff (bit-shift-right high 16))
      (bit-and 0xff (bit-shift-right high 8))
      (bit-and 0xff high)
      (bit-and 0xff (bit-shift-right low 24))
      (bit-and 0xff (bit-shift-right low 16))
      (bit-and 0xff (bit-shift-right low 8))
      (bit-and 0xff low)])))

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
