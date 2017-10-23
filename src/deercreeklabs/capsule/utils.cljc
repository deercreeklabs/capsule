(ns deercreeklabs.capsule.utils
  (:require
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   #?(:clj [puget.printer :refer [cprint]])
   [schema.core :as s]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]]))

#?(:cljs
   (set! *warn-on-infer* true))

;;;;;;;;;;;;;;;;;;;; Clojure Schemas ;;;;;;;;;;;;;;;;;;;;

(def Nil (s/eq nil))
(def AvroSchema (s/protocol deercreeklabs.lancaster.utils/IAvroSchema))
(def RpcOrEventName s/Str)
(def Path s/Str)
(def SubjectId s/Num)
(def RequestId s/Num)
(def EventDef {RpcOrEventName AvroSchema})
(def Handler (s/=> Nil Conn ConnId Path))
(def HandlerMap {RpcOrEventName Handler})
(def Identifier s/Str)
(def Credential s/Str)
(def Authenticator (s/=> (s/maybe SubjectId) Identifier Credential))
(def TubeConn (s/protocol deercreeklabs.tube.connection/IConnection))

(def RpcDef
  {RpcOrEventName {(s/required-key :arg) AvroSchema
                   (s/optional-key :ret) AvroSchema}})

(def Api
  {(s/optional-key :public-rpcs) RpcDef
   (s/optional-key :private-rpcs) RpcDef
   (s/optional-key :events) EventDef})

(def ApiImpl
  {(s/optional-key :path) Path
   (s/required-key :api) Api
   (s/required-key :handlers) HandlerMap})

(def RpcMetadata
  {:subject-id SubjectId
   :request-id RequestId
   :timeout-ms s/Num})

(def EndpointOptions
  {(s/optional-key :path) Path
   (s/optional-key :authenticator) Authenticator})


;;;;;;;;;;;;;;;;;;;; Avro Schemas ;;;;;;;;;;;;;;;;;;;;

(l/def-fixed-schema fp-schema
  16)

(l/def-union-schema null-or-string-schema
  :null :string)

(l/def-enum-schema match-schema
  :both :client :none)

(l/def-union-schema null-or-fp-schema
  :null fp-schema)

(l/def-record-schema handshake-req-schema
  [:client-fp fp-schema]
  [:client-api null-or-string-schema]
  [:server-fp fp-schema])

(l/def-record-schema handshake-rsp-schema
  [:match match-schema]
  [:server-fp null-or-fp-schema]
  [:server-api null-or-string-schema])

(l/def-union-schema null-or-handshake-req-schema
  :null handshake-req-schema)

(l/def-union-schema null-or-handshake-rsp-schema
  :null handshake-rsp-schema)

(l/def-record-schema c-to-s-capsule-schema
  [:handshake-req null-or-handshake-req-schema]
  [:encoded-msg l/bytes-schema])

(l/def-record-schema s-to-c-capsule-schema
  [:handshake-rsp null-or-handshake-rsp-schema]
  [:encoded-msg l/bytes-schema])

;;;;;;;;;;;;;;;;;;;; Helper Fns ;;;;;;;;;;;;;;;;;;;;

(defn configure-logging []
  (timbre/merge-config!
   {:level :debug
    :output-fn lu/short-log-output-fn
    :appenders
    {:println {:ns-blacklist
               ["org.apache.*"]}}}))
