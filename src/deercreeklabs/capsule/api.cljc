(ns deercreeklabs.capsule.api
  (:require
   [deercreeklabs.capsule.api-utils :as api-utils]
   [deercreeklabs.lancaster :as l]
   [schema.core :as s]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]]))

(defprotocol IAPI
  (encode [this msg-info])
  (decode [this writer-msg-schema ba])
  (get-msg-schema [this])
  (get-rpc-name-kws [this])
  (get-event-name-kws [this]))

(defrecord API [msg-schema rpc-name-kws event-name-kws]
  IAPI
  (encode [this msg-info]
    (api-utils/encode-msg* msg-schema msg-info))

  (decode [this writer-msg-schema ba]
    (api-utils/decode-msg* msg-schema writer-msg-schema ba))

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

(defmacro def-api [var-name api-info]
  (let [{:keys [rpcs events]} api-info
        rpc-names (into #{} (keys rpcs))
        event-names (into #{} (keys events))
        login-req-sym (api-utils/make-record-name-sym :login-req :login-req)
        login-rsp-sym (api-utils/make-record-name-sym :login-rsp :login-rsp)
        logout-req-sym (api-utils/make-record-name-sym :logout-req :logout-req)
        logout-rsp-sym (api-utils/make-record-name-sym :logout-rsp :logout-rsp)
        rpc-failure-sym (api-utils/make-record-name-sym :rpc-failure-rsp
                                                :rpc-failure-rsp)
        builtin-syms [login-req-sym login-rsp-sym logout-req-sym logout-rsp-sym
                      rpc-failure-sym]
        rpc-req-schemas (map api-utils/make-rpc-req-schema rpcs)
        rpc-success-rsp-schemas (map api-utils/make-rpc-success-rsp-schema rpcs)
        event-schemas (map api-utils/make-event-schema events)
        msg-schema-sym (gensym "msg-schema")
        msg-schema (api-utils/make-msg-schema msg-schema-sym
                                              api-info builtin-syms)]
    `(do
       (l/def-record-schema ~login-req-sym
         [:msg-id api-utils/msg-id-schema]
         [:content api-utils/login-info-schema])
       (l/def-record-schema ~login-rsp-sym
         [:msg-id api-utils/msg-id-schema]
         [:content l/boolean-schema])
       (l/def-record-schema ~logout-req-sym
         [:msg-id api-utils/msg-id-schema]
         [:content l/null-schema])
       (l/def-record-schema ~logout-rsp-sym
         [:msg-id api-utils/msg-id-schema]
         [:content l/boolean-schema])
       (l/def-record-schema ~rpc-failure-sym
         [:msg-id api-utils/msg-id-schema]
         [:content api-utils/rpc-failure-info-schema])
       ~@rpc-req-schemas
       ~@rpc-success-rsp-schemas
       ~@event-schemas
       ~msg-schema
       (def ~var-name
         (->API ~msg-schema-sym ~rpc-names ~event-names)))))
