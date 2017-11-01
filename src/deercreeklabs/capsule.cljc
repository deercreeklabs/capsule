(ns deercreeklabs.capsule
  (:require
   [deercreeklabs.capsule.utils :as u]
   [schema.core :as s]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]]))

(defmacro def-api [var-name api-info]
  (let [{:keys [rpcs events]} api-info
        rpc-names (into #{} (keys rpcs))
        event-names (into #{} (keys events))
        login-req-sym (u/make-record-name-sym :login-req :login-req)
        login-rsp-sym (u/make-record-name-sym :login-rsp :login-rsp)
        logout-req-sym (u/make-record-name-sym :logout-req :logout-req)
        logout-rsp-sym (u/make-record-name-sym :logout-rsp :logout-rsp)
        rpc-failure-sym (u/make-record-name-sym :rpc-failure-rsp
                                                :rpc-failure-rsp)
        builtin-syms [login-req-sym login-rsp-sym logout-req-sym logout-rsp-sym
                      rpc-failure-sym]
        rpc-req-schemas (map u/make-rpc-req-schema rpcs)
        rpc-success-rsp-schemas (map u/make-rpc-success-rsp-schema rpcs)
        event-schemas (map u/make-event-schema events)
        msg-schema-sym (gensym "msg-schema")
        msg-schema (u/make-msg-schema msg-schema-sym api-info builtin-syms)]
    `(do
       (l/def-record-schema ~login-req-sym
         [:msg-id u/msg-id-schema]
         [:content u/login-info-schema])
       (l/def-record-schema ~login-rsp-sym
         [:msg-id u/msg-id-schema]
         [:content l/boolean-schema])
       (l/def-record-schema ~logout-req-sym
         [:msg-id u/msg-id-schema]
         [:content l/null-schema])
       (l/def-record-schema ~logout-rsp-sym
         [:msg-id u/msg-id-schema]
         [:content l/boolean-schema])
       (l/def-record-schema ~rpc-failure-sym
         [:msg-id u/msg-id-schema]
         [:content u/rpc-failure-info-schema])
       ~@rpc-req-schemas
       ~@rpc-success-rsp-schemas
       ~@event-schemas
       ~msg-schema
       (def ~var-name
         (u/->API ~msg-schema-sym ~rpc-names ~event-names)))))
