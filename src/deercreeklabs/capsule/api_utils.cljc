(ns deercreeklabs.capsule.api-utils
  (:require
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]]))

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
