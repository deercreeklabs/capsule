(ns deercreeklabs.capsule.test-backend
  (:require
   [clojure.core.async :as ca]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.capsule.calc-protocols :as calc-protocols]
   [deercreeklabs.capsule.client :as cc]
   [deercreeklabs.capsule.utils :as u]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   [schema.core :as s]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]]))

(def greeting "Hello")

(defn handle-add [arg metadata]
  (apply + arg))

(defn handle-subtract [arg metadata]
  (apply - arg))

(defn handle-request-greeting-update [client msg metadata]
  (cc/send-msg client :set-greeting greeting))

(defn make-backend
  ([<get-gw-url <get-credentials]
   (make-backend <get-gw-url <get-credentials nil))
  ([<get-gw-url <get-credentials options]
   (let [protocol calc-protocols/gateway-backend-protocol
         client (if options
                  (cc/make-client <get-gw-url <get-credentials protocol
                                  :backend options)
                  (cc/make-client <get-gw-url <get-credentials protocol
                                  :backend))]
     (cc/set-handler client :add handle-add)
     (cc/set-handler client :subtract handle-subtract)
     (cc/set-handler client :request-greeting-update
                     (partial handle-request-greeting-update client))
     client)))
