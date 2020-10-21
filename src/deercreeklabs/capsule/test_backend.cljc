(ns deercreeklabs.capsule.test-backend
  (:require
   [clojure.core.async :as ca]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.capsule.calc-protocols :as calc-protocols]
   [deercreeklabs.capsule.client :as cc]
   [deercreeklabs.capsule.logging :as log]
   [deercreeklabs.capsule.utils :as u]
   [schema.core :as s]))

(def greeting "Hello")

(defn handle-add [arg metadata]
  (apply + arg))

(defn handle-subtract [arg metadata]
  (apply - arg))

(defn handle-request-greeting-update [client msg metadata]
  (cc/send-msg client :set-greeting greeting))

(defn backend
  ([<get-gw-url <get-credentials]
   (backend <get-gw-url <get-credentials nil))
  ([<get-gw-url <get-credentials options]
   (let [protocol calc-protocols/gateway-backend-protocol
         client (if options
                  (cc/client <get-gw-url <get-credentials protocol
                             :backend options)
                  (cc/client <get-gw-url <get-credentials protocol
                             :backend))]
     (cc/set-handler client :add handle-add)
     (cc/set-handler client :subtract handle-subtract)
     (cc/set-handler client :request-greeting-update
                     (partial handle-request-greeting-update client))
     client)))
