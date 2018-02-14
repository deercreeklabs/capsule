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

(defn handle-request-greeting-update [*client msg metadata]
  (cc/send-msg @*client :set-greeting greeting))

(defn make-backend
  ([<get-gw-url <get-credentials]
   (make-backend <get-gw-url <get-credentials nil))
  ([<get-gw-url <get-credentials options]
   (let [*client (atom nil)
         protocol calc-protocols/gateway-backend-protocol
         handlers {:add handle-add
                   :subtract handle-subtract
                   :request-greeting-update
                   (partial handle-request-greeting-update *client)}
         client (if options
                  (cc/make-client <get-gw-url <get-credentials protocol
                                  :backend handlers options)
                  (cc/make-client <get-gw-url <get-credentials protocol
                                               :backend handlers))]
     (reset! *client client)
     client)))
