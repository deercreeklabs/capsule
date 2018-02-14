(ns deercreeklabs.capsule.test-gateway
  (:gen-class)
  (:require
   [clojure.core.async :as ca]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.capsule.calc-protocols :as calc-protocols]
   [deercreeklabs.capsule.endpoint :as endpoint]
   [deercreeklabs.capsule.server :as cs]
   [deercreeklabs.capsule.utils :as u]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   [schema.core :as s]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]]))

(defn test-authenticate [subject-id credential]
  (boolean (and (#{"test" "client0" "client1" "client2" "backend"} subject-id)
                (= "test" credential))))

(defn <handle-client-add [be arg metadata]
  (let [ids (endpoint/get-subject-conn-ids be "backend")
        conn-id (first ids)]
    (endpoint/<send-rpc be conn-id :add arg)))

(defn <handle-client-subtract [*backend-endpoint arg metadata]
  (let [be @*backend-endpoint
        conn-id (first (endpoint/get-subject-conn-ids be "backend"))]
    (endpoint/<send-rpc be conn-id :subtract arg)))

(defn handle-client-request-greeting-update [*backend-endpoint msg metadata]
  (let [be @*backend-endpoint
        conn-id (first (endpoint/get-subject-conn-ids be "backend"))]
    (endpoint/send-msg be conn-id :request-greeting-update nil)))

(defn handle-client-request-conn-count [*client-endpoint msg metadata]
  (let [ce @*client-endpoint
        {:keys [subject-id]} metadata
        conns (endpoint/get-subject-conn-count ce subject-id)]
    (endpoint/send-msg-to-subject-conns ce subject-id :subject-conn-count
                                        conns)))

(defn handle-client-ping [*client-endpoint msg metadata]
  (let [ce @*client-endpoint
        {:keys [conn-id]} metadata]
    (endpoint/send-msg ce conn-id :pong nil)))

(defn handle-backend-set-greeting [*client-endpoint msg metadata]
  (endpoint/send-msg-to-all-conns @*client-endpoint :set-greeting msg))

(defn make-calc-gateway []
  (u/configure-logging)
  (let [client-proto calc-protocols/client-gateway-protocol
        backend-proto calc-protocols/gateway-backend-protocol
        *client-endpoint (atom nil)
        *backend-endpoint (atom nil)
        client-handlers {:subtract (partial <handle-client-subtract
                                            *backend-endpoint)
                         :request-greeting-update
                         (partial handle-client-request-greeting-update
                                  *backend-endpoint)
                         :request-conn-count
                         (partial handle-client-request-conn-count
                                  *client-endpoint)
                         :ping (partial handle-client-ping *client-endpoint)}
        backend-handlers {:set-greeting
                          (partial handle-backend-set-greeting
                                   *client-endpoint)}
        client-endpoint (endpoint/make-endpoint
                         "client" test-authenticate client-proto :gateway
                         client-handlers)
        backend-endpoint (endpoint/make-endpoint
                          "backend" test-authenticate backend-proto :gateway
                          backend-handlers)]
    (reset! *client-endpoint client-endpoint)
    (reset! *backend-endpoint backend-endpoint)
    (endpoint/set-rpc-handler client-endpoint :add
                              (partial <handle-client-add backend-endpoint)
)
    (cs/make-server [client-endpoint backend-endpoint])))

(defn -main
  [& args]
  (let [gateway (make-calc-gateway)]
    (cs/start gateway)))
