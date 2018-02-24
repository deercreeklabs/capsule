(ns deercreeklabs.capsule.test-gateway
  (:require
   [clojure.core.async :as ca]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.capsule.calc-protocols :as calc-protocols]
   [deercreeklabs.capsule.endpoint :as ep]
   [deercreeklabs.capsule.server :as cs]
   [deercreeklabs.capsule.utils :as u]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   [schema.core :as s]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]]))

(defn test-authenticate [subject-id credential]
  (boolean (and (#{"test" "client0" "client1" "client2" "backend"} subject-id)
                (= "test" credential))))

(defn <handle-client-add [be arg metadata]
  (let [ids (ep/get-subject-conn-ids be "backend")
        conn-id (first ids)]
    (ep/<send-rpc be conn-id :add arg)))

(defn <handle-client-subtract [be arg metadata]
  (let [conn-id (first (ep/get-subject-conn-ids be "backend"))]
    (ep/<send-rpc be conn-id :subtract arg)))

(defn handle-client-request-greeting-update [be msg metadata]
  (let [conn-id (first (ep/get-subject-conn-ids be "backend"))]
    (ep/send-msg be conn-id :request-greeting-update nil)))

(defn handle-client-request-conn-count [ce msg metadata]
  (let [{:keys [subject-id]} metadata
        conns (ep/get-subject-conn-count ce subject-id)]
    (ep/send-msg-to-subject-conns ce subject-id :subject-conn-count
                                  conns)))

(defn handle-client-ping [ce msg metadata]
  (let [{:keys [conn-id]} metadata]
    (ep/send-msg ce conn-id :pong nil)))

(defn handle-backend-set-greeting [ce msg metadata]
  (ep/send-msg-to-all-conns ce :set-greeting msg))

(defn make-calc-gateway []
  (u/configure-logging)
  (let [client-proto calc-protocols/client-gateway-protocol
        backend-proto calc-protocols/gateway-backend-protocol
        backend-handlers {}
        client-ep (ep/make-endpoint
                   "client" test-authenticate client-proto :gateway)
        backend-ep (ep/make-endpoint
                    "backend" test-authenticate backend-proto :gateway)]
    (ep/set-rpc-handler client-ep :add
                        (partial <handle-client-add backend-ep))
    (ep/set-rpc-handler client-ep :subtract
                        (partial <handle-client-subtract backend-ep))
    (ep/set-msg-handler client-ep :request-greeting-update
                        (partial handle-client-request-greeting-update
                                 backend-ep))
    (ep/set-msg-handler client-ep :ping (partial handle-client-ping client-ep))
    (ep/set-msg-handler client-ep :request-conn-count
                        (partial handle-client-request-conn-count client-ep))
    (ep/set-msg-handler client-ep :ping (partial handle-client-ping client-ep))
    (ep/set-msg-handler backend-ep :set-greeting
                        (partial handle-backend-set-greeting client-ep))
    (cs/make-server [client-ep backend-ep])))

(defn -main
  [& args]
  (let [gateway (make-calc-gateway)]
    (cs/start gateway)))
