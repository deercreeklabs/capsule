(ns deercreeklabs.capsule.test-gateway
  (:require
   [clojure.core.async :as ca]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.capsule.calc-protocols :as calc-protocols]
   [deercreeklabs.capsule.endpoint :as ep]
   [deercreeklabs.capsule.logging :as log]
   [deercreeklabs.capsule.server :as cs]
   [deercreeklabs.capsule.utils :as u]
   [schema.core :as s]))

(defn on-connect [conn conn-count]
  (log/info "Client connected."))

(defn on-disconnect [conn code reason conn-count]
  (log/info "Client disconnected."))

(defn test-authenticate [subject-id subject-secret metadata]
  (boolean (and (#{"test" "client0" "client1" "client2" "backend"} subject-id)
                (= "test" subject-secret))))

(defn <handle-client-add [be arg metadata]
  (let [ids (ep/get-subject-conn-ids be "backend")
        conn-id (first ids)]
    (ep/<send-msg be conn-id :add arg)))

(defn <handle-client-subtract [be arg metadata]
  (let [conn-id (first (ep/get-subject-conn-ids be "backend"))]
    (ep/<send-msg be conn-id :subtract arg)))

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

(defn <handle-invert [ce arg metadata]
  (au/go
    (let [{:keys [conn-id]} metadata
          i (au/<? (ep/<send-msg ce conn-id :arg-string-to-int arg))]
      (- 0 i))))

(defn run-calc-gateway []
  (let [client-proto calc-protocols/client-gateway-protocol
        backend-proto calc-protocols/gateway-backend-protocol
        client-ep (ep/endpoint
                   "client" test-authenticate client-proto :gateway
                   (u/sym-map on-connect on-disconnect))
        backend-ep (ep/endpoint
                    "backend" test-authenticate backend-proto :gateway
                    {:on-connect (fn [conn conn-count]
                                   (log/info "Backend connected."))
                     :on-disconnect (fn [conn code reason conn-count]
                                      (log/info "Backend disconnected."))})]
    (u/configure-logging)
    (log/info "Logging configured...")
    (ep/set-handler client-ep :add
                    (partial <handle-client-add backend-ep))
    (ep/set-handler client-ep :subtract
                    (partial <handle-client-subtract backend-ep))
    (ep/set-handler client-ep :request-greeting-update
                    (partial handle-client-request-greeting-update
                             backend-ep))
    (ep/set-handler client-ep :ping (partial handle-client-ping client-ep))
    (ep/set-handler client-ep :request-conn-count
                    (partial handle-client-request-conn-count client-ep))
    (ep/set-handler client-ep :ping (partial handle-client-ping client-ep))
    (ep/set-handler client-ep :invert (partial <handle-invert client-ep))
    (ep/set-handler backend-ep :set-greeting
                    (partial handle-backend-set-greeting client-ep))
    (cs/server [client-ep backend-ep])))

(defn -main
  [& args]
  (run-calc-gateway))
