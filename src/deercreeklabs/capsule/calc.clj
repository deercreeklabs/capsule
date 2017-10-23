(ns deercreeklabs.capsule.calc
  (:gen-class)
  (:require
   [deercreeklabs.capsule.calc-api :as calc-api]
   [deercreeklabs.capsule.server :as cs]
   [deercreeklabs.capsule.utils :as u]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   [schema.core :as s]))

(defn handle-calculate [arg metadata server]
  (let [{:keys [nums operator]} arg
        op (case operator
             :add +
             :subtract -
             :multiply *
             :divide /)]
    (apply op nums)))

(defn handle-request-event [arg metadata server]
  (let [{:keys [event-name]} arg
        event (case event-name
                "everybody-shake" {:duration-ms 1000}
                "custom-event" {:map {"Name" "Foo"}})]
    (cs/send-event server event)))

(defn -main
  [& args]
  (u/configure-logging)
  (let [handlers {"calculate" handle-calculate
                  "request-event" handle-request-event}
        impl {:path "calc"
              :api calc-api/api
              :handlers handlers}
        server (cs/make-server [impl])]
    (cs/start server)))
