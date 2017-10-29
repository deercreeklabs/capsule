(ns deercreeklabs.capsule.calc
  (:gen-class)
  (:require
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.capsule.calc-api :as calc-api]
   [deercreeklabs.capsule.server :as cs]
   [deercreeklabs.capsule.utils :as u]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   [schema.core :as s]))

(defn handle-calculate [arg metadata]
  (au/go
    (let [{:keys [nums operator]} arg
          op (case operator
               :add +
               :subtract -
               :multiply *
               :divide /)]
      (apply op nums))))

(defn handle-request-event [arg metadata]
  (au/go
    (let [{:keys [event-name]} arg
          event (case event-name
                  :everybody-shake {:duration-ms 1000}
                  :custom-event {:map {"Name" "Foo"}})]
      (endpoint/send-event-to-all-conns endpoint event-name event)
      nil)))

(defn -main
  [& args]
  (u/configure-logging)
  (let [handlers {:rpcs {:calculate handle-calculate
                         :request-event handle-request-event}}
        endpoint (make-endpoint calc-api/api handlers {:path "calc"})
        server (cs/make-server [endpoint])]
    (cs/start server)))
