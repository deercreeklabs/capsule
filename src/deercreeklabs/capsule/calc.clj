(ns deercreeklabs.capsule.calc
  (:gen-class)
  (:require
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.capsule.calc-api :as calc-api]
   [deercreeklabs.capsule.endpoint :as endpoint]
   [deercreeklabs.capsule.server :as cs]
   [deercreeklabs.capsule.utils :as u]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   [schema.core :as s]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]]))

(defn handle-calculate [arg metadata]
  (au/go
    (let [{:keys [nums operator]} arg
          op (case operator
               :add +
               :subtract -
               :multiply *
               :divide /)]
      (apply op nums))))

(defn make-handle-request-event [*endpoint]
  (fn handle-request-event [event-name-str metadata]
    (au/go
      (let [event (case event-name-str
                    "everybody-shake" {:duration-ms 1000}
                    "custom-event" {:map {"Name" "Foo"}})]
        (endpoint/send-event-to-all-conns @*endpoint event-name-str event)
        true))))

(defn <test-authenticate [subject-id credential]
  (au/go
    (when (and (= "test" subject-id)
               (= "test" credential))
      #{:admin})))

(defn -main
  [& args]
  (u/configure-logging)
  (let [*endpoint (atom nil)
        handlers {:rpcs {:calculate handle-calculate
                         :request-event (make-handle-request-event *endpoint)}}
        roles-to-rpcs {:public #{:calculate}
                       :admin #{:request-event}}
        endpoint-opts {:path "calc"
                       :<authenticator <test-authenticate}
        endpoint (endpoint/make-endpoint calc-api/api roles-to-rpcs handlers
                                         endpoint-opts)
        _ (reset! *endpoint endpoint)
        server (cs/make-server [endpoint])]
    (cs/start server)))
