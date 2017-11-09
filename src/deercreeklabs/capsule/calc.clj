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
      (let [[event-name-kw event] (case event-name-str
                                    "everybody-shake"
                                    [::calc-api/everybody-shake
                                     {:duration-ms 1000}]

                                    "custom-event"
                                    [::calc-api/custom-event "A nice string"])]
        (endpoint/send-event-to-all-conns @*endpoint event-name-kw event)
        true))))

(defn <test-authenticate [subject-id credential]
  (au/go
    (when (and (= "test" subject-id)
               (= "test" credential))
      #{:admin})))

(defn make-calc-server []
  (u/configure-logging)
  (let [*endpoint (atom nil)
        handlers {:rpcs {::calc-api/calculate handle-calculate
                         ::calc-api/request-event
                         (make-handle-request-event *endpoint)}}
        public-rpcs #{::calc-api/calculate}
        roles->rpcs {:public public-rpcs
                     :admin (clojure.set/union public-rpcs
                                               #{::calc-api/request-event})}
        endpoint-opts {:path "calc"
                       :<authenticator <test-authenticate}
        endpoint (endpoint/make-endpoint calc-api/api roles->rpcs handlers
                                         endpoint-opts)]
    (reset! *endpoint endpoint)
    (cs/make-server [endpoint])))

(defn -main
  [& args]
  (let [server (make-calc-server)]
    (cs/start server)))
