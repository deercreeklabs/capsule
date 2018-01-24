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
  (let [{:keys [nums operator]} arg
        op (case operator
             :add +
             :subtract -
             :multiply *
             :divide /)]
    (apply op nums)))

(defn make-handle-request-event [*endpoint]
  (fn <handle-request-event [event-name-str metadata]
    (au/go
      (let [[event-name-kw event] (case event-name-str
                                    "everybody-shake"
                                    [::calc-api/everybody-shake
                                     {:duration-ms 1000}]

                                    "custom-event"
                                    [::calc-api/custom-event
                                     {:event-name "My event"
                                      :event-data "Yo"}])]
        (endpoint/send-event-to-all-conns @*endpoint event-name-kw event)
        true))))

(defn <handle-notify-user [*endpoint arg metadata]
  (au/go
    (let [{:keys [subject-id event-name event-data]} arg]
      (endpoint/send-event-to-subject-conns
       @*endpoint subject-id ::calc-api/custom-event
       (u/sym-map event-name event-data))
      true)))

(defn <handle-get-num-user-conns [*endpoint subject-id metadata]
  (au/go
    (endpoint/get-subject-conn-count @*endpoint subject-id)))

(defn <test-authenticate [endpoint subject-id credential]
  (au/go
    (if (and (= "test" subject-id)
             (= "test" credential))
      {:was-successful true
       :roles #{:admin}}
      {:was-successful false
       :reason "Bad credentials"})))

(defn make-calc-server []
  (u/configure-logging)
  (let [*endpoint (atom nil)
        handlers {:rpcs {::calc-api/calculate handle-calculate
                         ::calc-api/request-event
                         (make-handle-request-event *endpoint)
                         ::calc-api/notify-user
                         (partial <handle-notify-user *endpoint)
                         ::calc-api/get-num-user-conns
                         (partial <handle-get-num-user-conns *endpoint)}}
        public-rpcs #{::calc-api/calculate}
        roles->rpcs {:public public-rpcs
                     :admin (clojure.set/union
                             public-rpcs
                             #{::calc-api/request-event
                               ::calc-api/notify-user
                               ::calc-api/get-num-user-conns})}
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
