(ns deercreeklabs.capsule.calc-api
  (:require
   [deercreeklabs.capsule :as c]
   [deercreeklabs.capsule.utils :as u]
   [deercreeklabs.lancaster :as l]))

(u/configure-logging)

(l/def-array-schema array-of-doubles-schema
  :double)

(l/def-enum-schema operator-schema
  :add :subtract :multiply :divide)

(l/def-map-schema string-map-schema
  :string)

(l/def-record-schema calculate-arg-schema
  [:nums array-of-doubles-schema]
  [:operator operator-schema])

(l/def-record-schema everybody-shake-event-schema
  [:duration-ms :int])

(l/def-record-schema custom-event-schema
  [:map string-map-schema])

(l/def-record-schema request-event-arg-schema
  [:event-name :string])

(def api
  {:public-rpcs {"calculate" {:arg calculate-req-schema
                              :ret l/double-schema}}
   :private-rpcs {"request-event" {:req request-event-arg-schema}}
   :events {"everybody-shake" everybody-shake-event-schema
            "custom-event" custom-event-schema}})
