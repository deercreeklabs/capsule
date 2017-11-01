(ns deercreeklabs.capsule.calc-api
  (:require
   [deercreeklabs.capsule :as capsule]
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

(l/def-record-schema request-event-arg-schema
  [:event-name :string])

(capsule/def-api api
  {:rpcs {:calculate {:arg-schema calculate-arg-schema
                      :ret-schema l/double-schema}
          :request-event {:arg-schema l/string-schema
                          :ret-schema l/boolean-schema}}
   :events {:everybody-shake everybody-shake-event-schema
            :custom-event string-map-schema}})
