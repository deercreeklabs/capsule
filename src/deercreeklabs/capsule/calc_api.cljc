(ns deercreeklabs.capsule.calc-api
  (:require
   [deercreeklabs.lancaster :as l]))

(def array-of-doubles-schema (l/make-array-schema l/double-schema))

(def operator-schema (l/make-enum-schema ::operator
                                         [:add :subtract :multiply :divide]))

(def string-map-schema (l/make-map-schema l/string-schema))

(def calculate-arg-schema
  (l/make-record-schema ::calculate-arg
                        [[:nums array-of-doubles-schema]
                         [:operator operator-schema]]))

(def everybody-shake-event-schema
  (l/make-record-schema ::everybody-shake-event
                        [[:duration-ms l/int-schema]]))

(def api
  {:rpcs {::calculate {:arg calculate-arg-schema
                       :ret l/double-schema}
          ::request-event {:arg l/string-schema
                           :ret l/boolean-schema}}
   :events {::everybody-shake everybody-shake-event-schema
            ::custom-event string-map-schema}})
