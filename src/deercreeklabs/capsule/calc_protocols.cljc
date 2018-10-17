(ns deercreeklabs.capsule.calc-protocols
  (:require
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]]))

(def op-arg-schema
  (l/array-schema l/float-schema))

(def client-gateway-protocol
  {:roles [:client :gateway]
   :msgs {:add {:arg op-arg-schema
                :ret l/float-schema
                :sender :client}
          :subtract {:arg op-arg-schema
                     :ret l/float-schema
                     :sender :client}
          :request-greeting-update {:arg l/null-schema
                                    :sender :client}
          :request-conn-count {:arg l/null-schema
                               :sender :client}
          :ping {:arg l/null-schema
                 :sender :either}
          :set-greeting {:arg l/string-schema
                         :sender :gateway}
          :subject-conn-count {:arg l/int-schema
                               :sender :gateway}
          :pong {:arg l/null-schema
                 :sender :gateway}}})

(def gateway-backend-protocol
  {:roles [:gateway :backend]
   :msgs {:add {:arg op-arg-schema
                :ret l/float-schema
                :sender :gateway}
          :subtract {:arg op-arg-schema
                     :ret l/float-schema
                     :sender :gateway}
          :request-greeting-update {:arg l/null-schema
                                    :sender :gateway}
          :set-greeting {:arg l/string-schema
                         :sender :backend}}})
