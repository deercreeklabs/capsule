(ns deercreeklabs.capsule.calc-protocols
  (:require
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]]))

(def op-arg-schema
  (l/make-array-schema l/float-schema))

(def client-gateway-protocol
  {:client {:rpcs {:add {:arg op-arg-schema
                         :ret l/float-schema}
                   :subtract {:arg op-arg-schema
                              :ret l/float-schema}}
            :msgs {:request-greeting-update l/null-schema
                   :request-conn-count l/null-schema}}
   :gateway {:msgs {:set-greeting l/string-schema
                    :subject-conn-count l/int-schema}}})

(def gateway-backend-protocol
  {:gateway {:rpcs {:add {:arg op-arg-schema
                          :ret l/float-schema}
                    :subtract {:arg op-arg-schema
                               :ret l/float-schema}}
             :msgs {:request-greeting-update l/null-schema}}
   :backend {:msgs {:set-greeting l/string-schema}}})
