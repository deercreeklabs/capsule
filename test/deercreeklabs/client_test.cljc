(ns deercreeklabs.client-test
  (:require
   [clojure.core.async :as ca]
   [clojure.test :refer [deftest is]]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.capsule.calc-api :as calc-api]
   [deercreeklabs.capsule.client :as cc]
   [deercreeklabs.capsule.utils :as u]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   [schema.core :as s]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]]))

;; Use this instead of fixtures, which are hard to make work w/ async testing.
(s/set-fn-validation! true)

(u/configure-logging)

(defn <get-uris []
  (au/go
    "http://localhost:8080/calc"))

(deftest test-calculate
  (au/test-async
   1000
   (ca/go
     (let [client (cc/make-client calc-api/api <get-uris)
           arg {:nums [1 2 3]
                :operator :add}]
       (is (= 67 (cc/<send-rpc client "calculate" arg)))))))

#_
(deftest test-request-event-not-authenticated
  (au/test-async
   1000
   (ca/go
     (let [client (cc/make-client calc-api/api <get-uris)
           arg {:event-name "everybody-shake"}]
       (try
         (au/<? (cc/<send-rpc client "request-event" arg))
         (is (= :did-not-throw true))
         (catch #?(:clj Exception :cljs js/Error) e
             (is (= :foo e))))))))

#_
(deftest test-request-event-authenticated
  (au/test-async
   1000
   (ca/go
     (let [opts {:subject-id "test"
                 :credential "test!"}
           client (cc/make-client calc-api/api)
           event-name "everybody-shake"
           arg {:event-name event-name}
           event-ch (ca/chan)
           event-handler #(ca/put! event-ch %)]
       (cc/bind-event client event-name event-handler)
       (is (= :foo (au/<? (cc/<send-rpc client "request-event" arg))))
       (is (= :foo (au/<? event-ch)))))))
