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
    ["wss://chadlaptop.f1shoppingcart.com:8080/calc"]))

(deftest test-calculate
  (au/test-async
   1000
   (ca/go
     (let [client (cc/make-client calc-api/api <get-uris)
           arg {:nums [1 2 3]
                :operator :add}
           expected {:error nil
                     :result 6.0}]
       (try
         (is (= expected (au/<? (cc/<send-rpc client :calculate arg))))
         (finally
           (cc/shutdown client)))))))

(deftest test-request-event-not-authenticated
  (au/test-async
   1000
   (ca/go
     (let [client (cc/make-client calc-api/api <get-uris)
           expected {}]
       (try
         (let [rsp (au/<? (cc/<send-rpc client
                                        :request-event "everybody-shake"))
               {:keys [error result]} rsp]
           (is (nil? result))
           (is (= "Unauthorized RPC to :request-event"
                  (subs error 0 34))))
         (finally
           (cc/shutdown client)))))))

(deftest test-request-event-authenticated
  (au/test-async
   1000
   (ca/go
     (let [opts {:subject-id "test"
                 :credential "test"}
           client (cc/make-client calc-api/api <get-uris opts)
           event-name :everybody-shake
           event-ch (ca/chan)
           event-handler #(ca/put! event-ch %)]
       (try
         (cc/bind-event client event-name event-handler)
         (is (= {:error nil, :result true}
                (au/<? (cc/<send-rpc client :request-event (name event-name)))))
         (is (= {:duration-ms 1000} (au/<? event-ch)))
         (finally
           (cc/shutdown client)))))))

(deftest test-login-logout-w-same-client
  (au/test-async
   1000
   (ca/go
     (let [client (cc/make-client calc-api/api <get-uris)
           event-name :everybody-shake
           event-ch (ca/chan)
           event-handler #(ca/put! event-ch %)]
       (try
         (cc/bind-event client event-name event-handler)
         (let [rsp (au/<? (cc/<send-rpc client
                                        :request-event (name event-name)))
               {:keys [error result]} rsp]
           (is (nil? result))
           (is (= "Unauthorized RPC to :request-event"
                  (subs error 0 34))))
         (au/<? (cc/<log-in client "test" "test"))
         (is (= {:error nil, :result true}
                (au/<? (cc/<send-rpc client :request-event (name event-name)))))
         (is (= {:duration-ms 1000} (au/<? event-ch)))
         (au/<? (cc/<log-out client))
         (let [rsp (au/<? (cc/<send-rpc client
                                        :request-event (name event-name)))
               {:keys [error result]} rsp]
           (is (nil? result))
           (is (= "Unauthorized RPC to :request-event"
                  (subs error 0 34))))
         (finally
           (cc/shutdown client)))))))
