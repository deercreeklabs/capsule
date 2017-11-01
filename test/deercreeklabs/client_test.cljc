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
     (let [client (cc/make-client calc-api/api <get-uris)]
       (try
         (let [arg {:nums [1 2 3]
                    :operator :add}
               expected {:error nil
                         :result 6.0}
               [v ch] (au/alts? [(cc/<send-rpc client :calculate arg)
                                 (ca/timeout 1000)])]
           (is (= expected v)))
         (finally
           (cc/shutdown client)))))))

(deftest test-request-event-not-authenticated
  (au/test-async
   1000
   (ca/go
     (let [client (cc/make-client calc-api/api <get-uris)]
       (try
         (let [[rsp ch] (au/alts? [(cc/<send-rpc client
                                                  :request-event
                                                  "everybody-shake")
                                   (ca/timeout 1000)])
               {:keys [error result]} rsp]
           (is (nil? result))
           (is (= "Unauthorized RPC to :request-event"
                  (subs error 0 34))))
         (finally
           (cc/shutdown client)))))))

(deftest test-request-event-authenticated
  (au/test-async
   3000
   (ca/go
     (let [client (cc/make-client calc-api/api <get-uris )
           event-name :everybody-shake
           event-ch (ca/chan)
           event-handler #(ca/put! event-ch %)]
       (try
         (cc/bind-event client event-name event-handler)
         (let [[v ch] (au/alts? [(cc/<log-in client "test" "test")
                                 (ca/timeout 1000)])
               _ (is (= {:error nil, :result true} v))
               [v ch] (au/alts? [(cc/<send-rpc client :request-event
                                               (name event-name))
                                 (ca/timeout 1000)])
               _ (is (= {:error nil, :result true} v))
               [v ch] (au/alts? [event-ch (ca/timeout 1000)])]
           (is (= {:duration-ms 1000} v)))
         (finally
           (cc/shutdown client)))))))

(deftest test-login-logout-w-same-client
  (au/test-async
   3000
   (ca/go
     (let [client (cc/make-client calc-api/api <get-uris)]
       (try
         (let [event-name :everybody-shake
               event-ch (ca/chan)
               event-handler #(ca/put! event-ch %)
               _ (cc/bind-event client event-name event-handler)
               [rsp ch] (au/alts? [(cc/<send-rpc client
                                                  :request-event
                                                  (name event-name))
                                   (ca/timeout 1000)])
               _ (is (= "Unauthorized RPC to :request-event"
                        (subs (:error rsp) 0 34)))
               [v ch] (au/alts? [(cc/<log-in client "test" "test")
                                 (ca/timeout 1000)])
               [rsp ch] (au/alts? [(cc/<send-rpc client
                                                  :request-event
                                                  (name event-name))
                                   (ca/timeout 1000)])
               _ (is (= {:error nil, :result true} rsp))
               [v ch] (au/alts? [event-ch (ca/timeout 1000)])
               _ (is (= {:duration-ms 1000} v))
               [v ch] (au/alts? [(cc/<log-out client) (ca/timeout 1000)])
               [rsp ch] (au/alts? [(cc/<send-rpc client
                                                  :request-event
                                                  (name event-name))
                                   (ca/timeout 1000)])
               _ (is (= "Unauthorized RPC to :request-event"
                        (subs (:error rsp) 0 34)))])
         (finally
           (cc/shutdown client)))))))

(deftest test-non-existent-rpc
  (au/test-async
   1000
   (ca/go
     (let [client (cc/make-client calc-api/api <get-uris)]
       (try
         (let [[rsp ch] (au/alts? [(cc/<send-rpc client
                                                 :non-existent "arg")
                                   (ca/timeout 1000)])]
           (is (= "RPC `:non-existent` is not in the API."
                  (subs (:error rsp) 0 38))))
         (finally
           (cc/shutdown client)))))))

(deftest test-bind-non-existent-event
  (let [client (cc/make-client calc-api/api <get-uris)]
    (try
      (cc/bind-event client :non-existent (constantly :foo))
      (is (= :did-not-throw :this-test))
      (catch #?(:clj Exception :cljs js/Error) e
          (let [{:keys [subtype error-str event-name-kw]} (ex-data e)]
            (is (= :unknown-event-name-kw subtype))
            (is (= "Event `:non-existent` is not in the API." error-str))
            (is (= :non-existent event-name-kw))))
      (finally
        (cc/shutdown client)))))
