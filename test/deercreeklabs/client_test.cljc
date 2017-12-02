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
    ["ws://localhost:8080/calc"]))

(def rpc-timeout #?(:cljs 10000 :clj 1000))

(defn slice-str [s len]
  (when s
    (let [str-len (count s)]
      (subs s 0 (min len str-len)))))

(deftest test-api
  (is (u/valid-api? calc-api/api)))

(deftest test-calculate
  (au/test-async
   1000
   (ca/go
     (let [client (cc/make-client calc-api/api <get-uris)]
       (try
         (let [arg {:nums [1 2 3]
                    :operator :add}
               [v ch] (au/alts?
                       [(cc/<send-rpc client
                                      ::calc-api/calculate
                                      arg)
                        (ca/timeout rpc-timeout)])]
           (is (= 6.0 v)))
         (finally
           (cc/shutdown client)))))))

(deftest test-request-event-not-authenticated
  (au/test-async
   1000
   (ca/go
     (binding [cc/**silence-log** true]
       (let [client (cc/make-client calc-api/api <get-uris)]
         (try
           (let [chs [(cc/<send-rpc client ::calc-api/request-event
                                    "everybody-shake")
                      (ca/timeout rpc-timeout)]
                 [rsp ch] (au/alts? chs)]
             (is (= :did-not-throw :should-not-get-here)))
           (catch #?(:clj Exception :cljs js/Error) e
             (is (re-find #"[Uu]nauthorized" (lu/get-exception-msg e))))
           (finally
             (cc/shutdown client))))))))

(deftest test-request-event-authenticated
  (au/test-async
   #?(:cljs 25000 :clj 3000)
   (ca/go
     (binding [cc/**silence-log** true]
       (let [client (cc/make-client calc-api/api <get-uris )
             event-name ::calc-api/everybody-shake
             event-ch (ca/chan)
             event-handler #(ca/put! event-ch %)]
         (try
           (cc/bind-event client event-name event-handler)
           (let [login-ch (cc/<log-in client "test" "test")
                 [v ch] (au/alts? [login-ch (ca/timeout rpc-timeout)])
                 _ (is (= login-ch ch))
                 _ (is (= true v))
                 [v ch] (au/alts? [(cc/<send-rpc client ::calc-api/request-event
                                                 (name event-name))
                                   (ca/timeout rpc-timeout)])
                 _ (is (= true v))
                 [v ch] (au/alts? [event-ch (ca/timeout rpc-timeout)])]
             (is (= event-ch ch))
             (is (= {:duration-ms 1000} v)))
           (finally
             (cc/shutdown client))))))))

(deftest test-login-logout-w-same-client
  (au/test-async
   #?(:cljs 15000 :clj 3000)
   (ca/go
     (binding [cc/**silence-log** true]
       (let [client (cc/make-client calc-api/api <get-uris)]
         (try
           (let [event-name ::calc-api/everybody-shake
                 event-ch (ca/chan)
                 event-handler #(ca/put! event-ch %)]
             (cc/bind-event client event-name event-handler)
             (try
               (au/alts? [(cc/<send-rpc client ::calc-api/request-event
                                        (name event-name))
                          (ca/timeout rpc-timeout)])
               (is (= :did-not-throw :should-not-get-here))
               (catch #?(:clj Exception :cljs js/Error) e
                 (is (re-find #"[Uu]nauthorized" (lu/get-exception-msg e)))))

             (let [[v ch] (au/alts? [(cc/<log-in client "test" "test")
                                     (ca/timeout rpc-timeout)])
                   _ (is (= true (cc/logged-in? client)))
                   [rsp ch] (au/alts? [(cc/<send-rpc client
                                                     ::calc-api/request-event
                                                     (name event-name))
                                       (ca/timeout rpc-timeout)])
                   _ (is (= true rsp))
                   [v ch] (au/alts? [event-ch (ca/timeout rpc-timeout)])
                   _ (is (= {:duration-ms 1000} v))
                   [rsp ch] (au/alts? [(cc/<log-out client)
                                       (ca/timeout rpc-timeout)])]
               (is (= true rsp))
               (is (= false (cc/logged-in? client)))
               (try
                 (au/alts? [(cc/<send-rpc client ::calc-api/request-event
                                          (name event-name))
                            (ca/timeout rpc-timeout)])
                 (is (= :did-not-throw :should-not-get-here))
                 (catch #?(:clj Exception :cljs js/Error) e
                   (is (re-find #"[Uu]nauthorized"
                                (lu/get-exception-msg e)))))))
           (finally
             (cc/shutdown client))))))))

(deftest test-non-existent-rpc
  (au/test-async
   1000
   (ca/go
     (let [client (cc/make-client calc-api/api <get-uris)]
       (try
         (au/alts? [(cc/<send-rpc client :non-existent "arg")
                    (ca/timeout rpc-timeout)])
         (is (= :did-not-throw :should-not-get-here))
         (catch #?(:clj Exception :cljs js/Error) e
           (is (re-find #"RPC `:non-existent` is not in the API."
                        (lu/get-exception-msg e))))
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
