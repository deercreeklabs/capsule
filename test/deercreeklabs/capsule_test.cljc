(ns deercreeklabs.capsule-test
  (:require
   [clojure.core.async :as ca]
   [clojure.test :refer [deftest is]]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.capsule.calc-protocols :as calc-protocols]
   [deercreeklabs.capsule.client :as cc]
   [deercreeklabs.capsule.test-backend :as tb]
   [deercreeklabs.capsule.utils :as u]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   [schema.core :as s]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]]))

;; Use this instead of fixtures, which are hard to make work w/ async testing.
(s/set-fn-validation! true)

(u/configure-logging)

(defn make-<get-gw-url [endpoint]
  (fn <get-gw-url []
    (let [ch (ca/chan)]
      (ca/put! ch (str "ws://localhost:8080/" endpoint))
      ch)))

(defn make-<get-credentials [subject-id credential]
  (fn <get-credentials []
    (let [ch (ca/chan)]
      (ca/put! ch (u/sym-map subject-id credential))
      ch)))

(def rpc-timeout #?(:cljs 10000 :clj 1000))
(def test-timeout #?(:cljs 20000 :clj 2000))
(def cg-proto calc-protocols/client-gateway-protocol)

(defn slice-str [s len]
  (when s
    (let [str-len (count s)]
      (subs s 0 (min len str-len)))))

(defn make-client-and-backend
  ([] (make-client-and-backend nil))
  ([set-greeting-ch]
   (let [set-greeting-ch (or set-greeting-ch (ca/chan))
         backend (tb/make-backend (make-<get-gw-url "backend")
                                  (make-<get-credentials "backend" "test")
                                  {:silence-log? true})
         options {:handlers {:set-greeting (fn [msg metadata]
                                             (ca/put! set-greeting-ch msg))}
                  :silence-log? true}
         client (cc/make-client (make-<get-gw-url "client")
                                (make-<get-credentials "client1" "test")
                                cg-proto :client options)]
     [client backend])))

(deftest test-make-name-maps
  (let [maps (u/make-name-maps cg-proto :client)
        expected {:rpc-name->req-name {:add :add-rpc-req
                                       :subtract :subtract-rpc-req}
                  :rpc-name->rsp-name {:add :add-rpc-success-rsp
                                       :subtract :subtract-rpc-success-rsp}
                  :msg-name->rec-name
                  {:request-greeting-update :request-greeting-update-msg
                   :request-conn-count :request-conn-count-msg
                   :ping :ping-msg}}]
    (is (= expected maps))))

(deftest test-protocols
  (is (nil? (u/check-protocol calc-protocols/client-gateway-protocol)))
  (is (nil? (u/check-protocol calc-protocols/gateway-backend-protocol))))

(deftest test-calculate
  (au/test-async
   test-timeout
   (ca/go
     (let [[client backend] (make-client-and-backend)]
       (try
         (let [arg [1 2 3]
               rpc-ch (cc/<send-msg client :add arg)
               [v ch] (au/alts? [rpc-ch (ca/timeout rpc-timeout)])]
           (is (= rpc-ch ch))
           (is (= 6.0 v)))
         (finally
           (cc/shutdown client)
           (cc/shutdown backend)))))))

(deftest test-send-msg-to-all-conns
  (au/test-async
   test-timeout
   (ca/go
     (let [client0-ch (ca/chan)
           client1-ch (ca/chan)
           client2-ch (ca/chan)
           [client0 backend] (make-client-and-backend client0-ch)
           client1-opts {:handlers {:set-greeting (fn [msg metadata]
                                                    (ca/put! client1-ch
                                                             msg))}
                         :silence-log? true}
           client2-opts {:handlers {:set-greeting (fn [msg metadata]
                                                    (ca/put! client2-ch
                                                             msg))}
                         :silence-log? true}
           client1 (cc/make-client (make-<get-gw-url "client")
                                   (make-<get-credentials "client1" "test")
                                   cg-proto :client client1-opts)
           client2 (cc/make-client (make-<get-gw-url "client")
                                   (make-<get-credentials "client2" "test")
                                   cg-proto :client client2-opts)
           expected-msg "Hello"]
       (try
         (cc/<send-msg client2 :request-greeting-update nil)
         (let [[v ch] (au/alts? [client0-ch (ca/timeout rpc-timeout)])]
           (is (= client0-ch ch))
           (is (= expected-msg v)))
         (let [[v ch] (au/alts? [client1-ch (ca/timeout rpc-timeout)])]
           (is (= client1-ch ch))
           (is (= expected-msg v)))
         (let [[v ch] (au/alts? [client2-ch (ca/timeout rpc-timeout)])]
           (is (= client2-ch ch))
           (is (= expected-msg v)))
         (finally
           (cc/shutdown client0)
           (cc/shutdown client1)
           (cc/shutdown client2)
           (cc/shutdown backend)))))))

(deftest test-send-msg-to-subject-conns
  (au/test-async
   test-timeout
   (ca/go
     (let [client0-conn-count-chan (ca/chan)
           client1-conn-count-chan (ca/chan)
           client0-opts {:handlers {:subject-conn-count
                                        (fn [msg metadata]
                                          (ca/put! client0-conn-count-chan
                                                   msg))}
                         :silence-log? true}
           client1-opts {:handlers {:subject-conn-count
                                        (fn [msg metadata]
                                          (ca/put! client1-conn-count-chan
                                                   msg))}
                         :silence-log? true}
           client0 (cc/make-client (make-<get-gw-url "client")
                                   (make-<get-credentials "client0" "test")
                                   cg-proto :client client0-opts)
           client1a (cc/make-client (make-<get-gw-url "client")
                                    (make-<get-credentials "client1" "test")
                                    cg-proto :client client1-opts)
           client1b (cc/make-client (make-<get-gw-url "client")
                                    (make-<get-credentials "client1" "test")
                                    cg-proto :client client1-opts)]
       (try
         (cc/send-msg client1a :request-conn-count nil)
         (dotimes [i 2]
           (let [[v ch] (au/alts? [client0-conn-count-chan
                                   client1-conn-count-chan
                                   (ca/timeout rpc-timeout)])]
             (is (= client1-conn-count-chan ch))
             (is (= 2 v))))
         (finally
           (cc/shutdown client0)
           (cc/shutdown client1a)
           (cc/shutdown client1b)))))))

(deftest test-non-existent-rpc
  (au/test-async
   test-timeout
   (ca/go
     (let [client (cc/make-client (make-<get-gw-url "client")
                                  (make-<get-credentials "client0" "test")
                                  cg-proto :client {:silence-log? true})]
       (try
         (au/alts? [(cc/<send-msg client :non-existent "arg")
                    (ca/timeout rpc-timeout)])
         (is (= :did-not-throw :should-not-get-here))
         (catch #?(:clj Exception :cljs js/Error) e
           (is (re-find #"is not a sender for msg `:non-existent`"
                        (lu/get-exception-msg e))))
         (finally
           (cc/shutdown client)))))))

(deftest test-non-existent-msg
  (au/test-async
   test-timeout
   (ca/go
     (let [client (cc/make-client (make-<get-gw-url "client")
                                  (make-<get-credentials "client0" "test")
                                  cg-proto :client {:silence-log? true})]
       (try
         (au/alts? [(cc/send-msg client :non-existent "yo")
                    (ca/timeout rpc-timeout)])
         (is (= :did-not-throw :should-not-get-here))
         (catch #?(:clj Exception :cljs js/Error) e
           (is (re-find #"is not a sender for msg `:non-existent`"
                        (lu/get-exception-msg e))))
         (finally
           (cc/shutdown client)))))))

(deftest test-set-handler-for-non-existent-rpc
  (au/test-async
   test-timeout
   (ca/go
     (let [client (cc/make-client (make-<get-gw-url "client")
                                  (make-<get-credentials "client0" "test")
                                  cg-proto :client {:silence-log? true})]
       (try
         (cc/set-handler client :non-existent (constantly nil))
         (is (= :did-not-throw :should-not-get-here))
         (catch #?(:clj Exception :cljs js/Error) e
           (is (re-find #"is not a sender for msg `:non-existent`"
                        (lu/get-exception-msg e))))
         (finally
           (cc/shutdown client)))))))

(deftest test-set-msg-handler
  (au/test-async
   test-timeout
   (ca/go
     (let [client (cc/make-client (make-<get-gw-url "client")
                                  (make-<get-credentials "client0" "test")
                                  cg-proto :client {:silence-log? true})
           client-chan (ca/chan)
           handler (fn [msg metadata]
                     (when (nil? msg)
                       (ca/put! client-chan :nil)))]
       (try
         (cc/set-handler client :pong handler)
         (cc/send-msg client :ping nil)
         (let [[v ch] (au/alts? [client-chan (ca/timeout rpc-timeout)])]
           (is (= client-chan ch))
           (is (= :nil v)))
         (finally
           (cc/shutdown client)))))))

(deftest test-set-handler-for-non-existent-msg
  (au/test-async
   test-timeout
   (ca/go
     (let [client (cc/make-client (make-<get-gw-url "client")
                                  (make-<get-credentials "client0" "test")
                                  cg-proto :client {:silence-log? true})]
       (try
         (cc/set-handler client :non-existent (constantly nil))
         (is (= :did-not-throw :should-not-get-here))
         (catch #?(:clj Exception :cljs js/Error) e
           (is (re-find #"is not a sender for msg `:non-existent`"
                        (lu/get-exception-msg e))))
         (finally
           (cc/shutdown client)))))))
