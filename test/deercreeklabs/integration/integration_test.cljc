(ns deercreeklabs.integration.integration-test
  (:require
   [clojure.core.async :as ca]
   [clojure.test :refer [deftest is]]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.capsule.calc-protocols :as calc-protocols]
   [deercreeklabs.capsule.client :as cc]
   [deercreeklabs.capsule.logging :refer [debug]]
   [deercreeklabs.capsule.test-backend :as tb]
   [deercreeklabs.capsule.utils :as u]
   [schema.core :as s])
  #?(:clj
     (:import
      (clojure.lang ExceptionInfo))))

;; Use this instead of fixtures, which are hard to make work w/ async testing.
(s/set-fn-validation! true)

(u/configure-logging :debug)

(defn make-get-gw-url [endpoint]
  (fn get-gw-url []
    (str "ws://localhost:8080/" endpoint)))

(defn make-get-credentials [subject-id subject-secret]
  (fn get-credentials []
    (u/sym-map subject-id subject-secret)))

(def rpc-timeout #?(:cljs 10000 :clj 2000))
(def test-timeout #?(:cljs 20000 :clj 2000))
(def cg-proto calc-protocols/client-gateway-protocol)

(defn slice-str [s len]
  (when s
    (let [str-len (count s)]
      (subs s 0 (min len str-len)))))

(defn <client-and-backend
  ([] (<client-and-backend nil))
  ([set-greeting-ch]
   (au/go
     (let [set-greeting-ch (or set-greeting-ch (ca/chan))
           backend (tb/backend (make-get-gw-url "backend")
                               (make-get-credentials "backend" "test")
                               {:silence-log? false})
           connected-ch (ca/chan)
           options {:handlers {:set-greeting (fn [msg metadata]
                                               (ca/put! set-greeting-ch msg))}
                    :on-connect (fn [conn]
                                  (ca/put! connected-ch true))
                    :silence-log? false}
           client (cc/client (make-get-gw-url "client")
                             (make-get-credentials "client1" "test")
                             cg-proto :client options)]
       (au/<? connected-ch)
       [client backend]))))

(deftest test-calculate
  (au/test-async
   test-timeout
   (ca/go
     (let [[client backend] (au/<? (<client-and-backend))]
       (try
         (let [arg [1 2 3]
               rpc-ch (cc/<send-msg client :add arg)
               [v ch] (au/alts? [rpc-ch (ca/timeout rpc-timeout)])]
           (is (= rpc-ch ch))
           (is (= 6.0 v)))
         (finally
           (cc/shutdown client)
           (cc/shutdown backend)))))))

(deftest test-one-way-w-channel
  (au/test-async
   test-timeout
   (ca/go
     (let [client (cc/client (make-get-gw-url "client")
                             (make-get-credentials "client1" "test")
                             cg-proto :client {:silence-log? true})]
       (try
         (let [msg-ch (cc/<send-msg client :request-greeting-update nil)
               [v ch] (au/alts? [msg-ch (ca/timeout 1000)])]
           (is (= msg-ch ch))
           (is (nil? v)))
         (finally
           (cc/shutdown client)))))))

(deftest ^:this test-send-msg-to-all-conns
  (au/test-async
   test-timeout
   (ca/go
     (let [client0-ch (ca/chan)
           client1-ch (ca/chan)
           client2-ch (ca/chan)
           connected1-ch (ca/chan)
           connected2-ch (ca/chan)
           [client0 backend] (au/<? (<client-and-backend client0-ch))
           client1-opts {:handlers {:set-greeting (fn [msg metadata]
                                                    (ca/put! client1-ch msg))}
                         :on-connect (fn [conn]
                                       (ca/put! connected1-ch true))
                         :silence-log? false}
           client2-opts {:handlers {:set-greeting (fn [msg metadata]
                                                    (ca/put! client2-ch msg))}
                         :on-connect (fn [conn]
                                       (ca/put! connected2-ch true))
                         :silence-log? false}
           client1 (cc/client (make-get-gw-url "client")
                              (make-get-credentials "client1" "test")
                              cg-proto :client client1-opts)
           client2 (cc/client (make-get-gw-url "client")
                              (make-get-credentials "client2" "test")
                              cg-proto :client client2-opts)
           expected-msg "Hello"]
       ;; Wait for everyone to connect
       (is (au/<? connected1-ch))
       (is (au/<? connected2-ch))
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
           client0 (cc/client (make-get-gw-url "client")
                              (make-get-credentials "client0" "test")
                              cg-proto :client client0-opts)
           client1a (cc/client (make-get-gw-url "client")
                               (make-get-credentials "client1" "test")
                               cg-proto :client client1-opts)
           client1b (cc/client (make-get-gw-url "client")
                               (make-get-credentials "client1" "test")
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
     (let [client (cc/client (make-get-gw-url "client")
                             (make-get-credentials "client0" "test")
                             cg-proto :client {:silence-log? true})]
       (is (thrown-with-msg?
            #?(:clj ExceptionInfo :cljs js/Error)
            #"is not a sender for msg `:non-existent`"
            (au/alts? [(cc/<send-msg client :non-existent "arg")
                       (ca/timeout rpc-timeout)])))
       (cc/shutdown client)))))

(deftest test-non-existent-msg
  (au/test-async
   test-timeout
   (ca/go
     (let [client (cc/client (make-get-gw-url "client")
                             (make-get-credentials "client0" "test")
                             cg-proto :client {:silence-log? true})]
       (is (thrown-with-msg?
            #?(:clj ExceptionInfo :cljs js/Error)
            #"is not a sender for msg `:non-existent`"
            (au/alts? [(cc/send-msg client :non-existent "yo")
                       (ca/timeout rpc-timeout)])))
       (cc/shutdown client)))))

(deftest test-set-handler-for-non-existent-rpc
  (au/test-async
   test-timeout
   (ca/go
     (let [client (cc/client (make-get-gw-url "client")
                             (make-get-credentials "client0" "test")
                             cg-proto :client {:silence-log? true})]
       (is (thrown-with-msg?
            #?(:clj ExceptionInfo :cljs js/Error)
            #"is not a sender for msg `:non-existent`"
            (cc/set-handler client :non-existent (constantly nil))))
       (cc/shutdown client)))))

(deftest test-set-msg-handler
  (au/test-async
   test-timeout
   (ca/go
     (let [client (cc/client (make-get-gw-url "client")
                             (make-get-credentials "client0" "test")
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
     (let [client (cc/client (make-get-gw-url "client")
                             (make-get-credentials "client0" "test")
                             cg-proto :client {:silence-log? true})]
       (is (thrown-with-msg?
            #?(:clj ExceptionInfo :cljs js/Error)
            #"is not a sender for msg `:non-existent`"
            (cc/set-handler client :non-existent (constantly nil))))
       (cc/shutdown client)))))

(deftest test-rpc-inside-rpc
  (au/test-async
   test-timeout
   (ca/go
     (let [client (cc/client (make-get-gw-url "client")
                             (make-get-credentials "client0" "test")
                             cg-proto :client {:silence-log? true})
           client-chan (ca/chan)
           handler (fn [arg metadata]
                     42)]
       (try
         (cc/set-handler client :arg-string-to-int handler)
         (let [i (au/<? (cc/<send-msg client :invert "42"))]
           (is (= -42 i)))
         (finally
           (cc/shutdown client)))))))
