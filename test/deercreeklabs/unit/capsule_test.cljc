(ns deercreeklabs.unit.capsule-test
  (:require
   [clojure.core.async :as ca]
   [clojure.test :refer [deftest is]]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.capsule.calc-protocols :as calc-protocols]
   [deercreeklabs.capsule.client :as cc]
   [deercreeklabs.capsule.test-backend :as tb]
   [deercreeklabs.capsule.utils :as u]
   [schema.core :as s])
  #?(:clj
     (:import
      (clojure.lang ExceptionInfo))))

;; Use this instead of fixtures, which are hard to make work w/ async testing.
(s/set-fn-validation! true)

(deftest test-name-maps
  (let [maps (u/name-maps calc-protocols/client-gateway-protocol :client)
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

(deftest test-bad-prototocol-roles-type
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"roles key must be a sequence of exactly two keywords"
       (u/check-protocol {:roles {:a :b :c :d}
                          :msgs {}}))))

(deftest test-bad-prototocol-roles-number
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"roles key must be a sequence of exactly two keywords"
       (u/check-protocol {:roles []
                          :msgs {}}))))

(deftest test-bad-prototocol-role-either
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #":roles key must not contain  the keyword `:either`"
       (u/check-protocol {:roles [:server :either]
                          :msgs {}}))))
