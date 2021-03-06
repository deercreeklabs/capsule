(ns deercreeklabs.node-test-runner
  (:require
   [cljs.nodejs :as nodejs]
   [cljs.test :as test :refer-macros [run-tests]]
   [deercreeklabs.unit.capsule-test]))

(nodejs/enable-util-print!)

(defn -main [& args]
  (run-tests 'deercreeklabs.unit.capsule-test))

(set! *main-cli-fn* -main)
