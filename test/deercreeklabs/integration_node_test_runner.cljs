(ns deercreeklabs.integration-node-test-runner
  (:require
   [cljs.nodejs :as nodejs]
   [cljs.test :as test :refer-macros [run-tests]]
   [deercreeklabs.integration.integration-test]))

(nodejs/enable-util-print!)

(defn -main [& args]
  (run-tests 'deercreeklabs.integration.integration-test))

(set! *main-cli-fn* -main)
