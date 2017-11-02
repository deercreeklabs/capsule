(ns deercreeklabs.doo-test-runner
  (:require
   [doo.runner :refer-macros [doo-tests]]
   [deercreeklabs.client-test]))

(enable-console-print!)

(doo-tests 'deercreeklabs.client-test)
