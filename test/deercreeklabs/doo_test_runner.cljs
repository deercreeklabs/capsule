(ns deercreeklabs.doo-test-runner
  (:require
   [doo.runner :refer-macros [doo-tests]]
   [deercreeklabs.capsule-test]))

(enable-console-print!)

(doo-tests 'deercreeklabs.capsule-test)
