(ns deercreeklabs.capsule
  (:require
   [deercreeklabs.capsule.utils :as u]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]]))

(defn check-protocol
  [protocol]
  (u/check-protocol protocol))
