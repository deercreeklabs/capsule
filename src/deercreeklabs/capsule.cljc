(ns deercreeklabs.capsule
  (:require
   [deercreeklabs.capsule.utils :as u]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]]))

(defn valid-protocol?
  [protocol]
  (u/valid-protocol? protocol))
