(ns deercreeklabs.capsule
  (:require
   [deercreeklabs.capsule.utils :as u]
   [schema.core :as s]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]]))

(s/defn valid-api? :- s/Bool
  [api :- s/Any]
  (u/valid-api? api))
