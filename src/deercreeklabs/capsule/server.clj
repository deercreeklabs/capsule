(ns deercreeklabs.capsule.server
  (:require
   [bidi.bidi :as bidi]
   [deercreeklabs.capsule.endpoint :as endpoint]
   [deercreeklabs.capsule.utils :as u]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   [deercreeklabs.tube.server :as ts]
   [schema.core :as s]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]]))

(defprotocol ICapsuleServer
  (start [this])
  (stop [this]))

(defrecord CapsuleServer [tube-server]
  ICapsuleServer
  (start [this]
    (ts/start tube-server))

  (stop [this]
    (ts/stop tube-server)))

(defn make-route [endpoint]
  (let [path (endpoint/get-path endpoint)
        connection-handler (fn [tube-conn conn-id path]
                             (endpoint/on-connect endpoint tube-conn conn-id))]
    [path connection-handler]))

(defn make-routes [endpoints]
  ["/" (apply hash-map (mapcat make-route endpoints))])

(defn make-on-server-connect [routes]
  (fn on-server-connect [conn conn-id path]
    (let [{:keys [handler]} (bidi/match-route routes path)]
      (if handler
        (handler conn conn-id path)
        (errorf "No handler matches for path %s" path)))))

(def default-port 8080)

(s/defn make-server :- (s/protocol ICapsuleServer)
  ([endpoints :- [(s/protocol endpoint/IEndpoint)]]
   (make-server endpoints default-port))
  ([endpoints :- [(s/protocol endpoint/IEndpoint)]
    port :- s/Int]
   (let [routes (make-routes endpoints)
         on-connect (make-on-server-connect routes)
         on-disconnect (fn [conn-id code reason])
         compression-type :smart
         tube-server (ts/make-tube-server port on-connect on-disconnect
                                          compression-type)]
     (->CapsuleServer tube-server))))
