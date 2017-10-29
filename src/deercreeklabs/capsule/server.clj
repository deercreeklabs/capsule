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
  (start [this]
    (ts/start tube-server))

  (stop [this]
    (ts/stop tube-server)))

(defn make-route [endpoint]
  (let [path (endpoint/get-path endpoint)
        connection-handler (fn [tube-conn conn-id path]
                             (endpoint/on-connect endpoint tube-conn conn-id))]
    [path connection-handler]))

(defn make-routes [api-impls]
  ["/" (apply hashmap (mapcat make-route api-impls))])

(defn make-on-server-connect [routes]
  (fn on-server-connect [conn conn-id path]
    (debugf "Got conn from %s on %s" conn-id path)
    (let [{:keys [handler]} (bidi/match-route routes path)]
      (if conn-handler
        (conn-handler conn conn-id path)
        (errorf "No handler matches for path %s" path)))))

(def default-port 8080)

(s/defn make-server :- (s/protocol ICapsuleServer)
  ([endpoints :- [(protocol u/IEndpoint)]]
   (run-server endpoints default-port))
  ([endpoints :- [(protocol u/IEndpoint)]
    port :- s/Int]
   (let [keystore-path (System/getenv "TUBE_JKS_KEYSTORE_PATH")
         keystore-password (System/getenv "TUBE_JKS_KEYSTORE_PASSWORD")
         routes (make-routes api-impls)
         on-connect (make-on-server-connect routes)
         on-disconnect (fn [conn-id code reason]
                         (debugf "Conn to %s disconnected (Code: %s Reason: %s)"
                                 conn-id code reason))
         compression-type :smart
         tube-server (ts/make-tube-server port keystore-path keystore-password
                                          on-connect on-disconnect
                                          compression-type)]
     (->CapsuleServer tube-server))))
