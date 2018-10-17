(ns deercreeklabs.capsule.server
  (:require
   [bidi.bidi :as bidi]
   [deercreeklabs.capsule.endpoint :as endpoint]
   [deercreeklabs.capsule.utils :as u]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   [deercreeklabs.tube.connection :as tc]
   [deercreeklabs.tube.server :as ts]
   [schema.core :as s]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]]))

(def default-port 8080)

(defprotocol ICapsuleServer
  (start [this])
  (stop [this])
  (get-conn-counts [this]))

(defrecord CapsuleServer [tube-server endpoints]
  ICapsuleServer
  (start [this]
    (ts/start tube-server))

  (stop [this]
    (ts/stop tube-server))

  (get-conn-counts [this]
    (reduce (fn [acc endpoint]
              (let [path (endpoint/get-path endpoint)
                    conn-count (endpoint/get-conn-count endpoint)]
                (assoc acc path conn-count)))
            {} endpoints)))

(defn route [endpoint]
  (let [path (endpoint/get-path endpoint)
        connection-handler (fn [tube-conn]
                             (endpoint/on-connect endpoint tube-conn))]
    [path connection-handler]))

(defn routes [endpoints]
  ["/" (apply hash-map (mapcat route endpoints))])

(defn on-server-connect [routes]
  (fn on-server-connect [conn]
    (let [uri (tc/get-uri conn)
          {:keys [handler]} (bidi/match-route routes uri)]
      (if handler
        (handler conn)
        (errorf "No handler matches for path %s" uri)))))

(s/defn server :- (s/protocol ICapsuleServer)
  ([endpoints :- [(s/protocol endpoint/IEndpoint)]]
   (server endpoints default-port {}))
  ([endpoints :- [(s/protocol endpoint/IEndpoint)]
    port :- s/Int
    options :- {s/Keyword s/Any}]
   (let [routes (routes endpoints)
         on-connect (on-server-connect routes)
         on-disconnect (fn [conn code reason])
         compression-type :smart
         tube-server-options (select-keys options
                                          [:handle-http :http-timeout-ms])
         tube-server (ts/tube-server port on-connect on-disconnect
                                     compression-type tube-server-options)]
     (->CapsuleServer tube-server endpoints))))
