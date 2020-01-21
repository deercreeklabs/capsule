(ns deercreeklabs.capsule.server
  (:require
   [bidi.bidi :as bidi]
   [deercreeklabs.capsule.endpoint :as endpoint]
   [deercreeklabs.capsule.logging :as logging :refer [debug error info]]
   [deercreeklabs.capsule.utils :as u]
   [deercreeklabs.tube.connection :as tc]
   [deercreeklabs.tube.server :as ts]
   [schema.core :as s]))

(def default-port 8080)

(defn route [endpoint]
  (let [path (endpoint/get-path endpoint)
        connection-handler (fn [tube-conn]
                             (endpoint/on-connect endpoint tube-conn))]
    [path connection-handler]))

(defn endpoints->routes [endpoints]
  ["/" (apply hash-map (mapcat route endpoints))])

(defn on-server-connect [routes]
  (fn on-server-connect [conn conn-req conn-count]
    (let [uri (tc/get-uri conn)
          {:keys [handler]} (bidi/match-route routes uri)]
      (if handler
        (handler conn)
        (error (str "No handler matches for path " uri))))))

(s/defn server :- (s/fn [])
  ([endpoints :- [(s/protocol endpoint/IEndpoint)]]
   (server endpoints default-port {}))
  ([endpoints :- [(s/protocol endpoint/IEndpoint)]
    port :- s/Int]
   (server endpoints port {}))
  ([endpoints :- [(s/protocol endpoint/IEndpoint)]
    port :- s/Int
    options :- {s/Keyword s/Any}]
   (let [routes (endpoints->routes endpoints)
         ws-on-connect (on-server-connect routes)
         logger (fn [level msg]
                  (logging/log* {:level level
                                 :ms (u/get-current-time-ms)
                                 :msg (str "TUBE: " msg)}))
         {:keys [handle-http http-timeout-ms]} options
         config (u/sym-map handle-http http-timeout-ms logger ws-on-connect)
         stop-server (ts/tube-server port config)]
     stop-server)))
