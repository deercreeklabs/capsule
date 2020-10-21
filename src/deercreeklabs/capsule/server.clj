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
  (let [path (endpoint/get-path endpoint)]
    [path (constantly endpoint)]))

(defn endpoints->routes [endpoints]
  ["/" (apply hash-map (mapcat route endpoints))])

(defn on-server-connect [routes]
  (fn on-server-connect [conn conn-req conn-count]
    (let [uri (tc/get-uri conn)
          endpoint ((:handler (bidi/match-route routes uri)))]
      (if endpoint
        (endpoint/on-connect endpoint conn)
        (error (str "No endpoint matches for path " uri))))))

(defn on-server-disconnect [routes]
  (fn on-server-disconnect [conn code reason conn-count]
    (let [uri (tc/get-uri conn)
          endpoint ((:handler (bidi/match-route routes uri)))]
      (if endpoint
        (endpoint/on-disconnect endpoint conn code reason)
        (error (str "No endpoint matches for path " uri))))))


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
         on-connect (on-server-connect routes)
         on-disconnect (on-server-disconnect routes)
         logger (fn [level msg]
                  (logging/log* {:level level
                                 :ms (u/get-current-time-ms)
                                 :msg (str "TUBE: " msg)}))
         {:keys [certificate-str private-key-str]} options
         config (u/sym-map logger on-connect on-disconnect
                           certificate-str private-key-str)
         stop-server (ts/tube-server port config)]
     stop-server)))
