(ns deercreeklabs.capsule.endpoint
  (:require
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.capsule.server-connection :as sc]
   [deercreeklabs.capsule.utils :as u]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   [deercreeklabs.tube.connection :as tc]
   [schema.core :as s]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]]))

(defprotocol IEndpoint
  (get-path [this])
  (on-connect [this tube-conn conn-id])
  (send-event-to-all-conns [this event-name event])
  (send-event-to-conn [this conn-id event-name event])
  (send-event-to-subject-conns [this subject-id event-name event]))

(defrecord Endpoint [path <authenticator api roles-to-rpcs handlers msg-schema
                     *conn-id->conn *subject-id->authenticated-conns]
  IEndpoint
  (get-path [this]
    path)

  (on-connect [this tube-conn conn-id]
    (let [server-conn (sc/make-server-connection
                       tube-conn api roles-to-rpcs handlers <authenticator
                       *subject-id->authenticated-conns)]
      (swap! *conn-id->conn assoc conn-id server-conn)
      (tc/set-on-rcv tube-conn (fn [tube-conn data]
                                 (sc/on-rcv server-conn tube-conn data)))
      (tc/set-on-close tube-conn
                       (fn [tube-conn code reason]
                         (debugf "Connection to %s closing. Code: %s Reason: %s"
                                 conn-id code reason)
                         (swap! *conn-id->conn dissoc conn-id)
                         (when-let [subject-id (sc/get-subject-id server-conn)]
                           (swap! *subject-id->authenticated-conns
                                  dissoc subject-id))))))

  (send-event-to-all-conns [this event-name event]
    ;; TODO: Parallelize this?
    (doseq [[conn-id conn] @*conn-id->conn]
      (sc/send-event conn event-name event)))

  (send-event-to-conn [this conn-id event-name event]
    (let [conn (@*conn-id->conn conn-id)]
      (sc/send-event conn event-name event)))

  (send-event-to-subject-conns [this subject-id event-name event]
    (let [conns (@*subject-id->authenticated-conns subject-id)]
      (doseq [conn conns]
        (sc/send-event conn event-name event)))))

(def default-endpoint-options
  {:path ""
   :<authenticator (fn [subject-id credential]
                     (au/go
                       false))})

(s/defn make-endpoint :- (s/protocol IEndpoint)
  ([api :- (s/protocol u/IAPI)
    roles-to-rpcs :- u/RolesToRpcs
    handlers :- u/HandlerMap]
   (make-endpoint api handlers default-endpoint-options))
  ([api :- (s/protocol u/IAPI)
    roles-to-rpcs :- u/RolesToRpcs
    handlers :- u/HandlerMap
    opts :- u/EndpointOptions]
   (let [opts (merge default-endpoint-options opts)
         {:keys [path <authenticator]} opts
         msg-schema (u/get-msg-schema api)
         *conn-id->conn (atom {})
         *subject-id->authenticated-conns (atom {})]
     (->Endpoint path <authenticator api roles-to-rpcs handlers msg-schema
                 *conn-id->conn *subject-id->authenticated-conns))))
