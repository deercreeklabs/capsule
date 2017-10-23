(ns deercreeklabs.capsule.endpoint
  (:require
   [deercreeklabs.capsule.server-connection :as sc]
   [deercreeklabs.capsule.utils :as u]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   [deercreeklabs.tube.connection :as tc]
   [schema.core :as s]))

(defprotocol IEndpoint
  (get-path [this])
  (authenticate-conn [this server-conn identifier credential])
  (on-connect [this tube-conn conn-id])
  (send-event-to-all-conns [this event])
  (send-event-to-conn [this conn-id event])
  (send-event-to-subject-conns [this subject-id event]))

(defrecord Endpoint [path authenticator api handlers *conn-id->conn
                     *subject-id->authenticated-conns]
  (get-path [this]
    path)

  (authenticate-conn [this server-conn identifier credential]
    (when-let [subject-id (authenticator identifier credential)]
      (swap! *subject-id->authenticated-conns update subject-id
             (fn [old-conns]
               (if old-conns
                 (conj old-conns server-conn)
                 #{server-conn})))
      subject-id))

  (on-connect [this tube-conn conn-id]
    (let [server-conn (sc/make-server-connection tube-conn api handlers)]
      (swap! *conn-id->conn assoc conn-id server-conn)
      (tc/set-on-rcv tube-conn (fn [tube-conn data]
                                 (connection/on-rcv server-conn data)))
      (tc/set-on-close tube-conn
                       (fn [tube-conn code reason]
                         (debugf "Connection to %s closing. Code: %s Reason: %s"
                                 conn-id code reason)
                         (swap! *conn-id->conn dissoc conn-id)
                         (when-let [subject-id (sc/get-subject-id server-conn)]
                           (swap! *subject-id->authenticated-conns
                                  dissoc subject-id))))))

  (send-event-to-all-conns [this event]
    ;; TODO: Parallelize this?
    (doseq [[conn-id conn] @*conn-id->conn]
      (sc/send-event conn event)))

  (send-event-to-conn [this conn-id event]
    (let [conn (@*conn-id->conn conn-id)]
      (sc/send-event conn event)))

  (send-event-to-subject-conns [this subject-id event]
    (let [conns (@*subject-id->authenticated-conns subject-id)]
      (doseq [conn conns]
        (sc/send-event conn event)))))

(defn default-endpoint-options
  {:path ""
   :authenticator (constantly nil)})

(s/defn make-endpoint :- (s/protocol IEndpoint)
  ([api :- u/Api
    handlers :- u/HandlerMap]
   (make-endpoint api handlers default-endpoint-options))
  ([api :- u/Api
    handlers :- u/HandlerMap
    opts :- u/EndpointOptions]
   (let [opts (merge default-endpoint-options opts)
         {:keys [path authenticator]} opts
         *conn-id->conn (atom {})
         *subject-id->authenticated-conns (atom {})]
     (->Endpoint path authenticator api handlers *conn-id->conn
                 *subject-id->authenticated-conns))))
