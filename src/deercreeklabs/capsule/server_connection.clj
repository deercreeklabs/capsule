(ns deercreeklabs.capsule.server-connection
  (:require
   [deercreeklabs.capsule.utils :as u]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.log-utils :as lu :refer [debugs]]))

(defprotocol IServerConnection
  (on-rcv [this tube-conn data])
  (send-event [this event])
  (get-subject-id [this]))

(defn handle-msg [tube-conn handlers data]
  )

(defrecord ServerConnection [tube-conn api handlers msg-schema
                             *handshake-complete?
                             *subject-id]
  IServerConnection
  (on-rcv [this tube-conn data]
    (if @*handshake-complete?
      (handle-msg tube-conn handlers data)
      (let [capsule (l/deserialize u/c-to-s-capsule-schema data)
            {:keys [handshake-req encoded-msg]} capsule
            msg (l/deserialize msg-schema)])))

  (send-event [this event]
    )

  (get-subject-id [this]
    @*subject-id))

(s/defn make-server-connection :- (s/protocol IServerConnection)
  [tube-conn :- u/TubeConn
   api :- u/Api
   handlers :- u/HandlerMap
   msg-schema :- u/AvroSchema]
  (let [msg-schema (make-msg)
        *handshake-complete? (atom false)
        *subject-id (atom nil)]
    (->ServerConnection tube-conn api handlers msg-schema *handshake-complete?
                        *subject-id)))
