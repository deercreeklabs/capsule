(ns deercreeklabs.capsule.server-connection
  (:require
   [cheshire.core :as json]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.capsule.utils :as u]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   [deercreeklabs.tube.connection :as tc]
   [schema.core :as s]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]]))

(defprotocol IServerConnection
  (on-rcv [this tube-conn data])
  (send-event [this event-name event])
  (get-subject-id [this]))

(defn <handle-msg [tube-conn api subject-id handlers endpoint k msg-info
                   client-schema-pcf]
  (au/go
    (let [{:keys [msg-type msg-name msg-id msg]} msg-info]
      (if-let [<handler (get-in handlers [k msg-name])]
        (try
          (let [metadata (u/sym-map subject-id msg-id)
                rsp (au/<? (<handler msg endpoint metadata))]
            (when (and (= :rpc-req msg-type) rsp)
              (u/send-msg tube-conn api :rpc-success-rsp msg-name msg-id rsp)))
          (catch Exception e
            (let [info {:rpc-name msg-name
                        :rpc-id-str (ba/byte-array->b64 msg-id)
                        :rpc-arg (str msg)
                        :error-str (lu/get-exception-msg)}]
              (errorf "Exception handling RPC.\nInfo:\n%s\nStacktrace:%s"
                      info (lu/get-exception-stacktrace e))
              (u/send-msg tube-conn api :rpc-failure-rsp msg-name
                          msg-id info))))
        (errorf "No handler found for %s" (dissoc msg-info :msg))))))

(defn send-schema-pcf [tube-conn api]
  (->> api
       (u/get-msg-schema)
       (l/get-parsing-canonical-form)
       (l/serialize l/string-schema)
       (tc/send tube-conn)))

(defn <handle-login-req
  [server-conn tube-conn api msg-id msg <authenticator *subject-id
   *subject-id->authenticated-conns]
  (au/go
    (let [{:keys [subject-id credential]} msg
          was-successful (au/<? (<authenticator subject-id credential))]
      (when was-successful
        (reset! *subject-id subject-id)
        (swap! *subject-id->authenticated-conns update subject-id
               (fn [old-conns]
                 (if old-conns
                   (conj old-conns server-conn)
                   #{server-conn}))))
      (u/send-msg tube-conn api :login-rsp :login msg-id was-successful))))

(defn send-unauthorized-msg [tube-conn api msg-id]
  (u/send-msg tube-conn api :auth :unauthorized msg-id nil))

(defrecord ServerConnection [tube-conn api handlers endpoint <authenticator
                             *client-schema-pcf *subject-id
                             *subject-id->authenticated-conns]
  IServerConnection
  (on-rcv [this tube-conn data]
    (if-let [client-schema-pcf @*client-schema-pcf]
      (let [subject-id @*subject-id
            msg-info (u/decode api client-schema-pcf data)
            {:keys [msg-type msg-name msg-id msg]} msg-info
            k (case msg-type
                :rpc-req :rpcs
                :event :events)]
        (if (= :login-req msg-type)
          (<handle-login-req this tube-conn api msg-id msg <authenticator
                             *subject-id *subject-id->authenticated-conns)
          (if (or subject-id (get-in api [k msg-name :public?]))
            (<handle-msg tube-conn api subject-id handlers endpoint k msg-info
                         client-schema-pcf)
            (send-unauthorized-msg tube-conn api msg-id))))
      (reset! *client-schema-pcf
              (l/deserialize l/string-schema l/string-schema data))))

  (send-event [this event-name event]
    (u/send-msg tube-conn api :event event-name (u/make-msg-id) event))

  (get-subject-id [this]
    @*subject-id))

(s/defn make-server-connection :- (s/protocol IServerConnection)
  [tube-conn :- u/TubeConn
   api :- (s/protocol u/IAPI)
   handlers :- u/HandlerMap
   endpoint :- s/Any
   <authenticator :- u/Authenticator
   *subject-id->authenticated-conns :- s/Any]
  (let [*client-schema-pcf (atom nil)
        *subject-id (atom nil)
        server-conn (->ServerConnection
                     tube-conn api handlers endpoint <authenticator
                     *client-schema-pcf *subject-id
                     *subject-id->authenticated-conns)]
    (send-schema-pcf tube-conn api)
    server-conn))
