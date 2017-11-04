(ns deercreeklabs.capsule.server-connection
  (:require
   [cheshire.core :as json]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.capsule.api :as api]
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

(defn <handle-msg [tube-conn api subject-id handlers msg-info
                   client-schema-pcf]
  (au/go
    (let [{:keys [msg-type msg-name msg-id msg]} msg-info
          k (case msg-type
              :rpc-req :rpcs
              :event :events)]
      (if-let [<handler (get-in handlers [k msg-name])]
        (try
          (let [metadata (u/sym-map subject-id msg-id)
                rsp (au/<? (<handler msg metadata))]
            (when (and (= :rpc-req msg-type) rsp)
              (u/send-msg tube-conn api :rpc-success-rsp msg-name msg-id rsp)))
          (catch Exception e
            (let [msg {:rpc-name msg-name
                       :rpc-id-str (ba/byte-array->b64 msg-id)
                       :rpc-arg (str msg)
                       :failure-type :server-exception
                       :error-str (lu/get-exception-msg)}]
              (errorf "Exception handling RPC.\nInfo:\n%s\nStacktrace:%s"
                      msg (lu/get-exception-stacktrace e))
              (u/send-msg tube-conn api :rpc-failure-rsp :rpc-failure-rsp
                          msg-id msg))))
        (errorf "No handler found for %s" (dissoc msg-info :msg))))))

(defn send-schema-pcf [tube-conn api]
  (->> api
       (api/get-msg-schema)
       (l/get-parsing-canonical-form)
       (l/serialize l/string-schema)
       (tc/send tube-conn)))

(defn <handle-login-req
  [server-conn tube-conn api roles-to-rpcs msg-id msg <authenticator *subject-id
   *subject-id->authenticated-conns *authorized-rpcs]
  (au/go
    (let [{:keys [subject-id credential]} msg
          roles (au/<? (<authenticator subject-id credential))]
      (when roles
        (reset! *subject-id subject-id)
        (reset! *authorized-rpcs (reduce (fn [acc role]
                                           (clojure.set/union
                                            acc (roles-to-rpcs role)))
                                         #{} roles))
        (swap! *subject-id->authenticated-conns update subject-id
               (fn [old-conns]
                 (if old-conns
                   (conj old-conns server-conn)
                   #{server-conn}))))
      (u/send-msg tube-conn api :login-rsp :login-rsp msg-id (boolean roles)))))

(defn init-authorized-rpcs! [*authorized-rpcs roles-to-rpcs]
  (reset! *authorized-rpcs (:public roles-to-rpcs)))

(defn <handle-logout-req
  [server-conn tube-conn api roles-to-rpcs msg-id *subject-id
   *subject-id->authenticated-conns *authorized-rpcs]
  (au/go
    (let [subject-id @*subject-id]
      (reset! *subject-id nil)
      (init-authorized-rpcs! *authorized-rpcs roles-to-rpcs)
      (swap! *subject-id->authenticated-conns update subject-id
             (fn [old-conns]
               (disj old-conns server-conn)))
      (when (zero? (count (@*subject-id->authenticated-conns subject-id)))
        (swap! *subject-id->authenticated-conns dissoc subject-id))
      (u/send-msg tube-conn api :logout-rsp :logout-rsp msg-id true))))

(defn send-unauthorized-msg [tube-conn api msg-info]
  (let [{:keys [msg-name msg-id msg]} msg-info
        err-msg (str "Unauthorized RPC to " msg-name)
        rpc-id-str (ba/byte-array->b64 msg-id)
        failure-msg {:rpc-name (str msg-name)
                     :rpc-id-str rpc-id-str
                     :rpc-arg (str msg)
                     :failure-type :unauthorized
                     :error-str err-msg}]
    (infof "%s (id: %s)" err-msg rpc-id-str)
    (u/send-msg tube-conn api :rpc-failure-rsp :rpc-failure-rsp msg-id
                failure-msg)))

(defrecord ServerConnection [tube-conn api roles-to-rpcs handlers <authenticator
                             *client-schema-pcf *subject-id
                             *subject-id->authenticated-conns *authorized-rpcs]
  IServerConnection
  (on-rcv [this tube-conn data]
    (if-let [client-schema-pcf @*client-schema-pcf]
      (let [subject-id @*subject-id
            msg-info (api/decode api client-schema-pcf data)
            {:keys [msg-type msg-name msg-id msg]} msg-info]
        (case msg-type
          :login-req
          (<handle-login-req this tube-conn api roles-to-rpcs msg-id msg
                             <authenticator *subject-id
                             *subject-id->authenticated-conns *authorized-rpcs)
          :logout-req
          (<handle-logout-req this tube-conn api roles-to-rpcs msg-id
                              *subject-id *subject-id->authenticated-conns
                              *authorized-rpcs)

          (if (@*authorized-rpcs msg-name)
            (<handle-msg tube-conn api subject-id handlers msg-info
                         client-schema-pcf)
            (send-unauthorized-msg tube-conn api msg-info))))
      (reset! *client-schema-pcf
              (l/deserialize l/string-schema
                             (l/get-parsing-canonical-form l/string-schema)
                             data))))

  (send-event [this event-name event]
    (u/send-msg tube-conn api :event (name event-name) (u/make-msg-id) event))

  (get-subject-id [this]
    @*subject-id))

(s/defn make-server-connection :- (s/protocol IServerConnection)
  [tube-conn :- u/TubeConn
   api :- (s/protocol api/IAPI)
   roles-to-rpcs :- u/RolesToRpcs
   handlers :- u/HandlerMap
   <authenticator :- u/Authenticator
   *subject-id->authenticated-conns :- s/Any]
  (let [*client-schema-pcf (atom nil)
        *subject-id (atom nil)
        *authorized-rpcs (atom nil)
        _ (init-authorized-rpcs! *authorized-rpcs roles-to-rpcs)
        server-conn (->ServerConnection
                     tube-conn api roles-to-rpcs handlers <authenticator
                     *client-schema-pcf *subject-id
                     *subject-id->authenticated-conns *authorized-rpcs)]
    (send-schema-pcf tube-conn api)
    server-conn))
