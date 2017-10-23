(ns deercreeklabs.capsule.client
  (:require
   [deercreeklabs.capsule.utils :as u]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.log-utils :as lu :refer [debugs]]
   [schema.core :as s]
   [taoensso.timbre :as timbre :refer [debugf errorf infof]]))

(def default-timeout-ms 3000)

(defprotocol ICapsuleClient
  (log-in [this identifier credential cb])
  (<log-in [this identifier credential])
  (send-rpc
    [this rpc-name arg]
    [this rpc-name arg cb]
    [this rpc-name arg cb timeout-ms])
  (<send-rpc
    [this rpc-name arg]
    [this rpc-name arg timeout-ms])
  (bind-event [this event-name event-handler]))

(defrecord CapsuleClient [*logged-in?]
  ICapsuleClient
  (log-in [this identifier credential cb]
    ;; TODO: Implement
    )

  (<log-in [this identifier credential]
    (let [ch (ca/chan)
          cb #(ca/put! ch %)]
      (log-in this identifier credential cb)
      ch))

  (send-rpc [this rpc-name arg]
    (send-rpc this rpc-name arg nil default-timeout-ms))

  (send-rpc [this rpc-name arg cb]
    (send-rpc this rpc-name arg cb default-timeout-ms))

  (send-rpc [this rpc-name arg cb timeout-ms]
    ;; TODO: Implement
    )

  (<send-rpc [this rpc-name arg]
    (<send-rpc this rpc-name arg default-timeout-ms))

  (<send-rpc [this rpc-name arg timeout-ms]
    (let [ch (ca/chan)
          cb #(ca/put! ch %)]
      (send-rpc this rpc-name arg cb timeout-ms)
      ch))

  (bind-event [this event-name event-handler]
    ;; TODO: Implement
    ))

(s/defn make-client :- (s/protocol ICapsuleClient)
  ([api :- u/Api]
   (make-client api nil nil))
  ([api :- u/Api
    identifier :- s/Str
    credential :- s/Str]
   (let [*logged-in? (atom false)]
     (->CapsuleClient *logged-in?))))
