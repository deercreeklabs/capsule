(ns deercreeklabs.capsule.logging
  (:require
   [clojure.core.async :as ca]
   [clojure.string :as string]
   [net.cgrand.macrovich :as macros])
  #?(:cljs
     (:require-macros deercreeklabs.capsule.logging)))

(def *current-log-level (atom :error))
(def *log-reporters (atom {}))
(def max-log-buffer 10000)
(def log-chan (ca/chan max-log-buffer))
(def log-level->priority
  {:fatal 0
   :error 10
   :warn 20
   :info 30
   :debug 40
   :trace 50})

(defn add-log-reporter! [id reporter-fn]
  (swap! *log-reporters assoc id reporter-fn))

(defn remove-log-reporter! [id]
  (swap! *log-reporters dissoc id))

(defn log-reporter-ids []
  (keys @*log-reporters))

(defn print* [x]
  #?(:clj (do
            (.write *out* x)
            (.flush *out*)) ;; Work around interleaving prob in print
     :cljs (print x)))

(defn println-reporter [info]
  (let [{:keys [column file level line ms msg namespace]} info
        file-or-ns (or file namespace)]
    (print* (str ms " " (string/upper-case (name level)) " "
                 file-or-ns ":" line " " msg "\n"))))

(defn- unknown-level-msg [level]
  (str "Unknown log level `" level "`. Acceptable levels are "
       (string/join " " (keys log-level->priority))))

(defn set-log-level! [level]
  (if (get log-level->priority level)
    (reset! *current-log-level level)
    (throw (ex-info (unknown-level-msg level) {:level level}))))

(defn get-current-time-ms []
  #?(:clj (System/currentTimeMillis)
     :cljs (.getTime (js/Date.))))

(defn ex-msg [e]
  #?(:clj (.toString ^Exception e)
     :cljs (.-message e)))

(defn ex-stacktrace [e]
  #?(:clj (clojure.string/join "\n" (map str (.getStackTrace ^Exception e)))
     :cljs (.-stack e)))

(defn ex-msg-and-stacktrace [e]
  (str "\nException:\n" (ex-msg e) "\nStacktrace:\n" (ex-stacktrace e)))

(defn log* [{:keys [level] :as info}]
  (let [cur-level (get log-level->priority @*current-log-level)
        msg-level (get log-level->priority level)]
    (when (not cur-level)
      (log* {:level :error
             :msg "*current-log-level is not properly set"}))
    (when (not msg-level)
      (log* {:level :error
             :msg (unknown-level-msg msg-level)}))
    (when (>= cur-level msg-level)
      (if-let [reporters @*log-reporters]
        (doseq [[reporter-id reporter] reporters]
          (reporter info))
        ;; Use println as fallback reporter
        (println-reporter info)))))

(defn do-log [level file namespace line column msg]
  `(log* {:column ~column
          :file ~file
          :level ~level
          :line ~line
          :ms (get-current-time-ms)
          :msg ~msg
          :namespace ~namespace}))

(defmacro fatal [msg]
  (let [{:keys [line column]} (meta &form)]
    (do-log :fatal (macros/case :clj *file* :cljs nil) (str *ns*)
            line column msg)))

(defmacro error [msg]
  (let [{:keys [line column]} (meta &form)]
    (do-log :error (macros/case :clj *file* :cljs nil) (str *ns*)
            line column msg)))

(defmacro warn [msg]
  (let [{:keys [line column]} (meta &form)]
    (do-log :warn (macros/case :clj *file* :cljs nil) (str *ns*)
            line column msg)))

(defmacro info [msg]
  (let [{:keys [line column]} (meta &form)]
    (do-log :info (macros/case :clj *file* :cljs nil) (str *ns*)
            line column msg)))

(defmacro debug [msg]
  (let [{:keys [line column]} (meta &form)]
    (do-log :debug (macros/case :clj *file* :cljs nil) (str *ns*)
            line column msg)))

(defmacro trace [msg]
  (let [{:keys [line column]} (meta &form)]
    (do-log :trace (macros/case :clj *file* :cljs nil) (str *ns*)
            line column msg)))

;; TODO: Fix this; it loses file & line number
(defmacro debug-syms [& syms]
  `(debug (str "Symbol map:\n"
               (deercreeklabs.capsule.utils/pprint-str
                (deercreeklabs.capsule.utils/sym-map ~@syms)))))
