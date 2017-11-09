(def externs ["capsule_externs.js"])

(def compiler-defaults
  {:npm-deps {:websocket "1.0.25"}
   :install-deps true
   :parallel-build true
   :static-fns true
   ;; :pseudo-names true
   ;; :pretty-print true
   ;; :infer-externs true
   })

(defn make-build-conf [id target-kw build-type-kw opt-level main]
  (let [build-type-str (name build-type-kw)
        target-str (if target-kw
                     (name target-kw)
                     "")
        node? (= :node target-kw)
        source-paths (case build-type-kw
                       :build ["src"]
                       :test ["src" "test"])
        build-name (str target-str "_" build-type-str "_" (name opt-level))
        output-name (case build-type-kw
                      :build "main.js"
                      :test "test_main.js")
        output-dir (str "target/" build-type-str "/" build-name)
        output-to (str output-dir "/" output-name)
        source-map (if (= :none opt-level)
                     true
                     (str output-dir "/map.js.map"))
        compiler (cond-> compiler-defaults
                   true (assoc :optimizations opt-level
                               :output-to output-to
                               :output-dir output-dir
                               :source-map source-map)
                   main (assoc :main main)
                   (= :advanced opt-level) (assoc :externs externs)
                   node? (assoc :target :nodejs))
        node-test? (and node? (= :test build-type-kw))]
    (cond-> {:id id
             :source-paths source-paths
             :compiler compiler}
      node-test? (assoc :notify-command ["node" output-to]))))

(defproject deercreeklabs/capsule "0.2.0"
  :description "A semantic layer for tube"
  :url "http://www.deercreeklabs.com"
  :license {:name "Apache License, Version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}
  :lein-release {:scm :git
                 :deploy-via :clojars}

  ;; For testing
  :main deercreeklabs.capsule.calc
  :pedantic? :abort

  :profiles
  {:dev
   {:global-vars {*warn-on-reflection* true}
    :source-paths ["dev" "src"]
    :repl-options {:init-ns user}
    :plugins
    [[lein-ancient "0.6.14"]
     [lein-cljsbuild "1.1.7"
      :exclusions [org.apache.commons/commons-compress org.clojure/clojure]]
     [lein-cloverage "1.0.10" :exclusions [org.clojure/clojure]]
     [lein-doo "0.1.8"]
     [lein-npm "0.6.2" :exclusions [com.fasterxml.jackson.core/jackson-core]]
     ;; Because of confusion with a defunct project also called
     ;; lein-release, we exclude lein-release from lein-ancient.
     [lein-release "1.0.9" :upgrade false :exclusions [org.clojure/clojure]]]
    :dependencies
    [[doo "0.1.8"]
     [org.clojure/tools.namespace "0.2.11"]]}}

  :npm {:devDependencies [[karma "1.7.1"]
                          [karma-chrome-launcher "2.2.0"]
                          [karma-cljs-test "0.1.0"]
                          [karma-firefox-launcher "1.0.1"]
                          [source-map-support "0.5.0"]]}

  :dependencies
  [[bidi "2.1.2" :exclusions [prismatic/schema]]
   [binaryage/oops "0.5.6"]
   [com.fzakaria/slf4j-timbre "0.3.7"]
   [com.taoensso/timbre "4.10.0"]
   [deercreeklabs/async-utils "0.1.4"]
   [deercreeklabs/baracus "0.1.1"]
   [deercreeklabs/lancaster "0.1.8"
    :exclusions [org.apache.commons/commons-compress
                 org.clojure/clojure org.slf4j/slf4j-api]]
   [deercreeklabs/log-utils "0.1.1"]
   [deercreeklabs/stockroom "0.1.11"]
   [deercreeklabs/tube "0.1.9" :exclusions [org.slf4j/slf4j-api]]
   [org.clojure/clojure "1.9.0-beta4"]
   [org.clojure/clojurescript "1.9.946"]
   [org.clojure/core.async "0.3.443"]
   [prismatic/schema "1.1.7"]]

  :cljsbuild
  {:builds
   [~(make-build-conf "node-test-none" :node :test :none
                      "deercreeklabs.node-test-runner")
    ~(make-build-conf "node-test-simple" :node :test :simple
                      "deercreeklabs.node-test-runner")
    ~(make-build-conf "node-test-adv" :node :test :advanced
                      "deercreeklabs.node-test-runner")
    ~(make-build-conf "doo-test-none" :doo :test :none
                      "deercreeklabs.doo-test-runner")
    ~(make-build-conf "doo-test-simple" :doo :test :simple
                      "deercreeklabs.doo-test-runner")
    ~(make-build-conf "doo-test-adv" :doo :test :advanced
                      "deercreeklabs.doo-test-runner")
    ~(make-build-conf "build-adv" nil :build :advanced
                      nil)]}

  :aliases
  {"auto-test-cljs" ["do"
                     "clean,"
                     "cljsbuild" "auto" "node-test-none"]
   "auto-test-cljs-simple" ["do"
                            "clean,"
                            "cljsbuild" "auto" "node-test-simple"]
   "auto-test-cljs-adv" ["do"
                         "clean,"
                         "cljsbuild" "auto" "node-test-adv"]
   "chrome-test" ["do"
                  "clean,"
                  "doo" "chrome" "doo-test-adv"]})
