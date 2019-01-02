(ns biiwide.deepend.alpha.pool
  (:require [biiwide.deepend.alpha.stats :as stats]
            [biiwide.deepend.alpha.reflect
             :refer [private-field]]
            [clojure.core.specs.alpha :as core]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [clojure.test.check.generators :as gen])
  (:import  [java.util.concurrent TimeUnit]
            [io.aleph.dirigiste
             IPool IPool$AcquireCallback IPool$Controller IPool$Generator
             Pool Pools Stats]))


(s/fdef generator?
  :args (s/cat :x any?)
  :ret  boolean?)


(definline generator?
  [x]
  `(instance? IPool$Generator ~x))


(s/def ::generator generator?)
(s/def ::generate fn?)
(s/def ::destroy fn?)


(s/fdef generator
  :args (s/cat :generate ::generate :destroy ::destroy)
  :ret  ::generator)


(defn generator
  [generate destroy]
  (reify
    IPool$Generator
    (generate [_ key]
      (generate key))
    (destroy [_ key obj]
      (destroy key obj))
    clojure.lang.IFn
    (invoke [_ key]
      (generate key))
    (invoke [_ key obj]
      (destroy key obj))))


(defmacro ^:private fnjorm
  ([name args]
   `(fnjorm ~name ~args any?))
  ([name args & body]
   `(s/spec (s/cat :name     #{~name}
                   :bindings (s/coll-of ::core/binding-form :kind vector? :count ~args)
                   :body     (s/* ~@body)))))


(s/fdef defgenerator
  :args (s/cat :name   simple-symbol?
               :fields (s/coll-of simple-symbol? :kind vector? :distinct true)
               :body   (s/* (s/or :generate (fnjorm 'generate 1)
                                  :destroy  (fnjorm 'destroy 2))))
  :ret  any?)


(defmacro defgenerator
  "Define a Generator class:
(defgenerator name
  [fields]
  (generate [key]
    ...)
  (destroy [key obj]
    ...))"
  [name fields & body]
  (s/assert (:args (s/spec `defgenerator))
            (list* name fields body))
  (let [typename (gensym (munge name))
        {:syms [generate destroy]}
        (zipmap (map first body)
                (map next body))]
    `(do (defrecord ~typename
           ~fields
           IPool$Generator
           (~'generate [_# ~@(first generate)]
             ~@(next generate))
           (~'destroy [_# ~@(first destroy)]
             ~@(next destroy)))
         (defn ~name ~fields
           (new ~typename ~@fields)))))


(s/fdef controller?
  :args (s/cat :x any?)
  :ret  boolean?)


(definline controller?
  [x]
  `(instance? IPool$Controller ~x))


(declare get-controller)


(s/def ::controller controller?)


(s/fdef controller
  :args (s/cat :increment? ::increment?
               :adjustment ::adjustment)
  :ret  ::controller)


(defn controller
  [increment? adjust]
  (reify IPool$Controller
    (shouldIncrement [_ key key-objects total-objects]
      (increment? key key-objects total-objects))
    (adjustment [_ stats-map]
      (adjust stats-map))))


(s/fdef defcontroller
  :args (s/cat :name   simple-symbol?
               :fields (s/coll-of simple-symbol? :kind vector? :distinct true)
               :body   (s/* (s/or :increment? (fnjorm 'increment? 3 any?)
                                  :adjustment (fnjorm 'adjustment 1 any?))))
  :ret  any?)


(defmacro defcontroller
  "Define Controller class:
(defcontroller name
  [fields]
  (increment? [key key-objects total-objects]
    ...)
  (adjustment [stats-map]
    ...))"
  [name fields & body]
  (s/assert (:args (s/spec `defcontroller))
            (list* name fields body))
  (let [typename (gensym (munge name))
        {:syms [increment? adjustment]}
        (zipmap (map first body)
                (map next body))]
    `(do (defrecord ~typename
           ~fields
           IPool$Controller
           (~'adjustment [_# ~@(first adjustment)]
             ~@(rest adjustment))
           (~'shouldIncrement [_# ~@(first increment?)]
             ~@(rest increment?))
           clojure.lang.IFn
           (~'invoke [_# ~@(first adjustment)]
             ~@(rest adjustment))
           (~'invoke [_# ~@(first increment?)]
             ~@(rest increment?)))
         (defn ~name ~fields
           (new ~typename ~@fields)))))


(defn- map-values
  [f m]
  (zipmap (keys m)
          (map f (vals m))))


(s/fdef utilization-controller
  :args (s/and (s/cat :target-utilization  ::target-utilization
                      :max-objects-per-key ::max-objects-per-key
                      :max-objects         ::max-objects)
               (fn [{:keys [max-objects-per-key max-objects]}]
                 (<= max-objects-per-key max-objects)))
  :ret  controller?)


(defcontroller utilization-controller
  [target-utilization max-objects-per-key max-objects]
  (increment? [key key-objects total-objects]
    (and (< key-objects max-objects-per-key)
         (< total-objects max-objects)))
  (adjustment [stats-map]
    (map-values
      (fn [^Stats s]
        (.intValue
          (Math/ceil (- (* (stats/num-workers s)
                           (/ (stats/utilization s 1.0) target-utilization))
                        (stats/num-workers s)))))
      stats-map)))


(s/fdef fixed-controller
  :args (s/and (s/cat :max-objects-per-key ::max-objects-per-key
                      :max-objects         ::max-objects)
               (fn [{:keys [max-objects-per-key max-objects]}]
                 (<= max-objects-per-key max-objects)))
  :ret  controller?)


(defcontroller fixed-controller
  [max-objects-per-key max-objects]
  (increment? [key key-objects total-objects]
    (and (< key-objects max-objects-per-key)
         (< total-objects max-objects)))
  (adjustment [stats]
    {}))


(definline callback?
  [x]
  `(instance? IPool$AcquireCallback ~x))


(s/def ::acquire-callback callback?)


(s/fdef acquire-callback
  :args (s/cat :binding (s/coll-of ::core/binding-form :kind vector? :count 1)
               :body    any?)
  :ret (and ::acquire-callback
            (s/fspec :args (s/cat :? any?) :ret any?)))


(defmacro acquire-callback
  [single-binding & body]
  `(reify
     IPool$AcquireCallback
     (handleObject [_ ~@single-binding]
       ~@body)
     clojure.lang.Fn
     clojure.lang.IFn
     (invoke [_ ~@single-binding]
       ~@body)))


(defn ^IPool$AcquireCallback callback*
  [callback-fn]
  (if (instance? IPool$AcquireCallback callback-fn)
    callback-fn
    (reify
      IPool$AcquireCallback
      (handleObject [_ obj]
        (callback-fn obj))
      clojure.lang.Fn
      clojure.lang.IFn
      (invoke [_ obj]
        (callback-fn obj)))))


(defn- checked-acquire
  [pool k healthy? ^IPool$AcquireCallback callback attempts]
  (acquire-callback [obj]
    (cond (healthy? k obj)
          (.handleObject callback obj)
          (pos? attempts)
          (do (.dispose pool k obj)
              (.acquire pool k (checked-acquire pool k healthy? callback (dec attempts))))
          :else
          (.handleObject callback nil))))


(defprotocol CheckedPool)

(defn checked-pool?
  [x]
  (extends? CheckedPool (class x)))


(defprotocol StatefulPool
  (-shutdown? [pool] "Internal predicate"))


(extend Pool
  StatefulPool
  {:-shutdown? (private-field Pool "_isShutdown" Boolean/TYPE)})


(deftype SimpleCheckedPool
  [delegate-pool healthy?
   check-on-acquire
   check-on-release
   max-acquire-attempts]
  CheckedPool
  IPool
  (acquire  [_ k callback]
    (.acquire delegate-pool
              k
              (if check-on-acquire
                (checked-acquire delegate-pool k
                                 healthy? callback
                                 max-acquire-attempts)
                callback)))
  (acquire  [_ k]
    (let [p (promise)]
      (.acquire _ k (acquire-callback [obj]
                      (deliver p obj)))
      (deref p)))
  (release  [_ k obj]
    (if (and check-on-release
             (not (healthy? k obj)))
      (.dispose delegate-pool k obj)
      (.release delegate-pool k obj)))
  (dispose  [_ k obj]
    (.dispose delegate-pool k obj))
  (shutdown [_]
    (.shutdown delegate-pool))
  StatefulPool
  (-shutdown? [_]
    (-shutdown? delegate-pool)))


(s/def ::pos-int32
  (s/with-gen (s/and pos-int?
                     #(<= % Integer/MAX_VALUE))
              (constantly
                (gen/fmap int
                  (gen/large-integer*
                    {:max Integer/MAX_VALUE
                     :min 1})))))


(s/def ::healthy? fn?)
(s/def ::check-on-acquire boolean?)
(s/def ::check-on-release boolean?)
(s/def ::max-acquire-attempts ::pos-int32)


(s/fdef simple-checked-pool
  :args (s/cat :pool     ::pool
               :healthy? ::healthy?
               :options  (s/keys :opt-un [::check-on-acquire
                                          ::check-on-release
                                          ::max-acquire-attempts])))


(defn simple-checked-pool
  [delegate-pool healthy?
   {:keys [check-on-acquire
           check-on-release
           max-acquire-attempts]
      :or {check-on-acquire true
           check-on-release true
           max-acquire-attempts 4}}]
  (SimpleCheckedPool. delegate-pool healthy?
     check-on-acquire check-on-release
     max-acquire-attempts))


(s/def ::simple-checked-pool-options
  (s/keys :req-un [::healthy?]
          :opt-un [::check-on-acquire
                   ::check-on-release
                   ::max-acquire-attempts]))


(definline ^:private time-unit?
  [x]
  `(instance? TimeUnit ~x))


(s/def ::time-unit
  (s/or :time-unit (set (seq (TimeUnit/values)))
        :keyword   #{:nanoseconds :microseconds
                     :milliseconds :seconds
                     :minutes :hours :days}))


(s/fdef ->timeunit
  :args (s/cat :time-unit? ::time-unit)
  :ret  time-unit?)


(defn- ^TimeUnit ->timeunit
  [timeunit]
  (cond (instance? TimeUnit timeunit) timeunit
        (string? timeunit)  (TimeUnit/valueOf
                              (str/upper-case timeunit))
        (keyword? timeunit) (recur (name timeunit))
        :else (throw (IllegalArgumentException.
                       (format "Invalid TimeUnit: %s %s"
                         (class timeunit) timeunit)))))


(s/def ::generator-options
  (s/keys :req-un [(or (and ::generate ::destroy)
                       ::generator)]))


(s/fdef get-generator
  :args (s/cat :opts ::generator-options)
  :ret  (s/or :generator ::generator
              :nothing   nil?))


(defn- get-generator
  "Find or construct an Object Generator from a pool options map."
  [{:keys [generate destroy]
      :as pool-opts}]
  (or (:generator pool-opts)
      (when (and generate destroy)
        (generator generate destroy))))


(s/def ::increment? fn?)
(s/def ::adjustment fn?)
(s/def ::target-utilization
  (s/double-in :infinite? false
               :NaN?      false
               :min       Double/MIN_VALUE
               :max       1.0))
(s/def ::max-objects-per-key ::pos-int32)
(s/def ::max-objects ::pos-int32)


(s/def ::fixed-controller-options
  (s/and (s/keys :req-un [::max-objects]
                 :opt-un [::max-objects-per-key])
         (fn [{:keys [max-objects max-objects-per-key]}]
           (<= (or max-objects-per-key max-objects)
               max-objects))))


(s/def ::utilization-controller-options
  (s/merge (s/keys :req-un [::target-utilization])
           ::fixed-controller-options))


(s/def ::controller-options
  (s/or :controller (s/keys :req-un [(or (and ::increment? ::adjustment)
                                         ::controller)])
        :utilz-opts ::utilization-controller-options
        :fixed-opts ::fixed-controller-options))


(s/fdef get-controller
  :args (s/cat :opts ::controller-options)
  :ret  (s/or :controller ::controller
              :nothing    nil?))


(defn- get-controller
  "Find or construct a Pool Controller from a pool options map."
  [{:keys [increment? adjustment
           max-objects-per-key
           max-objects
           target-utilization]
      :as pool-opts}]
  (or (:controller pool-opts)
      (when (and increment? adjustment)
        (controller increment? adjustment))
      (when (and (or max-objects max-objects-per-key)
                 target-utilization)
        (utilization-controller
          (double target-utilization)
          (int (or max-objects-per-key max-objects))
          (int max-objects)))
      (when (or max-objects max-objects-per-key)
        (fixed-controller
          (int (or max-objects-per-key max-objects))
          (int max-objects)))))


(def DEFAULT_MAX_QUEUE_LENGTH 65536)
(def DEFAULT_SAMPLE_PERIOD    25)
(def DEFAULT_CONTROL_PERIOD   1000)
(def DEFAULT_TIME_UNIT        TimeUnit/MILLISECONDS)


(s/def ::max-queue-length ::pos-int32)
(s/def ::sample-period    (s/int-in 1 (*  2 60 1000)))
(s/def ::control-period   (s/int-in 1 (* 10 60 1000)))


(s/def ::pool-options
  (s/merge ::generator-options
           ::controller-options
           (s/and (s/keys :opt-un [::max-queue-length
                                   ::sample-period
                                   ::control-period
                                   ::time-unit])
                  (fn [{:keys [sample-period control-period]}]
                    (<= (or sample-period DEFAULT_SAMPLE_PERIOD)
                        (or control-period DEFAULT_CONTROL_PERIOD)))
            (s/keys :opt-un [::healthy?
                             ::check-on-acquire
                             ::check-on-release
                             ::max-acquire-attempts]))))


(s/fdef pool?
  :args (s/cat :x any?)
  :ret  boolean?)


(definline pool?
  [x]
  `(instance? IPool ~x))


(s/def ::pool pool?)


(s/fdef pool
  :args (s/cat :opts (s/or :options ::pool-options
                           :pool    ::pool))
  :ret  pool?)


(defn pool
  [pool-options]
  (cond (pool? pool-options)
        pool-options
        (map? pool-options)
        (cond-> (Pool. (or (get-generator pool-options)
                           (throw (ex-info "Missing Generator" pool-options)))
                       (or (get-controller pool-options)
                           (throw (ex-info "Missing Controller" pool-options)))
                       (int (:max-queue-length pool-options DEFAULT_MAX_QUEUE_LENGTH))
                       (long (:sample-period pool-options DEFAULT_SAMPLE_PERIOD))
                       (long (:control-period pool-options DEFAULT_CONTROL_PERIOD))
                       (->timeunit (:time-unit pool-options DEFAULT_TIME_UNIT)))

                (s/valid? ::simple-checked-pool-options pool-options)
                (simple-checked-pool (:healthy? pool-options) pool-options))))


(s/fdef acquire!
  :args (s/cat :pool ::pool :key any?)
  :ret  any?)


(definline acquire!
  "Acquire an object from a pool for a key."
  [^IPool pool key]
  `(.acquire ~pool ~key))


(s/fdef async-acquire!
  :args (s/cat :pool ::pool :key any?
               :callback (s/or :function fn?
                               :callback ::acquire-callback))
  :ret  nil?)


(definline async-acquire!
  "Asynchronously acquire and use an object from
  a pool for a key"
  [^IPool pool key callback]
  `(.acquire ~pool ~key (callback* ~callback)))


(s/fdef release!
  :args (s/cat :pool ::pool :key any? :object any?)
  :ret  pool?)


(definline release!
  "Release an object for a key back to a pool.
Returns the pool."
  [^IPool pool key obj]
  `(do (.release ~pool ~key ~obj)
       ~pool))


(s/fdef dispose!
  :args (s/cat :pool ::pool :key any? :object any?)
  :ret  pool?)


(definline dispose!
  "Dispose of an object for a key from a pool.
Returns the pool."
  [^IPool pool key obj]
  `(do (.dispose ~pool ~key ~obj)
       ~pool))


(s/fdef shutdown!
  :args (s/cat :pool ::pool)
  :ret  nil?)


(defn shutdown!
  "Shutdown a pool."
  [^IPool pool]
  (.shutdown pool))


(defn shutdown?
  [pool]
  (-shutdown? pool))


(s/fdef with-resource
  :args (s/cat :binding (s/spec (s/and vector?
                                       (s/cat :binding ::core/binding-form
                                              :pool    any?
                                              :key     (s/? any?))))
               :body (s/* any?))
  :ret  any?)


(defmacro with-resource
  "Acquire, Bind, & Release a pooled object
  within a scope.
(with-resource [obj pool key]
  ...)"
  [binding & body]
  (let [[sym pool key] binding
        key (or key ::NO-KEY)]
    `(let [~sym (acquire! ~pool ~key)]
       (try (do ~@body)
         (finally (release! ~pool ~key ~sym))))))


;(s/fdef async-with-resource
;  :args (s/cat :binding (s/and vector?
;                               (s/spec (s/cat :sym simple-symbol?
;                                              :pool any?
;                                              :key  (s/? any?))))
;               :body (s/* any?))
;  :ret any?)


;(defmacro async-with-resource
;  "Like with-resource, but acquires a
;  resource asynchronously and returns
;  a promise yielding the result."
;  [binding & body]
;  (let [[sym pool key] binding
;        key (or key ::NO-KEY)
;    `(let [p# (promise)]
;       (acquire! ~pool ~key
;         (acquire-callback [~sym]
;           (try (deliver p# (do ~@body))
;             (finally (release! ~pool ~key ~sym))
;       p#)


(defmacro with-pool
  "Construct and use a pool within a scope.
Shuts down the pool when finished."
  [binding & body]
  (let [[sym pool-opts] binding]
    `(let [~sym (pool ~pool-opts)]
      (try (do ~@body)
        (finally (shutdown! ~sym))))))
