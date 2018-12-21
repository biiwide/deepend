(ns biiwide.deepend.alpha.pool
  (:require [biiwide.deepend.alpha.stats :as stats]
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
  (reify IPool$Generator
    (generate [_ key]
      (generate key))
    (destroy [_ key obj]
      (destroy key obj))))


(defmacro ^:private fnjorm
  ([name args]
   `(fnjorm ~name ~args any?))
  ([name args & body]
   `(s/spec (s/cat :name     #{~name}
                   :bindings (s/coll-of simple-symbol? :kind vector? :count ~args :distinct true)
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

(s/def ::controller
  (s/with-gen controller?
    #(gen/fmap get-controller
               (s/gen (s/or :fixed ::fixed-controller-options
                            :util  ::utilization-controller-options)))))


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
             ~@(rest increment?)))
         (defn ~name ~fields
           (new ~typename ~@fields)))))


(defn- map-values
  [f m]
  (zipmap (keys m)
          (map f (vals m))))


(s/fdef utilization-controller
  :args (s/cat :target-utilization  ::target-utilization
               :max-objects-per-key ::max-objects-per-key
               :max-objects-total   ::max-objects-total)

  :ret  controller?)


(defcontroller utilization-controller
  [target-utilization max-objects-per-key max-objects-total]
  (increment? [key key-objects total-objects]
    (and (< key-objects max-objects-per-key)
         (< total-objects max-objects-total)))
  (adjustment [stats-map]
    (map-values
      (fn [^Stats s]
        (Math/ceil (- (* (stats/num-workers s)
                         (/ (stats/utilization s 1.0) target-utilization))
                      (stats/num-workers s))))
      stats-map)))


(s/fdef fixed-controller
  :args (s/tuple ::max-objects-per-key ::max-objects-total)
  :ret  controller?)


(defcontroller fixed-controller
  [max-objects-per-key max-objects-total]
  (increment? [key key-objects total-objects]
    (and (< key-objects max-objects-per-key)
         (< total-objects max-objects-total)))
  (adjustment [stats]
    {}))


(definline callback?
  [x]
  `(instance? IPool$AcquireCallback ~x))


(s/def ::acquire-callback callback?)


(s/fdef acquire-callback
  :args (s/cat :binding (s/coll-of simple-symbol? :kind vector? :count 1)
               :body    any?)
  :ret (and ::acquire-callback
            (s/fspec :args (s/tuple any?) :ret any?)))


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
    (if (healthy? obj)
      (.handleObject callback obj)
      (if (pos? attempts)
        (.acquire pool k (checked-acquire pool k healthy? callback (dec attempts)))
        (.handleObject callback nil)))))


(deftype SimpleCheckedPool
  [delegate-pool healthy?
   check-on-acquire
   check-on-release
   max-acquire-attempts]
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
    (.shutdown delegate-pool)))

(s/def ::check-on-acquire boolean?)
(s/def ::check-on-release boolean?)
(s/def ::max-acquire-attempts pos-int?)


(s/fdef simple-checked-pool
  :args (s/cat :pool ::pool
               :healthy? ::healthy?
               :options (s/keys :opt-un [::check-on-acquire
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
  (s/or :time-unit time-unit?
        :keyword   #{:nanoseconds :microseconds
                     :milliseconds :seconds
                     :minutes :hours :days}))


(s/fdef ->time-unit
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
  (s/or :generator     (s/keys :req-un [::generator])
        :generator-fns (s/keys :req-un [::generate
                                        ::destroy])))


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
                :min       0.0
                :max       1.0))
(s/def ::max-objects-per-key pos-int?)
(s/def ::max-objects-total pos-int?)
(s/def ::max-objects pos-int?)


(s/def ::fixed-controller-options
  (s/and (s/keys :opt-un [::max-objects
                          ::max-objects-per-key
                          ::max-objects-total])
         (s/or   :max-objects         (s/keys :req-un [::max-objects])
                 :max-objects-per-key (s/keys :req-un [::max-objects-per-key])
                 :max-objects-total   (s/keys :req-un [::max-objects-total]))))


(s/def ::utilization-controller-options
  (s/merge (s/keys :req-un [::target-utilization])
           ::fixed-controller-options))


(s/def ::controller-options
  (s/or :controller     (s/keys :req-un [::controller])
        :controller-fns (s/keys :req-un [::increment?
                                         ::adjustment])
        :utilz-opts     ::utilization-controller-options
        :fixed-opts     ::fixed-controller-options))


(s/fdef get-controller
  :args (s/cat :opts ::controller-options)
  :ret  (s/or :controller ::controller
              :nothing    nil?))


(defn- get-controller
  "Find or construct a Pool Controller from a pool options map."
  [{:keys [increment? adjustment
           max-object-per-key
           max-objects-total
           max-objects
           target-utilization]
      :as pool-opts}]
  (or (:controller pool-opts)
      (when (and increment? adjustment)
        (controller increment? adjustment))
      (when (and (or max-objects max-objects-total max-object-per-key)
                 target-utilization)
        (utilization-controller
          (double target-utilization)
          (int (or max-object-per-key max-objects max-objects-total))
          (int (or max-objects-total max-objects max-object-per-key))))
      (when (or max-objects max-objects-total max-object-per-key)
        (fixed-controller
          (int (or max-object-per-key max-objects max-objects-total))
          (int (or max-objects-total max-objects max-object-per-key))))))


(s/def ::max-queue-length pos-int?)
(s/def ::sample-period    pos-int?)
(s/def ::control-period   pos-int?)


(s/def ::pool-options
  (s/and ::generator-options
         ::controller-options
         (s/keys :opt-un [::max-queue-length
                          ::sample-period
                          ::control-period
                          ::time-unit])))


(s/fdef pool?
  :args (s/cat :x any?)
  :ret  boolean?)


(definline pool?
  [x]
  `(instance? IPool ~x))


(s/def ::pool pool?)


(s/fdef pool
  :args (s/cat :opts (s/or :pool    ::pool
                           :options ::pool-options))
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
                       (int (:max-queue-length pool-options 65536))
                       (long (:sample-period pool-options 25))
                       (long (:control-period pool-options 1000))
                       (->timeunit (:time-unit pool-options TimeUnit/MILLISECONDS)))

                (s/valid? pool-options ::simple-checked-pool-options)
                (simple-checked-pool (:healty? pool-options) pool-options))))


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
  "Release an object for a key back to a pool."
  [^IPool pool key obj]
  `(do (.release ~pool ~key ~obj)
       ~pool))


(s/fdef dispose!
  :args (s/cat :pool ::pool :key any? :object any?)
  :ret  pool?)


(definline dispose!
  "Dispose of an object for a key from a pool."
  [^IPool pool key obj]
  `(do (.dispose ~pool ~key ~obj)
       ~pool))


(s/fdef shutdown!
  :args (s/cat :pool ::pool)
  :ret  nil?)


(definline shutdown!
  "Shutdown a pool."
  [^IPool pool]
  `(.shutdown ~pool))


(s/fdef with-resource
  :args (s/cat :binding (s/and vector?
                               (s/spec (s/cat :sym simple-symbol?
                                              :pool any?
                                              :key  (s/? any?))))
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
