(ns biiwide.deepend.alpha.pool
  (:require [biiwide.deepend.alpha.stats :as stats]
            [clojure.spec.alpha :as s]
            [clojure.string :as str])
  (:import  [java.util.concurrent TimeUnit]
            [io.aleph.dirigiste
             IPool IPool$AcquireCallback IPool$Controller IPool$Generator
             Pool Pools Stats]))


(definline generator?
  [x]
  `(instance? IPool$Generator ~x))


(s/def ::generator generator?)


(s/fdef generator
  :args (s/tuple fn? fn?)
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
               :body   (s/* (s/alt :generate (fnjorm 'generate 1)
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
           IPool$Generate
           (~'generate [_# ~@(first generate)]
             ~@(next generate))
           (~'destroy [_# ~@(first destroy)]
             ~@(next destroy)))
         (defn ~name ~fields
           (new ~typename ~@fields)))))


(definline controller?
  [x]
  `(instance? IPool$Controller ~x))


(s/def ::controller controller?)


(s/fdef controller
  :args (s/tuple fn? fn?)
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
               :body   (s/* (s/alt :increment? (fnjorm 'increment? 3 any?)
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
  [m f]
  (persistent!
    (reduce-kv (fn [m k v] (assoc! m k (f v)))
               (transient {})
               m)))


(s/fdef utilization-controller
  :args (and (s/tuple ::target-utilization ::max-objects-per-key ::max-objects-total)
             ())
  :ret  controller?)


(defcontroller utilization-controller
  [target-utilization max-objects-per-key max-objects-total]
  (increment? [key key-objects total-objects]
    (and (< key-objects max-objects-per-key)
         (< total-objects max-objects-total)))
  (adjustment [stats-map]
    (map-values stats-map
      (fn [^Stats s]
        (Math/ceil (- (* (stats/num-workers s)
                         (/ (stats/utilization s 1.0) target-utilization))
                      (stats/num-workers s)))))))


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
  :ret ::acquire-callback)


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


(definline ^:private time-unit?
  [x]
  `(instance? TimeUnit ~x))


(s/def ::time-unit
  (s/alt :time-unit time-unit?
         :keyword   #{:nanoseconds :microseconds
                      :milliseconds :seconds
                      :minutes :hours :days}))


(s/fdef ->time-unit
  :args (s/tuple ::time-unit)
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

(s/def ::generate fn?)
(s/def ::destroy fn?)

(s/def ::generator-options
  (s/alt :generator     (s/keys :req-un [::generator])
         :generator-fns (s/keys :req-un [::generate
                                         ::destroy])))

(s/fdef get-generator
  :args (s/tuple ::generator-options)
  :ret  (s/or :generator ::generator
              :nothing   nil?))

(defn- get-generator
  "Find or construct an Object Generator from a pool options map."
  [{:keys [generate destroy]
      :as pool-opts}]
  (or (:generator pool-opts)
      (when (and generate destroy)
        (generator generate destroy))))


(s/def ::controller IPool$Controller)
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
                          ::max-objects-per-keys
                          ::max-objects-total])
         (s/or   :max-objects         (s/keys :req-un [::max-objects])
                 :max-objects-per-key (s/keys :req-un [::max-objects-per-key])
                 :max-objects-total   (s/keys :req-un [::max-objects-total]))))


(s/def ::utilization-controller-options
  (s/and (s/keys :req-un [::target-utilization])
         ::fixed-controller-options))


(s/def ::controller-options
  (s/alt :controller     (s/keys :req-un [::controller])
         :controller-fns (s/keys :req-un [::increment?
                                          ::adjustment])
         :utilz-opts     ::utilization-controller-options
         :fixed-opts     ::fixed-controller-options))


(s/fdef get-controller
  :args (s/tuple ::controller-options)
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


(definline pool?
  [x]
  `(instance? IPool ~x))


(s/def ::pool pool?)


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


(s/fdef pool
  :args (s/tuple ::pool-options)
  :ret  pool?)


(defn pool
  [{:as pool-options
    :keys [max-queue-length sample-period control-period
           time-unit]}]
  (Pool. (or (get-generator pool-options)
             (throw (ex-info "Missing Generator" pool-options)))
         (or (get-controller pool-options)
             (throw (ex-info "Missing Controller" pool-options)))
         (int (or max-queue-length 65536))
         (long (or sample-period 25))
         (long (or control-period 1000))
         (->timeunit (or time-unit TimeUnit/MILLISECONDS))))


(s/fdef acquire!
  :args (s/cat :pool ::pool :key any?)
  :ret  any?)


(definline acquire!
  [^IPool pool key]
  `(.acquire ~pool ~key))


(s/fdef async-acquire!
  :args (s/cat :pool ::pool :key any?
               :callback (s/alt :function fn?
                                :callback ::acquire-callback))
  :ret  nil?)


(definline async-acquire!
  [^IPool pool key callback]
  `(.acquire ~pool ~key (callback* ~callback)))


(s/fdef release!
  :args (s/cat :pool IPool :key any? :object any?)
  :ret  nil?)


(definline release!
  [^IPool pool key obj]
  `(.release ~pool ~key ~obj))


(s/fdef dispose!
  :args (s/cat :pool IPool :key any? :object any?)
  :ret  nil?)


(definline dispose!
  [^IPool pool key obj]
  `(.destroy ~pool ~key ~obj))


(s/fdef shutdown!
  :args (s/tuple IPool)
  :ret  nil?)


(definline shutdown!
  [^IPool pool]
  `(.shutdown ~pool))


(s/fdef with-resource
  :args (s/cat :binding (s/and vector?
                               (s/spec (s/cat :sym simple-symbol?
                                              :pool any?
                                              :key  (s/? any?))))
               :body (s/* any?)))


(defmacro with-resource
  "Acquire, Bind, & Release a pooled object
  within a scope.
(with-resource [obj pool key]
  ...)"
  [binding & body]
  (let [[sym pool key] binding
        key (or key ::NO-KEY)]
    `(let [~sym (acquire ~pool ~key)]
       (try (do ~@body)
         (finally (release ~pool ~key))))))


(defmacro async-with-resource
  "Like with-resource, but acquire a
  resource asynchronously and return
  a promise yielding the result."
  [binding & body]
  (let [[sym pool key] binding
        key (or key ::NO-KEY)]
    `(let [p# (promise)]
       (acquire ~pool ~key
         (acquire-callback [~sym]
           (try (deliver p# (do ~@body))
             (finally (release ~pool ~key)))))
       p#)))
