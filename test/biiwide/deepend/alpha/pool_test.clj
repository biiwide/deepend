(ns biiwide.deepend.alpha.pool-test
  (:require [biiwide.deepend.alpha.pool :as p]
            [clojure.spec.alpha :as s]
            [clojure.spec.test.alpha :as stest]
            [clojure.test :refer :all]
            [clojure.test.check.clojure-test :refer :all]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as props])
  (:import  [java.util Date UUID]))


(p/defgenerator uuids
  []
  (generate [k] (UUID/randomUUID))
  (destroy  [k v] (assert (instance? UUID v))))


(deftest test-map-values-over-HashMap
  (is (= {:a "1" :b "2" :c "3"}
         (#'p/map-values str
           (doto (java.util.HashMap.)
             (.put :a 1)
             (.put :b 2)
             (.put :c 3))))))


(def get-generator #'p/get-generator)
(def get-controller #'p/get-controller)


(defn args-spec
  [fspec]
  (let [s (s/form fspec)]
    (assert (= `s/fspec (first s)))
    (eval (:args (apply hash-map (rest s))))))


(defn gen-result
  ([f-sym]
   (gen-result f-sym nil))
  ([f-sym overrides]
   (let [f (resolve f-sym)]
     (gen/fmap (partial apply f)
               (s/gen (args-spec f-sym) overrides)))))


(defn build-overrides
  ([spec-overrides]
   (build-overrides {} spec-overrides))
  ([overrides-map spec-overrides]
   (reduce
     (fn [gens [k gf]]
       (let [kgen (cond (gen/generator? gf) gf
                        (symbol? gf)        (gen-result gf gens)
                        (fn? gf)            (gf gens))]
         (assoc gens k (constantly kgen))))
     overrides-map
     (partition 2 spec-overrides))))


(def pool-overrides
  (build-overrides
    [::p/generate     (gen/return (fn [k] (java.util.Date.)))
     ::p/destroy      (gen/return (fn [k v] (assert (instance? java.util.Date v))))
     ::p/generator    (gen/return (uuids))
     ::p/generator    `p/get-generator
     ::p/increment?   (gen/return (constantly true))
     ::p/adjustment   (gen/return (constantly {}))
     ::p/controller   (fn [gs]
                        (gen/one-of [(gen-result `p/fixed-controller gs)
                                     (gen-result `p/utilization-controller gs)]))
     ::p/controller   `p/get-controller
     ::p/pool         (fn [gs]
                        (gen/fmap p/pool
                                  (s/gen ::p/pool-options gs)))]))


(stest/instrument
  (stest/instrumentable-syms)
  {:gen pool-overrides})


(deftest auto-checks
  (let [results (stest/summarize-results
                  (stest/check [`p/fixed-controller
                                `p/utilization-controller
                                `p/get-generator
                                `p/get-controller
                                `p/->timeunit]
                               {:gen pool-overrides}))]
    (println results)
    (is (= (:total results)
           (:check-passed results)))))


(deftest test-pool
  (is (p/pool? (p/pool {:generator (uuids)
                        :max-objects 4})))

  (is (p/pool? (p/pool {:generator (uuids)
                        :max-objects 3
                        :target-utilization 0.5}))))


(defspec with-pool-spec
  (props/for-all [opts (s/gen ::p/pool-options pool-overrides)]
    (p/with-pool [p opts]
      (is (p/pool? p)))))


(deftest with-pool-shutdown-test
  (let [p' (p/with-pool [p {:generator   (uuids)
                            :max-objects 3}]
             (is (p/pool? p))
             (is (not (p/shutdown? p)))
             p)]
    (is (p/shutdown? p'))))


(deftest test-lifecycle
  (is (true? (p/with-pool [p1 {:generator (uuids)
                               :max-objects 4}]
               (is (p/pool p1))
               (let [id1 (p/with-resource [uuid p1]
                           (is (instance? java.util.UUID uuid))
                           uuid)
                     id2 (p/with-resource [uuid p1]
                            (is (instance? java.util.UUID uuid))
                            uuid)]
                 (is (identical? id1 id2)))))))


(defn interrupt-threads!
  [thread-name-pattern]
  (let [re (re-pattern thread-name-pattern)]
    (->> (Thread/getAllStackTraces)
         (keys)
         (filter (fn [^Thread t] (re-find re (.getName t))))
         (map (fn [^Thread t]
                (.interrupt t)
                (.getName t)))
         (into []))))
