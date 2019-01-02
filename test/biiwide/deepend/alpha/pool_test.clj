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
     ::p/max-queue-length (gen/return  65536)
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


(defspec with-resource-spec 60
  (props/for-all [pool-opts (s/gen ::p/pool-options pool-overrides)
                  durations (gen/vector (gen/frequency [[5 (gen/choose 0 7)]
                                                        [1 (gen/return nil)]])
                                        10 200)]
    (let [history (agent [])]
      (p/with-pool [p (assoc pool-opts :generator (uuids)
                                       :max-queue-length (inc (count durations)))]
        (count (pmap (fn [i duration]
                       (try
                         (p/with-resource [obj p]
                           (send history conj [:acquire obj i])
                           (try
                             (Thread/sleep duration)
                             (finally
                               (send history conj [:release obj i]))))
                         (catch NullPointerException e e)))
                     (range)
                     durations)))
      (when (is (await-for 1000 history)
                "History must be complete")
        (and (is (= (* 2 (count durations)))
                 (count @history))
             (is (< (count durations))
                 (count (distinct (map second @history))))
             (is (empty?
                   (reduce (fn [state [action obj i]]
                             (case action
                               :acquire (do (is (not (contains? state obj))
                                                (str "Object re-acquired before release: " [i obj]))
                                            (conj state obj))

                               :release (do (is (contains? state obj)
                                                (str "Unexpected release: " [i obj]))
                                            (disj state obj))))
                           #{}
                           @history))))))))


(defspec simple-checked-pool-healthy?-spec
  (props/for-all [check-on-acquire? gen/boolean
                  check-on-release? gen/boolean
                  attempts          gen/s-pos-int
                  max-objects       gen/s-pos-int]
    (let [check-state (atom 0)
          checked     (fn []
                        (let [attempts @check-state]
                          (reset! check-state 0)
                          attempts))
          generated  (atom [])
          destroyed  (atom [])]
      (p/with-pool [p {:generate         (fn [k]
                                           (let [obj [k (rand-int 100)]]
                                             (swap! generated conj obj)
                                             obj))
                       :destroy          (fn [k obj]
                                           (swap! destroyed conj obj))
                       :max-objects      max-objects
                       :healthy?         (fn healthy? [k1 [k2 n]]
                                           (= attempts (swap! check-state inc)))
                       :check-on-acquire check-on-acquire?
                       :check-on-release check-on-release?
                       :max-acquire-attempts (inc attempts)}]
        (and (is (p/checked-pool? p))
             (p/with-resource [[k n] p]
               (and (is (= (if check-on-acquire? attempts 0)
                           (checked)))
                    (is (= (if check-on-acquire? attempts 1)
                           (count @generated)))))
             (is (= (if check-on-release? 1 0) (checked))))))))
