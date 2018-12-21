(ns biiwide.deepend.alpha.pool-test
  (:require [biiwide.deepend.alpha.pool :as p]
            [clojure.spec.alpha :as s]
            [clojure.spec.test.alpha :as stest]
            [clojure.test :refer :all]
            [clojure.test.check.generators :as gen]))


(p/defgenerator uuids
  []
  (generate [k] (java.util.UUID/randomUUID))
  (destroy  [k v] nil))


;(stest/instrument
;  (stest/instrumentable-syms)
;  {:gen {::p/generator (gen/return (uuids))}})


(deftest test-map-values-over-HashMap
  (is (= {:a "1" :b "2" :c "3"}
         (#'p/map-values str
           (doto (java.util.HashMap.)
             (.put :a 1)
             (.put :b 2)
             (.put :c 3))))))


(deftest test-pool
  (is (p/pool? (p/pool {:generator (uuids)
                        :max-objects 4})))

  (is (p/pool? (p/pool {:generator (uuids)
                        :max-objects 3
                        :target-utilization 0.5}))))


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
