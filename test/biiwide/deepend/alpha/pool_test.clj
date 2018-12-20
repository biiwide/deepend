(ns biiwide.deepend.alpha.pool-test
  (:require [biiwide.deepend.alpha.pool :as p]
            [clojure.spec.test.alpha :as stest]
            [clojure.test :refer :all]))


(p/defgenerator uuidgen
  []
  (generate [k] (java.util.UUID/randomUUID))
  (destroy  [k v] nil))



(deftest test-pool
  (is (p/pool? (p/pool {:generator (uuidgen)
                        :max-objects 4}))))


(deftest test-lifecycle
  (is (true? (p/with-pool [p1 {:generator (uuidgen)
                               :max-objects 4}]
               (is (p/pool p1))
               (let [id1 (p/with-resource [uuid p1]
                           (is (instance? java.util.UUID uuid))
                           uuid)
                     id2 (p/with-resource [uuid p1]
                            (is (instance? java.util.UUID uuid))
                            uuid)]
                 (is (identical? id1 id2)))))))
