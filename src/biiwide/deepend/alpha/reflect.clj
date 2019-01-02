(ns biiwide.deepend.alpha.reflect
  "Utility functions for reflective access."
  (:refer-clojure :exclude [get-method])
  (:require [clojure.spec.alpha :as s])
  (:import  [java.lang.invoke MethodHandle MethodHandles
                              VarHandle VarHandle$AccessMode]
            [java.lang.reflect Field Method]))


(s/fdef get-method
  :args (s/cat :class class?
               :method-name string?
               :arg-types (s/* class?))
  :ret  (s/or :method  #(instance? Method %)
              :nothing nil?))


(defn get-method
  [clazz method-name & arg-types]
  (try
    (.getMethod clazz method-name
      (into-array Class arg-types))
    (catch Exception e nil)))


(defn has-private-lookup-in?
  []
  (boolean
    (get-method java.lang.invoke.MethodHandles
                "privateLookupIn"
                Class java.lang.invoke.MethodHandles$Lookup)))


(defmacro ^:private ????
  ([] (throw (IllegalStateException. "No matching condition found!")))
  ([test form & more-pairs]
   (if (eval test)
     form
     (cons `???? ~@more-pairs))))


(s/fdef private-field
  :args (s/cat :clazz class?
               :field-name string?
               :field-type class?))


(defn private-field
  [clazz field-name field-type]
  (????
    (has-private-lookup-in?)
    (when-some [^MethodHandle mh
                (as-> (MethodHandles/lookup) lookup
                      (MethodHandles/privateLookupIn clazz lookup)
                      (.findVarHandle lookup clazz field-name field-type)
                      (.toMethodHandle lookup VarHandle$AccessMode/GET))]
      (fn [obj]
        (-> (.bindTo mh obj)
            (.invokeWithArguments []))))

    :else
    (when-some [field (or (try (.getDeclaredField clazz field-name)
                            (catch Exception e nil))
                          (try (.getField class field-name)
                            (catch Exception e nil)))]
      (.setAccessible field)
      (fn [obj]
        (.get ^Field field obj)))))
