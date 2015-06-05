(ns squee.impl.resultset
  (:import [java.sql ResultSet ResultSetMetaData])
  (:require [clojure.string :as string]
            [squee.impl.protocols :as p]))

(defn make-cols-unique
  [cols]
  (loop [acc (transient [])
         cols cols
         seen {}]
    (let [cols (seq cols)]
      (if cols
        (let [col (first cols)
              n   (get seen col 0)
              nom (if (zero? n)
                    col
                    (str col "_" n))]
          (recur (conj! acc nom)
                 (next cols)
                 (assoc seen col (inc n))))
        (persistent! acc)))))

(defn materialization-strategy
  [ctor complete add-field]
  (reify p/RowGenerator
    (make-row [_]
      (ctor))
    (complete-row [_ row]
      (complete row))
    (with-field [_ row column val]
      (add-field row column val))))

(def as-array
  (materialization-strategy #(transient [])
                            persistent!
                            (fn [r k v]
                              (conj! r v))))

(def as-map
  (materialization-strategy #(transient {}) persistent! assoc!))

(def canonicalize-key
  (comp keyword string/lower-case))

(defn default-key-fn
  [keys]
  (into [] (map canonicalize-key) (make-cols-unique keys)))

(defn result-set*
  ([rs]
   (result-set* rs as-map default-key-fn))
  ([^ResultSet rs gen keyfn]  ;; gen :- RowGenerator
   (reify clojure.lang.IReduceInit
     (reduce [this f init]
       (let [rsmeta (.getMetaData rs)
             n (.getColumnCount rsmeta)
             
             col    (fn [i]
                      (.getColumnLabel rsmeta (inc i)))
             
             keys   (-> (map col (range n))
                        keyfn)

             val    (fn [i]
                      (let [v (.getObject rs (int i))]
                        ;; make sure to coerce booleans to Clojure's Booleans
                        (if (instance? Boolean v)
                          (if (= true v) true false)
                          v)))]
         (loop [ret init]
           (if (.next rs)
             (let [row (loop [row (p/make-row gen)
                              i 0]
                         (if (< i n)
                           (recur (p/with-field gen row (keys i) (val (unchecked-inc i)))
                                  (unchecked-inc i))
                           (p/complete-row gen row)))
                   ret (f ret row)]
               (if (reduced? ret)
                 @ret
                 (recur ret)))
             ret)))))))
