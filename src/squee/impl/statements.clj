(ns squee.impl.statements
  (:import [java.sql Connection Statement PreparedStatement ResultSet])
  (:require [squee.impl.protocols :as p]
            [squee.impl.resultset :as rs]))

(extend-protocol p/ISQLValue
  Object
  (sql-value [v] v)

  nil
  (sql-value [_] nil))

(extend-protocol p/ISQLParameter
  Object
  (set-parameter [v ^PreparedStatement s ^long i]
    (.setObject s i v))

  nil
  (set-parameter [_ ^PreparedStatement s ^long i]
    (.setObject s i nil)))

(defn set-parameters
  "Add the parameters to the given statement."
  [stmt params]
  (let [iter (clojure.lang.RT/iter params)]
    (loop [i 1]
      (when (.hasNext iter)
        (p/set-parameter (p/sql-value (.next iter)) stmt i)
        (recur (unchecked-inc i))))))

(def ^{:private true
       :doc "Map friendly :concurrency values to ResultSet constants."}
  result-set-concurrency
  {:read-only ResultSet/CONCUR_READ_ONLY
   :updatable ResultSet/CONCUR_UPDATABLE})

(def ^{:private true
       :doc "Map friendly :cursors values to ResultSet constants."}
  result-set-holdability
  {:hold ResultSet/HOLD_CURSORS_OVER_COMMIT
   :close ResultSet/CLOSE_CURSORS_AT_COMMIT})

(def ^{:private true
       :doc "Map friendly :type values to ResultSet constants."}
  result-set-type
  {:forward-only ResultSet/TYPE_FORWARD_ONLY
   :scroll-insensitive ResultSet/TYPE_SCROLL_INSENSITIVE
   :scroll-sensitive ResultSet/TYPE_SCROLL_SENSITIVE})

;; ripped from c.j.j
(defn prepare-statement
  "Create a prepared statement from a connection, a SQL string and an
   optional list of parameters:
     :return-keys true | false - default false
     :result-type :forward-only | :scroll-insensitive | :scroll-sensitive
     :concurrency :read-only | :updatable
     :cursors
     :fetch-size n
     :max-rows n
     :timeout n"
  [^Connection con ^String sql opts]
  (let [{:keys [return-keys result-type concurrency cursors
                fetch-size max-rows timeout]}
        opts

        ^PreparedStatement
        stmt (cond return-keys
                   (try
                     (.prepareStatement con sql Statement/RETURN_GENERATED_KEYS)
                     (catch Exception _
                       ;; assume it is unsupported and try basic PreparedStatement:
                       (.prepareStatement con sql)))

                   (and result-type concurrency)
                   (if cursors
                     (.prepareStatement con sql
                                        (get result-set-type result-type result-type)
                                        (get result-set-concurrency concurrency concurrency)
                                        (get result-set-holdability cursors cursors))
                     (.prepareStatement con sql
                                        (get result-set-type result-type result-type)
                                        (get result-set-concurrency concurrency concurrency)))

                   :else
                   (.prepareStatement con sql))]
    (when fetch-size (.setFetchSize stmt fetch-size))
    (when max-rows (.setMaxRows stmt max-rows))
    (when timeout (.setQueryTimeout stmt timeout))
    stmt))

(defn apply-batch-params
  "Adds all the parameters from a row/batch to a statement, then
   calls PS.addBatch(). A reducing function, returns the statement."
  [^PreparedStatement jstmt row]
  (set-parameters jstmt row)
  (.addBatch jstmt)
  jstmt)

(defn multi-batch
  [collfn]
  (fn [jstmt]
    (reduce apply-batch-params jstmt (collfn))))

(defn realize-query!
  [f ^PreparedStatement stmt]
  (with-open [rs (.executeQuery stmt)]
    (f rs)))

(defn return-keys
  [^PreparedStatement stmt failure-value]
  (try
    (let [rs (.getGeneratedKeys stmt)
          result (into [] (rs/result-set* rs))]
      (.close rs)
      result)
    (catch Exception ignored failure-value)))

(defn realize-batch!
  "Executes a series of commands prepped with PS.addBatch()"
  [return-keys? ^PreparedStatement stmt]
  (let [counts (seq (.executeBatch stmt))]
    (if return-keys?
      (return-keys stmt counts)
      counts)))

;; TODO needed?
(defn realize-update!
  "Executes a single DML statement on PreparedStatement"
  [^PreparedStatement stmt]
  (.executeUpdate stmt))

(defn realize-commands!
  [^Statement stmt]
  (let [update-counts (.executeBatch stmt)]
    (seq update-counts)))
