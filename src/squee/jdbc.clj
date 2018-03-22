(ns squee.jdbc
  (:require [clojure.string :as string]
            [squee.impl.protocols :as p]
            [squee.impl.transactions :as tx]
            [squee.impl.resultset :as rs]
            [squee.impl.statements :as st]
            [squee.datasources])
  (:import [java.sql Connection Statement PreparedStatement]))

(extend-type Connection
  p/IConnection
  (get-connection [conn] conn)
  p/IDataSource
  (open-connection [conn] conn))

(defmacro transactionally
  "Takes a Connection (or open transaction) and performs its
   body within a transaction.  Arbitrarily nestable, but
   if an inner transaction rolls back on uncaught exception,
   the outermost will rollback as well.

   Takes an *optional* opts map between the connection and the body,
   which supports :isolation and :read-only? options.

   Isolation levels:
   :none :read-committed :read-uncommitted :repeatable-read :serializable

   Example usage:

   (transactionally conn
      (execute! conn ...)
      (query conn ...)
      (execute! conn ...))"
  [conn & body]
  (let [opts (when (map? (first body))
               (first body))
        body (if opts
               (next body)
               body)]
    `(let [func# (^:once fn* [~conn] ~@body)]
       (tx/run-transaction ~conn func# ~opts))))

(defn reducible-result-set
  "Low-level. Takes a result set and an option map, returns a
   reducible collection (clojure.lang.IReduceInit) on the result-set.

   opts supported:

   :row-strategy   something that extends RowGenerator. The
                    default RowGenerator turns rows into maps.
   :name-keys      a function that takes one arg, a collection keys,
                    and somehow transforms them. The default one
                    dedups, lower-cases, and keywordizes them."
  [rs opts]
  (let [{:keys [row-strategy name-keys]} opts]
    (rs/result-set* rs (or row-strategy rs/as-map)
                    (or name-keys rs/default-key-fn))))

(defn execute-against-connection*
  "Low-level.

   conn                  java.sql.Connection
   create-jdbc-statement function of a Connection -> Statement or PreparedStatement
   parameterize!         side-effecting function of a PreparedStatement.
   realize!              fn of a PreparedStatement whose return value is returned."
  [conn create-jdbc-statement parameterize! realize!]
  (with-open [^Statement jstmt (create-jdbc-statement conn)]
    (parameterize! jstmt)
    (realize! jstmt)))

(defn ^:private params
  "Returns a params function that parameterizes an SQL statement *once*.

   Takes multiple arguments"
  [xs]
  (fn [jstmt]
    (st/apply-batch-params jstmt xs)))

;; does this need to be a once fn?
(defmacro ^:private many
  "Returns a params function that parameterizes an SQL statement *multiple* times.

   Takes a collection of collections."
  [params]
  `(st/multi-batch (^:once fn* [] ~params)))

(defn default-rs-fn
  [opts]
  (fn [rs]
    (->> (reducible-result-set rs opts)
         (into []))))

(defn opts->statement-fn
  [{p :params
    m :many
    statement-fn :statement-fn
    :as opts}]
  (cond
    p (params p)
    m (many m)
    (ifn? statement-fn) (statement-fn)
    :else identity))

(defn ^:private query*
  [conn sql opts]
  (let [{:keys [create-statement! result-set-fn]} opts
        result-set-fn (or result-set-fn (default-rs-fn opts))]
    (execute-against-connection* (p/get-connection conn)
                                 (or create-statement! #(st/prepare-statement % sql opts))
                                 (opts->statement-fn opts)
                                 (partial st/realize-query! result-set-fn))))

(defn query
  "Execute an SQL query against a Connection or open transaction.

   Examples:
   (query conn \"select * from bar\")

   (query conn \"select * from foo where x = ? and y = ? \" {:params [4 5]}))

   opts supported:
    :params Coll of values that parameterizes an SQL statement *once*
    :many Collection of collections of values that parameterize an SQL statement *multiple* times
    :result-type :forward-only | :scroll-insensitive | :scroll-sensitive
    :concurrency :read-only | :updatable
    :cursors
    :fetch-size n
    :max-rows n
    :timeout n

    :create-statement!  Providing this function (of a Connection) will override the above arguments

    :result-set-fn      By default, this turns the ResultSet into a vector of maps.

    Described in #'reducible-result-set:
    :row-strategy
    :name-keys"
  ([conn sql]
   (query* conn sql nil))
  ([conn sql opts]
   (query* conn sql opts)))

;; query
;;  no params
;;  set single params

;; update
;;  no params
;;  set single params
;;  set single params, returning
;;  set multiple params, non returning
;;  set multiple params, returning keys

(defn ^:private execute*
  [conn sql opts]
  (let [{:keys [create-statement! return-keys]} opts]
    (execute-against-connection* (p/get-connection conn)
                                 (or create-statement! #(st/prepare-statement % sql opts))
                                 (opts->statement-fn opts)
                                 (partial st/realize-batch! return-keys))))

(defn execute!
  "Execute an SQL query against a Connection or open transaction.

   Examples:
   (query conn \"select * from bar\")

   (query conn \"select * from foo where x = ? and y = ? \" {:params [4 5]})

   Supports all opts that #'query supports
   Additionally `:return-keys? true`  will return database-generated keys."
  ([conn sql]
   (execute* conn sql nil))
  ([conn sql opts]
   (execute* conn sql opts)))

(defn insert-sql
  [table & cols]
  (str "INSERT INTO " table " ( "
       (string/join ", " cols)
       " ) VALUES ( "
       (string/join ", " (repeat (count cols) "?"))
       " )"))


;; maybe make SQL string be something that is protocolized.
;; would help with accepting honeysql directly

(defmacro with-connection
  "Macro that opens a connection from the provided spec, which must be
   one of the extenders of IDataSource.

   (with-connection [conn some-IDataSource])
     .... body)"
  [binding & body]
  `(with-open [~(with-meta (binding 0) {:tag 'Connection}) (p/open-connection ~(binding 1))]
     ~@body))

(defn connect!
  "Helper, equivalent to
    (with-connection [conn db-spec]
      (f conn .. args)
   and in the spirit of update-in/update/swap!, etc.

   (connect! \"db query select * from foo where x = ?\" {:params [a]}))"
  ([db f a]
   (with-connection [conn db]
     (f conn a)))
  ([db f a b]
   (with-connection [conn db]
     (f conn a b)))
  ([db f a b c]
   (with-connection [conn db]
     (f conn a b c)))
  ([db f a b c & args]
   (with-connection [conn db]
     (apply f a b c args))))

(defn do-commands
  "Runs a set of commands against the database."
  [conn & commands]
  (let [add-commands (fn [^Statement s]
                       (doseq [cmd commands]
                         (.addBatch s cmd)))]
    (execute-against-connection* (p/get-connection conn)
                                 (fn [c] (.createStatement ^Connection c))
                                 add-commands
                                 st/realize-commands!)))

;; TODO perhaps have a :transducer option for query
