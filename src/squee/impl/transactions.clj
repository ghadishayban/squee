(ns squee.impl.transactions
  (:require [squee.impl.protocols :as p])
  (:import [java.sql Connection]))

;; TODO: Warn on nested transaction being more restrictive than outer
;;   can check with int comparison. Serializable is 8, none is 0

(deftype NestedTransaction [conn rollback-outer!]
  p/IConnection
  (get-connection [_]
    conn)
  p/ITransactionFactory
  (begin! [this opts]
    this)
  p/ITransaction
  (rollback! [this]
    (rollback-outer!)
    true)
  (commit! [this]
    true))

(deftype RootTransaction [^Connection conn rollback? cleanup!]
  p/IConnection
  (get-connection [_]
    conn)
  p/ITransactionFactory
  (begin! [_ opts]
    (NestedTransaction. conn #(reset! rollback? true)))
  p/ITransaction
  (rollback! [this]
    (.rollback conn)
    (cleanup! conn)
    true)
  (commit! [this]
    (if @rollback?
      (.rollback conn)
      (.commit conn))
    (cleanup! conn)
    true))

(def ^{:doc "Transaction isolation levels."}
  isolation-levels
  {:none             java.sql.Connection/TRANSACTION_NONE
   :read-committed   java.sql.Connection/TRANSACTION_READ_COMMITTED
   :read-uncommitted java.sql.Connection/TRANSACTION_READ_UNCOMMITTED
   :repeatable-read  java.sql.Connection/TRANSACTION_REPEATABLE_READ
   :serializable     java.sql.Connection/TRANSACTION_SERIALIZABLE})

(extend-protocol p/ITransactionFactory
  Connection 
  (begin! [conn opts]
    (let [{:keys [isolation read-only?]} opts
          old-auto-commit (.getAutoCommit conn)
          old-isolation   (.getTransactionIsolation conn)
          old-readonly    (.isReadOnly conn)
          cleanup! (fn [^Connection c]
                      (.setAutoCommit c old-auto-commit)
                      (when isolation
                        (.setTransactionIsolation c old-isolation))
                      (when read-only?
                        (.setReadOnly c old-readonly)))]
      (when isolation
        (.setTransactionIsolation conn (isolation-levels isolation)))
      (when read-only?
        (.setReadOnly conn true))
      (.setAutoCommit conn false)
      (->RootTransaction conn (atom false) cleanup!))))

(defn run-transaction
  [conn txfn opts]
  (let [tx (p/begin! conn opts)]
    (try
      (let [ret (txfn tx)]
        (p/commit! tx)
        ret)
      (catch Throwable t
        (p/rollback! tx)
        (throw t)))))
