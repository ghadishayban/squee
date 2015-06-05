(ns squee.impl.protocols)

(defprotocol IDataSource
  (open-connection [_] "Must return a java.sql.Connection"))

(defprotocol IConnection
  "Internal, do not extend. Only extenders should be java.sql.Connection
   and things in the transaction namespace."
  (get-connection [_] "Returns the open underlying connection."))

(defprotocol ITransactionFactory
  (begin! [_ opts] "Starts a transaction, returning something that encompasses a connection and transaction"))

(defprotocol ITransaction
  (rollback! [_])
  (commit! [_]))

(defprotocol RowGenerator
  (make-row [_] "Creates a gestational row")
  (complete-row [_ row] "similar to an arity-1 reducing function completer")
  (with-field [_ row name val] "Adds the kv pair to the row, returning a new row"))



(defprotocol ISQLValue
  "Protocol for creating SQL values from Clojure values. Default
   implementations (for Object and nil) just return the argument,
   but it can be extended to provide custom behavior to support
   exotic types supported by different databases."
  (sql-value [val] "Convert a Clojure value into a SQL value."))

(defprotocol ISQLParameter
  "Protocol for setting SQL parameters in statement objects, which
   can convert from Clojure values. The default implementation just
   delegates the conversion to ISQLValue's sql-value conversion and
   uses .setObject on the parameter. It can be extended to use other
   methods of PreparedStatement to convert and set parameter values."
  (set-parameter [val stmt ix]
    "Convert a Clojure value into a SQL value and store it as the ix'th
     parameter in the given SQL statement object."))

