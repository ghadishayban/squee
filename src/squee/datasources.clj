(ns squee.impl.datasources
  (:require [squee.impl.protocols :as p]
            [squee.impl.util :as util])
  (:import [java.sql Connection DriverManager]
           [javax.sql DataSource]))

(defn from-factory
  "Take a factory function that returns a Connection, closes over it as an IDataSource."
  [f]
  (reify p/IDataSource
    (open-connection [_]
      (f))))

(extend-protocol p/IDataSource
  javax.sql.DataSource
  (open-connection [ds]
    (.getConnection ds)))

(defn from-spec
  "Takes a map with keys 
   :subprotocol
   :subname
   :classname

   Returns an IDataSource."
  [{:keys [subprotocol subname classname] :as db-spec}]
  (let [url (format "jdbc:%s:%s" subprotocol subname)
        etc (dissoc db-spec :classname :subprotocol :subname)
        classname (or classname (util/classnames subprotocol))
        props (util/as-properties etc)]

    (clojure.lang.RT/loadClassForName classname)

    (reify p/IDataSource
      (open-connection [_]
        (DriverManager/getConnection url props)))))

(defn from-datasource-with-auth
  "Takes a javax.sql.DataSource with credentials, returns
   an IDataSource."
  [datasource username password]
  (reify p/IDataSource
    (open-connection [_]
      (.getConnection ^DataSource datasource
                      ^String username
                      ^String password))))

#_(defn from-uri
  ([uri])
  ([uri user password])
  ([uri user password props]))

#_(defn from-jndi-name
  [name environment]
  (reify IDataSource
    (open-connection [_]
      (util/when-available javax.naming.InitialContext
         (let [env (Hashtable. ^Map environment)
             context (javax.naming.InitialContext. env)
             ^DataSource datasource (.lookup context ^String name)]
         (.getConnection datasource))))))
