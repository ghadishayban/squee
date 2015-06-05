# jdbc2

A refreshed JDBC wrapper, made for Clojure 1.7

# Rationale

tl;dr:
Ergonomics: consistent argument shapes
Modernized implementation with cheap reducible result-sets
Less seq and stack frame allocation compared to clojure.java.jdbc
Smaller methods, modularized. (e.g. RowGenerator protocol)

# Guide

## Arguments

Arguments all have the same shape `[conn sql params opts]`, and the first two are required.

1. conn:  a connection or transaction
2. sql:   a SQL string
3. params: a function that manipulates the sql statement, usually filling in templated values.
  There are two such built-ins, `params` and `many`. See below
4. opts: a map of options

# Examples

## Query
(query conn "select * from bar")

(query conn "select * from bar where x = ?" (params 4))

## Inserts

```clj
(let [sql "insert into foo (x,y) values (?,?)"]
  (execute! conn sql (params 42 40) {:return-keys? true}))

(let [sql "insert into foo (x,y) values (?,?)"]
  (execute! conn sql (many [[20 30] [40 50]])))
```

## Transactions
```clj
(transactionally conn
  (execute conn ...)
  (query conn...)
  (execute conn...))
```

You can set the isolation level like so:
```clj
(transactionally conn {:isolation-level :serializable}
  ...body)
```

### Param Helpers

Statement params come in two flavors, one or many, dependending on
how many times the entire sql statement is used

```clj
(params ...)
(many [ [...] [...] [...] ...])    ;; this can be an lazy or an eduction too
```

## Reducible Result sets

While clojure.java.jdbc uses lazy-seqs and a naive strategy to extract data from a ResultSet,
results from `query` in this library implement clojure.lang.IReduceInit, and are very efficient
to realize.

## Extension points
...


## License

Copyright © Ghadi Shayban

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
