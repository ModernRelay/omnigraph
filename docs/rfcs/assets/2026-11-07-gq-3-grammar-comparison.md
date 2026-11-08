# GQ 3 against GQL, Cypher, SQL and Datalog, construct by construct

**Status:** evidence asset of the [GQ 3 RFC](../2026-11-07-gq-3.md)
(2026-11-07). One row per GQ 3 production or semantic rule; the
four columns give the form the other grammar has for the same job, `none`
when it has no form, and a note when the nearest form differs in meaning.

Reference grammars:

| Column | Source read against |
|---|---|
| GQL | ISO/IEC 39075:2024, the BNF of the standard; no vendor extension |
| Cypher | openCypher 9 (`Cypher.g4`) plus the Neo4j 5 additions, marked `(Neo4j 5)` where openCypher lacks them |
| SQL | ISO/IEC 9075:2023 parts 2 (foundation) and 16 (SQL/PGQ), PostgreSQL 17 where the standard has no form, marked `(PG)` |
| Datalog | textbook stratified Datalog with Soufflé's concrete syntax (`parser.yy`), marked `(Soufflé)` where it is Soufflé's extension |

GQ 3 cells quote the production of the RFC's section 7 or today's
`query.pest`/`schema.pest` where the draft keeps it.

## 1. File and declarations

| GQ 3 | GQL | Cypher | SQL | Datalog |
|---|---|---|---|---|
| `gq 3;` edition line (`edition`) | none; `SESSION SET` for session state | `CYPHER 5` / `CYPHER 25` query prefix (Neo4j 5) | none; `SET` for session state (PG) | none; `.pragma` (Soufflé) |
| `query name($p: T = lit) @ann { read_body }`, a stored declared read (`query_decl`) | none in the language; procedures are catalog objects created outside GQL and invoked with `CALL` | none in openCypher; Neo4j has no stored Cypher (APOC `apoc.custom.declareProcedure`) | `CREATE VIEW v AS SELECT …` (no parameters); `CREATE FUNCTION f(p T) RETURNS TABLE (…) AS $$ … $$` (SQL/PSM, PG) | every rule is a named relation: `.decl r(x: T)` and `r(X) :- …` |
| `query name(…) [as commit \| as unattached branch] { mutation_body }`: the same keyword, the body's shape is the kind | none; write statements run inline inside a transaction | none | `CREATE PROCEDURE` (SQL/PSM) | none; pure Datalog has no writes |
| several declarations in one request, run on one snapshot and answered with one result each (a request shape, no keyword) | a procedure body: statements joined by `NEXT` | a script of `;`-separated statements | a transaction block | a program is a set of rules |
| `$p: Type = literal`, a typed parameter with a default (`param`) | `$p`, untyped, supplied by the session | `$p`, untyped | `$1` / `?` / `:p`, typed by context; `PREPARE p(int)`; function parameters with `DEFAULT` | none; an input relation stands in |
| `@name(pos, k = v)` on any declaration (`annotation`) | none | none; `OPTIONS { }` on index and constraint DDL (Neo4j 5) | `COMMENT ON`, `WITH (…)` storage options, function attributes (`IMMUTABLE`) | relation qualifiers `inline`, `overridable`, `.pragma` (Soufflé) |
| one grammar for schema and queries (`query_file` holds `schema_decl`) | one grammar: catalog, graph-type and query statements | none for schema in openCypher; Neo4j DDL (`CREATE CONSTRAINT`) shares the parser | one grammar: DDL and DML | one grammar: `.decl` and rules |

## 2. Read skeleton

| GQ 3 | GQL | Cypher | SQL | Datalog |
|---|---|---|---|---|
| `match { … } return { … } order { … }? limit n?`, fixed order, braces (`read_body`) | a linear statement: `MATCH … [LET \| FOR \| FILTER \| CALL \| ORDER BY \| LIMIT]* RETURN …`, clauses in any order, `NEXT` chains statements | `MATCH … WHERE … WITH … RETURN … ORDER BY … SKIP … LIMIT`, chained freely through `WITH` | `SELECT … FROM … WHERE … GROUP BY … HAVING … ORDER BY … LIMIT`, fixed order | `head(X, Y) :- body.`, no order beyond head and body |
| clauses separated by whitespace inside `match { }` (`clause+`) | keywords separate statements | keywords | keywords | `,` |
| `match { }` as the one pattern block | `MATCH <graph pattern>`, repeatable | `MATCH`, repeatable | `FROM … JOIN …`; SQL/PGQ `GRAPH_TABLE(g MATCH … COLUMNS (…))` | the rule body |
| no mid-query pipeline; `from { read }` nests instead | a second `MATCH`, `NEXT` | `WITH` | subqueries, CTEs | an auxiliary relation |
| `explain` as a statement prefix (`explain_stmt`) | none in the standard | `EXPLAIN` / `PROFILE` prefix | `EXPLAIN [ANALYZE]` | `.printsize`, `--profile` (Soufflé CLI) |

## 3. Patterns and bindings

| GQ 3 | GQL | Cypher | SQL | Datalog |
|---|---|---|---|---|
| `$x: Type { prop: expr, … }` (`binding`) | `(x:Type {prop: expr})` | `(x:Type {prop: expr})` | `FROM type_table x WHERE x.prop = expr` | `type(X), prop(X, V)` or `node(X, "Type", …)` |
| variable sigil `$x`; `$x` is also a parameter, told apart by declaration | bare `x`; `$x` is a parameter | bare `x`; `$x` is a parameter | an alias `x`; `$1` a parameter | capitalised `X`; no parameters |
| the type is required and one name, `UpperCamel` (`type_ref`) | label optional; label expressions `:A\|B`, `:A&B`, `:!A` | label optional; `:A\|B`, `:A&B`, `:!A` (Neo4j 5) | the table is required | the relation is required |
| an interface binds every implementor: `$n: Named` | none; labels are sets, a node may carry several | none | none (PG table inheritance is the nearest) | a rule per subtype, or a union relation |
| a property match value is any `expr`; a comparison is a filter clause, not a match | `(n {p: v})` literal map; `(n WHERE n.p > 1)` inside the element | `(n {p: v})`; `WHERE` outside | `WHERE` | a constraint in the body |
| no anonymous endpoint: every end is a variable | `()` and `-[]->` | `()` and `-->` | n/a | `_` |
| `$k:knows` binds the edge (`edge_binding`) | `-[k:KNOWS]->` | `-[k:KNOWS]->` | an alias on the edge table | an edge-id argument, `knows(X, Y, K)` |
| `a::b::Type` namespace segments (`type_ref`, a slot) | `/schema/path` and `USE graph` | none; `USE db` (Neo4j) | `schema.table` | none; Soufflé components `.comp` |
| `@Type`, `@Property`, catalogue types (a slot) | none; `SHOW` is vendor | `SHOW CONSTRAINTS`, `db.schema.visualization()` (Neo4j) | `information_schema`, `pg_catalog` | none |

## 4. Traversal

| GQ 3 | GQL | Cypher | SQL | Datalog |
|---|---|---|---|---|
| `$a e $b`, a triple, either written order, the schema fixes the orientation (`traversal`) | `(a)-[:E]->(b)` or `(b)<-[:E]-(a)`; the arrow is mandatory and carries the direction | same | `JOIN e ON e.src = a.id JOIN b ON b.id = e.dst`; SQL/PGQ arrows | `e(A, B)`, positional, no arrow |
| `$a <e> $b`, either orientation, deduplicated (`undirected_edge`) | `-[:E]-` (any direction), `~[:E]~` (undirected edges) | `-[:E]-` | a `UNION` of the two joins | two rules |
| `(e1 \| e2)` alternation, per-member orientation (`edge_alternation`) | `-[:E1\|E2]->` | `-[:E1\|E2]->` | `WHERE e.type IN (…)` | two rules, or a union relation |
| `*` any edge (`edge_wildcard`) | `-[]->` | `-[]->`, `-->` | a union over the edge tables | `edge(X, Y, _)` |
| `e{m,n}`, `*{m,n}`: bounded hops, one row per endpoint pair at its shortest distance (`traversal_bounds`) | `-[:E]->{m,n}`, `((a)-[:E]->(b)){m,n}`; one row per path; path modes `WALK \| TRAIL \| ACYCLIC \| SIMPLE`; `ANY SHORTEST`, `ALL SHORTEST`, `SHORTEST k`, `ANY k` | `-[:E*m..n]->`, one row per path (TRAIL: no relationship repeats); `shortestPath()`, `allShortestPaths()`; quantified path patterns `{m,n}` and `SHORTEST k` (Neo4j 5) | `WITH RECURSIVE` with a depth column | a recursive rule with a depth argument |
| unbounded reachability: refused (an open upper bound on `Expand` is the named later change) | `-[:E]->+`, `->*`, `{1,}` | `-[:E*]->` | `WITH RECURSIVE` | native: `path(X, Y) :- edge(X, Z), path(Z, Y).` |
| a path value: refused | `p = (a)-[]->*(b)`, `PATH_LENGTH(p)`, `NODES(p)` | `p = …`, `nodes(p)`, `length(p)` | none in `GRAPH_TABLE` | none (a list encoding) |
| `$d "in" $coll`, a quoted edge name (`string_lit` in `edge_sel`) | `` `in` ``, backtick-delimited | `` `in` `` | `"in"` (a delimited identifier) | none |
| `@edge` of the catalogue (`catalogue_edge`, a slot) | none | none | none | none |
| the written-incoming order (`$c worksAt $p`) is accepted and `fmt` rewrites it | the arrow says it; no rewrite | the arrow says it; LangChain's `CypherQueryCorrector` is the rewrite, after generation | the join keys say it | the argument order says it |
| `-e->`, `<-e-`, `-e-`: recognised to print the triple as the fix (`arrow_traversal`) | the form itself | the form itself | n/a | n/a |

## 5. Predicates and blocks

| GQ 3 | GQL | Cypher | SQL | Datalog |
|---|---|---|---|---|
| a bare `expr` clause inside `match` (`filter`) | `WHERE` after `MATCH`, `WHERE` inside an element pattern, a standalone `FILTER` | `WHERE` | `WHERE`, `ON`, `HAVING` | a constraint atom, `X > 1` |
| `exists { clauses }` (`exists_block`) | `EXISTS { pattern }`, `EXISTS { nested query }` | `EXISTS { }` (Neo4j 5); openCypher 9 has the pattern predicate `WHERE (n)-->()` | `EXISTS (subquery)` | a body atom is existential |
| `not { clauses }` (`negation`) | `NOT EXISTS { }` | `NOT EXISTS { }`, `WHERE NOT (n)-->()` | `NOT EXISTS` | `!p(X)`, stratified |
| `count { p } op expr`, `sum(e) { p }`, `avg`, `min`, `max` as a predicate (`subquery_predicate`) and, new, as a value (`block_value`) | none as a block; `CALL { MATCH … RETURN count(*) AS n }` | `COUNT { }`, `COLLECT { }` (Neo4j 5); `size([(n)-->() \| 1])` in openCypher | a scalar subquery `(SELECT count(*) FROM … WHERE …)` | `N = count : { p(X, Y) }`, `S = sum Y : { … }` (Soufflé) |
| `optional { clauses }`, the one outer join (`optional_block`) | `OPTIONAL MATCH`, `OPTIONAL CALL` | `OPTIONAL MATCH` | `LEFT [OUTER] JOIN`, `LEFT JOIN LATERAL` | none; a second rule with negation supplies the default |
| an inner read sees outer variables by name, implicitly | `CALL (x, y) { … }`: an explicit import list (the variable scope clause) | `CALL { WITH x … }` importing `WITH` (Neo4j 4/5); `CALL (x) { … }` (Cypher 25) | `LATERAL` for a derived table; a correlated subquery elsewhere | shared variable names across atoms |
| a forward reference or a self-reference in `let` is an error | a variable must be bound before use | same | same | no order: any atom may bind |

## 6. Composition: the closure constructs

| GQ 3 | GQL | Cypher | SQL | Datalog |
|---|---|---|---|---|
| `let $v = expr`: a named value, in scope downstream (`let_clause`) | `LET v = expr` | `WITH *, expr AS v` | a computed column in `SELECT`; `CROSS JOIN LATERAL (SELECT expr) t(v)` | `V = expr` in the body |
| `let $r = { read }`: a named relation, adds no rows | none; relations are not values, a nested query stands where it is used | none; a list through `COLLECT { }` is the nearest | `WITH r AS (SELECT …)`, a CTE | `.decl r` with a rule |
| `from { match … return … }`: a correlated inline read, rows multiplied (`from_clause`, `inline_read`) | `CALL (x) { MATCH … RETURN … }`; `FOR x IN list` for a list | `CALL { WITH x … RETURN … }`; `UNWIND list AS x` for a list | `FROM (SELECT …) t`; `CROSS JOIN LATERAL (…)` | an atom over an auxiliary relation |
| `from name(args)`: a declared read, inlined at compile time (`call` in `relation_expr`) | `CALL proc(args) YIELD cols` | `CALL proc(args) YIELD cols` | `FROM f(args)` (a table function); `FROM view` | an atom `r(args, X)` |
| `from $r` | none | none | `FROM r` | an atom |
| columns arrive under their inner names; a collision is `T68` | `CALL { RETURN x }` exports `x` by name; a clash is an error | same; "variable already declared" | `t.col`: qualified names make a clash legal | positional head arguments |
| renaming only through a wrapper read's `return { $x as y }` | `RETURN x AS y` inside the nested query | same | `AS` anywhere; `t(a, b)` column alias list | head argument order |
| a `Call` chain must be acyclic and fit a plan budget (`T70`) | n/a | n/a | a recursive view is refused; `WITH RECURSIVE` is the recursion door | recursion is the point |
| a relation-valued parameter: refused | none | a list parameter | a table-valued parameter (T-SQL), an array (PG) | an input relation |
| a relation-valued column: refused | none; `LIST<…>` and `RECORD` values | lists and maps as values | arrays, `ROW`, JSON (PG) | records, ADTs (Soufflé) |
| union of two reads: no surface form (the core `Union` is reached only by alternation, `<e>` and `optional`) | `UNION [ALL \| DISTINCT]`, `EXCEPT`, `INTERSECT`, `OTHERWISE` | `UNION [ALL]` | `UNION \| INTERSECT \| EXCEPT [ALL]` | two rules with one head |
| a list to rows: no form | `FOR x IN list` | `UNWIND list AS x` | `FROM unnest($1)` (PG) | n/a |

## 7. Ranking and search

| GQ 3 | GQL | Cypher | SQL | Datalog |
|---|---|---|---|---|
| `l2(rel, field, $q)`: every row scored, exact (`Score`) | none | `vector.similarity.euclidean(a, b)` function (Neo4j 5.18) | pgvector `a <-> $q` operator (PG) | none; a user functor |
| `nearest(rel, field, $q, k: n)`: at most `n` rows, declared recall (`ApproxTopK`) | none | `CALL db.index.vector.queryNodes('idx', k, $q) YIELD node, score` (Neo4j 5) | `ORDER BY a <-> $q LIMIT k`: the planner picks the index, recall is a session setting (`hnsw.ef_search`) and not declared | none |
| `bm25(rel, field, $q)` (`Score`, lexical) | none | `CALL db.index.fulltext.queryNodes('idx', $q) YIELD node, score` (Neo4j 5) | `ts_rank_cd(tsv, to_tsquery($q))` with `@@` (PG) | none |
| `rrf(rel, rel, …, k: n)` (`Fuse`) | none | written by hand in `WITH` over two `CALL`s | written by hand over two CTEs with `row_number()` | none |
| the score is a column, `$distance` / `$score`, named by the function | n/a | `YIELD score` | an aliased expression | n/a |
| exact and approximate are different operators, never the planner's choice | n/a | a function versus a procedure: the same split, by accident of packaging | one spelling, the planner chooses; `SET enable_indexscan` to force | n/a |
| `fuzzy`, `search`, `match_text` registry calls, kept for good | none | `=~` regex; `apoc.text.fuzzyMatch` | `LIKE`, `SIMILAR TO`, `~`, `pg_trgm` `%` (PG) | `match(re, s)`, `contains(a, b)` (Soufflé) |

## 8. Return, order, limit

| GQ 3 | GQL | Cypher | SQL | Datalog |
|---|---|---|---|---|
| `return [distinct] { expr [as alias], … }` (`return_clause`) | `RETURN [DISTINCT] items [GROUP BY …]` | `RETURN [DISTINCT] items` | `SELECT [DISTINCT] items` | the head |
| `return { $n }`: a whole node as a cell | `RETURN n`, a node value | `RETURN n` | `SELECT t.*` or the row value `t` | an id |
| grouping is implicit: the non-aggregated items are the keys; no keys gives one row | implicit, plus an explicit `GROUP BY` | implicit | explicit `GROUP BY` required | implicit per aggregate: the variables outside the braces (Soufflé) |
| aggregates `count sum avg min max`; `count(distinct x)` the one special form (`agg_distinct`) | adds `COLLECT_LIST`, `STDDEV_SAMP`, `STDDEV_POP`, `PERCENTILE_CONT`, `PERCENTILE_DISC` | adds `collect`, `stDev`, `percentileCont`, `percentileDisc` | adds `string_agg`, `array_agg`, `FILTER (WHERE …)`, window functions `OVER (…)` | `count sum min max mean` (Soufflé) |
| `order { expr [asc \| desc], … }`; nulls `[as today]` | `ORDER BY expr [ASC \| DESC] [NULLS FIRST \| LAST]` | `ORDER BY expr [ASC \| DESC]` | `ORDER BY expr [ASC \| DESC] [NULLS FIRST \| LAST]` | none; relations are sets |
| ties broken by node identities in column order: a total order | unspecified | unspecified | unspecified | n/a |
| `limit n` or `limit $k` (`limit_clause`) | `LIMIT n`, `LIMIT $k` | `LIMIT n`, `LIMIT $k` | `LIMIT n` (PG), `FETCH FIRST n ROWS [ONLY \| WITH TIES]` | none; `.limitsize r(n=10)` (Soufflé) |
| `limit` without `order`: legal and deterministic | legal, unspecified rows | legal, unspecified | legal, unspecified | n/a |
| offset or skip: none (the paging stage, `continuation`) | `OFFSET n`, `SKIP n` | `SKIP n` | `OFFSET n [ROWS]` | none |
| an inner `order` and `limit` stay inside the inline read | the same inside `CALL { }` | the same inside `CALL { }` | an `ORDER BY` in a subquery is kept only with `LIMIT`, by implementation | n/a |

## 9. Expressions

| GQ 3 | GQL | Cypher | SQL | Datalog |
|---|---|---|---|---|
| `and or not`, three-valued (`or_expr`, `and_expr`, `not_expr`) | `AND OR NOT XOR`, three-valued, the literal `UNKNOWN` | `AND OR XOR NOT`, three-valued | `AND OR NOT`, three-valued | `,` conjunction, `;` disjunction (Soufflé), `!` negation; two-valued |
| `= != < <= > >=`, non-associative (`comp_op`) | `= <> < <= > >=` | `= <> < <= > >=` | `= <> < <= > >=` (`!=` in PG) | `= != < <= > >=` |
| `x in [list]` (`kw_in` in `comparison`) | `x IN list` | `x IN list` | `x IN (…)`, `x = ANY(array)` | none; a relation |
| `x is [not] null` (`null_test`) | `IS [NOT] NULL` | `IS [NOT] NULL` | `IS [NOT] NULL`, `IS [NOT] DISTINCT FROM` | no nulls |
| arithmetic: the core names it and section 7 writes `0.7 * (…) + 0.3 * $lex`, but neither today's grammar nor section 6 has a production for it | `+ - * / %`, unary `-` | `+ - * / % ^`, unary `-` | `+ - * / %`, unary `-` | `+ - * / % ^`, `band bor bxor`, unary `-` (Soufflé) |
| negative numbers: no unary minus and no signed literal in `query.pest` (`schema.pest` has `signed_integer` for `@range`) | `-1` | `-1` | `-1` | `-1` |
| float literal `1.5` only; no exponent form | `1.5`, `1e3` | `1.5`, `1e3` | `1.5`, `1e3` | `1.5`, `1e3` (Soufflé) |
| string predicates as registry calls: `starts_with($s, "x")`, `contains($s, "x")`; the infix forms print the call as the fix | none in 39075 (string functions `UPPER`, `TRIM`, `CHAR_LENGTH` and `\|\|` only) | `STARTS WITH`, `ENDS WITH`, `CONTAINS`, `=~` | `LIKE`, `SIMILAR TO`, `~` (PG), `\|\|` | `contains(a, b)`, `match(re, s)`, `cat`, `substr`, `strlen` (Soufflé) |
| string concatenation: none | `\|\|` | `+`, `\|\|` (Neo4j 5) | `\|\|` | `cat(a, b)` |
| conditional: none (`CASE`, `coalesce` would be registry rows) | `CASE … END`, `COALESCE`, `NULLIF` | `CASE … END`, `coalesce` | `CASE … END`, `COALESCE`, `NULLIF` | a rule per case |
| type test: `$x.@type = "Person"` today, `$x is Person` once the polymorphism RFC lands | `x IS [NOT] LABELED :Person`, `x IS TYPED INT` | `x:Person`, `labels(x)`, `x IS :: INTEGER` (Neo4j 5) | none in general; `pg_typeof` (PG) | a relation per type |
| meta-fields `$x.@id`, `$e.@src`, `$e.@dst`, `$e.@type` (`meta_field`) | `ELEMENT_ID(x)`, `e IS SOURCE OF`, `e IS DESTINATION OF` predicates | `elementId(x)`, `startNode(e)`, `endNode(e)`, `type(e)` | columns of the table | arguments |
| `$x.prop`, one level deep (`prop_access`) | `x.prop`, nested record access `x.a.b` | `x.prop`, `x["prop"]`, map projection `x {.a, .b}` | `t.col`, `(row).field`, `j->'k'` | an argument of the relation |
| list literal `[lit \| call, …]`; `in` is the only list operation (`list_lit`) | lists: literal, index `l[0]`, slice, `SIZE`, `IN` | lists: literal, index, slice, comprehension `[x IN l WHERE … \| …]`, `reduce`, `range` | arrays (PG): `ARRAY[…]`, `a[1]`, `unnest`, `cardinality` | none; records `[a, b]` are tuples |
| map or record literal: none | `{a: 1}` records | `{a: 1}` maps | `ROW(…)`, `json_build_object` (PG) | records, ADTs (Soufflé) |
| `name(pos…, opt: v)`: every function a registry call, positional or named by the registry, never both (`call`, `option`) | the standard's built-ins only; no user functions in the language | built-ins; user functions and procedures in Java | built-ins; `CREATE FUNCTION`; named arguments `f(a => 1)` | built-in functors; `.functor` in C++ (Soufflé) |
| `now()` | `CURRENT_TIMESTAMP`, `CURRENT_DATE` | `datetime()`, `date()` | `CURRENT_TIMESTAMP`, `now()` | none |
| `date("…")`, `datetime("…")`: calls the compiler folds to literals | `DATE '…'`, `ZONED DATETIME '…'` typed literals | `date("…")`, `datetime("…")` | `DATE '…'`, `TIMESTAMP '…'` | none |
| `(expr)` grouping | same | same | same | same |

## 10. Literals, types and nulls

| GQ 3 | GQL | Cypher | SQL | Datalog |
|---|---|---|---|---|
| strings `"…"` only; `'…'` is an error | `'…'` and `"…"` | `'…'` and `"…"` | `'…'`; `"…"` delimits identifiers | `"…"` |
| `true`, `false` (`bool_lit`) | `TRUE FALSE UNKNOWN` | `true false` | `TRUE FALSE UNKNOWN` | none; `1`/`0` |
| `null` legal only as a whole mutation assignment value; a `null` in a read is a parse error | `NULL` anywhere | `null` anywhere | `NULL` anywhere | no nulls |
| property types `I32 I64 U32 U64 F32 F64 Bool String Date DateTime Blob Vector(n) [T] enum(…)`; `?` opts into null (`type_ref`) | `INT8 … INT256`, `UINT*`, `FLOAT32 FLOAT64`, `STRING`, `BOOL`, `DATE`, `ZONED DATETIME`, `LIST<T>`, `RECORD`; `NOT NULL` opts out of null | no declared types in openCypher; Neo4j property types `INTEGER FLOAT STRING BOOLEAN DATE ZONED DATETIME LIST<T>`, type constraints `IS :: T` (Neo4j 5) | `INTEGER BIGINT REAL DOUBLE PRECISION TEXT BOOLEAN DATE TIMESTAMP BYTEA`, arrays, `vector(n)` (pgvector), enums (`CREATE TYPE`); `NOT NULL` opts out | `symbol number unsigned float`, `.type` aliases, records, ADTs (Soufflé) |
| `Vector(n)` as a first-class property type | none | none (a `LIST<FLOAT>` with a vector index) | `vector(n)` extension type (PG) | none |
| `Blob` | none | none | `BYTEA` | none |
| enum `enum(a, b)` | none | none | `CREATE TYPE … AS ENUM` (PG) | none |
| `Date`, `DateTime` | `DATE`, `ZONED DATETIME`, `LOCAL DATETIME`, `DURATION` | `date`, `datetime`, `localdatetime`, `duration` | `DATE`, `TIMESTAMP [WITH TIME ZONE]`, `INTERVAL` | none |
| parameter kinds `Branch`, `Commit` (a slot) | none | none | none | none |

## 11. Writes

| GQ 3 | GQL | Cypher | SQL | Datalog |
|---|---|---|---|---|
| writes in a `query` whose body ends in statements, after a read's clauses; the kind is the body's shape, known at parse time, as in SQL, Cypher and GQL, not a head keyword as in GraphQL | reads and writes mix in one linear statement: `MATCH … INSERT … SET … RETURN …` | `MATCH … CREATE … SET … DELETE … RETURN …` | `INSERT … SELECT`, `UPDATE … FROM`, `DELETE … USING` | none; delta rules in Dedalus and LogicBlox |
| `insert Type { prop: expr, … }` (`insert_stmt`) | `INSERT (n:Type {p: v})` | `CREATE (n:Type {p: v})` | `INSERT INTO t (cols) VALUES (…)` | a fact `t(…).` |
| an edge by its endpoint keys: `insert WorksAt { from: $h, to: $slug }` (`from`/`to` reserved assignment names) | `INSERT (a)-[:E]->(b)` on bound variables | `CREATE (a)-[:E]->(b)` on bound variables | `INSERT INTO e (src, dst) VALUES (…)` | a fact |
| `update $x set { p: v }` over a bound entity; edition 2's `update T set { … } where φ` is rewritten to a binding and filter (`update_stmt`) | `MATCH … SET n.p = v`, `REMOVE n.p` | `SET n.p = v`, `SET n += {…}`, `REMOVE n.p` | `UPDATE t SET p = v WHERE …` | none |
| `delete $x` over a bound entity; edition 2's `delete T where φ` is rewritten (`delete_stmt`) | `MATCH … DELETE n`, `DETACH DELETE`, `NODETACH DELETE` | `DELETE n`, `DETACH DELETE n` | `DELETE FROM t WHERE …` | none |
| upsert: outside the language (`load --mode`) | none | `MERGE … ON CREATE SET … ON MATCH SET …` | `MERGE INTO … WHEN MATCHED …`, `INSERT … ON CONFLICT` (PG) | n/a |
| an edge by bound references: `$p $k:worksAt $c  delete $k` | the pattern binds the endpoints | the pattern binds the endpoints | `DELETE … USING` with a join | n/a |
| `as commit`, `as unattached branch` on the declaration | `START TRANSACTION`, `COMMIT`, `ROLLBACK` as statements | transactions are the driver's | `BEGIN`, `COMMIT`, `ROLLBACK` | none |
| `returning { @id }` (a slot) | `INSERT … RETURN n` | `CREATE … RETURN n` | `RETURNING …` (PG); `NEW TABLE (INSERT …)` (standard) | n/a |

## 12. Schema

| GQ 3 | GQL | Cypher | SQL | Datalog |
|---|---|---|---|---|
| `node Type: Iface @ann { p: T @ann  … }` (`node_decl`) | `CREATE GRAPH TYPE t AS { (Type :Label {p STRING NOT NULL}) }` | none in openCypher; `CREATE CONSTRAINT FOR (n:Type) REQUIRE n.p IS :: STRING` (Neo4j 5) | `CREATE TABLE t (p TEXT NOT NULL)`; SQL/PGQ `CREATE PROPERTY GRAPH g VERTEX TABLES (t KEY (id) LABEL Type PROPERTIES (p))` | `.decl type(id: symbol, p: symbol)` |
| `edge E: A -> B @card(m..n) { … }` (`edge_decl`) | `(:A)-[E :Label {…}]->(:B)` edge type pattern; no cardinality | none | an edge table with two `REFERENCES`; SQL/PGQ `EDGE TABLES (e SOURCE KEY … DESTINATION KEY …)`; cardinality by `UNIQUE` | `.decl e(a: symbol, b: symbol)` |
| `interface Name { … }` and `: Name` on a node | none; a label set | none | none | none |
| `@key`, `@unique`, `@index`, `@range`, `@check` (`body_constraint`) | none in 39075 beyond types and `NOT NULL` | `IS UNIQUE`, `IS NODE KEY`, `IS NOT NULL`, `CREATE INDEX`, `CREATE VECTOR INDEX`, `CREATE FULLTEXT INDEX` (Neo4j) | `PRIMARY KEY`, `UNIQUE`, `CHECK`, `CREATE INDEX … USING hnsw \| gin` | none; `choice-domain` (Soufflé) |
| `@index(fulltext, analyzer = …)`, `@embed(…)` on a property | none | index DDL, not a property attribute | `tsvector` column plus `GIN`; the embedding is the application's | none |
| `@description` on types, edges and properties | none | none | `COMMENT ON` | none |
| schema change: `schema apply` outside the language | `CREATE` / `DROP GRAPH TYPE` | `DROP CONSTRAINT` | `ALTER TABLE` | edit the program |

## 13. Statements outside a read

| GQ 3 | GQL | Cypher | SQL | Datalog |
|---|---|---|---|---|
| `show …`, `branch …`, `commit …`, `load …`, `set x = v;` | `SESSION SET`, `USE graph`, `CREATE \| DROP GRAPH`, `START TRANSACTION` | `USE db`, `SHOW INDEXES`, `LOAD CSV`, `CREATE DATABASE` (Neo4j) | `SET`, `COPY`, `BEGIN`, `\d` (psql) | `.input`, `.output`, `.printsize`, `.pragma` (Soufflé) |
| branches, commits and merges as language objects | none | none | none; `FOR SYSTEM_TIME AS OF` is time travel on one table; Dolt adds `AS OF` and branches outside the standard | none |
| `explain` | none in the standard | `EXPLAIN`, `PROFILE` | `EXPLAIN` | none |
| a missing `()` on a declaration: a diagnostic the instrument measures | n/a | n/a | n/a | n/a |

## 14. Semantic rules

| Rule | GQ 3 | GQL | Cypher | SQL | Datalog |
|---|---|---|---|---|---|
| rows | a bag; `distinct` opts out | bag | bag | bag | set |
| null | three-valued; a null is a group key; `limit` refuses a null | three-valued | three-valued | three-valued | none |
| grouping | implicit | implicit, `GROUP BY` optional | implicit | explicit | implicit per aggregate |
| pattern matching | homomorphism: two triples may bind one node unless `!=` says otherwise; `Expand` is set-semantic over endpoint pairs | `REPEATABLE ELEMENTS` (homomorphism) or `DIFFERENT EDGES` (edge isomorphism), declared per `MATCH` | relationship isomorphism within one `MATCH`; nodes may repeat | homomorphism (joins) | homomorphism |
| variable-length rows | one per endpoint pair at the shortest distance | one per path, mode declared | one per path (TRAIL) | whatever the recursion enumerates | the reachable set |
| closure | a relation expression in `from` and `let`; no relation-valued cell | a nested query in `CALL` and `EXISTS` only; no relation value | the same, plus lists as pseudo-relations (`collect`, `UNWIND`) | subqueries anywhere, CTEs, derived tables; no relation-valued cell (arrays and JSON stand in) | every relation is named and first-class |
| recursion | none; bounded `Expand`, `Call` acyclic | quantified paths only | none beyond `*` paths | `WITH RECURSIVE` | native, stratified |
| exact versus approximate | two operators, the text says which, recall declared | n/a | a function versus a procedure | the planner's choice, undeclared | n/a |
| order ties | a total order over node identities | unspecified | unspecified | unspecified | n/a |
| totality | every read terminates with a declared cost bound (A1) | terminates | terminates | `WITH RECURSIVE` need not | stratified Datalog terminates; functors can loop |
| data complexity | polynomial, degree set by `Correlate` and `Expand` nesting | polynomial with path quantifiers | polynomial; lists widen it | polynomial, recursion included; procedures are Turing complete | polynomial; Turing complete with functors |
| expressive class | first-order with counting and ordering, plus two declared-quality operators; below Datalog, above relational algebra | first-order with counting plus regular path queries | the same with list comprehension | relational algebra plus aggregation, windows and linear recursion | least fixpoint over Horn clauses |

## 15. Forms the four have and GQ 3 does not, classified

| Form | Where it exists | GQ 3 status |
|---|---|---|
| unbounded reachability, `*`, `WITH RECURSIVE`, recursive rules | all four | refused by decision (A1); an open upper bound on `Expand` is the named later change |
| path values | GQL, Cypher | refused by decision |
| relation-valued columns, lists of rows, maps as values | GQL, Cypher, SQL (arrays, JSON), Datalog (records) | refused by decision (first-order algebra) |
| reads feeding writes in one statement | GQL, Cypher, SQL | present since 2026-11-04: a write body's clauses feed its statements; a write inside a read stays refused (A7) |
| `WITH` / `NEXT` pipelines | GQL, Cypher | derived: `let` and `from` |
| `LEFT JOIN`, `OPTIONAL MATCH` | GQL, Cypher, SQL | present: `optional { }` |
| `HAVING` | SQL | derived: a filter after `from { … }` |
| `GROUP BY` | SQL, GQL | implicit |
| `CALL { }` subquery | GQL, Cypher | present as `from { }` |
| `RETURNING`, `RETURN` after a write | all three | a slot (`returning`) |
| `OFFSET` / `SKIP` | all three | a slot (the paging stage) |
| `IS LABELED`, `x:Label` as a predicate | GQL, Cypher | a slot (`is`, the polymorphism RFC) |
| `UNION` of two reads | all four | **gap**: the core has `Union`; the surface has no form for a reader to union two reads |
| a list to rows (`FOR`, `UNWIND`, `unnest`) | GQL, Cypher, SQL | **gap**: `from` takes a relation only, and no form turns a list parameter into rows |
| arithmetic, unary minus, negative and exponent literals | all four | **gap in the text**: the core and section 7 use arithmetic; section 6 and `query.pest` have no production |
| `CASE`, `COALESCE`, `NULLIF` | GQL, Cypher, SQL | absent; the registry rule admits `coalesce(a, b)` and `if(c, a, b)` as rows without a grammar change |
| string concatenation | all four | absent; a registry row (`concat`) |
| `NULL` as a read literal | GQL, Cypher, SQL | refused today; the comparison `is null` covers the common case, `coalesce` would cover the rest |
| anonymous endpoints `()`, `_` | GQL, Cypher, Datalog | absent by design: every end is a variable |
| `MERGE` / upsert in the language | Cypher, SQL | outside the language (`load --mode`) |
| `NULLS FIRST \| LAST` | GQL, SQL | absent; the null order is fixed `[as today]` |
| `WITH TIES` | SQL | absent |
| window functions | SQL | absent; `rrf` is the one rank operator |
| `COLLECT`, `array_agg`, `COLLECT_LIST` | GQL, Cypher, SQL | absent; the relation is the collection, and nesting is the transport's |
| a label set per node, `:A&B` | GQL, Cypher | absent; one type, interfaces for sharing |
| user-defined scalar functions | SQL, Cypher (Java) | absent; the registry is a compiler table |

## 16. Forms GQ 3 has and none of the four has

| Form | Note |
|---|---|
| a declared, typed, defaulted parameter list on a stored read | SQL functions come closest; no query language declares `$p: T` |
| exact and approximate ranking as two operators, with declared recall and an exhaustive flag in the result | SQL and Neo4j leave the choice to the planner or the packaging |
| `rrf` as an operator | hand-written elsewhere |
| a triple traversal with schema-resolved orientation and a formatter that writes schema order | Datalog is positional but has no schema to resolve against; the others carry the arrow |
| the one-spelling rule with a fix printed for each respelling | `gofmt -s` is the precedent, outside query languages |
| `@card(m..n)` on an edge type | SQL expresses one side through `UNIQUE` |
| branches, commits, `as unattached branch`, `merge-base`, `diff` as statements | Dolt adds branches to SQL outside the standard |
| a total order on ties through node identity | every other language leaves ties unspecified |
| a plan budget on inlined calls (`T70`) | recursion limits exist (PG `max_stack_depth`), not a static budget |
| a nullable-by-opt-in type system (`T?`) on a graph schema | GQL and SQL are nullable by default with `NOT NULL` |
| `Vector(n)` and `Blob` as property types with `@embed` and `@index(fulltext)` on the declaration | pgvector adds the type; no language declares the embedding on the schema |
| a catalogue (`@Type`, `@Property`) queried with the same traversal grammar | `information_schema` is tables; GQL has no reflection in the standard |

## 17. Findings for the draft

Ordered by what a reader of the RFC hits first. All six were folded into the RFC on 2026-11-04 (its decision log names where).

1. **Arithmetic has no production.** Section 2 lists arithmetic among the
   scalar expressions, section 7's `weighted` example writes `0.7 * (1.0 /
   (1.0 + $vec)) + 0.3 * $lex`, and `query.pest` has no `+ - * /`, no unary
   minus and no signed literal. Section 6 must add the ladder (`add_expr`,
   `mul_expr`, `unary`) between `comparison` and `operand`, say where
   `-` sits against the arrow-traversal recognizer (the grammar RFC's reason
   for dropping the arrow was partly this operator), and name the numeric
   contract (integer overflow, division by zero, mixed-width promotion;
   PR 1244's numerical contract is the draft to take).
2. **The edge `in` collides with the reserved word.** `cited_near` writes
   `$d in $x` over `edge In: Doc -> Collection`; `in` is one of the six
   reserved words, so the triple is unparseable as written, and section 7's
   `shapes` already shows the quoted form `$d "in" $coll`. Either the schema
   example renames the edge (`InCollection`) or every use quotes it; the
   lowercase edge name is `in` under today's camelCase rule.
3. **`$d: Doc { year >= 2025 }` is not a property match.** `prop_match` is
   `ident ":" expr`; a comparison belongs in a filter clause (`$d: Doc  $d.year
   >= 2025`). GQL's `(n WHERE …)` is the form the example reaches for; the
   draft either adds it (one more spelling for a filter, against A6) or fixes
   the example.
4. **No surface union.** The core has `Union` and the closure table says its
   arms must agree by type, but no form lets a reader union two reads; the
   only paths to the operator are alternation, `<e>` and `optional`. Every
   comparator has one. `from` over two inline reads cannot express it
   (that is a product). Either `union { read } { read }` as a relation
   expression, or the decision log says why relational completeness at the
   core is enough without it at the surface.
5. **No list to rows.** A `[String]` parameter can be tested with `in` but
   not enumerated; GQL `FOR`, Cypher `UNWIND` and SQL `unnest` all do this.
   `from $list` with a list-typed operand is one production; refusing it is a
   decision the Alternatives table should carry.
6. **Conditionals and concatenation.** `CASE`, `coalesce` and `||` are in
   every comparator. The registry rule admits them as rows (`coalesce`, `if`,
   `concat`) without a grammar change; the draft should say so in the
   registry paragraph so a reader does not look for `CASE`.
7. **Nearest kin by area**, for the precedent sections: the read skeleton
   and the two clauses are GQL's (`LET`, `CALL (x) { }`, `OPTIONAL`,
   `EXISTS`, `RETURN DISTINCT`, `LIMIT $k`, `$p`) with SQL's word `from`;
   the traversal is Datalog's (positional, schema-resolved, no arrow); the
   expression language is the smallest of the five, and it is the part the
   draft under-specifies.
