# GQ, evaluated from first principles

**Subject:** the Omnigraph query language (`.gq`) and schema language (`.pg`) as
of 0.14.1 (`ea895066`), read from `query.pest`, `schema.pest`, the user guides,
the language RFCs (shared expression model, typed edge alternation, branch
statements, search plan truth, engine catalogue, bulk load) and the GQT corpus (425
cases).
**Compared against:** SQL (with SQL:2023 SQL/PGQ, recursive CTEs, pgvector),
ISO GQL (39075:2024), Cypher (openCypher and Neo4j Cypher 25), GraphQL,
Datalog (classical, Soufflé, Datomic, CozoDB, Rel), Elasticsearch (Query DSL
and ES|QL), LanceDB (query builder and DataFusion SQL), and Vespa (YQL,
grouping, rank profiles).
**Status:** a design evaluation, not an RFC. Where it recommends, it ranks.
**Outcome:** the [GQ 3 grammar RFC](../2026-10-28-gq-3-grammar.md), which
cites this document as its evidence; §12 to §14 were added after that RFC
was drafted and are folded into it.

---

## 0. Method

Omnigraph's own systems paper (`docs/dev/systems.md`) defines a database as an
observable contract (state, operations, guarantees, interfaces) implemented by
physical mechanisms for a distribution of work. A query language is the
*interface* part of that contract: the notation in which a caller names a
subset of the state and the operations over it, and from which the guarantees
(typing, ordering, atomicity, bounds) are either derivable or not.

So the evaluation does not ask "is GQ like Cypher". It asks, for each thing a
query language must do, what GQ chose, what the alternatives chose, and what
each choice costs after five more changes of the same kind. The axes:

| # | Axis | The question |
|---|---|---|
| 1 | Model fidelity | Does the vocabulary name exactly the state model, no more, no less? |
| 2 | Compositional closure | What is the unit of composition, and does the language close over it? |
| 3 | Binding and scope | How are variables introduced, joined and scoped? |
| 4 | Typing | Static or dynamic; declared or inferred; how are coercions and nullability handled? |
| 5 | Absence | What does a missing value mean, and how is it tested? |
| 6 | Traversal | Reachability semantics, path identity, recursion bounds, direction. |
| 7 | Sub-pattern predicates | Negation, existence, correlated counting; the safety rule. |
| 8 | Aggregation | Grouping discipline, post-group filtering, numeric exactness. |
| 9 | Order and cut | Totality of order, paging, determinism. |
| 10 | Retrieval | Full-text, vector and fusion; how ranking interacts with predicates and traversal. |
| 11 | Writes | Mutation shape, atomicity, read-write mixing, idempotence. |
| 12 | Time and branches | Snapshots, history, branches, in or beside the language. |
| 13 | Reuse | Named queries, views, rules, fragments, functions. |
| 14 | Expressions | Arithmetic, scalar functions, conditionals, collections. |
| 15 | Result shape | Flat rows versus nested trees; whole entities; paths. |
| 16 | Self-description | Explain, catalogue, schema-as-data. |
| 17 | Refusal contract | Error codes, positions, fixes; suitability for machine authorship. |

The workload GQ is built for is named in the README and the schema-design
guide: many agents sharing one typed world model, retrieving high-precision
context and writing reviewable changes. Every verdict below is relative to that
workload, not to analytics, OLTP or web APIs.

---

## 1. GQ in one page

```gq
set ann_nprobes = 40;

query customer_evidence($slug: String, $q: String, $least: I64) {
  match {
    $c: Customer { slug: $slug }           // typed binding with inline equality
    $a: Assertion
    $a about $c                            // directed edge, direction from the schema
    $a $e:(statedIn | derivedFrom) $s      // bound edge over an alternation
    $s.published_at is not null
    ($s.kind = "primary" or $s.kind = "audit") and not $a.retracted
    count { $x supports $a } >= $least     // correlated block, stratified
    not { $a supersededBy $_ }
  }
  return { $a.statement, $e.@type as via, $s.title, count($x) as support }
  order { nearest($a.embedding, $q) }      // ranking lives in order
  limit 10
}
```

What the one shape fixes:

- **One declaration form.** Every executable unit is `query name(params) { body }`.
  The body is a read (`match` → `return` → `order`? → `limit`?) or a mutation
  (`insert`/`update`/`delete` statements, constructive *or* destructive, never
  both). A file is either declarations or exactly one statement (`branch …`,
  `commit …`, `load csv …`, `show …`, `explain …`), optionally prefixed by `set`
  lines.
- **Schema-first, statically typed.** Node and edge types, properties, keys,
  nullability, enums, vectors and Blobs come from `.pg`; every binding, edge,
  property and parameter is checked against it before planning. 51 stable `T`
  codes, 5 `Q` codes, 3 `P` codes.
- **Pattern conjunction by juxtaposition.** A `match` block is a set of clauses:
  bindings, triples (`$a edge $b`), Boolean filters, and correlated blocks
  (`not { }`, `exists { }`, `count { } op v`, `sum(e) { } op v`). Clause order
  is irrelevant to meaning except that a variable must be bound in a visible
  scope.
- **Triples, not arrows.** `$p worksAt $c` is subject–predicate–object; the
  schema fixes direction, so reversing a traversal is swapping the ends.
  `<knows>` is either direction, `(a | b)` and `*` select edge types, `{m,n}`
  bounds hops.
- **Reachability, not paths.** An unbound traversal has set semantics over
  endpoint pairs; `{m,n}` means shortest-distance *m..n* from the source; no
  path is ever bound; edge bindings are single-hop only.
- **Ranking is an order key.** `nearest`, `bm25`, `rrf` lead `order { }`,
  select the retrieved population *and* sort it; the projected score must repeat
  the order key; `limit` is required for `nearest`.
- **Writes are statements over one type with constant values.** Keyed insert is
  upsert; `where` is the same expression language as a read filter; no
  read-write mixing, no computed assignment, no edge update.
- **Control is in the language.** Branch create/delete/merge/list/diff/
  merge-base, unpublished commits (`as commit`, `commit publish|drop`),
  unattached snapshots, CSV load and `explain` are GQ statements.
- **The engine is a graph.** `@Operator`, `@Pass`, `@Setting`, `@Engine`,
  `@LogicalKind` are queryable node types.

---

## 2. The axes

### 2.1 Model fidelity

**GQ.** The grammar names exactly the state model: `node`/`edge`/`interface`
types; `String`, `Bool`, four integer widths, two float widths, `Date`,
`DateTime`, `Vector(N)`, `Blob`, `enum(...)`, `[T]`, `T?`; `@key`, `@unique`,
`@index`, `@range`, `@check`, `@card`; system fields `@id`, `@src`, `@dst`,
`@type`. Nothing in the query language exists that the storage model does not
have (no maps, no untyped JSON, no arbitrary labels), and nothing in the storage
model is unreachable from the language except Blob *values*, which are
deliberately routed to a dedicated byte API.

This is the tightest model–language fit of the nine systems. Cypher and GQL
describe a schema-*optional* graph (labels are sets, properties are open maps),
so their syntax carries machinery GQ does not need (label expressions `:A&B|C`,
`properties()`, `keys()`, dynamic property access `n[$k]`, `labels(n)`) and a
type system that is resolved at run time. SQL describes tables; SQL/PGQ layers a
graph *view* over them (`CREATE PROPERTY GRAPH … VERTEX TABLES … EDGE TABLES`),
which is powerful for existing relational data and heavy for a native graph.
GraphQL describes an API, not storage: its types are resolver signatures, so the
language is complete for shape and silent on filtering, ordering and
aggregation. Datalog describes relations; property graphs are an encoding
(Datomic's EAV, Soufflé's typed relations, Rel's modular relations), so every
"edge" is a predicate and every "property" another, which is maximally uniform
and minimally ergonomic. Elasticsearch and Vespa describe documents with
mappings/schemas and treat relationships as a special case (nested, parent–child,
imported fields). LanceDB describes Arrow tables and has no relationship
vocabulary at all.

**Verdict.** Keep. The schema-first, closed-vocabulary model is the foundation
of everything else GQ does well (static typing, plan acceptance, exact
diagnostics, deterministic results). The one fidelity gap is that `.pg` is
declared but not *queryable* from GQ (see 2.16).

### 2.2 Compositional closure

This is the axis on which GQ is most unlike every comparator, and the one whose
consequences show up in the most open issues.

**GQ.** A query is one fixed pipeline: `match` → `return` → `order` → `limit`.
Inside `match`, the only nesting is the correlated block, which is a predicate,
not a value. There is no construct that takes a query's result as input to
another query: no `WITH`/pipeline, no subquery in `return`, no `UNION`, no
view, no derived relation, no CTE. The result of a query is rows; nothing in the
language consumes rows.

Every comparator closes over something:

| System | Unit of closure | Mechanism |
|---|---|---|
| SQL | table | subquery, derived table, CTE (`WITH … AS`), view, `UNION`, lateral join, recursive CTE |
| Cypher | binding table (rows of variables) | `WITH` pipeline, `CALL { }` subquery, `UNION`, `UNWIND`, `COLLECT {}` |
| GQL | binding table | linear composition of statements, `CALL`, `UNION`/`EXCEPT`/`INTERSECT`, nested queries |
| GraphQL | selection set over a type | nesting; no closure over results (cannot query a result) |
| Datalog | relation | rules; a rule head is a relation any other rule may use; recursion |
| Elasticsearch | ranked hit list; bucket | retriever tree (`rrf` of retrievers of retrievers), sub-aggregations |
| LanceDB | Arrow table | DataFusion SQL closes; the fluent builder does not |
| Vespa | match set + rank | `rank()` composes operators; grouping pipelines compose over groups |

What GQ buys by not closing:

- The planner sees the whole query as *one* component shape. That is why plan
  acceptance can check an "exact subset" against a canonical form, why every
  read node lowers to exactly one operator, why `explain` can be a flat table,
  and why plan replay is a contract. Closure would turn validation into a
  per-stage problem.
- Every query is bounded by construction: one scan root per connected
  component, deferred bindings, streaming `Expand`, one `Sort`, one `Limit`.
  There is no place to write an unbounded cross-stage fan-out.
- An agent generating GQ cannot be wrong about *structure*; only about content.

What it costs, with the issue that reports it:

| Shape that needs closure | Issue / symptom |
|---|---|
| Optional traversal (keep the root when the edge is absent) | #658, P-high, needs-rfc |
| A block aggregate as a *value* (`count { } as n` in return) | #1050 |
| Top-N per group, "the latest assertion per customer" | not expressible; needs a second stage |
| Filter on a group aggregate (`HAVING`) | not expressible: block aggregates are per outer row, return aggregates are post-order |
| Union of heterogeneous types for discovery | #648 |
| One entity plus its neighbourhood in one answer | #649 |
| Reusing a derived relation across queries | no views/rules; see 2.13 |

**Verdict.** The non-closure is a *deliberate* trade and it has been paying
for itself: plan truth, bounded execution and the diagnostics all rest on it.
But every open language-feature issue (#658 P-high and needs-rfc, #1050,
#648, #649) is a closure shape, which is the five-more-changes signal: each will be solved ad hoc (an `optional { }`
block here, a projected block there) unless one closure mechanism is chosen.
The recommendation in §4 is a *single* closure step, not SQL-style general
subqueries: a `with`-free linear pipeline is not needed; what is needed is (a)
correlated blocks that yield a value (which already have the operator,
`SubqueryAggregate`), (b) an optional block, and (c) derived relations as
stored, typed queries usable as bindings. Those three cover every row above
without making the planner's job per-stage.

### 2.3 Binding and scope

**GQ.** `$var` is introduced by a typed binding (`$p: Person`), by appearing
as a traversal endpoint (its type inferred from the edge's declared endpoints),
or by an edge binding (`$e:knows`). Join is by shared variable, Datalog-style:
mentioning `$p` twice conjoins. Re-declaring `$p: Person { x: 1 }` adds a
constraint rather than a second binding; `$_` is a fresh anonymous node each
time; blocks see outer variables declared *before* them in text (a later outer
declaration is not visible to an earlier block); `__`-prefixed names are
reserved.

Cypher and GQL bind the same way inside one `MATCH` (shared variables conjoin;
anonymous nodes are `()`), but across clauses the rule is lexical and
*positional*: a variable survives only if `WITH` carries it. GQL made this
precise with the binding-table model. SQL binds by table alias and column; joins
are explicit or by `USING`. Datalog is the pure form: any shared variable in a
rule body is a join, there is one scope (the rule), and the safety condition
requires every head/negated variable to appear in a positive body atom. GraphQL
has no variables for data at all, only for arguments. ES/Vespa/LanceDB have
no binding variables; "joins" are structural (nested, parent) or absent.

Two GQ specifics deserve a note:

- *The type of an endpoint is inferred from the edge.* `$p worksAt $c` binds
  `$c: Company` without a declaration. That is concise and correct (the schema
  is closed), and it is also why wildcard and undirected traversals need both
  endpoints declared: the edge no longer determines the types. The rule is
  sound; the asymmetry ("sometimes I must declare, sometimes I needn't") is a
  learning cost the diagnostics (`T5`, `T6`) carry well.
- *Text-order visibility into blocks.* "Later outer declarations are not
  visible inside an earlier nested block" is a lexical rule in an otherwise
  order-free conjunction. It is the one place where clause order changes meaning,
  and the shared-expression RFC records the related trap (`T6` on a comparison
  written before the traversal that binds its variable) and the rejected fix (a
  separate `where`). Datalog and Cypher have no such rule; GQL does not either.

**Verdict.** Keep the Datalog-style conjunction. Consider making block
visibility order-free (resolve all top-level bindings of the `match` block
before checking its blocks), which removes the one order-dependent rule without
adding syntax.

### 2.4 Typing

**GQ.** Static, declared, closed. Parameters are declared with scalar, list or
vector types and explicit nullability; a missing non-nullable parameter is
refused before execution. Every expression retains a compiler type; every
numeric comparison conversion is an explicit `Cast` node; the planner never
chooses a domain from values. Result schemas are inferred and checked (every
GQT rows step carries `--- expect shape`). Integer `sum` accumulates exactly in
128 bits; mixed signed/`U64` comparison is exact; literal-to-property
comparison uses the property's type when exactly representable, else a common
type. Nullability propagates (`Bool?` from a nullable operand; `is null` is
always `Bool`). Blob is a type but not a value.

Comparators:

- *GraphQL* is the nearest peer: a declared, closed type system with
  nullability as a modifier (`String!`), variables typed in the operation
  header, and static validation of the document against the schema. Where GQ
  propagates nullability into results, GraphQL *bubbles* nulls (an error in a
  non-null field nulls its parent), which is the single most-criticized
  GraphQL semantic.
- *SQL* is statically typed but with implicit coercion rules that vary by
  dialect, `NUMERIC` versus float versus integer overflow behaviour that
  varies by engine, and no notion of a declared parameter list for an ad-hoc
  statement.
- *Cypher* is dynamically typed. A property may hold any type on any node;
  parameters are untyped placeholders; a property typo yields `null` rather
  than an error. Cypher 25 adds `IS :: INTEGER` type predicates precisely
  because the type is not known statically. *GQL* defines value types that
  carry `NOT NULL` and *closed* graph types under which static checking is
  possible, but the schema is optional (open graph types) and the one major
  implementation, Neo4j, declares open graph types only.
- *Datalog* varies: Soufflé is statically typed with declared relation
  schemas; Datomic/Datascript are dynamic (attributes have value types, but
  queries are not type-checked); Rel is typed. Cozo is dynamic.
- *Elasticsearch* types come from the mapping; the DSL coerces leniently
  (`"42"` to a long) and silently ignores unknown fields unless `strict`.
  ES|QL is typed at plan time and reports type errors.
- *Vespa* is schema-typed including tensor types; query-time parameters
  (`input.query(q) tensor<float>(x[384])`) are declared in the rank profile.
- *LanceDB* is Arrow-typed, but the filter is a SQL string parsed by
  DataFusion; the builder cannot type-check it before sending.

**Verdict.** Keep, and this is GQ's clearest advantage over the graph
incumbents. The declared-parameter list with nullability is the feature that
makes stored queries into typed tools (RFC 0003, MCP) and makes #1191
(codegen) straightforward. The one typing wart is that `limit` takes an
integer *literal* only, so page size cannot be a parameter (2.9).

### 2.5 Absence

**GQ.** Three-valued logic for comparisons and Boolean operators, SQL rules
exactly (`true or null = true`, `false and null = false`, `not null = null`);
a filter keeps rows whose expression is *true*; `is null` / `is not null` are
the only null tests and are always `Bool`; there is **no null literal**. A
nullable parameter bound to null makes every comparison on it null. `in []`
keeps no row.

The no-null-literal choice is unusual and mostly right. It removes the whole
class of `x = null` bugs that SQL and Cypher both suffer, and it makes the
three-valued rules teachable in a four-row table. Its one cost is spelled out
in the docs: clearing a nullable property (or Blob) requires a nullable
parameter bound to null, because `set { x: null }` cannot be written. That is
a mutation-side ergonomics tax paid for a read-side safety gain.

Comparators: SQL has `NULL`, 3VL, `IS [NOT] NULL`, `IS [NOT] DISTINCT FROM`,
`COALESCE`, `NULLIF`, and a long list of surprises (`NOT IN` with a null,
`NULL` in `GROUP BY` grouping together). Cypher has `null`, 3VL, `IS NULL`,
`coalesce()`, and the well-known pitfall that a missing property *is* null
(no "property absent" vs "property null" distinction). GQL inherits Cypher's
model and adds `IS [NOT] UNKNOWN`. GraphQL has `null` as a value and the
bubbling rule. Datalog has no nulls at all: absence of a fact is false under
the closed world, which is cleaner than 3VL but forces every "optional
attribute" into a separate relation or an explicit default (Datomic's pull
simply omits the key; `missing?` is a predicate). Elasticsearch: a missing
field matches nothing, `exists` tests presence, `must_not exists` is the null
test; `null_value` in the mapping can substitute. Vespa: a missing attribute
reads as the type's default (0, empty string), which is a documented wart.
LanceDB: Arrow nulls, DataFusion 3VL.

**Verdict.** Keep. GQ's model is SQL's 3VL with the footguns removed. Two
small additions would complete it without reintroducing them: a `coalesce`
(or default-on-null) expression once scalar functions arrive (2.14), and a
spelled way to assign null in a mutation that is not "declare a nullable
parameter and omit it", for instance `set { x: null }` *only* in assignment
position, where it is unambiguous.

### 2.6 Traversal

**GQ.** Directed typed edges; direction comes from the schema, so a triple is
written in schema orientation and reversed by swapping the ends. `<edge>` for
either direction (same-type endpoints only). `(a | b)` alternation and `*`
wildcard over compatible types, resolved against the captured catalog.
`{m,n}` bounds: finite, `1 <= m <= n`, meaning *shortest-distance* m..n from the
source (BFS with one visited set per source); omitted bounds mean exactly one
hop; `*` never means repetition. Unbound traversals have set semantics on
endpoint pairs; bound edges (`$e:knows`) yield one row per edge, single hop
only, with `$e.@id/@src/@dst/@type`. Self-loops count once. No path binding,
no shortest-path function, no unbounded recursion. Alternation/wildcard
statements carry a `traversal_work_limit` admission budget.

This is a *reachability* language, not a *path* language, and that is the
most consequential traversal decision:

- Cypher's variable-length pattern `(a)-[:KNOWS*2..3]->(b)` returns one row per
  *path* under relationship isomorphism (no relationship repeated in a path),
  so a diamond yields two rows for the same `b`. `shortestPath`/`allShortestPaths`
  and (in Cypher 25) quantified path patterns `((a)-[:KNOWS]->(b)){2,3}` with
  path modes exist because the default blows up combinatorially. Users must
  add `DISTINCT` to get GQ's default.
- GQL standardizes exactly this problem space: path modes `WALK`, `TRAIL`,
  `SIMPLE`, `ACYCLIC`, path search prefixes `ANY`, `ALL SHORTEST`, `ANY
  SHORTEST`, quantifiers `{m,n}`, `+`, `*`, and `KEEP` clauses. The
  standard's size here is the measure of how hard the path-semantics problem
  is when paths are first-class.
- SQL/PGQ (`GRAPH_TABLE … MATCH`) inherits GQL's path patterns, with
  `ANY SHORTEST` etc.
- Datalog: transitive closure is a recursive rule with *set* semantics (a
  derived fact is derived once), so Datalog's default is GQ's default; distance
  requires an explicit counter argument and stratification care; unbounded
  recursion is the norm and termination follows from finiteness.
- GraphQL: depth is fixed by the document; no recursion; no path.
- Elasticsearch/Vespa/LanceDB: no traversal in the language.

GQ's `{2,2}` therefore means "nodes at distance exactly 2", while Cypher's
`*2..2` means "paths of length 2". Both are defensible; they differ on every
graph with a cycle or a diamond. GQ's choice is the right one for its workload
(agents want the neighbourhood, not the path multiset) and it is what makes
the BFS bounded. The costs: no "how many distinct routes", no path
projection, no edge properties along a multi-hop (edge binding is single hop),
no `ALL SHORTEST`, and the `{2,2}` ≠ Cypher trap that LLMs trained on Cypher
will fall into. The direction-from-schema rule is elegant and hides direction
at the reading site; `(p)-[:WORKS_AT]->(c)` is self-documenting where
`$p worksAt $c` needs the schema, and the undirected form `<knows>` being
restricted to same-type edges is correct but surprising.

**Verdict.** Keep reachability semantics; document the Cypher difference in
the skill as prominently as the `mutation {}` trap. Two bounded additions would
cover most path demands without becoming a path language: (a) a `distance`
meta-field on the far end of a `{m,n}` traversal (`$b.@distance`), which the
BFS already knows; (b) multi-hop edge bindings as a *list* of edge objects once
collection results exist (2.15). Unbounded `*` recursion should stay refused:
`traversal_work_limit` is the honest form of the same promise.

### 2.7 Sub-pattern predicates

**GQ.** `not { }`, `exists { }`, `count { } op v`, `sum|min|max|avg(e) { } op v`.
Each block is matched once per outer row; each must read an outer-bound
variable (`T39`, the safety rule); the comparison's right side is a constant
(literal, `now()`, parameter); the aggregate's argument is scalar in the
block's scope. `not` is `count = 0`; `exists` is `count > 0`; empty `sum/min/
max/avg` satisfy nothing. Blocks narrow rows *before* `order`/`limit`, so
paged results filtered by relationship count are exact. The planner picks
`correlated` or `key_set` strategy; a single-hop filter-free `count` reads the
CSR degree.

This is stratified negation and aggregation sub-goals, the Datalog shape, with
Datalog's safety rule made explicit. SQL's equivalents are `NOT EXISTS`,
`EXISTS`, and correlated scalar subqueries (`(SELECT count(*) …) > 2`), which
are more general (the right side can be another column, the subquery can
return a value to project) and harder to plan (decorrelation is a research
area). Cypher 5 added `EXISTS { }`, `COUNT { }` and `COLLECT { }` subqueries
with the same block syntax GQ uses; Cypher's `COUNT { }` is a *value* usable
anywhere, which is what #1050 asks of GQ. GQL has `EXISTS { }` only; `COUNT { }` and
`COLLECT { }` are Cypher-only. GraphQL has nothing. Datalog: `not` (stratified), aggregates
in rule heads (`count`, `sum`, with grouping by the other head variables),
Soufflé's `count : { … }` as a value, Datomic's aggregates in `:find`. ES:
`must_not`, `exists`, `nested` with `inner_hits`; counting a relationship is a
`children` aggregation. Vespa: `!` negation, parent–child counts via grouping.

GQ's two restrictions, versus Cypher's `COUNT {}`:

1. *A block is a predicate, not a value.* So `return { count { $n knows $m } }`
   is refused; it costs one query per relationship group (#1050 measured 92
   requests for one node page). The operator (`SubqueryAggregate`) and the
   degree shortcut already exist; this is a grammar and result-schema change.
2. *The right side is a constant.* `count { $a knows $x } > $a.min_friends` is
   refused. Allowing an outer property on the right side costs nothing in the
   planner (the comparison is evaluated after the fold) and removes a
   round-trip.

**Verdict.** Keep the stratified-block design and the safety rule; they are
the correct foundation. Lift the two restrictions, both of which are expressed
in Cypher 5 and Datalog and both of which have their mechanism in the engine.

### 2.8 Aggregation

**GQ.** Aggregates `count`, `sum`, `avg`, `min`, `max` in `return`; grouping
is implicit on every non-aggregate return item (Cypher's rule); `count(distinct
$p)` / `count(distinct $p.x)`; `return distinct { }`; a Boolean expression
over aggregates under an alias (`count($p) > 0 as any`); an aggregate-only
return over zero rows yields one row. No `HAVING`: a group cannot be filtered
by its aggregate (the Boolean projection *labels* the group, it does not drop
it). No nested or two-stage aggregation. No `collect`/list aggregation. Exact
128-bit integer `sum` rounded once to `F64`; `avg` in `F64`; `min`/`max` keep
the column type including Bool and dates.

Implicit grouping is shared with Cypher, Datalog heads, Datomic, and
Vespa's grouping (which groups by the `group()` expression). GQL chose an
explicit `GROUP BY`, and Cypher 25 added one in 2026.07 beside the implicit
rule. SQL requires an explicit `GROUP BY` and offers `HAVING`, `ROLLUP`, `GROUPING SETS`, window
functions; its explicitness is verbose but makes "group by X, order by
aggregate, keep groups with aggregate > n" one statement. The implicit rule's
known pitfall (adding a column to `RETURN` silently changes the grouping) GQ
shares; its diagnostics mitigate it (`T58`, `T9`) but cannot remove it.

Numeric exactness is distinctive. SQL engines differ: PostgreSQL `sum(bigint)`
returns exact `numeric`; MySQL returns `decimal`; SQLite returns integer with
overflow error; DuckDB `HUGEINT`. Cypher's integers are 64-bit and `sum` does not
widen (`sum(null)` is `0`, where SQL's is `NULL`). GQ's choice (exact accumulate, one rounding, F64 result) is
coherent and documented to the bit, but it has a surprising edge: a sum of two
integers that is exactly representable in `I64` still comes back as `F64`, so
`sum` of an `I64` column is never an `I64`. SQL's answer (an exact wider
integer or decimal) is what a client with 64-bit integers would prefer.

**Verdict.** Keep implicit grouping (it is the graph-language convention) and
the exactness. Add post-group filtering, which is one `Filter` above
`Aggregate` in the plan and needs only grammar: either a `having { }` clause
or, more in GQ's style, allow a Boolean aggregate expression as a *filter*
rather than only as a projection. Reconsider returning an exact integer type
for integer `sum` (`I64` with overflow refusal, `exact_integer` internally),
since F64 is a lossy public type for a quantity the engine computed exactly.

### 2.9 Order and cut

**GQ.** `order { key [asc|desc], … }` with property keys, system fields,
return aliases, or expressions textually present in `return`; a leading search
function; nulls first ascending, last descending; a *total* order with entity-id
tiebreaks appended automatically (binding-name order), omitted only where rows
are indistinguishable. `limit N`, integer literal. No `offset`/`skip`, no
cursor, no parameterized limit. Without `order`, `limit` is any N rows.

The total-order guarantee is stronger than every comparator's default. SQL's
`ORDER BY` is a partial order unless the user adds a unique key; `LIMIT …
OFFSET` paging over a partial order skips and repeats rows, a widely documented
bug class. Cypher's `ORDER BY … SKIP … LIMIT` has the same property; Neo4j
documents that ties are returned in an unspecified order. GQL: `ORDER BY …
OFFSET … LIMIT`, same. GraphQL has no ordering in the core; Relay's connection
spec standardizes *cursor* paging (`first`/`after`, opaque cursors, `pageInfo`),
which is the right abstraction and which GQ's deterministic order would make
trivial to implement. Elasticsearch: `sort` plus `search_after` with a
point-in-time, explicitly because `from`/`size` breaks past 10,000; it adds
`_shard_doc` as the tiebreaker, which is GQ's id tiebreak by another name.
Vespa: `order by` then `offset`/`hits`, with a documented limit. LanceDB:
`limit`/`offset` on the builder.

GQ has the determinism and lacks the paging primitive. Keyset paging is
expressible by hand (`$p.name > $last or ($p.name = $last and $p.@id > $last_id)`)
now that `or` exists, but the client must know the tiebreak rule and the limit
cannot vary per call because it is a literal.

**Verdict.** Three small changes, in order of value: (1) `limit $n` with an
`I64` parameter, (2) an opaque continuation token in the read envelope,
derived from the total order the engine already computes (the engine's own
`commit list --page-token` is the precedent), (3) `offset` only if a client
needs random-access pages, since it is the weakest form.

### 2.10 Retrieval

**GQ.** Full-text predicates `search`, `fuzzy`, `match_text` (positive, in
`match`, index required at compile time `T27`), rankings `nearest` (L2 or text
via the configured embedder), `bm25`, and `rrf(a, b [, k])`, each written as
the *leading order key*. Filters in `match` are applied before ranking; `limit`
is required for `nearest`; projected scores must repeat the order key
(`T33`); `rrf` arms must rank one binding or unconnected bindings (`T59`); a
ranking may root a traversal at the ranked binding regardless of declaration
order; the result order is score, then the remaining keys, then ids. Under
aggregate or `distinct` returns, the leading search function *selects the
population and does not order*. `ann_nprobes`, an overfetch ladder and a
prefilter gate are declared plan policy, visible in `explain`, and the read
envelope reports retrievals and metric kinds with `recall: approximate|exact`.

Compared shapes:

- *pgvector in SQL*: `ORDER BY embedding <-> $q LIMIT k` is exactly GQ's
  shape, but with an HNSW or IVFFlat index the `WHERE` is applied *after* the
  index scan, so a filtered top-k can return fewer than `k` rows
  (`hnsw.iterative_scan` is the mitigation); hybrid is a hand-written join of
  two CTEs with `1/(60+rank)`; scores are ordinary expressions so weighting and
  thresholds are free.
- *Neo4j Cypher 25*: the procedures (`db.index.vector.queryNodes`,
  `db.index.fulltext.queryNodes`) are deprecated as of 2026.04 and 2026.10 in
  favour of a `SEARCH` subclause of `MATCH`: `SEARCH m IN (VECTOR INDEX idx FOR
  $q WHERE m.year > 2000 LIMIT 5) SCORE AS s`. Its inner `WHERE` prefilters
  inside the index on declared filter properties, AND-only; labels and the outer
  `WHERE` post-filter; the pattern is limited to one bound variable and at most
  one hop; there is no built-in fusion. GQL standardizes nothing here.
- *Elasticsearch*: scoring is the query tree (`bool` with `must`/`should` and
  `filter` for non-scoring), `knn` with `num_candidates` and filters, and the
  retriever tree (`rrf`, `linear`, `text_similarity_reranker`) composing
  retrievers of retrievers. Most expressive hybrid surface of the nine;
  scoring is opaque unless `explain: true`.
- *Vespa*: the cleanest separation. `where` decides the *match set*
  (`nearestNeighbor(embedding, q)` with `targetHits`, `weakAnd`, `userInput`),
  and the *rank profile* in the schema decides order with first/second/global
  phases over typed rank features (`bm25(title)`, `closeness(field,
  embedding)`, tensor expressions, ONNX). Hybrid is `rank(nearestNeighbor…,
  userInput…)` plus an expression. `order by` is separate again.
- *LanceDB*: `.search(vec).where("…", prefilter=True).limit(k)`, FTS via
  `query_type="fts"`, hybrid via reranker objects (RRF, linear, cross-encoder).
  Prefilter is an explicit flag because post-filter is the historical default.
- *Datalog/Cozo*: `~docs:idx{ … | query: $q, k: 10, bind_distance: d }` as a
  special relation inside a rule body; the result is just more atoms, so
  joining with graph patterns is natural, and ordering is a separate `:order`.

GQ's position: its filter-before-rank contract, exact total order, compile-time
index requirement, and plan-declared approximation policy are *correctness*
properties none of the search engines promise and the graph databases do not
have at all. Its weakness is *expressiveness*, and the weakness has one root:
retrieval is modelled as an order key, so the population-selecting role and the
sorting role are one syntactic object. Consequences:

- Fixed fusion: only `rrf`, only two arms, no weights, no linear combination,
  no reranker stage, no score arithmetic.
- No score threshold: `nearest(...) < 0.3` as a *filter* is not writable
  because `nearest` is not a value in `match`; "everything within radius"
  is inexpressible.
- The aggregate/`distinct` rule ("selects the population, does not order")
  is the overload made visible: the same token does two jobs and in one
  context does only one of them.
- Multi-field text (`bm25` over title *and* body) needs two arms; three
  fields are impossible.
- No phrase/proximity/boosting; `fuzzy` is the one analyzer control.
- Heterogeneous search across types (#648) has no home, because a ranking
  belongs to one binding's property.

Vespa's split is the model to learn from, adapted: a *retrieval* position that
yields typed score columns (`$d.@score`, `$d.@distance`) over a declared
candidate window, and a plain `order` over any expression including those
columns. RFC 0048 (retrieval algebra, PR #793) appears to be this; this
evaluation endorses its direction.

**Verdict.** Keep the contracts (prefilter, total order, `T27`, declared
policy, descriptors). Move retrieval out of `order` into its own position so
scores become values; then weights, thresholds, more arms and rerankers are
expressions, not grammar.

### 2.11 Writes

**GQ.** Inside a `query` declaration: `insert T { … }`, `update T set { … }
where expr`, `delete T where expr`. Several inserts/updates *or* several
deletes per query (D2 rule), never both; one query is one commit; a no-match
mutation publishes no commit. Assignment values are constants per invocation
(literals, parameters, `now()`, Boolean/comparison expressions over them);
keyed insert is upsert by derived id; unkeyed insert is strict; edges have no
`update`; `where` is the read expression language over the target type's own
properties and `@id/@src/@dst`; no binding variables (`T14`), no aggregates or
search calls (`T44`). `--if-commit` gives read-modify-write; `as commit` and
`as unattached branch` give proposals and private snapshots. Loads are `load
csv` statements or JSONL with `append|merge|overwrite`.

Comparators mix reads and writes freely: Cypher `MATCH (p)-[r:KNOWS]->(q)
WHERE … DELETE r`, `SET p.visits = p.visits + 1`, `MERGE` with `ON CREATE`/`ON
MATCH`, `FOREACH`; GQL `INSERT`, `SET`, `REMOVE`, `DELETE`, `DETACH DELETE`
chained after `MATCH`; SQL `UPDATE … FROM`, `INSERT … SELECT`, `MERGE`,
`DELETE … USING`, `RETURNING`. GraphQL mutations are opaque resolver calls
executed serially. Datomic transacts datoms plus transaction functions for
computed writes; Soufflé is batch-only; Cozo `:put`/`:rm` take a query's
output as the rows to write (query-to-write composition). Elasticsearch
`update_by_query` with Painless scripts; `_bulk`; optimistic concurrency via
`if_seq_no`/`if_primary_term`. Vespa document API with partial updates and
`condition` test-and-set. LanceDB `merge_insert` (`when_matched_update_all`,
`when_not_matched_insert_all`), `update(where, values)`, `delete(where)`.

GQ is the most restrictive write language of the nine, and the restriction is
load-bearing:

- A mutation's effect set is computable from its text and parameters before
  any read, so Cedar policy, `write_max_bytes`, the 8,192-entity bound and the
  removed-ID budget are all checkable up front.
- Constants per invocation make retry idempotent and make `as commit` a
  replayable proposal.
- No read in a write means no read-set to conflict on except the predicate's
  own rows; `--if-commit` is the explicit, whole-graph version of that.
- D2 keeps the staging protocol a two-case problem.

What it cannot do, and what agents do instead:

| Need | GQ today | Cost |
|---|---|---|
| Increment/append (`visits + 1`, `tags += "x"`) | read, compute client-side, update with `--if-commit` | 2 round trips, retry on conflict |
| Delete by pattern ("edges from `$a` to anyone retracted") | read ids, then `delete E where @id in $ids` | 2 round trips |
| Update an edge's property | reinsert keyed edge, or delete + insert unkeyed | fine for keyed; two mutations (two commits) for unkeyed, which D2 forbids in one |
| Write what a query computed (materialize) | export + load | out of the language |
| Clear a property to null | nullable param, omitted | awkward but sound |

**Verdict.** Keep statement-shaped writes, constants per invocation, upsert by
key and D2 for now. The RFC's own note ("the mutation `where` becoming the same
expression type as a read filter is the step a later RFC needs" toward
`MATCH … DELETE`) names the right next step: a *pattern-scoped* mutation,
`delete $e where { $a: Person { slug: $s }  $a $e:knows $b  $b.retracted }`,
that keeps the write's effect set visible (one target type) while letting the
pattern select it. Computed assignment (`set { visits: visits + 1 }`) waits on
arithmetic (2.14) and needs a read-set rule; it should be designed with
`--if-commit` as the default, not an exception. Edge `update` for keyed edges
is a gap with no principled reason.

### 2.12 Time and branches

**GQ.** Branch create/delete/merge/list/diff/merge-base as statements; merge
`dry run`; `as commit` (unpublished, publish/drop later); `as unattached
branch` (private, never publishes, chainable `on`); historical reads by
`--snapshot <commit>`; `--if-commit` preconditions; change feeds (`changes
poll`), commit list/show paging. Branch *selection* for a read or write is
outside the text (`--branch`, request field), branch *control* is inside.

No comparator has this in the language. SQL has none (Dolt adds `dolt_*`
procedures and `AS OF`; Postgres has transaction snapshots, not versions).
Cypher/GQL: none (GQL has session/transaction statements; Neo4j has no
time travel). GraphQL: none. Datomic is the closest relative: a database is a
value, `as-of`, `since`, `history` produce filtered database values, and the
query runs against a value, so "which version" is a *parameter of the
query*, not text in it, which is GQ's `--snapshot`. Datomic has no branches.
Cozo has validity-time triples. LanceDB has dataset versions, `checkout`,
`restore`, tags and branches, all at the *table* level; its docs say it
"does not reconcile one branch's history with another or detect conflicts",
and merge is Enterprise-only and limited to added columns; there is no
cross-table coordination and no language. Elasticsearch and
Vespa have none.

**Verdict.** Keep; this is a genuine differentiator and the statement form was
the right call (one transport, testable in GQT). One asymmetry to resolve
eventually: `--snapshot`/`--branch` as request fields versus `branch …` as
text means a stored `.gq` file cannot say which branch it reads, while it can
say which branch it merges. A `read on <branch>`/`at commit "<id>"` clause on
a declaration would close that, and Datomic's precedent suggests keeping it a
*parameter* (`$branch: Branch`) rather than a literal, so stored queries stay
branch-agnostic.

### 2.13 Reuse

**GQ.** The unit of reuse is the named, parameterized query, stored in the
cluster's registry and invoked by name, exposed as MCP tools with
`@description`/`@instruction`, and bound to CLI aliases. There is no view, no
rule, no fragment, no user function, and no way for a query to use another
query.

SQL: views, materialized views, CTEs, functions, procedures. Cypher: none
built in (Neo4j has user procedures in Java and APOC). GQL: procedures and
`CALL`. GraphQL: fragments (reuse of *selection shape*), schema stitching.
Datalog: rules are the whole language, and a rule is a reusable derived
relation; this is Datalog's defining strength, and Rel/Logica/Soufflé all build
large programs from them. Vespa: rank profiles inherit, query profiles store
parameters, fieldsets group fields. Elasticsearch: search templates
(Mustache), stored scripts, index aliases. LanceDB: none.

For GQ's workload the gap matters more than for a general database. The
schema-design guide's principle 7 ("store what was observed or decided;
compute the rest") and principle 9 (`GraphPolicy` with `check_query`) both
depend on *shared derived relations*: "a former customer", "a contested
claim", "an orphan assertion". Today each is a stored query that returns
rows; no other query can *bind* to it. An agent that wants "contested claims
about customer X" rewrites the contested-ness pattern inline, and two agents
write it two ways, which is exactly the convergence failure the guide warns
about.

**Verdict.** This is the highest-leverage missing feature after optional
match. A *typed view*: a stored read query whose result columns are declared
like a node type, usable as a binding (`$c: ContestedClaim` where
`ContestedClaim` is `view ContestedClaim($since: Date) = query …`). It is the
Datalog rule in GQ clothing, it is non-recursive (so stratification is
trivial), and it reuses the one closure step recommended in 2.2: a view is
inlined at compile time, so the planner still sees one component shape.

### 2.14 Expressions

**GQ.** Leaves: property access, system fields, literals (`"s"`, `1`, `1.5`,
`true`, `date("…")`, `datetime("…")`, `[…]`), parameters, `now()`. Operators:
six comparisons, `contains` (substring or list membership), `starts_with`,
`in`, `is [not] null`, `and`, `or`, `not`. Calls: the five aggregates and the
six search/ranking functions. That is the entire expression language. No
arithmetic, no string functions (`lower`, `trim`, `length`, concat), no date
arithmetic or extraction (`year(d)`, `d + 7d`), no `case`/`if`, no `coalesce`,
no list construction or indexing, no map/struct literals, no type conversion.

Every comparator has all of these. SQL has the largest library; Cypher/GQL
have arithmetic, string, list, map, temporal and `CASE`; Datomic allows any
Clojure function in a predicate; Soufflé has arithmetic and string functors;
ES has Painless scripts and `runtime_fields`; ES|QL has `EVAL` with a function
library; Vespa has rank expressions and `userInput` grammars; LanceDB's filter
string is DataFusion SQL with its function library.

The shared-expression RFC chose this scope deliberately and recorded the rule
the next step needs: a `can_fail` property per node so pushdown keeps a
fallible conjunct behind its guards. Three consequences of the gap for agents:
every derived value is computed client-side (`age` from `birth_date`,
`amount * rate`), so it cannot be filtered or ordered on server-side; string
normalization for keys (`lower(email)`) is impossible, which the schema-design
guide's convergence goal would want at write time; and `@check(p, "regex")` is
the only regular-expression surface, and it is write-time only.

**Verdict.** Add the smallest total library first: arithmetic (`+ - * /` with
typed overflow refusal and explicit division semantics), `lower`/`upper`/
`trim`/`length`, date extraction and `now() - duration`, `coalesce`, and a
`case` expression. Each is total or has one documented failure (division by
zero), so the `can_fail` rule stays simple. Resist a large library; resist
user-defined functions until views (2.13) exist, since a view covers most
"function" requests without a new execution surface.

### 2.15 Result shape

**GQ.** Flat rows of Arrow-typed columns, serialized with `arrow-json`
conventions (null keys omitted, dates as strings, `F32` at 32-bit width,
integers bare). A bare `$p` projects one struct column with `@id` and every
non-Blob, non-Vector property. Edge bindings cannot be projected bare;
blocks cannot be projected; no lists of nodes, no nested objects, no paths.
The envelope carries `columns`, `rows`, `graph_commit_id`, `retrievals`,
`metrics`, `usage`, and `profile` on inspected runs.

GraphQL is the opposite pole: the result is a tree whose shape *is* the query,
which is why clients love it (one request, exactly the needed fields) and why
servers suffer (N+1, no server-side joins). Cypher returns rows whose cells may
be nodes, relationships, paths, lists and maps, so `RETURN p, collect(f) AS
friends` gives one row per `p` with a nested list. GQL likewise. SQL returns
flat rows; nesting is `ARRAY_AGG`/`JSON_AGG`. Datomic's *pull* API nests by
pattern (`[:person/name {:person/friends [:person/name]}]`), and `:find` with
pull returns nested maps in a flat tuple. Elasticsearch returns hits with
`_source` (arbitrary JSON) plus `inner_hits` for nested matches and an
aggregation tree. Vespa returns hits with summary fields, including structs and
arrays, and grouping results as a tree. LanceDB returns Arrow, so nested
columns are natural.

Flat rows are the right *canonical* answer for a columnar engine and the right
thing for GQT to compare; the question is whether one nested level is worth
having. The evidence that it is: #649 (entity plus neighbourhood), #1050
(entity plus counts), the N+1 pattern in the schema-design guide's "node page"
discussion, and the fact that `return { $p }` already projects a struct, so
`list<struct>` (Arrow-native) is one step away.

**Verdict.** Keep flat rows as the model and add exactly one aggregation that
produces a collection: `collect($f)`/`collect($f.name)` as `[T]` or
`[struct]` columns under the existing implicit-grouping rule. With a projected
`count { }` and an optional block, that covers the entity-page shape in one
query, with no tree result model and no change to the row envelope.

### 2.16 Self-description

(Requirement added 2026-10-29: the schema must stay queryable as it grows,
through the same language as the data. The catalogue types are that answer;
the GQ 3 RFC reserves catalogue edges between them and makes a catalogue type
a node type for every purpose of the grammar, including a `retrieve` arm over
its `@instruction` text.)

**GQ.** `explain` as rows (logical, physical, DataFusion trees, passes,
assumptions, validation scope); `profile` rows for inspected runs; the engine
catalogue as `@` node types joinable with user types; `omnigraph schema show`
for the schema; `lint` with an `operation` descriptor (`result`, `reads`,
`writes`) per query; `show all` for settings; read envelopes with retrieval
and metric descriptors.

SQL: `EXPLAIN [ANALYZE]`, `information_schema`, `pg_catalog`, all queryable
tables. Cypher: `EXPLAIN`/`PROFILE`, `SHOW INDEXES|CONSTRAINTS|PROCEDURES`,
`db.schema.visualization()`. GQL: `CALL`-based catalog access, implementation
defined. GraphQL: `__schema`/`__type` introspection, the best-in-class example
of a schema fully queryable in the language itself, on which every GraphQL
IDE, codegen and client depends. Datomic: the schema *is* data (attributes are
entities with `:db/valueType`, `:db/cardinality`), so `[:find ?a :where [?a
:db/valueType]]` lists it, the purest form. Cozo: `::relations`, `::columns`,
`::explain`. ES: `_mapping`, `_explain`, `_validate/query`, profile API.
Vespa: `trace.level`, no schema query. LanceDB: `schema`, `explain_plan()`,
`analyze_plan()`.

GQ's `explain` and catalogue are strong; the schema is the omission. There is
no `match { $t: @NodeType }` or `$p: @Property` with `type`, `nullable`,
`key`, `indexed`, `description`, `instruction`, nor `@EdgeType` with `src`,
`dst`, `card`. The catalogue RFC built the mechanism (registry-derived
read-only `@` types); the schema is the registry that agents need most, and
`@instruction` text is already written *for* agents.

**Verdict.** Add `@NodeType`, `@EdgeType`, `@Property` (and `@Constraint`)
catalogue types from the accepted schema, snapshot-pinned like everything else.
It gives agents schema discovery in one query, lets a `GraphPolicy` check
query join schema facts to data, and answers #1058's "which properties does
this commit hold" as a query.

### 2.17 Refusal contract

**GQ.** Every refusal: a stable code (`Q…` parser, `T…` typechecker, `P…`
planner; meaning frozen, text may improve, numbers never reused), a position
(line, column, byte) or stage and expression, an `expected` line, one `fix`,
and optionally `suggestion` edits with `machine_applicable`/`needs_review`
applicability. A query-shape refusal is 400, never 500; an index prerequisite
is 409 with a typed detail. The grammar is word-bounded (`nothing`, `android`
are identifiers), reserved words are six and named in their error, keywords are
lowercase, every block is braces. The measured LLM run: 45/45 correct, 8/45
first attempts refused, all for one shape, now `Q002` with a machine-applicable
edit.

SQL errors: SQLSTATE codes exist but position and fix quality vary by engine;
PostgreSQL gives a caret position and `HINT`. Cypher: `Neo.ClientError.
Statement.SyntaxError` with line/column and a caret, GQLSTATUS codes in Cypher
25, generally good. GQL defines GQLSTATUS. GraphQL: validation errors with
`locations` and `path`, well specified, and the schema makes most errors
static. Datomic: exceptions with EDN data, variable quality. Soufflé: compiler
errors with positions. ES: verbose JSON `reason` chains, no stable codes.
Vespa: often opaque (`Could not create query from YQL`). LanceDB: DataFusion
parse errors as Python exceptions.

For machine authorship, two things matter beyond errors: *regularity* (one
way to write each thing) and *familiarity* (the author's prior). GQ wins on
regularity and loses on familiarity: `$e.@type`, lowercase-first edge names
derived from PascalCase declarations, `{m,n}` as distance, `count { } > 2`,
`nearest` in `order`, no `null` literal, no `mutation` wrapper, and the
direction-from-schema rule are each one more thing a model trained on Cypher
and SQL must be told. The skill carries them; the diagnostics repair them; but
the novelty tax is real and compounds with every new spelling.

**Verdict.** Keep, and treat the four-field diagnostic as a language feature
with the same compatibility discipline as the grammar. Spend novelty
deliberately: where a GQ spelling differs from Cypher/SQL *without* a semantic
reason, prefer the familiar one (for instance `!=` and `is null` already do;
`offset` and `collect` should, if added). Where it differs *for* a reason
(`{m,n}` distance, no `null`), say the reason in the error text, as the
`Q002` fix does.

---

## 3. Head to head

Each section: what GQ shares with the language, where GQ is stronger, where
it is weaker, and what is worth borrowing. Facts about the comparators were
checked against their primary documentation on 2026-10-28; the few that could
not be are marked *recalled*.

### 3.1 SQL (SQL:2023, SQL/PGQ, pgvector and friends)

**Shared.** Static typing over a declared schema; three-valued logic with the
same truth tables (GQ took SQL's `AND`/`OR`/`NOT` rules and dropped the `NULL`
literal); `ORDER BY … LIMIT` as the cut, and pgvector's `ORDER BY embedding <->
$q LIMIT k` as the exact shape of GQ's `order { nearest(…) } limit k`; exact
widening of integer sums (PostgreSQL `sum(bigint) → numeric`, GQ's 128-bit
accumulator); `EXPLAIN` as rows and `information_schema`/`pg_catalog` as
queryable catalogs (GQ's `explain` rows and `@` types); stable five-character
SQLSTATE classes (GQ's `Q`/`T`/`P` codes); typed prepared-statement parameters
(`PREPARE p (int, text) AS …`; GQ's declared `$params`); one statement, one
snapshot, one commit for a data-modifying CTE (GQ's one-commit mutation).

**GQ stronger.** For a graph workload, SQL/PGQ is a *view* (`CREATE PROPERTY
GRAPH … VERTEX TABLES … EDGE TABLES`) over tables, and `GRAPH_TABLE … COLUMNS`
must flatten every match to scalars; its adoption is fragile (Oracle spells
labels `IS`, DuckDB `:`; PostgreSQL merged `GRAPH_TABLE` for PG19 in March 2026
and reverted it in September). Every join is spelled; clause order is not
evaluation order; `NOT IN` with a null returns nothing, `sum()` of nothing is
`NULL`, and nulls group together but do not compare equal, the warts Date and
Darwen called "inherently flawed" and which GQ's subset avoids. `LIMIT …
OFFSET` over a partial order skips and repeats rows unless the author adds a
unique key; GQ's order is total. pgvector and DuckDB VSS apply `WHERE` *after*
the approximate index scan, so a filtered top-k can return fewer than `k`
rows (`hnsw.iterative_scan` and ACORN are the mitigations); GQ's
filter-before-rank contract and overfetch ladder make the filled `limit` a
promise. SQL has system-versioned tables (`FOR SYSTEM_TIME AS OF`) and no
branches; parameter nullability is not declarable.

**SQL stronger.** Closure and composition: subqueries, derived tables, CTEs
(recursive, with `SEARCH`/`CYCLE`), views, functions, `UNION`/`EXCEPT`,
`LATERAL`, `UNNEST`, `JSON_TABLE`. Aggregation: explicit `GROUP BY`,
`HAVING`, `FILTER (WHERE …)`, ordered-set aggregates, and window functions,
which answer every "top-N per group" and running-total request in one
statement. The whole expression library. Writes: `MERGE` with `WHEN MATCHED
… BY SOURCE`, `INSERT … SELECT`, `UPDATE … FROM`, `RETURNING`. Fifty years of
tooling and training data.

**Borrow.** (1) `HAVING` by any spelling. (2) `RETURNING`: a mutation that
answers the ids it touched saves the read that follows it. (3) A parameterised
`LIMIT` and `OFFSET`. (4) The exact-integer result type for integer `sum`.
(5) Views. (6) SQL:2023's `UNIQUE NULLS [NOT] DISTINCT` is a reminder to state
in the `.pg` guide how `@unique` treats a nullable member: the validator drops
a null key from the unique image (nulls never collide), and the guide does
not say so.

### 3.2 ISO GQL (39075:2024)

**Shared.** The pattern vocabulary: GQ's `$a knows{1,3} $b` and GPML's
`(a)-[:Knows]->{1,3}(b)` are the same quantifier spelling; `EXISTS { }`;
`OPTIONAL MATCH` in GQL and the requested `optional { }` in GQ (#658); value
types that carry `NOT NULL` (GQ's `T?` inverted); closed graph types (GQ is
always closed); `INSERT` rather than `CREATE`; in-language control statements
(GQL: `SESSION SET`, `START TRANSACTION`, `COMMIT`; GQ: `branch`, `commit`,
`set`); standardised GQLSTATUS codes (GQ's frozen codes). Both are young
languages written after Cypher with a chance to fix it.

**GQ stronger.** GQL standardises no full-text, vector or ranking, no
constraints beyond graph types, no indexes, no `EXPLAIN`, no views, and no
`MERGE`/upsert (Neo4j: "MERGE, FOREACH, and LOAD CSV" are "not in GQL"). GQL v1
has no `STARTS WITH`/`CONTAINS`/`IN` (Neo4j lists them as Cypher-only); GQ has
`starts_with`, `contains` and `in`. GQL's path machinery, modes `WALK`/`TRAIL`/
`SIMPLE`/`ACYCLIC` times selectors `ANY`/`ALL SHORTEST`/`SHORTEST k GROUPS` times
match modes `REPEATABLE ELEMENTS`/`DIFFERENT EDGES`, is what path-as-value
costs; implementations already diverge on the defaults (Ultipa `TRAIL`, Spanner
`REPEATABLE ELEMENTS`, Neo4j `DIFFERENT RELATIONSHIPS`) and on `YIELD`, so "GQL"
is a family at launch. GQ's reachability semantics need none of it. The
standard runs to 610 pages; GQ's grammar is 158 pest rules with an executable
corpus of 425 cases.

**GQL stronger.** Linear composition: a statement produces a working table
and `NEXT` hands it to the next (`MATCH … RETURN a, count(p) AS n GROUP BY a
NEXT FILTER n >= 3 RETURN …`), which is `HAVING`, top-N per group and
multi-stage aggregation in one mechanism. `LET` (non-blocking) and `FILTER`
(standalone) are cleaner than Cypher's `WITH`. Explicit `GROUP BY` removes the
implicit-grouping trap. `OPTIONAL MATCH`. Paths, records and lists as
first-class result values. `IS [NOT] UNKNOWN`. An open ANTLR grammar.

**Borrow.** (1) `OPTIONAL MATCH` semantics as the specification for #658:
an outer join whose inline filters belong to the pattern, nullable bindings
in the result schema. (2) `FILTER` after a grouped `return` as the HAVING
form, which fits GQ's one pipeline without `NEXT`. (3) A documented warning
that GQ's `{m,n}` means *distance* where GPML's identical spelling means
*path length*; the shared spelling with different meaning is the single most
likely cross-language error.

### 3.3 Cypher (openCypher, Neo4j Cypher 25)

**Shared.** Implicit grouping on non-aggregated return items; `EXISTS { }` and
`COUNT { }` block syntax (GQ's blocks are Cypher 5's with the braces and
without the keyword in the `not` case); `IS NULL`; three-valued logic with
"anything that is not `true` is false" in a filter; `STARTS WITH`/`CONTAINS`
case-sensitive (GQ `starts_with`/`contains`); `EXPLAIN`/`PROFILE`; stable
GQLSTATUS codes; and, since 2026.01, Neo4j's `MATCH (m:Movie) SEARCH m IN
(VECTOR INDEX idx FOR $q WHERE m.year > 2000 LIMIT 5) SCORE AS s`, a
declarative, in-index-prefiltered ranking inside `MATCH`, which is Cypher
moving toward GQ's shape and deprecating its procedures (`db.index.vector.
queryNodes` 2026.04, full-text 2026.10).

**GQ stronger.** Typing: Cypher parameters are untyped, properties are
untyped per node, and a property typo returns `null` instead of an error,
which Neo4j's own documentation names as the recurring LLM failure; GQ refuses
with `T2`. Schema: Cypher constraints are unique/exists/type/key only, no
range or check; GQ has `@range`, `@check`, `@card`, enums. Order: Cypher's
`ORDER BY` is partial; GQ's is total. Traversal: `*1..3` is paths with
`03N91` warnings for unbounded ones; GQ's `{m,n}` is distance with a work
budget. Retrieval: `SEARCH` prefilters only on declared filter properties,
AND-only, one binding, at most one hop, everything else post-filters, and
hybrid is hand-built RRF over `CALL { } UNION ALL`; GQ prefilters on the whole
pattern and has `rrf`. `OPTIONAL MATCH` chains fan out rows ("hey LLM, you're
using OPTIONAL MATCH wrong" is a Neo4j blog title); `MERGE` on a whole pattern
silently duplicates sub-parts. Two live language versions with monthly
deprecations; GQ's codes are frozen and its corpus executable. No branches,
no time travel.

**Cypher stronger.** Composition: `WITH`, `LET`, `FILTER`, `NEXT`, `CALL
(scope) { }`, `UNION`, `UNWIND`/`FOR`, list/map/pattern comprehensions,
`reduce`. `OPTIONAL MATCH`. `COUNT { }` and `COLLECT { }` as *values* usable
anywhere. Results as nodes, relationships, paths, lists, maps with map
projection `n { .id, .name }`. The expression library (`CASE`, temporal, list,
string, `coalesce`). Writes: `MATCH … SET/DELETE`, `MERGE ON CREATE/ON MATCH`,
`FOREACH`, `WHEN … THEN { } ELSE { }` conditional queries, `CALL (n) { … } IN
TRANSACTIONS OF 2 ROWS ON ERROR CONTINUE` batching. Quantified path patterns
with group variables and in-traversal `WHERE`; `SHORTEST k`. Vector index DDL
with quantization. A decade of training data.

**Borrow.** (1) `COUNT { }`/`COLLECT { }` as values. (2) `optional { }` with
Cypher's rule that the block's own filters are part of the pattern. (3) Map
projection as a partial node object: `$p { name, age }` between bare `$p` (all
properties) and one property at a time. (4) Planner *notifications* beside
errors: Neo4j's `03N90` (cartesian product) and `03N91` (unbounded pattern)
are the warning channel GQ lacks; GQ plans a `CrossJoin` silently and scans a
whole ranked type for a destination-ranked traversal (#758) with no word to the
author. (5) `CALL IN TRANSACTIONS`' idea, not its syntax: a batched mutation
form for the 8,192-entity bound.

### 3.4 GraphQL

**Shared.** Schema-first, closed type system; declared, typed variables with
explicit nullability (`$episode: Episode`, `String!` versus GQ's `T?`); static
validation of the document against the schema before execution; named
operations as the unit of exposure; metadata on types for clients
(`@deprecated`, descriptions; GQ's `@description`, `@instruction`); one
spelling for a reserved namespace (`__typename`, `__schema`; GQ's `@id`,
`@Operator`). Of the nine, GraphQL is the one GQ most resembles in *posture*:
both are contracts written for machine consumers first.

**GQ stronger.** GraphQL has no predicate language, no ordering, no
aggregation and no joins; every filter is a schema author's convention
(Hasura's `where: { name: { _eq: … } }` is one of dozens), so a client must
learn each API. GQ has one expression language, one grouping rule, one order
contract. GraphQL's nulls *bubble*: an error in a non-null field nulls its
parent up to the nearest nullable ancestor, which is why most schemas make
fields nullable "only to limit how far error propagation spreads" (GAP-49,
semantic nullability); GQ's nullability is a type fact, not an error-containment
device. GraphQL's N+1 is structural (every nested field is a resolver call) and
fixed by an ecosystem tool (DataLoader); GQ's traversal is a planned join.
GraphQL has no time travel and takes "a strong opinion on avoiding versioning";
GQ has branches, snapshots and commits in the language.

**GraphQL stronger.** Result shape. The selection set *is* the response: one
request returns the entity, its friends' names and their companies as a tree,
in requested order (§7.2.2). GQ returns flat rows and needs N queries or a
client-side join for the same page (#649, #1050). Introspection: `__schema`
and `__type` are queryable in the language, and the whole tooling ecosystem
(codegen, IDE validation, Relay compiler) stands on it; GQ's schema is
`schema show` output, not a queryable relation (2.16). Fragments reuse
selection *shape* across operations; GQ has no reuse below a whole query.
Pagination: Relay's connection spec (`first`/`after`, opaque cursors,
`pageInfo`) is a standard clients implement once; GQ has `limit N` and no
cursor (2.9).

**Borrow.** (1) Introspection as query: `@NodeType`/`@EdgeType`/`@Property`.
(2) One nested level in results (`collect`), not a tree. (3) Relay-style
opaque continuation tokens derived from the total order. (4) The discipline
that every result field's nullability is a declared type fact is already GQ's;
keep GraphQL's *explicit non-null marker* idea in mind if `T?` ever becomes the
default rather than the exception.

### 3.5 Datalog (classical, Soufflé, Datomic, CozoDB, Rel)

**Shared.** More than any other comparator, GQ's `match` block *is* a Datalog
rule body: typed unary atoms (`$p: Person`), binary atoms (`$a knows $b`),
built-in predicates (filters), stratified negation (`not { }`), aggregate
sub-goals with a safety rule (`T39` is range restriction), join by variable
repetition, set semantics on unbound traversals, and a head (`return`) whose
non-aggregated variables are the implicit group keys, which is exactly Cozo's
and Logica's rule-head aggregation. Datomic's `(not-join [?a] …)` requiring
the correlation variable is GQ's `T39`; Cozo's "negated atoms cannot introduce
bindings" is GQ's block rule; Soufflé's rejection of `A(x) :- !B(x). B(x) :-
!A(x).` is the stratification GQ gets for free by having no recursion.

**GQ stronger.** Typing at the query surface: Datomic's `:in` is untyped and
attributes are checked against data, not a declared schema; Cozo's columns are
typed but queries are dynamically checked; only Soufflé and Rel type-check
statically as GQ does. Ordering: Datomic has no `ORDER BY` at all ("handle
this in your application code"); Soufflé none; Cozo only on the entry rule.
Null semantics: Datomic has absence-only (clean but forces every optional
attribute into `missing?`/`get-else` dances); Cozo admits a totally ordered
`Null`; GQ's SQL-style 3VL with no literal is the most teachable of the three.
Bounded cost: Datalog recursion is unbounded by design and the engines warn
about it (Cozo "large recursive rules can be expensive", infinite relations
the compiler may accept; Rel non-monotone recursion "may oscillate forever");
GQ's finite `{m,n}` and `traversal_work_limit` make cost a contract. Search:
Cozo's `~idx{ … | query: $q, k: 10 }` is a body atom, elegant, but ranking is
then a plain `:order` with no total-order or prefilter contract; GQ's
retrieval contracts are stronger.

**Datalog stronger.** Reuse and recursion. A rule head is a relation every
other rule can use; a program is many rules; recursion is the normal case.
GQ has neither derived relations nor recursion, so "contested claim" cannot be
defined once and bound to (2.13), and transitive closure needs an explicit
finite bound. Schema as data: Datomic attributes *are* entities, so the
schema is queried with the same language as the data; GQ cannot (2.16).
Time: Datomic's `as-of`/`since`/`history` make the database version a *value*
passed to the query, and `history` exposes retractions as rows; GQ's
`--snapshot` is the same idea for a point, and the change feed is the history
read, but neither is composable inside a query. Cozo's `Validity` as a key
column is bitemporal modelling in the data model; GQ has transaction time
(commits) and leaves valid time to the user's schema, which the schema-design
guide recommends. Aggregates as values: Soufflé's `c = count : { … }` and
Cozo's head aggregates make a count a thing to project or compare to another
variable; GQ's blocks are predicates only (2.7).

**Borrow.** (1) Non-recursive derived relations as typed views, the one
Datalog feature GQ's planner can absorb by inlining. (2) Aggregate blocks as
values with a variable, not only a constant, on the comparison's right side.
(3) Datomic's "database as a value": a `$branch`/`$at` parameter on a stored
query. (4) Cozo's `::describe` (human- and agent-facing documentation stored
beside a relation) is already GQ's `@instruction`; make it queryable.
(5) Do *not* borrow unbounded recursion; borrow Datalog's *set* semantics
confidence and say so in the docs when explaining `{m,n}`.

### 3.6 Elasticsearch (Query DSL, ES|QL)

**Shared.** An index is required for full text (a `text` mapping; GQ's
`@index`, refused otherwise by `T27`); a `filter` inside the `knn` clause is a
pre-filter applied during the search so that `num_candidates` survive (GQ's
contract); `_shard_doc` as the implicit tiebreak under `search_after` (GQ's id
tiebreak); `rrf` with `rank_constant` 60 and a `rank_window_size` (GQ's `k`
60 and the nearest window); `_score` as a named column; `_validate/query`
(GQ's `lint`); `explain`/`profile`; ES|QL's typed `columns` and named
`?params` (GQ's `columns` and `$params`); `matched_queries` naming which
clause matched (GQ's `retrievals` descriptors).

**GQ stronger.** One language: Elasticsearch has four (Query DSL, ES|QL, EQL,
KQL) with different type systems; ES|QL remaps `float` to `double`, treats
`text` as `keyword`, returns `null` for a function over a multivalued field,
and caps output at 10,000 rows whatever `LIMIT` says. Semantics by position: a
clause scores under `must` and not under `filter`; `minimum_should_match`
defaults to 1 or 0 depending on its siblings; the *same* filter is a pre-filter
inside `knn` and a lossy post-filter one level up. GQ's positions are typed by
context rules and refuse rather than reinterpret. Hard ceilings (`index.
max_result_window` 10k, 1000 fields, 65,536 terms) versus GQ's declared and
settable budgets. Schema inferred on first sight and then frozen (`dynamic:
strict` opt-in) versus declared. Relationships are physical choices (nested
hidden documents, same-shard `join`, a lookup index with `index.mode: lookup`)
each with "a significant tax"; GQ traverses typed edges. Near-real-time
visibility (1s refresh, `refresh=wait_for`) and per-document CAS versus one
atomic commit with read-your-commit. `lenient` and `ignore_unmapped` swallow
type errors silently; `cardinality` is HyperLogLog and `track_total_hits` stops
at 10,000; GQ counts exactly. Search templates are Mustache text substitution;
GQ's stored queries are typed. No versioning.

**Elasticsearch stronger.** Retrieval expressiveness, by a wide margin:
`bool` with `should`/`minimum_should_match`/boosts, `function_score` decays,
phrase, proximity, fuzzy, wildcard, regexp, `multi_match`, highlighting,
`collapse`, `semantic_text`, and the retriever tree (`rrf` with per-arm
`weight`, `linear` with `minmax`/`l2_norm` normalizers, `rescorer`,
`text_similarity_reranker`, `diversify`, `pinned`). Aggregations: bucket,
metric and pipeline families nesting without limit, `composite` paging,
`bucket_selector` as `HAVING`. Deep paging with `search_after` over a
point-in-time. Runtime fields. ES|QL's `STATS … BY`, `EVAL`, `LOOKUP JOIN`,
`INLINE STATS`, and its hybrid pipeline `FORK … | FUSE RRF|LINEAR | RERANK`.
EQL's `sequence by … with maxspan` for temporal event patterns, an axis no
graph language here has. `_explain` says *why one document* matched.

**Borrow.** (1) The retriever tree as the model for retrieval composition:
fusion is a node over retrievals, arms carry weights, normalizers are explicit.
(2) `similarity`/`min_score` floors: a score threshold predicate. (3)
`search_after` over a PIT is exactly a continuation token over a pinned
snapshot, which GQ already has for free. (4) Per-row contribution: GQ's
descriptors say what a score column *means*; Elasticsearch's `explain: true`
on `rrf` says what each arm contributed to *this row*, which an agent choosing
between results can use. (5) Snippets (`TOP_SNIPPETS`, highlighting): for
passages, the matched span is the answer, not the row.

### 3.7 LanceDB and Lance

**Shared.** The substrate: GQ runs on Lance, so LanceDB is the comparison
with GQ's own storage spoken directly. Both name `_distance` and `_score`,
prefilter by default, label quantised distances approximate until refined
(GQ's `recall: approximate`), serve unindexed rows by a flat fallback while
indexes lag (GQ's "indexes are derived"), version every write (Lance versions;
GQ graph commits), expose `explain_plan`/`analyze_plan` (GQ `explain`/
`profile`), return Arrow, upsert by key (`merge_insert`; GQ keyed `insert`),
and attach the embedding model to the schema so a text query is embedded
automatically (LanceDB's registry in table metadata; GQ's `@embed` and text
`nearest`).

**GQ stronger.** A language. LanceDB's query is a builder chain whose filter
is "supplied as an SQL query string": no bound parameters (injection is the
caller's problem), no validation before execution, no stored or named queries,
no `ORDER BY`, no aggregation, no join in the builder (those are DataFusion
SQL or the client). No relationships. Versions are *per table*; LanceDB's
branches "do not reconcile one branch's history with another or detect
conflicts", and merge is Enterprise-only and "promotes only added columns",
whereas GQ's branch is a whole-graph lineage with three-way merge, merge-base,
diff and dry run in the language. No uniqueness or key constraints
(`merge_insert` "relies on the caller keeping keys unique"); GQ has `@key`,
`@unique`, `@card`, `@range`, `@check`, enums. No policy. The filter dialect
has its own warts ("field names containing periods are NOT supported", the
plain FTS string "doesn't support `OR`, `AND`").

**LanceDB stronger.** The DataFusion expression library in filters (`LIKE`,
`regexp_match`, `CAST`, `array_has_any`, nested paths, date literals).
Structured full-text queries (`MatchQuery` with `fuzziness` and `boost`,
`PhraseQuery(slop)`, `MultiMatchQuery(columns, boosts)`, `BooleanQuery` with
`MUST_NOT`). Hybrid with pluggable rerankers (RRF, linear, cross-encoder,
Cohere, ColBERT, Jina, …) and an explicit `normalize: rank|score`.
`distance_range(lower, upper)` as a threshold cut. `refine_factor`, `ef`,
`search_effort`, `bypass_vector_index` knobs. `add_columns` filled by a SQL
expression (computed backfill at schema time). Full `MERGE` including
`when_not_matched_by_source_delete`. `offset`.

**Borrow.** (1) `distance_range` as a `nearest` threshold predicate. (2)
Phrase and multi-field text queries. (3) A reranker *stage* as a plan node
with a provider named in `cluster.yaml`, the way embedding providers already
are. (4) SQL-fill `add_columns` as the backfill expression `schema apply`
needs for adding a required property to a populated type (#721). (5) Nothing
on branches: GQ's whole-graph lineage is the thing LanceDB's own docs
disclaim; say so in positioning.

### 3.8 Vespa (YQL, grouping, rank profiles)

**Shared.** Schema-first with typed fields including tensors (GQ's
`Vector(N)`); a required `targetHits`/`limit` for nearest-neighbour; exact
fallback when a filter is too selective (Vespa's `approximateThreshold`, GQ's
whole-type exact pass); prefilter as the default; a declared, inspectable
ANN/filter strategy (Vespa's `postFilterThreshold`, GQ's `rrf_plan` and
overfetch policy in `explain`); parameters kept out of the query string
(`@param`, `input.query(q)`; GQ's `$params`); a `relevance`/score per hit.

**GQ stronger.** Vespa has no null predicate ("there is no way to query for a
field that is not set") while its *write-side* selection language has `==
null`, two languages with two semantics; GQ has one expression language with
`is null`. Vespa's counts are approximate by default under `nearestNeighbor`,
`wand` and `weakAnd`, group counts are HyperLogLog, and the session cache
"might cause inaccuracies"; GQ's counts are exact and its `nearest` is labelled
`approximate` honestly in the envelope. Vespa's YQL `timeout` is milliseconds
while the `timeout` parameter is seconds; `in` works only on `match: word|
exact` fields; string semantics depend on schema `match` modes invisible in the
query. GQ has one typed expression model with no schema-dependent operator
meaning. Vespa has no joins beyond child-to-parent attribute imports on
globally replicated parents; GQ traverses typed edges in both directions with
bounds. Vespa has no versioning, no snapshots, no transactions beyond a
test-and-set `condition`.

**Vespa stronger.** Ranking as a first-class, separate, composable surface.
`where` decides the *match set*; `rank(a, b, …)` matches on `a` only and
scores on all; the rank profile decides *order* in four declared phases
(match-phase degradation → first → second → global) with declared budgets
(`rerank-count`, `rank-score-drop-limit`); rank features are typed
expressions (`bm25(title)`, `closeness(field, embedding)`, tensor algebra,
ONNX); hybrid is composed from primitives (`or`, `rank()`,
`reciprocal_rank_fusion`, `normalize_linear`) rather than one `rrf` operator;
`labeled()`/`itemRawScore` give per-subquery scores. Everything GQ cannot say
in 2.10 (weights, thresholds, three arms, rerank, multi-field) Vespa says as
an expression. Grouping as a pipeline (`| all(group(f) each(output(count())))`)
with nested groups and continuation tokens is more expressive than implicit
grouping, at the price of a third DSL. Query profiles are server-side, typed,
inheritable parameter sets that can hold the YQL itself with `%{name}`
splicing: stored queries with defaults and variants.

**Borrow.** (1) The match/rank split: a retrieval position that yields score
columns, and `order` over expressions. (2) Declared cost budgets for ranking
phases as *language* (GQ has `traversal_work_limit`, `ann_nprobes` and the
overfetch ladder; a `rerank N` stage would follow the same pattern). (3)
Continuation tokens for grouped results. (4) Do *not* borrow annotation-
before-term syntax, three bracket conventions, or ranking defined in the
schema rather than the query: GQ's stored queries already give ranking a home
beside the data, in one language.

---

## 4. Verdicts

### 4.1 Where GQ sits

Read against the nine, GQ is a *Datalog rule body* (typed atoms, join by
variable, stratified blocks, set semantics, implicit head grouping) with
*SQL's* typing and null logic minus the null literal, *pgvector's* ranking
shape, *Cypher 5's* block syntax, *Datomic's* "database as a value" for
snapshots, and *Vespa's* idea that cost budgets are part of the language.
Three things it has that none of the nine has together: whole-graph branch,
merge, diff and proposal as statements; a plan-truth contract (acceptance,
replay, declared switches, descriptors) that makes "what will this query do"
a checkable artifact; and a refusal contract designed for a machine author.

Its one structural choice that no comparator makes is **non-closure**: the
language does not consume its own results. That choice is why the three
differentiators above hold, and it is the root of every expressiveness gap the
open issues report.

### 4.2 Keep

These are the load-bearing walls. Each has been argued in an RFC or is
derivable from the invariants, and each has a cost that is already paid.

1. Schema-first, closed vocabulary; nothing in the query the storage model
   lacks.
2. Static typing with declared, nullable parameters; explicit casts; result
   schema inference.
3. Three-valued logic without a null literal; `is null` as the only null test.
4. Reachability semantics for traversal: set semantics on pairs, finite
   `{m,n}` as distance, `traversal_work_limit` admission.
5. Stratified correlated blocks with the safety rule.
6. Filter-before-rank, one total order, index required at compile time,
   approximation declared in the plan and the envelope.
7. Statement-shaped writes with constants per invocation, keyed upsert, D2,
   one commit per query.
8. Control as statements: branches, commits, proposals, snapshots, load,
   explain, settings.
9. One declaration shape; braces everywhere; word-bounded keywords; six
   reserved words.
10. The four-field diagnostic with frozen codes and machine-applicable edits.

### 4.3 Fix, in order of leverage

Each item names the gap, the comparator that shows the shape, and what it
costs the planner. Items 1 to 3 remove most of the open language issues; the
rest are smaller and independent.

| # | Change | Shows the shape | Issues | Planner cost |
|---|---|---|---|---|
| 1 | **Retrieval out of `order`.** A retrieval position yields typed score columns (`$d.@distance`, `$d.@score`) over a declared candidate window; `order` sorts any expression; fusion, weights, thresholds, more arms and a rerank stage become expressions and plan nodes, not grammar. Keeps every contract in 4.2 item 6. | Vespa `where`/rank split; ES retriever tree; LanceDB `distance_range`; RFC 0048 | #648 (partly), #917, the aggregate/`distinct` overload in 2.10 | The ranked `Scan` and `RankFuse` nodes exist; the change is where the compiler puts them and that scores become columns |
| 2 | **One closure step, three forms, all inlined:** (a) block aggregates as values in `return` (`count { } as n`, `exists { } as b`); (b) `optional { }` with Cypher/GQL outer-join semantics and nullable bindings; (c) typed views: a stored read declared as a binding type (`view ContestedClaim(...) = query …`), inlined at compile time. No `WITH`, no general subquery. | Cypher `COUNT {}`/`COLLECT {}`; GQL `OPTIONAL MATCH`; Datalog rules; SQL views | #1050, #658, #649, 2.13 | (a) `SubqueryAggregate` exists; (b) one new join kind (`LeftExpand`) and nullability propagation; (c) compile-time only |
| 3 | **Post-group filter and `collect`.** A Boolean aggregate expression as a filter after `return` (spelling: `having { }` or GQL's `filter`), and `collect($x)`/`collect($x.p)` producing `[T]`/`[struct]` under implicit grouping. | SQL `HAVING`; GQL `FILTER`; Cypher `collect`; Datomic pull | R3 in §5, #649 | One `Filter` above `Aggregate`; one Arrow list aggregate |
| 4 | **Smallest total expression library:** `+ - * /` with typed overflow refusal, `lower/upper/trim/length`, date extraction and `now() - duration`, `coalesce`, `case`. Each total or with one documented failure, so the `can_fail` pushdown rule stays one line. Then computed assignment in `update … set`, under `--if-commit` by default. | Every comparator | 2.14, 2.11 increment row | Expression evaluator arms; one `can_fail` flag |
| 5 | **Paging:** `limit $n` (I64 parameter), an opaque continuation token in the read envelope derived from the total order over the pinned snapshot, `offset` last. | ES `search_after` + PIT; Relay cursors; GQ's own `commit list --page-token` | 2.9 | Token = serialized last sort key + snapshot pin |
| 6 | **Schema catalogue types** `@NodeType`, `@EdgeType`, `@Property`, `@Constraint`, snapshot-pinned, carrying `@description`/`@instruction`. | GraphQL `__schema`; Datomic schema-as-data; `@Operator` precedent | #1058, 2.16 | Registry-derived rows, the catalogue mechanism |
| 7 | **Writes:** pattern-scoped selection (`delete $e where { pattern }`) with one target type; `update` for keyed edges; `set { x: null }` in assignment position; `returning` the affected ids. | Cypher `MATCH … DELETE`; SQL `RETURNING` | #660 lineage, 2.11 table | The mutation `where` already shares the read expression type; a pattern needs the read planner's `Expand` under the write door |
| 8 | **Traversal extras:** `$b.@distance` on the far end of `{m,n}`; multi-hop edge bindings as a list once `collect` exists. | GQL group variables; Cypher QPP lists | 2.6 | BFS already knows the distance |
| 9 | **Planner notifications** beside errors: a cross join of unconnected bindings, a destination-ranked scan of a whole type, a `bm25` without `limit`, a wildcard without a budget. Stable `N…` codes, same four fields. | Neo4j `03N90`/`03N91` | #758, #1057 | The planner knows each already |
| 10 | **Exact integer `sum`** returns an exact integer type (`I64` with overflow refusal) rather than `F64`. | PostgreSQL `numeric`, DuckDB `HUGEINT` | 2.8 | A result type change; `expect shape` in 33 cases |

Four further items are about *evolvability* rather than any one gap and are
argued in §7.5: generic call syntax over a typed function registry, interface
bindings, a generic declaration annotation, and an edition pragma with an N-1
compiler. They rank with items 1 and 2 above once agents are the authors.

### 4.4 Resist

Each of these is requested somewhere, has a comparator that does it, and would
cost one of the walls in 4.2.

- **Unbounded recursion** (`*` as repetition, Datalog fixpoints). It breaks
  the work-budget contract; finite `{m,n}` and a larger `traversal_work_limit`
  are the honest form.
- **Path as a value** with modes and selectors (GQL/Cypher). Its cost is the
  610 pages; its benefit for agents is small; `@distance` and edge lists cover
  the demand.
- **General subqueries and `WITH` pipelines** (SQL, Cypher, GQL `NEXT`). The
  three closure forms in 4.3 item 2 cover the reported shapes without making
  plan acceptance per-stage.
- **Read-write mixing in general** (Cypher `MATCH … SET`, SQL `UPDATE … FROM`).
  Pattern-scoped selection with one target type is the bounded form.
- **A `null` literal in reads.** The mutation-side assignment case is the one
  exception worth making.
- **User-defined functions** before views exist; most requests are views.
- **Schema-resident ranking** (Vespa rank profiles). Stored queries already
  give ranking a home beside the data, in one language.
- **Open labels and dynamic properties** (Cypher, GQL open graph types). The
  closed model is what makes the typing and the diagnostics possible.
- **Tree results** (GraphQL). One `collect` level, flat rows underneath.
- **Semantics by position** (Elasticsearch `must`/`filter`, `knn` filter
  placement). Every GQ position is typed by a context rule; keep it that way
  when retrieval moves.

### 4.5 Two questions the evaluation could not settle from the sources

1. How `@unique` treats a nullable member. The validator's unique image
   (`validate/kernels.rs`) drops a row whose key is null, so nulls never
   collide, SQL's default `NULLS DISTINCT`; the `.pg` guide does not say so,
   and SQL:2023 made the choice explicit (`UNIQUE NULLS [NOT] DISTINCT`)
   because users guess it wrong.
2. Whether a `view` should be inlined (planner sees one shape, no new
   validation scope) or materialised (a derived table with its own commits).
   The schema-design guide's principle 7 argues for inlining; the issue-#1050
   measurements (92 requests per node page) argue that some derived relations
   will want materialisation later. Inline first; the compile-time form costs
   nothing to reverse.


---

## 5. Rosetta stone: six shapes in nine notations

The schema, in `.pg`: `Person { email @key, name, age? }`, `Company { slug
@key, name }`, `WorksAt: Person -> Company { role? }`, `Knows: Person ->
Person`, `Author`, `Post { published: Bool }`, `Authored: Author -> Post`,
`Matter { number @key }`, `Passage { locator, text @index, embedding:
Vector(N) @index }`, `PassageOf: Passage -> Matter`. Document stores
(Elasticsearch, Vespa, LanceDB) denormalize the one-hop relationships onto the
child document, which is how they are used in practice; the snippets say so
where it changes the meaning.

### R1. Keyed lookup, one hop, filter, order, page

*People over 30 at company `$slug`, by name, first 20.*

```gq
query staff($slug: String) {
  match { $c: Company { slug: $slug }  $p worksAt $c  $p.age > 30 }
  return { $p.name }
  order { $p.name asc }
  limit 20
}
```

```sql
-- SQL
SELECT p.name FROM person p
JOIN works_at w ON w.src = p.id JOIN company c ON c.id = w.dst
WHERE c.slug = $1 AND p.age > 30
ORDER BY p.name, p.id LIMIT 20;          -- p.id added by hand for a total order

-- SQL/PGQ (SQL:2023)
SELECT name FROM GRAPH_TABLE (g
  MATCH (p:Person)-[:WorksAt]->(c:Company WHERE c.slug = $1)
  WHERE p.age > 30
  COLUMNS (p.name AS name))
ORDER BY name FETCH FIRST 20 ROWS ONLY;
```

```cypher
// Cypher and GQL are the same here
MATCH (p:Person)-[:WORKS_AT]->(c:Company {slug: $slug})
WHERE p.age > 30
RETURN p.name ORDER BY p.name LIMIT 20
```

```graphql
# GraphQL, Hasura-style conventions (the spec defines none of where/order_by/limit)
query Staff($slug: String!) {
  company(where: {slug: {_eq: $slug}}) {
    employees(where: {age: {_gt: 30}}, order_by: {name: asc}, limit: 20) { name }
  }
}
```

```clojure
;; Datomic: no order, no limit in the query; the caller sorts
[:find ?name :in $ ?slug
 :where [?c :company/slug ?slug] [?p :person/works-at ?c]
        [?p :person/age ?age] [(> ?age 30)] [?p :person/name ?name]]
```

```
# CozoScript
?[name] := *company{slug: $slug, id: c}, *works_at{src: p, dst: c},
           *person{id: p, name, age}, age > 30
:order name :limit 20
```

```json
// Elasticsearch, company denormalized onto the person document
{"query": {"bool": {"filter": [{"term": {"company.slug": "acme"}}, {"range": {"age": {"gt": 30}}}]}},
 "sort": [{"name.keyword": "asc"}, {"_id": "asc"}], "size": 20, "_source": ["name"]}
```

```
-- ES|QL
FROM people | WHERE company.slug == "acme" AND age > 30 | SORT name | LIMIT 20 | KEEP name
```

```sql
-- Vespa YQL, company imported through a parent reference
select name from person where company_slug contains "acme" and age > 30 order by name limit 20
```

```python
# LanceDB: the filter is a SQL string; the builder has no ORDER BY, so order comes from SQL or the client
tbl.search().where("company_slug = 'acme' AND age > 30").select(["name"]).limit(20).to_arrow()
```

What the row shows: GQ's direction-from-schema makes `$p worksAt $c` the
whole join; every other notation spells the join (`JOIN … ON`, `-[:X]->`,
nested field, repeated variable). GQ and ES (`_shard_doc`/`_id`) are the only
two whose order is total without the author adding a key; SQL and Cypher need
`p.id` for a stable page.

### R2. Negation

*People with no employer.*

```gq
match { $p: Person  not { $p worksAt $_ } }  return { $p.name }
```

```sql
SELECT p.name FROM person p WHERE NOT EXISTS (SELECT 1 FROM works_at w WHERE w.src = p.id);
```

```cypher
MATCH (p:Person) WHERE NOT EXISTS { (p)-[:WORKS_AT]->() } RETURN p.name   // Cypher 5 / GQL
```

```graphql
person(where: {_not: {works_at: {}}}) { name }        # Hasura convention
```

```clojure
[:find ?name :where [?p :person/name ?name] (not [?p :person/works-at _])]
```

```
?[name] := *person{id: p, name}, not *works_at{src: p}      # Cozo
Unemployed(p) :- Person(p), !WorksAt(p, _).                 # Soufflé
```

```json
{"query": {"bool": {"must_not": {"exists": {"field": "company"}}}}}     // Elasticsearch
```

```sql
select name from person where employed = false   -- Vespa: no existence test; a sentinel field
```

```python
tbl.search().where("company_slug IS NULL")        # LanceDB
```

Every notation has it except Vespa. GQ's block spelling is Cypher 5's
`EXISTS { }` with the braces and without the keyword; Datalog's `not` is the
same construct with the same safety rule.

### R3. Relationship count as a filter, plus the count as a value

*Authors with at least three published posts, with the count, most prolific
first.*

```gq
query prolific() {
  match {
    $a: Author
    count { $a authored $p  $p.published = true } >= 3     // the filter
    $a authored $q  $q.published = true                    // the same pattern again, for the value
  }
  return { $a.name, count($q) as posts }
  order { posts desc, $a.name }
  limit 20
}
```

```sql
SELECT a.name, count(*) AS posts FROM author a
JOIN authored x ON x.src = a.id JOIN post p ON p.id = x.dst
WHERE p.published GROUP BY a.id, a.name
HAVING count(*) >= 3 ORDER BY posts DESC, a.name LIMIT 20;
```

```cypher
MATCH (a:Author)-[:AUTHORED]->(p:Post {published: true})
WITH a, count(p) AS posts WHERE posts >= 3
RETURN a.name, posts ORDER BY posts DESC, a.name LIMIT 20
```

```
-- GQL: linear composition with NEXT and FILTER
MATCH (a:Author)-[:Authored]->(p:Post {published: true})
RETURN a, count(p) AS posts GROUP BY a
NEXT FILTER posts >= 3
RETURN a.name, posts ORDER BY posts DESC, a.name LIMIT 20
```

```clojure
;; Datomic: the count, grouped by name; no HAVING, no order
[:find ?name (count ?p) :with ?p
 :where [?a :author/name ?name] [?a :author/posts ?p] [?p :post/published true]]
```

```
# Cozo: a second rule is the HAVING
cnt[a, count(p)] := *authored{src: a, dst: p}, *post{id: p, published: true}
?[name, c] := cnt[a, c], c >= 3, *author{id: a, name}
:order -c, name :limit 20
```

```
-- ES|QL
FROM posts | WHERE published | STATS posts = COUNT(*) BY author
| WHERE posts >= 3 | SORT posts DESC | LIMIT 20
```

```json
// Elasticsearch Query DSL: a bucket_selector pipeline aggregation is the HAVING
{"size": 0, "query": {"term": {"published": true}},
 "aggs": {"by_author": {"terms": {"field": "author", "size": 20, "order": {"_count": "desc"}},
          "aggs": {"min3": {"bucket_selector": {"buckets_path": {"c": "_count"}, "script": "params.c >= 3"}}}}}}
```

```sql
-- Vespa grouping
select * from post where published = true | all(group(author) order(-count()) max(20) each(output(count())))
```

What the row shows: GQ is the only notation that writes the pattern twice,
because a block is a predicate and a return aggregate cannot be filtered. Each
other language has one of: `HAVING`, a `WITH`/`NEXT` stage, a second rule, a
pipeline aggregation. This is 2.2 and 2.8 in one example.

### R4. Bounded reachability

*People at distance exactly two from `$email`.*

```gq
match { $p: Person { email: $e }  $p knows{2,2} $f }  return { $f.name }
```

```cypher
// Cypher: paths of length 2; to get GQ's answer, exclude distance-1 nodes and the source
MATCH (p:Person {email: $e})-[:KNOWS*2..2]->(f)
WHERE NOT (p)-[:KNOWS]->(f) AND f <> p
RETURN DISTINCT f.name
```

```
-- GQL: quantified path pattern; same exclusions needed for distance semantics
MATCH (p:Person {email: $e}) ((x)-[:Knows]->(y)){2} (f)
WHERE NOT EXISTS { MATCH (p)-[:Knows]->(f) } AND f <> p
RETURN DISTINCT f.name
```

```sql
-- SQL: two derived tables and an anti-join
WITH d1 AS (SELECT dst FROM knows WHERE src = $1),
     d2 AS (SELECT k.dst FROM knows k JOIN d1 ON k.src = d1.dst)
SELECT DISTINCT p.name FROM d2 JOIN person p ON p.id = d2.dst
WHERE d2.dst NOT IN (SELECT dst FROM d1) AND d2.dst <> $1;
```

```graphql
person(email: $e) { knows { knows { name } } }   # a tree of paths: duplicates, and the source itself
```

```
# Cozo: Datalog's set semantics give GQ's answer once distance-1 is subtracted
d1[f] := *knows{src: $p, dst: f}
d2[f] := d1[m], *knows{src: m, dst: f}, not d1[f], f != $p
?[name] := d2[f], *person{id: f, name}
```

Elasticsearch, Vespa and LanceDB cannot express it. What the row shows: GQ's
`{2,2}` is a *distance*; Cypher's `*2..2` and GQL's `{2}` are *path lengths*,
and the two differ on every graph with a triangle. Datalog agrees with GQ.

### R5. Hybrid retrieval scoped by a traversal

*Passages of matter `$n`, ranked by RRF of vector similarity and BM25, top 5.*

```gq
query passages($n: String, $v: Vector(1536), $q: String) {
  match { $m: Matter { number: $n }  $p passageOf $m }
  return { $p.locator, rrf(nearest($p.embedding, $v), bm25($p.text, $q)) as score }
  order { rrf(nearest($p.embedding, $v), bm25($p.text, $q)) }
  limit 5
}
```

```sql
-- PostgreSQL with pgvector and tsvector: RRF by hand
WITH scope AS (SELECT p.* FROM passage p JOIN passage_of x ON x.src = p.id
               JOIN matter m ON m.id = x.dst WHERE m.number = $1),
v AS (SELECT id, row_number() OVER (ORDER BY embedding <-> $2) AS r FROM scope LIMIT 50),
t AS (SELECT id, row_number() OVER (ORDER BY ts_rank_cd(tsv, plainto_tsquery($3)) DESC) AS r
      FROM scope WHERE tsv @@ plainto_tsquery($3))
SELECT s.locator, coalesce(1.0/(60+v.r), 0) + coalesce(1.0/(60+t.r), 0) AS score
FROM scope s LEFT JOIN v USING (id) LEFT JOIN t USING (id)
WHERE v.id IS NOT NULL OR t.id IS NOT NULL
ORDER BY score DESC, s.id LIMIT 5;
```

```cypher
// Neo4j Cypher 25: two SEARCH subclauses in a UNION ALL, RRF assembled by hand (abridged).
// SEARCH prefilters only on declared filter properties of the index; the traversal to Matter is a post-filter.
CALL () {
  MATCH (p:Passage) SEARCH p IN (VECTOR INDEX passage_embedding FOR $v LIMIT 50) SCORE AS s
  MATCH (p)-[:PASSAGE_OF]->(:Matter {number: $n})
  WITH p, s ORDER BY s DESC WITH collect(p) AS ps UNWIND range(0, size(ps)-1) AS r RETURN ps[r] AS p, r
  UNION ALL
  MATCH (p:Passage) SEARCH p IN (FULLTEXT INDEX passage_text FOR $q LIMIT 50) SCORE AS s
  MATCH (p)-[:PASSAGE_OF]->(:Matter {number: $n})
  WITH p, s ORDER BY s DESC WITH collect(p) AS ps UNWIND range(0, size(ps)-1) AS r RETURN ps[r] AS p, r
}
RETURN p.locator, sum(1.0 / (60 + r)) AS score ORDER BY score DESC LIMIT 5
```

```json
// Elasticsearch retriever tree, matter denormalized onto the passage
{"retriever": {"rrf": {"rank_constant": 60, "rank_window_size": 50, "retrievers": [
   {"knn": {"field": "embedding", "query_vector": [/*…*/], "k": 50, "num_candidates": 100,
            "filter": {"term": {"matter": "M-1"}}}},
   {"standard": {"query": {"bool": {"must": {"match": {"text": "…"}},
                                     "filter": {"term": {"matter": "M-1"}}}}}}]}},
 "size": 5, "_source": ["locator"]}
```

```sql
-- Vespa: retrieval in the query, fusion in the schema's rank profile
select locator from passage
where matter_number contains "M-1"
  and (({targetHits: 50}nearestNeighbor(embedding, q)) or userInput(@q))
-- rank-profile hybrid { global-phase { expression: reciprocal_rank_fusion(closeness(field, embedding), bm25(text)) } }
-- &input.query(q)=…&ranking=hybrid&hits=5
```

```python
# LanceDB, matter denormalized
(tbl.search(query_type="hybrid").vector(v).text(q)
    .where("matter = 'M-1'", prefilter=True).rerank(reranker=RRFReranker()).limit(5).to_pandas())
```

```
# Cozo: the search atoms join with the scope; RRF needs ranks, which Datalog has no window for
scope[p] := *passage_of{src: p, dst: m}, *matter{id: m, number: $n}
?[locator, d, s] := scope[p], ~passage:emb{p | query: $v, k: 50, bind_distance: d},
                    ~passage:fts{p | query: $q, k: 50, bind_score: s}, *passage{id: p, locator}
:order d, -s :limit 5          # an inner join of the two arms, not a fusion
```

GQL has no standard retrieval; GraphQL leaves it to a resolver argument. What
the row shows: GQ, Elasticsearch, Vespa and LanceDB have a *first-class*
hybrid; GQ's is the shortest and the only one where the scope is a graph
traversal rather than a denormalized field; Vespa's is the most expressive;
Elasticsearch's composes most freely; SQL's is honest about what RRF is.
GQ's score appears twice because the projected rank must repeat the order key.

### R6. Upsert a keyed node and an edge

```gq
query hire($email: String, $name: String, $slug: String, $role: String) {
  insert Person { email: $email, name: $name }          // keyed: upsert
  insert WorksAt { from: $email, to: $slug, role: $role } // one commit for both
}
```

```sql
BEGIN;
INSERT INTO person (email, name) VALUES ($1, $2)
  ON CONFLICT (email) DO UPDATE SET name = EXCLUDED.name;
INSERT INTO works_at (src, dst, role)
  SELECT p.id, c.id, $4 FROM person p, company c WHERE p.email = $1 AND c.slug = $3;
COMMIT;
```

```cypher
MERGE (p:Person {email: $email}) SET p.name = $name
WITH p MATCH (c:Company {slug: $slug})
MERGE (p)-[w:WORKS_AT]->(c) SET w.role = $role
```

```graphql
mutation Hire($email: String!, …) { upsertPerson(input: {…}) { id }  hire(input: {…}) { id } }   # root fields run serially
```

```clojure
;; Datomic: unique identity on :person/email makes this an upsert; a lookup ref names the company
[{:person/email "ada@x" :person/name "Ada" :person/works-at [:company/slug "acme"]}]
```

```
?[email, name] <- [[$email, $name]]  :put person {email => name}      # Cozo, then a second :put for the edge
```

```json
// Elasticsearch: id = email, company denormalized; "doc_as_upsert"
POST /person/_update/ada@x  {"doc": {"name": "Ada", "company": {"slug": "acme", "role": "Engineer"}}, "doc_as_upsert": true}
```

```json
// Vespa: create-if-absent put with field operations
PUT /document/v1/ns/person/docid/ada@x?create=true
{"fields": {"name": {"assign": "Ada"}, "company_ref": {"assign": "id:ns:company::acme"}}}
```

```python
tbl.merge_insert("email").when_matched_update_all().when_not_matched_insert_all().execute(rows)  # LanceDB
```

What the row shows: GQ, Datomic and LanceDB make "keyed insert is upsert" a
property of the schema, so the statement carries no merge vocabulary; SQL and
Cypher spell the merge every time. GQ's edge endpoints are *keys*, not
internal ids, which removes SQL's `SELECT p.id … WHERE p.email = $1` dance.
GQ alone has no computed value anywhere in the statement.

---

## 6. Scorecard

● strong, ◐ partial, ○ weak or absent. The mark is relative to the axis as
defined in §0, for a typed-property-graph, many-agent workload; a ○ is not a
defect in a system built for another workload (Elasticsearch has no traversal
because it is not a graph database). Datalog is marked for its strongest
member on each row, named where it matters.

| Axis | GQ | SQL | GQL | Cypher | GraphQL | Datalog | ES | LanceDB | Vespa |
|---|---|---|---|---|---|---|---|---|---|
| 1 Model fidelity (typed property graph) | ● | ◐ view | ◐ optional schema | ◐ open | ○ API | ◐ encoding | ○ | ○ | ○ |
| 2 Compositional closure | ○ | ● | ● | ● | ○ | ● | ◐ retrievers, aggs | ◐ via SQL | ◐ |
| 3 Binding and scope | ● | ◐ explicit joins | ● | ● | ○ | ● | ○ | ○ | ○ |
| 4 Static typing | ● | ◐ | ◐ closed types optional | ○ | ● | ◐ Soufflé/Rel ●, Datomic ○ | ◐ ES\|QL | ◐ string filter | ◐ |
| 5 Absence | ● | ◐ | ◐ | ◐ | ◐ bubbling | ● closed world | ◐ | ◐ | ○ no null test |
| 6 Traversal | ◐ reachability only | ◐ recursive CTE, PGQ | ● paths, modes | ● | ○ | ● recursion | ○ | ○ | ○ parent–child |
| 7 Sub-pattern predicates | ◐ predicate, constant RHS | ● | ◐ EXISTS only | ● EXISTS/COUNT/COLLECT | ○ | ● | ◐ | ○ | ○ |
| 8 Aggregation | ◐ no HAVING, no collect | ● | ● | ● | ○ | ◐ varies | ● | ○ via SQL | ● approximate counts |
| 9 Order and cut | ◐ total order, no paging | ◐ | ◐ | ◐ | ◐ cursor spec only | ○ | ● search_after | ◐ | ◐ |
| 10 Retrieval (contracts / expressiveness) | ● / ○ | ◐ / ◐ | ○ / ○ | ◐ / ◐ | ○ | ◐ Cozo | ◐ / ● | ● / ● | ● / ● |
| 11 Writes | ◐ bounded, no computed | ● | ◐ no MERGE | ● | ◐ opaque | ◐ Datomic tx fns ● | ◐ no tx | ◐ merge_insert | ◐ |
| 12 Time and branches | ● | ◐ system time | ○ | ○ | ○ | ● Datomic, Cozo | ○ | ◐ table versions, no merge | ○ |
| 13 Reuse | ◐ named queries | ● | ◐ procedures | ◐ | ◐ fragments | ● rules | ◐ templates | ○ | ◐ profiles |
| 14 Expressions | ○ | ● | ● | ● | ○ | ● | ● | ● | ◐ |
| 15 Result shape | ◐ flat, node struct | ◐ | ● | ● | ● tree | ◐ pull ● | ● | ◐ | ● |
| 16 Self-description | ◐ explain ●, schema ○ | ● | ○ | ◐ | ● | ● Datomic | ◐ | ◐ | ○ |
| 17 Refusal contract, machine authorship | ● | ◐ | ◐ GQLSTATUS | ◐ | ● | ◐ | ◐ | ○ | ○ |

Reading down GQ's column: every ● is a contract (model, typing, nulls,
binding, retrieval contracts, time, refusals) and every ○ or ◐ is
expressiveness (closure, traversal breadth, blocks as values, HAVING, paging,
retrieval breadth, computed writes, reuse, expressions, result nesting, schema
introspection). That split is the finding. The contracts are the hard part and
they are done; the expressiveness items in 4.3 are additions that none of them
has to give up.

---

## 7. Expressiveness and evolvability: is there a ceiling?

The question this section answers: does GQ's initial structure (one
declaration shape, one pipeline, closed types, no closure) put a ceiling on how
far it can be extended, and how does the arrival of much stronger coding
agents change what the language should optimise for?

### 7.1 Two kinds of expressiveness

Expressiveness has to be split before it can be judged, because the two halves
move in opposite directions.

- **Grammar expressiveness**: what can be said in one query. GQ is low here
  (§2.2, §2.14, §6), and every item in §4.3 raises it.
- **System expressiveness**: what can be *defined once and reused* by every
  author without a release of the engine. SQL has views and functions, Datalog
  has rules, GraphQL has fragments and directives, Vespa has rank and query
  profiles, Elasticsearch has templates and scripts. GQ has stored queries,
  which other queries cannot bind to (§2.13). GQ is lowest of the nine here,
  and this is the half that decides whether there is a ceiling.

A language with high grammar expressiveness and low system expressiveness
(Cypher before procedures, Elasticsearch's Query DSL) grows by adding grammar
until the grammar is the ceiling; Elasticsearch's answer was a second language
(ES|QL), Vespa accreted three DSLs, SQL accumulated several hundred reserved
words and a dialect per vendor. A language with a small grammar and high system
expressiveness (Datalog: every rule is a new relation in the same notation)
grows by adding *definitions*, and its grammar can stay small for decades. The
no-ceiling structure is the second one. GQ is currently neither: its grammar is
small *and* its definitions are closed.

### 7.2 Where GQ's extension points are, and which ones are grammar

Walking the surface for what a new capability would touch:

| Surface | Extension today | Grammar change needed? | Breaks stored text? |
|---|---|---|---|
| New statement kind (`branch`, `commit`, `load csv`, `explain`) | ordered choice at file level, word-bounded keyword | yes, additive | no |
| New clause in `match` (`optional { }`) | ordered choice in `clause` | yes, additive | no |
| New clause after `limit` (`having`, `offset`) | optional suffix | yes, additive | no |
| New Boolean/comparison operator | expression ladder | yes, additive unless a word is reserved | only if reserved (as `and/or/not/is/null/in` were, GQ 2.0) |
| New scalar, aggregate, search or ranking **function** | each is its own grammar rule today: `search_call = { "search" ~ … }`, `agg_func = { "count" \| "sum" \| … }`, `rrf_call` fixed at two `rank_expr` arms | **yes, every time** | no, but every function is a release |
| New session setting | `DEFINITIONS` registry; `set <ident> = <value>` is generic | no | no |
| New schema annotation | unknown annotations retained as metadata | no | no |
| New schema type (`Map`, `Struct`, `Json`) | `base_type` list, `type_ref` | yes, additive | no |
| New engine catalogue type | `@`-prefixed registry rows | no | no |
| New plan node / operator | `physical_nodes!` and `logical_kinds!` registries | no | no |
| A derived relation (a view) | none | would be additive (`view … = query …`) | no |
| Polymorphic binding over an interface | `Catalog.interfaces` exists "for Phase 2 polymorphic queries"; `$x: Named` is `T1` today | no grammar change (a type name is a type name) | no |
| A query hint or option (`@timeout`, `@cache`, `@approximate`) | only `@description`/`@instruction` on a declaration | the annotation rule is closed; a generic `@ident(args)` would be additive | no |
| Language version | `GQ_LANGUAGE_VERSION` is `(2, 3)` in the compiler; no edition pragma in a file; the compiler has one grammar at a time | n/a | a breaking bump re-parses every stored query at boot and **quarantines the graph** on failure |

Three ceilings stand out, and none is structural:

1. **Functions are grammar.** A `lower()` or a third `rrf` arm is a pest rule, a
   parser arm, a type-check arm and a release. A generic call form
   (`ident "(" args ")"`) resolved against a typed function registry, with a
   `@Function` catalogue type listing name, signature, nullability rule and
   `can_fail`, turns every future function into a registry row. The registry
   is exactly how `@Operator`, `@Pass` and `@Setting` already work.
2. **Definitions are closed.** No view, no rule, no user-named relation. The
   fix is 4.3 item 2(c); its importance is larger than §2.13 said once agents
   are the authors (7.4).
3. **Versioning is all-or-nothing.** Stored queries persist as text and are
   recompiled at boot; a breaking grammar change is a graph outage unless the
   operator rewrote every stored query first (the shared-expression RFC's
   reserved-word break is the recorded instance). The compiler already carries
   a version; what is missing is a per-file or per-registration edition
   (`gq 2;` as the first settings line, or the registry recording the version
   a stored query was validated under) and a compiler that accepts edition
   N-1 for one release. Rust editions, Neo4j's `CYPHER 25` prefix with a
   per-database default, and GraphQL's `@deprecated`-first rule are the three
   working precedents; the compatibility-surfaces RFC's per-rule hashes are the
   detection half and need this as the migration half.

What is *not* a ceiling, checked against the planner model in `execution.md`:

- The one-scan-root-per-component rule survives inlined views (they are
  patterns), `optional { }` (a new join kind), projected blocks (the operator
  exists) and interface bindings (a scan over several tables with a common
  projection, the shape wildcard traversal already lowers to).
- Flat Arrow rows survive `collect` (`list<struct>` is Arrow-native) and would
  survive a nested result model later; the envelope's `columns` already carry
  Arrow types.
- Plan acceptance survives every additive clause because acceptance checks a
  plan against the checked query's requirements, not against a fixed grammar;
  only the exact-subset fragment is closed, by design and versioned.
- The `@` namespace (meta-fields, catalogue types) and the `__` reservation
  give engine-owned names a home that can never collide with user schema,
  which is the discipline GraphQL's `__` and SQL's `pg_` prefixes show
  paying off over decades.

### 7.3 Author-protecting versus system-protecting restrictions

GQ's refusals fall into two classes, and the distinction decides which ones
should loosen as authors get better.

**System-protecting** (they hold a bound or a contract; author skill is
irrelevant): `nearest` requires `limit`; `traversal_work_limit` admission;
finite `{m,n}`; constants per invocation in writes; D2; one commit per query;
`T27` index required for full text; filter-before-rank; `T59` one scan root
for `rrf` arms; the mutation `where` taking no binding; wildcard endpoints
declared. These should stay whatever the author is.

**Author-protecting** (they exist because a human, or a 2025 model, gets the
construct wrong): `T43` a Boolean in `return` needs an alias; `T42` an order
key must appear in `return`, chosen "because an explicit error with an obvious
fix costs one cheap retry while a silent row change costs a wrong answer";
non-associative comparison (`1 < $x < 3` refused); no `null` literal; `T38` a
search predicate must stand alone or be `= true`; a block's right side a
constant; implicit grouping with `T58`/`T9` guards. Each is a good default for
a human and a defensible one for a model that retries; none protects the
engine.

The rule this gives: as agents improve, **lift author-protecting restrictions
when they block a round trip, keep system-protecting ones, and never confuse
the two**. §4.3 already asks for the block right side and the order-key rule
to loosen; `T43` and the chained comparison are harmless to keep. The `null`
literal stays out of reads because the three-valued rules it would import are
a correctness hazard for any author, which makes it system-protecting after
all.

### 7.4 What stronger coding agents change

The design brief in the README is that agents and UIs become "thin stateless
consumers of durable state". Stronger agents change four things about the
language that serves them, and only one of them points toward *less*
expressiveness.

1. **The novelty tax falls; the round-trip tax does not.** GQ's bet in §2.17
   (regularity and diagnostics over familiarity) ages well: a model that reads
   the grammar, the skill and a four-field refusal, and repairs in one turn,
   does not need Cypher's training-data prior. Cypher's bet ages badly (two
   live versions, monthly deprecations, the documented `OPTIONAL MATCH`
   failure). But every missing construct is still a round trip, and a round
   trip costs tokens, latency and a consistency window regardless of how
   capable the author is. The 92-request node page (#1050) is not an author
   problem. So the balance shifts *toward* grammar expressiveness, not away
   from it.
2. **Agents will route around a ceiling, and the detour lands in the wrong
   place.** An agent that cannot write "contested claims about X" as one
   query writes three queries and the join in its own context. That logic is
   then unversioned, unreviewed, unshared and recomputed by every agent in its
   own words, which is precisely the convergence failure the schema-design
   guide warns about. The only way to keep agent-authored logic in the graph,
   where branches, review and merge apply to it, is for the language to have a
   place to put it: views, stored queries that other queries bind to, and
   functions in a registry. This is why system expressiveness (7.1) matters
   more under agents than under humans, and why it ranks above most grammar
   additions.
3. **Agents extend the system through data, not releases.** A human adds a
   function by shipping a compiler; an agent adds a view by writing a `.gq`
   file on a branch, linting it, and merging. Every extension point that is a
   registry row or a stored declaration (views, functions in a registry,
   catalogue types, settings, annotations) is agent-reachable; every one that
   is a grammar rule is not. The no-ceiling design is therefore the one where
   the *grammar* stays a small fixed core and *everything else* is a
   declaration the same agents can author under the same policy and review.
   Datalog is the proof that this scales; `GraphPolicy` nodes with a
   `check_query` are GQ already doing it by hand.
4. **Verification becomes the product.** The more autonomously an agent acts,
   the more it needs to *prove* a query before running it: `lint` with the
   operation descriptor, `explain` with validation scope, typed result
   columns, `recall: approximate` in the envelope, `--if-commit`, `as commit`
   proposals, `branch merge … dry run`. These are GQ's strongest assets for an
   agent workload and no comparator has the set. They should be extended with
   the one thing agents cannot get today: a *warning* channel (4.3 item 9),
   since a refusal is a stop and a notification is a chance to choose.

A fifth change arrived while this was being written. Parameswaran, Shankar,
Zaharia, Cheung, Crooks, Gonzalez, Hellerstein and Stoica ("Intelligence is
Free, Now What? Data Systems for, of, and by Agents", BAIR, July 2026) describe
the agent workload as *agentic speculation*: "a high-volume, heterogeneous
stream of work spanning schema introspection, columnar exploration, partial and
then full query formulation", thousands of queries per user request, of which
"only 10–20% of the sub-plans are distinct", with task success rising with the
number of attempts. Their asks of a data system map onto this evaluation
almost line by line: cheap in-language schema introspection (4.3 item 6);
reuse across overlapping sub-plans (a planner concern GQ's `logical_hash`
already identifies); batches of queries with per-query approximation
requirements (the declared `recall` and `ann_nprobes` policy, generalised);
"higher-level primitives rather than requiring agents to list each SQL query
explicitly" (views and registry functions, 7.5); and a *proactive* system that
"instead of executing an expensive query … could first provide the agent a
latency estimate", since "an agent can accept any form of textual feedback"
(the notification channel, 4.3 item 9, and `explain` as the estimate). On the
"data systems of agents" side they name the two things GQ already has and most
systems do not: that "knowledge graphs suffer from … their lack of structured
search", answered by typed, indexed properties rather than keyword or
embedding retrieval alone; and that under thousands of concurrent agents "the
effects of the vast majority of these transactions need to be rolled back,
with only the one 'correct' transaction's result persisting", which is the
branch, `as commit` and `as unattached branch` model with a merge. Their
"data systems by agents" section (engines synthesised per workload) implies
that the durable asset of a database is its *observable contract*, the
language and its executable corpus, not its implementation; that is an
argument for spending on GQ's semantics and the GQT corpus over any one
operator.

The single counter-current: agents are also better at *decomposition*, so one
could argue the language can stay small and let the agent be the compiler. The
argument fails on points 2 and 4: decomposition in the agent's context is
invisible to review, to policy, to plan truth and to other agents. Expressiveness
belongs in the shared language exactly to the extent that the shared language
can still verify it, which is the boundary 7.3 draws.

### 7.5 The no-ceiling shape, concretely

Putting 7.2 to 7.4 together, the structure that has no ceiling is already
mostly present and needs five moves, in this order:

1. **Generic call syntax plus a typed function registry** (`@Function`
   catalogue type; `can_fail`, nullability and type signature as data). After
   this, scalar functions, new aggregates, new rankings and new fusion arms
   are rows, not grammar; the reserved-word list stays at six.
2. **Views as declarations** (4.3 item 2(c)), inlined, typed like node types,
   bindable, versioned with the schema, reviewable on a branch. After this,
   derived relations are agent-authored data.
3. **Interface bindings** (`$x: Named`), the "Phase 2 polymorphic queries" the
   catalog already reserves space for. After this, heterogeneous discovery
   (#648) and uniform results over several types need no `UNION`.
4. **A generic declaration annotation** (`@ident(args)` on `query`, kept as
   metadata unless the engine knows it), the GraphQL-directive move, so hints,
   caching, timeouts and client metadata have a home before each is designed.
5. **An edition pragma and an N-1 compiler**, so the one breaking change a
   decade needs is a migration, not a quarantine.

With those, the fixed core is: one declaration shape, typed pattern
conjunction with stratified blocks, one expression ladder, one pipeline with
additive clauses, statements for control, `@` for the engine. Everything
agents will want to add lives outside that core as declarations. That is the
same split Datalog has had since 1977 and the opposite of the one that made
Elasticsearch write a second language.

### 7.6 Is there a formalism for this? Yes, two, and GQ's place in them

The grammar/system split in 7.1 is a design lens, not a theory. Two bodies of
theory stand behind it and give the words precise meanings.

**Expressive power of query languages** (database theory). A query language's
expressive power is the class of queries it can define, measured against logics
and complexity classes, independent of syntax. The standard ladder:

```text
CQ  ⊂  UCQ  ⊂  FO (relational algebra, "Codd-complete")  ⊂  FO + TC / Datalog  ⊂  LFP (= PTIME on ordered structures)
```

Codd (1972) defined relational completeness as first-order power; Aho and
Ullman (1979) proved first-order logic cannot express transitive closure, the
reason every language that wants reachability adds recursion (`WITH RECURSIVE`,
Datalog rules, Cypher `*`); Chandra and Harel (1980–85) placed Datalog and its
negation/aggregation extensions; Vardi (1982) and Immerman separated *data*
complexity (fixed query, growing data) from *combined* complexity and showed
Datalog with least fixpoint captures PTIME on ordered structures. For graphs,
Angles, Arenas, Barceló, Hogan, Reutter and Vrgoč (ACM CSUR 2017) organise the
path side: RPQ (regular path queries), CRPQ (conjunctions of them), UCRPQ, and
data paths (Libkin, Martens, Vrgoč, JACM 2016); Francis et al. gave Cypher a
formal semantics (SIGMOD 2018), Deutsch et al. did the same for GQL and
SQL/PGQ patterns (SIGMOD 2022), and the pattern calculus GPC (PODS 2023) is the
formal core both standards share. Two further measures matter beside power:
*succinctness* (how long the shortest expression of a query is; two languages
with equal power can differ exponentially) and *data complexity* (whether the
class is tractable at all).

GQ's read core on this ladder, from `query.pest` and the type rules:

- A `match` block is a **conjunctive query** over a fixed typed signature:
  unary atoms (`$p: Person { … }`), binary atoms (`$a knows $b`), and built-in
  comparisons. Join is by variable repetition.
- `not { }`, `exists { }` and `count|sum|min|max|avg(…) { } op c` are
  **stratified negation and aggregation sub-goals** with the Datalog safety
  (range-restriction) rule, `T39`. No recursion, so stratification is trivial.
- `return` with implicit grouping is a **rule head with aggregation**.
- Disjunction is **restricted**: Boolean `or` over one tuple's values and edge
  alternation `(a | b)`; there is no union of two *patterns*, so GQ is not
  UCQ-complete (it cannot say "match pattern A or pattern B" over different
  bindings in one query).
- `{m,n}` over a label set is a **bounded regular path query**: `(a|b)^{m..n}`,
  a strict fragment of RPQ that is still first-order (bounded repetition
  unfolds), with *shortest-distance set semantics* rather than path
  semantics; unbounded `*` is refused, so transitive closure is inexpressible.
- `order`/`limit` and `nearest`/`bm25`/`rrf` are **ordered, top-k** semantics,
  outside the pure relational ladder (which is unordered); top-k with
  approximate `nearest` is a separate dimension the theory treats under
  ranking/preference queries, not expressive power.
- No arithmetic or functions, so the built-in predicate set is comparisons
  only.

So: GQ's read language is **non-recursive stratified Datalog with aggregation
over a fixed typed schema, minus arithmetic, plus bounded label-set paths with
distance semantics and top-k ranking**. Because `not { }` blocks nest (the
corpus has a double-negation case), De Morgan gives disjunction over one
pattern's bindings, so for Boolean conditions on a conjunctive pattern GQ is
first-order complete with aggregates; what it cannot do is a *union at the
projection*, a UCQ whose arms bind different variables ("a Person or a Company
matching …" as one result). Data complexity is TC⁰ (AC⁰ without aggregates),
so every query is constant-depth parallel and LOGSPACE; combined complexity is
NP-complete for the conjunctive fragment. It sits below Cypher/GQL (no
recursion, no path values, no projection-level union) and far below SQL with
recursion (Turing-complete in practice). Standard examples: it *can* express "people
with no employer" (negation), "authors with ≥ 3 posts" (aggregate sub-goal),
"distance-2 neighbours" (bounded RPQ); it *cannot* express "everyone
reachable from X" (transitive closure), "the latest assertion per customer"
(needs a second stage or window), "a Person or a Company matching …" as one
projected result (union at the head), "the number of nodes is even" (no
arithmetic, so no `count mod 2`), or "balance × rate" (arithmetic). Every
addition in §4.3 and §7.5 except unbounded recursion keeps data complexity in
TC⁰ and the language first-order with counting, which is the property that
keeps plan acceptance tractable; unbounded recursion would move it to
NLOGSPACE (reachability) or PTIME (general fixpoint), a cost the operator
should declare.

**Expressive power of programming languages** (PL theory). Felleisen's "On
the Expressive Power of Programming Languages" (Science of Computer
Programming, 1991) gives the criterion the 7.2 table was reaching for: a
construct is *macro-expressible* in a language if it can be eliminated by a
local, compositional rewrite into the rest of the language; adding a
macro-expressible construct is a **conservative extension** (it changes
succinctness, not power), while a construct that cannot be locally rewritten
increases expressive power and forces every program analysis to learn a new
case. Steele's "Growing a Language" (OOPSLA 1998) is the design consequence:
a language should be small enough to learn and let its *users* define new
words that look like primitives, so the language grows by library, not by
grammar; Racket's "languages as libraries" (PLDI 2011) is the engineering of
it.

Under Felleisen's criterion, GQ's planned extensions sort cleanly:

| Extension | Felleisen class | What it means for the planner |
|---|---|---|
| `exists { }` (today) | macro: `count { } > 0` | already lowered that way |
| Inlined views | macro: textual substitution of a pattern | nothing new to accept; one shape |
| Scalar functions, `coalesce`, `case` | conservative over the expression ladder once a generic call form exists | evaluator arms, `can_fail` |
| Projected block aggregates | conservative: the `SubqueryAggregate` operator exists, the value is its column | result schema inference |
| `collect` | new aggregate kind, same `Aggregate` node | Arrow list type |
| `optional { }` | **not** macro-expressible in GQ today (no outer join) | one new join kind; genuine power increase, bounded |
| Pattern union | **not** macro-expressible (UCQ > CQ) | a second component shape, or interface bindings as the bounded form |
| Unbounded recursion | **not** macro-expressible (FO < TC) | breaks the work budget; refused |

The design rule this gives is the one §7.5 implied: **admit extensions in
order of Felleisen class**: sugar and conservative extensions freely, each
genuine power increase through an RFC that names the new plan node and the
bound that keeps it tractable. Non-closure stays a property of the *kernel*;
expressiveness grows in the *library*.

### 7.7 How to make GQ extensible: the design

A concrete layering, from what exists today, with the acceptance test for
each tier. Everything below is additive to the current grammar; the one
breaking element (editions) is what makes future breaking changes survivable.

**Tier 0: the kernel, frozen in shape.** One declaration form; typed pattern
conjunction with stratified blocks; one expression ladder; one pipeline
(`match` → `return` → `order` → `limit`) with additive clauses; statements for
control; the `@` namespace for the engine; the six reserved words. Changes
here are RFC-only and bump the edition. The compiler's `QueryIR` is the kernel
language; everything in tiers 1–3 lowers to it, so plan acceptance, replay and
`explain` never learn a tier-1 or tier-2 construct.

**Tier 1: sugar, lowered in the compiler.** Any surface form that is
macro-expressible into the kernel: `exists`, a future `having { }` (a `Filter`
over `Aggregate`), map projection `$p { name, age }` (a struct of properties),
`optional { }` *once* the kernel has the join kind, `$b.@distance` (a column the
BFS already computes). Acceptance test: the lowering is a local rewrite and
the `.gqt` case for the sugar is the same case as for its expansion. No edition
bump; `GQ_LANGUAGE_VERSION` minor.

**Tier 2: registries, data not grammar.** Four registries, each already
partly built, each exposed as a `@` catalogue type so agents can read it:

1. *Functions.* Replace the per-function grammar rules (`search_call`,
   `bm25_call`, `agg_func`, `now_call`) with one `call = { ident ~ "(" ~ args
   ~ ")" }` resolved against a typed registry: name, parameter types with
   nullability, result type rule, kind (scalar | aggregate | predicate |
   ranking), `can_fail`, `volatile` (`now()`), pushdown capability (which
   Lance/DataFusion function it maps to, if any). The existing six search
   functions and five aggregates become rows; `@Function` lists them; `lint`
   resolves against the snapshot's registry; the planner's pushdown rule reads
   `can_fail`. A new function is a Rust implementation plus a row, never a
   grammar change, never a reserved word.
2. *Views (derived relations).* `view ContestedClaim($since: Date) { match …
   return { … } }` declared in `.gq` files beside queries, registered with the
   stored queries, typed by its `return` list as a node type is by its
   properties, bindable as `$c: ContestedClaim($since)`, inlined at compile
   time, non-recursive (a view may bind other views; a cycle is `T…`),
   validated at `cluster plan` like stored queries today (a schema change that
   invalidates a view is a pre-apply finding). `@View` lists them with their
   columns. This is the Datalog rule and the SQL view in one, and it is the
   place agent-authored logic lives under branches and review.
3. *Types.* Interface bindings (`$x: Named`) over the `interfaces` the catalog
   already holds, lowering to a multi-table scan with the common projection
   (the wildcard-traversal lowering, which already normalises common
   properties). Later: `Struct`/`Map` scalars, each an additive `base_type`
   and an Arrow type.
4. *Annotations.* A generic `@ident(args)` on `query`/`view` declarations,
   retained as metadata like unknown `.pg` annotations are, with engine-known
   names typed (`@timeout(ms)`, `@approximate`, `@cache(ttl)`). `@Annotation`
   lists the known ones. This is GraphQL's directive mechanism, and it gives
   every future knob a home before the knob is designed.

Acceptance test for tier 2: the kernel grammar is unchanged, the new thing is
a row in a registry the catalogue exposes, and `lint` can refuse a wrong use
from the registry alone. No edition bump.

**Tier 3: kernel changes.** New plan nodes with new bounds: `LeftExpand` for
optional, a partitioned top-k (`limit 3 per $c`) for top-N per group, a rerank
stage, pattern union if interface bindings prove insufficient. Each is an RFC
that names the node, its bound, its acceptance check and its `.gqt` cases, as
the typed-edge-alternation RFC did (budget, admission, replay). Minor edition
bump when additive, major when a produced shape changes.

**Editions.** `gq 2;` as an optional first settings line (absent means the
compiler's current edition); the stored-query registry records the edition
each stored source was validated under; the compiler compiles the current
edition and the previous one; `queries validate` reports sources on the old
edition with the migration; a graph is never quarantined for an edition it
still compiles. This turns the reserved-word kind of break from an outage into
a deprecation cycle. Precedents: Rust editions, Neo4j's `CYPHER 25` prefix
with a per-database default, GraphQL's additive-only rule.

**Governance, so the tiers hold.** Additive-only within an edition; every
removal deprecated one release first (the compatibility-surfaces RFC's
markers); the `.gqt` corpus is the executable contract and every tier-1 sugar
cites its expansion case; the skill's reference pages are generated from the
registries (the command index already is from the clap tree), so
documentation cannot drift from what the binary accepts; refusal codes are
counted in served metrics so the language's roadmap is read off which `T`
codes fire most.

**What stays out, and why.** User-defined functions executed in the engine
(WASM or otherwise) are the tempting shortcut and the one tier-2 item to
refuse for now: a view covers most "function" requests, an opaque function
breaks pushdown and plan truth (Froid's lesson: inline or lose the
optimizer), and executing agent-written code inside the write path needs a
policy story first. Revisit when views have been in use long enough to show
what they cannot express.

---

## 8. What the literature says about query surfaces for agents

Scope: peer-reviewed work and preprints from major labs, read on 2026-10-28,
on three questions. What does the agent workload look like? Which properties
of a query language measurably change how well an LLM uses it? What do agent
memory and retrieval systems ask of the engine, and with which metrics? Each
entry carries the finding, the metric it was measured with, and the
implication for GQ. The question the user put, "do we optimise for precision,
recall and throughput?", is answered in 8.6 from the evidence.

### 8.1 The agent workload: Berkeley's agent-first thesis

**Parameswaran, Agarwal, Akillioglu, Shankar, Zeighami, Iyer, Zaharia, Cheung,
Crooks, Gonzalez, Hellerstein, Stoica. "Intelligence is Free, Now What? Data
Systems for, of, and by Agents." BAIR blog, 7 July 2026.** A perspective from
the EPIC and DSF groups. Three claims: agents become the dominant workload and
behave unlike humans or applications ("agentic speculation"); agent swarms need
a substrate for shared state, coordination and failure, where "files are all
you need" and knowledge graphs both fall short, the latter "due to their lack of
structured search"; and agents will synthesise disposable, workload-specific
engines, so verification of a specification becomes the bottleneck. On shared
state: with thousands of agents trying transactions in response to one
request, "the effects of the vast majority of these transactions need to be
rolled back, with only the one 'correct' transaction's result persisting", and
the failure mode to avoid is a "livelock" of compensating actions.
*Implication for GQ:* the three things the post asks for that most systems lack
are typed, faceted retrieval over structured attributes; cheap fork, speculate,
roll back, merge; and a verifiable contract independent of implementation. GQ
has the first two (typed indexed properties; branches, `as commit`, `as
unattached branch`) and the third in the GQT corpus and plan acceptance. The
post's own structured-memory proposal (attributes with `*` wildcards and
natural-language corrective instructions) is a `.pg` schema with enums and a
`GraphPolicy`-style node, which the schema-design guide already recommends.

**Liu, Ponnapalli, Shankar, Zeighami, Zhu, Agarwal, Chen, Suwito, Yuan, Stoica,
Zaharia, Cheung, Crooks, Gonzalez, Parameswaran. "Supporting Our AI Overlords:
Redesigning Data Systems to be Agent-First." UC Berkeley. CIDR 2026;
arXiv:2509.00997.** The paper behind the blog's first claim, with
measurements. On BIRD with 50 parallel attempts per task, "the number of
distinct sub-plans of each size is often a small fraction of less than 10–20%
of the total". On 44 traces of o3 against two-backend tasks, actions phase
from metadata and sample exploration, to column statistics, to partial then
full solutions, with every phase present throughout; "proactively provided
grounding pertinent to the task" cuts the queries an agent issues by more
than 20% depending on phase. The design they propose: *probes* instead of
queries, each a batch of queries plus a natural-language *brief* stating goal,
phase, approximation needs and priorities; semantic-similarity operators over
*any* data or metadata ("impossible to address within SQL"); a proactive side
channel of "auxiliary information" (related tables, why-not explanations for
empty results) and "cost estimates … even before executing a query"; a probe
optimizer that *satisfices* rather than maximises throughput ("our goal is not
to optimize overall throughput as in traditional databases, but to evaluate
probes enough such that agents can make their decision on how to proceed in
the next turn"); an agentic memory store as a queryable semantic cache; and
*branched updates* with "multi-world isolation", citing Neon's observation
that agents "created 20× more branches, and performed 50× more rollbacks,
relative to humans", with forks that "must eventually reconcile, not just with
the mainline, but with each other". *Metrics used:* distinct versus total
sub-plans, steps to solution, task success rate under repeated attempts.
*Implication for GQ:* the strongest external statement of what GQ's workload
is. Four of its asks are already in the language or one step from it: schema
introspection (`@` catalogue types, 4.3 item 6), cost before execution
(`explain` with `validation` and statistics; a notification channel, 4.3 item
9), declared approximation (`recall`, `ann_nprobes`, the overfetch ladder) and
branched updates with three-way merge. Two are not: shared-work reuse across
the 80–90% redundant sub-plans (a planner concern the `logical_hash` makes
tractable), and a search operator over metadata and data at once (#648 and
interface bindings, 7.7). The paper also reframes the throughput question: the
objective is time-to-next-decision for the agent, not queries per second.

**Zeighami et al. "Arming Data Agents with Tribal Knowledge." UC Berkeley.
arXiv:2602.13521 (February 2026).** Tk-Boost indexes
"tribal knowledge", corrective instructions learned from an agent's own
mistakes on a database, with *applicability conditions* on query features, and
feeds it back during generation; +16.9 points on Spider 2.0 and +13.7 on BIRD
across NL2SQL agents. *Metric:* execution accuracy. *Implication for GQ:*
corrective knowledge keyed by schema element and operation is a graph of
`(type, property, operation) → instruction` edges; GQ's `@instruction` on
types and edges is the static half of this, and a `GraphPolicy`-shaped node
type is the learned half. The paper is also evidence that the gap between a
model's prior and a specific schema is closed by *schema-attached* text, which
favours keeping `@description`/`@instruction` in the schema over a separate
prompt.

### 8.2 How language design changes what an LLM gets right

**Benchmarks, and what moved the numbers.** Spider (Yu et al., Yale, EMNLP
2018) is saturated at about 91% execution accuracy; BIRD (Li et al., HKU and
Alibaba DAMO, NeurIPS 2023) dropped the same model family to the fifties by
adding 33 GB databases, dirty values and external evidence, and introduced the
Valid Efficiency Score so a correct slow query scores below a correct fast
one (human R-VES is now matched, correctness is not); Spider 2.0 (Lei et al.,
HKU XLang with Google and Salesforce, ICLR 2025) dropped the best agent to 21%
on real warehouses with 1,000-column schemas and multi-dialect workflows.
Spider 2.0's failure analysis: query planning 17.7%, **dialect-specific
function usage 10.3%**, schema linking 27.6% (columns 16.6%, tables 10.1%),
**JOIN errors 8.3%, often from missing foreign-key declarations**. The two
surveys (Liu et al., HKUST/Tsinghua/Renmin, arXiv:2408.05109; Hong et al.,
PolyU, IEEE TKDE 2025) fix the metric vocabulary (EX, EM, VES, query-variance
robustness) and a two-level error taxonomy (which clause; which cause).
*Implication for GQ:* one dialect, declared edges and explicit composition
remove two of the top five error classes outright; and a new language needs an
execution oracle and a clause-level error taxonomy from day one, which the GQT
corpus and the `Q`/`T`/`P` codes are.

**Execution feedback is the lever; feedback-free self-correction is not.**
DIN-SQL (Pourreza and Rafiei, Alberta, NeurIPS 2023): schema linking is the
largest error class, JOINs second; *feedback-free* self-correction adds 0.9
points for GPT-4 and a generic "buggy SQL / fixed SQL" prompt *hurts* it by
4.2. Self-Debug (Chen, Lin, Schärli, Zhou, Google DeepMind, ICLR 2024):
unit-test execution feedback 72.2 → 75.2 on MBPP, up to +12% on TransCoder,
matching baselines that sampled ten times more candidates. MAC-SQL (Wang et
al., Beihang and Tencent, COLING 2025): the Refiner that executes, catches
exceptions *and empty results*, and loops is the single most valuable
component (−4.6 points without it). Olausson et al. (MIT CSAIL and Microsoft
Research, ICLR 2024), the cleanest measurement of *feedback quality* as a
variable: holding the repairing model fixed, better feedback doubles repair
success (GPT-3.5 4.7% → 9.6% with GPT-4's diagnosis; CodeLlama 0.4% →
17.6%); human feedback lifts GPT-4 from 33.3% to 52.6%; GPT-4's own diagnosis
was wrong 32 times in 80. *Implication for GQ:* the bottleneck in repair is
diagnosis, not editing; every byte of precise, located diagnosis the engine
emits substitutes for a stronger model. GQ's four-field refusal is the right
shape, and MAC-SQL's result says an *empty result* should carry a reason too
(why-not provenance; the agent-first paper asks for the same).

**Language comparisons.** Text2Cypher (Ozsoy et al., Neo4j, GenAIK at COLING
2025): 44,387 pairs, fine-tuning gains shrink as the base model improves;
evaluated by Google-BLEU and exact match, not execution. SM3-Text-to-Query
(Sivasubramaniam et al., ZHAW, NeurIPS 2024), the only peer-reviewed head-to-
head of SQL, Cypher, SPARQL and MongoDB MQL on the same 408 question templates
and data: Llama3-70B zero-shot with schema SQL 47.1, Cypher 34.5, MQL 21.6,
SPARQL 1.0; with five BM25-retrieved worked examples (GPT-3.5) SQL 88.6,
Cypher 78.3, MQL 78.7, SPARQL 75.8. The schema in the prompt doubles zero-shot
accuracy for SQL, Cypher and MQL and barely moves SPARQL. SPINACH (Liu et al.,
Stanford, Findings of EMNLP 2024): on Wikidata's open schema the winning
interface is *exploratory and incremental* (look up, probe a neighbourhood,
run a fragment, refine), beating one-shot generation by 38 F1. Pipe syntax
(Shute et al., Google, VLDB 2024): the LLM benefit of linear composition is
stated as explicit *speculation*; the measured benefits are readability,
local edits and that every prefix of a pipe query is itself runnable.
*Implication for GQ:* the zero-shot prior of a well-known language is worth
10–45 points and **retrieved worked examples erase most of it**, so a young
language is viable if its tooling injects examples (the skill, the GQT corpus
as an example bank, a `@Example` annotation on types); SPARQL's floor shows
that *opaque identifiers*, not grammar, are what schema text cannot fix,
which favours GQ's human-readable keys and names; and partial queries and
schema introspection should be cheap and in-language.

**Schema and context.** CHESS (Talaei et al., Stanford and Alberta, 2024):
pruning takes column precision 0.11 → 0.90 at recall 0.94 and uses five times
fewer tokens, but execution-driven revision is worth more (−6.8 without it
versus −2.7 to −6.1 for the pruning stages). "The Death of Schema Linking?"
(Maamari et al., Distyl, NeurIPS 2024 TRL workshop): aggressive linking
forfeits up to 22.6% of questions; when the full typed schema fits the window,
frontier models link it themselves and score higher with the whole schema.
DTS-SQL (Pourreza and Rafiei, Findings of EMNLP 2024): for 7B models the
picture inverts, a smaller visible schema is worth 3–7 points. DAIL-SQL (Gao et
al., Alibaba, VLDB 2024): including foreign keys adds 0.6–2.9 points, a terse
output rule up to 3. Lost in the Middle (8.5) applies to a 300-column schema
dump as much as to documents. *Implication for GQ:* the design target is a
schema whose complete typed description *fits*, with explicit edges (which GQ
has) and a cheap typed way to fetch a sub-schema by type or neighbourhood
(the `@NodeType`/`@EdgeType` catalogue, 4.3 item 6) for smaller models and
larger graphs.

**Agents acting on data.** TAG (Biswal, Patel et al., Berkeley and Stanford,
CIDR 2025): on questions needing world knowledge or semantic reasoning,
Text2SQL scores 17%, RAG 0%, and pipelines with *semantic operators* 55%.
Semantic operators (Patel et al., Stanford and Berkeley, VLDB 2025):
`sem_filter`, `sem_join`, `sem_topk`, `sem_agg` with natural-language
parameters, optimised with statistical accuracy guarantees (proxy models plus
verification), up to 1,000× speedups. DocETL (Shankar et al., Berkeley and
Columbia, VLDB 2025): an agent-driven optimiser that rewrites a declared
pipeline and synthesises its own validation prompts; 25–80% more accurate than
hand-built baselines. Palimpzest (Liu et al., MIT CSAIL, CIDR 2025): a
declarative program compiled against a cost, time and *quality* policy, picking
cheaper physical operators for exploration and exact ones for the answer.
*Implication for GQ:* three ideas transfer directly. A query should be able to
carry a declared accuracy/cost policy (GQ has `recall` and `ann_nprobes` as
declared plan policy; the generalisation is a per-query `@approximate` or
`@budget` annotation, 7.7). Natural-language predicates belong in the *typed
plan* where they can be bounded, not in strings; GQ's text `nearest` is the
first such operator, and a `sem_filter`-style predicate over a text property
with a declared verifier is the natural second. And the engine, not the
caller, should be allowed to rewrite a query into a cheaper plan under a
stated target, which is what plan acceptance already permits for exact plans.

### 8.3 Theory and the database community's vision papers

**Expressive power.** Codd (1972, relational completeness: calculus equals
algebra, first-order logic as the yardstick); Aho and Ullman (POPL 1979):
relational algebra cannot express transitive closure, so unbounded
reachability can never be a sugar; Chandra and Harel (JCSS 1980, 1982; JLP
1985): the ladder CQ ⊂ UCQ ⊂ FO (non-recursive stratified Datalog) ⊂ FO+LFP
⊂ while ⊂ computable, with positive Datalog incomparable to FO; Vardi (STOC
1982) and Immerman (1986): data versus combined complexity, FO in AC⁰ data
complexity, FO+LFP equals PTIME on ordered structures; Grohe and Schweikardt
(LMCS 2005): *succinctness* as a distinct measure, equi-expressive formalisms
differing exponentially in shortest expression. For graphs: Angles et al. (ACM
CSUR 2017) separate *patterns* (CQs, NP-complete combined) from *navigation*
(RPQ, 2RPQ, CRPQ, UCRPQ; NLOGSPACE data complexity; path-output semantics can
be intractable even in data complexity); Libkin, Martens and Vrgoč (JACM 2016,
the 2026 Alonzo Church Award) formalise data paths and show cross-position
value comparison is the expensive extension; Francis et al. (SIGMOD 2018) give
Cypher a denotational semantics as a pipeline of binding tables; Deutsch et al.
(SIGMOD 2022) present the pattern kernel GQL and SQL/PGQ share, with
restrictors (`TRAIL`, `ACYCLIC`, `SIMPLE`) and selectors (`ANY SHORTEST`, `ALL
SHORTEST`) that make infinite path sets finite; the GQL digest (ICDT 2023)
lists the open expressiveness questions path values create; GPC (PODS 2023)
types pattern variables as singleton, group or conditional so repetition feeds
aggregation coherently. *Implication for GQ:* the two instruments are
expressiveness (rung on the ladder) and succinctness (how much shorter);
GQ's `{m,n}`, implicit grouping and top-k are succinctness gains over their
first-order expansions, not new power, and that is the right thing to say in
their defence. If repetition ever becomes unbounded, GPC's typing and
GQL's restrictor/selector vocabulary are the published blueprint.

**Expressiveness and extensibility in programming languages.** Felleisen
(Science of Computer Programming, 1991): macro-expressibility and conservative extension,
the formal test 7.6 applies; Steele (OOPSLA 1998 keynote, HOSC 1999): design
to grow, users add vocabulary, designers promote proven idioms; Tobin-Hochstadt
et al., "Languages as Libraries" (PLDI 2011): whole languages, Typed Racket
included, as compile-time expansions into a small core with access to binding
and type information; Pombrio and Krishnamurthi, "Resugaring" (PLDI 2014):
desugarings must be compositional and position-preserving so evaluation, and
by extension errors and plans, read back in surface terms. *Implication for
GQ:* the tier-1 sugar layer of 7.7 should be a schema-aware expander whose
desugarings keep source positions, so `explain` rows and `T` refusals name the
construct the author wrote.

**Extensibility in database systems.** Froid (Ramachandra et al., PVLDB 2017):
inlining imperative UDFs into relational expressions turned row-at-a-time
callbacks into optimisable plans with speedups of orders of magnitude; the
lesson is that an extension executed as a callback is a performance cliff by
construction. Hirn and Grust (SIGMOD 2021), Duta et al. (CIDR 2020): compiling
PL/SQL into recursive CTEs works only because the kernel has recursion, so a
kernel without recursion cannot absorb iterative extensions by compilation.
LogicBlox (Aref et al., SIGMOD 2015) and Soufflé (CAV 2016): typed Datalog as
the single language for schema, constraints, queries and views, with derived
predicates inlined into worst-case-optimal joins, the best-evidenced
extensibility mechanism in the field. Volcano and Cascades (Graefe, 1993,
1995): extensibility lives in the rule set over typed operators; a new
operator with rules and properties joins the search space on equal terms.
*Implication for GQ:* inlined typed views and registry functions (7.7 tier 2)
are the Froid and LogicBlox shape; semantic filters, approximate top-k and a
rerank stage should enter as typed logical operators with rules and
properties, never as string rewrites or external callbacks; and the one thing
a non-recursive kernel cannot absorb is iteration, which is why unbounded
recursion is a kernel decision, not a library one.

**Vision papers.** Stonebraker and Pavlo, "What Goes Around Comes Around…
And Around…" (SIGMOD Record 2024): no new data model displaced the relational
one; graph systems' ideas are being absorbed through SQL/PGQ (DuckDB's PGQ
beats a leading graph DBMS by up to 10×) and they "do not expect specialized
graph DBMSs to be a large market"; a new language's lasting value is the
features it exports. Pan, Wang and Li, "Survey of Vector DBMSs" (VLDBJ 2024):
open problems are the semantics and cost of hybrid filter-plus-similarity
queries, approximation without declared accuracy, and the absence of a
composable algebra for ranking operators. Madden, Cafarella, Franklin and
Kraska, "Databases Unbound" (PVLDB 2024): natural language is a front end and
a structured declarative language is the optimizer's intermediate
representation; propose `convert` and `enrich` operators and a multi-objective
optimizer over cost, latency and quality. Castro Fernandez, Elmore, Franklin,
Krishnan and Tan, "How LLMs Will Disrupt Data Management" (PVLDB 2023):
design pressure moves from human ergonomics to *verifiability*; expose
schemas, constraints and provenance in machine-consumable form. Eckmann and
Binnig, "A Vision for Autonomous Data Agent Collaboration" (CIDR 2026):
per-source agents and a collaboration protocol replacing a central planner.
None of these argues for natural language as the sole interface; all put an
optimisable declarative layer beneath it. *Implication for GQ:* three
consequences. GQ's durable asset is a kernel of typed operators that would
survive translation into a SQL/PGQ host, so each feature should be stated as
an operator a host could absorb. The vector survey's three open problems are
exactly the contracts GQ already states (prefilter semantics, declared
approximation) plus the one it lacks (a composable ranking algebra, 4.3 item
1). And verifiability, the property the 2023 paper predicts matters most, is
where GQ's static types, compile-time refusals and plan acceptance are ahead
of every comparator.

### 8.4 Agent memory systems: what they ask of the engine

Every entry was confirmed against its arXiv or publisher record; numbers are
the papers' own.

**Packer et al., "MemGPT: Towards LLMs as Operating Systems", UC Berkeley,
arXiv:2310.08560 (2023).** Two-tier memory (working context plus recall and
archival stores on PostgreSQL and pgvector); the agent's whole interface is a
handful of typed calls (append, replace, search, insert) with paging. Deep
Memory Retrieval accuracy 92.5% versus 32.1% for fixed-context GPT-4.
*Implication:* the memory API is a few keyed reads and writes plus a vector
index, issued as sequential round trips, so per-call latency and paging shape
task time more than any single-query recall figure.

**Sumers, Yao, Narasimhan, Griffiths, "Cognitive Architectures for Language
Agents" (CoALA), Princeton, TMLR 2024, arXiv:2309.02427.** Episodic, semantic
and procedural long-term stores; retrieval already means a *fused* score over
recency, importance and similarity (Generative Agents' scoring), not one ranked
list. *Implication:* an engine serving agents needs several memory kinds with
different access patterns and a fusion that can take time and salience as
inputs, which is an argument for scores as values (4.3 item 1).

**Rasmussen et al., "Zep: A Temporal Knowledge Graph Architecture for Agent
Memory", arXiv:2501.13956 (2025).** Graphiti: episodes, entities with fact
edges, communities; facts are **bi-temporal** (`t_valid`/`t_invalid` on the
event line, created/expired on the ingestion line); a contradicting fact
*invalidates* the old edge; search is cosine plus BM25 plus bounded
breadth-first expansion, reranked by RRF, MMR or a cross-encoder. LongMemEval
with gpt-4o: full context 60.2% at 28.9 s median and about 115k tokens; Zep
71.2% at 2.58 s and about 1.6k tokens. *Implication:* the closest published
shape to GQ (typed graph, vector, BM25, RRF, time). Its differentiating
operation is invalidation, not retrieval, and its win was 10× latency and 70×
fewer tokens, not recall.

**Chhikara et al., "Mem0: Building Production-Ready AI Agents with Scalable
Long-Term Memory", arXiv:2504.19413 (2025).** A vector store of extracted
facts, and Mem0^g, a labelled graph in Neo4j; the write is retrieve-top-10 then
one of ADD, UPDATE, DELETE, NOOP; obsolete relations are marked invalid. LoCoMo
LLM-judge: full context 72.9, Mem0^g 68.4, Mem0 66.9, Zep 66.0, best RAG 61.0;
p95 latency 1.4 s (Mem0), 2.6 s (graph), 17.1 s (full context); retrieved
context about 1.8k tokens versus 26k. *Implication:* the dominant write is a
read-then-decide keyed upsert; the graph variant buys two points for 1.5–2×
latency; the quality headroom is small next to the latency and token gap.

**Xu et al., "A-Mem: Agentic Memory for LLM Agents", Rutgers, NeurIPS 2025,
arXiv:2502.12110.** Zettelkasten notes whose neighbours' derived attributes
are rewritten in place when a new note arrives; retrieval is plain top-k
cosine; 85–93% fewer tokens per question than MemGPT. *Implication:* structure
is exploited at write time; the engine needs cheap in-place property update
with versioning more than richer read queries.

**Lin et al., "Sleep-time Compute", Letta / UC Berkeley, arXiv:2504.13171
(2025).** Between turns the agent rewrites its memory (`rethink_memory`); about
5× less test-time compute for equal accuracy. *Implication:* heavy background
rewrite traffic interleaved with latency-sensitive reads; isolating in-progress
consolidation from readers is a first-class need, which is what a branch or an
unattached snapshot is.

**Zhang et al., "A Survey on the Memory Mechanism of LLM-based Agents",
Renmin / Huawei, arXiv:2404.13501 (2024; TOIS acceptance stated by the
authors).** Three write-side operations (write, manage, forget) for every read;
evaluation "an open problem". *Implication:* benchmark coverage of the write
side is essentially absent (8.5).

### 8.5 Graph retrieval, fusion, filtered ANN, and the benchmarks

**Edge et al., "From Local to Global: A Graph RAG Approach to Query-Focused
Summarization", Microsoft Research, arXiv:2404.16130.** Leiden communities with
LLM summaries; 72–83% win rates over vector RAG on comprehensiveness and
diversity for *global* questions, none on directness; indexing MuSiQue cost
115.5M input tokens (HippoRAG 2's measurement). *Implication:* the win is a
materialised-view workload (expensive LLM indexing, cheap root-level reads),
not a traversal problem.

**Gutiérrez et al., "HippoRAG", Ohio State / Stanford, NeurIPS 2024,
arXiv:2405.14831; "HippoRAG 2 / From RAG to Memory", ICML 2025,
arXiv:2502.14802.** Personalized PageRank over an extracted graph seeded at
query entities. Recall@5 on 2WikiMultiHopQA 68.2 → 89.1 versus ColBERTv2, but
*worse* on HotpotQA (79.3 → 77.7). HippoRAG 2, the most careful graph-versus-
vector measurement: average F1 across seven datasets 59.8 versus NV-Embed-v2
57.0, GraphRAG 49.6, LightRAG 6.6; the only graph method matching dense
retrieval on simple QA while leading multi-hop; indexing 9.2M input tokens
versus GraphRAG's 115.5M. *Implication:* graph structure must *augment* a
dense or lexical ranking, not replace the corpus; the winning design is
passage nodes carrying the embedding with edges carrying associations, which
is a property graph with per-node vectors, and a bounded walk should be a
ranking primitive the planner can choose, not the default path.

**Xiang et al., "When to use Graphs in RAG" (GraphRAG-Bench), ICLR 2026,
arXiv:2506.05690.** Vanilla RAG with rerank leads on fact retrieval (60.9
versus LightRAG 58.6, MS-GraphRAG 49.3); HippoRAG 2 leads complex reasoning
(53.4 versus 42.9); prompt tokens per query: vanilla ~879, HippoRAG 2 ~1,008,
LightRAG ~101k, MS-GraphRAG global ~331k. *Implication:* traversal pays only
when the question spans concepts; the token cost of the result set is a
first-order metric.

**Pan et al., "Unifying LLMs and Knowledge Graphs: A Roadmap", IEEE TKDE 2024,
arXiv:2306.08302.** KGs valued for explicit, editable, interpretable structure
and provenance. *Implication:* typed schema, keyed identity and lineage over
raw traversal speed.

**Cormack, Clarke, Büttcher, "Reciprocal Rank Fusion…", SIGIR 2009.** `k = 60`
was fixed "during a pilot investigation" on TREC runs, never tuned for
lexical-plus-dense fusion. **Bruch, Gai, Ingber, "An Analysis of Fusion
Functions for Hybrid Retrieval", ACM TOIS 42(1) 2023, arXiv:2210.11934.** A
convex combination of min-max-normalised scores beats RRF on every dataset
tried (MS MARCO nDCG@1000 0.454 versus 0.425; NQ 0.542 versus 0.514) and is
"largely agnostic" to the normalisation; RRF is parameter-sensitive (tuning
`k` gains on one dataset and loses on another); one tuned weight needs only a
handful of labelled queries. **Thakur et al., "BEIR", NeurIPS 2021.** Zero-shot,
BM25 beats most dense retrievers on about half of 18 datasets; BM25 plus a
cross-encoder reranker is the strongest configuration (+11%). *Implication:*
keep `rrf` as the zero-config default and expose weighted, normalised fusion
and a rerank stage; the 2–3 nDCG points on the table are larger than most
graph-versus-vector deltas, and they are exactly what 4.3 item 1 unlocks.

**Patel, Kraft, Guestrin, Zaharia, "ACORN", Stanford, SIGMOD 2024,
arXiv:2403.04871.** Pre-filtering is competitive at low selectivity but scales
poorly; post-filtering "failed to reach high recall" on high-cardinality
predicates; traversing the predicate-induced subgraph of a denser HNSW gives
2–50× QPS at recall 0.9. **Gollapudi et al., "Filtered-DiskANN", Microsoft,
WWW 2023.** Label-aware index construction for low-cardinality labels; post-
filtering collapses at low specificity. **Simhadri et al., "Results of the
Big-ANN NeurIPS'23 Competition", arXiv:2409.17424.** The filtered track fixes
recall@10 ≥ 0.9 and ranks on QPS; the winner branches its strategy on tag
cardinality. *Implication:* the recall of a filtered vector query depends on
selectivity and on its correlation with the embedding space; the engine must
choose pre-, post- or in-graph filtering from cardinality estimates and report
the recall it achieved. GQ's prefilter gate, overfetch ladder and `recall:
approximate` descriptor are this contract; node type is the natural
low-cardinality label for a label-aware index.

**Benchmarks.** **LoCoMo** (Maharana et al., ACL 2024): the best configuration
retrieved five dense LLM-extracted observations, beating 25 raw turns; no
scoring of writes or contradictions; scores are now reported under
incompatible metrics (F1 versus LLM-judge). **LongMemEval** (Wu et al., ICLR
2025): GPT-4o drops from 0.870 with oracle evidence to 0.606 with the full
115k history; the only benchmark scoring *knowledge updates* and *abstention*;
fact-augmented keys +9.4% recall@k, time-aware query expansion +6.8–11.3%
temporal recall. **RAGAS** (EACL 2024) and **ARES** (NAACL 2024): the deployed
metric is *context relevance*, the fraction of retrieved content that was
needed, a precision metric. **BEIR/MTEB**: nDCG@10 for ranked quality,
recall@100 for first-stage coverage; nothing measures freshness, update
visibility or consistency under concurrent writes.

**Context precision.** **Liu et al., "Lost in the Middle", TACL 2024.** With 20
retrieved documents GPT-3.5 scores 75.8% with the answer first, 53.8% in the
middle (below the 56.1% closed-book score), 63.2% last; reader accuracy
saturates near 20 documents while retriever recall keeps rising. **Hsieh et
al., "RULER", NVIDIA, COLM 2024.** Distractors, not length, drive degradation.
**Hong, Troynikov, Huber, "Context Rot", Chroma technical report, 2025.**
Across 18 models a focused ~300-token prompt beats the full 113k-token
LongMemEval history. **Anthropic, "Introducing Contextual Retrieval", 2024.**
Enriching the indexed key cut top-20 retrieval failures from 5.7% to 2.9% with
contextual BM25 and 1.9% with a reranker. *Implication:* beyond roughly 20
items more recall is worthless to the reader; the engineering target is the
smallest, most relevant context, in a deliberate order.

### 8.6 So what should the engine optimise? Precision, recall, throughput, revisited

The three named objectives are necessary and misordered, and the literature
adds two the three leave out. In evidence order:

1. **Precision at a small k, and the token cost of the result set.** Readers
   saturate near 20 items and lose up to 22 points when the needed fact sits
   mid-context (Lost in the Middle); distractors, not length, drive the drop
   (RULER, Context Rot); LoCoMo's best system retrieved five dense
   observations rather than 25 turns; the deployed RAG metric (RAGAS, ARES) is
   *context relevance*, a precision measure; Zep's headline gain was 70× fewer
   tokens and 11 points, Mem0's and A-Mem's were 85–93% token reductions.
   Recall beyond what the reader will use is a cost. *For GQ:* fused top-k
   precision with typed and temporal predicates is the primary quality metric,
   and "tokens returned per correct answer" is the headline number; flat typed
   rows that the caller hydrates are the right default, which is what GQ has.
2. **Diagnosis quality, because it decides the repair loop.** Feedback-free
   self-correction is worth under one point for a frontier model and can be
   negative (DIN-SQL); execution feedback is worth +4 to +12 (Self-Debug,
   MAC-SQL, CHESS); holding the model fixed, *better diagnosis alone* doubles
   repair success (Olausson et al.). An empty result deserves a reason
   (MAC-SQL loops on it; the agent-first paper asks for why-not provenance).
   *For GQ:* the four-field refusal is a quality metric to measure (turns to a
   valid query; share of refusals carrying a machine-applicable edit), and the
   next two items on that axis are why-not answers for empty results and the
   planner notification channel.
3. **Round trips per task, measured as time to the agent's next decision.**
   Agent memory is sequences of small calls (MemGPT paging, Mem0's
   retrieve-then-upsert, CoALA's loop); HippoRAG matched iterative retrieval at
   6–13× lower latency by collapsing hops into one query; grounding hints cut
   queries per task 18%; repeated attempts raise success 14–70% and 80–90% of
   their sub-plans are duplicates. The agent-first paper's objective is "to
   evaluate probes enough such that agents can make their decision on how to
   proceed in the next turn", not queries per second. *For GQ:* every construct
   in §4.3 that removes a round trip (blocks as values, optional, views,
   HAVING, collect, computed writes) is a throughput feature in this sense;
   shared-sub-plan reuse via `logical_hash` and a cost estimate before
   execution are the engine-side halves.
4. **Write correctness, temporal validity and isolation under speculation,
   which no benchmark measures yet.** Every production memory system
   invalidates rather than deletes and resolves contradictions at write time
   (Zep, Mem0, A-Mem); LongMemEval is the only benchmark scoring knowledge
   updates and abstention, and it is where systems fail; background
   consolidation interleaves with reads (sleep-time compute); agents make 20×
   the branches and 50× the rollbacks of humans and "the vast majority" of
   their transactions must be rolled back. *For GQ:* keyed upsert, `--if-
   commit`, branches, `as unattached branch`, three-way merge and snapshot
   reads are the differentiators the literature asks for and cannot score;
   add the metrics it lacks: update visibility latency, staleness of returned
   facts, merge conflict rate, wasted-speculation cost.
5. **Recall at k as a declared bound, not a maximum, above all under
   filters.** Filtered ANN recall collapses under naive post-filtering at low
   selectivity (ACORN, Filtered-DiskANN); the Big-ANN contract fixes recall@10
   ≥ 0.9 and then ranks on throughput; the winner picks its strategy from tag
   cardinality. *For GQ:* the prefilter gate, overfetch ladder and `recall:
   approximate` descriptor are already this contract; the missing piece is
   reporting the recall *achieved* on a filtered `nearest`, and using node
   type as the label for a label-aware index.
6. **Fusion quality over fusion default.** Hybrid beats either index alone
   everywhere; zero-shot BM25 beats most dense retrievers on half of BEIR;
   weighted, normalised fusion beats RRF by 2–3 nDCG points and RRF's `k = 60`
   was fixed in a 2009 pilot; reranking adds the largest marginal gain
   (BEIR +11%, Anthropic's contextual retrieval). *For GQ:* keep `rrf` as the
   zero-config path and expose weighted fusion and a rerank stage, which is
   what moving retrieval out of `order` (4.3 item 1) makes an expression.
7. **Graph traversal as a planner choice, not a default.** PPR wins on
   associative multi-hop (+21 recall@5 on 2WikiMultiHopQA) and loses on single-
   passage QA; community summaries win only on global questions and are a
   materialised-view workload; the cheapest graph method costs 15% more tokens
   than vector RAG, the others 100–400×. *For GQ:* "scope by traversal, then
   rank inside the scope" (the schema-design guide's rule) is the shape the
   measurements support; a bounded PPR-style ranking would be a ranking
   function in the registry, not a default path.
8. **Latency and raw throughput last among the quality metrics, but a floor.**
   Published wins sit at p95 under about 3 s for memory reads and 10× over
   full-context; past that floor further throughput buys little, while each
   item above buys measured accuracy. Under agentic speculation the throughput
   that matters is *shared*: 80–90% of sub-plans are redundant, so reuse beats
   raw queries per second.

The direct answer: **precision, yes, at small k and measured as context
relevance and tokens per correct answer; recall, as a bounded guarantee the
engine reports rather than a number to maximise; throughput, redefined as
round trips and time-to-next-decision with shared-work reuse, with raw latency
as a floor.** The two missing objectives are diagnosis quality and write
correctness under concurrent speculation, and both are places where GQ is
ahead of the comparators and where the literature has no benchmark, which is
an opportunity to publish one. The proposed metric set for GQ, in the order
above: tokens returned per correct answer; nDCG@10 and recall@k on the fused
ranking (comparable with BEIR and MTEB); turns to a valid query and share of
refusals with a machine-applicable fix; round trips per task on an agent
benchmark (LoCoMo and LongMemEval as read-path regression suites, with the
caveat that their scores are not comparable across papers); declared versus
achieved recall under filters; update visibility latency and staleness; merge
conflict and wasted-speculation rates; p95 latency; shared-sub-plan hit rate.

---

## 9. GQ redesigned without compatibility

The question: if nothing stored had to keep parsing, what would GQ be, derived
from the workload (§8) and the axes (§2) rather than from the sequence of RFCs
that produced today's grammar? The answer keeps more than it changes. Every
contract in §4.2 survives; what changes is the shape the contracts are
delivered in. Working name below: **GQ 3**. Each decision cites the evidence
that drove it.

### 9.1 The requirements, from the evidence

| # | Requirement | Evidence |
|---|---|---|
| R1 | Closed, typed schema the agent can read, that fits in context while small and is queried through the language (bound, traversed, searched, counted) once it does not; edges declared | SM3 (schema doubles zero-shot accuracy), DAIL (explicit FKs +0.6–2.9), Spider 2.0 (8.3% JOIN failures from missing FKs), Death of Schema Linking (full schema wins when it fits) |
| R2 | Diagnosis as a product: located, typed refusals with fixes; reasons for empty results; warnings | Olausson (feedback quality doubles repair), MAC-SQL (empty result is a signal), Self-Debug, GQ's own 45/45 measurement |
| R3 | Fewer round trips: one query for one question; batches under one snapshot; shared work | agent-first paper (12.7 queries per task, 80–90% duplicate sub-plans), HippoRAG (one-shot PPR at 6–13× lower latency), #1050 (92 requests per node page) |
| R4 | Small, precise results with their provenance; token cost is a quality variable | Lost in the Middle, RULER, Context Rot, Zep (70× fewer tokens), LoCoMo best config, RAGAS context relevance |
| R5 | Retrieval as an algebra: scoped by graph, fused with weights, thresholded, reranked, with declared and reported recall | Bruch (weighted fusion +2–3 nDCG over RRF), BEIR (rerank +11%), ACORN and Big-ANN (recall as a bound under filters), Vespa's match/rank split |
| R6 | Writes that invalidate rather than delete, upsert by key, compute under a precondition, and select by pattern | Zep, Mem0, A-Mem (invalidation; retrieve-then-upsert), LongMemEval (updates are where systems fail), GQ's own 2-round-trip table in 2.11 |
| R7 | Speculation as a first-class mode: fork, propose, roll back, merge, with isolation of background rewrites | agent-first paper (20× branches, 50× rollbacks, "roll back all but one"), sleep-time compute |
| R8 | Declared cost and accuracy per query; explicit, never silent, partial answers | Palimpzest, LOTUS, agent-first "satisfice", invariant 11 |
| R9 | Growth by declaration, not grammar: views, functions, interfaces, annotations; editions for the rare break | Felleisen, Steele, LogicBlox, Froid; Elasticsearch's second language as the counterexample |
| R10 | One language for schema, derived relations, reads, writes, control and introspection | RFC 0055's wider direction, Berkeley's "holistic source of truth", Datomic's schema-as-data |

### 9.2 What stays exactly as it is

Static closed typing with nullable parameters; three-valued logic with no
null literal in reads; pattern conjunction by juxtaposition with join by
variable; stratified blocks with the safety rule; reachability semantics for
bounded traversal; filter-before-rank, total order with id tiebreaks, index
required at compile time, approximation declared in plan and envelope;
keyed insert as upsert; one commit per mutation; D2 (constructive or
destructive per statement list: with invalidation as the write pattern,
deletes become rare and the two-case staging protocol stays worth its
simplicity); branches, proposals and snapshots as statements; the `@`
namespace; the four-field diagnostic with frozen codes; `$var`, braces,
lowercase keywords, word-bounded identifiers; flat Arrow rows as the
canonical result.

### 9.3 What changes, and why

| Today | GQ 3 | Driver |
|---|---|---|
| `query` for reads and writes alike | `query` for reads, `mutation` for writes, `view` for derived relations, `batch` for several under one snapshot | agents trained on GraphQL write `mutation { }` (the skill's documented trap); lint classifies by keyword, not body |
| `$p worksAt $c`, direction from the schema | `$p -worksAt-> $c`, `$c <-worksAt- $p`, `$a -knows- $b` undirected, `-$e:worksAt->` bound, `-(knows|likes){1,3}->` | direction visible to the reviewer and the model; `T5` becomes a reading error, not a schema lookup. **Withdrawn 2026-10-30 (§13.3): the triple stays; the arrowhead carries nothing position and schema do not** |
| Ranking functions as the leading `order` key | a `retrieve { }` clause that binds typed score columns; `order` sorts any expression | 2.10; Vespa; Bruch; thresholds, weights, more arms and rerank become expressions |
| `rrf(a, b)` only | `rrf(...)`, `norm(...)`, weighted sums, `rerank(model, text) k` as a stage, `where $a.vec.distance < 0.4` | Bruch (+2–3 nDCG), BEIR (+11% rerank), LanceDB `distance_range` |
| Blocks as predicates only, constant right side | blocks as values (`count { } as n`), any expression on the right | #1050, Cypher `COUNT {}`, Soufflé |
| No optional | `optional { }` with pattern-bound filters and nullable bindings | #658, GQL `OPTIONAL MATCH` |
| No post-group filter, no collection | `having { }`, `collect($x)` / `collect($x { a, b })` | R3 Rosetta row; Datomic pull; one nesting level |
| Functions are grammar rules | generic `name(args)` over a typed registry; `@Function` catalogue | 7.7 tier 2; Froid |
| No derived relations | `view Name($p: T) { match … return … }`, bindable as `$x: Name($p)`, inlined | 2.13; LogicBlox; convergence |
| No polymorphism | `$x: Named` over interfaces; `$x: *` over every node type with the common projection | #648; the catalog's reserved `interfaces` |
| Constant assignments only | computed assignments (`visits + 1`) allowed under an explicit precondition | 2.11; Mem0's retrieve-then-upsert |
| `where` over own properties only | pattern-scoped selection: `delete $e where { pattern }`, `update $a … where { pattern }`, bounded pattern-scoped `insert` | 2.11; Cypher `MATCH … DELETE` without the free mixing |
| Counts only on success | `returning { … }` ids and projected values | SQL `RETURNING`; one round trip saved per write |
| `limit N` literal; no paging | `limit $k`; continuation token in the envelope; no `offset` | 2.9; Relay; ES `search_after` |
| `--branch`/`--snapshot` outside the text | `at $commit` / `on $branch` typed parameters on a declaration | 2.12; Datomic's database-as-value |
| No cost or accuracy policy in the text | `@budget(rows, ms)`, `@approximate(recall)`, `@satisfice` annotations; refusal with estimate before running; `complete: false` with reason, never silent | R8; Palimpzest; agent-first |
| Empty result is empty | `why_not` funnel in the envelope (rows surviving each clause) | MAC-SQL; agent-first why-not provenance |
| Errors only | `N…` notifications beside `Q/T/P` refusals | Neo4j 03N90/03N91; agent-first "steer" |
| Schema not queryable | `@Type`, `@Property`, `@Edge`, `@Index`, `@Statistic`, `@Function`, `@View` beside `@Operator` | 2.16; GraphQL `__schema`; Datomic |
| `@description`/`@instruction` only | plus `@example` on types and views, served back as worked examples | SM3: five retrieved examples erase a 10–45 point prior |
| No temporal roles | `@valid(from, to)` on a type declares bitemporal validity; `invalidate` as a write verb; `at date(…)` on a binding | Zep; LongMemEval; the schema-design guide's axes |
| One grammar at a time | `gq 3;` edition line; compiler accepts N and N-1; registry records edition | 7.7; Rust editions; Neo4j `CYPHER 25` |
| Six hardcoded search functions, `.gq` and `.pg` separate | one file type holding `node`, `edge`, `interface`, `view`, `query`, `mutation`, `policy` declarations | R10 |

### 9.4 The shape, in one file

```gq
gq 3;

// ---- schema: the same declarations, one file type
interface Named @instruction("Anything a human refers to by name") { name: String }

node Customer implements Named {
  slug: String @key
  tier: enum(standard, strategic) @index
  @example("$c: Customer { tier: strategic }")
}

node Assertion @valid(asserted_at, retracted_at)
  @instruction("One actor's claim from one source. Never edit the statement; supersede it.") {
  slug: String @key                     // span + extractor + statement digest
  statement: String @index              // full-text
  embedding: Vector(1536) @embed("statement") @index
  asserted_at: DateTime
  retracted_at: DateTime?
  confidence: F64? @range(confidence, 0..1)
}

edge About: Assertion -> Customer @key(@src, @dst)
edge Supports: Assertion -> Assertion
edge Supersedes: Assertion -> Assertion @card(0..1)

// ---- a derived relation, declared once, bound anywhere
view Contested($since: DateTime) @description("Assertions attacked since a date") {
  match {
    $a: Assertion
    $r: Assertion -supports-> $a        // reuse: a 'Reason' would be its own type in a real schema
    $r.confidence < 0.3  $r.asserted_at >= $since
  }
  return { $a as assertion, count($r) as attacks }
}

// ---- a read: scope by graph, retrieve, fuse, filter on scores, project compactly
query evidence($slug: String, $q: String, $k: I64 = 10, $at: Commit?)
  @approximate(recall: 0.9) @budget(rows: 200000, ms: 2000) {
  match {
    $c: Customer { slug: $slug }
    $a: Assertion -about-> $c
    not { $a -supersedes-> $_ }                       // only current claims
    optional { $x: Contested(datetime("2026-01-01")) { assertion: $a } }
  }
  retrieve {
    $a.vec: nearest($a.embedding, $q) k 200
    $a.lex: bm25($a.statement, $q)
  }
  return {
    $a { slug, statement, asserted_at },
    $x.attacks,
    count { $s -supports-> $a } as support,
    0.7 * norm($a.vec) + 0.3 * norm($a.lex) as score
  }
  having { $a.vec.distance < 0.6 or $a.lex is not null }
  order { score desc }
  limit $k
}

// ---- a write: pattern-scoped, keyed, computed under a precondition, answering ids
mutation retract($slug: String, $why: String, $head: Commit) @if_commit($head) {
  update $a: Assertion where { $a: Assertion { slug: $slug } }
    set { retracted_at: now(), note: $why }
  invalidate $e where { $a: Assertion { slug: $slug } -$e:supports-> $_ }
  returning { $a.@id, count($e) as detached }
}

// ---- speculation: the same statements as today, plus a batch under one snapshot
batch explore @brief("which assertion types mention pricing") @phase(explore) {
  query types() { match { $t: @Type } return { $t.name, $t.instruction } }
  query hits($q: String) @satisfice(ms: 300) {
    match { $a: Assertion }
    retrieve { $a.lex: bm25($a.statement, $q) }
    return { $a.slug, $a.lex.score as s } order { s desc } limit 20
  }
}
```

Reading the file against the requirements: R1 is the schema with `@example`
and `@instruction` served through `@Type`; R2 is the diagnostic, `why_not` and
`N…` channel the envelope carries; R3 is `optional`, the block as a value,
`having`, the view and the batch, which turn the five-query node page into one
request; R4 is `$a { slug, statement, asserted_at }` (a partial object), typed
score columns and `limit $k` with a token; R5 is `retrieve` plus the score
expression and the `having` threshold under a declared recall; R6 is
`invalidate`, the computed `set` under `@if_commit`, the pattern-scoped
`update` and `returning`; R7 is the unchanged branch, proposal and snapshot
statements plus `batch`; R8 is `@budget`, `@approximate`, `@satisfice`; R9 is
`view`, the function registry behind `norm`, `nearest`, `bm25`, and the `gq 3;`
line; R10 is the one file.

### 9.5 The retrieval clause, precisely

`retrieve { $b.name: fn(args) [k N] … }` declares one or more *arms*, each
over one binding's indexed property, each binding a typed score column
(`$a.vec.distance: F64?`, `$a.lex.score: F64?`, null when the row is outside
that arm's window). Semantics, unchanged in substance from today's contracts:
the `match` block's filters and traversals are applied before any arm scores
(prefilter); a `nearest` arm is a top-`k` window over the eligible rows with
the declared recall, a `bm25` arm is the full matching population; the
retrieved population is the union of the arms' rows; `return`, `having` and
`order` may use the score columns as ordinary values, so `norm`, `rrf`, a
weighted sum and a threshold are expressions, not grammar; `rerank(model,
$a.statement, k 50)` is a stage declared after the arms, run over the fused
top-k with a provider named in `cluster.yaml` the way embedders are; a
retrieval forces a `limit`. The envelope reports per arm the recall contract
and, under filters, the recall achieved, and per row the arm contributions
behind a fused score. A ranking that scopes by traversal (`$a -about-> $c`
above) ranks only the reached rows, as today.

### 9.6 Writes, precisely

Three selection forms, one target type each, so the effect set stays visible:
`where expr` over the target's own properties (today's form); `where { pattern
}` binding the target and reaching it through edges; and for `insert`, a
bounded pattern that supplies endpoint values. Assignments admit expressions
over the target's current properties (`visits: visits + 1`) only when the
declaration carries `@if_commit` or the statement names a key equality, so a
computed write always has a stated read-set. `invalidate` is `update` that
sets the type's declared `@valid` end to `now()`; it exists because every
production memory system (Zep, Mem0, A-Mem) invalidates rather than deletes,
and because a graph that is also a history should not lose the fact that a
fact was once held. `delete` stays for genuine removal under D2. `returning`
projects over the affected rows after the write, inside the same commit.
Everything else about writes is today's: one commit, constants per invocation
for literals and parameters, `as commit`, `as unattached branch`.

### 9.7 The envelope, precisely

Beside `columns`, `rows` and `graph_commit_id`: `retrievals` and `metrics`
(today's descriptors, plus `recall_achieved` per filtered arm);
`notifications` (`N…` codes with the same four fields as refusals: a cross
join, a destination-ranked scan of a whole type, a wildcard without a budget,
a `bm25` arm without a cap); `why_not`, present when zero rows were returned
and cheap to produce from the profile, giving the rows that survived each
clause in plan order so the agent sees which clause emptied the result;
`complete` and `incomplete_reason` under `@satisfice`, so a partial answer is
never silent; `continuation` for paging over the pinned snapshot; and, when
the request was `explain`, `estimate` (rows, bytes, time) so a `@budget`
refusal is informative before any row is read.

### 9.8 What the redesign still refuses

Unbounded recursion without a declared work budget and a typed `exhausted`
outcome (and, with one, only as a kernel operator by RFC); path values with
modes and selectors; general subqueries and `WITH` pipelines (the three
closure forms plus `batch` cover the measured demand); free read-write mixing
(pattern-scoped selection with one target type is the bounded form); a `null`
literal in reads; executing user code in the engine (a registry function is a
Rust implementation plus a row; a provider-backed `judge($a.text, "about
pricing")` semantic predicate is the one exception worth designing, with LOTUS's
declared accuracy target, because TAG measured +38 points for it); schema-
resident ranking; open labels. Each is still a wall in §4.2 or a cost the
theory in §7.6 names.

### 9.10 Room for federation

Nothing above spans graphs, and a redesign is the moment to make sure nothing
above forecloses it. Federation has two shapes, and they need different
things from the language:

- **Query federation**: one query reads from several graphs (one server or
  several). The precedents are SPARQL 1.1 `SERVICE`, SQL foreign tables
  (`postgres_fdw`, Trino connectors) and GraphQL Federation, where subgraphs
  compose into one schema and an entity's `@key` is the join address across
  services.
- **Replication federation**: a graph, or a view of it, is kept current
  elsewhere. The precedents are git remotes (Dolt's `push`/`pull`), and in
  this repository the actor-views RFC's *shares* (a recipient's view
  materialised into its own graph from the source's change feed) and the
  branch-operations RFC's deferred "Git-style remotes".

What GQ 3 reserves so both stay possible, each a small rule now:

1. **A graph qualifier in type references.** The schema-namespaces RFC gives
   `crm::Contact`; a type reference grammar must admit one more segment for a
   graph (`sales@crm::Contact`, or a `from graph sales` declaration clause)
   without touching identity, exactly as the namespace RFC says the namespace
   is "part of the name and never part of identity".
2. **Keys stay the cross-graph address.** The integer-identity RFC makes
   `__id` engine-owned and packs type and row into 64 bits; those are
   graph-local by construction. The same RFC keeps `@key` as the public
   address, and federation depends on that staying true: a foreign reference
   is a key, never a packed id. This is GraphQL Federation's `@key` rule.
3. **One snapshot per graph, pinned in the envelope.** A cross-graph read
   cannot have one snapshot; it has one per graph. The envelope's
   `graph_commit_id` becomes a map, and `batch` pins per graph. Invariant 3
   ("one coherent accepted view") holds per graph and the result says which.
4. **Views are the unit that binds to a foreign graph.** A `view` declared over
   `sales@crm::Contact` is a foreign table; its result columns are its contract,
   so the local planner treats it as an opaque, typed, pushdown-capable source
   with its own budget. Pushdown rules are the planner's problem later; the
   language already has the declaration kind.
5. **Writes stay single-graph.** A mutation targets one graph; cross-graph
   consistency comes from change feeds and replication, never from a
   distributed commit. The one-commit-per-mutation wall holds.
6. **Search across corpora is the polymorphism problem again.** BM25
   statistics are per table and per graph; the polymorphism RFC's one-corpus
   contract for cross-member `bm25` is the same contract a cross-graph search
   needs, and `rrf` over per-graph arms is the honest form until then.
7. **Policy per graph.** Cedar resources are graphs already; actor views are
   per graph; a federated read is authorised per graph and the envelope's
   `view.restricted` is per graph too.

None of this is built in GQ 3; each is a sentence in the grammar or the
envelope that costs nothing now and is expensive to retrofit.

### 9.9 How far this is from today's GQ

Most of §9.3 is additive and already in §4.3 and §7.7: blocks as values,
`optional`, `having`, `collect`, views, the function registry, interface
bindings, annotations, catalogue types, paging, `returning`, pattern-scoped
writes, notifications, `why_not`, editions. Only four items are incompatible
with stored text and are the reason to ask the question at all: the arrow
spelling for traversals (withdrawn on 2026-10-30, §13.3), `mutation` as a keyword, `retrieve` replacing ranking
functions in `order`, and `@valid`/`invalidate` as schema and verb. Under an
edition line all four ship as GQ 3 beside a GQ 2 the compiler keeps compiling
for one release, with `queries validate` printing the rewrite; the rewrite from
`$p worksAt $c` to `$p -worksAt-> $c` and from `order { nearest(…) }` to a
`retrieve` arm is mechanical, which is what the `suggestion` field with
`machine_applicable` edits was built for.

---

## 10. The repository's RFCs: what to adopt, what to change, what is missing

Read for this section, in full or in their design sections: RFC 0047 (search
plan truth, merged, in progress), RFC 0048 (search contracts, PR #793), the
analyzed lexical search RFC (PR #792), the shared expression model and its
operator-vocabulary amendment (PR #805), typed edge alternation, RFC 0055
(branch statements) and RFC 0056 (one language, PR #670), the engine
catalogue, session settings, compatibility surfaces, bulk load as a plan, RFC
0041 and RFC 0003 (stored queries, MCP), RFC 0045 (GQT), RFC 0043, 0046 and
the index-facts and incremental-reconciliation RFCs (PR #1156), RFC 0012 and
0015 (embeddings), the polymorphic types RFC with its integer-identity
dependency (PR #884), schema namespaces (PR #1015), GQP query contracts (PR
#1197), actor views (PR #951) and branch operations v2 (PR #677). The
judgement is against §4, §7 and §9; "adopt" means the decision stands under
the redesign, "change" names what the redesign would alter, "missing" names a
decision no RFC makes.

### 10.1 Search: 0047, 0048, the lexical RFC, and the index RFCs

**RFC 0047, search plan validation and result guarantees. Adopt, nearly
whole.** Its seven items are the correctness contracts §4.2 keeps: the
four-field diagnostic with frozen codes and machine-applicable suggestions
(now shipped and measured), `T27` and `FullTextIndexRequired` (no silent
case-sensitive scan), the ranked binding rooting its component (declaration
order stops mattering), one total order with the fused score as a column, read
descriptors with `recall` as a contract rather than an observation, the served
read floor, and plan acceptance as the single gate for run, explain and
replay. Its alternatives table is right on every row, including the rejection
of a compile-time refusal of ranked destinations (`T26`) and of recording
retrieval in `QueryIR` as a second truth. Two things to change: (1) it fixes
`recall` as a function of retrieval *kind*; §8.5's evidence (ACORN, Big-ANN)
says the envelope should also carry the recall *achieved* under a filter, a
measured fact beside the contract; (2) its "served read floor" `usage` object
is the right place for the cost *estimate* the agent-first paper asks for
before execution, so `explain` should return the same object with
`estimated: true`. One thing it got right that the redesign keeps verbatim:
"the reader is often an agent that treats an error as the documentation it
acts on", which is §8.2's Olausson finding stated a year earlier.

**RFC 0048, search contracts and retrieval algebra. Adopt the laws, the
windows, the weights, exact retrieval and the schema declarations; change the
surface.** The eleven laws (eligibility before selection, cuts are semantic,
windows are separate from the final limit, target identity, exact answers
independent of index state, every cut total, partitions never redefine the
population, meaning resolved from the accepted schema, one snapshot, separate
facts, one resource protocol) are the retrieval algebra §2.10 asked for and
every one survives the redesign. `candidates: n` as a window distinct from
`limit`, with the rule that "a downstream limit never sizes an upstream
window", fixes a real defect (an `rrf` vector arm fetching exactly `limit`
targets). Weighted `rrf` with 2 to 16 arms and projectable per-arm scores that
are null outside the arm's window is exactly §9.5's arm model. `nearest(…,
exact: true)` is the right spelling for the exact contract. `Vector(n,
distance="cosine")`, the recipe identity (model revision plus encoding
choices, hashed over a canonical typed form, never a label) and `coverage =
exact` are schema and envelope decisions the redesign adopts unchanged.

What to change is the one sentence the RFC is proudest of: "It adds no clause
and no stage: the query surface is the Shared expression model, where search
predicates live in `match` and the leading ranking call lives in `order`." Its
own 2026-09-13 draft had a staged syntax (`rank … yield`, named sources, a
`filter` stage) and withdrew it because "stages would be a second surface for
the same plan". That is a compatibility argument, and §2.10 shows what it
costs: the ranking token does two jobs (select the population, sort the
rows), the aggregate and `distinct` cases make the overload visible, a score
cannot be a *filter* (`nearest(…) < 0.4` is unwritable), and every option the
RFC adds (`candidates`, `weight`, `exact`, `k`) is grammar on the ranking call
rather than a value in an expression. Under §9.5 the same laws hold over a
`retrieve { }` clause whose arms bind score columns; `candidates` becomes the
arm's `k`, `weight` becomes a coefficient in a score expression, the threshold
becomes a `having` conjunct, and `norm()` (min-max over the arm's window,
Bruch et al.) joins `rrf()` as a function because the evidence says weighted
normalised fusion beats RRF by 2–3 nDCG points. The RFC's deferred list (per-
group selection, a second ranking after a traversal, fusion across bindings,
discovery across node types, a reranker) is the list §9 unblocks, and the
RFC's own "evidence shape" example ("the 5 best documents, and all of their
passages") is the clearest case for a retrieval position separate from the
sort.

One more change: the RFC's "Agent use" section says "an in-context competence
measurement decides between spellings" and cites the 45/45 run. Keep the
instrument, but §8.2 says what it will show: SM3 found a 10–45 point zero-shot
gap between spellings that five retrieved examples erased. Spelling choices
should be made on reviewability and regularity and tested for repairability,
not on first-try accuracy.

**Analyzed lexical search (PR #792). Adopt the model; align the deprecation
with editions.** Schema-owned analyzer profiles (`standard_v1`,
`standard_folded_v1`, `english_v1`, immutable, every setting explicit,
NFC named as part of the profile), one typed lexical query `terms(q, mode:,
max_edits:)` consumed by both the predicate `match_terms` and the ranking
`bm25`, the membership laws (`matches(edits=0) ⊆ matches(edits=1) ⊆
matches(edits=2)`; `matches(indexed) = matches(unindexed)`), the exact scan as
oracle and the field-corpus statistics that graph filters never rescope are
the correct separation of meaning from acceleration (invariant 7) and the
reason §8.5's "index state changes which rows a text query matches" defects
(#747, #748, #749) stop being possible. The custom `bm25_v1` scorer is a bold
choice; the RFC earns it with guards showing native fuzzy BM25 rewarding a
rare expansion 38× and rounding a common term's IDF to zero at 2^24
documents. Two notes. First, `@analyzed` as a capability separate from
`@index` is right (searchability is semantics, the index is cost), and it is
the pattern the polymorphism RFC should reuse for "the full-text index is
declared on the interface property". Second, the RFC retires `search`,
`match_text` and `fuzzy` through a one-release deprecation and "the next GQ
language major"; that is exactly the migration an edition pragma (§7.7)
exists for, and the RFC should name it rather than a boot-time quarantine.

**RFC 0043, 0046, index facts as planner input, incremental reconciliation
(PR #1156). Adopt.** These are the engine's side of "indexes are derived":
analyzer certificates bound to artifacts, a read-only index status surface
that observes and never builds, access paths decided from pinned index facts
and stated in the plan, and appended-row segments with a Lance 13 fence for
moved rows. The language consequence they share is already in §9: coverage is
a planning fact the envelope reports, never a semantics. One request: RFC
0046's `index status` should be a catalogue type (`@Index` with `state`,
`coverage`, `reason`) rather than a verb, so an agent asks it in GQ with the
rest.

**RFC 0012 and 0015, embeddings. Adopt; make the lag visible.** Provider-
independent configuration with the embedding identity recorded in the schema
and a typed refusal of a mismatched query space is the right contract. 0015's
decision that an embedding is derived state materialised by a reconciler, not
a write-path network call, is invariant 7 applied correctly and matches how
every production memory system in §8.4 behaves (write the text, embed
later). Its cost for an agent is that a just-written memory is not a
`nearest` hit until reconciled; §9.7's envelope carries `embedding_coverage`
for exactly this, and the `@Index` catalogue row should report the pending
count so an agent can decide to wait or to fall back to `bm25`.

### 10.2 Language: the expression model, alternation, one language, catalogue, settings, versions

**Shared expression model and the operator-vocabulary amendment (PR #805).
Adopt, and take the amendment to its conclusion.** One expression type for
every clause, context rules as checks rather than second types, three-valued
logic with no null literal, the conjunct list placed by one pass, mutations
lowered through the same expression (deleting the string-built SQL): all
right, all kept. The amendment's three rules are the most important language-
design statement in the repository: the reserved set is closed and holds only
the logic of the language; every operation of one domain is a call, "so a new
capability adds a function to the type checker and no grammar"; options are
named. That is §7.7's tier-2 principle. What the amendment does not yet say,
and the redesign does: the calls are a *registry* exposed as `@Function`, not
match arms in the type checker, so a new function is a row and `lint` resolves
it from the snapshot. The amendment's own rollout (`contains` infix to
`contains()` call through a deprecated rule, `GQ_LANGUAGE_VERSION` 2.0 → 3.0
→ 4.0 over two releases) is the third time the project has paid for not
having editions; §7.7's pragma is the generalisation. One alternative the RFC
rejected deserves a second look under §9: "a separate `where { expr }` clause
after `match`" was set aside to keep conditions beside the pattern. That is
the right call for filters, and it is also why §9 puts scores in their own
`retrieve` clause rather than beside the pattern: a retrieval is not a
condition on one row.

**Typed edge alternation. Adopt as the template.** Exact `{1,1}` default,
finite bounds, common-property typing, `$e.@type`, a declared budget with
admission before the scan and a typed exhaustion error, replay validation of
every member pin: this RFC is the model every later kernel operator in §7.7
tier 3 should follow, and §9.8's "unbounded recursion only with a declared
work budget and a typed `exhausted` outcome" is its rule generalised.

**RFC 0055 and RFC 0056, branch statements and one language. Adopt the
direction; change two rules.** Every operation as a GQ statement, classified
by the compiler and entering through the door its classification names, is
the one-language requirement (R10) and the reason the GQT corpus can test
merges. Two of its rules should change. First, "a control statement takes no
parameters": that is a transport simplification that costs agents the ability
to store `branch merge $b into main` as a typed tool, and §9.3 gives
declarations typed `$branch`/`$commit` parameters instead. Second, 0056's
raw block `$$ … $$` for inline payloads is fine for schema text, but a load's
rows belong in a typed `load` statement with a source, as the shipped `load
csv` already does; the one-language RFC predates that statement and should
cite it. Its maintenance-as-job decision (RFC 0011 Decision 11) and the new
`maintain` Cedar action are right and overdue.

**Engine catalogue. Adopt; extend to the schema.** Registry-derived `@` types
are the mechanism §7.7 builds every registry on. The polymorphism RFC proposes
"a structured, read-gated catalog route" for types, properties, keys,
interfaces and members; it should be `@Type`, `@Property`, `@Edge` and
`@Interface` catalogue types instead, for the reason the catalogue RFC itself
gives ("a query surface also gives the join for free").

**Session settings. Adopt; split request intent from process knobs.** The
scope model (request versus process), `show` as a statement and the refusal
of unknown names are right. §9.3's `@budget`, `@approximate` and `@satisfice`
are declaration-level intent that belongs on the query, not in a `set` line,
and the `@Setting` catalogue makes the split visible.

**Compatibility surfaces. Adopt the hashes; add editions.** Per-rule hashes
and a `(major, minor)` per surface detect a change; they do not let two
grammars coexist. The lexical, operator-vocabulary and polymorphism RFCs each
schedule a "GQ language major"; without an edition pragma each is a boot-time
quarantine of every graph with a stored query that used the old spelling.

**Bulk load as a query plan, RFC 0041 and RFC 0003, RFC 0045. Adopt.** A load
with an `explain`able plan is the right shape; stored queries as typed tools
with `@description`/`@instruction` projected through MCP are the agent door
§8.2 wants; and RFC 0045's amendment making "agent experience … the format's
design priority" is the right ordering. The one gap in 0041/0003 is the one
§2.13 names: a stored query is callable, not bindable; views are the missing
declaration kind.

### 10.3 Compositional closure: no RFC owns it

This is the clearest finding of the review. Every closure shape §2.2 names
has an issue and none has an RFC:

| Closure shape | Issue | Nearest RFC text |
|---|---|---|
| Optional traversal | #658 (P-high, needs-rfc) | none; the shared expression RFC's alternatives mention nothing optional |
| Block aggregate as a value | #1050 | the shared expression RFC keeps blocks as predicates; 0048 fixes `rrf` arms as values but not blocks |
| Post-group filter (`having`) | none filed | none |
| `collect` / one nesting level | #649, #1050 | none |
| Derived relations (views) | none filed | 0041 (stored queries are callable, not bindable); #1197 names "definer-rights stored queries" as a *later* idea |
| Top-N per group | none filed | 0048 defers "per-group selection (at most `n` targets per group)" |
| Union at the projection | #648 | the polymorphism RFC (PR #884) is the one RFC that supplies a closure form: abstract bindings and inline unions `$x: A \| B` |

The polymorphism RFC therefore deserves credit it does not claim: interface
and union bindings *are* the bounded union §7.6 recommends over general
pattern union, and "result schemas never depend on the member set" is the
principle that keeps it conservative. The rest needs one RFC, "Derived
relations and optional patterns": views as declarations inlined at compile
time, `optional { }` with pattern-bound filters and nullable bindings, blocks
as values, `having`, `collect`, and the Felleisen test (§7.6) as its
acceptance criterion for each. It is the highest-value RFC not yet written,
because §8.1's measured workload (one question per query, 80–90% redundant
sub-plans, 92 requests per node page) is a closure problem.

### 10.4 Polymorphism: PR #884, with integer identity and namespaces

**Adopt the query model; accept the storage dependency with eyes open; change
one refusal.** The RFC makes interfaces and inline unions abstract node types
a query can bind, traverse to and mutate through; resolves each use once per
invocation to an id-sorted type set; lowers to one union-scan node with per-arm
resolution (pins, filters, index facts, late materialisation); gives `$x.@type`
and `$x.@type_id` as query-only columns; adds `is`/`is not` type tests and
narrowing by rebinding; and keeps result schemas independent of the member set
(a column's type comes from the declared interface, never from the current
implementors). Its direction-inference rules, migration tiers (adding
`implements` is metadata-only; narrowing is validated; cross-kind name reuse
in one apply is refused), stored-query dependency pinning by `StableTypeId`,
and the `ChangeScopeMembershipChanged` feed refusal are careful. The
prototype findings it records (a bare-id rule dropping a row that shares a
key across types; single-table shortcuts opening `node:<Interface>`) are the
right kind of evidence. This is §7.7 tier 2 item 3 and §9.3's interface
bindings, done properly.

The storage dependency is the integer-identity RFC: `__id` becomes a `UInt64`
from a distributed sequence and every edge endpoint a packed reference (16-bit
`StableTypeId`, 48-bit row id), while `@key` stays the public address. That is
a format change justified by polymorphic endpoints (an edge row needs to say
which table its endpoint is in) and it is the right design for that purpose;
the evaluation's one condition is §9.10 item 2: keys must stay the only
cross-graph address, which the RFC states. The 16-bit type limit (65,536
types) and 48-bit row limit are generous for the workload.

The refusal to change: "`bm25` on a binding whose set has more than one member
is refused" until a one-corpus scanner option exists upstream. The reasoning
(scores from two tables' statistics are not comparable; normalisation is
"unstable across `k`, filters and pages") is sound for a *single* `bm25` score,
but it leaves #648 (discovery across types) with no answer until Lance
changes. §8.5's evidence offers the interim form the RFC's own alternatives
table dismisses: per-member arms fused by rank (`rrf`), which is how every
multi-index search engine answers a cross-corpus query, and which §9.5's
`retrieve` clause expresses as one arm per member. Rank fusion "ignores
absolute relevance", as the RFC says; for a discovery query that is the point.

Two smaller notes. The RFC's "structured catalog route" should be catalogue
types (10.2). And `$x: *` (every node type) has no spelling; a schema can
declare a root interface, but an implicit `@Node` interface that every type
implements would give discovery one binding without a schema convention.

**Schema namespaces (PR #1015). Adopt.** Namespaces as part of the name and
never of identity, moves as renames through `@rename_from`, `use ns;` with no
search order and two matches a type error, and the canonical string on every
wire surface: this is the qualifier §9.10 extends with a graph segment, and
its "composition is declared once, in `cluster.yaml`" rule is the right
precedent for declaring a federated source.

### 10.5 Permissions: PR #1197 against PR #951

The two drafts present themselves as alternatives. Against the agent workload
they are two layers, and the review's recommendation is to keep both, each
narrowed to what it is for.

**Actor views (PR #951) is the data layer.** Its contract is non-interference:
two graphs that give an actor the same view give the same answer to every
query, including counts, scores, ranks, errors and the result schema, enforced
at one door per mechanism (plan acceptance for reads, attempt validation and
the publication CAS for writes). Its model is graph data: `@principal`,
`@group`, `@member`, `@securable`, `@in(p)`, `@grant`, `@inherits`,
`@requires`, `@deny`, `@starts`/`@expires`; users write facts, never rules, and
the readable set is a stratified fixpoint with denial in one layer. That is
the schema-design guide's principle 9 ("keep the rules in the graph") applied
to access, it is the Zanzibar shape every comparable product converged on
(its Appendix B), and it is the only one of the two that answers the company-
brain case where thousands of audiences already exist in source systems.
`view.restricted` as a policy fact rather than a data count is the right
signal, and "a name the view hides does not exist for the actor" is the right
disclosure rule (the polymorphism RFC adopted it after withdrawing a refusal
that leaked a hidden member's existence). Its costs are real: twelve phases,
two measurement gates it is blocked on, exact restricted search (D10) that
refuses approximate `nearest` to restricted actors and bounds visible text by
`max_exact_search_rows`, and sixteen annotations. The redesign's view: accept
the contract and the model, ship the first scope (type and column visibility,
per-row placement by property, reads), and treat exact restricted search as
the price of non-interference rather than a flaw, since §8.5's precision-first
evidence makes a bounded exact search over the visible set a good default
for memory-sized corpora.

**GQP (PR #1197) is the operation layer.** Its insight is agent-native: an
agent produces a program, so "validating that program is a productive
boundary"; a capability is a contract plus (for deployed tools) one exact
program revision; Cedar stays the only grant authority; trusted context
(`$auth.tenant_id`) is separate from parameters; preflight uses the execution
checker; diagnostics repair rather than refuse. That is §8's verifiability
thesis and §9.3's stored-queries-as-tools in policy form. Three changes.
First, GQP should not be a separate language: a `contract` is a declaration
kind in the one GQ file (§9.4), sharing the grammar, type checker and
registry, which the RFC's own "GQ language family" phrasing almost says. Second,
its initial fragment ("one concrete node binding"; no traversals, blocks,
negation or search) is too small for a graph: the planner's requirements
(RFC 0047) already retain every predicate, traversal and block, so guard
preservation can be checked over the kernel, not a sub-fragment; the fragment
should be the kernel minus retrieval. Third, its row scope is a guard the
author must write (`$ticket.tenant_id = $auth.tenant_id`) and the checker must
find; that is brittle where actor views' placement is structural. The clean
composition: #951 decides *which rows and columns exist* for an actor; #1197
decides *which programs* an actor may run over them and binds trusted
context; a definer-rights view (which #951 calls an "answer capability" and
#1197 a "deployed capability") is the same object in both, and in §9 it is a
`view` with a `contract`.

Both must be federation-aware (10.6): Cedar resources are graphs, views are
per graph, shares are the materialised form.

### 10.6 Federation: what the RFCs leave room for

No RFC proposes query federation, and three touch its preconditions. Schema
namespaces supply the qualifier that a graph segment extends. The integer-
identity RFC keeps `@key` as the public address, which is the cross-graph
join rule; its packed ids must never leak as foreign references, and its
export spelling (`from_type`/`to_type` with keys) already honours that. Actor
views' *shares* (a recipient's view materialised into a separate graph kept
current from the change feed, with the source as lineage) and branch
operations v2's deferred "Git-style remotes" are the replication form; both
would become one mechanism once a change feed can be consumed as a load
(RFC 0056's `load` statement with a feed source). The polymorphism RFC's
one-corpus contract for cross-member `bm25` is the cross-graph search
contract in waiting. What no RFC reserves and §9.10 asks for: a graph
qualifier in type references, per-graph commit pins in the envelope, views as
the foreign-source declaration, and the rule that writes stay single-graph.

### 10.7 Verdicts

| RFC | Verdict | The change, if any |
|---|---|---|
| 0047 search plan truth | adopt | report achieved recall beside the contract; `explain` returns the `usage` estimate |
| 0048 search contracts | adopt laws, windows, weights, exact, schema declarations; **change the surface** | a `retrieve` clause with score columns; `norm()` beside `rrf()`; thresholds as filters |
| Analyzed lexical search | adopt | ship the deprecation through an edition, not a quarantine |
| 0043, 0046, index facts, incremental reconciliation | adopt | `@Index` catalogue type instead of a verb |
| 0012, 0015 embeddings | adopt | pending counts in `@Index`; coverage in the envelope (exists) |
| Shared expression model + operator vocabulary | adopt; **go further** | functions as a registry (`@Function`), not type-checker arms |
| Typed edge alternation | adopt as the template for every budgeted operator | none |
| 0055 / 0056 one language | adopt direction | typed parameters on control statements; cite `load csv`; keep maintenance as jobs |
| Engine catalogue | adopt; extend | schema catalogue types |
| Session settings | adopt | declaration annotations for per-query intent |
| Compatibility surfaces | adopt hashes | add editions |
| Bulk load as plan, 0041, 0003, 0045 | adopt | views as the missing declaration kind |
| Polymorphic types + integer identity | adopt; accept the format change | `rrf` over member arms as the interim cross-member search; `@Node` root; catalogue types not a route |
| Schema namespaces | adopt | reserve a graph segment |
| Actor views (#951) | adopt the contract and model; narrow first scope | ship type/column/row visibility for reads first |
| GQP contracts (#1197) | adopt the capability idea | a `contract` declaration in GQ; fragment = kernel minus retrieval; compose with actor views |
| Branch operations v2 (#677) | out of this review's scope (storage) | its deferred remotes are federation's replication form |
| **Missing** | closure RFC | views, `optional`, blocks as values, `having`, `collect`, Felleisen test as acceptance |
| **Missing** | editions RFC | `gq N;`, N-1 compiler, registry records editions |
| **Missing** | federation reservation | qualifier, per-graph pins, views as foreign sources, single-graph writes |

---

## 11. Staging and sequencing

The order below is derived from three things: what each piece *depends on*
(a change cannot ship before its enabler), what each piece *is worth* to the
agent workload (§8.6's metrics: round trips per task, tokens per correct
answer, repair turns), and what each piece *breaks* (anything that breaks
stored text waits for editions). Each stage is independently shippable and
independently valuable; nothing in a later stage is needed to make an earlier
one pay. Sizes are relative (S, M, L), not dates.

### 11.1 The dependency graph

```text
                       ┌────────────── E  editions (gq N;, N-1 compiler, registry records) ──────────────┐
                       │                                                                                  │
 M0 measure ──► S1 ───┼──► F  function registry ──► R  retrieve clause ──► W2 computed writes            │
 (baseline,            │        (generic call,          (windows, weights,       (arithmetic + precondition)│
  task corpus)         │         @Function, can_fail)    norm, thresholds,                                 │
                       │                                 rerank; rewrites 0048)                           │
                       ├──► S  schema catalogue types ──────────────────────────────────────┐             │
                       ├──► N  envelope side channel (notifications, why_not, estimate) ────┤             │
                       ├──► P  paging (limit $k, continuation token) ────────────────────────┤             ▼
                       │                                                                    ├──► X  edition-3 spelling flip
                       ├──► C  closure (blocks as values, having, collect, optional, views) ─┤       (arrows, mutation, retrieve
                       │              │                                                      │        default, deprecations run
                       │              └──► A  permissions (contracts as declarations         │        out: search/fuzzy/match_text,
                       │                      over views; actor views first scope) ──────────┤        infix contains, rrf positional k)
                       ├──► L  lexical (@analyzed, terms, match_terms, bm25_v1) ─────────────┤
                       ├──► W1 writes (pattern-scoped, returning, @valid/invalidate) ────────┤
                       │                                                                    │
      storage upgrade ─┴──► I  integer identity ──► Poly polymorphic endpoints ─────────────┘
                                     (interface *bindings* need no storage and ship under C)

                                                           Fed  federation: reservation in E/S; build after Poly + views
```

Reading it: **E** is the root of every change that touches stored text, so it
is first and it is small. **F**, **S**, **N**, **P**, **C**, **L**, **W1** are
mutually independent and additive; they can run in parallel on separate
tracks. **R** needs F (scores as values) and E (it changes a surface). **X** is
the one deliberately breaking stage, and it is last among the language stages
because by then every new spelling has existed additively for a release and
every old one has had its deprecation. **I** and **Poly** are a storage
upgrade with their own RFC pair and their own clock; only interface
*bindings* (no storage change) are pulled forward into C because they are the
bounded union §7.6 wants. **A** waits for views (contracts bind to them).
**Fed** is reserved early and built late.

### 11.2 The stages

| Stage | Contents | Depends on | Breaks stored text? | Gate before ship | What an agent gets |
|---|---|---|---|---|---|
| **M0 Measure** (S) | Baseline the §8.6 metrics on the personal, CRM and engineering graphs: tokens per correct answer, round trips per task, first-try validity and repair turns on the 45-task instrument plus a node-page and a discovery task; count refusals by `T` code in served metrics; freeze a 30-task agent corpus with exact-query ground truth | nothing | no | the corpus and numbers are checked in beside the GQT corpus | nothing yet; this is what every later stage is judged by |
| **E Editions** (S) | `gq N;` optional first line (absent = current); compiler compiles N and N-1; stored-query registry records the edition at validation; `queries validate` and `cluster plan` report edition drift with the rewrite; `GQ_LANGUAGE_VERSION` becomes the edition; compatibility-surfaces hashes per edition; the grammar reserves a graph segment in type references (`Fed` reservation, unused) | nothing | no | a GQT case per edition path; a server boot test with a GQ 2 stored query under a GQ 3 default | nothing visible; the stage that makes X survivable |
| **S Schema catalogue** (S) | `@Type`, `@Property`, `@Edge`, `@Interface`, `@Index` (with coverage and pending counts), `@Function` (empty until F), `@View` (empty until C); `@example` annotation on types and views, stored as metadata and served through `@Type` | catalogue mechanism (exists) | no | GQT cases joining catalogue rows to user rows; `og_catalog` served | schema discovery in one query (#1058 answered); the agent-first paper's cheapest ask |
| **N Side channel** (S–M) | `N…` notifications with the four fields (cross join, destination-ranked scan, wildcard without budget, uncapped bm25); `why_not` funnel on zero rows from the profile; `estimate` on `explain` (`usage` with `estimated: true`); `complete`/`incomplete_reason` reserved for `@satisfice` | 0047's diagnostic type | no | the funnel checked against the profile's `actual_rows`; notification codes frozen in `codes.rs` | repair turns fall (Olausson); empty results explain themselves (MAC-SQL) |
| **P Paging** (S) | `limit $k` (I64 parameter, non-null); continuation token in the envelope derived from the total order over the pinned snapshot; `--after <token>` on CLI, `after` on HTTP | total order (exists) | no | token round trip across a write (snapshot pinned, no skip, no repeat); two-key tie cases | stable pages without hand-written keyset predicates |
| **F Function registry** (M) | generic `name(args, opt: v)` call rule; registry with signature, nullability rule, `can_fail`, `volatile`, pushdown mapping; the existing aggregates and search calls become rows; `contains()`/`starts_with()` land as the operator amendment specifies (infix kept, deprecated); first scalar library (`lower`, `upper`, `trim`, `length`, `coalesce`, date extraction, `case`); arithmetic with typed overflow refusal and the `can_fail` pushdown rule | E (for the infix deprecation) | no (infix stays one release) | `@Function` rows equal the compiler's registry (a test, not a doc); pushdown refuses to lift a fallible conjunct past its guard; GQT per function | derived values filter and order server-side; the end of client-side arithmetic |
| **C Closure** (L) | blocks as values (`count { } as n`, `exists { } as b`), any expression on a block's right side; `having { }`; `collect($x)` / `collect($x { a, b })`; `optional { }` with pattern-bound filters and nullable bindings (`LeftExpand`); `view` declarations inlined at compile time, bindable as `$x: View(args)`, registered and validated with stored queries, listed in `@View`; interface *bindings* `$x: Named` as a union scan with the common projection (from the polymorphism RFC, no storage change); the Felleisen test applied to each as an acceptance criterion in the RFC | S (for `@View`), registry infrastructure | no | GQT per form; plan acceptance updated for `LeftExpand` and the projected block; a view invalidation case under `cluster plan`; the node-page task in M0 goes from N requests to 1 | the largest round-trip reduction in the plan; derived relations agents can author on a branch |
| **L Lexical** (M–L) | `@analyzed` profiles in SchemaIR; `terms()`, `match_terms`; exact scan as oracle; `bm25_v1`; `search`/`fuzzy`/`match_text` compile to `match_terms` with a deprecation under E | E; F (named options) | no (deprecation window) | the RFC's oracle fixtures and the three Lance guards; #747/#748/#749 cases green at every index state | text answers that do not depend on index state |
| **R Retrieve** (L) | `retrieve { }` clause binding typed score columns; arms with `k`; `norm()`, `rrf()` with n arms and weights, `rerank(model, text, k)` as a provider-backed stage; score columns usable in `having`/`order`/`return`; `@approximate(recall)` declaration; achieved recall in the envelope; 0048's laws, `exact: true`, `Vector(n, distance=…)` and recipes; rankings in `order` keep compiling for one release and lint prints the rewrite | E, F, 0047 (shipped) | no until X | the fusion and vector oracles 0048 names; GQT for windows, weights, thresholds, rerank, n arms; the discovery task in M0 answered by `rrf` over interface arms | hybrid search with weights, thresholds and a reranker; the end of two-query hybrids |
| **W1 Writes** (M) | pattern-scoped `update $a … where { }` and `delete $e where { }` with one target type; `returning { }`; `@valid(from, to)` schema role and `invalidate`; `set { x: null }` in assignment position; bounded pattern-scoped `insert` | read planner under the write door (the shared expression RFC's "later RFC") | no | staging protocol unchanged (D2); GQT per form; affected receipts | invalidation as a verb; one round trip per write instead of read-then-write |
| **W2 Computed writes** (S) | `set { visits: visits + 1 }` under `@if_commit` or a key equality; the read-set rule | F (arithmetic), W1 | no | contention case under DST; retry idempotence | the increment without a round trip |
| **X Edition 3** (M) | `mutation` keyword; arrow traversal spelling (`-e->`, `<-e-`, `-e-`; withdrawn 2026-10-30, §13.3); `retrieve` as the only ranking position; removal of `search`/`fuzzy`/`match_text`, infix `contains`/`starts_with`, positional `rrf` `k`; the GQ 2 compiler kept for one more release; `queries validate` prints every rewrite as a machine-applicable suggestion | every deprecation above having lived one release | **yes, under the edition** | every stored query in the fleet rewritten by the suggestion tool and re-validated; the 45-task instrument re-run on the new spelling | one spelling per operation; direction visible; the novelty tax paid once |
| **I + Poly** (L, own clock) | integer identity storage upgrade; polymorphic endpoints; unions `A \| B`; type tests; `@type_id`; interface-wide mutations; `rrf` over member arms as the interim cross-member search | storage upgrade RFC pair; C (interface bindings already shipped) | no (schema feature; old binaries refuse) | the RFC's prototype findings as regressions; upgrade journey tests | edge families collapse; discovery across types |
| **A Permissions** (L) | actor views first scope (type and column visibility, per-row placement, reads); `contract` declarations in GQ bound to views and stored queries; Cedar grants on named capabilities; the fragment = kernel minus retrieval | C (views), S, both RFCs amended per §10.5 | no | #951's differential oracle for the first scope; #1197's adversarial matrix | query-only agents; shared graphs with audiences |
| **Fed Federation** (L, later) | graph qualifier in type references; per-graph commit pins in the envelope; views over a foreign graph; change-feed-as-load for shares | E/S reservation; C (views); Poly (one-corpus search) | no | nothing until designed | cross-graph reads; shares kept current |

### 11.3 Three parallel tracks

The stages group into tracks that touch different code and can run at once:

- **Compiler and language track:** E, F, C (grammar, type checker, lowering,
  inlining), X. Owner: `omnigraph-compiler`, the planner's `resolve_query`.
- **Planner and engine track:** N, P, R, W1/W2, the `LeftExpand` and union
  scan nodes, acceptance updates, the fusion and vector oracles. Owner:
  `omnigraph-planner`, `crates/omnigraph/src/engine`.
- **Server, schema and policy track:** S, L's SchemaIR features, I/Poly, A,
  the registry's edition records, `cluster plan` validation of views and
  contracts. Owner: `omnigraph-server`, `omnigraph-cluster`, `schema_ir`.

Two things keep the tracks from colliding. The GQT corpus is the shared
contract: every stage lands its cases first, and a case another track's
change turns red is a conversation, not a merge. And the catalogue is the
shared interface between tracks: F publishes `@Function`, C publishes `@View`,
S publishes the schema types, and each other track reads them rather than the
crate internals.

### 11.4 Rules of the sequence

1. **Additive before breaking.** Every new spelling exists beside the old
   one for a full release before X removes the old one, so an agent reading
   the deprecation warning has one release to learn the new form and the
   suggestion tool to apply it.
2. **Enabler before consumer.** E before anything that breaks; F before R;
   C before A; S before anything that needs a registry visible to agents.
3. **Measure at every stage.** M0's numbers are re-run after each stage on
   the same corpus. A stage that does not move round trips, tokens per
   correct answer or repair turns is questioned before the next starts;
   a stage that moves first-try validity down (a novelty tax) must show the
   repair turns compensate.
4. **One RFC per stage, the umbrella names them.** The umbrella document
   (§10.7) carries this table and nothing else; each stage's RFC carries its
   grammar, semantics, acceptance checks and cases. 0047 is already the
   shape: seven items, eight rollout steps, each a PR with its regression.
5. **Storage changes on their own clock.** I and Poly follow the storage-
   upgrade matrix (genuine predecessor binaries, whole-root restore); the
   language stages never wait on them, and interface bindings are pulled out
   of Poly precisely so that C does not either.
6. **The fleet is part of the gate.** Before X, every stored query on the
   personal, CRM and engineering graphs is rewritten by the suggestion tool
   and validated under edition 3; a rewrite the tool cannot make is a defect
   in the tool, not a reason to extend the window.

### 11.5 What the first month looks like

M0 and E in parallel, since M0 is measurement and E is compiler work; S and
N start the week E's grammar lands, because both are small and both change
what an agent sees immediately. F's RFC (the operator-vocabulary amendment
rewritten as a registry) and C's RFC (derived relations and optional
patterns) are drafted during the same weeks, since drafting is the long pole
and both can be written by agents against sections 7.7, 9 and 10 of this
evaluation. R's rewrite of 0048 waits until F's registry shape is accepted,
because `norm`, `rrf` and `rerank` are its first rows. Nothing in the first
month breaks stored text, and by its end the agent has schema introspection,
reasons for empty results, cost estimates and parameterised paging without a
single rewrite.

### 11.6 Breaking first: the revised sequence

Sections 11.1 to 11.5 put the breaking stage last, the compatibility-
preserving order. With a pre-stable language (the shared expression RFC's own
words: "a better language wins over compatibility wherever the two conflict")
and a fleet of a few graphs whose stored queries one tool can rewrite, the
better order is the reverse: **one break, as early as possible, then every
addition on the final grammar.** What it buys is concrete. No construct is
implemented twice (old spelling plus new); no deprecation shims, lint
warnings or dual-parse windows are written and then deleted; the GQT corpus,
the skill, the docs and the fleet are rewritten once; and agents learn one
spelling. What it costs is that the grammar must be decided before most of
its semantics exist, so the break must land *slots* whose behaviour arrives
later. This section supersedes rule 1 of 11.4 and the first-month plan in
11.5; the dependency graph, the stage contents and the tracks stand.

#### The principle: break the grammar once, then only fill slots

A later change is non-breaking exactly when the grammar already has a place
for it. So the break is defined by a **freeze list**: every syntactic
decision that could otherwise force a second break. Everything on it ships in
the one breaking release, even where its semantics are a stub; everything
not on it is additive forever after, by construction.

| Frozen in the break | Decision | Semantics at break time |
|---|---|---|
| Declaration kinds | `query`, `mutation`, `view`, `contract`, `batch`, plus the schema kinds `node`, `edge`, `interface` in the same grammar; one file type, `.pg` accepted as the same grammar | `view`, `contract`, `batch` parse and are refused with a stable code ("reserved; not yet served") until their stage |
| Clause set and order | `match` → `retrieve`? → `return` → `having`? → `order`? → `limit`? → `returning`? (mutations: statements → `returning`?) | `retrieve` carries exactly today's power (`nearest`, `bm25`, two-arm `rrf`); `having` works immediately (one `Filter` over `Aggregate`); `returning` parses, refused until W1 |
| Block kinds in `match` | `not`, `exists`, `count`/`sum`/… blocks, `optional` | `optional` parses, refused until C |
| Traversal spelling | the triple stays (`$a e $b`, `<e>`); the first draft's arrow forms were withdrawn on 2026-10-30 (§13.3) | today's semantics |
| Call syntax | `name(args, opt: v)`, generic, resolved against a registry; `contains()`, `starts_with()`, `terms()` reserved as names; no function in the grammar | today's functions become registry rows; `terms()` refused until L |
| Reserved words | the logic set only: `and`, `or`, `not`, `is`, `null`, `in`; every clause and declaration keyword contextual (word-bounded, position-based, as today) | closed; the amendment's rule becomes a grammar test |
| Null | no literal in reads; `null` legal in assignment position only | today's rules plus the assignment case |
| Parameters | `limit $k`; `$branch: Branch`, `$at: Commit` typed parameter kinds; defaults `($k: I64 = 10)` | `limit $k` works; `Branch`/`Commit` kinds parse and bind |
| Annotations | `@ident(args)` on any declaration, retained as metadata; engine-known names typed (`@budget`, `@approximate`, `@satisfice`, `@if_commit`, `@example`, `@description`, `@instruction`) | known ones that are stubs refuse with a code |
| Meta-fields and catalogue names | `@id`, `@src`, `@dst`, `@type`, `@type_id`, `@distance`, `@score` reserved; `@Type`, `@Property`, `@Edge`, `@Interface`, `@Index`, `@Function`, `@View` names reserved | `@distance`/`@score` are the `retrieve` columns; the catalogue types ship in S |
| Type references | `[graph@]ns::Type` admitted by the grammar | the graph segment refused ("federation reserved") |
| Edition line | `gq 3;` optional first line | the compiler is edition 3; edition 2 parses for one release through the rewriter |
| Schema roles | `@valid(from, to)`, `Vector(n, distance=…)`, `@analyzed(...)`, `@embed` recipe arguments | `@valid` and `@analyzed` are SchemaIR features whose enforcement arrives with W1 and L; distance ships |
| Envelope | `graph_commit_id` as a map keyed by graph (one entry today), `notifications`, `why_not`, `complete`, `continuation`, `retrievals.recall_achieved`, `estimate` reserved | present and empty until their stage; HTTP contract 0.14 → 0.15, explain and bound-plan versions bumped once |

Two things are deliberately *not* on the list, because they are additive by
nature: new registry rows (functions, catalogue types, annotations) and new
plan nodes behind existing syntax.

#### The revised order

```text
M0 measure ─► B  the break (grammar RFC, rewriter, corpus and fleet rewrite; one release)
                   │
                   ├──► parallel, additive, on the final grammar:
                   │       S catalogue · N side channel · P paging · F function library
                   │       C closure (fills optional/view/having/collect slots)
                   │       L lexical (fills terms/@analyzed)  · R retrieve semantics (n arms, weights, norm, rerank)
                   │       W1 writes (fills returning/invalidate)  · W2 computed writes
                   │
                   ├──► I + Poly on the storage-upgrade clock
                   ├──► A permissions after C
                   └──► Fed after Poly and views
```

**M0** is unchanged and becomes more important: the instrument must run on
the *new* spelling before the break, against a parser that accepts it and a
mock executor, so that the one break is made on evidence (first-try validity
and repair turns on the frozen grammar) rather than on taste.

**B, the break**, is one release and one RFC ("GQ 3 grammar"), with four
deliverables: the grammar and type rules of the freeze list; a **rewriter**
(`omnigraph queries migrate`, the machine-applicable `suggestion` machinery
turned into a tool) that rewrites every edition-2 source mechanically
(`$p worksAt $c` → `$p -worksAt-> $c`, `order { nearest(…) }` → a `retrieve`
arm, `query` writes → `mutation`, infix `contains` → `contains()`); the GQT
corpus rewritten by that tool and re-run as the proof that the rewrite is
semantics-preserving; and the fleet's stored queries rewritten under `cluster
plan` with the same tool. Edition 2 keeps parsing for that one release so a
stored query that escaped the sweep is rewritten at boot with a warning rather
than quarantined; the release after removes the edition-2 grammar. The
registry records the edition from this release on, which is all the edition
mechanism the project needs until the next break, if there ever is one.

**Everything after B is additive** and runs as 11.2 and 11.3 describe, with
one difference: no stage carries a deprecation, and the stages that were
gated on E (F, L, R) are gated on B instead. R shrinks to semantics only
(n arms, weights, `norm`, thresholds already work because scores are columns,
`rerank`), since its clause shipped in B. C fills slots the grammar already
parses, so a stored query written on day one with an `optional { }` block
starts working the day C ships, with no edit.

#### What this changes in the rules

Rule 1 of 11.4 becomes: **break once, before anything is built on the old
spelling; after the break, only fill slots.** Rule 2 (enabler before
consumer) now reads B before everything. Rule 6 (the fleet is part of the
gate) moves from the end of the plan to its start: the rewriter and the
rewritten fleet are B's acceptance criterion, not X's.

#### Risks, and what covers them

- *A frozen slot turns out wrong.* The slots are small (a keyword, a clause
  position, a reserved name) and each is backed by §9's semantics, §8's
  evidence and the instrument's run on the new spelling. The remaining risk
  is the price of never breaking again.
- *Grammar designed ahead of semantics drifts.* Each slot's refusal code
  names the stage that fills it, and the GQT case that will go green is
  written at B as an `expect error` case; the stage flips it.
- *The rewriter cannot rewrite something.* Then the break has found a real
  ambiguity in the old grammar; fix the rewriter, never widen the window.
- *The break lands before the metrics prove the spelling.* That is what M0 on
  the new spelling is for; if the instrument shows first-try validity down
  and repair turns not compensating, the freeze list is edited before B, not
  after.

#### The first month, revised

M0 on both spellings (the current grammar, and the frozen grammar through a
parser-only build), the GQ 3 grammar RFC, and the rewriter, all at once; the
GQT corpus rewritten by the rewriter as the RFC's evidence section. S, N and
P start the day the grammar RFC is accepted, since all three are additive and
independent of it, and ship in the same release as B so the first edition-3
release already gives an agent schema introspection, reasons for empty
results, cost estimates and parameterised paging. F, C, L, R and W follow on
the final grammar with nothing to deprecate.

---

## 12. TypeDB and TypeQL 3

Added 2026-10-29, after the grammar RFC was drafted, to answer one question:
does TypeDB change the design? Facts come from the TypeQL 3 grammar
(`rust/parser/typeql.pest` on `typedb/typeql`), the TypeQL reference at
typedb.com/docs, the GitHub release notes (TypeDB 3.0.0 GA 2024-12-20; 3.14.1
on 2026-10-22), TypeDB's `llms-full.txt`, and twelve dated TypeDB blog posts;
the Dorn and Pribadi paper ("TypeQL: A Type-Theoretic & Polymorphic Query
Language", Proc. ACM Manag. Data 2(2), SIGMOD 2024, DOI 10.1145/3651611) is
behind ACM's bot check, so only its abstract and the documentation's statement
of its model are cited, and its formal results are not claimed here.

### 12.1 What TypeDB is

TypeDB is a schema-first database over the PERA model, "Polymorphic
Entity-Relation-Attribute", which its documentation calls "an extension of the
ER model with dependent typing". Three kinds, each declared explicitly
(`entity person`, `relation friendship`, `attribute name`), single-inheritance
`sub` within a kind, `@abstract`. A relation declares roles (`relates employer,
relates employee`), is n-ary (zero or more players per role), and can itself
play a role in another relation. Attributes are shared value instances: "two
attributes are considered equal if they have the same associate value and have
the same (origin) type", ownerless attributes are removed unless
`@independent`, and attributes cannot own attributes. Annotations are a closed
grammar list: `@key`, `@unique`, `@card(N..M)`, `@range`, `@regex`, `@values`,
`@abstract`, `@independent`, `@doc`, `@meta`, and three the docs mark "planned
feature and not yet available" (`@cascade`, `@subkey`, `@distinct`).

TypeQL 3 is a **pipeline**: `with fun` preambles, an optional `given` stage of
typed input rows (3.12.0), one or more of `match`, `insert`, `put`, `update`,
`delete`, `select`, `sort`, `distinct`, `offset`, `limit`, `require`,
`reduce`, an optional terminal `fetch`, an optional `end;`. The semantics are
a row stream: "each stage receives the stream of rows output by the previous
stage, and only the variables in those rows are available to it"; the first
stage gets the unit row; a `match` cross-products its input rows with its
matches; `reduce … groupby` keeps only reduced and grouping variables and "need
not be the last stage", so `reduce` followed by `match` is the HAVING; `insert`
after other stages runs per row and emits "input rows extended with the newly
inserted values", so `match → reduce → match → insert` and `put → match →
insert` are documented pipelines. Answers are rows, or JSON documents from
`fetch`, which is terminal and builds nested documents from `$x.A`,
`[ $x.A ]`, `{ $x.* }`, expressions, function calls, and bracketed
sub-pipelines (`[ match … fetch { … } ]`, `( match … return count; )`).

**Functions replaced rules in 3.0.** `fun name($a: T) -> { T, U }` returns a
stream, `-> T, U` at most one row; a body is a read pipeline ending in
`return { … }`, `return first …`, `return count;`, `return check;`. Calls are
`let $x in f(…)` for streams, `let $x = f(…)` for singles, and scalar calls
inside expressions. Recursion is allowed through stream functions, with
"single return functions may not call themselves recursively" (3.5.0), a
documented stratification principle for negation, suspension to break cycles,
and a planner that "just sets the recursive call cost to 1". Names are
globally unique with no overloading. `with fun …` declares query-scoped
functions.

**Patterns**: conjunction, `{ } or { }` (a true logical or, branches
unordered), `not { }` (nestable, giving "a form of for all"), and `try { }`,
the optional block (read side 3.4.4, 2025-08-05; write side 3.7.0). Type
variables stand in every type position: `$x isa $t`, `$t sub person`, `$o owns
email`, `user plays $r`; `isa!`/`sub!` are the exact forms; `is` is identity.
Expressions have `==`, `!=`, `<`…, `contains` (case-insensitive) and `like`
(Rust regex), arithmetic with precedence, `let`. Lists and structs are in the
grammar and announced for 3.0 in September 2024, and the docs on 2026-10-29
still say "planned feature and not yet available"; the `list` reducer is
"unimplemented" in TypeQL 3.8.2.

**Validation**: type inference runs before execution, "if no valid
combinations of types can be inferred for the variables in body pattern of
the stage the query will be rejected"; expression types resolve at compile
time; errors carry a code (`[INF11] Type-inference was unable to find
compatible types…`, `[TQL03]`); an `analyze` HTTP endpoint (3.5.0) returns the
type-annotated structure without executing; `typeql-check` validates syntax
offline. Transactions are `read`, `write`, `schema` with snapshot isolation.
**There is no branching, versioning or time travel**, no vector or full-text
search in any release note or grammar rule (an alpha `cosine_similarity_search`
preview for invited testers, 2026-08-18; the features page marks vector
indexing "in development"), and string search is `contains` and `like`.

**The 2 to 3 break** had no query-migration tool: export the 2.x data,
"manually update your schema definition file", import, then "update your
queries using from 2.x syntax to 3.x syntax". Rules were removed, `get` became
`select`, `group`/`aggregate` became `reduce`, `?` value variables became `$`
with `let`, `long` became `integer`, root types went, `$r (role: $p) isa T`
changed meaning. Inside 3.x, `within` was renamed `groupby` in TypeQL 3.0.1,
a patch release, and `end;` arrived in 3.2.0 because "without `end`, TypeQL
parses the longest valid query it can".

**On agents**, TypeDB's own measurements (blog, 2026-09-17, 3,024 runs over
Reactome with Claude Sonnet 5 and DeepSeek V4 Pro): bare TypeQL 24.8% against
SQL 62.4%; with a skill, examples and four retries TypeQL reaches 92%, and
"in-context learning can almost entirely compensate for baseline training
data deficit". A companion post (2026-10-14) reports first attempts: SQL 82%
correct and 17% silently wrong, TypeDB 75% correct, 23% a visible error and 2%
silently wrong. The docs ship `llms-full.txt`, a skill, `typeql-check`, an
MCP server, and a `given` stage sold as the anti-injection parameterisation.

### 12.2 TypeDB against the axes, and against GQ 3

| Axis | TypeQL 3 | GQ 3 (RFC) | Reading |
|---|---|---|---|
| Closure (2.2) | Linear: every stage consumes the previous stage's rows, any order, kinds repeat; functions are named read pipelines | Non-closed kernel plus four reserved forms: `view` (a named pattern with typed columns, inlined), `optional`, block aggregates as values, `having` | Same demand, two mechanisms; see 12.3 |
| Polymorphism (2.1, 10.4) | Type variables in every type position; `isa` includes subtypes, `isa!` does not; parametric queries "valid over any schema" | Interface bindings, `@type`/`@type_id`, `is`; catalogue types `@Type` for the schema as data | TypeQL is more uniform; GQ's split (bind by interface, read the schema by catalogue) is the same power with two spellings |
| Direction (2.6) | None: relation ends are roles | Triple, source on the left, checked against the schema | §13 |
| Optional (2.5) | `try { }`; unbound optionals fail later stages; `require` filters them | `optional { }` slot | Same form, same rule; TypeDB's "may only occur in a single try-block" is the scoping rule the closure RFC needs |
| Aggregation (2.8) | `reduce … groupby`, then any stage | `return` with aggregates, then `having` | GQ's is one fixed shape; TypeQL's allows re-matching on the reduced value, which `view` gives GQ |
| Result shape (2.15) | Rows, or nested JSON from terminal `fetch` with sub-pipelines | Rows with node objects; a partial object projection and a nested block in `return` are additive (parse errors today) | No slot needed; the nested-document demand (#649) has a precedent |
| Writes (2.11) | Pipelines mix reads and writes; `insert` emits augmented rows; `put` is match-or-insert of the whole pattern; `update` only for cardinality-1 capabilities | Reads and writes separate; keyed insert as upsert; pattern-scoped writes with `returning` | GQ keeps the wall; `returning` covers the read-after-write TypeDB gets from a trailing `match` |
| Parameters | `given $u: username, $phone: string?;` typed input rows | scalar typed parameters, `limit $k` | A row-valued parameter is additive in GQ (a type in parameter position); worth an RFC when batch loads of lookups appear |
| Reuse (2.13) | `define fun`, `with fun` query-scoped; no overloading; `@doc`/`@meta` | `view`, `@Function` registry, generic annotations | Same shape; adopt the no-overloading rule for the registry |
| Refusal (2.17) | Codes, pre-execution type inference, `analyze`, `typeql-check` | Four-field diagnostics with fixes, explain, lint | Equal; GQ's `fix` field is the extra |
| Time and branches (2.12) | None | Branches, commits, snapshots, change feed | GQ's defining advantage stays |
| Retrieval (2.10) | None shipped | `retrieve` arms over `nearest`, `bm25`, `rrf` | Not comparable |
| Recursion | Stream functions recurse, cost 1, suspension, crash fixes through 3.14 | Refused without a budget | TypeDB's release notes are the evidence for the refusal |

### 12.3 Pipelines versus the non-closed kernel

TypeQL's pipeline is the cleanest statement of the alternative GQ did not take,
so it deserves the argument in full. A pipeline makes the language closed by
construction: `reduce` then `match` is HAVING, `match` then `insert` then
`match` is read-after-write, and every stage is a function from rows to rows,
so the agent never needs a second construct for "do something with the result
of that". What it costs:

1. **The query's text order is the plan's order.** "Sorting requires the
   server to fetch all results, sort them, then proceed"; a `match` after a
   `reduce` runs after it; a `limit` placed early cuts early. The optimizer
   orders statements *inside* a `match`, not stages. GQ's one shape
   (`match → retrieve → return → having → order → limit`) is the shape the
   planner reorders freely under a declared result; that is what plan truth
   and the replay contract (§2.17, RFC 0047) rest on. A stage pipeline moves
   the planner's job into the author's hands, which is the SQL `WITH` trade
   GQ refused in §4.4.
2. **Rows multiply.** A stage's output is its input "augmented": two input
   rows times two matches is four rows. TypeQL documents this and gives
   `select`, `distinct` and `$_` to manage it. GQ's pattern conjunction is one
   join with one result schema; `optional`, `view` and the block value add the
   closure cases without a cross product the author has to remember to prune.
3. **Reads and writes mix.** A `match → insert → match` pipeline is coherent
   under snapshot isolation inside one transaction, which TypeDB has and GQ
   does not want: GQ's mutation is one commit, with the D2 constructive-or-
   destructive rule and a `returning` slot for what the agent needs back.

Where TypeQL is right and GQ 3 already agrees: functions as the unit of
reuse, declared with a typed signature and called by binding (`let $x in
f($y)` is `$x: Name($y)`); `try` as the optional form with "may only occur in
a single try-block" scoping; reductions followed by a filter (`having`);
no overloading; query-scoped function declarations (`with fun`), which GQ's
`view` inside a `batch` covers. Where TypeQL is right and GQ 3 does not yet
say so: a **type variable in binding position** (`$x: $t` with `$t` ranging
over node types, with `$t` itself returnable) is the parametric query the
polymorphism review (§10.4) asked for as `$x: *`; it is additive (a variable in
`type_ref` position is a parse error today), so it needs no slot, and the
polymorphism RFC should take the TypeQL form rather than invent a wildcard.
Likewise a row-valued parameter (`given`) is additive.

**Verdict on closure**: TypeDB does not change the design. It supplies the
best-documented version of the mechanism §9.8 rejected, and the reasons to
reject it (plan truth, row multiplication, read-write mixing) are the ones its
own documentation states as behaviour.

### 12.4 Where TypeDB confirms the RFC

- **Slots that refuse loudly.** TypeQL's grammar carries lists, structs,
  `@cascade`, `@subkey` and `@distinct`; its documentation says "not yet
  available in TypeDB"; TypeQL 3.8.2 adds "a Collector variant of Reducer to
  accommodate the unimplemented list reducer". That is the freeze list's
  strategy, running in production for two years: parse the form, refuse it
  with a message, fill it when the semantics land.
- **One break, done manually, is the alternative the rewriter avoids.** The
  2 to 3 migration is export, hand-rewritten schema, import, hand-rewritten
  queries. TypeDB could afford it with rules removed and the data model
  changed; GQ's break changes text only, which is why `queries migrate` with
  IR equality is cheap and worth building.
- **Breaks inside a major are what the edition line prevents.** `within`
  became `groupby` in a patch release, `end;` changed how consecutive
  pipelines parse in a minor, and TypeDB's own LLM summary disagrees with its
  grammar on `=` versus `==`. An edition number in the file, a closed reserved
  set and one spelling per meaning (§14) are the answers.
- **Retries beat first-try accuracy, and visible errors beat silent ones.**
  TypeDB's 24.8% to 92% with a skill plus four retries, and 23% visible versus
  2% silent wrong, are the same shape as §8.3 and RFC 0047's 45-of-45 with
  8 refusals: optimise the diagnostic and the loop, and spend the prior on
  the few spellings that matter.
- **Annotations as documentation for agents.** `@doc` and `@meta` on types,
  capabilities and functions (3.12.0) are `@description`, `@instruction` and
  the generic `@name(args)`.
- **Validation before execution with a structured answer** (`analyze`) is
  GQ's lint and explain; the `fix` field remains GQ's edge.

### 12.5 What GQ should take from TypeDB, and what to resist

Take: the no-overloading rule for the function registry; the type-variable
binding as the polymorphism RFC's discovery form; the single-origin rule for
optional variables; a row-valued parameter as a later additive RFC; the
practice of shipping a grammar validator (`typeql-check`) as a standalone
tool, which `omnigraph lint` already is and the skill should name.

Resist: roles instead of direction for binary edges (§13); n-ary relations
(a reified node with typed edges says the same thing with the planner's
existing join, and every comparator in §3 is binary); attributes as shared
value instances (a storage model, not a language gain, and it is why TypeDB
needs `@independent` and the dangling-attribute rule); recursion through
functions without a budget (TypeDB's own fixes through 3.14 for suspended
recursive calls are the cost); pipelines (12.3).

## 13. Expressing direction

Added 2026-10-29. Facts from the Cypher 25 manual and openCypher 9, the
opengql ANTLR grammar generated from the ISO 39075 BNF, Deutsch et al.
"Graph Pattern Matching in GQL and SQL/PGQ" (arXiv 2112.06217), Oracle 23
`GRAPH_TABLE` and DuckPGQ's tests, the TinkerPop reference, SPARQL 1.1,
Datomic's query and pull references, Soufflé's relations page, the TypeQL 3
grammar and reference, the GraphQL specification, LangChain's
`CypherQueryCorrector` source and its origin pull request, the Cypher
direction competition, three Neo4j developer-blog posts, and CypherBench
(arXiv 2412.18702, Figure 6 read from the published image).

### 13.1 Three ways to say which way an edge goes

Every language that has edges answers the same three questions: where does the
orientation of a binary relation live, how is "either way" written, and what
happens when the author writes it the wrong way round.

| Language | Where orientation lives | Written form | Either way | Wrong way round |
|---|---|---|---|---|
| Cypher 25 | On each stored relationship ("all relationships have a direction"); no schema | `-[r]->`, `<-[r]-` | `-[r]-` matches "relationships of any direction"; a bound undirected pattern "in both directions", returning both bindings | No match: an empty result, no error |
| ISO GQL | Schema: `edgeKind : DIRECTED \| UNDIRECTED` with source and destination node types | seven forms: `<-[ ]-`, `~[ ]~`, `-[ ]->`, `<~[ ]~`, `~[ ]~>`, `<-[ ]->`, `-[ ]-`, plus abbreviations | `-[ ]-` is "left, undirected or right"; `<-[ ]->` either orientation but not undirected; `~[ ]~` undirected edges only | Unspecified in the sources fetched |
| SQL/PGQ (Oracle, DuckPGQ) | Edge table `SOURCE KEY … DESTINATION KEY …` | `-[ ]->`, `<-[ ]-` | `<-[ ]->` or `-[ ]-`: "any directed edge (right or left)", a `UNION ALL` of both orientations in DuckPGQ's test | Matches the other stored orientation; no error documented |
| Gremlin | Per edge | `out()`, `in()`, `outE()`, `inE()`, `outV()`, `inV()` | `both()`, `bothE()`, `otherV()` | No traversers; no error |
| SPARQL | The triple's order, subject to object | `?s p ?o`; inverse path `^p` "object to subject" | `(p \| ^p)` | No match |
| Datomic | The ref attribute's orientation, entity to value | `[?e :a ?v]`; reverse in pull as `:ns/_attr` | Two clauses | No match |
| Soufflé, Datalog | Argument position | `Edge(x, y)` | A symmetry rule `E(a,b) :- E(b,a)` | A different relation |
| GraphQL | The schema's field | a field | a second field, by convention only (the spec says nothing) | No such field: a validation error |
| TypeQL 3 | Nowhere: a relation has roles, `relates employer, relates employee`; `links (employee: $p, employer: $c)` | the role name | `rel ($a, $b)` with roles inferred; a symmetric relation is one role played twice (`relates friend @card(2)`) | A role the player's type cannot play: a type-inference refusal |
| GQ 2 (today) | The schema: `edge WorksAt: Person -> Company`; position in the triple | `$p worksAt $c`, source on the left | `$p <knows> $f`, same-type edges only (`T22`) | `T5` when the endpoint types do not fit; for a same-type edge, the other orientation |

The table has three designs, not nine. **Arrow-written** (Cypher, GQL, PGQ,
Gremlin, SPARQL's `^`): the author writes the orientation every time, the
schema may or may not know it, and a wrong arrow is a silent empty result.
**Schema-decided** (Datomic, GraphQL, Soufflé, GQ today): the orientation is a
fact about the type; the author positions the ends, and the compiler knows
which way the edge runs. **Role-named** (TypeQL): the relation has no
orientation at all; its ends have names, and a binary relation is the special
case of an n-ary one.

### 13.2 What the agent evidence says

Direction is a measured error class, and it is the one error class the
ecosystem has decided to fix by machine.

- CypherBench (gpt-4o and llama3.1-8b, 50 sampled failures each, multiple
  errors per instance): "Reversed Direction" is among the categories that
  "frequently occur in both models". Read off Figure 6, it is 9 of 55 error
  marks for gpt-4o and 13 of 71 for llama3.1-8b, the second-largest category
  for both after the pattern-level ones, ahead of syntax errors for gpt-4o.
- Neo4j's own prompt for GPT-4 carries "Relationship point from source to
  target nodes" and "Make sure to respect relationship types and directions",
  and the post concludes "sometimes it messes up the relationship direction
  (not frequently, though)".
- LangChain's `GraphCypherQAChain(validate_cypher=True)` runs
  `CypherQueryCorrector`, "used to correct relationship direction in generated
  Cypher statements", built from the graph's structured schema and copied
  from "the winner's submission to the Cypher competition" that Neo4j
  sponsored in 2023 because LLMs "can sometimes mess up the direction of the
  relationship". The author's claim is the important one: "the direction of
  the relationship can be deterministically [fixed] after the LLM generates a
  Cypher statement based on the provided schema". Its limits are also stated:
  "we only catch if the relationship direction doesn't fit the schema, we
  don't catch anything else", and a path that cannot be made to fit returns
  an empty query.
- Neo4j's iterative-refinement study lists "Rule-Based Relation Direction
  Correction" as a verification class and finds it "has low capacity (i.e.,
  only fixing relation direction)"; CyVer ships a "Wrong direction in path"
  test. Ozsoy et al. and SM3 do not report direction at all.

Two conclusions follow. First, when the two endpoint types differ, the arrow
carries **no information the schema does not already have**: a corrector can
rewrite it deterministically, and does. Second, when the endpoint types are
the same (`Person knows Person`, `Company subsidiaryOf Company`), the arrow is
the **only** carrier of meaning, and this is exactly where the models fail:
CypherBench's example is `subsidiaryOf` written from the parent. No
post-processor can fix that case, because both orientations fit the schema.

### 13.3 What follows for GQ 3

The RFC's first draft made direction visible with an arrow
(`$p -worksAt-> $c`) for the reader and for the model's prior, and made `T5`
check the written arrow against the schema. The evidence above and the
one-spelling test of §14 reversed that on 2026-10-30. Four rules remain.

1. **The triple stays, source on the left, and gains no arrowhead.** The
   arrowhead carries no information: when the endpoint types differ the
   schema fixes the orientation (13.2's first conclusion), and when they are
   the same the position does, in `$a subsidiaryOf $b` exactly as in
   `$a -subsidiaryOf-> $b`. It is a decoration on the one spelling, and its
   two justifications did not survive: `$p worksAt $c` reads in
   source-to-target order as a sentence does, and the only in-context
   measurement GQ has is on the triple (RFC 0047: 45 of 45, 37 first tries
   valid, no direction error) while CypherBench shows the arrow form does not
   prevent reversals. Dropping it also removes the break's largest rewrite,
   since every stored query has a traversal, and the arithmetic-minus rule.
   The arrow forms stay in the grammar as a recognizer only: `$p -worksAt->
   $c`, `$c <-worksAt- $p` and `$a -knows- $b` are parse errors whose fix
   prints the triple with the ends in schema order.
2. **A triple written against the schema is a typed error with a
   machine-applicable swap**, never an empty result and never a silent
   rewrite. `$c: Company worksAt $p` is `T5` with `fix` = `$p worksAt $c`.
   This is LangChain's corrector moved into the compiler, where it belongs,
   with the two differences the evidence asks for: it is loud (the agent
   learns in one turn, which Olausson's result in §8.3 says is worth more
   than the edit) and it never guesses when both orientations fit.
3. **Same-type edges are where direction is meaning, and position carries
   it.** `$a subsidiaryOf $b` means `$a` is the subsidiary. No rule can check
   that against the schema; what GQ can do is make the schema say which end
   is which in words, so the error is visible: `@description` on the edge
   today, and the skill's schema card prints `Company subsidiaryOf Company
   (subsidiary -> parent)`.
4. **`<e>` means "whichever orientation the endpoint types admit".** For a
   same-type edge that is both orientations, deduplicated, as today. For an
   alternation or wildcard over edges of differing orientation
   (`$p: Person <owns | ownedBy> $d: Doc`, the issue 659 case, which the
   directed triple resolves per member today) it is each member's declared
   orientation. The only refusal left is the genuinely ambiguous one: an
   asymmetric edge whose endpoints are not typed well enough to fix the
   orientation. Today's `T22` ("undirected traversal needs a
   same-endpoint-type edge") narrows to that case. The bound edge's `@src`
   and `@dst` keep reporting the stored orientation, which is why they are
   not redundant with the pattern's ends.

What GQ 3 does not take from TypeQL is roles. For a binary edge, `employee`
and `employer` are documentation of `src` and `dst`, and GQ puts
documentation in `@description`; for an n-ary relation, which GQ does not
have (§12.5), roles are the right design and a reified node with typed
edges is GQ's spelling of it. A role name on an edge declaration
(`edge WorksAt: employee Person -> employer Company`) would be additive if a
later RFC wanted it, since a word before a type in that position is a parse
error today.

## 14. One way to say it

Added 2026-10-29 at the maintainer's request: an overall requirement that
there be, ideally, one way of expressing something, in the spirit of Go.

### 14.1 The principle, as Go states it

Go's FAQ, on why there is no `?:`: "the language's designers had seen the
operation used too often to create impenetrably complex expressions. The
`if-else` form, although longer, is unquestionably clearer. A language needs
only one conditional control flow construct." On layout: "the advantages of a
single, programmatically mandated format for all Go programs greatly outweigh
any perceived disadvantages of the particular style", and the proverb
"Gofmt's style is no one's favorite, yet gofmt is everyone's favorite". On
design: "keep the concepts orthogonal … Orthogonality makes it easier to
understand what happens when things combine", and Pike's "Less is
exponentially more". Go is not absolutist (`var x = 1` and `x := 1` both
exist) but it has two mechanisms, and they are the two GQ should adopt:

- **The grammar has one form for one meaning.** A respelling that adds no
  meaning is not an alternative, it is absent, and the compiler names the one
  form when it sees the other.
- **The formatter canonicalises what falls out of general rules.** `gofmt -s`
  rewrites `s[a:len(s)]` to `s[a:]`: a form the grammar cannot forbid,
  because it is an instance of a general construct, is rewritten to its
  canonical instance by a tool everyone runs.

For an agent author the principle has a measured value: §8.3's finding that
retrieved examples erase the unfamiliar-spelling gap only works if there is
one spelling to retrieve, and the RFC's own motivation ("a second spelling
for the same operation costs it on every query it reads") is this principle
applied to deprecation shims.

### 14.2 The test applied to the freeze list

Every pair of forms in GQ 3 that could mean the same thing, and the ruling.

| Pair | Same meaning? | Ruling |
|---|---|---|
| `$a knows $b` and a reversed arrow `$b <-knows- $a` | Yes, exactly | Grammar: the triple only, source on the left; every arrow form is a parse error with the swapped triple as its fix (§13.3) |
| `$a knows $b` (triple) and `$a -knows-> $b` | Yes, and the arrowhead adds nothing | Grammar: triple only, decided 2026-10-30 (§13.3); the first draft had chosen the arrow |
| `x contains y` and `contains(x, y)` | Yes | Grammar: the call only; already in the RFC |
| `rrf(a, b, 60)` and `rrf(a, b, k: 60)` | Yes | Grammar: named option only; already in the RFC |
| `exists { P }` and `count { P } > 0`; `not { P }` and `count { P } = 0` | Yes | The grammar cannot forbid the count form without forbidding `count { } > 2`. Formatter: `omnigraph queries fmt` (the rewriter's second job) canonicalises the two thresholds to `exists`/`not`, and lint names it. The grammar keeps `exists` because GQL has only `EXISTS` and the planner's semi-join is the honest plan |
| `$x.@type = "Person"` and `$x is Person` (polymorphism RFC) | Yes, once `is` exists | The polymorphism RFC owns `is`; the formatter canonicalises the string comparison to it, so one way survives the addition |
| `$x: Interface` and a wildcard `$x: *` | Yes | Grammar: no wildcard; §12.3's type-variable binding is the discovery form and it is not a respelling (it binds the type) |
| `return { $a.slug, $a.name }` and `return { $a { slug, name } }` | No: flat columns versus one nested object | Both stay; different result shapes |
| `$e.@src` and the left variable of the pattern | No under rule 13.3.4: `@src` reports stored orientation, the pattern's end is the written one | Both stay |
| `limit 10` and `limit $k` | No: a literal and a parameter | Both stay |
| `count(distinct $x)` and `return distinct` | No: a column's domain versus the row set | Both stay |
| `having { }` and a filter in `match` | No: above and below the aggregate | Both stay |
| `optional { }` and a `view` bound with an outer join | No: `view` has no outer-join form | One way |
| `@if_commit($head)` and the request's precondition field | Same fact, two surfaces | The annotation is the declaration; the request field is its transport. The writes RFC should say the field is derived from the annotation, never a second source |
| positional and named call arguments | No: one argument has one form, positional if required, named if an option | Rule for the registry: a function declares which; a caller cannot choose |

Two cautionary examples from §12 show what skipping the test costs. TypeQL
lets a single-return function be consumed as `let $x = f()` or `let $x in
f()` ("a general `in` used for streamed results is also allowed"), and its
own LLM summary spells equality `=` where the grammar says `==`; its relation
patterns have a full form (`links (…)`) and a shorthand, both documented. Each
is a second form an agent has to recognise.

### 14.3 The rule, for the RFC

A form enters the grammar only if no existing form has its meaning; a form
that falls out of a general construct and has a canonical instance is
rewritten to that instance by the formatter and named by lint; the
diagnostics know every respelling an agent is likely to write (the triple,
the arrow forms, the infix operators, GraphQL's `mutation` for a read) and
print the one form as a machine-applicable fix. The freeze list is checked
against the rule once, in this RFC, and every later RFC's "Alternatives"
section answers it for the form it adds.

## 15. Sources

Sections 12 to 14 (added 2026-10-29) read, on that day: the TypeQL 3 grammar
(`rust/parser/typeql.pest`, `typedb/typeql` master), the TypeQL reference at
typedb.com/docs (data model, pipelines and every stage page, functions,
patterns, optionals, annotations, fetch, schema, transactions, the 2-vs-3
diff and process pages, `llms-full.txt`), the GitHub release notes of
`typedb/typedb` and `typedb/typeql` from 3.0.0-alpha-1 to 3.14.1, twelve
TypeDB blog posts dated 2026-01-07 to 2026-10-26, Crossref for Dorn and
Pribadi (DOI 10.1145/3651611; the paper's body is behind ACM's bot check and
is not cited for content); the Cypher 25 manual (patterns, MATCH, CREATE,
scalar functions, structural types, grammar appendix, GQL conformance),
openCypher 9, the opengql `GQL.g4` grammar, Deutsch et al. arXiv 2112.06217,
Libkin's LDBC slides, Spanner and Fabric GQL pattern pages, Oracle 23.2
`GRAPH_TABLE`, DuckPGQ's README and `undirected_edges.test`, the TinkerPop
reference, SPARQL 1.1, Datomic query and pull references, Soufflé relations,
the GraphQL October 2021 specification, Elasticsearch `join` and `nested`,
Vespa parent-child; LangChain's `cypher_utils.py` and `cypher.py`, pull
request langchain-ai/langchain#11078, the `cypher-direction-competition`
README, Neo4j developer-blog posts (ChatGPT-4 on any schema, iterative
refinement for Text2Cypher, model struggles, CyVer), CypherBench arXiv
2412.18702 with its Figure 6 image, Ozsoy et al. (ACL Anthology
2025.genaik-1.11), SM3 arXiv 2411.05521; the Go FAQ (go.dev/doc/faq), the Go
proverbs, and Pike's "Less is exponentially more" (2012). Items the sheets
could not confirm are marked in the text (the SQL/PGQ `ANY DIRECTED`
keywords, GQL's wrong-arrow behaviour, CyDRA's categories).

Omnigraph, at `ea895066`: `crates/omnigraph-compiler/src/query/query.pest`,
`src/schema/schema.pest`, `src/query/codes.rs`; `docs/user/queries/*`,
`docs/user/schema/index.md`, `docs/user/search/*`, `docs/user/mutations/*`,
`docs/user/branching/index.md`; RFCs 0041, 0045, 0047, 0055,
2026-09-14 (compatibility surfaces), 2026-09-16 (session settings),
2026-09-24 (shared expression model), 2026-09-30 (typed edge alternation),
2026-10-15 (bulk load), 2026-10-16 (engine catalogue); `docs/dev/execution.md`,
`docs/dev/systems.md`; `skills/omnigraph/references/queries.md`,
`schema-design.md`; issues #485, #563, #565, #648, #649, #658, #721, #749,
#758, #917, #1050, #1057, #1058, #1165, #1191; the GQT corpus
(`crates/omnigraph-gqt/cases/`, 425 files).

Comparators, primary documentation read on 2026-10-28: PostgreSQL 18
(queries-with, functions-aggregate, functions-window, sql-merge, sql-prepare,
errcodes), pgvector README, DuckDB VSS and DuckPGQ, Oracle 23 `GRAPH_TABLE`,
Spanner Graph ISO conformance; ISO/IEC 39075:2024 via the opengql ANTLR
grammar, Neo4j's GQL conformance appendix and Spanner's statement reference;
Neo4j Cypher 25 manual (clauses, patterns, subqueries, `SEARCH`, aggregating
functions, status codes, the OPTIONAL MATCH blog); GraphQL draft spec and
graphql.org learn pages, Relay connections spec, GAP-49; Datomic query/pull/
schema/time-filter references, Soufflé rules/aggregates/types, CozoDB queries/
aggregations/vector/timetravel/execution, Rel cheatsheet, Logica, Dedalus
(EECS-2009-173); Elasticsearch 9.x Query DSL (bool, knn, nested, join,
retrievers, RRF, paginate, sort, collapse) and ES|QL (syntax, limitations,
stats, lookup-join, fuse, fork, rerank); LanceDB docs (search, filtering,
hybrid, reranking, indexing, versioning, branching, schema, update) and
lance.org (transactions, tags and branches, DataFusion); Vespa docs (query
language reference, grouping, ranking, phased ranking, rank features, schema
reference, nearest-neighbor guides, document-v1, query profiles, consistency).
Facts the sources could not confirm are marked *recalled* in §3.

Literature (§8), read on 2026-10-28 and confirmed against arXiv, ACL Anthology,
Crossref or the venue's proceedings; full citations are in the entries. The
annotated bibliographies the section condenses were produced by three
research passes whose dropped candidates (unconfirmable or outside the source
policy: CypherBench, FeedbackEval, blog-only language comparisons) are not
cited. Two venue caveats stand: Text2Cypher is a COLING workshop paper and
"The Death of Schema Linking?" a NeurIPS workshop oral; both are included for
being the only direct measurements of their question. The BAIR post
"Intelligence is Free, Now What?" is a perspective, not a paper, and is cited
as such.
