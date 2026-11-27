# Upgrading queries to GQ 3

GQ 3 replaces the query language. This release parses GQ 3 only: a source
declares no language version, and there is no older parser. A source written
for an earlier release fails to compile like any other invalid source, and
every older form that GQ 3 refuses prints its fix, as text and as byte-range
edits ([Diagnostics](diagnostics.md)). `omnigraph lint --fix` applies those fixes
to a file, a directory or a cluster configuration's stored queries
([Lint](lint.md#fix)):

```bash
omnigraph lint --fix --query queries/ --schema schema.pg --dry-run --json
```

It leaves a file unchanged at a refusal with no fix, and at a fix printed for
review unless `--review-fixes` is given; finish those from the map below. It
also leaves a keyed `insert` that the older release meant as `replace`, since
that text still compiles (the map's `insert` row).

## Stored queries at upgrade

An older server cannot load GQ 3 sources, and this release cannot load
older ones, so a deployment's stored queries change in the same step as the
binary:

1. **Validate with the new binary first.** Before stopping anything, run the
   new CLI against the applied cluster: `omnigraph queries validate --target
   <cluster-root>` (add `--json` for the structured form). It reads the
   applied registry and schemas and reports every stored query that fails,
   parse and type refusals together, each with its code, position, fix and
   byte-range edits; byte offsets count into the stored query's own source.
2. **Rewrite the failing queries from their printed fixes.** Run
   `omnigraph lint --fix --config <bundle>` on the configuration directory
   your `cluster.yaml` is in, finish what it leaves by hand, and check the
   bundle with the new binary (`omnigraph cluster validate --config
   <bundle>`).
3. **Apply the rewritten registry with the binary switch.** Stop the old
   server, apply the bundle directly with the new CLI (`omnigraph cluster
   apply --target <cluster-root> --config <bundle>`; direct apply requires
   stopped serving), then start the new server. The old server never reads
   a GQ 3 source. The new CLI and server report `gq_language` `3.0`
   (`omnigraph version`, `GET /healthz`); an old one reports none
   ([Verify](../install.md#verify)).

A stored query that still fails to compile does not stop its graph. Each
declaration of a stored file compiles on its own: one the new binary refuses
is unavailable with its own diagnostic, placed in its file; one that calls an
unavailable declaration of its file is unavailable naming it; the others in
the file serve. The new server serves the graph with those queries alone
unavailable: `GET
/graphs/{graph_id}/queries`, `omnigraph queries list` and the MCP `queries`
tool list it under `unavailable` with its diagnostic, `GET /graphs` reports
it per graph and `GET /readyz` counts it, and invoking it answers HTTP 409
`stored_query_unavailable` carrying the same diagnostic. The boot log names
it as well:

```text
stored query unavailable: error[CODE] at line L, column C: MESSAGE; fix: FIX; edits: bytes S..E -> "TEXT"
```

Fix the source and apply it through the running server. An operator who
wants the whole graph refused instead sets `OMNIGRAPH_STRICT_STORED_QUERIES=1`:
the graph is then blocked with `invalid_stored_queries` when any declaration
of a stored file fails, as before this release, and with
`--require-all-graphs` the server refuses to start.

## The respelling map

Each row is an older form, the GQ 3 form, and the refusal that prints the
fix.

| Older form | GQ 3 | Code |
|---|---|---|
| `mutation name(…) { … }` | `query name(…) { … }` | `Q009` |
| a filter clause in `match`, `match { $p: Person  $p.age > 30 }` | a conjunct of the `where` after the braces, in written order, joined by `and`: `match { $p: Person } where $p.age > 30` | `Q017` |
| `$x: T { p: v }` | `$x: T` and the conjunct `$x.p = v`, leading the `where`; over a list property, the next pass's `"v" in $x.p` | `Q010`, then `T7` |
| elements, `return` items, order keys, arguments or assignments separated by whitespace | commas | `Q016` |
| `not { P }` | `not exists { P }` in `where` | `Q012` |
| `not { φ }`, a block holding only filters, `not { $p.age > 30 }` | `not coalesce(φ, false)`, which keeps the rows a null `φ` kept | `Q012` |
| a block whose only element re-mentions an outer binding to filter it, `not exists { $p: Person where $p.name = "bob" }` | that filter, `not ($p.name = "bob")`, or `not coalesce($p.age > 30, false)` where the predicate can be null; under `count` or an aggregate, the scalar the block folds to, as in the next row | `T68` |
| a block holding only filters under `exists`, `count` or an aggregate, `count { $p.age > 30 } > 0` | the scalar the block folds to over the one row it tests: `coalesce(φ, false)` for `exists`, `if(coalesce(φ, false), 1, 0)` for `count`, `if(coalesce(φ, false) and e is not null, 1, 0)` for `count(e)`, `if(coalesce(φ, false), e, null)` for `min` and `max`, and the same over `cast(e, F64)` for `sum` and `avg`; `collect`, `stddev` and `percentile` print no edit | `Q017` |
| a block's filter clauses, beside its other elements | the block's own `where`, inside its braces | `Q017` |
| a block among the elements naming a variable that only a later element binds, `$p: Person  exists { $p knows $x }  $x: Company` | the block kept where it was written as `let $block = exists { … }`, and the conjunct `$block`: a block among the elements correlated only on the names bound before it, and in the `where` it would read the later binding | `Q017` |
| a block, `optional` or inline read naming a new variable only in a triple, `count { $p worksAt $c }` | the binding inserted with the edge's end type, `count { $c: Company, $p worksAt $c }` | `T84` |
| a triple written against its edge's declared direction, `$c worksAt $p` | the ends swapped, `$p worksAt $c` | `T82` |
| two pattern components that share no variable, neither key-pinned | a triple joining them, or the product as a named relation drawn with `from` | `T81` |
| `$x.title contains "g"` | `contains($x.title, "g")` | `T7` |
| `$x.tags contains "rust"` (a list) | `"rust" in $x.tags` | `T7` |
| `$x.title starts_with "O"` | `starts_with($x.title, "O")` | `T7` |
| `search($x.t, $q)`, `match_text($x.t, $q)` | `match_terms($x.t, terms($q, mode: any))` | `T6` |
| `fuzzy($x.t, $q)`, `fuzzy($x.t, $q, n)` | `match_terms($x.t, terms($q, mode: any, max_edits: 2))`, or `max_edits: n` for the third argument (none for `0`); every removed lexical call's fix is a machine edit | `T6` |
| `delete T where φ` | `match { $x: T } where φ′  delete $x`: each bare property is written `$x.p`, and an edge's `from` and `to` are `$x.@src` and `$x.@dst` | `Q008` |
| `update T set { … } where φ` | `match { $x: T } where φ′  update $x set { … }`, a bare property in an assignment written `$x.p` | `Q008` |
| a body of several statements | one group per `update … where` and `delete … where`, in order, each opening with its `match`; a statement that selects nothing of its own (an `insert`) stays in the group it was written in, ahead of the groups the fix opens, so it runs once rather than once per selected row. A later group that selects a type an earlier statement changes is refused (`T91`, below), and so is the fix's own move where the moved statement changes the type the group selects: `update Doc … where …` then `insert Doc { … }` writes the `insert` first, and the `update`'s group then selects `Doc`, which it changed. Write the two statements as two writes | `Q008` |
| `insert T { … }` over a type with `@key` | `replace T { … }`, which means what that `insert` meant; narrow it to `insert` where only creation is meant, or `upsert` where only the named properties should change | none: it compiles, and on an existing key, or on a key an earlier statement of the same write inserted (where the older release kept the last row), fails at run time with `key_conflict`, naming `upsert` and `replace`, changing no data |
| `from:` and `to:` in an edge insert | `@src:` and `@dst:` | `T11` |
| `omnigraph load --mode append`, `load csv … mode append` | `insert` | the CLI's and HTTP's mode refusal, `Q004` |
| `--mode merge`, `mode merge` | `replace` | the same |
| a quoted edge name, `$d "in" $x` | the bare name, `$d in $x` (the quoted name is still accepted) | none |
| a read named after a registry function or an element keyword, `query count()`; a read named `search`, `match_text` or `fuzzy` whose body makes that call | the read renamed to the first `<name>_<n>` from 2 no declaration uses, `count_2`, printed for review since a caller invokes a read by its name; renamed, the call is the older function again and prints its own fix | `T74`; `T69` for the call |

The fix for `$x` picks a name the declaration does not use. A rewrite reads
the schema: `contains` over a list and over a `String` are different tests.

### Rankings

A ranking is a relation drawn with `from`, never a call in `order` or
`return` (`T65`); see [Search](../search/index.md). The population is named
first, the score is named where it is drawn, and the filters move to the
population's `where`. This older read:

```text
query near($v: Vector(3)) {
  match { $d: Doc  $d.year > 2000 }
  return { $d.title, nearest($d.embedding, $v) }
  order { nearest($d.embedding, $v) }
  limit 3
}
```

becomes, as `T65`'s edit writes it:

```gq
query near($v: Vector(3)) {
  match {
    let $pop = { match { $d: Doc } where $d.year > 2000 return { $d } },
    from nearest($pop, $d.embedding, $v, k: 3) as $distance,
  }
  return { $d.title, $distance as _distance }
  order { $distance }
  limit 3
}
```

| Older form | GQ 3 |
|---|---|
| `order { nearest($d.e, $q) }  limit n` | `from nearest($pop, $d.e, $q, k: n) as $distance` and `order { $distance }`; the window `k` and the `limit` are separate values |
| `order { bm25($d.t, $q) }`, a bare rank key sorting descending | `from bm25($pop, $d.t, $q) as $score` and `order { $score desc }` |
| `order { rrf(nearest($d.e, $q), bm25($d.t, $s), 60) }  limit n` | `let $all = { match { $d: T } return { $d } }, from rrf(nearest($all, $d.e, $q, k: n), bm25($all, $d.t, $s), k: 60) as $score`, the rest of the pattern after it, and `order { $score desc }`; `k: 100` in each arm when the read has no `limit` |
| an unaliased ranking in `return` | `$distance as _distance` or `$score as _score` |
| `asc` on a rank key | refused, as before |
| a ranking under an aggregate or `return distinct` | the ranking drawn with `from`, its `k` the old `limit`, and `limit` the cut of the groups; the refusal prints the form without an edit |

The edit also inserts the binding of every name a population's triple
introduces. For `rrf` it reads the ranked binding's type from its binding, or
from the end of the edge whose triple alone introduces it, and takes the
older third argument, `rrf(a, b, 60)`, as `k`. It is written only when the
read gives it every piece: a `limit` to size a `nearest` window, the same call
wherever the ranking appears, and no aggregate or `distinct`. The `rrf` form ranks the whole type in each arm,
which keeps the rows the older read returned; a read written new ranks its
population instead ([Fusion](../search/fusion.md#populations)).

## Moved error codes

A code keeps its meaning, so a refusal that changed meaning took another
code. Scripts and agents that match codes need these changes.

| Before | Situation | GQ 3 |
|---|---|---|
| `T2` | a property the type does not declare, read in a query (`$p.id`) | `T6` |
| `T11` | a property or meta-field the type does not have, in a write's `where` | `T6`; an assignment of one stays `T11` |
| `T3` | list equality written in property braces | `T7` |
| `T7` | an aggregate call in a filter, `where count($m) > 2` | `T8`, whose fix is a block |
| `T40` | a block aggregate compared with a value of another type | `T7` |
| `T40` | a block compared with a property of the outer row | accepted |
| `T44` | an aggregate in a write's `where` or an assignment | `T8` |
| `T44` | a full-text predicate (`match_terms`) in a write's `where` | accepted |
| `T45`, `T36` | a bare property name in an assignment or a filter; an alias projected again in `return` | `Q018` |
| `T23` | an edge binding returned whole, `return { $k }` | runs: its edge object (`@id`, `@src`, `@dst`, `@type` over several types, its properties) |
| `T23` | an edge binding inside a block, `optional` or inline read reusing an outer name | `T68` |
| `T32`, `T33`, `T54` | a ranking under an aggregate, a projected rank, `asc` on a rank key | `T65` |
| `T35` | a full-text call projected in `return` | runs: the row's Boolean |
| `T38` | a full-text predicate under `or` or `not` | accepted |
| `T58` | an aggregate compared with an ungrouped property in `return` | `T9` |
| `T59` | `rrf` arms ranking two bindings one traversal connects | `T75` |
| `Q000` | a parse refusal in `lint` | the refusal's own code, `Q001` and up |

`T23` still refuses an edge binding over a multi-hop traversal, one reusing
a name bound in `match`, and one named like its own endpoint. `T40` still
refuses a `sum`, `avg`, `min`, `max` or `collect` block without its argument.

## Other changes a reader sees

- A read may not be named after a registry function or an element keyword
  (`T74`): `distance`, `nearest`, `bm25`, `rrf`, `union`, every scalar
  function and aggregate on [Functions and operators](functions.md)
  (`contains`, `starts_with`, `match_terms`, `terms`, `now`, `lower`,
  `length`, `left`, `round`, `size`, `cast`, …), `let`, `from`, `optional`,
  `exists` and `not`. A write's name is not checked.
- Keywords and function names are read in any letter case, and `case … end`,
  `||`, `<>` and single-quoted strings are accepted as other spellings of
  `if`, `concat`, `!=` and double-quoted strings
  ([Functions and operators](functions.md#other-accepted-spellings)). Lint
  names each with its canonical form, and `lint --fix` writes that form.
- Over grouped rows (an aggregate or `return distinct`), every order key must
  be a `return` item or its alias (`T42`); before, a key such as `$p.@id` or
  `$e.@type` was accepted there.
- A `bm25` score is `F64`; it was `F32`. The `nearest` distance stays `F32`.
- An unaliased ranking in `return` gave `d._distance` or `d._score`; the
  rewrite names the column `_distance` or `_score`.
- `lint --json` reports every refusal of a refused query in that result's
  `diagnostics`, each with its own code, position and suggestion; a parse
  refusal is no longer `Q000` ([Lint](lint.md)).
- Writes name their intent: `insert` creates and fails on an existing key,
  `upsert` (new) sets only the named properties, `replace` writes the whole
  row, and `update` now takes an edge variable as well as a node one; an
  `update` assigning a key or an endpoint is `T93`, and `upsert` or `replace`
  on a type with no `@key` is `T92`. The load modes are `insert`, `upsert`,
  `replace` and `overwrite` ([Mutations and loads](../mutations/index.md#statements)).
- An assignment may copy a Blob read from its group's row (`content:
  $f.content`, or under `if` and `coalesce`) in every write statement
  ([Blobs](../blobs.md#copying-a-blob-from-a-row)). Everywhere else a Blob
  property is no value: a `let` binding one, `is null` on one, and one under a
  function outside an assignment are `T24`.
- A bound plan saved by an earlier release is refused with "unsupported bound
  plan version …; regenerate the saved plan": regenerate it from the query
  ([Explain](explain.md)).
- Every group of a write selects on the snapshot the write started from, where
  the older release ran its statements in order. A write whose `update … where`
  or `delete … where` reads a type an earlier statement of the same write
  changes is refused (`T91`), with no edit: no single write means the same.
  The `Q008` fix can lead there itself: it writes a statement that selects
  nothing ahead of the group it opens, so `update Doc … where …` followed by
  `insert Doc { … }` becomes an `insert Doc` and then a group selecting `Doc`.
  Split it into two writes, the `update` first, or write the final row in one
  keyed statement.
- A list of elements that holds predicates only, with no binding, is refused
  with no edit (`Q017`): `match { $p.age > 3 }`, and `optional { $p.age > 3 }`,
  which binds nothing and keeps every row whether its test holds or not. Move
  the test to the `where` if it should filter, or delete the `optional`.
- `fuzzy(f, q, n)` with `n` above 2 is refused (`T71`: `max_edits` is 0 to 2)
  with no edit. The older release searched with any budget, and no GQ 3 form
  matches more than two edits; choose a budget of 2 or less by hand, which
  can match fewer rows.
- Three texts the older release refused now run: an empty list `[]` assigned
  to a list property of any element type (it was typed `[String]`, `T3`); a
  whole-number float literal assigned to an integer property, `n: 100.0`,
  stored as `100` (it failed at run time; a fractional one is still refused);
  and a float literal with an exponent, `4.6e+18` (a parse error, `Q001`).

## Schemas

- A plain `@index` on a free-text String builds an NGRAM and a BTREE, a `@key`
  String a BTREE, and only `@index(fulltext, ...)` a full-text index
  ([What `@index` builds](../schema/index.md#what-index-builds)). No row
  changes: `match_terms` and `bm25` read the rows either way. A full-text index
  an earlier release built for a plain `@index` or a `@key` stays in place and
  no read uses it ([Full-text analyzers](../search/analyzers.md)); declare
  `@index(fulltext)` to keep one built.
- In `.pg`, `@index(fulltext)` after a property declares a full-text index on
  that property. In a type that also declares a property named `fulltext` it
  is refused as ambiguous; write `@index` on the property the index is for.
  Before, it declared an index on the property `fulltext`.
- A graph stores its schema text, and an earlier binary parses it again on
  every open. A schema apply that stores one of these constructs makes every
  earlier binary refuse the whole graph, every read, write, export and index
  pass, until a later apply stores none of them or the backup taken before
  the upgrade is restored:

  | Construct | `construct` | First read by |
  |---|---|---|
  | an explicit `distance` on a Vector, `l2` included | `vector_distance` | 0.18.0 |
  | `@index` on a List property | `list_index` | 0.18.0 |
  | `@index(fulltext)`, with or without options | `full_text_index` | 0.18.0 |
  | an `analyzer` in `@index(fulltext, …)` | `full_text_analyzer` | 0.18.0 |
  | a `scorer` in `@index(fulltext, …)` | `full_text_scorer` | 0.18.0 |

  A plan with a step (`schema plan`, `schema apply`, `cluster plan`) names
  each one the schema it applies holds, under `one_way_constructs`: its
  `construct`, `description`, `first_release` (the first release that reads
  it), `type_name`, `property` and position. A construct of a whole
  declaration, which a later release can add, names no `property`. The
  report ends with the line `binaries before 0.18.0 refuse the whole graph
  after this apply, until an apply stores none of these constructs or the
  backup taken before the upgrade is restored`, naming the latest
  `first_release` among them; the human report also ends each construct's
  line with `first read by 0.18.0`. A plan with no step applies nothing, so it names
  none, whatever the stored schema holds. They report only; nothing gates the
  apply.

  Where the schema allows the change, a later apply by this release takes a
  construct back, and an earlier binary opens the graph once none is stored:
  `@index(fulltext, …)` respelled as a plain `@index`, a `distance` other than
  `l2` omitted again (the field's distance becomes `l2`, and the next `index
  build` replaces its index), or the property dropped. Removing an `analyzer` or a `scorer` alone
  leaves `@index(fulltext)`, which is one-way too. Removing an `@index` is
  refused, so `@index` on a List is taken back only by dropping the property,
  and an explicit `distance = "l2"` is never taken back by omitting it, since
  that apply has no step. An earlier binary's full-text reads over an index
  this release built stay refused until its own
  `index rebuild --kind full-text`.
  Upgrade every binary that opens the graph before applying such a schema
  ([analyzers](../search/analyzers.md)). A body-level `@index(tags)` over a
  list, a plain `@index` on a String and an added property or type are not
  one-way.
- A schema apply whose plan has no step publishes nothing: the stored text
  and the graph head stay as they were, so an explicit `distance = "l2"` over
  an omitted one, or an edited comment, is never stored. A cluster applies
  such a text-only change of a graph's schema file with no graph commit:
  `cluster plan` reports it as `text_only`, and the graph keeps the source
  `omnigraph schema show` shows
  ([deployment boundaries](../clusters/index.md#deployment-boundaries)).
