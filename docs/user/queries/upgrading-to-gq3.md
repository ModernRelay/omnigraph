# Upgrading queries to GQ 3

GQ 3 replaces the query language. This release parses GQ 3 only: there is no
edition line, no older parser and no conversion command. A source written for
an earlier release fails to compile like any other invalid source, and every
older form that GQ 3 refuses prints its fix, as text and as byte-range edits
([Diagnostics](diagnostics.md)). Rewrite each source from the map below and
the fixes the compiler prints.

## Find what to rewrite

Run the checks before upgrading the server:

```bash
omnigraph lint --query queries.gq --schema schema.pg --json
omnigraph queries validate --target . --json    # the applied stored queries
omnigraph cluster plan --config . --json         # desired stored queries
```

Each refusal carries its code, position, fix and `suggestion` edits. Apply
every `machine_applicable` edit of one pass at once, compile again, and repeat
until no refusal is left ([every refusal in one pass](diagnostics.md#every-refusal-in-one-pass)).

## Stored queries at upgrade

A stored query that no longer compiles is refused, as an invalid source
always was: in `cluster plan` with its diagnostic, and at server boot, where
its graph does not serve (the graph is quarantined while healthy graphs
serve, and startup fails under `--require-all-graphs`). Nothing converts the
source. For a source that no longer parses, the boot log names the query and
gives the code, position, message, fix and edits:

```text
stored query 'NAME': error[CODE] at line L, column C: MESSAGE; fix: FIX; edits: bytes S..E -> "TEXT"
```

A source that parses but fails the type check is reported as
`query 'NAME': type error: CODE: MESSAGE`; `queries validate --json` gives its
fix and edits. Byte offsets count into the stored query's own source.

## The respelling map

Each row is an older form, the GQ 3 form, and the refusal that prints the
fix.

| Older form | GQ 3 | Code |
|---|---|---|
| `mutation name(…) { … }` | `query name(…) { … }` | `Q009` |
| a filter clause in `match`, `match { $p: Person  $p.age > 30 }` | a conjunct of the `where` after the braces, in written order, joined by `and`: `match { $p: Person } where $p.age > 30` | `Q017` |
| `$x: T { p: v }` | `$x: T` and the conjunct `$x.p = v`, leading the `where` | `Q010` |
| elements, `return` items, order keys, arguments or assignments separated by whitespace | commas | `Q016` |
| `not { P }` | `not exists { P }` in `where` | `Q012` |
| a block whose only element re-mentions an outer binding to filter it, `not exists { $p: Person where $p.name = "bob" }` | that filter, `not ($p.name = "bob")`, or `not coalesce($p.age > 30, false)` where the predicate can be null | `T68` |
| a block's filter clauses | the block's own `where`, inside its braces | `Q017` |
| a block, `optional` or inline read naming a new variable only in a triple, `count { $p worksAt $c }` | the binding inserted with the edge's end type, `count { $c: Company, $p worksAt $c }` | `T84` |
| a triple written against its edge's declared direction, `$c worksAt $p` | the ends swapped, `$p worksAt $c` | `T82` |
| two pattern components that share no variable, neither key-pinned | a triple joining them, or the product as a named relation drawn with `from` | `T81` |
| `$x.title contains "g"` | `contains($x.title, "g")` | `T7` |
| `$x.tags contains "rust"` (a list) | `"rust" in $x.tags` | `T7` |
| `$x.title starts_with "O"` | `starts_with($x.title, "O")` | `T7` |
| `search($x.t, $q)`, `match_text($x.t, $q)` | `match_terms($x.t, terms($q, mode: any))` | `T6` |
| `fuzzy($x.t, $q)`, `fuzzy($x.t, $q, n)` | `match_terms($x.t, terms($q, mode: any, max_edits: 2))`, or `max_edits: n` | `T6` |
| `delete T where φ` | `match { $x: T } where φ′  delete $x`: each bare property is written `$x.p`, and an edge's `from` and `to` are `$x.@src` and `$x.@dst` | `Q008` |
| `update T set { … } where φ` | `match { $x: T } where φ′  update $x set { … }`, a bare property in an assignment written `$x.p` | `Q008` |
| a body of several statements | one group per statement, in order, each opening with its `match`; consecutive statements with no `match` between them stay one group | `Q008` |
| a quoted edge name, `$d "in" $x` | the bare name, `$d in $x` (the quoted name is still accepted) | none |

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
introduces. It is written only when the read gives it every piece: a `limit`
to size a `nearest` window, the same call wherever the ranking appears, and
no aggregate or `distinct`. The `rrf` form ranks the whole type in each arm,
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
| `T23` | an edge binding returned whole, `return { $k }` | `T86`: return its fields |
| `T23` | an edge binding inside a block, `optional` or inline read reusing an outer name | `T68` |
| `T32`, `T33`, `T54` | a ranking under an aggregate, a projected rank, `asc` on a rank key | `T65` |
| `T35` | a full-text call projected in `return` | `T86` |
| `T38` | a full-text predicate under `or` or `not` | accepted |
| `T58` | an aggregate compared with an ungrouped property in `return` | `T9` |
| `T59` | `rrf` arms ranking two bindings one traversal connects | `T75` |
| `Q000` | a parse refusal in `lint` | the refusal's own code, `Q001` and up |

`T23` still refuses an edge binding over a multi-hop traversal, one reusing
a name bound in `match`, and one named like its own endpoint. `T40` still
refuses a `sum`, `avg`, `min` or `max` block without its argument.

## Other changes a reader sees

- A read may not be named after a registry function or an element keyword
  (`T74`): `distance`, `nearest`, `bm25`, `rrf`, `union`, `contains`,
  `starts_with`, `match_terms`, `terms`, `now`, `if`, `coalesce`, `concat`,
  `count`, `sum`, `avg`, `min`, `max`, `let`, `from`, `optional`, `exists`
  and `not`. A write's name is not checked.
- Over grouped rows (an aggregate or `return distinct`), every order key must
  be a `return` item or its alias (`T42`); before, a key such as `$p.@id` or
  `$e.@type` was accepted there.
- A `bm25` score is `F64`; it was `F32`. The `nearest` distance stays `F32`.
- An unaliased ranking in `return` gave `d._distance` or `d._score`; the
  rewrite names the column `_distance` or `_score`.
- `lint --json` reports every refusal of a refused query in that result's
  `diagnostics`, each with its own code, position and suggestion; a parse
  refusal is no longer `Q000` ([Lint](lint.md)).
- A bound plan saved by an earlier release is refused with "unsupported bound
  plan version …; regenerate the saved plan": regenerate it from the query
  ([Explain](explain.md)).

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
- A schema whose full-text index names an analyzer or a scorer
  (`@index(fulltext, analyzer = …)`), or whose vector declares a distance
  (`Vector(n, distance = …)`), is refused by earlier binaries ("schema
  feature … is unknown to this build"). Upgrade every binary that opens the
  graph before applying such a schema
  ([analyzers](../search/analyzers.md)).
