# Explain

`explain` before a read declaration returns the query's v2 plan instead of
its rows, regardless of the selected execution engine. It is a statement:
one per file, never beside a declaration. The query is compiled and planned
against the target's schema and snapshot without executing it. Planning
may read dataset metadata for byte estimates; it does not scan query data
or invoke the embedding client.

```gq
explain query adults() {
  match {
    $p: Person
    $p.age > 30
  }
  return { $p.name }
}
```

```bash
omnigraph query --query explain.gq --store graph.omni --format kv
```

The result is a table with one row per plan node, in the order an `EXPLAIN`
listing prints them:

| Column | Type | Meaning |
|---|---|---|
| `tree` | `String` | `logical`, `physical` or `datafusion` for a plan node; `plan` for the rows after the trees |
| `depth` | `I64` | the node's depth from its tree's root; absent on `plan` rows |
| `node` | `String` | the node kind (`TableScan`, `Filter`, `Expand`, `Projection`, …) or the operator (`ScanExec`, `FilterExec`, …); on `plan` rows the field name (`pass`, `route`, `logical_hash`, …) |
| `detail` | `String` | the node's own fields as JSON, without its children; an operator's own text; on `plan` rows the field's value, a string bare and anything else as JSON |

The logical tree comes first, then the physical tree, then the `datafusion`
tree, each in pre-order, so a node's children are the rows one level deeper
that follow it and the tree is rebuilt from the rows without loss. Every
physical row's `detail` carries `id`, the node's integer id in the physical
plan: unique within one plan, not stable across plans, and absent on
`logical` and `datafusion` rows. The explain document
(`Session::explain_query`) carries the same `id` on every node of its
`physical_plan`. A physical
`Expand` row's `detail` carries `mode` (`indexed_scan` or `csr`, the
traversal path the planner chose), `alternatives` (the modes the run may
switch to: the other mode when the cost model chose and may re-decide from
the observed frontier, the probed index coverage or a warm CSR; empty when
the mode was pinned or chosen without statistics), `frontier_estimate`
(the row-count estimate it chose from, `null` when the snapshot holds no
statistics) and `version` (the pinned dataset version of `edge:<type>`). A traversal's destination is reached one of two ways. A
physical `Scan` row with `id_restriction` `input` reads the destination once
per slice of at most 256 traversal rows and carries `access` `id_lookup`. A
physical `HashJoin` row reads the destination table once (its second input,
a table `Scan` of it) and joins the traversal (its first input) to it; it
carries `binding`, the joined binding, and `fallback`, the access path the
run may take instead: `id_lookup` when the build refuses memory before the
traversal is consumed, `null` when the plan declares none. A run takes no
switch the plan does not declare; which side ran is in the
[`profile`](#profile) rows, never in explain. A physical `Scan` row of a
binding a leading `nearest()` or `bm25()` ranks carries `ranked`: an object
with `kind` (`nearest` or `bm25`), `property`, `query` (the argument as the
query wrote it, a parameter name such as `$q` or a literal, never its bound
value), `fetch` (the candidates the index is asked for: the query's limit
for `nearest`, `null` for `bm25`, which is uncapped), `nprobes` on a
`nearest` scan (the `ann_nprobes` setting when the plan was built, the IVF
partitions probed per index delta; `null` is no cap) and `scope` (`order`
for the query's own order, `primary` or `secondary` for an arm of `rrf()`). A
physical
`RankFuse` row has two inputs, one subtree per arm, each with its own ranked
`Scan`; it carries `arms` (binding and kind per arm), `k` and `limit`. Then one
`plan` row per optimizer pass that fired (`node` `pass`), and one per
remaining field of the explain document: `route` (the engine route the plan
describes), `logical_hash` (the structural hash of the logical plan),
`explain_version`, `operation`, and the statistics used by planning. The
last `plan` row is `assumptions`, what the planner read to build the plan,
as one JSON object: `params`, the names of the parameters it read (never a
value); `settings`, each setting it read with its spelling (`traversal`,
`ann_nprobes`); `env`, each environment variable it read with the value it
resolved (`OMNIGRAPH_EXPAND_INDEXED_MAX_FRONTIER`,
`OMNIGRAPH_EXPAND_INDEXED_MAX_HOPS`, when the query traverses); `gate_policy`,
the `rrf_plan` mode and the prefilter gate's admission thresholds; and
`memory_limit`, the query pool in bytes. Query
plans omit `pipelines` and node `properties.schema`: their executable schema
is derived when lowering. Every query scan records its pinned `version`, or
`null` when that type has no dataset in the requested snapshot.
Sort keys and ordering use GQ text such as `$p.name desc`; a ranked scan's
ordering names `$p._distance asc` or `$p._score desc`, and the physical
`Sort` above a search order leads its `keys` with that score key, followed by
the query's plain keys; a fusion's ordering is `rrf($a, $b) desc` and it
plans no `Sort`. A `Sort` row's `tiebreak` lists the id keys it appends after
`keys` (`$p.@id`), empty where equal rows are indistinguishable.

The `datafusion` tree is the plan the query executes on the `v2` route: the
physical tree lowered to operators, every read operator omnigraph's own
(`ScanExec`, `ExpandExec`, `HashJoinExec`, `FilterExec`, `ProjectionExec`,
`SortExec`, `LimitExec`, `CrossJoinExec`, `AntiJoinMaskExec`, `RankFuseExec`,
`MetadataCountExec`) except the aggregate, DataFusion's `AggregateExec`, one
row per operator with `depth` from
its nesting and `detail` the operator's own text (a filter's predicate, a
sort's keys, a scan's columns; `AntiJoinMaskExec` serves every correlated
block, `not`, `exists`, `count` and the column aggregates, and prints its
predicate). It is the tree of the query's first pass; a
search that widens its scan after a short answer lowers it again. An
`explain` runs nothing, so when the lowering would need the embedding client
(a `nearest()` whose query is a string), the tree is left out and one `plan`
row `datafusion` reads `unavailable: …` with the reason.

## Profile

The third surface beside the rows and explain is `profile`: what each node
of the plan a `v2` run executed did, written back as rows in the explain row
schema (`tree`, `depth`, `node`, `detail`) with `tree` `profile`, no `depth`,
`node` the plan node's kind (`Scan`, `HashJoin`, `Expand`, `Sort`, `Page`,
…) and `detail` one JSON object per node of the run, from the one operator
the node built:

| Key | Meaning |
|---|---|
| `id` | the node's `id`, the same integer the physical explain row carries |
| `operator` | the operator's name (`ScanExec`, `HashJoinExec`, `SortExec`, …) |
| `status` | `executed` (a stream of the operator was polled) or `skipped` (in the tree, never polled) |
| `attempts` | one entry per pass: `rung` (0 the first pass, then the overfetch rung taken), `ran`, `actual_rows` (the operator's output rows) and `drained` (whether `actual_rows` is complete, defined in [execution.md](../../dev/execution.md#v2-plan-lowering-one-node-one-operator)) |

`ran` is `true` or `false` on an operator without a choice, and on the
operator of a declared switch it is the side that ran: `hash_join` or
`id_lookup` on the `HashJoinExec` of a `HashJoin`, `csr` or `indexed_scan`
on the `ExpandExec` of an `Expand`, the mode the traversal ended on. Explain itself runs nothing and carries no `profile` row; the
profile is returned beside the rows by the run that produced them
(`Session::query_inspected`, the v2 inspection door, through
`Executed::profile`). The row schema is `explain_version` 1.

An `explain` statement is served by `omnigraph query` and `POST /query`. It
takes the same `--branch`/`--snapshot` target and `--params` as the query
itself and needs the same `read` policy decision. `mutate` and the deprecated
routes refuse it, and so is a mutation declaration under `explain`.

The session setting `engine` selects how ordinary reads execute: `v1` by
default, or `v2` under `set engine = v2;`. Explain always describes v2, so
with the default setting its operators describe the alternate execution
route. See [session settings](index.md#session-settings).
