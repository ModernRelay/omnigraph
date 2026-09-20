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
that follow it and the tree is rebuilt from the rows without loss. A physical
`Expand` row's `detail` carries `mode` (`indexed_scan` or `csr`, the
traversal path the planner chose) and `frontier_estimate` (the row-count
estimate it chose from, `null` when the snapshot holds no statistics). A
physical `Scan` row with `id_restriction` `input` (a traversal's destination)
carries `access`: `hash_join` when the destination table is read once and
joined to the traversal, `id_lookup` when it is read once per slice of at
most 256 traversal rows. A planned hash build that exceeds the runtime memory
budget switches to ID lookup before consuming the traversal. Explain records
the planned choice. Then one
`plan` row per optimizer pass that fired (`node` `pass`), and one per
remaining field of the explain document: `route` (the engine route the plan
describes), `logical_hash` (the structural hash of the logical plan),
`explain_version`, `operation`, and the statistics used by planning. Query
plans omit `pipelines` and node `properties.schema`: their executable schema
is derived when lowering. Every query scan records its pinned `version`, or
`null` when that type has no dataset in the requested snapshot.
Sort keys and ordering use GQ text such as `$p.name desc`; search ordering
names `$p._distance asc`, `$p._score desc`, or `rrf($a, $b) desc`.

The `datafusion` tree is the plan the query executes on the `v2` route: the
physical tree lowered to DataFusion operators (`FilterExec`, `SortExec`,
`AggregateExec`, …) and omnigraph's own (`ScanExec`, `ExpandExec`,
`AntiJoinMaskExec`, `RankFuseExec`), one row per operator with `depth` from
its nesting and `detail` the operator's own text (a filter's predicate, a
sort's keys, a scan's columns). It is the tree of the query's first pass; a
search that widens its scan after a short answer lowers it again. An
`explain` runs nothing, so when the lowering would need the embedding client
(a `nearest()` whose query is a string), the tree is left out and one `plan`
row `datafusion` reads `unavailable: …` with the reason.

An `explain` statement is served by `omnigraph query` and `POST /query`. It
takes the same `--branch`/`--snapshot` target and `--params` as the query
itself and needs the same `read` policy decision. `mutate` and the deprecated
routes refuse it, and so is a mutation declaration under `explain`.

The session setting `engine` selects how ordinary reads execute: `v1` by
default, or `v2` under `set engine = v2;`. Explain always describes v2, so
with the default setting its operators describe the alternate execution
route. See [session settings](index.md#session-settings).
