# Engine catalogue

The engine describes itself in GQ. Five built-in node types, spelled with
the `@` prefix that already marks engine-owned metadata (`$p.@id`,
`$e.@type`), hold the registries the running binary executes with: its
physical operators, its planner passes, its session settings, its storage
format and its logical plan kinds. The rows are derived from those
registries at query time, so the list a reader sees is the list the engine
runs on. Nothing is stored; a user schema cannot declare an `@` type.

```gq
query operators() {
  match { $o: @Operator }
  return { $o.name, $o.inputs }
}
```

The declarations, one per type, are in [`catalogue.gq`](catalogue.gq);
`omnigraph schema show og_catalog` prints them. Ordinary `schema show`
prints only the graph's reusable schema source; `--json` includes the
built-in declarations in the separate `catalogue_source` field.

## Where to run it

| Scope | Command |
|---|---|
| no store at all | `omnigraph query --store og_catalog -e '<gq>'` |
| beside a graph's own types | `omnigraph query --store <uri> -e '<gq>'` |
| served | `omnigraph query --server <name> --graph og_catalog -e '<gq>'` |

`og_catalog` is a reserved scope the binary serves without opening a store:
no branch, no snapshot, no graph commit in the answer, and `--branch` or
`--snapshot` is refused. Only the exact token selects it; `./og_catalog`, an
absolute path and a `file://` URI stay ordinary store addresses. The prefix
`og_` is reserved for system graph ids: a deployment cannot create a graph
named `og_…`.

Inside a graph scope the catalogue types sit beside the user types, so one
query joins them:

```gq
query jobs_naming_no_operator() {
  match {
    $j: Job
    not { $o: @Operator $o.name = $j.uses }
  }
  return { $j.name }
}
```

A served read of `og_catalog` is authorized like a graph read: the policy
bound to `graph.og_catalog` in `cluster.yaml` decides, with the resource
`og_catalog` and no branch; without a bound policy the ordinary no-policy
rules apply (static tokens may read, signed data credentials need a
permit). See [Authorization and actors](../operations/policy.md).

## What is refused

- `insert`, `update`, `delete` and `load` targeting an `@` type: the rows
  are the engine's.
- `search`, `fuzzy`, `match_text`, `bm25`, `nearest` and `rrf` over a
  catalogue binding: catalogue rows have no index. Scalar predicates,
  projection, aggregation, ordering, limits and joins work as on any type.
- Under `og_catalog`: any stored type, any write, blob, branch, commit,
  snapshot, export, change feed or stored query.

`explain` names the source: a catalogue scan reports `source: catalogue`,
reads `access sequential`, is never counted from a manifest and never
hydrated by row address.

## The five types

**`@Operator`**: one row per distinct physical operator name the plan runner
executes; `inputs` lists every number of children the operator admits. Two
plan node kinds that report one name (`Limit` and `Page` both report `Page`)
share one row. `Scan` admits `[0, 1]`: a table scan has no input, a
dependent id lookup has one.

**`@Pass`**: one row per planner pass, `position` its place in the order
`explain` reports passes. A run fires a subsequence of this order; the two
scan-finalization passes `key_to_id` and `scan_access` come last.

**`@Setting`**: one row per session setting: `kind` spells the legal values
(`enum(…)`, `integer(min..)`, `integer(min..=max)`, `text`), `default` the
value `show` reports before any `set`, `scope` whether any caller
(`request`) or only the hosting process (`process`) may set it. The
descriptive text of each setting stays in [Session settings](settings.md).

**`@Engine`**: the one row describing the binary: `version` is its release
version, `current_internal_schema_version` the storage format it writes,
`min_supported_internal_schema_version` the oldest stored format it still
serves. A graph's own stored stamp is in `omnigraph snapshot`, not here.

**`@LogicalKind`**: one row per logical plan node kind with its routing
`coverage`: `routable` or `refused_by_name`. Coverage governs change-feed
and merge routing only. Read and load plans have their own admission checks.

## Inventories

Generated from a built CLI with
`python3 scripts/check-catalogue-docs.py --binary target/debug/omnigraph --write`;
the same script without `--write` fails when these tables drift from the
binary's rows.

<!-- catalogue:operators -->
| name | inputs |
|---|---|
| Aggregate | [1] |
| AntiJoin | [2] |
| ClassifyThreeWay | [1] |
| ContainsJoin | [2] |
| CrossJoin | [2] |
| CsvSource | [0] |
| Expand | [1] |
| Filter | [1] |
| FragmentWrite | [1] |
| HashJoin | [2] |
| HydrateByAddress | [1] |
| HydrateColumns | [1] |
| MetadataCount | [0] |
| OuterReference | [0] |
| Page | [1] |
| Projection | [1] |
| RankFuse | [2] |
| RowCompare | [1] |
| Scan | [0, 1] |
| Sort | [1] |
| SortMergeJoin | [2] |
| Validate | [1] |
<!-- /catalogue:operators -->

<!-- catalogue:passes -->
| position | name |
|---|---|
| 0 | resolve |
| 1 | predicate_pushdown |
| 2 | aggregate_pushdown |
| 3 | projection_pushdown |
| 4 | fragment_scope |
| 5 | address_short_circuit |
| 6 | late_materialization |
| 7 | join_algorithm |
| 8 | expand_mode |
| 9 | access_path |
| 10 | anti_join_strategy |
| 11 | key_to_id |
| 12 | scan_access |
<!-- /catalogue:passes -->

<!-- catalogue:settings -->
| name | kind | default | scope |
|---|---|---|---|
| ann_nprobes | integer(0..) | 20 | request |
| engine | enum(v2) | v2 | request |
| explain_dump_dir | text |  | process |
| history_release_bytes | integer(1024..=262144) | 262144 | request |
| load_source_max_bytes | integer(1..) | 33554432 | request |
| merge_lineage | enum(off, on, verify) | on | request |
| query_memory_bytes | integer(1..) | 157286400 | request |
| rrf_plan | enum(auto, force_prefilter, force_postfilter) | auto | process |
| stage_write_concurrency | integer(1..=64) | 8 | process |
| traversal_work_limit | integer(1..=9223372036854775807) | 1000000 | request |
| write_max_bytes | integer(1..) | 33554432 | request |
<!-- /catalogue:settings -->

<!-- catalogue:logical_kinds -->
| name | coverage |
|---|---|
| Aggregate | refused_by_name |
| AntiJoin | refused_by_name |
| CsvSource | refused_by_name |
| Expand | refused_by_name |
| Filter | routable |
| FragmentWrite | refused_by_name |
| Join | routable |
| Limit | routable |
| MergeClassify | routable |
| MetadataCount | refused_by_name |
| Nearest | refused_by_name |
| OuterReference | refused_by_name |
| Projection | routable |
| RankFuse | refused_by_name |
| RowDiff | routable |
| Sort | routable |
| TableScan | routable |
| TextSearch | refused_by_name |
| Validate | refused_by_name |
<!-- /catalogue:logical_kinds -->
