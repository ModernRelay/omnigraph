# Explaining a load statement

`explain` before a `load csv` statement returns the load's accepted plan
instead of running the load:

```gq
explain load csv "s3://imports/knows.csv" into edge:Knows mode overwrite delimiter "|" header
```

The statement accepts every load option. It reads metadata, but no payload,
and publishes nothing. HTTP and MCP require `change` on the target branch
before the external Blob policy check or metadata probe. A source over
`load_source_max_bytes` refuses as `csv_source_bytes`, as a load would.

The answer is the planner's document, in the same rows a query's
[explain](explain.md) uses: trees `logical_initial`, `logical` and `physical`
in pre-order, then the `plan` rows. A load plan has three nodes, from the
root down:

| `node` | `detail` fields |
|---|---|
| `FragmentWrite` | `table`, `version`, `mode`, `adapter`; the physical row adds `max_input_bytes`, `max_workers` |
| `Validate` | `table` and `checks`, every check of the load in the order it runs, which is the refusal precedence; the physical row adds `removal` and `max_workers` |
| `CsvSource` | `format`, `source`, `source_bytes`, `estimated_rows`, `delimiter`, `header`, `columns`, `table`, `version`; the physical row adds `max_partitions` (the streams that parse the source: one per 8 MiB of it, at most `stage_write_concurrency`), `max_workers`, `fallback` |

Each entry of `checks` carries `check` (`value`, `unique`, `edge_ri`,
`cardinality` or `retained_edges`), its identity fields, `consults` (the
committed table it reads, or `null`) and, on the physical node, `access`:

- `unique` carries `columns` and `role` (`key` or `unique`).
- `edge_ri` and `cardinality` carry `endpoint` (`from` or `to`) and
  `node_type`.
- `retained_edges`, on a node overwrite, carries `edge_table` and `endpoint`.

`access` is the path the check reads committed state by:

| `access` | Meaning |
|---|---|
| `delta` | the check reads no committed state: a `@key` group, or a non-key `@unique` group under overwrite, whose committed view is empty |
| `probe` | a filtered key lookup; uniqueness uses chunks of 8,192 keys |
| `scan` | one projected scan of the key columns, matched in memory |

Overwrite endpoint checks cost probes and scans using estimated input rows,
the consulted table's row count, and usable BTREE coverage at the accepted
version. A probe first pays its key list, one entry per input row, so an
unindexed endpoint check scans, and a partially or fully indexed table keeps
`probe` only while the key list is small against it. Unknown counts and equal
costs keep `probe`; statistics record `index_coverage` and both costs.
`cardinality` and `retained_edges` always use `probe`. An `edge_ri` check
also carries `probe`, where it reads the committed ids: `swept` when both
endpoint checks of the edge use `scan` and both node types carry one
`I32`, `I64` or `U32` `@key`, so the committed sets load while the source
parses and each parse partition probes its own rows; `judged` otherwise,
after every row is in. Either way the first missing endpoint in row order, `from` before `to`
within a row, is the refusal. The node's `removal`
says whether the committed ids an overwrite removes are found at all: `scan`,
one projected scan of the committed id column, when a `retained_edges` check
reads them; `none` when no planned check does. A node target whose committed
table holds no rows removes nothing, so its plan carries neither the
`retained_edges` checks nor the scan; a node target no edge type references,
an edge overwrite, an append and a merge carry `none` as well. Under `none`
no removed id is found, so the removed-id allowance of `write_max_bytes`
charges nothing. Retained-edge access describes the filtered
lookup in the incident edge table. Append and merge always use `probe`.

The `FragmentWrite` row's `mode` pairs with its `adapter`: an overwrite writes
`fragments`, an append uses `strict_insert`, and a merge uses `upsert`.

The `plan` rows are the query explain's, with these values for a load:

| `node` | `detail` |
|---|---|
| `datafusion` | `unavailable`: a load's operators are lowered at execution, under the statement's transaction |
| `route` | `engine` for overwrite, which runs the plan; `executor` for append and merge, which the plan describes while the keyed adapters run them (`reason` then says `mode_on_executor`) |
| `explain_version` | the shared counter, `7` since every route prints `pipelines` |
| `operation` | `kind` (`load`), `mode`, and `tables`: the target and each consulted committed table with its `type_key` and `version` |

An executed load's receipt carries the same document and an `executed`
object with `partitions` (the widest operator's), `rows`, `source_bytes` and
`operators`: one entry per node of the plan in post-order (the source
first), each with the node's `id` and `node` kind, the `operator` it built,
the `rows` it emitted (the sink counts the rows it staged),
`elapsed_compute_ns`, the time its stream and its workers were polled,
`wall_ns`, the span from its first poll to its end with every wait included,
and `partitions`, the streams it ran (1 for every operator but the source).
The source's entry also lists `slices`, one per record-aligned 8 MiB slice of
the source in source order: `slice`, the `partition` whose byte range held it,
its `rows`, `read_ns`, the wait for the slice's bytes, `elapsed_compute_ns`, and
`start_ns` and `end_ns` from the source's first poll. The statement's answer, the `ChangeOutput` of
`omnigraph mutate --json` and of `POST /mutate`, carries the object as
`executed` beside the counts and the commit. The compute, wait and wall numbers
are measurements of one run; a GQT case pins only the rows and the partitions.
