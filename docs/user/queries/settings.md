# Session settings

A file may open with settings lines, before its declarations or its one
statement. `set <name> = <value>;` gives one setting a value for that file:
the CLI applies it to the invocation, the server to the request, and nothing
is persisted, so the next file starts from the process defaults again. The
same name set twice in one file takes the last value. `reset <name>;`
restores one setting's process default; `reset all;` restores every setting
the caller may set. `show <name>;` and `show all;` are themselves the file's
one statement, run through `omnigraph query` or `POST /query`, and return one
row per setting with the columns `name`, `value`, `default`, `source` and
`scope`. `source` is `default`, `env` (a process default read from the
environment), `request` (the `settings` field, a `set` query parameter or
`--set`) or `file` (a `set` line). A file of only settings lines, `set` or
`reset`, `reset all;` alone included, is refused:
`a file of only settings lines carries no statement`.

```gq
set merge_lineage = verify;

query reports($who: String) {
  match { $p: Person { name: $who }  $p reportsTo{1,3} $m }
  return { $m.name }
}
```

```gq
reset merge_lineage;
branch merge review into main;
```

```gq
show all;
```

`show` is authorized as a scope-free read, the decision `branch list` takes: a
credential scoped to one branch, or one holding only change permissions, is
refused `show`. A graph-wide reader sees every row, process rows included, and
each row's `source` says whether the value came from the environment.

Every setting has a scope. A `request` setting is set by any caller. A
`process` setting is set only by the process that hosts the engine: the
server from its environment, and a direct CLI run (`--target`) from its
environment, its `--set` values and the file; a served caller's `settings`
field, `set` query parameter or `set` line naming one is refused.

A setting reaches the engine through one of three doors:

- CLI: `--set name=value`, repeatable, on `omnigraph query`, `mutate`,
  `branch merge`, `commit changes` and `changes poll`, and direct `--target` loads; see the
  [CLI guide](../cli/index.md#session-settings).
- HTTP body: the `settings` object on `POST /query`, `POST /mutate`,
  `POST /mutate/if-graph-commit` and `POST /branches/merge`, one key per
  `request` setting (`{"settings": {"merge_lineage": "verify"}}`); an unknown key
  is refused.
- HTTP query string: `set=<name>=<value>`, repeatable, on `GET /changes` and
  `GET /commits/{id}/changes`.

A `set` line in the text applies after the door's value, so the file wins.

A stored query runs under the process defaults: `omnigraph query <name>`
without `-e` or `--query` refuses `--set`, and `POST /queries/{query_name}` takes no
`settings` field.

Each setting's environment variable is its process default: the server reads
it once at startup, the CLI once per run, and the value is what every request
starts from and what `reset` returns to. An invalid or out-of-range value
refuses startup; no default is substituted.

| Name | Type and values | Default | Scope | Process default variable | What it chooses |
|---|---|---|---|---|---|
| `engine` | enum `v2` | `v2` | request | `OMNIGRAPH_ENGINE` | the engine a read query runs on; `v2`, the plan runner, is the only value, and `v1` is refused as an unknown value; this setting does not change change-feed or merge execution |
| `rrf_plan` | enum `auto`, `force_prefilter`, `force_postfilter` | `auto` | process | `OMNIGRAPH_RRF_PLAN` | the reciprocal rank fusion plan on a traversal-constrained `nearest`, for diagnosis |
| `merge_lineage` | enum `off`, `on`, `verify` | `on` (a debug build defaults to `verify`) | request | `OMNIGRAPH_MERGE_LINEAGE` | how a merge finds the entities it classifies: the full-scan walk, the lineage path, or both compared |
| `ann_nprobes` | integer, at least `0` | `20` | request | `OMNIGRAPH_ANN_NPROBES` | the partition cap per index delta of a `nearest` scan; `0` is no cap |
| `stage_write_concurrency` | integer `1..=64` | `8` | process | `OMNIGRAPH_LOAD_CONCURRENCY` | the width of the staged-write fan-out for `load` and `mutate`: tables staged at once, and in one `load csv` overwrite the Lance writers and the streams that parse the source (one per 8 MiB of it, at most this many) |
| `traversal_work_limit` | integer `1..=9223372036854775807` | `1000000` | request | `OMNIGRAPH_TRAVERSAL_WORK_LIMIT` | shared traversal row-work cap for statements containing alternatives or wildcard |
| `write_max_bytes` | integer, at least `1` | `33554432` (32 MiB) | request | `OMNIGRAPH_WRITE_MAX_BYTES` | independent row-data and Blob-payload allowances for bounded writes; changes the existing mutate, load and materializing branch-merge limits; a value raised above the default is held in memory as one typed batch at an overwrite load's commit; a `load csv` statement applies it to each staged-write input: an overwrite stages the reader's 8,192-row batches and refuses a batch over it as `staged_write_input_bytes`, while append and merge keep the keyed adapter's ceilings |
| `load_source_max_bytes` | integer, at least `1` | `33554432` (32 MiB) | request | `OMNIGRAPH_LOAD_SOURCE_MAX_BYTES` | the largest object a `load csv` statement reads, checked before any payload byte is fetched; a served load is additionally bounded by the server's workload admission |
| `query_memory_bytes` | integer, at least `1` | `157286400` (150 MiB) | request | `OMNIGRAPH_QUERY_MEMORY_BYTES` | the memory pool one read query runs under; operators that cannot spill refuse work beyond it, while spillable operators may use bounded scratch storage; raising it permits more tracked memory per query; a load statement additionally admits its read-reuse buffers (a statement-scoped session, held table handles and a byte-range memo) against a bounded share of this allowance, and runs its reads uncached when the share cannot be admitted; it is also the aggregate allowance of one `load csv` statement's tracked memory: the retained source bytes (captured once per statement and reused by every reprepare attempt), every decoded batch, validation state and the staging working set reserved before the first fragment write; a statement the allowance cannot hold refuses with the `query_memory_bytes` resource error before any fragment write; a CSV load needs roughly the source plus twice its typed input; JSONL loads keep their existing bounds |
| `history_release_bytes` | integer `1024..=262144` | `262144` | request | `OMNIGRAPH_HISTORY_RELEASE_BYTES` | the byte budget of a branch's buffer of unreleased commits; a mutate, load or branch merge of the session whose buffer and head reach it closes a history block, and the publish after it writes the block under `__history` (schema apply, branch create and delete, repair and upgrade publish under the production budget); a load reaches it only through the CLI or an embedded `Session`, since the HTTP load routes take no settings; query results never change, only the number of requests and files; production keeps the default, a caller may only lower it |
| `explain_dump_dir` | text | empty (disabled) | process | `OMNIGRAPH_EXPLAIN_DUMP_DIR` | the directory each finished explain document is written into, one `<operation>_<logical_hash>.explain.json` per operation and logical hash; each complete document atomically replaces the previous one at that path; empty disables the write; a failed write reports on stderr and never fails the operation |

For these statements the cap covers all traversals, including named and nested
ones, and remains shared across retries. It charges consumed source rows,
conservative physical edge rows before each scan, examined adjacency entries
and bound-edge source replication. Exceeding the cap fails with
`traversal_work_limit`; a result limit cannot bypass admission. This is a row-work
bound, not a byte, elapsed-time or total-query CPU/I/O bound. Memory and scratch
retain their existing limits. The budgeted route uses pinned Lance scans with
indexed filtering where available.

Sources are admitted in fixed windows of at most 8,192 rows, independent of
upstream batch boundaries. Each nonempty frontier probe charges the selected
table's full physical row count before opening the scan. Multiple windows,
members, directions and hops can therefore charge a table repeatedly. A directed
one-hop query over selected tables totaling 1,000,000 physical rows exceeds the
default cap even when its start node has only one neighbor. Index selectivity
does not reduce this conservative admission charge. The cap does not bound
index decoding, bytes read, or storage latency.

Internal traversal pins used by the GQT harness can force indexed execution. A
forced CSR pin is refused for statements with selections; there is no public
`set traversal` setting.

`write_max_bytes` applies to each existing write account: row data and Blob
descriptors, logical Blob payloads, parser estimates and retained removed IDs.
The accounts keep their existing per-table, per-operation or per-chunk scope.
Row data and Blob payloads have separate allowances: a payload exactly at the
limit can accompany a row whose metadata fits its own allowance. The value is
captured once for an operation, including its retries and merge publication.
A one-row keyed write costs about 1 to 3 KiB of row data from Arrow buffer
rounding, so allowances below a few KiB refuse every write.

Strict inserts and overwrite writes also pass `write_max_bytes` to Lance as
the buffer accumulation target for each data-file writer. Lance divides it
across top-level columns instead of using its default 8 MiB per column.
The default 32 MiB target therefore raises or lowers each column's threshold
depending on the number of columns.
This is a flush threshold, not a total memory cap: the incoming array can
cross it, nested fields and encoding have additional working memory, and
concurrent table writers each receive the target. Upserts and updates use
Lance merge-insert, which does not expose this option and keeps its native
buffering defaults. Row and Blob admission limits still apply to those writes.

Neither byte setting has a fixed maximum. A lower environment value is a
process default, so a request may still select any value of at least `1`.
Raising `write_max_bytes` above the default admits a larger staged-write
input (a JSONL load's typed batch, a CSV overwrite's reader batch), held in
memory at commit; raising `load_source_max_bytes` admits a
larger `load csv` source object into memory before parsing. On a server both
stay bounded by workload admission (`OMNIGRAPH_GLOBAL_BYTES_MAX`,
`OMNIGRAPH_PER_ACTOR_BYTES_MAX`). Embedded sessions and direct `--target` CLI
loads can override them; a `load csv` statement travels on `/mutate`, whose
`settings` field and `set` prefix apply. The JSONL HTTP load routes inherit
the process default and accept no request setting; remote CLI loads of those
routes refuse `--set`. HTTP bodies, Blob reads, compaction and external URI
metadata retain their independent limits. Lower values can produce smaller
merge chunks or reject a write that the default admits.

`history_release_bytes` is measured against the branch's buffered commit and
table-change rows as stored, plus the head whole; the row layout is in
[storage versioning](../../dev/versioning.md#current-storage-contract). The
commit that reaches the budget records the decision in its commit id's slot,
so sessions publishing to one branch under different budgets agree on where a
block closes, and the publish after it releases the block whatever its own
budget. A load carries the setting only from the CLI or an embedded
`Session`: the HTTP load routes take no settings. The bound on a head's
commit fields stays 256 KiB, the production budget. A lower budget means more
and smaller `__history` files and more requests per release, never different
rows.

A name outside the table, a value of the wrong type, and a value outside the
declared values or range are each refused with the table's row. A `process`
setting named in a request is refused too: the `settings` field has no member
for one, so the key is refused as unknown, while a `set` line in the text and
a `set=` parameter answer the row's message. `omnigraph lint` reports a
settings line's refusal as `ERROR line <n>, column <c>: <message>`.

```text
set merge_lineage = fast;
error: unknown value `fast` for setting `merge_lineage`; expected one of off, on, verify
set traversal = csr;                      (likewise reset traversal; and show traversal;)
error: unknown setting `traversal`; expected one of engine, rrf_plan, merge_lineage, ann_nprobes, stage_write_concurrency, traversal_work_limit, write_max_bytes, load_source_max_bytes, query_memory_bytes, history_release_bytes, explain_dump_dir
set ann_nprobes = "many";
error: setting `ann_nprobes` takes an integer of at least 0, got a string
set stage_write_concurrency = 0;
error: setting `stage_write_concurrency` takes an integer in 1..=64, got 0
set stage_write_concurrency = 64;        (in an HTTP request)
error: setting `stage_write_concurrency` is a process setting; it is read from the server's environment, not from a request
```

See [Query language](index.md) for query syntax.
