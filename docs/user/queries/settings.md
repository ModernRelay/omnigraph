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
server from its environment, and a direct CLI run (`--store`) from its
environment, its `--set` values and the file; a served caller's `settings`
field, `set` query parameter or `set` line naming one is refused.

A setting reaches the engine through one of three doors:

- CLI: `--set name=value`, repeatable, on `omnigraph query`, `mutate`,
  `branch merge`, `commit changes` and `changes poll`; see the
  [CLI guide](../cli/index.md#session-settings).
- HTTP body: the `settings` object on `POST /query`, `POST /mutate`,
  `POST /mutate/if-graph-commit` and `POST /branches/merge`, one key per
  `request` setting (`{"settings": {"merge_lineage": "verify"}}`); an unknown key
  is refused. The deprecated `/read` and `/change` run under the process
  defaults: neither takes a `settings` field, and each refuses a `set` or
  `reset` prefix in the source it carries with that same refusal.
- HTTP query string: `set=<name>=<value>`, repeatable, on `GET /changes` and
  `GET /commits/{id}/changes`.

A `set` line in the text applies after the door's value, so the file wins.

A stored query runs under the process defaults: `omnigraph query <name>`
without `-e` or `--query` refuses `--set`, and `POST /queries/{name}` takes no
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
| `stage_write_concurrency` | integer `1..=64` | `8` | process | `OMNIGRAPH_LOAD_CONCURRENCY` | the width of the staged-write fan-out for `load` and `mutate` |
| `traversal_work_limit` | integer `1..=9223372036854775807` | `1000000` | request | `OMNIGRAPH_TRAVERSAL_WORK_LIMIT` | shared traversal row-work cap for statements containing alternatives or wildcard |
| `history_release_bytes` | integer `1024..=262144` | `262144` | request | `OMNIGRAPH_HISTORY_RELEASE_BYTES` | the byte budget of a branch's buffer of unreleased commits; a mutate, load or branch merge of the session whose buffer and head reach it closes a history block, and the publish after it writes the block under `__history` (schema apply, branch create and delete, repair and upgrade publish under the production budget); a load reaches it only through the CLI or an embedded `Session`, since the HTTP load routes take no settings; query results never change, only the number of requests and files; production keeps the default, a caller may only lower it |

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
error: unknown setting `traversal`; expected one of engine, rrf_plan, merge_lineage, ann_nprobes, stage_write_concurrency, traversal_work_limit, history_release_bytes
set ann_nprobes = "many";
error: setting `ann_nprobes` takes an integer of at least 0, got a string
set stage_write_concurrency = 0;
error: setting `stage_write_concurrency` takes an integer in 1..=64, got 0
set stage_write_concurrency = 64;        (in an HTTP request)
error: setting `stage_write_concurrency` is a process setting; it is read from the server's environment, not from a request
```

See [Query language](index.md) for query syntax.
