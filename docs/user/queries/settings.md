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
| `engine` | enum `v1`, `v2` | `v1` | request | `OMNIGRAPH_ENGINE` | whether a read query runs through engine version 2, the plan runner; this setting does not change change-feed or merge execution |
| `rrf_plan` | enum `auto`, `force_prefilter`, `force_postfilter` | `auto` | process | `OMNIGRAPH_RRF_PLAN` | the reciprocal rank fusion plan on a traversal-constrained `nearest`, for diagnosis |
| `merge_lineage` | enum `off`, `on`, `verify` | `on` (a debug build defaults to `verify`) | request | `OMNIGRAPH_MERGE_LINEAGE` | how a merge finds the entities it classifies: the full-scan walk, the lineage path, or both compared |
| `ann_nprobes` | integer, at least `0` | `20` | request | `OMNIGRAPH_ANN_NPROBES` | the partition cap per index delta of a `nearest` scan; `0` is no cap; on `engine = v1` the setting is read at execution and no plan records it |
| `stage_write_concurrency` | integer `1..=64` | `8` | process | `OMNIGRAPH_LOAD_CONCURRENCY` | the width of the staged-write fan-out for `load` and `mutate` |

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
error: unknown setting `traversal`; expected one of engine, rrf_plan, merge_lineage, ann_nprobes, stage_write_concurrency
set ann_nprobes = "many";
error: setting `ann_nprobes` takes an integer of at least 0, got a string
set stage_write_concurrency = 0;
error: setting `stage_write_concurrency` takes an integer in 1..=64, got 0
set stage_write_concurrency = 64;        (in an HTTP request)
error: setting `stage_write_concurrency` is a process setting; it is read from the server's environment, not from a request
```

See [Query language](index.md) for query syntax.
