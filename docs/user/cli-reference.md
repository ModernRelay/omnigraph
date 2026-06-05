# CLI Reference (`omnigraph`)

A reference for the `omnigraph` binary's command surface and `omnigraph.yaml` schema. For a quick-start guide, see [cli.md](cli.md).

17 top-level command families, 40+ subcommands. All commands accept either a positional `URI`, `--uri`, or a `--graph <name>` resolved against `omnigraph.yaml`.

## Top-level commands

| Command | Purpose |
|---|---|
| `init` | `--schema <pg>` → initialize a graph (also scaffolds `omnigraph.yaml` if missing) |
| `load` | bulk load a branch (`--mode overwrite\|append\|merge`) |
| `ingest` | branch-creating transactional load (`--from <base>`) |
| `query` (alias: `read`) | run named read query; source via `--query <path>`, `-e`/`--query-string <GQ>`, or `--alias <name>` (exactly one). `read` is the deprecated previous name and prints a one-line warning to stderr |
| `mutate` (alias: `change`) | run mutation query; same `--query` / `-e` / `--alias` mutual-exclusion as `query`. `change` is the deprecated previous name and prints a one-line warning to stderr |
| `snapshot` | print current snapshot (per-table version + row count) |
| `export` | dump to JSONL on stdout (`--type T`, `--table K` filters) |
| `branch create \| list \| delete \| merge` | branching ops |
| `commit list \| show` | inspect commit graph |
| `run list \| show \| publish \| abort` | transactional run ops |
| `schema plan \| apply \| show (alias: get)` | migrations |
| `lint` (alias: `check`) | offline / graph-backed query validation. Replaces `query lint` / `query check`, which are kept as deprecated argv-level shims that print a one-line warning and rewrite to `omnigraph lint` |
| `queries validate \| list` | operate on the server-side stored-query registry (the `queries:` block). `validate` type-checks every stored query against the live schema offline (opens the selected graph; exits non-zero on any breakage), catching schema drift without restarting the server; `list` prints the selected registry's query names, MCP exposure, and typed params. For per-graph registries, pass `--graph <graph>` or set `defaults.graph`; with no graph selection, `list` shows only top-level `queries:`. Distinct from `lint`, which validates a single `.gq` file |
| `optimize` | non-destructive Lance compaction (skips tables with `Blob` columns; `--json` reports a `skipped` field) |
| `cleanup --keep N --older-than 7d --confirm` | destructive version GC |
| `embed` | offline JSONL embedding pipeline |
| `policy validate \| test \| explain` | Cedar tooling. Selects `defaults.graph`, else `serve.graphs`, else top-level `policy.file` |
| `config view [--resolved] [--show-origin] [--json] [<graph>]` | print the merged config; `--show-origin` labels each field with its source layer; `--resolved <graph>` prints the typed locator (embedded/remote) |
| `use <graph>` | set the active graph (writes `~/.omnigraph/state/active.yaml`); a bare command then targets it |
| `version` / `-v` | print `omnigraph 0.3.x` |

## `omnigraph.yaml` schema

```yaml
version: 1                          # omit for the legacy schema (lenient, deprecation-warned);
                                    # `1` = strict: unknown/typo'd keys, and the removed legacy
                                    #   keys (`project:`, `cli:`, `server:`, top-level
                                    #   `policy:`/`queries:`), are rejected at any depth
servers:                            # named remote endpoints (referenced by graphs.<>.server)
  <name>: { endpoint: <https://host:port> }
graphs:
  <name>:
    # Embedded XOR remote. Embedded → `storage:`; remote → `server:` (+ optional `graph_id:`).
    storage: <local|s3://>          # embedded; a bare string, or a block { uri, region, endpoint }
    # server: <name>                # remote: a `servers:` entry (mutually exclusive with storage)
    # graph_id: <id>                # the graph's id on that server (defaults to the entry key)
    # uri: <local|s3://|http(s)://> # DEPRECATED legacy spelling of storage/server; warns at load
    bearer_token_env: <ENV_NAME>
    branch: <name>                  # optional default branch
    snapshot: <version>             # optional read-pinned snapshot
    policy: { file: <policy.yaml> } # per-graph Cedar policy (embedded graphs only)
    queries:                      # per-graph stored-query registry (server-role; multi-graph mode)
      <query-name>:               # key MUST equal the `query <name>` symbol inside the .gq
        file: <path-to-.gq>       # relative to this config's directory
        mcp:
          expose: true            # default true: listed in the MCP catalog (GET /queries); set false to hide (still HTTP-callable)
          tool_name: <name>       # optional MCP tool-name override (defaults to <query-name>;
                                  #   must be unique across exposed queries)
serve:                              # host-role serving config (was `server:`)
  graphs: [<name>]                  # served set; one entry = single-graph mode, omit = serve all
  bind: <ip:port>
  policy: { file: <server-policy.yaml> }   # server-level Cedar (management endpoints)
defaults:                           # CLI/client defaults (was `cli:`)
  graph: <name>
  branch: <name>
  output_format: json|jsonl|csv|kv|table
  table_max_column_width: 80
  table_cell_layout: truncate|wrap
query:
  roots: [<dir>, …]   # search path for .gq files
auth:
  env_file: ./.env.omni
aliases:
  <alias>:
    # accepted values: `read` / `query` (read alias), `change` / `mutate`
    # (write alias). `query` and `mutate` are recommended; `read` and
    # `change` remain accepted forever for back-compat.
    command: read|change|query|mutate
    query: <path-to-.gq>
    name: <query-name>
    args: [<positional-name>, …]
    graph: <name>
    branch: <name>
    format: <output-format>
```

**Legacy spellings** (honored only when `version:` is omitted; each emits a load-time deprecation warning, and is rejected under `version: 1`):

| Legacy | v1 |
|---|---|
| `cli:` | `defaults:` |
| `server:` (`graph:` scalar) | `serve:` (`graphs:` list) |
| top-level `policy:` | `graphs.<name>.policy` |
| top-level `queries:` | `graphs.<name>.queries` |
| `project:` | removed (no consumer) |
| `uri:` | `storage:` (embedded) / `server:` (remote) |

## Layered config (global-first)

The CLI reads up to three config layers and merges them, so it works from any
directory with no project file (the `kubectl`/`gh` posture). Precedence, low → high:

**built-in defaults < global < active-context state < project < env vars < CLI flags**

| Layer | Path | Notes |
|---|---|---|
| Global | `~/.omnigraph/config.yaml` | the primary, self-sufficient default; secret-free, machine-local |
| State | `~/.omnigraph/state/active.yaml` | written by `omnigraph use`; sets the active graph |
| Project | `./omnigraph.yaml` (or `--config`) | optional repo-scoped override + deployment manifest |

**Global dir resolution:** `OMNIGRAPH_CONFIG` (explicit file) > `OMNIGRAPH_HOME` (dir) >
`$XDG_CONFIG_HOME/omnigraph` (if set) > `~/.omnigraph`. The global file is
`<dir>/config.yaml`.

**Merge semantics:** settings-objects (`defaults`, `serve`) deep-merge per field; the
named-resource maps (`servers`, `graphs`, `aliases`) union by key, with a higher
layer's entry replacing the lower *wholesale*; lists and scalars replace. Relative
paths (`policy.file`, `queries.<>.file`, `storage`, `query.roots`) resolve against the
directory of the layer that defined them.

`omnigraph config view --show-origin` prints which layer each value came from;
`config view --resolved <graph>` prints the final embedded/remote locator. The server
does **not** layer the global config — it reads only the project/`--config` manifest.

## Output formats (`query` command, alias: `read`)

- `json` — pretty-printed object with metadata + rows
- `jsonl` — one metadata line then one JSON object per row
- `csv` — RFC 4180-ish quoting
- `table` — fitted text table, honors `table_max_column_width` + `table_cell_layout`
- `kv` — grouped per-row key/value blocks

## Param resolution

Precedence (high to low): explicit `--params` / `--params-file`, alias positional args, `omnigraph.yaml` defaults. JS-safe-integer handling is built in (`is_js_safe_integer_i64`, `JS_MAX_SAFE_INTEGER_U64`) so 64-bit ids round-trip safely through JSON clients.

## Bearer token resolution (CLI)

1. `graphs.<name>.bearer_token_env`
2. `OMNIGRAPH_BEARER_TOKEN` global env
3. `auth.env_file` referenced `.env`

## Duration parsing (cleanup)

`s | m | h | d | w` units, e.g. `--older-than 7d`.
