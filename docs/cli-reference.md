# CLI Reference (`omnigraph`)

A reference for the `omnigraph` binary's command surface and `omnigraph.yaml` schema. For a quick-start guide, see [cli.md](cli.md).

17 top-level command families, 40+ subcommands. All commands accept either a positional `URI`, `--uri`, or a `--target <name>` resolved against `omnigraph.yaml`.

## Top-level commands

| Command | Purpose |
|---|---|
| `init` | `--schema <pg>` → initialize a repo (also scaffolds `omnigraph.yaml` if missing) |
| `load` | bulk load a branch (`--mode overwrite\|append\|merge`) |
| `ingest` | branch-creating transactional load (`--from <base>`) |
| `read` | run named query (params via `--params`, `--params-file`, or alias args) |
| `change` | run mutation query |
| `snapshot` | print current snapshot (per-table version + row count) |
| `export` | dump to JSONL on stdout (`--type T`, `--table K` filters) |
| `branch create \| list \| delete \| merge` | branching ops |
| `commit list \| show` | inspect commit graph |
| `run list \| show \| publish \| abort` | transactional run ops |
| `schema plan \| apply \| show (alias: get)` | migrations |
| `query lint \| check` | offline / repo-backed validation |
| `optimize` | non-destructive Lance compaction |
| `cleanup --keep N --older-than 7d --confirm` | destructive version GC |
| `embed` | offline JSONL embedding pipeline |
| `policy validate \| test \| explain` | Cedar tooling |
| `version` / `-v` | print `omnigraph 0.3.x` |

## `omnigraph.yaml` schema

```yaml
project: { name }
graphs:
  <name>:
    uri: <local|s3://|http(s)://>
    bearer_token_env: <ENV_NAME>
server:
  graph: <name>
  bind: <ip:port>
cli:
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
    command: read|change
    query: <path-to-.gq>
    name: <query-name>
    args: [<positional-name>, …]
    graph: <name>
    branch: <name>
    format: <output-format>
policy:
  file: ./policy.yaml
```

## Output formats (read command)

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
