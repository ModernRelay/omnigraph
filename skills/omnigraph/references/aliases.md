# Aliases & Agent Automation

## Contents
- What an alias is
- Operator alias schema
- Args binding & JSON-first parsing
- Default to structured output
- Alias naming convention
- Secrets don't belong in aliases
- Example alias set
- Invocation patterns

How to wire Omnigraph operations for agents and scripts.

## What an alias is

An **operator alias** decouples a stable **operation name** from its implementation, so an agent calling `omnigraph alias signal …` keeps working as the query evolves. Aliases live in `~/.omnigraph/config.yaml` and are personal *bindings* to a **stored query on a named server** — they carry no query content; the stored query in the cluster catalog is the team's contract.

```yaml
# ~/.omnigraph/config.yaml
aliases:
  triage:
    server: intel-dev      # an entry under servers:
    graph: spike           # optional (multi-graph servers)
    query: weekly_triage   # the STORED query's name — never a file
    args: [since]          # positional args → params, in order
    params: { limit: 20 }  # fixed defaults; positionals/--params win
    format: table
```

```bash
omnigraph alias triage 2026-06-01
# → POST <intel-dev>/graphs/spike/queries/weekly_triage with the keyed credential
```

> **Alias vs stored query.** The alias is *yours* (a personal name + defaults); the **stored query** it points at is the *team's* — declared in `cluster.yaml`, type-checked and served by the cluster (`GET /graphs/<id>/queries`, `POST /graphs/<id>/queries/<name>`, gated by `invoke_query`). See [`stored-queries.md`](stored-queries.md).
## Operator Alias Schema

```yaml
aliases:
  <alias-name>:
    server: <server-name>     # an entry under servers: in ~/.omnigraph/config.yaml
    graph: <graph-id>         # optional: for multi-graph servers
    query: <stored-query>     # the stored query's NAME (never a file path)
    args: [<name1>, <name2>]  # positional CLI args → named params, in order
    params: { <k>: <v> }      # fixed default params; positionals / --params win
    format: table|kv|csv|jsonl|json   # optional: output format
```

Dispatch with `omnigraph alias <name> [args]` — one subcommand for read **and** write stored queries (a mutation alias is double-gated by `invoke_query` + `change`). Aliases live in their own namespace, so one can never shadow or be shadowed by a built-in verb.

### `args` bind to query parameters

If `args: [slug, name, age]`, then:

```bash
omnigraph alias foo sig-bar "Some Name" 29
```

...maps to `{"slug":"sig-bar","name":"Some Name","age":29}`.

### Args are JSON-first

Each arg is parsed as JSON first, then falls back to string:
- `29` → integer
- `"29"` → string
- `true` → boolean
- `Alice` → string (JSON parse fails, falls back)
- `{"x":1}` → object

Explicit `--params '{...}'` wins on key conflict.

## Default to Structured Output

For scripts and agents, prefer `jsonl` or `json`; `table` is for humans. Set a default in `~/.omnigraph/config.yaml`:

```yaml
defaults:
  output: jsonl
```

Or per-alias (`format: jsonl`), or per-call (`--format jsonl`).

### When to use which

- **`jsonl`** — one JSON object per line, first line is metadata; streams; ideal for agents
- **`json`** — pretty-printed JSON array; smaller results; human-readable
- **`kv`** — `key: value` per line; good for single-row lookups
- **`csv`** — for spreadsheets or line-count-heavy analysis
- **`table`** — default human view; don't use in automation

## Alias Naming Convention

Short, hyphenated, matches the conceptual operation:

- `signal`, `pattern`, `element` — single lookup (typical pair with `format: kv`)
- `signals`, `patterns`, `elements` — list
- `signal-patterns`, `pattern-signals` — traversals
- `add-signal`, `link-forms-pattern` — mutations

## Secrets Don't Belong in Aliases

Credentials never live in an alias or any config file. For remote servers, `omnigraph login <server>` stores the bearer token in `~/.omnigraph/credentials` (`0600`); for S3-backed storage, AWS creds go in `.env.omni`. Aliases should only contain query names and parameter bindings — never tokens, passwords, or API keys.

## Example Alias Set

```yaml
# ~/.omnigraph/config.yaml
servers:
  intel-dev: { url: https://graph.example.com }
aliases:
  # Lookups (kv format for single-row readability)
  signal:   { server: intel-dev, graph: spike, query: get_signal,  args: [slug], format: kv }
  pattern:  { server: intel-dev, graph: spike, query: get_pattern, args: [slug], format: kv }
  # Lists
  signals:  { server: intel-dev, graph: spike, query: recent_signals }
  # Traversals
  pattern-signals: { server: intel-dev, graph: spike, query: pattern_signals, args: [slug] }
  # Mutations (stored mutation; invoke_query + change)
  add-signal:         { server: intel-dev, graph: spike, query: add_signal, args: [slug, name, brief, stagingTimestamp, createdAt, updatedAt] }
  link-forms-pattern: { server: intel-dev, graph: spike, query: link_signal_forms_pattern, args: [signal, pattern] }
```

Each `query:` names a stored query the cluster serves — declare them in `cluster.yaml` and `cluster apply` first (see [`stored-queries.md`](stored-queries.md)).

## Invocation Patterns

```bash
# Invoke an alias (read or write — the bound stored query decides)
omnigraph alias signal sig-kimi-k25
omnigraph alias add-signal sig-new "Name" "Brief" \
  2026-04-14T00:00:00Z 2026-04-14T00:00:00Z 2026-04-14T00:00:00Z

# Override output format
omnigraph alias signals --format jsonl

# Explicit --params (wins over positional args on key conflict)
omnigraph alias signal --params '{"slug":"sig-override"}'
```

The `alias` subcommand carries `--params`/`--params-file`, `--format`/`--json`, and `--config`; the server, graph, and stored-query name come from the binding. For a different server/graph or a branch read, call `query`/`mutate` directly.
