# CLI reference

This page is a map of the `omnigraph` command surface. The installed binary is
the exact reference:

```bash
omnigraph --help
omnigraph <command> --help
omnigraph <command> <subcommand> --help
```

## Addressing a graph

Most graph commands accept one of these scopes:

| Scope | Use |
|---|---|
| positional URI | Direct access for commands whose positional slot is not used by another value |
| `--store <URI>` | Direct access to one `file://`, `s3://`, or `az://` graph |
| `--server <NAME\|URL> --graph <ID>` | A graph served by a multi-graph server |
| `--cluster <DIR\|URI> --graph <ID>` | Direct maintenance of a cluster-managed graph |
| `--profile <NAME>` | A named scope from operator config |

A bare local path is accepted where a graph URI is expected. `--server` and
`--store` are mutually exclusive. A store already identifies one graph, so it
cannot be combined with `--graph`.

Common global flags:

| Flag | Meaning |
|---|---|
| `--as <ACTOR>` | Actor for direct writes and cluster operations |
| `--yes` | Non-interactive consent for destructive writes to non-local storage |
| `--quiet` | Suppress the resolved write target printed to stderr |

Served writes ignore `--as`: the server derives the actor from the bearer
token.

## Commands

| Command | Purpose | Scope |
|---|---|---|
| `init` | Create an empty graph from a `.pg` schema | direct |
| `query` | Run a read query | direct or served |
| `mutate` | Run an insert/update/delete query | direct or served |
| `load` | Load graph JSONL in `overwrite`, `append`, or `merge` mode | direct or served |
| `blob get`, `blob stat` | Read or inspect one Blob cell | direct or served |
| `branch create/list/delete/merge` | Manage graph branches | direct or served |
| `snapshot` | Show a branch snapshot | direct or served |
| `commit list/show/changes` | Inspect history or one commit's entity changes | direct or served |
| `changes poll/baseline` | Consume a branch change feed or establish a new baseline | direct or served |
| `export` | Stream a branch as JSONL | direct or served |
| `schema show` | Read the accepted schema | direct or served |
| `schema apply` | Apply a schema to a standalone graph | direct |
| `schema plan` | Preview a schema migration | direct |
| `lint` | Validate `.gq` source | local schema or direct graph |
| `optimize` | Compact data and reconcile declared indexes | direct |
| `repair` | Preview or publish classified storage drift | direct |
| `cleanup` | Delete old versions under an explicit retention policy | direct |
| `graphs list` | List graphs on a server | served |
| `queries list/validate` | Inspect or validate a cluster query registry | cluster |
| `cluster validate/plan/apply/...` | Operate declarative cluster state | cluster config |
| `policy validate/test/explain` | Validate or evaluate applied policy | cluster |
| `embed` | Generate, clean, or refresh seed embeddings | local tooling |
| `login`, `logout` | Manage a named server credential | local |
| `profile list/show` | Inspect operator profiles | local |
| `alias` | Invoke a personal stored-query alias | served |
| `version` | Print build and storage-format information | local |

The [CLI guide](index.md) gives end-to-end examples. Maintenance safety is
covered in [Maintenance](../operations/maintenance.md).

## Query inputs and output

For ad-hoc source, pass `--query <FILE>` or `-e/--query-string <GQ>`. When the
source contains multiple declarations, the positional name selects one. For a
stored server query, omit the source and pass its registry name.

Parameters can be supplied inline or from a file:

```bash
--params '{"name":"Ada"}'
--params-file params.json
```

Read output supports `table`, `json`, `jsonl`, `csv`, and `kv`. `--json` is the
stable machine-readable form for commands that do not use `--format`.

### Machine-readable read and write positions

When the read snapshot has an effective graph head, `omnigraph query --json`
returns its `graph_commit_id` in the complete read envelope. The id and rows
come from the same pinned snapshot; use that id when a later mutation must be
conditional on the state that was read.

Successful `mutate --json`, `load --json`, and compatibility
`ingest --json` responses include `commit`, the exact commit published by
that attempt. It contains `graph_commit_id`, optional `graph_branch`,
`graph_manifest_version`, optional parent and merged-parent ids, optional
`actor_id`, and `created_at` in Unix microseconds. A successful mutation
that changes no entities returns `"commit": null`.

### Conditional mutations

```bash
omnigraph query find_person --query queries.gq --store graph.omni --json
omnigraph mutate update_person --query queries.gq --store graph.omni \
  --if-commit <graph_commit_id> --json
```

`--if-commit` runs the mutation only while the target branch is still at that
commit. Any intervening commit on the branch invalidates the condition, even
when it changed unrelated data. A mismatch has no effect and exits with code
4; JSON output includes `precondition_failure` with `expected` and optional
`actual` commit ids. Re-read and decide again instead of retrying blindly.

## Load modes

`load --mode` is required:

| Mode | Existing entities | Typical use |
|---|---|---|
| `overwrite` | Each node or edge type represented in the batch is replaced; other types remain | Initial load or import of a complete export |
| `append` | Kept; duplicate IDs fail | Strict batch insertion |
| `merge` | Updated by ID | Idempotent synchronization |

`--branch <NAME>` selects an existing branch. Add `--from <BASE>` to create a
missing branch from an explicit base. Overwrite is destructive and may require
`--yes` for non-local storage.

Change-feed commands, cursor checkpointing, and baseline recovery are described
in [Changes and Change Feeds](../branching/changes.md).

## Blob commands

```text
omnigraph blob get  <node|edge> <TYPE> <ID> <PROPERTY> [scope] [options]
omnigraph blob stat <node|edge> <TYPE> <ID> <PROPERTY> [scope] [options]
```

`get` accepts `--branch` or `--snapshot`, `--offset`, `--length`, and
`--out <PATH>`. `stat` accepts `--branch` or `--snapshot` and `--json`.
See [Blob values](../blobs.md).

## Operator configuration

The default path is `~/.omnigraph/config.yaml`. Set `OMNIGRAPH_HOME` to use a
different directory.

```yaml
operator:
  actor: act-alice

defaults:
  output: table
  server: prod
  default_graph: knowledge

servers:
  prod:
    url: https://graph.example.com

clusters:
  company:
    root: s3://company-data/omnigraph

profiles:
  prod-knowledge:
    server: prod
    default_graph: knowledge
  company-admin:
    cluster: company
    default_graph: knowledge
  local-dev:
    store: file:///tmp/dev.omni

aliases:
  experts:
    server: prod
    graph: knowledge
    query: find_experts
    args: [topic]
    params:
      limit: 20
    format: table
```

Each profile binds exactly one of `server`, `cluster`, or `store`. Select it
with `--profile` or `OMNIGRAPH_PROFILE`. Explicit flags override values filled
by a profile.

Bearer tokens never belong in `config.yaml`. Store a token with
`omnigraph login <server>` or provide `OMNIGRAPH_BEARER_TOKEN` for the current
invocation.

## Confirmation rules

`cleanup` changes nothing until `--confirm` is present. Destructive operations
against non-local storage also require interactive confirmation or `--yes`; in
non-interactive and JSON modes they fail closed. The same non-local consent
rule applies to overwrite loads and branch deletion.

## Compatibility aliases

| Old name | Canonical name |
|---|---|
| `read` | `query` |
| `change` | `mutate` |
| `check` and `query lint` | `lint` |
| `ingest` | `load` |

These aliases are compatibility-only. Use the canonical names in new
automation.
