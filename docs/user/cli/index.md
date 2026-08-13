# CLI Guide

## Core Graph Flow

```bash
omnigraph init --schema schema.pg graph.omni
omnigraph load --data data.jsonl --mode overwrite graph.omni
omnigraph snapshot graph.omni --branch main --json
# Invoke a stored query BY NAME from the catalog (served — addressed by scope):
omnigraph query  get_person    --params '{"name":"Alice"}'
omnigraph mutate insert_person --params '{"name":"Mina","age":28}'
```

`omnigraph load` accepts strict graph-level NDJSON. Each nonblank line is
exactly one logical node or edge envelope:

```json
{"type":"Person","data":{"name":"Ada"}}
{"edge":"Knows","from":"Ada","to":"Grace","data":{}}
```

`data` may be omitted and defaults to `{}`. An optional `data.id` follows the
ordinary ID rules: it must match a node's canonical `@key` ID when one exists;
otherwise an omitted node or edge ID is generated. Duplicate members, unknown
properties, reserved physical fields, and noncanonical supplied node IDs are
rejected before effects. There is no table or dataset selector: one file may
touch several logical declarations and publishes as one atomic graph commit.
Local and remote CLI loads use this same grammar; the remote client sends it as
raw `application/x-ndjson` to `/graphs/{graph_id}/load/ndjson`.

`omnigraph query` is the canonical read command (pairs with `POST /query`);
`omnigraph mutate` is the canonical write command (pairs with `POST /mutate`).
The positional argument is the **stored-query name**, invoked from the served
catalog — the graph is addressed by scope (`--server` / `--profile`
/ defaults), and the verb asserts the query's kind (`query` rejects a stored
mutation, and vice-versa). The previous names `omnigraph read` and
`omnigraph change` keep working as visible aliases — invocations emit a one-line
deprecation warning to stderr. See [Deprecated names](#deprecated-names).

For **ad-hoc** reads and mutations (REPLs, AI agents, one-off scripts, local dev),
pass the GQ source with `-e` / `--query-string` (inline) or `--query <path>` (a
file), and address a graph's storage directly with `--store`. By-name catalog
invocation is served-only — a bare `--store` has no catalog, so it's the ad-hoc
lane:

```bash
omnigraph query --store graph.omni \
  -e 'query find($name: String) { match { $p: Person { name: $name } } return { $p.name, $p.age } }' \
  --params '{"name":"Alice"}'

omnigraph mutate --store graph.omni \
  -e 'query add($name: String, $age: I32) { insert Person { name: $name, age: $age } }' \
  --params '{"name":"Inline","age":42}'

# A multi-query file: the positional selects which query to run.
omnigraph query --store graph.omni --query queries.gq get_person --params '{"name":"Alice"}'
```

`-e` is mutually exclusive with `--query <path>`. With either, the positional
name (optional) selects which query in the source to run. The inline source
travels through the same parser, lint, params binding, and commit machinery as a
file-based query — only the source loader changes.

## Reading Blob Cells

`blob get` and `blob stat` address one logical node or edge property. They use
graph vocabulary—not a physical table, dataset, row address, or per-table lane:

```bash
# Stream the complete managed value to a file.
omnigraph blob get node Person ada portrait \
  --store graph.omni --out portrait.jpg

# Read bytes 1024..5119 from a served edge property.
omnigraph blob get edge Attachment attachment-42 payload \
  --server prod --graph knowledge --offset 1024 --length 4096 > payload.part

# Inspect metadata without reading payload bytes.
omnigraph blob stat node Person ada portrait \
  --server prod --graph knowledge --branch main --json
```

Use `--store` for an embedded graph or `--server … --graph …` for a served
graph. Store- and server-bound profiles/defaults work too. Blob commands do not
accept a positional graph URI or `--cluster`; the four positional values are
the Blob cell selector. They also reject `--as`, because these read-only verbs
do not consume a client-supplied actor. `ENTITY` is exactly `node` or `edge`.
Reads default to `main`; `--branch NAME` and `--snapshot ID` are mutually
exclusive.

`blob get` emits raw bytes—never JSON or base64—to stdout, or to `--out PATH`.
The range forms are:

- `--offset N --length M`: `N..N+M`
- `--offset N`: `N` through the end
- `--length M`: the first `M` bytes

An end beyond the value is clamped to the end, matching an HTTP byte range. A
start at or beyond the end of a non-empty value is unsatisfiable. Requested
length zero and arithmetic overflow fail before graph/server resolution; an
unsatisfiable start fails before payload transfer. This does not make a valid
empty Blob an error: a full get of one succeeds and writes zero bytes. Managed
values are streamed through consecutive bounded reads rather than buffered in
full.

A successful command writes the exact selected bytes. If storage or transport
fails after streaming begins, the command exits nonzero, but bytes already sent
to stdout cannot be recalled and an `--out` file may contain the successfully
delivered prefix. A failure before the first payload byte leaves an existing
`--out` path untouched. For atomic file replacement even after a mid-stream
failure, write to a temporary path and rename it only after a zero exit status.

`blob stat` is descriptor-only. For a managed value it reports the selector,
`managed`, byte size, strong ETag, the requested target, and an exact opaque
witness for the immutable graph view that resolved. For a whole-object external
value it reports `external`, the stored URI, and the target while omitting size
and ETag. A ranged external descriptor fails loudly rather than widening to the
whole target object. A null cell is not an external or zero-byte value; it
returns not found. The JSON shape is stable:

```json
{
  "selector": {
    "entity": "node",
    "type": "Person",
    "id": "ada",
    "property": "portrait"
  },
  "kind": "managed",
  "size": 58291,
  "etag": "\"0123456789abcdef0123456789abcdef\"",
  "target": {
    "branch": "main",
    "resolved_snapshot": "manifest:main:v42:etag:..."
  }
}
```

`target.resolved_snapshot` is always present and should be treated as opaque.
For a live branch it is a precise manifest witness, not necessarily a commit
ULID; a byte-for-byte graph copy can have a different ETag suffix because its
control object lives in a different store. `target.branch` or `target.snapshot`
appears only when that target was explicitly requested. In particular, an
explicit `--snapshot ID` is echoed separately as `target.snapshot`; callers
should not infer it by parsing `resolved_snapshot`.

The CLI never follows an external Blob reference. For a whole-object descriptor,
`blob stat` reports the URI with zero target-object I/O and `blob get` exits
nonzero, prints the URI, and suggests `blob stat`. Both commands refuse a ranged
external descriptor because redirecting it would silently widen the selected
cell. This rule also applies remotely: the client does not follow the server's
redirect into caller-owned storage.

Blob replacement and clear commands are not part of this read surface yet;
RFC-033 Phase 3 will add them through the ordinary graph mutation path.

## Branching And Reviewable Data Flows

```bash
omnigraph branch create --uri graph.omni --from main feature-x
omnigraph branch list --uri graph.omni
omnigraph branch merge --uri graph.omni feature-x --into main

omnigraph load --data batch.jsonl --branch review/import-2026-04-09 --from main --mode merge graph.omni
omnigraph export graph.omni --branch main --type Person > people.jsonl
omnigraph commit list graph.omni --branch main --json
omnigraph commit show --uri graph.omni <commit-id> --json
```

## Remote Server Mode

Serve a cluster-applied graph:

```bash
omnigraph cluster apply --config ./company-brain
omnigraph-server --cluster ./company-brain --bind 127.0.0.1:8080
```

Read through the HTTP API — invoke a stored query by name from the catalog:

```bash
omnigraph query get_person \
  --server http://127.0.0.1:8080 \
  --params '{"name":"Alice"}'
```

A server is addressed with `--server` (a name from `~/.omnigraph/config.yaml` or a
literal URL); a positional `http(s)://` URI is rejected. If the server requires
auth, set its bearer token and `omnigraph login <server>` (or
`OMNIGRAPH_BEARER_TOKEN`).

## Multi-graph servers

A server boots from a cluster directory (`omnigraph-server --cluster <dir>`) and
serves every graph the cluster declares. Use `omnigraph graphs list` to enumerate
them. The cluster's server-level policy must allow `graph_list`; `/graphs` is
closed by default even when the server runs with `--unauthenticated`.

```bash
OMNIGRAPH_BEARER_TOKEN=admin-token \
  omnigraph graphs list --server http://server.example.com --json
```

For an operator-defined server, store its token with `omnigraph login <name>` (or
`OMNIGRAPH_TOKEN_<NAME>`); the actor must be authorized by the cluster's
server-level policy.

`list` rejects local (`--store`) targets — it's for remote multi-graph servers only.

Runtime add/remove via API is not exposed. To add or remove a graph, edit the
cluster's `cluster.yaml`, run `omnigraph cluster apply`, then restart the server.

Per-graph addressing: select a graph on a multi-graph server with `--graph`:

```bash
omnigraph query get_person --server http://server.example.com --graph beta --params '{"name":"Ada"}'
```

## Runs, Policy, And Diagnostics

```bash
omnigraph lint  --query queries.gq --schema schema.pg --json
omnigraph check --query queries.gq graph.omni --json

omnigraph schema plan --schema next.pg graph.omni --json
omnigraph schema apply --schema next.pg graph.omni --json
omnigraph policy validate --cluster ./company-brain --graph knowledge
omnigraph policy test    --cluster ./company-brain --graph knowledge --tests policy.tests.yaml
omnigraph policy explain --cluster ./company-brain --graph knowledge --actor act-alice --action read --branch main

omnigraph commit list graph.omni --json
omnigraph commit show --uri graph.omni <commit-id> --json
```

(Mutations and loads publish atomically; the commit graph (`omnigraph commit list`) is the audit surface.)

`query lint` and `query check` are the same command surface. In v1, graph-backed
lint uses local or `s3://` graph URIs; HTTP targets are only supported when you
also pass `--schema`.

## Config

Configuration has two surfaces with single owners (see the
[CLI reference](reference.md#config-surfaces) for the full schema):

- **`~/.omnigraph/config.yaml`** — your personal operator config: default actor
  (`--as`), named servers + credentials, clusters, profiles, aliases, and
  default scope (`defaults.server` / `defaults.store` / `default_graph`). It
  decides *who you are* and *what you address by default*.
- **`cluster.yaml`** (a team-owned cluster directory) — declares *what the system
  is*: graphs, schemas, stored queries, policies, and storage. A server boots
  from it (`--cluster <dir>`); see the [cluster guide](../clusters/index.md).

```yaml
# ~/.omnigraph/config.yaml
operator:
  actor: act-andrew
servers:
  dev:
    url: http://127.0.0.1:8080
defaults:
  server: dev
  default_graph: knowledge
```

When policy is enabled, `schema apply` is authorized through the
`schema_apply` action and is typically limited to admins on protected `main`.

## Deprecated names

The CLI was renamed to align with the HTTP server's canonical endpoint
names (`POST /query`, `POST /mutate`) and the `query` keyword in the GQ
language. The previous spellings keep working forever; invocations emit a
one-line warning to stderr and otherwise behave identically.

| Old (deprecated)         | New (canonical)     | Migration                                                |
|--------------------------|---------------------|----------------------------------------------------------|
| `omnigraph read`         | `omnigraph query`   | Same flags and behavior. `read` is a visible clap alias. |
| `omnigraph change`       | `omnigraph mutate`  | Same flags and behavior. `change` is a visible clap alias. |
| `omnigraph query lint`   | `omnigraph lint`    | Same flags. The argv-level shim rewrites `query lint` to `lint`. |
| `omnigraph query check`  | `omnigraph check`   | `check` is a visible alias of `omnigraph lint`. |

The `command:` field in `aliases.<name>` in `~/.omnigraph/config.yaml` accepts
both `read` / `change` (legacy) and `query` / `mutate` (canonical); the two
spellings are interchangeable on the wire via serde aliases.
