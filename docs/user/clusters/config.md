# Cluster configuration reference

Cluster commands read a directory containing `cluster.yaml`:

```bash
omnigraph cluster validate --config ./company-brain
```

`--config` defaults to the current directory. Unknown fields and duplicate YAML
keys are errors so that misspelled intent is never ignored.

## Complete shape

```yaml
version: 1

metadata:
  name: company-brain

# Omit for a cluster stored in this directory.
storage: s3://company-data/omnigraph/company-brain

state:
  backend: cluster
  lock: true

providers:
  embedding:
    default:
      kind: openai-compatible
      base_url: https://api.example.com/v1
      model: text-embedding-3-large
      api_key: ${EMBEDDING_API_KEY}

graphs:
  knowledge:
    schema: knowledge.pg
    queries: queries/
    embedding_provider: default
    external_blobs:
      allow:
        - base: s3://company-assets/knowledge/
          scope: server_safe

policies:
  graph-access:
    file: graph.policy.yaml
    applies_to: [knowledge]
  server-access:
    file: server.policy.yaml
    applies_to: [cluster]
```

## Top-level fields

| Field | Required | Meaning |
|---|---:|---|
| `version` | yes | Configuration schema; currently `1` |
| `metadata.name` | no | Display name |
| `storage` | no | Cluster root: local by default, or `file://`, `s3://`, `az://` |
| `state.backend` | no | Omit or set to `cluster` |
| `state.lock` | no | Serialize cluster operations; defaults to `true` |
| `providers.embedding` | no | Named embedding provider profiles |
| `graphs` | no | Graph declarations keyed by graph ID |
| `policies` | no | Policy bundles keyed by bundle name |

Credentials are process configuration and must not appear in
`cluster.yaml`.

## Graphs

Each graph requires a schema file:

```yaml
graphs:
  knowledge:
    schema: knowledge.pg
```

Optional fields:

| Field | Meaning |
|---|---|
| `queries` | Stored-query files, directories, or explicit name mappings |
| `embedding_provider` | Name under `providers.embedding` |
| `external_blobs` | Allow-list for new external Blob references |

Query declarations support three forms:

```yaml
# Every declaration in top-level *.gq files in a directory
queries: queries/

# Every declaration in these files or directories
queries: [people.gq, reports/]

# Explicit registry names
queries:
  find_experts:
    file: knowledge.gq
```

Unreadable files, parse errors, duplicate query names, and queries that do not
type-check against the graph's desired schema fail validation.

## Embedding providers

Provider `kind` may be `openai-compatible`, `openai`, `gemini`, or `mock`.
Real providers require `api_key: ${ENVIRONMENT_VARIABLE}`; inline secrets are
rejected. The environment variable is resolved when the server boots, not by
`cluster validate`, `plan`, or `apply`. Vector dimensions remain part of the
graph schema.

See [Embeddings](../search/embeddings.md) for provider behavior.

## External Blob references

New external references are denied unless their normalized URI falls under an
allowed base:

```yaml
external_blobs:
  allow:
    - base: s3://company-assets/knowledge/
      scope: server_safe
```

`server_safe` permits the base for a served graph. `embedded_only` is for an
embedded host and may permit a local `file://` directory; it is not installed
by the HTTP server or direct-store CLI. Bases must be absolute, non-overlapping,
and free of credentials, query strings, fragments, and path traversal.

A base must also name storage outside the cluster's storage root: the config
directory when `storage` is omitted, or the `storage` URI. That root holds every
graph and the applied state, and ingress reads with the process's own storage
credentials, so a base over it would let any writer copy another graph's data
or the cluster ledger into a readable Blob value. `cluster validate`, `plan`,
and `apply` refuse such a base with `external_blob_base_overlaps_storage_root`,
whatever its scope. Put external objects under a sibling prefix instead, for
example `s3://company-assets/cluster-external/` beside
`storage: s3://company-assets/cluster`. A server that finds an overlapping
`server_safe` base in the applied state quarantines that graph and serves the
others; if no applied graph is left to serve, startup fails with
`cluster_no_healthy_graphs`. An embedded handle refuses a policy whose base
overlaps its own graph root.

A base is compared only with a storage root of its own kind: an `s3://` base
with an `s3://` root, a `file://` base with a local root. When the root is
spelled with a path component a base URI cannot express (an empty component
such as `s3://bucket/a//cluster`, or a percent sign in a local path), a
same-kind base is refused with `external_blob_storage_root_uncomparable`,
because disjointness cannot be proven. Moving the base does not clear that
code; the storage root spelling does.

The allow-list controls which external objects an authorized writer may cause
the process to inspect. Cedar policy separately decides who may write. See
[Blob values](../blobs.md).

## Policies

```yaml
policies:
  graph-access:
    file: graph.policy.yaml
    applies_to: [knowledge, catalog]
  registry-access:
    file: server.policy.yaml
    applies_to: [cluster]
```

A bundle targets either graph IDs or the `cluster` server scope, never both.
Only one bundle may bind a given graph or the cluster scope. See
[Authorization](../operations/policy.md).

## Storage

When `storage` is omitted, applied state and graph data live under the config
directory. An `s3://` or `az://` value puts them under that object-storage root;
the source bundle still stays in the operator's working tree.

Use the standard storage credential environment for the chosen backend. Azure
is a qualification preview and requires the admission wrapper for every writer;
see [Deployment](../deployment.md#azure-blob-preview).

## Declared paths

Every `schema`, `queries`, and policy `file` path is resolved against the
directory that holds `cluster.yaml`. A relative path must stay inside that
directory: a `..` segment is refused with `config_path_escape`, and a path
that reaches its file through a symbolic link, on the way to it or as a
query file discovered inside a declared directory, is refused with
`config_path_symlink`. Each diagnostic names the setting that declared the
path. The bundle is one directory of files read exactly as declared, so what
gets applied never depends on something outside it.

An absolute path is accepted as given and is not checked for either shape.
Prefer relative paths; they are what keep a bundle portable and hermetic.

## Command behavior

| Command | Changes graph or cluster state? | Use |
|---|---:|---|
| `validate` | no | Parse and type-check the declaration |
| `plan` | no | Preview creates, updates, and deletes |
| `apply` | yes | Converge to the declaration |
| `approve` | yes | Approve one exact destructive plan item |
| `status` | no | Read recorded state and lock status |
| `refresh` | state only | Refresh observations for declared graphs |
| `import` | state only | Adopt existing declared resources |
| `force-unlock` | yes | Remove one proven-stale lock by exact ID |
| `upgrade-ledger` | state only | Convert an applied, stopped cluster to durable offline deployments |

On a v1 ledger, `apply` can create graphs, apply supported schema changes, publish query
and policy resources, and execute approved graph deletion. It does not load
graph data or start servers. A schema drop removes the data from the branch
head and reclaims nothing at apply; `omnigraph cleanup` is the step that makes
it unrecoverable. The plan shows such a step as `drop_property` or `drop_type`,
with no mode.

After explicit conversion to a v2 ledger, apply supports only schema and stored
query changes on existing graphs, and requires `state.lock: true`. Other
bindings and inventory remain fixed. Root-addressed status, reconciliation and
conversion do not use `cluster.yaml`; see [the deployment workflow](index.md#durable-offline-deployments).

## Limits

Configuration loading refuses limits before schema effects. Shared files with
identical bytes count once toward the aggregate source limit.

| Resource | Limit |
|---|---:|
| Each `cluster.yaml`, schema, query or policy source | 1 MiB |
| Distinct source bytes in one captured bundle | 8 MiB |
| Declared resources (each graph and schema count separately) | 4096 |
| Query discovery paths plus directory entries, including ignored files | 4096 |
| Encoded immutable deployment bundle | 16 MiB |
| Encoded cluster ledger | 16 MiB |
| Lock metadata read | 64 KiB |
| Outstanding deployments | 1 |
| Retained deployment results | 32 records, 4 MiB total, 1 MiB each |
| Deployment actor / resource address / canonical root | 256 / 512 / 4096 UTF-8 bytes |

Deployment admission checks the actual encoded prepared intents and reserves
ledger/result space to record completion before accepting schema effects.
JSON escaping and prepared state can make a deployment exceed its encoded limit
even when raw source bytes fit. Result history may evict older completed records
to admit a new deployment; outstanding authority is retained. These bounds do
not cap graph data, native manifest-history scans or total process memory.

See [Operating a cluster](index.md) for the end-to-end workflow.
