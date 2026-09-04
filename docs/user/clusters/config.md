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

`apply` can create graphs, apply supported soft schema changes, publish query
and policy resources, and execute approved graph deletion. It does not load
graph data, start servers, or perform hard schema drops.

See [Operating a cluster](index.md) for the end-to-end workflow.
