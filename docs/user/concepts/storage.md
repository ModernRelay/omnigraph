# Storage

One OmniGraph graph lives at one storage root. The root contains the graph's
schema, data, branches, and commit history. Treat it as a single unit: do not
edit files beneath it with storage-provider tools or write to its underlying
datasets directly.

## Supported graph URIs

| URI | Use |
|---|---|
| `/absolute/path/graph.omni` or `file:///absolute/path/graph.omni` | Local filesystem |
| `s3://bucket/prefix/graph.omni` | Amazon S3 or an S3-compatible service |
| `az://container/prefix/graph.omni` | Azure Blob Storage qualification preview |

An `http://` or `https://` URL addresses an OmniGraph server; it is not a graph
storage URI. These are engine/server storage capabilities, not CLI target variants.
Operator version 1 permits only local direct targets. Register a server or managed
target to access remotely served graphs, including servers using cloud storage;
see [operator configuration](../cli/targets.md).

Azure support is a qualification preview: local Azurite validation and a live
managed-identity smoke deployment are complete, but the adversarial live-Azure
matrix is still pending. Every
mutation-capable Azure process must use the admission wrapper described in
[deployment](../deployment.md#azure-blob-preview).

## Consistency and history

Although a graph contains separate data for each node and edge type, OmniGraph
presents one graph-wide snapshot. A successful write makes all affected types
visible together. Readers never see half of a graph commit.

Renaming a type with `@rename_from` preserves its identity and history. Dropping
and later recreating a declaration starts a new lifetime, even when the public
name is reused.

`omnigraph optimize` rewrites physical layout without deleting history.
`omnigraph cleanup` is different: it permanently removes old storage versions,
which can make earlier commits unreadable. Review the retention policy and
backups before confirming it, and quiesce long-lived historical or Blob readers
first. An unconfirmed cleanup echoes the policy but does not inspect candidates.

Cleanup reclaims table versions, not the graph's own catalog. Every graph
commit adds a version of the branch's catalog, and no command removes the
versions of a live branch, so its storage grows with the number of commits;
plan storage for it on write-heavy graphs. A deleted branch's catalog can be
collected by `cleanup` once nothing else depends on it.

## Local filesystem requirement

Local read-write graphs require filesystem hard-link support. Read-only access
can work without it, but initialization and writes fail loudly when the
filesystem cannot provide the required create-if-absent behavior. Use a native
local filesystem rather than a mount that emulates files incompletely.

## S3 configuration

OmniGraph uses the standard AWS credential chain. Common settings include:

- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and optional
  `AWS_SESSION_TOKEN`;
- `AWS_REGION` or `AWS_DEFAULT_REGION`;
- `AWS_ENDPOINT_URL_S3` for an S3-compatible endpoint;
- `AWS_S3_FORCE_PATH_STYLE=true` when the endpoint requires path-style URLs;
- `AWS_ALLOW_HTTP=true` only for a trusted local development endpoint.

A graph's commit protocol relies on the store honouring conditional writes
(`If-None-Match` and `If-Match` on `PUT`), which the server requests on every
S3 binding. The project's S3 test suites run against RustFS; qualify any other
S3-compatible service before production. Google Cloud Storage's S3
interoperability endpoint is untested, and a store that ignores the conditions
can let two writers publish the same graph version. Native `gs://` URIs are not
supported.

## Azure configuration

Azure roots use the strict form `az://<container>/<prefix>`. Credentials never
belong in the URI.

- `AZURE_STORAGE_ACCOUNT_NAME` selects the account.
- `AZURE_STORAGE_CLIENT_ID` optionally selects a user-assigned managed identity.
- `AZURE_STORAGE_ENDPOINT` selects a custom Blob endpoint.
- `AZURE_STORAGE_USE_EMULATOR=true` enables Azurite; use
  `AZURITE_BLOB_STORAGE_URL` to override its endpoint.

Azure Container Apps supplies its managed-identity endpoint variables at
runtime. Restart the process after changing storage-account, endpoint, or
identity selection.

For deployment, server boot, and cloud-specific safety requirements, see
[Deployment](../deployment.md).

## Encryption at rest

The graph's data, history and Blob payloads are ordinary objects in the storage
root, and nothing encrypts them on the way. Encryption at rest is the storage's:
enable default bucket encryption (SSE-S3 or SSE-KMS) on S3, the service's
equivalent on other S3-compatible stores, Azure Storage service encryption, or
an encrypted volume for a local root. There is no field-level encryption; keep
secrets out of graph properties, or encrypt them before they are written.
