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
storage URI. CLI commands use `--server` for server URLs and `--store` for direct
storage access.

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
