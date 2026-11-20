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
| `gs://bucket/prefix/graph.omni` | Google Cloud Storage preview, embedded API only |

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
can let two writers publish the same graph version; use a native `gs://` root
for Google Cloud Storage instead.

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

## Google Cloud Storage configuration

Google Cloud Storage support is a preview. A `gs://` root works for a graph
opened by URI through the embedded Rust API (`Omnigraph::open`,
`Omnigraph::init`). Cluster storage roots, CLI targets and servers do not
accept `gs://` yet. No CI job runs against Google Cloud Storage: the bucket
tests run only when an operator points them at a bucket, and Lance's multipart
uploads of large files have not been exercised there.

Roots use the strict form `gs://<bucket>/<prefix>`. Credentials never belong
in the URI. OmniGraph reads its Google configuration once per process from
`GOOGLE_*` environment variables and gives the same options to the client for
control objects and to Lance's client for datasets:

- `GOOGLE_SERVICE_ACCOUNT` (a key file path) or `GOOGLE_SERVICE_ACCOUNT_KEY`
  (the key itself) selects a service account.
- Otherwise `GOOGLE_APPLICATION_CREDENTIALS`, or the
  `gcloud auth application-default login` file, supplies Google's
  application-default credentials, and then the metadata server of the
  Compute Engine, GKE or Cloud Run instance.
- `GOOGLE_STORAGE_TOKEN` or `GOOGLE_BEARER_TOKEN` supplies a fixed access token.
- `GOOGLE_BASE_URL` and the `GOOGLE_`-prefixed client options (for example
  `GOOGLE_HTTP1_ONLY`) apply to both clients.

Opening a `gs://` root refuses an environment that names a Google option
without the `GOOGLE_` prefix, such as `SERVICE_ACCOUNT`, `BEARER_TOKEN`,
`BASE_URL` or `TIMEOUT`: Lance would read such a name and the control-object
client would not. It also refuses two spellings of one option with different
values. Restart the process after changing any captured variable. A workload
identity federation (`external_account`) credentials file is not supported;
use the metadata server or a service-account key.

The identity needs read, create, overwrite, delete and list on objects in the
bucket, for example `roles/storage.objectUser`. Every conditional write is
checked against the object generation (`x-goog-if-generation-match`), so
graph publication keeps the same single-winner guarantee as on S3. Google
limits writes to one object name to about one per second.

Running one writer process per graph remains the operator's responsibility.
An embedded program can fence other writers out of a branch with
`claim_branch_writer`, as on any backend; see
[writer epoch](../operations/troubleshooting.md#writer-epoch).

For deployment, server boot, and cloud-specific safety requirements, see
[Deployment](../deployment.md).

## Encryption at rest

The graph's data, history and Blob payloads are ordinary objects in the storage
root, and nothing encrypts them on the way. Encryption at rest is the storage's:
enable default bucket encryption (SSE-S3 or SSE-KMS) on S3, the service's
equivalent on other S3-compatible stores, Azure Storage service encryption, or
an encrypted volume for a local root. There is no field-level encryption; keep
secrets out of graph properties, or encrypt them before they are written.
