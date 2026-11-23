# Native Azure Blob storage

**Decides:** Azure Blob is a native backend addressed as `az://<container>[/<prefix>]` through the existing Lance and `object_store` clients, and every Azure writer process is admitted, one per cluster, by an infinite Blob lease held by `omnigraph-azure-admission`.
**Contract:** keeps E2, write:3
**Code:** `crates/omnigraph-storage/src/lib.rs`, `crates/omnigraph-azure-admission`, `deploy/azure`
**Cited as:** RFC 0029

## Context

Azure operators could run an S3-compatible service or mount Azure Files, but
neither gives Entra RBAC, managed identity or Blob conditional writes. Lance
and `object_store` already speak Azure, so the backend can reuse one storage
adapter and one publication path. The engine still supports one writing
process per cluster, and Container Apps replica counts are targets that the
platform may exceed during maintenance. Something outside the engine has to
keep a second writer from starting.

## Decision

- Normal binaries enable `lance/azure` and `object_store/azure`. There is no
  Azure SDK, Azure-specific manifest or Azure-only engine path.
- The container is the URI authority. Account and endpoint come from the
  environment, captured once per process and replayed into both clients, so
  control objects and datasets cannot split across accounts. Userinfo,
  ports, queries, fragments, other Azure URI forms, empty or dot segments
  and encoded separators refuse. One adapter serves one container. An
  unknown or disabled scheme refuses; it is never a local path.
- Managed identity is the production credential. `MSI_ENDPOINT` is accepted
  only when equal to `IDENTITY_ENDPOINT`. Azurite is selected by
  `AZURE_STORAGE_USE_EMULATOR` and expanded to an explicit endpoint.
- Control compare-and-swap uses Blob ETags: create is `If-None-Match: *`,
  update is `If-Match`. On any backend with conditional update, a read or
  write without an ETag is a storage error, never a content hash.
- Rename is ETag-pinned range reads, a put, then a delete. It is not
  atomic, and recovery tolerates both names.
- Every Azure writer, server or job, runs under `omnigraph-azure-admission
  run`. Before starting its child it positively acquires an infinite lease
  on `__omnigraph_azure_admission/v1/<sha256 of the canonical root>/writer.lock`,
  outside the cluster root. An ambiguous acquire admits nothing unless a
  renew with the same lease id proves ownership.
- On `SIGTERM` the wrapper drains the whole child process group, then
  releases with the owning lease id. A crash, kill or nonzero bootstrap
  leaves the lease held. `break` is an explicit operator command after the
  runbook's proof that no old process lives.
- The lease is cooperative admission, not a fencing token. Replica targets
  are sizing, not correctness.
- The admission crate depends only on `omnigraph-storage` and exposes no
  graph or control-object read or write.

## Rejected

- **An S3-compatible service on Azure:** another stateful service and
  credential model, with no managed-identity path to Blob.
- **Azure Files with the local backend:** routes distributed storage through
  local-filesystem semantics, whose compare-and-swap is not cross-process.
- **An Azure SDK for storage I/O:** duplicates authentication, retries,
  conditional writes and listing, and can diverge from Lance's client.
- **Every Azure URI alias at once (`abfs://`, `abfss://`, HTTPS):** one
  canonical form keeps URI identity, errors and tests exact.

## Not built

- The adversarial live-Azure qualification: a checked script that runs the
  reference deployment and proves one child under forced concurrent
  replicas and overlapping jobs, a drain delaying the successor on
  `SIGTERM`, and a held lease after a kill or crash. Until it passes, Azure
  writes are a preview. Passing would close the preview part of `G4`; the
  lease requirement stays until cross-process writers are supported (`G2`).
