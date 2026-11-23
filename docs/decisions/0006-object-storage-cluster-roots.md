# Object-storage cluster roots

**Decides:** a cluster has one storage root, local, `s3://` or `az://`, that holds its control state and its derived graph roots through one backend configuration, so a server can serve from the root URI alone.
**Contract:** none; serves invariant 12
**Code:** `crates/omnigraph-storage`, `crates/omnigraph-cluster/src/store.rs`, `crates/omnigraph-cluster/src/config.rs`
**Cited as:** RFC 0006

## Context

Graph datasets can live on object storage. If the cluster's control state stayed in a local directory, there would be two deployment authorities, every serving host would need a configuration checkout, and local and object-store behavior would drift. The desired configuration still belongs in source control, where it is reviewed.

## Decision

- One root holds `__cluster/` (ledger, lock, content-addressed resources, operation record) and `graphs/<graph_id>.omni/` for each declared graph. A graph root is derived from the cluster root and the graph id; there is no second graph-location registry.
- The root may be a local path, `file://`, `s3://bucket/prefix` or `az://container/prefix`. Azure carries its own admission and qualification boundary.
- A root URI holds location only. Credentials and endpoints come from the backend environment and are never stored in a URI or in cluster state. A diagnostic that names a root removes userinfo, query strings and fragments.
- Every root is normalized once. The CLI selects the root from its target; a `storage:` value in `cluster.yaml` is an equality assertion against that root and a mismatch refuses before anything is submitted.
- All control-object I/O goes through the shared storage adapter: a replacement is visible whole, compare-and-swap uses the backend's version or ETag, a missing object is distinct from an unreadable one, and listings are sorted before they drive any decision.
- Object stores get no directory or rename semantics. A recursive delete is not atomic and runs only inside a protocol that records intent first and resumes.
- The root is a meeting point, not writer fencing. Writer exclusion is cluster admission and, on Azure, `omnigraph-azure-admission`.
- Moving a cluster to another root is an operator migration under quiescence, not a URI edit.

## Rejected

- **Keep control state local while graphs are remote:** two authorities, and no configuration-free serving.
- **A state backend unrelated to graph storage:** adds bootstrap, credential, failure and recovery surfaces with no requirement for them.
- **Infer a cluster root from any graph URI:** only the canonical `graphs/<id>.omni` layout proves the relation; a standalone graph must not trigger storage probes or silent retargeting.
