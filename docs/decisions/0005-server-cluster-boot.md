# Server boot from the applied cluster ledger

**Decides:** `omnigraph-server` has one boot source, `--cluster`, and serves exactly the applied revision recorded in the cluster ledger, never the operator's desired configuration and never a mix of the two.
**Contract:** keeps E5, E6
**Code:** `crates/omnigraph-cluster/src/serve.rs`, `crates/omnigraph-server/src/settings.rs`, `crates/omnigraph-server/src/main.rs`
**Cited as:** RFC 0005

## Context

A cluster's operator edits `cluster.yaml` and applies it. If a server read that file, an unapplied edit would reach callers, and two replicas could serve different policies from one ledger. If a server read some facts from the ledger and others from the source bundle, no single record would say what is served. One bad graph must also not stop its healthy siblings from serving.

## Decision

- `--cluster` takes a configuration directory or a storage-root URI. It is the only boot mode; there is no single-graph or file-configured boot.
- The served graph set, stored queries and policies are the ledger's applied revision plus the content-addressed objects it names. Desired configuration is never read for resource content; a divergence is served as applied and shows in `cluster plan`.
- The ledger is serving-sufficient: a policy's bindings (`cluster` or `graph.<id>`) are recorded in the applied revision at apply time, so boot never consults the source bundle.
- A catalog object that is missing or fails its digest refuses boot. So does a missing, malformed or unsupported ledger.
- A graph-local failure (root that does not open, stored query that does not type-check, unresolved provider binding, unsafe Blob policy, legacy recovery evidence) blocks that graph; the others serve. `--require-all-graphs` turns every graph-local failure into a boot failure.
- A nonempty applied set whose graphs all fail refuses to serve (`cluster_no_healthy_graphs`).
- An applied zero-graph revision is a valid boot: the server opens nothing, creates nothing, and reports that revision. An empty desired definition that was never applied is not.
- Listen address and credentials are process facts given by flags and environment, not cluster facts.

The ledger itself, its admission and online activation are in [Server runtime and online deployment](2026-09-29-server-runtime-and-online-deployment.md); the storage root in [Object-storage cluster roots](0006-object-storage-cluster-roots.md); readiness in [Control-plane seams](0049-control-plane-seams.md).

## Rejected

- **Merge the ledger with a local configuration file by precedence:** a fact with two readers has no single answer to what is served.
- **Serve the desired configuration:** unapplied drift would reach callers and the served surface could disagree with the ledger.
- **Read policy bindings from `cluster.yaml` at boot:** boot would silently depend on the source bundle as well as the ledger.
- **Fail the whole server on any graph failure by default:** one bad graph would crash-loop a healthy cluster; strict boot stays available as a flag.
