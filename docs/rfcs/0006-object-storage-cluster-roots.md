---
rfc: "0006"
title: "Object-storage cluster roots"
track: maintainer
status: accepted
implementation: complete
authors:
  - OmniGraph maintainers
created: 2026-06-10
updated: 2026-08-23
discussion: null
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC 0006: Object-storage cluster roots

## Summary

A cluster has one storage root for its applied state, catalog objects, recovery
and approval records, and derived graph roots. The root may be a local path,
`file://` URI, `s3://bucket/prefix`, or `az://container/prefix`. Cluster control
objects and Lance graph datasets use the same normalized backend configuration,
so an object-storage cluster does not split authority between a local control
directory and remote graph data.

`cluster.yaml` and the schema/query/policy source files it references remain in
the operator's working tree. Applying that declaration publishes the served
state under the storage root. A server can then boot from the root URI alone;
it does not need the source bundle.

Azure support was added later by [RFC 0029](0029-azure-blob-storage.md). Its
qualification and writer-admission restrictions apply in addition to this
root contract.

## Motivation

The original cluster control plane assumed local files even when graph datasets
lived on S3. That created two deployment authorities, required serving hosts to
carry a configuration checkout, and let local-file and object-store behavior
drift. One storage adapter and one root keep the applied cluster state beside
the graph roots it describes while leaving desired configuration reviewable in
source control.

## Contract

### Root selection

- If `cluster.yaml` omits `storage`, the normalized root is the configuration
  directory and existing local layouts remain byte-compatible.
- An explicit local path or `file://` URI selects the local backend.
- `s3://bucket/prefix` selects the S3-compatible backend.
- `az://container/prefix` selects native Azure Blob storage under RFC 0029's
  qualification boundary.
- Root URIs contain location only. Credentials and endpoint configuration come
  from the backend environment contract; secrets are never embedded in a URI
  or persisted in cluster state.

Every accepted root is normalized once, without a trailing slash. Invalid or
ambiguous paths, credential-bearing URIs, query strings, and fragments are
rejected with diagnostics that redact secret-like input.

### Layout

The applied root owns these logical areas:

```text
<root>/
├── __cluster/                 applied state, lock, approvals, recovery,
│   └── resources/             content-addressed catalog payloads
└── graphs/<graph-id>.omni/    one derived graph root per declaration
```

The exact internal filenames and schemas may evolve behind their versioned
readers. The stable contract is that graph roots derive from the cluster root
and graph ID; callers do not maintain a second graph-location registry.

### Storage semantics

All cluster control-object I/O goes through the shared storage adapter:

- a JSON replacement is atomically visible as a complete object;
- compare-and-swap uses the backend's version/ETag evidence;
- a missing object is distinct from an unreadable or malformed object;
- listings are sorted before they drive recovery or reconciliation;
- recursive prefix deletion is not atomic and therefore belongs only inside an
  idempotent, approval- and recovery-aware protocol.

Object stores do not gain directory or rename semantics merely because the
local implementation can provide them. Callers must tolerate retry and
re-observation at every multi-object boundary.

### Serving and operations

`omnigraph-server --cluster <root>` reads the last applied revision from the
root and serves only that revision. It never treats the operator's desired
files as served authority. Cluster commands may use a configuration directory
or the configured storage root; direct maintenance resolves
`--cluster <root> --graph <id>` to the derived graph root.

The root is a rendezvous point, not distributed writer fencing. Deployments
must still obey the documented single-writer-process boundary or provide the
backend-specific admission mechanism. In particular, every mutation-capable
Azure process must run through `omnigraph-azure-admission`.

## Invariants

1. Applied state, not desired configuration, is the server's authority.
2. Cluster control objects and graph datasets resolve through one backend
   configuration for a given root.
3. A graph root is derived from the cluster root and stable graph ID.
4. Control-object writes are never acknowledged before their complete object is
   durable and visible.
5. Multi-object actions remain explicit recovery protocols; no backend is
   assumed to make them atomic.
6. Credentials stay outside persisted configuration and diagnostic output.

## Compatibility and reversibility

Omitting `storage` preserves the original local layout. Moving a live cluster
to another root is an operator migration, not a transparent URI edit: copy or
rebuild the state and graphs under quiescence, validate the target, and boot
from the new root. Azure-specific compatibility remains governed by RFC 0029.

## Evidence

The shipped contract is covered by shared adapter tests, local cluster tests,
and the configured S3/Azure full-lifecycle suites. Those suites exercise
import, apply, graph creation, catalog publication, config-free serving,
recovery, lock behavior, and derived graph addressing through the same public
root shape.

## Alternatives

- **Keep control state local while graphs are remote.** Rejected because it
  creates two authorities and prevents config-free serving.
- **Give cluster state a backend unrelated to graph storage.** Deferred. It
  adds bootstrap, credential, failure, and recovery surfaces without a current
  requirement.
- **Infer a cluster root from any graph URI.** Rejected. Only the canonical
  `graphs/<id>.omni` layout proves that relationship; arbitrary standalone
  graphs must not trigger storage probes or silent retargeting.
