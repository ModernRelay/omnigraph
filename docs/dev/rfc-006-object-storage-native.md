# RFC-006: Object-Storage-Native OmniGraph

**Status:** Proposed
**Depends on:** RFC-005 (cluster serving, landed), Phase 4 (landed)
**Decides:** how the cluster control plane migrates from raw-filesystem I/O to
object storage, and what "local filesystem support" means afterwards.

## Motivation

The engine is already object-storage-native: Lance datasets live behind the
`object_store` abstraction, S3/RustFS graphs are CI-tested, and the classic
single-graph deployment runs stateless against a bucket. The **cluster control
plane is not**: the state ledger, lock, catalog blobs, recovery sidecars,
approval artifacts, and — most consequentially — the derived graph roots are
all raw `fs::*` against the config directory. Consequences:

- A cluster cannot put its data on S3 at all; cloud deployments need a
  persistent volume, abandoning the stateless-bucket shape the classic mode
  already has (cookbooks PR #12 validated it on Railway).
- The control plane carries a *second* storage layer with different semantics
  (rename-CAS, `read_dir`, `remove_dir_all`) instead of the one the substrate
  already provides.

The directive this RFC implements: **OmniGraph is object-storage only.** One
storage interface; every stored byte addressed by URI; S3-compatible object
storage is the deployment model. The local filesystem does not disappear — it
is demoted from "a separate code path" to "one backend of the same interface"
(`file://`), retained for development and tests, and removed from the
production story.

## Verified foundations

- **S3 conditional writes work on the target backend.** Tested against RustFS
  1.0.0-beta.8 (2026-06-11): `PUT` with `If-None-Match: *` → first write 200,
  second 412; `PUT` with `If-Match: <etag>` → fresh etag 200, stale 412. AWS
  S3 has shipped both since 2024/2025; the `object_store` crate exposes them
  as `PutMode::Create` / `PutMode::Update(version)`.
- **Lance 6.x commits natively via S3 conditional writes** — the engine's own
  multi-version safety on S3 needs no external lock table.
- **`Omnigraph::init/open` accept S3 URIs today** — derived graph roots on S3
  require no engine work.

## Design

### D1. One storage interface (the rule)

A new sealed `ClusterStore` abstraction (thin over the `object_store` crate,
reusing the engine's `storage_for_uri` URI plumbing) carries **every** cluster
read/write: state ledger, lock, catalog payloads, recovery sidecars, approval
artifacts. Raw `fs::*` in `omnigraph-cluster` for stored state becomes a
**deny-list entry** in [invariants.md](invariants.md). Backends: `s3://` (and
any S3-compatible endpoint — RustFS, MinIO, R2) and `file://` (dev/test).

The one deliberate exception: **declared configuration** — `cluster.yaml` and
the `.pg`/`.gq`/policy files it references — stays in the operator's working
tree, read-only, exactly like Terraform reads `.tf` files locally while the
*state backend* is remote. Config is versioned in git; state and data live in
the store.

### D2. The `storage:` root

```yaml
version: 1
storage: s3://omnigraph-local/clusters/intel   # the cluster's home
graphs:
  spike:
    schema: schema.pg          # config: read from the working tree
    queries: queries/
```

Everything currently under `<config-dir>/__cluster/` and `<config-dir>/graphs/`
moves under the storage root:

```
s3://omnigraph-local/clusters/intel/
├── state.json                  # the ledger (CAS via conditional put)
├── lock.json                   # create-only put; force-unlock = delete
├── resources/…                 # content-addressed catalog (immutable puts)
├── recoveries/…                # sidecars
├── approvals/…                 # approval artifacts
└── graphs/<id>                 # derived Lance roots (engine-native S3)
```

During the migration window `storage:` defaults to `file://<config-dir>`
(today's layout, byte-compatible). After the deprecation boundary (D8) the key
is **required** — naming your storage is the point; `file://` remains legal
but explicit.

Credentials are never in `cluster.yaml`: the standard `AWS_*` env contract
(already documented for the engine) applies to the control plane identically.

### D3. Ledger CAS and locking on object storage

- `write_state` (today: temp file + rename, guarded by recorded `state_cas`)
  becomes `PutMode::Update(etag)` — the etag read with the state replaces the
  sha256 sidecar field as the CAS token (the sha256 stays as content identity
  in audit/output). A 412 maps to the existing `state_cas_conflict` path.
- `acquire_lock` becomes `PutMode::Create` on `lock.json`; 412 → held (read
  the holder for the message); `force-unlock <id>` = read, verify id, delete.
  Same semantics as today, now correct across machines — which the file
  backend never was.
- Latency: one GET + one conditional PUT per command on the happy path —
  noise next to graph opens. The recovery sweep adds a LIST of `recoveries/`.

### D4. Catalog, sidecars, approvals

Mechanical ports: content-addressed payloads are immutable puts (idempotent by
construction — a re-put of the same digest is a no-op); sidecars and approvals
are small JSON objects with LIST + GET + DELETE lifecycles. Approval files
gain nothing; their digest-binding semantics are storage-agnostic.

### D5. Derived graph roots become URIs

`<storage>/graphs/<id>` replaces `<config-dir>/graphs/<id>.omni`. Executors:
create = `Omnigraph::init(uri)` (works today); schema apply = `open(uri)`
(works today); approved delete = object-store **prefix delete** replacing
`remove_dir_all`. The recovery sweep's `root.exists()` becomes a prefix LIST
(non-empty = exists). Tombstones and the digest classification logic are
unchanged — they never depended on the filesystem.

### D6. Serving from a bucket

`omnigraph-server --cluster <uri>` accepts the storage root URI directly
(`--cluster s3://omnigraph-local/clusters/intel`). `read_serving_snapshot`
reads ledger + catalog through `ClusterStore`; graphs open by their S3 URIs.
**A cluster deployment becomes stateless again**: no volume, restart-to-adopt
unchanged, replicas trivially safe (boot is read-only). Railway = service +
Bucket; ECS = task + S3; the PR-#12 topology and the cluster topology
converge.

### D7. Migration tooling

`omnigraph cluster migrate-storage <dest-uri> --config <dir>`: object-copy
graphs + catalog + approvals to the destination (Lance layouts are
path-relative; immutable files copy safely), write the ledger last via
create-only put, then print the `storage:` line to commit into `cluster.yaml`.
Idempotent and resumable (copy is keyed by listing diff; the ledger write is
the atomic cutover). The reverse direction works identically (S3 → `file://`
for local debugging). Fallback path: `cluster import` against live S3 graphs
already reconstructs a lost ledger.

### D8. Deprecation of local-FS as an operating mode

Per axiom 15's bridge rule (every bridge names its replacement and sunset):

| Phase | local FS status |
|---|---|
| Now → Stage C lands | implicit default (`storage:` absent ⇒ `file://<config-dir>`) |
| Stage D (docs flip) | S3-first everywhere: docs, cookbooks, skills, deployment recipes; `file://` documented **only** under development/testing; absent `storage:` emits a deprecation warning naming this RFC |
| v0.9 boundary | `storage:` **required**; `file://` stays a legal explicit backend for dev/test — it is the same code path, costs no second implementation, and keeps `cargo test` hermetic (no daemon dependency in unit tests) |

**Recommendation embedded here (the one place this RFC pushes back):** "object
storage only" is enforced at the *interface* level — one code path, every
location a URI — not by deleting the `file://` backend. Hard removal would
force a RustFS daemon into every unit test and air-gapped dev loop while
deleting zero code (the backend ships inside `object_store` either way).
Terraform's local state backend survives for the same reason its S3 backend is
still the only one anyone deploys. If a harder line is wanted later, it is a
docs-and-validation flip, not an architecture change.

## Staging

| Stage | Delivers | Size |
|---|---|---|
| **A** | `ClusterStore` + ledger/lock/sidecars/approvals/catalog ported; `file://` behavior byte-compatible; S3 backend live behind `storage:`; conditional-put CAS + cross-machine lock; RustFS-gated integration tests | the big one — touches every backend call in `omnigraph-cluster` |
| **B** | URI graph roots: executor init/apply/delete + sweep on URIs; prefix-delete; e2e: full lifecycle against RustFS | medium |
| **C** | `--cluster <uri>` serving + bucket-backed snapshot reads; system e2e: apply to RustFS, serve from it; Railway Bucket deploy validated (closes the loop with cookbooks PR #12's topology) | medium |
| **D** | `migrate-storage`, docs flip (S3-first), cookbooks/skills update, deprecation warning, deny-list entry | small code, wide docs |

Each stage is a PR with the usual gates; A and B are separable but land best
back-to-back (B is where the user-visible payoff starts).

## Open questions

1. **RustFS GA & conditional-write contract stability** — beta.8 passes both
   probes; pin the probes as a `lance_surface_guards`-style integration test
   so a regression in a RustFS bump turns red here, not in production.
2. **Multi-writer ergonomics** — conditional puts make concurrent applies
   *safe* (one wins, one gets `state_cas_conflict`); whether we want lease
   semantics (lock TTL + auto-break) is a later UX question, not a
   correctness one.
3. **Catalog GC on object storage** — deletes leave blobs today on FS too;
   the existing gap carries over unchanged, tracked separately.
4. **`--config` ergonomics** — once state is remote, two operators sharing a
   bucket need only the config repo; document the "config in git, state in
   S3" workflow as the primary pattern (it is the Terraform workflow).

## What this explicitly does not change

Engine storage (already object-store native), the `.pg`/`.gq` languages, the
plan/apply/approve model, recovery semantics (sidecar classification is
storage-agnostic), the serving API surface, and `omnigraph.yaml`'s
per-operator role.
