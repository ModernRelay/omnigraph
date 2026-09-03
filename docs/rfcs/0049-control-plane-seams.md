---
rfc: "0049"
title: "Control-plane seams: observe, readiness witness, bounded shutdown"
track: maintainer
status: accepted
implementation: not-started
authors:
  - OmniGraph maintainers
created: 2026-09-03
updated: 2026-09-03
discussion: null
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC 0049: Control-plane seams: observe, readiness witness, bounded shutdown

## Summary

Three small, independently shippable contracts let an external control plane
drive a cluster without a second implementation of anything the cluster
crate already does, and without bypassing it:

1. **Observe-only reads.** `cluster plan --observe` and a new
   `cluster observe` read the ledger and the live graphs without taking the
   cluster lock and without writing anything, and label their output
   `authority: observed` together with the exact `state_cas` they read.
2. **Readiness witness.** `GET /readyz` reports, without authentication,
   whether the server is serving or draining, the applied `config_digest` it
   booted from, the ledger revision and CAS it read, and how many graphs it
   serves and does not serve. The graph ids stay behind the existing
   authenticated `GET /graphs`, which gains the quarantined list.
3. **Bounded shutdown.** `--shutdown-grace-seconds` (default 25) puts one
   deadline on graceful shutdown: readiness turns off at the signal, in-flight
   requests drain, and at the deadline an operating-system thread exits the
   process non-zero instead of waiting forever.

Nothing here changes a storage format, the ledger's schema, the lock, the
recovery protocol, or any existing route's success shape. The wider designs of
RFC 0034 (recovery authority) and RFC 0035 (served-operation ownership) stay
independent; this RFC takes none of their decisions and its seams remain valid
under them. Restoring a ledger is deliberately not here: its real use arrives
with coherent restore points, where the ledger and the graphs come back
together, and it will be designed once, against those.

## Motivation

An operator that manages many clusters needs two things from the engine it
does not have today.

**A drift signal without a lock.** `plan` and `refresh` take
`__cluster/lock.json` (create-if-absent, deleted on release). A service that
observes hundreds of clusters would create and delete lock files on roots it
does not own, would be refused whenever an apply holds the lock, and would
have no way to say that what it returned was an observation rather than a
locked read. `refresh` additionally writes the ledger and runs the recovery
sweep, so it can only run while nothing else moves the cluster. The
`state.lock: false` bypass exists but is a configuration setting with a
warning, not a per-command intent.

**An honest replica.** `/healthz` reports the process is alive. Nothing
reports which applied revision a replica actually booted from, which graphs it
quarantined, or that it has started draining; an orchestrator replacing a
cohort cannot tell a replica serving the old revision from one serving the new
one. On the way down, axum's graceful shutdown has no bound, so a stalled
connection keeps a replica alive past any orchestration grace period, and the
orchestrator's kill is indistinguishable from a crash.

Both gaps are small and local. Neither needs RFC 0034's recovery modes or RFC
0035's admission cells to close; both of those remain the right larger
designs for the engine and the server, and this RFC is careful not to
preempt them.

## User and operational behavior

### Observe-only reads

```bash
omnigraph cluster plan --observe --config ./company-brain
omnigraph cluster observe --config ./company-brain
```

`plan --observe` is `plan` without the lock: it reads the ledger once, diffs
the desired bundle against it, and reports. `cluster observe` is `refresh`
without the lock, the sweep, or the write: it verifies catalog payloads and
observes every declared graph through the read-only open, and reports the
resource statuses and observations `refresh` would have recorded.

Both outputs carry `authority: "observed"`. The existing paths carry
`"locked"`, or `"unlocked"` when the bundle sets `state.lock: false`, so a
read never claims a lock it did not hold. Every output carries the
`state_cas` and `state_revision` of the ledger it read.
An existing lock is reported in `state_observations` (`locked`, `lock_id`,
`lock_operation`, `lock_age_seconds`) and does not refuse the command. Pending
recovery sidecars are reported as the `cluster_recovery_pending` warning that
read-only commands already emit; nothing is swept.

An observed result is never authority for an effect: `apply` still re-plans
under the lock, and an approval still binds to the digests `apply` sees.

### Readiness witness

```text
GET /readyz
200 {"ready": true, "status": "serving", "booted_serving_digest": "<sha256>",
     "state_revision": 42, "state_cas": "sha256:…",
     "served_graph_count": 3, "quarantined_graph_count": 0,
     "shutdown_grace_seconds": 25}
503 {"ready": false, "status": "draining", …same fields…}
```

Unauthenticated, like `/healthz`, and therefore minimal: graph ids are
topology, which the existing `GET /graphs` deliberately puts behind bearer
authentication and the Cedar `graph_list` action, so `/readyz` reports only
counts. `GET /graphs` gains `quarantined`, the ids the applied revision names
that this process does not serve, under the same gate. `booted_serving_digest`
is the `applied_revision.config_digest` of the ledger the process booted
from; it is fixed for the life of the process, because the server never
reloads. The digest and CAS are hashes of configuration bytes: they say
whether two replicas booted the same revision and nothing else. `/healthz` is
unchanged: it answers 200 while the process is alive, draining included.

### Bounded shutdown

```bash
omnigraph-server --cluster … --shutdown-grace-seconds 25
OMNIGRAPH_SHUTDOWN_GRACE_SECONDS=25
```

The signal listener is installed when `serve` starts, before any graph opens,
so the bound covers startup. At SIGTERM or Ctrl-C the server marks itself
draining (`/readyz` answers 503), starts an operating-system thread that
sleeps for the grace, stops accepting connections, and lets in-flight requests
finish. If they have finished before the deadline, the process exits 0 as it
does today. At the deadline the thread logs the unfinished work and exits with
status 2; being a thread, it does not depend on the async runtime making
progress, so a blocked executor or a stalled teardown cannot postpone it. Zero
means immediate cutoff. The flag wins over `OMNIGRAPH_SHUTDOWN_GRACE_SECONDS`,
which is read only when the flag is absent. The orchestrator's own termination
grace must be longer than this value; the deployment guide says so.

A cutoff is crash-equivalent for the work it interrupts: the engine's existing
durability and next-open recovery remain the authority, exactly as for a
crash. Nothing is deleted or repaired at the deadline.

## Design

**Observe.** `plan_config_dir_with_options(dir, PlanOptions { observe })`
and `observe_config_dir(dir)` in `omnigraph-cluster`. The observe path calls
`ClusterStore::observe_lock` (a read of `lock.json`) where the locked path
calls `acquire_lock`, runs `warn_pending_recovery_sidecars` where `refresh`
runs `sweep_recovery_sidecars`, mutates only its in-memory copy of the
ledger, and returns before `write_state`. `PlanOutput` and `StateSyncOutput`
gain `authority: LedgerAuthority` (`locked` | `unlocked` | `observed`);
`StateSyncOperation` gains `observe`. The graph observation pass already
opens graphs read-only and never runs the recovery sweep, so no engine change
is needed. `refresh` refuses with `state_revision_overflow` instead of
saturating at `u64::MAX`.

**Witness.** `ServingSnapshot` (the read-only loader the server boots from)
gains `config_digest`, `state_revision`, `state_cas`, `applied_graphs` (the
`graph.*` addresses of the applied revision), and `quarantined_graphs`
(sidecar-attributed graphs intersected with `applied_graphs`, so a sidecar
for a graph the revision does not name is never a phantom). `AppState` keeps
them as a `BootWitness` with a `draining` flag. `/readyz` is a new always-flat
route rendering `ReadinessOutput` from `omnigraph-api-types`; its counts are
the registry size and `applied_graphs` minus the registry. `GET /graphs`
renders the same difference as `quarantined`.

**Shutdown.** `ServerConfig` gains `shutdown_grace`, resolved in the binary
as flag, then environment, then default. `serve` spawns the signal listener
first; on the signal it sets `draining`, starts a `std::thread` that sleeps
for the grace and calls `std::process::exit(2)`, and releases the graceful
shutdown. The clean path is unchanged. This is the deadline half of RFC 0035
§8 without its participants: one absolute deadline created at signal receipt,
no participant-local timeouts, a hard non-zero exit that never claims
success and never depends on the runtime. RFC 0035 may later replace the
thread with its coordinator; the flag and the readiness change stay.

## Invariants

- **One source of truth (12).** Observe adds no shadow ledger: its output is
  labeled as an observation of a named `state_cas`.
- **Failures are loud and bounded (8, 11).** An observed read never refuses
  because of the lock and never pretends to hold one; an unlocked write says
  so. Shutdown is bounded by one deadline that the runtime cannot postpone,
  and its cutoff is reported as unfinished work, never as success.
- **Recovery is part of the commit protocol (5).** Observe never sweeps.
- **Trust is established at the boundary (10).** Public readiness discloses
  no graph id; the inventory stays behind the bearer and Cedar gate that
  already protects it.
- **Deny-list.** No process-local lock is presented as fencing: observe holds
  none, and the witness reports what a replica booted, not who may write. No
  cloud-only path.

The one-mutation-process support boundary is unchanged.

## Compatibility and reversibility

On the wire, additive: existing commands and routes keep their output
shapes, `authority` and `quarantined` are new fields, `/readyz` and the flag
are new, and `openapi.json` gains one path and one schema. For Rust
consumers of `omnigraph-cluster` and `omnigraph-api-types` it is not:
`StateSyncOperation` gains a variant, so an exhaustive `match` must add an
arm; `PlanOutput`, `StateSyncOutput`, `ServingSnapshot`, `GraphListResponse`,
and `ServerConfig` gain required fields, so a struct literal or an exhaustive
destructuring must name them. Every in-tree consumer is updated in the same
change; an out-of-tree consumer adds the arm and the fields. No persisted
bytes change. Reverting removes one verb, one flag, one route, and the new
fields.

## Alternatives

- **Use `state.lock: false` for observation.** It is a bundle setting that
  changes every command and warns on each, and it cannot label a result as
  observed. Rejected.
- **Implement RFC 0034's `ReadOnlyProbe` first.** The engine's read-only open
  already skips recovery; what an observer lacks is the cluster crate not
  taking its lock and not writing. RFC 0034 remains the right design for
  recovery authority and is not needed for this.
- **Restore a ledger from a file.** Drafted, then deferred: its real use is a
  coherent restore point where the ledger and the graphs come back together,
  and until those exist, `import` and `observe` rebuild a lost ledger with
  only its history and revision counter lost. Designing it once, against
  restore points, beats designing it twice.
- **List graph ids on `/readyz`.** It would bypass the authentication and
  Cedar `graph_list` gate that `GET /graphs` deliberately carries. Rejected.
- **Report the boot digest on `/healthz`.** Liveness and readiness have
  different consumers and different failure semantics; a draining replica is
  alive and not ready. Rejected.
- **Wait for RFC 0035's shutdown coordinator.** Its deadline half is
  separable and needed now; its admission cells are not.
- **A Tokio task as the watchdog.** A task cannot fire while the executor is
  blocked or the runtime is tearing down, which are exactly the cases a
  deadline exists for. A thread can.

## Evidence and tests

- `omnigraph-cluster` in-source tests (the existing owner of plan, refresh,
  and import): observe takes no lock and labels its authority; observe reports
  drift and leaves the ledger bytes and revision unchanged; a bundle with
  `state.lock: false` is labeled `unlocked`; refresh refuses at `u64::MAX`.
- `crates/omnigraph-server/tests/boot_settings.rs` and `multi_graph.rs`
  (the existing owners of boot and quarantine): `/readyz` reports the boot
  digest, revision, and counts and answers 503 while draining; `GET /graphs`
  reports the quarantined ids; a sidecar for a graph the revision does not
  name is not counted.
- The in-source `shutdown_signal_tests` subprocess owner: the watchdog exits
  2 at the deadline while the runtime thread is blocked; SIGTERM with no work
  exits 0; the flag wins over a malformed environment value.
- `crates/omnigraph-server/tests/openapi.rs`: the committed spec matches.

## Rollout

1. Cluster crate and CLI: observe. Ships alone.
2. Cluster crate and server: the witness and the flag, with the OpenAPI
   regeneration. Ships alone.

`implementation` moves to `partial` after either, `complete` after both.

## Unresolved questions

None that block acceptance. The default grace of 25 seconds matches RFC
0035 §8; it is a default, not a contract.

## Decision log

- 2026-09-03: drafted as 0048 with a fourth seam, ledger restore.
- 2026-09-03: renumbered to 0049 (0047 and 0048 are allocated by PR #606).
  Ledger restore deferred to a restore-point design; readiness reduced to
  counts with the ids on `GET /graphs`; the watchdog became a thread and the
  listener moved before graph open; the compatibility section now states the
  Rust-level breaks; `unlocked` added to the authority enum.
- 2026-09-03: accepted by the maintainer. Implementation follows in two
  PRs, the cluster crate and CLI (observe) and the server (witness and
  bounded shutdown); `implementation` advances with each.
