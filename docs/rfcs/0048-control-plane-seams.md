---
rfc: "0048"
title: "Control-plane seams: observe, ledger restore, readiness witness, bounded shutdown"
track: maintainer
status: draft
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

# RFC 0048: Control-plane seams: observe, ledger restore, readiness witness, bounded shutdown

## Summary

Four small, independently shippable contracts let an external control plane
drive a cluster without a second implementation of anything the cluster
crate already does, and without bypassing it:

1. **Observe-only reads.** `cluster plan --observe` and a new
   `cluster observe` read the ledger and the live graphs without taking the
   cluster lock and without writing anything, and label their output
   `authority: observed` together with the exact `state_cas` they read.
2. **Ledger restore.** `cluster state restore --from <file>` publishes a
   previously read `state.json` as a new, higher revision under the cluster
   lock, keeping the current approval and recovery records and never
   resurrecting the snapshot's.
3. **Readiness witness.** `GET /readyz` reports whether the server is
   serving or draining, the applied `config_digest` it booted from, the
   ledger revision and CAS it read, and its served and quarantined graphs.
4. **Bounded shutdown.** `--shutdown-grace-seconds` (default 25) puts one
   deadline on graceful shutdown: readiness turns off at the signal, in-flight
   requests drain, and at the deadline the process exits non-zero instead of
   waiting forever.

Nothing here changes a storage format, the ledger's schema, the lock, the
recovery protocol, or any existing route's success shape. The wider designs of
RFC 0034 (recovery authority) and RFC 0035 (served-operation ownership) stay
independent; this RFC takes none of their decisions and its four seams remain
valid under them.

## Motivation

An operator that manages many clusters needs three things from the engine it
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

**A way to put a ledger back.** The ledger is replaced by conditional put and
every mutating command bumps `state_revision`. There is no supported way to
publish a previously read ledger again: an operator with an exact copy of a
known-good `state.json` can only edit the file by hand, which
[`docs/dev/control-plane.md`](../dev/control-plane.md) forbids, and which
would carry that copy's approval records with it.

**An honest replica.** `/healthz` reports the process is alive. Nothing
reports which applied revision a replica actually booted from, which graphs it
quarantined, or that it has started draining; an orchestrator replacing a
cohort cannot tell a replica serving the old revision from one serving the new
one. On the way down, axum's graceful shutdown has no bound, so a stalled
connection keeps a replica alive past any orchestration grace period, and the
orchestrator's kill is indistinguishable from a crash.

Each gap is small and local. None needs RFC 0034's recovery modes or RFC
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

Both outputs carry `authority: "observed"` (the existing paths carry
`"locked"`) and the `state_cas` and `state_revision` of the ledger they read.
An existing lock is reported in `state_observations` (`locked`, `lock_id`,
`lock_operation`, `lock_age_seconds`) and does not refuse the command. Pending
recovery sidecars are reported as the `cluster_recovery_pending` warning that
read-only commands already emit; nothing is swept.

An observed result is never authority for an effect: `apply` still re-plans
under the lock, and an approval still binds to the digests `apply` sees.

### Ledger restore

```bash
omnigraph cluster state restore --config ./company-brain --from ./state-2026-09-01.json
```

Under the cluster lock, `state restore` parses the file as a ledger of the
current `version`, refuses it otherwise (`restore_ledger_invalid`,
`restore_ledger_version`), and refuses when any recovery sidecar is pending
(`state_restore_blocked_by_recovery`), because a sidecar classifies against
the ledger it will find. It then publishes a new ledger whose
`applied_revision`, `resource_statuses`, and `observations` are the file's,
whose `approval_records` and `recovery_records` are the current ledger's (or
empty when there is no current ledger), and whose `state_revision` is one
more than the higher of the current and the file's revision. The write is the
same conditional put every other command uses, so a concurrent writer loses
loudly (`state_cas_mismatch`).

Restore moves the ledger, not the graphs. The next `refresh` or `apply`
reconciles them exactly as after any other ledger change.

### Readiness witness

```text
GET /readyz
200 {"ready": true, "status": "serving", "booted_serving_digest": "<sha256>",
     "state_revision": 42, "state_cas": "sha256:…",
     "served_graphs": ["knowledge"], "quarantined_graphs": [],
     "shutdown_grace_seconds": 25}
503 {"ready": false, "status": "draining", …same fields…}
```

Unauthenticated, like `/healthz`. `booted_serving_digest` is the
`applied_revision.config_digest` of the ledger the process booted from; it
is fixed for the life of the process, because the server never reloads.
`quarantined_graphs` lists graphs the applied revision names that this
process does not serve, for any reason. `/healthz` is unchanged: it answers
200 while the process is alive, draining included.

### Bounded shutdown

```bash
omnigraph-server --cluster … --shutdown-grace-seconds 25
OMNIGRAPH_SHUTDOWN_GRACE_SECONDS=25
```

At SIGTERM or Ctrl-C the server marks itself draining (`/readyz` answers 503),
stops accepting connections, and lets in-flight requests finish. If they have
finished before the deadline, the process exits 0 as it does today. At the
deadline the process logs the unfinished work and exits with status 2. Zero
means immediate cutoff. The orchestrator's own termination grace must be
longer than this value; the deployment guide says so.

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
gain `authority: LedgerAuthority` (`locked` | `observed`);
`StateSyncOperation` gains `observe`. The graph observation pass already
opens graphs read-only and never runs the recovery sweep, so no engine change
is needed.

**Restore.** `restore_state_config_dir(dir, ledger_json)` in
`omnigraph-cluster`; `StateSyncOperation` gains `restore`. It shares the
`refresh`/`import` pipeline up to the lock, then composes the new ledger as
described above and calls the existing `write_state` with the CAS of the
ledger it read.

**Witness.** `ServingSnapshot` (the read-only loader the server boots from)
gains `config_digest`, `state_revision`, `state_cas`, and
`quarantined_graphs`. `AppState` keeps them, plus the set of graphs whose
open failed, as a `ServingWitness`, and a `draining` flag. `/readyz` is a
new always-flat route rendering `ReadinessOutput` from `omnigraph-api-types`.

**Shutdown.** `ServerConfig` gains `shutdown_grace`. The graceful-shutdown
future sets `draining`, then a watchdog task sleeps for the grace and calls
`std::process::exit(2)`. The clean path is unchanged. This is the deadline
half of RFC 0035 §8 without its participants: one absolute deadline created
at signal receipt, no participant-local timeouts, a hard non-zero exit that
never claims success. RFC 0035 may later replace the watchdog with its
coordinator; the flag and the readiness change stay.

## Invariants

- **One source of truth (12).** Observe adds no shadow ledger: its output is
  labeled as an observation of a named `state_cas`. Restore publishes through
  the one conditional put.
- **Failures are loud and bounded (8, 11).** An observed read never refuses
  because of the lock and never pretends to hold it. Restore refuses an
  invalid ledger, a wrong version, a pending sidecar, and a lost CAS. Shutdown
  is bounded by one deadline and its cutoff is reported as unfinished work,
  never as success.
- **Recovery is part of the commit protocol (5).** Observe never sweeps.
  Restore refuses while a sidecar is pending rather than letting the sweep
  classify against a ledger it did not see written.
- **Deny-list.** No process-local lock is presented as fencing: observe holds
  none, restore holds the same lock every writer holds, and the witness
  reports what a replica booted, not who may write. No cloud-only path.

The one-mutation-process support boundary is unchanged.

## Compatibility and reversibility

Additive everywhere. Existing commands and routes keep their output shapes;
`authority` is a new field with the old behavior's value. `/readyz` and the
flag are new. `openapi.json` gains one path and one schema. A client that
never calls `/readyz` sees no change. Reverting removes two verbs, one flag,
one route, and four fields; no persisted bytes change.

## Alternatives

- **Use `state.lock: false` for observation.** It is a bundle setting that
  changes every command and warns on each, and it cannot label a result as
  observed. Rejected.
- **Implement RFC 0034's `ReadOnlyProbe` first.** The engine's read-only open
  already skips recovery; what an observer lacks is the cluster crate not
  taking its lock and not writing. RFC 0034 remains the right design for
  recovery authority and is not needed for this.
- **Hand-edit `state.json` to restore.** Forbidden by the control-plane guide
  and it would resurrect the copy's approvals. Rejected.
- **Report the boot digest on `/healthz`.** Liveness and readiness have
  different consumers and different failure semantics; a draining replica is
  alive and not ready. Rejected.
- **Wait for RFC 0035's shutdown coordinator.** Its deadline half is
  separable and needed now; its admission cells are not.

## Evidence and tests

- `omnigraph-cluster` in-source tests (the existing owner of plan, refresh,
  and import): observe takes no lock and labels its authority; observe reports
  drift and leaves the ledger bytes and revision unchanged; restore publishes
  at a higher revision and keeps current approvals; restore creates a missing
  ledger without the file's approvals; restore refuses under a pending
  sidecar.
- `crates/omnigraph-server/tests/boot_settings.rs` and `multi_graph.rs`
  (the existing owners of boot and quarantine): `/readyz` reports the boot
  digest, revision, served and quarantined graphs; it answers 503 while
  draining.
- The in-source `shutdown_signal_tests` subprocess owner: SIGTERM with a
  stalled connection exits 2 within the grace; SIGTERM with no work exits 0.
- `crates/omnigraph-server/tests/openapi.rs`: the committed spec matches.

## Rollout

1. Cluster crate and CLI: observe and restore. Ships alone.
2. Cluster crate and server: the witness and the flag, with the OpenAPI
   regeneration. Ships alone.

`implementation` moves to `partial` after either, `complete` after both.

## Unresolved questions

None that block acceptance. The default grace of 25 seconds matches RFC
0035 §8; it is a default, not a contract.

## Decision log

- 2026-09-03: drafted.
