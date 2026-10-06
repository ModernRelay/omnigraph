---
rfc: "0049"
title: "Control-plane seams: observe, readiness witness, bounded shutdown"
track: maintainer
status: accepted
implementation: partial
authors:
  - OmniGraph maintainers
created: 2026-09-03
updated: 2026-10-04
discussion: null
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC 0049: Control-plane seams: observe, readiness witness, bounded shutdown

> **Server runtime disposition:**
> [Server runtime and online deployment](2026-09-29-server-runtime-and-online-deployment.md)
> removed `cluster refresh`, `cluster import`, and approval execution, refuses
> `state.lock: false`, and added online activation. Historical: observe defined
> as `refresh` without the lock, the `unlocked` label, the approval clause of
> the observe-only reads, "the server never reloads", and the Summary sentence
> that online activation remains unimplemented. Current: observe-only authority,
> the readiness witness and inventory, and bounded shutdown.

## Summary

Three small, independently shippable contracts let an external control plane
drive a cluster without a second implementation of anything the cluster
crate already does, and without bypassing it:

1. **Observe-only reads.** `cluster plan --observe` and a new
   `cluster observe` read the ledger and the live graphs without taking the
   cluster lock and without writing anything, and label their output
   `authority: observed` together with the exact `state_cas` they read.
2. **Readiness witness.** `GET /readyz` reports, without authentication,
   loading, serving, degraded, blocked or draining status, the applied `config_digest`
   it booted from, the ledger revision and CAS it read, and registry, ready,
   loading and blocked graph counts. Graph ids and per-graph availability stay behind the
   authenticated `GET /graphs` in one registry-derived list.
3. **Bounded shutdown.** `--shutdown-grace-seconds` (default 25) puts one
   deadline on graceful shutdown: readiness turns off at the signal, in-flight
   requests drain, and at the deadline an operating-system thread exits the
   process non-zero instead of waiting forever.

Nothing here changes a storage format, the ledger's schema, the lock or the
recovery protocol. The v0.12 availability amendment replaces the readiness and
inventory response shapes with coordinated in-tree consumer changes and no
legacy aliases. The wider
[Server runtime and online deployment](2026-09-29-server-runtime-and-online-deployment.md)
decision owns the initial loading listener and startup ownership. Deploying,
startup retry and online activation remain unimplemented. Observe-only authority and the absolute
shutdown deadline remain unchanged. Restoring a ledger is deliberately not here:
its real use arrives with coherent restore points, where the ledger and the graphs come back
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

Both gaps are small and local. Neither requires the wider server decision's
operation ownership or completion supervision to close; this RFC does not
preempt those contracts.

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
     "served_graph_count": 3, "ready_graph_count": 3, "loading_graph_count": 0,
     "blocked_graph_count": 0,
     "shutdown_grace_seconds": 25}
200 {"ready": true, "status": "degraded", …same fields…}
503 {"ready": false, "status": "loading", …same fields…}
503 {"ready": false, "status": "blocked", …same fields…}
503 {"ready": false, "status": "draining", …same fields…}
```

Unauthenticated, like `/healthz`, and therefore minimal: graph ids are
topology, which the existing `GET /graphs` deliberately puts behind bearer
authentication and the Cedar `graph_list` action, so `/readyz` reports only
counts. `served_graph_count` is the complete registry size; ready, loading and blocked
counts distinguish actual startup outcomes. `GET /graphs` returns one `graphs`
list including those outcomes under the same gate, with `state` (`loading`,
`ready`, `blocked`, `transitioning`, `stopping`), `read_available`, `write_available`, optional sanitized
`failure`, and `action` (`none`, `wait_for_startup`, `wait_for_transition`,
`restart_after_correction`, `wait_for_restart`). Closed transitions count as
blocked; pending initial admission counts as loading.
These booleans describe runtime availability, not permission. Raw failures stay
in server logs. The separate `quarantined` response field is removed.
`booted_serving_digest`
is the `applied_revision.config_digest` of the ledger the process booted
from; it is fixed for the life of the process, because the server never
reloads. The digest and CAS are hashes of configuration bytes: they say
whether two replicas booted the same revision and nothing else. `/healthz` is
unchanged: it answers 200 while the process is alive, draining included.

The accepted empty-cluster amendment in [RFC 0005](0005-server-cluster-boot.md)
permits an actual applied zero-graph revision to report serving with all
counts zero. Its real digest, positive ledger revision and CAS remain required;
canonical-root and configured public-trust validation remain internal boot
checks. Loading keeps readiness at 503 even when a sibling is ready. After all
startup attempts finish, a nonempty inventory is ready while any graph is ready,
with degraded status if some are blocked; no ready graph or draining is unready.
The listener starts after fixed configuration, admission and policy validation,
before engine opening. A nonempty, entirely failed graph set still refuses
startup after its attempts finish. `--require-all-graphs` retains successful
handles privately until all graphs succeed, then admits them atomically; any
blocked graph refuses startup. A listening address is not a readiness witness.

Graph resolution checks credential scope before registry lookup. A known loading, blocked or transitioning
graph returns 503 only to a caller authorized for graph `read` on `main` or
management `graph_list`; otherwise the graph remains undisclosed as 404. An
invalid graph policy or configuration cannot authorize the read fallback. Unknown
graphs return 404. This applies to HTTP and MCP; identity-only discovery retains its smaller
IDs/names response. No status response grants automatic write retry authority.

### Bounded shutdown

```bash
omnigraph-server --cluster … --shutdown-grace-seconds 25
OMNIGRAPH_SHUTDOWN_GRACE_SECONDS=25
```

The signal listener is installed when `serve` starts, before any graph opens,
so the bound covers startup. At SIGTERM or Ctrl-C the server marks itself
draining (`/readyz` answers 503), starts an operating-system thread that
sleeps for the grace, stops accepting connections, and lets in-flight requests
finish, including already entered graph opens. Their late completion cannot
install a ready graph after process admission closes. If they have finished before the deadline, the process exits 0 as it
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

**Witness.** `ServingSnapshot` supplies applied revision/digest/CAS boot facts
and graph startup inputs, including graph-specific admission refusals. The
server registry retains initial loading entries and actual outcomes as ready
handles or blocked entries. Loading and blocked entries retain captured
authorization context when available. Startup
classifies invalid configuration, invalid policy, invalid external-Blob policy,
open failure and invalid stored queries without exposing raw errors. `BootWitness`
records revision facts, not a second availability inventory. `/readyz`, `GET /graphs` and minimal graph
discovery derive their counts or entries from the registry; they do not infer
missing entries by subtracting handles from the boot witness. Status requires
no graph/storage I/O. Shutdown projects every entry as stopping and closes
its availability, retaining any startup failure. One process-owned startup
batch opens at most four graphs concurrently. Captured loading-entry identity
and the process admission boundary fence each completion. Policies are not
re-read after listening starts. There is no retry, attempt timeout, reopening
or mutable runtime graph set.

**Shutdown.** `ServerConfig` gains `shutdown_grace`, resolved in the binary
as flag, then environment, then default. `serve` spawns the signal listener
first; on the signal it sets `draining`, starts a `std::thread` that sleeps
for the grace and calls `std::process::exit(2)`, and releases the graceful
shutdown. The clean path is unchanged. This supplies the accepted server
[operation-ownership contract's](2026-09-29-server-runtime-and-online-deployment.md#operation-ownership)
deadline boundary without its participants: one absolute deadline created at
signal receipt, no participant-local timeouts, a hard non-zero exit that never
claims success and never depends on the runtime. The server runtime decision may
replace the thread with its coordinator; the flag and the readiness change stay.

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

The original observe/readiness additions were additive on the wire. The v0.12
availability amendment intentionally breaks that earlier inventory shape:
`GraphListResponse` has only one `graphs` list, each entry carries availability,
and readiness replaces `quarantined_graph_count` with ready/loading/blocked counts while
`served_graph_count` counts the whole registry. HTTP consumers must update
together with the server; no deprecated aliases or dual-response mode remain.
Known unavailable graphs use authorized 503, which is not automatic
retry permission. OpenAPI, CLI and tests change with these fields.

Rust consumers must update exhaustive matches and struct literals for changed
API types. Observe retains `StateSyncOperation::Observe`, ledger authority and
revision/CAS fields, and shutdown retains its configured absolute deadline.
No persisted bytes change. Reverting availability changes requires reverting its
wire consumers together; it does not require a storage migration.

## Alternatives

- **Use `state.lock: false` for observation.** It is a bundle setting that
  changes every command and warns on each, and it cannot label a result as
  observed. Rejected.
- **Implement RFC 0034's `ReadOnlyProbe` first.** This was an alternative when
  this RFC was drafted; RFC 0034 is now superseded. The engine's read-only open
  already skipped recovery; what observation needed was the cluster crate not
  taking its lock and not writing. The wider completion design was not required.
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
- **Wait for RFC 0035's shutdown coordinator.** This was an alternative when
  this RFC was drafted; RFC 0035 is now superseded. The deadline boundary was
  independently shippable without the full admission design.
- **A Tokio task as the watchdog.** A task cannot fire while the executor is
  blocked or the runtime is tearing down, which are exactly the cases a
  deadline exists for. A thread can.

## Evidence and tests

- `omnigraph-cluster` in-source tests (the existing owner of plan, refresh,
  and import): observe takes no lock and labels its authority; observe reports
  drift and leaves the ledger bytes and revision unchanged; a bundle with
  `state.lock: false` is labeled `unlocked`; refresh refuses at `u64::MAX`.
- `crates/omnigraph-server/tests/boot_settings.rs` and `multi_graph.rs`
  (the existing owners of boot and blocked graphs): `/readyz` reports boot
  facts, registry/ready/loading/blocked counts and loading/degraded/blocked/draining status;
  `GET /graphs` reports one authorized availability list. Real startup failures
  retain entries, while witness-only names cannot create phantom entries.
  Authorization owners prove scope-before-lookup, authorized 503, undisclosed
  unavailable graphs and invalid-policy refusal; MCP shares the resolution boundary.
  The existing production subprocess owner parks a real startup open and checks
  early liveness, loading disclosure, ready sibling progress, strict atomic
  admission, all-failed refusal, shutdown ownership and the original cutoff.
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

None that block acceptance. The default grace of 25 seconds matched the earlier
RFC 0035 proposal; it is a default, not a contract.

## Decision log

- 2026-10-03: Aggregate readiness remains unready throughout startup loading,
  while completed healthy graphs may serve direct requests. Degraded readiness
  applies after startup attempts finish; strict startup remains all-or-nothing.

- 2026-10-03: Added initial loading visibility under the accepted server runtime
  decision. Configuration and policies precede listening; one bounded startup
  batch precedes graph admission. Strict startup admits all graphs together.
  Updated Summary, Readiness, Shutdown, Witness, Compatibility and Evidence;
  automatic retries and native settlement remain unqualified.

- 2026-10-03: Current server-runtime cross-references now name the accepted
  decision; implementation and qualification gates remain with that owner.


- 2026-10-02: Accepted the v0.12 availability amendment in Summary, Readiness,
  Witness, Compatibility and Evidence. Actual startup outcomes own one
  ready/blocked inventory, authorized 503 disclosure and aggregate readiness;
  removed legacy quarantined fields and boot-witness-derived phantom entries.
  Loading/deploying states, bounded startup retry and online activation remain
  outside this implemented foundation. Observe and shutdown authority are unchanged.

- 2026-09-03: drafted as 0048 with a fourth seam, ledger restore.
- 2026-09-03: renumbered to 0049 (0047 and 0048 are allocated by PR #606).
  Ledger restore deferred to a restore-point design; readiness reduced to
  counts with the ids on `GET /graphs`; the watchdog became a thread and the
  listener moved before graph open; the compatibility section now states the
  Rust-level breaks; `unlocked` added to the authority enum.
- 2026-09-03: accepted by the maintainer. Implementation follows in two
  PRs, the cluster crate and CLI (observe) and the server (witness and
  bounded shutdown); `implementation` advances with each.
- 2026-09-06: accepted the zero-graph readiness amendment from RFC 0005;
  implementation and process HTTP evidence follow without a wire-format change.
- 2026-09-29: The server runtime proposal replaces the active references to
  RFCs 0034 and 0035 in Summary, Motivation, and Shutdown. Alternatives retain
  those identifiers as historical context. Observe, readiness, and bounded
  shutdown remain this RFC's accepted decisions.
- 2026-09-30: Clarified that the proposed v0.12 server contract may replace
  readiness and inventory wire shapes; observe-only authority and the absolute
  shutdown deadline remain its foundations. This clarification changes none of
  this RFC's accepted behavior or current wire shapes.
- 2026-10-02: Witness replaces the sidecar-only definition of
  `quarantined_graphs` with applied graphs refused for legacy sidecars or unsafe
  server-safe external Blob bases. Counts, authority and wire shapes are unchanged.
