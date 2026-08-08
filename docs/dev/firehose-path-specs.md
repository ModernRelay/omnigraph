# Firehose Path — Implementation Specs

> **Historical record — rejected architecture.** RFC-026 and this implementation
> plan are no longer active. The MemWAL path and its v7-v19 formats were removed
> after benchmarking showed that its per-dataset durability layer did not
> improve graph-level ingestion. See [the current decision](wal-removal.md).

**Type:** archived implementation plan
**Historical status at archival time:** slices F0–F1, F2 profile authority, the hidden F2 lifecycle tranche,
private F3a resume/abort-drain, F3b EnsureIndices, F3c Optimize, F3d physical
rebind, F3e authority retirement/export, F3f exact DataBlock correction, and
F5b terminal dead-letter handling are implemented. The current development
strand selects internal schema v19/token schema v3,
lifecycle protocol v3, recovery-v14 exact
enrollment/claim/fold/drain/terminal-receipt owners, recovery-v15 exact
`SEALED → OPEN` resume plus guarded `DRAINING → OPEN` abort, and recovery-v16
same-binding `SEALED` EnsureIndices plus recovery-v17 same-binding `SEALED`
Optimize, recovery-v18 private exact-`SEALED` physical rebind, recovery-v19
lineage-neutral root-wide authority retirement, recovery-v20 exact DataBlock
correction, and recovery-v21 mixed/all-diverted dead-letter folds plus
three-disposition retirement. F7a activates one graph-native served ingress
surface (`POST /graphs/{graph_id}/stream/ingest`) plus its remote client/CLI
path. It exposes logical node/edge rows only: declaration routing, lazy private
lane preparation, MemWAL ownership, and folding remain behind the graph
boundary. Operator lifecycle/rebind verbs, checked operational status, and
every maintenance transport surface remain inactive; retirement and DataBlock
inspection/correction are exposed only by narrow offline cluster controls.
The format-neutral F4, F5a, and F5b0 hidden-path slices are also implemented:
caller-shaped ingest/prepare, automatic `OPEN` folding, exact-`ENABLED`
goal-`SEALED` drain continuation, and the checked offline `DISABLING` loop.
F5b adds stopped/offline selected-token dead-letter list/export but no public
HTTP, SDK, remote CLI, or OpenAPI row surface. The first F6 acceptance slice,
F6a, adds only a failpoints-only process-local advisory driver snapshot and one
hidden in-process candidate-runtime composition. F6b1 adds the checked terminal
served-export authority and hidden immutable export-cut substrate without a
format/recovery change or public route. F6b2 is the implemented no-format
acceptance slice for SIGTERM/shared shutdown, sequential OS-process recovery,
frozen-round node/edge fairness, physical rebind → re-enable → reopen → resume,
combined maintenance, fresh-target import, and legacy writer refusal.
F6b3 implements the exact-selected uncovered-token-tail cost harness locally
and as ignored local/configured-RustFS sweeps. F6b7 preserves that historical
baseline and adds the paired failpoints-only selected-index decision instrument
for current-token and profile-receipt lookup work. F6b4 closes the
isolated production-size dead-letter encoding/materialization and peak-RSS
evidence without changing format, recovery, or a production route. Its narrow
stopped/offline Rust payload DTO source shape changes as recorded in §7. F6b5
implements the bounded stream-aware served-export transport on the existing
HTTP/remote-CLI/OpenAPI export surface: preflight precedes `200`, exact Lance
versions scan without whole-table collection, queue bytes are reserved under a
deadline, and stall/disconnect/error release ownership. F6b6 implements the
checked read-only operational-status core behind an engine-internal seam. It
uses the checked runtime for `ENABLED`, checked served-export authority for
terminal `DISABLED | RETIRED`, and explicit checked cluster-apply status
authority for `DISABLING`. Within the hard status envelope every sidecar is
reported and rebuild-blocking; exceeding any discovery bound refuses the whole
status rather than returning a partial inventory. Expensive immutable parity/
receipt work runs once without writer gates and the
short cut repeats only mutable witnesses. Only an exact canonical-main
recovery participant outcome can explain physical movement as an unavailable
projection rather than a movement error. Cold-replay and flushed-LWW pending accounting plus
exact oldest-uncovered-token age remain explicit unavailable values. The public
embedded `stream_status` stays nonblocking and manifest-only; F7b now exposes a
separate graph-redacted checked projection through served HTTP/OpenAPI and the
remote CLI, while direct-SDK parity remains later work. F6b7 supplies
covered/reconciled decision evidence; its
configured-RustFS result is a bounded NO-GO only for the uncompacted profile-
cycle fixture, so no standalone production reconciler is scheduled. F6b8 closes
the resume-to-driver handoff without changing format or recovery: resume
transfers its root producer permit into detached writer installation, arms an
urgent driver turn before that transfer can release, and performs an exact
empty-owner housekeeping prepass before the unchanged node-before-edge round
so the sole root slot is released promptly. Driver-first, resume-first/caller-
cancelled, cross-lane reuse, and clean-shutdown cells are green. The remaining
guardrail matrix and the broader post-claim install/retirement-failure matrix
remain in F6 before the remaining management surfaces activate. F7a does not
weaken those gates because it exposes only the already-proved graph-ingest and
resident-driver composition.
**Design authority:** [RFC-026](../rfcs/0026-memwal-streaming-ingest.md) — this
file never overrides it. Where they disagree, the RFC wins and this file is
wrong. §4.7 records the selected experimental profile; §4.3/§4.6 record the
contracts every slice below implements.
**Audience:** whoever picks up the next slice, human or agent.

This is the execution plan for making the firehose lane — RFC-026's streaming
write path — actually usable. The private compare-and-chain put/fold core is
correct within its current hidden `OPEN`, non-empty-generation seam, while
*nothing public can reach it*: no caller can put a row, and nothing schedules a
fold. Public correctness still depends on lifecycle, claim-receipt,
dead-letter, driver, shutdown, and maintenance integration. That work is large
enough that "just finish RFC-026" is not an actionable instruction.

Each slice below states what already exists to reuse, what must be built, the
exact seams, the contract it implements, and the evidence that closes it.

---

## 0. Orientation: the two lanes

The firehose is one of two lanes over **one correctness protocol**. Both lanes
publish through the same `__manifest` door with the same recovery discipline;
they differ only in *where the acknowledgement sits relative to the ceremony*.

```
   direct lane                          firehose lane
        │                                    │
  shape checks                         shape checks
  graph checks                              │
        │                             [ WAL PUT ] ──► ACK   (durability only)
        │                                    │
        │                        ...thousands accumulate...
        │                                    │
        │                        graph checks at fold
        │                        data conflict ──► dead letter
        │                        structural fault ──► loud fail/retry
        │                        object over bound ─► DataBlock
        ▼                                    ▼
 ╔══════════════════ THE ONE PROTOCOL ══════════════════╗
 ╚══════════════════════════════════════════════════════╝
        │                                    │
   ACK (current path)                  rows become VISIBLE
 (durable+visible+checked)            (acked long before)
```

The consequence that shapes every slice: **an acknowledgement cannot be
revoked.** Graph-state validation moves to fold time. A data conflict diverts
one terminal LWW candidate for each failing key durably; a structural schema,
witness, or token violation blocks the whole fold. Neither case silently drops
an acknowledged terminal value or retroactively un-acks it. Superseded
same-key occurrences remain authenticated by the incremental WAL/token chain
but are not promised as replayable payloads. Any latency or object-store-trip
comparison is a target until the F6 instrument proves it.

The format corollary is equally strict: **a format that can create terminal
authority ordinary export cannot represent must ship its own safe exit before
that authority becomes reachable.** A later strict-strand binary cannot rescue
an older root it refuses to open. V11 reserves a fail-closed `RETIRED` profile
shape, while recovery-v14 contains one frozen fail-closed retirement scaffold.
F3 may activate that scaffold only if its exact payload is sufficient;
otherwise it takes a new strand. F3e satisfied the requirement that an
irreversible retirement/export path exist before F3f made `WITHDRAWN`
reachable; v19/recovery-v21 extends that exit before making `DEAD_LETTERED`
reachable.

---

## 1. Current state

### 1.1 Shipped and reachable

| Capability | Where | Slice |
|---|---|---|
| Graph-global enablement flag (`stream_profile` manifest singleton) | `db/manifest/stream_profile.rs` | F0 (#389) |
| `cluster.yaml` → `cluster apply` → manifest propagation, refresh convergence | `omnigraph-cluster` | F0 |
| Typed `StreamingDisablePending` (disable is pending-until-drained) | `error.rs` | F0 |
| `stream_ingest` / `stream_manage` Cedar actions | `omnigraph-policy` | F1 (#392) |
| Read-only `Omnigraph::stream_status` — the compare-token source | `db/omnigraph/stream_status.rs` | F1 |
| Capability-bound stopped/offline apply and served-runtime ownership | `omnigraph-control-authority`, cluster/server adapters | F2 profile authority |
| Protocol-v2 profile state (`DISABLED`, `ENABLED`, resumable `DISABLING`, fail-closed `RETIRED`) | `db/manifest/stream_profile.rs` | F2 profile authority |
| Exact token-ledger `ProfileManagementReceipt` + recovery-v13 `StreamProfileChange` | manifest recovery/token store | F2 profile authority |
| Lifecycle-v3 fixed-size binding/management/claim chains + authenticated WAL-tail authority | manifest stream/token store | F2 lifecycle |
| Recovery-v14 exact enrollment, claim, ordinary/drain fold, and lifecycle receipt | manifest recovery | F2 lifecycle |
| Hidden empty/non-empty quiesce, including claim-before-seal and seal-before-fold restart | engine/worker private seams | F2 lifecycle |
| Recovery-v15 private `SEALED → OPEN` resume and guarded `DRAINING → OPEN` abort | manifest recovery/engine private seams | F3a |
| Checked-runtime, main-only `SEALED` EnsureIndices with recovery-v16 proof refresh | table maintenance/manifest recovery private seams | F3b EnsureIndices |
| Checked-runtime, main-only `SEALED` Optimize with recovery-v17 achieved-HEAD proof refresh | table maintenance/manifest recovery private seams | F3c Optimize |
| Stopped/offline exact-`SEALED` physical rebind with recovery-v18 | cluster maintenance/manifest recovery private seams | F3d physical rebind |
| Stopped/offline `WITHDRAWN` authority retirement and receipt-bearing export with recovery-v19 | cluster control/manifest recovery/export | F3e authority retirement |
| Stopped/offline exact `DataBlock` show/correct with recovery-v20 | cluster control/manifest recovery | F3f DataBlock correction |
| Caller-shaped authorized JSON/NDJSON ingest plus bodyless lazy-enrollment prepare, behind doc-hidden test seams | engine private seams | F4 |
| Format-neutral automatic `OPEN`-lane fold supervisor over the existing recovery-v14 fold adapter | engine private server bridge | F5a |
| Format-neutral resident goal-`SEALED` continuation plus checked offline `DISABLING` drain loop | engine private server bridge + existing cluster apply | F5b0 |
| Deterministic mixed/all-diverted terminal fold, one bounded object, token-schema-v3 `DEAD_LETTERED`, exact retry/ordinary successor, selected-token list/export, and extended retirement | engine/manifest recovery + stopped/offline cluster control | F5b |
| Typed failpoints-only process-local advisory driver snapshot plus one hidden in-process composed candidate-runtime acceptance | engine private test seams | F6a |
| Checked exact-terminal served-export authority plus one hidden immutable exact-version export cut | control authority, cluster/server boot, engine private seam | F6b1 |
| Bounded stream-aware served export with pre-header cut validation and disconnect-safe ownership | engine export, server HTTP body, remote CLI/OpenAPI | F6b5 |
| Checked read-only operational-status cut with typed movement/deadline refusal | engine stream status, worker/driver/token/recovery projections | F6b6 |
| Paired exact-selected token-index decision evidence with no production maintenance owner | engine failpoints-only token/status seams and cost harness | F6b7 |

Internal schema is **v19**, token schema is **v3**, profile protocol is **v2**, and lifecycle protocol
is **v3**. Recovery-v13 remains exactly `StreamProfileChange`: it owns the exact
`ProfileManagementReceipt` token-ledger transaction, and only its achieved token
witness plus fixed next profile may reach the terminal manifest CAS.
Recovery-v14 activates `StreamEnrollmentV2`, `StreamClaim`, `StreamFoldV2`,
`StreamDrainFold`, and `StreamLifecycleReceipt`; recovery-v15 owns private
resume/abort-drain; recovery-v16 layers exact `SEALED` lifecycle authority over
the existing EnsureIndices effect grammar; and recovery-v17 binds Optimize's
confirmed outputs and exact achieved HEADs to its `SEALED` proof refresh.
Recovery-v18 owns physical rebind, recovery-v19 retains its historical
two-disposition retirement meaning, recovery-v20 owns only exact DataBlock
correction, and recovery-v21 owns terminal fold plus three-disposition
retirement. The frozen
recovery-v14 correction/rebind/retirement/maintenance scaffolds and reserved
`AuthorityBlock` family still fail closed. Historical recovery-v10 enrollment
and recovery-v12 lifecycle-v2 folds are not reinterpreted.

The historical v10 bump also added an explicit-null fold-attribution
dead-letter compatibility placeholder
(`StreamFoldAttributionSummary::dead_letter_object`). That incomplete shape is
now frozen null and is not reinterpreted: the validator rejects a populated
reference. V19 instead uses versioned attribution and token-schema-v3 terminal
evidence for one recovery-v21-owned object, including a marker-only base
transition for an all-diverted cut.

### 1.2 Built but unreachable (the private core)

Behind `#[cfg(feature = "failpoints")] #[doc(hidden)]` seams only:

- **Enrollment** — `db/omnigraph/stream_enrollment.rs::enroll_stream_table_b1`,
  recovery-v14 `StreamEnrollmentV2`, one empty unsharded shard on main plus
  immutable enrollment and binding ledger receipts.
- **Put / acknowledge** — charge → shared admission → same-key queue → worker;
  ack requires watcher success *and* the same writer's post-durability
  `check_fenced()`; any post-invocation ambiguity is `AckUnknown` + retirement.
- **Compare-and-chain tokens** — canonical payload/token digests, trusted
  hidden row metadata, same-generation overlays, graph-global
  `_stream_tokens.lance` selected by `__manifest`.
- **Claim** — the sole cold opener arms recovery-v14 before invoking Lance,
  checkpoints manifest-only attempts in the ledger, and selects one terminal
  claim receipt with the lifecycle epoch/tail successor.
- **Fold** — `db/omnigraph/stream_ingest.rs::stream_fold_phase_b1`,
  recovery-v14 `StreamFoldV2`, exact base + token participants, selected current
  claim/tail authority, and one lineage-bearing manifest CAS.
- **Physical drain** — `table_store/mem_wal/worker.rs::seal_and_drain` →
  `force_seal_active` → `wait_for_flush_drain` → `prove_post_drain_cut`, with
  detached ownership and exclusive-admission carry-through. `quiesce_cut`
  additionally handles a fresh empty claimed generation, while
  `passive_quiesce_cut` reuses an exact already-flushed receipt-bound cut after
  restart.
- **Retain-all** — no GC, no canonical `_mem_wal` deletion, loud exhaustion.

Topology is fixed for the whole plan: **main-only, unsharded, one resident
writer, one externally enforced live writer process, upsert-only.**

### 1.3 The honest summary

The private put/fold/claim/quiesce/resume seam is closed and tested; the public
protocol is not. Profile mutation requires an opaque checked stopped/offline or
served-runtime owner, and `DISABLING` persists an exact continuation plan. F4's
caller-shaped ingest and lazy-enrollment prepare path is complete behind
feature-gated, doc-hidden seams. F5a adds a format-neutral, graph-root-scoped
automatic supervisor for `OPEN` lanes: detached ownership schedules one
coalesced timer entry immediately after physical put invocation, including
caller cancellation or an eventual `AckUnknown`; the passive readiness probe
filters no-effect wakes. Capacity pressure makes that entry immediately ready,
cold start rediscovers manifest-authoritative `OPEN` backlog, and each finite
round visits ready nodes before ready edges with a carried cursor inside both
cohorts. Every effect still enters the
existing recovery-v14 fold adapter under the live profile `FoldDelegation` and
checked served-runtime authority. Cluster server boot starts every selected
graph supervisor after binding the listener. Graceful shutdown first fences
and joins the driver, then reacquires the profile fence plus every resident
lane's exclusive admission, aborts the process-local writers, and joins their
idle authority owners. One detached owner keeps those gates across caller
cancellation or the shared deadline; failure stays fail-closed and loud. The hidden
lifecycle core can drain a non-`SEALED` lane to `SEALED`. F5b0 extends the same
exact-`ENABLED`, checked-runtime resident owner to restart and continue an
unblocked `DRAINING(goal = SEALED)` lane through recovery-v14. It also makes
the existing checked offline `cluster apply` disable path operational:
`DISABLING` is published before work; one finite manifest-derived lane cut is
visited deterministically, nodes before edges and one identity at a time;
`OPEN`, goal-`SEALED`, and existing `OPEN_AFTER_FOLD` lanes converge through
the existing quiesce and recovery-v14 lifecycle-receipt owners. A selected
`DataBlock` parks the exact durable disable plan loudly until stopped/offline
correction and an apply retry; apply resumes that continuation before schema
work, so schema and dependent queries cannot move ahead of the parked drain.
The resident and offline owners cannot overlap. F5b now lets the hidden fold
partition data conflicts deterministically: valid winners publish, losing
terminal candidates enter one bounded canonical object, and each losing key
becomes current `DEAD_LETTERED`. All-diverted folds advance Lance with a
marker-only base transaction. Exact retry returns the same terminal result
while current; a fresh ordinary successor can restore `PRESENT`.
There is still no public driver health/backlog projection, public lifecycle control, or public
maintenance integration. The doc-hidden offline seam can perform an exact physical rebind
while the profile is terminal `DISABLED`; doc-hidden checked-runtime seams can
run EnsureIndices and Optimize on productive enrolled tables only while they
are exactly `SEALED`. Their ambient forms remain fenced. The one active operator
repair is cluster-only, stopped/offline exact `DataBlock` show/correct. It
reconstructs one receipt-bound immutable cut and publishes bounded
`REPLACE`/`WITHDRAW` outcomes through recovery-v20 while leaving the lane
`DRAINING`; this is now a production path to `WITHDRAWN`. The other active
operator exit is stopped/offline authority retirement for a verified
current-`WITHDRAWN | DEAD_LETTERED` cut; it makes the source permanently query/status/export-only
and carries its receipt into the rebuild artifact. No active writer can produce
the reserved `AuthorityBlock` evidence shape, so its repair stays fail-closed
until a reachable producer and finalized evidence grammar exist. Stopped/offline
`cluster stream dead-letter list|export` inspects only selected current-token
authority; it is not an import or replay surface. A typed failpoints-only
snapshot now exposes process-local driver scheduling evidence to tests; it is
explicitly advisory, and its pending triggers are not a durable backlog.
Public durable `StreamStatus` remains manifest-only. F7a activates one
graph-native served row surface; F7b activates a graph-redacted read-only
projection of driver/recovery/rebuild health; lifecycle/maintenance surfaces
remain inactive. F6b6 adds a separate engine-internal checked operational cut
over physical, token, recovery, advisory-driver, and rebuild evidence. Its raw
physical form has no public transport;
`DISABLING` uses explicit checked cluster-apply status authority. Every sidecar
within the hard status envelope is visible and rebuild-blocking; exceeding any
discovery bound refuses the whole status. Only exact canonical-main recovery
ownership makes physical movement unavailable. Cold replay, flushed LWW projection accounting, and exact oldest-
uncovered age are unavailable. F6b1 provides
the checked terminal served-export capability and hidden immutable cut
described in §7, and F6b5 now routes that cut through the existing
HTTP/remote-client/CLI/OpenAPI export surface with bounded transport ownership.
F6b3 owns historical exact-selected uncovered-tail current-token hit/miss and
terminal-page measurement. F6b7 owns the paired failpoints-only selected-index
refresh, receipt-key/current-token comparison, and maintenance-cost evidence;
its uncompacted-profile-cycle bounded NO-GO schedules no standalone production
reconciler. Public status transport and remaining guardrail
acceptance stay in F6b/F7. F6b4 owns the production-size dead-letter
byte/capacity/timing and isolated peak-RSS acceptance described below.

---

## 2. Slice map

| Slice | Delivers | Format | Gate |
|---|---|---|---|
| ~~F0~~ | Enablement authority | v10 | shipped |
| ~~F1~~ | Cedar split + read-only status | — | shipped |
| ~~F2 profile authority~~ | Capability-bound cluster control/runtime delegation, profile protocol v2, resumable `DISABLING`, exact profile receipt recovery | internal v11 + recovery v13 `StreamProfileChange` only; historical ordinary fold remained recovery v12 | shipped |
| ~~F2 lifecycle tranche~~ | Claim receipts + hidden drain `OPEN→DRAINING→SEALED`, empty and non-empty, with restart continuation | internal v12 + lifecycle v3 + recovery v14 | implemented; public control activation remains closed |
| ~~F3a~~ | Private resume / guarded abort-drain | internal v13 + recovery v15 | implemented; public control activation remains closed |
| ~~F3b EnsureIndices~~ | Checked-runtime, main-only, same-binding `SEALED` EnsureIndices | internal v14 + recovery v16 | implemented; no public maintenance surface |
| ~~F3c Optimize~~ | Checked-runtime, main-only, same-binding `SEALED` Optimize | internal v15 + recovery v17 | implemented; no public maintenance surface |
| ~~F3d physical rebind~~ | Offline, terminal-`DISABLED`, exact fresh-scope `SEALED` physical rebind | internal v16 + recovery v18 | implemented; no public rebind surface |
| ~~F3e authority retirement~~ | Cluster-only stopped/offline terminal `WITHDRAWN` retirement plus receipt-bearing export/rebuild | internal v17 + recovery v19 | implemented; source becomes permanently query/status/export-only |
| ~~F3f DataBlock correction~~ | State-lock-held, stopped/offline exact `DataBlock` show plus bounded `REPLACE`/`WITHDRAW` correction | internal v18 + recovery v20 | implemented; clears one exact block and remains `DRAINING` |
| **Later authority repair** | Activate reason-gated `AuthorityBlock` repair only if a reachable producer and finalized evidence grammar exist; frozen v14 scaffolds are never reinterpreted | a new finalized authority-correction shape | deferred; not an F4/F5a/F5b0 prerequisite |
| ~~F4~~ | Hidden ingest vertical slice + lazy enrollment | no format change | implemented behind doc-hidden seams; no public transport |
| ~~F5a~~ | Hidden automatic `OPEN`-lane timer/cap fold supervisor, cold discovery, finite node-before-edge round-robin cohorts, and bounded server shutdown ownership | no format change; reuses recovery-v14 | implemented and server-owned; public status remains closed |
| ~~F5b0 operational cut~~ | Resident exact-`ENABLED` goal-`SEALED` continuation plus deterministic checked offline `DISABLING` convergence, `OPEN_AFTER_FOLD` adoption, and loud `DataBlock` park/resume | no format change; reuses recovery-v13/v14 and existing cluster controls | implemented; no public API |
| ~~F5b~~ | Minimal `DEAD_LETTERED` authority, one object, ordinary-ingest correction, selected-token inspection/export, and retirement | internal v19 + token schema v3 + recovery v21 | implemented behind the hidden row seam; no public HTTP/SDK/OpenAPI |
| ~~F6a~~ | Typed failpoints-only process-local advisory driver snapshot plus one hidden in-process candidate-runtime composition | no format or recovery change | implemented; public durable status remains manifest-only |
| ~~F6b1 checked immutable export cut~~ | Exact-terminal served-export authority, ambient-enrolled refusal, one nonwaiting root slot, and a move-only exact-version cut that releases writer gates before output | no format or recovery change | implemented behind a doc-hidden engine seam; no public transport |
| ~~F6b2 process/lifecycle acceptance~~ | SIGTERM/shared shutdown, sequential OS-process recovery, frozen-round node/edge fairness, rebind/re-enable/reopen/resume, combined maintenance, fresh-target import, and legacy writer refusal | no format or recovery change | implemented; no public route or status |
| ~~F6b3 selected-token uncovered-tail evidence~~ | Exact manifest-selected coverage diagnostics; fixed-cardinality fresh-handle hit/miss and first terminal-page plus warm hit/miss and repeat terminal-page cost across increasing receipt history; fast local plus ignored local/RustFS sweeps | no format or recovery change | implemented behind doc-hidden failpoints-only read seams; no reconciler or status |
| ~~F6b4 dead-letter envelope evidence~~ | Exact production 8,192-candidate one-under/exact/one-over encoding, cap-aware retained capacity, encode/verify timing, paired isolated peak RSS, and real overflow/no-partial-fold assertions | no format or recovery change | implemented behind failpoints-only cost/test seams; 192-MiB remeasurement tripwire, not admission or an SLO |
| ~~F6b5 bounded served export~~ | Pre-header stream-aware cut capture; incremental Lance scans with approximate targets, strict 64-KiB chunks, two-chunk queue, complete per-response/process queue-envelope reservation, deadline, backpressure, and disconnect-safe cut ownership on the existing HTTP/remote CLI/OpenAPI export surface | no format or recovery change | implemented; embedded/direct enrolled export remains refused |
| ~~F6b6 checked operational status~~ | One checked read-only multi-authority cut with physical lane/token/recovery/rebuild evidence, advisory driver projection, and typed `StreamStatusChanged` / `StreamStatusBusy` refusal | no format or recovery change | implemented behind an engine-internal seam; public manifest status unchanged |
| ~~F6b7 token-index decision evidence~~ | Paired current-token and profile-receipt hit/miss work before/after one content-identical exact-selected index refresh, with maintenance I/O and semantic-equivalence proof | no format or recovery change | bounded NO-GO only for the uncompacted profile-cycle fixture; failpoints-only and no standalone production reconciler |
| ~~F6b8 resume/driver handoff~~ | Compile-enforced root-producer-permit transfer into detached resume installation, urgent trigger-before-release, exact empty-owner housekeeping before the unchanged node-before-edge round, cancellation-safe shutdown, and cross-lane root-slot reuse | no format or recovery change | implemented behind existing hidden lifecycle/driver seams; broader retirement-failure matrix remains in F6 |
| **F6b remainder** | Remaining guardrail matrix, including F6b8's post-claim install/retirement-failure cells; token-index evidence reopens at greater depth, after a Lance/index-grammar change, or before considering graph-manifest-compacted / checked-Optimize-coupled maintenance | — | later |
| ~~F7a served graph ingress~~ | One graph-native mixed node/edge NDJSON route, strong graph-authority precondition, remote client/CLI, OpenAPI, and direct-mode refusals over the existing checked runtime and resident driver | no format or recovery change | implemented; no public table/lane selector or management surface |
| ~~F7b served graph status~~ | One checked graph-logical operational cut, read authorization, HTTP/remote-CLI/OpenAPI parity, and structural redaction of physical/recovery identity | no format or recovery change | implemented over F6b6; ambient SDK status remains manifest-only and no lifecycle writer is exposed |
| **F7 remainder** | Graph-level lifecycle and maintenance transports plus their SDK/HTTP/remote-CLI/OpenAPI parity | — | only after their remaining F6 cells pass; export and read-only status are the existing exceptions |

These are dependency milestones, not mandates for giant PRs. Keep each PR
reviewable behind the hidden seam: the next lifecycle tranche may land receipts,
then non-empty/empty drain; F3 may land resume/abort, data correction, then each
content-preserving maintenance owner and rebind. F5a lands orchestration without
a format strand; F5b owns terminal authority/object recovery and the associated
cluster-only inspection/correction.
Every sub-PR preserves the refusal for behavior it has not integrated.

Eight strict export/init/load rebuilds are already implemented across these
control/lifecycle slices: v10→v11/recovery-v13 profile authority,
v11→v12/recovery-v14 lifecycle, v12→v13/recovery-v15 resume, and
v13→v14/recovery-v16 SEALED EnsureIndices,
v14→v15/recovery-v17 SEALED Optimize, v15→v16/recovery-v18 physical rebind, and
v16→v17/recovery-v19 authority retirement, and v17→v18/recovery-v20 exact
`DataBlock` correction. Each future persisted
grammar takes another strand when its final shape differs from a dormant
scaffold. Dormant discriminator names never authorize reinterpretation of
their frozen payload. The exact pre-release strand count is recorded as shapes
settle and freezes at the 0.10.0 release gate; an honest extra strand is cheaper
than pre-registering a guessed on-disk contract.

The lifecycle tranche plus F3 are the operator lifecycle and maintenance
bridge. They ship **no row-admission or acknowledgement capability**; the
profile-authority tranche already narrowed the unsupported ambient profile flip
to validated cluster control. Drain alone is
not enough: current
`Snapshot::ensure_stream_effects_allowed` deliberately refuses merge,
optimize, index work, repair, cleanup, mutation/load, recovery, and schema
apply even at `SEALED`, because they can move the table witness, alter
token/binding authority, adopt drift, or destroy recovery evidence without a
lifecycle-aware proof. F3d supplies the exact witness/rebind transition, and
F3e supplies terminal `WITHDRAWN` retirement and F3f supplies the exact
DataBlock correction owner. The remaining F3 gate is reason-gated
`AuthorityBlock` repair and its evidence.
See §10 for the ordering rationale.

---

## 3. F2 lifecycle tranche — the drain path (`OPEN → DRAINING → SEALED`)

This section records the implemented hidden v12/v14 behavior. It is not a
public management contract: production control-plane and transport activation
remain closed until F3 completes lifecycle exits and maintenance integration.

### 3.1 Goal

An operator can quiesce a stream: close admission, fold everything
acknowledged, prove the exact empty cut, and reach `SEALED`. Native branch-ref
controls may then run. Other maintenance remains refused until F3 gives each
writer a sidecar-covered witness/rebind transition.

### 3.2 What exists

- The lifecycle-v3 vocabulary: `StreamLifecycle`, `DrainDescriptor`,
  `SealedProof`, `ManagementReceipt`, `ClaimReceipt`, `StrictBlock`,
  `LastFoldSummary` — all in `db/manifest/stream.rs`, all with per-state
  validators enforced by `StreamLifecycleEntry::validate`.
- `QuiesceRequestPayload`, the typed immutable request preimage retained inside
  every drain descriptor. It binds protocol/graph/table/stream/binding/
  enrollment/drain identity, the original lifecycle revision and goal, the
  physical-binding digest, original HEAD witness, original epoch targets, and
  a null fresh-request override. Its digest is recomputed from that object.
  The descriptor's current goal, HEAD witness, achieved epoch targets, and
  authenticated disable-adoption override are separate mutable continuation
  authority and never rewrite the request preimage.
- The full-entry lifecycle CAS: `ManifestChange::SetStreamLifecycle`, with the
  publisher requiring the witness to match the batch's effective table pointer.
- Physical seal/drain/abort in `worker.rs` (§1.2).
- Recovery-v14 claim, ordinary-fold, drain-fold, and terminal-receipt owners,
  including exact participant and selected-ledger validation.

### 3.3 Implemented hidden lifecycle path; public activation remaining

1. **Implemented: close the ambient profile-writer gap.** Replace public
   `Omnigraph::set_streaming_enabled_as` with a narrow capability-bound
   control-plane adapter. Its `CheckedClusterApplyAuthority` is distinct from
   the serving process's `CheckedClusterStreamRuntimeAuthority` and is minted
   only while reconciling one validated team-owned cluster snapshot. It binds
   the cluster/root identity, graph identity and exact store mapping,
   resource-local declaration revision/digest, requested profile state, and
   authenticated apply actor. That revision is derived from this graph and
   declaration, not the global cluster-config digest, so an unrelated config
   edit does not invalidate the live runtime. The engine rechecks that scope and
   `stream_manage` before the
   existing exact-entry CAS. V11 profile changes carry a stable apply operation
   ID and expected profile revision and publish an immutable tagged
   `ProfileManagementReceipt` in the manifest-selected
   `_stream_tokens.lance` ledger, so a delayed reconciliation retry returns its
   original result rather than retargeting a later cycle. The hot profile row
   retains only its bounded current receipt pointer and chain commitment. There is
   no ambient constructor or raw
   `Omnigraph` flip; direct SDK and direct `--store` callers receive typed
   `StreamingRequiresClusterControlPlane`. A test-only mint remains
   feature-gated and doc-hidden.

   A streaming-profile change has one supported process topology. The
   deployment controller first gracefully stops every writer-capable process
   for the graph (normally the sole server) and confirms process exit. F2 owns
   that minimal handoff plus the synchronous offline drain loop; at this slice
   no production streaming admission/supervisor existed. F5a now strengthens
   server shutdown to close transport admission, settle the invoked tail, and
   join every graph supervisor concurrently before exit, and factors the shared
   scheduler core without changing ownership. Only after the handoff may
   `cluster apply --confirm-stream-offline` acquire the mandatory cluster state
   lock, become the sole graph writer, and mint
   `CheckedClusterApplyAuthority`. `state.lock: false` refuses a streaming-
   profile change. The confirmation flag is an explicit attestation of the
   experimental profile's externally enforced single-writer precondition, not
   a distributed lease and not proof that process-local code found every
   foreign process. Its plan and apply output must also say, in plain words,
   that this release has no public firehose ingress and that enabling the
   profile removes embedded/direct content mutation. Disable can reach
   `DISABLED` with no lanes or with only already-`SEALED` lanes, but it cannot
   drain a non-`SEALED` lane in this tranche. Only the no-lane case restores
   embedded/direct mutation; after any table is enrolled, only a strict
   rebuild restores the non-streaming direct path.
   F2 updates the cluster operator guide: profile apply concurrent with a writer
   server is unsupported.

   The capability design compiles across the workspace DAG. The implemented
   split uses the shared `omnigraph-storage` leaf plus
   `omnigraph-control-authority`, preserving one storage path and unforgeable
   minting without an engine/cluster cycle. Filesystem/lock behavior does not
   move into `omnigraph-api-types`, and no caller-implemented trait is accepted
   as authority.

   The selected lower authority boundary mints
   private-field `ValidatedOfflineGuard` or `ValidatedRuntimeGuard` values only
   by loading and validating the canonical cluster snapshot and then retaining
   the acquired lock/registration for the guard lifetime. The engine exposes
   its checked authority wrappers as `pub` but doc-hidden, with private fields
   and no `Default`, deserialization, clone-to-extend-lifetime, raw-boolean, or
   ambient constructor. Their sole constructors consume one of those validated
   guards and bind the normalized cluster/root, graph/store mapping,
   declaration/profile revision, operation class, and actor. The cluster and
   server crates depend downward on that API; the engine never names a cluster
   or server type and no dependency cycle or trait-implemented-by-any-caller
   mint is permitted.

   Profile authority is graph-wide write authority, not merely a gate on the
   new ingest seam. F2 centralizes a pre-effect check in every content writer,
   including existing Mutation/Load insert, update, upsert, and delete paths.
   While the profile is `ENABLED`, those ordinary content mutations require the exact live
   `CheckedClusterStreamRuntimeAuthority` and therefore enter only through the
   sole served runtime. While it is `DISABLING`, ordinary content mutation is
   closed; only an exact operation admitted by the checked stopped/offline
   owner may continue its sidecar-bound drain, recovery, correction, or rebind
   effect. That offline capability is not a blanket Mutation/Load permission.
   Embedded SDK and direct `--store` callers refuse before body ownership,
   staging, recovery arm, or Lance effect. Cedar remains additionally required
   but cannot by itself authorize a direct lane. A checked owner is necessary,
   not sufficient: per-table stream/token rules may still refuse a legacy
   mutation on an enrolled table.
   BranchMerge is stricter than Mutation/Load/delete: it remains closed while
   the profile is `ENABLED` or `DISABLING`, even with the checked served
   runtime, until a token-aware merge sequencing transition exists.

   Profile apply loads the graph policy from both the currently applied
   revision and the desired revision and enforces their conjunction through
   the profile effect → cluster-state CAS window. If only one revision binds a
   policy, that policy governs; an unchanged address/digest pair is compiled
   once. Consequently, a newly needed `stream_manage` grant must land in a
   policy-only apply before the profile transition, while a revocation must
   follow the profile transition in a second apply. Any blocked profile
   transition demotes current- or desired-bound policy changes for that graph
   before the state CAS, preserving the currently selected policy authority
   needed to retry.
   The stable operation ID and request digest intentionally exclude actor, but
   the immutable receipt stores and commits actor separately; terminal receipt
   replay requires the same actor. A different actor cannot adopt that
   occurrence after the engine effect/state-CAS crash window. If the original
   identity is unavailable, the supported control-plane recovery is
   `cluster refresh` from manifest truth followed by a new plan, not
   relabeling the retained receipt.

   Before either profile transition's first CAS, apply runs the graph-global
   recovery barrier while the old profile revision/delegation is still
   authoritative and settles or refuses **every** graph-content or authority
   sidecar: Mutation/Load, SchemaApply, BranchMerge, Optimize/EnsureIndices,
   repair/cleanup, enrollment, writer claim/cold-WAL reconstruction, fold,
   lifecycle, token-ledger maintenance, authority retirement (the lifecycle
   strand or F5's version-appropriate terminal-authority strand),
   and rebind. It then releases those gates,
   reacquires from the root in canonical order, and recaptures/revalidates the
   exact cleared authority. Ambiguous old recovery returns `RecoveryRequired`
   with no profile effect. Enable then publishes its receipt and exits before
   the server restarts. F5b0 makes disable owned synchronously by the same no-ingress
   apply process: it publishes `DISABLING`, instantiates a temporary drain
   owner with only the durable fold continuation, recovers/drains every
   manifest-selected lane, and publishes `DISABLED`.
   A crash leaves the exact disable plan for the next offline apply to resume. A
   structural block leaves apply visibly pending with a block token. F3f's
   narrowly capability-bound offline `cluster stream block show|correct`
   controls take the same cluster lock and sole-writer attestation for an exact
   `DataBlock`, after which apply may resume. They are not raw `--store` arms.
   A reserved `AuthorityBlock` remains fail-closed until its separate repair
   owner lands. Normal server startup refuses a `DISABLING` graph and directs
   the operator to that offline recovery loop. Only after apply exits may the
   server restart and validate the cluster-ledger result against the manifest
   profile revision and delegation.

   Internal v11 also replaces the profile boolean with the discriminated
   `DISABLED | ENABLED | DISABLING | RETIRED` authority and makes
   automatic-fold authorization explicit. An enable CAS installs one bounded immutable
   `FoldDelegation`, issued by the Cedar-authorized apply actor to fixed
   `omnigraph:stream-fold` and bound to the cluster/declaration/profile
   revision. `DISABLED` has neither delegation nor plan; `ENABLED` has one
   active delegation only; the first disable CAS consumes it into the narrower
   plan continuation, so `DISABLING` has no admission-authorizing delegation.
   `RETIRED` carries its mandatory immutable retirement receipt/cut reference
   and has no outgoing transition. V12 decoded that dormant state fail-closed;
   F3e later activated the exact `DISABLED → RETIRED` transition before F3f
   made `WITHDRAWN` reachable.

   Disable is a durable multi-publication operation, not a retrying error. F5b0
   implements the continuation below without changing its v11/v14 persisted
   shapes. The
   profile-authority tranche introduces one graph-profile admission gate
   outside every table gate and takes it exclusively for the first disable
   CAS. It remains the outermost gate for ordinary and other
   non-resident-producing writers. F6b2 makes each resident-producing served
   row path reserve bounded preprocessing/inflight ownership, then take the
   root MemWAL opportunity shared, graph-profile shared, table admission, and
   the same-key queue. Those outer permits transfer through `put_no_wait`,
   watcher durability, and same-writer fence classification. The resident
   driver owns the root opportunity exclusively across one frozen finite round
   and then takes profile/admission per candidate; opportunity permits retain
   the `MemWalWorkerRegistry` `Arc`, preventing weak-root fence ABA.
   This gate orders owners inside the one current process; it is not the
   cross-process handoff. The required server-exit/apply-start sequence above
   supplies that boundary. Under the apply process's gate exclusively, the
   capability-bound adapter
   CASes `ENABLED → DISABLING`, advances the profile revision, and persists a
   `DisablePlan` containing operation ID, request digest, target declaration
   revision/digest, actor, and a drain-only `FoldContinuation` derived from the
   old delegation. From that
   CAS onward every put and lazy enrollment refuses, while the offline
   temporary owner and recovery may only fold/quiesce existing lanes under the
   exact continuation.
   For each manifest-selected lane, the continuation handles exact current
   state rather than blindly starting another drain:

   - `OPEN` derives a stable per-table drain ID/request digest from
     `(disable operation ID, stable table identity, incarnation)` and runs the
     ordinary receipt-bearing quiesce protocol under the fixed system actor.
   - `DRAINING(goal = SEALED)` continues that exact existing drain ID.
   - `DRAINING(goal = OPEN_AFTER_FOLD)` first settles its owners, then performs
     one metadata-only `DisableDrainAdoption` CAS. It compares the complete
     profile plan and lifecycle row, preserves drain ID, block, witnesses,
     epoch targets, initiating actor/time, and guarded-operation nullness,
     changes only the goal to `SEALED`, records the disable-operation override,
     increments lifecycle revision, and atomically publishes a terminal
     management-ledger receipt through recovery-v14
     `StreamLifecycleReceipt(kind = DisableDrainAdoption)`.
     Its deterministic adoption ID is derived from the disable operation,
     table identity, and existing drain ID. Receipt lookup precedes row/revision
     checks, so restart or a lost response is exact. Any correction preserves
     the adopted goal and the same drain can then reach its empty proof.
   - A non-null guarded operation must settle before adoption; disable remains
     visibly pending rather than stealing it. `SEALED` needs no work.

   This is the only permitted drain-goal retarget; it never opens admission or
   discards a block. The plan stores no parallel job queue—manifest lifecycle
   is work authority. A replacement offline apply reconstructs the closed gate
   and resumes the same operations; a serving runtime does not take over a
   partial disable. After every lane is `SEALED` and recovery is settled, one exact
   CAS publishes
   `DISABLING → DISABLED`, publishes its terminal
   `ProfileManagementReceipt` ledger row, and clears both plan and delegation.
   Profile-receipt/plan lookup precedes desired
   revision: while `DISABLING`, every later offline apply first finishes the
   persisted plan using its retained declaration and continuation, even if
   desired config has changed. Reusing that operation ID with another digest
   conflicts; after terminal `DISABLED`, a fresh operation reconciles the
   latest declaration. `DISABLING` has no cancel/re-enable edge: finish
   `DISABLED`, then a fresh enable operation may install a new delegation.
   That enable CAS does **not** rewrite lifecycle rows or silently
   reopen previously sealed lanes. Its bounded receipt/result records only the
   exact profile/lifecycle cut, a `resume_required_count`, and the canonical
   digest of the ordered sealed identities. After the serving runtime starts,
   paginated status lists the **currently remaining** sealed identities from
   one manifest snapshot; its cursor binds that revision and becomes stale
   rather than mixing pages if lifecycle moves. An operator must run the
   ordinary revision-fenced resume for each. Receipt-first replay returns the
   same bounded original summary even if some lanes have since resumed; it
   does not promise to reconstruct the original identity list.
   Absent lanes remain eligible for lazy enrollment. Status and `cluster
   apply` distinguish “profile enabled” from “all enrolled lanes open.”
   Removing `graphs.<id>.streaming` is **not** a disable operation. F2 changes
   cluster planning so removal while manifest mode is `ENABLED` or
   `DISABLING` returns typed `StreamingProfileMustDisableFirst`. The operator
   must apply explicit `enabled: false`, let the retained disable plan reach
   terminal `DISABLED`, and only then run a second apply that unmanages the
   declaration. Beside `RETIRED`, declaration removal is configuration-only
   and cannot clear or change manifest authority; any request to enable or
   otherwise transition/refine the profile returns `StreamAuthorityRetired`.
   Server startup refuses an absent declaration beside
   `ENABLED`/`DISABLING` instead of minting authority from stale ledger state.
   For manifest `RETIRED`, declaration presence or prior unmanagement can mint
   only the checked read/query/status/export-only boot capability described
   below, never runtime or mutation authority.
   Unmanaging after `DISABLED` does not erase an enrollment, discard its sealed
   proof, or re-authorize Mutation/Load on that table. Returning an enrolled
   graph to a non-streaming physical format still requires the strict
   export/init/load rebuild.
   The F2 operator-doc update replaces today's permissive “remove to stop
   managing” wording with this two-apply sequence.
   This evolves the currently inert `disable_pending_since` slot rather than
   leaving a non-durable "pending" loop. Thus a policy/config refresh cannot
   strand acknowledged work or admit another row behind disable. The existing
   read-only status projection becomes additively mode-aware and reports the
   disabling operation/revision and remaining undrained tables without
   claiming physical progress it has not observed.
2. **The hidden `stream_quiesce_as` engine seam** — new
   `db/omnigraph/stream_lifecycle.rs`, sibling to `stream_profile.rs`.
   It remains `pub(crate)` and doc-hidden; F7 exposes it only through the
   server-owned cluster-runtime capability.
   Requires caller-minted `drain_id` + expected `lifecycle_revision`.
3. **The receipt ledger and bounded hot authority.** V11 removes inline profile
   history from `stream_profile`. The manifest-selected
   `_stream_tokens.lance` dataset gains tagged immutable
   `ProfileManagementReceipt` rows. V12/recovery-v14 extends this mechanism
   with `EnrollmentReceiptV2`, `BindingReceipt`,
   `ManagementReceipt`, `ClaimAttemptEffect`, and terminal `ClaimReceipt`. A row has a
   deterministic identity derived from graph, stable table/incarnation and
   binding scope where applicable, receipt tag, operation ID, and attempt
   ordinal. Retain-all preserves every row, but the hot profile—and,
   lifecycle rows—retain only bounded current
   receipt IDs, counts, domain-separated chain digests, and tail commitments.
   They never contain a history `Vec`. V11 gives current-token and
   profile-receipt rows disjoint trusted row tags and canonical key domains; the
   v12 strand extends the control-ledger tags without reinterpreting them.
   Every token probe constrains the
   current-token tag and token lookup key, while every receipt probe constrains
   a ledger tag; a receipt can neither collide with nor materialize as a
   logical current-token row.

   Every v11 profile-receipt append is an exact pre-minted
   `_stream_tokens.lance` transaction owned by recovery-v13
   `StreamProfileChange`. Active v12 lifecycle-ledger appends use their
   separately selected recovery-v14 owners.
   Any later-format receipt tag uses its strand's matching recovery envelope
   and does not reinterpret v13/v14. F5's dead-letter authority lives in the
   one current per-key token row and one recovery-owned object; it does not add
   a receipt-history or replay-checkpoint family. The sole graph-manifest CAS
   advances the selected token-dataset pointer
   together with the corresponding hot pointer/count/chain commitment,
   lifecycle or profile revision, and any base/token effects. A ledger
   transaction without that CAS is inert recovery residue; the CAS cannot name
   a ledger row that the exact transaction did not create. Every ledger row
   has one versioned canonical
   `record_lookup_key` plus a common chain envelope containing its scope/tag,
   contiguous ordinal, predecessor record ID, prior chain digest, and resulting
   chain digest. Receipt-first retry performs one exact scalar-index lookup.
   Exact receipt lookup remains internal idempotency/recovery authority. The
   experimental product exposes no receipt-history pagination or audit-history
   cursor. Retained chain rows are not a public management-history product.

   A scalar index alone is **not** a no-scan proof: Lance scans fragments
   appended after that index's coverage until `optimize_indices` folds them in.
   V12 reserves `StreamTokenLedgerIndexMaintenance` but keeps its frozen
   scaffold fail-closed; a different final payload requires a new strand.
   Logical operations remain correct through Lance's uncovered-fragment
   fallback. The reconciler is not an F3 correctness prerequisite for EXP.
   F6b3 measures exact current-token hit/miss plus cluster-only terminal-page
   scans across increasing **uncovered** receipt history locally and on
   RustFS/S3. F6b7 adds a paired failpoints-only cut: after excluding token
   writers and proving raw HEAD equals manifest selection, it permits only the
   named lookup index's content-identical `CreateIndex` successor, selects that
   exact witness, and compares current-token plus profile-receipt hit/miss work.
   The measured maintenance window contains `optimize_indices`, exact transaction
   classification, and manifest selection; gate/coordinator setup, pre/post
   content proofs, and final graph refresh sit outside it. The configured-RustFS
   result is a bounded NO-GO only for the uncompacted profile-cycle fixture, not
   a universal token-index NO-GO, so no standalone production reconciler is
   scheduled. Remeasure at greater depth, after a Lance/index-grammar change, or
   before considering graph-manifest-compacted or checked-Optimize-coupled
   maintenance. F6b6's internal checked status reports
   exact uncovered counts when Lance exposes coverage and explicitly reports
   oldest age unavailable because the selected cut has no exact fragment-
   creation timestamp; its public transport remains F7. Ordinary graph
   `optimize` does not maintain this dataset.

   `ManagementReceipt` now carries bounded canonical request **and result**
   payloads with digest recomputation in validation. A quiesce request is one
   typed immutable `QuiesceRequestPayload`; it is not reconstructed from the
   descriptor's mutable current fields. The payload fixes the protocol and
   graph identity, stable table/incarnation, stream incarnation, binding scope,
   enrollment and drain IDs, original expected lifecycle revision and goal,
   physical-binding digest, original HEAD witness, original target-epoch map,
   and a null fresh-request seal override. Claims, folds, and disable adoption
   may advance the descriptor's current HEAD/target, or authenticate the sole
   `OPEN_AFTER_FOLD → SEALED` override, without changing that payload or its
   digest. The §4.3 lookup
   order is terminal indexed ledger receipt first; then an exact matching
   in-progress `DrainDescriptor`/sidecar (same operation ID, request digest,
   and original expected revision), which resumes that plan; only then the
   expected-revision check for a new operation. Same occurrence + same digest
   returns or resumes the recorded plan; same occurrence + different digest is
   `StreamIdempotencyConflict` at either retained authority; no retained
   occurrence + revision mismatch is effect-free `StreamLifecycleChanged` that
   **never retargets**.
4. **The effectful claim-receipt discipline.** Every B2 epoch-advancing
   claim—cold open, quiesce, resume/abort, and any writer claim whose epoch an
   ordinary fold adopts—that creates a manifest or sentinel effect remains
   sidecar-owned until the same lifecycle CAS publishes its complete terminal
   `ClaimReceipt` ledger row, names it with `current_claim_receipt_id`, and
   advances both the top-level epoch floor and any active drain target to the
   exact achieved epoch. The v14 fold binds and carries that already-produced
   receipt and authenticated tail; it does not mint a claim receipt itself.
   No effectful claim may finalize through the `(None, None)` state.

   Claim retries are durably incremental rather than capped or accumulated
   inline. The sidecar owns only the current Lance attempt. Recovery first
   classifies that exact attempt; before it may arm or invoke another Lance
   attempt, one ledger transaction plus the sole manifest CAS publishes a
   `ClaimAttemptEffect` with the next contiguous ordinal and advances the hot
   `attempt_count`, attempt-chain digest, and tail receipt ID. A terminal
   `ClaimReceipt` binds that chain commitment and achieved effect, not an
   attempt vector. Restart therefore cannot forget an attempt, duplicate an
   ordinal, or rescan old attempts, while transient authority movement does
   not require an arbitrary cross-restart retry cap. A contradictory or
   unclassifiable attempt remains recovery-owned and uses F3's reason-gated
   authority repair; it is never converted into another blind Lance call.
5. **`OPEN → DRAINING` CAS** with the crate-private `DrainDescriptor` builder,
   currently reachable only through the feature-gated hidden quiesce seam.
6. **Drain-mode fold.** This is not the historical `OPEN` fold with a relaxed
   check. Recovery-v14 `StreamDrainFold` binds the complete expected
   `DRAINING` row, `drain_id`, selected current claim, authenticated cut, and
   recomputed full-generation LWW projection. The fold consumes injected
   exclusive stream authority rather than reacquiring admission.
7. **The empty-lane path.** An enrolled-but-empty or already-folded lane has no
   generation for `seal_and_drain`, and the current worker rejects attempting
   to seal one. Under the still-held exclusive lease, settle every owner. If
   the current epoch/sentinel is already authenticated by a receipt/confirmed
   claim bound to this exact `drain_id` and its achieved epoch satisfies the
   descriptor's target floor, reuse that retry authority. The pre-drain
   `OPEN` writer's receipt is never sufficient: a new drain must claim strictly
   above its floor to fence that owner. Otherwise arm the claim sidecar before
   calling Lance: exact no-effect may retire, but any
   manifest/sentinel effect must publish its complete `ClaimReceipt` and
   current ID into the `DRAINING` row while advancing both its top-level epoch
   floor and mutable `drain.target_epoch_floor_by_shard` to the achieved epoch
   before proof construction. Immutable `drain.operation_request_payload`
   remains the complete original preimage for terminal receipt/recovery
   verification even when the current HEAD, target, goal, or override evolves.
   Then classify and commit the fence-only WAL segment,
   prove shard/base merge agreement directly, and proceed without inventing a
   generation or calling `stage_stream_fold`. `(None, None)` is never a
   `SealedProof` input.
8. **Incremental authenticated WAL-tail commitment and
   `verified_empty_digest`.** Lifecycle-v3 constructs and validates this
   digest from the exact current claim and empty-cut evidence. V12 adds bounded current
   per-binding fields for the committed WAL cursor, segment count, segment-
   chain digest, current segment LWW-projection digest, and current
   claim-receipt ID. A rebind starts a new scoped genesis; the old scope
   remains immutable ledger provenance.

   **Claims, not folds, own these segments.** Every terminal claim uses Lance's
   public WAL tailer to stream exactly
   `(prior_authenticated_cursor, achieved_sentinel_cursor]`. It densifies and
   charges each page, rejects gaps, duplicates, foreign
   binding/shard/epoch records, and authenticates trusted metadata plus
   token-chain continuity. If the latest manifest-selected
   `LastFoldSummary(outcome = PUBLISHED)` cut falls inside that delta, claim
   preparation treats it as the candidate published-prefix boundary and fixes
   that position in recovery. The terminal `ClaimReceipt` then owns the exact
   authenticated boundary; later empty proof reads the receipt and never
   reclassifies from the replaceable summary. Entries through the cut are a
   published prefix: each key's internal chain must terminate at exact current
   token authority. Entries after the cut are the active suffix, whose first
   occurrence starts a fresh depth-one chain from that same current token/base
   authority. A raw Lance replay cursor can also cover an unmerged generation,
   so it is only a physical upper bound and never chooses the published-prefix
   boundary. The no-roll delta is bounded by one
   8,192-row/32-MiB generation plus bounded control records. A claim-only or
   empty-lane cycle therefore commits a bounded control-only delta before
   another claim, so repeated empty cycles advance the cursor instead of
   rescanning from genesis. The receipt also commits the streaming LWW
   projection digest; a later drain fold recomputes it through
   `LsmScanner::without_base_table` and byte-compares the resulting winner/token
   plan.

   The terminal `ClaimReceipt` commits, with domain separation, the prior chain
   digest, exact lower/upper cursor, receipt-owned published-prefix position,
   record count/digest, decoded empty-fence state,
   binding/configuration/incarnation, and active-suffix plus cumulative LWW
   projection commitments. Its
   exact ledger transaction and lifecycle update are owned by recovery-v14
   `StreamClaim`;
   only their sole graph-manifest CAS advances the selected token pointer and
   lifecycle cursor/count/chain. It has no base-table or current-token effect.
   A failed or unclassifiable claim does not advance the segment cursor.

   Historical recovery-v12 remains byte-for-byte the lifecycle-v2
   `StreamFold` and is refused under v12. Recovery-v14 `StreamFoldV2` owns an
   ordinary `OPEN` fold over lifecycle-v3, while `StreamDrainFold` is the
   separate drain variant. `StreamFoldV2` owns the same exact
   base+token effects as v12, preserves the current claim/tail commitments
   byte-for-byte, and publishes no segment receipt. A drain fold additionally
   binds the current claim receipt and recomputes its LWW projection before its
   base/token publication. A strict-blocked fold leaves the already
   authenticated claim segment in place but advances no merge/base/token
   authority; `SEALED` remains impossible until a later successful fold proves
   agreement.
   `verified_empty_digest` then commits the current authenticated segment tail,
   exact empty fence, base witness, ordered shard-manifest state, and
   replay/merge cursors. It never scans WAL from genesis and never treats the
   replaceable `LastFoldSummary` as authority.
9. **`DRAINING → SEALED`** publishing the exact proof, current claim receipt,
   terminal management receipt, and complete empty-cut authority atomically
   through recovery-v14
   `StreamLifecycleReceipt(kind = QuiesceFinalize)`.
   `StreamLifecycleReceipt` is the ledger-plus-manifest recovery owner for
   these two metadata-only lifecycle transitions. Before its ledger effect it
   fixes the subkind, operation/request digest, exact profile and lifecycle
   prestate, pre-minted receipt-ledger transaction, target lifecycle row and
   receipt-chain commitment, and selected token-pointer outcome. Recovery
   classifies that exact transaction and permits only the one matching
   manifest CAS; a created receipt without the CAS remains inert. It does not
   subsume resume/abort, folds, corrections, or other already-discriminated
   sidecar families.

### 3.4 Contract points that are easy to get wrong

- **Quiesce is multi-publication.** It is *not* complete at the initial
  `OPEN → DRAINING` CAS. The `DrainDescriptor` is the restart plan; the
  terminal management receipt appears only with `SEALED`. Restart continues
  `DRAINING` and **never auto-opens**.
- **Admission remains exclusively closed for the whole quiesce.** Never hold a
  table queue while waiting for a fold that needs it: close admission first,
  then let the injected-authority fold acquire and release the normal table
  queue. Releasing the admission lease between folds is forbidden.
- **A permanent validation failure attaches a `StrictBlock`** to the same
  descriptor without changing its goal, and writes
  `LastFoldSummary(outcome = STRICT_BLOCKED, graph_commit_id = null)` in the
  same lifecycle CAS.

### 3.5 Closed decisions and format rule

- **Claim receipts are mandatory.** RFC-026 §4.3 and
  `SealedProof::validate` leave no receipt-free route to `SEALED`.
- **Quiesce retains one exclusive admission lease.** Refactor fold around
  injected checked authority; do not release/reacquire.
- **The bounded profile-authority tranche is exact.** It advanced the internal
  graph stamp from v10 to **v11**, selected profile protocol v2, and added
  recovery-v13 with exactly one emitted discriminator:
  `StreamProfileChange`. `protocol_v12` remains byte-for-byte
  `StreamFold`-only; v13 is not an ordinary- or drain-fold meaning.
  `StreamProfileChange` owns one exact token-ledger
  `ProfileManagementReceipt` transaction, and its sole terminal manifest CAS
  selects both the achieved token witness and fixed next profile.
  V11 stores a bounded receipt-chain reference plus `DISABLED`, delegated
  `ENABLED`, resumable `DISABLING`, and fail-closed `RETIRED` states.
  `DISABLING` owns the exact disable plan and drain-only fold continuation, so a
  restart cannot silently reopen or discard the operation. V17/recovery-v19
  activates only the stopped/offline, cluster-only
  `DISABLED → RETIRED` transition and receipt-bearing export/rebuild exit.
  Unknown discriminators and unsupported transitions fail closed.
- **The v12 lifecycle family is frozen and selectively active.** Recovery-v14
  activates claim, `StreamEnrollmentV2`, ordinary fold-v2, drain-fold, and
  `StreamLifecycleReceipt`. Its three-field resume scaffold, correction, retirement,
  token-ledger-index maintenance, sealed maintenance, and rebind are registered
  but fail closed under v14. V13/recovery-v15 activates the complete
  crate-private resume/guarded drain-abort owner without reinterpreting that
  scaffold. V14/recovery-v16 activates the distinct checked `SEALED`
  EnsureIndices shape; v15/recovery-v17 activates checked `SEALED` Optimize
  without reinterpreting the older maintenance scaffold. V16/recovery-v18
  activates the complete private physical-rebind owner without reinterpreting
  the three-field v14 rebind scaffold. Historical recovery-v10 enrollment and recovery-v12
  lifecycle-v2 base-plus-token fold retain their exact meanings and are refused
  under lifecycle-v3. V17/recovery-v19 activates retirement before correction
  can create `WITHDRAWN`; v18/recovery-v20 activates exact DataBlock correction
  without reinterpreting v14's incomplete correction scaffold. Current
  v19/recovery-v21 adds the distinct terminal fold and three-disposition
  retirement owners without changing either historical payload.
- **Each lifecycle strand requires genuine binary evidence.** The historical
  v11↔v12, v12↔v13, v13↔v14, v14↔v15, v15↔v16, and v16↔v17 seams plus the
  v17↔v18 seam use a genuine
  old-binary/new-format refusal and export/init/load rebuild test with the
  immutable final predecessor binary, not a stamp rewrite. The fixture is clean,
  disabled, and unenrolled because ordinary export does not transfer stream
  authority. V19's successor cell builds immutable final v18 and proves the
  genuine v18↔v19 boundary; its frozen production-export artifact separately
  pins v18 receipt-v1 retirement compatibility. Local v18-refusal/current-v19/
  future-v20 grammar assertions supplement, but do not substitute for, that
  evidence.

### 3.6 Evidence

Extend existing owners; do not open a new silo.

- **CI follows the sustainable repository policy.** Pull requests run the
  ordinary conservative classifier, documentation/link checks, entrypoint
  check, and AWS-feature test. Authors run the affected lifecycle/recovery,
  claim, drain, failpoint, and `forbidden_apis` owners locally and record the
  exact commands in the PR. The full feature-superset workspace,
  immediate-predecessor format fence, and RustFS graphs run post-merge, on
  tags, or by manual dispatch. A red post-merge `main` is stop-the-line until
  fixed or reverted. There is no custom attested/keyed dependency artifact or
  required firehose rebuild context. A dedicated PR protocol check may return
  only after an isolated harness demonstrates measured empty-runner and warm
  p95 within its proposed budget; an opportunistic cache is not correctness
  evidence. Near-cap, RustFS/S3 fault, endurance, and performance matrices
  remain post-merge, scheduled, or explicitly opt-in according to their cost.
- Capability tests prove only cluster-state-locked offline reconciliation
  after the explicit writer-process handoff can flip the profile; an ambient
  `Omnigraph`, embedded SDK, and direct `--store` caller cannot. The durable
  fold delegation/disable continuation is exact, auditable, required for
  runtime startup, and cannot be removed before disable drains. Removing the
  managed declaration refuses at `ENABLED`/`DISABLING`, succeeds only after a
  separate explicit disable reaches ordinary `DISABLED`, and beside `RETIRED`
  cannot change manifest authority; startup rejects every writer-authority
  declaration/manifest mismatch, while retired read-only boot mints only the
  checked served-export capability.
  Compile-time/API tests prove the selected
  authority/storage boundary creates the only validated guards, engine checked
  wrappers cannot be forged or deserialized, the engine has no cluster/server
  dependency, and no public trait implementation or ambient constructor mints
  either capability. With the profile `ENABLED` or `DISABLING`, legacy direct
  Mutation/Load/delete entry points and their `_as` variants refuse before
  staging/effect without the exact checked owner; Cedar authorization alone
  never opens them.
- **The dependency-DAG and checked-authority slice was implemented on
  2026-07-29.** The selected
  split adds `omnigraph-storage` and `omnigraph-control-authority`, producing a
  nine-crate workspace. The former owns the one local/S3 control-object
  implementation; the latter owns the exact persisted cluster lock below the
  engine/cluster split. Publish order is storage → control-authority → engine →
  cluster/server/CLI. Workspace members, `Cargo.lock`, dependent manifests, CI
  classifiers, forbidden-API scans, and the inventories in `AGENTS.md`,
  README, canon, and architecture move with it. Opaque checked offline and
  runtime guards bind the canonical cluster snapshot, declaration/profile
  revision, actor, runtime lifetime, and manifest fences; ambient embedded and
  direct-store callers cannot mint them. Historical v10 profile state,
  protocol-v10 enrollment, and protocol-v12 ordinary fold meanings remain
  unchanged.
- `db/manifest/tests.rs::stream_lifecycle_and_table_pointer_publish_in_one_manifest_cas`
  already walks `None→OPEN→DRAINING→SEALED` — extend with stale-`expected`
  refusal and revision monotonicity.
- `db/manifest/stream.rs` in-source tests own per-state validation.
- `memwal_stream.rs` owns drain with a resident generation, while worker and
  lifecycle tests own enrolled-never-written, already-folded empty, passive
  flushed-cut reuse, and fresh-claim-required restart decisions.
- `failpoints.rs` owns every lifecycle-CAS crash boundary; extend
  `assert_open_stream_lifecycle_conflict` with DRAINING/SEALED variants.
- New: revision-fence cells (stale refusal, lost-response replay returning the
  recorded receipt, same-ID/different-digest conflict).
- New: every claim effect class (no effect, manifest only, exact
  manifest+sentinel, lost result, higher-epoch recovery) leaves either no
  authoritative effect or one retained current receipt; higher-epoch recovery
  advances the entry and active drain target floors atomically.
- New: WAL-segment cells inject a gap, duplicate, foreign binding/epoch, strict
  block plus retry, and crash at segment publication; repeated empty claim
  cycles advance a control-only cursor, and the streaming LWW projection
  byte-matches the fold token plan without a genesis scan.
- Current format/recovery cells retain v13 `StreamProfileChange` exactly and
  exercise recovery-v14 ordinary/drain folds at both participants plus
  `StreamLifecycleReceipt` before/after its ledger transaction and terminal
  manifest CAS. They preserve the selected claim/tail commitment exactly and
  prove folds never append a claim receipt.
- **The lifecycle tranche co-lands the v11→v12 operator path** in
  `docs/user/operations/upgrade.md`, the cluster operator guide, and the release
  notes for the binary that introduces internal v12. The old v11 binary
  gracefully stops every writer, applies an
  explicit disabled profile, verifies terminal disabled state and zero
  production enrollments, and exports the visible logical graph; the operator
  then initializes a fresh v12 root, loads that export, applies cluster config,
  and restarts. V12 never opens or migrates a v11 root in place. The genuine
  cross-version fixture is deliberately clean, disabled, and unenrolled; any
  private/dev lifecycle, pending WAL, or receipt authority must be quarantined
  rather than claimed transferred by ordinary export.
- F2 also updates the mutation/CLI/error and cluster-configuration user docs
  for the behavior it changes immediately: under `ENABLED`, Mutation/Load/delete
  are served-runtime-only; under `DISABLING`, they are closed, and BranchMerge
  is closed under both modes even through the served runtime. Embedded SDK or
  direct `--store` mutation refuses even though public firehose ingress is not
  active yet. The same update states that unmanaging a terminally
  disabled declaration does not de-enroll its sealed tables or restore the
  direct lane. The F2 release note repeats—not merely links—the warning that
  `streaming: true` is non-additive in that release: it disables embedded/direct
  Mutation/Load/delete while no public firehose ingress exists, and it gives the
  explicit-disable escape for an unenrolled graph, the ability to finish the
  profile transition with only already-`SEALED` lanes, and the rebuild
  requirement after enrollment to restore the direct lane. F7 extends this
  already-public baseline with the actual
  streaming surfaces; it does not defer documentation of F2's refusal.

---

## 4. F3 — resume, abort-drain, and `SEALED` maintenance

### 4.1 Goal

`SEALED → OPEN` (resume) and `DRAINING → OPEN` (abort-drain), both
revision-fenced and caller-identified by `resume_id`; plus the missing bridge
that lets maintenance move a streamed table's HEAD without bypassing lifecycle
authority. All lifecycle and maintenance entry points remain crate-private,
doc-hidden physical seams. The separate EnsureIndices and Optimize bridges
already require the retained checked serving-runtime authority; F7 exposes
them through the served policy/API layer. A same-schema physical rebind is the
separate offline case: only terminal profile `DISABLED` plus
`CheckedClusterMaintenanceAuthority` may move an exact `SEALED` lane into a
fresh binding scope. Productive SchemaApply is not an EXP maintenance writer,
even after disable. A schema change instead uses a checked sealed/retired
export, initializes a fresh graph with the desired schema, and loads the
artifact there; it never applies over or imports back into the enrolled
source.

**Implemented F3a:** internal schema v13 and recovery-v15 cover the
crate-private `SEALED → OPEN` resume and guarded `DRAINING → OPEN` abort,
including receipt-first idempotency, the recovery-owned physical claim,
terminal claim/management receipts, and bounded current-binding ancestry
validation. Public lifecycle surfaces remain absent. **F3b/F3c status:** the
same-binding maintenance shapes are implemented separately for EnsureIndices
and Optimize. **F3d status:** recovery-v18 separately implements fresh physical
rebind for an exact `SEALED` lane; it was not smuggled through resume,
recovery-v16, recovery-v17, or the frozen v14 scaffold. **F3e/F3f status:**
recovery-v19 implements stopped/offline authority retirement, and recovery-v20
implements stopped/offline exact DataBlock show/correct. Public/production
rebind, the `AuthorityBlock` half of item 6, and the broader served surfaces
remain future work.

### 4.2 What must be built

1. **`SidecarKind::StreamResume`** + its roll-forward-only payload in
   recovery-v15. Recovery-v13 remains
   `StreamProfileChange`-specific and schema-v12 `protocol_v12` remains
   `StreamFold`-specific. `Armed` binds the
   complete expected row and revision, `resume_id`,
   request digest, binding, configuration, base witness, graph-branch
   topology, fixed actor/operation, an `OPEN` plan, and a **minimum next epoch
   floor**. The current hidden resume seam first acquires the graph-profile
   gate shared, binds the exact `ENABLED` profile and delegation, and retains
   that guard through claim, terminal-ledger confirmation, and manifest
   publication. F6b8 also transfers the root MemWAL producer permit into the
   detached install owner and arms an urgent driver trigger before transfer.
   Under the exclusive root fence, the finite round snapshots exact empty
   owners and retires them under lane-exclusive authority in a housekeeping
   prepass before the unchanged node-before-edge candidate order, so a resume
   owner cannot occupy the sole root slot until the ordinary idle timeout. F7's production
   wrapper must still require matching checked serving-runtime authority before
   invoking it.
2. **Two-phase epoch claim.** The achieved epoch is unknowable before the
   claim, so: claim under closed admission → durably record the exact
   sentinel/epoch plus one terminal `ClaimReceipt` + `ManagementReceipt`
   transaction, achieved shard manifest/replay cursor, and final `OPEN` row →
   **only that row may publish.**
3. **`SEALED → OPEN`** consuming the sealed proof (`sealed_proof = None`),
   advancing epoch floors, and publishing the terminal claim and management
   ledger rows with their bounded hot pointers.
4. **`DRAINING → OPEN`** abort, which accepts only `DRAINING` and additionally
   requires: no guarded operation began, binding and the complete current row
   still match, every background seal/abort owner settled, and **no unmerged
   or strict-blocked cut remains**.
5. **The `SEALED` maintenance bridge (F3b; EnsureIndices slice active).** Keep
   `ensure_stream_effects_allowed` closed by default. Integrate each sanctioned
   writer explicitly:
   - a same-binding HEAD mover extends its existing recovery sidecar with the
     complete prior `SEALED` row/proof and a bounded allowed effect. Exact-
     transaction writers may pre-mint the terminal row. For operations such as
     `compact_files` whose achieved HEAD is not caller-minted, `Armed` binds the
     allowed effect and `EffectsConfirmed` records the exactly classified
     achieved HEAD. Only then is `verified_empty_digest` recomputed over that
     witness while preserving the authenticated shard cut and current claim
     receipt, and only that row may publish;
   - a physical rebind under the same accepted schema leaves the old lifecycle
     `SEALED` and must complete recovery-covered `stream_rebind` with a fresh
     enrollment and shard namespace. Rebind publishes a new exact
     **`SEALED`** binding and proof; a separate `StreamResume` claims a higher
     epoch in that new scope before `OPEN`. Productive SchemaApply is not
     authorized by this shape; schema evolution uses checked export/rebuild
     into a fresh graph instead; and
   - native branch-ref controls remain the existing exception, but any named
     graph branch keeps resume safely `SEALED`.

   Internal schema v14 and recovery-v16 implement the exact-transaction case
   for EnsureIndices. Its doc-hidden entry point requires `stream_manage`, an
   actor, the retained exact `CheckedClusterStreamRuntimeAuthority`, canonical
   main, and exact `SEALED` state for every enrolled productive table. It takes
   sorted exclusive admission for every productive table, layers complete
   prior/next lifecycle rows over the existing recovery-v8 CreateIndex plan,
   re-proves the selected ClaimReceipt from the captured token authority, and
   publishes every index pointer, `CurrentHeadWitness`, proof digest, and
   lifecycle revision in one graph-manifest CAS. The operation creates no
   token-ledger row, does not advance token authority or a management-receipt
   chain, and accepts no caller operation ID. Existing recovery ownership plus
   EnsureIndices' convergent planning supplies retry idempotency: a residual
   sidecar settles before replanning, and a true no-work invocation records no
   sidecar, lineage, or lifecycle successor. Ambient/direct EnsureIndices keeps
   the generic lifecycle refusal.

   Internal schema v15 and recovery-v17 implement the separate
   non-caller-minted Optimize case under the same checked-runtime,
   canonical-main, exact-`SEALED`, sorted-exclusive-admission boundary. The
   recovery payload records each confirmed Optimize output and its exact
   achieved HEAD; only that complete set may refresh productive pointers,
   lifecycle HEAD witnesses, proofs, and revisions in the manifest CAS. A
   true no-work retry remains effect-free, and ambient/direct Optimize keeps
   the generic lifecycle refusal.

   Internal schema v16 and recovery-v18 implement the separate physical-rebind
   case. The private terminal-`DISABLED`, checked stopped/offline maintenance,
   canonical-main owner consumes the complete prior `SEALED` authority,
   creates a fresh enrollment and empty shard namespace, appends immutable
   binding and fence-only claim receipts, and publishes the fresh binding with
   one exact next `SEALED` proof. It
   retains prior binding/claim history, admits no writer or put, and requires a
   separate recovery-v15 resume to open the fresh scope. The v14 three-field
   scaffold keeps its old bytes and remains refused. Hot open/admission proves
   current authority with O(1) selected `BindingReceipt` + `ClaimReceipt`
   point lookups and one fixed-size cumulative shard-set commitment; only the
   terminal-`DISABLED` offline rebind owner walks the bounded binding history
   to prove fresh identifier disjointness.

   The hidden served same-binding matrix now covers only content-preserving
   Optimize and EnsureIndices. Productive SchemaApply on a graph with any
   enrollment remains refused in every lifecycle/profile state; terminal
   `DISABLED` does not turn it into a maintenance writer, and the physical
   rebind shape above does not change accepted schema. Schema evolution uses a
   checked sealed or retired export, fresh graph initialization with the
   desired schema, and ordinary load into that fresh target.
   For a multi-table operation, acquire all
   affected stream-admission leases exclusively in sorted table-identity order
   as the outermost gates, retain them through effects and recovery, and
   publish every table pointer plus lifecycle/proof update in the operation's
   one graph-manifest CAS. Never publish lifecycle per table. Mutation/Load and
   BranchMerge remain refused: they change logical contents, and a witness
   rewrite alone would leave `_stream_tokens` authority stale. Enabling any of
   them requires a separate exact token-aware direct-write/merge sequencing
   contract. Cleanup, drift repair/adoption, force-repair, drop/re-add, and
   incompatible rematerialization also remain refused unless their dedicated
   retention/adoption/rebind proof is implemented; the same-binding bridge
   does not authorize content writes, deletion, or adoption. No generic
   `allow_sealed=true` bypass is permitted. Automatic operation-scoped drain
   remains Phase D. F3b/F3c prove the private
   `quiesce → checked-runtime EnsureIndices → resume` and
   `quiesce → checked-runtime Optimize → resume` compositions; the served
   surface, terminal-disabled physical-rebind handoff, and fresh-target schema
   rebuild workflow remain later slices.
6. **A representable, non-circular structural-block exit.** Internal v11 makes
   `StrictBlock` a tagged authority:

   - `DataBlock` retains today's authenticated generation cut plus canonical
     validator correction view and continues through RFC-026 §4.4
     `StreamCorrection`. The current shape requires validator contract v1, a
     selected claim-chain head, a replay cursor equal to the authenticated WAL
     tail, and a writer epoch equal to both the shard floor and active drain
     target. Manifest decode checks those relations so an exact retry may
     return the durable token without reopening the WAL. Validator contract v1
     streams validation violations directly into a bounded evidence collector
     while `DRAINING`, without first retaining a global violation vector. It
     caps the detailed canonical-JSON correction view at 8,192 deduplicated
     entries and 32 MiB of complete canonical records. Overflow is itself a
     deterministic terminal, not an error: `CORRECTION_VIEW_OVERFLOW` commits
     one projection entry per exact current-winner `(logical key, token)`, with
     a stable item ID and `REPLACE` as its sole action. Entries are ordered by
     raw UTF-8 logical key and then token bytes and hashed incrementally with
     domain-separated length framing, including the table key once for the
     aggregate; no expanded JSON aggregate is retained. The projection is
     independently bounded by the 8,192-key acknowledged generation and the
     existing input/token byte envelopes, so adding schema constraints cannot
     strand a drain.
   - `AuthorityBlock` records a reason/failure phase, the complete expected
     lifecycle/binding/base/token/shard authority digest, a bounded canonical
     observed-authority classification, exact proof references (recovery
     operation and Lance transaction/version/content digests), an optional
     authenticated generation cut, allowed repair classes, and one
     reason-specific evidence digest. It does **not** pretend the data
     validator view proves a pre-cut binding or witness failure. This tag and
     its repair owner remain reserved/inactive in v19.

   The current v19 binary still activates only `DataBlock` in the strict-block
   vocabulary. The state-lock-held, stopped/offline
   cluster commands re-open the manifest-selected immutable generation under
   exclusive stream admission, re-run validator contract v1, and require exact
   count/digest equality before returning evidence or accepting a correction.
   `show` returns at most 256 entries and 256 MiB of complete serialized page
   data per page; its opaque cursor binds the block token, correction-view
   digest, lifecycle revision, and next ordinal.
   `correct` requires a caller-minted correction ID, exact expected revision,
   strictly ordered bounded action list, and optional engine-plan-digest
   equality assertion. It accepts `REPLACE` or `WITHDRAW` only for exact
   blocked winners, keeps unmentioned winners, validates the complete overlay
   before effect, and leaves the lane `DRAINING`.

   Recovery-v20 owns one pre-minted base transaction—including the marker-only
   all-`WITHDRAW` case—and one combined token-successor/correction-receipt/
   management-receipt transaction. Only their exact joint outcome publishes
   fold lineage, advances both pointers, clears that exact block, and records
   terminal sequencing authority for withdrawals. Receipt lookup precedes the
   now-cleared block/revision checks, so exact lost-result retry is durable.
   Neither this engine owner nor the cluster/CLI adapter activates production
   row admission, general lifecycle verbs, direct `--store`, remote SDK, HTTP,
   or OpenAPI surfaces.

   In the planned full contract, a fold may install a repairable
   `AuthorityBlock` only while exclusive admission is held and those exact
   facts can be authenticated and retained. The current v19 binary does not
   install or repair that reserved tag.
   If the cut or an authority fact needed by every safe repair is missing or
   ambiguous, it reports `RecoveryRequired` or loud storage corruption; it
   never emits an operator-repair token from incomplete evidence. Block
   inspection branches by tag: `DataBlock` rescans the immutable cut and reruns
   the validator, while `AuthorityBlock` re-resolves the exact proof references
   and requires byte-identical evidence digest. Either disagreement fails
   closed.

   A persistent eligible authority block still requires a new finalized
   `StreamAuthorityCorrection` recovery shape when the remaining F3 slice
   activates it; the registered recovery-v14 scaffold stays frozen and
   fail-closed. The operation is addressed by `block_token`, caller operation
   ID, expected revision, and a complete reason-gated repair-plan digest. The
   repair may adopt a binding/witness only from exact transaction and content
   proof, rebind by materializing a fresh base from the manifest-visible base
   plus every authenticated acknowledged row in the block, or replace affected
   current-token rows by exact expected-old/new authority. That strand supports
   only the `PRESENT | WITHDRAWN` token vocabulary; F5's new format/recovery
   strand must explicitly extend the block, repair, and receipt shapes before
   `DEAD_LETTERED` authority exists.

   The repair preserves logical rows, hidden attribution, write IDs, stream
   tokens, and all prior merged progress; it never uses ordinary export,
   prefix inference, wildcard adoption, or a latest-HEAD guess. If the repair
   materializes authenticated acknowledged rows into the fresh base, it also
   consumes that exact cut once: the terminal manifest CAS publishes fixed
   graph lineage/fold attribution, the exact generation merged marker, and
   matching token outcomes with the base pointer. It may not copy those rows
   while leaving the cut replayable, nor advance the cut outside the commit
   DAG. Contradictory keys require an explicit per-key
   `REPLACE | WITHDRAW` plan. `Armed` binds the
   exact inputs, target namespace, allowed bounded effect envelope,
   pre-minted transaction identities, and immutable terminal
   `ManagementReceipt(kind = AUTHORITY_CORRECTION)`, and, for token effects, a
   token-store `AuthorityCorrectionReceipt` before first effect. Outputs such
   as achieved Lance HEAD/fragments that cannot be pre-minted are never guessed
   or serialized as row payloads in `Armed`: after the exact owned effects,
   `EffectsConfirmed` binds their achieved ref/HEAD/content digests and the
   complete manifest delta. Only that confirmed state may roll forward. One
   graph-manifest CAS then publishes repaired base/token pointers, the exact
   next `DRAINING` row, and the terminal management receipt while clearing the
   matching block. A retry therefore looks up that receipt, then the exact
   sidecar, before checking the now-absent block or expected revision; exact
   replay returns the recorded result and a changed digest conflicts. Old
   objects remain retained. Successful full revalidation lets the same drain
   retry and reach `SEALED`; missing or unauthenticated acknowledged bytes
   remain loud unrecoverable storage corruption rather than data loss.
7. **The post-`SEALED` rebuild preflight and same-format retirement.** This is
   distinct from the in-`DRAINING` structural repair above. A normal preflight
   freshly proves every block cleared, the exact empty proof, token/base parity,
   and no current non-`PRESENT` token. If current `WITHDRAWN` or
   `DEAD_LETTERED` authority remains, ordinary export stays
   `StreamExportBlocked`; an ordinary successor can restore `PRESENT` but is
   not an absence-preserving export fix.

   The current v19 binary offers the offline cluster CLI
   `cluster stream retire-for-rebuild plan --graph <id>
   --confirm-stream-offline`, followed by `confirm --graph <id>
   --retirement-id <uuid> --expected-plan-digest <sha256>
   --confirm-stream-offline`, under the stopped-writer, cluster-state-locked
   owner. It has no HTTP, direct-`--store`, or serving-runtime equivalent.
   Planning remains callable for an enrolled ordinary `DISABLED` graph whose
   streaming declaration was already unmanaged; checked cluster graph/store
   mapping and `stream_manage` remain mandatory.

   The read-only plan requires at least one current `WITHDRAWN` or
   `DEAD_LETTERED` token and
   returns one canonical `plan_digest` over root/internal format, manifest and
   the complete live branch-head map, source profile revision, every
   binding/lifecycle/`SEALED` proof, all relevant recovery, exact PRESENT/base
   parity, and the exact immutable manifest-selected `_stream_tokens`
   `CurrentHeadWitness` (main version plus transaction UUID; e-tag `None`). It
   streams current dispositions and counts in bounded batches; it never
   materializes, sorts, or hashes an unbounded terminal-token vector or ledger
   history. Zero terminal authority uses ordinary export.

   Confirmation derives `operation_request_digest = H(protocol version,
   plan_digest, authenticated actor, confirmation intent)`, with the non-nil
   retirement ID as occurrence key. It arms that exact tuple and revalidates
   the same token-authority witness and every other plan input; any movement
   refuses effect-free `StreamRetirementPlanChanged`.

   Current recovery-v21 `StreamAuthorityRetirementV2` pre-mints the immutable
   ledger receipt transaction and exact receipt-bearing output token version.
   It extends recovery-v19's frozen two-disposition owner with exact
   `PRESENT | WITHDRAWN | DEAD_LETTERED` counts and the selected token cut. Its sole
   manifest CAS revalidates the pre-retirement witness, selects that output
   pointer, advances the profile revision and receipt-chain commitment, and
   publishes `DISABLED → RETIRED`. This control-only CAS appends no ordinary
   graph-lineage commit and moves no live branch head; the immutable retirement
   receipt/profile chain is its audit record. That is a deliberate exception
   to ordinary enable/disable lineage, so the pre-retirement logical cut
   remains the post-CAS export cut. Its idempotency occurrence is root-wide
   `(graph identity, AUTHORITY_RETIREMENT, retirement_id)`, not lifecycle- or
   stream-incarnation-scoped, and receipt lookup precedes current-state checks.
   Only exact recovery of that retirement sidecar may finish the transition.
   The receipt stores both digests, actor, source profile revision, exact
   pre-retirement witness digest, bounded disposition counts, and
   `export_cut_digest`. That cut digest covers the pre-retirement logical
   projection—accepted catalog plus the complete live branch-head map and every
   table witness reachable from it—and excludes the receipt-only token/profile
   pointer advance. Each later branch export is permitted only for a member of
   that frozen map. Its JSONL provenance pairs the unchanged root receipt with
   a closed `branch_member` witness containing the canonical branch name, exact
   Lance branch identifier, graph head, manifest version,
   `table_witness_digest`, and a recomputable `branch_member_digest`. The same
   row carries `source_schema_ir_hash`, the exact
   `ordered_branch_member_digests`, and `selected_member_index`. A fresh loader
   recomputes the selected member digest and its exact slot, then recomputes the
   receipt's `export_cut_digest` from the source schema hash and ordered member
   digests. The source schema hash is proof input for the retired source cut;
   it is not required to equal the fresh target graph identity, whose schema
   compatibility remains ordinary loader validation.

   Once selected, Mutation/Load/delete and `_as`, SchemaApply, BranchMerge,
   branch create/delete and every profile transition/refinement,
   Optimize/EnsureIndices/Repair/Cleanup, and every other graph, schema,
   branch-ref, profile, lifecycle, recovery, maintenance, writer-claim/fold,
   correction, enrollment, rebind, and content writer refuses before body
   admission or effect with
   `StreamAuthorityRetired { retirement_id, export_cut_digest }`; reads and
   repeated export of the recorded immutable cut remain available. Export
   verifies `RETIRED`, the exact selected receipt/profile-chain/logical-cut
   match, and the receipt-bearing token pointer without reapplying the ordinary
   terminal-token rejection. It emits one
   `_omnigraph_export_provenance` row before the logical rows containing the
   root receipt and the selected branch's closed, cut-membership-proved
   `branch_member` witness.
   The receipt is provenance, not live token authority: init/load creates a
   fresh graph identity and any later enrollment creates a fresh stream
   incarnation, so a delayed old-incarnation request remains effect-free
   `StreamBindingChanged`. Authority retirement never deletes, ages out,
   rewrites, or
   pretends a `WITHDRAWN` or `DEAD_LETTERED` token is `PRESENT`.

### 4.3 Contract points

- **Recovery never compensates an epoch or fence sentinel.** While admission
  stays closed it may claim a *still-higher* epoch and record a new exact
  confirmation. A byte-identical already-visible `OPEN` row finalizes the
  sidecar; anything divergent fails closed.
- **`ClaimReceipt` epochs are strictly increasing within one physical binding
  scope, not across rebinds.** V11 receipts carry the exact enrollment, shard,
  and binding-scope identity. The current receipt must be the greatest epoch
  in the lifecycle's current scope; old-scope receipts remain immutable tagged
  ledger history but cannot authenticate its sentinels or sealed proof. Rebind creates
  a fresh scope, publishes a newly proved `SEALED` row with its scoped initial
  **fence-only** claim receipt without admitting a writer or put, and a later
  resume must exceed that new receipt. The immutable
  original `EnrollmentReceipt` becomes provenance: v11 derives current
  enrollment/binding/shards from `current_binding_receipt_id`. Its immutable
  ledger chain starts with a mirror of the original enrollment and advances
  only in the rebind CAS; the lifecycle row stores no binding-history vector.
- **Abort is not a skip-invalid escape.** With residue or a strict block, the
  direct forward paths are fix + exact same-drain retry, exact data correction,
  or `StreamAuthorityCorrection`. The in-progress quiesce gets no success
  receipt until it reaches `SEALED`; ordinary rebuild preflight becomes
  available only after the block is cleared and that proof exists.
- **A named branch created after quiesce leaves resume safely `SEALED`.**
- **Disable follows an offline ownership handoff.** `DISABLING`/`DISABLED`
  refuse ordinary resume and abort before a claim, but the process-local gate
  is not credited with arbitrating a server/apply race. Graceful server
  shutdown settles every resume/abort owner before offline apply begins.
  `DisableDrainAdoption` is the sole no-reopen route from an existing
  `OPEN_AFTER_FOLD` drain to the disable plan's `SEALED` target.
- **`SEALED` is authority, not permission by itself.** A maintenance writer
  proceeds only through its explicit lifecycle-aware recovery shape. The
  capability-only EnsureIndices bridge records graph lineage and the ordinary
  recovery audit; it does not invent a token-ledger management receipt.

### 4.4 Evidence

F3e co-landed the focused authority-retirement proof from §7.2 with the upgrade,
cluster, CLI, error, and release-note contract for irreversible plan/confirm,
read/export-only source boot, and fresh-root rebuild. F3f then made
`WITHDRAW` reachable with recovery-v20 and co-landed exact DataBlock
reconstruction/pagination, replacement, marker-only withdrawal, receipt-first
replay, all two-participant crash cells, stopped/offline cluster/CLI preflight,
and the genuine v17↔v18 rebuild/refusal seam. F5b adds focused mixed and
all-diverted terminal-fold, exact-retry/ordinary-successor, object binding,
selected-token list/export, and three-disposition retirement evidence. The
genuine v18↔v19 adjacent-binary cell also pins both refusals, ordinary rebuild
fidelity, and final-v18 retirement receipt-v1 import without authority transfer.
The broader lifecycle/dead-letter/retirement composition and measurement matrix
remains an F6 integration gate. `AuthorityBlock` repair is not implied by the
DataBlock or dead-letter evidence.

The pinned matrix cell from [testing.md](testing.md) closes here:
*`quiesce → create named branch → resume` — bounded resume must recheck branch
topology under the closed gates and remain `SEALED`, while a compatible
main-only resume advances the epoch and opens.*
`db/omnigraph.rs::native_branch_controls_refuse_open_stream_and_allow_sealed`
is the half-built stand-in to extend.

The first EnsureIndices evidence pins a productive authorized `SEALED` effect,
its atomic pointer/witness/proof/revision refresh, true no-work, ambient
refusal, `quiesce → EnsureIndices → resume`, and cold roll-forward of an exact
confirmed v16 sidecar. Recovery-v8's existing matrix continues to own the
underlying CreateIndex physical boundaries and compensation classifier. When
that classifier restores a table, v16 recovery derives an exact new `SEALED`
successor for the Restore HEAD; it never republishes the stale pre-effect HEAD
as current authority. F3b-specific mixed ordinary/enrolled pins,
OPEN/DRAINING, named-branch, stale-proof, mismatched-runtime, and remaining
crash-boundary cells are still evidence requirements, not claims of this
slice. F3c evidence pins authorized `SEALED` Optimize, achieved-HEAD recovery,
true no-work, ambient refusal, and `quiesce → optimize → resume`. The later
matrix must still cover physical rebind, including
`disable to terminal DISABLED → physical rebind (still SEALED) → enable →
server restart → explicit resume`, old-binding receipt retention plus
new-scope epoch restart, and a blocked-cut correction or
authority-repair → lost response → receipt replay → retry → sealed path.
Schema changes use a separate checked-export → fresh-init-with-new-schema →
load matrix; no enrolled source graph is changed in place.
Add shutdown/resume handoff tests at every claim/confirmation/publication
boundary: graceful shutdown cannot complete until the in-process resume owner
settles, and offline disable cannot begin until that process exits. Race
prepare, put, and resume against transport close, invoked-tail settlement,
process exit, the first disable CAS, and terminal disable; each operation is
either joined and then drained by the persisted disable plan or refused before
effect. Include an existing blocked `OPEN_AFTER_FOLD` drain, lost adoption
response, correction that preserves the override, and final `SEALED`.

The production ownership matrix is part of the proof: same-binding
EnsureIndices and Optimize already require the serving process's retained
runtime capability. F7 exposes both only after their transport and policy
surfaces co-land.
Physical rebind does **not** rely on an operator-timed quiesce/shutdown gap.
The operator stops the server and runs the ordinary offline disable apply to
terminal `DISABLED`; its durable plan captures and drains any
prepare/put/resume that won before transport closed. Only then may a
cluster-state-locked `CheckedClusterMaintenanceAuthority` session with
`--confirm-stream-offline` bind that exact disabled profile revision and run
the same-schema physical rebind. It uses the same externally enforced
single-writer handoff and, before rebind capture or effect, the same
graph-global recovery barrier plus root-order reacquire. Re-enable is a later
offline apply; it exits before server restart, and rebound lanes remain
`SEALED` until explicit resume. Productive SchemaApply remains refused on an
enrolled graph; changing schema requires checked export/rebuild into a fresh
graph. The confirmation is not described as a distributed fence. Raw direct
`--store` maintenance on a stream lifecycle remains refused.

---

## 5. F4 — hidden ingest vertical slice

### 5.1 Goal

Exercise the complete caller-shaped ingest path through a crate-private,
feature-gated seam. It may produce real durable acknowledgements in tests, but
no stable SDK, server, CLI, or OpenAPI entry point exists until F7.

### 5.2 What must be built

1. **The hidden authorized engine seam** — resolves the actor, enforces the
   `stream_ingest` Cedar action, requires a
   `CheckedClusterStreamRuntimeAuthority` owned by the graph-scoped supervisor,
   acquires root-wide transport admission before accepting body ownership,
   charges per-actor admission, and hands off to the private core. The test
   feature may mint that authority; an ambient `Omnigraph` handle may not.
   Keep the physical method crate-private/doc-hidden when F7 exposes only the
   served/remote surface.
2. **The streaming envelope and normalizer** — parse `$stream` and require the
   exact stream incarnation, non-nil caller-owned `write_id`, canonical
   predecessor token, and explicit non-null logical `id`. Never reuse the
   loader's random-ID fallback. Reject body-supplied contributor/origin and
   every reserved physical field. JSON/NDJSON becomes dense Arrow only after
   required/default, type/coercion, enum/range/check, vector-dimension, and
   schema validation. Compute the exact post-tombstone/hidden-metadata
   dense-slice charge before recovery or Lance.

   **Implemented hidden sub-slice:** the crate-private, feature-gated request
   seam performs policy and checked-runtime authorization, then acquires
   separate root-wide and per-actor transport admission before polling body
   chunks. It rejects a transport chunk over 32 MiB before framing, then lazily
   emits at most one completed line at a time instead of expanding one chunk
   into a queue. It incrementally frames bounded NDJSON without collecting the
   request, drops an over-limit line while counting through its delimiter, and
   resumes at the next caller ordinal. Each retained line requires the exact
   `$stream` shape and an explicit canonical `id`, rejects duplicate, unknown,
   and reserved fields, and uses fresh accepted schema authority to convert
   node/edge scalar, list, enum, and caller-supplied vector values into dense
   B2 input. Raw bytes, pre-DOM structural slots, projected aggregate Arrow
   allocation, normalized runs, and ordered results are bounded before their
   respective allocation or ownership boundaries.

   Terminal result consumption drains every already-buffered per-line outcome,
   then joins the root request task before reporting clean EOF. A task panic or
   abort is therefore a loud request failure rather than a truncated successful
   stream. Cancelling that terminal receive retains join ownership for the next
   receive or explicit cancellation path.

   The seam forms contiguous multi-row physical prefixes containing distinct
   keys and closes a prefix at invalid input, a repeated key, a current-token
   disposition, or the row/byte ceiling. Authority classification is windowed
   at 256 rows. Exact token-prefix selection checks the full window first and
   then scans downward when necessary: adding a distinct-key successor may
   replace a larger current winner, so token projection size is not monotonic
   and cannot be binary-searched. One watcher/fence outcome covers each invoked
   prefix, while a bounded reorder owner maps tagged per-line outcomes back
   into caller order. A physical-admission blocker supplies
   `blocking_ordinal` to later uninvoked lines, but adapter-local `invalid` and
   `stream_input_too_large` results still take precedence. Intrinsic sizing for
   that stopped tail temporarily owns the same root B2 preprocessing permit as
   ordinary admission; waiting for it races output cancellation and cannot
   escape the two-envelope root budget. After invocation, ownership transfers
   to the root task: disconnect stops further body polling and admission but
   cannot cancel or erase the bounded invoked tail.
   Blob-bearing tables still fail before any MemWAL put because Lance's LSM
   fold scanner cannot yet materialize their logical Blob values.

   This is not a public transport or product surface: SDK, HTTP, CLI, API DTO,
   and OpenAPI ingress remain absent. F3a now supplies the separate private
   resume/abort owner and bounded current-binding ancestry validation. The
   checked-runtime EnsureIndices and Optimize bridges and private recovery-v18
   physical-rebind owner are active, while public/production rebind remains
   absent. This completes the deliberately hidden F4 milestone; it does not
   activate the later served SDK/HTTP/CLI/OpenAPI product surface.
3. **Lazy enrollment with a prepare handshake** (§4.7 P2) — every table is
   stream-eligible only while the profile is exactly `ENABLED`, but the wire
   invariant above still requires the engine-minted stream incarnation before
   any row body. F4 therefore adds a bodyless, retry-safe
   `prepare_stream_ingest_as` seam rather than inventing a first-row exception:

   - status for an absent lane returns no stream incarnation, but does return a
     canonical `StreamEligibilityWitness` over graph identity, stable table and
     table-incarnation IDs, accepted-catalog digest, profile revision, and fold
     delegation ID, plus the exact canonical-main table/ref
     `CurrentHeadWitness` and lifecycle-slot-absent compare evidence. An
     ingest-only actor need not also hold status permission. Prepare checks
     retained receipt/current lifecycle authority first: under exact `ENABLED`,
     an existing `OPEN | DRAINING | SEALED` lane returns bounded
     `already_enrolled` with current stream incarnation, binding digest,
     lifecycle, and revision even when no witness was supplied. Only an absent
     eligible lane with no/stale witness returns effect-free
     `witness_required` plus the current bounded witness under `stream_ingest`;
     neither response records a new operation occurrence;
   - prepare requires a caller-minted non-nil `enrollment_request_id`; the
     witness is optional only for an effect-free challenge and mandatory before
     arming. It enforces `stream_ingest`, validates the checked cluster runtime,
     and reuses the existing physical enrollment mechanics before any NDJSON
     body is opened, but arms only recovery-v14
     `StreamEnrollmentV2`; it never
     serializes the fuller intent beneath historical `protocol_v10`.
     It byte-revalidates the complete witness under the adapter's ReadSet.
     Concurrent lifecycle creation restarts at the current-authority branch and
     returns `already_enrolled`. `DISABLED`, `DISABLING`, a changed
     graph/table incarnation, or a no-longer-eligible table returns a typed
     profile/eligibility refusal with no witness. Only a still-`ENABLED`,
     still-absent lane whose HEAD/ref/catalog/profile witness moved returns
     effect-free `witness_required` with a fresh witness and does not arm;
   - for an absent lane, the adapter fixes the request/witness intent,
     authenticated actor, and engine-minted stream incarnation/binding in
     recovery before its first effect, then returns a durable
     `EnrollmentReceiptV2` carrying that actor. The client may reuse the
     request ID after an effect-free witness challenge. Once a participant
     effect makes the receipt durable, the same ID, actor, and intent after a
     lost response returns that receipt; another
     actor or intent conflicts. Durable intent covers the graph/table lifetime,
     accepted schema, original table HEAD, and fixed stream configuration;
     profile revision and fold delegation remain pre-arm freshness evidence
     because the receipt does not persist them. Concurrent prepare IDs resolve
     through one lifecycle CAS, and
     losers return `already_enrolled` only after revalidating the winner's
     complete receipt and current binding. A successful prepare followed by no
     body intentionally leaves an empty enrolled `OPEN` lane, owned by F2's
     empty quiesce/disable path. A sidecar crash that recovery proves had zero
     participant effects may retire and re-arm the same request with new
     engine-minted result IDs; no receipt or acknowledgement existed at that
     boundary; and
   - only a later ingest request carrying that exact stream incarnation on
     every row may own/read the NDJSON body. An absent lane returns request-
     level `StreamPrepareRequired` before body ownership. The remote
     `GraphClient` and CLI use a cached/status witness when available or perform
     witness challenge → prepare → ingest automatically, and cache only the
     witnessed incarnation. One `stream ingest` call follows at most one fresh
     witness challenge within its bounded request deadline; another movement
     returns typed `StreamAuthorityChanged`/retry guidance before body
     ownership rather than polling. There is no manual per-table opt-in/enroll
     command. Raw HTTP exposes the prepare exchange explicitly. A client never replaces
     an explicitly supplied stale stream incarnation or automatically
     reprepares/replays that body after `StreamBindingChanged`; crossing a
     rebuilt/re-enrolled authority requires a caller-owned new occurrence and
     predecessor decision.

   **Implemented sub-slice:** the feature-gated bodyless prepare seam now
   enforces `stream_ingest` and exact checked-runtime authority, accepts a
   canonical caller-owned UUID-v4 request ID, and returns an effect-free
   table-specific eligibility witness before it can mint plan IDs or arm
   recovery. An exact echo reuses recovery-v14 enrollment; durable same-request
   retries return the actor-bound receipt, while concurrent request IDs
   serialize at the existing admission/gate envelope and converge on one
   validated lane. Blob-bearing tables are refused before enrollment. The
   witness is ephemeral, no manifest/recovery grammar changed, and the only
   externally reachable adapter remains the feature-gated test seam. This
   slice originally failed closed unless the current binding was still the
   initial binding. F3a replaced that shortcut with bounded current-binding
   chain validation, and F3d now owns the only private physical-binding mint
   through recovery-v18's exact rebind effect and receipt shape.

   This activates the actor-bound `EnrollmentReceiptV2` and
   `StreamEnrollmentV2` selected by the implemented hidden
   v12/lifecycle-v3/recovery-v14 tranche; neither exists beneath
   v11/recovery-v13. F4 exposes that existing authority through prepare and
   does not mint a second enrollment shape or another ordinary format strand.

   Enrollment, which is not a resident-producing row put, acquires
   graph-profile shared before the table lease. Under the same exclusive table
   admission lease and existing schema/main/token/table gates, it reruns the
   recovery barrier and rereads canonical-main `stream_profile`; the enabled
   revision/delegation and eligibility witness must match the checked runtime.
   Resident-producing ordinary admission retains bounded preprocessing/
   inflight ownership, then acquires the shared root MemWAL opportunity,
   graph-profile guard, and table admission before handing off a run. It keeps
   those guards through `put_no_wait`, watcher durability, and the same-writer
   fence result; ownership transfers with the invoked tail if the request
   disconnects. The offline disable owner takes its own process's gate
   exclusively for `ENABLED → DISABLING` and releases it before any per-table
   drain. Unit tests cover that in-process order, while production
   disable-versus-first-write is closed by server-exit/apply-start handoff.
4. **Per-line response mapping** — the §4.6 response union
   (`durable`, `ack_unknown`, `already_durable`, `invalid`,
   `stream_resume_required`, `stream_fold_required`, `stream_backpressure`,
   `recovery_required`, `stream_retry_required`, …), emitted in caller order,
   with the reorder buffer and contiguous-run rules. F5 extends the exact
   tagged union with `dead_lettered` rather than returning an ad hoc error.
5. **Bounded transport ownership.** Parse incrementally; never collect the
   request. One request may retain at most one submitted run and one
   accumulating run, each within 8,192 rows / 32 MiB. A transport chunk over
   32 MiB is refused before framing, and a raw line over the selected 32-MiB
   line ceiling is terminal before materialization. Completed lines are
   yielded lazily from each accepted chunk, never collected into a parallel
   frame queue. The result channel and reorder buffer hold at most one run
   (8,192 statuses); when the
   output consumer stalls, parsing and new admission backpressure rather than
   accumulating. A separate root-wide slot/byte budget charges every live raw
   accumulator, normalized run, result queue, and reorder owner, so many slow
   requests cannot multiply the per-request bound without limit. These public
   buffers are additional to, and do not weaken, the root's two 128-MiB B2
   preprocessing envelopes. F6 measures parser expansion/RSS and records the
   exact transport defaults before F7 activation.

### 5.3 Contract points

- **Caller-supplied vectors** (§4.7 P3). A row for an `@embed` table must carry
  its vector; missing is effect-free per-line `invalid`. Dimensions are
  validated; model identity is a documented producer obligation. The ack path
  makes **no external calls** — computing embeddings inside fold is
  permanently rejected because it would destroy fold determinism.
- **Upsert-only.** Streamed deletes and direct deletes on an enrolled table are
  unsupported and remain Phase F because deletion needs token-aware
  sequencing. A direct delete is possible only before that table is enrolled.
- **One fresh occurrence per key per physical run.** A same-key successor,
  exact duplicate, or token disposition waits for the preceding run's watcher
  and is reclassified against the confirmed overlay before another Lance call.
- **Stop-tail rule.** After `AckUnknown` / capacity / lifecycle / authority /
  recovery blocks further physical admission, later uninvoked lines inherit the
  blocking status with `blocking_ordinal` — but adapter-local terminal results
  (`invalid`, `stream_input_too_large`) still take precedence because parsing,
  validation, normalization, and intrinsic sizing continue effect-free.
- **Partial success never becomes an HTTP error.** Once a line is emitted, the
  same condition is reported per-line, not by failing the request.
- **Cancellation transfers ownership; it does not erase ambiguity.** After
  `put_no_wait`, the root-owned task runs watcher + same-writer fence check to a
  terminal result even if the caller disconnects. A resolved durable result is
  not cancelled; an unsettled post-invocation result is `AckUnknown`. Once
  output is gone, stop reading/admitting new lines and settle the bounded
  invoked tail.

### 5.4 Evidence and slice boundary

Extend `memwal_stream.rs`, `memwal_stream_cost.rs`, policy tests, and
failpoints through the hidden seam. Cover raw-versus-normalized limits,
reserved fields, explicit IDs, stale eligibility witnesses, prepare
lost-response receipt replay, concurrent first prepares, no body ownership
before prepare, one followed challenge plus second-movement bounded refusal,
actor-bound retries, enable/disable versus lazy enrollment,
same-key run splitting, slow output, disconnect at every invocation/watcher
boundary, bounded reorder/backpressure, and every stop-tail precedence cell.

**Stopping after F4 is safe:** the seam remains inaccessible to production
callers, and the graph flag alone still acknowledges nothing.

**Stopping after F5b0 is also safe:** automatic folding and goal-`SEALED`
continuation are reachable only through checked resident/offline owners and
change no persisted grammar. Public transport and terminal dead-letter
authority remain closed.

---

## 6. F5 — fold driver and dead-letter

### 6.1 Fold driver

**F5a implemented (hidden, format-neutral):** the first scheduler tranche adds
orchestration only. It introduces no manifest field, token disposition,
sidecar discriminator, management receipt, or new fold idempotency domain.
Every automatic effect re-enters the existing recovery-v14 ordinary-fold
adapter, so an acknowledged cut has exactly the same recovery and publication
meaning as an explicit private fold.

- **Authority-bound automatic folds.** Starting the supervisor requires the
  exact checked served-runtime capability. Each attempt recaptures an
  `ENABLED` profile and its live `FoldDelegation`; a missing or mismatched
  runtime/profile refuses rather than silently running ambient background
  work. A fold already armed by recovery-v14 remains recovery-owned.
- **Bounded timer/cap scheduling.** The detached put owner creates or updates
  one root-scoped pending entry for its exact table identity immediately after
  physical put invocation. Caller cancellation, a lost response, or an
  eventual `AckUnknown` therefore cannot erase a possibly durable tail. The
  passive readiness probe removes a no-effect wake. The max-staleness timer
  makes an ordinary entry ready; generation-cap pressure shortens that same
  entry to immediate readiness. Repeated wakes coalesce and a wake that arrives
  during an attempt survives into the next finite round. There is no durable or
  unbounded job queue: the manifest and Lance MemWAL remain work authority.
- **Cold discovery and finite rounds.** Startup derives candidate `OPEN` lanes
  from the current manifest and uses the passive authenticated WAL cursor to
  distinguish real backlog from an idle lane. One round freezes the finite set
  of due table identities, refreshes authority before each attempt, and visits
  nodes before edges with a carried round-robin cursor within each immutable-
  identity cohort. Each captured identity gets at most one attempt; later work
  waits for the next round. This
  reduces avoidable RI conflicts but does not promise a graph-wide or
  same-window cut. Recovery is resolved before the round publishes, and crash
  restart derives progress from authoritative merged-generation state.
- **One process-local owner.** Independently opened handles for one graph root
  share one weakly root-scoped task and pending map. Retryable failures remain
  pending under bounded exponential backoff; a durable strict block is parked
  instead of hot-looped. The experimental topology still requires one
  externally enforced live writer process and makes no cross-process lease
  claim.
- **Graceful server ownership.** After the listener binds, the cluster server
  starts every selected graph supervisor and refuses startup if any start
  fails, cleaning up the already-started prefix. Graceful shutdown first takes
  each graph's root MemWAL opportunity exclusively, then its profile gate
  exclusively. It drops both fences before requesting/joining the driver,
  because the driver needs root-exclusive plus per-candidate profile shared to
  settle its final finite round. Supervisors are joined concurrently so the
  process pays one bounded deadline, not one deadline per graph; an early
  server error follows the same cleanup path.
  An invoked fold is never aborted out from under recovery-v14. Timeout is loud,
  retains the live task handle, and leaves any sidecar-owned effect recoverable.
  Public health/backlog status remains later work.

**F5b0 implemented (hidden, format-neutral):** the operational continuation
cut extends existing owners without adding a manifest field, token
disposition, sidecar discriminator, public method, or transport.

- **Resident goal-`SEALED` continuation.** Exact `ENABLED` startup and cold
  discovery now include unblocked `DRAINING` rows whose durable descriptor has
  `goal = SEALED`. Each attempt rechecks the same checked runtime and profile
  delegation as an automatic `OPEN` fold, then invokes the existing
  recovery-v14 quiesce adapter with the stored drain ID, expected revision,
  and actor. `OPEN_AFTER_FOLD`, blocked, and `SEALED` rows are not retargeted by
  this owner. A selected `DataBlock` is parked rather than hot-looped; exact
  correction makes the lane eligible again, and the next checked-runtime cold
  start rediscovers any still-unblocked goal-`SEALED` lane.
- **Offline disable continuation.** Checked `cluster apply` publishes the
  exact `ENABLED → DISABLING` plan before doing drain work. With admission
  durably closed, it derives one finite lane set from the accepted manifest,
  orders nodes before edges and immutable identities within each cohort, and
  owns one lane at a time. `OPEN` gets the deterministic disable drain;
  `DRAINING(goal = SEALED)` keeps its occurrence; and
  `DRAINING(goal = OPEN_AFTER_FOLD)` is narrowed once by deterministic
  `DisableDrainAdoption`, whose metadata CAS and immutable receipt use the
  existing recovery-v14 `StreamLifecycleReceipt` owner. `SEALED` is skipped.
  Once the finite cut is sealed, recovery-v13 publishes the existing terminal
  `DISABLING → DISABLED` receipt/CAS.
- **Loud park and retry.** A selected `DataBlock` returns its typed block token
  and leaves the exact `DISABLING` revision in both manifest truth and the
  cluster ledger; no later lane is attempted. The existing stopped/offline
  correction command clears only that exact block, and a rerun of apply
  reconstructs the same stored plan and deterministic lane order. It does not
  mint a second disable operation or relabel its actor.
- **No owner overlap.** A serving supervisor runs only at exact `ENABLED` with
  the checked runtime. Offline apply holds the stopped-writer cluster
  authority and runs only the persisted `DISABLING` continuation. The existing
  process-handoff contract, not a new distributed lease, separates them.

**Deferred beyond F5b:** operator-requested fold receipts,
dependency-level ordering, and a public health/backlog projection remain
inactive. No active path creates `AuthorityBlock`;
reason-gated repair stays fail-closed until such a producer and its finalized
evidence grammar exist. Section 6.2's terminal shape is implemented in the
separate v19/recovery-v21 strand.

Cadence is the visibility gap. Expect ~seconds under load; the contract says
"typically seconds, unbounded tail" and there is **no producer-facing flush**
in this profile (§4.7 P5).

### 6.2 Minimal terminal dead-letter authority (§4.7 P4)

**F5b implemented (hidden row path, narrow offline inspection):** internal
schema v19/token schema v3/recovery-v21 implements the following terminal
protocol. It adds no HTTP, SDK, remote CLI, or OpenAPI row surface; F6b owns the
remaining measurement and guardrail acceptance.

Fold-time outcomes remain split by semantics:

| Class | Examples | Disposition |
|---|---|---|
| **Dead-letter envelope overflow** | canonical terminal payload is above the selected one-object byte envelope after valid conflict evidence exists | Install a durable strict `DataBlock` before canonical-object creation, base-table effect, or current-token terminal-disposition transition; permit the manifest/token-ledger movement needed to persist the block; publish no partial fold |
| **Other structural fault** | corrupt/missing authenticated cut, schema or token-authority contradiction, malformed validation evidence | Fail loudly with no partial fold; the driver reports/retries according to the typed failure. `DataBlock` v1 cannot authenticate this evidence, and durable parking waits for a future `AuthorityBlock` strand |
| **Data conflict** | uniqueness, RI, cardinality, keyed row validation | Divert one terminal LWW candidate per losing key, apply independent winners, and keep the lane progressing |

Only the final LWW candidate for each losing key becomes a payload entry.
Superseded same-key occurrences remain authenticated by the WAL/token chain but
are not separately exported or replayed.

1. **One current terminal token per losing key.** The F5 format adds
   `DEAD_LETTERED` to the current-token disposition and a versioned terminal
   evidence shape containing the exact occurrence, predecessor, contributor
   and payload identity, reason code, object descriptor, and candidate ordinal.
   The visible base may remain absent or at its previous `PRESENT` token; it
   never pretends to match the terminal token. The manifest-selected token
   version remains the sole post-fold sequencing authority.

2. **One bounded object per fold.** The driver deterministically orders the
   terminal candidates and canonically encodes one NDJSON object under the
   reserved graph-relative prefix. The implementation pins a 67,108,864-byte
   encoded envelope. F6b4's production-size local evidence pins exact retained
   encoded capacity, encode/verify time, and isolated peak-RSS lift; exceeding
   the byte envelope installs `DataBlock` before any canonical object,
   base-table effect, or current-token terminal-disposition transition. There is
   no chunked fallback, chunk manifest,
   multipart protocol, or object-sized uncharged buffer.

3. **Recovery owns the object before PUT.** The F5 sidecar binds the
   authenticated generation cut, canonical candidate descriptors, versioned
   object path/digest/length/row count, exact base transaction (marker-only
   when all candidates divert), exact
   token transaction, and final manifest outcome before conditional create.
   `PutMode::Create` is accepted on `AlreadyExists` only after exact
   length/digest verification. A lost result remains recovery-owned. An object
   not selected by the terminal manifest CAS is inert retain-all residue and is
   never discovered through prefix listing.

4. **All-diverted folds are first-class.** A fold with no visible winner may
   still select the new token version, merged-generation progress, versioned
   fold attribution, and object reference in one manifest CAS. F5 uses a new
   attribution/recovery shape: the historical v10 nullable
   `dead_letter_object` placeholder remains explicit null under v12 and is
   never activated in place.

5. **Retry and correction use ordinary occurrence semantics.** While a
   `DEAD_LETTERED` token remains current, an exact retry returns the same
   terminal result without writing another object or moving authority. A
   correction is a fresh ordinary Admission with a new `write_id`, corrected
   payload, and predecessor equal to that terminal token. A successful fold
   makes its `PRESENT` token current. After that successor, retrying the old
   occurrence receives the normal current-authority conflict. There is no
   `Replay` origin, `StreamDeadLetterReplay` recovery family, replay
   checkpoint, or mutating replay endpoint.

6. **Current-token inspection, not a second inventory.** Cluster-only
   dead-letter list/export pins the manifest-selected token version, streams
   its one current row per graph key in bounded batches, filters
   `DEAD_LETTERED`, and groups object references/candidate ordinals for
   digest-verified payload output. It does not list object prefixes, walk graph
   history, or maintain a `DeadLetterRecord` chain. Physical scan work may
   grow with uncovered fragments/history; §3.3 and F6 own that explicit,
   instrumented EXP gap rather than adding hot disposition counters now.

7. **Retirement remains the same-format exit.** F5 extends cluster-only irreversible
   authority retirement to `WITHDRAWN | DEAD_LETTERED`. Planning pins the
   exact sealed root cut and selected token witness, streams current terminal
   rows in bounded batches, and records scan-derived disposition counts and an
   immutable plan digest. Confirmation is actor-bound, recovery-owned, and
   publishes `RETIRED` plus its receipt in one manifest CAS. It deletes no
   object and never relabels terminal authority as `PRESENT`.

8. **Operational surface stays narrow.** The primary workflows remain ingest,
   status, fold, quiesce, and resume. Automatic bodyless prepare is a protocol
   handshake. Dead-letter inspection/payload export, block correction,
   authority repair, and retirement remain cluster/offline operations for EXP;
   they do not receive served HTTP/OpenAPI parity. Every reachable
   `DataBlock`, `AuthorityBlock`, `WITHDRAWN`, or `DEAD_LETTERED` state
   nevertheless has one supported operator exit before public ingest activates.

F5 does not extend attribution merely because the object exists. It reuses the
trusted contributor/payload identity already required for sequencing and adds
only the object integrity fields needed by recovery and export. Additional
per-record principals, provenance chains, and public history are out of scope.

## 7. F6 — guardrails and acceptance evidence

### Implemented F6a subset

F6a is a deliberately in-process acceptance cut, not completion of F6. It adds
one typed, failpoints-only snapshot of the resident fold driver. The snapshot
reports process-local run state, pending trigger/backoff scheduling, and last
completion/error evidence as explicitly non-authoritative diagnostics. Pending
triggers are scheduling hints, not a durable WAL backlog, and a stopped driver
does not prove that checked stopped/offline authority is available. The public
durable `Omnigraph::stream_status` projection remains manifest-only.

One hidden candidate-runtime test now composes bodyless prepare, ordered NDJSON
admission, automatic mixed visible/dead-letter folding, stopped/offline
selected-token list/export, an ordinary corrected successor, driver restart,
clean shutdown ownership, and checked offline disable. It proves those already
implemented mechanisms work together without changing manifest, token, or
recovery grammar or adding an SDK, HTTP, CLI, or OpenAPI contract.

F6a itself does not prove OS-process forced termination, the full node+edge
fairness matrix, long-history token lookup, RSS/latency/object measurements,
or maintenance/rebind/resume composition. F6b2 later closes the named process,
fairness, and maintenance/rebind/resume cells; F6b3 closes the exact-selected
uncovered-tail current-token instrument. F6b4 separately closes the isolated
dead-letter envelope evidence and F6b5 closes bounded served export. F6b7 adds
the paired failpoints-only covered/reconciled decision instrument. Its
uncompacted-profile-cycle bounded NO-GO schedules no standalone production
reconciler. At that slice boundary, public operational-status transport and the
remaining guardrails still kept F6 open and F7 forbidden; F7a and F7b later
activated the proved graph row and graph-redacted status surfaces.

### Implemented F6b1 checked immutable export-cut subset

F6b1 implements the lower/control-authority and engine half of safe served
export. Cluster/server boot can mint a distinct non-cloneable
`CheckedClusterServedExportAuthority` from either one exact managed
`DISABLED | RETIRED` applied row or exact graph/state evidence whose engine
bind proves an unmanaged `RETIRED` or enrolled `DISABLED` profile. Retirement
confirmation CAS-converges a managed row to the exact `RETIRED` revision;
refresh preserves its declaration identity and treats that state as satisfying
`streaming: false`. The capability shares the process-local serving
registration with writer runtime authority but cannot authorize a writer,
fold delegation, supervisor, admission, or mutation. Ambient embedded/direct
export of an enrolled ordinary `DISABLED` graph returns
`StreamingRequiresClusterRuntime` before output. Because retirement is already
irreversible, F6b1 retained the existing receipt-verified `RETIRED`
direct/server export as the rebuild bridge; F6b5 now switches served transport
to the checked cut. Both retain the exclusive side of the same root gate
through output.

The doc-hidden capture seam is nonwaiting on one root-wide export gate. While
holding that gate exclusively it settles relevant recovery, takes the full profile/admission/schema/
branch/token/table gate envelope, validates terminal stream authority and the
requested filters, and freezes one accepted catalog, the selected branch
snapshot with exact Lance table versions, and any already-verified retired
provenance. It then drops every writer gate and returns one private-field,
non-cloneable `StreamExportCut`. The cut retains the checked served authority
and exclusive root gate through its consuming output operation, so a later writer may
proceed but cannot retarget the pinned bytes. Branch create/create-from/delete,
schema apply, cleanup, and supported whole-root deletion acquire the shared side
nonwaitingly, so they remain mutually concurrent while no selected path or exact
version can be removed or reused under a live cut. Terminal/refusal errors occur before the first
byte; a storage or writer error after output starts is preserved as that stream
error, and completion/drop/error releases the slot.

This slice changes no manifest, token, recovery, or storage format. F6b1 itself
adds no new public HTTP/SDK/remote-CLI/OpenAPI route, response contract, bounded
channel or queue-byte reservation, wait deadline, stall/disconnect handling,
measurement, or public status. F6b3 subsequently closed the exact-selected
uncovered-tail token instrument and F6b5 subsequently closed the bounded
transport. F6b7 subsequently added paired failpoints-only covered/reconciled
decision evidence without a production maintenance path; its uncompacted-
profile-cycle bounded NO-GO schedules no standalone reconciler. F7b later
activates the graph-redacted checked-status transport; the remaining
correctness/performance matrix stays in the F6b remainder/F7 boundary.

### Implemented F6b5 bounded served-export subset

F6b5 connects F6b1's move-only cut to the existing
`POST /graphs/{graph_id}/export` route. Authorization, a complete queue-envelope
reservation, recovery/profile/filter preflight, and exact cut capture all
finish before `200`. The same route therefore keeps ordinary pristine export,
accepts cluster-served exact terminal `DISABLED | RETIRED` authority, and
refuses enrolled ambient/direct export, nonterminal profiles, current terminal
tokens, a second graph cut, and transport saturation as ordinary typed JSON
before any NDJSON header or byte. Remote CLI export inherits the route without
a second buffering layer; OpenAPI pins the additional `409 | 413 | 503`
responses.

The engine no longer collects every table batch for export. Tables scan exact
pinned versions with an initial 8,192-row estimate and Lance's approximate
32-MiB decoded-byte target. Lance may emit a larger batch, so these scanner
settings are not described as allocator admission. Blob descriptor batches are
explicitly sliced to one logical row before its complete Blob-property set is
materialized; that set and the row's encoded JSON remain indivisible scratch.
Each JSON line is emitted as independently owned chunks no larger than 64 KiB.
The server owns a two-chunk Tokio queue and reserves 256 KiB per response for
the queue, producer-awaiting chunk, and consumer-current chunk. One production
server process reserves at most 2 MiB of queue envelopes (eight reservations)
and waits at most 250 ms. These are transport ownership bounds, not a cap on a
complete response, process RSS, Lance's scanner state, or one row's Blob/JSON
scratch.

The response body and producer jointly retain the byte permit. The producer
either keeps the
move-only cut in its in-flight future or places it in a terminal frame behind
all data frames. A stalled receiver therefore backpressures production; body
drop wakes cancellation and drops both cut and permit; completion and
post-header error also release them. A producer that disappears without a
terminal frame becomes a body error rather than a false clean EOF. This slice
changes no manifest, token, recovery, or storage grammar and itself activates
no row ingress, lifecycle, maintenance, or public status surface; F7b later
activates status without changing those grammars.

### Implemented F6b6 checked operational-status core

F6b6 adds one engine-internal `stream_operational_status` operation for the
mode-appropriate checked runtime, export, or apply owner. It is distinct from
the public
`Omnigraph::stream_status`: the public method remains a cheap, nonblocking
projection of one manifest snapshot. F7b now exposes only a graph-redacted
projection of the checked shape through HTTP/OpenAPI and remote CLI. The raw
cut and ambient SDK contract remain internal/manifest-only.

The checked operation first runs the expensive immutable work—token/base
parity, its bounded terminal sample, lookup-index coverage, and selected
lifecycle-ledger proofs—against exact manifest-selected versions under a
separate 60-second observation budget. It holds no writer gate during that
preflight and performs only one full current-token scan. Recovery inventory is
complete only within its hard advisory envelope: at most 256 matching direct
`.json` sidecars, 256 irrelevant direct-or-nested objects encountered below the
prefix, 4 MiB of cumulative input-anchored URI bytes across all encountered
objects, 32 MiB for any one sidecar body, and 32 MiB of cumulative bodies.
Crossing any bound returns a typed resource refusal before status can present a
partial inventory. It then
waits at most five seconds for the root fold round exclusively, profile shared, every
selected lane exclusively, then schema, canonical main branch, token, and
table gates. `ENABLED` requires the exact served-runtime authority; terminal
`DISABLED | RETIRED` requires the exact served-export authority. `DISABLING`
requires a distinct checked cluster-apply status authority rather than ambient
offline authority. The short cut proves the same manifest/recovery inventory
is still selected, reads the physical shard/generation and advisory process-
local driver state, then rereads only mutable physical state, recovery
inventory, and the canonical manifest before release. Unexplained movement
returns `StreamStatusChanged`; either bounded phase can return
`StreamStatusBusy`; neither result publishes or heals state.

Every pending recovery sidecar inside that accepted inventory appears in status
and blocks rebuild. When a sidecar exactly explains a moved physical HEAD, the affected physical
projection is explicitly unavailable while the recovery record remains
visible; unexplained movement still returns `StreamStatusChanged`. Status does
not hide recovery just because it can explain the physical witness.

Pending-generation rows/Arrow bytes/batches are exact only from resident
admit/fold accounting or a verified-empty `SEALED` proof. A missing resident
beside an active/replayable cold tail is
`UnavailableColdReplay`: read-only status does not advance a Lance cursor or
claim a writer merely to obtain a number. Flushed LWW projection accounting is
`UnavailableFlushed`. Token coverage reports exact
covered/uncovered fragments when Lance exposes it, but a nonempty uncovered
tail reports oldest age as unavailable because the selected cut contains no
exact fragment-creation timestamp. These are deliberate truth boundaries, not
temporary zeros or inferred timestamps. Driver scheduling remains explicitly
process-local and non-authoritative.

### Implemented F6b2 process/lifecycle acceptance subset

F6b2 reuses the server and hidden engine acceptance owners. Green cells cover
Unix `SIGTERM` reaching the same graceful-shutdown path as Ctrl-C;
sequential OS-process exit/reopen with persisted recovery; a frozen finite
driver round in which a newly ready node cannot overtake an edge already in the
round; and terminal disable → same-schema physical rebind → re-enable → reopen
→ explicit resume → exactly-once ingest/fold. The composed
`quiesce → EnsureIndices → Optimize → resume` chain and checked ordinary
`DISABLED` cut loaded into a fresh target with no imported lifecycle/token
authority are green too. The legacy Mutation/Load/delete, `load_file`, and
corresponding `_as` refusal matrix is green under `ENABLED` and interrupted
`DISABLING`. F6b2 is implemented. Process-local gates are not recast as
cross-process fencing: the process cases remain sequential under the externally
enforced sole-writer handoff.

The fairness proof covers resident-producing served puts: bounded
preprocessing/inflight → root MemWAL opportunity shared → profile shared →
table admission. The driver retains root opportunity exclusively across its
frozen finite round, then takes profile/admission per candidate. Both permit
kinds retain the worker-registry `Arc`, preventing weak-root fence ABA.
Shutdown fences root opportunity exclusive and then profile exclusive, drops
both, and only then joins the driver. F6b8 closes the previously excluded
resume case: the non-clone root producer permit transfers into detached writer
installation and every retained-retirement path, and an urgent trigger is
armed before release. The driver snapshots and retires only exact empty owners
under lane-exclusive authority before running its unchanged node-before-edge
candidate order. Tests pin driver-first and resume-first/caller-cancelled
races, prompt cross-lane slot reuse, and shutdown waiting for detached
ownership. The broader post-claim
install/retirement-failure matrix remains a later F6 acceptance owner.

Productive SchemaApply is deliberately absent: an
enrolled graph's schema changes only through checked sealed/retired export,
fresh graph initialization with the desired schema, and ordinary load there.
Physical rebind preserves accepted schema. F6b5 closes bounded stream-aware
served export; F6b7 closes the paired failpoints-only token-index decision
instrument with a bounded NO-GO for the uncompacted profile-cycle fixture.
F7b later closes graph-redacted operational-status transport; the other
served/public surfaces remain later F6b/F7 work. F6b4 separately closes the
isolated dead-letter envelope evidence.

### Implemented F6b3 exact-selected uncovered-tail evidence subset

F6b3 extends `memwal_stream_cost.rs`; it does not add a production maintenance
path. The fixture seeds one base uniqueness conflict while `DISABLED`, then
repeats zero-lane enable/disable cycles before enrollment. Each cycle adds two
immutable profile-management receipts without touching MemWAL. This is the
controlled token-ledger variable; the profile transitions also advance graph-
manifest history, while graph open and offline-authority setup stay outside the
timed first-probe windows. Enrollment, the final enable, one all-diverted
occurrence, current-token cardinality, and the returned terminal logical ID and
one-entry page cardinality stay fixed at every depth. Exact page fields and
serialized byte length are observations, not asserted byte-identical fixtures.

The normal local cell covers 1 and 8 cycles. Ignored local and
configured-RustFS cells cover 1/8/32/128. Every sample reports the exact token
version selected by main, the named lookup index's total/uncovered fragments,
serialized page size, and the cumulative advisory whole-process RSS high-water
mark. Fresh-handle hit/miss plus the first terminal page, then warm hit/miss and
repeat terminal pages, report token-read counts, total table-store read bytes,
manifest reads/bytes, adapter operations, and per-sample warm/repeat elapsed p50
plus max-of-eight. Graph
open happens before those windows, so “fresh handle” is not a cold-open or cold-
provider-cache claim. The
measured operation windows fail on authority writes, MemWAL/base-table reads,
prefix listing, or dead-letter payload-object access. Coverage is a separate
read-only sample-level probe. Wall time is evidence, not an SLO.

This is deliberately an uncovered-tail instrument. The production token index
is created at genesis, and no recovery-owned authority-safe reconciler exists.
Calling raw `optimize_indices` there would move an unselected physical HEAD and
would not prove production behavior. At the F6b3 boundary, receipt-key cost and
the paired covered/reconciled decision remained open; F6b7 closes that evidence
gap below without adding production maintenance. Count/oldest-age status and any
recovery-owned maintenance remain separate concerns; F6b7's uncompacted-
profile-cycle bounded NO-GO below schedules no standalone reconciler.

### Implemented F6b7 selected token-index decision instrument

F6b7 preserves F6b3 as the uncovered-tail baseline and extends the same fixture
with one paired, failpoints-only after-cut. Before physical maintenance the seam
settles recovery, excludes every stream-token writer, and proves the raw token
HEAD is exactly the manifest-selected witness. It accepts only a one-version
`CreateIndex` successor whose changed index metadata belongs to the named token
lookup index, whose fragment set/schema/row count are unchanged, and whose
coverage reaches the complete selected fragment set. Only then does the test
manifest select that exact witness for the reconciled sample.

The before/after windows compare current-token hit/miss, profile-management-
receipt hit/miss, and the bounded terminal page while proving identical logical
token/receipt identities and terminal entries. The measured maintenance window
contains `optimize_indices`, exact transaction classification, and manifest
selection. Gate/coordinator setup, the pre/post content proofs, and final graph
refresh are outside it. The instrument reports both request- and byte-based
benefit/amortization; latency remains supporting evidence rather than an SLO.

This seam is not production maintenance. It is compiled only for tests and
`failpoints`, owns no recovery sidecar, and deliberately cannot justify calling
raw `optimize_indices` from an ordinary runtime. The configured-RustFS sweep is
a bounded NO-GO only for the uncompacted profile-cycle fixture, not a universal
token-index NO-GO. No standalone production reconciler is scheduled. Remeasure
at greater depth, after a Lance/index-grammar change, or before considering
graph-manifest-compacted or checked-Optimize-coupled maintenance. Exact samples
and ratios remain in [RFC-026](../rfcs/0026-memwal-streaming-ingest.md); the
fixture owner remains in the [testing map](testing.md). Ordinary graph
`optimize` still does not maintain `_stream_tokens.lance`.

### Implemented F6b4 dead-letter envelope evidence subset

F6b4 extends the existing codec, `memwal_stream.rs`, and
`memwal_stream_cost.rs` owners; it adds one source-guarded, doc-hidden,
failpoints-only measurement seam, no production route, and no CI job. The small
codec regression keeps the inclusive one-under/exact/one-over writer contract.
The ignored production-size cell uses 8,192 adversarial candidates and the real
canonical-payload encoder/verifier. On the 2026-08-02 local macOS reference run,
10,364,432 source-value bytes became 62,301,270 canonical-payload input bytes
and exactly 67,108,864 encoded object bytes. The cap-aware writer retained
exactly 67,108,864 bytes of encoded capacity; before the fix the same shape was
observed retaining 132,644,864 bytes. Encoding took 286,280 microseconds and
verification took 2,254,424 microseconds. Verification and stopped/offline
payload export retain canonical payloads as bounded raw JSON, so a legal nested
list cannot expand the object into millions of `serde_json::Value` nodes. The
JSON value/schema is unchanged, but the Rust DTO field is now
`Box<serde_json::value::RawValue>` and serialized object-member order may
preserve the canonical payload rather than the old `Value` reserialization
order.

The isolated paired subprocess recorded an 85,557,248-byte baseline peak RSS
and 231,849,984 bytes for the exact-cap encoder/verifier, a 146,292,736-byte
lift. `201,326,592` bytes (192 MiB) is a one-sided remeasurement tripwire for
this implementation shape. These local measurements are evidence, not
allocator admission, a storage quota, or a latency/RSS SLO.

The one-over production shape is a typed encoded-byte refusal. The real fold
integration proves it publishes durable operational `DataBlock` evidence
before any canonical object, base-table effect, or current-token terminal-
disposition transition. Persisting the block may advance manifest and token-
ledger state; no recovery sidecar or partial fold remains. The existing
all-diverted success path creates one object and retains marker-only base
advancement semantics.

Run the production-size instrument explicitly:

```bash
cargo test -p omnigraph-engine --features failpoints --test memwal_stream_cost f6b4_dead_letter_object_records_production_envelope_and_peak_rss -- --ignored --exact --nocapture
```

### 7.1 Operational guardrails

- **Deployment**: main-only, unsharded, one resident writer, one externally
  enforced live writer process. Mutation surfaces require a server-owned
  cluster-runtime capability; embedded SDK and direct `--store` writers return
  typed `StreamingRequiresClusterRuntime` before body admission or effect.
  Cluster configuration/startup refuses known multi-replica layouts, but
  documentation remains explicit that process-local code cannot detect every
  foreign server process.
- **Control plane**: profile changes require F2's exact
  `CheckedClusterApplyAuthority`; the serving runtime capability cannot flip
  the profile, and an ambient engine handle cannot mint either authority.
  `DISABLING` durably closes puts/enrollment and retains only the fixed-principal
  fold continuation until disable has fully drained it.
- **Export/backup**: F6b1's checked capture closes the complete root gate
  envelope and freezes the selected branch's exact snapshot/catalog/table
  versions before releasing writers. It accepts exactly one of two terminal
  cases: (a) ordinary `DISABLED`, every enrolled lane `SEALED`, exact
  token/base parity, and zero current non-`PRESENT` authority; or (b)
  `RETIRED`, every enrolled lane `SEALED`, and an exact selected
  `AuthorityRetirementReceipt`/profile-chain/export-cut match. Case (b)
  verifies and precomputes the recorded branch provenance without reapplying
  case (a)'s terminal-token rejection. Any other mode/state, ambient enrolled
  ordinary caller, terminal ordinary entry, invalid filter, recovery blocker,
  or live root-slot conflict refuses before output; case (a)'s terminal entry remains
  typed `StreamExportBlocked`. The move-only cut retains checked serving
  authority and the sole slot through consuming output, while later writer
  movement cannot change its exact Lance-version pins. Cooperative cleanup,
  schema apply, branch replacement, and graph-root deletion cannot remove or
  reuse those pins. The existing receipt-verified `RETIRED` route is the only
  temporary ambient exception and owns the same slot through output. A post-start storage or
  writer failure remains a stream error. A dead-letter payload export is an
  inspection artifact—not an import/rebuild proof. Authority retirement resets
  sequencing only by rebuilding into a fresh graph identity; lossless terminal-
  authority transfer still needs a future authority-preserving export/import
  format. F6b5 owns public export authorization/response handling and the
  bounded chunk-queue/deadline/stall/disconnect contract; F7 retains public row,
  lifecycle, maintenance, and status activation.
- **Status**: F6b6 implements the checked read-only operational core internally.
  It exposes lifecycle revision, authoritative/observed epochs, exact
  generation/merge cuts, pending accounting when read-only proof exists,
  `StrictBlock`, receipt heads, last fold, all pending recovery, selected-token
  counts/coverage plus a bounded current-terminal sample, advisory driver
  health/completion/error/trigger/backoff, and rebuild blockers. Driver state
  remains non-authoritative. Cold-replay and flushed-LWW pending accounting
  plus exact oldest-uncovered age are explicit unavailable values, not guesses.
  `DISABLING` uses explicit checked cluster-apply status authority. Every
  sidecar is reported and blocks rebuild; a sidecar-explained physical move is
  unavailable rather than `StreamStatusChanged`. A reconciliation
  error appears only after measured evidence has scheduled a reconciler.
  Public `Omnigraph::stream_status` remains manifest-only. F7b owns the graph-
  redacted CLI/HTTP/OpenAPI transport; direct SDK and raw operational transport
  remain inactive. Cluster-only
  list/export continues to revalidate each current terminal row.
- **Shutdown**: the F5 supervisor protocol is wired into multi-graph server
  shutdown. F6a composes clean in-process shutdown ownership; F6b2 owns the
  active Unix `SIGTERM` and sequential OS-process recovery cells.

### 7.2 Correctness evidence

F3e did not claim this full F6 matrix. Its landed baseline pins recovery-v19's
closed grammar and exact N+1 receipt roll-forward, no lineage/audit movement,
receipt-bearing retired export, cluster/CLI preflight, and the genuine
v16↔v17 fence. F3f's focused recovery-v20, DataBlock, cluster/CLI, and
v17↔v18 evidence makes the narrow stopped/offline `WITHDRAWN` path active.
F5b's focused recovery-v21 evidence makes deterministic terminal diversion,
selected-token inspection/export, and `WITHDRAWN | DEAD_LETTERED` retirement
active; the genuine v18↔v19 adjacent-binary and frozen-receipt cell closes the
format-boundary evidence. The broader
retirement race/failpoint/freeze/export matrix below remains required before
the remaining management surfaces activate; F7a's narrower ingress evidence
is owned separately, and `AuthorityBlock` repair remains separate.

- Failpoints through the hidden candidate-runtime path at acknowledgement, claim,
  lifecycle, maintenance, both fold participants, canonical dead-letter
  encoding, conditional object creation, exact verification, confirmation,
  and manifest publication. Include sidecar-before-object, ambiguous/stalled
  upload, object-before-Lance, base-only, base+token-before-CAS, orphan
  inertness, all-diverted, exact retry, and an ordinary corrected successor; a structural assertion
  proves no object-write path is reachable before the sidecar is durable.
- `forbidden_apis` registration for every new writer; no raw Lance/MemWAL,
  token-HEAD, dead-letter-listing, or generic `allow_sealed` bypass.
- Genuine predecessor-binary format refusal/rebuild tests for every bundled
  strand, including populated dead-letter authority.
- One hidden candidate-runtime cluster test. F6a covers the in-process prefix:
  ordered NDJSON acknowledgement → automatic mixed fold → visible and
  dead-lettered terminal state → stopped/offline list/export → ordinary
  corrected successor → driver restart → clean shutdown ownership → offline
  disable to terminal `DISABLED`. F6b must extend the matrix through automatic
  node+edge fairness, forced OS-process shutdown, offline maintenance/rebind,
  enable, process restart, and resume. The full test uses
  sequential OS processes and proves the server has joined every writer owner
  before the cluster-state-locked apply process mutates profile or binding; a
  negative deployment-contract cell documents that process-local gates do not
  support overlapping writers.
- Hidden capability tests prove an ambient `Omnigraph` and direct `--store`
  caller cannot mint either cluster capability or reach a mutation, while the
  checked candidate runtime can. The matrix explicitly exercises legacy
  Mutation/Load insert/update/upsert/delete APIs under `ENABLED` and
  `DISABLING`, including Cedar-authorized `_as` calls, and proves refusal
  precedes body staging and every effect without the exact checked owner.
  Separate control-plane tests prove only
  validated offline `cluster apply` can flip the durable profile: enable exits
  before server start; disable survives crash at both profile CASes, resumes in
  a replacement offline apply, and cannot revoke its continuation after an
  acknowledgement until that fold is sealed. An offline block-correction
  command acquires the same cluster lock and sole-writer attestation. There is
  no claim that the in-process graph-profile gate arbitrates a concurrent
  server and apply process. Forced shutdown with an armed enrollment or
  old-delegation fold proves offline apply resolves it under old authority
  before the first disable profile CAS, then reacquires from the root.
  Physical rebind refuses before terminal `DISABLED` and reruns the recovery
  barrier under that exact disabled revision before its own CAS. Productive
  SchemaApply remains refused on an enrolled graph and is covered instead by
  a checked-export/fresh-target rebuild cell.
- F6b3's long-history token-ledger instrument measures exact current-token
  hit/miss lookup and bounded terminal-page scanning across increasing
  uncovered receipt tails, locally and on RustFS/S3. Per sample it records
  result/page bytes, coverage, and cumulative advisory whole-process RSS. The
  fresh-handle hit/miss and first page plus warm hit/miss and repeat pages report
  token-read counts, total table-store read bytes, manifest reads/bytes, adapter
  operations, and per-sample warm/repeat elapsed p50 plus max-of-eight while forbidding payload-object
  reads. It deliberately does not measure receipt-key lookup or synthesize a
  covered HEAD. F6b7 preserves that baseline and adds the paired failpoints-only
  receipt-key/current-token covered comparison plus exact maintenance-cost
  accounting. Its uncompacted-profile-cycle bounded NO-GO changes no production
  state machine and schedules no standalone authority-safe reconciler. Degraded-
  work status remains separate.
  No reconciler is required for logical EXP activation, and ordinary graph
  `optimize` is not credited with token-ledger convergence.
- A sustained mixed-backlog cell continuously makes node work ready while an
  edge is already in the frozen scheduling round and proves that the edge gets
  its bounded turn with a fresh post-node snapshot.
- F6b1's hidden stream-aware export uses the full exclusive cut envelope, then
  drops those gates before output. The landed cells prove one concurrent writer
  waits only through capture, its later commit cannot retarget the cut, the
  single nonwaiting root slot prevents accumulated pins and named-branch
  delete/recreate ABA, managed and unmanaged terminal authority binds exactly,
  ambient-enrolled and terminal-authority refusals produce zero bytes, and a
  post-start storage failure retains its provider error and releases the slot.
  Retirement confirmation also converges the managed applied row to `RETIRED`
  so immediate restart and refresh need no manual repair. The F6b remainder
  still owns focused prepare/put/resume/rebind and fresh-target round-trip
  composition. F6b5 owns typed HTTP preflight-before-response, the bounded
  channel/queue-envelope reservation and deadline defaults, plus stalled/
  disconnected consumer cells.
- V17 retirement planning begins with at least one current `WITHDRAWN` token
  whose graph key is absent or retains its prior value. Repeating plan across
  reopen returns the same digest and bounded counts. A structural plan
  assertion proves the implementation binds the exact manifest-selected
  pre-retirement token version/transaction witness, scans in bounded batches,
  and never `SortExec`s, materializes a terminal-key vector, walks ledger
  history, or lists raw WAL/object prefixes. Injected movement of the
  manifest/branch cut, profile revision, any binding/lifecycle/`SEALED` proof,
  base parity, token pointer, or relevant recovery makes confirmation
  effect-free `StreamRetirementPlanChanged`.
- Retirement failpoints cover pre-arm refusal, sidecar-armed-before-ledger,
  ledger-effect-before-manifest-CAS, manifest-CAS-before-finalization, and lost
  terminal response. Pre-arm refusal is effect-free; once durably armed,
  recovery may only roll forward the exact plan. Same graph/kind/ID plus digest
  returns the immutable receipt, the same ID with another digest conflicts,
  and the same plan/ID under another actor conflicts because the confirmed
  operation digest differs. A fresh ID after `RETIRED` returns typed
  `StreamAuthorityRetired`. The
  sole CAS selects the receipt-bearing token pointer, `RETIRED` profile row,
  retirement fields, and profile receipt-chain commitment together while
  advancing no live branch head or graph lineage. Race an armed pre-CAS
  authority-retirement sidecar with enable and disable apply: the graph-global
  recovery barrier must exactly finalize `RETIRED` or refuse before either
  profile CAS.
- A freeze matrix proves `RETIRED` refuses before effect for
  Mutation/Load/delete and `_as`, SchemaApply, BranchMerge, branch
  create/delete, every profile transition/refinement,
  Optimize/EnsureIndices/Repair/Cleanup,
  prepare/admission, quiesce/resume, correction/fold,
  enrollment/rebind, and every new recovery arm; only exact finalization of the
  already-armed retirement sidecar is allowed. Read/query/status and repeated
  export of the recorded cut remain available.
- Ordinary export before retirement returns `StreamExportBlocked`. After
  retirement, repeated exact-cut export includes the exact receipt; fresh
  init/load round-trips logical rows but imports no token, lifecycle,
  enrollment, receipt, or dead-letter authority. The source token rows, receipt
  ledger, WAL/dead-letter artifacts, and base versions remain byte-for-byte
  authoritative and retained. A two-live-branch cell proves each export keeps
  the same root receipt and ordered member-digest proof while emitting a
  distinct, recomputable `branch_member` witness and selected index for the
  chosen frozen branch. Any later enrollment
  of the fresh
  graph mints a new stream incarnation; an old-incarnation request is effect-free
  `StreamBindingChanged`.
  A declared terminal graph, previously unmanaged `RETIRED` graph, or
  previously unmanaged enrolled `DISABLED` graph restarts with the narrow
  `CheckedClusterServedExportAuthority`; no fold delegation, supervisor,
  admission, or other runtime authority is implied.
- F5 repeats the matrix with `WITHDRAWN | DEAD_LETTERED`, pins the immutable
  token witness plus scan-derived disposition counts, proves retirement
  requires no payload mutation and never deletes canonical dead-letter
  objects, and activates `DEAD_LETTERED` only after its format-specific
  retirement tag/recovery path is green. The same-format source binary
  performs retirement/export before the refusing successor format is used.
- Exact retry after restart returns the same terminal result while the
  `DEAD_LETTERED` token is current; a corrected ordinary successor must name
  that token, and an older predecessor cannot resurrect or bypass the key.
  Race the successor with another table's fold and prove the graph-global token
  pointer serializes both effects without a special replay queue.
- F6b4 dead-letter publication tests pin the measured single-object byte/RSS
  envelope. One-below/exact/one-over expansion proves over-limit `DataBlock`
  occurs before canonical-object, base-table, or current-token terminal-
  disposition transition while permitting the operational manifest/token-
  ledger movement needed to persist the block.
  Crash before/after sidecar, conditional PUT,
  exact existing-object verification, optional base effect, token effect, and
  manifest CAS. Digest/length mismatch fails closed; unselected objects remain
  inert. Mixed and 8,192-key all-diverted folds prove no intermediate token
  version becomes graph authority.

### 7.3 Resource and performance evidence

Instrument sustained throughput; p50/p95/p99 acknowledgement and visibility
latency; request/run/result queue depth; preprocessing and fold RSS; driver
backlog; base/token/manifest/dead-letter growth; token-ledger index coverage,
uncovered-tail age/count and reconciliation cost; node-before-edge priority; cold restart;
slow/failing stores; and local plus RustFS/S3 behavior. Compare warm
acknowledgement with the direct writer at fresh and long-history cuts. The
closing evidence commit records numeric pass/fail thresholds from the measured
baseline; until then the diagram makes no trip-count or latency claim.
The fold matrix includes a near-cap generation whose strings maximize JSON
escaping and repeated field-name/null expansion, plus a store stalled during
upload and verification. It measures canonical encoded bytes and peak RSS,
pins the single-object envelope, and proves admission above that envelope
blocks before canonical-object, base-table, or current-token terminal-
disposition transition. F6b4 closes the isolated production-size
encoder/verifier term with the 2026-08-02 local macOS
146,292,736-byte peak-RSS lift and 192-MiB remeasurement tripwire; broader
sustained throughput, store, and transport measurements remain open.

Keep CI sustainable:

- pull requests run the ordinary conservative classifier, documentation/link
  checks, entrypoint check, and AWS-feature test;
- authors run the affected focused owners locally and record exact commands
  and results in the PR;
- the full feature-superset workspace, immediate-predecessor format fence, and
  RustFS graphs run post-merge, on tags, or by manual dispatch;
- a red post-merge `main` is stop-the-line until fixed or reverted;
- high-entropy near-cap, endurance, and full performance matrices remain
  scheduled or explicitly opt-in with bounded timeouts; and
- no custom attested dependency archive, keyed rebuild/publisher, or shadow
  reporter is part of the design. A dedicated required protocol check returns
  only after an isolated harness demonstrates measured empty-runner and warm
  p95 within its proposed budget.

**Stopping after F6 was safe:** at that boundary row ingress, lifecycle/
maintenance, and operational-status transport remained behind the internal
activation seam; F6b5's exact-terminal served export was the narrow public
exception. F7a and F7b later activate only the proved graph row and graph-
redacted status surfaces. Remaining F7 control capabilities, DTOs,
authorization, and direct-refusal tests must still co-land with each surface.

---

## 8. F7 — staged public activation

F7a activates only the server-owned graph-ingest runtime, shared challenge DTO,
HTTP/OpenAPI route, remote `GraphClient`, and remote CLI arm. The public seam is
graph-first: callers submit mixed logical node and edge rows and never select a
Lance dataset, table incarnation, lane, writer, shard, epoch, or generation.
The engine resolves each declaration and reuses the existing lazy private-lane
prepare, checked runtime, bounded request owner, and resident fold driver. It
adds no coordinator, recovery owner, persisted authority, or format strand.

F6b5's export arm is already active. F7b exposes F6b6's checked status core
through a graph-logical read-only projection at HTTP and remote CLI while the
raw physical cut remains internal. Lifecycle and maintenance transport stay in
the F7 remainder and must not be exposed by weakening that cut. Raw physical
operations never become ambient `Omnigraph` writers in this cluster-only
profile. F2 already landed the profile adapter and
`cluster apply --confirm-stream-offline`; F7 does not restage that control.

| Capability | Owned cluster runtime | HTTP | Remote client / CLI |
|---|---|---|---|
| graph ingest | checked graph authority then the existing hidden core | `POST /graphs/{graph_id}/stream/ingest` (NDJSON in/out); missing `If-Match` returns a bodyless `428` challenge | `stream ingest --data <PATH\|-> [--graph-token <token>]` performs the challenge before opening input |
| status | F6b6 checked exclusive-cut status behind the F7b graph-redacted bridge | `GET /graphs/{graph_id}/stream/status` | `stream status [--json]` |
| fold | internal resident driver; explicit operator fold remains private | not exposed in F7a | later graph-level management surface |
| quiesce | capability-bound quiesce remains private | not exposed in F7a | later graph-level management surface |
| resume / abort | capability-bound resume remains private | not exposed in F7a | later graph-level management surface |
| graph export / rebuild artifact | runtime-pinned exact sealed cut | existing `POST /graphs/{graph_id}/export` with stream-aware guards | existing `export --server`; direct `--store` refuses an enrolled graph |
| same-binding maintenance | lifecycle-aware Optimize / EnsureIndices remain private | not exposed in F7a | later graph-level maintenance surface |
| physical rebind | no serving runtime; exact terminal `DISABLED` revision + `CheckedClusterMaintenanceAuthority`; accepted schema unchanged | none | disable to `DISABLED`, then `cluster apply --confirm-stream-offline`; later enable/restart/resume |
| schema change | no in-place EXP writer; freeze one checked sealed/retired export cut | none | initialize a fresh graph with the desired schema and load the artifact; never load over the enrolled source |

The table separates the activated graph row workflow from future graph-level
management plus existing export and private maintenance integration. The strong-ETag
challenge is an authority precondition, not a user-visible lane prepare verb.
After the exact token is accepted, lazy lane preparation remains internal.
Reachable terminal states retain narrow cluster/offline
exits without served HTTP/OpenAPI parity:

| Cluster/offline support | Command shape |
|---|---|
| current dead-letter inspection and payload export | `cluster stream dead-letter list|export --confirm-stream-offline` |
| exact DataBlock inspection and correction (**already active in F3f**) | `cluster stream block show|correct --confirm-stream-offline` |
| exact AuthorityBlock repair (**future**) | `cluster stream block repair-authority --confirm-stream-offline` |
| authority retirement / rebuild exit | `cluster stream retire-for-rebuild plan|confirm --confirm-stream-offline` |

The challenge and HTTP error types live in `omnigraph-api-types`; the engine
owns the canonical newline-terminated, redacted per-line result projection so
handlers do not rederive private evidence. Every future single-lane mutating management call requires
its operation ID and expected `lifecycle_revision`, with receipt-first replay;
profile apply instead binds the expected profile revision.
Root-wide authority retirement binds `(graph identity, AUTHORITY_RETIREMENT,
retirement_id)`, the expected profile revision, and exact plan digest.
Graph-wide Optimize/EnsureIndices is the multi-table exception to the
single-lane occurrence grammar. These naturally convergent maintenance calls
carry no caller operation ID and create no lifecycle management receipt. Their
checked served entry point authorizes one fresh plan; exact recovery settles
any armed plan before a retry replans against current graph/catalog/lifecycle
authority. A no-work retry is therefore a true no-op rather than a replayed
terminal receipt. EnsureIndices already implements that engine boundary;
Optimize must match it before F7 exposes either route.
Graph ingest uses one graph-scoped `stream_ingest` decision before body
ownership; lazy private-lane preparation does not add per-table policy checks.
Lifecycle and fold use `stream_manage`. Cluster-only DataBlock correction, future AuthorityBlock repair, and
retirement require their exact offline checked authority plus
`stream_manage`. Read-only status uses operational-metadata authorization.
Dead-letter payload export additionally requires the existing `export` action.

F6b1 has landed the lower/control-authority and engine half of the two-stage
stream-aware export seam. An exact managed `DISABLED | RETIRED` applied row,
or exact graph/state evidence subsequently restricted by the engine to
unmanaged `RETIRED` or enrolled `DISABLED`, can mint only
`CheckedClusterServedExportAuthority`, sharing the one process-local serving
registration without gaining writer authority. Retirement confirmation
CAS-converges a managed applied row to its exact `RETIRED` revision and refresh
preserves that declaration identity. The doc-hidden capture
method nonwaitingly reserves the exclusive root export gate, settles recovery, and then
holds profile plus sorted admission/schema/branch/token/table gates while it
validates terminal authority, prevalidates filters, and freezes the accepted
catalog, selected snapshot's exact Lance table versions, and retired
provenance. It releases those gates only by returning a private-field,
non-cloneable `StreamExportCut`. That cut retains the checked authority and
exclusive root gate through consuming output; later writer movement cannot retarget it,
and branch create/create-from/delete, schema apply, cleanup, and supported
whole-root deletion cannot remove or reuse a selected path/version until the
cut is consumed or dropped.
Ambient `Omnigraph::export_jsonl[_to_writer]`, embedded SDK, and direct
`--store` export of an enrolled ordinary `DISABLED` graph return
`StreamingRequiresClusterRuntime` before any byte. The existing receipt-
verified `RETIRED` ambient route remains a compatibility rebuild bridge
alongside F6b5's checked served route and, like every ambient export, acquires
the exclusive root gate before its first manifest read
and retains it through output. Under an ordinary
`DISABLED` profile, current terminal token authority still returns
`StreamExportBlocked` before output, while a storage/writer failure after
output begins remains that stream error. This implementation
changes no format or recovery grammar.

F6b5 implements the public export half on the existing route. Before
constructing a response or sending HTTP `200`, the served route authorizes,
reserves the complete queue/producer/consumer envelope under a bounded
deadline, and captures the checked cut. Preflight/slot refusal is ordinary
typed JSON before headers; exact Lance versions scan incrementally using
approximate batch targets and feed a strict bounded chunk queue; a stalled
receiver backpressures production; and
completion, disconnect, and error release every reservation. The HTTP/remote-
CLI/OpenAPI export cells co-land with those limits. F7a later activates public
graph row ingress and F7b graph-redacted status; F7 still owns lifecycle,
maintenance, direct SDK status, and their remaining transport parity.
The export artifact may initialize only a fresh target through the normal
cluster workflow; it is never loaded back over the enrolled source.

The operator workflow is intentionally split by owner. A same-binding
EnsureIndices request stays in the serving process, requires every affected
lane already be exactly `SEALED`, and otherwise returns a typed lifecycle
refusal. After the operator explicitly quiesces them, the runtime executes the
lifecycle-aware writer while holding the required sorted exclusive leases.
Optimize joins this workflow only after its separate recovery integration. A
same-schema physical rebind uses `graceful server shutdown → offline disable
to terminal DISABLED → cluster apply --confirm-stream-offline → separate
enable apply → server restart → explicit resume`. Disable, not an
operator-timed quiesce, closes the last-ingress race and drains every operation
that won before shutdown. The offline process holds the cluster state lock,
validates the exact disabled profile revision, declaration, graph/store
mapping, and sealed proofs, and leaves every rebound lane `SEALED`. Productive
SchemaApply is not part of that workflow: changing schema freezes a checked
sealed/retired export, initializes a fresh graph with the desired schema, and
loads the artifact there without transferring stream authority or modifying
the enrolled source.
The existing operation's Cedar action and `stream_manage` are both required;
offline block correction additionally applies the reason-specific
authorization and payload-export rules. A raw direct `--store` caller has
neither capability and refuses.

The remote capability classifier marks experimental stream mutation as
served-only. Embedded SDK and direct `--store` CLI mutation arms return
`StreamingRequiresClusterRuntime` before body ownership, writer claim, or
lifecycle effect; embedded manifest-only status remains available. Regenerate
and pin OpenAPI, prepare/lost-response/no-body handler tests, served
maintenance and stream-aware export cut/limit/stall/disconnect tests,
remote/embedded capability parity, offline block-control refusal/handoff tests,
audit actor attribution, and typed errors in the same activation slice.

F7 extends F2's already-public cluster-ownership, direct-mutation-refusal, and
v10→v11 rebuild baseline with the newly activated safe workflows. The same
activation PR updates:

- `docs/user/cli/reference.md`, `docs/user/operations/server.md`,
  `docs/user/operations/policy.md`, and `docs/user/operations/errors.md` for
  served prepare/ingest, status, lifecycle, maintenance, safe export,
  authorization, tagged results, and the
  stream/export-specific extension of the served-only versus embedded/direct
  refusal boundary; cluster docs/CLI reference separately own the offline
  block/correction, authority-repair, retirement, and dead-letter-export exits;
- `docs/user/operations/maintenance.md` for exact
  `quiesce → served Optimize/EnsureIndices → resume`, and
  `docs/user/clusters/index.md` plus `docs/user/operations/upgrade.md` for the
  distinct `graceful stop → offline disable to terminal DISABLED →
  cluster-state-locked physical rebind → separate enable → restart → explicit
  resume` and checked-export → fresh-init/load schema-change workflows. The
  latter includes the stream-aware old-format binary's safe served export from
  an exact pinned sealed cut before a **later activated-stream** format
  cutover; it never imports over the enrolled source. This extends, and does
  not defer or replace, F2's already-landed v10→v11 refusal/rebuild guide; and
- `docs/user/reference/constants.md` for every activated, measured F6/F7
  row/byte/count/time default: ingress line/run/root ownership, preprocessing,
  fold/dead-letter single-object byte/RSS envelope, driver cadence/backoff,
  bounded current-terminal scan pages, export slot/queue/deadline, and shutdown
  bounds.

User-doc examples and error tables are tested or link-checked in the same PR;
no active operational default remains discoverable only in an RFC or source
constant.

---

## 9. Cross-cutting rules

**Format bumps.** Under the strand model each new manifest or sidecar
vocabulary is a bump with a genuine-binary cross-version cell and a CI-pinned
predecessor build. CI pins **only the immediate predecessor**; older seams stay
env-gated. Assume a bump until an audit proves otherwise. Internal graph schema
and recovery-envelope schema are named separately; sharing a number never means
two incompatible payload shapes may decode under one stamp.

**Same-format terminal exit.** A strand that can make a non-`PRESENT` token
current must include, before that state is reachable, either lossless transfer
or irreversible retirement/export understood by that same binary. A successor
strand is not an exit because strict refusal prevents it from opening the old
root. V17 owns `WITHDRAWN` retirement; F5 extends it for `DEAD_LETTERED`.

**Strand budget.** V11 through v19 are implemented. Each unsettled future payload
is either exactly the frozen scaffold already registered or takes a new honest
pre-release strand. There is no guessed numeric ceiling: every added strand is
recorded with predecessor refusal/rebuild evidence, and the complete count
freezes at the 0.10.0 release gate. No discriminator acquires a different
payload meaning in place merely to save a rebuild.

**What the experimental designation does and does not buy.** It licenses
trimming: explicit enrollment, per-token producer barriers, fresh reads, and
configurable per-stream policy. It does **not** license trimming: Cedar
enforcement, typed bounded failures, shutdown ownership, durable attribution,
terminal dead-letter sequencing, recovery coverage, block
inspection/correction, the post-`SEALED` rebuild path, safe export, or the
evidence gates.

**The Hyrum boundary.** Committed: acknowledgement semantics and the loud
terminal/progress contract—graph-visible, terminally dead-lettered, or
explicitly structural-blocked with recovery authority. Declared unstable: fold
cadence, dead-letter object layout, status field shapes.

---

## 10. The ordering question

F2+F3 deliver no row-admission capability, but they provide a real exit and the
authority bridge required for maintenance while narrowing the profile flip to
cluster control. F4+F5a build the caller-shaped lane and automatic `OPEN`
progress owner while it remains hidden; F5b0 closes goal-`SEALED` resident
continuation and the offline disable loop without a format change; F5b adds
the terminal disposition. F6a proves the first in-process composition and
advisory driver diagnostics; F6b1–F6b5 close export/process/cost ownership and
F6b6 adds the checked read-only operational cut. F6b7 adds the failpoints-only
paired token-index decision instrument and records a bounded NO-GO for the
uncompacted profile-cycle fixture, so the F6b remainder owns only the remaining
guardrails. F7a activates the proved graph-ingest composition without exposing
its private lanes; F7b activates the graph-redacted checked status projection;
the F7 remainder owns graph-level lifecycle and maintenance surfaces.

This ordering makes every intermediate merge safe:

- after F2/F3, no caller can acknowledge a row;
- after F4, only tests can acknowledge through the hidden seam;
- after F5a, the server-owned hidden seam has automatic `OPEN` progress but no
  terminal dead-letter disposition;
- after F5b0, goal-`SEALED` drains and checked offline disable converge, but
  terminal data conflicts still have no dead-letter disposition;
- after F5b, the hidden seam also has that terminal disposition;
- after F6a, one hidden in-process candidate-runtime composition passes and
  tests can inspect typed advisory driver scheduling state, but F6 remains open;
- after F6b6, the checked owner can obtain one coherent read-only operational
  cut internally, while the public method remains manifest-only and no status
  transport exists;
- after F6b7, tests can compare one exact selected uncovered cut with its
  content-identical reconciled successor, but no production maintenance owner or
  recovery protocol exists; the fixture-scoped bounded NO-GO schedules neither;
- after the ingest-owned F6 evidence, F7a exposes graph-native served SDK,
  HTTP, remote CLI, and OpenAPI ingress together while direct mutation remains
  a typed refusal;
- the F7 remainder exposes only separately proved graph-level management and
  status workflows; it does not turn private lanes into user resources.

A performance spike may still invoke the hidden F4/F5a/F5b0 seam after F3. It never
lands a production writer or claims an SLO before F6.

---

## 11. Closed decisions and measured parameters

| Decision | Selected shape |
|---|---|
| Effectful claims | Every effect is classified into one immutable attempt-ledger row before another Lance call; the terminal `ClaimReceipt` commits the chain and there is no arbitrary attempt cap or receipt-free `SEALED` route |
| Receipt authority | Tagged immutable rows live in manifest-selected `_stream_tokens.lance`; hot profile/lifecycle rows retain bounded current pointers/count/chain commitments. Exact lookup remains recovery/idempotency authority, but EXP exposes no public receipt-history pagination. Uncovered-fragment fallback is correct and observable. F6b7's paired failpoints-only selected-index cut records a bounded NO-GO only for the uncompacted profile-cycle fixture, schedules no standalone production reconciler, and reopens at greater depth, after a Lance/index-grammar change, or before considering graph-manifest-compacted or checked-Optimize-coupled maintenance |
| Quiesce ownership | One exclusive admission lease; folds consume injected checked authority |
| Empty lane | Dedicated fence/tail/empty-proof path with an incremental authenticated WAL-segment cursor/chain; never scan from genesis or invent/seal an empty generation |
| Lifecycle format | Internal v12/lifecycle-v3 + recovery-v14 activates hidden enrollment, claim, ordinary/drain fold, and terminal lifecycle receipt with fixed-size ledger-chain/current authority. Dormant v14 scaffold meanings are immutable: F3 uses them only if exact, otherwise takes a new pre-release strand. F5a and F5b0 change no format; F5b requires a new terminal-authority/object strand. The release gate records the final strand count |
| Maintenance | Explicit lifecycle-aware integration per writer; no generic `SEALED` bypass |
| Public ordering | Hidden F4/F5a/F5b0 → format-bearing F5b → acceptance F6a → measurements/full matrix F6b → atomic served/remote activation F7 |
| Dead letter | One terminal LWW candidate per losing key; one deterministic, conditionally created NDJSON object under a measured 64-MiB encoded envelope and a 192-MiB isolated remeasurement tripwire; one current `DEAD_LETTERED` token per losing key; ordinary-ingest correction; `DataBlock` before canonical-object/base-table/current-token terminal-disposition transition on expansion |
| Process topology | One externally enforced writer process; profile apply requires stop → cluster-state-locked offline owner → restart; physical rebind additionally requires terminal `DISABLED` before its checked offline authority, with no claim that process-local locks detect foreign processes. Productive SchemaApply has no in-place EXP authority and uses checked export/rebuild into a fresh graph |
| Capability placement | `omnigraph-storage` plus `omnigraph-control-authority` resolve the engine/storage/cluster-lock dependency without a cycle; opaque stopped/offline and runtime guards preserve one storage path and expose no forgeable mint |
| Public topology | Under `ENABLED`, Mutation/Load/delete require the exact checked served runtime; under `DISABLING`, they are closed. BranchMerge is closed under both modes even with that runtime. Ambient SDK/direct CLI and Cedar-only lanes refuse before effect |
| Control authority | Profile flip requires validated offline cluster-apply capability; `DISABLING` closes admission durably and retains one fixed-principal fold continuation until the sole apply owner seals all lanes |
| CI policy | Conservative lightweight PR checks plus author-recorded local evidence; full workspace, format fence, and RustFS post-merge/manual; red `main` is stop-the-line. No custom attested/keyed dependency pipeline; a dedicated required protocol harness returns only from measured latency evidence |
| Driver identity | Timer/cap folds bind the durable delegation and deterministic cut authority, with no append-only management receipt |
| Fold ordering | One serial root cut; finite ready-identity rounds prioritize nodes then serve every captured edge with fresh validation |
| Export | Normal export requires fresh exact `SEALED` proof, token/base parity, and no current terminal token; same-format irreversible retirement may instead freeze the entire source at an exact cut and permit row-only export with a provenance receipt into a fresh graph identity whose later enrollment mints a fresh stream incarnation; payload export alone is not rebuild |
| Structural block | Fix + same-drain retry, exact data correction, or recovery-bound authority correction; ordinary rebuild preflight is post-`SEALED` |

Fold cadence, timeout defaults, and performance thresholds are measured
parameters, not architectural guesses. F5 starts with conservative bounded
defaults; F6 records the numeric values and pass/fail thresholds that evidence
supports before F7 exposes them.
