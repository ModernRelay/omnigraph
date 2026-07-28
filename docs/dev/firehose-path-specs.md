# Firehose Path — Implementation Specs

**Type:** implementation plan for in-flight work
**Status:** slices F0–F1 shipped; F2 next, blocked on one decision
**Design authority:** [RFC-026](../rfcs/0026-memwal-streaming-ingest.md) — this
file never overrides it. Where they disagree, the RFC wins and this file is
wrong. §4.7 records the selected experimental profile; §4.3/§4.6 record the
contracts every slice below implements.
**Audience:** whoever picks up the next slice, human or agent.

This is the execution plan for making the firehose lane — RFC-026's streaming
write path — actually usable. It exists because the private core is complete
and correct while *nothing public can reach it*: no caller can put a row, and
nothing schedules a fold. The gap is surface, lifecycle, and driver work, and
it is large enough that "just finish RFC-026" is not an actionable instruction.

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
        │                             [ WAL PUT ] ──► ACK   (durable, ~1 trip)
        │                                    │
        │                        ...thousands accumulate...
        │                                    │
        │                        graph checks (at fold; failures dead-letter)
        ▼                                    ▼
 ╔══════════════════ THE ONE PROTOCOL ══════════════════╗
 ╚══════════════════════════════════════════════════════╝
        │                                    │
   ACK (~12 trips)                     rows become VISIBLE
 (durable+visible+checked)            (acked long before)
```

The consequence that shapes every slice: **an acknowledgement cannot be
revoked.** Graph-state validation moves to fold time, so a row that fails
there is diverted loudly (§4.7 P4), never silently dropped and never
retroactively un-acked.

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

Internal schema is **v10**. The bump also **reserved** the fold-attribution
dead-letter slot (`StreamFoldAttributionSummary::dead_letter_object`, always
explicit null today), so F5 needs no second format change for it.

### 1.2 Built but unreachable (the private core)

Behind `#[cfg(feature = "failpoints")] #[doc(hidden)]` seams only:

- **Enrollment** — `db/omnigraph/stream_enrollment.rs::enroll_stream_table_b1`,
  recovery-v10, one empty unsharded shard on main.
- **Put / acknowledge** — charge → shared admission → same-key queue → worker;
  ack requires watcher success *and* the same writer's post-durability
  `check_fenced()`; any post-invocation ambiguity is `AckUnknown` + retirement.
- **Compare-and-chain tokens** — canonical payload/token digests, trusted
  hidden row metadata, same-generation overlays, graph-global
  `_stream_tokens.lance` selected by `__manifest`.
- **Fold** — `db/omnigraph/stream_ingest.rs::stream_fold_phase_b1`, recovery-v12,
  exact base + token participants, one manifest CAS.
- **Physical drain** — `table_store/mem_wal/worker.rs::seal_and_drain` →
  `force_seal_active` → `wait_for_flush_drain` → `prove_post_drain_cut`, with
  detached ownership and exclusive-admission carry-through.
- **Retain-all** — no GC, no canonical `_mem_wal` deletion, loud exhaustion.

Topology is fixed for the whole plan: **main-only, unsharded, one resident
writer, one live writer process, upsert-only.**

### 1.3 The honest summary

Everything that makes streaming *correct* exists. Everything that makes it
*usable* does not. In production today: no lifecycle transition except
enrollment and the fold's own witness update; no caller-facing ingest; no fold
scheduler.

---

## 2. Slice map

| Slice | Delivers | Format | Gate |
|---|---|---|---|
| ~~F0~~ | Enablement authority | v10 | shipped |
| ~~F1~~ | Cedar split + read-only status | — | shipped |
| **F2** | Drain path `OPEN→DRAINING→SEALED` | maybe v11 | **claim-receipt decision** |
| **F3** | Resume / abort-drain `→OPEN` | likely v11 | after F2 |
| **F4** | The front door: public ingest + lazy enrollment | — | after F3 |
| **F5** | Fold driver + dead-letter | — | after F4 |
| **F6** | HTTP / CLI / OpenAPI surfaces | — | after F5 |
| **F7** | Guardrails + acceptance evidence | — | last |

F2+F3 are the operator lifecycle. They ship **no user-visible capability** and
are roughly 40% of the remaining work — the deliberate "exit before the
entrance" ordering, because every structural operation (merge, optimize,
schema apply) on a streaming graph needs drain to exist. See §10 for the
alternative ordering and why it was not chosen.

---

## 3. F2 — the drain path (`OPEN → DRAINING → SEALED`)

### 3.1 Goal

An operator can quiesce a stream: seal the lane, fold everything acknowledged,
and reach `SEALED` — the state in which branch/maintenance operations are
permitted again.

### 3.2 What exists

- The complete state-v2 vocabulary: `StreamLifecycle`, `DrainDescriptor`,
  `SealedProof`, `ManagementReceipt`, `ClaimReceipt`, `StrictBlock`,
  `LastFoldSummary` — all in `db/manifest/stream.rs`, all with per-state
  validators enforced by `StreamLifecycleEntry::validate`.
- The full-entry lifecycle CAS: `ManifestChange::SetStreamLifecycle`, with the
  publisher requiring the witness to match the batch's effective table pointer.
- Physical seal/drain/abort in `worker.rs` (§1.2).
- A working `OPEN` fold to fork from.

### 3.3 What must be built

1. **`Omnigraph::stream_quiesce_as`** — new
   `db/omnigraph/stream_lifecycle.rs`, sibling to `stream_profile.rs`.
   Requires caller-minted `drain_id` + expected `lifecycle_revision`.
2. **The management-receipt layer.** `ManagementReceipt` has a struct and a
   validator that **nothing ever appends to** — `management_receipts` is
   always `Vec::new()`. Needs canonical request-digest derivation and the
   §4.3 lookup order: *receipt history first, expected revision second.* Same
   occurrence + same digest returns the recorded result; same occurrence +
   different digest is `StreamIdempotencyConflict`; no receipt + revision
   mismatch is effect-free `StreamLifecycleChanged` that **never retargets**.
3. **`OPEN → DRAINING` CAS** with a production `DrainDescriptor` builder
   (today constructed only in tests).
4. **Drain-mode fold.** §4.3 forbids implementing this as the `OPEN` fold with
   a relaxed check: it must bind the complete expected `DRAINING` row and
   `drain_id`. Requires a `DRAINING`-accepting variant of
   `capture_stream_authority` (which today refuses any lifecycle but `Open`)
   and a recovery variant, since the v12 `StreamFold` validator requires
   `prior.lifecycle == Open` and byte-identical drain/block/proof slots.
5. **`verified_empty_digest`** — the field and its `sha256:` validator exist in
   `SealedProof`; **no producer does.** Domain-separated over binding,
   configuration, incarnation, base witness, ordered shard-manifest and
   referenced-generation state, replay/merge cursors, and every decoded empty
   WAL fence sentinel. Deepest Lance-facing work in the plan.
6. **`DRAINING → SEALED`** publishing the exact proof.

### 3.4 Contract points that are easy to get wrong

- **Quiesce is multi-publication.** It is *not* complete at the initial
  `OPEN → DRAINING` CAS. The `DrainDescriptor` is the restart plan; the
  terminal management receipt appears only with `SEALED`. Restart continues
  `DRAINING` and **never auto-opens**.
- **Never hold a table queue while waiting for a fold that needs it.** The
  admission gate closes first; the fold's commit takes the normal table queue.
- **A permanent validation failure attaches a `StrictBlock`** to the same
  descriptor without changing its goal, and writes
  `LastFoldSummary(outcome = STRICT_BLOCKED, graph_commit_id = null)` in the
  same lifecycle CAS.

### 3.5 Blockers

**B1 — the claim-receipt fork (decide before writing code).**
`SealedProof::validate` requires a `ClaimReceipt`: the entry must carry it in
`claim_receipts`, `current_claim_receipt_id` must name it, and **every**
`epoch_floor_by_shard` value must equal the proof's `writer_epoch`.

Today `claim_receipts` is always empty and `current_claim_receipt_id` is always
`None` — legal only because the validator permits the `(None, None)` case. The
ordinary fold advances the epoch floor **without** minting a receipt.

The moment quiesce mints the first `ClaimReceipt`, the `(Some, Some)` branch
activates and begins enforcing "current receipt epoch == every shard floor" —
which the *existing* fold then violates. So either:

- **(a)** the claim-receipt discipline lands atomically **including the fold**
  (larger F2, but the validator's invariant holds continuously), or
- **(b)** quiesce reaches `SEALED` without minting one, which needs a
  different route to a valid `SealedProof`.

This is a correctness fork, not a style choice. Settle it first.

**B2 — deadlock hazard.** `stream_fold_phase_b1` acquires exclusive stream
admission itself. A quiesce that already holds the lease and then calls fold
will deadlock. Either refactor fold to accept an injected
`CheckedExclusiveStreamAuthority`, or have quiesce publish `DRAINING`, release,
and re-acquire per fold. §4.3's "never holds a table queue while waiting for a
fold" points at the release-and-reacquire shape.

**B3 — format audit.** State-v2 already carries every quiesce slot with
`deny_unknown_fields` + present-option, so *populating* `drain` /
`sealed_proof` / receipts needs **no bump**. A new `SidecarKind` would;
`SidecarKind` currently has only `StreamEnrollment` and `StreamFold`. Run the
§4.7 audit rule — assume a bump until proven otherwise.

### 3.6 Evidence

Extend existing owners; do not open a new silo.

- `db/manifest/tests.rs::stream_lifecycle_and_table_pointer_publish_in_one_manifest_cas`
  already walks `None→OPEN→DRAINING→SEALED` — extend with stale-`expected`
  refusal and revision monotonicity.
- `db/manifest/stream.rs` in-source tests own per-state validation.
- `memwal_stream.rs` owns drain-with-a-resident-generation and the
  empty-generation fast path.
- `failpoints.rs` owns every lifecycle-CAS crash boundary; extend
  `assert_open_stream_lifecycle_conflict` with DRAINING/SEALED variants.
- New: revision-fence cells (stale refusal, lost-response replay returning the
  recorded receipt, same-ID/different-digest conflict).

---

## 4. F3 — resume and abort-drain

### 4.1 Goal

`SEALED → OPEN` (resume) and `DRAINING → OPEN` (abort-drain), both
revision-fenced and caller-identified by `resume_id`.

### 4.2 What must be built

1. **`SidecarKind::StreamResume`** + its roll-forward-only recovery-v12
   payload. `Armed` binds the complete expected row and revision, `resume_id`,
   request digest, binding, configuration, base witness, graph-branch
   topology, fixed actor/operation, an `OPEN` template, the management
   receipt, and a **minimum next epoch floor**.
2. **Two-phase epoch claim.** The achieved epoch is unknowable before the
   claim, so: claim under closed admission → durably record
   `EffectsConfirmed` with the exact sentinel/epoch, `ClaimReceipt`, achieved
   shard manifest/replay cursor, and final `OPEN` row → **only that row may
   publish.**
3. **`SEALED → OPEN`** consuming the sealed proof (`sealed_proof = None`),
   advancing epoch floors, appending the `ClaimReceipt` and terminal receipt.
4. **`DRAINING → OPEN`** abort, which accepts only `DRAINING` and additionally
   requires: no guarded operation began, binding and the complete current row
   still match, every background seal/abort owner settled, and **no unmerged
   or strict-blocked cut remains**.

### 4.3 Contract points

- **Recovery never compensates an epoch or fence sentinel.** While admission
  stays closed it may claim a *still-higher* epoch and record a new exact
  confirmation. A byte-identical already-visible `OPEN` row finalizes the
  sidecar; anything divergent fails closed.
- **`ClaimReceipt` epochs are strictly increasing** across retained history,
  so resume's receipt must exceed the sealed proof's.
- **Abort is not a skip-invalid escape.** With residue or a strict block, the
  only forward paths are retry-fold or exact correction.
- **A named branch created after quiesce leaves resume safely `SEALED`.**

### 4.4 Evidence

The pinned matrix cell from [testing.md](testing.md) closes here:
*`quiesce → create named branch → resume` — bounded resume must recheck branch
topology under the closed gates and remain `SEALED`, while a compatible
main-only resume advances the epoch and opens.*
`db/omnigraph.rs::native_branch_controls_refuse_open_stream_and_allow_sealed`
is the half-built stand-in to extend.

---

## 5. F4 — the front door

### 5.1 Goal

A caller can put rows and get durable acknowledgements. **This is the firehose.**

### 5.2 What must be built

1. **`Omnigraph::stream_ingest_as`** — resolves actor, enforces the
   `stream_ingest` Cedar action, hands off to the private core.
2. **The streaming normalizer** — JSON/NDJSON → dense Arrow within the
   8,192-row / 32-MiB caps, defaults applied, types coerced. The loader has
   this machinery; it needs a streaming-shaped variant.
3. **Lazy enrollment** (§4.7 P2) — every table is streamable with the flag on;
   the lane is created on that table's first streamed write via the existing
   recoverable adapter. Concurrent first writers resolve through the one-winner
   lifecycle CAS; losers re-read the winner's binding and proceed. No explicit
   `enroll` verb in this profile.
4. **Per-line response mapping** — the §4.6 response union
   (`durable`, `ack_unknown`, `already_durable`, `invalid`,
   `stream_fold_required`, `stream_backpressure`, `recovery_required`,
   `stream_retry_required`, …), emitted in caller order, with the reorder
   buffer and contiguous-run rules.

### 5.3 Contract points

- **Caller-supplied vectors** (§4.7 P3). A row for an `@embed` table must carry
  its vector; missing is effect-free per-line `invalid`. Dimensions are
  validated; model identity is a documented producer obligation. The ack path
  makes **no external calls** — computing embeddings inside fold is
  permanently rejected because it would destroy fold determinism.
- **Upsert-only.** Deletes stay direct-lane operations.
- **Stop-tail rule.** After `AckUnknown` / capacity / lifecycle / authority /
  recovery blocks further physical admission, later uninvoked lines inherit the
  blocking status with `blocking_ordinal` — but adapter-local terminal results
  (`invalid`, `stream_input_too_large`) still take precedence.
- **Partial success never becomes an HTTP error.** Once a line is emitted, the
  same condition is reported per-line, not by failing the request.

---

## 6. F5 — fold driver and dead-letter

### 6.1 Fold driver

Today fold is test-invoked only. Needs:

- **Triggers**: generation cap, max-staleness timer, the operator `fold` verb,
  and drain.
- **A home**: a resident background task in the serving process.
- **Dependency ordering** (§4.7 P6): within each cycle, fold **node tables
  before edge tables**, derived from the accepted catalog's endpoint mapping,
  so an entity and its edges acknowledged in the same window fold RI-clean.
  Cross-cycle skew still dead-letters; bounded multi-cycle retry is
  deliberately deferred until measured need.

Cadence is the visibility gap. Expect ~seconds under load; the contract says
"typically seconds, unbounded tail" and there is **no producer-facing flush**
in this profile (§4.7 P5).

### 6.2 Dead-letter (§4.7 P4)

Two failure classes, treated differently — collapsing them is how systems rot:

| Class | Examples | Disposition |
|---|---|---|
| **Data conflict** | uniqueness, RI, cardinality | Divert the failing **key-chain suffix**, apply every independent key, keep the stream flowing |
| **Structural violation** | schema mismatch, witness violation, token-chain corruption | **Whole fold refuses, fails closed** — these mean bug or tampering, not bad data |

Diversion is **per key, not per row**: tokens chain per key, so successors of a
diverted occurrence in the same generation are diverted with it.

Destination: durable NDJSON objects under a reserved graph prefix, written
before the fold's manifest CAS and referenced from the fold-attribution
record — **whose slot v10 already reserved.** An unreferenced object from a
crashed fold is inert residue like any other pre-publication artifact. The
reject identity is the row's compare-and-chain token, which satisfies §7's
restart-stable-identity requirement without consuming any WAL statistic.

Operator verbs: list, export, replay (replay = ordinary resubmission under a
fresh predecessor). Diversion is loud — counted in the fold summary, the status
surface, and the attribution record.

---

## 7. F6 — surfaces

- **HTTP**: the §4.6 endpoints for ingest (NDJSON in/out), status, fold,
  quiesce, resume. OpenAPI regenerated.
- **CLI**: the `omnigraph stream` tree — `status`, `fold`, `quiesce`,
  `resume [--abort-drain]`, plus dead-letter verbs. Deferred from F1
  deliberately so no embedded-only verb enters the parity matrix only to be
  reworked when HTTP lands.
- **Parity matrix**: arms for every forked verb, embedded vs remote.
- **Compare tokens on the wire**: every mutating call requires its operation ID
  plus expected `lifecycle_revision`; status and block views expose the
  revision to use.

---

## 8. F7 — guardrails and acceptance evidence

**Guardrails**

- Topology limits enforced loudly at the public boundary: main-only,
  unsharded, one live writer process (embedded SDK + server on one graph is a
  typed refusal).
- Export/backup with pending WAL: refuse-until-drained or fold-first. An export
  must never silently drop acked rows.
- Server shutdown: drain/retire cleanly, never strand an ambiguous ack.

**Evidence (non-negotiable)**

- Failpoints through the **public** path at acknowledgement and every fold
  boundary, including dead-letter diversion (divert-object-before-CAS crash,
  orphan inertness, token-idempotent replay).
- `forbidden_apis` registration for each new public writer.
- CLI parity + OpenAPI drift.
- **The latency instrument**: warm-ack cost flat in graph history, plus an
  ack-versus-direct-write comparison. This is the measured claim that
  justifies the lane existing; without it the feature is unproven.

---

## 9. Cross-cutting rules

**Format bumps.** Under the strand model each new manifest or sidecar
vocabulary is a bump with a genuine-binary cross-version cell and a CI-pinned
predecessor build. CI pins **only the immediate predecessor**; older seams stay
env-gated. Assume a bump until an audit proves otherwise.

**What the experimental designation does and does not buy.** It licenses
trimming: explicit enrollment, correction, block-view, rebuild-preflight,
per-token barriers, richer status, configurable policy. It does **not** license
trimming: Cedar enforcement, typed bounded failures, shutdown drain, durable
attribution, recovery coverage, or the evidence gates.

**The Hyrum boundary.** Committed: acknowledgement semantics and the
applied-or-dead-lettered-loudly contract. Declared unstable: fold cadence,
dead-letter object layout, status field shapes.

---

## 10. The ordering question

F2+F3 deliver no user-visible capability yet consume ~40% of the remaining
work. The alternative — build F4+F5 against the test seam first, measure the
latency win, then finish the lifecycle — would demonstrate the firehose sooner.

It was **not** chosen because the lifecycle is what makes a streaming graph
operable: while a lane is `OPEN` it is its table's only writer, so merge,
optimize, index builds, and schema apply are all refused until drain exists. A
graph that can stream but cannot be maintained is not shippable, and an
experiment with no exit is a trap.

The tradeoff is worth re-examining only if the goal shifts from *ship it* to
*prove the latency claim early*. If it does, F4+F5-against-the-seam is a
legitimate spike — but it must not reach a production surface before F2+F3.

---

## 11. Decisions still open

| # | Decision | Blocks | Recommendation |
|---|---|---|---|
| 1 | **Claim-receipt fork** (§3.5 B1) — receipts land atomically with the fold, or quiesce reaches `SEALED` without minting one | F2 start | Needs a call; (a) keeps the validator invariant continuously true at the cost of a larger F2 |
| 2 | Fold-injected authority vs release-and-reacquire (§3.5 B2) | F2 design | Release-and-reacquire — matches §4.3's queue rule |
| 3 | Does `SidecarKind::StreamResume` force v11 (§3.5 B3) | F3 start | Assume yes until the audit says otherwise |
| 4 | Fold cadence defaults | F5 | Single-digit seconds under load; stretch when idle. Cadence is commit-history growth rate, so it is a real cost knob |

Everything else is settled and recorded in RFC-026 §4.7.
