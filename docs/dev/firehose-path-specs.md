# Firehose Path — Implementation Specs

**Type:** implementation plan for in-flight work
**Status:** slices F0–F1 and the bounded F2 profile-authority tranche are
implemented. F2 selected internal schema v11/profile protocol v2 plus
recovery-v13 `StreamProfileChange`; the ordinary private fold remains
byte-for-byte recovery-v12. Public ingress, production enrollment, claim/drain,
correction, and maintenance integration remain later and require another strict
format/recovery strand before activation.
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
        │                        structural fault ──► block
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
shape, but does not activate retirement or correction. The next lifecycle strand
must activate an irreversible retirement/export path before `WITHDRAWN` becomes
reachable; F5 must extend that path before `DEAD_LETTERED` becomes reachable.

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

Internal schema is **v11** and the profile protocol is **v2**. Recovery-v13 has
one emitted discriminator, `StreamProfileChange`: it owns the exact
`ProfileManagementReceipt` token-ledger transaction, and only its achieved token
witness plus fixed next profile may reach the terminal manifest CAS. Unknown or
later v13 variants fail closed. The existing private ordinary fold remains
byte-for-byte recovery-v12; v13 does not change fold meaning.

The historical v10 bump also reserved the *shape* of the fold-attribution
dead-letter object reference
(`StreamFoldAttributionSummary::dead_letter_object`, explicit null today).
That reservation is not a dead-letter protocol: the validator still rejects a
populated reference, token authority has no terminal dead-letter disposition,
and current fold recovery cannot represent an all-diverted cut. F5 therefore
starts with a full format/recovery audit and assumes a new strand.

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
  detached ownership and exclusive-admission carry-through, **for a non-empty
  admitted or replayed generation**. The current worker explicitly refuses to
  seal an empty admitted generation.
- **Retain-all** — no GC, no canonical `_mem_wal` deletion, loud exhaustion.

Topology is fixed for the whole plan: **main-only, unsharded, one resident
writer, one externally enforced live writer process, upsert-only.**

### 1.3 The honest summary

The private non-empty put/fold seam is closed and tested. The public protocol
is not. Profile mutation now requires an opaque checked stopped/offline or
served-runtime owner, and `DISABLING` persists an exact restart/resume plan plus
drain-only continuation authority. This tranche can finish disable when no
lifecycle rows exist or every existing lane is already `SEALED`; it cannot
drain a non-`SEALED` lane and does not activate the enrolled-lane claim/drain
protocol. There is
still no effectful claim-receipt producer, empty-lane quiesce, caller-facing
ingest, dead-letter authority, fold scheduler, correction path, or `SEALED`
maintenance integration.

---

## 2. Slice map

| Slice | Delivers | Format | Gate |
|---|---|---|---|
| ~~F0~~ | Enablement authority | v10 | shipped |
| ~~F1~~ | Cedar split + read-only status | — | shipped |
| ~~F2 profile authority~~ | Capability-bound cluster control/runtime delegation, profile protocol v2, resumable `DISABLING`, exact profile receipt recovery | internal v11 + recovery v13 `StreamProfileChange` only; ordinary fold stays v12 | shipped |
| **F2 lifecycle tranche** | Claim receipts + drain `OPEN→DRAINING→SEALED`, including enrolled-lane disable continuation | another strict graph/recovery strand, versions not yet selected | after profile authority |
| **F3** | Resume / abort-drain + safe `SEALED` maintenance bridge + activate `WITHDRAWN` retirement before correction can create it | co-land with or follow the next lifecycle strand after audit | after F2 lifecycle |
| **F4** | Hidden ingest vertical slice + lazy enrollment | audit | after F3 |
| **F5** | Fold driver + terminal dead-letter authority + extend retirement for `DEAD_LETTERED` | a new strand after lifecycle activation | after F4 |
| **F6** | Guardrails + acceptance evidence | — | after F5 |
| **F7** | Served SDK / HTTP / remote CLI / OpenAPI activation | — | only after F6 passes |

These are dependency milestones, not mandates for giant PRs. Keep each PR
reviewable behind the hidden seam: the next lifecycle tranche may land receipts,
then non-empty/empty drain; F3 may land resume/abort, data/authority correction,
then each content-preserving maintenance owner and rebind; F5 may land the
serial driver, terminal authority/object recovery, then operations/replay.
Every sub-PR preserves the refusal for behavior it has not integrated.

The plan now contains **at least three** strict export/init/load rebuilds before
F7: the shipped v10→v11/v13 profile-authority tranche, another strand for the
complete lifecycle family, and F5's complete dead-letter/replay tranche. F4 may
require another strand if its enrollment receipt/payload cannot co-land with the
lifecycle family. Any additional strand requires an RFC/spec amendment and
explicit approval before selecting versions; it is not an automatic fallback.

The lifecycle tranche plus F3 are the operator lifecycle and maintenance
bridge. They ship **no row-admission or acknowledgement capability**; the
profile-authority tranche already narrowed the unsupported ambient profile flip
to validated cluster control. Drain alone is
not enough: current
`Snapshot::ensure_stream_effects_allowed` deliberately refuses merge,
optimize, index work, repair, cleanup, mutation/load, recovery, and schema
apply even at `SEALED`, because they can move the table witness, alter
token/binding authority, adopt drift, or destroy recovery evidence without a
lifecycle-aware proof. F3 must integrate the exact
witness/rebind transition before this plan may claim maintenance is available.
See §10 for the ordering rationale.

---

## 3. F2 lifecycle tranche — the drain path (`OPEN → DRAINING → SEALED`)

This section specifies future behavior. It is not part of the v11/v13
profile-authority tranche and cannot reuse recovery-v13 for new meanings. The
complete lifecycle family must select another strict graph/recovery strand
before any claim or drain effect becomes reachable.

### 3.1 Goal

An operator can quiesce a stream: close admission, fold everything
acknowledged, prove the exact empty cut, and reach `SEALED`. Native branch-ref
controls may then run. Other maintenance remains refused until F3 gives each
writer a sidecar-covered witness/rebind transition.

### 3.2 What exists

- The complete state-v2 vocabulary: `StreamLifecycle`, `DrainDescriptor`,
  `SealedProof`, `ManagementReceipt`, `ClaimReceipt`, `StrictBlock`,
  `LastFoldSummary` — all in `db/manifest/stream.rs`, all with per-state
  validators enforced by `StreamLifecycleEntry::validate`.
- The full-entry lifecycle CAS: `ManifestChange::SetStreamLifecycle`, with the
  publisher requiring the witness to match the batch's effective table pointer.
- Physical seal/drain/abort in `worker.rs` (§1.2).
- A working `OPEN` fold to fork from.

### 3.3 Profile authority implemented; lifecycle work remaining

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
   no production streaming admission/supervisor exists. F5 later strengthens
   server shutdown to close transport admission, settle the invoked tail, and
   join its supervisor/workers before exit, and factors the shared scheduler
   core without changing ownership. Only after the handoff may
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
   repair/cleanup, enrollment, fold, replay, lifecycle, token-ledger
   maintenance, authority retirement (the lifecycle strand or the
   version-appropriate F5 V2),
   and rebind. It then releases those gates,
   reacquires from the root in canonical order, and recaptures/revalidates the
   exact cleared authority. Ambiguous old recovery returns `RecoveryRequired`
   with no profile effect. Enable then publishes its receipt and exits before
   the server restarts. Disable is owned synchronously by the same no-ingress
   apply process: it publishes `DISABLING`, instantiates a temporary drain
   owner with only the durable fold continuation, recovers/drains every
   manifest-selected lane, and publishes `DISABLED`.
   A crash leaves the exact disable plan for the next offline apply to resume. A
   structural block leaves apply visibly pending with a block token;
   the narrowly capability-bound offline `cluster stream block
   show|correct|repair-authority` control in F7 takes the same cluster lock and
   sole-writer attestation, after which apply resumes. It is not a raw
   `--store` arm. Normal server startup refuses a `DISABLING` graph and directs
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
   and has no outgoing transition. V11 decodes that dormant state fail-closed;
   the next lifecycle strand must add the exact `DISABLED → RETIRED` transition
   before `WITHDRAWN` becomes reachable.

   Disable is a durable multi-publication operation, not a retrying error. The
   profile-authority tranche introduces one graph-profile admission gate outside
   every table gate and takes it exclusively for the first disable CAS; F4 makes
   every row path hold
   it shared from its final `ENABLED`/delegation check through
   `put_no_wait`, watcher durability, and same-writer fence classification;
   lock order is graph-profile shared → table admission → same-key queue.
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
     management-ledger receipt through the next lifecycle recovery strand's
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
   `ProfileManagementReceipt` rows. The next lifecycle strand extends this
   mechanism with `EnrollmentReceiptV2`, `BindingReceipt`,
   `ManagementReceipt`, `ClaimAttemptEffect`, and terminal `ClaimReceipt`; F5
   later adds fold-bound `DeadLetterRecord` and replay-checkpoint rows. A row has a
   deterministic identity derived from graph, stable table/incarnation and
   binding scope where applicable, receipt tag, operation ID, and attempt or
   checkpoint ordinal. Retain-all preserves every row, but the hot profile—and,
   after the lifecycle strand, lifecycle rows—retain only bounded current
   receipt IDs, counts, domain-separated chain digests, and tail commitments.
   They never contain a history `Vec`. V11 gives current-token and
   profile-receipt rows disjoint trusted row tags and canonical key domains; the
   next strand extends the control-ledger tags without reinterpreting them.
   Every token probe constrains the
   current-token tag and token lookup key, while every receipt probe constrains
   a ledger tag; a receipt can neither collide with nor materialize as a
   logical current-token row.

   Every v11 profile-receipt append is an exact pre-minted
   `_stream_tokens.lance` transaction owned by recovery-v13
   `StreamProfileChange`. Future control-ledger appends use the separately
   selected lifecycle recovery strand.
   Later-format tags, including F5 `DeadLetterRecord` and replay-checkpoint
   rows, use their strand's matching recovery envelope and do not reinterpret
   v13. The sole graph-manifest CAS advances the selected token-dataset pointer
   together with the corresponding hot pointer/count/chain commitment,
   lifecycle or profile revision, and any base/token effects. A ledger
   transaction without that CAS is inert recovery residue; the CAS cannot name
   a ledger row that the exact transaction did not create. Every ledger row
   has one versioned canonical
   `record_lookup_key` plus a common chain envelope containing its scope/tag,
   contiguous ordinal, predecessor record ID, prior chain digest, and resulting
   chain digest. Receipt-first retry performs one exact scalar-index lookup.
   History pagination is newest-first chain walking: it starts at the bounded
   hot `ReceiptChainRef` (or an opaque next-record cursor) and performs at most
   the requested exact predecessor probes plus one. It never asks Lance to
   order a range—Lance 9 scalar-index hits are row IDs, and `order_by` before
   `limit` would sort the whole matching scope. The cursor binds the selected
   token version, scope/head commitment, and exact profile/lifecycle revision,
   and continuation rechecks that they are still current. Movement returns a
   typed stale cursor rather than opening a client-named raw historical token
   version, so a page cannot mix chains. Both paths materialize at most the
   requested rows plus one and issue no application-level fold over retained
   history; neither broadens a current-token lookup into a ledger query.

   A scalar index alone is **not** a no-scan proof: Lance scans fragments
   appended after that index's coverage until `optimize_indices` folds them in.
   The lifecycle tranche therefore also lands a manifest-derived token-ledger
   coverage reconciler and its own
   `StreamTokenLedgerIndexMaintenance` recovery envelope. Under the
   graph-global recovery barrier and root token gate, it uses the existing
   Optimize-style exact-effect classification and auto-cleanup stripping to
   advance only the selected token-dataset pointer to a content-identical,
   newly covered version without weakening B2a retain-all. It does not alter
   current token rows, receipt chains, or logical results. Logical
   operations remain correct through Lance's uncovered-fragment fallback and
   never fail merely because this derived maintenance is late; status reports
   uncovered-fragment count/age and the last reconciliation error. The
   lifecycle control owner and, later, the F5 supervisor schedule reconciliation from
   selected dataset state rather than an in-memory job queue. F6 must measure
   and publish the trigger target, prove old indexed history is not rescanned,
   and show that the uncovered tail and lookup I/O stay within the accepted
   steady-state envelope before activation.

   `ManagementReceipt` still needs bounded canonical request **and result**
   preimages, builders, and digest recomputation in validation. The §4.3 lookup
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
   exact achieved epoch. The fold must bind and carry that already-produced
   receipt when it advances the epoch floor; today it preserves empty claim
   history. No effectful claim may finalize through the `(None, None)` state.

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
5. **`OPEN → DRAINING` CAS** with a production `DrainDescriptor` builder
   (today constructed only in tests).
6. **Drain-mode fold.** §4.3 forbids implementing this as the `OPEN` fold with
   a relaxed check: it must bind the complete expected `DRAINING` row and
   `drain_id`. Requires a `DRAINING`-accepting variant of
   `capture_stream_authority` (which today refuses any lifecycle but `Open`)
   and a dedicated recovery mode, since the current schema-v12 `StreamFold`
   validator requires `prior.lifecycle == Open` and byte-identical
   drain/block/proof slots. The fold consumes an injected
   `CheckedExclusiveStreamAuthority`; it does not reacquire admission.
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
   floor and `drain.target_epoch_floor_by_shard` to the achieved epoch before
   proof construction. Then classify and commit the fence-only WAL segment,
   prove shard/base merge agreement directly, and proceed without inventing a
   generation or calling `stage_stream_fold`. `(None, None)` is never a
   `SealedProof` input.
8. **Incremental authenticated WAL-tail commitment and
   `verified_empty_digest`.** The digest field and its `sha256:` validator
   exist in `SealedProof`; **no producer does.** V11 adds bounded current
   per-binding fields for the committed WAL cursor, segment count, segment-
   chain digest, current segment LWW-projection digest, and current
   claim-receipt ID. A rebind starts a new scoped genesis; the old scope
   remains immutable ledger provenance.

   **Claims, not folds, own these segments.** Every terminal claim uses Lance's
   public WAL tailer to stream exactly
   `(prior_authenticated_cursor, achieved_sentinel_cursor]`. It densifies and
   charges each page, rejects gaps, duplicates, foreign
   binding/shard/epoch records, and authenticates trusted metadata plus
   token-chain continuity. The no-roll delta is bounded by one
   8,192-row/32-MiB generation plus bounded control records. A claim-only or
   empty-lane cycle therefore commits a bounded control-only delta before
   another claim, so repeated empty cycles advance the cursor instead of
   rescanning from genesis. The receipt also commits the streaming LWW
   projection digest; a later drain fold recomputes it through
   `LsmScanner::without_base_table` and byte-compares the resulting winner/token
   plan.

   The terminal `ClaimReceipt` commits, with domain separation, the prior chain
   digest, exact lower/upper cursor, record count/digest, decoded empty-fence
   state, binding/configuration/incarnation, and LWW projection commitment. Its
   exact ledger transaction and lifecycle update are owned by the future claim
   recovery envelope;
   only their sole graph-manifest CAS advances the selected token pointer and
   lifecycle cursor/count/chain. It has no base-table or current-token effect.
   A failed or unclassifiable claim does not advance the segment cursor.

   Recovery-v12 remains byte-for-byte the ordinary private `StreamFold`.
   The next lifecycle recovery strand adds `StreamFoldV2` for an ordinary
   `OPEN` fold over the expanded lifecycle and keeps the separate drain-fold
   variant. `StreamFoldV2` owns the same exact
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
   through the next lifecycle recovery strand's
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
  restart cannot silently reopen or discard the operation. `RETIRED` requires a
  retirement receipt ID and cut digest, but its transition and read/export
  activation remain inactive. Unknown v13 discriminators and unsupported
  transitions fail closed.
- **The complete lifecycle family was not pre-registered.** Claim,
  `StreamEnrollmentV2`, ordinary fold-v2, drain-fold,
  `StreamLifecycleReceipt`, resume/abort, correction, retirement,
  token-ledger-index maintenance, sealed maintenance, and rebind must select
  another strict graph/recovery strand before any such effect becomes
  reachable. Historical `protocol_v10` enrollment and `protocol_v12`
  base-plus-token fold remain byte-for-byte unchanged. In particular, the next
  strand must activate same-format retirement before correction can create
  `WITHDRAWN`; F5 must extend that exit before `DEAD_LETTERED`.
- **The v10↔v11 boundary requires genuine binary evidence.** The
  old-binary/new-format refusal and export/init/load rebuild test must use a
  genuine v10 binary, not a stamp rewrite. Until that gate is green, the format
  implementation is incomplete even though the shapes and refusals are fixed.
  `SidecarKind` has nine current variants, three stream-specific
  (`StreamEnrollment`, `StreamFold`, and `StreamProfileChange`).

### 3.6 Evidence

Extend existing owners; do not open a new silo.

- **F2 first creates stable shadow `Firehose PR smoke` and
  `Firehose dependency rebuild` jobs** in `.github/workflows/ci.yml`. They
  always report; their classifier runs the bounded suite for engine, cluster,
  server, policy, API-types, CLI, Cargo/build inputs, tests, workflow, or
  branch-protection changes and quickly no-ops only for proven docs-only
  changes. Only after the exact artifact path satisfies the latency evidence
  below and provides the durable write-once key→digest binding does the F2 CI
  tranche add both verbatim contexts to branch protection. Job existence alone
  is not evidence for activation. The tier starts with manifest
  lifecycle/recovery, claim,
  token-ledger index reconciliation, empty/non-empty drain, failpoint, and
  `forbidden_apis` owners, then F3–F5 extend the same required tier in the PR
  that adds each behavior. For an
  ordinary source PR, its critical path and job timeout are both at most 15
  minutes. It may not depend on a
  best-effort GitHub cache hit: before the check becomes required, F2 proves an
  empty-runner p95 within budget by either isolating a cold-buildable protocol
  harness or using an immutable, attested dependency image keyed by Rust,
  target, every Cargo manifest/configuration and `Cargo.lock`, features/profile/
  flags, build scripts, exact base image/system packages, workflow/action
  digests, and `protoc` inputs. Durable keyed artifacts are published only by a
  separately privileged protected `push` to `refs/heads/main`, with a write-once
  key→digest binding and provenance for the repository, exact publisher
  workflow, main ref, and source digest. A PR is read-only with respect to that
  durable namespace. On a changed or unavailable trusted key,
  `Firehose PR smoke` succeeds only as an explicit delegation to the separately
  required dependency-rebuild context; it does not claim that smoke ran. A
  separate required
  `Firehose dependency rebuild` check **always starts and reports** on every PR:
  with an unchanged key it verifies the exact attested image/artifact metadata
  and quickly no-ops successfully; with a changed or unavailable trusted key it
  is the sole explicit exception and cold-builds plus runs the smoke tier inside
  the same PR workflow under one aggregate hard 60-minute timeout. Jobs may
  share only an artifact scoped to that exact workflow-run ID; a PR artifact is
  never promoted for another run. The post-merge main run publishes the durable
  keyed artifact for later PRs. Neither
  required check may disappear behind a workflow/path filter. Once activated,
  branch protection requires both contexts. The two gates
  never run duplicate cold compiles or fall back to an unbounded hour-long job.
  If the aggregate cold build plus smoke cannot meet its bound, the
  isolated harness is mandatory. Near-cap, RustFS/S3 fault, endurance, and
  performance matrices remain scheduled/opt-in.
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
- `memwal_stream.rs` owns drain with a resident generation. Add separate
  enrolled-never-written and already-folded empty-lane cells; there is no
  current empty-generation fast path to extend.
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
- Current format/recovery cells keep an ordinary fold byte-for-byte on v12 and
  crash v13 `StreamProfileChange` before/after its exact profile-receipt ledger
  transaction and sole manifest CAS; only the exact receipt/profile pair may
  become authoritative, and an unselected ledger version remains inert.
- Future lifecycle-strand cells must crash its ordinary/drain folds at every
  participant and crash `StreamLifecycleReceipt` before/after its ledger
  transaction and terminal manifest CAS. They must preserve the current
  claim/tail commitment exactly and prove that folds never append a claim
  segment receipt.
- **F2 co-lands the v10→v11 operator path** in
  `docs/user/operations/upgrade.md`, the cluster operator guide, and the release
  notes for the binary that introduces internal v11. The old v10 binary
  gracefully stops every writer, applies an
  explicit disabled profile, verifies terminal disabled state and zero
  production enrollments, and exports the visible logical graph; the operator
  then initializes a fresh v11 root, loads that export, applies cluster config,
  and restarts. V11 never opens or migrates a v10 root in place. V10 exposed no
  production enrollment/row caller, so any private/dev v10 lifecycle, pending
  WAL, or enrollment fixture is outside the supported rebuild contract and
  must be quarantined rather than claimed transferred by ordinary export; v10's
  ordinary exporter does not certify the absence of test-only stream state.
  This refusal/rebuild guidance lands with F2's immediate format bump, not
  later public activation.
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
that lets an operator run maintenance without moving a streamed table's HEAD
behind lifecycle authority. All lifecycle and maintenance entry points remain
crate-private, doc-hidden physical seams. F7 gives lifecycle plus same-binding
Optimize/EnsureIndices a server-owned runtime wrapper, and gives
cluster-declared schema/rebind work, only after the graph reaches terminal
profile `DISABLED`, a separate offline
`CheckedClusterMaintenanceAuthority`; neither becomes an ambient engine
writer.

### 4.2 What must be built

1. **`SidecarKind::StreamResume`** + its roll-forward-only payload in the next
   lifecycle recovery strand. Recovery-v13 remains
   `StreamProfileChange`-specific and schema-v12 `protocol_v12` remains
   `StreamFold`-specific. `Armed` binds the
   complete expected row and revision, `resume_id`,
   request digest, binding, configuration, base witness, graph-branch
   topology, fixed actor/operation, an `OPEN` template, the management
   receipt, and a **minimum next epoch floor**. Every `OPEN`-producing resume or
   abort first acquires the graph-profile gate shared, requires exact `ENABLED`
   plus matching runtime/delegation authority, and retains that outer guard
   through claim, `EffectsConfirmed`, and manifest publication.
2. **Two-phase epoch claim.** The achieved epoch is unknowable before the
   claim, so: claim under closed admission → durably record
   `EffectsConfirmed` with the exact sentinel/epoch, `ClaimReceipt`, achieved
   shard manifest/replay cursor, and final `OPEN` row → **only that row may
   publish.**
3. **`SEALED → OPEN`** consuming the sealed proof (`sealed_proof = None`),
   advancing epoch floors, and publishing the terminal claim and management
   ledger rows with their bounded hot pointers.
4. **`DRAINING → OPEN`** abort, which accepts only `DRAINING` and additionally
   requires: no guarded operation began, binding and the complete current row
   still match, every background seal/abort owner settled, and **no unmerged
   or strict-blocked cut remains**.
5. **The `SEALED` maintenance bridge.** Keep
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
   - a schema/config/path/native-ref change that rematerializes the table or
     changes its physical binding leaves the old lifecycle `SEALED` and must
     complete recovery-covered `stream_rebind` with a fresh enrollment and
     shard namespace. Rebind publishes a new exact **`SEALED`** binding and
     proof; a separate `StreamResume` claims a higher epoch in that new scope
     before `OPEN`; and
   - native branch-ref controls remain the existing exception, but any named
     graph branch keeps resume safely `SEALED`.

   The served same-binding matrix covers only content-preserving Optimize and
   EnsureIndices. Cluster-declared schema operations—even when they preserve
   physical row bytes, hidden stream metadata, token semantics, table identity,
   and binding—require the terminal-`DISABLED` offline authority; a
   rematerializing schema operation additionally uses the rebind shape above.
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
   remains Phase D; this slice enables explicit
   `quiesce → served same-binding maintenance → resume` and the separate
   disabled offline schema/rebind workflow.
6. **A representable, non-circular structural-block exit.** Internal v11 makes
   `StrictBlock` a tagged authority:

   - `DataBlock` retains today's authenticated generation cut plus canonical
     validator correction view and continues through RFC-026 §4.4
     `StreamCorrection`.
   - `AuthorityBlock` records a reason/failure phase, the complete expected
     lifecycle/binding/base/token/shard authority digest, a bounded canonical
     observed-authority classification, exact proof references (recovery
     operation and Lance transaction/version/content digests), an optional
     authenticated generation cut, allowed repair classes, and one
     reason-specific evidence digest. It does **not** pretend the data
     validator view proves a pre-cut binding or witness failure.

   A fold may install a repairable `AuthorityBlock` only while exclusive
   admission is held and those exact facts can be authenticated and retained.
   If the cut or an authority fact needed by every safe repair is missing or
   ambiguous, it reports `RecoveryRequired` or loud storage corruption; it
   never emits an operator-repair token from incomplete evidence. Block
   inspection branches by tag: `DataBlock` rescans the immutable cut and reruns
   the validator, while `AuthorityBlock` re-resolves the exact proof references
   and requires byte-identical evidence digest. Either disagreement fails
   closed.

   A persistent eligible authority block uses the future lifecycle strand's
   `StreamAuthorityCorrection`, addressed by `block_token`, caller operation
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
   and no current non-`PRESENT` token. If current `WITHDRAWN` authority
   remains, ordinary export stays `StreamExportBlocked`; a replacement is an
   available semantic successor but is not an absence-preserving export fix.

   The old-format binary instead offers the offline cluster CLI
   `cluster stream retire-for-rebuild plan --graph <id>
   --confirm-stream-offline`, followed by `confirm --graph <id>
   --retirement-id <uuid> --expected-plan-digest <sha256>
   --confirm-stream-offline`, under the stopped-writer, cluster-state-locked
   owner. It has no HTTP, direct-`--store`, or serving-runtime equivalent.
   Planning remains callable for an enrolled ordinary `DISABLED` graph whose
   streaming declaration was already unmanaged; checked cluster graph/store
   mapping and `stream_manage` remain mandatory.

   The read-only plan requires at least one current `WITHDRAWN` token and
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

   The future lifecycle recovery strand's `StreamAuthorityRetirement` pre-mints
   the immutable ledger receipt transaction and exact receipt-bearing output
   token version. Its sole
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
   pointer advance. Each later per-branch export identifies one member of that
   frozen map; branch selection never changes the root receipt.

   Once selected, Mutation/Load/delete and `_as`, SchemaApply, BranchMerge,
   branch create/delete and every profile transition/refinement,
   Optimize/EnsureIndices/Repair/Cleanup, and every other graph, schema,
   branch-ref, profile, lifecycle, recovery, maintenance, replay, fold,
   correction, enrollment, rebind, and content writer refuses before body
   admission or effect with
   `StreamAuthorityRetired { retirement_id, export_cut_digest }`; reads and
   repeated export of the recorded immutable cut remain available. Export
   verifies `RETIRED`, the exact selected receipt/profile-chain/logical-cut
   match, and the receipt-bearing token pointer without reapplying the ordinary
   terminal-token rejection. It emits that receipt beside the logical artifact.
   The receipt is provenance, not live token authority: init/load creates a
   fresh graph identity and any later enrollment creates a fresh stream
   incarnation, so a delayed old-incarnation request remains effect-free
   `StreamBindingChanged`. Authority retirement never deletes, ages out,
   rewrites, or
   pretends a `WITHDRAWN` token is `PRESENT`.

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
  proceeds only through its audited lifecycle-aware recovery shape.

### 4.4 Evidence

The lifecycle-strand authority-retirement portions of §7.2 are **F3 merge gates before
`WITHDRAW` becomes reachable**. F3 co-lands the upgrade, cluster, CLI, error,
and release-note contract for irreversible plan/confirm, read/export-only
source boot, and fresh-root rebuild. F6 reruns and integrates those cells; it
does not defer them.

The pinned matrix cell from [testing.md](testing.md) closes here:
*`quiesce → create named branch → resume` — bounded resume must recheck branch
topology under the closed gates and remain `SEALED`, while a compatible
main-only resume advances the epoch and opens.*
`db/omnigraph.rs::native_branch_controls_refuse_open_stream_and_allow_sealed`
is the half-built stand-in to extend.

Add one table-driven maintenance matrix covering every writer above: an
authorized `SEALED` same-binding effect advances the table pointer, both
witness copies, recomputed empty digest, and lifecycle revision atomically; a
rematerializing effect remains `SEALED` until its dedicated rebind; destructive
cleanup/adoption, BranchMerge, Mutation/Load, and every unintegrated writer
refuse; a crash at each physical/manifest boundary recovers exactly. Include
`quiesce → optimize → resume`,
`disable to terminal DISABLED → schema change → rebind (still SEALED) →
enable → server restart → explicit resume`, old-binding receipt retention plus
new-scope epoch restart, and a blocked-cut correction or
authority-repair → lost response → receipt replay → retry → sealed path.
Add shutdown/resume handoff tests at every claim/confirmation/publication
boundary: graceful shutdown cannot complete until the in-process resume owner
settles, and offline disable cannot begin until that process exits. Race
prepare, put, and resume against transport close, invoked-tail settlement,
process exit, the first disable CAS, and terminal disable; each operation is
either joined and then drained by the persisted disable plan or refused before
effect. Include an existing blocked `OPEN_AFTER_FOLD` drain, lost adoption
response, correction that preserves the override, and final `SEALED`.

The production ownership matrix is part of the proof: same-binding
Optimize/EnsureIndices run in the serving process under its runtime capability.
Cluster-declared schema/rebind does **not** rely on an operator-timed
quiesce/shutdown gap. The operator stops the server and runs the ordinary
offline disable apply to terminal `DISABLED`; its durable plan captures and
drains any prepare/put/resume that won before transport closed. Only then may a
cluster-state-locked `CheckedClusterMaintenanceAuthority` session with
`--confirm-stream-offline` bind that exact disabled profile revision and run
schema/rebind. It uses the same externally enforced single-writer handoff and,
before schema/rebind capture or effect, the same graph-global recovery barrier
plus root-order reacquire. Re-enable is a later offline apply; it exits before
server restart, and rebound lanes remain `SEALED` until explicit resume. The
confirmation is not described as a distributed fence. Raw direct `--store`
maintenance on a stream lifecycle remains refused.

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
     body is opened, but arms only the future lifecycle strand's
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
     request ID after an effect-free witness challenge. Once arming occurs, same ID,
     actor, and intent after a lost response returns that receipt; another
     actor or intent conflicts. Concurrent prepare IDs resolve through one
     lifecycle CAS, and
     losers return `already_enrolled` only after revalidating the winner's
     complete receipt and current binding. A successful prepare followed by no
     body intentionally leaves an empty enrolled `OPEN` lane, owned by F2's
     empty quiesce/disable path; and
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

   This activates the actor-bound `EnrollmentReceiptV2` and
   `StreamEnrollmentV2` selected in the future lifecycle strand; neither exists
   beneath v11/recovery-v13. If implementation audit shows either shape could
   not co-land with that strand, F4 stops for another RFC/spec amendment and
   explicit approval of a new ordinary graph/recovery strand with
   predecessor-binary and historical-sidecar refusal before prepare becomes
   reachable.

   Enrollment acquires graph-profile shared before the table lease. Under the
   same exclusive table admission lease and existing schema/main/token/table
   gates, it reruns the recovery barrier and rereads canonical-main
   `stream_profile`; the enabled revision/delegation and eligibility witness
   must match the checked runtime. Ordinary admission performs the same final
   profile/delegation/runtime match before handing off a run and retains the
   graph-profile guard through `put_no_wait`, watcher durability, and the
   same-writer fence result; ownership transfers with the invoked tail if the
   request disconnects. The offline disable owner takes its own process's gate
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
   accumulating run, each within 8,192 rows / 32 MiB. A raw line over the
   selected 32-MiB line ceiling is terminal before materialization. The result
   channel and reorder buffer hold at most one run (8,192 statuses); when the
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

---

## 6. F5 — fold driver and dead-letter

### 6.1 Fold driver

Today fold is test-invoked only. F5 adds:

- **Two idempotency domains over one fold core.** An explicit operator writer,
  `stream_fold_as(table, fold_operation_id, expected_lifecycle_revision,
  actor)`, still crate-private/doc-hidden behind cluster-runtime authority. It enforces
  `stream_manage`, applies receipt/in-progress-authority-first idempotency, has
  one bounded deadline, and returns the exact persisted result after a lost
  response. The cluster runtime's automatic driver uses a separate internal
  system entry point keyed deterministically by exact binding + generation cut.
  It relies on the fold sidecar, merged-generation authority, and replaceable
  `LastFoldSummary`; it appends **no `ManagementReceipt`** on timer/cap folds.
  Both entry points share the same validation/effect/publication core, but the
  driver cannot fabricate a user authorization or grow append-only lifecycle
  history every few seconds.
- **Durable fold authority with two non-overlapping owners.** Timer/cap folds
  consume the exact live `FoldDelegation` of an `ENABLED` profile plus a
  `CheckedClusterStreamRuntimeAuthority`. The resident supervisor also owns an
  operator fold or ordinary quiesce while serving. A disable fold consumes
  only the `DISABLING` plan's scoped `FoldContinuation` plus checked offline-
  apply authority and runs in the no-ingress temporary owner, never the serving
  supervisor. The shared fold core binds the applicable authority ID/profile
  revision and fixed `omnigraph:stream-fold` actor in its sidecar and
  attribution. Neither owner re-checks a mutable user grant after
  acknowledgement. Profile reconciliation cannot revoke or replace retained
  authority until every acknowledged cut is terminal. A missing/mismatched
  resident runtime scope refuses new admission and reports driver-unhealthy; a
  missing/mismatched offline scope refuses disable recovery; an already armed
  fold remains recovery-owned under its bound authority.
- **Split triggers over one scheduler core**: the resident owner handles
  generation cap, max-staleness timer, operator fold, and ordinary quiesce.
  The temporary offline owner handles only disable/recovery. Multiple resident
  triggers coalesce into one bounded per-table pending bit; the disable owner
  derives its finite work directly from the manifest plan. There is no
  unbounded parallel job queue.
- **One owner at a time**: a graph-scoped supervisor owned by the cluster server, shared
  by all in-process graph handles. It singleflights each physical binding,
  discovers `OPEN`/`DRAINING` backlog and relevant recovery at startup,
  publishes health/backlog into status, uses bounded exponential backoff with
  fair table rotation, and retains failed task ownership until recovery can
  classify it. A panic or exhausted retry is loud unhealthy state, never a
  silently dead driver. After the supported shutdown handoff, a separate
  temporary supervisor in `cluster apply` uses the same scheduler/fold core
  only to finish `DISABLING`; the two supervisors never overlap.
- **An honest process boundary**: the current lease and registry are
  process-local. The experimental deployment requires one externally enforced
  live writer process per graph. It does **not** claim a typed refusal can
  detect another OS process. Multiple replicas/direct writers remain
  unsupported until a distributed owner lease or substrate admission seal
  lands. Graceful shutdown is the ownership handoff for F2 profile apply: it
  closes transport admission, settles the invoked tail, joins all driver
  owners, and exits before the cluster-state-locked offline process begins.
  F3 schema/rebind is allowed only after that offline owner has driven the
  durable disable plan to terminal `DISABLED`; it cannot substitute a
  best-effort served quiesce for the profile freeze.
- **Dependency-prioritized serial cuts without starvation** (§4.7 P6): the
  fixed profile permits one resident writer and one exclusive fold root-wide,
  so it never claims a simultaneous graph-wide cut. At the start of a
  scheduling round, either owner freezes only the finite set of
  manifest-derived **ready table identities** under one accepted catalog—not
  generation cuts, rows, locks, or physical authority. Each identity gets at
  most one attempt in that round; work that becomes ready later waits for the
  next round. It visits ready node tables by dependency level and then every
  ready edge table, using a carried round-robin cursor within a level. Thus
  continuous new node work cannot leapfrog an edge already in the finite
  round. Before each attempt it refreshes exact manifest authority and skips a
  table that is no longer ready; immediately before validating an edge cut it
  opens the freshly manifest-selected post-node snapshot. Crash restart derives
  a new round from authoritative merged-generation progress and never
  reapplies a visible cut. This ordering reduces avoidable RI conflicts but
  does **not** promise “same-window RI-clean”; cross-table arrival skew remains
  an ordinary dead-letter case. A true graph-wide cut requires a future
  multi-resident memory budget and graph admission barrier.
- **Shutdown ownership**: stop new request parsing/admission, settle the bounded
  invoked tail into `durable` or `AckUnknown`, stop trigger creation, let an
  armed fold finish or remain recovery-owned, then join every graph supervisor
  and worker by a bounded deadline. Forced termination is covered by cold
  recovery. Generic Axum graceful shutdown is not sufficient evidence.

Cadence is the visibility gap. Expect ~seconds under load; the contract says
"typically seconds, unbounded tail" and there is **no producer-facing flush**
in this profile (§4.7 P5).

### 6.2 Terminal dead-letter authority (§4.7 P4)

Two failure classes, treated differently — collapsing them is how systems rot:

| Class | Examples | Disposition |
|---|---|---|
| **Data conflict** | uniqueness, RI, cardinality, keyed row validation | Deterministically divert one terminal **LWW candidate per losing key**, apply every independent key, keep the stream flowing |
| **Structural violation** | schema mismatch, witness violation, token-chain corruption | **Whole fold refuses, fails closed** — these mean bug or tampering, not bad data |

Diversion is **per key, not per row**. The LSM projection deliberately
collapses `P → X → Y` to terminal winner `Y`; it does not promise to preserve
superseded payload `X`. The immutable artifact therefore contains exactly one
terminal LWW candidate for each losing logical key, whose token is the current
`DEAD_LETTERED` authority. Earlier occurrences are represented only by the
authenticated WAL-segment and token-chain commitments. They are neither
payload-export entries nor replay selections.

The reserved v10 object-reference slot is only one field. F5 must amend the
RFC and format contract, then implement all of the following in one strand:

1. **Per-key terminal sequencing.** Add a durable `DEAD_LETTERED` token
   disposition (or an equivalently indexed current-authority state) distinct
   from `PRESENT` and correction-owned `WITHDRAWN`. It carries the exact fold
   operation and dead-letter object/replay-candidate reference. Base/token validation
   permits no matching current base row for this disposition. An exact retry
   returns terminal `dead_lettered`; a corrected successor must name that
   current token as predecessor and use a fresh `write_id`. Leaving authority
   at the old predecessor, pretending the row is `PRESENT`, or scanning object
   history on every admission is forbidden.
   The same F5 format strand adds the engine-only `Replay { replay_id,
   dead_letter_object_digest, candidate_entry_ordinal }` origin to hidden base
   metadata, token rows, fold attribution, and response DTO validation. V11/v13
   reject that tag; request bodies can never supply it. `durable`,
   `already_durable`, `withdrawn`, and `dead_lettered` return the exact
   persisted origin appropriate to their variant.
2. **Mixed and all-diverted folds.** The token participant advances every
   accepted key to either `PRESENT` or `DEAD_LETTERED`; the base participant
   still publishes exact merged-generation progress when visible-row count is
   zero. Recovery and attribution must allow zero visible winners with nonzero
   diverted rows. The current non-empty token plan, nonzero visible summary,
   and token-only/base-missing refusal cannot be reused unchanged.
   Because a legal cut may contain 8,192 token rows and the fold also appends
   one `DeadLetterRecord`, the F5 sidecar owns a deterministic token-dataset
   transaction chain of at most two links rather than silently creating an
   8,193-row transaction or lowering the admitted generation cap. It packs the
   record into the first transaction only when the exact row/byte limits permit;
   otherwise the one-row record is the final pre-minted link. No intermediate
   token version is manifest authority.
3. **Complete deterministic conflicts.** Replace the current first-error
   validator seam with a bounded structured result that identifies every
   keyed violation. Build stable conflict components and process them in
   canonical `(constraint identity, table identity, logical id, stream token)`
   order; for uniqueness/cardinality, the canonical admissible prefix wins and
   the remainder diverts. Remove losers, revalidate to a fixed point, and treat
   any unkeyed or contradictory result as structural. Persist the reason code
   and digest for the one final candidate of every diverted key.
4. **Recovery-bound, bounded object publication.** Sort at most 8,192 bounded
   canonical terminal-candidate descriptors—row ordinals plus fixed-width
   identities/digests, not copied keys or encoded NDJSON—and retain references
   to the already-owned dense generation/conflict plan. Publication uses
   immutable 1-MiB chunks, at most 256 chunks and 256 MiB of encoded payload,
   plus one canonical final manifest of at most 64 KiB. This is a fixed format
   bound, not a configurable provider multipart assumption.

   A deterministic first pass reserves the finite root-wide exclusive-fold
   scratch envelope and streams canonical NDJSON into hash/byte-count and
   1-MiB chunk-boundary sinks. It retains only bounded chunk
   `{ordinal, length, digest}` descriptors and the count plus chain/root digest
   of the ordered terminal-candidate identities; it writes no object and holds
   no encoded payload buffer. If payload would require more than 256 chunks,
   exceed 256 MiB, or produce a final manifest over 64 KiB, no object or Lance
   effect is allowed. The fold instead publishes a whole-cut tagged
   `DataBlock(reason = DEAD_LETTER_EXPANSION)` on the same
   `DRAINING` row—creating `goal = OPEN_AFTER_FOLD` when the ordinary fold had
   no existing drain—so exact correction/withdrawal can resolve it. Retry
   remains on that block; it does not repeatedly encode an impossible object.

   Otherwise the fold sidecar arms the exact immutable cut, logical manifest
   path, complete payload digest/length/candidate count and identity-chain
   commitment, ordered fixed chunk paths/descriptors, and exact final manifest
   bytes/digest. The manifest bound is validated before arming. The second pass
   re-encodes the immutable cut and conditionally creates each chunk with
   `PutMode::Create`; `AlreadyExists` is success only after exact length/digest
   verification. After all chunks verify, it conditionally creates the final
   manifest the same way, then stream-reads manifest and chunks through the
   charged scratch envelope and verifies the complete artifact **before either
   Lance participant effect**. This deliberately avoids unsupported
   conditional streaming multipart semantics in `object_store`.

   Recovery resumes the exact first missing chunk or verifies an already
   present one from the armed descriptors; it never overwrites a mismatch and
   never retries an ambiguous write outside recovery. The same two-pass encoder
   makes recovery deterministic without an object-sized `Vec`. Chunk,
   descriptor, encoder, and verification memory remain charged through final
   verification. The 32-MiB logical Arrow cap and 384-MiB RSS remeasurement
   tripwire are not misrepresented as JSON expansion bounds. Pre-effect
   chunks/manifests are inert retained B2a residue, and payload export/replay
   resolves only the logical manifest named by manifest-selected attribution.
   The fold sidecar's pre-minted token-dataset transaction chain also appends
   one immutable
   `DeadLetterRecord` containing the final-manifest reference/digest,
   candidate count/identity commitment, fold operation, and attribution
   digest. The sole manifest CAS advances a bounded graph-global
   dead-letter-chain head/count/digest together with the token/base pointers
   and graph-commit attribution. A record is authoritative only through that
   selected chain; an object or unselected ledger version alone is inert.
5. **Authoritative operations.** List historical graph-visible dead-letter
   artifacts newest-first by walking the selected `DeadLetterRecord`
   predecessor chain with the same exact-probe/page bounds as the F2 receipt
   ledger—never by raw prefix listing, graph-commit-history folding, or a
   Lance ordered range. A listed candidate may since have a successor; replay
   still revalidates its exact current `DEAD_LETTERED` token under gates.
   Payload export re-verifies each object digest and requires the existing
   `export` Cedar action; listing only bounded non-payload metadata follows
   status authorization.
6. **Retry-safe replay.** Replay requires caller-minted `replay_id`, exact
   selected object/entry-set digest, expected lifecycle revision, and both
   `stream_manage` and `stream_ingest`. The normalized selection contains at
   most one terminal candidate per logical key; selecting a duplicate key or a
   candidate whose terminal token is no longer current is an effect-free typed
   refusal. F5's new format strand adds
   recovery-owned `StreamDeadLetterReplay` arming/checkpoint/finalization; no
   raw `_stream_tokens` HEAD is authority. Replay progress is an immutable
   tagged checkpoint row in the F2 receipt ledger. Each exact pre-minted
   checkpoint-only ledger transaction advances the manifest-selected token-
   dataset pointer through one graph-manifest CAS but does **not** insert,
   replace, or withdraw a current-token row. The original
   `DEAD_LETTERED` token remains current until the admitted replay occurrence
   is folded through the ordinary token protocol. Each bounded page has unique
   keys. Before arming the page or revalidating its terminal tokens,
   replay passes the graph-global sidecar barrier, then acquires in order:
   graph-profile shared; sorted relevant stream-admission leases
   **exclusively**; schema; main-branch; the root stream-token gate; sorted
   graph-table gates; and every selected same-key queue in canonical key order.
   Under that frozen cut and F4's preprocessing budget, it computes exact
   normalized row/byte charges and chooses the canonical non-empty prefix that
   fits the resident generation's exact remaining capacity. If no next row
   fits remaining capacity but it fits an empty generation, replay creates no
   replay sidecar and retains the graph-profile shared guard plus the relevant
   exclusive stream-admission leases. It releases only the inner
   schema/main/token/table/key gates, invokes the ordinary fold core with the
   already checked exclusive authority, reruns the graph-global barrier, then
   reacquires the inner gates in canonical order and recaptures authority,
   capacity, reservation, and prefix before reopening admission. Continuous
   producer ingress therefore cannot starve replay between the fold and its
   replan.

   A candidate that cannot fit an empty legal generation is not a repeatedly
   returned page-level refusal. Replay appends a terminal per-entry
   `replay_candidate_too_large` checkpoint through the ledger-only transaction
   and manifest CAS, with no WAL put or current-token change, and proceeds to
   the next candidate. The `DEAD_LETTERED` token remains current so an ordinary
   corrected successor is still possible; same-ID retry returns the exact
   checkpoint result.

   `Armed` binds exact page membership, charges, resident-generation/capacity
   witness, and the reservation that prevents another put from consuming it.
   It retains that bounded
   ownership, including the root token gate, through receipt publication, all
   watcher/fence outcomes, and the checkpoint manifest CAS. Resubmission takes
   injected complete checked replay authority and may not reacquire those gates
   or queues. `StreamDeadLetterReplay` is graph-global relevant to every
   manifest/main-authority publisher just like a fold sidecar. It derives a
   stable UUIDv5 `write_id` from `(stream incarnation, replay_id, object digest,
   candidate entry ordinal, terminal token)`, records that replay origin on
   the row, and uses the ordinary ingest core with that candidate's terminal
   token as predecessor. After the page's ordinary ingest outcomes are
   exact/idempotently classifiable, a sidecar-owned checkpoint-ledger
   transaction and manifest CAS publish the bounded receipt checkpoint and
   terminal per-entry results without changing current-token authority.
   `stream_fold_required` is never a terminal replay disposition: seeing it
   after the bound reservation is an authority/protocol contradiction that
   remains recovery-owned and fails closed.
   Crash recovery classifies token transaction and manifest-pointer outcomes;
   the graph-global binding recovery barrier must settle an armed replay page
   before any manifest publisher, ordinary admission, or selected same-key
   queue reopens. An acknowledged
   page with an unpublished checkpoint can therefore replay the same write IDs
   and derive `already_durable` before any corrected successor advances, then
   publish progress once. The terminal update uses the same protocol. Same-ID
   retry resumes the persisted page;
   already completed entries reuse the same identity and resolve through
   current token authority. Same ID with another selection conflicts. A lost
   response can neither mint a second successor nor restart from entry zero.
   Retain-all deletes no referenced or orphan dead-letter object; future
   reclamation needs its own authority protocol.
7. **Extend same-format retirement.** Before F5 may make `DEAD_LETTERED`
   reachable, its graph/recovery strand adds format-specific
   `StreamAuthorityRetirementV2` recovery and receipt tags. They bind the exact
   manifest-selected pre-retirement token witness, stream current-token
   dispositions/counts in bounded batches, and never materialize, sort, or hash
   a token-row vector. They bind the bounded manifest dead-letter chain
   head/count/digest without enumerating ledger records or canonical objects.
   The V2 receipt adds
   `dead_lettered_token_count`, requires at least one current
   `WITHDRAWN | DEAD_LETTERED` token, retains every canonical object unchanged,
   and does not reinterpret v11/v13. A successful replay or
   corrected successor can make a dead-lettered key `PRESENT`; authority
   retirement remains the logical-graph-preserving, authority-discarding exit
   when an operator intentionally accepts loss of sequencing continuity for a
   fresh-root rebuild.

The V2/dead-letter authority-retirement portions of §7.2 are **F5 merge gates
before `DEAD_LETTERED` becomes reachable**. F5 co-lands the upgrade, cluster,
CLI, error, and release-note changes for that disposition and
`StreamAuthorityRetirementV2`. F6 reruns and integrates those cells; it does
not defer them.

Diversion is loud in the fold result, status, attribution, audit, and metrics.
No object reference is enough by itself to claim a key is terminal.

---

## 7. F6 — guardrails and acceptance evidence

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
- **Export/backup**: under one manifest snapshot, preflight accepts exactly one
  of two cases: (a) ordinary `DISABLED`, every enrolled lane `SEALED`, exact
  token/base parity, and zero current non-`PRESENT` authority; or (b)
  `RETIRED`, every enrolled lane `SEALED`, and an exact selected
  `AuthorityRetirementReceipt`/profile-chain/export-cut match. Case (b)
  verifies the recorded logical cut and receipt-bearing final token pointer; it
  does not reapply case (a)'s terminal-token rejection or rescan terminal rows.
  Any other mode/state refuses before headers or effect. A plain fold is
  insufficient, and a separate authorized dead-letter payload export is an
  inspection artifact—not an import/rebuild proof. In case (a), any terminal
  entry returns typed `StreamExportBlocked`. Replay applies to
  `DEAD_LETTERED`; a fresh accepted successor may replace either terminal
  state, but no absence-preserving successor clears `WITHDRAWN`. Authority
  retirement resets sequencing only by rebuilding into a fresh graph identity;
  any later enrollment creates a fresh stream incarnation, and the source
  never resumes. Lossless terminal-authority transfer still needs a future
  stream-aware export/import format. Export never silently omits an
  acknowledged WAL/dead-letter cut.
- **Status**: expose driver health, last success/error, pending trigger/backoff,
  token-ledger index coverage/uncovered-tail age and reconciliation error,
  lifecycle revision, exact generation/merge cut, advisory backlog,
  `StrictBlock`, relevant recovery, last fold, visible/diverted counts, and
  authoritative paginated historical dead-letter-artifact references.
  Advisory current-state summaries remain labeled non-authoritative; replay
  always revalidates the selected candidate's current token.
- **Shutdown**: the F5 supervisor protocol is wired into multi-graph server
  shutdown and tested under graceful and forced termination.

### 7.2 Correctness evidence

- Failpoints through the hidden candidate-runtime path at acknowledgement, claim,
  lifecycle, maintenance, both fold participants, both dead-letter encoder
  passes, conditional object creation, streaming verification, confirmation,
  and manifest publication. Include sidecar-before-object, ambiguous/stalled
  upload, object-before-Lance, base-only, base+token-before-CAS, orphan
  inertness, all-diverted, exact retry, and replay; a structural assertion
  proves no object-write path is reachable before the sidecar is durable.
- `forbidden_apis` registration for every new writer; no raw Lance/MemWAL,
  token-HEAD, dead-letter-listing, or generic `allow_sealed` bypass.
- Genuine predecessor-binary format refusal/rebuild tests for every bundled
  strand, including populated dead-letter authority.
- One hidden candidate-runtime cluster test:
  ordered NDJSON acknowledgement → automatic node/edge fold → visible or
  dead-lettered terminal state → list/export/replay → restart and forced
  shutdown → offline disable to terminal `DISABLED` → offline
  maintenance/rebind → enable → restart → resume. The test uses
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
  Schema/rebind refuses before terminal `DISABLED` and reruns the recovery
  barrier under that exact disabled revision before its own CAS.
- A long-history token-ledger cell keeps exact receipt lookup and bounded
  pagination correct across covered history plus a measured uncovered tail,
  crashes before and after `optimize_indices` and the pointer CAS, and proves
  reconciliation never changes current-token rows or receipt commitments.
  The negative control grows uncovered fragments and must expose the degraded
  physical scan/status honestly; the reconciled path must show old history is
  index-addressed rather than decoded or scanned by application code. A plan
  assertion forbids `SortExec`/ordered range pagination and pins at most
  page-size-plus-one exact predecessor probes.
- A sustained mixed-backlog cell continuously makes node work ready while an
  edge is already in the frozen scheduling round and proves that the edge gets
  its bounded turn with a fresh post-node snapshot.
- Hidden stream-aware export parks concurrent lazy prepare/put/resume/rebind
  only through exact cut capture, then proves their later movement cannot
  change streamed output;
  it refuses terminal token authority and ambient embedded/direct export, and
  round-trips the artifact only into a fresh target. The served-handler test
  proves preflight refusal returns the normal typed JSON status before response
  headers/body, export-slot exhaustion refuses before cut capture, concurrent
  callers cannot accumulate pinned cuts, and a post-start storage failure
  remains a stream error. F7 owns the existing route's bounded-channel defaults
  and stalled/disconnected consumer cells.
- V11 retirement planning begins with at least one current `WITHDRAWN` token
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
  prepare/admission, quiesce/resume, correction, replay/fold,
  enrollment/rebind, and every new recovery arm; only exact finalization of the
  already-armed retirement sidecar is allowed. Read/query/status and repeated
  export of the recorded cut remain available.
- Ordinary export before retirement returns `StreamExportBlocked`. After
  retirement, repeated exact-cut export includes the exact receipt; fresh
  init/load round-trips logical rows but imports no token, lifecycle,
  enrollment, receipt, or dead-letter authority. The source token rows, receipt
  ledger, WAL/dead-letter artifacts, and base versions remain byte-for-byte
  authoritative and retained. A two-live-branch cell exports each branch as a
  distinct member of the same frozen root cut and receipt; later branch
  selection cannot alter either digest. Any later enrollment of the fresh
  graph mints a new stream incarnation; an old-incarnation request is effect-free
  `StreamBindingChanged`.
  A declared or previously unmanaged `RETIRED` graph restarts in
  read/query/status/export-only server mode and can mint only
  `CheckedClusterServedExportAuthority`; no fold delegation, supervisor,
  admission, mutation, or other runtime authority exists.
- F5 repeats the matrix with `WITHDRAWN | DEAD_LETTERED`, pins the bounded
  manifest dead-letter chain commitment and immutable token witness, proves
  retirement does not require replay and never deletes canonical dead-letter
  objects, and activates `DEAD_LETTERED` only after its format-specific
  retirement tag/recovery path is green. The same-format source binary
  performs retirement/export before the refusing successor format is used.
- A replay page parks a concurrent corrected successor behind canonical
  same-key ownership through its checkpoint CAS. Crash immediately after the
  replay acknowledgement but before that CAS, then restart: binding recovery
  proves `already_durable`, publishes the checkpoint once, releases ownership,
  and only then may the successor advance. An unrelated-table fold racing the
  same graph-global token pointer waits behind replay's root token gate and
  then replans from the published pointer; neither side can bury the other's
  token effect. A partially full generation admits only the exact fitting
  canonical replay prefix. Under sustained producer ingress, a no-fit/empty-fit
  page retains outer admission across its injected-authority fold and replan.
  An empty-generation-oversize candidate publishes one durable terminal
  checkpoint without changing its `DEAD_LETTERED` token, survives a lost
  response, and lets the operation continue to the next candidate.
- Dead-letter publication tests hit 255/256/257 chunks, the 256-MiB payload
  edge, and the 64-KiB manifest edge; over-limit expansion installs the exact
  structural block before any object/Lance effect. Crash/retry at every chunk
  and final-manifest create verifies `PutMode::Create`, byte-identical
  `AlreadyExists`, mismatch corruption, inert partial residue, and second-pass
  recovery without conditional multipart or an object-sized buffer. The exact
  8,192-key all-diverted case forces the two-link token/record transaction
  boundary and crashes before, between, and after those links, proving no
  intermediate token version becomes manifest authority.

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
upload and verification. It proves the two-pass dead-letter encoder retains
only the bounded descriptor set and configured chunk reservation, never an
object-sized buffer, and stays within the selected exclusive-fold RSS limit.

Keep CI sustainable:

- for an ordinary source PR, the required `Firehose PR smoke` tier created in
  F2 has a **hard ≤15-minute critical-path budget** and no smoke job timeout
  above 15 minutes; it uses
  event-driven rendezvous rather than sleeps and gives every stalled owner a
  fail-fast diagnostic timeout. Its classifier includes `omnigraph-engine`,
  `omnigraph-cluster`, `omnigraph-server`, `omnigraph-policy`,
  `omnigraph-api-types`, CLI, Cargo/build inputs, tests, CI, and branch
  protection. Empty-runner and warm p95 for an unchanged image key must both
  fit before the check becomes required; the immutable dependency-image or
  isolated-harness strategy from F2 is correctness infrastructure, while an
  opportunistic GitHub cache is only an optimization;
- the required `Firehose dependency rebuild` check always starts and reports.
  With an unchanged immutable image key it verifies the exact attested
  main-published artifact/image metadata and quickly no-ops successfully. A
  changed or missing trusted key takes the one explicit exception: the PR
  cold-builds and runs smoke within one aggregate **hard ≤60-minute timeout**,
  sharing only a current-run artifact and publishing nothing durable. The
  `Firehose PR smoke` context explicitly delegates to this required rebuild
  context and does not claim that smoke ran itself. The protected post-merge
  main run may publish the write-once key→digest artifact.
  It never duplicates the cold build in multiple jobs. Workflow/path filters
  may not omit either required check, so branch protection never waits for a
  check that was not created;
- high-entropy near-cap, RustFS/S3 fault, endurance, and full performance
  matrices run scheduled or explicitly opt-in, have a **hard ≤60-minute
  timeout**, and publish artifacts; and
- no required PR job may normalize the prior hour-plus feedback loop. If a
  deterministic correctness cell cannot fit the PR tier, split the fixture or
  optimize the harness rather than silently dropping the gate.

**Stopping after F6 is safe:** all behavior remains behind the internal
activation seam. F7 is forbidden until every required F6 cell is green.
F7's own HTTP/remote capability, DTO, authorization, and direct-refusal tests
co-land with those surfaces and must pass before that activation PR merges;
F6 does not require a route that does not yet exist.

---

## 8. F7 — atomic public activation

The server-owned cluster runtime, shared wire DTO, HTTP/OpenAPI, remote
`GraphClient`, and remote CLI arms land together. The raw physical operations
never become ambient `Omnigraph` writers in this cluster-only profile. By the
time F7 executes, F2 will already have landed the profile adapter and
`cluster apply --confirm-stream-offline`; F7 does not restage that control.

| Capability | Owned cluster runtime | HTTP | Remote client / CLI |
|---|---|---|---|
| ingest preparation + rows | capability-bound prepare then hidden core | `POST /graphs/{graph_id}/streams/{type_name}/prepare` (JSON), then `POST .../ingest` (NDJSON in/out) | `stream ingest` performs prepare automatically |
| status | full exclusive-cut status | `GET /graphs/{graph_id}/streams[/{type_name}]` | `stream status` |
| fold | explicit operator fold + internal driver fold | `POST .../streams/{type_name}/fold` | `stream fold` |
| quiesce | capability-bound quiesce | `POST .../streams/{type_name}/quiesce` | `stream quiesce` |
| resume / abort | capability-bound resume | `POST .../streams/{type_name}/resume` | `stream resume [--abort-drain]` |
| block / correction | block view + data correction | `GET .../blocks/{block_token}`, `POST .../correct` | `stream block show`, `stream correct` |
| authority repair | exact DRAINING repair plan | `POST .../blocks/{block_token}/repair-authority` | `stream block repair-authority` |
| rebuild proof | post-`SEALED` preflight | `POST .../rebuild-preflight` | `stream rebuild-preflight` |
| graph export / rebuild artifact | runtime-pinned exact sealed cut | existing `POST /graphs/{graph_id}/export` with stream-aware guards | existing `export --server`; direct `--store` refuses an enrolled graph |
| dead letter | newest-first historical artifact list / payload export / current-token-validated replay | `GET .../dead-letters`, `GET .../dead-letters/{dead_letter_id}`, `POST .../{dead_letter_id}/replay` | `stream dead-letter list|export|replay` |
| same-binding maintenance | lifecycle-aware Optimize / EnsureIndices | `POST /graphs/{graph_id}/maintenance/optimize`, `POST /graphs/{graph_id}/maintenance/ensure-indices` | `optimize --server`, `maintenance ensure-indices --server` |
| offline blocked-disable repair | no serving runtime; cluster-state-locked sole-writer adapter | none | `cluster stream block show|correct|repair-authority --confirm-stream-offline` |
| offline authority retirement | no serving runtime; cluster-state-locked stopped-writer adapter | none | `cluster stream retire-for-rebuild plan|confirm --confirm-stream-offline` |
| cluster schema / rebind | no serving runtime; exact terminal `DISABLED` revision + `CheckedClusterMaintenanceAuthority` | none | disable to `DISABLED`, then `cluster apply --confirm-stream-offline`; later enable/restart/resume |

All request/response types live in `omnigraph-api-types`; pagination, canonical
token/digest parsing, and tagged per-line dispositions are shared rather than
reimplemented in handlers. Every single-lane mutating management call requires
its operation ID and expected `lifecycle_revision`, with receipt-first replay;
profile apply instead binds the expected profile revision.
Root-wide authority retirement binds `(graph identity, AUTHORITY_RETIREMENT,
retirement_id)`, the expected profile revision, and exact plan digest.
Graph-wide Optimize/EnsureIndices is the multi-table exception: its request
binds the caller operation ID, exact graph head and accepted-catalog digest,
and the canonical sorted compare set of
`{table_identity, lifecycle_revision}`. Its recovery/management receipt retains
that request digest and terminal graph-manifest result, so a lost-response
retry cannot discover a newer table set or retarget a later revision.
Prepare and ingest use `stream_ingest`; lifecycle, fold, block, correction,
rebuild, and authority repair use `stream_manage`; dead-letter replay requires both
`stream_manage` and `stream_ingest`. Read-only status and non-payload
dead-letter listing use operational-metadata authorization. Any endpoint or CLI
command that returns full row payloads requires the existing `export` action.

The existing graph export route/remote command becomes stream-aware in this
same slice through a two-stage checked engine seam. Before constructing the
response or sending HTTP `200`, synchronous authorization/preflight first
acquires one root-wide export slot plus the complete configured queue-byte
reservation under a bounded deadline; exhaustion returns typed backpressure
before a cut is pinned. With that reservation held, it takes the graph-profile
gate exclusively (blocking prepare, put, and resume), then sorted exclusive
stream admission followed by schema/main/token/table gates. Under one manifest
snapshot it requires every enrolled lane be `SEALED`, pins the catalog plus
exact Lance versions, and accepts exactly ordinary `DISABLED` plus zero current
terminal authority, or `RETIRED` plus the exact selected
receipt/profile-chain/export-cut and receipt-bearing token-pointer match. The
retired case verifies the recorded cut without rescanning terminal rows or
reapplying the ordinary terminal-token refusal. A refusal
releases the reservation and returns the ordinary typed JSON error/status
before headers or an NDJSON body begin. Only success returns an immutable
`StreamExportCut` carrying that reservation; the handler then releases the
gates and starts the producer from that cut. Later resume/rebind cannot retarget
it, and P7's cleanup refusal preserves its objects. This avoids holding
admission for a slow export consumer and prevents queued requests from
accumulating pinned cuts. F7 also replaces the current unbounded export channel
with a bounded chunk/byte queue charged to the already-held root-wide export
reservation. The producer
backpressures on a stalled receiver, stops promptly when the receiver closes,
never accumulates the graph in memory, and releases the slot/bytes with the cut
only after completion, disconnect, or error. A storage failure after streaming
starts terminates/errors that stream; it is not rewritten as a preflight JSON
error. The F7 activation PR records measured chunk, queue, and root limits and
passes preflight-refusal-before-response plus stalled/disconnect handler tests
before merge. The served handler calls a checked export seam requiring
`CheckedClusterServedExportAuthority`. An enrolled graph with exact manifest
profile `DISABLED | RETIRED` may restart in read/query/status/export-only mode
and mint only that narrow capability—no fold delegation, supervisor,
admission, mutation, or other stream runtime authority. F3 lands the retired
boot/export path with the retirement transition so the exit is reachable
before F7; F7 extends the same seam to ordinary disabled-profile export.
Ambient
`Omnigraph::export_jsonl[_to_writer]`, embedded SDK, and direct `--store`
export of a graph with any enrollment return
`StreamingRequiresClusterRuntime`. None can substitute its own process-local
gate. The artifact may initialize only a fresh target through the normal
cluster workflow; it is never loaded back over the enrolled source.

The operator workflow is intentionally split by owner. A same-binding
Optimize/EnsureIndices request stays in the serving process, requires every
affected lane already be exactly `SEALED`, and otherwise returns a typed
quiesce-required refusal. After the operator explicitly quiesces them, the
runtime executes the lifecycle-aware writer while holding the required sorted
exclusive leases. A cluster schema/configuration, path, native-ref, or rebind
change uses `graceful server shutdown → offline disable to terminal DISABLED →
cluster apply --confirm-stream-offline → separate enable apply → server
restart → explicit resume`. Disable, not an operator-timed quiesce, closes the
last-ingress race and drains every operation that won before shutdown. The
offline process holds the cluster state lock, validates the exact disabled
profile revision, declaration, graph/store mapping, and sealed proofs, and
leaves every rebound lane `SEALED`.
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
  prepare/ingest, status, lifecycle, block/correction, dead-letter, maintenance,
  safe-export endpoints/commands, authorization, tagged results, and the
  stream/export-specific extension of the served-only versus embedded/direct
  refusal boundary;
- `docs/user/operations/maintenance.md` for exact
  `quiesce → served Optimize/EnsureIndices → resume`, and
  `docs/user/clusters/index.md` plus `docs/user/operations/upgrade.md` for
  `graceful stop → offline disable to terminal DISABLED → cluster-state-locked
  offline schema/rebind → separate enable → restart → explicit resume`,
  including the stream-aware old-format binary's safe served export from an
  exact pinned sealed cut before a **later activated-stream** format cutover
  and init/load into a fresh target rather than in-place import. This extends,
  and does not defer or replace, F2's already-landed v10→v11 refusal/rebuild
  guide; and
- `docs/user/reference/constants.md` for every activated, measured F6/F7
  row/byte/count/time default: ingress line/run/root ownership, preprocessing,
  fold/dead-letter scratch chunks, driver cadence/backoff, status/dead-letter
  pagination, export slot/queue/deadline, and shutdown bounds.

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
root. V11 owns `WITHDRAWN` retirement; F5 extends it for `DEAD_LETTERED`.

**Strand budget.** The selected path expects two rebuilds before F7: F2 and F5.
The separately approved F2/F3 and F4 contingencies make four the stated
ceiling. A failure to co-land those shapes stops for amendment and approval; it
does not silently consume another graph/recovery version.

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
cluster control. F4+F5 then build the complete
caller-shaped lane while it remains hidden. F6 proves correctness, bounded
ownership, sustainable CI, and the performance premise. F7 alone activates the
surface.

This ordering makes every intermediate merge safe:

- after F2/F3, no caller can acknowledge a row;
- after F4, only tests can acknowledge through the hidden seam;
- after F5, the hidden seam has a progress owner and terminal disposition;
- after F6, all gates are proved but no compatibility surface is committed;
- F7 exposes the served SDK, HTTP, remote CLI, and OpenAPI together while
  direct mutation remains a typed refusal.

A performance spike may still invoke the hidden F4/F5 seam after F3. It never
lands a production writer or claims an SLO before F6.

---

## 11. Closed decisions and measured parameters

| Decision | Selected shape |
|---|---|
| Effectful claims | Every effect is classified into one immutable attempt-ledger row before another Lance call; the terminal `ClaimReceipt` commits the chain and there is no arbitrary attempt cap or receipt-free `SEALED` route |
| Receipt history | Tagged immutable rows live in manifest-selected `_stream_tokens.lance`; hot profile/lifecycle rows retain bounded current pointers/count/chain commitments; newest-first pagination follows exact indexed predecessor IDs instead of sorting a Lance range, and recovery-covered derived index reconciliation keeps old history index-addressed while the correct uncovered-tail fallback remains explicit and observable |
| Quiesce ownership | One exclusive admission lease; folds consume injected checked authority |
| Empty lane | Dedicated fence/tail/empty-proof path with an incremental authenticated WAL-segment cursor/chain; never scan from genesis or invent/seal an empty generation |
| Lifecycle format | The bounded profile tranche selects internal v11/profile-v2 + recovery-v13 `StreamProfileChange` only; recovery-v12 ordinary fold meaning is unchanged. The full lifecycle/claim/enrollment/correction/retirement family requires another strict strand, and F5 requires a later dead-letter/replay strand. The plan therefore has at least three rebuilds before F7; any additional split requires explicit amendment. |
| Maintenance | Explicit lifecycle-aware integration per writer; no generic `SEALED` bypass |
| Public ordering | Hidden F4/F5 → evidence F6 → atomic served/remote activation F7 |
| Dead letter | One terminal LWW candidate per losing key; two-pass conditional-create 1-MiB chunks (max 256/256 MiB) plus ≤64-KiB manifest, with pre-effect structural block on expansion |
| Process topology | One externally enforced writer process; profile apply requires stop → cluster-state-locked offline owner → restart; schema/rebind additionally requires terminal `DISABLED` before its checked offline authority, with no claim that process-local locks detect foreign processes |
| Capability placement | `omnigraph-storage` plus `omnigraph-control-authority` resolve the engine/storage/cluster-lock dependency without a cycle; opaque stopped/offline and runtime guards preserve one storage path and expose no forgeable mint |
| Public topology | Under `ENABLED`, Mutation/Load/delete require the exact checked served runtime; under `DISABLING`, they are closed. BranchMerge is closed under both modes even with that runtime. Ambient SDK/direct CLI and Cedar-only lanes refuse before effect |
| Control authority | Profile flip requires validated offline cluster-apply capability; `DISABLING` closes admission durably and retains one fixed-principal fold continuation until the sole apply owner seals all lanes |
| Required CI latency | Smoke and dependency checks always report; ordinary smoke ≤15 minutes, unchanged main-published dependency key verifies/no-ops, and a changed/missing key cold-builds plus smokes in the PR within one aggregate ≤60-minute bound without durable publication |
| Driver identity | Timer/cap folds bind the durable delegation and deterministic cut authority, with no append-only management receipt |
| Fold ordering | One serial root cut; finite ready-identity rounds prioritize nodes then serve every captured edge with fresh validation |
| Replay | One final candidate per key; ledger-only checkpoints never mutate current tokens, no-fit retains outer admission across fold/replan, and oversize candidates get terminal per-entry checkpoints |
| Export | Normal export requires fresh exact `SEALED` proof, token/base parity, and no current terminal token; same-format irreversible retirement may instead freeze the entire source at an exact cut and permit row-only export with a provenance receipt into a fresh graph identity whose later enrollment mints a fresh stream incarnation; payload export alone is not rebuild |
| Structural block | Fix + same-drain retry, exact data correction, or recovery-bound authority correction; ordinary rebuild preflight is post-`SEALED` |

Fold cadence, timeout defaults, and performance thresholds are measured
parameters, not architectural guesses. F5 starts with conservative bounded
defaults; F6 records the numeric values and pass/fail thresholds that evidence
supports before F7 exposes them.
