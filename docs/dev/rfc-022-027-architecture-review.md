# Architecture review: RFC-022 through RFC-028

**Status:** RFC-022, RFC-023, and RFC-028 implemented; RFC-024, RFC-025, and
RFC-027 research-blocked; RFC-026 Phase A and private B1 implemented/evidence-
green, with public B2 inactive
**Date:** 2026-07-11
**Audience:** RFC authors, engine/storage maintainers, and release reviewers
**Reviewed against:** OmniGraph 0.8.1; Lance 9.0.0-beta.15 at
`f24e42c11a742581365e1cbe17c906ea2dac1bc6`; full Lance transaction,
branch/tag, MemWAL, row-lineage, read/write, compaction, and cleanup
specifications; pinned Rust implementation where the public specification is
not precise enough
**Current Lance revalidation:** all RFC-022–028 substrate contracts were
re-audited on 2026-07-17 against Lance 9.0.0-rc.1 at
`cec0b7dffe2d85c7e66dbe9d1f3891c297903a1d`; see the
[RC.1 alignment audit](lance.md#last-alignment-audit-2026-07-17-lance-900-rc1-git-rev-cec0b7df).
Stable V2_2, native branch/tag/cleanup semantics, exact full-table index
staging, and RFC-023's route-dependent/directional key-filter behavior remain
aligned. Data Overlay is opt-in and unused. MemWAL now inherits store/session
configuration but still lacks RFC-026's exact enrollment receipt and reversible
admission seal. RFC-026 therefore keeps the general multi-process profile
closed. A production-neutral bounded Gate E0 tested the existing public
surface, and its 2026-07-18 redesign is green: exact
`N`/`N + 1` immediate-successor probes replace latest resolution, complete
attempt tracking is flat at versions 8/80 with zero lists, and configured
RustFS runs the strict positive/negative matrix non-vacuously. Phase A then
activated internal schema v7 with exact empty-enrollment recovery,
identity-keyed lifecycle authority, process-local admission/exclusion,
partial-format refusal, and strict v6↔v7 rebuild. Private Phase B1 subsequently
activated schema v8/config-v2, one root-singleflight no-roll generation,
watcher-only acknowledgement, conservative replay/fold-only reopen, and a
recovery-v11 exact graph-atomic fold. Genuine v7↔v8 rebuild/refusal, all 24
graph-level race/crash cells, and the qualified local plus configured-RustFS
cost/RSS evidence pass. No production or public caller reaches the row seam;
B2 remains inactive.
The local RFC-024/025 decision instruments were rerun: both remain
research-blocked, with the current RC-specific counters recorded in the Lance
audit and RFC-025.
**Prior RFC-022 close-out revalidation:** Lance 9.0.0-beta.21 at
`1aec14652dcbace23ac277fa8ced35000bea0c40`; see the
[2026-07-12 alignment audit](lance.md#prior-alignment-audit-2026-07-12-lance-900-beta21-upstream-omnigraph-pinned-at-900-beta21-via-git-rev).
**RFC-023 substrate evidence revalidated against:** the same beta.21 revision;
filter-shape and conflict-order probes are recorded in RFC-023 §2. Internal
schema v6 introduced that evidence through exact-`id` fenced production
routing, and current v8 preserves it. Its final insertion-absence certificate/no-target-preflight route and
predeclared 10K/100K production series now satisfy the remaining implementation
and acceptance gates.
**RFC-026 substrate contract revalidated against:** RC.1 at the current revision;
the full MemWAL table and system-index specifications, including the
durability/fencing behavior carried forward from beta.17, are reflected in the
RFC's survey and acceptance guards. Gate E0 passed at the explicit bounded
profile and Phase A consumed it as described above; RFC-026 remains draft and
public row streaming is inactive.
**RFC-024/025/027 substrate claims revalidated against:** the current RC.1
revision and the full matching table/index/branch/tag/cleanup, transaction, and
row-lineage specification sections; their survey headers now record RC.1.
RFC-024 Gate A was subsequently measured on 2026-07-15: its exact BTREE scan
work is flat, but the required RustFS latest-manifest/object-byte curves grow,
so the candidate and format are research-blocked rather than accepted.
RFC-025 Gate 0 was measured on 2026-07-17: Lance tag semantics pass, but the
current in-manifest checkpoint-registry BTREE shape has history-sensitive
compacted scan bytes and crosses another scan-operation boundary at 1,000
commits. RFC-025 is therefore also research-blocked; current internal schema v8
still contains no retention state. The bucket-gated S3/RustFS cost cell is checked in but was
not run for that decision.

This is a review ledger, not a competing specification. The RFCs remain the
normative design records: RFC-022/RFC-028 document implemented contracts;
RFC-023 now documents an implemented and fully accepted contract; the later
siblings remain proposals. A finding is closed only when the affected RFC
either:

1. incorporates the required contract and tests; or
2. explicitly rejects the finding with evidence and records the resulting
   support boundary.

Items marked **BLOCKER** must be dispositioned before the RFC that owns them is
accepted. Items marked **TIGHTENING** may be resolved during revision, but they
must not silently disappear from review.

This ledger is durable review history, not a throwaway implementation plan.
After every finding is dispositioned, change its status to closed and record
the resolving RFC sections/commits; keep the RFC backlinks valid.

## Overall architectural judgment

The central architecture remains the right one:

- Lance is the physical storage/versioning truth.
- `__manifest` is the graph-visible authority and one graph publish is the
  visibility point for a logical graph commit.
- RFC-022 defines one correctness protocol with explicit physical-effect
  adapters rather than pretending that Lance offers one universal staged
  primitive.
- Stable schema identity, key fencing, durable heads, retention, MemWAL, and
  lineage acceleration are separate irreversible decisions and should keep
  separate evidence and format gates.

The remaining open blockers are boundary problems, not a reason to replace that
core. They concern sibling capability activation, strict rebuild rollout,
cross-process claims, and acceptance evidence. RFC-022's structural
rollout and the RFC-track classification are now closed. The native branch crash
classifier, foreign-source merge semantics, and shipped lazy-branch cleanup
exposure found by this review are also dispositioned below.

> 💬 **Second-pass verification (2026-07-11):** I independently re-verified this
> ledger's two sharpest new substrate claims against the pinned checkout before
> commenting: BLOCKER-01's two-phase branch create is confirmed in Lance's own
> doc comment, and BLOCKER-04's lazy-branch exposure is confirmed
> architecturally. Both findings produced shipped fixes on 2026-07-11 and their
> durable dispositions are recorded below. Overall judgment: concur with this
> ledger; three other findings supersede corrections applied to the RFCs on
> 2026-07-11 (noted inline at BLOCKER-07, BLOCKER-11, and tightening 5).

The latest revisions improved several important points and those changes should
remain:

- exact recovery-goal convergence is distinguished from adopting an unrelated
  newer version;
- MemWAL is treated as Lance's strategic streaming architecture, with API and
  format evolution as the integration risk;
- keyed fast-forward merge now has an explicit embedding-table memory gate;
- its final production shortcut consumes a complete persisted v1
  insertion-absence chain, revalidates both native ref incarnations, and emits
  another certified filtered `Update` without a target preflight, target join,
  or committed Append, so the proof composes across branch generations;
- the legacy `__manifest` PK representation is preserved rather than
  “normalized” into an incompatible form;
- cleanup's required operating cadence and the cost of indefinite deferral are
  explicit;
- native branch refs are no longer described as cross-process-safe merely
  because a process-local gate exists.

## Dependency correction

Stable table identity and incarnation are consumed by more than durable heads.
The intended dependency shape is therefore:

```text
RFC-022 unified write protocol
    |
    `-- RFC-028 stable identity/incarnation capability
            +-- RFC-023 key fencing
            |       `-- RFC-026 keyed streaming mode
            +-- RFC-024 durable heads
            +-- RFC-025 checkpoint rows
            `-- RFC-026 stream/reject authority
```

RFC-023 through RFC-026 also consume RFC-022's write/recovery protocol
directly; RFC-025 additionally owns retention, RFC-026 owns MemWAL, and RFC-027
depends on RFC-022's branch-merge pipeline without consuming RFC-028 yet. The
diagram highlights the identity dependency rather than duplicating every direct
protocol edge.

The maintainer design-series process permits incremental draft merges, but it
does not make a sub-contract of an unaccepted RFC independently authoritative.
RFC-028 now owns the extracted identity capability. RFC-023 through RFC-026
depend on that shared contract and cannot activate their identity-bearing
formats until RFC-028 is accepted and implemented. An implementation may phase
an accepted RFC internally, but it cannot silently promote one section of a
draft into a shared format contract.

## Blocking findings

### BLOCKER-01 — native graph-branch control is multi-phase

**Affected:** [RFC-022 §7](../rfcs/0022-unified-write-path.md#7-native-graph-branch-ref-control-protocol)

**Status:** Closed in specification and implementation on 2026-07-11.

At review time RFC-022 modeled branch create/delete as one native ref mutation whose
completion is the visibility point. The pinned Lance implementation is more
specific:

- [`Dataset::create_branch`](https://github.com/lance-format/lance/blob/f24e42c11a742581365e1cbe17c906ea2dac1bc6/rust/lance/src/dataset.rs#L488-L535)
  first commits a shallow-cloned branch dataset and only then writes
  `BranchContents`. Lance explicitly calls this a non-atomic two-phase
  operation and calls `BranchContents` the source of truth.
- [branch deletion](https://github.com/lance-format/lance/blob/f24e42c11a742581365e1cbe17c906ea2dac1bc6/rust/lance/src/dataset/refs.rs#L548-L584)
  removes `BranchContents` before cleaning the branch directory.

Therefore create can crash with a zombie branch dataset and no authoritative
ref. A retry with the same name can fail until the zombie is reclaimed. Delete
can make the branch logically absent and then return a cleanup error.

**Required disposition:** define an idempotent control-operation classifier for
at least these states:

| Operation | Physical state | Logical result |
|---|---|---|
| create | no clone, no `BranchContents` | not started |
| create | clone only | zombie; reclaim before retry, or wait for an upstream completion API |
| create | clone plus matching `BranchContents` | complete |
| delete | `BranchContents` present | not deleted |
| delete | `BranchContents` absent, directory remains | deleted; reclaim pending |
| delete | both absent | complete |

The pinned public API cannot adopt the clone-only state: `BranchIdentifier` is
generated later by Lance's private `BranchContents` creation phase. The RFC
must therefore say whether a durable operation marker/sidecar is written before
the shallow clone, how retry distinguishes its own zombie from foreign state,
and whether it reclaims and retries or waits for a new upstream completion API.
It must also explain why cleanup failure after authoritative deletion is not
reported as if the branch still existed. Add failpoints at both create phases
and between delete authority removal and directory cleanup.

> 💬 **Verified (2026-07-11):** confirmed verbatim in the pinned source —
> `dataset.rs:488` documents "This is a two-phase operation… These two phases
> are not atomic. We consider `BranchContents` as the source of truth… may
> leave a zombie branch dataset", including the zombie-blocks-retry
> consequence and the cleanup duties this finding transcribes. The 2026-07-11
> RFC-022 §7 revision (single-writer-process boundary + conditional-ref
> upstream ask) addressed the *conditional-put* gap but not this crash
> classification — the two dispositions compose; both are needed.

> ✅ **Disposition (2026-07-11):** RFC-022 §7 now makes `BranchContents` the sole
> logical authority, defines the bounded create/delete classifier, explicitly
> rejects a graph-branch sidecar/lineage entry, and retains the
> single-writer-process boundary. The implementation prevalidates names,
> requires live graph names to be physical-path-prefix-disjoint, reclaims an
> absent-ref clone before one bounded retry, accepts only a current-attempt
> matching lost acknowledgement, and fences delete by exact identifier. Full
> first-touch recovery also force-reclaims clone-only table residue under its
> already-durable writer sidecar. Coverage includes flat and named-source
> clone-only recovery, invalid-name-before-clone, lost acknowledgements,
> absent-ref/tree-present delete, same-identifier error preservation,
> recreated-identifier refusal, path-prefix collision, legacy no-effect
> leaf-first delete, mixed-effect rollback fail-closed behavior, and no
> manifest-version/lineage movement.

### BLOCKER-02 — create-if-absent ownership is not a fencing lease

**Affected:** [RFC-025 §2.3](../rfcs/0025-checkpoint-retention.md#23-cross-process-retention-claim)

**Status:** Closed in specification on 2026-07-14; subprocess death proof and
offline-repair implementation remain RFC-025 acceptance gates.

`PutMode::Create` correctly elects one initial owner. A random token in that
object does not by itself fence a paused owner, because Lance tag, branch, and
cleanup effects do not condition their commits on that token. The reviewed
RFC-025 proposed a check-then-delete release plus takeover. For that shape, the
race is:

1. old owner reads and verifies token A;
2. old owner pauses;
3. takeover removes A and creates token B;
4. old owner resumes and deletes B, or resumes a destructive physical effect.

“The stale process checks again” does not close the window after its final
check. Calling the random owner token a fencing token overstates the substrate
guarantee.

**Required disposition:** choose one honest contract:

- no takeover while the prior process can resume; operator recovery proves the
  process/host is dead before removing the claim;
- a substrate-enforced lease/fencing primitive whose monotonically newer token
  is checked by every protected effect, plus conditional compare-and-delete;
- or a non-takeoverable claim that requires explicit offline repair.

The `StorageAdapter` does not currently expose conditional delete, and its local
`write_text_if_match` is not a cross-process CAS. Any selected design must work
on local and S3 or explicitly narrow the supported backend/topology. For a
substrate-enforced lease, tests pause an owner after its last token check,
perform takeover, then resume it; the old owner must be unable to mutate state
or release the new claim. A host-death or non-takeoverable design instead proves
that takeover is refused until external fencing/death proof is established.

> 💬 **Concur (2026-07-11):** the paused-owner race is real and the "fencing
> token" label overstated the guarantee. Given the substrate as it stands (no
> conditional delete on the adapter; the documented local `write_text_if_match`
> gap), the only honest near-term contract is the boring one — no takeover
> while the prior process can resume; operator proves process/host death or
> uses explicit offline repair. The substrate-enforced lease is the right end
> state but should be written as an upstream/adapter work item, not assumed.

**Disposition:** RFC-025 now calls the value an owner token, makes a crashed
claim non-takeoverable, and refuses stale-claim removal until an operator proves
the prior process/host cannot resume or runs offline repair after revoking that
ability. A fleet outage, elapsed time, and token recheck are explicitly
insufficient. Acceptance tests pause a live owner and require takeover refusal,
then exercise removal only after subprocess death/offline isolation.

### BLOCKER-03 — key-conflict retry must first resolve partial effects

**Affected:** [RFC-022 §9](../rfcs/0022-unified-write-path.md#9-concurrency-and-retry-semantics),
[RFC-023 §6](../rfcs/0023-key-conflict-fencing.md#6-retry-and-validation-semantics),
and [RFC-026 §6](../rfcs/0026-memwal-streaming-ingest.md#6-fold-protocol)

**Status:** Closed in specification on 2026-07-14 and in the RFC-023
implementation on 2026-07-15. RFC-026 failpoint evidence remains an acceptance
gate for that draft.

A retryable concurrent inserted-row-filter conflict is detected when a table
transaction attempts to commit. (`WhenMatched::Fail` may instead report a
pre-existing match during merge execution.) In a multi-table graph write or
MemWAL fold, table A may already have advanced under the armed sidecar before
table B reports the concurrent key conflict. Immediately “restart the entire
logical attempt” would replan around unresolved physical state, which RFC-022
forbids.

**Required disposition:** distinguish:

- a conflict before any physical table commit — finalize the already-durable
  empty sidecar, then perform a safe full semantic restart;
- a conflict after any physical commit — keep the sidecar and resolve the
  RFC-022 recovery outcome first. Normally the partial attempt must roll back
  before a new semantic attempt can be prepared. If rollback is not safe, return
  a typed recovery-required failure rather than retrying around it.

For strict insert, finalize a proven-empty intent and return the terminal
`KeyConflict`; if an earlier participant advanced or emptiness is not provable,
retain the sidecar and return `RecoveryRequired` instead. Do not start another
attempt around that sidecar. For non-strict upsert, the barrier must resolve the
sidecar before the bounded automatic whole-operation retry. Add a failpoint/race
in which table N conflicts after table 1 has committed.

> ✅ **RFC-022 disposition (2026-07-11):** mutation/load take the conservative
> safe branch of this requirement. A pre-effect authority mismatch discards and
> fully reprepares the attempt. Once the exact-effect sidecar is durable and the
> commit phase begins, every Lance commit failure returns `RecoveryRequired` and
> leaves that sidecar intact, even when Full recovery later proves that no table
> effect landed. The adapter performs zero transparent Lance commit retries and
> never prepares a new semantic attempt around unresolved ownership; the next
> synchronous barrier classifies and resolves it first. Finalizing a proven-empty
> intent and automatically retrying would improve ergonomics, but is not required
> for safety. RFC-023's fenced key-conflict contract and RFC-026's MemWAL fold
> protocol still need their adapter-specific resolutions.

**RFC-023/RFC-026 disposition:** RFC-023 §6 now permits a bounded whole-operation
retry only before sidecar arm or after exact classification proves the armed
intent effect-free and finalizes it. Any landed or unclassifiable participant
retains the sidecar and returns `RecoveryRequired`; strict insert applies the
same recovery precedence and never changes mode. RFC-026 §6 applies that rule
to folds and forbids replanning `merged_generations` around unresolved state.
Both RFCs require the table-N-after-table-1 failpoint before acceptance.
RFC-023 now has both Mutation/Load adapter-specific cells:
`rfc023_effect_free_conflict_is_typed_or_fully_reprepared` proves strict
`KeyConflict` versus a fresh upsert reprepare, and
`rfc023_table_n_conflict_after_table_1_keeps_recovery_ownership` proves the
partial-effect `RecoveryRequired` outcome with the exact sidecar retained.
This normalization does not extend to BranchMerge's internal strict chunks:
once its exact `protocol_v4` chain is armed, any chunk conflict—including the
first chunk before a merge-owned effect lands—retains recovery ownership and
returns `RecoveryRequired` without a semantic merge retry.
Nor is Lance's broad retryable class alone proof of a key collision. After an
effect-free Mutation/Load intent is retired, strict insert re-probes the
attempted IDs against fresh manifest-visible authority; only an exact match is
`KeyConflict`, while no match is an internal typed read-set conflict that causes
bounded full strict-mode reprepare.
RFC-026 must supply the equivalent evidence for folds when implemented.

### BLOCKER-04 — live graph branches need physical GC protection

**Affected:** [RFC-025 §6](../rfcs/0025-checkpoint-retention.md#6-cleanup-protocol-and-pruned-through-boundary)

**Status:** Closed in the shipped baseline and RFC-025 on 2026-07-11.

The cleanup root set includes live graph branches, but RFC-025 creates Lance
tags only for named checkpoints. This is insufficient for lazy graph branches.
A graph branch may store, in its `__manifest`, a foreign reference to an old
version on a data table's `main` branch. Lance cleanup on that data table cannot
see the foreign OmniGraph row; it protects versions through its own branch and
tag references.

**Required disposition:** for every live graph-branch table reference, either:

- materialize a deterministic Lance tag/native ref that physically protects the
  exact `(table branch, version)`; or
- choose a per-dataset cleanup cutoff no newer than the oldest live reference,
  accepting that all intervening versions remain retained.

Checkpoint tags alone do not solve this. Add a test where a lazy branch pins an
old main-table version, main advances beyond the retention window, cleanup
runs, and the lazy branch still opens and reads the exact pinned state.

> 💬 **Escalation (2026-07-11):** this is very likely a live bug in the shipped
> `cleanup`, independent of RFC-025. Verified in code: `cleanup_all_tables`
> (`optimize.rs:802`) runs Lance `cleanup_old_versions` per data table with no
> lazy-branch pin logic, and a lazy graph branch creates **no Lance-native ref
> on the data table** until its first write to that table — its pin exists only
> as a foreign `__manifest` row Lance cannot see. Repro shape: create a branch,
> never write table X on it, advance main past the keep window, `cleanup
> --keep N` → the branch's pinned version of X is collected and the branch
> breaks. Per the repo's test-first rule this deserves a red regression test
> and an issue *now*; RFC-025's disposition should then build on that fix
> rather than owning the discovery. (Snapshot/time-travel pins share the
> mechanism but are history-trimming under an operator-confirmed policy; a
> live branch's working state is not history.)

> ✅ **Disposition (2026-07-11):** shipped cleanup now resolves main plus every
> live non-system graph branch under schema → all-branch → all-table gates,
> verifies every exact inherited-main version opens, and caps each dataset's
> cutoff at the oldest such pin before the first table GC. It also derives
> `keep=N` from Lance's actual ordered version list and refuses uncovered main
> HEAD drift. Unclassifiable roots abort graph-wide; only later per-table GC
> failures are fault-isolated. Regression coverage pins count- and time-based
> retention, the oldest of multiple live pins, exact keep counts, graph-wide
> fail-closed ordering, and uncovered drift. RFC-025 §6 now composes checkpoint
> tags with this required lazy-branch floor rather than treating tags as a
> replacement.

### BLOCKER-05 — durable-head OCC must compare the full head token

**Affected:** [RFC-024 §5](../rfcs/0024-durable-table-heads.md#5-atomic-publisher-contract)

**Status:** Closed in specification and public-substrate proof on 2026-07-15;
publisher-level stale-writer rejection remains unimplemented because Gate A
rejected the heads format.

The RFC says expected table versions are compared against table heads. Lance
version numbers can repeat across recreated datasets/branches, so version-only
comparison admits drop/recreate ABA.

**Required disposition:** define the OCC token as the complete logical head
identity, at least:

```text
(state, stable_table_id, incarnation_id, table_path,
 table_branch, current_head_witness, table_version, schema_ir_hash)
```

The publisher may encode that token differently, but retry/revalidation must
not reduce it to `table_version`. `current_head_witness` is the public
`BranchIdentifier` + current table version + current `Transaction.uuid` +
`ManifestLocation.e_tag` composite. Capture brackets transaction/e_tag
collection with branch-identifier reads and rejects movement; missing/empty
transaction identity fails closed. It is intentionally mutable: every ordinary
table commit advances it. This is distinct from the logical `incarnation_id`,
which RFC-028 deliberately preserves across an owner handoff. Add stale-writer
races for both logical drop/recreate and physical ref recreation at the same
numeric version.

**Disposition:** RFC-024 §3.2 now persists the separate current-HEAD witness and
refuses activation on a backend without a proven source. Pinned-Lance local manifest
resolution supplies an inode-mtime-size e_tag; S3/RustFS supplies the object
e_tag. Public-surface guards pass main and named-ref same-version ABA on both,
unchanged reopen stability on S3, and original-shared-`Session` local
recreation. Main's canonical empty branch identifier makes UUID/e_tag
load-bearing; named refs also change branch identifier. Raw byte-identical or
forged replacement is outside the supported writer topology, so the token is
not authentication. §5 still requires the unbuilt publisher to compare the
full token and reject stale writers; substrate proof is not production
integration.

### BLOCKER-06 — MemWAL needs a capability/format activation barrier

**Affected:** [RFC-026 §3](../rfcs/0026-memwal-streaming-ingest.md#3-enrollment-is-a-recoverable-multi-effect-adapter),
[§5](../rfcs/0026-memwal-streaming-ingest.md#5-ack-path-validation-and-writer-lifecycle),
[§6](../rfcs/0026-memwal-streaming-ingest.md#6-fold-protocol),
[§8](../rfcs/0026-memwal-streaming-ingest.md#8-epoch-fenced-quiescence-barrier),
[§9](../rfcs/0026-memwal-streaming-ingest.md#9-fresh-read-cuts),
and [§11–13](../rfcs/0026-memwal-streaming-ingest.md#11-format-activation-and-rebuild)

**Status:** Closed through private Phase B1 implementation and evidence on
2026-07-19; public B2 activation remains open. RC.1 lacks the ideal caller-owned enrollment
receipt and cross-process shard-admission seal, but upstream release timing is
not the bounded-profile calendar. Gate E0's exact-classification and ABA cells
passed using known-version probes and strict object-store classification.
Phase A then activated a main-only, unsharded, single-live-writer-process empty
enrollment foundation in internal schema v7. Private B1 activated the bounded
data-bearing schema-v8/config-v2 core. RFC-026 remains draft and public row
streaming remains inactive.

Enrollment creates persistent MemWAL metadata and `stream_state` changes the
correctness preconditions for schema, branch, maintenance, and data operations.
An older binary that understands key fencing but not stream lifecycle can ignore
`OPEN | DRAINING | SEALED`, mutate the base table, or perform a schema/branch
operation without draining acknowledged rows.

**Format disposition:** RFC-026 §11 makes the stream foundation a graph-format
capability present at new-root initialization, never a feature stamp added by
first enrollment. Phase A implemented schema v7, recovery-v10
`StreamEnrollment`, lifecycle CAS, compatible-open validation, partial-format
refusal, and genuine v6↔v7 rebuild. The contract includes:

- a graph capability/internal-format stamp written only after every enrolled
  table and lifecycle authority is valid;
- old-binary/new-format and new-binary/partial-format refusal behavior;
- strict export/init/load rebuild and source/target failure isolation;
- preservation rules for later heads/retention formats;
- genuine cross-version tests, not a stamp-rewind simulation.

B1 is a second format activation, not an in-place reinterpretation of the v7
empty-enrollment contract: implemented v8 is the first data-bearing stream
format, uses stream-config v2 and recovery schema v11, refuses v7/config-v1,
and carries genuine v7↔v8 rebuild/refusal evidence.

RFC-026 §12 records those tests and §13 places the completed Phase A before the
public stream path. RFC-026 §3/§8 also make a rematerialized table a separate physical
enrollment boundary: SchemaApply leaves the preserved logical identity
`SEALED`, and only an exact sidecar-covered rebind with a fresh enrollment and
shard namespace may publish `OPEN`. Recovery never infers adoption from the
logical pair, name, path, or a compatible-looking index.

**Remaining required disposition:** in RC.1,
`InitializeMemWalBuilder::execute` internally commits the `CreateIndex`
transaction and returns only `Result<()>`; `mem_wal_writer` separately creates
or claims shard-manifest objects. That cannot satisfy a sidecar that pre-arms
one caller-minted combined effect, so the public receipt plus seal/reopen API
remains the preferred simplification and the gate for overlapping processes.
RFC-026 still rejects private-module/object-store emulation. That missing
combined receipt gates broader topology, not private B1 inside the implemented
process boundary. At the row boundary, `put_no_wait` supplies an optional
status-only durability watcher. RC.1's watcher watermark is writer-wide while
`WriteResult.batch_positions` reset with the active MemTable, so a later
generation can falsely resolve from an earlier watermark. `put_no_wait` can
also mutate before returning a flush-scheduling error. B1 therefore cannot
interpret an error after invocation as effect-free or use one writer across
rollover. RC.1 replay also rebuilds a fresh BatchStore without advancing its
per-MemTable WAL-flush watermark, so a later put or plain reseal re-appends and
re-indexes the replayed prefix; repeated pre-manifest crashes can multiply it.
And a late `wait_for_flush_drain` can observe an empty watcher queue after a
failed handler removed the watcher. Both are explicit B1 compatibility gates,
not inferred success.

The implemented private design is a root-scoped singleflight worker and one
hard-bounded generation per writer: internal schema v8/config-v2, no automatic
rollover, at most 8,192 rows/32 MiB for the complete generation, explicit
admission from before epoch claim, conservative fold-only replay/unmerged-state
routing, the pinned public BatchStore watermark bridge before replay reseal,
drain proof from empty frozen refs plus the authoritative generation/cursor,
immediate writer retirement, one recovery-v11 keyed fold, and higher-epoch
reopen only after publication. `AckUnknown` preserves possible
residue through replay but remains permanently caller-ambiguous. B2 later owns
public strict activation and cannot start merely because B1 is green: it also
needs durable authenticated-contributor attribution, bounded reclamation with a
retained-storage stop and derived-orphan cleanup, strict correction/disposition,
same-key retry sequencing/idempotency, and persistent
status/quiesce/resume/abort-drain/rebuild. A `SEALED` B2 stream permits
export/rebuild; in-place maintenance still waits for Phase D.

The bounded candidate instead makes the support restriction load-bearing. It
grants an `OPEN` stream exclusive authority to advance that base-table HEAD;
every other writer/maintenance/control path touching the table must refuse
pre-effect or drain. Stable enrollment identity is kept separate from the
mutable `CurrentHeadWitness` (`BranchIdentifier`, current table version,
transaction UUID, manifest e_tag), which advances atomically with every
allowed table-pointer publish. A root-scoped admission lease closes the final
claim-to-check and check-to-put races only inside the one live writer process;
the shared lease begins before `mem_wal_writer` can advance the shard epoch.

Gate E0 must classify exact no effect, the sole `N -> N + 1` singleton MemWAL
`CreateIndex` carrying the namespaced enrollment/config-version marker, and
that successor plus the pre-minted empty shard; wrong config, intervening HEAD,
foreign shard, data-bearing WAL/generation state, or ambiguity must yield
`RecoveryRequired` without deletion. It also proves unchanged-reopen
witness stability, ordinary-commit movement, local/S3 same-coordinate ABA, and
history-depth cost. The first cost attempt observed a flat tracked GET count but
missed local filesystem `read_dir` work outside `IOTracker` and was discarded.
The accepted replacement freshly ABA-verifies exact `N`, uses the public but
guide-hidden `Dataset::has_successor_version` to probe only `N + 1`, then uses
the exact `N + 1` handle to reject a buried `N + 2`. Exclusive HEAD and
cleanup/version-GC exclusion remain held through decision/publish; only
`Ok(false)` means absence, while errors, overflow, and detached boundaries fail
closed.

The final local run has 14 substantive cells plus one explicit S3 skip. Its
attempt-counting wrapper includes failed/`NotFound` HEADs and records the same
six-attempt, zero-list shape at baseline versions 8 and 80; a Unix permissions
tripwire proves exact probes succeed when latest enumeration fails and that an
unreadable exact HEAD errors. The configured RustFS cell passes non-vacuously
with the same shape plus the declared positive and listing-dependent negative
matrix. Surface guards pin the doc-hidden successor, flush/drain,
merged-generation, and S3 ABA shapes; CI rejects skipped E0/ABA cells. This was
green evidence for the bounded profile; Phase A subsequently consumed it, but
it did not authorize a row acknowledgement.

Replica scope must match RFC-023's recovery support boundary. The bounded
profile does not permit overlapping replicas: a crash successor proceeds only
after external exclusivity, recovery, and a higher shard epoch. Do not
advertise general replica failover merely because MemWAL has a shard epoch.

### BLOCKER-07 — stable identity ownership contradicts sibling dependencies

**Affected:** [RFC-023 §4.1](../rfcs/0023-key-conflict-fencing.md#41-keyed-graph-tables),
[RFC-024 §3.1](../rfcs/0024-durable-table-heads.md#31-identity-and-object-key),
[RFC-025 §2.1](../rfcs/0025-checkpoint-retention.md#21-logical-authority-manifest-rows),
[RFC-026 §2](../rfcs/0026-memwal-streaming-ingest.md#2-stream-mode-and-key-semantics),
[RFC-026 §7](../rfcs/0026-memwal-streaming-ingest.md#7-fold-time-rejection-is-atomic),
and [RFC-026 §8](../rfcs/0026-memwal-streaming-ingest.md#8-epoch-fenced-quiescence-barrier),
with the shared contract in [RFC-028](../rfcs/0028-stable-schema-identity.md)

**Status:** Closed in implementation on 2026-07-15. RFC-028 was activated by
SchemaIR v2 and internal manifest schema v5 and remains intact in v6, v7, and v8, every
writer/recovery envelope, and identity-derived table path. Identity-consuming
siblings may rely on that contract while retaining their independent activation
gates.

The reviewed drafts had RFC-024 exclusively owning stable table identity and
incarnation while RFC-025 called heads optional and RFC-026 persisted a stable
table ID without declaring that dependency.

**Disposition:** RFC-028 now exclusively owns graph-scoped type/property IDs,
table incarnation, allocation, SchemaApply recovery, and strict rebuild. RFC-023
through RFC-026 declare it as the prerequisite; RFC-024 owns only durable-head
storage and lookup. RFC-023 uses the stable pair for keyed-table format
authority, and RFC-026 distinguishes that preserved logical pair from its
separately fenced physical MemWAL enrollment.

RFC-026's `_ingest_rejects` deterministic key now uses stable table ID plus
incarnation rather than mutable `table_key`. If a future explicit
rematerializing-rename capability is added, it must mint a fresh shard namespace
under an exact sidecar-covered rebind. Thus
logical history ownership survives rename, physical WAL artifacts are never
adopted by implication, and drop/re-add cannot claim a predecessor's rows.

The same correction keeps logical identity separate from physical ABA proof.
RFC-025 now owns a heads-independent exact manifest/table version token,
backend refusal, recovery/reconciler checks, and local/S3 same-coordinate ABA
tests. RFC-026 owns a stable enrollment identity plus a separately mutable
`CurrentHeadWitness`; it does not pretend one token can be both stable across
fold commits and sensitive to current-HEAD movement. Phase A closed
BLOCKER-06's format-foundation requirement and private B1 closed its bounded row
correctness/evidence gate; RFC-026 remains draft because B2 public/operational
gates are still open. Neither
silently imports RFC-024's head storage merely to obtain those proofs.

> 💬 **Concur; supersedes a 2026-07-11 correction (2026-07-11):** the earlier
> pass placed identity ownership *inside* RFC-024 §3.1, which created exactly
> the contradiction described here (RFC-025 calls heads optional while
> consuming the identity RFC-024 claims to own). The extracted
> identity/incarnation RFC is the cleaner shape and should replace that
> ownership note. The `_ingest_rejects` key observation is an additional catch
> the earlier pass missed — endorsed.

### BLOCKER-08 — retention activation cannot precede migration selection

**Affected:** [RFC-025 §8](../rfcs/0025-checkpoint-retention.md#8-format-activation-and-rebuild-compatibility)
and [§11](../rfcs/0025-checkpoint-retention.md#11-phasing)

**Status:** Closed in specification on 2026-07-14; rebuild implementation and
cross-version evidence remain RFC-025 acceptance gates.

The reviewed draft left in-place migration versus export/import undecided while
putting format activation before migration selection.

**Disposition:** RFC-025 §8 selects strict export/init/load rebuild before Phase
A, defines old/new-format refusal and co-release ordering, and names the loss of
branches not separately rebuilt, commit DAG, snapshots, tombstones, recovery
history, time travel, and historical checkpoints. Its phasing now puts the
format/refusal/rebuild contract in Phase A.

### BLOCKER-09 — decide whether these are public or internal RFCs

**Affected:** [RFC process](../rfcs/README.md), RFC-022 through RFC-028

**Status:** Closed in process and documentation on 2026-07-13.

At review time the files lived in a directory defined exclusively as the public
track, while using internal-style `rfc-022-*` names, `draft` and
`research-blocked` statuses, and empty owner metadata. That made merge mean both
“accepted” and “still under review.”

**Disposition:** keep formal RFCs together in `docs/rfcs/` and use one
four-digit `NNNN-title.md` namespace. The filename identifies the RFC; explicit
metadata selects its lifecycle:

- `Author track: Public contribution` uses the public template and status
  vocabulary. Anyone may author it, and merge means maintainer acceptance.
- `Author track: Maintainer design series` permits `draft`,
  `research-blocked`, `accepted`, `implemented`, and `superseded` as
  machine-readable statuses. A merged revision is durable review state, not
  automatic acceptance.
- Every RFC declares an explicit status. If YAML frontmatter and rendered
  metadata are both present, they agree.

RFC-022 through RFC-028 now identify the maintainer design-series track and an
owner. RFC-022 and RFC-028 are `implemented` at their documented support
boundaries, RFC-023 is `implemented`, RFC-024, RFC-025, and RFC-027 are
`research-blocked`, and RFC-026 remains `draft`.
All formal RFC filenames in the central directory are normalized to four
digits. Legacy `docs/dev/rfc-00N-*` files remain in place with their existing
lifecycle; they are internal design and implementation records, not members of
the formal RFC filename namespace.

> 📝 **Historical review record (2026-07-11):** the original disposition offered
> two choices under the then-current process: normalize the files into the
> public track and resolve every blocker before merge, or move the in-flight
> series to `docs/dev/`. The reviewer recommended the latter as the
> lower-friction choice. Maintainers instead selected a third option: keep one
> RFC directory, distinguish lifecycles through explicit `Author track` and
> `Status` metadata, and normalize every formal filename to four digits. The
> public merge-equals-acceptance rule was preserved rather than weakened.

> ✅ **RFC-022 implementation close-out (2026-07-13; identity envelope activated
> 2026-07-15):** every currently supported graph-write surface is enrolled in an
> exact adapter, the bounded Optimize payload, or an explicit authority/physical/bootstrap/
> recovery exception. Rust visibility keeps the supported SDK set closed: raw
> storage/coordinator/handle-cache modules are crate-private and public snapshots
> expose a read-only table/scan facade without Lance's raw scanner or physical
> plan. `crates/omnigraph/tests/forbidden_apis.rs` adds a
> defense-in-depth registry over public async inherent `Omnigraph`/loader
> surfaces, crate-visible low-level coordinator methods, and exact per-file
> occurrences of registered durable-call shapes including recovery. Exact Optimize
> provenance is intentionally deferred until both a stable public Lance
> maintenance-transaction API and distributed recovery fencing exist. Together
> with the lifecycle disposition above, this closes RFC-022 and marks it
> implemented at the documented single-writer-process support boundary. It does
> not claim distributed destructive recovery or make exact Optimize provenance
> a rollout gate.

## Protocol clarifications

### BLOCKER-10 — do not put foreign-branch facts in an atomic `ReadSet`

**Affected:** [RFC-022 §6.2](../rfcs/0022-unified-write-path.md#62-branch-merge)

**Status:** Closed in specification and implementation on 2026-07-11. Branch
merge captures an immutable source commit and revalidates only its incarnation;
the target publishes and recovers under its own exact authority token.

RFC-022 requires every `ReadSet` member to be arbitrated atomically by the
publish CAS. A CAS on reserved main cannot arbitrate a row on a named source
branch; a merge-target CAS cannot arbitrate a source-branch row.

Use the right category for each fact:

- checkpoint creation captures an immutable source version, revalidates it
  before tagging, and then relies on the physical tag. A later source-head
  advance is intentionally harmless, so the source head is an effect
  precondition, not target-CAS authority;
- offline cleanup facts are protected by the selected fleet/retention barrier,
  not by pretending one main-branch CAS covers every named branch;
- branch merge should define captured-source-commit semantics. If “latest
  source at target publish” is required instead, it needs a real source-branch
  fence held through the target CAS.

Keep target-branch values that must remain stable in the atomic `ReadSet`.

> ✅ **Disposition (2026-07-11):** RFC-022 §6.2 defines the exact captured source
> commit/snapshot as an immutable effect precondition, not a member of the
> target publisher's atomic `ReadSet`. A later source-head advance is harmless
> under captured-source semantics; only a future latest-at-target-publish
> contract would require a source fence through target CAS. The shipped
> active schema-v9 envelope's retained `protocol_v4` exact adapter preserves
> that captured-source contract while
> revalidating source incarnation before effects.

### BLOCKER-11 — RFC-022 and no-heads MemWAL need coarse OCC

**Affected:** [RFC-022 §10](../rfcs/0022-unified-write-path.md#10-rollout-and-compatibility)
and [RFC-026 §6](../rfcs/0026-memwal-streaming-ingest.md#6-fold-protocol)

**Status:** RFC-022 is closed in the shipped adapter. RFC-026 is closed in
specification on 2026-07-14; its no-heads concurrency/failpoint evidence remains
an acceptance gate.

[RFC-022's rollout](../rfcs/0022-unified-write-path.md#10-rollout-and-compatibility)
said at review time that mutation/load read-set arbitration was likely blocked
on RFC-024 because probed-but-untouched tables lacked mutable head rows.

For graph-content writes, `(branch incarnation, optional graph head)` is already
a conservative branch-wide authority token. An established branch changes its
`graph_head:<branch>` row on every graph commit; a fresh branch may initially
have no branch-specific head row, so absence plus incarnation is part of the
captured token. Schema identity needs its own atomically contended authority.
Any concurrent logical graph change forces full revalidation. RFC-024 table
heads can later reduce false contention by narrowing that token to the tables
actually read.

Today's publisher retries head-row contention by re-reading the live head and
reparenting the prepared write. That is insufficient for a
validation-sensitive plan. On every publisher retry, it must compare the
captured token and return control to full revalidation rather than reparenting
and continuing with stale validation.

The lower-liability rollout is therefore:

1. ship correct coarse OCC with `graph_head` plus schema identity;
2. introduce table-head tokens only after RFC-024 passes its independent format
   and cost gates.

The no-heads RFC-026 fold path must carry the same captured branch token in its
`ReadSet`. Do not recouple RFC-022 or RFC-026 correctness to RFC-024
performance.

> 💬 **Concur; supersedes a 2026-07-11 correction (2026-07-11):** the premise
> is independently verified — `graph_head:<branch>` is one mutable row updated
> in every graph-content publish (the deliberate contention point pinned by
> the concurrent-disjoint-writers tests), so the coarse token genuinely
> arbitrates probed-but-untouched same-branch tables. This is a better design
> than the earlier RFC-022 §10 wording ("likely blocked on RFC-024"), which
> should be revised to the two-step rollout described here. One cost worth
> stating in the revision: coarse OCC makes *any* concurrent commit on a busy
> branch force full revalidation of in-flight writers — real throughput cost
> on agent-fleet branches, and precisely the honest motivation for RFC-024's
> later narrowing (rather than a correctness argument for it).

**Disposition:** shipped RFC-022 mutation/load and BranchMerge retain their
documented coarse branch/schema authority. RFC-026 §6 now captures
`(native branch incarnation, optional graph_head)` plus schema identity and the
complete stream/probe cut, and requires every publisher retry to return to full
fold revalidation on a change rather than reparenting. Its acceptance suite
races a concurrent commit on a probed-but-untouched table and requires that
revalidation without RFC-024.

> **Implementation disposition (2026-07-11):** mutation/load now capture the
> native Lance `BranchIdentifier`, exact optional `graph_head`, and accepted
> schema identity; revalidate under a branch-then-table gate; and pass the same
> token to every publisher retry and the active schema-v9 envelope's retained
> `protocol_v3` recovery decision. Metadata-only
> schema-apply tests pin the required invariant that supported schema changes
> move `graph_head` even when no data-table version changes. This closes the
> coarse mutation/load cell without claiming a general schema-authority row or
> multi-process native-ref fencing. RFC-024 remains the false-contention
> narrowing step.

> **Branch-merge implementation disposition (2026-07-11):** branch merge uses
> the same coarse target token without pretending the source belongs to that
> atomic read set. The active schema-v9 envelope's retained `protocol_v4`
> recovery payload persists the captured source parent,
> fixed merge/rollback ids, pre-minted exact transaction chains for multi-commit
> data effects, ref-only physical effects, and the complete logical delta. A later source advance remains harmless; a target
> advance after effects becomes `RecoveryRequired` and is compensated rather
> than re-parented.

### BLOCKER-12 — in-place format conversion contradicted the strand model

**Affected:** [RFC-023 §7–8](../rfcs/0023-key-conflict-fencing.md#7-format-boundary-and-rebuild-cutover),
[RFC-024 §9–11](../rfcs/0024-durable-table-heads.md#9-heads-format-boundary-and-compatibility),
[RFC-025 §8](../rfcs/0025-checkpoint-retention.md#8-format-activation-and-rebuild-compatibility),
[RFC-026 §11](../rfcs/0026-memwal-streaming-ingest.md#11-format-activation-and-rebuild),
and [RFC-028 §8](../rfcs/0028-stable-schema-identity.md#8-format-activation-and-upgrade)

**Status:** Closed in specification on 2026-07-14. RFC-023 assigned and
implemented internal schema v6; its genuine v5↔v6 rebuild and bidirectional
refusal run passed on 2026-07-15. RFC-026 Phase A subsequently assigned v7 and
passed its genuine v6↔v7 rebuild/refusal evidence. Every future format still
requires its own implementation and genuine evidence.

The reviewed RFC-023 and RFC-024 drafts designed all-branch, in-place conversion
protocols even though OmniGraph's strict-single-version strand intentionally has
no migration dispatcher or compatibility reader. That would create permanent
claims, ledgers, crash recovery, and branch traversal solely to preserve a
transition model the project had already rejected.

**Disposition:** RFC-023 through RFC-026 and RFC-028 now use one rule. A format
capability is complete at new-graph initialization; old and new binaries refuse
the opposite format; an existing graph moves by quiescing, exporting current
logical state with the old binary, initializing a different root with the new
binary, and loading through RFC-022. No source graph is mutated in place.
Capabilities may co-release to reduce cutovers only after independent
acceptance and a combined initialization/recovery matrix. The rebuild's loss of
unselected branches and history is explicit rather than hidden behind the word
“migration.”

### BLOCKER-13 — checkpoint authority lookup is not physically history-flat

**Affected:** [RFC-025 §0.1](../rfcs/0025-checkpoint-retention.md#01-pre-activation-gate-0),
[§9](../rfcs/0025-checkpoint-retention.md#9-observability-and-bounds), and
[§11](../rfcs/0025-checkpoint-retention.md#11-phasing)

**Status:** Research-blocking no-go recorded on 2026-07-17. Current production
schema v8 still activates no retention format or checkpoint API.

The production-neutral `checkpoint_retention_cost.rs` fixture holds three live
checkpoints and catalog width ten fixed while unrelated journal history grows.
It measures complete list, exact show, and cleanup-root reads from cold open and
warm repeat across compacted/uncompacted layouts and absent, reconciled, and
eight-fragment uncovered-tail index states.

The local 10→100→1,000 decision run separates a real access-shape win from the
required history-slope claim:

- reconciled uncompacted list is flat at 3 rows / 3 ranges / 1 fragment / 1
  index page and 24 scan operations / 13,752 bytes; exact show is flat at
  12 / 2 / 2 / 3 and 34 operations / 22,952 bytes; cleanup returns 44 rows with
  list-like cost, and the bounded tail remains exact and history-flat;
- after compaction, list/cleanup cold scan bytes grow 17,012→38,000 and warm
  bytes 12,336→15,064; exact-show cold bytes grow 29,348→53,064 and warm bytes
  24,672→30,128. At depth 1,000 the one-scan paths cross 24→25 scan operations
  and show crosses 34→35.

The default 20/80 test passes its no-go-preservation assertions. That is a
successful instrument reproducing a failed design gate, not acceptance. The
S3/RustFS cost cell exists and remains bucket-gated; it was skipped locally and
is not credited as evidence. One required local compacted cell already fails,
so the missing S3 run cannot turn this result green.

The substrate itself is not the blocker. All 23 RC.1 Lance surface guards pass; the
RFC-025 cells prove exact main/named-branch tag targets, sparse cleanup pin and
unpin, and that tags do not protect a named branch tree. This supports the
checkpoint-row/Lance-tag architecture and keeps the branch-deletion guard
load-bearing. It does not make the surviving registry authority read
history-flat.

**Required disposition:** retain `__manifest` as the only checkpoint authority
and find a history-flat current-authority lookup shape, or revise the
operational contract with measured evidence and explicit user-visible cost.
Do not weaken the physical-I/O gate, infer success from flat row/range counters,
or add a second authority dataset whose rows cannot share the main-manifest
CAS.

### TIGHTENING-01 — symmetric Lance conflicts are not obviously an activation gate

**Status:** Closed in implementation and acceptance on 2026-07-15. The beta.21
substrate check, v6 production routing proof, genuine v5↔v6 cross-version
evidence, small-upsert/one-ceiling substrate cells, and corrected production
BranchMerge latency/RSS series are green.

RFC-023 correctly identifies Lance's directional filtered/unfiltered behavior.
However, its own mandatory fleet outage, recovery drain, historical-duplicate
validation, stamp-last activation, and old-binary refusal are intended to make
unfiltered writers unreachable after activation.

At the time of this finding, the RFC-023 routing table still permitted a direct
existing-row `Update`, and the emitted filter shape had not yet been measured.
The required disposition was therefore to prove that every insertion-bearing
keyed path routes through the filtered adapter and that a matched-only update
cannot insert a row or change `id`; affected-row conflict metadata must continue
to cover updates to the same existing row. The beta.21 measurement and v6
routing closure recorded below satisfy that disposition.

After that narrowing, upstream symmetry is useful defense in depth and a
valuable Lance surface guard, but it need not block activation for transaction
pairs that cannot violate key insertion. Two old unfiltered writers are not
protected by symmetry in either case.

> 💬 **Concur, with the deciding check named (2026-07-11):** the concrete
> unfiltered-`Update` reachable after activation is the matched-only
> partial-schema update merge (`WhenMatched::UpdateAll` +
> `WhenNotMatched::DoNothing`, the field-level-updates path in PR #342): it
> classifies zero inserts, so whether it emits an **empty** filter (`Some`
> with no keys — symmetric-safe) or **no** filter (`None` — the reachable
> unfiltered transaction) is exactly what decides whether this demotion is
> sound. That is a one-test question against the pinned revision and should be
> pinned as a surface guard before the demotion is accepted.

> 🧪 **Beta.21 evidence (2026-07-14):** with `use_index(false)`, the matched-only
> partial-schema UpdateAll + DoNothing shape emits `Some` with a semantically
> empty Bloom filter, not `None`. That closes the narrow substrate question in
> favor of a possible demotion. It does **not** close activation: the
> scalar-indexed/default-v1 path still emits `None`, keyed Append remains
> reachable in today's engine, and beta.21 still permits unfiltered Update or
> Append to land second after a filtered Update. Internal schema v6 introduced
> the routing closure, and current v8 preserves it: every production
> insertion-bearing graph path uses
> the exact-`id`, forced-v2 keyed adapter, generic Append is test-only, and the
> adapter verifies the emitted field-ID filter. Upstream symmetry is therefore
> defense in depth rather than an activation prerequisite.

## Specification and acceptance tightening

These are smaller than the blockers above, but should be resolved before the
affected RFC is accepted or implemented:

1. **Checkpoint-name normalization (closed in specification and reference test
   on 2026-07-17):** RFC-025 defines V1 as an ASCII-only, case-sensitive,
   identity transform over 1..=128 bytes with slash-separated nonempty
   `[A-Za-z0-9._-]+` segments, explicit path-like refusals, reserved `__` and
   `ogcp_` prefixes, a versioned reservation key, and no trim, Unicode
   normalization, or case folding. Future versions must use bounded exact
   cross-version collision checks and may not reinterpret V1 reservations.
2. **RFC lifecycle values (closed 2026-07-13):**
   [`docs/rfcs/README.md`](../rfcs/README.md) now uses one four-digit filename
   namespace and distinguishes the public contribution lifecycle from the
   maintainer design-series lifecycle through explicit `Author track` and
   `Status` metadata. `draft` and `research-blocked` are valid only in the
   latter; public merge remains acceptance.
3. **Version naming (closed 2026-07-15):** RFC-024 says “heads format”; RFC-028
   activated internal manifest v5. Sibling drafts name capabilities rather than
   assuming the next numeral.
4. **Capability ordering (closed in specification 2026-07-14):** RFC-025 and
   RFC-026 define independent-release rebuilds and combined initialization /
   recovery gates for co-release. Cleanup continues to require persistent
   quiescence whenever streams are enrolled.
5. **Memory measurement (RFC-023 production bulk cell closed 2026-07-15;
   RFC-027 still pending):**
   `helpers::cost` measures I/O, not peak RSS. RFC-023's
   adopted-merge and RFC-027's lineage memory gates should use the subprocess
   `scenarios.rs` harness or an equivalent `wait4`/`ru_maxrss` instrument and
   name dataset sizes, baseline, cap, and pass threshold.

   > 💬 **Concur; catches a gap in a 2026-07-11 correction:** the RFC-023
   > §11.4 memory gate added that day states the bound ("peak memory bounded by
   > batch size") without naming an instrument that can measure it — this item
   > is what makes that gate enforceable rather than aspirational.

   RFC-023 now has a subprocess `wait4`/`ru_maxrss` scenario instrument for the
   indexed-target upsert scale curve, one exact filtered transaction at the
   Mutation/Load ceiling, and an embedding-bearing all-new adopt. The corrected
   adopt normal arm executes the full graph lifecycle and measured production
   `Omnigraph::branch_merge`; `--baseline` is explicitly only a labeled direct
   Lance Append comparator.

   The final production route admits its shortcut only from a complete
   persisted `omnigraph.insert_absence = "v1"` chain. StrictInsert mints a link
   after exact preflight; an all-new Upsert may mint one only when completed
   statistics prove one-attempt insert-only effects, with certification failure
   treated as an optimization miss. Every consumed link must bind the exact
   parent, filtered insertion-only `Update`, full nested schema field-ID
   preorder, and physical-row total. Under the final gates the source and
   existing target native ref incarnations are both revalidated. The output
   uses public `InsertBuilder` only to stage immutable fragments, replaces its
   uncommitted Append descriptor with another certified filtered `Update`, and
   therefore composes across later branch generations. It performs no target-ID
   preflight, target merge join, committed Append, general ordered diff, or
   temporary delta. A first-touch lazy target stays on the ref-only fork path;
   missing or unfamiliar history falls back to the general route. Raw Lance
   writers remain unsupported and the marker is explicitly non-cryptographic.
   Its first whole-delta fenced-adopt diagnostic failed decisively (about 447
   MB peak RSS at 100K × 256 versus 74 MB for Append), so production was changed
   to actual chunks capped at 8,192 rows / 32 MiB in a pre-minted transaction
   chain under one sidecar, with a 1,024 logical data-transaction ceiling.
   Exact recovery's distinct 1,026-version history bound reserves one allowed
   index tail and one compensating restore.
   Mutation/Load intentionally remains one transaction per table and refuses
   the same row/byte ceiling before arming; larger incremental loads are
   operator-chunked graph commits rather than an unreviewed v3 chain protocol.
   External Blob URI cells are size-summed before payload reads and materialized
   on keyed Append/Merge because Lance merge-insert has no `WriteParams` hook;
   Overwrite retains external-reference semantics.
   Three-run release evidence on an Apple M5 Pro records the host and uncapped
   macOS `RLIMIT_AS` limitation. The 1M small-upsert cell passed 29 ms median /
   243,875,840-byte max RSS against 50 ms / 256 MiB thresholds, and the 8,192 ×
   256 ceiling cell peaked at 104,333,312 bytes against 128 MiB. The recorded
   10K/100K bounded-bulk rows passed their *direct-substrate* comparison, but
   the old normal arm directly staged Lance merge-insert chunks and omitted the
   production BranchMerge pipeline. The first corrected full-lifecycle 10K
   series then failed both production thresholds: 30.0× median latency and
   108,625,920-byte maximum paired RSS overhead versus the raw Append
   comparator. That result triggered the certificate/source-interval shortcut
   above while retaining the bounded recovery chain. A one-transaction
   replacement was rejected because beta.21's v2 join constructs an unbounded
   DataFusion runtime.

   After a passing no-target-preflight diagnostic, the exact output paths,
   clean tree/binary digest, five seeds per size, and AB/BA/AB/BA/AB order were
   predeclared. The final 10K production/comparator medians were 31/8 ms
   (**3.875×**) with maximum signed paired peak-RSS overhead 24,297,472 bytes.
   The 100K medians were 136/35 ms (**about 3.886×**) with maximum overhead
   32,604,160 bytes. Both pass the fixed 5× and 67,108,864-byte gates. All 20
   aggregate records, all 60 setup/operation/verification child phases,
   exact-content checks, and route assertions passed with no replacement or
   completed-trial exclusion. RFC-023's production performance gate is closed.
   Full tables and methodology are in RFC-023 §11.4. RFC-027 still needs its own
   evidence.
6. **Derived-index fallbacks:** RFC-024's Gate A instrument forces index-absent,
   one-uncovered, eight-uncovered, and reconciled-after-tail states. Results are
   logically identical; the absent negative control grows; tail work is
   observable; and `optimize_indices` returns coverage to zero uncovered and
   representative `rows_scanned`/range work from 27→10 / 17→10. That part
   passes, but
   the overall candidate is rejected because object-read/byte curves fail.
   RFC-027 still must prove its absent/partial candidate-discovery states.

## Acceptance order after disposition

The review does not require all RFCs to land together. A safe order is:

1. RFC-022 is implemented at the documented single-writer-process boundary;
   branch-control recovery, coarse `graph_head` OCC, foreign-branch fact
   classification, writer-surface closure, and lifecycle are dispositioned;
2. RFC-028's stable identity capability landed on 2026-07-15;
3. RFC-023's optimized production BranchMerge bulk latency/RSS cells passed on
   2026-07-15; the RFC is implemented with its operator duplicate-repair
   runbook, v6 routing/PK, effect-aware recovery, bounded safety, certificate
   chain, and independent named-branch rebuild evidence complete;
4. RFC-024's independent physical lookup evaluation completed on 2026-07-15:
   the exact BTREE's scan work is flat, but uncompacted RustFS cold object
   reads/bytes and compacted byte terms grow, so the format is research-blocked
   and current production remains on internal schema v8 without table heads;
5. keep RFC-025 research-blocked after its 2026-07-17 Gate 0 no-go; reconsider
   only after a history-flat current-authority lookup shape or revised
   evidence-backed operational contract passes the full physical-I/O boundary,
   then require physical protection of checkpoints/live graph branches and
   strict-rebuild implementation evidence;
6. retain RFC-026 Gate E0's green exact-version evidence, Phase A's implemented
   bounded enrollment foundation, and private B1's green schema-v8/config-v2
   one-generation put/watcher/replay/recovery-v11 strict fold evidence; expose
   B2 only after accepted designs and evidence for durable
   contributor attribution, bounded reclamation, strict correction, same-key
   retry sequencing, and
   persistent status/quiesce/resume/abort-drain/rebuild. Retain the public
   exact enrollment receipt and cross-process admission seal as the gate for
   broader topology;
7. keep RFC-027 in research until deletion-delta discovery passes its stated
   correctness and flat-cost gates.

This ordering preserves the split's main benefit: a blocked performance
optimization or research result does not hold correctness work hostage.

> 💬 **Order update (2026-07-14):** BLOCKER-04's shipped-cleanup prerequisite is
> complete and RFC-025 §6 incorporates its floor. TIGHTENING-01's beta.21 check
> landed `Some(empty)`, so an upstream symmetry change may be demoted after the
> engine routing proof. That routing proof, stable identity, and RFC-023's
> effect-aware recovery implementation are now present in v6. The 16-writer
> stress, blob/duplicate-rebuild, branch-fork, restore, later-import, fresh-no-
> match, row/byte/transaction-boundary, both between-chunk recovery directions,
> separately rebuilt named-branch identity/history, genuine-v5 reverse-refusal,
> and the small-upsert/one-ceiling cost cells are green as of 2026-07-15. The
> historical bounded-bulk rows were substrate-only, and the first corrected
> 10K production series failed both thresholds. That historical block is now
> closed: the certificate/no-target-preflight diagnostic passed, followed by
> predeclared five-pair 10K and 100K series at 3.875× / ~3.886× median latency
> and 24,297,472 / 32,604,160-byte maximum paired RSS overhead. The operator
> duplicate-repair procedure in RFC-023 §11 is complete. The destructive-
> recovery topology remains single-writer-process.
