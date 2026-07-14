# Architecture review: RFC-022 through RFC-028

**Status:** RFC-022 implemented; RFC-023–028 review and acceptance work remains open
**Date:** 2026-07-11
**Audience:** RFC authors, engine/storage maintainers, and release reviewers
**Reviewed against:** OmniGraph 0.8.1; Lance 9.0.0-beta.15 at
`f24e42c11a742581365e1cbe17c906ea2dac1bc6`; full Lance transaction,
branch/tag, MemWAL, row-lineage, read/write, compaction, and cleanup
specifications; pinned Rust implementation where the public specification is
not precise enough
**RFC-022 close-out revalidated against:** Lance 9.0.0-beta.21 at
`1aec14652dcbace23ac277fa8ced35000bea0c40`; see the
[2026-07-12 alignment audit](lance.md#last-alignment-audit-2026-07-12-lance-900-beta21-upstream-omnigraph-pinned-at-900-beta21-via-git-rev).
**RFC-023 substrate evidence revalidated against:** the same beta.21 revision;
filter-shape and conflict-order probes are recorded in RFC-023 §2 and do not
constitute fencing activation or acceptance.
**RFC-026 substrate contract revalidated against:** the same beta.21 revision;
the full MemWAL table and system-index specifications, including the
durability/fencing behavior carried forward from beta.17, are reflected in the
RFC's survey and acceptance guards.
**RFC-024/025/027 substrate claims revalidated against:** the same beta.21
revision and the full matching table/index/branch/tag/cleanup, transaction, and
row-lineage specification sections; their survey headers now record beta.21.

This is a review ledger, not a competing specification. The RFCs remain the
normative design records: RFC-022 documents the implemented protocol and its
siblings remain proposals. A finding is closed only when the affected RFC either:

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

**Status:** Closed in specification on 2026-07-14. RFC-022 is shipped;
RFC-023/RFC-026 failpoint evidence remains an acceptance gate for those drafts.

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

**Status:** Closed in specification on 2026-07-14; backend-token proof and ABA
tests remain RFC-024 acceptance gates.

The RFC says expected table versions are compared against table heads. Lance
version numbers can repeat across recreated datasets/branches, so version-only
comparison admits drop/recreate ABA.

**Required disposition:** define the OCC token as the complete logical head
identity, at least:

```text
(state, stable_table_id, incarnation_id, table_path,
 table_branch, physical_ref_incarnation, table_version, schema_ir_hash)
```

The publisher may encode that token differently, but retry/revalidation must
not reduce it to `table_version`. `physical_ref_incarnation` means an e_tag when
the backend supplies one, or another proven token that changes when a dataset
or native ref is deleted and recreated at the same path/branch/version. This is
distinct from the logical `incarnation_id`, which RFC-028 deliberately preserves
across an owner handoff. Add stale-writer races for both logical drop/recreate
and physical ref recreation at the same numeric version.

**Disposition:** RFC-024 §3.2 now persists the separate physical-ref token and
refuses activation on a backend without a proven source. §5 compares the full
token on every retry and derives the desired token from the achieved effect;
§12 adds local and S3/RustFS same-version ABA tests. Its open gates retain the
backend proof rather than silently accepting a timestamp/version fallback.

### BLOCKER-06 — MemWAL needs a capability/format activation barrier

**Affected:** [RFC-026 §3](../rfcs/0026-memwal-streaming-ingest.md#3-enrollment-is-a-recoverable-multi-effect-adapter),
[§5](../rfcs/0026-memwal-streaming-ingest.md#5-ack-path-validation-and-writer-lifecycle),
[§6](../rfcs/0026-memwal-streaming-ingest.md#6-fold-protocol),
[§8](../rfcs/0026-memwal-streaming-ingest.md#8-epoch-fenced-quiescence-barrier),
[§9](../rfcs/0026-memwal-streaming-ingest.md#9-fresh-read-cuts),
and [§11–13](../rfcs/0026-memwal-streaming-ingest.md#11-format-activation-and-rebuild)

**Status:** Open. The format/refusal portion is specified, but beta.21 lacks the
public exact enrollment and reversible shard-admission surface needed by the
recovery/quiescence adapter. RFC-026 cannot be accepted or activated until that
API gate and its crash evidence close.

Enrollment creates persistent MemWAL metadata and `stream_state` changes the
correctness preconditions for schema, branch, maintenance, and data operations.
An older binary that understands key fencing but not stream lifecycle can ignore
`OPEN | DRAINING | SEALED`, mutate the base table, or perform a schema/branch
operation without draining acknowledged rows.

**Format disposition:** RFC-026 §11 makes streaming a graph-format capability present
at new-root initialization, never a feature stamp added by first enrollment.
It defines:

- a graph capability/internal-format stamp written only after every enrolled
  table and lifecycle authority is valid;
- old-binary/new-format and new-binary/partial-format refusal behavior;
- strict export/init/load rebuild and source/target failure isolation;
- preservation rules for later heads/retention formats;
- genuine cross-version tests, not a stamp-rewind simulation.

RFC-026 §12 carries those tests and §13 places them in Phase A before the public
stream path. RFC-026 §3/§8 also make a rematerialized table a separate physical
enrollment boundary: SchemaApply leaves the preserved logical identity
`SEALED`, and only an exact sidecar-covered rebind with a fresh enrollment and
shard namespace may publish `OPEN`. Recovery never infers adoption from the
logical pair, name, path, or a compatible-looking index.

**Remaining required disposition:** in beta.21,
`InitializeMemWalBuilder::execute` internally commits the `CreateIndex`
transaction and returns only `Result<()>`; `mem_wal_writer` separately creates
or claims shard-manifest objects. That cannot satisfy a sidecar that pre-arms
one exact combined effect. RFC-026 §3 now rejects private-module/object-store
emulation and requires a public caller-controlled transaction plus idempotent
shard provision/classify/seal/reopen/reclaim APIs, or one recoverable enrollment
receipt covering both effects. §5/§8 require the physical admission seal that
closes the final lifecycle-check-to-put race. The same-path/ref ABA, multi-
effect crash, and paused-claimant tests remain red acceptance gates.

Replica scope must also match RFC-023's recovery support boundary. Multiple
replicas may route acknowledgement traffic to one shard owner, but enrollment,
fold, and sidecar recovery remain single-writer-process operations until
foreign-process sidecar ownership is fenced. Do not advertise general replica
failover for those operations merely because MemWAL has a shard epoch.

### BLOCKER-07 — stable identity ownership contradicts sibling dependencies

**Affected:** [RFC-023 §4.1](../rfcs/0023-key-conflict-fencing.md#41-keyed-graph-tables),
[RFC-024 §3.1](../rfcs/0024-durable-table-heads.md#31-identity-and-object-key),
[RFC-025 §2.1](../rfcs/0025-checkpoint-retention.md#21-logical-authority-manifest-rows),
[RFC-026 §2](../rfcs/0026-memwal-streaming-ingest.md#2-stream-mode-and-key-semantics),
[RFC-026 §7](../rfcs/0026-memwal-streaming-ingest.md#7-fold-time-rejection-is-atomic),
and [RFC-026 §8](../rfcs/0026-memwal-streaming-ingest.md#8-epoch-fenced-quiescence-barrier),
with the shared contract in [RFC-028](../rfcs/0028-stable-schema-identity.md)

**Status:** Closed in specification on 2026-07-14; RFC-028 acceptance and
implementation remain prerequisites for every identity-consuming sibling.

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
incarnation rather than mutable `table_key`, while a rematerializing rename
mints a fresh shard namespace under an exact sidecar-covered rebind. Thus
logical history ownership survives rename, physical WAL artifacts are never
adopted by implication, and drop/re-add cannot claim a predecessor's rows.

The same correction keeps logical identity separate from physical ABA proof.
RFC-025 now owns a heads-independent exact manifest/table version token,
backend refusal, recovery/reconciler checks, and local/S3 same-coordinate ABA
tests. RFC-026 owns a binding-stable physical-ref incarnation token and remains
blocked under BLOCKER-06 until its backend/API guards pass. Neither silently
imports RFC-024's head storage merely to obtain those proofs.

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
owner. RFC-022 is `implemented` at its documented support boundary, RFC-023
through RFC-026 and RFC-028 remain `draft`, and RFC-027 remains
`research-blocked`.
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

> ✅ **RFC-022 implementation close-out (2026-07-13):** every currently
> supported graph-write surface is enrolled in an exact adapter, the bounded
> Optimize schema-v2 adapter, or an explicit authority/physical/bootstrap/
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
> schema-v4 exact adapter preserves that captured-source contract while
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
> token to every publisher retry and schema-v3 recovery decision. Metadata-only
> schema-apply tests pin the required invariant that supported schema changes
> move `graph_head` even when no data-table version changes. This closes the
> coarse mutation/load cell without claiming a general schema-authority row or
> multi-process native-ref fencing. RFC-024 remains the false-contention
> narrowing step.

> **Branch-merge implementation disposition (2026-07-11):** branch merge uses
> the same coarse target token without pretending the source belongs to that
> atomic read set. Schema-v4 recovery persists the captured source parent,
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

**Status:** Closed in specification on 2026-07-14; each format still requires
its own implementation and genuine old/new-binary evidence before acceptance.

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

### TIGHTENING-01 — symmetric Lance conflicts are not obviously an activation gate

**Status:** the deciding beta.21 substrate check is closed; the production
routing and activation dispositions remain open.

RFC-023 correctly identifies Lance's directional filtered/unfiltered behavior.
However, its own mandatory fleet outage, recovery drain, historical-duplicate
validation, stamp-last activation, and old-binary refusal are intended to make
unfiltered writers unreachable after activation.

The current RFC-023 routing table still permits a direct existing-row `Update`,
whose Lance operation has no inserted-row filter. Therefore unfiltered
transactions do remain reachable after activation. Symmetry can be demoted only
if every insertion-bearing keyed path routes through filtered merge-insert and
the RFC proves a direct update cannot insert a row or change `id`; affected-row
conflict metadata must continue to cover updates to the same existing row.

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
> Append to land second after a filtered Update. TIGHTENING-01 remains open
> until the engine proves every insertion-bearing keyed path is filtered,
> structurally forbids keyed Append, and proves any remaining unfiltered update
> cannot insert or alter `id`. Upstream symmetry is defense in depth only after
> that proof.

## Specification and acceptance tightening

These are smaller than the blockers above, but should be resolved before the
affected RFC is accepted or implemented:

1. **Checkpoint-name normalization:** define allowed bytes, Unicode
   normalization, case sensitivity, maximum encoded length, reserved prefixes,
   and normalization-version compatibility. Test collisions and reuse across
   versions.
2. **RFC lifecycle values (closed 2026-07-13):**
   [`docs/rfcs/README.md`](../rfcs/README.md) now uses one four-digit filename
   namespace and distinguishes the public contribution lifecycle from the
   maintainer design-series lifecycle through explicit `Author track` and
   `Status` metadata. `draft` and `research-blocked` are valid only in the
   latter; public merge remains acceptance.
3. **Provisional version naming (closed 2026-07-14):** RFC-024 now says “heads
   format”; RFC-028 provisionally takes the next number while explicitly
   allowing the numeral to move. No sibling treats `v5` as permanent.
4. **Capability ordering (closed in specification 2026-07-14):** RFC-025 and
   RFC-026 define independent-release rebuilds and combined initialization /
   recovery gates for co-release. Cleanup continues to require persistent
   quiescence whenever streams are enrolled.
5. **Memory measurement:** `helpers::cost` measures I/O, not peak RSS. RFC-023's
   adopted-merge and RFC-027's lineage memory gates should use the subprocess
   `scenarios.rs` harness or an equivalent `wait4`/`ru_maxrss` instrument and
   name dataset sizes, baseline, cap, and pass threshold.

   > 💬 **Concur; catches a gap in a 2026-07-11 correction:** the RFC-023
   > §11.4 memory gate added that day states the bound ("peak memory bounded by
   > batch size") without naming an instrument that can measure it — this item
   > is what makes that gate enforceable rather than aspirational.
6. **Derived-index fallbacks:** acceptance tests must force index-absent and
   partially covered states for RFC-024 head lookup and RFC-027 candidate
   discovery, assert identical logical results, and assert the promised
   degraded-cost/fallback telemetry.

## Acceptance order after disposition

The review does not require all RFCs to land together. A safe order is:

1. RFC-022 is implemented at the documented single-writer-process boundary;
   branch-control recovery, coarse `graph_head` OCC, foreign-branch fact
   classification, writer-surface closure, and lifecycle are dispositioned;
2. accept and land RFC-028's stable identity capability;
3. accept RFC-023 once partial-effect retry and its format/fleet implementation
   and evidence are complete;
4. evaluate RFC-024 independently on its physical lookup cost gate;
5. accept RFC-025 after physical protection of both checkpoints and live graph
   branches plus strict-rebuild implementation evidence;
6. accept RFC-026 only after Lance exposes the public exact-enrollment and
   admission-seal surface, then its capability/refusal/rebuild, multi-effect
   recovery, and writer-ownership evidence match the specified scope;
7. keep RFC-027 in research until deletion-delta discovery passes its stated
   correctness and flat-cost gates.

This ordering preserves the split's main benefit: a blocked performance
optimization or research result does not hold correctness work hostage.

> 💬 **Order update (2026-07-14):** BLOCKER-04's shipped-cleanup prerequisite is
> complete and RFC-025 §6 incorporates its floor. TIGHTENING-01's beta.21 check
> landed `Some(empty)`, so an upstream symmetry change may be demoted after the
> engine routing proof. Step 3 remains blocked on that proof, RFC-023's
> partial-effect recovery implementation evidence, stable identity, the cost
> gates, and its format/fleet barrier; the substrate result alone does not move
> acceptance.
