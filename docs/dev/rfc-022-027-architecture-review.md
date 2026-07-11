# Architecture review: RFC-022 through RFC-027

**Status:** Open review findings
**Date:** 2026-07-11
**Audience:** RFC authors, engine/storage maintainers, and release reviewers
**Reviewed against:** OmniGraph 0.8.1; Lance 9.0.0-beta.15 at
`f24e42c11a742581365e1cbe17c906ea2dac1bc6`; full Lance transaction,
branch/tag, MemWAL, row-lineage, read/write, compaction, and cleanup
specifications; pinned Rust implementation where the public specification is
not precise enough

This is a review ledger, not a competing specification. The RFCs remain the
normative proposals. A finding is closed only when the affected RFC either:

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
- RFC-022 should define one correctness protocol with explicit physical-effect
  adapters, not pretend that Lance offers one universal staged primitive.
- Key fencing, durable heads, retention, MemWAL, and lineage acceleration are
  separate irreversible decisions and should keep separate evidence and format
  gates.

The remaining open blockers are boundary problems, not a reason to replace that
core. They concern actual cross-process fencing, capability activation, format
rollout, and public RFC lifecycle. The native branch crash classifier,
foreign-source merge semantics, and shipped lazy-branch cleanup exposure found
by this review are now dispositioned below.

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
    +-- stable identity/incarnation capability
    |       +-- RFC-024 durable heads
    |       +-- RFC-025 checkpoint rows
    |       `-- RFC-026 stream/reject authority
    |
    +-- RFC-023 key fencing
    |       `-- RFC-026 keyed streaming mode
    |
    +-- RFC-025 retention
    +-- RFC-026 MemWAL
    `-- RFC-027 merge-delta research
```

The public RFC process accepts whole RFCs, not independently accepted sections.
The identity capability must therefore be extracted into its own RFC, or
RFC-024 must be accepted in full before identity-dependent siblings. An
implementation may phase an accepted RFC internally, but it cannot treat a
sub-contract of an unaccepted RFC as a separately accepted decision.

## Blocking findings

### BLOCKER-01 — native graph-branch control is multi-phase

**Affected:** [RFC-022 §7](../rfcs/rfc-022-unified-write-path.md#7-native-graph-branch-ref-control-protocol)

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

**Affected:** RFC-023 migration claim, RFC-024 migration claim, and
[RFC-025 §2.3](../rfcs/rfc-025-checkpoint-retention.md#23-cross-process-retention-claim)

`PutMode::Create` correctly elects one initial owner. A random token in that
object does not by itself fence a paused owner, because Lance tag, branch,
migration, and cleanup effects do not condition their commits on that token.
RFC-025 proposes a check-then-delete release; RFC-023 does not yet specify a
release protocol at all. For the check-then-delete shape, the race is:

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

### BLOCKER-03 — key-conflict retry must first resolve partial effects

**Affected:** [RFC-022 §9](../rfcs/rfc-022-unified-write-path.md#9-concurrency-and-retry-semantics),
[RFC-023 §6](../rfcs/rfc-023-key-conflict-fencing.md#6-retry-and-validation-semantics),
and [RFC-026 §6](../rfcs/rfc-026-memwal-streaming-ingest.md#6-fold-protocol)

**Status:** The RFC-022 mutation/load portion is closed in the shipped adapter
as of 2026-07-11. RFC-023 and RFC-026 retain their own open dispositions.

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

For strict insert, finalize an empty intent or retain a partial-effect sidecar,
as applicable, and return the terminal `KeyConflict`; RFC-022 permits partial
recovery to finish at the next synchronous barrier. Do not start another
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

### BLOCKER-04 — live graph branches need physical GC protection

**Affected:** [RFC-025 §6](../rfcs/rfc-025-checkpoint-retention.md#6-cleanup-protocol-and-pruned-through-boundary)

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

**Affected:** [RFC-024 §5](../rfcs/rfc-024-durable-table-heads.md#5-atomic-publisher-contract)

The RFC says expected table versions are compared against table heads. Lance
version numbers can repeat across recreated datasets/branches, so version-only
comparison admits drop/recreate ABA.

**Required disposition:** define the OCC token as the complete logical head
identity, at least:

```text
(state, stable_table_id, incarnation_id, table_path,
 table_branch, physical_ref_incarnation, table_version, schema_hash)
```

The publisher may encode that token differently, but retry/revalidation must
not reduce it to `table_version`. `physical_ref_incarnation` means an e_tag when
the backend supplies one, or another proven token that changes when a dataset
or native ref is deleted and recreated at the same path/branch/version. This is
distinct from the logical `incarnation_id`, which RFC-024 deliberately preserves
across an owner handoff. Add stale-writer races for both logical drop/recreate
and physical ref recreation at the same numeric version.

### BLOCKER-06 — MemWAL needs a capability/format activation barrier

**Affected:** [RFC-026 §3](../rfcs/rfc-026-memwal-streaming-ingest.md#3-enrollment-is-a-recoverable-inline-commit),
[§5](../rfcs/rfc-026-memwal-streaming-ingest.md#5-ack-path-validation-and-writer-lifecycle),
[§6](../rfcs/rfc-026-memwal-streaming-ingest.md#6-fold-protocol),
[§8](../rfcs/rfc-026-memwal-streaming-ingest.md#8-epoch-fenced-quiescence-barrier),
and [§12](../rfcs/rfc-026-memwal-streaming-ingest.md#12-phasing)

Enrollment creates persistent MemWAL metadata and `stream_state` changes the
correctness preconditions for schema, branch, maintenance, and data operations.
An older binary that understands key fencing but not stream lifecycle can ignore
`OPEN | DRAINING | SEALED`, mutate the base table, or perform a schema/branch
operation without draining acknowledged rows.

**Required disposition:** define:

- a graph capability/internal-format stamp written only after every enrolled
  table and lifecycle authority is valid;
- old-binary/new-format and new-binary/partial-format refusal behavior;
- migration and rollback/roll-forward ordering;
- preservation rules for later heads/retention formats;
- genuine cross-version tests, not a stamp-rewind simulation.

Replica scope must also match RFC-023's recovery support boundary. Multiple
replicas may route acknowledgement traffic to one shard owner, but enrollment,
fold, and sidecar recovery remain single-writer-process operations until
foreign-process sidecar ownership is fenced. Do not advertise general replica
failover for those operations merely because MemWAL has a shard epoch.

### BLOCKER-07 — stable identity ownership contradicts sibling dependencies

**Affected:** [RFC-024 §3.1](../rfcs/rfc-024-durable-table-heads.md#31-identity-and-object-key),
[RFC-025 §2.1](../rfcs/rfc-025-checkpoint-retention.md#21-logical-authority-manifest-rows),
[RFC-026 §7](../rfcs/rfc-026-memwal-streaming-ingest.md#7-fold-time-rejection-is-atomic),
and [RFC-026 §8](../rfcs/rfc-026-memwal-streaming-ingest.md#8-epoch-fenced-quiescence-barrier)

RFC-024 now says it exclusively owns stable table identity and incarnation and
that identity-dependent sibling claims must wait for it. RFC-025 still calls
RFC-024 optional, and RFC-026 persists `stable-table-id` without declaring the
dependency.

**Required disposition:** extract the identity/incarnation format and migration
as a shared prerequisite RFC, or accept RFC-024 in full before RFC-025 and
RFC-026. Durable-head implementation can remain a later phase only after its
owning RFC and format contract are accepted.

The `_ingest_rejects` deterministic key must use stable table ID plus
incarnation rather than mutable `table_key`, or rename/recreate can change reject
identity and break replay idempotence.

> 💬 **Concur; supersedes a 2026-07-11 correction (2026-07-11):** the earlier
> pass placed identity ownership *inside* RFC-024 §3.1, which created exactly
> the contradiction described here (RFC-025 calls heads optional while
> consuming the identity RFC-024 claims to own). The extracted
> identity/incarnation RFC is the cleaner shape and should replace that
> ownership note. The `_ingest_rejects` key observation is an additional catch
> the earlier pass missed — endorsed.

### BLOCKER-08 — retention activation cannot precede migration selection

**Affected:** [RFC-025 §8](../rfcs/rfc-025-checkpoint-retention.md#8-migration-and-compatibility)
and [§11](../rfcs/rfc-025-checkpoint-retention.md#11-phasing)

RFC-025 leaves in-place migration versus export/import undecided. Its phase
table nevertheless puts “format activation” in Phase A and “selected migration
path” in Phase D. Activation cannot be implemented or reviewed before the
upgrade contract is known.

**Required disposition:** select the migration before Phase A, then specify
quiescence, partial-state refusal, main-stamp ordering, branch coverage, crash
recovery, and later capability preservation. If export/import is selected,
state the irreversible loss of branches, commit DAG, snapshots, and historical
checkpoints as part of the acceptance decision.

### BLOCKER-09 — decide whether these are public or internal RFCs

**Affected:** [public RFC process](../rfcs/README.md), RFC-022 through RFC-027

The files currently live in the public RFC track but use internal-style
`rfc-022-*` names, noncanonical `draft`/`research-blocked` statuses, and empty
owner metadata. In the public process, merging an RFC is acceptance; a merged
public RFC cannot simultaneously retain undispositioned acceptance blockers.

**Required disposition:** choose the track before merge:

- for the public track, rename to `NNNN-title.md`, use `Status: Proposed`, fill
  the template's author/discussion/implementation metadata, and resolve every
  blocker before the RFC PR merges; or
- move the proposals to `docs/dev/` and follow the maintainer-internal process,
  leaving the public RFC directory for externally authorable accepted records.

RFC-027 may state “research-blocked” prominently as its technical state while
retaining the public lifecycle status `Proposed`.

> 💬 **Concur (2026-07-11):** same recommendation as the original family
> review — these are maintainer-internal proposals mid-revision; `docs/dev/`
> with the internal process is the low-friction disposition, keeping
> `docs/rfcs/` for its defined merge-equals-acceptance lifecycle. Whichever
> track is chosen, decide it before any of the set merges, since it defines
> what "dispositioned before acceptance" means for every other finding here.

## Protocol clarifications

### BLOCKER-10 — do not put foreign-branch facts in an atomic `ReadSet`

**Affected:** [RFC-022 §6.2](../rfcs/rfc-022-unified-write-path.md#62-branch-merge)

**Status:** Closed in specification on 2026-07-11; full merge-adapter conversion
remains rollout work rather than an architecture ambiguity.

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
> contract would require a source fence through target CAS. The current bridge
> remains conservatively stricter before effects while its full adapter lands.

### BLOCKER-11 — RFC-022 and no-heads MemWAL need coarse OCC

**Affected:** [RFC-022 §10](../rfcs/rfc-022-unified-write-path.md#10-rollout-and-compatibility)
and [RFC-026 §6](../rfcs/rfc-026-memwal-streaming-ingest.md#6-fold-protocol)

[RFC-022's rollout](../rfcs/rfc-022-unified-write-path.md#10-rollout-and-compatibility)
currently says mutation/load read-set arbitration is likely blocked on
RFC-024 because probed-but-untouched tables lack mutable head rows.

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

> **Implementation disposition (2026-07-11):** mutation/load now capture the
> native Lance `BranchIdentifier`, exact optional `graph_head`, and accepted
> schema identity; revalidate under a branch-then-table gate; and pass the same
> token to every publisher retry and schema-v3 recovery decision. Metadata-only
> schema-apply tests pin the required invariant that supported schema changes
> move `graph_head` even when no data-table version changes. This closes the
> coarse mutation/load cell without claiming a general schema-authority row or
> multi-process native-ref fencing. RFC-024 remains the false-contention
> narrowing step.

### TIGHTENING-01 — symmetric Lance conflicts are not obviously an activation gate

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

## Specification and acceptance tightening

These are smaller than the blockers above, but should be resolved before the
RFC set is merged:

1. **Checkpoint-name normalization:** define allowed bytes, Unicode
   normalization, case sensitivity, maximum encoded length, reserved prefixes,
   and normalization-version compatibility. Test collisions and reuse across
   versions.
2. **RFC lifecycle values:** [`docs/rfcs/README.md`](../rfcs/README.md#status-values)
   defines `Proposed`, `Accepted`, `Declined`, `Superseded by NNNN`, and
   `Implemented`. Either use those values or amend the process before using
   `draft` and `research-blocked` as machine-readable statuses. Research-blocked
   can remain prominent in RFC-027's body while its lifecycle status is
   `Proposed`.
3. **Provisional version naming:** sibling RFCs should say “RFC-024 heads
   format” rather than treating `v5` as permanent while RFC-024 says the numeral
   will change if another format lands first.
4. **Capability ordering:** RFC-025 must specify retention-first, heads-first,
   and co-release preservation rules. RFC-026 is a conditional dependency when
   streams are enrolled because cleanup must persistently quiesce them.
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

1. accept RFC-022 after branch-control recovery, coarse `graph_head` OCC, and
   foreign-branch fact classification are explicit (all three are now
   dispositioned; the public lifecycle decision in BLOCKER-09 still governs
   what “accepted” means);
2. accept and land the stable identity RFC/capability;
3. accept RFC-023 once partial-effect retry and its format/fleet barrier are
   complete;
4. evaluate RFC-024 independently on its physical lookup cost gate;
5. accept RFC-025 after physical protection of both checkpoints and live graph
   branches plus a selected upgrade path;
6. accept RFC-026 after its capability barrier and writer-ownership scope are
   explicit;
7. keep RFC-027 in research until deletion-delta discovery passes its stated
   correctness and flat-cost gates.

This ordering preserves the split's main benefit: a blocked performance
optimization or research result does not hold correctness work hostage.

> 💬 **Order update (2026-07-11):** BLOCKER-04's shipped-cleanup prerequisite is
> now complete and RFC-025 §6 incorporates its floor. Step 3's TIGHTENING-01
> demotion still hinges on the partial-update filter check
> noted there; if that check lands `None`, upstream symmetry returns to the
> activation gate and RFC-023's timeline moves accordingly.
