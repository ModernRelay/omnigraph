---
rfc: "0032"
title: "Adversarial correctness harness"
track: maintainer
status: draft
implementation: not-started
authors:
  - OmniGraph maintainers
created: 2026-08-06
updated: 2026-08-23
discussion: null
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC 0032: Adversarial correctness harness

**Depends on:** nothing. It may reuse RFC 0031's versioned local-record envelope,
but no phase depends on a shared record store. No product behavior, format, or
protocol change; test-only engine/storage seams are in scope and guarded.
**Complements, does not replace:** the failpoint suites (hand-placed crash
windows, compile-checked names, `Rendezvous` race orchestration),
`proptest_equivalence.rs`, `merge_truth_table.rs`, and the `helpers::cost` /
RFC 0031 cost instruments.
**Audience:** engine maintainers; anyone adding a writer kind, a recovery
path, or a query-execution fork.

(This RFC absorbs and supersedes the scratch test-harness library survey of
2026-07-28. That survey's streaming-module targets did not survive the
RFC 0026 removal (#449); they are retargeted here.)

---

## 0. Decision summary

Correctness bugs in this codebase have so far been found by incident and by
hand. The cross-type id-collision divergence (fixed in f6a0e53) was a silent
wrong-result fork between two Expand modes, caught only because someone
hand-built the one colliding fixture. Every one of the engine's
compile-checked failpoints marks a crash window a human thought of; the
windows nobody thought of have no coverage. Property testing covers several
query-equivalence laws with 48 cases per property, but not operation sequences;
the two Pest grammars parse untrusted input with zero fuzz coverage; nothing
explores thread schedules.

This RFC turns each of those one-off discoveries into a search:

1. **Durable-write-cut sweeps** — run an operation in a child process and kill
   it after selected completed durable effects observed across both storage
   paths. This searches a bounded observed schedule; it is not an exhaustive
   proof over every concurrent subset or ordering.
2. **Seeded operation-sequence fuzzing** — generate valid op sequences and
   check them differentially, with no parallel model of graph semantics.
3. **Parser fuzzing** — crash-safety on `schema.pest` / `query.pest`.
4. **Scoped schedule exploration** — shuttle on the owned concurrency modules
   that never call Lance.

The design's center is a thesis about oracles: **an oracle you already
maintain beats an oracle you must build.** The instruments check explicit
pre/post authority witnesses, existing truth tables and invariants,
cross-configuration observation transcripts, and narrow metamorphic laws. They
do not claim that two agreeing implementations must be correct, that a replay
proves semantics, or that bounded crash/schedule search proves the absence of
bugs (§6 records why a second full graph implementation is rejected).

**Amendment note (2026-08-17).** §13 records
[RFC 0037's deterministic simulation harness](0037-deterministic-simulation-harness.md)
as an execution layer this RFC's instruments can also run on, notes the
stronger determinism contract now available beside §3.3, and corrects two §6
entries against measured evidence. Nothing in §§1-12 is restructured; each
affected section is engaged in place.

## 1. Why the existing instruments do not cover this

| Instrument | Provides | Cannot |
|---|---|---|
| `failpoints.rs` + its compile-checked named points | deterministic crash/race coverage at chosen boundaries | enumerate windows nobody chose; a typo-guarded point still has to be *placed* |
| `proptest_equivalence.rs` | generated cross-type collision, Expand-mode, no-phantom-row, and anti-join partition properties, 48 cases per property | vary the *operation sequence*; cover writers, branches, recovery |
| `merge_truth_table.rs` | hand-built 9×9 merge oracle, compile-forced completeness | anything outside merge; generated compositions |
| `helpers::cost`, RFC 0031 | cost regression evidence | correctness under faults, schedules, or generated inputs |

Each is good at its job. The gap is bounded exploration of additional crash
cuts, input sequences, and schedules — with automatic shrinking to a minimal
failing case when the search hits something. The result is stronger sampled
evidence, not exhaustive state-space coverage.

## 2. What can and cannot be virtualized

OmniGraph deliberately does not own its runtime. Lance owns storage,
transactions, physical indexes, and substantial internal randomness (branch
identifiers, CAS staging names), and invariant
1 forbids reaching through it. A FoundationDB/TigerBeetle-style
"virtualize the world" simulation is therefore not available without forking
the substrate or buying whole-VM determinism (§6). The seams OmniGraph
actually controls:

1. **Two storage paths, one test sequencer.** Lance datasets use the process-
   wide `ObjectStoreRegistry`; recovery/schema/cluster control objects use the
   implementation in `omnigraph-storage` through the engine's compatibility
   `StorageAdapter`. An outer engine-trait decorator is too high:
   `rename_text`, `delete_prefix`, multipart handles, and local raw filesystem
   operations can hide several effects inside one call. V1 therefore uses
   persistent RustFS and two lower wrappers backed by one shared
   `DurableEventSequencer`: a Lance `WrappingObjectStore` supplied through
   `ObjectStoreParams` at every dataset create/open/write chokepoint, and a
   feature-gated injected `DynObjectStore` inside
   `omnigraph_storage::ObjectStorageAdapter`. The engine facade remains a
   delegate, not a third implementation. The lower injection constructor is
   available only through a non-default
   `omnigraph-storage/adversarial-harness` feature;
   `omnigraph-engine/failpoints` enables that feature for integration tests.
   `#[cfg(test)]` alone is insufficient because dependency crates are not built
   with it for an engine integration test. Both fronts model successful
   multipart initiation before returning the wrapped upload, then wrap part,
   complete, and abort calls. Local-FS cut evidence is excluded until a lower
   filesystem seam can interrupt direct `rename`/`remove_dir_all` stages. No
   operation joins the sweep until the chosen lower seams expose each stage it
   can leave durable. Sensitivity cells observe known effects on both paths. A
   source guard inventories Lance `DatasetBuilder`/`WriteParams`/session
   acquisition and every `omnigraph-storage` constructor, so an unsequenced
   create or reopen fails the harness rather than silently shrinking coverage.
   Object atomicity does not create a global total order; §4.1 defines the
   bounded model.
2. **The failpoint registry** — OmniGraph-code windows (gate ordering,
   in-memory states *between* durable writes) that no store-level fault can
   reach. Retained as-is; the cut sweep complements, never replaces it.
3. **The owned concurrency modules** — the root-scoped `WriteQueueManager`
   that orders recovery against live writers, and the schema → branch → table
   gate-ordering core: code whose lock-order logic runs without Lance I/O and
   can run under a schedule explorer.

Consequences: storage-fault search is admissible only through the dual-path
sequencer, schedule search lives at seam 3, and input search lives at the
parsers and operation generator. Whole-run byte-level determinism and exhaustive
concurrent durable-effect ordering are deferred (§6).

## 3. Oracles

### 3.1 Writer-specific recovery projections

Invariants 2 and 5 constrain recovery, but they are not one zero-cost oracle.
Each crash case captures a normalized authoritative pre-state, the operation's
semantic postcondition, and a pre-recovery `CutWitness`: manifest publication
state, durable sidecar phase/body, exact participant outcomes, native-ref
authority where relevant, and whether a result crossed the child protocol. A
writer-specific `CrashOracle` maps that witness to the required terminal
projection by reusing the writer's existing recovery truth table. It is not a
generic `{pre, post}` allowance: if the authoritative manifest publication is
retained, or success reached the pre-ack/ack barrier, only the postcondition is
valid; a rollback is a failure. For an earlier ambiguous cut, the writer's
documented `Armed`/confirmed/compensation rules choose the allowed result.

After reopen/recovery, every relevant recovery record must have a terminal
disposition, and a second recovery pass must leave the projection unchanged. A
clean acknowledged control and the explicit acknowledgement-crash cell must
equal the postcondition. Each case also lists the exact derived or temporary
residue that may remain; everything else is a failure. An initiated but
uncompleted multipart upload is persistent provider-side resource state even
when it is absent from ordinary object listing, so the backend/writer projection
must explicitly allow or reject it and bound its staged parts. A backend that
cannot enumerate uploads and parts for the test root cannot qualify this sweep.

| Writer/control | Projection beyond canonical logical rows |
|---|---|
| Mutation/Load | accepted schema identity, every participant pointer, graph head/lineage, and sidecar disposition |
| SchemaApply | accepted schema/catalog identity, table lifetimes, participant pointers, graph lineage, and promotion/sidecar state |
| BranchMerge | source/target authority, participant pointers, canonical target content, native refs, graph head/lineage, and sidecar state |
| EnsureIndices | declared index intent plus manifest-selected index metadata/version state; rows alone are insufficient |
| Optimize | participant pointers, fragment/index maintenance state, graph lineage, and permitted compensated physical residue; rows alone are insufficient |
| Branch create/delete | `BranchContents` authority, native per-table refs/forks, namespace visibility, and permitted reclaimable residue |

Every run restores a byte-identical immutable baseline under a fresh root; root
URIs are normalized. Persistent identities minted after that baseline are
compared through writer-owned equivalence, not raw equality: graph commits map
to topological ordinals, branch identifiers map by logical name/ancestry,
transaction and participant IDs map by plan position, and fragment/index IDs map
through manifest ownership. Multipart upload IDs map by stable logical owner and
that owner's initiation ordinal. EnsureIndices compares selected coverage/status
and ownership; Optimize compares selected participant versions, logical content,
and its declared layout/coverage postconditions. Neither requires the same UUIDs
or one clean run's incidental fragment layout. `CutWitness` retains the actual
per-run IDs long enough to prove ownership before this normalization.

The decision table is case-specific and keyed by retained authority, not by the
cut ordinal or one clean run's incidental layout. Native branch control follows
its documented authority-derived reconciliation rules rather than pretending it
is an ordinary manifest-published data write.

### 3.2 Stepwise differential observation

A final graph digest cannot detect a wrong query result or a transient
divergence erased by a later write. Every generated operation therefore appends
to a canonical `ObservationTranscript`: operation kind, typed success value or
typed error variant/fields, normalized query/snapshot result, and the relevant
logical-state projection after mutating steps. The same seed runs on two arms —
the same arm twice, two Expand modes using the existing scoped
`with_traversal_mode` seam, or two backends — and transcripts must agree.

Rows from unordered queries compare as typed multisets. A query with explicit
ordering compares an ordered sequence under its declared tie behavior. Display
strings, temporary object names, and incidental physical ordering are never an
oracle. Each operation owns a success/error projection: compare the public
error discriminant and only contract-bearing structured fields; alpha-normalize
root URIs and minted IDs; omit opaque Lance/DataFusion source strings, ETags,
backend reasons, and provider request IDs. An opaque backend error cannot prove
cross-backend semantic equality and makes an otherwise-success cell
incomparable/failing rather than being string-matched. Final canonical export
remains a fixture/final-state cross-check, not a substitute for the transcript
and not a shared contract with RFC 0031.

### 3.3 Determinism by normalized replay

Same seed twice implies equal observation transcripts and lineage topology.
Fresh commit IDs and timestamps are alpha-normalized to deterministic
topological ordinals while preserving parent and branch shape; Lance temporary
UUID names are excluded. Logical replay is deliberately weaker than byte-level
replay, which is unnecessary and deferred (§6).

### 3.4 Hand-built truth tables where they exist

Merge semantics stay owned by `merge_truth_table.rs`. The fuzz alphabet
excludes `branch_merge` in v1 rather than duplicating that oracle (§12, open
decision 2).

### 3.5 Validity is not semantics

V1 uses a constraint-free schema and a small syntactic generator model only to
choose existing IDs/branches and form parseable operations. It predicts no
result. Once constrained schemas join the alphabet, uniqueness, RI, cardinality,
and similar typed refusals are ordinary observations that both arms must match;
syntactic legality alone never makes a refusal a bug.

Differential agreement can still preserve a common-mode bug. V1 therefore adds
narrow public-contract metamorphic checks without building a second graph
engine: read-after-successful-insert; old-snapshot stability after a later
write; delete visibility in current-but-not-old snapshots; and source-branch
stability after a child-branch write. Query-specific laws such as ternary logic
partitioning may extend the existing `proptest_equivalence.rs` owner. The
harness claims violations of these laws and cross-arm consistency gaps, not a
proof of all graph semantics.

## 4. Instruments

### 4.1 Durable-write-cut sweep

**Event model.** Before forwarding a mutating store call, the shared lower
wrapper acquires one test gate across both storage paths; V1 therefore has at
most one modeled durable call in flight. It assigns an ordinal only when that
call successfully completes and can block before returning the completion to
Lance/OmniGraph. The vocabulary is backend- and operation-specific: successful
create/overwrite/conditional PUT; multipart initiation, each durable part,
completion, and abort; copy; each exposed non-atomic rename stage; delete; and
each exposed element of a non-atomic bulk operation. An operation is ineligible
if its lower seam collapses stages that can survive independently. Reads are
recorded for diagnosis but do not advance the durable ordinal. Each descriptor
carries a normalized action/target and the stable logical owner derivable from
the writer's pre-registered plan (sidecar, manifest, or stable table identity),
enabling declared semantic cut selectors without comparing minted physical
paths across runs.

The gate deliberately selects one schedule and changes production concurrency;
it is useful crash evidence for that schedule, not a claim about all concurrent
subsets. Fresh children may present concurrent calls to the gate in a different
order. V1 accepts each resulting cut as its own observed schedule and does not
pretend to replay one clean trace. A future record/replay arbiter would need
stable logical event ownership plus a per-run bijection for every minted
persistent ID before it could claim trace replay.

**Mechanic.** A clean child run against a byte-identical baseline on persistent
RustFS must complete within its watchdog, record a reference `N > 0`, and
satisfy the operation's semantic postcondition. For each ordinal cut
`c ∈ 0..=N`, a fresh-root copy of that baseline and an isolated child are
created. At `c = 0`, the wrapper blocks before forwarding the first durable
call. At `c > 0`, it permits exactly `c` completions, then signals the parent and
blocks before returning the `c`th completion; because admission is serialized,
no later mutating call has been forwarded. The parent hard-kills and reaps the
child, captures `CutWitness` from retained storage, then opens the root in a
fresh recovery process and applies §3.1. If a run returns before reaching `c`,
the cut is admissibly recorded unreachable only when the operation returns its
expected typed success and exact semantic postcondition with a legitimately
shorter observed trace. An error, timeout, or divergent result/state fails the
harness instead of erasing the cut. A required CI or evidence selector must
reach and block at its barrier non-vacuously; `unreachable` cannot satisfy it.
`N` is a bounded search depth, not proof that ordinal `N` is that run's final
effect.

Two additional barriers test the terminal contract directly. At **PRE_ACK** the
operation has returned typed success inside the child, but the child blocks
before sending the public result; the parent kills it and requires the semantic
postcondition. At **ACK** the parent receives typed success, the child blocks
without clean shutdown, and the parent kills/reopens; the postcondition is again
mandatory. These cells cover lost-result and acknowledged-success durability
without inferring either from ordinal `N`.

**V1 sweep envelope.** A qualified case touches at most two graph tables and at
most 32 aggregate rows / 1 MiB of logical input, 64 modeled durable transitions,
4,096 current objects, 64 live incomplete multipart uploads, 256 MiB combined
current-object plus uncommitted-part bytes, and 256 MiB of cumulative payload
forwarded to mutating lower-store calls in one child, including overwritten
objects and abandoned multipart parts. One complete sweep may forward at most 4
GiB and retain at most 256 incomplete upload sessions across all fresh roots.
The parent inventories uploads/parts through the backend test API before
recovery and again at terminal projection; ordinary object listing is
insufficient. Phase 1 includes a small single-table sensitivity case **and** a
mandatory two-participant Mutation/Load case whose projection covers both
participant pointers plus the graph head; a selected cut after the first
participant effect but before graph publication pins the coordinator gap rather
than only Lance's single-dataset atomicity. Each child and recovery process has a
30 s watchdog; one complete ordinal sweep (including PRE_ACK/ACK) has at most 67
cells and a 20 min wall-clock ceiling. Crossing any bound is a typed harness
refusal. If the clean trace has `N > 64`, the qualified sweep does not run. A
separately versioned on-demand diagnostic may declare higher transition,
storage, and time caps and sample that trace, but it cannot satisfy the V1
full-sweep evidence gate. These bounds contain the otherwise quadratic sum of
replayed prefixes.

Dropping an in-process handle is not credited as a crash. The external RustFS
root survives child death, and the wrapper signals a cut only with zero lower
mutating calls in flight. The parent still uses bounded process-reap and backend
visibility deadlines before recovery; a timeout is a harness failure.
After the terminal projection and bounded evidence record are durable, the
parent aborts every inventoried upload and removes its unique test root. Cleanup
is not part of the recovery oracle, but a cleanup failure is loud so scheduled
runs cannot accumulate billed multipart residue.

**Scope.** Phase 1 qualifies the sequencer plus the Mutation/Load `CrashOracle`
and terminal projection only. SchemaApply, BranchMerge, EnsureIndices, Optimize,
and native branch create/delete join only with their writer-specific projections
and residue rules from §3.1.

**Complementarity.** Existing failpoints keep OmniGraph-code windows and
unserialized races that the selected storage schedule cannot reach. The cut
sweep adds systematic coverage of the modeled dual-path durable trace. Neither
subsumes the other.

### 4.2 Seeded operation-sequence fuzzing

**Generator.** proptest-state-machine-style sequence generation with
shrinking, over the alphabet {insert, update, delete, load, query, branch
create, branch delete, snapshot read} — validity model only (3.5). Merge and
schema-apply are excluded from v1 (§12). The seed is a single `u64`.

**V1 envelope.** One sequence has at most 32 operations, 256 live rows, four
non-main branches, eight retained snapshots, 5 s per operation, 30 s total, and
4 MiB combined serialized transcript plus state projections. The canonical
workspace subset is four fixed seeds capped at 16 operations each. Scheduled
search may vary more seeds but not these per-sequence resource limits; shrinking
has its own 60 s deadline and emits the best minimized sequence reached by then.
Any cap hit is a typed harness outcome, never a silent discard or an invitation
to grow the normal suite.

**Checks per sequence.** Compare the complete §3.2 transcript across arms,
apply the §3.5 metamorphic laws, and compare normalized replay under §3.3. V1's
constraint-free fixture avoids conflating syntactic validity with constraint
semantics. When constrained fixtures are later enabled, typed refusals are
compared as observations rather than automatically classified as findings.

**Discipline.** Every run writes a local versioned record containing seed,
generator/alphabet/schema versions, serialized sequence, arm configuration,
non-vacuous operation counts, transcript digest, and verdict. CI uploads
summaries and minimized failures as bounded artifacts. Every confirmed failure
ships as a checked-in minimal regression in the existing owning suite, with the
shrunk sequence as the fixture; raw successful runs need not accumulate in a
shared service. Normal tests write only to their tempdir; scheduled/on-demand
runs require an explicit artifact path. No test appends to a workspace-default
log.

### 4.3 Parser fuzzing

cargo-fuzz targets over the two untrusted-input paths: schema parse → validate,
and query parse → typecheck → lint. Property: no panic, resource escape, or
hang. Each target defines a maximum input length, per-input timeout, child RSS
limit, and whole-job wall-clock budget before activation. The normal compiler
suite replays a checked-in minimized smoke corpus with fixed file-count and
byte ceilings plus permanent minimized regressions. The evolving raw corpus is
a size/retention-bounded scheduled-run artifact, never an unbounded addition to
`cargo test --workspace --locked`. The cargo-fuzz target stays outside the
workspace and confines its nightly toolchain to its own scheduled job; the
workspace remains on stable.

A structure-aware second phase generates typed ASTs via `arbitrary` (so
everything past the typechecker gets exercised) and feeds differential
execution; scoped later, same targets crate.

### 4.4 Schedule exploration

shuttle (randomized PCT scheduling, seed-replayable) over seam 3: the
root-scoped `WriteQueueManager` and the gate-ordering core. The engineering
cost is a cfg seam so shuttle's sync/rng primitives substitute in the modules
under test; the lock-order logic runs without Lance I/O, which is what makes
this tractable. loom is adopted only if a small closed atomic core
emerges that merits exhaustive checking; none is planned.

### 4.5 Hardening phases (later)

- **Kani** on the closed admission arithmetic — Mutation/Load's 8,192-row /
  32-MiB caps and BranchMerge's chunk-plan math. The class has a prior: the
  retired streaming path's Gate R0 closure defect was precisely a
  byte-accounting bug (sparse vs. dense charging); the surviving arithmetic
  deserves the cheaper assurance of proof, and nothing wider does.
- **cargo-mutants**, scoped to `validate.rs` and `exec/merge.rs`, as an audit
  of oracle strength: a mutant surviving the truth table plus these suites is
  a hole in the harness, found before it is a hole in production.

## 5. Relationship to RFC 0031

- **One small envelope vocabulary, at most.** The two harnesses may share
  versioned run/environment stamping. Cost samples and high-volume fuzz/crash
  evidence keep separate schemas, retention, prefixes/artifacts, and reporters.
- **Different canonical contracts.** RFC 0031 owns cross-build fixture and
  post-state fingerprints. RFC 0032 owns typed operation transcripts and
  writer-specific state projections. Similar NDJSON normalization is not enough
  reason to centralize them; existing owners are extended until the shapes
  demonstrably converge.
- **Protocol faults remain later work.** If 503, slow-body, or conditional-put
  injection is later justified, extend RFC 0031's already-qualified proxy rather
  than add a second S3 implementation. RFC 0032 V1 does not depend on that mode.
- **Independent landing order** otherwise; neither RFC blocks the other.

## 6. Rejected approaches (recorded to prevent relitigating)

- **A full reference model** (a second in-memory implementation of graph
  semantics as the fuzz oracle). Two copies of business logic that must stay
  in sync are a perpetual drift liability; when merge or validation semantics
  change, the model must change too, and a wrong model rejects correct
  behavior. The validity-model-plus-differential design (3.2/3.5) gets the
  search without the second implementation. Narrow exception: if a future need
  demands a predictive model, scope it to a closed alphabet small enough that
  the model stays trivially auditable.
- **madsim** — a cfg-patched world requires every dependency to be
  simulatable; Lance's real I/O and rayon index builds are not. Adopting it is
  a fork-and-patch of the substrate, which invariant 1 forbids.
- **turmoil** — simulates TCP topologies and partitions. This system's risks
  are object-store CAS, crash windows, and schedules; the cluster protocol is
  conditional-put on blobs, not a network protocol. Wrong failure model.
- **stateright** (spec-level model checking of recovery) — the current ordinary
  writer envelope is recovery-v9, while the removed RFC 0026 experiment alone
  advanced through several private recovery shapes in weeks. A model that lags
  the implemented writer truth tables is a second source of truth for exactly
  the thing we least want two of. Revisit only for a closed, stable subprotocol.
- **Antithesis / libc-override byte determinism** — buys byte-level replay of
  bugs already found. Logical replay (3.3) captures most of the value at a
  small fraction of the lift. Revisit only when full-run determinism is
  explicitly judged worth its cost.
- **Criterion** — not applicable here, and already rejected in `testing.md`
  for the benchmark harness.

## 7. Where it lives

- **Lower storage front:** `omnigraph-storage` owns the feature-gated injected-
  store constructor plus its wrapper/constructor contract tests. The seam is
  compiled only by its non-default `adversarial-harness` feature, which the
  engine's existing `failpoints` feature propagates for integration tests.
- **Dual-path sequencer and persistent child runner:** dependency-neutral engine
  test support owns the shared gate; Lance receives its `WrappingObjectStore`
  through every create/open/write path, while the lower storage front receives
  the same sequencer. The engine storage facade only delegates.
- **Durable-cut assertions:** an engine integration target owns the child
  protocol, cross-path Mutation/Load oracle, and genuinely new ordinal sweep.
  It reuses `tests/helpers/recovery.rs` and the writer projections/truth tables
  already owned by `failpoints.rs` instead of copying recovery logic. Add the
  lower contracts and integration target to `testing.md` in the landing change.
  `forbidden_apis.rs` plus a storage-crate source guard inventory both acquisition
  surfaces and forbid the raw constructor outside the harness feature.
- **Operation-sequence driver:** an engine integration test reusing existing
  fixture/query collectors and the scoped traversal-mode seam. Expand-specific
  properties remain in `proptest_equivalence.rs`.
- **Fuzz targets:** `fuzz/` in the cargo-fuzz layout, outside the workspace
  (§12, open decision 1).
- **shuttle:** cfg-gated inside the owned modules plus a dedicated test
  target.

No product, format, or protocol behavior changes. Test-only injection inside
production modules is allowed only where the real production acquisition or
storage function is driven unchanged and the seam is structurally guarded; a
copied lock/storage model earns no evidence.

## 8. CI posture

Per the standing CI budget:

- **Canonical Test Workspace invocation:** minimized checked-in regressions,
  bounded parser smoke corpus, four fixed operation seeds, and deterministic
  local failpoint cells run in their existing workspace owners. Post-merge CI
  already compiles one workspace feature superset with engine/cluster failpoints;
  RFC 0032 does not add a second Cargo invocation or a third feature graph. The
  current CI does not run the full workspace suite on every PR, so this is not
  described as a PR gate. Corpus file count/bytes and non-vacuous cases are
  asserted. The ordinary additions own an internally enforced **20 s** aggregate
  parent allocation and the failpoint sensitivity cells own **10 s**, both
  inside that same invocation. Qualified durable-cut cells do not run on local
  FS because it lacks the required lower seam. Full `0..=N` sweeps and shuttle
  search never run here.
- **Scheduled/on-demand workflow:** a new workflow (none exists today) owns
  sanitizer cargo-fuzz on nightly Rust plus deeper stable-Rust operation, cut,
  and shuttle searches. Every job has wall-clock, RSS, input-size, corpus-size,
  and artifact-retention limits and records exact seeds, iteration counts,
  serialized minimized failures, generator/alphabet/schema versions, and tool
  versions. A scheduled finding becomes stop-the-line only after deterministic
  reproduction on current main.
- **Existing RustFS failpoints shard:** the qualified Mutation/Load set `{c=0, c=1,
  first-participant-before-publish, PRE_ACK, ACK}` plus dual-path/event-vocabulary
  sensitivity share an internally enforced **30 s** aggregate parent deadline.
  They extend the existing `rustfs_integration` failpoints matrix cell and its
  `s3_` ownership rather than creating another shard. The three pre-allocated
  test-body allocations sum to **60 s** across the existing workspace and
  RustFS jobs; each parent also has a slightly larger hard timeout for harness
  teardown. Compilation remains existing job overhead, not hidden in a claim
  that these tests are free. A cell that does not fit moves to scheduled/on-
  demand rather than an uncapped job. The full ordinal sweep is scheduled only.

## 9. Evidence gates

Mirroring RFC 0031 §9: a harness that cannot rediscover the bugs that motivated
it is not finished. Every instrument ships a **sensitivity** self-test (a
seeded defect it must find) and a **soundness** run (a clean pass on current
code); where a historical fix can be cleanly reverted on a scratch branch,
that rediscovery is the flagship evidence.

- **Dual-path cut sweep:** first proves that a Mutation/Load trace contains both
  Lance and `omnigraph-storage` control-object transitions, that scheduled cuts
  `0` and `N` plus PRE_ACK/ACK execute, that every mandatory CI selector in §8
  reaches its named barrier rather than being classified unreachable, that no
  later transition lands after the child blocks, and that a byte-identical fresh
  fixture is used for every cut. Multipart sensitivity separately demonstrates
  initiation-before-first-part, part, completion, and abort ordinals and detects
  an incomplete upload that ordinary object listing cannot see.
  The bounded two-participant cell must stop after the first participant effect,
  recover both participant pointers plus graph head atomically, and reject a
  rollback whenever publication/ack authority requires the postcondition. It
  then rediscovers a historical recovery class requiring sidecar plus table
  effects (by clean revert where possible, otherwise an isolated per-instance
  test perturbation). A Lance-only wrapper is a required sensitivity failure,
  not partial credit.
- **Schedule exploration:** with a test-only permutation inverting two gate
  acquisitions in the write-queue/gate-ordering core, finds the resulting
  deadlock within the bounded budget, seed-replayable. Against current code it
  observes no violation within the recorded seeds, iterations, and time. (No
  historical revert exists here: the one prior
  deadlock in this area belonged to the removed streaming admission, so this
  gate is sensitivity-plus-soundness only.)
- **Op-sequence fuzzing:** uses an isolated per-instance perturbation that only
  a composition can expose — for example insert → update/delete → old/current
  snapshot read, or fork → divergent writes → source/child snapshot reads — and
  shrinks it to that multi-operation sequence within a bounded seed budget.
  The already-owned Expand-dedup class remains in `proptest_equivalence.rs` and
  is not claimed as evidence for this new driver.
- **Observation transcript:** a query-only perturbation changes a typed row
  multiset while leaving the final graph digest unchanged; the transcript must
  catch it. Separate fixtures pin ordered-result and typed-error normalization.
- **Parser fuzzing:** catches a deliberately seeded panic and timeout/resource
  escape while enforcing every declared input/RSS/time/corpus bound; scheduled
  sanitizer runs report no violation within the recorded budget.
- **Determinism oracle:** catches a deliberately seeded iteration-order
  nondeterminism while accepting alpha-equivalent fresh IDs/timestamps.

## 10. Phasing

1. Add bounded parser-fuzz targets plus minimized compiler-owned smoke corpus.
   In parallel, spike the persistent RustFS child runner and the shared
   sequencer's Lance-wrapper plus `omnigraph-storage` lower-wrapper fronts;
   define the complete event vocabulary, freeze/kill/drain plus PRE_ACK/ACK
   protocol, Mutation/Load `CrashOracle`, scheduled cuts `0..=N`, and the exact
   §8 per-step budget allocations. No crash-coverage claim precedes this gate.
2. Add the stepwise operation transcript, narrow metamorphic laws, and seeded
   multi-operation driver. Then add writer-specific projections and cut cells
   one writer at a time; row-digest reuse alone is insufficient.
3. Add the production-function-driven shuttle seam and bounded queue/gate
   exploration only if phases 1–2 leave a demonstrated schedule gap.
4. Consider Kani arithmetic proofs and scoped cargo-mutants oracle audit only
   after their narrower sensitivity gates justify the maintenance cost.

Each phase lands with its §9 gate. No phase blocks RFC 0031's implementation.

## 11. Out of scope

- **SI-history checking** (an Elle-style snapshot-isolation checker over
  recorded multi-handle histories). The commit DAG's free version order will
  make this unusually cheap someday; deferred until a concurrent multi-handle
  workload generator exists to record histories worth checking.
- **Byte-level deterministic replay** (§6).
- **Exhaustive enumeration of every concurrent durable-effect subset/order.**
  Each run observes and records one serialized arrival schedule; V1 does not
  exhaust schedules, subsets, or orders (§4.1).
- **Sustained-load / latency-distribution testing** — the RFC 0031 family's
  separate future instrument.
- **Production chaos** against live deployments.
- **Replacing** failpoints, `merge_truth_table.rs`, `proptest_equivalence.rs`,
  or any existing suite.

## 12. Open decisions

1. **Fuzz layout** — `fuzz/` (recommended cargo-fuzz convention, outside the
   stable workspace) vs. an explicitly excluded tool crate.
2. **Alphabet growth** — when and under which oracle `branch_merge` and
   schema-apply join the op alphabet (truth-table composition vs. observation
   transcript vs. both).
3. **shuttle seam shape** — cfg alias inside the modules vs. extracting the
   lock-order core into a shape both production and shuttle drive.

## 13. Amendment (2026-08-17): the deterministic execution layer

Added after the original draft. Since this RFC merged, DST v1 (the first
iteration of the project's deterministic simulation testing) built and
measured a harness that runs engine operations in a fully seeded simulated
world; its design authority is
[RFC 0037](0037-deterministic-simulation-harness.md). RFC 0037 stands beside this
RFC as the simulation architecture; this amendment records only how RFC 0032's
instruments compose with that execution layer. Details live in RFC 0037; this
amendment cites, it does not restate.

### 13.1 §3.3: two determinism contracts, each with its domain

§3.3 chose normalized replay: fresh commit IDs and timestamps are
alpha-normalized to ordinals before transcripts compare. A stronger contract
now exists in the simulation: a run is a function of its (scenario, seed)
pair, where a scenario names the sampled workload's shape and the seed
drives every draw, and IDs and timestamps are seed-determined through the
engine's builder injection points, so they compare directly instead of
being normalized away. The two contracts divide cleanly by execution layer:
instruments running on the real substrate (the RustFS child runner, real
backends) keep normalized replay, the right frame where the substrate mints
identities a test cannot seed; instruments running in the simulated world
get the ID-inclusive contract for free. §3.3 stands as written for its
layer.

### 13.2 Instrument twins in the simulation

Two of §4's instruments now have working twins in the simulated world:

- The §4.1 durable-write-cut sweep's twin is crash-state enumeration: the
  simulation kills the run at each durable write in turn and judges the
  reopened store, with recovery required to either restore the committed
  state or refuse cleanly. The two cover different territory: the sweep's
  cells are evidence about the real substrate's schedules; the
  enumeration's cells cover every durable write its sampled universes
  perform, at in-process cost per cell (a universe is one sampled
  operation stream run in the simulated world).
- The §4.2 generator and the simulation's operation sampler share one
  shape: a single seed drives generation, generation is operation-aware,
  and a new operation kind joins sampling automatically. Whether the two
  converge on one alphabet definition is a landing-time decision, not
  forced by either RFC.

### 13.3 §6 corrected: two entries, against evidence

- The Antithesis / libc-override entry priced byte determinism as one
  bundle. Measured, the libc half is separable and small: the simulation's
  entropy shim is a ~60-line crate-level link-time interposition that
  feeds getrandom-class calls from a seeded stream. The entry's conclusion
  survives for the hypervisor half (whole-VM byte determinism stays
  deferred), but its price argument no longer covers crate-level
  interposition, which is in use today.
- The rejection table never evaluated one lane: contributing the missing
  seams upstream to Lance instead of forking it or buying around it. That
  lane is now open and cheap to name: the first ask is an injectable clock
  (the mock-time ask, evidenced by the counting golden's byte wobble
  recorded in
  [RFC 0031 §11](0031-comparative-cost-harness.md#11-amendment-2026-08-16-the-counting-side-as-built)),
  with entropy and scheduling seams as candidates behind it. Each accepted
  upstream seam shrinks §2's cannot-virtualize list by one.

### 13.4 §4.1 replay hygiene: the checked upgrade path

§4.1 honestly disclaims trace replay: each cut is its own observed
schedule. The simulation contributes a checked pattern for the claim's
strong form: a strict-replay meta-test, which reruns the same (scenario,
seed) pair and byte-diffs the two runs' reports, so the replay claim is
itself under test instead of asserted. If the record/replay arbiter
sketched in §4.1 is ever attempted, that meta-test shape is its acceptance
gate.
