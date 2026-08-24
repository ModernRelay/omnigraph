---
rfc: "0037"
title: "Deterministic simulation harness"
track: public
status: draft
implementation: in-progress
authors:
  - Azim Afroozeh
created: 2026-08-18
updated: 2026-08-23
discussion: null
supersedes: []
superseded_by: []
blocked_on:
  - Upstream Lance entropy and mock-time seams
  - Pool-thread identity propagation
  - Per-instance failpoint registries
---

# RFC 0037: Deterministic simulation harness

The proposed design is backed by the measured prototype and findings described
below. Integration of the complete `crates/omnigraph-dst` harness and its
remaining determinism seams is still in progress.

## Summary

Omnigraph gains deterministic simulation: the real engine runs inside
a fully controlled world (in-memory object store, controlled clock,
seeded randomness, seeded workload). Every source of nondeterminism is
seeded, so one (scenario, seed) pair names one exact
execution, where the scenario is the experiment's rules and the seed
is the one number that picks everything within them. Rerun the pair
today or in a year and it regenerates the identical universe (one
complete simulated lifetime): the same operation stream, the same
generated IDs and timestamps, the same storage calls in the same
order, the same final store state, the same ***verdicts*** (the pass
or fail judgments on the run). That is what determinism buys,
repeatability, and repeatability pays twice. In testing, a failing
universe replays from its (scenario, seed) pair, so the rarest crash
or race becomes
a permanent, debuggable test case instead of a one-time event;
***oracles*** (the independent checks that compare what the store did
against what it must have done) judge every universe while crashes,
faults, and races strike. In benchmarking, every observable event
counts identically on every run, so per-operation costs become
regression tests instead of measurements. Further uses (coverage
census, cost accounting, counting needs not yet named) ride the same
repeatability. This RFC documents the design, the determinism
contract, and the measured results of the first two uses: 12 findings
across 8,000+ universes, three public issues (#473, #494, #495), and a
per-operation cost profile guarded as a CI ***golden*** (a committed
answer file that any diff fails against).

## Motivation

Omnigraph's worst bugs are crash and concurrency interleavings on the
object-storage path. They fire under timing nobody can recreate, and a
bug that cannot be re-run is nearly worthless. RFC 0016 named the goal
(deterministic simulation testing) and RFC 0032 shipped the current
official position (an adversarial harness with normalized replay). Both
stopped short of the same thing: executions that replay exactly.

The long-run liability of NOT having this is measured, not
hypothetical. Before the harness: every correctness bug so far was
found by incident or by hand (RFC 0032's own motivation), and a
55-extra-requests-per-op cost regression shipped unnoticed because
wall-clock benchmarks hide storage costs in variance. With the harness,
in its first week: 12 findings, each replayable from its
(scenario, seed) pair, and the cost-regression class is now a failing
test.

## Guide-level explanation

> Reading convention: a term set in ***bold italics*** is being
> defined at that exact spot, once. Its definition is either the
> parenthetical immediately after the term, or, when the term opens
> the sentence or bullet, that whole sentence. Afterwards the term
> appears in plain text. The Definitions list below holds the core
> objects; every other term is defined inline where it first appears.

### Definitions

One line each; every term's full story lives in the architecture
sections that follow.

- **World**: one graph's observable durable state at a moment; state,
  not a run.
- **Scenario**: the experiment's rules; what CAN happen. One scenario
  admits many universes, one per seed (the simulation-literature
  sense: a condition you replicate, not a single unfolding).
- **Seed**: one number; picks what DOES happen within the rules.
- **Universe**: one judged lifetime around a world, named by
  (scenario, seed).
- **Reference model** (short: the model): the harness's own
  implementation of expected state; the expectation half of every
  verdict.
- **Territory**: the set of every universe some (scenario, seed) pair
  can produce today: seeds range over all of u64, scenarios over every
  combination the current knobs and operations can express. It widens
  exactly when a new knob or operation becomes expressible, and never
  from running more seeds.
- **Crash window**: the gap between two consecutive durable writes of
  one operation, where dying leaves a distinct partial state that
  recovery must handle. The engine catalogs 66 of them.
- **Multiverse**: Antithesis's term for a fork-tree of one execution;
  this harness approximates its payoff without forking (see "Reaching
  rare states" below).

### The design: five components

The harness design has five components, each answering a question the
other four cannot ask, and each question earns its place by what goes
wrong when nobody asks it:

- ***Reach***: which states can the engine get into? An
  ***instrument*** is one built capability of the harness (a workload
  arm, a fault knob, a crash scheduler, a recipe); all the situations
  the instruments can create form the territory, and a defect
  outside the territory is unfindable by everything else in this
  design, at any effort.
- ***Fault injection***: which disasters strike there? A healthy-world
  suite never executes error and recovery code, so the least-run code
  is exactly where the worst bugs live; unasked, that code ships
  untested.
- ***Oracle***: would a wrong result even be noticed? Determinism only
  makes failures reproducible; without judgment the harness replays
  silent corruption and calls it green.
- ***Volume***: how many universes run? Each universe visits one
  seed-chosen path through the territory, the way one dealt hand
  visits one possibility of a deck; rare-but-reachable defects need
  many deals, and no number of deals creates a card outside the deck.
- ***Method***: can the coverage and the numbers be proven? Without
  accounting, a green run claims everything and certifies nothing; the
  ledger, the versioned identity, and the goldens are what make the
  other four's claims checkable.

The connections run both ways: reach without oracles explores blind;
oracles without reach judge only easy states; volume without new reach
re-rolls the same dice (measured by ***DST v1***, the build-and-hunt
effort behind this harness, whose recorded runs and findings this RFC
cites: new territory finds bugs, volume only multiplies territory). One component at a
time, top down.

#### One universe, end to end (all five components in action)

A universe runs: birth, a fresh in-memory object store sharing nothing
(reach built the world); life, a seeded workload of ~30 operations,
optionally one injected fault or scheduled crash (reach picks the
ops, fault injection strikes); judgment, every observation compared
against the reference model (oracle); teardown. About 0.1 seconds
each, and a nightly ***fleet*** (the recurring mass run of fresh
universes) delivers thousands (volume). The failure
report is the scenario plus `seed=30012`, replaying to the identical
execution, and every recorded number cites its run (method).

### Reach: which states the engine gets into

#### The two inputs

- **Scenario contains the rules:** how many operations a life holds,
  which operation kinds are allowed (including a widened alphabet and
  a hostile naming alphabet), which storage faults may strike, where a
  crash may land (a fixed index, a family-matched operation, the k-th
  durable write, or inside recovery itself), and which rare state to
  construct or probe. Together the rules define what CAN happen. (The
  field-by-field mapping to the `Scenario` type is reference-level
  material below.)
- **Seed picks within the rules:** one number, fed to the ***sampler***
  (the component that turns the pair into the workload; its own
  section follows), which derives every concrete choice: the exact
  operations, their order, their data. It defines what DOES happen.

#### The sampler: one seed, operation-aware, forward-compatible

The sampler turns (scenario, seed) into a concrete workload: the
operation stream of a universe's life, not the universe itself (the
harness around it builds the world, injects the faults, and runs the
judgment). Three properties define it.

**1. One seed suffices.** The seed fans out through a ***seed tree***
(a deterministic derivation of child seeds; SplitMix64): one child for
the workload stream, the ID stream, the logical clock, the scheduler,
and, in concurrent universes, one per writer. Every random-shaped
decision in the universe traces to the one root number. Operations are
therefore never stored: a workload is always re-derived from
(scenario, seed), so a bug report is a scenario plus one number,
storage-free and immune to transcript drift. Why it matters: if
randomness entered anywhere else, replaying a failure would mean
capturing every source of it, and a universe would have no single name
for the record to cite; the seed tree is what keeps one number
sufficient.

**2. Operation-aware.** The sampler does not emit an opaque byte
stream; it knows operation kinds and their relationships. Why it
matters: an unaware sampler cannot aim. Crashes would land on
operations that cannot reach the targeted window, composed rare states
would exist only by luck, and no storage call could be billed to the
operation that caused it. Awareness enables three mechanisms:

- Crash placement matches op families: `crash_on_match` schedules the
  crash on the N-th sampled operation whose kind can actually reach the
  targeted crash window, so merge windows get hit by real merges.
- ***Window recipes*** treat a composed operation sequence as one unit:
  states that need fork, then branch write, then merge, in that order,
  are reached by weaving those milestones into the seeded stream, with
  positions and parameters still seeded. Rare states become certain per
  recipe universe instead of vanishingly improbable. Measured:
  crash-window coverage moved from 35/66 to 50/66, at most three seeded
  universes per newly crossed window.
- Cost attribution labels every storage call with the op kind that made
  it, which is what turns the harness into a benchmark (RFC 0031's
  counting side).

**3. Forward-compatible.** New operations join without invalidating
anything that exists. Why it matters: the harness must outlive today's
engine. Without this property, every added operation would silently
change what old seeds mean, and pinned tests, recorded runs, and the
coverage record would all rot at once. The convention:

1. A new op kind enters behind a scenario flag (the `wide` precedent),
   so every pre-existing pinned seed keeps its exact op stream. A seed
   never changes meaning; new behavior is new input, not redefined
   input.
2. The engine's new ***failpoints*** (named in-code crash hooks marking
   each window) enter the ***window catalog*** (the enumeration of
   every failpoint the engine ships; what EXISTS, where the ledger
   below records what has been DONE) under a standing rule: the
   catalog always names the failpoints of the engine version the
   harness runs.
3. New windows appear in the ***coverage ledger*** (the record of every
   window's standing) as named dark rows until crossed; recipes are
   written where dice cannot compose.
4. Any coverage-affecting change bumps the versioned ***harness
   identity*** (the number naming what the harness could see and do
   when a run was recorded), so runs before and after are never
   silently compared.

#### Reaching rare states: recipes instead of forking

This is where the multiverse question lands. In the Antithesis
vocabulary a multiverse is a tree of executions branched from mid-run
snapshots of one machine: reach an interesting moment once, freeze it,
resume it a thousand different ways. That amortizes rarity, and it
requires owning the layer beneath the OS; snapshotting a live process
is hypervisor territory, the purchasable tier.

This harness does not fork. It approximates the multiverse's payoff
from two sides: window recipes construct the rare state from birth by
weaving (reaching by construction what a fork tree reaches by search,
with the measured coverage move above as the yield), and the fleet
multiplies independent draws across seeds. Cheap universes are the
compensation: rebuilding an interesting moment from birth is
affordable when birth to death costs 0.1 seconds.

### Fault injection: which disasters strike

Faults enter through two composing mechanisms: ***call-level*** faults
at the storage boundary (each storage call, taken as one indivisible
unit, can be made to fail, lie, or vanish, but never to stop halfway)
and ***line-level*** crash points at the engine's failpoints (the
process dies mid-operation, at a marked line, even between storage
calls). Three ***failure models*** (a failure model is one
answer to two questions: who sees the fault, and which code body must
survive it) ride them, and the two questions define each model
exactly:

| # | Name | Imitated reality | Who sees the fault | What must survive | Positions indexed by | Mechanism |
|---|---|---|---|---|---|---|
| 1 | injected storage faults | the store misbehaves on a call | the operation (an error or wrong bytes) | its retry and error paths | the call | call-level |
| 2 | crash windows | the process dies at a phase boundary | nobody; the process is gone | recovery, from a marked place | code location (the 66 cataloged windows) | line-level |
| 3 | crash-state enumeration | the process dies between durable writes | nobody; the process is gone | recovery, from every write cut | write ordinal (W writes = W states) | call-level (a write counter) |

Per model, what is built and measured:

1. The scenario's `faults` field (the `FaultPlan` from "The two
   inputs") injects clean errors and latency, corruption on the read
   path (value-aware bit rot, truncated reads, persistent latent
   sector errors), lost and misdirected writes, ack-loss (the effect
   durable, the confirmation lost), and bounded staleness.
2. The judged contract is two-sided: a failed operation is invisible
   XOR fully applied, and committed writes never roll back.
3. Since memory dies with the process, storage is the only surviving
   witness: dying anywhere between the same two durable writes leaves
   the identical surviving store, so a run with W durable writes
   defines exactly W distinguishable crash states. The write ordinal k
   is therefore not a sampling heuristic but the exact coordinate
   system of distinguishable aftermaths (a time-indexed kill would
   duplicate some states and miss others; an instruction-indexed one
   needs a hypervisor). Kill-at-kth-write (with post-mortem refusal of
   all storage calls until revival) manufactures every one. Measured:
   a full enumeration of 326 states, 326/326 with zero violations, 50
   of them absorbed deaths (legal best-effort losses the engine never
   promised to keep).

The counts differ because mechanisms name entry points and models name
imitated realities, mapped many-to-one (the table's last column).

**Why these three suffice.** The claim is completeness over disaster
entry points, not over bugs (logic bugs on healthy paths are the
oracles' job under normal workload). An engine process has exactly two
doors the outside world can hurt it through: its storage calls (the
only external dependency it has) and its own death. A fault through
the first door is model 1; death is either placed at a chosen line
(model 2) or classified by how much durable work survived (model 3,
complete by construction). There is no network door to model:
omnigraph nodes never send each other messages; they coordinate
through shared state in the store, with compare-and-swap, so a lost or
stale message in this architecture IS a failed or stale storage call,
already inside model 1. One named boundary remains deliberately
outside this RFC: the server surface above the engine (a client
disconnecting mid-request drops a future at an await point, which runs
destructors rather than killing a process); making the server
deterministic is later work, not a gap in the engine's model set.

**Why two mechanisms cover every injection point.** The same doors,
seen from the injector's side. A running engine is interposable at
exactly two grains: its storage calls, every one of which passes
through the boundary wrapper (so any call's fate can be dictated), and
its own execution, which without a hypervisor can be interrupted only
at lines someone marked (so death can be placed exactly at the
failpoints, and counted-write deaths ride the boundary's write
counter). Nothing else the engine observes is a fault surface: clock,
randomness, and IDs are seized by seams, which are control inputs the
harness sets, not disasters it inflicts. So every injectable disaster
is either the fate of a call or a stop of execution; a third mechanism
would need a third grain, and the architecture has none.

### Oracle: noticing wrong results

The reference model is the component's foundation: the engine under
test never supplies its own expectations (the accused may not testify
about itself), so the model applies each operation's contract to cheap
structures of its own, including the physical/logical split: rows the
store carries that no query should show.

#### The observation channels

The five ***observation channels*** (claim, query, physical, history,
session) all read one truth, the graph's durable state, but each rides
a different engine surface, and each catches a defect class the other
four are structurally blind to:

| Channel | What it reads | What only it catches | DST v1 case |
|---|---|---|---|
| claim | the write path's asserted effects, op by op | the write path lying about its own work, in either polarity: claimed-but-invisible, visible-but-unclaimed | the (claim, birth-contract) detector caught #495: an error return after a durably completed init destroyed a finished graph's schema files |
| query | what queries and traversals return, the surface a user sees | wrong results served to a correct store | the (query, legal-rejection) detector caught #494: apply_schema poisons live-handle traversals |
| physical | the raw stored rows via the export surface, ghosts included | rows the store carries that no query will show; the physical = logical plus ghosts contract | its first run caught a defect in the HARNESS itself, a wrong ghost tie-break in the model: the channels police the judge too |
| history | time-travel reads of recorded past versions | the past changing: a committed version answering differently later | discovered the retention horizon on its first run: maintenance retires old versions, so verified history truncates at every cleanup |
| session | the same graph through differently-aged session handles | staleness: a stale handle disagreeing with a fresh one | established that data-plane reads are read-time-fresh, locating the staleness class in metadata and plan caches, which is #494's dimension |

Why independence detects: one truth, five readers. A defect in one
path surfaces as disagreement between paths, so a channel does not
need to know it is broken to be caught. Issue #474 is the worked
example: an edge write was acknowledged (claim), physically stored
(physical), and invisible to most query spellings (query); the
claim-versus-query disagreement is what made a silent lost write
loud.

Why all five and not fewer: remove any one and its column above goes
dark. Without physical, ghost rows are unobservable; without history,
the past can rot silently; without session, staleness is invisible by
construction, because every check would use a fresh handle; without
claim, the write path is trusted on its own testimony; without query,
the one surface users actually read goes unwatched. One honest
boundary: all five are engine-mediated readings, so their independence
is between paths, not from the engine as a whole; the non-engine half
of every verdict is the reference model.

#### Verdicts and detectors

Determinism only makes failures reproducible; oracles make them
visible. The design: every verdict is observation plus independent
expectation plus mechanical comparison; oracles pair with channels
into ***detectors*** (a detector is one channel-oracle pairing, the
unit that fires); and every green is backed by a ***sensitivity
proof*** (the oracle demonstrates it can go red on a planted defect
before its green is trusted). As built today: 21 oracles pairing into
24 detectors, with the census generated from the code itself so the
documented set cannot drift from the real one.

### Volume: how many universes run

The fleet re-draws from the territory: fresh seeds nightly over
the built scenario shapes. Seed-keyed universes shard across processes
with zero code changes (each universe is a pure function of its pair,
so shard boundaries are bookkeeping): measured, 300 seeds yielding
2,356 universes in about 140 seconds across 12 processes. Every fleet
failure row carries its scenario plus seed and replays on its own.
Volume's role is stated exactly: it multiplies draws from reachable
territory and never widens it; the widening is reach's job, which is
why the fleet's find rate follows the newest instrument, not the
biggest night.

### Method: the accounting that makes claims honest

#### Coverage, stated honestly

Coverage is a ledger, not a scoreboard: 50 of 66 crash windows crossed
with verdicts; the 16 dark windows are each named with an unlock
condition, in three classes: quarantined on a known bug (#494 blocks
schema ops in the sampler until its fix), owned by a filed future
workload (blob-bearing paths), and recipe debt (recovery internals
whose preconditions have no recipe yet). 50 crossed plus 16 named is 66
accounted for.

#### Provenance

Every recorded number cites a chain: a numbered run (`runs/run-NNN`,
immutable once recorded) names the harness identity it ran under; the
identity names the design version. Runs before and after an identity
bump are never silently compared. Numbers in this document bind to
recorded runs, not to live artifacts.

#### The benchmark dividend

The simulation cannot measure wall clock, but it counts storage actions
exactly. Per-op call counts are a golden file in CI: any cost change
fails the suite as a named regression ("Optimize l.get moved 979 to
N"). Byte totals join once Lance's wall-clock-into-file-bytes write is
behind a ***seam*** (an injection point where a controlled stand-in
replaces a real dependency): the upstream mock-time ask.

#### Where findings are proven

Every finding is caught and reproduced in simulation; no finding has
been replayed against a real backend. For the v1 findings that proof
is complete anyway: they are engine-logic defects (a merge rule, an
init cleanup path, a delete race), and the broken sequence lives in
engine code that runs the same on any backend. The boundary is stated
for the class that will need more: a finding whose mechanism depends
on backend behavior (visibility timing, conditional-write races,
listing semantics) carries a real-backend replay obligation, and that
replay runs in the same after-merge real-backend lane as the cost
columns. Simulation finds and proves; the real backend audits the
simulator's model of it, never replaces it.

### Determinism: what holds today, and what remains

**What holds.** For sequential universes, the enumerated identity from
the Summary holds in full. Each source of nondeterminism, and what
seized it:

- **The workload**: derived from the seed tree; the operations, their
  order, and their data are functions of (scenario, seed).
- **Identifiers and timestamps**: dedicated ID and clock seams make
  them seed-determined; this is what makes the contract ID-inclusive,
  so they stay comparable instead of being normalized away.
- **OS randomness**: libc symbol interposition answers the entropy
  calls from a seeded stream; this also covers hash-map hash seeds, so
  hash-based collections stop varying per process.
- **Iteration order**: roughly thirty data-structure walks
  canonicalized, so no visit order is left to the hasher, under a
  standing rule: an iterated map uses insertion order (`IndexMap`)
  when its order needs only stability, canonical key order
  (`BTreeMap`) when the order carries meaning; lookup-only hash maps
  are harmless and stay (iteration is the leak, not storage). The rule
  is enforced by lint, not memory: `clippy::iter_over_hash_type` fires
  on any new walk of a hash-ordered structure. This mirrors the
  repository's own deny-list bar on hash-map iteration order reaching
  observable output.
- **Storage**: the in-memory object store; beyond removing real IO, it
  quiesces Lance's adaptive machinery (constant near-zero latencies,
  no io_uring threads ever spawn).
- **The async scheduler**: one runtime per universe, with seeded
  scheduler randomness and a paused clock.
- **And it is checked, not assumed**: an active replay meta-test
  reruns the pair and requires identical output before any other
  claim is made.

**Multiple actors in one universe (measured boundaries).** Concurrent
universes race several writers on purpose; determinism there is seized
in three pieces and measured at its edge:

- **Per-actor seed streams**: each writer draws from its own child
  seed, so its values depend only on its own name and draw count,
  never on when the other actors draw.
- **Seeded turn-taking where actors have names**: the seam scheduler
  grants storage turns from the seed, and at the engine's write queue
  escapes are zero by construction; no named actor slips past.
- **Order-blind judgment for what still races**: writes self-identify,
  and the judge reconstructs whichever serialization actually happened
  and checks its legality, instead of predicting one outcome.

The measured residue is Lance's internal pool threads, whose calls
carry no stable identity: at the quiesced shape their unattributed
traffic is zero, while unquiesced hunts measure thousands of
unattributed calls per universe. The standing doctrine follows the
measurement: hunt unquiesced (racier finds more), replay quiesced
(deterministic).

**What remains, each with an owner:**

1. Lance-internal entropy (retry jitter, internal ID draws): closed by
   a prepared upstream Lance contribution (four flag-gated changes in
   one crate, with measured evidence: twelve identical-seed concurrent
   runs produced six distinct worlds unpatched, eleven of twelve
   identical patched), not yet posted.
2. The pool-thread identity gap (the unattributed calls above): closed
   by carrying task-local identity through Lance's spawn boundary;
   scoped as follow-on work.
3. Wall-clock timestamps written into file bytes: closed by the
   upstream mock-time seam (a one-line gate widening asked of Lance),
   with a libc-level clock interposition as the in-house interim;
   unblocks byte totals joining the cost golden.
4. Strict replay for concurrent universes: arrives when items 1 and 2
   close; sequential replay is already strict.
5. Per-instance failpoint registries (a dependency swap): makes
   universe independence structural instead of disciplined, and
   unlocks in-process parallel universes. Verification rides with it:
   order-swap and first-versus-last position tests (the #503 table's
   AB/BA row) stand guard in CI so no future shared state quietly
   reintroduces a leak between universes.

## Reference-level design

Module names in this section are as they land in the harness
contribution; line-level anchors belong to that PR's review.

### The Scenario type, field by field

One struct carries the rules and the seed (`Scenario` in the harness
crate; conceptually two inputs, structurally one recipe value):

- `seed`: the u64 root of the seed tree (SplitMix64 derivation).
- `ops`: how many operations the universe's life holds.
- `wide`: widens the sampler's op alphabet with schema evolution, bulk
  loads, and refresh operations (gated so old seeds keep their exact
  streams).
- `hostile`: draws entity names from a hostile alphabet (unicode, very
  long, keyword-like, whitespace) instead of the clean set.
- `faults` (a `FaultPlan`): which storage faults may strike, and how
  often.
- `crash_at`: schedule a crash at a fixed operation index.
- `crash_on_match`: schedule it on the N-th operation whose kind can
  reach the targeted crash window.
- `probe_only`: with `crash_on_match` set, install a record-only probe
  instead of an injected failure, so the census can tell "never
  reached" apart from "crossed but absorbed".
- `die_at_write`: kill the process at the k-th durable write.
- `recovery_crash`: schedule a second crash inside recovery itself.
- `reach_target`: weave a recipe's milestone operations into the
  stream to construct one rare state.
- `probe_window`: hold a record-only probe on one window for the whole
  universe, for the coverage census.
- A test-only tail, never set by pinned tests: one sensitivity lever
  (`fail_maintenance_rerun`, forcing a red to prove an oracle can
  produce one) and eight triage-only ablation and bracketing knobs
  used to localize one finding's trigger.

### The two realms and their seams

Storage traffic reaches the object store on two paths, and the harness
interposes both without modifying either side:

- **Adapter realm** (`a.*`): innermost wrappers around the engine's own
  storage adapter: fault injection (`FailingStorage`) and cost
  counting (`CostStorage`) compose as layers, inserted per universe
  only when the scenario arms them.
- **Lance realm** (`l.*`): a provider registered in Lance's public
  object-store provider registry under the engine's shared-memory
  scheme, decorating every call Lance issues. Zero Lance code changes;
  the registry is a public extension surface.
- **Entropy**: a libc symbol-interposition module (~60 lines,
  env-gated, inert by default) answering the OS entropy calls from a
  seeded stream. It is the only seam that is process-global by nature,
  so it is re-seeded per universe as a mandatory setup step.
- **Clock and IDs**: builder-injected seams making timestamps and
  identifiers seed-derived; these are the additive production-API
  touch points (injection parameters with real defaults).

### The determinism contract, precisely

For a sequential universe, the external trace includes the operation
stream, every generated identifier and timestamp, every storage call
and its order, the final store state, and the verdicts; that whole
trace is a function of (scenario, seed). The contract is verified per
run, not assumed: the replay meta-test reruns the pair and
byte-compares the reports before the golden or any oracle claim is
trusted. Relative to RFC 0032's 3.3: identifiers and timestamps are
inside the compared trace here (seed-determined), where 3.3 normalizes
them away; 3.3 remains the right contract for instruments running on
the real substrate.

### Concurrent universes

One seeded serialization point (the seam scheduler) sits behind every
mutating actor's storage wrapper in both realms. Turns name actors,
never calls, so determinism follows by induction over the grant
sequence; a stalled actor causes a re-draw, and every bypass is
counted as an escape. Inside the engine, the write queue's lock
waiters are held in visible slots whose acquisitions and releases both
take turns, which is what makes escapes zero by construction there;
the escape counter stays on as a standing detector for any foreign
lock introduced later. Judgment does not predict an interleaving:
writes carry self-identifying values, and the judge reconstructs the
serialization that actually happened, then checks its legality (no
lost update, no double-apply, program order per writer, exact final
state).

### Crash machinery

The window catalog names the failpoints compiled at the engine's
write-path phase boundaries (66 at the surveyed version), kept honest
by a names-guard test that fails when catalog and engine drift. A
scheduled crash is scoped to exactly one operation and disarms on
guard drop, so a panicking test cannot leak a live trap; per-window
census states are scheduled, hit, never reached, and unschedulable,
so dark windows carry reasons. Kill-at-kth-write maintains one durable
write counter across both realms; death is followed by post-mortem
refusal (a dead process performs nothing) until the explicit revival
that models restart, and `recovery_crash` schedules a second death
inside recovery itself, the double fault.

### Fault plans

The `FaultPlan` carries per-class probabilities and latency, plus:
read-path corruption (value-aware bit rot, truncated reads, and
persistent per-object latent sector errors), write-side weather
(corrupted, lost, and misdirected writes), ack-loss (the effect
durable, the confirmation replaced by an error, across the write
classes including conditional writes), and bounded staleness (per-key
version history with as-of reads and listings, conditional writes
strict at head). Injected damage is judged detected-or-harmless by
attribution: a damage ledger records what was injected, so
engine-born detection errors are legalized by overlap instead of
guessed at.

### Provenance

Every recorded execution is a numbered run whose meta names the
instrument and the harness identity it ran under; identities are
append-only versions, each naming the design version it implements.
The citation chain is run to identity to design, and numbers in
documents bind to immutable run metas, never to live artifacts. Two
census artifacts are generated from code rather than written: the
detector census (from the oracle enums) and the cost golden (from the
counting pass), so the documented sets cannot drift from the real
ones. The coverage ledger holds one row per window with its census
state, its verdict when crossed, and its named unlock condition when
dark.

## Invariants & deny-list check

No invariant is weakened; several are the harness's test subjects.

- **Invariant 1 (respect the substrate):** interposition uses public
  extension surfaces only (Lance's provider registry and the engine's
  own adapter trait); no private APIs, no fork. Where Lance itself
  lacks a seam (its clock, its entropy), the closure path is upstream
  contribution, not a reach-through, consistent with the deny-list's
  bar against forks of the engine or substrate.
- **Invariants 2, 4, 5 (one publication door; publish once; recovery
  in the commit protocol):** the harness adds no write or publication
  path; universes drive the engine's own doors, and the crash contract
  plus recovery obligations exist to TEST exactly these invariants
  under death and fault.
- **Invariant 6 (strong consistency default):** the one deliberately
  weaker mode (the staleness-modeling store) is explicit, test-only,
  scenario-armed, and non-default, which is the exact carve-out the
  invariant prescribes.
- **Deny-list touch points:** "hash-map iteration order in result
  ordering": the harness's canonicalized walks enforce the same rule
  on itself. "A performance claim without a checked-in instrument":
  the counting golden IS such an instrument, checked in. "Shipping
  observable behavior as if not part of the contract": the goldens
  make observable behavior explicitly contractual. Nothing on
  the deny-list is brushed: the harness is dev-only, every instrument
  is slot-armed and off by default (zero draws, zero behavior change
  unarmed), and the only production-code touch points are additive
  builder injection parameters with real defaults.

## Drawbacks & alternatives

- RFC 0016 surveyed the tooling and concluded the practical choice was
  between in-process simulators (rejected: Lance cannot be shimmed
  without a fork) and the Antithesis hypervisor (deferred: cost). Its
  table has no row for the option this RFC implements: a custom-seam
  harness on stock tokio that quiesces Lance by configuration and
  measures the residue instead of forking or buying. The measured
  envelope is the evidence that the missing row was viable.
- RFC 0032 chose normalized replay (its 3.3): alpha-normalize fresh IDs
  and timestamps, then compare. This RFC's contract is stronger where
  it matters: IDs and timestamps are seed-determined, so they stay
  comparable instead of being normalized away. 3.3 is engaged as what
  it is, a boundary choice on one axis, not a competing technique.
- RFC 0032's rejection of the libc route priced it as an
  Antithesis-scale purchase; measured, it is a ~60-line crate-level
  interposition pattern (the entropy shim already in the harness).
- PR #503 (the node-versus-edge logical-cost comparator) is an
  independent, concurrent implementation of RFC 0031's counting idea on
  real backends. Its relationship to this harness, element by element:

  | #503 element | What it does | Disposition here |
  |---|---|---|
  | real-backend comparator (local FS + RustFS 1.0.0-beta.12) | counts the storage calls of a single insert on real backends, gated by hand-tuned upper bounds; measured: node insert 14 ops (12 reads, 2 writes) and edge insert 34 ops (32 reads, 2 writes) on local FS; node insert 37 ops (30 reads, 7 writes) and edge insert 57 ops (50 reads, 7 writes) on RustFS | v2 of this harness takes over this job: after each merge, the counting test also runs against a real backend. #503 measures two ops (one node insert, one edge insert) and sets their limits by hand; the harness measures every op kind, and the run itself writes the table that the next run is checked against. The same op costs different amounts on different backends, so the table gets one column per supported provider (S3, MinIO, R2, GCS). This lane is one part of supporting more backends over time, including eventually consistent ones (backends whose reads may briefly lag writes), whose behavior the harness already models with its bounded-staleness store. The full design goes to the DST benchmark's v2, not this RFC |
  | AB/BA order-swap | both measurement orders must reproduce exactly | v2, credited to #503: it proves measurement independence, not just determinism, which this harness's same-order replay test does not. One caution: a swap is a detector, not a proof, since a leak can accumulate across many universes, hide in one specific pair, or shift both orders equally. Independence itself is made structural first (each universe owns its store, runtime, and seed streams; the one remaining shared state, the failpoint registry, is item 5 of the determinism residue), and the swap plus its stronger forms (several orders; the same universe measured first and last in a long pass) then stay in CI as the alarm that the isolation keeps holding. Neither half is designed in this RFC: the measurement half goes to the DST benchmark's v2, the isolation half to the DST harness's v2 |
  | per-field ceilings | upper-bound gates tolerant of benign drift | v2: the budget form this cost gate grows into; this goes to the DST benchmark's v2 design, not this RFC |
  | declarative branch-protection context | CI policy shipped in-repo, applied post-merge | v2: the pattern this harness's CI wiring follows; this goes to the DST benchmark's v2 design, not this RFC |
  | S3-prefix leak check | proves no keys remain under test prefixes | v2: rides along when this harness's counting runs on real buckets; this goes to the DST benchmark's v2 design, not this RFC |
  | digest-pinned backend identity | the substrate versioned by image digest | v2: same, substrate identity joins the harness identity chain; this goes to the DST benchmark's v2 design, not this RFC |
  | exact AB/BA reproduction on both real backends | logical counts stable outside simulation | evidence from outside our own work that this RFC's main claim holds: an operation's storage-call counts repeat exactly on real backends too, not only in simulation |

  The intent, stated plainly: this harness is proposed as the one
  cost-counting instrument, with #503's techniques and its
  real-backend role absorbed into the harness's v2, credited
  throughout. The absorption is gated, not assumed: unresolved
  question 4's same-ruler comparison must first show the two
  instruments agree where their rulers overlap. Its first measurement
  (question 4) shows exact write agreement and near edge-read
  agreement against the RustFS column, with one named residual (node
  reads) still open.
- Do nothing: the pre-harness record (bugs found by incident and by
  hand, cost regressions invisible) is the do-nothing cost, already
  paid at least once.

## Reversibility

Dev-only crate plus feature-gated seams; no on-disk, wire, or format
impact. Removing it deletes tests, not behavior. The one production
surface touched is builder injection points (clock, entropy, IDs,
storage), which are additive.

## Unresolved questions

1. Does this RFC amend RFC 0032 in place or stand beside it as the
   execution layer its instruments run on?
2. How much of the findings ledger belongs in the RFC body vs linked
   issues? (Unfiled findings stay count-only regardless.)
3. Number: 0037 assumed; re-check for in-flight collisions at PR time
   (the number is reserved by the merge, per the lifecycle doc).
4. This harness's whole-universe cost profile and #503's fresh-store
   marginal costs show very different numbers; the divergence
   decomposes across four named axes (marginal versus summed
   denominator, counting boundary, store age, backend). A first
   same-ruler measurement now exists (bench run-002: a fresh-store
   single-op scenario, counted on the Lance realm only, the two
   rulers' overlap): write counts match #503's RustFS column exactly,
   7 puts on both insert kinds; edge-insert reads land within three
   calls (53 versus 50); node-insert reads are the one open residual
   (47 versus 30, #503's indexed fixture the prime suspect). The
   local-FS column agrees with neither, which is the backend axis
   measured from this side: the in-memory object-store backend
   behaves like a real object store, while a filesystem's atomic
   rename elides the staged writes an object store must make.
   Remaining before the full verdict table: whether #503's "reads"
   include listings, and an indexed-fixture variant of the floor
   scenario.
