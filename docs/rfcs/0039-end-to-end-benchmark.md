---
rfc: "0039"
title: "The end-to-end benchmark"
track: public
status: accepted
implementation: in-progress
authors:
  - Azim Afroozeh (@azimafroozeh)
created: 2026-08-19
updated: 2026-08-26
discussion: "https://github.com/ModernRelay/omnigraph/issues/539"
supersedes: []
superseded_by: []
blocked_on:
  - Target-path benchmark run records
---

# RFC 0039: The end-to-end benchmark

Implementation is landing in independently reviewed slices.
[PR #551](https://github.com/ModernRelay/omnigraph/pull/551) landed the engine's
merge-timing seam; the declarative harness and target-path run records remain in
progress.

## Evidence status of this document

**This document is design intent, not evidence.** No runs on the instrument's target path (the public surface) exist at this writing; prototype runs informed the design, and none are citable until run records land with the harness. This document therefore contains zero measured claims: every number below is a design parameter or a citation of an external benchmark tradition. When target-path runs exist, published numbers enter by citation of run records (defined below), never retyped.

## Summary

Omnigraph gains its ***end-to-end benchmark***: one instrument that drives the public surface and measures both elapsed time and storage calls. Every run applies a workload to a fixture under stated conditions, measuring one system under test; that description (equivalently, one level chosen for every factor in five classes) is persisted as a run record which every published number must cite, and seven protocol rules make a rule-breaking number invalid, or a rule-breaking practice nonconforming, rather than merely unpolished. The instrument runs under two named profiles (a profile is a canonical region of the space of run descriptions, named so it can be invoked without reciting parameters): the micro profile (single-operation synthetic workload, per-phase attribution: it attributes time to mechanisms) and the realistic profile (scheduled realistic workload against a named backend, reporting latency percentiles, throughput, answer quality, and cost: it measures what users feel). End-to-end means over the ***public surface*** (the interfaces omnigraph ships for its users, as opposed to internal seams): both profiles drive it; the micro profile simulates micro-benchmarking through the same door users enter, so its numbers carry the same request path the realistic profile's do. When a realistic number moves, the micro profile says which mechanism moved it. The two profiles are not two instruments: they share the harness, the record schema with its identity keys, and every rule below; they differ only in factor levels.

The whole contract, as one tree (axes and sweep points illustrative; the harness documentation, arriving with the harness PRs, owns the full lists):

```
end-to-end benchmark: one instrument, two profiles
│                        (micro: mechanism attribution · realistic: what users feel)
├── a RUN = a workload applied to a fixture under conditions,
│            measuring one system under test
│            (equivalently: one level per factor, in five classes)
│   ├── 1 Data ─ what is logically stored
│   │   ├── provenance ············· synthetic · corpus-derived
│   │   ├── row count N ············ 1k · 100k · 750k
│   │   ├── table count T ·········· 8 · 50 · 140
│   │   ├── payload bytes per row ·· ~100 B · 4 KiB · 32 KiB
│   │   ├── column shape ··········· scalars · +vector · +blob
│   │   ├── topology skew ·········· uniform · power-law
│   │   └── scale factor SF ········ spec-generator shorthand: sets N, delta size,
│   │                                and history depth jointly, recorded as the
│   │                                resulting levels, never as a factor of its own
│   ├── 2 State ─ how the store got that way (anti-showroom)
│   │   ├── F1 aging ··············· bulk-loaded · thousands of small commits
│   │   ├── F2 index existence ····· none · BTREE · FTS · ANN
│   │   ├── F3 index freshness ····· optimized · rows-stale
│   │   ├── F4 deletion history ···· none · heavy, at equal live rows
│   │   ├── F5 compaction recency ·· optimized · not
│   │   └── history depth ·········· commits per branch
│   ├── 3 Workload ─ what the run does to it
│   │   ├── operation kind ········· query shapes (hops, selectivity, width) · writes · branch controls
│   │   ├── delta size ············· rows actually changed
│   │   ├── read/write mix ········· read-heavy · balanced · write-heavy
│   │   ├── contention shape ······· same-key · distinct-key
│   │   ├── arrival ················ unscheduled single-shot · scheduled steady · scheduled bursty
│   │   └── client count k ········· 1 · 4 · 16
│   ├── 4 Environment ─ where it runs
│   │   ├── backend ················ local FS (filesystem + storage class)
│   │   │                              · MinIO (digest-pinned) · real S3 (region + class)
│   │   ├── network position ······· same-host · same-region · remote
│   │   ├── execution surface ······ embedded engine · server
│   │   └── cache warmth ··········· cold · warm · post-invalidation
│   ├── 5 Protocol ─ how it is measured
│   │   ├── deadline ··············· none · 30 s · 60 s · 180 s
│   │   ├── attribution ············ per-phase on · off
│   │   ├── schedule ··············· manual · earned per rule 5
│   │   ├── repetition reset ······· plain copy · APFS clonefile · S3 version undo
│   │   ├── timer ················· monotonic
│   │   └── record contents ········ the run-record contract below
│   └── acquisition quantity ······· requested repetitions
│       (recorded; not a factor, run-spec field, point, or cell identity;
│       observed count + dispersion per rule 3)
├── MEASUREMENTS = what a run outputs (the other half of the record)
│   ├── latency ········ p50 · p95 · p99 per workload operation kind, under rule 3
│   ├── throughput ····· operations/s at declared on-time validity
│   ├── answer quality · judge-free floor: verifiable-answer correctness
│   │                    · multi-hop completion
│   │                    (recall/precision@k: future work)
│   ├── cost ··········· $/query decomposed: requests · egress · compute · tokens
│   ├── storage calls ·· counts per RFC 0031 layer-specific action class
│   │                    (logical always · physical where exposed)
│   ├── request timing · per-layer, per-action-class cumulative time
│   │                    · matching calibration per layer-specific action class
│   │                    · concurrency witness (physical layer, where captured:
│   │                      max requests in flight per measured span)
│   └── noise residual · measured floor (A/A) · disturbance flags · per-rep spread
├── the RUN RECORD = the run spec + the measurements
│   ├── persists ··· the run spec (fixture · workload · conditions)
│   │                · the SUT (source commit · build profile · engine configuration)
│   │                · the invocation id · the invocation timestamp · the session id
│   │                · backend identity · machine specification (auto-captured,
│   │                  record-level identity, not a factor)
│   │                · dataset-builder identity (version · parameters · fetch digests)
│   │                · the point-identity canonicalization version · full point_id
│   │                · the readable point_name (display only)
│   │                · requested and observed repetition counts
│   │                · per-layer presence statements (counts · calibration
│   │                  · timing · witness)
│   │                · directional labels (rule 3) · claim margins (rule 7)
│   │                · the stamped fixture-manifest reference
│   │                · raw result rows (one per repetition)
│   ├── cited by every published number
│   ├── append-only until first cited, immutable after
│   └── sufficient for a stranger to rebuild the run
├── BACKENDS, one job each
│   ├── MinIO ······ repeatable rig: real S3 semantics at local latency;
│   │                sweeps and comparisons run in numbers; never simulates faults
│   ├── real S3 ···· truth: scheduled, budget-capped subset; distributions
│   │                and trend, never single-run headlines
│   └── cross-check  Σ class(count[layer,class] × calibration[layer,class])
│                    ⇒ predicted cumulative time for that same layer, reconciled
│                    against observed cumulative time; any nonzero class without
│                    calibration makes reconciliation unavailable, never partial;
│                    elapsed joins only in serial windows, read from the
│                    concurrency witness (no witness ⇒ no elapsed reconciliation);
│                    logical-only records make no physical fan-out/request claim
├── ADDING A CASE = definition, not wiring
│   ├── new point ······ schema-versioned declarative YAML case definition;
│   │                    its parsed, default-materialized identity projection
│   │                    is its registration; acquisition quantities and
│   │                    display prose do not enter point identity
│   ├── new scenario ··· implements the harness interface; verification
│   │                    has no default; unprovable work cannot be added
│   └── inheritance ···· every rule below applies to every case structurally
└── SEVEN PROTOCOL RULES · violation = invalid number (1-4, 7)
                           · nonconforming practice (5-6)
    ├── 1 open-loop driving ······ scheduled dispatch; on-time validity declared;
    │                              an off-schedule run is invalid, not slow
    ├── 2 release-build guard ···· wall-clock only from release builds, guard-
    │                              enforced; counts are build-profile-independent
    ├── 3 reps + dispersion ······ bare means banned; one declared warmth regime
    │                              per cell; small samples labeled directional
    ├── 4 identity on numbers ···· backend + machine spec named; different
    │                              identities never compare silently
    ├── 5 manual before automatic  a schedule is earned by understood variance,
    │                              never default
    ├── 6 this instrument ········ never gates, at any CI stage; gating stays
    │                              with the counting instruments per RFC 0031
    └── 7 effects clear the floor · a claimed effect exceeds the session's
                                   A/A noise floor by a declared, persisted
                                   margin, or reads "no detected effect"
```

## Motivation

**Counting is specified and arriving; time, cost, and quality have nothing yet.** The repository's measurement foundation is storage-call counting:
[RFC 0031 §11](0031-comparative-cost-harness.md#11-amendment-2026-08-16-the-counting-side-as-built)
records the DST counting golden, the calibrated real-backend ceilings first
explored in PR #503, their implementation evidence, and their division of
roles. Those instruments answer "how many calls"; this RFC builds the next
layer by applying the same discipline to the questions users ask next: "how
fast, at what throughput, at what cost, with what answer quality, on a
realistic workload against a named backend". Stating the rules first, in the
counting instruments' spirit of determinism and checkability, is what lets
those numbers be published with the same confidence the counts already enjoy.

**Unstated rules, not weak systems, are what discredit benchmark numbers.** Benchmark history shows what happens without stated rules. Database benchmarking (LDBC SNB, SIGMOD 2015 and VLDB 2022; "Fair Benchmarking Considered Difficult", DBTest 2018) catalogues the same mistakes recurring for decades: drivers that hide the worst latencies, setup work left untimed, backends left unnamed, debug builds, bare averages. Agent-memory benchmarking (the public LoCoMo scoring disputes) shows where that ends: the harness decides the ranking, not the system, and every number starts a public fight. Both fields agree on the fix: write the rules down before producing numbers, so any reader can check a number against them. This RFC is that write-down, for the one instrument whose numbers leave the repository.

## Guide-level explanation

> Reading convention: a term set in ***bold italics*** is being defined at that exact spot, once. Afterwards it appears in plain text.

### Definitions

In dependency order; each entry depends only on plain English, terms the Summary defines, or entries above it.

- **End-to-end benchmark**: the instrument this RFC specifies. It measures both elapsed time and storage calls per run, and is distinguished from the repository's call-counting instruments by measuring time at all. End-to-end names the path: every run drives omnigraph's public surface, so every number includes the full request path a user pays.
- **Factor** (in the design-of-experiments sense): one parameter under the benchmark's control. A run fixes every factor at one ***level*** (a factor's chosen value): row count is a factor, 100k is a level of it.
- **Acquisition quantity**: a control over how much evidence one invocation collects, not what experiment it performs. The requested repetition count is the initial and only acquisition quantity. Requested and observed counts are persisted, but changing only the requested count keeps the same run spec, point, and cell: a 5-repetition and a 20-repetition invocation add evidence to the same series.
- **The five factor classes**: every factor belongs to exactly one of five classes, named by the question it answers: **Data** (what is logically stored), **State** (how the store got that way), **Workload** (what the run does to it), **Environment** (where it runs), **Protocol** (how it is measured). A run description missing any class is not a run description.
- **Dataset builder**: the versioned program that obtains the logical data a run measures against by generating it, fetching digest-pinned inputs, or transforming pinned inputs. Generated data is reproducible as logical content from the same builder version and parameters; generated identifiers, timestamps, and substrate encodings may make rebuilt fixture bytes differ. Fetched inputs remain pinned by content digest in the ***artifact archive*** (the repository-side, append-only store of fetched inputs), because origins vanish and a digest that resolves to nothing is a citation to nothing; transforms record their pinned inputs. The builder identity (version, parameters, and fetched-input digests) is persisted with every run and determines the logical input, not the physical bytes of a built store.
- **Fixture**: the built store realizing the Data and State classes. Its ***logical fixture identity*** is the dataset-builder identity plus its Data and State levels; this identity enters the run spec and `point_id`. Validation proves the logical content with a logical digest over user-visible content, schema, and structured index inventory, and validates the declared State realization separately. Its ***physical fixture identity*** is the stamped digest and inventory of the exact built store. Physical identity is persisted for audit and reset verification but does not enter `point_id`, so logically equivalent rebuilds remain one measurement series even when their bytes differ.
- **Conditions**: the Environment and Protocol classes taken together: where the run executes and how it is measured.
- **Backend**: the storage world the engine runs against during measurement; always a storage layer (an object-store implementation, local or real), never a harness layer.
- **Backend identity**: the naming a number's storage backend must carry: backend kind plus the filesystem and storage class for a local filesystem, image digest for a local object store, or region and storage class for real S3. Generic words ("local", "disk") are not a backend identity.
- **Run spec**: a run's complete experiment identity: its logical fixture, workload, and conditions (one level for every factor), excluding acquisition quantities and display prose. Before identity is computed, the schema-versioned YAML case is parsed into typed fields and every default is materialized. The authoritative ***point id*** (`point_id`) is the full SHA-256 digest of the versioned canonical serialization of that typed run spec; the canonicalization version and full run spec are persisted, and readers fail closed if equal digests accompany unequal specs. The ***point name*** (`point_name`) is a readable display string only: it may include a short digest, but it is never a key and may collide. Runs sharing a `point_id` are one measurement series; changing only requested repetitions does not create a new point. Design of experiments calls the spec a treatment or design point.
- **Scenario**: a named family of run specs sharing one workload shape; a scenario crossed with levels yields specs. (RFC 0037 separately types Scenario inside the DST harness; that narrower sense is unchanged there.)
- **Profile**: a canonical region of the instrument's space of run specs, given a name so it can be invoked without reciting factor levels, and decidable from a spec's levels alone. Two profiles are defined, in exact level names. The **micro profile** is the region where arrival is "unscheduled single-shot", Data provenance is "synthetic", and Protocol attribution is "per-phase on": it attributes cost to mechanisms. The **realistic profile** is the region where arrival is "scheduled steady" or "scheduled bursty" and attribution is "off": it measures what users feel. Both drive the public surface. The engine's merge phase-timing seam has shipped; the first harness runner may consume it through the embedded engine surface while equivalent server exposure remains later API work, without creating a different instrument. A new profile is a definition, not a new instrument.
- **Run**: one execution of the end-to-end benchmark, under at most one profile. A spec falling in neither profile's region is a valid run, named by its full spec; profiles are named regions, not a partition.
- **System under test (SUT)**: what the run measures: the engine's source commit, build profile, and engine configuration (feature flags and enabled techniques). Deliberately outside the run spec: the spec describes the experiment, the SUT is the subject. Equal specs with different SUTs compare systems (a fix verdict, a regression); equal SUTs across a sweep compare scaling.
- **Session**: one harness invocation batch on one machine, bounded by a ***session id*** the harness mints at batch start and persists in every record the batch produces.
- **Run record**: the persisted description of one run: its run spec plus the measurements produced. One record holds one invocation; its repetitions are rows inside the record, never separate records, and a record is append-only until first cited and immutable after (repetition rows append; nothing ever rewrites), so no number can be trimmed between measurement and citation and no cited percentile can shift under its citation. The citation target for every published number; its contract hardens at first citation. This is the reproducibility contract: a stranger holding the record can rebuild the run. The run spec remains the results table's ***natural key*** (a key formed from the data's own attributes rather than assigned), and `point_id` is its canonical persisted identifier: records with equal specs are directly comparable and form one series over time. Requested repetitions is invocation acquisition metadata and observed repetitions are the record's raw rows; neither changes point identity. One record is identified uniquely by an ***invocation id*** the harness mints when the invocation starts: globally unique by format (ULID-class), so identity never rests on clock resolution and the id alone may key derived surfaces; the id and the invocation timestamp (ordering only) are both persisted fields.
- **Cell**: one (run spec, SUT) pair's samples: every number in a cell shares every factor level and the subject ("series" is reserved for the spec grain: a point's records over time). Dispersion, warmth, and sample-count obligations attach at cell grain.
- **Sweep**: a series of runs varying one factor's level, every other factor pinned. The only shape from which a scaling claim ("cost grows with X") may be read.
- **Showroom fixture**: a store built fresh and clean immediately before measurement. It understates the costs a production store carries, the way a showroom car understates ownership; the State class exists to forbid measuring only showrooms.
- **Fixture-state axes (F1 to F5)**: the five history effects a real store accumulates and a showroom fixture lies about: **F1** fragmentation and aging (the same content bulk-loaded versus written as thousands of small commits), **F2** index existence and type, **F3** index freshness (rows written since the last index optimize), **F4** deletion history (deletion-vector accumulation at equal live rows), **F5** compaction recency.
- **Warmth**: which caches are populated when measurement starts. Three declared regimes: **cold** (fresh process per repetition), **warm** (discarded warm-up repetitions, then measurement), **post-invalidation** (warm, then caches invalidated, then measurement).
- **Open-loop driver**: a workload driver that dispatches each operation at its scheduled time regardless of whether earlier responses have returned. Its opposite, a closed-loop driver, sends the next request only after the previous response, and therefore pauses exactly when the system stalls, silently deleting the worst latencies from the percentiles (the effect known as coordinated omission).
- **On-time validity rule**: a workload's declared bound on schedule slip under open-loop driving (the LDBC SNB v2 form: 95% of operations start within one second of schedule). A run that cannot sustain its schedule is invalid, not slow.
- **Noise floor**: the per-metric delta distribution between two same-spec, same-SUT invocations inside one session (an A/A pair). A floor is computed per metric and gates claims about that metric only (a floor measured on medians says nothing about a tail percentile); it licenses comparisons at its own spec, the cell it was measured in, and applying it to any other spec is an extrapolation that must be stated as such. ***Disturbance flags*** (recorded indicators that the machine was busy or thermally constrained during a run) travel with the floor. An effect that does not clearly exceed the applicable floor is not a detected effect.

### The factor classes, concretely

The axes and sweep points live in the contract tree above (Summary); this section carries only what the tree cannot: the commitments.

**Fresh fixtures flatter; published numbers report the aged store too.** Separating Data from State is the anti-showroom commitment: fixtures must be buildable at both ends of the fixture-state axes, and a published number reports both ends where they differ materially, since a real store lives between them.

**The tree's field list is normative.** The run record's persisted fields are enumerated in the tree's run-record branch; the field list is normative, not illustrative: a record missing any field is invalid, where conditional fields (marked "where captured") satisfy the requirement by stating their absence.

**Machine specification is record-level identity, not a factor.** It is auto-captured at run time, so a checked-in case definition could never assign it; keeping it outside the spec lets a definition assign every factor, lets one point's series span machines visibly, and loses nothing: rule 4 already forbids silent cross-machine comparison.

**A fixture is validated once, before anything is ever measured against it.** Every fixture build ends with a validation pass: row counts per table match the spec, declared indexes are present and covering, fetched artifacts match their pinned digests, the declared State realization is checked, and both a logical-content digest and a physical store digest and inventory are computed. Validation ends by writing the ***fixture manifest*** (the logical fixture identity, both digests, the physical inventory, and a validation stamp); a fixture is ***frozen*** exactly when a stamped manifest exists, and run records reference fixtures by their stamped manifests, so a crash between validation and the stamp leaves an unusable build, harmlessly. A fixture that fails validation never freezes. This is deliberately separate from per-run verification (the case interface's verify obligation): fixture validation asks "is the world right?" once; run verification asks "did the run do real work?" every time. A wrong world validates no work, however real.

### Backends

- **Local filesystem**: the developer smoke and calibration backend. Its identity includes the exact filesystem and storage class. Local-only conclusions are provisional under rule 4 and never substitute for the MinIO or real-S3 evidence required for a backend claim.
- **MinIO**: the repeatable rig. Real S3 request semantics at local latency, cheap enough that comparisons and sweeps run in numbers. Never simulates faults; fault injection belongs to the DST harness (RFC 0032, RFC 0037).
- **Real S3**: the truth. Scheduled runs on a budget-capped scenario subset produce latency distributions and a regression trend over time, never single-run headline numbers, because a single real-network observation is weather, not climate.

**Wall-clock is one measurement dimension; storage-call counts are the other,
and every run records both.** The harness adopts both of RFC 0031's action
vocabularies unchanged. Logical operations use `get`, `put`, `put_part`,
`head`, `list`, `delete`, `copy`, `rename`, and logical multipart
complete/abort. Physical attempts use HTTP `GET`, `HEAD`, `LIST`, `PUT`,
`POST`, and `DELETE`, refined into multipart initiation, part upload,
completion, abort, and copy where RFC 0031 does so. Retries and multipart
fan-out make those layers and vocabularies differ; there is no implicit
logical-to-physical class mapping. The logical layer is mandatory in every
record; the physical layer is recorded where the backend seam exposes it, and
a record states per layer whether it is present or absent, never silently
conflating the two. Counts land in the same record beside the timings, as
measurement columns, not gates: RFC 0031's comparator remains the only pinned,
gating count.

The cross-check compares like with like, with every operand recorded. A
***latency calibration*** is a map keyed by `(measurement layer,
layer-specific action class)`: logical keys come only from RFC 0031's logical
vocabulary, and physical keys come only from its physical request vocabulary.
It is measured by the harness under the record's backend identity and
conditions and persisted in the record. No calibration is translated or
shared across layers. For one layer `L`, predicted ***cumulative request time*** is
`Σ class(count[L,class] × calibration[L,class])`. The observed side is the
matching per-operation-class cumulative time at layer `L`; summing those cells
produces the observed layer total. A nonzero counted class without a matching
calibration or observed timing makes reconciliation for that entire layer
unavailable—implementations never drop the class or substitute another
class's calibration. Physical-layer reconciliation includes retry and
multipart fan-out directly in its attempt counts. Logical-only reconciliation
compares logical counts, calibrations, and timings and makes no claim about the
number or duration of physical attempts. It therefore defines neither a
fan-out allowance nor a hidden-physical-request finding. A material residual
may be reported only for a layer whose complete inputs are present, and the
finding names that layer.

Elapsed wall-clock joins the reconciliation only inside a ***serial window***:
a ***measured span*** (one repetition's measured window) whose ***concurrency
witness*** reads one. The witness records the span's maximum number of physical
storage requests simultaneously in flight; logical operations have no such
witness because one logical operation can fan out into concurrent physical
requests. In a serial window cumulative request time is at most elapsed time,
and the gap is attributable non-storage time such as engine compute between
requests. Where the witness exceeds one, cumulative time may exceed elapsed
time by up to the concurrency achieved, so only cumulative-versus-cumulative
reconciles. A single operation is not automatically serial. A record without
the physical layer carries no witness, so elapsed reconciliation is
unavailable rather than assumed. This is the same-ruler pattern recorded in
[RFC 0031 §11](0031-comparative-cost-harness.md#11-amendment-2026-08-16-the-counting-side-as-built).

**Cost is a decomposed, priced column.** The realistic profile's cost column decomposes into storage requests, egress, compute, and, for workloads whose answers involve a language model, token spend; each component is priced against a dated price table recorded with the run, so a cost number carries its own exchange rate the way RFC 0031's pricing does.

### Answer quality: the initial step

**The floor is script-checkable.** The realistic profile's first quality metrics are ***judge-free*** (computable by a script against labeled data, no language model scoring in the loop): answer correctness by exact or containment match on questions with verifiable answers, and multi-hop completion rate (whether a question needing a chain of connected facts got the full chain assembled; the question-list artifact carries, per multi-hop question, its expected chain members, so a script checks assembly against labels rather than judging it). Question lists are versioned repository artifacts, part of the Data class's identity like any dataset. Language-model judges are deliberately excluded from this initial step: the agent-memory tradition's published disputes show judge choice alone can flip rankings, so judge-based scoring waits for the dedicated quality document and its control battery (see Non-goals). Retrieval-level metrics (recall and precision at k against curated evidence sets) are future work: they need per-question evidence-set curation, a real investment that should not gate the first result row (see Future work). This section defines the floor the first published result row must meet; it does not cap what later adds.

### The initial workload: branch merge

**The instrument's first shipped content is branch-merge measurement, under the micro profile.** Branch merge is the branch control that agent workflows depend on, and the operation with a live latency problem, so it is the first workload the instrument must measure well. The initial state comprises four things. **Fixtures:** at minimum one small unindexed shape and one large indexed shape, frozen and reproducible through the dataset builder. **Scenarios:** the delta sweep (does merge cost track the change or the table), the table-count sweeps at pinned and full divergence (the per-table taxes), and an all-diverged composite. **The noise floor:** an A/A pair per session, so every reported effect has its floor. **Baselines:** records on unmodified main, taken before any merge optimization lands, so every future improvement has its before-picture in the same table it will be judged in. Acceptance for the initial state: from its records alone, a reader must be able to determine whether merge wall-clock and merge storage-call counts scale with table size or with delta size; records whose effects sit below the session floor satisfy acceptance as "no detected effect at these scales", which is itself a determination. The named run set and its parameters live in the harness documentation (see Non-goals).

### Adding a case: definition, not wiring

**Protocol compliance is structural.** A new benchmark case enters by definition alone, at one of two tiers: a new point of an existing scenario is a schema-versioned declarative YAML ***case definition***. YAML is presentation: comments, key order, formatting, display names, and acquisition quantities do not enter identity. The harness parses it into the typed case schema, materializes defaults, and derives `point_id` from the identity-bearing projection, so the definition is its own registration. A new scenario kind implements a fixed harness interface whose obligations mirror the protocol (declare the factors it consumes, prepare against a fixture, run the measured operation, and verify non-vacuous work: an interface with no default verification, so a scenario that cannot prove it did real work cannot be added). Everything this RFC requires is inherited by every case from the harness, never reimplemented per case: point identity and display naming; the record contract and its identity keys, series keyed by spec and each record by invocation id; the release-build guard; warmth control; dispersion reporting. A case author cannot produce a rule-violating case without modifying the harness itself.

## Reference-level design: protocol rules

Each rule states what it forbids; violating a measurement rule (1 to 4, 7) makes a number invalid, not merely unpolished, and violating a process rule (5, 6) makes the practice nonconforming, with the number-level consequence stated in the rule.

1. **Open-loop driving with on-time validity.** Applies when the workload is scheduled (the realistic profile); a single-operation workload has no schedule to violate. Every scheduled-workload latency or throughput claim comes from an open-loop driver, and each workload declares its on-time validity rule. Numbers from an invalid run are unpublishable.
2. **Release-build guard (wall-clock only).** A wall-clock number is recorded only from a release-profile build, and the harness makes recording from any other build profile impossible without deliberately bypassing a guard. Wall-clock numbers from different build profiles never compare. Storage-call counts are the exception by nature: counting is build-profile-independent (the same operations issue the same calls at any optimization level), so counts may be compared across build profiles; this is RFC 0031's timing-versus-counting separation applied per dimension.
3. **Repetitions, dispersion, controlled warmth.** Every wall-clock cell reports its repetition count and a dispersion measure (percentiles, or median with minimum and maximum); bare means are banned, per DBTest 2018's guidance that means reported without dispersion mislead. A cell declares its warmth level, and every repetition must actually execute under it: a cold first repetition folded into a warm cell invalidates the cell. A tail percentile requires a sample count that supports it: p95 at least 20 samples (RFC 0031's rule), p99 at least 100; smaller-sample cells are directional evidence only, labeled so in the record. The requested repetition count is sample quantity, not cell identity: changing only that target keeps the same point and cell, while every record persists both requested and observed counts.
4. **Identity on every number.** Every published number carries its backend identity and its machine specification; numbers from different backends or machine specifications never compare silently. Conclusions drawn only from a local backend are provisional and labeled so.
5. **Manual before automatic.** The benchmark runs manually until its run-to-run variance is understood; only then may it earn a schedule, because automating un-understood variance automates the production of noise. Numbers produced by a schedule that was not earned this way are unpublishable. All scheduling (nightly MinIO runs, the real-S3 trend series, alerting) is a separate later change gated on that understanding.
6. **This instrument never gates.** No measurement from this instrument gates anything, at any CI stage. Wall-clock never gates because timing variance on shared runners converts a gate into a lottery; this instrument's per-run counts never gate because they are unpinned measurement columns, and turning an unpinned measurement into a gate would recreate the counting golden without its review discipline. Count-based gating belongs to the counting instruments, at the stages RFC 0031 §§6 and 11 assign them (RFC 0031 states its harness is a release gate and an on-demand tool, not a per-PR performance gate, aside from one deliberately bounded structural guard it mandates; this RFC neither adds a gate nor moves one). The number-level consequence: a number produced by this instrument stays a valid measurement even when misused as a gate; the gate is what must be removed.
7. **Effects clear the floor.** Every comparison-bearing claim (a fix's effect, a regression, a difference between systems) must exceed the applicable per-metric noise floor by a declared margin, and the margin is itself persisted: a protocol-level default, or a per-claim declaration recorded beside the citation, never an after-the-fact choice. An effect below floor-plus-margin is reported as "no detected effect", never as a small effect. The floor is itself a recorded measurement, so every effect claim carries its own denominator.

## Relation to existing instruments

**RFC 0031 owns the counting instruments; this RFC adds only above them.** The repository has two other measurement tools: the logical-cost comparator and the real-backend qualifier (both call-counting, both owned by RFC 0031 §§6 and 11, which this RFC adopts by name and re-specifies nothing of). Three tools produce three kinds of number, and this instrument's two profiles differ again, so measurement-bearing prose should name which tool (and, for this one, which profile) a number came from; "the benchmark" unqualified hides exactly the fact a reader needs. One term boundary is restated for visibility: RFC 0037 separately uses "instrument" for one built capability inside the DST harness; that narrower, harness-internal sense is unchanged there, and this RFC's use is always the whole measurement tool. If review finds any sentence here in tension with RFC 0031, RFC 0031 wins and this RFC is the document to fix.

## Non-goals

- **Per-scenario detail of the micro profile.** Its scenario families and standard run sets live in the harness documentation; this RFC governs how any run is recorded and published, not which runs exist.
- **Dataset choice.** LDBC-generated graph, agent-memory-shaped corpus, or both is a later decision; this RFC only requires that whatever is chosen comes through a dataset builder (generated or digest-pinned) and that its identity lands in the Data class.
- **Quality metrics beyond the initial step.** The judge-free floor is specified above; everything past it (language-model judges and their controls, baseline batteries, task-type splits) is a discipline with its own credibility rules and gets its own document.
- **Replacing existing tools.** `helpers::cost`, `benches/scenarios.rs`, and RFC 0031's harness remain the right tools for their jobs.
- **Automation machinery** (gated behind protocol rule 5).

## Invariants & deny-list check

Docs-only methodology RFC: no product behavior, storage format, wire protocol, or API change. No Hard Invariant is touched and no deny-list item is brushed.

## Drawbacks & alternatives

**Discipline overhead is the price; the alternatives cost more.** This RFC has two costs. First, bookkeeping: every run must carry its full spec, where naming a file by hand is easier. Second, and sharper: the validity rules will sometimes reject a run someone liked: a number that broke a rule may not be published, even when it looks good.

Three alternatives were considered and rejected. **No rules:** Motivation documents where that ends. **One RFC for every measurement tool:** the counting tools already have their rules under RFC 0031, and one tool should not have two rulebooks. **Micro and realistic as two separate instruments:** an earlier draft did exactly this; the five-class model then showed the two differ only in factor levels, so the single instrument with two profiles replaced it.

## Reversibility

**Cheap now, hardening at first citation.** Cheap to revise while claims are internal: amending documentation is one PR. It hardens exactly when published claims start citing run records, since a cited record cannot change retroactively without orphaning the claims built on it; rule changes after first publication therefore version the run-record contract rather than editing it in place.

## Future work

- **Retrieval-level quality measurement.** Recall and precision at k against curated evidence sets (the labeled ground truth naming, per question, exactly which stored items a correct retrieval returns). The curation of evidence sets is the gating investment; once they exist as versioned Data-class artifacts, these metrics join the result row beside the correctness floor.
- **A queryable result surface in omnigraph itself.** Raw run records in an append-only archive are the source of truth; over them, a `bench` graph in this very database is the leading idea for the team-facing surface: one node per run record, keyed by the invocation id (mirroring the record identity contract, so reruns and different-SUT comparisons never merge), beside one node per benchmark point, keyed by the full `point_id`, collecting its series; progress is a stored query over the point's records, multi-user access comes through the existing policy layer, and citation immutability holds by construction via (node, graph commit id) with time travel. The runner writing through the public surface is itself a live end-to-end workload. The archive stays authoritative so results about the system never depend on the system version under test; the graph is an index, rebuildable from the records.
- **Automated running and result management** (schedules, trend storage, alerting), gated behind protocol rule 5 and owned separately from this specification.

## Unresolved questions

- Harness code location (workspace crate versus extension of an existing bench surface): left to the first landing PR.
- The dataset decision (see Non-goals).
- The real-S3 schedule's budget cap and scenario subset (decidable only after manual runs establish variance, rule 5).
- Whether the quality-metrics document amends this RFC or stands alone.
