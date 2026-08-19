# RFC 0039: The end-to-end benchmark

| | |
|---|---|
| **Status** | Draft |
| **Date** | 2026-08-19 |
| **Author track** | Public contribution |
| **Author(s)** | Azim Afroozeh |
| **Number** | 0039 (next free: `docs/rfcs/` runs through 0038 at PR time; the number is reserved by the merge, re-check for in-flight collisions before opening the PR) |
| **Discussion** | <link when opened> |
| **Implementation** | harness prototype in progress (uncommitted; recorded in the harness documentation) |

## Evidence status of this document

**This document is design intent, not evidence.** No runs on the instrument's target path (the public surface) exist at this writing; interim-harness runs exist and are recorded in the harness documentation, not cited here. This document therefore contains zero measured claims: every number below is a design parameter or a citation of an external benchmark tradition. When target-path runs exist, published numbers enter by citation of run records (defined below), never retyped.

## Summary

Omnigraph gains its ***end-to-end benchmark***: one instrument that drives the public surface and measures both elapsed time and storage calls. Every run applies a workload to a fixture under stated conditions, measuring one system under test; that description (equivalently, one level chosen for every factor in five classes) is persisted as a run record which every published number must cite, and seven protocol rules make dishonest or irreproducible numbers invalid rather than merely unpolished. The instrument runs under two named ***profiles*** (a profile is a canonical region of the space of run specs, named so it can be invoked without reciting levels): the ***micro profile*** (single-operation synthetic workload, per-phase attribution: it attributes time to mechanisms) and the ***realistic profile*** (scheduled realistic workload against a named backend, reporting latency percentiles, throughput, answer quality, and cost: it measures what users feel). End-to-end means over the ***public surface*** (the interfaces omnigraph ships for its users, as opposed to internal seams): both profiles drive it; the micro profile simulates micro-benchmarking through the same door users enter, so its numbers carry the same request path the realistic profile's do. When a realistic number moves, the micro profile says which mechanism moved it. The two profiles are not two instruments: they share the harness, the record schema, the run-spec key, and every rule below; they differ only in factor levels.

The whole contract, as one tree (axes and sweep points illustrative; the harness documentation owns the full lists):

```
end-to-end benchmark: one instrument, two profiles
│                        (micro: mechanism attribution · realistic: what users feel)
├── a RUN = a workload applied to a fixture under conditions,
│            measuring one system under test
│            (equivalently: one level per factor, in five classes)
│   ├── 1 Data ─ what is logically stored
│   │   ├── row count N ············ 1k · 100k · 750k
│   │   ├── table count T ·········· 8 · 50 · 140
│   │   ├── payload bytes per row ·· ~100 B · 4 KiB · 32 KiB
│   │   ├── column shape ··········· scalars · +vector · +blob
│   │   ├── topology skew ·········· uniform · power-law
│   │   └── scale factor SF ········ one knob multiplying N, delta, branches together
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
│   │   ├── arrival pattern ········ steady · bursty
│   │   └── client count k ········· 1 · 4 · 16
│   ├── 4 Environment ─ where it runs
│   │   ├── backend ················ MinIO (digest-pinned) · real S3 (region + class)
│   │   ├── network position ······· same-region · remote
│   │   ├── machine specification ·· processor, cores, memory, storage class (auto-captured)
│   │   └── cache warmth ··········· cold · warm · post-invalidation
│   └── 5 Protocol ─ how it is measured
│       ├── deadline profile ······· none · 30 s · 60 s · 180 s
│       ├── repetitions ············ count + dispersion, per rule 3
│       ├── warmth regime ·········· declared per cell, per rule 3
│       ├── schedule ··············· manual · earned per rule 5
│       └── record contents ········ the run-record contract below
├── MEASUREMENTS = what a run outputs (the other half of the record)
│   ├── latency ········ p50 · p95 · p99 per operation class, under rule 3
│   ├── throughput ····· operations/s at declared on-time validity
│   ├── answer quality · judge-free floor: verifiable-answer correctness
│   │                    · multi-hop completion (recall/precision@k: future work)
│   ├── cost ··········· $/query decomposed: requests · egress · compute · tokens
│   ├── storage calls ·· counts per class (get · put · list), DST-style, per run
│   └── noise residual · measured floor (A/A) · disturbance flags · per-rep spread
├── the RUN RECORD = the run spec + the measurements
│   ├── persists ··· the run spec (fixture · workload · conditions)
│   │                · the SUT (source commit · build profile · engine configuration)
│   │                · backend identity · machine specification
│   │                · dataset-builder identity (version · parameters · fetch digests)
│   │                · raw result rows
│   ├── cited by every published number
│   ├── immutable once cited
│   └── sufficient for a stranger to rebuild the run
├── BACKENDS, one job each
│   ├── MinIO ······ repeatable rig: real S3 semantics at local latency;
│   │                sweeps and comparisons run in numbers; never simulates faults
│   ├── real S3 ···· truth: scheduled, budget-capped subset; distributions
│   │                and trend, never single-run headlines
│   └── cross-check  counts × per-request latency ⇒ predicted wall-clock,
│                    reconciled against observed; deviation = a finding
├── ADDING A CASE = definition, not wiring
│   ├── new point ······ declarative case definition (levels per factor);
│   │                    the definition is its own registration
│   ├── new scenario ··· implements the harness interface; verification
│   │                    has no default; unprovable work cannot be added
│   └── inheritance ···· every rule below applies to every case structurally
└── SEVEN PROTOCOL RULES, violation = invalid number
    ├── 1 open-loop driving ······ scheduled dispatch; on-time validity declared;
    │                              an off-schedule run is invalid, not slow
    ├── 2 release-profile guard ·· wall-clock only from release builds, guard-
    │                              enforced; counts are profile-independent
    ├── 3 reps + dispersion ······ bare means banned; one declared warmth regime
    │                              per cell; small samples labeled directional
    ├── 4 identity on numbers ···· backend + machine spec named; different
    │                              identities never compare silently
    ├── 5 manual before automatic  a schedule is earned by understood variance,
    │                              never default
    ├── 6 PR gates stay RFC-031's  only its pinned golden gates; this
    │                              instrument's counts and wall-clock trend
    └── 7 effects clear the floor · a claimed effect exceeds the session's
                                   A/A noise floor, or reads "no detected effect"
```

## Motivation

**Counting is specified and arriving; time, cost, and quality have nothing yet.** The repository's measurement foundation is storage-call counting: the DST counting golden and the calibrated real-backend ceilings (originated in PR #503 and absorbed into the in-flight contributions), both specified under RFC-031 and its in-flight amendment, give exact, deterministic, CI-gateable storage-call counts per operation once landed. This RFC builds the next layer on that foundation: the counting instruments answer "how many calls", and the end-to-end benchmark extends the same discipline to the questions users ask next: "how fast, at what throughput, at what cost, with what answer quality, on a realistic workload against a named backend". Stating the rules first, in the counting instruments' spirit of determinism and checkability, is what lets those numbers be published with the same confidence the counts already enjoy.

**Unstated rules, not weak systems, are what discredit benchmark numbers.** Benchmark history shows what happens without stated rules. Database benchmarking (LDBC SNB, SIGMOD 2015 and VLDB 2022; "Fair Benchmarking Considered Difficult", DBTest 2018) catalogues the same mistakes recurring for decades: drivers that hide the worst latencies, setup work left untimed, backends left unnamed, debug builds, bare averages. Agent-memory benchmarking (the public LoCoMo scoring disputes) shows where that ends: the harness decides the ranking, not the system, and every number starts a public fight. Both fields agree on the fix: write the rules down before producing numbers, so any reader can check a number against them. This RFC is that write-down, for the one instrument whose numbers leave the repository.

## Guide-level explanation

> Reading convention: a term set in ***bold italics*** is being defined at that exact spot, once. Afterwards it appears in plain text.

### Definitions

In introduction order; each entry depends only on plain English or entries above it.

- **End-to-end benchmark**: the instrument this RFC specifies. It measures both elapsed time and storage calls per run, and is distinguished from the repository's call-counting instruments by measuring time at all. End-to-end names the path: every run drives omnigraph's public surface, so every number includes the full request path a user pays.
- **Factor** (in the design-of-experiments sense): one parameter under the benchmark's control. A run fixes every factor at one ***level*** (a factor's chosen value): row count is a factor, 100k is a level of it.
- **The five factor classes**: every factor belongs to exactly one of five classes, named by the question it answers: **Data** (what is logically stored), **State** (how the store got that way), **Workload** (what the run does to it), **Environment** (where it runs), **Protocol** (how it is measured). A run description missing any class is not a run description.
- **Dataset builder**: the script that obtains the data a run measures against, by any means: generating it synthetically from a seed, downloading a published dataset, or transforming one into another. Reproducibility is the only requirement, and it is met per mode: generated data is re-derivable on demand (same builder version, same parameters, same bytes: it never needs archiving); fetched data is pinned by content digest and must land in the archive, because origins vanish and a digest that resolves to nothing is a citation to nothing. Either way, the builder's identity (version, parameters, and the digests of anything fetched) fully determines the bytes and lands in the run record.
- **Fixture**: the built store a run measures against, realizing the Data and State classes as actual bytes. A fixture's identity is (dataset-builder identity, its Data and State levels); a fixture built from generated data is re-derivable on demand, and one built from fetched data is reproducible through the archived, digest-pinned artifacts.
- **Conditions**: the Environment and Protocol classes taken together: where the run executes and how it is measured.
- **Run spec**: a run's complete specification: its fixture, its workload, and its conditions (equivalently: one level assigned to every factor). A run's spec is its identity; its serialized form is the ***point name*** (the spec flattened into one string, e.g. `m3-t8-n100k-btree-d50`). Design of experiments calls this a treatment or design point.
- **Profile**: a canonical region of the instrument's space of run specs, given a name so it can be invoked without reciting factor levels. Two profiles are defined: the **micro profile** (single-operation synthetic workload, per-phase attribution on) attributes cost to mechanisms; the **realistic profile** (scheduled realistic workload, named backend) measures what users feel. Both drive the public surface. Per-phase attribution in the micro profile is served through that same surface by the engine's phase-timing exposure; while that exposure is unshipped, the harness's in-process access is an implementation interim, not a different instrument. A new profile is a definition, not a new instrument.
- **Run**: one execution of the end-to-end benchmark, under one profile.
- **System under test (SUT)**: what the run measures: the engine's source commit, build profile, and engine configuration (feature flags and enabled techniques). Deliberately outside the run spec: the spec describes the experiment, the SUT is the subject. Equal specs with different SUTs compare systems (a fix verdict, a regression); equal SUTs across a sweep compare scaling.
- **Run record**: the persisted description of one run: its run spec plus the measurements produced. The citation target for every published number, and immutable once cited. This is the reproducibility contract: a stranger holding the record can rebuild the run. The run spec doubles as the results table's ***natural key*** (a key formed from the data's own attributes rather than assigned): runs with equal specs are directly comparable and form one series over time; spec plus SUT plus timestamp identifies one run uniquely.
- **Sweep**: a series of runs varying one factor's level, every other factor pinned. The only shape from which a scaling claim ("cost grows with X") may be read.
- **Showroom fixture**: a store built fresh and clean immediately before measurement. It understates the costs a production store carries, the way a showroom car understates ownership; the State class exists to forbid measuring only showrooms.
- **Fixture-state axes (F1 to F5)**: the five history effects a real store accumulates and a showroom fixture lies about: **F1** fragmentation and aging (the same content bulk-loaded versus written as thousands of small commits), **F2** index existence and type, **F3** index freshness (rows written since the last index optimize), **F4** deletion history (deletion-vector accumulation at equal live rows), **F5** compaction recency.
- **Backend**: the storage world the engine runs against during measurement; always a storage layer (an object-store implementation, local or real), never a harness layer.
- **Warmth**: which caches are populated when measurement starts. Three declared regimes: **cold** (fresh process per repetition), **warm** (discarded warm-up repetitions, then measurement), **post-invalidation** (warm, then caches invalidated, then measurement).
- **Open-loop driver**: a workload driver that dispatches each operation at its scheduled time regardless of whether earlier responses have returned. Its opposite, a closed-loop driver, sends the next request only after the previous response, and therefore pauses exactly when the system stalls, silently deleting the worst latencies from the percentiles (the effect known as coordinated omission).
- **On-time validity rule**: a workload's declared bound on schedule slip under open-loop driving (the LDBC SNB v2 form: 95% of operations start within one second of schedule). A run that cannot sustain its schedule is invalid, not slow.
- **Backend identity**: the naming a number's storage backend must carry: backend kind plus pinning (image digest for local object stores; region and storage class for real S3). Generic words ("local", "disk") are not a backend identity.
- **Noise floor**: the delta distribution between two runs with equal spec and equal SUT in one session (an A/A pair). The floor is the session's measured noise; an effect that does not clearly exceed it is not a detected effect.

### The factor classes, concretely

The axes and sweep points live in the contract tree above (Summary); this section carries only what the tree cannot: the commitments.

**Fresh fixtures flatter; published numbers report the aged store too.** Separating Data from State is the anti-showroom commitment: fixtures must be buildable at both ends of the fixture-state axes, and a published number reports both ends where they differ materially, since a real store lives between them.

**The tree's field list is normative.** The run record's persisted fields are enumerated in the tree's run-record branch; the field list is normative, not illustrative: a record missing any field is invalid.

**A fixture is validated once, before anything is ever measured against it.** Every fixture build ends with a validation pass: row counts per table match the spec, declared indexes are present and covering, fetched artifacts match their pinned digests, and a content digest of the built store is recorded in the fixture manifest. A fixture that fails validation never freezes, and run records may reference only validated fixtures. This is deliberately separate from per-run verification (the case interface's verify obligation): fixture validation asks "is the world right?" once; run verification asks "did the run do real work?" every time. A wrong world validates no work, however real.

### Backends

- **MinIO**: the repeatable rig. Real S3 request semantics at local latency, cheap enough that comparisons and sweeps run in numbers. Never simulates faults; fault injection belongs to the DST harness (RFC-032, RFC-037).
- **Real S3**: the truth. Scheduled runs on a budget-capped scenario subset produce latency distributions and a regression trend over time, never single-run headline numbers, because a single real-network observation is weather, not climate.

**Wall-clock is one measurement dimension; storage-call counts are the other, and every run records both.** The harness counts the run's storage calls (per class: get, put, list) with the same counting technique the DST golden uses, into the same record beside the timings. Per-run counts are a measurement column, not a gate: RFC-031's comparator remains the only pinned, gating count. What the counts buy is the cross-check per run: counts times per-request latency predict a wall-clock, reconciled against the wall-clock observed; a material deviation (hidden requests, retries, contention) is a finding in its own right, per the same-ruler precedent the RFC-031 amendment records.

### Answer quality: the initial step

**The floor is script-checkable.** The realistic profile's first quality metrics are ***judge-free*** (computable by a script against labeled data, no language model scoring in the loop): answer correctness by exact or containment match on questions with verifiable answers, and multi-hop completion rate (whether a question needing a chain of connected facts got the full chain assembled). Question lists are versioned repository artifacts, part of the Data class's identity like any dataset. Language-model judges are deliberately excluded from this initial step: the agent-memory tradition's published disputes show judge choice alone can flip rankings, so judge-based scoring waits for the dedicated quality document and its control battery (see Non-goals). Retrieval-level metrics (recall and precision at k against curated evidence sets) are future work: they need per-question evidence-set curation, a real investment that should not gate the first result row (see Future work). This section defines the floor the first published result row must meet; it does not cap what later adds.

### The initial workload: branch merge

**The instrument's first shipped content is branch-merge measurement, under the micro profile.** Branch merge is the branch control that agent workflows depend on, and the operation with a live latency problem, so it is the first workload the instrument must measure well. The initial state comprises four things. **Fixtures:** at minimum one small unindexed shape and one large indexed shape, frozen and reproducible through the dataset builder. **Scenarios:** the delta sweep (does merge cost track the change or the table), the table-count sweeps at pinned and full divergence (the per-table taxes), and an all-diverged composite. **The noise floor:** an A/A pair per session, so every reported effect has its floor. **Baselines:** records on unmodified main, taken before any merge optimization lands, so every future improvement has its before-picture in the same table it will be judged in. Acceptance for the initial state: from its records alone, a reader must be able to determine whether merge cost scales with table size or with delta size. The named run set and its parameters live in the harness documentation (see Non-goals).

### Adding a case: definition, not wiring

**Protocol compliance is structural.** A new benchmark case enters by definition alone, at one of two tiers: a new point of an existing scenario is a declarative ***case definition*** (a checked-in artifact assigning one level to every factor; its point name derives from its content, so the definition is its own registration), and a new scenario kind implements a fixed harness interface whose obligations mirror the protocol (declare the factors it consumes, prepare against a fixture, run the measured operation, and verify non-vacuous work: an interface with no default verification, so a scenario that cannot prove it did real work cannot be added). Everything this RFC requires (point naming, the spec-keyed record, the release-profile guard, warmth control, the dispersion reporting) is inherited by every case from the harness, never reimplemented per case; a case author cannot produce a rule-violating case without modifying the harness itself.

## Reference-level design: protocol rules

Each rule states what it forbids; violating it makes a number invalid, not merely unpolished.

1. **Open-loop driving with on-time validity.** Applies when the workload is scheduled (the realistic profile); a single-operation workload has no schedule to violate. Every scheduled-workload latency or throughput claim comes from an open-loop driver, and each workload declares its on-time validity rule. Numbers from an invalid run are unpublishable.
2. **Release-profile guard (wall-clock only).** A wall-clock number is recorded only from a release-profile build, and the harness makes recording from any other profile impossible without deliberately bypassing a guard. Wall-clock numbers from different build profiles never compare. Storage-call counts are the exception by nature: counting is build-profile-independent (the same operations issue the same calls at any optimization level), so counts may be compared across build profiles; this is RFC-031's timing-versus-counting separation applied per dimension.
3. **Repetitions, dispersion, controlled warmth.** Every wall-clock cell reports its repetition count and a dispersion measure (percentiles, or median with minimum and maximum); bare means are banned (the DBTest 2018 finding: a bare mean is valid only under zero variance). Every cell declares one warmth regime; mixing regimes within a cell invalidates it. A tail percentile requires a sample count that supports it (RFC-031's rule, a p95 needs at least 20 samples, governs); small-sample cells are directional evidence only, labeled so.
4. **Identity on every number.** Every published number carries its backend identity and its machine specification; numbers from different backends or machine specifications never compare silently. Conclusions drawn only from a local backend are provisional and labeled so.
5. **Manual before automatic.** The benchmark runs manually until its run-to-run variance is understood; only then may it earn a schedule, because automating un-understood variance automates the production of noise. All scheduling (nightly MinIO runs, the real-S3 trend series, alerting) is a separate later change gated on that understanding.
6. **Pull-request gates stay count-based, and stay RFC-031's.** The only measurement that gates a pull request is the pinned call-count golden under RFC-031, zero-flake by construction. This instrument's per-run counts, though equally deterministic, do not gate either: they are unpinned measurement columns, and turning an unpinned measurement into a gate would recreate the golden without its review discipline. Wall-clock trends; it never gates, because timing variance on shared runners converts a gate into a lottery.
7. **Effects clear the floor.** Every comparison-bearing claim (a fix's effect, a regression, a difference between systems) must exceed the session's noise floor by a declared margin; an effect below the floor is reported as "no detected effect", never as a small effect. The floor is itself a recorded measurement, so every effect claim carries its own denominator.

## Relation to existing instruments

**RFC-031 owns the counting instruments; this RFC adds only above them.** The repository has two other measurement tools: the logical-cost comparator and the real-backend qualifier (both call-counting, both owned by RFC-031 and its in-flight amendment, which this RFC adopts by name and re-specifies nothing of). Three tools produce three kinds of number, and this instrument's two profiles differ again, so measurement-bearing prose should name which tool (and, for this one, which profile) a number came from; "the benchmark" unqualified hides exactly the fact a reader needs. If review finds any sentence here in tension with RFC-031 or its amendment, RFC-031 wins and this RFC is the document to fix.

## Non-goals

- **Per-scenario detail of the micro profile.** Its scenario families and standard run sets live in the harness documentation; this RFC governs how any run is recorded and published, not which runs exist.
- **Dataset choice.** LDBC-generated graph, agent-memory-shaped corpus, or both is a later decision; this RFC only requires that whatever is chosen comes through a dataset builder (generated or digest-pinned) and that its identity lands in the Data class.
- **Quality metrics beyond the initial step.** The judge-free floor is specified above; everything past it (language-model judges and their controls, baseline batteries, task-type splits) is a discipline with its own credibility rules and gets its own document.
- **Replacing existing tools.** `helpers::cost`, `benches/scenarios.rs`, and RFC-031's harness remain the right tools for their jobs.
- **Automation machinery** (gated behind protocol rule 5).

## Invariants & deny-list check

Docs-only methodology RFC: no product behavior, storage format, wire protocol, or API change. No Hard Invariant is touched and no deny-list item is brushed.

## Drawbacks & alternatives

**Discipline overhead is the price; the alternatives cost more.** This RFC has two costs. First, bookkeeping: every run must carry its full spec, where naming a file by hand is easier. Second, and sharper: the validity rules will sometimes reject a run someone liked: a number that broke a rule may not be published, even when it looks good.

Three alternatives were considered and rejected. **No rules:** Motivation documents where that ends. **One RFC for every measurement tool:** the counting tools already have their rules under RFC-031, and one tool should not have two rulebooks. **Micro and realistic as two separate instruments:** an earlier draft did exactly this; the five-class model then showed the two differ only in factor levels, so the single instrument with two profiles replaced it.

## Reversibility

**Cheap now, hardening at first citation.** Cheap to revise while claims are internal: amending documentation is one PR. It hardens exactly when published claims start citing run records, since a cited record cannot change retroactively without orphaning the claims built on it; rule changes after first publication therefore version the run-record contract rather than editing it in place.

## Future work

- **Retrieval-level quality measurement.** Recall and precision at k against curated evidence sets (the labeled ground truth naming, per question, exactly which stored items a correct retrieval returns). The curation of evidence sets is the gating investment; once they exist as versioned Data-class artifacts, these metrics join the result row beside the correctness floor.
- **A queryable result surface in omnigraph itself.** Raw run records in an append-only archive are the source of truth; over them, a `bench` graph in this very database is the leading idea for the team-facing surface: one node per run, the natural key as node identity, progress as a stored query, multi-user access through the existing policy layer, and citation immutability by construction via (node, graph commit id) with time travel. The runner writing through the public surface is itself a live end-to-end workload. The archive stays authoritative so results about the system never depend on the system version under test; the graph is an index, rebuildable from the records.
- **Automated running and result management** (schedules, trend storage, alerting), gated behind protocol rule 5 and owned separately from this specification.

## Unresolved questions

- Harness code location (workspace crate versus extension of an existing bench surface): left to the first landing PR.
- The dataset decision (see Non-goals).
- The real-S3 schedule's budget cap and scenario subset (decidable only after manual runs establish variance, rule 5).
- Whether the quality-metrics document amends this RFC or stands alone.
