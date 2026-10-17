---
rfc: "2026-10-13-request-aware-plan-cost"
title: "Request-aware plan cost"
track: maintainer
status: draft
implementation: not-started
authors:
  - Ragnor Comerford
created: 2026-10-13
updated: 2026-10-14
discussion: https://github.com/ModernRelay/omnigraph/pull/996
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC: Request-aware plan cost

> A term set in ***bold italics*** is being defined at that exact spot;
> it is used in plain text everywhere after.

Code references are at `main` `32caae4b`; Lance references are at the pinned
11.0.0 crates. Measurements name their instrument and conditions where they
appear.

## Summary

The v2 planner prices every cost-driven choice in one unit that covers two
kinds of work: the rows an alternative processes, which is today's whole
model, and the ***request waves*** on its critical path. A request wave is a
set of object-store requests the engine issues together, so that its
requests overlap and the wave costs about one round trip. Each wave is
charged the per-request latency of a ***storage profile***, chosen from the
snapshot's storage kind when the query is planned and recorded in the plan.
The local profile charges nothing for a wave, so on a local filesystem, and
under DST, every plan is the plan the planner builds today. The
object-store profile, for S3-compatible and Azure roots, charges a wave
about 20 ms, so a chain of dependent lookups stops looking cheaper than
reading a table in parallel.

The first two decisions to use the model are the traversal mode of an
`Expand` (per-hop indexed scans or the in-memory CSR), at plan time and in
its run-time re-decisions, and the access path of a traversal destination
(`id_lookup` or `hash_join`). The RFC does not reorder joins or change which
binding roots a pattern, does not make the planner aware of what a
process-wide cache already holds, and adds no statistic beyond the fragment
facts the planner already opens. Every plan returns the same rows as
before: only operator choice changes, and both alternatives of every
affected decision already have identical semantics.

## Motivation

### The rows-only model picks the slow plan on object storage

`omnigraph_planner::cost` prices alternatives in rows: an indexed hop costs
`hops × frontier × fanout × probe_factor`, a CSR `CSR_BUILD_FACTOR (1.5) ×
|E|`, and a destination is read by `hash_join` only when its table holds at
most `HASH_JOIN_RATIO (8)` rows per estimated frontier row. On a local
filesystem a request costs about 0.1 ms and rows dominate. On object
storage a request costs about 20 ms, while the engine overlaps up to
Lance's I/O parallelism of 64 requests. What dominates there is how many
times a plan waits on storage in series. Rows do not measure that.

Measured with GQT `--measure --model s3-like` (17 ms per request plus bytes
at 50 MiB/s, DST, indexes built, `# traversal: auto`), on a Person(200,000,
50 fragments)–WorksAt(200,000)–Company(200) graph. The query is
`$p: Person $p worksAt $c $c: Company { slug: "c-007" } return { count($p) }`
(1,005 matches), with `$c` declared last or first:

| Warm step | Plan the planner chose | Requests | Request waves | Simulated time |
|---|---|---:|---:|---:|
| `$c` declared last | scan Person, CSR, `hash_join` Company | 104 | 3 | 53 ms |
| `$c` declared first | `$c` by key, indexed hop, `id_lookup` Person | 313 | 9 | 161 ms |
| `$c` declared first, CSR pinned | `$c` by key, CSR, `id_lookup` Person | 178 | 6 | 107 ms |

The selective start loses. That is not because it reads more bytes: its
`id_lookup` read 4.0 MB against the scan's 4.8 MB, since every
scattered row costs a whole chunk. It loses because it is serial. The
indexed hop's take ran in 3 waves, and `id_lookup` awaits each slice of at
most 256 ids before the next, so its 1,005 ids cost 4 waves. The scan
read 100 column pages in 2 waves. The 20,000-row version of the same
graph shows the same order: 14 requests in 3 waves against 30 in 4. The
hosted personal graph agreed in direction, timed end to end from a client
outside the server's region: 1.05–1.09 s with the selective binding
declared last against 1.23–1.43 s declared first.

On a local filesystem the order reverses. Over a local graph of 50,000
nodes and 500,000 edges, one-shot CLI runs answered the selective start in
0.03 s against 0.16 s. So
no single rows-only constant can serve both stores, and a
cardinality-driven choice of the root binding, which an LLM-authored
query most needs, would slow hosted reads until the cost model can see
waves.

### The request cost is known and stable

The in-region cost of one request was measured from inside the serving
container on Railway against its S3-compatible bucket, 10 requests per
row on one reused connection:

| Request | Median | Range |
|---|---:|---:|
| HEAD | 19–20 ms | 17–26 ms |
| LIST of 1,000 keys (308 KB) | 40 ms | 39–42 ms |

GQT's `s3-like` model (17 ms per request) was set from the branch-age
chart. This measurement agrees with it closely enough to price waves, and
the per-request cost is a property of the store, not of the data.

## User and operational behavior

Results do not change. Plans on object storage do: a traversal from a
selective binding may run on the CSR and read its destination by
`hash_join`, where today it runs indexed hops and `id_lookup`. Explain
shows why. It adds a `storage_profile` plan row naming the profile and
its constants, and statistics rows for the priced alternatives of each
affected decision. An illustration of the shape, not of the final format:

```text
plan  storage_profile   object_store latency_units=… parallelism=64
expand $c WorksAt $p: mode csr   (indexed: … units in 3 waves; csr: … units in 1 wave)
statistics  fragments(node:Person)  50  manifest fragments at the pinned version
```

Explain also starts printing `ExpandCostInputs` for every costed `Expand`,
which closes a gap: today those inputs reach the plan but not the explain
document.

A plan carries its profile. A plan replayed on another backend runs with
the recorded profile, as it runs with its recorded memory limit; nothing
about correctness depends on it.

A GQT case can pin the profile with a `# storage-profile: object-store`
header, beside `# traversal:`, so a DST or local case can assert the plan
an object-store graph would get. The pin is harness-only, as the traversal
pin is: there is no `set` statement, and a served run refuses it.

Operators need not do anything. The profile follows the graph's URI.

## Design

### Definitions

- ***Wave***: a set of requests the engine issues without waiting on each
  other. A wave of `R` requests on a store of I/O parallelism `N` costs
  `⌈R / N⌉` round trips.
- ***Serial depth***: the number of waves an alternative waits through in
  series. Its request cost is `serial depth × latency`.
- ***Storage profile***: the triple `(latency, parallelism, byte cost)` that
  converts waves and bytes into the planner's cost unit. It is chosen from
  `omnigraph_storage::StorageKind` when the query is planned.

### The model

The unit stays today's row. Today's constants already are the ratios of
a per-row time model: a CSR build row costs 1.5 indexed probe rows, and an
`id_lookup` row costs 8 scanned rows. The two decisions define their rows
separately and are never compared with each other; this RFC treats an
indexed probe row and a scanned row as the same unit, which changes no
current decision. The model adds one term per alternative:

```text
cost(alternative) = row work (today's formula, unchanged)
                  + serial depth × latency_units
                  + bytes × byte_units
```

`latency_units` is the profile's request latency expressed in row units.
For the object-store profile it is calibrated as 20 ms divided by the
measured cost of one indexed-probe row on the reference host. The local
profile sets `latency_units = 0` and `byte_units = 0`, so every local
decision is exactly today's decision. That is a property the tests prove
(Evidence), not an approximation.

Serial depth comes from the request model below. It needs only facts the
planner holds without reading data: row estimates (`estimate_rows`),
fragment counts and file bytes from the pinned manifests `gather` already
opens, the projected column count, and the profile's parallelism.

### The request model

The planner prices the warm steady state of a server: Lance's session
caches hold manifests, file footers, column metadata and index pages. Lance
11 caches no data pages, so data reads are paid on every query. Cold costs
are paid once per process and per table version, so they do not decide
between alternatives that run many times.

| Operator | Serial depth (warm) | Bytes priced | Source of the shape |
|---|---|---|---|
| Table scan (`ScanExec`, pipelined) with `F` fragments, `C` projected columns | `⌈F × C / N⌉` (one GET per column page; fragments open together and share one scheduler capped at `N`) | the projected columns' bytes over all fragments | `operators/scan/pipelined.rs`; Lance `filtered_read.rs` |
| Scan with a pushed key or index filter | 1 (index from cache, then one take wave over the matching fragments only) | 0 | Lance `ScalarIndexExec`; GQT step logs |
| `id_lookup` over a frontier of `n` rows | `Σ_slices ⌈R_s / N⌉` over `⌈n / 256⌉` slices, which are awaited one at a time; `R_s` is the projected columns times the fragments holding the slice's ids (in the measured runs 38–52 requests, one wave per slice) | 0 | `operators/scan/input.rs` `hydrate_nodes` |
| `HashJoin` build of a destination table | depth of the destination scan, plus 1 because the build waits for the probe's first batch | the build's projected bytes (`build_side_bytes`, as the memory admission computes them) | `operators/hash_join.rs` |
| Indexed `Expand`, per hop and orientation | `⌈2 × min(F_e, matched) / N⌉` per input batch, plus a full scan of the edge table's unindexed fragments. The lookup is one Lance scan per input batch and orientation over all of the batch's distinct source ids (`scan_neighbor_map`). An input batch is the producer's, at most the session's 8,192 rows and smaller when the producer reaches its byte target first. The 256-row `EXPAND_OUTPUT_ROWS` bounds only the output chunks, which issue no requests | 0 for the indexed take; the unindexed fragments' key and opposite-endpoint columns | `operators/single_hop.rs` `prepare`; `engine/graph.rs` `scan_neighbor_map` |
| CSR `Expand` | the build's depth `Σ_types ⌈2 × F_e / N⌉`, priced per query | the `src` and `dst` columns' bytes of every edge table it covers | `graph_index/mod.rs` |
| `MetadataCount` | 0 | 0 | `operators/metadata_count.rs` |

Bytes come from the column statistics and file sizes the planner already
reads for `hash_join` admission, falling back to whole-file bytes as
`build_side_bytes` does today. Dependent reads (a key-filtered scan,
`id_lookup`, an indexed take) price zero bytes in this RFC. That
understates them: Lance fetches whole chunks, and in the measured run
`id_lookup` over 0.5% of Person read 4.0 MB against a full scan's 4.8 MB.
But the error leans toward today's choice, and a chunk-level byte
estimate can refine it later without changing the model.

The CSR is priced as a build because the planner cannot see the
process-wide `RuntimeCache` without taking a hidden input (see
Alternatives). Inside one plan, a CSR an earlier operator realized is
already free (`csr_cached`). Today every `AntiJoin` sets `csr_cached`
(`Lowering::lower`, the `AntiJoin` arm in `optimizer.rs`), even when its
block will not build the CSR. Once step 2 makes the CSR's price the main
lever on object storage, that flag would make an `Expand` after such a
block see the CSR as free, and then pay its build at run time. Correcting
the flag is therefore a co-requisite of step 2 (Rollout). It is tracked
as its own change because it also changes some local plans, which the
profile itself never does.

Applied to the measured plans, the model reproduces the measured order. In
the 200,000-row `$c`-first plan, the indexed hop (3 waves) prices above the
CSR build (2 waves and about 3 MB), and `id_lookup` (4 waves) prices above
a `hash_join` of Person (2 waves plus 1). The planner therefore picks the
key lookup, the CSR and the `hash_join`. On a warm server the CSR is
usually already built, so the run costs about 4 waves against today's 9.
The acceptance threshold in Evidence holds the implementation to the
measured numbers.

### Known limitation: average-degree estimates

Serial depth is computed from the same row estimates as today's model,
and those use an edge type's average fanout. A hub root, a node whose
degree is far above average, is therefore under-priced: its frontier,
and with it the number of `id_lookup` slices, is larger than estimated.
The review of PR #973 flags the same effect for its correlated-block
strategy, which reported an estimate of 1,890 rows against 42,912 actual.
The request term does not cause this error, but it multiplies its cost
on object storage, since each missed slice is a round trip. The
traversal mode already re-decides at run time on the observed frontier;
the destination access path does not (Unresolved question 5).

### Where each decision uses it

- **Expand mode.** `choose_expand_mode` compares `cost(indexed)` with
  `cost(csr)` under the recorded profile. The frontier and hop ceilings
  (`OMNIGRAPH_EXPAND_INDEXED_MAX_FRONTIER`, `_MAX_HOPS`) are unchanged.
  The run-time re-decisions (`decide_expand_start`, `should_switch_to_csr`,
  and the per-batch check of `single_hop`) call the same function with the
  profile carried on the node in `ExpandCostInputs`, so execution cannot
  undo a plan-time choice by pricing with a different model.
- **Access path.** `choose_access_path` compares `cost(id_lookup)` with
  `cost(hash_join)`. The memory admission (build bytes at most a quarter
  of the query pool) stays a hard condition, not a cost, and the declared
  `id_lookup` fallback on memory refusal is unchanged.
- Late materialization, the contains-join side and the correlated-block
  strategy of PR #973 keep their current rules here. Each can adopt the
  model in a later change without a new RFC, because it reads the same
  recorded profile.

### Recording, explain and replay

- `QuerySource::gather` reads the snapshot's root URI and classifies it
  with `storage_kind_for_uri`. `Local` maps to the local profile, and `S3`
  and `Azure` to the object-store profile. DST's `shared-memory://` roots
  classify as `Local`, as they do today. The harness pin overrides the
  choice.
- `Assumptions` gains a typed `storage_profile` field, read by execution
  exactly as `memory_limit` and `gate_policy` are. The storage kind cannot
  be derived from the dataset pins, because `dataset_path` is relative to
  the root, so it has to be recorded.
- `ExpandCostInputs` carries the profile constants, and `AccessPath`
  decisions record their priced alternatives as statistics rows.
- Fragment counts are statistics rows (`fragments(<table>)`), computed from
  the manifests `gather` already opened. Like file bytes they are pure
  functions of the pins, so they need no assumption field.
- `BOUND_PLAN_VERSION` increases. An older bound plan is refused with the
  existing regeneration instruction.
- Replay reads the recorded profile. The scrubbed-replay rule holds: no
  input beside the bound plan and the snapshot reaches execution.

## Invariants

- **7. Physical acceleration is derived state.** The profile changes which
  operator runs and never what a query means. Every affected decision
  chooses between alternatives the execution guide already declares
  semantically identical.
- **9. Query semantics are typed structures.** The profile is a typed
  field of `Assumptions` and `ExpandCostInputs`, not a string or an ambient
  read.
- **11. Bounded and observable.** Explain prints the profile and every
  priced alternative. The memory admission of `hash_join` is unchanged.
- **13. Evidence matches the boundary.** The cost claims carry a
  checked-in instrument (Evidence).
- **Deny-list: cost-blind plan choice or hidden statistics.** Every input
  of every priced decision is recorded in the plan and printed by
  explain, including the profile and the fragment counts.
- **Deny-list: a cloud-only correctness fix or a separate cloud fork.**
  This is cost, not correctness. There is one model and one code path,
  and the local profile is the same model with two constants set to zero.
  No result, error or durable effect depends on the profile.
- **Deny-list: a logical precondition on fragment count.** Fragment
  counts only price alternatives. A wrong count yields a slower plan,
  never a wrong answer.
- **RFC 0047.** Identical inputs, now including the recorded profile, give
  the identical accepted plan. Execution chooses only declared
  alternatives and reports which it took (`ran`). Plan acceptance does not
  validate `ExpandPolicy` or `AccessPath` today, and nothing here needs it
  to.

## Compatibility and reversibility

No storage, wire or query-language change. Bound plans need regeneration
across the version bump, as every plan-envelope change does. The profile
leaves local and DST plans unchanged, and the GQT corpus with them. GQT
runs on local and DST storage, which keep the local profile, and the one
case that declares both environments and asserts a mode
(`issue_659_typed_edge_alternation.gqt`) asserts a budgeted selection,
which ignores cost. The one local change in this RFC's rollout is the
`AntiJoin` `csr_cached` correction that lands with step 2, which carries
its own case updates.

Reverting takes two constants: setting the object-store profile's
`latency_units` and `byte_units` to zero restores today's plans everywhere,
without a format change.

## Alternatives

- **Do nothing; tell authors to declare the selective binding first.** The
  measurements show that advice slows hosted reads.
- **Two models: rows for local storage, time for object storage.** This
  yields the same plans as the chosen design, but leaves two formulas to
  keep in step. Expressing today's constants as per-row costs makes the
  local model a special case of one formula.
- **Measure latency at run time and feed it into planning.** The plan
  would depend on process history that it does not record, breaking the
  equal-inputs, equal-plan rule of RFC 0047. A recorded profile with a
  later operator-configurable value gets the same benefit
  deterministically.
- **Price the CSR the `RuntimeCache` already holds as free.** On the
  20,000-row graph this alone would turn the selective start's 30 requests
  into 18. But cache contents are process state, not plan input, and a
  replay in another process could choose differently. That needs its own
  decision about declared alternatives, and is out of scope here.
- **Make dependent lookups cheaper first** (larger or concurrent
  `id_lookup` slices, the Lance 13 index-metadata cache and BTREE page
  coalescing). These are worth doing and shrink the gap, but the planner
  still has to know a wave's price to choose. They change the request
  model's constants, not the need for it.
- **Kuzu-style cost-based join ordering now.** It was measured: ordering by
  rows alone would pick the 9-wave plan above. It can follow on this
  model.

## Evidence and tests

- **Planner unit tests** (`omnigraph-planner` `cost.rs`): a table-driven
  check that the local profile reproduces today's decisions across a grid
  of frontier, fanout, table and fragment sizes, for both decisions and
  the run-time switch; and the object-store flips at the measured shapes.
- **GQT cases** under `cases/v2/planner/`: the two declaration orders of
  the measured query with `# storage-profile: object-store`, asserting
  `expand … mode csr` and `hash join $p` for the selective start, and the
  unchanged plan for the scan start. Every current case keeps passing
  unchanged.
- **Cost instrument**: the measured fixtures checked in under
  `benchmarks/`, run with `--measure --model s3-like`. Acceptance holds
  the warm selective-start step of the 200,000-row graph at **at most 6
  request waves and 110 ms** (from 9 and 161), the scan-start step at its
  current 3 waves and 53 ms, and both steps under `--model local`
  unchanged.
- **Replay**: `engine_v2_plan_replay.rs` gains a plan gathered under the
  object-store pin and replayed in a local session, with the same plan and
  trace; `engine_v2_scrubbed_replay.rs` checks that an ambient storage kind
  does not reach a replay.
- **Calibration record**: the in-region request timings above and the
  reference-host measurement behind `latency_units`, kept beside the
  benchmark fixtures with the host description and command that Rollout
  step 4 lists.

## Rollout

1. **Plumbing.** The profile is gathered, recorded, explained and pinnable,
   fragment counts become statistics, and the plan version increases. Both
   profiles price waves at zero, so no plan changes. This can ship alone.
2. **Expand mode** uses the object-store profile, at plan time and in the
   run-time re-decisions. This is the first change object-store plans see.
   It lands together with the `AntiJoin` `csr_cached` correction, whose
   local plan changes are reviewed with their own GQT updates.
3. **Access path** uses the object-store profile.
4. **Calibration** of `latency_units` and `byte_units` against a reference
   host. The record, kept with the fixtures, names the host's CPU model,
   core count, memory, operating system and storage, the Rust toolchain
   and Lance versions, the exact benchmark command and fixture, and the
   measured per-row cost the constants are derived from, so a later
   maintainer can reproduce the ratio.

`implementation` advances to `in-progress` with step 1 and to `complete`
with step 4. Anchor selection, cache-aware CSR pricing and the other
decisions are separate work that reads the same profile.

## Unresolved questions

1. **Where the object-store constants come from.** The options are fixed
   defaults per storage kind, a process environment knob read once in
   `gather` and recorded (as the expand ceilings are), or a per-graph value
   in `cluster.yaml`. The proposal is fixed defaults plus a recorded
   environment override.
2. **One object-store profile or several.** S3 Standard, Azure and
   low-latency stores such as S3 Express differ. The proposal is one
   profile until a measurement shows a decision that differs.
3. **Warm pricing for one-shot processes.** An embedded CLI invocation is
   cold, so warm pricing understates its index and footer requests. The
   proposal is to accept that, since cold costs are a once-per-process
   overhead of every plan.
4. **The persisted CSR artifact.** After `optimize`,
   `__graph_index/csr-current.bin` turns a CSR build into one GET. Knowing
   whether it matches the pins at plan time costs a request. The proposal
   is to price the build and leave the artifact as a run-time discount.
5. **A run-time switch for the access path.** Expand mode corrects an
   under-estimated frontier at run time; `id_lookup` cannot become a
   `hash_join` once it runs. A declared switch, taken when the observed
   frontier's slices outgrow the build's price and reported as `ran`,
   would bound the hub-root error. The proposal is to leave it to a
   follow-up and record the limitation here.

## Decision log

- 2026-10-13: drafted from the anchor-order measurements (GQT `--measure`,
  DST, `s3-like` and `unit` models; local CLI), the in-region request
  timings on the hosted personal graph, and a survey of the cost-driven
  decisions and their recording.
- 2026-10-14: review on PR #996. Added a priced-bytes column to the
  request model and stated that dependent reads price zero bytes; made
  the `AntiJoin` `csr_cached` correction a co-requisite of step 2;
  clarified that an indexed hop scans once per input batch (the 256-row
  `EXPAND_OUTPUT_ROWS` bounds only output chunks); required the
  calibration record to name its host and command; added the
  average-degree limitation from PR #973's review and Unresolved
  question 5.
