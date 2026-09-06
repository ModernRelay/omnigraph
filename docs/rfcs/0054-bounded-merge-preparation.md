---
rfc: "0054"
title: "Accepted-context reuse and bounded merge preparation"
track: maintainer
status: accepted
implementation: in-progress
authors:
  - Codex
created: 2026-09-05
updated: 2026-09-06
discussion: https://github.com/ModernRelay/omnigraph/pull/638
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC 0054: Accepted-context reuse and bounded merge preparation

## Summary

Prepare up to four eligible non-Blob tables concurrently, using the merge
attempt's accepted context. Retain the existing three-way classifier,
combined graph validation, lock acquisition boundaries, recovery ownership,
serial durable table effects, and single graph publication.

The earlier isolated prototype motivated this work; its measurements below
are not results for the implementation in this RFC. This change requires no Lance upgrade,
storage migration, new transaction protocol, or wire API. It does not make
merge cost constant in catalog size, history depth, or unrelated fragments.

The accepted-context opener is already delivered in the implementation base
`86aa508580097cf46fb4c32b55f053d41f98e412`. Its historical savings below must
not be counted again as scheduler gains. The implementation extends the
existing scenario harness with small multi-table fixtures, scoped width controls
and delayed ObjectStore calls; that delay is an API-level diagnostic, not a
claim of wire-level S3 latency equivalence.

## Motivation

Two repeated costs appear in the existing implementation:

1. The existing-target preflight in
   [merge.rs](../../crates/omnigraph/src/exec/merge.rs) calls the generic
   [mutation opener](../../crates/omnigraph/src/db/omnigraph/table_ops.rs)
   without a transaction. That route resolves the branch again and re-enters
   [schema contract validation](../../crates/omnigraph/src/db/schema_state.rs)
   for each such target, even though merge already captured accepted authority
   and revalidates it before effects.
2. Independent table candidates are prepared sequentially. Their snapshot
   reads, candidate hydration, three-way comparison, and local staging spend
   substantial time waiting for storage. Preparing another table can overlap
   that wait without changing which graph state is eventually published.

The repeated-schema claim needs precise wording. The opener reads the schema
source, performs two existence checks, reads persisted IR and state, parses
the declarations, and compiles their semantic shape to validate the accepted
IR. It does not recompile every query. The number of requests per opener is
fixed; schema bytes and parsing work increase with schema size.

Context reuse removed eight physical requests per touched existing target in
the measured fixtures: five schema requests and three branch-resolution
requests. Concurrency produced the larger latency reduction. Recording both
changes together makes the measured benefit and the pre-effect concurrency
contract reviewable; the opener refactor remains independently shippable.

## User and operational behavior

Existing branch merge commands, HTTP requests, actor checks, outcomes, and
graph visibility remain the contract. A successful merge still exposes all
table changes and lineage together. Genuine three-way conflicts and graph
constraint failures retain their typed outcomes and deterministic ordering.

The expected benefit is lower latency when several tables require preparation
and storage wait dominates. A one-table merge has little work to overlap.
This proposal does not improve branch create/delete directly or establish
concurrent merges to different targets: the current exclusive coordination
remains. Existing index defects and merge-base bugs require their own fixes.

Concurrency is an internal cap of at most four. Production remains at one
until the qualification gates pass; HTTP has a separate hard ceiling of one
because its request owner cannot drain on disconnect. There is no new CLI flag,
HTTP field, or environment-variable API. The prototype's
`OMNIGRAPH_DESIGN_VARIANT` switch is investigation instrumentation only.

## Design

### 1. Reuse the accepted attempt when opening an existing target

Use the existing source and target `WriteTxn` captures, accepted catalog,
snapshots, and native branch bindings. Do not introduce another transaction
type, mutable cache, or independently refreshable copy of authority.

At the current pre-arm existing-target preparation site, use a private helper
whose inputs come from that accepted attempt:

- the target manifest entry, including stable table identity and dataset path;
- the target native branch binding, including its existing incarnation
  semantics, rather than a branch name freshly resolved through the coordinator;
- the existing storage boundary used to open the physical target HEAD.

The helper always returns an opened `SnapshotHandle`, full path, and native
binding for the existing `PreparedExistingMergeTarget` owner. It must check
that this is the existing-owned-target case. Main uses its existing native
binding representation. An inherited target requiring a first-touch fork
continues through the original deferred-fork path.

Passing `Some(txn)` to the generic opener is insufficient: its accumulation
route may intentionally return `handle: None` for an owned/main target. The
merge preflight requires an actual handle with a physical baseline that can
be checked and carried through the effect phase.

Keep every operation-level fence, including the schema-apply exclusion check,
the pending-recovery barriers, and `revalidate_merge_inputs`. Keep the final
target authority and manifest-version checks. The existing source-advance
rule remains: use the captured source commit, while validating its schema
and incarnation under the current protocol.

After collecting all existing target handles, retain
`ensure_existing_effect_baseline` at the current last pre-arm boundary under
the full table gate envelope. Retain pure-insert target-incarnation
revalidation where applicable. Consume the same prepared handles in the
physical effect phase; do not reopen mutable HEADs after arming recovery.
An authority failure follows the existing typed failure/retry policy; any
new authority attempt must recapture its entire accepted view.

### 2. Schedule existing scalar preparation with bounded concurrency

Keep `ordered_table_keys` as the canonical order. Apply the existing unchanged
table checks and identity-compatibility checks before admitting work.
For each eligible non-Blob table, call the existing `classify_adopt` or
`stage_streaming_table_merge` path with the same base, source, target, catalog,
and native target binding used today. Preserve the existing adoption proofs,
lineage candidate selection, hydration, and fallback to ordered three-way
comparison. Continue comparing logical graph IDs; numeric Lance row IDs are
not unique across independently written branches.

Each worker owns its temporary state and returns the table key, candidate,
and table-local conflicts. It does not mutate shared candidate/conflict
collections or graph authority. A bounded ordered stream polls at most four
table preparations. Collect results in the original table order, preserving
conflict ordering and deterministic error selection among concurrently
eligible scalar tables. Preparation results awaiting their turn count
toward the resource budget. Avoid eagerly constructing an unbounded queue
of materialized jobs. Admission-time identity/schema errors occupy their
ordered result slot too; do not surface a later table's precheck failure
ahead of an earlier table's preparation failure.

Keep Blob descriptor preflight, shared external-payload budgets, payload
materialization, and their existing phase ordering on the original path.
Mixed Blob/scalar cases must preserve the original diagnostic ordering too;
the prototype alone is not evidence for that property. An implementation may
use ordered barriers at ineligible tables to preserve it, accepting less
overlap in mixed workloads.

Preparation includes **local scratch writes**: `StagedTableWriter` creates
temporary Lance datasets. It is not a read-only operation. Workers may read
captured graph datasets and write exclusively owned staging directories, but
may not create/move graph-owned native refs, mutate graph tables, persist a
graph recovery intent, or publish graph content. Scratch ownership transfers
to the existing candidate owner only when the collector accepts the result.

### 3. Bound the additional retained working set

The initial scheduler drains fixed ordered windows before refilling. A singleton
window uses serial preparation immediately. Source-state adoption is an ordered
serial barrier because it can enter transaction-history proofs and their source
normalizer; those allocation owners remain to be qualified. General non-Blob
cursor, lineage, comparison and staging routes use shared preparation accounting. Small metadata is charged conservatively
until collection, so a large merge may fall back earlier than its live bytes
alone require. Scratch telemetry counts successful logical Arrow payload, not
physical disk usage. Parallel scans set both the scanner and execution-context
concurrency to one, one fragment of read-ahead, and an 8 MiB I/O buffer per
scan. Typed stable row-ID masks keep hydration on the configured filtered-read
path instead of Lance's take shortcut. Serial scans retain their existing
tuning. The caller-owned allowance is not an RSS cap.

Qualification admits only small, known V2.2 snapshots: at most 64 fragments,
one data file per fragment, 8 MiB aggregate encoded data, and 8,192 physical
rows. Unknown sizes/counts, overlays, any persisted index section and external
row-ID metadata use serial fallback. Inline row-ID metadata and deletion
counts are checked separately. The primary store and every referenced data or
deletion base must report I/O parallelism no greater than 64. Qualification
uses accepted metadata and existing store resolution; it does not HEAD unknown
files, rewrite native base identities, or construct replacement stores.

Inherited branch files retain their original base IDs. Lance 11 ignores the
supplied 8 MiB scheduler for those files and constructs a native scheduler
with `32 MiB * io_parallelism` soft capacity instead. Their decoder options
still follow the configured reader path. The small-file gate narrows admitted
encoded input; compressed file size does not bound decoded memory. Native
decoder expansion and process RSS remain independent activation gates.

The native resource envelope is separate: an ordered cursor retains its existing
150 MiB spill pool and 100 GiB scratch ceiling. Three cursors per table and four
workers can expose up to twelve such pools (1,800 MiB total capacity), not twelve
fully allocated buffers. Native I/O buffers apply soft backpressure; an oversized
page/request can exceed the requested 8 MiB. Qualification records process RSS
as well as controlled retained bytes, and does not claim a 128 MiB process bound.

Four workers alone are not a byte bound. Preserve the existing per-row,
keyed-write, hydration, lineage-candidate, and operation-wide validation
limits. In particular, the existing `MergeValidationBudget` is shared across
the combined change set; it must not become four independent allowances.

Add operation-local accounting to the preparation owners, not a new storage
buffer pool. The proposed initial parallel-preparation allowance is
`4 * KEYED_WRITE_MAX_BYTES` (currently **128 MiB**). Charge controlled retained
allocations in active and completed-but-uncollected workers: candidate IDs,
hydrated batches, copied rows, staging buffers, and worker-local result and
conflict payloads. Charge shared backing arrays conservatively; small slices
must not hide retained parent buffers. Transfer ownership/accounting exactly
once when a result joins the existing serial candidate/validation state.

This is a cap on the speculative parallel working set, not a promise that
total process RSS fits 128 MiB. Lance/DataFusion pools, decoder allocations,
the already-required collected merge state, and local staging files have
their existing owners. Share the existing storage/session resources rather
than constructing a fresh memory pool per worker. Instrument peak accounted
bytes, in-flight batches, scratch bytes, and physical request concurrency;
four active tables can issue more than four physical requests.

Use non-blocking reservations before controllable buffer growth. A worker
must not wait for permits while retaining the memory that prevents another
worker from finishing. On parallel-budget pressure, stop admission, settle
the current uncollected window, discard its candidates and scratch state,
and retry that window at width two, then width one. Already collected results
are retained once. This gives at most three width passes for a window and
does not retry logical conflicts, storage failures, or changed authority.

At width one, use the existing serial limits and behavior. A row or merge
that previously fit those limits must not acquire a new refusal solely
because parallel work exhausted the additional allowance. All attempts use
the same immutable accepted inputs and stay before recovery arming; a width
fallback is a scheduling retry, not an authority retry. Expose fallback
counts in existing instrumentation.

Decoder allocations that precede size discovery need separate qualification:
record the maximum in-flight overshoot, reduce batch/prefetch settings where
the existing API permits it, and disable concurrent preparation for shapes
whose overshoot cannot be bounded acceptably. The 128 MiB default and its
coverage are proposed policy, **not measured properties of the prototype**.
The resource gate blocks enabling width four until this accounting and
fallback preserve valid serial behavior without deadlock or unbounded queues.

### 4. Preserve authority, validation, and publication sequencing

The operation remains:

1. Acquire the existing schema/lifecycle protection and capture merge inputs.
2. Hold the existing merge-exclusive coordination while preparing candidates,
   with the bounded scheduling above.
3. Run the existing combined graph validation at its current boundary,
   including adoption deltas, uniqueness, endpoints, cardinality, and deletion
   effects. Retain the existing proven pure-insert fast-forward exemption;
   add no new validation bypass. Independent table preparation does not
   establish graph validity.
4. Acquire the existing conservative source/target table gate envelope in its
   current global order; resolve/refuse relevant recovery and perform the
   current final authority and physical-baseline checks.
5. Persist the existing complete recovery intent before graph-owned effects.
6. Apply durable table effects serially, confirm outcomes through the existing
   recovery protocol, and publish every visible pointer and lineage update
   through one `__manifest` publication.

This preserves the actual distinction between preparation and the final
table-gated phase. It does not move preparation outside today's higher-level
locks or claim that the final table gates are held throughout preparation.

### 5. Own failure and cancellation to completion

Keep preparation futures inside the merge operation; do not detach workers.
On an error or budget fallback, stop admitting new tables, signal active
workers to stop at bounded checkpoints, await issued scratch operations,
and settle every worker before releasing its scratch owner or beginning a
replacement window. Cleanup must not race a still-running temporary writer.
Return the original typed failure after settlement; additional cleanup errors
remain observable under existing error reporting.

Cooperative cancellation before arming must settle all preparations before
the operation returns, leaving no graph-owned effects or live preparation
task. A forced task drop or process death cannot promise asynchronous scratch
cleanup: preserve the existing staging ownership/abandoned-temp behavior and
prove that any residual files remain private and unreachable from graph
publication. Qualify the actual server cancellation path; if it cannot own
the drain, keep width one for that entry point until it can. Do not claim a
new general served-operation supervisor as part of this change.

Cancellation after arming follows the unchanged existing recovery protocol.
This proposal does not introduce concurrent durable effects or rely on
dropping a remote write future to prove that it did not land.

## Invariants

The [architectural invariants](../dev/invariants.md) apply without exception.

| Boundary | Preservation argument |
|---|---|
| Substrate ownership (1) | Lance still owns dataset reads, scratch datasets, writes, versions, and indexes; no dependency or storage format change. |
| Single publication and recovery (2, 4, 5) | Preparations cannot move graph-owned refs; durable effects remain serial under the existing intent and one graph publication. |
| Coherent accepted view and identity (3, 6, 12) | Workers and the private opener borrow the same accepted attempt; exact final authority, baseline, and incarnation checks remain. |
| Derived indexes and integrity (7, 8) | Reuse existing comparison and validation paths; index coverage does not become a precondition for success. |
| Typed semantics and trust (9, 10) | No new query/API semantics or actor bypass; all entry-point authorization stays in place. |
| Bounded resources and evidence (11, 13) | Cap workers and account additional retained data; settle retries and cancellation; qualify through existing test and instrumentation owners. |

No deny-list exception is requested. In particular, this adds no WAL, shadow
manifest, public writable dataset handle, custom storage buffer pool, index
rebuild on the write path, or claim of distributed fencing. Existing
catalog/history/fragment scaling remains a known limitation against the
working-set objective; the speedup is not evidence that it has been resolved.

## Compatibility and reversibility

Storage and wire formats, recovery records, branch identities, merge outcomes,
and the Lance 11.0.0 dependency remain compatible. No migration or operator
maintenance is required. Local and object-storage execution use the same
engine path. Existing one-mutation-process limitations and Azure qualification
boundaries remain as documented in the architectural invariants.

Either change can be reverted independently. Width one retains context reuse
while disabling parallel preparation. Test-only scoped controls may compare
widths and the old opener; production must not inherit the prototype's global
four-variant switch or repurpose `OMNIGRAPH_LOAD_CONCURRENCY`, which controls a
different write phase.

## Alternatives

| Alternative | Assessment |
|---|---|
| Keep current behavior | Lowest change risk, but retains demonstrated repeated reads and avoidable serial wait. |
| Ship context reuse alone | Valid first phase: about 1.13× in both multi-table fixtures. Smaller gain, independently useful. |
| Parallelize only | About 1.90×/2.01× for eight/29 tables, but leaves the repeated opener work. |
| Global schema cache | Introduces lifetime/invalidation concerns and does not by itself reuse the accepted native binding. Attempt-local reuse is sufficient. |
| Parallel durable commits or shorter locks | Changes recovery/interleaving obligations; unnecessary for the measured improvement. Requires a separate proposal. |
| Replace comparison with Lance merge-insert | Lance upsert does not supply graph ancestry, three-way conflict semantics, or multi-table validation/publication. |
| Persistent catalog/fragment trees and precomputed merge-base structures | Relevant to flat catalog/history/fragment cost, but requires substantially broader storage and authority changes. Not a dependency of this RFC. |

## Evidence and tests

### Measured evidence and limits

The experiment used Omnigraph commit
`a09176d72601ce9c965de638cb4fc4179ab710c4`, Lance **11.0.0**, and upstream
commit `ab6b5bbe46009ed78746b444df8db59a8bc5d842`. The retained
[24-run CSV](https://github.com/ModernRelay/omnigraph/blob/1b71a19ebe2433b5aeb682b512fb7f9efcaadc95/docs/rfcs/assets/0053-merge-preparation-results.csv) and
[investigation patch archive](https://github.com/ModernRelay/omnigraph/blob/1b71a19ebe2433b5aeb682b512fb7f9efcaadc95/docs/rfcs/assets/0053-merge-preparation-prototype.zip) establish
which runs and code produced these observations. The archive preserves the
exact patch bytes; CSV line endings are normalized without changing cells.
The patch is evidence, not
the production implementation: it also contains server instrumentation and
a native-row-ID guard, and has no aggregate preparation accounting.

| Touched tables | Control | Context reuse | Parallel preparation | Both | Combined gain |
|---|---:|---:|---:|---:|---:|
| 8 | 14.420 s | 12.749 s | 7.582 s | 6.124 s | 2.35× / 57.5% lower latency |
| 29 | 47.680 s | 42.208 s | 23.737 s | 18.992 s | 2.51× / 60.2% lower latency |

These are medians of two repetitions per variant on a 121-table populated
catalog, with tiny disjoint source/target changes in each touched table.
The second repetition reversed variant order. The backend was local MinIO
behind an asynchronous proxy adding 17 ms per physical request. The proxy
preserved concurrency but buffered complete bodies; there was no bandwidth
cap, jitter, TLS, throttling, or provider tail-latency emulation in this run.

One diagnostic executable supplied all four variants. It used the dev profile
with dependencies optimized at level two, `RUSTFLAGS=--cfg tokio_unstable`,
an Apple M5 Pro, and `LANCE_MEM_POOL_SIZE=1073741824`. Compilation completed
before timing. Lineage mode was enabled. Each timed merge used a fresh server
and the same point-read warmup; fixture setup and complete source/target
export checks were outside the timed interval. These are not production p95
estimates, release-build results, or a cold-cache guarantee.

At 29 tables, context reuse reduced requests from 2,647 to 2,415 and schema
requests from 157 to 12. At eight tables, the corresponding counts were
883 to 819 and 52 to 12. The remaining 12 schema requests are fixture-specific
operation-level work, not a universal allowance for every merge/retry path.
The variant named `schema` also reuses branch/table bindings; its gain cannot
be attributed to schema parsing alone.

In the 29-table comparison, final revalidation fell from 7.196 s to 2.153 s;
serial physical publication stayed near 4.4 s and graph publication near
0.4 s. Both variants performed 87 hydration calls for 174 rows and 29
upserts, with zero ordered full-table cursors. Per-table timing sums overlap
under concurrency and must not be added as elapsed phase time.

Additional one-repetition controls found 1.07× for one touched table, 2.30×
for eight tables at 50 ms request delay, 1.05× for the 100,000-row fixture,
and 1.02× for the 256-history-update fixture. History and fragments grew
together in that last fixture. Neither these controls nor the multi-table
experiment proves flat cost as catalog, history, or unrelated fragments grow.

All 24 measured merges returned HTTP 200. Full source and target exports
verified exact payloads, logical IDs, row/type counts, and edge endpoints.
The clean baseline reported 142 passing focused tests; each experimental
variant reported 143, including the additional sibling-branch row-ID guard.
Each local suite included one S3 guard that returned early without its bucket
setting; that guard subsequently passed explicitly against MinIO. The
three-way truth-table test executed 49 operation pairs; 51 unsupported
grammar cells were not successful executable cases. Expected-bug guards
remained expected-bug evidence. Full workspace CI and release qualification
were not performed for this prototype.

Provenance SHA-256 values:

```text
prototype.patch
42da0d84ec934b2d30b047ae1ba4c313cf7b519e35c1f2213707803bc6d435c7
measured server executable
34e2fc158fc3c308ad49821c2d2762eb0f0bfb5314c912347cab63cdb74a0e6f
```

### Production acceptance gates

Extend existing owners from [the testing map](../dev/testing.md). Do not
create a second merge semantics suite or rely on elapsed-time assertions in
ordinary CI.

| Gate and owner | Required evidence |
|---|---|
| Opener cost: `merge_cost.rs`, existing instrumentation; pattern in `write_cost.rs` | Increasing touched existing targets adds no generic schema/branch re-resolution calls. Required capture/fence reads and physical baseline opens remain. Reproduce the fixture's eight-request-per-target reduction with physical traces. |
| Authority: `branching.rs`, `merge_fast_forward.rs`, `failpoints.rs`, existing server merge concurrency test | Target movement, source advance, same-name branch recreation, schema changes, stale physical heads, and first-touch targets retain exact outcomes. Verify the carried handle is the one checked before arming. |
| Semantics: `merge_truth_table.rs`, `merge_net_zero.rs`, `merge_projection_cache.rs` | Width one and four have identical full graph results, source preservation, conflict values/order, and typed failures across supported operation pairs, node/edge dependencies, and adoption/rewrite mixtures. |
| Blob and resource ownership: existing Blob cases in `branching.rs`, `merge_cost.rs` | Mixed scalar/Blob preparation keeps preflight limits and diagnostics. Wide values, a slow large table among small tables, dense conflicts, and pressured budgets demonstrate width fallback, bounded queues, no permit deadlock, and no new serial-valid refusal. |
| Cancellation/recovery: `failpoints.rs` and existing staging owners | Inject failures, cooperative cancellation, forced drop, and process death at preparation boundaries and fallback. Verify drain/cleanup where the operation can await, and private unreachable residual scratch where it cannot. Prove no graph-owned effects before arm. Run existing merge crash/reopen cases unchanged after arm. |
| Substrate: `lance_surface_guards.rs` | Retain pinned row-ID, historical-image, index, and native-branch guards. Label inherited known failures explicitly. Run required storage guards against a real configured disposable bucket with no early-return substitute. |
| Performance: `merge_cost.rs`, `write_cost_s3.rs`, shared instrumentation | Add a checked-in repeatable benchmark driver with exact fixture/build/store settings, separate elapsed and overlapping intervals, and export-based correctness checks. Record peak accounted bytes, allocation overshoot, active workers, requests/bytes, and fallback counts. |

For the performance gate, use a release build and at least five paired,
alternating repetitions of baseline versus the combined production design.
With the same eight- and 29-table, 17 ms fixtures, require at least **2.0×
median speedup**. For the one-table fixture, require no more than **10% median
regression**. These are proposed promotion thresholds, not measured release
results. If they fail, context reuse may still ship independently; revise the
parallel proposal using the observed bottleneck instead of claiming 2.5×.

Also qualify cold starts, 50 ms delay, bandwidth limits, jitter/throttling,
wide/skewed rows, indexes, catalog size, history depth, and unrelated fragments.
Vary history and fragmentation independently where the fixture permits, and
report limitations where it does not. Include a real deployed-store run
before a production latency claim. Record distributions; five repetitions
are not a reliable p95 estimate. Passing the speed gate cannot waive a
correctness/resource gate.

### Lance sources and related tracking

The investigation read the full upstream domains linked by
[the Lance reading protocol](../dev/lance.md), including transactions,
branches/tags/cleanup, read/write and distributed writing, row IDs, indexes,
object stores, performance, and Blob behavior. Public documentation explains
the model; the pinned code and executable guards decide the relied-on
behavior. Relevant entry points include the
[read/write guide](https://lance.org/guide/read_and_write/),
[distributed-write guide](https://lance.org/guide/distributed_write/),
[transaction specification](https://lance.org/format/table/transaction/),
[pinned merge-insert implementation](https://github.com/lance-format/lance/blob/ab6b5bbe46009ed78746b444df8db59a8bc5d842/rust/lance/src/dataset/write/merge_insert.rs),
[delta readers](https://github.com/lance-format/lance/blob/ab6b5bbe46009ed78746b444df8db59a8bc5d842/rust/lance/src/dataset/delta.rs), and
[commit conflict resolver](https://github.com/lance-format/lance/blob/ab6b5bbe46009ed78746b444df8db59a8bc5d842/rust/lance/src/io/commit/conflict_resolver.rs).

Lance merge-insert is a target/source upsert; transaction rebase reconciles
compatible dataset commits. Neither substitutes for Omnigraph's graph
three-way merge. Distributed fragment writing is a separate, per-dataset
durable-write mechanism. This RFC changes none of those upstream mechanisms.

| Public issue | Relationship to this decision |
|---|---|
| [Omnigraph #310](https://github.com/ModernRelay/omnigraph/issues/310) | Earlier branch-operation request amplification work. The completed fork/delete changes do not establish that merge preparation is bounded. |
| [Omnigraph #504](https://github.com/ModernRelay/omnigraph/issues/504) | Related mutation concurrency work; not evidence that branch merge preparation is already parallel. |
| [Omnigraph #384](https://github.com/ModernRelay/omnigraph/issues/384) | Related target-size scaling, which this RFC does not eliminate. |
| [Omnigraph #641](https://github.com/ModernRelay/omnigraph/issues/641) | Remaining catalog, history, and fragment metadata costs, including fragmented historical merge bases after optimize. |
| [Omnigraph #642](https://github.com/ModernRelay/omnigraph/issues/642) | Fork/delete amplification with live sibling branches; separate from merge table preparation. |
| [Omnigraph #643](https://github.com/ModernRelay/omnigraph/issues/643) | Independent-target merge serialization. This RFC retains the existing graph-wide guards. |
| [Lance #6444](https://github.com/lance-format/lance/issues/6444), [#7363](https://github.com/lance-format/lance/issues/7363), [#8853](https://github.com/lance-format/lance/issues/8853) | Upstream join/update/pruning context; not prerequisites for the two proposed changes. |
| [Lance #7263](https://github.com/lance-format/lance/issues/7263) | Native branch-merge feature request, not an available graph-merge replacement. |
| [Omnigraph #624](https://github.com/ModernRelay/omnigraph/issues/624); [Lance #7840](https://github.com/lance-format/lance/issues/7840) | Reproduced inherited-index defect; an independent correctness issue, not fixed by this optimization. |

Review this proposal in [RFC PR #638](https://github.com/ModernRelay/omnigraph/pull/638).
The issues above track related performance and correctness work separately.

## Rollout

1. Implement the private accepted-context opener and its cost/authority tests.
   This can ship alone once its gates pass; mark implementation `partial`.
2. Implement the ordered scheduler, operation-local accounting, width
   fallback, and cleanup in existing merge/staging owners. Qualify with scoped
   test controls at widths one, two, and four while retaining serial effects.
3. Enable the internal width-four cap only after every relevant correctness,
   resource, and release-benchmark gate passes. Update the measured evidence
   and mark implementation `complete` only when both changes are delivered.

If qualification requires broader storage or recovery changes, keep width
one and return that larger decision to review. Adoption of the persistent
tree redesign, new lock protocols, or parallel durable effects is not part
of this rollout.

## Unresolved questions

No architectural choice is intentionally delegated to implementation. The
initial 128 MiB parallel allowance, width-four cap, serial fallback, and
unchanged authority/effect protocol are the proposed decisions. Whether the
allowance covers the real retained allocations and preserves the measured
gain is an explicit evidence gate above; revise this document if evidence
requires a different policy.

## Decision log

- 2026-09-05: Drafted the narrow accepted-context and concurrent-preparation
  proposal after the controlled four-variant experiment. Kept the measured
  prototype separate from proposed resource accounting and production
  qualification. The broader constant-cost redesign is outside this decision.

- 2026-09-05: Allocated 0053 for publication after checking current main and
  open RFC PRs; 0050 was already reserved by the engine-crate topology proposal.

- 2026-09-06: The maintainer approved implementing the bounded-preparation plan.
  Reallocated this proposal to 0054 because the implementation base reserves
  0053 for retained merged ancestry; checked the registry and open RFC PRs.
  Kept the existing accepted-context opener and publication protocol. The
  resource, cancellation and release qualification gates remain required before
  enabling the production cap.

- 2026-09-06: A real TCP disconnect test confirmed that the pinned Axum/Hyper
  HTTP/1 owner drops a pending merge service future. The HTTP entry therefore
  retains a hard width-one ceiling, including under diagnostic overrides.
  The same authorization and publication body serves both entry points. The
  embedded path remains eligible for bounded preparation under the forced-drop
  private-scratch contract above; a general served-operation supervisor is
  outside this RFC.

- 2026-09-06: Native-reader review found that inherited branch files bypass
  the caller's I/O scheduler. Added conservative metadata eligibility rather
  than altering Lance source identities. Small inherited fixtures can qualify
  at diagnostic widths; unsupported layouts replay serially. This does not
  establish a total-memory bound or waive production activation gates.
