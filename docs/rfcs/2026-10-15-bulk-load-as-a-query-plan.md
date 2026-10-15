---
rfc: "2026-10-15-bulk-load-as-a-query-plan"
title: "Bulk load as a query plan"
track: maintainer
status: draft
implementation: not-started
authors:
  - azimafroozeh
created: 2026-10-15
updated: 2026-10-15
discussion: null
supersedes: []
superseded_by: []
blocked_on: []
---

<!-- Date-slug naming: no provisional number to reserve; the registry row in
README.md is the merge-time collision check. -->

# RFC: Bulk load as a query plan

> A term set in ***bold italics*** is defined at that spot; it is plain text afterwards.

## Summary

A bulk `load csv` statement executes as a ***load plan***, a physical operator
tree the engine's executor runs, where today the loader is one fixed pipeline
that parses and validates on one core. The statement text and validation
semantics stay the same for inputs admitted by both routes. Validation still
precedes table staging, and one `__manifest` publication per statement makes
the load visible atomically. Resource acceptance changes: `write_max_bytes`
bounds each staged-write input, while `query_memory_bytes` becomes the
aggregate allowance for tracked load input and working state. The initial
route retains the whole input and refuses it when that allowance cannot fit.

Load operators own bounded parallel parsing and, for overwrite, a new sealed
within-table fragment writer. Validation shares committed-state reads within
one statement, suppressing duplicate reads while still reading the distinct
fragments its chosen access path needs. `explain` accepts a load statement and
prints the accepted adaptive plan, including its maximum worker count and
fallback. All three modes must use the plan before this RFC is complete.

## Motivation

Three measured facts, all from the LDBC SNB benchmark work (SF30 is the 34 GB
CsvBasic dump; the workstation is the Apple silicon machine the engine
comparisons ran on, local disk, 2026-10-13 to 2026-10-15):

1. **The gap is two orders of magnitude.** Kùzu 0.11.3 loads the SF30 CsvBasic
   set in 97 s and LadybugDB 0.21.1 in 117 s on the measured workstation. Our
   chunked JSONL route (one commit per 8,192 rows, the shape required before
   the `load csv` statement and the byte-bound settings existed) ran over 24
   hours for the same data, and the `load csv` route (about 33 statements,
   one per table) is estimated near an hour, parsing on one core.
2. **The cost shape is measured, not guessed.** Eleven GQT cases using
   generated load recipes under `--measure` (each a one-call load, varying one
   dimension) show: store requests do not grow with the loaded batch, growing
   the edge batch 16x leaves request counts and bytes read identical, so
   referential checks are batched, not per-key store probes. But every load
   call re-reads the tables it validates against: bytes read grow with the
   committed table's size, each additional fragment of a validated table costs
   about six additional GETs per call, and at sixteen fragments about half of
   a call's reads fetch bytes the same call already fetched. A chunked load
   therefore degrades as fragments accumulate between compactions, which
   matches the SF30 run's observed per-commit time creeping up between
   optimizes.
3. **Load behavior is not assertable.** A query's chosen access path is pinned
   deterministically by a plan expectation; a load has no plan, so the eleven
   cases above had to infer its behavior from scaling shapes. The engine
   should declare what a load will do, not leave it to measurement.

The proposed change crosses the loader, shared validation evaluator and
executor: it adds parallel load operators and a new `explain` surface, while
preserving the evaluator's statement-wide semantics. A load call already is
one statement; the read improvement is reuse within that statement, not a
change in validation scope. The in-place alternative is compared below.

## User and operational behavior

- The statement is unchanged:
  `load csv "<uri>" into <kind>:<Type> mode <overwrite|append|merge>
  [delimiter "<c>"] [header] [columns (...)]`. Options may come in any
  order; `mode` stays required (a modeless statement is refused at parse).
- `explain` on a load statement prints the load plan. This is a grammar
  addition: today `explain` wraps only a query declaration
  (`crates/omnigraph-compiler/src/query/query.pest`). Explain reads no payload
  bytes. Source metadata and catalog statistics supply estimates recorded in
  the accepted plan; they do not establish exact row counts or safe CSV
  boundaries. Acceptance fixes the access paths, worker maximum and permitted
  fallback. Execution of that accepted artifact may reduce scan parallelism
  after boundary inspection; it cannot silently select another access path.
  Executed-plan measurements record actual partitions and rows. Separate
  explain and execution calls may accept different plans if their inputs or
  captured statistics change. This follows RFC 0047's inspectable acceptance
  principle while making the load plan's adaptive choice explicit. The
  fields below are normative; exact line spellings are settled at step 2.

  ```text
  load csv "s3://bucket/knows.csv" into edge:Knows mode overwrite
  LoadPlan
    Scan        csv max_partitions=8 fallback=single_scan source_bytes=412345678
    Validate    value_enum edge:Knows columns=(kind)      -> value_constraint_violation
    Validate    unique edge:Knows role=key columns=(key)  -> unique_violation
    Validate    edge_ri edge:Knows endpoint=from node:Person -> orphan_edge
    Validate    edge_ri edge:Knows endpoint=to node:Person -> orphan_edge
    Validate    cardinality edge:Knows endpoint=from node:Person -> cardinality_violation
    Write       overwrite fragments target_size
    Publish     staged edge:Knows, one __manifest publication
  ```

  Operator identity includes the check kind and qualified table key. A
  property constraint also names its canonical column tuple; uniqueness
  additionally names its key/unique role. Endpoint constraints name the
  endpoint and referenced type. For a
  node overwrite, retained-edge checks additionally name the affected edge
  table and endpoint. Thus a key and two unique groups on one table remain
  separately addressable, including their probe/scan access paths. The
  example assumes the named properties and constraints exist; operators
  present in a real plan follow its accepted schema.
- Source admission remains before payload reads: a source above
  `load_source_max_bytes` raises `csv_source_bytes`. Successfully admitted
  and read inputs preserve the evaluator's semantic refusal taxonomy and
  precedence, as defined once in Design. Stable-source enforcement can
  additionally refuse a source that changes during range reads. Worker
  completion order never chooses the semantic refusal.
- Validation completes before any table fragment write. A validation refusal
  therefore leaves zero staged fragment bytes. Later failures follow the
  publication-outcome rules in Design: an in-doubt publication is not proof
  that the load remained invisible or may be replayed.
- Served CSV loads already reserve the GQ text length plus
  `load_source_max_bytes` in `run_load_statement`
  (`crates/omnigraph-server/src/handlers/dispatch.rs`), before entering the
  engine. Keep this conservative source-cap reservation; this RFC does not
  replace it with a HEAD-sized charge. Controller admission still refuses
  with 429. Execution memory and worker bounds below are additional engine
  obligations; their typed resource refusals are not all workload 429s.
  Hold the served reservation through owned execution settlement.
- The initial plan retains the whole decoded input until validation and any
  permitted reprepare finish. Partitions do not reduce that total. Extend
  the existing `query_memory_bytes` setting, currently read-query-only, to
  one load plan's aggregate tracked memory through the existing executor
  pool. Retained raw source buffers, decoded Arrow allocations, consulted
  keys, validation hash/aggregate state and load staging-adapter buffers all
  consume it. Shared backing allocations are charged once through their last
  borrower. Design defines acquisition and worker ownership; the settlement
  RFC's unimplemented native-resource qualification is not assumed complete.
- `write_max_bytes` additionally bounds each staged-write input. Move the
  current CSV whole-statement typed-byte check to the aggregate pool above;
  keeping that old check would prevent the intended relaxation. Existing
  append/merge keyed-row and retained-keyed-byte limits, removed-ID and Blob
  guards remain additional checks with their existing scopes. A single-scan
  fallback emits bounded batches and can subdivide writes; a row or adapter
  operation that cannot fit its applicable bounds is refused. Today's route
  retains one table's typed input; the 34 GB SF30 dump spans about 33 table
  statements, and CSV source bytes are not decoded Arrow memory.
- GQT load steps may carry plan expectations, so "does this load probe or
  scan" becomes one deterministic case.

## Design

Lowering, not a new surface: the compiler already parses the statement; this
RFC adds a lowering from the parsed statement to a physical plan.

The following attempt procedure owns validation order, retry and publication
outcomes for every load mode. The operator tree exposes these stages; it does
not introduce a second set of semantic rules.

1. **Capture authority and a stable source.** Capture one `WriteTxn` before
   interpreting input. Its catalog, schema identity, target incarnation,
   base and mode govern normalization, validation and staging. Enforce source
   admission before reading payload. Every boundary and data range request
   must name the same object version or enforce the same strong validator.
   A failed precondition or inconsistent source response refuses through the
   existing external-source error category before staging. Response size and
   streamed body bounds remain checked after metadata admission.

   Where version-bound ranges are unavailable, perform one bounded
   whole-object read and freeze that response into an owned immutable buffer
   for all workers. Workers never read independent unversioned ranges of the
   live URI. This preserves the current one-response semantics; it does not
   assert historical immutability of a local file modified during that read.
   Retain the captured raw bytes for modes that permit reprepare, so a new
   attempt never substitutes a later source version. No source-spool format
   is introduced; these buffers are part of the aggregate memory allowance.
2. **Parse the capture without changing error precedence.** Use the existing
   Arrow CSV reader (`datafusion::arrow::csv::ReaderBuilder`,
   `crates/omnigraph/src/loader/csv.rs`). A byte-range boundary must be a row
   boundary under its quoting semantics, including embedded newlines. If a
   candidate boundary cannot be proved safe, the accepted fallback uses one
   scan. That scan still emits bounded batches; a partition is not one
   arbitrarily large Arrow allocation. Header mapping and global row
   positions refer to the same capture at every worker count.

   Parsing, normalization and validation may compute concurrently, but
   refusal selection preserves the sequential route's phase order, the
   order from `constraints_for`, and each constraint's established row/key
   ordering. Intra-input duplicate failures precede committed-holder
   collisions within uniqueness; a later Value violation can precede an
   earlier row's EdgeRi violation. Source positions break ties only where
   the existing check requires them. A later check cannot win while a
   higher-precedence check remains unsettled. Resource and external-source
   failures follow their admission/read contracts, outside this semantic
   ordering promise.
3. **Use accepted access paths and share reads.** Today's uniqueness probes use
   chunks of 8,192 keys, each a filtered scan (`UNIQUE_PROBE_CHUNK_KEYS`). A
   bulk input may favor a shared scan; a small input into a large table may
   favor probes. The accepted plan records its estimates, selected access
   path and the relevant catalog/statistics snapshot. It chooses from
   estimated input size and consulted-table size before payload inspection;
   estimates are not promised row counts. Reuse is scoped to the pinned
   table version, columns and compatible predicates. Deduplicating identical
   metadata/range fetches does not eliminate reads of distinct fragments or
   the differing key columns a check needs. This follows the cost-based
   discipline of RFC 2026-10-08, index facts as planner input.
4. **Validate one logical source and its final image.** All partitions form
   one logical input for validation, before staging coalesces any rows.
   Value, enum, Date and DateTime text guards keep their existing judgment,
   including protection against digits Arrow's native parsers drop. Checks
   against committed state use `CommittedState::load`
   (`crates/omnigraph/src/validate.rs`): append/merge see the pinned base;
   an overwritten table's committed view is empty, while untouched tables
   retain the base. Under that view:
   - **Uniqueness** first aggregates duplicates across the whole source,
     including repeated non-null keys that derive the same ID, preserving
     existing nullable-tuple exemptions. Only afterwards may staging apply
     its source-order coalescing. CSV merge therefore still refuses repeated
     input keys; last-writer-wins staging cannot erase an invalid row. For
     non-key unique groups, a ***semi-join***, which keeps probe rows with a
     build-side match, checks committed holders excluding superseded rows.
     Its output must be empty. Key-derived identity needs no redundant
     committed uniqueness probe; append's exact-ID refusal stays in its
     fenced keyed adapter. Overwrite has no committed holders in its view.
   - **Edge referential integrity** uses an ***anti-join***, which keeps
     probe rows without a build-side match, against the final node image;
     its output must be empty. A CSV edge load has no node delta and resolves
     endpoints against committed nodes. A CSV node overwrite additionally
     computes removed node IDs and checks surviving incident edges in
     untouched committed edge tables. Those retained-edge checks are plan
     operators even when the statement supplies no edge rows.
   - **Cardinality** counts the final edge-ID set: subtract deleted/replaced
     committed IDs, then add the validated, coalesced delta. Check both
     minimum and maximum for the evaluator's affected sources, including
     moved-from sources in append/merge, with its deleted-source exclusions.
     All lookups use the same per-mode view; overwrite does not introduce a
     new scan of the replaced edge table's old sources. Replacing one keyed
     edge must not count it twice.

   Extract shared judgment kernels from the current evaluator, which today
   interleaves checks and fetches in monolithic async functions. Both the
   evaluator and load operators must use the extracted predicates and
   refusal-order rules. Fetch orchestration may differ. Differential tests
   compare outcomes for inputs admitted and read successfully by both routes.
5. **Stage one table result after validation.** No table fragment write
   starts until the validation barrier passes and staging reservations are
   acquired. Today's `stage_write_concurrency` (default 8) runs across tables;
   one CSV table has width one and its current adapter concatenates batches.
   Parallel overwrite therefore requires a new sealed within-table adapter.
   Bounded workers produce uncommitted Lance fragments from admitted batches;
   one coordinator combines their metadata into one `Operation::Overwrite`
   at the captured table base and returns one staged table transaction. It
   never concatenates the complete payload back into one Arrow batch. Lance
   11's `execute_batch` accepts append transactions only and is not this
   overwrite composition mechanism. Fragment sizes follow Lance's target
   sizing, allowing a final partial fragment.

   Append/merge retain the existing fenced `StrictInsert`/`Upsert` keyed
   adapters and their bounds. Plan enablement does not promise parallel
   fragment production for those modes or replace them with raw appends.
   All fragment writers remain inside the sealed storage boundary. The
   existing cross-table width is not a second multiplier on load workers.
6. **Publish through the existing authority gate.** Recheck the complete
   `WriteTxn` through the existing commit gates before committing the staged
   table transaction to one detached result. The graph publisher names that
   result in one branch `__manifest` publication. The new fragment adapter
   adds no graph-publication door. Publisher-internal retries preserving the
   accepted authority remain publisher work; they do not authorize
   retargeting a prepared load.
7. **Reprepare only when the mode and outcome permit it.** Append/merge keep
   the existing bounded outer retry on definitive retryable `ReadSetChanged`.
   Overwrite keeps its strict mismatch refusal without outer replay. Every
   new attempt captures fresh authority and reruns affected validation.
   Reuse typed batches only after proving unchanged interpretation authority:
   accepted schema/catalog shape, target incarnation, column mapping, system
   columns and identity derivation. Otherwise renormalize the retained raw
   capture under the new accepted catalog, charging any raw/typed overlap.
   A missing or replaced target, or unsupported transition, preserves the
   existing typed authority refusal instead of transplanting old batches.
   Prior validation results never survive a changed base; no attempt rereads
   the mutable URI as a shortcut.
8. **Keep uncertain publication distinct from rejection.** A definitive
   pre-publication failure or rejected publication leaves no graph publication
   from that attempt. Existing authority-based rules govern staged-artifact
   retention and reclamation. A lost acknowledgement/readback may instead
   return `manifest_publish_in_doubt`: visibility is then unknown, blind
   replay is forbidden, and durable artifacts retain the existing uncertainty
   guards. A returned error alone never authorizes their collection.

**One executor pool owns tracked load memory.** Extend/refactor the existing
`QueryContext`, `QueryResources` and `WorkMemory` owners for load execution;
this integration is new work. The captured `query_memory_bytes` governs the
aggregate representations listed in User and operational behavior. Each
load-owned allocation reserves before allocation, using a conservative
preallocation charge transferred to its Arrow lease without an uncharged
interval. Charge shared buffers once through the last consumer. Validation
collections, key caches and retained batches use the same pool, so several
individually small partitions can still exhaust the aggregate allowance.

Before the first fragment write, reserve the complete concurrently live
staging-adapter working set in addition to retained input and validation
state. Failed preparation admission returns the existing typed resource
limit before staging. Reducing worker count may reduce staging demand; it
cannot shrink retained input. If necessary work cannot fit, this initial
route refuses the load. Retained input stays charged across reprepare;
charges release only after the last borrower and owned workers settle.
Existing supported validation-operator spill uses its existing scratch owner;
it neither spills the retained source nor establishes a process-wide quota.

This is a bound on named, tracked load representations, not process RSS.
Lance-internal buffers/caches, opaque native I/O and allocator overhead need
the settlement RFC's remaining qualification work (2026-10-01, engine
settlement and resource bounds). Registering a worker does not account for
those resources. This RFC does not claim those broader gates are complete.

**Parallelism lives inside load operators.** The outer DataFusion plan keeps
one output partition, matching the current `QueryContext` and graph-operator
contract. New load operators own bounded parser and fragment-writer children;
this RFC does not widen every graph operator's partition contract. Register
all children, including asynchronous writers, before dispatch through the
shared ownership helpers. One accepted finite worker maximum bounds all
active load children, not separate multiplicative parser/writer widths.
They share the memory allowance and settle before owned execution completes,
including failure or cancellation. Registration alone is not a worker cap;
the load operators enforce the maximum. Step 3 settles its selection policy.

- **Attribution.** The plan's stages cross named seams (the engine's
  instrumented decision points), so measure mode attributes a load's
  requests to scan, validate, write and publish instead of the anonymous
  span before the first seam, which is where all validation traffic lands
  today. The write seam is new alongside the validation seams: fragment
  writes are the largest request class a load produces.

Precedents followed: plans as the inspectable execution artifact, shared
validation judgment, one graph-publication authority and existing resource
owners. The setting-scope extension and internal load workers are explicit
integration work, not facilities the current loader already has.

## Invariants

- **Invariant 4 (a mutation publishes once).** Unchanged, and this RFC's
  publish wording is the invariant's: the load stages all participants and
  publishes once, never per table. Staging is per table; publication is one
  `__manifest` publication per statement.
- **Invariant 2 (one graph-content publication door).** Unchanged: the plan
  route's effects become authoritative through that one publication, and
  per-table publication is never graph publication.
- **Invariant 5 (crash convergence).** Preserved through the existing
  publication-outcome and retention rules in Design. Proven unpublished
  staging and `manifest_publish_in_doubt` are distinct states; uncertainty
  does not authorize replay or reclamation.
- **Invariant 7 (physical acceleration is derived state).** Unchanged: the
  CSR and secondary indexes are still built by `optimize`, and inline index
  build stays out of scope.
- **Invariant 8 (integrity failures are loud).** Unchanged: the plan
  operators raise the same typed refusals the evaluator raises, the
  differential suite holds them equal, and "validation precedes visibility"
  (a local phrasing, not canon's) remains a consequence of invariants 4 and
  8: every check settles before the publication that makes rows readable.
- **Invariant 11 (failures and resource use are bounded and observable).**
  The plan adds aggregate tracked-memory admission and bounded owned workers;
  OCC retries remain bounded and seams expose stage costs. Native-resource
  qualification remains with the settlement RFC.

Two deny-list items are near this design and both are argued, not brushed:
parallel fragment writers stay inside the sealed storage boundary (no raw
Lance writers or public writable `Dataset` handles leave it), and the
probe-or-scan choice is cost-based and plan-visible where the deny-list
rejects cost-blind plan choice. No deny-list item is touched; no invariant
is weakened.

## Compatibility and reversibility

No storage format change: the plan route writes ordinary Lance fragments and
manifest commits, so a tree without it can open the resulting stores. The
statement text is unchanged; load `explain` is an additive grammar surface.

Resource acceptance is deliberately different. With enough aggregate memory,
a 64 MiB typed overwrite in four 16 MiB writes can pass a 32 MiB per-write
limit; the current whole-statement limit refuses it. Reverting to the old
pipeline can therefore refuse statements previously admitted by the plan.
Validation equivalence applies to inputs admitted and read successfully by
both routes. Reversion also removes load `explain` support; callers using it
must account for that grammar difference. The single-scan fallback's bounded
batching and refusal rules are part of the new route, not rollback behavior.

## Alternatives

1. **Do nothing.** The measured gap stands, and bulk import is the first
   operation a migrating user or agent performs.
2. **Optimize the pipeline in place.** A bounded parallel parser and new
   within-table writer could use the same memory and ownership helpers, and
   recover much of the throughput benefit. A typed inspectable description
   could also expose that pipeline's access paths. The selected design makes
   that typed plan the artifact execution consumes, so access-path choices,
   adaptive worker bounds, explanation and expectations share one contract.
   It adds load-specific operators and shared-kernel extraction; it does not
   obtain a pre-existing parallel loader scheduler for free. That integration
   cost buys a shared execution/inspection artifact. Duplicate-read
   suppression still ships independently as step 1.
3. **Defer the checks** (direct-path style: write fast, validate during
   index rebuild). Refused: validation would follow visibility, and a
   governed load must not publish rows it has not checked.
4. **Per-key probes against the store** (classic row-store COPY shape).
   Refused by scale arithmetic: at thirty times SF1's 17,256,038 edge rows,
   the SF30 set holds roughly 500 million edge rows, each checked at two
   endpoints, on the order of a billion key probes, and against an object
   store each probe is a request. Kùzu makes this shape work by probing an
   in-memory primary-key hash index per row during COPY, a structure a
   store-backed engine does not hold; our measured check is already
   batched, the better half of this trade.
5. **An external converter or importer tool.** Parsing moves outside policy
   and audit, and the engine still pays the same validation and write path,
   so the gap remains and governance is weaker.

## Evidence and tests

- Published numbers follow invariant 13 and its owners: cost claims ride
  the checked-in instruments, the end-to-end benchmark (RFC 0039) for the
  SF30 acceptance run and the comparative cost harness (RFC 0031) for
  competitor numbers, each with a persisted run record.
- The eleven scaling cases ship with step 1 into
  `crates/omnigraph-gqt/cases/`. At 1/4/16 consulted fragments, the measured
  request counts are 29/45/117 and repeat-read counts 12/19/55. Even removing
  every counted repeat leaves 17/26/62 requests. Pin suppression of duplicate
  ranges/metadata when reuse is admitted at a recorded cache allowance, and
  exercise eviction/uncached fallback when it is not. Report distinct
  fragment reads separately; these measurements do not promise a flat total
  request count.
- Load plan expectations extend `cases/v2/planner/`: distinguish multiple
  unique groups, retained-edge RI on node overwrite, chosen access paths,
  and an accepted adaptive scan from its actual executed partition count.
- Differential semantic tests include competing phase/constraint errors,
  duplicate CSV keys across partitions before coalescing, nullable unique
  tuples, node overwrite with retained incident edges, replacement at maximum
  cardinality and moved-from minimum cardinality. They compare admitted inputs
  against the shared evaluator in all three modes.
- Authority/source cases cover schema changes requiring renormalization,
  incarnation replacement refusal, append/merge reprepare, strict overwrite
  conflict, version changes between range reads, single-response fallback,
  and publication acknowledgement loss without unsafe replay/reclamation.
- Resource cases cross the aggregate allowance with several individually
  admissible partitions, retain charges for live borrowers/workers, test
  cancellation settlement, and prove preparation refusal starts no fragment
  writer. Exercise write subdivision under single-scan fallback and record
  both new admission and old-route rollback refusal. The SF30 record states
  source, write and execution-memory settings and actual worker maxima.
- Acceptance threshold: an SF30 CsvBasic load on the measured workstation
  in single-digit minutes, the exact bound fixed when rollout step 3 lands;
  competitor numbers on that workstation are recorded above.
- Upstream surfaces surveyed: DuckDB binds `COPY FROM` into a bound plan
  (`src/planner/binder/statement/bind_copy.cpp`, `Binder::BindCopyFrom`);
  Kùzu executes `COPY` as one auto-commit transaction through its operator
  pipelines (`src/main/client_context.cpp:504`). Both get parallel load and
  load explainability from their executors, which is the design this RFC
  adopts.

## Rollout

1. **Deduplicate fetches in the current loader.** Share identical metadata
   and byte-range reads within a statement, leaving its probe shape
   unchanged. Ships alone for every mode; distinct fragment reads may still
   grow with fragmentation. Bound newly retained reuse buffers under
   `query_memory_bytes` through the existing pool/lease owners: reserve before
   retaining, hold through the last borrower, and evict unborrowed cache
   entries or skip caching when the allowance cannot admit them. Keep the
   current source and whole-statement write guards. This is the first setting
   scope extension; update its owning settings documentation and the session
   settings RFC (2026-09-16) here. Step 2 extends this same pool to the entire
   load plan. The eleven cases and before/after request, byte and repeat-read
   measurements, with admitted reuse and uncached fallback, verify this step.
   `implementation` moves to in-progress.
2. **Lower embedded overwrite and add load `explain`.** Execution consumes
   the accepted adaptive plan; unsupported modes report their pipeline route.
   Establish stable-source capture, shared judgment, aggregate tracked-memory
   accounting and retry/publication rules here, before adding parallelism.
   Complete the setting-scope documentation for aggregate load memory.
   Verifying record: load plan expectations,
   differential semantic cases and memory/authority refusal cases green.
   `implementation` moves to partial.
3. **Add bounded internal parser workers and the sealed overwrite fragment
   adapter.** Keep the outer plan single-partition. Choose the finite worker
   maximum policy under the same aggregate allowance and verify single-scan
   fallback, source-version enforcement, fragment composition and worker
   settlement. Verifying record: those regressions plus the SF30 acceptance
   run through the end-to-end benchmark (RFC 0039), with settings and actual
   worker counts recorded, meeting the threshold above.
4. **Enable append and merge plans, attribution and served loads.** Retain
   fenced keyed adapters and their existing mode-specific bounds. Require
   mode-specific plan expectations, validation parity and retry records for
   both modes, including cross-partition duplicate refusal. Add validation
   and write seams, and make GQT load expectations corpus convention. Served
   enablement keeps the current conservative source-cap reservation and
   verifies added execution accounting and settlement. Its records include
   per-stage attribution, controller admission refusal with 429 and distinct
   engine resource refusals. Only when all three modes pass these records
   does `implementation` move to complete.

Each step ships independently.

## Unresolved questions

1. How is the finite load-worker maximum selected, and what is its default?
   A new setting would belong to the session settings RFC (2026-09-16);
   deriving it must not mistake today's outer target-partitions value of one
   for an existing parallel scheduler. Decided by the author in the step-3
   change. One maximum still bounds all active load children.
2. Which supported validation join/aggregate implementations best use the
   admitted memory, and when should they spill through the existing scratch
   owner? Decided in the step-3 change. This cannot bypass the retained-input
   cap or imply a new process-wide scratch guarantee.
3. Does load `explain` reuse query `explain_version` formats or a
   load-specific shape? Decided in the step-2 change; either encoding must
   carry the accepted estimates, access paths, worker maximum, fallback and
   addressable constraint identities defined above.

## Decision log

- 2026-10-15: Created, from the LDBC SNB benchmark's load measurements and
  the GQT one-call load measurements that located the cost in per-call
  re-reads of validated-against tables.
