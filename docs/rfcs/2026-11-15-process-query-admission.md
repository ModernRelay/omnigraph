---
rfc: "2026-11-15-process-query-admission"
title: "Process query admission"
track: public
status: draft
implementation: not-started
authors:
  - QinXi-ai
created: 2026-11-15
updated: 2026-11-15
discussion: https://github.com/ModernRelay/omnigraph/issues/724
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC: Process query admission

## Summary

Give engine execution one process-wide byte budget. An execution reserves its
declared memory envelope before dispatch, keeps its own DataFusion pool, and
returns capacity only after the resources that reservation covers are gone.
When concurrent work holds the remaining capacity, admission refuses immediately
with a retryable outcome. A request whose envelope exceeds the entire budget
receives a request refusal instead.

This specifies the execution-admission part of
[Engine settlement and resource bounds](2026-10-01-engine-settlement-and-resource-bounds.md)
for [#724](https://github.com/ModernRelay/omnigraph/issues/724). It retains that
RFC's ownership and native-settlement requirements. The accounted unit is
reserved execution bytes; deployments also need headroom for caches, outputs,
the allocator and runtime.

## Motivation

Each call to `engine::execute` creates a `QueryContext` from the accepted plan's
`memory_limit`. `QueryContext::with_limits` constructs a new
`SpillHeadroomPool`, so a query's reservation never competes with another
query's reservation. Ordered table walks construct another bounded pool in
`TableStore::execute_bounded_ordered_scan`. These are useful local limits,
but their sum grows with concurrent execution.

The existing server `WorkloadController` limits admitted operations and retained
input bytes. It does not account the execution pools or scanner buffers of those
operations. Admission belongs below the transports: HTTP, MCP, direct CLI and
embedded callers must contend for the same capacity, across graph handles.

The issue reports OOMs from concurrent analytical queries and identifies scanner
read-ahead outside the query pool. The local probes below reproduce independent
pool admission on Omnigraph `6d92b436`, with Lance 13.0.0 and DataFusion 54.0.0.

## User and operational behavior

The host configures one finite `process_query_memory_bytes` limit before the
first execution. The proposed server/direct-CLI startup environment name is
`OMNIGRAPH_PROCESS_QUERY_MEMORY_BYTES`; embedded applications configure the same
engine owner through a typed initialization API. Values are positive integer
bytes. Invalid values fail initialization. The limit is immutable while the
process serves work, and a second incompatible configuration is refused.

The first release makes configuration explicit: an unset value preserves the
existing admission behavior. A configured process applies the budget to every
covered engine entry point. Documentation shows the setting alongside cache
and output limits, as a separate deployment allowance. Choosing a default for
all installations is a later release decision supported by workload evidence.

| Condition | Outcome |
|---|---|
| The full envelope fits in available capacity | Reserve it atomically, then dispatch. |
| The envelope fits alone, but concurrent work holds capacity | HTTP 429, `too_many_requests`, `effects: none`, `action: retry`. |
| The envelope is larger than the configured total | HTTP 413, `payload_too_large`, `effects: none`, `action: fix`. |
| A query exhausts its admitted private pool | Keep the existing `query_memory_bytes` refusal. |
| A caller disconnects or abandons its future | Keep the charge with the work and resources it covered. |

Both admission refusals carry `resource_limit` with resource
`process_query_memory_bytes` and the configured `limit`. For temporary occupancy,
`actual` is the attempted aggregate; for an oversized request, it is the requested
envelope. The typed engine refusal retains the requested envelope and held
capacity, so transports distinguish the two conditions without parsing text.
These use the existing error codes: the registry already assigns admission
exhaustion to 429 and a request's work exceeding its bound to 413. HTTP keeps the
existing `Retry-After: 60` admission policy; the CLI keeps its retry-exit
convention and MCP its existing error projection. No work is queued and no retry
is performed by the engine.

Admission does not lower an already accepted plan's `memory_limit` to whatever
capacity happens to be free. The same plan retains its pool and spill decisions;
the host either admits that execution or refuses it. A caller can explicitly
select a smaller query limit and plan again.

The reservation is conservative: queries with the same declared limit reserve
the same pool allowance even when their actual use differs. This trades
concurrency for a predictable admitted envelope. Mixed-workload evaluation must
include small reads with their normal session limits, not only specially tuned
small-query limits.

## Design

### One owner, separate query pools

The engine owns one process-scoped admission state, shared by all roots,
branches, graph handles and sessions. It records the configured limit, held
envelopes, active execution count and refusals. Configuration and first-use
admission share one initialization boundary, so late configuration cannot omit
already running work. Individual graphs cannot create independent budgets.

Admission performs checked arithmetic and reserves the complete envelope in one
synchronous critical section. It never waits for another execution or holds its
lock during allocation, I/O or user code. Refused admission leaves the state
unchanged. The reservation is a byte permit, not another DataFusion memory pool.

Each admitted query keeps its existing `SpillHeadroomPool`, spill headroom and
scratch manager. One query exhausting its private pool does not take memory
another admitted query was promised. Optional work and nested scans consume the
parent's declared allowances; independent contexts reserve additional capacity.
A child cannot turn one parent's reservation into several full-size pools.

### Envelope and entry points

An envelope states the maximum simultaneous execution allowances, with an owner
for each component. The plan's memory limit is one component. Additional
concurrent native contexts, encoded I/O and decoded read-ahead need explicit
allowances before dispatch; their nominal settings alone are not byte proofs.
Shared allocations are charged once through their existing owner.

| Current path | Integration needed |
|---|---|
| `engine::execute`, including inspected execution and plan replay | Admit the accepted plan once before constructing its execution context; keep the reservation across search passes. |
| `LoadMemory` and `execute_load` | Own one envelope across a statement's validation, reuse caches, staging and reruns. A nested read uses a declared suballowance. |
| `TableStore::execute_bounded_ordered_scan` | Admit its pool and native read allowance before plan execution; retain ownership with the returned stream and its actual children. |
| Ordinary, ranked, edge and dependent scanners | Set read-ahead explicitly and establish the lifetime and size of the native allowance before counting the path as covered. |
| Hydration and managed Blob reads | Reuse their existing explicit I/O reservations and bounded batch owners rather than charge their configured maxima a second time. |
| Export | Charge execution here; keep response buffers under their existing owner. A shared buffer crossing the two boundaries transfers its charge. |

The initial implementation inventories all pool and scanner constructors used
by these paths. Paths still outside an enforceable envelope remain documented
coverage gaps; configuring this budget must not silently claim their memory is
included. A complete #724 implementation closes those gaps through the existing
owners or a qualified public substrate hook, without vendoring Lance.

### Lifetime

A permit follows the resources, not the request future. The execution root,
registered producers, blocking workers, runtime/pool owners and covered native
children retain shared ownership of its one charge. Their captured values drop
before their lease announces completion. Abandoning a join handle, a panic or a
timeout never refunds resources still held by that child.

`QueryContext::run_owned` already closes graph-worker registration and waits for
registered descendants on success, error and panic. Embedded cancellation only
closes registration through `Drop`; live children remain. Attach admission to
that existing ownership tree instead of introducing another settlement counter.
The observing `QueryWorkSettlement` handle must not itself keep capacity held
after all actual resource owners are gone.

Opaque DataFusion tasks and Lance I/O have a different lifetime from that graph
worker tree. A stream wrapper that merely refunds on `Drop` is insufficient for
those children. Their integration needs an actual retaining owner or the public
native hooks required by the settlement RFC. Likewise, a returned result can
outlive execution: its retained buffers transfer to output ownership before the
execution permit is refunded.

`QueryResult` lives in the compiler crate and exposes cloned batches and
`into_batches`. A permit stored only on the result object would disappear while
legal Arrow batch clones still retain its data. Output accounting must follow
those buffer owners or an explicit, bounded ownership transfer; changing the
outer result's destructor alone does not establish that lifetime.

### Observation

Expose a consistent engine snapshot of configured bytes, reserved bytes, active
executions and admission refusals through the existing diagnostics/tracing
surface. Byte and execution counters describe reservations; RSS is recorded
separately by the benchmark instrument. A refused execution starts no scanner.
Status and health requests do not require an ordinary execution permit.

## Substrate alignment

Lance retains its dataset sessions, caches, scanners and execution machinery;
DataFusion retains query pools and spilling. The relevant upstream guides are
[performance](https://lance.org/guide/performance/),
[read/write](https://lance.org/guide/read_and_write/),
[object stores](https://lance.org/guide/object_store/),
[observability](https://lance.org/guide/observability/) and
[DataFusion integration](https://lance.org/integrations/datafusion/).

Source qualification uses the pinned releases, not capabilities described only
by the live guides. In particular, Lance's I/O buffer is a soft bound, decoded
batches leave its accounting when delivered, and a dropped receiver can leave
native work alive. An allowance must account for those behaviors and the
simultaneous contexts in the selected path. The settlement RFC already records
native read/CPU lifetime probes; extend them for Lance 13 rather than replace
them with a timer or a count of returned futures.

The pinned crate sources expose three concrete integration boundaries:

- [`Scanner::try_into_stream`](https://docs.rs/crate/lance/13.0.0/source/src/dataset/scanner.rs)
  passes default `LanceExecutionOptions` to `execute_plan`. A caller's graph
  pool does not automatically become the pool of that independent execution.
- [`ScanScheduler`](https://docs.rs/crate/lance-io/13.0.0/source/src/scheduler.rs)
  closes its queue on drop, but its I/O loop drains accepted requests. Its
  response owner refunds local backpressure on final response drop. A graph
  permit needs to follow that actual work, not only its waiting future.
- [`spawn_cpu`](https://docs.rs/crate/lance-core/13.0.0/source/src/utils/tokio.rs)
  submits a closure to a separate blocking runtime. Dropping the returned future
  leaves a started closure and its captured input alive. The existing
  `dropped_native_cpu_futures_retain_each_captured_input_until_completion` and
  `dropped_scan_schedulers_retain_independent_native_read_budgets` guards own
  these two lifetime cases.

DataFusion 54's
[`MemoryPool::grow`](https://docs.rs/crate/datafusion-execution/54.0.0/source/src/memory_pool/mod.rs)
must succeed; only `try_grow` may refuse. The current graph pool forwards
infallible growth to its inner pool. Qualification must therefore include
consumers' infallible allocations and batch overshoot, as well as successful
`try_grow` reservations. Reserving a pool's nominal limit alone does not cover
that additional demand.

## Invariants and compatibility

The design implements invariant 11's bounded, observable resources through
existing owners, as required by invariants 1 and 12. Refusal precedes execution
dispatch. A load or mutation must obtain every additional execution allowance
it needs before that attempt starts table effects; already-admitted completion
never waits for an ordinary permit held by its own parent.

It changes no graph format, snapshot authority or publication protocol. Existing
per-query limits remain part of the accepted plan. Configured admission adds a
new resource name under the existing HTTP/CLI/MCP error codes. The affected
OpenAPI responses, troubleshooting guide and shipped agent skill change
together. The current strict `ErrorCode` deserializer keeps accepting these
known codes. Disabling
configuration on restart restores the earlier admission behavior without a data
migration.

## Alternatives

- A fixed query-count semaphore treats a metadata count and a large join alike
  and does not compose with different declared memory limits.
- One shared DataFusion pool makes already-admitted queries compete during
  execution and still leaves native buffers and other pool owners unaccounted.
- Reducing the per-query default reduces each term but leaves the concurrent
  sum unrestricted.
- Clamping a bound plan to currently free memory changes its declared execution
  assumptions. Admission refusal preserves the plan and lets the caller replan.
- A waiting queue adds retained requests, scheduling and cancellation ownership
  before this resource contract exists. Immediate refusal follows the accepted
  settlement RFC.
- Container limits remain useful for deployment containment. Execution admission
  supplies the typed outcome before concurrent reservations exhaust that limit.

## Evidence and tests

### Current behavior

Measured on Windows x86_64 with Rust 1.98.1, against the engine source at
`6d92b436`. The seven existing `engine::context::tests` pass before adding the
two [reservation probes](https://github.com/QinXi-ai/omnigraph/commit/dc224eec8a3dc52ed7973917e51482f8cb23ad2d).
Both probes pass and release their reservations to zero:

| Probe | Observation |
|---|---|
| Three real `QueryContext` instances, each with a 32 MiB limit | All three reserve their full private allowance: 100,663,296 bytes combined. |
| Direct infallible pool API, 1,024-byte nominal limit | `grow(2_048)` succeeds and the pool records 2,048 reserved bytes. |

The two existing native lifetime guards named above also pass on the same
source: captured CPU inputs and accepted native reads remain owned after their
callers disappear, then release after the actual work finishes. These instruments
measure reservation counters and retained owners. Mixed-workload RSS measurements
belong to the implementation evidence below.

Commands, with the probe commit checked out for the second command:

```bash
cargo test -p omnigraph-engine --lib engine::context::tests --locked
cargo test -p omnigraph-engine --lib issue_724 --locked -- --nocapture
cargo test -p omnigraph-engine --test lance_surface_guards --locked -- --exact dropped_native_cpu_futures_retain_each_captured_input_until_completion dropped_scan_schedulers_retain_independent_native_read_budgets
```

### Implementation evidence

Extend the existing owners in [Testing](../dev/testing.md):

- `engine::context::tests` for independent query pools, exact admission,
  concurrent contention, constructor failure and release after the final owner.
  Use barriers and held resources to exercise success, error, unwind and abandoned
  callers; repeat with a live descendant and an observer-only settlement handle.
- `integration/engine_v2_memory.rs` for two graph handles sharing capacity,
  inspected/replayed queries, a smaller query admitted while a larger one is
  refused, and unchanged logical results and graph state after refusal.
- Ordered-cursor, loader and query-memory owners for nested allowances, search
  passes, load reruns and pre-effect refusal. Extend native lifetime guards for
  retained I/O and CPU work after a dropped stream.
- Server `data_routes`, `multi_graph`, `mcp` and boot-setting owners for the
  retryable/permanent distinction, health during saturation, disconnects and
  startup configuration. CLI outcome and parity owners cover retry exits.
- Existing benchmark owners for a mixed small-read/scan/join workload at several
  concurrency levels. Record offered, admitted, refused and completed work,
  latency, RSS, reserved bytes, scratch and I/O. Timing remains report-only.

The core admission assertion is `reserved <= configured` at every transition,
with no scanner dispatch for a refused attempt. A test that releases the permit
at the outer future's drop must fail while a covered child still owns resources.
Native path coverage and output ownership are tested separately from the pool
counter, so a passing counter test cannot hide an uncovered allocation.

## Rollout

1. Establish the current independent-pool and native-lifetime reproductions in
   the existing owners, and measure the scanner paths against the pinned source.
2. Add the process owner and query-context reservation, with explicit startup
   configuration and engine-level lifetime tests.
3. Integrate standalone ordered walks, nested/load execution, native allowances
   and retained outputs under the same owner; extend transport projection and
   mixed-workload evidence.
4. Document the supported execution envelope and operator headroom. Revisit an
   enabled-by-default server budget from those measurements.

## Unresolved questions

- Which public Lance 13 owners can retain admission through each native read and
  CPU tail, and which paths require an upstream hook?
- What qualified native allowance can each scanner path enforce, including an
  oversized encoded page and concurrent decoding?
- Which existing output owner can accept the last execution-buffer charge for
  direct embedded results as well as HTTP/MCP responses?

## Decision log

- 2026-11-15: Proposed process-wide envelope admission with private query pools,
  immediate capacity refusal and resource-owned permit lifetimes for #724.
