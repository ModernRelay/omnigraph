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

The issue's October 18 reproduction reports three contexts each retaining
32 MiB, with no shared refusal; its constrained process was OOM-killed. Its
November 10 follow-up identifies scanner read-ahead outside the query pool.
Those observations are reported evidence. The source inventory below is against
Omnigraph `6d92b436`, with Lance 13.0.0 and DataFusion 54.0.0.

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
| The envelope fits alone, but concurrent work holds capacity | HTTP 503, `execution_capacity_exceeded`, `effects: none`, `action: retry`. |
| The envelope is larger than the configured total | HTTP 400, `bad_request`, `effects: none`, `action: fix`. |
| A query exhausts its admitted private pool | Keep the existing `query_memory_bytes` refusal. |
| A caller disconnects or abandons its future | Keep the charge with the work and resources it covered. |

Both admission refusals carry `resource_limit` with resource
`process_query_memory_bytes`, the configured `limit`, and the attempted aggregate
`actual`. A retryable refusal also retains the execution's requested envelope
in the engine error, so transports can distinguish temporary occupancy from an
oversized request without parsing text. No work is queued and no retry is
performed by the engine. Existing CLI retry-exit conventions and MCP error
projection apply to the new code.

Admission does not lower an already accepted plan's `memory_limit` to whatever
capacity happens to be free. The same plan retains its pool and spill decisions;
the host either admits that execution or refuses it. A caller can explicitly
select a smaller query limit and plan again.

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

## Invariants and compatibility

The design implements invariant 11's bounded, observable resources through
existing owners, as required by invariants 1 and 12. Refusal precedes execution
dispatch. A load or mutation must obtain every additional execution allowance
it needs before that attempt starts table effects; already-admitted completion
never waits for an ordinary permit held by its own parent.

It changes no graph format, snapshot authority or publication protocol. Existing
per-query limits remain part of the accepted plan. Configured admission adds a
temporary-capacity error to HTTP/CLI/MCP; the new API error registry entry,
OpenAPI responses, troubleshooting guide and shipped agent skill change
together. Old clients can still read the structured error body. Disabling
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
