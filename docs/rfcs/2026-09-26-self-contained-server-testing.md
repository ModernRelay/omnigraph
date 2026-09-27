---
rfc: "2026-09-26-self-contained-server-testing"
title: "Self-contained server testing with GQT and DST"
track: maintainer
status: draft
implementation: not-started
authors:
  - azimafroozeh
created: 2026-09-26
updated: 2026-09-26
discussion: null
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC: Self-contained server testing with GQT and DST

## Summary

Extend ***GQT***, the declarative graph-query test runner, to provision and test
a local server automatically. Existing query and mutation cases will run
through HTTP with their existing expectations once the HTTP read response
carries the executed result column types that `--- expect shape` compares;
until then a rows step under a server route fails preflight with
`unsupported_capability: result_types`, after route admission. Add sequential lifecycle
steps on the real process: kill or restart the server process, request
shutdown and a physical assertion after containment. Directed concurrency
(named clients, holds, joins) is a separate later RFC amending RFC 0045; rows
that need it are listed here with that prerequisite. Authors will not configure
a connection profile or start a server.

***Deterministic simulation testing (DST)*** runs the system under controlled
scheduling, clocks, randomness and I/O. Running the server deterministically
under the shared DST environment is a separate proposal, whose title is
Deterministic server execution; the coverage rows below name its scope as D.
Ordinary server tests supply real socket and process evidence;
`omnigraph-bench` supplies real performance measurements. The engine remains
independently testable. This proposal defines testing architecture and
qualification, not new production recovery or deployment guarantees.

## Motivation

A dropped request future does not prove that storage work it started has
stopped. A shutdown can observe a returned error while accepted storage work is still
pending. Testing either condition only through the engine misses the server's
admission, authorization, body ownership and shutdown behavior.

The current [GQT admission code](../../crates/omnigraph-gqt/src/runner_config.rs)
still parses a declared `target` field, recognizes both server names there
and admits only the ordinary engine route on local filesystem storage without
seams and engine-DST on in-memory storage when built with `tokio_unstable`;
RFC 0045's route derivation replaces that field with the API each step names. Its
[step loop](../../crates/omnigraph-gqt/src/lib.rs) awaits each authored step through
one session. Overlapping operations need runner scheduling and attribution
support beyond parser forms; that work is deferred to a separate RFC.

The [server launcher](../../crates/omnigraph-server/src/lib.rs) combines real
listener setup, signal handling and an OS shutdown watchdog. The shared
[DST executor](../../crates/omnigraph-dst/src/environment.rs) already separates
environment setup from running the scenario and leaves process containment to
its caller. A second server implementation or a separate
scenario runner would make server and engine tests diverge.

The intended product-test interfaces are GQT, GQT under DST and
`omnigraph-bench`. Rust still implements their adapters, controls, generators
and assertions, and tests those mechanisms directly. Existing Rust regressions
remain until a replacement detects the same defect. These interfaces do not
make every scenario expressible immediately or prove exhaustive bug coverage.

## User and operational behavior

> A term set in bold italics is being defined at that exact spot.

### Definitions

GQT and DST are defined in the Summary; the terms below are new to this RFC.

- An ***execution*** is one selected case, environment, seed where applicable,
  and replay attempt, with exclusively owned test resources.
- An ***operation*** is one authored invocation. It can own a request, engine
  work and response production with different completion times.
- ***Settlement*** means all work in the declared ownership scope has become
  quiescent, so no accepted task or I/O in that scope can cause a later effect.
- An ***oracle*** is an independent check of an expected result or invariant,
  rather than another reading of the implementation's own status field.
- A ***capability*** is a specific route, API, operation, control or observation
  whose implementation and evidence are qualified for the selected environment.
- A ***storage decoration*** is the wrapper each storage realm puts around its
  object-store calls: adapter `FailingStorage` or Lance `LanceFaultDecorator`
  (RFC 0066 §Design, Store places).
- The runner is the GQT process that executes cases; the supervisor is its
  role that owns worker, server and helper processes.
- A ***retained-version query*** is a query bound to an earlier graph version
  that the server still retains, rather than to the current head.

### An ordinary case

This proposed server case retains the existing GQT sections and comparisons:

```text
# issue: none
--- runner
timeout_ms: 10000
environments:
  - storage: local-filesystem

--- schema
node Person {
    name: String @key
}

--- seed
{"type":"Person","data":{"name":"alice"}}

--- query via http
query names() {
    match { $p: Person }
    return { $p.name }
}

--- expect unordered
{"p.name":"alice"}

--- expect shape
p.name: String
```

The step's `via http` derives the `omnigraph-server` route (RFC 0045
§Execution routes): the runner creates the graph and cluster configuration,
starts an owned server on loopback, verifies readiness and sends the query
through HTTP. It compares executed types and rows, then settles and contains
the execution before deleting its resources. Provisioning and readiness count
against the `timeout_ms` file budget, which starts before case reading (RFC 0045
§Explicit execution environments). A file mixing `via http`, `via mcp` and `--- cli` steps runs them
all against that one server process, and a `--- query` naming several server APIs
compares each against the same expectation.

Adding `seeds` to a file whose server steps are `via http` or `via mcp` only
derives `omnigraph-server-dst`, specified by
the Deterministic server execution proposal, which lands separately. No
profile or external URL is needed for the local route. Unsupported route and
storage combinations continue to fail admission until their rollout gate
passes.

### Deferred: directed concurrency

This RFC defers directed concurrency: named clients, the Start, Hold, Wait,
Release and Join controls, the operation registry, identities beyond the
server epoch, retained-write ownership, a control channel into a running
server process, relational receipt assertions and per-client principals. That
work will be specified in its own RFC amending RFC 0045. Coverage rows in this
RFC that need overlapping work carry the prerequisite "Concurrent controls
(separate RFC amending RFC 0045)". The sequential lifecycle steps on the real
process (process kill or restart, request shutdown and physical assertion after
containment) are in scope here. Modeled server restart and virtual-time advance
belong to the Deterministic server execution proposal, which also lands
separately.

## Design

### Ownership and execution boundaries

| Owner | Responsibility |
|---|---|
| `omnigraph-gqt` runner (`src/lib.rs`) | Case parsing, server epoch identities, comparisons, capability admission and terminal reporting |
| `omnigraph-gqt` supervisor (`src/dst_runner.rs`) | Immutable input, worker/server/helper lifetime, real deadlines, replay and containment |
| `omnigraph-gqt` client adapters, one per API (`engine`, `http`, `cli`; `mcp` once an MCP tool executes an authored GQ read) | Dispatch of each step by its header's API against the one owned server; the physical read after containment |
| CLI/server process support (`crates/omnigraph-cli/tests/support/mod.rs`: `converged_loaded_cluster`, `spawn_server_with_cluster`, `TestServer`) | Local cluster provisioning, production connection adapter, readiness and lifecycle controls |
| `omnigraph-dst` `run_universe` | One controlled scheduling, time, entropy and storage environment; its server consumer is owned by the Deterministic server execution proposal |
| Engine test support (`crates/omnigraph/tests/helpers/`) | Real engine calls, storage controls and physical-state oracles |
| `omnigraph-server` route suites (`crates/omnigraph-server/tests/`), owner of the scripted engine adapter | Bounded legal-trace catalog, trace-to-composition pairing, component-only reports |
| `omnigraph-bench` | Arrival schedules, measured windows, repetitions and measurement records |

Reuse the existing owners in [the test map](../dev/testing.md). This design
adds no durable graph authority, background job service or test-control HTTP
endpoint. This RFC's lifecycle steps are supervisor actions on the process
(signal, kill, spawn), so no channel into the server process is needed.

| Scope | Execution | Evidence supplied |
|---|---|---|
| Ordinary server | Real server process, HTTP client, kernel sockets and real engine | Wire behavior, actual disconnect, signals, watchdog exit and durable process restart |
| Server-DST | Owned by the Deterministic server execution proposal (separate proposal) | Listed for the coverage mapping only |
| Server component | Production transitions against a narrow scripted engine adapter | Server logic over that adapter's qualified traces |
| Engine DST | Existing engine workloads and controlled environment, without server code | Engine results, physical state and storage behavior |

The server calls the engine in process. Independent testing does not introduce
a network service between them. Engine tests use a workload driver; server
component tests use the same declared operation contract as composition.

### Request path per API

Every query, mutate and CLI step names its API in its header, the path it takes from the case file
to the engine and back; each environment's execution route is derived from
the set of APIs the file names and that environment's `seeds` (RFC 0045
§Execution routes). Evidence is worth exactly
that path.

| Step header | Where the step goes under the `omnigraph-server` route |
|---|---|
| `via http` | The runner's real HTTP client, kernel TCP on loopback, the real server process and its listener, `axum::serve`, router, authorization, handler, the embedded engine, and the response back over the same socket. |
| `via mcp` | The server's MCP surface at `/mcp` (`crates/omnigraph-server/src/mcp.rs`, streamable HTTP, mounted only when an OIDC resource is configured). Its shipped tools invoke stored reads only, so `via mcp` is refused before fixture effects as `unsupported_capability: mcp` until a tool that executes an authored GQ read exists ([RFC 0003](0003-mcp-server-surface.md) or its amendment); `via mcp` is admitted on `--- query` only. |
| `--- cli` | The `omnigraph` binary spawned by the runner in served mode against the same listener, so the CLI's own HTTP client crosses the same socket path; its evidence is exit code, stdout and stderr. |
| `via engine` | On a `--- query`, a read-only `Omnigraph` open of the owned root, admitted only after a lifecycle step has contained the serving process: the physical read. `--- mutate via engine` in such a file is refused as `invalid_case`. |

Under the `omnigraph-engine` route (`via engine` only, no server) the runner
calls `Omnigraph` directly in its own process. A path that enters at
`Router::oneshot`, as the existing server route suites do, skips the
connection loop and the HTTP codec. No header selects it; it is scope C
component evidence. The two DST routes are composed component by component
in the Deterministic server execution proposal.

### Transport facts

Initial qualification is HTTP/1. The server declares no HTTP/2 support: axum's
`http2` feature is off and any HTTP/2 acceptance today is hyper-util feature
unification through a transitive dependency, not a workspace choice. Before an
HTTP/2 control is admitted the server
either pins `http1_only()` (HTTP/2 controls refused permanently) or enables
axum's `http2` feature as a declared, tested capability and qualifies cleartext
prior-knowledge HTTP/2. Real socket reset remains ordinary process evidence. A
dropped result receiver is a distinct action from closing the client transport.
TLS handshakes and remote services are outside this RFC. Actual CLI/proxy
scenarios retain those binaries and processes.

### Engine outcomes and ownership

The real adapter invokes the production engine operation once and forwards its
actual outcome. The runner never retries a write implicitly. Production engine
or storage retries retain their own recorded configuration and attribution.

| Observation | Meaning | Separate obligation |
|---|---|---|
| Result available | A value, error or contained panic ended the call | Accepted work may still be pending |
| Delivery terminated | A response completed, was abandoned or was lost | Determine result and effects independently |
| Cancellation requested | A supported owner received a cancellation request | Prove what actually stopped; infer no rollback |
| Settlement disposition | Scoped tasks, producers and I/O are proved quiescent or explicitly unresolved | An unresolved disposition is not settlement proof or retry permission |

A scripted engine adapter is optional for component tests. It supports only a
bounded catalog of legal traces, each paired with a real-engine composition
case. It cannot invent graph contents, recovery guarantees or settlement proof.
A component pass never replaces physical, crash or reconstruction evidence.
If production cannot expose or preserve a required ownership boundary, the
product capability remains a prerequisite; the adapter cannot supply it.

### Provisioning, limits and containment

Static input and capability validation happens before fixture effects. The
runner then guards partial acquisitions while creating the minimal cluster
configuration, graphs and local credentials required to boot. Before scenario
dispatch, readiness compares `/readyz`'s
`booted_serving_digest` and `state_revision` with the applied revision the
runner wrote, digests the executable it spawned, and reads the backend from the
runner-owned graph configuration; `/readyz` does not report the backend. Setup
failure supplies no executed product coverage. A server epoch is identified by
the server alias and a boot ordinal; restart steps create a new epoch and the
report records both.

The outer supervisor owns every worker, server and helper from spawn. A retained
handle and containment domain cover the spawn-to-registration interval;
registration completes before waiting for readiness. Worker-requested launches
use the same owner or a handshake that leaves the process contained on failure.
Enumerating PIDs after a failure is not proof of ownership. The GQT supervisor
adopts the bench supervisor's process-group containment: each worker is a
process-group leader, servers and helpers join that group, containment signals
the group and `process_group_is_gone` is the reap proof. The supervisor spawns
the server as its own direct child placed in the worker's group: a kill or
restart step signals that process
and reaps it, and the proof is the reaped pid plus no surviving descendant of
it, while the worker keeps running. The whole process-group proof stays for
final teardown only. A plain `Child::kill`
does not reach grandchildren and is not containment.

Admission requires finite bounds for processes, transport buffers and
diagnostic output. The implementation records
effective bounds in the input and exercises each overflow/refusal path. Limits
do not invent measured performance targets or silently truncate evidence.
Per-case bounds compose with the runner's case-concurrency limit.

The supervisor preserves the primary result, stops new authored dispatch on
harness failure and attempts bounded settlement under the existing wall-time
budget. Expected operation errors remain scenario results. At cutoff, the
separate existing containment allowance covers termination and reaping of all
owned processes. New descendants do not reset that deadline.

Storage cleanup is a separate decision. Delete only an exclusively owned
namespace with backend-qualified quiescence. Reaping a process alone cannot
prove external I/O settlement. If effects or ownership remain unproved, preserve
the namespace and evidence, report cleanup failure and prohibit its reuse.
The initial route uses local filesystem storage; an external backend requires
its own containment qualification.

Apply RFC 0045's continuation rules unchanged: completed assertion failures can
receive fresh replay and later seeds while isolation and deadline permit; an
incomplete worker report stops that environment; other environments run only
after confirmed containment. Deadline exhaustion or uncontained cleanup stops
all dispatch. Suppressed executions, including mandatory replay, remain
`not_run` with a cause. Containment failure never turns into a passing case.

### Assertion authority and current recovery

Typed receipts, lifecycle status and resource counters are observations under
test. Expected values come from the authored operation history, independent
storage acceptance/completion records and narrowly scoped physical-state checks.
Memory assertions need a test-side allocation/lifetime observer, including
shared-allocation deduplication, separate from the budget counters being tested.
RSS or another endpoint exposing the same counter cannot prove exact accounting.

State verification is read-only or runs after containment. It must not open a
second writable engine beside a serving process. Production state observations
cannot themselves be treated as proof of historical availability or settlement.

Current [recovery](../dev/recovery.md) uses final detached table pins. A table
effect is complete when the target branch's `__manifest` publishes it; no table
promotion follows. Published-but-uninstalled schema contracts remain a separate
completion case. Unpublished detached artifacts may remain unreachable until
the collector proves reclamation safe. Older promotion behavior belongs only
in explicit versioned compatibility cases, not the active server controls.

## Invariants

This proposal preserves [the architectural invariants](../dev/invariants.md):

- One graph publication remains the visibility authority. Test reports are
  derived execution evidence, never graph authority.
- Each operation uses its accepted view.
- Historical serving tests require exact data, schema, incarnation and query
  bindings under current authorization.
- Crash convergence stays in the commit protocol (invariant 5): a published pin
  is complete at publication; cases assert no reconciler, no promotion and
  collector safety, never a harness-side finish.
- Cancellation, unknown delivery and process death grant no retry permission.
  Settlement requires evidence about accepted work.
- Controls stay within test-owned execution. Authorization is exercised through
  the production boundary, and no client supplies a trusted actor identity.
- Bounded resource use includes disconnected operations, producers, cleanup
  and diagnostic collection. Unknown ownership cannot become free space.
  Quarantined resources remain charged and prevent unbounded acquisition.
- Evidence matches its boundary. Storage faults, HTTP disconnects, real death
  and performance measurements retain their distinct qualification requirements.
- State verification never opens a second writable engine beside a serving
  process because the one-mutation-process boundary is process-local.

No deny-list exception is requested.
Lance's publication, cleanup and compatibility rules remain with their existing
engine owners. No Lance algorithm, storage format or dependency change is proposed.

## Compatibility and reversibility

Existing sequential cases and `scope: next_step` seam directives keep their
meaning.
`--- restart` continues to close and reopen a graph handle; process restart is a
separately named step; modeled server restart belongs to the Deterministic
server execution proposal. The `omnigraph-server` route refuses `--- restart`
as `unsupported_capability: restart` until a same-process handle-reopen control is qualified as a sequential
lifecycle step; corpus cases that use `--- restart` stay engine-only until
then, and T4.progress's reopen carries scope E + D.

The local server combination requires no connection profile and cannot attach
to an ambient service. Existing external-backend profile proposals and
qualification work remain separate. This RFC introduces no remote testing mode.
It changes no production endpoint or wire behavior except the additive
result-type witness of the Local HTTP phase. It changes no graph format or
compatibility fence.

A case whose derived route the build's admission table does not admit fails;
disabling a route through admission changes no engine behavior, and
that route's cases move to a directory outside `crates/omnigraph-gqt/cases/`,
run only by explicit file execution, rather than being deleted. Keep
the previous qualified tests until
equivalent defect detection is demonstrated. Accepted engine
simulation claims are not widened by a step naming a server API.

## Alternatives

| Alternative | Concrete limitation and decision |
|---|---|
| Keep only Rust server suites | Retains useful evidence but repeats scenario setup and does not provide the common authored test interface. Reuse their mechanisms and migrate only after equivalence. |
| Point GQT at a manually started local server (the S3/Azure profile remains for external backends) | Cannot guarantee fresh configuration, exclusive state or teardown. Automatic local provisioning is the chosen default. |
| Invoke only the router as server evidence | Skips HTTP framing, connection backpressure and protocol cancellation. Useful component evidence only; the existing route suites stay the owner of scope C. |
| Replace the engine by default | Can pass a drain test while real storage work survives. Use the real engine by default; qualify narrow component traces separately. |
| Infer completion from the returned result | A contained panic can leave accepted storage work pending. Retain independent ownership and settlement observations. |
| Rely on Rust guards for process cleanup | A hard-killed worker cannot run them and may leave a server/helper alive. Supervisor ownership from spawn covers that interval. |

What each decision forces:

- No test-control HTTP endpoint: lifecycle steps are supervisor actions on the
  process, any in-process control waits for the concurrency RFC, and no feature-gated server
  build.

The nearest precedents are GQT's immutable worker input and reports, the
existing seam catalog and server/CLI lifecycle support. This proposal extends
those patterns.

## Evidence and tests

### Qualification rule

A required assertion qualifies only when its capability is admitted, its case
executes, every required step effect is observed, its independent oracle passes,
fresh replay matches where the environment requires it (RFC 0045 §Seeds and
reproducibility; for server-DST, the Deterministic server execution
proposal), and containment/cleanup are qualified. Missing
capabilities, zero execution, absent events, unresolved cleanup or missing
reports leave that assertion unqualified. A passing capability-refusal test
contributes only harness evidence, never positive product coverage.
A settlement claim requires proof that no accepted task or I/O in the declared
ownership scope can cause a later effect. A missing executed-type witness is a
refused capability; response types are never inferred from the query or its
expected rows. Cleanup failure never becomes a passing case. Confirmed containment
covers surviving processes after an attempt; supervisor or machine failure is
outside that guarantee. An in-memory client cannot stand in for CLI behavior.

The evidence record binds exact case/workload bytes, route, each step's API, build and backend,
epoch identities, actual protocol, results, publication identities, pending
work, the production idempotency key (or its digest) when the operation
supplies one, and separate primary and cleanup outcomes. Epoch is inapplicable
for the engine routes. Independent oracles need a deliberate bad-result test:
wrong receipt, early permit release or missing history must turn the corresponding assertion red. Sensitivity
failures inject the wrong value on the actual side through the production path,
never by editing the expected value or the oracle's read.

### Harness qualification

| Owner to extend | Required evidence |
|---|---|
| GQT format and dispatch tests | Unsupported routes and APIs, a header without `via`, `--- cli` or a server API under `seeds` where refused, `via engine` before containment in a server file, zero selections, immutable input and report failure |
| Server and CLI support | Partial boot, configuration mismatch, actual request census, response truncation, HTTP/1 close, HTTP/2 reset once HTTP/2 is a declared server capability (product prerequisite), signals and same-root process restart |
| Engine/storage owners | Final pins, staged schema completion, exact published state and collector-safe retained history |
| Supervisor | Failure before readiness, worker/helper death, hung executor, descendant containment, quarantine, exhausted deadlines and preserved primary failure |
| Independent assertions | Sensitivity to wrong publication identity, missing/extra history and duplicated shared-allocation charging |

### Server coverage requirements

The T and B identifiers below name scenario families in this proposal. They
require the production guarantees being exercised; testing infrastructure does
not implement those guarantees. Current storage expectations follow the recovery
contract above. Proposed ownership and activation work remains separately owned
by [RFC 0035](0035-served-operation-ownership.md) and
[RFC 0036](0036-atomic-runtime-activation.md).

#### Execution scopes

| Symbol | Scope | Qualification |
|---|---|---|
| H | GQT-managed real local HTTP, CLI/proxy where required, supervised server process | Actual transport, process and wire behavior |
| D | Deterministic server execution (separate proposal) | Listed for coverage mapping |
| E | Real engine plus independent physical/publication assertions in its existing test/DST owner | Exact graph/storage result and completion evidence |
| C | Production server component plus contract-qualified engine adapter | Server transition logic only; never substitutes for D/E physical evidence |
| M | `omnigraph-bench` with real server/driver and shared workload/verification | Real latency, throughput, memory and interference measurements |

Scopes joined by `+` supply complementary evidence. Scope D is qualified by the
Deterministic server execution proposal; rows carrying D alone or D + E are
listed here for the coverage mapping and are not claimable from this RFC. D
obligations do not prevent an earlier H delivery backed by equivalent existing
Rust lifecycle controls. C is supplemental and cannot close
publication, external-I/O, crash-recovery or historical-reconstruction rows.

#### Correctness requirements

Adapt existing fixture owners to these minimum shapes. Record the initial graph
head, expected rows and types, touched tables and expected lineage. Conserved
totals alone cannot prove atomic publication.

| Fixture | Required shape |
|---|---|
| Atomic batch | Transfer balances from 100/0 to 90/10 across multiple statements, insert a transfer record in another table, include an edge participant and retain an untouched marker. |
| Merge | Include inherited and independently changed tables on a named target; use a shared base and divergence for three-way merge, plus no-op and fast-forward variants and a competing target writer. |
| Online schema | Warm a traversal and stored query across two node types and an edge; include an unaffected graph, optional-field addition, rename and drop/re-add. |
| Wide feed | Include narrow rows, wide escaped strings, wide keys and Blob descriptors; follow wide changes with a small sentinel commit. |

| Assertion ID | Scope | Required control and assertion | Independent oracle and sensitivity failure | Required evidence / prerequisite |
|---|---|---|---|---|
| T1.receipt | H + D + E | Hold changing merge A after publication, or no-op A after its outcome is established, before response construction; publish B; release A. Cover no-op, fast-forward and three-way through `POST /mutate` with a `branch merge` statement; the `/branches/merge` route stays with the server route suites. | Expected A outcome, parents, actor and manifest version from its bound result/history; return B's valid receipt deliberately and require failure. No-op creates no commit and retains its established target identity. | Operation/outcome bindings, publication identity where present, reached hold, wire receipt and protected history. Prerequisite: Concurrent controls (separate RFC amending RFC 0045); product exact-receipt contract; no-op receipts must carry the established target `graph_commit_id` (today `commit` is null); until then the no-op variant is refused. |
| T2.delivery | H + E | Suppress/truncate an already successful response; separately proxy 504 and caller timeout. | CLI request census proves one submission; accepted graph state proves effects despite delivery loss. Deliberate automatic resubmission must fail. | Actual CLI/server/proxy processes, wire attempts and final content; no inferred idempotency. Prerequisite: a transport control (response truncation, proxy 504, caller timeout) spelled in a later RFC 0045 amendment; until then the row's evidence stays with the CLI Rust owner. |
| T2.compound | H + E | Permit merge but deny optional source deletion. | Independently verify merged target and retained source. The established behavior of merge-with-`delete_branch` is exit 0, the merge receipt on stdout and `warning: merged, but could not delete branch` on stderr with `branch_delete_error` carrying the authorization error; the row asserts exactly that shape. Asserting `branch_deleted` and `branch_delete_error` needs a receipt-field expect the format does not define; it is a prerequisite of this row. Changing it to a nonzero exit is a CLI decision outside this RFC. A deletion failure absent from both stderr and `branch_delete_error`, or a lost receipt, fails. | Per-action authorization/outcomes and physical/publication evidence. Prerequisite: a case-declared policy (a `--- runner` field, format addition owned by RFC 0045, not specified here); a receipt-field expect (format addition owned by RFC 0045). |
| T3.retry | H + E | Exercise typed pre-admission 429, generic 409/503, recovery-required (RFC 0035 §5 `RecoveryRequired`), malformed success, precondition failure and earlier compound effects. | Expected CLI classifications from the product contract plus request/effect census. Treating unknown effects as safe replay must fail. | Exit/output records. A retry-specific exit code and the 429/409/503 classification are CLI contract prerequisites; today remote errors exit 1 and a lost `--if-commit` precondition exits 4 (`EXIT_PRECONDITION_FAILED`), and the managed-run exit 4 for recovery required is a different command family. Prerequisite: those CLI contract items; a transport control for malformed success and a recovery-required state fixture (later RFC 0045 amendment); Concurrent controls (separate RFC amending RFC 0045) for pre-admission 429. |
| T4.visibility | E + D | Fault mutation, multi-table load, merge and optimize around detached effects and graph publication, including acknowledgment loss and named branches. | Exact authored rows/types, table pins and lineage; unpublished detached artifacts may remain unreachable, but a visible participant prefix or duplicate publication fails. | Reached phase, raw outcome, physical accepted-state evidence; one-graph atomicity, no invented cross-graph transaction. Prerequisite: none for E, which keeps its engine-DST owner; D is the Deterministic server execution proposal; not claimable from this RFC. |
| T4.progress | E + D | Fail staged-schema installation or schema-sentinel release, then remove a supported transient fault and issue sentinel work on the same live handle; repeat reopen twice. | Published table pins are final; no reconciler is needed ([recovery](../dev/recovery.md): no reconciler over pins). Required schema completion restores coherent execution and supported writes resume without duplicate publication. Forced reopen-only success fails. | Same-handle/process identity, pin/schema evidence and writer-specific supported/refused outcomes. Prerequisite: none for E, which keeps its engine-DST owner; D is the Deterministic server execution proposal; not claimable from this RFC. |
| T4.foreign | E + H | Introduce a foreign linear version above the registration's `omnigraph.last_linear_version`. | Reads/writes use published pins; `repair` reports `foreign_drift` without adoption; cleanup preserves foreign manifests/files. Substituting foreign rows fails. | Before/after accepted-pin and foreign-artifact census under current detached-only semantics. Prerequisite: a fixture that constructs a foreign linear version (later RFC 0045 amendment; schema and JSONL seed cannot); until then its Rust owner `crates/omnigraph/tests/maintenance.rs` keeps it. |
| T4.legacy | E + H | Present legacy graph sidecars to current read-write open/upgrade; inspect read-only behavior separately. | Evidence remains unchanged; writable paths refuse and name originating-build recovery. Deleting or interpreting legacy evidence as current repair fails. | Before/after artifact census and typed diagnostics; cluster recovery remains separate. Prerequisite: a fixture that constructs legacy graph sidecars (later RFC 0045 amendment; schema and JSONL seed cannot); until then its Rust owner `crates/omnigraph/tests/recovery.rs` keeps it. |
| T5.disconnect | H + D + E | Close an incomplete body, then disconnect admitted mutations/merges at each relevant T4 boundary. | No effects for incomplete pre-admission requests; admitted work remains counted and settles; same-PID sentinel write succeeds. Freeing a disconnected write's capacity early fails. | Real HTTP/1 close; actual HTTP/2 reset for that claim once HTTP/2 is a declared server capability (product prerequisite); request/admission/settlement evidence. D modeled disconnect is additional evidence. Prerequisite: Concurrent controls (separate RFC amending RFC 0045); HTTP/2 reset additionally needs HTTP/2 as a declared server capability. |
| T6.pending | D + E; H where required for ordinary behavior | The storage decoration owns an accepted pending write after caller cancellation, returned error or contained panic; attempt drain, replacement and teardown before explicit release. | Pending storage owner is independent of server status. Early permit release, drain proof, duplicate execution or namespace deletion must fail. After release, verify the actual backing-store effect and permitted graph visibility, then settlement; deliberately dropping the retained write must fail. | Acceptance and completion events, original/registered-successor attribution, reservations, publication identity where present and unresolved effects. An engine-call hold alone is insufficient. Prerequisite: Concurrent controls (separate RFC amending RFC 0045); RFC 0035 §6.4 and retained-write ownership in the storage decoration (part of the separate RFC). |
| T7.shutdown | H + D | Park a write and stream producer; request shutdown; race activation; attempt new admission. | No admission after closure, one absolute deadline, no premature drain and no reopening old epoch. A renewed deadline or omitted producer fails. | D transition/timer ordering; H actual signal, grace and exit status. Worker watchdog expiry is a harness failure, not a simulated pass. Prerequisite: Concurrent controls (separate RFC amending RFC 0045); RFC 0035 §8. |
| T7.restart | H + E | Kill the actual process between completed requests; restart the same owned root twice. | Reads resolve exact published detached versions and transaction identities; no table promotion is performed. Duplicate publication fails. | Distinct boot/process identity, preserved root, exact durable census and readiness. Handle reopen cannot discharge this row. Prerequisite: none. |
| T7.boundary | H + E | Kill the actual process at durable boundaries (between detached effects, graph publication and schema installation); restart the same owned root. | Published-but-uninstalled schema contracts complete before coherent readiness; no table promotion is performed. Duplicate publication or falsely ready schema fails. | Reached boundary, distinct boot/process identity, preserved root and exact durable census. Prerequisite: a qualified hold or crash trigger inside the server process; until then its Rust owner `crates/omnigraph/tests/detached_commit_matrix.rs` keeps it. |
| T8.live | H + D + E | Deploy valid schema/query changes with warm caches; invalid candidates; fault/crash deployment phases; observe an unaffected graph. | Exact matched schema/query projection, unchanged PID/listener for successful online change, safe partial convergence and unaffected progress. Mixed bindings or false full activation fails. | Captured identities, publication/activation events, live transport and per-resource outcome. Prerequisite: Concurrent controls (separate RFC amending RFC 0045); product deployment support (RFC 0036 §7). |
| T8.history | H + D + E | Hold apply; progress an existing historical stream and admit a new retained-version query before release. Rename/drop-re-add and query-only deployment. | Fixture's exact historical data, schema, resource incarnation (RFC 0036 §5) and separately bound query revision. Completion only after apply, new incarnation substitution or current-query fallback fails. | Hold interval and within-interval results; exact view identities. Prerequisite: Concurrent controls (separate RFC amending RFC 0045); qualified historical views. |
| T8.retention | H + D + E | Evict old views, restart, change current authorization, race acquisition/expiry with cleanup, hold producers and saturate old/new overlap. | Retained views reconstruct; expired/unavailable targets refuse; current-policy denial holds; admitted readers keep protection; insufficient overlap refuses/defer before effects. Hidden cache dependence or premature GC fails. | Durable references, current auth, capture/cleanup ordering, resource evidence and process restart. No second writable oracle during serving. Prerequisite: Concurrent controls (separate RFC amending RFC 0045); RFC 0036 and qualified historical views. |
| T9.identity | H + D + E | Lose acceptance/result; concurrent same-key submission; change input/base; complete deployment 2 then look up deployment 1; change auth and expire retention. Crash identity/publication/finalization/activation boundaries. | One original execution/result; original key resolved before stale-base rejection; no deployment 1 replay/reactivation; unknown activation remains unknown. Duplicate apply, unauthorized lookup or fabricated activation fails. | Input/key/principal/incarnation bindings, request census, exact outcomes, restart and retention evidence. Prerequisite: Concurrent controls (separate RFC amending RFC 0045); product deployment identity, not general data idempotency. |
| T10.admission | H + D | Saturate process and per-actor caps, declared queue capacity or refusal policy; disconnect held callers; submit extra work. | Independent admitted/executing/queued request and effect census. Premature reservation release permits extra effects and must fail. | Declared cap domain and queue semantics, multiple actors, retained inputs and resource lifetime. Prerequisite: Concurrent controls (separate RFC amending RFC 0045); served operation ownership (RFC 0035 §6.1). |
| T10.reserve | H + D | Saturate ordinary traffic while finalization, qualified recovery, shutdown and status need capacity; separately keep storage faults persistent. | Actual completion/status actions use reserved capacity; persistent faults yield bounded refusal/unresolved evidence. A reserve consumed by ordinary work or fictitious storage progress fails. | Per-class reservation and effect observations; fault duration and deadline. Prerequisite: Concurrent controls (separate RFC amending RFC 0045); served operation ownership (RFC 0035 §6.1). |
| T10.retained | H + D + E | Slow export/Blob/feed consumers, staged inputs, candidate preparation (RFC 0036 §7), repeated failed deployments and retained historical readers. | A test-side allocation/lifetime observer, separate from tested budget counters and deduplicating shared allocations, reconciles live buffers, owners and release; repeated failures cannot grow residency without bound. Early release or leaked retained view fails. | Aggregate allocation/queue counters with owner identities, backpressure, final baseline; larger resource/feed delivery. Prerequisite: Concurrent controls (separate RFC amending RFC 0045); RFC 0036 §7 for candidate and deployment inputs; qualified historical views for retained readers. |
| T11.feed | H + D + E | Page wide rows/keys/Blob values and encoding expansion; append sentinel commit; abandon bodies and resume under the cursor semantics of RFC 0030 §5.2. | Concatenated complete changes equal authored workload history, not another reader sharing pagination logic. Omission, torn commit, stalled cursor or leaked producer fails. | Full-body history, cursor sequence, reached sentinel, buffer/producer lifetimes and explicit oversize outcome. Prerequisite: an expect that reads change-feed bodies and cursors (later RFC 0045 amendment); D is the Deterministic server execution proposal. |
| T11.status | H + D | Hold loading, recovery/drain and shutdown while polling through authorized and unauthorized clients. | Known fixture/transition availability and current authorization determine expected status independently. Hidden blocked polling, false readiness or leaked graph details fails. | Bounded status responses during holds, current actor, partial read/write availability and process-liveness distinction. Prerequisite: Concurrent controls (separate RFC amending RFC 0045); RFC 0036 §12 for transition status. |

Rows whose control column parks, overlaps or disconnects in-flight work require
the concurrent controls of the separate RFC; they are listed here so the
coverage mapping is complete, and none is claimable from this RFC alone.

These rows retain the proposed product requirements while using the current
[engine recovery contract](../dev/recovery.md). They do not relax semantics to fit an executor. A required observation without a control or oracle
is recorded as a qualification gap, never a skip or an inferred pass.

#### Performance requirements

All B rows use M. D's simulated durations and C's substitute work are ineligible
as performance evidence. E supplies verification where required, outside the
measured window. Existing backend qualification remains separately owned.

| ID | Workload and measurements | Verification and sensitivity | Required records |
|---|---|---|---|
| B1 | Merge modes; target/history/delta size sweeps; latency, peak RSS, storage calls and response/request counts | Exact receipts and protected target content; no later-HEAD reconstruction. Invalid receipt or extra submission invalidates the result. | Server/driver builds, fixture identity, cache condition, request census and repetitions. |
| B2 | Read/write mix, clients, hot keys, shared/separate branches and graphs; offered/admitted/completed/refused/unknown/failed counts and tails | Authored operation/results census; retain refused, timed-out and unfinished work. Dropping non-successes invalidates the claim. | Arrival schedule, actual dispatch, queue/execution/body times, client attribution, isolated baseline, a heavy merge/load client, its burst-end event, light-traffic slowdown and recovery after release. |
| B3 | Additive schema change, rename, rejected candidate and repeated activation under foreground load | Exact schema/query witness and unchanged server process for successful online deployment; rejected candidates preserve expected service. A candidate accepted with a changed server process invalidates the result. | Phase durations, admission pause, peer tails, RSS and retained generation lifetimes. |
| B4 | Equal final data from bulk versus fragmented/deleted histories; optimize during foreground work | Exact rows/pins before and after, one productive publication where required. Different fixtures cannot support the comparison. | Fragment/history identity, maintenance calls, temporary storage/memory and foreground slowdown. |
| B5 | T4 fault phases and participant widths; disconnect versus actual process death separately | Exact durable outcome and supported same-process progress; preserve refusal/unknown cases instead of omitting them. Omitting a refused or unknown case invalidates the comparison. | Fault delivery, result/settlement/admission times, process identity and affected graphs. |
| B6 | Wide feed, escaping/Blob expansion, consumer throttling, pause and disconnect with fixed changed rows while unrelated extent grows independently | Complete expected history and sentinel reachability; assert buffer release and cursor progress. Truncated bodies cannot count as completed throughput. | Full-body bytes/times, owned/reserved bytes, storage work, disconnect-to-release and peer tails. |

Use [RFC 0039](0039-end-to-end-benchmark.md) for fixture,
manual variance assessment, warm-up, repetition, A/A and scheduled-load validity
rules. Record the actual protocol; a different cache,
backend or schedule is a different experiment. Automate broader performance
runs only after manual runs establish variance. Benchmark results gate nothing
at any CI stage; reviewed deterministic cost assertions retain their own policy.

#### Delivery and evidence status

| Product delivery | Required correctness evidence | Measurement baseline |
|---|---|---|
| Executed result-type witness on the HTTP read response | prerequisite for every rows step under the server routes | none |
| Receipt/outcome | T1–T3, receipt sensitivity, physical request census and a deterministic merge cost recipe with operation-scoped counts excluding fixture/oracle traffic | B1 |
| Ownership/completion | T4–T7, T10.admission and T10.reserve, including delayed-I/O and actual process/transport cases | B2 and B5 |
| Resource/feed | Remaining T10 assertions and T11 | B6 |
| Online deployment | T8–T9, activation/shutdown race, historical reconstruction and overlap | B3; B4 with owned maintenance |

### Preventing false qualification

| Apparent success without the required work | Required rejection or evidence |
|---|---|
| Unsupported-route negative case reported as feature coverage | Separate harness and product records |
| Zero cases or silent engine substitution | Selection/execution counts and exact route and API identity |
| Dropped receiver reported as actual HTTP disconnect | Transport action and actual protocol evidence |
| Handle reopen reported as process restart | Boot/process/root identities and explicit process control |
| Worker reaped while helper or accepted write survives | Whole-domain process containment and separate storage quiescence |
| Two readings of the same wrong counter | Independent observer and deliberate sensitivity failure |
| Row closed by H plus an equivalence claim | Named Rust owner test, its defect-detection demonstration and a reviewer disposition in the Decision log |
| Filtered partial run credited to an unselected environment | Coverage credit binds to the executed route and the APIs its steps named; an `issue_N` case earns gate credit only for the route that ran |
| CLI step served by the runner's HTTP client | Spawned `omnigraph` process identity, its argv, exit code and both streams in the record |
| One API executed, every listed API credited | One execution record per API named on the step; a missing record fails the step |

| Honest author route | Accepted evidence |
|---|---|
| Ordinary local query or mutation | Automatic setup, real HTTP and existing typed comparisons |
| Component server exploration | Declared adapter trace and real-engine conformance, with component-only claims |
| Real signal, reset or process-death case | Supervised processes and actual transport, without a strict replay claim |
| Physical state after containment | `--- query via engine` after a containment step: a read-only open of the owned root comparing rows/pins against expectation; refused while a serving process owns the root |
| CLI scenario | `--- cli` against the owned server: exit code, stderr substring and, with `--json`, the receipt's mutate expect |
| Expected capability refusal | Passing negative harness test with no product-coverage credit |
| Real performance comparison | Shared workload/verification with benchmark-owned driving and measurement |

### Scope of evidence for this draft

The design was checked against the source owners linked above. No server route,
control or coverage row is qualified by this document. Existing engine tests
remain evidence for their original scope. The proposal changes no Lance surface;
future storage controls must extend the existing compatibility and fault-test
owners and follow [the Lance reading protocol](../dev/lance.md).

## Rollout

| Phase | Deliverable and independent stopping point | Gate |
|---|---|---|
| Local HTTP | Automatically provisioned ordinary server and existing sequential comparisons. Prerequisite: executed result-type witness on the HTTP read response (additive `ReadOutput` field; owner RFC 0051 or its amendment). Move the cluster-boot lifecycle into a library test-support module both `omnigraph-cli` tests and `omnigraph-gqt` depend on; convergence through `omnigraph_cluster::apply_config_dir`, not the CLI binary, unless the case is a CLI scenario. Preflight resolves the server (and CLI) executable, records path and digest in the immutable input; the worker receives them through the input. | Executed result column types over HTTP, isolation, guarded partial setup and whole-process containment; refusal messages name the route, the step's API and storage, never a mode |
| Sequential lifecycle steps | Process kill/restart, request shutdown and physical assertion after containment as step kinds; syntax owned by RFC 0045 | Parser and admission tests, distinct evidence for handle reopen and real process restart, process-group containment |
| Sequential lifecycle coverage | Process restart between requests (T7.restart) | Rows without any prerequisite |
| Coverage expansion | Historical views, deployment identity, feeds, generators and additional protocols; server-DST and concurrent controls arrive with their own proposals | Product prerequisites plus independent evidence for each selected assertion |
| Measurements | Server workload adapter for `omnigraph-bench` | Real measurements and verification under RFC 0039; variance established before broader automation |

Two gates precede each admission: qualification of the `omnigraph-server`
route and API, each control and independent assertion, and GQT format acceptance for the sequential
lifecycle steps before that syntax is enabled. Acceptance as architecture is
not blocked by these gates; `implementation` advances as they pass. Each phase can stop
with its remaining capabilities refused. Existing Rust tests can support an
earlier production change before its equivalent GQT control is available;
the testing program must not block a correct fix merely to replace its harness.

## Unresolved questions

Each decision below is taken in this draft as stated in the body. Each names
the maintainer who confirms or reverses it and the event that forces that call.

- Scope C owned by the `omnigraph-server` route suites (§Ownership and execution
  boundaries): server maintainer; Coverage-expansion phase.
- Numeric limits for processes and buffers: the server lifecycle maintainer
  sets and records each bound at the Sequential-lifecycle-steps gate.
- Result-type witness on the HTTP read response as the Local HTTP prerequisite:
  RFC 0051 owner; Local HTTP phase.
- Directed concurrency (clients, holds, joins, registry): separate RFC amending
  RFC 0045; GQT maintainer; before any overlap-prerequisite row is claimed.
- Deterministic server execution: separate proposal; server maintainer; before
  any D-scope row is claimed.
- Step spellings for kill, restart and request shutdown: GQT maintainer;
  Sequential-lifecycle-steps phase, before that syntax is enabled.

## Decision log

- 2026-09-26: drafted at `52ae6391`; amends RFC 0045 the same day.
