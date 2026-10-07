---
rfc: "2026-09-26-self-contained-server-testing"
title: "Self-contained server testing with GQT and DST"
track: maintainer
status: draft
implementation: not-started
authors:
  - azimafroozeh
created: 2026-09-26
updated: 2026-10-07
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
shutdown and a physical assertion after containment. Engine-DST already has
ordered concurrent blocks; server concurrency controls (named clients, holds,
joins) require a later amendment to RFC 0045. Rows that need those controls
are listed here with that prerequisite. Authors will not configure
a connection profile or start a server.

***Deterministic simulation testing (DST)*** runs the system under controlled
scheduling, clocks, randomness and I/O. Running the server deterministically
under the shared DST environment is a separate proposal, whose title is
Deterministic server execution; the coverage rows below name its scope as D.
Ordinary server tests supply real socket and process evidence;
`omnigraph-bench` supplies real performance measurements. The engine remains
independently testable. This proposal defines testing architecture and
qualification, not new production recovery or deployment guarantees. Server
qualification targets the v0.13 release/wire contract with coordinated CLI,
server and cluster tools. Independent historical serving, durable data-operation
idempotency and broader runtime binding changes are outside this scope.

## Motivation

A dropped request future does not prove that storage work it started has
stopped. A shutdown can observe a returned error while accepted storage work is still
pending. Testing either condition only through the engine misses the server's
admission, authorization, body ownership and shutdown behavior.

The current [GQT admission code](../../crates/omnigraph-gqt-core/src/runner_config.rs)
still parses a declared `target` field, recognizes both server names there
and admits only the ordinary engine route on local filesystem storage without
seams or concurrent blocks and engine-DST on in-memory storage when built with
`tokio_unstable`;
RFC 0045's route derivation replaces that field with the API each step names. Its
[concurrent block](../../crates/omnigraph-gqt/README.md#concurrent-block), landed
in PR #797, runs two to four parameterless query or mutation declarations on
one handle under engine-DST. It orders session starts, completions and named
object-store requests, including parking a request before its backing-store
call. Session outcomes and script failures are replay-compared; `--measure`
attributes Lance and control-store requests to each session. Row assertions inside a block, control statements,
separate handles, seam holds and server transports remain unsupported.
This engine control does not prove ownership of accepted I/O after caller
termination, and admits no server coverage.

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

### Deferred: server concurrency controls

This RFC defers server concurrency controls: named clients, the Start, Hold, Wait,
Release and Join controls, the operation registry, identities beyond the
server epoch, retained-write ownership, a control channel into a running
server process, relational receipt assertions and per-client principals. That
work extends RFC 0045's existing engine concurrent blocks; their scheduling
alone supplies none of these server capabilities. Coverage rows in this RFC
that need overlapping server work carry the prerequisite "Server concurrent controls
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
configuration, graphs and local credentials required to boot. Executable and
wire admission require the identified v0.13 contract; unsupported or unidentified
contracts refuse before request admission. Before initial scenario dispatch,
readiness compares `/readyz`'s
`booted_serving_digest` and `state_revision` with the applied revision the
runner wrote, digests the executable it spawned, and reads the backend from the
runner-owned graph configuration; `/readyz` does not report the backend. Setup
failure supplies no executed product coverage. A server epoch is identified by
the server alias and a boot ordinal; restart steps create a new epoch and the
report records both. Online deployment assertions additionally compare the
qualified versioned active witness with the achieved revision and resource
digests. The boot digest/revision remain boot facts and cannot prove a later
activation. Missing active evidence leaves that assertion unqualified without
changing the initial Local HTTP gate.

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

Current [recovery](../dev/recovery.md) uses final detached table pins and an
[inline schema contract](2026-09-30-schema-contract-in-manifest.md). One
`__manifest` publication accepts both; no table promotion, schema-file
installation or schema sentinel follows. A lost acknowledgment still needs the
existing exact-publication outcome check. Same-engine schema/catalog refresh,
stored-query activation and native-I/O settlement are separate obligations;
atomic durable visibility proves none of them. Unpublished detached artifacts
remain unreachable until the collector proves reclamation safe. Older protocols
belong only in explicit versioned conversion cases, not active server controls.

## Invariants

This proposal preserves [the architectural invariants](../dev/invariants.md):

- One graph publication remains the visibility authority. Test reports are
  derived execution evidence, never graph authority.
- Each operation uses its accepted view.
- Existing supported history retains its rows, identities and lineage under the
  current accepted schema and authorization; independent availability during
  apply is deferred. Conversion must not reset that history.
- Crash convergence stays in the commit protocol (invariant 5): published pins
  and the accepted contract are complete together; cases assert atomic visibility
  and collector safety, never a harness-side installation or promotion.
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
then, and T4.progress's handle reopen carries scope E + D.

The local server combination requires no connection profile and cannot attach
to an ambient service. Existing external-backend profile proposals and
qualification work remain separate. This RFC introduces no remote testing mode.
The server decision owns the v0.13 wire contract: CLI, server and cluster tools
upgrade together; unsupported or unidentified contracts refuse before admission.
No mixed-version execution, fallback or legacy error alias is qualified here.
The exact contract discriminator and CLI discovery/response checks are owned by
[the accepted HTTP admission decision](2026-09-30-v012-http-admission.md); qualifying
that transport does not provide this runner's missing server controls. `/healthz` reports package `version` and separate storage
`internal_schema_version`; neither proves the required wire behavior. v0.13 names
the release/wire line, not a manifest stamp. Normal serving requires format 13;
explicit storage conversion remains separately owned. The executed-type witness
remains a Local HTTP prerequisite, not an inferred response shape.

Existing cluster data must reach format 13 without reset, reinitialization or
export/reload. Cluster-managed conversion is currently unsupported: T7.rollout
requires a separately qualified offline admission protocol, operator-established
exclusion of readers/writers/maintenance, a restorable whole-root backup and
preserved graph/branch identities, rows, lineage and retained snapshots. Extend
existing cluster, upgrade and CLI owners to test interruption/resume and serving
admission; standalone conversion evidence alone cannot qualify this cluster path.
This requirement does not authorize a new command or automatic boot migration.

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
  process; in-process server controls wait for the server concurrency amendment.
  This proposal uses no feature-gated server build.

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
work, the identified v0.13 server contract and deployment revision where
applicable, and separate primary and cleanup outcomes. Epoch is inapplicable
for the engine routes. Independent oracles need a deliberate bad-result test:
wrong receipt, early permit release or missing history must turn the corresponding assertion red. Sensitivity
failures inject the wrong value on the actual side through the production path,
never by editing the expected value or the oracle's read.

### Harness qualification

| Owner to extend | Required evidence |
|---|---|
| GQT format and dispatch tests | Unsupported routes and APIs, a header without `via`, `--- cli` or a server API under `seeds` where refused, `via engine` before containment in a server file, zero selections, immutable input and report failure |
| Server and CLI support | Partial boot, configuration mismatch, actual request census, response truncation, HTTP/1 close, HTTP/2 reset once HTTP/2 is a declared server capability (product prerequisite), signals and same-root process restart |
| Engine/storage owners | Atomic pin/contract publication, same-engine schema visibility, exact outcomes and collector-safe retained history |
| Supervisor | Failure before readiness, worker/helper death, hung executor, descendant containment, quarantine, exhausted deadlines and preserved primary failure |
| Independent assertions | Sensitivity to wrong publication identity, missing/extra history and duplicated shared-allocation charging |

### Server coverage requirements

The T and B identifiers name scenario families, not implemented guarantees.
Current storage expectations follow the recovery contract above. Production
guarantees belong to
[Server runtime and online deployment](2026-09-29-server-runtime-and-online-deployment.md#qualification),
including [operation ownership](2026-09-29-server-runtime-and-online-deployment.md#operation-ownership)
and [serving views](2026-09-29-server-runtime-and-online-deployment.md#serving-views).
That RFC owns delivery gates; this proposal owns their scenario and evidence
requirements. The active scope is v0.13 outcomes, owned work, bounded resources,
status and drained online schema/query deployment. T8.history/T8.retention are
reserved deferred identifiers, not delivery promises. A follow-up must be accepted
before independent historical serving or its benchmarks become requirements.
Durable data-operation idempotency and broader runtime binding changes are also
outside scope; existing history semantics and authorization remain required.

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
publication, external-I/O or crash-recovery rows.

#### Correctness requirements

Adapt existing fixture owners to these minimum shapes. Record the initial graph
head, expected rows and types, touched tables and expected lineage. Conserved
totals alone cannot prove atomic publication.

| Fixture | Required shape |
|---|---|
| Atomic batch | Transfer balances from 100/0 to 90/10 across multiple statements, insert a transfer record in another table, include an edge participant and retain an untouched marker. |
| Merge | Include inherited and independently changed tables on a named target; use a shared base and divergence for three-way merge, plus no-op and fast-forward variants and a competing target writer. |
| Online schema | Warm a traversal and stored query across two node types and an edge; park requests across admission pause; include an unaffected graph, optional-field addition, rename and drop/re-add. |
| Wide feed | Include narrow rows, wide escaped strings, wide keys and Blob descriptors; follow wide changes with a small sentinel commit. |

| Assertion ID | Scope | Required control and assertion | Independent oracle and sensitivity failure | Required evidence / prerequisite |
|---|---|---|---|---|
| T1.receipt | H + D + E | Hold changing merge A after publication, or no-op A after its outcome is established, before response construction; publish B; release A. Cover no-op, fast-forward and three-way through `POST /mutate` with a `branch merge` statement; the `/branches/merge` route stays with the server route suites. | For a changing merge, expect A's own outcome, parents, actor and manifest version; substituting B's receipt fails. For no-op, an independent held outcome observation and protected history bound to A prove that A published nothing; the wire result is `already_up_to_date` with `commit: null`. Fabricating a receipt for that no-op fails. | Identified v0.13 contract, harness operation identity, established outcome, reached hold, protected history and wire response; publication identity only where a publication exists. Prerequisite: Server concurrent controls (separate RFC amending RFC 0045); [exact outcomes](2026-09-29-server-runtime-and-online-deployment.md#exact-outcomes). No additional no-op wire identity is required. |
| T2.delivery | H + E | Suppress/truncate an already successful response; separately proxy 504 and caller timeout. | CLI request census proves one submission; accepted graph state proves effects despite delivery loss. Deliberate automatic resubmission must fail. | Actual CLI/server/proxy processes, identified v0.13 CLI/server contract, wire attempts and final content; no inferred idempotency. Prerequisite: a transport control (response truncation, proxy 504, caller timeout) spelled in a later RFC 0045 amendment; until then the row's evidence stays with the CLI Rust owner. |
| T2.compound | H + E | Permit merge but deny optional source deletion under the v0.13 contract. | Independently verify the merged target and retained source. Require exit 0, the merge's own receipt, `branch_deleted: false` and `branch_delete_error_details` as `ErrorOutput`; no legacy error alias or automatic merge replay. Missing structured failure or a lost receipt fails. | Per-action authorization/outcomes and publication evidence. Prerequisite: a case-declared policy and receipt-field expect (format additions owned by RFC 0045); until then the existing CLI/server Rust owners retain these assertions. |
| T3.retry | H + E | Exercise v0.13 typed pre-admission 429 with caller-backoff `Retry-After`, generic 409/503, [`RecoveryRequired`](../dev/writes.md#failure-outcomes), malformed success, precondition failure and earlier compound effects. Present unsupported or unidentified request contracts and an incompatible response after dispatch. | Whole-command exits/actions follow [exact outcomes](2026-09-29-server-runtime-and-online-deployment.md#exact-outcomes), checked against request/effect census. Unknown effects, a generic status or a retry header alone never permit replay. Unsupported/unidentified request contracts refuse before admission with no effects or fallback; response incompatibility after dispatch reports unknown effects. Structured output preserves admission backoff without requiring a scheduled server retry. | Exit/output, headers, identified contract and admission/effect census. The explicit contract discriminator and proposed exits/classifications are product prerequisites; current health version fields and v0.11 behavior are not their qualification. Prerequisite: refusal/malformed-response fixtures and Server concurrent controls (separate RFC amending RFC 0045) for held pre-admission 429. |
| T4.visibility | E + D | Fault mutation, multi-table load, schema apply, merge and optimize around detached effects and graph publication, including acknowledgment loss and named branches where supported. | Exact authored rows/types, table pins, accepted contract and lineage; unpublished artifacts stay unreachable. A visible participant prefix, mixed pin/contract view or duplicate publication fails. | Reached phase, raw outcome and exact accepted-state evidence. Existing `schema_apply`, `failpoints` and `detached_commit_matrix` owners retain E; D remains separately qualified. One-graph atomicity, no cross-graph transaction. |
| T4.progress | E + D | Remove supported transient capture/publication faults; issue a traversal and follow-up write on the same live engine, then repeat handle reopen twice. Include schema apply before publication and after an acknowledgment loss. | The accepted contract and pins agree without file installation or a sentinel. Proven outcomes permit supported progress without duplicate publication; uncertain work retains authority and refuses reuse. Reopen-only success cannot prove same-engine progress. | Engine/process identity, exact publication outcome and schema/table evidence. Existing `schema_apply`, `failpoints` and `detached_commit_matrix` owners retain E; native settlement and D remain separately gated. |
| T4.foreign | E + H | Introduce a foreign linear version above the registration's `omnigraph.last_linear_version`. | Reads/writes use published pins; `repair` reports `foreign_drift` without adoption; cleanup preserves foreign manifests/files. Substituting foreign rows fails. | Before/after accepted-pin and foreign-artifact census under current detached-only semantics. Prerequisite: a fixture that constructs a foreign linear version (later RFC 0045 amendment; schema and JSONL seed cannot); until then its Rust owner `crates/omnigraph/tests/maintenance.rs` keeps it. |
| T4.legacy | E + H | Present legacy graph sidecars to current read-write open/upgrade; inspect read-only behavior separately. | Evidence remains unchanged; writable paths refuse and name originating-build recovery. Deleting or interpreting legacy evidence as current repair fails. | Before/after artifact census and typed diagnostics; cluster recovery remains separate. Prerequisite: a fixture that constructs legacy graph sidecars (later RFC 0045 amendment; schema and JSONL seed cannot); until then its Rust owner `crates/omnigraph/tests/recovery.rs` keeps it. |
| T5.disconnect | H + D + E | Close an incomplete body, then disconnect admitted mutations/merges at each relevant T4 boundary. | No effects for incomplete pre-admission requests; admitted work stays counted. Known settled outcomes permit a same-PID follow-up write; uncertain work retains containment. Freeing a disconnected write's capacity early fails. | Real HTTP/1 close; actual HTTP/2 reset for that claim once HTTP/2 is a declared server capability (product prerequisite); request/admission/settlement evidence. D modeled disconnect is additional evidence. Prerequisite: Server concurrent controls (separate RFC amending RFC 0045); HTTP/2 reset additionally needs HTTP/2 as a declared server capability. |
| T6.pending | D + E; H where required for ordinary behavior | The storage decoration owns an accepted pending write after caller cancellation, returned error or contained panic; attempt drain, replacement and teardown before explicit release. | Pending storage owner is independent of server status. Early permit release, drain proof, duplicate execution or namespace deletion must fail. After release, verify the actual backing-store effect and permitted graph visibility, then settlement; deliberately dropping the retained write must fail. | Acceptance and completion events, original/registered-successor attribution, reservations, publication identity where present and unresolved effects. An engine-call hold alone is insufficient. Prerequisite: Server concurrent controls (separate RFC amending RFC 0045); [accepted-I/O ownership](2026-09-29-server-runtime-and-online-deployment.md#operation-ownership) and retained-write ownership in the storage decoration (part of the separate RFC). |
| T7.shutdown | H + D | Park a write, query worker, accepted native I/O and stream producer; race shutdown with activation and cleanup, then attempt new admission. | One absolute deadline; no admission after closure, reopened old epoch, premature drain, activation or namespace deletion. Uncertain work stays owned until qualified settlement or bounded fail-stop. Omitting a child or renewing the deadline fails. | Actual signal/grace/exit under H; D orders transitions. Extend existing server lifecycle and native-pending owners. Prerequisite: Server concurrent controls and qualified native settlement; worker watchdog expiry is harness failure. |
| T7.restart | H + E | Kill the actual process between completed requests; restart the same owned root twice. | Reads resolve exact published detached versions and transaction identities; no table promotion is performed. Duplicate publication fails. | Distinct boot/process identity, preserved root, exact durable census and readiness. Handle reopen cannot discharge this row. Prerequisite: none. |
| T7.boundary | H + E | Kill the actual process between detached effects, atomic pin/contract publication and response or serving activation; restart the same owned root. | The old complete graph or new complete graph is visible; no schema installation or table promotion occurs. Exact outcome reconciliation prevents duplicate publication; mixed schema/query readiness fails. | Reached boundary, distinct process identity, preserved root and durable census. Until an in-process server crash trigger is qualified, `detached_commit_matrix` retains engine crash evidence; activation needs T8.live. |
| T7.rollout | H + E | Offline conversion of an existing cluster to format 13, with interruptions and resume before serving; include named branches and retained history. | Exact rows, identities, lineage, retained pins and contract survive without reset/reload. Partial conversion cannot admit unsupported graphs; retries do not duplicate conversion or erase evidence. | Exclusion of all prior work, backup identity, per-graph format/contract census, durable cluster authority and actual server boot. Cluster conversion is unsupported until its protocol is qualified; extend cluster/upgrade/CLI owners, not standalone bypasses. |
| T8.live | H + D + E | Submit schema/stored-query changes and graph additions with warm caches; exercise remote input capture without a client storage mount or cloud credentials. Park admitted requests before snapshot capture and new arrivals across admission closure. Exercise invalid candidates, missing provider secrets, writer handoff and an unaffected graph. Hold a candidate past its absolute deadline, then release it before its timer callback runs. | Admitted requests/workers finish before atomic apply on the same engine/PID/listener; later admissions capture matching schema/query bindings. Only classified, bounded native read tails may remain owned/charged, unable to publish, reclaim or change bindings. Unclassified tails or uncertain writes refuse activation; shutdown wins. Expired candidates never activate. A proved pre-effect abort restores unchanged service while retaining prior request owners and charges; a fresh transition must still drain them. | Engine/epoch identity, capture/activation witnesses, deadline/clock and reservation observations, writer exclusion and per-resource results. Prerequisite: [E1 transition proof](2026-09-29-server-runtime-and-online-deployment.md#serving-views), resource bounds and Server concurrent controls. Cover direct apply, retired import/refresh command refusal and unlocked-mode refusal; Azure retains its wrapper and separate live-provider qualification. No active-query overlap claim. |
| T8.history | Deferred | Reserved for independent historical reads during live apply. | Outside this RFC; no product commitment or coverage credit. | Requires an accepted follow-up defining capture/reconstruction and qualification. |
| T8.retention | Deferred | Reserved for retention and cleanup ordering supporting independent historical serving. | Outside this RFC; existing supported retained-history safety remains required. | Requires an accepted follow-up; no historical-overlap benchmark claim here. |
| T9.identity | H + D + E | Race submissions; submit D2 while D1 is pending, running or needs completion, then restart and reobserve D1. Require three graphs: D1 publishes A, fails B before publication, and leaves unaffected C serving; separately lose B's publication acknowledgement. Submit corrective D2 after a settled partial/inactive result, present a stale base, inspect D1 after D2 and expire lookup while completion is unresolved. Race publication/result/activation boundaries with shutdown and cleanup. | One durable outstanding slot; competing submission refuses without recording, queueing, supersession or effects. Assert each graph's exact achieved result; A is never rolled back or reapplied to hide B's failure. C remains available after a known refusal; an uncertain effect retains process containment until reconciliation. Slot release requires settled effects/control/candidate work and a resolved or fenced activation attempt. Settled partial/inactive results admit corrective D2; stale bases need freshly authorized input. Restart reconciles the original immutable input without duplicate apply. Lookup enforces current authorization and cannot replay/reactivate D1 or expire unfinished authority. | A/B/C state census, admitted/refused submissions, immutable revision and achieved-result/CAS, principals/incarnations, exact publication evidence and distinct active witness across restart. Extend cluster apply/failpoint and server multi-graph owners. Prerequisite: Server concurrent controls and [online deployment](2026-09-29-server-runtime-and-online-deployment.md#online-deployment). |
| T10.admission | H + D | Saturate process/per-actor caps and independent read/write lanes; disconnect held callers; submit extra work. | Independent admitted/executing/refused request and effect census. Refused work has no effects; premature reservation release or read traffic consuming protected write admission fails. | Multiple actors, cap domains, retained inputs and actual owner lifetimes. Existing workload/route owners retain component evidence; Server concurrent controls and the [resource bounds](2026-09-29-server-runtime-and-online-deployment.md#resource-bounds) gates remain prerequisites. |
| T10.reserve | H + D | Saturate ordinary traffic while publication/outcome resolution, shutdown and status require capacity; separately keep storage faults persistent. | Actual protected actions run within qualified reserves; persistent faults yield bounded unresolved/refused outcomes. Ordinary work consuming the reserve, cleanup before settlement or fictitious progress fails. | Measured per-class envelope, reservation and independent effect observations; no guessed reserve default. Prerequisite: Server concurrent controls and qualified engine/native completion capacity. |
| T10.retained | H + D + E | Slow read/export/Blob/feed consumers; vary staged inputs, schema source/IR bytes and history; repeat candidate failures and evict caches while borrowed. | Independent allocation/lifetime observation deduplicates shared buffers and accounts for decoded catalog, publication copies, serialization and retiring owners. Pre-effect refusal precedes excess allocation; failures do not grow residency indefinitely. | Owner identities, backpressure, final baseline and existing memory/loader/catalog/route owners. Prerequisite: Server concurrent controls and qualified resource envelopes; landed keyed-write limits alone do not cover catalog copies or RSS. |
| T11.feed | H + D + E | Page wide rows/keys/Blob values and encoding expansion; append a small sentinel commit; abandon bodies and resume under RFC 0030 §5.2. | Complete changes equal authored history. Omission, torn commit, stalled cursor or leaked producer fails; oversize handling must preserve the declared progress contract. Ranged-reference correctness is tested separately below. | Full-body history, cursor sequence, sentinel reachability and buffer/producer lifetimes. Extend `changes`, `changes_cost` and server route owners. A GQT body/cursor expect remains a format prerequisite; D is separately qualified. |
| T11.reference | H + E | Feed/baseline a ranged external Blob, update only its range, delete it and continue to a later commit. | Exact `{uri, offset, length}` images without external fetch; baseline/resume and cursor progress agree. Whole-object export/redirect refusal stays distinct. This does not qualify wide-row capacity. | Extend `changes.rs::change_feed_describes_ranged_external_blob_and_advances_past_it` and server route owners; authored descriptors and external-I/O census. GQT needs a qualified ranged-reference fixture and feed expect. |
| T11.status | H + D | Hold loading/drain/shutdown while authorized and unauthorized clients poll inventory, health/readiness and known/unknown graph data routes. Restore a transient startup fault; separately present a persistent fault or invalid external-Blob policy. | One complete authorized inventory includes unavailable graphs: authorized known-unavailable requests return 503, unknown graphs 404, without unauthorized disclosure. Assert live-but-unready loading with a single-graph or all-unavailable fixture, live-but-unready stopping, valid-empty readiness and counts matching inventory; an unaffected graph remains directly available during peer loading, while aggregate readiness stays 503 until all startup attempts finish. Status remains bounded outside blocked lanes. Transient retries progress within a cap; overlap/uncomparable-root Blob-policy quarantine never becomes a timed retry or permission to weaken policy. Digest mismatch remains boot refusal. | Independent state/scheduler evidence, actual status codes/bodies, unchanged PID and retry budget. Extend cluster serving and server boot/auth/route owners. Supervisory Retry-After needs a scheduled retry; T3 owns admission hints. Online retry/status coverage still needs its product gates and Server concurrent controls. |
| T11.embed | H + E | Configure an embedding provider and @embed; load one row with an omitted nullable vector and one with an explicit vector, plus a schema without @embed. Exercise HTTP and CLI structured/human output. | The annotated load/capability diagnostic explicitly reports that ingestion does not generate vectors; supplied values remain exact and omitted values follow declared nullability. No silent generation claim or ingestion-time provider request. A successful nullable load is not reported as failed; automatic embedding remains outside scope. | Exact stored vectors/nulls, response/CLI diagnostic and a provider-request census independent of returned metadata. Extend existing server data_routes/schema_routes, CLI load and loader owners; a provider-config boot test alone does not qualify the diagnostic. GQT needs the corresponding load/result assertions before claiming this row. |

Rows whose control column parks, overlaps or disconnects in-flight server work require
the server concurrent controls of the separate RFC; they are listed here so the
coverage mapping is complete, and none is claimable from this RFC alone.

The active rows exercise proposed product requirements while using the current
[engine recovery contract](../dev/recovery.md). They do not relax semantics to fit an executor. A required observation without a control or oracle
is recorded as a qualification gap, never a skip or an inferred pass.

#### Performance requirements

All active B rows use M. D's simulated durations and C's substitute work are ineligible
as performance evidence. E supplies verification where required, outside the
measured window. Existing backend qualification remains separately owned.

| ID | Workload and measurements | Verification and sensitivity | Required records |
|---|---|---|---|
| B1 | Publication/merge modes; vary touched tables/rows, target extent, delta size, projected columns, row width and concurrency. Independently sweep schema-source bytes × serialized-IR bytes × history depth; measure latency, RSS, store requests/bytes and retained catalog bytes. | Exact receipts/no-op and protected content/history. Hold live rows/table count fixed for schema/history, and the small merge delta fixed while widening unrelated target rows/columns. Flat request counts cannot bound copied bytes or residency; small input cannot bound merge scan memory. | Schema byte counts/digests, target/delta/column-width identity, history/cache identity, catalog and hydration byte/RSS curves, repetitions. Extend manifest_history_curve and merge_fast_forward wide-row owners; timing/RSS stay in the benchmark harness. Keep issue #694's merge mechanism distinct from feed limits. |
| B2 | Read/write mix, clients, hot keys, shared/separate branches and graphs; offered/admitted/completed/refused/unknown/failed counts and tails | Authored operation/results census; retain refused, timed-out and unfinished work. Dropping non-successes invalidates the claim. | Arrival schedule, actual dispatch, admission/execution/body times, client attribution, isolated baseline, a heavy merge/load client, its burst-end event, light-traffic slowdown and recovery after release. |
| B3 | Additive schema change, rename, rejected candidate and repeated activation under foreground load, including parked requests | Exact schema/query witness on the same engine and process. Admitted work finishes before apply; any qualified native read tails remain owned/charged. Rejected candidates preserve expected service; mixed bindings or hidden engine replacement invalidate the result. | Phase durations, admission pause, peer tails, RSS and retained-owner lifetimes. No independent historical or active-query overlap claim. |
| B4 | Equal final data from bulk versus fragmented/deleted histories; owned optimize, rebuild and cleanup during foreground work | Exact rows/pins before and after, one productive publication where required and protection of every retained pin. Different fixtures or reclaimed protected history invalidate the comparison. | Fragment/history identity, maintenance calls, temporary storage/memory and foreground slowdown. |
| B5 | T4 fault phases/participant widths; caller cancellation, early-return reads, returned errors, disconnect and process death as separate cases | Exact durable outcome and supported same-engine progress; retain refusal/unknown cases. Returning before owned work settles and waiting for owned work are distinct observations, neither a universal native-settlement claim. | Cancellation/early-return trigger, result, last child/native completion, admission reuse and containment times; process identity and affected graphs. Report added response latency and peer tails from waiting for owned work. |
| B6 | Wide feed, export and Blob delivery; escaping/Blob expansion, consumer throttling, pause and disconnect. For feeds, hold changed rows fixed while unrelated extent grows independently. | Complete expected bodies/history; for feeds, sentinel reachability and cursor progress. Assert buffer/producer release. Truncated bodies cannot count as completed throughput. | Full-body bytes/times, owned/reserved bytes, storage work, disconnect-to-release and peer tails. |

Current GQT [`--measure`](../../crates/omnigraph-gqt/README.md#run-and-reproduce) covers
Lance and the supplied control adapter under engine-DST; inline contract traffic
is in the Lance realm. Refresh case/seed baselines after this coverage change and
format 13 rather than compare unlike traffic. Request counts are replay evidence;
modeled times, bytes and logs are observational, and baseline deltas never fail.
Direct-file traffic, adapter-internal prefix deletes and independently constructed
adapters are not fully covered. These measurements qualify neither HTTP/process
behavior nor real latency, RSS or native settlement; keep their existing owners.

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
| E1 online deployment | T8.live and T9, including parked-request capture and activation/shutdown/cleanup races; [production rollout](2026-09-29-server-runtime-and-online-deployment.md#rollout) permits a bounded live pause | B3 without a historical-overlap claim; B4 with owned maintenance |
| Offline cluster v13 rollout | T7.rollout; separately qualified cluster admission/conversion protocol, no reset | Conversion measurements follow its protocol; no current support claim |
| Deferred historical-serving extension | T8.history/T8.retention reserved; no gate until a follow-up is accepted | Historical-overlap benchmarks outside scope |

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
| Expected harness capability refusal | Passing negative harness test with no product-coverage credit |
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
| Coverage expansion | Deployment identity, feeds, generators and additional protocols; server-DST and server concurrent controls arrive with their own proposals | Product prerequisites plus independent evidence for each selected assertion; historical-serving extensions remain outside scope |
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
- Server concurrency controls (clients, holds, joins, registry): an amendment
  extending RFC 0045's engine concurrent blocks; GQT maintainer; before any
  server-overlap prerequisite row is claimed.
- Deterministic server execution: separate proposal; server maintainer; before
  any D-scope row is claimed.
- Step spellings for kill, restart and request shutdown: GQT maintainer;
  Sequential-lifecycle-steps phase, before that syntax is enabled.

## Decision log

- 2026-10-03: T11.status now requires aggregate unready status throughout
  startup loading while checking that healthy siblings remain directly usable.

- 2026-10-03: Current server-runtime cross-references now name the accepted
  decision; implementation and qualification gates remain with that owner.


- 2026-09-26: drafted at `52ae6391`; amends RFC 0045 the same day.
- 2026-09-29: production ownership moves to [Server runtime and online deployment](2026-09-29-server-runtime-and-online-deployment.md); record the landed engine-DST concurrent subset, retain unqualified server controls, bind T9 to ledger revisions and distinguish E1 activation from E2 historical serving.
- 2026-09-30: Link `RecoveryRequired` to current write failure outcomes; qualify submission-only deployment, cluster-root writer admission, achieved-result successor bases, unavailable-graph status and the distinct admission/supervision retry hints. These remain product and harness gates, not implemented server coverage.
- 2026-09-30: Narrow server qualification to the v0.12 release/wire line, structured compound errors and one outstanding deployment per cluster. Remove mixed-version execution and supersession requirements; reserve T8.history/T8.retention and historical-overlap benchmarks for an accepted follow-up. Keep original-result reconciliation, bounded transient startup retry and one authorized inventory. Storage-version support is unchanged.

- 2026-09-30: Compatibility now links to the accepted A1 HTTP admission decision for the exact discriminator and CLI checks; server-runner and remaining outcome prerequisites stay separate.

- 2026-10-02: Replace the current-recovery, invariant, harness and T4/T7/T9 claims about staged schema installation/sentinel completion with atomic inline pin/contract publication. Rebase T8/T10/T11 and B1/B3/B5 on same-engine capture without active-query overlap, classified retained read tails, uncertain work, shutdown/cleanup races, permanent Blob-policy quarantine, separate ranged-reference/feed-width evidence and schema/IR/history cost. Replace generic migration wording with an unqualified data-preserving offline cluster-v13 gate; record two-realm GQT measurement limits and baseline refresh. No server capability is qualified by this amendment.

- 2026-10-02: Qualification restores T11.status's exact inventory, 503/404 and liveness/readiness assertions and adds T11.embed for the promised ingestion diagnostic. T8.live now exercises candidate expiry before timer delivery; T9.identity requires partial convergence across A/B with unaffected C, distinguishing known refusal from process-contained uncertainty. B1 restores independent target extent, projected columns and row width alongside the schema/history sweep; small payloads and feed evidence cannot qualify merge memory.

- 2026-10-03: Extend T8.live to graph creation, server-bound remote capture without client storage access, runtime provider preflight and pre-effect abort with retained request ownership. Retired import/refresh commands are refusal cases; Azure emulator evidence does not replace live-provider qualification.

- 2026-10-07: Replaced the body's v0.12 release/wire qualification sentences,
  rollout labels and evidence cells with the single current v0.13 contract.
  The [HTTP admission decision](2026-09-30-v012-http-admission.md) owns the
  discriminator change and refusal rules; historical decision-log entries retain
  their original release scope. No storage-format change follows from the wire bump.
