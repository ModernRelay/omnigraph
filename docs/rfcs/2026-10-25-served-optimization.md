---
rfc: "2026-10-25-served-optimization"
title: "Served optimization"
track: maintainer
status: draft
implementation: in-progress
authors:
  - aaltshuler
created: 2026-10-25
updated: 2026-10-27
discussion: https://github.com/ModernRelay/omnigraph/pull/1186
supersedes: []
superseded_by: []
blocked_on:
  - "Align the implementation with the synchronous execution contract and qualify disconnect, publication and shutdown outcomes."
  - "Qualify finite native work bounds and the supported deployment resource envelope before enabling execution."
  - "Before managed enablement: amend LCY-04.2 and DEC-08-76 and qualify native enforcement of control-plane capacity allocation."
---

# RFC: Served optimization

## In plain language

**Purpose:** let an operator optimize a graph while its server stays running.

**The problem:** as data changes, its storage can become fragmented.
`optimize` reorganizes data files and maintains supported indexes so the engine
can read them more efficiently, while preserving the graph's data. Today the
command opens storage itself. Running it beside a server would introduce a
second writer that the server does not control.

**The proposed solution:** send the request to the server that already manages
the graph. That server checks permission and available capacity, runs the
existing engine operation, and returns a report of what happened. The command
waits for that report. Reads can continue; writes may wait while optimization
holds the resources it needs.

**Key design decisions:**

- **Keep management in the control plane.** For managed clusters, it decides
  when to request work and keeps scheduling records, operational history and
  collected logs. The server checks each request and owns its execution; the
  engine performs the storage changes safely. A self-hosted operator can call
  the server directly without a control plane.
- **Keep the server interface small.** One request optimizes one graph's
  `main` branch and returns its result. The server does not gain a persistent
  jobs database, automatic scheduler or maintenance recovery command. Looking
  up an individual operation after disconnect is outside this first slice.
- **Reuse the existing safety rules.** The engine makes the optimized graph
  visible as one complete change. The server enforces permissions and work
  limits, and admits only one optimization at a time per cluster. Optimization
  still consumes server resources; this proposal does not move the computation
  to the control plane or promise maintenance without impact on other work.
- **Be honest when the answer is lost.** Losing the connection does not cancel
  the work. If the caller never receives the result, the outcome is unknown;
  neither the CLI nor the control plane automatically submits it again. The
  server does not promise to recover that request's report after a restart.

**The tradeoff:** this gives operators a safe way to request optimization with
little new server machinery. It deliberately gives up guaranteed recovery of
every request's result after a crash. The control plane can keep a history of
what it requested and observed, but cannot turn a missing answer into proof
that the work succeeded or failed.

## Summary

Expose `optimize` through the server that already owns the cluster. The server
owns safe execution and returns the exact result it observes. The control plane
owns scheduling, delivery, operational history and collected logs. The engine
keeps its existing snapshot, staging and graph-publication contract.

The first slice is one synchronous request for one graph's `main`. It adds no
durable maintenance journal, scheduler, restart lookup or recovery command.
A lost response leaves the caller's outcome unknown; it never authorizes an
automatic retry. This is a deliberate limit on the execution contract.

The implementation in [#1177](https://github.com/ModernRelay/omnigraph/pull/1177)
currently includes a stronger durable invocation protocol. It must be reduced
to this revised design before it can claim conformance. This document changes
the proposal; it does not claim the implementation already matches it.

## Motivation

The existing direct `optimize` opens graph storage. A running cluster already
owns that storage, so an independent process cannot safely optimize beside it.
Operators need to run the existing operation through that owner.

Durable job tracking is a separate requirement. Making receipt retention a
prerequisite for serving creates an availability dependency unrelated to whether
the graph is safe. A manual operation does not need its own sequence ledger,
publication-witness protocol or offline recovery workflow to be useful.

## User and operational behavior

For a configured server target, the proposed command is:

```bash
omnigraph optimize --target prod --graph knowledge
omnigraph optimize --target prod --graph knowledge --json
```

The command waits for this attempt's result. It reports the accepted input
commit, its resulting commit or unchanged commit, table statistics, skips,
deferred indexes and any optional-work failures. It may block writes while
holding existing gates; admitted reads retain their snapshots.

The existing direct command remains available under normal exclusive native
ownership. A remote refusal never falls back to direct storage. Existing
target selection, actor assertion and result-output conventions apply. There
are no new `--no-wait`, `--operation-id` or `--writers-stopped` modes.

| Observation | Meaning and caller action |
|---|---|
| Definitive refusal before effects | This request did not optimize. Correct the reported cause before making another request. |
| Completed result | Use this attempt's exact report, including no-work and optional-work outcomes. |
| Lost response or observation timeout | Execution may still be running or may have completed. Record unknown; do not automatically resubmit. |
| Native failure | Preserve its typed effect and settlement evidence; an error alone does not prove that nothing published. |

A later explicitly requested optimization is a new attempt against a fresh
accepted snapshot, after native admission permits it. It may perform additional
work or publish another commit. It is not replay, recovery or proof that the
previous attempt failed. A changed graph head or equal logical data does not
identify the old attempt's result.

## Design

### Responsibility boundary

| Owner | Responsibility |
|---|---|
| Control plane | Scheduling, fleet capacity allocation, delivery policy, durable workflow history, log collection/retention and operator presentation. |
| Server | Authentication, local admission, ownership of execution through disconnect, bounded execution status and exact responses. |
| Engine | Authorization at the native entry point, accepted snapshot, physical optimization, graph publication and typed storage outcomes. |

The control plane records intent, delivery attempts and responses it actually
observed. Its records are authoritative for its workflow, not native acceptance
or publication. An unobserved result remains unknown. Self-hosted callers can
use the same native endpoint without deploying a control plane.

The server emits ordinary correlated diagnostic events and metrics through the
existing observability facilities. It does not persist a second operational
history or manage log delivery. Loss of an observational record is neither
proof of safety nor a reason to close otherwise valid native admission.
This minimizes management overhead; physical optimization still runs in the
engine. Moving that computation to independent workers is out of scope.

### Native execution and outcomes

Add the graph policy action `optimize`, scoped to `main`. Installed policies
must explicitly grant it; `admin`, configuration management and membership do
not imply it. HTTP resolves the bearer to an actor; cluster admission checks
current applied policy and the engine checks the same action before effects.
Keep normal access discovery and the existing `--as` credential-actor assertion.
An assertion never supplies a trusted actor. Do not retain bearer bytes.

Reuse the existing cluster admission, owned-operation runtime and deployment
gate. Admit at most one optimization per cluster and serialize it with
configuration deployment. Refuse another optimization while that slot or the
deployment gate is busy; ordinary writes keep their existing gate behavior.
Accepted work remains owned after its HTTP waiter exits. Disconnect and
observation deadlines end waiting, not native execution.

The engine retains its schema, branch and sorted-table gates, one coherent
accepted snapshot, detached staging and single graph-manifest publication.
Capture the exact before/after commits as part of that attempt, while the
relevant gates are held; never infer the result from a later head read.
Direct and served entry points share these semantics. Resource limits constrain
work; their presence must not select a different publication or error protocol.

Separate known graph publication from optional derived-artifact work. A settled
artifact failure is reported explicitly without relabeling a known commit as
unknown. Genuine uncertain I/O, including a possibly accepted write to a mutable
cache object, still needs the existing settlement and containment rules. Never
discard that uncertainty merely because the data is derived.
Key-scoped storage uncertainty does not itself close process admission;
containment remains limited to the cases defined by the existing storage-outcome
contract, rather than every failure after acceptance.

Shutdown and successor admission follow the existing
[owned-operation](2026-09-30-owned-server-operations.md) and
[storage-outcome](2026-10-19-storage-outcomes-and-restartable-ownership.md)
contracts. A dropped future does not exclude accepted I/O or a predecessor.
There is no additional startup blocker for a missing maintenance receipt, no
neutral graph commit solely to settle one, and no maintenance-specific unlock.

### HTTP, receipts and status

Add `POST /graphs/{graph_id}/optimize` through the shared operation registry,
typed API and [HTTP conventions](2026-10-19-http-api-conventions.md). The graph
is addressed once, in the path. The initial operation uses the existing optimize
behavior without new request options; clients do not copy server ceilings into
a maintenance envelope.
Return the exact completed report after durable publication, or the typed
failure. A no-work result names the unchanged commit. Do not return `202` as a
promise of durable acceptance.

Here, a receipt means the completed response observed by the caller. It is not
a server-persisted record. The first slice exposes completion through that
response and keeps execution status in the existing owned-operation runtime;
it adds no per-invocation lookup endpoint. A request correlation ID supplies
identification only, not idempotency, deduplication or restart recovery.
Clients and control-plane delivery code must disable automatic POST retries
after ambiguous dispatch. Native status may be lost at restart.

MCP exposure and asynchronous lookup are deferred. If reconnect lookup becomes
necessary, a separate bounded, process-local extension can retain active and
terminal statuses scoped to the server incarnation. Missing, evicted or old
statuses must mean unknown, never not-executed, and lookup must never execute
work. That extension must define its limits and authorization before shipping;
this RFC does not require a generic jobs API to expose optimize.

### Bounds and enablement

Execution is disabled until explicitly configured and qualified. Keep typed
limits under the server's existing configuration/builder ownership, without a
maintenance-only YAML file or parser. The server selects finite ceilings for
tables, fragments, physical rows, source bytes and file references; callers
cannot raise them. Start with one dataset and one compaction task in flight.
Bound metadata enumeration and result construction as well as physical work.

Validate the complete accepted source inventory before the first rewrite,
including dropped-column materialization, catalog work, table manifests,
index files, deletion files and managed Blob payloads. External Blob references
remain references; their payloads are not fetched. Count before exceeding the
limit, rather than materializing an unbounded inventory and checking afterwards.
Unknown sizes or unsupported layouts refuse. Preserve the current full-text
rebuild boundary; index/statistics/storage errors must not masquerade as
insufficient vector training data. Do not add historical-prefix scans or output
truncation.

Finite source ceilings and concurrency do not establish hard memory, CPU,
scratch-space or I/O-rate guarantees. Native work and completion resources need
measured limits and process/platform isolation for the supported envelope,
including competition with foreground requests. Existing request-count and
request-byte admission is not a substitute. Ordinary bounded result delivery
remains necessary; the durable-journal report reservation and recovery-envelope
machinery are not part of this contract.

Managed execution additionally requires native enforcement of control-plane
capacity allocation on every entry point, including direct remote CLI calls.
A proxy check alone is insufficient. State support through explicit deployment
capability and resource admission, not an inference from static-token, signed
token or OIDC authentication. Until qualified, managed execution stays disabled.

## Invariants

The [architectural invariants](../dev/invariants.md) remain unchanged: one
accepted snapshot, detached pre-publication effects, one graph publication,
engine authorization, bounded work and typed uncertain outcomes. Derived indexes
and caches affect cost, never logical correctness. Existing ownership and I/O
settlement remain necessary even when observational status is missing.

There is no second storage writer, manifest-derived job queue, custom WAL,
shadow publication authority, implicit retry, or inline content-write rebuild.
The control plane cannot bypass native policy, resource admission or fencing.
Cleanup, repair, history deletion and isolated workers remain outside this RFC.

## Compatibility and reversibility

This design adds no graph format or durable maintenance object. Adding the
`optimize` action requires policy and access-discovery updates for direct and
served callers. Removing the endpoint later leaves ordinary graph commits and
detached output governed by existing retention and cleanup rules.

The candidate in #1177 has not established a supported wire or disk contract.
Remove its maintenance journal, sequence protocol, lookup/recovery routes,
receipt-only admission blocks and recovery CLI flags before shipping. Do not
silently delete an outstanding journal in an experimental deployment: exclude
its owner and settle its native work using the candidate's recovery procedure
before switching binaries. Migration of such experimental roots is not supplied
by this documentation change.

The control-plane specifications
[LCY-04.2](https://github.com/ModernRelay/og-control-plane/blob/58a29d6dd4ea00cef84829f03181ffe740d01974/specs/08-LCY-Lifecycle.md#L702)
and [DEC-08-76](https://github.com/ModernRelay/og-control-plane/blob/58a29d6dd4ea00cef84829f03181ffe740d01974/specs/02-DEC-Decisions.md#L3601)
currently require durable native invocation identity, acceptance, results and
original-ID reconciliation. Amend those requirements before claiming managed
compatibility with this weaker observation contract. Moving the same guarantees
into control-plane bookkeeping would not satisfy them.

## Alternatives

- **Keep direct-only optimization:** requires stopping the owner for routine
  physical maintenance; does not solve the operational problem.
- **Durable native invocation journal:** supports original-ID lookup and
  publication reconciliation across crashes, with unresolved or partial outcomes
  when evidence is insufficient. It adds persistent identities, retention,
  recovery and an availability dependency on receipt settlement. Defer until a
  concrete use case requires that stronger guarantee.
- **Reuse the deployment ledger:** couples configuration convergence to physical
  optimization without removing durable invocation complexity.
- **Control-plane-only execution receipts:** cannot prove native publication
  when a response was lost. Keep workflow history there and preserve unknowns.
- **Process-local asynchronous status:** useful for reconnecting observers, but
  adds retention, authorization and incarnation semantics. Defer until the
  synchronous slice demonstrates that need.

## Evidence and tests

Extend the existing engine `integration/maintenance.rs`, policy, cluster
admission, server operation/runtime and CLI maintenance owners. Prove:

- Denied actors, unsupported deployments, exhausted bounds and competing work
  refuse before physical effects; direct calls cannot bypass engine policy.
- Disconnect and timeout leave execution owned, with no client resubmission.
  Shutdown and predecessor exclusion preserve native I/O settlement.
- Success reports this attempt's exact commit; no-work reports the unchanged
  commit. Lost responses and crashes never become inferred success or failure.
- Known publication plus settled optional failure preserves both facts;
  uncertain publication or optional I/O retains the required native fencing.
- Rows, schema, Blob values, index behavior, retained history and live branches
  are preserved. Use GQT for expressible logical assertions; retain Rust owners
  for physical mechanisms, concurrency and faults.

Update the registry, OpenAPI, CLI contracts and DST storage-cost golden with
implementation changes. Verify that no receipt lookup, journal write or
maintenance-specific startup gate remains. Qualify resource and live-cloud
settlement separately; local correctness tests cannot establish those limits.
Measure large Blob rows, wide/high-dimensional rows, many small fragments and
index rebuilds under the intended allocation. Publish the tested limits.

The candidate's source accounting was inspected against unmodified Lance
13.0.0: [compaction planning/execution](https://docs.rs/crate/lance/13.0.0/source/src/dataset/optimize.rs),
[fragment metadata](https://docs.rs/crate/lance-table/13.0.0/source/src/format/fragment.rs),
[index inventories](https://docs.rs/crate/lance-table/13.0.0/source/src/format/index.rs)
and [object-store sizing](https://docs.rs/crate/lance-io/13.0.0/source/src/object_store.rs).
Lance's compaction byte option alone excludes Blob-v2 payload accounting and
does not bound all optimization work. Reuse qualified native mechanisms rather
than introducing another transaction or recovery protocol. Tests of the prior
journal design are not evidence that this revised contract is implemented.

## Rollout

1. Review this reduced contract and align #1177's code, tests, OpenAPI and user
   documentation. Keep execution disabled during qualification.
2. Enable the synchronous self-hosted slice only for the measured supported
   resource envelope. Preserve explicit unknown outcomes and existing shutdown
   behavior; publish the policy change and limits with the implementation.
3. Amend the control-plane specifications and qualify native capacity admission
   before enabling managed calls. Keep scheduling and operational history in
   the control plane. This phase does not gate the self-hosted slice.

## Unresolved questions

No additional design decision is required for the synchronous scope. The
implementation, resource and managed-integration evidence gates are listed in
`blocked_on`. Durable receipts, asynchronous lookup and worker offload require
separate justification and are not implied follow-on commitments.

## Decision log

- **2026-10-27:** Narrow the initial candidate to safe execution through the
  existing owner. Replace durable invocation/lookup and offline recovery with
  synchronous exact results and explicit unknown outcomes. Assign orchestration,
  history and log collection to the control plane; retain native authorization,
  resource admission, publication and settlement. Record the control-plane
  specification changes needed before managed enablement. The implementation
  still needs to be aligned; this revision does not accept its earlier protocol.
- **2026-10-27:** Move design review to
  [#1186](https://github.com/ModernRelay/omnigraph/pull/1186), independently of
  the implementation in #1177. The proposal and its qualification gates are
  unchanged by the split.
