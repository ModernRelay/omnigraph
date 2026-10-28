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
  - "Implement and verify the request, disconnect, publication and shutdown behavior specified here."
  - "Measure and enforce finite work limits within a supported deployment resource allocation before enabling execution."
  - "Before managed enablement: amend the control-plane requirements for durable operation results and verify capacity enforcement at every native entry point."
---

# RFC: Served optimization

## Summary

### In plain language

This RFC proposes running `optimize` through a graph's running server.
Optimization reorganizes data files and maintains supported indexes to improve
storage and read efficiency while preserving the graph's logical data.

Today, the CLI performs optimization by opening storage itself. That is suitable
when no server owns the graph. While a server is running, maintenance must go
through that server so it can coordinate the work with other requests. The
proposed command sends one request and waits for a report of what happened.

The server checks permission, limits the work and runs it through the engine,
which is the database code responsible for storage. The control plane, when
used, is the separate service that manages clusters: it schedules requests,
allocates resources and keeps operational history and collected logs. A
self-hosted operator can call the server without a control plane.

The main design choices are:

- **Use the existing server and engine.** This keeps optimization under the same
  ownership and data-safety rules as other writes. The work still consumes
  server resources and can delay ordinary writes.
- **Start with one request that waits for completion.** There is no separate
  background-job API or later result lookup. This keeps the server's management
  work small, but callers need to keep the connection open to receive the report.
- **Keep graph durability separate from report recovery.** A completed graph
  change is saved in storage and survives a server restart, even if the caller
  loses the response. This proposal
  does not store reports for retrieval after a restart. If an answer is lost,
  the caller records an unknown outcome and does not automatically repeat the
  request. The failure example below explains why.

The initial workflow is operator-requested optimization of one graph's `main`
branch. A control plane can schedule requests using the same execution contract
once the managed-enablement requirements below are met. Building that scheduler,
separate compute workers, cleanup of old data, repair and deletion of graph
history are outside this proposal.

## Motivation

Repeated writes can leave many small data fragments or indexes that would
benefit from maintenance. An operator should be able to request that work
without stopping the server and handing its storage to a separate process.

The running server already coordinates changes to graph data and configuration.
A CLI process that independently opens the same storage would bypass that
coordination. Sending the request to the server solves this ownership problem;
adding another writer would require a different concurrency design.

The initial workload is occasional, operator-requested maintenance with a
bounded amount of work. It can tolerate waiting and some interference with
ordinary writes. It does not require an audit system that can reconstruct every
maintenance request after a crash. Those assumptions motivate the execution
and result-retention choices below; they are limits of this proposal, not
claims that all future maintenance workloads will have the same needs.

## User and operational behavior

With `prod` configured as a server target and `knowledge` as a graph on it:

```bash
omnigraph optimize --target prod --graph knowledge
omnigraph optimize --target prod --graph knowledge --json
```

Both commands wait for the same result; `--json` selects machine-readable output.
The report identifies the input graph commit, the resulting or unchanged commit,
per-table work, deliberate skips, deferred indexes and optional-work failures.
An optimization can create a new commit because the stored layout changed even
though the graph's logical data did not. A no-work result identifies the
unchanged commit.

| What the caller receives | What it establishes | What to do next |
|---|---|---|
| Explicit refusal before effects | This request performed no optimization writes. | Address the permission, capacity or other reported cause before requesting work again. |
| Completed report | This attempt's reported graph result is known; inspect any skips or optional-work failures too. | Keep the report if operational history is needed. |
| Connection loss or timeout without a definitive result | The request may not have started, may still be running, or may have finished. | Record unknown; do not automatically resend it. |
| Engine/storage error | Only the effects and uncertainty stated by that error are known. An error is not proof that nothing changed. | Follow the reported storage outcome; do not infer permission to replay. |

For example, the server can finish optimization, make its graph commit durable,
and then lose the connection before the report reaches the CLI. The graph is
safe, but the CLI cannot know the result. Repeating the command could perform
more work and create another commit. A later head read also cannot prove what
this particular request did: other requests can advance the graph too.

After an unknown outcome, an operator may deliberately request a new
optimization once the server can safely admit it. That request uses the graph
state accepted at that time. It does not recover or classify the old request,
and may repeat its cost. The CLI and control plane must not silently make that
decision on the operator's behalf.

The direct-storage command remains available under exclusive storage ownership.
Remote errors never cause a fallback to direct storage, because that would
bypass the running owner's coordination.

## Design

### Responsibilities and why they are separated

| Component | Responsibility | Reason |
|---|---|---|
| Control plane | Decide when managed maintenance should run, allocate fleet resources, deliver requests, retain workflow history and collect logs. | It has the view across clusters and owns operator workflows. Each database server should not duplicate that management system. |
| Server | Authenticate callers, check whether work can start, own its lifetime and return the observed result. | It knows the active graph and configuration, current local work and storage ownership. A scheduler's approval alone cannot make execution safe. |
| Engine | Enforce graph policy, choose a consistent input version, optimize storage and make the graph change visible. | The same data-safety contract must hold for HTTP, direct CLI and embedded callers. |

In this document, **native** means the OmniGraph server/engine boundary, as
distinct from the external control plane. Control-plane records prove what it
requested or observed. If the server's answer never arrived, a record saying
"request sent" cannot prove that optimization succeeded. Ordinary diagnostic
logs and request correlation IDs help investigation; they are not a guaranteed
result-recovery protocol.

The server emits diagnostic events and metrics through its existing facilities.
Collection, long-term retention and presentation belong to the surrounding
operational system. Losing a log or report must not by itself make a safe graph
unavailable. This division reduces management overhead; it does not offload the
CPU, memory or storage traffic of optimization.

### Admission, authorization and concurrency

Add the policy action `optimize` on the selected graph's `main` branch. Installed
policies must grant it explicitly: permission to manage configuration or
membership does not imply permission to run expensive maintenance. The reserved
`admin` action does not grant it either. HTTP resolves the credential to an
actor, and the engine checks the action as well so direct calls cannot bypass
policy. Do not retain bearer credentials in logs or results.

Use normal access discovery and the existing `--as` behavior. On HTTP, `--as`
asserts that the credential resolves to the expected actor; it never supplies
a trusted identity. Keeping this assertion avoids a special authentication
rule for optimization.

The server admits at most one optimization per cluster, and none concurrently
with configuration deployment. A conflicting optimization request is refused
rather than queued. This bounds maintenance concurrency and keeps graph/policy
configuration stable during the operation. The cost is that different graphs
in one cluster cannot optimize in parallel. Restricting the initial operation
to `main` similarly avoids adding branch selection and branch-maintenance
coordination to the first supported surface.

Ordinary writes retain the engine's existing coordination and may wait for
optimization. Already admitted readers retain their consistent graph versions,
but shared CPU and storage bandwidth can still affect their latency. This is
not a promise of maintenance without performance impact.

These local coordination gates do not prevent an independent process from
writing storage. Existing cluster ownership and exclusion of previous writers
remain responsible for that boundary.

### One attempt and one graph publication

The engine carries out one attempt in this order:

1. Hold the existing schema, branch and table coordination gates and capture one
   coherent graph version: schema, branch and all participating table versions.
   Revalidate that the captured authority still permits the operation before
   effects, and check the complete supported workload against its limits.
2. Prepare replacement table versions without making them visible to readers.
   These are detached staged changes; an incomplete preparation must not expose
   a partly optimized graph.
3. When graph changes need publication, publish the selected versions together
   through the existing commit mechanism. This is the point at which the new
   layout becomes visible. Report that publication as successful only after it
   is durable. A no-work attempt skips publication and keeps its input commit.
4. Report this attempt's input and output commits and exact work outcomes.
   Capture those identities while the relevant gates still protect them;
   reading the latest commit later could attribute another request's work to
   this one.

Skips and deferred indexes remain explicit. The operation preserves logical
data and supported query guarantees; it does not promise every index is rebuilt
or that approximate-search results are identical after a physical index change.
Direct and served entry points use the same publication and failure semantics.
A resource limit can restrict admission or concurrency, but must not silently
select another correctness protocol.

### Request lifetime and truthful failure reporting

**Synchronous** describes the interface: the caller waits for the final answer.
It does not mean that closing the connection cancels storage work. The server's
existing owned-operation runtime keeps accepted work and its resource
reservations alive after the HTTP waiter disappears. An observation timeout
ends waiting; it does not establish that the engine stopped.

Three different situations must remain distinct:

| Situation | Required behavior | Why |
|---|---|---|
| The client loses a report after a known successful publication. | The caller records unknown; the server does not treat response loss alone as a storage failure. | The missing information is at the client, not necessarily in the database. |
| Publication succeeds, then optional derived-cache work fails with a known outcome. | Preserve the successful commit and report the optional failure separately. | A cache that can be rebuilt must not erase a known graph result. |
| A storage request may still take effect, or the process loses reliable ownership of work. | Apply native storage settlement and any required restrictions on further writes or shutdown. | Releasing ownership too early could allow a delayed write to interfere with later work. |

Here, **settled** means the storage request's outcome is known or its possible
late effect has been made safe. **Containment** means stopping admission or
terminating a process that can no longer safely own its work. Uncertainty
confined to particular files or object names follows the existing per-key rules; it
does not automatically close the entire process. Uncertain writes to an
optional mutable cache still need those rules: being derived does not make a
late overwrite harmless.

Shutdown and replacement servers keep the existing
[operation-ownership](2026-09-30-owned-server-operations.md) and
[storage-outcome](2026-10-19-storage-outcomes-and-restartable-ownership.md)
requirements. Dropping a future or seeing no response does not prove that a
previous writer and its accepted I/O are excluded. Conversely, a missing
maintenance report adds no new startup blocker. Graph safety and the ability
to recover a report are separate obligations.

### HTTP and result retention

Add `POST /graphs/{graph_id}/optimize` using the shared operation registry,
typed API and [HTTP conventions](2026-10-19-http-api-conventions.md). The graph
is specified once, in the path. No new request options or caller-supplied
resource ceilings are needed for the initial operation.

Return the completed report or a typed failure on that request. There is no
initial `202` job-acceptance response, persistent operation record, polling
endpoint or restart-resume promise. A request correlation ID is for diagnosis,
not a key that makes repeated requests execute once. Clients and control-plane
delivery code must disable automatic POST retries after ambiguous dispatch.

A caller can retain a received report as its operational record. The server
does not guarantee later retrieval if that response is lost or the process
restarts. The benefit and cost of this choice are explained under Alternatives.
MCP exposure is outside the initial HTTP/CLI contract.

### Work limits and enablement

An optimization request can be small while the work it triggers is large.
Request-count and request-byte limits therefore cannot protect the server on
their own. Before any rewrite, inspect the accepted input and enforce finite
ceilings on tables, fragments, physical rows, source bytes and file references.
Stop counting when a limit is exceeded; checking only after an unbounded
inventory has been collected would defeat the limit.

Account for the complete supported work: catalog and table manifests, data,
deletion and index files, and managed Blob payloads. A small Blob descriptor
can refer to a large payload, and rewriting a table can read those bytes.
External Blob references remain references; their payloads are not fetched.
Include rewriting needed to physically remove dropped columns and index work,
even when ordinary compaction alone would select less data. Unknown sizes or
unsupported layouts refuse before effects, rather than enabling an unbounded
fallback or silently optimizing only a prefix.

Start with one dataset and one compaction task in flight. This reduces competing
allocations, but one task or one Blob row can still be large. Source ceilings
are not hard limits on peak memory, CPU, temporary space or I/O rate. Enabling
execution requires measured limits within the deployment's process/platform
resource allocation, including contention with foreground requests. Metadata
enumeration, result construction and completion also need bounded resources.
No report may silently truncate work outcomes to fit a response.

Configure these limits through the existing server configuration/builder owner
so operators have one settings authority. Execution is off until explicitly
enabled within a qualified allocation; callers cannot raise its ceilings.
The endpoint exposes existing optimization behavior: it does not expand that
behavior into full-text rebuilding, which has a separate explicit operation.
A vector column with no non-null values may legitimately leave index work
pending. A failed read or index build does not prove that the column is empty;
index, statistics and storage errors must remain errors.

For managed clusters, the control plane allocates capacity and the server must
enforce it at every execution entry point. Otherwise, a direct remote CLI call
could bypass the scheduler's limits. Authentication proves identity, not
available resources: static tokens, signed tokens and OIDC are not capacity
policies. Managed execution remains disabled until native enforcement is
qualified, even if self-hosted execution is available.

## Invariants

The [architectural invariants](../dev/invariants.md) continue to govern every
entry point: one coherent input snapshot, unreachable preparation work, one
graph publication, native policy enforcement and bounded observable failures.
Indexes and caches may affect cost but cannot silently weaken logical
correctness. No second storage writer or graph-publication authority is added.
Process-local gates do not replace storage ownership, and recording a request
in the control plane does not authorize a write or prove its completion.

## Alternatives

### Require operators to stop the server

Keeping direct-only optimization avoids adding an HTTP operation. It also
requires an outage and a storage-ownership handoff for routine maintenance.
Execution through the existing owner avoids that handoff while retaining its
coordination rules, at the cost of competing with foreground work.

### Store durable jobs and results in the server

A durable job system could let callers reconnect and retrieve the result using
an operation ID. It would need to record acceptance before work, bind the ID to
one request, retain results, authorize their retrieval and define expiration.
It must also handle a crash after the graph commit but before the result record:
otherwise it still cannot say what happened to that request.

Those responsibilities add storage writes, recovery logic and retention rules
to every participating server. Reusing the configuration-deployment ledger
would still require these rules and couple physical maintenance to configuration
management. Gating normal serving on unresolved job records would also make
result recovery a database-availability dependency.

This proposal chooses the smaller contract because its workload is occasional
manual optimization. Graph durability is retained; guaranteed result retrieval
after disconnect or restart is not. The concrete cost is that an operator may
never learn the exact outcome of one request. If that is unacceptable for a
workload, durable result recovery needs a separate design before supporting it.

### Keep temporary results in server memory

A bounded in-memory status cache could support reconnection while the same
process remains alive. It avoids durable writes but still needs lookup
authorization, memory/retention bounds and a way to distinguish process
instances. Restart or eviction would again leave unknown outcomes; a missing
entry could not safely mean "never executed." Defer this interface until the
need to reconnect justifies those extra lifecycle rules.

### Let the control plane guarantee completion history

The control plane can durably record requests and responses it receives. It
cannot close the example's gap between a native commit and a lost response:
its own "sent" record does not establish completion. Treating that record as
success would invent evidence; treating it as failure and resending could
repeat work. The chosen boundary keeps workflow history there and preserves
unknown results unless native evidence establishes more.

### Move optimization to separate workers

Separate workers could reduce competition with the serving process. They would
need stable input versions, constrained storage permissions, bounded resources
and validation of their output before the owning engine publishes it. That is
an additional execution architecture. The initial proposal accepts shared
server load and possible write delays to reuse the existing execution path.

## Compatibility and reversibility

The new HTTP operation and policy action require API, CLI and access-discovery
updates. Installed policies must explicitly grant `optimize`, including for
direct callers. Existing target selection and actor assertion stay unchanged;
no additional submit, observe or recover command is needed. The graph format
is unchanged. Removing the endpoint later leaves graph commits and staged
output governed by ordinary retention and cleanup.

### Coordination with existing specifications and prototypes

Managed-service specifications currently require a stronger contract than the
one proposed here: native operation identities and results retained for lookup
and reconciliation after interruption. Those requirements appear in
[LCY-04.2](https://github.com/ModernRelay/og-control-plane/blob/58a29d6dd4ea00cef84829f03181ffe740d01974/specs/08-LCY-Lifecycle.md#L702)
and [DEC-08-76](https://github.com/ModernRelay/og-control-plane/blob/58a29d6dd4ea00cef84829f03181ffe740d01974/specs/02-DEC-Decisions.md#L3601).
They must explicitly permit unknown, unrecoverable request results before
managed enablement. Control-plane bookkeeping cannot substitute for the native
proof those documents currently require. This dependency does not block an
independently qualified self-hosted implementation.

The unmerged prototype in
[#1177](https://github.com/ModernRelay/omnigraph/pull/1177) implements durable
operation records and offline reconciliation. It is not an implementation of
this document's complete contract. Its job journal, sequence protocol, lookup
and recovery interfaces, report-retention machinery and startup restrictions
caused solely by a missing operation report must be removed or replaced when
aligning the code.

An experimental root used with that prototype may contain an unfinished
operation. Before changing binaries, exclude the previous writer and resolve
its accepted work using that prototype's recovery procedure. Deleting its
record would discard evidence without proving the work is safe. This RFC does
not supply a migration for such experimental roots.

## Evidence and tests

Extend the existing engine `integration/maintenance.rs`, policy, cluster
admission, server operation/runtime and CLI maintenance test owners. Cover the
observable contract rather than treating prototype test results as proof:

- Refusals for policy, capacity, unsupported layouts and competing maintenance
  happen before physical effects; direct calls still enforce engine policy.
- Disconnects and timeouts do not cancel accepted work, release its capacity
  early or trigger client resubmission. Shutdown preserves storage ownership.
- Reports identify the exact attempt's commits, including unchanged no-work
  results; response loss never becomes inferred success or failure.
- Known publication and settled optional failure are reported separately;
  uncertain storage requests and ownership failures retain their specific
  restrictions without blanket process shutdown for every error.
- Logical data, schema, Blob values, retained history, live branches and
  supported index/query behavior survive optimization. Use GQT for expressible
  logical assertions and existing Rust owners for mechanisms, concurrency and
  faults.

Update the operation registry, OpenAPI, CLI contracts and DST storage-cost
golden with the implementation. Verify there are no persistent job-record
writes or result-history prerequisites for startup. Measure large Blob rows,
wide/high-dimensional rows, many small fragments and index rebuilds alongside
foreground requests. Publish the tested allocation and limits; local tests do
not establish live-cloud settlement or a production resource envelope.

The resource analysis uses unmodified Lance 13.0.0
[compaction planning/execution](https://docs.rs/crate/lance/13.0.0/source/src/dataset/optimize.rs),
[fragment metadata](https://docs.rs/crate/lance-table/13.0.0/source/src/format/fragment.rs),
[index inventories](https://docs.rs/crate/lance-table/13.0.0/source/src/format/index.rs)
and [object-store sizing](https://docs.rs/crate/lance-io/13.0.0/source/src/object_store.rs).
Lance's compaction byte option excludes Blob-v2 payload accounting and does not
cover all graph optimization work, which is why the complete-work checks above
are required. This source analysis is not a measurement of resource limits.

## Rollout

1. Implement the specified HTTP/CLI, policy, ownership and failure contract;
   align prototype code and user documentation. Keep execution disabled while
   its behavior and resource limits are qualified.
2. Enable self-hosted execution only for a measured supported allocation.
   Publish its limits, policy migration and lost-response behavior with it.
3. Amend the managed-service result guarantees and verify native capacity
   enforcement before enabling managed calls. Scheduling, history and log
   collection remain control-plane responsibilities.

## Unresolved questions

No additional design choice is required for this bounded synchronous scope.
Implementation and qualification gates are listed in `blocked_on`; numerical
deployment limits require measurements, not assumptions. Reconnect lookup,
durable results and worker offload are possible separate proposals, not promises
made by this RFC.

## Decision log

- **2026-10-27:** Choose synchronous execution through the existing owner,
  explicit unknown outcomes and control-plane orchestration/history. A prior
  prototype included durable invocation lookup and offline result recovery;
  those guarantees are outside the selected contract. Native authorization,
  resource admission, graph publication and storage settlement remain required.
- **2026-10-27:** Separate design review in
  [#1186](https://github.com/ModernRelay/omnigraph/pull/1186) from the
  implementation prototype in #1177.
- **2026-10-27:** Rewrite the proposal as a standalone contract. Explain the
  request lifecycle, lost-response example, component responsibilities and the
  benefit and cost of each alternative without requiring prototype history.
