---
rfc: "2026-10-29-gqp-query-contracts-and-cedar-grants"
title: "GQP query contracts and Cedar grants"
track: maintainer
status: draft
implementation: not-started
authors:
  - aaltshuler
created: 2026-10-29
updated: 2026-10-29
discussion: null
supersedes: []
superseded_by: []
blocked_on:
  - "Demonstrate the supported read fragment and guard-preservation checks against the adversarial cases in Evidence and tests before enabling contracted execution."
  - "Measure contract checking on the agent workload corpus and set hard source, proof-work, and cache limits before enabling submitted queries."
---

# RFC: GQP query contracts and Cedar grants

## In plain language

1. **GQ** defines the query.
2. **GQP** defines what the query is allowed to read or do.
3. **Cedar** decides whether the user may run that deployed query—or submit queries under a particular GQP contract.

A caller can receive permission to use a few deployed queries without getting
general access to their underlying data. A caller that needs exploration can
receive permission to submit new queries under a contract. The database checks
each submitted query and refuses it if it cannot establish compliance.

For example, a support agent can list and count tickets within its tenant,
but cannot read internal notes. Another agent can run an approved global
ticket-count query without gaining access to individual tickets across tenants.

Omnigraph is a database for agents. Creating a query, linting it, repairing a
diagnostic, verifying its behavior, and deploying it is a natural automated
workflow for an agent. The same sequence can be daunting manual work for a
human. We should design permissions around the work our primary users can
perform cheaply: producing explicit, typed, verifiable programs.

This is an alternative to
[PR #951, Actor views and subgraph sharing](https://github.com/ModernRelay/omnigraph/pull/951).
It proposes a smaller initial authorization contract for curated tools and
ad-hoc exploration. It does not claim to deliver all of that proposal's
actor-view, sharing, search, or write guarantees. Both proposals remain drafts;
this document does not supersede or accept the other proposal.

## Summary

Add GQP, a declarative extension of the GQ language family, to specify typed
query contracts in `.gqp` files. A contract names permitted data and operations
and the predicates a query must satisfy. Reuse GQ's schema resolution,
expressions, typechecking, and diagnostics; compile contracts into a dedicated
typed representation enforced by query admission and plan acceptance.

Retain the existing policy configuration and Cedar evaluator as the authority
for grants. Extend them to authorize named deployed queries and named
submitted-query capabilities. GQP contains no actor-group grants. A deployment
binds each capability to one contract and, for a deployed capability, one exact
program revision. Execution requires both Cedar authorization and contract
compliance for that complete capability.

The first scope is read-only execution on the current `main` snapshot, using
an explicitly supported relational fragment. Broader exploration uses a
broader contract within the supported language; it never bypasses checking.
Writes, graph-derived access rules, search, history, and automatic row filtering
require later extensions with their own evidence.

## Motivation

### The workload that should drive the decision

The expected workload includes both stable agent tools and agents that invent
new questions during a task. A deployed-only system cannot serve all of those
questions. Unrestricted ad-hoc access gives a compromised or mistaken agent
too much authority. We need narrow reusable tools and a controlled way to
create new computations over the same graph.

A human-facing permission design often treats query creation and deployment
as scarce engineering work. That makes a universal, transparent actor view
attractive: define access once and let users issue arbitrary queries against
it. An agent changes that tradeoff. It can generate a task-specific query,
inspect machine-readable diagnostics, repair it, run fixtures, and prepare or
perform an authorized deployment as part of ordinary task execution.

Our product assumption is that this loop will become more common as agents
take on more work. GQ programs and GQP contracts make that loop reproducible:
the program, its declared boundary, and the authorization to run it are
separately inspectable artifacts. A successful query can become a reusable
tool without granting its callers a general-purpose data credential.

This is why the proposal is the better initial design for Omnigraph's agent
workload:

- It turns existing query-generation and linting work into a permission
  workflow, with actionable compiler feedback.
- One contract serves many deployed tools and newly submitted questions;
  every new question does not require a new role or a policy edit.
- It can deliver useful query-only access before a general graph ACL algebra,
  transparent actor views, and view-specific search semantics exist.
- Its complexity grows with the admitted language and capabilities. We can
  refuse an unsupported construct until its enforcement is established.
- It makes the authority of an agent tool explicit: callers receive a right
  to a computation, with defined parameters, rather than ambient access to
  all of the computation's inputs.

These are architectural advantages for this workload, not measured claims
about agent reliability or implementation cost. Agents can generate unsafe
queries too. Compiler and authorization enforcement remain mandatory, and
deployment authority remains separate from permission to prepare or run a
query. The evidence plan measures the authoring loop rather than assuming it
always succeeds.

### Existing foundations and the missing step

At the surveyed main revision `c1a6c83a`, Omnigraph already has:

- Named typed GQ programs, structured lint diagnostics, and conservative
  operation descriptors in the [compiler](../../crates/omnigraph-compiler/src/query/lint.rs).
- [Stored-query validation](../../crates/omnigraph-server/src/queries.rs) using
  the same typechecker as inline execution.
- [Cedar-backed policy](../../crates/omnigraph-policy/src/lib.rs), generated
  from applied YAML rules over actors, actions, and graph/branch scopes.
- [Plan requirements](../../crates/omnigraph-planner/src/validate/requirements.rs)
  that retain typed bindings, predicates, projections, and other semantics.
- Coherent [request bindings](../../crates/omnigraph-server/src/registry.rs)
  for the active policy, schema context, and stored-query registry.

These are reusable foundations, not an existing permission proof. Current
lint checks query correctness; its read-type descriptor does not establish
property access, row scope, or safe operator placement. Current `invoke_query`
covers every stored query on a graph and still requires the query body's
ordinary `read` or `change` permission. It cannot provide query-only access.

The new work is a typed contract checker, an explicit capability grant, and
engine enforcement of the accepted contract. Making a lint warning fatal or
adding a list of query names alone is insufficient.

## User and operational behavior

### Three ways to work, one admission model

| Caller grant | Available work | Admission |
|---|---|---|
| Invoke named deployed queries | Supply parameters to approved programs | Cedar authorizes the named capability; the installed program has a valid contract check |
| Submit under a narrow contract | Generate task-specific queries over limited data and operations | Cedar authorizes that capability; each submitted program must satisfy its contract |
| Submit under an exploration contract | Explore a broader permitted part of the graph | The same checks, with a broader explicit contract |

These are distinct rights, not automatic levels of privilege. Permission to
invoke a query does not grant permission to submit under its input contract.
A caller may request a capability by name, but that name is an untrusted
selector until the server authorizes it.

### Illustrative schema and contract

All GQP syntax, `$auth` expressions, and capability bindings below are proposed,
not supported syntax today. Ordinary query syntax follows the existing GQ
language. The final grammar and configuration spelling require review before
acceptance; the semantics below are the proposed contract.

Assume a graph with this schema:

```pg
node Ticket {
    tenant_id: String
    title: String
    status: String
    internal_note: String?
}
```

The contract allows public ticket fields and requires a tenant constraint:

```gqp
contract SupportRead {
    context { tenant_id: String }
    allow read Ticket { @id, tenant_id, title, status }

    require $ticket: Ticket {
        $ticket.tenant_id = $auth.tenant_id
    }

    allow operators { filter, project, group, count, order, limit }
}
```

`$ticket` is a policy metavariable. Its requirement applies to every relevant
binding of `Ticket`, regardless of the variable name in the submitted query.
The operator names describe logical query behavior, not physical plan nodes.
Every allowed type must declare a row requirement or an explicit all-rows
scope; omission is a deployment error. Unlisted types, fields, and operators
are unavailable under the contract.

The tenant attribute comes from a new, typed, trusted applied-policy binding
keyed by the authenticated actor. It is not inferred from a query parameter,
an email domain, or the requested graph. Current signed and OIDC identities
do not supply a trusted tenant attribute; OIDC `client_id` identifies an
application and must not be repurposed as a tenant. A missing or incorrectly
typed required attribute refuses admission. `$auth.actor_id` can use the
already resolved actor; all other attributes require an explicit trusted
binding. Attribute values and bindings activate with the policy bundle.
The contract's `context` declaration lists the attributes this computation
may observe and their types. Every `$auth` reference must resolve through
that declaration, including references in projections and filters. Merely
installing an actor attribute does not expose it to submitted programs.
`$auth` is reserved and cannot be shadowed by a parameter or node binding.

### A deployed support tool

```gq
query tickets_by_status($status: String) {
    match {
        $t: Ticket
        $t.tenant_id = $auth.tenant_id
        $t.status = $status
    }
    return { $t.@id as id, $t.title as title }
    order { title asc, id asc }
    limit 50
}
```

Deployment binds this program to `SupportRead`. An applied Cedar grant lets
the support-tool group invoke this named capability. The caller supplies
`status`; the server supplies trusted context. The caller has no need for
ordinary graph-wide `read` permission.

The program is checked symbolically for all valid caller parameters. A
successful check with one sample value does not approve all values. Parameter
restrictions, if introduced, are part of the capability and are enforced on
every invocation.

### A new exploratory question

An agent with permission to submit under `SupportRead` can create this query
without deploying a new tool or changing policy:

```gq
query ticket_counts_by_status() {
    match {
        $ticket: Ticket
        $ticket.tenant_id = $auth.tenant_id
    }
    return { $ticket.status as status, count($ticket) as total }
    order { status asc }
}
```

GQ groups by the non-aggregate projection; no separate `group by` clause is
needed. Counts and grouping consume the scoped ticket rows. `count($ticket)`
observes row existence and does not expand every property of the node.

The agent can use preflight diagnostics to repair a rejected query. Successful
preflight is an explanation of compliance with particular revisions, not a
bearer credential or a permanent approval. Execution repeats the necessary
authorization and binding checks.

### Refusals that explain how to repair the program

This query is refused because it lacks the required tenant constraint:

```gq
query all_titles() {
    match { $t: Ticket }
    return { $t.title as title }
}
```

This query is also refused, although it does not return the private field:

```gq
query tickets_with_private_notes() {
    match {
        $t: Ticket
        $t.tenant_id = $auth.tenant_id
        $t.internal_note is not null
    }
    return { $t.title as title }
}
```

The diagnostic identifies the forbidden field reference. Field rules cover
filters, projections, grouping, ordering, and every supported nested scope.
`return { $t }` also fails because it expands properties the contract does not
allow. Declaring `$tenant: String` and comparing against it does not satisfy
the trusted-tenant requirement. Wrapping the required guard in `or true` does
not satisfy it either.

### An approved answer without general access to its inputs

A separately installed capability can deliberately disclose a global count:

```gqp
contract GlobalTicketRead {
    allow read Ticket { @id }
    scope Ticket all
    allow operators { project, count }
}
```

```gq
query global_ticket_count() {
    match { $t: Ticket }
    return { count($t) as total }
}
```

The grant authorizes invocation of this exact deployed program on current
`main`, with no caller parameters. It does not authorize submitting programs
under `GlobalTicketRead`. That contract bounds input access; the installed
program fixes the output being authorized. The caller remains the
authenticated actor; no service impersonation or `run_as` is needed.

The output is an intentional disclosure, including changes visible over
repeated calls. An aggregate is not automatically private. Approving a
parameterized aggregate must account for all permitted parameter choices and
repeated observations; this RFC provides no differential-privacy or general
inference-prevention guarantee.

### Discovery, diagnostics, and the agent workflow

An authenticated agent can discover capabilities it may use, their parameter
and result types, and the contract/schema information needed to write an
admissible query. Submitted-query capability descriptions expose permitted
fields, required constraints, and supported operations. Query-only callers
see the tool interface, not an automatic grant to its body or input contract.
This metadata permission accompanies the capability; it must not require
granting the existing unrestricted schema/catalog `read` permission.

The intended workflow is: discover an authorized interface; generate GQ;
lint and check its GQP contract; repair diagnostics; verify behavior on
fixtures; execute under a submission grant or deploy through an authorized
configuration change. An appropriately authorized agent can automate the
whole sequence. A runtime-only agent cannot grant itself deployment rights,
broaden its contract, or edit its group membership.

CLI and MCP preflight use the same compiler checker as execution. Diagnostics
provide a stable category, source location, missing obligation, and an
actionable repair using only authorized metadata. Unknown or unauthorized
capability discovery follows the existing nondisclosure convention. Server
execution never trusts a client assertion that lint passed.

Submitted-query diagnostics use the contract's permitted schema names.
References outside that interface are refused without confirming whether a
guessed private type or property exists. Full-schema compiler diagnostics
must not become a discovery path around the capability's metadata boundary.

## Design

### A capability joins the two decisions

A capability is an applied deployment binding with:

- a stable logical identity scoped to its graph;
- one mode: deployed invocation or submitted program;
- one GQP contract revision and its resolved schema identities;
- the exact installed program revision for deployed invocation;
- typed parameter and trusted-context requirements;
- a permitted target, initially current `main` only.

These are configuration and derived compiled artifacts in the existing
deployment system. They do not introduce a second query registry, a durable
authorization ticket store, or a graph-data publication mechanism.

| Fact | Authoritative owner |
|---|---|
| Credential resolution and actor identity | Authentication boundary |
| Group membership, trusted actor attributes, and grants | Applied policy configuration |
| Whether the actor may use the requested capability | Cedar evaluation |
| Permitted program structure and data constraints | GQP contract |
| Program/contract/target association | Applied capability binding |
| Preservation of accepted requirements during execution | Engine plan acceptance |

Cedar evaluates a concrete capability resource and action with trusted
request context. The resource is selected before evaluation; Cedar is not
asked to return a profile. Multiple matching rules retain Cedar's ordinary
decision semantics. Evaluation errors refuse execution, preserving the
current adapter's stricter handling of Cedar diagnostics.

In particular, `PolicyDecision.matched_rule_id` remains diagnostic. The
current adapter chooses one matching ID by lexical order; using it to select
GQP would make a rule rename change data access. The request/resource schema
needs a real capability dimension. GQP contains no independent `grant ... to
group` statements. A future unified authoring frontend may generate Cedar
rules, but would replace the corresponding authored rules rather than create
a second authority.

### Admission and execution

```text
authenticate and capture applied bindings
    -> resolve requested capability and permitted target
    -> Cedar authorizes actor/action/capability/context
    -> typecheck program and establish GQP obligations
    -> bind caller parameters and trusted context separately
    -> accept a plan that preserves the obligations
    -> execute against one accepted data snapshot
```

For a deployed query, compilation and symbolic contract checking can be reused
from deployment. Reuse never skips current invocation authorization, required
attribute checks, or parameter validation. A submitted program is checked
against the selected authorized contract; choosing a broader name supplies no
additional authority.

The engine consumes an internal accepted execution object binding actor,
capability, program, contract, schema, target, context, and deployment epoch.
An HTTP handler's boolean or a caller-provided digest is insufficient. Every
executable plan, including replay, must retain or re-establish those bindings.
Changing the query, parameters that affect a retained proof, contract, schema
identity, or trusted context invalidates the relevant reuse.

The same requirement applies to HTTP and MCP. An embedded caller using an
installed policy cannot mint this authority by constructing public request
fields. Direct storage holders remain outside this served-access boundary,
as they are today; this proposal does not protect files from their owner.

### One complete grant per execution

For a requested capability `c` and program `q`, admission requires:

```text
CedarAllows(actor, action, c, context)
AND GQPAccepts(q, contract(c), trusted_context)
AND target_and_parameters_match(c)
```

If an interface considers more than one candidate capability, at least one
complete candidate must independently satisfy all three conditions. It must
not union fields, row scopes, operators, or program modes across candidates.

For example, tenant-scoped raw-ticket exploration plus invocation of the
global-count tool must never become global raw-ticket exploration. Likewise,
a public-fields global scope and a private-fields tenant scope must not
become private-fields global access. A genuinely needed combination requires
an explicitly defined and granted composite contract.

Cedar forbids, if exposed by the policy frontend, apply to the complete
authorization request; an allow from GQP cannot override them. A global
suspension must cover each applicable capability action. A denial for one
resource does not silently revoke a different explicitly granted resource.

### GQP is a checked fragment of the GQ language family

GQP reuses typed property references and scalar expressions, but its top-level
declarations describe obligations on programs. They are not arbitrary GQ
queries executed to discover allowed rows. The first version has no recursive
policy queries, graph membership evaluation, user-defined authorization
functions, implicit inheritance, or deny/allow composition language of its own.

The initial admitted fragment contains:

- one concrete node binding;
- non-null `String`/`Bool` input properties and parameters, with
  trusted-context equality for row requirements;
- conjunctions of supported, total scalar filters over allowed fields and
  typed parameters;
- explicit scalar projections; optional grouping/count, ordering, and a
  literal limit. `count` retains GQ's existing numeric result type.

The checker must explicitly recognize each admitted operator and expression.
Additional input scalar types and operators require documented proof rules and
tests. Multiple bindings, joins, traversals, correlated blocks, negation,
search, external providers, Blob access, arbitrary arithmetic/casts, and
mutation bodies are refused in this first fragment, even if ordinary GQ
supports them. A broader exploration contract can widen allowed types,
fields, and row scopes; it cannot enable an unimplemented operator.

This is intentionally conservative. An agent can rewrite an equivalent query
into a recognized form. The compiler does not need a general query-equivalence
or containment solver. Supporting bounded graph traversal later requires
separate obligations for each node, edge, and relevant scope; a join on a
business key alone never proves equal authorization scope.

### Required guards survive optimization

A query must already express the required restriction. GQP does not silently
insert a tenant filter or change its declared result semantics. However,
recognizing the guard in source is only the first check.

The compiler records a typed, binding-specific scope obligation, identified by
stable schema and binding identities. Plan acceptance verifies that excluded
rows cannot feed user-dependent computation, grouping, count, ordering, or
projection. A planner cannot erase the guard or move a potentially disclosing
expression ahead of it. Unknown propagation or placement is a refusal, not a
fallback to an unchecked plan. Physical scans may touch excluded rows; this
is a logical authorization boundary, not a promise about physical I/O.

Every property reference participates in checking, including implicit
whole-object expansion. Contracts use accepted schema identities, so a
supported rename preserves identity and a drop/re-add cannot inherit the
old contract merely by reusing its name. Rebinding a contract to a new
schema lifetime requires an explicitly authorized deployment.

### Disclosure boundary

For the admitted fragment, successful results are computed from the rows
satisfying the required scope, using only permitted fields and operations.
User-expression outcomes must not be affected by evaluating those expressions
against excluded rows. The initial total-expression fragment and placement
checks make this obligation narrower than general arbitrary-query security.
Ordering, ties, and multiplicity retain ordinary GQ semantics; the contract
does not create a deterministic order where the query language promises none.

This is not PR #951's full actor-view non-interference contract. Authorized
capability metadata, static rejection reasons, ordinary storage-integrity and
availability failures, timing, resource-limit outcomes, and the intentional
answers of approved global programs remain observable. Data-bearing error
payloads must not contain excluded rows or forbidden values. No claim is made
that all observable failures or costs depend only on scoped data.

Search is refused in the initial scope rather than inheriting unspecified
shared-index score or rank disclosures. History, export, unrestricted schema
inspection, physical explain/profile output, feeds, and direct Blob reads
receive no permission from a query capability. Each remains separately gated
or unavailable. Adding one requires specifying what it exposes and how the
same authorization is enforced. A resource limit can refuse a query; it must
never return a silent partial answer.

### Deployment, revocation, and bounds

Compile and link contracts, query programs, capability bindings, trusted
attributes, and Cedar policy as one candidate deployment. Dangling references,
unsupported contracts, incompatible schema identities, or a deployed program
that fails its contract prevent activation. Existing applied management
policy authorizes the change; a proposed grant cannot authorize itself.

Changing a deployed program can broaden what its callers learn even when the
new program still satisfies its input contract. Therefore program replacement
is a security-relevant configuration change, just like contract replacement.
A stable name grant trusts authorized deployers to revise that program. It is
not an immutable approval of all future bodies. Separating query publishers
from contract/grant administrators would require a narrower deployment right;
this RFC does not invent that separation within today's `config_manage`.

Requests retain coherent immutable serving bindings. Activation follows the
existing affected-request admission/drain protocol. A request already admitted
may finish under its captured policy; revocation does not promise immediate
cancellation or retract results already delivered. Requests admitted after
activation use the new grants and attributes. Data execution still uses one
accepted snapshot, with no mixed old policy and new program lookup.

Contract checks, source size, typed-program size, proof work, result delivery,
and caches have explicit bounds. Exceeding a checker budget refuses admission
with a typed outcome. Deployment checks and offline preflight use the same
semantics as execution. Cache keys include the relevant immutable revisions;
cache entries never serve as independent grants. Audit events retain the real
actor, capability, program/contract revisions, target, and decision category,
without logging bearer credentials or result/private parameter values.

## Invariants

The [architectural invariants](../dev/invariants.md) remain in force:

- **One authority and one snapshot:** use existing deployment activation and
  request capture, and one accepted data snapshot. Compiled contracts are
  derived from applied sources, not a shadow policy store.
- **Typed semantics and stable identity:** GQP obligations live in AST/IR and
  accepted plans. No query-string matching, rule-ID interpretation, or
  name-based resurrection of schema identities establishes authority.
- **Boundary authentication and engine enforcement:** trusted context is
  separate from ordinary parameters; every capability execution reaches
  engine acceptance with its authority intact.
- **Derived indexes and bounded work:** correctness cannot depend on index
  coverage. Proof budgets and execution limits refuse explicitly.
- **Graph publication, crash safety, and writes:** no graph storage or write
  protocol changes are proposed. Existing mutation gates and the one graph
  publication door remain unchanged; read capabilities authorize no writes.

The design adds no raw Lance writer, custom transaction system, queue for
manifest-derived work, inline index rebuild, process-local distributed fence,
cloud-only enforcement path, or string-built query semantics. No deny-list
exception is requested.

## Compatibility and reversibility

This is additive configuration and query-language functionality. Existing
queries and legacy graph/branch grants retain their behavior. In particular,
the current graph-wide `invoke_query` plus `read`/`change` path is not silently
reinterpreted as a query-only capability. New capability grants explicitly
authorize the contracted path without granting ordinary graph-wide `read`.

A principal that also retains broad legacy `read` can still use that broader
right. Assigning a narrow capability does not attenuate unrelated existing
grants. Operators must remove broad grants when moving a caller to query-only
access. Existing `change`, schema, management, and branch-control permissions
do not expand as a side effect of this RFC.

GQP and trusted-context syntax require explicit version admission. A binary
that cannot enforce an installed capability must refuse that configuration or
request; it must never ignore the contract or treat it as an absent policy.
Wire changes follow the [HTTP conventions](../dev/http-api.md), including
version/refusal behavior and regenerated OpenAPI evidence.

No graph storage-format migration or new Lance metadata is required by this
proposal. Reverting it means removing dependent capabilities and restoring
explicit legacy grants as an authorized configuration change. Broad access is
never an automatic downgrade fallback. Grants, `.gqp` syntax, capability
descriptions, and their public diagnostics are lasting compatibility costs.

## Alternatives

### PR #951: actor views and subgraph sharing

The compared revision is
[PR #951 at `d700c334`](https://github.com/ModernRelay/omnigraph/blob/d700c33455f1c82ef789de14e58538e5cb952e8e/docs/rfcs/2026-10-11-actor-views-and-subgraph-sharing.md).
It defines an actor-visible graph from groups, securables, grants, inheritance,
requirements, denial, placement, and time windows. Its initial requested
scope includes reads and writes under a view; its full design addresses
search, sharing, and further operations. It specifies a stronger observable
contract, including scores, ranks, errors, and result schema.

| Decision | This proposal | Actor-view proposal |
|---|---|---|
| Unit of granted authority | A complete computation capability | Access to an actor-visible subgraph, with operation gates |
| New questions | Generate and check GQ under a granted contract | Query the actor's view through supported operators |
| Curated output above ordinary row access | A fixed deployed capability is a first-class case | Definer-style stored queries appear in the later design |
| Main initial machinery | Query contracts, capability grants, conservative proof and plan checks | Graph access vocabulary, protected access facts, view construction, and operator enforcement |
| Initial data operations | A small read fragment on current `main` | Reads and writes under the proposed view contract |
| Strongest natural fit | Agent-authored tools and constrained exploration | Transparent access over overlapping, changing graph audiences |

For our selected workload, this proposal is superior as the first step: the
agent already produces a program, so validating that program is a productive
boundary; we do not need to solve every graph ACL and operator interaction
before granting useful access. The advantage comes from choosing a smaller,
explicit contract. It is not evidence that full actor views are unnecessary
or that this design provides the same guarantees more cheaply.

If the dominant requirement becomes automatic preservation of thousands of
source-system ACLs through arbitrary joins, graph sharing, or writes, actor
views may be the better foundation. GQP could later reference a separately
specified actor scope, but this RFC neither requires that model nor claims
that combining the two would be free.

### Named-query allowlists alone

These provide useful curated endpoints but do not support new questions.
They also leave data authority undefined if the caller lacks ordinary `read`.
Attaching a checked input contract and fixed program resolves that boundary;
submitted-query capabilities then support exploration through the same model.

### Cedar alone, or Cedar predicates translated to row filters

Cedar remains well suited to principal/action/resource decisions. Giving it
trusted query facts does not eliminate the compiler work that establishes
those facts or proves scope guards. Translating arbitrary Cedar predicates
into GQ/plan filters adds a second expression-semantics translation problem.
This proposal keeps data expressions in the query type system and authorizes
their use through Cedar, without depending on residual-policy evaluation.

### GQP also replaces the grant language

A single frontend could compile grant declarations to Cedar and contract
declarations to typed obligations. That is coherent if it replaces the
corresponding authored YAML. It is not needed for the first benefit and adds
actor/group/grant syntax and migration work. Maintaining independent grant
rules in both languages would make authority harder to explain and audit.

### Arbitrary GQ permission programs or automatic filtering

Running a GQ program to calculate authorization requires specifying policy
execution authority, data dependencies, snapshot/revocation behavior, cost,
and recursion. Automatically rewriting every query to a filtered graph needs
operator-by-operator semantics. Both are plausible later capabilities, but
they are larger contracts than recognizing explicit constraints in a bounded
program fragment and refusing what cannot be established.

### Keep broad graph grants or enforce everything in application code

Broad grants leave agents overprivileged. Application wrappers can expose
curated functions, but every integration must maintain parameter trust,
query selection, and enforcement correctly. Native capabilities make that
boundary reusable across HTTP, MCP, and authorized engine execution.

## Evidence and tests

This document records a proposal, not a claim that the checker exists. Current
source and test ownership were surveyed at `c1a6c83a`; the alternative was
read at `d700c334`. The current main dependency is Lance 13.0.0. This RFC
introduces no Lance format, index, transaction, or storage-behavior assumption.
Implementations that change scan/filter placement must perform the full
[Lance reading protocol](../dev/lance.md) for the affected read/execution
surfaces rather than treating this document as substrate evidence.

Extend existing owners from the [testing map](../dev/testing.md):

| Owner | Required evidence |
|---|---|
| Compiler `query/parser_tests.rs`, `typecheck_tests.rs`, and `lint_tests.rs` | Contract parsing, stable references, trusted-context types, complete field/operator footprints, and useful refusals |
| `omnigraph-policy/src/lib.rs` tests | Named capability resources, actor/group decisions, error refusal, and unchanged legacy grants |
| Planner `validate/tests.rs` | Guard binding and placement, rejected proof loss/replay, optimizer preservation, and budget exhaustion |
| Existing GQT corpus | Accepted query rows, counts, ordering, types, and rejection behavior expressible through the supported test surface |
| Server `auth_policy`, `stored_queries`, and `mcp` | Query-only access, forbidden inline reuse, spoofed context, discovery, transport parity, and retained legacy double gates |
| Server registry/deployment tests and CLI `cli_queries`/`cli_cluster_e2e` | Coherent activation, invalid registry refusal, in-flight revocation behavior, and end-to-end agent workflow |

The minimum adversarial matrix requires zero unexpected authorizations:
missing guards; guards inside `or`; ordinary parameters posing as trusted
attributes; shadowed context; undeclared trusted-attribute references;
forbidden fields used only in filters/order;
whole-node projection; multiple bindings and nested scopes outside the
supported fragment; cross-capability scope/field mixing; unauthorized target
or history selection; altered programs and stale replay; schema drop/re-add;
missing attributes; and attempts to reach export, Blob, catalog, or physical
plan data through a query-only grant. Existing accepted queries must preserve
their results and legacy authorization behavior.

Plan tests must corrupt or reorder required scope operations and prove that
acceptance refuses them. Differential fixtures change excluded rows and
forbidden fields and assert identical successful results within the promised
fragment, comparing bags for unordered results and using explicit total
orders for limited queries. Other tie/limit cases are checked against GQ's
permitted results rather than asserting an order the query did not request.
Error-producing expressions and unsupported joins/search must be
refused before execution; enabling them later requires hidden-row error,
join/negation, and ranking evidence. These tests establish the stated scope,
not the stronger all-observations equation from PR #951.

The workflow instrument uses a fixed schema and a published task corpus:
deployed support lookup, a newly generated tenant aggregation, missing-guard
repair, forbidden-field repair, unsupported-shape refusal, and the fixed
global count. Record agent/model/tool versions, attempts, diagnostics, wall
time, deployment changes, and human interventions. Compare the same tasks
using hand-managed endpoint wrappers and broad-read workflows. Require every
supported task to complete without expanding its grant; unsupported tasks
must end in an explicit refusal. Report authoring outcomes separately from
query latency and authorization latency.

Measure cold and warm contract compilation, per-invocation Cedar/binding cost,
adversarial proof work, cache memory, and plan-acceptance overhead. Set numeric
admission budgets from those measurements before enabling submitted queries;
no unmeasured speedup or performance target is claimed here. Any future change
to storage reads/writes must update the existing DST storage-cost golden.

## Rollout

1. **Contracts and offline preflight.** Implement the parser, typed contract,
   trusted-context declarations, checker, and diagnostics for the stated
   fragment. Offline success grants no new runtime access.
2. **Deployed read capabilities.** Add explicit Cedar resources/grants,
   coherent deployment bindings, engine acceptance, scoped metadata discovery,
   and query-only execution. Ship only after the adversarial and lifecycle
   gates pass. Legacy stored-query behavior remains intact.
3. **Submitted-query capabilities.** Expose the same checked fragment for
   new programs, with bounded compilation, current authorization, and safe
   cache/replay reuse. Broader contracts provide exploration within that
   fragment. Measure the agent workflow and review the limits.
4. **Later amendments.** Add typed traversal, richer expressions,
   graph-derived membership, search, history, or mutations only with their
   explicit contracts and tests. No general write or actor-view authority is
   implied by completing the read phases.

Every stop is independently safe. `implementation` advances only as work
lands; merging this draft does not authorize implementation or acceptance.

## Unresolved questions

- Finalize `.gqp` grammar, explicit all-rows syntax, trusted-attribute
  serialization, and capability references in the existing deployment/policy
  formats without duplicating their authority.
- Finalize the Cedar action/resource schema and public CLI/HTTP/MCP discovery
  and submission shapes, including their version admission and redaction.
- Confirm the exact total-expression and plan shapes of the initial fragment
  through the checker prototype. Any expansion needs new proof rules;
  numerical admission limits remain the stated measurement gate.

The choice to separate GQ programs, GQP contracts, and Cedar grants is the
proposed decision. Full actor views, arbitrary policy queries, and write
authorization are future scope, not unresolved prerequisites for that choice.

## Decision log

- 2026-10-29: Drafted as an alternative to PR #951 for curated agent tools and
  ad-hoc exploration. Selected GQP contracts with Cedar-owned grants, one
  complete capability per execution, explicit guard preservation, and an
  initial read-only fragment. No disposition of PR #951 is implied.
