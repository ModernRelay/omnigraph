---
rfc: "0059"
title: "Configuration and CLI coherency"
track: maintainer
status: draft
implementation: not-started
authors:
  - andrew
created: 2026-09-06
updated: 2026-09-08
discussion: https://github.com/ModernRelay/omnigraph/pull/675
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC 0059: Configuration and CLI coherency

This RFC proposes a compatibility correction; it does not authorize new
behavior until accepted. Requirement identifiers beginning `CC-` are local to
this RFC and are not public error codes. Acceptance of an individual issue
referenced here does not accept this entire proposal.

The [configuration and CLI framework](../dev/config-cli-framework.md) records
the vocabulary, current boundaries, and explicitly proposed coherence rules.
This RFC owns the correction's requirements, rationale, alternatives,
migration, and evidence gates. The [CLI reference](../user/cli/reference.md)
and [operator configuration](0007-operator-config.md) own their existing
public contracts. Conflicts are review findings to resolve, not permission to
change an accepted contract implicitly.

## 1. Summary and evidence boundary

Keep offline signed data credentials, WorkOS control sessions, and the existing
server authorization model. Replace the cwd-based data dispatcher introduced
by PR #633 with managed connections selected through the existing operator
scope resolver. Keep config-directory selection for cluster operations.

The proposed operator syntax adds a managed variant to `clusters:` and a
cluster binding to read aliases. Existing server entries, root entries,
profiles, direct graph addressing, and legacy credentials remain valid.
The first implementation enables managed `query` and `mutate` through explicit
cluster/profile selection. Other managed HTTP operations need separate
qualification; their existence on the server is insufficient evidence of
CLI support.

The accepted scope of [issue #653](https://github.com/ModernRelay/omnigraph/issues/653)
is included as a separately testable served catalog CLI extension. It uses an
existing HTTP endpoint and preserves direct listing. Accepted issue scope is
not implementation evidence or acceptance of this entire RFC. No requirements
are imported from other open or needs-triage issues merely because they are
related.

There is no universally transparent migration: an identical invocation can
mean one target before #633 and another after it. A temporary refusal protects
the ambiguous implicit `query`/`mutate` case while consumers move to explicit
selection. It never chooses a target or supplies a credential.

### 1.1 Inspected baselines

| Baseline | Meaning here |
| --- | --- |
| `02d182b6` | Parent of #633; compatibility reference for the operator resolver and public Rust shapes. |
| `e23778d9` | Merged #633; managed control CLI, offline data credentials, and cwd data interception. |
| `c253f111` | Merged #663; inspected merged baseline, including actor provenance. |
| `c9275697` | Inspected local lifecycle worktree; extra commands are not part of the merged baseline. |
| Publication base `500cdc29` | Production CLI/cluster/server behavior inspected here is unchanged from `c253f111`; this is not an execution result. |
| CP design references | The separately owned managed authority, permission, execution, and lifecycle contracts; not acceptance of this RFC. |

“Source-verified” means a statement was checked in those source revisions.
“Test-verified” requires a named executed test and its result. This document
contains source findings and an acceptance plan; no new runtime, build,
platform, or live test execution is claimed. Examples use illustrative names
and origins. Proposed examples must not be advertised as accepted by the
current binary.

## 2. Problem

Before #633, graph commands selected a target through explicit addresses,
profiles, environment-selected profiles, and operator defaults. #633 adds a
second dispatcher before that resolver. When cwd contains managed context,
`query`/`mutate` use its API and cluster identity and the cached credential's
endpoint. Explicit addressing conflicts; inherited profiles and defaults do
not participate. Other graph/tool commands are rejected, including commands
that could otherwise operate entirely on explicit local files or storage.
Malformed context can fail before ordinary resolution.

For example, assume the shell has `OMNIGRAPH_PROFILE=staging`, the profile
selects a staging server, cwd has production context, and a valid production
data credential exists:

```sh
omnigraph --graph knowledge mutate update_person --params '{"slug":"alice"}'
```

The ordinary resolver selects staging. The #633 dispatcher can instead select
production if the graph/action grants and applied policy permit the request.
Authentication can be correct for the endpoint while operator intent was
misresolved. An explicit `--profile staging` currently causes a scope conflict
rather than restoring ordinary precedence.

The problem is the competing routing authority, not offline verification or
managed permissions. The resolver and overlay are visible in
[scope.rs](https://github.com/ModernRelay/omnigraph/blob/c253f111/crates/omnigraph-cli/src/scope.rs#L55-L151)
and [managed/data.rs](https://github.com/ModernRelay/omnigraph/blob/e23778d9/crates/omnigraph-cli/src/managed/data.rs#L318-L377).
The correction changes operator syntax, credential selection, and compatibility
across existing public contracts; a local dispatch patch alone cannot establish
those boundaries. They require this RFC and owned migration evidence.

## 3. Goals and non-goals

The goals are:

- Restore one data-target resolver with established precedence and explicit
  connection identity carried through authentication and transport.
- Add managed cluster naming without repurposing legacy servers or direct
  roots, and preserve existing valid operator configuration.
- Preserve offline data access, least privilege, exact credential binding,
  and no fallback across execution or credential modes.
- Give existing #633 consumers a migration that cannot silently retarget a
  mutation, while removing the blanket folder gate from unrelated commands.
- Make compatibility boundaries and acceptance evidence explicit.

This correction does not introduce infrastructure provisioning, a console,
public graph routing, implicit token renewal, a new token service, sticky
profile state, automatic endpoint discovery, or administrative ownership
transfer. It does not repair actor-setting omission semantics from #663,
change engine lock/CAS protocols, complete writer fencing, or change lifecycle
retention. It does not make every server route eligible for managed CLI use.
It does not introduce managed approvals, a second approver, or per-action
step-up.

## 4. Authority and effect boundaries

**CC-01 — Keep each authority in its existing home.**

| Surface | Authority | Explicit boundary |
| --- | --- | --- |
| `cluster.yaml` and referenced `.pg`, `.gq`, policy/config files | Requested cluster definition | Personal routing and credentials cannot replace these inputs. |
| Managed service source binding | One external or managed repository and its revision contract | No merge of two config homes; arbitrary directory contents are not execution input. |
| Applied ledger | Effective cluster resource projection under the Core's existing rules | Routing cannot write it or reconstruct desired source from it. |
| Accepted schema | Effective schema and actor-setting authority under the engine contract | A requested `.pg` file and a cluster-ledger projection cannot substitute for accepted state. |
| Root identity marker and guard records | Immutable root identity and protocol-owned exclusion/execution/lifecycle evidence | Service identity/status projections cannot replace these authoritative records. |
| Operator `config.yaml` | Personal connections, profiles, aliases, output, direct actor preference | Does not enroll a cluster, change grants, or alter desired/effective state. |
| Config directory `.omnigraph/context` | API origin and service cluster identity for cluster commands | Does not become a data default or credential store. |
| Credential stores | Credentials and their scope metadata | May validate a selected target; never select a different target. |
| Service account/principal records and server boot trust | Service-owned principal/grant facts and offline verification configuration | A cluster identity projection must agree with the root marker; an operator nickname or client actor does not create authority. |

Operator names are mutable references. Renaming one may change operator YAML
and, for legacy servers, name-keyed credential storage. It must not rename a
service cluster/principal or alter graph identity/content. This rule applies
to authoritative identity, not every persisted byte. Legacy URL-to-server
credential lookup is preserved and is not a service identity mechanism.

**CC-02 — Preserve existing Core and managed execution protocols.** This
change does not add a writer of `__cluster/state.json`. Existing apply,
partial apply, recovery, import, refresh, and serving contracts remain with
their present owners. Partial apply can persist achieved resource state and
advance `state_revision` before full convergence; recording the requested
`config_digest` has its own conditional contract. Current import/refresh can
insert or alter observed schema/graph digests as well as remove or re-status
entries; import can admit externally created graphs. This RFC does not
incorrectly redefine those commands as observation-only, removal-only, or
restricted to an unchanged desired inventory. Routing must not change their
mutation rules, lock acquisition, revision/CAS handling, or sidecar/recovery
behavior.

A read-shaped CLI verb is not a universal no-write promise. Some direct
branch/snapshot/commit reads, baseline/export, and query validation open the
engine for read-write operation and can perform recovery. Schema show and
graph-backed lint use read-only opening paths. Preserve these distinctions
and record their existing effects in fixtures; this correction must neither
silently suppress recovery nor claim that every read is side-effect-free.

Serving continues to consume the applied snapshot and report its boot
witness, with existing quarantine, readiness, and shutdown semantics. A
directory-based boot may read desired configuration to locate the storage
root; applied resources remain the authority for what boots. Opening a graph
can perform its existing recovery work without making serving a writer of the
cluster ledger. A fixed boot witness, the later current ledger, and authorized
status projections need not be identical; public readiness remains minimal.
Operator connections do not become boot inputs. This RFC does not require a
new rollout, restart policy, hot-reload protocol, universal status/readiness
parity, or treatment of an empty applied cluster.

Managed apply still executes the exact saved plan with current scoped
permission and verified execution authority. It does not silently replan,
commit source, approve itself, or bypass existing destructive-resource
authority checks. Local direct prompts and the historical Core `approve`
command retain their existing meaning; neither becomes a managed approval
step.

### 4.1 Architectural invariants and substrate alignment

The [standing invariants](../dev/invariants.md) remain mandatory. The routing
correction changes how a caller selects an existing access path; it does not
create an exception to the publication, recovery, or trust boundaries.

| Invariants | How this RFC preserves the boundary |
| --- | --- |
| 1, substrate ownership; 2 and 4, one graph publication | No new storage protocol, per-table writer, publication door, or Lance API. Existing engine operations continue to own effects. |
| 3 and 5, coherent attempts and recovery | A resolved connection stays fixed for an attempt. Unknown outcomes do not trigger a new target; retry/recovery remains operation-specific. |
| 6, stable identity | Operator names remain personal references; exact API/cluster/endpoint binding does not replace root/incarnation or accepted schema identity. |
| 7 and 12, derived state and one authority | Credentials validate rather than select targets. Routing does not rewrite desired files, the applied ledger, accepted schema, or guard records. |
| 8 and 9, explicit failures and typed semantics | Selected invalid/unsupported paths refuse without substitution. Query parsing, typed semantics, and source-selection contracts remain unchanged. |
| 10, trust at the boundary and engine policy | Managed data retains verified graph/action ceilings plus Cedar; restored public actor shapes cannot manufacture verified signed authority. Direct writes retain their existing policy checks. |
| 11, bounded observable failure and resource use | Keep explicit managed transport bounds, distinguish unknown outcomes from refusal, and define eligibility before adding another operation. |
| 13, evidence at the owning layer | Resolver/cache/HTTP/boot/public API changes have separate owning fixtures; source inspection and executed evidence remain distinct. |

No deny-list exception is requested: in particular, no shadow source of truth,
cloud-only engine correctness path, process-local writer fence, or silent
retry is introduced. The transient migration guard can only refuse an
ambiguous call; it cannot become another routing or credential authority.

The repository pins Lance 11.0.0. This proposal does not change a Lance-shaped
behavior or depend on a new undocumented substrate guarantee; it preserves
the existing OmniGraph facade contracts. No new Lance qualification is claimed
by this documentation change. If an implementation crosses that boundary,
its owner must perform the complete relevant reading and compatibility checks
required by [the Lance guide](../dev/lance.md) before changing the design.

## 5. Proposed operator configuration

**CC-03 — Add a flat, explicit cluster variant.** Extend the existing
`~/.omnigraph/config.yaml`, or `$OMNIGRAPH_HOME/config.yaml`:

```yaml
operator:
  actor: act-andrew

defaults:
  output: table
  table_max_column_width: 80
  table_cell_layout: wrap
  server: legacy-local
  default_graph: knowledge

servers:
  legacy-local:
    url: http://localhost:8080

clusters:
  pilot:
    managed: true
    id: CLUSTER_ID
    api: https://control.example
    endpoint: https://pilot.graphs.example
  local-root:
    managed: false
    root: file:///Users/andrew/data/local-cluster
  existing-root:
    root: s3://example-bucket/cluster

profiles:
  pilot:
    cluster: pilot
    default_graph: knowledge
  maintenance:
    cluster: local-root
    default_graph: knowledge
  legacy:
    server: legacy-local
    default_graph: knowledge
  scratch:
    store: file:///Users/andrew/data/scratch

aliases:
  legacy-people:
    server: legacy-local
    graph: knowledge
    query: list_people
    args: []
    params: {}
    format: table
  pilot-people:
    cluster: pilot
    graph: knowledge
    query: list_people
    args: []
    params: {}
    format: table
```

`pilot-people` illustrates the proposed additive alias binding. Managed alias
execution is a separately qualified extension after the initial query/mutate
slice; its presence here is not a claim of current eligibility.

### 5.1 Validation

| Entry | Required | Rejected combinations |
| --- | --- | --- |
| `clusters.NAME` with YAML `managed: true` | `id`, `api`, `endpoint` | `root`, missing/invalid binding fields, embedded credentials. |
| `clusters.NAME` with `managed: false` or omitted | `root` | Managed `id`, `api`, or `endpoint` fields. |
| `servers.NAME` | Existing `url` | No new managed auth is inferred from this entry. |
| Profile | Exactly one of `server`, `cluster`, `store` | Multiple binding kinds; a store binding with `default_graph`. |
| Existing alias | Existing server/query binding | Global address overrides remain invalid. |
| New cluster alias | Exactly one cluster binding, `graph`, `query` | Simultaneous server binding; root-backed cluster as a served alias. |

Validate the discriminator as a YAML boolean, not a truthy string. Use the
existing cluster-ID grammar. Validate managed API and endpoint as canonical
origins: HTTPS, or HTTP for the existing exact loopback hosts `localhost`,
`127.0.0.1`, and `::1`; reject userinfo, non-root paths, query, and fragment.
Explicit valid ports remain part of canonical origin identity. These managed
rules do not tighten the legacy server URL contract.

The new variant must fail closed for misspelled, missing, contradictory, or
unknown routing/auth fields. Ordinary unrelated preference keys retain their
current warning behavior. A malformed new managed entry must not degrade to
the old root variant or a legacy server. Its lack of `root` is deliberate:
the inspected older CLI requires that field and refuses the new entry.
That can prevent an old CLI from loading the whole operator file; migration
must account for mixed binary versions, not promise per-entry compatibility.

All existing top-level sections remain optional. No `defaults.cluster`,
`current_profile`, token field, nested managed object, or per-profile output
format is added. Flat defaults retain their existing server/store exclusivity.
Output and alias parameter rules remain unchanged. Setting `managed` does
not prove ownership or convert an existing deployment: it declares the
connection variant used by the client.

## 6. Data and storage resolution

**CC-04 — Preserve the established priority order.** Resolve an address
without network access or credential inspection:

| Rank | Source | Result |
| --- | --- | --- |
| 1 | One explicit primitive: `--server`, `--store`, `--cluster`, or supported positional/verb-local URI | Select that address; conflicting explicit primitives refuse. |
| 2 | Explicit `--profile NAME` | Select that profile's complete binding. |
| 3 | `OMNIGRAPH_PROFILE` | Select that profile's complete binding. |
| 4 | Flat operator defaults | Select the existing server/store default. |
| 5 | None | Refuse if this command needs a target. |

An explicit address intentionally shadows unused profile/environment/default
scope. An explicit profile intentionally shadows the environment profile.
These inputs are accounted for by precedence, not silently ignored as a
defect. Unknown selected names and invalid selected bindings refuse; the
resolver does not try a lower-priority source after selection fails.

An explicit `--graph` overrides the graph default from the selected profile.
The profile does not inherit a flat default graph. An explicit primitive does
not inherit a graph from an unused profile. Flat default graph behavior
remains attached to the flat server scope.

**CC-05 — Carry the selected connection, then check capability.** The
resolved representation distinguishes legacy HTTP, standalone graph storage,
root tooling, and managed HTTP with exact API/ID/endpoint. It also records
selection provenance for diagnostics. Authentication variant is a property
of the connection, not whether a flag, profile, or environment selected it.
No public Rust type name or enum layout is mandated here.

Once selected, an unsupported target refuses in place. For example, a managed
profile with `optimize` must not fall through to `defaults.store`. A bad
credential must not cause another profile, endpoint, transport, or root to be
tried. Capability checking occurs after variant resolution so a named managed
cluster can support data access without expanding legacy root eligibility.

Preserve names and literals:

- `--server NAME` selects an existing server; literal `--server URL` stays
  legacy HTTP, including its existing credential-owner lookup.
- `--cluster NAME` recognizes a configured managed entry or existing root
  entry; preserve current literal directory/root handling when no name binds.
- `--store GRAPH_URI` continues to name one graph. A root is not a graph.
- Positional storage/`--store` HTTP strings do not gain server semantics.
- Aliases use their own binding, reject scope overrides, and eventually use
  the same connection/authentication helper as ordinary data commands.
- Local/session commands do not acquire graph targets from this cascade.

**CC-06 — Remove cwd from final data routing.** Normal data resolution does
not inspect `.omnigraph/context`, search parents, or scan caches. An unrelated
or malformed context cannot affect an explicit target or local/offline tool.
Section 10 defines the sole temporary exception, a refusal-only migration
check for implicit query/mutate.

### 6.1 Graph omissions and initial eligibility

| Target/operation | Rule preserved or proposed |
| --- | --- |
| Standalone graph store | URI identifies the graph; an additional graph selector refuses. |
| Legacy HTTP graph operation | Preserve selected graph defaults and existing omission probe behavior. |
| Legacy `graphs list` | Registry operation; explicit graph refuses and default graph is not used. |
| Root maintenance | Preserve selected profile graph and existing sole-applied-graph derivation; zero/several refuse. |
| Root policy/query tooling | Preserve current helper's applied-source selection, including its profile-graph exception. |
| Managed query/mutate | Require explicit graph or selected profile graph; never infer from token grants or graph inventory. |
| Managed registry operation | Deferred qualification; cluster scope with no graph selector. |

Current legacy HTTP omission is not sole-graph selection: the best-effort
registry probe refuses any nonempty result, including one graph; empty,
unsupported, denied, or failed probes may continue to the bare server URL.
Repairing that behavior is a separate compatibility change.

**CC-07 — Start with the existing managed operation set.**

| Managed operation | First correction slice |
| --- | --- |
| Stored `query` / `read` | Eligible with `read` and `invoke_query`. |
| Ad-hoc `query` | Eligible with `read`. |
| Stored `mutate` / `change` | Eligible with `change` and `invoke_query`. |
| Ad-hoc `mutate` | Eligible with `change`. |
| `queries list` | Accepted served HTTP CLI mapping in section 6.2; named managed connection support needs separate qualification. |
| New managed alias | Design included; activate only after its explicit alias-path qualification. |
| Load/ingest, snapshot, Blob, export, branch, commit, changes, schema show, graph registry | Deferred per-operation CLI, permissions, and transport qualification. |
| Managed schema apply, direct maintenance, initialization, graph-backed lint/schema plan, policy tooling, `queries validate` | Unsupported through this managed data connection. |

Every eligible request also requires current applied Cedar authorization.
Grant possession is not policy enrollment. Apart from CC-18's served-list
extension, existing legacy HTTP/storage eligibility remains unchanged. In
particular, ordinary root-backed cluster profiles still do not become direct
query/mutate access paths.

Stored names select the applied query catalog; `--query`/`-e` provide ad-hoc
source. There is no implicit file search for a stored name. Data branch,
snapshot, output, conditional mutation, and query-source semantics retain
their [existing CLI contracts](../user/cli/reference.md), with omission
exceptions recorded in the [framework](../dev/config-cli-framework.md).
`ingest` remains a distinct hidden permissive command, not an alias for
canonical `load`, whose mode is required.

### 6.2 Accepted served query listing

**CC-18 — Expose the active served catalog through the CLI.**
[Issue #653](https://github.com/ModernRelay/omnigraph/issues/653) is labeled
`accepted`, `feature`, `P-high`; the CLI mapping is not implemented in the
inspected source. Include its accepted scope as a bounded extension alongside
the resolver work:

```sh
# Accepted work; requires the served-list CLI implementation.
omnigraph queries list --server example --graph example --json
```

- Use the existing `GET /graphs/{graph}/queries` endpoint. Preserve normal
  server, graph, profile, default and credential resolution and conflicting
  address handling; do not create a parallel resolver or catalog.
- Split command classification for `queries list` from `queries validate`.
  Listing gains served access while retaining its existing direct cluster
  path; validation retains its existing direct helper and capabilities.
- Return a stable documented JSON shape with operation names/kinds and the
  metadata actually supplied by the active server catalog. Richer compiled
  descriptors are not a dependency or an assumed response field.
- Preserve endpoint authorization and unknown/unauthorized graph behavior.
  Catalog visibility does not imply permission to invoke an operation; do not
  add per-query invocation filtering as an unrequested policy change.
- Qualify a future named managed connection separately through the common
  bound-credential path. Accepted ordinary served CLI support alone does not
  approve or prove that additional authentication variant.

Tests cover a catalog containing a read and a mutation, an empty catalog,
unknown and unauthorized graphs, server/profile/default selection, conflicting
addresses, stable JSON and active-catalog authority. Existing direct listing
and validation must retain their behavior. This extension requires no new
graph endpoint, data writer or stored-query registry.

## 7. Separate config-directory control pipeline

**CC-08 — Do not retarget cluster controls through data profiles.** Keep
`cluster COMMAND --config DIR`, with `DIR` defaulting to `.`. For ordinary
Core-capable verbs: explicit `--direct` skips context; otherwise valid context
dispatches managed control; absent context uses the existing Core route;
malformed context refuses. Search only the exact selected directory.

Managed-only verbs refuse without the required context or when direct is
requested. This is not a blanket fallback rule. The lifecycle worktree's
initial create and operation-status recovery have their own explicit
selection rules; retain those if/when that work lands.

```sh
omnigraph cluster plan --config ./checkout
omnigraph cluster apply --config ./checkout --plan PLAN_RUN_ID
omnigraph cluster plan --config ./checkout --direct
```

The apply example requires a managed-bound directory and an exact saved plan.
Existing direct apply retains its own Core arguments. `--cluster pilot` and
data profiles cannot redirect these operations, infer desired files, or select
a different source revision. There is no new environment override for
`--config` in this proposal.

Preserve existing `login --api`, `logout --api`, `use ... --config`, and
`cluster token --config` behavior and strict context handling. A token/status
shortcut by operator nickname can be considered later, independently of
plan/apply and without inferring desired source.

At `c9275697`, create, push, delete, undo-delete, and operation-status recovery
are worktree-only additions. This correction neither releases nor qualifies
them, changes their replay identity, nor adds a purge verb.
That worktree's push capture is Unix-only and bounded to 4,096 files, 2 MiB
per file, 32 MiB total, 120 seconds, and 512-byte paths. Preserve its referenced
file checks rather than replacing them with a directory upload. Its local
lock, pending, and last-operation records support capture/replay; they do not
become cluster identity, execution authority, or desired/effective state.

## 8. Credentials, actor, and transport

**CC-09 — Select one authentication variant from the resolved connection.**

| Connection | Credential contract |
| --- | --- |
| Graph/root storage | Existing filesystem/backend credentials; no managed credential substitution. |
| Legacy HTTP | Existing matched server's keyed environment, then credentials file, then legacy bearer environment; existing anonymous/server-policy behavior when absent. |
| Managed control | Separate origin-bound control session or explicitly bound automation control credential. |
| Managed data | Existing separate data-keychain namespace, keyed by canonical API origin and cluster ID, with exact selected endpoint validation. |

No cross-namespace fallback is permitted. The legacy chain intentionally has
several sources within its own contract; preserving it is not permission for
managed requests to use `OMNIGRAPH_BEARER_TOKEN`. Adding a managed entry at a
legacy URL must not change a literal `--server URL` request's credentials.

**CC-10 — Validate the complete selected managed binding before sending.**
Require equality between operator selection and cached canonical API, cluster
ID, and canonical endpoint. Preserve credential format/size, expiry, graph,
and action validation. Read only the selected keychain entry; do not scan for
one that happens to match grants. A cache edit, endpoint mismatch, expired
token, or insufficient scope refuses before sending that credential.

Current `Credential::validate` checks API and cluster equality, but only
canonical syntax for endpoint. It then connects to the cached endpoint; there
is no independently selected expected endpoint today. Exact endpoint equality
is therefore a proposed correction, not a source-verified existing guarantee.
See [the current check and use](https://github.com/ModernRelay/omnigraph/blob/c253f111/crates/omnigraph-cli/src/managed/data.rs#L97-L134)
and [client construction](https://github.com/ModernRelay/omnigraph/blob/c253f111/crates/omnigraph-cli/src/managed/data.rs#L281-L315).

Client metadata validation is not signature verification. The server still
verifies the signed credential, issuer, audience, actual root/incarnation,
expiry, graph/action ceiling, and applied policy. A matching client tuple does
not make a graph ready or authenticate the server beyond ordinary TLS.

Keep the existing keychain format and token-mint/clear output contract. Minting
remains an explicit control request for a graph/action subset with current
data grants; it does not edit operator YAML or defaults. Metadata output may
be used to prepare the explicit connection. Endpoint rotation requires a
deliberate connection update and matching newly issued/current credential;
neither side silently wins. Clearing the local entry does not revoke an
offline server credential, and control logout does not imply such revocation.
Keep existing TTL validation (default 3,600 seconds; accepted range
60–86,400 seconds). Token issuance does not gain an idempotency-key contract:
the existing endpoint rejects one. Run/lifecycle replay guarantees must not
be generalized to token minting.

**CC-11 — Bound offline managed query/mutation requests.** Use a 10-second
connection deadline and a 30-second total request deadline for the initially
eligible stored and ad-hoc queries/mutations. The total deadline includes
connection establishment and consumption of the complete response body; body
progress does not restart it. Retain no redirects, no automatic retries, and
the existing 8 MiB JSON-response bound. A mutation timeout after submission
leaves the outcome unknown; it does not prove cancellation or lack of effects.

The 30-second total replaces the inspected baseline's 10-second total in this
proposed contract; it is not a claim that those historical binaries already
use 30 seconds. Preserve the 64 KiB cached-record and 8 KiB token bounds and
existing validators. Data requests do not call the Intent API, mint
credentials, refresh sessions, or fall back to another authority. Other
operations and streaming routes require their own explicit bounded transport
contract before qualification; they must not inherit an unsuitable JSON limit
or remove bounds silently. Legacy transport does not acquire new restrictions
merely because managed support is added.

**CC-12 — Preserve actor attribution boundaries.** Direct writes retain
`--as` and operator actor defaults. Authenticated served writes derive actor
from verified server identity and reject client actor overrides. Actor
provenance records a fact about a write; it grants no permission. Login,
token issuance, and ordinary reads do not require materializing an actor.

## 9. Failures and direct compatibility

**CC-13 — Preserve explicit direct syntax without auth downgrade.** Both
global `--direct` and its established cluster-local spelling remain accepted.
Legacy data HTTP invocations may continue using it; the historical flag is
not exclusively a storage selector. A newly selected managed data connection
plus `--direct` refuses the conflict before credential transmission or storage
access. It must not discover a root, substitute a static token, or turn into
direct execution.

Local syntax, binding, capability, and credential preflight errors should
occur before request or operation effects. Remote authorization and readiness
refusals necessarily follow a request; they must not be misrepresented as
local preflight or as evidence that no request happened. Preserve their
existing safe diagnostics and effect guarantees.

A lost write/run-submission response remains an unknown outcome, not proof of
a no-op. Do not retry against another target. Reuse the existing operation
idempotency protocol where present; otherwise inspect the relevant head/effect
under that operation's recovery contract before resubmitting.

Use existing typed managed failures when applicable. Name any needed new
public failure class in the owning implementation/spec review, not by silently
turning a local `CC-` requirement into a protocol code. Current legacy errors
are not uniformly a closed typed registry; normalizing all errors is outside
this compatibility correction. Diagnostics may show nonsecret selected
bindings and selection source, never bearer values or complete keychain data.

## 10. Migration and removal of the temporary guard

**CC-14 — Protect ambiguous old invocations without routing through context.**
The final resolver has no data context source. During one announced transition,
only implicit former #633 `query`/`mutate` invocations may run a migration
refusal check. The parsed `read`/`change` aliases share those command cases.

The guard is ineligible and returns before any context read for:

- An explicit address primitive or explicit `--profile`.
- Explicit global or supported local `--direct`.
- An alias with its own binding.
- Any other command, including local/offline tools and cluster controls.

For an eligible implicit invocation, first determine the proposed selection
using the ordinary resolver. If no exact-directory context exists, continue
ordinary behavior. If context is malformed, refuse the migration ambiguity
without guessing the old identity. If it exists, allow only when the old
managed API/cluster and validated cached endpoint can be shown to agree with
the already selected new managed API/cluster/endpoint. Comparing a legacy URL
alone, graph-name overlap, or token grants is insufficient. If proof is absent,
invalid, or different, request an explicit selector and execute against
neither candidate. Normal credential and capability validation still follows
any successful comparison.

The cache can supply evidence for this narrow equivalence test; it cannot
supply the new target. Missing cache metadata cannot turn an ambiguous call
into an ordinary legacy write. Context in a parent directory is not searched.
No context is rewritten and no permanent per-operator migration marker is
created. This transitional refusal is an explicit, temporary exception to
context-independent preflight, not a second resolver or permanent mode gate.

The release owner must publish a finite removal boundary before shipping the
guard: the introduction release, last release containing it, supported
consumer migration interval, and removal release. No arbitrary version is
invented by this discussion RFC. The documented boundary is a release blocker,
not permission for indefinite retention. Automated consumers must migrate
before upgrading past removal, or implicit legacy/default resolution could
change a former context-dependent destination.

### 10.1 Consumer rollout

1. Inventory released and Git-revision consumers separately, including scripts,
   CI images, aliases, operator files, and #633-only public Rust consumers.
2. Save the existing binary/config/script versions and metadata-only bindings.
   Do not export credentials into the project or commit operator secrets.
3. Prepare the managed entry and profile using verified service/token metadata;
   do not derive identity from an endpoint URL. Update calls to explicit
   selection. Do not change global defaults automatically.
4. Qualify the new binary against the prepared file and unchanged legacy file,
   then switch each consumer's binary/config/scripts as a coordinated unit.
   Use isolated operator homes where old and new binaries must coexist.
5. Preserve control login and valid #633-format data credentials where their
   metadata exactly matches. Remint explicitly only when required.
6. Observe the declared migration boundary, then remove only the temporary
   refusal path. Keep the independent resolver and no-fallback regressions.

Proposed data calls after migration:

```sh
omnigraph --cluster pilot --graph knowledge query list_people
omnigraph --profile pilot query list_people
omnigraph --profile pilot mutate update_person --params '{"slug":"alice"}'
```

These are valid proposed grammar examples, not executed requests. The profile
supplies `knowledge`. The examples require the named catalog entries and
appropriate grants/policy. Existing explicit legacy calls remain legacy:

```sh
omnigraph --server legacy-local --graph knowledge query list_people
omnigraph --profile legacy query list_people
omnigraph --direct --server legacy-local --graph knowledge query list_people
```

### 10.2 Rollback

Routing rollback restores the matching saved binary, operator file, and
scripts together. Do not launch an old binary on a new root-less managed
variant and expect it to ignore that entry. Do not restore implicit
context-dependent mutations as a “safe default”: pin the intended legacy
target with explicit addressing and `--direct` where #633 requires it, or
pause that consumer until its managed path is restored.

This correction does not itself require graph, service identity, infrastructure,
or database migration. Any separately deployed server/API or public Rust
adaptation has its own matched artifact rollback and auth regression gates.
An engine downgrade across #663's accepted schema format is not made safe by
rolling back routing configuration; it remains outside this procedure.

## 11. HTTP, public Rust, and #663 compatibility

**CC-15 — Keep wire and authority contracts stable.** The CLI routing repair
does not need new graph routes, JSON request/response formats, token claims,
control-session namespaces, or keychain schema. Preserve signed-only grant
ceilings, graph filtering, static-plus-signed coexistence, and server policy
enforcement. Compare OpenAPI and captured request shapes rather than assuming
that an unchanged command name proves wire compatibility.

PR #633 also changed public Rust struct shapes and made root canonicalization
an unconditional snapshot step. These are separate compatibility axes from
the data resolver and should be addressed in an isolated implementation step:

- Prefer restoring the pre-633 public field shapes of `ServerConfig`,
  `ServingSnapshot`, and `ResolvedActor`, and expose additive opt-in managed
  boot/root-bound snapshot entry points. Exact new API names need owner review.
- Keep verified signed claims and selected-graph state in an internal
  authenticated-request representation. Restoring old constructible identity
  fields must not let callers forge signed authority or omit the grant ceiling.
- Obtain canonical root from the same resolved store as the managed snapshot;
  validate signed trust before opening graph engines. Fail strictly on managed
  root mismatch/canonicalization failure. A lexical or empty substitute is
  invalid. Trust-disabled paths should not inherit the newly added fallible
  canonicalization operation solely to support managed boot.
- Keep static-token, policy, and explicit unauthenticated boot behavior.
  Inspecting an added failure point is not proof that a particular old root
  succeeded; regression fixtures must establish that comparison.

The proposed API repair has three concrete seams, regardless of final names:

1. The existing snapshot function retains its prior return shape and
   trust-disabled behavior. An additive managed snapshot function returns
   that snapshot together with a canonical-root binding obtained from the
   same opened store. It does not reopen a caller-supplied root and assume
   that two independent resolutions refer to the same snapshot.
2. The existing server entry point continues to accept the restored legacy
   `ServerConfig`. An additive managed boot entry point accepts the legacy
   options plus the root-bound snapshot/public-trust inputs, validates their
   agreement before engine opening, and constructs internal server state.
   It must not default managed callers to an unverified legacy entry point
   when validation fails.
3. The old public `ResolvedActor` remains the identity projection consumed by
   legacy callers. Internal authenticated request state retains whether the
   identity was statically or cryptographically authenticated, verified token
   claims, and selected graph. All signed authorization, including graph
   listing and action checks, consumes that internal state through the final
   handler; converting to the public projection cannot discard the ceiling.
   A public actor literal or `Scope::DataToken` value is not proof of a
   verified token. Legacy static authority retains its current Cedar path.

Use one implementation of claim verification, graph selection, and policy
intersection beneath the additive entry points. Fixtures must cover expiry,
wrong graph/root/incarnation, graph-list filtering, and callers constructing
the old public actor shape, as well as compilation. This keeps the public
compatibility work substantive without freezing unreviewed API names here.

One public struct cannot preserve both incompatible literal/destructuring
shapes. `Default`, constructors, and newly adding `non_exhaustive` alone do not
restore old struct-literal compatibility. Prefer the established pre-633 shape
and provide migration instructions for consumers already using #633 fields.
Inventory those consumers before landing the public API step. Existing
`Scope`/`AuthSource` enums were non-exhaustive; do not list their added variants
as the same source break. Source/pin consumers need compile fixtures, not a
claim of universal zero-break compatibility.

**CC-16 — Keep the actor configuration question separate.** At #663,
actor-provenance omission means default-on for new graphs but preservation of
the accepted setting for existing graphs. A Git revert to an omitted setting
can therefore retain a later explicit toggle. The accepted schema is effective
authority under that implementation; cluster state is its projection.
Replacing this with deterministic desired-setting semantics requires a
separate decision and migration plan. This RFC neither fixes nor endorses
that history-sensitive desired-config behavior, changes protected `OmniActor`
binding rules, or removes schema IR v3 upgrade/downgrade constraints.

## 12. Implementation ownership and sequence

All steps are proposed work. Obtain acceptance through the [RFC
process](README.md#process), then land the affected operator/addressing and
managed CLI/data RFC amendments before implementation changes their
contracts. Align the separately owned CP decision/spec documents before CP
implementation or packaging changes. Preserve existing identifiers; merging
this draft alone does not authorize implementation.

| Step | Owner/module | Deliverable and gate |
| --- | --- | --- |
| 1. Contract | Upstream CLI/RFC owners; CP spec owners | Amend operator/addressing and managed CLI/data RFCs, document compatibility boundary, update CP DEC/UIS/IDN/CFG references. |
| 2. Baseline fixtures | Upstream CLI tests | Capture exact legacy routes, credential choices, refusals, and accepted syntax before replacing dispatch. |
| 3. Config and resolver | `operator.rs`, `scope.rs`, command classification | Validated variants, selected connection identity, deterministic precedence; no auth/network selection. |
| 4. Managed query/mutate | Managed credential helper and `GraphClient` | Exact endpoint match, existing cache format and CC-11 transport bounds, removal of normal cwd dispatch, bounded temporary guard. |
| Accepted catalog extension | CLI command classification, scope/client helpers and integration tests | CC-18 / #653: existing served endpoint, standard addressing, stable JSON, preserved direct list/validate; managed variant separately qualified. |
| 5. Compatibility | Server/cluster library owners | Isolated public-shape/opt-in boot work with external compile and authorization fixtures. |
| 6. Consumers | CLI release and CP packaging/pilot owners | Matched scripts/config/binary rollout after tests; update upstream pins only with relevant conformance results. |
| 7. Extensions | Individual command/transport owners | Managed aliases, registry, and other HTTP verbs one qualified operation set at a time. |
| 8. Guard removal | Release owner | Remove at the published boundary after supported consumer migration; retain target/fallback tests. |

The separate framework may centralize command applicability and derive help
or matrix fixtures from existing metadata. Building a complete generated
registry is not a prerequisite for this bounded routing fix, and its future
existence must not be reported as current evidence. Do not silently normalize
unrelated legacy omissions during the refactor: schema-plan profile asymmetry,
policy/query graph defaults, offline lint's ignored flags, and read `--as`
exceptions need explicit subsequent compatibility choices.

## 13. Acceptance criteria

**CC-17 — Prove destinations, credentials, and effects.** These are required
new or adapted tests, not results. Each fixture records argv, operator config,
environment, context, credential metadata, expected request/storage target,
credential identity, refusal/effect outcome, and owning source revision.
Secrets remain test-only fixtures and are never emitted in diagnostics.

| Case | Required observation |
| --- | --- |
| Explicit staging profile inside production context | Only staging is contacted; context is not read by the migration guard. |
| Explicit server/store/root plus malformed context | Established target/capability behavior; no context failure. |
| Explicit profile plus another environment profile | Explicit profile wins with its own graph default. |
| Explicit primitive plus profile/default graph | Unused profile graph is not inherited. |
| Unknown selected profile or invalid selected target | Refuse; do not try a lower-rank default. |
| Selected managed profile plus direct maintenance | Refuse; no HTTP request or fallback storage open. |
| Implicit environment/default versus unrelated valid context | Temporary ambiguity refusal before operation effects. |
| Implicit selection and malformed context during transition | Temporary refusal; no guessed old target. |
| Implicit exact managed API/ID/endpoint agreement | Ordinary selected managed path, followed by normal credential/grant checks. |
| Missing cache in an ambiguous implicit call | Refuse; absence does not authorize legacy fallback. |
| Parent-only context | Not searched or used. |
| Explicit `--direct` with legacy HTTP and malformed context | Existing legacy request; guard never reads context. |
| `--direct` with selected managed connection | Conflict refusal before credential transmission/storage access. |
| Init with positional storage URI; load with `--store`; offline lint | Unrelated/malformed context cannot gate them; existing argument requirements remain. |
| Init addressed only by unsupported `--store` | Existing refusal preserved; do not invent a supported form. |
| Managed profile default graph versus explicit graph | Profile supplies omission; explicit graph overrides it. |
| Managed token granting one graph, graph omitted | Refuse; grant inventory cannot choose a graph. |
| Legacy registry probe: zero/one/several/denied/unavailable | Existing omission behavior retained explicitly. |
| Root-backed query and schema-plan profile cases | Existing capability refusals/asymmetry retained, not broadened by managed naming. |
| Valid #633 cached credential and exact operator binding | Reused without automatic issuance; no Intent API request during data execution. |
| Other API, cluster, endpoint, expired or malformed managed cache | Refuse before bearer transmission, even with valid ambient legacy tokens. |
| Managed entry added at an existing legacy URL | Literal legacy URL keeps its prior credential chain and transport. |
| Managed redirect, timeout, oversized JSON | Bounded failure; no redirected credential, retry to another target, or silent unbounded path. |
| Managed query/mutation connection stalls | Connection deadline remains 10 seconds within the 30-second total; no automatic retry or alternate authority. |
| Managed stored/ad-hoc query/mutation completes after 10 seconds but before 30 | One request succeeds with its complete bounded response; it does not inherit the old 10-second total deadline. |
| Managed query/mutation response headers or body remain incomplete at 30 seconds | Total request expires, including when body chunks arrive before expiry; no automatic retry, target fallback, or Intent API request. A submitted mutation's outcome remains unknown. |
| Stored/ad-hoc read and mutation | Exact action ceiling plus Cedar; stored invocation additionally requires `invoke_query`. |
| Direct actor and authenticated HTTP actor override | Direct attribution preserved; remote override refused and principal server-derived. |
| Managed schema apply or another unqualified route | Unsupported in place; no direct/API/static fallback. |
| Legacy alias with unrelated context | Existing bound destination and parameter/output rules; no global override. |
| New managed alias when its extension lands | Same selected tuple/auth rules; unsupported until explicitly qualified. |
| Served `queries list` with read/mutation entries or empty catalog | Stable documented JSON reflects the active HTTP catalog and existing metadata, not local desired files or invented descriptors. |
| Served catalog addressing and authorization | Explicit server/graph, profiles/defaults and conflicts follow the normal resolver; unknown/unauthorized graphs retain endpoint behavior; visibility does not imply invocation permission. |
| Direct `queries list` and `queries validate` after served listing lands | Existing direct helpers and capability/omission behavior preserved; validation does not acquire served execution accidentally. |
| Token mint/clear and control logout | Existing output/namespace semantics; no operator-default edit or implied offline revocation. |
| Lost mutation/run-submission response | Outcome remains unknown; no blind duplicate or alternate-target request. |
| Legacy operator file on old/new binary | Existing supported syntax and selected behavior preserved. |
| New managed variant on old binary | Fails closed for missing root; no degraded auth/storage path. |
| Mixed/misspelled new authority variant | Refuse instead of interpreting partial fields as another variant. |
| External pre-633 Rust literal/destructure fixture | Compiles after the proposed public compatibility step. |
| Known #633 Rust consumers migrated from `canonical_root`, `data_token_trust`, and `data_claims` usage | Compile and retain root binding and verified grant behavior through the additive APIs; document the exact source migration. |
| Opt-in signed boot and external managed API fixture | Correct binding; callers cannot bypass verified claims/grant ceiling. |
| Static/no-trust boot versus signed root mismatch | Existing legacy boot behavior, strict managed refusal before graph opening. |
| Control plan/apply/import/refresh, partial apply, readiness | Existing source, state/CAS, effect, and boot contracts unchanged. |
| Consumer rollout then matched binary/config/script rollback | Intended destination and credential namespace preserved; no engine downgrade, implicit retargeting, or replay of an unknown write outcome. |

Exhaust the dangerous crosses: writes × implicit selection × unrelated
context; managed selection × every credential mismatch; direct × every
connection kind; selected unsupported target × usable lower default; and
aliases/registry paths that use separate helpers. Pairwise coverage may
supplement ordinary presentation/default combinations, not replace these.

Source inspection must be followed by focused unit/integration fixtures,
external Rust compile checks, and appropriate platform checks. Keep those
results separate from any later kind/pilot/live qualification. The inspected
worktree's empty-cluster serving issue remains independently tracked; this
RFC cannot claim lifecycle completion from passing routing tests.

### 13.1 Existing evidence owners

Follow [the testing map](../dev/testing.md) and extend an existing owner before
creating a parallel harness. CLI examples need process/environment fixtures,
not graph-query logic tests alone, because cwd, keychain, precedence, and
transport are the changed boundary.

| Promise | Existing owner to extend |
| --- | --- |
| Config variants, precedence, command applicability, legacy syntax | `omnigraph-cli` in-source operator/scope/plane tests and `tests/cli_schema_config.rs`. |
| Managed context, cache, migration guard, and control compatibility | `omnigraph-cli` managed module tests and `tests/cli_cluster.rs`, using hermetic `tests/support` setup. |
| Data requests and stored catalog CLI mapping | `omnigraph-cli/tests/cli_data.rs`, `cli_queries.rs`, and `parity_matrix.rs`; preserve exact destination/credential assertions. |
| Signed/static actor and route authorization | `omnigraph-server/tests/auth_policy.rs`, `data_routes.rs`, and `stored_queries.rs`, plus data-token unit tests. |
| Root binding and legacy boot | `omnigraph-cluster` serving tests and `omnigraph-server/tests/boot_settings.rs`. |
| Wire stability | `omnigraph-server/tests/openapi.rs` and existing API serialization assertions. |
| Public Rust consumer source compatibility | Extend the owning cluster/server public-API checks with representative external-consumer compile fixtures; new setup needs an explicit ownership rationale. |

A focused clean baseline precedes future test/code changes. The accepted
#653 implementation must include the repository's issue regression marker
and the cheapest fixture that proves served listing while preserving direct
listing/validation. Documentation publication runs documentation checks only;
it does not mark any of the implementation acceptance rows passed.

## 14. Alternatives

| Alternative | Reason not selected |
| --- | --- |
| Keep cwd as another data source with a rank | Reintroduces shared folder routing into the personal data resolver and leaves former intent ambiguous. |
| Permanent context conflict gate | Preserves the blanket failure mode this correction removes. |
| Choose managed mode from a cached token or URL match | Credential state becomes routing authority and can change legacy authentication unexpectedly. |
| Put managed auth under `servers.url` | Older clients can ignore new optional auth fields and send legacy credentials; the root-less explicit cluster variant fails closed. |
| Replace all existing servers with clusters | Requires unnecessary config/script migration and lacks a direct-root-to-HTTP association contract. |
| Silently install a new default profile | Changes unrelated future commands and creates hidden migration state. |
| Immediately remove all transition checks | Can silently retarget old implicit #633 writes to ambient legacy defaults. |
| Add another managed-only data command family | Duplicates query/mutate semantics and avoids repairing the common resolver. |
| Qualify every HTTP command in the first patch | Couples streaming, partial-effect, and action coverage to the urgent routing correction. |

## 15. Unresolved questions and deferred scope

Before acceptance, settle the migration/removal boundary, public API direction
and consumer migration obligations, and ownership of any new public failure
class. The remaining operation extensions are deferred scope, not hidden
prerequisites for this correction. In particular:

- Publish the temporary guard's actual release/removal boundary and consumer
  inventory; no open-ended transition.
- Choose additive public Rust API names and migration guidance for known
  #633-field consumers, with external compile fixtures.
- Allocate any additional typed public failure classes through their owners;
  local requirement labels are not protocol codes.
- Qualify managed aliases/registry and each further HTTP operation, including
  streaming bounds, pagination, partial effects, and platform restrictions.
- Decide any future endpoint-rotation convenience, API-only cluster entry,
  token/status nickname shorthand, default cluster, or self-hosted HTTP cluster
  association separately. None is required for this correction.
- Review historical actor-setting omission and desired/effective projection
  semantics independently from routing. Do not hide a #663 schema behavior
  change inside this patch.
- Normalize existing omission/flag inconsistencies only through explicit
  compatibility decisions. The routing fix must first expose and preserve
  their current behavior in tests.

## 16. Primary source references

The links identify inspected source, not executed test results:

- [RFC 0007, operator configuration](0007-operator-config.md)
  and [RFC 0011, addressing](0011-cli-addressing-and-config.md).
- [RFC 0052, managed cluster controls](0052-managed-control-plane-cli.md)
  and [RFC 0053, offline data credentials](0053-offline-data-token-verification.md).
- [Current operator configuration and credentials](https://github.com/ModernRelay/omnigraph/blob/c253f111/crates/omnigraph-cli/src/operator.rs),
  [scope](https://github.com/ModernRelay/omnigraph/blob/c253f111/crates/omnigraph-cli/src/scope.rs),
  [plane/capability classification](https://github.com/ModernRelay/omnigraph/blob/c253f111/crates/omnigraph-cli/src/planes.rs),
  and [legacy helper behavior](https://github.com/ModernRelay/omnigraph/blob/c253f111/crates/omnigraph-cli/src/helpers.rs).
- [Managed data/cache dispatch](https://github.com/ModernRelay/omnigraph/blob/c253f111/crates/omnigraph-cli/src/managed/data.rs),
  [client construction and actor handling](https://github.com/ModernRelay/omnigraph/blob/c253f111/crates/omnigraph-cli/src/client.rs),
  and [managed control dispatcher](https://github.com/ModernRelay/omnigraph/blob/c253f111/crates/omnigraph-cli/src/managed.rs).
- [#633 public server configuration](https://github.com/ModernRelay/omnigraph/blob/e23778d9/crates/omnigraph-server/src/lib.rs#L173-L195),
  [actor representation](https://github.com/ModernRelay/omnigraph/blob/e23778d9/crates/omnigraph-server/src/identity.rs#L172-L179),
  and [snapshot root canonicalization](https://github.com/ModernRelay/omnigraph/blob/e23778d9/crates/omnigraph-cluster/src/serve.rs#L469-L485).
- [#663 omission and accepted-binding tests](https://github.com/ModernRelay/omnigraph/blob/c253f111/crates/omnigraph-cluster/src/tests.rs#L2080-L2204).
  These are source anchors for the separate semantic finding; they were not
  rerun for this proposal.
- [CP configuration](https://github.com/ModernRelay/og-control-plane/blob/main/specs/05-CFG-Config.md), [identity](https://github.com/ModernRelay/og-control-plane/blob/main/specs/06-IDN-Identity.md),
  [runs](https://github.com/ModernRelay/og-control-plane/blob/main/specs/07-RUN-Runs.md), and [decision record](https://github.com/ModernRelay/og-control-plane/blob/main/specs/02-DEC-Decisions.md).

## 17. Decision log

- **2026-09-06 — Draft prepared for upstream review.** Consolidated the
  configuration/CLI correction and local requirements CC-01 through CC-18 into
  this canonical RFC. Preserved accepted issue #653 as a separately testable
  served-list extension; related unaccepted issue requests were not imported.
  Review corrections distinguish endpoint syntax from exact binding, preserve
  explicit-profile precedence and legacy/direct exceptions, prohibit
  unsupported-target fallback, and keep partial apply/recovery/boot authority
  unchanged. No maintainer acceptance or implementation completion is recorded.
- **2026-09-08 — Narrow query/mutation deadline agreement.** CC-11 replaces its
  proposed 10-second total request deadline with 30 seconds, including the
  complete response body, while retaining the 10-second connection deadline.
  Redirect refusal, the 8 MiB response bound, offline authority, and no
  automatic retry remain required. CC-17 adds late-success and deadline
  coverage, including unknown mutation outcomes. This agreement does not
  accept the rest of this draft or record implementation completion.
