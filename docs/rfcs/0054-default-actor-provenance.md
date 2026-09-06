---
rfc: "0054"
title: "Default graph actor provenance"
track: maintainer
status: accepted
implementation: complete
authors:
  - Codex
created: 2026-09-06
updated: 2026-09-06
discussion: https://github.com/ModernRelay/omnigraph/issues/661
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC 0054: Default graph actor provenance

## Decision

New graphs provide a queryable actor node by default. Graph owners can turn
automatic materialization off through the same planned schema configuration
that owns the graph. Existing graphs keep their current behavior until an
explicit migration enables it. This implements the direction in managed
control-plane DEC-08-27 without introducing a managed-only engine path.

The actor node is an identity anchor for commit attribution and application
provenance relationships. It does not grant access, enroll a Cedar principal,
copy a user profile, or add automatic provenance edges to every changed row.
Existing commit attribution remains present independently of this feature.

## Identity and schema

The engine-owned logical node is `OmniActor` with exactly one user-visible
property, `actorId: String @key`. Its value is the exact actor identifier
already supplied to the authorized engine operation. Normal graph row identity
and stable schema identity rules apply. The managed server derives
`principal:<sub>` from verified data-token authority under
[RFC 0053](0053-offline-data-token-verification.md); token renewal therefore
reuses the same actor. A served caller cannot select a different trusted actor
through a query parameter or header.

When automatic actor provenance is enabled, an actor identifier is 1–1024
UTF-8 bytes and contains no ASCII control characters. Invalid attributed
identities refuse before graph effects; legacy and opted-out graphs retain
their existing opaque commit-attribution strings. The
engine does not trim, case-fold, hash into a different public identity, or
infer an identity from an email or display name. Unattributed operations retain
`actor_id: None` and create no actor node. Direct writers remain self-asserted;
an `OmniActor` row alone makes no assertion that the identity was authenticated
or that it represents a human. Authentication assurance remains the property
of the credential and execution boundary that established it.

`SchemaIR.actor_provenance` is the sole persisted configuration authority. Its
optional binding contains `enabled`, the stable node type ID, table incarnation
ID, and `actorId` property ID. A binding requires schema IR version 3, including
when disabled. Version 2 without the binding retains its exact legacy meaning
and hash. Readers support both versions; older readers that support only
version 2 refuse version 3 instead of silently ignoring the feature.

The compiler constructs the built-in through typed schema structures and
validates its exact shape and identity. A deterministic augmentation step is
shared by initialization, schema evolution, source/IR validation, catalog
construction and schema inspection. The source schema remains customer-owned;
the accepted schema and catalog include the built-in. No separate mutable
registry or configuration sidefile duplicates this authority.

Graph inspection exposes unchanged customer source separately from the full
accepted schema and binding. Query lint against a graph uses that accepted
catalog. Generic source-only compilation and offline `lint --schema` remain
unbound; use a graph target to validate queries or customer edges that refer
to the built-in. The source file alone cannot establish accepted ownership.

`Actor` remains available for any customer-defined domain type. The engine
does not adopt an unbound `OmniActor` declaration by its spelling: a collision
on initialization or explicit enablement refuses with a schema diagnostic.
The first release does not support custom actor-table bindings, renaming the
built-in, extra required fields, or replacing its key. An owner can rename a
conflicting domain type through the existing explicit migration path first.

Ordinary query mutations, bulk input and other direct row-writing entry points
cannot insert, update, overwrite or delete the bound actor table, even when
automatic creation is disabled. Schema evolution retains its identity and
shape. Application edges may reference it through ordinary schema and
referential-integrity rules; those edges convey no authorization.

## Configuration and lifecycle

The following fields express desired configuration; they are not additional
runtime authority:

- `InitOptions.actor_provenance: bool` defaults to `true`.
- `SchemaApplyOptions.actor_provenance: Option<bool>` defaults to `None`,
  which preserves the accepted setting.
- Each graph in `cluster.yaml` accepts optional `actor_provenance: true|false`.
  Omission uses the initialization default for a new graph and preserves the
  accepted setting for an existing graph.
- Direct CLI init and schema plan/apply expose the same optional
  `--actor-provenance true|false` intent. The plan displays a setting change
  even if no customer table changes.

An explicit false on new initialization omits the built-in. Enabling an
existing graph is an ordinary visible schema migration that provisions the
empty bound table. It does not backfill historical actors from the commit log.
Disabling a previously enabled graph changes the accepted setting while
retaining the table, existing rows, identities and history. Re-enabling reuses
them. Neither transition requires a data-plane call to the control plane.

Configuration changes use existing schema serialization, branch preconditions,
recovery and publication. A stale planned configuration does not override a
different accepted schema. Managed changes still use exact plan/apply, drain,
termination proof and controlled boot; no online hot toggle is added.

## One atomic content write

After authorization and successful evaluation of a data-changing operation,
the writer resolves the binding and actor against that attempt's accepted
schema and branch snapshot. If enabled and attributed, it stages a missing
actor as a protocol-owned participant in the same write transaction. The actor
table uses the existing keyed storage adapter and validation boundary. Lookup
is an exact-key query with a bounded result, not a scan of commit history.

The actor participant joins the operation's normal read set, staged effects,
durable recovery ownership, manifest delta and fixed commit lineage. Actor,
data and attribution become graph-visible through one manifest publication.
No actor-only preparatory commit is permitted. Retries start from a fresh
accepted view under the existing bounded retry contract; repeated or concurrent
first use cannot publish duplicate actor identities.

The constructive/destructive split still applies to the caller's query.
An automatic actor insertion is a declared protocol participant on a protected
table that the caller cannot also modify. A delete-only query may therefore
stage deletions in customer tables and a missing actor in the disjoint actor
table. No table mixes inserts with deletes, and the query grammar continues to
refuse mixed constructive/destructive statements. This distinction must be
explicit in the staging contract and proved by first-use deletion tests.

Validation failure, authorization refusal and zero-effect mutations do not
create actor nodes. Reads, token issuance and login never write graph content.
An existing empty-load or metadata-only lineage event does not become a content
write merely to materialize an actor. A post-effect failure retains the existing
recovery-required semantics; reopening either publishes the complete owned
outcome or refuses, never exposes an actor without its associated content.

## Branches and other writers

Actor nodes are branch-scoped graph content. A branch inherits the actor rows
visible at its fork. First use on that branch creates the actor only there,
using the same branch identity and first-touch recovery as customer tables.
Merges preserve those identities and cannot forge or remove protected actors.
An attributed content-changing merge must also include its initiating actor.

Every public content-writing path must either include the actor in its owning
atomic transaction or refuse before effects when materialization is required.
This includes mutations, all bulk-load modes, table administration, schema
migrations that rewrite content, and branch merge. A transport wrapper may not
perform a second write to supply a missing actor. Metadata-only operations,
initialization, schema configuration and physical maintenance do not invent
content changes or an attributed actor.

The first implementation may explicitly refuse an otherwise supported control
operation on an enabled graph if that writer cannot yet carry the required
actor participant. Such refusals must be documented at the affected public
entry point and covered by state-neutrality tests; they must not strand an
already-started mutation, disable existing authorization, or silently fall back
to untracked writes. Unsupported behavior is never evidence of full coverage.

## Compatibility and rollout

Existing graph schemas, actor strings and version-2 stores remain readable.
Graphs with a version-3 binding require a compatible writer even when
materialization is disabled. Opt-out is not a downgrade operation. A complete
graph-state restore preserves the accepted schema binding and actor content.
Ordinary data exports include actor rows under their normal selection rules,
but are not a provenance-preserving restore format: replaying protected rows
through raw data import refuses, and must not silently drop them.

Rollout order is engine contract and tests, transport/config plumbing, then a
coordinated managed dependency/image update and explicit pilot migration.
The control plane retains no graph data authority and makes no request-time
identity lookup on behalf of the server.

## Validation

Extend existing owners in [testing](../dev/testing.md):

- Compiler and schema lifecycle: default init, explicit false, legacy version-2
  opening/hash stability, version-3 validation, name collision, exact binding,
  planned enable/disable/re-enable, retention and source/IR agreement.
- Writes and policy: first insert/update/delete, repeated and concurrent actor
  use, all load modes, missing identity, identifier bounds, protected-table
  refusal, denied/no-op state neutrality and exact commit actor attribution.
- Branching: fork inheritance, first-use isolation, merge behavior and
  unsupported-operation refusal before any branch or table effect.
- Recovery/failpoints: before arm, partial table effects, before publication,
  acknowledgement ambiguity and reopen; actor and customer content share one
  authoritative outcome.
- Server/CLI: verified actor resists spoofing, renewed data tokens reuse it,
  graph policies remain mandatory, options reach the same Core contracts.
- Managed pilot: use the native authenticated CLI, verify actor and commit
  identity agree, exercise repeated writes and opt-out, and retain generated
  sanitized evidence bound to the tested source and images.

The implementation status may become complete only after the supported and
refused write surfaces are explicit and their owning tests pass. Pilot
qualification does not establish unrelated writer-fencing or isolation gates.

## Alternatives

Creating nodes during login or token minting would make authentication mutate
arbitrary graphs and require graph-storage authority in the control plane.
Creating one in a separate preparatory mutation would leave false provenance
after a rejected or failed write. Automatically reusing a customer `Actor`
would assign system meaning to arbitrary keys and required fields. Deriving a
table from the full commit log on every query would add unbounded work and
make retention part of identity lookup. The accepted schema binding and one
existing publication protocol avoid those costs.
