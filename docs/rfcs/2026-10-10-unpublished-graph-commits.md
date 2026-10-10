---
rfc: "2026-10-10-unpublished-graph-commits"
title: "Unpublished graph commits"
track: maintainer
status: draft
implementation: not-started
authors:
  - azimafroozeh
created: 2026-10-10
updated: 2026-10-10
discussion: null
supersedes: []
superseded_by: []
blocked_on:
  - "Demonstrate candidate identity preservation across history-buffer release and metadata-only catalog advancement."
  - "Prove candidate and retained-input safety across acknowledgement loss, branch retirement, and cleanup."
---

# RFC: Unpublished graph commits

> A term set in ***bold italics*** is defined at that spot; it is plain text afterwards.

## Summary

An ***unpublished commit*** is the complete result of one write, readable by its
graph commit id but invisible to reads of every branch. The GQ clause `as commit`
requests one. `commit publish` later installs that exact result on its recorded
target, provided the target still has the captured head and incarnation.
`commit drop` removes the result's addressability. An unpublished commit is a
short-lived proposal: it expires when its publication precondition is provably
obsolete. It is not a permanent audit record.

This RFC proposes a detached Lance version of the target branch's `__manifest`,
held by an engine-owned tag. A ***detached version*** is an immutable dataset
version outside a branch's linear sequence; Lance allocates its physical id.
Publishing the proposal writes one ordinary `__manifest` version through the
existing graph publication door. It does not replay the table effects.
Mutation declarations and branch merges are the first release. Load, schema
apply, anonymous branches, enforced approval roles, and immediate reclamation
after drop are outside this release.

## Motivation

An agent can already create a branch, write, inspect, merge, and delete it.
Current branch writes stage detached versions on the inherited datasets; they
do not fork each table. That is the supported baseline described by
[Detached-only tables](2026-09-21-detached-only-tables.md#branches).

The proposed capability makes a one-write proposal explicit without creating a
named graph branch. Its target branch and exact-parent publication condition
are fixed at creation. A caller can submit the result for inspection, then
publish or discard it without re-executing the mutation or merge. This saves
caller-side branch lifecycle steps, at the cost of a new catalog representation
and retention rules. It does not establish a performance advantage over branches.

A served write benchmark can also repeat an effectful write against an unchanged
target and inspect each returned snapshot afterwards. Pre-provisioned branches
are a viable alternative. Request counts and wall time must be measured before
claiming either approach is cheaper. Stored proposals accumulate during such a
run; dropping them is not a promise to reclaim their bytes on an idle target.

## User and operational behavior

### Write, inspect, publish

The clause follows a declaration's parameters and annotations, before its body,
or follows the merge destination:

```gq
query add_person($name: String) as commit {
  insert Person { name: $name }
}
```

```gq
branch merge "review" into main as commit
```

The declaration targets the request's branch; a merge targets its `into` branch.
An omitted target means `main`, as today. `as commit` is refused on a read-only
declaration. Stored query invocation preserves the declaration's clause; request
settings cannot override it. Snapshot targets remain read-only.

An effectful write returns the existing receipt and a `CommitOutput` with
`published: false`. A no-effect mutation or already-up-to-date merge retains
today's `commit: null` result and creates no proposal. A consumer requiring a
snapshot must reject that null result explicitly.

Read the result through `ReadTarget::Snapshot`, `query --snapshot <id>`, or the
existing read request's `snapshot` field. The read contains the captured target
state plus the write's effects. It uses the proposal's complete schema and pins.
An expired or dropped proposal is unavailable to new reads. An admitted read
retains its captured view under the same lifetime guarantee as an ordinary read;
drop and collection must not invalidate its in-flight data access.

```gq
commit publish "<graph_commit_id>"
```

```gq
commit drop "<graph_commit_id>"
```

These are control writes, dispatched through `POST /mutate` or `mutate -e`.
They take no target argument: the proposal records its target incarnation.
Publish returns the same graph commit id and inspected content. Its physical
`graph_manifest_version` changes from the detached location to the ordinary
published location; callers must use the graph commit id as snapshot identity.

### Responses and errors

`CommitOutput` gains `published: bool`, true for existing published commits.
Its `actor_id` and `created_at` describe the original write, including after
publication. `ChangeOutput.actor_id` describes the authenticated caller of the
current operation. This release does not add durable approval or publisher
provenance; applications requiring that audit record must retain it separately.

| Operation | Success contract |
|---|---|
| Effectful mutation or merge `as commit` | Existing counts and outcome; non-null `commit` with `published: false` |
| No-effect mutation or merge | Existing counts and outcome; `commit: null`; no proposal |
| GQ `commit publish` | Target branch, `query_name: "commit publish"`, zero affected counts, caller actor, non-null published commit, `outcome: {"kind":"published"}` |
| GQ `commit drop` | Target branch, `query_name: "commit drop"`, zero affected counts, caller actor, `commit: null`, `outcome: {"kind":"dropped","graph_commit_id":"<id>"}` |
| Structured publish | HTTP 200 with `CommitOutput` |
| Structured drop | HTTP 204 with no body |

Publish against a moved head returns the existing HTTP **412**
`precondition_failure` details, including the current head, when the proposal
is still available to identify its captured precondition. An expired proposal
already removed by collection returns 404. Drop of a published commit returns
409. Unknown, dropped, or unavailable ids return 404; id-based routes mask a
forbidden target as unavailable rather than disclose its existence.

A repeated publish returns the established published result when retained
lineage proves that id already published, even if the head has since moved.
It never creates a second commit. A repeated drop returns 404: there is no
tombstone distinguishing a dropped id from an unknown one. Authoritative
published lineage takes precedence over leftover proposal tags.

Candidate creation and publication use bounded exact-attempt readback after
ambiguous storage errors. Unavailable readback retains the existing uncertain
completion classification. A caller must resolve that attempt before retrying
with a new identity. Failure after durable success is not reported as proof
that nothing happened. The Design section owns the success points.

### Surfaces

| Surface | Change |
|---|---|
| `POST /mutate` | GQ carries the clause/control statements; the response additions above |
| `POST /branches/merge` | `BranchMergeRequest.as_commit: bool`, default false; refuse `delete_branch` together with it |
| `POST /commits/{id}/publish` | New bodyless structured control route |
| `DELETE /commits/{id}` | New structured drop route |
| `GET /commits?unpublished=true&branch=B` | Proposals for the selected live incarnation, ordered descending by `(created_at, graph_commit_id)`; default branch is `main` |
| `GET /commits/{id}` | Resolve a proposal or published commit; return `CommitOutput` |
| `GET /commits/{id}/changes` | Resolve a proposal before the ordinary first-parent diff; keep `CommitChangesOutput` unchanged |
| Query and stored-query read routes | Existing snapshot selector; candidate resolution is an engine change |
| CLI | `branch merge --as-commit`, `commit publish <id>`, `commit drop <id>`, `commit list --unpublished [--branch B]`; mutation uses GQ text |

Ordinary commit listing continues to walk published first parents. Proposal
listing excludes expired entries and tags whose commit already published.
Listing cost may grow with retained proposals and must be reported by the cost
instrument; random detached version numbers do not define creation order.

`GET /snapshot` and `omnigraph snapshot` keep their branch-only contract in
this release. Query-by-snapshot provides inspection. A later load extension
must separately add its option to raw NDJSON's `GraphBatchLoadQuery` and JSON
load's `IngestRequest`; neither route changes here.

### Policy and lifetime

| Operation | Cedar action | Existing scope to use |
|---|---|---|
| Candidate mutation | `change` | `branch` |
| Candidate merge | `branch_merge` | Source `branch` and destination `target_branch` |
| Publish | New `commit_publish` | Recorded target via `target_branch_scope` |
| Drop | New `commit_drop` | Recorded target via `target_branch_scope` |
| Query, commit/diff inspection, proposal listing | `read` | Proposal target branch |

The action parser, Cedar schema, configuration validation, and engine `_as`
entry points must implement these mappings before either control operation is
exposed. The server resolves the actor; clients cannot provide a trusted actor.
This is a voluntary review workflow: an actor with ordinary write rights can
omit `as commit`. Proposal-only grants and mandatory approval enforcement need
a separate policy proposal; this RFC does not imply that security boundary.

The collector's three-way lifetime rule is:

| Evidence about captured target authority | Proposal state |
|---|---|
| Same live incarnation and same logical head | Retained and publishable |
| Head provably advanced past the captured parent, or incarnation retired | Expired; cannot publish; tag eligible for removal |
| Incomplete or uncertain authority/ancestry evidence | Retained; operations fail closed until resolved |

There is no TTL. Drop removes addressability and releases the proposal's input
holds. It does not revoke the ordinary table-staging witnesses. On an idle
head, those witnesses remain live; table reclamation waits for proven authority
loss. Detached `__manifest` versions themselves are not physically pruned by
the current collector. These limits also apply to abandoned pre-tag writes.
An operator must not treat drop or `cleanup --older-than 0` as an immediate
storage-release guarantee.

## Design

### Coordinates and authoritative state

The published head is the `graph_commit` row of the latest linear
`__manifest` version on a branch's native Lance ref. There is no separate head
row to omit. Table effects already use detached commits; the new representation
is for the catalog, not the tables.

A proposal has three separate coordinates: its stable graph commit id **C**,
its captured linear catalog version **M**, and its physical detached catalog
version **D**. All version fields can represent a `u64`. Lance chooses D after
the row data has been prepared, so D cannot be written into that same immutable
batch by predicting it.

The proposed candidate codec writes complete result table rows, the complete
schema contract, and a candidate head with proposal-local row clocks M+1. Its
history buffer is empty. These clocks describe the captured publication recipe,
not the physical location. D lives in the native tag. Candidate decoding is
explicit and never inserts this row into the published history cache.

The detached catalog transaction carries a versioned
`omnigraph.unpublished_graph_commit` property containing C, the original intent
nonce, target logical name and exact incarnation, parent id, M, author/time,
captured history-release byte allowance, and exact held input coordinates/tag
names. A merge includes the source head and its lineage inputs. Validate the
property against the candidate row, schema, tag branch and tag version before
returning a view. Unknown or inconsistent metadata is an integrity error.

The sole addressability authority is the immutable native tag
`unpublished_<C>`, pointing at D on the captured native branch. The prefix and
the complete `hb1` id use characters allowed by Lance's tag validator. The tag
must be a conditional create; a conflicting value is never overwritten.
Input holds use the existing engine merge-input tag mechanism, whose witness
is the captured target incarnation/head. They preserve lineage inputs and
are not an independent proposal-existence authority.

### Create a proposal

1. Capture and validate the ordinary write authority. Use the existing schema,
   branch and table gate order for the durable effect window. Mint a fresh
   intent nonce and capture the history-release allowance for this attempt.
2. Hold exact target-parent and, for a merge, source/base catalog inputs before
   their lifecycle protection can be released. Retain their full buffered
   records and captured parent table rows through the holds, not just their
   head ids. Those table rows are inputs to history-release sizing and identity.
3. Stage and commit table effects as today. An empty staging/no-op path returns
   no commit. Compute C with the existing history-block identity algorithm
   against the captured parent, buffer and captured parent table rows. Result
   rows do not replace those identity inputs.
4. Encode the complete candidate rows and transaction property through a
   candidate adapter at the existing catalog writer. Write them detached and
   obtain D from Lance. This path does not call ordinary
   `CommitBuffer::after_publish`, `settle_closed`, or `install_head`, and does
   not use the ordinary row-clock-equals-physical-version assertion.
5. Create `unpublished_<C>` at D. Confirm the exact tag contents after an
   ambiguous response. Before acknowledgement, revalidate target authority;
   never return an already-expired candidate as publishable. A matching tag
   and valid authority establish success; unavailable readback is uncertain.
6. Transfer the input holds to the proposal lifecycle and return C with
   `published: false` and output `graph_manifest_version: D`.

No candidate enters `__history` or the branch ref. A retry after a definitive
failure starts from a fresh capture and identity. A failed or cancelled attempt
with unresolved tag completion retains its input protection; cleanup cannot
interpret uncertain completion as abandonment.

### Read and publish

Candidate lookup is a fallback after ordinary published-record resolution.
Resolve `unpublished_<C>`, validate its transaction property and target
incarnation, and apply the lifetime rule. Build the view directly from that
detached version's full table rows and schema. Do not reuse
`settled_commit_state` unchanged: its archived-schema/history assumptions are
not the candidate representation. Changes inspection resolves the held parent
view as well as the candidate view.

Publication runs under the existing control-write gates:

1. Resolve C. If retained published lineage already proves it published, return
   that result. Otherwise require its proposal tag and live target incarnation.
2. Validate `ExactGraphHead` against the captured parent, together with the
   ordinary complete-authority checks. Load held merge inputs from their exact
   coordinates, including retired native input refs when permitted by their
   existing retention contract.
3. Rebuild the ordinary publication rows against the current target catalog.
   Use the saved nonce, author/time and history allowance in `LineageIntent`,
   never the full `hb1` id as a nonce. Require the computed graph commit id to
   equal C before publication. Preserve the result pins, accepted schema and
   both parents. An identity mismatch is an integrity error, not permission to
   return a different snapshot.
4. Use current linear version N+1 for the published row clocks and perform
   ordinary history settlement, buffer release and catalog publication. M and
   N may differ after metadata-only work that preserved the logical head.
   No table effect is repeated. The normal exact-attempt readback determines
   whether the linear publication succeeded.
5. A confirmed linear publication establishes success. Run `install_head` and
   release candidate/input tags as retryable cleanup. Tag deletion failure
   cannot turn a proven publication into a failed write. Published lineage
   remains authoritative for show/list/repeat-publish/drop.

Identity preservation at buffer release, changed runtime allowances and
metadata-only advancement is an explicit evidence gate. A draft mechanism is
not proof that the current identity algorithm already meets that gate.

### Drop, retirement and collection

Publish and drop of one candidate share its target branch control gate.
Drop resolves authoritative publication first, refuses a published C, then
deletes the candidate tag and releases its input holds. Confirm deletion after
an ambiguous response before acknowledging it. Within the admitted control
topology, a completed drop prevents any later publication of that candidate;
an earlier publication wins and makes drop fail. This does not establish
distributed fencing for concurrent control processes.

The collector recognizes the reserved proposal tags and validates their
metadata. It roots their complete result pins and their exact held lineage
inputs while the lifetime rule retains them. It must not pass an empty-buffer
candidate head to the ordinary merge-base frontier as if it were published
history. It traces the captured input records instead. Marking, native-ref
validation and destructive sweep retain the collector's existing ordering and
fail-closed behavior, including admitted read protection.

Branch retirement recognizes valid engine-owned proposal tags separately from
user tags. It may retire the target under the existing control protocol;
subsequent candidate reads/publication refuse that incarnation. Cleanup then
releases expired proposal/input holds. Ordinary user tags retain their current
deletion fence. A source/base input hold must continue to protect its exact
required data through source retirement until the target proposal is terminal.

Unreferenced table versions retain their ordinary staging witnesses after
drop, expiry or a pre-tag crash. Apply the lifetime and reclamation limits
above; this RFC adds neither a dropped-id tombstone nor a revocation record.

## Invariants

- **2, graph publication:** branch visibility still changes at one ordinary
  catalog publication. The proposal tag creates snapshot addressability, not
  branch visibility.
- **3, coherent view:** the candidate contains its full schema and pins; later
  publication preserves that inspected content and validates its captured
  authority. Held inputs remain available for lineage and changes inspection.
- **4–5, acknowledgement and crash convergence:** an explicit proposal write
  acknowledges durable snapshot addressability with `published: false`.
  Ordinary writes still acknowledge durable branch publication. This is a
  scoped extension of the current “pre-publication effects are unreachable”
  wording, not a claim that the wording already covers proposals. The engine
  change must update the invariant and write/recovery guides accordingly.
- **7, derived state:** caches remain derived. The proposal tag is an explicit
  lifetime authority, not a cache; losing it removes candidate addressability.
- **10, trust:** policy gates apply in the engine and server, with the concrete
  action/scope table above. No approval-enforcement claim is made.
- **11–12, bounded work and one truth:** no candidate history scan runs on the
  normal branch read path. Tags locate immutable candidate records; published
  lineage overrides stale tags. The design adds no WAL, queue or parallel
  table history. Listing and retained-input costs require measurements.

The deny-list's acknowledgement rule is extended only for explicitly requested
unpublished snapshots. Table partial effects remain unobservable. The existing
single-writer-process boundary for branch controls and cleanup remains; local
gates are not presented as distributed fencing.

## Compatibility and reversibility

The implementation requires a new supported storage stamp for candidate
transaction metadata, the new interpretation of candidate row clocks, and
engine-owned tag lifecycle. It does not require a wider version column.
Graphs enter that stamp through creation or the explicit storage upgrade;
`as commit` does not silently upgrade a graph. Existing published commits and
their ids must survive that upgrade unchanged. Assign the concrete next stamp
in the engine change under [versioning](../dev/versioning.md).

Older binaries must refuse the new stamp before writes or cleanup. Without
that fence, today's collector retains unknown tags indefinitely and native
branch deletion treats them as user deletion blockers. “Old readers ignore
the tags” is not an adequate compatibility contract. Upgrade interruption,
reopening, old-binary refusal and export/rebuild evidence are required.

Wire changes are the commit field, two outcome kinds, merge request option,
two control routes and the unpublished-list parameter. Existing GQ files keep
their parse and behavior. Add AST variants and compiler validation; `as commit`
must not become an execution setting. Regenerate OpenAPI and the compatibility
surface inventory with the surface implementation.

Removing tags only removes proposals; it does not reverse the storage stamp or
reclaim all artifacts. Reversion uses the supported export/rebuild path after
proposals are published or discarded. No raw downgrade is promised.

[RFC 0068](0068-graph-commit-record.md) proposes a different catalog substrate.
Its reverse sequence paths and sequence-bearing records require a separate
adaptation for proposals without an allocated published sequence. That work
must preserve snapshot identity, exact-parent publication, input retention
and failure outcomes; moving this record under an `unpublished/` path alone
does not establish compatibility.

## Alternatives

| Alternative | What it buys | Why this proposal differs |
|---|---|---|
| Named branch per proposal | Existing, supports many writes, inspection and merge; no per-table first-write forks | The strongest baseline. This RFC chooses a single-write target-bound contract and fewer caller lifecycle steps, accepting additional storage complexity. No performance rejection is claimed. |
| Hidden anonymous branch | Reuses branch machinery and permits multiple writes | Adds hidden branch discovery, policy and lifecycle semantics. Defer until a multi-write requirement justifies that scope. |
| Publish then reset | Reuses ordinary writes | Readers can observe the intermediate write; reset adds concurrent-writer and audit semantics. |
| Skip the catalog write | Minimal benchmark-only effect staging | Produces no addressable snapshot for inspection. |
| Mark a linear catalog version as unpublished | Avoids detached catalog addressing | Every branch reader must skip versions and sequence/history code gains exceptions. |
| Retain proposals after head movement | Supports longer review and audit | Changes the chosen short-lived contract and needs retention independent of publication authority. Consider separately if product use requires it. |
| Durable dropped markers | Can establish abandonment on an idle target | Adds revocation authority and a reclamation protocol. The first release instead states its storage limitation. |

The precedents are detached table versions, native tags and retained merge
inputs. None supplies candidate row clocks, publish-existing identity or idle
drop reclamation automatically. Iceberg's stage-only and later publish flow
motivates the workflow; its snapshot identity and cherry-pick rules are not
imported as omnigraph guarantees. See [Iceberg procedures](https://iceberg.apache.org/docs/1.10.0/spark-procedures/#publish_changes).

## Evidence and tests

No implementation or request-cost evidence is claimed by this draft. Extend
the owners in [testing](../dev/testing.md):

- GQT: effectful mutation and merge proposals, target unchanged, candidate
  snapshot rows, later publication, competing proposal refusal, no-op null
  receipts, read-only declaration refusal, and drop of a published commit.
- GQT result transport: `--- bind commit $name` binds the preceding successful
  write's non-null id. Duplicate or unbound names fail. Bindings last for the
  case, including restart. `snapshot: $name` resolves a read target. A quoted
  whole commit-id operand `"${name}"` in a commit control statement substitutes
  that bound id before dispatch; no arbitrary GQ text substitution is added.
  The engine receives an ordinary literal. Test two separately bound proposals.
- Catalog tests: candidate codec/transaction property, tag names and exact
  readback, full/empty history buffers, source/base input retention, identity
  preservation, changed history allowances, and physical-only advancement.
- Existing branching/point-in-time/change owners: list/show/changes contracts,
  target retirement/recreation, source retirement while a candidate holds it,
  and admitted snapshot reads across drop/collection. List checks use the
  engine API and HTTP/CLI conformance; no GQ list statement is implied.
- Maintenance and the collector oracle: Live/Dead/Undecidable cases, unknown
  tags, user-tag deletion fences, result and input roots, stale published tags,
  and retention of idle-head abandoned staging. Assert the documented absence
  of immediate catalog-version reclamation rather than promising deletion.
- `detached_commit_matrix.rs` and existing failpoint owners: before/after the
  detached catalog write, tag create, publication and tag removal; lost
  acknowledgements, unavailable readback, restart and retry. DST adds proposal,
  publish and drop operations within the admitted control topology.
- Policy and served conformance: every action/scope/actor mapping, forbidden-id
  masking, 412 details, both structured/GQ control doors, and no bypass through
  embedded `_as` operations or stored-query invocation.
- Public write-cost owners: count detached writes, tag create/lookup/delete,
  retained inputs, normal publication and history settlement separately. Compare
  direct write, branch workflow and proposal workflow at the same fixture;
  no equality tolerance or performance win is assumed.
- Storage upgrade owners and `forbidden_apis.rs`: stamp transition/refusal,
  retained published identities, export/rebuild, and the existing sealed catalog
  write boundary.

Acceptance requires all conformance cases to pass in-process and served,
zero oracle violations in the fault/collector matrix, exact candidate-id and
content preservation, and checked-in cost records covering both history-buffer
regimes. Measured requests inform the decision; wall time is report-only.

## Rollout

1. Review the RFC and its two evidence gates. No engine capability ships with
   this documentation change; `implementation` remains `not-started`.
2. Engine/language change: candidate representation, storage stamp/upgrade,
   writer and reader adapters, retained inputs, publication/drop/collector,
   compiler grammar/AST/validation, in-process control dispatch, policy actions,
   receipt types and GQT result bindings. Require in-process GQT, fault,
   compatibility and cost evidence before `implementation: partial`.
3. Surfaces change: server and CLI adapters, structured merge option, control
   routes, list parameter, OpenAPI and compatibility inventory, served
   conformance, user branching/mutation docs and release note. Mark
   `implementation: complete` only when these are green.
4. Consumers may adopt the workflow after their required surface ships. A served
   mutating benchmark and any later load extension have their own scope and
   evidence; neither is silently counted as delivered here.

At stop 2 the embedded engine and GQ support the operation. At stop 3 the
specified served and CLI surfaces do. Load and schema apply remain excluded.

## Unresolved questions

Azim, as RFC author, and the accepting maintainer decide before acceptance
whether short-lived proposals and deferred idle-head reclamation meet the
product requirement. If proposals must outlive head movement or drop must
release storage on an idle graph, revise lifetime and its evidence gate before
implementation; neither behavior is included implicitly.

## Decision log
