---
rfc: "2026-10-11-unattached-graph-branches"
title: "Unattached graph branches"
track: maintainer
status: draft
implementation: not-started
authors:
  - azimafroozeh
created: 2026-10-11
updated: 2026-10-11
discussion: null
supersedes: []
superseded_by: []
blocked_on:
  - "A named-branch baseline of the chained `tiny_mutate` workload under the RFC 0039 micro profile, reporting the share of requests and time in branch-head publication and history settlement, recorded before implementation starts."
  - "A validated version 2 codec and retention protocol for nonpublishable graph snapshots, including upgrade and fault evidence."
  - "The accepted Unpublished graph commits RFC's collector retention and reclamation coverage and served expiry reruns recorded, or a statement in Evidence and tests of which of this RFC's cases supply them."
---

# RFC: Unattached graph branches

> A term set in ***bold italics*** is defined at that spot; it is plain text afterwards.

## Summary

An ***unattached graph branch*** is a set of immutable graph snapshots, each with exactly one parent, rooted at a named graph branch's head; writes to it never advance that named branch. It is not a catalog branch: it has no name, no head and no entry in `branch list`; the word names the caller's view of successive snapshots. This RFC adds support for benchmark workloads that write, inspect the result, write again on that result, and discard the work. Ordinary reads of the named branch see none of these changes. Actors with `read` on that branch may list the unattached snapshots and read them by id; commit show by id keeps its existing scope-free `read`. This is not actor-private storage.

Each effectful write returns a new immutable snapshot id. The caller keeps the latest id as its ***tip***, the snapshot on which its next write will run. There is no separate branch name or server-maintained mutable tip. Two writers using the same base each get their own new snapshot; neither sees the other's write. The GQ clause `as unattached branch`, optionally followed by `on $base` or `on "<id>"`, requests one. A snapshot expires when its named branch's head advances or the branch is retired; `commit drop` removes a snapshot and every descendant.

Publication, attachment and merging these results into a named branch are outside this RFC. The accepted [Unpublished graph commits](2026-10-10-unpublished-graph-commits.md) RFC (implementation partial; the sibling RFC below) defines a separate single-write kind that can publish. The new kind is nonpublishable from its first write. Its scope and identity rules do not promise a future publication protocol.

## Motivation

A benchmark sometimes needs a stream of writes that builds on its own preceding results while leaving its named starting branch unchanged. A single `as commit` proposal cannot currently be extended. A named scratch branch can run that stream and remains the baseline for ordinary branch writes.

The workload this RFC serves is the served-write benchmark's chained variant: the `tiny_mutate` fixture with N successive effectful writes, each reading the snapshot the previous write returned before writing again, on one named starting branch. Per-phase attribution of the same stream on a named branch (the RFC 0039 micro profile) reports how much of each write is branch-head publication and history settlement. It cannot report what the remaining phases cost when those phases are absent, because publication changes the state the next write runs against: the history buffer grows, the linear head moves and staging witnesses (the `staged_against_*` properties that keep a staged table version from reclamation) retire. This RFC supplies that second measurement; the attribution run stays the baseline.

This RFC adds the narrower benchmark operation: detached table writes (table commits outside a branch's linear sequence, per [Detached table commits](0067-detached-table-commits.md)) followed by addressable graph-snapshot metadata, with no named branch-head publication or ordinary published-history settlement. It measures that operation, not normal branch-write latency. No speed advantage is assumed.

A named branch appears in branch listings, occupies a logical name, requires `branch_create`, and has an explicit create/delete lifecycle. Creation and deletion manage native branch authority; they do not append ordinary graph create/delete commits to the target branch's published lineage. Avoiding that lifecycle is a scope difference, not evidence that the new path is cheaper.

## User and operational behavior

### Create, extend and inspect

The proposed GQ clause selects the new kind explicitly:

```gq
query add_person($name: String) as unattached branch {
  insert Person { name: $name }
}
```

```gq
query add_address($id: String, $base: String) as unattached branch on $base {
  insert Address { owner: $id, city: "Delft" }
}
```

The first write creates a ***root snapshot***, whose base is the request branch's head as read at creation (its captured head). That named branch is the ***recorded target***: its captured head and the branch's exact incarnation (its native ref identity) govern authorization and lifetime. Omitting the request branch on a root write selects `main`.

The second write creates a ***child snapshot*** from the named unattached snapshot's complete schema and table pins. Its `parent_commit_id` is the base snapshot id. It inherits the root's recorded target; an unrelated request `branch` cannot select another target. A child contains its ancestors' effects and its own, never a sibling's. The server authorizes `change` on the recorded target first; a denial answers 404. An authorized request whose `branch` names another branch is then refused with 400; an omitted `branch` is accepted. `ChangeOutput.branch` carries the recorded target.

An effectful write returns `CommitOutput` with `kind: "unattached"`, `published: false`, a fresh id and its parent. `graph_branch` is the recorded target, `null` for `main`, on every unattached receipt and list row. A no-effect write returns `commit: null` and creates no snapshot. After a no-effect child write, the caller retains its previous tip. A no-effect root write creates nothing. Read-only declarations refuse the clause.

A child base must be a Live unattached snapshot; a published, version 1 (the sibling RFC's record layout, Design below) or merge-proposal base is refused with 400. The base operand is a `String` parameter (`on $base`) or a quoted literal (`on "<id>"`); stored-query invocation preserves the declaration's clause and receives the base per invocation; request settings cannot change its mode. Snapshot selectors remain read-only; the explicit clause is the new write entry point.

Reads use `ReadTarget::Snapshot`, `query --snapshot <id>` and GQT `snapshot: $name`. Reads still validate the shared lifetime rule in Design, including ancestry evidence. A read holds its immutable snapshot (invariant 3) and the collector does not reclaim a detached `__manifest` version, so a read admitted before a drop (an admitted read is one that passed the lifetime check) completes on the version it opened.

### Discard and publication refusal

`commit drop "<id>"` on an unattached snapshot removes its addressability and that of every unattached descendant. Dropping the root discards every child, including alternative paths created by concurrent extensions. It never deletes a named branch or a published graph commit. After dropping one child, siblings remain usable while the root remains Live. `commit drop` of an Expired unattached id succeeds and removes its tags, as for expired proposals; only an Undecidable id refuses the drop. Transitive drop is an operational-cost choice, not a correctness requirement: it leaves no orphan Undecidable records.

`commit publish "<id>"` and the structured publish route refuse every unattached id before durable effects with HTTP 409 and an error code distinct from the authority-moved 409; the body is `{"error": "commit '<id>' is unattached and cannot publish"}` on HTTP and GQ alike. Neither a root nor a child can publish. Existing one-write proposals keep their current publish and drop behavior. This is a kind-specific contract, not an approval policy or a restriction on ordinary writes.

### Responses, discovery and failures

`CommitOutput` gains `kind: "published" | "unpublished" | "unattached"` and, for `kind: "unattached"`, `root_commit_id` (the root's own id on a root). Existing `published` is retained: true exactly for `kind: "published"`. `kind` is required on the wire from this RFC's first release; a client reading a receipt without it derives `published` from `published: true`, else `unpublished`. The distinction must appear in receipts, show/list results and CLI rendering; `published: false` alone does not tell clients whether an id can publish. CLI exit codes follow the existing `command_outcome` classification; this RFC adds no code.

| Operation | Contract |
|---|---|
| Effectful unattached write | Existing counts; non-null unattached commit; `parent_commit_id` is the captured effective head (the inherited head on a fresh branch) or the supplied unattached id; it is never null; `merged_parent_commit_id` is null |
| No-effect unattached write | Existing counts; `commit: null`; no snapshot created |
| Drop unattached id | GQ: existing `dropped` outcome; `DELETE /commits/{commit_id}`: existing 204. Both only after all descendants and that id lose addressability |
| Drop Expired unattached id | Succeeds and removes its tags, as for expired proposals; only an Undecidable id refuses the drop |
| Partial drop | 503 with `error`; the requested id stays addressable; retry the same id |
| Publish unattached id | HTTP 409 with the distinct error code and text above; no publication or other durable effect |
| List with `unattached=true&branch=B` | Live unattached snapshots for B's current incarnation, descending by `(created_at, graph_commit_id)` |
| List with `unpublished=true&branch=B` | Existing unpublished commits only; the proposal listing skips the `unattached_` prefix by name, so an unattached tag never fails or appears in `unpublished=true`; `commit_target_branch` resolves both prefixes |

`unattached=true` with `unpublished=true` answers 400. Default commit listing remains published history only. The unattached list is flat; `parent_commit_id` identifies the paths. A parent outside this list is not proof of corruption: the root's parent is a published commit. Expired snapshots are not listed and have no enumeration route; their retained storage is reported by the collector's statistics only.

Unknown, Expired or dropped bases return 404. Expired and dropped ids answer 404 whose `error` contains `not found`; the expired text additionally says `expired`. A visible base of the wrong kind returns 400. An Undecidable lifetime answers 400 with an `error` naming the missing evidence, as an undecidable unpublished commit does today; it is never a 404 and is not proof the id is absent. Id-based routes return 404 for a forbidden recorded target before disclosing kind or lifetime. A repeated successful drop returns 404, as for existing dropped proposals.

Creation acknowledges a snapshot only after confirming its tag and revalidating authority. A lost response has no readback route; the runner drops every snapshot it cannot attribute. A later failure must not convert an earlier durable effect into `BeforeEffect` evidence, the completion class that says nothing durable happened. Drop may remove some descendants before an error; retry the same requested root and preserve uncertainty about unconfirmed removals. A 404 alone is not evidence that an earlier request had no effects.

### Surfaces and policy

| Surface | Change |
|---|---|
| GQ and `POST /mutate` | New declaration clause and optional base operand, a `String` parameter or a quoted literal; stored invocation retains the clause and receives the base per invocation |
| `POST /branches/merge` | Unchanged; no unattached merge mode |
| Commit show and query-by-snapshot | Resolve the new kind and apply its lifetime; query-by-snapshot applies the recorded-target read policy; show keeps its existing scope-free `read` |
| Commit publish/drop routes | Publish refusal by stored kind; transitive drop only for unattached ids |
| Commit listing and CLI | New `unattached` filter and `commit list --unattached`; existing proposal filter unchanged |
| GQT | Existing commit bindings and snapshot selectors. The parameter form `on $base` needs no substitution. For the literal form, GQT scans a `--- mutate` declaration for a whole quoted `on "${name}"` operand, requires the binding and substitutes it before compiling, as the commit-statement arm does; the engine receives an ordinary literal and no other GQ text is substituted |

GQT commit bindings, as the sibling RFC defines them, are immutable and require a non-null successful receipt. Bind successive effectful writes under distinct names; no implicit rebinding or null tip is introduced.

| Operation | Cedar action | Context |
|---|---|---|
| Root or child write | `change` | Recorded target as `ResourceScope::Branch`, using existing `branch_scope` grants |
| Read, diff and unattached list | `read` | Recorded target branch; show keeps its existing scope-free `read` |
| Drop | `commit_drop` | Recorded target via `target_branch_scope` |
| Publish attempt | `commit_publish` | Existing recorded-target authorization and masking, followed by kind refusal |

Resolve a child's recorded target with `commit_target_branch` before authorization, schema capture, admission and operation gates. The engine and served entry points must use the same target. `ChangeOutput.branch` is the recorded target; its description changes accordingly. The mode adds no `target_branch_scope` support to `change`. Listing and snapshot reads require `read` on the recorded target; `GET /commits/{commit_id}` keeps its existing scope-free `read`.

## Design

### Version 2 records are not version 1 layouts

Use a version 2 variant of the `omnigraph.unpublished_graph_commit` transaction property with `kind: unattached`. Version 1 remains the existing publishable-proposal layout. Every new root and child has the new kind; extending version 1 is refused. Dispatch validation and publication by the stored version/kind, never by an id's spelling alone.

A snapshot has two coordinates: its graph commit id C and the physical detached `__manifest` version D that Lance allocates. The sibling RFC's third coordinate M, the captured linear `__manifest` version, is not part of a version 2 record.

Each record contains the id, the root id `root`, the immediate parent id `parent`, complete result table rows and schema, and author/time. A record is a root exactly when `root` equals its own `graph_commit_id`; no separate root/child discriminator is stored. A root additionally captures the recorded target's logical/native branch, exact incarnation and effective published parent (the head the branch inherited or last published). A child record repeats the root's logical/native branch, incarnation identifier and effective parent, and adds `root` and `parent`. Validation checks the repeated coordinates against the head row and the tag; agreement with the root is checked when the root is resolved. A root or child creates no input hold. The base candidate's tag roots its result pins in the collector while it is Live or Undecidable, and transitive drop removes descendants before their base, so no snapshot outlives its input view.

A ***detached version*** is an immutable Lance dataset version outside its native branch's linear sequence. Store the complete candidate in a detached version D of the root's recorded target `__manifest` branch, and address it through the conditionally created native tag `unattached_<root>_<C>`, the ***candidate tag***, under a second reserved prefix that carries the root id. The tag's branch/version, property and candidate rows must agree. A tag whose value differs from the attempt's (branch, D) is never overwritten. Candidate tags and detached versions are durable: ids and tags survive process restart. Detached names a Lance version; unattached names the graph kind.

### Every snapshot has a fresh ULID and no row clock

Mint C with `omnigraph_core::dst_ids::new_ulid()` per effectful attempt. The new validator requires C to be canonical. Version 2 records carry no `intent_nonce`, `history_release_bytes`, `expected_table_versions` or `merge`; C is the attempt identity. Existing public snapshot-id validation already accepts canonical ULIDs; the candidate tag's two-id name needs its own name validation, and the candidate codec adds the version-specific rule because version 1 requires `hb1` ids, the prefixed history-block ids the existing identity algorithm computes.

Do not run the history-block id allocator on an empty candidate buffer. These ids have no published-history block or slot. The unattached record has an empty history buffer because it carries a full snapshot, not a published history segment. Creation performs no ordinary history settlement, buffer release or `install_head`.

Version 2 records carry no row clock. The head row's `graph_manifest_version` carries D as the physical coordinate, and `CommitOutput.graph_manifest_version` carries D for `kind: unattached`, as for proposals; identity is C. Never predict D.

Never reuse C as a published write's intent nonce. Existing nonce aliases can resolve a nonce to a later `hb1` id; such reuse would make snapshot identity ambiguous. Future attachment must define its own identity and conversion contract and cannot reinterpret these records as version 1 publishable layouts.

### One lifetime rule for every operation

Resolve the retained root and parent path, validating kind, parent linkage, root id and target agreement. Detect cycles and inconsistent metadata as integrity failures. Missing required ancestry is uncertain evidence, not permission to delete data. Apply the following rule to creation, reads, show/diff, listing, drop eligibility and collection:

| Evidence about the recorded target | Unattached state |
|---|---|
| Same live incarnation and the same logical head as captured (the inherited-head case counts as the same head) | Live; readable and extendable |
| Proven head advance or incarnation retirement/replacement | Expired; unavailable to new reads/writes; tags eligible for cleanup |
| Incomplete authority or ancestry evidence | Undecidable; retain and fail closed |

Preserve the inherited-head case: a fresh named branch can inherit an effective parent while its exact materialized head is absent. Do not replace that absence with an unattached id. Metadata-only changes that preserve required authority do not by themselves prove expiry.

The root's authority never moves as children are added. Target activity can still expire an entire benchmark run; the runner must report that failure, not silently recreate its base.

Full snapshot rows eliminate mutation replay, not the ancestry/lifetime checks. A branch read never opens a candidate tag. Listing and collection may share validated ancestor results within one operation, but caches are never lifetime authority.

### Creation revalidates the root's authority before acknowledging

1. Resolve the selected declaration and mode; refuse read-only declarations. The engine parses the selected declaration before its policy gate and before `open_write_txn`; lowering keeps its place. A child's recorded target comes from `commit_target_branch(base)`, the call `commit_control_body` already makes before authorization; `POST /mutate` resolves it the same way and authorizes `change` on it; a lookup failure answers 404 before authorization.
2. Authorize the recorded target and mask forbidden ids. Then validate the base kind and lifetime, resolve its root, and capture the selected view. Acquire the existing schema gate, the recorded target's control gate and the table gates in their established order. Under those gates, revalidate the root authority (`revalidate_write_txn`) and re-read the base's tag and lifetime; a base dropped or Expired since its validation earlier in this step refuses the write before any detached table commit. Creation, drop and collection retain the existing single-writer-process boundary for branch controls and cleanup.
3. Keep the base snapshot separate from write authority. Evaluate a child against its base snapshot's schema and table pins. Authority checks and detached-table staging witnesses use the root's captured real branch head/incarnation, including inherited-head absence, never the child's unattached parent id.
4. The candidate writer loads the live linear head for authority and stamp. For a child it separately checks out the base's own detached version (the base candidate's D) for its table rows, schema contract and head row, and `check_expected_table_versions` and `apply_changes` run against those rows; for a root the live linear head is the base and the single load suffices. Stage detached table effects using the checked base table versions. For an effectful result mint C and encode its complete rows, schema and version 2 property.
5. Write the detached candidate and conditionally create its candidate tag. Confirm ambiguous storage responses with bounded exact-attempt readback: the creator re-reads its own tag to learn whether this attempt landed. Revalidate root authority before acknowledging. The post-tag probe for the new kind compares the live incarnation identifier and exact head with the root's captured coordinates, not the linear version number.
6. If revalidation fails after tag creation, release the candidate tag and classify the attempt from confirmed effects. After an authority mismatch, a failed release of the new kind's tag is reported as `manifest_publish_in_doubt`, as the probe-failure arm does; the version 1 arm is unchanged. A confirmed tag plus valid authority establishes success; return C.

### Drop and cleanup remove descendants before ancestors

Under the recorded target's control gate, re-resolve the requested unattached id and capture its descendant set from the candidate tags named under its root. Child creation acquires the recorded target's control gate before its first detached table commit, so a new acknowledged child cannot escape the drop scan. This ordering holds within the single-writer-process boundary; it is not distributed fencing, and a second control process is outside the support boundary. Process descendants before parents; confirm tag removal before proceeding upward. A completed root drop means no descendant remains newly addressable. Preserve admitted reads throughout.

If any removal fails, keep the requested parent until its descendants' removals are confirmed, preserve completion evidence, and allow retry. A dropped ancestor that is neither published nor present as a validated unattached record leaves an orphan; retain it as Undecidable and report it. Missing metadata is not a deletion proof.

The collector roots every Live or Undecidable snapshot's complete result pins. It must not treat an empty-buffer unattached record as published history or pass it to the ordinary merge-base frontier. Expiry cleanup runs under the collector's existing control gate; the release loop orders unattached tags descendants-first and stops an ancestor's release on any unconfirmed descendant. Retain an ancestor until every descendant's removal is confirmed; an Undecidable descendant also blocks ancestor removal. A crash must not erase the ancestry evidence needed to finish cleanup.

Drop does not promise immediate physical storage release. Table staging witnesses may remain live on an idle target, and the current collector does not prune detached `__manifest` versions.

## Invariants

- **2, graph publication:** unattached operations never advance a named branch head.
- **3, coherent view:** each snapshot contains complete schema and table pins; lifetime validation and admitted-read protection remain required.
- **4–5, one publication and crash convergence:** successful creation means durable snapshot addressability, not publication. Creation and drop retain exact-attempt completion evidence and, for drop, per-descendant removal evidence. The deny-list's acknowledgement rule and invariant 5 name one exception, the publishable proposal. This RFC adds a second, nonpublishable one: acceptance rewrites invariant 5's exception sentence to name both kinds and adds the unattached writer row to the write-protocol guide's adapter table.
- **6, stable identity:** C is the snapshot's identity, minted fresh per effectful attempt; it is never reused as a published write's intent nonce, and identity is never inferred from a tag name, path or Lance version D.
- **8, loud integrity failures:** a version 2 record with an unknown kind or layout is refused, never reinterpreted; the storage stamp fences an older binary before it writes or cleans.
- **10, trust:** the policy table defines the recorded-target checks; this mode grants no actor privacy or approval enforcement.
- **11–12, bounded work and one truth:** ancestor and descendant traversal is cycle-checked and subject to existing request cancellation/resource limits; its cost grows with retained work. Descendant discovery lists candidate tags by name under the root's `unattached_<root>_` prefix without decoding them, and the proposal listing's cost stays independent of the unattached population. No second publication authority is added. Immutable records and candidate tags own snapshot addressability; ids and tags are durable across process restart; the caller's latest id is a mutable-tip hint, never durable server authority.
- **13, evidence matches the boundary:** every cost claim in Evidence and tests comes from a checked-in cost record.

## Compatibility and reversibility

The new record kind takes the next storage stamp on each branch's linear `__manifest`. A graph at the current stamp is served as it is; each branch is stamped by its first publish under the new binary or by `omnigraph upgrade`'s stamp-only route; `as unattached branch` on a ref below the stamp is refused naming `omnigraph upgrade`; `MIN_SUPPORTED` is unchanged. Older binaries refuse the new stamp before any open. The concrete stamp and supported source versions follow [versioning](../dev/versioning.md).

Version 1 proposals retain their codec, identity and lifecycle behavior. The upgraded reader supports both kinds, and publish dispatch refuses version 2 unattached records rather than replaying them through the version 1 writer. Without the fence a v15 binary that meets a version 2 record fails the operation, a whole cleanup run included, and retains the tag; the property alone cannot refuse an old binary before it writes or cleans.

Wire changes are the new clause/base operand, `CommitOutput.kind`, `CommitOutput.root_commit_id`, the unattached list filter, and kind-specific publish refusal/drop behavior. Existing GQ files, ordinary published commits and `as commit` proposals keep their behavior. Existing unpublished listing is not broadened.

Dropping unattached work does not downgrade the storage stamp or guarantee artifact reclamation. Reversion uses the supported export/rebuild path for the retained published graph. Export of unattached branches and migration into any future attachable representation are not promised.

## Alternatives

| Alternative | What it buys | Decision for this scope |
|---|---|---|
| Named scratch branch | Existing multi-write/read/delete workflow and ordinary publication semantics | Required baseline for normal branch-write cost; use it when a workload does not need the detached operation separately. |
| Per-phase attribution of a named-branch write (RFC 0039 micro profile) | Separates publication and history settlement from table staging on the existing path, with no new record kind | Required baseline; it measures phases inside a run that still performs them, not a run without them. Evidence and tests reports both. |
| Independent existing `as commit` proposals | Repeated isolated writes from one unchanged head | Retained for single-operation measurements; it cannot measure a stream whose later writes consume earlier results. |
| Extend publishable proposals | Reuses their creation spelling and permits eventual publication | Excluded: mixed root/child publication brings identity replay, partial effects and lifetime rules that this RFC does not need. |
| Hidden native branch with a mutable tip | Stable handle and ordinary branch machinery | More discovery, policy and lifecycle behavior than a caller-held tip needs; revisit if a stable shared handle becomes a requirement. |
| Table effects without a graph snapshot tag | Less catalog metadata | Fails read-after-write inspection and gives the next write no durable complete graph state to use. |
| Tag name carrying the root id (`unattached_<root>_<C>`) | Descendant scan by name, no decode; the proposal listing skips the prefix | Chosen over keeping `unpublished_<C>`, which would require every reader to decode each tag and skip by kind. |
| Per-record row clock (version 1's M+1) | A validator cross-check between record and head row | Dropped for version 2: no reader of the clock was found; D is the physical coordinate and C the identity. |
| Derive root-versus-child by walking `parent` | No stored root id | Fails closed for every descendant once one ancestor is Undecidable; the root id is stored in each record. |
| Drop only the requested id | Smaller drop | Children would repeat their root's coordinates and expire with the target; kept transitive for operational cost (no orphan Undecidable records), not correctness. |

Publication, squash or attachment of unattached work is deferred to a separate RFC (atomicity, identities, retries, migration); this RFC implies no choice among them.

A native tag is the sole addressability authority here too, as in the sibling RFC, under the new prefix. This RFC adds no durable record beyond the version 2 candidate: no second branch registry, no mutable tip record, no dropped-id tombstone and no TTL. Detached table versions ([Detached table commits](0067-detached-table-commits.md)) and native tags are the in-repo precedents. The versioned candidate property is inherited from the sibling RFC, whose implementation is partial; its retention and decoding obligations still need adaptation for the new kind.

## Evidence and tests

No implementation, correctness run or performance result is claimed. Required evidence:

- GQT: an effectful root and two successive children; cumulative snapshot reads; named target unchanged; the no-op step carries `--- expect affected: nodes=0 edges=0` and no `--- bind`; the following child extends the earlier binding; distinct ids are evidenced by distinct snapshot reads under each binding; two children from one base are independent.
- Refusals: every root and child refuses publication through GQ, structured HTTP and engine entry points before effects; reject published, legacy proposal and merge-proposal bases; preserve existing proposal publish/drop behavior.
- Identity/codec: distinct canonical ULIDs; version-specific validation; stable ids across reopen; unknown kind/layout refusal; no accidental history-block address or nonce alias.
- Lifecycle: root-head advance, inherited-head target, retirement/recreation, missing ancestor, cycle, sibling drop, root cascade and partial-drop retry. Test reads and listing under the same lifetime verdict, plus admitted reads across cleanup.
- Collector and failpoints: result/input pin retention; real branch authority in staging witnesses; create/tag acknowledgement loss; unavailable readback; failed post-tag revalidation/removal; competing child/drop under the shared gate; interrupted expiry cleanup preserves ancestors until descendants are confirmed removed; no idle-head reclamation claim.
- Policy and served conformance: protected/unprotected `branch_scope` grants on the recorded target, request branch mismatch, engine enforcement, masked forbidden ids, authorized cross-actor inspection and kind/filter rendering.
- Compatibility: explicit upgrade and old-binary refusal before write/cleanup; unchanged published identities and version 1 proposals; export/rebuild of retained published data.
- Cost: the same fixture and write stream on a named branch and on unattached snapshots. Separately report detached table work, candidate writes, tag operations, snapshot resolution/lifetime checks, retained inputs and cleanup. Existing independent proposals are a third comparison only for workloads they can express. Each workload's owner specifies snapshot count per unattached branch, retained-population limits and cleanup placement before reporting results.

The unattached benchmark result must be labelled as successive detached writes and snapshot work, excluding ordinary branch-head publication/history settlement. It cannot stand in for end-to-end published-write results. Before acceptance, record fixture size, snapshot count per unattached branch and retained population for every cost comparison; do not attribute a win without measured evidence.

Implementation gates: green in-process and served conformance; zero violations in the fault/collector oracle; snapshot identity and content preservation; compatibility evidence; checked-in cost records.

## Rollout

1. Accept this RFC for benchmarking and its named-branch comparison. Publication remains outside this RFC; no engine implementation is included in the documentation change.
2. Specify and implement version 2 fields, immutable snapshot creation, shared lifetime, transitive drop, collector adaptation, stamp/upgrade, grammar, policy and GQT operand substitution. The catalog implementer and accepting maintainer settle the exact version 2 field schema and source-version upgrade matrix before implementation approval. Require in-process correctness and fault evidence before `implementation: partial`.
3. Add served/CLI kind and list support, publish refusal, compatibility inventory, user documentation and measured benchmark costs. Mark `implementation: complete` only when those gates are green.
4. Any later publication or stable server-side branch handle needs a separate RFC. It must decide semantics for existing snapshots instead of treating this RFC as a promise of forward attachment.

## Unresolved questions

- Before acceptance, the author and the accepting maintainer decide, from the cost record in `blocked_on`, whether any proposed workload needs this operation separately from the named-branch baseline.

## Decision log

- 2026-10-11: created.
