---
rfc: "2026-10-11-actor-views-and-subgraph-sharing"
title: "Actor views and subgraph sharing"
track: maintainer
status: draft
implementation: not-started
authors:
  - ragnorc
created: 2026-10-11
updated: 2026-10-12
discussion: null
supersedes: []
superseded_by: []
blocked_on:
  - "Measure full-text ranking quality under view-independent statistics (public and uniform) against index-wide statistics on a checked-in fixture, and choose the restricted-actor statistics source."
  - "Measure readable-set construction and incremental maintenance at the target scale (10^5 principals, 10^6 securables, membership depth 16, connector access churn) on main, warm and cold; confirm the limits in Bounds and that maintenance work after an access change scales with the actors it affects."
---

# RFC: Actor views and subgraph sharing

> A term set in ***bold italics*** is defined at that spot; it is plain text
> afterwards. Every `file:line` anchor into OmniGraph is at commit `6e31d0ab`;
> every anchor into Lance is at crate `lance` 11.0.0 and its sibling crates, the
> versions in `Cargo.lock`, written as `lance-11.0.0/<path>`.

## Summary

OmniGraph authorizes today at graph × branch class × action. An actor can read
every row of a branch or none of it. This RFC adds an ***actor view***: the part
of the graph a given actor may see and write.

Views are built from two roles and a fixed set of facts. A ***group*** is a set
of actors, defined by membership. A ***securable*** is a node that data is
placed in, such as a channel, a folder, a deal room or a sensitivity class; who
can read it is defined by edges into it. Grants and inheritance add readers,
`@requires` intersects audiences, and denial removes readers. Annotations in the
schema say which types and edges play which part and place each row, column,
type or edge in securables. An actor sees a piece of data when it can read every
securable the data is placed in. Memberships, grants and placements are graph
data; users write facts, never rules.

The observable contract is ***non-interference***: an actor's answer to any
query equals the answer the same query would give on a graph that physically
contained only that actor's view. Counts, aggregates, traversals, search ranks
and errors are computed on the view, never filtered afterwards. The contract is
enforced at one place per mechanism, the ***visibility door***, and a
differential test proves it for every operator.

External sharing needs no further access concept. A ***recipient*** is a
principal that stands for an outside party. A share is the recipient's view,
served live or materialized into a separate graph kept current from the
source's change feed.

What does not change: Cedar remains the gate on operations; a graph that
declares no securables behaves exactly as today; anyone holding storage
credentials still sees everything, as today.

## Motivation

### The current boundary

The policy model has graph-scoped actions with an optional branch class.
`ResourceScope` is "branch-grained on purpose", and per-type and per-row scope
were reserved for a later `predicate_for` hook
(`crates/omnigraph-policy/src/lib.rs:964-990`). The user guide states the limit:
"Per-entity and per-property authorization is not currently supported"
(`docs/user/operations/policy.md`). Rules are allow-only, grouped by static actor
lists in YAML, and compiled to Cedar text (`crates/omnigraph-policy/src/lib.rs:825-866`).
Reads are authorized only in HTTP handlers; the engine's read doors take no
actor (`crates/omnigraph/src/exec/query_doors.rs:41-170`).

### A company brain

A graph that ingests Slack, email, documents and CRM records for agents to
reason over holds data with thousands of overlapping audiences. Nearly all of
them already exist in the source systems: channel membership, folder ACLs,
thread participants, record ownership. They change daily. A graph-wide read
grant forces a choice between giving agents everything and splitting the brain
into graphs that can no longer be joined.

Three further needs appear in every comparable product surveyed
([Appendix B](#appendix-b-prior-art)):

- **Barriers.** Legal ethical walls, finance information barriers and Purview
  Information Barriers are deny rules that override grants.
- **Column and type confidentiality.** A salary or a personal email address is
  confidential on a record whose other fields are not.
- **Derived content.** A summary of a private thread must not be more visible
  than the thread.

### External sharing

A firm needs to give a client, partner or auditor a chosen part of the graph: a
deal room, a matter, a project. Today the only unit is a whole graph, copied by
export and load with no lineage, no incremental update and no revocation story.

### Why an RFC

This is a cross-cutting authorization boundary with new schema, policy, query
and HTTP contracts. A partial implementation that filters only some operators
is worse than none: it suggests confidentiality while leaking through the
operators it missed. The contract, the enforcement points and the evidence must
be fixed together.

## The contract

### Non-interference

For an actor `a` with view `V(a)` and any read `q`:

```text
answer(q, G, a) = answer(q, materialize(V(a)), bypass)
```

`materialize(V(a))` is a graph containing exactly the types, properties, rows
and edges `V(a)` admits. The equality covers rows, order where the query orders,
counts, aggregates, search ranks, errors and the result schema. It holds at the
snapshot the read targets, under the authorization state captured for the
request.

Writes obey the dual rule: a write by `a` succeeds only if every row it creates,
changes or removes is writable in `V(a)`, and its outcome reveals nothing
outside `V(a)` beyond the disclosures listed under
[What is not promised](#what-is-not-promised).

### Silent filtering with a signal

Reads are evaluated over the view without being refused, as in Postgres row
security, Oracle VPD, Oracle Label Security, Neo4j, Stardog and Elasticsearch.
Refusing queries that are not provably answerable from authorized views (the
Non-Truman model of Rizvi et al., 2004) needs query-equivalence reasoning that
does not scale to ad-hoc agent queries.

Silent filtering can mislead: an average over the view is not the company's
average. Every query result therefore carries a `view` object whose
`restricted` field is `true` when the view excluded anything from a type the
plan read, or a member of an abstract type the plan bound. It is computed from
the view and the plan, never from data, so it reveals only a policy fact the
actor may know. A client that needs a whole answer refuses on it. An answer
that must be whole for an actor who cannot see the rows behind it comes from a
stored query that runs with its definer's view
([Answer capabilities](#answer-capabilities)).

A name the view hides (a type, an edge type or a property) does not exist for
the actor. A query naming it fails with the same error, byte for byte, as a
query naming something never declared.

### What is not promised

- **Storage holders.** Lance files contain every row. Anyone who can read the
  graph's objects sees everything. A recipient who needs storage-level access
  gets a materialized share with its own storage.
- **Timing.** Execution time can depend on hidden rows. Timing channels are out
  of scope for live access; they are one reason external parties get
  materialized shares.
- **Key existence for writers.** A writer of a keyed type can learn whether a
  key exists in that type ([D4](#d4-writes-that-collide-with-hidden-rows)).
- **Edge existence on delete.** Deleting a node that has an edge the actor
  cannot see is refused, which reveals that such an edge exists.

Every other channel in [Surfaces](#surfaces) is closed or refused.

## User and operational behavior

### The model

These annotations declare which types and edges take part. Each has one
meaning, and each corresponds to a step of the derivation in
[Why these constructs](#why-these-constructs).

| Annotation | On | Meaning | Role in the algebra |
|---|---|---|---|
| `@principal` | a `String` property | The node stands for the authenticated actor whose id equals this value. | Identity |
| `@group` | node type | Rows are sets of actors. | Actor side |
| `@member` | edge type, principal or group → group | The subject belongs to the group. Membership chains. | Closure |
| `@securable` | node type | Rows hold data. Securable keys share one namespace across every securable type. | Data side |
| `@in(p)`, `@in("key")` | node type, edge type or property | Places each row in the securable named by its property `p`, or a whole type, edge type or column in a fixed securable. | Placement |
| `@grant` | edge type, principal or group → securable, with a `role` | The subject's members get `viewer`, `editor` or `owner` on the securable. One hop. | Union |
| `@inherits` | edge type, securable → securable | Readers of the target get their role on the source, at most the edge's optional `role` (default `viewer`). | Union |
| `@requires` | edge type, securable → securable | The source is readable only if every target is readable too. | Intersection |
| `@deny` | edge type, principal or group → securable | The subject's members never read the securable, and nothing flows through it. | Difference |
| `@starts`, `@expires` | a `DateTime` property of a `@member`, `@grant` or `@inherits` edge | The edge counts only from `@starts` until `@expires`. | Time |

Roles are ordered `viewer < editor < owner`: a viewer reads data in the
securable, an editor also writes it, an owner also changes who has access. A
***subject*** is a principal or a group. "Securable" is the term SQL Server,
Databricks Unity Catalog and SharePoint use for the thing grants attach to;
data placed in several securables behaves like data in several classified
compartments, which needs all of them ([Naming](#naming)).

A type may be both a group and a securable: a Slack channel has members and
holds messages. Members of such a row can read it. The two roles stay distinct:
being able to read a team's page does not make you a member of the team, so the
team's grants do not reach you.

### Declaring the model

```pg
node Person @securable {               // each person has a personal securable
  email: String @key
  actor: String? @principal @unique     // "principal:<sub>" or a static-token actor id
}

node Team @group @securable @in(slug) { slug: String @key }
node Channel @group @securable @in(slug) { slug: String @key  name: String }
node Folder @securable @in(slug) { slug: String @key }
node Class @securable { slug: String @key }     // fixed securables for columns and types

edge MemberOf: Person -> Team @member {
  starts: DateTime? @starts
  expires: DateTime? @expires
}
edge InChannel: Person -> Channel @member
edge SubteamOf: Team -> Team @member
edge TeamAccess: Team -> Channel @grant {
  role: enum(viewer, editor, owner)
  expires: DateTime? @expires
  @key(@src, @dst)
}
edge PersonAccess: Person -> Channel @grant {
  role: enum(viewer, editor, owner)
  @key(@src, @dst)
}
edge FolderAccess: Team -> Folder @grant { role: enum(viewer, editor, owner)  @key(@src, @dst) }
edge ClassAccess: Team -> Class @grant { role: enum(viewer, editor, owner)  @key(@src, @dst) }
edge FolderParent: Folder -> Folder @inherits
edge Screened: Team -> Channel @deny

node Message @in(channel) {
  source_ref: String @key               // opaque source-system id
  channel: String?
  text: String
  author_email: String? @in("pii")
}

node Document @in(folder) { file_id: String @key  folder: String }

node Salary @in("compensation") {
  person: String @key
  amount: I64
}
```

With polymorphic endpoints ([Polymorphic types](#polymorphic-types)) the access
edges collapse into one edge type per relation.

### Rules that complete the model

- **One key namespace.** Securable keys are unique across every `@securable`
  type, checked when a securable row is written. A placement value therefore
  needs no type, and connectors use namespaced keys such as `slack:C123`,
  `folder:q3-board` or a person's email.
- **Actor ids are unique** across every `@principal` property. A graph may have
  several principal types: people, agents, recipients.
- **The organization group.** A `@group` row keyed `organization` is the
  ***organization group***. Its members are implicit: every actor in the cluster
  policy groups listed under `views.organization`. A grant from it is a
  company-wide grant. Recipients and other outside principals are never in
  those policy groups, so no company-wide grant reaches them.
- **Placement.** `@in(p)` on a type places each row in the securable whose key
  is the row's `p`. `@in("key")` on a type places the whole type; on a property,
  that column. A type may carry several `@in`; data is visible only when every
  one of its securables is readable. A null `p` places the row in no securable,
  so only `bypass` actors see it. Edge types take `@in` the same way; an edge is
  visible when both endpoints are visible and its own placements are readable.
- **Fixed placements are checked references.** Schema apply refuses an
  `@in("key")` whose key is not a securable on `main`, and deleting a securable
  row that a fixed `@in` names is refused. A misspelling fails loudly instead of
  hiding a column from everyone.
- **Securables are placed like any row.** `node Channel @group @securable
  @in(slug)` places each channel in itself, so only those who can read it know
  it exists. `Person` is a securable but unplaced, so people stay visible while
  each person's personal securable holds their private data.
- **Unplaced types** stay visible to every actor with `read`, as today.

Schema apply validates the declarations:

- `@principal` is on a `String` property.
- `@member` edges run from a principal or group type to a group type.
- `@grant` and `@deny` edges run from a principal or group type to a securable
  type. `@grant` edges carry a `role` enum drawn from the three roles and are
  keyed by `(@src, @dst)`.
- `@inherits` and `@requires` edges run between securable types, and an
  `@inherits` `role` is drawn from the three roles.
- `@in(p)` names a `String` property that is not a list, Blob or vector.
- `@starts` and `@expires` are on `DateTime` properties of member, grant or
  inherits edges.

These declarations set the schema feature `actor-views`, which builds that do
not implement this RFC refuse to open
([Compatibility](#compatibility-and-reversibility)). Each `@in(p)` property gets
a BTREE index. A schema lint warns when a type with a per-row `@in` is keyed on
a property that looks human-meaningful, such as an email or a name, because
writers can probe keys.

### Computing access

For actor `a` with principal `p`, evaluated at the request's admission time `t`.
An edge is ***live*** when its `@starts` is null or at most `t` and its
`@expires` is null or after `t`.

```text
1. Membership:    C0 = {p}, plus organization if a is in views.organization,
                       plus every group reached from them over live @member edges
2. Denial:        D  = every securable with a live @deny edge from a subject in C0
3. Membership:    C  = as C0, but never entering a group that is in D
4. Readable set:  the least fixed point of
                    role(s) ≥ viewer           if s ∈ C and s is a securable
                    role(s) ≥ r                if some x ∈ C has a live @grant(r) edge to s
                    role(s) ≥ min(role(x), c)  if x ∈ R and s has a live @inherits(c) edge to x
                    R = { s ∉ D | role(s) ≥ viewer and every @requires target of s is in R }
                  W = { s ∈ R | role(s) ≥ editor }     O = { s ∈ R | role(s) ≥ owner }
```

Membership is the only relation that chains through groups; a grant is a single
hop from the membership closure, and inheritance passes access between
securables. Denial is computed from the full closure, then removes the denied
securables and everything that would flow through them: no membership above a
denied group, no grant from it, no inheritance through it. The negation sits in
one layer and everything after it only adds, so evaluation terminates and is
deterministic, and a cycle never makes anything readable.

Worked example:

```text
alice ─member→ team:eng ─editor→ channel:eng
bob   ─member→ team:legal ─viewer→ channel:deal-acme
bob   ─member→ team:acme-wall ─deny→ channel:deal-acme
carol ─editor→ channel:deal-acme
organization ─viewer→ channel:general          (alice, bob, carol are staff)
team:hr ─viewer→ pii, compensation
digest:acme ─inherits→ channel:deal-acme
            ─requires→ channel:deal-acme, channel:general
client:globex ─viewer→ folder:globex-shared    (a recipient, not staff)
```

| | alice | bob | carol | client:globex |
|---|---|---|---|---|
| Memberships | team:eng, organization | team:legal, team:acme-wall, organization | organization | — |
| Granted | channel:general, channel:eng (editor) | channel:general, channel:deal-acme | channel:general, channel:deal-acme (editor) | folder:globex-shared |
| Denied | — | channel:deal-acme | — | — |
| `digest:acme` | ✗ cannot read channel:deal-acme | ✗ channel:deal-acme denied | ✓ | ✗ |

`digest:acme` holds summaries drawn from both channels: it inherits its readers
from the deal room and requires both sources, so exactly those who can read both
see it. Bob is granted the deal room through legal, but the wall's deny wins.
The client never sees `channel:general`, because it is not in the organization
group.

### Placement in practice

- A `Message` row is visible to `a` only if its `channel` is in `R(a)`. For
  alice above, a `count` over messages counts rows in `channel:general` and
  `channel:eng`, and the response carries `"view": { "restricted": true }`.
- `author_email` exists for actors who can read `pii`; for everyone else it is
  absent, and naming it is an unknown-name error.
- `Salary` exists only for actors who can read `compensation`.

Fixed securables are created and granted as data:

```gq
query create_pii() {
  insert Class { slug: "pii" }
}

query grant_pii($team: String) {
  insert ClassAccess { from: $team, to: "pii", role: "viewer" }
}
```

### Access data is itself placed

Membership, grant, inherits and requires edges are visible when both endpoints
are, so the members of a team see its member list and others do not. Deny edges
are visible only to owners of the securable they deny, so walls stay
confidential. A securable or group row is visible according to its own type's
placement.

### Policy configuration

Cedar continues to gate operations: `read`, `change`, `schema_apply`, branch
and commit actions, `invoke_query`. Securables gate data. The graph policy
bundle gains version 2 with a `views` block that holds only the roots of trust
and bounds:

```yaml
version: 2
groups:
  staff: [principal:alice, principal:bob, principal:carol, principal:slack-connector]
  connectors: [principal:slack-connector]
  platform-admins: [principal:root]
  recipients: [principal:globex]
rules:
  - id: staff-read
    allow: { actors: { group: staff }, actions: [read, invoke_query] }
  - id: recipients-read
    allow: { actors: { group: recipients }, actions: [read] }
  - id: connectors-write
    allow: { actors: { group: connectors }, actions: [change] }
views:
  organization: [staff]
  bypass: [platform-admins]
  limits:
    max_memberships: 4096
    max_securables: 100000
```

- `organization` lists the cluster groups whose actors are implicit members of
  the organization group.
- `bypass` lists cluster groups whose view is the whole graph: the break-glass
  and maintenance path, named after Postgres's `BYPASSRLS`. Every bypass read is
  audited. Only a bypass actor may remove a placement from the schema.
- `limits` bound one actor's view ([Bounds](#bounds)).

No new Cedar action is needed. Who may change access to a securable is the
`owner` role on it; who may move a row between securables is the `editor` role
on both.

Cluster apply validates `views` where it already validates stored queries
against the catalog (`crates/omnigraph-cluster/src/deployment/execution.rs:782-800`,
`crates/omnigraph-server/src/deployment.rs:676`,
`crates/omnigraph-server/src/registry.rs:644`). Binding a version-1 bundle to a
graph with `actor-views` declarations fails the apply.

### Reading under a view

Queries need no change:

```gq
query recent($topic: String) {
  match {
    $m: Message
    $m.text contains $topic
  }
  return { $m.source_ref, $m.text }
  order { $m.source_ref desc }
  limit 20
}
```

For a restricted actor this returns at most 20 visible messages. Aggregates run
over the same rows. A traversal never passes through a node the actor cannot
see, including intermediate nodes of a multi-hop pattern. `nearest`, `bm25` and
`rrf` rank only visible rows and return `min(k, visible)` results. Restricted
actors get full-text scores computed from view-independent corpus statistics
([Full-text statistics](#full-text-statistics)).

### Writing under a view

For an actor whose view of a placed type is restricted:

| Operation | Rule |
|---|---|
| Insert or update of a placed row | The actor needs `editor` on every securable the row is placed in, before and after the change. Moving a row between securables needs `editor` on both. |
| Insert without a value for a per-row `@in` property | The row goes into the writer's personal securable. A writer without one is refused with `placement_required`. An explicit null is allowed only for bypass writers. |
| A placement value naming a securable the writer cannot edit, or no securable at all | Refused with `view_write_denied` in both cases, so the refusal does not reveal whether the securable exists. |
| Insert of a keyed row whose id exists but is not writable | Refused with `key_conflict`, without an entity id or any value the actor did not send. Otherwise it upserts as today. |
| Update or delete statement | Matches only rows in the view. A matched row that is visible but not writable refuses the statement with `view_write_denied`. |
| Delete that would cascade to an edge the actor cannot see | Refused with `delete_blocked`. |
| Edge insert | Both endpoints must be visible, and the edge's own placements writable; a hidden endpoint fails exactly as a missing one. |
| `load --mode overwrite` | Refused when the actor's view of any type in the batch is restricted. |
| Branch merge | Refused unless every row in the merge diff is writable by the actor. Conflicts are reported without entity ids. |
| Constraint violations | Reported by constraint and property name only. `@unique` omits the other holder's id; `@card` omits the count. |

Access edges and principals have their own rules, for every actor that is not
bypass:

- Writing a `@grant`, `@deny`, `@inherits` or `@requires` edge into or from a
  securable needs `owner` on that securable (the target of a grant or deny, the
  source of an inherits or requires edge).
- Writing a `@member` edge into a group needs `owner` on it. A group type that
  is not also a securable has no owners, so only bypass actors manage it,
  typically the connector that mirrors it.
- Access edges and `@principal` properties are written only on `main`. A branch
  write or a merge that touches them is refused. Without that rule a branch
  forked before a revocation would bring the revoked access back when merged.
- A new securable row must be written together with an `owner` grant into it
  from a subject in the writer's membership closure; otherwise the write is
  refused with `securable_owner_required`.

A connector that mirrors source ACLs either owns the securables it creates or
runs as a bypass principal. Bypass actors keep today's messages and rules.

### Agents and delegated credentials

An agent acting for a user presents a ***delegated credential***: the user's
principal as subject, the agent's principal as actor (the RFC 8693 `act`
claim), an audience bound to this server, and optional ***caveats*** that only
narrow: a list of securables, a list of types, an action subset and an expiry.
A caveat's securable list covers the listed securables and every securable that
inherits from them. The effective readable set is

```text
R(user) ∩ R(agent) ∩ covered(caveats)
```

and the schema view is narrowed by the caveat types. Keeping content away from
agents needs no special tier: put agent principals in a team and `@deny` that
team the securables in question. The MCP surface accepts only tokens whose
audience is this server and never passes a token upstream.

### Derived content

A summary, chunk or note derived from several sources is placed in a securable
that `@inherits` one source and `@requires` every source, as `digest:acme`
above. It is then visible exactly to those who can read every source, and stays
correct as memberships change. OmniGraph does not check that a writer declared
all of its sources; an agent runtime that tracks information flow (CaMeL, FIDES)
owns that.

### Access derived from data

Some access follows data rather than lists. Each such rule maps onto the model
in a way that keeps every read a column test
([D8](#d8-access-rules-are-facts-not-queries)):

- **Attributes that name an audience** become placement on the attribute: deals
  visible to their region's team are `node Deal @in(region)` with regions as
  securables.
- **Rules over the row's own columns** become ***computed placement***: a
  placement property whose value the engine sets on every insert and update from
  an expression over that row's own columns and constants, in the same write.
  Writers cannot set it directly. For example, deals of $1M or more are placed in
  `deals:board` and others in `deals:standard`; the syntax is open
  ([Unresolved questions](#unresolved-questions)).
- **Access that starts at a time** uses `@starts`: an embargoed release is
  placed in a securable granted to the organization with `starts` at the release
  time.
- **Relationships that should confer access** become access edges by explicit
  opt-in, for example a `CompChain: CompFile -> CompFile @inherits` edge mirrored
  from the org chart so that each manager chain reads its reports' compensation
  files. Such edges take on the access-edge rules: written only on `main` and by
  owners. They belong on purpose-specific securables: an `@inherits` edge between
  personal securables would also hand a manager the report's private notes.
- **Rules over other rows, counts and negation over other rows** (notes that
  mention my accounts, records visible after three approvals) are served by
  stored queries that run with their definer's view: the caller gets answers,
  not a view to query freely ([Answer capabilities](#answer-capabilities)).

### History, change feed and export

Authorization always comes from current authority: rules from the applied
policy, access edges from `main`'s current head. Revocation therefore covers
history, and joining a team shows that team's history. Relaxing access exposes
history too; BigQuery documents the same consequence.

In the first releases a restricted actor cannot read a historical snapshot, the
change feed, a commit's changes, an export or a baseline of any placed type its
view restricts. These refuse with `view_restricted`. Bypass actors and share
reconcilers are unaffected. A later phase serves view-filtered history
([Rollout](#rollout), Phase 8).

### Surfaces

| Surface | Under a restricted view |
|---|---|
| `POST /query`, `POST /queries/{name}`, MCP `query` | Answered over the view. |
| `explain` | Omits row estimates, statistics, dataset paths, versions and index facts for restricted types. |
| `profile` | Bypass actors only. |
| `GET /schema` | The schema view: hidden types and properties are absent. |
| Structured catalog route and schema hash on reads ([Polymorphic types](#polymorphic-types)) | The schema view, and the view's digest in place of the schema hash. |
| `GET /queries`, MCP `queries` | Only queries that typecheck against the schema view. |
| `GET /snapshot` | Omits `entity_count` and dataset paths for restricted types. |
| `GET /commits`, `/commits/{id}`, `/commits/{id}/changes`, `GET /changes`, `POST /changes/baseline`, `POST /export` | `view_restricted` until Phase 8. |
| `GET`/`HEAD /blob` | Only for a visible row; a hidden row answers exactly as a missing one. |
| Write responses | As in [Writing under a view](#writing-under-a-view). |
| Denial messages | Do not echo the actor or branch. |

### Sharing

A recipient is a principal of a principal type whose actor id the outside party
authenticates as. It is never in the organization group. A share's content is
whatever the recipient can read. Three delivery modes use that one definition:

| Mode | Recipient reads | Revocation | Retention and audit |
|---|---|---|---|
| ***live*** | Our server, under the recipient's view | Immediate | Ours; we see every read |
| ***hosted copy*** | A separate graph we host, kept current by a reconciler | Immediate; the copy is deleted | Ours; we see every read |
| ***delivered copy*** | A separate graph in storage the recipient controls | Stops future updates only | The recipient's |

Live shares inherit every side channel of live access, so they are for trusted
collaborators. External parties get copies. Copies are declared in cluster
configuration:

```yaml
shares:
  globex-matter:
    source: knowledge
    recipient: principal:globex
    mode: hosted
    target: globex-matter          # graph id of the copy
    terms: odrl/globex-agreement.json
```

The copy's schema is the source's schema view for the recipient. Ids are
preserved, so rows map back to the source.

#### Answer capabilities

A stored query may declare `run_as: definer`. It then runs with the view of the
principal that defined it, and a caller needs only `invoke_query` on it. This is
how a recipient gets a complete aggregate without the rows behind it, and how
rules over other rows are served. Because repeated parameterized queries can
isolate individuals, a definer query must declare a domain for every parameter,
cannot filter a key property by a caller-supplied value, and logs every
invocation's parameters for its owner. Minimum group sizes alone do not prevent
differencing; AWS Clean Rooms and Snowflake both say so. Per-recipient privacy
budgets are later work.

#### Write-back

A recipient of a hosted copy writes on a branch of the copy. The reconciler
maps that branch into a proposal branch on the source, checks that every changed
row is writable by the recipient, and an owner reviews and merges. Revoking the
share deletes its proposal branches.

### Vaults

A ***vault*** is a compartment kept in its own graph or cluster instead of
placed by securables. Use securables for the long tail of channels, folders,
deals and matters. Use a separate graph when a defect in view enforcement would
be unacceptable, retention differs, or full-text statistics must be isolated.
Use a separate cluster when the threat includes holders of storage credentials,
when compliance requires key separation, or when a recipient gets storage
access: every graph in one cluster shares one storage root and one set of
credentials today ([Appendix A](#appendix-a-validated-assumptions)).

### Operator tools

```bash
omnigraph policy explain-view --graph knowledge --actor principal:alice
omnigraph policy who-can-see --graph knowledge --type Message --id <id>
omnigraph policy diff-view --graph knowledge --branch proposed-access
```

`explain-view` prints the actor's effective access: memberships, readable
securables (bounded), denials and the edges that produced them. `who-can-see`
inverts it for one row. `diff-view` reports how many securables change
visibility for how many actors between `main` and a branch carrying proposed
access edges, before they merge.

## Design

### Why these constructs

The constructs are derived, not collected.

1. **The object.** Authorization is one relation,
   `Access ⊆ Actors × Items × Ops`, with `Ops = read ⊇ write ⊇ administer` and
   items being node rows, edge rows, columns and types. Non-interference fixes
   how reads behave once `Access` is known.
2. **Size forces two factorings.** At 10^5 actors and 10^8 items, `Access`
   cannot be stored pair by pair. It must factor through a small middle:
   items carry labels (placement), and actors fall into groups (membership).
   The fewest labels is the number of distinct audiences, which source systems
   already provide. Each item has one label per dimension, so a label is a
   column: O(1) per item and indexable ([D3](#d3-placement-is-a-property)).
   Groups contain groups, so membership is a closure. Groups and labels are
   different roles, a set of subjects against a class of objects, even when one
   node plays both.
3. **Audiences are set expressions.** Every audience is a Boolean combination of
   groups, and each operation is needed: union over groups (several teams share a
   space), union over labels (inheritance, multiple parents, clearance ladders;
   without it the expressions are not closed under union), intersection (derived
   content; unions and group differences cannot produce it), and difference
   (walls; unions and intersections cannot remove anyone). These are `@grant`,
   `@inherits`, `@requires` and `@deny`. Difference is only by groups: walls are
   lists of people, and a general difference would add a second layer of
   negation with no requirement behind it.
4. **Edges are the expression.** Each label's audience has the normal form
   `(granted groups ∪ readers of inherited labels) ∩ readers of required labels −
   denied groups`. Each source-system ACL entry becomes one edge, so connectors
   mirror ACLs without writing formulas.
5. **The rest follows.** Operations form a chain, so roles are ordered and
   inheritance passes them capped; write without read would allow blind writes.
   Temporary access is common, and an edge window is exact at its instant where a
   deleting job would be a queue and a lag. The actor must be located in the
   subject graph (`@principal`), and the organization root is defined by policy
   because it is the root of trust. Several placements on one item are
   intersection evaluated at the item, which avoids a label for every combination
   of orthogonal dimensions. Access facts are data, so they are placed too.
6. **The physical design follows the workload.** Reads dominate and each belongs
   to one actor, so the relation is inverted once per actor into `R(a)` and items
   are filtered by `label IN R(a)` on an index ([Scans and search](#scans-and-search)).

Each construct is required by a step above and none can be built from the
others, so the set is minimal for these requirements.

### Naming

Names follow the field where its meaning matches:

| Name | Field usage |
|---|---|
| `@principal` | AWS IAM, Cedar, Active Directory security principals |
| `@group`, `@member` | Zanzibar's `member` relation, AD groups, OpenFGA |
| `@securable` | SQL Server securables, Databricks Unity Catalog securable objects, SharePoint `SecurableObject` |
| `@grant`, `role`, `viewer`/`editor`/`owner` | SQL `GRANT`; GCP basic roles, Google Drive |
| `@inherits` | NTFS ACL inheritance, SharePoint inherited permissions, Azure RBAC, OCI compartments |
| `@deny` | AWS explicit deny, Windows deny entries, Databricks DENY; deny-overrides in XACML |
| `@expires` | SpiceDB expiring relationships, Drive access expiry |
| `bypass` | Postgres `BYPASSRLS` |
| organization group | Google "anyone in the organization" |
| recipient | Delta Sharing, Snowflake |

Names chosen against the field, and why:

- **Not "scope".** It already means OAuth token scopes (MCP authorization),
  `ResourceScope` and `ChangeFeedScope` in OmniGraph.
- **Not "label".** It means node type in a property graph.
- **Not "compartment".** It is exact but known mainly from classified systems and
  Oracle.
- **Not "everyone" or `*`.** OpenFGA and SpiceDB use `user:*` for all users, the
  public included; the organization group deliberately excludes recipients.
- **`@requires` rather than "within".** "Within" reads as containment and invites
  modeling folders with intersection instead of inheritance.
- **`@in`** names set membership, as GQ's `in` operator does.

### Authority

| Fact | Owner | Changes | Versioned by |
|---|---|---|---|
| Which types and edges take part, and how data is placed | Accepted schema contract | Rarely; schema apply | The contract in `__manifest` |
| Organization groups, bypass groups, limits | Applied cluster policy | Reviewed; cluster apply | Policy digest |
| Principals, groups, securables, access edges, placement values | Graph rows on `main` | Daily | `main`'s manifest pins of the access tables |

Cluster policy groups gate operations and define the two roots; groups and
securables in the graph gate data. Neither mirrors the other.

### Capturing a view

A request resolves its actor at the server as today. The engine gains read
doors that take the actor; invariant 10 extends from writes to reads, so
embedded and CLI-direct readers obey the same view when a policy is installed.

A read captures two things under one schema permit, in this order:

1. ***Authorization state***: the applied policy digest and the `__manifest`
   pins on `main` of every ***access table*** (each group and securable type,
   each principal type, each access edge type), each pin being the table's
   identity, published dataset version, staged version and transaction id.
2. ***Data snapshot***: the read target, as today.

Snapshot-target reads already load `main`'s live contract under the schema
permit and then discard that `main` snapshot
(`crates/omnigraph/src/db/omnigraph.rs:1009-1024`); this design keeps it. A
branch read probes `main`'s version once more. The capture is one new function,
because the schema gate is not reentrant
(`crates/omnigraph/src/db/write_queue.rs:455-458`).

The ***access version*** is the policy digest plus the access pins. It is
deliberately not `main`'s head commit: every unrelated write and every
maintenance run advances the head, while the pins move only when an access table
is written. Because authorization comes from the freshest `main`, it is never
older than the data it filters, so Zanzibar's "new enemy" problems (content added
after a revocation, or a revocation ordered after a move) cannot occur.

Edge windows are evaluated at the request's admission time on the server, one
clock for the whole request, as SpiceDB evaluates expiry in its datastore to
avoid clock skew. A cached view is valid until its earliest `@starts` or
`@expires` boundary.

### Computing the readable set

The computation has two layers, following Zanzibar's Leopard index:

- A ***shared closure*** per access version, used by every actor: the
  group-to-group membership closure and, for each securable, its grant, inherits,
  requires and deny inputs. Building it is the recursive, expensive part. It is
  derived state keyed by the access version.
- A ***per-actor view***: the actor's direct memberships joined with the shared
  closure give `C0` and `C`; denial, grants, inheritance and requirements then
  give `R`, `W` and `O` as set operations, not traversals. The result is an
  immutable ***view spec*** (the schema view, the sets, the earliest window
  boundary and a digest), cached by access version, actor and caveats.

The first build of the shared closure reads the access tables on `main` with
indexed scans on their source and destination columns. Readable sets are stored
as compressed bitmaps over dense securable ordinals assigned by the shared
closure: a few kilobytes for an actor that reads a thousand securables.

### Incremental maintenance

Access tables change whenever a connector syncs. Keyed only by access version, a
single membership change anywhere would invalidate every actor's view; with
10^4 active actors and a sync every few minutes, the engine would rebuild 10^4
views per sync. Maintenance is therefore incremental:

- The shared closure moves from one access version to the next by applying the
  changed access rows between the two pins, read through the change feed of the
  access tables. If the change history is gone (after cleanup), the closure is
  rebuilt from scratch.
- A cached per-actor view is carried forward to the new access version without
  recomputation when no changed row touches the actor's membership closure or a
  securable whose audience it depends on. Otherwise it is rebuilt from the
  shared closure.
- Connectors publish access changes in batches, so access versions move in
  steps.

Maintenance work after an access change is proportional to the changed rows and
the actors they affect, not to all actors. Derived state that lags is never
used: a stale closure or view is rebuilt before it serves a request.

### Bounds

The membership closure is bounded by `max_memberships`, `R` by
`max_securables`, membership depth by 16. Exceeding a bound refuses the request
with `view_limit_exceeded`; the engine never truncates a view. The view cache is
an LRU bounded in bytes and lives outside `invalidate_read_caches`
(`crates/omnigraph/src/db/omnigraph.rs:2026-2034`), whose flush on every observed
advance of `main` would otherwise discard views keyed by immutable state. Reads
by external principals get per-actor read admission, extending the existing
per-actor write caps (`crates/omnigraph-server/src/workload.rs:15-19`).

`|R(a)|` equals the number of distinct audiences an actor belongs to. It is
small for channels and folders, and can reach 10^5 for a heavy email user when
every thread is its own audience. Beyond `max_securables`, the cheaper physical
encoding stores each item's reader groups on the item and filters by overlap
with the actor's membership closure, as Elasticsearch and Azure AI Search trim
results. That changes the physical encoding only; this RFC does not implement it.

### The visibility door in the plan

The view enters the plan in three places.

**Schema view.** The query typechecks against a catalog without the types,
edge types and properties whose fixed securables are not in `R`. The catalog is
memoized per (contract, schema-view digest), because the compiled-query cache
matches catalogs by `Arc` identity (`crates/omnigraph/src/exec/query_doors.rs:318-341`)
and the accepted-catalog memo holds one slot (`crates/omnigraph/src/runtime_cache.rs:288-333`).
The engine context receives the same view catalog as the planner, since the
output-schema checks reject a mix. Whole-node returns build their struct from
the catalog's node-object members (`crates/omnigraph/src/engine/expr.rs:79-90`),
so hidden properties never appear. A hidden property that is part of a key is
still read for identity but never projected.

**Row predicate.** Every scan of a type with a per-row `@in` whose `R` is not
everything gets a view conjunct per placement property, a new typed IR leaf
`ViewSet { type_id, property_id }`, resolved at lowering from the engine
context's view spec to an `IN` list. It is not a GQ parameter: a replayed plan
could carry swapped parameter values, while the leaf is always resolved from
the replaying request's own view. The conjunct is placed into the scan spec at
plan construction, inside `Predicate::Gq`, because `Predicate::gq_filters()`
drops every other variant (`crates/omnigraph-planner/src/logical.rs:125-140`).

**Coverage validator.** A new check runs in `engine::execute`, beside
`validate_aggregate_specs` and `validate_output_schemas`, so every door including
replay passes it (`crates/omnigraph/src/engine/mod.rs:409-418`). It refuses a
plan that:

- scans a restricted placed type without its view conjuncts;
- uses `MetadataCount` on a restricted type (its lowering ignores the scan filter,
  `crates/omnigraph/src/engine/lower.rs:473-487`);
- uses the CSR, the `count`/`not` degree shortcut
  (`crates/omnigraph/src/engine/lower.rs:230-277`) or the CSR-derived search gates on
  a restricted type;
- expands into a restricted type, or over a placed edge type, without a mask.

The bound plan's assumptions gain the view digest. Replay recomputes the view
for the replaying actor and refuses if the digest differs, so a saved plan
cannot be replayed by another actor or after a revocation.

### Scans and search

The view conjunct lowers to `col IN (...)` through `ir_expr_to_df_expr`
(`crates/omnigraph/src/engine/scan.rs:1093-1119`) and reaches Lance through
`Scanner::filter_expr`, which takes a DataFusion expression without SQL parsing
(`lance-11.0.0/src/dataset/scanner.rs:1585`). With a BTREE on the placement
column, Lance evaluates the list as an exact `SargableQuery::IsIn` with no
length limit, and the filtered read skips fragments without candidates
(`lance-index-11.0.0/src/scalar/expression.rs:348-352`;
`lance-11.0.0/src/io/exec/filtered_read.rs:163-206`). The engine already sets
`prefilter(true)` on every scan with a filter
(`crates/omnigraph/src/engine/scan.rs:248-251`), so vector and full-text search
rank only rows that pass the view conjunct.

Today a `@index` on a `String` builds a full-text index, not a BTREE
(`crates/omnigraph/src/db/omnigraph/table_ops.rs:582-605`). A per-row `@in`
maps its property to a BTREE.

The engine does not use Lance's row-address prefilter for views in this RFC. On
a plain scan the mask demotes the filter so it loses its scalar index
(`lance-11.0.0/src/dataset/scanner.rs:3298-3306`), and the mask is in stable
row-id space for one exact table version. A per-view mask cache is a later
optimization behind its own surface guard ([Rollout](#rollout), Phase 7).

**Vector search.** Lance has no automatic switch to flat search for a selective
prefilter; its late search widens up to every partition, and when the prefilter
admits at most `k` rows the unreached ones come back at distance `+inf`
(`lance-11.0.0/src/io/exec/knn.rs:1710-1782`). The planner instead chooses from
the visible row count `V`, obtained by an index-answered count:

- **Exact**: when `V × dimensions ≤ 8·10^6` (about 10,000 rows at 768
  dimensions, tunable), scan the visible rows with the index bypassed. Lance
  honors the filter on that path only with `prefilter(true)`
  (`lance-11.0.0/src/dataset/scanner.rs:5015-5019`), which the engine sets.
- **Indexed**: otherwise use the IVF index with probes set up front to about
  `k · partitions / (s · N) × 2`, where `s` is the visible fraction.
- **Fallback**: if that probe count exceeds half the partitions, use exact.

The existing overfetch and exact-pass loop
(`crates/omnigraph/src/engine/mod.rs:473-555`) remains the safety net. Whole-table
counts such as `nearest_dataset_rows` (`crates/omnigraph/src/engine/scan.rs:338-346`)
never leave the engine for a restricted actor.

### Full-text statistics

Lance's BM25 scorer takes its corpus statistics from the whole index, including
rows the prefilter removed and deleted rows
(`lance-index-11.0.0/src/scalar/inverted/index/search.rs:53-63`;
`lance-index-11.0.0/src/scalar/inverted/scorer.rs:197`). With stable row ids,
compaction does not remap the index, so deleted postings stay until the index is
optimized or rebuilt (`lance-11.0.0/src/dataset/optimize.rs:2580`). A restricted
actor's scores therefore encode term frequencies of hidden documents. Hiding the
score is not enough: the STRESS attack recovered document frequencies from rank
order alone. Its precondition, write access to the same index for calibration,
is common in a company brain.

Restricted actors' full-text search on a placed type uses view-independent
statistics. Lance accepts external statistics below the scanner:
`MatchQueryExec`, `PhraseQueryExec`, `CompoundQueryExec` and
`FlatMatchQueryExec` take a `MemBM25Scorer` through `with_base_scorer`
(`lance-11.0.0/src/io/exec/fts.rs:624, 2175, 3058, 3485`). The engine builds
that plan itself for restricted actors. Two sources are candidates:

- ***Public statistics***: corpus statistics over rows placed in securables
  granted to the organization group, built by explicit index reconciliation and
  stored as derived state keyed by index identity and that group's grants.
- ***Uniform statistics***: a fixed document count, average length and document
  frequency. They leak nothing and rank by term frequency and length only.

Public statistics are the proposal and uniform statistics the fail-closed
fallback when the artifact is missing or stale. The choice is gated on ranking
measurements (`blocked_on`). The flat path adds its own filtered input rows to a
supplied scorer (`lance-index-11.0.0/src/scalar/inverted/flat_search.rs:548-559`);
because the engine filters by expression, those rows are visible rows, which
adds no leak. For OR queries, Lance's WAND switches to a flat search when the
allow-list is at most 10% of the index
(`lance-index-11.0.0/src/scalar/inverted/wand.rs:201-206`), so small views are
cheap. Bypass actors keep index-wide statistics.

### Traversal

Traversal reads edge tables by key and never reads intermediate node tables
(`crates/omnigraph/src/engine/graph.rs:1132-1472`). Edge reads carry no filter
(`crates/omnigraph/src/engine/graph.rs:1647-1662`). Under a view each hop is
masked:

- **Indexed mode**: after each hop, the frontier's ids of a restricted placed
  type are intersected with the view by one indexed read of that node table
  (`__id IN frontier AND placement IN R`). The final destination scan already
  carries the view conjunct through `hydrate_nodes`, which refuses unsupported
  predicates (`crates/omnigraph/src/engine/operators/scan/input.rs:270-284`).
  An edge type with its own placement adds its view conjunct to the edge read.
- **CSR mode**: refused for restricted placed types until a per-view mask
  exists. The CSR assigns dense ordinals per node type from edge endpoints only,
  per index instance, and is cached by edge-table versions with no actor
  (`crates/omnigraph/src/graph_index/mod.rs:168-245`;
  `crates/omnigraph/src/runtime_cache.rs:24-57`).

Wildcard traversal expands only over edge types whose declared endpoints match
(`crates/omnigraph-compiler/src/query/typecheck.rs:1981-1989`), so the schema
view prunes hidden edge types. Query bindings are concrete node types today
(`crates/omnigraph-compiler/src/query/typecheck.rs:1785-1790`);
[Polymorphic types](#polymorphic-types) covers abstract bindings.

`Omnigraph::graph_index()` returns the whole topology
(`crates/omnigraph/src/db/omnigraph.rs:2681`) and becomes crate-private.

### Writes

The write check runs in end-of-attempt validation, where the engine has the
staged after rows, the deleted ids (cascades and overwrite removals included),
and a committed state pinned to the transaction base with a batched lookup by
id (`crates/omnigraph/src/validate.rs:391-433`). The validation points are
`validate_staged_mutation` (`crates/omnigraph/src/exec/mutation.rs:886-904`),
`validate_changeset` (`crates/omnigraph/src/loader/mod.rs:958-959`) and merge
evaluation (`crates/omnigraph/src/exec/merge.rs:3433-3436`). `commit_all`
revalidates the whole transaction under the schema, branch and table locks
before any effect (`crates/omnigraph/src/exec/staging.rs:920-984`), so the
checked state is exactly the state publication replaces. Effects staged before a
refusal are detached and unreachable.

The committed state gains a lookup of committed placement values by id, which
supplies the before image for upserts, deletes and overwrite removals. Lance's
merge-insert cannot express "update if writable, else fail" atomically:
`WhenMatched::UpdateIf` turns a false condition into a no-op
(`lance-11.0.0/src/dataset/write/merge_insert.rs:425-449`;
`lance-11.0.0/src/dataset/write/merge_insert/assign_action.rs:107-139`). The check
therefore belongs to OmniGraph's validation, not to Lance.

Computed placement is evaluated in the same validation: the engine computes the
placement value from the staged row's own columns, refuses a writer-supplied
value that differs, and checks `editor` on the result as for any placement.

Today an upsert reports `affected_nodes: 1` whether it inserted or updated, and
delete counts include rows matched by id regardless of visibility
(`crates/omnigraph/src/exec/mutation.rs:1645-1661, 1717-1741`). Under a view the
statement's match runs over the view, so counts are view counts.

Errors carry their foreign facts (another row's id, a count, an ETag, another
type's version) as structured fields, and the server strips them for a
restricted actor. Today they are formatted into messages
(`crates/omnigraph/src/validate.rs:1259, 1295-1296`) and returned verbatim
(`crates/omnigraph-server/src/lib.rs:1357`).

The same validation enforces the access-edge rules: owner roles, `main` only, an
owner grant with every new securable, and global uniqueness of securable keys
and actor ids. Cross-table uniqueness rests on read-set validation and the exact
graph-head precondition, not a fence; a concurrent duplicate fails publication.

### History

A read of a snapshot target captures authorization from `main` and data from the
snapshot, as above. Historical queries already use the live contract with tables
rebound by stable identity (`crates/omnigraph-catalog/src/lib.rs:608-656`), so
annotations bound to stable identity apply to history without new mechanism.
The first releases refuse historical reads of restricted placed types. A later
phase admits a historical row only when its placement is readable both at the
snapshot and, if the row still exists, at `main`'s head, so a row moved from a
public to a private channel stays hidden in old snapshots.

Property binding on historical reads is by name today
(`crates/omnigraph/src/engine/typed_value.rs:383-420`), unlike Blob reads, which
compare stable property ids (`crates/omnigraph/src/blob.rs:1541-1550`). A column
with a fixed `@in`, dropped and re-added under the same name, must not expose its
old values; historical reads must bind properties by stable identity before
column views serve history.

### Change feed and export

A view-filtered change feed filters where rows are emitted
(`crates/omnigraph/src/changes/enumerate.rs:132, 179`). Three requirements: the
view digest joins the cursor and page-token digest
(`crates/omnigraph/src/changes/token.rs:283`); an update across a visibility
boundary is emitted as an insert or a delete; equality across vintages compares
projected rows, so a change to a hidden column emits nothing.

### Server surfaces

Every route in [Surfaces](#surfaces) passes the actor to an engine door that
applies the view, or refuses with `view_restricted`. MCP tools call the same
handlers (`crates/omnigraph-server/src/mcp.rs:272-310`). Explain redaction strips
statistics and assumptions (`crates/omnigraph/src/engine/explain.rs:236, 272`),
per-node row estimates (`crates/omnigraph-planner/src/physical.rs:363-370`) and
index facts (`crates/omnigraph-planner/src/gate.rs:58-71`).

A graph with `actor-views` declarations requires an applied version-2 policy.
The server refuses to serve it in unauthenticated mode, which today allows every
graph action (`crates/omnigraph-server/src/handlers.rs:651-668`), and with
static tokens and no policy, which today allows every read
(`crates/omnigraph-server/src/handlers.rs:661`).

### Polymorphic types

The draft RFC for polymorphic types
([ModernRelay/omnigraph#884](https://github.com/ModernRelay/omnigraph/pull/884),
`2026-10-07-polymorphic-types.md`) makes interfaces and inline unions bindable
and lets an edge endpoint be an interface or a union. It builds on the integer
node identity draft stacked with it (`2026-10-12-integer-node-identity.md`),
under which every edge endpoint is a ***packed node reference***: the endpoint
type's `StableTypeId` in the high 16 bits and the node's `__id` in the low 48.
That RFC defers per-type visibility to this one; its Policy section adopts the
rules below. The two designs compose as follows.

**Member sets come from the schema view.** An abstract binding resolves its
member set against the actor's view catalog, so a member type placed in a
securable the actor cannot read is absent, exactly as in `materialize(V(a))`.
Visibility is decided per concrete type; `implements` neither grants nor removes
it. `$x is T`, `@type` and `@type_id` comparisons naming a hidden type fail with
the unknown-name error. Result schemas still never depend on the member set.

This replaces that RFC's `policy_denied_member` refusal and its
`policy_filtered` marker with `view.restricted`. Refusing a query because an
interface includes a hidden member leaks the member's existence, and the
refusal's only escape, a type test that removes the member, requires the caller
to name a type it is not allowed to know. A stored query shared by actors with
different views could never be written that way.

**Placement on interfaces.** Under that RFC, property annotations on an
interface property apply to every implementor that inherits it. A fixed
`@in("key")` on an interface property follows that rule with two additions: a
member that redeclares the property may not drop it (a refusal, not that RFC's
lint warning, because dropping it would expose the column), and a member may not
add a fixed `@in` to an inherited property, so the property's visibility is the
same for every member. Type-level `@in` belongs to concrete types and is not
inherited through `implements`.

**Removing a placement** from any concrete type, edge type or property, by any
route, is refused unless the applying actor is in `bypass`. The routes are
deleting the annotation, removing an interface property, removing `implements`,
and narrowing an endpoint so a member stops inheriting a placed property.

**Access edges.** With polymorphic endpoints one edge type covers every kind of
subject and securable:

```pg
edge MemberOf: Person | Team -> Team | Channel @member { expires: DateTime? @expires }
edge Access: Person | Team -> Team | Channel | Folder | Class @grant {
  role: enum(viewer, editor, owner)
  expires: DateTime? @expires
  @key(@src, @dst)
}
```

The membership closure is then one bounded multi-hop traversal, which that
RFC's set-based multi-hop rule admits.

**Placement values under integer identity.** A placement stores a securable's
key, so securable keys share one namespace across securable types. Under
integer node identity a placement could instead store the securable's packed
reference, resolved from the key the writer supplies, as an edge endpoint is.
The type half would identify the securable type, removing the shared key
namespace, and the view conjunct would become an `IN` over integers. Whether to
switch is open ([Unresolved questions](#unresolved-questions)).

**Union scans and polymorphic expansion.** Every arm of a union scan whose member
is a restricted placed type carries its own view conjunct, placed during that
RFC's per-arm resolution. The coverage validator inspects arms. A `count` over
an abstract binding may sum per-member metadata counts only for members the
actor's view does not restrict. A polymorphic expansion groups each hop's
frontier by the type half of its packed references, as that RFC's interner
already keys by packed reference, and masks each restricted member type.

**Graph index.** That RFC's phase 3 moves the graph index to one global ordinal
space. Per-view masks ([Rollout](#rollout), Phase 7) target that space: one
bitmap per view and index instance.

**Search.** That RFC refuses `bm25` on a multi-member binding because each
table's statistics differ. Uniform statistics are identical across tables, so
for restricted actors that reason disappears. This RFC does not admit
multi-member `bm25`; a later change may.

**Catalog route, schema hash, descriptors and feeds.** The structured catalog
route that RFC adds serves the schema view. Reads by a restricted actor carry
the digest of the schema view where they would carry `schema_ir_hash`, so
clients cache type names per view and a change to a hidden type is not
observable. Operation descriptors list visible members. In Phase 8, interface
filters on the change feed expand over the view, and a
`ChangeScopeMembershipChanged` refusal names only visible members.

**History.** Both RFCs refuse abstract bindings on historical targets for now.

**Order of implementation.** Whichever lands second carries the integration: if
union scans land first, Phase 2 extends the coverage validator to arms and
expansions; if views land first, that RFC's union scan adds the per-arm view
conjunct when it resolves arms.

### Shares

A live share needs nothing beyond a recipient principal and its grants.

A ***share reconciler*** maintains a copy. It reads the source's view-filtered
change feed as the recipient, applies inserts and updates with
`load --mode merge` and deletions with a mutation, and records the source cursor
in a row of the target graph in the same publication as the data, so a restart
resumes exactly. Because a mutation cannot mix constructive and destructive
changes (`crates/omnigraph/src/exec/mutation.rs:666-683`) and load has no delete
mode, one source commit becomes a branch on the target, written in two steps and
merged once. A `409 schema_boundary` or `410 change_feed_gap` triggers a new
baseline. The reconciler is an idempotent function of the source's accepted
state, not a queue. Before each sync it re-checks the share's terms and stops
when the share is revoked or the terms lapse, as the Eclipse Dataspace
Components policy monitor does.

Hosted copies live in a graph of the same cluster. Delivered copies need a
cluster the recipient controls, because graphs in one cluster share storage and
credentials.

### Lance surfaces relied on

| Surface | Use | Guard |
|---|---|---|
| `Scanner::filter_expr` with `prefilter(true)` | View conjunct on every scan, vector and full-text search | New guard: a `String` BTREE `IsIn` is exact and prefilters KNN and WAND |
| BTREE `SargableQuery::IsIn` | `placement IN R` | Same guard; list sizes 1 to 10^5 |
| `MatchQueryExec::with_base_scorer` and siblings | View-independent BM25 statistics | New guard: supplied statistics change scores; masked rows never enter the heap |
| KNN late search and `+inf` placeholders | Exact-pass safety net | Existing KNN fence in `lance_surface_guards.rs` |
| `count_rows` through `CountPushdown` | Visible count `V` for vector planning | New guard: an exact BTREE count equals a filtered scan count |

## Decisions

### D1. The access model

Roles and a fixed relation vocabulary (membership, three grant roles,
inheritance, requirement, denial), whose facts are graph data, compiled by
OmniGraph into its typed IR. Cedar remains the gate on operations.

Cedar's partial evaluation and type-aware partial evaluation are experimental in
4.11.2, the version in the lockfile, and remained so in 4.13.0. Their residuals
cannot express graph traversal: `resource in Space::"x"` means a transitive
descendant, which would have to be precomputed anyway. OPA's partial evaluation
has the same limit, and its translation to SQL strings produced an injection
fixed in v1.19.0 (July 2026). Invariant 9 already forbids string-built
semantics.

The vocabulary is fixed where Zanzibar, SpiceDB and OpenFGA let each type define
its own relations. A fixed vocabulary is analyzable without a solver, maps
directly to a precomputed readable set, and covers mirrored source ACLs. The role
enum can grow (a `commenter` role, say) without changing the set operations.

### D2. Where access facts live

In the graph, on `main`. Sensitivity classes are securable rows like any other,
not a list in policy. One mechanism governs rows, columns, types and edges, and
one kind of group governs data. The trade-off is that the owner of a fixed
securable can expose a column by granting it, where a policy list would have
needed a reviewed change. Every grant is an audited commit, owners are already
trusted with their securable's rows, and removing a placement from the schema
still needs a bypass actor.

### D3. Placement is a property

Each placement is a single-valued `String` property, filtered by an indexed
`IN`. An edge would make every read a join through the edge table, and keeping
an edge as well as the property would be a second source of truth. The cost is
that "messages in channels Alice owns" is a join on property equality rather than
a traversal; a GQ form that traverses a key-valued property would close that
gap for all modeling, not only access.

### D4. Writes that collide with hidden rows

Refuse, with contentless errors, and accept key-existence disclosure to writers
of a keyed type. Systems either keep a separate copy per label (Accumulo,
Oracle RDF), which breaks key identity; refuse (Stardog, Memgraph, Dgraph,
Oracle Label Security), which discloses existence; or exempt the write
(TigerGraph upserts), which overwrites hidden rows. Postgres documents that
constraint checks "always bypass row security". Opaque keys on placed types make
probing impractical; the lint enforces the guidance.

### D5. Silent filtering versus refusal

Silent filtering with `view.restricted`, definer-rights stored queries for
whole answers, and the closing of every whole-table channel, including
full-text statistics.

### D6. Historical snapshots

Current rules and access edges always. Historical reads of restricted placed
types are refused at first; the dual-placement rule follows. Snowflake applies
current membership to time travel; Databricks applies current ABAC policies to
history and fails closed when a policy column is missing; BigQuery refuses
history to all but administrators on tables that ever had row policies. None
evaluates past policy.

### D7. Vaults

Securables for the long tail; separate graphs where a defect would be
unacceptable or statistics must be isolated; separate clusters where storage
holders, key separation or delivered copies are involved. Per-graph storage
roots, credentials and keys inside one cluster are a gap this RFC names and does
not fill.

### D8. Access rules are facts, not queries

Access is defined by facts under one fixed rule set, not by per-type predicates
over arbitrary data. [Appendix C](#appendix-c-query-based-access-compared)
compares the two. The models are incomparable in expressiveness: query-based
policies can depend on any data but, in GQ, recurse only to a fixed depth; this
model recurses fully but lets access depend only on placement values, access
edges and time. Restricting what access may depend on is what makes the model
efficient and safe:

- the actor's side is computed once per access version and cached across data
  writes, so every read is one indexed `IN`;
- every access change is an enumerable, reviewable edge or placement write;
- the row test is a total equality, so it cannot leak hidden rows through errors;
- "who can see this row?" is a reverse traversal, not an evaluation per actor.

Data-derived rules enter through computed placement, `@starts`, opt-in access
edges, or definer-rights stored queries ([Access derived from data](#access-derived-from-data)).

## Invariants

1. **Respect the substrate.** Views use Lance's scalar indexes, prefilter, KNN
   and FTS execution and the public base-scorer hook. No storage primitive is
   added. Index state stays derived.
2. **One publication door.** Principals, groups, securables and access edges are
   graph rows published through `__manifest`. Share copies publish through load
   and mutate. No side table holds access facts.
3. **One coherent view.** A request captures its authorization state and its
   data snapshot once; a retry captures both again. The plan pins the view
   digest. Authorization is never older than the data it filters.
4. **Publish once.** Write checks run inside attempt validation and the commit
   revalidation; a refused write publishes nothing.
5. **Crash convergence.** No new durable pre-publication effect. The share
   cursor publishes with the data it describes.
6. **Stable identity.** Annotations bind to stable type and property identity.
   Historical property binding must move to stable identity before column views
   serve history.
7. **Derived acceleration.** The shared closure, view specs, public statistics,
   graph-index masks and row masks are derived state keyed by immutable versions.
   Missing or stale derived state is rebuilt or refuses; it never widens a view.
8. **Loud integrity.** Every refusal is typed. A view is never truncated
   silently. A fixed placement naming a missing securable is refused, not hidden.
9. **Typed semantics.** The view is a typed IR leaf resolved from the engine
   context. Caveats and computed placements compile to typed structures. No
   filter is built as a string.
10. **Boundary and engine.** Actors resolve at the server. The engine enforces
    views on reads as well as writes when a policy is installed.
11. **Bounded.** Memberships, securables, depth, caches, maintenance work and
    external reads are bounded; exceeding a bound refuses.
12. **One source of truth.** Access facts exist only in the graph; the roots of
    trust only in policy. Connectors mirror source ACLs into the graph, which is
    then the authority.
13. **Evidence.** See [Evidence and tests](#evidence-and-tests).

Deny-list: incremental maintenance and the share reconciler are idempotent
reconcilers from accepted state, not job queues. Public statistics are built by
explicit reconciliation, not inline on content writes. No process-local lock
fences anything new.

## Compatibility and reversibility

- **Opt-in.** Graphs without the new annotations and version-1 policies behave
  as today.
- **Schema feature.** The annotations set `actor-views` in the contract's
  required features. Builds refuse unknown features
  (`crates/omnigraph-compiler/src/catalog/schema_ir.rs:99-123, 1402-1406`).
  Without that, an older build would treat the annotations as metadata
  (`crates/omnigraph-compiler/src/schema/parser.rs:1074`) and serve the graph
  unfiltered.
- **Schema apply.** Adding these annotations is a new plan step; today any
  annotation change other than `@description` and `@instruction` is refused
  (`crates/omnigraph-compiler/src/catalog/schema_plan.rs:985-1001`). Removing a
  placement requires a bypass actor.
- **Policy.** Version-2 bundles are refused by older builds, which refuse
  unknown versions and fields.
- **Wire.** The `view` response object, the new error codes and the redacted
  explain fields are additive; `openapi.json` is regenerated.
- **Storage.** No format change beyond contract content and a BTREE on each
  per-row placement property, which is derived.
- **Reversal.** Removing the declarations returns a graph to graph-wide
  authorization. Views are derived, so nothing else needs migration.

## Alternatives

- **Do nothing; one graph per audience.** Splits the brain into graphs that
  cannot be joined and multiplies cache budgets.
- **Query-based access** (per-type predicates, as in Postgres row security,
  Hasura or Firestore rules). Rejected for the core under D8; kept as
  definer-rights stored queries.
- **Cedar or OPA residuals compiled to filters.** Rejected under D1.
- **A per-type relation language** (Zanzibar namespaces, SpiceDB, OpenFGA).
  More expressive; deferred because the fixed vocabulary covers mirrored ACLs
  and is analyzable without a solver.
- **One role for groups and securables.** Conflates "member of X" with "can read
  X": making a team's page readable company-wide would hand the team's grants to
  everyone.
- **One access edge with a multi-valued relation.** Mixes chaining membership,
  one-hop grants and subtractive denial in one enum.
- **A public `*` subject.** Every authenticated actor, recipients included,
  would receive company-wide grants.
- **Denial that does not cut membership.** Denying a group would leave its
  grants flowing to its members.
- **Sensitivity classes in policy.** A second group system beside graph
  membership, and two mechanisms for one idea.
- **Placement as an edge.** A join on every read and, alongside a property, a
  second source of truth (D3).
- **Typed securable references.** Before integer node identity, a typed
  reference would need a type tag beside every placement; a shared key
  namespace needs none. Packed references remove that cost, which is why the
  switch is an open question rather than rejected.
- **An external Zanzibar service** (SpiceDB with Materialize, OpenFGA). A second
  store with its own consistency tokens and a dual-write problem; list APIs are
  bounded (OpenFGA ListObjects defaults to 1,000 results in 3 s).
- **Per-row ACL lists** (Accumulo, LabelList). Every row carries principals;
  Lance's LabelList index looks labels up one at a time
  (`lance-index-11.0.0/src/scalar/label_list.rs:140-144`), and access changes
  rewrite rows. A securable makes an access change one edge. Kept as the
  physical encoding for very large per-item audiences ([Bounds](#bounds)).
- **One dataset per securable.** Hard isolation, but table count would follow
  securable count into the hundreds of thousands.
- **Encryption per securable.** Breaks indexes and search; deferred to vault
  infrastructure.
- **Visibility tiers** (readable but not searchable, readable but not for
  agents). The agent case is a `@deny` on an agents team; the search case makes
  visibility depend on the operator and is deferred.
- **Post-filtering.** Leaks through counts, ranks and errors; the cause of the
  incidents in [Appendix B](#appendix-b-prior-art).
- **Branch per share.** A branch inherits its source's data and cannot hold a
  subset.

## Evidence and tests

### Owners to extend

| Behavior | Owner |
|---|---|
| Annotation parsing, validation, IR, features, lint | In-source compiler tests; `schema_plan` tests |
| Policy version 2 | `omnigraph-policy` in-source tests |
| Access semantics | In-source engine tests over the worked example, plus generated access graphs checked against a direct set-semantics evaluator |
| Incremental maintenance | In-source engine tests: after random sequences of access changes, incrementally maintained closures and views equal those built from scratch |
| Engine read and write enforcement | `crates/omnigraph/tests/policy_engine_chassis.rs` |
| Non-interference across operators | `crates/omnigraph/tests/proptest_equivalence.rs`: for generated graphs and access edges, a restricted actor's result equals a bypass result on the materialized view |
| Restricted vector and full-text search | `crates/omnigraph/tests/search.rs` |
| Traversal masks | `crates/omnigraph/tests/integration/traversal_indexed.rs` |
| Write rules, computed placement | `crates/omnigraph/tests/writes.rs` |
| History refusal and dual placement | `crates/omnigraph/tests/integration/point_in_time.rs` |
| View-filtered change feed | `crates/omnigraph/tests/integration/changes.rs` |
| Replay refusal on view change | `crates/omnigraph/tests/integration/engine_v2_plan_replay.rs` |
| Lance facts in the table above | `crates/omnigraph/tests/lance_surface_guards.rs` |
| HTTP surfaces and redaction | `crates/omnigraph-server/tests` `auth_policy` suite; OpenAPI |
| Embedded and remote parity | `crates/omnigraph-cli/tests` parity matrix |
| Access writes concurrent with reads | `omnigraph-dst` scenario |

GQT gains an actor section so view semantics become logic tests; until then the
Rust owners above carry them.

Prior-art lessons shape the generated cases:

- OpenFGA shipped authorization bypasses in models combining exclusion and
  intersection (CVE-2024-31452) and, with a public wildcard, in ListUsers
  (CVE-2026-61709). The generators favor graphs combining the organization
  group, `@requires`, `@inherits` and `@deny`, including denial of groups that
  carry grants.
- SpiceDB evaluates expiry in its datastore to avoid clock skew. Tests cross
  `@starts` and `@expires` boundaries inside a cached view's lifetime.
- Recipients must never read data granted to the organization group.

### Acceptance thresholds

- A bypass actor's queries make no more object-store requests than today (the
  warm read cost instrument).
- A restricted read with a warm view cache adds no object-store request on
  `main` and one version probe on another branch.
- A restricted scan of a placed type with a covered BTREE adds at most the index
  reads of one `IsIn` evaluation per placement property.
- Restricted vector search: recall@10 ≥ 0.95 against view ground truth at
  visible fractions 100%, 10%, 1%, 0.1% and 0.01%, and with views negatively
  correlated with the query; latency within 2× of a bypass actor's at ≥ 10%, and within
  the exact-scan cost at ≤ 1%.
- Restricted full-text search: nDCG@10 with the chosen statistics source within
  10% of index-wide statistics on the fixture.
- An access change is one edge write and governs the next request; the
  maintenance work it causes is proportional to the changed rows and the actors
  they affect.

### Upstream surfaces surveyed

Lance 11.0.0: `dataset/scanner.rs`, `io/exec/knn.rs`, `io/exec/fts.rs`,
`io/exec/filtered_read.rs`, `io/exec/count_pushdown.rs`, `dataset/optimize.rs`,
`dataset/write/merge_insert.rs`, `lance-index` `scalar/expression.rs`,
`scalar/btree.rs`, `scalar/bitmap.rs`, `scalar/label_list.rs`,
`scalar/zonemap.rs`, `scalar/inverted/*`, `lance-select` `mask.rs`. Pages read
in full: the BTREE, Bitmap, LabelList and full-text index formats, and the
performance guide.

## Rollout

Each phase ships independently and leaves the system safe. Every phase lands
with its user-guide changes, a `changelog.d` note when it is user-visible, and
the regenerated `openapi.json` when the wire changes.

| Phase | Ships | Still unavailable |
|---|---|---|
| 0. Hardening | Engine read doors take an actor; refusal of shapes that would drop a filter. | No new contract. |
| 1. Access model and schema placement | Every construct, readable sets, fixed `@in` on types and columns, policy version 2. | Per-row placement. |
| 2. Row reads | Per-row `@in`, the visibility door, masked traversal, restricted search. | Restricted actors cannot write placed types. |
| 3. Row writes | Write checks and access-edge rules. | — |
| 4. Incremental maintenance | Shared closure, targeted view revalidation. | — |
| 5. Agents | Delegated credentials and caveats. | — |
| 6. Data-derived and edge placement | Computed placement; `@in` on edge types. | — |
| 7. Search quality and performance | Public statistics; graph-index masks; row-mask cache. | — |
| 8. History and feeds | Dual-placement history; view-filtered feeds, commits and export. | — |
| 9. Sharing | Recipients, copies, answer capabilities, write-back. | Privacy budgets. |
| 10. Vault infrastructure | Separate RFC. | — |

`implementation` moves to `in-progress` with Phase 0 and to `partial` when
Phase 2 ships.

### Phase 0: read-path hardening

No contract changes.

- Add actor-taking variants of every read door: `query`, `run_query_at`,
  `explain_query`, `query_inspected`, `replay_bound_plan`
  (`crates/omnigraph/src/exec/query_doors.rs`), `snapshot_of`,
  `commit_changes_page`, `poll_change_feed`, `get_commit`, `list_commits`
  (`crates/omnigraph/src/db/omnigraph.rs`), the export and baseline cuts
  (`crates/omnigraph/src/db/omnigraph/export.rs`) and `read_blob_at`
  (`crates/omnigraph/src/blob.rs`). Handlers pass the resolved actor. When a
  policy is installed, the engine applies the `read` and `export` gates itself.
- Make `Predicate::gq_filters()` match exhaustively.
- Make the `MetadataCount` lowering refuse a scan spec that carries a filter.
- Carry foreign facts in errors as structured fields.
- Make `Omnigraph::graph_index()` crate-private.

Exit: the canonical workspace gates pass; `policy_engine_chassis.rs` covers an
embedded read denied by policy.

### Phase 1: access model and schema placement

Ships type- and column-level confidentiality on the full access model.

- Compiler: validate the annotations; store them in typed IR bound to stable
  ids; derive `actor-views`; add schema-apply steps; check fixed `@in` keys
  against securables on `main`.
- Engine: authorization capture; the readable-set computation with membership,
  denial cutting membership, grants, inheritance, requirements, edge windows and
  the organization group; the byte-bounded view cache; the schema view and
  memoized view catalogs; global uniqueness of securable keys and actor ids;
  refusal to delete a securable a fixed `@in` names.
- Policy: version 2 `views.organization`, `views.bypass` and `views.limits`,
  validated at cluster apply.
- Until Phase 2: access edges are readable only by bypass actors; group and
  securable rows follow their type's placement, which can only be a fixed
  `@in("key")`; schema apply refuses a per-row `@in`.
- Server: schema view on `/schema`; filtered `/queries`; explain redaction; the
  `view` response object; `view_restricted` on history, feeds and export for any
  actor whose view hides something; the operator tools.
- GQT: an actor section in the case format.

Exit: the non-interference property holds for schema views; a hidden name and
an undeclared name produce identical errors; the readable-set computation agrees
with the set-semantics evaluator over generated access graphs, including denial
of granting groups, cycles and window boundaries; recipients never read
organization grants.

### Phase 2: row reads

Restricted actors cannot yet write placed types; they are refused with
`view_write_denied`.

- Per-row `@in` on node types with a BTREE on each placement property.
- The `ViewSet` IR leaf, view conjuncts in scan specs, the coverage validator,
  the view digest in plan assumptions.
- Per-hop frontier masks in indexed traversal; refusal of CSR, degree shortcuts
  and CSR-derived search gates for restricted types.
- Vector planning from the visible count; full-text search for restricted
  actors with uniform statistics; Blob reads checked against row visibility.
- Visibility of access data: rows by their own placement, access edges by their
  endpoints, deny edges to owners only.
- Server refusal of unauthenticated and policy-less modes for `actor-views`
  graphs.
- Polymorphic types, if union scans have landed: per-arm conjuncts, validator
  coverage of arms and expansions, member sets from the schema view.
- Lance guards for `IsIn` prefiltering, the base scorer and `CountPushdown`.

Exit: the non-interference property holds over scans, aggregates, single- and
multi-hop traversal, `count` and `not` blocks, anti-joins, and vector, full-text
and fused search at visible fractions from 100% to 0.01%; the cost and recall
thresholds hold; a DST scenario interleaving access writes with reads never
returns a row its request's authorization state hides.

### Phase 3: row writes

- Committed-state lookup of placement values; checks in
  `validate_staged_mutation`, `validate_changeset` and merge evaluation.
- Personal-securable default, `placement_required`, `view_write_denied`,
  `key_conflict` without ids, `delete_blocked`, overwrite refusal, edge endpoint
  visibility, view-scoped matching and counts.
- Access-edge rules: owner roles, `main` only, `securable_owner_required`.
- Server stripping of foreign facts for restricted actors.

Exit: `writes.rs` and `policy_engine_chassis.rs` cover every row of the write
table; each refusal leaves the manifest head, table pins and lineage unchanged.

### Phase 4: incremental maintenance

- The shared closure keyed by access version, moved forward from the access
  tables' changes and rebuilt from scratch when their history is gone.
- Targeted revalidation that carries unaffected views forward.
- Compressed bitmaps over securable ordinals for readable sets.

Exit: incrementally maintained closures and views equal from-scratch ones over
generated change sequences; the maintenance threshold holds at connector churn.

### Phase 5: agents

- Delegated credentials with subject, actor and audience; caveats compiled into
  the view spec and closed under inheritance; acceptance of the RFC 8693 `act`
  claim only for credentials of this form.
- MCP audience check; no token passthrough.

Exit: the server identity suites cover delegated credentials and caveats.

### Phase 6: data-derived and edge placement

- Computed placement: the expression subset (the row's own columns and
  constants), evaluation in write validation, and the schema-apply step for a
  changed expression.
- `@in` on edge types, with view conjuncts on edge reads.

Exit: the non-interference property holds with computed and edge placements;
writers cannot set a computed placement.

### Phase 7: search quality and performance

- Public statistics built by explicit index reconciliation, with uniform
  statistics as the fallback; a recall sampling instrument.
- Per-view masks over a graph-index instance, keyed by the instance and the
  node-table versions; CSR re-enabled for restricted types. With the polymorphic
  types RFC's global ordinal index, one bitmap per view covers every node type.
- An optional per-view row-mask cache, after a guard covers Lance's
  `CountPushdown` with a row mask.

Exit: the ranking threshold holds on the fixture; traversal and search cost
instruments show no regression against Phase 2 at equal results.

### Phase 8: history and feeds

- Historical reads bind properties by stable identity (prerequisite).
- Dual-placement historical reads; view-filtered change feed (digest in cursors,
  visibility transitions as inserts and deletes, projected equality);
  view-filtered commits, commit changes, export and baseline.

Exit: `point_in_time.rs` and `changes.rs` cover moved rows, revoked members and
hidden-column changes; cursors refuse a changed view.

### Phase 9: sharing

- `shares` in cluster configuration; live shares; the share reconciler for
  hosted and delivered copies, with the cursor row, baselines and terms checks.
- Definer-rights stored queries with parameter domains and invocation logs.
- Write-back proposals; per-actor read admission.

Exit: a hosted copy equals the materialized view of its recipient after every
reconciled commit; revocation deletes the copy and its proposals.

### Phase 10: vault infrastructure

A separate RFC: per-graph storage roots, credentials and keys in one cluster;
non-discoverable graphs; a cache budget shared across graphs; read-many,
write-one queries across graphs.

## Unresolved questions

1. Which full-text statistics restricted actors get: public or uniform
   (`blocked_on`).
2. The syntax of computed placement, and how a changed expression rewrites
   existing rows within one publication.
3. Whether a GQ form that traverses a key-valued property should accompany
   Phase 2, so placement can be traversed like an edge.
4. Whether denied principals should see the deny edges that bar them, as some
   ethical-wall practice expects, or only owners.
5. In Phase 8, whether commits with no visible change are listed to restricted
   actors.
6. Whether design partners accept key-existence disclosure to writers, or need
   uniqueness scoped to a placement.
7. At what audience size the item-side reader encoding replaces `IN` for a type,
   and whether that choice is declared or measured.
8. Whether placements store packed securable references once integer node
   identity lands, replacing the shared securable-key namespace.

## Decision log

- 2026-10-11: Drafted. The design was checked against the code at `6e31d0ab` and
  Lance 11.0.0; [Appendix A](#appendix-a-validated-assumptions) records every
  assumption with its verdict.
- 2026-10-11: Aligned with the polymorphic types draft
  (ModernRelay/omnigraph#884): member sets from the schema view, placements on
  interface properties fixed across members, removal of a placement restricted,
  polymorphic access edges, per-arm view conjuncts, global-ordinal masks.
- 2026-10-11: Rewrote the declaration layer to a single scope concept, replacing
  thirteen annotations, four policy keys, four new actions, visibility tiers,
  integrity labels and compound securables.
- 2026-10-12: Revised from a first-principles review. The scope concept split
  into the `@group` and `@securable` roles, because conflating membership with
  readership let a visible team page hand out the team's grants. `@inherits` was
  added: without union over securables the expressions are not closed, and
  hierarchies, multiple parents and clearance ladders needed workarounds.
  Denial now cuts membership through a denied group. The public `*` subject
  became the organization group defined by policy, so recipients never receive
  company-wide grants, and derived content uses `@inherits` with `@requires`
  instead of a `*` grant. `@in` extends to edge types; caveats cover inherited
  securables; edge windows gained `@starts`. The semantics are stated as a
  least fixed point with one negation layer, with a derivation of every
  construct. Names follow the field: `@securable`, `@requires` (was `@within`),
  `role` (was `level`), `bypass` (was `unrestricted`), recipient. Added computed
  placement, incremental maintenance of readable sets (Phase 4), the item-side
  encoding boundary for per-item audiences, D8 and
  [Appendix C](#appendix-c-query-based-access-compared).
- 2026-10-12: The polymorphic types draft (ModernRelay/omnigraph#884) adopted
  this RFC's rules for abstract types and withdrew `policy_denied_member` and
  `policy_filtered`, closing the cross-RFC question. [Polymorphic types](#polymorphic-types)
  now follows that draft's current design: packed node references from integer
  node identity instead of tag columns, which reopens typed placement values as
  a question.

## Appendix A: validated assumptions

Verdicts: ✓ confirmed, ◐ partly true (the design uses the corrected form),
✗ false (the design does not rely on it).

### Lance 11.0.0

| Claim | Verdict | Evidence |
|---|---|---|
| `filter_expr` bypasses SQL; `prefilter` defaults to false | ✓ | `dataset/scanner.rs:1585, 1286` |
| Row-address prefilter honored by KNN, indexed FTS, flat FTS, plain scan | ◐ | Mask is in `_rowid` space; applied after scoring on flat FTS; demotes the filter on plain scans (`scanner.rs:1466-1470, 4995, 3298-3306`) |
| BTREE and Bitmap `IsIn` exact, unbounded, NULL disables index | ✓ | `lance-index` `scalar/expression.rs:348-352`; `btree.rs:3347`; `bitmap.rs:1848` |
| LabelList looks labels up sequentially | ✓ | `scalar/label_list.rs:140-144` |
| Indexed FTS masks before the top-k heap | ✓ | `scalar/inverted/wand.rs:2299` |
| BM25 statistics are index-wide, including filtered and deleted rows | ✓ | `scalar/inverted/index/search.rs:53-63`; `scorer.rs:197` |
| External BM25 statistics can be supplied | ◐ | Only below the scanner, `with_base_scorer` (`io/exec/fts.rs:624, 2175, 3058, 3485`) |
| No automatic flat switch for selective KNN; `+inf` placeholders | ✓ | `io/exec/knn.rs:1710-1782` |
| `count_rows(None)` is manifest-only | ✓ | `dataset/fragment.rs:1366-1392` |
| Mask plus `count_rows` | ◐ | `CountPushdown` expects a `ScalarIndexExec` (`io/exec/count_pushdown.rs:228-235`); untested, guarded before use |
| Compaction materializes deletions above 10%; stable ids skip index remap | ✓ | `dataset/optimize.rs:322, 2580` |
| Merge-insert cannot fail on a conditional match | ✓ | `merge_insert.rs:425-449`; `assign_action.rs:107-139` |
| WAND switches to flat at ≤ 10% allow-list for OR queries | ✓ | `wand.rs:201-206` |

### Engine read path

| Claim | Verdict | Evidence |
|---|---|---|
| Non-pushable conjuncts become an in-memory filter after `nearest` top-k | ✓ | `crates/omnigraph-planner/src/optimizer.rs:1426-1461` |
| Lowering silently skips conjuncts | ◐ | Guarded by `validate_plan_values` (`crates/omnigraph/src/engine/typed_value.rs:826-833`), except `MetadataCount` (`engine/lower.rs:473-487`) |
| `prefilter(true)` on every filtered scan | ✓ | `crates/omnigraph/src/engine/scan.rs:248-251` |
| List parameters lower to `in_list` | ✓ | `scan.rs:1093-1119` |
| BFS never reads intermediate nodes; edge reads unfiltered | ✓ | `crates/omnigraph/src/engine/graph.rs:1132-1472, 1647-1662` |
| CSR is a whole-graph cache with no actor | ✓ | `crates/omnigraph/src/graph_index/mod.rs:168-245`; `runtime_cache.rs:24-57` |
| Degree shortcut and `MetadataCount` require an unfiltered spec | ✓ | `engine/lower.rs:230-277`; `optimizer.rs:1603-1647` |
| Interfaces cause polymorphic scans | ✗ | Bindings are concrete (`typecheck.rs:1785-1790`) |
| `Predicate::gq_filters()` keeps every variant | ✗ | Drops non-`Gq` variants (`crates/omnigraph-planner/src/logical.rs:125-140`) |
| Multi-hop traversal accepts different endpoint types | ✗ | Requires equal endpoint types (`crates/omnigraph/src/engine/operators/expand.rs:177-183`) |
| Multi-hop traversal accepts a zero minimum or an unbounded maximum | ✗ | Requires `{1,n}` with `n` given (`crates/omnigraph-compiler/src/query/typecheck.rs:2045-2066`) |
| Engine read doors take no actor | ✓ | `crates/omnigraph/src/exec/query_doors.rs:41-170` |
| Explain exposes whole-table facts | ✓ | `engine/explain.rs:236, 272`; `physical.rs:363-370`; `gate.rs:58-71` |

### Writes and schema

| Claim | Verdict | Evidence |
|---|---|---|
| Keyed insert is a full-row upsert | ✓ | `crates/omnigraph/src/exec/mutation.rs:425-426, 1314-1318`; `table_store.rs:3442-3443` |
| Overwrite replaces all rows of a type | ✓ | `crates/omnigraph/src/loader/mod.rs:765-772, 939-957` |
| Delete cascades and counts hidden edges | ✓ | `mutation.rs:1717-1741` |
| Constraint messages name other rows | ✓ | `crates/omnigraph/src/validate.rs:1259, 1295-1296` |
| A before image exists at validation | ◐ | Not held; obtainable from the committed state (`validate.rs:391-433`) |
| `@index` on a `String` builds a BTREE | ✗ | Builds full-text (`table_ops.rs:582-605`) |
| Unknown annotations are refused | ✗ | Kept as metadata (`parser.rs:1074`); unknown features are refused (`schema_ir.rs:1402-1406`) |
| Edge properties may be named `from` | ✗ | `from` and `to` are reserved insert parameters on edges (`docs/user/schema/index.md`) |
| Schema apply is main-only | ✓ | `crates/omnigraph/src/db/omnigraph/schema_apply.rs:153-162` |
| Edge endpoints have BTREEs | ✓ | `table_ops.rs:874-890` |

### Server, policy and cluster

| Claim | Verdict | Evidence |
|---|---|---|
| Reads authorized only in handlers; MCP shares them | ✓ | `crates/omnigraph-server/src/handlers.rs`; `mcp.rs:272-310` |
| Static tokens without policy allow reads; open mode allows all graph actions | ✓ | `handlers.rs:651-668` |
| Signed and OIDC actors without policy are denied | ✓ | `handlers.rs:622-626` |
| Delegation claims accepted | ✗ | OIDC rejects `act` and `impersonator` (`oidc_identity.rs:346-347`) |
| Principal kind exists | ✓ | `data_tokens.rs:26-29` |
| Policy is allow-only, compiled with `format!` | ✓ | `crates/omnigraph-policy/src/lib.rs:236-240, 825-866` |
| Per-actor limits exist | ◐ | Writes only (`workload.rs:15-19`) |
| Durable read audit exists | ✗ | Decision logs only when a policy is installed (`handlers.rs:575-589`) |
| Snapshot reads use current policy | ✓ | `handlers.rs:1764-1784` |
| Historical reads rebind tables by stable identity | ✓ | `crates/omnigraph-catalog/src/lib.rs:608-656` |
| One snapshot per query | ✓ | `crates/omnigraph/src/db/graph_coordinator.rs:52` |
| Graphs in a cluster share storage and credentials | ✓ | `crates/omnigraph-cluster/src/store.rs:240-242`; `crates/omnigraph-storage/src/lib.rs:1247` |
| Per-graph Lance caches have no shared budget | ✓ | `crates/omnigraph-core/src/lance_access.rs:38-42` |
| Read caches flush on every observed advance of `main` | ✓ | `crates/omnigraph/src/db/omnigraph.rs:2026-2034` |

## Appendix B: prior art

How each part of the model maps to prior art:

| Part | Prior art | Relation |
|---|---|---|
| Groups with nested membership | Zanzibar `group#member` usersets; Active Directory nested groups | Same |
| Securables that hold data and receive grants | SQL Server securables; Databricks Unity Catalog securable objects; SharePoint `SecurableObject`; OCI compartments | Same |
| Membership chains, grants are one hop | Zanzibar's Leopard index (group-to-group closure, then member-to-group); AD groups and ACL entries | Same shape |
| `@grant`, `@inherits`, `@requires`, `@deny` | Zanzibar userset rewrites (union, intersection, exclusion; `tuple_to_userset` for inheritance); SpiceDB `+ & -`; OpenFGA `or`, `and`, `but not` | Same set operations, fixed vocabulary |
| Organization group | Google "anyone in the organization"; AD Domain Users | Same; differs from the public `user:*` of OpenFGA and SpiceDB |
| Deny-overrides | XACML; Windows deny entries; Purview Information Barriers; Ranger tag denies; Databricks DENY | Same; avoids GraphDB first-match ordering, Neo4j denies that fail open on null, and Drive limited access overridden by a group grant |
| `@starts`, `@expires` | SpiceDB expiring relationships; OpenFGA conditions; Azure PIM | Same |
| Per-row placement with `IN` | Snowflake row access policies with mapping tables, Postgres row security, Elasticsearch document-level security, Glean and Azure AI Search security trimming | Same |
| Several placements, all required | Oracle Label Security compartments, MarkLogic compartment security, classified compartments | Same |
| Fixed placement of a column or type | BigQuery policy tags; Unity Catalog tags driving row filters and column masks | Close |
| Derived content | FIDES and CaMeL reader-set intersection | Same; Purview takes the highest label instead |
| Delegation as intersection | Microsoft delegated permissions; RFC 8693; Macaroon and Biscuit attenuation | Same |
| Access facts in the data graph | Fong's relationship-based access control; Snowflake mapping tables | Variant: one versioned graph, consistency by snapshot instead of Zanzibar zookies |
| Shared closure with incremental maintenance | Leopard (offline index plus incremental layer); AuthZed Materialize | Same shape, keyed by access version |
| Fixed rule set, facts from users | Biscuit and Oso use Datalog with user rules | Same logic fragment, no user rules |
| Bypass | Postgres `BYPASSRLS`; Oracle `EXEMPT ACCESS POLICY`; Databricks exempt principals | Same |

Sources consulted:

- Zanzibar (USENIX ATC 2019), the new enemy problem and Leopard.
  https://www.usenix.org/system/files/atc19-pang.pdf
- Cedar type-aware partial evaluation, RFC 0095.
  https://github.com/cedar-policy/rfcs/blob/main/text/0095-type-aware-partial-evaluation.md
- OPA partial evaluation fragment and the v1.19.0 SQL injection fix.
  https://www.openpolicyagent.org/docs/filtering/fragment
- SpiceDB consistency, expiring relationships and Materialize; OpenFGA
  ListObjects limits and public access.
  https://authzed.com/docs/spicedb/concepts/consistency ,
  https://openfga.dev/docs/modeling/public-access
- OpenFGA advisories CVE-2024-31452 and CVE-2026-61709.
  https://osv.dev/vulnerability/CVE-2026-61709
- OpenID AuthZEN Authorization API 1.0.
  https://openid.net/specs/authorization-api-1_0.html
- PostgreSQL row security, constraint covert channels and policy recursion.
  https://www.postgresql.org/docs/current/ddl-rowsecurity.html
- Firestore security rules ("rules are not filters", document access limits);
  Hasura and Supabase row permissions.
- Oracle Label Security, Db2 LBAC, MarkLogic compartments, Accumulo visibility
  labels, OCI compartments.
  https://accumulo.apache.org/docs/2.x/security/authorizations
- SQL Server securables; Databricks Unity Catalog securable objects and ABAC;
  SharePoint `SecurableObject`; BigQuery column-level security.
  https://cloud.google.com/bigquery/docs/column-level-security-intro
- Neo4j fine-grained access control and its documented limits.
  https://neo4j.com/docs/operations-manual/current/authentication-authorization/limitations/
- SurrealDB GHSA-vjjx-rfw4-rmfc and CVE-2026-49997; Dgraph CVE-2026-41327;
  TigerGraph row policy exemptions; Stardog and AllegroGraph.
- Rizvi, Mendelzon, Sudarshan, Roy (SIGMOD 2004); Kanza et al. (EDBT 2006);
  Fong (CODASPY 2011).
- Elasticsearch document-level security and global statistics; Büttcher and
  Clarke (FAST 2005); STRESS (IEEE S&P 2017).
  https://stressattack.github.io/paper/sp17stress.pdf
- Filtered vector search: ACORN (SIGMOD 2024), Filtered-DiskANN (WWW 2023),
  Curator, HoneyBee (SIGMOD 2026); Qdrant, Weaviate, pgvector, OpenSearch and
  Elasticsearch cut-offs.
- Microsoft Purview label inheritance for Copilot, Restricted Content Discovery,
  DLP for Copilot, Information Barriers.
  https://learn.microsoft.com/en-us/purview/ai-m365-copilot
- iManage Security Policy Manager; Harvey and Intapp ethical walls.
  https://www.harvey.ai/blog/agents-and-ethical-walls
- Apache Atlas classification propagation; Ranger tag policies.
- FIDES (arXiv 2505.23643), CaMeL (arXiv 2503.18813), design patterns for LLM
  agents (arXiv 2506.08837).
- Snowflake row access policies and secure data sharing; BigQuery time travel
  with row-level security; Databricks ABAC with time travel and Delta Sharing.
- Slack Connect, Teams shared channels, AWS Clean Rooms analysis rules and
  differential privacy, Snowflake aggregation policies, Solid WAC and ACP, ODRL
  2.2, Eclipse Dataspace Components policy monitor, DoltHub forks, lakeFS hooks.
- RFC 8693 token exchange; MCP authorization 2026-07-28; Biscuit; Macaroons;
  UCAN; XACML combining algorithms; GCP basic roles; Azure RBAC.

## Appendix C: query-based access compared

***Query-based access*** defines an actor's view of a type as the result of a
predicate over the data and the actor: `V(a) = q(G, a)`, as in Postgres row
security, Oracle VPD, Hasura and Supabase permissions, Firestore rules and
Datomic filters. This model is query-based access with one fixed query: a row is
visible when `placement IN R(a)`, and `R(a)` comes from the fixed rule set in
[Computing access](#computing-access).

### Expressiveness

Both models can represent any access relation given enough facts (a securable
per row, or an ACL table queried by a predicate). The comparison is which rules
can be stated once and stay correct as data changes. With GQ as the policy
language, a policy is first-order logic with counting and bounded paths; this
model is a fixed Datalog program over designated relations plus an equality test
per row.

| Rule class | Example | Query-based (GQ) | This model |
|---|---|---|---|
| Nested groups, any depth | Teams in departments | Only to a fixed depth | ✓ |
| Inheritance mixing edge kinds | Folder → folder → team space | Only fixed path shapes | ✓ |
| Union of audiences | Two teams share a space | ✓ (several policies) | ✓ |
| Intersection over a variable number of sources | A summary of N sources | Needs policies calling policies | ✓ `@requires` |
| Exception across all types | Ethical wall | Repeated in every policy | ✓ once |
| Access that expires or starts | Contractor until December; embargo | ✓ | ✓ `@expires`, `@starts` |
| Row attribute equals an actor attribute | Deal region = rep's region | ✓ | ✓ when regions are securables |
| Row attribute threshold | Deals ≥ $1M board-only | ✓ | Computed placement |
| Row value against an actor value | Amount ≤ my approval limit | ✓ | Partial: bucket into tiers |
| Ordered clearances | Secret readers read confidential | ✓ | ✓ `@inherits` chain |
| Content of other rows | Notes mentioning my accounts | ✓ | Opt-in access edge, or definer query |
| Counting | Visible after three approvals | ✓ | Definer query |
| Request context | Office hours, managed device | ✗ in GQ | ✗; Cedar or the gateway |
| Column visibility per row | Phone visible to the same team | ✓ | Separate type, placed per row |

The models are incomparable: query-based policies are stronger on rules derived
from data, this model on structure, composition and exclusion. Allowing both
arbitrary data dependence and recursion gives full stratified Datalog over the
graph (Oso's Polar, Biscuit), which contains both and loses caching, leak
freedom and "who can see" analysis.

### Efficiency

Both models execute `rows WHERE label IN S(a)` for most rules. This model
computes `S(a)` once per actor and access version; query-based access recomputes
it in every query, for every type and traversal hop, and cannot cache it because
it may depend on any data.

| Operation | This model | Query-based |
|---|---|---|
| Preparing an actor's access | Once per access version, incrementally maintained | None |
| Each read, per placed type | One BTREE `IN` | The policy's joins and hops, every query |
| Recursive and composed rules | Paid once, building the view | Paid on every read |
| Vector and full-text top-k | Prefilter from the cached set | Allowed set built inside each query |
| Data write | One batched lookup of placements | The write predicate per written row |
| Access change | One edge write and targeted revalidation | Free, but any data write may change access |
| Who can see this row | Reverse traversal | Evaluation per actor |

Query-based access is cheaper for rules over the row's own columns (no
write-time work), for rules whose actor side is tiny, and when actors read
rarely between access changes. Firestore shows the cost of the general case at
scale: it limits each rule evaluation to 10 document reads and refuses queries
that might return disallowed documents ("rules are not filters").

### Examples

The query-based side uses a hypothetical `allow read` block whose body is a GQ
match over the row and the actor's principal `$me`.

**Folder hierarchy.**

```gq
allow read Document as $d for $me { match { $d inFolder $f  $me canView $f } }
allow read Document as $d for $me { match { $d inFolder $f  $f childOf{1,16} $a  $me canView $a } }
```

```pg
edge FolderParent: Folder -> Folder @inherits
node Document @in(folder) { file_id: String @key  folder: String }
```

The query-based version walks up to 16 folders on every read; this model walks
them once per actor and access version.

**Ethical wall.**

```gq
allow read Document as $d for $me {
  match { $d inMatter $m  $me memberOf $t  $t staffs $m  not { $me screenedFrom $m } }
}
```

```pg
edge Staffs: Team -> Matter @grant { role: enum(viewer, editor, owner)  @key(@src, @dst) }
edge Screens: Team -> Matter @deny
```

The query-based `not` must be repeated in the policy of every type related to a
matter; this model's deny sits on the matter and covers every type placed there.

**Summary of two sources.**

```gq
allow read Summary as $s for $me {
  match { $s derivedFrom $x  not { $s derivedFrom $y  not { readable($me, $y) } } }
}
```

```pg
edge DigestBase: Digest -> Channel @inherits
edge DigestNeedsChannel: Digest -> Channel @requires
edge DigestNeedsMatter: Digest -> Matter @requires
```

The query-based version needs `readable`, a policy calling other policies; this
model intersects audiences.

**Notes mentioning my accounts.**

```gq
allow read Note as $n for $me { match { $n mentions $a  $me owns $a } }
```

The query-based version is one line, and every `mentions` edge written by an
extraction pipeline then grants access. This model keeps the note where its
source placed it and serves account owners through a definer-rights stored
query, or makes mentions an access edge by explicit opt-in.
