---
rfc: "2026-10-11-actor-views-and-subgraph-sharing"
title: "Actor views and subgraph sharing"
track: maintainer
status: draft
implementation: not-started
authors:
  - ragnorc
created: 2026-10-11
updated: 2026-10-11
discussion: null
supersedes: []
superseded_by: []
blocked_on:
  - "Measure full-text ranking quality under view-independent statistics (public and uniform) against index-wide statistics on a checked-in fixture, and choose the restricted-actor statistics source."
  - "Measure readable-set construction at the target scale (10^5 principals, 10^6 scopes, membership depth 16) on main, warm and cold, and confirm the limits in Bounds."
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

Views are built from one concept. A ***scope*** is an ordinary node that data
lives in: a team, a Slack channel, a deal room, a sensitivity class. Edges
between principals and scopes say who belongs to what, who may read or write
what, and who is barred from what. Annotations in the schema place each row,
column or type in scopes. An actor sees a piece of data when it can read every
scope the data lives in. Membership, grants and placements are graph data;
the schema only declares which types and edges play which part.

The observable contract is ***non-interference***: an actor's answer to any
query equals the answer the same query would give on a graph that physically
contained only that actor's view. Counts, aggregates, traversals, search ranks
and errors are computed on the view, never filtered afterwards. The contract is
enforced at one place per mechanism, the ***visibility door***, and a
differential test proves it for every operator.

External sharing needs no further access concept. A ***share*** is the view of
a principal that stands for the recipient. It is served live, or materialized
into a separate graph kept current from the source's change feed.

What does not change: Cedar remains the gate on operations; a graph that
declares no scopes behaves exactly as today; anyone holding storage credentials
still sees everything, as today.

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
answer(q, G, a) = answer(q, materialize(V(a)), unrestricted)
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

Seven annotations, plus `@expires` on access edges, declare which types and
edges take part. Each has one meaning.

| Annotation | On | Meaning |
|---|---|---|
| `@principal` | a `String` property | The node stands for the authenticated actor whose id equals this value. |
| `@scope` | node type | Rows of this type are scopes. Scope keys share one namespace across every scope type. |
| `@member` | edge type, subject → scope | The subject belongs to the scope. Membership chains. |
| `@grant` | edge type, subject → scope, with a `level` | Everyone in the subject's membership closure gets `viewer`, `editor` or `owner` on the scope. One hop. |
| `@deny` | edge type, subject → scope | Everyone in the subject's membership closure never reads the scope, whatever grants say. |
| `@within` | edge type, scope → scope | The source scope is readable only if every target is readable too. |
| `@in(p)`, `@in("key")` | node type or property | Where data lives: each row in the scope named by its property `p`, or a whole type or one column in a fixed scope. |
| `@expires` | a `DateTime` property of a `@member` or `@grant` edge | The edge stops counting at this time. |

A ***subject*** is a principal node or a scope. The levels are ordered
`viewer < editor < owner`: a viewer reads rows in the scope, an editor also
writes them, an owner also changes who has access.

They map one-to-one onto set operations over who can read a scope:
`@member` builds a closure, `@grant` is union, `@deny` is difference and
`@within` is intersection. Google Zanzibar's userset rewrites (union,
intersection, exclusion) have the same shape
([Appendix B](#appendix-b-prior-art)).

### Declaring the model

```pg
node Person @scope {
  email: String @key
  actor: String? @principal @unique     // "principal:<sub>" or a static-token actor id
}

node Team @scope @in(slug) {           // a team is visible to those who can read it
  slug: String @key
}

node Space @scope @in(slug) {
  slug: String @key
  name: String
}

node Class @scope {                    // fixed scopes for columns and types
  slug: String @key
}

edge MemberOf: Person -> Team @member { expires: DateTime? @expires }
edge SubteamOf: Team -> Team @member
edge TeamAccess: Team -> Space @grant {
  level: enum(viewer, editor, owner)
  expires: DateTime? @expires
  @key(@src, @dst)
}
edge ClassAccess: Team -> Class @grant {
  level: enum(viewer, editor, owner)
  @key(@src, @dst)
}
edge Screened: Team -> Space @deny
edge Within: Space -> Space @within

node Message @in(space) {
  source_ref: String @key               // opaque source-system id
  space: String?
  text: String
  author_email: String? @in("pii")
}

node Salary @in("compensation") {
  person: String @key
  amount: I64
}
```

Rules that complete the model:

- **One key namespace.** Scope keys are unique across every `@scope` type. The
  engine checks it when a scope row is written. A placement value therefore
  needs no type, and connectors use namespaced keys such as `slack:C123`,
  `team:eng` or a person's email.
- **Actor ids are unique** across every `@principal` property. A graph may have
  several principal types (people, agents, share recipients).
- **Everyone.** A scope row keyed `*` is the ***everyone scope***: every actor
  that passes the `read` action is implicitly its member. A grant from it is a
  company-wide grant.
- **Placement.** `@in(p)` on a type places each row in the scope whose key is
  the row's `p`. `@in("key")` on a type places the whole type; on a property,
  that column. A type may carry several `@in`; data is visible only when every
  one of its scopes is readable. A null `p` places the row in no scope:
  only unrestricted actors see it.
- **Fixed placements are checked references.** Schema apply refuses an
  `@in("key")` whose key is not a scope on `main`. Deleting a scope row that a
  fixed `@in` names is refused. A misspelling therefore fails loudly instead of
  hiding a column from everyone.
- **Scopes are placed like any row.** `node Space @scope @in(slug)` places each
  space in itself, so only those who can read it know it exists. `Person` is
  unplaced, so people stay visible.
- **Unplaced types** stay visible to every actor with `read`, as today.

Schema apply validates the declarations: `@principal` on a `String`;
`@member`, `@grant` and `@deny` edges from a principal or scope type to a scope
type; `@grant` edges with a `level` enum drawn from the three levels and keyed
by `(@src, @dst)`; `@within` between scope types; `@in(p)` naming a `String`
property that is not a list, Blob or vector; `@expires` on a `DateTime` of
a member or grant edge. These declarations set the schema feature
`actor-views`, which builds that do not implement this RFC refuse to open
([Compatibility](#compatibility-and-reversibility)). Each `@in(p)` property
gets a BTREE index.

A schema lint warns when a type with a per-row `@in` is keyed on a property
that looks human-meaningful, such as an email or a name, because writers can
probe keys.

### Computing a view

For actor `a`, the engine computes the set `R` of scopes `a` can read:

```text
C  = {a's principal} ∪ {*} ∪ every scope reached from them over unexpired @member edges
L(s) = viewer if s ∈ C, raised to the highest level of any unexpired @grant x → s with x ∈ C
D  = every scope s with a @deny edge x → s, x ∈ C
R  = { s : L(s) ≥ viewer, s ∉ D, and every @within target of s is in R }
W  = { s ∈ R : L(s) ≥ editor }        O = { s ∈ R : L(s) ≥ owner }
```

Membership is the only relation that chains: "who is in what" is one closure,
and a grant is a single hop from it, so a grant never extends through another
grant. Inheritance from a parent folder is written explicitly by making the
parent's audience a member of the child's audience; a connector does this when
it mirrors ACLs. Denial is applied after every grant, so no grant brings a
denied scope back. `@within` edges must not form a cycle; a write that would
create one is refused.

An actor whose principal node is itself a scope (a `Person` above) can read its
own scope, its ***personal scope***.

Worked example:

```text
alice ─member→ team:eng ─member→ team:all ─viewer→ space:general
               team:eng ─editor→ space:eng
bob   ─member→ team:legal ─member→ team:all
               team:legal ─viewer→ space:deal-acme
bob   ─member→ team:acme-wall ─deny→ space:deal-acme
carol ─member→ team:all
carol ─editor→ space:deal-acme
team:hr ─viewer→ pii, compensation
* ─viewer→ space:acme-digest ─within→ space:deal-acme
                             ─within→ space:general
```

| | alice | bob | carol |
|---|---|---|---|
| Memberships | team:eng, team:all | team:legal, team:all, team:acme-wall | team:all |
| Granted | space:general, space:eng (editor), space:acme-digest | space:general, space:deal-acme, space:acme-digest | space:general, space:deal-acme (editor), space:acme-digest |
| Denied | — | space:deal-acme | — |
| `space:acme-digest` | ✗ cannot read space:deal-acme | ✗ space:deal-acme denied | ✓ |

`space:acme-digest` holds summaries drawn from both sources: the `*` grant opens
it to everyone and the two `@within` edges narrow it to those who can read
both. Bob is granted the deal room through legal, but the wall's deny wins.

### Placement in practice

- A `Message` row is visible to `a` only if its `space` is in `R(a)`. For alice
  in the example, a `count` over messages counts rows in `space:general` and
  `space:eng`, and the response carries `"view": { "restricted": true }`.
- `author_email` exists for actors who can read `pii`; for everyone else it is
  absent, and naming it is an unknown-name error.
- `Salary` exists only for actors who can read `compensation`.
- An edge is visible when both endpoints are visible.

Fixed scopes are created and granted as data:

```gq
query create_pii() {
  insert Class { slug: "pii" }
}

query grant_pii($team: String) {
  insert ClassAccess { from: $team, to: "pii", level: "viewer" }
}
```

### Access data is itself placed

Membership, grant and within edges are visible when both endpoints are, so the
members of a team see its member list and others do not. Deny edges are visible
only to owners of the scope they deny, so walls stay confidential. A scope row
is visible according to its own type's placement.

### Policy configuration

Cedar continues to gate operations: `read`, `change`, `schema_apply`, branch
and commit actions, `invoke_query`. Scopes gate data. The graph policy bundle
gains version 2 with a `views` block holding only the trust root and bounds:

```yaml
version: 2
groups:
  staff: [principal:alice, principal:bob, principal:slack-connector]
  connectors: [principal:slack-connector]
  platform-admins: [principal:root]
rules:
  - id: staff-read
    allow: { actors: { group: staff }, actions: [read, invoke_query] }
  - id: connectors-write
    allow: { actors: { group: connectors }, actions: [change] }
views:
  unrestricted: [platform-admins]
  limits:
    max_memberships: 4096
    max_scopes: 100000
```

- `unrestricted` lists cluster groups whose view is the whole graph: the
  break-glass and maintenance path. Every unrestricted read is audited. Only an
  unrestricted actor may remove a placement from the schema.
- `limits` bound one actor's view ([Bounds](#bounds)).

No new Cedar action is needed. Who may change access to a scope is the
`owner` level on that scope; who may move a row between scopes is the `editor`
level on both.

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
| Insert or update of a placed row | The actor needs `editor` on every scope the row lives in, before and after the change. Moving a row between scopes needs `editor` on both. |
| Insert without a value for a per-row `@in` property | The row goes into the writer's personal scope. A writer with no personal scope is refused with `placement_required`. An explicit null is allowed only for unrestricted writers. |
| A placement value naming a scope the writer cannot edit, or no scope at all | Refused with `view_write_denied` in both cases, so the refusal does not reveal whether the scope exists. |
| Insert of a keyed row whose id exists but is not writable | Refused with `key_conflict`, without an entity id or any value the actor did not send. Otherwise it upserts as today. |
| Update or delete statement | Matches only rows in the view. A matched row that is visible but not writable refuses the statement with `view_write_denied`. |
| Delete that would cascade to an edge the actor cannot see | Refused with `delete_blocked`. |
| Edge insert | Both endpoints must be visible; a hidden endpoint fails exactly as a missing one. |
| `load --mode overwrite` | Refused when the actor's view of any type in the batch is restricted. |
| Branch merge | Refused unless every row in the merge diff is writable by the actor. Conflicts are reported without entity ids. |
| Constraint violations | Reported by constraint and property name only. `@unique` omits the other holder's id; `@card` omits the count. |

Access edges and principals have their own rules, for every actor that is not
unrestricted:

- Writing a `@member`, `@grant`, `@deny` or `@within` edge into a scope needs
  `owner` on that scope.
- These edges, and `@principal` properties, are written only on `main`. A branch
  write or a merge that touches them is refused. Without that rule a branch
  forked before a revocation would bring the revoked access back when merged.
- A new scope row must be written together with an `owner` grant into it from a
  subject in the writer's membership closure; otherwise the write is refused
  with `scope_owner_required`.

A connector that mirrors source ACLs either owns the scopes it creates or runs
as an unrestricted principal.

Unrestricted actors keep today's messages and rules.

### Agents and delegated credentials

An agent acting for a user presents a ***delegated credential***: the user's
principal as subject, the agent's principal as actor (the RFC 8693 `act`
claim), an audience bound to this server, and optional ***caveats*** that only
narrow: a list of scopes, a list of types, an action subset and an expiry. The
effective readable set is

```text
R(user) ∩ R(agent) ∩ caveat scopes
```

and the schema view is narrowed by the caveat types. Keeping content away from
agents needs no special tier: put agent principals in a team and `@deny` that
team the scopes in question. The MCP surface accepts only tokens whose audience
is this server and never passes a token upstream.

### Derived content

A summary, chunk or note derived from several sources is written into a scope
that is `@within` each source's scope and granted to `*`, as
`space:acme-digest` above. It is then visible exactly to those who can read
every source, and stays correct as memberships change. OmniGraph does not check
that a writer declared all of its sources; an agent runtime that tracks
information flow (CaMeL, FIDES) owns that.

### History, change feed and export

Authorization always comes from current authority: rules from the applied
policy, access edges from `main`'s current head. Revocation therefore covers
history, and joining a team shows that team's history. Relaxing access exposes
history too; BigQuery documents the same consequence.

In the first releases a restricted actor cannot read a historical snapshot, the
change feed, a commit's changes, an export or a baseline of any placed type its
view restricts. These refuse with `view_restricted`. Unrestricted actors and
share reconcilers are unaffected. A later phase serves view-filtered history
([Rollout](#rollout), Phase 7).

### Surfaces

| Surface | Under a restricted view |
|---|---|
| `POST /query`, `POST /queries/{name}`, MCP `query` | Answered over the view. |
| `explain` | Omits row estimates, statistics, dataset paths, versions and index facts for restricted types. |
| `profile` | Unrestricted actors only. |
| `GET /schema` | The schema view: hidden types and properties are absent. |
| Structured catalog route and schema hash on reads ([Polymorphic types](#polymorphic-types)) | The schema view, and the view's digest in place of the schema hash. |
| `GET /queries`, MCP `queries` | Only queries that typecheck against the schema view. |
| `GET /snapshot` | Omits `entity_count` and dataset paths for restricted types. |
| `GET /commits`, `/commits/{id}`, `/commits/{id}/changes`, `GET /changes`, `POST /changes/baseline`, `POST /export` | `view_restricted` until Phase 7. |
| `GET`/`HEAD /blob` | Only for a visible row; a hidden row answers exactly as a missing one. |
| Write responses | As in [Writing under a view](#writing-under-a-view). |
| Denial messages | Do not echo the actor or branch. |

### Sharing

A share recipient is a principal: a row of a principal type whose actor id the
recipient authenticates as. The share's content is whatever that principal can
read. Three delivery modes use that one definition:

| Mode | Recipient reads | Revocation | Retention and audit |
|---|---|---|---|
| ***live*** | Our server, under the recipient principal's view | Immediate | Ours; we see every read |
| ***hosted copy*** | A separate graph we host, kept current by a reconciler | Immediate; the copy is deleted | Ours; we see every read |
| ***delivered copy*** | A separate graph in storage the recipient controls | Stops future updates only | The recipient's |

Live shares inherit every side channel of live access, so they are for trusted
collaborators. External parties get copies. Copies are declared in cluster
configuration:

```yaml
shares:
  acme-deal-room:
    source: knowledge
    principal: share:acme
    mode: hosted
    target: acme-deal-room       # graph id of the copy
    terms: odrl/acme-agreement.json
```

The copy's schema is the source's schema view for the recipient principal. Ids
are preserved, so rows map back to the source.

#### Answer capabilities

A stored query may declare `run_as: definer`. It then runs with the view of the
principal that defined it, and a caller needs only `invoke_query` on it. This is
how a recipient gets a complete aggregate without the rows behind it. Because
repeated parameterized queries can isolate individuals, a definer query must
declare a domain for every parameter, cannot filter a key property by a
caller-supplied value, and logs every invocation's parameters for its owner.
Minimum group sizes alone do not prevent differencing; AWS Clean Rooms and
Snowflake both say so. Per-recipient privacy budgets are later work.

#### Write-back

A recipient of a hosted copy writes on a branch of the copy. The reconciler
maps that branch into a proposal branch on the source, checks that every changed
row is writable by the recipient principal, and an owner reviews and merges.
Revoking the share deletes its proposal branches.

### Vaults

A ***vault*** is a compartment kept in its own graph or cluster instead of
placed by scopes. Use scopes for the long tail of channels, folders, deals and
matters. Use a separate graph when a defect in view enforcement would be
unacceptable, retention differs, or full-text statistics must be isolated. Use
a separate cluster when the threat includes holders of storage credentials,
when compliance requires key separation, or when a recipient gets storage
access: every graph in one cluster shares one storage root and one set of
credentials today ([Appendix A](#appendix-a-validated-assumptions)).

### Operator tools

```bash
omnigraph policy explain-view --graph knowledge --actor principal:alice
omnigraph policy who-can-see --graph knowledge --type Message --id <id>
omnigraph policy diff-view --graph knowledge --branch proposed-access
```

`explain-view` prints the actor's memberships, readable scopes (bounded),
denials and the edges that produced them. `who-can-see` inverts it for one
row. `diff-view` reports how many scopes change visibility for how many actors
between `main` and a branch carrying proposed access edges, before they merge.

## Design

### Authority

| Fact | Owner | Changes | Versioned by |
|---|---|---|---|
| Which types and edges take part, and how data is placed | Accepted schema contract | Rarely; schema apply | The contract in `__manifest` |
| Who is unrestricted; limits | Applied cluster policy | Reviewed; cluster apply | Policy digest |
| Principals, scopes, memberships, grants, denials, placements | Graph rows on `main` | Daily | `main`'s manifest pins of the access tables |

Cluster policy groups gate operations; scopes gate data. Neither mirrors the
other.

### Capturing a view

A request resolves its actor at the server as today. The engine gains read
doors that take the actor; invariant 10 extends from writes to reads, so
embedded and CLI-direct readers obey the same view when a policy is installed.

A read captures two things under one schema permit, in this order:

1. ***Authorization state***: the applied policy digest and the `__manifest`
   pins on `main` of every ***access table*** (each scope type, each principal
   type, each `@member`, `@grant`, `@deny` and `@within` edge type), each pin
   being the table's identity, published dataset version, staged version and
   transaction id.
2. ***Data snapshot***: the read target, as today.

Snapshot-target reads already load `main`'s live contract under the schema
permit and then discard that `main` snapshot
(`crates/omnigraph/src/db/omnigraph.rs:1009-1024`); this design keeps it. A
branch read probes `main`'s version once more. The capture is one new function,
because the schema gate is not reentrant
(`crates/omnigraph/src/db/write_queue.rs:455-458`).

The ***authorization key*** is `(policy digest, access pins, actor, caveats)`.
It is deliberately not `main`'s head commit: every unrelated write and every
maintenance run advances the head, while the pins move only when an access table
is written.

Because authorization comes from the freshest `main`, it is never older than
the data it filters, so Zanzibar's "new enemy" problems (content added after a
revocation, or a revocation ordered after a move) cannot occur.

Expiry is evaluated against the request's admission time on the server, one
clock for the whole request. A cached view is valid until its earliest
`@expires` boundary.

### Computing the readable set

The engine evaluates the definitions in [Computing a view](#computing-a-view) as
an internal plan on `main`'s snapshot:

1. Resolve the principal by its `@principal` property. If absent, the closure
   holds only `*`.
2. Compute the membership closure over all `@member` edge types with GQ's
   bounded multi-hop traversal (`crates/omnigraph-compiler/src/query/query.pest:124,130`),
   whose visited set handles cycles (`crates/omnigraph/src/engine/graph.rs:1395-1404`).
   Multi-hop requires equal endpoint types today, so the closure runs as a
   principal-to-scope hop followed by a scope-to-scope recursion; with
   polymorphic endpoints it is one traversal.
3. Read `@grant` and `@deny` edges whose source is in the closure, by an
   indexed scan on the edge tables' source column.
4. Subtract denials, then evaluate `@within` from the leaves up.
5. Intersect with the agent's set and the caveats for a delegated credential.

The result is an immutable ***view spec***: the schema view, `R`, `W`, `O`, the
earliest expiry and a digest, cached by authorization key.

### Bounds

The closure is bounded by `max_memberships`, `R` by `max_scopes`, membership
depth by 16. Exceeding a bound refuses the request with
`view_limit_exceeded`; the engine never truncates a view. The view cache is an
LRU bounded in bytes and lives outside `invalidate_read_caches`
(`crates/omnigraph/src/db/omnigraph.rs:2026-2034`), whose flush on every observed
advance of `main` would otherwise discard views keyed by immutable state. Reads
by external principals get per-actor read admission, extending the existing
per-actor write caps (`crates/omnigraph-server/src/workload.rs:15-19`).

### The visibility door in the plan

The view enters the plan in three places.

**Schema view.** The query typechecks against a catalog without the types,
edge types and properties whose fixed scopes are not in `R`. The catalog is
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
- expands into a restricted type without a destination mask.

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
optimization behind its own surface guard ([Rollout](#rollout), Phase 6).

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

- ***Public statistics***: corpus statistics over rows placed in scopes granted
  to `*`, built by explicit index reconciliation and stored as derived state
  keyed by index identity and the everyone scope's grants.
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
cheap. Unrestricted actors keep index-wide statistics.

### Traversal

Traversal reads edge tables by key and never reads intermediate node tables
(`crates/omnigraph/src/engine/graph.rs:1132-1472`). Edge reads carry no filter
(`crates/omnigraph/src/engine/graph.rs:1647-1662`). The destination of each hop
is masked:

- **Indexed mode**: after each hop, the frontier's ids of a restricted placed
  type are intersected with the view by one indexed read of that node table
  (`__id IN frontier AND placement IN R`). The final destination scan already
  carries the view conjunct through `hydrate_nodes`, which refuses unsupported
  predicates (`crates/omnigraph/src/engine/operators/scan/input.rs:270-284`).
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

Today an upsert reports `affected_nodes: 1` whether it inserted or updated, and
delete counts include rows matched by id regardless of visibility
(`crates/omnigraph/src/exec/mutation.rs:1645-1661, 1717-1741`). Under a view the
statement's match runs over the view, so counts are view counts.

Errors carry their foreign facts (another row's id, a count, an ETag, another
type's version) as structured fields, and the server strips them for a
restricted actor. Today they are formatted into messages
(`crates/omnigraph/src/validate.rs:1259, 1295-1296`) and returned verbatim
(`crates/omnigraph-server/src/lib.rs:1357`).

The same validation enforces the access-edge rules: `owner` on the target
scope, `main` only, no `@within` cycle, an owner grant with every new scope, and
global uniqueness of scope keys and actor ids. Cross-table uniqueness rests on
read-set validation and the exact graph-head precondition, not a fence; a
concurrent duplicate fails publication.

### History

A read of a snapshot target captures authorization from `main` and data from the
snapshot, as above. Historical queries already use the live contract with tables
rebound by stable identity (`crates/omnigraph-catalog/src/lib.rs:608-656`), so
annotations bound to stable identity apply to history without new mechanism.
The first releases refuse historical reads of restricted placed types. A later
phase admits a historical row only when its placement is readable both at the
snapshot and, if the row still exists, at `main`'s head, so a row moved from a
public to a private space stays hidden in old snapshots.

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
`2026-10-07-polymorphic-types.md`) makes interfaces and inline unions bindable,
adds polymorphic edge endpoints with `__src_type` and `__dst_type` tag columns,
and leaves per-type policy to the `predicate_for` work this RFC defines. The two
designs compose as follows.

**Member sets come from the schema view.** An abstract binding resolves its
member set against the actor's view catalog, so a member type placed in a scope
the actor cannot read is absent, exactly as in `materialize(V(a))`. Visibility
is decided per concrete type; `implements` neither grants nor removes it.
`$x is T`, `@type` and `@type_id` comparisons naming a hidden type fail with the
unknown-name error. Result schemas still never depend on the member set.

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

**Removing a placement** from any concrete type or property, by any route, is
refused unless the applying actor is unrestricted. The routes are deleting the
annotation, removing an interface property, removing `implements`, and narrowing
an endpoint so a member stops inheriting a placed property.

**Access edges.** With polymorphic endpoints one edge type covers every kind of
subject and scope:

```pg
edge MemberOf: Person | Team -> Team @member { expires: DateTime? @expires }
edge Access: Person | Team -> Team | Space | Class @grant {
  level: enum(viewer, editor, owner)
  expires: DateTime? @expires
  @key(@src, @dst)
}
```

The membership closure is then one bounded multi-hop traversal, which that
RFC's set-based multi-hop rule admits. The global scope-key namespace means a
placement value never needs a type tag.

**Union scans and polymorphic expansion.** Every arm of a union scan whose member
is a restricted placed type carries its own view conjunct, placed during that
RFC's per-arm resolution. The coverage validator inspects arms. A `count` over
an abstract binding may sum per-member metadata counts only for unrestricted
members. A polymorphic expansion groups each hop's frontier by tag, as that
RFC's interner already keys by `(type, id)`, and masks each restricted member.

**Graph index.** That RFC's phase 3 moves the graph index to one global ordinal
space. Per-view masks ([Rollout](#rollout), Phase 6) target that space: one
bitmap per view and index instance.

**Search.** That RFC refuses `bm25` on a multi-member binding because each
table's statistics differ. Uniform statistics are identical across tables, so
for restricted actors that reason disappears. This RFC does not admit
multi-member `bm25`; a later change may.

**Catalog route, schema hash, descriptors and feeds.** The structured catalog
route that RFC adds serves the schema view. Reads by a restricted actor carry
the digest of the schema view where they would carry `schema_ir_hash`, so
clients cache type names per view and a change to a hidden type is not
observable. Operation descriptors list visible members. In Phase 7, interface
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
change feed as the recipient principal, applies inserts and updates with
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

A fixed relation vocabulary (member, three grant levels, deny, within) whose
facts are graph data, compiled by OmniGraph into its typed IR. Cedar remains the
gate on operations.

Cedar's partial evaluation and type-aware partial evaluation are experimental in
4.11.2, the version in the lockfile, and remained so in 4.13.0. Their residuals
cannot express graph traversal: `resource in Space::"x"` means a transitive
descendant, which would have to be precomputed anyway. OPA's partial evaluation
has the same limit, and its translation to SQL strings produced an injection
fixed in v1.19.0 (July 2026). Invariant 9 already forbids string-built
semantics.

The vocabulary is fixed where Zanzibar, SpiceDB and OpenFGA let each type define
its own relations. A fixed vocabulary is analyzable without a solver, maps
directly to a precomputed readable set, and covers mirrored source ACLs. The
level enum can grow (a `commenter` level, say) without changing the set
operations. Parent inheritance (Zanzibar's `tuple_to_userset`) is expressed
through membership between audiences and is the first extension users are
likely to ask for ([Unresolved questions](#unresolved-questions)).

### D2. Where access facts live

In the graph, on `main`. Sensitivity classes are scope rows like any other,
not a list in policy. One mechanism governs rows, columns and types, and one
kind of group governs data. The trade-off is that the owner of a fixed scope
can expose a column by granting it, where a policy list would have needed a
reviewed change. Every grant is an audited commit, owners are already trusted
with their scope's rows, and removing a placement from the schema still needs
an unrestricted actor.

### D3. Placement is a property

Each placement is a single-valued `String` property, filtered by an indexed
`IN`. An edge would make every read a join through the edge table, and keeping
an edge as well as the property would be a second source of truth. The cost is
that "messages in spaces Alice owns" is a join on property equality rather than
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

Scopes for the long tail; separate graphs where a defect would be unacceptable
or statistics must be isolated; separate clusters where storage holders, key
separation or delivered copies are involved. Per-graph storage roots,
credentials and keys inside one cluster are a gap this RFC names and does not
fill.

## Invariants

1. **Respect the substrate.** Views use Lance's scalar indexes, prefilter, KNN
   and FTS execution and the public base-scorer hook. No storage primitive is
   added. Index state stays derived.
2. **One publication door.** Principals, scopes and access edges are graph rows
   published through `__manifest`. Share copies publish through load and
   mutate. No side table holds access facts.
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
7. **Derived acceleration.** View specs, public statistics, graph-index masks
   and row masks are derived state keyed by immutable versions. Missing or stale
   derived state falls back to an exact path or refuses; it never widens a view.
8. **Loud integrity.** Every refusal is typed. A view is never truncated
   silently. A fixed placement naming a missing scope is refused, not hidden.
9. **Typed semantics.** The view is a typed IR leaf resolved from the engine
   context. Caveats compile to the same structure. No filter is built as a
   string.
10. **Boundary and engine.** Actors resolve at the server. The engine enforces
    views on reads as well as writes when a policy is installed.
11. **Bounded.** Memberships, scopes, depth, caches and external reads are
    bounded; exceeding a bound refuses.
12. **One source of truth.** Access facts exist only in the graph; the trust
    root only in policy. Connectors mirror source ACLs into the graph, which is
    then the authority.
13. **Evidence.** See [Evidence and tests](#evidence-and-tests).

Deny-list: the share reconciler is an idempotent reconciler from accepted
state, not a job queue. Public statistics are built by explicit reconciliation,
not inline on content writes. No process-local lock fences anything new.

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
  placement requires an unrestricted actor.
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
- **Cedar or OPA residuals compiled to filters.** Rejected under D1.
- **A per-type relation language** (Zanzibar namespaces, SpiceDB, OpenFGA).
  More expressive; deferred because the fixed vocabulary covers mirrored ACLs
  and is analyzable without a solver.
- **One access edge with a five-valued relation.** Mixes three semantics
  (chaining membership, one-hop grants, subtractive denial) in one enum, and a
  key on the endpoint pair would forbid a scope from both granting and denying.
- **Sensitivity classes in policy.** A second group system beside graph
  membership, and two mechanisms for one idea.
- **Placement as an edge.** A join on every read and, alongside a property, a
  second source of truth (D3).
- **Typed scope references through polymorphic endpoints.** Needs type tags on
  every placement; a global key namespace needs none.
- **An external Zanzibar service** (SpiceDB with Materialize, OpenFGA). A second
  store with its own consistency tokens and a dual-write problem; list APIs are
  bounded (OpenFGA ListObjects defaults to 1,000 results in 3 s).
- **Per-row ACL lists** (Accumulo, LabelList). Every row carries principals;
  Lance's LabelList index looks labels up one at a time
  (`lance-index-11.0.0/src/scalar/label_list.rs:140-144`), and access changes
  rewrite rows. A scope makes an access change one edge.
- **One dataset per scope.** Hard isolation, but table count would follow scope
  count into the hundreds of thousands.
- **Encryption per scope.** Breaks indexes and search; deferred to vault
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
| Readable-set semantics | In-source engine tests over the worked example, plus generated access graphs checked against a direct set-semantics evaluator |
| Engine read and write enforcement | `crates/omnigraph/tests/policy_engine_chassis.rs` |
| Non-interference across operators | `crates/omnigraph/tests/proptest_equivalence.rs`: for generated graphs and access edges, a restricted actor's result equals an unrestricted result on the materialized view |
| Restricted vector and full-text search | `crates/omnigraph/tests/search.rs` |
| Traversal masks | `crates/omnigraph/tests/integration/traversal_indexed.rs` |
| Write rules | `crates/omnigraph/tests/writes.rs` |
| History refusal and dual placement | `crates/omnigraph/tests/integration/point_in_time.rs` |
| View-filtered change feed | `crates/omnigraph/tests/integration/changes.rs` |
| Replay refusal on view change | `crates/omnigraph/tests/integration/engine_v2_plan_replay.rs` |
| Lance facts in the table above | `crates/omnigraph/tests/lance_surface_guards.rs` |
| HTTP surfaces and redaction | `crates/omnigraph-server/tests` `auth_policy` suite; OpenAPI |
| Embedded and remote parity | `crates/omnigraph-cli/tests` parity matrix |
| Access writes concurrent with reads | `omnigraph-dst` scenario |

GQT gains an actor section so view semantics become logic tests; until then the
Rust owners above carry them.

Two prior-art lessons shape the generated cases. OpenFGA shipped authorization
bypasses in models combining exclusion and intersection (CVE-2024-31452) and,
with a public wildcard, in ListUsers (CVE-2026-61709); the generators must favor
graphs combining `*`, `@within` and `@deny`. SpiceDB evaluates expiry in its
datastore to avoid clock skew; tests must cross an `@expires` boundary inside a
cached view's lifetime.

### Acceptance thresholds

- An unrestricted actor's queries make no more object-store requests than today
  (the warm read cost instrument).
- A restricted read with a warm view cache adds no object-store request on
  `main` and one version probe on another branch.
- A restricted scan of a placed type with a covered BTREE adds at most the index
  reads of one `IsIn` evaluation per placement property.
- Restricted vector search: recall@10 ≥ 0.95 against view ground truth at
  visible fractions 100%, 10%, 1%, 0.1% and 0.01%, and with views negatively
  correlated with the query; latency within 2× unrestricted at ≥ 10%, and within
  the exact-scan cost at ≤ 1%.
- Restricted full-text search: nDCG@10 with the chosen statistics source within
  10% of index-wide statistics on the fixture.
- An access change is one edge write and governs the next request.

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
| 1. Scopes and schema placement | The model, readable sets, fixed `@in` on types and columns, policy version 2. | Per-row placement. |
| 2. Row reads | Per-row `@in`, the visibility door, masked traversal, restricted search. | Restricted actors cannot write placed types. |
| 3. Row writes | Write checks and access-edge rules. | — |
| 4. Agents | Delegated credentials and caveats. | — |
| 5. Search quality | Public statistics. | — |
| 6. Performance | Graph-index masks; row-mask cache. | — |
| 7. History and feeds | Dual-placement history; view-filtered feeds, commits and export. | — |
| 8. Sharing | Recipient principals, copies, answer capabilities, write-back. | Privacy budgets. |
| 9. Vault infrastructure | Separate RFC. | — |

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

### Phase 1: scopes and schema placement

Ships type- and column-level confidentiality on the full access model.

- Compiler: validate the seven annotations; store them in typed IR bound to
  stable ids; derive `actor-views`; add schema-apply steps; check fixed `@in`
  keys against scopes on `main`.
- Engine: authorization capture; the readable-set plan (closure, grants,
  denials, `@within`, expiry, `*`); the byte-bounded view cache; the schema view
  and memoized view catalogs; global uniqueness of scope keys and actor ids;
  refusal to delete a scope a fixed `@in` names.
- Policy: version 2 `views.unrestricted` and `views.limits`, validated at
  cluster apply.
- Until Phase 2: `@member`, `@grant`, `@deny` and `@within` edges are readable
  only by unrestricted actors; scope rows follow their type's placement, which
  can only be a fixed `@in("key")`; schema apply refuses a per-row `@in`.
- Server: schema view on `/schema`; filtered `/queries`; explain redaction; the
  `view` response object; `view_restricted` on history, feeds and export for any
  actor whose view hides something; the operator tools.
- GQT: an actor section in the case format.

Exit: the non-interference property holds for schema views; a hidden name and
an undeclared name produce identical errors; the readable-set evaluator agrees
with the set-semantics oracle over generated access graphs.

### Phase 2: row reads

Restricted actors cannot yet write placed types; they are refused with
`view_write_denied`.

- Per-row `@in` with a BTREE on each placement property.
- The `ViewSet` IR leaf, view conjuncts in scan specs, the coverage validator,
  the view digest in plan assumptions.
- Per-hop frontier masks in indexed traversal; refusal of CSR, degree shortcuts
  and CSR-derived search gates for restricted types.
- Vector planning from the visible count; full-text search for restricted
  actors with uniform statistics; Blob reads checked against row visibility.
- Visibility of access data: scope rows by their own placement, access edges by
  their endpoints, deny edges to owners only.
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
- Personal-scope default, `placement_required`, `view_write_denied`,
  `key_conflict` without ids, `delete_blocked`, overwrite refusal, edge
  endpoint visibility, view-scoped matching and counts.
- Access-edge rules: `owner` on the target, `main` only, no `@within` cycle,
  `scope_owner_required`.
- Server stripping of foreign facts for restricted actors.

Exit: `writes.rs` and `policy_engine_chassis.rs` cover every row of the write
table; each refusal leaves the manifest head, table pins and lineage unchanged.

### Phase 4: agents

- Delegated credentials with subject, actor and audience; caveats compiled into
  the view spec; acceptance of the RFC 8693 `act` claim only for credentials of
  this form.
- MCP audience check; no token passthrough.

Exit: the server identity suites cover delegated credentials and caveats.

### Phase 5: search quality

- Public statistics built by explicit index reconciliation, with uniform
  statistics as the fallback; a recall sampling instrument.

Exit: the ranking threshold holds on the fixture.

### Phase 6: performance

- Per-view masks over a graph-index instance, keyed by the instance and the
  node-table versions; CSR re-enabled for restricted types. With the polymorphic
  types RFC's global ordinal index, one bitmap per view covers every node type.
- An optional per-view row-mask cache, after a guard covers Lance's
  `CountPushdown` with a row mask.

Exit: traversal and search cost instruments show no regression against Phase 2
at equal results.

### Phase 7: history and feeds

- Historical reads bind properties by stable identity (prerequisite).
- Dual-placement historical reads; view-filtered change feed (digest in cursors,
  visibility transitions as inserts and deletes, projected equality);
  view-filtered commits, commit changes, export and baseline.

Exit: `point_in_time.rs` and `changes.rs` cover moved rows, revoked members and
hidden-column changes; cursors refuse a changed view.

### Phase 8: sharing

- `shares` in cluster configuration; live shares; the share reconciler for
  hosted and delivered copies, with the cursor row, baselines and terms checks.
- Definer-rights stored queries with parameter domains and invocation logs.
- Write-back proposals; per-actor read admission.

Exit: a hosted copy equals the materialized view of its recipient principal
after every reconciled commit; revocation deletes the copy and its proposals.

### Phase 9: vault infrastructure

A separate RFC: per-graph storage roots, credentials and keys in one cluster;
non-discoverable graphs; a cache budget shared across graphs; read-many,
write-one queries across graphs.

## Unresolved questions

1. Which full-text statistics restricted actors get: public or uniform
   (`blocked_on`).
2. Whether parent inheritance needs a direct form beyond membership between
   audiences, and in which phase.
3. Whether a GQ form that traverses a key-valued property should accompany
   Phase 2, so placement can be traversed like an edge.
4. Whether denied principals should see the deny edges that bar them, as some
   ethical-wall practice expects, or only owners.
5. In Phase 7, whether commits with no visible change are listed to restricted
   actors.
6. Whether design partners accept key-existence disclosure to writers, or need
   uniqueness scoped to a placement.
7. The matching amendment to the polymorphic types RFC's Policy section, which
   still refuses hidden members.

## Decision log

- 2026-10-11: Drafted. The design was checked against the code at `6e31d0ab` and
  Lance 11.0.0; [Appendix A](#appendix-a-validated-assumptions) records every
  assumption with its verdict.
- 2026-10-11: Aligned with the polymorphic types draft
  (ModernRelay/omnigraph#884): member sets from the schema view, placements on
  interface properties fixed across members, removal of a placement restricted,
  polymorphic access edges, per-arm view conjuncts, global-ordinal masks.
- 2026-10-11: Rewrote the declaration layer to the scope model. Thirteen
  annotations, four policy keys, four new actions, two visibility tiers,
  integrity labels and compound securables collapse into seven annotations
  (`@principal`, `@scope`, `@member`, `@grant`, `@deny`, `@within`, `@in`, with
  `@expires`), a policy block holding only `unrestricted` and `limits`, and no new
  action. Sensitivity classes became scope rows; the `*` principal became the
  everyone scope; scope keys share one namespace; fixed placements became
  checked references; access data became placed. The `ground` tier became a
  `@deny`; the `discover` tier and integrity labels were deferred; enforced
  provenance moved to agent runtimes; the opt-in completeness refusal was dropped
  in favor of `view.restricted`.

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
| Scopes as ordinary objects; a group is a scope with members | Zanzibar objects and the `member` relation with nested `group#member` usersets | Same |
| Membership chains, grants are one hop | Zanzibar's Leopard index (group-to-group closure, then member-to-group); Active Directory nested groups and ACL entries | Same shape |
| `@member`, `@grant`, `@deny`, `@within` | Zanzibar userset rewrites (union, intersection, exclusion); SpiceDB `+ & -`; OpenFGA `or`, `and`, `but not` | Same set operations, fixed vocabulary |
| The everyone scope | OpenFGA and SpiceDB public wildcard `user:*` | Same |
| Deny applied last | Windows deny entries, Purview Information Barriers, Ranger tag denies, Databricks DENY | Same; avoids GraphDB first-match ordering, Neo4j denies that fail open on null, and Drive limited access overridden by a group grant |
| `@expires` | SpiceDB expiring relationships, OpenFGA conditions | Same |
| Per-row placement with `IN` | Snowflake row access policies with mapping tables, Postgres row security, Elasticsearch document-level security, Glean and Azure AI Search security trimming | Same |
| Several placements, all required | Oracle Label Security compartments, MarkLogic compartment security | Same |
| Fixed placement of a column or type | BigQuery policy tags; Unity Catalog tags driving row filters and column masks | Close |
| `@within` for derived content | FIDES and CaMeL reader-set intersection | Same; Purview takes the highest label instead |
| Delegation as intersection | Microsoft delegated permissions; Macaroon and Biscuit attenuation | Same |
| Access facts in the data graph | Fong's relationship-based access control; Snowflake mapping tables | Variant: same versioned graph, consistency by snapshot instead of Zanzibar zookies |
| Precomputed readable set | Leopard; AuthZed Materialize | Recomputed per access-table version |

Sources consulted:

- Zanzibar (USENIX ATC 2019), the new enemy problem and Leopard.
  https://www.usenix.org/system/files/atc19-pang.pdf
- Cedar type-aware partial evaluation, RFC 0095.
  https://github.com/cedar-policy/rfcs/blob/main/text/0095-type-aware-partial-evaluation.md
- OPA partial evaluation fragment and the v1.19.0 SQL injection fix.
  https://www.openpolicyagent.org/docs/filtering/fragment
- SpiceDB consistency and Materialize; OpenFGA ListObjects limits and public
  access. https://authzed.com/docs/spicedb/concepts/consistency ,
  https://openfga.dev/docs/modeling/public-access
- OpenFGA advisories CVE-2024-31452 and CVE-2026-61709.
  https://osv.dev/vulnerability/CVE-2026-61709
- OpenID AuthZEN Authorization API 1.0.
  https://openid.net/specs/authorization-api-1_0.html
- PostgreSQL row security and constraint covert channels.
  https://www.postgresql.org/docs/current/ddl-rowsecurity.html
- Oracle Label Security, Db2 LBAC, MarkLogic compartments, Accumulo visibility
  labels. https://accumulo.apache.org/docs/2.x/security/authorizations
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
- Apache Atlas classification propagation; Ranger tag policies; Unity Catalog
  ABAC; BigQuery column-level security.
  https://cloud.google.com/bigquery/docs/column-level-security-intro
- FIDES (arXiv 2505.23643), CaMeL (arXiv 2503.18813), design patterns for LLM
  agents (arXiv 2506.08837).
- Snowflake row access policies and secure data sharing; BigQuery time travel
  with row-level security; Databricks ABAC with time travel and Delta Sharing.
- Slack Connect, Teams shared channels, AWS Clean Rooms analysis rules and
  differential privacy, Snowflake aggregation policies, Solid WAC and ACP, ODRL
  2.2, Eclipse Dataspace Components policy monitor, DoltHub forks, lakeFS hooks.
- RFC 8693 token exchange; MCP authorization 2026-07-28; Biscuit; Macaroons;
  UCAN.
