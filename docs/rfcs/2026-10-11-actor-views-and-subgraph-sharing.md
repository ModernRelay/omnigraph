---
rfc: "2026-10-11-actor-views-and-subgraph-sharing"
title: "Actor views and subgraph sharing"
track: maintainer
status: draft
implementation: not-started
authors:
  - ragnorc
created: 2026-10-11
updated: 2026-10-20
discussion: null
supersedes: []
superseded_by: []
blocked_on:
  - "Measure exact restricted search at the target sizes (view-statistics bm25 over the visible text, exact nearest over the visible vectors; local disk and S3) and set the admission budgets in Bounds."
  - "Measure readable-set construction at the target scale (10^5 principals, 10^6 securables, membership depth 16) on main, warm and cold, and confirm the limits in Bounds."
---

# RFC: Actor views and subgraph sharing

> A term set in ***bold italics*** is defined at that spot; it is plain text
> afterwards. OmniGraph code is cited by file and symbol at commit `9575b044`.
> Lance is cited as `lance-11.0.0/<path>:<line>` and its sibling crates, the
> versions in `Cargo.lock`.

## Summary

OmniGraph authorizes today at graph × branch class × action. An actor can read
every row of a branch or none of it. This RFC adds an ***actor view***: the part
of the graph a given actor may see and write.

Views are built from two roles and a fixed set of facts. A ***group*** is a set
of actors, defined by membership. A ***securable*** is a node that data is
placed in, such as a channel, a folder, a deal room or a sensitivity class; who
can read it is defined by edges into it. Grants and inheritance add readers,
`@requires` intersects audiences, and denial removes readers. Annotations in the
schema say which types and edges play which part and place each row, column or
type in securables. An actor sees a piece of data when it can read every
securable the data is placed in. Memberships, grants and placements are graph
data; users write facts, never rules.

The observable contract is ***non-interference***: two graphs that give an
actor the same view give that actor the same answer to every query. Counts,
aggregates, traversals, search scores and ranks, and errors therefore depend
only on what the actor may see. The contract is enforced at one place per
mechanism, the ***visibility door***: plan acceptance for reads, attempt
validation and the publication compare-and-swap for writes. A differential test
over pairs of graphs that differ only in hidden data proves it for every
operator.

Restricted search is exact and scored from the view: `bm25` uses statistics
over the actor's visible rows, and `nearest` scans the visible vectors. Actor
ids, securable keys and the internal group are protected ***identity
bindings*** with one lifecycle.

External sharing needs no further access concept. A ***recipient*** is a
principal that stands for an outside party. A share is the recipient's view,
served live or materialized into a separate graph kept current from the
source's change feed.

This RFC asks acceptance for the contract, the model and Phases 0–3: reads and
writes under a view. Later phases (delegated agents, incremental maintenance,
packed placement references, data-derived and edge placement, search
acceleration, view-filtered history and feeds, sharing) are designed here to show
that the model extends; each is accepted by amendment when its evidence exists.

What does not change: Cedar remains the gate on operations; a graph that
declares no securables behaves exactly as today; anyone holding storage
credentials still sees everything, as today.

## Motivation

### The current boundary

The policy model has graph-scoped actions with an optional branch class.
`ResourceScope` in `crates/omnigraph-policy/src/lib.rs` is "Branch-grained on
purpose", and per-type and per-row scope were reserved for a later
`predicate_for` hook in the query layer. The user guide states the limit: "Per-entity and
per-property authorization is not currently supported"
(`docs/user/operations/policy.md`). Rules are allow-only, grouped by static actor
lists in YAML, and compiled to Cedar text (`compile_policy_source`). Reads are
authorized only in HTTP handlers; the engine's read doors in
`crates/omnigraph/src/exec/query_doors.rs` take no actor.

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

Let `V_G(a)` be the view actor `a` has in graph `G`: the visible types,
properties, rows and edges with their values. For any read `q` and any two
graphs `G` and `G′`:

```text
V_G(a) = V_G′(a)   ⟹   answer(q, G, a) = answer(q, G′, a)
```

The equality covers rows, order where the query orders, counts, aggregates,
search scores and ranks, errors, refusals and the result schema. It holds for a
read at the request's captured authorization state and data snapshot
([Capturing a view](#capturing-a-view)). Timing and resource use are excluded
([What is not promised](#what-is-not-promised)).

A stronger form is the test oracle:

```text
answer(q, G, a) = answer(q, materialize(V(a)), bypass)
```

`materialize(V(a))` contains exactly what `V(a)` admits, and the bypass actor
evaluates it with the same evaluators a restricted read uses. Both forms hold
for every operator in the first scope because restricted search is exact and
scored only from the view ([D10](#d10-restricted-search-is-exact-and-scored-from-the-view)).
Approximate operators satisfy neither form, so they are not offered to
restricted actors on placed types.

Writes obey the dual rule: a write by `a` succeeds only if every row it creates,
changes or removes is writable in `V(a)` at the authorization state its attempt
captured, and its outcome reveals nothing outside `V(a)` beyond the disclosures
listed under [What is not promised](#what-is-not-promised). On `main` that state
is also the state at publication. On another branch it can be one revocation
older, the window stated there.

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

A name the view hides (a type, an edge type, a property, or a namespace that
holds only hidden types) does not exist for the actor. A query naming it fails
with the same error, byte for byte, as a query naming something never declared.

### What is not promised

- **Storage holders and direct access.** Lance files contain every row.
  Anyone who can read the graph's objects sees everything, and direct CLI and
  embedded access are storage-holder access
  ([Direct and served access](#direct-and-served-access)). A recipient who needs
  storage-level access gets a materialized share with its own storage.
- **Timing.** Execution time can depend on hidden rows. Timing channels are out
  of scope for live access; they are one reason external parties get
  materialized shares.
- **Write disclosures.** A writer of a placed type can learn, from whether its
  write succeeds, exactly these facts and nothing more
  ([Constraint disclosures](#constraint-disclosures)): whether a key exists in
  that type; whether a value tuple of a `@unique` constraint on that type exists
  in it; whether adding or removing an edge would take its source past a `@card`
  maximum or below a `@card` minimum; when it deletes a node, whether the node
  has an edge the writer cannot see; and, when it merges, whether a row outside
  its view changed on either side since the merge base.
- **Branch writes during a revocation.** A write on a branch other than `main`
  is authorized against the authorization state its attempt captured. A
  revocation published on `main` while that attempt runs can miss that one
  attempt ([Capturing a view](#capturing-a-view)). The window is one attempt,
  bounded by the write deadline; its rows land only on that branch; every later
  read of the branch applies the current state, and merging the rows into
  `main` checks them again for the merging actor. Closing the window would need
  a compare-and-swap across two branch chains, which Lance does not provide, or
  a process-local lock, which fences nothing across processes.
- **Retraction on existing branches.** Moving a row to a more restricted
  securable on `main` does not change the row on branches forked before the
  move; readers of those branches keep the old placement until
  [Phase 9](#rollout). Restricted actors cannot create a branch at a past commit
  or restore an old state of a placed type, so only branches that already
  existed are affected.

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
| `@securable` | node type | Rows hold data. Securable keys are unique across securable types. | Data side |
| `@in(p)`, `@in("key")` | node type or property | Places each row in the securable named by its property `p`, or a whole type or one column in a fixed securable. | Placement |
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
  subject: String @key                  // identity-provider subject, never reassigned
  email: String?
  actor: String? @principal @unique     // "principal:<sub>" or a static-token actor id
}

node Team @group @securable @in(slug) { slug: String @key }          // "okta:00g1…"
node Channel @group @securable @in(slug) { slug: String @key  name: String? }  // "slack:C123"
node Folder @securable @in(slug) { slug: String @key }                // "drive:0B7x…"
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

`Team` here is a user schema type, unrelated to the managed control plane's
organization members. With polymorphic endpoints
([Polymorphic types](#polymorphic-types)) the access edges collapse into one edge
type per relation.

### Rules that complete the model

- **Unique, permanent securable keys.** Securable keys are unique across every
  `@securable` type, checked when a securable row is written. A placement value
  therefore needs no type. A key names one securable for the graph's lifetime
  ([Identity bindings](#identity-bindings)), so connectors use prefixed,
  immutable source ids such as `slack:C123`, a Drive file id or an
  identity-provider subject, never a name or an email address that can be
  reassigned.
- **Unique actor ids** across every `@principal` property. A graph may have
  several principal types: people, agents, recipients.
- **The internal group.** The ***internal group*** is the `@group` row keyed
  `internal`. Its members are implicit: every actor in the cluster policy groups
  listed under `views.internal`. A grant from it reaches everyone inside the
  company and no recipient. It is structural:
  - its key is reserved across every `@group` type, and at most one row holds
    it;
  - write validation refuses an explicit `@member` edge into it and a `@deny`
    edge onto it;
  - only bypass actors create it, on `main`, and it is never deleted.
- **Placement.** `@in(p)` on a type places each row in the securable whose key
  is the row's `p`. `@in("key")` on a type places the whole type; on a property,
  that column. A type may carry several `@in`; data is visible only when every
  one of its securables is readable. A null `p` places the row in no securable,
  so only bypass actors see it.
- **Fixed placements are checked references.** Schema apply refuses an
  `@in("key")` whose key is not a securable on `main`. A misspelling fails
  loudly instead of hiding a column from everyone.
- **Placed columns are nullable.** Schema apply refuses a fixed `@in` on a
  non-null property. A writer who cannot see the column leaves it null on
  insert and unchanged on update, and receives no error that names it.
- **Securables are placed like any row.** `node Channel @group @securable
  @in(slug)` places each channel in itself, so only those who can read it know
  it exists. `Person` is a securable but unplaced, so people stay visible while
  each person's personal securable holds their private data.
- **Unplaced types**, the engine catalogue's built-in types among them, stay
  visible to every actor with `read`, as today.

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
a BTREE index. A schema lint warns when a securable type, or a type with a
per-row `@in`, is keyed on or declares `@unique` over a property that looks
human-meaningful, such as an email or a name: such a key can be reassigned, and
writers can probe it ([Constraint disclosures](#constraint-disclosures)).

### Identity bindings

Three kinds of value decide who a request is and which data belongs to which
audience: the actor id on a `@principal` property, the key of a securable, and
the internal group row. They are ***identity bindings***, and they change only
through one protected lifecycle, enforced in the shared validation of mutation,
load and merge ([Writes](#writes)):

| Operation | Rule in the first scope |
|---|---|
| Assign, change or clear a `@principal` value | Bypass actors only, on `main`, typically the connector that mirrors the identity provider. |
| Create a securable | Bypass actors only, on `main`. |
| Change a securable's key | Refused for everyone; a key is identity, not data. |
| Delete a securable | Refused for everyone. A securable is retired by deleting its access edges and moving or deleting its rows; the row stays, so its key is never reused. |
| Create or re-key the internal group row | Bypass actors only, on `main`; it is never deleted. |
| Write any other property of a securable or group row | On `main` only, under the ordinary write rules. |

Refusals carry `identity_binding_denied`. With these rules a restricted writer
cannot take over another person's identity by moving an actor id, and cannot
squat a securable key before a connector mirrors it.

No key that ever named a securable names another. Rows placed under a key
therefore never pass to a different securable, on `main`, on a branch forked at
any time, or in a retained snapshot. A check at delete time could not promise
that: placement values also live on branches, whose chains `main`'s
compare-and-swap does not fence, and in retained history. A securable row holds
an identity, not personal data: erasing a person clears their properties and
removes their personal data elsewhere, and leaves the key-only row.

Deletion returns with packed placement references ([Phase 6](#rollout)): a
placement then names a securable by a node id that is never reused, so a new
securable cannot be named by any existing placement, and restricted actors may
create securables.

### Computing access

For actor `a` with principal `p`, evaluated at the request's admission time `t`.
An edge is ***live*** when its `@starts` is null or at most `t` and its
`@expires` is null or after `t`.

```text
1. Membership:    C0 = {p}, plus internal if a is in views.internal,
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
internal ─viewer→ channel:general              (alice, bob, carol are staff)
team:hr ─viewer→ pii, compensation
digest:acme ─inherits→ channel:deal-acme
            ─requires→ channel:deal-acme, channel:general
client:globex ─viewer→ folder:globex-shared    (a recipient, not staff)
```

| | alice | bob | carol | client:globex |
|---|---|---|---|---|
| Memberships | team:eng, internal | team:legal, team:acme-wall, internal | internal | — |
| Granted | channel:general, channel:eng (editor) | channel:general, channel:deal-acme | channel:general, channel:deal-acme (editor) | folder:globex-shared |
| Denied | — | channel:deal-acme | — | — |
| `digest:acme` | ✗ cannot read channel:deal-acme | ✗ channel:deal-acme denied | ✓ | ✗ |

`digest:acme` holds summaries drawn from both channels: it inherits its readers
from the deal room and requires both sources, so exactly those who can read both
see it. Bob is granted the deal room through legal, but the wall's deny wins.
The client never sees `channel:general`, because it is not in the internal
group and no edge can put it there.

### Placement in practice

- A `Message` row is visible to `a` only if its `channel` is in `R(a)`. For
  alice above, a `count` over messages counts rows in `channel:general` and
  `channel:eng`, and the response carries `"view": { "restricted": true }`.
- `author_email` exists for actors who can read `pii`; for everyone else it is
  absent, and naming it is an unknown-name error.
- `Salary` exists only for actors who can read `compensation`.

Fixed securables are created and granted as data by a bypass actor:

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
  internal: [staff]
  bypass: [platform-admins, connectors]
  limits:
    max_memberships: 4096
    max_securables: 100000
    max_exact_search_rows: 200000
```

- `internal` lists the cluster groups whose actors are implicit members of the
  internal group. Membership of the managed control plane's organization grants
  nothing here: invited outside collaborators are organization members too. The
  control plane may maintain the cluster group this key lists.
- `bypass` lists cluster groups whose view is the whole graph: the break-glass,
  connector and maintenance path, named after Postgres's `BYPASSRLS`. Served
  bypass reads are recorded through the existing policy decision log. Only a
  bypass actor manages identity bindings, applies a schema or runs maintenance
  on an `actor-views` graph.
- `limits` bound one actor's view and its exact searches ([Bounds](#bounds)).

No new Cedar action is needed. Who may change access to a securable is the
`owner` role on it; who may move a row between securables is the `editor` role
on both.

Cluster apply validates `views` where it already validates stored queries
against the catalog. Binding a version-1 bundle to a graph with `actor-views`
declarations fails the apply, and a `views` block on a bundle bound to the
engine catalogue target `og_catalog` is refused. Branch names, where version 2
mentions any, follow the canonical branch-name rule.

### Direct and served access

How the actor's view applies follows the access path, in the terms of the
graph access RFC
([ModernRelay/omnigraph#1017](https://github.com/ModernRelay/omnigraph/pull/1017)),
which has one serving model, the cluster-backed `omnigraph-server`, on any host
including localhost:

| Path | Actor | View |
|---|---|---|
| Served (the cluster-backed server) | Resolved from the credential; `--as` sends `Omnigraph-If-Actor`, an assertion the server checks | The actor's view |
| Direct access from the CLI or an embedding | Declared with `--as`, else `operator.actor` | Storage holder. Where the engine enforces the graph's policy for the declared actor, it applies that actor's view too, as a convenience, not as confidentiality |

The server activates a graph with `actor-views` declarations only under a
version-2 cluster policy, which defines `internal` and `bypass`. It refuses to
serve such a graph without authentication, which today allows every graph
action, and with static tokens but no policy, which today allows every read
(`handlers.rs`, the no-policy branch of the authorization check). Cluster apply
reports the refusal before activation.

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
see, including intermediate nodes of a multi-hop pattern.

Search is exact and scored from the view
([Restricted search](#restricted-search)):

- `bm25` ranks the visible rows using statistics over the visible rows only:
  document count, document frequencies and average length. Hidden rows and the
  state of the full-text index never change a restricted score.
- `nearest` compares the query with every visible vector.
- `rrf` fuses those ranks.
- All three return `min(k, visible)` results. A search whose visible input
  exceeds `max_exact_search_rows` is refused with `view_limit_exceeded`.

Bypass actors and unrestricted reads keep today's search.

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
| Edge insert | Both endpoints must be visible, and the edge type's fixed placement writable; a hidden endpoint fails exactly as a missing one. |
| `load --mode overwrite` and `load csv … overwrite` | Refused when the actor's view of any type in the batch is restricted. |
| Branch merge, including a dry run and `--as-commit` | Refused with `view_write_denied` and no conflict payload unless every row changed on either side since the merge base is writable by the actor. Only then is the merge evaluated, and its conflicts, all on visible rows, are reported as today. |
| Writes to identity bindings | As in [Identity bindings](#identity-bindings). |
| Schema apply, `optimize`, `cleanup`, index and full-text builds, `upgrade` | They validate or report on every row of the types they touch, so on an `actor-views` graph they need a bypass actor as well as their Cedar action. |

Access edges have their own rules, for every actor that is not bypass:

- Writing a `@grant`, `@deny`, `@inherits` or `@requires` edge into or from a
  securable needs `owner` on that securable (the target of a grant or deny, the
  source of an inherits or requires edge).
- Writing a `@member` edge into a group needs `owner` on it. A group type that
  is not also a securable has no owners, so only bypass actors manage it.
- Access edges, `@principal` properties, and securable and group rows are
  written only on `main`. A branch write, a merge, a branch promotion or a
  restore that touches them is refused. Without that rule a branch forked before
  a revocation would bring the revoked access back.

#### Constraint disclosures

Constraints hold over the whole type, hidden rows included, so a constraint
check's outcome can depend on hidden rows. Every system surveyed enforces
constraints globally and documents the resulting channel; Postgres says
constraint checks "always bypass row security". This RFC states exactly what a
restricted writer of a placed type may learn. The table lists every write check
whose outcome can depend on hidden rows or hidden schema; every other check
reads only the view, and every outcome that would say more is refused:

| Check | What the outcome may reveal | What is withheld |
|---|---|---|
| `@key` | Whether the key exists in the type | The row's id, values and placement |
| `@unique` (including an interface-wide one) | Whether the written value tuple exists in the type | The other holder's id, values and placement |
| `@card` maximum, on an edge insert | Whether the source already has the maximum number of edges | The count and the hidden edges |
| `@card` minimum, on an edge delete | Whether the source keeps at least the minimum without the deleted edge, which can depend on hidden edges | The count and the hidden edges |
| Cascade on delete | Whether the node has an edge the writer cannot see | Which edges, how many |
| Branch merge | Whether a row outside the view changed on either side since the merge base | Which rows, how many, and every conflict |
| Required properties | Nothing: a placed column is nullable, and no error names a hidden column | — |
| Securable keys | Nothing: only bypass actors create securables in the first scope | — |

Errors for restricted actors carry the constraint and visible property names
only; today's `@card` message names the source and the count, which the server
strips. The lint above flags placed types whose keys or `@unique` properties
are human-meaningful; opaque keys make probing impractical. Uniqueness scoped
to a placement, which would close the `@unique` channel, and cardinality over
visible edges, which would close the `@card` channels at the cost of integrity,
are open questions ([Unresolved questions](#unresolved-questions)).

### Agents and delegated credentials

An agent acting for a user presents a ***delegated credential***, whose issuance
belongs to the delegated-credential work tracked in
ModernRelay/omnigraph#978. It names the user's principal as ***subject*** and
the agent's principal as ***actor*** (the RFC 8693 `act` claim). It may carry
***caveats*** that only narrow: a list of securables, a list of types, an action
subset, a list of branches and an expiry. The effective readable set is

```text
R(subject) ∩ R(actor) ∩ covered(caveats)
```

and the schema view is narrowed by the caveat types. A caveat's securable list
covers each listed securable, and each securable that inherits from a covered
one when every one of its `@requires` targets is covered too. A summary that
also derives from a source outside the caveat therefore stays outside it.

The `Omnigraph-If-Actor` assertion of the graph access RFC names the actor. Cedar evaluates each action for both the subject and the actor, and both
must be allowed. Commit receipts record both. `omnigraph access` reports the
effective view.

Keeping content away from agents needs no special tier: put agent principals in
a team and `@deny` that team the securables in question.

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
- **Access that starts at a time** uses `@starts`: an embargoed release is
  placed in a securable granted to the internal group with `starts` at the
  release time.
- **Relationships that should confer access** become access edges by explicit
  opt-in, for example a `CompChain: CompFile -> CompFile @inherits` edge mirrored
  from the org chart so that each manager chain reads its reports' compensation
  files. Such edges take on the access-edge rules: written only on `main` and by
  owners. They belong on purpose-specific securables: an `@inherits` edge between
  personal securables would also hand a manager the report's private notes.
- **Rules over the row's own columns** become ***computed placement*** in
  [Phase 7](#rollout): a placement property whose value the engine sets on every
  insert and update from an expression over that row's own columns.
- **Rules over other rows, counts and negation over other rows** (notes that
  mention my accounts, records visible after three approvals) are served by
  stored queries that run with their definer's view: the caller gets answers,
  not a view to query freely ([Answer capabilities](#answer-capabilities)).

### History, branches and feeds

Authorization always comes from current authority: rules from the applied
policy, access edges and placement annotations from `main`. Revocation
therefore covers history, and joining a team shows that team's history. Relaxing
access exposes history too; BigQuery documents the same consequence.

In the first scope, for any placed type the actor's view restricts:

- reading a historical snapshot, a commit's changes, the change feed, an export
  or a baseline is refused with `view_restricted`;
- graph diff, branch diff and merge-base, which compare states and show row
  images, are refused with `view_restricted`;
- creating a branch at a past commit, promoting a branch or restoring an old
  state is refused with `view_restricted`;
- reading or inspecting an unpublished commit or an unattached snapshot is a
  snapshot read and is refused with `view_restricted`;
- ordinary branch reads follow the branch's placements, with the retraction
  limit in [What is not promised](#what-is-not-promised).

Bypass actors and share reconcilers are unaffected. A restricted writer settles
an uncertain write through its own attempt ([Server surfaces](#server-surfaces)).
[Phase 9](#rollout) serves view-filtered history, diffs and feeds, and applies
the dual-placement rule to branches.

### Surfaces

Each operation has one name, request and result across CLI, HTTP and MCP, as the
graph access RFC requires. Under a restricted view:

| Operation (CLI, HTTP, MCP tool or resource) | Under a restricted view |
|---|---|
| `query`, stored queries, `read` and MCP `query`, `queries` | Answered over the view; the catalog of stored queries lists only queries that typecheck against the schema view. |
| Writes: `mutate`, `load`, `load csv`, branch writes, MCP opt-in write tools | As in [Writing under a view](#writing-under-a-view). |
| `explain`, `explain load csv` | Omits row estimates, statistics, dataset paths, versions, index facts, full-text coverage and eligibility, pipeline and cost inputs for restricted types. |
| `profile` | Local only today, so storage-holder access; served profiles would be bypass only. |
| `as commit`, `as unattached branch`, `commit publish`, `commit drop`, `commit list --unpublished`, `--unattached` | Creating a proposal or snapshot follows the write rules. Reading or inspecting one is a snapshot read: `view_restricted` until Phase 9. Publishing re-checks the commit's changes for the publishing actor ([Writes](#writes)). Drop changes no data and is gated by Cedar alone. |
| `schema`, MCP `schema` and the schema resource | The schema view, regenerated as `.pg` text with hidden types, properties and namespaces absent. |
| The structured type catalog of the polymorphic types draft | The schema view; reads carry the schema view's digest in place of `schema_ir_hash`. |
| The engine catalogue (`og_catalog`, built-in types) | Unchanged; it describes the binary, not the graph. |
| `snapshot`, MCP `snapshot` | Omits `entity_count` and dataset paths for restricted types. |
| `branch list`, MCP `branches_list` and the branches resource | Branch names and heads, as today; branches hold no access facts. |
| `commit list` (paged), `commit show`, `commit changes`, `changes`, `changes baseline`, `export`, MCP `commits_*`, `changes_poll` | `view_restricted` for restricted placed types until Phase 9; a page token binds the view digest. |
| `branch diff`, `graph diff`, `branch merge-base`, `branch merge … dry run` | Diffs and merge-base: `view_restricted` until Phase 9. Dry run: the merge rule. |
| `blob get`, `blob stat` | Only for a visible row; a hidden row answers exactly as a missing one. |
| `access` (`GET /graphs/{id}/access`) | Reports the caller's effective view ([Operator tools](#operator-tools)). |
| Replay of a saved plan | Refused for another actor's plan or a changed view; refusals are redacted ([Plan acceptance](#plan-acceptance)). |
| Error and denial messages | Carry no other row's ids, values, counts, ETags or versions, and no schema hash; the error code and `disposition` object stay as defined. |

### Sharing

A recipient is a principal of a principal type whose actor id the outside party
authenticates as. It is never in the internal group. A share's content is
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
be unacceptable, retention differs, or search must not depend on rows other
compartments hold. Use a separate cluster when the threat includes holders of
storage credentials, when compliance requires key separation, or when a
recipient gets storage access: every graph in one cluster shares one storage
root and one set of credentials today
([Appendix A](#appendix-a-validated-assumptions)).

### Operator tools

The graph access RFC's `omnigraph access` command and
`GET /graphs/{graph_id}/access` route report the caller's own effective access.
This RFC fills their `view` field with facts about the caller's own view only,
none of which changes with hidden rows: whether the view is restricted or
bypass, its digest and access version, the counts of readable, writable and
owned securables (bounded), and the caveats of a delegated credential.

Three operator operations explain other actors' views. They require bypass, are
addressed with `--target`, and are routes as well as commands:

```bash
omnigraph access --target knowledge --actor principal:alice --explain
omnigraph access who-can-see --target knowledge --type Message --id <id>
omnigraph access diff --target knowledge --branch proposed-access
```

`--explain` prints the actor's memberships, readable securables (bounded),
denials and the edges that produced them. `who-can-see` inverts it for one row.
`diff` reports how many securables change visibility for how many actors if a
bypass actor's branch of proposed access edges were merged.

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
   subject graph (`@principal`), and the internal root is defined by policy
   because it is the root of trust. Several placements on one item are
   intersection evaluated at the item, which avoids a label for every combination
   of orthogonal dimensions. Access facts are data, so they are placed too, and
   the values that bind identity to data are protected.
6. **The physical design follows the workload.** Reads dominate and each belongs
   to one actor, so the relation is inverted once per actor into `R(a)` and items
   are filtered by `label IN R(a)` on an index ([Scans](#scans)).

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
| internal group | GitHub's internal visibility: every member of the enterprise, no outside collaborators |
| recipient | Delta Sharing, Snowflake |

Names chosen against the field, and why:

- **Not "scope".** It already means OAuth token scopes, `ResourceScope` and
  `ChangeFeedScope`.
- **Not "label".** It means node type in a property graph.
- **Not "compartment".** It is exact but known mainly from classified systems and
  Oracle.
- **Not "organization".** The managed control plane's organization is a
  WorkOS/AuthKit tenant whose members include invited outside collaborators.
- **Not "everyone" or `*`.** OpenFGA and SpiceDB use `user:*` for all users, the
  public included; the internal group deliberately excludes recipients.
- **Not "namespace" for securable keys.** The schema namespaces draft uses the
  word for type names.
- **`@requires` rather than "within".** "Within" reads as containment and invites
  modeling folders with intersection instead of inheritance.
- **`@in`** names set membership, as GQ's `in` operator does.

### Authority

| Fact | Owner | Changes | Versioned by |
|---|---|---|---|
| Which types and edges take part, and how data is placed | Accepted schema contract on `main` | Rarely; schema apply | The contract in `main`'s `__manifest` |
| Internal groups, bypass groups, limits | Applied cluster policy | Reviewed; cluster apply | Policy digest |
| Principals, groups, securables, access edges, placement values | Graph rows on `main` (placement values also on branches) | Daily | `main`'s manifest pins of the access tables |

Cluster policy groups gate operations and define the two roots; groups and
securables in the graph gate data. Neither mirrors the other. Placement
annotations are read from `main`'s accepted contract by stable identity for every
target, so a schema apply that adds or removes a placement takes effect on every
branch at once, whatever the branch's own contract says.

### Capturing a view

A request resolves its actor at the server as today. The engine gains read
doors that take the actor; invariant 10 extends from writes to reads where a
policy is installed ([Direct and served access](#direct-and-served-access)).

The ***authorization state*** of a request is the applied policy digest and the
`__manifest` pins on `main` of every ***access table*** (each group and
securable type, each principal type, each access edge type), each pin being the
table's identity, published dataset version, staged version and transaction id.
Its ***access version*** is that policy digest plus those pins. It is
deliberately not `main`'s head commit: every unrelated write and every
maintenance run advances the head, while the pins move only when an access table
is written.

The capture must never apply authorization older than the data it protects, the
ordering Zanzibar's §2.2 requires. Otherwise a request could capture access,
then a revocation and new content could both commit, and the request could
capture the new content under the old access.

- **Reads on `main`** take the authorization state and the data from one
  `__manifest` image, captured once under the schema permit. They cannot
  disagree.
- **Reads on another branch or a historical target** capture the data snapshot
  first and `main`'s authorization state after it, as one function under one
  schema permit. Any revocation published on `main` before the data cut is then
  in the captured authorization state. Snapshot-target reads already load
  `main`'s live contract under the schema permit
  (`load_accepted_view_with_schema_gate_held` in `crates/omnigraph/src/db/omnigraph.rs`);
  this design keeps that snapshot instead of discarding it.
- **Writes on `main`** capture the authorization state with their baselines. The
  access pins become expected versions of the publication's `__manifest`
  compare-and-swap, so a concurrent access change by any process makes the
  publication fail and the attempt retry with fresh authority. The schema,
  branch and table gates are contention control, not the guarantee.
- **Writes on another branch** publish on that branch's chain, which `main`'s
  access tables are not part of, so the compare-and-swap cannot fence a
  revocation on `main`. `commit_all` takes the schema permit shared, then the
  attempt branch's gate and its table gates (`crates/omnigraph/src/exec/staging.rs`);
  no gate orders a `main` publication against it, and the gates are
  process-local in any case. Such a write is therefore authorized against the
  authorization state captured in its attempt: a revocation published on
  `main` during that attempt can miss that one attempt, the window stated in
  [What is not promised](#what-is-not-promised). Every later attempt and read
  applies the current state.

Edge windows are evaluated at the request's admission time on the server, one
clock for the whole request, as SpiceDB evaluates expiry in its datastore to
avoid clock skew. A cached view is valid until its earliest `@starts` or
`@expires` boundary.

### Computing the readable set

The engine evaluates the definitions in [Computing access](#computing-access) as
an internal plan on `main`'s snapshot:

1. Resolve the principal by its `@principal` property. If absent, the closure
   holds only the internal group, and only if the actor is in `views.internal`.
2. Compute the membership closure over all `@member` edge types with GQ's
   bounded multi-hop traversal. Multi-hop requires equal endpoint types today
   (`crates/omnigraph/src/engine/operators/expand.rs`), so the closure runs as a
   principal-to-group hop followed by a group-to-group recursion; with
   polymorphic endpoints it is one traversal.
3. Read `@grant` and `@deny` edges whose source is in the closure, by an
   indexed scan on the edge tables' source column.
4. Subtract denials, then evaluate inheritance and requirements to the least
   fixed point.
5. Intersect with the actor's set and the caveats for a delegated credential.

The result is an immutable ***view spec***: the schema view, `R`, `W`, `O`, the
earliest window boundary and a digest. It is cached by access version, actor and
caveats, in a byte-bounded cache that lives outside `invalidate_read_caches`,
whose flush on every observed advance of `main` would otherwise discard views
keyed by immutable state. Readable sets are stored as compressed bitmaps over
dense securable ordinals.

### Bounds

- The membership closure is bounded by `max_memberships`, `R` by
  `max_securables`, membership depth by 16.
- A restricted exact search reads at most `max_exact_search_rows` visible rows.
  The budget comes from the measurements in `blocked_on`.
- Exceeding a bound refuses with `view_limit_exceeded`; the engine never
  truncates a view or a search.
- Reads by external principals get per-actor read admission, extending the
  existing per-actor write caps (`DEFAULT_PER_ACTOR_INFLIGHT_MAX` in
  `crates/omnigraph-server/src/workload.rs`).

`|R(a)|` equals the number of distinct audiences an actor belongs to. It is
small for channels and folders, and can reach 10^5 for a heavy email user when
every thread is its own audience. Beyond `max_securables`, the cheaper physical
encoding stores each item's reader groups on the item and filters by overlap
with the actor's membership closure, as Elasticsearch and Azure AI Search trim
results. That changes the physical encoding only; this RFC does not implement it.

### Identity bindings in validation

Identity-binding rules are checked in the shared attempt validation of mutation,
load and merge ([Writes](#writes)), against the committed state and the staged
rows:

- a staged change to a `@principal` value, a new securable, a changed securable
  key, or a write to the internal group row by a non-bypass actor is refused;
- a staged delete of a securable row is refused for every actor;
- a staged write to a securable or group row, an access edge or a `@principal`
  value on a branch other than `main` is refused;
- uniqueness of securable keys and actor ids across types, and the internal
  group's reserved key, are checked against the committed state of `main`;
  every such row is written on `main`, so a concurrent duplicate makes the
  publication's compare-and-swap fail and the retry sees it.

Under the integer node identity draft (carried with the polymorphic types draft)
a per-row placement would store the securable's packed node reference instead of
its key. Ids are never reused, so a placement can never name a securable created
after it, securables may be deleted, and restricted creation becomes safe. That
switch is [Phase 6](#rollout) and needs four things: placement properties join merge's remapping of coalesced ids
and its correspondence table; securable types are excluded from natural-key
coalescing; a reference to a deleted securable is unreadable rather than an
error; and writers name a securable by key, typed when keys are not unique
across types.

### Plan acceptance

RFC 0047's plan acceptance (`crates/omnigraph-planner/src/validate/`) is the one
door from a plan to execution: `accept` and `check` for a fresh plan,
`accept_replay` for a saved one. The view enters there, not in a second check:

- **Input.** `AcceptInput` gains the request's view spec. Acceptance compiles
  against the ***view catalog***, the accepted catalog without the types, edge
  types and properties whose fixed securables are not in `R`. The engine
  catalogue's built-in types, reached through `Catalog::query_node_type`, stay
  in it. View catalogs are memoized per (contract, schema-view digest), because
  the compiled-query cache matches catalogs by `Arc` identity and the
  accepted-catalog memo holds one slot today.
- **Requirements.** `Requirements` gain view requirements, checked in
  `Requirements::check`:
  - every scan of a restricted placed type carries its view conjuncts, a typed
    IR leaf `ViewSet { type_id, property_id }`, with their own eligibility,
    always before scoring, independent of the user's filter eligibility;
  - no `MetadataCount`, no degree shortcut and no graph-index-derived search gate
    reads a restricted type;
  - every expansion into a restricted type is masked;
  - a restricted `bm25` uses the view-statistics evaluator and a restricted
    `nearest` the exact route ([Restricted search](#restricted-search)).
- **Identity.** The view digest is part of the catalog identity, so
  `catalog_digest` hashes the schema view together with the view digest. The
  existing comparison in `accept_replay` then refuses another actor's plan, and
  a plan whose view changed, with no new mechanism. The digest is recomputed at
  the replay target from the replaying request's own view, as
  `replayed_coverage_holds` recomputes coverage.
- **Redaction.** For a restricted actor, refusals from `accept_replay`,
  `plan_pins_snapshot` and `replayed_coverage_holds` omit schema hashes, dataset
  paths, versions, branches and coverage states.

The `ViewSet` leaf is resolved at lowering from the engine context's view spec to
an `IN` list. It is not a GQ parameter: a replayed plan could carry swapped
parameter values, while the leaf is always resolved from the replaying request's
own view. The conjunct lives in `Predicate::Gq`, because
`Predicate::gq_filters()` drops every other variant.

### Scans

The view conjunct lowers to `col IN (...)` through `ir_expr_to_df_expr` and
reaches Lance through `Scanner::filter_expr`, which takes a DataFusion
expression without SQL parsing (`lance-11.0.0/src/dataset/scanner.rs:1585`).
With a BTREE on the placement column, Lance evaluates the list as an exact
`SargableQuery::IsIn` with no length limit, and the filtered read skips
fragments without candidates
(`lance-index-11.0.0/src/scalar/expression.rs:348-352`;
`lance-11.0.0/src/io/exec/filtered_read.rs:163-206`).

The scan applies the view conjunct as a prefilter in every case. Today the scan
sets `prefilter(!(filter_after_scoring && fts_query.is_some()))`: for `bm25` on a
property whose full-text coverage is partial or absent, the planner chooses
`Eligibility::AfterScoring`, and every pushed filter moves after scoring. A view
conjunct riding on that eligibility would let hidden rows compete for result
slots. The view conjunct therefore has its own placement, and restricted `bm25`
never reaches that code path ([Restricted search](#restricted-search)).

Today a `@index` on a `String` builds a full-text index, not a BTREE
(`node_prop_index_kind` in `crates/omnigraph/src/db/omnigraph/table_ops.rs`). A
per-row `@in` maps its property to a BTREE.

The engine does not use Lance's row-address prefilter for views. On a plain scan
the mask demotes the filter so it loses its scalar index
(`lance-11.0.0/src/dataset/scanner.rs:3298-3306`), and the mask is in stable
row-id space for one exact table version.

### Restricted search

Restricted search on a placed type is exact and scored only from the view
([D10](#d10-restricted-search-is-exact-and-scored-from-the-view)).

**`bm25`.** The engine builds the plan itself:

1. A filtered read of exactly the visible rows: the view conjuncts, prefiltered,
   and nothing else.
2. Lance's `FlatMatchQueryExec::new` over that input
   (`lance-11.0.0/src/io/exec/fts.rs:2949`), with
   `with_base_scorer(MemBM25Scorer::new(0, 0, {}))`
   (`lance-11.0.0/src/io/exec/fts.rs:3058`).
3. The user's eligibility (filters, gate set) after scoring, then the top `k`.

Lance's flat path adds every counted input row to the supplied scorer
(`initialize_scorer`, `lance-index-11.0.0/src/scalar/inverted/index/flat_search.rs:539-588`).
With a zero base the statistics are exactly the visible rows': document count,
document frequencies and total length over the visible rows with nonempty
analyzed text, since the operator counts only a row with at least one token
(`tokenize_and_count`, same file). The query's own filters run after scoring,
so they never rescope the statistics. Every visible row is scored from its
text and no postings are read, so neither hidden rows, nor index coverage, nor
stale index entries left by updates (the overlay-stale rows Lance's indexed
path blocks) can change a restricted score. The tokenizer comes from the column's full-text segment, which
every committed version holds for a declared column (the full-text analyzer
fence in `docs/dev/lance.md`).

This is the statistics population of the analyzed lexical search draft
(ModernRelay/omnigraph#792), "policy-visible entities", with policy-visible
defined as the actor's view. Once that draft's exact evaluator lands, restricted
`bm25` uses it with the same population.

A restricted `bm25` across several member types of an abstract binding needs one
scorer over the union of the visible members' rows. Lance 11's flat operator
counts only its own input and its shared-scorer hook (`with_shared_scorer`) is
`pub(crate)`, so each arm takes the other arms' view statistics as its base
scorer, collected in a first pass. Multi-member `bm25` stays refused for every
actor until the polymorphic types draft admits it
([Polymorphic types](#polymorphic-types)).

The cost is one pass over the visible rows' text per query; the counted batch is
charged to the query's work memory. `max_exact_search_rows` bounds it.

**`nearest`.** Restricted `nearest` on a placed type uses the exact route of the
search contracts draft (ModernRelay/omnigraph#793, RFC 0048): `use_index(false)`
with the view conjunct prefiltered, ties broken by id. An IVF index chooses
partitions from the query alone (`find_partitions`,
`lance-11.0.0/src/index/vector/ivf/v2.rs:1459-1469`) and its late search stops
once it has `k` results (`lance-11.0.0/src/io/exec/knn.rs:1725-1728`), so hidden
vectors can change which visible neighbours an approximate search finds. The cost
is `V × dimensions`, bounded by `max_exact_search_rows`.

**`rrf`** fuses the ranks of restricted arms and inherits both properties.

**No public or uniform statistics.** An earlier draft scored restricted `bm25`
from statistics over rows granted company-wide, or from fixed constants. The
first leaked: those rows include rows recipients may not read and embargoed rows
before their release. The second gives different ranks from the materialized
oracle, and Lance's flat path adds a table's unindexed rows to any supplied
constants, so scores would still depend on index coverage.

Bypass actors and reads that no view restricts keep today's search.

### Traversal

Traversal reads edge tables by key and never reads intermediate node tables
(`execute_expand_bfs` in `crates/omnigraph/src/engine/graph.rs`). Edge reads carry
no filter. Under a view each hop is masked:

- **Indexed mode**: after each hop, the frontier's ids of a restricted placed
  type are intersected with the view by one indexed read of that node table
  (`__id IN frontier AND placement IN R`). The final destination scan carries the
  view conjunct through `hydrate_nodes`, which refuses unsupported predicates.
- **Graph-index (CSR) mode**: refused for restricted placed types until a per-view
  mask exists ([Phase 8](#rollout)). The graph index assigns dense ordinals per
  node type from edge endpoints only, per index instance, and is cached by
  edge-table versions with no actor (`GraphIndex` and `TypeIndex` in
  `crates/omnigraph/src/graph_index/mod.rs`).

Wildcard traversal expands only over edge types whose declared endpoints match,
so the view catalog prunes hidden edge types. Query bindings are concrete node
types today; [Polymorphic types](#polymorphic-types) covers abstract bindings.
`Omnigraph::graph_index()` returns the whole topology and becomes crate-private.

### Writes

The write check runs in end-of-attempt validation, where the engine has the
staged after rows, the deleted ids (cascades and overwrite removals included),
and a committed state pinned to the transaction base with a batched lookup by id
(`CommittedState` in `crates/omnigraph/src/validate.rs`). The validation points
are `validate_staged_mutation` in `crates/omnigraph/src/exec/mutation.rs`, the
loader's `validate_changeset_with_access`, and merge evaluation over
`CommittedState::merge`. `commit_all` in `crates/omnigraph/src/exec/staging.rs`
revalidates the whole transaction before any effect, and the publication's
compare-and-swap expects the access pins captured by the attempt
([Capturing a view](#capturing-a-view)). Effects staged before a refusal are
detached and unreachable.

The committed state gains a lookup of committed placement values by id, which
supplies the before image for upserts, deletes and overwrite removals. Lance's
merge-insert cannot express "update if writable, else fail" atomically:
`WhenMatched::UpdateIf` turns a false condition into a no-op
(`lance-11.0.0/src/dataset/write/merge_insert.rs:425-449`;
`lance-11.0.0/src/dataset/write/merge_insert/assign_action.rs:107-139`). The check
therefore belongs to OmniGraph's validation, not to Lance. An interface-wide
mutation under the polymorphic types draft is checked at the same points, so a
refusal leaves every staged arm detached and the mutation is never partly
applied.

Today an upsert reports `affected_nodes: 1` whether it inserted or updated, and
delete counts include rows matched by id regardless of visibility. Under a view
the statement's match runs over the view, so counts are view counts.

Errors carry their foreign facts (another row's id, a count, an ETag, another
type's version) as structured fields, and the server strips them for a
restricted actor. Today they are formatted into messages, for example the
`@unique` and `@card` messages in `crates/omnigraph/src/validate/kernels.rs`, and
returned verbatim.

The same validation enforces the access-edge rules (owner roles, `main` only)
and the identity-binding lifecycle.

An unpublished commit (`as commit`) is checked when it is written, for its
author. Publishing it (`commit publish`) runs the write check again over the
commit's changes, for the publishing actor and against the authorization state
the publication captures. On `main` publication already requires the head to
equal the proposal's captured parent (`ExactGraphHead`), so an access change
published in between expires the proposal rather than slipping past it. A
merge with `--as-commit` is a merge and follows the merge rule.

### History and branches

A read of a snapshot target captures data and then authorization from `main`, as
above. Historical queries use the live contract with tables rebound by stable
identity (`bind_catalog_aliases` in `crates/omnigraph-catalog/src/lib.rs`), so
annotations bound to stable identity apply to history without new mechanism.
Property binding on historical reads is by name today (`check_stored_fields` in
`crates/omnigraph/src/engine/typed_value.rs`), unlike Blob reads, which compare
stable property ids. A column with a fixed `@in`, dropped and re-added under the
same name, must not expose its old values; historical reads must bind
properties by stable identity before column views serve history.

The first scope refuses historical reads, branch creation at a past commit,
promotion and restore of restricted placed types
([History, branches and feeds](#history-branches-and-feeds)). [Phase 9](#rollout)
admits a historical or branch row only when its placement is readable both at
the target and, if the row exists at `main`'s head, there too, so a row moved
from a public to a private channel stays hidden in old snapshots and on old
branches.

### Change feed, diffs and export

[Phase 9](#rollout) filters where rows are emitted
(`next_emit` and `emitted_image` in `crates/omnigraph/src/changes/enumerate.rs`).
Three requirements: the view digest joins every cursor, page token and diff
token digest (`crates/omnigraph/src/changes/token.rs`); an update across a
visibility boundary is emitted as an insert or a delete; equality across
vintages compares projected rows, so a change to a hidden column emits nothing.
Graph diff and branch diff follow the same rules.

### Server surfaces

Every operation in [Surfaces](#surfaces) passes the actor to an engine door that
applies the view, or refuses with `view_restricted`.

- **MCP.** `/mcp` is mounted whenever the server has a credential source (static
  tokens, signed data tokens or OIDC). Every tool calls its HTTP route's handler
  with the request's actor (RFC 0003, One handler), so tools need no separate
  enforcement. Resources serve the schema view; the schema resource regenerates
  `.pg` text instead of serving the stored source. OIDC tokens are bound to their
  audience and organization, signed data tokens to the cluster; no tool forwards
  a credential.
- **Errors.** Every new refusal is a code in the graph access RFC's one
  registry (`ErrorCode`), with its HTTP status and `disposition`. Each is a
  refusal met before the write started, so its effects are `none`:

  | Code | HTTP | `effects` | `action` |
  |---|---|---|---|
  | `view_restricted` | 403 | `none` | `fix` |
  | `view_write_denied` | 403 | `none` | `fix` |
  | `placement_required` | 400 | `none` | `fix` |
  | `delete_blocked` | 409 | `none` | `fix` |
  | `identity_binding_denied` | 403 | `none` | `fix` |
  | `view_limit_exceeded` | 422 | `none` | `fix` |

  `key_conflict` becomes a code too, and a restricted actor's response omits its
  entity id. Stripping foreign facts never changes the code or the
  `disposition`, and a hidden name produces the same error, disposition
  included, as an undeclared one.
- **Settling uncertain writes.** Today a client settles an unknown outcome by
  comparing the branch head and listing commits, then replays with
  `if-graph-commit`. Branch heads stay visible under a view, but commit listings
  are refused in the first scope. A restricted writer therefore settles through
  the `disposition.check` that a later phase of the graph access RFC adds
  (branch, publication id, head before the write): the server answers whether
  that publication landed and returns that actor's own commit receipt, never
  the commit's other effects, and a conditional replay works as today. Until
  that phase ships, a restricted writer settles by head comparison and a
  conditional replay alone.
- **Serving** follows [Direct and served access](#direct-and-served-access): the
  server refuses to activate an `actor-views` graph without a version-2 cluster
  policy, and cluster apply reports the refusal before activation.

### Polymorphic types

The draft RFC for polymorphic types
([ModernRelay/omnigraph#884](https://github.com/ModernRelay/omnigraph/pull/884),
`2026-10-07-polymorphic-types.md`) makes interfaces and inline unions bindable
and lets an edge endpoint be an interface or a union. It builds on the integer
node identity draft stacked with it (`2026-10-12-integer-node-identity.md`),
under which every edge endpoint is a ***packed node reference***: the endpoint
type's `StableTypeId` in the high 16 bits and the node's `__id` in the low 48.
That RFC adopts this one's rules for per-type visibility. The two designs compose
as follows.

**Member sets come from the view catalog.** An abstract binding resolves its
member set against the actor's view catalog, so a member type placed in a
securable the actor cannot read is absent, exactly as in `materialize(V(a))`.
Visibility is decided per concrete type; `implements` neither grants nor removes
it. `$x is T`, `@type` and `@type_id` comparisons naming a hidden type fail with
the unknown-name error, and a type id bound to a hidden member is refused like an
absent id. Result schemas still never depend on the member set. `view.restricted`
replaces that RFC's withdrawn `policy_denied_member` refusal and
`policy_filtered` marker.

**Placement on interfaces.** A fixed `@in("key")` on an interface property
applies to every implementor that inherits it. That RFC refuses a redeclaration
that drops it and a member that adds a fixed `@in` to an inherited property, so
the property's visibility is the same for every member. Type-level `@in` belongs
to concrete types and is not inherited through `implements`.

**Removing a placement** from any concrete type or property, by any route, is
the act of a bypass actor, since schema apply on an `actor-views` graph needs
one, and schema plan lists each removal. The routes are deleting the
annotation, dropping the placement property (which the constraint-dropping rule
of ModernRelay/omnigraph#1062 would otherwise do silently), removing an
interface property, removing `implements`, and narrowing an endpoint so a member
stops inheriting a placed property.

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
RFC's set-based multi-hop rule admits, and the readable set comes straight from
the grant edges' packed destinations.

**Union scans and polymorphic expansion.** Every arm of a union scan whose member
is a restricted placed type carries its own view conjunct, required at plan
acceptance. A `count` over an abstract binding may sum per-member metadata counts
only for members the actor's view does not restrict. A polymorphic expansion
groups each hop's frontier by the type half of its packed references and masks
each restricted member type.

**Search across members.** That RFC admits `bm25` across members only under one
corpus contract, with one population per query chosen by the actor as this RFC
chooses it, collected by one engine module shared by both RFCs, the only code
that chooses or sums statistics. Since its 2026-10-18 revision a restricted
actor's population is view statistics for every arm
([D10](#d10-restricted-search-is-exact-and-scored-from-the-view)): each member's
visible rows with nonempty analyzed text, summed, with each arm's base scorer
the other arms' sum. The two drafts agree on the plans. The bypass route keeps
Lance's planner and waits for an upstream scanner option, because an
engine-built indexed plan could not block overlay-stale rows. The restricted
route reads no postings, so this RFC builds its plan in the engine. Lance 11's
public `flat_bm25_search_stream_with_options_and_scorer` returns the scorer it
counted, so a first pass with a zero base yields each arm's view statistics
today, at the cost of tokenizing the visible text twice. Multi-member `bm25`
stays refused for every actor until the scanner option ships, so a restricted
actor never ranks across members where a bypass actor cannot.

**Graph index.** That RFC's phase 3 moves the graph index to one global ordinal
space. Per-view masks ([Phase 8](#rollout)) target that space: one bitmap per
view and index instance.

**Write enforcement point.** An interface-wide mutation is checked in attempt
validation and again in the commit revalidation, as every write is
([Writes](#writes)); a check before staging could not see staged placement
values. That RFC's Policy section names the same point.

**Catalog route, schema hash, descriptors and feeds.** The structured type
catalog route that RFC adds serves the view catalog. Reads by a restricted actor
carry the schema view's digest where they would carry `schema_ir_hash`.
Operation descriptors list visible members. In Phase 9, interface filters on the
change feed expand over the view, and a `ChangeScopeMembershipChanged` refusal
names only visible members.

**History.** Both RFCs refuse abstract bindings on historical targets for now.

**Order of implementation.** Whichever lands second carries the integration.

### Other open work

- **Schema apply with live branches** (ModernRelay/omnigraph#963) keeps schema
  apply on `main` and lets it run while branches exist. Adding a placement adds
  an annotation and a BTREE, and that draft classifies a non-full-text index as
  changing no table. Placement
  annotations are access facts read from `main` for every target
  ([Authority](#authority)), so such an apply needs no refusal and takes effect on
  every branch at once.
- **Schema namespaces** (ModernRelay/omnigraph#1015): an ambiguous unqualified
  name's candidate list, and the unknown-namespace error, are computed on the view
  catalog, so neither reveals a hidden type or namespace. Namespace-pattern policy
  rules would contradict [D8](#d8-access-rules-are-facts-not-queries): access is
  never granted by rules naming types.
- **Graph access** (ModernRelay/omnigraph#1017): direct and served access, the
  `access` route, the shared error registry and cross-process writes, as used
  above.
- **Branch at a graph commit** (ModernRelay/omnigraph#1016) and **branch
  operations v2** (ModernRelay/omnigraph#677): creating a branch at a past commit,
  promotion and restore are history operations under the first scope's refusal;
  restore and promotion never bring back access edges on `main` except by a bypass
  actor.
- **Request-aware plan cost** (ModernRelay/omnigraph#996): its fragment
  statistics and cost inputs join explain redaction, and the planner's choice
  between graph-index and indexed traversal for a restricted type is already
  forced by the view requirements.
- **One language for every graph operation** (RFC 0056,
  ModernRelay/omnigraph#670): control reads authorized as their route are
  filtered as their route is.

### Shares

A live share needs nothing beyond a recipient principal and its grants.

A ***share reconciler*** maintains a copy. It reads the source's view-filtered
change feed as the recipient, applies inserts and updates with
`load --mode merge` and deletions with a mutation, and records the source cursor
in a row of the target graph in the same publication as the data, so a restart
resumes exactly. A mutation cannot mix constructive and destructive changes
(`enforce_no_mixed_destructive_constructive` in
`crates/omnigraph/src/exec/mutation.rs`) and load has no delete mode, so one
source commit becomes a branch on the target, written in two steps and merged
once; a restart that finds the branch already present resumes it. A
`409 schema_boundary` or `410 change_feed_gap` triggers a new baseline. The
reconciler is an idempotent function of the source's accepted state, not a queue.
Before each sync it re-checks the share's terms and stops when the share is
revoked or the terms lapse, as the Eclipse Dataspace Components policy monitor
does.

Hosted copies live in a graph of the same cluster. Delivered copies need a
cluster the recipient controls, because graphs in one cluster share storage and
credentials.

### Incremental maintenance

Access tables change whenever a connector syncs. Keyed only by access version, a
single membership change anywhere invalidates every actor's view. In the first
scope connectors publish access changes in batches, and views are rebuilt per
access version. [Phase 5](#rollout) makes maintenance incremental, following
Zanzibar's Leopard index:

- A ***shared closure*** per access version holds the group-to-group membership
  closure and each securable's inputs, and moves to the next version by applying
  the changed access rows between the two pins, read through the change feed of
  the access tables. When that history is gone it is rebuilt from scratch.
- A cached per-actor view is carried forward unchanged when no changed row
  touches the actor's membership closure or a securable it depends on, and is
  otherwise rebuilt from the shared closure.

Derived state that lags is never used: a stale closure or view is rebuilt before
it serves a request.

### Lance surfaces relied on

| Surface | Use | Guard |
|---|---|---|
| `Scanner::filter_expr` with `prefilter(true)` | View conjunct on every scan and on exact `nearest` | New guard: a `String` BTREE `IsIn` is exact and prefilters KNN |
| BTREE `SargableQuery::IsIn` | `placement IN R` | Same guard; list sizes 1 to 10^5 |
| `FlatMatchQueryExec::new` with `with_base_scorer` of a zero `MemBM25Scorer` | Restricted `bm25` over the visible rows | New guard: identical scores before and after optimize and after an index rebuild, and identical scores for two graphs that differ only in hidden rows |
| `use_index(false)` with `prefilter(true)` | Exact restricted `nearest` | New guard: identical results for two graphs that differ only in hidden vectors |
| `count_rows` through `CountPushdown` | Visible count for the search budget | New guard: an exact BTREE count equals a filtered scan count |

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
not a list in policy. One mechanism governs rows, columns and types, and one
kind of group governs data. The trade-off is that the owner of a fixed
securable can expose a column by granting it, where a policy list would have
needed a reviewed change. Every grant is an audited commit, owners are already
trusted with their securable's rows, and schema apply, the only way to remove a
placement, needs a bypass actor.

### D3. Placement is a property

Each placement is a single-valued property, filtered by an indexed `IN`. An edge
would make every read a join through the edge table, and keeping an edge as well
as the property would be a second source of truth. In the first scope the
property holds the securable's key; under integer node identity it holds the
securable's packed reference, which removes key squatting and reuse by
construction ([Phase 6](#rollout)). The cost is that "messages in channels Alice
owns" is a join rather than a traversal until GQ can traverse a reference-valued
property.

### D4. Writes that collide with hidden rows

Refuse, with contentless errors, and state the disclosures exactly
([Constraint disclosures](#constraint-disclosures)). Systems either keep a
separate copy per label (Accumulo, Oracle RDF), which breaks key identity;
refuse (Stardog, Memgraph, Dgraph, Oracle Label Security), which discloses
existence; or exempt the write (TigerGraph upserts), which overwrites hidden
rows. Postgres documents that constraint checks "always bypass row security".

### D5. Silent filtering versus refusal

Silent filtering with `view.restricted`, definer-rights stored queries for
whole answers, and the closing of every whole-table channel.

### D6. Historical snapshots and branches

Current rules, access edges and placement annotations always. Historical reads
and history operations on restricted placed types are refused at first; the
dual-placement rule for history and branches follows. Snowflake applies current
membership to time travel; Databricks applies current ABAC policies to history
and fails closed when a policy column is missing; BigQuery refuses history to all
but administrators on tables that ever had row policies. None evaluates past
policy.

### D7. Vaults

Securables for the long tail; separate graphs where a defect would be
unacceptable or search must not depend on rows other compartments hold; separate
clusters where storage holders, key separation or delivered copies are involved.
Per-graph storage roots, credentials and keys inside one cluster are a gap this
RFC names and does not fill.

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

Data-derived rules enter through `@starts`, opt-in access edges, computed
placement in a later phase, or definer-rights stored queries
([Access derived from data](#access-derived-from-data)).

### D9. Identity bindings are protected

Actor ids, securable keys and the internal group decide who a request is and
which data belongs to which audience, so they are not free text that any writer
with `change` may set. One lifecycle, enforced in shared write validation,
covers all three, instead of an exception per construct. The first scope
reserves their changes to bypass actors, which are typically the connectors that
mirror identity providers and source ACLs, writes them only on `main`, and never
deletes a securable, so no key ever names a second securable. Invariant 6 says a
dropped identity is not the next row that reuses its name; with string keys the
only way to honor it everywhere a placement can live (live branches and retained
snapshots, not only `main`) is never to drop the identity. Packed references
later make deletion safe by construction.

### D10. Restricted search is exact and scored from the view

A restricted actor's `bm25` is scored from statistics over its visible rows, and
its `nearest` scans its visible vectors. That is the only design among those
considered that satisfies the pair contract on Lance 11 without upstream
changes, and it matches the analyzed lexical search draft's population and the
search contracts draft's exact route. Public statistics leaked rows recipients
and embargoes must not reach; uniform constants diverged from the oracle and
still depended on index coverage; approximate vector search depends on hidden
vectors. The cost is CPU and I/O proportional to the visible set, which the
`blocked_on` measurements bound. Acceleration returns only with a proof under
the same contract ([Phase 8](#rollout)).

## Invariants

1. **Respect the substrate.** Views use Lance's scalar indexes, prefilter, exact
   KNN and the public flat full-text operator with a supplied scorer. No storage
   primitive is added. Index state stays derived.
2. **One publication door.** Principals, groups, securables and access edges are
   graph rows published through `__manifest`. Share copies publish through load
   and mutate. No side table holds access facts.
3. **One coherent view.** A read takes authorization no older than its data: one
   image on `main`, data then authorization elsewhere. A write's access pins are
   expected versions of its publication. A retry captures both again. The plan's
   catalog identity includes the view digest.
4. **Publish once.** Write checks run inside attempt validation and the commit
   revalidation; a refused write publishes nothing.
5. **Crash convergence.** No new durable pre-publication effect. The share
   cursor publishes with the data it describes.
6. **Stable identity.** Annotations bind to stable type and property identity,
   and identity bindings in data change only through one protected lifecycle.
   Historical property binding must move to stable identity before column views
   serve history.
7. **Derived acceleration.** View specs, the shared closure, graph-index masks and
   any future search cache are derived state keyed by immutable versions. Missing
   or stale derived state is rebuilt or refuses; it never widens a view. Index
   coverage never changes a restricted search result.
8. **Loud integrity.** Every refusal is a typed code with a disposition. A view or a
   search is never truncated silently. A fixed placement naming a missing
   securable is refused, not hidden.
9. **Typed semantics.** The view is a typed IR leaf with its own eligibility,
   required at plan acceptance. Caveats compile to the same structure. No filter
   is built as a string.
10. **Boundary and engine.** Actors resolve at the server. When a policy is
    installed, the engine applies views on reads as it already enforces writes;
    direct and local access are storage-holder access.
11. **Bounded.** Memberships, securables, depth, exact searches, caches and
    external reads are bounded; exceeding a bound refuses.
12. **One source of truth.** Access facts exist only in the graph; the roots of
    trust only in policy. Connectors mirror source ACLs into the graph, which is
    then the authority.
13. **Evidence.** See [Evidence and tests](#evidence-and-tests).

Deny-list: incremental maintenance and the share reconciler are idempotent
reconcilers from accepted state, not job queues. No logical precondition depends
on index coverage: restricted search is coverage-independent instead. No
process-local lock fences anything new: writes rest on the `__manifest`
compare-and-swap.

## Compatibility and reversibility

- **Opt-in.** Graphs without the new annotations and version-1 policies behave
  as today.
- **Schema feature.** The annotations set `actor-views` in the contract's
  required features. Builds refuse unknown features (`is_known_feature` and
  `required_features` in `crates/omnigraph-compiler/src/catalog/schema_ir.rs`).
  Without that, an older build would treat the annotations as metadata and serve
  the graph unfiltered.
- **Schema apply.** Adding these annotations is a new plan step; today any
  annotation change other than `@description` and `@instruction` is refused
  (`crates/omnigraph-compiler/src/catalog/schema_plan.rs`). On an `actor-views`
  graph schema apply needs a bypass actor.
- **Policy.** Version-2 bundles are refused by older builds, which refuse
  unknown versions and fields.
- **Wire.** The `view` response object, the new error codes and the redacted
  explain fields are additive; `openapi.json` is regenerated.
- **Storage.** No format change beyond contract content and a BTREE on each
  per-row placement property, which is derived. Packed placement references in
  Phase 6 depend on the integer node identity draft's format change.
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
- **A public `*` subject, or the managed organization as the company-wide
  group.** Recipients and invited outside collaborators would receive company-wide
  grants.
- **Denial that does not cut membership.** Denying a group would leave its
  grants flowing to its members.
- **Sensitivity classes in policy.** A second group system beside graph
  membership, and two mechanisms for one idea.
- **Placement as an edge.** A join on every read and, alongside a property, a
  second source of truth (D3).
- **Free-text identity bindings with per-construct exceptions.** Lets any writer
  take over identities, squat keys and inherit rows by key reuse (D9).
- **Public or uniform statistics for restricted `bm25`.** Leak or diverge (D10).
- **Approximate restricted `nearest`.** Hidden vectors change the answer (D10).
- **The view filter riding the user's eligibility.** Moves after scoring on a
  partly indexed table and lets hidden rows compete for results.
- **A separate view validator in `engine::execute`.** Two acceptance doors that
  can drift; plan acceptance is the one door.
- **Authorization captured before data.** Lets a revocation and new content slip
  between the two captures.
- **An external Zanzibar service** (SpiceDB with Materialize, OpenFGA). A second
  store with its own consistency tokens and a dual-write problem; list APIs are
  bounded (OpenFGA ListObjects defaults to 1,000 results in 3 s).
- **Per-row ACL lists** (Accumulo, LabelList). Every row carries principals;
  Lance's LabelList index looks labels up one at a time
  (`lance-index-11.0.0/src/scalar/label_list.rs:140-144`), and access changes
  rewrite rows. Kept as the physical encoding for very large per-item audiences
  ([Bounds](#bounds)).
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
| Plan acceptance with views | Planner `validate` tests: requirements, view eligibility, catalog identity, replay refusal redaction |
| Engine read and write enforcement, identity bindings | `crates/omnigraph/tests/policy_engine_chassis.rs` |
| Non-interference across operators | `crates/omnigraph/tests/integration/proptest_equivalence.rs`: pairs of generated graphs that differ only in hidden rows, edges, vectors and text give a restricted actor identical answers, and each equals a bypass answer on the materialized view |
| Restricted vector and full-text search | `crates/omnigraph/tests/search.rs` |
| Traversal masks | `crates/omnigraph/tests/integration/traversal_indexed.rs` |
| Write rules, constraint disclosures | `crates/omnigraph/tests/writes.rs` |
| History and branch refusals | `crates/omnigraph/tests/integration/point_in_time.rs` |
| Replay refusal on view change | `crates/omnigraph/tests/integration/engine_v2_plan_replay.rs` |
| Lance facts in the table above | `crates/omnigraph/tests/lance_surface_guards.rs`, beside `fts_prefilter_changes_unindexed_scores_and_postfilter_keeps_them` |
| HTTP, MCP tools and resources, redaction | `crates/omnigraph-server/tests` `auth_policy` and `mcp` suites; OpenAPI |
| Embedded and remote parity | `crates/omnigraph-cli/tests` parity matrix |
| Capture order and access writes concurrent with reads | `omnigraph-dst` scenario |

GQT gains an actor section so view semantics become logic tests; until then the
Rust owners above carry them.

Cases the generators and fixtures must include:

- **Identity:** a restricted writer moving its actor id onto a privileged
  principal; squatting a securable key before a connector mirrors it; deleting a
  securable on `main`; writing a securable, group or access row on a branch and
  merging it; an explicit `@member` edge into the internal group; a `@deny`
  onto it.
- **Capture order:** a DST schedule that commits a revocation and new content
  between the two captures of a branch read; a write on `main` racing an access
  change in another process, which must retry or refuse; and a branch write
  racing a revocation on `main`, which may land at most that one attempt and
  whose rows the next read of the branch hides.
- **Search:** paired graphs that differ only in hidden rows for `bm25`, `nearest`
  and `rrf`; the same graph before and after optimize and after a full-text
  rebuild, giving identical restricted scores.
- **Set semantics:** graphs combining the internal group, `@requires`,
  `@inherits` and `@deny`, including denial of groups that carry grants. OpenFGA
  shipped authorization bypasses in exactly these combinations
  (CVE-2024-31452, CVE-2026-61709).
- **Windows:** `@starts` and `@expires` boundaries crossed inside a cached view's
  lifetime.
- **Constraints:** paired visible and hidden rows for each row of the disclosure
  table, asserting that nothing beyond the stated fact differs.
- **Caveats:** an inheritor whose `@requires` target lies outside the caveat.

### Acceptance thresholds

- A bypass actor's queries make no more object-store requests than today (the
  warm read cost instrument).
- A restricted read with a warm view cache adds no object-store request on
  `main` and one version probe on another branch.
- A restricted scan of a placed type with a covered BTREE adds at most the index
  reads of one `IsIn` evaluation per placement property.
- Restricted search returns identical results for paired graphs, and identical
  scores across optimize and index rebuilds; its latency at
  `max_exact_search_rows` is recorded by the `blocked_on` measurement.
- An access change is one edge write and governs the next request.

### Upstream surfaces surveyed

Lance 11.0.0: `dataset/scanner.rs`, `io/exec/knn.rs`, `io/exec/fts.rs`,
`io/exec/filtered_read.rs`, `io/exec/count_pushdown.rs`, `dataset/optimize.rs`,
`dataset/write/merge_insert.rs`, `index/vector/ivf/v2.rs`, `lance-index`
`scalar/expression.rs`, `scalar/btree.rs`, `scalar/bitmap.rs`,
`scalar/label_list.rs`, `scalar/zonemap.rs`, `scalar/inverted/*` including
`index/flat_search.rs`, `lance-select` `mask.rs`. Pages read in full: the BTREE,
Bitmap, LabelList and full-text index formats, and the performance guide.

## Rollout

Each phase ships independently and leaves the system safe. Every phase lands
with its user-guide changes, a `changelog.d` note when it is user-visible, and
the regenerated `openapi.json` when the wire changes. This RFC asks acceptance
for Phases 0–3; each later phase is accepted by amendment when its evidence
exists.

| Phase | Ships | Still unavailable |
|---|---|---|
| 0. Hardening | Engine read doors take an actor; acceptance input carries a view slot; refusal of shapes that would drop a filter; registered error codes and redaction framework. | No new contract. |
| 1. Access model and schema placement | Every construct, identity bindings, the internal group, capture order, readable sets, fixed `@in` on types and columns, policy version 2, the `access` route's `view` field. | Per-row placement. |
| 2. Row reads | Per-row `@in`, the visibility door at plan acceptance, masked traversal, exact restricted search, every read surface. | Restricted actors cannot write placed types. |
| 3. Row writes | Write checks, constraint disclosures, access-edge rules, compare-and-swap access pins, own-attempt settling. | — |
| 4. Agents | Delegated credentials and caveats. | — |
| 5. Incremental maintenance | Shared closure, targeted view revalidation. | — |
| 6. Packed placement references | Placements by packed reference; restricted securable creation. Needs integer node identity. | — |
| 7. Data-derived and edge placement | Computed placement; `@in` on edge types. | — |
| 8. Search acceleration | Cached view statistics, accelerated restricted `nearest` with a proof under the pair contract, graph-index masks, row-mask cache. | — |
| 9. History, branches and feeds | Dual placement for history and branches; view-filtered history, feeds, commits, diffs and export. | — |
| 10. Sharing | Recipients, copies, answer capabilities, write-back. | Privacy budgets. |
| 11. Vault infrastructure | Separate RFC. | — |

`implementation` moves to `in-progress` with Phase 0 and to `partial` when
Phase 2 ships.

### Phase 0: hardening

No contract changes.

- Add actor-taking variants of every read door in
  `crates/omnigraph/src/exec/query_doors.rs` and of the snapshot, commit, change
  feed, export and Blob reads. Handlers pass the resolved actor. When a policy is
  installed, the engine applies the `read` and `export` gates itself.
- Give `AcceptInput` a view slot, empty for now.
- Make `Predicate::gq_filters()` match exhaustively.
- Make the `MetadataCount` lowering refuse a scan spec that carries a filter.
- Carry foreign facts in errors as structured fields, and register the new codes
  in the shared error registry.
- Make `Omnigraph::graph_index()` crate-private.

Exit: the canonical workspace gates pass; `policy_engine_chassis.rs` covers an
embedded read denied by policy.

### Phase 1: access model, identity and schema placement

Ships type- and column-level confidentiality on the full access model.

- Compiler: validate the annotations; store them in typed IR bound to stable
  ids; derive `actor-views`; add schema-apply steps; check fixed `@in` keys
  against securables on `main`.
- Engine: the capture order; the readable-set computation with membership,
  denial cutting membership, grants, inheritance, requirements, edge windows and
  the internal group; the byte-bounded view cache; the view catalog at plan
  acceptance; identity-binding validation; the internal group's structural
  rules.
- Policy: version 2 `views.internal`, `views.bypass` and `views.limits`,
  validated at cluster apply.
- Until Phase 2: access edges are readable only by bypass actors; group and
  securable rows follow their type's placement, which can only be a fixed
  `@in("key")`; schema apply refuses a per-row `@in`.
- Server: the schema view on every schema surface; filtered stored-query
  catalogs; explain redaction; the `view` response object and the `access`
  route's `view` field; `view_restricted` on history, diffs, feeds and export for
  any actor whose view hides something; the operator operations.
- GQT: an actor section in the case format.

Exit: the non-interference property holds for schema views; a hidden name and an
undeclared name produce identical errors and outcomes; the readable-set
computation agrees with the set-semantics evaluator over generated access
graphs; every identity case in Evidence is refused.

### Phase 2: row reads

Restricted actors cannot yet write placed types; they are refused with
`view_write_denied`.

- Per-row `@in` on node types with a BTREE on each placement property.
- The `ViewSet` IR leaf with its own eligibility; view requirements in
  `Requirements::check`; the view digest in the catalog identity; redacted replay
  refusals.
- Per-hop frontier masks in indexed traversal; refusal of graph-index mode,
  degree shortcuts and graph-index search gates for restricted types.
- Restricted `bm25` over the visible rows with a zero base scorer; exact
  restricted `nearest`; the search budget; Blob reads checked against row
  visibility.
- Visibility of access data: rows by their own placement, access edges by their
  endpoints, deny edges to owners only.
- Every read surface in [Surfaces](#surfaces), MCP tools and resources included;
  the direct-and-served rule; serving refusals.
- Polymorphic types, if union scans have landed: per-arm conjuncts and member
  sets from the view catalog; multi-member `bm25` stays refused for every actor
  until the polymorphic types draft admits it.
- Lance guards for `IsIn` prefiltering, the flat scorer and `CountPushdown`.

Exit: the paired-graph property holds over scans, aggregates, single- and
multi-hop traversal, `count` and `not` blocks, anti-joins, `bm25`, `nearest` and
`rrf`; restricted scores are identical across optimize and index rebuilds; the
cost thresholds hold; the DST capture-order schedule never returns a row its
request's authorization state hides.

### Phase 3: row writes

- Committed-state lookup of placement values; checks in
  `validate_staged_mutation`, the loader's validation and merge evaluation.
- Personal-securable default, `placement_required`, `view_write_denied`,
  `key_conflict` without ids, `delete_blocked`, overwrite refusal, edge endpoint
  visibility, view-scoped matching and counts.
- Constraint disclosures exactly as tabled, including the `@card` minimum and the
  merge refusal without a conflict payload.
- Access-edge rules: owner roles, `main` only.
- Access pins as compare-and-swap expectations; the own-attempt settle lookup.
- Server stripping of foreign facts for restricted actors.

Exit: `writes.rs` and `policy_engine_chassis.rs` cover every row of the write and
disclosure tables; each refusal leaves the manifest head, table pins and lineage
unchanged; a write on `main` racing an access change in another process retries
or refuses; a branch write racing a revocation on `main` lands at most that one
attempt, as [What is not promised](#what-is-not-promised) states, and the next
read of the branch applies the revocation.

### Later phases

Phases 4–11 are designed in the sections above. Each lands with an amendment
that states its evidence:

- **Phase 4, agents**: delegated credentials from
  ModernRelay/omnigraph#978, caveats including branches, subject and actor in
  Cedar, receipts and the `access` route.
- **Phase 5, incremental maintenance**: the shared closure and targeted
  revalidation, proved equal to from-scratch builds over generated change
  sequences.
- **Phase 6, packed placement references**: after integer node identity, with
  the four conditions in [Identity bindings in validation](#identity-bindings-in-validation).
- **Phase 7, data-derived and edge placement**: computed placement and `@in` on
  edge types, with view conjuncts on edge reads.
- **Phase 8, search acceleration**: only mechanisms proved under the pair
  contract.
- **Phase 9, history, branches and feeds**: stable-identity property binding,
  dual placement, view-filtered feeds, commits, diffs and export.
- **Phase 10, sharing**: shares, the reconciler, definer-rights stored queries,
  write-back, per-actor read admission.
- **Phase 11, vault infrastructure**: a separate RFC.

## Unresolved questions

For the first scope:

1. Whether design partners accept the write disclosures in
   [Constraint disclosures](#constraint-disclosures), or need `@unique` scoped to
   a placement, or `@card` counted over visible edges for restricted writers,
   before Phase 3.
2. Whether denied principals should see the deny edges that bar them, as some
   ethical-wall practice expects, or only owners.
3. Whether restricted reads on branches forked before a move need the
   dual-placement rule in Phase 2 rather than Phase 9.
4. At what audience size the item-side reader encoding replaces `IN` for a type,
   and whether that choice is declared or measured.

For later phases: the syntax of computed placement and how a changed expression
rewrites existing rows; whether commits with no visible change are listed to
restricted actors in Phase 9; and a GQ form that traverses a reference-valued
placement.

## Decision log

- 2026-10-11: Drafted. The design was checked against the code at `6e31d0ab` and
  Lance 11.0.0.
- 2026-10-11: Aligned with the polymorphic types draft
  (ModernRelay/omnigraph#884): member sets from the schema view, placements on
  interface properties fixed across members, removal of a placement restricted,
  polymorphic access edges, per-arm view conjuncts, global-ordinal masks.
- 2026-10-11: Rewrote the declaration layer to a single scope concept, replacing
  thirteen annotations, four policy keys, four new actions, visibility tiers,
  integrity labels and compound securables.
- 2026-10-12: Revised from a first-principles review: `@group` and `@securable`
  roles, `@inherits`, denial cutting membership, a policy-defined company-wide
  group, `@starts`, edge placement, computed placement, incremental maintenance,
  field names, D8 and [Appendix C](#appendix-c-query-based-access-compared).
- 2026-10-17: Revised against the reviews of 2026-10-12 and 2026-10-17, against
  `main` at `9575b044`, and against the open drafts that touch access:
  - The contract is stated over pairs of graphs with equal views; the
    materialized equation is the oracle under identical evaluators.
  - Restricted search is exact and scored from the view (D10). Public and uniform
    statistics are withdrawn; restricted `bm25` uses Lance's flat operator over
    the visible rows with a zero base scorer, matching the analyzed lexical
    search draft's population; restricted `nearest` is exact.
  - The view conjunct has its own eligibility, always before scoring, because
    `main`'s eligibility rule moves filters after scoring on partly indexed
    properties.
  - View requirements and the view digest moved into RFC 0047's plan acceptance;
    replay refusals are redacted.
  - Authorization is never older than the data: one image on `main`, data then
    authorization elsewhere, access pins as compare-and-swap expectations for
    writes, stated staleness for writes on other branches.
  - Identity bindings (actor ids, securable keys, the internal group) have one
    protected lifecycle (D9); packed placement references are a phase.
  - The company-wide group is the structural internal group, renamed from
    "organization", which already names the managed control plane's tenant.
  - Constraint disclosures are stated exhaustively.
  - Caveats cover an inheritor only when every `@requires` target is covered.
  - Direct and served access, the `access` route, the shared error registry and
    MCP's current surface follow the graph access draft and RFC 0003.
  - History operations of ModernRelay/omnigraph#1016 and #677 fall under the
    history rule; dropping a placement property is a placement removal; schema
    namespace errors are computed on the view catalog.
  - Acceptance is asked for Phases 0–3; incremental maintenance, packed
    references, computed and edge placement, acceleration, history and feeds,
    and sharing are later phases.
  - Code citations moved to file and symbol at `9575b044`.
- 2026-10-20: Revised against the review of 2026-10-18:
  - A branch write is authorized at its attempt's captured state. The window in
    which a revocation on `main` misses one branch attempt is stated in the
    contract, in What is not promised and in the Phase 3 exit, because no
    cross-chain compare-and-swap exists and a process-local gate fences
    nothing across processes.
  - Securables are never deleted in the first scope, and securable, group and
    access rows are written only on `main`, so no key ever names a second
    securable on any branch or snapshot. Keys are immutable source ids;
    deletion returns with packed references.
  - The disclosure table lists every check whose outcome can depend on hidden
    rows or schema: the `@card` minimum and merge are added; placed columns are
    nullable so no error names a hidden column; a restricted merge with hidden
    changes is refused without a conflict payload; schema apply and maintenance
    need a bypass actor.
  - Restricted `bm25` statistics cover visible rows with nonempty analyzed text,
    as the analyzed lexical search draft defines them; the query's filters do
    not rescope them.
  - The polymorphic types draft adopted view statistics on 2026-10-18 and
    records the same plan split; multi-member `bm25` stays refused for every
    actor until its scanner option ships.
  - Aligned with the graph access RFC as revised on 2026-10-20: one serving
    model, the cluster-backed server on any host; the `Omnigraph-If-Actor`
    assertion; the `disposition` object, whose `check` a later phase adds.

## Appendix A: validated assumptions

The assumptions were validated at `6e31d0ab` and rechecked at `9575b044`.
Verdicts: ✓ confirmed, ◐ partly true (the design uses the corrected form),
✗ false (the design does not rely on it).

### Lance 11.0.0

| Claim | Verdict | Evidence |
|---|---|---|
| `filter_expr` bypasses SQL; `prefilter` defaults to false | ✓ | `dataset/scanner.rs:1585, 1286` |
| Row-address prefilter honored by KNN, indexed FTS, flat FTS, plain scan | ◐ | Mask is in `_rowid` space; applied after scoring on flat FTS; demotes the filter on plain scans (`scanner.rs:1466-1470, 4995, 3298-3306`) |
| BTREE and Bitmap `IsIn` exact, unbounded, NULL disables index | ✓ | `lance-index` `scalar/expression.rs:348-352`; `btree.rs:3347`; `bitmap.rs:1848` |
| LabelList looks labels up sequentially | ✓ | `scalar/label_list.rs:140-144` |
| BM25 statistics are index-wide, including filtered and deleted rows | ✓ | `scalar/inverted/index/search.rs:53-63`; `scorer.rs:197` |
| The flat full-text path adds its counted input to any supplied scorer | ✓ | `initialize_scorer`, `scalar/inverted/index/flat_search.rs:539-588` |
| `FlatMatchQueryExec::new` and `with_base_scorer` are public; `with_shared_scorer` is not | ✓ | `io/exec/fts.rs:2949, 3058`; `with_shared_scorer` is `pub(crate)` at `fts.rs:2180, 3063, 3490` |
| IVF partition choice ignores the prefilter; late search stops at `k` | ✓ | `index/vector/ivf/v2.rs:1459-1469`; `io/exec/knn.rs:1725-1728` |
| `count_rows(None)` is manifest-only | ✓ | `dataset/fragment.rs:1366-1392` |
| Mask plus `count_rows` | ◐ | `CountPushdown` expects a `ScalarIndexExec` (`io/exec/count_pushdown.rs:228-235`); untested, guarded before use |
| Compaction materializes deletions above 10%; stable ids skip index remap | ✓ | `dataset/optimize.rs:322, 2580` |
| Merge-insert cannot fail on a conditional match | ✓ | `merge_insert.rs:425-449`; `assign_action.rs:107-139` |
| The flat full-text operator counts only rows with at least one analyzed token | ✓ | For row-level documents, `tokenize_and_count` in `scalar/inverted/index/flat_search.rs` appends a row only when `all_tokens > 0` |
| A public flat search returns the scorer it counted | ✓ | `flat_bm25_search_stream_with_options_and_scorer`, `scalar/inverted/index/flat_search.rs:857` |

### Engine read path

| Claim | Verdict | Evidence |
|---|---|---|
| Every filtered scan prefilters | ✗ on `9575b044` | A `bm25` scan with `Eligibility::AfterScoring` (partial or absent coverage) sets `prefilter(false)`: `NodeRead` in `crates/omnigraph/src/engine/scan.rs`, the eligibility choice in `crates/omnigraph-planner/src/optimizer.rs` |
| Plan validation runs in `engine::execute` | ✗ on `9575b044` | `execute` takes an `AcceptedBoundPlan`; acceptance is `accept`, `check` and `accept_replay` in `crates/omnigraph-planner/src/validate/` |
| `AcceptInput` carries an actor or view | ✗ | `AcceptInput` in `validate/mod.rs` |
| Replay compares a catalog digest and prints both hashes on mismatch | ✓ | `accept_replay` and `catalog_digest` in `validate/replay.rs` |
| `MetadataCount` ignores the scan filter at lowering | ✓ | `crates/omnigraph/src/engine/lower.rs` |
| `Predicate::gq_filters()` keeps every variant | ✗ | Drops non-`Gq` variants (`crates/omnigraph-planner/src/logical.rs`) |
| BFS never reads intermediate nodes; edge reads unfiltered | ✓ | `execute_expand_bfs` in `crates/omnigraph/src/engine/graph.rs` |
| The graph index is a whole-graph cache with no actor | ✓ | `GraphIndex` in `crates/omnigraph/src/graph_index/mod.rs`; `GraphIndexCacheKey` in `runtime_cache.rs` |
| Interfaces cause polymorphic scans | ✗ | Bindings are concrete; lookups go through `Catalog::query_node_type` |
| Multi-hop traversal accepts different endpoint types | ✗ | "a multi-hop traversal requires the same endpoint type" (`crates/omnigraph/src/engine/operators/expand.rs`) |
| Engine read doors take no actor | ✓ | `crates/omnigraph/src/exec/query_doors.rs` |
| Explain exposes whole-table facts | ✓ | `crates/omnigraph/src/engine/explain.rs`; dataset pins in `crates/omnigraph-planner/src/physical.rs`; index statistics in `crates/omnigraph-planner/src/gate.rs` |

### Writes and schema

| Claim | Verdict | Evidence |
|---|---|---|
| Keyed insert is a full-row upsert | ✓ | `PendingMode::Upsert` in `exec/mutation.rs`; `WhenMatched::UpdateAll` in `table_store.rs` |
| Overwrite replaces all rows of a type | ✓ | `PendingMode::Overwrite` and `overwrite_removed_ids` in the loader |
| Delete cascades and counts hidden edges | ✓ | Cascade in `exec/mutation.rs` |
| Constraint messages name other rows or counts | ✓ | `@unique` and `@card` messages in `crates/omnigraph/src/validate/kernels.rs` |
| A before image exists at validation | ◐ | Not held; obtainable from `CommittedState` in `validate.rs` |
| `@index` on a `String` builds a BTREE | ✗ | Builds full-text (`node_prop_index_kind` in `table_ops.rs`) |
| Unknown annotations are refused | ✗ | Kept as metadata by the schema parser; unknown features are refused (`is_known_feature`) |
| Edge properties may be named `from` | ✗ | `from` and `to` are reserved insert parameters on edges (`docs/user/schema/index.md`) |
| Schema apply is main-only with a single live branch | ✓ | `schema_apply.rs`; ModernRelay/omnigraph#963 would lift the single-branch part |
| Edge endpoints have BTREEs | ✓ | `table_ops.rs` builds BTREEs on id, src and dst |
| `@card` is checked only at its maximum | ✗ | `card_violations` checks the maximum, then the minimum, and its message names the source and the count (`crates/omnigraph/src/validate/kernels.rs`) |
| A null for a non-null property is refused with a message naming it | ✓ | `resolve_assignments` in `crates/omnigraph/src/exec/mutation.rs` |

### Server, policy and cluster

| Claim | Verdict | Evidence |
|---|---|---|
| Reads authorized only in handlers; MCP shares them | ✓ | `crates/omnigraph-server/src/handlers.rs`; MCP `call_tool` in `mcp.rs` |
| `/mcp` is mounted only with OIDC | ✗ | Mounted whenever the server has a credential source (`has_credential_source` in `lib.rs`) |
| Static tokens without policy allow reads; open mode allows all graph actions | ✓ | The no-policy branch of the authorization check in `handlers.rs` |
| Signed and OIDC actors without policy are denied | ✓ | "signed data credentials require an applied Cedar policy permit" (`handlers.rs`) |
| Delegation claims accepted | ✗ | OIDC rejects `act` and `impersonator` (`oidc_identity.rs`) |
| Signed data tokens are bound to this server | ✗ | Bound to the cluster: `urn:omnigraph:data:{cluster_id}` (`data_tokens.rs`) |
| Direct CLI access installs a policy | ✗ | "Direct-store access carries no Cedar policy" (`crates/omnigraph-cli/src/client.rs`) |
| Per-actor limits exist | ◐ | Writes only (`workload.rs`) |
| Durable read audit exists | ✗ | Decision logs only when a policy is installed (`log_policy_decision` in `handlers.rs`) |
| Snapshot reads use current policy | ✓ | `resolved_branch_of` in the snapshot-target authorization in `handlers.rs` |
| Historical reads rebind tables by stable identity | ✓ | `bind_catalog_aliases` in `crates/omnigraph-catalog/src/lib.rs` |
| Graphs in a cluster share storage and credentials | ✓ | `crates/omnigraph-cluster/src/store.rs`; `crates/omnigraph-storage/src/lib.rs` |
| Read caches flush on every observed advance of `main` | ✓ | `invalidate_read_caches` in `crates/omnigraph/src/db/omnigraph.rs` |
| A commit holds the schema gate exclusively | ✗ | `commit_all` takes the shared side (`acquire_schema_shared`), so the gates order but do not exclude a concurrent access write |
| A branch commit is ordered against a publication on `main` | ✗ | `commit_all` takes the shared schema permit, the attempt branch's gate and its table gates (`crates/omnigraph/src/exec/staging.rs`); the `__manifest` compare-and-swap is per branch chain |

## Appendix B: prior art

How each part of the model maps to prior art:

| Part | Prior art | Relation |
|---|---|---|
| Groups with nested membership | Zanzibar `group#member` usersets; Active Directory nested groups | Same |
| Securables that hold data and receive grants | SQL Server securables; Databricks Unity Catalog securable objects; SharePoint `SecurableObject`; OCI compartments | Same |
| Membership chains, grants are one hop | Zanzibar's Leopard index (group-to-group closure, then member-to-group); AD groups and ACL entries | Same shape |
| `@grant`, `@inherits`, `@requires`, `@deny` | Zanzibar userset rewrites (union, intersection, exclusion; `tuple_to_userset` for inheritance); SpiceDB `+ & -`; OpenFGA `or`, `and`, `but not` | Same set operations, fixed vocabulary |
| Internal group | GitHub internal visibility; AD Domain Users | Same; differs from the public `user:*` of OpenFGA and SpiceDB |
| Deny-overrides | XACML; Windows deny entries; Purview Information Barriers; Ranger tag denies; Databricks DENY | Same; avoids GraphDB first-match ordering, Neo4j denies that fail open on null, and Drive limited access overridden by a group grant |
| `@starts`, `@expires` | SpiceDB expiring relationships; OpenFGA conditions; Azure PIM | Same |
| Per-row placement with `IN` | Snowflake row access policies with mapping tables, Postgres row security, Elasticsearch document-level security, Glean and Azure AI Search security trimming | Same |
| Several placements, all required | Oracle Label Security compartments, MarkLogic compartment security, classified compartments | Same |
| Fixed placement of a column or type | BigQuery policy tags; Unity Catalog tags driving row filters and column masks | Close |
| Derived content | FIDES and CaMeL reader-set intersection | Same; Purview takes the highest label instead |
| Delegation as intersection | Microsoft delegated permissions; RFC 8693; Macaroon and Biscuit attenuation | Same |
| Access facts in the data graph | Fong's relationship-based access control; Snowflake mapping tables | Variant: one versioned graph; ordering by capture and compare-and-swap instead of Zanzibar zookies |
| Shared closure with incremental maintenance | Leopard (offline index plus incremental layer); AuthZed Materialize | Same shape, keyed by access version |
| Fixed rule set, facts from users | Biscuit and Oso use Datalog with user rules | Same logic fragment, no user rules |
| Bypass | Postgres `BYPASSRLS`; Oracle `EXEMPT ACCESS POLICY`; Databricks exempt principals | Same |
| Search statistics per user view | Büttcher and Clarke's query integration (FAST 2005); STRESS (IEEE S&P 2017) shows that global statistics and even ranks leak | Same remedy |

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
  UCAN; XACML combining algorithms; GCP basic roles; Azure RBAC; GitHub internal
  repository visibility.

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
| Row attribute threshold | Deals ≥ $1M board-only | ✓ | Computed placement (Phase 7) |
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
| Preparing an actor's access | Once per access version | None |
| Each read, per placed type | One BTREE `IN` | The policy's joins and hops, every query |
| Recursive and composed rules | Paid once, building the view | Paid on every read |
| Restricted `bm25` and `nearest` | Exact over the visible set, scored from it | The allowed set built inside each query, and the same statistics problem |
| Data write | One batched lookup of placements | The write predicate per written row |
| Access change | One edge write; views rebuilt per access version (incrementally in Phase 5) | Free, but any data write may change access |
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
