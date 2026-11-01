---
rfc: "2026-10-30-access-authority-views-and-capabilities"
title: "Access authority: views in the graph"
track: maintainer
status: draft
implementation: not-started
authors:
  - ragnorc
created: 2026-10-30
updated: 2026-11-01
discussion: https://github.com/ModernRelay/omnigraph/pull/1210
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC: Access authority: views in the graph

> Written for an OmniGraph contributor who has not read anything about
> permissions yet and has to judge whether this is the right design. It
> assumes nothing beyond the engine itself. Two earlier drafts proposed parts
> of this; they are discussed once, in
> [Relationship to the earlier drafts](#relationship-to-the-earlier-drafts),
> and nothing here depends on having read them. A term set in ***bold
> italics*** is defined at that spot. Code is cited at commit `118161dd`;
> Lance at 13.0.0. The appendix,
> [Implementation notes for agents](#appendix-implementation-notes-for-agents),
> is for whoever builds it.

## Summary

OmniGraph decides access today at the level of a whole graph: an actor may
read a branch or may not. The graphs we want to serve hold data with
thousands of different audiences, are queried by agents that ask questions
nobody wrote down in advance, run tools whose callers must not see the data
behind them, and are shared in part with people outside the company. One
coarse switch cannot do that.

This RFC proposes that access authority is data in the graph, that the
schema says which data with one annotation per meaning, that the engine
enforces it on every request, and that it is carried past a caller only by
a stored query that says so.

- A ***view*** is the set of data a request may see and change. The engine
  derives it from edges that are already graph data: edges that place rows
  in securables, edges that put principals in groups, and edges that grant,
  inherit, require and deny. Six annotations on edge types give those edges
  their meaning. The engine applies the view to every query automatically;
  a query under a view runs as if the data outside the view did not exist.
- `view` is a declaration on a policy rule or on a stored query that an
  action runs under a view other than the caller's own. It has two values:
  `caller`, the view the graph gives the caller, and `all`, the whole
  graph. On a stored query, which is granted by name, `view: { read: all }`
  is how a caller runs an approved computation over data it cannot
  otherwise see; on a rule, `view: all` is how a connector or an
  administrator sees everything. There is no other way a request's view
  differs from what the graph says.

The rule behind the design: **who may see what is data in the graph; a rule
that mentions the caller is a stored query.** A view must hold for any
query, so it is a set with an algebra and a proof. A stored query returns
an answer or performs a write and composes with nothing, so its body may
carry predicates, and it needs no proof beyond its name and its declared
view.

Four layers, each with one job: the data (edges) says who is where; the
schema (annotations) says what those edges mean; the engine (the view)
enforces it on every read and write; policy (Cedar) decides which actions
an actor may perform at all and holds the few roots of trust that must not
be graph data.

External parties get copies of what their view admits, not live access, and
compartments that must not share an engine get their own graph.

## The problem

### What OmniGraph decides today

Every request reaches the server with a bearer token. The server resolves it
to an ***actor***, an identity such as `principal:alice`, and evaluates the
graph's policy bundle, a YAML file compiled to Cedar, for the action the
request performs: `read`, `change`, `schema_apply`, the branch and commit
actions, `invoke_query`. A rule names groups of actors and the actions they
may perform:

```yaml
version: 1
groups:
  readers: [principal:alice, principal:bob]
rules:
  - id: readers-can-read
    allow: { actors: { group: readers }, actions: [read], branch_scope: any }
```

That is the whole model: graph × branch × action. An actor with `read` sees
every row of every type; an actor without it sees nothing. Stored queries
add one step, `invoke_query`, but a stored read still needs `read`, and
`invoke_query` covers every stored query or none, so they do not let anyone
run a computation over data they cannot otherwise see. Finer rules, if any,
live in application code in front of the server.

### What we need to serve

Five situations, each real, each unservable today.

**A company brain.** One graph mirrors Slack, Drive, mail, the ticket
system, the CRM and HR. Every row came from a system that already had an
audience: this channel's members, that folder's sharing list, the
compensation files a manager may see. There are thousands of such audiences,
they overlap, and they change every minute as people join channels and files
are shared. A person, or an agent acting for them, asks a question, and the
answer must be what the graph would say if the rows they cannot see did not
exist: not a count that includes them, not a search result ranked by them,
not an error that mentions them.

**Agent tools.** An agent writes a query that answers one question well,
tests it and deploys it. Other callers, people or agents, should run it by
name without receiving the data behind it: a global ticket count, a weekly
digest, a lookup. OmniGraph is a database for agents; producing, checking and
deploying a query is cheap for them, so authorization should be able to
attach to a query.

**Multi-tenant service.** One graph holds every tenant's data; each tenant's
staff and agents see only theirs. The audience is static and should need no
connector and no per-person bookkeeping.

**External sharing.** A firm gives a client, a partner or an auditor one part
of the graph: a deal room, a matter, a project. Today the only unit is a
whole graph, exported and loaded by hand, with no update and no revocation.
Live access by an outsider also exposes side channels (timing, error paths)
that an insider is trusted with and an outsider is not.

**Agents acting for people.** An agent presents a credential that names the
person it works for. It must hold at most that person's access, usually less,
and when a person's access is revoked, their agents' access must go with it.

### Goals

From those situations:

| | Goal | Why it matters |
|---|---|---|
| G1 | **Correctness.** Hidden data never influences what an actor sees: rows, counts, scores, errors, result schemas. | A permission system that leaks through `count` or through a ranking is worse than none, because it suggests confidentiality. |
| G2 | **Any query.** The rule applies to every query the language accepts, traversal and search included, without a person or an agent proving anything per query. | Agents ask new questions. |
| G3 | **Mirrors source systems cheaply.** One ACL entry in Slack or Drive becomes one write in the graph, and audience changes take effect without a deployment. | Thousands of audiences, changing by the minute. |
| G4 | **Answers above access.** A caller can be given an approved computation, an aggregate or a tool, without the data behind it, and the disclosure is fixed by the query's text, not by whoever wrote it. | Agent tools; cross-tenant reports. |
| G5 | **Explainable.** An actor's effective access can be listed, and no rule order decides it. | Operators must be able to answer "why can Bob see this?" |
| G6 | **Cheap at scale.** Every read under a rule is an index lookup, not a per-row evaluation; limits refuse rather than truncate. | 10^8 rows, 10^5 actors. |
| G7 | **Reviewable and revocable.** An access change can be reviewed before it takes effect; a revocation takes effect at once; stale state cannot revive revoked access. | Legal walls, offboarding. |
| G8 | **Shares outward.** A chosen part of the graph can be given to an outside party with update and revocation. | Clients, partners, auditors. |
| G9 | **Delegation narrows.** An agent acting for a person holds at most the person's access. | Agents everywhere. |
| G10 | **Expressive enough.** The standard catalogue of permission patterns (tenants, folders, groups in groups, per-item sharing, walls, expiring access, need-to-know on several axes, column hiding, private rows, derived content) is expressible, and what is not is named. | A design that handles the first three patterns and improvises the rest grows a second system beside it. |
| G11 | **Few primitives that compose.** Every construct is required by a goal, none is derivable from another, each property of the system has one place it is changed, and no rule exists only to arbitrate between two mechanisms. Explicit enough that an agent can read the schema and the policy and know what it may do. | Each extra construct is a place to be wrong, and agents are the main users. |

### Constraints we inherit

- **The property graph model.** A property holds a scalar, a list or a
  vector; a relationship between two nodes is an edge. A reference to a node
  is therefore an edge, never a value, and the engine already validates edge
  endpoints, cardinality and cascade on delete.
- **The storage substrate.** Rows live in Lance datasets, one per type. A
  filter reaches Lance as an expression it pushes into an index over that
  dataset's own columns; an `IN` list over an indexed column is cheap, an
  arbitrary predicate per row is a scan, and a predicate over another
  dataset is a join the engine runs itself. Full-text search scores a
  document against statistics (how many documents, how often a term occurs)
  that Lance computes over the rows its prefilter admits, and the index's
  own statistics cover every row, hidden ones included. Vector search over
  an index visits partitions, not a filtered subset.
- **One acceptance door.** Every read plan is checked against its query
  before it runs (`crates/omnigraph-planner/src/validate/`), and a bound plan
  can be replayed later. Whatever access rule a plan obeys must be a typed
  part of the plan, checked there and retained through replay, or it can be
  dropped by an optimizer or a replay.
- **Cedar is the policy language.** Actors are resolved at the server;
  policy is a bundle applied by deployment; a rule's name is diagnostic and
  never selects authority. Policy holds the roots of trust: the few actor
  sets that nothing a graph writer can change may alter.
- **Schema changes are serialized.** A schema apply targets `main` with a
  single live branch. Anything that is a schema change pays that cost.
- **No cross-branch atomicity.** Lance publishes one dataset version at a
  time, and OmniGraph publishes a graph commit per branch. There is no
  compare-and-swap across two branches and no distributed lock; a
  process-local lock would fence nothing across processes, and the engine's
  invariants forbid pretending otherwise.
- **No query-equivalence solver.** Deciding whether an arbitrary query is
  answerable from an authorized subset is undecidable in general and
  expensive in practice. OmniGraph will not carry one.

## The design space

Where can the decision "may this actor see this row" live? Each place has
been built somewhere; each gets some goals and fails others.

| Option | How it works | Gets right | Fails |
|---|---|---|---|
| **In the application** (today's practice) | The app filters results before showing them. | Anything the app author can write. | G1 (the engine cannot guarantee anything), G2 (every app repeats it), G5, G6. |
| **At the action level** (today's engine) | Cedar allows or denies `read` on a graph. | Simple, auditable. | Everything finer than a whole branch. |
| **Labels on the data** (row- and column-level security: Oracle Label Security, Elasticsearch document security, SharePoint) | Each row carries a label naming its audience; actors belong to groups; the engine filters every query by the actor's labels. | G1 with a proof, G2, G3 (a label per source audience), G5, G6 (a label is an indexed value). | G4 on its own: an aggregate over hidden rows is silently wrong, and a tool cannot be given more reach than its caller. Row rules that mention other rows or the actor. |
| **Predicates per type** (Postgres row-level security, Oracle VPD) | A policy is any boolean expression over the row and the session; the planner adds it to every query on the type. | Expressiveness: ownership rules, attribute rules, joins. | G1 (no proof; `count`, ordering and errors leak unless each operator is argued), G6 (a predicate is not an index lookup), G5 (predicates compose by evaluation, not by listing). |
| **In the query** (named-query allowlists; query contracts with proofs; the Non-Truman model) | Only approved queries run, or every query must state its own scope and the engine proves it does not exceed it. | G4: the query's text is the disclosure. | G2 (a new question needs a proof; traversal, search and writes need proof rules per operator), G3 (audiences become attributes compared by equality, so groups, inheritance, walls and expiry have no form), G8. A refusal tells an agent to rewrite, which is right for a tool under construction and wrong for a person's question. |
| **Separate graphs per audience** | Each audience gets its own graph or cluster. | The strongest isolation: no shared index, no shared statistics, separate keys. | Overlapping audiences: a company brain would need a graph per channel. |
| **Relations with rewrites** (Zanzibar, SpiceDB, OpenFGA) | Per type, named relations (`viewer`, `editor`, `parent`) and rewrite rules (`viewer = direct viewer ∪ viewer of parent`); the engine resolves a check by walking relations. | Expressiveness including custom roles; a proven production model; the relation tuples are the authority and nothing else is. | The same G4 gap as labels; custom relations cost the simple properties a fixed vocabulary gives (inheritance caps, "write implies read", a one-to-one mapping from source roles, a listable effective access without rewrite evaluation); the relation store sits beside the data, so listing what a user may see needs a second, denormalized index. |

Three things fall out of the table.

First, the options answer two different questions. Labels, predicates and
relations answer *what may this actor see?* Allowlists and contracts answer
*what may this caller run?* A tool that returns a global count has an answer
to the second question and no answer to the first: its caller sees no
tickets and yet receives a number computed over all of them. No amount of
row filtering produces that; no amount of query checking produces a company
brain. The goals need both answers, and a design that offers one and
improvises the other grows a second system in application code.

Second, among the answers to the first question, labels are the only one
that gives G1 with a proof, G3 and G6 at the same time. Predicates buy
expressiveness with the proof and the index; relations with rewrites buy
custom roles with the simple properties. What labels cannot express is
precisely the set of rules whose predicate mentions something other than
the row's own audience: the caller, another row, a count. Those are the
rules a stored query expresses naturally.

Third, in a property graph the label is already there. "This document is in
that folder", "this person is in that team", "that team may read this
folder" are edges, and the relation store Zanzibar keeps beside the data is
the graph itself. The design below therefore does not add a label column:
it reads the edges the graph has, once the schema says which edge types
carry which meaning.

The second question needs much less machinery than it looks. "Run this
approved computation over data you cannot see" is a stored query, which
exists, with two additions: it is granted by name, and it declares that an
action of its body runs under a view other than its caller's. That
declaration, `view`, is also what a bypass actor needs. One declaration
answers the second question and the side need; nothing else is required.

## The decision

**Who may see what is data in the graph. A rule that mentions the caller is
a stored query.**

The decisions that follow, each explained in
[The design, by example](#the-design-by-example):

1. **Access facts are edges.** A row's placement in a securable, a
   principal's membership in a group, and every grant, inheritance,
   requirement and denial is an edge between two nodes. A securable is
   whatever node such edges target; a group is whatever node a membership
   edge targets. Nothing declares either.
2. **Six annotations on edge types give the edges their meaning**:
   `@in`, `@member`, `@grant`, `@inherits`, `@requires`, `@deny`, plus a
   time window on a property. The view is computed by one fixed algebra
   over them. There is no role algebra and no predicate language on the
   data side.
3. **A wall is absolute** for everything beneath it.
4. **Policy's actor sets may be defined from graph groups**, so a company
   does not maintain a list of ten thousand actor ids in a YAML file; the
   rules that can change authority itself name explicit actors only, so the
   roots of trust stay out of the graph.
5. **`view` is the one way a request's view differs from the graph's
   answer**: on a rule or on a stored query, per action, `all` or `caller`.
   Nothing narrows a view except the graph, and nothing widens it except
   `view: all`.
6. **A stored query is invoked by its name alone.** A rule lists the names
   its actors may invoke; the caller needs neither `read` nor `change`,
   because the view bounds the data and the name is the grant.
7. **Access facts live on `main` only**; review of an access change goes
   through an unpublished commit, never a branch.
8. **What the line excludes is declined by name**, each with its fallback.

## The design, by example

The running example is a company brain: people, teams, a folder hierarchy
mirrored from Drive, a ticket system with tenants, documents, private notes,
and comments.

### Who: principals and groups

A ***principal*** is a node that stands for an actor. One of its properties,
marked `@principal`, holds the actor id the server resolves from the token:

```pg
node Person {
  slug: String @key
  actor: String? @principal @unique     // "principal:alice"
}
```

A ***group*** is a node whose row is a set of principals and other groups.
Nothing marks a group; a type is a group because a `@member` edge type
targets it. Membership chains: a member of a subteam is a member of the
team.

```pg
node Team   { slug: String @key }
node Tenant { slug: String @key }
edge MemberOf:     Person -> Team   @member { expires: DateTime? @expires }
edge SubteamOf:    Team   -> Team   @member
edge TenantMember: Person -> Tenant @member
```

A ***subject*** is a principal or a group: anything that can hold access.

Policy decides which actions an actor may perform at all. Its rules name
groups, and a policy group is either an explicit list of actor ids or
defined from a graph group:

```yaml
version: 2
groups:
  platform:  [principal:ops-1, principal:ops-2]           # explicit
  staff:     { members_of: { type: Team, key: staff } }   # the closure of one graph group
  tenants:   { members_of: { type: Tenant } }             # members of any row of the type
```

An actor is in `staff` when its principal reaches `Team:staff` over live
`@member` edges in the captured authorization state of the request (defined
in [Changing access safely](#changing-access-safely)), so a membership
change in the graph moves policy at the next admission, with no deployment.
This is a read-only coupling from policy to the graph, and it is the only
one: policy never writes the graph, and the graph never widens an action
policy denies.

The rules that can change authority itself name explicit groups only: a rule
with `view: all` (below), a rule for `schema_apply`, and the cluster's
`config_manage`. Bundle validation refuses a graph-defined group on those.
The reason is who controls the set. A graph group is controlled by whoever
holds `owner` on it, which is data; the roots of trust must be controlled
by whoever applies policy, and those sets are small: operators, connectors,
bootstrap identities.

### What: securables and placement

A ***securable*** is a node whose row is a thing access attaches to: a
folder, a channel, a tenant, a person. A type is a securable type because an
`@in`, `@grant`, `@inherits`, `@requires` or `@deny` edge type targets it,
because a property names it, or because it places itself. Securables are
identified by type and key, so a key need only be unique in its type, as
every node key is.

***Placement*** says which securable a row is in, and it is an edge:

```pg
node Folder @in { slug: String @key }         // each folder is placed in itself
node Doc    { id: String @key  title: String  status: String }
node Note   { id: String @key  body: String }
node Class  { slug: String @key }             // fixed securables for columns
node Ticket {
  id: String @key
  status: String
  hr_notes: String? @in(Class, "hr-notes")    // this column is in one fixed securable
}

edge InFolder: Doc    -> Folder @in @card(1..1)   // each document is in exactly one folder
edge OwnedBy:  Note   -> Person @in @card(1..1)   // each note is in its owner's securable
edge InTenant: Ticket -> Tenant @in @card(1..1)   // each ticket is in exactly one tenant
```

`@in` has one meaning, "placed in", and three places to say it, differing
only in what names the securable:

- **On an edge type**, the source row is placed in the target row. `@card`
  is the ordinary edge cardinality: `1..1` makes placement required, `0..1`
  makes it optional. A row whose placement edge is absent is in no securable
  and is visible only under a view of everything; nothing becomes visible by
  omission. A type may have several `@in` edge types; a row is then visible
  only to someone who can read all of its placements, which is how "the
  EMEA deals, classified confidential" composes without a securable for
  every combination.
- **On a node type**, each row is placed in itself: the row is its own
  securable. `Folder @in` makes a folder's existence as private as its
  contents. `Person` is a securable (notes are placed in people) but is not
  self-placed, so people stay visible while each person's own securable
  holds their private rows.
- **On a property**, the column is placed in one fixed securable, named by
  type and key. For an actor who cannot read `Class "hr-notes"`, the column
  does not exist. The row must exist on `main` when the schema is applied,
  or the apply is refused; a misspelt key fails loudly instead of hiding a
  column from everyone.

Two models of a document follow from the first two forms, and a schema
chooses per type.

**Containment.** `InFolder: Doc -> Folder @in` says a document is visible to
whoever can read its folder, and nothing else. A document is not a
securable: it cannot be shared on its own, and the readable set `R` a view
computes holds folders, not documents.

**Item-level control.** Drive's model is that every file has its own
sharing list and also inherits its folder's. That is a self-placed type with
inheritance and grants:

```pg
node Doc @in { id: String @key  title: String  status: String }
edge InFolder:   Doc    -> Folder @inherits @card(1..1)   // a folder's readers read the document
edge DocAccess:  Person -> Doc    @grant { role: enum(viewer, editor, owner)  @key(@src, @dst) }
```

Zanzibar writes this as `viewer = direct viewer ∪ parent.viewer`. The price
is that `R` now holds every readable document, so an actor who can read
10^5 documents has a view of 10^5 securables; that regime is bounded by
`max_securables` ([Limits](#limits)), and a type with 10^6 rows that are
all individually shared is past this design's first scope. Containment is
the right model for the long tail of channels, folders and tenants;
item-level control is for the types whose rows are shared one by one.

Why not a `folder` column holding the folder's key? In meaning placement is
a reference to a node, and in a property graph a reference is an edge. A
key stored as a string is a foreign key the engine does not know about: no
endpoint check, no cascade, no cardinality, no traversal, and one more way
to reference a node beside the one the model has. Zanzibar stores the
reference as an opaque `namespace:id` string because it has no graph; we
have one.

The physical side is the engine's business. A view filter runs inside one
Lance dataset, so it must be a predicate over the placed row's own columns.
The engine therefore keeps, for every `@in` edge type, a ***placement
column*** on the source type's table holding the target's key, exactly as
an edge row keeps its endpoints in `__src` and `__dst`. It is a system
column: not in the schema, not nameable by a query, written in the same
graph publication as the placement edge (an edge insert, update or delete
and the column write are one multi-table commit through the one publication
door), so no published graph has one without the other; a schema apply that
adds `@in` to an existing edge type backfills it from the edge table in the
apply's single publication, and one that removes `@in` drops it. `repair`
compares it with the edge table and reports a difference as integrity drift;
a type whose column drifted refuses reads under a view until repaired. A
self-placed type needs no column: its key is its placement. In the
invariants' terms the column is derived physical state of the edge, the
edge is the only logical placement, and nothing reads the column as
authority outside the filter.

### Linking them: grants and roles

A ***grant*** is an edge from a subject to a securable carrying a ***role***:

```pg
edge TeamAccess:   Team   -> Folder @grant { role: enum(viewer, editor, owner)  @key(@src, @dst) }
edge PersonAccess: Person -> Folder @grant { role: enum(viewer, editor, owner)  @key(@src, @dst) }
edge ClassAccess:  Team   -> Class  @grant { role: enum(viewer, editor, owner)  @key(@src, @dst) }
edge TenantAccess: Team   -> Tenant @grant { role: enum(viewer, editor, owner)  @key(@src, @dst) }
```

Roles are ordered: `viewer < editor < owner`. A viewer reads the data in the
securable, an editor also writes it, an owner also changes who has access
to it. There are exactly three, and they form a chain, because operations
form a chain: write without read would allow blind writes, and administer
without write would be a role nobody can act on. A grant reaches one hop
from the subject's membership closure; it does not chain through
securables.

A connector mirroring Drive writes one `TeamAccess` or `PersonAccess` edge
per sharing entry, with `viewer`/`editor`/`owner` mapped one to one. That is
goal G3 in one sentence. A tenant needs no grant at all for reading: a node
that is both a group and a securable, as `Tenant` is (`TenantMember` targets
it, `InTenant` targets it), is readable by its members, by the rule in
[Computing a view](#computing-a-view). A tenant's agents that write tickets
get one `TenantAccess` edge at `editor` from their team.

### Composing audiences: inheritance, requirements, walls, time

Four more meanings finish the algebra. Each exists because one of the
situations needs it and none can be built from the others.

```pg
edge FolderParent: Folder  -> Folder  @inherits @card(0..1)   // readers of the parent read the child
edge DerivedFrom:  Summary -> Channel @inherits @requires     // readable by readers of every source
edge Walled:       Team    -> Folder  @deny                   // never, whatever else says
// Summary is a self-placed node type (`node Summary @in`); Channel is a securable because
// DerivedFrom and the channel grants target it.
```

- **`@inherits`** (securable → securable): readers of the target get their
  role on the source, capped by the edge's optional `role` property
  (default `viewer`). A folder tree is a chain of these. Union over
  securables.
- **`@requires`** (securable → securable): the source is readable only if
  every target is. Intersection, at variable arity; several `@in` on a type
  is the same intersection at fixed arity.
- **`@inherits @requires`** together on one edge type: readers of each
  source flow in, and all sources are required, so exactly those who can
  read every source read the row. A summary derived from two channels is a
  self-placed `Summary` with one `DerivedFrom` edge per source, and it stays
  correct as memberships change. Two meanings composing on one edge, which
  is what an algebra is for.
- **`@deny`** (subject → securable): the subject's members never read the
  securable, nor anything that inherits from it, whatever grants say. An
  ethical wall is a list of people who must not see a deal, so denial is by
  subject, and it is absolute for the subtree. Difference, in one layer.
- **`@starts` and `@expires`** on a `DateTime` property of a member, grant or
  inherits edge: the edge counts only inside its window, evaluated at the
  request's admission time. An embargo, a contractor's window, a temporary
  share: exact at the instant, with no expiry job.

Every audience is then a set expression over groups and securables:
`(granted ∪ inherited) ∩ required − denied`. Union, intersection and
difference are each needed; difference is by subjects only, because a
general difference would add a second layer of negation that nothing
requires.

### Computing a view

For actor `a` with principal `p`, at admission time `t`:

```text
1. Closure      C0 = {p} ∪ every group reached from p over live @member edges
2. Denial       D0 = every securable with a live @deny edge from a subject in C0
                D  = D0 ∪ every securable that @inherits, transitively, from one in D0
3. Closure      C  = as C0, but never entering a group that is in D
4. Roles        the least fixed point of
                   role(p) ≥ owner             if p is a securable         (you own your own securable)
                   role(s) ≥ viewer            if s ∈ C and s is a securable (members read a group that is one)
                   role(s) ≥ r                 if some x ∈ C has a live @grant(r) edge to s
                   role(s) ≥ min(role(x), c)   if x ∈ R and s has a live @inherits(c) edge to x
                R  = { s ∉ D | role(s) ≥ viewer and every @requires target of s is in R }
                W  = { s ∈ R | role(s) ≥ editor }
                O  = { s ∈ R | role(s) ≥ owner }
```

The actor's own view is `(R, W, O)`: the securables it may read, write and
administer, and through placement the types, columns and rows those admit.
Each securable is a (type, key) pair.

Denial is computed first, from the full closure, closed downward over
inheritance, and then removed from everything after it: no membership above
a denied group, no grant from one, no inheritance through one, and no direct
grant on anything beneath one. The negation sits in one layer and everything
after it only adds, so the fixed point is unique, evaluation terminates, and
a cycle never makes anything readable. An operator reading
`GET /graphs/{id}/access` sees exactly this: memberships, the policy groups
they resolve to, denied securables, readable securables with roles.

Worked example:

```text
alice ─member→ team:eng ─editor→ folder:eng
bob   ─member→ team:legal ─viewer→ folder:deal-acme
bob   ─member→ team:acme-wall ─deny→ folder:deal-acme
carol ─editor→ folder:deal-acme
carol ─viewer→ folder:deal-acme/term-sheet     (a subfolder; FolderParent → folder:deal-acme)
bob   ─viewer→ folder:deal-acme/term-sheet     (shared with bob directly)
team:staff ─viewer→ folder:general             (alice, bob and carol are members of team:staff)
team:hr ─viewer→ class:hr-notes
dana  ─member→ tenant:acme                     (a tenant is a group and a securable)
```

| | alice | bob | carol | dana |
|---|---|---|---|---|
| Closure | team:staff, team:eng | team:staff, team:legal, team:acme-wall | team:staff | tenant:acme |
| Denied | — | folder:deal-acme, folder:deal-acme/term-sheet | — | — |
| Readable | person:alice (owner), folder:general, folder:eng (editor) | person:bob (owner), folder:general | person:carol (owner), folder:general, folder:deal-acme (editor), folder:deal-acme/term-sheet (editor) | person:dana (owner), tenant:acme |
| Sees `hr_notes` | no | no | no | no |

Bob is granted the deal room through legal and the term sheet directly; the
wall removes both, because the term sheet inherits from the walled folder.
Carol holds `editor` on the term sheet by inheritance, above her direct
`viewer`; the higher role wins. Dana is a member of one tenant, reads it
because a tenant is a securable, and sees nothing else. Everyone owns their
own securable, which is where their notes are.

### What the engine does with a view

A query needs no change. The engine applies the view in three places, all
before the query runs:

1. **The schema view.** The catalog the actor's query is compiled against
   omits every type and column placed in a securable the actor cannot read,
   and every namespace holding only hidden types. A hidden name is therefore
   an unknown name, and the compiler reports it with the same diagnostic,
   byte for byte, as a name nobody ever declared.
2. **The row filter.** When the plan is accepted, every scan of a placed type
   gains one conjunct per placement: `placement column IN R` for each `@in`
   edge type, `key IN R` for a self-placed type, each restricted to the keys
   of the target type. It is a typed leaf of the plan carrying the view's
   digest, not a string and not a parameter; acceptance refuses a plan on
   which any placed scan lacks it or on which an operator could consume a
   placed row before it. Lance serves it as an `IN` list over the column's
   BTREE index. A traversal applies it at every hop, so no path passes
   through a hidden node. An edge is visible when both its endpoints are.
3. **Search.** Full-text search under a view scores the visible rows with
   statistics computed over the visible rows only, because Lance's flat
   scorer and the index both count hidden rows; vector search compares the
   query with every visible vector; rank fusion fuses those. Each returns
   `min(k, visible)`. A search whose visible input exceeds a configured bound
   is refused, never truncated.

Non-interference (G1) follows: for two graphs that agree on a request's view,
every operator consumes identical inputs, so rows, counts, scores, errors and
result schemas agree. Execution time may differ, and that is why outsiders
get copies.

Silent filtering can mislead: an average over the view is not the company's
average. So every result carries one boolean in its existing envelope,
`restricted`, which is `true` when the view excluded anything from a type the
plan read. It is computed from the view and the plan, never from data, so it
reveals only a policy fact the actor may know. A client that needs a whole
answer refuses on it, or asks for a stored query with `view: { read: all }`.

### Writing under a view

| Operation | Rule |
|---|---|
| Insert or update a placed row | `editor` on every securable the row is placed in, before and after the change: the targets of its `@in` edges, and the row itself for a self-placed type. |
| Insert without a required placement | The `@card` lower bound refuses it, as for any edge. A writer places a private row in its own securable by writing an `@in` edge to `$subject`, its principal row. |
| A placement edge to a securable the writer cannot see | Exactly as a missing endpoint, so the refusal does not reveal whether the securable exists. |
| A placement edge to a securable the writer can see but not edit | `view_write_denied`. |
| Creating a self-placed row | Creating a securable. The mutation must leave the writer with `editor` on it: a `@grant` edge from `$subject` or an `@inherits` edge to a securable the writer holds `owner` on, written in the same mutation. During that mutation the creator may write the new securable's access edges without holding `owner`, since nothing holds it yet. Those edges are access facts, so a self-placed row is created on `main`. |
| Update or delete | Matches rows in the view; a visible row that is not writable refuses the statement. |
| Delete that would cascade to an edge the writer cannot see | `delete_blocked`. |
| Deleting a securable | `owner` on it; its edges cascade under the rule above. A branch forked earlier that still places rows in it carries a dangling endpoint, which its merge reports as a conflict. |
| Edge insert | Both endpoints visible, else exactly as a missing endpoint. |
| Branch merge | Refused unless every row changed on either side since the merge base is writable by the merging actor; only then are conflicts evaluated and reported. |
| Schema apply, `optimize`, `cleanup`, index builds, `upgrade` | Need a view of everything (`view: all` for their actions); they touch every row. |

A writer of a placed type can learn, from whether a write succeeds, a short
list of facts and nothing more: whether a key exists in that type; whether a
`@unique` tuple exists; whether an edge would cross a `@card` bound; whether
a deleted node has a hidden edge; whether a merged row changed outside its
view. Those disclosures are stated, bounded, and the price of keeping the
constraints honest.

***Access edges*** (grant, deny, inherits, requires, member) are the facts
the view is computed from, and they get stricter rules: writing one needs
`owner` on the securable it gives access to (the target of a grant or deny,
the source of an inherits or requires edge) or on the group it adds to; a
principal value is written only under a view of everything for `change`.
All are written on `main` only, for the reason given in
[Changing access safely](#changing-access-safely). Placement edges are
ordinary data: they say where a row is, they are written wherever the row
is, and the merge rule above checks them.

### Rules from a row's own columns: published documents

"Published documents are public" is a rule that reads the document's own
`status`. It is not a label and not a predicate: it is a change to who can
see the document, so it is an edge, written by someone allowed to change
that.

```pg
node Doc @in { id: String @key  title: String  status: String }
edge InFolder:  Doc -> Folder @inherits @card(1..1)
edge Published: Doc -> Folder @inherits @card(0..1)   // to folder:public, which team:staff reads
```

Publishing inserts the `Published` edge, which needs `owner` on the
document, as changing its audience should. A stored mutation keeps the two
facts together so neither drifts:

```gq
query publish($id: String) {
  update Doc set { status: "published" } where id = $id
  insert Published { from: $id, to: "public" }
}
```

deployed with `view: { change: all }` and granted to the team that publishes,
so that an editor who may not administer the document can still publish it
through the approved path, and nothing else. If the status column and the
edge ever disagree, the edge decides and the row is not public: fail
closed. A contained document (`@in` to its folder) has one audience and
cannot be published this way; a row that needs a union of audiences is its
own securable.

### `view`: the view an action runs under

Every action a request performs runs under a view. By default that is the
caller's own, computed from the graph as above. One declaration changes it:

```yaml
view: all                             # every action of the holder runs over the whole graph
view: { read: all, change: caller }   # per action; an omitted action is caller
```

It has two values, `caller` and `all`, and two places to live.

**On a rule.** The view for the actions the rule grants:

```yaml
rules:
  - id: platform
    allow: { actors: { group: platform },   actions: [read, change, schema_apply], branch_scope: any, view: all }
  - id: connectors
    allow: { actors: { group: connectors }, actions: [change], branch_scope: any, view: all }
  - id: staff
    allow: { actors: { group: staff },      actions: [read, change], branch_scope: any }
```

A bypass actor is a rule with `view: all`; it may name explicit groups only.
When several allow rules match one action, the request runs under `all` if
any of them says so, since `all` contains every view. Served requests under
`view: all` are recorded in the policy decision log. A rule's map may name
only actions the rule grants.

**On a stored query.** This is the invoker's-rights versus definer's-rights
switch that databases spell `SECURITY INVOKER` and `SECURITY DEFINER`, with
two differences: there is no definer, because stored queries are deployed by
operators, so the elevated side is simply the whole graph; and it is per
action, so one query can read as its caller and write past it. A stored
query's entry declares the view of each action its body performs:

```yaml
queries:
  global_ticket_count: { file: reports.gq,  view: { read: all } }               # count what the caller may not list
  add_comment:         { file: comments.gq, view: { change: all } }             # match the document as the caller, insert with reach
  edit_own_comment:    { file: comments.gq, view: { read: all, change: all } }  # the body carries author = $subject
  drop_box:            { file: intake.gq,   view: all }                         # a body that only inserts discloses nothing
```

```gq
query global_ticket_count() {
  match { $t: Ticket }
  return { count($t) as tickets }
}

query add_comment($id: String, $doc: String, $body: String) {
  insert Comment { id: $id, body: $body, author: $subject }
  insert OnDoc { from: $id, to: $doc }    // the endpoint check on $doc is a read, under the caller's view;
                                          // the comment's placement is a write, under all
}

query edit_own_comment($id: String, $body: String) {
  update Comment set { body: $body } where id = $id and author = $subject
}
```

`read: all` lets the body see every row; `change: all` lets it write
wherever it writes, with the editor requirement met everywhere. An action
under `caller` is exactly the caller's own: `add_comment` cannot comment on
a document its caller cannot see, because the new edge's endpoint check is
a read under the caller's view. `edit_own_comment` edits exactly the caller's own comments
wherever they are, because `$subject` is the caller; a document editor's
ordinary `change` edits any comment in documents they can edit. Two
different rights, granted separately.

`$subject` is the key of the principal row of the person the request acts
for (the caller itself without delegation; null when the actor has no
principal row, which refuses a body that names it). It is a constant per
invocation, like `now()`, so it may appear in a `where` and in an
assignment value. It is the only caller-derived value a stored query may
name, because the engine already holds it. Every other fact about a caller, its tenant, its groups, its
clearance, is a row or a group and is reached through membership, never
through a trusted attribute: an attribute that decides access *is* an
audience, so it is a group or a securable.

**Invocation is by name.** In a version 2 bundle a rule granting
`invoke_query` lists the stored queries its actors may invoke, and takes a
`branch_scope` like `read` and `change`:

```yaml
rules:
  - id: reporting
    allow: { actors: { group: reporting }, actions: [invoke_query], queries: [global_ticket_count], branch_scope: any }
  - id: staff-comment
    allow: { actors: { group: staff }, actions: [invoke_query], queries: [add_comment, edit_own_comment], branch_scope: any }
```

That rule is the whole gate. The caller needs neither `read` nor `change`:
the name is the grant, and the view bounds the data. `queries: all` names
every stored query and is written explicitly. A caller without the grant
gets the same answer as for a query that does not exist. Ad hoc query text
still needs `read` or `change`; the difference between the two doors is who
wrote the text, which is why `view` exists only on stored queries and rules.

Rules for a stored query that declares `all` for any action:

- **Deployed under authority.** Deploying or redeploying it changes who can
  reach the data its view admits. Besides `config_manage`, the deployer must
  hold `view: all` for each such action under the applied policy. The
  deployment records the deployer and the authorizing policy digest.
- **Bounded parameters.** Every parameter has a bounded type: an enum, a
  number with a `@range`, or a string with a length limit. The deployer can
  then enumerate what the body can disclose. Repeated parameterized calls
  over a view the caller cannot otherwise read can isolate individuals, so
  every invocation is an audit event with its parameters; minimum group
  sizes do not prevent differencing, and per-recipient privacy budgets are
  later work.
- **Terminal.** Its result cannot be joined with the caller's view inside one
  query. That would reintroduce the rows the view excludes into a query the
  caller wrote, which is the hole the view exists to close.
- **Activation.** Queries, bindings and policy are one candidate deployment;
  a dangling name prevents activation. A request already admitted finishes
  under its captured bindings; later requests use the new ones.

The result's `restricted` reports the view each action ran under, so a
caller of `global_ticket_count` knows the count is whole.
`GET /graphs/{id}/queries` lists, under the caller's view, the stored
queries it may invoke that typecheck against its schema view, with their
parameter types. `GET /graphs/{id}/access` reports the view and the names.

### Agents acting for people: delegation

A delegated credential names the ***subject***, the person, and the
***actor***, the agent, with an expiry, and nothing else. The request's view
is `view(subject) ∩ view(actor)`, where each side's view is the one the
graph and its rules give it; Cedar must allow the action for both
identities; commit receipts and audit events record both; `$subject` is the
subject's principal. Every composition is an intersection, so a chain of
delegations only narrows. Keeping content away from an agent needs no
special construct: the agent is a principal, so grant it less, or put its
team behind a `@deny` edge.

### Sharing outward

A recipient is a principal that no group inside the company contains; a
share is whatever its view admits. Trusted collaborators may use that view
live. Outside parties get the view materialized into a separate graph, kept
current or delivered once, because live access exposes side channels
(timing, resource limits) an outsider is not trusted with; the
materialization and its reconciler are a later phase and belong to the actor
views draft. Compartments where a defect in view enforcement would be
unacceptable, where retention differs, or where the threat includes holders
of storage credentials get their own graph or cluster: securables are for
the long tail of channels, folders, deals and matters, separate graphs for
the few compartments that justify their own infrastructure.

### Changing access safely

Three rules make a revocation mean something.

**Access facts live on `main` only.** A branch forked before a revocation
holds the old edges; if those could be written on a branch and merged back,
the merge would revive the access. So access edges and principal values are
written on `main`, and a branch write, merge, promotion or restore that
touches them is refused. Review of an access change goes through an
unpublished graph commit on `main`
([Unpublished graph commits](2026-10-10-unpublished-graph-commits.md)): the
proposal captures the exact head it was written against, a reviewer reads it
as a snapshot, publication re-checks every change for the publishing actor,
and an access change published in between expires the proposal.

**Authorization is never older than the data.** The ***authorization
state*** of a request is the applied policy digest, the applied stored-query
bindings' epoch, and the published versions of every ***access table*** on
`main`: the five access edge types and the principal type. Placement edges
and securable rows are not access tables; a document write does not move the
authorization state. It is not `main`'s head commit, which every unrelated
write advances. A read on `main` takes the authorization state and the data
from one catalog image. A read on another branch or a historical target
captures the data first and `main`'s authorization state after it, so any
revocation published before the data cut is applied. A write on `main`
captures the state with its baselines, and the access tables' versions
become expected versions of the publication's compare-and-swap, so a
concurrent access change by any process fails the publication and the
attempt retries with fresh authority.

**One window is stated, not closed.** A write on a branch other than `main`
publishes on that branch's chain, which `main`'s access tables are not on.
Lance offers no compare-and-swap across two chains, and a process-local lock
would fence nothing across processes. Such a write is therefore authorized
against the state its attempt captured: a revocation on `main` during that
one attempt can miss it, bounded by the write deadline, landing rows only on
that branch, which every later read and the eventual merge re-check. This
RFC states the window rather than pretending to close it.

### Limits

Cluster configuration bounds a view per graph: the membership closure by
`max_memberships` and a depth of 16, the readable set by `max_securables`,
an exact search by `max_exact_search_rows`. Exceeding a bound refuses with
`view_limit_exceeded`; the engine never truncates a view or a search. `|R|`
is the number of distinct securables an actor can read: small under
containment, up to 10^5 under item-level control for a heavy user. A second
physical encoding for that regime (reader groups stored on the row, filtered
by overlap with the actor's closure) is evidence-gated, not designed; this
RFC commits to one encoding. A stored query with `view: all` adds no per-row
cost.

## Evaluating the design

### Does it express what companies need?

The standard catalogue, against the design. ✓ is direct; ◐ is a named
modeling step; ✗ is declined with a fallback.

| | Pattern | | How |
|---|---|---|---|
| 1 | Tenant isolation | ✓ | `InTenant @in @card(1..1)`; a tenant is a group and a securable, so its members read it with no grant; one `@grant` edge at `editor` for its writers |
| 2 | Folder and org hierarchies | ✓ | An `@inherits` chain; the edge's role caps what flows down |
| 3 | Groups in groups | ✓ | The `@member` closure |
| 4 | Sharing one item with one person | ✓ | A self-placed type with a `@grant` from the principal (item-level control), or share its container |
| 5 | Ethical walls, blocklists, keeping agents out | ✓ | `@deny`; absolute for the subtree |
| 6 | Temporary access, embargoes | ✓ | `@starts` / `@expires`, exact at the instant |
| 7 | Need-to-know on several axes | ✓ | Several `@in` edge types on one node type, intersected at the row |
| 8 | Derived content visible to readers of all its sources | ✓ | A self-placed row with one `@inherits @requires` edge per source |
| 9 | Hiding a column | ✓ | `@in(Type, key)` on the property; absent from the schema view |
| 10 | Private rows | ✓ | An `@in` edge to `$subject`; you own your own securable; nothing is visible by omission |
| 11 | Read, write, administer as separate rights | ✓ | `viewer < editor < owner` |
| 12 | Staff versus outsiders | ✓ | A policy group defined from `Team:staff`; who may add to it is whoever owns the team, and `read` itself is policy |
| 13 | Delegation that only narrows | ✓ | `view(subject) ∩ view(actor)` |
| 14 | Context conditions (MFA, network, time of day) | ✓ | Cedar context on the action; the view is context-free |
| 15 | Computation rights: count but not list, run this tool | ✓ | A stored query with `view: { read: all }`, granted by name |
| 16 | A rule over the row's own columns (published is public) | ◐ | An `@inherits` edge to the public securable, written by an owner or through a stored mutation that keeps the column and the edge together |
| 17 | External sharing with revocation | ✓ | A recipient's view, materialized |
| 18 | A relationship confers access | ◐ | Annotate the relationship's edge type `@grant` or `@inherits`; the edge must run subject → securable, so a relationship stored the other way round is declared the other way round or mirrored by a connector |
| 19 | A rule over other rows (notes that mention my accounts; visible after three approvals) | ◐ | A stored query with `view`; the caller gets answers |
| 20 | Custom roles (commenter, approver) | ◐ | A stored mutation with `view: { change: all }` per right, matching under the caller's view |
| 21 | Ownership writes (edit your own comment) | ◐ | Self-placed comments with an `@inherits` edge to the document and an `@grant` at `owner` from the author, when comments are few; a stored mutation with `author = $subject` when they are many |
| 22 | A column rule that varies by row | ◐ | A self-placed row per subject (a compensation file per person) with `@inherits` edges to the person and to the HR root |
| 23 | Placement on edges | ◐ | An edge is visible when both endpoints are; a relationship that needs its own audience is reified as a node |
| 24 | Branch-scoped and historical access | ◐ | `branch_scope` on rules, as today; history binds in a later phase |
| 25 | Aggregates with a privacy guarantee | ◐ | Bounded parameters and audited invocations; budgets are later work |
| 26 | Masking (last four digits) | ✗ | A column is present or absent; store the masked form as its own placed column, or deploy a stored query |
| 27 | Write without read | ◐ | A stored mutation with `view: all` whose body only inserts |

The frame: Zanzibar expresses union, intersection, exclusion and
tuple-to-userset over relations an operator names per type, stored beside
the data; the view algebra is that with a fixed vocabulary, and the
relations are the graph's own edges. SQL row-level security accepts any
predicate and returns no proof and no index. Cedar decides actions with
context and never filters rows. This design takes the closed algebra for
data and gets predicates back only as stored queries, which return answers.

### Does it have the fewest primitives that compose?

Applying G11 to every construct:

| Construct | Required by | Derivable from another? |
|---|---|---|
| `@principal` | identity | No: which property holds the actor id cannot be inferred. |
| `@member` | closure | No: the only chaining relation. A group needs no mark: it is whatever `@member` targets. |
| `@in` | placement | No. One meaning in three places; a securable needs no mark: it is whatever `@in`, a grant, an inheritance, a requirement, a denial or a property targets. |
| `@grant` | union over groups | No: one hop, with a role. |
| `@inherits` | union over securables | No: a tree of grants without it is a grant per node per subject. |
| `@requires` | intersection at variable arity | No: several `@in` give fixed arity only. |
| `@deny` | difference | No: unions and intersections cannot remove anyone. |
| `@starts` / `@expires` | time | No: a deleting job would be a queue and a lag. |
| `@card` | required or optional placement | Existing; nothing new. |
| `view` | bypass, answers above access | No, and it has one meaning everywhere: the view an action runs under. |
| `queries:` on a rule | granting a stored query to a caller | No: the graph-wide action cannot name one query. |
| `members_of` in a policy group | actor sets at scale | No: an explicit list of 10^4 ids is not maintainable; the coupling is read-only. |
| `$subject` | ownership rules, private rows | No: the only caller fact the engine holds that is not a row. |

Each property of the system has one place it is changed: who is in a group
(a `@member` edge), where a row is (its `@in` edge), who may read a
securable (a `@grant`, an `@inherits`, a `@deny`), which actions an actor
may perform (a rule), whether an action runs past the caller (`view`),
which queries a caller may invoke (`queries:`). No rule in the design
exists only to choose between two mechanisms: a wider view comes only from
`view: all`, a narrower one only from the graph.

### What it costs

| Choice | Gained | Given up | Why acceptable |
|---|---|---|---|
| Edges, not predicates, for data | Composition under any operator; a proof; an indexed read | Row rules that mention other rows or the caller | Those are stored queries; a company brain's audiences are its source ACLs' audiences, which are edges already |
| Placement as an edge with a derived column | One way to reference a node; endpoint, cardinality and cascade for free; traversable | A system column the engine maintains in the same publication | The same shape as `__src` and `__dst`; `repair` verifies it; it is never read as authority outside the filter |
| Three roles | Inheritance caps are well defined; write implies read; source roles map one to one | Commenter, approver and other action-shaped roles | Each is a stored mutation with `view`; a per-type write floor stays available as the one chain-preserving extension if free-form writes at `viewer` are ever needed |
| Deny is absolute for a subtree | Walls mean what they say; one spelling | "Deny the folder but allow this one file" | That shape is a modeling error in a wall; restructure instead |
| Item-level control is self-placement | Drive's model, expressed with the same six annotations | `R` holds items, bounded by `max_securables`; creation happens on `main` | Containment serves the long tail; the regime past the bound is named, not improvised |
| `view` on a stored query | Approved answers whose reach is fixed by the query's text | A second place, besides the actor's grants, that defines data reach | Deployed only by someone who holds that view; bounded parameters and audited invocations bound differencing |
| Graph-defined policy groups | No actor lists to maintain; membership changes move policy at once | A read-only coupling from policy to the graph | The rules that change authority stay explicit, so the roots of trust stay in policy |
| No explicit default placement | An agent reading a schema knows exactly where each row goes | Writing an `@in` edge to `$subject` for private rows | Nothing becomes visible by omission either way; explicitness wins |
| Access edges `main`-only | No revival of revoked access through a fork | Branch review for access changes; self-placed rows created on `main` | Proposals give review, with expiry on a moved head |
| `$subject` as the only caller binding | No trusted-attribute mechanism to deploy and audit | Attribute rules that are not rows | An attribute that decides access is an audience, so it is a group or a securable |
| A new access dimension is a schema change | Placement edges are typed, indexed and part of the contract | Serialization behind the schema gate | A dimension is schema; live-branch schema apply is a separate proposal |
| No narrowing declaration | One way to narrow: the graph | "This agent reads tickets only" as a policy line | The agent is a principal; grant it tickets and nothing else |

### What it declines, and the fallback

Masking and value transformation (deploy a stored query, or store the
masked form); a fourth role (a stored query, or a securable per right); a
column rule that varies by row (a securable per row); joining a stored
query's answer into the caller's view inside one query (the hole the view
exists to close); operator allowlists (a resource policy, out of scope);
relaxing the schema change for a new dimension; a second physical encoding
past `max_securables` (evidence-gated); placement of an edge (reify it).

### Composition properties

- Closed under union and intersection; difference by subject only, in one
  layer; monotone after denial; unique fixed point; cycles inert.
- Orthogonal dimensions compose at the row without a securable per
  combination.
- Delegation composes by intersection; chains never widen.
- Derived content composes as an information-flow lattice.
- `view: all` is the one widening, and it is held by a rule the root of
  trust wrote or by a stored query deployed by someone who already held that
  view.

## Alternatives considered

Each option from [The design space](#the-design-space), with the reason it
was not chosen as the whole answer, and then the shapes this RFC itself
went through.

**Views alone.** Everything a company brain needs, and no answer above
access except by letting a stored query run with its author's view. Then a
tool's disclosure changes when a person's access changes, withdrawing a
person's access can silently break every tool they wrote, and a caller still
needs `read` to invoke anything. `view` on the query, deployed by someone
who holds that view, replaces "whoever wrote it" with an explicit,
authorized declaration.

**Stored queries alone.** Everything a tool needs, and no data authority a
query can rely on without restating it. Audiences become attributes compared
by equality: groups, inheritance, walls, time windows and several audiences
on one row have no form; a connector mirroring Drive has nothing to write;
traversal, search and writes each need their own proof rules; a refusal
tells a person to rewrite their question. The company brain is unserved.

**Predicates as the data authority.** Strictly more expressive than edges,
and rejected because it composes only by re-proving under every operator,
has no index form beyond what the planner can push, offers no
non-interference proof (an aggregate over a predicate-filtered scan leaks
through `count`, ordering and error paths unless each is argued), and makes
the planner reason per predicate. Translating Cedar predicates into plan
filters has the same shape and adds a second expression semantics.

**Refusal for everything.** Refuse any query not provably answerable from
the caller's authority, in the Non-Truman model of Rizvi, Mendelzon,
Sudarshan and Roy (2004). That needs query-equivalence reasoning. We will not
carry a solver, and a person asking a question should get the answer their
view admits, with the signal, not a request to rewrite.

**Relations with rewrites.** Custom roles at the cost of the chain
(inheritance caps, write implies read, one-to-one source roles, listable
effective access). The only pattern the chain cannot express is an
action-shaped right, which is a stored query. A per-type write floor is the
one extension that would keep the chain; it is held, not adopted.

**Explicit-beats-inherited denial.** NTFS lets a direct allow on a child
beat an inherited deny from its parent. It is the behavior "nothing flows
through a denied securable" gives by accident, and a wall that a direct
share can breach is not a wall. AWS's explicit deny, which no allow
overrides, is what walls expect.

**Separate graphs for everything.** The right answer for compartments that
must not share an engine, and the wrong one for a company brain, whose
audiences overlap.

**Doing nothing.** Graph × branch × action, with everything finer in
application code, where neither non-interference nor fixed disclosure can be
shown. The situations above are not served.

The shapes this RFC had and dropped, each because it failed G11 in one of
three ways: a new noun for an old thing with one attribute; a parallel
mechanism justified by a rule that arbitrates between the two; or a
construct kept from a parent draft because the parent had it.

**A capability object beside stored queries.** The first revision had one: a
named binding of a query, a scope and a contract, with its own routes and
configuration. It was a stored query with two attributes and a new noun.
Per-name `invoke_query` and `view` are the attributes; the noun, the
routes, the config block and three error codes were removed.

**Contracts for submitted queries.** A caller submits new query text under a
contract that lists allowed types, fields and operators, and the engine
checks the shape. Once the view is the data authority the contract proves
nothing about rows; what remained was a type allowlist, which is the graph's
job, and operator limits, which are cost control. Removed.

**Placement as a column holding a key.** The second revision stored a
document's folder as a string column, `folder: String`, and placed the row
with `@in(folder)`. The reason given was physical: the filter must be
answerable from the row's own table. That reason is right and the
conclusion was wrong: a foreign key the engine does not know about is a
second way to reference a node, with no endpoint check, no cascade and no
traversal, and the physical need is met by a derived column the engine
maintains. The same goes for a reference-typed property (`folder: Folder`),
which would put a node into a property value and break the property graph
model, and for a `@ref(Folder)` constraint on a string, which would make a
key column with a validated foreign key: still a second way to say what an
edge says.

**A declared `@securable`.** The second revision marked securable types,
because placement values were strings and the key space had to be known.
With placement as an edge the target type is the edge type's declared
target, and a securable is derivable: the targets of the access edge types.
Same for `@group`, derived in the second revision already. Removed.

**Computed placement.** `audience: String @computed(status == "published"
? "public" : folder)`, a column the engine derives from the row on every
write. It was the column model's way to say "published documents are
public". Under edges that rule is an `@inherits` edge written by an owner,
which is also the right authority for changing who can see a document; the
expression language, its evaluation point in the write path and its rewrite
at schema apply are gone.

**Grants from the policy bundle, and the bridge row.** A list of
`(group, role, securable)` entries in policy, beside `@grant` edges in the
graph, with "rate of change" deciding the home, was the first shape: a rule
whose job is to arbitrate between two mechanisms. The second revision
replaced it with a bridge row, `policy:<name>`, a graph node whose members
were the policy group's actors, so that grants had one mechanism. That is a
noun for a group that already exists on the other side. The third revision
turns the coupling round: policy groups are defined from graph groups, and
the only lists that stay explicit are the roots of trust.

**A narrowing `over`.** The second revision let a rule, a credential or a
stored query intersect the view with listed securables, types and
branches. It was the second way to narrow a view, beside the graph, with a
rule for what happens when two rules disagree. The graph narrows: an agent
is a principal, and a branch is `branch_scope`. Removed, with the credential
attachment point; a delegated credential now names a subject and an actor
and nothing else.

**The name `over`.** The first draft's capabilities ran "over" a subgraph;
the second revision removed the capability and kept its preposition, which
told a reader neither the value set nor the default. The declaration
selects a view, so it is named `view`.

**A default placement for inserts.** Rows without a placement going to the
writer's own securable is convenient and safe, and it is a second, implicit
way a row gets placed. An agent reading the schema should see exactly where
each row goes; an `@in` edge to `$subject` makes the private case one
explicit edge.

### Relationship to the earlier drafts

Two drafts preceded this one and each contributed to it.

[Actor views and subgraph sharing](https://github.com/ModernRelay/omnigraph/pull/951)
(PR #951) defines the view: the vocabulary, the algebra, placement, the
engine's enforcement, restricted search, writes, delegation, sharing and
vaults, with the Lance evidence behind each. This RFC adopts it as the data
authority and changes these things: placement is an edge type marked `@in`,
with a derived placement column where it had a key column; securables and
groups are derived from the edge types, not declared; denial closes
downward over inheritance; a principal owns its own securable; the `views`
block of its policy bundle (internal, bypass, grants, limits) is replaced by
graph-defined policy groups, `view` on rules and cluster-configured limits;
computed placement is dropped for an inheritance edge; the default placement
of inserts is replaced by an explicit edge to `$subject`; its definer-rights
stored queries are replaced by `view` on a stored query; the `view` result
object becomes one field. Its Lance citations are re-anchored from 11.0.0 to
13.0.0; every fact holds.

[GQP query contracts and Cedar grants](https://github.com/ModernRelay/omnigraph/pull/1197)
(PR #1197) proposed granting computation by name and fixing a query's text
as the unit of disclosure. Both survive, as per-name `invoke_query` and
`view` on a stored query. Its contract language, guard-preservation proofs,
per-operator obligations, single-binding fragment, trusted attributes,
submitted mode and capability binding do not: the view carries the proof,
and the rest was either the graph's job or cost control.

Both remain drafts; this RFC supersedes neither. Each is amended as above and
cites this RFC for the division.

## Invariants

- **5, stable identity survives renames.** Securables are nodes; their
  identity is the node's, and a placement is an edge whose endpoints the
  engine already keeps stable through renames.
- **7, physical acceleration is derived.** The row filter is a conjunct the
  plan declares over the placement column; the column is derived from the
  placement edge, written in the same publication, verified by `repair`,
  and never read as authority outside the filter. An index serves the
  conjunct or a scan does, and a missing index changes cost only.
  Restricted search's statistics are declared plan state.
- **8, integrity failures are loud.** A placement column that disagrees
  with its edge table is integrity drift that refuses reads under a view on
  that type; it is never reconciled silently in either direction.
- **9, query semantics are typed structures.** The view is a typed plan
  leaf; `view` is a typed binding in the accepted execution object; none
  passes through a string, a transport flag or a side table.
- **10, trust at the boundary, enforced at the engine.** Bearer tokens
  resolve actors; the view is computed from that identity and the applied
  bindings; every mutating engine entry point applies the write rules under
  the request's view, so embedded and CLI-direct writers bypass nothing.
- **11, bounded and observable.** Every bound refuses with a registered
  outcome; no partial result exists.
- **12, one source of truth.** Access edges and placement edges are graph
  data; the roots of trust are policy; `view` is deployment configuration;
  a view is derived per request and cached only per authorization state.
  The placement column is derived state that no published graph can hold
  without its edge. Nothing shadows anything.

Deny-list items touched: the placement column is checked against "a logical
precondition based on physical index coverage" and "maintained parallel
truth". It is neither: the logical precondition is the edge, the column has
no coverage to be missing because it is written in the edge's publication,
and a disagreement is an integrity failure, not a fallback. The branch-write
window is stated, not fenced by a process-local lock; no queue, no inline
rebuild, no string-built semantics, no cloud-only path.

## Compatibility and reversibility

- **Schema.** The view declarations set the schema feature `actor-views`; a
  build without this RFC refuses to open such a graph. A schema apply that
  adds or removes `@in` on an edge type backfills or drops the placement
  column in its single publication.
- **Policy.** Bundle version 2: `groups` entries may be `members_of`; rules
  gain `view` and `queries:`; `invoke_query` rules list their queries and
  take `branch_scope`. A version 1 bundle keeps today's meaning, graph-wide
  `invoke_query` double-gated on `read` or `change` included, so nothing
  deployed changes until a bundle says `version: 2`. `view: all` on a graph
  without placements changes nothing, which is how the plumbing is proved
  before any view exists.
- **Configuration.** A stored query entry under `queries:` gains an optional
  `view`; limits are cluster configuration per graph.
- **Wire.** `restricted` on results, the new fields of
  `GET /graphs/{id}/access`, parameter types on `GET /graphs/{id}/queries`,
  and three error codes (`view_write_denied`, `delete_blocked`,
  `view_limit_exceeded`), plus `view_restricted` while restricted search is
  a later stop than the row filter. A required placement that is missing is
  the existing `@card` refusal. No new route. Graphs without the feature and
  bundles at version 1 see no change.
- **Withdrawn surfaces.** Definer-rights stored queries, the GQP contract
  language, the capability object, the key-column placement and the
  narrowing `over`: none shipped, nothing deployed changes meaning.
- **Reversibility.** The division is a design rule, not a format; reverting
  it means choosing one half and losing the workload the other serves. The
  format decisions that are hard to reverse (the placement column, history
  binding) are owned by the actor views draft and gated by its evidence.

## Evidence and tests

The mechanism's owners are named by the actor views draft:
`policy_engine_chassis.rs`, `writes.rs`, the planner's `validate/tests.rs`,
`search.rs`, `lance_surface_guards.rs` and the server's `auth_policy` suite.
This RFC adds the evidence for the division itself:

- **The ledger as a corpus.** One `.gqt` case per row of the ledger under
  `crates/omnigraph-gqt/cases/access/`, each stating the pattern, the schema
  and policy that express it, and the rows each actor sees; ◐ rows show the
  modeling step and ✗ rows the fallback. The corpus is the proof of G10 and
  the regression guard for every later change to the algebra.
- **Denial closure.** A direct grant on a file inside a denied folder is
  hidden; a direct grant on a sibling outside it is not.
- **The placement column.** Every writer (mutate, load, merge, schema apply
  adding or removing `@in`) leaves it equal to the edge table; a DST
  invariant checks the equality after every operation; a column edited
  behind the engine's back makes `repair` report drift and a view read
  refuse.
- **Graph-defined policy groups.** A rule over `members_of` admits an actor
  the moment its `@member` edge is published and refuses it the moment the
  edge is deleted or expires; bundle validation refuses `members_of` on a
  rule with `view: all`, `schema_apply` or `config_manage`.
- **Self-placement.** A folder is invisible to a non-reader; creating a
  self-placed row without an edge that makes it editable by its creator is
  refused; the same mutation with the edge succeeds; on a branch it is
  refused.
- **`view`.** Server `auth_policy` and `stored_queries` cases: a stored query
  with `read: all` answered for a caller with no `read`; the same caller
  refused on an ordinary read and on an unlisted query, with the not-found
  answer; `add_comment` refused for a document outside the caller's view
  and accepted inside it; `edit_own_comment` editing only the subject's
  rows; a deployment refused when the deployer lacks `view: all` for an
  action marked `all`; a version 1 bundle behaving as today.
- **Replay.** `engine_v2_plan_replay.rs` gains a plan bound to a stored
  query's `view` whose replay refuses when the view digest or the subject
  changed.
- **Surfaces.** The codes join the `ErrorCode` registry; `openapi.json` is
  regenerated and the conventions test of
  [HTTP API conventions](2026-10-19-http-api-conventions.md) covers the
  changed responses.
- **Lance.** The actor views draft's anchors re-read at 13.0.0, one row per
  fact.

## Rollout

Each stop is independently safe.

1. **Shared foundation.** Engine read doors that take an actor; the typed
   view slot in plan acceptance; diagnostics that do not confirm hidden
   names; the view digest and `$subject` in replay bindings.
2. **Bundle version 2: per-name `invoke_query` and `view` on stored
   queries.** Invocation without `read`, bounded parameter types, deployment
   authority, listing. Needs no securables: on a graph without placements
   `view: all` behaves as today, which proves the plumbing. Query-only
   access for agent tools ships here.
3. **Type and column views.** The annotations, the view computation with
   property placements only, the schema view, graph-defined policy groups,
   denial closure, `view` on rules.
4. **Row views.** `@in` on edge and node types, the placement column, the
   row filter, masked traversal, restricted search, `restricted` on results.
5. **Writes under a view, then the later phases** as the actor views draft
   orders them: incremental maintenance, search acceleration, history and
   feeds, materialized shares.

`implementation` moves to `in-progress` at stop 1 and to `partial` at
stop 4; the actor views draft advances its own field as its phases land.

## Unresolved questions

1. **Re-authorization of `view` on a stored query.** Whether the deployer's
   `view: all` is re-checked at every bundle re-apply or only at deployment.
   Re-checking makes a tool fail when its deployer loses the rule, the
   liability definer queries had; not re-checking leaves a view standing
   after its authorizer has gone.
2. **Spellings.** `view`, `queries:` and `members_of` in the policy bundle,
   `view` on a stored query entry, and `@in(Type, key)` on a property are
   proposed here and need review against the policy schema, the cluster
   configuration schema and the schema grammar before acceptance.
3. **Self-placed rows on branches.** Creation is on `main` because the edge
   that makes a new securable reachable is an access fact. Whether a branch
   may hold a self-placed row that nobody but `view: all` can see until it
   is merged and granted is a usability question this RFC has not settled.

## Decision log

- **2026-10-30.** Drafted from first principles for contributors, from the
  comparison of PR #951 at `d700c334` and PR #1197 at `6d7217a1` against the
  code at `118161dd` and Lance 13.0.0.
- **2026-10-30, second revision.** Retitled from "Access authority: views and
  capabilities" to "Access authority: views and reach" after an audit
  against G11. Removed: the capability object, its routes, configuration and
  three error codes; the contract language and the submitted mode;
  `views.grants`, `views.internal` and `views.bypass`; `@group`; the
  input-scope and caveat vocabulary; `$actor` in stored queries; the default
  placement of inserts. Added: `over` on rules, credentials and stored
  queries; per-name `invoke_query`; the bridge row; G11 and the primitive
  audit.
- **2026-11-01, third revision.** Retitled to "Access authority: views in the
  graph" after an orthogonality review against G11. Placement is an edge
  type marked `@in`, with a derived placement column where the second
  revision had a key column; a node type marked `@in` is its own securable,
  which gives item-level control beside containment; securables are derived
  from the edge types, so `@securable` is gone; computed placement is gone,
  "published is public" being an `@inherits` edge; the bridge row and the
  internal group are gone, policy groups being definable from graph groups
  with explicit lists kept for the rules that change authority; `over` is
  renamed `view`, is per action with the values `all` and `caller`, lives
  on rules and stored queries only, and narrows nothing, so the credential
  attachment point and the narrowing form are gone; invocation of a stored
  query is gated by its name alone in a version 2 bundle; a principal owns
  its own securable; "program" and "capability" are no longer used, the
  thing being a stored query; the error codes are three plus one temporary;
  the ledger is re-run and the primitive audit, costs and alternatives
  updated.

## Appendix: implementation notes for agents

For whoever implements a stop of the rollout. Read the invariants and the
Lance guide first; this appendix says where each piece goes and what to
watch.

### Where each piece lives

| Piece | Crate and place | Notes |
|---|---|---|
| Schema annotations (`@principal`, `@member`, `@in`, `@grant`, `@inherits`, `@requires`, `@deny`, `@starts`, `@expires`) | `omnigraph-compiler` schema parsing and catalog; the `actor-views` schema feature | A type is a group iff a `@member` edge type targets it; a securable iff an access or placement edge type, a property placement or a node-level `@in` names it. `@in` on an edge type requires `@card` max 1. |
| The placement column | `crates/omnigraph/src/exec/staging.rs` (`commit_all`: the column write joins the edge write's publication), `crates/omnigraph/src/exec/mutation.rs`, the load path, schema apply (backfill and drop in one publication), `repair` | A system column per `@in` edge type on the source table, with a BTREE index declaration. Self-placed types use their key. Never exposed in the schema. |
| View computation (`C0`, `D`, `C`, the fixed point) | `omnigraph-engine`, a new `engine/view` module reading the access tables of the captured authorization state | Policy groups with `members_of` resolve from the same closure. Cache per (authorization state, actor); invalidate at the earliest `@starts`/`@expires` boundary. Bounds from cluster configuration. |
| `view` | `omnigraph-policy` (rule field; `all` only with explicit groups), `omnigraph-cluster` (stored query entry, bundle validation), the engine's accepted execution object | One type, two attachment points, per action. On a rule: `all` if any matching rule says so. On a stored query: the entry's map, `caller` for an omitted action. |
| `$subject` | `omnigraph-compiler` (a reserved constant, typed `String?`, accepted wherever `now()` is), bound once per invocation from the request | The principal row's key, not the actor id. |
| Authorization state capture | `crates/omnigraph/src/db/omnigraph.rs` beside `load_accepted_view_with_schema_gate_held` | Reads on `main`: one catalog image. Other targets: data first, then `main`'s access pins. Writes on `main`: access pins as expected versions of the `__manifest` compare-and-swap. Access tables: the five access edge types and the principal type. |
| Schema view | The compiler's catalog construction, parameterized by the readable set | Hidden names must produce the same diagnostic bytes as undeclared ones; test by diffing error payloads. |
| Row filter (`ViewSet` leaf) | `omnigraph-planner`: a leaf in the IR and a check in `crates/omnigraph-planner/src/validate/invariants.rs` (`Requirements::check`) that every placed scan carries it; `AcceptInput` in `validate/mod.rs` takes the view | The leaf carries the view's digest, never the list; the list is bound at execution. Replay compares the digest. |
| Scan pushdown | `crates/omnigraph/src/engine/scan.rs` | The conjunct becomes an `IN` list on the placement column (or the key); Lance pushes it through `visit_in_list` to the BTREE/bitmap index. |
| Restricted search | `crates/omnigraph/src/engine/scan.rs` (the prefilter decision at line 268 and `Eligibility` in `crates/omnigraph-planner/src/optimizer.rs`) and the search ladder in `engine/search.rs` | bm25: `FlatMatchQueryExec` with a scorer built from visible-row statistics; `nearest`: exact over visible vectors (`use_index(false)` path). Statistics are declared plan state. |
| Write rules | `crates/omnigraph/src/exec/mutation.rs` (`resolve_assignments`), `crates/omnigraph/src/validate/` (kernels such as `card_violations`), `crates/omnigraph/src/exec/staging.rs` (`commit_all`) | Evaluate placement from the staged edges after assignments and before validation. Creation of a self-placed row is checked against the staged state: the creator must hold `editor` after the mutation. Constraint disclosures are the only facts a refusal may carry; audit every message. |
| Per-name `invoke_query` | `omnigraph-policy` (Cedar schema: the query as a resource; bundle version 2), `crates/omnigraph-server/src/handlers.rs` beside `server_invoke_query` (today double-gated on `invoke_query` plus the body's `read`/`change`) | Under version 2 the name is the only gate. Unlisted and unknown names answer alike. Version 1 keeps the double gate. |
| `access` route | `GET /graphs/{graph_id}/access` exists (operation `access`) and today evaluates policy per action | Add memberships, resolved policy groups, readable and denied securables with roles, and the names invocable. |
| Error codes | The `ErrorCode` registry with HTTP status and `disposition` | Three codes plus the temporary `view_restricted`; all `effects: none`, `action: fix`. |
| Delegation | The delegated-credential work (ModernRelay/omnigraph#978): subject, actor, expiry | Cedar evaluated for both identities; receipts record both; no narrowing field. |
| Materialized shares | Later phase; the actor views draft's reconciler | Not in the first stops. |

### Order of work, concretely

1. Thread the actor through the engine read doors and add the view slot to
   `AcceptInput`; make acceptance refuse any plan shape that could drop a
   filter on a placed scan (today no scan is placed, so the check is
   vacuous but present). Add the view digest and `$subject` to the replay
   envelope.
2. Bundle version 2: the query as a Cedar resource; `queries:` on rules;
   `view` on rules and stored query entries; the handler gated on the name
   alone; bounded parameter types; deployment authority. Test on a graph
   with no `actor-views` declarations: `view: all` and no `view` behave the
   same, which proves the plumbing before any view exists.
3. Schema annotations, the view computation with property placements only
   (no `@in` on edge or node types yet), graph-defined policy groups, the
   schema view, denial closure.
4. `@in` on edge and node types, the placement column and its `repair`
   check, the `ViewSet` leaf, pushdown, traversal masking, restricted
   search, `restricted` on results.
5. Write rules, constraint disclosures, access-edge rules, self-placed
   creation, proposals for access changes.

### Substrate facts to rely on (Lance 13.0.0)

- A filter expression on a scan: `rust/lance/src/dataset/scanner.rs:1776`
  (`filter_expr`); `prefilter` defaults as at `scanner.rs:1437`; the
  effective filter plan at `scanner.rs:3627`.
- `IN` lists reach scalar indexes: `rust/lance-index/src/scalar/expression.rs:212`
  (`visit_in_list`); BTREE `IsIn` at `scalar/btree.rs:2509`; bitmap at
  `scalar/bitmap.rs:846`; label list at `scalar/label_list.rs:165`.
- A filtered read skips fragments the index excludes:
  `rust/lance/src/dataset/scanner/filtered_read.rs:836`; count pushdown at
  `count_pushdown.rs:228`; a fragment's live count at `fragment.rs:1624`.
- Full-text: `FlatMatchQueryExec::new` at `rust/lance/src/io/exec/fts.rs:4114`,
  `with_base_scorer` at `fts.rs:4223`; the flat scorer counts only rows that
  hold the token, `rust/lance-index/src/scalar/inverted/flat_search.rs:373`
  and `:510`; scorer initialization at `flat_search.rs:756`; the stream with
  a supplied scorer at `flat_search.rs:1074`. This is why restricted bm25
  supplies statistics over visible rows instead of trusting a prefilter.
- Vector: IVF partition selection at `rust/lance-index/src/vector/ivf/v2.rs:2197`;
  KNN stops at `k` at `rust/lance/src/io/exec/knn.rs:1967`. This is why
  restricted `nearest` is exact over visible vectors in the first scope.
- Merge-insert `UpdateIf` at `rust/lance/src/dataset/write/merge_insert.rs:502`;
  stable-row-id compaction skips remap at `optimize.rs:847`. The placement
  column is an ordinary column of the source table, so compaction and
  schema evolution carry it like any other.

Re-read every page the Lance guide lists for scalar indexes, full-text and
vector search before changing the scan or search paths; the line numbers
above are anchors, not substitutes.

### Test owners to extend

| Behavior | Owner |
|---|---|
| Policy gates on engine doors | `crates/omnigraph/tests/policy_engine_chassis.rs` |
| Write rules, disclosures, access edges, the placement column | `crates/omnigraph/tests/writes.rs`, `tests/integration/validators.rs`, `tests/integration/maintenance.rs` (`repair`) |
| Schema annotations, backfill and drop at apply | `crates/omnigraph/tests/schema_apply.rs` |
| Plan acceptance refusals, the `ViewSet` leaf | `crates/omnigraph-planner/src/validate/tests.rs` |
| Replay with bindings | `crates/omnigraph/tests/integration/engine_v2_plan_replay.rs` |
| Restricted search | `crates/omnigraph/tests/search.rs`; substrate fences in `tests/lance_surface_guards.rs` |
| Query results under a view, the ledger | `crates/omnigraph-gqt/cases/access/` (new directory; one case per ledger row) |
| The placement column equals its edge table after every operation | the DST crate's invariant checks, `crates/omnigraph-dst/tests/scenarios.rs` |
| Routes, Cedar, per-name invocation, `view`, bundle versions | `crates/omnigraph-server/tests/integration/auth_policy.rs`, `stored_queries.rs`, `openapi.rs` |
| Bundle parsing, `members_of`, stored query entries, activation | `omnigraph-policy` in-source tests; `omnigraph-cluster` in-source `tests.rs` and `admission.rs` |
| MCP parity | `crates/omnigraph-server/tests/integration/mcp.rs` |
| CLI surfaces | `crates/omnigraph-cli/tests/integration/cli_queries.rs`, `cli_cluster.rs` |

### Gotchas

- **Byte-identical diagnostics.** A hidden name and an undeclared name must
  produce the same error, disposition included. Build the schema view before
  the compiler sees the query; do not filter diagnostics afterwards.
- **`matched_rule_id` is diagnostic.** It is `.min()` over matched ids in
  `crates/omnigraph-policy/src/lib.rs`. Never select a view or a group by
  it; the request runs under `all` if any matching rule says so.
- **Capture order.** Authorization must never be older than the data it
  protects. On `main`, one catalog image. Elsewhere, data first, then
  `main`'s access pins. Writes on `main` pin the access tables in the
  compare-and-swap. Do not "fix" the branch-write window with a lock.
- **The digest, not the list.** The plan leaf carries the view's digest; the
  list is bound at execution. A plan replayed under another view must refuse
  on the digest.
- **The placement column is written with the edge, never after it.** One
  publication for both; a crash between them does not exist because there
  is no between. A backfill at schema apply is the apply's single
  publication. `repair` reports a difference; nothing reconciles it.
- **Self-placed types use the key.** No column; the filter is `key IN R`
  restricted to that type.
- **`view` on a stored query is per action and replaces the caller's view
  for that action.** `read: caller` with `change: all` is the common shape:
  match as the caller, write with reach. An endpoint check on a new edge is
  a read.
- **`view: all` and `members_of` never meet.** Bundle validation refuses a
  graph-defined group on a rule with `view: all`, `schema_apply` or
  `config_manage`.
- **Version 2 gates on the name alone.** The existing stored-query handler
  is double-gated; under version 2 neither `read` nor `change` is required
  of the caller. Version 1 bundles keep the double gate.
- **`$subject` is the principal row's key.** Not the actor id. Null when the
  actor has no principal row; a body that names it then refuses.
- **A principal owns its own securable.** Without that rule nobody can write
  their own notes.
- **bm25 statistics.** The index's statistics include hidden rows and the
  flat scorer counts over the prefilter's rows. Supply statistics over
  visible rows; never let a prefilter change a restricted score.
- **`IN` list size.** `max_securables` bounds it; refuse above, never
  truncate. Measure plan size and pushdown at 10^4 and 10^5 keys before
  setting the default.
- **Access facts on `main` only.** Every door: mutate, load, merge, branch
  promotion, restore. A proposal (`as commit`) targets `main` and is allowed.
  Placement edges are data and go anywhere; the merge rule checks them.
- **Replay bindings.** Subject, actor, view digest, the stored query's name
  and `view`. Refuse on any difference.
- **Generated contracts.** `openapi.json` regenerates through the server
  OpenAPI test (`OMNIGRAPH_UPDATE_OPENAPI=1` only when the drift is
  intentional); the HTTP conventions test must pass on the changed
  responses; the error registry owns codes.
- **DST storage-cost golden.** Capturing the authorization state reads the
  access tables' pins, and a placed write writes one more column; both are
  changes to what an operation reads or writes. Add labeled rows to
  `crates/omnigraph-dst/cost_table.txt` and explain each in the pull
  request.
- **Release notes.** Each user-visible step needs a `changelog.d` note.

### Reading list

[Actor views and subgraph sharing](https://github.com/ModernRelay/omnigraph/pull/951)
for the full mechanism of the view, with its evidence and its phases;
[GQP query contracts and Cedar grants](https://github.com/ModernRelay/omnigraph/pull/1197)
for the adversarial test plan behind per-name grants;
[Identity credentials and applied policy authorization](2026-09-09-identity-credentials-and-applied-policy.md)
for how actors are resolved;
[Graph access and cross-process writes](2026-10-16-graph-access-serving-and-cross-process-writes.md)
for the operation table, dispositions and the `access` route;
[Unpublished graph commits](2026-10-10-unpublished-graph-commits.md) for
proposals; the Lance guide's scalar index, full-text and vector pages.
