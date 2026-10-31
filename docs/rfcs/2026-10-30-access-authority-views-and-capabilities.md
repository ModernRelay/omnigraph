---
rfc: "2026-10-30-access-authority-views-and-capabilities"
title: "Access authority: views and reach"
track: maintainer
status: draft
implementation: not-started
authors:
  - ragnorc
created: 2026-10-30
updated: 2026-10-30
discussion: https://github.com/ModernRelay/omnigraph/pull/1210
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC: Access authority: views and reach

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

This RFC proposes one kind of access authority and one declaration.

- A ***view*** is the set of data a request may see and change. By default
  it is derived from labels on the data and from the actor's memberships,
  through a small fixed algebra, and the engine applies it to every query
  automatically. A query under a view runs as if the data outside the view
  did not exist.
- `over` is a declaration that a grant, a delegated credential or a stored
  query runs under a different view than the actor's own: a narrower one, or
  the whole graph. `over: all` on a stored query is how a caller runs an
  approved computation over data it cannot otherwise see; `over: all` on a
  grant is how a connector or an administrator sees everything.

The rule behind the design: **an audience is a label on the data; a rule
whose predicate mentions the caller is a program.** A view must hold for any
query, so it is a set with an algebra and a proof. A stored query with reach
returns an answer or performs a write and composes with nothing, so it may
carry predicates and needs no proof beyond its own identity and its declared
view.

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
every row of every type; an actor without it sees nothing. Stored queries add
one step, `invoke_query`, but a stored read still needs `read`, and
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
digest, a lookup. Omnigraph is a database for agents; producing, checking and
deploying a program is cheap for them, so authorization should be able to
attach to a program.

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
| G4 | **Answers above access.** A caller can be given an approved computation, an aggregate or a tool, without the data behind it, and the disclosure is fixed by the program, not by whoever wrote it. | Agent tools; cross-tenant reports. |
| G5 | **Explainable.** An actor's effective access can be listed, and no rule order decides it. | Operators must be able to answer "why can Bob see this?" |
| G6 | **Cheap at scale.** Every read under a rule is an index lookup, not a per-row evaluation; limits refuse rather than truncate. | 10^8 rows, 10^5 actors. |
| G7 | **Reviewable and revocable.** An access change can be reviewed before it takes effect; a revocation takes effect at once; stale state cannot revive revoked access. | Legal walls, offboarding. |
| G8 | **Shares outward.** A chosen part of the graph can be given to an outside party with update and revocation. | Clients, partners, auditors. |
| G9 | **Delegation narrows.** An agent acting for a person holds at most the person's access. | Agents everywhere. |
| G10 | **Expressive enough.** The standard catalogue of permission patterns (tenants, folders, groups in groups, per-item sharing, walls, expiring access, need-to-know on several axes, column hiding, private rows, derived content) is expressible, and what is not is named. | A design that handles the first three patterns and improvises the rest grows a second system beside it. |
| G11 | **Few primitives that compose.** Every construct is required by a goal, none is derivable from another, and no rule exists only to arbitrate between two mechanisms. Explicit enough that an agent can read the schema and the policy and know what it may do. | Each extra construct is a place to be wrong, and agents are the main users. |

### Constraints we inherit

- **The storage substrate.** Rows live in Lance datasets. A filter reaches
  Lance as an expression it pushes into an index; an `IN` list over an
  indexed column is cheap, an arbitrary predicate per row is a scan. Full-text
  search scores a document against statistics (how many documents, how often
  a term occurs) that Lance computes over the rows its prefilter admits, and
  the index's own statistics cover every row, hidden ones included. Vector
  search over an index visits partitions, not a filtered subset.
- **One acceptance door.** Every read plan is checked against its query
  before it runs (`crates/omnigraph-planner/src/validate/`), and a bound plan
  can be replayed later. Whatever access rule a plan obeys must be a typed
  part of the plan, checked there and retained through replay, or it can be
  dropped by an optimizer or a replay.
- **Cedar is the policy language.** Actors are resolved at the server;
  policy is a bundle applied by deployment; a rule's name is diagnostic and
  never selects authority. Policy is the root of trust: nothing a graph
  writer can change may alter who is in a policy group.
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
| **Labels on the data** (row- and column-level security: Oracle Label Security, Elasticsearch document security, SharePoint) | Each row carries a label naming its audience; actors belong to groups; the engine filters every query by the actor's labels. | G1 with a proof, G2, G3 (a label per source audience), G5, G6 (a label is an indexed column). | G4 on its own: an aggregate over hidden rows is silently wrong, and a tool cannot be given more reach than its caller. Row rules that mention other rows or the actor. |
| **Predicates per type** (Postgres row-level security, Oracle VPD) | A policy is any boolean expression over the row and the session; the planner adds it to every query on the type. | Expressiveness: ownership rules, attribute rules, joins. | G1 (no proof; `count`, ordering and errors leak unless each operator is argued), G6 (a predicate is not an index lookup), G5 (predicates compose by evaluation, not by listing). |
| **In the query** (named-query allowlists; query contracts with proofs; the Non-Truman model) | Only approved programs run, or every program must state its own scope and the engine proves it does not exceed it. | G4: the program is the disclosure. | G2 (a new question needs a proof; traversal, search and writes need proof rules per operator), G3 (audiences become attributes compared by equality, so groups, inheritance, walls and expiry have no form), G8. A refusal tells an agent to rewrite, which is right for a tool under construction and wrong for a person's question. |
| **Separate graphs per audience** | Each audience gets its own graph or cluster. | The strongest isolation: no shared index, no shared statistics, separate keys. | Overlapping audiences: a company brain would need a graph per channel. |
| **Relations with rewrites** (Zanzibar, SpiceDB, OpenFGA) | Per type, named relations (`viewer`, `editor`, `parent`) and rewrite rules (`viewer = direct viewer ∪ viewer of parent`); the engine resolves a check by walking relations. | Expressiveness including custom roles; a proven production model. | The same G4 gap as labels; custom relations cost the simple properties a fixed vocabulary gives (inheritance caps, "write implies read", a one-to-one mapping from source roles, a listable effective access without rewrite evaluation). |

Two things fall out of the table.

First, the options answer two different questions. Labels, predicates and
relations answer *what may this actor see?* Allowlists and contracts answer
*what may this caller run?* A tool that returns a global count has an answer
to the second question and no answer to the first: its caller sees no
tickets and yet receives a number computed over all of them. No amount of
row filtering produces that; no amount of program checking produces a
company brain. The goals need both answers, and a design that offers one
and improvises the other grows a second system in application code.

Second, among the answers to the first question, labels are the only one
that gives G1 with a proof, G3 and G6 at the same time. Predicates buy
expressiveness with the proof and the index; relations with rewrites buy
custom roles with the simple properties. What labels cannot express is
precisely the set of rules whose predicate mentions something other than
the row's own audience: the caller, another row, a count. Those are the
rules a program expresses naturally.

The second question needs much less machinery than it looks. "Run this
approved program over data you cannot see" is a stored query, which exists,
with two additions: it is granted by name, and it declares that it runs over
a view other than its caller's. That declaration, `over`, is also what a
bypass actor and a narrowed agent need. One declaration answers the second
question and two side needs; nothing else is required.

## The decision

**An audience is a label. A predicate that mentions the caller is a
program.**

- A view is computed from labels on rows and columns, memberships of actors,
  and a fixed algebra (grant, inherit, require, deny, time window). The
  engine applies it to every query as one filter and to the schema as a set
  of hidden names. Nothing about a query is proved; the view is proved once.
- `over` sets the view for a grant, a delegated credential or a stored query.
  A stored query with `over: all` is an approved computation over the whole
  graph, granted by name; its program is the disclosure. A grant with
  `over: all` is bypass. A narrowing `over` on any of the three is a caveat.
- Outsiders get copies: a share is whatever a recipient's view admits,
  materialized into a separate graph, because live access exposes side
  channels an outsider is not trusted with. Compartments that must not share
  an engine get their own graph.

Seven concrete decisions follow and are explained in
[The design, by example](#the-design-by-example):

1. Views use one fixed vocabulary and algebra; there is no role algebra and
   no predicate language on the data side.
2. A wall (deny) is absolute for everything beneath it.
3. Policy groups are subjects in the graph through one bridge, so every
   grant is a graph edge and policy stays the root of trust for who is in
   which set.
4. A label may be computed from a row's own columns on every write, so
   "published documents are public" is a declaration the engine keeps true.
5. `over` is the one way a request's view differs from the actor's own, on
   three attachment points; a stored query with reach is granted by name and
   deployed by someone whose own view covers it.
6. Access facts live on `main` only; review of an access change goes through
   an unpublished commit, never a branch.
7. What the line excludes is declined by name, each with its fallback.

## The design, by example

The running example is a company brain: people, teams, a folder hierarchy
mirrored from Drive, a ticket system with tenants, documents with a status,
and comments.

### Who: principals and groups

A ***principal*** is a node that stands for an actor. One of its properties,
marked `@principal`, holds the actor id the server resolves from the token:

```pg
node Person @securable {
  slug: String @key
  actor: String? @principal @unique     // "principal:alice"
}
```

A ***group*** is a node whose row is a set of principals and other groups.
Nothing marks a group; a type is a group because a `@member` edge type
targets it. Membership chains: a member of a subteam is a member of the
team.

```pg
node Team @securable @in(slug) { slug: String @key }
edge MemberOf: Person -> Team @member { expires: DateTime? @expires }
edge SubteamOf: Team -> Team @member
```

A ***subject*** is a principal or a group: anything that can hold access.

Cedar's policy bundle also has groups, static lists of actor ids such as
`staff` or `tenant-acme`. They are the root of trust: nothing a graph writer
can do changes who is staff. One bridge makes them subjects in the graph: a
group row keyed `policy:<name>` has as its members exactly the actors of the
policy group `<name>`. It has no explicit members; a `@member` edge into it
is refused. `policy:staff` is then "everyone inside the company", and a
grant from it reaches every employee and no outsider, because no graph write
can put an outsider into a policy group.

### What: securables and placement

A ***securable*** is a node whose row is a thing access attaches to: a
folder, a channel, a tenant, a deal, a person. Its key is unique across every
securable type and is never reused, which is why connectors use immutable
source ids (`drive:0B7x…`, `slack:C123`) and never names or email addresses.
`@securable` is declared, not derived, because placement values are data and
the key space they draw from has to be known in advance.

```pg
node Folder @securable @in(slug) { slug: String @key }
node Tenant @securable { slug: String @key }
node Class  @securable { slug: String @key }    // fixed securables for columns
```

***Placement*** says which securable a row is in, and it is a column:

```pg
node Doc @in(folder) {                 // each document is in its folder
  id: String @key
  folder: String
  title: String
}
node Ticket @in(tenant) {              // each ticket is in its tenant
  id: String @key
  tenant: String
  status: String
  hr_notes: String? @in("hr-notes")    // this column is in a fixed securable
}
```

`@in(folder)` on `Doc` places each row in the securable named by its `folder`
value. `@in("hr-notes")` on a property places the whole column in one fixed
securable: for an actor who cannot read `hr-notes`, the column does not
exist. On a type, `@in("key")` is sugar for a constant placement without a
stored column. A type may carry several placements; a row is then visible
only to someone who can read all of them, which is how "the EMEA deals,
classified confidential" composes without a securable for every combination.

A row with a null placement is in no securable and is visible only under a
view of everything. A securable is itself placed like any row (`Folder
@in(slug)` places each folder in itself), so a folder's existence is as
private as its contents. `Person` is a securable but is not placed, so people
stay visible while each person's own securable holds their private rows.

Why a column? At 10^5 actors and 10^8 rows, "who may see what" cannot be
stored pair by pair. It has to factor through a small middle: rows carry a
label, actors fall into groups, and the label is one value per row, indexed,
read with one `IN` lookup. The number of distinct labels is the number of
distinct audiences, which the source systems already have.

### Linking them: grants and roles

A ***grant*** is an edge from a subject to a securable carrying a ***role***:

```pg
edge TeamAccess:   Team   -> Folder @grant { role: enum(viewer, editor, owner) @key(@src, @dst) }
edge PersonAccess: Person -> Folder @grant { role: enum(viewer, editor, owner) @key(@src, @dst) }
edge ClassAccess:  Team   -> Class  @grant { role: enum(viewer, editor, owner) @key(@src, @dst) }
edge TenantAccess: Team   -> Tenant @grant { role: enum(viewer, editor, owner) @key(@src, @dst) }
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
goal G3 in one sentence. A tenant is one edge from the bridge row:
`TenantAccess: policy:tenant-acme -> tenant:acme, viewer`, written once.

### Composing audiences: inheritance, requirements, walls, time

Four more edge kinds finish the algebra. Each exists because one of the
situations needs it and none can be built from the others.

```pg
edge FolderParent: Folder -> Folder @inherits     // readers of the parent read the child
edge Derives:      Class  -> Folder @requires     // readable only if every source is
edge Walled:       Team   -> Folder @deny         // never, whatever else says
```

- **`@inherits`** (securable → securable): readers of the target get their
  role on the source, capped by the edge's optional `role` (default
  `viewer`). A folder tree is a chain of these. Union over labels.
- **`@requires`** (securable → securable): the source is readable only if
  every target is. A summary derived from two channels is placed in a
  securable that inherits from one and requires both, so exactly those who
  can read both sources can read the summary, and it stays correct as
  memberships change. Intersection, at variable arity; several `@in` on a
  type is the same intersection at fixed arity.
- **`@deny`** (subject → securable): the subject's members never read the
  securable, nor anything that inherits from it, whatever grants say. An
  ethical wall is a list of people who must not see a deal, so denial is by
  subject, and it is absolute for the subtree. Difference, in one layer.
- **`@starts` and `@expires`** on a `DateTime` property of a member, grant or
  inherits edge: the edge counts only inside its window, evaluated at the
  request's admission time. An embargo, a contractor's window, a temporary
  share: exact at the instant, with no expiry job.

Every audience is then a set expression over groups and labels:
`(granted ∪ inherited) ∩ required − denied`. Union, intersection and
difference are each needed; difference is by groups only, because a general
difference would add a second layer of negation that nothing requires.

### Computing a view

For actor `a` with principal `p`, at admission time `t`, with `G(a)` the
policy groups `a` belongs to:

```text
1. Closure      C0 = {p} ∪ { policy:g | g ∈ G(a) }
                   ∪ every group reached from C0 over live @member edges
2. Denial       D0 = every securable with a live @deny edge from a subject in C0
                D  = D0 ∪ every securable that @inherits, transitively, from one in D0
3. Closure      C  = as C0, but never entering a group that is in D
4. Roles        the least fixed point of
                   role(s) ≥ viewer            if s ∈ C and s is a securable
                   role(s) ≥ r                 if some x ∈ C has a live @grant(r) edge to s
                   role(s) ≥ min(role(x), c)   if x ∈ R and s has a live @inherits(c) edge to x
                R  = { s ∉ D | role(s) ≥ viewer and every @requires target of s is in R }
                W  = { s ∈ R | role(s) ≥ editor }
                O  = { s ∈ R | role(s) ≥ owner }
```

The actor's own view is `(R, W, O)`: the securables it may read, write and
administer, and through placement the types, columns and rows those admit.

Denial is computed first, from the full closure, closed downward over
inheritance, and then removed from everything after it: no membership above
a denied group, no grant from one, no inheritance through one, and no direct
grant on anything beneath one. The negation sits in one layer and everything
after it only adds, so the fixed point is unique, evaluation terminates, and
a cycle never makes anything readable. An operator reading
`GET /graphs/{id}/access` sees exactly this: memberships, denied securables,
readable securables with roles.

Worked example:

```text
alice ─member→ team:eng ─editor→ folder:eng
bob   ─member→ team:legal ─viewer→ folder:deal-acme
bob   ─member→ team:acme-wall ─deny→ folder:deal-acme
carol ─editor→ folder:deal-acme
carol ─viewer→ folder:deal-acme/term-sheet     (a subfolder; FolderParent → folder:deal-acme)
bob   ─viewer→ folder:deal-acme/term-sheet     (shared with bob directly)
policy:staff ─viewer→ folder:general           (alice, bob and carol are in policy group staff)
team:hr ─viewer→ hr-notes
policy:tenant-acme ─viewer→ tenant:acme
```

| | alice | bob | carol | acme-support |
|---|---|---|---|---|
| Closure | policy:staff, team:eng | policy:staff, team:legal, team:acme-wall | policy:staff | policy:tenant-acme |
| Denied | — | folder:deal-acme, folder:deal-acme/term-sheet | — | — |
| Readable | folder:general, folder:eng (editor) | folder:general | folder:general, folder:deal-acme (editor), folder:deal-acme/term-sheet (editor) | tenant:acme |
| Sees `hr_notes` | no | no | no | no |

Bob is granted the deal room through legal and the term sheet directly; the
wall removes both, because the term sheet inherits from the walled folder.
Carol holds `editor` on the term sheet by inheritance, above her direct
`viewer`; the higher role wins. `acme-support` is in one policy group, holds
`tenant:acme` through its bridge row, and sees nothing else.

### What the engine does with a view

A query needs no change. The engine applies the view in three places, all
before the query runs:

1. **The schema view.** The catalog the actor's query is compiled against
   omits every type and column placed in a securable the actor cannot read,
   and every namespace holding only hidden types. A hidden name is therefore
   an unknown name, and the compiler reports it with the same diagnostic,
   byte for byte, as a name nobody ever declared.
2. **The row filter.** When the plan is accepted, every scan of a placed type
   gains the conjunct `placement IN R`, one per placement. It is a typed leaf
   of the plan carrying the view's digest, not a string and not a parameter;
   acceptance refuses a plan on which any placed scan lacks it or on which an
   operator could consume a placed row before it. Lance serves it as an `IN`
   list over the placement column's BTREE index. A traversal applies it at
   every hop, so no path passes through a hidden node.
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
answer refuses on it, or asks for a stored query with reach.

### Writing under a view

| Operation | Rule |
|---|---|
| Insert or update a placed row | `editor` on every securable the row is placed in, before and after the change. |
| Insert without a placement value | Refused (`placement_required`). A writer places a private row in its own securable by writing `$subject`, its principal row's key, into the placement column. Nothing becomes visible by omission. |
| A placement naming a securable the writer cannot edit, or no securable at all | `view_write_denied`, in both cases alike, so the refusal does not reveal whether the securable exists. |
| Update or delete | Matches rows in the view; a visible row that is not writable refuses the statement. |
| Delete that would cascade to an edge the writer cannot see | `delete_blocked`. |
| Edge insert | Both endpoints visible, else exactly as a missing endpoint. |
| Branch merge | Refused unless every row changed on either side since the merge base is writable by the merging actor; only then are conflicts evaluated and reported. |
| Schema apply, `optimize`, `cleanup`, index builds, `upgrade` | Need a view of everything (`over: all` on their actions); they touch every row. |

A writer of a placed type can learn, from whether a write succeeds, a short
list of facts and nothing more: whether a key exists in that type; whether a
`@unique` tuple exists; whether an edge would cross a `@card` bound; whether
a deleted node has a hidden edge; whether a merged row changed outside its
view. Those disclosures are stated, bounded, and the price of keeping the
constraints honest.

***Access edges*** (grant, deny, inherits, requires, member) and ***identity
values*** (principal ids, securable keys, bridge rows) are the facts the view
is computed from, and they get stricter rules: writing an access edge needs
`owner` on the securable or group it attaches to; creating a securable, a
bridge row or a principal value needs a view of everything for `change`; a
securable's key never changes and a securable is never deleted (a check at
delete time cannot see every branch and retained snapshot that may still
place rows in it). All are written on `main` only, for the reason given in
[Changing access safely](#changing-access-safely).

### Rules from data: computed placement

"Published documents are public" is a rule over a row's own columns. Under
labels it becomes a placement the engine derives:

```pg
node Doc @in(audience) {
  id: String @key
  folder: String
  status: String
  audience: String @computed(status == "published" ? "public" : folder)
}
```

- The expression is a total scalar expression over the row's own columns,
  with a `String` result. It may not name another row, a parameter, the
  caller or the clock: none of those is a label.
- The engine evaluates it on every insert and update, after the statement's
  assignments and before validation, so an update that changes `status`
  moves the row and the write rules see the new placement.
- A writer who assigns the column directly is refused, as is a load file
  that carries it.
- The computed value must name a securable that exists on `main`, like any
  written placement, or the write is refused; a misspelt literal fails the
  first write loudly instead of hiding rows.
- Changing the expression at schema apply rewrites the column on every
  existing row of the type in that apply's single publication.
- It reads one row and nothing else: one expression per written row, no
  second rule engine, no reads of other rows.

`@computed` is a computed column, not an access construct; placement is a
column, so the two compose for free. Without it, every connector and every
agent that writes documents has to set the audience by hand and keep it
right.

### `over`: when a request's view is not the actor's own

Three kinds of thing can carry one optional declaration, `over`:

```yaml
over: all                                    # the whole graph
over: { types: [Ticket, Comment] }           # the actor's view, narrowed
over: { securables: ["tenant:acme"], branches: [main] }
```

`over: all` sets the view to everything. A narrowing `over` intersects the
view with the listed securables (each listed one, plus each securable that
inherits from it when every securable it requires is also listed), types and
branches. The three attachment points:

**On a rule.** The actor's view for the actions the rule grants.

```yaml
rules:
  - id: platform-bypass
    allow: { actors: { group: platform }, actions: [read, change, schema_apply], over: all }
  - id: connectors-write
    allow: { actors: { group: connectors }, actions: [change], over: all }
  - id: triage-agent
    allow: { actors: { group: triage-agents }, actions: [read], over: { types: [Ticket] } }
```

A bypass actor is a rule with `over: all`. A standalone agent that may read
tickets and nothing else is a rule with a narrowing. When several allow rules
match one action, the request's view is the union of their views, as several
grants would be; a narrowing therefore narrows only when it is the only
grant. Served reads under `over: all` are recorded in the policy decision
log.

**On a delegated credential.** The credential names the ***subject***, the
person, and the ***actor***, the agent, and may carry a narrowing `over` and
an expiry. The request's view is `view(subject) ∩ view(actor) ∩ over`, where
each side's view is the one its rules give it for the action. Cedar must
allow the action for both identities; commit receipts and audit events
record both. A credential's `over` may not say `all`.

**On a stored query.** A stored query normally runs under its caller's view
and needs `invoke_query` plus `read`, as today. A stored query that declares
`over` runs under *that* view instead, and needs only `invoke_query` on its
own name:

```yaml
queries:
  global_ticket_count:
    file: reports.gq
    over: all
  acme_digest:
    file: reports.gq
    over: { securables: ["tenant:acme", "folder:acme-shared"] }
  edit_own_comment:
    file: comments.gq
    over: all
```

```yaml
rules:
  - id: reporting-count
    allow: { actors: { group: reporting }, actions: [invoke_query], queries: [global_ticket_count] }
  - id: staff-comment
    allow: { actors: { group: staff }, actions: [invoke_query], queries: [edit_own_comment] }
```

The caller invokes it as any stored query, `POST /graphs/{g}/queries/{name}`.
Its view is the declared one, so the reporting agent gets a count over every
tenant's tickets while being able to read none of them; the result's
`restricted` reports the declared view, so the caller still knows whether an
answer is whole. The program is fixed by the deployment, so the disclosure is
whatever that program computes over that view, and it never changes because
some person's access changed.

A stored query with `over` may write. Within its view it holds `viewer` for
reads and `editor` for writes:

```gq
query edit_own_comment($id: String, $body: String) {
  update Comment { id: $id } where author == $subject
  set { body: $body }
}
```

`$subject` is the key of the principal row of the person the request acts
for (the caller itself without delegation; null when the actor has no
principal row, which refuses a program that names it). It is the only
caller-derived value a program may name, because the engine already holds
it. Every other fact about a caller, its tenant, its groups, its clearance,
is a row or a policy group and is reached through membership, never through
a trusted attribute: an attribute that decides access *is* an audience, so
it is a group or a label. Deployed with `over: all`, this program edits
exactly the caller's own comments wherever they are placed; a document
editor's ordinary `change` edits any comment in documents they can edit. Two
different rights, granted separately.

Rules for a stored query with `over`:

- **Granted by name.** It is invocable only through a rule that lists it
  under `queries:`. A rule granting `invoke_query` without a list covers
  view-bound stored queries only. A caller without the grant gets the same
  answer as for a query that does not exist.
- **Deployed by someone whose view covers it.** Deploying or redeploying a
  query whose view reveals a securable's data changes who has access to it,
  which is the `owner` role's job. The deployer must hold `owner` on every
  securable in the query's view; for `over: all` that is a deployer whose
  own view for `change` is everything. Without `over`, `config_manage`
  suffices, as today. The deployment records the deployer and the authorizing
  policy digest.
- **Bounded parameters.** Every parameter has a bounded type: an enum, a
  number with a `@range`, or a string with a length limit. The deployer can
  then enumerate what the program can disclose. Repeated parameterized calls
  over a view the caller cannot otherwise read can isolate individuals, so
  every invocation is an audit event with its parameters; minimum group
  sizes do not prevent differencing, and per-recipient privacy budgets are
  later work.
- **Terminal.** Its result cannot be joined with the caller's view inside one
  query. That would reintroduce the rows the view excludes into a query the
  caller wrote, which is the hole the view exists to close.
- **Activation.** Queries, bindings and policy are one candidate deployment;
  a dangling name or a securable in `over` that does not exist on `main`
  prevents activation. A request already admitted finishes under its
  captured bindings; later requests use the new ones.

`GET /graphs/{id}/queries` lists, under the caller's view, the stored queries
that typecheck against its schema view plus the reach queries it may invoke,
with their parameter types. `GET /graphs/{id}/access` reports the view and
the names.

### Agents acting for people: delegation

Covered by `over` on a credential above. Every composition is an
intersection, so a chain of delegations only narrows. Keeping content away
from agents needs no special tier: a narrowing `over` on the agent's rule or
credential, or a team of agent principals with a `@deny` edge.

### Sharing outward

A recipient is a principal in a policy group that no bridge row inside the
company grants anything to; a share is whatever its view admits. Trusted
collaborators may use that view live. Outside parties get the view
materialized into a separate graph, kept current or delivered once, because
live access exposes side channels (timing, resource limits) an outsider is
not trusted with; the materialization and its reconciler are a later phase
and belong to the actor views draft. Compartments where a defect in view
enforcement would be unacceptable, where retention differs, or where the
threat includes holders of storage credentials get their own graph or
cluster: securables are for the long tail of channels, folders, deals and
matters, separate graphs for the few compartments that justify their own
infrastructure.

### Changing access safely

Three rules make a revocation mean something.

**Access facts live on `main` only.** A branch forked before a revocation
holds the old edges; if those could be written on a branch and merged back,
the merge would revive the access. So access edges, identity values and
securable rows are written on `main`, and a branch write, merge, promotion
or restore that touches them is refused. Review of an access change goes
through an unpublished graph commit on `main`
([Unpublished graph commits](2026-10-10-unpublished-graph-commits.md)): the
proposal captures the exact head it was written against, a reviewer reads it
as a snapshot, publication re-checks every change for the publishing actor,
and an access change published in between expires the proposal.

**Authorization is never older than the data.** The ***authorization
state*** of a request is the applied policy digest, the applied stored-query
bindings' epoch, and the published versions of every ***access table*** (each
securable and principal type, each type a `@member` edge targets, each access
edge type) on `main`. It is not `main`'s head commit, which every unrelated
write advances. A read on `main` takes the authorization state and the data
from one catalog image. A read on another branch or a historical target
captures the data first and `main`'s authorization state after it, so any
revocation published before the data cut is applied. A write on `main`
captures the state with its baselines, and the access tables' versions become
expected versions of the publication's compare-and-swap, so a concurrent
access change by any process fails the publication and the attempt retries
with fresh authority.

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
is the number of distinct audiences an actor belongs to: small for channels
and folders, up to 10^5 for a heavy mail user when every thread is its own
audience. A second physical encoding for that regime (reader groups stored
on the row, filtered by overlap with the actor's closure) is evidence-gated,
not designed; this RFC commits to one encoding. A stored query with `over`
adds no per-row cost.

## Evaluating the design

### Does it express what companies need?

The standard catalogue, against the design. ✓ is direct; ◐ is a named
modeling step; ✗ is declined with a fallback.

| | Pattern | | How |
|---|---|---|---|
| 1 | Tenant isolation | ✓ | `@in(tenant)`; one grant edge from the tenant's bridge row |
| 2 | Folder and org hierarchies | ✓ | An `@inherits` chain; the edge's role caps what flows down |
| 3 | Groups in groups | ✓ | The `@member` closure |
| 4 | Sharing one item with one person | ✓ | The item or its container is a securable with a `@grant` from the principal |
| 5 | Ethical walls, blocklists, keeping agents out | ✓ | `@deny`; absolute for the subtree |
| 6 | Temporary access, embargoes | ✓ | `@starts` / `@expires`, exact at the instant |
| 7 | Need-to-know on several axes | ✓ | Several `@in` on one type, intersected at the row |
| 8 | Derived content visible to readers of all its sources | ✓ | `@inherits` one source, `@requires` every source |
| 9 | Hiding a column | ✓ | A fixed `@in` on the property; absent from the schema view |
| 10 | Private rows | ✓ | Place the row in your own securable with `$subject`; nothing is visible by omission |
| 11 | Read, write, administer as separate rights | ✓ | `viewer < editor < owner` |
| 12 | Staff versus outsiders | ✓ | Grants from `policy:staff`; no graph write can add an outsider to it |
| 13 | Delegation that only narrows | ✓ | `view(subject) ∩ view(actor) ∩ over` |
| 14 | Context conditions (MFA, network, time of day) | ✓ | Cedar context on the action; the view is context-free |
| 15 | Computation rights: count but not list, run this tool | ✓ | A stored query with `over`, granted by name |
| 16 | A rule over the row's own columns | ✓ | Computed placement |
| 17 | External sharing with revocation | ✓ | A recipient's view, materialized |
| 18 | A relationship confers access | ◐ | Declare the relationship edge type `@grant` or `@inherits`; a multi-edge relationship takes one `@inherits` edge per hop, mirrored by a connector |
| 19 | A rule over other rows (notes that mention my accounts; visible after three approvals) | ◐ | A stored query with `over`; the caller gets answers |
| 20 | Custom roles (commenter, approver) | ◐ | A stored mutation with `over` per right |
| 21 | Ownership writes (edit your own comment) | ◐ | A stored mutation with `over` and `author == $subject` |
| 22 | A column rule that varies by row | ◐ | A securable per row, as a compensation file per person |
| 23 | Placement on edges | ◐ | Edges are visible when both endpoints are, until `@in` on edge types (a later phase) |
| 24 | Branch-scoped and historical access | ◐ | `over.branches` narrows; history binds in a later phase |
| 25 | Aggregates with a privacy guarantee | ◐ | Bounded parameters and audited invocations; budgets are later work |
| 26 | Masking (last four digits) | ✗ | A column is present or absent; store the masked form as its own placed column, or deploy a program |
| 27 | Write without read | ✗ | Roles form a chain; a drop box is a stored mutation with `over` |

The frame: Zanzibar expresses union, intersection, exclusion and
tuple-to-userset over relations an operator names per type; the view algebra
is that with a fixed vocabulary and labels stored as columns. SQL row-level
security accepts any predicate and returns no proof and no index. Cedar
decides actions with context and never filters rows. This design takes the
closed algebra for data and gets predicates back only as programs, which
return answers.

### Does it have the fewest primitives that compose?

Applying G11 to every construct:

| Construct | Required by | Derivable from another? |
|---|---|---|
| `@principal` | identity | No: which property holds the actor id cannot be inferred. |
| `@member` | closure | No: the only chaining relation. |
| `@securable` | the key space | No: placement values are data, so the types they may name must be declared. (A group needs no mark: it is whatever `@member` targets.) |
| `@in` | labels | No. The fixed-key form on a type is sugar for a constant placement. |
| `@grant` | union over groups | No: one hop, with a role. |
| `@inherits` | union over labels | No: a tree of grants without it is a grant per node per subject. |
| `@requires` | intersection at variable arity | No: several `@in` give fixed arity only. |
| `@deny` | difference | No: unions and intersections cannot remove anyone. |
| `@starts` / `@expires` | time | No: a deleting job would be a queue and a lag. |
| `@computed` | attribute rules | A general computed column; not an access construct. |
| `over` | bypass, caveats, reach | No, and it replaces three: a bypass list, a caveat object and a capability object were each this declaration on one attachment point. |
| Per-name `invoke_query` | granting a tool to a caller | No: the graph-wide action cannot name one program. |
| `$subject` | ownership rules, private rows | No: the only caller fact the engine holds that is not a row. |
| The bridge row `policy:<name>` | tenants, staff | Replaces a second grant mechanism in policy. |

No rule in the design exists only to choose between two mechanisms: policy
holds actor sets because it is the root of trust, and the graph holds every
access edge; a wider view comes only from `over`; a narrower one only from
`over`.

### What it costs

| Choice | Gained | Given up | Why acceptable |
|---|---|---|---|
| Labels, not predicates, for data | Composition under any operator; a proof; an indexed read | Row rules that mention other rows or the caller | Those are programs; a company brain's audiences are its source ACLs' audiences, which are labels already |
| Three roles | Inheritance caps are well defined; write implies read; source roles map one to one | Commenter, approver and other action-shaped roles | Each is a stored mutation with `over`; a per-type write floor stays available as the one chain-preserving extension if free-form writes at `viewer` are ever needed |
| Deny is absolute for a subtree | Walls mean what they say; one spelling | "Deny the folder but allow this one file" | That shape is a modeling error in a wall; restructure instead |
| Computed placement in the write path | Attribute rules become declarations | One expression evaluation per written row | Bounded by the row; no reads of other rows |
| `over` on a stored query | Approved answers whose reach is fixed by the program | A second place, besides the actor's grants, that defines data reach | Deployed only by someone whose own view covers it; bounded parameters and audited invocations bound differencing |
| No explicit default placement | An agent reading a schema knows exactly where each row goes | Writing `$subject` into private rows | Nothing becomes visible by omission either way; explicitness wins |
| Access edges `main`-only | No revival of revoked access through a fork | Branch review for access changes | Proposals give review, with expiry on a moved head |
| Bridge rows instead of grants in policy | One grant mechanism; policy stays identity-only | One graph write per static audience | Written once, by an actor with a view of everything |
| `$subject` as the only caller binding | No trusted-attribute mechanism to deploy and audit | Attribute rules that are not rows | An attribute that decides access is an audience, so it is a group or a label |
| A new access dimension is a schema change | Placement columns are indexed, typed and part of the contract | Serialization behind the schema gate | A dimension is schema; live-branch schema apply is a separate proposal |
| No operator allowlists | No contract language | "This agent may not search" | That is a resource policy, not access; the view already bounds what a search can return |

### What it declines, and the fallback

Masking and value transformation (deploy a program, or store the masked
form); write without read (a program); a fourth role (a program, or a
securable per right); a column rule that varies by row (a securable per row);
joining a reach query's answer into the caller's view inside one query (the
hole the view exists to close); operator allowlists (a resource policy, out
of scope); relaxing the schema change for a new dimension; a second physical
encoding past `max_securables` (evidence-gated).

### Composition properties

- Closed under union and intersection; difference by group only, in one
  layer; monotone after denial; unique fixed point; cycles inert.
- Orthogonal dimensions compose at the row without a label per combination.
- Narrowing composes by intersection on every attachment point; chains never
  widen.
- Derived content composes as an information-flow lattice.
- `over: all` is the one widening, and it is held by a rule the root of trust
  wrote or by a stored query deployed by someone who already held that view.

## Alternatives considered

Each option from [The design space](#the-design-space), with the reason it
was not chosen as the whole answer.

**Views alone.** Everything a company brain needs, and no answer above
access except by letting a stored query run with its author's view. Then a
tool's disclosure changes when a person's access changes, withdrawing a
person's access can silently break every tool they wrote, and a caller still
needs `read` to invoke anything. `over` on the query, deployed by someone
whose view covers it, replaces "whoever wrote it" with an explicit,
authorized declaration.

**Programs alone.** Everything a tool needs, and no data authority a query
can rely on without restating it. Audiences become attributes compared by
equality: groups, inheritance, walls, time windows and several audiences on
one row have no form; a connector mirroring Drive has nothing to write;
traversal, search and writes each need their own proof rules; a refusal tells
a person to rewrite their question. The company brain is unserved.

**A capability object beside stored queries.** An earlier revision of this
RFC had one: a named binding of a program, a scope and a contract, with its
own routes and configuration. It was a stored query with two attributes and
a new noun. Per-name `invoke_query` and `over` are the attributes; the noun,
the routes, the config block and three error codes were removed.

**Contracts for submitted queries.** A caller submits new programs under a
contract that lists allowed types, fields and operators, and the engine
checks the shape. Once the view is the data authority the contract proves
nothing about rows, which the earlier revision admitted; what remained was a
type allowlist, which is a narrowing `over`, and operator limits, which are
cost control. Removed.

**Grants from the policy bundle.** A list of `(group, role, securable)`
entries in policy, beside `@grant` edges in the graph, with "rate of change"
deciding the home. A rule whose job is to arbitrate between two mechanisms
says one of them should not exist. The bridge row makes the policy group a
subject and the grant an edge.

**Predicates as the data authority.** Strictly more expressive than labels,
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
action-shaped right, which is a program. A per-type write floor is the one
extension that would keep the chain; it is held, not adopted.

**Explicit-beats-inherited denial.** NTFS lets a direct allow on a child
beat an inherited deny from its parent. It is the behavior "nothing flows
through a denied securable" gives by accident, and a wall that a direct
share can breach is not a wall. AWS's explicit deny, which no allow
overrides, is what walls expect.

**A default placement for inserts.** Rows without a placement going to the
writer's own securable is convenient and safe, and it is a second, implicit
way a row gets placed. An agent reading the schema should see exactly where
each row goes; `$subject` makes the private case one explicit value.

**Separate graphs for everything.** The right answer for compartments that
must not share an engine, and the wrong one for a company brain, whose
audiences overlap.

**Doing nothing.** Graph × branch × action, with everything finer in
application code, where neither non-interference nor fixed disclosure can be
shown. The situations above are not served.

### Relationship to the earlier drafts

Two drafts preceded this one and each contributed to it.

[Actor views and subgraph sharing](https://github.com/ModernRelay/omnigraph/pull/951)
(PR #951) defines the view: the vocabulary, the algebra, placement, the
engine's enforcement, restricted search, writes, delegation, sharing and
vaults, with the Lance evidence behind each. This RFC adopts it as the data
authority and changes these things: denial closes downward over inheritance;
`@group` is derived rather than declared; the `views` block of its policy
bundle (internal, bypass, grants, limits) is replaced by the bridge row,
`over` on rules and cluster-configured limits; computed placement moves from
its seventh phase into the row-view phase; the default placement of inserts
is replaced by explicit `$subject`; its definer-rights stored queries are
replaced by `over` on a stored query; the `view` result object becomes one
field. Its Lance citations are re-anchored from 11.0.0 to 13.0.0; every fact
holds.

[GQP query contracts and Cedar grants](https://github.com/ModernRelay/omnigraph/pull/1197)
(PR #1197) proposed granting computation by name and fixing a program as the
unit of disclosure. Both survive, as per-name `invoke_query` and `over` on a
stored query. Its contract language, guard-preservation proofs, per-operator
obligations, single-binding fragment, trusted attributes, submitted mode and
capability binding do not: the view carries the proof, and the rest was
either a narrowing `over` or cost control.

Both remain drafts; this RFC supersedes neither. Each is amended as above and
cites this RFC for the division.

## Invariants

- **7, physical acceleration is derived.** The row filter is a conjunct the
  plan declares; an index serves it or a scan does, and a missing index
  changes cost only. Restricted search's statistics are declared plan state.
- **9, query semantics are typed structures.** The view is a typed plan
  leaf; `over` is a typed binding in the accepted execution object; none
  passes through a string, a transport flag or a side table.
- **10, trust at the boundary, enforced at the engine.** Bearer tokens
  resolve actors; the view is computed from that identity and the applied
  bindings; every mutating engine entry point applies the write rules under
  the request's view, so embedded and CLI-direct writers bypass nothing.
- **11, bounded and observable.** Every bound refuses with a registered
  outcome; no partial result exists.
- **12, one source of truth.** Access edges are graph data; actor sets are
  policy; `over` is deployment configuration; a view is derived per request
  and cached only per access version. Nothing shadows anything.

Deny-list items touched: none. The branch-write window is stated, not fenced
by a process-local lock; no queue, no inline rebuild, no string-built
semantics, no cloud-only path.

## Compatibility and reversibility

- **Schema.** The view declarations set the schema feature `actor-views`; a
  build without this RFC refuses to open such a graph. `@computed` is one
  more declaration under that feature.
- **Policy.** The bundle gains two optional fields on a rule, `queries:` and
  `over`, and nothing else. A bundle using `over` on a graph without the
  feature is accepted (`over: all` on a graph without placements changes
  nothing), which is how the plumbing is proved before any view exists.
- **Configuration.** A stored query entry under `queries:` gains an optional
  `over`; limits are cluster configuration per graph.
- **Wire.** `restricted` on results, the new fields of
  `GET /graphs/{id}/access`, parameter types on `GET /graphs/{id}/queries`,
  and six error codes (`view_restricted`, `view_write_denied`,
  `placement_required`, `delete_blocked`, `identity_binding_denied`,
  `view_limit_exceeded`). No new route. Graphs without the feature and
  callers without `over` see no change.
- **Withdrawn surfaces.** Definer-rights stored queries, the GQP contract
  language and the earlier capability object: none shipped, nothing deployed
  changes meaning.
- **Reversibility.** The division is a design rule, not a format; reverting
  it means choosing one half and losing the workload the other serves. The
  format decisions that are hard to reverse (placement columns, history
  binding, packed references) are owned by the actor views draft and gated
  by its evidence.

## Evidence and tests

The mechanism's owners are named by the actor views draft:
`policy_engine_chassis.rs`, `writes.rs`, the planner's `validate/tests.rs`,
`search.rs`, `lance_surface_guards.rs` and the server's `auth_policy` suite.
This RFC adds the evidence for the division itself:

- **The ledger as a corpus.** One `.gqt` case per row of the ledger under
  `crates/omnigraph-gqt/cases/access/`, each stating the pattern, the schema
  and policy that express it, and the rows each actor sees; ✗ rows show the
  fallback. The corpus is the proof of G10 and the regression guard for every
  later change to the algebra.
- **Denial closure.** A direct grant on a file inside a denied folder is
  hidden; a direct grant on a sibling outside it is not.
- **Bridge rows.** A grant from `policy:tenant-acme` equals a grant from a
  graph group with the same members; a `@member` edge into a bridge row is
  refused; a policy change moves the view at the next admission.
- **Computed placement.** Insert, an update that moves the row, a writer who
  assigns the column, a load file that carries it, a changed expression at
  schema apply rewriting rows in one publication (`schema_apply.rs`).
- **`over`.** Server `auth_policy` and `stored_queries` cases: a stored query
  with `over: all` answered for a caller with no `read`; the same caller
  refused on an ordinary read and on an unlisted query, with the not-found
  answer; a narrowing on a rule hiding a type the actor's placements would
  admit, and not narrowing when a second unnarrowed rule matches; a
  delegated credential's narrowing; a stored mutation under
  `author == $subject`; a deployment refused when the deployer lacks `owner`
  on a securable in `over`; `over: all` on a graph without placements
  behaving as today.
- **Replay.** `engine_v2_plan_replay.rs` gains a plan bound to a stored
  query's `over` whose replay refuses when the view digest or the subject
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
2. **Per-name `invoke_query` and `over` on stored queries.** Invocation
   without `read`, bounded parameter types, deployment authority, listing.
   Needs no securables: on a graph without placements `over: all` behaves
   as today, which proves the plumbing. Query-only access for agent tools
   ships here.
3. **Type and column views.** The annotations, identity-binding rules, the
   view computation with fixed placements, the schema view, bridge rows,
   denial closure, `over` on rules.
4. **Row views.** Per-row placement, the row filter, masked traversal,
   restricted search, `restricted` on results, `@computed`, `over` on
   credentials.
5. **Writes under a view, then the later phases** as the actor views draft
   orders them: incremental maintenance, packed references, edge placement,
   search acceleration, history and feeds, materialized shares.

`implementation` moves to `in-progress` at stop 1 and to `partial` at
stop 4; the actor views draft advances its own field as its phases land.

## Unresolved questions

1. **Re-authorization of `over` on a stored query.** Whether the deployer's
   `owner` roles are re-checked at every bundle re-apply or only at
   deployment. Re-checking makes a tool fail when its deployer loses access,
   the liability definer queries had; not re-checking leaves a view standing
   after its authorizer has gone.
2. **Spellings.** `over` and `queries:` on a rule and on a stored query entry
   are proposed here and need review against the policy schema and the
   cluster configuration schema before acceptance.
3. **Computed placement syntax**, carried from the actor views draft.

## Decision log

- **2026-10-30.** Drafted from first principles for contributors, from the
  comparison of PR #951 at `d700c334` and PR #1197 at `6d7217a1` against the
  code at `118161dd` and Lance 13.0.0.
- **2026-10-30, second revision.** Retitled from "Access authority: views and
  capabilities" after an audit against G11. Removed: the capability object,
  its routes, configuration and three error codes; the contract language and
  the submitted mode; `views.grants`, `views.internal` and `views.bypass`;
  `@group`; the input-scope and caveat vocabulary; `$actor` in programs; the
  default placement of inserts. Added: `over` on rules, credentials and
  stored queries; per-name `invoke_query`; the bridge row; G11 and the
  primitive audit. The goals, the design space, the view algebra, denial
  closure, computed placement and the ledger are unchanged.

## Appendix: implementation notes for agents

For whoever implements a stop of the rollout. Read the invariants and the
Lance guide first; this appendix says where each piece goes and what to
watch.

### Where each piece lives

| Piece | Crate and place | Notes |
|---|---|---|
| Schema annotations (`@principal`, `@member`, `@securable`, `@in`, `@computed`, `@grant`, `@inherits`, `@requires`, `@deny`, `@starts`, `@expires`) | `omnigraph-compiler` schema parsing and catalog; the `actor-views` schema feature | A type is a group iff a `@member` edge type targets it. Each `@in(p)` property gets a BTREE index declaration. |
| View computation (`C0`, `D`, `C`, the fixed point) | `omnigraph-engine`, a new `engine/view` module reading the access tables of the captured authorization state | Bridge rows expand from the policy bundle's groups. Cache per (access version, actor); invalidate at the earliest `@starts`/`@expires` boundary. Bounds from cluster configuration. |
| `over` | `omnigraph-policy` (rule field), `omnigraph-cluster` (stored query entry, bundle validation), the engine's accepted execution object | One type, three attachment points. On a rule: union across matching rules. On a credential: narrowing only. On a stored query: replaces the view. |
| `$subject` | `omnigraph-compiler` (a reserved binding, typed `String?`), bound at execution from the request | The principal row's key, not the actor id. |
| Authorization state capture | `crates/omnigraph/src/db/omnigraph.rs` beside `load_accepted_view_with_schema_gate_held` | Reads on `main`: one catalog image. Other targets: data first, then `main`'s access pins. Writes on `main`: access pins as expected versions of the `__manifest` compare-and-swap. |
| Schema view | The compiler's catalog construction, parameterized by the readable set | Hidden names must produce the same diagnostic bytes as undeclared ones; test by diffing error payloads. |
| Row filter (`ViewSet` leaf) | `omnigraph-planner`: a leaf in the IR and a check in `crates/omnigraph-planner/src/validate/invariants.rs` (`Requirements::check`) that every placed scan carries it; `AcceptInput` in `validate/mod.rs` takes the view | The leaf carries the view's digest, never the list; the list is bound at execution. Replay compares the digest. |
| Scan pushdown | `crates/omnigraph/src/engine/scan.rs` | The conjunct becomes an `IN` list on the placement column; Lance pushes it through `visit_in_list` to the BTREE/bitmap index. |
| Restricted search | `crates/omnigraph/src/engine/scan.rs` (the prefilter decision at line 268 and `Eligibility` in `crates/omnigraph-planner/src/optimizer.rs`) and the search ladder in `engine/search.rs` | bm25: `FlatMatchQueryExec` with a scorer built from visible-row statistics; `nearest`: exact over visible vectors (`use_index(false)` path). Statistics are declared plan state. |
| Write rules | `crates/omnigraph/src/exec/mutation.rs` (`resolve_assignments`), `crates/omnigraph/src/validate/` (kernels such as `card_violations`), `crates/omnigraph/src/exec/staging.rs` (`commit_all`) | Computed placement is evaluated after assignments and before validation. Constraint disclosures are the only facts a refusal may carry; audit every message. |
| Per-name `invoke_query` | `omnigraph-policy` (Cedar schema: the query as a resource), `crates/omnigraph-server/src/handlers.rs` beside `server_invoke_query` (today double-gated on `invoke_query` plus the body's `read`/`change`) | A query with `over` is gated on its name alone and must *not* require `read`. Unlisted and unknown names answer alike. |
| `access` route | `GET /graphs/{graph_id}/access` exists (operation `access`) and today evaluates policy per action | Add memberships, readable and denied securables with roles, and the names invocable. |
| Error codes | The `ErrorCode` registry with HTTP status and `disposition` | Six codes; all `effects: none`, `action: fix`. `view_restricted` is temporary phase accounting. |
| Delegation | The delegated-credential work (ModernRelay/omnigraph#978): subject, actor, `over`, expiry | Cedar evaluated for both identities; receipts record both. |
| Materialized shares | Later phase; the actor views draft's reconciler | Not in the first stops. |

### Order of work, concretely

1. Thread the actor through the engine read doors and add the view slot to
   `AcceptInput`; make acceptance refuse any plan shape that could drop a
   filter on a placed scan (today no scan is placed, so the check is
   vacuous but present). Add the view digest and `$subject` to the replay
   envelope.
2. The query as a Cedar resource; `queries:` on rules; `over` on stored
   query entries; the handler gated on the name alone; bounded parameter
   types; deployment authority. Test on a graph with no `actor-views`
   declarations: `over: all` and no `over` behave the same, which proves
   the plumbing before any view exists.
3. Schema annotations, identity-binding rules, the view computation with
   fixed placements only (no per-row `@in` yet), bridge rows, the schema
   view, denial closure, `over` on rules.
4. Per-row `@in`, the `ViewSet` leaf, pushdown, traversal masking,
   restricted search, `restricted` on results, `@computed`, `over` on
   credentials.
5. Write rules, constraint disclosures, access-edge rules, proposals for
   access changes.

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
  stable-row-id compaction skips remap at `optimize.rs:847`.

Re-read every page the Lance guide lists for scalar indexes, full-text and
vector search before changing the scan or search paths; the line numbers
above are anchors, not substitutes.

### Test owners to extend

| Behavior | Owner |
|---|---|
| Policy gates on engine doors | `crates/omnigraph/tests/policy_engine_chassis.rs` |
| Write rules, disclosures, identity bindings | `crates/omnigraph/tests/writes.rs`, `tests/integration/validators.rs` |
| Schema annotations, `@computed` rewrite at apply | `crates/omnigraph/tests/schema_apply.rs` |
| Plan acceptance refusals, the `ViewSet` leaf | `crates/omnigraph-planner/src/validate/tests.rs` |
| Replay with bindings | `crates/omnigraph/tests/integration/engine_v2_plan_replay.rs` |
| Restricted search | `crates/omnigraph/tests/search.rs`; substrate fences in `tests/lance_surface_guards.rs` |
| Query results under a view, the ledger | `crates/omnigraph-gqt/cases/access/` (new directory; one case per ledger row) |
| Routes, Cedar, per-name invocation, `over` | `crates/omnigraph-server/tests/integration/auth_policy.rs`, `stored_queries.rs`, `openapi.rs` |
| Bundle parsing, stored query entries, activation | `omnigraph-cluster` in-source `tests.rs` and `admission.rs` |
| MCP parity | `crates/omnigraph-server/tests/integration/mcp.rs` |
| CLI surfaces | `crates/omnigraph-cli/tests/integration/cli_queries.rs`, `cli_cluster.rs` |

### Gotchas

- **Byte-identical diagnostics.** A hidden name and an undeclared name must
  produce the same error, disposition included. Build the schema view before
  the compiler sees the query; do not filter diagnostics afterwards.
- **`matched_rule_id` is diagnostic.** It is `.min()` over matched ids in
  `crates/omnigraph-policy/src/lib.rs`. Never select a view, an `over` or a
  group by it; the request's view for an action is the union over matching
  rules.
- **Capture order.** Authorization must never be older than the data it
  protects. On `main`, one catalog image. Elsewhere, data first, then
  `main`'s access pins. Writes on `main` pin the access tables in the
  compare-and-swap. Do not "fix" the branch-write window with a lock.
- **The digest, not the list.** The plan leaf carries the view's digest; the
  list is bound at execution. A plan replayed under another view must refuse
  on the digest.
- **`over` on a query replaces the view, it does not intersect it.** A
  stored query with `over: all` must see everything whatever its caller
  sees; a narrowing `over` on a query is absolute, not relative to the
  caller.
- **`invoke_query` on a name needs no `read`.** The existing stored-query
  handler is double-gated; a query with `over` is gated on its name alone.
- **Bridge rows have no explicit members.** Refuse `@member` edges into
  `policy:*`; expand them from the applied bundle at view computation.
- **`$subject` is the principal row's key.** Not the actor id. Null when the
  actor has no principal row; a program that names it then refuses.
- **Computed placement evaluation point.** After the statement's assignments,
  before validation, so the write rules see the new placement. Validate the
  computed value as a written one.
- **bm25 statistics.** The index's statistics include hidden rows and the
  flat scorer counts over the prefilter's rows. Supply statistics over
  visible rows; never let a prefilter change a restricted score.
- **`IN` list size.** `max_securables` bounds it; refuse above, never
  truncate. Measure plan size and pushdown at 10^4 and 10^5 keys before
  setting the default.
- **Access facts on `main` only.** Every door: mutate, load, merge, branch
  promotion, restore. A proposal (`as commit`) targets `main` and is allowed.
- **Replay bindings.** Subject, actor, view digest, the stored query's name
  and `over`. Refuse on any difference.
- **Generated contracts.** `openapi.json` regenerates through the server
  OpenAPI test (`OMNIGRAPH_UPDATE_OPENAPI=1` only when the drift is
  intentional); the HTTP conventions test must pass on the changed
  responses; the error registry owns codes.
- **DST storage-cost golden.** Capturing the authorization state reads the
  access tables' pins; that is a change to what a read reads. Add labeled
  rows to `crates/omnigraph-dst/cost_table.txt` and explain each in the
  pull request.
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
