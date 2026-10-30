---
rfc: "2026-10-30-access-authority-views-and-capabilities"
title: "Access authority: views and capabilities"
track: maintainer
status: draft
implementation: not-started
authors:
  - ragnorc
created: 2026-10-30
updated: 2026-10-30
discussion: null
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC: Access authority: views and capabilities

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

This RFC proposes two kinds of access authority with one enforcement point.

- A ***view*** says what data an actor may see and change. It is derived
  from labels on the data and from the actor's memberships, through a small
  fixed algebra, and the engine applies it to every query automatically. A
  query under a view runs as if the data outside the view did not exist.
- A ***capability*** says what computation a caller may run. It is a named
  program, deployed and approved, that a caller may invoke without holding
  the data it reads; or a contract under which a caller may submit programs
  of its own. Cedar, the policy language OmniGraph already uses, grants it.

The rule that separates the two: **an audience is a label on the data; a
rule whose predicate mentions the caller is a program.** A view must hold
for any query, so it is a set with an algebra and a proof. A capability
returns an answer or performs a write and composes with nothing, so it may
carry predicates and needs no proof beyond its own identity.

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
one step, `invoke_query`, but a stored read still needs `read`, so they do
not let anyone run a computation over data they cannot otherwise see. Finer
rules, if any, live in application code in front of the server.

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
digest, a lookup. Another agent should be allowed to explore one tenant's
tickets with new queries of its own, within stated limits, and be refused
anything else. Omnigraph is a database for agents; producing, checking and
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
| G5 | **Bounded computation rights.** A caller can be limited to a shape of query: count but not list, these types and fields, these operators. | Exploration within limits. |
| G6 | **Explainable.** An actor's effective access can be listed, and no rule order decides it. | Operators must be able to answer "why can Bob see this?" |
| G7 | **Cheap at scale.** Every read under a rule is an index lookup, not a per-row evaluation; limits refuse rather than truncate. | 10^8 rows, 10^5 actors. |
| G8 | **Reviewable and revocable.** An access change can be reviewed before it takes effect; a revocation takes effect at once; stale state cannot revive revoked access. | Legal walls, offboarding. |
| G9 | **Shares outward.** A chosen part of the graph can be given to an outside party with update and revocation. | Clients, partners, auditors. |
| G10 | **Delegation narrows.** An agent acting for a person holds at most the person's access. | Agents everywhere. |
| G11 | **Expressive enough.** The standard catalogue of permission patterns (tenants, folders, groups in groups, per-item sharing, walls, expiring access, need-to-know on several axes, column hiding, private by default, derived content) is expressible, and what is not is named. | A design that handles the first three patterns and improvises the rest grows a second system beside it. |

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
  never selects authority.
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
| **In the application** (today's practice) | The app filters results before showing them. | Anything the app author can write. | G1 (the engine cannot guarantee anything), G2 (every app repeats it), G6, G7. |
| **At the action level** (today's engine) | Cedar allows or denies `read` on a graph. | Simple, auditable. | Everything finer than a whole branch. |
| **Labels on the data** (row- and column-level security: Oracle Label Security, Elasticsearch document security, SharePoint) | Each row carries a label naming its audience; actors belong to groups; the engine filters every query by the actor's labels. | G1 with a proof, G2, G3 (a label per source audience), G6, G7 (a label is an indexed column). | G4 on its own: an aggregate over hidden rows is silently wrong, and a tool cannot be given more reach than its caller. Row rules that mention other rows or the actor. |
| **Predicates per type** (Postgres row-level security, Oracle VPD) | A policy is any boolean expression over the row and the session; the planner adds it to every query on the type. | Expressiveness: ownership rules, attribute rules, joins. | G1 (no proof; `count`, ordering and errors leak unless each operator is argued), G7 (a predicate is not an index lookup), G6 (predicates compose by evaluation, not by listing). |
| **In the query** (named-query allowlists; query contracts with proofs; the Non-Truman model) | Only approved programs run, or every program must state its own scope and the engine proves it does not exceed it. | G4, G5: the program is the disclosure. | G2 (a new question needs a proof; traversal, search and writes need proof rules per operator), G3 (audiences become attributes compared by equality, so groups, inheritance, walls and expiry have no form), G9. A refusal tells an agent to rewrite, which is right for a tool under construction and wrong for a person's question. |
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
that gives G1 with a proof, G3 and G7 at the same time. Predicates buy
expressiveness with the proof and the index; relations with rewrites buy
custom roles with the simple properties. What labels cannot express is
precisely the set of rules whose predicate mentions something other than
the row's own audience: the caller, another row, a count. Those are the
rules a program expresses naturally.

That is the line this RFC draws.

## The decision

**An audience is a label. A predicate that mentions the caller is a
program.**

- Data authority is a view: labels on rows and columns, memberships of
  actors, and a fixed algebra (grant, inherit, require, deny, time window)
  that turns them into the set of labels an actor may read, write or
  administer. The engine applies the view to every query as one filter and
  to the schema as a set of hidden names. Nothing about a query is proved;
  the view is proved once.
- Computation authority is a capability: a deployed program with an approved
  input scope, or a contract under which a caller submits programs. Cedar
  grants it. A deployed program may read or write data its caller cannot,
  because its program and scope were approved by someone who may; a
  submitted program runs under the caller's own view, so it needs only a
  shape check.
- Outsiders get copies: a share is whatever a recipient's view admits,
  delivered as a hosted or exported copy, because live access exposes side
  channels an outsider is not trusted with. Compartments that must not share
  an engine get their own graph.

Seven concrete decisions follow and are explained in
[The design, by example](#the-design-by-example):

1. Views use one fixed vocabulary and algebra; there is no role algebra and
   no predicate language on the data side.
2. A wall (deny) is absolute for everything beneath it.
3. A static audience may be granted from the policy bundle; a changing one is
   granted by edges in the graph.
4. A label may be computed from a row's own columns on every write, so
   "published documents are public" is a declaration the engine keeps true.
5. Capabilities come in two modes, deployed and submitted; a deployed
   capability may write; the only caller-derived values a program may use are
   the caller's identities.
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
node Person {
  slug: String @key
  actor: String? @principal @unique     // "principal:alice"
}
```

A ***group*** is a node whose row is a set of principals and other groups.
Membership is an edge type marked `@member`, and it chains: a member of a
subteam is a member of the team.

```pg
node Team @group { slug: String @key }
edge MemberOf: Person -> Team @member { expires: DateTime? @expires }
edge SubteamOf: Team -> Team @member
```

A ***subject*** is a principal or a group: anything that can hold access.
One group is special. The ***internal group***, keyed `internal`, has no
explicit members; its members are every actor in the policy groups the
bundle lists under `views.internal`. It is the root of trust for "everyone
inside the company", and an outside recipient can never be put in it.

### What: securables and placement

A ***securable*** is a node whose row is a thing access attaches to: a
folder, a channel, a tenant, a deal, a person's private space. Its key is
unique across every securable type and is never reused, which is why
connectors use immutable source ids (`drive:0B7x…`, `slack:C123`) and never
names or email addresses.

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
exist. A type may carry several placements; a row is then visible only to
someone who can read all of them, which is how "the EMEA deals, classified
confidential" composes without a securable for every combination.

A row with a null placement is in no securable and is visible only to bypass
actors. A securable is itself placed like any row (`Folder @in(slug)` places
each folder in itself), so a folder's existence is as private as its
contents. `Person` is a securable but is not placed, so people stay visible
while each person's ***personal securable*** holds their private rows.

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
goal G3 in one sentence.

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
  memberships change. Intersection.
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
1. Closure      C0 = {p}
                   ∪ {internal}                        if G(a) meets views.internal
                   ∪ every group reached from C0 over live @member edges
2. Denial       D0 = every securable with a live @deny edge from a subject in C0
                D  = D0 ∪ every securable that @inherits, transitively, from one in D0
3. Closure      C  = as C0, but never entering a group that is in D
4. Roles        the least fixed point of
                   role(s) ≥ viewer            if s ∈ C and s is a securable
                   role(s) ≥ r                 if some x ∈ C has a live @grant(r) edge to s
                   role(s) ≥ r                 if views.grants has (g, r, s) and g ∈ G(a)
                   role(s) ≥ min(role(x), c)   if x ∈ R and s has a live @inherits(c) edge to x
                R  = { s ∉ D | role(s) ≥ viewer and every @requires target of s is in R }
                W  = { s ∈ R | role(s) ≥ editor }
                O  = { s ∈ R | role(s) ≥ owner }
```

The ***view*** of `a` is `(R, W, O)`: the securables it may read, write and
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
internal ─viewer→ folder:general
team:hr ─viewer→ hr-notes
tenant-acme (a policy group) ─viewer→ tenant:acme
```

| | alice | bob | carol | acme-support |
|---|---|---|---|---|
| Closure | team:eng, internal | team:legal, team:acme-wall, internal | internal | — |
| Denied | — | folder:deal-acme, folder:deal-acme/term-sheet | — | — |
| Readable | folder:general, folder:eng (editor) | folder:general | folder:general, folder:deal-acme (editor), folder:deal-acme/term-sheet (editor) | tenant:acme |
| Sees `hr_notes` | no | no | no | no |

Bob is granted the deal room through legal and the term sheet directly; the
wall removes both, because the term sheet inherits from the walled folder.
Carol holds `editor` on the term sheet by inheritance, above her direct
`viewer`; the higher role wins. `acme-support` is not inside the company,
holds `tenant:acme` from the policy bundle alone, and sees nothing else.

### What the engine does with a view

A query needs no change. The engine applies the view in three places, all
before the query runs:

1. **The schema view.** The catalog the actor's query is compiled against
   omits every type and column placed in a securable the actor cannot read,
   and every namespace holding only hidden types. A hidden name is therefore
   an unknown name, and the compiler reports it with the same diagnostic,
   byte for byte, as a name nobody ever declared.
2. **The row filter.** When the plan is accepted, every scan of a placed type
   gains the conjunct `placement IN R(a)`, one per placement. It is a typed
   leaf of the plan carrying the view's digest, not a string and not a
   parameter; acceptance refuses a plan on which any placed scan lacks it or
   on which an operator could consume a placed row before it. Lance serves
   it as an `IN` list over the placement column's BTREE index. A traversal
   applies it at every hop, so no path passes through a hidden node.
3. **Search.** Full-text search under a view scores the visible rows with
   statistics computed over the visible rows only, because Lance's flat
   scorer and the index both count hidden rows; vector search compares the
   query with every visible vector; rank fusion fuses those. Each returns
   `min(k, visible)`. A search whose visible input exceeds a configured bound
   is refused, never truncated.

Non-interference (G1) follows: for two graphs that agree on an actor's view,
every operator consumes identical inputs, so rows, counts, scores, errors and
result schemas agree. Execution time may differ, and that is why outsiders
get copies.

Silent filtering can mislead: an average over the view is not the company's
average. So every result carries a signal:

```json
{ "rows": [...], "view": { "restricted": true } }
```

`restricted` is `true` when the view excluded anything from a type the plan
read. It is computed from the view and the plan, never from data, so it
reveals only a policy fact the actor may know. A client that needs a whole
answer refuses on it, or asks for a capability.

### Writing under a view

| Operation | Rule |
|---|---|
| Insert or update a placed row | `editor` on every securable the row is placed in, before and after the change. |
| Insert without a placement value | The row goes into the writer's personal securable; a writer without one is refused (`placement_required`). Private by default. |
| A placement naming a securable the writer cannot edit, or no securable at all | `view_write_denied`, in both cases alike, so the refusal does not reveal whether the securable exists. |
| Update or delete | Matches rows in the view; a visible row that is not writable refuses the statement. |
| Delete that would cascade to an edge the writer cannot see | `delete_blocked`. |
| Edge insert | Both endpoints visible, else exactly as a missing endpoint. |
| Branch merge | Refused unless every row changed on either side since the merge base is writable by the merging actor; only then are conflicts evaluated and reported. |
| Schema apply, `optimize`, `cleanup`, index builds, `upgrade` | Need a bypass actor as well as their Cedar action; they touch every row. |

A writer of a placed type can learn, from whether a write succeeds, a short
list of facts and nothing more: whether a key exists in that type; whether a
`@unique` tuple exists; whether an edge would cross a `@card` bound; whether
a deleted node has a hidden edge; whether a merged row changed outside its
view. Those disclosures are stated, bounded, and the price of keeping the
constraints honest.

***Access edges*** (grant, deny, inherits, requires, member) and ***identity
values*** (principal ids, securable keys, the internal group) are the facts
the view is computed from, and they get stricter rules: writing an access
edge needs `owner` on the securable or group it attaches to; creating a
securable or assigning a principal value is bypass-only; a securable's key
never changes and a securable is never deleted (a check at delete time
cannot see every branch and retained snapshot that may still place rows in
it). All are written on `main` only, for the reason given in
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

Without this, every connector and every agent that writes documents has to
set the audience by hand and keep it right. With it, the rule is declared
once and the read path does not change: the placement is a column with an
index like any other.

### Static audiences: grants from policy

The internal group already lets a policy group act as a subject. This RFC
generalizes it: a policy group may hold a role on a securable, declared in
the bundle:

```yaml
version: 2
groups:
  staff:       [principal:alice, principal:bob, principal:carol]
  connectors:  [principal:slack-connector, principal:drive-connector]
  platform:    [principal:root]
  tenant-acme: [principal:acme-support, principal:acme-agent]
rules:
  - id: staff-read
    allow: { actors: { group: staff }, actions: [read, invoke_query] }
  - id: connectors-write
    allow: { actors: { group: connectors }, actions: [change] }
views:
  internal: [staff]
  bypass:   [platform, connectors]
  grants:
    - { group: tenant-acme, role: viewer, securable: "tenant:acme" }
  limits:
    max_memberships: 4096
    max_securables: 100000
    max_exact_search_rows: 200000
```

A tenant deployment then needs no connector, no principal rows and no edges:
the audience never changes, so it lives in policy and is applied by
deployment. An audience that changes with a source system lives in the graph
as edges, which writers write. One rule decides the home, the rate of change,
and no fact lives in both. `views.bypass` names the groups whose view is the
whole graph: break-glass, connectors, maintenance; served bypass reads are
logged.

### Capabilities: what you can run

A ***capability*** is a named unit of computation authority, declared in
cluster configuration beside stored queries and granted by Cedar. It has two
modes.

**Deployed.** One stored query, bound at the revision it was approved at,
with parameter domains and an ***input scope***: the securables the program
reads and writes.

```yaml
queries: queries/
contracts: contracts/
capabilities:
  global_ticket_count:
    mode: deployed
    query: global_ticket_count     # match { $t: Ticket } return { count($t) as total }
    scope: all
  acme_digest:
    mode: deployed
    query: weekly_digest
    scope: ["tenant:acme", "folder:acme-shared"]
  edit_own_comment:
    mode: deployed
    query: edit_own_comment
    scope: all
    params: { id: { key_of: Comment } }
```

A caller invokes it by name:

```http
POST /graphs/knowledge/capabilities/global_ticket_count
{ "params": {} }
```

The caller needs Cedar `invoke` on that capability and nothing else: no
`read`, no view. The program runs with the scope substituted for the view in
the row filter: `scope: caller` (the default) is the caller's own view, a
list is that list, `all` is no filter. The disclosure is whatever the fixed
program computes over the fixed scope, which is what makes the approval
meaningful: it never changes because a person's access changed. The result
carries `view.restricted` for the scope, so a caller still knows whether an
answer is whole.

A deployed program may write. Within its scope it holds `viewer` for reads
and `editor` for writes, so a program with scope `all` can edit rows its
callers cannot. The only caller-derived values a program may name are
`$subject`, the principal id of the person the request acts for, and
`$actor`, the acting identity (both are the caller when there is no
delegation):

```gq
query edit_own_comment($id: String, $body: String) {
  update Comment { id: $id } where author == $subject
  set { body: $body }
}
```

Deployed with scope `all`, this edits exactly the caller's own comments
wherever they are placed. A document editor's ordinary `change` edits any
comment in documents they can edit. Two different rights, granted separately.
Every other fact about a caller, its tenant, its groups, its clearance, is a
row or a policy group and is reached through membership, never through a
trusted attribute: an attribute that decides access *is* an audience, so it
is a group or a label.

**Submitted.** One ***contract***, a shape over the schema the caller can
see, under which the caller sends programs of its own:

```gqp
contract ticket_explore {
  allow Ticket  { id, status, tenant }
  allow Comment { id, doc, body }
  allow operators { match, filter, project, aggregate, order, limit, traverse, search }
  params { $status: enum(open, closed, blocked), $k: int 1..200 }
  limit rows 10000
}
```

```yaml
capabilities:
  ticket_explore:
    mode: submitted
    contract: ticket_explore
    caveat: { types: [Ticket, Comment] }
```

```http
POST /graphs/knowledge/capabilities/ticket_explore
{ "query": "query q($status: String) { match { $t: Ticket  $t.status == $status } return { count($t) as n } }",
  "params": { "status": "open" } }
```

The caller needs Cedar `submit` on the capability. The program is typechecked
against the caller's schema view, then checked against the contract: every
type and field it names is allowed, every logical operator it uses is
allowed, every parameter is declared and in its domain, the limit holds. The
check reads the checked query; it proves nothing about rows because the
caller's view already bounds them. The program then runs as an ordinary read
under the caller's view narrowed by the capability's caveat, and so it may
traverse and search like any read. A program outside the contract is refused
with `contract_violation` naming the clause; because the schema view
resolved names first, a hidden field is an unknown name before the contract
sees it.

A contract has no `require` clause that acts as a security boundary. A tool
author may still write `require` obligations to demand that programs under a
contract carry some predicate, as documentation and lint, and the checker
reports a violation like any other clause.

**Rules common to both modes.**

- Cedar gains two actions on a new resource kind, the capability: `invoke`
  and `submit`. A rule names the capabilities it covers:

  ```yaml
  rules:
    - id: reporting-count
      allow: { actors: { group: reporting }, actions: [invoke], capabilities: [global_ticket_count] }
    - id: acme-explore
      allow: { actors: { group: tenant-acme }, actions: [submit], capabilities: [ticket_explore] }
  ```

  Neither action implies the other, and a caller holding several
  capabilities uses one per request; the server never unions scopes, fields
  or operators across them.
- **Who may deploy.** Deploying a program whose scope reveals a securable's
  data changes who has access to it, which is the `owner` role's job. A
  deployment with a list scope needs `owner` on every listed securable; a
  scope of `all` needs a bypass actor; a scope of `caller` needs
  `config_manage` alone, as a stored query does today. The binding records
  the deployer, the approved revision and the authorizing policy digest.
  Redeploying the query is a security-relevant change for the same reason.
- **Parameter domains.** For a scope other than `caller`, every parameter
  declares a domain (an enum, a numeric range, `key_of <Type>` for an id the
  caller can already name, a length-limited string), a program may not
  compare a key property with a parameter that is not `key_of`, and the
  server logs every invocation's parameters for the capability's owner.
  Repeated parameterized calls over a scope the caller cannot otherwise read
  can isolate individuals; minimum group sizes do not prevent that, and
  per-recipient privacy budgets are later work.
- **Discovery.** `GET /graphs/{id}/capabilities` lists the capabilities Cedar
  allows the caller, with mode, parameter domains and contract, so an agent
  can plan without probing. An absent capability and a denied one answer
  alike (`capability_denied`).
- **Activation.** Contracts, programs, bindings and policy are one candidate
  deployment; a dangling reference, a program that fails its contract or an
  incompatible schema identity prevents activation. A request already
  admitted finishes under its captured bindings; later requests use the new
  ones.

### Agents acting for people: delegation

A delegated credential names the ***subject***, the person, and the
***actor***, the agent, and may carry ***caveats***: a list of securables, a
list of types, an action subset, a list of branches, an expiry. Caveats only
remove. The effective view is

```text
R(subject) ∩ R(actor) ∩ covered(caveats)
```

where a securable caveat covers each listed securable and each securable that
inherits from a covered one when every securable it requires is covered
too, so a summary that also derives from an uncovered source stays outside.
Cedar must allow the action, and any capability, for both identities; a
capability's own caveat narrows further; commit receipts and audit events
record both. Every composition is an intersection, so a chain of delegations
and capabilities only narrows. The one widening point in the whole system is
a deployed capability's declared scope, authorized at deployment by owners.

Keeping content away from agents needs no special tier: put agent principals
in a team and `@deny` that team the securables in question.

### Sharing outward: recipients, copies, vaults

A ***recipient*** is a principal of a principal type whose actor id an
outside party authenticates as. It is never in the internal group, so a
grant from the internal group never reaches it. A share's content is
whatever the recipient can read; that one definition is delivered three
ways:

| Mode | Recipient reads | Revocation | Audit |
|---|---|---|---|
| live | Our server, under the recipient's view | Immediate | Ours |
| hosted copy | A separate graph we host, kept current by a reconciler | Immediate; the copy is deleted | Ours |
| delivered copy | A separate graph in storage the recipient controls | Stops future updates | The recipient's |

Live shares inherit every side channel of live access (timing, resource
limits), so they are for trusted collaborators; outside parties get copies.
A copy is declared in cluster configuration with its source, recipient, mode
and target graph; its schema is the recipient's schema view, and ids are
preserved so rows map back. A recipient of a hosted copy may write on a
branch of the copy; the reconciler maps it to a proposal on the source,
checks that every changed row is writable by the recipient, and an owner
reviews and merges.

A ***vault*** is a compartment kept in its own graph or cluster instead of
by placement: for data where a defect in view enforcement would be
unacceptable, where retention differs, where search must not depend on rows
other compartments hold, or where the threat includes holders of storage
credentials. Securables are for the long tail of channels, folders, deals and
matters; vaults are for the few compartments that justify their own
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
state*** of a request is the applied policy digest, the capability bindings'
epoch, and the published versions of every ***access table*** (each group,
securable and principal type, each access edge type) on `main`. It is not
`main`'s head commit, which every unrelated write advances. A read on `main`
takes the authorization state and the data from one catalog image. A read on
another branch or a historical target captures the data first and `main`'s
authorization state after it, so any revocation published before the data
cut is applied. A write on `main` captures the state with its baselines, and
the access tables' versions become expected versions of the publication's
compare-and-swap, so a concurrent access change by any process fails the
publication and the attempt retries with fresh authority.

**One window is stated, not closed.** A write on a branch other than `main`
publishes on that branch's chain, which `main`'s access tables are not on.
Lance offers no compare-and-swap across two chains, and a process-local lock
would fence nothing across processes. Such a write is therefore authorized
against the state its attempt captured: a revocation on `main` during that
one attempt can miss it, bounded by the write deadline, landing rows only on
that branch, which every later read and the eventual merge re-check. This
RFC states the window rather than pretending to close it.

### Limits

The membership closure is bounded by `max_memberships` and a depth of 16,
the readable set by `max_securables`, an exact search by
`max_exact_search_rows`. Exceeding a bound refuses with
`view_limit_exceeded`; the engine never truncates a view or a search.
`|R(a)|` is the number of distinct audiences an actor belongs to: small for
channels and folders, up to 10^5 for a heavy mail user when every thread is
its own audience. Beyond `max_securables`, a cheaper encoding stores each
row's reader groups on the row and filters by overlap with the actor's
closure, as Elasticsearch and Azure AI Search do; it is designed and not
built, and its trigger is an open question. A capability adds one compile
per approved revision at deployment, one shape check per submitted program
at admission, and no per-row cost.

## Evaluating the design

### Does it express what companies need?

The standard catalogue, against the design. ✓ is direct; ◐ is a named
modeling step; ✗ is declined with a fallback.

| | Pattern | | How |
|---|---|---|---|
| 1 | Tenant isolation | ✓ | `@in(tenant)`; a policy grant per tenant |
| 2 | Folder and org hierarchies | ✓ | An `@inherits` chain; the edge's role caps what flows down |
| 3 | Groups in groups | ✓ | The `@member` closure |
| 4 | Sharing one item with one person | ✓ | The item or its container is a securable with a `@grant` from the principal |
| 5 | Ethical walls, blocklists, keeping agents out | ✓ | `@deny`; absolute for the subtree |
| 6 | Temporary access, embargoes | ✓ | `@starts` / `@expires`, exact at the instant |
| 7 | Need-to-know on several axes | ✓ | Several `@in` on one type, intersected at the row |
| 8 | Derived content visible to readers of all its sources | ✓ | `@inherits` one source, `@requires` every source |
| 9 | Hiding a column | ✓ | A fixed `@in` on the property; absent from the schema view |
| 10 | Private by default | ✓ | An insert without a placement goes to the personal securable |
| 11 | Read, write, administer as separate rights | ✓ | `viewer < editor < owner` |
| 12 | Staff versus outsiders | ✓ | The internal group; a recipient can never enter it |
| 13 | Delegation that only narrows | ✓ | `R(subject) ∩ R(actor) ∩ caveats` |
| 14 | Context conditions (MFA, network, time of day) | ✓ | Cedar context on the action; the view is context-free |
| 15 | Computation rights: count but not list, run this tool | ✓ | A capability |
| 16 | A rule over the row's own columns | ✓ | Computed placement |
| 17 | External sharing with revocation | ✓ | A recipient's view, delivered as a copy |
| 18 | A relationship confers access | ◐ | Declare the relationship edge type `@grant` or `@inherits`; a multi-edge relationship takes one `@inherits` edge per hop, mirrored by a connector |
| 19 | A rule over other rows (notes that mention my accounts; visible after three approvals) | ◐ | A deployed capability with a declared scope; the caller gets answers |
| 20 | Custom roles (commenter, approver) | ◐ | A deployed mutation capability per right |
| 21 | Ownership writes (edit your own comment) | ◐ | A deployed mutation capability with `author == $subject` |
| 22 | A column rule that varies by row | ◐ | A securable per row, as a compensation file per person |
| 23 | Placement on edges | ◐ | Edges are visible when both endpoints are, until `@in` on edge types (a later phase) |
| 24 | Branch-scoped and historical access | ◐ | Caveats narrow branches; history binds in a later phase |
| 25 | Aggregates with a privacy guarantee | ◐ | Parameter domains and invocation logs; budgets are later work |
| 26 | Masking (last four digits) | ✗ | A column is present or absent; store the masked form as its own placed column, or deploy a program |
| 27 | Write without read | ✗ | Roles form a chain; a drop box is a deployed mutation capability |

The frame: Zanzibar expresses union, intersection, exclusion and
tuple-to-userset over relations an operator names per type; the view algebra
is that with a fixed vocabulary and labels stored as columns. SQL row-level
security accepts any predicate and returns no proof and no index. Cedar
decides actions with context and never filters rows. This design takes the
closed algebra for data and gets predicates back only as programs, which
return answers.

### What it costs

| Choice | Gained | Given up | Why acceptable |
|---|---|---|---|
| Labels, not predicates, for data | Composition under any operator; a proof; an indexed read | Row rules that mention other rows or the caller | Those are programs; a company brain's audiences are its source ACLs' audiences, which are labels already |
| Three roles | Inheritance caps are well defined; write implies read; source roles map one to one | Commenter, approver and other action-shaped roles | Each is a deployed mutation capability; a per-type write floor stays available as the one chain-preserving extension if free-form writes at `viewer` are ever needed |
| Deny is absolute for a subtree | Walls mean what they say; one spelling | "Deny the folder but allow this one file" | That shape is a modeling error in a wall; restructure instead |
| Computed placement in the write path | Attribute rules become declarations | One expression evaluation per written row | Bounded by the row; no reads of other rows |
| Declared scopes on deployed capabilities | Approved answers whose reach is fixed by the program | A second place, besides the view, that defines data reach | Authorized at deployment by owners or bypass; parameter domains and logs bound differencing |
| Shape check for submitted programs | Any operator the language has | The property that every program states its own scope | The view states it; `view.restricted` says when it did |
| Access edges `main`-only | No revival of revoked access through a fork | Branch review for access changes | Proposals give review, with expiry on a moved head |
| Policy grants beside graph grants | Tenants without connectors | Two homes for grants | One rule decides the home; no fact lives in both |
| `$subject` and `$actor` as the only caller bindings | No trusted-attribute mechanism to deploy and audit | Attribute rules that are not rows | An attribute that decides access is an audience, so it is a group or a label |
| A new access dimension is a schema change | Placement columns are indexed, typed and part of the contract | Serialization behind the schema gate | A dimension is schema; live-branch schema apply is a separate proposal |

### What it declines, and the fallback

Masking and value transformation (deploy a program, or store the masked
form); write without read (a program); a fourth role (a program, or a
securable per right); a column rule that varies by row (a securable per row);
joining a capability's answer into a view inside one query (that reintroduces
the rows the view excludes into a query the caller wrote, which is the hole
the view exists to close); relaxing the schema change for a new dimension;
the regime past `max_securables` (designed, not built).

### Composition properties

- Closed under union and intersection; difference by group only, in one
  layer; monotone after denial; unique fixed point; cycles inert.
- Orthogonal dimensions compose at the row without a label per combination.
- Delegation and caveats compose by intersection; chains never widen.
- Derived content composes as an information-flow lattice.
- The two authorities compose by intersection, with the deployed scope as
  the one authorized exception.

## Alternatives considered

Each option from [The design space](#the-design-space), with the reason it
was not chosen as the whole answer.

**Views alone.** Everything a company brain needs, and no answer above
access except by letting a stored query run with its author's view. Then a
tool's disclosure changes when a person's access changes, withdrawing a
person's access can silently break every tool they wrote, and a caller still
needs `read` to invoke anything. The tools workload is unserved.

**Capabilities alone.** Everything a tool needs, and no data authority a
query can rely on without restating it. Audiences become attributes compared
by equality: groups, inheritance, walls, time windows and several audiences
on one row have no form; a connector mirroring Drive has nothing to write;
traversal, search and writes each need their own proof rules; a refusal
tells a person to rewrite their question. The company brain is unserved.

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

**Grants in one home only.** Only in policy: every audience change is a
deployment, and per-row audiences need principal rows anyway. Only in the
graph: a tenant needs a connector and principal rows for an audience that
never changes. Two homes, one rule, no fact in both.

**Separate graphs for everything.** The right answer for compartments that
must not share an engine, and the wrong one for a company brain, whose
audiences overlap. Vaults keep the option where it belongs.

**Doing nothing.** Graph × branch × action, with everything finer in
application code, where neither non-interference nor fixed disclosure can be
shown. The situations above are not served.

### Relationship to the earlier drafts

Two drafts preceded this one and each contributed half of it.

[Actor views and subgraph sharing](https://github.com/ModernRelay/omnigraph/pull/951)
(PR #951) defines the view: the vocabulary, the algebra, placement, the
engine's enforcement, restricted search, writes, delegation, sharing and
vaults, with the Lance evidence behind each. This RFC adopts it as the data
authority and changes four things: denial closes downward over inheritance;
a policy group may hold a role on a securable; computed placement moves from
its seventh phase into the row-view phase; and its definer-rights stored
queries, which let a query run with its author's view, are withdrawn in
favour of deployed capabilities. Its Lance citations are re-anchored from
11.0.0 to 13.0.0; every fact holds.

[GQP query contracts and Cedar grants](https://github.com/ModernRelay/omnigraph/pull/1197)
(PR #1197) defines the capability: a Cedar-granted binding of a contract and
a program revision, in deployed and submitted modes, with discovery and
deployment lifecycle. This RFC adopts it as the computation authority and
changes four things: the guard-preservation proofs, per-operator obligations
and single-binding fragment are withdrawn, because the view carries the
proof and the contract becomes a shape check; trusted attributes are
withdrawn except the caller's identities; deployed capabilities gain a
declared input scope and may write; submitted programs may traverse and
search.

Both remain drafts and keep ownership of their halves; each is amended as
above and cites this RFC for the division. This RFC supersedes neither.

## Invariants

- **7, physical acceleration is derived.** The row filter is a conjunct the
  plan declares; an index serves it or a scan does, and a missing index
  changes cost only. Restricted search's statistics are declared plan state.
- **9, query semantics are typed structures.** The view is a typed plan
  leaf, the capability a typed binding in the accepted execution object, the
  contract a typed obligation; none passes through a string, a transport
  flag or a side table.
- **10, trust at the boundary, enforced at the engine.** Bearer tokens
  resolve actors; the view and the capability are computed from that
  identity; every mutating engine entry point applies the write rules and
  the scope, so embedded and CLI-direct writers bypass neither authority.
- **11, bounded and observable.** Every bound refuses with a registered
  outcome; no partial result exists in either mode.
- **12, one source of truth.** Access facts are graph rows and edges or
  policy entries, with the rule for which stated; a view is derived per
  request and cached only per access version; a capability binding is
  deployment configuration. Nothing shadows either.

Deny-list items touched: none. The branch-write window is stated, not fenced
by a process-local lock; no queue, no inline rebuild, no string-built
semantics, no cloud-only path.

## Compatibility and reversibility

- **Schema.** The view declarations set the schema feature `actor-views`; a
  build without this RFC refuses to open such a graph. `@computed` is one
  more declaration under that feature.
- **Policy.** The bundle gains version 2 (`views`, `capabilities:` on rules,
  the `invoke` and `submit` actions). A version 1 bundle on an `actor-views`
  graph is refused at apply.
- **Configuration.** `capabilities:` and `contracts:` are cluster
  configuration beside `queries:`, applied by the same deployment.
- **Wire.** `POST /graphs/{id}/capabilities/{name}`,
  `GET /graphs/{id}/capabilities`, the `view` object on results, the new
  fields of `GET /graphs/{id}/access`, and four error codes
  (`capability_denied`, `contract_violation`, `parameter_domain`,
  `scope_unavailable`) beside the six the view adds (`view_restricted`,
  `view_write_denied`, `placement_required`, `delete_blocked`,
  `identity_binding_denied`, `view_limit_exceeded`). Graphs without the
  feature and callers without capabilities see no change.
- **Withdrawn surfaces.** Definer-rights stored queries, the GQP fragment
  and guard proofs, trusted attributes: none shipped, nothing deployed
  changes meaning.
- **Reversibility.** The division is a design rule, not a format; reverting
  it means choosing one half and losing the workload the other serves. The
  format decisions that are hard to reverse (placement columns, history
  binding, packed references) are owned by the actor views draft and gated
  by its evidence.

## Evidence and tests

The mechanism's owners are named by the two drafts: the actor views draft
names `policy_engine_chassis.rs`, `writes.rs`, the planner's
`validate/tests.rs`, `search.rs`, `lance_surface_guards.rs` and the server's
`auth_policy` suite; the GQP draft names its parser, checker, deployment and
`stored_queries` owners. This RFC adds the evidence for the division itself:

- **The ledger as a corpus.** One `.gqt` case per row of the ledger under
  `crates/omnigraph-gqt/cases/access/`, each stating the pattern, the schema
  and policy that express it, and the rows each actor sees; ✗ rows show the
  fallback. The corpus is the proof of G11 and the regression guard for every
  later change to either algebra.
- **Denial closure.** A direct grant on a file inside a denied folder is
  hidden; a direct grant on a sibling outside it is not.
- **Policy grants.** A policy group's role equals a graph grant's; the two
  homes holding different securables leak into neither.
- **Computed placement.** Insert, an update that moves the row, a writer who
  assigns the column, a load file that carries it, a changed expression at
  schema apply rewriting rows in one publication (`schema_apply.rs`).
- **Capability scope.** Server `auth_policy` and `stored_queries` cases: a
  deployed capability over `all` answered for a caller with no `read`; the
  same caller refused on an ordinary read; a submitted program refused by
  shape and accepted when rewritten; a submitted program that traverses and
  searches answering as the ordinary read would; a deployed mutation under
  `author == $subject` with a delegated credential; a deployment refused when
  the deployer lacks `owner` on a scoped securable.
- **Replay.** `engine_v2_plan_replay.rs` gains a capability-bound plan whose
  replay refuses when the binding, its scope or the subject changed.
- **Surfaces.** The new codes join the `ErrorCode` registry; the routes and
  MCP tools join the operation table of
  [Graph access and cross-process writes](2026-10-16-graph-access-serving-and-cross-process-writes.md);
  `openapi.json` is regenerated and the conventions test of
  [HTTP API conventions](2026-10-19-http-api-conventions.md) covers the
  routes.
- **Lance.** The actor views draft's anchors re-read at 13.0.0, one row per
  fact.

## Rollout

Each stop is independently safe.

1. **Shared foundation.** Engine read doors that take an actor; the typed
   scope obligation in plan acceptance; diagnostics that do not confirm
   hidden names; the capability binding in the accepted execution object and
   in replay bindings.
2. **Deployed capabilities.** The Cedar resource, invocation without `read`,
   parameter domains, input scopes with their deployment authority,
   discovery. Needs no contract checker and no securables. Query-only access
   for agent tools ships here.
3. **Type and column views, with policy grants.** The full access model,
   fixed placements, the schema view, denial closure, `views.grants`.
4. **Row views, computed placement, submitted capabilities.** Per-row
   placement, the row filter, masked traversal, restricted search, every read
   surface; `@computed`; the shape contract.
5. **Writes under a view, then the later phases** as the actor views draft
   orders them: delegation, incremental maintenance, packed references, edge
   placement, search acceleration, history and feeds, sharing (reduced to
   recipients, copies and write-back), vaults.

`implementation` moves to `in-progress` at stop 1 and to `partial` at
stop 4; the owning drafts advance their own fields as their phases land.

## Unresolved questions

1. **Re-authorization of a declared scope.** Whether a deployed capability's
   scope is re-checked against its deployer's `owner` roles at every bundle
   re-apply or only at deployment. Re-checking makes a tool fail when its
   deployer loses access, the liability definer queries had; not re-checking
   leaves a scope standing after its authorizer has gone.
2. **The subject binding under delegation.** Whether an ownership rule binds
   `$subject` alone, so an agent edits its user's comments, or may also name
   `$actor`.
3. **Spellings.** The `invoke` and `submit` actions, `capabilities:` on a
   rule and in configuration, the contract syntax and the routes are
   proposed here and need review against the policy schema and the HTTP
   conventions before acceptance.
4. **Computed placement syntax**, and the trigger for the item-side reader
   encoding past `max_securables`, both carried from the actor views draft.

## Decision log

- **2026-10-30.** Drafted from first principles for contributors, from the
  comparison of PR #951 at `d700c334` and PR #1197 at `6d7217a1` against the
  code at `118161dd` and Lance 13.0.0. Records the goals, the design space,
  the division, the design, the ledger, the costs and the amendments each
  draft needs.

## Appendix: implementation notes for agents

For whoever implements a stop of the rollout. Read the invariants and the
Lance guide first; this appendix says where each piece goes and what to
watch.

### Where each piece lives

| Piece | Crate and place | Notes |
|---|---|---|
| Schema annotations (`@principal`, `@group`, `@member`, `@securable`, `@in`, `@computed`, `@grant`, `@inherits`, `@requires`, `@deny`, `@starts`, `@expires`) | `omnigraph-compiler` schema parsing and catalog; the `actor-views` schema feature | Validation rules are in [What: securables and placement](#what-securables-and-placement) and the write rules. Each `@in(p)` property gets a BTREE index declaration. |
| View computation (`C0`, `D`, `C`, the fixed point) | `omnigraph-engine`, a new `engine/view` module reading the access tables of the captured authorization state | Cache per (access version, actor); invalidate at the earliest `@starts`/`@expires` boundary. Bounds from `views.limits`. |
| Authorization state capture | `crates/omnigraph/src/db/omnigraph.rs` beside `load_accepted_view_with_schema_gate_held` | Reads on `main`: one catalog image. Other targets: data first, then `main`'s access pins. Writes on `main`: access pins as expected versions of the `__manifest` compare-and-swap. |
| Schema view | The compiler's catalog construction, parameterized by the readable set | Hidden names must produce the same diagnostic bytes as undeclared ones; test by diffing error payloads. |
| Row filter (`ViewSet` leaf) | `omnigraph-planner`: a leaf in the IR and a check in `crates/omnigraph-planner/src/validate/invariants.rs` (`Requirements::check`) that every placed scan carries it; `AcceptInput` in `validate/mod.rs` takes the scope | The leaf carries the readable set's digest, never the list; the list is bound at execution. Replay compares the digest. |
| Scan pushdown | `crates/omnigraph/src/engine/scan.rs` | The conjunct becomes an `IN` list on the placement column; Lance pushes it through `visit_in_list` to the BTREE/bitmap index. |
| Restricted search | `crates/omnigraph/src/engine/scan.rs` (the prefilter decision at line 268 and `Eligibility` in `crates/omnigraph-planner/src/optimizer.rs`) and the search ladder in `engine/search.rs` | bm25: `FlatMatchQueryExec` with a scorer built from visible-row statistics; `nearest`: exact over visible vectors (`use_index(false)` path). Statistics are declared plan state, not a cache the plan trusts. |
| Write rules | `crates/omnigraph/src/exec/mutation.rs` (`resolve_assignments`), `crates/omnigraph/src/validate/` (kernels such as `card_violations`), `crates/omnigraph/src/exec/staging.rs` (`commit_all`) | Computed placement is evaluated after assignments and before validation. Constraint disclosures are the only facts a refusal may carry; audit every message. |
| Policy bundle v2 (`views`, `capabilities:` on rules, `invoke`/`submit`) | `omnigraph-policy` (Cedar schema: a capability resource kind), `omnigraph-cluster` (bundle parsing and apply validation) | `matched_rule_id` is `.min()` over matched ids in `crates/omnigraph-policy/src/lib.rs`; it stays diagnostic. |
| Capability bindings and contracts | `omnigraph-cluster` configuration beside `queries:`; contract parsing in `omnigraph-compiler` | A binding records query name, approved revision, scope, params, caveat, deployer, policy digest. Activation goes through the existing affected-request admission and drain. |
| Capability routes and MCP tools | `crates/omnigraph-server/src/handlers.rs` beside `server_invoke_query` (today double-gated on `invoke_query` plus the body's `read`/`change`); `omnigraph-mcp` tools calling the same handlers | `invoke` must *not* require `read`. Deployed: scope substituted for the view. Submitted: typecheck against the schema view, then the shape check, then an ordinary read. |
| `access` route | `GET /graphs/{graph_id}/access` exists (operation `access`) and today evaluates policy per action | Add memberships, readable/denied securables with roles, and held capabilities. |
| Error codes | The `ErrorCode` registry with HTTP status and `disposition` | Ten codes; all `effects: none`, `action: fix`. |
| Delegation | The delegated-credential work (ModernRelay/omnigraph#978): subject, actor, caveats | Cedar evaluated for both identities; receipts record both. |
| Sharing, vaults | Later phase; cluster configuration `shares:` and the reconciler | Not in the first stops. |

### Order of work, concretely

1. Thread the actor through the engine read doors and add the scope slot to
   `AcceptInput`; make acceptance refuse any plan shape that could drop a
   filter on a placed scan (today no scan is placed, so the check is
   vacuous but present). Add the binding to the replay envelope.
2. Capability resource in the Cedar schema; `capabilities:` in cluster
   config; the routes; scope substitution. Test with a graph that has no
   `actor-views` declarations: `scope: all` and `scope: caller` behave the
   same, which proves the plumbing before any view exists.
3. Schema annotations, identity-binding rules, the view computation with
   fixed placements only (no per-row `@in` yet), the schema view, policy
   bundle v2, denial closure, `views.grants`.
4. Per-row `@in`, the `ViewSet` leaf, pushdown, traversal masking,
   restricted search, `@computed`, the contract checker.
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
| Routes, Cedar, capabilities | `crates/omnigraph-server/tests/integration/auth_policy.rs`, `stored_queries.rs`, `openapi.rs` |
| Bundle parsing, capability bindings, activation | `omnigraph-cluster` in-source `tests.rs` and `admission.rs` |
| MCP parity | `crates/omnigraph-server/tests/integration/mcp.rs` |
| CLI surfaces | `crates/omnigraph-cli/tests/integration/cli_queries.rs`, `cli_cluster.rs` |

### Gotchas

- **Byte-identical diagnostics.** A hidden name and an undeclared name must
  produce the same error, disposition included. Build the schema view before
  the compiler sees the query; do not filter diagnostics afterwards.
- **`matched_rule_id` is diagnostic.** Never select a capability, a scope or
  a group by it.
- **Capture order.** Authorization must never be older than the data it
  protects. On `main`, one catalog image. Elsewhere, data first, then
  `main`'s access pins. Writes on `main` pin the access tables in the
  compare-and-swap. Do not "fix" the branch-write window with a lock.
- **The digest, not the list.** The plan leaf carries the readable set's
  digest; the list is bound at execution. A plan replayed under another view
  must refuse on the digest.
- **`invoke` without `read`.** The whole point of a deployed capability. The
  existing stored-query handler is double-gated; the capability handler is
  gated on the capability alone.
- **Scope substitution, not a second filter.** A deployed program with a
  scope gets the scope *in place of* the view in the same conjunct. Do not
  intersect it with the caller's view, or `scope: all` is meaningless.
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
- **Replay bindings.** Subject, actor, view digest, capability id, revision,
  scope digest. Refuse on any difference.
- **Generated contracts.** `openapi.json` regenerates through the server
  OpenAPI test (`OMNIGRAPH_UPDATE_OPENAPI=1` only when the drift is
  intentional); the HTTP conventions test must pass on the new routes; the
  error registry and the operation table are the owners of codes and names.
- **DST storage-cost golden.** Capturing the authorization state reads the
  access tables' pins; that is a change to what a read reads. Add labeled
  rows to `crates/omnigraph-dst/cost_table.txt` and explain each in the
  pull request.
- **Release notes.** Each user-visible step needs a `changelog.d` note.

### Reading list

[Actor views and subgraph sharing](https://github.com/ModernRelay/omnigraph/pull/951)
for the full mechanism of the view, with its evidence and its phases;
[GQP query contracts and Cedar grants](https://github.com/ModernRelay/omnigraph/pull/1197)
for the capability lifecycle and its adversarial test plan;
[Identity credentials and applied policy authorization](2026-09-09-identity-credentials-and-applied-policy.md)
for how actors are resolved;
[Graph access and cross-process writes](2026-10-16-graph-access-serving-and-cross-process-writes.md)
for the operation table, dispositions and the `access` route;
[Unpublished graph commits](2026-10-10-unpublished-graph-commits.md) for
proposals; the Lance guide's scalar index, full-text and vector pages.
