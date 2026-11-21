# The contract

**Audience:** everyone who changes or reviews Omnigraph, coding agents included
**Authority:** what each operation promises its caller, and where the engine
differs today; the rules that keep the promises are in
[invariants](invariants.md), the mechanisms in the area guides, and the work
the promises serve in [the work](workload.md)
**Written for:** agents first. Every session loads this page and checks
designs against it, so precision comes before ease of reading. People start
at [For human readers](#for-human-readers)

## Editing this page

A database is an observable contract, implemented by physical mechanisms,
serving a distribution of work ([systems](systems.md)). This page is the
first of the three: the observable contract Omnigraph wants to guarantee,
in exact terms. [The work](workload.md) is the third. The mechanisms follow
from the two and live in the area guides, never here.

- **Write what a caller can observe.** Two tests for every sentence: could
  a caller tell the difference from outside, and would the sentence still
  be true if the storage engine were replaced? If not, it belongs in a
  guide. An outcome, a refusal and an order are observable; a wait is not,
  beyond the bounds of `E5`.
- **Use the exact term, once.** Where the literature names a property, the
  statement's *Named* line gives the term and its source.
  [Position](#position) only indexes it. Use a term only as its source
  defines it, and say at which scope it holds: one request, one branch, or
  across branches.
- **Say each thing once.** Before adding a statement, a table or a list,
  find the one that already owns the subject and change that. A derived
  view cites the statement and repeats none of it.
- **This contract is the target, and the code is today's behavior.** A
  statement says what Omnigraph is to guarantee, whether or not the code
  does yet. Where the code differs, a row under [Gaps](#gaps) says what
  happens today. To change what Omnigraph does, change the statement here
  first; the code then follows, and the gap row is removed when it has.
- **Do not weaken a statement to match the code.** A statement changes
  because the work is understood differently, never because the engine
  cannot reach it: an unreached statement is a gap.
- **Where the target is not yet chosen, say so.** A row under
  [Decisions](#decisions) names the question and what the code does
  meanwhile. Choosing the target means writing it as a statement and, if
  the code differs, adding its gap. There is no third list: what the work
  asks for is a statement with a gap, or a decision marked †.
- **Claim nothing as verified.** A statement earns `✓` only from a test
  that names it. Check a statement against the code before writing it.
- **Keep addresses stable.** Other documents cite `E2`, `write:3` or `D5`.
  Add numbers at the end; never reuse or renumber one.
- **Precision before readability.** A statement is exact and complete even when
  that makes it harder to read. [For human readers](#for-human-readers) is
  the one part written for a first reading, and it is written in ASD-STE100
  Simplified Technical English (the standard is free at asd-ste100.org):
  sentences of 25 words or fewer, active voice and simple tenses, `can`,
  `will` and `must` as the only modal verbs, one word for one meaning, and
  each term defined at its first use. It says nothing that a statement below
  does not, and it changes in the same pull request as the statement it
  summarizes.
- **Keep it short.** Every agent session loads this page. Evidence, history
  and reasoning about the workload go to [the work](workload.md).

## For human readers

This section is for a person who reads the contract for the first time. It
is written in ASD-STE100 Simplified Technical English. It adds no promise. If
this section and a statement in a later section are different, the statement
is correct.

Omnigraph is one graph that many agents read and change. A schema gives the
types and the rules of the graph. Each change is a commit, which is a new
version of the full graph. A branch is a line of commits. The history stays
until a person removes it.

Each operation obeys seven rules:

1. It reads one version of the graph. It does not read a mixture of versions
   (`E1`).
2. It makes all of its changes, or it makes none (`E2`).
3. It cannot break a rule that the schema gives (`E3`).
4. Its answer is true. A success tells you that the change is stored and
   that all later reads see it. A failure tells you which changes stayed
   (`E4`).
5. It gives its full answer, or it refuses. It does not give a part of an
   answer as the full answer (`E5`).
6. The server makes sure that the caller has permission (`E6`).
7. It changes only what its kind of operation can change. Only `cleanup`
   removes history. An index does not change an answer (`E7`).

Rules 2, 3 and 4, with the isolation that the list after this paragraph
describes, are the ACID properties. Atomicity is rule 2. Consistency is
rule 3, for the rules that the schema gives. Durability is rule 4. If the
storage does not answer, Omnigraph refuses the request: it selects
consistency before availability.

The promise changes with the distance between two operations:

- In one request, other operations have no effect on it. A request is the
  only transaction (a set of changes that is applied fully or not at all).
- On one branch, the result is the same as if the requests occurred one at a
  time, in their sequence in real time. The name of this property is strict
  serializability. A read sees each change that the system acknowledged
  before the read started. The name of this property is linearizability. To
  connect two requests, the second request must name the commit
  that the first request read. If it does not, a value that you read and
  then write can replace the change of a different writer. The name of this
  problem is a lost update.
- Between branches, nothing is shared until a merge. A merge compares the
  two branches one entity at a time. The merge refuses if the two branches
  changed the same entity differently. It also refuses if the result breaks
  the schema. It does not correct a conflict automatically. It examines what
  each branch wrote, not what each branch read. Thus the isolation between
  branches is weaker than snapshot isolation.

[Position](#position) compares the contract with each standard anomaly and
model, and gives the source of each name.

The sections that follow give the other rules for each kind of operation,
the pairs of operations that refuse each other, and the conditions that the
storage must satisfy. Two tables show where the software is today.
[Gaps](#gaps) lists the statements that the code does not obey at this time.
[Decisions](#decisions) lists the questions that have no chosen target.

## How this page is organized

This page is organized by operation. Operations fall into seven kinds, and
the page asks the same questions of each kind. Each statement is in plain
words, and where the literature has an exact name for it a *Named* line
gives the name and its source. [Position](#position) places the whole
contract against the standard anomalies and models. The page says what a caller can observe,
never how the engine does it.

## Addresses and marks

Every statement has an address, so a change can say which one it touches.

| Thing | Address | Example |
|---|---|---|
| A rule for every operation | `E` and a number | `E2` |
| A cell of the table of rules per kind | `kind.column` | `write.sees` |
| Two kinds of operation running at once | `kind+kind`, in the order of the kinds table | `read+reclaim` |
| A detail | `kind:` and a number | `merge:3` |
| A gap | `G` and a number | `G2` |
| An open decision | `D` and a number | `D1` |

A mark after a statement says how far to trust it.

| Mark | Meaning |
|---|---|
| none | Claimed. No test is named for it yet |
| `✓` | A named test holds it |
| `G2` | The code differs from this statement. The statement is the target; the gap row says what happens today |
| `D1` | The target is not yet chosen. The statement describes what the code does; the decision row says what is open |
| `?` | Nobody has stated it |

## Definitions

- **Graph:** a property graph: nodes and edges under one declared schema, at
  one storage root. Every promise on this page is about one graph.
- **Entity:** one node or edge, identified by its type and id. A `@key`
  derives the id from the key properties, so the same key is the same entity
  on every branch. An edge is directed from `src` to `dst`, has a type,
  properties and its own id; unkeyed edges of one type may join the same two
  nodes more than once, and a `@key` over the pair makes it one entity. An
  entity with no `@key` gets a new id unless its writer gives one, so the
  same unkeyed row written twice is two entities.
- **Value:** a property holds a value of its declared type, or null. A value
  reads back as it was written: an integer exactly, at every door, and a
  `DateTime` to the millisecond in UTC. A float that is not finite is refused
  on write. Strings and keys compare byte for byte, with no folding of case
  or Unicode form.
- **Schema:** the accepted `.pg`: types, properties and constraints
  (required, `@key`, `@unique`, `@range`, `@check`, enums, edge cardinality,
  and that both endpoints of an edge exist). A null in a `@unique` property
  exempts its entity from that constraint.
- **Commit:** an unchangeable version of the whole graph, with an id, its
  parent or parents, its branch, the time it was committed, its actor when
  known, and the task and reason its writer gave `G9`. The commit time is
  information only. When a fact held true in the world is modeled by the
  schema `D22`.
- **Branch:** a name for a line of commits. `main` always exists. A deleted
  name that is created again is a different branch.
- **Head:** the newest commit of a branch.
  - **Head at start:** the head when a read captures its view, after it is
    authorized. A commit published before that moment is seen; one published
    after it is not.
  - **Head at publish:** the head at the moment a change lands. A change
    never lands on a head other than the one it was checked against: if the
    head moved first, it is checked against the new head, or refused.
  - **Named head:** the caller says "only if the head is still commit X".
- **Proposal:** a commit stored on no branch (`as commit`), published later
  or dropped. A **snapshot** (`as unattached branch`) is a stored commit that
  is never published.
- **Request:** one call of one operation. It is the transaction: the only
  unit that is all-or-nothing and isolated. Nothing joins two requests except
  a named head (`write:3`).
- **Scope:** where a statement holds. There are three: within one request;
  on one branch, across requests; and across branches, where a merge joins
  them.
- **Door:** a way in: the embedded engine, the CLI, or the HTTP server with
  its MCP surface.
- **Physical state:** files, indexes, caches and layout. It is not part of
  the graph.

## Kinds of operation

An operation's kind is what it is allowed to change, which is the `changes`
column of [the rules for each kind](#rules-for-each-kind-of-operation).
Operation names are those of the shared inventory in
`crates/omnigraph-api-types/src/operations.rs`.

| Kind | Operations |
|---|---|
| `read` | `query`, `query invoke` of a stored read, `queries list`, `export`, `blob get`, `blob stat`, `schema show`, `schema plan`, `lint`, `snapshot`, `commit list`, `commit show`, `commit changes`, `diff`, `branch list`, `branch diff`, `branch merge base`, `changes poll`, `changes baseline` |
| `write` | `mutate`, `query invoke` of a stored mutation, `load`, `blob put`, `blob clear`, `commit publish`; a mutation written `as commit` or `as unattached branch` stores its commit on no branch |
| `merge` | `branch merge` |
| `schema` | `schema apply` |
| `branch` | `branch create`, `branch delete`, `commit drop`; claiming a branch (embedded engine only) |
| `maintain` | `optimize`, `index build`, `index rebuild`, `upgrade`, `schema upgrade-system-columns` |
| `reclaim` | `cleanup`; removing a graph from a cluster deployment |

Two operations are two steps:

- `load` onto a branch that does not exist yet: `branch`, then `write` `D14`.
- `branch merge` that also deletes its source: `merge`, then `branch`.

The steps are not all-or-nothing together. If a later step fails, the earlier
one stays, and the answer says so (`effects: partial`) `D12`.

## Rules for every operation

**E1. It sees one commit.** An operation reads the graph as of one commit,
every type together, for its whole run. It never sees part of another
operation's change.
*Why:* an agent decides from what it read, and a mixed view is a state that
never existed. *Kept by:* invariant 3.
*Named:* a snapshot read. With `E2`, one branch is a linearizable object
[HW90]: each read and each write takes effect at one moment between its
request and its answer. Requests on one branch are therefore strictly
serializable [P79].

**E2. It changes all or nothing.** An operation that changes the graph
publishes one commit or none. No statement, type or table becomes visible on
its own.
*Why:* an agent can stop at any point, and half a step misleads every other
agent. *Kept by:* invariants 2 and 4.
*Named:* atomicity [K17, ch. 7].

**E3. It leaves the graph valid.** Every constraint of the schema holds at
every commit, whichever door or writer made it. An operation that would break
one is refused before it publishes. Valid means the constraints the schema
declares: any other rule over several entities is the caller's, and
concurrent writes can break it (`write:3`, `merge:7`).
*Why:* the graph is shared, so one bad write is every agent's wrong premise.
*Kept by:* invariant 8.
*Named:* consistency in the ACID sense [K17, ch. 7], which a database can
hold only for the constraints it is told.

**E4. Its answer is true and final.** Success names the commit `D15`, or says
that nothing was published, and comes only after the commit is stored for good
and visible to every later read `G1`. A failure says what it left behind:
nothing, part, or unknown `D12`, and whether sending it again can succeed:
a moved head or contention may, a broken constraint or a denial cannot. A
write can carry a request identity its client chose: sent again with the same
identity, it applies at most once and returns the first answer `G7`. Without
one, an unknown outcome is settled by reading history for a mark the caller
put in its write (a named head, or a key or unique value it chose), never by
sending the request again unseen.
*Why:* agents act on answers with nobody watching, and they retry.
*Kept by:* invariants 5 and 11.
*Named:* durability [K17, ch. 7]: an acknowledged write survives the faults
in [Boundary](#boundary).

**E5. It is bounded.** It never presents a cut-off answer as complete; that
holds always. It returns its whole answer within declared limits or refuses,
and contention ends in a refusal after a bounded number of attempts; these
hold while the store answers. A server over its load refuses, and says when
to try again. When the store cannot be reached, every operation is refused:
none is served stale or queued.
*Why:* nobody is watching to notice a hung or shortened answer.
*Kept by:* invariant 11.
*Named:* the first part is a safety property, the second a liveness
property, which can only be promised under a condition [K17, ch. 8]. The
last chooses consistency over availability [K17, ch. 9].

**E6. It is checked.** The server resolves the caller from its token; a client
cannot name itself. The server checks every action against the installed
policy. The engine checks every write when a policy is installed, before the
first lasting effect, so a denied write changes nothing. A direct CLI write to
a cluster graph uses that cluster's policy. Reads through the engine or the
direct CLI are not checked `D9`, and a direct or embedded caller states its
own actor. A served graph with no policy denies every authenticated request,
unless the operator lets static tokens read it. An operator can also start a
server with no authentication. Both choices are logged at startup. A policy
change binds the requests admitted after it; a request already admitted
finishes under the policy it was admitted with. The CLI,
HTTP and MCP share one list of operations, one set of error codes and one
failure shape; the embedded engine returns its own error type.
*Why:* agents of different authority share one graph.
*Kept by:* invariant 10.

**E7. It stays in its kind.** An operation changes only what its kind may
change. Only `reclaim` removes published commits `D1`. Only `maintain`
rewrites physical state, and it never changes an answer `G3`: a read says
what it wants, never how to find it.
*Why:* history is the agents' memory, and a search must mean the same
whatever has been built to speed it up. *Kept by:* invariants 7 and 12.
*Named:* data independence, in Codd's sense of indexing and ordering
independence [C70].

**Two strengths of promise.** `E1` to `E4`, `E7`, the never-skips part of
`read:5` and the refusals of `merge:3` are integrity: a violation is
permanent, and no retry or wait repairs it. The delay of the change feed,
the coverage of indexes and the freshness of values derived by a pipeline
are timeliness: they may lag and catch up. `read:2` is the one timeliness
this page promises.

## Rules for each kind of operation

| | sees | changes | refuses when | cost grows with |
|---|---|---|---|---|
| **read** | head at start, or a named commit | nothing | commit reclaimed; search index missing | rows touched |
| **write** | head at publish; a `.gq` mutation may require a named head `D3` `D4` | one commit on one branch; none when no value would change `G6` | constraint broken; named head moved | what changed |
| **merge** | two heads and their ancestor | one commit on the target, or none | sides disagree on an entity `D5`; constraint broken | what both sides changed |
| **schema** | head of `main` at publish | one commit on `main` that changes the schema, or none | stored data breaks a new rule; another branch is live `D6` | types changed |
| **branch** | a commit to start from | a name, never data `D1` | name taken | not the graph's size |
| **maintain** | all heads | physical state, never an answer `G3` | graph is held; the branch moved since it began | what is out of shape |
| **reclaim** | all branches | removes commits | consent missing; graph is held | history removed |

Not allowed (`E6`) and over a limit (`E5`) apply to every kind and are not
repeated in the table. The cost column restates invariant 11 for each kind;
the quantities behind it are in [the work](workload.md).

## Concurrent operations

Any two operations that run at the same time both take effect, in some
order, and each sees the other's change whole or not at all (`E1`, `E2`).
The exceptions are the pairs below, where one side is refused. A merge acts
as a write on its target and reads its source at the start.

One operation may wait for another. How long is not promised beyond `E5`,
and where a wait is long today is in `D7` and `D8`.

| Pair | One side is refused when |
|---|---|
| `read+schema`, `read+branch`, `read+maintain`, `read+reclaim` | An export or a `changes baseline` runs beside a schema apply, a branch create or delete, `optimize` or `cleanup`: each refuses the other. Two exports refuse each other `D7` |
| `read+reclaim` | The read names a commit that `cleanup` removes `G5` |
| `write+schema` | The write selects rows and was prepared on the old schema `D8` |
| `write+branch` | The write lands after its branch was deleted |
| `write+maintain` | A write lands on the branch before an `index build` finishes: the build is refused |
| `write+reclaim` | `main` moved before `cleanup` began: the `cleanup` is refused |
| `schema+branch` | Another branch is live: the apply is refused `D6` |
| `schema+maintain` | A schema apply lands first: the maintenance may be refused |
| `branch+branch` | Both create the same name: one wins |
| `maintain+maintain` | A second `optimize` or `index build` is started without the server |
| `maintain+reclaim` | `optimize` published first: the `cleanup` is refused |
| `reclaim+reclaim` | Two removals of one graph: one wins |

This holds for requests to one server, or within one embedding program.
Across two, see `G2`.

## Rules specific to one kind

Statements that only one kind needs. Where a guide already owns the topic,
the guide has the full rules and this page keeps the promise.

### read

1. `read:1` A result is a bag of rows, or a set under `return distinct`. Rows
   have no order unless the query gives one with `order` or a ranking. Under
   a sort, ties break by the ids of the query's bindings `D10`. A hop range
   is bounded and returns each reachable pair of endpoints once, however
   many paths join them; a single hop returns one row for each edge `D25`.
2. `read:2` On one branch, a read sees every commit acknowledged before the
   read began, whichever process answers and however the caller learned of
   the commit. So a client's requests see its own writes and never go back
   in time. A client that needs a fixed view names a commit. Exception: a
   process that claimed a branch reads it without checking the head, so once
   another process replaces it its reads are stale, and a client can see an
   older state than it saw before.
   *Named:* recency, the real-time part of linearizability [HW90]. It
   implies the four session guarantees [TDP94] (read-your-writes, monotonic
   reads, monotonic writes, writes-follow-reads) with no session token.
3. `read:3` A past state is read by commit id, through today's schema. The
   read is refused when a property it asks for was added, renamed, dropped
   or created again since that commit `D18`.
4. `read:4` `nearest` applies the query's filters before it ranks, and says
   whether it searched everything (`exhaustive`); only a result that is not
   exhaustive can miss a nearer row. Its distance is exact, so it compares
   across commits. A full-text search is refused when the property is not
   yet searchable `D25`.
5. `read:5` The change feed delivers a branch's own commits in order from a
   cursor the caller holds and never skips one. A merge commit carries what
   it merged, and the merged branch's own commits do not appear. A feed can
   start at any retained commit of that history. A consumer sees each commit
   at least once, and a commit delivered again is the same block with the
   same commit id, so a consumer that stores the commit id or the cursor
   with its effect applies each commit once. Two branches' feeds have no
   order between them. Reclaimed history, a schema change and a cursor from
   an earlier branch of the same name are refusals, never silent gaps. A
   change is the net change of its commit against its first parent: the
   entity after an insert, before and after an update, before a delete.
6. `read:6` Any two commits can be compared, except across a schema change
   over tables that hold rows, which is refused.
7. `read:7` `changes baseline` returns an exact snapshot of one commit and a
   cursor that resumes the feed with the first commit after it. A consumer
   installs the snapshot before it saves the cursor.
8. `read:8` A consumer can wait on the feed for the next commit after its
   cursor that matches a filter: by type, by operation, and by a condition
   over the changed entities. A read names the commit it saw and a wait
   starts after it, so the two join without a gap. How soon after its
   acknowledgement a commit is delivered is timeliness, not a bound `G10`.
9. `read:9` A read can search the schema: the names and descriptions of its
   types and properties, by word and by meaning `G12`.

10. `read:10` A comparison with null is neither true nor false, and a filter
    keeps only the rows that are true. Null sorts first ascending and last
    descending. `distinct` and grouping treat nulls as equal. An aggregate
    skips nulls; over no rows `count` is 0 and every other aggregate is
    null `D25`.
11. `read:11` A read at a named commit returns the same rows every time,
    whatever maintenance ran in between `G14`. Two values come from outside
    the commit: `now()`, which is one clock reading for the whole request,
    and the vector of a `nearest` given text, which the embedding provider
    computes during the read.
12. `read:12` A listing or comparison returned in pages answers every page
    from the view its first page fixed, whatever its branches did since. A
    page whose history was reclaimed is refused, never answered from
    another view.

Full rules: [queries](../user/queries/index.md),
[traversal](../user/queries/traversal.md),
[search](../user/search/index.md),
[changes](../user/branching/changes.md).

### write

1. `write:1` Inside one mutation, a later insert or update sees the earlier
   statements' rows. Its delete statements all read the starting commit and
   remove everything they match. A mutation holds inserts and updates, or
   deletes, never both. An assigned value is a constant for the request (a
   literal, a parameter or `now()`) and cannot read the stored value, so an
   increment is a read followed by a write with a named head `D25`. So a
   writer that records a position (a sync's
   place in its source) can commit it with its inserts and updates, and
   needs a second commit for deletes.
2. `write:2` A strict insert never becomes an upsert. An insert into a `@key`
   type is an upsert by design. A key cannot be updated: changing it deletes
   one entity and inserts another `D25`.
3. `write:3` A write with a named head applies only if that commit is still
   the head; the comparison is made at publication with the current head,
   never with the view the write was prepared on. Without one, a write
   lands on whatever the head is when it publishes: a value read in one
   request and written back in another can then overwrite a commit made in
   between (a lost update), which cannot happen inside one mutation.
   `blob put` and `blob clear` can require the cell's ETag instead, and
   `commit publish` always requires the commit the proposal was built on.
   *Named:* compare-and-set on the branch head [K17, ch. 7]: the caller's
   read and write become one transaction, or nothing is written.
4. `write:4` A revert publishes the inverse of one commit or one merge as a
   new commit, and leaves every other change in place. It is refused where a
   later commit depends on what it would undo `G8`.
5. `write:5` A `load` has a mode. `append` refuses an id that exists.
   `merge` upserts by id, and the last duplicate in the input wins.
   `overwrite` makes each type in the batch exactly the batch's rows, so it
   removes the entities the batch omits; types not in the batch are
   unchanged. `merge` and `overwrite` are safe to send again; `append` is
   refused the second time `D25`.
6. `write:6` An edge whose `src` or `dst` does not exist is refused before
   publication, at every door. Deleting a node in a mutation deletes its
   edges in both directions in the same commit. A `load` that would leave a
   kept edge without an endpoint is refused.
7. `write:7` A write the server admitted is published even if its caller
   disconnects or stops waiting. A caller cannot cancel it, and nothing
   bounds how much later it lands. A caller that stopped waiting holds an
   unknown outcome (`E4`).

Full rules: [writes](writes.md), [mutations](../user/mutations/index.md).

### merge

1. `merge:1` A merge compares source and target with one common ancestor,
   entity by entity. The source is unchanged, and only the target head moves.
   *Named:* a state-based, keyed three-way merge [KKP07]. Two commits are
   concurrent when neither is an ancestor of the other, so history is a
   total order on one branch and a partial order across branches, by
   ancestry and never by a clock.
2. `merge:2` The same change on both sides is kept once, deletes included.
   Different changes to one entity are a conflict: `divergent_insert`,
   `divergent_update`, `delete_vs_update`. Changes to different entities
   never conflict, unless together they break a constraint.
3. `merge:3` A merged result that breaks a constraint is refused:
   `orphan_edge`, `unique_violation`, `cardinality_violation`,
   `value_constraint_violation`. A value is therefore unique on the branch
   that wrote it, and final only once merged: a claim made on a task branch
   is not exclusive until it reaches the shared branch `D19`.
   *Named:* invariant confluence [BFF15] says which constraints a merge of
   two valid branches can break: uniqueness of chosen values, an edge's
   endpoints when a node is deleted, and cardinality bounds. A constraint
   on one entity's own values cannot break while an entity is taken whole
   from one side.
4. `merge:4` Conflicts, orphan edges and uniqueness violations name the
   entity. Cardinality and value violations name the type; a cardinality
   message names the source node in its text, with no entity field.
5. `merge:5` Nothing is resolved automatically, and there is no "ours" or
   "theirs" rule. A conflict is resolved by writing to a branch and merging
   again.
6. `merge:6` The common ancestor is picked by a fixed rule, so the same merge
   gives the same result. A merge over its declared size limits is refused.
7. `merge:7` A merge checks what was written, not what was read `D20`. Two
   branches that each raised a counter from 3 to 4 merge to 4. To count
   across branches, record each increment as its own entity and count them
   in a read: both branches' entities survive the merge.
   *Named:* across branches, weaker than snapshot isolation [BBG95]: a merge
   has no read set, and identical changes converge.
   [Position](#position) lists what that admits.

Full rules: [merge](../user/branching/merge.md).

### schema

1. `schema:1` A rename keeps a type's or property's identity and history. A
   drop followed by an add of the same name is a new thing.
2. `schema:2` Apply checks stored data against each constraint it adds and
   refuses the ones the data breaks.
3. `schema:3` Apply admits: a new type; a new optional property, which
   existing entities read as null; a rename; a dropped type or property; a
   new `@index`; a new `@unique` on a node type; a wider enum or
   cardinality; and a property made required when no stored value is null.
   It refuses: a new required property; a changed property type; a `@key`
   added or dropped; changed edge endpoints; and a tighter cardinality.

Full rules: [schema](../user/schema/index.md).

### branch

1. `branch:1` A branch starts at a head or at a published commit of a live
   branch. Creating it copies no data.
2. `branch:2` A proposal becomes unreadable when it is dropped or when it
   expires. It expires when its target's head moves past the commit it was
   built on, or when its target branch is deleted or created again.
3. `branch:3` A branch listing says, for each live branch, who owns it, for
   what task, and which entities it has changed `G11`.
4. `branch:4` A branch create, a branch delete and a `commit drop` are seen
   by every request that begins after their acknowledgement, as a commit is
   (`read:2`).

### maintain

1. `maintain:1` Maintenance runs only when asked. Nothing runs it in the
   background.
2. `maintain:2` On a served cluster, `optimize` runs only through the
   server, as a recorded operation the operator enables. `index build`,
   `index rebuild` and `cleanup` are refused while the server holds the
   cluster, and `upgrade` refuses any graph inside a cluster.
3. `maintain:3` Between maintenance runs the cost of a read can grow with the
   writes since the last `optimize`, and storage grows with retained commits
   until `cleanup`. Neither is bounded by the engine `D17`.

### reclaim

1. `reclaim:1` `cleanup` removes commits outside the keep rule (`--keep`,
   `--older-than`). It never removes a live head, a commit needed as a merge
   ancestor, or a proposal or snapshot that has not expired. A change-feed cursor retains nothing: a
   consumer behind the keep rule is refused and installs a new baseline.
2. `reclaim:2` `cleanup` asks for consent in the CLI only.
3. `reclaim:3` Removing a graph from a cluster deployment deletes its whole
   root.
4. `reclaim:4` A deleted value's bytes are gone only after `cleanup` and a
   maintenance run that rewrites them `D11`. There is no way to remove one
   value from retained history: `cleanup` removes whole commits, so a value
   in a live head, a merge ancestor or a live proposal stays, and so does
   any copy the operator's storage versioning or backups hold.

## Position

Where this contract stands against the standard anomalies and models. Both
tables are derived: the statements they cite own the promise and its exact
term, and where a row and a statement disagree, the statement holds.

### Anomalies

Each anomaly at the three scopes. The cells follow from `E1`, `E2`,
`write:3`, `merge:2`, `merge:3` and `merge:7`.

| Anomaly | What it is | One request | One branch, across requests | Across branches |
|---|---|---|---|---|
| Dirty write | Overwriting a change that is not yet committed | prevented | prevented | prevented |
| Dirty read | Seeing a change that is not yet committed | prevented | prevented | A reader that names another branch sees its unmerged commits; a reader of the target never does |
| Read skew | Two reads of one transaction see different commits | prevented | admitted, unless both reads name one commit | does not arise |
| Lost update | Two writers read a value and both write it back; one change is lost | prevented | admitted without a named head | admitted when both sides wrote the same value; different values are a conflict |
| Write skew | Two writers each read, then write different things, and together break a rule neither broke | prevented | admitted without a named head | admitted, unless the rule is a declared constraint |
| Phantom | A write changes the result of a search another transaction relied on | prevented | admitted without a named head | admitted, unless a declared constraint catches it |
| Read-only anomaly | A reader sees a state no serial order of the writers produces | prevented | prevented | admitted: a reader of the target between two merges |

Sources: [BBG95] for the first six, [FLO05] for the last two as they arise
under snapshot isolation.

### Models

| Concept | Position | Stated in |
|---|---|---|
| Data model | A property graph with a declared schema, checked on write | Definitions, `E3` |
| Queries | Declarative; three-valued logic over null; repeatable at a commit | `E7`, `read:10`, `read:11` |
| Transaction | One request | Definitions, `write:3` |
| Atomicity | One commit or none | `E2` |
| Isolation | Strictly serializable on one branch; weaker than snapshot isolation across branches | `E1`, `merge:7` |
| Consistency of reads | Linearizable on one branch; no order across branches | `E1`, `read:2` |
| Session guarantees | All four on one branch; none across branches until a merge | `read:2` |
| Ordering of history | Total on one branch, partial across branches | `merge:1` |
| Integrity | Every declared constraint at every commit, edge endpoints included, and no other rule | `E3`, `write:6`, `merge:3` |
| Durability | What the store survives | `E4`, `G1` |
| Availability | Consistency over availability | `E5` |
| Time | Commit time only | Definitions, `D22` |
| Delivery of changes | At least once, in commit order, with no gaps | `read:5`, `read:7` |
| Data independence | Physical state never changes an answer | `E7`, `G3` |

## Gaps

Where the code differs from this contract. The statement is the target; the
row says what happens today, and it is removed when the code reaches the
statement. "See" names the invariant or the design that bears on it.

| Gap | Where | Today | See |
|---|---|---|---|
| `G1` | `E4` | On a local filesystem an acknowledged commit survives a crash of the process, not necessarily a power loss. On an object store it is stored for good at success | Invariants deny-list: no acknowledgement before durable visibility |
| `G2` | Boundary, Processes | One writing server or program per cluster is supported. Across two, a `write`, `merge`, `schema` or `maintain` that loses the race for a head is checked against the new head or refused; branch create and delete, schema apply, dropping a proposal and `cleanup` are safe from one only. Writers through one server are served in arrival order; across two, a writer that keeps losing the race is refused after five attempts and nothing keeps it from starving. A writer that was displaced while it was stalled can no longer publish over a head that moved; it can still delete a branch or finish a `cleanup` | [Graph access and cross-process writes](../rfcs/2026-10-16-graph-access-serving-and-cross-process-writes.md), accepted, in progress |
| `G3` | `E7`, `maintain.changes` | Building a full-text index can change `bm25` scores, their order and a limited result | Invariant 7 |
| `G4` | Boundary, The store | Azure writes are a preview, and need a separate guard that admits one writer | [RFC 0029](../rfcs/0029-azure-blob-storage.md), accepted, in progress |
| `G5` | `read+reclaim` | Nothing coordinates `cleanup` with a running read. A read of a commit the keep rule drops is refused as reclaimed if the removal came first, and can fail with an untyped error if the removal lands while it runs. No test covers it | Invariant 3 |
| `G6` | `write.changes` | A write that changes no value still publishes a commit, and so does a load with no rows | No design yet |
| `G7` | `E4` | Graph writes carry no request identity. Only a write with a named head or a keyed upsert is safe to send again, the second at the cost of a commit. Recorded operations do carry one | No design yet |
| `G8` | `write:4` | There is no revert. Undo is a hand-written inverse commit, or a branch started at the earlier commit | No design yet |
| `G9` | Definitions, Commit | A commit records its actor only | No design yet |
| `G10` | `read:8` | The feed is polled. It filters by kind, type and operation, with no condition over the changed entities | No design yet |
| `G11` | `branch:3` | A branch listing shows head, origin and times, not owner, task or changed entities | No design yet |
| `G12` | `read:9` | Types and properties carry descriptions, and nothing searches them | No design yet |
| `G13` | Boundary, Processes | One server serves a cluster, and read-only CLI commands may read beside it. There is no read-only serving mode | [Graph access and cross-process writes](../rfcs/2026-10-16-graph-access-serving-and-cross-process-writes.md) |
| `G14` | `read:11`, `E7` | A `limit` without an `order` returns the first rows in physical order, which `optimize` can change. No test pins it | [GQ 3](https://github.com/ModernRelay/omnigraph/pull/1302), draft: one total order for every read |

## Decisions

Places where this contract has not yet chosen its target. Each row is the
question, with what the code does meanwhile. Choosing the target turns the
row into a statement, with a gap if the code differs.

A row marked † is one [the work](workload.md#what-the-work-requires) asks
for: the direction is known and its exact statement is not. A design moves
toward a † row, never away from it, and one that makes it harder to reach
says so in its contract line.

| Decision | Where | Today | Open question |
|---|---|---|---|
| `D1` | `E7`, `branch.changes` | Deleting a branch makes its commits that no live branch reaches through parents or merged parents unreadable by id at once, so a published commit is not final across a delete: a reader that acted on one cannot read it again. `commit show` can still return such a commit's record | Keep them readable by id until `cleanup`, or make delete a `reclaim`? |
| `D3` | `write.sees` | Only a `.gq` mutation can require a named head | Every write? |
| `D4` † | `write.sees` | A named head is refused by any commit in between, related or not | A condition as narrow as what the caller read, so that an unrelated commit does not refuse a decision. How narrow: an entity, a property, a search? |
| `D5` † | `merge.refuses` | Two agents that changed different properties of one entity conflict | Merge by property? It can build an entity neither branch wrote, so the merge would have to check per-entity constraints too |
| `D6` † | `schema.refuses`, `schema+branch` | Schema apply runs on `main` only, with no other branch live | A schema change made on a branch and merged, under the same review as data? The test would be that each side's schema reads the other's data: additions optional or defaulted, no identity reused |
| `D7` | `read+schema`, `read+branch`, `read+maintain` | On one server a read can wait behind schema apply, branch create or `schema upgrade-system-columns`. The refusals between an export or `changes baseline` and other kinds hold on one server only; nothing excludes them across servers | Reads never wait or refuse? |
| `D8` † | `write+schema`, `write+maintain`, `write+reclaim` | Writers wait while schema apply or `cleanup` runs, and writers on `main` wait while `optimize` runs, for a time a run started without the server does not bound | Reads and writes served without pause through maintenance? |
| `D9` | `E6` | Reads are checked at the server only. Policy scope stops at the branch class. A write needs no permission to read, so a refusal such as `key_conflict` can name an entity on a branch its caller cannot read. A denial is recorded in server logs only | Check reads at every door? Scope by type and property? Require read for a write that can reveal? Record denials where an operator can read them? |
| `D10` | `read:1` | Groups that tie on the sort keys come back in no stated order | One total order for every sorted read? |
| `D11` | `reclaim:4` | `optimize` does not rewrite every file that holds a deleted value | An erasure path? |
| `D12` | `E4`, Kinds | A stream that fails after its headers ends in a cut-off body. A merge that deletes its source can report success when the delete failed | Every failure typed? |
| `D14` | Kinds | A load onto a new branch is two steps | One commit? |
| `D15` | `E4` | The CLI's direct `schema apply` reports a version number and no commit id; the engine and a served deployment name the commit | Name the commit at every door? |
| `D16` | `E5` | A served request can raise four resource settings up to the larger of the server's value and the built-in default, so a budget an operator set below the default can be raised back to it | The server's value as the ceiling? |
| `D17` | `maintain:3` | Maintenance runs only when asked, so at a steady write rate the debt grows until an operator acts, and `optimize` then makes writers on `main` wait | Who runs maintenance when the work never stops: a reconciler started by a threshold, or only the operator? |
| `D18` | `read:3` | A past read is refused when a property it asks for was added or renamed since | Resolve a past read through identity: a renamed property under its new name, one added since as null, refusing only one created again? |
| `D19` † | `merge:3`, `write:3` | A claim is a write with a named head, so claimants on a busy branch refuse each other and unrelated writers refuse them | A claim on a task or entity that exactly one of several claimants wins, that tells the others who holds it, and that no unrelated commit refuses? One that expires needs a version its holder's later writes must name, not only an expiry |
| `D20` † | `merge:7` | A merge records and checks no read dependency, so a conclusion can merge on evidence that changed on the target | A branch that records what its conclusions depend on, and a merge that reports a dependency changed since the fork? |
| `D21` † | `merge.refuses` | A merge is gated by policy on who may merge, not by a check on what is merged | A check the target names and the branch must pass, run by a party other than its author? |
| `D22` † | Definitions, Commit | When a fact held true is an ordinary property, and nothing ranks, filters or supersedes by it | Valid time and supersession as a schema convention, or as a construct? |
| `D23` † | Boundary, Versions | Every process on a graph is upgraded together | Old and new releases serving one graph while one replaces the other? |
| `D24` † | Boundary, Failures covered | Nothing checks stored state against its history, so storage that lies is not detected | A check an operator can run? |
| `D25` | `read:1`, `read:4`, `read:10`, `write:1`, `write:2`, `write:5` | These statements describe the language as it is. [GQ 3](https://github.com/ModernRelay/omnigraph/pull/1302), a draft, changes each: a keyed `insert` becomes strict, beside `upsert` and `replace`; the load modes are renamed; an assignment can read the stored value; a write is a series of groups; a full-text search with no index is answered by a scan; one total order covers every read | Accept GQ 3? Then these statements change in the same pull request |
| `D26` | Boundary, Versions | No document promises that error codes, query syntax or the export encoding stay the same across releases | Which of them are stable, and from which release? |

## Boundary

### Assumptions

- **One graph.** No operation spans two graphs. A shallow clone reads its
  base's files. `init`, which creates a graph, is outside the kinds. Which
  graphs, policies and stored queries a cluster holds is owned by
  [control plane](control-plane.md), and so are server state reads such as
  health, `access` and operation status.
- **Doors.** A process that reads or writes the files directly is outside
  the contract; keeping it out is the operator's job.
- **The store.** A store where creating a name has exactly one winner and a
  read sees every completed write: a local filesystem, S3-compatible object
  storage, or Azure `G4`. That one-winner rule is the one agreement the
  whole contract rests on: it decides which commit is next on a branch. A
  store that lets two creations of one name both succeed, or whose read
  misses a completed write, breaks `E1`, `E2` and `write:3` with nothing to
  detect it; a shared network filesystem may be such a store. Omnigraph
  copies nothing to a second place and encrypts nothing at rest.
- **Failures covered.** A process may stop at any point. A request to the
  store may fail, land late or answer unclearly. A response may be lost.
  Storage that lies is not covered `D24`. Statements about what is seen and
  changed hold under any delay or pause; bounds (`E5`) hold only while the
  store answers. Two faults are not failures of the engine and are met
  another way. A bug in a release is the same in every process running it.
  A caller, an agent included, may make a valid but wrong change: it stays
  recoverable from history (`write:4`) until `cleanup` removes it. `cleanup`
  and removing a graph are the only operations that cannot be undone, and
  they are guarded by consent.
- **Time.** No promise about what is seen or in what order depends on a
  clock. Clocks are used for timestamps, for `now()` in a query, for
  `cleanup --older-than` and the seven-day orphan rule, and for a server's
  hold on its cluster. A skewed clock can make `cleanup --older-than` drop
  or keep a commit early, never a live head, a merge ancestor or a live
  proposal.
- **Processes.** Any number of servers and programs read and write one
  graph, and every statement on this page holds whichever of them answers
  `G2` `G13`. A reader added for capacity still reads the head at start: it
  never answers from a head it has not checked (`read:2`).
- **Versions.** An older release refuses a graph that a newer release has
  written to, before it reads anything, and never misreads it. So every
  process on a graph is upgraded together `D23`. A newer release opens the
  storage versions its support matrix lists and refuses the others, naming
  `upgrade` or a rebuild. A client and a server speak one HTTP contract
  version, and a mismatch is refused before anything runs. Nothing is
  promised across releases for error codes, query syntax or the export
  encoding `D26` ([versioning](versioning.md)).
- **Copies.** `export` followed by `load` reproduces the ids, values and
  edges of one branch at one commit. It keeps no history, no other branch
  and no schema identity. The supported backup is a copy of the root taken
  with writers stopped.
- **Outside the commit.** Stored vectors are computed by `embed` or by the
  client. A `nearest` given text calls the embedding provider during the
  read. An external Blob names an object outside the root, which a commit
  does not keep.

### Not promised

Deliberately outside the contract. Something wanted and not yet reached is a
gap or a decision, never a line here.

- One all-or-nothing change across two requests, two branches or two graphs.
  Work on a branch and merge it.
- A view across two branches at once. A merge, a two-sided diff and a branch
  create read each branch on its own.
- A read "as of" a time, or a stale read that trades freshness for speed.
- Moving a branch back to an earlier commit. Undo is a new commit
  (`write:4`), or a branch started at the earlier commit.
- Per-row policy.
- A recall figure for a `nearest` that did not search everything.
- A latency bound.
- A consistent backup from copying a root while writers run.

## Citing

A design, an RFC or a pull request for a non-trivial change carries one line:

```text
Contract: keeps E2, write.changes; closes G2; decides D1
```

| Verb | Meaning |
|---|---|
| `keeps` | The change touches this statement and it still holds |
| `closes` | The gap is gone. Delete its row; its number is not used again |
| `decides` | The target is chosen. The decision row becomes a statement, with a gap if the code differs; its number is not used again |
| `changes` | A statement changes. Change it on this page in the same pull request, before the code that follows it |

A cost claim also names the activity it is for in [the work](workload.md).

A test names the statement it holds in a comment on a line of its own,
`// contract: merge:2`, and several as `// contract: merge:2, E2`. A `.gqt`
case names it in a header line, `# notes: contract: merge:2`. A statement
gets its `✓` from such a test, never by hand:
`python3 scripts/check-contract-marks.py --update` writes the mark straight
after the statement's address, or at the end of its cell in the table of
rules per kind. Without the flag the script is the check that CI runs. It
refuses a mark that no test earns, a named statement without its mark, and
an address that this page does not define.

## Sources

For the terms on this page.

- [BBG95] Berenson, Bernstein, Gray, Melton, O'Neil and O'Neil, "A Critique
  of ANSI SQL Isolation Levels", SIGMOD 1995.
- [BFF15] Bailis, Fekete, Franklin, Ghodsi, Hellerstein and Stoica,
  "Coordination Avoidance in Database Systems", PVLDB 2015.
- [C70] Codd, "A Relational Model of Data for Large Shared Data Banks", CACM
  1970.
- [FLO05] Fekete, Liarokapis, O'Neil, O'Neil and Shasha, "Making Snapshot
  Isolation Serializable", TODS 2005.
- [HW90] Herlihy and Wing, "Linearizability: A Correctness Condition for
  Concurrent Objects", TOPLAS 1990.
- [K17] Kleppmann, "Designing Data-Intensive Applications", O'Reilly 2017.
- [KKP07] Khanna, Kunal and Pierce, "A Formal Investigation of Diff3",
  FSTTCS 2007.
- [P79] Papadimitriou, "The Serializability of Concurrent Database
  Updates", JACM 1979.
- [TDP94] Terry, Demers, Petersen, Spreitzer, Theimer and Welch, "Session
  Guarantees for Weakly Consistent Replicated Data", PDIS 1994.
