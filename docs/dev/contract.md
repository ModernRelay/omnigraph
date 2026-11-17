# The contract and the work

**Audience:** everyone who changes or reviews Omnigraph, coding agents included
**Authority:** the work Omnigraph is built for, the observable contract that
work requires, and where the engine stands against it today; the rules that
keep the contract are in [invariants](invariants.md), the mechanisms in the
area guides

A database is an observable contract, implemented by physical mechanisms,
serving a distribution of work ([systems](systems.md)). This page reasons in
that order, from the outside in:

1. **The work** ([W](#w-the-work-a-hypothesis)): a dated hypothesis of what
   long-horizon multi-agent systems do to a shared state.
2. **What the work requires** ([R](#r-what-the-work-requires)): the
   derivation from that work to a contract.
3. **The contract** (B, S, O, [G](#g-guarantees), I, A): the target, what
   Omnigraph aspires to promise because the work requires it.
4. **Today** ([today and the target](#today-and-the-target)): where current
   behavior falls short of the target, stated explicitly.

Mechanisms are out of scope here. Every change names the requirement it
serves and the clause it keeps or moves toward
([how](#how-to-use-this-page)).

## In one line

Omnigraph is the shared, typed world model of a multi-agent system: a
property graph whose every change is one atomic, durable, attributed **graph
commit**; whose every read is one fresh **snapshot** of one commit; whose
**branches** are isolated workspaces that integrate by three-way merge; and
whose **history** stays readable until an explicit cleanup. That is the
target; today it holds with the exceptions listed under
[today and the target](#today-and-the-target).

## W: the work, a hypothesis

**Hypothesis dated 2026-11-07.** Everything on this page is derived from one
sentence: the new work that long-horizon multi-agent systems introduce is
**multiple agents sharing a typed world model to continuously retrieve and
update context and coordinate work**. The work comes first. The contract
below is what that work requires, whether or not Omnigraph has it yet, and
the mechanisms follow the contract. Nothing here is measured at scale; each
quantity says what would revise it. Revise by editing these tables with the
evidence.

### What an agent does

An agent is a policy that acts over a shared model of some domain, in many
steps, toward goals it does not finish in one session. It holds no state of
its own worth keeping, so everything it must remember goes through the graph.
Its activities range over the following, each with its footprint; the
distribution is their sum.

| Activity | What the agent does | Footprint on the graph |
|---|---|---|
| Orient | Learns what exists and what the words mean: the schema, the types and their counts, what changed since it last looked, what others are doing now | Schema and catalogue reads; change feed from a cursor; branch and commit listings; cheap, at every session start and after every pause |
| Retrieve | Assembles the context for the next step, in two modes. Lookup: the entity by key, its neighborhood, the things like it, the passages that mention it, under filters on type, time and status. Analytical: how many, which most, grouped by what, over a type or a filtered slice, to size a problem, rank candidates or summarize before deciding | Lookup: selective reads combining key lookup, one to three hops, vector, lexical and fused ranking; tens to a few hundred rows; several per step; the most common read. Analytical: aggregates, group-bys, top-N and filtered scans over whole types, small results from wide reads; fewer than lookups but on the same critical path of a step |
| Acquire | Brings outside information in: fetches pages, documents, API results and files; imports a dataset it was handed or found, structured (tables, exports, API dumps) or unstructured (PDFs, documents, transcripts, mail); extracts entities, relations and passages from the unstructured content; embeds text | Keyed loads from 10^2 records to 10^7 for a whole source, unplanned and bursty, on the task's branch, as many bounded batches; Blob writes of thousands of source documents; extracted entities and passages linked to the Blob they came from; embedding columns; the usual way a new source enters before a sync exists |
| Author | Records what it learned, decided and did: observations, claims with evidence, decisions with reasons, tasks, plans, status, and links between things that already exist | Small attributed commits of one to tens of entities, many per task, on the task's branch, append-mostly; the most common write and the densest edges |
| Derive | Computes over what is there: classifies, scores, resolves duplicates, summarizes, infers edges, consolidates old memory into new | Reads wide across a type, writes many small updates or a batch per run; periodic, often as a pipeline |
| Coordinate | Shares work with other agents and people: claims a task, hands one off, checks whether something was already done, avoids writing over a concurrent change | Concurrent reads of shared state; conditional writes from the commit it read; one branch per task, so one writer per branch by construction of the work, not by a lock; merges into shared state; a conflict re-decided from a fresh read, not retried blind |
| Verify and correct | Checks what it holds against sources and constraints, finds drift and duplicates, fixes errors | Targeted reads by key and by external identity; corrections as updates or new facts; deletes rarely; constraint refusals as signal |
| Propose and review | Puts a change up for a person or another agent to accept; reads what a branch would change; approves, merges, or discards | Branch diffs and merge previews; a merge or a branch delete; unpublished commits that wait |
| Reflect and evaluate | Looks back: what was known at a time, what was decided and why, what happened after; compares a past state with now; evaluates its own earlier actions | Reads at a past commit; diffs across history; commit changes with actors; with orient, the second most common read |
| Maintain | Adjusts the model as the domain is understood better: adds a type or property, widens an enum, re-embeds, rebuilds coverage, prunes | Additive schema applies; index and layout maintenance; rare, reviewed destructive changes and cleanup |
| Report | Answers a person's question or produces a report or dashboard: the analytical mode of retrieve, with larger results and no step waiting on it | Aggregation over whole types; larger results; scheduled or occasional |

People do a subset by hand or through a UI, at a low rate: orient, retrieve,
author (a correction, an annotation), propose and review (the approvals that
make agent work trusted), and answer. Programs do acquire and derive on
schedules or events: an initial backfill per system of record (CRM,
ticketing, mail and calendar, documents, code hosting, warehouses), then
incremental idempotent keyed upserts under the source's external identity,
written to shared state directly, with the source's deletes propagated and
reruns for reconciliation; and they feed downstream systems from the ordered
change feed, exports and baselines. The graph sits beside those systems and
claims no authority over their records: a mirrored record is true as of its
sync and owned by its source, authored knowledge is owned by the graph, and
the two provenances stay distinguishable, because agents read both together
and write only the second, or propose changes to the first.

### Quantities

| Quantity | Hypothesis | Revise when |
|---|---|---|
| Actors | A few to tens of agents per graph at once, a few people, a handful of programs; many graphs per deployment rather than one | Hundreds of concurrent agents on one graph |
| Read capacity | Tens of agents at several retrieve reads a step is more read load than one process should carry, and it grows with agents while write load grows with tasks; reads are served by several processes | One process serves a graph's reads with headroom at the hypothesized actors |
| Mix | Reads outnumber writes by one to two orders of magnitude; most reads are lookups, and a real minority, perhaps one in ten, are analytical reads over whole types; authored commits dominate writes by count, acquisition and sync by rows; deletes are rare everywhere | Analytical reads become the majority; updates or deletes become common; result sets pass 10^4 rows |
| Volume | 10^4 to 10^7 entities per graph, set by the sources; 10 to 100 types; small scalar properties, text bodies, embeddings on a few types, Blobs for documents; agent batches of 10^2 to 10^5 records, whole-source imports and backfills to 10^7 records and 10^4 documents | 10^8 entities or 10^3 types; batches past 10^6; sync as streaming at thousands of events a second |
| Rate | 10^2 to 10^4 commits a day; tens of live branches with hundreds of commits each and one writer; incremental sync every few minutes; an additive schema change about weekly; history retained long and read as often as the present | Commit rate or branch count up an order; several writers on one branch; conflict retries common; destructive schema changes frequent; history read rarely |

| Targets | No latency is measured yet. As a starting hypothesis for a warm handle in the store's region: a retrieve read answers in about 100 ms at the median and under 1 s at the tail; a small commit publishes in under 1 s; one branch publishes its commits serially, a few a second at most, which the hypothesized rate needs on average at one every several seconds, with bursts on shared state from merges and syncs | Measured figures replace these; shared state needs sustained tens of commits a second |

### Against the usual categories

OLTP, OLAP, HTAP and LTAP each name a workload and usually imply a contract
and a mechanism ([systems](systems.md#appendix-a-terminology-expansion)).
Omnigraph is defined by its three parts above, not by a bundle, and matches
each bundle in some claims and not in others. The first three are categories
of the literature; LTAP is one vendor's architecture.

| Category | What it bundles | Where Omnigraph matches | Where it does not |
|---|---|---|---|
| OLTP [10] [11] | Work: many short transactions that read or update a few records by key, on current data; transaction throughput is the measure. Contract: atomic, isolated transactions, serializable or weaker; often interactive sessions, though some engines run stored procedures one at a time. Mechanism: row storage, B-tree indexes, a write-ahead log, updates in place under locks or multi-version rows | The footprint: small reads by key and small commits on a critical path, each seeing current data; serial execution per branch, as in the engines that run transactions one at a time [12] | The transaction is one request, not an interactive session. A branch's commits are serialized by a compare-and-swap on object storage, so a commit costs object-store round trips and one branch sustains a few commits a second, the rate Delta Lake reports for the same mechanism [15], not thousands. Write concurrency comes from branches |
| OLAP [10] [16] | Work: few complex ad hoc queries that scan, join and aggregate much historical data consolidated from many sources, loaded in batches; response time is the measure. Contract: read-mostly; a consistent snapshot of the last load, refreshed periodically; often a multidimensional model. Mechanism: star schemas, bitmap indexes, materialized views, parallel scans; later columnar storage and vectorized execution; in the cloud, compute apart from storage | The mechanism: columnar immutable files, batch loads, compute apart from storage; analytical reads are part of the work | The scale, the model and the lag: analytical reads cover one graph of up to 10^7 entities, not a warehouse consolidating many sources; there is no multidimensional model and no pre-aggregated summary; an analytical read sees the latest commit, the one a lookup sees, not the last load |
| HTAP [12] [13] [14] | Work: transactions and analytical queries on the same data at once, analytics on the latest committed data. Contract: transactions plus analytical reads of a fresh, consistent snapshot; the named requirements are freshness and performance isolation. Mechanism, two families: one copy with snapshots, or a row store plus a columnar replica kept consistent by replication | The mixed read work on one system over one copy, every read at a fresh snapshot: the single-copy family, not the replicated one | With no replica there is no replication lag. Isolation of lookups from analytical reads would come from separate stateless reader processes over the same files, not from separate representations, and it is unmeasured. The transactional half is weaker than OLTP as above, so HTAP's hard case, analytics beside a high write rate, is not this work |
| LTAP [17] | Databricks' term (June 2026), Lake Transactional/Analytical Processing: separate transactional and analytical engines over one durable copy in open table formats on object storage. Postgres writes with full transactions through a replicated log and a page server that transcodes pages to columns; analytical engines read the same files, scaled independently, at a log position plus the tail not yet written out; row pages are a cache. It unifies at the storage layer, not in one engine | The shape: one copy in an open format on object storage, one write path, readers that scale out, no export to a second system | The unit and the engines: one engine and a typed graph commit published by compare-and-swap, with no log and no row tier, against multi-statement Postgres transactions over a replicated log. Its branches are copies for experiments; Omnigraph's merge three-way. Retrieval over structure, vectors and text is a first-class read, which the term does not name |

What none of the four names, and what this work is mostly made of: isolation
by branch and merge instead of by lock or session; history read as often as
the present, where those systems keep it for rollback and reproduction;
ranked retrieval as the common read; and readers and writers that are
programs acting on what they just read. Two existing labels sit closer than
the four and are still only shorthand: data versioning systems, which share
the branch, commit and merge contract [9], and hybrid serving and analytical
processing, which names selective latency-bound reads beside analytics. Use
a category word only as shorthand for the row it points at; a design argues
from the clauses and the work, never from "it is an HTAP system".

### What we optimize for

In this order, and a later item never buys an earlier one: the clauses above
(correctness), then the fewest mechanisms that keep them (simplicity), then
cost on this work. On cost the direction is: retrieve reads first, lookups
and then analytical reads, because both sit on the critical path of an agent
step; then the cost of a small
commit and of a branch, because authoring and coordination are the common
writes; then history and change discovery; then bulk acquisition and sync.
Read capacity scales out by adding reader processes; write capacity on one
branch does not, and need not at the hypothesized rate. An analytical read
may cost more than a lookup but not in proportion to history, and a lookup
is never slowed to serve one. Warehouse-scale scans and single-branch write
throughput beyond the hypothesized rate are not optimized for, and a design
may trade them away.

What the hypothesis asks of the mechanisms, each with its cost owner in
[testing](testing.md#cost-tests-and-benchmarks): a read on a warm handle
costs a bounded number of storage requests independent of history, the
freshness check included, and a reader's warm state is disposable, so a new
reader is useful at once; a commit
costs the same whatever the history depth and live branch count; a branch is
free until written; a keyed upsert of an unchanged record costs little and
publishes nothing (not so today, see
[today and the target](#today-and-the-target)); change discovery costs the changes, not the tables; a
search reads the index, not the column. Wall time and memory are evidence in
the benchmark harness, not clauses.

## R: what the work requires

Each phrase of the sentence asks something of the shared state. This table
is the derivation; the clauses under [G](#g-guarantees) are its right-hand
column made precise. A clause is justified by a row here or it does not
belong in the contract.

| The work says | So the shared state must | Clause |
|---|---|---|
| **Multiple agents**, in parallel, restarting at any time | Let many act at once without corrupting each other: each step's writes land whole or not at all, a retried request is not applied twice, and every change names who made it | 1, 5, 7 |
| **Sharing** one model | Be one authoritative state that every agent reads and writes through the same rules, with access scoped to what each agent may see and change | 2, 7 |
| A **typed world model** | Enforce the model on every write, by every writer; let the model change as understanding grows, as reviewably as the data does | 4 |
| **Continuously**, over a long horizon | Stay available while it is written, maintained and evolved; keep what was known and when, so an agent can resume, reflect, audit and undo; never grow slower with age | 5, 6 |
| **Retrieve** context | Answer selective, combined-mode and analytical reads from one coherent, current view, at a latency a step can afford, from as many readers as the agents need | 2, 7 |
| **Update** context | Accept small frequent writes and whole-source imports; let an agent protect a decision against the state it was based on | 1, 2 |
| **Coordinate work** | Give agents long isolated workspaces that integrate without losing or silently overriding each other's work; let one agent learn of another's change without polling; make a claim on a task exclusive | 2, 3, 6 |

## B: the boundary

The subject is **one graph at one storage root**, observed through three doors
that share one semantics: the embedded engine, the CLI, and the HTTP server
with its MCP surface. The observer is a client of a door. A process that reads
or writes the root's files directly is outside the contract; excluding it is
an operator obligation. Every promise below is scoped to one graph: no read
or write spans two graphs, and graphs served together share only the server
and its cluster deployment state.

## S: state

- A graph is nodes and edges under **one accepted schema**: typed properties,
  nullability, keys, `@unique`, `@range`, `@check`, enums and edge
  cardinality. Blob and vector values are properties.
- An **entity** is identified by its type and id. A `@key` derives the id
  from the key properties, so the same key is the same entity on every
  branch; without a key the id is supplied or generated, and two unkeyed
  edges between one pair of nodes are two entities. An id deleted and
  inserted again is the same id, not a tracked lifetime, and a key cannot be
  updated: changing it is a delete of one entity and an insert of another.
- Types, properties and tables have **stable identity** that survives a
  supported rename and ends at a drop; a re-added name is a new identity.
  Names and paths are aliases, never identity.
- A **graph commit** is an immutable, id-addressed version of the whole graph
  with its parent or parents, branch, actor when known, and a writer-clock
  timestamp. Commits form a version graph, a directed acyclic graph per
  graph; a merge commit has exactly two parents. Time is transaction time
  only: there is no valid time. There is no checkout or working copy: a read
  names a branch head or a commit, and every write is a commit.
- A **branch** is a named history: a head commit and the commits reachable
  from it. `main` always exists. A deleted name and a recreated one are
  different branches.
- An **unpublished commit** (`as commit`) and an **unattached snapshot** are
  durable, id-addressed commits that no branch head shows.
- Indexes, caches, topology, fragment layout and compaction output are
  **derived physical state**, not part of S.

## O: operations

| Family | Operations |
|---|---|
| Read | `.gq` reads over one branch head or one commit id: patterns, traversal, filters, aggregation, `bm25`, `nearest`, `rrf`, with explain and an execution report. A result is a bag of rows (a set under `return distinct`), ordered only by `order` or a ranking; under a sort, ties break by the ids of the query's bindings, or by the projected columns under `return distinct` |
| Change | A `.gq` mutation query (several statements; inserts and updates, or deletes, never both), optionally conditional on a commit id or written `as commit`; a bounded load batch (`append`, `merge` or `overwrite`); a Blob cell put or clear, optionally conditional on its ETag |
| Branch | Create at a head or at a commit; delete; three-way merge, with preview and merge base; publish or drop an unpublished commit; claim a branch's writer epoch (embedded engine only) |
| History | Commit listing, one commit's changes, diff between any two points, branch review, change feed with a durable cursor, baseline snapshot, export |
| Schema | Apply a new `.pg` against `main`; lint and migration plan |
| Maintenance | `optimize` (layout and index coverage, history preserved; direct, or served as a recorded operation); index builds and the full-text rebuild; `cleanup` (removes history and lists foreign versions) |
| Access | Bearer authentication, Cedar policy, and `access` (HTTP, CLI and MCP): the caller's actor and allowed actions |

These are the operations that exist today. There is no revert (a target of
[clause 6](#6-history)), no restore of a branch to an earlier commit, and no
read "as of" a time: a past state is reached by commit id, and undone by a new
commit or a branch started at that commit.

## G: guarantees

Seven clauses. Each is a **target**: what the work requires of the shared
state, stated as the contract Omnigraph aspires to, with the reason first. A
target is not a description of the current engine. A clause states admitted
histories and outcomes, never how the engine gets there, and ends with its
owners: the guide that holds the mechanism and the tests that hold the
evidence.

**Today** is stated separately and completely in
[today and the target](#today-and-the-target): every place where current
behavior falls short of a clause, with what is wanted. A sentence of a clause
with no row there holds today, validated against the code on 2026-11-16. A
target that no accepted RFC has decided yet is marked *derived* in that
table: the work asks for it, and the design is open.

### 1. Atomicity

*Why: an agent's step writes several related things and the agent can stop
at any instruction; half a step in the shared model misleads every other
agent.*

- One mutation query, one load batch, one merge, one schema apply or one Blob
  write becomes visible as one graph commit or not at all. No statement,
  table or type is visible on its own.
- The commit is the transaction. Not promised: an interactive
  `BEGIN`/`ROLLBACK`, or anything atomic across two commits, two branches or
  two graphs. A multi-step change is a branch, made atomic by its merge.

Owners: [writes](writes.md), [recovery](recovery.md); `failpoints.rs`,
`detached_commit_matrix.rs`, the DST suite.

### 2. Isolation

*Why: an agent decides from what it read. The read must be one coherent,
current view; many agents must write at once; and a long task needs a
workspace others do not see until it is ready.*

- **One branch is one linearizable object.** Every read and every write on a
  branch is one atomic operation on the branch head, and takes effect at its
  publication [1]. A read names one branch head or one commit, resolves it
  once, and sees exactly that commit, every type together, for its whole
  life; at a head it returns the latest commit published before it answers,
  which includes every commit acknowledged before it began. A write takes
  effect against the head at its publication: that is the head it read, or
  it was prepared again on the newer head, or it is refused. The reads and
  commits of one branch are therefore strictly serializable in commit order
  [2]. A write whose outcome is unknown counts as still pending: it is
  concurrent with everything after it until history shows whether it landed.
- **Branches compose.** Each such operation touches one branch, so by
  locality [1] operations on different branches are linearizable together.
  A merge, a two-sided diff and a branch create read two branches and are
  not covered: two branches have no joint view.
- **Within a mutation** a later insert or update sees the earlier
  statements' pending rows; its delete statements read the same commit and
  remove the union of what they match.
- **Conditional writes.** A write that names the commit its caller read
  (`--if-commit`) is a compare-and-set on the branch head: it applies only
  if that commit is still the head, whatever changed in between, so the
  caller's read and write take effect as one transaction in the serial
  order, or not at all. Without it, a write is serialized where it lands,
  not where its caller last looked. An unpublished commit publishes under
  the same rule. The condition is as narrow as the decision: an agent names
  the commit, the entities or the query its decision depended on, so that an
  unrelated commit by another agent does not refuse it.
- **Readers and writers scale out.** A read learns the head from storage,
  never from another process. Any number of reader processes may serve one
  root; none coordinates with the writer or with another reader, and each is
  always fresh. Any number of writer processes may write one root: writers
  of different branches never contend, and every write, control writes
  included, is decided by the same rule whichever process issues it. Not
  promised: a stale read mode that trades freshness for latency.
- **Sessions.** On one branch a client's sequential requests get all four
  session guarantees [4]: read-your-writes, monotonic reads, monotonic
  writes and writes-follow-reads, on whichever process serves them, with no
  session token. They cover acknowledged writes. Across branches none holds
  until a merge. A client that needs a fixed view pins a commit id. Not
  promised: a read "at least as new as commit X" on another branch.
- **Across branches.** A merge resembles snapshot isolation [3]: each branch
  reads from its fork point and its own commits, and a merge certifies what
  was written, entity by entity, with no read set. It is weaker in two ways.
  An identical change made on both branches converges, so a lost update
  across branches is admitted: two branches that each read a counter at 3
  and write 4 merge to 4. And a branch's unmerged commits are readable by
  others, who may act on a state that is never merged. Write skew, write
  skew through a predicate read (both branches see no matching entity and
  each inserts one) and the read-only anomaly [5] are admitted unless a
  declared constraint catches them. The target closes the gap that matters
  for knowledge work, the stale premise: a branch can record what its
  conclusions depend on, and a merge reports a dependency that changed on
  the target since the fork, so a claim is not integrated on evidence that
  moved. Not promised: serializability across branches by default.

Owners: [writes](writes.md), [execution](execution.md),
[architecture](architecture.md#concurrency-and-support-boundary);
`writes.rs`, `integration/consistency.rs`, `integration/warm_read_cost.rs`,
the DST suite.

### 3. Merge

*Why: agents work in parallel for hours on overlapping parts of one model.
Integration must keep both agents' work where they did not disagree, and
stop for a decision where they did, never pick silently.*

- A merge is a state-based, keyed three-way merge [8]: it compares source
  and target against one merge base and publishes the result as one commit
  on the target, or nothing. The source is unchanged, and only the target
  head moves.
- Conflict detection is as fine as the disagreement. Two agents that
  changed different properties of one entity did not disagree, so both
  changes are kept, as in systems that merge by field or cell [9]; the
  merged entity is then checked against its constraints, since it is no
  longer one side's whole state. The same change on both sides converges,
  deletes on both sides included. Different values for one property, and a
  delete on one side against an update on the other, are entity conflicts:
  `divergent_insert`, `divergent_update`, `delete_vs_update`.
- Each entity's result depends only on its base, source and target states,
  so a merge is repeatable and changes to disjoint entities never conflict,
  except through a declared constraint that spans entities. A merged result
  that breaks one is refused as a constraint violation: `orphan_edge`,
  `unique_violation`, `cardinality_violation`,
  `value_constraint_violation`.
- Which constraints a merge of two valid branches can break follows from
  invariant confluence [6]. While a merge takes each entity whole from one
  side, it cannot break a per-entity value constraint under one schema; once
  it combines properties from both sides it can, which is the price of the
  finer merge and why the check runs. It can break uniqueness of chosen values, edge
  endpoints when a node is deleted, and cardinality bounds, so those are the
  checks a merge must run, and refusing the merge is the coordination they
  require.
- The merge base is a lowest common ancestor of the two heads, picked by a
  fixed rule (nearest both heads, then highest generation, then smallest
  commit id). After criss-cross merges there can be several; the rule makes
  the result repeatable, not independent of the choice.
- For two branches forked from one commit that have merged nothing since and
  conflict neither by entity nor by constraint, the merged entities are the
  same whichever merges first; commit ids, history shape and feed order
  differ. This is a property of this merge, not a convergence result:
  conflicting changes are refused, not resolved.
- Conflicts and violations are reported per entity, typed, and resolved by
  writing to a branch and merging again. Not promised: automatic resolution,
  a precedence rule (ours, theirs or last-writer-wins), or a merge of more
  than two heads.

Owners: [merge](../user/branching/merge.md), [merge internals](merge.md);
`integration/merge_truth_table.rs`, `integration/merge_fast_forward.rs`.

### 4. Integrity

*Why: the model is shared, so one agent's malformed write is every agent's
wrong premise; and the model itself changes as the domain is understood,
by the same agents, under the same review as data.*

- Every constraint of the accepted schema holds at every commit, on every
  door, whichever writer produced it. A violation is a typed refusal before
  publication. The engine never writes a placeholder, weakens a constraint or
  reinterprets unknown persisted state.
- A strict insert never becomes an upsert; an insert into a `@key` type is an
  upsert by design. Schema apply checks stored rows for the constraints it
  can add and refuses the rest.
- A change to the schema is a change like any other: an agent proposes it
  on a branch, it is reviewed with the data that depends on it, and it
  merges or is refused, while other branches stay live.
- Not promised: any invariant the schema does not declare.

Owners: [execution](execution.md#validation), [invariants](invariants.md);
`integration/validators.rs`, `schema_apply.rs`.

### 5. Durability and outcomes

*Why: agents act on answers without a person watching, and they retry. An
answer must be true, machine-readable and final, a retry must be harmless,
and the state must be there continuously.*

- A success answer is returned only after the commit is visible at the
  storage root and durable, and it names the commit, or says that nothing
  was published. Durable means it survives every failure named under
  [assumptions](#a-assumptions), a stop of the process or the machine at any
  point, short of the loss of the root.
- Every operation ends in success or in a typed refusal that states its
  effects: none, partial (an earlier step of the same operation published),
  or unknown. A request can carry an identity, so that sending it again
  after a lost answer applies it at most once and returns the first answer;
  a request without one is resolved by reading history, never by replaying
  it.
- Work is bounded. A read returns the complete answer within declared limits
  or a refusal, never a truncated answer presented as complete; contention
  ends in a bounded number of attempts and then a typed conflict.
- Availability follows the store: when the root is unreachable, writes are
  refused, never accepted for later, and a read that needs a branch head or
  table data is refused. While the root is reachable the graph stays
  readable and writable through maintenance, schema change and replacement
  of a serving process. Not promised: a latency bound, or a consistent
  backup from copying a root while writers run.

Owners: [writes](writes.md#failure-outcomes),
[server](../user/operations/server.md#errors-and-retries); CLI
`system_remote`, the catalog's manifest-request tests, `failpoints.rs`.

### 6. History

*Why: an agent holds no memory of its own. What was known, who changed it,
why, and what changed since it last looked are reads the work makes all the
time, and a mistake by one agent must be undoable without losing the rest.*

- Every published commit stays readable and diffable until `cleanup` removes
  it, and no other operation on a graph removes published history. History is
  read through the current schema: a property whose identity changed since
  refuses rather than guesses. A branch can start at a published commit of a
  live branch under the current schema.
- A change feed delivers a branch's own (first-parent) commits in order from
  a cursor the caller holds, and never skips one; a merge commit carries what
  it merged in. A consumer sees each commit at least once and applies it
  idempotently. Reclaimed history, a schema boundary and a cursor from
  another branch lifetime are typed refusals, never silent gaps. A consumer
  can wait for the next commit instead of polling for it.
- A commit records who made it and, when the caller says so, why: the task
  and the reason, readable wherever the commit is.
- A commit or a merge can be reverted by one operation that publishes its
  inverse as a new commit, refused where later changes depend on it.
- Erasure is explicit: a deleted value is gone once no retained commit,
  branch, merge base or tag names it and its bytes are rewritten. Not
  promised: erasure by delete alone, or by any deadline.

Owners: [branching](../user/branching/index.md),
[changes](../user/branching/changes.md);
`integration/point_in_time.rs`, `integration/changes.rs`,
`integration/maintenance.rs`.

### 7. Data independence, identity and policy

*Why: retrieval must mean the same whatever has been built to speed it up,
and agents of different authority share one model, so who may read and
change what is part of the state, not of each client.*

- **Data independence**, in Codd's sense of indexing and ordering
  independence [7]. A read answers from committed data, so building,
  dropping or partly covering an index changes cost, never rows, and rows
  have no order but the one `order` or a ranking states. One declared
  exception: a read that needs an index for its meaning is refused without
  it, rather than answered differently. A read that is approximate by request says so in its answer:
  `nearest` under a probe cap reports whether the run was `exhaustive`. Not
  promised: a recall figure for a non-exhaustive run.
- **Identity.** A served actor comes from the bearer token; a client cannot
  name a trusted actor. A commit records the actor that was resolved or
  declared.
- **Policy.** Where a policy is installed, every action is checked before
  its first durable effect, so a denied write leaves the graph unchanged, and
  a served graph with no policy denies every authenticated request. Policy
  scope is the graph, the action, the branch, and the types and properties
  an agent may read or change. Not promised: per-row policy, or any policy
  on the CLI's direct handle, whose caller is trusted as the operator; an
  embedded caller may install one.

Owners: [search](../user/search/index.md),
[policy](../user/operations/policy.md),
[writes](writes.md#policy-and-attribution); `integration/scalar_indexes.rs`,
`search.rs`, `policy_engine_chassis.rs`, server `auth_policy`.

### Today and the target

Every place where current behavior differs from a clause, and what is wanted
instead. A row leaves this table when the code reaches the target; a row that
is a deliberate limit says so.

| Clause | Today | Wanted |
|---|---|---|
| 1 | A load with a base onto a missing branch publishes the branch first, so a failed load can leave a new empty branch (`effects: partial`) | The branch and its first rows in one commit, or neither |
| 1 | An update or keyed upsert with unchanged values still publishes a commit, and so does a load with no rows. Only a mutation that stages nothing publishes nothing | A write that changes no row publishes nothing, which continuous sync needs |
| 2 | Within one process a read's capture can wait behind an exclusive schema apply, branch create or handle open, and behind the writes that operation waits for | Reads never wait for writers |
| 2 | One mutation-capable writer process per cluster is supported. Branch create and delete, schema apply, dropping a proposal and `cleanup` are safe only from one process, and schema apply's only-`main` check can miss another process's branch create | Any number of writer processes, every write decided by the same compare-and-swap |
| 2, 5 | A process that claimed a branch's writer epoch (embedded engine only) reads its bound branch without checking the head: it is stale once another process replaces it or deletes the branch, and can answer some reads with no storage request while the store is unreachable | Deliberate: the owner trades the check for latency. Wanted is a bounded staleness the owner can state |
| 2 | Read replicas are not a supported topology: a served process takes the cluster's writer admission, so one server serves a cluster, and only read-only CLI commands read beside it | A read-only serving mode that takes no writer admission, so read capacity grows with processes |
| 2 | A conditional write names one commit and is refused by any intervening commit on the branch, related or not | *Derived.* A condition scoped to the entities or query the decision read |
| 2 | A merge records and checks no read dependency, so a conclusion can merge on evidence that changed on the target | *Derived.* Declared dependencies, reported at merge when stale |
| 3 | Conflict detection is by whole entity: two agents changing different properties of one entity conflict (`divergent_update`) | *Derived.* Property-level merge with the merged entity rechecked |
| 4 | Schema apply runs against `main` only, with no other branch live, outside review by branch | A schema change proposed on a branch and merged, with branches live |
| 5 | Graph writes carry no request identity: a lost answer is resolved by reading history, and only a conditional write is safe to repeat. Recorded operations do carry one | *Derived.* An idempotency identity on every write |
| 5 | Replacing a serving process is stop, then start; `optimize` and schema apply hold writer gates while they run | Continuous service through replacement and maintenance |
| 6 | A commit records an actor only; a change feed is polled; there is no revert, and undoing is a hand-written inverse commit | *Derived.* Task and reason on the commit, a waiting feed, revert as an operation |
| 7 | Policy scope stops at a branch class (any, protected, unprotected): no type-level or property-level scope | Type and property scope, as the access-authority work proposes |
| O | An aggregate read with an `order` breaks ties between groups by nothing, so groups equal on the order keys come back in unspecified order | One total order for every sorted read |
| 5 | On a local filesystem root nothing is fsynced: an acknowledged commit survives a process crash, not necessarily power loss or an operating-system crash. On an object store it is durable at acknowledgement | Durable at acknowledgement on every supported root, or local roots declared development-only |
| 5 | A stream (export, Blob GET) that fails after its headers ends in an aborted body, not a typed refusal; a merge that also deletes its source can succeed while the deletion fails; retries a write took are not reported in its answer | Every failure typed at the door; retries visible in the answer |
| 5 | The server answers the schema route from memory while the store is unreachable | Deliberate: the schema it serves is the one it accepted |
| 5 | Azure is a preview whose writer must run under an external admission lease that the engine does not check | Qualified Azure writes with the fence in the engine |
| 6 | `optimize` rewrites a fragment for deletions only above Lance's 10% threshold or when it has neighbours to merge, so deleted bytes can stay in files after `optimize` and `cleanup` | An erasure path that rewrites every fragment holding a deleted value |
| 6 | `cleanup` asks for consent in the CLI only. Removing a graph from a cluster deployment deletes its whole root. An unpublished commit or unattached snapshot becomes unreadable on drop or expiry, before `cleanup` | Deliberate limits; consent at the engine door is wanted |
| 7 | `bm25`, and `rrf` over a `bm25` arm, scores rows that no full-text segment covers on a separate scale, so scores, order and a limited result can change when a build extends coverage. `bm25` on a property with no certified segment is refused | Scores independent of coverage, or a declared flag in the answer as `nearest` has |
| 7 | A served graph with no policy still permits reads by static tokens under an operator opt-in, and a server started unauthenticated with no tokens and no policy permits every request | Deliberate operator choices, both logged at startup |

## I: interfaces

`.pg` is the schema language and `.gq` the read, mutation, load and branch
language; both are typed and checked against the accepted schema before any
effect. The three doors share one operation inventory, one error code set and
one `disposition` object; the HTTP contract is versioned by one request header
and the CLI exit code follows the disposition. Storage is open Lance plus a
few control objects under one root, readable by Lance tools as a view, with
no promise about what a foreign reader infers from it. Storage-format and
wire compatibility are owned by [versioning](versioning.md).

## A: assumptions

- **Storage.** One root on a local filesystem with hard links, or an object
  store with put-if-absent and read-after-write (S3-compatible, or Azure as a
  preview). Omnigraph replicates nothing itself and encrypts nothing at rest:
  durability, replication and encryption are the store's.
- **Failures.** A process may stop at any instruction; a storage request may
  fail, land late or answer ambiguously; a response may be lost. The contract
  holds under all of these. Byzantine storage is out of scope.
- **Clocks.** Timestamps come from writer clocks and are informational:
  ordering authority is the branch history. Time only orders listings of
  proposals and snapshots and selects what `cleanup --older-than` retains.
- **Topology.** The target is one mutation-capable writer process per
  cluster and any number of stateless reader processes over the same root,
  added and removed freely. The writer half is the supported topology today
  ([support boundaries](invariants.md#current-support-boundaries)); the
  reader half is wanted.
  Ordinary writes and merges stay correct across processes; the control
  writes listed under [today and the target](#today-and-the-target) do not.
- **Trust.** The server boundary is trusted to resolve actors. An embedded or
  direct caller is trusted as the operator who gave it the root.
- **Delegated.** Three things a commit depends on sit outside it. Stored
  vectors are computed outside the write path, by the offline `embed` step or
  the client: no load or mutation calls a provider, and nothing detects a
  vector that is stale against its source text. An external Blob value
  references an object outside the root, which a commit neither retains,
  copies nor validates at a later read. A mirrored record is as true as its
  last sync, with its source the system of record.

## How to use this page

- A design, RFC or review names the requirement in [R](#r-what-the-work-requires)
  it serves, the clause it keeps or moves toward, and the activity or
  quantity it is for. "Faster" without a row and "safe" without
  a clause are not claims.
- A clause changes only because the work is understood differently, never
  because the engine cannot reach it: an unreached clause is a row of the
  today table. A change that would weaken a clause is a contract change:
  write the RFC first and say what the new history set admits.
- A *derived* target is a requirement, not a design. Turning one into
  behavior takes its own RFC, which may refine the target.
- A change tuned for a different workload revises W in the same pull request,
  with the measurement that moved it.
- A user-facing document states a clause in the user's words and links here
  for the precise form; it never states a stronger or weaker one.

## Sources

Read in full on 2026-11-16 to ground the terms above; the number is the
citation used in the text. The method's own sources are in
[systems](systems.md#references-and-source-scope).

1. Herlihy and Wing, "Linearizability: A Correctness Condition for
   Concurrent Objects", TOPLAS 1990: linearizability and locality.
2. Papadimitriou, "The Serializability of Concurrent Database Updates",
   JACM 1979: strict serializability.
3. Berenson et al., "A Critique of ANSI SQL Isolation Levels", SIGMOD 1995:
   snapshot isolation, lost update, write skew. Also Adya, Liskov and
   O'Neil, "Generalized Isolation Level Definitions", ICDE 2000.
4. Terry et al., "Session Guarantees for Weakly Consistent Replicated
   Data", PDIS 1994.
5. Fekete et al., "Making Snapshot Isolation Serializable", TODS 2005:
   predicate write skew and the read-only anomaly.
6. Bailis et al., "Coordination Avoidance in Database Systems", PVLDB 2015:
   invariant confluence and its table of constraints.
7. Codd, "A Relational Model of Data for Large Shared Data Banks", CACM
   1970: ordering and indexing dependence.
8. Khanna, Kunal and Pierce, "A Formal Investigation of Diff3", FSTTCS
   2007: state-based three-way merge and its properties.
9. Maddox et al., "Decibel: The Relational Dataset Branching System", PVLDB
   2016; Huang et al., "OrpheusDB", PVLDB 2017; Bhardwaj et al., "DataHub",
   CIDR 2015; and the Dolt and lakeFS merge documentation: version graphs
   and field-level or cell-level merge.
10. Chaudhuri and Dayal, "An Overview of Data Warehousing and OLAP
    Technology", SIGMOD Record 1997.
11. Stonebraker and Cetintemel, "One Size Fits All: An Idea Whose Time Has
    Come and Gone", ICDE 2005.
12. Kemper and Neumann, "HyPer: A Hybrid OLTP&OLAP Main Memory Database
    System Based on Virtual Memory Snapshots", ICDE 2011.
13. Ozcan, Tian and Tozun, "Hybrid Transactional/Analytical Processing: A
    Survey", SIGMOD 2017.
14. Huang et al., "TiDB: A Raft-based HTAP Database", PVLDB 2020.
15. Armbrust et al., "Delta Lake: High-Performance ACID Table Storage over
    Cloud Object Stores", PVLDB 2020; and "Lakehouse", CIDR 2021.
16. Codd, Codd and Salley, "Providing OLAP to User-Analysts: An IT
    Mandate", 1993.
17. Databricks, "Databricks Launches LTAP" (press release, 16 June 2026),
    and R. Xin, "From monolith to Lakebase to LTAP" (30 June 2026).

Not grounded in a source read here: the definition of durability, and the
latency and rate figures in the work, which are hypotheses.
