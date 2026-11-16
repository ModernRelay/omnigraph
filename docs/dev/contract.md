# The contract and the work

**Audience:** everyone who changes or reviews Omnigraph, coding agents included
**Authority:** the observable contract Omnigraph is built toward, and the
dated hypothesis of the work it serves; the rules that keep the contract are
in [invariants](invariants.md), the mechanisms in the area guides

A database is an observable contract, implemented by physical mechanisms,
serving a distribution of work ([systems](systems.md)). This page fixes the
first and the third for Omnigraph: the contract is a promise and is current,
the workload is a hypothesis and is dated. Every change names the clause it
keeps and the row of the work it serves ([how](#how-to-use-this-page)).
Mechanisms are out of scope here: they live in the area guides.

## In one line

Omnigraph is a typed property graph whose every change is one atomic,
durable, attributed **graph commit**; whose every read is one **snapshot** of
one commit; whose **branches** are strictly serializable commit histories that
merge three-way; and whose **history** stays readable until an explicit
cleanup. The work it is built for is **agentic knowledge work**, described in
[W](#w-the-work-a-hypothesis).

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
  inserted again is the same id, not a tracked lifetime.
- Types, properties and tables have **stable identity** that survives a
  supported rename and ends at a drop; a re-added name is a new identity.
  Names and paths are aliases, never identity.
- A **graph commit** is an immutable, id-addressed version of the whole graph
  with its parent or parents, branch, actor when known, and a writer-clock
  timestamp. Commits form a directed acyclic history per graph; merge commits
  have two parents. Time is transaction time only: there is no valid time.
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

There is no revert, no restore of a branch to an earlier commit, and no read
"as of" a time: a past state is reached by commit id, and undone by a new
commit or a branch started at that commit.

## G: guarantees

Seven clauses. Each clause is the **target**: what Omnigraph is built to
promise and what a change must move toward, never away from. A clause states
admitted histories and outcomes, never how the engine gets there. Each ends
with its owners: the guide that holds the mechanism and the tests that hold
the evidence.

**Today** is the target minus the rows of [today and the target](#today-and-the-target),
which lists every place where current behavior falls short, with what is
wanted instead. A clause with no row there holds as written, validated
against the code on 2026-11-16.

### 1. Atomicity

- One mutation query, one load batch, one merge, one schema apply or one Blob
  write becomes visible as one graph commit or not at all. No statement,
  table or type is visible on its own.
- The commit is the transaction. Not promised: an interactive
  `BEGIN`/`ROLLBACK`, or anything atomic across two commits, two branches or
  two graphs. A multi-step change is a branch, made atomic by its merge.

Owners: [writes](writes.md), [recovery](recovery.md); `failpoints.rs`,
`detached_commit_matrix.rs`, the DST suite.

### 2. Isolation

- **Reads.** A read names one branch head or one commit, resolves it once,
  and sees exactly that commit, every type together, for its whole life.
  Within one mutation a later insert or update sees the earlier statements'
  pending rows; its delete statements read the same commit and remove the
  union of what they match.
- **Writes on one branch.** A branch's commits form a total order, and every
  history over one branch is equivalent to its commits running one at a time
  in that order, consistent with real time (strict serializability). A write
  takes effect against the head at its publication: either that is the head
  it read, or it was prepared again on the newer head, or it is refused. A
  read at a branch head observes every commit acknowledged before it began.
- **Conditional writes.** A write that names the commit its caller read
  (`--if-commit`) applies only if that commit is still the head, whatever
  changed in between, and is otherwise refused with `expected` and `actual`.
  This is how a client extends the serial order across its own read and a
  later write; without it, a write is serialized where it lands, not where
  its caller last looked. An unpublished commit publishes under the same
  rule.
- **Readers scale out.** A read learns the head from storage, never from
  another process. Any number of reader processes may serve one root; none
  coordinates with the writer or with another reader, and each is always
  fresh: it observes every commit acknowledged before the read began. Not
  promised: a stale read mode that trades freshness for latency.
- **Sessions.** On one branch a client gets read-your-writes and monotonic
  reads from the rules above, on whichever reader process serves it. A client that needs a fixed view across
  requests pins a commit id. Not promised: a read "at least as new as commit
  X" on another branch, or any joint view of two branches.
- **Across branches.** Branches are isolated until merged, and a merge
  detects conflicts by entity and by declared constraint
  ([clause 3](#3-merge)). That is snapshot isolation between branches, not
  serializability: a merge tracks no read set. Two branches that each read a
  fact and wrote different entities merge cleanly even when each decision
  depended on what the other changed (write skew), unless the schema declares
  the invariant. An invariant that must hold across branches is declared as a
  constraint or checked on the target after the merge.

Owners: [writes](writes.md), [execution](execution.md),
[architecture](architecture.md#concurrency-and-support-boundary);
`writes.rs`, `integration/consistency.rs`, `integration/warm_read_cost.rs`,
the DST suite.

### 3. Merge

- A merge compares source and target against one merge base, a lowest
  common ancestor chosen by a fixed rule (nearest both heads, then highest
  generation, then smallest commit id), and publishes the result as one
  commit on the target, or nothing. The source is unchanged.
- The unit of comparison is the entity. An entity changed on one side takes
  that side's state, a delete included. The same change on both sides
  converges. Different changes to one entity are a conflict, even when they
  touch different properties: there is no property-level merge. The kinds are
  `divergent_insert`, `divergent_update`, `delete_vs_update`. A result that
  breaks a constraint is a conflict too: `orphan_edge`, `unique_violation`,
  `cardinality_violation`, `value_constraint_violation`.
- Conflicts are reported per entity, typed, and resolved by writing to a
  branch and merging again. Without conflicts the merged entities do not
  depend on which branch merges first; commit ids, history shape and feed
  order do. Not promised: automatic resolution, or a policy such as
  last-writer-wins.

Owners: [merge](../user/branching/merge.md), [merge internals](merge.md);
`integration/merge_truth_table.rs`, `integration/merge_fast_forward.rs`.

### 4. Integrity

- Every constraint of the accepted schema holds at every commit, on every
  door, whichever writer produced it. A violation is a typed refusal before
  publication. The engine never writes a placeholder, weakens a constraint or
  reinterprets unknown persisted state.
- A strict insert never becomes an upsert; an insert into a `@key` type is an
  upsert by design. Schema apply checks stored rows for the constraints it
  can add and refuses the rest.
- Not promised: any invariant the schema does not declare.

Owners: [execution](execution.md#validation), [invariants](invariants.md);
`integration/validators.rs`, `schema_apply.rs`.

### 5. Durability and outcomes

- A success answer is returned only after the commit is durable and visible
  at the storage root, and it names the commit, or says that nothing was
  published. What survives is what the root survives: any crash of the
  process or the machine, never the loss of the root.
- Every operation ends in success or in a typed refusal that states its
  effects: none, partial (an earlier step of the same operation published),
  or unknown. An unknown outcome is resolved by reading history, never by
  replaying the request: graph writes carry no idempotency key and are not
  exactly-once across a lost answer. A conditional write is safe to repeat,
  and a recorded operation is observed by its id.
- Work is bounded. A read returns the complete answer within declared limits
  or a refusal, never a truncated answer presented as complete; contention
  ends in a bounded number of attempts and then a typed conflict.
- Availability follows the store: when the root is unreachable, writes are
  refused, never accepted for later, and a read that needs a branch head or
  table data is refused. Not promised: a latency bound, or a consistent
  backup from copying a root while writers run.

Owners: [writes](writes.md#failure-outcomes),
[server](../user/operations/server.md#errors-and-retries); CLI
`system_remote`, the catalog's manifest-request tests, `failpoints.rs`.

### 6. History

- Every published commit stays readable and diffable until `cleanup` removes
  it, and no other operation on a graph removes published history. History is
  read through the current schema: a property whose identity changed since
  refuses rather than guesses. A branch can start at a published commit of a
  live branch under the current schema.
- A change feed delivers a branch's own (first-parent) commits in order from
  a cursor the caller holds, and never skips one; a merge commit carries what
  it merged in. A consumer sees each commit at least once and applies it
  idempotently. Reclaimed history, a schema boundary and a cursor from
  another branch lifetime are typed refusals, never silent gaps.
- Erasure is explicit: a deleted value is gone once no retained commit,
  branch, merge base or tag names it and its bytes are rewritten. Not
  promised: erasure by delete alone, or by any deadline.

Owners: [branching](../user/branching/index.md),
[changes](../user/branching/changes.md);
`integration/point_in_time.rs`, `integration/changes.rs`,
`integration/maintenance.rs`.

### 7. Data independence, identity and policy

- **Data independence.** A read answers from committed data: a missing or
  partial index changes cost, never rows or their order, and where an index
  is required for meaning the read is refused rather than answered
  differently. A read that is approximate by request says so in its answer:
  `nearest` under a probe cap reports whether the run was `exhaustive`. Not
  promised: a recall figure for a non-exhaustive run.
- **Identity.** A served actor comes from the bearer token; a client cannot
  name a trusted actor. A commit records the actor that was resolved or
  declared.
- **Policy.** Where a policy is installed, every action is checked before
  its first durable effect, so a denied write leaves the graph unchanged, and
  a served graph with no policy denies every authenticated request. Policy
  scope is the graph, the action and a branch class (any, protected or
  unprotected). Not promised: row-level or property-level read control, or
  any policy on the CLI's direct handle, whose caller is trusted as the
  operator; an embedded caller may install one.

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
| 2 | Branch create and delete, schema apply, dropping a proposal and `cleanup` are safe only from one writer process per graph; schema apply's only-`main` check can miss another process's branch create, and schema apply needs `main` to be the only branch | Every write decided by the same compare-and-swap, across processes; schema apply with live branches |
| 2, 5 | A process that claimed a branch's writer epoch (embedded engine only) reads its bound branch without checking the head: it is stale once another process replaces it or deletes the branch, and can answer some reads with no storage request while the store is unreachable | Deliberate: the owner trades the check for latency. Wanted is a bounded staleness the owner can state |
| 2 | Read replicas are not a supported topology: a served process takes the cluster's writer admission, so one server serves a cluster, and only read-only CLI commands read beside it | A read-only serving mode that takes no writer admission, so read capacity grows with processes |
| 2 | An aggregate read with an `order` breaks ties between groups by nothing, so groups equal on the order keys come back in unspecified order | One total order for every sorted read |
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

## W: the work, a hypothesis

**Hypothesis dated 2026-11-07.** Derived from the shape of long-horizon agent
knowledge work, not from Omnigraph's current users, features or mechanisms:
the mechanisms follow the work, never the reverse. Nothing here is measured
at scale; each quantity says what would revise it. Revise by editing these
tables with the evidence.

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

**In category shorthand**, which [systems](systems.md#appendix-a-terminology-expansion)
treats as a bundle to unpack: a hybrid. The larger half is operational (small,
selective lookups on the critical path of a step, many small attributed
commits); beside it run analytical reads over whole types that agents issue
while retrieving, batch ingestion of whole sources, and a heavy history and
change-data half. Both read modes run against the same fresh snapshot, with
no copy to a second system. It is not a warehouse workload: analytical reads
are over one graph at the volumes above, not over billions of rows.

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

## How to use this page

- A design, RFC or review names the clause it keeps or extends and the
  activity or quantity it serves. "Faster" without a row and "safe" without
  a clause are not claims.
- A change that cannot keep a clause is a contract change: write the RFC
  first and say what the new history set admits.
- A change tuned for a different workload revises W in the same pull request,
  with the measurement that moved it.
- A user-facing document states a clause in the user's words and links here
  for the precise form; it never states a stronger or weaker one.
