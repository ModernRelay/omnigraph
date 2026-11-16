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
or writes the root's files directly is outside the contract; the root is
opaque, and excluding such writers is an operator obligation. The scope of
every promise below is one graph: no read or write spans two graphs, and
graphs served together share only the server and its cluster deployment
state.

## S: state

- A graph is nodes and edges under **one accepted schema**: typed properties,
  nullability, keys, `@unique`, `@range`, `@check`, enums and edge
  cardinality. Blob and vector values are properties.
- Types, properties and tables have **stable identity** that survives a
  supported rename and ends at a drop; a re-added name is a new identity.
  Names and paths are aliases, never identity.
- A **graph commit** is an immutable, id-addressed version of the whole graph
  with its parent or parents, branch, actor when known, and a writer-clock
  timestamp. Commits form one history per graph; merge commits have two
  parents.
- A **branch** is a named history: a head commit and the commits reachable
  from it. `main` always exists. A deleted name and a recreated one are
  different branches.
- An **unpublished commit** (`as commit`) and an **unattached snapshot** are
  durable, id-addressed commits that no branch head shows.
- Indexes, caches, topology, fragment layout and compaction output are
  **derived physical state**; they are not part of S.

## O: operations

| Family | Operations |
|---|---|
| Read | `.gq` reads over one branch head or one commit: patterns, traversal, filters, aggregation, `bm25`, `nearest`, `rrf`, with explain and an execution report |
| Change | A `.gq` mutation query (several statements, constructive or destructive, never both), optionally conditional on a commit id or written `as commit`; a bounded load batch (`append`, `merge` or `overwrite`); a Blob cell put or clear, optionally conditional on its ETag |
| Branch | Create at a head or at a commit; delete; three-way merge, with preview and merge base; publish or drop an unpublished commit; claim a branch's writer epoch (embedded engine only) |
| History | Commit listing, one commit's changes, diff between any two points, branch review, change feed with a durable cursor, baseline snapshot, export |
| Schema | Apply a new `.pg` against `main`; lint and migration plan |
| Maintenance | `optimize` (layout and index coverage, history preserved; direct, or served as a recorded operation); index builds and the full-text rebuild; `cleanup` (removes history and lists foreign versions; with graph removal from a cluster, the destructive operations) |
| Access | Bearer authentication, Cedar policy, and `access` (HTTP `GET /graphs/{graph_id}/access`, CLI `omnigraph access`, MCP `access`): the caller's actor and allowed actions |

## G: guarantees

Each clause is named so a change, a test or a review can cite it, and each
states its own limit.

| Clause | Promise | Kept in |
|---|---|---|
| **Graph-commit atomicity** | One mutation query, one load batch, one merge, one schema apply or one Blob write becomes visible as one commit or not at all; no statement, table or type is visible on its own. One exception: a load with a base onto a missing branch publishes the branch first, so a failed load can leave a new empty branch (`effects: partial`). A mutation whose statements match no rows publishes nothing (`commit: null`); an update that matches rows publishes even when the values are unchanged. The commit is the transaction: there is no interactive `BEGIN`/`ROLLBACK`, and nothing spans two commits, two branches or two graphs | [writes](writes.md), [recovery](recovery.md) |
| **Snapshot reads** | A read names one branch head or one commit, resolves it once, and sees exactly that commit for its whole life, every type together; nothing concurrent moves under it. Within one mutation (inserts and updates, or deletes, never both), a later statement sees the earlier statements' pending effects. Two branches have no joint cut: a two-sided diff resolves each side in turn, and only a merge moves state between branches | [execution](execution.md) |
| **Per-branch strict serializability** | A branch's commits form a total order. Every commit's parent is the head its writer validated against, enforced at publication by a version compare-and-swap. A mutation, load or Blob write whose head moved before its effects is carried over publications that changed no row (optimize, an index build) and otherwise prepared again from its request on the current head, in at most two attempts within a process and 32 across processes: an update or delete reads the rows as they are when it publishes. Such a write is refused with a read-set conflict only when it selects existing rows and the branch incarnation or accepted schema changed under it; a merge or schema apply whose captured authority moved is refused. A read placed at a branch head probes storage for that head, so it observes every commit acknowledged before it began on the read-after-write store assumed below. One exception: a process that claimed the branch's writer epoch, which only the embedded engine can, reads its bound branch with no probe, and is stale once another process replaces it or deletes the branch, until its next publish is refused. Every history over one branch is therefore equivalent to the commits running one at a time, in commit order, consistent with real time. A read holds no lock while it executes and none another process sees; its capture takes a process-local shared permit, so within one process it waits behind an exclusive schema apply, system-column upgrade, branch create, `sync_branch`, `refresh` or handle open, and behind the in-flight writes such an operation waits for. The expected shape is many readers and one writer per branch, which the contract exploits but does not require: further writers of one branch are admitted, serialized and told | [writes](writes.md), [architecture](architecture.md#concurrency-and-support-boundary) |
| **Branch-level compare-and-set** | A conditional write (`--if-commit`, the HTTP conditional routes) applies only if the branch head is the commit the caller read, whatever an intervening commit changed, optimize included: it is never carried or prepared again. Otherwise nothing is written and the caller gets `412 precondition_failure` with `expected` and, when the branch has a head, `actual` (CLI exit 4, embedded or served), wherever the race lands. An unpublished commit publishes under the same rule against its captured parent; a repeat publish answers the published commit, and dropping a published commit is a conflict | [conditional mutations](../user/mutations/conditional.md), [branching](../user/branching/index.md#unpublished-commits) |
| **Fail-closed integrity** | Every constraint of the accepted schema is checked before publication on every content write: mutation, load in every mode, Blob write, merge. Schema apply checks stored rows for the constraints it can add and refuses the rest. A merge skips only checks that cannot fail: a fast-forward of proven pure inserts into node types with no value, enum, `@unique` or `@card`-minimum constraint. A violation refuses before publication: `bad_request` for a mutation or load, `key_conflict` for an existing id on a strict insert, `merge_conflict` with a per-entity kind for a merge. The engine never writes a placeholder, weakens a constraint or reinterprets unknown persisted state. A strict insert (`load --mode append`, or `insert` into a type without `@key`) never becomes an upsert; `insert` into a `@key` type is an upsert by design | [execution](execution.md#validation), [invariants](invariants.md) |
| **Durable on acknowledgement; unknown stays unknown** | A success answer is returned only after the commit is durable and visible at the storage root, and carries the commit id (the system-column upgrade reports the manifest version; a recorded operation, a served deployment or optimize, is accepted first and observed by its id, and acceptance is not completion). A lost or incomplete answer means the outcome is unknown; it is resolved by reading history, never by replaying the request: graph writes have no idempotency key and no exactly-once for a lost answer (recorded operations and managed control-plane requests carry an identity that is observed, not replayed). A conditional write is safe to repeat, since a landed first attempt fails the second's precondition. Every refusal says whether it had effects (`disposition`), and a write the server could not settle is `outcome_unknown` | [writes](writes.md#failure-outcomes), [server](../user/operations/server.md#errors-and-retries) |
| **Isolated branches, three-way merge** | A commit on one branch is invisible on every other until a merge. A merge compares base, source and target and publishes the result as one commit on the target: `fast_forward`, decided table by table from proven pure inserts or adopted snapshots, or `merged`, a three-way comparison entity by entity; `already_up_to_date` publishes nothing. Conflicts are reported per entity, typed, and publish nothing. The source's history is unchanged; the merge holds a retention tag on its snapshot until publication | [merge](../user/branching/merge.md), [merge internals](merge.md) |
| **Retained, addressable history** | Every published commit stays readable and diffable until `cleanup` removes it. A branch can start at a commit only if it is published, on its live branch incarnation and under the current accepted schema; otherwise a typed `branch_start_refusal` names the reason. An unpublished commit or unattached snapshot stays readable only while the target head and incarnation it captured stand; then it expires and awaits cleanup. `cleanup` runs only with `--keep` or `--older-than` and, in the CLI, `--confirm` (the preview opens nothing); no HTTP route runs it, and removing a graph from a cluster deployment deletes its whole root. History is read through the current schema: a property whose identity changed since refuses rather than guesses. A change feed delivers a branch's commits in order from a durable cursor and never skips: reclaimed history is `410 change_feed_gap`, and a commit crossing a schema change on a non-empty type is `409 change_diff_refusal` (`schema_boundary`) | [branching](../user/branching/index.md), [changes](../user/branching/changes.md) |
| **Exact, or declared** | A read answers from the committed data, never from an index alone: a missing or partial scalar, vector or topology index, or a partial full-text index, changes cost, not rows; `bm25` on a property with no full-text segment at the snapshot, or one without analyzer proof, is refused (`FullTextIndexRequired`, `FullTextIndexRebuildRequired`) rather than answered differently. `bm25` scores every match. `nearest` reports `exhaustive` per run. A `nearest`-ordered read with a `limit`, outside `rrf` and aggregation, whose later operators drop candidates reruns at 4 and 16 times its fetch, then exactly, so `limit` is filled whenever that many survivors exist; `rrf` arms and aggregates do not widen. No recall figure beyond `exhaustive` and no latency bound is promised; both are evidence in the cost owners | [search](../user/search/index.md), [execution](execution.md#search-and-rank) |
| **Boundary-fixed identity, engine-enforced policy** | A served actor comes from the bearer token; a client cannot name a trusted actor, and `Omnigraph-If-Actor` can only refuse a request whose actor differs (`412 actor_mismatch`). Token plaintext is not retained. A static token's actor cannot take the `principal:` prefix reserved for signed identities. A policy is installed by the server and by cluster deployments, never by a direct or embedded handle; where installed, every mutating entry point checks it before the first durable effect, maintenance included (`maintain` for optimize and index builds, `reclaim` for cleanup, `branch_claim` for a writer epoch), so a denied write leaves the graph unchanged. A served graph with no applied policy denies every action, reads included, unless the operator opts static tokens back into reads. A commit records `actor_id` when one was resolved by the server or declared by `--as` or `operator.actor`; otherwise none | [writes](writes.md#policy-and-attribution), [policy](../user/operations/policy.md) |
| **Bounded, typed outcomes** | Every refusal is typed, with the limit or conflict it hit; a served request cannot raise a resource setting above the server's budget; a `429` or `503` that asks for a retry carries `Retry-After`. A read returns the complete answer within the declared limits or a refusal, never a truncated answer presented as complete; a stream (export, Blob GET) that fails after its headers ends in an aborted body, not a typed refusal. Retries are bounded (publisher CAS 5, pre-effect reprepare 32) and taken only where no effect is reachable; they do not appear in the answer, except search widening, which the execution report records. There are no swallowed errors or partial results | [invariants](invariants.md), [server](../user/operations/server.md#errors-and-retries) |

## I: interfaces

`.pg` is the schema language and `.gq` the read, mutation, load and branch
language; both are typed and checked against the accepted schema before any
effect. The three doors share one operation inventory, one error code set and
one `disposition` object; the HTTP contract is versioned by one request header
and the CLI exit code follows the disposition. Storage is open Lance (V2_2 files)
plus a few control objects under one root, readable by Lance tools as a view,
with no promise about what a foreign reader infers from it.

## A: assumptions

- **Storage.** One root on a local filesystem with hard links, or an object
  store with put-if-absent and read-after-write (S3-compatible, with the ETag
  conditional put the binding sets; Azure as a preview whose writer runs under
  the external admission lease, which the engine does not check). Durability
  is the root's: Omnigraph replicates nothing itself, and a backup is a copy
  of the root, which external Blob values reference objects outside of.
- **Failures.** A process may stop at any instruction; a storage request may
  fail, land late or answer ambiguously; a response may be lost. The contract
  holds under all of these. Byzantine storage is out of scope.
- **Clocks.** Timestamps come from writer clocks and are informational:
  ordering authority is the branch history. Time only orders the listings of
  proposals and snapshots and selects what `cleanup --older-than` retains.
- **Writers.** One mutation-capable writer process per cluster is the
  supported topology. Ordinary writes, merges and index maintenance are safe
  across processes and machines through the version compare-and-swap, and a
  branch whose writer epoch a process claimed refuses every other process's
  publication, the server's and the CLI's included; ref deletes, the
  collector and `optimize`'s configuration commit are not fenced. Branch
  create and delete, schema apply, dropping a
  proposal and `cleanup` are safe only from one writer process per graph
  ([support boundaries](invariants.md#current-support-boundaries); the engine
  phase of [graph access](../rfcs/2026-10-16-graph-access-serving-and-cross-process-writes.md)
  lifts this). The engine refuses schema apply unless `main` is the only live
  branch, as a read before the publish, so another process's branch create in
  between is not caught; the CLI, not the engine, refuses direct schema apply
  on a cluster-managed graph.
- **Trust.** The server boundary is trusted to resolve actors. An embedded or
  direct caller is trusted as the operator who gave it the root.

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
| Retrieve | Assembles the context for the next step: the entity by key, its neighborhood, the things like it, the passages that mention it, under filters on type, time and status | Selective reads combining key lookup, one to three hops, vector, lexical and fused ranking; tens to a few hundred rows; several per step, on the step's critical path; the most common read |
| Acquire | Brings outside information in: fetches pages, documents, API results and files; imports a dataset it was handed or found; extracts entities, relations and passages from unstructured content; embeds text | Bulk keyed loads, unplanned and bursty, on the task's branch; Blob writes of documents and transcripts; embedding columns; the usual way a new source enters before a sync exists |
| Author | Records what it learned, decided and did: observations, claims with evidence, decisions with reasons, tasks, plans, status, and links between things that already exist | Small attributed commits of one to tens of entities, many per task, on the task's branch, append-mostly; the most common write and the densest edges |
| Derive | Computes over what is there: classifies, scores, resolves duplicates, summarizes, infers edges, consolidates old memory into new | Reads wide across a type, writes many small updates or a batch per run; periodic, often as a pipeline |
| Coordinate | Shares work with other agents and people: claims a task, hands one off, checks whether something was already done, avoids writing over a concurrent change | Concurrent reads of shared state; conditional writes from the commit it read; one branch per task, so one writer per branch by construction of the work, not by a lock; merges into shared state; a conflict re-decided from a fresh read, not retried blind |
| Verify and correct | Checks what it holds against sources and constraints, finds drift and duplicates, fixes errors | Targeted reads by key and by external identity; corrections as updates or new facts; deletes rarely; constraint refusals as signal |
| Propose and review | Puts a change up for a person or another agent to accept; reads what a branch would change; approves, merges, or discards | Branch diffs and merge previews; a merge or a branch delete; unpublished commits that wait |
| Reflect and evaluate | Looks back: what was known at a time, what was decided and why, what happened after; compares a past state with now; evaluates its own earlier actions | Reads at a past commit; diffs across history; commit changes with actors; with orient, the second most common read |
| Maintain | Adjusts the model as the domain is understood better: adds a type or property, widens an enum, re-embeds, rebuilds coverage, prunes | Additive schema applies; index and layout maintenance; rare, reviewed destructive changes and cleanup |
| Answer | Answers a person's question or produces a report: how many, which, since when, grouped by | Aggregation over whole types; larger results; occasional |

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
| Mix | Reads outnumber writes by one to two orders of magnitude; authored commits dominate writes by count, acquisition and sync by rows; deletes are rare everywhere | Analytical scans, updates or deletes become common; result sets pass 10^4 rows |
| Volume | 10^4 to 10^7 entities per graph, set by the sources; 10 to 100 types; small scalar properties, text bodies, embeddings on a few types, Blobs for documents; agent batches of 10^2 to 10^5 records, backfills to 10^7 | 10^8 entities or 10^3 types; batches past 10^6; sync as streaming at thousands of events a second |
| Rate | 10^2 to 10^4 commits a day; tens of live branches with hundreds of commits each and one writer; incremental sync every few minutes; an additive schema change about weekly; history retained long and read as often as the present | Commit rate or branch count up an order; several writers on one branch; conflict retries common; destructive schema changes frequent; history read rarely |

**In category shorthand**, which [systems](systems.md#appendix-a-terminology-expansion)
treats as a bundle to unpack: an operational footprint (small, selective,
latency on the critical path of a step, many small attributed commits) with
batch ingestion phases, a heavy history and change-data half, and occasional
analytical aggregation. Not a warehouse workload; "HTAP" would overstate the
analytical half; nearest to operational retrieval over a versioned store.

What the hypothesis asks of the mechanisms, each with its cost owner in
[testing](testing.md#cost-tests-and-benchmarks): a read on a warm handle
costs a bounded number of storage requests independent of history; a commit
costs the same whatever the history depth and live branch count; a branch is
free until written; a keyed upsert of an unchanged record costs little and
publishes nothing; change discovery costs the changes, not the tables; a
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
