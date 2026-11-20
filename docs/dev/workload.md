# The work

**Audience:** everyone who designs or reviews a change to Omnigraph
**Authority:** a dated hypothesis of the work Omnigraph is built for, what
that work asks of the shared state, and the evidence for and against it; what
each operation promises is in [the contract](contract.md)

## Editing this page

A database is an observable contract, implemented by physical mechanisms,
serving a distribution of work ([systems](systems.md)). This page is the
third of the three: the distribution of work Omnigraph is optimized for.
[The contract](contract.md) is the first. The mechanisms follow from the two
and live in the area guides, never here.

- **Describe the work, not the engine.** Derive every row from what agents,
  people and programs do to a shared model, never from what Omnigraph has
  or does today. A sentence that names a mechanism belongs in a guide.
- **It is a hypothesis.** Give a quantity as a range or a percentile, say
  what would revise it, and replace it with a measurement when one exists.
  State what the evidence supports, what it argues against, and what nobody
  has measured.
- **Say what is optimized for and what is not**, in order, so that a design
  can tell which cost it may trade away.
- **Keep the contract out.** A promise, a guarantee or its exact term goes
  in [the contract](contract.md). Something the work asks for that nothing
  has decided is listed under [Asked, not decided](#asked-not-decided), and
  named in the contract's Direction table.
- **Cite a source for every claim about the world**, and say when a figure
  comes from a narrow study or from a vendor.
- **Revise it in the pull request that needs it.** A change tuned for a
  different workload changes this page in the same pull request, with the
  measurement that moved it.

A database is an observable contract, implemented by physical mechanisms,
serving a distribution of work ([systems](systems.md)). This page is the
distribution of work. [The contract](contract.md) states what that work
requires, by operation, and the mechanisms are in the area guides.

## The hypothesis

**Hypothesis dated 2026-11-07, last revised 2026-11-16.** The new work that
long-horizon multi-agent systems introduce is **multiple agents sharing a
typed world model to continuously retrieve and update context and coordinate
work**. The work comes first: the contract is what that work requires, and
the mechanisms follow the contract. What the work asks for and nobody has
decided is listed under [asked, not decided](#asked-not-decided). Nothing
here is measured at scale; each quantity says what would revise it. Revise by
editing these tables with the evidence.

### What an agent does

An agent is a policy that acts over a shared model of some domain, in many
steps, toward goals it does not finish in one session. Its working state is
private and short-lived; it holds no durable state of its own, so every
result it or another agent must rely on later goes through the graph.
Its activities range over the following, each with its footprint; the
distribution is their sum.

| Activity | What the agent does | Footprint on the graph |
|---|---|---|
| Orient | Learns what exists and what the words mean, throughout a task and not only at its start: the schema, the types and their counts, which type or property holds what it is looking for, what changed since it last looked, what others are working on now | Schema and catalogue reads, search over type and property names and descriptions, change feed from a cursor, branch and commit listings. In one set of measured data-agent traces (44 traces of one model) about half of all queries are this kind of exploration [18], so it is frequent and must be cheap |
| Speculate | Probes before it commits to an approach: partial, exploratory and repeated reads to test an idea, and what-if writes on a throwaway branch to see their effect, most of which are discarded | Many small reads, largely overlapping with each other and with other agents' reads (most sub-plans across parallel attempts are duplicates [18]); many short-lived branches created, written a little and dropped, far more than a person would make [18] [24] |
| Retrieve | Assembles the context for the next step, in two modes. Lookup: the entity by key, its neighborhood, the things like it, the passages that mention it, under filters on type, time and status, ranked by relevance, recency and importance. Analytical: how many, which most, grouped by what, over a type or a filtered slice, to size a problem, rank candidates or summarize before deciding | Lookup: selective reads combining key lookup, one to three hops or a ranking that spreads over the graph, vector, lexical and fused ranking, often cut by a time range; tens to a few hundred rows; several per step; the most common read. Analytical: aggregates, group-bys, top-N and filtered scans over whole types, small results from wide reads, on the same critical path of a step |
| Acquire | Brings outside information in: fetches pages, documents, API results and files; imports a dataset it was handed or found, structured (tables, exports, API dumps) or unstructured (PDFs, documents, transcripts, mail); extracts entities, relations and passages from the unstructured content; embeds text | Keyed loads from 10^2 records to 10^7 for a whole source, unplanned and bursty, on the task's branch, as many bounded batches; Blob writes of thousands of source documents; extracted entities and passages linked to the Blob they came from; embedding columns; the usual way a new source enters before a sync exists |
| Sync | Keeps part of the model in step with a system that stays the record of its own data (CRM, ticketing, mail and calendar, documents, code hosting, a warehouse), as a pipeline it runs or supervises: backfill, then incremental pulls or pushed events, then reconciliation; and feeds other systems from the graph | Idempotent keyed upserts under the source's external identity, every few minutes or per event, mostly rows that did not change; the source's deletes propagated; whole-source reruns; outbound, an ordered read of everything that changed since a cursor |
| Author | Records what it learned, decided and did: observations, claims with their evidence, confidence and competing alternatives, decisions with reasons, tasks, plans and checkpoints it can resume from, status, and links between things that already exist | Small attributed commits of one to tens of entities, many per task, on the task's branch, append-mostly, each tied to the sources it came from; the most common write and the densest edges |
| Derive | Computes over what is there: classifies, scores, resolves duplicates, summarizes groups of related entities, infers edges, and consolidates what it observed into new, cited insights (what the agent-memory literature calls reflection [27]) | Reads wide across a type, writes many small updates or a batch per run; periodic, often as a pipeline; later reads depend on how fresh its output is |
| Coordinate | Shares work with other agents and people: sees who is working on what, claims a task so that no one else takes it, hands one off, checks whether something was already done, avoids writing over a concurrent change | Reads of others' activity; an exclusive claim that exactly one of several claimants wins; conditional writes from the commit it read; one branch per task, so one writer per task branch by construction of the work, not by a lock, while the shared branch takes merges and several independent sync writers; a conflict re-decided from a fresh read, not retried blind |
| React | Wakes on a change instead of on a schedule: a new document to extract, a task assigned, a claim contradicted, a branch ready for review, a sync that landed. Reads what changed, decides, and writes, which may wake others | A standing wait on the change feed of a branch, selective enough that an agent wakes only for what concerns it: filtered by type and operation and by a condition over the changed state; delivery in commit order, each change at least once, soon after its commit; a read of the changed entities at that commit; a write conditional on it. Chains of reactions make write load follow write load |
| Verify and correct | Checks work before others rely on it, its own and other agents': against sources, against constraints, by an independent checker before a branch is integrated; finds drift and duplicates, fixes errors. Unchecked work by parallel agents compounds errors, which makes this larger than it looks [21] [22] | Targeted reads by key and by external identity; a check that runs over a branch and gates its merge; corrections as updates or new facts; deletes rarely; constraint refusals as signal |
| Propose and review | Puts a change up for a person or another agent to accept; reads what a branch would change; approves, merges, or discards | Branch diffs and merge previews; a merge or a branch delete; unpublished commits that wait |
| Look back | Asks what was known at a time, what was decided and why, what happened after; compares a past state with now; evaluates its own earlier actions and those of others | Reads at a past commit; diffs across history; commit changes with actors; questions about when a fact held true, not only when it was recorded; with orient, the second most common read |
| Maintain | Adjusts the model as the domain is understood better: adds a type or property, widens an enum, re-embeds, rebuilds coverage, prunes | Additive schema applies; index and layout maintenance; rare, reviewed destructive changes and cleanup |
| Report | Answers a person's question or produces a report or dashboard: the analytical mode of retrieve, with larger results and no step waiting on it | Aggregation over whole types; larger results; scheduled or occasional |

People do a subset by hand or through a UI, at a low rate: orient, retrieve,
author (a correction, an annotation), propose and review (the approvals that
make agent work trusted), and report. Programs without an agent in them do
acquire, sync, derive and react on schedules or events, and write to shared
state directly. The graph sits beside the systems it syncs with and
claims no authority over their records: a mirrored record is true as of its
sync and owned by its source, authored knowledge is owned by the graph, and
the two provenances stay distinguishable, because agents read both together
and write only the second, or propose changes to the first. Values
a pipeline derives inside the graph are a third provenance, reproducible
from the commit they read. An agent changes a mirrored record at its source
and lets the sync bring it back, because writing both places is a dual
write that can leave them disagreeing [31]. A sync is safe to replay because
its upserts are keyed, not because it is one commit, and a replay of an
older batch overwrites a newer value unless the update checks a stored
source version.

### Quantities

| Quantity | Hypothesis | Revise when |
|---|---|---|
| Actors | A few to tens of agents per graph at once, most of them reading for one task while one lead writes it (three to five sub-agents is common, more than ten for a hard task [23]); a few people; a handful of programs; many graphs per deployment rather than one | Hundreds of concurrent agents on one graph; many agents writing one task's branch |
| Read capacity | Tens of agents at several retrieve reads a step is more read load than one process should carry, and it grows with agents while write load grows with tasks; reads are served by several processes | One process serves a graph's reads with headroom at the hypothesized actors |
| Mix | Reads outnumber writes by one to two orders of magnitude; most reads are lookups and exploration, largely overlapping across agents, and a real minority, one in ten or more, are analytical reads over whole types; authored commits dominate writes by count, acquisition and sync by rows; deletes are rare everywhere | Analytical reads become the majority; a write on every agent turn brings reads and writes near one to one; updates or deletes become common; result sets pass 10^4 rows |
| Volume | 10^4 to 10^7 entities per graph, set by the sources; 10 to 100 types; small scalar properties, text bodies, embeddings on a few types, Blobs for documents; agent batches of 10^2 to 10^5 records, whole-source imports and backfills to 10^7 records and 10^4 documents | 10^8 entities or 10^3 types; batches past 10^6; sync as streaming at thousands of events a second |
| Rate | 10^2 to 10^4 commits a day; two kinds of branch: tens of task branches with hundreds of commits each and one writer, and many more short speculative branches of a few commits, most of them discarded, about ten per database project in one vendor's telemetry, some nested deep [24]; incremental sync every few minutes; an additive schema change about weekly; history retained long and read as often as the present | Commit rate or branch count up an order; several writers on one task branch; conflict retries common; destructive schema changes frequent; history read rarely; branches rarely abandoned |
| Fan-out | Edges per entity are skewed: most entities have tens, a few hubs have 10^4 to 10^5. A lookup costs what the hubs it touches cost, not the mean degree | Hub degree passes 10^6, or hubs dominate lookups |
| Maintenance debt | The cost of a read can grow with the writes since the last maintenance run, and small commits arrive all day | The slowest reads grow with time since the last `optimize` |
| Reaction | Tens of standing consumers per graph (agents, pipelines, downstream systems), each filtered to a few types; a change reaches its consumers within seconds of its commit; reaction chains a few steps deep | Hundreds of consumers; sub-second delivery needed; reaction storms; re-reading a baseline at every schema change, which each consumer must do today, costs more than delivery |
| Targets | No latency is measured yet. As a starting hypothesis for a server already serving the graph, near its store: a retrieve read answers in about 100 ms at the median and under 1 s at the 99th percentile, measured at the client; a step of several reads waits for its slowest one, so the read target is set for a step of five reads to stay under 1 s at its own 99th percentile; (one agent-memory system reports search latencies of 0.15 to 0.5 s [30]); a small commit publishes in under 1 s; creating and discarding a branch costs about what a small commit costs; one branch publishes its commits serially, a few a second at most, which the hypothesized rate needs on average at one every several seconds, with bursts on shared state from merges and syncs | Measured figures replace these; shared state needs sustained tens of commits a second |

### What we optimize for

In this order, and a later item never buys an earlier one: the contract
(correctness), then the fewest mechanisms that keep them (simplicity), then
cost on this work. On cost the direction is: retrieve reads first, lookups and
then analytical reads, because both sit on the critical path of an agent step;
then the cost of a small commit and of creating, writing and discarding a
branch, because authoring, speculation and coordination are the common writes;
then history, change discovery and delivery of changes to those waiting on
them; then bulk acquisition and sync, whose cost should follow what changed,
not what was sent. Read capacity scales out by adding readers; write
capacity on one branch does not, and need not at the hypothesized rate. Reads
that overlap, across agents or across one agent's attempts, should share work.
An analytical read may cost more than a lookup but not in proportion to
history, and no design
makes lookups slower to make analytical reads faster. Warehouse-scale scans and
single-branch write throughput beyond the hypothesized rate are not optimized
for, and a design may trade them away.

What the work needs to stay cheap, each with its cost owner in
[testing](testing.md#cost-tests-and-benchmarks): a read costs the same
however long the history is, and a reader that has just started is useful at
once; a commit costs the same however deep the history and however many
branches exist; a branch costs nothing until it is written; an upsert of a
record that did not change costs little and publishes nothing (not so today,
[`D2`](contract.md#decisions)); finding what changed costs the changes, not
the size of the graph; a search costs the matches, not the size of the type.
Wall time and memory are evidence in the benchmark harness, not contract
statements.

## What the work requires

Each phrase of the sentence asks something of the shared state. This table is
the derivation; the contract's rules and cells are its right-hand column made
precise. A statement in the contract is justified by a row here, or it does
not belong there.

| The work says | So the shared state must | In the contract |
|---|---|---|
| **Multiple agents**, in parallel, restarting at any time | Let many act at once without corrupting each other: each step's writes land whole or not at all, a retried request is not applied twice, and every change names who made it | `E2`, `E4`, `E6`; asked: a request identity |
| **Sharing** one model | Be one authoritative state that every agent reads and writes through the same rules, with access scoped to what each agent may see and change | `E1`, `E6` |
| A **typed world model** | Let agents find what the model holds by searching its types, properties and descriptions; enforce the model on every write, by every writer; let the model change as understanding grows, as reviewably as the data does | `E3`, `schema`, `D6`; asked: search over the schema |
| **Continuously**, over a long horizon | Stay available while it is written, maintained and evolved; keep what was known and when, so an agent can resume, reflect, audit and undo; never grow slower with age | `E4`, `E5`, `E7`, [two at once](contract.md#concurrent-operations); asked: revert |
| **Retrieve** context | Answer selective, combined-mode and analytical reads from one coherent, current view, at a latency a step can afford, from as many readers as the agents need | `E1`, `read`, `D13` |
| **Update** context | Accept small frequent writes, whole-source imports of structured and unstructured data, and sync pipelines that mostly rewrite what did not change, each at a cost that follows what changed; let an agent protect a decision against the state it was based on | `E2`, `write`, `write:3`, `D2`, `D4` |
| **Coordinate work**, with awareness | See what others are working on, so that work is not duplicated: active branches with their owner, task and what they touch, and live claims | Asked: work in progress, exclusive claims |
| **Coordinate work** | Give agents long isolated workspaces that integrate without losing or silently overriding each other's work; let an agent or a pipeline react to a change soon after it commits, in order, without polling and without missing one; make a claim on a task exclusive, with exactly one winner; integrate a workspace only after an independent check passes; make trying an idea and discarding it nearly free | `merge`, `branch`, `read:5`; asked: a change feed that waits, exclusive claims, merge checks |

## Asked, not decided

New capabilities the work asks for. None is a statement in
[the contract](contract.md) yet. One becomes part of the contract when it is
written there as a statement, with a gap for what the code does not do yet. The contract's own [open decisions](contract.md#decisions) are questions
about statements that already exist; the work asks for several of those too
(`D2`, `D4` to `D8`, `D13`).

| Ask | Today | The work asks for |
|---|---|---|
| Revert | There is no revert. Undoing is a hand-written inverse commit | One operation that publishes the inverse of a commit or a merge as a new commit, refused where later changes depend on it |
| A change feed that waits | The feed is polled and filters by kind, type and operation, with no condition over state | A consumer waits for the next matching change, filtered by a condition over the changed state, and learns of a commit soon after it is acknowledged |
| Task and reason on a commit | A commit records an actor only | The task and the reason, readable wherever the commit is |
| Work in progress | A branch listing shows head, origin and times, not owner, task or touched entities | Which branches are live, who owns each, for what task, and which entities it has touched, recorded as the work happens |
| A request identity | Graph writes carry no request identity: a lost answer is resolved by reading history, and only a conditional write or a keyed upsert is safe to repeat, the second at the cost of a commit. Recorded operations do carry one | A request sent again after a lost answer applies at most once and returns the first answer |
| Exclusive claims | A claim is a conditional write on the whole branch head, so claimants on a busy branch refuse each other and unrelated writers refuse them | When several agents claim one task or entity, exactly one succeeds and the others learn who holds it; an unrelated commit never makes a claim fail |
| Merge checks | A merge is gated by policy on who may merge, not by a check on what is merged | The target names a check the branch must pass, run by a party other than its author |
| Declared dependencies | A merge records and checks no read dependency, so a conclusion can merge on evidence that changed on the target | A branch records what its conclusions depend on, and a merge reports a dependency that changed on the target since the fork |
| Search over the schema | Types and properties carry descriptions, but nothing searches the schema by meaning | Search over the schema's names and descriptions |
| Valid time | A schema models when a fact held true as ordinary properties, and nothing ranks or filters by them specially. A mirrored record has three times: when the fact held at the source, when the source recorded it, and when the graph committed it; a backfill bunches commit times, so "what changed last week" by commit time is wrong for synced data | Decide whether valid time stays a schema convention or becomes a construct |

## What the evidence says

The hypothesis was checked against the literature on 2026-11-16 ([sources](#sources)).
No source measures several agents sharing one persistent model, so the
premise is untested, not confirmed.

- **Supported.** Agents explore schema and metadata heavily, issue many
  redundant and partial reads, and create and discard branches at many times
  the human rate [18] [24]. Shared structured state coordinates better than
  messages passed between agents [20] [23]. Task verification is one of
  the three failure categories of multi-agent systems, though not the
  largest [21], and centralized verification contains errors that agents
  working without it amplify [22]. Long-term memory needs time-aware
  retrieval and facts that are superseded, not erased [28]. Coordination
  through a shared medium needs change-triggered activation with a
  condition, an atomic claim, and hypotheses held side by side with
  confidence, as blackboard systems and tuple spaces established [25] [26].
- **Against.** The production multi-agent systems described in [23] are
  several readers and one writing lead per task, and strong practitioners
  argue against agents writing in parallel at all [23]; the measured gain
  from adding agents averages about zero, with wide variance, and is
  negative for sequential work [22]. The read-to-write
  ratio is unmeasured, and memory pipelines that write on every turn would
  bring it near one to one. Most agent-memory systems are schemaless: that
  the shared model is **typed** is this page's bet, argued from
  standardization as the way to make one party's output usable by another
  [29], not an observation. Schema-on-read is the
  better fit for heterogeneous data and for structure owned by an outside
  system that changes it at will [31], which is the work of acquire and
  sync: the bet is that authored knowledge is typed, and that outside data
  enters as a raw Blob or a loosely typed property and becomes typed by
  extraction.
- **Outside the store.** Scheduling and control (which agent runs next, and
  on what) is a separate component in every coordination architecture
  [25]. The contract owes it the facts it decides from, visible in the
  state: pending work, claims and who is working on what.

## Against the usual categories

OLTP, OLAP, HTAP and LTAP each name a workload and usually imply a contract
and a mechanism ([systems](systems.md#appendix-a-terminology-expansion)).
Omnigraph is defined by its contract, its mechanisms and this work, not by a
bundle, and matches each bundle in some claims and not in others. The first
three are categories of the literature; LTAP is one vendor's architecture.

| Category | What it bundles | Where Omnigraph matches | Where it does not |
|---|---|---|---|
| OLTP [10] [11] [31] | Work: many short, low-latency reads and writes of a few records by key, on current data, from end users. Contract: usually atomic, isolated transactions, serializable or weaker, though the category does not require them; often interactive sessions, though some engines run stored procedures one at a time. Mechanism: row storage and a write-ahead log, with B-trees updated in place or log-structured merge trees | The footprint: small reads by key and small commits on a critical path, each seeing current data; serial execution per branch, as in the engines that run transactions one at a time [12] | The transaction is one request, not an interactive session. A branch's commits are serialized by a compare-and-swap on object storage, so a commit costs object-store round trips and one branch sustains a few commits a second, the rate Delta Lake reports for the same mechanism [15], not thousands. Write concurrency comes from branches |
| OLAP [10] [16] [31] | Work: few complex ad hoc queries that scan, join and aggregate much historical data consolidated from many sources, loaded in batches; response time is the measure. Contract: read-mostly; a consistent snapshot of the last load, refreshed periodically; often a multidimensional model. Mechanism: star schemas, bitmap indexes, materialized views, parallel scans; later columnar storage and vectorized execution; in the cloud, compute apart from storage | The mechanism: columnar immutable files, batch loads, compute apart from storage; analytical reads are part of the work | The scale, the model and the lag: analytical reads cover one graph of up to 10^7 entities, not a warehouse consolidating many sources; there is no multidimensional model and no pre-aggregated summary; unlike a warehouse fed by periodic loads, there is no second copy for an analytical read to lag behind |
| HTAP [12] [13] [14] | Work: transactions and analytical queries on the same data at once, analytics on the latest committed data. Contract: transactions plus analytical reads of a fresh, consistent snapshot; the named requirements are freshness and performance isolation. Mechanism, two families: one copy with snapshots, or a row store plus a columnar replica kept consistent by replication | The mixed read work on one system over one copy, every read at a fresh snapshot: the single-copy family, not the replicated one | With no replica there is no replication lag. Isolation of lookups from analytical reads would come from separate stateless reader processes over the same files, not from separate representations, and it is unmeasured. The transactional half is weaker than OLTP as above, so HTAP's hard case, analytics beside a high write rate, is not this work |
| LTAP [17] | Databricks' term (June 2026), Lake Transactional/Analytical Processing: separate transactional and analytical engines over one durable copy in open table formats on object storage. Postgres writes with full transactions through a replicated log and a page server that transcodes pages to columns; analytical engines read the same files, scaled independently, at a log position plus the tail not yet written out; row pages are a cache. It unifies at the storage layer, not in one engine | The shape: one copy in an open format on object storage, one write path, readers that scale out, no export to a second system | The unit and the engines: one engine and a typed graph commit published by compare-and-swap, with no log and no row tier, against multi-statement Postgres transactions over a replicated log. Its branches are copies for experiments; Omnigraph's merge three-way. Retrieval over structure, vectors and text is a first-class read, which the term does not name |

What none of the four names, and what this work is mostly made of: isolation
by branch and merge instead of by lock or session; the history of the state's versions read as often as
the present, where a warehouse's history is a table of events and the others keep versions for rollback and reproduction;
ranked retrieval as the common read; and readers and writers that are
programs acting on what they just read. Two existing labels sit closer than
the four and are still only shorthand: data versioning systems, which share
the branch, commit and merge contract [9], and hybrid serving and analytical
processing, which names selective latency-bound reads beside analytics. Against composing
specialized stores through a log [31], Omnigraph bundles graph, vector,
full-text and history over one copy: one snapshot covers every mode and
nothing lags between them, at the cost that retrieval is as deep as one
engine provides. Seen from outside it is itself one component of such a
dataflow, with syncs in and a feed out. Use
a category word only as shorthand for the row it points at; a design argues
from the contract and the work, never from "it is an HTAP system".

## Sources

The number is the citation used in the text. Numbers 1 to 8 are no longer
used here: they were the sources for the contract's terms, which are now
listed in [the contract](contract.md#sources). The method's own sources are
in [systems](systems.md#references-and-source-scope).

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

On the work itself, read on 2026-11-16:

18. Liu et al. (UC Berkeley), "Supporting Our AI Overlords: Redesigning Data
    Systems to be Agent-First", CIDR 2026; and Parameswaran et al.,
    "Intelligence is Free, Now What? Data Systems for, of, and by Agents",
    BAIR blog, 2026.
19. Lei et al., "Spider 2.0", ICLR 2025: agent query behavior on large
    schemas.
20. Hong et al., "MetaGPT", ICLR 2024; Wu et al., "AutoGen", 2023: shared
    structured pools and message-driven activation.
21. Cemri et al., "Why Do Multi-Agent LLM Systems Fail?", NeurIPS 2025.
22. Kim et al., "Towards a Science of Scaling Agent Systems", 2025.
23. Anthropic, "How we built our multi-agent research system" and
    "Effective context engineering for AI agents"; Cognition, "Don't Build
    Multi-Agents", the opposing view.
24. Pandis, Shamgunov and Xin, "How agentic software development will change
    databases", Databricks, 2026 (vendor telemetry); Tagliabue and Greco,
    "Safe, Untrusted, 'Proof-Carrying' AI Agents", 2025; Chang and Geng,
    "SagaLLM", PVLDB 2025; Rezazadeh et al., "Collaborative Memory", 2025.
25. Erman et al., "The Hearsay-II Speech-Understanding System", Computing
    Surveys 1980; Han and Zhang, and Salemi et al., blackboard systems for
    LLM agents, 2025.
26. Gelernter, "Generative Communication in Linda", TOPLAS 1985.
27. Park et al., "Generative Agents", UIST 2023; Packer et al., "MemGPT",
    2023; Sumers et al., "Cognitive Architectures for Language Agents",
    TMLR 2024; Edge et al., "From Local to Global: A GraphRAG Approach",
    2024; Gutierrez et al., "HippoRAG", NeurIPS 2024.
28. Rasmussen et al., "Zep: A Temporal Knowledge Graph Architecture for
    Agent Memory", 2025; Wu et al., "LongMemEval", ICLR 2025; Maharana et
    al., "LoCoMo", ACL 2024.
29. Malone and Crowston, "The Interdisciplinary Study of Coordination",
    Computing Surveys 1994; Dourish and Bellotti, "Awareness and
    Coordination in Shared Workspaces", CSCW 1992; Ellis and Gibbs,
    "Concurrency Control in Groupware Systems", SIGMOD 1989.
30. Chhikara et al., "Mem0", 2025: measured memory search latency.

31. Kleppmann, "Designing Data-Intensive Applications", O'Reilly 2017:
    describing load and tail latency (ch. 1), schema-on-read (ch. 2), OLTP,
    OLAP and compaction (ch. 3), evolution across program versions (ch. 4),
    conflict handling (ch. 5), isolation anomalies, serial execution and
    durability (ch. 7), clocks, leases and fencing (ch. 8), linearizability,
    ordering and consensus (ch. 9), change capture and effectively-once
    delivery (ch. 11), the end-to-end argument, timeliness against integrity
    and auditability (ch. 12).

Not grounded in a source read here: the
read-to-write ratio, the latency targets and the rate figures in the work,
which no source measures for a shared model and which remain hypotheses.
