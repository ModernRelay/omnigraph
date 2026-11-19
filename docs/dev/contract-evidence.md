# The contract and the work: evidence and sources

**Audience:** reviewers of [the contract and the work](contract.md) and authors of an RFC that changes it
**Authority:** the evidence behind that page's workload hypothesis, its placement against the usual database categories, and the sources its terms and numbered citations come from. Not required reading for a change; the clauses and the today table are on the main page.

## What the evidence says

The hypothesis was checked against the literature on 2026-11-16 ([sources](#sources)).
No source measures several agents sharing one persistent model, so the
premise is untested, not confirmed.

- **Supported.** Agents explore schema and metadata heavily, issue many
  redundant and partial reads, and create and discard branches at many times
  the human rate [18] [24]. Shared structured state coordinates better than
  messages passed between agents [20] [23]. Verification is a leading cause
  of multi-agent failure, and independent checking contains errors that
  unchecked agents amplify [21] [22]. Long-term memory needs time-aware
  retrieval and facts that are superseded, not erased [28]. Coordination
  through a shared medium needs change-triggered activation with a
  condition, an atomic claim, and hypotheses held side by side with
  confidence, as blackboard systems and tuple spaces established [25] [26].
- **Against.** Production multi-agent systems are mostly several readers and
  one writing lead per task, and strong practitioners argue against agents
  writing in parallel at all [23]; measured gains from adding agents are
  small on average and negative for sequential work [22]. The read-to-write
  ratio is unmeasured, and memory pipelines that write on every turn would
  bring it near one to one. Most agent-memory systems are schemaless: that
  the shared model is **typed** is this page's bet, argued from
  standardization as the way to make one party's output usable by another
  [29], not an observation.
- **Outside the store.** Scheduling and control (which agent runs next, and
  on what) is a separate component in every coordination architecture
  [25]. The contract owes it the facts it decides from, visible in the
  state: pending work, claims and who is working on what.

## Against the usual categories

OLTP, OLAP, HTAP and LTAP each name a workload and usually imply a contract
and a mechanism ([systems](systems.md#appendix-a-terminology-expansion)).
Omnigraph is defined by the three parts on [the main page](contract.md), not by a bundle, and matches
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

Not grounded in a source read here: the definition of durability; the
read-to-write ratio, the latency targets and the rate figures in the work,
which no source measures for a shared model and which remain hypotheses.
