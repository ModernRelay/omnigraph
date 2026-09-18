# Evaluating Data Systems from First Principles

## Contracts, physical mechanisms, and distributions of work

**Technical draft · 13 September 2026**

### Abstract

Database terminology mixes logical models, execution guarantees, implementation techniques, workload patterns, and commercial packaging. These descriptors are useful individually but unsuitable as mutually exclusive classes: a system can expose relational and graph operations, maintain both row and column representations, and serve transactional and analytical work through different execution paths. This paper proposes a structured evaluation model built around three descriptions: an observable contract, its physical realization, and the distribution of work it serves. Contracts specify state, operations, execution guarantees, and interfaces at a declared system boundary. Implementations are evaluated against those contracts under explicit environmental assumptions; performance, quality, and cost are measured under specified workloads. Composition records how obligations pass among components, while a constrained design space distinguishes improvements enabled by architectural change from trade-offs within a fixed design. Examples drawn from transactional stores, lakehouse architectures, open table formats, incremental computation, and approximate retrieval test the separation boundaries. The contribution is an integrated method for constructing and comparing system profiles, rather than a new classification of database products or a claim of a unique mathematical basis.

## 1. The classification problem

A database shortlist might include PostgreSQL, a graph database, a columnar warehouse, a serverless database, and Iceberg. The descriptions appear comparable because they inhabit the same market. Technically, they answer different questions.

| Description | Information actually supplied |
|---|---|
| Relational, document, property graph | Parts of a logical representation and its associated operations |
| Columnar, LSM-based, in-memory | Physical organization or implementation mechanisms |
| Serializable, eventually consistent | Constraints on executions, qualified by scope and assumptions |
| OLTP, OLAP, event store | Workload patterns, sometimes bundled with application semantics |
| Embedded, distributed, serverless | Deployment boundaries, resource allocation, or operational responsibility |
| Open table format, lakehouse | An interoperability contract or an architectural composition |

The resulting errors are substantive. A logical model does not establish a transaction boundary. A query language does not determine storage layout. Replication does not establish what an acknowledgement survives. Supporting an operation does not establish that it is economical under the intended load.

This confusion has a historical explanation. Database categories often name combinations that proved useful under a particular hardware, workload, or organizational regime. Those combinations persist in language after their components become separately configurable. Codd's original account of data independence already separated logical access from physical access paths; it also explicitly allowed logically symmetric access to have asymmetric performance. Later architectural surveys describe extensive specialization beneath similar interfaces. [1], [3], [4], [5]

The evaluation problem is therefore to recover the constituent claims hidden by a category. The question is whether a particular configuration supplies the required behavior, at an acceptable cost, under the work and failures it will encounter.

## 2. Unit of analysis and core model

> **A database is an observable contract, implemented by physical mechanisms, serving a distribution of work.**

This is an operational definition for evaluation. It also applies to data-system components, provided their incomplete responsibilities remain explicit. A file format, transaction substrate, or execution library need not constitute an entire database service to receive a useful profile.

### 2.1 Fix the boundary before assigning properties

Let **B** identify the subject of evaluation:

$$
\begin{aligned}
B=(&\text{component/service},\ \text{edition},\ \text{configuration},\\
&\text{deployment},\ \text{observer},\ \text{access path},\ \text{operation scope}).
\end{aligned}
$$

The observer may be a SQL client, a storage client, an administrator, a replica reader, or a direct file reader. Scope may be an object, transaction, table, database, or application workflow. One deployment generally has multiple profiles because its interfaces expose different behavior.

For a fixed boundary, write the profile as

$$
\Pi_B=\langle C,P,W;A,E\rangle,\qquad C=(S,O,G,I).
$$

| Field | Definition | Representative contents |
|---|---|---|
| **S — State** | Logical information and its interpretation | Values, types, identities, relationships, multiplicities, schema, retained versions, invariants |
| **O — Operations** | Meaning of permitted observations and transformations | Reads, mutations, joins, traversal, inference, search, subscription, schema change, restore |
| **G — Guarantees** | Constraints on executions and observable outcomes | Atomicity, isolation, ordering, integrity preservation, durability, convergence, progress |
| **I — Interfaces** | Public means of expressing and exchanging the contract | Language dialects, APIs, protocols, public encodings, compatibility rules |
| **P — Physical mechanisms** | Algorithms, representations, and resource arrangements implementing C | Layouts, indexes, plans, concurrency control, replication, recovery, scheduling, maintenance |
| **W — Work distribution** | Distribution or generative description of demands | Data, request sequences, arrivals, dependencies, contention, lifecycle operations |
| **A — Assumptions** | Conditions qualifying guarantees and evaluation | Failure model, network and clock behavior, client obligations, resource conditions, trust boundaries |
| **E — Operating envelope** | Qualified evidence about achieved outcomes | Latency, throughput, freshness, quality, resource use, cost, recovery, saturation |

Only **C, P, and W** are the three primary descriptions. **B** selects what is being described; **A** qualifies the claims; **E** records their operational consequences. Evidence attaches to each claim. Licensing, pricing rules, and operational responsibility accompany deployment decisions without becoming additional semantic dimensions.

These fields are typed descriptions, not independent real-valued coordinates. Their separation concerns the questions being answered. A familiar term may expand into several fields: Codd's later definition of a *data model* includes structures, operators or inference rules, and integrity constraints, and consequently spans S, O, and G. [2]

### 2.2 Contract satisfaction

For ordinary trace properties, let $H_C$ be the histories permitted by a contract. Histories include invocation and response events and the failures, recovery observations, and publication events relevant to the boundary. Let $\operatorname{Exec}(P,A)$ denote executions admitted by an implementation under assumptions A, and let $\operatorname{obs}_B$ project an execution onto the chosen observer.

An implementation satisfies the trace part of C when

$$
\operatorname{obs}_B(\operatorname{Exec}(P,A))\subseteq H_C.
$$

The abstraction mapping from physical to logical state must also preserve the meaning specified by S and O. Histories include infinite executions when expressing liveness; safety-only inclusion would allow a system that avoids incorrect results by never responding.

This formulation distinguishes an obligation from an implementation and from observations of that implementation. It is not a universal reduction of all requirements to single-execution trace inclusion. Probabilistic quality bounds require distributions over executions; information-flow requirements may compare multiple executions. Such requirements retain their own formal model within G.

Contract strength is likewise scoped. At the same boundary, sequential semantics, and assumptions, $H_{C_1}\subseteq H_{C_2}$ expresses that $C_1$ permits fewer histories. Changing assumptions or adding operations prevents an unqualified inference that one entire product is “stronger.”

## 3. The observable contract

### 3.1 State: representation, identity, and invariants

S describes logical state independently of its physical encoding. Relevant distinctions include sets versus bags, ordered versus unordered collections, entity identity versus structural equality, explicit relationships versus derived relationships, missing values versus null values, and schema identity across evolution.

Identity requires particular care. An entity key, a schema field identifier, a version identifier, and a physical row position are different objects. A storage rewrite may preserve entity identity while changing row positions; a rename may preserve field identity; replacing an entity with the same external label may or may not preserve its identity. These are semantic decisions.

An invariant $\mathcal{K}\subseteq S$ defines acceptable states. For example, customer identifiers may be unique. O defines permitted transitions; G determines at which boundaries the invariant must hold and whether concurrent execution preserves it. Deferred constraint checking can allow intermediate states that must be repaired before commit. The physical enforcement method belongs in P.

Temporal and provenance features fit this same structure. Valid time describes when a fact holds in the modeled world; transaction time describes its recorded database history. Neither is automatically equivalent to an event timestamp, processing time, or a logical iteration counter. Provenance can be part of the returned logical result, with its own algebra for combining derivations. A record's creation timestamp alone supplies neither temporal query semantics nor derivation provenance. [12], [13]

### 3.2 Operations: semantics before language

O specifies each operation's inputs, outputs, effects, errors, and relevant preconditions. A state-changing operation can be represented schematically as a relation

$$
\llbracket o\rrbracket\subseteq S\times\operatorname{Input}_o
\times S\times\operatorname{Output}_o,
$$

allowing nondeterministic results where the specification permits them. Concurrent guarantees subsequently constrain how operations compose.

Operation profiles must specify details that alter correctness: duplicate handling, ordering, numerical behavior, recursion semantics, approximate result criteria, side effects, and whether a result is provisional. For streaming computation, windows, triggers, accumulation, and retraction determine what an emitted answer means. The Dataflow model explicitly separates these choices from the execution of bounded or unbounded input. [18]

Administrative operations belong here when they change observable behavior: schema migration, branch merge, retention, backup, restore, and access-policy changes. Compaction may be required to preserve all logical observations; retention deliberately removes some historical observations. Their shared use of file deletion does not make them the same operation.

Expressibility and suitability must remain separate. A language capable of expressing recursion does not establish a particular evaluation strategy or acceptable recursive-query latency. Positive Datalog inference, provenance propagation, and incremental execution are related subjects with different responsibilities. The language fragment and semantics must be stated before assessing an implementation. [12], [19]

### 3.3 Guarantees: histories, failures, and progress

G contains several families of obligations; a single “consistency” score cannot represent them.

| Family | Required specification |
|---|---|
| Integrity | Which state or transition invariants are preserved, and when they are checked |
| Atomicity | Which effects become visible, commit, or roll back together |
| Isolation and ordering | Which interleavings, versions, session dependencies, and real-time orders are permitted |
| Durability and recovery | What a successful acknowledgement establishes under each supported failure |
| Convergence | Which replicas or derived results must agree, after which updates, under which delivery assumptions |
| Progress | Which operations must terminate or succeed under partition, contention, recovery, and overload |
| Quantitative obligations | Any promised latency, freshness, accuracy, or availability bound and its qualifying conditions |

Serializability requires transaction outcomes to correspond to an allowed serial execution. Linearizability requires operations on a specified object to appear atomic between invocation and response, respecting real-time precedence. Strict serializability applies both serial equivalence and real-time precedence to transactions. The object specification still determines what an operation means; a historical read can legitimately return an old version. [7], [8]

For a latest-value register initially holding 0, consider separate clients:

```text
T1: write(x, 1); commit succeeds
                              T2: read(x) returns 0; commit succeeds
```

The serial order T2, T1 explains the returned values. Pure serializability can therefore admit this history even though that order reverses real-time precedence. Strict serializability forbids it. Independently linearizable reads and writes still do not supply an atomic transaction spanning several objects.

A different example separates snapshot isolation from invariant preservation. Let $x,y\in\{0,1\}$, initially both 1, with invariant $x+y\geq1$. Two concurrent transactions read the initial snapshot; one writes x = 0 and the other y = 0. Their write sets are disjoint. Snapshot isolation can admit both commits, violating the invariant. The analysis requires the operations and invariant together, not only an isolation label.

Guarantees must also name the observations they cover. A property stated over committed transactions does not automatically protect intermediate results used before commit. An aborted transaction's database effects, sequence allocations, and external side effects can have different scopes. PostgreSQL's isolation documentation makes such exceptions explicit. [32]

Finally, safety does not imply timely service. A replica can wait until it reaches a requested version and return a coherent snapshot; the wait remains a latency and progress question. A successful commit can be durable against a process crash yet vulnerable to a larger failure domain. A full profile states these obligations separately.

### 3.4 Interfaces and exposed representation

I specifies the public language, protocol, encoding, and compatibility rules through which the other contract fields are available. Two endpoints accepting SQL need not implement identical types, null semantics, operations, or isolation. A common intermediate representation similarly requires agreement on function semantics and versioning. [17]

Whether a representation is physical or contractual depends on B. Files hidden behind a SQL endpoint are implementation details for that observer. Direct file access creates another interface whose format and interpretation become public obligations. The Lakehouse paper explicitly recognizes the resulting restriction on physical data independence. [6]

Authorization follows the same rule. A SQL policy is an effective guarantee only for paths that enforce it or are controlled by equivalent protections. Describing a composed system therefore requires the intended observers, trust boundaries, and permitted bypass paths.

## 4. Physical realization and work distribution

### 4.1 Physical mechanisms

P includes the software and hardware arrangements that realize C, not merely the storage medium.

| Responsibility | Mechanisms to describe |
|---|---|
| Representation and access | Row/column grouping, compression, partitioning, indexes, materialized views, caches |
| Planning and execution | Statistics, optimization, join and traversal algorithms, compilation, vectorization, parallelism, incremental evaluation |
| Coordination and publication | Locks, validation, version selection, consensus, commit protocols, conflict resolution |
| Durability and recovery | Logs, replication, checkpoints, recovery ownership, replay, repair |
| Resource and lifecycle management | Admission, scheduling, placement, memory and I/O budgets, scaling, compaction, garbage collection, rebalance |

This breadth is necessary because a storage-centric profile cannot explain optimizer failures, memory thrashing, lock contention, or maintenance interference. The established database architecture literature places these responsibilities throughout the execution path. [3]

Mechanism names do not establish guarantees. MVCC can support multiple isolation levels; OCC depends on the conflicts actually validated; replication depends on acknowledgement and read rules. Even correct scalar quorum arithmetic is insufficient when the participating replica sets change, as the original Dynamo's sloppy-quorum design illustrates. [31], [32]

Physical substitution is a useful boundary test. If an index can be replaced by a scan while preserving all required observations, the index serves a physical role at that boundary. If removing it changes result semantics or violates a promised quality bound, its replacement requires additional semantic reasoning. In either case, losing it may make the operating target unattainable.

### 4.2 A generative workload model

W must characterize the joint behavior of data and requests. Marginal ratios such as “90% reads” omit query complexity, selectivity, result size, contention, and timing.

A compact generative representation is

$$
W=(\mu_0,\pi,\eta),
$$

where $\mu_0$ describes initial logical data, $\pi$ generates requests and arrival times, and $\eta$ describes exogenous lifecycle demands and workload phases. The request policy may depend on prior observations:

$$
\pi(o_{t+1},a_{t+1}\mid h_t),
$$

with $a_{t+1}$ an arrival time and $h_t$ the client-visible history. This accommodates retries, interactive sessions, exploratory queries, and workflows whose next operation depends on the last result. It avoids assuming that requests are independent or stationary.

The profile should record:

- **Data:** volume, record size, cardinalities, skew, correlation, locality, relationship structure, working set, growth, and churn.
- **Operations:** query shapes, selectivity, output sizes, transaction footprints, recursion or traversal depth, append/update/delete behavior, and historical access.
- **Interaction:** offered load, concurrency, bursts, dependency chains, contention, retries, and feedback.
- **Lifecycle demands:** ingestion phases, schema changes, retention, backup, restore, index creation, and rebalance.

Separate requested work from implementation-induced work. The same logical updates may cause different index maintenance, compaction, replication, and recovery work under different P. Those costs must be measured, but should not be fixed identically when comparing alternative implementations. Failure conditions belong in A and the experiment's scenario; the retries and recovery work they trigger belong in the resulting execution.

YCSB makes operation mixes and access distributions explicit. LDBC's business-intelligence workload further illustrates why graph structure, correlations, parameter selection, and execution mode matter. Neither benchmark name alone specifies an equivalent evaluation across systems. [21], [22]

## 5. Evaluation as constrained design

### 5.1 Feasibility precedes optimization

Let $C_{\mathrm{req}}$ be the required contract, $\theta$ an implementation configuration, and r a resource allocation. For comparison, $B_{\mathrm{req}}$ fixes the observer, operation scope, and required interface semantics; each candidate retains its own complete B, including edition and deployment. The feasible design set is

$$
\mathcal{F}(C_{\mathrm{req}},A,B_{\mathrm{req}})
=\{(P,\theta,r):P_{\theta,r}\models_{B_{\mathrm{req}},A}C_{\mathrm{req}}\}.
$$

This is a specification of the evaluation problem, not a claim that membership is generally decidable or established by a benchmark. Unknown conformance remains unknown. Comparing speed after silently weakening atomicity, durability, or result quality changes the feasible set and therefore changes the question.

For feasible candidates, measure an outcome vector

$$
J(P,\theta,r\mid W,A)
=(L,Q,F,K,U,R,\ldots),
$$

where L describes latency, Q completed useful work per unit time, F freshness, K result quality, U resource use and cost, and R recovery behavior. Each component can itself be a distribution or function of offered load. There is no general scalar performance score without an application-supplied objective.

For example, a service target might require p99 transaction latency below $\ell$, analytical snapshot age below $\delta$, and cost below b, while meeting a specified isolation and failure contract. The operating envelope is the region of workloads, conditions, and allocations for which those targets are supported or observed. A latency target belongs in C when promised; measured latency belongs in E. These are the requirement and its evidence, respectively.

### 5.2 Support, optimization, and saturation

“Supports joins” is an operation claim. “Optimized for joins” requires a workload region and objective: join cardinalities, selectivities, skew, memory pressure, concurrency, and the costs being minimized. The latter claim is relational to alternatives or a target, rather than an intrinsic property of the product name.

Measurements should distinguish offered load, admission, successful completion, retries, and rejected work. A system can reduce measured latency by rejecting more requests; a closed-loop generator can reduce offered load when responses slow. Neither effect can be interpreted from successful-request latency alone. Likewise, a short experiment can hide accumulating compaction debt or an unstable queue.

Quality belongs in the same comparison. For approximate nearest-neighbor retrieval, latency and throughput must be paired with the quality metric and query distribution. Exact distance computation for visited candidates does not establish exact global top-k retrieval. Index construction and maintenance also consume resources outside the serving loop, as the DiskANN design makes clear. [20]

### 5.3 Pareto comparisons require a fixed question

Orient each objective so that smaller values are preferable. Candidate $d_1$ dominates $d_2$ only if it is no worse on every selected objective and strictly better on at least one, at the declared C, W, A, and resource-accounting boundary. Otherwise the candidates can be incomparable.

A trade-off observed while tuning one design does not establish a limit over all designs. A new representation, algorithm, hardware arrangement, or component boundary can change the feasible set. Conversely, expanding that set does not prove that a particular new design dominates existing candidates. The framework requires the improvement and its conditions to be stated explicitly.

## 6. Composition and decoupling

### 6.1 Composition carries obligations

Represent a composed service as components $P_1,\ldots,P_n$ connected by interfaces and protocols L. Each component provides guarantees under assumptions supplied by its environment. The resulting service has behavior

$$
\operatorname{Compose}(P_1,\ldots,P_n;L),
$$

whose contract must be established at the application's boundary. It cannot be obtained by taking the union of component feature lists.

For each end-to-end claim, record four roles:

| Role | Meaning |
|---|---|
| Defines | Specifies the obligation and its semantics |
| Implements | Performs the work enforcing the obligation |
| Requires | Depends on another component or client satisfying a condition |
| Delegates | Leaves responsibility outside the component being profiled |

A valid composition must discharge these assumptions, align representations, preserve publication and failure semantics, and establish progress for the whole path. Circular dependencies require a joint argument. Two services each waiting for the other do not establish liveness merely because their individual contracts contain conditional progress promises.

Publication authority is often the critical boundary. A group of immutable files does not identify which combination is committed. A metadata record does not become an atomic commit service without a protocol for concurrent replacement and recovery. A transaction at one layer does not automatically include a notification, file export, or external side effect at another.

### 6.2 Preserve a contract through a representation change

Consider logical data D, a transactional representation $R_T$, and an analytical representation $R_A$. Separate representations allow different access and execution optimizations. Their composition must still specify:

1. Which committed version of D each representation contains.
2. Whether a query reads one coherent version across its inputs.
3. How a reader selects or waits for an admissible version.
4. How schema changes, corrections, and deletions propagate.
5. What happens when propagation or publication fails.

If an analytical reader selects logical version v, coherence requires the relevant inputs to represent v according to the query's semantics. Freshness separately constrains how old v may be relative to a named reference point. An asynchronously maintained representation can satisfy both by waiting for sufficient progress, but the wait changes latency and availability. Returning immediately from a lagging representation chooses a different contract unless stale results were already permitted.

This applies equally to replicas, materialized views, search indexes, caches, and exported tables. The required relation between representations is semantic; the propagation mechanism and cost belong in P and E.

### 6.3 What decoupling can improve

Decoupling can remove avoidable work, separate contending resources, or allow specialized representations and independent scaling. These changes can improve several objectives simultaneously. Eliminating a redundant transfer may reduce cost, delay, and failure opportunities; separating long scans from latency-sensitive execution may improve both workloads' service behavior.

The mechanisms differ. FoundationDB separates transaction processing, logging, and storage responsibilities. Aurora moves substantial redo and recovery work into distributed storage. Snowflake separates compute clusters over shared persistent data. Each creates particular optimization opportunities while retaining dependencies on shared authority, storage, or metadata. [24], [25], [26]

The corresponding costs include propagation, additional retained state, coordination, network transfers, compatibility, and operation of more components. These costs may be smaller than the removed costs. They may also dominate. Spanner's discussion of integrating replication and concurrency control supplies a useful counterexample to a blanket preference for separation: integration can expose information that reduces protocol overhead. [29]

The design question is therefore which representations, resources, and authorities should be shared, and which should be independently replaceable or scalable.

### 6.4 Scope the underlying limits

Three established results help distinguish sources of constraint:

- **RUM** concerns read, update, and memory or space amplification in access methods. It is a conjecture about those costs, and explicitly permits simultaneous improvements before reaching a limit. It is not a theorem that every database improvement must worsen another objective. [16]
- **Invariant confluence** relates coordination requirements to the permitted transactions, invariant, reachable states, and merge operation under a specified model. Valid states produced independently must remain valid when merged for the relevant coordination-free execution strategy to preserve the invariant. [9]
- **CALM** relates monotonic computation to coordination-free consistency in its distributed model. Here consistency concerns program outcomes under distributed execution; it does not mean linearizable latest-value reads. Communication and coordination are also different requirements. [10]

These results constrain different objects. Changing an operation—for example, allocating identifiers from disjoint namespaces instead of allowing arbitrary competing choices—can change an invariant-confluence argument. Adding a CRDT can provide convergence under its update and delivery rules while leaving an application invariant unprotected. Changing a physical component alone cannot establish a guarantee whose semantic premises remain unsatisfied. [9], [14]

## 7. Worked boundary tests

The following examples use the cited papers and documentation editions. They test the framework's distinctions; they are not current product rankings.

### 7.1 FoundationDB: a logical layer over a transactional substrate

The 2021 FoundationDB paper describes an ordered key-value service with transactions, implemented through distinct transaction, log, and storage responsibilities. A higher layer can map documents, relations, or graph entities onto that service. The layer defines additional state and operation semantics; the substrate's guarantees apply only where the layer preserves their assumptions and transaction scope. [24]

| Field | Profile consequence |
|---|---|
| C | Distinguish the substrate's key-value transactions from the higher layer's objects, queries, constraints, and side effects |
| P | Record encoding, access paths, transaction footprints, and the substrate's separated responsibilities |
| W | Include contention, transaction size, query fan-out, and layer-induced requests |
| E | Measure the higher-level operation through the complete path |

A transactional substrate is valuable precisely because multiple logical layers can reuse it. It does not make arbitrary higher-level operations atomic or efficient. A graph mutation spanning several substrate transactions still needs its own argument for the graph-level publication contract.

### 7.2 Iceberg and Delta Lake: a format is a distributed obligation

An open-table deployment includes data files, metadata, a publication mechanism, writers, readers, and execution engines. Iceberg's specification assigns requirements to these participants; the atomic commit mechanism and writer validation are essential to the resulting concurrency behavior. Identifier fields describe identity but do not themselves enforce uniqueness. [34]

The 2020 Delta Lake paper provides a concrete access-path distinction. Native log-aware readers obtain table snapshots. Its symlink-manifest integration for partitioned tables provides snapshot consistency at the directory level, so an independent reader using that path cannot inherit the native table-wide claim without further machinery. [30], §4.8

This yields a precise evaluation rule: “supports the format” must expand into supported protocol versions, metadata interpretation, writer validation, publication authority, and reader snapshot selection. Compatibility is directional and operation-specific.

The 2021 Lakehouse paper then composes transactional table management, open storage access, and analytical execution. Its experiments evaluate analytical SQL; OLTP integration is discussed as a possible future layer. The architecture motivates reducing copies and combining workloads, but the evidence does not establish that all transactional and analytical demands have converged into one operating envelope. [6]

### 7.3 HyPer and TiDB: mixed work with distinct freshness paths

The original HyPer prototype uses virtual-memory snapshots to run analysis against a consistent view while transactional execution advances. TiDB's 2020 architecture uses transactional and analytical representations with replication and version coordination. Both combine work commonly separated into OLTP and OLAP categories; their implementation and visibility paths differ. [27], [28]

Three measurements must therefore remain separate: transactional service behavior under analytical load, analytical service behavior under writes, and the age of the version observed by analysis. A frozen snapshot answers a coherence question. Propagation delay answers a progress question. Resource interference answers an execution-cost question.

For operation family k, one possible interference measure is

$$
\iota_k=\frac{L_k^{\mathrm{mixed}}}{L_k^{\mathrm{isolated}}},
$$

with latency statistic, offered load, logical data, allocation policy, and resource accounting declared. This is meaningful only alongside throughput and freshness: holding latency constant by throttling writes or serving older data changes another outcome. “HTAP” identifies the desired workload combination, not a uniform guarantee or zero-interference architecture.

### 7.4 Datalog, Differential Dataflow, and DiskANN: language and mechanism

Datalog contributes rule and inference semantics. Materializing derived relations, incrementally maintaining them, and executing recursive dependencies are implementation choices. Differential Dataflow supplies mechanisms for incremental computation over changing collections with partially ordered logical timestamps; its component boundary does not by itself define an entire durable database service. [12], [19]

DiskANN makes the converse separation visible. It uses a graph as a physical search structure for approximate nearest-neighbor retrieval. The user-visible operation concerns vector similarity; the physical graph does not turn that contract into a general graph-data interface. [20]

These examples prevent “graph,” “recursive,” and “vector” from becoming interchangeable labels. The profile identifies whether the word refers to modeled information, an operation, an access structure, or a workload.

### 7.5 PostgreSQL and SurrealDB: model breadth and isolation

PostgreSQL's version 18 documentation describes different execution guarantees over multiversion machinery. The selected SurrealDB transaction documentation describes snapshot isolation and backend-dependent durability choices. These documented obligations must be recorded separately from relational or multimodel positioning. [32], [33]

The comparison is not which model label implies stronger consistency. It is which invariants the intended transactions require, which histories each selected configuration permits, and which failures a commit survives. The write-skew example in §3.3 remains applicable even when an interface supports several logical representations.

## 8. Evaluation protocol and validation of the framework

### 8.1 Constructing a system profile

An evaluation proceeds through six steps:

1. **State the decision.** Specify required observations and effects, failure tolerance, workload policy, and acceptable outcomes.
2. **Fix B and A.** Pin editions, configurations, access paths, observers, operation scopes, client obligations, and environmental conditions.
3. **Expand claims into C and P.** Separate public behavior from its proposed implementation. Identify which component owns each obligation.
4. **Check composition and feasibility.** Trace the complete operation path, including retries, connectors, publication, recovery, and external effects. Record unresolved obligations.
5. **Measure E under W.** Include representative phases, contention, maintenance, and failure scenarios; report the relevant distributions and resource costs.
6. **Compare candidates under the same requirements.** Explain differences through mechanisms and workload interactions. State when a comparison changes the contract or accounting boundary.

Every consequential claim should carry a record of the form

$$
q=(\text{statement},B,A,\text{owner/dependencies},\text{evidence},\text{status}).
$$

Evidence types are distinct: a theorem establishes a result in a model; documentation states an obligation; an architecture paper describes an implementation; an experiment reports behavior under conditions; a test may expose a counterexample. An inference made in this framework remains an inference.

Claim status should distinguish *specified*, *observed*, *refuted*, *unknown*, *unsupported*, and *delegated*, allowing combinations where appropriate. Documentation and observation can disagree. A delegated property can be satisfied by the complete service while remaining absent from the component. Failure to find an anomaly does not prove its absence: Elle's conclusions depend on the observations that can be reconstructed and the anomalies its model can detect. [11]

### 8.2 Separation tests

Useful boundaries should survive counterfactual changes. The following tests make the proposed decomposition falsifiable in practice.

| Test | Substitution or challenge | Expected explanatory result |
|---|---|---|
| Mechanism substitution | Replace an access path while preserving semantics | P and E can change while C remains satisfied |
| Contract substitution | Change isolation or approximate-result obligations | The admissible executions and feasible designs change, even with similar mechanisms |
| Workload substitution | Hold the contract and implementation fixed; change skew, contention, or query shapes | E can change materially without a new database category |
| Boundary substitution | Read through the native engine versus a connector or direct files | Contract scope can change within the same deployment |
| Composition failure | Combine individually valid components with mismatched publication or interpretation rules | The missing end-to-end obligation must be identifiable |
| Terminology expansion | Replace a category with explicit claims | Ambiguous claims become qualified fields; bundles can span fields |

The examples in §7 supply instances of these tests. They establish explanatory coverage of selected cases, not statistical validation of the method.

### 8.3 What “exhaustive” can reasonably mean

At the top level, an evaluation claim describes required behavior, realization, demand, context, or resulting evidence. This provides a broad organizing scheme, but breadth can become vacuous if every difficult case is placed into an unspecified “guarantees” field.

The stronger practical test is whether evaluators can encode a new claim precisely, preserve its assumptions, assign its owner, and identify how it could be checked. A future validation should use held-out systems and independently coded claims, measuring ambiguous assignments, missing fields, and disagreements about scope. New fields are justified when existing ones repeatedly conceal materially different obligations.

Mutual exclusivity is achievable for the questions a claim answers after decomposition. It is neither desirable nor generally possible for the original vocabulary: “transactional,” “streaming,” and “lakehouse” each bundle multiple claims. Nor are the coordinates causally independent. Their dependencies are a central object of analysis.

## 9. Relationship to prior work

Codd supplies the foundational separation between logical description and access machinery, and an account of data models broad enough to include operations and integrity. Concurrency theory supplies precise history properties. Temporal and provenance research supplies semantic distinctions that product labels often omit. These are established foundations of the proposed contract fields. [1], [2], [7], [8], [12], [13]

The Data Calculator is a particularly close precedent for first-principles decomposition of physical designs. It assembles data-structure primitives and estimates costs through analytical structure and learned models. Its evaluated scope is substantially narrower than all data-system behavior; it does not establish a universal minimal basis. The RUM conjecture provides a related lens on access-method costs. [15], [16]

The Composable Data Management System Manifesto treats reusable components and their boundaries directly, including semantic incompatibilities that can persist across shared interfaces. Database architecture surveys and historical analyses explain how familiar packages combine these components. [3], [4], [5], [17]

The contribution here is the integration of these perspectives into a scoped evaluation record: connect semantic obligations, physical responsibilities, workload policies, composition dependencies, and evidence. The framework does not replace isolation theory, a query-language semantics, or a detailed cost model. It identifies where each is required and prevents a result from one domain from being silently used as evidence in another.

## 10. Limits and implications

This paper is an analytical synthesis of selected literature and documentation. It reports no new benchmark experiment, implementation-conformance study, or proof of a unique, minimal, or universally exhaustive coordinate system. Historical system papers describe their published designs; they are not automatic evidence for current releases. Detailed formal treatment of security composition, Byzantine behavior, and the full range of language and temporal semantics requires further sources and models.

The practical output is nevertheless concrete. A system comparison should identify the required observations, the implementation responsibilities that preserve them, and the workloads under which the resulting service meets its targets. Category names remain useful as shorthand after those facts are available. Decoupling can improve several outcomes by changing the available designs; coordination requirements remain conditional on the semantics and assumptions of the work.

The framework also provides a disciplined way to investigate agents as knowledge workers. Their operation policies may adapt to previous results, revise intermediate conclusions, retain evidence, and coordinate concurrent changes. Those behaviors can be represented as changes to W and C before proposing new infrastructure. The subsequent research question is which additional contracts and operating envelopes these workloads require from shared knowledge systems.

## Appendix A. Terminology expansion

The table maps terms to the questions they can answer. Entries are prompts for claim extraction, not universal definitions or verified capabilities of every product using a label. A term spanning multiple fields must be decomposed before evaluation.

| Term | Fields | Required expansion |
|---|---|---|
| Relational | S, O, G | Relations, domains, operators, integrity; actual set/bag and null semantics |
| Document | S, O | Nesting, identity, field semantics, mutation and query operations; document atomicity stated separately |
| Property graph | S, O | Node/edge identity, properties, multiplicity, traversal and path semantics |
| RDF / knowledge graph | S, O, G | Asserted facts, identity, entailment regime if any, constraints and provenance; the label alone leaves these open |
| Key-value | S, O | Key identity/order, value interpretation, lookup and range operations; transaction scope separate |
| Wide-column / column-family | S, O, P | Row keys, families or qualifiers, sparsity, clustering; distinguish these from columnar physical layout |
| Multimodel | S, O, I | Models and operations exposed; mappings between them; identities and transactions shared across models |
| SQL | I, O | Dialect, types, function behavior, query and mutation semantics; isolation is a separate selection |
| Datalog | O, I | Language fragment, recursion, negation and inference semantics; evaluation and persistence mechanisms separate |
| Vector database | S, O, P, E | Vector representation, metric, filters, retrieval semantics, index maintenance, quality and cost |
| Time-series | S, O, W | Timestamp meaning, series identity, windows, retention, late data and corrections, arrival distribution |
| OLTP / OLAP | W, E | Transaction/query footprints, concurrency, latency objectives, data scale, throughput and cost |
| HTAP | W, P, G, E | Mixed work, representations, snapshot selection, freshness, isolation and interference |
| Streaming / batch | O, P, W | Input boundedness, arrival behavior, scheduling, windows, emission, refinement and completion |
| Event sourcing / event store | S, O, G, W | Event identity and order, append and correction rules, replay semantics, projections and retention |
| Temporal / bitemporal | S, O, G | Time dimensions, interval semantics, historical queries, correction and retention behavior |
| Versioned / time travel | S, O, G, P | Version identity, selection, schema interpretation, retention, physical reuse and deletion |
| Immutable | S, O or P | Specify whether immutability applies to logical facts, operation history, versions, or physical files |
| Row / column / hybrid | P | Physical grouping, encoding, and conversion; multiple representations may coexist |
| B-tree / LSM / inverted index | P, E | Access and maintenance algorithms, amplification, locality and supported workload region |
| MVCC / OCC / locking | P | Version selection or conflict-management machinery; G records the histories actually permitted |
| WAL / checkpoint | P, G | Persisted state and replay mechanism; acknowledgement, recovery, and failure guarantees |
| Consensus / quorum | P, A, G | Participants, membership, ordering scope, failure assumptions, acknowledgement and read protocol |
| Distributed / sharded / replicated | P, A | Placement, partitioning, replication, shared authorities and failure domains |
| In-memory | P, A | Residency, spill, logging, replication and restart path; durability stated independently |
| Embedded | B, I, P | Process boundary, invocation, sharing and failure behavior; analytical execution can coexist [23] |
| Managed / serverless | B, P, E; responsibility | Provisioning, scaling, cold starts, quotas, billing and operator duties |
| Open table format | I, S, O, G | Representation/protocol version and reader, writer, validation and publication obligations |
| Lakehouse | C, P, W | Particular composition of table management, storage access, execution, and governance |
| Zero-ETL | P, O, G, E | Transfers or transformations removed, remaining replication/materialization, freshness and failure handling |
| Exactly once | O, G, A | Effect being counted, duplicate identity, atomic boundary, retry protocol, deduplication retention and external participants |
| ACID | S, O, G, A | Atomic unit, invariants, actual isolation, acknowledgement and supported durability failures |
| Strong consistency | G, A | Replace with a named, scoped history or visibility property and its assumptions |
| Real time | O, G, W, E | Reference event, computation/visibility deadline, supported load and measured latency or freshness |
| Secure / governed | S, O, G, I, A | Actors, permitted observations/effects, trust and enforcement boundaries, revocation and audit obligations |

This expansion exposes an important asymmetry: a physical feature may be unnecessary for logical expressibility but essential for the required operating envelope. Encoding a graph in a key-value store can preserve information without preserving the cost of graph operations.

## Appendix B. Guarantee and temporal specification checklist

### B.1 A guarantee record

For each guarantee, supply the following particulars rather than a boolean feature flag:

| Particular | Example of the distinction it resolves |
|---|---|
| Sequential semantics | Latest-value read versus explicitly historical read |
| Atomic unit | Record, batch, transaction, table, or cross-service workflow |
| Observation scope | Committed results, intermediate reads, aborted effects, or pending outcomes |
| Ordering | Per-object, session, causal, serial, or real-time precedence |
| Enforcement point | Per operation, at commit, asynchronously, or during repair |
| Failure model | Process loss, machine loss, storage loss, partition, or regional failure |
| Client obligations | Retry identity, conflict handling, session context, permitted writers |
| Progress condition | Completion or success under defined reachability, contention, fairness, and load |
| Retention | How long versions, tombstones, deduplication records, or recovery evidence remain available |
| Evidence | Specification, implementation account, proof, test observation, or measurement |

Snapshot isolation requires snapshot and conflict rules; it is not synonymous with serializability. Strong eventual consistency requires the specified convergence and delivery conditions; it does not supply arbitrary integrity constraints. An exactly-once claim must identify an effect and its participating systems. Atomic publication alone does not establish exactly-once effects after a lost acknowledgement. The retry or recovery protocol must define how replay identifies prior effects and how unresolved outcomes are exposed. [8], [14], [30]

### B.2 Time dimensions

| Dimension | Meaning | Separate question |
|---|---|---|
| Valid time | When a fact is true in the modeled domain | Whether the system has recorded it yet |
| Transaction time | The fact's recorded history in the database | Its truth in the modeled domain |
| Event time | Timestamp assigned to an event | When the system receives or processes it |
| Processing time | Time at which processing occurs | Whether all relevant events have arrived |
| Logical time | Version, epoch, or iteration position, possibly partially ordered | Wall-clock recency |
| Real-time precedence | One operation finishes before another begins | Domain time carried in their data |

Freshness also needs a reference and scope. Snapshot age relative to database commit time, ingest delay relative to source events, and completeness relative to an external world are different measures. A coherent snapshot may be old; a newly processed event may describe an old fact. Watermarks and finality require their own assumptions about late or missing input. [7], [13], [18], [19]

## Appendix C. Reusable evaluation record

The following is a schema sketch, not a mandated serialization format. It separates requirements from supplied behavior and evidence so that a documented feature is not silently treated as verified conformance.

```yaml
subject:
  component_or_service:
  edition_and_configuration:
  deployment_and_resources:
  observers_and_access_paths:
  operation_and_failure_scopes:

requirements:
  state_and_identity:
  operations_and_results:
  guarantees:
  interfaces:
  operating_targets:

claimed_contract:
  state:
  operations:
  guarantees:
  interfaces:

implementation:
  representation_and_access:
  planning_and_execution:
  coordination_and_publication:
  durability_and_recovery:
  resources_and_lifecycle:

work:
  initial_data_distribution:
  request_and_arrival_policy:
  dependencies_contention_and_retries:
  lifecycle_demands_and_phases:

assumptions:
  faults_networks_clocks_and_trust:
  client_and_operator_obligations:

composition:
  components_and_interfaces:
  claim_owners_and_dependencies:
  publication_and_recovery_authorities:

evidence:
  claims: []  # statement, scope, source/type, status, limitations
  experiments: []  # workload, conditions, method, observations
  unresolved_obligations: []

evaluation:
  supported_or_observed_operating_region:
  feasibility_findings:
  comparative_outcomes:
  sensitivity_and_failure_results:

decision_context:
  operational_responsibility:
  licensing_pricing_and_migration_constraints:
```

## References and source scope

The selected corpus comprises 31 paper/chapter works, represented by 32 PDFs because two editions of the CRDT paper were read, plus selected official documentation. Full-text reading records and edition-specific qualifications were kept with the research notes that produced this guide and are not part of this repository. The reading did not reproduce experiments or mechanically verify proofs. Documentation below was read on 10 September 2026; claims are limited to the selected pages and versions.

1. E. F. Codd. *A Relational Model of Data for Large Shared Data Banks*. CACM, 1970. [Full text][1].
2. E. F. Codd. *Data Models in Database Management*. 1980 publication. [Full text][2].
3. J. M. Hellerstein, M. Stonebraker, and J. Hamilton. *Architecture of a Database System*. Foundations and Trends in Databases, 2007. [Full text][3].
4. M. Stonebraker and J. M. Hellerstein. *What Goes Around Comes Around*. Readings in Database Systems, 2005. [Full text][4].
5. M. Stonebraker and A. Pavlo. *What Goes Around Comes Around... And Around...*. SIGMOD Record, 2024. [Full text][5].
6. M. Armbrust et al. *Lakehouse: A New Generation of Open Platforms that Unify Data Warehousing and Advanced Analytics*. CIDR, 2021. [Full text][6].
7. M. P. Herlihy and J. M. Wing. *Linearizability: A Correctness Condition for Concurrent Objects*. TOPLAS, 1990. [Full text][7].
8. A. Adya, B. Liskov, and P. O'Neil. *Generalized Isolation Level Definitions*. ICDE, 2000. [Full text][8].
9. P. Bailis et al. *Coordination Avoidance in Database Systems*. Extended version, arXiv:1402.2237v4, 2014. [Full text][9].
10. J. M. Hellerstein and P. Alvaro. *Keeping CALM: When Distributed Consistency Is Easy*. arXiv:1901.01930v2, 2019 preprint. [Full text][10].
11. K. Kingsbury and P. Alvaro. *Elle: Inferring Isolation Anomalies from Experimental Observations*. arXiv:2003.10554v1, 2020. [Full text][11].
12. T. J. Green, G. Karvounarakis, and V. Tannen. *Provenance Semirings*. PODS, 2007. [Full text][12].
13. C. S. Jensen et al. *A Consensus Glossary of Temporal Database Concepts*. SIGMOD Record, 1994. [Full text][13].
14. M. Shapiro et al. *Conflict-free Replicated Data Types*. INRIA RR-7687, version 2, and SSS, 2011. [Research report][14]; [conference edition](https://perso.lip6.fr/Marc.Shapiro/papers/2011/CRDTs_SSS-2011.pdf).
15. S. Idreos et al. *The Data Calculator: Data Structure Design and Cost Synthesis from First Principles and Learned Cost Models*. SIGMOD, 2018. [Full text][15].
16. M. Athanassoulis et al. *Designing Access Methods: The RUM Conjecture*. EDBT, 2016. [Full text][16].
17. P. Pedreira et al. *The Composable Data Management System Manifesto*. PVLDB, 2023. [Full text][17].
18. T. Akidau et al. *The Dataflow Model: A Practical Approach to Balancing Correctness, Latency, and Cost in Massive-Scale, Unbounded, Out-of-Order Data Processing*. PVLDB, 2015. [Full text][18].
19. F. McSherry et al. *Differential Dataflow*. CIDR, 2013. [Full text][19].
20. S. J. Subramanya et al. *DiskANN: Fast Accurate Billion-point Nearest Neighbor Search on a Single Node*. NeurIPS, 2019. [Full text][20].
21. B. F. Cooper et al. *Benchmarking Cloud Serving Systems with YCSB*. SoCC, 2010. [Full text][21].
22. G. Szárnyas et al. *The LDBC Social Network Benchmark: Business Intelligence Workload*. PVLDB 16(4), 2022. [Full text][22].
23. M. Raasveldt and H. Mühleisen. *DuckDB: An Embeddable Analytical Database*. SIGMOD demonstration, 2019. [Full text][23].
24. J. Zhou et al. *FoundationDB: A Distributed Unbundled Transactional Key Value Store*. SIGMOD, 2021. [Full text][24].
25. A. Verbitski et al. *Amazon Aurora: Design Considerations for High Throughput Cloud-Native Relational Databases*. SIGMOD, 2017. [Full text][25].
26. B. Dageville et al. *The Snowflake Elastic Data Warehouse*. SIGMOD, 2016. [Full text][26].
27. A. Kemper and T. Neumann. *HyPer: A Hybrid OLTP&OLAP Main Memory Database System Based on Virtual Memory Snapshots*. ICDE, 2011. [Full text][27].
28. D. Huang et al. *TiDB: A Raft-Based HTAP Database*. PVLDB, 2020. [Full text][28].
29. J. C. Corbett et al. *Spanner: Google's Globally-Distributed Database*. OSDI, 2012. [Full text][29].
30. M. Armbrust et al. *Delta Lake: High-Performance ACID Table Storage over Cloud Object Stores*. PVLDB, 2020. [Full text][30].
31. G. DeCandia et al. *Dynamo: Amazon's Highly Available Key-value Store*. SOSP, 2007. [Full text][31].
32. PostgreSQL. Version 18 documentation, *Transaction Isolation* and *Introduction* to concurrency control. [Isolation][32]; [MVCC introduction](https://www.postgresql.org/docs/18/mvcc-intro.html).
33. SurrealDB. *Transactions and Isolation*. Selected official documentation. [Documentation][33].
34. Apache Iceberg. *Table Specification*. Retrieved specification marks format versions 1–3 adopted and version 4 draft. [Specification][34].

[1]: https://web.eecs.umich.edu/~michjc/eecs584/Papers/codd_1970.pdf
[2]: https://courses.washington.edu/geog482/resource/Codd_1981_Data_Model.pdf
[3]: https://db.cs.berkeley.edu/papers/fntdb07-architecture.pdf
[4]: https://people.csail.mit.edu/tdanford/6830papers/stonebraker-what-goes-around.pdf
[5]: https://db.cs.cmu.edu/papers/2024/whatgoesaround-sigmodrec2024.pdf
[6]: https://www.cidrdb.org/cidr2021/papers/cidr2021_paper17.pdf
[7]: https://cs.brown.edu/people/mph/HerlihyW90/p463-herlihy.pdf
[8]: https://www.cs.cmu.edu/~15721-f24/papers/Generalized_Isolation_Levels_Definitions.pdf
[9]: https://arxiv.org/pdf/1402.2237v4
[10]: https://arxiv.org/pdf/1901.01930v2
[11]: https://arxiv.org/pdf/2003.10554v1
[12]: https://web.cs.ucdavis.edu/~green/papers/pods07.pdf
[13]: https://sigmodrecord.org/publications/sigmodRecord/9403/pdfs/181550.181560.pdf
[14]: https://decomposition.al/CSE290S-2023-01/readings/crdts.pdf
[15]: https://faculty.cc.gatech.edu/~jarulraj/courses/8803-f18/papers/data_calculator.pdf
[16]: http://openproceedings.org/2016/conf/edbt/paper-12.pdf
[17]: https://www.vldb.org/pvldb/vol16/p2679-pedreira.pdf
[18]: https://research.google.com/pubs/archive/43864.pdf
[19]: https://www.cidrdb.org/cidr2013/Papers/CIDR13_Paper111.pdf
[20]: https://proceedings.neurips.cc/paper_files/paper/2019/file/09853c7fb1d3f8ee67a61b6bf4a7f8e6-Paper.pdf
[21]: https://www.cs.albany.edu/~jhh/courses/readings/cooper.socc10.benchmarking.pdf
[22]: https://ldbcouncil.org/docs/papers/ldbc-snb-bi-vldb-2023.pdf
[23]: https://duckdb.org/pdf/SIGMOD2019-demo-duckdb.pdf
[24]: https://www.foundationdb.org/files/fdb-paper.pdf
[25]: https://cdn.amazon.science/dc/2b/4ef2b89649f9a393d37d3e042f4e/amazon-aurora-design-considerations-for-high-throughput-cloud-native-relational-databases.pdf
[26]: https://www.cs.cmu.edu/~15721-f24/papers/Snowflake.pdf
[27]: https://cs.brown.edu/courses/cs227/archives/2012/papers/olap/hyper.pdf
[28]: https://www.vldb.org/pvldb/vol13/p3072-huang.pdf
[29]: https://research.google.com/archive/spanner-osdi2012.pdf
[30]: https://web.stanford.edu/class/cs245/readings/delta-lake.pdf
[31]: https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf
[32]: https://www.postgresql.org/docs/18/transaction-iso.html
[33]: https://surrealdb.com/docs/transactions-and-isolation
[34]: https://iceberg.apache.org/spec/
