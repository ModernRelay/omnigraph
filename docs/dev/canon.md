# The OmniGraph Canon

**Audience:** maintainers, contributors, and coding agents — internal
**Type:** narrative reference ("the book"), read top-to-bottom
**Status:** living document
**Surveyed:** OmniGraph 0.8.1 development (`main`), with the v6/0.10.x format work present; Lance 9.0.0-beta.21 (git rev `1aec1465`); internal manifest schema v6

---

## What this document is (and is not)

This is the linear narrative of OmniGraph: why it exists, what it is built on,
how a read, a write, a merge, and a crash actually unfold, what we deliberately
refuse to build, where the risks are, and where the design is going. Read it
start to finish to build the complete mental model; use it to onboard, to
re-anchor after time away, or to check a design instinct against the system's
reasoning.

It is **not** the mechanical authority for any subsystem. Every section links to
the per-area doc that owns the details ([invariants.md](invariants.md),
[writes.md](writes.md), [execution.md](execution.md), …); when this document and
an area doc disagree, the area doc wins and this one gets fixed. It is also not
user documentation — that lives under [docs/user/](../user/index.md).

Truth discipline: claims here are current shipped behavior unless explicitly
marked **(roadmap)**, **(draft RFC)**, or **(research-blocked)**. There is no
single "status disclaimer" that licenses aspirational prose elsewhere — each
claim carries its own status, per the maintenance contract's "don't lie" rule.

---

## TL;DR

**The problem.** Fleets of agents (and the humans supervising them) need shared
operational state that is simultaneously: a typed graph (entities and
relationships with schema), multi-modal retrievable (vector ANN + full-text +
graph traversal fused in one query), *reviewable* (isolated proposals, diffs,
merges — Git semantics over data, not files), time-travelable, and deployable on
plain object storage without a database server fleet. Existing systems give you
one or two of these. Gluing a graph database, a vector database, a search
engine, and an audit trail together means N consistency boundaries, N failure
modes, and no coherent notion of "the state of the graph at commit X."

**The solution.** OmniGraph is a typed property-graph engine built as a
*coordination layer* over many [Lance](https://lance.org) datasets — one
columnar, versioned, branchable dataset per node/edge type — with one manifest
table (`__manifest`) making multi-dataset changes visible atomically. On top of
that substrate it adds:

1. **Git-style branches and commits across the whole graph.** Every publish
   appends to a commit DAG; branches fork lazily (copy-on-write via Lance);
   merges are three-way at the row level with typed conflicts. Branches *are*
   the multi-query transaction model — there is deliberately no cross-query
   `BEGIN`/`COMMIT`.
2. **One unified write protocol (RFC-022, implemented).** Every graph-visible
   write — mutation, bulk load, schema apply, branch merge, index build —
   follows one state machine: prepare against a pinned authority token, arm a
   durable recovery intent, apply exact physical effects, publish exactly one
   manifest CAS. Within the documented single-writer-process recovery boundary,
   a crash leaves enough durable evidence for a later quiesced read-write open
   to converge the fixed outcome all-or-nothing.
3. **Multi-modal querying in one runtime.** A `.gq` query can combine vector
   `nearest`, BM25/FTS, Reciprocal Rank Fusion, property filters, and graph
   traversal (`Expand`, anti-join) against one snapshot.
4. **A declared control plane.** A `.pg` schema language with migration
   planning; a cluster directory (`cluster.yaml`) that declares graphs,
   policies, and stored queries as code; Cedar policy enforced engine-wide on
   every writer; an Axum HTTP server and a CLI over the same engine gate.

Storage is any S3-compatible object store or local filesystem. The whole system
is a single process per server; correctness never depends on a coordinator
service.

---

## Why OmniGraph — the gap it fills

The design center is **context assembly and coordination for agent fleets**:
hundreds of writers proposing changes concurrently, every change attributable
and reviewable, retrieval spanning similarity + text + structure. Measured
against the systems you would otherwise compose:

| You could use… | What's missing for this workload |
|---|---|
| A graph database (Neo4j-class, server-based) | No whole-graph branching/merge as a data-review workflow; storage not object-store-native; vector/FTS usually bolted on with separate consistency |
| An embedded analytics graph DB (Kùzu-class) | Same branching gap; single-process analytics focus rather than coordinated multi-writer state |
| A vector DB / LanceDB directly | Per-dataset versioning only — no *cross-dataset* atomic commit, no typed graph semantics, no traversal, no merge |
| Postgres + pgvector + AGE | Real transactions, but no git-style branch/merge of data, no object-store-native storage; multi-modal fusion is manual |
| Git/dolt-style versioned tables | Versioning without the retrieval runtime (ANN/FTS/traversal) or graph typing |

(Positioning summary for internal orientation — verify specific competitor
claims before using them externally; they age.)

The honest inverse — what those systems have that OmniGraph deliberately does
not — is catalogued in [What we deliberately exclude](#what-we-deliberately-exclude-and-why)
and [Known gaps](invariants.md#known-gaps). Chief among them: no cross-query
transactions (branches instead), single-writer-process support boundary for
destructive recovery (a distributed fence is roadmap), and a pinned pre-stable
Lance version.

---

## Design philosophy

These are the reasoning tools behind every section that follows. The normative
statements live in [invariants.md](invariants.md); this is the narrative form.

### 1. Engineering is programming integrated over time

The operative question for any change is *which option has the lower ongoing
liability* — not shorter now, not fastest to ship. Complexity must be earned by
demonstrated correctness, performance, or future-shape cost, never by
speculation. This cuts both ways: sometimes the lower-liability option is more
code (a centralized dispatcher instead of scattered hooks), sometimes less (no
migration framework until a concrete graph demands one — see the
[strand model](#schema-and-migration--the-strand-model)). Ask: *what does this
look like after five more changes like it?*

### 2. Respect the substrate

Lance owns columnar storage, per-dataset versioning, fragments, branches,
compaction, cleanup, and index primitives. DataFusion owns relational execution
where it fits. We do not build WALs, transaction managers, buffer pools, or
local clones of substrate behavior — and we *use* the substrate idiomatically:
long-lived handles, shared sessions, cheap freshness probes instead of
re-scans. Re-deriving per call what the substrate keeps warm is a substrate
violation even when no code is reimplemented.

### 3. Logical contract over physical state

Logical state is the contract; physical state — index coverage, fragment
layout, staged writes — is derived, rebuildable, possibly asynchronous. **A
physical operation must never fail a logical one.** Preconditions are checked
against logical state; physical reconciliation is idempotent and may lag. The
smell to watch for: a logical operation whose precondition is a physical fact
(an index's existence, a fragment count). Genuine logical conflicts still fail
loudly — the licence to lag covers convergence, not correctness.

### 4. One source of truth, cheaply derived

Lance and the manifest are the source of truth; everything else is a derived
view — held warm, refreshed by a cheap probe. Two failure modes are forbidden:
a *parallel copy* that can drift (divergence compounds), and *cold
re-derivation* on every call (cost grows with history instead of the working
set). The commit graph, the compiled catalog, the CSR topology index, and the
handle cache are all derived views engineered under this rule.

### 5. Graph visibility is manifest-atomic

Lance commits are per dataset. Graph-level atomicity is manufactured: one
`__manifest` update flips every touched sub-table version visible together.
No write path may make a subset of touched tables visible as a graph commit,
and any writer that can advance a Lance HEAD before manifest publish must carry
a durable recovery intent covering the gap.

### 6. Branches are the transaction model

Per-query writes are atomic at the manifest boundary. Anything larger — a batch
of related changes, an agent's proposal, a risky migration — is a branch:
isolated, diffable, mergeable, discardable. This replaces cross-query
`BEGIN`/`COMMIT` deliberately (deny-listed), because branches give the same
isolation with review, attribution, and history for free, and they compose with
the object-store substrate where long-lived interactive transactions do not.

### 7. Strong consistency, loud failures

Reads are snapshot-isolated; writes are durable before acknowledgement; branch
reads observe current committed state. Integrity violations (type errors,
missing endpoints, cardinality, uniqueness) fail *before* publish. OOM,
timeout, partial results, recovery, and conflicts are surfaced and typed, never
swallowed. Any eventual-consistency mode must be explicit, read-only, and
non-default (none ships today).

### 8. Semantics are first-class structures

Search modes, mutations, polymorphism, traversal, scores, and policy predicates
belong in typed AST/IR/planner structures — never smuggled through magic
strings, side tables, globals, or transport flags. Transport and auth stay at
the boundary: kernel crates know nothing of HTTP or bearer tokens.

### 9. Correctness > simplicity > performance; reversibility shapes evidence

Lexicographic: give up performance for simpler code, simplicity for correct
code, correctness never. And the evidence demanded of a change scales with its
reversibility: reversible changes wait for production evidence; irreversible
ones (substrate choice, on-disk format, consistency guarantees) earn an RFC,
because by the time production proves them wrong you've shipped years of
dependent code.

### 10. Observable behavior is the contract (Hyrum's Law)

Output ordering, error text, timestamp precision, default flags, latency
profiles — once shipped, someone depends on them. We don't expose what we won't
commit to, and we treat behavior changes found in substrate upgrades (e.g. a
BM25 stop-word change reordering ties) as contract events to pin in tests, not
incidental noise.

---

## Architecture at a glance

One process, layered:

```
CLI (omnigraph)        HTTP Server (omnigraph-server: Axum + Cedar + admission)
        │                            │
        └─────────────┬──────────────┘
                      ▼
           omnigraph-compiler   Pest grammars (.pg / .gq), catalog, typecheck,
                      │         IR lowering, lint, migration planning — zero Lance dependency
                      ▼
           omnigraph (engine)   exec (query/mutation/loader), MutationStaging,
                      │         GraphCoordinator/ManifestCoordinator, CommitGraph projection,
                      │         GraphIndex (CSR/CSC), merge, recovery, validate
                      ▼
           storage boundary     sealed TableStorage (staged writes only) +
                      │         read-only snapshot facade
                      ▼
           Lance 9.x            columnar Arrow, per-dataset versions/branches,
                      │         BTREE/FTS/vector indexes, merge_insert, compaction
                      ▼
           object store         local FS · S3 · RustFS · MinIO · S3-compatible
```

Workspace crates: `omnigraph-compiler`, `omnigraph` (package name
`omnigraph-engine` — the directory and package names differ),
`omnigraph-policy`, `omnigraph-api-types` (shared wire DTOs), `omnigraph-cluster`
(control plane), `omnigraph-cli`, `omnigraph-server`. Full diagrams and code
paths: [architecture.md](architecture.md).

Two structural boundaries deserve emphasis because everything else leans on
them:

- **The compiler knows no Lance.** Schema and query semantics are decided in
  typed structures before the engine binds them to storage. This is what makes
  invariant 8 (semantics are first-class) enforceable rather than aspirational.
- **The write surface is closed by Rust visibility.** Raw storage,
  handle-cache, and coordinator modules are crate-private; public snapshot
  access is a read-only facade that does not expose Lance's raw scanner. A
  defense-in-depth source guard (`tests/forbidden_apis.rs`) classifies every
  public async engine method and exact-counts registered durable-call shapes,
  so adding a writer is an explicit registry change, never an accidental call
  site.

---

## The substrate contract: Lance

OmniGraph's relationship with Lance is a *contract*, managed like one:

- **Pinned, audited versions.** The engine pins one Lance version
  (currently 9.0.0-beta.21 via git rev, until 9.0.0 stable reaches crates.io).
  Every bump gets a full alignment audit — all intervening upstream commits
  reviewed, findings recorded in [lance.md](lance.md)'s dated audit stanzas.
  History has justified the paranoia: audits have caught a default flip that
  would have GC'd manifest-pinned versions (`auto_cleanup`), a row-id overlap
  that corrupted filtered reads (lance#7444, temporarily fixed by a vendored
  one-hunk pin), and behavioral changes in merge_insert, BTREE range bounds,
  and BM25 scoring.
- **Surface guards as tripwires.** `tests/lance_surface_guards.rs` pins every
  Lance API shape and behavior we depend on — compile-shape guards, runtime
  behavior guards, and "this upstream bug is fixed" guards that turn red when
  reality changes. A Lance bump runs this file first; a clean build is *not* a
  clean alignment.
- **The L1/L2 split as a review tool.** Every capability is classified as L1
  (inherited from Lance: columnar storage, per-dataset versioning/branches,
  index primitives, merge_insert, compaction) or L2 (added by OmniGraph:
  typing, graph semantics, cross-dataset atomicity, graph-level branches and
  lineage, the query runtime, policy, serving). The full matrix lives in
  [AGENTS.md](../../AGENTS.md). When a proposal reimplements something in the
  L1 column, it's deny-listed on sight.

What Lance does **not** give us — and where OmniGraph's hardest engineering
lives — is anything *cross-dataset*: no multi-dataset atomic commit, no
conditional branch-ref create/delete, no caller-controlled transaction identity
for maintenance operations. The next three sections are the story of
manufacturing those guarantees on top.

---

## Anatomy of a graph on disk

A graph is one directory (or S3 prefix). Details: [storage.md](../user/concepts/storage.md).

```
graph-root/
  __manifest/                      # the coordination table (a Lance dataset itself)
  nodes/{stable-id}-{incarnation}/ # one dataset per node-table lifetime
  edges/{stable-id}-{incarnation}/ # one dataset per edge-table lifetime
  _graph_commit_recoveries.lance/  # internal crash-recovery audit log
  __recovery/{ulid}.json           # transient recovery sidecars (empty at steady state)
  _refs/branches/{name}.json       # graph-level branch metadata
```

`__manifest` is the load-bearing object. Its rows describe, per branch, which
version of each identity-paired sub-table is published (`table_version` rows,
minus tombstones scoped to the same stable ID + incarnation), **and** — since
internal schema v4 (RFC-013 Phase 7) — the graph
commit lineage itself: one immutable `graph_commit` row per commit (ULID id,
parents, merge parents, actor, timestamp) plus one mutable `graph_head:<branch>`
pointer per branch. Lineage rows are written *in the same merge-insert commit*
as the table-version rows, so a graph commit and its lineage land at one
manifest version atomically — there is no second write to fail between. The
in-memory `CommitGraph` is a pure projection of these rows (invariant 15 in
action; the former `_graph_commits.lance` tables are retired).

Two mechanisms make concurrent publishes safe:

- `__manifest.object_id` carries Lance's unenforced-primary-key annotation, so
  the substrate's bloom-filter conflict resolver rejects two concurrent commits
  landing the same row — **row-level CAS**. Without it, Lance's transparent
  rebase would admit silent duplicates from racing publishers.
- Same-branch writers all touch the shared `graph_head:<branch>` row, so even
  commits to *disjoint tables* contend there: one wins and the other's exact
  publish precondition fails. Only a writer whose semantics permit it may then
  discard the whole effect-free attempt and reprepare from fresh authority; the
  publisher never re-parents a prepared intent. This closes the
  disjoint-table-fork race and yields a linear per-branch chain (pinned by the
  N-writer convergence tests).

The internal manifest schema is stamped
(`omnigraph:internal_schema_version`, currently v6) and **strict
single-version** — see [the strand model](#schema-and-migration--the-strand-model).

---

## The life of a read

Contract: **a query holds one snapshot for its lifetime** (invariant 3). It
never re-reads the branch head mid-query, so concurrent writes cannot leak in.

The path (details: [execution.md](execution.md)):

1. **Capture.** Resolve the target (branch or historical version) to a manifest
   snapshot plus the compiled catalog. Capture happens under a short
   process-local schema gate so snapshot and catalog are *coherent* — a
   concurrently applying schema can't be observed half-published. Execution
   then releases the gate and runs entirely on the captured pair.
2. **Compile.** Parse + typecheck the `.gq` against the catalog, lower to a
   typed IR pipeline (`NodeScan`, `Filter`, `Expand`, `AntiJoin`, projections,
   ordering).
3. **Topology, if needed.** If the pipeline traverses, build or fetch a CSR/CSC
   `GraphIndex` **scoped to exactly the edge types the query touches** —
   never the whole catalog. The cache key is each edge table's physical
   identity `(stable table ID, incarnation ID, table_key, version, branch,
   e_tag)`, so a lazy-fork branch whose
   edge tables physically *are* main's reuses main's built index instead of
   cold-scanning.
4. **Execute.** Scans push structured filters down to Lance (BTREE/FTS/vector
   indexes accelerate what they cover; correctness never depends on coverage —
   invariant 7). Multi-modal ops (`nearest`, `bm25`, `rrf`) run in the same
   pipeline; RRF fans out sub-rankings and fuses by rank.

**The cost model is a tested contract, not an aspiration.** The warm read path
was once O(commit-history) per query — fresh coordinator per read, full
manifest re-scans, and independently-created object-store clients. Lance
graph-dataset access is now split deliberately: a process-wide
`ObjectStoreRegistry` pools graph-dataset clients, graph data uses a
graph-handle-scoped cached `Session`, and mutable-tip control state uses a
zero-cache `Session`. A coordinator open or full refresh derives its manifest
state and lineage projection together from one row scan. The warm query path
remains one cheap freshness probe, one schema read, and zero dataset opens on a
warm repeat. These are *pinned* by IO-counted cost-budget tests
(`warm_read_cost.rs`) that count object-store operations at commit-history
depth — because cost-scaling bugs pass every correctness test and only bite in
production. A required full journal fold may still grow with uncompacted
history; client/session reuse does not change that fact. See
[testing.md](testing.md) "Cost-budget tests".

---

## The life of a write

This is the heart of the system. Read [writes.md](writes.md) for the mechanics
and [RFC-022](../rfcs/0022-unified-write-path.md) for the full protocol; this
is the story.

### The problem being solved

Lance has no multi-dataset atomic commit. A graph write touching three tables
makes three independent Lance commits plus one manifest publish — four durable
operations, any prefix of which can survive a crash. Meanwhile other writers
race on the same branch, schema state can change under a prepared plan, and a
first write to a branch may need to *create* the per-table Lance fork it is
writing to. The unified protocol makes all of this safe with one state machine:

```
recovery barrier                    # never write over an unresolved crash
  → prepare pinned base + read set  # capture (branch identity, exact graph head,
  → stage effects (no HEAD moves)   #  schema identity, table pins); validate everything
  → acquire ordered gates           # schema → branch → sorted tables (process-local)
  → revalidate the complete token   # or restart / typed conflict — never rebase a stale plan
  → arm durable recovery intent     # __recovery/{ulid}.json, before any durable effect
  → apply exact physical effects    # commit_staged with pre-minted identities, zero retries
  → publish ONE __manifest CAS      # entire graph delta + lineage, exact precondition
  → finalize (delete sidecar)
```

### Mutations and loads, concretely

`mutate_as` and every `load` mode accumulate work in an in-memory
`MutationStaging` — inserts/updates as pending `RecordBatch`es, deletes as
predicates. **No Lance HEAD advances during statement execution.** Reads inside
the query union the committed snapshot with the pending batches (DataFusion
`MemTable`), so a multi-statement mutation gets read-your-writes: statement N+1
sees statement N's inserts when validating referential integrity or
cardinality.

The **D₂ rule** keeps this unambiguous: one mutation query is constructive
(insert/update) XOR destructive (delete), enforced at parse time. This is a
deliberate boundary, not scaffolding — allowing mixing would require an
in-query delete view, pending-batch pruning, and per-table two-commit ordering
in the hot path. Compose mixed work as two mutations, or a branch for one
atomic commit.

At end-of-query, `stage_all` prepares exactly one staged Lance transaction per
touched table (exact-`id` fenced strict insert or upsert / deletion-vector
delete / overwrite), still without moving HEAD. A keyed Mutation/Load table
above 8,192 rows or 32 MiB fails here with `ResourceLimitExceeded`, before
recovery arm. Bare Lance Append is not a production graph-write route. External
Blob URI cells on keyed Append/Merge are size-summed before payload reads and
materialized under the same 32 MiB ceiling because Lance merge-insert has no
`WriteParams` hook; Overwrite retains external references. Then the gates are
acquired, the full authority token revalidated, the identity-bearing v9
recovery envelope armed, tables committed with
their exact pre-minted transaction identities and **zero transparent conflict
retries**, and the pre-minted lineage published under the exact
native-branch/head + table-version precondition.

Pure keyed insertions also leave a durable, inductive proof link in Lance
history: `omnigraph.insert_absence = "v1"`. StrictInsert mints it only after
the exact target-ID preflight; an all-new Upsert may mint it only when Lance's
completed statistics prove one attempt inserted every row and changed/skipped
nothing. Upsert certification is optional—an unfamiliar shape disables the
optimization, not the logical write. The marker is accepted only with an exact
parent, filtered insertion-only `Update`, full nested schema field-ID preorder,
physical-row totals, and the rest of the structural proof; it is not a
cryptographic trust signal for raw Lance writers.

Failure semantics are typed by *when* the failure happens:

- **Before any effect** (validation failure, or authority changed):
  the attempt is discarded whole. Insert-only mutations and Append/Merge loads
  may reprepare from fresh authority with a bounded retry after unrelated
  authority movement; load Append remains strict insert. Strict
  Update/Delete/Overwrite (and branch merge) return `ReadSetChanged` (HTTP 409)
  — a stale plan is never rebased onto a moved base. A pre-existing or
  effect-free concurrent strict same-key match is terminal `KeyConflict`
  (HTTP 409) only after a fresh manifest-visible probe finds an attempted ID;
  a broad storage conflict without that exact match becomes an internal
  `ReadSetChanged`, causing bounded full strict-mode reprepare rather than a
  false duplicate. An effect-free upsert conflict likewise reprepares the
  entire operation and reruns validation. Those
  effect-free key outcomes are Mutation/Load `protocol_v3` behavior;
  BranchMerge retains its armed `protocol_v4` chain and returns
  `RecoveryRequired` for any chunk conflict, even before its first owned table
  effect.
- **After any effect**: any later error returns `RecoveryRequired` (HTTP 503)
  and leaves the sidecar authoritative. This is deliberately *not* a retry
  loop — the fixed outcome converges through recovery, preserving the
  interrupted writer's exact commit identity and actor.

A cancelled future leaves no graph-visible state: the accumulator evaporates,
and anything durable is sidecar-covered.

### Validation is unified and Δ-scoped

Value/enum constraints, uniqueness, edge referential integrity, and
cardinality route through **one** catalog-derived evaluator (`crate::validate`)
on all three write surfaces — mutation, load, and branch merge — so the
surfaces cannot drift. It checks the *delta* against a pinned committed view
(not the whole graph), uses a BTREE probe when the index is reconciled, and
stays correct by scanning while it is pending (invariant 7 again).

### Writer-specific adapters

"Unified" means one set of safety obligations, not one Lance primitive. Each
writer describes its physical effects to the shared coordinator:

| Writer | Sidecar schema | Physical shape |
|---|---|---|
| Mutation / Load | v9 (`protocol_v3` payload) | one exact staged transaction per touched table |
| Branch merge | v9 (`protocol_v4` payload) | new and changed keyed rows use actual chunks capped at 8,192 rows / 32 MiB in a pre-minted exact-`id` strict-insert/upsert chain, capped at 1,024 logical data transactions per table; deletes and pointer-only deltas are recorded too. Exact recovery scans at most 1,026 versions so one allowed index tail and one compensating restore remain classifiable |
| Schema apply | v9 (`protocol_v7` payload) | exact `Overwrite` per rewritten table + strict read-version-zero `Create` per new type; a pure rename retains its existing identity/path/version. The payload also carries the schema registration/rename/tombstone delta (a metadata-only apply has an empty effect set but still arms — schema staging is durable state) |
| EnsureIndices | v9 (`protocol_v8` payload) | one pre-minted *mixed* CreateIndex transaction per table (every missing BTREE + FTS + full-table vector together) |
| Optimize | v9 (bounded payload) | compaction + index folds have **no** public caller-controlled Lance transaction identity, so Optimize keeps looser, bounded provenance inside the identity-bearing envelope: one graph-wide sidecar pinning the complete productive set, one monotonic batch CAS for visibility. Exact provenance is trigger-gated on upstream API + distributed fencing |

First-touch tables (a branch's first write to a table) follow
**sidecar-before-ref** ordering: the recovery intent that names the
`(table_path, target ref)` is durable *before* the Lance ref exists, so an
orphaned fork is always attributable and reclaimable, and reclaim/cleanup treat
any pending claim as a hard stop.

### Gates, and what they are not

Every handle for one canonical graph root shares a process-local
`WriteQueueManager`: schema gate → branch gate → sorted table gates, one
deadlock-free order used by writers, healers, and the recovery sweep alike.
These gates prevent same-process races and reduce publisher retries. **They are
not distributed locks.** Cross-process safety comes from the publisher's exact
CAS precondition (one winner; the loser gets a typed conflict) — and the honest
support boundary that follows from it is described in
[Concurrency and the support boundary](#concurrency-and-the-support-boundary).

---

## The life of a crash

Recovery is part of the commit protocol, not an afterthought (invariant 5).
Full mechanics: [writes.md](writes.md) "Open-time recovery sweep".

The lifecycle every staged writer follows: **Phase A** write the sidecar
(before any independently durable effect) → **Phase B** apply per-table
effects, then atomically *confirm* the exact achieved versions/identities into
the sidecar → **Phase C** publish the manifest → **Phase D** delete the
sidecar. The Phase-B confirmation is the commit point of this recovery WAL:

- Crash **before confirmation** → roll back: restore each table to its
  manifest pin, then publish the restored state so `manifest == HEAD` converges
  with *no residual drift* (this symmetry is what lets a failed-then-retried
  operation succeed instead of failing one version higher each time).
- Crash **after confirmation** → roll forward, but only under the captured
  authority token, and only through *exact* proof: the observed Lance
  transaction UUIDs and versions must match the pre-minted plan. Roll-forward
  publishes the interrupted writer's fixed lineage — original commit ID,
  original actor — so history is what the writer intended, not a synthetic
  recovery artifact.

The exactness is the security property. Recovery never adopts a foreign commit
that happens to sit at the expected version; never reparents a prepared write
onto a newer branch winner; preserves a disjoint foreign winner while
compensating only owned effects; and **fails closed** when a foreign commit
buries an owned effect on the same table (restoring through someone else's
data is the one thing it will never do). Every completed action lands an
internal audit row in `_graph_commit_recoveries.lance` (no public CLI query for
it yet — a known gap).

When recovery runs:

- **Read-write open** runs the full all-or-nothing sweep.
- **Every write entry point and `refresh`** runs a roll-forward-only heal
  in-process, so a long-lived server converges on its next write without a
  restart. Rollback-requiring sidecars defer to the next read-write open
  (a background reconciler for that residual is **(roadmap)**).
- **Read-only open** repairs nothing, but refuses to serve a torn
  schema-apply state (fixed manifest outcome visible, schema identity not yet
  live) rather than lying.

All active writers emit an identity-bearing schema-v9 envelope. Historical
payload field names such as `protocol_v3`, `protocol_v4`, `protocol_v7`, and
`protocol_v8` describe retained per-writer payload shapes, not older active
envelopes. A pre-v9 file without explicit table identity is refused; recovery
never infers ownership from an alias or path.

Ahead-of-manifest drift *not* covered by any sidecar is never silently
adopted: writers refuse it and point at `omnigraph repair`, which classifies it
explicitly — verified maintenance drift (`ReserveFragments`/`Rewrite`) publishes
with `--confirm`; anything semantic requires `--force --confirm` after
deliberate review.

---

## Branches, commits, and merge

### Branch mechanics

A graph branch is logically one row set in `__manifest` (`BranchContents` is
the single authority); physically, per-table Lance forks materialize **lazily**
on first write — a fresh branch costs O(1) regardless of graph size, and its
unwritten tables physically *are* the parent's (which is why the read path's
cache keys use physical identity, and why cross-branch index reuse is free).

Branch create/delete are *control operations*, not graph commits: they emit no
lineage, and they deliberately have no recovery sidecar. Lance's native
create/delete are each physically two-phase with no conditional primitive, so
OmniGraph wraps them in authority-derived reconcilers: names are prevalidated,
live names are path-prefix-disjoint, an absent-ref clone-only tree is
reclaimed by the next same-name create, and delete removes authority before
best-effort tree cleanup (absent ref ⇒ success). This is invariant 3's shape
applied to metadata: when the logical target is fully derivable from one
existing authority, a reconciler beats a sidecar — and it degrades to a no-op
if Lance later closes the physical gap.

After the complete schema → branch → table gate envelope is held, each control
uses one operation-local manifest/namespace capture. It does not refresh the
handle-local coordinator before and after table-gate acquisition. A successful
ref transition explicitly invalidates derived read caches so a reused branch
name cannot inherit stale handles or topology from its prior incarnation.

### Commits and time travel

Every publish appends one `graph_commit` row (ULID, parents, actor) and moves
`graph_head:<branch>` — atomically with the data, as described in
[Anatomy](#anatomy-of-a-graph-on-disk). Time travel (`snapshot_at_version`,
`entity_at`, historical queries, diffs via `diff_between`/`diff_commits`) reads
older manifest states; retention is governed by `cleanup` (see
[Maintenance](#maintenance-optimize-cleanup-repair)). Checkpoint-pinned
retention — named snapshots as authoritative retention roots — is
**(draft RFC-025)**.

### Three-way merge

`branch_merge` computes the merge base from captured commit IDs and publishes
one atomic manifest update. Its general route classifies row-by-row against
immutable base/source/target snapshots (ordered cursor merge, batched staging
writer). A proven insertion-only descendant instead verifies every contiguous
v1 certificate in the complete source-history interval: exact parent and UUID,
filtered insertion-only `Update`, no rewrite/update residue, full nested schema
preorder, physical-row totals, stable identity/path/row IDs, and native ancestry.
It then streams the exact source row-version interval through the same bounded
fenced transaction chain without the general ordered diff or temporary delta.
Conflicts are typed (`DivergentInsert`, `DivergentUpdate`, `DeleteVsUpdate`,
`OrphanEdge`, plus constraint violations re-checked Δ-scoped at the merge
boundary) and surface as structured 409s. The merge-pair truth table test pins
all 81 `(left_op, right_op)` cells — adding an op forces a compile error until
the new row and column are dispositioned.

The contract under concurrency: **"merge the captured source commit," never
"substitute whatever source is latest."** A source advance after capture is
fine; a target change is `ReadSetChanged`. Merge's identity-bearing v9 recovery envelope
pre-mints each table's exact ordered transaction chain, so recovery proves a
contiguous prefix of *this merge's* commits rather than inferring ownership
from version arithmetic.

Cost honesty: a keyed append-only fast-forward routes proven new rows through
bounded exact-`id` filtered transactions—never committed Append—and the test
structurally pins that writer, zero target-ID preflights, zero target merge
joins, and zero ordered-diff cursors. Lance `InsertBuilder` only stages the
immutable files; OmniGraph replaces its uncommitted Append descriptor with the
certified `Update`, so a second branch generation can prove the output again.
Both source and existing-target native ref incarnations are revalidated under
the final gates. A first-touch lazy target does not enter this data-replay route;
it keeps the ref-only fork. Missing or unfamiliar history falls back to the
general ordered diff. A diverged or unproven merge still classifies full-width;
its `__manifest` open amplification still grows with history. O(delta) merge
via row-version lineage is **(research-blocked RFC-027)** — the deletion-delta
source doesn't exist yet — and fragment-adoption merge is **(draft RFC-0001)**.

The final predeclared five-pair production series passed the fixed bulk gates.
At 10K rows, median production/comparator operation time was 31/8 ms
(**3.875×**) and maximum signed paired peak-RSS overhead was 24,297,472 bytes;
at 100K it was 136/35 ms (**about 3.886×**) and 32,604,160 bytes. Every route,
exact-content, and setup/operation/verification phase check passed.

---

## Schema and migration — the strand model

The `.pg` language declares node/edge types, interfaces, properties,
constraints (`@key`, `@unique`, `@card`, `@range`, `@check`, enums), and
physical intent (`@index`, `@embed`). The compiler produces a typed catalog;
a linter (`OG-XXX-NNN` codes) gates footguns. `schema plan` diffs the accepted
schema against the proposal and produces a migration plan; `schema apply`
executes it under the `__schema_apply_lock__` system branch, as a first-class
RFC-022 writer (identity-bearing v9 envelope — the fixed manifest outcome lands *before* schema
staging is promoted, and capture-time coherence means readers can never observe
the manifest-before-catalog window on a live handle).

Applies are metadata-only wherever possible — adding an `@index` or widening an
enum bumps no table version. Destructive or narrowing changes are refused
(`OG-MF-106`) rather than lossy.

**Storage versioning is strict single-version** (the strand model,
[versioning.md](versioning.md)): this binary reads exactly one internal
manifest schema (`MIN_SUPPORTED == CURRENT == 6`). An older graph is refused
with a self-service export/import rebuild recipe naming the right old release;
a newer graph is refused with "upgrade omnigraph". There is deliberately no
in-place migration dispatcher — that machinery is permanent liability (every
format change would carry a tested `vN→vN+1` walker plus legacy readers plus
crash-recovery paths, forever) for a pre-1.0 format. The stamp +
`refuse_if_stamp_unsupported` floor is exactly the seam that would re-introduce
it if a concrete graph ever demands it. Note the four version axes (release /
wire / storage / Lance) have deliberately different policies — conflating them
is how you ship a silent misread or carry migration code you don't need.

Internal schema v5 introduced [RFC-028](../rfcs/0028-stable-schema-identity.md):
accepted SchemaIR v2 owns one graph identity domain and monotonic allocator;
type/property IDs survive explicit renames, while drop/re-add mints a new table
identity and incarnation. Manifest rows, paths, OCC, and recovery carry that
identity pair instead of reconstructing ownership from a mutable name. The
currently served v6 format preserves that identity model and adds RFC-023:
every graph table declares exact non-null physical `id` as Lance's unenforced
PK from creation, and every production insert/upsert uses the filter-bearing
keyed adapter. Moving from v5 to v6 is rebuild-only; the genuine cross-version
binary rebuild/refusal run passed on 2026-07-15.

---

## The query engine

The `.gq` language: named queries with parameters, `match` patterns over typed
nodes/edges (including interface polymorphism), `where` filters, `not { … }`
anti-joins, variable-length traversal, `order`/`limit`, aggregations, and the
search functions. Everything lowers to typed IR — there is no string-SQL
side-channel; filters push down to Lance as structured expressions.

Multi-modal retrieval is the differentiating runtime capability: one query can
rank by `rrf(nearest($d.embedding, $q), bm25($d.body, $q_text))` — vector ANN
fused with BM25 by reciprocal rank — then expand graph structure from the
survivors, all against one snapshot.

Traversal executes on the scoped CSR/CSC topology index by default, with a
BTREE-indexed `Expand` mode asserted semantically equivalent (property-based
tests generate adversarial graphs — cross-type ID collisions, cycles,
self-loops — so a future divergence between modes fails loudly).

Current honesty (**roadmap** items, from [Known gaps](invariants.md#known-gaps)):
execution is lowering-ordered, not cost-based — planner capability/statistics
surfaces don't exist yet; multi-hop still uses `TypeIndex` and eager
materialization in places (stable row-IDs, SIP, factorization are target
patterns); rank/score don't yet propagate everywhere as ordinary columns;
Cedar predicates in the planner and a unified `Source` operator are design
directions, not code.

---

## Derived state: indexes, embeddings, topology

Invariant 7's family, all one shape — *declared intent, derived
materialization, correct reads at any coverage*:

- **Physical indexes.** `@index`/`@key` declare intent; type dispatch picks the
  kind (enum/orderable scalar → BTREE, free-text String → FTS, Vector → vector
  ANN). Writes never build indexes inline — mutation/load/schema-apply publish
  only their exact data effects. `ensure_indices` materializes every missing
  artifact for a table in **one** staged mixed CreateIndex transaction (the v9
  envelope retains the `protocol_v8` payload field); `optimize` separately
  folds new fragments into existing indexes.
  Reads are correct under partial coverage (Lance unions indexed and scan
  paths; vector search falls back to brute force). A background reconciler to
  automate the explicit calls is **(roadmap)**.
- **Embeddings.** `@embed` records which text property seeds which vector and
  with which model; the loader does *not* call an embedding API on the write
  path (deny-listed — a network call in the commit path). Vectors arrive in
  the load data or via the offline `omnigraph embed` pipeline; query-time
  `nearest($v, "text")` auto-embeds the query string. Ingest-time embedding via
  an `ensure_embeddings`-style reconciler — an embedding is derived state, same
  class as an index — is **(draft RFC-015 / RFC-012 phase)**.
- **Topology.** The CSR/CSC graph index is built per query, scoped to
  traversed edge types, cached by physical table identity, reused across
  lazy-fork branches. Never persisted, never authoritative.

---

## Maintenance: optimize, cleanup, repair

- **`optimize`** compacts fragments and folds index coverage across all tables
  with bounded parallelism and **one graph visibility envelope**: one
  identity-bearing v9 sidecar with bounded maintenance provenance pins the
  complete productive set before any HEAD moves, and one
  monotonic manifest CAS publishes everything together — two changed tables
  become visible atomically, a no-work run leaves no trace. It also compacts
  `__manifest` itself (physical-only, no graph commit), which is what keeps
  write/read cost flat in history on a periodically-optimized graph.
- **`cleanup`** is version GC: explicit `--keep`/`--older-than` cutoffs derived
  from Lance's actual version lists, floored by live lazy-branch inheritance
  and recovery needs, refusing on pending sidecars or uncovered drift (GC must
  never outrun the recovery barrier). Internal-table version GC is not yet
  wired in **(deferred — needs the cleanup-resurrection watermark)**, so
  `__manifest/_versions/` grows until explicit cleanup.
- **`repair`** is the human-in-the-loop path for uncovered drift, described in
  [The life of a crash](#the-life-of-a-crash).

None of these are background loops today — they are explicit operator/agent
calls (the cluster control plane and a future reconciler are the automation
story).

---

## Serving: server, cluster, policy

- **Engine-wide Cedar enforcement.** Every `_as` writer — mutation, load,
  schema apply, branch create/delete/merge — calls
  `Omnigraph::enforce(action, scope, actor)` inside the engine, so HTTP, CLI,
  and embedded SDK callers hit the *same* gate. HTTP additionally resolves
  bearer → actor and applies per-actor admission control before the engine.
- **Auth hygiene.** Bearer tokens are hashed (SHA-256) at startup — plaintext
  never persists in process memory; comparison is constant-time; the actor ID
  is server-resolved from the hash match and never client-settable. Kernel
  crates have no HTTP/auth dependency (invariant 11).
- **Cluster-only boot (RFC-011).** The server always boots from a cluster
  directory (`--cluster <dir|s3://…>`) and serves N ≥ 1 graphs under
  `/graphs/{id}/…`. The cluster directory (`cluster.yaml` + content-addressed
  state ledger) declares graphs, schemas, stored queries, embedding providers,
  and policies as code; `cluster apply` converges it (with digest-bound
  approvals for destructive dispositions, sidecar-covered executors, and
  crash-window failpoint coverage in `omnigraph-cluster`). Runtime add/remove
  endpoints are deliberately absent: operators `cluster apply` and restart.
- **Two-surface operator config (RFC-007/008/011):** the team-owned cluster
  directory plus per-operator `~/.omnigraph/config.yaml` (servers, credentials,
  actor, profiles, aliases). The CLI's embedded and remote paths are held
  equivalent by a parity-matrix test (every forked verb run against both arms;
  divergences pinned in a ledger, never silently repaired).

---

## Concurrency and the support boundary

What is guaranteed, from strongest to most bounded:

1. **Snapshot isolation per query** — always, any topology.
2. **Same-process concurrency** — fully arbitrated: shared root-scoped gates
   order writers, healers, and recovery; readers are never gated; capture-time
   coherence protects snapshot/catalog pairs.
3. **Cross-process, failure-free non-destructive commit arbitration** — an
   individual fenced table transaction plus the publisher's exact CAS
   precondition admits one same-key winner; losers get typed conflicts
   (`KeyConflict`/`ReadSetChanged`/`RecoveryRequired`), never silent key merge.
   This does not make recovery beside a live foreign writer safe. Concurrent
   multi-process writers on one graph are *functional* in the failure-free
   one-winner-CAS case, not a supported high-contention or failover topology.
4. **Cross-process, destructive recovery** — **the documented boundary.**
   Recovery's rollback (`Dataset::restore`) and first-touch reclaim are unsafe
   beside a live writer in *another process*: Lance's restore silently orphans
   concurrent commits (empirically pinned), Lance exposes no conditional
   ref create/delete, and the gates are process-local. Exact v3/v4/v7/v8
   ownership prevents *false adoption* — recovery will never claim foreign
   work — but it cannot *fence* a live foreign process. Closing this needs a
   distributed fence (a lease on the schema-apply lock branch is the sketched
   direction, **(roadmap)**; RFC-019's fencing direction now lives in
   RFC-023/024).

Multi-version binaries against one graph are refused by the storage stamp
(open- and publish-time checks); the residual read-only hole is a recorded
known gap, deliberately not defended because the topology itself is
unsupported.

---

## How we test

[testing.md](testing.md) is the map; these are the principles that make the
suite worth trusting:

- **Extend before adding; test at the boundary being changed.** Planner
  changes get planner-level tests; storage changes get storage/recovery tests;
  end-to-end tests never substitute for missing lower-level assertions.
- **Cost budgets are tests.** IO-counted budgets at commit-history depth
  (`warm_read_cost.rs`, `write_cost.rs`, `merge_cost.rs`) pin that hot-path
  cost is bounded by working set, not history — because an O(commits) bug is
  green in every correctness test.
- **Failure injection over hope.** The `failpoints` suites crash every writer
  at every protocol boundary (post-stage, pre-effect, post-effect,
  pre-publish, post-publish…) and assert exact recovery outcomes, including
  the adversarial cells: foreign winners preserved not adopted, buried effects
  failing closed, ABA on branch delete/recreate fenced.
- **Truth tables force disposition.** The 9×9 merge-pair matrix compiles-in
  completeness: a new op variant fails the build until every combination is
  dispositioned.
- **Guards pin the outside world.** Lance surface guards pin substrate
  behavior; `forbidden_apis.rs` pins the closed write surface;
  `failpoint_names_guard.rs` pins that no failpoint can silently never fire.
  Red guards are *information* — several are deliberately built to turn red
  when an upstream bug gets fixed, so the workaround gets removed.
- **Test-first bug fixes**, with the red commit landing immediately before the
  green one so any reviewer can reproduce the failure from history.

---

## What we deliberately exclude (and why)

The normative deny-list is in [invariants.md](invariants.md); this is the
reader-facing rationale for the exclusions people actually ask about. None are
"not yet" — each is a decision with reasoning. (The burden on any exception is
on the proposer.)

- **Cross-query `BEGIN`/`COMMIT` transactions.** Branches are strictly better
  for this substrate and workload: same isolation, plus review, diff,
  attribution, and history; no interactive lock lifetime held over object
  storage. See [design principle 6](#6-branches-are-the-transaction-model).
- **A custom WAL / transaction manager / buffer pool.** Lance owns durability
  primitives. Our recovery sidecars are *intents over Lance commits*, not a
  parallel log of data. (Streaming ingest will consume Lance's MemWAL rather
  than building one — **(draft RFC-026)**.)
- **Mixed constructive/destructive single mutations (D₂).** Keeps in-query
  read-your-writes unambiguous and each table at one commit per query; the
  alternative buys an in-query delete-view machine in the hot path.
- **Inline index/embedding builds on the write path.** Expensive derived state
  converges from manifest state (reconciler shape); a network call or index
  retrain in the commit path is deny-listed.
- **Placeholder nodes for orphan edges** and any silent integrity weakening.
  Integrity failures are loud, pre-publish.
- **In-place storage migration.** The strand model — see
  [Schema and migration](#schema-and-migration--the-strand-model).
- **Job queues for manifest-derivable state.** A reconciler that re-derives
  from the source of truth is drift-proof; a queue is a second copy of intent.
- **Actor identity from clients.** The server resolves actor from the token
  hash; a client-supplied actor would make the audit trail decorative.
- **Cloud-only correctness fixes / engine forks.** Correctness is always OSS;
  Cloud extends by trait, never by fork.
- **Runtime graph add/remove endpoints.** The cluster directory is the source
  of truth; converge it and restart — a mutable serving topology invites
  drift between declared and actual state.

---

## Current implementation status

The always-current shipped-vs-roadmap ledger is
[invariants.md](invariants.md)'s **Current Truth Matrix** and **Known Gaps** —
this section is the orientation summary, not the authority.

**Solid and shipped:** the unified write protocol with exact per-writer
recovery (RFC-022 implemented 2026-07-13, across PRs #343–#353); manifest-atomic
multi-table publish with lineage-in-CAS; branches/commits/time-travel/merge
with typed conflicts; the strict-strand storage model; unified Δ-scoped
validation; engine-wide Cedar; cluster control plane and
cluster-only serving; the warm read-path cost contract; sealed write surface;
the full failpoint/recovery test lattice; stable schema identity and table
incarnation (RFC-028, introduced in internal schema v5).

**Explicitly bounded:** Optimize's v9 recovery envelope (bounded maintenance
provenance; exact provenance trigger-gated on upstream API + distributed fencing); destructive
recovery's single-writer-process boundary; merge cost at divergence
(full-width classification).

**Implemented:** substrate-native key-conflict fencing **(RFC-023, internal
schema v6)**, including the sealed production routing/PK/error/recovery
contract, the inductive insertion-absence certificate, bounded branch replay,
cross-version rebuild/refusal evidence, duplicate-repair runbook, and passed
10K/100K production latency/RSS acceptance series.

**Roadmap / research:** durable table heads /
heads format **(RFC-024, research-blocked after Gate A rejected the first
physical access shape)**; checkpoint-pinned retention **(RFC-025, draft)**;
MemWAL streaming ingest **(RFC-026, draft)**; lineage-based merge deltas
**(RFC-027, research-blocked)**; background reconciler; planner
statistics/cost model; policy pushdown; ingest-time embeddings; per-query
resource budgets.

---

## Risk register

| Risk | Severity | Posture / mitigation |
|---|---|---|
| **R1: Destructive recovery beside a live foreign process.** Lance restore orphans concurrent commits; no conditional ref primitives; gates are process-local. | High | Documented single-writer-process support boundary; exact ownership prevents false adoption; distributed fence (lease on the schema-apply lock branch) is the sketched close **(roadmap)**. Do not promote multi-process write topologies before it exists. |
| **R2: Pre-stable Lance pin.** 9.0.0-beta.21 via git rev; betas have regressed mid-line before (blob reads broke in beta.13, fixed beta.15). Blocks crates.io publishing (v0.8.1 is binaries-only; v0.9.0 gated on 9.0.0 stable). | High | Full alignment audit per bump (all commits reviewed, findings in [lance.md](lance.md)); surface guards as first smoke check; `cargo test --workspace` as the alignment gate, never the build alone. |
| **R3: RFC-023 consumes a beta.21 route-dependent key-filter primitive.** Lance's filter emission and filtered/unfiltered resolution are directional (probed 2026-07-14). | Medium | v6 closes production insertion-bearing routes through exact-`id`, forced-v2 filtered staging and source-guards bare Append; the adapter verifies the emitted field-ID filter and effect-aware recovery refuses ambiguity. Guards pin both conflict orders so any upstream symmetry/route change forces an audit. Three-run release evidence passed the 1M forced-v2 50 ms median / 256 MiB max-RSS thresholds (29 ms / 243,875,840 bytes). |
| **R4: Manifest fold cost grows with commit count.** Current-state resolution folds history. | Medium | `optimize` compacts internal tables (keeps periodically-optimized graphs flat — cost-gated every PR). RFC-024 Gate A found flat exact-BTREE scan work at width 10, including observable/reconcilable uncovered tails, but rejected the format because representative RustFS 20→80 uncompacted cold reads/bytes (34→94 / 61,947→121,592) and compacted byte terms grow. RFC-024 is research-blocked; v6 retains the journal fold and internal-table *cleanup* remains deferred behind the resurrection watermark. |
| **R5: Schema identity corruption or alias/identity drift.** Internal schema v5 introduced stable IDs/incarnation as durable authority; v6 preserves them. | Medium | Open/init validate the SchemaIR domain and exact bidirectional IR↔manifest identity/path/alias contract; every active recovery envelope carries the identity pair; zero, duplicate, missing, or mismatched identity fails closed. |
| **R6: Merge cost at divergence** — full-width classification and history-growing manifest folds. | Medium | Coherent coordinator scans plus retained probe handles reduced the pre-slice measured depth-5/depth-80 baseline from 59/651 manifest reads to 40/410 and cap the common fast-forward route at three internal opens and three scans, but the uncompacted-history slope remains. `merge_cost.rs` keeps both facts visible; O(delta) merge is blocked on a real deletion-delta source **(RFC-027)**; fragment adoption is **(draft RFC-0001)**. |
| **R7: No streaming ingest** — per-branch write throughput is capped by the `graph_head` CAS rate; high-frequency small writes are wasteful. | Medium | Deliberate: the interactive path's guarantees come first. MemWAL-based ingest with durable per-row ack + graph-atomic folds is the design **(RFC-026)**. MemWAL is the strategic substrate, but beta.21's public initializer commits opaquely and shard provisioning is separate; activation waits for a public exact enrollment receipt plus reversible admission seal rather than using private APIs. |
| **R8: Some operations lack enforced memory/time budgets.** | Medium | Known gap, narrowed and accepted for RFC-023. Its direct-substrate instrument rejected the first whole-delta fenced adopt (~447 MB peak at 100K × 256 versus ~74 MB Append), and the first corrected production 10K series failed at 30.0× / 108,625,920 bytes overhead; both negative results remain evidence. Mutation/Load now refuses a keyed table above 8,192 rows / 32 MiB before arm, while BranchMerge uses a recovery-enrolled chain with the same per-chunk bounds and a 1,024-transaction ceiling. The inductive certificate route removes the general diff, temporary delta, target preflight, and target join without weakening that chain. Final five-pair production medians passed at 31/8 ms (3.875×) for 10K and 136/35 ms (~3.886×) for 100K; maximum signed paired RSS overheads were 24,297,472 and 32,604,160 bytes. Inclusive row/transaction ceilings, byte refusal (including materialized blobs), operation-wide validation retention, exact source/target incarnation revalidation, second-generation certificate composition, and both between-chunk recovery directions are pinned; other operations still need explicit bounds. |
| **R9: Local-FS conditional-write emulation** (`write_text_if_match` check-then-act gap). | Low | All current callers sit behind the cluster lock protocol; S3 uses true conditional puts; close before admitting any lock-free caller. |
| **R10: Doc/spec drift as the system grows** — this document included. | Low | Maintenance contract (same-PR doc updates, `check-agents-md.sh` link CI, "don't lie" stale markers); this canon defers to area docs by construction. |

---

## Open questions

Live design questions, each owned by an RFC or a known gap — not a wishlist:

1. **What fences a live foreign process?** Lease on `__schema_apply_lock__`, a
   Lance conditional-ref primitive if upstream ships one, or an external lease
   service? Owns the R1 close; prerequisite for exact Optimize provenance and
   any multi-process write story.
2. **What makes deletion discovery sublinear?** RFC-027 is blocked precisely
   here: a deleted row is absent from the target snapshot, so version columns
   can't identify it, and `_row_last_updated_at_version` filtering is O(rows)
   without a substrate index or change log.
3. **What makes latest current-state authority physically history-flat?**
   RFC-024's first in-manifest BTREE candidate bounds exact row/range/page work,
   but cannot bound latest-manifest object reads and compacted byte ranges. A
   successor access shape must pass the original cold/warm,
   compacted/uncompacted local and object-store gate; the answer is not to
   weaken the gate or add a second authority dataset.
4. **Which capability owns the next rebuild boundary after v6?** RFC-028
   activated stable identity in v5; RFC-023 assigns exact-`id` fencing to v6.
   RFC-024's heads, RFC-025's retention, and RFC-026's stream capability remain
   independently reviewable. Any later format activation requires its own
   export/init/load rebuild unless capabilities deliberately co-release after
   their combined matrix passes.
5. **What is the checkpoint/retention contract?** RFC-025: checkpoint rows as
   logical authority, Lance tags as physical pins, and the asymmetric safe
   ordering between them — plus how the Q8 resurrection watermark unlocks
   internal-table cleanup.
6. **How does the background reconciler arrive without violating the recovery
   model?** It must serialize with writers through the same gates and stay
   roll-forward-only until the fence exists.
7. **When Lance 9.0.0 stabilizes**, what does the beta→stable alignment audit
   surface, and does crates.io publishing (v0.9.0) unblock cleanly?

---

## Roadmap — the RFC family

The plan of record is the RFC-022…028 family (all under
[docs/rfcs/](../rfcs/README.md), maintainer design series, reviewed in the
[RFC-022–028 ledger](rfc-022-027-architecture-review.md)):

| RFC | Owns | Status |
|---|---|---|
| [0022 — Unified graph-write protocol](../rfcs/0022-unified-write-path.md) | One correctness protocol for all graph-visible writes; per-writer effect adapters; synchronous recovery | **Implemented** (2026-07-13) |
| [0028 — Stable schema identity and table incarnation](../rfcs/0028-stable-schema-identity.md) | Graph-scoped rename-stable type/property IDs, table lifetimes, SchemaApply recovery, and the shared strict-rebuild prerequisite | **Implemented** (2026-07-15) |
| [0023 — Key-conflict fencing](../rfcs/0023-key-conflict-fencing.md) | Substrate-native keyed-write fencing via Lance's unenforced-PK filter; fleet/format activation barrier | **Implemented** (2026-07-15) |
| [0024 — Durable table heads](../rfcs/0024-durable-table-heads.md) | Materialized head-row research; the first exact-BTREE candidate bounded scan work but failed the full latest-manifest/object-byte cost gate | **Research blocked** |
| [0025 — Checkpoint-pinned retention](../rfcs/0025-checkpoint-retention.md) | Named checkpoints as authoritative retention roots, materialized as Lance tags | Draft |
| [0026 — MemWAL streaming ingest](../rfcs/0026-memwal-streaming-ingest.md) | Durability-first streaming writes: ack on WAL durability, asynchronous graph-atomic folds | Draft |
| [0027 — Lineage merge deltas](../rfcs/0027-lineage-merge-deltas.md) | O(delta) merge classification from row-version lineage | Research-blocked |

Deliberately split, not one mega-format: identity, key fencing, head rows,
retention, ingest, and merge deltas are separate irreversible decisions with
different substrate gates and rollout barriers ("reversibility shapes evidence
demand").
Release-wise: v0.8.1 ships as binaries only (the Lance git pin blocks
crates.io); v0.9.0 is gated on Lance 9.0.0 stable.

---

## Maintaining this document

- This canon is a **narrative over the area docs**, which stay authoritative.
  When behavior changes, update the owning area doc in the same PR (per the
  maintenance contract) and fix the corresponding narrative here; if you can't
  rewrite a section fully, mark it `*(stale — needs update after <change>)*`.
- Every claim is either current-shipped or carries an explicit status marker.
  Keep it that way — one top-of-file disclaimer does not license aspirational
  prose below it.
- Update the **Surveyed** line, the status ledger, the risk register, and the
  RFC table when the underlying facts move (release, Lance bump, RFC status
  change, gap opened/closed).
- Structural changes (new sections) should keep the reading order a *story*:
  substrate → disk → read → write → crash → branch/merge → schema → query →
  derived state → operations → boundaries → status → future.
