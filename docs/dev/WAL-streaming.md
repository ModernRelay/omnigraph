# Graph WAL / Change Streaming Architecture

## 1. Purpose

This document defines the right long-term architecture for answering one question:

- **what changed in the graph?**

The answer needs to work for:

- one in-process consumer
- many consumers
- polling/library mode
- a separate streaming worker

without redesigning the consumer-facing event model every time the deployment model changes.

This document keeps the term **WAL** for continuity, but the concrete artifact described here is a
**graph-level commit-time change log**, not Lance's storage-level WAL.

## 2. Core Decision

Omnigraph should not standardize on:

- diff snapshots on every poll
- read Lance MemWAL directly
- execute sinks from inside the main database runtime

Instead:

1. writes determine the committed graph delta
2. Omnigraph persists an immutable change batch for that committed graph delta
3. the graph commit row points to that batch
4. library consumers and stream workers read the same committed change batches
5. enrichment happens downstream from the canonical change log
6. delivery cursors and retry state live outside the graph repo

That gives the clean separation of concerns:

- **database**: commit graph state correctly
- **graph WAL / CDC log**: describe committed graph changes canonically
- **library pollers / stream worker**: read committed change batches
- **sinks**: deliver to external systems

## 3. What The Change Source Should Be

There are three candidate sources of "what changed":

### 3.1 Snapshot Diffing

Snapshot diffing is acceptable as a bootstrap and fallback, but it is not the right scaling model.

Pros:

- zero write-path overhead
- no extra persisted artifacts
- useful for debugging and old-commit fallback

Cons:

- every consumer re-derives the same logical change set
- N consumers means N identical diffs
- rapid polling keeps recomputing already-known facts

This remains useful for:

- ad hoc inspection
- migration fallback
- validating the new change log against current behavior

It should not remain the primary change source for subscriptions or streaming.

### 3.2 Lance MemWAL

Lance MemWAL is a storage/write optimization, not the correct application-level change abstraction.

Problems:

- one logical graph mutation can span multiple table-level WAL entries
- consumers would need to reconstruct graph events from storage internals
- internal metadata writes would be mixed with user-visible graph changes
- the abstraction level is too low

MemWAL matters for write batching and checkpoint cadence, not as the public CDC surface.

### 3.3 Commit-Time Graph Change Batches

This is the correct abstraction.

The write path already knows what it changed:

- inserts know the inserted IDs
- updates know the matched IDs
- deletes know the deleted IDs and cascaded edge deletes
- loads know every parsed entity
- merges know the resolved graph delta

That information should be captured once at commit time and persisted as an immutable change batch.

## 4. Architectural Principles

### 4.1 The Graph Commit Is The Visibility Boundary

Consumers must only process changes that belong to a commit that is visible on a branch.

Internal staging state should not be public change history.

### 4.2 The Canonical Log Should Be Minimal

The graph WAL should contain stable graph-level facts:

- commit identity
- branch visibility
- entity identity
- operation kind
- optional cheap metadata

It should not be the final sink payload.

### 4.3 Enrichment Is Downstream

Entity hydration, webhook shaping, and sink-specific payload choices belong in library consumers or
the stream worker, not in the canonical persisted log.

### 4.4 Consumer Progress Is Not Graph State

Consumer cursors:

- should not branch or merge
- should not roll back with the repo
- should not live inside the graph repo

### 4.5 One Writer Per Repo Still Applies

This design improves change streaming. It does not introduce distributed write coordination.

Omnigraph remains a single-writer-per-repo system until a separate multi-writer design exists.

## 5. Canonical Change Model

Conceptually:

```rust
struct ChangeBatch {
    commit_id: String,
    parent_commit_id: Option<String>,
    branch: String,
    pre_manifest_version: u64,
    post_manifest_version: u64,
    timestamp: i64,
    source: ChangeSource,
    entry_count: u64,
}

struct ChangeEntry {
    event_seq: u64,
    table_key: String,
    kind: EntityKind,
    type_name: String,
    entity_id: String,
    op: ChangeOp,
    endpoints: Option<Endpoints>,
    changed_fields: Option<Vec<String>>,
}
```

Important constraints:

- one immutable batch per committed graph commit
- entries are graph-level, not storage-level
- no full entity payloads in the canonical log
- `event_seq` is stable within the batch

Recommended `source` values:

- `load`
- `mutate`
- `merge`
- `publish_run`

## 6. Storage Model

Recommended persisted shape:

- `_graph_commits.lance` remains the commit DAG and branch visibility source
- each graph commit row gains:
  - `changes_uri`
  - `change_count`
  - `pre_manifest_version`
  - `post_manifest_version`

Recommended object layout:

- `_changes/commits/<graph_commit_id>.arrow`

Arrow is a good fit because:

- the change data is tabular
- the runtime already uses Arrow/Lance heavily
- it avoids inventing a second event encoding stack

The important properties are:

- **immutable**
- **commit-scoped**
- **cheap to read**
- **referenced by the graph commit row**

## 7. Publication Semantics

Publication order matters.

Recommended order:

1. compute the committed graph delta
2. write the immutable change batch artifact
3. write the graph commit row referencing that batch and recording pre/post manifest versions
4. publish that commit into the branch-visible history

Required guarantees:

- if the change batch write succeeds but the commit never becomes visible:
  - the batch is harmless orphan data and may be garbage-collected
- if a commit is visible in branch history:
  - its referenced change batch must exist and be readable
- consumers only process commits that are visible in branch history

This is the key reason to prefer per-commit immutable batches over one shared append-only change table.

## 8. Branch Semantics And Ordering

Consumers should see **branch-visible commits**, not arbitrary DAG traversal.

Rules:

- internal run branches do not emit public change history
- `publish_run()` emits the target branch commit that became visible
- merge emits the target branch's new merged commit
- a consumer subscribed to `main` sees only commits visible on `main`

Ordering must be explicit:

- consumers process commits in the branch's published commit order
- not "walk ancestors backward and hope the order is obvious"

The cursor unit is:

- `graph_commit_id`

That is the natural unit of "what has been processed."

## 9. Core Read API

The same committed change batches should power both:

- in-process/library polling
- a separate `omnigraph-stream` worker

Suggested runtime API:

```rust
async fn read_changes(
    branch: &str,
    after_commit: Option<&str>,
) -> Result<Vec<ChangeBatchRef>>
```

Where each `ChangeBatchRef` contains:

- commit metadata
- branch visibility metadata
- batch location
- pre/post snapshot references

This API should not do sink delivery.
It should expose committed change history.

## 10. Cursor Model

Cursor state should be external to the graph repo.

Recommended key shape:

- `repo_uri`
- `consumer_name`
- `branch`
- `last_graph_commit_id`
- optional `last_event_seq`
- `status`
- `last_attempt_at`
- `last_error`

Good storage options:

- SQLite for simple local installs
- DynamoDB for AWS-managed deployments
- Postgres for general server deployments

Do not store delivery cursors inside the graph repo.

## 11. Enrichment Model

The worker or library consumer may expose two modes:

- **raw change events**
- **enriched change events**

Enrichment must use exact snapshot context, not "whatever HEAD is when I happen to process it."

Rules:

- insert and update hydrate from the **post-commit snapshot**
- delete hydrates from the **pre-commit snapshot** when payload is needed
- canonical change batches stay minimal

That is why the commit metadata needs exact pre/post manifest references.

## 12. Write Path Responsibilities

The write path should do exactly these streaming-related things:

1. determine the net committed graph delta
2. write that delta into an immutable change batch
3. record the graph commit with a pointer to that batch and pre/post manifest metadata
4. publish the commit into branch-visible history

The write path should not:

- execute sinks
- own retry policy
- own consumer cursors
- know about delivery endpoints

## 13. Implementation Constraint: Do Not Buffer Huge Loads In Memory

`Vec<ChangeEntry>` is a good conceptual model, but not the right implementation strategy for large loads.

Large `load_jsonl` operations and large merges should write change entries incrementally through a
streaming batch writer.

The producer should conceptually build a batch, but physically it should support:

- incremental append
- bounded memory
- final immutable batch materialization

## 14. Relationship To Lance MemWAL

Lance MemWAL does not change the public change model.

With MemWAL:

- writes may be buffered and checkpointed differently
- a single checkpoint may cover multiple low-level writes
- the graph change batch is produced at the commit/checkpoint publication boundary

The important point is:

- the **cadence** of batch production may change
- the **format and consumer model** should not

## 15. Worker Architecture

The worker should remain a separate binary or service.

Suggested commands:

```text
omnigraph-stream run
omnigraph-stream once
omnigraph-stream status
omnigraph-stream replay --from-commit ...
omnigraph-stream reset-consumer ...
```

Responsibilities:

- discover unseen branch-visible commits
- read change batches
- optionally enrich from exact snapshots
- deliver to sinks
- persist cursor and retry state

The core runtime and the worker should share the same change-batch model.

## 16. Relationship To The Current Subscriptions System

The current in-process subscriptions implementation remains useful as a bridge for:

- event shape validation
- filter semantics
- sink trait design
- enrichment policy experiments

But it should be treated as:

- **Phase 1 bootstrap**

not the final change-streaming architecture.

Snapshot diffing should remain available for:

- debugging
- ad hoc inspection
- old-commit fallback during migration

## 17. Phased Plan

### Phase 1: Bootstrap

- keep snapshot diffing as the active change source
- use it for library-mode polling
- keep validating event and sink semantics

### Phase 2: Commit-Time Change Batches

- define the canonical change batch schema
- produce one immutable batch per committed graph commit
- attach `changes_uri` and manifest metadata to graph commits
- make library polling read change batches instead of diffing snapshots

### Phase 3: Separate Stream Worker

- build `omnigraph-stream`
- move sink execution and retries out of the main runtime
- use external cursor storage

### Phase 4: MemWAL / Checkpoint Integration

- allow batch production at checkpoint publication boundaries when needed
- keep the same consumer-facing change model

## 18. Bottom Line

The right long-term architecture is not:

- diff snapshots forever
- read Lance's storage WAL directly
- fire sinks from the DB handle

The right architecture is:

- **Omnigraph writes committed graph state**
- **Omnigraph persists a graph-level commit-time change log**
- **library consumers and stream workers read the same committed change batches**
- **the worker enriches and delivers**
- **cursor state stays outside the repo**

That is the clean boundary that scales from one local poller to a dedicated streaming service without
changing the core consumer model.
