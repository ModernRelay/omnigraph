# WAL Streaming Architecture

## 1. Purpose

This document defines the right long-term architecture for Omnigraph change streaming.

The core decision is:

- **Omnigraph core should produce a canonical WAL**
- **stream delivery should happen in a separate process**
- **sink execution and consumer cursor state should not live inside the main database runtime**

This is the architecture to build toward.

It is intentionally different from an in-process polling-and-sink model.

## 2. Core Decision

Omnigraph should not treat subscriptions as “diff snapshots and fire sinks from the DB handle.”

Instead:

1. writes produce committed graph changes
2. committed graph changes produce WAL batches
3. a separate streaming worker reads WAL
4. the worker enriches if needed
5. the worker delivers to sinks
6. the worker owns cursor and retry state

That gives the clean separation of concerns:

- **database**: commit graph state correctly
- **WAL**: describe committed graph changes canonically
- **stream worker**: manage delivery and retries
- **sinks**: push changes to external systems

## 3. Architectural Principles

### 3.1 Single Source Of Truth

The committed graph state is the source of truth.

The WAL is the canonical change log derived from committed writes.
Sinks are downstream effects only.

### 3.2 No Sink Logic In The Core Runtime

The main Omnigraph runtime should not:

- spawn shell commands
- POST webhooks
- call Inngest
- own retry policies
- own delivery cursors

Those are streaming concerns, not database concerns.

### 3.3 Commit First, Deliver Later

Graph commits must not depend on external delivery.

Writers should never block on:

- HTTP delivery
- subprocess success
- downstream agent availability

### 3.4 Consumer Progress Is Operational State

Consumer cursor state is not graph state.

It should not:

- branch with the graph
- merge with the graph
- roll back when the graph rolls back
- live inside the graph repo as part of database history

### 3.5 Enrichment Is Optional And Downstream

The WAL should contain stable graph-level change facts.

Full entity hydration should be a worker concern, not the canonical WAL payload.

## 4. Recommended System Shape

```text
Omnigraph Write Path
    ↓
Committed Graph State
    ↓
WAL Batch Per Commit
    ↓
WAL Stream Worker
    ↓
Optional Enrichment
    ↓
Sink Delivery
```

Components:

- **Omnigraph core**
  - applies writes
  - creates graph commits
  - writes WAL batches
- **WAL storage**
  - durable, ordered by branch commit history
- **stream worker**
  - reads committed WAL
  - owns cursor and retry state
  - delivers to sinks
- **sinks**
  - shell
  - webhook
  - Inngest
  - future queue/bus integrations

## 5. What Goes In The WAL

The WAL should contain **graph-level committed events**, not fully enriched sink payloads.

Each event should include at least:

- `graph_commit_id`
- `branch`
- `manifest_version`
- `parent_commit_id`
- `event_seq`
- `kind`
  - `node`
  - `edge`
- `type_name`
- `entity_id`
- `op`
  - `insert`
  - `update`
  - `delete`
- `endpoints`
  - edges only
- `timestamp`
- optional `changed_fields`
- optional `source`
  - `load`
  - `mutate`
  - `merge`
  - `publish_run`

The WAL should **not** try to be the final webhook payload.

Reason:

- sink formats will evolve
- enrichment policy will evolve
- full payload snapshots are heavier and less stable
- canonical logs should stay minimal and durable

## 6. WAL Storage Model

Do **not** start with one giant global append-only subscription table.

The cleaner model is:

- one WAL batch per committed graph commit
- commit graph row points to that batch

Recommended shape:

- `_graph_commits.lance` remains the branch and commit DAG
- each commit row gains a WAL pointer, such as:
  - `wal_uri`
  - `wal_event_count`

Recommended WAL object layout:

- `wal/commits/<graph_commit_id>.jsonl`
- or `wal/commits/<graph_commit_id>.json.zst`
- or another immutable batch format

The important property is not the exact encoding.
The important property is:

- **WAL is immutable**
- **WAL is commit-scoped**
- **commit graph is the visibility barrier**

## 7. Why Per-Commit WAL Batches Are Better

This is better than a single shared append-only WAL table because it avoids awkward atomicity problems.

Recommended write order:

1. compute the committed change batch
2. write the WAL batch artifact for the future commit
3. write the graph commit row that references the WAL batch
4. publish that commit into the branch head

This gives clean semantics:

- if WAL batch write succeeds but commit fails:
  - orphan batch exists
  - it is harmless and can be garbage-collected
- if commit row exists:
  - the referenced WAL batch must exist
- workers only stream commits that are actually visible in commit history

That is much cleaner than trying to make one shared WAL table globally atomic with manifest updates.

## 8. Write Path Responsibilities

The write path should do exactly these streaming-related things:

1. determine the net graph changes for the committed branch transition
2. encode those changes into a WAL batch
3. persist the WAL batch
4. record the graph commit with a pointer to that WAL batch

The write path should not:

- execute sinks
- manage retry
- manage consumer cursors
- know about delivery endpoints

## 9. Branch Semantics

WAL must follow **branch-visible commits**, not internal staging branches.

Rules:

- internal run branches do not emit public WAL
- `publish_run()` emits WAL for the target branch commit that becomes visible
- merge emits WAL for the target branch’s new merged commit
- source branch history remains source branch history

A worker subscribing to branch `main` should see:

- only commits that become visible on `main`
- in branch commit order

That is the right external mental model.

## 10. Worker Architecture

The worker should be a separate binary or service.

Suggested name:

- `omnigraph-stream`

Responsibilities:

- discover new graph commits on a branch
- fetch the WAL batch for each unseen commit
- optionally hydrate entity payloads from snapshots
- deliver to sinks
- persist consumer progress
- handle retries and dead-letter policy

This worker can be deployed:

- locally
- on EC2
- in ECS
- on-prem with RustFS/MinIO-backed repos

## 11. Cursor And Retry State

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
- Postgres for general server-side deployments

Do **not** store delivery cursors inside the graph repo itself.

Reasons:

- consumer state is not graph state
- consumer state should not branch or merge
- repo rollback should not silently roll back delivery state
- multiple independent consumers need isolation

## 12. Enrichment Model

The worker may expose two delivery modes:

- **raw WAL events**
- **enriched events**

Enriched event flow:

1. worker reads WAL event
2. worker opens the relevant snapshot context
3. insert and update can hydrate from the post-commit snapshot
4. delete can hydrate from the pre-commit snapshot if needed
5. sink receives the enriched payload

That preserves a clean layering:

- WAL stays canonical and minimal
- enrichment stays configurable
- sinks stay decoupled from storage internals

## 13. Sink Layer

Sinks belong in the worker, not in the core runtime.

Initial sinks can still be:

- shell
- webhook
- Inngest

But they should be worker plugins over a common sink trait.

That lets the database stay stable while delivery evolves independently.

## 14. Recommended APIs

## Core runtime

Keep the core runtime focused on graph operations plus WAL production.

Possible internal API shape:

```rust
struct WalEvent { ... }
struct WalBatch { ... }

fn build_wal_batch(...) -> WalBatch
async fn persist_wal_batch(...) -> Result<WalBatchRef>
async fn record_graph_commit_with_wal(...) -> Result<GraphCommitId>
```

## Worker

The worker should expose operational commands like:

```text
omnigraph-stream run
omnigraph-stream once
omnigraph-stream status
omnigraph-stream replay --from-commit ...
omnigraph-stream reset-consumer ...
```

## 15. Deployment Model

### Current bridge phase

Before full streaming rollout:

- Omnigraph can still expose manual inspection APIs
- but sink delivery should not become part of the main server contract

### Target AWS shape

- Omnigraph server runtime
- separate `omnigraph-stream` worker service
- shared S3-backed repo
- external cursor store
- independent scaling and restart behavior

### Target on-prem shape

- Omnigraph server runtime
- separate stream worker
- RustFS or another S3-compatible store
- local SQLite or another external cursor store

## 16. What This Means For The Current Subscriptions Work

The current in-process subscriptions implementation is still useful as a prototype for:

- event shape
- sink trait ideas
- enrichment rules
- filtering semantics

But it should not be treated as the final architecture.

The right end state is:

- WAL in the core
- streaming worker outside the core
- cursor state outside the repo

## 17. Phased Plan

### Phase 1

- define canonical WAL event schema
- produce WAL batches for committed writes
- attach WAL pointer to graph commits

### Phase 2

- build `omnigraph-stream`
- support one branch and one consumer
- raw event replay only

### Phase 3

- add enrichment in the worker
- add sink plugins
- add external cursor store implementations

### Phase 4

- add operational tooling:
  - replay
  - reset
  - dead-letter handling
  - metrics

## 18. Bottom Line

The right architecture is not:

- “diff snapshots in the DB handle and fire sinks from there”

The right architecture is:

- **Omnigraph writes committed graph state**
- **Omnigraph emits WAL**
- **a separate worker streams WAL**
- **the worker enriches and delivers**

That is the clean separation of concerns and the architecture that will scale operationally.
