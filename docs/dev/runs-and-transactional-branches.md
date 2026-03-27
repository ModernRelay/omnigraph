# Runs And Transactional Branches

Design note derived from:

- [Building a Correct-by-Design Lakehouse](https://arxiv.org/abs/2602.02335)
- [PDF](https://arxiv.org/pdf/2602.02335)

This note applies the useful control-plane ideas from that paper to Omnigraph.
It is not adopting the paper wholesale.
Omnigraph is a graph database, not a DAG-only lakehouse product.

The useful overlap is in:

- graph-wide publish semantics over many tables
- branch and snapshot visibility
- failure isolation
- reviewable, reproducible batch work

---

## Why This Matters

Omnigraph already has the right low-level pieces:

- per-type Lance tables
- graph-wide manifest coordination
- branch and merge semantics
- pinned snapshots

But multi-table work still needs a stronger control-plane model.

The key issue is simple:

> single-table atomicity is not enough when one logical operation touches many graph tables.

If a future batch load, index rebuild, derived-materialization job, or agent-authored graph update writes several tables and fails halfway through, Omnigraph should not expose a half-published graph state on the target branch.

That is the main useful lesson from the paper.

---

## Core Idea

For graph-wide jobs, Omnigraph should not write directly to the target branch.

Instead:

1. create a temporary transactional ref from the target branch
2. perform all writes there
3. run graph validation there
4. publish to the target branch only on success
5. otherwise keep the failed state isolated for debugging

In short:

```text
target branch
  -> transactional run ref
  -> write / validate / inspect
  -> publish or abort
```

This upgrades:

- partial failure

into:

- total failure from the perspective of downstream readers

---

## What "Run" Means In Omnigraph

This note uses `run` in a control-plane sense:

- one isolated graph job
- one pinned input graph state
- one output candidate graph state
- one publish decision

This is not limited to DAG pipelines.
For Omnigraph, a run could mean:

- multi-table ingest
- backfill / derived table generation
- branch-wide schema-compatible maintenance job
- future hook/materialization workflow
- agent-authored batch graph rewrite

It does **not** need to mean "every small mutation."

Normal interactive writes can still target a branch directly.
Transactional runs are for coarse-grained, reviewable, multi-table operations.

---

## Proposed Model

### 1. A Run Has Pinned Inputs

Every run should record:

- `run_id`
- `target_branch`
- `base_snapshot_id`
- `base_manifest_version`
- `code_ref` or `operation_hash`
- `status`

Recommended statuses:

- `running`
- `published`
- `failed`
- `aborted`

This gives Omnigraph a reproducible unit of work:

```text
run = code + pinned input graph state + resulting published state or failure
```

### 2. Each Run Gets An Internal Transactional Ref

If the user asks to run a job against branch `main`, Omnigraph should internally create something like:

```text
main
  -> refs/runs/run_123
```

That ref starts from `main`'s current head and receives all intermediate writes.

Important:

- this ref is not a normal user branch
- it is owned by the run
- it should not be the default source for new branches or merges

### 3. Publish Is A Promotion Step

A run becomes visible to normal readers only if it passes validation and is promoted back to the target branch.

Conceptually:

```text
begin run from branch head
write candidate graph state
validate candidate graph state
if valid:
  promote target branch to run head
else:
  leave target branch unchanged
```

This matches the existing Omnigraph direction:

- one graph coordinator
- one publish authority
- branch heads as visible graph state

---

## Visibility Rules

This is the most important part of the note.

The paper's strongest lesson is not "use branches."
It is:

> transactional refs need stricter visibility rules than user branches.

Without guardrails, a failed transactional branch can leak back into visible history if other actors branch from it or merge it later.

### Omnigraph Rule

Internal run refs must not be treated as ordinary branches.

Defaults:

- hidden from normal `branch list`
- not mergeable through normal branch APIs
- not valid as `branch create --from ...` sources
- not valid as general-purpose collaboration refs

Allowed operations:

- inspect for debugging
- resolve pinned snapshots for diagnostics
- explicit admin promotion or adoption, if Omnigraph adds that later

### Why

Bad flow:

```text
main
  -> run ref created
  -> run fails after writing table A
  -> another actor branches from failed run ref
  -> later merge lands that state back into main
```

That violates the original intent of transactional publication.

So Omnigraph should distinguish:

- **user collaboration refs**
- **internal transactional refs**

even if both are implemented using the same underlying Lance branching primitives.

---

## Validation Layers

The paper's other useful pattern is layered fail-fast validation.

Applied to Omnigraph:

### 1. Author-Time

Catch what can be caught before execution:

- schema parse/type errors
- query and mutation type errors
- invalid run configuration

### 2. Coordinator Preflight

Before any job begins:

- resolve target branch head
- pin input snapshot
- verify target branch is writable
- verify run operation is schema-compatible
- allocate transactional ref

### 3. Worker / Write-Time

Before publish:

- value constraints
- endpoint validity
- edge cardinality
- uniqueness checks that are active in runtime
- any job-specific verifiers

### 4. Publish-Time

Only the coordinator may:

- mark the run successful
- advance the visible branch head
- attach final metadata

This is consistent with the current refactor direction:

- stage
- validate
- publish

---

## Suggested API Shape

This is a future control-plane API, not a required immediate runtime API.

```rust
let run = db.begin_run("main", operation).await?;

db.execute_run(&run).await?;

let report = db.validate_run(&run).await?;

if report.ok {
    let snapshot_id = db.publish_run(&run).await?;
} else {
    db.abort_run(&run).await?;
}
```

Or, more realistically for Omnigraph:

```rust
let run = coordinator.begin_run("main", op_spec).await?;
let candidate = mutation_executor.execute_on_run(&run, op_spec).await?;
coordinator.validate_run(&run, &candidate).await?;
let published = coordinator.publish_run(&run).await?;
```

The public user-facing API can still stay simple:

```rust
db.run_job("main", job).await?;
```

But internally it should still go through:

- pinned input
- isolated write ref
- validation
- single publish step

---

## Omnigraph-Specific Adaptation

The paper is pipeline- and table-oriented.
Omnigraph needs graph-specific versions of the same idea.

### Run Validation Must Be Graph-Aware

Validation is not just "does each table have a valid schema?"

It also includes:

- no orphan edges
- endpoint type correctness
- graph-level uniqueness expectations
- branch/merge invariants
- graph-index rebuild or invalidation rules where needed

### Publish Unit Is A Graph Snapshot, Not A Single Table Snapshot

The visible outcome of a successful run should be:

- one new graph commit / snapshot id
- one branch-head advancement
- one coherent mapping from table keys to table versions

### Reproducibility Should Include Code Identity

For future agentic or batch jobs, Omnigraph should store:

- source query / mutation / job spec hash
- schema hash
- input snapshot id
- published snapshot id

That gives a reproducible audit trail for:

- "what code ran?"
- "against what graph state?"
- "what graph state did it publish?"

---

## Non-Goals

This note does **not** propose:

- making every single mutation a transactional run
- replacing normal user branches with run refs
- exposing internal run refs as general collaboration branches
- solving federation here
- solving high-contention same-branch concurrency here

This is a control-plane design for coarse-grained graph jobs.

---

## Recommended Defaults For Omnigraph

If Omnigraph adopts this model, the safest defaults are:

1. run refs are internal by default
2. failed run refs are inspectable but not mergeable
3. only the graph coordinator may publish a run
4. branch heads point only to published graph commits
5. run metadata always stores pinned input snapshot identity
6. future admin adoption of failed runs must be explicit and exceptional

---

## Suggested Implementation Order

This should come after the current CLI and architecture cleanup, not before it.

### Phase 1

Add metadata only:

- `RunId`
- run records
- `base_snapshot_id`
- `status`
- `operation_hash`

No new publish semantics yet.

### Phase 2

Introduce internal run refs:

- hidden from normal branch APIs
- begin / abort / inspect flows

### Phase 3

Move coarse-grained multi-table jobs to:

- write on internal run ref
- validate candidate graph state
- publish via coordinator promotion only on success

### Phase 4

Add reproducibility / review affordances:

- inspect failed runs
- diff run result against target branch
- optional human approval / PR-like workflow later

---

## Bottom Line

The best reusable pattern from the paper is:

> treat multi-table work as isolated candidate graph state, and make visibility a separate publish decision.

For Omnigraph, that means:

- internal transactional refs
- graph-aware validation before publication
- strict separation between failed candidate state and visible branch history

That is a strong fit for:

- server-first operation
- agent-authored changes
- future review workflows
- keeping the graph coordinator as the single source of visible graph state
