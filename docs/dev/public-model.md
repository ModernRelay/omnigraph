## Public Model

This note records a public API dilemma that appeared during the graph-coordinator refactor.

### The Problem

Omnigraph had two different public read models at the same time:

- explicit-target reads such as `query(ReadTarget::Branch("main"), ...)`
- handle-local state APIs such as `snapshot()`, `version()`, and `refresh()`

That was confusing because those calls did not mean the same thing.

Example:

```rust
let qr = db.query(ReadTarget::branch("main"), src, "friends_of", &params).await?;
let snap = db.snapshot();
```

Both look like "read main", but they are different:

- `query(ReadTarget::branch("main"), ...)` should mean "read the latest visible head of main"
- `snapshot()` meant "read whatever manifest state this handle currently has pinned"

That split leaked stale-handle behavior into the public model and made it easy to misread the API.

### Option 1

Keep `snapshot()`, `version()`, and `refresh()` public as advanced APIs.

This preserves a low-level escape hatch for:

- stale-snapshot inspection
- manual refresh flows
- write-retry patterns after version drift

But it leaves two public mental models:

- explicit target reads
- handle-local pinned state reads

### Option 2

Move the public model fully to explicit targets.

That means:

- reads use explicit `ReadTarget`
- public snapshot access becomes `snapshot_of(target)`
- public version access becomes `version_of(target)`
- stale-write retry becomes an explicit branch sync operation instead of generic handle refresh

This keeps one public rule:

> If you want to read graph state, name the target explicitly.

The handle may still keep internal coordinator state for optimistic write validation, but that is not the public read model.

### Decision

Choose Option 2.

Public API direction:

- `query(ReadTarget, ...)`
- `diff_between(ReadTarget, ReadTarget, ...)`
- `entity_at_target(ReadTarget, ...)`
- `snapshot_of(ReadTarget)`
- `version_of(ReadTarget)`
- `resolve_snapshot(branch)`
- `sync_branch(branch)` for stale-write recovery

Demoted from the public model:

- `snapshot()`
- `version()`
- `refresh()`
- `active_branch()`

### Intended Semantics

- explicit reads resolve the latest branch head at call time
- pinned snapshots remain immutable once acquired
- stale writes still detect version drift
- retrying a stale write requires an explicit `sync_branch(branch)` step

### Why This Is Better

- one public read model instead of two
- better alignment with server-first explicit-target design
- fewer stale-state footguns
- cleaner SDK and HTTP mapping

The tradeoff is that some low-level workflows become more explicit, but that is preferable to keeping ambiguous semantics in the public surface.
