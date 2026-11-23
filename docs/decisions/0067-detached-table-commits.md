# Detached table commits

**Decides:** every graph-content table effect is a Lance detached commit staged from its manifest pin, and only the one `__manifest` publication that names it makes it visible.
**Contract:** keeps E2, E4, E5
**Code:** `crates/omnigraph-core/src/metadata.rs`, `crates/omnigraph-core/src/staging.rs`, `crates/omnigraph-core/src/instrumentation.rs` (`open_pinned_dataset`), `crates/omnigraph/src/table_store.rs`, `crates/omnigraph/src/db/legacy_sidecars.rs`, `crates/omnigraph/tests/detached_commit_matrix.rs`, `crates/omnigraph/tests/lance_surface_guards.rs`
**Cited as:** RFC 0067

## Context

A linear Lance commit is visible at its table's HEAD before the graph
publishes it. Lance's commit always runs its conflict pass and rebases onto
HEAD, so an unpublished linear effect is not inert: a later write prepared on
the pin lands on top of it and carries its fragments. Undoing it needs a
durable record that the effect is ours, a classifier, `Restore` compensation
and a barrier before every write, and one stuck record blocks every writer on
the graph. A detached commit (`_versions/d{id}.manifest`) is durable, moves no
HEAD, and nothing rebases onto it. `__manifest` already records lineage, which
is the use Lance's detached commit was made for.

## Decision

- A mutation, a load, a merge chunk, an index build, a schema rewrite, the
  system-column upgrade and a compaction each commit a detached version from
  the pinned base, or from the previous link of their own chain. No writer
  advances a graph table's linear HEAD. Only a new table is created linearly,
  at version 1, because Lance cannot create a dataset detached.
- A registration row's pin is three values: `published_dataset_version`,
  `omnigraph.staged_version` and `omnigraph.transaction_uuid`, carried in the
  row's version metadata map. One `__manifest` publication carries every pin of
  the graph commit.
- A reader opens the staged version. The transaction file name its manifest
  records is the identity witness, so the check costs no extra request.
- An attempt that fails before publication returns its original typed error,
  leaves the graph head and every pin unchanged, and leaves only unreferenced
  detached manifests. There is no recovery sidecar, classifier, write barrier,
  rollback or `Restore`, and no writer compares linear HEAD with the pin.
- Optimize stages one detached `Rewrite` whose fragment ids start above the
  base's maximum and publishes with an exact compare-and-swap on the pins it
  planned from. A moved pin discards the compaction.
- A publication whose outcome cannot be confirmed is an in-doubt error naming
  its `graph_commit_id`. The engine settles it from the branch's history when
  it can and never retries a non-idempotent write on its own.
- A read-write open refuses a root that still holds a sidecar under
  `__recovery/` from an older build, and so does `upgrade`.
- A pin is never promoted onto linear history; reclamation follows
  `__manifest` ([Detached-only tables](2026-09-21-detached-only-tables.md)).

## Rejected

- **Linear commits gated on HEAD equal to the pin:** keeps the sidecar, the
  classifier, the barrier and `Restore`; upstream closed the non-rebasing
  linear commit, and its replacements exclude detached commits.
- **Transaction-bearing sidecar:** replays instead of restoring, but keeps the
  HEAD check, the classifier, the barrier and `Restore` for abandonment.
- **Staged manifests with finalize:** Lance writes staged manifests only inside
  its external-store handler, so every operation would need a custom
  `CommitHandler`.
- **Private native branches per merge:** covers merge only, and keeps native
  ref create and delete, which are not compare-and-swap.
- **One Lance dataset per graph:** rewrites the authority model, needs a
  format reached only by export and rebuild, and has no scaling evidence for
  wide schemas or per-type compaction.
