# Merging (three-way) and Conflicts

`exec/merge.rs`.

## Strategy

Ordered, row-by-row cursor merge:

- `OrderedTableCursor` scans each table sorted by `id` and supports peek/pop matching.
- `StagedTableWriter` buffers `MERGE_STAGE_BATCH_ROWS = 8192` rows into a temp Lance dataset (`OMNIGRAPH_MERGE_STAGING_DIR`).
- The merge runs per sub-table; results are published as one atomic manifest update.

## Outcome enum

`MergeOutcome { AlreadyUpToDate | FastForward | Merged }`

## Conflict types (`error.rs`)

```
MergeConflictKind:
  DivergentInsert        // same id inserted on both branches
  DivergentUpdate        // updated differently on both branches
  DeleteVsUpdate         // one side deletes, other updates
  OrphanEdge             // edge references a node deleted by the other side
  UniqueViolation
  CardinalityViolation
  ValueConstraintViolation
```

Returned as `OmniError::MergeConflicts(Vec<MergeConflict { table_key, row_id?, kind, message }>)`. The HTTP server surfaces this as a 409 with structured `merge_conflicts[]` (top 3 + "+N more").
