# Change Detection / Diff

`changes/mod.rs`. Three-level algorithm:

1. **Manifest diff**: skip sub-tables whose `(table_version, table_branch)` is unchanged.
2. **Lineage check**:
   - Same branch lineage → fast path: use the per-row `_row_last_updated_at_version` column to classify Insert/Update/Delete.
   - Different lineages → ID-based streaming comparison.
3. **Row-level diff**: streaming, no full materialization.

## Public API

- `diff_between(from: ReadTarget, to: ReadTarget, filter: Option<ChangeFilter>) -> ChangeSet`
- `diff_commits(from_commit_id, to_commit_id, filter)` — cross-branch safe.

## Types

```
ChangeOp: Insert | Update | Delete
EntityKind: Node | Edge
EntityChange { table_key, kind, type_name, id, op, manifest_version, endpoints?: {src, dst} }
ChangeFilter { kinds?, type_names?, ops? }
ChangeSet { from_version, to_version, branch?, changes[], stats }
```
