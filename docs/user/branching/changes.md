# Change Detection / Diff

Diffing two read targets uses a three-level algorithm:

1. **Manifest diff**: skip sub-tables whose `(table_version, table_branch)` is unchanged.
2. **Lineage check**:
   - Same branch lineage → fast path: use the per-row `_row_last_updated_at_version` column to classify Insert/Update/Delete.
   - Different lineages → ID-based streaming comparison.
3. **Row-level diff**: streaming, no full materialization.

## Public API

- `diff_between(from: ReadTarget, to: ReadTarget, filter: Option<ChangeFilter>) -> ChangeSet`
- `diff_commits(from_commit_id, to_commit_id, filter)` — cross-branch safe.
- `commit_changes_page(commit_id, cursor, limit, max_bytes)` — exact first-parent change images with bounded pagination.

## Types

```
ChangeOp: Insert | Update | Delete
EntityKind: Node | Edge
EntityChange { change_index, table_key, kind, type_name, id, op, manifest_version, endpoints?, before?, after? }
ChangeFilter { kinds?, type_names?, ops? }
ChangeSet { from_version, to_version, branch?, changes[], stats }
```

The commit-change wire shape differs from the internal `EntityChange` in one
deliberate way: each page states its cause once, on the block (`commit` with
id, parent, branch, `actor_id`, `manifest_version`, `created_at`), and the
per-entity records carry no `manifest_version`. `created_at` is authorship
time, minted before table effects and stable across retries and recovery; it
is not a publication-time witness. A parentless commit — only the empty
genesis a fresh graph mints at init — always yields an empty page.

## Ordering

`diff_between` retains its existing table-grouped ordering. The commit-change feed has a
stronger ABI: `table_key`, `stable_table_id`, `table_incarnation_id`, entity `id`, then operation
rank (`insert`, `update`, `delete`). `change_index` is zero-based across every
page of one commit; resume only with the opaque `next_cursor`. Pages default to
1000 rows and 4 MiB and reject requests above 8192 rows or 32 MiB. HTTP 410
with `change_feed_gap` means retained table history can no longer reconstruct the
requested continuation; restart from a newer application checkpoint. A cursor
that fails decoding or names a different graph, commit, or cursor version is a
typed 400 rejection (`change cursor rejected: …`), never a silent skip.

The current exact-image implementation streams an ordered merge of each changed
table lifetime. This bounds retained page memory but can scan the full
changed table; replace it with substrate candidate pruning only when Lance exposes
a stable bounded ordered change stream.
