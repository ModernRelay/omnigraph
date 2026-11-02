- A branch merge reads only the table fragments that changed. A fast forward,
  or any table only the source changed, no longer walks both versions of the
  table in full, and a three-way merge reads its candidate rows only from the
  fragments that can hold them. Before, a merge's storage requests grew with
  each touched table's fragment count (about four per fragment for a fast
  forward); now they stay flat as uncompacted one-row commits accumulate. See
  [merge classification][merge-lineage].

[merge-lineage]: ../docs/user/branching/merge.md#merge-classification-mode
