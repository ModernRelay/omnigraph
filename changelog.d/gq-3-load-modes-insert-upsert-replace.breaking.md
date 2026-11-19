- Load modes are `insert`, `upsert`, `replace` and `overwrite` in `omnigraph load
  --mode`, `load csv … mode` and the HTTP load route; `append` and `merge` are
  refused with `insert` and `replace`, which mean what they meant, as the fixes.
  See [load modes][gq-3-load-modes-guide].

[gq-3-load-modes-guide]: ../docs/user/cli/reference.md#load-modes
