- A branch's `__manifest` now keeps a byte-bounded buffer of its latest
  commits and releases older ones into immutable Lance files under
  `__history`, so a publish no longer rewrites the branch's whole history.
  Graph commit ids take the form `hb1.<block>.<slot>.<nonce>`. This is
  storage format 14: a standalone format 13 graph is converted by the offline
  `omnigraph upgrade`, and a graph at any older format is rebuilt by export,
  `init` and `load --mode overwrite`, as described in the
  [upgrade guide][history-release-upgrade]. A session may lower the release
  budget with `set history_release_bytes = <bytes>;` (`1024..=262144`, default
  `262144`); the default is production's and cannot be raised.

[history-release-upgrade]: ../docs/user/operations/upgrade.md
