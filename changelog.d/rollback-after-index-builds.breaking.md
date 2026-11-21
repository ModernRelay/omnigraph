- Once this release builds or rebuilds an index, an earlier binary cannot roll back in place: it refuses full-text search on that index and rebuilds a lagging BITMAP, LABEL_LIST or NGRAM index as a BTREE or fails. Keep the whole-root backup taken before the upgrade and restore it to roll back. See the [upgrade guide][rollback-after-index-builds-guide].

[rollback-after-index-builds-guide]: ../docs/user/operations/upgrade.md#lance-substrate-upgrades-v010-v014
