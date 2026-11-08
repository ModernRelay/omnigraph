- `omnigraph-gqt --store` runs an `omnigraph-engine-dst` environment on a prepared graph of any size: reads fall through to the root, writes stay in memory, the root is never written ([guide][gqt-store-read-through]).

[gqt-store-read-through]: ../crates/omnigraph-gqt/README.md#explicit-execution
