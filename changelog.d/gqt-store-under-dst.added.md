- `omnigraph-gqt --store <file:// root>` runs an `omnigraph-engine-dst` environment on a prepared graph: reads fall through to the root, writes stay in memory, the root is never written ([GQT runner guide][gqt-store-under-dst]).

[gqt-store-under-dst]: ../crates/omnigraph-gqt/README.md#explicit-execution
