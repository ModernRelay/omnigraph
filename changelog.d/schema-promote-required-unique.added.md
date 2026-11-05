- `omnigraph schema apply` can make a nullable property required and add `@unique` to a node type that already has rows ([#721][721]). The apply reads the stored rows under its locks and refuses, changing nothing, when a row holds a null (deleted rows included) or two rows share a key. A successful apply keeps entity ids, relationships and retained commits, rewrites no data file, and makes the storage column non-nullable. The `@unique` check holds at most 5,000,000 distinct keys.

[721]: https://github.com/ModernRelay/omnigraph/issues/721
