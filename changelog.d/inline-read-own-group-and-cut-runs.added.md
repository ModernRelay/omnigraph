- An inline read in a `from` keeps its own aggregate, `distinct`, `order` and `limit`, run per outer row, so an outer `where` never widens its cut. See [inline reads][inline-read-guide].

[inline-read-guide]: ../docs/user/queries/traversal.md#values-and-inline-reads
