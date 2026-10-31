- **A non-zero `@card` minimum now counts every source node a write inserts or updates**, and schema apply refuses a new such edge type from an existing node type (`OG-MF-107`). Writers that load nodes before their edges must send each node with its edges in one write; an orphan query finds nodes already below the minimum. See [edge cardinality][card-minimum-schema].

[card-minimum-schema]: ../docs/user/schema/index.md#edge-cardinality
