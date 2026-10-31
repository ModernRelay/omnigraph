- **A non-zero `@card` minimum now counts every source node a write inserts or
  updates.** A mutation, load or merge that writes a node of an edge type's
  source type without enough edges of that type is refused with
  `@card violation on edge <Edge>: source '<id>' has <n> edges (min <m>)`;
  earlier releases counted only the sources whose edges the write added, moved
  or removed, so such a node published below the minimum. An `overwrite` load
  of an edge table now also recounts the sources of the edges it removes.
  Affected: graphs with an edge type such as `@card(1..)` whose writers load
  nodes before their edges. Write each such node with its edges in one mutation
  query or one JSONL load; a `load csv` into the node type can no longer add a
  node. Schema apply refuses a new edge type with a non-zero minimum whose
  source node type already exists (`OG-MF-107`), since its existing nodes
  have none of the new edges. A graph may already hold nodes below the
  minimum: each is refused the next time a write inserts or updates it, and
  a rebuild by export and load refuses it, until its edges are added; an
  orphan query (`not { $p worksAt $c }`) lists them.
  `explain` of a node `load csv` names the new `source_cardinality` check. See
  [edge cardinality][card-minimum-schema].

[card-minimum-schema]: ../docs/user/schema/index.md#edge-cardinality
