- A mutation may name the namespace it targets: `insert node:Person { … }` and
  `insert edge:Knows { … }`, the spelling `load csv` already uses. A node type
  and an edge type may share a name, so a bare name both namespaces declare is
  refused (`T60`) instead of silently selecting the node type. Graphs whose
  schema declares a node type and an edge type with the same name must spell
  the namespace on their mutations. See [Mutation targets][mutation-targets].

[mutation-targets]: ../docs/user/mutations/index.md#statements
