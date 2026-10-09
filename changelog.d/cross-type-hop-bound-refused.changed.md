- A named traversal whose hop bound allows more than one hop is refused with
  `T5` when its edge connects two different node types, as in
  `$p worksAt{1,2} $c` for `WorksAt: Person -> Company`. No path over such an
  edge continues past its first hop. Before, such a range was capped at one
  hop without a word: `{1,n}` returned the one-hop rows, and a range starting
  above one, such as `{2,2}`, returned none. For `{1,n}`, omit the bound or
  write `{1,1}` to keep the same rows. A range starting above one never
  matched anything, and dropping its bound would return the one-hop rows
  instead, so check which path the query meant. A stored query with such a
  bound now fails type checking, which `omnigraph queries validate` reports.
  See [traversal and match patterns][cross-type-hop-bound-refused-traversal].

[cross-type-hop-bound-refused-traversal]: ../docs/user/queries/traversal.md
