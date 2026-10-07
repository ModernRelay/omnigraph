- A named traversal whose hop bound allows more than one hop is refused with
  `T5` when its edge connects two different node types, as in
  `$p worksAt{1,2} $c` for `WorksAt: Person -> Company`. No path over such an
  edge continues past its first hop, and the query used to run as `{1,1}`
  without saying so. Omit the bound, or write `{1,1}`, which stays valid. A
  stored query with such a bound now fails type checking, which
  `omnigraph queries validate` reports. See
  [traversal and match patterns][cross-type-hop-bound-refused-traversal].

[cross-type-hop-bound-refused-traversal]: ../docs/user/queries/traversal.md
