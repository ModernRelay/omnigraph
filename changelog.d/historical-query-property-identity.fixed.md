- A historical query no longer reads a dropped property's values through a
  property later re-added with the same name. At a commit that stores a property
  under another lifetime or another name, or not at all, the query is refused
  with a request error naming the property, where before it returned the dropped
  values or failed with an internal error. Commits that read table data written
  before v0.10, which records no property identity, are refused the same way
  unless that data is still current. See [historical reads][historical-query-property-identity-reads].

[historical-query-property-identity-reads]: ../docs/user/branching/index.md#historical-reads
