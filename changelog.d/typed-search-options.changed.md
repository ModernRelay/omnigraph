- Search arguments execute in their declared types, including explicit casts.
  Supplied `fuzzy` edit limits and `rrf` rank constants now refuse null and
  values outside their U32 API range; `rrf` also requires a positive value.
  Defaults apply only when the optional argument is absent. See the
  [search argument ranges][typed-search-argument-ranges].

[typed-search-argument-ranges]: ../docs/user/search/index.md#functions
