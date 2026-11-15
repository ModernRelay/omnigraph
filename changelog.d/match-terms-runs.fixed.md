- `match_terms($d.f, terms($q, mode: all | any))` runs wherever a full-text call does instead of failing; an edit budget needs a declared analyzer (`T86`). See the [search guide][match-terms-guide].

[match-terms-guide]: ../docs/user/search/index.md
