- `search`, `match_text` and `fuzzy` are refused at type check (`T6`) with the `match_terms` call each means as the fix; `match_terms` is a Boolean operand that runs under `not`, `or` and in blocks. Run `omnigraph queries validate` on stored queries before upgrading. See the [search guide][gq-3-match-terms-guide].

[gq-3-match-terms-guide]: ../docs/user/search/index.md#full-text-search
