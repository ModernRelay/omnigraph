- A `bm25` reads and analyzes its field's text once, scoring every match in that pass; scores and rows are unchanged, and its memory follows its matches. See [the exact scan's cost][bm25-single-pass].

[bm25-single-pass]: ../docs/user/search/analyzers.md#the-exact-scans-cost
