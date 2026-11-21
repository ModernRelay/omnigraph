- Every full-text field has an analyzer profile, `english_v1` when its index names none, and `match_terms` and `bm25` answer by an exact scan of the rows: the same rows whatever the index's state, an edit budget on every field, and `bm25` scores as `F64` over the field corpus. `full_text_index_required` is gone, and a raw `contains_tokens` scan filter refuses on any full-text field. See [analyzers][gq-3-full-text-guide].

[gq-3-full-text-guide]: ../docs/user/search/analyzers.md#exact-matching-and-scoring
