# Full-text analyzers

A full-text index's analysis comes from the schema, never from a query.
Declare it on a node's String property with `@index(fulltext, ...)`:

```pg
node Doc {
  slug: String @key
  title: String @index(fulltext, analyzer = "english_v1")
  body: String @index(fulltext, analyzer = "standard_v1", scorer = "bm25_v1")
  note: String @index(fulltext, analyzer = "standard_folded_v1", scorer = "none")
}
```

`analyzer` names one of three profiles. A profile never changes once released;
a different pipeline is a new profile. Every setting is stated:

| Setting | `standard_v1` | `standard_folded_v1` | `english_v1` |
|---|---|---|---|
| Tokenizer | split on every character that is not a letter or digit | same | same |
| Lowercase | yes | yes | yes |
| Stemming and stop words | neither | neither | English stemmer, then English stop words |
| ASCII folding | no | yes | yes, after stemming |
| Token-length filter | none | none | none |
| Unicode normalization | NFC before tokenization (see below) | same | same |

So `"run"` matches `"Runners were RUNNING"` under `english_v1` only, `"cafe"`
matches `"Café"` under `standard_folded_v1` and `english_v1`, and every
profile matches `"RUNNING"` in either case.

`scorer` is `bm25_v1`, the default, or `none`. A `none` field matches
(`search`, `match_text`, `fuzzy`) but does not rank: `bm25`
over it, alone or as an `rrf` arm, is refused at type checking (`T87`). An
unknown profile, scorer or option is a schema error that lists the admitted
names. No query names an analyzer: `terms(…)` admits `mode` and `max_edits`
only (`T71`). This release runs neither `match_terms` nor a `bm25` over
`terms(…)` with `mode: all` or an edit budget, and refuses both at type check
(`T86`): `bm25` takes a String, which matches any term with no edits.

`@index(fulltext)` without `analyzer`, and a bare `@index`, keep the analysis
earlier releases used: Lance's default, which lowercases, stems, removes
English stop words, folds accents and drops tokens longer than 40 bytes. No
existing graph changes rows. `omnigraph lint` reports each such index
(`L202`) and names the profile to declare, `english_v1` being the closest: it
differs by the 40-byte filter and NFC.

Declaring, changing or removing an analyzer is a schema change applied in
place (`schema plan` shows `change full-text analysis`). The apply replaces
the column's index with one built for the new analysis, whose compatibility
certificate records the field's analyzer fingerprint; a certificate made for
the old analysis does not establish the new one. Until `omnigraph index
build` (or `optimize`, on `main`) builds the postings again, the field is matched by
scanning with the new analysis. Renaming the property keeps the analysis and
its index; dropping and re-adding it declares a new one. The fingerprint
covers the profile and the scorer, so changing the scorer of a field that
declares a profile rebuilds its index too; a scorer change on a field without
a profile changes no index. Searching a snapshot from before the change
reads that snapshot's index, which no longer matches the field's declared
analysis, and is refused with `full_text_index_rebuild_required`.

The full-text index applies the profile's tokenizer and filters but no
Unicode normalization: Lance has no NFC step, and how NFC reaches the index
is not settled yet. Text that is already NFC, which most text is, matches as
the profile states; a value or query written in decomposed form (an `e`
followed by a combining accent) is tokenized as written, and the combining
mark splits the word.
