# Lance 10 full-text compatibility fixture

`lance10-fts.json` is an exact saved Lance 10.0.0 dataset, represented as a map
of relative filenames to base64 bytes so review and extraction need no archive
dependency. Tests extract it only into a fresh temporary directory.

It contains three non-null UTF-8 `id`, `text` pairs: `a, organism`,
`b, university`, and `c, running`. It was built on 2026-08-31 with Lance
10.0.0, Arrow 58, stable row IDs, explicit V2_2, then indexed using
`create_index_builder(&["text"], IndexType::Inverted,
&InvertedIndexParams::default())`. Version 1 contains rows; version 2 adds the
index. No OmniGraph compatibility certificate is present.

Lance 10 returns one hit for each exact term. Stock Lance 11 returns zero,
zero, and one respectively against the same saved postings. The owning
`table_store::staged_tests` test proves the refusal gate and rebuild from these
actual old bytes, rather than generating an allegedly old index with the new
library. Keep the saved bytes when changing the reader dependency.
