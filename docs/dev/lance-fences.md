# Lance compatibility fences

**Audience:** engine and storage contributors changing a Lance-shaped behavior
**Authority:** Omnigraph's current deltas over stock Lance 13.0.0; the required
upstream reading is in [lance.md](lance.md)

Each fence names a Lance behavior that Omnigraph depends on, works around or
forbids, and the tests that pin it. A fence that ends with a "Drop … at the
bump" sentence is a workaround: remove it, and replace its negative guard,
when the named upstream change ships. This page holds current fences only.
Rationale and bump evidence belong in the owning RFC or release note.

## Format, keys and row identity

### File format

Every production write explicitly selects stable V2_2. Experimental V2_3,
spilled row lineage and stable-row-ID fragment-reuse indexes are not enabled as
graph capabilities.

**Owners:** `integration/lifecycle.rs`, write-site source guards

### Graph keys

Schema-v9 node/edge tables carry the exact non-null `__id` unenforced primary
key (edges also `__src`/`__dst`); v8 tables keep the `id`/`src`/`dst` spellings
introduced in v6, and the explicit system-column upgrade (RFC 0040) renames them
with one rename-only `Operation::Project` commit per table
(`TableStore::renamed_schema`), staged detached and published as a pin (RFC
0067, detached-only), which must keep every fragment, field id, the primary-key
marker and index attachments. Strict insert/upsert uses the sealed
filter-bearing adapter; raw keyed Append is forbidden.

**Owners:** `lance_surface_guards.rs`, staged-table tests, `forbidden_apis.rs`,
`system_column_upgrade.rs`

### Stable row IDs

Graph tables use stable row IDs; delete/update/index maintenance must retain
their mapping. Overwrite allocates fresh IDs and restore retains the allocation
high-water marks. Staged-view IDs are provisional, not committed identity.

**Owners:** `lance_surface_guards.rs`, staged-table tests, `writes.rs`

### Row version stamps

Every graph-table write keeps a fragment's `_rowid` and
`_row_last_updated_at_version` sequences inline in the manifest
(`RowIdMeta::Inline`, `RowDatasetVersionMeta::Inline`; Lance 13 writes them to a
file only under its spill configuration, which no graph write sets), one entry
per physical row, the deletion vector masking the deleted ones. A merge_insert
update keeps the row's `_rowid` and stamps it with the read manifest's version
plus one, assigned before the detached commit receives its random version, so
two sibling first commits over one parent stamp a row they both update with the
same value; a deletion-only commit restamps nothing; compaction carries every
live row's id and stamp into the rewritten fragments. Change discovery across a
window with no adjacent transaction
(`changes::candidate_scan::row_identity_plan`, the `change_lineage` setting)
pairs the live rows of two pinned versions by `_rowid` and reads only the pairs
whose stamps differ, and only where the `from` side is on the `to` side's
first-parent chain with no merge commit after it, since a merge can take the
merged branch's pin, whose history never read the target's; a fragment whose
sequences are absent, spilled or short takes the exact merge. Drop the ancestry
restriction at the bump whose stamp is the commit's own version.

**Owners:**
`lance_surface_guards.rs::row_identity_discovery_stamps_row_ids_and_rewrites_lance_13`,
`integration/changes_cost.rs`
(`graph_diff_over_several_commits_reads_data_flat_in_table_extent`,
`graph_diff_across_a_compaction_reads_data_flat_in_table_extent`,
`graph_diff_of_diverged_heads_takes_the_exact_merge`),
`cases/issue_969_graph_diff_wider_windows.gqt`,
`cases/graph_diff_across_a_fast_forward.gqt`,
`cases/branch_review_across_a_fast_forward_on_the_source.gqt`,
`integration/changes.rs::commit_changes_feed_and_diff_across_a_fast_forward_report_the_source_rows`

## Search and filters

### KNN result order

Lance 13 V2 row-stream payload hydration preserves ordering metadata, which
`WorkMemory` uses to merge parallel results in rank order. Supported graph
writers, including the supported upgrade sources, select V2_2. Lance's top-k
keeps whichever rows it meets first at the `k`-th distance, so a tie at a
window's last place would follow fragment order: a `nearest` scan asks Lance for
`k + 1` rows, doubles that fetch while the last row it returned ties the `k`-th,
and cuts to `k` by distance then identity itself (`engine/scan.rs`,
`window_by_identity`); an unreached `+inf` row counts only within the first `k`
places.

**Owners:** `lance_surface_guards.rs::flat_knn_v2_late_payload_preserves_order`,
`search.rs::nearest_large_k_preserves_global_order_through_payload_hydration`,
omnigraph-gqt
`layout_hunts::nearest_window_boundary_tie_is_by_identity_under_every_layout`

### KNN probe budget

A `nearest` scan sets `maximum_nprobes` per index delta and widens it from
Lance's execution summary (`partitions_searched` / `partitions_ranked`, read
through `scan_stats_callback`); a prefilter admitting fewer rows than `k` makes
Lance emit the unreached rows at `_distance = +inf`, which the engine resolves
with a flat exact rescan (`use_index(false)`); `count_rows(None)` is the live
row count (deletions excluded), read for the ladder's exhaustion stop and as the
overfetch loop's exact-pass `k`. A renamed counter or marker turns the ladder
fail-closed. The factor and both fallbacks are the plan's declared
`NearestPolicy`. Every `nearest` scan pins the field's declared distance as its
metric (`ScanTuning::nearest` sets `distance_metric`: `l2`, `cosine` or `dot`),
and it uses a vector index only when every vector index on the column ranks
exactly by that distance: IVF_FLAT with that metric, as the engine builds it
(`table_store::vector_indexes_rank_exactly`). Any other index on disk
(quantized, HNSW, built for another distance, details with fields Lance 13 does
not know or no details) makes the scan flat; index facts report an IVF_FLAT
index of any of the three metrics as a vector index and every other one
`Unknown`; an index whose statistics name no partition count fails the fold,
except one whose segments cover no fragment (a column patch withdrew them all,
and Lance reports no statistics for such a segment), which holds no rows and is
rebuilt with the engine's one partition (`TableStore::vector_partition_count`).
The same counters decide whether a read reports its `nearest` search exhaustive:
an attempt that searched every partition of such an IVF_FLAT index, or that
carries neither counter (Lance's flat kNN), ranked exactly; one counter alone
does not.

**Owners:** `lance_surface_guards.rs`, `search.rs`
(`nearest_ranks_by_l2_over_a_cosine_index_issue_1297`,
`nearest_stays_exact_over_a_quantized_index_issue_1297`),
`integration/maintenance.rs::index_build_and_optimize_rebuild_a_vector_index_a_column_patch_left_empty`,
`omnigraph-dst` storage-cost rows `VectorIndexRefreshEmptied` beside
`VectorIndexRefreshLagging` (`scenarios.rs::vector_index_recovery_cost_rows`)

### Vector distance

A field's declared distance (`Vector(n, distance = ...)`, absent meaning `l2`)
is the metric of every index the engine builds on it (`IndexBuildSpec::Vector`,
`table_store::metric_type`) and of every kNN over it. Lance 13's metrics are the
RFC's values: `l2` the sum of squared differences, `cosine`
`1 - dot / (norm · norm)`, `dot` `1 - dot` (`lance_linalg::distance`). A cosine
IVF_FLAT index stores normalized vectors and normalizes the query, so its
`_distance` is the cosine distance up to `F32` rounding, not the flat path's
value; every `cosine` search therefore sets `refine(1)` (`ScanTuning::nearest`),
which re-scores the candidates on the stored vectors with the flat kernel, so a
row's distance does not depend on index coverage
(`lance_surface_guards.rs::refined_cosine_index_search_equals_the_flat_distance_bit_for_bit`);
it leaves a zero vector out (`KeepFiniteVectors` after normalizing), and the
flat kNN drops the zero vector's NaN distance, so the index and the scan agree
on the rows a stored zero vector drops. An explicit metric other than the
index's makes Lance scan flat (`Scanner::create_plan`), so an index built for
another distance is never used. `has_vector_index_for` counts only an index
built for the declared distance, so a changed distance makes the reconciler
build a replacement, which removes every vector index on the column; the fold
keeps an index's own metric and skips a column a replacement builds. The exact
`distance` operator scores through `DistanceType::arrow_batch_func`, the kernel
Lance's flat kNN uses (`lance-index` `vector/flat.rs`), so `distance` and an
exhaustive `nearest` give one `F32` value per row.

**Owners:**
`lance_surface_guards.rs::distance_kernel_equals_the_flat_knn_distance_bit_for_bit`,
`lance_surface_guards.rs::ivf_flat_and_flat_knn_score_cosine_and_dot_exactly`,
`schema_apply.rs::vector_distance_change_is_applied_in_place_and_serves_exactly_until_the_rebuild`,
`staged_tests.rs::stage_create_indices_batches_mixed_types_into_one_exact_commit`,
`cases/vector_distance_*.gqt`

### Full-text analyzer

Every selected FTS segment needs artifact-scoped compatibility proof. Every
lexical field (a free-text String with a one-column `@key`, `@index` or
`@index(fulltext, ...)`, `NodeType::is_lexical_field`) has an analyzer profile
(`@index(fulltext, analyzer = ...)`, `english_v1` where none is named); only
`@index(fulltext, ...)` builds segments (`NodeType::builds_full_text_index`),
and every segment is built with every `InvertedIndexParams` field set from it
(`fts_analysis::native_params`: tokenizer, language, case, stemmer, the
profile's stop list passed as `custom_stop_words` so Lance's language list never
applies, folding, token length, n-gram and code flags, `with_position(false)`,
block size 128 and posting format V2, so `LANCE_FTS_FORMAT_VERSION` cannot
change it), relying on Lance 13 applying `max_token_length`, lowercase, stemmer,
stop words and ASCII folding in that order (`InvertedIndexParams::build`); Lance
has no Unicode normalization, so the profile's NFC is not applied to the index.
The exact lexical scan (`engine/lexical_scan.rs`), which answers every
`match_terms` and `bm25` and reads no segment, runs the profile analyzer
(`fts_analysis::ProfileAnalyzer`): the same tokenizer through
`InvertedIndexParams::build`, a value's tokens through `token_stream_for_doc`
and a query's through `token_stream_for_search` (one filter chain in Lance 13),
after its own NFC (`unicode-normalization`). Every certificate is format 2: it
records the field's declared analyzer fingerprint (profile, stop-list digest,
the stemmer's algorithm, scorer; never positions or an implementation version),
the native configuration's measured fingerprint over the probe corpus, and the
native build (no normalization, block size, posting format, positions off). A
segment qualifies only where its recorded native fingerprint equals the running
binary's and that equals the profile analyzer's; no profile has that parity
under Lance 13, which has no NFC step
([full-text compatibility](#full-text-compatibility)). A raw `contains_tokens`
scan filter names no field analysis and refuses over any column holding a
segment. Explicit branch rebuilds replace all segments from rows. With no
segment, Lance's flat path tokenizes with a bare `SimpleTokenizer`, so every
committed version that holds no segment for a column declaring
`@index(fulltext, ...)` gets an untrained one
(`TableStore::commit_staged_detached`), and `ensure_indices` builds postings
where no segment covers a fragment. A full-text index on a column that no longer
declares one (a plain `@index` or a `@key` an earlier build indexed) is left: no
read uses it, no writer declares a segment for it, `ensure_indices` neither
folds nor removes it and reports it pending when it lags, and the explicit
rebuild replaces it.

**Owners:**
`lance_surface_guards.rs::native_configuration_tokenizes_as_the_index_builder_does`
(the tokens `build` emits per profile, and that the postings hold them),
`omnigraph-core` `fts_analysis::tests` (the measured-fingerprint golden, parity
only off NFC probes) and `fts_compat::tests` (each refusal), staged-table tests,
`search.rs`, `integration/maintenance.rs`,
`schema_apply.rs::full_text_analyzer_survives_schema_evolution_issue_904`,
`search.rs::full_text_analyzer_holds_after_first_rows_and_overwrites_issue_904`,
`lance_surface_guards.rs::fts_untrained_segment_applies_its_analyzer_to_rows_it_does_not_cover`
(the untrained segment carries its analyzer),
`core_tests.rs::each_profile_builds_explicit_parameters_and_certifies_its_fingerprint`,
`schema_apply.rs::full_text_analyzer_change_is_applied_in_place_with_its_profile`,
`cases/fulltext_analyzer_profiles_match.gqt`, in-source
`engine::lexical_scan::tests` (the profile's tokens through `build`, NFC first)

### Full-text compatibility

No query reads a full-text index; the exact lexical scan answers every lexical
call. A full-text index qualifies for a field only through a certificate whose
measured analyzer fingerprint this binary reproduces, so a Lance or analyzer
dependency change shows as a refusal, never as different terms.

Lance 11's stemmer is not query-compatible with existing Lance 10 postings,
and no version string proves what a stemmer or tokenizer emits. Every
full-text segment a native path reads must carry Omnigraph's artifact-scoped
analyzer certificate, bound to the immutable UUID, index details, and file
inventory, not the current dataset writer or graph head. No query reads one
today: the exact lexical scan answers every `match_terms` and `bm25` without
an index ([RFC](../rfcs/0043-full-text-index-compatibility.md) and the GQ 3
RFC, section 4).

A profile has two implementations in a binary
(`omnigraph_core::fts_analysis`): the profile analyzer the exact scan runs
(NFC, then the profile's tokenizer and filters) and the native configuration
an index is built with (`native_params`, whose `InvertedIndexParams::build`
is the tokenizer Lance 13's builder runs over each document and its query
path over each query). The measured fingerprint of either is a SHA-256 over a
versioned framing of the tokens it emits, document and query path, for every
probe of the checked-in corpus `crates/omnigraph-core/fts/probe_corpus.txt`,
whose header names the pipeline step each group exercises. Each profile is
measured once per process, on first use (a few milliseconds for all three).

Every certificate this build writes is format 2. It records the field's
declared analyzer fingerprint, the native configuration's measured fingerprint
at build time, and the native build's options (no normalization, block size,
posting format, no positions); it names no implementation version. A segment
qualifies for a field (`fts_compat::verify_index`, which always takes the
field's accepted fingerprint and profile) only when the certificate names that
fingerprint, its recorded native fingerprint equals the one the running binary
measures (a dependency change under an existing index fails here), and the
running binary's native fingerprint equals its profile analyzer's (parity
between index and scan). Lance 13's native configuration has no NFC step, so
no profile has parity: every native configuration differs from its analyzer
on the corpus's NFC-sensitive probes and nowhere else
(`fts_analysis::tests::native_configurations_differ_from_their_analyzers_only_by_nfc`),
and no segment qualifies until stage L shares one analyzer between index and
scan. A format-1 certificate, edition 2's analysis, never qualifies for a
profile. A raw `contains_tokens` scan filter names no field analysis, so it
refuses over any column holding a segment. Every refusal is
`FullTextIndexRebuildRequired` with a reason naming the check, raised before
execution; since no query reads a segment, a `match_terms` or `bm25` is never
refused for its index's state. Successful immutable proof uses Lance's bounded session metadata
cache, keyed by the expected and measured fingerprints; failures are not cached.

The full builder writes proof before the existing CreateIndex publication.
The same builder with `train(false)` writes an untrained segment (an empty
fragment bitmap, no postings) with the same proof; Lance matches every row it
does not cover with its analyzer, and every writer declares one for a column
declaring `@index(fulltext, ...)` whose committed version holds no segment
([writes](writes.md#mutation-and-load));
public object-store/base-path resolution and native garbage collection own the
bounded JSON file. Directory placement mirrors Lance's private helper and is
guarded against independently written native index files. Stable-ID
compaction preserves proof. Ordinary optimize excludes incremental full-text
folding, which lacks an uncommitted provenance hook; uncovered rows remain
searchable until an explicit rebuild. Planning and execution share the same
foldable-index predicate; pending full-text coverage alone is not recovery work.
Other index maintenance is unchanged.

A binary that reads only format 1, such as the Lance 11 predecessor, refuses a
format-2 index as malformed proof (`FullTextIndexRebuildRequired`) and returns
no rows, never the rows of a different analysis. Rollback in place after this
build has built or rebuilt an index is therefore not supported; the supported
rollback restores the whole-root backup taken before the upgrade, or rebuilds
the indexes with the older binary
([upgrade guide](../user/operations/upgrade.md#lance-substrate-upgrades-v010-v014)).
The CLI's `genuine_lance11_storage_upgrade_round_trip` journey pins the refusal
and the backup restore.

Two tests hold the analyzer. The resolved-lockfile guard
(`fts_compat::tests::analyzer_dependencies_are_the_audited_resolved_set`) pins
the audited versions; the golden
`crates/omnigraph-core/fts/measured_fingerprints.golden`
(`fts_analysis::tests::measured_fingerprints_are_the_golden`) pins what each
profile's two implementations emit. A bump that changes either fingerprint of
a profile is a profile change: keep the old behavior or define a new profile,
never refresh the golden alone ([test ownership](test-ownership.md#full-text-fingerprint-golden)).
See [RFC 0043](../rfcs/0043-full-text-index-compatibility.md) for the decision
and [the operator procedure](../user/operations/upgrade.md#full-text-index-upgrade).

### Profile filter order

A profile is a set of `InvertedIndexParams` booleans, and Lance 13's
`InvertedIndexParams::build` applies them in one order: token length, lowercase,
stemmer, stop words, ASCII folding. `english_v1`'s stop list holds each NLTK
stop word and its Snowball stem because the stemmer runs first, and folding
after stemming keeps `résumés` (`resume`) apart from `resumes` (`resum`). A bump
that reorders the filters changes a released profile's tokens; the profile
fingerprint names the stop list's digest and the stemmer's identity, not the
order, so the guard is the fence.

**Owners:**
`lance_surface_guards.rs::inverted_params_lowercase_stem_then_remove_stop_words_then_fold`,
in-source
`engine::lexical_scan::tests::english_v1_removes_stop_words_and_their_stems`

### Exact corpus statistics

The exact lexical scan's `bm25_v1` corpus is every entity whose value has at
least one analyzed term: a null, empty or token-empty value is outside `N`, the
document frequencies and the mean length, and never matches. This is the scan's
own rule, not Lance's BM25 statistics, which no query reads.

**Owners:** `cases/bm25_v1_matches_the_lexical_scoring_oracle.gqt` (null and
token-empty rows), in-source `engine::lexical_scan::tests`

### Scan-access planning

A scan's access path (`id_lookup`, `index_probe <column>`, `sequential`, or
decided at run time) is read from the physical plan Lance 13 builds for its
filter (`plan_source/scan_access.rs::inspect`): a `ScalarIndexExec` or
`MaterializeIndexExec` node names the index query, a `LanceFilterExec` a
residual, and a `FilteredReadExec` the read. Each leaf of the recorded index
query names the index's family from the search's `index_type` (`btree`,
`bitmap`, `label_list`, `ngram`, or Lance's own name, lowercased, for another).
A bump that renames or restructures those nodes reads every scan as sequential,
which changes cost, never rows.

**Owners:** `cases/planner/scalar_index_coercion.gqt`,
`cases/planner/key_sequential.gqt`,
`cases/planner/scan_access_hash_join_build_side_is_sequential.gqt`, the
`cases/planner/scalar_index_*.gqt` cases (their `access` lines go red)

### Indexes the engine did not build

The engine builds BTREE, BITMAP, LABEL_LIST, NGRAM, inverted and IVF_FLAT
indexes only (`PropType::index_kinds`). Index facts classify any other family (a
zone map, a Bloom filter, a future BTREE details type, empty vector details) as
`Unknown`, matching the details message name exactly (`ScalarIndexFamily::of`),
so the planner never claims key coverage from one. Lance's own filter planning
may still use such an index another writer built on a scan whose access is
decided at run time; such an index is derived state and changes cost only. A
fold rebuilds a lagging index whole in its own family, and an index of no known
family is not folded (`TableStore::can_fold_index`).

**Owners:** in-source
`plan_source::indexes::tests::unknown_details_never_claim_a_btree`,
`integration/scalar_indexes.rs::a_lagging_bitmap_label_list_or_ngram_is_rebuilt_in_its_own_family`

### BITMAP

An enum or `Bool` `@index` builds a BITMAP (`{column}_bitmap`) and no BTREE:
Lance 13 parses it with `SargableQueryParser` without the LIKE-prefix query
(`bitmap.rs` `new_query_parser`), so it answers `=`, `!=` (a negated equality),
`IN`, ranges, `IS NULL`, `IS NOT NULL` and a Boolean column test exactly and
keeps null rows out of `!=` and `NOT`; `starts_with` on an enum takes the scan,
and no read orders through an index. A null literal in a comparison or an `IN`
list is not sent to it (`visit_comparison`, `visit_in_list`). Rows of a fragment
outside its bitmap are scanned with the whole filter.

**Owners:**
`lance_surface_guards.rs::bitmap_answers_equality_ranges_membership_and_nulls_exactly`,
`cases/planner/scalar_index_bitmap_serves_enum_and_bool.gqt`

### LABEL_LIST

A list `@index` builds a LABEL_LIST (`{column}_label_list`; the item type must
be non-nested, `validate_label_list_data_type`). Lance 13 answers
`array_has(list, value)`, the engine's pushed form of `value in $x.list`
(`LabelListQueryParser`; DataFusion normalizes `array_contains` to it), and its
negation exactly: a null or empty list never holds a value, a null list stays
out of the negation, and a list holding a null item still matches its other
items. A null needle is not sent to the index.

**Owners:**
`lance_surface_guards.rs::label_list_answers_list_membership_exactly_and_never_matches_a_null`,
`cases/planner/scalar_index_label_list_serves_list_membership.gqt`

### NGRAM

A free-text String `@index` builds an NGRAM (`{column}_ngram`) beside its BTREE
(`{column}_btree`), and `@index(fulltext, ...)` adds its full-text index; a
`@key` alone builds the BTREE only (`IndexDeclaration`,
`PropType::index_kinds`). A free-text String's BTREE is always named, never
Lance's default `{column}_idx`, which a full-text index an earlier build made
for a plain `@index` may hold, so a replacing BTREE build cannot remove it.
Lance 13 indexes each value's trigrams after lowercasing and ASCII folding
(`TEXT_PREPPER`, `NGRAM_TOKENIZER`), keeps only trigrams of ASCII alphanumerics
(`AlphaNumOnlyFilter`), and answers `contains(text, needle)` with candidates
(`TextQueryParser`, `needs_recheck`) that the filtered read rechecks with the
whole filter (`filtered_read.rs`), so a needle differing in case or accents
never changes rows. It sends no needle of fewer than three characters
(`min_contains_chars`) and no negated `contains` (`maybe_not` refuses a query
that needs a recheck). A needle of three or more characters with no alphanumeric
trigram makes `NGramIndex::search` answer "at least no row", and the filtered
read then reads only that guaranteed set, dropping every match in an indexed
fragment, on a read and a delete alike. The engine pushes such a needle as
`replace(text, needle, '') != text` (`engine::scan::string_contains_to_df_expr`,
deciding with Lance's own tokenizers), which has the same value on every row and
which no index parser answers. Drop the rewrite at the bump whose filtered read
rechecks that result. The prepper lowercases before it folds, so a character
that lowercases to itself and folds to capitals (`™` to `TM`) yields a trigram
with capitals (`x™` gives `xTM`), which `ngram_to_token` maps with wrapping `u8`
arithmetic; a release build wraps alike when it builds and when it searches, and
the recheck keeps rows exact, but a debug build's overflow checks and a
`debug_assert!` panic in the index build. The workspace builds lance-index with
`overflow-checks` and `debug-assertions` off in the `dev` and `dst` profiles
(`Cargo.toml`), so tests run the code that ships; this compiles stock Lance with
release flags and patches nothing. Drop that override at the bump whose prepper
folds before lowercasing.

**Owners:**
`lance_surface_guards.rs::ngram_returns_a_case_folded_superset_that_lance_rechecks`
(its trigram-less half goes red at that bump),
`lance_surface_guards.rs::ngram_maps_a_folded_capital_trigram_by_wrapping` (its
first assertion goes red at the folding bump),
`cases/planner/scalar_index_ngram_serves_contains_with_a_recheck.gqt` (the
`contains_no_trigram` and `delete_no_trigram` steps),
`cases/planner/ngram_index_builds_over_a_letter_before_a_trade_mark_sign.gqt`

### Pushed filter simplification

Lance 13 prepares every scanner filter with DataFusion 54's `ExprSimplifier`
(`Planner::optimize_expr`), whose IN-list set rules fold two `=`/`!=`/`IN` tests
of one operand whose lists leave nothing to a bare `true` or `false`; for a null
operand the pair is null, so under `NOT`, or in the `NOT IN … OR NOT IN …` form,
the folded constant selects the null rows (apache/datafusion#24258 keeps the
null; no DataFusion that Lance uses has it). `ir_expr_to_df_expr` splits a
predicate on each nullable column that two or more such tests read,
`(c IS NOT NULL AND p) OR (c IS NULL AND p[c := NULL])`, at most
`MAX_NULL_SPLIT_COLUMNS` (4) columns, beyond which the predicate is not pushed
(a read evaluates it in memory, a mutation `where` refuses). When `p[c := NULL]`
reads no other column it is folded to a constant first, giving `c IS NULL OR p`
or `c IS NOT NULL AND p`: Lance has no index for a literal, so a
`c IS NULL AND NULL` branch carries a refine expression, and
`IndexedExpression::maybe_or` drops the index for an `OR` with a refine on
either side. The combined staged delete wraps each statement's predicate in its
own `IS TRUE`, which the simplifier leaves alone, so tests of different
statements never meet; a delete statement leaves out earlier statements' rows by
their recorded addresses, not by a filter. Lance reads `IS TRUE` only as a test
of a boolean column (`visit_is_bool`), so a combined staged delete of two or
more statements does not use a scalar index. Drop the split and the wrapper at
the bump whose DataFusion contains #24258.

**Owners:** in-source
`engine::scan::null_split_tests::the_split_keeps_null_rows_out_of_a_prepared_filter_issue_1277`
(goes red at that bump), `exec::staging::delete_predicate_tests`,
`writes.rs::delete_dedup_filter_does_not_drop_null_column_rows`,
`cases/issue_1277_disjoint_in_lists_keep_null_rows_out.gqt` (its `expect plan`
step pins the index probe),
`cases/issue_1277_composed_and_wide_predicates_keep_null_rows_out.gqt`

## Blob and hydration reads

### Blob v2

Null, valid empty, non-empty, selector cardinality, neighboring bytes, and 3→1
compaction are pinned on Lance 13. A filtered scan that projects Blob-v2
descriptors is pinned before and after compaction: the single-cell read shape
(`read_blob_at`: one descriptor column, an `id` equality filter through a BTREE
plus an unindexed tail, stable row ids) and a predicate on the Blob column
itself under every `BlobHandling`. Omnigraph carries no workaround or fallback
for this shape, so a red guard blocks the bump.

**Owners:** `lance_surface_guards.rs`
(`filtered_scan_projects_blob_descriptors_on_indexed_multi_fragment_table`),
`integration/maintenance.rs`

### Blob batch reads

Every complete managed Blob read goes through
`TableStore::managed_blob_payloads`, the one
`Dataset::read_blobs(column).with_row_ids(..).preserve_order(true)` call, and
consumes `try_into_stream`, never `execute`. Materializing rewrites
(`TableStore::rebuild_blob_column`) take one batch's values; export and the
change-feed baseline (`BlobRowReader`) open one call per Blob column per batch
of rows and take its values row by row as they render; change-feed images, the
change comparator's payload tie-break and entity reads open a one-row reader.
Callers pass managed row ids only: given an external row, `read_blobs` opens and
reads the referenced object. Every call sets
`with_io_buffer_size_bytes(BLOB_READ_IO_BUFFER_BYTES)` (8 MiB); Lance's default
is `SchedulerConfig::max_bandwidth`, 32 MiB times the store's I/O parallelism (2
GiB at the cloud default of 64). Lance plans the reads in request-order batches
no larger than that buffer (a larger value alone) and reads a batch only when
the stream reaches it, so a reader that holds a stream across rows keeps one
batch per column resident. The stream yields one `ReadBlob` per requested id in
order, with duplicates, `None` for null and empty bytes for a valid empty value;
the engine checks each length against the descriptor. Only the single-cell read
facade keeps `take_blobs`, for bounded ranges of one value.

**Owners:** `lance_surface_guards.rs::compact_files_succeeds_on_blob_columns`
(Guard 10: every managed placement, small-buffer stream equals `execute`, a
taken value leaves the next batch unread, external rows are read),
`forbidden_apis.rs::lance_batched_blob_read_call_counts_are_pinned` (one
production `read_blobs` and one `with_io_buffer_size_bytes` call, both in
`table_store.rs`; the helper's callers in `table_store.rs` and `export.rs`;
`take_blobs` in `blob.rs` alone; no `read_blob_ranges`),
`integration/export.rs::export_reads_each_batch_of_managed_blobs_in_one_read_per_column`

### Hydration reads

`HydrateExec` reads deferred return columns through
`FileFragment::open(projection, FragReadConfig::with_row_address(true).with_scan_scheduler(..))`
and `FragmentReader::take(offsets, 1, ..)`: decode tasks of one row each (the
reader merges overlays and applies the deletion vector as `TakeBuilder` does),
each copied at its own size and charged before the next is awaited, because a
decoded row can slice a buffer of the whole read and the pool charges a buffer
by its allocation's capacity. The engine's `ScanScheduler` caps Lance's
read-ahead at an I/O buffer reserved in the query pool; `io_buffer_size` is a
soft limit, since the queue always admits its head request and a logical request
keeps moving once admitted. Lance 13 still ignores the passed scheduler for a
data file with a `base_id` (`open_current_file_reader`). Its optional
`BaseSchedulers` cache can supply a scheduler for each base, but the current
caller supplies none, so Lance opens a `SchedulerConfig::max_bandwidth`
scheduler for each inherited file. A Lance branch reaches its parent's files
through `base_id`; a graph branch stages detached commits in the table's own
base and never holds such a file, but a table fork created before v11 (a Lance
branch) does, so a fragment holding one is read one row per `take`, without I/O
coalescing, and counted as `single_row_reads`. Drop that fallback at the bump
that honors the passed scheduler for every base.

**Owners:**
`lance_surface_guards.rs::fragment_reads_bypass_the_passed_scheduler_for_inherited_files`
(goes red at that bump), in-source
`operators::hydrate::tests::fragment_rows_reads_inherited_and_own_fragments_by_address`,
`integration/execution_memory.rs::a_hydrated_fetch_stops_at_the_chunk_bound_when_rows_widen`,
`integration/execution_memory.rs::hydration_on_a_graph_branch_reads_in_coalesced_requests`,
`integration/execution_memory.rs::hydrated_copies_of_a_joined_row_stay_within_the_chunk_bound`

## Schema evolution and compaction

### Schema evolution

Schema apply changes an existing table's columns without writing a data file
(`TableStore::stage_schema_evolution`). Renames and drops commit as
`Operation::Project` over the same field ids (Lance's own `alter_columns` rename
and `drop_columns`); additions commit as `Operation::Merge` over the unchanged
fragments with nullable fields whose ids exceed `Manifest::max_field_id`
(Lance's own all-null `add_columns`). Lance 13 validates detached commits too,
and its `merge_schema_valid` refuses a Merge that moves a field id to another
path, so a table with renames or drops and additions chains the Merge on the
Project. Lance 13 reads a field that no data file of a fragment holds as null
(`NullReader`) and skips a data file holding no projected field; Project drops a
data file whose every field is dropped; `retain_relevant_indices` keeps each
index whose fields survive, with its fragment bitmap, and removes an index on a
dropped field. Merge stamps no row version for fragments whose files are
unchanged. Keyed writes send batches in catalog column order, and Lance 13's
merge-insert matches a source to the full table schema only in the same field
order (otherwise it takes its subschema path), so the evolved schema keeps the
catalog order. A dropped column's values stay in the data files until optimize
rewrites the fragment (dropped-column erasure, next row). Making a nullable
column required (#721) commits a `Project` with `preserves_nullability: false`,
the flag Lance's own `alter_columns` uses, after the engine has checked the
rows. A deleted row keeps its value in the data file, and lance#10557 (after
13.0.0) makes `alter_columns` count deleted rows for the same reason, so the
check scans with `include_deleted_rows` (a deleted row returns with a null
`_rowid`). Fragments whose deleted rows hold a null are rewritten first as
single-fragment compaction tasks (`TableStore::stage_fragment_rewrite`), which
materialize deletions whatever Lance's `materialize_deletions_threshold`, a
planning-only option that `optimize` leaves at its 10% default.

**Owners:**
`lance_surface_guards.rs::metadata_only_schema_evolution_keeps_blob_files_ids_and_index_coverage`,
`lance_surface_guards.rs::include_deleted_rows_returns_a_deleted_rows_stored_null_issue_721`,
`schema_apply.rs` (`issue_721_*`), the deferred
`benchmarks/deferred/crates/omnigraph/tests/compaction_memory.rs.disabled`
schema-apply instrument (not an active test)

### Dropped-column erasure

A drop's `Operation::Project` keeps every data file that also holds a surviving
column, and that file's `DataFile::fields` still lists the dropped field id. A
Blob-v2 column is listed by its own id, never by its descriptor children's, so
on a table without drops every listed id is a schema field id. Lance 13's
planner also selects a fragment when a file contains only dropped user columns
but remains necessary for row lineage. It does not select a lone fragment solely
because a file mixes kept and dropped columns, so those bytes still need the
engine's extra task. `TableStore::plan_table_compaction`, the plan optimize
makes once per table, measures against its ceilings and hands to
`stage_compaction`, appends a single-fragment `TaskData` (public, as is
`CompactionPlan::tasks`) for every other fragment whose data or overlay files
list a non-negative id outside the schema. Lance's planner applies its per-run
source budgets (`max_source_fragments`, `max_source_rows`, `max_source_bytes`,
in `limit_tasks_to_source_budget`) to the tasks it returns, so the appended
tasks would fall outside them; the engine sets no budget, and
`plan_table_compaction` refuses options that set one. A single-fragment group is
the shape Lance plans for deletion materialization, and `Rewrite` replaces it in
place. Compaction re-encodes by default (`CompactionMode::Reencode`; binary copy
would carry pages) and scans the current schema, so the new files list only
current field ids and only the kept Blob values get new sidecars; the task is
sized like any other, from the kept Blob columns alone. `cleanup` then sweeps
the old files with their sidecars once no retained version names them.

**Owners:**
`lance_surface_guards.rs::compacting_a_chosen_fragment_drops_the_bytes_of_dropped_columns`,
`integration/maintenance.rs::optimize_then_cleanup_erases_dropped_property_values`,
`staged_tests.rs::table_compaction_planning_refuses_source_budgets`, the
deferred
`benchmarks/deferred/crates/omnigraph/tests/compaction_memory.rs.disabled`
erasure instrument (not an active test)

### Compaction of Blob tables

Lance 13 compaction cannot binary-copy a Blob table; it rewrites through a
scanner and materializes every managed Blob-v2 payload of one scanner batch
(external descriptors are carried unread). A batch never crosses a fragment, so
at the default batch size one batch can hold a whole fragment's payloads.
`TableStore::stage_compaction` executes the plan's tasks itself (a Blob table's
one at a time, other tables' side by side; each task holds one permit of the
run's `MaintenanceBudget`, which the run's index builds share) and sets each
task's `CompactionTask::options.batch_size` from a descriptor-only scan of that
task's fragments: the largest row's managed Blob bytes, summed over its Blob
columns, with external descriptors counting nothing
(`compaction_blob_batch_rows`: a 32 MiB budget, 1 to 8192 rows, so a row over 16
MiB puts its whole task on one-row batches; a nested or non-Blob-v2 Blob field
fails closed to one row). Every task of a Blob table gets the derived size
explicitly, capped by a caller's smaller one, so it takes precedence over
`LANCE_DEFAULT_BATCH_SIZE`, including when the variable is smaller; all tasks
are sized before any executes, so when a fragment being compacted holds a
descriptor the decoder refuses, sizing can refuse the compaction with a Blob
integrity error before that table is rewritten, and nothing is committed; a
table with nothing to compact is not scanned. Lance reads a task's options into
its scanner and ignores the batch size when planning. A set batch size beats
`LANCE_DEFAULT_BATCH_SIZE` on the V2.x read path, where the variable is only the
fallback. The fence is the scanner shape: a scanner built as compaction builds
it honors an explicit batch size over the variable. No CI-run test observes
Lance passing `CompactionOptions::batch_size` into that scanner; the historical
`compaction_memory.rs` instrument was the only observer and is now deferred
under `benchmarks/deferred/` pending GQ optimization support. This bounds
managed payload bytes per scanner batch, not total peak memory: decoder, I/O and
copy buffers are additional, and one oversized value may exceed the target. Drop
the derivation at the bump whose compaction streams Blob payloads.

**Owners:**
`lance_surface_guards.rs::explicit_scanner_batch_size_beats_lance_default_batch_size_env`
(Guard 10b: the compaction scanner shape honors an explicit batch size over the
variable),
`integration/maintenance.rs::optimize_compacts_large_blob_rows_in_bounded_batches`,
`integration/maintenance.rs::optimize_sizes_a_blob_batch_by_the_sum_of_a_rows_blob_columns`,
`integration/maintenance.rs::optimize_sizes_each_compaction_task_from_its_own_fragments`,
`integration/maintenance.rs::optimize_does_not_size_a_blob_batch_by_external_references`,
`integration/maintenance.rs::optimize_compacts_blob_fragments_with_deleted_rows`,
`staged_tests.rs::compaction_blob_batch_rows_bounds_one_batch`, the deferred
`benchmarks/deferred/crates/omnigraph/tests/compaction_memory.rs.disabled`
instrument (not an active test)

## Indexes and column patches

### Index coverage

Indexes are derived. Appends leave an uncovered tail, and reads must combine
indexed and scan paths until explicit reconciliation. A stable-row-id rewrite
carries every row-id-domain index's coverage to the new fragments
(`manifest_build.rs`, `recalculate_fragment_bitmap`); only an index whose
`results_are_row_addrs` (zone map, Bloom filter, FM), which the engine never
builds, loses the rewritten fragments, and an index older than a fragment's data
overlay loses that fragment too (`prune_overlay_stale_fields_from_indices`),
which no engine writer creates (`forbidden_apis.rs` refuses an
`Operation::DataOverlay` literal in engine sources; recheck at each bump that no
public Lance write API commits one), so optimize's compaction causes no fold
after it and its index-rows ceiling measures the lag the plan shows.

**Owners:** `integration/scalar_indexes.rs`, `search.rs`,
`integration/maintenance.rs`

### Index segments

Lance masks a logical index's results by the union of its segments' fragment
coverage. On a stable-row-ID table an update keeps the row ID and moves the row
to a new fragment; once a delta segment covers that fragment, the older
segment's entry for the row passes the mask, so full-text search returns the row
for a term it no longer holds (twice for a term both versions hold) and a BTREE
equality returns it for its old value. Lance applies per-segment ownership to
vector search only (lance#7371, lance#8351); the full-text and scalar paths are
reported as lance#9817. Omnigraph never adds a delta segment: `stage_index_fold`
rebuilds a lagging index whole in its own family (BTREE, BITMAP, LABEL_LIST or
NGRAM) and the full-text rebuild replaces every segment.
An untrained full-text segment (an empty fragment bitmap) carries its analyzer
to every row it does not cover, with rows and scores equal to a full build; with
no segment, Lance's flat path tokenizes with a bare `SimpleTokenizer`. Drop the
whole-rebuild rule at the bump whose scalar and full-text search apply segment
ownership.

**Owners:**
`lance_surface_guards.rs::index_delta_segment_serves_stale_entries_of_an_updated_row`
(goes red at that bump),
`lance_surface_guards.rs::fts_untrained_segment_applies_its_analyzer_to_rows_it_does_not_cover`

### Untrainable vector columns

A fold rebuilds a lagging vector index whole under its partition count
(`TableStore::stage_index_fold`, and the same rebuild inside
`stage_index_reconciliation`). It skips the column, as pending index work, only
when the uncommitted build fails with `lance::Error::Unprocessable`, and treats
every other failure as an error, so a storage fault during a rebuild is never
reported as a column that cannot train. Lance 13 raises `Unprocessable` only
from vector training (`train_kmeans`, the PQ trainers), when the finite,
non-null vectors are fewer than the centroids to train: a column whose vectors
are all null or all non-finite has none, and a column with at least one builds
even with fewer vectors than partitions. The declared-index planner asks the
same question before it builds, by counting non-null vectors
(`vector_column_trainable`). Re-derive the skip at the bump that moves the
variant or the training rule.

**Owners:**
`lance_surface_guards.rs::vector_index_build_without_trainable_vectors_is_unprocessable_lance_13`,
`staged_tests.rs::index_fold_skips_only_a_vector_column_that_cannot_train`

### Column patch

An `update` may stage `MergeInsertWriteMode::RewriteColumns`
(`KeyedWriteSemantics::KnownPresentColumnPatch`,
[writes](writes.md#column-patch-updates)). Lance 13 accepts it only for a
matched-rows update whose source omits a column, carries a column besides the
key and carries no Blob (`select_write_sink`); it attaches one data file per
touched fragment holding every source column for all of that fragment's rows,
tombstones (`-2`) their older copies, keeps every other data file, Blob sidecar,
row id and fragment id, and restamps exactly the patched offsets with the read
version plus one (`updated_fragment_offsets`, re-stamped by `build_manifest`
against the manifest the commit reads, so a detached commit stamps its read
version plus one). Lance writes the join key into the patch and lists it in
`fields_modified`, which withdraws each touched fragment from every index over
the key; the engine removes the key from `fields_modified` (the values and row
ids the index holds are unchanged), so only indexes over patched columns lose
coverage. The patch file takes the dataset's storage format (V2_2). Lance's
planner does not select a fragment for tombstoned bytes alone, so the old copies
stay until the fragment is otherwise compacted; a column patch carries no
inserted-row filter. Drop `keep_key_index_coverage` at the bump whose patch
omits the key or no longer lists it (requested upstream as lance#10781).

**Owners:**
`lance_surface_guards.rs::column_patch_keeps_blob_files_row_ids_and_key_index_coverage_issue_1264`,
`writes.rs::update_of_one_scalar_patches_instead_of_rewriting_blobs_and_vectors_issue_1264`,
`cases/update_column_patch_matches_whole_row_update.gqt`

### Inherited index files

Lance 13 preserves inherited index bases during nested clones and resolves
external fragment-reuse details at their original base, pinned by cold
nested-clone queries. Production branch creation clones only the catalog's
`__manifest` dataset, which has no fragment-reuse index; graph tables are not
cloned, and stable-row-ID graph compaction does not produce external
fragment-reuse details. Malformed external fragment-reuse payloads are outside
the adapter's supported graph contract: native branch creation need not read or
reject them before publication, and such a payload can fail only when an index
is later read.

The clone adapter is installed during normal dataset opens and initial dataset
creation, preserving configured commit handlers without an additional reopen.
It checks the exact source version at branch creation and delegates clone
metadata to Lance. A task-local scope reuses the
known main-source `ManifestLocation` only for the matching native V2 path and
version, avoiding a listing; other locations retain native resolution. The
adapter delegates manifest writing, transactions and commit-handler capabilities
to the original handler. It neither captures an index inventory nor repairs
index bases or reads external fragment-reuse payloads.

**Owners:** `lance_clone` tests (exact source-version refusal, source-location
reuse and fallback, local ETag normalization, configured-handler/session/failure
semantics);
`lance_surface_guards.rs::stock_lance_nested_clone_preserves_inherited_index_base_issue_7840`

## Branches, refs and cleanup

### Branches/tags

Each graph-branch lifetime is `{logical}.{ULID}`; bare names identify legacy
lifetimes. Unretired native `__manifest/_refs/branches/` entries define logical
branches. Deletion writes identity-bound retirement metadata, archives the exact
`BranchContents` inside its native tree, then unlinks the active ref. Needed
native ancestry and historical merge-base providers remain readable through the
archive. User `__manifest` tags block logical deletion; valid engine merge-input
tags instead protect their exact snapshots until the target publication witness
expires.

Stock `Branches::get` and `list` include every physical ref. Omnigraph's logical
manifest helpers validate and filter `omnigraph.retired_manifest_branch`, a
version-1 JSON value binding the exact native name and identifier. An absent
marker means live; malformed, unsupported, or mismatched metadata fails closed.
The metadata update publishes retirement and preserves unrelated metadata.
Before unlinking the active ref, retirement archives the exact `BranchContents`
inside its native tree; retries validate the same identity in the ref or
archive. Native branch controls retain Omnigraph's existing process-local
serialization. Cold logical enumeration reads active refs; cached named-write
admission checks the existing ref lookup without an added request. Cleanup and
historical resolution read archives. Cleanup retains required native ancestors
and exact merge-base providers, then reclaims unneeded trees with their
archives. Delayed staging is judged from the complete post-list identity cut and
final capture validation, so its retirement history need not remain forever.

**Owners:** `integration/branching.rs`, `branch_control.rs` tests,
`integration/merge_cleanup.rs`

### Branch ref reads

Stock Lance 13 reads a ref as `head` then `get_range(0..size)`, so a
`replace_metadata` landing between the two calls serves a prefix of the
rewritten object and the read fails to parse instead of returning either
version. The engine reads an exact ref (`branch_control::get_branch_contents`,
`get_branch_identifier`, `dataset_branch_identifier` and each candidate of
`resolve_live_native_branch`) with one whole-object `get` (`read_ref_contents`)
instead of Lance's `exists`, `head` and `get_range`, so that read cannot be torn
and costs one request instead of three; a ref written before identifiers existed
still goes through Lance's reader, which derives its synthetic identifier.
`branch_control::read_branch_refs` rereads a torn read (`Arrow`, `CorruptFile`)
or a raced delete (`NotFound`), at most three reads in all, around the listed
reads that still go through Lance (`list_branches`) and the exact reads above.
Resolving a logical branch name (`branch_control::resolve_live_native_branch`)
lists `_refs/branches/` once and reads only that branch's own incarnations, so
another branch's retirement rewrite, or its malformed retirement record, no
longer fails a branch open or a held-ref re-resolve; fresh creation narrows content reads to its source and the target's equal,
ancestor and descendant names.
Operations that require a complete inventory, including logical enumeration and
cleanup, still read every active ref; maintenance and historical resolution also
read retirement archives. Delete the retry arm and un-ignore the guard at the
bump whose `from_path` reads a ref in one `get`.

**Owners:** `branch_control.rs` stale-head tests (they go red at that bump),
`lance_surface_guards.rs::branch_ref_read_survives_concurrent_metadata_rewrite`
(run with `--ignored`)

### Cleanup

Stock `cleanup_old_versions` is never called on a graph table. Lance 13 does not
trace detached manifests, and since lance#8944 it lists `data/` through
`max(retained-manifest floor, now − 7 days)`, making old files referenced only
by detached versions eligible for deletion while their manifests remain. The
native guard puts the attached v1 manifest before the detached file, and the
file before the seven-day cutoff, so only the seven-day rule can sweep the file.
Files shared with retained attached versions stay protected; a recent detached
snapshot can still lose an old file it inherits. An external Lance cleanup of
graph tables or `__manifest`, whose proposals and unattached snapshots are
detached versions, can therefore make retained snapshots, including the current
graph, unreadable. The engine collector discovers canonical registrations,
including tombstoned table lifetimes. Roots include retained graph snapshots,
selected merge bases, exact tagged graph snapshots and exact tagged table
versions. It marks base and overlay data files, deletion files, index
directories, transaction files and Blob sidecars, preserving borrowed physical
origins. Native tree candidates are frozen before graph capture; after table
inventories, unchanged branches, identities, heads and tag inventories must
validate before deletion. Valid staging witnesses are judged against captured
heads and a complete post-list live-identity cut; ambiguous ownership stays.
Unneeded retired trees and their archives are collectible after dependency
closure. Foreign linear versions remain untouched; orphan objects require their
own seven-day age. A failed trace keeps its table, and incomplete origin tracing
prevents physical-file deletion across the run.

`--keep N` retains the newest N `__manifest` versions per live branch, which is
N graph commits, and every table version they pin; it never counts Lance
versions of a dataset. Every policy takes the version numbers from the
`_versions/` listing (`Dataset::version_refs`, which reads no manifest, as
Lance's `retain_n_versions` does since lance#9019), so `--keep` alone reads only
the manifests it retains and those of the selected merge bases, which
`retained_merge_bases` pins even when the policy prunes them. `--older-than`
retains every `__manifest` version newer than the cutoff. It reads the commit
time of each version that the newest-N, HEAD and branch-base rule leaves
undecided through `Dataset::checkout_version` (a HEAD and a GET each), as many
at once as the store's `io_parallelism` allows, and no other: Lance stamps a
manifest with its writer's clock, so the times are not monotonic in the version.
Each is classified under its listed number, and a body naming another version is
refused. A manifest the run reads must decode or the plan fails; one it prunes
unread is not checked, since its pins are not roots either way. `--older-than`
also gates pre-v11 fork collection using tree and native ref object timestamps
before closing over dependencies. Any recent object retains such a fork,
including a fresh retirement metadata update over an old tree. Ref metadata
listings for this age check occur only during cleanup with an explicit age
policy.

**Owners:**
`lance_surface_guards.rs::stock_cleanup_sweeps_a_week_old_detached_data_file_lance_13`
(the stock sweep; goes red at the bump that traces detached manifests),
`forbidden_apis.rs` (`cleanup_old_versions(` is deny-listed in engine sources),
`integration/maintenance.rs`, `collector.rs` tests, the DST collector oracle,
`integration/merge_cleanup.rs`, `integration/branching.rs`

### Bound branch create

Lance 13 `Dataset::create_branch` hands its `CommitBuilder` the dataset's object
store and store parameters but not its session. The commit writes through that
store, but its existence probe of the new branch location
(`DatasetBuilder::load`, a `_versions/` listing) opens on a default session,
whose registry builds a client from the store parameters alone. A bound handle's
datasets carry no provider options (its registry supplies them), so
`lance_clone::create_branch` passes the captured binding's options as those
parameters; without them the probe reaches the ambient provider configuration.
Drop the parameters at the bump whose `create_branch` passes the dataset's
session.

**Owners:** `bound_storage.rs` (S3-configured decoy)

### Table forks

No table fork is created for a graph branch: a branch write stages detached on
the dataset and native ref its inherited registration names. Lance's
`create_branch` accepts a detached id as the parent but leaves the branch
headless (a linear commit on it is refused), which is why the engine forks
nothing. Forks created before v11
(`fork.{incarnation}.m{base_version}.{commit_ULID}`, `legacy` in the incarnation
position) stay valid: a registration naming one opens there, `table_fork_owner`
records ownership, and `cleanup` may reclaim an unreferenced one after its
parsed owner incarnation is gone and tag, ancestry and age protections allow it.
Canonical ULIDs and version numbers are required for that name-based liveness
check; generated forks with malformed or `legacy` owner components stay
retained. Names never establish table ownership.

**Owners:** `lance_surface_guards.rs`, `integration/branching.rs`, metadata
tests

## Commits and the catalog

### Detached commits

Every graph-table write is a Lance detached commit
(`_versions/d{version}.manifest`), which never moves HEAD and needs no conflict
pass, and it is the table's version for its whole life: nothing replays it
linearly, no `Restore` is used, a graph table's linear HEAD stays at its `v1`
create, and the registration's `omnigraph.last_linear_version` records where the
linear history stopped. Every manifest names its transaction file
`{read_version}-{uuid}.txn`, which is how the engine matches a pin's
`transaction_uuid` and follows a chain of detached commits by `read_version`
links without a second request (the collector, `walk_chain`); the pure-insert
merge proof follows the same links and also reads each link's transaction file
to certify it, crossing a `CreateIndex` link as moving no row because Lance
applies it with every fragment of the version it read. A detached commit never
triggers Lance auto-cleanup: `auto_cleanup_hook` runs only from
`commit_transaction`, and every engine commit passes `skip_auto_cleanup: true`.

**Owners:** `lance_surface_guards.rs` (`rfc_0067_*`, including
`rfc_0067_create_index_keeps_every_fragment_of_its_read_version`),
`collector.rs` tests

### Catalog publication

Every `__manifest` publish is an `Operation::Overwrite` committed through
`CommitBuilder::with_max_retries(0)`. Lance 13 treats that as a strict
overwrite: it skips conflict resolution and writes
`_versions/{read_version + 1}.manifest` only if absent, so a concurrent commit
fails the publish with `CommitConflict` instead of being rebased over; the
publisher maps that to `RowLevelCasContention` and retries from a fresh load. A
Lance release that rebased overwrites under zero retries would break this
version CAS.

**Owners:** `omnigraph-catalog`
`stale_overwrite_loses_the_version_cas_without_replacing_the_winner` (the stale
publish itself), `publisher_retry_vocabulary_is_the_version_cas` (the error
mapping)

### Manifest request resolution

Lance 13 writes a version's manifest with one put-if-absent through the
dataset's `CommitHandler` (`ConditionalPutCommitHandler` on local Unix, S3 and
Azure roots). It writes the transaction file first and propagates that put's
failure through a plain `?`, so no manifest request was sent. On a failed
manifest put it reads the version back (`verify_commit_outcome`), and because
that handler calls a missing version definitive
(`is_version_not_found_definitive`), an absent version ends the attempt at once,
although an abandoned put can still land afterwards. The handler Omnigraph
installs on every dataset (`lance_clone`) therefore re-issues an ambiguous
manifest put with the same manifest and precondition, by the storage crate's
shared conditional-request policy (`omnigraph_storage::conditional`, up to
`CONDITIONAL_REQUEST_REISSUES` times), before Lance reads back, and records per
commit whether a request was sent and how it ended (`manifest_request`). A held
version goes back as `CommitConflict` to Lance's read-back, which compares the
complete transaction at that version. A re-issue that meets a held version is
recorded unresolved, not held: S3 answers 409, which `object_store` reports as
"already exists", while the earlier copy is still in flight, so a missing
version on read-back proves nothing. The first copy's conflict stays held; a
direct writer's retrying Lance client can repeat that copy itself, which this
handler cannot see. Lance deletes the attempt's transaction file only when the
handler calls a missing version definitive (`is_version_not_found_definitive`,
read nowhere else), so only the commit's own request record answers it: the
catalog's per-commit handler answers from its probe and keeps the inner answer
unless the request may still land, and a dataset's shared handler, which holds
no record of the attempt Lance reads back, never calls a missing version
definitive. Lance then returns `CommitStatusUnknown` and keeps the file a late
manifest would name; a table's orphaned transaction file goes to the collector's
orphan sweep. The catalog publisher reads the record: no request sent, or one
the store refused outright, is `Unpublished`; a request that may have applied is
read back. A generic local-filesystem error is ambiguous too: the Windows rename
handler can create the destination before temporary-file cleanup fails. Drop the
re-issue at the bump whose commit path settles an ambiguous manifest put itself,
and keep the record while Lance's errors do not say whether the manifest was
sent.

**Owners:** in-source
`lance_clone::tests::manifest_request_is_re_issued_until_the_store_answers_issue_1067`,
`lance_clone::tests::a_shared_handler_never_calls_a_missing_version_definitive_issue_1067`,
`manifest_request` tests; `omnigraph-catalog`
`transaction_file_failure_before_the_manifest_is_unpublished_issue_1067`,
`unresolved_manifest_create_keeps_its_transaction_file_issue_1067`,
`lost_manifest_acknowledgement_resolves_by_re_issue_issue_1067`,
`abandoned_manifest_create_that_lands_late_publishes_once_issue_1067`,
`manifest_create_without_a_definite_answer_stays_in_doubt_issue_1067`

### Catalog freshness probe

A read on a warm handle learns whether its branch's `__manifest` moved by asking
for attached versions by name (`ManifestNamingScheme::manifest_path`, V1 or V2
paths): the held version `v` and `v + 1` in two concurrent HEADs, then `v + 2`
when `v + 1` exists; with `v` present, the first "not found" ends the history.
Lance 13's `latest_version_id` lists `_versions/` instead, and on a store with
ordered listing (S3, GCS, Azure) that one request returns up to 1,000 keys, so a
branch past 1,000 commits paid a full page on every read. The first absent name
is the latest because Lance writes an attached version only with a put-if-absent
over a read of the version below it, and nothing deletes a version of a live
branch's `__manifest` (`cleanup` collects a retired branch's whole tree, which
the named-branch probe's identifier read catches); a reclamation of old
`__manifest` versions must keep every version above one a live handle holds. An
absent `v` (a removed `__manifest` answers "not found" for every name), a gap of
more than two versions, a detached handle or any error falls back to the
listing, which reports a removed `__manifest` as `NotFound`. A process that
holds the branch's writer epoch skips the probe on its handle's bound branch, so
nothing there catches another process's delete of that branch until a publish of
the holder loads it ([writes](writes.md#writer-epoch)).

**Owners:** `omnigraph-catalog`
`freshness_probe_asks_for_the_next_version_by_name` (V1 and V2 paths on an
ordered store: HEADs and no listing, the listing past two versions, the
listing's `NotFound` for a removed `__manifest`); engine
`lifecycle::write_preparation_manifest_read_failures_carry_before_effect_evidence`
(a write on a warm handle refuses before effects when `__manifest` is gone)

### Local manifest identity

`object_store` 0.14 quotes its local `inode-mtime-size` ETag, while Lance 13
direct discovery (`current_manifest_local`, through `object_writer::get_etag`)
returns the same token bare. The configured native-local commit handler quotes
only that known three-part hexadecimal representation when the store exposes
direct local paths, preserving warm manifest metadata reuse without extra I/O.
Strict generation and identity checks remain; custom handlers, cloud tokens,
absent tokens and unrecognized representations are unchanged.

**Owners:**
`lance_clone::tests::local_discovery_only_quotes_the_native_generation_token`,
`creation_preserves_configured_handler_session_and_failure_semantics`,
`newly_created_dataset_can_fork_twice_without_reopening`; catalog
`acknowledged_publisher_reuses_metadata_in_each_fresh_probe`,
`publisher_reuses_only_its_exact_bounded_published_rows`

### Packed catalog record

Since stamp v12 the `__manifest` record fields are one `Struct` column with
`lance-encoding:packed = true`, which Lance 2.1+ stores row-major in one column
(`PackedStructVariablePerValueEncoder`), so a scan reads one column's pages for
the record. Two Lance 13 limits still shape the layout: a packed struct child
with a null is refused at write
(`Per-value compression not yet supported for block type Nullable`, the row
format has no per-child null bit), so every child is non-null and the `present`
bitmask child carries the logical nulls; and projecting a fixed-width child
alone fails to decode (`invalid variable-width layout for UInt64`), so every
scan projects `record` whole (no scan filters on a record field). If packed
struct children gain null support, removing the persisted `present` layout
requires a separate graph-format decision. Remove the whole-record projection
only when native child projection is qualified.

**Owners:** `omnigraph-catalog`
`packed_record_round_trips_nulls_through_present_bits`,
`lance_surface_guards.rs::packed_struct_refuses_null_values_and_lone_child_projection_lance_11`
(goes red at either bump)

### Catalog fragment layout

A branch's highest-ID `__manifest` fragment holds its hot rows; other fragments
hold cold registry rows reused by the publisher. Generic `optimize` preserves
this layout. The stamp that introduced each row kind, and the stamps a binary
serves, are in [versioning](versioning.md#current-storage-contract).

## Other surfaces

### Ordered walks

Lance 13's `HardCapBatchSizeExec` deep-copies a one-row slice before measuring
it, fixing the false rejection caused by its parent buffer's capacity
(lance#9048). `FilteredReadExec` still re-slices tails without copying in
`rechunk_stream_by_size` (lance#9692), and a truly wide row still exceeds the
37.5 MiB sorter cap. Every `id`-ordered walk (branch merge, change feed, export)
therefore sorts only `id`, `_rowid` and `_rowaddr` and hydrates rows in bounded
chunks (`ordered_cursor.rs`).

**Owners:** `integration/changes.rs`, `integration/export.rs` and
`integration/merge_fast_forward.rs` wide-row tests
(`tests/helpers/wide_rows.rs`)

### MemWAL

Upstream MemWAL has no Omnigraph consumer: no stream profile, token ledger,
hidden stream column, or `_mem_wal` path exists.

**Owners:** `integration/lifecycle.rs`, cluster removed-field diagnostics
