# Full-text index compatibility proof and explicit rebuild

**Decides:** a full-text index is used for search only when its own immutable artifact carries a certificate naming the supported analyzer generation, and an explicit branch-scoped rebuild is the only way to earn one for an old index.
**Contract:** keeps E5, E7, read:4, read.refuses, maintain.changes, maintain:1
**Code:** `crates/omnigraph-core/src/fts_compat.rs`, `crates/omnigraph-core/src/dataset_index.rs`, `crates/omnigraph/src/db/omnigraph/table_ops.rs`, `crates/omnigraph/src/table_store.rs`, `crates/omnigraph-cli/src/commands/maintenance/mod.rs`, `crates/omnigraph/tests/search.rs`, `crates/omnigraph/src/core_tests.rs`
**Cited as:** RFC 0043

## Context

A change of stemmer between Lance releases changes the stems of ordinary
English words. An index built by the old stemmer, read by the new one,
returns fewer matches with no error. Lance's persisted tokenizer parameters,
the dataset writer version and the posting encoding do not say which
stemmer built an index. So nothing Lance stores can tell a valid index from
one that silently misses rows, and a silent miss is a wrong answer.

## Decision

- A full build writes `omnigraph_fts_compat.json` inside the new index UUID
  directory before the staged CreateIndex publishes. It holds a format
  version, the index UUID, the analyzer generation and a digest of the
  immutable index details and file inventory.
- The generation (`ANALYZER_GENERATION`) names analyzer semantics, not a
  Lance release. A guard binds it to the pinned Lance, tokenizer and stemmer
  versions; changing them needs an audit that keeps or replaces the
  generation.
- A full-text search verifies every snapshot-selected segment on the
  columns it reads. Missing, malformed, mismatched or unknown proof refuses
  with `FullTextIndexRebuildRequired` (HTTP 409, `code: conflict`, detail
  `full_text_index_rebuild_required`). Storage failures stay storage
  failures. Ordinary reads never read certificates.
- Successful verification is cached only in Lance's bounded session-local
  metadata cache. Failures are never cached.
- `omnigraph index rebuild --kind full-text` (engine
  `rebuild_full_text_indices_on_as`) rebuilds every full-text segment of a
  branch from its current rows, with engine-default English analysis, in one
  graph publication through the ordinary staged writer, under `maintain`
  policy. It reports the commit and the rebuilt pairs. The spelling
  `rebuild-full-text-indexes` is a deprecated alias.
- History keeps its original indexes. A rebuild of one branch migrates no
  other branch.
- An untrained segment declared in the commit that would leave a version
  without one carries the same proof, so a declared column is never searched
  with a bare tokenizer.
- Ordinary optimize does not fold incremental full-text postings, since a
  merged or remapped segment has no pre-publication proof. Uncovered rows
  stay searchable by Lance's tail scan until a rebuild. Stable-row-ID
  compaction keeps the proof.
- The certificate is derived index state, not a graph-format change and not
  protection against a malicious writer.

## Rejected

- **No gate:** reproduces silently missing results.
- **Mark the graph or the latest table version as upgraded:** unsound for
  history, branch inheritance and retained old segments.
- **Fork Lance or revert the stemmer:** a far larger maintenance boundary,
  and the old stemmer has an upstream correctness defect.
- **Automatic inline rebuild or a migration service:** unbounded foreground
  work and extra infrastructure, where an explicit operation suffices.
- **Certify incremental outputs after publication:** adds a second recovery
  obligation.
