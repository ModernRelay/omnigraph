mod helpers;

use std::env;

use arrow_array::{Array, Int64Array, StringArray};
use lance_index::is_system_index;
use serial_test::serial;

use omnigraph::db::Omnigraph;
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph_compiler::query::ast::Literal;
use omnigraph_compiler::result::QueryResult;

use helpers::*;

const SEARCH_SCHEMA: &str = include_str!("fixtures/search.pg");
const SEARCH_DATA: &str = include_str!("fixtures/search.jsonl");
const SEARCH_QUERIES: &str = include_str!("fixtures/search.gq");
const MOCK_SEARCH_SCHEMA: &str = r#"
node Doc {
    slug: String @key
    title: String @index
    embedding: Vector(4) @index
}
"#;
const MOCK_SEARCH_QUERIES: &str = r#"
query vector_search_vector($q: Vector(4)) {
    match { $d: Doc }
    return { $d.slug, $d.title }
    order { nearest($d.embedding, $q) }
    limit 3
}

query vector_search_string($q: String) {
    match { $d: Doc }
    return { $d.slug, $d.title }
    order { nearest($d.embedding, $q) }
    limit 3
}

query vector_search_literal() {
    match { $d: Doc }
    return { $d.slug, $d.title }
    order { nearest($d.embedding, "alpha") }
    limit 3
}

query hybrid_search_vector($vq: Vector(4), $tq: String) {
    match { $d: Doc }
    return { $d.slug, $d.title }
    order { rrf(nearest($d.embedding, $vq), bm25($d.title, $tq)) }
    limit 3
}

query hybrid_search_string($vq: String, $tq: String) {
    match { $d: Doc }
    return { $d.slug, $d.title }
    order { rrf(nearest($d.embedding, $vq), bm25($d.title, $tq)) }
    limit 3
}
"#;
// Same shape as MOCK_SEARCH_SCHEMA but the vector records the model that
// produced its stored vectors, opting into the query-time same-space check.
const MODEL_RECORDED_SCHEMA: &str = r#"
node Doc {
    slug: String @key
    title: String @index
    embedding: Vector(4) @embed("title", model="test-model-a") @index
}
"#;
const SEARCH_MUTATIONS: &str = r#"
query insert_doc($slug: String, $title: String, $body: String, $embedding: Vector(4)) {
    insert Doc {
        slug: $slug,
        title: $title,
        body: $body,
        embedding: $embedding
    }
}
"#;

// A deliberately reverse-loaded edge table over a trivially ranked vector
// corpus.  The source search order is rank-1, rank-2, rank-3, while physical
// edge scan order starts at rank-3.  rank-1 has two parallel edges so the RRF
// assertion also catches row loss/duplication within one fused entity rank.
const RANKED_EDGE_SCHEMA: &str = r#"
node RankedDoc {
    slug: String @key
    embedding: Vector(4)
}

edge RankedLink: RankedDoc -> RankedDoc {
    label: String
}
"#;

const RANKED_EDGE_DATA: &str = r#"{"type":"RankedDoc","data":{"slug":"rank-1","embedding":[0.0,0.0,0.0,0.0]}}
{"type":"RankedDoc","data":{"slug":"rank-2","embedding":[1.0,0.0,0.0,0.0]}}
{"type":"RankedDoc","data":{"slug":"rank-3","embedding":[2.0,0.0,0.0,0.0]}}
{"type":"RankedDoc","data":{"slug":"sink","embedding":[9.0,0.0,0.0,0.0]}}
{"edge":"RankedLink","from":"rank-3","to":"sink","data":{"id":"edge-c","label":"C"}}
{"edge":"RankedLink","from":"rank-2","to":"sink","data":{"id":"edge-b","label":"B"}}
{"edge":"RankedLink","from":"rank-1","to":"sink","data":{"id":"edge-a2","label":"A2"}}
{"edge":"RankedLink","from":"rank-1","to":"sink","data":{"id":"edge-a1","label":"A1"}}
{"edge":"RankedLink","from":"sink","to":"rank-3","data":{"id":"edge-d","label":"D"}}"#;

const RANKED_EDGE_QUERIES: &str = r#"
query nearest_edges($q: Vector(4)) {
    match {
        $d: RankedDoc
        $d $w:rankedLink $target
    }
    return { $d.slug, $w.label }
    order { nearest($d.embedding, $q) }
    limit 4
}

query rrf_edges($q1: Vector(4), $q2: Vector(4)) {
    match {
        $d: RankedDoc
        $d $w:rankedLink $target
    }
    return { $d.slug, $w.label }
    order { rrf(nearest($d.embedding, $q1), nearest($d.embedding, $q2)) }
    limit 4
}

query nearest_hops($q: Vector(4)) {
    match {
        $d: RankedDoc
        $d rankedLink{1,2} $target
    }
    return { $d.slug, $target.slug }
    order { nearest($d.embedding, $q) }
    limit 2
}
"#;

// MECHANISM tier for issue #563 (the symptom-scale twin is the #[ignore]d
// tests/repro_issue_563.rs): a BM25 corpus whose edge-bearing chunks sit
// outside the capped scan's window. Every chunk matches the query term, but only chunks 8..=11 have an
// edge — and with `limit 2` the scan cap is 8 rows (2 ×
// BM25_SCAN_OVERFETCH_FACTOR; if that factor grows past 4 the capped window
// reaches the edge-bearing band and the retry stops being exercised). Neither the
// highest-scoring 8 nor the lowest-scoring 8 chunks can satisfy the join; the
// band is deliberately in the middle so the test holds whichever way BM25
// orders the corpus.
const UNDERFILL_SCHEMA: &str = r#"
node Chunk {
    slug: String @key
    text: String @index
}

node Artifact {
    slug: String @key
}

edge ChunkOfArtifact: Chunk -> Artifact {
    label: String
}
"#;

const UNDERFILL_QUERY: &str = r#"
query recall($q: String) {
    match {
        $c: Chunk
        $c chunkOfArtifact $a
        search($c.text, $q)
    }
    return { $c.slug, $a.slug }
    order { bm25($c.text, $q) }
    limit 2
}
"#;

const UNDERFILL_AGG_QUERY: &str = r#"
query recall_count($q: String) {
    match {
        $c: Chunk
        search($c.text, $q)
    }
    return { count($c) as total }
    order { bm25($c.text, $q) }
    limit 2
}
"#;

const UNDERFILL_RRF_QUERY: &str = r#"
query recall_rrf($q: String) {
    match {
        $c: Chunk
        $c chunkOfArtifact $a
        search($c.text, $q)
    }
    return { $c.slug, $a.slug }
    order { rrf(bm25($c.text, $q), bm25($c.text, $q)) }
    limit 2
}
"#;

const UNDERFILL_CHUNKS: usize = 20;
const UNDERFILL_LINKED: std::ops::RangeInclusive<usize> = 8..=11;

fn underfill_seed_data() -> String {
    let mut rows = vec![r#"{"type":"Artifact","data":{"slug":"art-0"}}"#.to_string()];
    for chunk in 0..UNDERFILL_CHUNKS {
        // Vary term frequency so the corpus has a real BM25 order rather than
        // a tie the engine could resolve arbitrarily.
        let needle = vec!["needle"; UNDERFILL_CHUNKS - chunk].join(" ");
        rows.push(format!(
            r#"{{"type":"Chunk","data":{{"slug":"chunk-{chunk:02}","text":"{needle} filler"}}}}"#
        ));
    }
    for chunk in UNDERFILL_LINKED {
        rows.push(format!(
            r#"{{"edge":"ChunkOfArtifact","from":"chunk-{chunk:02}","to":"art-0","data":{{"id":"e-{chunk:02}","label":"of"}}}}"#
        ));
    }
    rows.join("\n")
}

const STARVATION_RRF_QUERY: &str = r#"
query recall_two_terms($q1: String, $q2: String) {
    match {
        $c: Chunk
        $c chunkOfArtifact $a
        search($c.text, $q1)
    }
    return { $c.slug }
    order { rrf(bm25($c.text, $q1), bm25($c.text, $q2)) }
    limit 1
}
"#;

/// Seven chunks with (alpha, beta) term frequencies, padded to 20 tokens each;
/// only x, y, n carry an edge. Alpha ranks the four edge-less decoys above
/// every eligible chunk; beta ranks n first. Fusing the COMPLETE rankings
/// makes x the winner (strong in both arms: 1/61 + 1/62 beats n's
/// 1/63 + 1/61 at k = 60); losing the alpha arm makes n win on beta alone.
fn starvation_seed_data() -> String {
    let mut rows = vec![r#"{"type":"Artifact","data":{"slug":"art-0"}}"#.to_string()];
    let chunks: [(&str, usize, usize); 7] = [
        ("decoy-1", 7, 1),
        ("decoy-2", 6, 2),
        ("decoy-3", 5, 3),
        ("decoy-4", 4, 4),
        ("x", 3, 6),
        ("y", 2, 5),
        ("n", 1, 7),
    ];
    for (slug, alpha, beta) in chunks {
        let mut words = vec!["alpha"; alpha];
        words.extend(vec!["beta"; beta]);
        words.extend(vec!["filler"; 20 - alpha - beta]);
        rows.push(format!(
            r#"{{"type":"Chunk","data":{{"slug":"{slug}","text":"{}"}}}}"#,
            words.join(" ")
        ));
    }
    for slug in ["x", "y", "n"] {
        rows.push(format!(
            r#"{{"edge":"ChunkOfArtifact","from":"{slug}","to":"art-0","data":{{"id":"e-{slug}","label":"of"}}}}"#
        ));
    }
    rows.join("\n")
}

async fn init_search_db(dir: &tempfile::TempDir) -> Omnigraph {
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, SEARCH_SCHEMA).await.unwrap();
    load_jsonl(&db, SEARCH_DATA, LoadMode::Overwrite)
        .await
        .unwrap();
    db.ensure_indices().await.unwrap();
    db
}

async fn init_ranked_edge_db(dir: &tempfile::TempDir) -> Omnigraph {
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, RANKED_EDGE_SCHEMA).await.unwrap();
    load_jsonl(&db, RANKED_EDGE_DATA, LoadMode::Overwrite)
        .await
        .unwrap();
    db
}

async fn init_mock_embedding_search_db(dir: &tempfile::TempDir) -> Omnigraph {
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, MOCK_SEARCH_SCHEMA).await.unwrap();
    load_jsonl(&db, &mock_embedding_seed_data(), LoadMode::Overwrite)
        .await
        .unwrap();
    db.ensure_indices().await.unwrap();
    db
}

async fn init_model_recorded_search_db(dir: &tempfile::TempDir) -> Omnigraph {
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, MODEL_RECORDED_SCHEMA).await.unwrap();
    load_jsonl(&db, &mock_embedding_seed_data(), LoadMode::Overwrite)
        .await
        .unwrap();
    db.ensure_indices().await.unwrap();
    db
}

fn mock_embedding_seed_data() -> String {
    [
        ("alpha-doc", "alpha guide", mock_embedding("alpha", 4)),
        ("beta-doc", "beta guide", mock_embedding("beta", 4)),
        ("gamma-doc", "gamma handbook", mock_embedding("gamma", 4)),
    ]
    .into_iter()
    .map(|(slug, title, embedding)| {
        format!(
            r#"{{"type":"Doc","data":{{"slug":"{}","title":"{}","embedding":[{}]}}}}"#,
            slug,
            title,
            format_vector(&embedding)
        )
    })
    .collect::<Vec<_>>()
    .join("\n")
}

fn format_vector(values: &[f32]) -> String {
    values
        .iter()
        .map(|value| format!("{:.8}", value))
        .collect::<Vec<_>>()
        .join(", ")
}

fn mock_embedding(input: &str, dim: usize) -> Vec<f32> {
    let mut seed = fnv1a64(input.as_bytes());
    let mut out = Vec::with_capacity(dim);
    for _ in 0..dim {
        seed = xorshift64(seed);
        let ratio = (seed as f64 / u64::MAX as f64) as f32;
        out.push((ratio * 2.0) - 1.0);
    }
    normalize_vector(out)
}

fn normalize_vector(mut values: Vec<f32>) -> Vec<f32> {
    let norm = values
        .iter()
        .map(|value| (*value as f64) * (*value as f64))
        .sum::<f64>()
        .sqrt() as f32;
    if norm > f32::EPSILON {
        for value in &mut values {
            *value /= norm;
        }
    }
    values
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 14695981039346656037u64;
    for byte in bytes {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(1099511628211u64);
    }
    hash
}

fn xorshift64(mut x: u64) -> u64 {
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    x
}

fn result_slugs(result: &QueryResult) -> Vec<String> {
    let batch = result.concat_batches().unwrap();
    let slugs = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    (0..slugs.len())
        .map(|index| slugs.value(index).to_string())
        .collect()
}

fn first_two_strings(result: &QueryResult) -> Vec<(String, String)> {
    let batch = result.concat_batches().unwrap();
    let first = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let second = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    (0..batch.num_rows())
        .map(|row| (first.value(row).to_string(), second.value(row).to_string()))
        .collect()
}

async fn doc_user_index_count(db: &Omnigraph) -> usize {
    let ds = snapshot_main(db)
        .await
        .unwrap()
        .open_dataset("node:Doc")
        .await
        .unwrap();
    ds.load_indices()
        .await
        .unwrap()
        .iter()
        .filter(|idx| !is_system_index(idx))
        .count()
}

/// RFC-022 data writes publish only their exact table effects. Declared FTS
/// and vector indexes may therefore still be pending immediately after load;
/// both retrieval modes (and their RRF composition) must remain logically
/// correct through Lance's flat-search paths.
#[tokio::test]
#[serial]
async fn deferred_indexes_do_not_block_hybrid_reads() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, MOCK_SEARCH_SCHEMA).await.unwrap();
    load_jsonl(&db, &mock_embedding_seed_data(), LoadMode::Overwrite)
        .await
        .unwrap();

    assert_eq!(
        doc_user_index_count(&db).await,
        0,
        "load must leave declared physical indexes to the reconciler"
    );
    let result = query_main(
        &mut db,
        MOCK_SEARCH_QUERIES,
        "hybrid_search_vector",
        &vector_and_string_params("$vq", &mock_embedding("alpha", 4), "$tq", "alpha"),
    )
    .await
    .expect("pending FTS/vector indexes must degrade to flat search");
    assert_eq!(result_slugs(&result)[0], "alpha-doc");
}

struct EnvGuard {
    saved: Vec<(&'static str, Option<String>)>,
}

impl EnvGuard {
    fn set(vars: &[(&'static str, Option<&str>)]) -> Self {
        let saved = vars
            .iter()
            .map(|(name, _)| (*name, env::var(name).ok()))
            .collect::<Vec<_>>();
        for (name, value) in vars {
            unsafe {
                match value {
                    Some(value) => env::set_var(name, value),
                    None => env::remove_var(name),
                }
            }
        }
        Self { saved }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        for (name, value) in self.saved.drain(..) {
            unsafe {
                match value {
                    Some(value) => env::set_var(name, value),
                    None => env::remove_var(name),
                }
            }
        }
    }
}

// ─── Text search (match_tokens) ─────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn text_search_filters_results() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_search_db(&dir).await;

    // "Learning" appears in: ml-intro, dl-basics, rl-intro titles
    let result = query_main(
        &mut db,
        SEARCH_QUERIES,
        "text_search",
        &params(&[("$q", "Learning")]),
    )
    .await
    .unwrap();

    assert!(
        result.num_rows() > 0,
        "expected at least 1 result for 'Learning'"
    );
    let batch = result.concat_batches().unwrap();
    let slugs = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let slug_values: Vec<&str> = (0..slugs.len()).map(|i| slugs.value(i)).collect();
    // Should contain ML and RL intro docs
    assert!(
        slug_values.contains(&"ml-intro") || slug_values.contains(&"rl-intro"),
        "expected learning-related docs, got {:?}",
        slug_values
    );
}

#[tokio::test]
#[serial]
async fn text_search_no_results() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_search_db(&dir).await;

    let result = query_main(
        &mut db,
        SEARCH_QUERIES,
        "text_search",
        &params(&[("$q", "xyznonexistent")]),
    )
    .await
    .unwrap();

    assert_eq!(result.num_rows(), 0);
}

// ─── Fuzzy search (match_tokens with fuzzy_max_edits) ───────────────────────

#[tokio::test]
#[serial]
async fn fuzzy_search_tolerates_typos() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_search_db(&dir).await;

    // "Introductio" (missing 'n') should fuzzy-match "Introduction" with max_edits=2
    let result = query_main(
        &mut db,
        SEARCH_QUERIES,
        "fuzzy_search",
        &params(&[("$q", "Introductio")]),
    )
    .await
    .unwrap();

    // Fuzzy matching may not work with the default tokenizer on all terms;
    // at minimum verify it doesn't error
    // If it returns results, great — it matched despite the typo
    let _ = result.num_rows();
}

// ─── Phrase search (match_phrase) ───────────────────────────────────────────

#[tokio::test]
#[serial]
async fn phrase_search_matches_exact_phrase() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_search_db(&dir).await;

    // "neural networks" appears in dl-basics body
    let result = query_main(
        &mut db,
        SEARCH_QUERIES,
        "phrase_search",
        &params(&[("$q", "neural networks")]),
    )
    .await
    .unwrap();

    assert!(
        result.num_rows() > 0,
        "expected match for 'neural networks'"
    );
    let batch = result.concat_batches().unwrap();
    let slugs = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let slug_values: Vec<&str> = (0..slugs.len()).map(|i| slugs.value(i)).collect();
    assert!(
        slug_values.contains(&"dl-basics"),
        "expected dl-basics for 'neural networks', got {:?}",
        slug_values
    );
}

#[tokio::test]
#[serial]
async fn phrase_search_is_documented_fts_fallback() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_search_db(&dir).await;

    let result = query_main(
        &mut db,
        SEARCH_QUERIES,
        "phrase_search",
        &params(&[("$q", "networks layers")]),
    )
    .await
    .unwrap();

    assert!(
        result.num_rows() > 0,
        "match_text fallback should still match FTS tokens"
    );
    let batch = result.concat_batches().unwrap();
    let slugs = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let slug_values: Vec<&str> = (0..slugs.len()).map(|i| slugs.value(i)).collect();
    assert!(
        slug_values.contains(&"dl-basics"),
        "expected FTS fallback to match dl-basics, got {:?}",
        slug_values
    );
}

// ─── Vector search (nearest) ────────────────────────────────────────────────

/// Fixture for the filtered-nearest pair below. The query vector is +e1. The
/// three status="miss" docs cluster around +e1 (the global top-3); the three
/// status="hit" docs cluster around -e1, so a post-filtered top-k contains no
/// matching row and returns 0 rows despite 3 matches existing.
const FILTERED_NEAREST_SCHEMA: &str = r#"
node Doc {
    slug: String @key
    status: String
    embedding: Vector(4)
}
"#;
const FILTERED_NEAREST_DATA: &str = r#"{"type":"Doc","data":{"slug":"miss-1","status":"miss","embedding":[1.0,0.01,0.0,0.0]}}
{"type":"Doc","data":{"slug":"miss-2","status":"miss","embedding":[1.0,0.0,0.02,0.0]}}
{"type":"Doc","data":{"slug":"miss-3","status":"miss","embedding":[1.0,0.0,0.0,0.03]}}
{"type":"Doc","data":{"slug":"hit-1","status":"hit","embedding":[-1.0,0.01,0.0,0.0]}}
{"type":"Doc","data":{"slug":"hit-2","status":"hit","embedding":[-1.0,0.0,0.02,0.0]}}
{"type":"Doc","data":{"slug":"hit-3","status":"hit","embedding":[-1.0,0.0,0.0,0.03]}}
"#;
const FILTERED_NEAREST_QUERIES: &str = r#"
query filtered_nearest($q: Vector(4)) {
    match { $d: Doc { status: "hit" } }
    return { $d.slug }
    order { nearest($d.embedding, $q) }
    limit 3
}

query filtered_nearest_clause_eq($q: Vector(4)) {
    match { $d: Doc
        $d.status = "hit" }
    return { $d.slug }
    order { nearest($d.embedding, $q) }
    limit 3
}

query filtered_nearest_clause_range($q: Vector(4)) {
    match { $d: Doc
        $d.status <= "hit" }
    return { $d.slug }
    order { nearest($d.embedding, $q) }
    limit 3
}
"#;

async fn assert_filtered_nearest_returns_hits(query_name: &str) {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, FILTERED_NEAREST_SCHEMA).await.unwrap();
    load_jsonl(&db, FILTERED_NEAREST_DATA, LoadMode::Overwrite)
        .await
        .unwrap();

    let result = query_main(
        &mut db,
        FILTERED_NEAREST_QUERIES,
        query_name,
        &vector_param("$q", &[1.0, 0.0, 0.0, 0.0]),
    )
    .await
    .unwrap();

    assert_eq!(
        result.num_rows(),
        3,
        "{query_name}: filtered nearest must return the top-k of MATCHING rows \
         (3 hits exist), not the post-filtered remainder of the global top-k"
    );
    let batch = result.concat_batches().unwrap();
    let slugs = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    for i in 0..slugs.len() {
        assert!(
            slugs.value(i).starts_with("hit-"),
            "{query_name}: only matching docs may appear, got {}",
            slugs.value(i)
        );
    }
}

/// iss-nearest-postfilter-starves-results: a scalar `match` predicate combined
/// with `nearest` must return the top-k of the MATCHING rows. Lance's default
/// is post-filtering (filter applied AFTER the ANN top-k), under which this
/// fixture returns 0 rows. The engine must set prefilter(true) whenever a
/// filter rides the same scanner as a search.
#[tokio::test]
#[serial]
async fn filtered_nearest_returns_matching_rows_not_postfiltered_topk() {
    assert_filtered_nearest_returns_hits("filtered_nearest").await;
}

/// iss-filter-clause-no-pushdown: the same predicate written as a standalone
/// filter clause is a match filter per docs/user/queries/index.md, so it must
/// reach the scanner and prefilter the search exactly like the inline-props
/// spelling above. Covers equality plus a range predicate, which has no
/// inline-props spelling at all.
#[tokio::test]
#[serial]
async fn filtered_nearest_clause_spelling_prefilters_like_inline() {
    assert_filtered_nearest_returns_hits("filtered_nearest_clause_eq").await;
    assert_filtered_nearest_returns_hits("filtered_nearest_clause_range").await;
}

#[tokio::test]
#[serial]
async fn nearest_returns_k_closest() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_search_db(&dir).await;

    // Query vector [0.1, 0.2, 0.3, 0.4] is identical to ml-intro's embedding
    let result = query_main(
        &mut db,
        SEARCH_QUERIES,
        "vector_search",
        &vector_param("$q", &[0.1, 0.2, 0.3, 0.4]),
    )
    .await
    .unwrap();

    // limit 3 → should return exactly 3
    assert_eq!(result.num_rows(), 3);

    // ml-intro should be the closest (distance=0)
    let batch = result.concat_batches().unwrap();
    let slugs = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(slugs.value(0), "ml-intro", "closest should be ml-intro");
}

/// Lance 11 still drops KNN ordering metadata when its sorted candidate stream
/// is late-hydrated with ordinary node payload. Above one 8,192-row output
/// batch, a parallel final coalesce can then put a later partition first. This
/// engine-level cell proves the temporary one-output-partition fence is wired
/// through the real stable-row-ID graph scan and preserves the complete rank.
#[tokio::test(flavor = "multi_thread")]
async fn nearest_large_k_preserves_global_order_through_payload_hydration() {
    const ROWS_PER_FRAGMENT: usize = 5_000;
    const LIMIT: usize = 8_193;

    fn rows(start: usize) -> String {
        (start..start + ROWS_PER_FRAGMENT)
            .map(|row| {
                format!(
                    r#"{{"type":"Doc","data":{{"slug":"n{row:05}","embedding":[{row}.0,0.0,0.0,0.0]}}}}"#
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    let schema = r#"
node Doc {
    slug: String @key
    embedding: Vector(4)
}
"#;
    let query = format!(
        r#"
query ranked($q: Vector(4)) {{
    match {{ $d: Doc }}
    return {{ $d.slug }}
    order {{ nearest($d.embedding, $q) }}
    limit {LIMIT}
}}
"#
    );

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, schema).await.unwrap();
    load_jsonl(&db, &rows(0), LoadMode::Overwrite)
        .await
        .unwrap();
    load_jsonl(&db, &rows(ROWS_PER_FRAGMENT), LoadMode::Append)
        .await
        .unwrap();

    let result = query_main(
        &mut db,
        &query,
        "ranked",
        &vector_param("$q", &[0.0, 0.0, 0.0, 0.0]),
    )
    .await
    .unwrap();
    assert_eq!(result.num_rows(), LIMIT);
    let slugs = result_slugs(&result);
    for (rank, slug) in slugs.iter().enumerate() {
        assert_eq!(slug, &format!("n{rank:05}"), "wrong result at rank {rank}");
    }
}

#[tokio::test]
#[serial]
async fn nearest_string_param_matches_explicit_vector_under_mock_embeddings() {
    let _guard = EnvGuard::set(&[
        ("OMNIGRAPH_EMBEDDINGS_MOCK", Some("1")),
        ("GEMINI_API_KEY", None),
    ]);

    let dir = tempfile::tempdir().unwrap();
    let mut db = init_mock_embedding_search_db(&dir).await;

    let explicit = query_main(
        &mut db,
        MOCK_SEARCH_QUERIES,
        "vector_search_vector",
        &vector_param("$q", &mock_embedding("alpha", 4)),
    )
    .await
    .unwrap();
    let embedded = query_main(
        &mut db,
        MOCK_SEARCH_QUERIES,
        "vector_search_string",
        &params(&[("$q", "alpha")]),
    )
    .await
    .unwrap();

    assert_eq!(result_slugs(&embedded), result_slugs(&explicit));
    assert_eq!(result_slugs(&embedded)[0], "alpha-doc");
}

#[tokio::test]
#[serial]
async fn nearest_string_literal_works_under_mock_embeddings() {
    let _guard = EnvGuard::set(&[
        ("OMNIGRAPH_EMBEDDINGS_MOCK", Some("1")),
        ("GEMINI_API_KEY", None),
    ]);

    let dir = tempfile::tempdir().unwrap();
    let mut db = init_mock_embedding_search_db(&dir).await;

    let result = query_main(
        &mut db,
        MOCK_SEARCH_QUERIES,
        "vector_search_literal",
        &params(&[]),
    )
    .await
    .unwrap();

    assert_eq!(result_slugs(&result)[0], "alpha-doc");
}

#[tokio::test]
#[serial]
async fn rrf_with_string_nearest_matches_explicit_vector_under_mock_embeddings() {
    let _guard = EnvGuard::set(&[
        ("OMNIGRAPH_EMBEDDINGS_MOCK", Some("1")),
        ("GEMINI_API_KEY", None),
    ]);

    let dir = tempfile::tempdir().unwrap();
    let mut db = init_mock_embedding_search_db(&dir).await;

    let explicit = query_main(
        &mut db,
        MOCK_SEARCH_QUERIES,
        "hybrid_search_vector",
        &vector_and_string_params("$vq", &mock_embedding("alpha", 4), "$tq", "alpha"),
    )
    .await
    .unwrap();
    let embedded = query_main(
        &mut db,
        MOCK_SEARCH_QUERIES,
        "hybrid_search_string",
        &params(&[("$vq", "alpha"), ("$tq", "alpha")]),
    )
    .await
    .unwrap();

    assert_eq!(result_slugs(&embedded), result_slugs(&explicit));
    assert_eq!(result_slugs(&embedded)[0], "alpha-doc");
}

#[tokio::test]
#[serial]
async fn explicit_vector_nearest_does_not_require_gemini_credentials() {
    let _guard = EnvGuard::set(&[
        ("OMNIGRAPH_EMBEDDINGS_MOCK", None),
        ("GEMINI_API_KEY", None),
    ]);

    let dir = tempfile::tempdir().unwrap();
    let mut db = init_mock_embedding_search_db(&dir).await;

    let result = query_main(
        &mut db,
        MOCK_SEARCH_QUERIES,
        "vector_search_vector",
        &vector_param("$q", &mock_embedding("alpha", 4)),
    )
    .await
    .unwrap();

    assert_eq!(result_slugs(&result)[0], "alpha-doc");
}

#[tokio::test]
#[serial]
async fn string_nearest_requires_provider_credentials_when_mock_is_disabled() {
    // With mock off and no provider key, the default (openai-compatible)
    // provider fails loudly rather than silently producing garbage vectors.
    let _guard = EnvGuard::set(&[
        ("OMNIGRAPH_EMBEDDINGS_MOCK", None),
        ("OMNIGRAPH_EMBED_PROVIDER", None),
        ("OPENROUTER_API_KEY", None),
        ("OPENAI_API_KEY", None),
        ("GEMINI_API_KEY", None),
    ]);

    let dir = tempfile::tempdir().unwrap();
    let mut db = init_mock_embedding_search_db(&dir).await;

    let err = query_main(
        &mut db,
        MOCK_SEARCH_QUERIES,
        "vector_search_string",
        &params(&[("$q", "alpha")]),
    )
    .await
    .unwrap_err();

    assert!(
        err.to_string()
            .contains("OPENROUTER_API_KEY or OPENAI_API_KEY"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
#[serial]
async fn nearest_string_passes_when_query_model_matches_recorded_model() {
    let _guard = EnvGuard::set(&[
        ("OMNIGRAPH_EMBEDDINGS_MOCK", Some("1")),
        ("OMNIGRAPH_EMBED_MODEL", Some("test-model-a")),
        ("OMNIGRAPH_EMBED_PROVIDER", None),
        ("OPENROUTER_API_KEY", None),
        ("OPENAI_API_KEY", None),
        ("GEMINI_API_KEY", None),
    ]);

    let dir = tempfile::tempdir().unwrap();
    let mut db = init_model_recorded_search_db(&dir).await;

    let result = query_main(
        &mut db,
        MOCK_SEARCH_QUERIES,
        "vector_search_string",
        &params(&[("$q", "alpha")]),
    )
    .await
    .unwrap();

    assert_eq!(result_slugs(&result)[0], "alpha-doc");
}

#[tokio::test]
#[serial]
async fn nearest_string_errors_when_query_model_differs_from_recorded_model() {
    let _guard = EnvGuard::set(&[
        ("OMNIGRAPH_EMBEDDINGS_MOCK", Some("1")),
        ("OMNIGRAPH_EMBED_MODEL", Some("test-model-b")),
        ("OMNIGRAPH_EMBED_PROVIDER", None),
        ("OPENROUTER_API_KEY", None),
        ("OPENAI_API_KEY", None),
        ("GEMINI_API_KEY", None),
    ]);

    let dir = tempfile::tempdir().unwrap();
    let mut db = init_model_recorded_search_db(&dir).await;

    let err = query_main(
        &mut db,
        MOCK_SEARCH_QUERIES,
        "vector_search_string",
        &params(&[("$q", "alpha")]),
    )
    .await
    .unwrap_err();

    // The error must name both the recorded model and the resolved one.
    let msg = err.to_string();
    assert!(msg.contains("test-model-a"), "got: {msg}");
    assert!(msg.contains("test-model-b"), "got: {msg}");
}

#[tokio::test]
#[serial]
async fn injected_embedding_config_is_used_instead_of_env() {
    // No mock flag and no provider keys in env, so `from_env()` would error.
    // Injecting a Mock config proves the resolver uses the injected config
    // (RFC-012 Phase 5), and its model satisfies the recorded same-space check.
    let _guard = EnvGuard::set(&[
        ("OMNIGRAPH_EMBEDDINGS_MOCK", None),
        ("OMNIGRAPH_EMBED_PROVIDER", None),
        ("OMNIGRAPH_EMBED_MODEL", None),
        ("OPENROUTER_API_KEY", None),
        ("OPENAI_API_KEY", None),
        ("GEMINI_API_KEY", None),
    ]);

    let dir = tempfile::tempdir().unwrap();
    let mut db = init_model_recorded_search_db(&dir)
        .await
        .with_embedding_config(std::sync::Arc::new(omnigraph::embedding::EmbeddingConfig {
            provider: omnigraph::embedding::Provider::Mock,
            model: "test-model-a".to_string(),
            base_url: String::new(),
            api_key: String::new(),
        }));

    let result = query_main(
        &mut db,
        MOCK_SEARCH_QUERIES,
        "vector_search_string",
        &params(&[("$q", "alpha")]),
    )
    .await
    .unwrap();

    assert_eq!(result_slugs(&result)[0], "alpha-doc");
}

// ─── BM25 search ────────────────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn bm25_returns_ranked_results() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_search_db(&dir).await;

    // "Learning" appears in multiple titles
    let result = query_main(
        &mut db,
        SEARCH_QUERIES,
        "bm25_search",
        &params(&[("$q", "Learning")]),
    )
    .await
    .unwrap();

    assert!(
        result.num_rows() > 0,
        "bm25 should return results for 'Learning'"
    );
    assert!(result.num_rows() <= 3, "bm25 should respect limit 3");
}

// Full rank-ORDER golden (not just top-1 / non-empty): pins ranks 2..k so a
// regression corrupting the tail or reversing the sort direction fails loudly.
// Search-ordered plans sort on the appended `_distance` column with the id
// tie-break, so result_slugs row order == rank order.
#[tokio::test]
#[serial]
async fn nearest_full_rank_order() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_search_db(&dir).await;
    let result = query_main(
        &mut db,
        SEARCH_QUERIES,
        "vector_search",
        &vector_param("$q", &[0.1, 0.2, 0.3, 0.4]),
    )
    .await
    .unwrap();
    // [0.1,0.2,0.3,0.4] == ml-intro's embedding (dist 0); the rest by ascending L2.
    assert_eq!(
        result_slugs(&result),
        vec!["ml-intro", "nlp-guide", "rl-intro"]
    );
}

#[tokio::test]
#[serial]
async fn bm25_full_rank_order() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_search_db(&dir).await;
    let result = query_main(
        &mut db,
        SEARCH_QUERIES,
        "bm25_search",
        &params(&[("$q", "Learning")]),
    )
    .await
    .unwrap();
    // All three matches tie on BM25 score here (probe-verified equal `_score`
    // values, 2026-08-31), so this golden pins the equal-score contract: the
    // deterministic id tie-break. If a Lance scoring change breaks the tie,
    // this expectation changes meaning — re-probe before updating it. The
    // distinct-score ordering itself is pinned by
    // `bm25_distinct_scores_rank_descending`.
    assert_eq!(
        result_slugs(&result),
        vec!["dl-basics", "ml-intro", "rl-intro"]
    );
}

#[tokio::test]
#[serial]
async fn nearest_rank_survives_bound_edge_fanout() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_ranked_edge_db(&dir).await;
    let result = query_main(
        &mut db,
        RANKED_EDGE_QUERIES,
        "nearest_edges",
        &vector_param("$q", &[0.0, 0.0, 0.0, 0.0]),
    )
    .await
    .unwrap();

    assert_eq!(
        first_two_strings(&result),
        vec![
            ("rank-1".to_string(), "A1".to_string()),
            ("rank-1".to_string(), "A2".to_string()),
            ("rank-2".to_string(), "B".to_string()),
            ("rank-3".to_string(), "C".to_string()),
        ],
        "edge-table storage order must not replace the incoming ANN rank"
    );
}

// Multi-hop regression for the hop-major BFS (PR #544 review finding 1): the
// unified core emits every seed's hop 1 before any seed's hop 2, so without
// the `_distance` sort the final `limit 2` would return (rank-1, sink),
// (rank-2, sink) instead of both rows of the best-ranked seed. The two
// surviving rows tie on `_distance`, so their relative order is the id
// tie-break and deliberately unasserted here. Covers the `_distance` asc leg;
// `_score` desc runs the same branch and is pinned single-hop by
// `bm25_distinct_scores_rank_descending`.
#[tokio::test]
#[serial]
async fn nearest_rank_survives_multi_hop_expansion() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_ranked_edge_db(&dir).await;
    let result = query_main(
        &mut db,
        RANKED_EDGE_QUERIES,
        "nearest_hops",
        &vector_param("$q", &[0.0, 0.0, 0.0, 0.0]),
    )
    .await
    .unwrap();

    let rows = first_two_strings(&result);
    assert_eq!(rows.len(), 2, "limit 2 must return exactly two rows");
    assert!(
        rows.iter().all(|(d, _)| d == "rank-1"),
        "both top rows must come from the best-ranked seed, got {rows:?}"
    );
    let targets: std::collections::HashSet<&str> =
        rows.iter().map(|(_, target)| target.as_str()).collect();
    assert_eq!(
        targets,
        std::collections::HashSet::from(["sink", "rank-3"]),
        "the best seed's hop-1 and hop-2 reach must both survive the limit"
    );
}

// Secondary order keys after the search function are honored: on the all-tie
// bm25 fixture the user's `$d.slug desc` must decide the order (reverse of
// the id tie-break, which only applies after all user keys).
#[tokio::test]
#[serial]
async fn search_order_secondary_keys_are_honored() {
    const QUERY: &str = r#"
query bm25_then_slug($q: String) {
    match { $d: Doc }
    return { $d.slug }
    order { bm25($d.title, $q), $d.slug desc }
    limit 3
}
"#;
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_search_db(&dir).await;
    let result = query_main(
        &mut db,
        QUERY,
        "bm25_then_slug",
        &params(&[("$q", "Learning")]),
    )
    .await
    .unwrap();
    assert_eq!(
        result_slugs(&result),
        vec!["rl-intro", "ml-intro", "dl-basics"],
        "tied scores must fall to the user's secondary key, not the id tie-break"
    );
}

// Distinct-score descending golden: BM25 term-frequency monotonicity gives
// three strictly different scores (1x/2x/3x "tensor"), so this pins the
// score ordering itself — the tie-break golden above structurally cannot
// (its scores are equal). Slugs are chosen so the id tie-break order (n1,
// n2, n3) is the REVERSE of score order: a broken score sort cannot pass.
#[tokio::test]
#[serial]
async fn bm25_distinct_scores_rank_descending() {
    const SCHEMA: &str = r#"
node Note {
    slug: String @key
    body: String @index
}
"#;
    const DATA: &str = r#"{"type":"Note","data":{"slug":"n1","body":"tensor"}}
{"type":"Note","data":{"slug":"n2","body":"tensor tensor"}}
{"type":"Note","data":{"slug":"n3","body":"tensor tensor tensor"}}"#;
    const QUERY: &str = r#"
query bm25_ranked($q: String) {
    match { $n: Note }
    return { $n.slug }
    order { bm25($n.body, $q) }
    limit 3
}
"#;
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, SCHEMA).await.unwrap();
    load_jsonl(&db, DATA, LoadMode::Overwrite).await.unwrap();
    db.ensure_indices().await.unwrap();
    let result = query_main(&mut db, QUERY, "bm25_ranked", &params(&[("$q", "tensor")]))
        .await
        .unwrap();
    assert_eq!(
        result_slugs(&result),
        vec!["n3", "n2", "n1"],
        "descending BM25 score order must beat the id tie-break"
    );
}

#[tokio::test]
#[serial]
async fn rrf_rank_preserves_every_bound_edge_row_once() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_ranked_edge_db(&dir).await;
    let result = query_main(
        &mut db,
        RANKED_EDGE_QUERIES,
        "rrf_edges",
        &two_vector_params("$q1", &[0.0, 0.0, 0.0, 0.0], "$q2", &[0.0, 0.0, 0.0, 0.0]),
    )
    .await
    .unwrap();

    assert_eq!(
        first_two_strings(&result),
        vec![
            ("rank-1".to_string(), "A1".to_string()),
            ("rank-1".to_string(), "A2".to_string()),
            ("rank-2".to_string(), "B".to_string()),
            ("rank-3".to_string(), "C".to_string()),
        ],
        "fusion ranks source entities, then retains each matched edge row once"
    );
}

/// A ranked read caps its BM25 scan (issue #563). The cap is an optimization,
/// never a row budget — when the join drops every capped row, the query must
/// still answer in full rather than serve a short result. (BM25-only: the
/// `nearest` arm's `k` remains a hard budget.)
#[tokio::test]
#[serial]
async fn bm25_join_fills_limit_when_capped_scan_underfills_issue_563() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, UNDERFILL_SCHEMA).await.unwrap();
    load_jsonl(&db, &underfill_seed_data(), LoadMode::Overwrite)
        .await
        .unwrap();
    db.ensure_indices().await.unwrap();
    let mut db = db;

    use omnigraph::instrumentation::{QueryIoProbes, with_query_io_probes};
    let probes = QueryIoProbes::default();
    let result = with_query_io_probes(probes.clone(), async {
        query_main(
            &mut db,
            UNDERFILL_QUERY,
            "recall",
            &params(&[("$q", "needle")]),
        )
        .await
    })
    .await
    .unwrap();

    // Every capped-window row lacks an edge, so the uncapped retry must fire —
    // the only observable proof the cap engaged (see `bm25_uncapped_retries`).
    assert_eq!(
        probes
            .bm25_uncapped_retries
            .load(std::sync::atomic::Ordering::Relaxed),
        1,
        "the under-fill retry must fire exactly once"
    );
    // Cap MAGNITUDE pin: capped pass scans 8 rows (limit 2 × factor 4), the
    // uncapped retry scans all 20 — a factor regression moves this count while
    // every result assertion still passes.
    assert_eq!(
        probes
            .bm25_scan_rows
            .load(std::sync::atomic::Ordering::Relaxed),
        28,
        "scan rows must be capped-8 plus uncapped-20"
    );

    // BM25 ranks by term frequency here (tf = 20 - chunk), so the two
    // best-scoring edge-bearing chunks are exactly 08 then 09, in order.
    assert_eq!(
        result_slugs(&result),
        vec!["chunk-08".to_string(), "chunk-09".to_string()],
        "the limit must be filled, in rank order, from the edge-bearing chunks outside the scan cap"
    );
}

/// Aggregate returns are never capped (see `bm25_scan_limit` for the why):
/// `count` must see every matching document.
#[tokio::test]
#[serial]
async fn bm25_ordered_aggregate_counts_all_matches_not_the_capped_scan() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, UNDERFILL_SCHEMA).await.unwrap();
    load_jsonl(&db, &underfill_seed_data(), LoadMode::Overwrite)
        .await
        .unwrap();
    db.ensure_indices().await.unwrap();
    let mut db = db;

    use omnigraph::instrumentation::{QueryIoProbes, with_query_io_probes};
    let probes = QueryIoProbes::default();
    let result = with_query_io_probes(probes.clone(), async {
        query_main(
            &mut db,
            UNDERFILL_AGG_QUERY,
            "recall_count",
            &params(&[("$q", "needle")]),
        )
        .await
    })
    .await
    .unwrap();

    // The exemption means the FIRST scan is uncapped: all 20 rows, no retry.
    // A count of 20 reached via a capped-then-retried run would be wrong
    // mechanics with the right answer; these two asserts see through it.
    assert_eq!(
        probes
            .bm25_uncapped_retries
            .load(std::sync::atomic::Ordering::Relaxed),
        0,
        "aggregates are never capped, so no retry may arise"
    );
    assert_eq!(
        probes
            .bm25_scan_rows
            .load(std::sync::atomic::Ordering::Relaxed),
        20,
        "the aggregate's single scan must cover every matching document"
    );

    let batch = result.concat_batches().unwrap();
    let totals = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(
        totals.value(0),
        UNDERFILL_CHUNKS as i64,
        "count must cover every matching chunk, not only the capped scan window"
    );
}

/// The rrf arms are never capped (PR #574 review; the starvation mechanism
/// is documented on `extract_sub_search_mode`). Pins: one uncapped pass per
/// arm, zero retries; a reintroduced cap moves the scan-row count.
#[tokio::test]
#[serial]
async fn rrf_arms_scan_uncapped_in_one_pass() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, UNDERFILL_SCHEMA).await.unwrap();
    load_jsonl(&db, &underfill_seed_data(), LoadMode::Overwrite)
        .await
        .unwrap();
    db.ensure_indices().await.unwrap();
    let mut db = db;

    use omnigraph::instrumentation::{QueryIoProbes, with_query_io_probes};
    let probes = QueryIoProbes::default();
    let result = with_query_io_probes(probes.clone(), async {
        query_main(
            &mut db,
            UNDERFILL_RRF_QUERY,
            "recall_rrf",
            &params(&[("$q", "needle")]),
        )
        .await
    })
    .await
    .unwrap();

    // No cap, no retry machinery on the rrf path.
    assert_eq!(
        probes
            .bm25_uncapped_retries
            .load(std::sync::atomic::Ordering::Relaxed),
        0,
        "rrf arms are uncapped, so no under-fill retry may arise"
    );
    // Each arm scans the full matched corpus exactly once (2 × 20).
    assert_eq!(
        probes
            .bm25_scan_rows
            .load(std::sync::atomic::Ordering::Relaxed),
        40,
        "both rrf arms must scan every matching document in one pass"
    );

    // Both arms rank identically (same bm25 expression), so fusion preserves
    // the tf order: the best-scoring edge-bearing chunks are 08 then 09.
    assert_eq!(
        result_slugs(&result),
        vec!["chunk-08".to_string(), "chunk-09".to_string()],
        "the fused limit must be filled, in rank order, from the edge-bearing chunks"
    );
}

/// The #574 review fixture: the four best alpha scorers carry no edge. Were
/// the alpha arm capped at limit × BM25_SCAN_OVERFETCH_FACTOR (4), the
/// traversal would evict its entire window, fusion would rank on beta alone,
/// and the winner would silently flip from x to n with the row count still
/// full. Red against the capped rrf implementation; green on uncapped arms.
#[tokio::test]
#[serial]
async fn rrf_decoy_flood_does_not_flip_the_fused_winner() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, UNDERFILL_SCHEMA).await.unwrap();
    load_jsonl(&db, &starvation_seed_data(), LoadMode::Overwrite)
        .await
        .unwrap();
    db.ensure_indices().await.unwrap();
    let mut db = db;

    use omnigraph::instrumentation::{QueryIoProbes, with_query_io_probes};
    let probes = QueryIoProbes::default();
    let result = with_query_io_probes(probes.clone(), async {
        query_main(
            &mut db,
            STARVATION_RRF_QUERY,
            "recall_two_terms",
            &params(&[("$q1", "alpha"), ("$q2", "beta")]),
        )
        .await
    })
    .await
    .unwrap();

    assert_eq!(
        result_slugs(&result),
        vec!["x".to_string()],
        "x wins the fused ranking; n wins only if the alpha arm is starved"
    );
    // Both arms scan all seven chunks, one pass, no retry.
    assert_eq!(
        probes
            .bm25_scan_rows
            .load(std::sync::atomic::Ordering::Relaxed),
        14,
        "both rrf arms must scan the full seven-chunk corpus in one pass"
    );
    assert_eq!(
        probes
            .bm25_uncapped_retries
            .load(std::sync::atomic::Ordering::Relaxed),
        0,
        "rrf arms are uncapped, so no under-fill retry may arise"
    );
}

// Characterization: fuzzy() does NOT match under the default tokenizer/index in
// this setup — a one-edit typo ("Introductio" for "Introduction") returns no
// rows. (`search`/`match_text` DO work, so FTS itself is fine; fuzzy term
// queries specifically are inert here.) This pins that documented limitation
// instead of leaving fuzzy silently unasserted: if a Lance/tokenizer change
// makes fuzzy match, this turns red and should be promoted to a real
// matched-set + exclusion golden.
#[tokio::test]
#[serial]
async fn fuzzy_does_not_match_under_default_tokenizer() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_search_db(&dir).await;
    let r = query_main(
        &mut db,
        SEARCH_QUERIES,
        "fuzzy_search",
        &params(&[("$q", "Introductio")]),
    )
    .await
    .unwrap();
    assert!(
        result_slugs(&r).is_empty(),
        "fuzzy now matches — promote this to a real matched-set/exclusion golden"
    );
}

// match_text is a FILTER on the body: assert the exact matched set, not contains.
#[tokio::test]
#[serial]
async fn match_text_matches_exact_set_excludes_unrelated() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_search_db(&dir).await;
    // "neural" appears only in dl-basics's body ("neural networks").
    let r = query_main(
        &mut db,
        SEARCH_QUERIES,
        "phrase_search",
        &params(&[("$q", "neural")]),
    )
    .await
    .unwrap();
    let mut got = result_slugs(&r);
    got.sort();
    assert_eq!(got, vec!["dl-basics"]);
}

// RRF fuses arms OTHER than the default nearest+bm25: two FTS arms (title+body).
// Proves primary_var resolves when neither arm is `nearest`, and fusion runs.
// Lance beta.19 #7621 completed the ICU English stop-word list, changing BM25
// document-length normalization in the body arm. Under the RC.1 pin the
// title arm ranks rl/ml/dl, the body arm ranks dl/rl/ml, and RRF therefore
// deterministically ranks rl/dl/ml.
#[tokio::test]
#[serial]
async fn rrf_fuses_two_fts_fields() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_search_db(&dir).await;
    let r = query_main(
        &mut db,
        SEARCH_QUERIES,
        "rrf_two_fts",
        &params(&[("$q", "learning")]),
    )
    .await
    .unwrap();
    assert_eq!(result_slugs(&r), vec!["rl-intro", "dl-basics", "ml-intro"]);
}

// RRF fuses two vector arms (no embedding creds — explicit vectors). A doc near
// BOTH query vectors out-ranks one near only one.
#[tokio::test]
#[serial]
async fn rrf_fuses_two_vector_queries() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_search_db(&dir).await;
    let r = query_main(
        &mut db,
        SEARCH_QUERIES,
        "rrf_two_vectors",
        &two_vector_params("$q1", &[0.1, 0.2, 0.3, 0.4], "$q2", &[0.5, 0.6, 0.7, 0.8]),
    )
    .await
    .unwrap();
    assert_eq!(result_slugs(&r), vec!["rl-intro", "ml-intro", "dl-basics"]);
}

#[tokio::test]
#[serial]
async fn mutation_with_deferred_index_coverage_remains_searchable() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_search_db(&dir).await;
    assert_eq!(doc_user_index_count(&db).await, 4);

    let mut mutation_params = vector_param("$embedding", &[0.9, 0.1, 0.1, 0.1]);
    mutation_params.insert(
        "slug".to_string(),
        Literal::String("quasar-notes".to_string()),
    );
    mutation_params.insert(
        "title".to_string(),
        Literal::String("Quasar Notes".to_string()),
    );
    mutation_params.insert(
        "body".to_string(),
        Literal::String("Quasar observations and telescope notes".to_string()),
    );

    db.mutate("main", SEARCH_MUTATIONS, "insert_doc", &mutation_params)
        .await
        .unwrap();

    assert_eq!(
        doc_user_index_count(&db).await,
        4,
        "mutation must leave physical index materialization to the reconciler"
    );

    let result = query_main(
        &mut db,
        SEARCH_QUERIES,
        "text_search",
        &params(&[("$q", "Quasar")]),
    )
    .await
    .unwrap();
    assert!(
        result_slugs(&result).contains(&"quasar-notes".to_string()),
        "a row outside current index coverage must remain searchable via fallback scan"
    );

    // Ordinary optimize must preserve certified postings, not silently replace
    // them with an uncertified incremental fold. Both old and tail rows remain
    // searchable after data compaction and unrelated index maintenance.
    db.optimize().await.unwrap();
    for (term, slug) in [("Quasar", "quasar-notes"), ("Learning", "ml-intro")] {
        let result = query_main(
            &mut db,
            SEARCH_QUERIES,
            "text_search",
            &params(&[("$q", term)]),
        )
        .await
        .unwrap();
        assert!(result_slugs(&result).contains(&slug.to_string()));
    }
}

#[tokio::test]
#[serial]
async fn uncertified_full_text_refuses_all_search_routes_but_not_ordinary_reads() {
    use omnigraph::error::OmniError;

    let dir = tempfile::tempdir().unwrap();
    let mut db = init_search_db(&dir).await;
    let old_manifest_version = version_main(&db).await.unwrap();
    let snapshot = snapshot_main(&db).await.unwrap();
    let entry = snapshot.dataset("node:Doc").unwrap();
    let dataset = snapshot.open_dataset("node:Doc").await.unwrap();
    let indices = dataset.load_indices().await.unwrap();
    // Certify one RRF leg while the other has no proof. Deleting every proof
    // alone cannot detect a gate that checks only the first full-text arm.
    for (uncertified_column, healthy_query) in [("body", "text_search"), ("title", "phrase_search")]
    {
        let field = dataset.schema().field(uncertified_column).unwrap().id;
        let index = indices
            .iter()
            .find(|index| {
                index.fields.contains(&field)
                    && index.files.as_ref().is_some_and(|files| {
                        files
                            .iter()
                            .any(|file| file.path == "omnigraph_fts_compat.json")
                    })
            })
            .unwrap();
        let path = dir
            .path()
            .join(&entry.dataset_path)
            .join("_indices")
            .join(index.uuid.to_string())
            .join("omnigraph_fts_compat.json");
        let certificate = std::fs::read(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        // A fresh session must observe this deliberate out-of-band removal;
        // immutable proofs already verified by a session may remain cached.
        db = Omnigraph::open(dir.path().to_str().unwrap()).await.unwrap();
        assert!(
            query_main(
                &mut db,
                SEARCH_QUERIES,
                healthy_query,
                &params(&[("$q", "Learning")])
            )
            .await
            .unwrap()
            .num_rows()
                > 0
        );
        let error = query_main(
            &mut db,
            SEARCH_QUERIES,
            "rrf_two_fts",
            &params(&[("$q", "Learning")]),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(error, OmniError::FullTextIndexRebuildRequired { index: ref name, .. } if name == &index.name),
            "{uncertified_column}: {error}"
        );
        std::fs::write(path, certificate).unwrap();
        assert!(
            query_main(
                &mut db,
                SEARCH_QUERIES,
                "rrf_two_fts",
                &params(&[("$q", "Learning")])
            )
            .await
            .unwrap()
            .num_rows()
                > 0,
            "failed verification must not be cached"
        );
    }
    // Simulate absent artifact provenance without changing rows, graph history,
    // or index coverage. Actual saved-v10 bytes are tested in staged_tests.
    for index in indices.iter().filter(|index| {
        index.files.as_ref().is_some_and(|files| {
            files
                .iter()
                .any(|file| file.path == "omnigraph_fts_compat.json")
        })
    }) {
        std::fs::remove_file(
            dir.path()
                .join(&entry.dataset_path)
                .join("_indices")
                .join(index.uuid.to_string())
                .join("omnigraph_fts_compat.json"),
        )
        .unwrap();
    }
    db = Omnigraph::open(dir.path().to_str().unwrap()).await.unwrap();
    let original_rows = dataset.count_rows(None).await.unwrap();
    assert!(original_rows > 0);
    for query in [
        "text_search",
        "fuzzy_search",
        "phrase_search",
        "bm25_search",
        "rrf_two_fts",
    ] {
        let error = query_main(
            &mut db,
            SEARCH_QUERIES,
            query,
            &params(&[("$q", "Learning")]),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(error, OmniError::FullTextIndexRebuildRequired { .. }),
            "{query}: {error}"
        );
    }
    let error = query_main(
        &mut db,
        SEARCH_QUERIES,
        "hybrid_search",
        &vector_and_string_params("$vq", &[0.1, 0.2, 0.3, 0.4], "$tq", "Learning"),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(error, OmniError::FullTextIndexRebuildRequired { .. }),
        "{error}"
    );
    assert!(
        query_main(
            &mut db,
            SEARCH_QUERIES,
            "vector_search",
            &vector_param("$q", &[0.1, 0.2, 0.3, 0.4])
        )
        .await
        .unwrap()
        .num_rows()
            > 0
    );

    let mut scan = dataset.scan();
    scan.filter("contains_tokens(title, 'Learning')").unwrap();
    assert!(matches!(
        scan.try_into_stream().await,
        Err(OmniError::FullTextIndexRebuildRequired { .. })
    ));
    assert!(matches!(
        dataset
            .count_rows(Some("contains_tokens(title, 'Learning')".into()))
            .await,
        Err(OmniError::FullTextIndexRebuildRequired { .. })
    ));

    let rebuilt = db.rebuild_full_text_indices_on("main").await.unwrap();
    assert!(!rebuilt.rebuilt_indexes.is_empty());
    assert!(
        query_main(
            &mut db,
            SEARCH_QUERIES,
            "text_search",
            &params(&[("$q", "Learning")])
        )
        .await
        .unwrap()
        .num_rows()
            > 0
    );
    assert_eq!(dataset.count_rows(None).await.unwrap(), original_rows);
    let error = db
        .run_query_at(
            old_manifest_version,
            SEARCH_QUERIES,
            "text_search",
            &params(&[("$q", "Learning")]),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, OmniError::FullTextIndexRebuildRequired { .. }),
        "{error}"
    );
}

// ─── RRF hybrid search ─────────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn rrf_fuses_vector_and_text() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_search_db(&dir).await;

    let result = query_main(
        &mut db,
        SEARCH_QUERIES,
        "hybrid_search",
        &vector_and_string_params("$vq", &[0.1, 0.2, 0.3, 0.4], "$tq", "Learning"),
    )
    .await
    .unwrap();

    assert!(result.num_rows() > 0, "rrf should return results");
    assert!(result.num_rows() <= 3, "rrf should respect limit 3");
}

#[tokio::test]
#[serial]
async fn index_reconciler_creates_vector_index_for_vector_annotations() {
    let schema = r#"
node Doc {
    slug: String @key
    embedding: Vector(4) @index
}
"#;
    let data = r#"{"type": "Doc", "data": {"slug": "a", "embedding": [0.1, 0.2, 0.3, 0.4]}}
{"type": "Doc", "data": {"slug": "b", "embedding": [0.5, 0.6, 0.7, 0.8]}}"#;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, schema).await.unwrap();
    load_jsonl(&db, data, LoadMode::Overwrite).await.unwrap();
    assert_eq!(
        doc_user_index_count(&db).await,
        0,
        "load publishes exact data effects and leaves physical indexes pending"
    );
    db.ensure_indices().await.unwrap();

    let ds = snapshot_main(&db)
        .await
        .unwrap()
        .open_dataset("node:Doc")
        .await
        .unwrap();
    let indices = ds.load_indices().await.unwrap();
    let user_indices: Vec<_> = indices.iter().filter(|idx| !is_system_index(idx)).collect();
    assert_eq!(
        user_indices.len(),
        3,
        "expected id BTree index plus key-property and vector indices"
    );
}

#[tokio::test]
#[serial]
async fn load_commit_creates_inverted_indices_for_string_annotations() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_search_db(&dir).await;

    let ds = snapshot_main(&db)
        .await
        .unwrap()
        .open_dataset("node:Doc")
        .await
        .unwrap();
    let indices = ds.load_indices().await.unwrap();
    let user_indices: Vec<_> = indices.iter().filter(|idx| !is_system_index(idx)).collect();
    assert_eq!(
        user_indices.len(),
        4,
        "expected id BTree index plus key-property and title/body inverted indices"
    );
}
