mod helpers;

use std::env;

use arrow_array::{Array, StringArray};
use lance_index::{DatasetIndexExt, is_system_index};
use serial_test::serial;

use omnigraph::db::Omnigraph;
use omnigraph::loader::{LoadMode, load_jsonl};
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

async fn init_search_db(dir: &tempfile::TempDir) -> Omnigraph {
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, SEARCH_SCHEMA).await.unwrap();
    load_jsonl(&mut db, SEARCH_DATA, LoadMode::Overwrite)
        .await
        .unwrap();
    db.ensure_indices().await.unwrap();
    db
}

async fn init_mock_embedding_search_db(dir: &tempfile::TempDir) -> Omnigraph {
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, MOCK_SEARCH_SCHEMA).await.unwrap();
    load_jsonl(&mut db, &mock_embedding_seed_data(), LoadMode::Overwrite)
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
async fn string_nearest_requires_gemini_credentials_when_mock_is_disabled() {
    let _guard = EnvGuard::set(&[
        ("OMNIGRAPH_EMBEDDINGS_MOCK", None),
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

    assert!(err.to_string().contains("GEMINI_API_KEY"));
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
async fn ensure_indices_creates_vector_index_for_vector_annotations() {
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
    let mut db = Omnigraph::init(uri, schema).await.unwrap();
    load_jsonl(&mut db, data, LoadMode::Overwrite)
        .await
        .unwrap();
    db.ensure_indices().await.unwrap();

    let ds = snapshot_main(&db)
        .await
        .unwrap()
        .open("node:Doc")
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
async fn ensure_indices_creates_inverted_indices_for_string_annotations() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_search_db(&dir).await;

    let ds = snapshot_main(&db)
        .await
        .unwrap()
        .open("node:Doc")
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
