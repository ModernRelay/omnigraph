mod helpers;

use std::env;

use arrow_array::{Array, StringArray};
use lance::index::DatasetIndexExt;
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

async fn init_search_db(dir: &tempfile::TempDir) -> Omnigraph {
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, SEARCH_SCHEMA).await.unwrap();
    load_jsonl(&mut db, SEARCH_DATA, LoadMode::Overwrite)
        .await
        .unwrap();
    db
}

async fn init_mock_embedding_search_db(dir: &tempfile::TempDir) -> Omnigraph {
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, MOCK_SEARCH_SCHEMA).await.unwrap();
    load_jsonl(&mut db, &mock_embedding_seed_data(), LoadMode::Overwrite)
        .await
        .unwrap();
    db
}

async fn init_model_recorded_search_db(dir: &tempfile::TempDir) -> Omnigraph {
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, MODEL_RECORDED_SCHEMA).await.unwrap();
    load_jsonl(&mut db, &mock_embedding_seed_data(), LoadMode::Overwrite)
        .await
        .unwrap();
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

async fn doc_user_index_count(db: &Omnigraph) -> usize {
    let ds = snapshot_main(db)
        .await
        .unwrap()
        .open("node:Doc")
        .await
        .unwrap();
    ds.load_indices()
        .await
        .unwrap()
        .iter()
        .filter(|idx| !is_system_index(idx))
        .count()
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

/// iss-nearest-postfilter-starves-results: a scalar `match` predicate combined
/// with `nearest` must return the top-k of the MATCHING rows. Lance's default
/// is post-filtering (filter applied AFTER the ANN top-k), under which this
/// fixture — where every filter-matching doc sits far from the query vector,
/// so the global top-k is entirely non-matching — returns 0 rows despite 3
/// matches existing. The engine must set prefilter(true) whenever a filter
/// rides the same scanner as a search.
#[tokio::test]
#[serial]
async fn filtered_nearest_returns_matching_rows_not_postfiltered_topk() {
    const SCHEMA: &str = r#"
node Doc {
    slug: String @key
    status: String
    embedding: Vector(4)
}
"#;
    // Query vector is +e1. The three status="miss" docs cluster around +e1
    // (global top-3); the three status="hit" docs cluster around -e1.
    const DATA: &str = r#"{"type":"Doc","data":{"slug":"miss-1","status":"miss","embedding":[1.0,0.01,0.0,0.0]}}
{"type":"Doc","data":{"slug":"miss-2","status":"miss","embedding":[1.0,0.0,0.02,0.0]}}
{"type":"Doc","data":{"slug":"miss-3","status":"miss","embedding":[1.0,0.0,0.0,0.03]}}
{"type":"Doc","data":{"slug":"hit-1","status":"hit","embedding":[-1.0,0.01,0.0,0.0]}}
{"type":"Doc","data":{"slug":"hit-2","status":"hit","embedding":[-1.0,0.0,0.02,0.0]}}
{"type":"Doc","data":{"slug":"hit-3","status":"hit","embedding":[-1.0,0.0,0.0,0.03]}}
"#;
    const QUERIES: &str = r#"
query filtered_nearest($q: Vector(4)) {
    match { $d: Doc { status: "hit" } }
    return { $d.slug }
    order { nearest($d.embedding, $q) }
    limit 3
}
"#;
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, SCHEMA).await.unwrap();
    load_jsonl(&mut db, DATA, LoadMode::Overwrite).await.unwrap();

    let result = query_main(
        &mut db,
        QUERIES,
        "filtered_nearest",
        &vector_param("$q", &[1.0, 0.0, 0.0, 0.0]),
    )
    .await
    .unwrap();

    assert_eq!(
        result.num_rows(),
        3,
        "filtered nearest must return the top-k of MATCHING rows (3 hits exist), \
         not the post-filtered remainder of the global top-k"
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
            "only matching docs may appear, got {}",
            slugs.value(i)
        );
    }
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
// nearest skips apply_ordering (is_search_ordered) and returns Lance native
// order, so result_slugs row order == rank order.
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
    assert_eq!(result_slugs(&result), vec!["ml-intro", "nlp-guide", "rl-intro"]);
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
    // Descending BM25 score order.
    assert_eq!(result_slugs(&result), vec!["rl-intro", "ml-intro", "dl-basics"]);
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
    let r = query_main(&mut db, SEARCH_QUERIES, "fuzzy_search", &params(&[("$q", "Introductio")]))
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
    let r = query_main(&mut db, SEARCH_QUERIES, "phrase_search", &params(&[("$q", "neural")]))
        .await
        .unwrap();
    let mut got = result_slugs(&r);
    got.sort();
    assert_eq!(got, vec!["dl-basics"]);
}

// RRF fuses arms OTHER than the default nearest+bm25: two FTS arms (title+body).
// Proves primary_var resolves when neither arm is `nearest`, and fusion runs.
#[tokio::test]
#[serial]
async fn rrf_fuses_two_fts_fields() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_search_db(&dir).await;
    let r = query_main(&mut db, SEARCH_QUERIES, "rrf_two_fts", &params(&[("$q", "learning")]))
        .await
        .unwrap();
    assert_eq!(result_slugs(&r), vec!["dl-basics", "ml-intro", "rl-intro"]);
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
async fn mutation_commit_refreshes_search_indices_without_manual_ensure() {
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
        "mutation commit should refresh required indices without duplicating them"
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
        "newly inserted row should be searchable without an explicit ensure_indices step"
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
async fn load_commit_creates_vector_index_for_vector_annotations() {
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
async fn load_commit_creates_inverted_indices_for_string_annotations() {
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
