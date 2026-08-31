// ModernRelay/omnigraph#563: an unbounded bm25-ranked read that also traverses
// an edge materializes the joined variable-width column for the whole matched
// corpus before `limit` prunes; past Arrow's i32 offset ceiling (2 GiB per
// Utf8 column) the query fails with an Offset overflow error. The BM25 scan
// cap bounds it; this test pins the fixed behavior at overflow scale.
//
// Geometry: 6,000 Chunk rows x ~200 KB text = ~1.2 GiB of matched text (under
// the 2 GiB ceiling, so the scan-side concat survives and the no-join shape
// returns), each chunk linked to 2 artifacts, so the join's take
// re-materializes ~2.4 GiB of text (over the ceiling).
//
// Expensive (loads ~1.2 GiB, builds an inverted index over it): run explicitly
//   cargo test -p omnigraph-engine --test repro_issue_563 -- --ignored --nocapture
//
// This is the SYMPTOM tier: proof at overflow scale, run at explicit
// checkpoints. The always-on MECHANISM tier lives in tests/search.rs: the
// capped-scan tests (bm25 under-fill + aggregate exemption, 20-row corpus)
// and the rrf uncapped-arm pins (20- and 7-row corpora).
//
// A second #[ignore]d test below times the join-free ranked read over the
// same corpus geometry: the scan cap's cost is hydration count, so the
// timing scales with matched rows (measured 2026-08-29, warm debug builds:
// 119 ms median on the pre-fix parent commit vs 2 ms with the cap).

mod helpers;

use std::time::Instant;

use arrow_array::StringArray;

use omnigraph::db::Omnigraph;
use omnigraph::loader::{LoadMode, load_jsonl};

use helpers::*;

const CHUNKS: usize = 6_000;
const ARTIFACTS: usize = 750;
const FANOUT: usize = 2; // artifacts per chunk
const TEXT_BYTES: usize = 200 * 1024;
const LOAD_BATCH_ROWS: usize = 64; // 64 x ~200 KB stays under the 32 MiB write cap

const SCHEMA: &str = r#"
node Chunk {
    slug: String @key
    text: String @index
}

node Artifact {
    slug: String @key
    name: String
}

edge ChunkOfArtifact: Chunk -> Artifact {
    label: String
}
"#;

const RANKED_JOIN_QUERY: &str = r#"
query recall_join($q: String) {
    match {
        $c: Chunk
        $c chunkOfArtifact $a
        search($c.text, $q)
    }
    return { $c.slug, $a.slug, $a.name }
    order { bm25($c.text, $q) }
    limit 20
}

query recall_no_join($q: String) {
    match {
        $c: Chunk
        search($c.text, $q)
    }
    return { $c.slug }
    order { bm25($c.text, $q) }
    limit 20
}
"#;

fn filler_block() -> String {
    // Tiny vocabulary of long tokens keeps the inverted-index term dictionary
    // small while the stored column carries full byte weight.
    let words = [
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "cccccccccccccccccccccccccccccccc",
        "dddddddddddddddddddddddddddddddd",
        "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
        "ffffffffffffffffffffffffffffffff",
        "gggggggggggggggggggggggggggggggg",
        "hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh",
    ];
    let unit = words.join(" ") + " ";
    let mut out = String::with_capacity(TEXT_BYTES + unit.len());
    while out.len() < TEXT_BYTES {
        out.push_str(&unit);
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "expensive: ~1.2 GiB corpus + inverted index build"]
async fn ranked_read_with_join_returns_top_limit_issue_563() {
    let filler = filler_block();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, SCHEMA).await.unwrap();

    // Artifacts + edges first (small), then the chunk corpus in capped batches.
    let mut head = String::new();
    for a in 0..ARTIFACTS {
        head.push_str(&format!(
            "{{\"type\":\"Artifact\",\"data\":{{\"slug\":\"art-{a:04}\",\"name\":\"Artifact {a}\"}}}}\n"
        ));
    }
    load_jsonl(&db, &head, LoadMode::Overwrite).await.unwrap();

    let mut chunk_batch = String::new();
    for c in 0..CHUNKS {
        chunk_batch.push_str(&format!(
            "{{\"type\":\"Chunk\",\"data\":{{\"slug\":\"chunk-{c:05}\",\"text\":\"needle563 {filler}\"}}}}\n"
        ));
        if (c + 1) % LOAD_BATCH_ROWS == 0 || c + 1 == CHUNKS {
            load_jsonl(&db, &chunk_batch, LoadMode::Append)
                .await
                .unwrap();
            chunk_batch.clear();
        }
    }

    let mut edges = String::new();
    let mut edge_rows = 0usize;
    for c in 0..CHUNKS {
        for f in 0..FANOUT {
            let a = (c * FANOUT + f) % ARTIFACTS;
            edges.push_str(&format!(
                "{{\"edge\":\"ChunkOfArtifact\",\"from\":\"chunk-{c:05}\",\"to\":\"art-{a:04}\",\"data\":{{\"id\":\"e-{c:05}-{f}\",\"label\":\"of\"}}}}\n"
            ));
            edge_rows += 1;
            // 8,192-keyed-entity write cap: flush well under it.
            if edge_rows == 4_000 {
                load_jsonl(&db, &edges, LoadMode::Append).await.unwrap();
                edges.clear();
                edge_rows = 0;
            }
        }
    }
    if edge_rows > 0 {
        load_jsonl(&db, &edges, LoadMode::Append).await.unwrap();
    }

    db.ensure_indices().await.unwrap();

    // The ranked read without the join stays under the i32 offset ceiling.
    let no_join = query_main(
        &mut db,
        RANKED_JOIN_QUERY,
        "recall_no_join",
        &params(&[("$q", "needle563")]),
    )
    .await
    .unwrap();
    assert_eq!(no_join.num_rows(), 20);

    // The same ranked read with the join must also return the top-limit rows.
    // Unbounded (issue #563, pre-cap), it materialized matched text x fanout
    // through the join and failed with "Offset overflow error: 2147489268".
    let joined = query_main(
        &mut db,
        RANKED_JOIN_QUERY,
        "recall_join",
        &params(&[("$q", "needle563")]),
    )
    .await
    .unwrap();
    assert_eq!(joined.num_rows(), 20);

    let batch = joined.concat_batches().unwrap();
    let chunk_slugs = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let artifact_slugs = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    for row in 0..batch.num_rows() {
        let c: usize = chunk_slugs.value(row)["chunk-".len()..].parse().unwrap();
        let a: usize = artifact_slugs.value(row)["art-".len()..].parse().unwrap();
        assert!(
            a == (c * FANOUT) % ARTIFACTS || a == (c * FANOUT + 1) % ARTIFACTS,
            "row {row}: artifact art-{a:04} is not linked to chunk-{c:05}"
        );
    }
}

/// Timing instrument for the scan cap: the join-free ranked read over the same
/// corpus geometry (no edges loaded), one warm-up then five timed runs. Prints
/// per-run and median wall time; asserts correctness only, never a duration
/// (wall-time asserts flake in CI). Compare against the parent commit to see
/// the cap's effect; the cost is hydration count, so the gap grows with the
/// matched-corpus size.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "expensive: ~1.2 GiB corpus + inverted index build; timing instrument"]
async fn times_join_free_ranked_read_issue_563() {
    let filler = filler_block();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, SCHEMA).await.unwrap();

    let mut chunk_batch = String::new();
    for c in 0..CHUNKS {
        chunk_batch.push_str(&format!(
            "{{\"type\":\"Chunk\",\"data\":{{\"slug\":\"chunk-{c:05}\",\"text\":\"needle563 {filler}\"}}}}\n"
        ));
        if (c + 1) % LOAD_BATCH_ROWS == 0 || c + 1 == CHUNKS {
            load_jsonl(&db, &chunk_batch, LoadMode::Append)
                .await
                .unwrap();
            chunk_batch.clear();
        }
    }
    db.ensure_indices().await.unwrap();

    let warmup = query_main(
        &mut db,
        RANKED_JOIN_QUERY,
        "recall_no_join",
        &params(&[("$q", "needle563")]),
    )
    .await
    .unwrap();
    assert_eq!(warmup.num_rows(), 20);

    let mut millis: Vec<u128> = Vec::new();
    for _ in 0..5 {
        let started = Instant::now();
        let result = query_main(
            &mut db,
            RANKED_JOIN_QUERY,
            "recall_no_join",
            &params(&[("$q", "needle563")]),
        )
        .await
        .unwrap();
        let elapsed = started.elapsed().as_millis();
        assert_eq!(result.num_rows(), 20);
        millis.push(elapsed);
    }
    millis.sort_unstable();
    println!("TIMING runs_ms={millis:?} median_ms={}", millis[2]);
}
