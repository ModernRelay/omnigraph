//! Differential oracle and admission/fence tests for the rrf prefilter gate
//! (`exec::query::rrf_prefilter_gate`).
//!
//! The gate's two plans — prefilter (uncapped bm25 arms rank only the
//! traversal's eligible ids) and postfilter (today's uncapped corpus-wide
//! arms) — are ANSWER-IDENTICAL over FTS-index-covered data up to BM25 score
//! ties. A result-level test therefore cannot see a gate that silently
//! always falls back; every test here asserts on the `rrf_gate_verdicts`
//! probe as well as on results.
//!
//! The oracle is a metamorphic relation: the same query forced down both
//! plans must return
//! the identical ordered fused id sequence — EXACT comparison, no tolerance,
//! because the fixtures are tie-free (constant document length, pairwise
//! distinct per-term frequencies, so per-query BM25 scores are pairwise
//! distinct) and float scores are never compared, only the fused order.
//!
//! Two red controls prove the oracle is not vacuous:
//! - every oracle pair asserts via the probe that the two forced runs took
//!   DIFFERENT plans — a same-plan pair is a test FAILURE;
//! - `subset_injection_turns_the_oracle_red` drops one surviving id from the
//!   eligible set (`with_rrf_gate_subset_drop`) and asserts the equivalence
//!   relation breaks — the superset fence's consumer-side observable.
//!
//! The build-Err fence (a failing `GraphIndexHandle` at the gate) has no
//! executable guard here: no injectable failing-handle seam exists yet, so
//! today its only protection is the gate's own fallback code path (a build
//! error can never fail the query — it runs postfilter). An injectable seam
//! or a fault-injection case would close the gap; until one exists, that
//! leg is untested, not claimed covered.

mod helpers;

use arrow_array::{Array, StringArray};
use serial_test::serial;

use omnigraph::db::Omnigraph;
use omnigraph::instrumentation::{
    QueryIoProbes, RrfGateFallback, RrfGatePlan, RrfGateVerdict, with_query_io_probes,
    with_rrf_plan, with_traversal_mode,
};
// The subset-drop red-control seam is compiled out of release binaries
// (an answer-corrupting API must not ship); its test is cfg-gated the same.
#[cfg(debug_assertions)]
use omnigraph::instrumentation::with_rrf_gate_subset_drop;
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph_compiler::ir::ParamMap;
use omnigraph_compiler::result::QueryResult;

use helpers::*;

const GATE_SCHEMA: &str = r#"
node Chunk {
    slug: String @key
    text: String @index
    embedding: Vector(4)
}

node Artifact {
    slug: String @key
}

edge ChunkOfArtifact: Chunk -> Artifact {
    label: String
}

edge ChunkCites: Chunk -> Chunk {
    label: String
}
"#;

const GATE_CHUNKS: usize = 20;

/// Tie-free corpus: chunk i holds "needle" × (20 − i) and "sharp" × (i + 1)
/// plus one "filler" — constant 22-token documents, so BM25 is strictly
/// monotone in term frequency and per-query scores are pairwise distinct
/// (the oracle's tie-freedom precondition). bm25(needle) ranks chunk-00
/// first; bm25(sharp) ranks chunk-19 first; nearest([0,0,0,0]) ranks
/// chunk-00 first (embedding [i, 0, 0, 0], distinct distances).
fn gate_chunk_rows() -> Vec<String> {
    (0..GATE_CHUNKS)
        .map(|chunk| {
            let mut words = vec!["needle"; GATE_CHUNKS - chunk];
            words.extend(vec!["sharp"; chunk + 1]);
            words.push("filler");
            format!(
                r#"{{"type":"Chunk","data":{{"slug":"chunk-{chunk:02}","text":"{}","embedding":[{chunk}.0,0.0,0.0,0.0]}}}}"#,
                words.join(" ")
            )
        })
        .collect()
}

/// The oracle fixture's edges.
///
/// `ChunkOfArtifact` (Chunk -> Artifact): chunk-04..06 → art-0 and
/// chunk-07 → art-1 (so a `$a.slug = "art-0"` dst filter is selective).
/// `ChunkCites` (Chunk -> Chunk): 02→10, 05→12, 08→03, 12→15 — giving
/// distinct eligibility sets per direction (Out sources {02,05,08,12}, In
/// targets {03,10,12,15}, Both their union) and a 2-hop chain 05→12→15.
/// chunk-05 carries both edge kinds, so the several-Expands intersection is
/// non-empty.
fn gate_seed_data() -> String {
    let mut rows = vec![
        r#"{"type":"Artifact","data":{"slug":"art-0"}}"#.to_string(),
        r#"{"type":"Artifact","data":{"slug":"art-1"}}"#.to_string(),
    ];
    rows.extend(gate_chunk_rows());
    for chunk in 4..=6 {
        rows.push(format!(
            r#"{{"edge":"ChunkOfArtifact","from":"chunk-{chunk:02}","to":"art-0","data":{{"id":"eoa-{chunk:02}","label":"of"}}}}"#
        ));
    }
    rows.push(
        r#"{"edge":"ChunkOfArtifact","from":"chunk-07","to":"art-1","data":{"id":"eoa-07","label":"of"}}"#
            .to_string(),
    );
    for (from, to) in [(2, 10), (5, 12), (8, 3), (12, 15)] {
        rows.push(format!(
            r#"{{"edge":"ChunkCites","from":"chunk-{from:02}","to":"chunk-{to:02}","data":{{"id":"ec-{from:02}-{to:02}","label":"cites"}}}}"#
        ));
    }
    rows.join("\n")
}

/// One query per admitted shape of the gate's admission table, plus the two
/// threading cases (bm25 in the secondary position; both arms bm25). The
/// ranked variable is always `$c`.
const GATE_QUERIES: &str = r#"
query single_hop_both_bm25($q1: String, $q2: String) {
    match {
        $c: Chunk
        $c chunkOfArtifact $a
    }
    return { $c.slug }
    order { rrf(bm25($c.text, $q1), bm25($c.text, $q2)) }
    limit 5
}

query search_filter_single_hop($q1: String, $q2: String) {
    match {
        $c: Chunk
        $c chunkOfArtifact $a
        search($c.text, $q1)
    }
    return { $c.slug }
    order { rrf(bm25($c.text, $q1), bm25($c.text, $q2)) }
    limit 5
}

query bm25_secondary_position($v: Vector(4), $q: String) {
    match {
        $c: Chunk
        $c chunkOfArtifact $a
    }
    return { $c.slug }
    order { rrf(nearest($c.embedding, $v), bm25($c.text, $q)) }
    limit 5
}

query multi_hop($q1: String, $q2: String) {
    match {
        $c: Chunk
        $c chunkCites{1,2} $d
    }
    return { $c.slug }
    order { rrf(bm25($c.text, $q1), bm25($c.text, $q2)) }
    limit 5
}

query multi_hop_min_two($q1: String, $q2: String) {
    match {
        $c: Chunk
        $c chunkCites{2,2} $d
    }
    return { $c.slug }
    order { rrf(bm25($c.text, $q1), bm25($c.text, $q2)) }
    limit 5
}

query dst_filtered($q1: String, $q2: String) {
    match {
        $c: Chunk
        $c chunkOfArtifact $a
        $a.slug = "art-0"
    }
    return { $c.slug }
    order { rrf(bm25($c.text, $q1), bm25($c.text, $q2)) }
    limit 5
}

query direction_both($q1: String, $q2: String) {
    match {
        $c: Chunk
        $c <chunkCites> $d
    }
    return { $c.slug }
    order { rrf(bm25($c.text, $q1), bm25($c.text, $q2)) }
    limit 5
}

query direction_in($q1: String, $q2: String) {
    match {
        $c: Chunk
        $d chunkCites $c
    }
    return { $c.slug }
    order { rrf(bm25($c.text, $q1), bm25($c.text, $q2)) }
    limit 5
}

query several_expands($q1: String, $q2: String) {
    match {
        $c: Chunk
        $c chunkOfArtifact $a
        $c chunkCites $d
    }
    return { $c.slug }
    order { rrf(bm25($c.text, $q1), bm25($c.text, $q2)) }
    limit 5
}

query antijoin_only($q1: String, $q2: String) {
    match {
        $c: Chunk
        not { $c chunkOfArtifact $a }
    }
    return { $c.slug }
    order { rrf(bm25($c.text, $q1), bm25($c.text, $q2)) }
    limit 5
}

query different_var_arms($q1: String, $q2: String) {
    match {
        $c: Chunk
        $d: Chunk
        $c chunkOfArtifact $a
    }
    return { $c.slug }
    order { rrf(bm25($c.text, $q1), bm25($d.text, $q2)) }
    limit 5
}

query ranked_var_is_expand_dst($q1: String, $q2: String) {
    match {
        $d: Chunk
        $d chunkCites $c
    }
    return { $c.slug }
    order { rrf(bm25($c.text, $q1), bm25($c.text, $q2)) }
    limit 5
}

query no_traversal($q1: String, $q2: String) {
    match {
        $c: Chunk
    }
    return { $c.slug }
    order { rrf(bm25($c.text, $q1), bm25($c.text, $q2)) }
    limit 5
}
"#;

async fn init_gate_db(dir: &tempfile::TempDir) -> Omnigraph {
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, GATE_SCHEMA).await.unwrap();
    load_jsonl(&db, &gate_seed_data(), LoadMode::Overwrite)
        .await
        .unwrap();
    db.ensure_indices().await.unwrap();
    db
}

fn fused_slugs(result: &QueryResult) -> Vec<String> {
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

/// Run `query_name` with the gate forced to `plan`, returning the ordered
/// fused slug sequence, the gate verdicts the run recorded, and the total
/// BM25-scanned row count (the plan-EFFECT observable — verdicts alone
/// cannot distinguish a gate that picks prefilter from one whose id push
/// actually reaches the scan).
async fn run_forced(
    db: &mut Omnigraph,
    plan: &'static str,
    query_name: &str,
    params: &ParamMap,
) -> (Vec<String>, Vec<RrfGateVerdict>, u64) {
    let probes = QueryIoProbes::default();
    let result = with_query_io_probes(
        probes.clone(),
        with_rrf_plan(plan, async {
            query_main(db, GATE_QUERIES, query_name, params).await
        }),
    )
    .await
    .unwrap();
    let verdicts = probes.rrf_gate_verdicts.lock().unwrap().clone();
    let scan_rows = probes
        .bm25_scan_rows
        .load(std::sync::atomic::Ordering::Relaxed);
    (fused_slugs(&result), verdicts, scan_rows)
}

/// The differential oracle for one query: force-prefilter ≡ force-postfilter
/// on the ordered fused id sequence (rank agreement — integer fusion ranks
/// are row order — with float scores never compared). Red control (a): the
/// probe must show the two runs took DIFFERENT plans; a same-plan pair fails
/// here, never passes vacuously.
async fn assert_plans_equivalent(db: &mut Omnigraph, query_name: &str, params: &ParamMap) {
    let (prefilter_slugs, prefilter_verdicts, prefilter_scan_rows) =
        run_forced(db, "force_prefilter", query_name, params).await;
    let (postfilter_slugs, postfilter_verdicts, _) =
        run_forced(db, "force_postfilter", query_name, params).await;

    assert_eq!(
        prefilter_verdicts.len(),
        1,
        "{query_name}: expected exactly one gate verdict per rrf run"
    );
    assert_eq!(
        postfilter_verdicts.len(),
        1,
        "{query_name}: expected exactly one gate verdict per rrf run"
    );
    assert_eq!(
        prefilter_verdicts[0].plan,
        RrfGatePlan::Prefilter,
        "{query_name}: forced-prefilter run fell back — the pair is same-plan \
         and the oracle would be vacuous: {:?}",
        prefilter_verdicts[0]
    );
    assert_eq!(
        postfilter_verdicts[0].plan,
        RrfGatePlan::Postfilter,
        "{query_name}: forced-postfilter run did not run postfilter: {:?}",
        postfilter_verdicts[0]
    );
    assert!(
        !prefilter_slugs.is_empty(),
        "{query_name}: empty fused result — the equivalence would be vacuous"
    );
    // Plan EFFECT, not just plan selection: a threading no-op (verdict says
    // Prefilter, but the id push never reaches the scan) returns the
    // corpus-wide answer from both runs and passes the equality vacuously.
    // At most two bm25 arms rank at most |eligible| rows each.
    let eligible = prefilter_verdicts[0]
        .eligible
        .expect("a prefilter verdict carries the eligible count");
    assert!(
        prefilter_scan_rows <= 2 * eligible,
        "{query_name}: prefiltered arms scanned {prefilter_scan_rows} rows — more than \
         2 arms x {eligible} eligible; the id prefilter did not reach the scan"
    );
    assert_eq!(
        prefilter_slugs, postfilter_slugs,
        "{query_name}: the two answer-identical plans disagreed"
    );
}

/// C9 sweep: every "prefilter" row of the admission table plus the two
/// threading cases — single hop (both arms bm25), the #563 shape with a
/// search() filter, bm25 in the secondary position under a nearest primary,
/// multi-hop {1,2}, dst_filters present, direction Both, direction In, and
/// several Expands from the ranked variable (intersection).
#[tokio::test]
#[serial]
async fn oracle_admission_sweep_prefilter_equals_postfilter() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_gate_db(&dir).await;

    let text_params = params(&[("$q1", "needle"), ("$q2", "sharp")]);
    for query_name in [
        "single_hop_both_bm25",
        "search_filter_single_hop",
        "multi_hop",
        // Superset-via-path-length: {2,2} makes eligibility (first-hop
        // existence, {02,05,08,12}) a STRICT superset of the survivors
        // (only chunk-05 has a 2-hop chain, 05→12→15) — the strongest
        // over-approximation the gate performs.
        "multi_hop_min_two",
        "dst_filtered",
        "direction_both",
        "direction_in",
        "several_expands",
    ] {
        assert_plans_equivalent(&mut db, query_name, &text_params).await;
    }

    let hybrid_params = vector_and_string_params("$v", &[0.0, 0.0, 0.0, 0.0], "$q", "needle");
    assert_plans_equivalent(&mut db, "bm25_secondary_position", &hybrid_params).await;
}

/// Red control (b), the superset fence's consumer-side observable: dropping
/// ONE surviving id from the eligible set must break the equivalence
/// relation. If this passes with the relation intact, the oracle cannot
/// detect a subset bug and every green sweep above is meaningless.
#[cfg(debug_assertions)]
#[tokio::test]
#[serial]
async fn subset_injection_turns_the_oracle_red() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_gate_db(&dir).await;
    let text_params = params(&[("$q1", "needle"), ("$q2", "sharp")]);

    let (postfilter_slugs, _, _) = run_forced(
        &mut db,
        "force_postfilter",
        "single_hop_both_bm25",
        &text_params,
    )
    .await;
    let survivor = postfilter_slugs
        .first()
        .expect("the fixture must produce a non-empty fused result")
        .clone();

    let probes = QueryIoProbes::default();
    let corrupted = with_query_io_probes(
        probes.clone(),
        with_rrf_gate_subset_drop(
            survivor.clone(),
            with_rrf_plan("force_prefilter", async {
                query_main(&mut db, GATE_QUERIES, "single_hop_both_bm25", &text_params).await
            }),
        ),
    )
    .await
    .unwrap();
    let verdicts = probes.rrf_gate_verdicts.lock().unwrap().clone();
    assert_eq!(verdicts[0].plan, RrfGatePlan::Prefilter);

    let corrupted_slugs = fused_slugs(&corrupted);
    assert_ne!(
        corrupted_slugs, postfilter_slugs,
        "dropping surviving id '{survivor}' from the eligible set did not \
         change the fused answer — the oracle cannot see subset violations"
    );
    assert!(
        !corrupted_slugs.contains(&survivor),
        "the dropped id must be missing from the corrupted prefilter answer"
    );
}

/// Admission-table fall-back row: an Expand inside an AntiJoin inner is
/// inverted, so it must never be an eligibility source. With no top-level
/// Expand constraining `$c`, the shape guard falls back — asserted via the
/// probe's `Shape` reason, since both plans return the same rows either way.
#[tokio::test]
#[serial]
async fn antijoin_only_shape_falls_back() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_gate_db(&dir).await;
    let text_params = params(&[("$q1", "needle"), ("$q2", "sharp")]);

    let (slugs, verdicts, _) =
        run_forced(&mut db, "force_prefilter", "antijoin_only", &text_params).await;
    assert_eq!(verdicts.len(), 1);
    assert_eq!(verdicts[0].plan, RrfGatePlan::Postfilter);
    assert_eq!(
        verdicts[0].fallback,
        Some(RrfGateFallback::Shape),
        "an AntiJoin-only constraint must fall back with the shape reason: {:?}",
        verdicts[0]
    );
    assert!(
        !slugs.is_empty(),
        "the anti-join query itself must still answer (edge-less chunks exist)"
    );
}

/// The Shape fence's remaining admission-failure rows (the AntiJoin row has
/// its own test above): arms targeting different variables, the ranked
/// variable introduced as an Expand dst (the answer-relevant row — its
/// NodeScan never installs the search), and a traversal-free rrf scan.
/// Each takes a distinct early return in the gate; all must record the
/// Shape reason even under force_prefilter.
#[tokio::test]
#[serial]
async fn shape_fence_covers_all_fallback_rows() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_gate_db(&dir).await;
    let text_params = params(&[("$q1", "needle"), ("$q2", "sharp")]);

    for query_name in [
        "different_var_arms",
        "ranked_var_is_expand_dst",
        "no_traversal",
    ] {
        let (_, verdicts, _) =
            run_forced(&mut db, "force_prefilter", query_name, &text_params).await;
        assert_eq!(verdicts.len(), 1, "{query_name}: one verdict per rrf run");
        assert_eq!(
            verdicts[0].plan,
            RrfGatePlan::Postfilter,
            "{query_name}: must not prefilter"
        );
        assert_eq!(
            verdicts[0].fallback,
            Some(RrfGateFallback::Shape),
            "{query_name}: must fall back with the shape reason: {:?}",
            verdicts[0]
        );
    }
}

/// Threshold boundary: 2 eligible of 20 is EXACTLY the default 0.10 ratio.
/// Admission is `<=`, so the natural gate must still prefilter here — a
/// `<=`→`<` regression at the boundary flips this test.
#[tokio::test]
#[serial]
async fn natural_gate_prefilters_at_ratio_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, GATE_SCHEMA).await.unwrap();
    let mut rows = vec![r#"{"type":"Artifact","data":{"slug":"art-0"}}"#.to_string()];
    rows.extend(gate_chunk_rows());
    for chunk in 4..=5 {
        rows.push(format!(
            r#"{{"edge":"ChunkOfArtifact","from":"chunk-{chunk:02}","to":"art-0","data":{{"id":"eoa-{chunk:02}","label":"of"}}}}"#
        ));
    }
    load_jsonl(&db, &rows.join("\n"), LoadMode::Overwrite)
        .await
        .unwrap();
    db.ensure_indices().await.unwrap();
    let mut db = db;

    let text_params = params(&[("$q1", "needle"), ("$q2", "sharp")]);
    let probes = QueryIoProbes::default();
    let _ = with_query_io_probes(probes.clone(), async {
        query_main(&mut db, GATE_QUERIES, "single_hop_both_bm25", &text_params).await
    })
    .await
    .unwrap();
    let verdicts = probes.rrf_gate_verdicts.lock().unwrap().clone();
    assert_eq!(verdicts.len(), 1);
    assert_eq!(
        verdicts[0],
        RrfGateVerdict {
            plan: RrfGatePlan::Prefilter,
            fallback: None,
            forced: false,
            eligible: Some(2),
            corpus: Some(20),
        },
        "2/20 sits exactly on the 0.10 ratio and must still prefilter (<=)"
    );
}

/// Cross-route equivalence. At this suite's scale both plans naturally pick
/// the same Expand route, so the oracle above never exercises the property
/// the plans lean on at production scale: prefilter's small frontier takes
/// the IndexedScan route while postfilter's corpus-wide frontier takes the
/// Csr route, and fused ranks are row-order ordinals (`is_search_ordered`
/// skips the final sort — row order IS the ranking). Force the routes
/// crosswise so the fused sequence must survive the route divergence too.
#[tokio::test]
#[serial]
async fn oracle_holds_across_expand_routes() {
    async fn run_forced_with_route(
        db: &mut Omnigraph,
        plan: &'static str,
        route: &'static str,
        query_name: &str,
        params: &ParamMap,
    ) -> (Vec<String>, Vec<RrfGateVerdict>) {
        let probes = QueryIoProbes::default();
        let result = with_query_io_probes(
            probes.clone(),
            with_traversal_mode(
                route,
                with_rrf_plan(plan, async {
                    query_main(db, GATE_QUERIES, query_name, params).await
                }),
            ),
        )
        .await
        .unwrap();
        let verdicts = probes.rrf_gate_verdicts.lock().unwrap().clone();
        (fused_slugs(&result), verdicts)
    }

    let dir = tempfile::tempdir().unwrap();
    let mut db = init_gate_db(&dir).await;
    let text_params = params(&[("$q1", "needle"), ("$q2", "sharp")]);

    for query_name in ["single_hop_both_bm25", "multi_hop"] {
        let (pre_indexed, pre_verdicts) = run_forced_with_route(
            &mut db,
            "force_prefilter",
            "indexed",
            query_name,
            &text_params,
        )
        .await;
        let (post_csr, post_verdicts) =
            run_forced_with_route(&mut db, "force_postfilter", "csr", query_name, &text_params)
                .await;
        assert_eq!(pre_verdicts[0].plan, RrfGatePlan::Prefilter, "{query_name}");
        assert_eq!(
            post_verdicts[0].plan,
            RrfGatePlan::Postfilter,
            "{query_name}"
        );
        assert_eq!(
            pre_indexed, post_csr,
            "{query_name}: prefilter+indexed vs postfilter+csr disagreed"
        );

        let (pre_csr, _) =
            run_forced_with_route(&mut db, "force_prefilter", "csr", query_name, &text_params)
                .await;
        let (post_indexed, _) = run_forced_with_route(
            &mut db,
            "force_postfilter",
            "indexed",
            query_name,
            &text_params,
        )
        .await;
        assert_eq!(
            pre_csr, post_indexed,
            "{query_name}: prefilter+csr vs postfilter+indexed disagreed"
        );
        assert!(
            !pre_indexed.is_empty(),
            "{query_name}: cross-route equivalence would be vacuous on empty results"
        );
    }
}

/// Correctness fence (never overridden by force): fragments appended after
/// the FTS index build are scored filter-dependently, so the gate must
/// refuse the prefilter plan on a partially covered table. Recipe: build
/// indices, then append rows — those fragments are uncovered.
#[tokio::test]
#[serial]
async fn partial_fts_coverage_falls_back() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_gate_db(&dir).await;
    // Append one more matching, linked chunk AFTER ensure_indices: its
    // fragment is not in the FTS index's fragment bitmap.
    let appended = [
        r#"{"type":"Chunk","data":{"slug":"chunk-99","text":"needle sharp filler","embedding":[99.0,0.0,0.0,0.0]}}"#,
        r#"{"edge":"ChunkOfArtifact","from":"chunk-99","to":"art-0","data":{"id":"eoa-99","label":"of"}}"#,
    ]
    .join("\n");
    load_jsonl(&db, &appended, LoadMode::Append).await.unwrap();

    let text_params = params(&[("$q1", "needle"), ("$q2", "sharp")]);
    let (prefilter_slugs, verdicts, _) = run_forced(
        &mut db,
        "force_prefilter",
        "single_hop_both_bm25",
        &text_params,
    )
    .await;
    assert_eq!(verdicts.len(), 1);
    assert_eq!(verdicts[0].plan, RrfGatePlan::Postfilter);
    assert_eq!(
        verdicts[0].fallback,
        Some(RrfGateFallback::Coverage),
        "a partially covered FTS index must trip the coverage fence even under \
         force_prefilter: {:?}",
        verdicts[0]
    );
    assert!(verdicts[0].forced, "the forced flag must be recorded");

    // Both runs postfilter, so results still agree.
    let (postfilter_slugs, _, _) = run_forced(
        &mut db,
        "force_postfilter",
        "single_hop_both_bm25",
        &text_params,
    )
    .await;
    assert_eq!(prefilter_slugs, postfilter_slugs);
}

/// |eligible| = 0 overrides everything, force included: the postfilter plan
/// yields the same (empty) join and `IN ()` edge semantics never arise.
#[tokio::test]
#[serial]
async fn empty_eligible_set_falls_back() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, GATE_SCHEMA).await.unwrap();
    // Nodes only — no edge rows anywhere, so no Chunk is eligible.
    load_jsonl(
        &db,
        &[
            r#"{"type":"Artifact","data":{"slug":"art-0"}}"#.to_string(),
            gate_chunk_rows().join("\n"),
        ]
        .join("\n"),
        LoadMode::Overwrite,
    )
    .await
    .unwrap();
    db.ensure_indices().await.unwrap();
    let mut db = db;

    let text_params = params(&[("$q1", "needle"), ("$q2", "sharp")]);
    let (slugs, verdicts, _) = run_forced(
        &mut db,
        "force_prefilter",
        "single_hop_both_bm25",
        &text_params,
    )
    .await;
    assert_eq!(verdicts.len(), 1);
    assert_eq!(verdicts[0].plan, RrfGatePlan::Postfilter);
    assert_eq!(
        verdicts[0].fallback,
        Some(RrfGateFallback::EmptyEligible),
        "an empty eligible set must fall back even under force_prefilter: {:?}",
        verdicts[0]
    );
    assert_eq!(verdicts[0].eligible, Some(0));
    assert!(
        slugs.is_empty(),
        "no chunk has an edge, so the traversal must yield no rows"
    );
}

/// The natural (un-forced) gate on a selective fixture: 1 eligible of 20
/// (5%) passes the default ratio (10%) and absolute cap, so the gate picks
/// prefilter on its own — pinning the threshold's direction without env
/// overrides. The result must equal a forced-postfilter run.
#[tokio::test]
#[serial]
async fn natural_gate_prefilters_selective_fixture() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, GATE_SCHEMA).await.unwrap();
    let mut rows = vec![r#"{"type":"Artifact","data":{"slug":"art-0"}}"#.to_string()];
    rows.extend(gate_chunk_rows());
    rows.push(
        r#"{"edge":"ChunkOfArtifact","from":"chunk-04","to":"art-0","data":{"id":"eoa-04","label":"of"}}"#
            .to_string(),
    );
    load_jsonl(&db, &rows.join("\n"), LoadMode::Overwrite)
        .await
        .unwrap();
    db.ensure_indices().await.unwrap();
    let mut db = db;

    let text_params = params(&[("$q1", "needle"), ("$q2", "sharp")]);
    let probes = QueryIoProbes::default();
    let natural = with_query_io_probes(probes.clone(), async {
        query_main(&mut db, GATE_QUERIES, "single_hop_both_bm25", &text_params).await
    })
    .await
    .unwrap();
    let verdicts = probes.rrf_gate_verdicts.lock().unwrap().clone();
    assert_eq!(verdicts.len(), 1);
    assert_eq!(
        verdicts[0],
        RrfGateVerdict {
            plan: RrfGatePlan::Prefilter,
            fallback: None,
            forced: false,
            eligible: Some(1),
            corpus: Some(20),
        },
        "1/20 eligible must pass the natural threshold"
    );

    let (postfilter_slugs, _, _) = run_forced(
        &mut db,
        "force_postfilter",
        "single_hop_both_bm25",
        &text_params,
    )
    .await;
    assert_eq!(fused_slugs(&natural), postfilter_slugs);
}

/// Acceptance: the #574 review's seven-chunk decoy-flood scenario (the
/// aaltshuler P1 — four edge-less decoys out-score every eligible chunk in
/// the alpha arm; fusing COMPLETE rankings makes x the winner, and any arm
/// starvation silently flips it to n) must hold under BOTH v1 plans. The
/// prefiltered alpha arm ranks only {x, y, n} — the decoys never enter —
/// and x must still win.
#[tokio::test]
#[serial]
async fn decoy_flood_winner_holds_under_both_plans() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, GATE_SCHEMA).await.unwrap();
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
    for (index, (slug, alpha, beta)) in chunks.into_iter().enumerate() {
        let mut words = vec!["alpha"; alpha];
        words.extend(vec!["beta"; beta]);
        words.extend(vec!["filler"; 20 - alpha - beta]);
        rows.push(format!(
            r#"{{"type":"Chunk","data":{{"slug":"{slug}","text":"{}","embedding":[{index}.0,0.0,0.0,0.0]}}}}"#,
            words.join(" ")
        ));
    }
    for slug in ["x", "y", "n"] {
        rows.push(format!(
            r#"{{"edge":"ChunkOfArtifact","from":"{slug}","to":"art-0","data":{{"id":"e-{slug}","label":"of"}}}}"#
        ));
    }
    load_jsonl(&db, &rows.join("\n"), LoadMode::Overwrite)
        .await
        .unwrap();
    db.ensure_indices().await.unwrap();
    let mut db = db;

    let text_params = params(&[("$q1", "alpha"), ("$q2", "beta")]);
    let (prefilter_slugs, prefilter_verdicts, _) = run_forced(
        &mut db,
        "force_prefilter",
        "single_hop_both_bm25",
        &text_params,
    )
    .await;
    let (postfilter_slugs, postfilter_verdicts, _) = run_forced(
        &mut db,
        "force_postfilter",
        "single_hop_both_bm25",
        &text_params,
    )
    .await;

    assert_eq!(prefilter_verdicts[0].plan, RrfGatePlan::Prefilter);
    assert_eq!(prefilter_verdicts[0].eligible, Some(3));
    assert_eq!(postfilter_verdicts[0].plan, RrfGatePlan::Postfilter);
    assert_eq!(
        prefilter_slugs.first().map(String::as_str),
        Some("x"),
        "x wins the fused ranking; n wins only if the alpha arm is starved"
    );
    assert_eq!(
        prefilter_slugs, postfilter_slugs,
        "the decoy-flood fused order must be identical under both plans"
    );
}

/// The natural gate on a broad fixture: 8 eligible of 20 (40%) fails the
/// default 10% ratio — the threshold reason, with the counts recorded.
#[tokio::test]
#[serial]
async fn natural_gate_falls_back_on_broad_fixture() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, GATE_SCHEMA).await.unwrap();
    let mut rows = vec![r#"{"type":"Artifact","data":{"slug":"art-0"}}"#.to_string()];
    rows.extend(gate_chunk_rows());
    for chunk in 4..12 {
        rows.push(format!(
            r#"{{"edge":"ChunkOfArtifact","from":"chunk-{chunk:02}","to":"art-0","data":{{"id":"eoa-{chunk:02}","label":"of"}}}}"#
        ));
    }
    load_jsonl(&db, &rows.join("\n"), LoadMode::Overwrite)
        .await
        .unwrap();
    db.ensure_indices().await.unwrap();
    let mut db = db;

    let text_params = params(&[("$q1", "needle"), ("$q2", "sharp")]);
    let probes = QueryIoProbes::default();
    let _ = with_query_io_probes(probes.clone(), async {
        query_main(&mut db, GATE_QUERIES, "single_hop_both_bm25", &text_params).await
    })
    .await
    .unwrap();
    let verdicts = probes.rrf_gate_verdicts.lock().unwrap().clone();
    assert_eq!(verdicts.len(), 1);
    assert_eq!(
        verdicts[0],
        RrfGateVerdict {
            plan: RrfGatePlan::Postfilter,
            fallback: Some(RrfGateFallback::Threshold),
            forced: false,
            eligible: Some(8),
            corpus: Some(20),
        },
        "8/20 eligible must fail the natural 10% ratio"
    );
}
