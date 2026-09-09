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

/// RFC 0048's relational selection prototype. This checks DataFusion's
/// public operators, not the unimplemented GQ stage lowering. Graph paths
/// are represented by repeated target rows so deduplication must precede
/// target windows. Global cuts restore all winning target bindings; quotas
/// restore only winning target/group pairs, using null-safe group equality.
#[tokio::test]
async fn staged_target_selection_preserves_cutoffs_and_binding_rows() {
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch, RecordBatchIterator};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::catalog::TableProvider;
    use datafusion::common::NullEquality;
    use datafusion::dataframe::DataFrame;
    use datafusion::functions_aggregate::expr_fn::min;
    use datafusion::functions_window::row_number::row_number;
    use datafusion::logical_expr::{ExprFunctionExt, JoinType, LogicalPlanBuilder, Partitioning};
    use datafusion::prelude::{SessionConfig, SessionContext, col, lit};
    use lance::Dataset;
    use lance::datafusion::LanceTableProvider;
    use lance::dataset::WriteParams;
    use lance_file::version::LanceFileVersion;

    async fn strings(frame: DataFrame, column: &str) -> Vec<String> {
        frame
            .collect()
            .await
            .unwrap()
            .iter()
            .flat_map(|batch| {
                let ids = batch
                    .column_by_name(column)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                ids.iter()
                    .map(|id| id.unwrap().to_string())
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("group_key", DataType::Utf8, true),
        Field::new("score", DataType::Int64, false),
        Field::new("binding_id", DataType::Utf8, false),
        Field::new("group_part", DataType::Int64, false),
        Field::new("pair_rank", DataType::Int64, true),
    ]));
    for (source, partitions) in [
        ("memory", 1),
        ("memory", 4),
        ("lance-ordered", 1),
        ("lance-ordered", 4),
        ("lance-unordered", 1),
        ("lance-unordered", 4),
    ] {
        for reverse in [false, true] {
            let mut rows = vec![
                ("a", Some("g1"), 10, "path-1"),
                ("a", Some("g2"), 10, "path-2"),
                ("a", Some("g2"), 10, "path-3"),
                ("b", Some("g2"), 9, "path-4"),
                ("c", Some("g1"), 11, "path-5"),
                ("d", None, 12, "path-6"),
                ("d", None, 12, "path-7"),
                ("e", None, 11, "path-8"),
                ("f", Some("g3"), 8, "path-9"),
            ];
            if reverse {
                rows.reverse();
            }
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(
                        rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        rows.iter().map(|r| r.3).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        rows.iter()
                            .map(|r| {
                                if matches!(r.3, "path-3" | "path-4") {
                                    2
                                } else {
                                    1
                                }
                            })
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        rows.iter()
                            .map(|r| match r.3 {
                                "path-1" | "path-2" => Some(2),
                                "path-3" => Some(4),
                                "path-4" => Some(3),
                                "path-6" | "path-7" => None,
                                _ => Some(1),
                            })
                            .collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap();
            let mut config = SessionConfig::new()
                .with_target_partitions(partitions)
                .with_batch_size(1);
            // Qualify a partitioned hash-join route. Default CollectLeft fails
            // distribution validation for the memory source at four partitions;
            // the real Lance sources below must be qualified independently.
            config
                .options_mut()
                .optimizer
                .hash_join_single_partition_threshold = 0;
            config
                .options_mut()
                .optimizer
                .hash_join_single_partition_threshold_rows = 0;
            let ctx = SessionContext::new_with_config(config.clone());
            let directory = tempfile::tempdir().unwrap();
            let source_frame = if source == "memory" {
                ctx.read_batches((0..batch.num_rows()).map(|row| batch.slice(row, 1)))
                    .unwrap()
            } else {
                let uri = directory.path().join("bindings.lance");
                let dataset = Dataset::write(
                    RecordBatchIterator::new(vec![Ok(batch)], schema.clone()),
                    uri.to_str().unwrap(),
                    Some(WriteParams {
                        max_rows_per_file: 3,
                        enable_stable_row_ids: true,
                        data_storage_version: Some(LanceFileVersion::V2_2),
                        ..Default::default()
                    }),
                )
                .await
                .unwrap();
                assert_eq!(dataset.get_fragments().len(), 3);
                let provider = Arc::new(
                    LanceTableProvider::new_with_ordering(
                        Arc::new(dataset),
                        false,
                        false,
                        source == "lance-ordered",
                    )
                    .with_batch_size(1),
                );
                // Call the public scan contract directly so this checks an
                // actual provider limit, not only a sort's top-K rewrite.
                let projection = vec![schema.index_of("binding_id").unwrap()];
                let scan = provider
                    .scan(
                        &ctx.state(),
                        Some(&projection),
                        &[col("score").lt(lit(10_i64))],
                        Some(1),
                    )
                    .await
                    .unwrap();
                let batches = datafusion::physical_plan::collect(scan, ctx.task_ctx())
                    .await
                    .unwrap();
                assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
                for batch in &batches {
                    assert_eq!(batch.num_columns(), 1);
                    let ids = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    for id in ids.iter().flatten() {
                        assert!(matches!(id, "path-4" | "path-9"), "{source}: {id}");
                    }
                }
                ctx.read_table(provider).unwrap()
            };
            let bindings = source_frame
                .repartition(Partitioning::RoundRobinBatch(partitions))
                .unwrap();
            let target_fields = || vec![col("id"), col("score")];
            let pair_fields = || vec![col("id"), col("group_key"), col("score")];
            let targets = bindings
                .clone()
                .select(target_fields())
                .unwrap()
                .distinct()
                .unwrap();
            let order = || vec![col("score").sort(false, false), col("id").sort(true, false)];
            assert_eq!(
                strings(targets.clone().sort(order()).unwrap(), "id").await,
                ["d", "c", "e", "a", "b", "f"]
            );
            let top_two = targets
                .clone()
                .sort(order())
                .unwrap()
                .limit(0, Some(2))
                .unwrap();
            assert_eq!(strings(top_two.clone(), "id").await, ["d", "c"]);

            let selected_bindings = bindings
                .clone()
                .join(
                    top_two.select(vec![col("id")]).unwrap(),
                    JoinType::LeftSemi,
                    &["id"],
                    &["id"],
                    None,
                )
                .unwrap();
            let binding_order = || {
                let mut keys = order();
                keys.push(col("group_key").sort(true, true));
                keys.push(col("binding_id").sort(true, false));
                keys
            };
            assert_eq!(
                strings(
                    selected_bindings.clone().sort(binding_order()).unwrap(),
                    "binding_id",
                )
                .await,
                ["path-6", "path-7", "path-5"]
            );

            // A final projection hides every ordering/filter/target key. The
            // optimized native plan must still choose targets before binding
            // pagination, and cannot move a later filter through that limit.
            let page = selected_bindings
                .clone()
                .sort(binding_order())
                .unwrap()
                .limit(1, Some(1))
                .unwrap()
                .filter(col("group_key").is_null())
                .unwrap()
                .select(vec![col("binding_id")])
                .unwrap();
            assert_eq!(page.schema().fields().len(), 1);
            assert_eq!(strings(page, "binding_id").await, ["path-7"]);

            let first_binding = selected_bindings
                .clone()
                .sort(binding_order())
                .unwrap()
                .limit(0, Some(1))
                .unwrap();
            assert!(
                strings(
                    first_binding
                        .filter(col("binding_id").eq(lit("path-7")))
                        .unwrap()
                        .select(vec![col("binding_id")])
                        .unwrap(),
                    "binding_id",
                )
                .await
                .is_empty()
            );
            // Red control for premature pushdown: this placement has one row.
            assert_eq!(
                strings(
                    selected_bindings
                        .clone()
                        .filter(col("binding_id").eq(lit("path-7")))
                        .unwrap()
                        .sort(binding_order())
                        .unwrap()
                        .limit(0, Some(1))
                        .unwrap()
                        .select(vec![col("binding_id")])
                        .unwrap(),
                    "binding_id",
                )
                .await,
                ["path-7"]
            );

            // Also qualify an optimized ordered cut with unprojected
            // predicate and ordering columns over fragmented native input.
            assert_eq!(
                strings(
                    bindings
                        .clone()
                        .filter(col("score").lt(lit(10_i64)))
                        .unwrap()
                        .sort(binding_order())
                        .unwrap()
                        .limit(0, Some(1))
                        .unwrap()
                        .select(vec![col("binding_id")])
                        .unwrap(),
                    "binding_id",
                )
                .await,
                ["path-4"]
            );

            // Filtering after the global cut cannot refill from f; filtering
            // before selection has a different logical population.
            assert!(
                strings(
                    selected_bindings
                        .clone()
                        .filter(col("group_key").eq(lit("g3")))
                        .unwrap(),
                    "id"
                )
                .await
                .is_empty()
            );
            assert_eq!(
                strings(
                    bindings
                        .clone()
                        .filter(col("group_key").eq(lit("g3")))
                        .unwrap()
                        .select(target_fields())
                        .unwrap()
                        .distinct()
                        .unwrap()
                        .sort(order())
                        .unwrap()
                        .limit(0, Some(2))
                        .unwrap(),
                    "id"
                )
                .await,
                ["f"]
            );

            // Deduplicate within each group. A may compete in g1 and g2,
            // but its two paths in g2 consume only one slot there.
            let pairs = bindings
                .clone()
                .select(pair_fields())
                .unwrap()
                .distinct()
                .unwrap();
            let quota = |input: DataFrame, count: u64| {
                input
                    .window(vec![
                        row_number()
                            .partition_by(vec![col("group_key")])
                            .order_by(order())
                            .build()
                            .unwrap()
                            .alias("within_group"),
                    ])
                    .unwrap()
                    .filter(col("within_group").lt_eq(lit(count)))
                    .unwrap()
                    .sort(order())
                    .unwrap()
            };
            let winners = quota(pairs.clone(), 1);
            assert_eq!(strings(winners.clone(), "id").await, ["d", "c", "a", "f"]);
            let cut_pairs = selected_bindings
                .select(pair_fields())
                .unwrap()
                .distinct()
                .unwrap();
            assert_eq!(strings(quota(cut_pairs, 1), "id").await, ["d", "c"]);

            let reattach = |selected: DataFrame, null_equality: NullEquality| {
                let right = selected
                    .select(vec![
                        col("id").alias("selected_id"),
                        col("group_key").alias("selected_group"),
                    ])
                    .unwrap();
                let plan = LogicalPlanBuilder::from(bindings.clone().into_unoptimized_plan())
                    .join_detailed(
                        right.into_unoptimized_plan(),
                        JoinType::LeftSemi,
                        (
                            vec!["id", "group_key"],
                            vec!["selected_id", "selected_group"],
                        ),
                        None,
                        null_equality,
                    )
                    .unwrap()
                    .build()
                    .unwrap();
                DataFrame::new(ctx.state(), plan)
                    .sort(binding_order())
                    .unwrap()
            };
            let selected_pairs = reattach(winners.clone(), NullEquality::NullEqualsNull);
            let expected_paths = ["path-6", "path-7", "path-5", "path-2", "path-3", "path-9"];
            let hash_plan = selected_pairs.clone().create_physical_plan().await.unwrap();
            let hash_summary = datafusion::physical_plan::displayable(hash_plan.as_ref())
                .indent(true)
                .to_string();
            assert!(hash_summary.contains("HashJoinExec"), "{hash_summary}");
            if partitions == 4 {
                assert!(hash_summary.contains("mode=Partitioned"), "{hash_summary}");
            }
            if source != "memory" {
                assert!(
                    hash_summary.contains("Lance"),
                    "native source was lost: {hash_summary}"
                );
            }
            assert_eq!(
                strings(selected_pairs.clone(), "binding_id").await,
                expected_paths
            );

            // Qualify the alternate physical join against the same typed
            // logical plan and pair oracle, including null-safe equality.
            // prefer_hash_join=false selects sort-merge only when target
            // partitions > 1; the native planner still chooses hash at one.
            config.options_mut().optimizer.prefer_hash_join = false;
            let merge_ctx = SessionContext::new_with_config(config);
            let merge_join = DataFrame::new(
                merge_ctx.state(),
                selected_pairs.clone().into_unoptimized_plan(),
            );
            let physical = merge_join.clone().create_physical_plan().await.unwrap();
            let summary = datafusion::physical_plan::displayable(physical.as_ref())
                .indent(true)
                .to_string();
            assert!(
                summary.contains(if partitions == 1 {
                    "HashJoinExec"
                } else {
                    "SortMergeJoinExec"
                }),
                "{source}/{partitions}: {summary}"
            );
            assert_eq!(strings(merge_join, "binding_id").await, expected_paths);

            // Default planning succeeds for these real Lance sources at both
            // partition counts. Only the memory source exposes CollectLeft's
            // distribution defect at four. Keep that refusal fence, without
            // inferring that a global planner override is needed for Lance.
            let default_ctx = SessionContext::new_with_config(
                SessionConfig::new()
                    .with_target_partitions(partitions)
                    .with_batch_size(1),
            );
            let default_join =
                DataFrame::new(default_ctx.state(), selected_pairs.into_unoptimized_plan());
            if source == "memory" && partitions == 4 {
                let error = default_join.collect().await.unwrap_err().to_string();
                assert!(error.contains("CollectLeft"), "{error}");
                assert!(
                    error.contains("does not satisfy distribution requirements: SinglePartition"),
                    "{error}"
                );
            } else {
                assert_eq!(strings(default_join, "binding_id").await, expected_paths);
            }
            // With quota two, a wins both groups; e and b remain eligible
            // despite duplicate paths for higher-ranked targets d and a.
            assert_eq!(
                strings(
                    reattach(quota(pairs, 2), NullEquality::NullEqualsNull),
                    "binding_id"
                )
                .await,
                [
                    "path-6", "path-7", "path-5", "path-8", "path-1", "path-2", "path-3", "path-4",
                    "path-9"
                ]
            );

            // Red control: joining winners by target alone restores a's
            // losing g1 path, making g1 exceed its quota of one.
            let wrong_target_join = bindings
                .clone()
                .join(
                    winners.clone().select(vec![col("id")]).unwrap(),
                    JoinType::LeftSemi,
                    &["id"],
                    &["id"],
                    None,
                )
                .unwrap()
                .sort(binding_order())
                .unwrap();
            assert_eq!(
                strings(wrong_target_join, "binding_id").await,
                [
                    "path-6", "path-7", "path-5", "path-1", "path-2", "path-3", "path-9"
                ]
            );

            // Red control: ordinary equality drops the winning null group.
            assert_eq!(
                strings(
                    reattach(winners, NullEquality::NullEqualsNothing),
                    "binding_id"
                )
                .await,
                ["path-5", "path-2", "path-3", "path-9"]
            );

            // Red control: applying the quota to paths excludes e from the
            // null group's two slots, although there are two distinct targets.
            assert_eq!(
                strings(
                    quota(
                        bindings.clone().filter(col("group_key").is_null()).unwrap(),
                        2
                    ),
                    "id"
                )
                .await,
                ["d", "d"]
            );

            // A composite key must survive both window partitioning and
            // reattachment. Here a wins (g2, 1), but loses (g2, 2) to b.
            // Explicit min is over each pair's paths; all-null d sorts last.
            let composite = bindings
                .clone()
                .aggregate(
                    vec![col("id"), col("group_key"), col("group_part")],
                    vec![min(col("pair_rank")).alias("best_rank")],
                )
                .unwrap();
            let composite_winners = |nulls_first| {
                composite
                    .clone()
                    .window(vec![
                        row_number()
                            .partition_by(vec![col("group_key"), col("group_part")])
                            .order_by(vec![
                                col("best_rank").sort(true, nulls_first),
                                col("id").sort(true, false),
                            ])
                            .build()
                            .unwrap()
                            .alias("position"),
                    ])
                    .unwrap()
                    .filter(col("position").eq(lit(1u64)))
                    .unwrap()
            };
            let composite_join = |selected: DataFrame, complete_key: bool| {
                let right = selected
                    .select(vec![
                        col("id").alias("winner_id"),
                        col("group_key").alias("winner_group"),
                        col("group_part").alias("winner_part"),
                    ])
                    .unwrap();
                let mut left_keys = vec!["id", "group_key"];
                let mut right_keys = vec!["winner_id", "winner_group"];
                if complete_key {
                    left_keys.push("group_part");
                    right_keys.push("winner_part");
                }
                let plan = LogicalPlanBuilder::from(bindings.clone().into_unoptimized_plan())
                    .join_detailed(
                        right.into_unoptimized_plan(),
                        JoinType::LeftSemi,
                        (left_keys, right_keys),
                        None,
                        NullEquality::NullEqualsNull,
                    )
                    .unwrap()
                    .build()
                    .unwrap();
                DataFrame::new(ctx.state(), plan)
                    .sort(vec![col("binding_id").sort(true, false)])
                    .unwrap()
            };
            assert_eq!(
                strings(composite_join(composite_winners(false), true), "binding_id").await,
                ["path-2", "path-4", "path-5", "path-8", "path-9"]
            );
            // Red controls distinguish a missing tuple component and the
            // opposite null ordering; both otherwise produce plausible rows.
            assert_eq!(
                strings(
                    composite_join(composite_winners(false), false),
                    "binding_id"
                )
                .await,
                ["path-2", "path-3", "path-4", "path-5", "path-8", "path-9"]
            );
            assert_eq!(
                strings(composite_join(composite_winners(true), true), "binding_id").await,
                ["path-2", "path-4", "path-5", "path-6", "path-7", "path-9"]
            );

            // Red control: counting paths as candidates makes d consume both
            // slots. The oracle must distinguish that implementation.
            let wrong = bindings
                .sort(order())
                .unwrap()
                .limit(0, Some(2))
                .unwrap()
                .select(target_fields())
                .unwrap()
                .distinct()
                .unwrap();
            assert_eq!(strings(wrong, "id").await, ["d"]);
            println!(
                "staged selection passed: source={source}, partitions={partitions}, reversed={reverse}"
            );
        }
    }
}

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

/// Stats-sensitive corpus for `stats_sensitive_corpus_plans_agree`: s-00..s-03
/// are eligible (`ChunkOfArtifact` → art-0), s-04..s-11 only shape the corpus
/// statistics. Per-row (alpha, beta, gamma, delta, pad) term counts; the pad
/// tokens are unique per doc, so they vary document length without touching
/// the query terms' document frequencies. df(alpha) = df(gamma) = 9
/// corpus-wide but 1 inside the eligible subset; df(beta) = df(delta) = 3
/// both corpus-wide and inside it — the alpha/beta (and gamma/delta) IDF
/// order flips between the two stats sources.
fn stats_seed_data() -> String {
    let profiles: [(usize, usize, usize, usize, usize); 12] = [
        (3, 0, 0, 1, 4),  // s-00: the subset's only alpha carrier
        (0, 2, 0, 2, 9),  // s-01
        (0, 3, 2, 0, 1),  // s-02: the subset's only gamma carrier
        (0, 1, 0, 2, 14), // s-03
        (1, 0, 1, 0, 3),  // s-04..s-11: alpha+gamma common outside the subset
        (1, 0, 1, 0, 4),
        (1, 0, 1, 0, 5),
        (1, 0, 1, 0, 6),
        (1, 0, 1, 0, 7),
        (1, 0, 1, 0, 8),
        (1, 0, 1, 0, 9),
        (1, 0, 1, 0, 10),
    ];
    let mut rows = vec![r#"{"type":"Artifact","data":{"slug":"art-0"}}"#.to_string()];
    for (i, &(alpha, beta, gamma, delta, pad)) in profiles.iter().enumerate() {
        let mut words: Vec<String> = Vec::new();
        words.extend(std::iter::repeat_n("alpha".to_string(), alpha));
        words.extend(std::iter::repeat_n("beta".to_string(), beta));
        words.extend(std::iter::repeat_n("gamma".to_string(), gamma));
        words.extend(std::iter::repeat_n("delta".to_string(), delta));
        words.extend((0..pad).map(|j| format!("pad{i:02}x{j}")));
        rows.push(format!(
            r#"{{"type":"Chunk","data":{{"slug":"s-{i:02}","text":"{}","embedding":[{i}.0,0.0,0.0,0.0]}}}}"#,
            words.join(" ")
        ));
    }
    for chunk in 0..4 {
        rows.push(format!(
            r#"{{"edge":"ChunkOfArtifact","from":"s-{chunk:02}","to":"art-0","data":{{"id":"soa-{chunk:02}","label":"of"}}}}"#
        ));
    }
    rows.join("\n")
}

async fn init_stats_db(dir: &tempfile::TempDir) -> Omnigraph {
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, GATE_SCHEMA).await.unwrap();
    load_jsonl(&db, &stats_seed_data(), LoadMode::Overwrite)
        .await
        .unwrap();
    db.ensure_indices().await.unwrap();
    db
}

/// Ragnor's stats-sensitive oracle case (PR #587 review r1). The main
/// fixture's single-term constant-length docs make arm rank order invariant
/// to the BM25 stats source — every score is one shared IDF/avgdl factor
/// times a per-doc term frequency — so those oracle pairs would stay green
/// even if a future Lance regression derived IDF/avgdl from the prefiltered
/// subset (the filter-dependent-scoring regression the gate's equivalence
/// claim cannot survive). This corpus makes rank order stats-DEPENDENT:
/// multi-term arms whose terms' document frequencies flip between corpus and
/// eligible subset (`stats_seed_data`) and varying doc lengths, so
/// subset-derived stats would reorder the surviving docs and turn this pair
/// red. Tie-freedom under index-wide stats: same-term competitors are
/// strictly dominance-ordered (higher tf AND shorter doc, or equal tf and
/// shorter doc); cross-term pairs differ in tf, IDF, and doc length at once,
/// so an exact BM25 float tie would require a designed coincidence — and the
/// fixture is deterministic, so any such tie would fail identically on every
/// run, never flake.
#[tokio::test]
#[serial]
async fn stats_sensitive_corpus_plans_agree() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_stats_db(&dir).await;
    let stats_params = params(&[("$q1", "alpha beta"), ("$q2", "gamma delta")]);
    assert_plans_equivalent(&mut db, "single_hop_both_bm25", &stats_params).await;
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
