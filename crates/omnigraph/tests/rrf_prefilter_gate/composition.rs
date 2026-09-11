//! C1-C4 physical building blocks for RFC 0048. GQ cannot execute the deferred
//! operators yet. These fixtures therefore qualify typed DataFusion plans and
//! invalid rewrites, not compiler lowering, native retrieval or resource bounds.
use std::sync::Arc;

use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::DataType;
use datafusion::common::ScalarValue;
use datafusion::dataframe::DataFrame;
use datafusion::functions::core::expr_fn::{coalesce, named_struct};
use datafusion::functions_aggregate::count::count_distinct;
use datafusion::functions_aggregate::expr_fn::{array_agg, count, min};
use datafusion::functions_window::row_number::row_number;
use datafusion::logical_expr::{ExprFunctionExt, JoinType, Partitioning, when};
use datafusion::prelude::{SessionConfig, SessionContext, col, lit};
use omnigraph_compiler::result::QueryResult;
use serde_json::{Value, json};

fn frame(ctx: &SessionContext, batch: RecordBatch, partitions: usize, reverse: bool) -> DataFrame {
    let mut rows: Vec<_> = (0..batch.num_rows()).map(|i| batch.slice(i, 1)).collect();
    if reverse {
        rows.reverse();
    }
    ctx.read_batches(rows)
        .unwrap()
        .repartition(Partitioning::RoundRobinBatch(partitions))
        .unwrap()
}

async fn rows(frame: DataFrame) -> Value {
    let schema = Arc::new(frame.schema().as_arrow().clone());
    QueryResult::new(schema, frame.collect().await.unwrap())
        .to_rust_json()
        .unwrap()
}

#[tokio::test]
async fn staged_composition_population_and_collection_contracts() {
    for partitions in [1, 4] {
        for reverse in [false, true] {
            let mut config = SessionConfig::new()
                .with_target_partitions(partitions)
                .with_batch_size(1);
            // The existing selection owner fences the memory/CollectLeft
            // distribution defect. This experiment qualifies partitioned joins.
            config
                .options_mut()
                .optimizer
                .hash_join_single_partition_threshold = 0;
            config
                .options_mut()
                .optimizer
                .hash_join_single_partition_threshold_rows = 0;
            let ctx = SessionContext::new_with_config(config);

            // C1: count rows in each period before selecting a service. C has
            // no current-period incidents; a service with no rows is absent.
            let mut incidents = Vec::new();
            for (service, prior, current) in [("A", 2, 8), ("B", 6, 7), ("C", 1, 0)] {
                for (period, n) in [("prior", prior), ("current", current)] {
                    for i in 0..n {
                        incidents.push((service, period, format!("{service}-{period}-{i}")));
                    }
                }
            }
            let input = frame(
                &ctx,
                RecordBatch::try_from_iter([
                    (
                        "service",
                        Arc::new(StringArray::from(
                            incidents.iter().map(|r| r.0).collect::<Vec<_>>(),
                        )) as ArrayRef,
                    ),
                    (
                        "period",
                        Arc::new(StringArray::from(
                            incidents.iter().map(|r| r.1).collect::<Vec<_>>(),
                        )) as ArrayRef,
                    ),
                    (
                        "incident",
                        Arc::new(StringArray::from(
                            incidents.iter().map(|r| r.2.as_str()).collect::<Vec<_>>(),
                        )) as ArrayRef,
                    ),
                ])
                .unwrap(),
                partitions,
                reverse,
            );
            let totals = |input: DataFrame| {
                input
                    .aggregate(
                        vec![col("service")],
                        vec![
                            count(col("incident"))
                                .filter(col("period").eq(lit("prior")))
                                .build()
                                .unwrap()
                                .alias("prior"),
                            count(col("incident"))
                                .filter(col("period").eq(lit("current")))
                                .build()
                                .unwrap()
                                .alias("current"),
                        ],
                    )
                    .unwrap()
                    .with_column("increase", col("current") - col("prior"))
                    .unwrap()
            };
            assert_eq!(
                rows(
                    totals(input.clone())
                        .sort(vec![col("service").sort(true, false)])
                        .unwrap()
                )
                .await,
                json!([{"service":"A","prior":2,"current":8,"increase":6},{"service":"B","prior":6,"current":7,"increase":1},{"service":"C","prior":1,"current":0,"increase":-1}])
            );
            let winner = |input: DataFrame| {
                totals(input)
                    .sort(vec![
                        col("increase").sort(false, false),
                        col("service").sort(true, false),
                    ])
                    .unwrap()
                    .limit(0, Some(1))
                    .unwrap()
            };
            assert_eq!(
                rows(
                    winner(input.clone())
                        .select(vec![col("service"), col("increase")])
                        .unwrap()
                )
                .await,
                json!([{"service":"A","increase":6}])
            );
            // A report-driven prefilter keeps only B, changing the question.
            assert_eq!(
                rows(
                    winner(input.clone().filter(col("service").eq(lit("B"))).unwrap())
                        .select(vec![col("service"), col("increase")])
                        .unwrap()
                )
                .await,
                json!([{"service":"B","increase":1}])
            );
            // count(Boolean) also counts false; it is not count_if.
            let wrong_count = input
                .clone()
                .aggregate(
                    vec![col("service")],
                    vec![count(col("period").eq(lit("current"))).alias("n")],
                )
                .unwrap()
                .sort(vec![col("service").sort(true, false)])
                .unwrap();
            assert_eq!(
                rows(wrong_count).await,
                json!([{"service":"A","n":10},{"service":"B","n":13},{"service":"C","n":1}])
            );
            let duplicate = input
                .clone()
                .union(
                    input
                        .filter(col("incident").eq(lit("A-current-0")))
                        .unwrap(),
                )
                .unwrap();
            assert_eq!(
                rows(
                    winner(duplicate)
                        .select(vec![col("current"), col("increase")])
                        .unwrap()
                )
                .await,
                json!([{"current":9,"increase":7}])
            );

            // C2/C3: source membership and the scoring relation are different.
            // Feature values are fixture inputs, not a BM25 implementation.
            let passages = frame(
                &ctx,
                RecordBatch::try_from_iter([
                    (
                        "id",
                        Arc::new(StringArray::from(vec!["p1", "p2", "p3", "p4"])) as ArrayRef,
                    ),
                    (
                        "dense_rank",
                        Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as ArrayRef,
                    ),
                    (
                        "lexical_rank",
                        Arc::new(Int64Array::from(vec![Some(1), None, None, None])) as ArrayRef,
                    ),
                    (
                        "feature",
                        Arc::new(Float64Array::from(vec![
                            Some(3.0),
                            Some(2.0),
                            Some(0.0),
                            None,
                        ])) as ArrayRef,
                    ),
                ])
                .unwrap(),
                partitions,
                reverse,
            );
            let selected = passages
                .clone()
                .sort(vec![
                    col("dense_rank").sort(true, false),
                    col("id").sort(true, false),
                ])
                .unwrap()
                .limit(0, Some(2))
                .unwrap();
            let edges = frame(
                &ctx,
                RecordBatch::try_from_iter([
                    (
                        "passage",
                        Arc::new(StringArray::from(vec!["p1", "p1", "p2", "p3"])) as ArrayRef,
                    ),
                    (
                        "project",
                        Arc::new(StringArray::from(vec!["P", "P", "P", "P"])) as ArrayRef,
                    ),
                ])
                .unwrap(),
                partitions,
                reverse,
            );
            let grouped = |targets: DataFrame| {
                targets
                    .join(edges.clone(), JoinType::Inner, &["id"], &["passage"], None)
                    .unwrap()
                    .aggregate(
                        vec![col("project")],
                        vec![
                            count(col("id")).alias("binding_rows"),
                            count_distinct(col("id")).alias("passages"),
                            min(col("dense_rank")).alias("best_rank"),
                        ],
                    )
                    .unwrap()
            };
            assert_eq!(
                rows(grouped(selected.clone())).await,
                json!([{"project":"P","binding_rows":3,"passages":2,"best_rank":1}])
            );
            assert_eq!(
                rows(grouped(passages.clone())).await,
                json!([{"project":"P","binding_rows":4,"passages":3,"best_rank":1}])
            );
            assert_eq!(
                rows(
                    selected
                        .clone()
                        .select(vec![col("id"), col("lexical_rank"), col("feature")])
                        .unwrap()
                        .sort(vec![col("id").sort(true, false)])
                        .unwrap()
                )
                .await,
                json!([{"id":"p1","lexical_rank":1,"feature":3.0},{"id":"p2","feature":2.0}])
            );
            let wrong_scoring = selected.filter(col("lexical_rank").is_not_null()).unwrap();
            assert_eq!(
                rows(wrong_scoring.select(vec![col("id")]).unwrap()).await,
                json!([{"id":"p1"}])
            );
            // Zero and null fixture features remain observations, not filters.
            // Their production meaning and scorer policy are not proved here.
            assert_eq!(
                rows(
                    passages
                        .select(vec![col("id"), col("feature")])
                        .unwrap()
                        .sort(vec![col("id").sort(true, false)])
                        .unwrap()
                )
                .await,
                json!([{"id":"p1","feature":3.0},{"id":"p2","feature":2.0},{"id":"p3","feature":0.0},{"id":"p4"}])
            );

            // C4: two parent rows can import the same entity but carry different
            // computed facts. Enrichment must preserve both rows and empty B.
            let parents = frame(
                &ctx,
                RecordBatch::try_from_iter([
                    (
                        "parent_row",
                        Arc::new(StringArray::from(vec!["a0", "a1", "b0"])) as ArrayRef,
                    ),
                    (
                        "entity",
                        Arc::new(StringArray::from(vec!["A", "A", "B"])) as ArrayRef,
                    ),
                    (
                        "prior_count",
                        Arc::new(Int64Array::from(vec![2, 3, 6])) as ArrayRef,
                    ),
                ])
                .unwrap(),
                partitions,
                reverse,
            );
            let reports = frame(
                &ctx,
                RecordBatch::try_from_iter([
                    (
                        "service_id",
                        Arc::new(StringArray::from(vec!["A", "A", "A"])) as ArrayRef,
                    ),
                    (
                        "report_id",
                        Arc::new(StringArray::from(vec!["r1", "r2", "r3"])) as ArrayRef,
                    ),
                    (
                        "report_text",
                        Arc::new(StringArray::from(vec!["first", "second", "third"])) as ArrayRef,
                    ),
                    (
                        "rank",
                        Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
                    ),
                ])
                .unwrap(),
                partitions,
                reverse,
            );
            let items = parents
                .clone()
                .join(reports, JoinType::Inner, &["entity"], &["service_id"], None)
                .unwrap();
            let report_value = || {
                named_struct(vec![
                    lit("id"),
                    col("report_id"),
                    lit("text"),
                    col("report_text"),
                ])
            };
            let collect_items = |input: DataFrame| {
                input
                    .window(vec![
                        row_number()
                            .partition_by(vec![col("parent_row")])
                            .order_by(vec![
                                col("rank").sort(true, false),
                                col("report_id").sort(true, false),
                                col("path").sort(true, false),
                            ])
                            .build()
                            .unwrap()
                            .alias("position"),
                    ])
                    .unwrap()
                    .filter(col("position").lt_eq(lit(2u64)))
                    .unwrap()
                    .aggregate(
                        vec![col("parent_row").alias("collection_parent")],
                        vec![
                            array_agg(report_value())
                                .order_by(vec![
                                    col("rank").sort(true, false),
                                    col("report_id").sort(true, false),
                                    col("path").sort(true, false),
                                ])
                                .build()
                                .unwrap()
                                .alias("reports"),
                        ],
                    )
                    .unwrap()
            };
            let cut_items = items
                .clone()
                .filter(col("rank").lt_eq(lit(2i64)))
                .unwrap()
                .with_column("path", lit(0i64))
                .unwrap();
            let collections = collect_items(cut_items.clone());
            // Fan-out after the distinct-target window still counts binding
            // rows at collection. Silent deduplication changes r1/r1 to r1/r2.
            let expanded_items = cut_items
                .clone()
                .union(
                    cut_items
                        .filter(col("report_id").eq(lit("r1")))
                        .unwrap()
                        .with_column("path", lit(1i64))
                        .unwrap(),
                )
                .unwrap();
            assert_eq!(
                rows(
                    collect_items(expanded_items.clone())
                        .sort(vec![col("collection_parent").sort(true, false)])
                        .unwrap()
                )
                .await,
                json!([{"collection_parent":"a0","reports":[{"id":"r1","text":"first"},{"id":"r1","text":"first"}]},{"collection_parent":"a1","reports":[{"id":"r1","text":"first"},{"id":"r1","text":"first"}]}])
            );
            assert_eq!(
                rows(
                    expanded_items
                        .aggregate(vec![], vec![count(col("report_id")).alias("input_rows")])
                        .unwrap()
                )
                .await,
                json!([{"input_rows":6}])
            );
            let DataType::List(item) = collections
                .schema()
                .field_with_unqualified_name("reports")
                .unwrap()
                .data_type()
                .clone()
            else {
                panic!("collection must retain a list type");
            };
            assert!(matches!(item.data_type(), DataType::Struct(fields) if fields.len() == 2));
            let empty_list = ScalarValue::new_list(&[], item.data_type(), item.is_nullable());
            let assembled = parents
                .clone()
                .join(
                    collections,
                    JoinType::Left,
                    &["parent_row"],
                    &["collection_parent"],
                    None,
                )
                .unwrap()
                .with_column(
                    "reports",
                    coalesce(vec![col("reports"), lit(ScalarValue::List(empty_list))]),
                )
                .unwrap();
            let owners = frame(
                &ctx,
                RecordBatch::try_from_iter([
                    (
                        "owned_entity",
                        Arc::new(StringArray::from(vec!["A"])) as ArrayRef,
                    ),
                    (
                        "owner_id",
                        Arc::new(StringArray::from(vec!["person-1"])) as ArrayRef,
                    ),
                    (
                        "owner_name",
                        Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
                    ),
                ])
                .unwrap(),
                partitions,
                reverse,
            );
            let with_owner = assembled
                .join(
                    owners.clone(),
                    JoinType::Left,
                    &["entity"],
                    &["owned_entity"],
                    None,
                )
                .unwrap()
                .with_column(
                    "owner_value",
                    named_struct(vec![lit("name"), col("owner_name")]),
                )
                .unwrap();
            let owner_type = with_owner
                .schema()
                .field_with_unqualified_name("owner_value")
                .unwrap()
                .data_type();
            let null_owner = ScalarValue::try_from(owner_type).unwrap();
            let result = with_owner
                .with_column(
                    "owner",
                    when(col("owner_id").is_not_null(), col("owner_value"))
                        .otherwise(lit(null_owner))
                        .unwrap(),
                )
                .unwrap()
                .select(vec![
                    col("parent_row"),
                    col("prior_count"),
                    col("owner"),
                    col("reports"),
                ])
                .unwrap()
                .sort(vec![col("parent_row").sort(true, false)])
                .unwrap();
            assert_eq!(
                rows(result).await,
                json!([
                    {"parent_row":"a0","prior_count":2,"owner":{},"reports":[{"id":"r1","text":"first"},{"id":"r2","text":"second"}]},
                    {"parent_row":"a1","prior_count":3,"owner":{},"reports":[{"id":"r1","text":"first"},{"id":"r2","text":"second"}]},
                    {"parent_row":"b0","prior_count":6,"reports":[]}
                ])
            );
            // A global array aggregate over zero child rows yields null; an
            // outer join before aggregation instead produces a [null] item.
            let no_items = items.clone().filter(col("entity").eq(lit("B"))).unwrap();
            assert_eq!(
                rows(
                    no_items
                        .aggregate(vec![], vec![array_agg(report_value()).alias("reports")])
                        .unwrap()
                )
                .await,
                json!([{}])
            );
            let null_child = parents
                .clone()
                .filter(col("entity").eq(lit("B")))
                .unwrap()
                .join(
                    items
                        .clone()
                        .select(vec![
                            col("parent_row").alias("child_parent"),
                            col("report_id"),
                            col("report_text"),
                        ])
                        .unwrap(),
                    JoinType::Left,
                    &["parent_row"],
                    &["child_parent"],
                    None,
                )
                .unwrap();
            assert_eq!(
                rows(
                    null_child
                        .clone()
                        .aggregate(
                            vec![col("parent_row")],
                            vec![array_agg(col("report_id")).alias("reports")]
                        )
                        .unwrap()
                )
                .await,
                json!([{"parent_row":"b0","reports":[null]}])
            );
            assert_eq!(
                rows(
                    null_child
                        .aggregate(
                            vec![col("parent_row")],
                            vec![array_agg(report_value()).alias("reports")]
                        )
                        .unwrap()
                )
                .await,
                json!([{"parent_row":"b0","reports":[{}]}])
            );
            // Cardinality must be checked before building an optional object.
            // A raw left join accepts two owners and multiplies parent rows.
            let ambiguous = owners.clone().union(owners).unwrap();
            assert_eq!(
                rows(
                    ambiguous
                        .aggregate(
                            vec![col("owned_entity")],
                            vec![count(col("owner_id")).alias("owner_count")]
                        )
                        .unwrap()
                        .filter(col("owner_count").gt(lit(1i64)))
                        .unwrap()
                )
                .await,
                json!([{"owned_entity":"A","owner_count":2}])
            );
        }
    }
}
