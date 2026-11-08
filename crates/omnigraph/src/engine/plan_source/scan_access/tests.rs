//! GQT observes rows and access claims, but cannot inspect Lance nodes or metadata gates.
use super::*;
use arrow_array::{Array, Int32Array, RecordBatch, RecordBatchIterator, StringArray};
use datafusion::functions::expr_fn::{contains, random, starts_with};
use datafusion::prelude::{ident, lit};
use futures::TryStreamExt;
use lance::dataset::{WriteMode, WriteParams};
use lance::index::DatasetIndexExt;
use lance_file::version::LanceFileVersion;
use lance_index::IndexType;
use lance_index::scalar::{FullTextSearchQuery, InvertedIndexParams, ScalarIndexParams};
use serde_json::{Value, json};

fn batch(rows: &[(i32, i32, i32, &str, i32)]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("x", DataType::Int32, false),
        Field::new("y", DataType::Int32, false),
        Field::new("text", DataType::Utf8, false),
        Field::new("flag", DataType::Int32, false),
        Field::new("nullable", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from_iter_values(rows.iter().map(|r| r.0))),
            Arc::new(Int32Array::from_iter_values(rows.iter().map(|r| r.1))),
            Arc::new(Int32Array::from_iter_values(rows.iter().map(|r| r.2))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.3))),
            Arc::new(Int32Array::from_iter_values(rows.iter().map(|r| r.4))),
            Arc::new(StringArray::from_iter(rows.iter().map(|r| {
                (r.0 != 2).then_some(match r.0 {
                    0 => "a_b0",
                    1 => "axb0",
                    _ => "a_b1",
                })
            }))),
        ],
    )
    .unwrap()
}

fn reader(batch: RecordBatch) -> impl arrow_array::RecordBatchReader + Send + 'static {
    let schema = batch.schema();
    RecordBatchIterator::new(vec![Ok(batch)], schema)
}

async fn fixture(format: LanceFileVersion) -> (tempfile::TempDir, Dataset) {
    let dir = tempfile::tempdir().unwrap();
    let mut ds = Dataset::write(
        reader(batch(&[
            (0, 1, 9, "abcd", 1),
            (1, 2, 2, "abcXbcd", 2),
            (2, 1, 2, "none", 0),
            (3, 3, 8, "abcd", 2),
        ])),
        dir.path().join("data.lance").to_str().unwrap(),
        Some(WriteParams {
            mode: WriteMode::Create,
            data_storage_version: Some(format),
            enable_stable_row_ids: false,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    for (column, kind) in [
        ("x", IndexType::BTree),
        ("y", IndexType::BTree),
        ("nullable", IndexType::BTree),
        ("text", IndexType::NGram),
    ] {
        ds.create_index_builder(&[column], kind, &ScalarIndexParams::default())
            .name(format!("{column}_probe"))
            .replace(true)
            .await
            .unwrap();
    }
    (dir, ds)
}

fn nodes(plan: &Arc<dyn ExecutionPlan>) -> Vec<Arc<dyn ExecutionPlan>> {
    let mut pending = vec![Arc::clone(plan)];
    let mut found = Vec::new();
    while let Some(node) = pending.pop() {
        if let Some(read) = node.downcast_ref::<FilteredReadExec>() {
            if let Some(input) = read.index_input() {
                pending.push(Arc::clone(input));
                found.push(node);
                continue;
            }
        }
        pending.extend(node.children().into_iter().cloned());
        found.push(node);
    }
    found
}

fn complete_signature(query: &ScalarIndexExpr) -> Value {
    match query {
        ScalarIndexExpr::Query(search) => json!({
            "column": search.column, "index": search.index_name,
            "type": search.index_type, "query": search.query.format(&search.column),
            "needs_recheck": search.needs_recheck,
            "fragments": search.fragment_bitmap.as_ref().map(|bitmap| bitmap.iter().collect::<Vec<_>>()),
        }),
        ScalarIndexExpr::And(a, b) => {
            json!({"and": [complete_signature(a), complete_signature(b)]})
        }
        ScalarIndexExpr::Or(a, b) => json!({"or": [complete_signature(a), complete_signature(b)]}),
        ScalarIndexExpr::Not(a) => json!({"not": complete_signature(a)}),
    }
}

struct Expected<'a> {
    rows: &'a [i32],
    residual: bool,
    recheck: bool,
    coverage: &'a [u32],
}

async fn check(
    ds: &Dataset,
    filter: Expr,
    enabled: bool,
    legacy: bool,
    expected: Expected<'_>,
) -> ScanAccess {
    let mut scanner = ds.scan();
    scanner.filter_expr(filter);
    scanner.prefilter(true);
    scanner.use_scalar_index(enabled);
    let plan = scanner.create_plan().await.unwrap();
    let configured = scanner.get_expr_filter().unwrap();
    let all_nodes = nodes(&plan);
    let actual = inspect(&plan, ds, configured.clone()).await.unwrap();
    assert_eq!(
        matches!(actual, ScanAccess::IndexProbe { .. }),
        !expected.coverage.is_empty(),
        "{actual:?}"
    );
    if let ScanAccess::IndexProbe { query, residual } = &actual {
        assert_eq!(residual.is_some(), expected.residual, "{actual:?}");
        if expected.residual && !expected.recheck {
            let text = residual.as_ref().unwrap();
            assert!(text.contains("flag"), "{text}");
            assert!(
                !text.contains("x =") && !text.contains("y ="),
                "fallback predicate leaked into residual: {text}"
            );
        }
        let raw = legacy_query(ds, configured.unwrap()).await.unwrap();
        assert_eq!(raw.needs_recheck(), expected.recheck);
        let mut pending = vec![&raw];
        while let Some(expr) = pending.pop() {
            match expr {
                ScalarIndexExpr::Query(leaf) => assert_eq!(
                    leaf.fragment_bitmap
                        .as_ref()
                        .map(|bitmap| bitmap.iter().collect::<Vec<_>>()),
                    Some(expected.coverage.to_vec()),
                ),
                ScalarIndexExpr::And(a, b) | ScalarIndexExpr::Or(a, b) => {
                    pending.extend([a.as_ref(), b.as_ref()])
                }
                ScalarIndexExpr::Not(a) => pending.push(a),
            }
        }
        let materialized = all_nodes
            .iter()
            .filter(|node| node.is::<MaterializeIndexExec>())
            .count();
        let scalar: Vec<_> = all_nodes
            .iter()
            .filter_map(|node| node.downcast_ref::<ScalarIndexExec>())
            .collect();
        if legacy {
            assert_eq!(materialized, 1);
            assert!(scalar.is_empty());
            assert_eq!(*query, mirror(&raw));
        } else {
            assert_eq!(materialized, 0);
            assert_eq!(scalar.len(), 1);
            assert_eq!(
                complete_signature(scalar[0].expr()),
                complete_signature(&raw.optimize())
            );
            assert_eq!(*query, mirror(scalar[0].expr()));
            assert!(all_nodes.iter().any(|node| {
                node.downcast_ref::<FilteredReadExec>().is_some_and(|read| {
                    read.index_input().is_some() && read.options().full_filter.is_some()
                })
            }));
        }
    } else {
        assert!(
            !all_nodes
                .iter()
                .any(|node| node.is::<ScalarIndexExec>() || node.is::<MaterializeIndexExec>())
        );
    }
    let batches: Vec<RecordBatch> = lance_datafusion::exec::execute_plan(plan, Default::default())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut ids = Vec::new();
    for batch in batches {
        let column = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        ids.extend(column.values().iter().copied());
    }
    ids.sort_unstable();
    assert_eq!(ids, expected.rows, "legacy={legacy}, access={actual:?}");
    actual
}

#[tokio::test]
async fn scalar_plans_preserve_tree_residual_recheck_and_partial_rows() {
    for (format, legacy) in [
        (LanceFileVersion::Legacy, true),
        (LanceFileVersion::V2_2, false),
    ] {
        let (_dir, mut ds) = fixture(format).await;
        let coverage: Vec<u32> = ds.fragments().iter().map(|f| f.id as u32).collect();
        let indexed = ident("x").eq(lit(1i32)).or(ident("y").eq(lit(2i32)));
        let residual = ident("flag").eq(lit(1i32)).or(ident("flag").eq(lit(2i32)));
        let access = check(
            &ds,
            indexed.clone(),
            true,
            legacy,
            Expected {
                rows: &[0, 1, 2],
                residual: false,
                recheck: false,
                coverage: &coverage,
            },
        )
        .await;
        assert!(matches!(
            access,
            ScanAccess::IndexProbe {
                query: IndexQuery::Or { .. },
                ..
            }
        ));
        let nullable_batches = ds
            .scan()
            .project(&["nullable"])
            .unwrap()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            nullable_batches
                .iter()
                .map(|b| b.column(0).null_count())
                .sum::<usize>(),
            1
        );
        check(
            &ds,
            ident("nullable").is_null(),
            false,
            legacy,
            Expected {
                rows: &[2],
                residual: false,
                recheck: false,
                coverage: &[],
            },
        )
        .await;
        for (filter, rows, refine, recheck) in [
            (ident("nullable").is_null(), vec![2], false, false),
            (ident("nullable").is_not_null(), vec![0, 1, 3], false, false),
            (
                starts_with(ident("nullable"), lit("a_b")),
                vec![0, 3],
                false,
                false,
            ),
            (!ident("x").eq(lit(1i32)), vec![1, 3], false, false),
            (
                starts_with(ident("text"), lit("abcd")),
                vec![0, 3],
                true,
                true,
            ),
            (
                indexed.clone().and(residual.clone()),
                vec![0, 1],
                true,
                false,
            ),
            (
                contains(ident("text"), lit("abcd")).and(residual.clone()),
                vec![0, 3],
                true,
                true,
            ),
            (
                lit(1i64).lt_eq(ident("x")).and(ident("x").lt_eq(lit(2i64))),
                vec![0, 1, 2],
                false,
                false,
            ),
        ] {
            check(
                &ds,
                filter,
                true,
                legacy,
                Expected {
                    rows: &rows,
                    residual: refine,
                    recheck,
                    coverage: &coverage,
                },
            )
            .await;
        }
        ds.append(
            reader(batch(&[(4, 1, 0, "abcd", 1), (5, 9, 0, "abcXbcd", 2)])),
            None,
        )
        .await
        .unwrap();
        assert!(ds.fragments().len() > coverage.len());
        check(
            &ds,
            indexed.and(residual.clone()),
            true,
            legacy,
            Expected {
                rows: &[0, 1, 4],
                residual: true,
                recheck: false,
                coverage: &coverage,
            },
        )
        .await;
        check(
            &ds,
            contains(ident("text"), lit("abcd")).and(residual),
            true,
            legacy,
            Expected {
                rows: &[0, 3, 4],
                residual: true,
                recheck: true,
                coverage: &coverage,
            },
        )
        .await;
    }
}

#[tokio::test]
async fn scanner_gates_win_over_filter_reconstruction() {
    for (format, legacy) in [
        (LanceFileVersion::Legacy, true),
        (LanceFileVersion::V2_2, false),
    ] {
        let (_dir, mut ds) = fixture(format).await;
        let filter = ident("x").eq(lit(1i32));
        assert!(legacy_query(&ds, filter.clone()).await.is_ok());
        assert_eq!(
            check(
                &ds,
                filter.clone(),
                false,
                legacy,
                Expected {
                    rows: &[0, 2],
                    residual: false,
                    recheck: false,
                    coverage: &[],
                }
            )
            .await,
            ScanAccess::Sequential
        );
        let manifest = Arc::make_mut(&mut ds.manifest);
        Arc::make_mut(&mut manifest.fragments)[0].physical_rows = None;
        if legacy {
            assert_eq!(
                check(
                    &ds,
                    filter,
                    true,
                    legacy,
                    Expected {
                        rows: &[0, 2],
                        residual: false,
                        recheck: false,
                        coverage: &[],
                    }
                )
                .await,
                ScanAccess::Sequential
            );
        } else {
            let mut scanner = ds.scan();
            scanner.filter_expr(filter);
            scanner.prefilter(true);
            assert!(
                scanner
                    .create_plan()
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("missing row count stats")
            );
        }
    }
}

#[test]
fn expression_guard_rejects_stable_and_volatile_functions() {
    assert!(immutable(&ident("x").eq(lit(1i32))).unwrap());
    assert!(immutable(&contains(ident("text"), lit("abcd"))).unwrap());
    assert!(!immutable(&random().gt(lit(0.5f64))).unwrap());
    let now = datafusion::functions::datetime::now(&Default::default()).call(vec![]);
    assert!(!immutable(&now.is_not_null()).unwrap());
}

#[tokio::test]
async fn first_scalar_parser_and_direct_exact_prefilter_follow_lance() {
    for format in [LanceFileVersion::Legacy, LanceFileVersion::V2_2] {
        let (_dir, mut ds) = fixture(format).await;
        ds.drop_index("nullable_probe").await.unwrap();
        ds.create_index(
            &["nullable"],
            IndexType::Inverted,
            Some("tag_fts".into()),
            &InvertedIndexParams::default()
                .stem(false)
                .max_token_length(None),
            false,
        )
        .await
        .unwrap();
        ds.create_index(
            &["nullable"],
            IndexType::BTree,
            Some("tag_btree".into()),
            &ScalarIndexParams::default(),
            false,
        )
        .await
        .unwrap();
        let indices = ds.load_indices().await.unwrap();
        let tags: Vec<_> = indices
            .iter()
            .filter(|i| i.name.starts_with("tag_"))
            .map(|i| i.name.as_str())
            .collect();
        assert_eq!(tags, ["tag_fts", "tag_btree"]);
        check(
            &ds,
            ident("nullable").eq(lit("a_b0")),
            true,
            format == LanceFileVersion::Legacy,
            Expected {
                rows: &[0],
                residual: false,
                recheck: false,
                coverage: &[],
            },
        )
        .await;
        ds.create_index(
            &["text"],
            IndexType::Inverted,
            Some("text_fts".into()),
            &InvertedIndexParams::default(),
            false,
        )
        .await
        .unwrap();
        let mut scanner = ds.scan();
        scanner
            .full_text_search(
                FullTextSearchQuery::new("abcd".into())
                    .with_column("text".into())
                    .unwrap(),
            )
            .unwrap();
        scanner.filter_expr(ident("x").eq(lit(1i32)));
        scanner.prefilter(true);
        scanner.project(&["id"]).unwrap();
        let plan = scanner.create_plan().await.unwrap();
        let all = nodes(&plan);
        assert!(!all.iter().any(|node| node.is::<MaterializeIndexExec>()));
        let scalar: Vec<_> = all
            .iter()
            .filter_map(|node| node.downcast_ref::<ScalarIndexExec>())
            .collect();
        assert_eq!(scalar.len(), 1);
        let access = inspect(&plan, &ds, scanner.get_expr_filter().unwrap())
            .await
            .unwrap();
        assert_eq!(
            access,
            ScanAccess::IndexProbe {
                query: mirror(scalar[0].expr()),
                residual: None
            }
        );
        let batches: Vec<RecordBatch> =
            lance_datafusion::exec::execute_plan(plan, Default::default())
                .unwrap()
                .try_collect()
                .await
                .unwrap();
        let ids: Vec<_> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        assert_eq!(ids, [0]);
    }
}

type FilterSignature = (String, String, Option<Expr>, Option<Expr>, Option<Value>);

pub(super) fn filter_signature(plan: &Arc<dyn ExecutionPlan>) -> Vec<FilterSignature> {
    let mut pending = vec![("root".to_owned(), plan.clone())];
    let mut signature = Vec::new();
    while let Some((path, node)) = pending.pop() {
        if let Some(read) = node.downcast_ref::<FilteredReadExec>() {
            signature.push((
                path.clone(),
                "FilteredRead".into(),
                read.options().full_filter.clone(),
                read.options().refine_filter.clone(),
                None,
            ));
            if let Some(input) = read.index_input() {
                pending.push((format!("{path}/index"), input.clone()));
            }
        }
        if let Some(filter) = node.downcast_ref::<LanceFilterExec>() {
            signature.push((
                path.clone(),
                "Filter".into(),
                Some(filter.expr().clone()),
                None,
                None,
            ));
        }
        if let Some(index) = node.downcast_ref::<ScalarIndexExec>() {
            signature.push((
                path.clone(),
                "ScalarIndex".into(),
                None,
                None,
                Some(complete_signature(index.expr())),
            ));
        }
        if node.is::<MaterializeIndexExec>() {
            signature.push((path.clone(), "MaterializeIndex".into(), None, None, None));
        }
        for (i, child) in node.children().into_iter().enumerate() {
            pending.push((format!("{path}/{i}"), child.clone()));
        }
    }
    signature
}
