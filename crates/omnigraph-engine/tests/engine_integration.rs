use std::collections::HashSet;

use arrow_array::{
    Array, FixedSizeListArray, Float32Array, Float64Array, Int32Array, Int64Array, RecordBatch,
    StringArray, UInt64Array,
};
use lance::Dataset;
use lance_index::DatasetIndexExt;

use nanograph::query::parser::parse_query;
use nanograph::query::typecheck::{CheckedQuery, typecheck_query, typecheck_query_decl};
use nanograph::schema::parser::parse_schema;
use nanograph::schema_ir::SchemaIR;
use nanograph::store::database::{Database, LoadMode};
use nanograph::store::export::build_export_rows_at_path;
use nanograph::store::manifest::GraphManifest;
use nanograph::store::txlog::read_visible_cdc_entries;
use nanograph::store::{scalar_index_name, vector_index_name};
use nanograph::{MutationExecResult, ParamMap, build_catalog};

fn test_schema() -> &'static str {
    r#"
node Person {
    name: String @key
    age: I32?
}
node Company {
    name: String @key
}
edge Knows: Person -> Person {
    since: Date?
}
edge WorksAt: Person -> Company
"#
}

fn indexed_test_schema() -> &'static str {
    r#"
node Person {
    name: String
    email: String @index
}
"#
}

fn list_membership_test_schema() -> &'static str {
    r#"
node Person {
    name: String @key
    tags: [String]?
}
"#
}

fn list_membership_test_data() -> &'static str {
    r#"{"type":"Person","data":{"name":"Alice","tags":["rust","db"]}}
{"type":"Person","data":{"name":"Bob","tags":["graph"]}}
{"type":"Person","data":{"name":"Cara","tags":["rust"]}}
{"type":"Person","data":{"name":"Dylan","tags":null}}
"#
}

fn indexed_test_data() -> &'static str {
    r#"{"type":"Person","data":{"name":"Alice","email":"alice@example.com"}}
{"type":"Person","data":{"name":"Bob","email":"bob@example.com"}}
"#
}

fn vector_test_schema() -> &'static str {
    r#"
node Doc {
    slug: String @key
    title: String
    embedding: Vector(4) @index
}
"#
}

fn vector_test_data() -> &'static str {
    r#"{"type":"Doc","data":{"slug":"a","title":"A","embedding":[1.0,0.0,0.0,0.0]}}
{"type":"Doc","data":{"slug":"b","title":"B","embedding":[0.0,1.0,0.0,0.0]}}
{"type":"Doc","data":{"slug":"c","title":"C","embedding":[-1.0,0.0,0.0,0.0]}}
{"type":"Doc","data":{"slug":"d","title":"D","embedding":[0.7,0.7,0.0,0.0]}}
"#
}

fn vector_filtered_test_schema() -> &'static str {
    r#"
node Doc {
    slug: String @key
    topic: String
    embedding: Vector(4) @index
}
"#
}

fn vector_filtered_test_data() -> &'static str {
    r#"{"type":"Doc","data":{"slug":"a","topic":"x","embedding":[1.0,0.0,0.0,0.0]}}
{"type":"Doc","data":{"slug":"b","topic":"y","embedding":[0.0,1.0,0.0,0.0]}}
{"type":"Doc","data":{"slug":"c","topic":"y","embedding":[-1.0,0.0,0.0,0.0]}}
"#
}

fn keyed_mutation_schema() -> &'static str {
    r#"
node Person {
    name: String @key
    age: I32?
}
edge Knows: Person -> Person
"#
}

fn keyed_mutation_data() -> &'static str {
    r#"{"type":"Person","data":{"name":"Alice","age":30}}
{"type":"Person","data":{"name":"Bob","age":25}}
{"edge":"Knows","from":"Alice","to":"Bob"}
"#
}

fn test_data() -> &'static str {
    r#"{"type":"Person","data":{"name":"Alice","age":30}}
{"type":"Person","data":{"name":"Bob","age":25}}
{"type":"Person","data":{"name":"Charlie","age":35}}
{"type":"Person","data":{"name":"Diana","age":28}}
{"type":"Company","data":{"name":"Acme"}}
{"type":"Company","data":{"name":"Globex"}}
{"edge":"Knows","from":"Alice","to":"Bob"}
{"edge":"Knows","from":"Alice","to":"Charlie"}
{"edge":"Knows","from":"Bob","to":"Diana"}
{"edge":"WorksAt","from":"Alice","to":"Acme"}
{"edge":"WorksAt","from":"Bob","to":"Globex"}
"#
}

async fn setup_storage() -> Database {
    let db = Database::open_in_memory(test_schema()).await.unwrap();
    db.load_with_mode(test_data(), LoadMode::Overwrite)
        .await
        .unwrap();
    db
}

fn manifest_dataset_version(manifest: &GraphManifest, kind: &str, type_name: &str) -> u64 {
    manifest
        .datasets
        .iter()
        .find(|entry| entry.kind == kind && entry.type_name == type_name)
        .map(|entry| entry.dataset_version)
        .unwrap()
}

async fn run_query_test(query_str: &str, db: &Database) -> Vec<RecordBatch> {
    run_query_test_with_params(query_str, db, &ParamMap::new()).await
}

async fn run_query_test_with_params(
    query_str: &str,
    db: &Database,
    params: &ParamMap,
) -> Vec<RecordBatch> {
    let qf = parse_query(query_str).unwrap();
    let query = &qf.queries[0];
    match db.run_query(query, params).await.unwrap() {
        nanograph::RunResult::Query(result) => result.into_batches(),
        nanograph::RunResult::Mutation(_) => panic!("expected query result"),
    }
}

async fn run_db_query_test_with_params(
    query_str: &str,
    db: &Database,
    params: &ParamMap,
) -> Vec<RecordBatch> {
    let qf = parse_query(query_str).unwrap();
    let query = &qf.queries[0];
    match db.run_query(query, params).await.unwrap() {
        nanograph::RunResult::Query(result) => result.into_batches(),
        nanograph::RunResult::Mutation(_) => panic!("expected query result"),
    }
}

async fn run_db_mutation_test_with_params(
    query_str: &str,
    db: &Database,
    params: &ParamMap,
) -> MutationExecResult {
    let qf = parse_query(query_str).unwrap();
    let query = &qf.queries[0];
    let checked = typecheck_query_decl(db.catalog(), query).unwrap();
    assert!(matches!(checked, CheckedQuery::Mutation(_)));
    match db.run_query(query, params).await.unwrap() {
        nanograph::RunResult::Mutation(result) => MutationExecResult {
            affected_nodes: result.affected_nodes,
            affected_edges: result.affected_edges,
        },
        nanograph::RunResult::Query(_) => panic!("expected mutation result"),
    }
}

async fn export_rows_for_db(db: &Database) -> Vec<serde_json::Value> {
    build_export_rows_at_path(db.path(), true, true)
        .await
        .expect("export rows")
}

fn extract_string_column(batches: &[RecordBatch], col_name: &str) -> Vec<String> {
    let mut result = Vec::new();
    for batch in batches {
        let col_idx = batch.schema().index_of(col_name).unwrap();
        let col = batch.column(col_idx);
        let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..arr.len() {
            if !arr.is_null(i) {
                result.push(arr.value(i).to_string());
            }
        }
    }
    result
}

fn extract_string_pairs(
    batches: &[RecordBatch],
    left_col: &str,
    right_col: &str,
) -> Vec<(String, String)> {
    let mut result = Vec::new();
    for batch in batches {
        let left_idx = batch.schema().index_of(left_col).unwrap();
        let right_idx = batch.schema().index_of(right_col).unwrap();
        let left = batch
            .column(left_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let right = batch
            .column(right_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            if !left.is_null(i) && !right.is_null(i) {
                result.push((left.value(i).to_string(), right.value(i).to_string()));
            }
        }
    }
    result
}

fn extract_u64_column(batches: &[RecordBatch], col_name: &str) -> Vec<u64> {
    let mut result = Vec::new();
    for batch in batches {
        let col_idx = batch.schema().index_of(col_name).unwrap();
        let col = batch.column(col_idx);

        if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    result.push(arr.value(i));
                }
            }
            continue;
        }

        if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    result.push(u64::try_from(arr.value(i)).unwrap());
                }
            }
            continue;
        }

        if let Some(arr) = col.as_any().downcast_ref::<Int32Array>() {
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    result.push(u64::try_from(arr.value(i)).unwrap());
                }
            }
            continue;
        }

        panic!("unsupported numeric type for column {col_name}");
    }
    result
}

fn extract_f64_column(batches: &[RecordBatch], col_name: &str) -> Vec<f64> {
    let mut result = Vec::new();
    for batch in batches {
        let col_idx = batch.schema().index_of(col_name).unwrap();
        let col = batch.column(col_idx);
        let arr = col
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("expected Float64 column");
        for i in 0..arr.len() {
            if !arr.is_null(i) {
                result.push(arr.value(i));
            }
        }
    }
    result
}

#[tokio::test]
async fn test_bind_by_property() {
    let storage = setup_storage().await;
    let mut params = ParamMap::new();
    params.insert(
        "name".to_string(),
        nanograph::query::ast::Literal::String("Alice".to_string()),
    );
    let results = run_query_test_with_params(
        r#"
query q($name: String) {
    match { $p: Person { name: $name } }
    return { $p.name, $p.age }
}
"#,
        &storage,
        &params,
    )
    .await;

    let names = extract_string_column(&results, "name");
    assert_eq!(names, vec!["Alice"]);
}

#[tokio::test]
async fn test_filter_age() {
    let storage = setup_storage().await;
    let results = run_query_test(
        r#"
query q() {
    match {
        $p: Person
        $p.age > 30
    }
    return { $p.name, $p.age }
}
"#,
        &storage,
    )
    .await;

    let names = extract_string_column(&results, "name");
    assert_eq!(names, vec!["Charlie"]);
}

#[tokio::test]
async fn test_list_membership_inline_binding_filter_with_literal() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("db");

    let db = Database::init(&db_path, list_membership_test_schema())
        .await
        .unwrap();
    db.load(list_membership_test_data()).await.unwrap();

    let results = run_db_query_test_with_params(
        r#"
query q() {
    match { $p: Person { tags: "rust" } }
    return { $p.name }
    order { $p.name asc }
}
"#,
        &db,
        &ParamMap::new(),
    )
    .await;

    let names = extract_string_column(&results, "name");
    assert_eq!(names, vec!["Alice", "Cara"]);
}

#[tokio::test]
async fn test_list_membership_inline_binding_filter_with_param() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("db");

    let db = Database::init(&db_path, list_membership_test_schema())
        .await
        .unwrap();
    db.load(list_membership_test_data()).await.unwrap();

    let mut params = ParamMap::new();
    params.insert(
        "tag".to_string(),
        nanograph::query::ast::Literal::String("graph".to_string()),
    );

    let results = run_db_query_test_with_params(
        r#"
query q($tag: String) {
    match { $p: Person { tags: $tag } }
    return { $p.name }
}
"#,
        &db,
        &params,
    )
    .await;

    let names = extract_string_column(&results, "name");
    assert_eq!(names, vec!["Bob"]);
}

#[tokio::test]
async fn test_list_membership_contains_filter_with_param() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("db");

    let db = Database::init(&db_path, list_membership_test_schema())
        .await
        .unwrap();
    db.load(list_membership_test_data()).await.unwrap();

    let mut params = ParamMap::new();
    params.insert(
        "tag".to_string(),
        nanograph::query::ast::Literal::String("rust".to_string()),
    );

    let results = run_db_query_test_with_params(
        r#"
query q($tag: String) {
    match {
        $p: Person
        $p.tags contains $tag
    }
    return { $p.name }
    order { $p.name asc }
}
"#,
        &db,
        &params,
    )
    .await;

    let names = extract_string_column(&results, "name");
    assert_eq!(names, vec!["Alice", "Cara"]);
}

#[tokio::test]
async fn test_inline_binding_filters_with_cross_join() {
    let storage = setup_storage().await;
    let results = run_query_test(
        r#"
query q() {
    match {
        $p: Person { name: "Alice" }
        $q: Person { name: "Bob" }
    }
    return { $p.name as p_name, $q.name as q_name }
}
"#,
        &storage,
    )
    .await;

    let p_names = extract_string_column(&results, "p_name");
    let q_names = extract_string_column(&results, "q_name");
    assert_eq!(p_names, vec!["Alice"]);
    assert_eq!(q_names, vec!["Bob"]);
}

#[tokio::test]
async fn test_multi_scan_explicit_filter_on_single_binding() {
    let storage = setup_storage().await;
    let results = run_query_test(
        r#"
query q() {
    match {
        $p: Person
        $q: Person
        $p.name = "Alice"
    }
    return { $p.name as p_name, $q.name as q_name }
}
"#,
        &storage,
    )
    .await;

    let p_names = extract_string_column(&results, "p_name");
    let mut q_names = extract_string_column(&results, "q_name");
    q_names.sort();

    assert_eq!(p_names.len(), 4);
    assert!(p_names.iter().all(|n| n == "Alice"));
    assert_eq!(q_names, vec!["Alice", "Bob", "Charlie", "Diana"]);
}
#[tokio::test]
async fn test_one_hop_traversal() {
    let storage = setup_storage().await;
    let results = run_query_test(
        r#"
query q() {
    match {
        $p: Person { name: "Alice" }
        $p knows $f
    }
    return { $f.name }
}
"#,
        &storage,
    )
    .await;

    let mut names = extract_string_column(&results, "name");
    names.sort();
    assert_eq!(names, vec!["Bob", "Charlie"]);
}

#[tokio::test]
async fn test_indexed_point_lookup_after_persist_and_reopen() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("db");

    let db = Database::init(&db_path, indexed_test_schema())
        .await
        .unwrap();
    db.load(indexed_test_data()).await.unwrap();

    let person = db
        .schema_ir
        .node_types()
        .find(|n| n.name == "Person")
        .unwrap();
    let dataset_path = db_path
        .join("nodes")
        .join(SchemaIR::dir_name(person.type_id));
    let dataset = Dataset::open(dataset_path.to_string_lossy().as_ref())
        .await
        .unwrap();
    let index_names: HashSet<String> = dataset
        .load_indices()
        .await
        .unwrap()
        .iter()
        .map(|idx| idx.name.clone())
        .collect();
    let expected_index = scalar_index_name(person.type_id, "email");
    assert!(index_names.contains(&expected_index));

    drop(db);
    let reopened = Database::open(&db_path).await.unwrap();

    let mut params = ParamMap::new();
    params.insert(
        "email".to_string(),
        nanograph::query::ast::Literal::String("alice@example.com".to_string()),
    );
    let results = run_db_query_test_with_params(
        r#"
query q($email: String) {
    match {
        $p: Person
        $p.email = $email
    }
    return { $p.name }
}
"#,
        &reopened,
        &params,
    )
    .await;

    let names = extract_string_column(&results, "name");
    assert_eq!(names, vec!["Alice"]);
}

#[tokio::test]
async fn test_nearest_query_on_indexed_vectors() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("db");

    let db = Database::init(&db_path, vector_test_schema())
        .await
        .unwrap();
    db.load(vector_test_data()).await.unwrap();

    let doc = db.schema_ir.node_types().find(|n| n.name == "Doc").unwrap();
    let dataset_path = db_path.join("nodes").join(SchemaIR::dir_name(doc.type_id));
    let dataset = Dataset::open(dataset_path.to_string_lossy().as_ref())
        .await
        .unwrap();
    let index_names: HashSet<String> = dataset
        .load_indices()
        .await
        .unwrap()
        .iter()
        .map(|idx| idx.name.clone())
        .collect();
    let expected_index = vector_index_name(doc.type_id, "embedding");
    assert!(index_names.contains(&expected_index));

    let mut params = ParamMap::new();
    params.insert(
        "q".to_string(),
        nanograph::query::ast::Literal::List(vec![
            nanograph::query::ast::Literal::Float(1.0),
            nanograph::query::ast::Literal::Float(0.0),
            nanograph::query::ast::Literal::Float(0.0),
            nanograph::query::ast::Literal::Float(0.0),
        ]),
    );

    let results = run_db_query_test_with_params(
        r#"
query q($q: Vector(4)) {
    match {
        $d: Doc
    }
    return { $d.slug as slug, nearest($d.embedding, $q) as score }
    order { nearest($d.embedding, $q) }
    limit 2
}
"#,
        &db,
        &params,
    )
    .await;

    let slugs = extract_string_column(&results, "slug");
    assert_eq!(slugs, vec!["a", "d"]);
    let scores = extract_f64_column(&results, "score");
    assert_eq!(scores.len(), 2);
    assert!(scores[0] <= scores[1]);
    assert!(scores[0].abs() < 1e-9);
}

#[tokio::test]
async fn test_vector_projection_on_lance_scan_does_not_panic() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("db");

    let db = Database::init(&db_path, vector_test_schema())
        .await
        .unwrap();
    db.load(vector_test_data()).await.unwrap();

    let mut params = ParamMap::new();
    params.insert(
        "slug".to_string(),
        nanograph::query::ast::Literal::String("a".to_string()),
    );

    let results = run_db_query_test_with_params(
        r#"
query q($slug: String) {
    match {
        $d: Doc { slug: $slug }
    }
    return { $d.slug as slug, $d.embedding as embedding }
    limit 1
}
"#,
        &db,
        &params,
    )
    .await;

    let slugs = extract_string_column(&results, "slug");
    assert_eq!(slugs, vec!["a"]);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_rows(), 1);

    let emb_idx = results[0].schema().index_of("embedding").unwrap();
    let emb = results[0]
        .column(emb_idx)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .expect("embedding should be a FixedSizeListArray");
    assert_eq!(emb.len(), 1);

    let first_embedding = emb.value(0);
    let values = first_embedding
        .as_any()
        .downcast_ref::<Float32Array>()
        .expect("embedding values should be Float32");
    assert_eq!(values.len(), 4);
    assert_eq!(values.value(0), 1.0);
}

#[tokio::test]
async fn test_nearest_filter_prefilter_returns_filtered_result_with_small_limit() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("db");

    let db = Database::init(&db_path, vector_filtered_test_schema())
        .await
        .unwrap();
    db.load(vector_filtered_test_data()).await.unwrap();

    let mut params = ParamMap::new();
    params.insert(
        "topic".to_string(),
        nanograph::query::ast::Literal::String("y".to_string()),
    );
    params.insert(
        "q".to_string(),
        nanograph::query::ast::Literal::List(vec![
            nanograph::query::ast::Literal::Float(1.0),
            nanograph::query::ast::Literal::Float(0.0),
            nanograph::query::ast::Literal::Float(0.0),
            nanograph::query::ast::Literal::Float(0.0),
        ]),
    );

    let results = run_db_query_test_with_params(
        r#"
query q($topic: String, $q: Vector(4)) {
    match {
        $d: Doc { topic: $topic }
    }
    return { $d.slug as slug }
    order { nearest($d.embedding, $q) }
    limit 1
}
"#,
        &db,
        &params,
    )
    .await;

    let slugs = extract_string_column(&results, "slug");
    assert_eq!(slugs, vec!["b"]);
}

#[tokio::test]
async fn test_two_hop_traversal() {
    let storage = setup_storage().await;
    let results = run_query_test(
        r#"
query q() {
    match {
        $p: Person { name: "Alice" }
        $p knows $mid
        $mid knows $fof
    }
    return { $fof.name }
}
"#,
        &storage,
    )
    .await;

    let names = extract_string_column(&results, "name");
    // Alice->Bob->Diana, Alice->Charlie->nobody
    assert_eq!(names, vec!["Diana"]);
}

#[tokio::test]
async fn test_bounded_traversal_exact_two_hops() {
    let storage = setup_storage().await;
    let results = run_query_test(
        r#"
query q() {
    match {
        $p: Person { name: "Alice" }
        $p knows{2,2} $x
    }
    return { $x.name }
}
"#,
        &storage,
    )
    .await;

    let names = extract_string_column(&results, "name");
    assert_eq!(names, vec!["Diana"]);
}

#[tokio::test]
async fn test_bounded_traversal_range_one_to_two_hops() {
    let storage = setup_storage().await;
    let results = run_query_test(
        r#"
query q() {
    match {
        $p: Person { name: "Alice" }
        $p knows{1,2} $x
    }
    return { $x.name }
    order { $x.name asc }
}
"#,
        &storage,
    )
    .await;

    let names = extract_string_column(&results, "name");
    assert_eq!(names, vec!["Bob", "Charlie", "Diana"]);
}

#[tokio::test]
async fn test_unbounded_traversal_is_rejected() {
    let storage = setup_storage().await;
    let qf = parse_query(
        r#"
query q() {
    match {
        $p: Person { name: "Alice" }
        $p knows{1,} $x
    }
    return { $x.name }
}
"#,
    )
    .unwrap();
    let err = typecheck_query(storage.catalog(), &qf.queries[0]).unwrap_err();
    assert!(err.to_string().contains("unbounded traversal is disabled"));
}

#[tokio::test]
async fn test_negation_unemployed() {
    let storage = setup_storage().await;
    let results = run_query_test(
        r#"
query q() {
    match {
        $p: Person
        not { $p worksAt $_ }
    }
    return { $p.name }
}
"#,
        &storage,
    )
    .await;

    let mut names = extract_string_column(&results, "name");
    names.sort();
    // Charlie and Diana don't work anywhere
    assert_eq!(names, vec!["Charlie", "Diana"]);
}

#[tokio::test]
async fn test_negation_inner_filter_applies() {
    let storage = setup_storage().await;
    let results = run_query_test(
        r#"
query q() {
    match {
        $p: Person
        not {
            $p worksAt $c
            $c.name = "Acme"
        }
    }
    return { $p.name }
}
"#,
        &storage,
    )
    .await;

    let mut names = extract_string_column(&results, "name");
    names.sort();
    // Excludes only Alice (worksAt Acme). Bob worksAt Globex and should remain.
    assert_eq!(names, vec!["Bob", "Charlie", "Diana"]);
}

#[tokio::test]
async fn test_aggregation_friend_counts() {
    let storage = setup_storage().await;
    let results = run_query_test(
        r#"
query q() {
    match {
        $p: Person
        $p knows $f
    }
    return {
        $p.name
        count($f) as friends
    }
    order { friends desc }
}
"#,
        &storage,
    )
    .await;

    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 2);

    let names = extract_string_column(&results, "name");
    let friends = extract_u64_column(&results, "friends");
    assert_eq!(names.len(), 2);
    assert_eq!(friends.len(), 2);

    let mut pairs = names.into_iter().zip(friends).collect::<Vec<_>>();
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        pairs,
        vec![("Alice".to_string(), 2), ("Bob".to_string(), 1)]
    );
}

#[tokio::test]
async fn test_order_and_limit() {
    let storage = setup_storage().await;
    let results = run_query_test(
        r#"
query q() {
    match {
        $p: Person
    }
    return { $p.name, $p.age }
    order { $p.age desc }
    limit 2
}
"#,
        &storage,
    )
    .await;

    let names = extract_string_column(&results, "name");
    assert_eq!(names.len(), 2);
    assert_eq!(names[0], "Charlie"); // age 35
    assert_eq!(names[1], "Alice"); // age 30
}

#[tokio::test]
async fn test_limit_without_order() {
    let storage = setup_storage().await;
    let results = run_query_test(
        r#"
query q() {
    match {
        $p: Person
    }
    return { $p.name }
    order { $p.name asc }
    limit 2
}
"#,
        &storage,
    )
    .await;

    let names = extract_string_column(&results, "name");
    assert_eq!(names, vec!["Alice", "Bob"]);
}

#[tokio::test]
async fn test_limit_with_single_scan_filter_pushdown() {
    let storage = setup_storage().await;
    let results = run_query_test(
        r#"
query q() {
    match {
        $p: Person
        $p.age > 25
    }
    return { $p.name }
    limit 2
}
"#,
        &storage,
    )
    .await;

    let names = extract_string_column(&results, "name");
    assert_eq!(names, vec!["Alice", "Charlie"]);
}

#[tokio::test]
async fn test_single_scan_filter_pushdown_parity_with_nonpushdown_tautology() {
    let storage = setup_storage().await;
    let pushed = run_query_test(
        r#"
query q() {
    match {
        $p: Person
        $p.age > 25
    }
    return { $p.name }
    order { $p.name asc }
}
"#,
        &storage,
    )
    .await;
    let mixed = run_query_test(
        r#"
query q() {
    match {
        $p: Person
        $p.age > 25
        $p.name = $p.name
    }
    return { $p.name }
    order { $p.name asc }
}
"#,
        &storage,
    )
    .await;

    let pushed_names = extract_string_column(&pushed, "name");
    let mixed_names = extract_string_column(&mixed, "name");
    assert_eq!(pushed_names, mixed_names);
    assert_eq!(pushed_names, vec!["Alice", "Charlie", "Diana"]);
}

#[tokio::test]
async fn test_multi_scan_filter_pushdown_parity_with_nonpushdown_tautology() {
    let storage = setup_storage().await;
    let pushed = run_query_test(
        r#"
query q() {
    match {
        $p: Person
        $q: Person
        $p.name = "Alice"
    }
    return { $p.name as p_name, $q.name as q_name }
    order { $q.name asc }
}
"#,
        &storage,
    )
    .await;
    let mixed = run_query_test(
        r#"
query q() {
    match {
        $p: Person
        $q: Person
        $p.name = "Alice"
        $p.name = $p.name
    }
    return { $p.name as p_name, $q.name as q_name }
    order { $q.name asc }
}
"#,
        &storage,
    )
    .await;

    let pushed_pairs = extract_string_pairs(&pushed, "p_name", "q_name");
    let mixed_pairs = extract_string_pairs(&mixed, "p_name", "q_name");
    assert_eq!(pushed_pairs, mixed_pairs);
    assert_eq!(
        pushed_pairs,
        vec![
            ("Alice".to_string(), "Alice".to_string()),
            ("Alice".to_string(), "Bob".to_string()),
            ("Alice".to_string(), "Charlie".to_string()),
            ("Alice".to_string(), "Diana".to_string())
        ]
    );
}

#[tokio::test]
async fn test_insert_mutation_query() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("db");
    let db = Database::init(&db_path, test_schema()).await.unwrap();
    db.load(test_data()).await.unwrap();

    let mut params = ParamMap::new();
    params.insert(
        "name".to_string(),
        nanograph::query::ast::Literal::String("Eve".to_string()),
    );
    params.insert(
        "age".to_string(),
        nanograph::query::ast::Literal::Integer(29),
    );
    let result = run_db_mutation_test_with_params(
        r#"
query add_person($name: String, $age: I32) {
    insert Person {
        name: $name
        age: $age
    }
}
"#,
        &db,
        &params,
    )
    .await;
    assert_eq!(result.affected_nodes, 1);
    assert_eq!(result.affected_edges, 0);

    let names = extract_string_column(
        &run_db_query_test_with_params(
            r#"
query q() {
    match { $p: Person { name: "Eve" } }
    return { $p.name }
}
"#,
            &db,
            &ParamMap::new(),
        )
        .await,
        "name",
    );
    assert_eq!(names, vec!["Eve"]);
}

#[tokio::test]
async fn test_insert_edge_mutation_query() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("db");
    let db = Database::init(&db_path, test_schema()).await.unwrap();
    db.load(test_data()).await.unwrap();

    let mut params = ParamMap::new();
    params.insert(
        "from".to_string(),
        nanograph::query::ast::Literal::String("Bob".to_string()),
    );
    params.insert(
        "to".to_string(),
        nanograph::query::ast::Literal::String("Charlie".to_string()),
    );

    let result = run_db_mutation_test_with_params(
        r#"
query add_knows($from: String, $to: String) {
    insert Knows {
        from: $from
        to: $to
    }
}
"#,
        &db,
        &params,
    )
    .await;
    assert_eq!(result.affected_nodes, 0);
    assert_eq!(result.affected_edges, 1);
    let edge_count = extract_u64_column(
        &run_db_query_test_with_params(
            r#"
query q() {
    match {
        $p: Person
        $p knows $f
    }
    return { count($f) as edge_count }
}
"#,
            &db,
            &ParamMap::new(),
        )
        .await,
        "edge_count",
    );
    assert_eq!(edge_count, vec![4]);
}

#[tokio::test]
async fn test_update_mutation_query_preserves_id_and_edges() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("db");
    let db = Database::init(&db_path, keyed_mutation_schema())
        .await
        .unwrap();
    db.load(keyed_mutation_data()).await.unwrap();
    let manifest_before = GraphManifest::read(&db_path).unwrap();

    let before_rows = export_rows_for_db(&db).await;
    let alice_id_before = before_rows
        .iter()
        .find(|row| {
            row.get("type").and_then(serde_json::Value::as_str) == Some("Person")
                && row["data"]["name"].as_str() == Some("Alice")
        })
        .and_then(|row| row["id"].as_u64())
        .unwrap();

    let mut params = ParamMap::new();
    params.insert(
        "name".to_string(),
        nanograph::query::ast::Literal::String("Alice".to_string()),
    );
    params.insert(
        "age".to_string(),
        nanograph::query::ast::Literal::Integer(31),
    );
    let result = run_db_mutation_test_with_params(
        r#"
query update_person($name: String, $age: I32) {
    update Person set { age: $age } where name = $name
}
"#,
        &db,
        &params,
    )
    .await;
    assert_eq!(result.affected_nodes, 1);

    let after_rows = export_rows_for_db(&db).await;
    let alice_after = after_rows
        .iter()
        .find(|row| {
            row.get("type").and_then(serde_json::Value::as_str) == Some("Person")
                && row["data"]["name"].as_str() == Some("Alice")
        })
        .unwrap();
    assert_eq!(alice_after["id"].as_u64(), Some(alice_id_before));
    assert_eq!(alice_after["data"]["age"].as_i64(), Some(31));
    let edge_count = extract_u64_column(
        &run_db_query_test_with_params(
            r#"
query q() {
    match {
        $p: Person
        $p knows $f
    }
    return { count($f) as edge_count }
}
"#,
            &db,
            &ParamMap::new(),
        )
        .await,
        "edge_count",
    );
    assert_eq!(edge_count, vec![1]);
    let manifest_after = GraphManifest::read(&db_path).unwrap();
    assert!(
        manifest_dataset_version(&manifest_after, "node", "Person")
            > manifest_dataset_version(&manifest_before, "node", "Person")
    );
    assert_eq!(
        manifest_dataset_version(&manifest_after, "edge", "Knows"),
        manifest_dataset_version(&manifest_before, "edge", "Knows")
    );

    let cdc = read_visible_cdc_entries(&db_path, manifest_before.db_version, None).unwrap();
    assert!(
        cdc.iter().any(|row| {
            row.db_version == manifest_after.db_version
                && row.op == "update"
                && row.entity_kind == "node"
                && row.type_name == "Person"
        }),
        "expected node update CDC event in latest db version"
    );
}

#[tokio::test]
async fn test_delete_edge_mutation_query() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("db");
    let db = Database::init(&db_path, test_schema()).await.unwrap();
    db.load(test_data()).await.unwrap();

    let mut params = ParamMap::new();
    params.insert(
        "from".to_string(),
        nanograph::query::ast::Literal::String("Alice".to_string()),
    );
    let result = run_db_mutation_test_with_params(
        r#"
query delete_knows($from: String) {
    delete Knows where from = $from
}
"#,
        &db,
        &params,
    )
    .await;
    assert_eq!(result.affected_nodes, 0);
    assert_eq!(result.affected_edges, 2);
    let edge_count = extract_u64_column(
        &run_db_query_test_with_params(
            r#"
query q() {
    match {
        $p: Person
        $p knows $f
    }
    return { count($f) as edge_count }
}
"#,
            &db,
            &ParamMap::new(),
        )
        .await,
        "edge_count",
    );
    assert_eq!(edge_count, vec![1]);
}

#[tokio::test]
async fn test_delete_mutation_query_cascades_edges() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("db");
    let db = Database::init(&db_path, test_schema()).await.unwrap();
    db.load(test_data()).await.unwrap();

    let mut params = ParamMap::new();
    params.insert(
        "name".to_string(),
        nanograph::query::ast::Literal::String("Alice".to_string()),
    );
    let result = run_db_mutation_test_with_params(
        r#"
query delete_person($name: String) {
    delete Person where name = $name
}
"#,
        &db,
        &params,
    )
    .await;
    assert_eq!(result.affected_nodes, 1);
    assert_eq!(result.affected_edges, 3);

    let people = run_db_query_test_with_params(
        r#"
query q() {
    match { $p: Person }
    return { $p.name }
}
"#,
        &db,
        &ParamMap::new(),
    )
    .await;
    assert_eq!(extract_string_column(&people, "name").len(), 3);
    let knows_count = extract_u64_column(
        &run_db_query_test_with_params(
            r#"
query q() {
    match {
        $p: Person
        $p knows $f
    }
    return { count($f) as edge_count }
}
"#,
            &db,
            &ParamMap::new(),
        )
        .await,
        "edge_count",
    );
    assert_eq!(knows_count, vec![1]);
    let works_at_count = extract_u64_column(
        &run_db_query_test_with_params(
            r#"
query q() {
    match {
        $p: Person
        $p worksAt $c
    }
    return { count($c) as edge_count }
}
"#,
            &db,
            &ParamMap::new(),
        )
        .await,
        "edge_count",
    );
    assert_eq!(works_at_count, vec![1]);
}

#[tokio::test]
async fn test_type_error_unknown_type() {
    let storage = setup_storage().await;
    let catalog = storage.catalog();
    let qf = parse_query(
        r#"
query q() {
    match { $p: Foo }
    return { $p.name }
}
"#,
    )
    .unwrap();
    let err = typecheck_query(catalog, &qf.queries[0]).unwrap_err();
    assert!(err.to_string().contains("T1"));
}

#[tokio::test]
async fn test_type_error_bad_endpoints() {
    let storage = setup_storage().await;
    let catalog = storage.catalog();
    let qf = parse_query(
        r#"
query q() {
    match {
        $c: Company
        $c knows $f
    }
    return { $c.name }
}
"#,
    )
    .unwrap();
    let err = typecheck_query(catalog, &qf.queries[0]).unwrap_err();
    assert!(err.to_string().contains("T5"));
}

#[tokio::test]
async fn test_schema_typecheck_all_valid() {
    // Test that all valid queries from test.gq parse and typecheck
    let schema = parse_schema(test_schema()).unwrap();
    let catalog = build_catalog(&schema).unwrap();

    let query_src = std::fs::read_to_string("tests/fixtures/test.gq").unwrap();
    let qf = parse_query(&query_src).unwrap();

    for q in &qf.queries {
        let result = typecheck_query(&catalog, q);
        assert!(
            result.is_ok(),
            "query {} failed typecheck: {:?}",
            q.name,
            result.err()
        );
    }
}
