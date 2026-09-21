//! Shared bound-edge aggregation fixture for issue 723's streaming regressions.

use std::collections::HashSet;

use arrow_array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use omnigraph::db::Omnigraph;
use omnigraph::instrumentation::{
    QueryMemoryProbes, with_query_memory_limit, with_query_memory_probes,
};
use omnigraph::loader::LoadMode;

use super::{Session, params, query_main, session, with_setting};

const SCHEMA: &str = r#"
node Account {
    accountId: String @key
}
edge AccountTransferAccount: Account -> Account {
    amount: I64?
}
"#;

const QUERY: &str = r#"
query top_senders() {
    match {
        $src: Account
        $src $t:accountTransferAccount $dst
    }
    return { $src.accountId, count($t.amount) as transfers, sum($t.amount) as sent }
    order { sent desc, $src.accountId }
    limit 20
}

query first_transfers() {
    match {
        $src: Account
        $src $t:accountTransferAccount $dst
    }
    return { $src.accountId, $dst.accountId, $t.@id as edge_id, $t.amount }
    order { $src.accountId, $dst.accountId, $t.@id }
    limit 1025
}
"#;

const EDGE_COUNTS: [usize; 4] = [65_536, 16_384, 8_192, 4_096];

fn account(index: usize) -> String {
    format!("account-{index}-{}", "x".repeat(54))
}

pub async fn fixture(dir: &tempfile::TempDir, scale: usize) -> Session {
    assert!(scale > 0);
    let db = session(
        Omnigraph::init(dir.path().to_str().unwrap(), SCHEMA)
            .await
            .unwrap(),
    );
    let nodes = (0..=EDGE_COUNTS.len())
        .map(|index| {
            serde_json::json!({"type":"Account", "data":{"accountId":account(index)}}).to_string()
        })
        .collect::<Vec<_>>();
    db.load_jsonl(&nodes.join("\n"), LoadMode::Overwrite)
        .await
        .unwrap();
    let destination = account(EDGE_COUNTS.len());
    for (sender, edges) in EDGE_COUNTS.into_iter().enumerate() {
        let source = account(sender);
        let edges = edges.checked_mul(scale).unwrap();
        for start in (0..edges).step_by(4_096) {
            let mut lines = Vec::new();
            for edge in start..(start + 4_096).min(edges) {
                let data = if edge % 4 == 0 {
                    serde_json::json!({})
                } else {
                    serde_json::json!({"amount":2})
                };
                lines.push(
                    serde_json::json!({"edge":"AccountTransferAccount", "from":source,
                        "to":destination, "data":data})
                    .to_string(),
                );
            }
            db.load_jsonl(&lines.join("\n"), LoadMode::Append)
                .await
                .unwrap();
        }
    }
    db
}

fn rows(batches: &[RecordBatch]) -> Vec<(String, i64, u64)> {
    let mut result = Vec::new();
    for batch in batches {
        let accounts = batch
            .column_by_name("src.accountId")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let counts = batch
            .column_by_name("transfers")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let sums = batch
            .column_by_name("sent")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert!(!accounts.is_null(row) && !counts.is_null(row) && !sums.is_null(row));
            result.push((
                accounts.value(row).to_string(),
                counts.value(row),
                sums.value(row).to_bits(),
            ));
        }
    }
    result
}

pub async fn assert_streaming_contract(db: &Session, scale: usize, limit: u64) {
    let expected: Vec<_> = EDGE_COUNTS
        .into_iter()
        .enumerate()
        .map(|(sender, edges)| {
            let count = edges.checked_mul(scale).unwrap() / 4 * 3;
            let count = i64::try_from(count).unwrap();
            (account(sender), count, (count as f64 * 2.0).to_bits())
        })
        .collect();
    let v1 = with_setting(db, "engine", "v1");
    let old = query_main(&v1, QUERY, "top_senders", &params(&[]))
        .await
        .unwrap();
    assert_eq!(rows(old.batches()), expected);
    drop(old);

    let v2 = with_setting(db, "engine", "v2");
    let probes = QueryMemoryProbes::default();
    let result = with_query_memory_probes(
        probes.clone(),
        with_query_memory_limit(limit, query_main(&v2, QUERY, "top_senders", &params(&[]))),
    )
    .await;
    assert!(probes.pools_created() > 0);
    let result = result.unwrap_or_else(|error| {
        panic!(
            "bound-edge aggregation must stream within {limit} bytes: {error}; refusals={:?}",
            probes.refusals()
        )
    });
    assert_eq!(probes.reserved_bytes(), 0);
    assert_eq!(probes.active_blocking_work(), 0);
    assert_eq!(rows(result.batches()), expected);
    let total_edges = EDGE_COUNTS
        .iter()
        .sum::<usize>()
        .checked_mul(scale)
        .unwrap();
    let metrics = probes.execution_metrics();
    let expand = metrics
        .iter()
        .find(|metric| metric.operator == "ExpandExec")
        .expect("the bound-edge expand must execute");
    assert_eq!(expand.output_rows, total_edges);
    assert!(
        expand.values.get("output_batches").copied().unwrap_or(0) > 1,
        "ExpandExec must feed multiple bounded batches into aggregation: {expand:?}"
    );
    assert!(
        metrics
            .iter()
            .any(|metric| metric.operator == "AggregateExec")
    );

    let ordered_v1 = query_main(&v1, QUERY, "first_transfers", &params(&[]))
        .await
        .unwrap()
        .concat_batches()
        .unwrap();
    assert_eq!(ordered_v1.num_rows(), 1_025);
    let edge_ids = ordered_v1
        .column_by_name("edge_id")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(edge_ids.null_count(), 0);
    assert_eq!(
        edge_ids.iter().collect::<HashSet<_>>().len(),
        1_025,
        "every parallel edge must carry a distinct identity into the order comparison"
    );
    let order_probes = QueryMemoryProbes::default();
    let ordered_v2 = with_query_memory_probes(
        order_probes.clone(),
        with_query_memory_limit(
            limit,
            query_main(&v2, QUERY, "first_transfers", &params(&[])),
        ),
    )
    .await
    .unwrap_or_else(|error| {
        panic!(
            "plain traversal failed: {error}; refusals={:?}",
            order_probes.refusals()
        )
    })
    .concat_batches()
    .unwrap();
    assert_eq!(
        ordered_v2, ordered_v1,
        "an explicitly ordered bound-edge LIMIT must preserve its total key across output batches"
    );
}
