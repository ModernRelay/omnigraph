//! Shared fanout fixture for issue 703's memory admission and scale regressions.

use arrow_array::{Int64Array, StringArray};
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::error::OmniError;
use omnigraph::instrumentation::{
    QueryMemoryProbes, with_query_memory_limit, with_query_memory_probes,
};
use omnigraph::loader::LoadMode;

use super::{Session, params, query_main, session, with_setting};

const SCHEMA: &str = r#"
node Segment {
    slug: String @key
}
node Artifact {
    slug: String @key
    content: String
    embedding: Vector(4)
}
edge SegmentOf: Segment -> Artifact
"#;

const QUERIES: &str = r#"
query segment_counts() {
    match { $s: Segment $s segmentOf $t }
    return { $t.slug, count($s) as segments }
    order { segments desc }
    limit 10
}
query requested_payload() {
    match { $s: Segment $s segmentOf $t }
    return { $t.content }
}
"#;

pub async fn fixture(dir: &tempfile::TempDir, sources: usize, payload_bytes: usize) -> Session {
    let db = session(
        Omnigraph::init(dir.path().to_str().unwrap(), SCHEMA)
            .await
            .unwrap(),
    );
    let destination = serde_json::json!({
        "type": "Artifact",
        "data": {"slug": "transcript", "content": "x".repeat(payload_bytes),
            "embedding": [1.0, 2.0, 3.0, 4.0]}
    });
    db.load_jsonl(&destination.to_string(), LoadMode::Overwrite)
        .await
        .unwrap();
    for start in (0..sources).step_by(4_096) {
        let mut lines = Vec::new();
        for index in start..(start + 4_096).min(sources) {
            let slug = format!("segment-{index:06}");
            lines.push(serde_json::json!({"type": "Segment", "data": {"slug": slug}}).to_string());
            lines.push(
                serde_json::json!({"edge": "SegmentOf", "from": slug, "to": "transcript"})
                    .to_string(),
            );
        }
        db.load_jsonl(&lines.join("\n"), LoadMode::Append)
            .await
            .unwrap();
    }
    db.ensure_indices().await.unwrap();
    with_setting(&db, "engine", "v2")
}

fn assert_released(probes: &QueryMemoryProbes) {
    assert!(probes.pools_created() > 0);
    assert_eq!(probes.reserved_bytes(), 0);
    assert_eq!(probes.active_blocking_work(), 0);
}

pub async fn assert_memory_contract(db: &Session, sources: usize, limit: u64) {
    let explain = db
        .explain_query(
            ReadTarget::branch("main"),
            QUERIES,
            "segment_counts",
            &params(&[]),
        )
        .await
        .unwrap();
    let mut pending = vec![&explain["physical_plan"]];
    let mut destination_expand = false;
    while let Some(node) = pending.pop() {
        if node["node"] == "Expand" && node["dst_type"] == "Artifact" {
            destination_expand = true;
        }
        pending.extend(node["inputs"].as_array().into_iter().flatten());
    }
    assert!(
        destination_expand,
        "wide destination must be expanded: {explain}"
    );

    let probes = QueryMemoryProbes::default();
    let result = with_query_memory_probes(
        probes.clone(),
        with_query_memory_limit(
            limit,
            query_main(db, QUERIES, "segment_counts", &params(&[])),
        ),
    )
    .await
    .unwrap();
    assert_eq!(result.num_rows(), 1);
    let batch = result.concat_batches().unwrap();
    assert_eq!(
        batch
            .column_by_name("t.slug")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "transcript"
    );
    assert_eq!(
        batch
            .column_by_name("segments")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        i64::try_from(sources).unwrap()
    );
    assert!(
        probes
            .execution_metrics()
            .iter()
            .any(|metric| metric.operator == "ExpandExec" && metric.output_rows == sources)
    );
    assert_released(&probes);

    let probes = QueryMemoryProbes::default();
    let error = with_query_memory_probes(
        probes.clone(),
        with_query_memory_limit(
            limit,
            query_main(db, QUERIES, "requested_payload", &params(&[])),
        ),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(error, OmniError::ResourceLimitExceeded {
            ref resource, limit: actual, ..
        } if resource == "query_memory_bytes" && actual == limit),
        "requested payload must respect the query pool: {error:?}"
    );
    assert!(
        probes
            .refusals()
            .iter()
            .any(|name| name == "graph take output" || name == "hash join output"),
        "refusal must happen within one aligned or joined output batch: {:?}",
        probes.refusals()
    );
    assert_released(&probes);
}
