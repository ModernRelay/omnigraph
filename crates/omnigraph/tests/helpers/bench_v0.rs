//! Narrow v0 node-versus-edge logical-cost comparator.
//!
//! This module is path-imported only by `write_cost.rs` and
//! `write_cost_s3.rs`; it is intentionally not part of the shared `helpers`
//! module compiled into every engine integration-test binary.

use std::future::Future;
use std::io::{Read as _, Write as _};
use std::panic::{AssertUnwindSafe, resume_unwind};

use futures::FutureExt as _;
use serde::Serialize;
use sha2::{Digest as _, Sha256};

use omnigraph::db::Omnigraph;
use omnigraph_compiler::result::MutationResult;

use crate::helpers::cost::{IoCounts, measure};
use crate::helpers::{
    MUTATION_QUERIES, TEST_DATA, TEST_QUERIES, TEST_SCHEMA, count_rows, first_column_sorted,
    mixed_params, params, query_main,
};

pub const COST_BENCH_RESULTS_ENV: &str = "OMNIGRAPH_COST_BENCH_RESULTS";
const RECORD_SCHEMA_VERSION: u32 = 1;

/// Per-arm regression ceiling. Improvements pass; every raw counter and each
/// object-store total is bounded independently.
#[derive(Debug, Clone, Copy)]
pub struct LogicalInsertCostCeilingV1 {
    pub max_total_ops: u64,
    pub max_total_reads: u64,
    pub max_total_writes: u64,
    pub max_counts: IoCounts,
}

impl LogicalInsertCostCeilingV1 {
    fn assert_allows(self, operation: &str, observed: IoCounts) {
        assert_eq!(
            self.max_counts.total_ops(),
            self.max_total_ops,
            "{operation} ceiling total_ops disagrees with its raw maxima"
        );
        assert_eq!(
            self.max_counts.total_reads(),
            self.max_total_reads,
            "{operation} ceiling total_reads disagrees with its raw maxima"
        );
        assert_eq!(
            self.max_counts.total_writes(),
            self.max_total_writes,
            "{operation} ceiling total_writes disagrees with its raw maxima"
        );
        let checks = [
            ("total_ops", observed.total_ops(), self.max_total_ops),
            ("total_reads", observed.total_reads(), self.max_total_reads),
            (
                "total_writes",
                observed.total_writes(),
                self.max_total_writes,
            ),
            (
                "data_reads",
                observed.data_reads,
                self.max_counts.data_reads,
            ),
            (
                "data_writes",
                observed.data_writes,
                self.max_counts.data_writes,
            ),
            (
                "data_opener_reads",
                observed.data_opener_reads,
                self.max_counts.data_opener_reads,
            ),
            (
                "data_scan_reads",
                observed.data_scan_reads,
                self.max_counts.data_scan_reads,
            ),
            (
                "manifest_reads",
                observed.manifest_reads,
                self.max_counts.manifest_reads,
            ),
            (
                "manifest_writes",
                observed.manifest_writes,
                self.max_counts.manifest_writes,
            ),
            (
                "version_probes",
                observed.version_probes,
                self.max_counts.version_probes,
            ),
            (
                "data_open_count",
                observed.data_open_count,
                self.max_counts.data_open_count,
            ),
            (
                "internal_open_count",
                observed.internal_open_count,
                self.max_counts.internal_open_count,
            ),
            (
                "manifest_scan_count",
                observed.manifest_scan_count,
                self.max_counts.manifest_scan_count,
            ),
        ];
        for (metric, value, maximum) in checks {
            assert!(
                value <= maximum,
                "{operation} {metric} regressed: observed {value}, ceiling {maximum}; \
                 full counts: {observed:?}"
            );
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct LogicalInsertCostCeilingsV1 {
    pub node_insert: LogicalInsertCostCeilingV1,
    pub edge_insert: LogicalInsertCostCeilingV1,
}

#[derive(Debug, Clone, Copy)]
pub enum LogicalInsertPairOrder {
    NodeThenEdge,
    EdgeThenNode,
}

impl LogicalInsertPairOrder {
    fn label(self) -> &'static str {
        match self {
            Self::NodeThenEdge => "node-then-edge",
            Self::EdgeThenNode => "edge-then-node",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LogicalInsertCorrectnessV1 {
    affected_nodes: usize,
    affected_edges: usize,
    person_rows: usize,
    knows_rows: usize,
    query_values: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ObjectStoreOpsV1 {
    pub total: u64,
    pub reads: ObjectStoreReadsV1,
    pub writes: ObjectStoreWritesV1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ObjectStoreReadsV1 {
    pub total: u64,
    pub data_tables: u64,
    pub manifest: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ObjectStoreWritesV1 {
    pub total: u64,
    pub data_tables: u64,
    pub manifest: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ReadAttributionV1 {
    pub data_opener: u64,
    pub data_scan: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct EngineDiagnosticsV1 {
    pub version_probes: u64,
    pub data_table_opens: u64,
    pub internal_table_opens: u64,
    pub manifest_scans: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct LogicalInsertInstrumentationV1 {
    pub object_store_ops: ObjectStoreOpsV1,
    pub read_attribution: ReadAttributionV1,
    pub engine_diagnostics: EngineDiagnosticsV1,
}

impl From<IoCounts> for LogicalInsertInstrumentationV1 {
    fn from(counts: IoCounts) -> Self {
        Self {
            object_store_ops: ObjectStoreOpsV1 {
                total: counts.total_ops(),
                reads: ObjectStoreReadsV1 {
                    total: counts.total_reads(),
                    data_tables: counts.data_reads,
                    manifest: counts.manifest_reads,
                },
                writes: ObjectStoreWritesV1 {
                    total: counts.total_writes(),
                    data_tables: counts.data_writes,
                    manifest: counts.manifest_writes,
                },
            },
            read_attribution: ReadAttributionV1 {
                data_opener: counts.data_opener_reads,
                data_scan: counts.data_scan_reads,
            },
            engine_diagnostics: EngineDiagnosticsV1 {
                version_probes: counts.version_probes,
                data_table_opens: counts.data_open_count,
                internal_table_opens: counts.internal_open_count,
                manifest_scans: counts.manifest_scan_count,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct LogicalInsertCostSampleV1 {
    pub sample_key: String,
    pub pair_key: String,
    pub pair_order: String,
    pub position: u8,
    pub operation: String,
    pub instrumentation: LogicalInsertInstrumentationV1,
    pub correctness: LogicalInsertCorrectnessV1,
    #[serde(skip)]
    counts: IoCounts,
}

#[derive(Debug, Clone, Copy, Serialize)]
struct LogicalInsertCostCeilingRecordV1 {
    max_total_ops: u64,
    max_total_reads: u64,
    max_total_writes: u64,
    max_instrumentation: LogicalInsertInstrumentationV1,
}

impl From<LogicalInsertCostCeilingV1> for LogicalInsertCostCeilingRecordV1 {
    fn from(ceiling: LogicalInsertCostCeilingV1) -> Self {
        Self {
            max_total_ops: ceiling.max_total_ops,
            max_total_reads: ceiling.max_total_reads,
            max_total_writes: ceiling.max_total_writes,
            max_instrumentation: ceiling.max_counts.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
struct LogicalInsertCostCeilingsRecordV1 {
    node_insert: LogicalInsertCostCeilingRecordV1,
    edge_insert: LogicalInsertCostCeilingRecordV1,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct LogicalInsertCostSummaryV1 {
    pub node_insert: LogicalInsertInstrumentationV1,
    pub edge_insert: LogicalInsertInstrumentationV1,
    pub edge_to_node_total_ops_ratio: f64,
}

#[derive(Debug, Clone, Serialize)]
struct LogicalInsertCostBuildV1 {
    omnigraph_version: String,
    git_commit: Option<String>,
    git_tree_sha: Option<String>,
    git_worktree_dirty: Option<bool>,
    benchmark_binary_sha256: String,
    profile: String,
    failpoints_enabled: bool,
}

#[derive(Debug, Clone, Serialize)]
struct LogicalInsertCostHostV1 {
    os: String,
    arch: String,
    available_parallelism: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
struct LogicalInsertCostFixtureV1 {
    id: String,
    schema_sha256: String,
    data_sha256: String,
    backend: String,
    endpoint_origin: Option<String>,
    branch: String,
    fresh_fixture_per_sample: bool,
}

#[derive(Debug, Clone, Serialize)]
struct LogicalInsertCostBoundaryV1 {
    included: String,
    excluded: Vec<String>,
    record_semantics: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct LogicalInsertCostRecordV1 {
    record_schema_version: u32,
    harness_version: String,
    instrument_kind: String,
    benchmark: String,
    run_id: String,
    timestamp_unix_seconds: u64,
    build: LogicalInsertCostBuildV1,
    host: LogicalInsertCostHostV1,
    fixture: LogicalInsertCostFixtureV1,
    boundary: LogicalInsertCostBoundaryV1,
    samples: Vec<LogicalInsertCostSampleV1>,
    ceilings: LogicalInsertCostCeilingsRecordV1,
    pub summary: LogicalInsertCostSummaryV1,
    outcome: String,
}

async fn measure_node_insert(db: &mut Omnigraph, tag: &str) -> (MutationResult, IoCounts) {
    let (result, counts) = measure(db.mutate(
        "main",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", tag)], &[("$age", 30)]),
    ))
    .await;
    (result.unwrap(), counts)
}

async fn measure_edge_insert(db: &mut Omnigraph) -> (MutationResult, IoCounts) {
    let (result, counts) = measure(db.mutate(
        "main",
        MUTATION_QUERIES,
        "add_friend",
        &mixed_params(&[("$from", "Charlie"), ("$to", "Diana")], &[]),
    ))
    .await;
    (result.unwrap(), counts)
}

async fn verify_node_insert(
    db: &mut Omnigraph,
    tag: &str,
    result: MutationResult,
) -> LogicalInsertCorrectnessV1 {
    assert_eq!(result.affected_nodes, 1, "node arm must insert one node");
    assert_eq!(result.affected_edges, 0, "node arm must insert no edges");
    let person_rows = count_rows(db, "node:Person").await;
    let knows_rows = count_rows(db, "edge:Knows").await;
    let query_values = first_column_sorted(
        &query_main(db, TEST_QUERIES, "get_person", &params(&[("$name", tag)]))
            .await
            .unwrap(),
    );
    assert_eq!(person_rows, 5, "node arm must publish exactly one Person");
    assert_eq!(knows_rows, 3, "node arm must not change Knows");
    assert_eq!(query_values, vec![tag.to_string()]);
    LogicalInsertCorrectnessV1 {
        affected_nodes: result.affected_nodes,
        affected_edges: result.affected_edges,
        person_rows,
        knows_rows,
        query_values,
    }
}

async fn verify_edge_insert(
    db: &mut Omnigraph,
    result: MutationResult,
) -> LogicalInsertCorrectnessV1 {
    assert_eq!(result.affected_nodes, 0, "edge arm must insert no nodes");
    assert_eq!(result.affected_edges, 1, "edge arm must insert one edge");
    let person_rows = count_rows(db, "node:Person").await;
    let knows_rows = count_rows(db, "edge:Knows").await;
    let query_values = first_column_sorted(
        &query_main(
            db,
            TEST_QUERIES,
            "friends_of",
            &params(&[("$name", "Charlie")]),
        )
        .await
        .unwrap(),
    );
    assert_eq!(person_rows, 4, "edge arm must not change Person");
    assert_eq!(knows_rows, 4, "edge arm must publish exactly one Knows");
    assert_eq!(query_values, vec!["Diana"]);
    LogicalInsertCorrectnessV1 {
        affected_nodes: result.affected_nodes,
        affected_edges: result.affected_edges,
        person_rows,
        knows_rows,
        query_values,
    }
}

/// Run one matched pair. Both measurements finish before either post-state
/// check, so verification cannot warm the other arm.
pub async fn measure_logical_insert_pair(
    node_db: &mut Omnigraph,
    edge_db: &mut Omnigraph,
    pair_key: &str,
    order: LogicalInsertPairOrder,
) -> Vec<LogicalInsertCostSampleV1> {
    let node_tag = format!("cost-v0-{pair_key}");
    let ((node_result, node_counts), (edge_result, edge_counts)) = match order {
        LogicalInsertPairOrder::NodeThenEdge => {
            let node = measure_node_insert(node_db, &node_tag).await;
            let edge = measure_edge_insert(edge_db).await;
            (node, edge)
        }
        LogicalInsertPairOrder::EdgeThenNode => {
            let edge = measure_edge_insert(edge_db).await;
            let node = measure_node_insert(node_db, &node_tag).await;
            (node, edge)
        }
    };
    let node_correctness = verify_node_insert(node_db, &node_tag, node_result).await;
    let edge_correctness = verify_edge_insert(edge_db, edge_result).await;
    let node_sample = LogicalInsertCostSampleV1 {
        sample_key: format!("{pair_key}/node"),
        pair_key: pair_key.to_string(),
        pair_order: order.label().to_string(),
        position: match order {
            LogicalInsertPairOrder::NodeThenEdge => 1,
            LogicalInsertPairOrder::EdgeThenNode => 2,
        },
        operation: "node_insert".to_string(),
        instrumentation: node_counts.into(),
        correctness: node_correctness,
        counts: node_counts,
    };
    let edge_sample = LogicalInsertCostSampleV1 {
        sample_key: format!("{pair_key}/edge"),
        pair_key: pair_key.to_string(),
        pair_order: order.label().to_string(),
        position: match order {
            LogicalInsertPairOrder::NodeThenEdge => 2,
            LogicalInsertPairOrder::EdgeThenNode => 1,
        },
        operation: "edge_insert".to_string(),
        instrumentation: edge_counts.into(),
        correctness: edge_correctness,
        counts: edge_counts,
    };
    match order {
        LogicalInsertPairOrder::NodeThenEdge => vec![node_sample, edge_sample],
        LogicalInsertPairOrder::EdgeThenNode => vec![edge_sample, node_sample],
    }
}

fn sha256_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn git_value(args: &[&str]) -> Option<String> {
    let output = std::process::Command::new("git")
        .args(args)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn git_worktree_dirty() -> Option<bool> {
    let output = std::process::Command::new("git")
        .args(["status", "--porcelain=v1", "--untracked-files=normal"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .ok()?;
    output.status.success().then_some(!output.stdout.is_empty())
}

fn current_binary_sha256() -> String {
    let path = std::env::current_exe().expect("cost benchmark current executable");
    let mut file = std::fs::File::open(&path).unwrap_or_else(|error| {
        panic!(
            "open cost benchmark executable {} for digest: {error}",
            path.display()
        )
    });
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer).unwrap_or_else(|error| {
            panic!(
                "read cost benchmark executable {} for digest: {error}",
                path.display()
            )
        });
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    format!("{:x}", digest.finalize())
}

/// Return only the endpoint origin. Userinfo, paths, queries, and fragments
/// must never enter a benchmark record.
fn sanitized_endpoint_origin(raw: &str) -> Option<String> {
    let parsed = url::Url::parse(raw.trim()).ok()?;
    let host = match parsed.host()? {
        url::Host::Domain(domain) => domain.to_string(),
        url::Host::Ipv4(address) => address.to_string(),
        url::Host::Ipv6(address) => format!("[{address}]"),
    };
    let port = parsed
        .port()
        .map(|port| format!(":{port}"))
        .unwrap_or_default();
    Some(format!("{}://{host}{port}", parsed.scheme()))
}

fn endpoint_origin() -> Option<String> {
    std::env::var("AWS_ENDPOINT_URL_S3")
        .ok()
        .and_then(|raw| sanitized_endpoint_origin(&raw))
        .or_else(|| {
            std::env::var("AWS_ENDPOINT_URL")
                .ok()
                .and_then(|raw| sanitized_endpoint_origin(&raw))
        })
}

/// Validate AB/BA repeatability, enforce the selected backend's ceilings, and
/// construct one success-only invocation record.
pub fn finalize_logical_insert_record(
    backend: &str,
    samples: Vec<LogicalInsertCostSampleV1>,
    ceilings: LogicalInsertCostCeilingsV1,
) -> LogicalInsertCostRecordV1 {
    let observed_shape: Vec<_> = samples
        .iter()
        .map(|sample| {
            (
                sample.sample_key.as_str(),
                sample.pair_key.as_str(),
                sample.pair_order.as_str(),
                sample.position,
                sample.operation.as_str(),
            )
        })
        .collect();
    assert_eq!(
        observed_shape,
        [
            ("ab/node", "ab", "node-then-edge", 1, "node_insert"),
            ("ab/edge", "ab", "node-then-edge", 2, "edge_insert"),
            ("ba/edge", "ba", "edge-then-node", 1, "edge_insert"),
            ("ba/node", "ba", "edge-then-node", 2, "node_insert"),
        ],
        "v0 samples must carry one exact AB/BA pair shape"
    );
    for sample in &samples {
        assert_eq!(
            sample.instrumentation,
            sample.counts.into(),
            "{} serialized instrumentation diverged from asserted counters",
            sample.sample_key
        );
    }
    let node_samples: Vec<_> = samples
        .iter()
        .filter(|sample| sample.operation == "node_insert")
        .collect();
    let edge_samples: Vec<_> = samples
        .iter()
        .filter(|sample| sample.operation == "edge_insert")
        .collect();
    assert_eq!(node_samples.len(), 2);
    assert_eq!(edge_samples.len(), 2);
    assert_eq!(
        node_samples[0].counts, node_samples[1].counts,
        "node-insert logical IO changed when pair order was reversed"
    );
    assert_eq!(
        edge_samples[0].counts, edge_samples[1].counts,
        "edge-insert logical IO changed when pair order was reversed"
    );
    let node_counts = node_samples[0].counts;
    let edge_counts = edge_samples[0].counts;
    for (operation, counts) in [("node_insert", node_counts), ("edge_insert", edge_counts)] {
        assert!(
            counts.data_reads > 0
                && counts.data_writes > 0
                && counts.manifest_reads > 0
                && counts.manifest_writes > 0,
            "{operation} produced vacuous logical IO evidence; both data and manifest wrappers \
             must observe reads and writes: {counts:?}"
        );
    }
    ceilings
        .node_insert
        .assert_allows("node_insert", node_counts);
    ceilings
        .edge_insert
        .assert_allows("edge_insert", edge_counts);

    // Capture identity before the output file exists so a workspace-local
    // result path cannot itself change this dirty stamp.
    let build = LogicalInsertCostBuildV1 {
        omnigraph_version: env!("CARGO_PKG_VERSION").to_string(),
        git_commit: git_value(&["rev-parse", "HEAD"]),
        git_tree_sha: git_value(&["rev-parse", "HEAD^{tree}"]),
        git_worktree_dirty: git_worktree_dirty(),
        benchmark_binary_sha256: current_binary_sha256(),
        profile: if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        }
        .to_string(),
        failpoints_enabled: cfg!(feature = "failpoints"),
    };
    let timestamp_unix_seconds = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_secs();
    LogicalInsertCostRecordV1 {
        record_schema_version: RECORD_SCHEMA_VERSION,
        harness_version: "v0".to_string(),
        instrument_kind: "logical_lance_object_store_ops".to_string(),
        benchmark: "node-vs-edge-insert".to_string(),
        run_id: ulid::Ulid::new().to_string(),
        timestamp_unix_seconds,
        build,
        host: LogicalInsertCostHostV1 {
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
            available_parallelism: std::thread::available_parallelism()
                .map(std::num::NonZero::get)
                .ok(),
        },
        fixture: LogicalInsertCostFixtureV1 {
            id: "standard-indexed-test-fixture-v1".to_string(),
            schema_sha256: sha256_bytes(TEST_SCHEMA.as_bytes()),
            data_sha256: sha256_bytes(TEST_DATA.as_bytes()),
            backend: backend.to_string(),
            endpoint_origin: (backend != "local_file").then(endpoint_origin).flatten(),
            branch: "main".to_string(),
            fresh_fixture_per_sample: true,
        },
        boundary: LogicalInsertCostBoundaryV1 {
            included:
                "logical calls observed by Lance data-table and __manifest object-store wrappers"
                    .to_string(),
            excluded: vec![
                "non-Lance schema and recovery control-object IO".to_string(),
                "SDK retries and physical HTTP attempts".to_string(),
                "multipart part attempts".to_string(),
                "bytes, billing, wall time, and RSS".to_string(),
                "typed failure evidence".to_string(),
            ],
            record_semantics:
                "v0 emits a record only after correctness, repeatability, and ceilings pass"
                    .to_string(),
        },
        samples,
        ceilings: LogicalInsertCostCeilingsRecordV1 {
            node_insert: ceilings.node_insert.into(),
            edge_insert: ceilings.edge_insert.into(),
        },
        summary: LogicalInsertCostSummaryV1 {
            node_insert: node_counts.into(),
            edge_insert: edge_counts.into(),
            edge_to_node_total_ops_ratio: edge_counts.total_ops() as f64
                / node_counts.total_ops() as f64,
        },
        outcome: "passed".to_string(),
    }
}

/// Print one JSON record and append it to the configured JSONL file. If the
/// output environment variable is present, blank or unwritable paths fail.
pub fn emit_logical_insert_record(record: &LogicalInsertCostRecordV1) -> String {
    let line = serde_json::to_string(record).expect("serialize logical insert cost record");
    let parsed: serde_json::Value =
        serde_json::from_str(&line).expect("logical insert cost record must round-trip as JSON");
    assert_eq!(parsed["record_schema_version"], RECORD_SCHEMA_VERSION);
    for pointer in [
        "/harness_version",
        "/instrument_kind",
        "/benchmark",
        "/run_id",
        "/timestamp_unix_seconds",
        "/build/omnigraph_version",
        "/build/git_commit",
        "/build/git_tree_sha",
        "/build/git_worktree_dirty",
        "/build/benchmark_binary_sha256",
        "/build/profile",
        "/build/failpoints_enabled",
        "/host/os",
        "/host/arch",
        "/host/available_parallelism",
        "/fixture/id",
        "/fixture/schema_sha256",
        "/fixture/data_sha256",
        "/fixture/backend",
        "/fixture/endpoint_origin",
        "/fixture/branch",
        "/fixture/fresh_fixture_per_sample",
        "/boundary/included",
        "/boundary/excluded",
        "/boundary/record_semantics",
        "/ceilings/node_insert/max_total_ops",
        "/ceilings/node_insert/max_total_reads",
        "/ceilings/node_insert/max_total_writes",
        "/ceilings/node_insert/max_instrumentation",
        "/ceilings/edge_insert/max_total_ops",
        "/ceilings/edge_insert/max_total_reads",
        "/ceilings/edge_insert/max_total_writes",
        "/ceilings/edge_insert/max_instrumentation",
        "/summary/node_insert",
        "/summary/edge_insert",
        "/summary/edge_to_node_total_ops_ratio",
        "/outcome",
    ] {
        assert!(
            parsed.pointer(pointer).is_some(),
            "schema-v1 record omitted required field {pointer}"
        );
    }
    let samples = parsed["samples"]
        .as_array()
        .expect("schema-v1 samples must be an array");
    assert_eq!(samples.len(), 4);
    for sample in samples {
        for pointer in [
            "/sample_key",
            "/pair_key",
            "/pair_order",
            "/position",
            "/operation",
            "/instrumentation/object_store_ops/total",
            "/instrumentation/object_store_ops/reads/total",
            "/instrumentation/object_store_ops/reads/data_tables",
            "/instrumentation/object_store_ops/reads/manifest",
            "/instrumentation/object_store_ops/writes/total",
            "/instrumentation/object_store_ops/writes/data_tables",
            "/instrumentation/object_store_ops/writes/manifest",
            "/instrumentation/read_attribution/data_opener",
            "/instrumentation/read_attribution/data_scan",
            "/instrumentation/engine_diagnostics/version_probes",
            "/instrumentation/engine_diagnostics/data_table_opens",
            "/instrumentation/engine_diagnostics/internal_table_opens",
            "/instrumentation/engine_diagnostics/manifest_scans",
            "/correctness/affected_nodes",
            "/correctness/affected_edges",
            "/correctness/person_rows",
            "/correctness/knows_rows",
            "/correctness/query_values",
        ] {
            assert!(
                sample.pointer(pointer).is_some(),
                "schema-v1 sample omitted required field {pointer}"
            );
        }
    }
    assert_eq!(
        parsed["summary"]["node_insert"],
        samples[0]["instrumentation"]
    );
    assert_eq!(
        parsed["summary"]["edge_insert"],
        samples[1]["instrumentation"]
    );
    println!("{line}");

    match std::env::var(COST_BENCH_RESULTS_ENV) {
        Ok(configured) => {
            assert!(
                !configured.trim().is_empty(),
                "{COST_BENCH_RESULTS_ENV} is set but blank"
            );
            let path = std::path::PathBuf::from(configured.trim());
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).unwrap_or_else(|error| {
                    panic!(
                        "create cost benchmark result directory {}: {error}",
                        parent.display()
                    )
                });
            }
            let mut output = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .unwrap_or_else(|error| {
                    panic!(
                        "open cost benchmark JSONL {} for append: {error}",
                        path.display()
                    )
                });
            writeln!(output, "{line}").unwrap_or_else(|error| {
                panic!("append cost benchmark JSONL {}: {error}", path.display())
            });
        }
        Err(std::env::VarError::NotPresent) => {}
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("{COST_BENCH_RESULTS_ENV} is not valid Unicode")
        }
    }
    line
}

/// Run an async cost-test body on the established large-stack test thread.
pub fn on_big_stack<F>(body: impl FnOnce() -> F + Send + 'static)
where
    F: Future<Output = ()> + 'static,
{
    std::thread::Builder::new()
        .stack_size(64 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build cost benchmark runtime")
                .block_on(body());
        })
        .expect("spawn cost benchmark thread")
        .join()
        .expect("cost benchmark thread panicked");
}

/// Run cleanup after success or panic, preserving the original panic when both
/// the guarded operation and exact-prefix cleanup fail.
#[allow(dead_code)] // Used by the S3 importer; the local importer compiles this module separately.
pub async fn run_with_cleanup<RunFuture, CleanupFuture>(
    run: RunFuture,
    cleanup: CleanupFuture,
    cleanup_context: &str,
) where
    RunFuture: Future<Output = ()>,
    CleanupFuture: Future<Output = Result<(), String>>,
{
    let run_result = AssertUnwindSafe(run).catch_unwind().await;
    let cleanup_result = cleanup.await;
    match run_result {
        Ok(()) => cleanup_result.unwrap_or_else(|error| panic!("{cleanup_context}: {error}")),
        Err(payload) => {
            if let Err(error) = cleanup_result {
                eprintln!("{cleanup_context} after benchmark panic also failed: {error}");
            }
            resume_unwind(payload);
        }
    }
}
