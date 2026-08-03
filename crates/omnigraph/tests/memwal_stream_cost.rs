#![cfg(feature = "failpoints")]

//! RFC-026 private-stream cost evidence.
//!
//! Keep the terms separate.  A warm already-claimed acknowledgement, a cold
//! claim/replay, one selected flushed-generation scan, retained shard-manifest
//! metadata, and the shared graph-manifest publisher are different costs with
//! different scaling variables.  Combining them into one latency number would
//! hide exactly the regressions this instrument exists to expose.
//!
//! B2a's retained-history sweep keeps wall-clock numbers diagnostic.  The
//! real private B1 path grows retained shard metadata and the folded base table
//! together, so the sweep reports each observable term but does not attribute a
//! slope to retained objects alone or turn local timings into a product claim.
//!
//! F6b3 separately holds current-token cardinality fixed while immutable
//! control-ledger history grows, then measures the exact manifest-selected
//! uncovered index tail. F6b7 extends that owner with one failpoints-only,
//! content-identical reconciled cut so the production-reconciler decision is
//! based on paired evidence rather than an assumed index-maintenance benefit.

mod helpers;

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow_schema::Schema;
use fail::FailScenario;
use futures::TryStreamExt;
use lance_core::utils::bloomfilter::sbbf::Sbbf;
use lance_index::mem_wal::{FlushedGeneration, ShardId, ShardManifest, ShardStatus};
use lance_io::utils::tracking_store::{IOTracker, IoStats};
use omnigraph::db::{
    Omnigraph, ReadTarget, StreamDeadLetterEncodingCostForTest, StreamDeadLetterPage,
    failpoint_measure_stream_dead_letter_object_for_test,
};
use omnigraph::error::OmniError;
use omnigraph::failpoints::{ScopedFailPoint, names};
use omnigraph::instrumentation::{CountingStorageAdapter, StorageReadCounts};
use omnigraph::storage::storage_for_uri;
use serial_test::serial;

use helpers::cost::with_raw_io_trackers;
use helpers::memwal::{CurrentMemWalInventory, MemWalObjectKind};

const STREAM_SCHEMA: &str = r#"
node Person { score: I32 }
node History { tick: I32 }
"#;
const PAYLOAD_SCHEMA: &str = "node Person { payload: String }\n";
const TABLE: &str = "node:Person";
const ADD_HISTORY: &str = r#"
query add_history($tick: I32) {
    insert History { tick: $tick }
}
"#;
const TOKEN_COST_SCHEMA: &str = r#"
node Person {
    score: I32
    @unique(score)
}
"#;
const TOKEN_COST_SEED: &str = r#"
query seed_token_cost_conflict($score: I32) {
    insert Person { score: $score }
}
"#;
const TOKEN_COST_LOGICAL_ID_PREFIX: &str = "token-cost-current";
const TOKEN_COST_LOGICAL_ID: &str = "token-cost-current-00000";
const TOKEN_COST_MISSING_ID: &str = "token-cost-missing";
const TOKEN_COST_GRAPH_ID: &str = "knowledge";
const TOKEN_COST_MISSING_RECEIPT_OPERATION: &str = "memwal-token-cost-missing-receipt";
const TOKEN_COST_CONTRIBUTOR: &str = "agent:f6b7-token-cost";
const TOKEN_COST_SERIES_REPETITIONS: usize = 8;
const F6B7_DECISION_BACKEND_ENV: &str = "OMNIGRAPH_F6B7_DECISION_BACKEND";

const WIDEST_ROWS: usize = 8_192;
const HARD_ARROW_BYTES: usize = 32 * 1024 * 1024;
const WIDEST_PAYLOAD_BYTES: usize = 3_742;
const WIDEST_ONE_BYTE_OVER_PAYLOAD_BYTES: usize = WIDEST_PAYLOAD_BYTES + 1;
const WIDEST_ATTRIBUTED_ARROW_BYTES: usize = 33_550_336;
const WIDEST_ONE_BYTE_OVER_ATTRIBUTED_ARROW_BYTES: usize = 33_558_528;
const FOLD_RSS_DELTA_REMEASURE_BYTES: u64 = 384 * 1024 * 1024;
const NO_AUTO_ROLL_BYTES: usize = 1024 * 1024 * 1024;
const NO_AUTO_ROLL_ROWS: usize = 8_193;
const NO_AUTO_ROLL_BATCHES: usize = 8_193;

const RSS_CHILD_ENV: &str = "OMNIGRAPH_MEMWAL_COST_CHILD";
const DEAD_LETTER_RSS_CHILD_ENV: &str = "OMNIGRAPH_DEAD_LETTER_COST_CHILD";
const DEAD_LETTER_CANDIDATES: usize = 8_192;
const DEAD_LETTER_OBJECT_BYTES: u64 = 64 * 1024 * 1024;
const DEAD_LETTER_BASE_CONTROL_CHARS: usize = 1_250;
// These two fixture terms close the current production grammar from its
// 8,192-candidate base shape to exactly 64 MiB. A prefix/field change must
// deliberately recalibrate them instead of silently changing the evidence.
const DEAD_LETTER_EXACT_EXTRA_CONTROL_CHARS: usize = 124_430;
const DEAD_LETTER_EXACT_LITERAL_CHARS: usize = 2;
// Filled from the first isolated F6b4 run; this remains a remeasurement
// tripwire, not admission, a storage quota, or a product RSS SLO.
const DEAD_LETTER_RSS_DELTA_REMEASURE_BYTES: u64 = 192 * 1024 * 1024;
/// The exact Lance release Gate R0's source audit was performed against.
/// Through the 9.0.0-rc.1 era this was a git rev (`cec0b7df`); the 9.0.0
/// stable bump moved every Lance crate to the crates.io registry, so the
/// tripwire now pins the released version instead. Its purpose is unchanged:
/// the audit must not silently outlive the source it surveyed.
const SURVEYED_LANCE_VERSION: &str = "9.0.0";
/// Two members of the Lance repository family are versioned against Arrow
/// rather than Lance, so they carry their own surveyed version.
const SURVEYED_LANCE_ARROW_VERSION: &str = "58.0.0";
const SURVEYED_LANCE_ARROW_PACKAGES: [&str; 2] = ["lance-arrow-scalar", "lance-arrow-stats"];

fn classify_mem_wal_object(
    components: &[&str],
    mem_wal_index: usize,
) -> (MemWalObjectKind, Option<String>) {
    helpers::memwal::classify_mem_wal_object(components, mem_wal_index)
}

fn validated_referenced_generation_roots(
    table_base: &str,
    directory_shard_id: ShardId,
    manifest: &ShardManifest,
    listed_generation_roots: &BTreeSet<String>,
    listed_manifest_versions: &BTreeSet<u64>,
) -> BTreeSet<String> {
    helpers::memwal::validated_referenced_generation_roots(
        table_base,
        directory_shard_id,
        manifest,
        listed_generation_roots,
        listed_manifest_versions,
    )
}

async fn current_mem_wal_inventory(uri: &str) -> CurrentMemWalInventory {
    helpers::memwal::current_mem_wal_inventory(uri).await
}

async fn current_recovery_sidecars(uri: &str) -> Vec<String> {
    let (store, root_path) = lance_io::object_store::ObjectStore::from_uri(uri)
        .await
        .expect("Gate R0 fixture URI must resolve");
    let mut paths = store
        .inner
        .list(Some(&root_path))
        .try_collect::<Vec<_>>()
        .await
        .expect("Gate R0 recovery inventory must fail closed on a listing error")
        .into_iter()
        .map(|metadata| metadata.location.as_ref().to_string())
        .filter(|path| path.split('/').any(|component| component == "__recovery"))
        .collect::<Vec<_>>();
    paths.sort();
    paths
}

#[derive(Debug, Clone, Copy, Default)]
struct PathIo {
    total_reads: u64,
    total_read_bytes: u64,
    total_writes: u64,
    total_written_bytes: u64,
    list_requests: u64,
    memwal_reads: u64,
    memwal_writes: u64,
    base_table_reads: u64,
    base_table_writes: u64,
    stream_token_reads: u64,
    stream_token_writes: u64,
    dead_letter_object_reads: u64,
    dead_letter_object_writes: u64,
    other_reads: u64,
    other_writes: u64,
    wal_reads: u64,
    wal_writes: u64,
    shard_manifest_reads: u64,
    shard_manifest_writes: u64,
    generation_reads: u64,
    generation_writes: u64,
    generation_data_reads: u64,
    generation_data_writes: u64,
    pk_sidecar_reads: u64,
    pk_sidecar_writes: u64,
    memwal_atomic_temp_delete_requests: u64,
    memwal_canonical_delete_requests: u64,
}

fn is_lance_atomic_memwal_temp_path(path: &str) -> bool {
    fn canonical_lower_uuid(value: &str) -> bool {
        value.len() == 36
            && value.bytes().enumerate().all(|(index, byte)| match index {
                8 | 13 | 18 | 23 => byte == b'-',
                _ => byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte),
            })
    }

    let components = path.split('/').collect::<Vec<_>>();
    let Some(mem_wal) = components
        .iter()
        .position(|component| *component == "_mem_wal")
    else {
        return false;
    };
    let tail = &components[mem_wal + 1..];
    if tail.len() != 3 || tail[1] != "manifest" {
        return false;
    }
    let Ok(shard) = ShardId::parse_str(tail[0]) else {
        return false;
    };
    if shard.as_hyphenated().to_string() != tail[0] {
        return false;
    }
    let Some((manifest_name, temporary_id)) = tail[2].split_once(".tmp.") else {
        return false;
    };
    helpers::memwal::parse_positive_bit_reversed_filename(manifest_name, "binpb").is_some()
        && canonical_lower_uuid(temporary_id)
}

fn is_graph_table_identity_component(component: &str) -> bool {
    let Some((stable_table_id, table_incarnation_id)) = component.split_once('-') else {
        return false;
    };
    [stable_table_id, table_incarnation_id]
        .into_iter()
        .all(|part| part.len() == 16 && part.bytes().all(|byte| byte.is_ascii_hexdigit()))
}

impl PathIo {
    fn from_stats(stats: &IoStats) -> Self {
        let mut out = Self {
            total_reads: stats.read_iops,
            total_read_bytes: stats.read_bytes,
            total_writes: stats.write_iops,
            total_written_bytes: stats.written_bytes,
            ..Self::default()
        };
        for request in &stats.requests {
            let path = request.path.as_ref();
            let components = path.split('/').collect::<Vec<_>>();
            if matches!(
                request.method,
                "list" | "list_with_offset" | "list_with_delimiter"
            ) {
                out.list_requests += 1;
            }
            let write = matches!(
                request.method,
                "put_opts" | "put_part" | "copy" | "rename" | "delete"
            );
            let read = !write;
            let memwal = components.contains(&"_mem_wal");
            let stream_token = components.contains(&"_stream_tokens.lance");
            let dead_letter_object = components.contains(&"__dead_letter");
            let base_table = !memwal
                && components.windows(2).any(|pair| {
                    matches!(pair[0], "nodes" | "edges")
                        && is_graph_table_identity_component(pair[1])
                });
            let (classified_reads, classified_writes) = if memwal {
                (&mut out.memwal_reads, &mut out.memwal_writes)
            } else if stream_token {
                (&mut out.stream_token_reads, &mut out.stream_token_writes)
            } else if base_table {
                (&mut out.base_table_reads, &mut out.base_table_writes)
            } else {
                (&mut out.other_reads, &mut out.other_writes)
            };
            if read {
                *classified_reads += 1;
            } else {
                *classified_writes += 1;
            }
            if dead_letter_object && read {
                out.dead_letter_object_reads += 1;
            }
            if dead_letter_object && write {
                out.dead_letter_object_writes += 1;
            }
            let wal = memwal && (path.contains("/wal/") || path.ends_with("/wal"));
            let shard_manifest =
                memwal && (path.contains("/manifest/") || path.ends_with("/manifest"));
            let generation = memwal && path.contains("_gen_");
            let generation_data = generation
                && (path.contains("/data/") || path.ends_with("/data") || path.ends_with(".lance"));
            let pk_sidecar = generation && path.contains("/_pk_index/");
            if read && wal {
                out.wal_reads += 1;
            }
            if write && wal {
                out.wal_writes += 1;
            }
            if read && shard_manifest {
                out.shard_manifest_reads += 1;
            }
            if write && shard_manifest {
                out.shard_manifest_writes += 1;
            }
            if read && generation {
                out.generation_reads += 1;
            }
            if write && generation {
                out.generation_writes += 1;
            }
            if read && generation_data {
                out.generation_data_reads += 1;
            }
            if write && generation_data {
                out.generation_data_writes += 1;
            }
            if read && pk_sidecar {
                out.pk_sidecar_reads += 1;
            }
            if write && pk_sidecar {
                out.pk_sidecar_writes += 1;
            }
            if request.method == "delete" && memwal {
                if is_lance_atomic_memwal_temp_path(path) {
                    out.memwal_atomic_temp_delete_requests += 1;
                } else {
                    out.memwal_canonical_delete_requests += 1;
                }
            }
        }
        assert_eq!(
            out.memwal_reads + out.base_table_reads + out.stream_token_reads + out.other_reads,
            out.total_reads,
            "top-level path classes must account for every tracked read"
        );
        assert_eq!(
            out.memwal_writes + out.base_table_writes + out.stream_token_writes + out.other_writes,
            out.total_writes,
            "top-level path classes must account for every tracked write"
        );
        out
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct RetainedRootIo {
    reads: u64,
    writes: u64,
    deletes: u64,
}

fn path_is_under_any_root(path: &str, roots: &BTreeSet<String>) -> bool {
    roots.iter().any(|root| {
        path == root
            || path
                .strip_prefix(root)
                .is_some_and(|suffix| suffix.starts_with('/'))
    })
}

impl RetainedRootIo {
    fn from_stats(stats: &IoStats, roots: &BTreeSet<String>) -> Self {
        let mut out = Self::default();
        for request in &stats.requests {
            let path = request.path.as_ref();
            if !path_is_under_any_root(path, roots) {
                continue;
            }
            match request.method {
                "delete" => out.deletes += 1,
                "put_opts" | "put_part" | "copy" | "rename" => out.writes += 1,
                _ => out.reads += 1,
            }
        }
        out
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct AdapterIo {
    read_text: u64,
    read_text_if_exists: u64,
    exists: u64,
    read_text_versioned: u64,
    list_dir: u64,
    mutation_calls: u64,
}

impl AdapterIo {
    fn snapshot(counts: &StorageReadCounts) -> Self {
        Self {
            read_text: counts.read_text(),
            read_text_if_exists: counts.read_text_if_exists(),
            exists: counts.exists(),
            read_text_versioned: counts.read_text_versioned(),
            list_dir: counts.list_dir(),
            mutation_calls: counts.mutation_calls(),
        }
    }

    fn since(self, before: Self) -> Self {
        let delta = |current: u64, prior: u64| {
            current
                .checked_sub(prior)
                .expect("storage call counter regressed")
        };
        Self {
            read_text: delta(self.read_text, before.read_text),
            read_text_if_exists: delta(self.read_text_if_exists, before.read_text_if_exists),
            exists: delta(self.exists, before.exists),
            read_text_versioned: delta(self.read_text_versioned, before.read_text_versioned),
            list_dir: delta(self.list_dir, before.list_dir),
            mutation_calls: delta(self.mutation_calls, before.mutation_calls),
        }
    }

    fn operations(self) -> u64 {
        self.read_text
            + self.read_text_if_exists
            + self.exists
            + self.read_text_versioned
            + self.list_dir
            + self.mutation_calls
    }
}

#[derive(Debug)]
struct HistorySample {
    depth: u64,
    ack_elapsed_us: u128,
    ack_table: PathIo,
    ack_manifest: PathIo,
    ack_adapter: AdapterIo,
    fold_elapsed_us: u128,
    fold_table: PathIo,
    fold_manifest: PathIo,
}

#[derive(Debug)]
struct ColdReplaySample {
    retained_batches: u64,
    elapsed_us: u128,
    table: PathIo,
}

#[derive(Debug)]
struct FoldRowsSample {
    rows: usize,
    elapsed_us: u128,
    table: PathIo,
    physical_generation_data_bytes: u64,
}

#[derive(Debug)]
struct RetainedMetadataSample {
    merged_generations: u64,
    serialized_manifest_bytes: u64,
    cold_elapsed_us: u128,
    table: PathIo,
    inventory: CurrentMemWalInventory,
}

#[derive(Debug)]
struct RetentionTermSample {
    elapsed_us: u128,
    table: PathIo,
    manifest: PathIo,
    adapter: AdapterIo,
    retained_root_io: RetainedRootIo,
}

#[derive(Debug)]
struct RetentionDepthSample {
    retained_generations_before: u64,
    retained_generations_after: u64,
    before: CurrentMemWalInventory,
    after_ack: CurrentMemWalInventory,
    after_replay: CurrentMemWalInventory,
    after_fold: CurrentMemWalInventory,
    warm_ack: RetentionTermSample,
    cold_reopen_replay: RetentionTermSample,
    fold: RetentionTermSample,
    visibility_probe: RetentionTermSample,
    manifest_version_before: u64,
    manifest_version_after_ack: u64,
    manifest_version_after_replay: u64,
    manifest_version_after_fold: u64,
    table_version_before: u64,
    table_version_after_ack: u64,
    table_version_after_replay: u64,
    table_version_after_fold: u64,
    visible_rows_after_fold: usize,
    whole_process_peak_rss_bytes: Option<u64>,
}

#[derive(Debug)]
struct WidestRetainedGrowthSample {
    before: CurrentMemWalInventory,
    after_ack: CurrentMemWalInventory,
    after_fold: CurrentMemWalInventory,
    table_ack: PathIo,
    table_fold: PathIo,
    manifest_version_before: u64,
    manifest_version_after_ack: u64,
    manifest_version_after_fold: u64,
    table_version_before: u64,
    table_version_after_ack: u64,
    table_version_after_fold: u64,
    visible_rows: usize,
    sampled_payloads: Vec<(String, String)>,
    recovery_sidecars_after_fold: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
struct TokenCostObservation {
    elapsed_us: u128,
    table: PathIo,
    manifest: PathIo,
    adapter: AdapterIo,
}

#[derive(Debug)]
struct TokenCostFixture {
    uri: String,
    terminal_token: String,
    profile_revision: u64,
    receipt_operation_id: String,
}

#[derive(Debug)]
struct TokenAuthorityCostCut {
    selected_token_version: u64,
    total_fragments: u64,
    uncovered_fragments: Option<u64>,
    fresh_handle_hit: TokenCostObservation,
    fresh_handle_miss: TokenCostObservation,
    fresh_receipt_hit: TokenCostObservation,
    fresh_receipt_miss: TokenCostObservation,
    first_terminal_page: TokenCostObservation,
    warm_hits: Vec<TokenCostObservation>,
    warm_misses: Vec<TokenCostObservation>,
    warm_receipt_hits: Vec<TokenCostObservation>,
    warm_receipt_misses: Vec<TokenCostObservation>,
    repeat_terminal_pages: Vec<TokenCostObservation>,
    current_hit_result: Option<String>,
    receipt_hit_result: Option<String>,
    terminal_entries_json: serde_json::Value,
    terminal_next_cursor: Option<String>,
    terminal_page_entries: usize,
    terminal_page_bytes: usize,
}

#[derive(Debug)]
struct TokenIndexReconciliationCost {
    prior_selected_version: u64,
    next_selected_version: u64,
    observation: TokenCostObservation,
}

#[derive(Debug, Clone, Copy)]
struct TokenIndexDecisionTerm {
    request_ratio: f64,
    request_benefit: u64,
    request_break_even_calls: Option<u64>,
    request_qualifies: bool,
    byte_ratio: f64,
    byte_benefit: u64,
    byte_break_even_calls: Option<u64>,
    byte_qualifies: bool,
}

impl TokenIndexDecisionTerm {
    fn fully_qualifies(self) -> bool {
        self.request_qualifies && self.byte_qualifies
    }
}

#[derive(Debug)]
struct TokenAuthorityCostSample {
    profile_cycles: u64,
    profile_revision: u64,
    uncovered: TokenAuthorityCostCut,
    reconciliation: TokenIndexReconciliationCost,
    reconciled: TokenAuthorityCostCut,
    cumulative_process_peak_rss_bytes: Option<u64>,
}

fn token_cost_observation(
    started: Instant,
    table_tracker: &IOTracker,
    manifest_tracker: &IOTracker,
    adapter_counts: &StorageReadCounts,
    adapter_before: AdapterIo,
) -> TokenCostObservation {
    TokenCostObservation {
        elapsed_us: started.elapsed().as_micros(),
        table: PathIo::from_stats(&table_tracker.incremental_stats()),
        manifest: PathIo::from_stats(&manifest_tracker.incremental_stats()),
        adapter: AdapterIo::snapshot(adapter_counts).since(adapter_before),
    }
}

async fn initialize_token_authority_cost_fixture(
    cluster_uri: &str,
    profile_cycles: u64,
) -> TokenCostFixture {
    assert!(profile_cycles > 0);
    let uri = helpers::stream_authority::graph_uri(cluster_uri);
    let db = Arc::new(Omnigraph::init(&uri, TOKEN_COST_SCHEMA).await.unwrap());
    db.mutate(
        "main",
        TOKEN_COST_SEED,
        "seed_token_cost_conflict",
        &helpers::int_params(&[("$score", 0)]),
    )
    .await
    .expect("seed the fixed base uniqueness conflict before profile history");

    let setup_table_tracker = IOTracker::default();
    let setup_manifest_tracker = IOTracker::default();
    with_raw_io_trackers(&setup_table_tracker, &setup_manifest_tracker, async {
        for cycle in 0..profile_cycles {
            let status = db.stream_status().await.unwrap();
            assert_eq!(status.profile_mode, "DISABLED");
            assert!(
                status.tables.is_empty(),
                "variable authority history must be generated before enrollment"
            );
            helpers::stream_authority::enable_stream_profile(&db, cluster_uri).await;
            helpers::stream_authority::disable_stream_profile_with_operation_id(
                &db,
                cluster_uri,
                &format!("memwal-token-cost-disable-{cycle:08}"),
            )
            .await
            .expect("a zero-lane profile cycle must finish disabled");
        }
    })
    .await;
    let setup_table = PathIo::from_stats(&setup_table_tracker.incremental_stats());
    let _ = setup_manifest_tracker.incremental_stats();
    assert_eq!(setup_table.memwal_reads, 0, "{setup_table:#?}");
    assert_eq!(setup_table.memwal_writes, 0, "{setup_table:#?}");

    let status = db.stream_status().await.unwrap();
    assert_eq!(status.profile_mode, "DISABLED");
    assert!(status.tables.is_empty());
    db.failpoint_enroll_stream_table_for_test(TABLE)
        .await
        .expect("cost fixture enrollment must remain profile-disabled");
    helpers::stream_authority::enable_stream_profile(&db, cluster_uri).await;
    let profile_revision = db.stream_status().await.unwrap().profile_revision;
    assert!(profile_revision > 0);

    let db = helpers::stream_authority::bind_checked_stream_runtime(db, cluster_uri).await;
    let stream_incarnation_id = db
        .failpoint_stream_incarnation_for_test(TABLE)
        .await
        .unwrap();
    let batch = score_batch(table_schema(&db).await, TOKEN_COST_LOGICAL_ID_PREFIX, 1);
    let terminal_token = db
        .failpoint_stream_b2_for_test(
            TABLE,
            batch,
            0,
            &stream_incarnation_id,
            "f6b3f6b3-f6b3-46b3-86b3-f6b3f6b3f6b3",
            None,
            TOKEN_COST_CONTRIBUTOR,
        )
        .await
        .expect("the fixed conflicting occurrence must be acknowledged before fold");
    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .expect("the fixed conflicting occurrence must become terminal");
    drop(db);

    TokenCostFixture {
        uri,
        terminal_token,
        profile_revision,
        receipt_operation_id: format!("memwal-token-cost-disable-{:08}", profile_cycles - 1),
    }
}

async fn measure_token_lookup(
    db: &Omnigraph,
    logical_id: &str,
    table_tracker: &IOTracker,
    manifest_tracker: &IOTracker,
    adapter_counts: &StorageReadCounts,
) -> (TokenCostObservation, u64, Option<String>) {
    let adapter_before =
        reset_retention_term_trackers(table_tracker, manifest_tracker, adapter_counts);
    let started = Instant::now();
    let (selected_version, token) = db
        .failpoint_stream_token_lookup_for_cost_test(TABLE, logical_id)
        .await
        .expect("the exact selected-token cost probe must succeed");
    (
        token_cost_observation(
            started,
            table_tracker,
            manifest_tracker,
            adapter_counts,
            adapter_before,
        ),
        selected_version,
        token,
    )
}

async fn measure_profile_receipt_lookup(
    db: &Omnigraph,
    cluster_uri: &str,
    operation_id: &str,
    table_tracker: &IOTracker,
    manifest_tracker: &IOTracker,
    adapter_counts: &StorageReadCounts,
) -> (TokenCostObservation, u64, Option<String>) {
    let adapter_before =
        reset_retention_term_trackers(table_tracker, manifest_tracker, adapter_counts);
    let started = Instant::now();
    let (selected_version, record_id) = db
        .failpoint_stream_profile_receipt_lookup_for_cost_test(
            cluster_uri,
            TOKEN_COST_GRAPH_ID,
            operation_id,
        )
        .await
        .expect("the exact selected receipt cost probe must succeed");
    (
        token_cost_observation(
            started,
            table_tracker,
            manifest_tracker,
            adapter_counts,
            adapter_before,
        ),
        selected_version,
        record_id,
    )
}

async fn measure_terminal_page(
    db: &Omnigraph,
    cluster_uri: &str,
    table_tracker: &IOTracker,
    manifest_tracker: &IOTracker,
    adapter_counts: &StorageReadCounts,
) -> (TokenCostObservation, StreamDeadLetterPage) {
    let mut adapter_before = None;
    let mut started = None;
    let page = helpers::stream_authority::list_stream_dead_letters_after_authority(
        db,
        cluster_uri,
        None,
        || {
            adapter_before = Some(reset_retention_term_trackers(
                table_tracker,
                manifest_tracker,
                adapter_counts,
            ));
            started = Some(Instant::now());
        },
    )
    .await
    .expect("the exact selected-token terminal scan must succeed");
    (
        token_cost_observation(
            started.expect("the authority boundary must start the measurement"),
            table_tracker,
            manifest_tracker,
            adapter_counts,
            adapter_before.expect("the authority boundary must reset the adapters"),
        ),
        page,
    )
}

async fn token_authority_cost_cut(
    fixture: &TokenCostFixture,
    cluster_uri: &str,
    adapter: &Arc<dyn omnigraph::storage::StorageAdapter>,
    adapter_counts: &StorageReadCounts,
    table_tracker: &IOTracker,
    manifest_tracker: &IOTracker,
) -> TokenAuthorityCostCut {
    let coverage_db = Arc::new(
        Omnigraph::open_with_storage(&fixture.uri, Arc::clone(adapter))
            .await
            .unwrap(),
    );
    let (selected_token_version, total_fragments, uncovered_fragments) = coverage_db
        .failpoint_stream_token_lookup_coverage_for_cost_test()
        .await
        .expect("selected token-index coverage must be observable");
    drop(coverage_db);

    let fresh_handle_hit_db = Arc::new(
        Omnigraph::open_with_storage(&fixture.uri, Arc::clone(adapter))
            .await
            .unwrap(),
    );
    let (fresh_handle_hit, hit_version, hit) = measure_token_lookup(
        &fresh_handle_hit_db,
        TOKEN_COST_LOGICAL_ID,
        &table_tracker,
        &manifest_tracker,
        &adapter_counts,
    )
    .await;
    assert_eq!(hit_version, selected_token_version);
    assert_eq!(hit.as_deref(), Some(fixture.terminal_token.as_str()));
    drop(fresh_handle_hit_db);

    let fresh_handle_miss_db = Arc::new(
        Omnigraph::open_with_storage(&fixture.uri, Arc::clone(adapter))
            .await
            .unwrap(),
    );
    let (fresh_handle_miss, miss_version, miss) = measure_token_lookup(
        &fresh_handle_miss_db,
        TOKEN_COST_MISSING_ID,
        &table_tracker,
        &manifest_tracker,
        &adapter_counts,
    )
    .await;
    assert_eq!(miss_version, selected_token_version);
    assert!(miss.is_none());
    drop(fresh_handle_miss_db);

    let fresh_receipt_hit_db = Arc::new(
        Omnigraph::open_with_storage(&fixture.uri, Arc::clone(adapter))
            .await
            .unwrap(),
    );
    let (fresh_receipt_hit, receipt_hit_version, receipt_hit) = measure_profile_receipt_lookup(
        &fresh_receipt_hit_db,
        cluster_uri,
        &fixture.receipt_operation_id,
        table_tracker,
        manifest_tracker,
        adapter_counts,
    )
    .await;
    assert_eq!(receipt_hit_version, selected_token_version);
    assert!(receipt_hit.is_some());
    drop(fresh_receipt_hit_db);

    let fresh_receipt_miss_db = Arc::new(
        Omnigraph::open_with_storage(&fixture.uri, Arc::clone(adapter))
            .await
            .unwrap(),
    );
    let (fresh_receipt_miss, receipt_miss_version, receipt_miss) = measure_profile_receipt_lookup(
        &fresh_receipt_miss_db,
        cluster_uri,
        TOKEN_COST_MISSING_RECEIPT_OPERATION,
        table_tracker,
        manifest_tracker,
        adapter_counts,
    )
    .await;
    assert_eq!(receipt_miss_version, selected_token_version);
    assert!(receipt_miss.is_none());
    drop(fresh_receipt_miss_db);

    let first_page_db = Arc::new(
        Omnigraph::open_with_storage(&fixture.uri, Arc::clone(adapter))
            .await
            .unwrap(),
    );
    let (first_terminal_page, first_page) = measure_terminal_page(
        &first_page_db,
        cluster_uri,
        &table_tracker,
        &manifest_tracker,
        &adapter_counts,
    )
    .await;
    assert_eq!(first_page.token_table_version, selected_token_version);
    assert_eq!(first_page.entries.len(), 1);
    assert_eq!(first_page.entries[0].logical_id, TOKEN_COST_LOGICAL_ID);
    assert_eq!(
        first_page.entries[0].occurrence_token,
        fixture.terminal_token
    );
    assert!(first_page.next_cursor.is_none());
    drop(first_page_db);

    let warm_db = Arc::new(
        Omnigraph::open_with_storage(&fixture.uri, Arc::clone(adapter))
            .await
            .unwrap(),
    );
    let (_, warmup_version, warmup_hit) = measure_token_lookup(
        &warm_db,
        TOKEN_COST_LOGICAL_ID,
        &table_tracker,
        &manifest_tracker,
        &adapter_counts,
    )
    .await;
    assert_eq!(warmup_version, selected_token_version);
    assert_eq!(warmup_hit.as_deref(), Some(fixture.terminal_token.as_str()));
    let (_, warmup_version, warmup_miss) = measure_token_lookup(
        &warm_db,
        TOKEN_COST_MISSING_ID,
        &table_tracker,
        &manifest_tracker,
        &adapter_counts,
    )
    .await;
    assert_eq!(warmup_version, selected_token_version);
    assert!(warmup_miss.is_none());
    let (_, warmup_version, warmup_receipt_hit) = measure_profile_receipt_lookup(
        &warm_db,
        cluster_uri,
        &fixture.receipt_operation_id,
        table_tracker,
        manifest_tracker,
        adapter_counts,
    )
    .await;
    assert_eq!(warmup_version, selected_token_version);
    assert_eq!(warmup_receipt_hit, receipt_hit);
    let (_, warmup_version, warmup_receipt_miss) = measure_profile_receipt_lookup(
        &warm_db,
        cluster_uri,
        TOKEN_COST_MISSING_RECEIPT_OPERATION,
        table_tracker,
        manifest_tracker,
        adapter_counts,
    )
    .await;
    assert_eq!(warmup_version, selected_token_version);
    assert!(warmup_receipt_miss.is_none());
    let (_, repeat_warmup_page) = measure_terminal_page(
        &warm_db,
        cluster_uri,
        &table_tracker,
        &manifest_tracker,
        &adapter_counts,
    )
    .await;
    assert_eq!(repeat_warmup_page.entries.len(), 1);

    let mut warm_hits = Vec::with_capacity(TOKEN_COST_SERIES_REPETITIONS);
    for _ in 0..TOKEN_COST_SERIES_REPETITIONS {
        let (observation, version, hit) = measure_token_lookup(
            &warm_db,
            TOKEN_COST_LOGICAL_ID,
            &table_tracker,
            &manifest_tracker,
            &adapter_counts,
        )
        .await;
        assert_eq!(version, selected_token_version);
        assert_eq!(hit.as_deref(), Some(fixture.terminal_token.as_str()));
        warm_hits.push(observation);
    }
    let mut warm_misses = Vec::with_capacity(TOKEN_COST_SERIES_REPETITIONS);
    for _ in 0..TOKEN_COST_SERIES_REPETITIONS {
        let (observation, version, miss) = measure_token_lookup(
            &warm_db,
            TOKEN_COST_MISSING_ID,
            &table_tracker,
            &manifest_tracker,
            &adapter_counts,
        )
        .await;
        assert_eq!(version, selected_token_version);
        assert!(miss.is_none());
        warm_misses.push(observation);
    }
    let mut warm_receipt_hits = Vec::with_capacity(TOKEN_COST_SERIES_REPETITIONS);
    for _ in 0..TOKEN_COST_SERIES_REPETITIONS {
        let (observation, version, selected) = measure_profile_receipt_lookup(
            &warm_db,
            cluster_uri,
            &fixture.receipt_operation_id,
            table_tracker,
            manifest_tracker,
            adapter_counts,
        )
        .await;
        assert_eq!(version, selected_token_version);
        assert_eq!(selected, receipt_hit);
        warm_receipt_hits.push(observation);
    }
    let mut warm_receipt_misses = Vec::with_capacity(TOKEN_COST_SERIES_REPETITIONS);
    for _ in 0..TOKEN_COST_SERIES_REPETITIONS {
        let (observation, version, selected) = measure_profile_receipt_lookup(
            &warm_db,
            cluster_uri,
            TOKEN_COST_MISSING_RECEIPT_OPERATION,
            table_tracker,
            manifest_tracker,
            adapter_counts,
        )
        .await;
        assert_eq!(version, selected_token_version);
        assert!(selected.is_none());
        warm_receipt_misses.push(observation);
    }
    let mut repeat_terminal_pages = Vec::with_capacity(TOKEN_COST_SERIES_REPETITIONS);
    for _ in 0..TOKEN_COST_SERIES_REPETITIONS {
        let (observation, page) = measure_terminal_page(
            &warm_db,
            cluster_uri,
            &table_tracker,
            &manifest_tracker,
            &adapter_counts,
        )
        .await;
        assert_eq!(page.token_table_version, selected_token_version);
        assert_eq!(page.entries, first_page.entries);
        assert!(page.next_cursor.is_none());
        repeat_terminal_pages.push(observation);
    }

    TokenAuthorityCostCut {
        selected_token_version,
        total_fragments,
        uncovered_fragments,
        fresh_handle_hit,
        fresh_handle_miss,
        fresh_receipt_hit,
        fresh_receipt_miss,
        first_terminal_page,
        warm_hits,
        warm_misses,
        warm_receipt_hits,
        warm_receipt_misses,
        repeat_terminal_pages,
        current_hit_result: hit,
        receipt_hit_result: receipt_hit,
        terminal_entries_json: serde_json::to_value(&first_page.entries).unwrap(),
        terminal_next_cursor: first_page.next_cursor.clone(),
        terminal_page_entries: first_page.entries.len(),
        terminal_page_bytes: serde_json::to_vec(&first_page).unwrap().len(),
    }
}

async fn token_authority_cost_sample_at(
    cluster_uri: &str,
    profile_cycles: u64,
) -> TokenAuthorityCostSample {
    let fixture = initialize_token_authority_cost_fixture(cluster_uri, profile_cycles).await;
    let table_tracker = IOTracker::default();
    let manifest_tracker = IOTracker::default();

    with_raw_io_trackers(&table_tracker, &manifest_tracker, async {
        let (adapter, adapter_counts) =
            CountingStorageAdapter::new(storage_for_uri(&fixture.uri).unwrap());
        let uncovered = token_authority_cost_cut(
            &fixture,
            cluster_uri,
            &adapter,
            &adapter_counts,
            &table_tracker,
            &manifest_tracker,
        )
        .await;

        let reconcile_db = Arc::new(
            Omnigraph::open_with_storage(&fixture.uri, Arc::clone(&adapter))
                .await
                .unwrap(),
        );
        let measurement_start = RefCell::new(None);
        let measured_observation = RefCell::new(None);
        let (prior_selected_version, next_selected_version) = reconcile_db
            .failpoint_reconcile_stream_token_lookup_index_for_cost_test(
                || {
                    let adapter_before = reset_retention_term_trackers(
                        &table_tracker,
                        &manifest_tracker,
                        &adapter_counts,
                    );
                    measurement_start.replace(Some((Instant::now(), adapter_before)));
                },
                || {
                    let (started, adapter_before) = measurement_start
                        .borrow_mut()
                        .take()
                        .expect("the F6b7 maintenance window must start exactly once");
                    measured_observation.replace(Some(token_cost_observation(
                        started,
                        &table_tracker,
                        &manifest_tracker,
                        &adapter_counts,
                        adapter_before,
                    )));
                },
            )
            .await
            .expect("the F6b7 evidence-only token-index cut must reconcile");
        let reconciliation = TokenIndexReconciliationCost {
            prior_selected_version,
            next_selected_version,
            observation: measured_observation
                .into_inner()
                .expect("the F6b7 maintenance window must finish exactly once"),
        };
        drop(reconcile_db);

        let reconciled = token_authority_cost_cut(
            &fixture,
            cluster_uri,
            &adapter,
            &adapter_counts,
            &table_tracker,
            &manifest_tracker,
        )
        .await;

        TokenAuthorityCostSample {
            profile_cycles,
            profile_revision: fixture.profile_revision,
            uncovered,
            reconciliation,
            reconciled,
            cumulative_process_peak_rss_bytes: current_process_peak_rss_bytes(),
        }
    })
    .await
}

fn assert_token_cost_observation(
    sample: &TokenAuthorityCostSample,
    term: &str,
    observation: &TokenCostObservation,
) {
    assert_eq!(
        observation.table.total_writes, 0,
        "{term} moved table authority: {sample:#?}"
    );
    assert_eq!(
        observation.manifest.total_writes, 0,
        "{term} moved graph authority: {sample:#?}"
    );
    assert_eq!(
        observation.table.list_requests, 0,
        "{term} listed the table store: {sample:#?}"
    );
    // A fresh graph-manifest handle may legitimately resolve its object-store
    // namespace with LIST. The token dataset itself must remain prefix-list
    // free; pinning manifest LIST to zero would reject the configured backend's
    // real opener rather than a token-lookup regression.
    assert_eq!(
        observation.table.memwal_reads, 0,
        "{term} consulted MemWAL history: {sample:#?}"
    );
    assert_eq!(
        observation.table.base_table_reads, 0,
        "{term} consulted the base table: {sample:#?}"
    );
    assert_eq!(
        observation.table.other_reads, 0,
        "{term} escaped the selected token dataset: {sample:#?}"
    );
    assert_eq!(
        observation.table.dead_letter_object_reads, 0,
        "{term} fetched a dead-letter payload object: {sample:#?}"
    );
    assert_eq!(
        observation.table.dead_letter_object_writes, 0,
        "{term} wrote a dead-letter payload object: {sample:#?}"
    );
    assert_eq!(
        observation.adapter.read_text_if_exists, 0,
        "{term} fetched a dead-letter payload object: {sample:#?}"
    );
    assert_eq!(
        observation.adapter.read_text_versioned, 0,
        "{term} performed an unexpected versioned metadata read: {sample:#?}"
    );
    assert_eq!(
        observation.adapter.list_dir, 0,
        "{term} listed through the storage adapter: {sample:#?}"
    );
    assert_eq!(
        observation.adapter.mutation_calls, 0,
        "{term} mutated through the storage adapter: {sample:#?}"
    );
}

fn assert_token_authority_cost_cut(
    sample: &TokenAuthorityCostSample,
    label: &str,
    cut: &TokenAuthorityCostCut,
    expect_uncovered: bool,
) {
    assert!(cut.selected_token_version > 0);
    assert!(cut.total_fragments > 0, "{label}: {sample:#?}");
    let uncovered = cut
        .uncovered_fragments
        .expect("the named token lookup index must report exact bitmap coverage");
    if expect_uncovered {
        assert!(uncovered > 0, "the fixture must retain an uncovered tail");
    } else {
        assert_eq!(uncovered, 0, "the reconciled cut must be fully covered");
    }
    assert!(uncovered <= cut.total_fragments, "{label}: {sample:#?}");
    assert_eq!(cut.terminal_page_entries, 1);
    assert!(cut.terminal_page_bytes > 0);
    assert!(cut.current_hit_result.is_some());
    assert!(cut.receipt_hit_result.is_some());
    assert!(cut.terminal_next_cursor.is_none());

    let fresh = [
        ("fresh_handle_hit", &cut.fresh_handle_hit),
        ("fresh_handle_miss", &cut.fresh_handle_miss),
        ("fresh_receipt_hit", &cut.fresh_receipt_hit),
        ("fresh_receipt_miss", &cut.fresh_receipt_miss),
        ("first_terminal_page", &cut.first_terminal_page),
    ];
    for (term, observation) in fresh {
        assert_token_cost_observation(sample, &format!("{label}_{term}"), observation);
        assert!(
            observation.table.stream_token_reads > 0,
            "{label}_{term}: {sample:#?}"
        );
    }
    assert_eq!(cut.first_terminal_page.adapter.operations(), 0);

    let series = [
        ("warm_hit", &cut.warm_hits),
        ("warm_miss", &cut.warm_misses),
        ("warm_receipt_hit", &cut.warm_receipt_hits),
        ("warm_receipt_miss", &cut.warm_receipt_misses),
        ("repeat_terminal_page", &cut.repeat_terminal_pages),
    ];
    for (term, observations) in series {
        assert_eq!(observations.len(), TOKEN_COST_SERIES_REPETITIONS);
        for observation in observations {
            assert_token_cost_observation(sample, &format!("{label}_{term}"), observation);
        }
    }
    for observation in &cut.repeat_terminal_pages {
        assert_eq!(observation.adapter.operations(), 0);
    }
}

fn assert_token_authority_cost_sample(sample: &TokenAuthorityCostSample) {
    assert!(sample.profile_cycles > 0);
    assert!(sample.profile_revision > 0);
    assert!(sample.cumulative_process_peak_rss_bytes.is_some() || !cfg!(unix));
    assert_token_authority_cost_cut(sample, "uncovered", &sample.uncovered, true);
    assert_token_authority_cost_cut(sample, "reconciled", &sample.reconciled, false);

    assert_eq!(
        sample.reconciliation.prior_selected_version,
        sample.uncovered.selected_token_version
    );
    assert_eq!(
        sample.reconciliation.next_selected_version,
        sample.uncovered.selected_token_version + 1
    );
    assert_eq!(
        sample.reconciled.selected_token_version,
        sample.reconciliation.next_selected_version
    );
    assert_eq!(
        sample.uncovered.total_fragments, sample.reconciled.total_fragments,
        "index reconciliation must not change the selected fragment set"
    );
    assert_eq!(
        sample.uncovered.current_hit_result,
        sample.reconciled.current_hit_result
    );
    assert_eq!(
        sample.uncovered.receipt_hit_result,
        sample.reconciled.receipt_hit_result
    );
    assert_eq!(
        sample.uncovered.terminal_entries_json,
        sample.reconciled.terminal_entries_json
    );
    assert_eq!(
        sample.uncovered.terminal_next_cursor,
        sample.reconciled.terminal_next_cursor
    );

    let maintenance = &sample.reconciliation.observation;
    assert!(maintenance.table.stream_token_reads > 0, "{sample:#?}");
    assert!(maintenance.table.stream_token_writes > 0, "{sample:#?}");
    assert!(maintenance.manifest.total_writes > 0, "{sample:#?}");
    assert_eq!(maintenance.table.memwal_reads, 0, "{sample:#?}");
    assert_eq!(maintenance.table.memwal_writes, 0, "{sample:#?}");
    assert_eq!(maintenance.table.base_table_reads, 0, "{sample:#?}");
    assert_eq!(maintenance.table.base_table_writes, 0, "{sample:#?}");
    assert_eq!(maintenance.table.dead_letter_object_reads, 0, "{sample:#?}");
    assert_eq!(
        maintenance.table.dead_letter_object_writes, 0,
        "{sample:#?}"
    );
    assert_eq!(maintenance.adapter.mutation_calls, 0, "{sample:#?}");
}

fn assert_token_authority_cost_growth(
    shallow: &TokenAuthorityCostSample,
    deep: &TokenAuthorityCostSample,
) {
    assert!(
        deep.uncovered.selected_token_version > shallow.uncovered.selected_token_version,
        "the variable fixture failed to grow selected token authority: shallow={shallow:#?} deep={deep:#?}"
    );
    assert!(
        deep.profile_revision > shallow.profile_revision,
        "the variable fixture failed to grow selected profile authority: shallow={shallow:#?} deep={deep:#?}"
    );
    assert!(
        deep.uncovered.total_fragments > shallow.uncovered.total_fragments,
        "the variable fixture failed to grow retained token fragments: shallow={shallow:#?} deep={deep:#?}"
    );
    assert!(
        deep.uncovered.uncovered_fragments.unwrap()
            > shallow.uncovered.uncovered_fragments.unwrap(),
        "the variable fixture failed to grow the selected uncovered tail: shallow={shallow:#?} deep={deep:#?}"
    );
}

fn sample_p50_elapsed_us(observations: &[TokenCostObservation]) -> u128 {
    assert!(!observations.is_empty());
    let mut values = observations
        .iter()
        .map(|observation| observation.elapsed_us)
        .collect::<Vec<_>>();
    values.sort_unstable();
    values[(values.len() - 1) / 2]
}

fn series_io(observations: &[TokenCostObservation]) -> (u64, u64, u64, u64, u64) {
    observations.iter().fold(
        (0, 0, 0, 0, 0),
        |(token_reads, table_bytes, manifest_reads, manifest_bytes, adapter_ops), observation| {
            (
                token_reads.saturating_add(observation.table.stream_token_reads),
                table_bytes.saturating_add(observation.table.total_read_bytes),
                manifest_reads.saturating_add(observation.manifest.total_reads),
                manifest_bytes.saturating_add(observation.manifest.total_read_bytes),
                adapter_ops.saturating_add(observation.adapter.operations()),
            )
        },
    )
}

fn token_index_decision_term(
    sample: &TokenAuthorityCostSample,
    before: &[TokenCostObservation],
    after: &[TokenCostObservation],
) -> TokenIndexDecisionTerm {
    assert_eq!(before.len(), after.len());
    let (before_requests, before_bytes, _, _, _) = series_io(before);
    let (after_requests, after_bytes, _, _, _) = series_io(after);
    let request_benefit = before_requests.saturating_sub(after_requests);
    let byte_benefit = before_bytes.saturating_sub(after_bytes);
    let maintenance = &sample.reconciliation.observation;
    let maintenance_requests = maintenance
        .table
        .total_reads
        .saturating_add(maintenance.table.total_writes)
        .saturating_add(maintenance.manifest.total_reads)
        .saturating_add(maintenance.manifest.total_writes)
        .saturating_add(maintenance.adapter.operations());
    let maintenance_bytes = maintenance
        .table
        .total_read_bytes
        .saturating_add(maintenance.table.total_written_bytes)
        .saturating_add(maintenance.manifest.total_read_bytes)
        .saturating_add(maintenance.manifest.total_written_bytes);
    let samples = u64::try_from(before.len()).unwrap();
    let request_break_even_calls = (request_benefit > 0).then(|| {
        maintenance_requests
            .saturating_mul(samples)
            .div_ceil(request_benefit)
    });
    let byte_break_even_calls = (byte_benefit > 0).then(|| {
        maintenance_bytes
            .saturating_mul(samples)
            .div_ceil(byte_benefit)
    });
    let request_ratio = before_requests as f64 / after_requests.max(1) as f64;
    let byte_ratio = before_bytes as f64 / after_bytes.max(1) as f64;
    TokenIndexDecisionTerm {
        request_ratio,
        request_benefit,
        request_break_even_calls,
        request_qualifies: request_ratio >= 2.0
            && request_break_even_calls.is_some_and(|calls| calls <= 1_000),
        byte_ratio,
        byte_benefit,
        byte_break_even_calls,
        byte_qualifies: byte_ratio >= 2.0
            && byte_break_even_calls.is_some_and(|calls| calls <= 1_000),
    }
}

fn token_index_decision_terms(
    sample: &TokenAuthorityCostSample,
) -> [(&'static str, TokenIndexDecisionTerm); 4] {
    [
        (
            "warm_hit",
            token_index_decision_term(
                sample,
                &sample.uncovered.warm_hits,
                &sample.reconciled.warm_hits,
            ),
        ),
        (
            "warm_miss",
            token_index_decision_term(
                sample,
                &sample.uncovered.warm_misses,
                &sample.reconciled.warm_misses,
            ),
        ),
        (
            "warm_receipt_hit",
            token_index_decision_term(
                sample,
                &sample.uncovered.warm_receipt_hits,
                &sample.reconciled.warm_receipt_hits,
            ),
        ),
        (
            "warm_receipt_miss",
            token_index_decision_term(
                sample,
                &sample.uncovered.warm_receipt_misses,
                &sample.reconciled.warm_receipt_misses,
            ),
        ),
    ]
}

fn assert_configured_rustfs_token_index_uncompacted_bounded_no_go(
    samples: &[TokenAuthorityCostSample],
) {
    assert_eq!(samples.len(), 4);
    assert!(
        samples[..samples.len() - 1]
            .iter()
            .all(|sample| token_index_decision_terms(sample)
                .into_iter()
                .all(|(_, term)| term.fully_qualifies())),
        "the shallower configured-RustFS samples must establish the provisional crossing"
    );

    let deepest = samples.last().unwrap();
    assert_eq!(deepest.profile_cycles, 128);
    assert_eq!(deepest.uncovered.uncovered_fragments, Some(260));
    for (name, term) in token_index_decision_terms(deepest) {
        assert!(term.request_ratio >= 2.0, "{name}: {term:#?}");
        assert!(term.request_benefit > 0, "{name}: {term:#?}");
        assert!(
            term.request_break_even_calls
                .is_some_and(|calls| calls > 1_000),
            "{name}: the token-table read-request term must record the bounded NO-GO: {term:#?}"
        );
        assert!(!term.request_qualifies, "{name}: {term:#?}");
        assert!(term.byte_ratio >= 2.0, "{name}: {term:#?}");
        assert!(term.byte_benefit > 0, "{name}: {term:#?}");
        assert!(
            term.byte_break_even_calls
                .is_some_and(|calls| calls <= 1_000),
            "{name}: the byte term must preserve the measured mixed result: {term:#?}"
        );
        assert!(term.byte_qualifies, "{name}: {term:#?}");
        assert!(!term.fully_qualifies(), "{name}: {term:#?}");
    }
    eprintln!(
        "F6b7 token-index-decision rustfs-decision fixture=uncompacted_profile_cycles outcome=bounded_no_go max_exact_uncovered_fragments=260 failed_dimension=token_table_read_requests remeasure_above_fragments=260 remeasure_on_manifest_compaction=true"
    );
}

fn print_token_cost_observation(
    sample: &TokenAuthorityCostSample,
    scale: &str,
    cut: &str,
    term: &str,
    observation: &TokenCostObservation,
) {
    eprintln!(
        "F6b7 token-index-decision {scale} profile_cycles={} cut={cut} term={term} elapsed_us={} token_reads={} table_read_bytes={} manifest_reads={} manifest_read_bytes={} adapter_ops={}",
        sample.profile_cycles,
        observation.elapsed_us,
        observation.table.stream_token_reads,
        observation.table.total_read_bytes,
        observation.manifest.total_reads,
        observation.manifest.total_read_bytes,
        observation.adapter.operations(),
    );
}

fn print_token_cost_series(
    sample: &TokenAuthorityCostSample,
    scale: &str,
    cut: &str,
    term: &str,
    observations: &[TokenCostObservation],
) {
    let (token_reads, table_read_bytes, manifest_reads, manifest_read_bytes, adapter_ops) =
        series_io(observations);
    eprintln!(
        "F6b7 token-index-decision {scale} profile_cycles={} cut={cut} term={term} samples={} sample_p50_us={} sample_max_us={} token_reads={} table_read_bytes={} manifest_reads={} manifest_read_bytes={} adapter_ops={}",
        sample.profile_cycles,
        observations.len(),
        sample_p50_elapsed_us(observations),
        observations
            .iter()
            .map(|observation| observation.elapsed_us)
            .max()
            .unwrap(),
        token_reads,
        table_read_bytes,
        manifest_reads,
        manifest_read_bytes,
        adapter_ops,
    );
}

fn print_token_authority_cost_cut(
    sample: &TokenAuthorityCostSample,
    scale: &str,
    label: &str,
    cut: &TokenAuthorityCostCut,
) {
    let uncovered = cut.uncovered_fragments.unwrap_or(0);
    eprintln!(
        "F6b7 token-index-decision {scale} profile_cycles={} cut={label} term=summary profile_revision={} selected_token_version={} fragments={} covered_fragments={} uncovered_fragments={:?} terminal_entries={} terminal_page_bytes={} cumulative_process_peak_rss_bytes={:?}",
        sample.profile_cycles,
        sample.profile_revision,
        cut.selected_token_version,
        cut.total_fragments,
        cut.total_fragments.saturating_sub(uncovered),
        cut.uncovered_fragments,
        cut.terminal_page_entries,
        cut.terminal_page_bytes,
        sample.cumulative_process_peak_rss_bytes,
    );
    print_token_cost_observation(
        sample,
        scale,
        label,
        "fresh_handle_hit",
        &cut.fresh_handle_hit,
    );
    print_token_cost_observation(
        sample,
        scale,
        label,
        "fresh_handle_miss",
        &cut.fresh_handle_miss,
    );
    print_token_cost_observation(
        sample,
        scale,
        label,
        "fresh_receipt_hit",
        &cut.fresh_receipt_hit,
    );
    print_token_cost_observation(
        sample,
        scale,
        label,
        "fresh_receipt_miss",
        &cut.fresh_receipt_miss,
    );
    print_token_cost_observation(
        sample,
        scale,
        label,
        "first_terminal_page",
        &cut.first_terminal_page,
    );
    print_token_cost_series(sample, scale, label, "warm_hit", &cut.warm_hits);
    print_token_cost_series(sample, scale, label, "warm_miss", &cut.warm_misses);
    print_token_cost_series(
        sample,
        scale,
        label,
        "warm_receipt_hit",
        &cut.warm_receipt_hits,
    );
    print_token_cost_series(
        sample,
        scale,
        label,
        "warm_receipt_miss",
        &cut.warm_receipt_misses,
    );
    print_token_cost_series(
        sample,
        scale,
        label,
        "repeat_terminal_page",
        &cut.repeat_terminal_pages,
    );
}

fn print_token_index_decision_term(
    sample: &TokenAuthorityCostSample,
    scale: &str,
    term: &str,
    before: &[TokenCostObservation],
    after: &[TokenCostObservation],
) {
    let decision = token_index_decision_term(sample, before, after);
    eprintln!(
        "F6b7 token-index-decision {scale} profile_cycles={} term={term} request_ratio={:.3} request_benefit={} request_break_even_calls={:?} request_qualifies={} byte_ratio={:.3} byte_benefit={} byte_break_even_calls={:?} byte_qualifies={} fully_qualifies={}",
        sample.profile_cycles,
        decision.request_ratio,
        decision.request_benefit,
        decision.request_break_even_calls,
        decision.request_qualifies,
        decision.byte_ratio,
        decision.byte_benefit,
        decision.byte_break_even_calls,
        decision.byte_qualifies,
        decision.fully_qualifies(),
    );
}

fn print_token_authority_cost_sample(sample: &TokenAuthorityCostSample, scale: &str) {
    print_token_authority_cost_cut(sample, scale, "uncovered", &sample.uncovered);
    let maintenance = &sample.reconciliation.observation;
    eprintln!(
        "F6b7 token-index-decision {scale} profile_cycles={} cut=maintenance prior_selected_version={} next_selected_version={} elapsed_us={} token_reads={} token_writes={} table_read_bytes={} table_written_bytes={} manifest_reads={} manifest_writes={} manifest_read_bytes={} manifest_written_bytes={} adapter_ops={}",
        sample.profile_cycles,
        sample.reconciliation.prior_selected_version,
        sample.reconciliation.next_selected_version,
        maintenance.elapsed_us,
        maintenance.table.stream_token_reads,
        maintenance.table.stream_token_writes,
        maintenance.table.total_read_bytes,
        maintenance.table.total_written_bytes,
        maintenance.manifest.total_reads,
        maintenance.manifest.total_writes,
        maintenance.manifest.total_read_bytes,
        maintenance.manifest.total_written_bytes,
        maintenance.adapter.operations(),
    );
    print_token_authority_cost_cut(sample, scale, "reconciled", &sample.reconciled);
    for (term, before, after) in [
        (
            "warm_hit",
            &sample.uncovered.warm_hits,
            &sample.reconciled.warm_hits,
        ),
        (
            "warm_miss",
            &sample.uncovered.warm_misses,
            &sample.reconciled.warm_misses,
        ),
        (
            "warm_receipt_hit",
            &sample.uncovered.warm_receipt_hits,
            &sample.reconciled.warm_receipt_hits,
        ),
        (
            "warm_receipt_miss",
            &sample.uncovered.warm_receipt_misses,
            &sample.reconciled.warm_receipt_misses,
        ),
    ] {
        print_token_index_decision_term(sample, scale, term, before, after);
    }
}

async fn initialize_history(
    cluster_uri: &str,
    depth: u64,
    compact_before_enrollment: bool,
) -> String {
    let uri = helpers::stream_authority::graph_uri(cluster_uri);
    let db = Arc::new(Omnigraph::init(&uri, STREAM_SCHEMA).await.unwrap());
    for tick in 0..depth {
        db.mutate(
            "main",
            ADD_HISTORY,
            "add_history",
            &helpers::int_params(&[("$tick", tick as i64)]),
        )
        .await
        .unwrap();
    }
    if compact_before_enrollment {
        db.optimize()
            .await
            .expect("normalize internal/data fragments before the flat ack measurement");
    }
    db.failpoint_enroll_stream_table_for_test(TABLE)
        .await
        .unwrap();
    helpers::stream_authority::enable_stream_profile(&db, cluster_uri).await;
    uri
}

async fn init_enrolled(cluster_uri: &str, schema: &str) -> String {
    let uri = helpers::stream_authority::graph_uri(cluster_uri);
    let db = Arc::new(Omnigraph::init(&uri, schema).await.unwrap());
    db.failpoint_enroll_stream_table_for_test(TABLE)
        .await
        .unwrap();
    helpers::stream_authority::enable_stream_profile(&db, cluster_uri).await;
    uri
}

async fn table_schema(db: &Omnigraph) -> Arc<Schema> {
    let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let table = snapshot.open(TABLE).await.unwrap();
    Arc::new(Schema::from(table.schema()))
}

fn score_batch(schema: Arc<Schema>, id_prefix: &str, rows: usize) -> RecordBatch {
    assert_eq!(
        schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["id", "score"]
    );
    let ids = Arc::new(StringArray::from_iter_values(
        (0..rows).map(|row| format!("{id_prefix}-{row:05}")),
    )) as ArrayRef;
    let scores = Arc::new(Int32Array::from_iter_values(
        (0..rows).map(|row| i32::try_from(row).unwrap()),
    )) as ArrayRef;
    RecordBatch::try_new(schema, vec![ids, scores]).unwrap()
}

async fn payload_batch(db: &Omnigraph, rows: usize, payload_bytes: usize) -> RecordBatch {
    let schema = table_schema(db).await;
    assert_eq!(
        schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["id", "payload"]
    );
    let ids = Arc::new(StringArray::from_iter_values(
        (0..rows).map(|row| format!("rss-{row:05}")),
    )) as ArrayRef;
    let payload = "x".repeat(payload_bytes);
    let values = Arc::new(StringArray::from_iter_values(
        (0..rows).map(|_| payload.as_str()),
    )) as ArrayRef;
    RecordBatch::try_new(schema, vec![ids, values]).unwrap()
}

async fn history_sample_at_uri(
    cluster_uri: &str,
    depth: u64,
    compact_before_enrollment: bool,
) -> HistorySample {
    let uri = initialize_history(cluster_uri, depth, compact_before_enrollment).await;

    let table_tracker = IOTracker::default();
    let manifest_tracker = IOTracker::default();
    with_raw_io_trackers(&table_tracker, &manifest_tracker, async {
        let (adapter, adapter_counts) = CountingStorageAdapter::new(storage_for_uri(&uri).unwrap());
        let db = Arc::new(Omnigraph::open_with_storage(&uri, adapter).await.unwrap());
        let schema = table_schema(&db).await;
        let first = score_batch(Arc::clone(&schema), &format!("d{depth}-warmup"), 1);
        let measured = score_batch(schema, &format!("d{depth}-measured"), 1);

        // This first put performs the cold claim.  The tracker is deliberately
        // persistent across it and the measured put: the warm writer carries
        // the object-store wrapper it acquired when it was opened.
        db.failpoint_stream_b1_for_test(TABLE, Some(first), 0)
            .await
            .unwrap();
        let _ = table_tracker.incremental_stats();
        let _ = manifest_tracker.incremental_stats();
        let adapter_before = AdapterIo::snapshot(&adapter_counts);

        let ack_started = Instant::now();
        db.failpoint_stream_b1_for_test(TABLE, Some(measured), 1)
            .await
            .unwrap();
        let ack_elapsed_us = ack_started.elapsed().as_micros();
        let ack_table = PathIo::from_stats(&table_tracker.incremental_stats());
        let ack_manifest = PathIo::from_stats(&manifest_tracker.incremental_stats());
        let ack_adapter = AdapterIo::snapshot(&adapter_counts).since(adapter_before);

        let fold_started = Instant::now();
        db.failpoint_stream_b1_for_test(TABLE, None, 0)
            .await
            .unwrap();
        let fold_elapsed_us = fold_started.elapsed().as_micros();
        let fold_table = PathIo::from_stats(&table_tracker.incremental_stats());
        let fold_manifest = PathIo::from_stats(&manifest_tracker.incremental_stats());

        HistorySample {
            depth,
            ack_elapsed_us,
            ack_table,
            ack_manifest,
            ack_adapter,
            fold_elapsed_us,
            fold_table,
            fold_manifest,
        }
    })
    .await
}

async fn local_history_sample(depth: u64, compact: bool) -> HistorySample {
    let dir = tempfile::tempdir().unwrap();
    history_sample_at_uri(dir.path().to_str().unwrap(), depth, compact).await
}

fn assert_warm_ack_evidence(samples: &[HistorySample], backend: &str) {
    assert!(samples.len() >= 2);
    for sample in samples {
        eprintln!(
            "B1 warm ack {backend} depth={}: elapsed_us={} table_reads={} table_read_bytes={} table_writes={} table_written_bytes={} wal_writes={} manifest_reads={} adapter_ops={}",
            sample.depth,
            sample.ack_elapsed_us,
            sample.ack_table.total_reads,
            sample.ack_table.total_read_bytes,
            sample.ack_table.total_writes,
            sample.ack_table.total_written_bytes,
            sample.ack_table.wal_writes,
            sample.ack_manifest.total_reads,
            sample.ack_adapter.operations(),
        );
        assert!(
            sample.ack_table.wal_writes > 0,
            "warm ack recorded no WAL write; detached-task probe propagation regressed: {sample:?}"
        );
        assert_eq!(
            sample.ack_table.generation_writes, 0,
            "ack must not flush/roll a generation"
        );
        assert_eq!(
            sample.ack_manifest.total_writes, 0,
            "ack must not publish graph visibility"
        );
    }
    let shallow = &samples[0];
    let deep = &samples[samples.len() - 1];
    assert!(
        deep.ack_table.total_reads <= shallow.ack_table.total_reads + 2,
        "warm ack table reads grew with compacted graph history: {samples:#?}"
    );
    assert!(
        deep.ack_table.total_writes <= shallow.ack_table.total_writes + 2,
        "warm ack table writes grew with compacted graph history: {samples:#?}"
    );
    assert!(
        deep.ack_manifest.total_reads <= shallow.ack_manifest.total_reads + 4,
        "warm ack graph-authority reads grew with compacted graph history: {samples:#?}"
    );
    assert!(
        deep.ack_adapter.operations() <= shallow.ack_adapter.operations() + 2,
        "warm ack schema/sidecar adapter operations grew with graph history: {samples:#?}"
    );
}

#[tokio::test]
#[serial]
async fn warm_already_claimed_ack_is_flat_in_compacted_graph_history_local() {
    let mut samples = Vec::new();
    for depth in [8, 80] {
        samples.push(local_history_sample(depth, true).await);
    }
    assert_warm_ack_evidence(&samples, "local");
}

#[tokio::test]
#[serial]
async fn warm_already_claimed_ack_is_measured_on_configured_rustfs() {
    let Some(bucket) = std::env::var("OMNIGRAPH_S3_TEST_BUCKET").ok() else {
        eprintln!("SKIP: OMNIGRAPH_S3_TEST_BUCKET not set; RustFS B1 ack cost not measured");
        return;
    };

    let mut samples = Vec::new();
    for depth in [8, 80] {
        let uri = format!(
            "s3://{bucket}/cost-tests/memwal-b1-ack-{}-{depth}",
            ulid::Ulid::new()
        );
        let sample = history_sample_at_uri(&uri, depth, true).await;
        storage_for_uri(&uri)
            .unwrap()
            .delete_prefix(&uri)
            .await
            .expect("configured RustFS cleanup failed");
        samples.push(sample);
    }
    assert_warm_ack_evidence(&samples, "rustfs");
}

async fn cold_replay_sample(retained_batches: u64) -> ColdReplaySample {
    let dir = tempfile::tempdir().unwrap();
    let uri = init_enrolled(dir.path().to_str().unwrap(), STREAM_SCHEMA).await;

    let table_tracker = IOTracker::default();
    let manifest_tracker = IOTracker::default();
    with_raw_io_trackers(&table_tracker, &manifest_tracker, async {
        let (adapter, _) = CountingStorageAdapter::new(storage_for_uri(&uri).unwrap());
        let mut db = Arc::new(
            Omnigraph::open_with_storage(&uri, Arc::clone(&adapter))
                .await
                .unwrap(),
        );
        let schema = table_schema(&db).await;
        for ordinal in 0..retained_batches {
            let batch = score_batch(
                Arc::clone(&schema),
                &format!("cold-{retained_batches}-{ordinal}"),
                1,
            );
            if ordinal + 1 == retained_batches {
                let retired = helpers::failpoint::Rendezvous::park_first(
                    names::STREAM_B1_AFTER_RETIREMENT_RELEASE,
                );
                let error = {
                    let _after_durable =
                        ScopedFailPoint::new(names::STREAM_B1_AFTER_WATCHER_SUCCESS, "return");
                    db.failpoint_stream_b1_for_test(TABLE, Some(batch), ordinal)
                        .await
                        .expect_err("the final durable entry retires the warm writer")
                };
                assert!(matches!(error, OmniError::AckUnknown { .. }), "{error:?}");
                retired.wait_until_reached().await;
                retired.release();
            } else {
                db.failpoint_stream_b1_for_test(TABLE, Some(batch), ordinal)
                    .await
                    .unwrap();
            }
        }

        // Recovery-v14 can resolve durable claim/lifecycle authority while the
        // graph reopens. Reset before that reopen so the cold interval owns
        // every read needed to reach the claimed pre-seal worker.
        drop(db);
        let _ = table_tracker.incremental_stats();
        let _ = manifest_tracker.incremental_stats();

        let started = Instant::now();
        db = Arc::new(Omnigraph::open_with_storage(&uri, adapter).await.unwrap());
        let before_seal = Arc::new(helpers::failpoint::Rendezvous::park_first(
            names::STREAM_B1_BEFORE_FORCE_SEAL,
        ));
        let controller_rendezvous = Arc::clone(&before_seal);
        let controller_tracker = table_tracker.clone();
        let controller = tokio::spawn(async move {
            controller_rendezvous.wait_until_reached().await;
            let elapsed_us = started.elapsed().as_micros();
            let table = PathIo::from_stats(&controller_tracker.incremental_stats());
            controller_rendezvous.release();
            (elapsed_us, table)
        });

        // Continue the same claimed worker into its fold so the replay term
        // excludes both generation output and a second operational claim.
        let _ = manifest_tracker.incremental_stats();
        db.failpoint_stream_b1_for_test(TABLE, None, 0)
            .await
            .expect("cold replay fixture must close its claimed generation");
        let (elapsed_us, table) = controller
            .await
            .expect("cold replay controller task must remain joinable");
        ColdReplaySample {
            retained_batches,
            elapsed_us,
            table,
        }
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn cold_claim_replay_is_reported_separately_by_retained_wal_depth() {
    let _scenario = FailScenario::setup();
    let mut samples = Vec::new();
    for retained_batches in [1, 8, 32] {
        let sample = cold_replay_sample(retained_batches).await;
        eprintln!(
            "B1 cold replay retained_batches={}: elapsed_us={} wal_reads={} table_reads={} aggregate_table_read_bytes={} generation_writes={}",
            sample.retained_batches,
            sample.elapsed_us,
            sample.table.wal_reads,
            sample.table.total_reads,
            sample.table.total_read_bytes,
            sample.table.generation_writes,
        );
        assert!(
            sample.table.wal_reads > 0,
            "cold claim did not observe retained WAL: {sample:?}"
        );
        assert_eq!(
            sample.table.generation_writes, 0,
            "pre-seal cold replay measurement must exclude generation output"
        );
        samples.push(sample);
    }
    assert!(
        samples.last().unwrap().table.total_read_bytes
            > samples.first().unwrap().table.total_read_bytes,
        "aggregate cold claim/replay bytes must expose retained-WAL depth: {samples:#?}"
    );
}

async fn fold_rows_sample(rows: usize) -> FoldRowsSample {
    let dir = tempfile::tempdir().unwrap();
    let uri = init_enrolled(dir.path().to_str().unwrap(), STREAM_SCHEMA).await;
    let table_tracker = IOTracker::default();
    let manifest_tracker = IOTracker::default();
    with_raw_io_trackers(&table_tracker, &manifest_tracker, async {
        let db = Arc::new(Omnigraph::open(&uri).await.unwrap());
        let schema = table_schema(&db).await;
        let batch = score_batch(schema, &format!("fold-{rows}"), rows);
        db.failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
            .await
            .unwrap();
        let _ = table_tracker.incremental_stats();
        let _ = manifest_tracker.incremental_stats();

        let started = Instant::now();
        db.failpoint_stream_b1_for_test(TABLE, None, 0)
            .await
            .unwrap();
        let elapsed_us = started.elapsed().as_micros();
        let table = PathIo::from_stats(&table_tracker.incremental_stats());
        assert_eq!(
            helpers::count_rows(&db, TABLE).await,
            rows,
            "the measured fold must select exactly this generation's rows"
        );
        FoldRowsSample {
            rows,
            elapsed_us,
            table,
            physical_generation_data_bytes: generation_data_file_bytes(dir.path()),
        }
    })
    .await
}

#[tokio::test]
#[serial]
async fn one_generation_fold_data_scan_tracks_selected_rows() {
    let mut samples = Vec::new();
    for rows in [1, 4_096] {
        let sample = fold_rows_sample(rows).await;
        eprintln!(
            "B1 fold selected_rows={}: elapsed_us={} generation_data_reads={} generation_reads={} observed_range_read_bytes={} physical_generation_data_bytes={} pk_sidecar_reads={}",
            sample.rows,
            sample.elapsed_us,
            sample.table.generation_data_reads,
            sample.table.generation_reads,
            sample.table.total_read_bytes,
            sample.physical_generation_data_bytes,
            sample.table.pk_sidecar_reads,
        );
        assert!(
            sample.table.generation_data_reads > 0,
            "fold recorded no selected-generation data read; detached-task probe propagation regressed: {sample:?}"
        );
        samples.push(sample);
    }
    assert!(
        samples[1].physical_generation_data_bytes > samples[0].physical_generation_data_bytes,
        "the selected generation's physical data must expose row volume: {samples:#?}"
    );
}

#[tokio::test]
#[serial]
async fn fold_keeps_known_uncompacted_graph_manifest_history_term_visible() {
    let mut samples = Vec::new();
    for depth in [8, 80] {
        let sample = local_history_sample(depth, false).await;
        eprintln!(
            "B1 fold uncompacted_history={}: elapsed_us={} graph_manifest_reads={} graph_manifest_read_bytes={} generation_data_reads={}",
            sample.depth,
            sample.fold_elapsed_us,
            sample.fold_manifest.total_reads,
            sample.fold_manifest.total_read_bytes,
            sample.fold_table.generation_data_reads,
        );
        assert!(sample.fold_table.generation_data_reads > 0, "{sample:?}");
        samples.push(sample);
    }
    assert!(
        samples[1].fold_manifest.total_reads > samples[0].fold_manifest.total_reads
            || samples[1].fold_manifest.total_read_bytes
                > samples[0].fold_manifest.total_read_bytes,
        "the shared fold authority/publisher's known uncompacted graph-manifest term disappeared; do not replace it with a false history-flat claim: {samples:#?}"
    );
}

fn max_shard_manifest_bytes(root: &Path) -> u64 {
    fn walk(path: &Path, max_bytes: &mut u64) {
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                walk(&path, max_bytes);
                continue;
            }
            let is_shard_manifest = path.extension().is_some_and(|ext| ext == "binpb")
                && path
                    .components()
                    .any(|component| component.as_os_str() == "_mem_wal")
                && path
                    .components()
                    .any(|component| component.as_os_str() == "manifest");
            if is_shard_manifest {
                *max_bytes = (*max_bytes).max(entry.metadata().unwrap().len());
            }
        }
    }

    let mut max_bytes = 0;
    walk(root, &mut max_bytes);
    assert!(
        max_bytes > 0,
        "no local shard manifest found under {root:?}"
    );
    max_bytes
}

fn generation_data_file_bytes(root: &Path) -> u64 {
    fn walk(path: &Path, total: &mut u64) {
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                walk(&path, total);
                continue;
            }
            let in_generation = path
                .components()
                .any(|component| component.as_os_str().to_string_lossy().contains("_gen_"));
            let in_data = path
                .components()
                .any(|component| component.as_os_str() == "data");
            if in_generation && in_data {
                *total = total.saturating_add(entry.metadata().unwrap().len());
            }
        }
    }

    let mut total = 0;
    walk(root, &mut total);
    assert!(total > 0, "no flushed-generation data found under {root:?}");
    total
}

fn retention_term_sample(
    started: Instant,
    table_tracker: &IOTracker,
    manifest_tracker: &IOTracker,
    adapter_counts: &StorageReadCounts,
    adapter_before: AdapterIo,
    retained_roots: &BTreeSet<String>,
) -> RetentionTermSample {
    let elapsed_us = started.elapsed().as_micros();
    let table_stats = table_tracker.incremental_stats();
    let manifest_stats = manifest_tracker.incremental_stats();
    RetentionTermSample {
        elapsed_us,
        table: PathIo::from_stats(&table_stats),
        manifest: PathIo::from_stats(&manifest_stats),
        adapter: AdapterIo::snapshot(adapter_counts).since(adapter_before),
        retained_root_io: RetainedRootIo::from_stats(&table_stats, retained_roots),
    }
}

fn reset_retention_term_trackers(
    table_tracker: &IOTracker,
    manifest_tracker: &IOTracker,
    adapter_counts: &StorageReadCounts,
) -> AdapterIo {
    let _ = table_tracker.incremental_stats();
    let _ = manifest_tracker.incremental_stats();
    AdapterIo::snapshot(adapter_counts)
}

#[cfg(unix)]
fn current_process_peak_rss_bytes() -> Option<u64> {
    let mut rusage: libc::rusage = unsafe { std::mem::zeroed() };
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut rusage) };
    assert_eq!(result, 0, "getrusage(RUSAGE_SELF) failed");
    Some(normalized_peak_rss_bytes(&rusage))
}

#[cfg(not(unix))]
fn current_process_peak_rss_bytes() -> Option<u64> {
    None
}

async fn retained_history_scaling_samples_at(
    cluster_uri: &str,
    depths: &[u64],
) -> Vec<RetentionDepthSample> {
    assert!(!depths.is_empty());
    assert!(
        depths
            .windows(2)
            .all(|pair| pair[0] > 0 && pair[0] < pair[1])
            && depths.last().copied().unwrap() > 0,
        "retained-history sweep depths must be positive and strictly increasing"
    );

    let uri = init_enrolled(cluster_uri, STREAM_SCHEMA).await;
    let table_tracker = IOTracker::default();
    let manifest_tracker = IOTracker::default();

    with_raw_io_trackers(&table_tracker, &manifest_tracker, async {
        let (adapter, adapter_counts) = CountingStorageAdapter::new(storage_for_uri(&uri).unwrap());
        let mut db = Arc::new(
            Omnigraph::open_with_storage(&uri, Arc::clone(&adapter))
                .await
                .unwrap(),
        );
        let schema = table_schema(&db).await;
        let mut retained_generations = 0u64;
        let mut visible_rows = 0usize;
        let mut ordinal = 0u64;
        let mut samples = Vec::new();

        for &depth in depths {
            while retained_generations < depth {
                let batch = score_batch(
                    Arc::clone(&schema),
                    &format!("retention-fill-{retained_generations}"),
                    1,
                );
                db.failpoint_stream_b1_for_test(TABLE, Some(batch), ordinal)
                    .await
                    .unwrap();
                ordinal += 1;
                db.failpoint_stream_b1_for_test(TABLE, None, 0)
                    .await
                    .unwrap();
                retained_generations += 1;
                visible_rows += 1;
            }

            let before = current_mem_wal_inventory(&uri).await;
            assert_eq!(
                before.generation_roots.len(),
                usize::try_from(depth).unwrap(),
                "fixture must reach the requested retained-generation depth exactly"
            );
            let retained_roots = before.generation_roots.clone();

            // Establish one resident writer, then measure only the second put.
            // This keeps warm acknowledgement separate from cold claim/replay.
            let warmup = score_batch(Arc::clone(&schema), &format!("retention-{depth}-warmup"), 1);
            db.failpoint_stream_b1_for_test(TABLE, Some(warmup), ordinal)
                .await
                .unwrap();
            ordinal += 1;
            let before_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
            let manifest_version_before = before_snapshot.version();
            let table_version_before = before_snapshot.entry(TABLE).unwrap().table_version;
            let adapter_before =
                reset_retention_term_trackers(&table_tracker, &manifest_tracker, &adapter_counts);
            let started = Instant::now();
            let measured = score_batch(
                Arc::clone(&schema),
                &format!("retention-{depth}-measured"),
                1,
            );
            db.failpoint_stream_b1_for_test(TABLE, Some(measured), ordinal)
                .await
                .unwrap();
            ordinal += 1;
            let warm_ack = retention_term_sample(
                started,
                &table_tracker,
                &manifest_tracker,
                &adapter_counts,
                adapter_before,
                &retained_roots,
            );
            let after_ack = current_mem_wal_inventory(&uri).await;
            let after_ack_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
            let manifest_version_after_ack = after_ack_snapshot.version();
            let table_version_after_ack = after_ack_snapshot.entry(TABLE).unwrap().table_version;

            // Add one definitely durable but acknowledgement-ambiguous entry to
            // retire the resident writer.  The measured cold interval then
            // includes graph reopen, shard claim, and authoritative WAL replay,
            // while stopping before generation materialization.
            let ambiguous = score_batch(
                Arc::clone(&schema),
                &format!("retention-{depth}-ambiguous"),
                1,
            );
            let ambiguous_retired = helpers::failpoint::Rendezvous::park_first(
                names::STREAM_B1_AFTER_RETIREMENT_RELEASE,
            );
            let error = {
                let _after_durable =
                    ScopedFailPoint::new(names::STREAM_B1_AFTER_WATCHER_SUCCESS, "return");
                db.failpoint_stream_b1_for_test(TABLE, Some(ambiguous), ordinal)
                    .await
                    .expect_err("the injected post-durability boundary must retire the writer")
            };
            assert!(matches!(error, OmniError::AckUnknown { .. }), "{error:?}");
            ordinal += 1;
            ambiguous_retired.wait_until_reached().await;
            ambiguous_retired.release();
            drop(db);

            let adapter_before =
                reset_retention_term_trackers(&table_tracker, &manifest_tracker, &adapter_counts);
            let started = Instant::now();
            db = Arc::new(
                Omnigraph::open_with_storage(&uri, Arc::clone(&adapter))
                    .await
                    .unwrap(),
            );
            let before_seal = Arc::new(helpers::failpoint::Rendezvous::park_first(
                names::STREAM_B1_BEFORE_FORCE_SEAL,
            ));
            let controller_rendezvous = Arc::clone(&before_seal);
            let controller_table = table_tracker.clone();
            let controller_manifest = manifest_tracker.clone();
            let controller_counts = Arc::clone(&adapter_counts);
            let controller_roots = retained_roots.clone();
            let controller_db = Arc::clone(&db);
            let controller_uri = uri.clone();
            let controller = tokio::spawn(async move {
                controller_rendezvous.wait_until_reached().await;
                let cold_reopen_replay = retention_term_sample(
                    started,
                    &controller_table,
                    &controller_manifest,
                    &controller_counts,
                    adapter_before,
                    &controller_roots,
                );
                let after_replay = current_mem_wal_inventory(&controller_uri).await;
                let after_replay_snapshot = controller_db
                    .snapshot_of(ReadTarget::branch("main"))
                    .await
                    .unwrap();
                let manifest_version_after_replay = after_replay_snapshot.version();
                let table_version_after_replay =
                    after_replay_snapshot.entry(TABLE).unwrap().table_version;
                let fold_adapter_before = reset_retention_term_trackers(
                    &controller_table,
                    &controller_manifest,
                    &controller_counts,
                );
                let fold_started = Instant::now();
                controller_rendezvous.release();
                (
                    cold_reopen_replay,
                    after_replay,
                    manifest_version_after_replay,
                    table_version_after_replay,
                    fold_adapter_before,
                    fold_started,
                )
            });
            db.failpoint_stream_b1_for_test(TABLE, None, 0)
                .await
                .expect("the replayed generation must fold and publish");
            let (
                cold_reopen_replay,
                after_replay,
                manifest_version_after_replay,
                table_version_after_replay,
                adapter_before,
                started,
            ) = controller
                .await
                .expect("retained-history controller task must remain joinable");
            let fold = retention_term_sample(
                started,
                &table_tracker,
                &manifest_tracker,
                &adapter_counts,
                adapter_before,
                &retained_roots,
            );

            let adapter_before =
                reset_retention_term_trackers(&table_tracker, &manifest_tracker, &adapter_counts);
            let started = Instant::now();
            let after_fold_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
            let visibility_probe = retention_term_sample(
                started,
                &table_tracker,
                &manifest_tracker,
                &adapter_counts,
                adapter_before,
                &retained_roots,
            );
            let manifest_version_after_fold = after_fold_snapshot.version();
            let table_version_after_fold = after_fold_snapshot.entry(TABLE).unwrap().table_version;
            let after_fold = current_mem_wal_inventory(&uri).await;
            assert!(
                current_recovery_sidecars(&uri).await.is_empty(),
                "successful retained-history fold must retire its recovery sidecar"
            );

            retained_generations += 1;
            visible_rows += 3;
            let visible_rows_after_fold = helpers::count_rows(&db, TABLE).await;
            samples.push(RetentionDepthSample {
                retained_generations_before: depth,
                retained_generations_after: retained_generations,
                before,
                after_ack,
                after_replay,
                after_fold,
                warm_ack,
                cold_reopen_replay,
                fold,
                visibility_probe,
                manifest_version_before,
                manifest_version_after_ack,
                manifest_version_after_replay,
                manifest_version_after_fold,
                table_version_before,
                table_version_after_ack,
                table_version_after_replay,
                table_version_after_fold,
                visible_rows_after_fold,
                whole_process_peak_rss_bytes: current_process_peak_rss_bytes(),
            });
            assert_eq!(visible_rows_after_fold, visible_rows);
        }
        samples
    })
    .await
}

async fn retained_history_scaling_samples_local(depths: &[u64]) -> Vec<RetentionDepthSample> {
    let dir = tempfile::tempdir().unwrap();
    // Heap-allocate the sweep future. Each depth drives the full enrollment,
    // acknowledgement, cold-replay, and fold stack; in a debug build those
    // nested async frames accumulate into the caller's frame and overflow a
    // default test-thread stack. Same `Box::pin` remedy `helpers::cost::
    // cost_harness` documents for whole-test-body futures.
    Box::pin(retained_history_scaling_samples_at(
        dir.path().to_str().unwrap(),
        depths,
    ))
    .await
}

fn assert_retained_history_scaling_sample(sample: &RetentionDepthSample) {
    let before_depth = usize::try_from(sample.retained_generations_before).unwrap();
    let after_depth = usize::try_from(sample.retained_generations_after).unwrap();
    assert_eq!(after_depth, before_depth + 1);
    for inventory in [
        &sample.before,
        &sample.after_ack,
        &sample.after_replay,
        &sample.after_fold,
    ] {
        assert!(
            inventory.unknown_paths().is_empty(),
            "retained-history census missed paths: {:?}",
            inventory.unknown_paths()
        );
        assert_eq!(
            inventory.generation_roots, inventory.referenced_generation_roots,
            "success-only retained-history sweep must not adopt or invent orphan roots"
        );
    }
    assert_eq!(sample.before.generation_roots.len(), before_depth);
    assert_eq!(sample.after_ack.generation_roots.len(), before_depth);
    assert_eq!(sample.after_replay.generation_roots.len(), before_depth);
    assert_eq!(sample.after_fold.generation_roots.len(), after_depth);
    sample
        .before
        .assert_path_class_size_retained_by(&sample.after_ack);
    sample
        .after_ack
        .assert_path_class_size_retained_by(&sample.after_replay);
    sample
        .after_replay
        .assert_path_class_size_retained_by(&sample.after_fold);
    assert!(
        sample.after_fold.immutable_object_bytes() > sample.before.immutable_object_bytes(),
        "one additional retained generation must increase advisory current-listed bytes"
    );

    for (name, term) in [
        ("warm ack", &sample.warm_ack),
        ("cold reopen/replay", &sample.cold_reopen_replay),
        ("fold", &sample.fold),
        ("visibility probe", &sample.visibility_probe),
    ] {
        assert_eq!(
            term.retained_root_io,
            RetainedRootIo::default(),
            "{name} touched an older retained generation root: {term:#?}"
        );
        assert_eq!(
            term.table.memwal_canonical_delete_requests, 0,
            "{name} requested deletion of a canonical MemWAL object: {term:#?}"
        );
    }
    assert!(sample.warm_ack.table.wal_writes > 0, "{sample:#?}");
    assert!(sample.warm_ack.table.memwal_writes > 0, "{sample:#?}");
    assert_eq!(sample.warm_ack.table.base_table_writes, 0, "{sample:#?}");
    assert_eq!(sample.warm_ack.table.stream_token_writes, 0, "{sample:#?}");
    assert_eq!(sample.warm_ack.table.other_reads, 0, "{sample:#?}");
    assert_eq!(sample.warm_ack.table.other_writes, 0, "{sample:#?}");
    assert_eq!(sample.warm_ack.table.generation_writes, 0, "{sample:#?}");
    assert_eq!(sample.warm_ack.manifest.total_writes, 0, "{sample:#?}");
    assert!(sample.cold_reopen_replay.table.wal_reads > 0, "{sample:#?}");
    assert_eq!(
        sample.cold_reopen_replay.table.generation_writes, 0,
        "{sample:#?}"
    );
    assert!(sample.fold.table.generation_reads > 0, "{sample:#?}");
    assert!(sample.fold.table.generation_writes > 0, "{sample:#?}");
    assert!(sample.fold.manifest.total_writes > 0, "{sample:#?}");
    assert_eq!(sample.visibility_probe.table.total_writes, 0, "{sample:#?}");
    assert_eq!(
        sample.visibility_probe.manifest.total_writes, 0,
        "{sample:#?}"
    );

    assert_eq!(
        sample.manifest_version_after_ack, sample.manifest_version_before,
        "acknowledgement must remain graph-invisible"
    );
    assert_eq!(
        sample.manifest_version_after_replay,
        sample.manifest_version_before + 1,
        "the cold claim must publish exactly one operational ClaimReceipt"
    );
    assert_eq!(
        sample.manifest_version_after_fold,
        sample.manifest_version_before + 2,
        "the same claimed worker must add exactly one fold visibility point"
    );
    assert_eq!(
        sample.table_version_after_ack, sample.table_version_before,
        "acknowledgement must not advance the base table"
    );
    assert_eq!(
        sample.table_version_after_replay, sample.table_version_before,
        "cold replay must not advance the base table"
    );
    assert_eq!(
        sample.table_version_after_fold,
        sample.table_version_before + 1,
        "one fold must commit exactly one Lance table effect"
    );
}

fn print_retained_history_scaling_sample(sample: &RetentionDepthSample, scale: &str) {
    let print_term = |name: &str, term: &RetentionTermSample| {
        eprintln!(
            "B2a retained-history {scale} depth={} term={name} elapsed_us={} table_reads={} table_read_bytes={} table_writes={} table_written_bytes={} memwal_reads={} memwal_writes={} base_table_reads={} base_table_writes={} stream_token_reads={} stream_token_writes={} other_reads={} other_writes={} graph_manifest_reads={} graph_manifest_read_bytes={} graph_manifest_writes={} graph_manifest_written_bytes={} adapter_ops={} retained_root_reads={} retained_root_writes={} retained_root_deletes={} memwal_atomic_temp_delete_requests={} memwal_canonical_delete_requests={}",
            sample.retained_generations_before,
            term.elapsed_us,
            term.table.total_reads,
            term.table.total_read_bytes,
            term.table.total_writes,
            term.table.total_written_bytes,
            term.table.memwal_reads,
            term.table.memwal_writes,
            term.table.base_table_reads,
            term.table.base_table_writes,
            term.table.stream_token_reads,
            term.table.stream_token_writes,
            term.table.other_reads,
            term.table.other_writes,
            term.manifest.total_reads,
            term.manifest.total_read_bytes,
            term.manifest.total_writes,
            term.manifest.total_written_bytes,
            term.adapter.operations(),
            term.retained_root_io.reads,
            term.retained_root_io.writes,
            term.retained_root_io.deletes,
            term.table.memwal_atomic_temp_delete_requests,
            term.table.memwal_canonical_delete_requests,
        );
    };
    print_term("warm_ack", &sample.warm_ack);
    print_term("cold_reopen_replay", &sample.cold_reopen_replay);
    print_term("fold", &sample.fold);
    print_term("visibility_probe", &sample.visibility_probe);
    eprintln!(
        "B2a retained-history {scale} depth={} advisory_current_listed_before_objects={} advisory_current_listed_before_immutable_bytes={} advisory_current_listed_after_ack_objects={} advisory_current_listed_after_ack_immutable_bytes={} advisory_current_listed_after_replay_objects={} advisory_current_listed_after_replay_immutable_bytes={} advisory_current_listed_after_fold_objects={} advisory_current_listed_after_fold_immutable_bytes={} retained_roots_before={} retained_roots_after={} visible_rows={} whole_process_peak_rss_bytes={:?}",
        sample.retained_generations_before,
        sample.before.objects.len(),
        sample.before.immutable_object_bytes(),
        sample.after_ack.objects.len(),
        sample.after_ack.immutable_object_bytes(),
        sample.after_replay.objects.len(),
        sample.after_replay.immutable_object_bytes(),
        sample.after_fold.objects.len(),
        sample.after_fold.immutable_object_bytes(),
        sample.before.generation_roots.len(),
        sample.after_fold.generation_roots.len(),
        sample.visible_rows_after_fold,
        sample.whole_process_peak_rss_bytes,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn b2a_retained_history_small_depth_keeps_terms_and_old_roots_separate() {
    let _scenario = FailScenario::setup();
    let samples = retained_history_scaling_samples_local(&[1, 8]).await;
    for sample in &samples {
        print_retained_history_scaling_sample(sample, "ci");
        assert_retained_history_scaling_sample(sample);
    }

    let shallow = &samples[0];
    let deep = &samples[1];
    assert_eq!(
        deep.warm_ack.table.memwal_reads, shallow.warm_ack.table.memwal_reads,
        "warm acknowledgement MemWAL read-operation count must not scale with retained history"
    );
    assert_eq!(
        deep.warm_ack.table.memwal_writes, shallow.warm_ack.table.memwal_writes,
        "warm acknowledgement MemWAL write-operation count must not scale with retained history"
    );
    assert_eq!(
        deep.cold_reopen_replay.table.wal_reads, shallow.cold_reopen_replay.table.wal_reads,
        "the fixed three-entry replay tail must keep one deterministic WAL-read shape"
    );
    assert!(
        deep.cold_reopen_replay.table.shard_manifest_reads
            <= shallow.cold_reopen_replay.table.shard_manifest_reads + 2,
        "cold shard-authority read-operation count grew with retained history: {samples:#?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
#[ignore = "on-demand RFC-026 B2a retain-all decision-scale instrument"]
async fn b2a_retained_history_decision_scale_sweeps_to_128_generations() {
    let _scenario = FailScenario::setup();
    let samples = retained_history_scaling_samples_local(&[1, 8, 32, 128]).await;
    for sample in &samples {
        print_retained_history_scaling_sample(sample, "decision");
        assert_retained_history_scaling_sample(sample);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
#[ignore = "on-demand RFC-026 B2a configured-object-store decision-scale instrument"]
async fn b2a_retained_history_decision_scale_sweeps_to_128_generations_on_configured_rustfs() {
    let _scenario = FailScenario::setup();
    let Some(uri) = helpers::s3_test_graph_uri("memwal-b2a-retained-history") else {
        eprintln!(
            "SKIP b2a_retained_history_decision_scale_sweeps_to_128_generations_on_configured_rustfs"
        );
        return;
    };
    let samples = Box::pin(retained_history_scaling_samples_at(&uri, &[1, 8, 32, 128])).await;
    for sample in &samples {
        print_retained_history_scaling_sample(sample, "rustfs-decision");
        assert_retained_history_scaling_sample(sample);
    }

    // This destroys the benchmark's unique isolated graph root only after all
    // retention assertions. It is fixture teardown, not production MemWAL GC.
    let (store, root) = lance_io::object_store::ObjectStore::from_uri(&uri)
        .await
        .expect("configured S3/RustFS benchmark URI must resolve for cleanup");
    store
        .remove_dir_all(root)
        .await
        .expect("configured S3/RustFS benchmark fixture cleanup must succeed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn f6b7_selected_token_reconciliation_cost_is_measured_at_small_depth() {
    let _scenario = FailScenario::setup();
    let mut previous = None;
    for depth in [1, 8] {
        let dir = tempfile::tempdir().unwrap();
        let sample = token_authority_cost_sample_at(dir.path().to_str().unwrap(), depth).await;
        print_token_authority_cost_sample(&sample, "ci");
        assert_token_authority_cost_sample(&sample);
        if let Some(shallow) = previous.as_ref() {
            assert_token_authority_cost_growth(shallow, &sample);
        }
        previous = Some(sample);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
#[ignore = "on-demand RFC-026 F6b7 selected-token reconciliation decision instrument"]
async fn f6b7_selected_token_reconciliation_cost_sweeps_to_128_profile_cycles() {
    let _scenario = FailScenario::setup();
    let mut previous = None;
    for depth in [1, 8, 32, 128] {
        let dir = tempfile::tempdir().unwrap();
        let sample = token_authority_cost_sample_at(dir.path().to_str().unwrap(), depth).await;
        print_token_authority_cost_sample(&sample, "decision");
        assert_token_authority_cost_sample(&sample);
        if let Some(shallow) = previous.as_ref() {
            assert_token_authority_cost_growth(shallow, &sample);
        }
        previous = Some(sample);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
#[ignore = "on-demand RFC-026 F6b7 configured-object-store reconciliation decision instrument"]
async fn f6b7_selected_token_reconciliation_cost_sweeps_on_configured_object_store() {
    let _scenario = FailScenario::setup();
    let enforce_rustfs_decision = match std::env::var(F6B7_DECISION_BACKEND_ENV) {
        Ok(value) if value == "rustfs" => true,
        Ok(value) => panic!("{F6B7_DECISION_BACKEND_ENV} must be 'rustfs' when set, got '{value}'"),
        Err(std::env::VarError::NotPresent) => false,
        Err(error) => panic!("failed to read {F6B7_DECISION_BACKEND_ENV}: {error}"),
    };
    let scale = if enforce_rustfs_decision {
        "rustfs-decision"
    } else {
        "configured-object-store-advisory"
    };
    let Some(uri) = helpers::s3_test_graph_uri("memwal-f6b7-token-authority") else {
        assert!(
            !enforce_rustfs_decision,
            "{F6B7_DECISION_BACKEND_ENV}=rustfs requires a configured S3/RustFS bucket"
        );
        eprintln!("SKIP f6b7_selected_token_reconciliation_cost_sweeps_on_configured_object_store");
        return;
    };
    let mut samples = Vec::new();
    for depth in [1, 8, 32, 128] {
        let sample = Box::pin(token_authority_cost_sample_at(
            &format!("{}/cycles-{depth}", uri.trim_end_matches('/')),
            depth,
        ))
        .await;
        print_token_authority_cost_sample(&sample, scale);
        assert_token_authority_cost_sample(&sample);
        if let Some(shallow) = samples.last() {
            assert_token_authority_cost_growth(shallow, &sample);
        }
        samples.push(sample);
    }
    if enforce_rustfs_decision {
        assert_configured_rustfs_token_index_uncompacted_bounded_no_go(&samples);
    } else {
        eprintln!(
            "F6b7 token-index-decision configured-object-store outcome=advisory decision_assertion=disabled set_{F6B7_DECISION_BACKEND_ENV}=rustfs_for_recorded_backend"
        );
    }

    let (store, root) = lance_io::object_store::ObjectStore::from_uri(&uri)
        .await
        .expect("configured S3/RustFS F6b7 fixture URI must resolve for cleanup");
    store
        .remove_dir_all(root)
        .await
        .expect("configured S3/RustFS F6b7 fixture cleanup must succeed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn gate_r0_retain_all_current_object_growth_is_swept_explicitly() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = init_enrolled(dir.path().to_str().unwrap(), STREAM_SCHEMA).await;
    let table_tracker = IOTracker::default();
    let manifest_tracker = IOTracker::default();

    let samples = with_raw_io_trackers(&table_tracker, &manifest_tracker, async {
        let db = Arc::new(Omnigraph::open(&uri).await.unwrap());
        let schema = table_schema(&db).await;
        let mut completed = 0u64;
        let mut ordinal = 0u64;
        let mut samples = Vec::new();

        for checkpoint in [1u64, 4, 8] {
            while completed < checkpoint {
                let batch = score_batch(
                    Arc::clone(&schema),
                    &format!("metadata-normal-{completed}"),
                    1,
                );
                db.failpoint_stream_b1_for_test(TABLE, Some(batch), ordinal)
                    .await
                    .unwrap();
                ordinal += 1;
                db.failpoint_stream_b1_for_test(TABLE, None, 0)
                    .await
                    .unwrap();
                completed += 1;
            }

            let serialized_manifest_bytes = max_shard_manifest_bytes(dir.path());
            let inventory = current_mem_wal_inventory(&uri).await;
            let pending = score_batch(
                Arc::clone(&schema),
                &format!("metadata-measured-{checkpoint}"),
                1,
            );
            let retired = helpers::failpoint::Rendezvous::park_first(
                names::STREAM_B1_AFTER_RETIREMENT_RELEASE,
            );
            let error = {
                let _after_durable =
                    ScopedFailPoint::new(names::STREAM_B1_AFTER_WATCHER_SUCCESS, "return");
                db.failpoint_stream_b1_for_test(TABLE, Some(pending), ordinal)
                    .await
                    .expect_err("retire the writer so the metadata probe is a cold claim")
            };
            assert!(matches!(error, OmniError::AckUnknown { .. }), "{error:?}");
            ordinal += 1;
            retired.wait_until_reached().await;
            retired.release();
            let _ = table_tracker.incremental_stats();
            let _ = manifest_tracker.incremental_stats();

            let started = Instant::now();
            let before_seal = Arc::new(helpers::failpoint::Rendezvous::park_first(
                names::STREAM_B1_BEFORE_FORCE_SEAL,
            ));
            let controller_rendezvous = Arc::clone(&before_seal);
            let controller_table = table_tracker.clone();
            let controller_manifest = manifest_tracker.clone();
            let controller = tokio::spawn(async move {
                controller_rendezvous.wait_until_reached().await;
                let elapsed_us = started.elapsed().as_micros();
                let table = PathIo::from_stats(&controller_table.incremental_stats());
                let _ = controller_manifest.incremental_stats();
                controller_rendezvous.release();
                (elapsed_us, table)
            });
            db.failpoint_stream_b1_for_test(TABLE, None, 0)
                .await
                .expect("retained-metadata fixture must close its claimed generation");
            let (cold_elapsed_us, table) = controller
                .await
                .expect("retained-metadata controller task must remain joinable");
            samples.push(RetainedMetadataSample {
                merged_generations: checkpoint,
                serialized_manifest_bytes,
                cold_elapsed_us,
                table,
                inventory,
            });

            completed += 1;
        }
        samples
    })
    .await;

    for (index, sample) in samples.iter().enumerate() {
        eprintln!(
            "B1 retained merged_generations={}: largest_retained_manifest_bytes={} current_listed_immutable_bytes={} current_listed_objects={} generation_roots={} manifest_version_objects={} cold_elapsed_us={} shard_manifest_reads={} aggregate_table_read_bytes={}",
            sample.merged_generations,
            sample.serialized_manifest_bytes,
            sample.inventory.immutable_object_bytes(),
            sample.inventory.objects.len(),
            sample.inventory.generation_roots.len(),
            sample
                .inventory
                .object_count(MemWalObjectKind::ShardManifestVersion),
            sample.cold_elapsed_us,
            sample.table.shard_manifest_reads,
            sample.table.total_read_bytes,
        );
        assert!(sample.table.shard_manifest_reads > 0, "{sample:?}");
        assert_eq!(
            sample.inventory.generation_roots.len(),
            usize::try_from(sample.merged_generations).unwrap(),
            "one no-roll B1 fold must retain one generation root"
        );
        assert_eq!(
            sample.inventory.generation_roots, sample.inventory.referenced_generation_roots,
            "the success-only sweep must not confuse referenced generations with crash orphans"
        );
        assert!(
            sample.inventory.unknown_paths().is_empty(),
            "current-object census missed paths: {:?}",
            sample.inventory.unknown_paths()
        );
        if index > 0 {
            samples[index - 1]
                .inventory
                .assert_path_class_size_retained_by(&sample.inventory);
        }
    }
    assert!(
        samples.last().unwrap().serialized_manifest_bytes
            > samples.first().unwrap().serialized_manifest_bytes,
        "B1 intentionally retains merged generation entries; the serialized shard metadata must expose that growth: {samples:#?}"
    );
    assert!(
        samples.last().unwrap().inventory.immutable_object_bytes()
            > samples.first().unwrap().inventory.immutable_object_bytes(),
        "retain-all physical objects must expose cumulative growth: {samples:#?}"
    );
    assert!(
        samples
            .last()
            .unwrap()
            .inventory
            .object_count(MemWalObjectKind::ShardManifestVersion)
            > samples
                .first()
                .unwrap()
                .inventory
                .object_count(MemWalObjectKind::ShardManifestVersion),
        "versioned shard authority is retained growth too: {samples:#?}"
    );
}

fn classify_test_path(path: &str) -> MemWalObjectKind {
    let components = path.split('/').collect::<Vec<_>>();
    let mem_wal_index = components
        .iter()
        .position(|component| *component == "_mem_wal")
        .expect("test path must contain _mem_wal");
    classify_mem_wal_object(&components, mem_wal_index).0
}

fn bit_reversed_stem(value: u64) -> String {
    format!("{:064b}", value.reverse_bits())
}

fn gate_r0_manifest(
    shard_id: ShardId,
    flushed_generations: Vec<FlushedGeneration>,
) -> ShardManifest {
    ShardManifest {
        shard_id,
        version: 1,
        shard_spec_id: 0,
        shard_field_values: Default::default(),
        writer_epoch: 1,
        replay_after_wal_entry_position: 0,
        wal_entry_position_last_seen: 0,
        current_generation: flushed_generations
            .iter()
            .map(|generation| generation.generation)
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .expect("Gate R0 fixture generation must leave successor authority"),
        flushed_generations,
        status: ShardStatus::Active,
    }
}

#[test]
fn gate_r0_current_inventory_path_classifier_is_canonical_and_fail_closed() {
    let shard = "abcdefab-cdef-abcd-efab-cdefabcdefab";
    let bits = bit_reversed_stem(1);
    assert_eq!(
        classify_test_path(&format!("table/_mem_wal/{shard}/wal/{bits}.arrow")),
        MemWalObjectKind::Wal
    );
    assert_eq!(
        classify_test_path(&format!("table/_mem_wal/{shard}/manifest/{bits}.binpb")),
        MemWalObjectKind::ShardManifestVersion
    );
    assert_eq!(
        classify_test_path(&format!(
            "table/_mem_wal/{shard}/abcdef12_gen_1/data/file.lance"
        )),
        MemWalObjectKind::GenerationData
    );

    let malformed = [
        format!("table/_mem_wal/{shard}/wal/{}.arrow", "0".repeat(63)),
        format!("table/_mem_wal/{shard}/wal/{}.arrow", bit_reversed_stem(0)),
        format!("table/_mem_wal/{shard}/wal/{bits}.arrow.extra"),
        format!("table/_mem_wal/{shard}/manifest/not-a-version.binpb"),
        format!(
            "table/_mem_wal/{}/wal/{bits}.arrow",
            shard.to_ascii_uppercase()
        ),
        format!("table/_mem_wal/{shard}/ABCDEF12_gen_1/data/file.lance"),
        format!("table/_mem_wal/{shard}/abcdef12_gen_0/data/file.lance"),
        format!("table/_mem_wal/{shard}/abcdef12_gen_01/data/file.lance"),
    ];
    for path in malformed {
        assert_eq!(
            classify_test_path(&path),
            MemWalObjectKind::Unknown,
            "malformed path must fail closed: {path}"
        );
    }

    let retained_root = format!("table/_mem_wal/{shard}/abcdef12_gen_1");
    let retained_roots = BTreeSet::from([retained_root.clone()]);
    assert!(path_is_under_any_root(&retained_root, &retained_roots));
    assert!(path_is_under_any_root(
        &format!("{retained_root}/data/file.lance"),
        &retained_roots
    ));
    assert!(!path_is_under_any_root(
        &format!("{retained_root}-other/data/file.lance"),
        &retained_roots
    ));
    assert!(is_lance_atomic_memwal_temp_path(&format!(
        "table/_mem_wal/{shard}/manifest/{bits}.binpb.tmp.01234567-89ab-cdef-0123-456789abcdef"
    )));
    assert!(!is_lance_atomic_memwal_temp_path(&format!(
        "table/_mem_wal/{shard}/manifest/{bits}.binpb"
    )));
    assert!(!is_lance_atomic_memwal_temp_path(&format!(
        "table/_mem_wal/{shard}/wal/{bits}.arrow.tmp.01234567-89ab-cdef-0123-456789abcdef"
    )));
    assert!(!is_lance_atomic_memwal_temp_path(&format!(
        "table/_mem_wal/{shard}/manifest/{bits}.binpb.tmp.NOT-A-UUID"
    )));
}

#[test]
#[should_panic(expected = "decoded shard authority disagrees with its directory")]
fn gate_r0_current_inventory_refuses_manifest_for_another_shard() {
    let directory_shard = ShardId::parse_str("abcdefab-cdef-abcd-efab-cdefabcdefab").unwrap();
    let foreign_shard = ShardId::parse_str("01234567-89ab-cdef-0123-456789abcdef").unwrap();
    let manifest = gate_r0_manifest(foreign_shard, Vec::new());
    let _ = validated_referenced_generation_roots(
        "table",
        directory_shard,
        &manifest,
        &BTreeSet::new(),
        &BTreeSet::from([1]),
    );
}

#[test]
#[should_panic(expected = "zero or duplicate flushed generation")]
fn gate_r0_current_inventory_refuses_duplicate_generation_authority() {
    let shard = ShardId::parse_str("abcdefab-cdef-abcd-efab-cdefabcdefab").unwrap();
    let generation = FlushedGeneration {
        generation: 1,
        path: "abcdef12_gen_1".to_string(),
    };
    let manifest = gate_r0_manifest(shard, vec![generation.clone(), generation]);
    let listed = BTreeSet::from([format!(
        "table/_mem_wal/{}/abcdef12_gen_1",
        shard.as_hyphenated()
    )]);
    let _ = validated_referenced_generation_roots(
        "table",
        shard,
        &manifest,
        &listed,
        &BTreeSet::from([1]),
    );
}

#[test]
#[should_panic(expected = "zero next-generation allocation authority")]
fn gate_r0_current_inventory_refuses_zero_current_generation() {
    let shard = ShardId::parse_str("abcdefab-cdef-abcd-efab-cdefabcdefab").unwrap();
    let mut manifest = gate_r0_manifest(shard, Vec::new());
    manifest.current_generation = 0;
    let _ = validated_referenced_generation_roots(
        "table",
        shard,
        &manifest,
        &BTreeSet::new(),
        &BTreeSet::from([1]),
    );
}

#[test]
#[should_panic(expected = "allocation authority has not advanced past a flushed generation")]
fn gate_r0_current_inventory_refuses_non_advanced_current_generation() {
    let shard = ShardId::parse_str("abcdefab-cdef-abcd-efab-cdefabcdefab").unwrap();
    let generation = FlushedGeneration {
        generation: 1,
        path: "abcdef12_gen_1".to_string(),
    };
    let mut manifest = gate_r0_manifest(shard, vec![generation]);
    manifest.current_generation = 1;
    let listed = BTreeSet::from([format!(
        "table/_mem_wal/{}/abcdef12_gen_1",
        shard.as_hyphenated()
    )]);
    let _ = validated_referenced_generation_roots(
        "table",
        shard,
        &manifest,
        &listed,
        &BTreeSet::from([1]),
    );
}

#[test]
#[should_panic(expected = "latest shard-manifest filename disagrees with its body")]
fn gate_r0_current_inventory_refuses_manifest_filename_body_version_mismatch() {
    let shard = ShardId::parse_str("abcdefab-cdef-abcd-efab-cdefabcdefab").unwrap();
    let manifest = gate_r0_manifest(shard, Vec::new());
    let _ = validated_referenced_generation_roots(
        "table",
        shard,
        &manifest,
        &BTreeSet::new(),
        &BTreeSet::from([2]),
    );
}

#[test]
#[should_panic(expected = "gapped or noncanonical shard-manifest version chain")]
fn gate_r0_current_inventory_refuses_gapped_manifest_version_chain() {
    let shard = ShardId::parse_str("abcdefab-cdef-abcd-efab-cdefabcdefab").unwrap();
    let mut manifest = gate_r0_manifest(shard, Vec::new());
    manifest.version = 3;
    let _ = validated_referenced_generation_roots(
        "table",
        shard,
        &manifest,
        &BTreeSet::new(),
        &BTreeSet::from([1, 3]),
    );
}

#[tokio::test]
#[should_panic(expected = "listed shard has no latest manifest")]
async fn gate_r0_current_inventory_refuses_listed_shard_without_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let shard = "abcdefab-cdef-abcd-efab-cdefabcdefab";
    let wal_dir = dir.path().join("_mem_wal").join(shard).join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();
    std::fs::write(
        wal_dir.join(format!("{}.arrow", bit_reversed_stem(1))),
        b"x",
    )
    .unwrap();
    let _ = current_mem_wal_inventory(dir.path().to_str().unwrap()).await;
}

#[test]
fn gate_r0_source_audit_revision_tripwire_matches_surveyed_lance() {
    let expected_source = "registry+https://github.com/rust-lang/crates.io-index";
    let expected_lance_repository_packages = BTreeSet::from([
        "fsst",
        "lance",
        "lance-arrow",
        "lance-arrow-scalar",
        "lance-arrow-stats",
        "lance-bitpacking",
        "lance-core",
        "lance-datafusion",
        "lance-datagen",
        "lance-derive",
        "lance-encoding",
        "lance-file",
        "lance-index",
        "lance-index-core",
        "lance-io",
        "lance-linalg",
        "lance-namespace",
        "lance-namespace-impls",
        "lance-select",
        "lance-table",
        "lance-tokenizer",
    ]);
    let mut observed = BTreeMap::<&str, Vec<&str>>::new();
    for package in include_str!("../../../Cargo.lock")
        .split("[[package]]")
        .skip(1)
    {
        let value = |key: &str| {
            package.lines().find_map(|line| {
                line.strip_prefix(&format!("{key} = \""))
                    .and_then(|value| value.strip_suffix('"'))
            })
        };
        let Some(name) = value("name") else {
            continue;
        };
        let Some(source) = value("source") else {
            continue;
        };
        if !expected_lance_repository_packages.contains(name) {
            continue;
        }
        assert!(
            !source.starts_with("git+"),
            "Lance-repository package {name} is pinned to a git source again; Gate R0's source audit must be rerun against it"
        );
        assert_eq!(
            source, expected_source,
            "Lance-repository package {name} left the crates.io registry; Gate R0's source audit must be rerun"
        );
        let version = value("version").expect("every locked package declares a version");
        let expected_version = if SURVEYED_LANCE_ARROW_PACKAGES.contains(&name) {
            SURVEYED_LANCE_ARROW_VERSION
        } else {
            SURVEYED_LANCE_VERSION
        };
        assert_eq!(
            version, expected_version,
            "audited Lance-repository package {name} moved off the surveyed version; Gate R0's source audit must be rerun"
        );
        observed.entry(name).or_default().push(version);
    }
    assert_eq!(
        observed.keys().copied().collect::<BTreeSet<_>>(),
        expected_lance_repository_packages,
        "the exact audited Lance-repository package family changed; Gate R0's source audit must be rerun"
    );
    for package in &expected_lance_repository_packages {
        let expected_version = if SURVEYED_LANCE_ARROW_PACKAGES.contains(package) {
            SURVEYED_LANCE_ARROW_VERSION
        } else {
            SURVEYED_LANCE_VERSION
        };
        assert_eq!(
            observed.get(package).map(Vec::as_slice),
            Some([expected_version].as_slice()),
            "audited Lance-repository package {package} moved, disappeared, or became mixed; Gate R0's source audit must be rerun"
        );
    }
    let assert_direct_manifest_pin = |manifest: &str, package: &str| {
        let prefix = format!("{package} = ");
        let lines = manifest
            .lines()
            .filter(|line| line.starts_with(&prefix))
            .collect::<Vec<_>>();
        assert_eq!(
            lines.len(),
            1,
            "Gate R0 must find exactly one direct source declaration for {package}"
        );
        assert!(
            !lines[0].contains("git = "),
            "direct Lance manifest pin returned to a git source without rerunning Gate R0: {}",
            lines[0]
        );
        assert!(
            lines[0].contains(&format!("\"{SURVEYED_LANCE_VERSION}\"")),
            "direct Lance manifest pin moved off the surveyed version without rerunning Gate R0: {}",
            lines[0]
        );
    };
    let workspace_manifest = include_str!("../../../Cargo.toml");
    for package in [
        "lance",
        "lance-core",
        "lance-select",
        "lance-datafusion",
        "lance-file",
        "lance-index",
        "lance-linalg",
        "lance-namespace",
        "lance-namespace-impls",
        "lance-table",
    ] {
        assert_direct_manifest_pin(workspace_manifest, package);
    }
    assert_direct_manifest_pin(include_str!("../Cargo.toml"), "lance-io");
}

#[tokio::test]
#[serial]
async fn gate_r0_referenced_cut_retry_reuses_the_same_generation_root_local() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = init_enrolled(dir.path().to_str().unwrap(), STREAM_SCHEMA).await;
    let db = Arc::new(Omnigraph::open(&uri).await.unwrap());
    let batch = score_batch(table_schema(&db).await, "r0-retry", 1);
    db.failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
        .await
        .unwrap();

    let error = {
        let _after_drain =
            ScopedFailPoint::new(names::STREAM_FOLD_POST_DRAIN_PRE_SIDECAR, "return");
        db.failpoint_stream_b1_for_test(TABLE, None, 0)
            .await
            .expect_err("stop after Lance published the generation cut")
    };
    assert!(
        error
            .to_string()
            .contains(names::STREAM_FOLD_POST_DRAIN_PRE_SIDECAR),
        "{error:?}"
    );
    let after_failed_fold = current_mem_wal_inventory(&uri).await;
    assert_eq!(after_failed_fold.generation_roots.len(), 1);
    assert_eq!(
        after_failed_fold.generation_roots,
        after_failed_fold.referenced_generation_roots
    );
    assert!(
        after_failed_fold.unknown_paths().is_empty(),
        "{after_failed_fold:#?}"
    );

    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .unwrap();
    let after_retry = current_mem_wal_inventory(&uri).await;
    assert_eq!(
        after_retry.generation_roots, after_failed_fold.generation_roots,
        "a retry after the referenced cut must fold-only, not materialize another random root"
    );
    assert!(after_retry.unknown_paths().is_empty(), "{after_retry:#?}");
    assert_eq!(
        after_retry.generation_roots,
        after_retry.referenced_generation_roots
    );
    assert_eq!(
        after_retry.generation_subtree_objects(),
        after_failed_fold.generation_subtree_objects(),
        "fold-only retry must not add, replace, or resize any generation-subtree object"
    );
    after_failed_fold.assert_path_class_size_retained_by(&after_retry);
    assert_eq!(helpers::count_rows(&db, TABLE).await, 1);
}

fn deterministic_high_entropy_ascii(row: usize, len: usize) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut state = (row as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    let mut bytes = Vec::with_capacity(len);
    for ordinal in 0..len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state = state.wrapping_add((ordinal as u64).wrapping_mul(0xd6e8_feb8_6659_fd93));
        bytes.push(ALPHABET[(state as usize) & (ALPHABET.len() - 1)]);
    }
    String::from_utf8(bytes).unwrap()
}

async fn widest_high_entropy_payload_batch(db: &Omnigraph, payload_bytes: usize) -> RecordBatch {
    let schema = table_schema(db).await;
    let ids = Arc::new(StringArray::from_iter_values(
        (0..WIDEST_ROWS).map(|row| format!("rss-{row:05}")),
    )) as ArrayRef;
    let values = Arc::new(StringArray::from_iter_values(
        (0..WIDEST_ROWS).map(|row| deterministic_high_entropy_ascii(row, payload_bytes)),
    )) as ArrayRef;
    let batch = RecordBatch::try_new(schema, vec![ids, values]).unwrap();
    assert!(
        batch.get_array_memory_size() > 29 * 1024 * 1024,
        "Gate R0 fixture must remain a concrete high-entropy near-cap caller batch"
    );
    batch
}

async fn widest_retained_growth_at_uri(cluster_uri: &str) -> WidestRetainedGrowthSample {
    let uri = init_enrolled(cluster_uri, PAYLOAD_SCHEMA).await;
    let table_tracker = IOTracker::default();
    let manifest_tracker = IOTracker::default();
    with_raw_io_trackers(&table_tracker, &manifest_tracker, async {
        let db = Arc::new(Omnigraph::open(&uri).await.unwrap());
        let claim_setup = payload_batch(&db, 1, 1).await;
        let claim_error = {
            let _before_put = ScopedFailPoint::new(names::STREAM_B1_BEFORE_PUT_INVOKE, "return");
            db.failpoint_stream_b1_for_test(TABLE, Some(claim_setup), 0)
                .await
                .expect_err("fixture setup must stop after the cold claim and before WAL input")
        };
        assert!(
            claim_error
                .to_string()
                .contains(names::STREAM_B1_BEFORE_PUT_INVOKE),
            "{claim_error:?}"
        );
        let before = current_mem_wal_inventory(&uri).await;
        let before_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
        let manifest_version_before = before_snapshot.version();
        let table_version_before = before_snapshot.entry(TABLE).unwrap().table_version;
        let _ = table_tracker.incremental_stats();
        let _ = manifest_tracker.incremental_stats();

        let error = {
            let batch =
                widest_high_entropy_payload_batch(&db, WIDEST_ONE_BYTE_OVER_PAYLOAD_BYTES).await;
            db.failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
                .await
                .expect_err("the first byte-per-row over the exact B2 limit must be rejected")
        };
        match error {
            OmniError::FoldRequired {
                table_key,
                rows,
                bytes,
            } => {
                assert_eq!(table_key, TABLE);
                assert_eq!(rows, WIDEST_ROWS as u64);
                assert_eq!(bytes, WIDEST_ONE_BYTE_OVER_ATTRIBUTED_ARROW_BYTES as u64);
            }
            other => panic!("expected exact FoldRequired boundary, got {other:?}"),
        }
        let rejected_table_io = PathIo::from_stats(&table_tracker.incremental_stats());
        let rejected_manifest_io = PathIo::from_stats(&manifest_tracker.incremental_stats());
        assert_eq!(
            rejected_table_io.total_writes, 0,
            "over-cap admission must remain effect-free: {rejected_table_io:#?}"
        );
        assert_eq!(
            rejected_manifest_io.total_writes, 0,
            "over-cap admission must not arm or publish recovery: {rejected_manifest_io:#?}"
        );
        let after_rejection = current_mem_wal_inventory(&uri).await;
        assert_eq!(after_rejection.objects, before.objects);
        assert_eq!(after_rejection.generation_roots, before.generation_roots);
        assert_eq!(
            after_rejection.referenced_generation_roots,
            before.referenced_generation_roots
        );
        let after_rejection_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
        assert_eq!(after_rejection_snapshot.version(), manifest_version_before);
        assert_eq!(
            after_rejection_snapshot.entry(TABLE).unwrap().table_version,
            table_version_before
        );
        assert!(
            current_recovery_sidecars(&uri).await.is_empty(),
            "over-cap admission must not leave a recovery sidecar"
        );
        let _ = table_tracker.incremental_stats();
        let _ = manifest_tracker.incremental_stats();

        let batch = widest_high_entropy_payload_batch(&db, WIDEST_PAYLOAD_BYTES).await;
        db.failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
            .await
            .unwrap();
        let table_ack = PathIo::from_stats(&table_tracker.incremental_stats());
        let _ = manifest_tracker.incremental_stats();
        let after_ack = current_mem_wal_inventory(&uri).await;
        let after_ack_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
        let manifest_version_after_ack = after_ack_snapshot.version();
        let table_version_after_ack = after_ack_snapshot.entry(TABLE).unwrap().table_version;

        db.failpoint_stream_b1_for_test(TABLE, None, 0)
            .await
            .expect("every admitted near-cap generation must close");
        let table_fold = PathIo::from_stats(&table_tracker.incremental_stats());
        let after_fold = current_mem_wal_inventory(&uri).await;
        let after_fold_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
        let manifest_version_after_fold = after_fold_snapshot.version();
        let table_version_after_fold = after_fold_snapshot.entry(TABLE).unwrap().table_version;

        let wanted = BTreeSet::from([
            "rss-00000".to_string(),
            format!("rss-{:05}", WIDEST_ROWS / 2),
            format!("rss-{:05}", WIDEST_ROWS - 1),
        ]);
        let visible = helpers::read_table(&db, TABLE).await;
        let visible_rows = visible.iter().map(RecordBatch::num_rows).sum();
        let mut sampled_payloads = Vec::new();
        for batch in &visible {
            let ids = batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("folded id must remain Utf8");
            let payloads = batch
                .column_by_name("payload")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("folded payload must remain Utf8");
            for row in 0..batch.num_rows() {
                if wanted.contains(ids.value(row)) {
                    sampled_payloads
                        .push((ids.value(row).to_string(), payloads.value(row).to_string()));
                }
            }
        }
        sampled_payloads.sort_by(|left, right| left.0.cmp(&right.0));

        WidestRetainedGrowthSample {
            before,
            after_ack,
            after_fold,
            table_ack,
            table_fold,
            manifest_version_before,
            manifest_version_after_ack,
            manifest_version_after_fold,
            table_version_before,
            table_version_after_ack,
            table_version_after_fold,
            visible_rows,
            sampled_payloads,
            recovery_sidecars_after_fold: current_recovery_sidecars(&uri).await,
        }
    })
    .await
}

fn assert_widest_retained_growth(sample: &WidestRetainedGrowthSample, backend: &str) {
    eprintln!(
        "Gate R0 widest {backend}: fold=closed visible_rows={} payload_bytes_per_row={} attributed_arrow_bytes={} first_illegal_payload_bytes_per_row={} first_illegal_attributed_arrow_bytes={} before_immutable_bytes={} after_ack_immutable_bytes={} after_fold_immutable_bytes={} wal_bytes={} generation_data_bytes={} generation_pk_bytes={} generation_bloom_bytes={} ack_writes={} fold_generation_writes={} manifest_versions={}/{}/{} table_versions={}/{}/{}",
        sample.visible_rows,
        WIDEST_PAYLOAD_BYTES,
        WIDEST_ATTRIBUTED_ARROW_BYTES,
        WIDEST_ONE_BYTE_OVER_PAYLOAD_BYTES,
        WIDEST_ONE_BYTE_OVER_ATTRIBUTED_ARROW_BYTES,
        sample.before.immutable_object_bytes(),
        sample.after_ack.immutable_object_bytes(),
        sample.after_fold.immutable_object_bytes(),
        sample.after_fold.bytes(MemWalObjectKind::Wal),
        sample.after_fold.bytes(MemWalObjectKind::GenerationData),
        sample
            .after_fold
            .bytes(MemWalObjectKind::GenerationPkSidecar),
        sample.after_fold.bytes(MemWalObjectKind::GenerationBloom),
        sample.table_ack.total_writes,
        sample.table_fold.generation_writes,
        sample.manifest_version_before,
        sample.manifest_version_after_ack,
        sample.manifest_version_after_fold,
        sample.table_version_before,
        sample.table_version_after_ack,
        sample.table_version_after_fold,
    );
    assert_eq!(
        WIDEST_ONE_BYTE_OVER_ATTRIBUTED_ARROW_BYTES - WIDEST_ATTRIBUTED_ARROW_BYTES,
        WIDEST_ROWS,
        "one additional payload byte per row must move the exact charge by one byte per row"
    );
    sample
        .before
        .assert_path_class_size_retained_by(&sample.after_ack);
    sample
        .after_ack
        .assert_path_class_size_retained_by(&sample.after_fold);
    assert!(sample.before.unknown_paths().is_empty(), "{sample:#?}");
    assert!(sample.after_ack.unknown_paths().is_empty(), "{sample:#?}");
    assert!(sample.after_fold.unknown_paths().is_empty(), "{sample:#?}");
    assert_eq!(sample.table_ack.generation_writes, 0, "{sample:#?}");
    assert!(
        sample.after_ack.bytes(MemWalObjectKind::Wal)
            >= u64::try_from(HARD_ARROW_BYTES * 9 / 10).unwrap(),
        "high-entropy WAL evidence compressed below its remeasurement tripwire"
    );
    assert!(sample.after_ack.generation_roots.is_empty());
    assert_eq!(sample.after_fold.generation_roots.len(), 1);
    assert_eq!(
        sample.after_fold.generation_roots,
        sample.after_fold.referenced_generation_roots
    );
    assert!(
        sample.after_fold.bytes(MemWalObjectKind::GenerationData)
            >= u64::try_from(HARD_ARROW_BYTES * 9 / 10).unwrap(),
        "high-entropy generation evidence compressed below its remeasurement tripwire"
    );
    assert!(
        sample
            .after_fold
            .bytes(MemWalObjectKind::GenerationPkSidecar)
            > 0
    );
    assert!(sample.after_fold.bytes(MemWalObjectKind::GenerationBloom) > 0);
    assert!(sample.after_fold.immutable_object_bytes() > sample.after_ack.immutable_object_bytes());
    assert_eq!(
        sample.manifest_version_after_ack, sample.manifest_version_before,
        "durable acknowledgement must not publish the graph"
    );
    assert_eq!(
        sample.table_version_after_ack, sample.table_version_before,
        "durable acknowledgement must not advance the base table"
    );
    assert_eq!(
        sample.manifest_version_after_fold,
        sample.manifest_version_before + 1,
        "one fold must have exactly one graph visibility point"
    );
    assert_eq!(
        sample.table_version_after_fold,
        sample.table_version_before + 1,
        "one fold must commit exactly one Lance table effect"
    );
    assert_eq!(sample.visible_rows, WIDEST_ROWS);
    let expected_samples = [0, WIDEST_ROWS / 2, WIDEST_ROWS - 1]
        .into_iter()
        .map(|row| {
            (
                format!("rss-{row:05}"),
                deterministic_high_entropy_ascii(row, WIDEST_PAYLOAD_BYTES),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(sample.sampled_payloads, expected_samples);
    assert!(
        sample.recovery_sidecars_after_fold.is_empty(),
        "successful closure must retire its recovery sidecar: {:?}",
        sample.recovery_sidecars_after_fold
    );
}

#[tokio::test]
#[serial]
async fn gate_r0_widest_generation_closes_and_records_retain_all_growth_local() {
    let dir = tempfile::tempdir().unwrap();
    let sample = widest_retained_growth_at_uri(dir.path().to_str().unwrap()).await;
    assert_widest_retained_growth(&sample, "local");
}

#[tokio::test]
#[serial]
async fn gate_r0_widest_generation_closes_and_records_retain_all_growth_on_configured_rustfs() {
    let Some(bucket) = std::env::var("OMNIGRAPH_S3_TEST_BUCKET").ok() else {
        eprintln!("SKIP: OMNIGRAPH_S3_TEST_BUCKET not set; RustFS Gate R0 closure not measured");
        return;
    };
    let prefix = std::env::var("OMNIGRAPH_S3_TEST_PREFIX")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "omnigraph-itests".to_string());
    let uri = format!(
        "s3://{bucket}/{prefix}/memwal-gate-r0/{}-{}",
        std::process::id(),
        ulid::Ulid::new()
    );
    let sample = widest_retained_growth_at_uri(&uri).await;
    storage_for_uri(&uri)
        .unwrap()
        .delete_prefix(&uri)
        .await
        .expect("configured RustFS Gate R0 cleanup failed");
    assert_widest_retained_growth(&sample, "rustfs");
}

fn f6b4_dead_letter_payloads(literal_adjustment: i8) -> (Vec<Vec<u8>>, u64) {
    let literal_chars =
        i64::try_from(DEAD_LETTER_EXACT_LITERAL_CHARS).unwrap() + i64::from(literal_adjustment);
    assert!(literal_chars >= 0);
    let literal_chars = usize::try_from(literal_chars).unwrap();

    let base_value = "\0".repeat(DEAD_LETTER_BASE_CONTROL_CHARS);
    let base_payload = serde_json::to_vec(&serde_json::json!({
        "payload": base_value,
    }))
    .unwrap();
    let mut payloads = Vec::with_capacity(DEAD_LETTER_CANDIDATES);
    for _ in 0..DEAD_LETTER_CANDIDATES - 1 {
        payloads.push(base_payload.clone());
    }
    let mut final_value =
        "\0".repeat(DEAD_LETTER_BASE_CONTROL_CHARS + DEAD_LETTER_EXACT_EXTRA_CONTROL_CHARS);
    final_value.push_str(&"x".repeat(literal_chars));
    payloads.push(
        serde_json::to_vec(&serde_json::json!({
            "payload": final_value,
        }))
        .unwrap(),
    );
    let source_value_bytes = DEAD_LETTER_BASE_CONTROL_CHARS
        .checked_mul(DEAD_LETTER_CANDIDATES)
        .and_then(|bytes| bytes.checked_add(DEAD_LETTER_EXACT_EXTRA_CONTROL_CHARS))
        .and_then(|bytes| bytes.checked_add(literal_chars))
        .and_then(|bytes| u64::try_from(bytes).ok())
        .unwrap();
    (payloads, source_value_bytes)
}

fn assert_f6b4_dead_letter_measurement(
    measurement: &StreamDeadLetterEncodingCostForTest,
    expected_encoded_bytes: u64,
) {
    assert_eq!(
        measurement.candidate_count,
        u64::try_from(DEAD_LETTER_CANDIDATES).unwrap()
    );
    assert_eq!(
        measurement.verified_candidate_count,
        measurement.candidate_count
    );
    assert_eq!(measurement.encoded_bytes, expected_encoded_bytes);
    assert!(measurement.encoded_capacity_bytes >= measurement.encoded_bytes);
    assert!(measurement.encoded_capacity_bytes <= DEAD_LETTER_OBJECT_BYTES);
    assert!(measurement.canonical_payload_bytes > 58 * 1024 * 1024);
    assert!(measurement.candidate_view_bytes > 0);
    assert!(measurement.ordinal_index_bytes > 0);
    assert!(measurement.line_range_index_bytes > 0);
}

#[test]
#[ignore = "F6b4 production-size dead-letter envelope; run explicitly with a 120-second timeout"]
fn f6b4_dead_letter_object_records_production_envelope_and_peak_rss() {
    let (one_under_payloads, _) = f6b4_dead_letter_payloads(-1);
    let one_under = failpoint_measure_stream_dead_letter_object_for_test(&one_under_payloads)
        .expect("the production object one byte below its cap must encode and verify");
    assert_f6b4_dead_letter_measurement(&one_under, DEAD_LETTER_OBJECT_BYTES - 1);
    drop(one_under_payloads);

    let (exact_payloads, source_value_bytes) = f6b4_dead_letter_payloads(0);
    let exact = failpoint_measure_stream_dead_letter_object_for_test(&exact_payloads)
        .expect("the production object exactly at its cap must encode and verify");
    assert_f6b4_dead_letter_measurement(&exact, DEAD_LETTER_OBJECT_BYTES);
    drop(exact_payloads);

    let (one_over_payloads, _) = f6b4_dead_letter_payloads(1);
    let one_over = failpoint_measure_stream_dead_letter_object_for_test(&one_over_payloads)
        .expect_err("the production object one byte above its cap must not become prepared");
    assert!(
        matches!(
            one_over,
            OmniError::ResourceLimitExceeded {
                ref resource,
                limit,
                actual,
            } if resource == "stream_dead_letter_object_encoded_bytes"
                && limit == DEAD_LETTER_OBJECT_BYTES
                && actual == DEAD_LETTER_OBJECT_BYTES + 1
        ),
        "{one_over:?}"
    );

    #[cfg(unix)]
    {
        let baseline_peak_rss = run_rss_child(
            "f6b4_dead_letter_object_cost_child",
            DEAD_LETTER_RSS_CHILD_ENV,
            "baseline",
        );
        let exact_peak_rss = run_rss_child(
            "f6b4_dead_letter_object_cost_child",
            DEAD_LETTER_RSS_CHILD_ENV,
            "exact",
        );
        let signed_peak_rss_lift = i128::from(exact_peak_rss) - i128::from(baseline_peak_rss);
        eprintln!(
            "F6b4 dead-letter envelope: candidates={} source_value_bytes={} canonical_payload_bytes={} encoded_bytes={} encoded_capacity_bytes={} candidate_view_bytes={} ordinal_index_bytes={} line_range_index_bytes={} encode_elapsed_micros={} verify_elapsed_micros={} baseline_peak_rss={} exact_peak_rss={} signed_peak_rss_lift={} rss_delta_remeasure_bytes={}",
            exact.candidate_count,
            source_value_bytes,
            exact.canonical_payload_bytes,
            exact.encoded_bytes,
            exact.encoded_capacity_bytes,
            exact.candidate_view_bytes,
            exact.ordinal_index_bytes,
            exact.line_range_index_bytes,
            exact.encode_elapsed_micros,
            exact.verify_elapsed_micros,
            baseline_peak_rss,
            exact_peak_rss,
            signed_peak_rss_lift,
            DEAD_LETTER_RSS_DELTA_REMEASURE_BYTES,
        );
        assert!(
            signed_peak_rss_lift <= i128::from(DEAD_LETTER_RSS_DELTA_REMEASURE_BYTES),
            "the isolated dead-letter encoder/verifier crossed its measured peak-RSS-lift envelope; remeasure before changing the object grammar or materialization shape"
        );
    }

    #[cfg(not(unix))]
    eprintln!(
        "F6b4 dead-letter envelope: candidates={} source_value_bytes={} canonical_payload_bytes={} encoded_bytes={} encoded_capacity_bytes={} candidate_view_bytes={} ordinal_index_bytes={} line_range_index_bytes={} encode_elapsed_micros={} verify_elapsed_micros={}; peak RSS unavailable on this platform",
        exact.candidate_count,
        source_value_bytes,
        exact.canonical_payload_bytes,
        exact.encoded_bytes,
        exact.encoded_capacity_bytes,
        exact.candidate_view_bytes,
        exact.ordinal_index_bytes,
        exact.line_range_index_bytes,
        exact.encode_elapsed_micros,
        exact.verify_elapsed_micros,
    );
}

/// Subprocess half of
/// `f6b4_dead_letter_object_records_production_envelope_and_peak_rss`.
#[test]
#[ignore = "subprocess helper; exercised by the F6b4 dead-letter cost cell"]
fn f6b4_dead_letter_object_cost_child() {
    let Some(mode) = std::env::var_os(DEAD_LETTER_RSS_CHILD_ENV) else {
        return;
    };
    let mode = mode.to_string_lossy();
    let (payloads, source_value_bytes) = f6b4_dead_letter_payloads(0);
    std::hint::black_box(source_value_bytes);
    match mode.as_ref() {
        "baseline" => {
            let canonical_payload_bytes = payloads.iter().map(Vec::len).sum::<usize>();
            assert!(canonical_payload_bytes > 58 * 1024 * 1024);
            std::hint::black_box(&payloads);
        }
        "exact" => {
            let measurement = failpoint_measure_stream_dead_letter_object_for_test(&payloads)
                .expect("the exact F6b4 child must encode and verify the production envelope");
            assert_f6b4_dead_letter_measurement(&measurement, DEAD_LETTER_OBJECT_BYTES);
            std::hint::black_box(measurement);
        }
        other => panic!("unknown F6b4 dead-letter RSS child mode '{other}'"),
    }
}

#[cfg(unix)]
fn normalized_peak_rss_bytes(rusage: &libc::rusage) -> u64 {
    #[cfg(target_os = "macos")]
    let peak = rusage.ru_maxrss as u64;
    #[cfg(not(target_os = "macos"))]
    let peak = (rusage.ru_maxrss as u64) * 1024;
    peak
}

#[cfg(unix)]
fn wait4_rusage(pid: i32) -> (i64, u64) {
    let mut status: libc::c_int = 0;
    let mut rusage: libc::rusage = unsafe { std::mem::zeroed() };
    let reaped = loop {
        let result = unsafe { libc::wait4(pid, &mut status, 0, &mut rusage) };
        if result != -1 || std::io::Error::last_os_error().raw_os_error() != Some(libc::EINTR) {
            break result;
        }
    };
    assert_eq!(reaped, pid, "wait4 reaped an unexpected process");
    let exit = if libc::WIFEXITED(status) {
        libc::WEXITSTATUS(status) as i64
    } else if libc::WIFSIGNALED(status) {
        -(libc::WTERMSIG(status) as i64)
    } else {
        i64::MIN
    };
    (exit, normalized_peak_rss_bytes(&rusage))
}

#[cfg(unix)]
fn run_rss_child(test_name: &str, env_name: &str, mode: &str) -> u64 {
    use std::process::{Command, Stdio};

    let child = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg(test_name)
        .arg("--ignored")
        .arg("--nocapture")
        .env(env_name, mode)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let pid = i32::try_from(child.id()).unwrap();
    let (exit, peak) = wait4_rusage(pid);
    // `wait4` reaped the child; dropping the handle only closes its pipes.
    drop(child);
    assert_eq!(exit, 0, "{mode} RSS child '{test_name}' failed");
    peak
}

#[test]
fn widest_legal_generation_records_no_roll_estimates_and_peak_rss() {
    let arrow_reservation = WIDEST_ATTRIBUTED_ARROW_BYTES;
    let one_byte_over_arrow_reservation = WIDEST_ONE_BYTE_OVER_ATTRIBUTED_ARROW_BYTES;
    let batch_store_estimate = arrow_reservation + std::mem::size_of::<RecordBatch>();
    let bloom_estimate = Sbbf::with_ndv_fpp(WIDEST_ROWS as u64, 0.00057)
        .unwrap()
        .estimated_memory_size();
    let rc_trigger_estimate = batch_store_estimate + bloom_estimate;
    let fragmented_rc_trigger_upper_bound =
        HARD_ARROW_BYTES + WIDEST_ROWS * std::mem::size_of::<RecordBatch>() + bloom_estimate;
    assert!(
        arrow_reservation > 31 * 1024 * 1024 && arrow_reservation <= HARD_ARROW_BYTES,
        "fixture must be a concrete near-cap legal generation, got {arrow_reservation} bytes"
    );
    assert!(
        one_byte_over_arrow_reservation > HARD_ARROW_BYTES,
        "the next uniform payload byte must cross the exact Arrow cap; legal={} next={one_byte_over_arrow_reservation}",
        arrow_reservation,
    );
    assert!(WIDEST_ROWS < NO_AUTO_ROLL_ROWS);
    assert!(1 < NO_AUTO_ROLL_BATCHES);
    assert!(rc_trigger_estimate < NO_AUTO_ROLL_BYTES);
    assert!(
        fragmented_rc_trigger_upper_bound < NO_AUTO_ROLL_BYTES,
        "even the conservative 8,192-batch trigger bound must remain below automatic roll"
    );
    assert!(WIDEST_ROWS < NO_AUTO_ROLL_BATCHES);

    #[cfg(unix)]
    {
        let baseline_peak_rss = run_rss_child(
            "widest_legal_generation_cost_child",
            RSS_CHILD_ENV,
            "baseline",
        );
        let widest_peak_rss = run_rss_child(
            "widest_legal_generation_cost_child",
            RSS_CHILD_ENV,
            "widest",
        );
        let baseline_fold_peak_rss = run_rss_child(
            "widest_legal_generation_cost_child",
            RSS_CHILD_ENV,
            "baseline-fold",
        );
        let widest_fold_peak_rss = run_rss_child(
            "widest_legal_generation_cost_child",
            RSS_CHILD_ENV,
            "widest-fold",
        );
        let signed_process_delta = i128::from(widest_peak_rss) - i128::from(baseline_peak_rss);
        let signed_fold_peak_lift =
            i128::from(widest_fold_peak_rss) - i128::from(baseline_fold_peak_rss);
        eprintln!(
            "B2 widest legal generation: rows={WIDEST_ROWS} one_batch_payload_bytes_per_row={WIDEST_PAYLOAD_BYTES} one_batch_attributed_post_tombstone_arrow_reservation={arrow_reservation} first_illegal_payload_bytes_per_row={WIDEST_ONE_BYTE_OVER_PAYLOAD_BYTES} first_illegal_attributed_post_tombstone_arrow_reservation={one_byte_over_arrow_reservation} one_batch_batch_store_estimate={batch_store_estimate} rc_pk_bloom_estimate={bloom_estimate} one_batch_rc_roll_trigger_estimate={rc_trigger_estimate} fragmented_batches={WIDEST_ROWS} fragmented_rc_roll_trigger_upper_bound={fragmented_rc_trigger_upper_bound} no_roll_bytes={NO_AUTO_ROLL_BYTES} rss_shape=one_batch baseline_peak_rss={baseline_peak_rss} widest_peak_rss={widest_peak_rss} signed_whole_process_delta={signed_process_delta} baseline_fold_peak_rss={baseline_fold_peak_rss} widest_fold_peak_rss={widest_fold_peak_rss} signed_fold_peak_lift={signed_fold_peak_lift} fold_delta_remeasure_bytes={FOLD_RSS_DELTA_REMEASURE_BYTES}"
        );
        // `ru_maxrss` is a lifetime high-water mark. Common graph initialization
        // can dominate both children on a constrained runner, so the paired lift
        // may be zero or negative even though the widest child completed its
        // exact writes and fold assertions. Keep this as a one-sided regression
        // tripwire rather than requiring the workload to establish a new peak.
        assert!(
            signed_fold_peak_lift <= i128::from(FOLD_RSS_DELTA_REMEASURE_BYTES),
            "the dense fold path crossed its measured peak-RSS-lift envelope; remeasure before changing the admission or compaction shape"
        );
    }

    #[cfg(not(unix))]
    eprintln!(
        "B2 widest legal generation: rows={WIDEST_ROWS} one_batch_payload_bytes_per_row={WIDEST_PAYLOAD_BYTES} one_batch_attributed_post_tombstone_arrow_reservation={arrow_reservation} first_illegal_payload_bytes_per_row={WIDEST_ONE_BYTE_OVER_PAYLOAD_BYTES} first_illegal_attributed_post_tombstone_arrow_reservation={one_byte_over_arrow_reservation} one_batch_batch_store_estimate={batch_store_estimate} rc_pk_bloom_estimate={bloom_estimate} one_batch_rc_roll_trigger_estimate={rc_trigger_estimate} fragmented_batches={WIDEST_ROWS} fragmented_rc_roll_trigger_upper_bound={fragmented_rc_trigger_upper_bound}; peak RSS unavailable on this platform"
    );
}

/// Subprocess half of `widest_legal_generation_records_no_roll_estimates_and_peak_rss`.
/// Keep it non-`serial`: the parent waits for this process and must not share an
/// interprocess serial-test lock with it.
#[test]
#[ignore = "subprocess helper; exercised by the widest-generation cost cell"]
fn widest_legal_generation_cost_child() {
    let Some(mode) = std::env::var_os(RSS_CHILD_ENV) else {
        return;
    };
    let mode = mode.to_string_lossy().into_owned();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move {
            let dir = tempfile::tempdir().unwrap();
            let uri = init_enrolled(dir.path().to_str().unwrap(), PAYLOAD_SCHEMA).await;
            let db = Arc::new(Omnigraph::open(&uri).await.unwrap());
            let (batch, fold) = match mode.as_str() {
                "baseline" => (payload_batch(&db, 1, 1).await, false),
                "widest" => (
                    payload_batch(&db, WIDEST_ROWS, WIDEST_PAYLOAD_BYTES).await,
                    false,
                ),
                "baseline-fold" => (payload_batch(&db, 1, 1).await, true),
                "widest-fold" => (
                    widest_high_entropy_payload_batch(&db, WIDEST_PAYLOAD_BYTES).await,
                    true,
                ),
                other => panic!("unknown RSS child mode '{other}'"),
            };
            let expected_rows = batch.num_rows();
            let table_tracker = IOTracker::default();
            let manifest_tracker = IOTracker::default();
            with_raw_io_trackers(&table_tracker, &manifest_tracker, async {
                let _ = table_tracker.incremental_stats();
                db.failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
                    .await
                    .unwrap();
                let table = PathIo::from_stats(&table_tracker.incremental_stats());
                assert!(table.wal_writes > 0, "{table:?}");
                assert_eq!(table.generation_writes, 0, "{table:?}");
                if fold {
                    db.failpoint_stream_b1_for_test(TABLE, None, 0)
                        .await
                        .expect("RSS fold child must close its admitted generation");
                    let fold_table = PathIo::from_stats(&table_tracker.incremental_stats());
                    assert!(fold_table.generation_writes > 0, "{fold_table:?}");
                    assert_eq!(
                        helpers::count_rows(&db, TABLE).await,
                        expected_rows,
                        "RSS fold child must publish every admitted row"
                    );
                }
            })
            .await;
        });
}
