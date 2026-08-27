//! Rebuildable OmniGraph query projection over the immutable run archive.
//!
//! The JSON archive remains the telemetry authority. This module has no API
//! for incrementally changing projection rows: a rebuild validates the whole
//! archive, constructs a fresh immutable generation, verifies its inventory,
//! and atomically publishes a small `CURRENT` pointer. Readers expose only the
//! named queries below and never accept caller-supplied query text.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
#[cfg(unix)]
use std::ffi::OsStr;
use std::ffi::OsString;
use std::fmt::{Display, Formatter};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
#[cfg(unix)]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(unix)]
use std::thread;
#[cfg(unix)]
use std::time::{Duration, Instant};

#[cfg(unix)]
use nix::fcntl::{Flock, FlockArg, OFlag, openat, renameat};
#[cfg(unix)]
use nix::sys::stat::{Mode, mkdirat};
#[cfg(unix)]
use nix::unistd::{UnlinkatFlags, unlinkat};
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::loader::LoadMode;
use omnigraph_compiler::ParamMap;
use omnigraph_compiler::query::ast::Literal;
use omnigraph_compiler::result::QueryResult;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

use crate::archive::{ARCHIVE_FORMAT_VERSION, ArchiveError, ArchiveRecordIter, iter_archive};
use crate::counting::LogicalCallCounts;
use crate::record::RunRecordV1;

/// Version of the projection layout, schema, manifest, and pointer contract.
pub const PROJECTION_FORMAT_VERSION: u32 = 1;
/// Largest page a public projection query may return.
pub const MAX_PROJECTION_PAGE_SIZE: u32 = 100;
/// Default size used by the first-page convenience functions.
pub const DEFAULT_PROJECTION_PAGE_SIZE: u32 = MAX_PROJECTION_PAGE_SIZE;

const CURRENT_FILE: &str = "CURRENT";
const GENERATIONS_DIRECTORY: &str = "generations";
const GRAPH_DIRECTORY: &str = "graph";
const MANIFEST_FILE: &str = "manifest-v1.json";
const MAX_POINTER_BYTES: u64 = 16 * 1024;
const MAX_MANIFEST_BYTES: u64 = 64 * 1024;
const PROJECTION_TRANSFORM_VERSION: u32 = 4;
const MAX_PROJECTED_RECORDS: usize = 100_000;
const MAX_BATCH_ROWS: usize = 2_048;
const MAX_BATCH_BYTES: usize = 24 * 1024 * 1024;
// A public fetch is 100 returned rows plus one continuation witness. At 64 KiB
// per canonical source row, the serialized upper bound remains below 8 MiB.
const MAX_PROJECTED_ROW_BYTES: usize = 64 * 1024;
const MAX_PUBLIC_PAGE_BYTES: usize = 8 * 1024 * 1024;
const MAX_RETAINED_METADATA_BYTES: usize = 64 * 1024 * 1024;
const MAX_INVENTORY_ENTRY_BYTES: usize = 512;
const MAX_RETAINED_GENERATIONS: usize = 8;
const MAX_GENERATION_DIRECTORY_ENTRIES: usize = 1_024;
#[cfg(unix)]
const MAX_CURRENT_STAGING_ATTEMPTS: usize = 64;
const VERIFICATION_PAGE_SIZE: usize = 100;
// Public pages contain at most 100 rows. The extra row is a bounded witness
// that another page exists, because GQ V1 accepts only a literal LIMIT.
const QUERY_FETCH_LIMIT: usize = MAX_PROJECTION_PAGE_SIZE as usize + 1;
#[cfg(unix)]
const REBUILD_LOCK_TIMEOUT: Duration = Duration::from_secs(30);
#[cfg(unix)]
const REBUILD_LOCK_RETRY_INTERVAL: Duration = Duration::from_millis(50);
const GENERATION_DOMAIN: &[u8] = b"omnigraph-bench-projection-generation-v1\0";
const INVENTORY_DOMAIN: &[u8] = b"omnigraph-bench-projection-inventory-v1\0";
const TRANSFORM_DOMAIN: &[u8] = b"omnigraph-bench-projection-transform-v1\0";
const PROJECTED_ROWS_DOMAIN: &[u8] = b"omnigraph-bench-projected-rows-v1\0";
const PROJECTED_TABLE_DOMAIN: &[u8] = b"omnigraph-bench-projected-table-v1\0";

#[cfg(unix)]
static CURRENT_STAGING_COUNTER: AtomicU64 = AtomicU64::new(0);

// This prose is a machine contract, not documentation: bump the transform
// version and update this declaration whenever the source-to-row mapping or
// canonical row digest semantics change. Schema and named-query bytes are
// hashed alongside it so a generation cannot silently survive either change.
const PROJECTION_TRANSFORM_CONTRACT: &str = concat!(
    "point=canonical-json(PointRow),ordered-by=point_id;",
    "run=canonical-json(RunRow),ordered-by=invocation_id;",
    "acquisition=status,claim-eligible=complete-and-effective-codegen-proved,nullable-terminal;",
    "edges=inventory(invocation_id,record_sha256,point_id);",
    "logical-calls=min,p50-nearest-rank,max-per-plane;",
    "canonical-json=serde-struct-declaration-order-v1;",
    "digest=table-domain,table-name,row-domain,ordered-framed-row-bytes"
);

const _: () = assert!(QUERY_FETCH_LIMIT * MAX_PROJECTED_ROW_BYTES <= MAX_PUBLIC_PAGE_BYTES);

const PROJECTION_SCHEMA: &str = r#"
node BenchmarkPoint {
    point_id: String @key
    point_name: String
    point_identity_version: U32
    scenario: String
    run_spec_json: String
}

node BenchmarkRun {
    invocation_id: String @key
    record_sha256: String @unique
    archive_object: String @unique
    archive_pointer: String @unique
    session_id: String
    invoked_at_unix_ms: U64
    case_id: String
    case_digest: String
    package_version: String
    source_commit: String
    source_tree_dirty: Bool
    build_profile: String
    build_opt_level: String
    debug_assertions: Bool
    target_triple: String
    rustc_version: String
    build_declared_release_lto: String
    build_declared_release_codegen_units: U32
    build_declared_release_strip: Bool
    build_cargo_encoded_rustflags_present: Bool
    build_release_profile_environment_overrides_supported: Bool
    build_effective_codegen_options_proved: Bool
    worker_executable_sha256: String
    sut_fingerprint: String
    sut_json: String
    machine_fingerprint: String
    machine_format_version: U32
    machine_os_name: String
    machine_os_version: String
    machine_kernel_version: String
    machine_architecture: String
    machine_cpu_model: String
    machine_logical_cores: U32
    machine_physical_cores: U32
    machine_total_memory_bytes: U64
    machine_resource_control_json: String
    machine_scheduling_json: String
    machine_resource_limits_json: String
    machine_label: String
    backend_fingerprint: String
    backend_json: String
    fixture_manifest_sha256: String
    fixture_logical_sha256: String
    fixture_physical_sha256: String
    acquisition_status: String
    claim_eligible: Bool
    terminal_failed_repetition: U32?
    terminal_stage: String?
    terminal_code: String?
    requested_repetitions: U32
    observed_repetitions: U32
    min_us: U64
    p50_us: U64
    max_us: U64
    p95_us: U64?
    p95_supported: Bool
    wall_evidence: String
    floor_multiplier_millis: U32
    lance_data_plane_logical_calls_min: U64
    lance_data_plane_logical_calls_p50: U64
    lance_data_plane_logical_calls_max: U64
    control_plane_logical_calls_min: U64
    control_plane_logical_calls_p50: U64
    control_plane_logical_calls_max: U64
    logical_counts_presence_json: String
    physical_counts_presence_json: String
}

edge Measures: BenchmarkRun -> BenchmarkPoint @card(1..1) {
    @unique(src, dst)
}
"#;

const PROJECTION_QUERIES: &str = r#"
query list_points_page($after_key: String) {
    match {
        $point: BenchmarkPoint
        $point.point_id > $after_key
    }
    return {
        $point.point_id as point_id
        $point.point_name as point_name
        $point.point_identity_version as point_identity_version
        $point.scenario as scenario
        $point.run_spec_json as run_spec_json
    }
    order { point_id asc }
    limit 101
}

query list_runs_for_point_page($point_id: String, $after_key: String) {
    match {
        $run: BenchmarkRun
        $point: BenchmarkPoint { point_id: $point_id }
        $run measures $point
        $run.invocation_id > $after_key
    }
    return {
        $run.invocation_id as invocation_id
        $run.record_sha256 as record_sha256
        $point.point_id as point_id
        $run.session_id as session_id
        $run.invoked_at_unix_ms as invoked_at_unix_ms
        $run.case_id as case_id
        $run.case_digest as case_digest
        $run.package_version as package_version
        $run.source_commit as source_commit
        $run.source_tree_dirty as source_tree_dirty
        $run.build_profile as build_profile
        $run.build_opt_level as build_opt_level
        $run.debug_assertions as debug_assertions
        $run.target_triple as target_triple
        $run.rustc_version as rustc_version
        $run.build_declared_release_lto as build_declared_release_lto
        $run.build_declared_release_codegen_units as build_declared_release_codegen_units
        $run.build_declared_release_strip as build_declared_release_strip
        $run.build_cargo_encoded_rustflags_present as build_cargo_encoded_rustflags_present
        $run.build_release_profile_environment_overrides_supported as build_release_profile_environment_overrides_supported
        $run.build_effective_codegen_options_proved as build_effective_codegen_options_proved
        $run.worker_executable_sha256 as worker_executable_sha256
        $run.sut_fingerprint as sut_fingerprint
        $run.sut_json as sut_json
        $run.machine_fingerprint as machine_fingerprint
        $run.machine_format_version as machine_format_version
        $run.machine_os_name as machine_os_name
        $run.machine_os_version as machine_os_version
        $run.machine_kernel_version as machine_kernel_version
        $run.machine_architecture as machine_architecture
        $run.machine_cpu_model as machine_cpu_model
        $run.machine_logical_cores as machine_logical_cores
        $run.machine_physical_cores as machine_physical_cores
        $run.machine_total_memory_bytes as machine_total_memory_bytes
        $run.machine_resource_control_json as machine_resource_control_json
        $run.machine_scheduling_json as machine_scheduling_json
        $run.machine_resource_limits_json as machine_resource_limits_json
        $run.machine_label as machine_label
        $run.backend_fingerprint as backend_fingerprint
        $run.backend_json as backend_json
        $run.fixture_manifest_sha256 as fixture_manifest_sha256
        $run.fixture_logical_sha256 as fixture_logical_sha256
        $run.fixture_physical_sha256 as fixture_physical_sha256
        $run.acquisition_status as acquisition_status
        $run.claim_eligible as claim_eligible
        $run.terminal_failed_repetition as terminal_failed_repetition
        $run.terminal_stage as terminal_stage
        $run.terminal_code as terminal_code
        $run.requested_repetitions as requested_repetitions
        $run.observed_repetitions as observed_repetitions
        $run.min_us as min_us
        $run.p50_us as p50_us
        $run.max_us as max_us
        $run.p95_us as p95_us
        $run.p95_supported as p95_supported
        $run.wall_evidence as wall_evidence
        $run.floor_multiplier_millis as floor_multiplier_millis
        $run.lance_data_plane_logical_calls_min as lance_data_plane_logical_calls_min
        $run.lance_data_plane_logical_calls_p50 as lance_data_plane_logical_calls_p50
        $run.lance_data_plane_logical_calls_max as lance_data_plane_logical_calls_max
        $run.control_plane_logical_calls_min as control_plane_logical_calls_min
        $run.control_plane_logical_calls_p50 as control_plane_logical_calls_p50
        $run.control_plane_logical_calls_max as control_plane_logical_calls_max
        $run.logical_counts_presence_json as logical_counts_presence_json
        $run.physical_counts_presence_json as physical_counts_presence_json
    }
    order { invocation_id asc }
    limit 101
}

query projection_inventory_page($after_key: String) {
    match {
        $run: BenchmarkRun
        $point: BenchmarkPoint
        $run measures $point
        $run.invocation_id > $after_key
    }
    return {
        $run.invocation_id as invocation_id
        $run.record_sha256 as record_sha256
        $point.point_id as point_id
    }
    order { invocation_id asc }
    limit 100
}

query projection_point_rows_page($after_key: String) {
    match {
        $point: BenchmarkPoint
        $point.point_id > $after_key
    }
    return {
        $point.point_id as point_id
        $point.point_name as point_name
        $point.point_identity_version as point_identity_version
        $point.scenario as scenario
        $point.run_spec_json as run_spec_json
    }
    order { point_id asc }
    limit 100
}

query projection_run_rows_page($after_key: String) {
    match {
        $run: BenchmarkRun
        $run.invocation_id > $after_key
    }
    return {
        $run.invocation_id as invocation_id
        $run.record_sha256 as record_sha256
        $run.archive_object as archive_object
        $run.archive_pointer as archive_pointer
        $run.session_id as session_id
        $run.invoked_at_unix_ms as invoked_at_unix_ms
        $run.case_id as case_id
        $run.case_digest as case_digest
        $run.package_version as package_version
        $run.source_commit as source_commit
        $run.source_tree_dirty as source_tree_dirty
        $run.build_profile as build_profile
        $run.build_opt_level as build_opt_level
        $run.debug_assertions as debug_assertions
        $run.target_triple as target_triple
        $run.rustc_version as rustc_version
        $run.build_declared_release_lto as build_declared_release_lto
        $run.build_declared_release_codegen_units as build_declared_release_codegen_units
        $run.build_declared_release_strip as build_declared_release_strip
        $run.build_cargo_encoded_rustflags_present as build_cargo_encoded_rustflags_present
        $run.build_release_profile_environment_overrides_supported as build_release_profile_environment_overrides_supported
        $run.build_effective_codegen_options_proved as build_effective_codegen_options_proved
        $run.worker_executable_sha256 as worker_executable_sha256
        $run.sut_fingerprint as sut_fingerprint
        $run.sut_json as sut_json
        $run.machine_fingerprint as machine_fingerprint
        $run.machine_format_version as machine_format_version
        $run.machine_os_name as machine_os_name
        $run.machine_os_version as machine_os_version
        $run.machine_kernel_version as machine_kernel_version
        $run.machine_architecture as machine_architecture
        $run.machine_cpu_model as machine_cpu_model
        $run.machine_logical_cores as machine_logical_cores
        $run.machine_physical_cores as machine_physical_cores
        $run.machine_total_memory_bytes as machine_total_memory_bytes
        $run.machine_resource_control_json as machine_resource_control_json
        $run.machine_scheduling_json as machine_scheduling_json
        $run.machine_resource_limits_json as machine_resource_limits_json
        $run.machine_label as machine_label
        $run.backend_fingerprint as backend_fingerprint
        $run.backend_json as backend_json
        $run.fixture_manifest_sha256 as fixture_manifest_sha256
        $run.fixture_logical_sha256 as fixture_logical_sha256
        $run.fixture_physical_sha256 as fixture_physical_sha256
        $run.acquisition_status as acquisition_status
        $run.claim_eligible as claim_eligible
        $run.terminal_failed_repetition as terminal_failed_repetition
        $run.terminal_stage as terminal_stage
        $run.terminal_code as terminal_code
        $run.requested_repetitions as requested_repetitions
        $run.observed_repetitions as observed_repetitions
        $run.min_us as min_us
        $run.p50_us as p50_us
        $run.max_us as max_us
        $run.p95_us as p95_us
        $run.p95_supported as p95_supported
        $run.wall_evidence as wall_evidence
        $run.floor_multiplier_millis as floor_multiplier_millis
        $run.lance_data_plane_logical_calls_min as lance_data_plane_logical_calls_min
        $run.lance_data_plane_logical_calls_p50 as lance_data_plane_logical_calls_p50
        $run.lance_data_plane_logical_calls_max as lance_data_plane_logical_calls_max
        $run.control_plane_logical_calls_min as control_plane_logical_calls_min
        $run.control_plane_logical_calls_p50 as control_plane_logical_calls_p50
        $run.control_plane_logical_calls_max as control_plane_logical_calls_max
        $run.logical_counts_presence_json as logical_counts_presence_json
        $run.physical_counts_presence_json as physical_counts_presence_json
    }
    order { invocation_id asc }
    limit 100
}
"#;

/// A fixed projection query. Raw GQ text is deliberately not part of the API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectionQuery {
    ListPoints {
        limit: u32,
        after: Option<ProjectionCursorV1>,
    },
    ListRunsForPoint {
        point_id: String,
        limit: u32,
        after: Option<ProjectionCursorV1>,
    },
}

/// Cursor bound to one immutable generation and one fixed query scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum ProjectionCursorV1 {
    Points {
        generation_id: String,
        after_point_id: String,
    },
    RunsForPoint {
        generation_id: String,
        point_id: String,
        after_invocation_id: String,
    },
}

/// One bounded page from an immutable projection generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProjectionPageV1 {
    pub generation_id: String,
    pub rows: Vec<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<ProjectionCursorV1>,
}

impl ProjectionPageV1 {
    /// Compatibility projection for CLI serialization.
    pub fn to_rust_json(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("projection page contains only JSON-native values")
    }

    pub fn num_rows(&self) -> usize {
        self.rows.len()
    }
}

/// Result of publishing or reusing one immutable projection generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProjectionBuildV1 {
    pub format_version: u32,
    pub generation_id: String,
    pub schema_sha256: String,
    pub transform_version: u32,
    pub transform_sha256: String,
    pub inventory_sha256: String,
    pub projected_rows_sha256: String,
    pub record_count: u64,
    pub point_count: u64,
    pub graph_commit_id: String,
    pub reused: bool,
}

/// Stable, machine-readable projection failure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProjectionError {
    pub code: &'static str,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_path_lossy"
    )]
    pub path: Option<PathBuf>,
    pub message: String,
}

fn serialize_optional_path_lossy<S>(
    path: &Option<PathBuf>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    path.as_ref()
        .map(|path| path.to_string_lossy())
        .serialize(serializer)
}

impl ProjectionError {
    fn new(code: &'static str, path: Option<&Path>, message: impl Into<String>) -> Self {
        Self {
            code,
            path: path.map(Path::to_path_buf),
            message: message.into(),
        }
    }

    fn io(code: &'static str, path: &Path, error: std::io::Error) -> Self {
        Self::new(code, Some(path), error.to_string())
    }
}

impl Display for ProjectionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(path) = &self.path {
            write!(
                formatter,
                "{} at {}: {}",
                self.code,
                path.display(),
                self.message
            )
        } else {
            write!(formatter, "{}: {}", self.code, self.message)
        }
    }
}

impl Error for ProjectionError {}

fn projection_archive_error(error: ArchiveError) -> ProjectionError {
    ProjectionError::new(
        "projection_archive_invalid",
        error.path.as_deref(),
        format!("{}: {}", error.code, error.message),
    )
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CurrentPointerV1 {
    format_version: u32,
    generation_id: String,
    manifest_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectionManifestV1 {
    format_version: u32,
    generation_id: String,
    schema_sha256: String,
    transform_version: u32,
    transform_sha256: String,
    inventory_sha256: String,
    projected_rows_sha256: String,
    record_count: u64,
    point_count: u64,
    graph_commit_id: String,
    graph_relative_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct InventoryEntry {
    invocation_id: String,
    record_sha256: String,
    point_id: String,
}

struct InventoryScan {
    inventory_sha256: String,
    count: u64,
    head: String,
}

struct ProjectedRowsScan {
    projected_rows_sha256: String,
    run_count: u64,
    point_count: u64,
    head: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PointRow {
    point_id: String,
    point_name: String,
    point_identity_version: u32,
    scenario: String,
    run_spec_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RunRow {
    invocation_id: String,
    record_sha256: String,
    archive_object: String,
    archive_pointer: String,
    session_id: String,
    invoked_at_unix_ms: u64,
    case_id: String,
    case_digest: String,
    package_version: String,
    source_commit: String,
    source_tree_dirty: bool,
    build_profile: String,
    build_opt_level: String,
    debug_assertions: bool,
    target_triple: String,
    rustc_version: String,
    build_declared_release_lto: String,
    build_declared_release_codegen_units: u32,
    build_declared_release_strip: bool,
    build_cargo_encoded_rustflags_present: bool,
    build_release_profile_environment_overrides_supported: bool,
    build_effective_codegen_options_proved: bool,
    worker_executable_sha256: String,
    sut_fingerprint: String,
    sut_json: String,
    machine_fingerprint: String,
    machine_format_version: u32,
    machine_os_name: String,
    machine_os_version: String,
    machine_kernel_version: String,
    machine_architecture: String,
    machine_cpu_model: String,
    machine_logical_cores: u32,
    machine_physical_cores: u32,
    machine_total_memory_bytes: u64,
    machine_resource_control_json: String,
    machine_scheduling_json: String,
    machine_resource_limits_json: String,
    machine_label: String,
    backend_fingerprint: String,
    backend_json: String,
    fixture_manifest_sha256: String,
    fixture_logical_sha256: String,
    fixture_physical_sha256: String,
    acquisition_status: String,
    claim_eligible: bool,
    terminal_failed_repetition: Option<u32>,
    terminal_stage: Option<String>,
    terminal_code: Option<String>,
    requested_repetitions: u32,
    observed_repetitions: u32,
    min_us: u64,
    p50_us: u64,
    max_us: u64,
    p95_us: Option<u64>,
    p95_supported: bool,
    wall_evidence: String,
    floor_multiplier_millis: u32,
    lance_data_plane_logical_calls_min: u64,
    lance_data_plane_logical_calls_p50: u64,
    lance_data_plane_logical_calls_max: u64,
    control_plane_logical_calls_min: u64,
    control_plane_logical_calls_p50: u64,
    control_plane_logical_calls_max: u64,
    logical_counts_presence_json: String,
    physical_counts_presence_json: String,
}

#[derive(Serialize)]
struct NodeEnvelope<'a, T> {
    #[serde(rename = "type")]
    node_type: &'a str,
    data: T,
}

#[derive(Serialize)]
struct EdgeEnvelope<'a, T> {
    edge: &'a str,
    from: &'a str,
    to: &'a str,
    data: T,
}

#[derive(Serialize)]
struct EdgeIdentity {
    id: String,
}

#[derive(Debug)]
struct ValidatedInventory {
    entries: Vec<InventoryEntry>,
    points: BTreeMap<String, PointRow>,
    inventory_sha256: String,
    projected_rows_sha256: String,
    generation_id: String,
}

struct ProjectionRebuildLock {
    #[cfg(unix)]
    root_path: PathBuf,
    #[cfg(unix)]
    root: Flock<File>,
}

impl ProjectionRebuildLock {
    #[cfg(unix)]
    fn validate(&self) -> Result<(), ProjectionError> {
        if rebuild_lock_path_matches_descriptor(&self.root_path, &self.root)? {
            return Ok(());
        }
        Err(ProjectionError::new(
            "projection_rebuild_lock_replaced",
            Some(&self.root_path),
            "projection root was replaced after its rebuild lock was acquired",
        ))
    }

    #[cfg(unix)]
    fn root_file(&self) -> &File {
        &self.root
    }

    #[cfg(not(unix))]
    fn validate(&self) -> Result<(), ProjectionError> {
        Err(ProjectionError::new(
            "projection_rebuild_lock_unsupported",
            None,
            "projection rebuild publication requires Unix flock semantics",
        ))
    }
}

async fn acquire_rebuild_lock(
    projection_root: &Path,
) -> Result<ProjectionRebuildLock, ProjectionError> {
    #[cfg(unix)]
    {
        let root_path = projection_root.to_path_buf();
        let task_path = root_path.clone();
        let lock = tokio::task::spawn_blocking(move || {
            acquire_rebuild_lock_blocking(
                &task_path,
                REBUILD_LOCK_TIMEOUT,
                REBUILD_LOCK_RETRY_INTERVAL,
            )
        })
        .await
        .map_err(|error| {
            ProjectionError::new(
                "projection_rebuild_lock_task_failed",
                Some(&root_path),
                error.to_string(),
            )
        })??;
        Ok(ProjectionRebuildLock {
            root_path,
            root: lock,
        })
    }

    #[cfg(not(unix))]
    {
        Err(ProjectionError::new(
            "projection_rebuild_lock_unsupported",
            Some(projection_root),
            "projection rebuild publication requires Unix flock semantics",
        ))
    }
}

#[cfg(unix)]
fn acquire_rebuild_lock_blocking(
    projection_root: &Path,
    timeout: Duration,
    retry_interval: Duration,
) -> Result<Flock<File>, ProjectionError> {
    acquire_rebuild_lock_blocking_with_open_observer(
        projection_root,
        timeout,
        retry_interval,
        |_, _| Ok(()),
    )
}

#[cfg(unix)]
fn acquire_rebuild_lock_blocking_with_open_observer(
    projection_root: &Path,
    timeout: Duration,
    retry_interval: Duration,
    mut opened: impl FnMut(usize, &File) -> Result<(), ProjectionError>,
) -> Result<Flock<File>, ProjectionError> {
    let started = Instant::now();
    let mut open_attempt = 0usize;
    loop {
        let mut file =
            open_directory_no_follow(projection_root, "projection_rebuild_lock_open_failed")?;
        if !file
            .metadata()
            .map_err(|error| {
                ProjectionError::io(
                    "projection_rebuild_lock_inspection_failed",
                    projection_root,
                    error,
                )
            })?
            .file_type()
            .is_dir()
        {
            return Err(ProjectionError::new(
                "projection_rebuild_lock_invalid",
                Some(projection_root),
                "rebuild lock descriptor is not the projection root directory",
            ));
        }
        open_attempt = open_attempt.checked_add(1).ok_or_else(|| {
            ProjectionError::new(
                "projection_rebuild_lock_attempt_overflow",
                Some(projection_root),
                "rebuild lock open attempt count overflowed",
            )
        })?;
        opened(open_attempt, &file)?;

        loop {
            match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
                Ok(lock) => {
                    if rebuild_lock_path_matches_descriptor(projection_root, &lock)? {
                        return Ok(lock);
                    }
                    // The pathname was unlinked or replaced after this
                    // descriptor opened. Never enter the publication critical
                    // section on the orphaned inode: unlock it, wait within the
                    // original deadline, then open and lock the current path.
                    drop(lock);
                    wait_for_rebuild_lock_retry(projection_root, started, timeout, retry_interval)?;
                    break;
                }
                Err((returned, error)) if error == nix::errno::Errno::EWOULDBLOCK => {
                    file = returned;
                    wait_for_rebuild_lock_retry(projection_root, started, timeout, retry_interval)?;
                }
                Err((_, error)) => {
                    return Err(ProjectionError::new(
                        "projection_rebuild_lock_failed",
                        Some(projection_root),
                        error.to_string(),
                    ));
                }
            }
        }
    }
}

#[cfg(unix)]
fn rebuild_lock_path_matches_descriptor(
    projection_root: &Path,
    file: &File,
) -> Result<bool, ProjectionError> {
    let descriptor = file.metadata().map_err(|error| {
        ProjectionError::io(
            "projection_rebuild_lock_inspection_failed",
            projection_root,
            error,
        )
    })?;
    if !descriptor.file_type().is_dir() {
        return Err(ProjectionError::new(
            "projection_rebuild_lock_invalid",
            Some(projection_root),
            "locked rebuild descriptor is not the projection root directory",
        ));
    }
    let current = open_directory_no_follow(
        projection_root,
        "projection_rebuild_lock_revalidation_failed",
    )?;
    let path_metadata = current.metadata().map_err(|error| {
        ProjectionError::io(
            "projection_rebuild_lock_revalidation_failed",
            projection_root,
            error,
        )
    })?;
    Ok(descriptor.dev() == path_metadata.dev() && descriptor.ino() == path_metadata.ino())
}

#[cfg(unix)]
fn wait_for_rebuild_lock_retry(
    lock_path: &Path,
    started: Instant,
    timeout: Duration,
    retry_interval: Duration,
) -> Result<(), ProjectionError> {
    let elapsed = started.elapsed();
    if elapsed >= timeout {
        return Err(ProjectionError::new(
            "projection_rebuild_lock_timeout",
            Some(lock_path),
            format!(
                "could not acquire the publication lock on a stable pathname within {} ms",
                timeout.as_millis()
            ),
        ));
    }
    let delay = retry_interval.min(timeout.saturating_sub(elapsed));
    if delay.is_zero() {
        thread::yield_now();
    } else {
        thread::sleep(delay);
    }
    Ok(())
}

async fn reconcile_generation_directory(
    generations_root: &Path,
) -> Result<BTreeSet<String>, ProjectionError> {
    let generations_root = generations_root.to_path_buf();
    let task_root = generations_root.clone();
    tokio::task::spawn_blocking(move || reconcile_generation_directory_blocking(&task_root))
        .await
        .map_err(|error| {
            ProjectionError::new(
                "projection_generation_reconcile_task_failed",
                Some(&generations_root),
                error.to_string(),
            )
        })?
}

fn reconcile_generation_directory_blocking(
    generations_root: &Path,
) -> Result<BTreeSet<String>, ProjectionError> {
    let mut retained = BTreeSet::new();
    let mut removed_staging = false;
    let mut entries = 0usize;
    for entry in fs::read_dir(generations_root).map_err(|error| {
        ProjectionError::io(
            "projection_generation_directory_read_failed",
            generations_root,
            error,
        )
    })? {
        let entry = entry.map_err(|error| {
            ProjectionError::io(
                "projection_generation_entry_read_failed",
                generations_root,
                error,
            )
        })?;
        entries = entries.checked_add(1).ok_or_else(|| {
            ProjectionError::new(
                "projection_generation_directory_unbounded",
                Some(generations_root),
                "generation entry count overflowed",
            )
        })?;
        if entries > MAX_GENERATION_DIRECTORY_ENTRIES {
            return Err(ProjectionError::new(
                "projection_generation_directory_unbounded",
                Some(generations_root),
                format!(
                    "generation directory exceeds {MAX_GENERATION_DIRECTORY_ENTRIES} entries; remove the disposable projection root and rebuild it from the JSON archive"
                ),
            ));
        }
        let path = entry.path();
        let name = entry.file_name().into_string().map_err(|_| {
            ProjectionError::new(
                "projection_generation_layout_invalid",
                Some(&path),
                "generation entry name is not UTF-8",
            )
        })?;
        let file_type = entry.file_type().map_err(|error| {
            ProjectionError::io("projection_generation_entry_invalid", &path, error)
        })?;
        if name.starts_with(".build-") {
            if file_type.is_symlink() || !file_type.is_dir() {
                return Err(ProjectionError::new(
                    "projection_generation_layout_invalid",
                    Some(&path),
                    "stale build entry must be a real directory",
                ));
            }
            fs::remove_dir_all(&path).map_err(|error| {
                ProjectionError::io("projection_stale_build_cleanup_failed", &path, error)
            })?;
            removed_staging = true;
            continue;
        }
        if file_type.is_symlink()
            || !file_type.is_dir()
            || name.len() != 64
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(ProjectionError::new(
                "projection_generation_layout_invalid",
                Some(&path),
                "generation entries must be real lowercase SHA-256 directories",
            ));
        }
        retained.insert(name);
    }
    if removed_staging {
        sync_directory(generations_root)?;
    }
    Ok(retained)
}

fn enforce_generation_retention(
    retained: &BTreeSet<String>,
    requested_generation: &str,
    projection_root: &Path,
) -> Result<(), ProjectionError> {
    let would_add = !retained.contains(requested_generation);
    if retained.len() > MAX_RETAINED_GENERATIONS
        || (would_add && retained.len() == MAX_RETAINED_GENERATIONS)
    {
        return Err(ProjectionError::new(
            "projection_generation_retention_exceeded",
            Some(projection_root),
            format!(
                "projection root retains {} immutable generation(s); the maximum is {MAX_RETAINED_GENERATIONS}. Published generations are never deleted automatically because readers may still use them; remove the disposable projection root and rebuild it from the JSON archive",
                retained.len()
            ),
        ));
    }
    Ok(())
}

/// Rebuild a projection from the immutable archive and atomically publish it.
///
/// An identical archive inventory reuses an already verified generation. No
/// archive path is ever created, removed, or modified by this function.
pub async fn rebuild_projection(
    archive_root: &Path,
    projection_root: &Path,
) -> Result<ProjectionBuildV1, ProjectionError> {
    let archive_root = require_root_directory(archive_root, "projection_archive_root_invalid")?;
    let prospective_projection_root = resolve_prospective_path(projection_root)?;
    reject_overlapping_roots(&archive_root, &prospective_projection_root)?;

    ensure_real_directory(
        &prospective_projection_root,
        "projection_root_create_failed",
    )?;
    let projection_root =
        require_real_directory(&prospective_projection_root, "projection_root_invalid")?;
    reject_overlapping_roots(&archive_root, &projection_root)?;
    let generations_root = projection_root.join(GENERATIONS_DIRECTORY);
    ensure_real_directory(
        &generations_root,
        "projection_generation_root_create_failed",
    )?;

    // The lock covers archive snapshot acquisition through CURRENT publication.
    // A later rebuild therefore cannot publish an older snapshot after a newer
    // one, even when two processes started in the opposite order.
    let rebuild_lock = acquire_rebuild_lock(&projection_root).await?;
    rebuild_lock.validate()?;
    let retained_generations = reconcile_generation_directory(&generations_root).await?;

    let records = iter_archive(&archive_root).map_err(projection_archive_error)?;
    if records.remaining() > MAX_PROJECTED_RECORDS {
        return Err(ProjectionError::new(
            "projection_inventory_too_large",
            Some(&archive_root),
            format!(
                "archive has {} records; maximum is {MAX_PROJECTED_RECORDS}",
                records.remaining()
            ),
        ));
    }
    // Both passes share one fixed invocation-id snapshot. Archive objects and
    // pointers are immutable, so a concurrent append cannot enter only one
    // pass and a single record is the largest telemetry object retained.
    let load_records = records.clone();
    let inventory = validate_inventory(records)?;

    let record_count = u64::try_from(inventory.entries.len()).map_err(|_| {
        ProjectionError::new(
            "projection_count_overflow",
            None,
            "record count does not fit u64",
        )
    })?;
    let point_count = u64::try_from(inventory.points.len()).map_err(|_| {
        ProjectionError::new(
            "projection_count_overflow",
            None,
            "point count does not fit u64",
        )
    })?;
    let final_generation = generations_root.join(&inventory.generation_id);
    enforce_generation_retention(
        &retained_generations,
        &inventory.generation_id,
        &projection_root,
    )?;

    if retained_generations.contains(&inventory.generation_id) {
        let manifest = verify_generation(
            &final_generation,
            Some(&inventory.entries),
            Some(record_count),
            Some(point_count),
            Some(&inventory.projected_rows_sha256),
        )
        .await?;
        // A generation can exist after its rename reached the filesystem but
        // before its parent-directory sync survived a crash. Reuse heals that
        // durability edge before CURRENT can name the generation.
        sync_directory(&generations_root)?;
        publish_current(&projection_root, &manifest, &rebuild_lock)?;
        return Ok(build_result(&manifest, true));
    }

    let staging = tempfile::Builder::new()
        .prefix(".build-")
        .tempdir_in(&generations_root)
        .map_err(|error| {
            ProjectionError::io("projection_staging_create_failed", &generations_root, error)
        })?;
    let graph_path = staging.path().join(GRAPH_DIRECTORY);
    let graph_uri = path_to_utf8(&graph_path)?;
    let db = Omnigraph::init(graph_uri, PROJECTION_SCHEMA)
        .await
        .map_err(|error| {
            ProjectionError::new(
                "projection_graph_init_failed",
                Some(&graph_path),
                error.to_string(),
            )
        })?;

    load_points(&db, inventory.points.values()).await?;
    load_runs(&db, load_records).await?;
    let graph_commit_id = verify_graph_inventory(
        &db,
        &inventory.entries,
        &inventory.projected_rows_sha256,
        record_count,
        point_count,
    )
    .await?;
    drop(db);

    let manifest = ProjectionManifestV1 {
        format_version: PROJECTION_FORMAT_VERSION,
        generation_id: inventory.generation_id.clone(),
        schema_sha256: schema_sha256(),
        transform_version: PROJECTION_TRANSFORM_VERSION,
        transform_sha256: transform_sha256(),
        inventory_sha256: inventory.inventory_sha256.clone(),
        projected_rows_sha256: inventory.projected_rows_sha256.clone(),
        record_count,
        point_count,
        graph_commit_id,
        graph_relative_path: GRAPH_DIRECTORY.to_string(),
    };
    write_new_manifest(staging.path(), &manifest)?;
    sync_directory(staging.path())?;

    rebuild_lock.validate()?;
    let (installed, reused) = match fs::rename(staging.path(), &final_generation) {
        Ok(()) => {
            sync_directory(&generations_root)?;
            let installed = verify_generation(
                &final_generation,
                Some(&inventory.entries),
                Some(record_count),
                Some(point_count),
                Some(&inventory.projected_rows_sha256),
            )
            .await?;
            (installed, false)
        }
        Err(error) => {
            if symlink_metadata_if_present(
                &final_generation,
                "projection_generation_inspection_failed",
            )?
            .is_none()
            {
                return Err(ProjectionError::io(
                    "projection_generation_publish_failed",
                    &final_generation,
                    error,
                ));
            }

            // A concurrent rebuild or a prior crash may have installed the
            // same deterministic logical generation even though this rename
            // did not report success. Verify that installed generation once
            // and retain the exact manifest that passed verification for
            // CURRENT publication below.
            let installed = verify_generation(
                &final_generation,
                Some(&inventory.entries),
                Some(record_count),
                Some(point_count),
                Some(&inventory.projected_rows_sha256),
            )
            .await?;
            (installed, true)
        }
    };
    if reused {
        sync_directory(&generations_root)?;
    }
    publish_current(&projection_root, &installed, &rebuild_lock)?;
    Ok(build_result(&installed, reused))
}

/// Execute one fixed named query against the currently published generation.
pub async fn query_projection(
    projection_root: &Path,
    query: ProjectionQuery,
) -> Result<ProjectionPageV1, ProjectionError> {
    let projection_root = require_root_directory(projection_root, "projection_root_invalid")?;
    let mut params = ParamMap::new();
    let (query_name, limit, requested_generation, after_key) = match &query {
        ProjectionQuery::ListPoints { limit, after } => {
            validate_page_limit(*limit)?;
            let (generation, after_key) = match after {
                None => (None, String::new()),
                Some(ProjectionCursorV1::Points {
                    generation_id,
                    after_point_id,
                }) => {
                    require_sha256(generation_id, "cursor generation id")?;
                    require_sha256(after_point_id, "cursor point id")?;
                    (Some(generation_id.clone()), after_point_id.clone())
                }
                Some(ProjectionCursorV1::RunsForPoint { .. }) => {
                    return Err(ProjectionError::new(
                        "projection_cursor_scope_mismatch",
                        None,
                        "a runs-for-point cursor cannot continue a points query",
                    ));
                }
            };
            params.insert("after_key".to_string(), Literal::String(after_key.clone()));
            ("list_points_page", *limit, generation, after_key)
        }
        ProjectionQuery::ListRunsForPoint {
            point_id,
            limit,
            after,
        } => {
            validate_page_limit(*limit)?;
            require_sha256(point_id, "point id")?;
            let (generation, after_key) = match after {
                None => (None, String::new()),
                Some(ProjectionCursorV1::RunsForPoint {
                    generation_id,
                    point_id: cursor_point_id,
                    after_invocation_id,
                }) => {
                    require_sha256(generation_id, "cursor generation id")?;
                    require_sha256(cursor_point_id, "cursor point id")?;
                    require_invocation_id(after_invocation_id, "cursor invocation id")?;
                    if cursor_point_id != point_id {
                        return Err(ProjectionError::new(
                            "projection_cursor_scope_mismatch",
                            None,
                            "runs-for-point cursor belongs to a different point id",
                        ));
                    }
                    (Some(generation_id.clone()), after_invocation_id.clone())
                }
                Some(ProjectionCursorV1::Points { .. }) => {
                    return Err(ProjectionError::new(
                        "projection_cursor_scope_mismatch",
                        None,
                        "a points cursor cannot continue a runs-for-point query",
                    ));
                }
            };
            params.insert("point_id".to_string(), Literal::String(point_id.clone()));
            params.insert("after_key".to_string(), Literal::String(after_key.clone()));
            ("list_runs_for_point_page", *limit, generation, after_key)
        }
    };
    let (db, manifest) = match requested_generation {
        Some(generation_id) => open_generation_by_id(&projection_root, &generation_id).await?,
        None => open_current_generation(&projection_root).await?,
    };
    let (result, head) = db
        .query_with_head(
            ReadTarget::branch("main"),
            PROJECTION_QUERIES,
            query_name,
            &params,
        )
        .await
        .map_err(|error| {
            ProjectionError::new(
                "projection_query_failed",
                Some(&projection_root),
                error.to_string(),
            )
        })?;
    require_expected_head(head, &manifest, &projection_root)?;
    page_from_query_result(result, &manifest.generation_id, limit, &after_key, &query)
}

/// Return the first bounded page of projected points.
pub async fn list_points(projection_root: &Path) -> Result<ProjectionPageV1, ProjectionError> {
    list_points_page(projection_root, DEFAULT_PROJECTION_PAGE_SIZE, None).await
}

/// Return one bounded point page, continuing the cursor's immutable generation.
pub async fn list_points_page(
    projection_root: &Path,
    limit: u32,
    after: Option<ProjectionCursorV1>,
) -> Result<ProjectionPageV1, ProjectionError> {
    query_projection(
        projection_root,
        ProjectionQuery::ListPoints { limit, after },
    )
    .await
}

/// Return the first bounded page of runs for one exact point identity.
pub async fn list_runs_for_point(
    projection_root: &Path,
    point_id: impl Into<String>,
) -> Result<ProjectionPageV1, ProjectionError> {
    list_runs_for_point_page(
        projection_root,
        point_id,
        DEFAULT_PROJECTION_PAGE_SIZE,
        None,
    )
    .await
}

/// Return one bounded run page, continuing the cursor's immutable generation.
pub async fn list_runs_for_point_page(
    projection_root: &Path,
    point_id: impl Into<String>,
    limit: u32,
    after: Option<ProjectionCursorV1>,
) -> Result<ProjectionPageV1, ProjectionError> {
    query_projection(
        projection_root,
        ProjectionQuery::ListRunsForPoint {
            point_id: point_id.into(),
            limit,
            after,
        },
    )
    .await
}

fn validate_page_limit(limit: u32) -> Result<(), ProjectionError> {
    if !(1..=MAX_PROJECTION_PAGE_SIZE).contains(&limit) {
        return Err(ProjectionError::new(
            "projection_page_limit_invalid",
            None,
            format!(
                "projection page limit must be in 1..={MAX_PROJECTION_PAGE_SIZE}, observed {limit}"
            ),
        ));
    }
    Ok(())
}

fn page_from_query_result(
    result: QueryResult,
    generation_id: &str,
    limit: u32,
    exclusive_after: &str,
    query: &ProjectionQuery,
) -> Result<ProjectionPageV1, ProjectionError> {
    let mut rows = match result.to_rust_json() {
        serde_json::Value::Array(rows) => rows,
        _ => {
            return Err(ProjectionError::new(
                "projection_page_result_invalid",
                None,
                "projection page query did not return a JSON array",
            ));
        }
    };
    if rows.len() > QUERY_FETCH_LIMIT {
        return Err(ProjectionError::new(
            "projection_page_result_unbounded",
            None,
            format!(
                "projection query returned {} rows; internal ceiling is {QUERY_FETCH_LIMIT}",
                rows.len()
            ),
        ));
    }

    let (key_field, expected_point_id) = match query {
        ProjectionQuery::ListPoints { .. } => ("point_id", None),
        ProjectionQuery::ListRunsForPoint { point_id, .. } => {
            ("invocation_id", Some(point_id.as_str()))
        }
    };
    let mut previous = exclusive_after;
    for (index, row) in rows.iter().enumerate() {
        validate_projected_json_row_size(row, "public query", index)?;
        let object = row.as_object().ok_or_else(|| {
            ProjectionError::new(
                "projection_page_result_invalid",
                None,
                format!("projection row {index} is not an object"),
            )
        })?;
        let key = object
            .get(key_field)
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                ProjectionError::new(
                    "projection_page_result_invalid",
                    None,
                    format!("projection row {index} has no string {key_field}"),
                )
            })?;
        if key <= previous {
            return Err(ProjectionError::new(
                "projection_page_order_invalid",
                None,
                format!("projection row {index} key {key:?} is not strictly after {previous:?}"),
            ));
        }
        match query {
            ProjectionQuery::ListPoints { .. } => require_sha256(key, "projected point id")?,
            ProjectionQuery::ListRunsForPoint { .. } => {
                require_invocation_id(key, "projected invocation id")?;
                if object.get("point_id").and_then(serde_json::Value::as_str) != expected_point_id {
                    return Err(ProjectionError::new(
                        "projection_page_scope_invalid",
                        None,
                        format!("projection row {index} belongs to a different point"),
                    ));
                }
            }
        }
        previous = key;
    }

    let limit = usize::try_from(limit).map_err(|_| {
        ProjectionError::new(
            "projection_page_limit_invalid",
            None,
            "projection page limit does not fit usize",
        )
    })?;
    let has_more = rows.len() > limit;
    rows.truncate(limit);
    let next_cursor = if has_more {
        let last = rows.last().ok_or_else(|| {
            ProjectionError::new(
                "projection_page_result_invalid",
                None,
                "nonempty continuation page unexpectedly returned no rows",
            )
        })?;
        let key = last
            .get(key_field)
            .and_then(serde_json::Value::as_str)
            .expect("page row key was validated above")
            .to_string();
        Some(match query {
            ProjectionQuery::ListPoints { .. } => ProjectionCursorV1::Points {
                generation_id: generation_id.to_string(),
                after_point_id: key,
            },
            ProjectionQuery::ListRunsForPoint { point_id, .. } => {
                ProjectionCursorV1::RunsForPoint {
                    generation_id: generation_id.to_string(),
                    point_id: point_id.clone(),
                    after_invocation_id: key,
                }
            }
        })
    } else {
        None
    };
    Ok(ProjectionPageV1 {
        generation_id: generation_id.to_string(),
        rows,
        next_cursor,
    })
}

fn validate_inventory(records: ArchiveRecordIter) -> Result<ValidatedInventory, ProjectionError> {
    let mut digests = BTreeMap::<String, String>::new();
    let mut points = BTreeMap::<String, PointRow>::new();
    let mut entries = Vec::with_capacity(records.remaining().min(MAX_PROJECTED_RECORDS));
    let mut retained = RetainedMetadataBudget::default();
    let mut previous_invocation = None::<String>;
    let mut run_rows_digest = projected_table_digest("BenchmarkRun");

    for archived in records {
        if entries.len() >= MAX_PROJECTED_RECORDS {
            return Err(ProjectionError::new(
                "projection_inventory_too_large",
                None,
                format!("archive exceeds {MAX_PROJECTED_RECORDS} records"),
            ));
        }
        let archived = archived.map_err(projection_archive_error)?;
        let record = &archived.record;
        let receipt = &archived.receipt;
        if receipt.archive_format_version != ARCHIVE_FORMAT_VERSION {
            return Err(ProjectionError::new(
                "projection_archive_version_unsupported",
                None,
                format!(
                    "receipt version {} is unsupported; expected {ARCHIVE_FORMAT_VERSION}",
                    receipt.archive_format_version
                ),
            ));
        }
        if receipt.invocation_id != record.invocation.invocation_id {
            return Err(ProjectionError::new(
                "projection_invocation_mismatch",
                None,
                format!(
                    "receipt invocation {} differs from record invocation {}",
                    receipt.invocation_id, record.invocation.invocation_id
                ),
            ));
        }
        require_sha256(&receipt.record_sha256, "record digest")?;
        validate_relative_archive_path(&receipt.object_relative_path, "archive object")?;
        validate_relative_archive_path(&receipt.pointer_relative_path, "archive pointer")?;

        require_invocation_id(&record.invocation.invocation_id, "record invocation id")?;
        if previous_invocation
            .as_ref()
            .is_some_and(|previous| previous >= &record.invocation.invocation_id)
        {
            return Err(ProjectionError::new(
                "projection_inventory_order_invalid",
                None,
                format!(
                    "archive invocation {} is not strictly after {:?}",
                    record.invocation.invocation_id, previous_invocation
                ),
            ));
        }
        previous_invocation = Some(record.invocation.invocation_id.clone());

        if let Some(previous) = digests.insert(
            receipt.record_sha256.clone(),
            record.invocation.invocation_id.clone(),
        ) {
            return Err(ProjectionError::new(
                "projection_record_digest_collision",
                None,
                format!(
                    "record digest {} appears for invocations {previous} and {}",
                    receipt.record_sha256, record.invocation.invocation_id
                ),
            ));
        }
        retained.add(
            std::mem::size_of::<(String, String)>()
                .saturating_add(receipt.record_sha256.len())
                .saturating_add(record.invocation.invocation_id.len()),
            "record digest index",
        )?;

        let point = point_row(record)?;
        validate_projected_row_size(&point, "BenchmarkPoint")?;
        match points.get(&point.point_id) {
            Some(previous) if previous != &point => {
                return Err(ProjectionError::new(
                    "projection_point_id_collision",
                    None,
                    format!(
                        "point id {} resolves to conflicting typed identities",
                        point.point_id
                    ),
                ));
            }
            Some(_) => {}
            None => {
                retained.add(retained_point_row_bytes(&point), "point row index")?;
                points.insert(point.point_id.clone(), point);
            }
        }

        let entry = InventoryEntry {
            invocation_id: record.invocation.invocation_id.clone(),
            record_sha256: receipt.record_sha256.clone(),
            point_id: record.run.point_id.clone(),
        };
        let entry_bytes = retained_inventory_entry_bytes(&entry);
        if entry_bytes > MAX_INVENTORY_ENTRY_BYTES {
            return Err(ProjectionError::new(
                "projection_inventory_entry_too_large",
                None,
                format!(
                    "inventory entry accounts for {entry_bytes} bytes; maximum is {MAX_INVENTORY_ENTRY_BYTES}"
                ),
            ));
        }
        retained.add(entry_bytes, "inventory entries")?;
        entries.push(entry);

        let run = run_row(record, receipt)?;
        digest_projected_row(&mut run_rows_digest, &run, "BenchmarkRun")?;
    }
    entries.sort();
    let inventory_sha256 = inventory_sha256(&entries);
    let mut point_rows_digest = projected_table_digest("BenchmarkPoint");
    for point in points.values() {
        digest_projected_row(&mut point_rows_digest, point, "BenchmarkPoint")?;
    }
    let projected_rows_sha256 = projected_rows_sha256(
        point_rows_digest,
        points.len(),
        run_rows_digest,
        entries.len(),
    )?;
    let schema_sha256 = schema_sha256();
    let transform_sha256 = transform_sha256();
    let generation_id = generation_id(
        &schema_sha256,
        &transform_sha256,
        &inventory_sha256,
        &projected_rows_sha256,
    );
    Ok(ValidatedInventory {
        entries,
        points,
        inventory_sha256,
        projected_rows_sha256,
        generation_id,
    })
}

fn point_row(record: &RunRecordV1) -> Result<PointRow, ProjectionError> {
    require_sha256(&record.run.point_id, "point id")?;
    let run_spec_json = canonical_json(&record.run.run_spec, "run spec")?;
    Ok(PointRow {
        point_id: record.run.point_id.clone(),
        point_name: record.run.point_name.clone(),
        point_identity_version: record.run.point_identity_version,
        scenario: json_string(&record.run.run_spec.scenario, "scenario")?,
        run_spec_json,
    })
}

fn run_row(
    record: &RunRecordV1,
    receipt: &crate::archive::ArchiveReceiptV1,
) -> Result<RunRow, ProjectionError> {
    let machine = &record.machine;
    let wall = &record.measurements.wall_clock;
    let calls = logical_call_summaries(record)?;
    Ok(RunRow {
        invocation_id: record.invocation.invocation_id.clone(),
        record_sha256: receipt.record_sha256.clone(),
        archive_object: receipt.object_relative_path.clone(),
        archive_pointer: receipt.pointer_relative_path.clone(),
        session_id: record.invocation.session_id.clone(),
        invoked_at_unix_ms: record.invocation.invoked_at_unix_ms,
        case_id: record.run.case_id.clone(),
        case_digest: record.run.case_digest.clone(),
        package_version: record.sut.package_version.clone(),
        source_commit: record.sut.source_commit.clone(),
        source_tree_dirty: record.sut.source_tree_dirty,
        build_profile: record.sut.build.profile.clone(),
        build_opt_level: record.sut.build.cargo_opt_level.clone(),
        debug_assertions: record.sut.build.debug_assertions,
        target_triple: record.sut.build.target_triple.clone(),
        rustc_version: record.sut.build.rustc_version.clone(),
        build_declared_release_lto: record.sut.build.declared_release_lto.clone(),
        build_declared_release_codegen_units: record.sut.build.declared_release_codegen_units,
        build_declared_release_strip: record.sut.build.declared_release_strip,
        build_cargo_encoded_rustflags_present: record.sut.build.cargo_encoded_rustflags_present,
        build_release_profile_environment_overrides_supported: record
            .sut
            .build
            .release_profile_environment_overrides_supported,
        build_effective_codegen_options_proved: record.sut.build.effective_codegen_options_proved,
        worker_executable_sha256: record.sut.build.worker_executable_sha256.clone(),
        sut_fingerprint: typed_json_sha256(&record.sut, "system under test")?,
        sut_json: canonical_json(&record.sut, "system under test")?,
        machine_fingerprint: typed_json_sha256(machine, "machine identity")?,
        machine_format_version: machine.format_version,
        machine_os_name: machine.os_name.clone(),
        machine_os_version: machine.os_version.clone(),
        machine_kernel_version: machine.kernel_version.clone(),
        machine_architecture: machine.architecture.clone(),
        machine_cpu_model: machine.cpu_model.clone(),
        machine_logical_cores: machine.logical_cores,
        machine_physical_cores: machine.physical_cores,
        machine_total_memory_bytes: machine.total_memory_bytes,
        machine_resource_control_json: canonical_json(
            &machine.resource_control,
            "machine resource control",
        )?,
        machine_scheduling_json: canonical_json(&machine.scheduling, "machine scheduling")?,
        machine_resource_limits_json: canonical_json(
            &machine.resource_limits,
            "machine resource limits",
        )?,
        machine_label: machine.machine_label.clone(),
        backend_fingerprint: typed_json_sha256(&record.backend, "backend evidence")?,
        backend_json: canonical_json(&record.backend, "backend evidence")?,
        fixture_manifest_sha256: record.fixture.manifest_sha256.clone(),
        fixture_logical_sha256: record
            .fixture
            .manifest
            .logical
            .logical_content_sha256
            .clone(),
        fixture_physical_sha256: record.fixture.manifest.physical.tree_sha256.clone(),
        acquisition_status: json_string(&record.acquisition.status, "acquisition status")?,
        claim_eligible: record.claim_eligible(),
        terminal_failed_repetition: record
            .acquisition
            .terminal
            .as_ref()
            .map(|terminal| terminal.failed_repetition),
        terminal_stage: record
            .acquisition
            .terminal
            .as_ref()
            .map(|terminal| terminal.stage.as_str().to_string()),
        terminal_code: record
            .acquisition
            .terminal
            .as_ref()
            .map(|terminal| terminal.code.clone()),
        requested_repetitions: record.acquisition.requested_repetitions,
        observed_repetitions: record.acquisition.observed_repetitions,
        min_us: wall.min_us,
        p50_us: wall.p50_us,
        max_us: wall.max_us,
        p95_us: wall.p95_us,
        p95_supported: wall.p95_supported,
        wall_evidence: json_string(&wall.evidence, "wall evidence")?,
        floor_multiplier_millis: record.measurements.claim_policy.floor_multiplier_millis,
        lance_data_plane_logical_calls_min: calls.lance_data_plane.min,
        lance_data_plane_logical_calls_p50: calls.lance_data_plane.p50,
        lance_data_plane_logical_calls_max: calls.lance_data_plane.max,
        control_plane_logical_calls_min: calls.control_plane.min,
        control_plane_logical_calls_p50: calls.control_plane.p50,
        control_plane_logical_calls_max: calls.control_plane.max,
        logical_counts_presence_json: canonical_json(
            &record.measurements.layer_presence.logical.counts,
            "logical counts presence",
        )?,
        physical_counts_presence_json: canonical_json(
            &record.measurements.layer_presence.physical.counts,
            "physical counts presence",
        )?,
    })
}

async fn load_points<'a>(
    db: &Omnigraph,
    points: impl Iterator<Item = &'a PointRow>,
) -> Result<(), ProjectionError> {
    let mut batch = NdjsonBatch::default();
    for point in points {
        let line = serialize_graph_row(&NodeEnvelope {
            node_type: "BenchmarkPoint",
            data: point,
        })?;
        if let Some(body) = batch.push_group(&[line])? {
            load_batch(db, &body).await?;
        }
    }
    if let Some(body) = batch.finish() {
        load_batch(db, &body).await?;
    }
    Ok(())
}

async fn load_runs(db: &Omnigraph, records: ArchiveRecordIter) -> Result<(), ProjectionError> {
    let mut batch = NdjsonBatch::default();
    for archived in records {
        let archived = archived.map_err(projection_archive_error)?;
        let record = &archived.record;
        let run = run_row(record, &archived.receipt)?;
        let run_line = serialize_graph_row(&NodeEnvelope {
            node_type: "BenchmarkRun",
            data: &run,
        })?;
        let edge_line = serialize_graph_row(&EdgeEnvelope {
            edge: "Measures",
            from: &record.invocation.invocation_id,
            to: &record.run.point_id,
            data: EdgeIdentity {
                id: format!("measures:{}", record.invocation.invocation_id),
            },
        })?;
        if let Some(body) = batch.push_group(&[run_line, edge_line])? {
            load_batch(db, &body).await?;
        }
    }
    if let Some(body) = batch.finish() {
        load_batch(db, &body).await?;
    }
    Ok(())
}

async fn load_batch(db: &Omnigraph, body: &str) -> Result<(), ProjectionError> {
    db.load_graph_batch("main", body, LoadMode::Append)
        .await
        .map_err(|error| {
            ProjectionError::new("projection_graph_load_failed", None, error.to_string())
        })?;
    Ok(())
}

async fn verify_generation(
    generation_root: &Path,
    expected_inventory: Option<&[InventoryEntry]>,
    expected_record_count: Option<u64>,
    expected_point_count: Option<u64>,
    expected_projected_rows_sha256: Option<&str>,
) -> Result<ProjectionManifestV1, ProjectionError> {
    let generation_root = require_real_directory(generation_root, "projection_generation_invalid")?;
    let manifest_path = generation_root.join(MANIFEST_FILE);
    let manifest: ProjectionManifestV1 = read_canonical_json(
        &manifest_path,
        MAX_MANIFEST_BYTES,
        "projection_manifest_invalid",
    )?;
    validate_manifest(&manifest, &generation_root)?;
    if expected_record_count.is_some_and(|expected| manifest.record_count != expected)
        || expected_point_count.is_some_and(|expected| manifest.point_count != expected)
        || expected_projected_rows_sha256
            .is_some_and(|expected| manifest.projected_rows_sha256 != expected)
    {
        return Err(ProjectionError::new(
            "projection_generation_collision",
            Some(&manifest_path),
            "generation content differs from the validated archive projection",
        ));
    }

    let graph_path = generation_root.join(&manifest.graph_relative_path);
    let graph_path = require_real_directory(&graph_path, "projection_graph_invalid")?;
    let db = Omnigraph::open_read_only(path_to_utf8(&graph_path)?)
        .await
        .map_err(|error| {
            ProjectionError::new(
                "projection_graph_open_failed",
                Some(&graph_path),
                error.to_string(),
            )
        })?;
    let observed = scan_graph_inventory(
        &db,
        expected_inventory,
        Some(&manifest.graph_commit_id),
        Some(&graph_path),
        "graph inventory differs from the validated archive inventory",
    )
    .await?;
    if observed.inventory_sha256 != manifest.inventory_sha256 {
        return Err(ProjectionError::new(
            "projection_inventory_digest_mismatch",
            Some(&graph_path),
            "graph inventory does not match the generation manifest",
        ));
    }
    if observed.count != manifest.record_count {
        return Err(ProjectionError::new(
            "projection_record_count_mismatch",
            Some(&graph_path),
            format!(
                "manifest declares {} records; graph contains {}",
                manifest.record_count, observed.count
            ),
        ));
    }

    let projected =
        scan_projected_rows(&db, Some(&manifest.graph_commit_id), Some(&graph_path)).await?;
    if projected.head != manifest.graph_commit_id {
        return Err(ProjectionError::new(
            "projection_graph_head_mismatch",
            Some(&graph_path),
            "projected-row scan observed a different graph head",
        ));
    }
    if projected.point_count != manifest.point_count {
        return Err(ProjectionError::new(
            "projection_point_count_mismatch",
            Some(&graph_path),
            format!(
                "manifest declares {} points; graph contains {}",
                manifest.point_count, projected.point_count
            ),
        ));
    }
    if projected.run_count != manifest.record_count {
        return Err(ProjectionError::new(
            "projection_record_count_mismatch",
            Some(&graph_path),
            format!(
                "manifest declares {} runs; graph contains {}",
                manifest.record_count, projected.run_count
            ),
        ));
    }
    if projected.projected_rows_sha256 != manifest.projected_rows_sha256 {
        return Err(ProjectionError::new(
            "projection_projected_rows_digest_mismatch",
            Some(&graph_path),
            "all canonical graph point/run fields do not match the generation manifest",
        ));
    }
    Ok(manifest)
}

async fn verify_graph_inventory(
    db: &Omnigraph,
    expected: &[InventoryEntry],
    expected_projected_rows_sha256: &str,
    expected_record_count: u64,
    expected_point_count: u64,
) -> Result<String, ProjectionError> {
    let inventory = scan_graph_inventory(
        db,
        Some(expected),
        None,
        None,
        "freshly built graph inventory differs from the validated archive inventory",
    )
    .await?;
    let projected = scan_projected_rows(db, Some(&inventory.head), None).await?;
    if projected.projected_rows_sha256 != expected_projected_rows_sha256
        || projected.run_count != expected_record_count
        || projected.point_count != expected_point_count
    {
        return Err(ProjectionError::new(
            "projection_projected_rows_mismatch",
            None,
            "freshly built graph point/run rows differ from canonical archive-derived rows",
        ));
    }
    Ok(inventory.head)
}

async fn scan_graph_inventory(
    db: &Omnigraph,
    expected: Option<&[InventoryEntry]>,
    expected_head: Option<&str>,
    path: Option<&Path>,
    mismatch_message: &str,
) -> Result<InventoryScan, ProjectionError> {
    let mut after_key = String::new();
    let mut observed_head = None;
    let mut digest = Sha256::new();
    digest.update(INVENTORY_DOMAIN);
    let mut count = 0usize;

    loop {
        let mut params = ParamMap::new();
        params.insert("after_key".to_string(), Literal::String(after_key.clone()));
        let (result, head) = db
            .query_with_head(
                ReadTarget::branch("main"),
                PROJECTION_QUERIES,
                "projection_inventory_page",
                &params,
            )
            .await
            .map_err(|error| {
                ProjectionError::new(
                    "projection_verification_query_failed",
                    path,
                    error.to_string(),
                )
            })?;
        observe_scan_head(&mut observed_head, head, expected_head, path)?;
        let page = parse_inventory_page(&result, &after_key)?;
        for entry in &page {
            if count >= MAX_PROJECTED_RECORDS {
                return Err(ProjectionError::new(
                    "projection_inventory_too_large",
                    path,
                    format!("graph inventory exceeds {MAX_PROJECTED_RECORDS} records"),
                ));
            }
            if let Some(expected) = expected
                && expected.get(count) != Some(entry)
            {
                return Err(ProjectionError::new(
                    "projection_inventory_mismatch",
                    path,
                    format!("{mismatch_message} at canonical row {count}"),
                ));
            }
            digest_field(&mut digest, entry.invocation_id.as_bytes());
            digest_field(&mut digest, entry.record_sha256.as_bytes());
            digest_field(&mut digest, entry.point_id.as_bytes());
            count += 1;
        }
        if page.len() < VERIFICATION_PAGE_SIZE {
            break;
        }
        after_key = page
            .last()
            .expect("a full verification page is nonempty")
            .invocation_id
            .clone();
    }

    if expected.is_some_and(|entries| entries.len() != count) {
        return Err(ProjectionError::new(
            "projection_inventory_mismatch",
            path,
            format!("{mismatch_message}: observed {count} row(s)"),
        ));
    }
    let count = u64::try_from(count).map_err(|_| {
        ProjectionError::new(
            "projection_count_overflow",
            path,
            "observed graph inventory does not fit u64",
        )
    })?;
    Ok(InventoryScan {
        inventory_sha256: format!("{:x}", digest.finalize()),
        count,
        head: observed_head.expect("a verification scan always executes at least one query"),
    })
}

fn parse_inventory_page(
    result: &QueryResult,
    exclusive_after: &str,
) -> Result<Vec<InventoryEntry>, ProjectionError> {
    let rows = result.to_rust_json();
    let rows = rows.as_array().ok_or_else(|| {
        ProjectionError::new(
            "projection_inventory_result_invalid",
            None,
            "inventory query did not return a JSON array",
        )
    })?;
    if rows.len() > VERIFICATION_PAGE_SIZE {
        return Err(ProjectionError::new(
            "projection_inventory_result_unbounded",
            None,
            format!(
                "inventory page returned {} rows; maximum is {VERIFICATION_PAGE_SIZE}",
                rows.len()
            ),
        ));
    }
    let mut inventory = Vec::with_capacity(rows.len());
    let mut previous = exclusive_after.to_string();
    for (index, row) in rows.iter().enumerate() {
        validate_projected_json_row_size(row, "inventory verification", index)?;
        let object = row.as_object().ok_or_else(|| {
            ProjectionError::new(
                "projection_inventory_result_invalid",
                None,
                format!("inventory row {index} is not an object"),
            )
        })?;
        let string = |field: &str| -> Result<String, ProjectionError> {
            object
                .get(field)
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned)
                .ok_or_else(|| {
                    ProjectionError::new(
                        "projection_inventory_result_invalid",
                        None,
                        format!("inventory row {index} has no string {field}"),
                    )
                })
        };
        let entry = InventoryEntry {
            invocation_id: string("invocation_id")?,
            record_sha256: string("record_sha256")?,
            point_id: string("point_id")?,
        };
        require_invocation_id(&entry.invocation_id, "projected invocation id")?;
        require_sha256(&entry.record_sha256, "record digest")?;
        require_sha256(&entry.point_id, "point id")?;
        if entry.invocation_id <= previous {
            return Err(ProjectionError::new(
                "projection_inventory_order_invalid",
                None,
                format!(
                    "inventory row {index} invocation {:?} is not strictly after {:?}",
                    entry.invocation_id, previous
                ),
            ));
        }
        previous.clone_from(&entry.invocation_id);
        inventory.push(entry);
    }
    Ok(inventory)
}

async fn scan_projected_rows(
    db: &Omnigraph,
    expected_head: Option<&str>,
    path: Option<&Path>,
) -> Result<ProjectedRowsScan, ProjectionError> {
    let (point_rows_digest, point_count, point_head) =
        scan_projected_point_rows(db, expected_head, path).await?;
    let (run_rows_digest, run_count, run_head) =
        scan_projected_run_rows(db, Some(&point_head), path).await?;
    if run_head != point_head {
        return Err(ProjectionError::new(
            "projection_graph_head_mismatch",
            path,
            "point and run verification scans observed different graph heads",
        ));
    }
    Ok(ProjectedRowsScan {
        projected_rows_sha256: projected_rows_sha256(
            point_rows_digest,
            point_count,
            run_rows_digest,
            run_count,
        )?,
        run_count: usize_to_u64(run_count, path, "observed run count")?,
        point_count: usize_to_u64(point_count, path, "observed point count")?,
        head: point_head,
    })
}

async fn scan_projected_point_rows(
    db: &Omnigraph,
    expected_head: Option<&str>,
    path: Option<&Path>,
) -> Result<(Sha256, usize, String), ProjectionError> {
    let mut after_key = String::new();
    let mut observed_head = None;
    let mut count = 0usize;
    let mut digest = projected_table_digest("BenchmarkPoint");
    loop {
        let mut params = ParamMap::new();
        params.insert("after_key".to_string(), Literal::String(after_key.clone()));
        let (result, head) = db
            .query_with_head(
                ReadTarget::branch("main"),
                PROJECTION_QUERIES,
                "projection_point_rows_page",
                &params,
            )
            .await
            .map_err(|error| {
                ProjectionError::new(
                    "projection_verification_query_failed",
                    path,
                    error.to_string(),
                )
            })?;
        observe_scan_head(&mut observed_head, head, expected_head, path)?;
        let page = parse_projected_point_page(result, &after_key)?;
        for row in &page {
            digest_projected_row(&mut digest, row, "BenchmarkPoint")?;
        }
        count = count.checked_add(page.len()).ok_or_else(|| {
            ProjectionError::new(
                "projection_count_overflow",
                path,
                "observed point count overflowed usize",
            )
        })?;
        if count > MAX_PROJECTED_RECORDS {
            return Err(ProjectionError::new(
                "projection_inventory_too_large",
                path,
                format!("graph contains more than {MAX_PROJECTED_RECORDS} points"),
            ));
        }
        if page.len() < VERIFICATION_PAGE_SIZE {
            break;
        }
        after_key = page
            .last()
            .expect("a full verification page is nonempty")
            .point_id
            .clone();
    }
    Ok((
        digest,
        count,
        observed_head.expect("a verification scan always executes at least one query"),
    ))
}

async fn scan_projected_run_rows(
    db: &Omnigraph,
    expected_head: Option<&str>,
    path: Option<&Path>,
) -> Result<(Sha256, usize, String), ProjectionError> {
    let mut after_key = String::new();
    let mut observed_head = None;
    let mut count = 0usize;
    let mut digest = projected_table_digest("BenchmarkRun");
    loop {
        let mut params = ParamMap::new();
        params.insert("after_key".to_string(), Literal::String(after_key.clone()));
        let (result, head) = db
            .query_with_head(
                ReadTarget::branch("main"),
                PROJECTION_QUERIES,
                "projection_run_rows_page",
                &params,
            )
            .await
            .map_err(|error| {
                ProjectionError::new(
                    "projection_verification_query_failed",
                    path,
                    error.to_string(),
                )
            })?;
        observe_scan_head(&mut observed_head, head, expected_head, path)?;
        let page = parse_projected_run_page(result, &after_key)?;
        for row in &page {
            digest_projected_row(&mut digest, row, "BenchmarkRun")?;
        }
        count = count.checked_add(page.len()).ok_or_else(|| {
            ProjectionError::new(
                "projection_count_overflow",
                path,
                "observed run count overflowed usize",
            )
        })?;
        if count > MAX_PROJECTED_RECORDS {
            return Err(ProjectionError::new(
                "projection_inventory_too_large",
                path,
                format!("graph contains more than {MAX_PROJECTED_RECORDS} runs"),
            ));
        }
        if page.len() < VERIFICATION_PAGE_SIZE {
            break;
        }
        after_key = page
            .last()
            .expect("a full verification page is nonempty")
            .invocation_id
            .clone();
    }
    Ok((
        digest,
        count,
        observed_head.expect("a verification scan always executes at least one query"),
    ))
}

fn parse_projected_point_page(
    result: QueryResult,
    exclusive_after: &str,
) -> Result<Vec<PointRow>, ProjectionError> {
    let rows = bounded_verification_rows(result, "point")?;
    let mut points = Vec::with_capacity(rows.len());
    let mut previous = exclusive_after.to_string();
    for (index, value) in rows.into_iter().enumerate() {
        validate_projected_json_row_size(&value, "point verification", index)?;
        let row: PointRow = serde_json::from_value(value).map_err(|error| {
            ProjectionError::new(
                "projection_points_result_invalid",
                None,
                format!("point row {index} is not the complete canonical DTO: {error}"),
            )
        })?;
        validate_projected_row_size(&row, "BenchmarkPoint")?;
        require_sha256(&row.point_id, "projected point id")?;
        if row.point_id <= previous {
            return Err(ProjectionError::new(
                "projection_points_order_invalid",
                None,
                format!("point row {index} is not strictly after {previous:?}"),
            ));
        }
        previous.clone_from(&row.point_id);
        points.push(row);
    }
    Ok(points)
}

fn parse_projected_run_page(
    result: QueryResult,
    exclusive_after: &str,
) -> Result<Vec<RunRow>, ProjectionError> {
    let rows = bounded_verification_rows(result, "run")?;
    let mut runs = Vec::with_capacity(rows.len());
    let mut previous = exclusive_after.to_string();
    for (index, value) in rows.into_iter().enumerate() {
        validate_projected_json_row_size(&value, "run verification", index)?;
        let row: RunRow = serde_json::from_value(value).map_err(|error| {
            ProjectionError::new(
                "projection_runs_result_invalid",
                None,
                format!("run row {index} is not the complete canonical DTO: {error}"),
            )
        })?;
        validate_projected_row_size(&row, "BenchmarkRun")?;
        require_invocation_id(&row.invocation_id, "projected invocation id")?;
        require_sha256(&row.record_sha256, "projected record digest")?;
        if row.invocation_id <= previous {
            return Err(ProjectionError::new(
                "projection_runs_order_invalid",
                None,
                format!("run row {index} is not strictly after {previous:?}"),
            ));
        }
        previous.clone_from(&row.invocation_id);
        runs.push(row);
    }
    Ok(runs)
}

fn bounded_verification_rows(
    result: QueryResult,
    noun: &str,
) -> Result<Vec<serde_json::Value>, ProjectionError> {
    let rows = match result.to_rust_json() {
        serde_json::Value::Array(rows) => rows,
        _ => {
            return Err(ProjectionError::new(
                "projection_verification_result_invalid",
                None,
                format!("{noun} verification query did not return a JSON array"),
            ));
        }
    };
    if rows.len() > VERIFICATION_PAGE_SIZE {
        return Err(ProjectionError::new(
            "projection_verification_result_unbounded",
            None,
            format!(
                "{noun} page returned {} rows; maximum is {VERIFICATION_PAGE_SIZE}",
                rows.len()
            ),
        ));
    }
    Ok(rows)
}

fn observe_scan_head(
    first_head: &mut Option<String>,
    observed: Option<String>,
    expected: Option<&str>,
    path: Option<&Path>,
) -> Result<(), ProjectionError> {
    let observed = observed.ok_or_else(|| {
        ProjectionError::new(
            "projection_graph_head_missing",
            path,
            "verification query returned no main-branch graph commit id",
        )
    })?;
    if let Some(expected) = expected
        && observed != expected
    {
        return Err(ProjectionError::new(
            "projection_graph_head_mismatch",
            path,
            format!("expected graph head {expected}, query observed {observed}"),
        ));
    }
    if first_head.as_ref().is_some_and(|first| first != &observed) {
        return Err(ProjectionError::new(
            "projection_graph_head_mismatch",
            path,
            "graph head changed between verification pages",
        ));
    }
    if first_head.is_none() {
        *first_head = Some(observed);
    }
    Ok(())
}

async fn open_current_generation(
    projection_root: &Path,
) -> Result<(Omnigraph, ProjectionManifestV1), ProjectionError> {
    let pointer_path = projection_root.join(CURRENT_FILE);
    let pointer: CurrentPointerV1 = read_canonical_json(
        &pointer_path,
        MAX_POINTER_BYTES,
        "projection_current_invalid",
    )?;
    if pointer.format_version != PROJECTION_FORMAT_VERSION {
        return Err(ProjectionError::new(
            "projection_version_unsupported",
            Some(&pointer_path),
            format!(
                "pointer version {} is unsupported; expected {PROJECTION_FORMAT_VERSION}",
                pointer.format_version
            ),
        ));
    }
    require_sha256(&pointer.generation_id, "generation id")?;
    require_sha256(&pointer.manifest_sha256, "manifest digest")?;
    let generation_root = projection_root
        .join(GENERATIONS_DIRECTORY)
        .join(&pointer.generation_id);
    let generation_root =
        require_real_directory(&generation_root, "projection_generation_invalid")?;
    let manifest_path = generation_root.join(MANIFEST_FILE);
    let manifest_bytes = read_bounded_regular_file(
        &manifest_path,
        MAX_MANIFEST_BYTES,
        "projection_manifest_invalid",
    )?;
    if sha256_hex(&manifest_bytes) != pointer.manifest_sha256 {
        return Err(ProjectionError::new(
            "projection_manifest_digest_mismatch",
            Some(&manifest_path),
            "manifest bytes do not match CURRENT",
        ));
    }
    let manifest: ProjectionManifestV1 = parse_canonical_json(
        &manifest_bytes,
        &manifest_path,
        "projection_manifest_invalid",
    )?;
    validate_manifest(&manifest, &generation_root)?;
    if manifest.generation_id != pointer.generation_id {
        return Err(ProjectionError::new(
            "projection_generation_mismatch",
            Some(&manifest_path),
            "manifest generation id differs from CURRENT",
        ));
    }
    let graph_path = generation_root.join(&manifest.graph_relative_path);
    let graph_path = require_real_directory(&graph_path, "projection_graph_invalid")?;
    let db = Omnigraph::open_read_only(path_to_utf8(&graph_path)?)
        .await
        .map_err(|error| {
            ProjectionError::new(
                "projection_graph_open_failed",
                Some(&graph_path),
                error.to_string(),
            )
        })?;
    Ok((db, manifest))
}

async fn open_generation_by_id(
    projection_root: &Path,
    generation_id: &str,
) -> Result<(Omnigraph, ProjectionManifestV1), ProjectionError> {
    require_sha256(generation_id, "cursor generation id")?;
    let generation_root = projection_root
        .join(GENERATIONS_DIRECTORY)
        .join(generation_id);
    let generation_root =
        require_real_directory(&generation_root, "projection_cursor_generation_invalid")?;
    let manifest_path = generation_root.join(MANIFEST_FILE);
    let manifest: ProjectionManifestV1 = read_canonical_json(
        &manifest_path,
        MAX_MANIFEST_BYTES,
        "projection_manifest_invalid",
    )?;
    validate_manifest(&manifest, &generation_root)?;
    if manifest.generation_id != generation_id {
        return Err(ProjectionError::new(
            "projection_generation_mismatch",
            Some(&manifest_path),
            "cursor generation differs from its manifest",
        ));
    }
    let graph_path = generation_root.join(&manifest.graph_relative_path);
    let graph_path = require_real_directory(&graph_path, "projection_graph_invalid")?;
    let db = Omnigraph::open_read_only(path_to_utf8(&graph_path)?)
        .await
        .map_err(|error| {
            ProjectionError::new(
                "projection_graph_open_failed",
                Some(&graph_path),
                error.to_string(),
            )
        })?;
    Ok((db, manifest))
}

fn validate_manifest(
    manifest: &ProjectionManifestV1,
    generation_root: &Path,
) -> Result<(), ProjectionError> {
    let path = generation_root.join(MANIFEST_FILE);
    if manifest.format_version != PROJECTION_FORMAT_VERSION {
        return Err(ProjectionError::new(
            "projection_version_unsupported",
            Some(&path),
            format!(
                "manifest version {} is unsupported; expected {PROJECTION_FORMAT_VERSION}",
                manifest.format_version
            ),
        ));
    }
    require_sha256(&manifest.generation_id, "generation id")?;
    require_sha256(&manifest.schema_sha256, "schema digest")?;
    require_sha256(&manifest.transform_sha256, "transform digest")?;
    require_sha256(&manifest.inventory_sha256, "inventory digest")?;
    require_sha256(&manifest.projected_rows_sha256, "projected rows digest")?;
    if manifest.schema_sha256 != schema_sha256() {
        return Err(ProjectionError::new(
            "projection_schema_mismatch",
            Some(&path),
            "generation uses a different projection schema",
        ));
    }
    if manifest.transform_version != PROJECTION_TRANSFORM_VERSION
        || manifest.transform_sha256 != transform_sha256()
    {
        return Err(ProjectionError::new(
            "projection_transform_mismatch",
            Some(&path),
            format!(
                "generation transform {} does not match current transform {PROJECTION_TRANSFORM_VERSION}",
                manifest.transform_version
            ),
        ));
    }
    let expected_generation = generation_id(
        &manifest.schema_sha256,
        &manifest.transform_sha256,
        &manifest.inventory_sha256,
        &manifest.projected_rows_sha256,
    );
    if manifest.generation_id != expected_generation {
        return Err(ProjectionError::new(
            "projection_generation_digest_mismatch",
            Some(&path),
            "generation id is not derived from its schema, transform, inventory, and projected-row digests",
        ));
    }
    let directory_name = generation_root
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            ProjectionError::new(
                "projection_generation_path_invalid",
                Some(generation_root),
                "generation directory name is not UTF-8",
            )
        })?;
    if directory_name != manifest.generation_id {
        return Err(ProjectionError::new(
            "projection_generation_path_mismatch",
            Some(generation_root),
            "generation directory does not match manifest generation id",
        ));
    }
    if manifest.graph_relative_path != GRAPH_DIRECTORY {
        return Err(ProjectionError::new(
            "projection_graph_path_invalid",
            Some(&path),
            format!("graph path must be exactly {GRAPH_DIRECTORY}"),
        ));
    }
    if manifest.record_count > MAX_PROJECTED_RECORDS as u64
        || manifest.point_count > manifest.record_count
    {
        return Err(ProjectionError::new(
            "projection_manifest_count_invalid",
            Some(&path),
            "manifest counts are outside the V1 projection bounds",
        ));
    }
    if manifest.graph_commit_id.is_empty() {
        return Err(ProjectionError::new(
            "projection_graph_head_missing",
            Some(&path),
            "manifest graph commit id is empty",
        ));
    }
    Ok(())
}

fn require_expected_head(
    observed: Option<String>,
    manifest: &ProjectionManifestV1,
    path: &Path,
) -> Result<(), ProjectionError> {
    match observed {
        Some(head) if head == manifest.graph_commit_id => Ok(()),
        Some(head) => Err(ProjectionError::new(
            "projection_graph_head_mismatch",
            Some(path),
            format!(
                "manifest records graph head {}, query observed {head}",
                manifest.graph_commit_id
            ),
        )),
        None => Err(ProjectionError::new(
            "projection_graph_head_missing",
            Some(path),
            "query returned no main-branch graph commit id",
        )),
    }
}

fn write_new_manifest(
    generation_root: &Path,
    manifest: &ProjectionManifestV1,
) -> Result<(), ProjectionError> {
    let path = generation_root.join(MANIFEST_FILE);
    let bytes = serde_json::to_vec(manifest).map_err(|error| {
        ProjectionError::new(
            "projection_manifest_serialization_failed",
            Some(&path),
            error.to_string(),
        )
    })?;
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .map_err(|error| ProjectionError::io("projection_manifest_write_failed", &path, error))?;
    file.write_all(&bytes)
        .map_err(|error| ProjectionError::io("projection_manifest_write_failed", &path, error))?;
    file.sync_all()
        .map_err(|error| ProjectionError::io("projection_manifest_sync_failed", &path, error))
}

fn publish_current(
    projection_root: &Path,
    manifest: &ProjectionManifestV1,
    rebuild_lock: &ProjectionRebuildLock,
) -> Result<(), ProjectionError> {
    publish_current_with_observer(projection_root, manifest, rebuild_lock, || Ok(()))
}

fn publish_current_with_observer(
    projection_root: &Path,
    manifest: &ProjectionManifestV1,
    rebuild_lock: &ProjectionRebuildLock,
    before_publish: impl FnOnce() -> Result<(), ProjectionError>,
) -> Result<(), ProjectionError> {
    publish_current_with_observers(
        projection_root,
        manifest,
        rebuild_lock,
        || Ok(()),
        || Ok(()),
        before_publish,
    )
}

fn publish_current_with_observers(
    projection_root: &Path,
    manifest: &ProjectionManifestV1,
    rebuild_lock: &ProjectionRebuildLock,
    after_initial_validation: impl FnOnce() -> Result<(), ProjectionError>,
    after_manifest_read: impl FnOnce() -> Result<(), ProjectionError>,
    before_publish: impl FnOnce() -> Result<(), ProjectionError>,
) -> Result<(), ProjectionError> {
    #[cfg(not(unix))]
    let _ = rebuild_lock;
    #[cfg(unix)]
    rebuild_lock.validate()?;
    after_initial_validation()?;
    let manifest_relative_path = Path::new(GENERATIONS_DIRECTORY)
        .join(&manifest.generation_id)
        .join(MANIFEST_FILE);
    let manifest_path = projection_root.join(&manifest_relative_path);
    #[cfg(unix)]
    let manifest_bytes = read_bounded_regular_file_at(
        rebuild_lock.root_file(),
        &manifest_relative_path,
        &manifest_path,
        MAX_MANIFEST_BYTES,
        "projection_manifest_invalid",
    )?;
    #[cfg(not(unix))]
    let manifest_bytes = read_bounded_regular_file(
        &manifest_path,
        MAX_MANIFEST_BYTES,
        "projection_manifest_invalid",
    )?;
    after_manifest_read()?;
    let observed_manifest: ProjectionManifestV1 = parse_canonical_json(
        &manifest_bytes,
        &manifest_path,
        "projection_manifest_invalid",
    )?;
    if observed_manifest != *manifest {
        return Err(ProjectionError::new(
            "projection_manifest_changed",
            Some(&manifest_path),
            "generation manifest differs from the verified manifest selected for CURRENT publication",
        ));
    }
    let pointer = CurrentPointerV1 {
        format_version: PROJECTION_FORMAT_VERSION,
        generation_id: manifest.generation_id.clone(),
        manifest_sha256: sha256_hex(&manifest_bytes),
    };
    let bytes = serde_json::to_vec(&pointer).map_err(|error| {
        ProjectionError::new(
            "projection_current_serialization_failed",
            Some(projection_root),
            error.to_string(),
        )
    })?;
    #[cfg(unix)]
    {
        let (staging_name, mut staging) = create_current_staging(rebuild_lock)?;
        let result = (|| {
            staging.write_all(&bytes).map_err(|error| {
                ProjectionError::io("projection_current_stage_failed", projection_root, error)
            })?;
            staging.sync_all().map_err(|error| {
                ProjectionError::io("projection_current_sync_failed", projection_root, error)
            })?;
            before_publish()?;
            rebuild_lock.validate()?;
            renameat(
                rebuild_lock.root_file(),
                Path::new(&staging_name),
                rebuild_lock.root_file(),
                Path::new(CURRENT_FILE),
            )
            .map_err(|error| {
                ProjectionError::io(
                    "projection_current_publish_failed",
                    &projection_root.join(CURRENT_FILE),
                    std::io::Error::from_raw_os_error(error as i32),
                )
            })?;
            rebuild_lock.root_file().sync_all().map_err(|error| {
                ProjectionError::io("projection_current_sync_failed", projection_root, error)
            })?;
            rebuild_lock.validate()
        })();
        if result.is_err() {
            let _ = unlinkat(
                rebuild_lock.root_file(),
                Path::new(&staging_name),
                UnlinkatFlags::NoRemoveDir,
            );
        }
        result
    }

    #[cfg(not(unix))]
    {
        let mut staging = tempfile::NamedTempFile::new_in(projection_root).map_err(|error| {
            ProjectionError::io("projection_current_stage_failed", projection_root, error)
        })?;
        staging.write_all(&bytes).map_err(|error| {
            ProjectionError::io("projection_current_stage_failed", staging.path(), error)
        })?;
        staging.as_file().sync_all().map_err(|error| {
            ProjectionError::io("projection_current_sync_failed", staging.path(), error)
        })?;
        before_publish()?;
        let destination = projection_root.join(CURRENT_FILE);
        staging.persist(&destination).map_err(|error| {
            ProjectionError::io(
                "projection_current_publish_failed",
                &destination,
                error.error,
            )
        })?;
        sync_directory(projection_root)
    }
}

#[cfg(unix)]
fn create_current_staging(
    rebuild_lock: &ProjectionRebuildLock,
) -> Result<(String, File), ProjectionError> {
    for _ in 0..MAX_CURRENT_STAGING_ATTEMPTS {
        let counter = CURRENT_STAGING_COUNTER.fetch_add(1, Ordering::Relaxed);
        let name = format!(".current-staging-{}-{counter}", std::process::id());
        match openat(
            rebuild_lock.root_file(),
            Path::new(&name),
            OFlag::O_WRONLY
                | OFlag::O_CREAT
                | OFlag::O_EXCL
                | OFlag::O_NOFOLLOW
                | OFlag::O_NONBLOCK
                | OFlag::O_CLOEXEC,
            Mode::from_bits_truncate(0o600),
        ) {
            Ok(descriptor) => return Ok((name, File::from(descriptor))),
            Err(nix::errno::Errno::EEXIST) => continue,
            Err(error) => {
                return Err(ProjectionError::io(
                    "projection_current_stage_failed",
                    &rebuild_lock.root_path,
                    std::io::Error::from_raw_os_error(error as i32),
                ));
            }
        }
    }
    Err(ProjectionError::new(
        "projection_current_stage_failed",
        Some(&rebuild_lock.root_path),
        "could not reserve a bounded unique CURRENT staging name",
    ))
}

fn build_result(manifest: &ProjectionManifestV1, reused: bool) -> ProjectionBuildV1 {
    ProjectionBuildV1 {
        format_version: PROJECTION_FORMAT_VERSION,
        generation_id: manifest.generation_id.clone(),
        schema_sha256: manifest.schema_sha256.clone(),
        transform_version: manifest.transform_version,
        transform_sha256: manifest.transform_sha256.clone(),
        inventory_sha256: manifest.inventory_sha256.clone(),
        projected_rows_sha256: manifest.projected_rows_sha256.clone(),
        record_count: manifest.record_count,
        point_count: manifest.point_count,
        graph_commit_id: manifest.graph_commit_id.clone(),
        reused,
    }
}

#[derive(Default)]
struct NdjsonBatch {
    body: String,
    rows: usize,
}

impl NdjsonBatch {
    fn push_group(&mut self, lines: &[String]) -> Result<Option<String>, ProjectionError> {
        let group_bytes = lines.iter().try_fold(0usize, |total, line| {
            if line.len() > MAX_PROJECTED_ROW_BYTES {
                return Err(ProjectionError::new(
                    "projection_row_too_large",
                    None,
                    format!(
                        "projected row is {} bytes; maximum is {MAX_PROJECTED_ROW_BYTES}",
                        line.len()
                    ),
                ));
            }
            total
                .checked_add(line.len().saturating_add(1))
                .ok_or_else(|| {
                    ProjectionError::new(
                        "projection_batch_size_overflow",
                        None,
                        "projected batch byte count overflowed",
                    )
                })
        })?;
        if lines.len() > MAX_BATCH_ROWS || group_bytes > MAX_BATCH_BYTES {
            return Err(ProjectionError::new(
                "projection_group_too_large",
                None,
                "one atomic projection row group exceeds the batch bounds",
            ));
        }

        let flush = if self.rows != 0
            && (self.rows.saturating_add(lines.len()) > MAX_BATCH_ROWS
                || self.body.len().saturating_add(group_bytes) > MAX_BATCH_BYTES)
        {
            Some(std::mem::take(&mut self.body))
        } else {
            None
        };
        if flush.is_some() {
            self.rows = 0;
        }
        for line in lines {
            self.body.push_str(line);
            self.body.push('\n');
            self.rows += 1;
        }
        Ok(flush)
    }

    fn finish(self) -> Option<String> {
        (!self.body.is_empty()).then_some(self.body)
    }
}

fn serialize_graph_row(value: &impl Serialize) -> Result<String, ProjectionError> {
    serde_json::to_string(value).map_err(|error| {
        ProjectionError::new(
            "projection_row_serialization_failed",
            None,
            error.to_string(),
        )
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MinP50Max {
    min: u64,
    p50: u64,
    max: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LogicalCallSummaries {
    lance_data_plane: MinP50Max,
    control_plane: MinP50Max,
}

/// Summarize independently measured repetitions; acquisition quantity never
/// enters an operation-cost value.
///
/// The exact per-repetition formulas are:
///
/// ```text
/// lance_data_plane = manifest.(get + put + put_part + head + list + delete
///     + copy + rename + multipart_complete + multipart_abort)
///     + table.(get + put + put_part + head + list + delete + copy + rename
///     + multipart_complete + multipart_abort)
/// control_plane = read_text + read_text_if_exists + read_text_versioned
///     + exists + list_dir + mutation_calls
/// ```
///
/// Control `write_text` and `delete` are mutation subcategories and are not
/// added again. Each plane stores its minimum, maximum, and nearest-rank p50:
/// after sorting `n` repetition totals, p50 is index `ceil(0.5 * n) - 1`.
fn logical_call_summaries(record: &RunRecordV1) -> Result<LogicalCallSummaries, ProjectionError> {
    let mut lance_data_plane = Vec::with_capacity(record.measurements.raw_samples.len());
    let mut control_plane = Vec::with_capacity(record.measurements.raw_samples.len());
    for sample in &record.measurements.raw_samples {
        let manifest = sum_call_counts(sample.logical_store_calls.manifest)?;
        let table = sum_call_counts(sample.logical_store_calls.table)?;
        lance_data_plane.push(manifest.checked_add(table).ok_or_else(|| {
            ProjectionError::new(
                "projection_logical_call_overflow",
                None,
                "per-repetition Lance data-plane logical call total does not fit u64",
            )
        })?);
        let control = &sample.control_store_calls;
        control_plane.push(sum_u64_values(
            [
                control.read_text,
                control.read_text_if_exists,
                control.read_text_versioned,
                control.exists,
                control.list_dir,
                control.mutation_calls,
            ],
            "per-repetition control-plane logical call total",
        )?);
    }
    Ok(LogicalCallSummaries {
        lance_data_plane: summarize_nearest_rank(lance_data_plane, "Lance data-plane")?,
        control_plane: summarize_nearest_rank(control_plane, "control-plane")?,
    })
}

fn sum_call_counts(counts: LogicalCallCounts) -> Result<u64, ProjectionError> {
    sum_u64_values(
        [
            counts.get,
            counts.put,
            counts.put_part,
            counts.head,
            counts.list,
            counts.delete,
            counts.copy,
            counts.rename,
            counts.multipart_complete,
            counts.multipart_abort,
        ],
        "Lance logical call total",
    )
}

fn sum_u64_values(
    values: impl IntoIterator<Item = u64>,
    noun: &str,
) -> Result<u64, ProjectionError> {
    let mut total = 0u64;
    for value in values {
        total = total.checked_add(value).ok_or_else(|| {
            ProjectionError::new(
                "projection_logical_call_overflow",
                None,
                format!("{noun} does not fit u64"),
            )
        })?;
    }
    Ok(total)
}

fn summarize_nearest_rank(mut values: Vec<u64>, noun: &str) -> Result<MinP50Max, ProjectionError> {
    if values.is_empty() {
        return Err(ProjectionError::new(
            "projection_logical_call_summary_empty",
            None,
            format!("cannot summarize {noun} calls without repetitions"),
        ));
    }
    values.sort_unstable();
    let p50_index = 50usize
        .checked_mul(values.len())
        .ok_or_else(|| {
            ProjectionError::new(
                "projection_logical_call_overflow",
                None,
                format!("{noun} nearest-rank index overflowed"),
            )
        })?
        .div_ceil(100)
        .saturating_sub(1);
    Ok(MinP50Max {
        min: values[0],
        p50: values[p50_index],
        max: values[values.len() - 1],
    })
}

fn canonical_json<T: Serialize>(value: &T, noun: &str) -> Result<String, ProjectionError> {
    serde_json::to_string(value).map_err(|error| {
        ProjectionError::new(
            "projection_value_serialization_failed",
            None,
            format!("could not serialize {noun}: {error}"),
        )
    })
}

fn typed_json_sha256<T: Serialize>(value: &T, noun: &str) -> Result<String, ProjectionError> {
    let bytes = serde_json::to_vec(value).map_err(|error| {
        ProjectionError::new(
            "projection_value_serialization_failed",
            None,
            format!("could not serialize {noun}: {error}"),
        )
    })?;
    Ok(sha256_hex(&bytes))
}

fn json_string<T: Serialize>(value: &T, noun: &str) -> Result<String, ProjectionError> {
    match serde_json::to_value(value).map_err(|error| {
        ProjectionError::new(
            "projection_value_serialization_failed",
            None,
            format!("could not serialize {noun}: {error}"),
        )
    })? {
        serde_json::Value::String(value) => Ok(value),
        _ => Err(ProjectionError::new(
            "projection_value_shape_invalid",
            None,
            format!("{noun} did not serialize as a string"),
        )),
    }
}

fn inventory_sha256(entries: &[InventoryEntry]) -> String {
    let mut digest = Sha256::new();
    digest.update(INVENTORY_DOMAIN);
    for entry in entries {
        digest_field(&mut digest, entry.invocation_id.as_bytes());
        digest_field(&mut digest, entry.record_sha256.as_bytes());
        digest_field(&mut digest, entry.point_id.as_bytes());
    }
    format!("{:x}", digest.finalize())
}

#[derive(Default)]
struct RetainedMetadataBudget {
    accounted_bytes: usize,
}

impl RetainedMetadataBudget {
    fn add(&mut self, bytes: usize, noun: &str) -> Result<(), ProjectionError> {
        self.accounted_bytes = self.accounted_bytes.checked_add(bytes).ok_or_else(|| {
            ProjectionError::new(
                "projection_metadata_size_overflow",
                None,
                format!("retained {noun} byte count overflowed"),
            )
        })?;
        if self.accounted_bytes > MAX_RETAINED_METADATA_BYTES {
            return Err(ProjectionError::new(
                "projection_metadata_too_large",
                None,
                format!(
                    "retained projection metadata accounts for {} bytes; maximum is {MAX_RETAINED_METADATA_BYTES}",
                    self.accounted_bytes
                ),
            ));
        }
        Ok(())
    }
}

fn retained_inventory_entry_bytes(entry: &InventoryEntry) -> usize {
    std::mem::size_of::<InventoryEntry>()
        .saturating_add(entry.invocation_id.len())
        .saturating_add(entry.record_sha256.len())
        .saturating_add(entry.point_id.len())
}

fn retained_point_row_bytes(point: &PointRow) -> usize {
    // The BTreeMap owns a second point-id String as its key. Container-node
    // overhead is independently bounded by MAX_PROJECTED_RECORDS.
    std::mem::size_of::<PointRow>()
        .saturating_add(std::mem::size_of::<String>())
        .saturating_add(point.point_id.len().saturating_mul(2))
        .saturating_add(point.point_name.len())
        .saturating_add(point.scenario.len())
        .saturating_add(point.run_spec_json.len())
}

fn validate_projected_row_size(row: &impl Serialize, table: &str) -> Result<(), ProjectionError> {
    let _ = canonical_projected_row_bytes(row, table)?;
    Ok(())
}

fn validate_projected_json_row_size(
    row: &serde_json::Value,
    context: &str,
    index: usize,
) -> Result<(), ProjectionError> {
    let bytes = serde_json::to_vec(row).map_err(|error| {
        ProjectionError::new(
            "projection_row_serialization_failed",
            None,
            format!("could not size {context} row {index}: {error}"),
        )
    })?;
    if bytes.len() > MAX_PROJECTED_ROW_BYTES {
        return Err(ProjectionError::new(
            "projection_row_too_large",
            None,
            format!(
                "{context} row {index} is {} bytes; maximum is {MAX_PROJECTED_ROW_BYTES}",
                bytes.len()
            ),
        ));
    }
    Ok(())
}

fn canonical_projected_row_bytes(
    row: &impl Serialize,
    table: &str,
) -> Result<Vec<u8>, ProjectionError> {
    let bytes = serde_json::to_vec(row).map_err(|error| {
        ProjectionError::new(
            "projection_row_serialization_failed",
            None,
            format!("could not serialize canonical {table} DTO: {error}"),
        )
    })?;
    if bytes.len() > MAX_PROJECTED_ROW_BYTES {
        return Err(ProjectionError::new(
            "projection_row_too_large",
            None,
            format!(
                "canonical {table} DTO is {} bytes; maximum is {MAX_PROJECTED_ROW_BYTES}",
                bytes.len()
            ),
        ));
    }
    Ok(bytes)
}

fn projected_table_digest(table: &str) -> Sha256 {
    let mut digest = Sha256::new();
    digest.update(PROJECTED_TABLE_DOMAIN);
    digest.update(PROJECTION_TRANSFORM_VERSION.to_be_bytes());
    digest_field(&mut digest, table.as_bytes());
    digest
}

fn digest_projected_row(
    digest: &mut Sha256,
    row: &impl Serialize,
    table: &str,
) -> Result<(), ProjectionError> {
    let bytes = canonical_projected_row_bytes(row, table)?;
    digest_field(digest, b"row");
    digest_field(digest, &bytes);
    Ok(())
}

fn projected_rows_sha256(
    point_digest: Sha256,
    point_count: usize,
    run_digest: Sha256,
    run_count: usize,
) -> Result<String, ProjectionError> {
    let mut digest = Sha256::new();
    digest.update(PROJECTED_ROWS_DOMAIN);
    digest.update(PROJECTION_TRANSFORM_VERSION.to_be_bytes());
    digest_field(&mut digest, b"BenchmarkPoint");
    digest_field(&mut digest, &point_digest.finalize());
    digest.update(usize_to_u64(point_count, None, "point count")?.to_be_bytes());
    digest_field(&mut digest, b"BenchmarkRun");
    digest_field(&mut digest, &run_digest.finalize());
    digest.update(usize_to_u64(run_count, None, "run count")?.to_be_bytes());
    Ok(format!("{:x}", digest.finalize()))
}

fn usize_to_u64(value: usize, path: Option<&Path>, noun: &str) -> Result<u64, ProjectionError> {
    u64::try_from(value).map_err(|_| {
        ProjectionError::new(
            "projection_count_overflow",
            path,
            format!("{noun} does not fit u64"),
        )
    })
}

fn generation_id(
    schema_digest: &str,
    transform_digest: &str,
    inventory_digest: &str,
    projected_rows_digest: &str,
) -> String {
    let mut digest = Sha256::new();
    digest.update(GENERATION_DOMAIN);
    digest.update(PROJECTION_FORMAT_VERSION.to_be_bytes());
    digest_field(&mut digest, schema_digest.as_bytes());
    digest.update(PROJECTION_TRANSFORM_VERSION.to_be_bytes());
    digest_field(&mut digest, transform_digest.as_bytes());
    digest_field(&mut digest, inventory_digest.as_bytes());
    digest_field(&mut digest, projected_rows_digest.as_bytes());
    format!("{:x}", digest.finalize())
}

fn digest_field(digest: &mut Sha256, bytes: &[u8]) {
    digest.update(u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_be_bytes());
    digest.update(bytes);
}

fn schema_sha256() -> String {
    sha256_hex(PROJECTION_SCHEMA.as_bytes())
}

fn transform_sha256() -> String {
    let mut digest = Sha256::new();
    digest.update(TRANSFORM_DOMAIN);
    digest.update(PROJECTION_TRANSFORM_VERSION.to_be_bytes());
    digest_field(&mut digest, PROJECTION_TRANSFORM_CONTRACT.as_bytes());
    digest_field(&mut digest, PROJECTION_SCHEMA.as_bytes());
    digest_field(&mut digest, PROJECTION_QUERIES.as_bytes());
    format!("{:x}", digest.finalize())
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(bytes);
    format!("{:x}", digest.finalize())
}

fn require_sha256(value: &str, noun: &str) -> Result<(), ProjectionError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ProjectionError::new(
            "projection_digest_invalid",
            None,
            format!("{noun} must be exactly 64 lowercase hexadecimal characters"),
        ));
    }
    Ok(())
}

fn require_invocation_id(value: &str, noun: &str) -> Result<(), ProjectionError> {
    let valid_crockford = |byte: u8| {
        byte.is_ascii_digit()
            || matches!(
                byte,
                b'A'..=b'H' | b'J'..=b'K' | b'M'..=b'N' | b'P'..=b'T' | b'V'..=b'Z'
            )
    };
    if value.len() != 26
        || !value.is_ascii()
        || !matches!(value.as_bytes().first(), Some(b'0'..=b'7'))
        || !value.bytes().all(valid_crockford)
    {
        return Err(ProjectionError::new(
            "projection_cursor_key_invalid",
            None,
            format!("{noun} must be a canonical uppercase Crockford ULID"),
        ));
    }
    Ok(())
}

fn read_canonical_json<T>(
    path: &Path,
    max_bytes: u64,
    code: &'static str,
) -> Result<T, ProjectionError>
where
    T: for<'de> Deserialize<'de> + Serialize,
{
    let bytes = read_bounded_regular_file(path, max_bytes, code)?;
    parse_canonical_json(&bytes, path, code)
}

fn parse_canonical_json<T>(
    bytes: &[u8],
    path: &Path,
    code: &'static str,
) -> Result<T, ProjectionError>
where
    T: for<'de> Deserialize<'de> + Serialize,
{
    let value: T = serde_json::from_slice(bytes)
        .map_err(|error| ProjectionError::new(code, Some(path), error.to_string()))?;
    let canonical = serde_json::to_vec(&value)
        .map_err(|error| ProjectionError::new(code, Some(path), error.to_string()))?;
    if canonical != bytes {
        return Err(ProjectionError::new(
            code,
            Some(path),
            "JSON is not the canonical compact typed serialization",
        ));
    }
    Ok(value)
}

fn read_bounded_regular_file(
    path: &Path,
    max_bytes: u64,
    code: &'static str,
) -> Result<Vec<u8>, ProjectionError> {
    #[cfg(unix)]
    let file = open_regular_file_no_follow(path, code)?;
    #[cfg(not(unix))]
    let file = File::open(path).map_err(|error| ProjectionError::io(code, path, error))?;
    read_bounded_open_regular_file(file, path, max_bytes, code)
}

fn read_bounded_open_regular_file(
    mut file: File,
    path: &Path,
    max_bytes: u64,
    code: &'static str,
) -> Result<Vec<u8>, ProjectionError> {
    let metadata = file
        .metadata()
        .map_err(|error| ProjectionError::io(code, path, error))?;
    if !metadata.file_type().is_file() {
        return Err(ProjectionError::new(
            code,
            Some(path),
            "opened descriptor must be a regular file",
        ));
    }
    if metadata.len() > max_bytes {
        return Err(ProjectionError::new(
            code,
            Some(path),
            format!("file is {} bytes; maximum is {max_bytes}", metadata.len()),
        ));
    }
    let mut bytes = Vec::with_capacity(usize::try_from(metadata.len()).unwrap_or(0));
    Read::by_ref(&mut file)
        .take(max_bytes.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|error| ProjectionError::io(code, path, error))?;
    if u64::try_from(bytes.len())
        .ok()
        .is_none_or(|length| length > max_bytes)
    {
        return Err(ProjectionError::new(
            code,
            Some(path),
            format!("file exceeds the {max_bytes}-byte limit"),
        ));
    }
    Ok(bytes)
}

#[cfg(unix)]
fn read_bounded_regular_file_at(
    root: &File,
    relative_path: &Path,
    display_path: &Path,
    max_bytes: u64,
    code: &'static str,
) -> Result<Vec<u8>, ProjectionError> {
    let file = open_regular_file_at(root, relative_path, display_path, code)?;
    read_bounded_open_regular_file(file, display_path, max_bytes, code)
}

#[cfg(unix)]
fn projection_path_components(
    path: &Path,
    code: &'static str,
) -> Result<(File, Vec<OsString>), ProjectionError> {
    if path.as_os_str().is_empty() {
        return Err(ProjectionError::new(code, Some(path), "path is empty"));
    }
    let mut components = path.components().peekable();
    let anchor = if components.peek() == Some(&Component::RootDir) {
        components.next();
        Path::new("/")
    } else {
        Path::new(".")
    };
    let mut options = fs::OpenOptions::new();
    options.read(true).custom_flags(
        nix::libc::O_DIRECTORY
            | nix::libc::O_NOFOLLOW
            | nix::libc::O_NONBLOCK
            | nix::libc::O_CLOEXEC,
    );
    let directory = options
        .open(anchor)
        .map_err(|error| ProjectionError::io(code, path, error))?;
    let mut names = Vec::new();
    for component in components {
        match component {
            Component::Normal(name) => names.push(name.to_os_string()),
            Component::CurDir
            | Component::ParentDir
            | Component::Prefix(_)
            | Component::RootDir => {
                return Err(ProjectionError::new(
                    code,
                    Some(path),
                    "path must contain only normalized components",
                ));
            }
        }
    }
    Ok((directory, names))
}

#[cfg(unix)]
fn open_child_directory(
    parent: &File,
    name: &OsStr,
    full_path: &Path,
    code: &'static str,
) -> Result<File, ProjectionError> {
    open_child_directory_raw(parent, name).map_err(|error| {
        ProjectionError::io(
            code,
            full_path,
            std::io::Error::from_raw_os_error(error as i32),
        )
    })
}

#[cfg(unix)]
fn open_child_directory_raw(parent: &File, name: &OsStr) -> Result<File, nix::errno::Errno> {
    let descriptor = openat(
        parent,
        Path::new(name),
        OFlag::O_RDONLY
            | OFlag::O_DIRECTORY
            | OFlag::O_NOFOLLOW
            | OFlag::O_NONBLOCK
            | OFlag::O_CLOEXEC,
        Mode::empty(),
    )?;
    let directory = File::from(descriptor);
    match directory.metadata() {
        Ok(metadata) if metadata.file_type().is_dir() => Ok(directory),
        Ok(_) => Err(nix::errno::Errno::ENOTDIR),
        Err(_) => Err(nix::errno::Errno::EIO),
    }
}

#[cfg(unix)]
fn open_directory_no_follow(path: &Path, code: &'static str) -> Result<File, ProjectionError> {
    let (mut directory, names) = projection_path_components(path, code)?;
    for name in names {
        directory = open_child_directory(&directory, &name, path, code)?;
    }
    Ok(directory)
}

#[cfg(unix)]
fn open_regular_file_no_follow(path: &Path, code: &'static str) -> Result<File, ProjectionError> {
    let (mut directory, mut names) = projection_path_components(path, code)?;
    let file_name = names.pop().ok_or_else(|| {
        ProjectionError::new(code, Some(path), "regular-file path has no filename")
    })?;
    for name in names {
        directory = open_child_directory(&directory, &name, path, code)?;
    }
    let descriptor = openat(
        &directory,
        Path::new(&file_name),
        OFlag::O_RDONLY | OFlag::O_NOFOLLOW | OFlag::O_NONBLOCK | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .map_err(|error| {
        ProjectionError::io(code, path, std::io::Error::from_raw_os_error(error as i32))
    })?;
    Ok(File::from(descriptor))
}

#[cfg(unix)]
fn open_regular_file_at(
    root: &File,
    relative_path: &Path,
    display_path: &Path,
    code: &'static str,
) -> Result<File, ProjectionError> {
    if relative_path.as_os_str().is_empty() || relative_path.is_absolute() {
        return Err(ProjectionError::new(
            code,
            Some(display_path),
            "descriptor-relative path must be nonempty and relative",
        ));
    }
    let mut names = Vec::new();
    for component in relative_path.components() {
        match component {
            Component::Normal(name) => names.push(name.to_os_string()),
            Component::CurDir
            | Component::ParentDir
            | Component::Prefix(_)
            | Component::RootDir => {
                return Err(ProjectionError::new(
                    code,
                    Some(display_path),
                    "descriptor-relative path must contain only normalized components",
                ));
            }
        }
    }
    let file_name = names.pop().ok_or_else(|| {
        ProjectionError::new(
            code,
            Some(display_path),
            "descriptor-relative regular-file path has no filename",
        )
    })?;
    let mut directory = root
        .try_clone()
        .map_err(|error| ProjectionError::io(code, display_path, error))?;
    for name in names {
        directory = open_child_directory(&directory, &name, display_path, code)?;
    }
    let descriptor = openat(
        &directory,
        Path::new(&file_name),
        OFlag::O_RDONLY | OFlag::O_NOFOLLOW | OFlag::O_NONBLOCK | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .map_err(|error| {
        ProjectionError::io(
            code,
            display_path,
            std::io::Error::from_raw_os_error(error as i32),
        )
    })?;
    Ok(File::from(descriptor))
}

fn require_real_directory(path: &Path, code: &'static str) -> Result<PathBuf, ProjectionError> {
    #[cfg(unix)]
    let directory = open_directory_no_follow(path, code)?;
    #[cfg(not(unix))]
    let directory = File::open(path).map_err(|error| ProjectionError::io(code, path, error))?;
    let metadata = directory
        .metadata()
        .map_err(|error| ProjectionError::io(code, path, error))?;
    if !metadata.file_type().is_dir() {
        return Err(ProjectionError::new(
            code,
            Some(path),
            "opened descriptor must be a real directory",
        ));
    }
    let canonical =
        fs::canonicalize(path).map_err(|error| ProjectionError::io(code, path, error))?;
    #[cfg(unix)]
    {
        let resolved = open_directory_no_follow(&canonical, code)?;
        let resolved_metadata = resolved
            .metadata()
            .map_err(|error| ProjectionError::io(code, &canonical, error))?;
        if metadata.dev() != resolved_metadata.dev() || metadata.ino() != resolved_metadata.ino() {
            return Err(ProjectionError::new(
                code,
                Some(path),
                "directory was replaced while its canonical path was resolved",
            ));
        }
    }
    Ok(canonical)
}

fn require_root_directory(path: &Path, code: &'static str) -> Result<PathBuf, ProjectionError> {
    let path_metadata =
        fs::symlink_metadata(path).map_err(|error| ProjectionError::io(code, path, error))?;
    if path_metadata.file_type().is_symlink() || !path_metadata.file_type().is_dir() {
        return Err(ProjectionError::new(
            code,
            Some(path),
            "path must name a real directory, not a symlink",
        ));
    }
    // Resolve a caller-facing root once, then use only its canonical spelling.
    // macOS exposes standard paths such as /var through a system symlink; the
    // descriptor walk below rejects symlinks beneath this explicit boundary.
    let canonical =
        fs::canonicalize(path).map_err(|error| ProjectionError::io(code, path, error))?;
    #[cfg(unix)]
    let directory = open_directory_no_follow(&canonical, code)?;
    #[cfg(not(unix))]
    let directory =
        File::open(&canonical).map_err(|error| ProjectionError::io(code, path, error))?;
    let metadata = directory
        .metadata()
        .map_err(|error| ProjectionError::io(code, path, error))?;
    if !metadata.file_type().is_dir() {
        return Err(ProjectionError::new(
            code,
            Some(path),
            "opened descriptor must be a real directory",
        ));
    }
    #[cfg(unix)]
    {
        if path_metadata.dev() != metadata.dev() || path_metadata.ino() != metadata.ino() {
            return Err(ProjectionError::new(
                code,
                Some(path),
                "directory was replaced while its canonical path was resolved",
            ));
        }
    }
    Ok(canonical)
}

fn ensure_real_directory(path: &Path, code: &'static str) -> Result<(), ProjectionError> {
    #[cfg(unix)]
    {
        let (mut directory, names) = projection_path_components(path, code)?;
        for name in names {
            directory = match open_child_directory_raw(&directory, &name) {
                Ok(directory) => directory,
                Err(nix::errno::Errno::ENOENT) => {
                    match mkdirat(
                        &directory,
                        Path::new(&name),
                        Mode::from_bits_truncate(0o755),
                    ) {
                        Ok(()) | Err(nix::errno::Errno::EEXIST) => {}
                        Err(error) => {
                            return Err(ProjectionError::io(
                                code,
                                path,
                                std::io::Error::from_raw_os_error(error as i32),
                            ));
                        }
                    }
                    open_child_directory(&directory, &name, path, code)?
                }
                Err(error) => {
                    return Err(ProjectionError::io(
                        code,
                        path,
                        std::io::Error::from_raw_os_error(error as i32),
                    ));
                }
            };
        }
        Ok(())
    }

    #[cfg(not(unix))]
    {
        fs::create_dir_all(path).map_err(|error| ProjectionError::io(code, path, error))?;
        require_real_directory(path, code)?;
        Ok(())
    }
}

fn resolve_prospective_path(path: &Path) -> Result<PathBuf, ProjectionError> {
    if path.as_os_str().is_empty() {
        return Err(ProjectionError::new(
            "projection_root_invalid",
            Some(path),
            "projection root is empty",
        ));
    }
    if path
        .components()
        .any(|component| matches!(component, Component::ParentDir | Component::CurDir))
    {
        return Err(ProjectionError::new(
            "projection_root_invalid",
            Some(path),
            "projection root may not contain '.' or '..' components",
        ));
    }
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(ProjectionError::new(
                "projection_root_invalid",
                Some(path),
                "projection root itself may not be a symlink",
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(ProjectionError::io(
                "projection_root_resolution_failed",
                path,
                error,
            ));
        }
    }
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|error| ProjectionError::io("projection_current_dir_failed", path, error))?
            .join(path)
    };
    let mut cursor = absolute.as_path();
    let mut suffix = Vec::<OsString>::new();
    loop {
        if symlink_metadata_if_present(cursor, "projection_root_resolution_failed")?.is_some() {
            let mut resolved = fs::canonicalize(cursor).map_err(|error| {
                ProjectionError::io("projection_root_resolution_failed", cursor, error)
            })?;
            for component in suffix.iter().rev() {
                resolved.push(component);
            }
            return Ok(resolved);
        }
        let name = cursor.file_name().ok_or_else(|| {
            ProjectionError::new(
                "projection_root_resolution_failed",
                Some(path),
                "could not find an existing ancestor",
            )
        })?;
        suffix.push(name.to_os_string());
        cursor = cursor.parent().ok_or_else(|| {
            ProjectionError::new(
                "projection_root_resolution_failed",
                Some(path),
                "could not find an existing ancestor",
            )
        })?;
    }
}

/// Read path-entry metadata without following the final symlink while keeping
/// every failure except a genuine missing entry observable. `Path::exists`
/// cannot be used at publication or path-resolution boundaries because it
/// collapses permission, I/O, and symlink-loop errors into `false`.
fn symlink_metadata_if_present(
    path: &Path,
    code: &'static str,
) -> Result<Option<fs::Metadata>, ProjectionError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => Ok(Some(metadata)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(ProjectionError::io(code, path, error)),
    }
}

fn reject_overlapping_roots(
    archive_root: &Path,
    projection_root: &Path,
) -> Result<(), ProjectionError> {
    if archive_root == projection_root
        || archive_root.starts_with(projection_root)
        || projection_root.starts_with(archive_root)
    {
        return Err(ProjectionError::new(
            "projection_archive_overlap",
            Some(projection_root),
            format!(
                "archive root {} and projection root must be disjoint",
                archive_root.display()
            ),
        ));
    }
    Ok(())
}

fn validate_relative_archive_path(value: &str, noun: &str) -> Result<(), ProjectionError> {
    let path = Path::new(value);
    if value.is_empty()
        || path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(ProjectionError::new(
            "projection_archive_path_invalid",
            None,
            format!("{noun} path must be a nonempty normalized relative path"),
        ));
    }
    Ok(())
}

fn path_to_utf8(path: &Path) -> Result<&str, ProjectionError> {
    path.to_str().ok_or_else(|| {
        ProjectionError::new(
            "projection_path_not_utf8",
            Some(path),
            "OmniGraph local URIs require a UTF-8 path",
        )
    })
}

fn sync_directory(path: &Path) -> Result<(), ProjectionError> {
    #[cfg(unix)]
    let directory = open_directory_no_follow(path, "projection_directory_sync_failed")?;
    #[cfg(not(unix))]
    let directory = File::open(path)
        .map_err(|error| ProjectionError::io("projection_directory_sync_failed", path, error))?;
    directory
        .sync_all()
        .map_err(|error| ProjectionError::io("projection_directory_sync_failed", path, error))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn projection_errors_have_byte_safe_json_paths() {
        use std::os::unix::ffi::OsStringExt;

        let path = PathBuf::from(std::ffi::OsString::from_vec(b"projection-\xff".to_vec()));
        let error = ProjectionError::new("projection-test", Some(&path), "failed");
        let json = serde_json::to_string(&error).expect("lossy paths remain serializable");
        let decoded: serde_json::Value = serde_json::from_str(&json).expect("error JSON");
        assert_eq!(decoded["path"], path.to_string_lossy().as_ref());
    }

    #[test]
    fn path_presence_only_classifies_not_found_as_absent() {
        let holder = tempfile::tempdir().unwrap();
        let missing = holder.path().join("missing");
        assert!(
            symlink_metadata_if_present(&missing, "projection_test_inspection_failed")
                .unwrap()
                .is_none()
        );

        let nondirectory = holder.path().join("not-a-directory");
        fs::write(&nondirectory, b"file").unwrap();
        let blocked_child = nondirectory.join("child");
        let error =
            symlink_metadata_if_present(&blocked_child, "projection_test_inspection_failed")
                .unwrap_err();
        assert_eq!(error.code, "projection_test_inspection_failed");
        assert_eq!(error.path.as_deref(), Some(blocked_child.as_path()));

        let error = resolve_prospective_path(&blocked_child).unwrap_err();
        assert_eq!(error.code, "projection_root_resolution_failed");
        assert_eq!(error.path.as_deref(), Some(blocked_child.as_path()));
    }

    #[cfg(unix)]
    #[test]
    fn path_presence_propagates_symlink_loop_failures() {
        use std::os::unix::fs::symlink;

        let holder = tempfile::tempdir().unwrap();
        let left = holder.path().join("left");
        let right = holder.path().join("right");
        symlink("right", &left).unwrap();
        symlink("left", &right).unwrap();
        let through_loop = left.join("child");

        let error = symlink_metadata_if_present(&through_loop, "projection_test_inspection_failed")
            .unwrap_err();
        assert_eq!(error.code, "projection_test_inspection_failed");
        assert_eq!(error.path.as_deref(), Some(through_loop.as_path()));

        let error = resolve_prospective_path(&through_loop).unwrap_err();
        assert_eq!(error.code, "projection_root_resolution_failed");
        assert_eq!(error.path.as_deref(), Some(through_loop.as_path()));
    }

    #[cfg(unix)]
    #[test]
    fn bounded_reads_reject_special_files_and_symlinked_ancestors_without_blocking() {
        use std::os::unix::fs::symlink;

        let holder = tempfile::tempdir().unwrap();
        let holder_path = fs::canonicalize(holder.path()).unwrap();
        let fifo = holder_path.join("telemetry-fifo");
        nix::unistd::mkfifo(&fifo, Mode::from_bits_truncate(0o600)).unwrap();
        let started = Instant::now();
        let error =
            read_bounded_regular_file(&fifo, 64, "projection_test_file_invalid").unwrap_err();
        assert_eq!(error.code, "projection_test_file_invalid");
        assert!(started.elapsed() < Duration::from_secs(1));

        let outside = tempfile::tempdir().unwrap();
        let outside_path = fs::canonicalize(outside.path()).unwrap();
        let real_directory = outside_path.join("generation");
        fs::create_dir(&real_directory).unwrap();
        fs::write(real_directory.join("manifest.json"), b"{}").unwrap();
        let alias = holder_path.join("alias");
        symlink(&outside_path, &alias).unwrap();
        assert_eq!(
            read_bounded_regular_file(
                &alias.join("generation/manifest.json"),
                64,
                "projection_test_file_invalid",
            )
            .unwrap_err()
            .code,
            "projection_test_file_invalid"
        );
        assert_eq!(
            open_directory_no_follow(
                &alias.join("generation"),
                "projection_test_directory_invalid"
            )
            .unwrap_err()
            .code,
            "projection_test_directory_invalid"
        );
    }

    #[test]
    fn inventory_digest_is_framed_and_order_sensitive() {
        let first = vec![InventoryEntry {
            invocation_id: "one".to_string(),
            record_sha256: "a".repeat(64),
            point_id: "b".repeat(64),
        }];
        let split_differently = vec![InventoryEntry {
            invocation_id: "on".to_string(),
            record_sha256: format!("e{}", "a".repeat(63)),
            point_id: "b".repeat(64),
        }];
        assert_ne!(
            inventory_sha256(&first),
            inventory_sha256(&split_differently)
        );
    }

    #[test]
    fn generation_identity_binds_transform_and_complete_projected_rows() {
        let record = crate::record::tests::valid_record_fixture();
        let receipt = crate::archive::ArchiveReceiptV1 {
            archive_format_version: ARCHIVE_FORMAT_VERSION,
            invocation_id: record.invocation.invocation_id.clone(),
            record_sha256: "a".repeat(64),
            object_relative_path: "objects/sha256/aa/record.json".to_string(),
            pointer_relative_path: "invocations/01/pointer.json".to_string(),
            newly_published: true,
        };
        let point = point_row(&record).unwrap();
        let run = run_row(&record, &receipt).unwrap();
        assert_eq!(run.acquisition_status, "complete");
        assert!(
            !run.claim_eligible,
            "complete acquisition is not enough without effective-codegen proof"
        );
        assert_eq!(run.terminal_failed_repetition, None);

        let digest_for = |point: &PointRow, run: &RunRow| {
            let mut point_digest = projected_table_digest("BenchmarkPoint");
            digest_projected_row(&mut point_digest, point, "BenchmarkPoint").unwrap();
            let mut run_digest = projected_table_digest("BenchmarkRun");
            digest_projected_row(&mut run_digest, run, "BenchmarkRun").unwrap();
            projected_rows_sha256(point_digest, 1, run_digest, 1).unwrap()
        };
        let projected = digest_for(&point, &run);
        let generation = generation_id(
            &schema_sha256(),
            &transform_sha256(),
            &"b".repeat(64),
            &projected,
        );

        let mut changed_hidden_run_field = run.clone();
        changed_hidden_run_field.archive_object.push_str(".changed");
        let changed_projected = digest_for(&point, &changed_hidden_run_field);
        assert_ne!(projected, changed_projected);
        assert_ne!(
            generation,
            generation_id(
                &schema_sha256(),
                &transform_sha256(),
                &"b".repeat(64),
                &changed_projected,
            )
        );
        assert_ne!(
            generation,
            generation_id(
                &schema_sha256(),
                &"c".repeat(64),
                &"b".repeat(64),
                &projected,
            ),
            "transform contract changes must create a different generation"
        );
    }

    #[test]
    fn censored_acquisition_projects_terminal_state_and_denies_claims() {
        let mut record = crate::record::tests::valid_record_fixture();
        record.acquisition.status = crate::record::AcquisitionStatusV1::Censored;
        record.acquisition.observed_repetitions = 1;
        record.acquisition.terminal = Some(
            crate::record::AcquisitionTerminalV1::new(
                1,
                crate::record::AcquisitionTerminalStageV1::Measure,
                "worker_timeout",
            )
            .unwrap(),
        );
        record.measurements.raw_samples.truncate(1);
        let elapsed = record.measurements.raw_samples[0].elapsed_us;
        record.measurements.wall_clock.min_us = elapsed;
        record.measurements.wall_clock.p50_us = elapsed;
        record.measurements.wall_clock.max_us = elapsed;
        crate::record::validate_run_record(&record).unwrap();

        let receipt = crate::archive::ArchiveReceiptV1 {
            archive_format_version: ARCHIVE_FORMAT_VERSION,
            invocation_id: record.invocation.invocation_id.clone(),
            record_sha256: "a".repeat(64),
            object_relative_path: "objects/sha256/aa/record.json".to_string(),
            pointer_relative_path: "invocations/01/pointer.json".to_string(),
            newly_published: true,
        };
        let row = run_row(&record, &receipt).unwrap();
        assert_eq!(row.acquisition_status, "censored");
        assert!(!row.claim_eligible);
        assert_eq!(row.terminal_failed_repetition, Some(1));
        assert_eq!(row.terminal_stage.as_deref(), Some("measure"));
        assert_eq!(row.terminal_code.as_deref(), Some("worker_timeout"));
    }

    #[test]
    fn projected_metadata_has_individual_and_aggregate_byte_ceilings() {
        let oversized = PointRow {
            point_id: "a".repeat(64),
            point_name: "point".to_string(),
            point_identity_version: 1,
            scenario: "branch-merge-v1".to_string(),
            run_spec_json: "x".repeat(MAX_PROJECTED_ROW_BYTES),
        };
        let error = validate_projected_row_size(&oversized, "BenchmarkPoint").unwrap_err();
        assert_eq!(error.code, "projection_row_too_large");

        let mut retained = RetainedMetadataBudget::default();
        retained.add(MAX_RETAINED_METADATA_BYTES, "test").unwrap();
        let error = retained.add(1, "test").unwrap_err();
        assert_eq!(error.code, "projection_metadata_too_large");
    }

    #[cfg(unix)]
    #[test]
    fn rebuild_lock_wait_is_bounded_and_typed() {
        let holder = tempfile::tempdir().unwrap();
        let holder_path = fs::canonicalize(holder.path()).unwrap();
        let root = holder_path.join("projection");
        fs::create_dir(&root).unwrap();
        let held =
            acquire_rebuild_lock_blocking(&root, Duration::from_secs(1), Duration::from_millis(1))
                .unwrap();
        let started = Instant::now();
        let error = acquire_rebuild_lock_blocking(
            &root,
            Duration::from_millis(25),
            Duration::from_millis(2),
        )
        .unwrap_err();
        assert_eq!(error.code, "projection_rebuild_lock_timeout");
        assert!(started.elapsed() < Duration::from_secs(1));
        drop(held);
    }

    #[cfg(unix)]
    #[test]
    fn rebuild_lock_replacement_does_not_split_the_critical_section() {
        let holder = tempfile::tempdir().unwrap();
        let holder_path = fs::canonicalize(holder.path()).unwrap();
        let root = holder_path.join("projection");
        let moved = holder_path.join("projection-old");
        fs::create_dir(&root).unwrap();
        let opened_path = holder_path.join("child-opened");
        let acquired_path = holder_path.join("child-acquired");
        let release_path = holder_path.join("release-child");
        let original =
            acquire_rebuild_lock_blocking(&root, Duration::from_secs(1), Duration::from_millis(1))
                .unwrap();

        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("projection::tests::rebuild_lock_replacement_subprocess_helper")
            .arg("--ignored")
            .env("OMNIGRAPH_BENCH_TEST_LOCK_PATH", &root)
            .env("OMNIGRAPH_BENCH_TEST_LOCK_OPENED", &opened_path)
            .env("OMNIGRAPH_BENCH_TEST_LOCK_ACQUIRED", &acquired_path)
            .env("OMNIGRAPH_BENCH_TEST_LOCK_RELEASE", &release_path)
            .spawn()
            .unwrap();
        if !wait_for_test_path(&opened_path, Duration::from_secs(5)) {
            let _ = child.kill();
            let _ = child.wait();
            panic!("lock test child did not report its first opened descriptor");
        }

        // The child has opened the old inode and is blocked behind `original`.
        // Replace the root name and hold the replacement inode before
        // releasing the old one. A process that trusted only its open
        // descriptor would now enter a split critical section.
        fs::rename(&root, &moved).unwrap();
        fs::create_dir(&root).unwrap();
        let replacement =
            acquire_rebuild_lock_blocking(&root, Duration::from_secs(1), Duration::from_millis(1))
                .unwrap();
        drop(original);
        thread::sleep(Duration::from_millis(100));
        let acquired_while_replacement_held = acquired_path.exists();

        drop(replacement);
        if !wait_for_test_path(&acquired_path, Duration::from_secs(5)) {
            let _ = child.kill();
            let _ = child.wait();
            panic!("lock test child did not acquire the replacement inode");
        }
        fs::write(&release_path, b"release").unwrap();
        let status = child.wait().unwrap();
        assert!(status.success());
        assert!(
            !acquired_while_replacement_held,
            "child entered through the unlinked lock inode while the current pathname was locked"
        );
    }

    #[cfg(unix)]
    #[test]
    #[ignore = "invoked by rebuild_lock_replacement_does_not_split_the_critical_section"]
    fn rebuild_lock_replacement_subprocess_helper() {
        let Some(lock_path) = std::env::var_os("OMNIGRAPH_BENCH_TEST_LOCK_PATH") else {
            return;
        };
        let opened_path = PathBuf::from(
            std::env::var_os("OMNIGRAPH_BENCH_TEST_LOCK_OPENED").expect("opened marker path"),
        );
        let acquired_path = PathBuf::from(
            std::env::var_os("OMNIGRAPH_BENCH_TEST_LOCK_ACQUIRED").expect("acquired marker path"),
        );
        let release_path = PathBuf::from(
            std::env::var_os("OMNIGRAPH_BENCH_TEST_LOCK_RELEASE").expect("release marker path"),
        );
        let lock_path = PathBuf::from(lock_path);
        let lock = acquire_rebuild_lock_blocking_with_open_observer(
            &lock_path,
            Duration::from_secs(10),
            Duration::from_millis(2),
            |attempt, _| {
                if attempt == 1 {
                    fs::write(&opened_path, b"opened").map_err(|error| {
                        ProjectionError::io("test_lock_marker_write_failed", &opened_path, error)
                    })?;
                }
                Ok(())
            },
        )
        .unwrap();
        fs::write(&acquired_path, b"acquired").unwrap();
        assert!(wait_for_test_path(&release_path, Duration::from_secs(5)));
        drop(lock);
    }

    #[cfg(unix)]
    #[test]
    fn current_publication_fails_closed_if_root_is_replaced_after_locking() {
        let holder = tempfile::tempdir().unwrap();
        let holder_path = fs::canonicalize(holder.path()).unwrap();
        let root = holder_path.join("projection");
        let moved = holder_path.join("projection-old");
        let generation_id = "a".repeat(64);
        let generation = root.join(GENERATIONS_DIRECTORY).join(&generation_id);
        fs::create_dir_all(&generation).unwrap();
        let manifest = ProjectionManifestV1 {
            format_version: PROJECTION_FORMAT_VERSION,
            generation_id,
            schema_sha256: "b".repeat(64),
            transform_version: PROJECTION_TRANSFORM_VERSION,
            transform_sha256: "c".repeat(64),
            inventory_sha256: "d".repeat(64),
            projected_rows_sha256: "e".repeat(64),
            record_count: 0,
            point_count: 0,
            graph_commit_id: "graph-head".to_string(),
            graph_relative_path: GRAPH_DIRECTORY.to_string(),
        };
        write_new_manifest(&generation, &manifest).unwrap();
        let root = require_root_directory(&root, "test_projection_root").unwrap();
        let lock = ProjectionRebuildLock {
            root_path: root.clone(),
            root: acquire_rebuild_lock_blocking(
                &root,
                Duration::from_secs(1),
                Duration::from_millis(1),
            )
            .unwrap(),
        };

        let error = publish_current_with_observer(&root, &manifest, &lock, || {
            fs::rename(&root, &moved).map_err(|error| {
                ProjectionError::io("test_projection_root_replace_failed", &root, error)
            })?;
            fs::create_dir(&root).map_err(|error| {
                ProjectionError::io("test_projection_root_replace_failed", &root, error)
            })
        })
        .unwrap_err();

        assert_eq!(error.code, "projection_rebuild_lock_replaced");
        assert!(!root.join(CURRENT_FILE).exists());
        assert!(!moved.join(CURRENT_FILE).exists());
        assert!(fs::read_dir(&moved).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".current-staging-")
        }));
    }

    #[cfg(unix)]
    #[test]
    fn current_publication_reads_manifest_through_locked_root_descriptor() {
        let holder = tempfile::tempdir().unwrap();
        let holder_path = fs::canonicalize(holder.path()).unwrap();
        let root = holder_path.join("projection");
        let moved = holder_path.join("projection-original");
        let replacement = holder_path.join("projection-replacement");
        let generation_id = "a".repeat(64);
        let original_generation = root.join(GENERATIONS_DIRECTORY).join(&generation_id);
        let replacement_generation = replacement.join(GENERATIONS_DIRECTORY).join(&generation_id);
        fs::create_dir_all(&original_generation).unwrap();
        fs::create_dir_all(&replacement_generation).unwrap();
        let manifest = ProjectionManifestV1 {
            format_version: PROJECTION_FORMAT_VERSION,
            generation_id,
            schema_sha256: "b".repeat(64),
            transform_version: PROJECTION_TRANSFORM_VERSION,
            transform_sha256: "c".repeat(64),
            inventory_sha256: "d".repeat(64),
            projected_rows_sha256: "e".repeat(64),
            record_count: 0,
            point_count: 0,
            graph_commit_id: "original-graph-head".to_string(),
            graph_relative_path: GRAPH_DIRECTORY.to_string(),
        };
        let mut replacement_manifest = manifest.clone();
        replacement_manifest.graph_commit_id = "replacement-graph-head".to_string();
        write_new_manifest(&original_generation, &manifest).unwrap();
        write_new_manifest(&replacement_generation, &replacement_manifest).unwrap();
        let expected_manifest_sha256 = sha256_hex(&serde_json::to_vec(&manifest).unwrap());
        let root = require_root_directory(&root, "test_projection_root").unwrap();
        let lock = ProjectionRebuildLock {
            root_path: root.clone(),
            root: acquire_rebuild_lock_blocking(
                &root,
                Duration::from_secs(1),
                Duration::from_millis(1),
            )
            .unwrap(),
        };

        publish_current_with_observers(
            &root,
            &manifest,
            &lock,
            || {
                fs::rename(&root, &moved).map_err(|error| {
                    ProjectionError::io("test_projection_root_replace_failed", &root, error)
                })?;
                fs::rename(&replacement, &root).map_err(|error| {
                    ProjectionError::io("test_projection_root_replace_failed", &root, error)
                })
            },
            || {
                fs::rename(&root, &replacement).map_err(|error| {
                    ProjectionError::io("test_projection_root_restore_failed", &root, error)
                })?;
                fs::rename(&moved, &root).map_err(|error| {
                    ProjectionError::io("test_projection_root_restore_failed", &root, error)
                })
            },
            || Ok(()),
        )
        .unwrap();

        let pointer: CurrentPointerV1 = read_canonical_json(
            &root.join(CURRENT_FILE),
            MAX_POINTER_BYTES,
            "test_projection_current_invalid",
        )
        .unwrap();
        assert_eq!(pointer.generation_id, manifest.generation_id);
        assert_eq!(pointer.manifest_sha256, expected_manifest_sha256);
    }

    #[cfg(unix)]
    fn wait_for_test_path(path: &Path, timeout: Duration) -> bool {
        let started = Instant::now();
        while started.elapsed() < timeout {
            if path.exists() {
                return true;
            }
            thread::sleep(Duration::from_millis(2));
        }
        path.exists()
    }

    #[test]
    fn fixed_queries_encode_the_bounded_parameterized_page_contract() {
        assert_eq!(PROJECTION_QUERIES.matches("limit 101").count(), 2);
        assert_eq!(PROJECTION_QUERIES.matches("limit 100").count(), 3);
        assert!(
            PROJECTION_QUERIES.contains("$point.point_id > $after_key"),
            "point cursors must remain parameterized and exclusive"
        );
        assert!(
            PROJECTION_QUERIES.contains("$run.invocation_id > $after_key"),
            "run and inventory cursors must remain parameterized and exclusive"
        );
    }

    #[test]
    fn archive_and_projection_roots_must_be_disjoint() {
        let root = tempfile::tempdir().unwrap();
        let archive = root.path().join("archive");
        fs::create_dir(&archive).unwrap();
        let projection = archive.join("projection");
        let archive = fs::canonicalize(archive).unwrap();
        let projection = resolve_prospective_path(&projection).unwrap();
        let error = reject_overlapping_roots(&archive, &projection).unwrap_err();
        assert_eq!(error.code, "projection_archive_overlap");
    }

    #[tokio::test]
    async fn empty_archive_rebuild_is_queryable_and_idempotent() {
        let root = tempfile::tempdir().unwrap();
        let archive = root.path().join("archive");
        let projection = root.path().join("projection");
        fs::create_dir(&archive).unwrap();

        let first = rebuild_projection(&archive, &projection).await.unwrap();
        assert!(!first.reused);
        assert_eq!(first.record_count, 0);
        assert_eq!(first.point_count, 0);
        assert_eq!(list_points(&projection).await.unwrap().num_rows(), 0);

        let generations = projection.join(GENERATIONS_DIRECTORY);
        let reconciled = reconcile_generation_directory_blocking(&generations).unwrap();
        assert_eq!(
            reconciled,
            BTreeSet::from([first.generation_id.clone()]),
            "the first reuse decision must come from the reconciled generation inventory"
        );

        let second = rebuild_projection(&archive, &projection).await.unwrap();
        assert!(second.reused);
        assert_eq!(second.generation_id, first.generation_id);
        assert_eq!(second.graph_commit_id, first.graph_commit_id);

        let continuation = list_points_page(
            &projection,
            1,
            Some(ProjectionCursorV1::Points {
                generation_id: first.generation_id.clone(),
                after_point_id: "a".repeat(64),
            }),
        )
        .await
        .unwrap();
        assert_eq!(continuation.generation_id, first.generation_id);
        assert!(continuation.rows.is_empty());
        assert!(continuation.next_cursor.is_none());

        let error = list_points_page(&projection, 0, None).await.unwrap_err();
        assert_eq!(error.code, "projection_page_limit_invalid");
        let error = list_points_page(&projection, MAX_PROJECTION_PAGE_SIZE + 1, None)
            .await
            .unwrap_err();
        assert_eq!(error.code, "projection_page_limit_invalid");

        let error = list_points_page(
            &projection,
            1,
            Some(ProjectionCursorV1::RunsForPoint {
                generation_id: first.generation_id,
                point_id: "a".repeat(64),
                after_invocation_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_string(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, "projection_cursor_scope_mismatch");
    }

    #[tokio::test]
    async fn canonical_record_round_trips_through_archive_and_projection_queries() {
        let root = tempfile::tempdir().unwrap();
        let root_path = fs::canonicalize(root.path()).unwrap();
        let archive = root_path.join("archive");
        let projection = root_path.join("projection");
        crate::archive::preflight_archive_publication(&archive).unwrap();
        let record = crate::record::tests::valid_record_fixture();
        let receipt = crate::archive::publish_record(&archive, &record).unwrap();

        let build = rebuild_projection(&archive, &projection).await.unwrap();
        assert_eq!(build.record_count, 1);
        assert_eq!(build.point_count, 1);
        assert_eq!(build.schema_sha256, schema_sha256());
        assert_eq!(build.transform_version, PROJECTION_TRANSFORM_VERSION);
        assert_eq!(build.transform_sha256, transform_sha256());
        require_sha256(&build.projected_rows_sha256, "build row digest").unwrap();
        let manifest: ProjectionManifestV1 = read_canonical_json(
            &projection
                .join(GENERATIONS_DIRECTORY)
                .join(&build.generation_id)
                .join(MANIFEST_FILE),
            MAX_MANIFEST_BYTES,
            "test_manifest_invalid",
        )
        .unwrap();
        assert_eq!(manifest.transform_version, PROJECTION_TRANSFORM_VERSION);
        assert_eq!(manifest.transform_sha256, transform_sha256());
        assert_eq!(
            manifest.generation_id,
            generation_id(
                &manifest.schema_sha256,
                &manifest.transform_sha256,
                &manifest.inventory_sha256,
                &manifest.projected_rows_sha256,
            )
        );

        let points = list_points_page(&projection, 1, None).await.unwrap();
        assert_eq!(points.generation_id, build.generation_id);
        assert_eq!(points.rows.len(), 1);
        assert_eq!(points.rows[0]["point_id"], record.run.point_id);
        assert!(points.next_cursor.is_none());

        let runs = list_runs_for_point_page(&projection, &record.run.point_id, 1, None)
            .await
            .unwrap();
        assert_eq!(runs.generation_id, build.generation_id);
        assert_eq!(runs.rows.len(), 1);
        assert_eq!(
            runs.rows[0]["invocation_id"],
            record.invocation.invocation_id
        );
        assert_eq!(runs.rows[0]["record_sha256"], receipt.record_sha256);
        assert_eq!(
            runs.rows[0]["machine_resource_control_json"],
            serde_json::to_string(&record.machine.resource_control).unwrap()
        );
        assert_eq!(
            runs.rows[0]["machine_scheduling_json"],
            serde_json::to_string(&record.machine.scheduling).unwrap()
        );
        assert_eq!(
            runs.rows[0]["machine_resource_limits_json"],
            serde_json::to_string(&record.machine.resource_limits).unwrap()
        );
        assert!(runs.next_cursor.is_none());
    }

    #[tokio::test]
    async fn concurrent_rebuilds_accept_one_verified_logical_generation() {
        let root = tempfile::tempdir().unwrap();
        let archive = root.path().join("archive");
        let projection = root.path().join("projection");
        fs::create_dir(&archive).unwrap();

        let (left, right) = tokio::join!(
            rebuild_projection(&archive, &projection),
            rebuild_projection(&archive, &projection),
        );
        let left = left.unwrap();
        let right = right.unwrap();
        assert_eq!(left.generation_id, right.generation_id);
        assert_eq!(left.graph_commit_id, right.graph_commit_id);
        assert!(left.reused || right.reused);
        assert_eq!(list_points(&projection).await.unwrap().num_rows(), 0);
    }

    #[tokio::test]
    async fn projection_schema_accepts_rows_and_named_queries() {
        let root = tempfile::tempdir().unwrap();
        let graph = root.path().join("graph");
        let db = Omnigraph::init(path_to_utf8(&graph).unwrap(), PROJECTION_SCHEMA)
            .await
            .unwrap();
        let point_id = "a".repeat(64);
        let record_sha256 = "b".repeat(64);
        let point = PointRow {
            point_id: point_id.clone(),
            point_name: "point".to_string(),
            point_identity_version: 1,
            scenario: "branch-merge-v1".to_string(),
            run_spec_json: "{}".to_string(),
        };
        let point_line = serialize_graph_row(&NodeEnvelope {
            node_type: "BenchmarkPoint",
            data: &point,
        })
        .unwrap();
        load_batch(&db, &point_line).await.unwrap();
        let second_point_id = "c".repeat(64);
        let second_point = PointRow {
            point_id: second_point_id.clone(),
            point_name: "point-two".to_string(),
            point_identity_version: 1,
            scenario: "branch-merge-v1".to_string(),
            run_spec_json: "{}".to_string(),
        };
        let second_point_line = serialize_graph_row(&NodeEnvelope {
            node_type: "BenchmarkPoint",
            data: &second_point,
        })
        .unwrap();
        load_batch(&db, &second_point_line).await.unwrap();

        let first_invocation_id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let second_invocation_id = "01ARZ3NDEKTSV4RRFFQ69G5FAW";
        let run = RunRow {
            invocation_id: first_invocation_id.to_string(),
            record_sha256: record_sha256.clone(),
            archive_object: "objects/record.json".to_string(),
            archive_pointer: "invocations/pointer.json".to_string(),
            session_id: "session".to_string(),
            invoked_at_unix_ms: 1,
            case_id: "case".to_string(),
            case_digest: "c".repeat(64),
            package_version: "0.10.0".to_string(),
            source_commit: "d".repeat(40),
            source_tree_dirty: false,
            build_profile: "release".to_string(),
            build_opt_level: "3".to_string(),
            debug_assertions: false,
            target_triple: "aarch64-apple-darwin".to_string(),
            rustc_version: "rustc 1.97.1".to_string(),
            build_declared_release_lto: "thin".to_string(),
            build_declared_release_codegen_units: 16,
            build_declared_release_strip: true,
            build_cargo_encoded_rustflags_present: false,
            build_release_profile_environment_overrides_supported: true,
            build_effective_codegen_options_proved: false,
            worker_executable_sha256: "e".repeat(64),
            sut_fingerprint: "f".repeat(64),
            sut_json: "{}".to_string(),
            machine_fingerprint: "1".repeat(64),
            machine_format_version: 1,
            machine_os_name: "macos".to_string(),
            machine_os_version: "26.0".to_string(),
            machine_kernel_version: "25.0".to_string(),
            machine_architecture: "aarch64".to_string(),
            machine_cpu_model: "Apple".to_string(),
            machine_logical_cores: 8,
            machine_physical_cores: 8,
            machine_total_memory_bytes: 16,
            machine_resource_control_json: "{\"kind\":\"macos-native\"}".to_string(),
            machine_scheduling_json:
                "{\"nice_level\":0,\"policy\":\"other\",\"priority\":31,\"reset_on_fork\":false}"
                    .to_string(),
            machine_resource_limits_json:
                "{\"scope_version\":1,\"values_sha256\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"}"
                    .to_string(),
            machine_label: "hostname-sha256:label".to_string(),
            backend_fingerprint: "2".repeat(64),
            backend_json: "{}".to_string(),
            fixture_manifest_sha256: "3".repeat(64),
            fixture_logical_sha256: "4".repeat(64),
            fixture_physical_sha256: "5".repeat(64),
            acquisition_status: "complete".to_string(),
            claim_eligible: false,
            terminal_failed_repetition: None,
            terminal_stage: None,
            terminal_code: None,
            requested_repetitions: 1,
            observed_repetitions: 1,
            min_us: 10,
            p50_us: 10,
            max_us: 10,
            p95_us: None,
            p95_supported: false,
            wall_evidence: "directional".to_string(),
            floor_multiplier_millis: 2_000,
            lance_data_plane_logical_calls_min: 7,
            lance_data_plane_logical_calls_p50: 7,
            lance_data_plane_logical_calls_max: 7,
            control_plane_logical_calls_min: 2,
            control_plane_logical_calls_p50: 2,
            control_plane_logical_calls_max: 2,
            logical_counts_presence_json: r#"{"status":"observed"}"#.to_string(),
            physical_counts_presence_json: r#"{"status":"absent"}"#.to_string(),
        };
        let run_line = serialize_graph_row(&NodeEnvelope {
            node_type: "BenchmarkRun",
            data: &run,
        })
        .unwrap();
        let mut second_run: serde_json::Value = serde_json::from_str(&run_line).unwrap();
        let second_run_data = second_run["data"].as_object_mut().unwrap();
        second_run_data.insert(
            "invocation_id".to_string(),
            serde_json::json!(second_invocation_id),
        );
        second_run_data.insert(
            "record_sha256".to_string(),
            serde_json::json!("8".repeat(64)),
        );
        second_run_data.insert(
            "archive_object".to_string(),
            serde_json::json!("objects/second-record.json"),
        );
        second_run_data.insert(
            "archive_pointer".to_string(),
            serde_json::json!("invocations/second-pointer.json"),
        );
        let second_run_line = serde_json::to_string(&second_run).unwrap();
        let edge_line = serialize_graph_row(&EdgeEnvelope {
            edge: "Measures",
            from: first_invocation_id,
            to: &point_id,
            data: EdgeIdentity {
                id: format!("measures:{first_invocation_id}"),
            },
        })
        .unwrap();
        let second_edge_line = serialize_graph_row(&EdgeEnvelope {
            edge: "Measures",
            from: second_invocation_id,
            to: &point_id,
            data: EdgeIdentity {
                id: format!("measures:{second_invocation_id}"),
            },
        })
        .unwrap();
        load_batch(
            &db,
            &format!("{run_line}\n{edge_line}\n{second_run_line}\n{second_edge_line}"),
        )
        .await
        .unwrap();

        let mut inventory_params = ParamMap::new();
        inventory_params.insert("after_key".to_string(), Literal::String(String::new()));
        let inventory = db
            .query(
                "main",
                PROJECTION_QUERIES,
                "projection_inventory_page",
                &inventory_params,
            )
            .await
            .unwrap();
        assert_eq!(parse_inventory_page(&inventory, "").unwrap().len(), 2);
        let error = parse_inventory_page(&inventory, first_invocation_id).unwrap_err();
        assert_eq!(error.code, "projection_inventory_order_invalid");
        let mut params = ParamMap::new();
        params.insert("point_id".to_string(), Literal::String(point_id.clone()));
        params.insert("after_key".to_string(), Literal::String(String::new()));
        let runs = db
            .query(
                "main",
                PROJECTION_QUERIES,
                "list_runs_for_point_page",
                &params,
            )
            .await
            .unwrap();
        assert_eq!(runs.num_rows(), 2);
        let run_query = ProjectionQuery::ListRunsForPoint {
            point_id: point_id.clone(),
            limit: 1,
            after: None,
        };
        let first_run_page =
            page_from_query_result(runs, &"9".repeat(64), 1, "", &run_query).unwrap();
        assert_eq!(first_run_page.rows.len(), 1);
        let run_cursor = first_run_page.next_cursor.unwrap();
        assert_eq!(
            run_cursor,
            ProjectionCursorV1::RunsForPoint {
                generation_id: "9".repeat(64),
                point_id: point_id.clone(),
                after_invocation_id: first_invocation_id.to_string(),
            }
        );

        params.insert(
            "after_key".to_string(),
            Literal::String(first_invocation_id.to_string()),
        );
        let second_runs = db
            .query(
                "main",
                PROJECTION_QUERIES,
                "list_runs_for_point_page",
                &params,
            )
            .await
            .unwrap();
        let second_run_page = page_from_query_result(
            second_runs,
            &"9".repeat(64),
            1,
            first_invocation_id,
            &ProjectionQuery::ListRunsForPoint {
                point_id: point_id.clone(),
                limit: 1,
                after: Some(run_cursor),
            },
        )
        .unwrap();
        assert_eq!(second_run_page.rows.len(), 1);
        assert_eq!(
            second_run_page.rows[0]["invocation_id"],
            second_invocation_id
        );
        assert!(second_run_page.next_cursor.is_none());

        let mut point_params = ParamMap::new();
        point_params.insert("after_key".to_string(), Literal::String(String::new()));
        let first_result = db
            .query(
                "main",
                PROJECTION_QUERIES,
                "list_points_page",
                &point_params,
            )
            .await
            .unwrap();
        let first_page = page_from_query_result(
            first_result,
            &"9".repeat(64),
            1,
            "",
            &ProjectionQuery::ListPoints {
                limit: 1,
                after: None,
            },
        )
        .unwrap();
        assert_eq!(first_page.rows.len(), 1);
        let cursor = first_page.next_cursor.unwrap();
        assert_eq!(
            cursor,
            ProjectionCursorV1::Points {
                generation_id: "9".repeat(64),
                after_point_id: point_id.clone(),
            }
        );

        point_params.insert("after_key".to_string(), Literal::String(point_id));
        let second_result = db
            .query(
                "main",
                PROJECTION_QUERIES,
                "list_points_page",
                &point_params,
            )
            .await
            .unwrap();
        let second_page = page_from_query_result(
            second_result,
            &"9".repeat(64),
            1,
            &"a".repeat(64),
            &ProjectionQuery::ListPoints {
                limit: 1,
                after: Some(cursor),
            },
        )
        .unwrap();
        assert_eq!(second_page.rows.len(), 1);
        assert_eq!(second_page.rows[0]["point_id"], second_point_id);
        assert!(second_page.next_cursor.is_none());
    }

    #[test]
    fn logical_call_summaries_are_per_repetition_and_keep_planes_separate() {
        let mut record = crate::record::tests::valid_record_fixture();
        let second = &mut record.measurements.raw_samples[1];
        second.logical_store_calls.manifest = LogicalCallCounts {
            get: 20,
            ..Default::default()
        };
        second.logical_store_calls.table = LogicalCallCounts {
            put: 10,
            ..Default::default()
        };
        second.control_store_calls.read_text = 4;
        second.control_store_calls.mutation_calls = 3;
        // These are subcategories of mutation_calls and must not be added.
        second.control_store_calls.write_text = 100;
        second.control_store_calls.delete = 100;

        let summary = logical_call_summaries(&record).unwrap();
        assert_eq!(
            summary.lance_data_plane,
            MinP50Max {
                min: 6,
                p50: 6,
                max: 30,
            }
        );
        assert_eq!(
            summary.control_plane,
            MinP50Max {
                min: 2,
                p50: 2,
                max: 7,
            }
        );
    }

    #[test]
    fn stale_builds_are_removed_and_published_generation_retention_is_bounded() {
        let root = tempfile::tempdir().unwrap();
        let root_path = fs::canonicalize(root.path()).unwrap();
        let generations = root_path.join(GENERATIONS_DIRECTORY);
        fs::create_dir(&generations).unwrap();
        let stale = generations.join(".build-crashed");
        fs::create_dir(&stale).unwrap();
        fs::write(stale.join("residue"), b"stale").unwrap();
        let mut generation_ids = Vec::new();
        for digit in 0..MAX_RETAINED_GENERATIONS {
            let id = format!("{digit:x}").repeat(64);
            fs::create_dir(generations.join(&id)).unwrap();
            generation_ids.push(id);
        }

        let retained = reconcile_generation_directory_blocking(&generations).unwrap();
        assert!(!stale.exists());
        assert_eq!(retained.len(), MAX_RETAINED_GENERATIONS);
        enforce_generation_retention(&retained, &generation_ids[0], &root_path).unwrap();
        let error =
            enforce_generation_retention(&retained, &"f".repeat(64), &root_path).unwrap_err();
        assert_eq!(error.code, "projection_generation_retention_exceeded");
        assert!(error.message.contains("never deleted automatically"));
    }
}
