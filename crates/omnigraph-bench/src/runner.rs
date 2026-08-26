//! Execution of resolved benchmark plans against the embedded local engine.
//!
//! Runner-v1 intentionally has a narrow support envelope: local verified
//! APFS storage, an already-diverged branch-merge fixture frozen once as a
//! clonefile template, and one fresh attested worker process per repetition.
//! Every worker restores the same stable path, performs the declared read-only
//! cache-preparation treatment behind a preparation-write firewall, and
//! executes exactly one bounded merge followed by exact verification. Durable
//! run records are finalized from its successful evidence by the archive
//! layer; fixture caching, cold page-cache control, S3 reset, and AWS
//! orchestration remain separate slices.

use std::error::Error;
use std::ffi::OsStr;
use std::fmt::{Display, Formatter};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures::FutureExt;
use lance::io::WrappingObjectStore;
use omnigraph::db::{MergeOutcome, Omnigraph};
use omnigraph::instrumentation::{
    CountingStorageAdapter, MergeWriteProbes, QueryIoProbes, StorageReadCounts,
    with_merge_write_probes, with_query_io_probes,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::branch_merge::{
    BranchMergePlan, FixtureBuildSummary, FixturePreflight, SOURCE_BRANCH, TARGET_BRANCH,
    capture_protected_branch_heads, initialize_local_fixture, verify_merged_graph, warm_read_set,
};
use crate::case::{
    Attribution, Backend, CacheCondition, Data, EnginePreparation, FixtureBuilder,
    MAX_WARMUP_ITERATIONS, PageCacheCondition, ProcessLifecycle, ResetMode, State, WarmupProgram,
};
use crate::counting::{LogicalCallCounter, LogicalCallCounts};
use crate::environment::{LocalEnvironmentEvidence, verify_local_environment};
use crate::fixture_worker::supervise_fixture_build;
use crate::machine::{MachineIdentityV1, capture_machine_identity};
use crate::preparation::{PreparationWriteGate, guard_preparation_writes};
use crate::reset::{
    ClonefileTemplate, MetadataDigest, PhysicalDigest, TraversalLimits,
    accept_clonefile_template_handoff, copy_verified, digest_metadata_tree, digest_physical_tree,
    freeze_clonefile_template, verify_metadata_shape, verify_metadata_tree,
};
use crate::suite::{MAX_REPETITIONS_PER_CASE, MAX_SUITE_RUNS, MAX_TOTAL_REPETITIONS};
use crate::supervisor::{SupervisionInput, supervise_repetition};
use crate::worker_protocol::{WorkerBuildV1, digest_worker_executable};
use crate::{ResolvedRun, ResolvedSuite, validate_case};

pub const RUNNER_OUTPUT_VERSION: u32 = 1;
pub const FIXTURE_MANIFEST_FORMAT_VERSION: u32 = 1;
pub const FIXTURE_VALIDATOR_VERSION: u32 = 1;
pub(crate) const PHYSICAL_TREE_DIGEST_ALGORITHM: &str = "omnigraph-bench-physical-tree-v1";
const BUILD_PROFILE: &str = env!("OMNIGRAPH_BENCH_BUILD_PROFILE");
const BUILD_OPT_LEVEL: &str = env!("OMNIGRAPH_BENCH_BUILD_OPT_LEVEL");
const SOURCE_GIT_COMMIT: &str = env!("OMNIGRAPH_BENCH_SOURCE_GIT_COMMIT");
const SOURCE_WORKTREE_DIRTY: &str = env!("OMNIGRAPH_BENCH_SOURCE_WORKTREE_DIRTY");
const MAX_RECORDED_ENV_VALUE_BYTES: usize = 256;
const SOURCE_COMMIT: &str = env!("OMNIGRAPH_BENCH_SOURCE_COMMIT");
const SOURCE_DIRTY: &str = env!("OMNIGRAPH_BENCH_SOURCE_DIRTY");
const TARGET_TRIPLE: &str = env!("OMNIGRAPH_BENCH_TARGET_TRIPLE");
const RUSTC_VERSION: &str = env!("OMNIGRAPH_BENCH_RUSTC_VERSION");
const DECLARED_RELEASE_LTO: &str = env!("OMNIGRAPH_BENCH_DECLARED_RELEASE_LTO");
const DECLARED_RELEASE_CODEGEN_UNITS: &str = env!("OMNIGRAPH_BENCH_DECLARED_RELEASE_CODEGEN_UNITS");
const DECLARED_RELEASE_STRIP: &str = env!("OMNIGRAPH_BENCH_DECLARED_RELEASE_STRIP");
const CARGO_ENCODED_RUSTFLAGS_PRESENT: &str =
    env!("OMNIGRAPH_BENCH_CARGO_ENCODED_RUSTFLAGS_PRESENT");
const RELEASE_PROFILE_ENVIRONMENT_OVERRIDES: &str =
    env!("OMNIGRAPH_BENCH_RELEASE_PROFILE_ENVIRONMENT_OVERRIDES");
const EFFECTIVE_CODEGEN_OPTIONS_PROVED: &str =
    env!("OMNIGRAPH_BENCH_EFFECTIVE_CODEGEN_OPTIONS_PROVED");
const RUN_OWNER_STACK_BYTES: usize = 64 * 1024 * 1024;
const FIXTURE_BUILD_WATCHDOG: Duration = Duration::from_secs(3_600);

/// Invocation-only options. No option may override experiment identity.
#[derive(Debug, Clone, Default)]
pub struct RunOptions {
    /// Existing directory under which disposable fixture and repetition trees
    /// are created. The actual created tree is what environment probing checks.
    pub scratch_root: Option<PathBuf>,
    /// Executable that implements the private repetition-worker protocol.
    /// Isolated runner-v1 requires its bytes to equal the currently running
    /// parent executable, preventing fixture construction and measurement from
    /// silently using different engine builds.
    pub worker_executable: Option<PathBuf>,
}

/// Machine-readable runner failure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RunnerError {
    pub code: String,
    pub message: String,
    #[serde(flatten)]
    pub context: Box<RunnerErrorContext>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct RunnerErrorContext {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub case_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub point_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repetition: Option<u32>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub completed_runs: Vec<RunExecution>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub completed_samples: Vec<RepObservation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub settled_sample: Option<Box<RepObservation>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub child_process: Option<ChildProcessEvidence>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "serialize_optional_path_lossy")]
    pub quarantined_workspace: Option<PathBuf>,
}

impl RunnerError {
    pub(crate) fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            context: Box::default(),
        }
    }

    fn with_run_identity(mut self, run: &ResolvedRun) -> Self {
        self.context.case_id = Some(run.case.definition.id.clone());
        self.context.point_id = Some(run.case.point_id.clone());
        self
    }

    pub(crate) fn with_repetition(mut self, repetition: u32) -> Self {
        self.context.repetition.get_or_insert(repetition);
        self
    }

    fn with_completed_samples(mut self, samples: Vec<RepObservation>) -> Self {
        self.context.completed_samples = samples;
        self
    }

    fn with_completed_runs(mut self, runs: Vec<RunExecution>) -> Self {
        self.context.completed_runs = runs;
        self
    }

    pub(crate) fn with_settled_sample(mut self, sample: RepObservation) -> Self {
        self.context.settled_sample = Some(Box::new(sample));
        self
    }

    pub(crate) fn with_child_process(mut self, evidence: ChildProcessEvidence) -> Self {
        self.context.child_process = Some(evidence);
        self
    }

    fn with_quarantined_workspace(mut self, path: PathBuf) -> Self {
        self.context.quarantined_workspace = Some(path);
        self
    }
}

impl Display for RunnerError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl Error for RunnerError {}

pub type RunnerResult<T> = Result<T, RunnerError>;

/// Containment evidence retained when a fixture or repetition child fails.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ChildProcessEvidence {
    pub stage: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub declared_deadline_us: Option<u64>,
    pub measurement_watchdog_us: u64,
    pub supervisor_elapsed_us: u64,
    pub termination: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signal: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peak_rss_bytes: Option<u64>,
    pub direct_child_reaped: bool,
    pub process_group_gone: bool,
    pub stdio_closed_cleanly: bool,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub stderr_tail: String,
    pub stderr_truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "serialize_optional_path_lossy")]
    pub quarantined_workspace: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SuiteExecution {
    pub runner_output_version: u32,
    pub suite: String,
    #[serde(serialize_with = "serialize_path_lossy")]
    pub suite_path: PathBuf,
    pub runs: Vec<RunExecution>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RunExecution {
    pub runner_output_version: u32,
    pub case_id: String,
    #[serde(serialize_with = "serialize_path_lossy")]
    pub case_path: PathBuf,
    pub point_id: String,
    pub point_name: String,
    pub cache_condition: CacheCondition,
    pub requested_repetitions: u32,
    pub build: BuildEvidence,
    /// Exact process-effective identity reported by every measured worker.
    pub machine: MachineIdentityV1,
    pub environment: LocalEnvironmentEvidence,
    pub fixture: FixtureObservation,
    pub samples: Vec<RepObservation>,
    pub wall_clock: WallClockSummary,
    /// This execution projection is diagnostic output, not the immutable run
    /// record contract owned by the next telemetry slice.
    pub durable_record: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BuildEvidence {
    pub source_commit: String,
    pub source_tree_dirty: bool,
    pub cargo_profile: String,
    pub cargo_opt_level: String,
    pub debug_assertions: bool,
    /// Effective `LANCE_MEM_POOL_SIZE` environment condition inherited by the
    /// measured process, retained in a bounded representation.
    pub effective_lance_mem_pool_size: EffectiveEnvironmentValue,
    pub target_triple: String,
    pub rustc_version: String,
    pub declared_release_lto: String,
    pub declared_release_codegen_units: u32,
    pub declared_release_strip: bool,
    pub cargo_encoded_rustflags_present: bool,
    pub release_profile_environment_overrides_supported: bool,
    pub effective_codegen_options_proved: bool,
    pub engine_feature_flags: Vec<String>,
    pub enabled_techniques: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_executable_sha256: Option<String>,
}

/// Bounded evidence for one effective process environment value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "kebab-case")]
pub enum EffectiveEnvironmentValue {
    Unset,
    Utf8 { value: String },
    OversizedUtf8 { bytes: usize, sha256: String },
    NonUtf8 { bytes: usize, sha256: String },
}

fn serialize_path_lossy<S>(path: &Path, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&path.to_string_lossy())
}

fn serialize_optional_path_lossy<S>(
    path: &Option<PathBuf>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    path.as_deref()
        .map(Path::to_string_lossy)
        .serialize(serializer)
}

/// Canonical pre-measurement fixture evidence. The runner seals this after
/// exact logical validation and physical freezing, before it starts rep 0.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StampedFixtureManifestV1 {
    pub manifest_sha256: String,
    pub manifest: FixtureManifestV1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureManifestV1 {
    pub format_version: u32,
    pub logical: LogicalFixtureIdentityV1,
    pub physical: PhysicalFixtureIdentityV1,
    pub validation: FixtureValidationStampV1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogicalFixtureIdentityV1 {
    pub builder: FixtureBuilder,
    pub data: Data,
    pub state: State,
    pub logical_content_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PhysicalFixtureIdentityV1 {
    pub digest_algorithm: String,
    pub tree_sha256: String,
    pub files: u64,
    pub bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureValidationStampV1 {
    pub validator: String,
    pub validator_version: u32,
    pub validated_at_unix_ms: u64,
    pub logical_content_verified: bool,
    pub declared_state_verified: bool,
    pub frozen: bool,
}

impl FixtureValidationStampV1 {
    pub fn verified(validated_at_unix_ms: u64) -> Self {
        Self {
            validator: "omnigraph-bench-fixture-validator".to_string(),
            validator_version: FIXTURE_VALIDATOR_VERSION,
            validated_at_unix_ms,
            logical_content_verified: true,
            declared_state_verified: true,
            frozen: true,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FixtureObservation {
    pub preflight: FixturePreflight,
    pub stamp: StampedFixtureManifestV1,
    pub base_load_commits: usize,
    pub optimized_user_tables: usize,
    pub source_history_depth: u64,
    pub target_history_depth: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepObservation {
    pub repetition: u32,
    pub input_physical_digest_sha256: String,
    pub elapsed_us: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peak_rss_bytes: Option<u64>,
    pub outcome: String,
    pub phases: Vec<PhaseObservation>,
    pub route: MergeRouteObservation,
    pub logical_store_calls: LogicalStoreCallObservation,
    pub control_store_calls: ControlCallObservation,
    pub verification: VerificationObservation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhaseObservation {
    pub phase: String,
    pub total_us: u64,
    pub max_us: u64,
    pub interval_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergeRouteObservation {
    pub table_walk_intervals: u64,
    pub stage_merge_insert_calls: u64,
    pub stage_merge_insert_rows: u64,
    pub stage_known_present_update_calls: u64,
    pub stage_known_present_update_rows: u64,
    pub stage_fenced_insert_calls: u64,
    pub stage_fenced_insert_rows: u64,
    pub strict_insert_preflight_calls: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalStoreCallObservation {
    /// Calls against graph-manifest Lance datasets.
    pub manifest: LogicalCallCounts,
    /// Calls against user-table Lance datasets.
    pub table: LogicalCallCounts,
    pub physical_attempts_observed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlCallObservation {
    pub read_text: u64,
    pub read_text_if_exists: u64,
    pub read_text_versioned: u64,
    pub exists: u64,
    pub list_dir: u64,
    pub mutation_calls: u64,
    pub write_text: u64,
    pub delete: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerificationObservation {
    pub branch: String,
    pub tables: usize,
    pub rows: u64,
    pub exact_content: bool,
    pub source_exact_content: bool,
    pub main_exact_content: bool,
    pub protected_heads_unchanged: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WallClockSummary {
    pub observed_repetitions: u32,
    pub min_us: u64,
    pub p50_us: u64,
    pub max_us: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p95_us: Option<u64>,
    pub p95_supported: bool,
}

/// Execute every resolved run in suite order.
///
/// The complete envelope is re-sealed before any execution. Callers should
/// pass the output of [`crate::load_suite`]; manually constructed values must
/// exactly match the suite and case definitions currently loaded from the same
/// canonical paths.
pub async fn execute_suite(
    suite: &ResolvedSuite,
    options: &RunOptions,
) -> RunnerResult<SuiteExecution> {
    validate_execution_suite(suite)?;
    enforce_release_build()?;
    refuse_unmodeled_runtime_overrides()?;
    if suite.runs.len() > MAX_SUITE_RUNS {
        return Err(RunnerError::new(
            "resolved_plan_unbounded",
            format!(
                "resolved suite contains {} runs; the execution limit is {MAX_SUITE_RUNS}",
                suite.runs.len()
            ),
        ));
    }
    let total_repetitions = suite.runs.iter().try_fold(0_u64, |total, run| {
        total.checked_add(u64::from(run.repetitions))
    });
    if total_repetitions.is_none_or(|total| total > MAX_TOTAL_REPETITIONS) {
        return Err(RunnerError::new(
            "resolved_plan_unbounded",
            format!(
                "resolved suite repetition total exceeds the execution limit of {MAX_TOTAL_REPETITIONS}"
            ),
        ));
    }
    let mut runs = Vec::with_capacity(suite.runs.len());
    for run in &suite.runs {
        let execution =
            match Box::pin(execute_run_inner(run, options, ExecutionGuards::public())).await {
                Ok(execution) => execution,
                Err(error) => {
                    return Err(error.with_run_identity(run).with_completed_runs(runs));
                }
            };
        runs.push(execution);
    }
    Ok(SuiteExecution {
        runner_output_version: RUNNER_OUTPUT_VERSION,
        suite: suite.definition.name.clone(),
        suite_path: suite.suite_path.clone(),
        runs,
    })
}

/// Execute one resolved suite entry.
pub async fn execute_run(run: &ResolvedRun, options: &RunOptions) -> RunnerResult<RunExecution> {
    enforce_release_build().map_err(|error| error.with_run_identity(run))?;
    refuse_unmodeled_runtime_overrides().map_err(|error| error.with_run_identity(run))?;
    Box::pin(execute_run_inner(run, options, ExecutionGuards::public()))
        .await
        .map_err(|error| error.with_run_identity(run))
}

#[derive(Debug, Clone, Copy)]
struct ExecutionGuards {
    verify_environment: bool,
    isolate_repetitions: bool,
    allow_plain_copy: bool,
}

impl ExecutionGuards {
    fn public() -> Self {
        Self {
            verify_environment: true,
            isolate_repetitions: true,
            allow_plain_copy: false,
        }
    }
}

pub(crate) fn enforce_release_build() -> RunnerResult<()> {
    if let Err(configuration) =
        validate_supported_build_configuration(&worker_build_attestation(String::new()))
    {
        return Err(RunnerError::new(
            "release_build_required",
            format!(
                "wall-clock execution requires Cargo profile=release, Cargo-reported opt-level=2, debug-assertions=false, the checked-in release-profile declaration, no build-script-visible encoded Rust flags, and no unsupported release-profile environment overrides: {}; effective LTO/codegen/strip options remain explicitly unproved until a controlled build receipt is available; run `cargo run --release --locked -p omnigraph-bench -- suite run ...`",
                configuration.message
            ),
        ));
    }
    Ok(())
}

/// Runner-v1 has no typed runtime-configuration block. Refuse every
/// `OMNIGRAPH_*` process override rather than recording an empty configuration
/// while the engine silently consumes one. Values are never inspected or
/// echoed because some supported variables can carry credentials.
pub(crate) fn refuse_unmodeled_runtime_overrides() -> RunnerResult<()> {
    validate_runtime_overrides(std::env::vars_os())
}

fn is_omnigraph_runtime_override(name: &OsStr) -> bool {
    name.to_string_lossy().starts_with("OMNIGRAPH_")
}

fn validate_runtime_overrides(
    variables: impl IntoIterator<Item = (std::ffi::OsString, std::ffi::OsString)>,
) -> RunnerResult<()> {
    for (name, value) in variables {
        if !is_omnigraph_runtime_override(&name) {
            continue;
        }
        let Some(expected) = expected_build_attestation_environment(&name) else {
            return Err(RunnerError::new(
                "unsupported_runtime_override",
                "runner-v1 does not represent OMNIGRAPH_* runtime overrides in its SUT identity; unset them before execution",
            ));
        };
        if value != OsStr::new(expected) {
            return Err(RunnerError::new(
                "build_attestation_environment_mismatch",
                "a runtime OMNIGRAPH_BENCH_* build-attestation value differs from the facts compiled into this executable",
            ));
        }
    }
    Ok(())
}

fn expected_build_attestation_environment(name: &OsStr) -> Option<&'static str> {
    match name.to_str()? {
        "OMNIGRAPH_BENCH_BUILD_PROFILE" => Some(BUILD_PROFILE),
        "OMNIGRAPH_BENCH_BUILD_OPT_LEVEL" => Some(BUILD_OPT_LEVEL),
        "OMNIGRAPH_BENCH_SOURCE_COMMIT" => Some(SOURCE_COMMIT),
        "OMNIGRAPH_BENCH_SOURCE_DIRTY" => Some(SOURCE_DIRTY),
        "OMNIGRAPH_BENCH_TARGET_TRIPLE" => Some(TARGET_TRIPLE),
        "OMNIGRAPH_BENCH_RUSTC_VERSION" => Some(RUSTC_VERSION),
        "OMNIGRAPH_BENCH_DECLARED_RELEASE_LTO" => Some(DECLARED_RELEASE_LTO),
        "OMNIGRAPH_BENCH_DECLARED_RELEASE_CODEGEN_UNITS" => Some(DECLARED_RELEASE_CODEGEN_UNITS),
        "OMNIGRAPH_BENCH_DECLARED_RELEASE_STRIP" => Some(DECLARED_RELEASE_STRIP),
        "OMNIGRAPH_BENCH_CARGO_ENCODED_RUSTFLAGS_PRESENT" => Some(CARGO_ENCODED_RUSTFLAGS_PRESENT),
        "OMNIGRAPH_BENCH_RELEASE_PROFILE_ENVIRONMENT_OVERRIDES" => {
            Some(RELEASE_PROFILE_ENVIRONMENT_OVERRIDES)
        }
        "OMNIGRAPH_BENCH_EFFECTIVE_CODEGEN_OPTIONS_PROVED" => {
            Some(EFFECTIVE_CODEGEN_OPTIONS_PROVED)
        }
        _ => None,
    }
}

fn build_evidence(worker: Option<&WorkerBuildV1>) -> RunnerResult<BuildEvidence> {
    let build = worker
        .cloned()
        .unwrap_or_else(|| worker_build_attestation(String::new()));
    let source_tree_dirty = build.source_tree_dirty.ok_or_else(|| {
        RunnerError::new(
            "worker_build_attestation_invalid",
            "worker build-time source-tree state is unavailable",
        )
    })?;
    if worker.is_some() {
        validate_worker_build_attestation(&build, &build.executable_sha256)?;
    } else {
        validate_build_text(&build.source_commit, "source commit")?;
        validate_build_text(&build.target_triple, "target triple")?;
        validate_build_text(&build.rustc_version, "rustc version")?;
    }
    let declared_release_codegen_units = build.declared_release_codegen_units.ok_or_else(|| {
        RunnerError::new(
            "worker_build_attestation_invalid",
            "worker checked-in release-profile codegen-unit declaration is unavailable",
        )
    })?;
    let declared_release_strip = build.declared_release_strip.ok_or_else(|| {
        RunnerError::new(
            "worker_build_attestation_invalid",
            "worker checked-in release-profile strip declaration is unavailable",
        )
    })?;
    let cargo_encoded_rustflags_present =
        build.cargo_encoded_rustflags_present.ok_or_else(|| {
            RunnerError::new(
                "worker_build_attestation_invalid",
                "worker build-script observation of CARGO_ENCODED_RUSTFLAGS is unavailable",
            )
        })?;
    let release_profile_environment_overrides_supported = build
        .release_profile_environment_overrides_supported
        .ok_or_else(|| {
            RunnerError::new(
                "worker_build_attestation_invalid",
                "worker build-script observation of release-profile environment overrides is unavailable",
            )
        })?;
    Ok(BuildEvidence {
        source_commit: build.source_commit,
        source_tree_dirty,
        cargo_profile: build.cargo_profile,
        cargo_opt_level: build.cargo_opt_level,
        debug_assertions: build.debug_assertions,
        effective_lance_mem_pool_size: *build.effective_lance_mem_pool_size,
        target_triple: build.target_triple,
        rustc_version: build.rustc_version,
        declared_release_lto: build.declared_release_lto,
        declared_release_codegen_units,
        declared_release_strip,
        cargo_encoded_rustflags_present,
        release_profile_environment_overrides_supported,
        effective_codegen_options_proved: build.effective_codegen_options_proved,
        engine_feature_flags: build.engine_feature_flags,
        enabled_techniques: build.enabled_techniques,
        worker_executable_sha256: worker.map(|build| build.executable_sha256.clone()),
    })
}

pub(crate) fn worker_build_attestation(executable_sha256: String) -> WorkerBuildV1 {
    let engine_feature_flags = omnigraph::instrumentation::enabled_engine_cargo_features()
        .iter()
        .map(|feature| (*feature).to_string())
        .collect();
    WorkerBuildV1 {
        source_commit: SOURCE_COMMIT.to_string(),
        source_tree_dirty: match SOURCE_DIRTY {
            "true" => Some(true),
            "false" => Some(false),
            _ => None,
        },
        cargo_profile: BUILD_PROFILE.to_string(),
        cargo_opt_level: BUILD_OPT_LEVEL.to_string(),
        debug_assertions: cfg!(debug_assertions),
        effective_lance_mem_pool_size: Box::new(classify_effective_environment_value(
            std::env::var_os("LANCE_MEM_POOL_SIZE").as_deref(),
        )),
        target_triple: TARGET_TRIPLE.to_string(),
        rustc_version: RUSTC_VERSION.to_string(),
        declared_release_lto: DECLARED_RELEASE_LTO.to_string(),
        declared_release_codegen_units: DECLARED_RELEASE_CODEGEN_UNITS.parse().ok(),
        declared_release_strip: match DECLARED_RELEASE_STRIP {
            "true" => Some(true),
            "false" => Some(false),
            _ => None,
        },
        cargo_encoded_rustflags_present: match CARGO_ENCODED_RUSTFLAGS_PRESENT {
            "false" => Some(false),
            "true" => Some(true),
            _ => None,
        },
        release_profile_environment_overrides_supported: match RELEASE_PROFILE_ENVIRONMENT_OVERRIDES
        {
            "supported" => Some(true),
            "unsupported" => Some(false),
            _ => None,
        },
        effective_codegen_options_proved: EFFECTIVE_CODEGEN_OPTIONS_PROVED == "true",
        engine_feature_flags,
        // Runner-v1 exposes no runtime technique selector, and the runtime
        // override gate refuses the environment-based engine controls.
        enabled_techniques: Vec::new(),
        executable_sha256,
    }
}

fn classify_effective_environment_value(value: Option<&OsStr>) -> EffectiveEnvironmentValue {
    let Some(value) = value else {
        return EffectiveEnvironmentValue::Unset;
    };
    let encoded = value.as_encoded_bytes();
    let bytes = encoded.len();
    let sha256 = || format!("{:x}", Sha256::digest(encoded));
    match value.to_str() {
        Some(value) if bytes <= MAX_RECORDED_ENV_VALUE_BYTES => EffectiveEnvironmentValue::Utf8 {
            value: value.to_string(),
        },
        Some(_) => EffectiveEnvironmentValue::OversizedUtf8 {
            bytes,
            sha256: sha256(),
        },
        None => EffectiveEnvironmentValue::NonUtf8 {
            bytes,
            sha256: sha256(),
        },
    }
}

pub(crate) fn validate_worker_build_attestation(
    build: &WorkerBuildV1,
    expected_executable_sha256: &str,
) -> RunnerResult<()> {
    validate_supported_build_configuration(build)?;
    if build.source_tree_dirty.is_none() {
        return Err(RunnerError::new(
            "worker_build_attestation_invalid",
            "worker build-time source-tree state is unavailable",
        ));
    }
    if !valid_lower_hex(&build.source_commit, &[40, 64]) {
        return Err(RunnerError::new(
            "worker_build_attestation_invalid",
            "worker source commit must be exactly 40 or 64 lowercase hexadecimal characters",
        ));
    }
    validate_build_text(&build.target_triple, "target triple")?;
    validate_build_text(&build.rustc_version, "rustc version")?;
    if !valid_lower_hex(&build.executable_sha256, &[64])
        || build.executable_sha256 != expected_executable_sha256
    {
        return Err(RunnerError::new(
            "worker_build_attestation_invalid",
            "worker executable digest does not match the parent-attested executable",
        ));
    }
    Ok(())
}

fn validate_supported_build_configuration(build: &WorkerBuildV1) -> RunnerResult<()> {
    if build.cargo_profile != "release"
        || build.cargo_opt_level != "2"
        || build.debug_assertions
        || build.declared_release_lto != "thin"
        || build.declared_release_codegen_units != Some(16)
        || build.declared_release_strip != Some(true)
    {
        return Err(RunnerError::new(
            "worker_build_attestation_invalid",
            "worker must report Cargo profile=release, Cargo opt-level=2, debug-assertions=false, and the checked-in release declaration lto=thin/codegen-units=16/strip=true",
        ));
    }
    if build.cargo_encoded_rustflags_present != Some(false) {
        return Err(RunnerError::new(
            "worker_build_attestation_invalid",
            "runner-v1 refuses build-script-visible CARGO_ENCODED_RUSTFLAGS",
        ));
    }
    if build.release_profile_environment_overrides_supported != Some(true) {
        return Err(RunnerError::new(
            "worker_build_attestation_invalid",
            "runner-v1 refuses unsupported CARGO_PROFILE_RELEASE_* overrides",
        ));
    }
    if build.effective_codegen_options_proved {
        return Err(RunnerError::new(
            "worker_build_attestation_invalid",
            "runner-v1 has no controlled build receipt and must not claim that effective codegen options were proved",
        ));
    }
    let expected_engine_features = omnigraph::instrumentation::enabled_engine_cargo_features();
    if build.engine_feature_flags.len() != expected_engine_features.len()
        || !build
            .engine_feature_flags
            .iter()
            .map(String::as_str)
            .eq(expected_engine_features.iter().copied())
    {
        return Err(RunnerError::new(
            "worker_build_attestation_invalid",
            "worker engine feature flags do not match the linked omnigraph-engine build",
        ));
    }
    if !build.enabled_techniques.is_empty() {
        return Err(RunnerError::new(
            "worker_build_attestation_invalid",
            "runner-v1 does not admit configured engine techniques",
        ));
    }
    Ok(())
}

fn validate_build_text(value: &str, noun: &str) -> RunnerResult<()> {
    if value.is_empty()
        || value == "unknown"
        || value.len() > 1024
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        return Err(RunnerError::new(
            "worker_build_attestation_invalid",
            format!("worker {noun} is unavailable or non-canonical"),
        ));
    }
    Ok(())
}

fn valid_lower_hex(value: &str, lengths: &[usize]) -> bool {
    lengths.contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[derive(Debug, Clone)]
struct ResolvedWorker {
    executable: PathBuf,
    executable_sha256: String,
}

async fn execute_run_inner(
    run: &ResolvedRun,
    options: &RunOptions,
    guards: ExecutionGuards,
) -> RunnerResult<RunExecution> {
    validate_execution_run(run)?;
    let worker = resolve_worker(options, guards)?;
    if run.case.definition.protocol.attribution != Attribution::PerPhase {
        return Err(RunnerError::new(
            "unsupported_runner_axis",
            "runner-v1 requires protocol.attribution: per-phase so every measured merge carries exact phase evidence",
        ));
    }
    if run.case.definition.protocol.reset != ResetMode::LocalClonefile && !guards.allow_plain_copy {
        return Err(RunnerError::new(
            "unsupported_runner_axis",
            "runner-v1 wall-clock execution requires protocol.reset: local-clonefile; byte copying would pre-warm file contents",
        ));
    }

    let plan = BranchMergePlan::try_from(&run.case)
        .map_err(|error| RunnerError::new("unsupported_runner_axis", error.to_string()))?;
    let preflight = plan
        .preflight()
        .map_err(|error| RunnerError::new("runner_preflight_failed", error.to_string()))?;
    let owned_run = run.clone();
    let owned_options = options.clone();
    tokio::task::spawn_blocking(move || {
        std::thread::Builder::new()
            .name("omnigraph-bench-run-owner".to_string())
            .stack_size(RUN_OWNER_STACK_BYTES)
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|error| {
                        RunnerError::new(
                            "runner_owner_runtime_failed",
                            format!("could not start cancellation-independent run owner: {error}"),
                        )
                    })?;
                runtime.block_on(execute_owned_run(
                    owned_run,
                    owned_options,
                    guards,
                    worker,
                    plan,
                    preflight,
                ))
            })
            .map_err(|error| {
                RunnerError::new(
                    "runner_owner_thread_failed",
                    format!("could not start cancellation-independent run owner: {error}"),
                )
            })?
            .join()
            .map_err(|_| {
                RunnerError::new(
                    "runner_owner_panicked",
                    "cancellation-independent run owner panicked",
                )
            })?
    })
    .await
    .map_err(|error| {
        RunnerError::new(
            "runner_owner_task_failed",
            format!("cancellation-independent run owner failed: {error}"),
        )
    })?
}

async fn execute_owned_run(
    run: ResolvedRun,
    options: RunOptions,
    guards: ExecutionGuards,
    worker: Option<ResolvedWorker>,
    plan: BranchMergePlan,
    preflight: FixturePreflight,
) -> RunnerResult<RunExecution> {
    if !guards.isolate_repetitions && !cfg!(test) {
        return Err(RunnerError::new(
            "process_isolation_required",
            "every current cache condition requires one fresh worker process per repetition",
        ));
    }
    let workspace = scratch_workspace(&options)?;
    let environment = local_environment(&run, workspace.path(), guards.verify_environment)?;
    if environment.available_bytes < preflight.required_scratch_bytes {
        return Err(RunnerError::new(
            "insufficient_scratch_capacity",
            format!(
                "runner-v1 requires at least {} available scratch bytes for this fixture recipe, but {} are available at {}",
                preflight.required_scratch_bytes,
                environment.available_bytes,
                environment.mount_point
            ),
        ));
    }

    let fixture = std::panic::AssertUnwindSafe(Box::pin(async {
        let active_root = workspace.path().join("active");
        let template_root = workspace.path().join("template");
        std::fs::create_dir(&active_root).map_err(|error| {
            RunnerError::new(
                "fixture_directory_error",
                format!("could not create {}: {error}", active_root.display()),
            )
        })?;
        let limits = TraversalLimits::default();
        let (build, template, physical) = if guards.isolate_repetitions {
            let executable = worker.as_ref().ok_or_else(|| {
                RunnerError::new(
                    "worker_executable_required",
                    "contained fixture construction requires an explicit worker executable",
                )
            })?;
            let handoff = supervise_fixture_build(
                &executable.executable,
                &run.case,
                &active_root,
                &template_root,
                workspace.path(),
                FIXTURE_BUILD_WATCHDOG,
            )?;
            let template = accept_clonefile_template_handoff(
                &active_root,
                &template_root,
                handoff.physical.clone(),
                handoff.template_metadata,
                limits,
            )
            .map_err(|error| {
                RunnerError::new(
                    "fixture_handoff_failed",
                    format!(
                        "could not accept contained fixture template {}: {error}",
                        template_root.display()
                    ),
                )
            })?;
            (
                handoff.summary,
                FrozenTemplate::Clonefile(template),
                handoff.physical,
            )
        } else {
            // Owning-layer tests keep their tiny fixtures in-process. Public
            // wall-clock execution always takes the bounded child path above.
            let active_uri = utf8_path(&active_root, "active fixture")?;
            let build = initialize_local_fixture(active_uri, &plan)
                .await
                .map_err(|error| RunnerError::new("fixture_build_failed", error.to_string()))?;
            let template = if run.case.definition.protocol.reset == ResetMode::LocalClonefile {
                FrozenTemplate::Clonefile(
                    freeze_clonefile_template(&active_root, &template_root, limits).map_err(
                        |error| {
                            RunnerError::new(
                                "fixture_freeze_failed",
                                format!(
                                    "could not freeze {} as an APFS clonefile template: {error}",
                                    active_root.display()
                                ),
                            )
                        },
                    )?,
                )
            } else {
                FrozenTemplate::plain_copy_for_test(&active_root, &template_root, limits)?
            };
            remove_active_tree(&active_root)?;
            let physical = template.physical_digest().clone();
            (build, template, physical)
        };
        let validated_at_unix_ms = unix_time_millis()?;
        let stamp = stamp_frozen_fixture(&run, &build, &physical, validated_at_unix_ms)?;
        Ok::<_, RunnerError>((build, template, stamp))
    }))
    .catch_unwind()
    .await;
    let (build, template, fixture_stamp) = match fixture {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => {
            let quarantined = workspace.keep();
            return Err(error.with_quarantined_workspace(quarantined));
        }
        Err(_) => {
            let quarantined = workspace.keep();
            return Err(RunnerError::new(
                "fixture_builder_panicked",
                "fixture builder panicked; its workspace was quarantined instead of being removed",
            )
            .with_quarantined_workspace(quarantined));
        }
    };

    let (samples, attested_worker_build, machine) = if guards.isolate_repetitions {
        let worker = worker.clone().ok_or_else(|| {
            RunnerError::new(
                "worker_executable_required",
                "process-isolated execution requires an explicit repetition-worker executable",
            )
        })?;
        let supervised_run = run.clone();
        let supervised_stamp = fixture_stamp.clone();
        let repetitions = run.repetitions;
        let (samples, worker_build, machine) = tokio::task::spawn_blocking(move || {
            supervise_workspace(
                workspace,
                template,
                supervised_run,
                supervised_stamp,
                worker,
                repetitions,
            )
        })
        .await
        .map_err(|error| {
            RunnerError::new(
                "worker_supervisor_panicked",
                format!("repetition supervisor task failed: {error}"),
            )
        })??;
        (samples, Some(worker_build), machine)
    } else {
        let machine = capture_machine_identity().map_err(|error| {
            RunnerError::new(
                "machine_identity_capture_failed",
                format!("could not capture in-process runner machine identity: {error}"),
            )
        })?;
        let samples = run_in_process_repetitions(&workspace, &template, &run, &plan).await?;
        (samples, None, machine)
    };

    let wall_clock = summarize_wall_clock(&samples)?;
    Ok(RunExecution {
        runner_output_version: RUNNER_OUTPUT_VERSION,
        case_id: run.case.definition.id.clone(),
        case_path: run.case_path.clone(),
        point_id: run.case.point_id.clone(),
        point_name: run.case.point_name.clone(),
        cache_condition: run.case.definition.environment.cache_condition.clone(),
        requested_repetitions: run.repetitions,
        build: build_evidence(attested_worker_build.as_ref())?,
        machine,
        environment,
        fixture: FixtureObservation {
            preflight,
            stamp: fixture_stamp,
            base_load_commits: build.base_load_commits,
            optimized_user_tables: build.optimized_user_tables,
            source_history_depth: build.source_history_depth,
            target_history_depth: build.target_history_depth,
        },
        samples,
        wall_clock,
        durable_record: false,
    })
}

fn unix_time_millis() -> RunnerResult<u64> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            RunnerError::new(
                "system_clock_invalid",
                format!("system clock is before the Unix epoch: {error}"),
            )
        })?;
    u64::try_from(elapsed.as_millis()).map_err(|_| {
        RunnerError::new(
            "system_clock_invalid",
            "current Unix timestamp in milliseconds does not fit u64",
        )
    })
}

fn stamp_frozen_fixture(
    run: &ResolvedRun,
    build: &FixtureBuildSummary,
    physical: &PhysicalDigest,
    validated_at_unix_ms: u64,
) -> RunnerResult<StampedFixtureManifestV1> {
    if !valid_lower_hex(&build.logical_content_sha256, &[64])
        || !valid_lower_hex(&physical.digest_sha256, &[64])
        || physical.files == 0
        || physical.bytes == 0
        || validated_at_unix_ms == 0
    {
        return Err(RunnerError::new(
            "fixture_stamp_invalid",
            "validated fixture evidence is incomplete and cannot be sealed before measurement",
        ));
    }
    let manifest = FixtureManifestV1 {
        format_version: FIXTURE_MANIFEST_FORMAT_VERSION,
        logical: LogicalFixtureIdentityV1 {
            builder: run.case.definition.fixture.builder.clone(),
            data: run.case.definition.fixture.data.clone(),
            state: run.case.definition.fixture.state.clone(),
            logical_content_sha256: build.logical_content_sha256.clone(),
        },
        physical: PhysicalFixtureIdentityV1 {
            digest_algorithm: PHYSICAL_TREE_DIGEST_ALGORITHM.to_string(),
            tree_sha256: physical.digest_sha256.clone(),
            files: physical.files,
            bytes: physical.bytes,
        },
        validation: FixtureValidationStampV1::verified(validated_at_unix_ms),
    };
    let bytes = serde_json::to_vec(&manifest).map_err(|error| {
        RunnerError::new(
            "fixture_stamp_failed",
            format!("could not serialize canonical pre-measurement fixture manifest: {error}"),
        )
    })?;
    let mut digest = Sha256::new();
    digest.update(bytes);
    Ok(StampedFixtureManifestV1 {
        manifest_sha256: format!("{:x}", digest.finalize()),
        manifest,
    })
}

fn validate_execution_run(run: &ResolvedRun) -> RunnerResult<()> {
    if !(1..=MAX_REPETITIONS_PER_CASE).contains(&run.repetitions) {
        return Err(RunnerError::new(
            "resolved_plan_unbounded",
            format!(
                "resolved run requests {} repetitions; the execution limit is 1..={MAX_REPETITIONS_PER_CASE}",
                run.repetitions
            ),
        ));
    }
    let sealed = validate_case(run.case.definition.clone())
        .into_result()
        .map_err(|diagnostics| {
            RunnerError::new(
                "resolved_plan_invalid",
                diagnostics
                    .into_iter()
                    .map(|diagnostic| {
                        format!(
                            "{} at {}: {}",
                            diagnostic.code, diagnostic.path, diagnostic.message
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("; "),
            )
        })?;
    if sealed != run.case {
        return Err(RunnerError::new(
            "resolved_plan_tampered",
            "resolved case identity or digest does not match its validated definition",
        ));
    }
    Ok(())
}

fn validate_execution_suite(suite: &ResolvedSuite) -> RunnerResult<()> {
    let sealed = crate::load_suite(&suite.suite_path)
        .into_result()
        .map_err(|diagnostics| {
            RunnerError::new(
                "resolved_plan_invalid",
                diagnostics
                    .into_iter()
                    .map(|diagnostic| {
                        format!(
                            "{} at {}: {}",
                            diagnostic.code, diagnostic.path, diagnostic.message
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("; "),
            )
        })?;
    if sealed != *suite {
        return Err(RunnerError::new(
            "resolved_plan_tampered",
            "resolved suite envelope does not match the suite and cases currently sealed at its declared path",
        ));
    }
    for run in &suite.runs {
        validate_execution_run(run)?;
    }
    Ok(())
}

fn resolve_worker(
    options: &RunOptions,
    guards: ExecutionGuards,
) -> RunnerResult<Option<ResolvedWorker>> {
    if !guards.isolate_repetitions {
        return Ok(None);
    }
    let executable = options.worker_executable.as_ref().ok_or_else(|| {
        RunnerError::new(
            "worker_executable_required",
            "RunOptions.worker_executable must name the exact runner binary used for isolated repetitions",
        )
    })?;
    let current_executable = std::env::current_exe().map_err(|error| {
        RunnerError::new(
            "worker_executable_error",
            format!("could not resolve the running parent executable: {error}"),
        )
    })?;
    resolve_bound_worker(executable, &current_executable).map(Some)
}

fn resolve_bound_worker(
    executable: &Path,
    current_executable: &Path,
) -> RunnerResult<ResolvedWorker> {
    let executable = std::fs::canonicalize(executable).map_err(|error| {
        RunnerError::new(
            "worker_executable_error",
            format!(
                "could not resolve worker executable {}: {error}",
                executable.display()
            ),
        )
    })?;
    let current_executable = std::fs::canonicalize(current_executable).map_err(|error| {
        RunnerError::new(
            "worker_executable_error",
            format!(
                "could not canonicalize running parent executable {}: {error}",
                current_executable.display()
            ),
        )
    })?;
    let parent_digest = digest_worker_executable(&current_executable).map_err(|error| {
        RunnerError::new(
            "worker_executable_error",
            format!(
                "could not attest running parent executable {}: {error}",
                current_executable.display()
            ),
        )
    })?;
    let digest = digest_worker_executable(&executable).map_err(|error| {
        RunnerError::new(
            "worker_executable_error",
            format!(
                "could not attest worker executable {}: {error}",
                executable.display()
            ),
        )
    })?;
    if digest != parent_digest {
        return Err(RunnerError::new(
            "worker_parent_executable_mismatch",
            "isolated runner-v1 requires the parent and configured worker executable to have identical bytes",
        ));
    }
    Ok(ResolvedWorker {
        executable,
        executable_sha256: digest,
    })
}

enum FrozenTemplate {
    Clonefile(ClonefileTemplate),
    PlainCopyForTest {
        template_root: PathBuf,
        active_root: PathBuf,
        physical: PhysicalDigest,
        metadata: MetadataDigest,
        limits: TraversalLimits,
    },
}

impl FrozenTemplate {
    fn plain_copy_for_test(
        active_root: &Path,
        template_root: &Path,
        limits: TraversalLimits,
    ) -> RunnerResult<Self> {
        let physical = digest_physical_tree(active_root, limits)
            .map_err(|error| RunnerError::new("fixture_digest_failed", error.to_string()))?;
        copy_verified(active_root, template_root, &physical, limits)
            .map_err(|error| RunnerError::new("fixture_copy_failed", error.to_string()))?;
        let metadata = digest_metadata_tree(template_root, limits)
            .map_err(|error| RunnerError::new("fixture_metadata_failed", error.to_string()))?;
        Ok(Self::PlainCopyForTest {
            template_root: template_root.to_path_buf(),
            active_root: active_root.to_path_buf(),
            physical,
            metadata,
            limits,
        })
    }

    fn physical_digest(&self) -> &PhysicalDigest {
        match self {
            Self::Clonefile(template) => template.physical_digest(),
            Self::PlainCopyForTest { physical, .. } => physical,
        }
    }

    fn active_root(&self) -> &Path {
        match self {
            Self::Clonefile(template) => template.active_root(),
            Self::PlainCopyForTest { active_root, .. } => active_root,
        }
    }

    fn verify_unchanged(&self) -> RunnerResult<()> {
        match self {
            Self::Clonefile(template) => template.verify_unchanged().map(|_| ()),
            Self::PlainCopyForTest {
                template_root,
                metadata,
                limits,
                ..
            } => verify_metadata_tree(template_root, metadata, *limits).map(|_| ()),
        }
        .map_err(|error| {
            RunnerError::new(
                "fixture_template_mutated",
                format!("frozen fixture template changed: {error}"),
            )
        })
    }

    fn restore_active(&self) -> RunnerResult<MetadataDigest> {
        match self {
            Self::Clonefile(template) => template
                .restore_active()
                .map(|prepared| prepared.metadata_digest().clone()),
            Self::PlainCopyForTest {
                template_root,
                active_root,
                physical,
                limits,
                ..
            } => copy_verified(template_root, active_root, physical, *limits)
                .and_then(|_| digest_metadata_tree(active_root, *limits)),
        }
        .map_err(|error| RunnerError::new("reset_failed", error.to_string()))
    }
}

fn run_supervised_repetitions(
    _workspace: &tempfile::TempDir,
    template: FrozenTemplate,
    run: ResolvedRun,
    fixture_stamp: StampedFixtureManifestV1,
    worker: ResolvedWorker,
    repetitions: u32,
) -> RunnerResult<(Vec<RepObservation>, WorkerBuildV1, MachineIdentityV1)> {
    if fixture_stamp.manifest.physical.tree_sha256 != template.physical_digest().digest_sha256 {
        return Err(RunnerError::new(
            "fixture_stamp_mismatch",
            "pre-measurement fixture stamp does not name the frozen repetition template",
        ));
    }
    let mut samples = Vec::with_capacity(repetitions as usize);
    let mut attested_build = None::<WorkerBuildV1>;
    let mut attested_machine = None::<MachineIdentityV1>;
    for repetition in 0..repetitions {
        template.verify_unchanged()?;
        let metadata = template.restore_active()?;
        let result = supervise_repetition(SupervisionInput {
            worker_executable: worker.executable.clone(),
            expected_worker_executable_sha256: worker.executable_sha256.clone(),
            expected_machine: attested_machine.clone(),
            fixture_manifest_sha256: fixture_stamp.manifest_sha256.clone(),
            repetition,
            case: run.case.clone(),
            repetition_root: template.active_root().to_path_buf(),
            physical_digest: template.physical_digest().clone(),
            metadata_digest: metadata,
            deadline: run
                .case
                .definition
                .protocol
                .deadline_seconds
                .map(Duration::from_secs),
            #[cfg(test)]
            auxiliary_deadline_override: None,
        });

        if result
            .as_ref()
            .err()
            .is_some_and(|error| !containment_proven(error))
        {
            return Err(result
                .expect_err("containment status came from an error")
                .with_completed_samples(samples));
        }

        let template_result = template.verify_unchanged();
        let cleanup_result = remove_active_tree(template.active_root());
        if let Err(error) = template_result {
            return Err(error.with_completed_samples(samples));
        }
        if let Err(error) = cleanup_result {
            return Err(error.with_completed_samples(samples));
        }
        match result {
            Ok(observed) => {
                if attested_build
                    .as_ref()
                    .is_some_and(|previous| previous != &observed.worker_build)
                {
                    return Err(RunnerError::new(
                        "worker_build_attestation_changed",
                        "repetitions from one run reported different worker build identities",
                    )
                    .with_completed_samples(samples));
                }
                if attested_machine
                    .as_ref()
                    .is_some_and(|previous| previous != &observed.machine)
                {
                    return Err(RunnerError::new(
                        "worker_machine_identity_changed",
                        "repetitions from one run reported different process-effective machine identities",
                    )
                    .with_completed_samples(samples));
                }
                attested_build.get_or_insert(observed.worker_build);
                attested_machine.get_or_insert(observed.machine);
                samples.push(observed.sample);
            }
            Err(error) => return Err(error.with_completed_samples(samples)),
        }
    }
    let attested_build = attested_build.ok_or_else(|| {
        RunnerError::new(
            "worker_build_attestation_missing",
            "successful supervised run produced no worker build attestation",
        )
    })?;
    let attested_machine = attested_machine.ok_or_else(|| {
        RunnerError::new(
            "worker_machine_identity_missing",
            "successful supervised run produced no worker machine identity",
        )
    })?;
    Ok((samples, attested_build, attested_machine))
}

fn supervise_workspace(
    workspace: tempfile::TempDir,
    template: FrozenTemplate,
    run: ResolvedRun,
    fixture_stamp: StampedFixtureManifestV1,
    worker: ResolvedWorker,
    repetitions: u32,
) -> RunnerResult<(Vec<RepObservation>, WorkerBuildV1, MachineIdentityV1)> {
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        run_supervised_repetitions(
            &workspace,
            template,
            run,
            fixture_stamp,
            worker,
            repetitions,
        )
    }));
    match outcome {
        Ok(Ok(samples)) => Ok(samples),
        Ok(Err(mut error)) if !containment_proven(&error) => {
            let quarantined = workspace.keep();
            let evidence = error
                .context
                .child_process
                .get_or_insert_with(ChildProcessEvidence::default);
            evidence.quarantined_workspace = Some(quarantined);
            Err(error)
        }
        Ok(Err(error)) => Err(error),
        Err(_) => {
            let quarantined = workspace.keep();
            Err(RunnerError::new(
                "worker_supervisor_panicked",
                "repetition supervisor panicked; the disposable workspace was quarantined",
            )
            .with_child_process(ChildProcessEvidence {
                stage: "supervisor-panic".to_string(),
                quarantined_workspace: Some(quarantined),
                ..ChildProcessEvidence::default()
            }))
        }
    }
}

fn containment_proven(error: &RunnerError) -> bool {
    error
        .context
        .child_process
        .as_ref()
        .is_some_and(|evidence| {
            evidence.direct_child_reaped
                && evidence.process_group_gone
                && evidence.stdio_closed_cleanly
        })
}

async fn run_in_process_repetitions(
    _workspace: &tempfile::TempDir,
    template: &FrozenTemplate,
    run: &ResolvedRun,
    plan: &BranchMergePlan,
) -> RunnerResult<Vec<RepObservation>> {
    // This path exists only for owning-layer tests. Public execution always
    // takes the supervised path above, which creates the fresh-per-repetition
    // process declared by every current cache condition.
    let mut samples = Vec::with_capacity(run.repetitions as usize);
    for repetition in 0..run.repetitions {
        template.verify_unchanged()?;
        let metadata = template.restore_active()?;
        let result = execute_rep(
            repetition,
            template.active_root(),
            template.physical_digest(),
            &metadata,
            plan,
            &run.case.definition.environment.cache_condition,
            run.case
                .definition
                .protocol
                .deadline_seconds
                .map(Duration::from_secs),
        )
        .await;
        template.verify_unchanged()?;
        remove_active_tree(template.active_root())?;
        match result {
            Ok(sample) => samples.push(sample),
            Err(error) => return Err(error.with_completed_samples(samples)),
        }
    }
    Ok(samples)
}

fn remove_active_tree(active_root: &Path) -> RunnerResult<()> {
    match std::fs::symlink_metadata(active_root) {
        Ok(metadata) if metadata.file_type().is_dir() => std::fs::remove_dir_all(active_root)
            .map_err(|error| {
                RunnerError::new(
                    "reset_cleanup_failed",
                    format!("could not remove {}: {error}", active_root.display()),
                )
            }),
        Ok(_) => Err(RunnerError::new(
            "reset_cleanup_failed",
            format!(
                "active fixture path changed to a non-directory: {}",
                active_root.display()
            ),
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(RunnerError::new(
            "reset_cleanup_failed",
            format!("could not inspect {}: {error}", active_root.display()),
        )),
    }
}

fn scratch_workspace(options: &RunOptions) -> RunnerResult<tempfile::TempDir> {
    match &options.scratch_root {
        Some(root) => tempfile::Builder::new()
            .prefix("omnigraph-bench-")
            .tempdir_in(root),
        None => tempfile::Builder::new()
            .prefix("omnigraph-bench-")
            .tempdir(),
    }
    .map_err(|error| {
        RunnerError::new(
            "scratch_directory_error",
            format!("could not create disposable benchmark workspace: {error}"),
        )
    })
}

fn local_environment(
    run: &ResolvedRun,
    scratch_path: &Path,
    verify: bool,
) -> RunnerResult<LocalEnvironmentEvidence> {
    let Backend::LocalFs {
        filesystem,
        storage_class,
    } = run.case.definition.environment.backend
    else {
        return Err(RunnerError::new(
            "unsupported_runner_axis",
            "runner-v1 supports local-fs only",
        ));
    };
    if !verify {
        return Ok(LocalEnvironmentEvidence {
            filesystem: format!("{filesystem:?}"),
            storage_class: format!("{storage_class:?}"),
            mount_point: "test-only".to_string(),
            storage_protocol: "test-only".to_string(),
            available_bytes: u64::MAX,
            probe: "internal-test-only",
        });
    }
    verify_local_environment(scratch_path, filesystem, storage_class)
        .map_err(|message| RunnerError::new("environment_mismatch", message))
}

async fn execute_rep(
    repetition: u32,
    root: &Path,
    input_digest: &PhysicalDigest,
    input_metadata: &MetadataDigest,
    plan: &BranchMergePlan,
    cache_condition: &CacheCondition,
    deadline: Option<Duration>,
) -> RunnerResult<RepObservation> {
    let mut signals = ImmediateMeasurementSignals;
    execute_rep_signaled(
        repetition,
        root,
        input_digest,
        input_metadata,
        plan,
        cache_condition,
        deadline,
        &mut signals,
    )
    .await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CachePreparationAction {
    PreparationOnly,
    WarmSameHandle { iterations: u32 },
    WarmThenReopen { iterations: u32 },
}

fn cache_preparation_action(condition: &CacheCondition) -> RunnerResult<CachePreparationAction> {
    match (
        condition.process,
        condition.engine,
        condition.page_cache,
        condition.program,
        condition.iterations,
    ) {
        (
            ProcessLifecycle::FreshPerRepetition,
            EnginePreparation::PreparationOnly,
            PageCacheCondition::Uncontrolled,
            WarmupProgram::None,
            0,
        ) => Ok(CachePreparationAction::PreparationOnly),
        (
            ProcessLifecycle::FreshPerRepetition,
            EnginePreparation::WarmedByProgram,
            PageCacheCondition::ProgramConditioned,
            WarmupProgram::BranchMergeReadSetV1,
            iterations,
        ) if (1..=MAX_WARMUP_ITERATIONS).contains(&iterations) => {
            Ok(CachePreparationAction::WarmSameHandle { iterations })
        }
        (
            ProcessLifecycle::FreshPerRepetition,
            EnginePreparation::ReopenedAfterProgram,
            PageCacheCondition::ProgramConditioned,
            WarmupProgram::BranchMergeReadSetV1,
            iterations,
        ) if (1..=MAX_WARMUP_ITERATIONS).contains(&iterations) => {
            Ok(CachePreparationAction::WarmThenReopen { iterations })
        }
        _ => Err(RunnerError::new(
            "unsupported_cache_condition",
            "runner-v1 refuses a cache-condition tuple that is not one of the schema's exact supported treatments",
        )),
    }
}

pub(crate) trait MeasurementSignals {
    fn ready(&mut self) -> RunnerResult<()>;
    fn settled(&mut self, elapsed_us: u64) -> RunnerResult<()>;
}

struct ImmediateMeasurementSignals;

impl MeasurementSignals for ImmediateMeasurementSignals {
    fn ready(&mut self) -> RunnerResult<()> {
        Ok(())
    }

    fn settled(&mut self, _elapsed_us: u64) -> RunnerResult<()> {
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn execute_rep_signaled<S: MeasurementSignals>(
    repetition: u32,
    root: &Path,
    input_digest: &PhysicalDigest,
    input_metadata: &MetadataDigest,
    plan: &BranchMergePlan,
    cache_condition: &CacheCondition,
    deadline: Option<Duration>,
    signals: &mut S,
) -> RunnerResult<RepObservation> {
    let manifest_counter = LogicalCallCounter::default();
    let table_counter = LogicalCallCounter::default();
    let query_probes = QueryIoProbes {
        manifest_wrapper: Some(Arc::new(manifest_counter.clone()) as Arc<dyn WrappingObjectStore>),
        table_wrapper: Some(Arc::new(table_counter.clone()) as Arc<dyn WrappingObjectStore>),
        ..Default::default()
    };
    with_query_io_probes(
        query_probes,
        execute_rep_body(
            repetition,
            root,
            input_digest,
            input_metadata,
            plan,
            cache_condition,
            deadline,
            manifest_counter,
            table_counter,
            signals,
        ),
    )
    .await
    .map_err(|error| error.with_repetition(repetition))
}

#[allow(clippy::too_many_arguments)]
async fn execute_rep_body<S: MeasurementSignals>(
    repetition: u32,
    root: &Path,
    input_digest: &PhysicalDigest,
    input_metadata: &MetadataDigest,
    plan: &BranchMergePlan,
    cache_condition: &CacheCondition,
    deadline: Option<Duration>,
    manifest_counter: LogicalCallCounter,
    table_counter: LogicalCallCounter,
    signals: &mut S,
) -> RunnerResult<RepObservation> {
    let root_uri = utf8_path(root, "repetition store")?;
    let preparation_action = cache_preparation_action(cache_condition)?;
    let (mut db, mut control_counts, mut preparation_gate) = open_counting(root_uri).await?;
    match preparation_action {
        CachePreparationAction::PreparationOnly => {}
        CachePreparationAction::WarmSameHandle { iterations }
        | CachePreparationAction::WarmThenReopen { iterations } => {
            warm_read_set(&db, plan, iterations)
                .await
                .map_err(|error| RunnerError::new("cache_preparation_failed", error.to_string()))?;
        }
    }
    let protected_heads = capture_protected_branch_heads(&db)
        .await
        .map_err(|error| RunnerError::new("protected_head_capture_failed", error.to_string()))?;
    if matches!(
        preparation_action,
        CachePreparationAction::WarmThenReopen { .. }
    ) {
        preparation_gate
            .validate_preparation()
            .map_err(|message| RunnerError::new("pre_measurement_write_detected", message))?;
        drop(db);
        (db, control_counts, preparation_gate) = open_counting(root_uri).await?;
    }

    // The engine handle must be read-write for the measured mutation. Prove
    // that open and the declared cache-preparation treatment issued no write through either
    // storage seam, then compare the complete metadata shape without reading
    // file contents. The clonefile syscall already proved byte identity.
    verify_metadata_shape(root, input_metadata, TraversalLimits::default()).map_err(|error| {
        RunnerError::new(
            "pre_measurement_shape_mismatch",
            format!(
                "repetition {repetition} did not retain the clonefile metadata shape through open and cache preparation: {error}"
            ),
        )
    })?;
    // Keep the storage mutation gate closed while Ready waits for the
    // supervisor's Begin. Once Begin arrives, clear all preparation/wait reads,
    // validate that no Lance mutation crossed the firewall, and open the gate
    // immediately before the measured operation.
    signals.ready()?;
    let preparation_manifest_calls = manifest_counter.take();
    let preparation_table_calls = table_counter.take();
    if preparation_manifest_calls.has_mutations() || preparation_table_calls.has_mutations() {
        return Err(RunnerError::new(
            "pre_measurement_write_detected",
            format!(
                "repetition {repetition} open/cache-preparation/ready-wait issued Lance object-store mutations (manifest={preparation_manifest_calls:?}, table={preparation_table_calls:?})"
            ),
        ));
    }
    let control_before = ControlSnapshot::read(&control_counts);
    preparation_gate
        .begin_measurement()
        .map_err(|message| RunnerError::new("pre_measurement_write_detected", message))?;
    let merge_probes = MergeWriteProbes::default();
    let started = Instant::now();
    let outcome = with_merge_write_probes(
        merge_probes.clone(),
        db.branch_merge(SOURCE_BRANCH, TARGET_BRANCH),
    )
    .await;
    let elapsed = started.elapsed();
    let elapsed_us = u64::try_from(elapsed.as_micros()).map_err(|_| {
        RunnerError::new(
            "duration_overflow",
            format!("repetition {repetition} duration does not fit u64 microseconds"),
        )
    })?;
    // The child supervisor uses this boundary to stop the hard mutation
    // deadline. It may terminate the process only before this signal; once the
    // future settled, exact verification runs under a separate bounded watch.
    signals.settled(elapsed_us)?;
    let outcome = outcome.map_err(|error| {
        RunnerError::new("merge_failed", format!("repetition {repetition}: {error}"))
    })?;
    let deadline_exceeded = deadline.is_some_and(|deadline| elapsed > deadline);

    let manifest_calls = manifest_counter.take();
    let table_calls = table_counter.take();
    let control_calls = control_before.delta(ControlSnapshot::read(&control_counts))?;
    if outcome != MergeOutcome::Merged {
        return Err(RunnerError::new(
            "vacuous_merge",
            format!(
                "repetition {repetition} returned {outcome:?}; expected the general three-way Merged route"
            ),
        ));
    }

    let phase_readings = merge_probes.merge_timing_snapshot();
    let table_walk = phase_readings
        .iter()
        .find(|reading| reading.phase == "TableWalk")
        .ok_or_else(|| {
            RunnerError::new(
                "missing_table_walk_phase",
                "merge timing snapshot did not contain the stable TableWalk phase",
            )
        })?;
    let expected_intervals = u64::try_from(plan.diverged_tables).map_err(|_| {
        RunnerError::new("interval_overflow", "diverged table count does not fit u64")
    })?;
    if table_walk.interval_count != expected_intervals {
        return Err(RunnerError::new(
            "vacuous_merge",
            format!(
                "repetition {repetition} completed {} TableWalk intervals; expected exactly {expected_intervals}",
                table_walk.interval_count
            ),
        ));
    }

    // Verification runs only after all measured clocks and counters are read.
    let verification = verify_merged_graph(&db, plan, &protected_heads)
        .await
        .map_err(|error| RunnerError::new("verification_failed", error.to_string()))?;
    drop(db);

    let sample = RepObservation {
        repetition,
        input_physical_digest_sha256: input_digest.digest_sha256.clone(),
        elapsed_us,
        peak_rss_bytes: None,
        outcome: "merged".to_string(),
        phases: phase_readings
            .into_iter()
            .map(|reading| PhaseObservation {
                phase: reading.phase.to_string(),
                total_us: reading.total_us,
                max_us: reading.max_us,
                interval_count: reading.interval_count,
            })
            .collect(),
        route: MergeRouteObservation {
            table_walk_intervals: merge_probes.table_walk_interval_count(),
            stage_merge_insert_calls: merge_probes.stage_merge_insert_calls(),
            stage_merge_insert_rows: merge_probes.stage_merge_insert_rows(),
            stage_known_present_update_calls: merge_probes.stage_known_present_update_calls(),
            stage_known_present_update_rows: merge_probes.stage_known_present_update_rows(),
            stage_fenced_insert_calls: merge_probes.stage_fenced_insert_calls(),
            stage_fenced_insert_rows: merge_probes.stage_fenced_insert_rows(),
            strict_insert_preflight_calls: merge_probes.strict_insert_preflight_calls(),
        },
        logical_store_calls: LogicalStoreCallObservation {
            manifest: manifest_calls,
            table: table_calls,
            physical_attempts_observed: false,
        },
        control_store_calls: control_calls,
        verification: VerificationObservation {
            branch: verification.target.branch,
            tables: verification.target.tables,
            rows: verification.target.rows,
            exact_content: true,
            source_exact_content: verification.source_exact_content,
            main_exact_content: verification.main_exact_content,
            protected_heads_unchanged: verification.protected_heads_unchanged,
        },
    };
    if deadline_exceeded {
        let deadline = deadline.expect("deadline_exceeded requires a declared deadline");
        Err(RunnerError::new(
            "merge_deadline_exceeded",
            format!(
                "repetition {repetition} exceeded the declared {} second deadline; the runner waited for the mutating operation to quiesce, captured its settled evidence, and then rejected the sample",
                deadline.as_secs()
            ),
        )
        .with_settled_sample(sample))
    } else {
        Ok(sample)
    }
}

async fn open_counting(
    root_uri: &str,
) -> RunnerResult<(Omnigraph, Arc<StorageReadCounts>, PreparationWriteGate)> {
    let storage = omnigraph::storage::storage_for_uri(root_uri).map_err(|error| {
        RunnerError::new(
            "storage_open_failed",
            format!("could not select storage for {root_uri}: {error}"),
        )
    })?;
    let (storage, preparation_gate) = guard_preparation_writes(storage, root_uri);
    let (storage, counts) = CountingStorageAdapter::new(storage);
    let db = Omnigraph::open_with_storage(root_uri, storage)
        .await
        .map_err(|error| {
            RunnerError::new(
                "engine_open_failed",
                format!("could not open repetition store {root_uri}: {error}"),
            )
        })?;
    Ok((db, counts, preparation_gate))
}

#[derive(Debug, Clone, Copy)]
struct ControlSnapshot {
    read_text: u64,
    read_text_if_exists: u64,
    read_text_versioned: u64,
    exists: u64,
    list_dir: u64,
    mutation_calls: u64,
    write_text: u64,
    delete: u64,
}

impl ControlSnapshot {
    fn read(counts: &StorageReadCounts) -> Self {
        Self {
            read_text: counts.read_text(),
            read_text_if_exists: counts.read_text_if_exists(),
            read_text_versioned: counts.read_text_versioned(),
            exists: counts.exists(),
            list_dir: counts.list_dir(),
            mutation_calls: counts.mutation_calls(),
            write_text: counts.write_text(),
            delete: counts.delete(),
        }
    }

    fn delta(self, after: Self) -> RunnerResult<ControlCallObservation> {
        fn checked(after: u64, before: u64, field: &str) -> RunnerResult<u64> {
            after.checked_sub(before).ok_or_else(|| {
                RunnerError::new(
                    "counter_regression",
                    format!("control-store counter '{field}' decreased during measurement"),
                )
            })
        }

        Ok(ControlCallObservation {
            read_text: checked(after.read_text, self.read_text, "read_text")?,
            read_text_if_exists: checked(
                after.read_text_if_exists,
                self.read_text_if_exists,
                "read_text_if_exists",
            )?,
            read_text_versioned: checked(
                after.read_text_versioned,
                self.read_text_versioned,
                "read_text_versioned",
            )?,
            exists: checked(after.exists, self.exists, "exists")?,
            list_dir: checked(after.list_dir, self.list_dir, "list_dir")?,
            mutation_calls: checked(after.mutation_calls, self.mutation_calls, "mutation_calls")?,
            write_text: checked(after.write_text, self.write_text, "write_text")?,
            delete: checked(after.delete, self.delete, "delete")?,
        })
    }
}

fn summarize_wall_clock(samples: &[RepObservation]) -> RunnerResult<WallClockSummary> {
    if samples.is_empty() {
        return Err(RunnerError::new(
            "empty_execution",
            "a resolved run produced no repetitions",
        ));
    }
    let mut durations = samples
        .iter()
        .map(|sample| sample.elapsed_us)
        .collect::<Vec<_>>();
    durations.sort_unstable();
    let observed_repetitions = u32::try_from(durations.len()).map_err(|_| {
        RunnerError::new("repetition_overflow", "observed repetitions do not fit u32")
    })?;
    let p95_supported = durations.len() >= 20;
    Ok(WallClockSummary {
        observed_repetitions,
        min_us: durations[0],
        p50_us: nearest_rank(&durations, 50),
        max_us: durations[durations.len() - 1],
        p95_us: p95_supported.then(|| nearest_rank(&durations, 95)),
        p95_supported,
    })
}

fn nearest_rank(sorted: &[u64], percentile: usize) -> u64 {
    debug_assert!(!sorted.is_empty());
    debug_assert!((1..=100).contains(&percentile));
    let rank = percentile
        .checked_mul(sorted.len())
        .expect("validated suite bounds keep percentile rank in usize")
        .div_ceil(100)
        .saturating_sub(1);
    sorted[rank]
}

fn utf8_path<'a>(path: &'a Path, label: &str) -> RunnerResult<&'a str> {
    path.to_str().ok_or_else(|| {
        RunnerError::new(
            "non_utf8_path",
            format!("{label} path is not valid UTF-8: {}", path.display()),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse_case;

    #[cfg(unix)]
    #[test]
    fn recovery_evidence_serializes_non_utf8_quarantine_paths_lossily() {
        use std::os::unix::ffi::OsStringExt;

        let path = PathBuf::from(std::ffi::OsString::from_vec(b"quarantined-\xff".to_vec()));
        let error = RunnerError::new("worker_failed", "worker failed")
            .with_quarantined_workspace(path.clone())
            .with_child_process(ChildProcessEvidence {
                quarantined_workspace: Some(path),
                ..ChildProcessEvidence::default()
            });

        let encoded = serde_json::to_value(error).expect("recovery evidence JSON");
        assert!(
            encoded["quarantined_workspace"]
                .as_str()
                .expect("lossy outer quarantine path")
                .contains("quarantined-")
        );
        assert!(
            encoded["child_process"]["quarantined_workspace"]
                .as_str()
                .expect("lossy child quarantine path")
                .contains("quarantined-")
        );
    }

    fn complete_worker_attestation() -> WorkerBuildV1 {
        WorkerBuildV1 {
            source_commit: "a".repeat(40),
            source_tree_dirty: Some(false),
            cargo_profile: "release".to_string(),
            cargo_opt_level: "2".to_string(),
            debug_assertions: false,
            target_triple: "aarch64-apple-darwin".to_string(),
            rustc_version: "rustc 1.97.1".to_string(),
            declared_release_lto: "thin".to_string(),
            declared_release_codegen_units: Some(16),
            declared_release_strip: Some(true),
            cargo_encoded_rustflags_present: Some(false),
            release_profile_environment_overrides_supported: Some(true),
            effective_codegen_options_proved: false,
            engine_feature_flags: omnigraph::instrumentation::enabled_engine_cargo_features()
                .iter()
                .map(|feature| (*feature).to_string())
                .collect(),
            enabled_techniques: Vec::new(),
            executable_sha256: "b".repeat(64),
        }
    }

    #[test]
    fn worker_build_attestation_requires_every_worker_reported_fact() {
        let valid = complete_worker_attestation();
        validate_worker_build_attestation(&valid, &valid.executable_sha256).unwrap();

        let mut invalid = valid.clone();
        invalid.source_commit = "unknown".to_string();
        assert!(validate_worker_build_attestation(&invalid, &valid.executable_sha256).is_err());

        let mut invalid = valid.clone();
        invalid.source_tree_dirty = None;
        assert!(validate_worker_build_attestation(&invalid, &valid.executable_sha256).is_err());

        let mut invalid = valid.clone();
        invalid.cargo_opt_level = "3".to_string();
        assert!(validate_worker_build_attestation(&invalid, &valid.executable_sha256).is_err());

        let mut invalid = valid.clone();
        invalid.target_triple = "unknown".to_string();
        assert!(validate_worker_build_attestation(&invalid, &valid.executable_sha256).is_err());

        let mut invalid = valid.clone();
        invalid.rustc_version = "unknown".to_string();
        assert!(validate_worker_build_attestation(&invalid, &valid.executable_sha256).is_err());

        let mut invalid = valid.clone();
        invalid.declared_release_lto = "fat".to_string();
        assert!(validate_worker_build_attestation(&invalid, &valid.executable_sha256).is_err());

        let mut invalid = valid.clone();
        invalid.cargo_encoded_rustflags_present = Some(true);
        assert!(validate_worker_build_attestation(&invalid, &valid.executable_sha256).is_err());

        let mut invalid = valid.clone();
        invalid.release_profile_environment_overrides_supported = Some(false);
        assert!(validate_worker_build_attestation(&invalid, &valid.executable_sha256).is_err());

        let mut invalid = valid.clone();
        invalid.effective_codegen_options_proved = true;
        assert!(validate_worker_build_attestation(&invalid, &valid.executable_sha256).is_err());

        let mut invalid = valid.clone();
        invalid.engine_feature_flags.push("invented".to_string());
        assert!(validate_worker_build_attestation(&invalid, &valid.executable_sha256).is_err());

        let mut invalid = valid.clone();
        invalid.enabled_techniques.push("invented".to_string());
        assert!(validate_worker_build_attestation(&invalid, &valid.executable_sha256).is_err());

        assert!(validate_worker_build_attestation(&valid, &"c".repeat(64)).is_err());
    }

    #[test]
    fn worker_reports_features_from_the_linked_engine_artifact() {
        let build = worker_build_attestation("b".repeat(64));
        assert_eq!(
            build.engine_feature_flags,
            omnigraph::instrumentation::enabled_engine_cargo_features()
                .iter()
                .map(|feature| (*feature).to_string())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn isolated_worker_must_have_the_same_bytes_as_its_parent() {
        let directory = tempfile::tempdir().unwrap();
        let parent = directory.path().join("parent");
        let worker = directory.path().join("worker");
        std::fs::write(&parent, b"parent-build").unwrap();
        std::fs::write(&worker, b"other-build").unwrap();

        assert_eq!(
            resolve_bound_worker(&worker, &parent).unwrap_err().code,
            "worker_parent_executable_mismatch"
        );

        std::fs::write(&worker, b"parent-build").unwrap();
        let resolved = resolve_bound_worker(&worker, &parent).unwrap();
        assert_eq!(
            resolved.executable_sha256,
            digest_worker_executable(&parent).unwrap()
        );
    }

    #[test]
    fn runtime_override_namespace_is_fail_closed() {
        assert!(is_omnigraph_runtime_override(OsStr::new(
            "OMNIGRAPH_TRAVERSAL_MODE"
        )));
        assert!(is_omnigraph_runtime_override(OsStr::new(
            "OMNIGRAPH_MERGE_STAGING_DIR"
        )));
        assert!(!is_omnigraph_runtime_override(OsStr::new(
            "AWS_ENDPOINT_URL"
        )));
        assert!(!is_omnigraph_runtime_override(OsStr::new("OMNIGRAPH")));

        let names = [
            "OMNIGRAPH_BENCH_BUILD_PROFILE",
            "OMNIGRAPH_BENCH_BUILD_OPT_LEVEL",
            "OMNIGRAPH_BENCH_SOURCE_COMMIT",
            "OMNIGRAPH_BENCH_SOURCE_DIRTY",
            "OMNIGRAPH_BENCH_TARGET_TRIPLE",
            "OMNIGRAPH_BENCH_RUSTC_VERSION",
            "OMNIGRAPH_BENCH_DECLARED_RELEASE_LTO",
            "OMNIGRAPH_BENCH_DECLARED_RELEASE_CODEGEN_UNITS",
            "OMNIGRAPH_BENCH_DECLARED_RELEASE_STRIP",
            "OMNIGRAPH_BENCH_CARGO_ENCODED_RUSTFLAGS_PRESENT",
            "OMNIGRAPH_BENCH_RELEASE_PROFILE_ENVIRONMENT_OVERRIDES",
            "OMNIGRAPH_BENCH_EFFECTIVE_CODEGEN_OPTIONS_PROVED",
        ];
        let exact = names
            .iter()
            .map(|name| {
                (
                    std::ffi::OsString::from(name),
                    std::ffi::OsString::from(
                        expected_build_attestation_environment(OsStr::new(name)).unwrap(),
                    ),
                )
            })
            .collect::<Vec<_>>();
        validate_runtime_overrides(exact.clone()).unwrap();

        let mut mismatched = exact;
        mismatched[0].1 = std::ffi::OsString::from("forged");
        assert_eq!(
            validate_runtime_overrides(mismatched).unwrap_err().code,
            "build_attestation_environment_mismatch"
        );
        assert_eq!(
            validate_runtime_overrides([(
                std::ffi::OsString::from("OMNIGRAPH_TRAVERSAL_MODE"),
                std::ffi::OsString::from("indexed"),
            )])
            .unwrap_err()
            .code,
            "unsupported_runtime_override"
        );
    }

    #[test]
    fn source_provenance_parser_never_turns_unknown_into_a_claim() {
        assert_eq!(parse_source_git_commit("unknown"), None);
        assert_eq!(parse_source_git_commit(&"a".repeat(39)), None);
        assert_eq!(parse_source_git_commit(&"G".repeat(40)), None);
        assert_eq!(
            parse_source_git_commit(&"A".repeat(40)),
            Some("a".repeat(40))
        );
        assert_eq!(parse_source_worktree_dirty("true"), Some(true));
        assert_eq!(parse_source_worktree_dirty("false"), Some(false));
        assert_eq!(parse_source_worktree_dirty("unknown"), None);
    }

    #[test]
    fn effective_environment_evidence_is_explicit_and_bounded() {
        assert_eq!(
            classify_effective_environment_value(None),
            EffectiveEnvironmentValue::Unset
        );
        assert_eq!(
            classify_effective_environment_value(Some(OsStr::new("1GiB"))),
            EffectiveEnvironmentValue::Utf8 {
                value: "1GiB".to_string()
            }
        );
        let oversized = "1".repeat(MAX_RECORDED_ENV_VALUE_BYTES + 1);
        assert!(matches!(
            classify_effective_environment_value(Some(OsStr::new(&oversized))),
            EffectiveEnvironmentValue::OversizedUtf8 { bytes, sha256 }
                if bytes == MAX_RECORDED_ENV_VALUE_BYTES + 1 && sha256.len() == 64
        ));
    }

    #[cfg(unix)]
    #[test]
    fn non_utf8_environment_evidence_is_hashed_not_lossily_rewritten() {
        use std::os::unix::ffi::OsStrExt;

        let value = OsStr::from_bytes(b"1\xffGiB");
        assert!(matches!(
            classify_effective_environment_value(Some(value)),
            EffectiveEnvironmentValue::NonUtf8 { bytes: 5, sha256 }
                if sha256.len() == 64
        ));
    }

    #[test]
    fn run_build_evidence_is_derived_from_the_measured_worker() {
        let worker = ResolvedWorker {
            executable: PathBuf::from("/not-opened"),
            build: WorkerBuildV1 {
                cargo_profile: "sut-profile".into(),
                opt_level: "sut-opt".into(),
                debug_assertions: true,
                source_git_commit_sha: Some("2".repeat(40).into_boxed_str()),
                source_worktree_dirty: Some(true),
                effective_lance_mem_pool_size: Box::new(EffectiveEnvironmentValue::Utf8 {
                    value: "768MiB".to_string(),
                }),
                executable_sha256: "3".repeat(64).into_boxed_str(),
            },
        };

        let evidence = build_evidence(Some(&worker));
        assert_eq!(evidence.cargo_profile, "sut-profile");
        assert_eq!(evidence.opt_level, "sut-opt");
        assert!(evidence.debug_assertions);
        assert_eq!(evidence.source_git_commit_sha, Some("2".repeat(40)));
        assert_eq!(evidence.source_worktree_dirty, Some(true));
        assert_eq!(
            evidence.effective_lance_mem_pool_size,
            EffectiveEnvironmentValue::Utf8 {
                value: "768MiB".to_string()
            }
        );
        assert_eq!(evidence.worker_executable_sha256, Some("3".repeat(64)));
    }

    #[test]
    fn missing_child_evidence_never_authorizes_cleanup() {
        let error = RunnerError::new("worker_spawn_failed", "no child was contained");
        assert!(!containment_proven(&error));

        let contained = error.with_child_process(ChildProcessEvidence {
            direct_child_reaped: true,
            process_group_gone: true,
            stdio_closed_cleanly: true,
            ..ChildProcessEvidence::default()
        });
        assert!(containment_proven(&contained));
    }

    #[test]
    fn percentile_summary_never_invents_an_unsupported_tail() {
        let samples = (0..5)
            .map(|repetition| RepObservation {
                repetition,
                input_physical_digest_sha256: "d".repeat(64),
                elapsed_us: u64::from(repetition + 1) * 10,
                peak_rss_bytes: None,
                outcome: "merged".to_string(),
                phases: Vec::new(),
                route: MergeRouteObservation {
                    table_walk_intervals: 1,
                    stage_merge_insert_calls: 0,
                    stage_merge_insert_rows: 0,
                    stage_known_present_update_calls: 0,
                    stage_known_present_update_rows: 0,
                    stage_fenced_insert_calls: 0,
                    stage_fenced_insert_rows: 0,
                    strict_insert_preflight_calls: 0,
                },
                logical_store_calls: LogicalStoreCallObservation {
                    manifest: LogicalCallCounts::default(),
                    table: LogicalCallCounts::default(),
                    physical_attempts_observed: false,
                },
                control_store_calls: ControlCallObservation {
                    read_text: 0,
                    read_text_if_exists: 0,
                    read_text_versioned: 0,
                    exists: 0,
                    list_dir: 0,
                    mutation_calls: 0,
                    write_text: 0,
                    delete: 0,
                },
                verification: VerificationObservation {
                    branch: TARGET_BRANCH.to_string(),
                    tables: 1,
                    rows: 1,
                    exact_content: true,
                    source_exact_content: true,
                    main_exact_content: true,
                    protected_heads_unchanged: true,
                },
            })
            .collect::<Vec<_>>();
        let summary = summarize_wall_clock(&samples).unwrap();
        assert_eq!(summary.min_us, 10);
        assert_eq!(summary.p50_us, 30);
        assert_eq!(summary.max_us, 50);
        assert_eq!(summary.p95_us, None);
        assert!(!summary.p95_supported);
    }

    #[test]
    fn complete_cache_condition_selects_one_exact_preparation_action() {
        let process_cold = CacheCondition {
            process: ProcessLifecycle::FreshPerRepetition,
            engine: EnginePreparation::PreparationOnly,
            page_cache: PageCacheCondition::Uncontrolled,
            program: WarmupProgram::None,
            iterations: 0,
        };
        assert_eq!(
            cache_preparation_action(&process_cold).unwrap(),
            CachePreparationAction::PreparationOnly
        );

        let warm = CacheCondition {
            engine: EnginePreparation::WarmedByProgram,
            page_cache: PageCacheCondition::ProgramConditioned,
            program: WarmupProgram::BranchMergeReadSetV1,
            iterations: 2,
            ..process_cold.clone()
        };
        assert_eq!(
            cache_preparation_action(&warm).unwrap(),
            CachePreparationAction::WarmSameHandle { iterations: 2 }
        );

        let post_reopen = CacheCondition {
            engine: EnginePreparation::ReopenedAfterProgram,
            ..warm.clone()
        };
        assert_eq!(
            cache_preparation_action(&post_reopen).unwrap(),
            CachePreparationAction::WarmThenReopen { iterations: 2 }
        );

        let mismatched = CacheCondition {
            page_cache: PageCacheCondition::Uncontrolled,
            ..warm
        };
        let error = cache_preparation_action(&mismatched).unwrap_err();
        assert_eq!(error.code, "unsupported_cache_condition");
    }

    #[test]
    fn warm_and_post_reopen_start_from_identical_frozen_bytes_and_verify_exact_content() {
        // The debug-build merge future plus two task-local measurement layers
        // exceeds libtest's 2 MiB worker stack. Public wall-clock execution is
        // release-only; keep this owning-layer debug regression on the same
        // explicit stack budget as the engine merge-cost tests.
        std::thread::Builder::new()
            .stack_size(64 * 1024 * 1024)
            .spawn(|| {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(async {
                        let warmed_source = r#"
version: 1
id: tiny-runner
scenario: branch-merge-v1
fixture:
  builder: { kind: synthetic-branch-merge, version: 2, seed: 0 }
  data:
    provenance: synthetic
    tables: 2
    rows_per_table: 12
    payload_bytes: 8
    column_shape: scalars
    topology_skew: uniform
  state:
    aging: bulk-loaded
    indexes: []
    deletion_history: none
    compaction_recency: not-optimized
    history_depth: 6
workload:
  delta_rows_per_side: 6
  diverged_tables: 1
  arrival: unscheduled-single-shot
  clients: 1
  read_write_mix: write-heavy
  contention: distinct-key
environment:
  backend: { kind: local-fs, filesystem: apfs, storage_class: nvme-ssd }
  network_position: same-host
  execution: embedded
  cache_condition: { process: fresh-per-repetition, engine: warmed-by-program, page_cache: program-conditioned, program: branch-merge-read-set-v1, iterations: 1 }
protocol:
  deadline_seconds: 60
  attribution: per-phase
  schedule: manual
  reset: plain-copy
  timer: monotonic
"#;
                        let post_reopen_source = warmed_source.replace(
                            "engine: warmed-by-program",
                            "engine: reopened-after-program",
                        );
                        for source in [warmed_source.to_owned(), post_reopen_source] {
                            let case = parse_case(&source).into_result().unwrap();
                            let run = ResolvedRun {
                                case_path: PathBuf::from("tiny-runner.case-v1.yaml"),
                                repetitions: 2,
                                case,
                            };

                            let execution = execute_run_inner(
                                &run,
                                &RunOptions::default(),
                                ExecutionGuards {
                                    verify_environment: false,
                                    isolate_repetitions: false,
                                    allow_plain_copy: true,
                                },
                            )
                            .await
                            .unwrap();

                            assert_eq!(
                                execution.cache_condition,
                                run.case.definition.environment.cache_condition
                            );
                            assert_eq!(
                                serde_json::to_value(&execution).unwrap()["cache_condition"]
                                    ["page_cache"],
                                "program-conditioned"
                            );
                            assert_eq!(execution.fixture.source_history_depth, 6);
                            assert_eq!(execution.fixture.target_history_depth, 6);
                            execution.machine.validate().unwrap();
                            assert_eq!(execution.samples.len(), 2);
                            assert_eq!(
                                execution.samples[0].input_physical_digest_sha256,
                                execution.fixture.stamp.manifest.physical.tree_sha256
                            );
                            assert_eq!(
                                execution.samples[1].input_physical_digest_sha256,
                                execution.fixture.stamp.manifest.physical.tree_sha256
                            );
                            for sample in execution.samples {
                                assert_eq!(sample.outcome, "merged");
                                assert_eq!(sample.route.table_walk_intervals, 1);
                                assert!(sample.verification.exact_content);
                                assert!(sample.verification.source_exact_content);
                                assert!(sample.verification.main_exact_content);
                                assert!(sample.verification.protected_heads_unchanged);
                                assert_eq!(sample.verification.tables, 2);
                                assert_eq!(sample.verification.rows, 24);
                                assert!(!sample.logical_store_calls.physical_attempts_observed);
                            }
                        }
                    });
            })
            .unwrap()
            .join()
            .unwrap();
    }

    fn checked_catalog_run(repetitions: u32) -> ResolvedRun {
        let case = parse_case(include_str!(
            "../../../benchmarks/cases/branch-merge-d50-warm.case-v1.yaml"
        ))
        .into_result()
        .unwrap();
        ResolvedRun {
            case_path: PathBuf::from("branch-merge-d50-warm.case-v1.yaml"),
            repetitions,
            case,
        }
    }

    fn checked_catalog_suite() -> ResolvedSuite {
        crate::load_suite(
            &PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../benchmarks/suites/local-smoke.suite-v1.yaml"),
        )
        .into_result()
        .unwrap()
    }

    #[test]
    fn execution_rejects_a_forged_unbounded_resolved_run() {
        let run = checked_catalog_run(MAX_REPETITIONS_PER_CASE + 1);
        let error = validate_execution_run(&run).unwrap_err();
        assert_eq!(error.code, "resolved_plan_unbounded");
    }

    #[test]
    fn execution_rejects_a_forged_resolved_identity() {
        let mut run = checked_catalog_run(1);
        run.case.point_id = "0".repeat(64);
        let error = validate_execution_run(&run).unwrap_err();
        assert_eq!(error.code, "resolved_plan_tampered");
    }

    #[test]
    fn execution_reseals_the_complete_resolved_suite() {
        let suite = checked_catalog_suite();
        validate_execution_suite(&suite).unwrap();

        let mut mislabeled = suite.clone();
        mislabeled.definition.name = "forged-suite-name".to_string();
        let error = validate_execution_suite(&mislabeled).unwrap_err();
        assert_eq!(error.code, "resolved_plan_tampered");

        let mut duplicated = suite;
        duplicated.runs.push(duplicated.runs[0].clone());
        duplicated
            .definition
            .runs
            .push(duplicated.definition.runs[0].clone());
        let error = validate_execution_suite(&duplicated).unwrap_err();
        assert_eq!(error.code, "resolved_plan_tampered");
    }
}
