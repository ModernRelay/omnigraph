//! Claim-ineligible execution against a registered real graph fixture.
//!
//! V1 deliberately supports one FinGraph-native branch-merge probe. The
//! imported graph supplies realistic catalog size, history, fragments, and
//! indexes; each side inserts two `Account` nodes and one transfer edge. The
//! prepared tree is frozen once, every repetition is restored at the same
//! path with APFS clonefiles or verified Linux/XFS plain copies, and the
//! measured merge runs in a fresh process. Results are diagnostic JSON only
//! and cannot enter the durable benchmark archive.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{ExitCode, Stdio};
use std::time::{Duration, Instant};

use omnigraph::db::{MergeOutcome, Omnigraph, ReadTarget, Snapshot};
use omnigraph::instrumentation::{MergeWriteProbes, with_merge_write_probes};
use omnigraph::loader::LoadMode;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::process::Command;
use tokio::time::timeout;

use crate::case::{LocalFilesystem, LocalStorageClass, ResetMode};
use crate::environment::{LocalEnvironmentEvidence, verify_local_environment};
use crate::fixture_reference::NormalizedFixtureReferenceV1;
use crate::machine::{MachineIdentityV1, capture_machine_identity};
use crate::model::{
    Diagnostic, ValidationOutcome, declared_version, read_yaml_file, strict_yaml, valid_kebab_id,
};
use crate::real_graph::{observe_real_graph, validate_real_graph_reference};
use crate::registered_fixture::{
    FixtureCopyPreflightReceiptV1, StagedRegisteredFixtureV1, stage_registered_fixture_binding,
};
use crate::reset::{
    ClonefileTemplate, PhysicalDigest, PlainCopyTemplate, PreparedFixtureTree, TraversalLimits,
    digest_metadata_tree, freeze_clonefile_template, freeze_plain_copy_template,
};
use crate::runner::{
    MergePhaseEvidenceForm, MergeRouteObservation, PhaseObservation,
    configure_benchmark_worker_environment, phase_observations,
    validate_successful_merge_phase_topology,
};

pub const REAL_GRAPH_RUN_SPEC_VERSION: u32 = 1;
const WORKER_PROTOCOL_VERSION: u32 = 1;
const MAX_REPETITIONS: u32 = 20;
const MAX_DEADLINE_SECONDS: u64 = 3_600;
const MAX_WORKER_FILE_BYTES: u64 = 1024 * 1024;
const WORKER_VERIFY_GRACE_SECONDS: u64 = 120;
const PLAIN_COPY_HEADROOM_BYTES: u64 = 1024 * 1024 * 1024;
const SOURCE_BRANCH: &str = "bench-source";
const TARGET_BRANCH: &str = "bench-target";
const ACCOUNT_TABLE: &str = "node:Account";
const TRANSFER_TABLE: &str = "edge:AccountTransferAccount";

const SOURCE_A: &str = "omnigraph-bench-fin-v1-src-a";
const SOURCE_B: &str = "omnigraph-bench-fin-v1-src-b";
const SOURCE_EDGE: &str = "omnigraph-bench-fin-v1-src-transfer";
const TARGET_A: &str = "omnigraph-bench-fin-v1-tgt-a";
const TARGET_B: &str = "omnigraph-bench-fin-v1-tgt-b";
const TARGET_EDGE: &str = "omnigraph-bench-fin-v1-tgt-transfer";

const SOURCE_BATCH: &str = r#"{"type":"Account","data":{"accountId":"omnigraph-bench-fin-v1-src-a","createTime":"2020-01-01T00:00:00Z","isBlocked":false,"accountType":"internet_account","freqLoginType":"ipv4","accountLevel":"basic"}}
{"type":"Account","data":{"accountId":"omnigraph-bench-fin-v1-src-b","createTime":"2020-01-01T00:00:00Z","isBlocked":false,"accountType":"internet_account","freqLoginType":"ipv4","accountLevel":"basic"}}
{"edge":"AccountTransferAccount","from":"omnigraph-bench-fin-v1-src-a","to":"omnigraph-bench-fin-v1-src-b","data":{"id":"omnigraph-bench-fin-v1-src-transfer","amount":1.25,"createTime":"2020-01-01T00:00:00Z","orderNum":"omnigraph-bench-fin-v1-src-order","payType":"bank_transfer","goodsType":"bank_transfer"}}"#;

const TARGET_BATCH: &str = r#"{"type":"Account","data":{"accountId":"omnigraph-bench-fin-v1-tgt-a","createTime":"2020-01-01T00:00:01Z","isBlocked":false,"accountType":"internet_account","freqLoginType":"ipv4","accountLevel":"basic"}}
{"type":"Account","data":{"accountId":"omnigraph-bench-fin-v1-tgt-b","createTime":"2020-01-01T00:00:01Z","isBlocked":false,"accountType":"internet_account","freqLoginType":"ipv4","accountLevel":"basic"}}
{"edge":"AccountTransferAccount","from":"omnigraph-bench-fin-v1-tgt-a","to":"omnigraph-bench-fin-v1-tgt-b","data":{"id":"omnigraph-bench-fin-v1-tgt-transfer","amount":2.5,"createTime":"2020-01-01T00:00:01Z","orderNum":"omnigraph-bench-fin-v1-tgt-order","payType":"bank_transfer","goodsType":"bank_transfer"}}"#;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RealGraphWorkloadV1 {
    FinbenchDisjointInsertMerge,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RealGraphRunSpecV1 {
    pub version: u32,
    pub fixture_id: String,
    pub workload: RealGraphWorkloadV1,
    pub repetitions: u32,
    pub operation_deadline_seconds: u64,
}

pub fn load_real_graph_run_spec(path: &Path) -> ValidationOutcome<RealGraphRunSpecV1> {
    let source = match read_yaml_file(path, "real_graph_run") {
        Ok(source) => source,
        Err(diagnostic) => return ValidationOutcome::failure(vec![diagnostic]),
    };
    let version = match declared_version(&source, "real_graph_run") {
        Ok(version) => version,
        Err(diagnostic) => return ValidationOutcome::failure(vec![diagnostic]),
    };
    if version != REAL_GRAPH_RUN_SPEC_VERSION {
        return ValidationOutcome::failure(vec![Diagnostic::error(
            "unsupported_real_graph_run_version",
            "version",
            format!(
                "unsupported real-graph run version {version}; expected {REAL_GRAPH_RUN_SPEC_VERSION}"
            ),
        )]);
    }
    let spec: RealGraphRunSpecV1 = match strict_yaml(&source, "real_graph_run") {
        Ok(spec) => spec,
        Err(diagnostic) => return ValidationOutcome::failure(vec![diagnostic]),
    };
    let mut diagnostics = Vec::new();
    if !valid_kebab_id(&spec.fixture_id) || spec.fixture_id.len() > 128 {
        diagnostics.push(Diagnostic::error(
            "invalid_real_graph_fixture_id",
            "fixture_id",
            "fixture_id must be 1..=128 characters of path-free kebab-case ASCII",
        ));
    }
    if !(1..=MAX_REPETITIONS).contains(&spec.repetitions) {
        diagnostics.push(Diagnostic::error(
            "invalid_real_graph_repetitions",
            "repetitions",
            format!("repetitions must be in 1..={MAX_REPETITIONS}"),
        ));
    }
    if !(1..=MAX_DEADLINE_SECONDS).contains(&spec.operation_deadline_seconds) {
        diagnostics.push(Diagnostic::error(
            "invalid_real_graph_deadline",
            "operation_deadline_seconds",
            format!("operation_deadline_seconds must be in 1..={MAX_DEADLINE_SECONDS}"),
        ));
    }
    if diagnostics.is_empty() {
        ValidationOutcome::success(spec)
    } else {
        ValidationOutcome::failure(diagnostics)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TablePointerV1 {
    pub type_key: String,
    pub dataset_path: String,
    pub published_dataset_version: u64,
    pub native_dataset_branch: Option<String>,
    pub entity_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RealGraphRepV1 {
    pub repetition: u32,
    pub elapsed_us: u64,
    pub outcome: String,
    pub phases: Vec<PhaseObservation>,
    pub route: MergeRouteObservation,
    pub before_target_manifest_version: u64,
    pub after_target_manifest_version: u64,
    pub inserted_delta_verified: bool,
    pub existing_rows_in_changed_tables_verified: bool,
    pub protected_heads_verified: bool,
    pub untouched_tables_verified: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RealGraphRunReportV1 {
    pub version: u32,
    pub fixture_id: String,
    pub workload: RealGraphWorkloadV1,
    pub fixture: FixtureCopyPreflightReceiptV1,
    pub reference_sha256: String,
    pub prepared_input_physical: PhysicalDigest,
    pub machine: MachineIdentityV1,
    pub environment: LocalEnvironmentEvidence,
    pub reset: String,
    pub process_state: String,
    pub page_cache_state: String,
    pub warmup: String,
    pub claim_eligible: bool,
    pub durable_record: bool,
    pub repetitions: u32,
    pub operation_deadline_seconds: u64,
    pub p50_us: u64,
    pub samples: Vec<RealGraphRepV1>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RealGraphRunError {
    pub code: &'static str,
    pub message: String,
}

impl RealGraphRunError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for RealGraphRunError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for RealGraphRunError {}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerRequestV1 {
    version: u32,
    repetition: u32,
    root: PathBuf,
    operation_deadline_seconds: u64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerEnvelopeV1 {
    version: u32,
    ok: bool,
    sample: Option<RealGraphRepV1>,
    machine: Option<MachineIdentityV1>,
    error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RealGraphLocalProfile {
    filesystem: LocalFilesystem,
    storage_class: LocalStorageClass,
    reset: ResetMode,
    reset_label: &'static str,
}

#[cfg(target_os = "macos")]
fn real_graph_local_profile() -> Result<RealGraphLocalProfile, RealGraphRunError> {
    Ok(RealGraphLocalProfile {
        filesystem: LocalFilesystem::Apfs,
        storage_class: LocalStorageClass::NvmeSsd,
        reset: ResetMode::LocalClonefile,
        reset_label: "apfs-clonefile-same-active-path",
    })
}

#[cfg(target_os = "linux")]
fn real_graph_local_profile() -> Result<RealGraphLocalProfile, RealGraphRunError> {
    Ok(RealGraphLocalProfile {
        filesystem: LocalFilesystem::Xfs,
        storage_class: LocalStorageClass::NvmeSsd,
        reset: ResetMode::PlainCopy,
        reset_label: "xfs-plain-copy-syncfs-same-active-path",
    })
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn real_graph_local_profile() -> Result<RealGraphLocalProfile, RealGraphRunError> {
    Err(RealGraphRunError::new(
        "unsupported_real_graph_platform",
        "real-graph execution supports macOS/APFS clonefile or Linux/XFS EC2 instance-store NVMe only",
    ))
}

enum RealGraphTemplate {
    Clonefile(ClonefileTemplate),
    PlainCopy(PlainCopyTemplate),
}

impl RealGraphTemplate {
    fn freeze(
        profile: RealGraphLocalProfile,
        active: &Path,
        template: &Path,
        limits: TraversalLimits,
    ) -> std::io::Result<Self> {
        match profile.reset {
            ResetMode::LocalClonefile => {
                freeze_clonefile_template(active, template, limits).map(Self::Clonefile)
            }
            ResetMode::PlainCopy => {
                let frozen = freeze_plain_copy_template(active, template, limits)?;
                sync_plain_copy_filesystem(frozen.template_root())?;
                Ok(Self::PlainCopy(frozen))
            }
            ResetMode::S3Versioning => Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "real-graph execution supports only local reset modes",
            )),
        }
    }

    fn physical_digest(&self) -> &PhysicalDigest {
        match self {
            Self::Clonefile(template) => template.physical_digest(),
            Self::PlainCopy(template) => template.physical_digest(),
        }
    }

    fn verify_unchanged(&self) -> std::io::Result<()> {
        match self {
            Self::Clonefile(template) => template.verify_unchanged().map(|_| ()),
            Self::PlainCopy(template) => template.verify_unchanged().map(|_| ()),
        }
    }

    fn restore_active(&self) -> std::io::Result<PreparedFixtureTree> {
        match self {
            Self::Clonefile(template) => template.restore_active(),
            Self::PlainCopy(template) => {
                let prepared = template.restore_active()?;
                sync_plain_copy_filesystem(prepared.root())?;
                Ok(prepared)
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn sync_plain_copy_filesystem(root: &Path) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;

    let directory = fs::File::open(root)?;
    // SAFETY: the descriptor remains open for this call; syncfs does not
    // retain it. This waits for the entire dedicated benchmark filesystem,
    // including data and directory metadata, outside the measured interval.
    let result = unsafe { libc::syncfs(directory.as_raw_fd()) };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(not(target_os = "linux"))]
fn sync_plain_copy_filesystem(_root: &Path) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "real-graph plain-copy writeback synchronization requires Linux syncfs",
    ))
}

pub async fn execute_real_graph_run(
    spec: &RealGraphRunSpecV1,
    reference: &NormalizedFixtureReferenceV1,
    fixture_binding: &str,
    scratch_root: Option<&Path>,
) -> Result<RealGraphRunReportV1, RealGraphRunError> {
    if cfg!(debug_assertions) {
        return Err(RealGraphRunError::new(
            "release_build_required",
            "real-graph timing requires the checked-in Cargo release profile",
        ));
    }
    if spec.fixture_id != reference.definition.fixture_id {
        return Err(RealGraphRunError::new(
            "real_graph_fixture_id_mismatch",
            format!(
                "run spec identifies {:?}, but the logical reference identifies {:?}",
                spec.fixture_id, reference.definition.fixture_id
            ),
        ));
    }
    let staged = stage_registered_fixture_binding(fixture_binding, scratch_root)
        .into_result()
        .map_err(|diagnostics| diagnostics_error("real_graph_staging_failed", diagnostics))?;
    let outcome = execute_staged_real_graph_run(spec, reference, &staged).await;
    let cleanup = staged.finish().map(|_| ());
    complete_real_graph_run(outcome, cleanup)
}

async fn execute_staged_real_graph_run(
    spec: &RealGraphRunSpecV1,
    reference: &NormalizedFixtureReferenceV1,
    staged: &StagedRegisteredFixtureV1,
) -> Result<RealGraphRunReportV1, RealGraphRunError> {
    if staged.receipt().fixture_id != spec.fixture_id {
        let observed = staged.receipt().fixture_id.clone();
        return Err(RealGraphRunError::new(
            "real_graph_fixture_id_mismatch",
            format!(
                "run spec identifies {:?}, but the registered fixture identifies {observed:?}",
                spec.fixture_id
            ),
        ));
    }
    let active = staged.root().to_path_buf();
    let workspace = active.parent().ok_or_else(|| {
        RealGraphRunError::new(
            "real_graph_scratch_failed",
            "staged graph root has no run-owned parent",
        )
    })?;
    let profile = real_graph_local_profile()?;
    let mut environment = observe_real_graph_environment(workspace, profile)?;

    let observation = observe_real_graph(staged.root()).await.map_err(|error| {
        RealGraphRunError::new("real_graph_observation_failed", error.to_string())
    })?;
    validate_real_graph_reference(reference, &observation).map_err(|error| {
        RealGraphRunError::new("real_graph_reference_mismatch", error.to_string())
    })?;
    staged.verify_unchanged().map_err(|diagnostic| {
        RealGraphRunError::new("staged_fixture_changed", diagnostic.message)
    })?;

    let template = workspace.join("template");
    let fixture_receipt = staged.receipt().clone();

    prepare_finbench_delta(&active).await?;
    let limits = TraversalLimits::default();
    if profile.reset == ResetMode::PlainCopy {
        let prepared = digest_metadata_tree(&active, limits).map_err(|error| {
            RealGraphRunError::new(
                "real_graph_capacity_probe_failed",
                format!("inventory prepared graph before plain-copy freeze: {error}"),
            )
        })?;
        environment = observe_real_graph_environment(&active, profile)?;
        require_plain_copy_capacity(
            prepared.bytes,
            environment.available_bytes,
            &environment.mount_point,
        )?;
    }
    let frozen =
        RealGraphTemplate::freeze(profile, &active, &template, limits).map_err(|error| {
            RealGraphRunError::new(
                "real_graph_freeze_failed",
                format!(
                    "freeze prepared graph with {}: {error}",
                    profile.reset_label
                ),
            )
        })?;
    remove_active(&active, workspace)?;

    let executable = std::env::current_exe().map_err(|error| {
        RealGraphRunError::new(
            "real_graph_worker_unavailable",
            format!("resolve current benchmark executable: {error}"),
        )
    })?;
    let mut samples = Vec::with_capacity(spec.repetitions as usize);
    let mut worker_machine = None::<MachineIdentityV1>;
    for repetition in 1..=spec.repetitions {
        frozen.verify_unchanged().map_err(|error| {
            RealGraphRunError::new(
                "real_graph_template_changed",
                format!("prepared template changed before repetition {repetition}: {error}"),
            )
        })?;
        let prepared = frozen.restore_active().map_err(|error| {
            RealGraphRunError::new(
                "real_graph_reset_failed",
                format!("restore repetition {repetition}: {error}"),
            )
        })?;
        prepared.verify_unchanged().map_err(|error| {
            RealGraphRunError::new(
                "real_graph_reset_changed",
                format!("restored input changed before repetition {repetition}: {error}"),
            )
        })?;
        let request_path = workspace.join(format!("request-{repetition}.json"));
        let result_path = workspace.join(format!("result-{repetition}.json"));
        let worker_scratch = workspace.join(format!("worker-{repetition}"));
        fs::create_dir(&worker_scratch).map_err(|error| {
            RealGraphRunError::new(
                "real_graph_worker_protocol_failed",
                format!("create repetition worker scratch: {error}"),
            )
        })?;
        let request = WorkerRequestV1 {
            version: WORKER_PROTOCOL_VERSION,
            repetition,
            root: prepared.root().to_path_buf(),
            operation_deadline_seconds: spec.operation_deadline_seconds,
        };
        fs::write(
            &request_path,
            serde_json::to_vec(&request).map_err(|error| {
                RealGraphRunError::new(
                    "real_graph_worker_protocol_failed",
                    format!("serialize worker request: {error}"),
                )
            })?,
        )
        .map_err(|error| {
            RealGraphRunError::new(
                "real_graph_worker_protocol_failed",
                format!("write worker request: {error}"),
            )
        })?;
        let outcome = invoke_worker(
            &executable,
            &request_path,
            &result_path,
            &worker_scratch,
            spec.operation_deadline_seconds,
        )
        .await;
        remove_active(&active, workspace)?;
        let (sample, machine) = outcome?;
        accept_worker_machine(&mut worker_machine, machine, repetition)?;
        samples.push(sample);
    }
    frozen.verify_unchanged().map_err(|error| {
        RealGraphRunError::new(
            "real_graph_template_changed",
            format!("prepared template changed after execution: {error}"),
        )
    })?;
    let machine = worker_machine.ok_or_else(|| {
        RealGraphRunError::new(
            "real_graph_worker_protocol_failed",
            "real-graph execution produced no repetition-worker machine identity",
        )
    })?;

    let mut elapsed = samples
        .iter()
        .map(|sample| sample.elapsed_us)
        .collect::<Vec<_>>();
    elapsed.sort_unstable();
    let p50_us = elapsed[(elapsed.len() - 1) / 2];
    let prepared_input_physical = frozen.physical_digest().clone();
    Ok(RealGraphRunReportV1 {
        version: REAL_GRAPH_RUN_SPEC_VERSION,
        fixture_id: spec.fixture_id.clone(),
        workload: spec.workload,
        fixture: fixture_receipt,
        reference_sha256: reference.reference_sha256.clone(),
        prepared_input_physical,
        machine,
        environment,
        reset: profile.reset_label.to_string(),
        process_state: "fresh-process-per-repetition".to_string(),
        page_cache_state: "uncontrolled".to_string(),
        warmup: "none".to_string(),
        claim_eligible: false,
        durable_record: false,
        repetitions: spec.repetitions,
        operation_deadline_seconds: spec.operation_deadline_seconds,
        p50_us,
        samples,
    })
}

fn observe_real_graph_environment(
    scratch_path: &Path,
    profile: RealGraphLocalProfile,
) -> Result<LocalEnvironmentEvidence, RealGraphRunError> {
    verify_local_environment(scratch_path, profile.filesystem, profile.storage_class)
        .map_err(|message| RealGraphRunError::new("environment_mismatch", message))
}

fn require_plain_copy_capacity(
    prepared_bytes: u64,
    available_bytes: u64,
    mount_point: &str,
) -> Result<u64, RealGraphRunError> {
    let required = prepared_bytes
        .checked_add(PLAIN_COPY_HEADROOM_BYTES)
        .ok_or_else(|| {
            RealGraphRunError::new(
                "required_scratch_capacity_overflow",
                "prepared graph plus plain-copy headroom overflowed u64",
            )
        })?;
    if available_bytes < required {
        return Err(RealGraphRunError::new(
            "insufficient_scratch_capacity",
            format!(
                "real-graph plain-copy reset requires at least {required} available bytes, but {available_bytes} are available at {mount_point}"
            ),
        ));
    }
    Ok(required)
}

fn accept_worker_machine(
    expected: &mut Option<MachineIdentityV1>,
    observed: MachineIdentityV1,
    repetition: u32,
) -> Result<(), RealGraphRunError> {
    match expected {
        Some(machine) if machine != &observed => Err(RealGraphRunError::new(
            "machine_identity_changed",
            format!("repetition-worker machine identity changed before repetition {repetition}"),
        )),
        None => {
            *expected = Some(observed);
            Ok(())
        }
        Some(_) => Ok(()),
    }
}

fn complete_real_graph_run<T>(
    outcome: Result<T, RealGraphRunError>,
    cleanup: Result<(), Diagnostic>,
) -> Result<T, RealGraphRunError> {
    match (outcome, cleanup) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(cleanup)) => Err(RealGraphRunError::new(
            "fixture_cleanup_failed",
            format!("{} at {}: {}", cleanup.code, cleanup.path, cleanup.message),
        )),
        (Err(error), Err(cleanup)) => Err(RealGraphRunError::new(
            "real_graph_run_and_cleanup_failed",
            format!(
                "run failed with {}: {}; cleanup also failed with {} at {}: {}",
                error.code, error.message, cleanup.code, cleanup.path, cleanup.message
            ),
        )),
    }
}

fn diagnostics_error(code: &'static str, diagnostics: Vec<Diagnostic>) -> RealGraphRunError {
    RealGraphRunError::new(
        code,
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
}

async fn prepare_finbench_delta(root: &Path) -> Result<(), RealGraphRunError> {
    let uri = path_utf8(root)?;
    let db = Omnigraph::open(uri).await.map_err(|error| {
        RealGraphRunError::new(
            "real_graph_prepare_failed",
            format!("open disposable FinGraph copy: {error}"),
        )
    })?;
    let mut branches = db.branch_list().await.map_err(engine_prepare_error)?;
    branches.sort();
    if branches != ["main"] {
        return Err(RealGraphRunError::new(
            "real_graph_prepare_failed",
            format!("FinGraph base must contain only main; observed {branches:?}"),
        ));
    }
    verify_reserved_entities(&db, "main", SideExpectation::None).await?;
    let main = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .map_err(engine_prepare_error)?;
    require_finbench_tables(&main)?;
    db.branch_create_from(ReadTarget::branch("main"), SOURCE_BRANCH)
        .await
        .map_err(engine_prepare_error)?;
    db.branch_create_from(ReadTarget::branch("main"), TARGET_BRANCH)
        .await
        .map_err(engine_prepare_error)?;
    let source = db
        .load_graph_batch_as_with_receipt(SOURCE_BRANCH, None, SOURCE_BATCH, LoadMode::Append, None)
        .await
        .map_err(engine_prepare_error)?;
    require_load_receipt(&source.result, SOURCE_BRANCH)?;
    let target = db
        .load_graph_batch_as_with_receipt(TARGET_BRANCH, None, TARGET_BATCH, LoadMode::Append, None)
        .await
        .map_err(engine_prepare_error)?;
    require_load_receipt(&target.result, TARGET_BRANCH)?;
    verify_reserved_entities(&db, "main", SideExpectation::None).await?;
    verify_reserved_entities(&db, SOURCE_BRANCH, SideExpectation::SourceOnly).await?;
    verify_reserved_entities(&db, TARGET_BRANCH, SideExpectation::TargetOnly).await?;
    verify_prepared_counts(&db, &main).await?;
    drop(db);
    Ok(())
}

fn require_load_receipt(
    result: &omnigraph::loader::LoadResult,
    branch: &str,
) -> Result<(), RealGraphRunError> {
    if result.branch != branch
        || result.branch_created
        || result.nodes_loaded.get("Account") != Some(&2)
        || result.edges_loaded.get("AccountTransferAccount") != Some(&1)
        || result.nodes_loaded.len() != 1
        || result.edges_loaded.len() != 1
    {
        return Err(RealGraphRunError::new(
            "real_graph_prepare_failed",
            format!("unexpected strict load receipt on {branch}: {result:?}"),
        ));
    }
    Ok(())
}

fn require_finbench_tables(snapshot: &Snapshot) -> Result<(), RealGraphRunError> {
    for table in [ACCOUNT_TABLE, TRANSFER_TABLE] {
        if snapshot.dataset(table).is_none() {
            return Err(RealGraphRunError::new(
                "real_graph_prepare_failed",
                format!("registered graph is missing required FinGraph table {table}"),
            ));
        }
    }
    Ok(())
}

async fn verify_prepared_counts(db: &Omnigraph, main: &Snapshot) -> Result<(), RealGraphRunError> {
    for branch in [SOURCE_BRANCH, TARGET_BRANCH] {
        let snapshot = db
            .snapshot_of(ReadTarget::branch(branch))
            .await
            .map_err(engine_prepare_error)?;
        require_count_delta(main, &snapshot, ACCOUNT_TABLE, 2)?;
        require_count_delta(main, &snapshot, TRANSFER_TABLE, 1)?;
    }
    Ok(())
}

fn require_count_delta(
    before: &Snapshot,
    after: &Snapshot,
    table: &str,
    delta: u64,
) -> Result<(), RealGraphRunError> {
    let before = before.dataset(table).ok_or_else(|| {
        RealGraphRunError::new("real_graph_verification_failed", format!("missing {table}"))
    })?;
    let after = after.dataset(table).ok_or_else(|| {
        RealGraphRunError::new("real_graph_verification_failed", format!("missing {table}"))
    })?;
    if after.entity_count != before.entity_count.saturating_add(delta) {
        return Err(RealGraphRunError::new(
            "real_graph_verification_failed",
            format!(
                "{table} count mismatch: before={}, expected delta={delta}, after={}",
                before.entity_count, after.entity_count
            ),
        ));
    }
    Ok(())
}

async fn invoke_worker(
    executable: &Path,
    request: &Path,
    result: &Path,
    worker_scratch: &Path,
    deadline_seconds: u64,
) -> Result<(RealGraphRepV1, MachineIdentityV1), RealGraphRunError> {
    let mut command = Command::new(executable);
    configure_benchmark_worker_environment(command.as_std_mut(), worker_scratch);
    command
        .arg("__real-graph-worker-v1")
        .arg(request)
        .arg(result)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let mut child = command.spawn().map_err(|error| {
        RealGraphRunError::new(
            "real_graph_worker_failed",
            format!("spawn fresh benchmark worker: {error}"),
        )
    })?;
    let hard_seconds = deadline_seconds
        .checked_add(WORKER_VERIFY_GRACE_SECONDS)
        .ok_or_else(|| RealGraphRunError::new("real_graph_worker_failed", "deadline overflow"))?;
    let status = match timeout(Duration::from_secs(hard_seconds), child.wait()).await {
        Ok(status) => status.map_err(|error| {
            RealGraphRunError::new(
                "real_graph_worker_failed",
                format!("wait for fresh benchmark worker: {error}"),
            )
        })?,
        Err(_) => {
            child.start_kill().map_err(|error| {
                RealGraphRunError::new(
                    "real_graph_worker_timeout",
                    format!("terminate worker after {hard_seconds}s: {error}"),
                )
            })?;
            child.wait().await.map_err(|error| {
                RealGraphRunError::new(
                    "real_graph_worker_timeout",
                    format!("reap terminated worker: {error}"),
                )
            })?;
            return Err(RealGraphRunError::new(
                "real_graph_worker_timeout",
                format!("fresh worker exceeded the {hard_seconds}s hard deadline"),
            ));
        }
    };
    let envelope = read_worker_envelope(result)?;
    if !status.success() || !envelope.ok {
        return Err(RealGraphRunError::new(
            "real_graph_worker_failed",
            envelope
                .error
                .unwrap_or_else(|| format!("worker exited {status}")),
        ));
    }
    let sample = envelope.sample.ok_or_else(|| {
        RealGraphRunError::new(
            "real_graph_worker_protocol_failed",
            "successful worker returned no sample",
        )
    })?;
    let machine = envelope.machine.ok_or_else(|| {
        RealGraphRunError::new(
            "real_graph_worker_protocol_failed",
            "successful worker returned no machine identity",
        )
    })?;
    Ok((sample, machine))
}

pub async fn run_real_graph_worker_files(request: &Path, result: &Path) -> ExitCode {
    let outcome = match read_worker_request(request) {
        Ok(request) => execute_worker(request).await,
        Err(error) => Err(error),
    };
    let envelope = match outcome {
        Ok((sample, machine)) => WorkerEnvelopeV1 {
            version: WORKER_PROTOCOL_VERSION,
            ok: true,
            sample: Some(sample),
            machine: Some(machine),
            error: None,
        },
        Err(error) => WorkerEnvelopeV1 {
            version: WORKER_PROTOCOL_VERSION,
            ok: false,
            sample: None,
            machine: None,
            error: Some(error.to_string()),
        },
    };
    let serialized = match serde_json::to_vec(&envelope) {
        Ok(serialized) => serialized,
        Err(error) => {
            eprintln!("serialize real-graph worker result: {error}");
            return ExitCode::FAILURE;
        }
    };
    if let Err(error) = write_new(result, &serialized) {
        eprintln!("write real-graph worker result: {error}");
        return ExitCode::FAILURE;
    }
    if envelope.ok {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

async fn execute_worker(
    request: WorkerRequestV1,
) -> Result<(RealGraphRepV1, MachineIdentityV1), RealGraphRunError> {
    if request.version != WORKER_PROTOCOL_VERSION {
        return Err(RealGraphRunError::new(
            "real_graph_worker_protocol_failed",
            format!("unsupported worker protocol {}", request.version),
        ));
    }
    if !(1..=MAX_DEADLINE_SECONDS).contains(&request.operation_deadline_seconds) {
        return Err(RealGraphRunError::new(
            "real_graph_worker_protocol_failed",
            "worker operation deadline is out of range",
        ));
    }
    let db = Omnigraph::open(path_utf8(&request.root)?)
        .await
        .map_err(engine_worker_error)?;
    let main_before = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .map_err(engine_worker_error)?;
    let source_before = db
        .snapshot_of(ReadTarget::branch(SOURCE_BRANCH))
        .await
        .map_err(engine_worker_error)?;
    let target_before = db
        .snapshot_of(ReadTarget::branch(TARGET_BRANCH))
        .await
        .map_err(engine_worker_error)?;
    // Capture process-effective facts in the fresh repetition worker after
    // preparation and immediately before the measured operation. The CLI
    // parent can have different affinity, cgroup, scheduling, or limits.
    let machine = capture_machine_identity().map_err(|error| {
        RealGraphRunError::new(
            "machine_identity_capture_failed",
            format!("capture repetition-worker machine identity: {error}"),
        )
    })?;
    let probes = MergeWriteProbes::default();
    let started = Instant::now();
    let outcome = with_merge_write_probes(
        probes.clone(),
        db.branch_merge(SOURCE_BRANCH, TARGET_BRANCH),
    )
    .await
    .map_err(engine_worker_error)?;
    let elapsed = started.elapsed();
    let elapsed_us = u64::try_from(elapsed.as_micros()).map_err(|_| {
        RealGraphRunError::new("real_graph_worker_failed", "elapsed time exceeds u64")
    })?;
    if elapsed > Duration::from_secs(request.operation_deadline_seconds) {
        return Err(RealGraphRunError::new(
            "real_graph_operation_deadline_exceeded",
            format!(
                "measured merge took {elapsed_us}us, exceeding the declared {}s deadline",
                request.operation_deadline_seconds
            ),
        ));
    }
    if outcome != MergeOutcome::Merged {
        return Err(RealGraphRunError::new(
            "real_graph_vacuous_merge",
            format!("expected Merged, observed {outcome:?}"),
        ));
    }
    let phases = phase_observations(probes.merge_timing_snapshot());
    let route = MergeRouteObservation::from_probes(&probes);
    validate_successful_merge_phase_topology(
        &phases,
        &route,
        2,
        MergePhaseEvidenceForm::RawSnapshot,
    )
    .map_err(|error| {
        RealGraphRunError::new(
            error.code(),
            format!("invalid merge phase evidence: {error}"),
        )
    })?;
    let main_after = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .map_err(engine_worker_error)?;
    let source_after = db
        .snapshot_of(ReadTarget::branch(SOURCE_BRANCH))
        .await
        .map_err(engine_worker_error)?;
    let target_after = db
        .snapshot_of(ReadTarget::branch(TARGET_BRANCH))
        .await
        .map_err(engine_worker_error)?;
    require_snapshot_unchanged("main", &main_before, &main_after)?;
    require_snapshot_unchanged(SOURCE_BRANCH, &source_before, &source_after)?;
    let untouched_tables_verified = verify_target_delta(&target_before, &target_after)?;
    verify_reserved_entities(&db, "main", SideExpectation::None).await?;
    verify_reserved_entities(&db, SOURCE_BRANCH, SideExpectation::SourceOnly).await?;
    verify_reserved_entities(&db, TARGET_BRANCH, SideExpectation::All).await?;
    verify_merge_commit(&db, &source_before, &target_before, &target_after).await?;
    drop(db);
    Ok((
        RealGraphRepV1 {
            repetition: request.repetition,
            elapsed_us,
            outcome: "merged".to_string(),
            phases,
            route,
            before_target_manifest_version: target_before.graph_manifest_version(),
            after_target_manifest_version: target_after.graph_manifest_version(),
            inserted_delta_verified: true,
            existing_rows_in_changed_tables_verified: false,
            protected_heads_verified: true,
            untouched_tables_verified,
        },
        machine,
    ))
}

fn require_snapshot_unchanged(
    name: &str,
    before: &Snapshot,
    after: &Snapshot,
) -> Result<(), RealGraphRunError> {
    if before.graph_head(Some(name).filter(|name| *name != "main"))
        != after.graph_head(Some(name).filter(|name| *name != "main"))
        || snapshot_pointers(before) != snapshot_pointers(after)
    {
        return Err(RealGraphRunError::new(
            "real_graph_verification_failed",
            format!("protected {name} head or table pointers changed during target merge"),
        ));
    }
    Ok(())
}

fn verify_target_delta(before: &Snapshot, after: &Snapshot) -> Result<u32, RealGraphRunError> {
    if before.graph_head(Some(TARGET_BRANCH)) == after.graph_head(Some(TARGET_BRANCH)) {
        return Err(RealGraphRunError::new(
            "real_graph_verification_failed",
            "target head did not advance",
        ));
    }
    let before_pointers = snapshot_pointers(before);
    let after_pointers = snapshot_pointers(after);
    if before_pointers.len() != after_pointers.len() {
        return Err(RealGraphRunError::new(
            "real_graph_verification_failed",
            "target table inventory changed during merge",
        ));
    }
    let mut untouched = 0u32;
    for (table, old) in &before_pointers {
        let new = after_pointers.get(table).ok_or_else(|| {
            RealGraphRunError::new(
                "real_graph_verification_failed",
                format!("target lost table {table}"),
            )
        })?;
        let expected_delta = match table.as_str() {
            ACCOUNT_TABLE => Some(2),
            TRANSFER_TABLE => Some(1),
            _ => None,
        };
        if let Some(delta) = expected_delta {
            if new.dataset_path != old.dataset_path
                || new.native_dataset_branch != old.native_dataset_branch
                || new.published_dataset_version <= old.published_dataset_version
                || new.entity_count != old.entity_count.saturating_add(delta)
            {
                return Err(RealGraphRunError::new(
                    "real_graph_verification_failed",
                    format!("target changed {table} outside the declared count/version delta"),
                ));
            }
        } else if new != old {
            return Err(RealGraphRunError::new(
                "real_graph_verification_failed",
                format!("untouched imported table {table} changed during merge"),
            ));
        } else {
            untouched = untouched.saturating_add(1);
        }
    }
    Ok(untouched)
}

fn snapshot_pointers(snapshot: &Snapshot) -> BTreeMap<String, TablePointerV1> {
    snapshot
        .datasets()
        .map(|entry| {
            (
                entry.type_key.clone(),
                TablePointerV1 {
                    type_key: entry.type_key.clone(),
                    dataset_path: entry.dataset_path.clone(),
                    published_dataset_version: entry.published_dataset_version,
                    native_dataset_branch: entry.native_dataset_branch.clone(),
                    entity_count: entry.entity_count,
                },
            )
        })
        .collect()
}

async fn verify_merge_commit(
    db: &Omnigraph,
    source_before: &Snapshot,
    target_before: &Snapshot,
    target_after: &Snapshot,
) -> Result<(), RealGraphRunError> {
    let source_head = source_before
        .graph_head(Some(SOURCE_BRANCH))
        .ok_or_else(|| {
            RealGraphRunError::new(
                "real_graph_verification_failed",
                "source has no prepared head",
            )
        })?;
    let target_head = target_before
        .graph_head(Some(TARGET_BRANCH))
        .ok_or_else(|| {
            RealGraphRunError::new(
                "real_graph_verification_failed",
                "target has no prepared head",
            )
        })?;
    let merged_head = target_after
        .graph_head(Some(TARGET_BRANCH))
        .ok_or_else(|| {
            RealGraphRunError::new(
                "real_graph_verification_failed",
                "target has no merged head",
            )
        })?;
    let commits = db
        .list_commits(Some(TARGET_BRANCH))
        .await
        .map_err(engine_worker_error)?;
    let merge = commits
        .iter()
        .find(|commit| commit.graph_commit_id == merged_head)
        .ok_or_else(|| {
            RealGraphRunError::new(
                "real_graph_verification_failed",
                "merged target head is absent from target lineage",
            )
        })?;
    if merge.parent_commit_id.as_deref() != Some(target_head)
        || merge.merged_parent_commit_id.as_deref() != Some(source_head)
    {
        return Err(RealGraphRunError::new(
            "real_graph_verification_failed",
            "merge commit parent provenance does not match prepared source and target heads",
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum SideExpectation {
    None,
    SourceOnly,
    TargetOnly,
    All,
}

async fn verify_reserved_entities(
    db: &Omnigraph,
    branch: &str,
    expectation: SideExpectation,
) -> Result<(), RealGraphRunError> {
    let source_present = matches!(
        expectation,
        SideExpectation::SourceOnly | SideExpectation::All
    );
    let target_present = matches!(
        expectation,
        SideExpectation::TargetOnly | SideExpectation::All
    );
    for (id, present) in [
        (SOURCE_A, source_present),
        (SOURCE_B, source_present),
        (TARGET_A, target_present),
        (TARGET_B, target_present),
    ] {
        let entity = db
            .entity_at_target(ReadTarget::branch(branch), ACCOUNT_TABLE, id)
            .await
            .map_err(engine_worker_error)?;
        verify_account(id, present, entity.as_ref())?;
    }
    for (id, source, target, amount, present) in [
        (SOURCE_EDGE, SOURCE_A, SOURCE_B, 1.25, source_present),
        (TARGET_EDGE, TARGET_A, TARGET_B, 2.5, target_present),
    ] {
        let entity = db
            .entity_at_target(ReadTarget::branch(branch), TRANSFER_TABLE, id)
            .await
            .map_err(engine_worker_error)?;
        verify_transfer(id, source, target, amount, present, entity.as_ref())?;
    }
    Ok(())
}

fn verify_account(
    id: &str,
    present: bool,
    entity: Option<&Value>,
) -> Result<(), RealGraphRunError> {
    if !present {
        if entity.is_some() {
            return Err(verification_error(format!(
                "reserved Account {id} unexpectedly exists"
            )));
        }
        return Ok(());
    }
    let entity = entity.ok_or_else(|| verification_error(format!("missing Account {id}")))?;
    if entity.as_object().map(serde_json::Map::len) != Some(7) {
        return Err(verification_error(format!(
            "Account {id} does not contain exactly the seven non-null schema fields (a null cell's key is omitted)"
        )));
    }
    for (field, expected) in [
        ("id", json!(id)),
        ("accountId", json!(id)),
        ("isBlocked", json!(false)),
        ("accountType", json!("internet_account")),
        ("freqLoginType", json!("ipv4")),
        ("accountLevel", json!("basic")),
        ("nickname", Value::Null),
        ("phonenum", Value::Null),
        ("email", Value::Null),
        ("lastLoginTime", Value::Null),
    ] {
        verify_field(entity, field, &expected, "Account", id)?;
    }
    let expected_time_ms = if id.starts_with("omnigraph-bench-fin-v1-src-") {
        1_577_836_800_000
    } else {
        1_577_836_801_000
    };
    verify_datetime_millis(entity.get("createTime"), expected_time_ms, "Account", id)?;
    Ok(())
}

fn verify_transfer(
    id: &str,
    source: &str,
    target: &str,
    amount: f64,
    present: bool,
    entity: Option<&Value>,
) -> Result<(), RealGraphRunError> {
    if !present {
        if entity.is_some() {
            return Err(verification_error(format!(
                "reserved transfer {id} unexpectedly exists"
            )));
        }
        return Ok(());
    }
    let entity = entity.ok_or_else(|| verification_error(format!("missing transfer {id}")))?;
    if entity.as_object().map(serde_json::Map::len) != Some(8) {
        return Err(verification_error(format!(
            "transfer {id} does not contain exactly the eight non-null schema fields (a null cell's key is omitted)"
        )));
    }
    let source_side = id == SOURCE_EDGE;
    for (field, expected) in [
        ("id", json!(id)),
        ("src", json!(source)),
        ("dst", json!(target)),
        ("amount", json!(amount)),
        ("payType", json!("bank_transfer")),
        ("goodsType", json!("bank_transfer")),
        ("comment", Value::Null),
        (
            "orderNum",
            json!(if source_side {
                "omnigraph-bench-fin-v1-src-order"
            } else {
                "omnigraph-bench-fin-v1-tgt-order"
            }),
        ),
    ] {
        verify_field(entity, field, &expected, "transfer", id)?;
    }
    verify_datetime_millis(
        entity.get("createTime"),
        if source_side {
            1_577_836_800_000
        } else {
            1_577_836_801_000
        },
        "transfer",
        id,
    )?;
    Ok(())
}

/// A null expectation means the key is absent: entity JSON omits a null cell.
fn verify_field(
    entity: &Value,
    field: &str,
    expected: &Value,
    kind: &str,
    id: &str,
) -> Result<(), RealGraphRunError> {
    let observed = entity.get(field);
    let matches = match expected {
        Value::Null => observed.is_none(),
        expected => observed == Some(expected),
    };
    if !matches {
        return Err(verification_error(format!(
            "{kind} {id}.{field} differs: expected {expected}, observed {observed:?}"
        )));
    }
    Ok(())
}

fn verify_datetime_millis(
    observed: Option<&Value>,
    expected: i64,
    kind: &str,
    id: &str,
) -> Result<(), RealGraphRunError> {
    let spelled = arrow_array::temporal_conversions::date64_to_datetime(expected)
        .map(|datetime| format!("{datetime:?}"))
        .ok_or_else(|| {
            verification_error(format!(
                "{kind} {id}.createTime expectation {expected} is unformattable"
            ))
        })?;
    let observed = observed
        .and_then(Value::as_str)
        .ok_or_else(|| verification_error(format!("{kind} {id}.createTime is not a string")))?;
    if observed != spelled {
        return Err(verification_error(format!(
            "{kind} {id}.createTime differs: expected {spelled} (epoch-ms {expected}), observed {observed}"
        )));
    }
    Ok(())
}

fn verification_error(message: impl Into<String>) -> RealGraphRunError {
    RealGraphRunError::new("real_graph_verification_failed", message)
}

fn engine_prepare_error(error: impl std::fmt::Display) -> RealGraphRunError {
    RealGraphRunError::new("real_graph_prepare_failed", error.to_string())
}

fn engine_worker_error(error: impl std::fmt::Display) -> RealGraphRunError {
    RealGraphRunError::new("real_graph_worker_failed", error.to_string())
}

fn path_utf8(path: &Path) -> Result<&str, RealGraphRunError> {
    path.to_str().ok_or_else(|| {
        RealGraphRunError::new("real_graph_path_invalid", "graph path must be valid UTF-8")
    })
}

fn remove_active(active: &Path, workspace: &Path) -> Result<(), RealGraphRunError> {
    if active.parent() != Some(workspace)
        || active.file_name().and_then(|name| name.to_str()) != Some("root")
    {
        return Err(RealGraphRunError::new(
            "real_graph_cleanup_refused",
            "refused to remove a path outside the staged run-owned root slot",
        ));
    }
    match fs::remove_dir_all(active) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(RealGraphRunError::new(
            "real_graph_cleanup_failed",
            format!(
                "remove disposable active graph {}: {error}",
                active.display()
            ),
        )),
    }
}

fn read_worker_request(path: &Path) -> Result<WorkerRequestV1, RealGraphRunError> {
    let bytes = read_bounded(path)?;
    serde_json::from_slice(&bytes).map_err(|error| {
        RealGraphRunError::new(
            "real_graph_worker_protocol_failed",
            format!("parse worker request: {error}"),
        )
    })
}

fn read_worker_envelope(path: &Path) -> Result<WorkerEnvelopeV1, RealGraphRunError> {
    let bytes = read_bounded(path)?;
    let envelope: WorkerEnvelopeV1 = serde_json::from_slice(&bytes).map_err(|error| {
        RealGraphRunError::new(
            "real_graph_worker_protocol_failed",
            format!("parse worker result: {error}"),
        )
    })?;
    if envelope.version != WORKER_PROTOCOL_VERSION
        || envelope.ok != envelope.sample.is_some()
        || envelope.ok != envelope.machine.is_some()
        || envelope.ok == envelope.error.is_some()
    {
        return Err(RealGraphRunError::new(
            "real_graph_worker_protocol_failed",
            "worker result envelope is inconsistent",
        ));
    }
    Ok(envelope)
}

fn read_bounded(path: &Path) -> Result<Vec<u8>, RealGraphRunError> {
    let metadata = fs::metadata(path).map_err(|error| {
        RealGraphRunError::new(
            "real_graph_worker_protocol_failed",
            format!("inspect worker file {}: {error}", path.display()),
        )
    })?;
    if metadata.len() > MAX_WORKER_FILE_BYTES {
        return Err(RealGraphRunError::new(
            "real_graph_worker_protocol_failed",
            "worker protocol file exceeds 1 MiB",
        ));
    }
    fs::read(path).map_err(|error| {
        RealGraphRunError::new(
            "real_graph_worker_protocol_failed",
            format!("read worker file {}: {error}", path.display()),
        )
    })
}

fn write_new(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;
    use omnigraph::loader::load_jsonl;

    #[test]
    fn run_spec_is_strict_and_bounded() {
        let directory = tempfile::tempdir().unwrap();
        let valid = directory.path().join("run.yaml");
        fs::write(
            &valid,
            "version: 1\nfixture_id: finbench-sf10-v1\nworkload: finbench-disjoint-insert-merge\nrepetitions: 2\noperation_deadline_seconds: 60\n",
        )
        .unwrap();
        assert!(load_real_graph_run_spec(&valid).ok);

        fs::write(
            &valid,
            "version: 1\nfixture_id: finbench-sf10-v1\nworkload: finbench-disjoint-insert-merge\nrepetitions: 0\noperation_deadline_seconds: 60\n",
        )
        .unwrap();
        assert_eq!(
            load_real_graph_run_spec(&valid).diagnostics[0].code,
            "invalid_real_graph_repetitions"
        );
    }

    #[test]
    fn cleanup_failure_is_never_hidden_by_the_run_outcome() {
        let cleanup = || {
            Diagnostic::error(
                "fixture_preflight_cleanup_failed",
                "$",
                "could not remove staged fixture",
            )
        };

        let cleanup_only = complete_real_graph_run::<()>(Ok(()), Err(cleanup())).unwrap_err();
        assert_eq!(cleanup_only.code, "fixture_cleanup_failed");
        assert!(
            cleanup_only
                .message
                .contains("fixture_preflight_cleanup_failed")
        );

        let both = complete_real_graph_run::<()>(
            Err(RealGraphRunError::new(
                "real_graph_worker_failed",
                "worker failed",
            )),
            Err(cleanup()),
        )
        .unwrap_err();
        assert_eq!(both.code, "real_graph_run_and_cleanup_failed");
        assert!(both.message.contains("real_graph_worker_failed"));
        assert!(both.message.contains("fixture_preflight_cleanup_failed"));

        let primary = complete_real_graph_run::<()>(
            Err(RealGraphRunError::new(
                "real_graph_worker_failed",
                "worker failed",
            )),
            Ok(()),
        )
        .unwrap_err();
        assert_eq!(primary.code, "real_graph_worker_failed");
        assert!(complete_real_graph_run(Ok(7_u8), Ok(())).is_ok());
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_real_graph_profile_remains_apfs_clonefile() {
        let profile = real_graph_local_profile().unwrap();
        assert_eq!(profile.filesystem, LocalFilesystem::Apfs);
        assert_eq!(profile.storage_class, LocalStorageClass::NvmeSsd);
        assert_eq!(profile.reset, ResetMode::LocalClonefile);
        assert_eq!(profile.reset_label, "apfs-clonefile-same-active-path");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_real_graph_profile_is_xfs_instance_nvme_plain_copy() {
        let profile = real_graph_local_profile().unwrap();
        assert_eq!(profile.filesystem, LocalFilesystem::Xfs);
        assert_eq!(profile.storage_class, LocalStorageClass::NvmeSsd);
        assert_eq!(profile.reset, ResetMode::PlainCopy);
        assert_eq!(
            profile.reset_label,
            "xfs-plain-copy-syncfs-same-active-path"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_real_graph_plain_copy_syncs_and_restores_the_exact_path() {
        let directory = tempfile::tempdir().unwrap();
        let active = directory.path().join("root");
        let template = directory.path().join("template");
        fs::create_dir(&active).unwrap();
        fs::write(active.join("data"), b"prepared graph").unwrap();
        let frozen = RealGraphTemplate::freeze(
            real_graph_local_profile().unwrap(),
            &active,
            &template,
            TraversalLimits::default(),
        )
        .unwrap();
        fs::remove_dir_all(&active).unwrap();

        let restored = frozen.restore_active().unwrap();
        assert_eq!(restored.root(), active);
        assert_eq!(fs::read(active.join("data")).unwrap(), b"prepared graph");
        restored.verify_unchanged().unwrap();
        frozen.verify_unchanged().unwrap();
        assert!(sync_plain_copy_filesystem(&directory.path().join("missing")).is_err());
    }

    #[test]
    fn plain_copy_capacity_is_checked_at_the_exact_boundary() {
        let prepared_bytes = 42;
        let required = prepared_bytes + PLAIN_COPY_HEADROOM_BYTES;

        let insufficient =
            require_plain_copy_capacity(prepared_bytes, required - 1, "/scratch").unwrap_err();
        assert_eq!(insufficient.code, "insufficient_scratch_capacity");
        assert_eq!(
            require_plain_copy_capacity(prepared_bytes, required, "/scratch").unwrap(),
            required
        );

        let overflow = require_plain_copy_capacity(u64::MAX, u64::MAX, "/scratch").unwrap_err();
        assert_eq!(overflow.code, "required_scratch_capacity_overflow");
    }

    #[tokio::test]
    async fn native_finbench_delta_merges_and_verifies_exactly() {
        const SCHEMA: &str = r#"
            node Account {
                accountId: String @key
                createTime: DateTime
                isBlocked: Bool
                accountType: enum(internet_account)
                nickname: String?
                phonenum: String?
                email: String?
                freqLoginType: enum(ipv4)
                lastLoginTime: DateTime?
                accountLevel: enum(basic)
            }
            edge AccountTransferAccount: Account -> Account {
                amount: F64
                createTime: DateTime
                orderNum: String
                comment: String?
                payType: enum(bank_transfer)
                goodsType: enum(bank_transfer)
            }
        "#;
        const BASE: &str = r#"{"type":"Account","data":{"accountId":"existing-a","createTime":"2019-01-01T00:00:00Z","isBlocked":false,"accountType":"internet_account","freqLoginType":"ipv4","accountLevel":"basic"}}
{"type":"Account","data":{"accountId":"existing-b","createTime":"2019-01-01T00:00:00Z","isBlocked":false,"accountType":"internet_account","freqLoginType":"ipv4","accountLevel":"basic"}}
{"edge":"AccountTransferAccount","from":"existing-a","to":"existing-b","data":{"id":"existing-transfer","amount":9.0,"createTime":"2019-01-01T00:00:00Z","orderNum":"existing-order","payType":"bank_transfer","goodsType":"bank_transfer"}}"#;
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("active");
        let db = Omnigraph::init(root.to_str().unwrap(), SCHEMA)
            .await
            .unwrap();
        load_jsonl(&db, BASE, LoadMode::Overwrite).await.unwrap();
        drop(db);

        prepare_finbench_delta(&root).await.unwrap();
        let (sample, machine) = execute_worker(WorkerRequestV1 {
            version: WORKER_PROTOCOL_VERSION,
            repetition: 1,
            root,
            operation_deadline_seconds: 60,
        })
        .await
        .unwrap();

        assert_eq!(sample.outcome, "merged");
        machine.validate().unwrap();
        assert_eq!(sample.route.table_walk_intervals, 2);
        assert!(sample.inserted_delta_verified);
        assert!(!sample.existing_rows_in_changed_tables_verified);
        assert!(sample.protected_heads_verified);
    }

    #[test]
    fn worker_machine_identity_is_established_once_and_drift_is_refused() {
        let first = capture_machine_identity().unwrap();
        let mut expected = None;
        accept_worker_machine(&mut expected, first.clone(), 1).unwrap();
        assert_eq!(expected.as_ref(), Some(&first));
        accept_worker_machine(&mut expected, first.clone(), 2).unwrap();

        let mut changed = first.clone();
        changed.machine_label = format!("hostname-sha256:{}", "0".repeat(64));
        let error = accept_worker_machine(&mut expected, changed, 3).unwrap_err();
        assert_eq!(error.code, "machine_identity_changed");
        assert!(error.message.contains("repetition 3"));
        assert_eq!(expected, Some(first));
    }
}
