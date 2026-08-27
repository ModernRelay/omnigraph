//! Durable, content-addressable benchmark run records.
//!
//! A record contains one resolved experiment, one system under test, and every
//! successful repetition from one invocation.  It deliberately excludes host
//! paths and suite presentation: the full typed point identity is the natural
//! series key, while the invocation ULID identifies this one acquisition.
//!
//! JSON is a storage encoding, not an alternate schema.  Readers accept only
//! the compact typed serialization emitted by [`canonical_record_bytes`].
//! Rejecting reordered, duplicate, unknown, or otherwise non-canonical input
//! makes the SHA-256 content address unambiguous.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::branch_merge::{BranchMergePlan, TARGET_BRANCH};
use crate::case::{
    Backend, CaseV1, LocalFilesystem, LocalStorageClass, PointIdentityV1, S3Implementation,
    S3Versioning,
};
use crate::counting::LogicalCallCounts;
use crate::machine::MachineIdentityV1;
use crate::model::typed_sha256;
use crate::runner::{
    ControlCallObservation, EffectiveEnvironmentValue, MergeRouteObservation,
    PHYSICAL_TREE_DIGEST_ALGORITHM, PhaseObservation, RepObservation, RunExecution,
    VerificationObservation,
};
use crate::suite::MAX_REPETITIONS_PER_CASE;
use crate::{CASE_FORMAT_VERSION, POINT_IDENTITY_VERSION, ResolvedRun, validate_case};

pub use crate::runner::{
    FIXTURE_MANIFEST_FORMAT_VERSION, FIXTURE_VALIDATOR_VERSION, FixtureManifestV1,
    FixtureValidationStampV1, LogicalFixtureIdentityV1, PhysicalFixtureIdentityV1,
    StampedFixtureManifestV1,
};

/// Version of the durable run-record contract.
pub const RUN_RECORD_FORMAT_VERSION: u32 = 1;
/// Rule-7 default: a claim must exceed two times its applicable floor.
pub const DEFAULT_FLOOR_MULTIPLIER_MILLIS: u32 = 2_000;

/// Maximum canonical bytes accepted for one run-record-v1 authority object.
pub const MAX_RECORD_BYTES: usize = 64 * 1024 * 1024;
const MAX_TEXT_BYTES: usize = 1_024;
const MAX_ACQUISITION_ERROR_CODE_BYTES: usize = 128;
// The projection stores the complete SUT JSON as one escaped field beside
// selected normalized SUT fields in a 64 KiB RunRow. Keeping the canonical SUT
// identity at or below 8 KiB leaves bounded headroom for that second escaping,
// the independently bounded machine/backend facts, and the remaining row
// fields. Changing either side requires a record/projection contract review.
const MAX_SUT_IDENTITY_BYTES: usize = 8 * 1024;

/// One finalized benchmark invocation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunRecordV1 {
    pub format_version: u32,
    pub invocation: InvocationIdentityV1,
    pub run: ResolvedRunIdentityV1,
    pub sut: SutIdentityV1,
    pub machine: MachineIdentityV1,
    pub backend: ObservedBackendV1,
    pub fixture: StampedFixtureManifestV1,
    pub acquisition: AcquisitionV1,
    pub measurements: MeasurementsV1,
}

impl RunRecordV1 {
    /// A performance claim requires both a complete acquisition and proof of
    /// the effective build settings that produced the measured executable.
    /// Current local records deliberately lack the latter proof and therefore
    /// remain useful evidence without becoming claim-authorizing evidence.
    pub const fn claim_eligible(&self) -> bool {
        self.acquisition.is_complete() && self.sut.build.effective_codegen_options_proved
    }
}

/// Globally unique invocation identity and its containing machine session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvocationIdentityV1 {
    pub invocation_id: String,
    pub session_id: String,
    /// UTC milliseconds since the Unix epoch.  Ordering only; the ULID is the
    /// identity and carries the same timestamp prefix.
    pub invoked_at_unix_ms: u64,
}

/// Complete canonical experiment identity persisted beside its full digest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedRunIdentityV1 {
    pub point_identity_version: u32,
    pub point_id: String,
    pub point_name: String,
    pub case_id: String,
    pub case_digest: String,
    pub run_spec: PointIdentityV1,
}

/// The system under test.  It is intentionally outside the point identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SutIdentityV1 {
    pub package_version: String,
    pub source_commit: String,
    pub source_tree_dirty: bool,
    pub build: BuildIdentityV1,
    pub engine: EngineConfigurationV1,
}

/// Build facts tied to the exact worker binary used for every repetition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BuildIdentityV1 {
    pub profile: String,
    pub cargo_opt_level: String,
    pub debug_assertions: bool,
    pub target_triple: String,
    pub rustc_version: String,
    /// Checked-in `[profile.release]` declarations. Cargo build scripts cannot
    /// prove that these survived config files or direct target rustc flags.
    pub declared_release_lto: String,
    pub declared_release_codegen_units: u32,
    pub declared_release_strip: bool,
    pub cargo_encoded_rustflags_present: bool,
    pub release_profile_environment_overrides_supported: bool,
    /// Explicit absence statement until a digest-bound controlled build
    /// receipt captures the final target rustc invocation.
    pub effective_codegen_options_proved: bool,
    pub worker_executable_sha256: String,
}

/// Engine settings whose presence can change the measured implementation.
///
/// Both lists are canonical sorted sets.  An empty list explicitly means that
/// no setting in that class was enabled; prose labels never substitute for
/// configuration identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineConfigurationV1 {
    pub feature_flags: Vec<String>,
    pub enabled_techniques: Vec<String>,
    pub lance_mem_pool_size: EffectiveEnvironmentValue,
}

/// Backend facts observed by a trusted probe, separate from the declaration
/// already carried in the point identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum ObservedBackendV1 {
    LocalFs {
        filesystem: LocalFilesystem,
        storage_class: LocalStorageClass,
        storage_protocol: String,
        probe: String,
    },
    S3 {
        implementation: S3Implementation,
        implementation_version: String,
        region: String,
        storage_class: String,
        versioning: S3Versioning,
        image_digest: Option<String>,
        probe: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcquisitionV1 {
    pub status: AcquisitionStatusV1,
    pub requested_repetitions: u32,
    pub observed_repetitions: u32,
    pub terminal: Option<AcquisitionTerminalV1>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AcquisitionStatusV1 {
    Complete,
    Censored,
}

/// Stable terminal evidence for an acquisition that stopped after a verified
/// prefix. Prose and stderr are intentionally excluded from durable identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcquisitionTerminalV1 {
    /// Zero-based index of the first repetition that did not become a verified
    /// sample. It therefore equals `observed_repetitions` for a censored run.
    pub failed_repetition: u32,
    pub stage: AcquisitionTerminalStageV1,
    /// Stable lowercase identifier: underscore-delimited segments beginning
    /// with an ASCII letter, with a total encoded length of at most 128 bytes.
    pub code: String,
}

/// Closed acquisition-stage vocabulary persisted by run-record-v1.
///
/// Worker stages name structured worker failures. The remaining stages are
/// exact supervisor boundaries that can stop an acquisition after an earlier
/// repetition has been fully verified. `Runner` covers failures outside a
/// live child-process boundary, such as reset or fixture-integrity checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AcquisitionTerminalStageV1 {
    Runner,
    SupervisorPanic,
    Bootstrap,
    Prepare,
    Measure,
    Verify,
    Finalize,
    Protocol,
    PipeSetup,
    WriterSetup,
    ReaderSetup,
    RequestWrite,
    PrepareTimeout,
    PrepareProtocol,
    BeginWrite,
    MeasureTimeout,
    MeasureProtocol,
    VerifyTimeout,
    VerifyProtocol,
    FinalizeProtocol,
    ExitTimeout,
    GroupProof,
    FinalizeExit,
    StructuredFailureReap,
}

impl AcquisitionTerminalStageV1 {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Runner => "runner",
            Self::SupervisorPanic => "supervisor-panic",
            Self::Bootstrap => "bootstrap",
            Self::Prepare => "prepare",
            Self::Measure => "measure",
            Self::Verify => "verify",
            Self::Finalize => "finalize",
            Self::Protocol => "protocol",
            Self::PipeSetup => "pipe-setup",
            Self::WriterSetup => "writer-setup",
            Self::ReaderSetup => "reader-setup",
            Self::RequestWrite => "request-write",
            Self::PrepareTimeout => "prepare-timeout",
            Self::PrepareProtocol => "prepare-protocol",
            Self::BeginWrite => "begin-write",
            Self::MeasureTimeout => "measure-timeout",
            Self::MeasureProtocol => "measure-protocol",
            Self::VerifyTimeout => "verify-timeout",
            Self::VerifyProtocol => "verify-protocol",
            Self::FinalizeProtocol => "finalize-protocol",
            Self::ExitTimeout => "exit-timeout",
            Self::GroupProof => "group-proof",
            Self::FinalizeExit => "finalize-exit",
            Self::StructuredFailureReap => "structured-failure-reap",
        }
    }
}

impl AcquisitionTerminalV1 {
    /// Construct terminal evidence while enforcing the record's stable code
    /// grammar before any archive publication is attempted.
    pub fn new(
        failed_repetition: u32,
        stage: AcquisitionTerminalStageV1,
        code: impl Into<String>,
    ) -> RecordResult<Self> {
        let terminal = Self {
            failed_repetition,
            stage,
            code: code.into(),
        };
        validate_acquisition_error_code(&terminal.code, "acquisition.terminal.code")?;
        Ok(terminal)
    }
}

impl AcquisitionV1 {
    /// Whether acquisition itself reached its requested verified sample count.
    /// Record-level claim eligibility has additional SUT/build gates.
    pub const fn is_complete(&self) -> bool {
        matches!(self.status, AcquisitionStatusV1::Complete)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MeasurementsV1 {
    pub wall_clock: WallClockSummaryV1,
    pub raw_samples: Vec<RawSampleV1>,
    pub layer_presence: MeasurementLayerPresenceV1,
    pub claim_policy: ClaimPolicyV1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WallClockSummaryV1 {
    pub min_us: u64,
    pub p50_us: u64,
    pub max_us: u64,
    pub p95_us: Option<u64>,
    pub p95_supported: bool,
    pub evidence: EvidenceStrengthV1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum EvidenceStrengthV1 {
    Directional,
    DistributionSupported,
}

/// Raw measured row.  Physical request attempts are not smuggled into the
/// logical counts; their record-level presence statement is separate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawSampleV1 {
    pub repetition: u32,
    pub input_physical_digest_sha256: String,
    pub elapsed_us: u64,
    pub peak_rss_bytes: u64,
    pub outcome: String,
    pub phases: Vec<PhaseObservation>,
    pub route: MergeRouteObservation,
    pub logical_store_calls: LogicalStoreCallsV1,
    pub control_store_calls: ControlCallObservation,
    pub verification: VerificationObservation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogicalStoreCallsV1 {
    pub manifest: LogicalCallCounts,
    pub table: LogicalCallCounts,
}

/// Explicit presence statements for each layer and each RFC-0039 operand.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MeasurementLayerPresenceV1 {
    pub logical: LayerMeasurementsV1,
    pub physical: LayerMeasurementsV1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LayerMeasurementsV1 {
    pub counts: MeasurementPresenceV1,
    pub calibration: MeasurementPresenceV1,
    pub request_timing: MeasurementPresenceV1,
    pub concurrency_witness: MeasurementPresenceV1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "kebab-case", deny_unknown_fields)]
pub enum MeasurementPresenceV1 {
    Observed,
    Absent { reason: MeasurementAbsenceReasonV1 },
    NotApplicable { reason: MeasurementAbsenceReasonV1 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MeasurementAbsenceReasonV1 {
    PhysicalAttemptsNotObservableAtLogicalWrappingSeam,
    LogicalCalibrationNotRun,
    PhysicalCalibrationNotRun,
    LogicalRequestTimingNotCaptured,
    PhysicalRequestTimingNotCaptured,
    LogicalConcurrencyWitnessUndefined,
    PhysicalConcurrencyWitnessNotCaptured,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimPolicyV1 {
    pub floor_multiplier_millis: u32,
}

/// Facts supplied at record-finalization time, after execution has quiesced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordInputV1 {
    pub invocation: InvocationIdentityV1,
    pub sut: SutIdentityV1,
    pub backend: ObservedBackendV1,
    pub fixture: StampedFixtureManifestV1,
}

/// Stable, path-addressed refusal from record construction or loading.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RecordError {
    pub code: &'static str,
    pub path: String,
    pub message: String,
}

impl RecordError {
    fn new(code: &'static str, path: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code,
            path: path.into(),
            message: message.into(),
        }
    }
}

impl Display for RecordError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{} at {}: {}",
            self.code, self.path, self.message
        )
    }
}

impl Error for RecordError {}

pub type RecordResult<T> = Result<T, RecordError>;

/// Bind durable SUT identity to the exact worker build attested by the runner.
pub fn sut_identity_for_execution(execution: &RunExecution) -> RecordResult<SutIdentityV1> {
    let worker_executable_sha256 = execution
        .build
        .worker_executable_sha256
        .clone()
        .ok_or_else(|| {
            RecordError::new(
                "missing_worker_attestation",
                "execution.build.worker_executable_sha256",
                "a durable timing record requires an attested worker executable",
            )
        })?;
    let sut = SutIdentityV1 {
        package_version: env!("CARGO_PKG_VERSION").to_string(),
        source_commit: execution.build.source_commit.clone(),
        source_tree_dirty: execution.build.source_tree_dirty,
        build: BuildIdentityV1 {
            profile: execution.build.cargo_profile.clone(),
            cargo_opt_level: execution.build.cargo_opt_level.clone(),
            debug_assertions: execution.build.debug_assertions,
            target_triple: execution.build.target_triple.clone(),
            rustc_version: execution.build.rustc_version.clone(),
            declared_release_lto: execution.build.declared_release_lto.clone(),
            declared_release_codegen_units: execution.build.declared_release_codegen_units,
            declared_release_strip: execution.build.declared_release_strip,
            cargo_encoded_rustflags_present: execution.build.cargo_encoded_rustflags_present,
            release_profile_environment_overrides_supported: execution
                .build
                .release_profile_environment_overrides_supported,
            effective_codegen_options_proved: execution.build.effective_codegen_options_proved,
            worker_executable_sha256,
        },
        engine: EngineConfigurationV1 {
            feature_flags: execution.build.engine_feature_flags.clone(),
            enabled_techniques: execution.build.enabled_techniques.clone(),
            lance_mem_pool_size: execution.build.effective_lance_mem_pool_size.clone(),
        },
    };
    validate_sut(&sut)?;
    Ok(sut)
}

impl FixtureManifestV1 {
    /// Revalidate and reference the manifest sealed before repetition zero.
    pub fn new(run: &ResolvedRun, execution: &RunExecution) -> RecordResult<Self> {
        validate_resolved_run(run)?;
        validate_execution_binding(run, execution)?;
        validate_stamped_fixture(&execution.fixture.stamp, &run.case.identity)?;
        Ok(execution.fixture.stamp.manifest.clone())
    }
}

impl StampedFixtureManifestV1 {
    /// Seal a validated fixture manifest with the digest of its canonical
    /// compact JSON bytes.
    pub fn stamp(manifest: FixtureManifestV1) -> RecordResult<Self> {
        validate_fixture_manifest(&manifest)?;
        let manifest_sha256 = sha256_json(&manifest, "fixture_manifest_serialization_failed")?;
        Ok(Self {
            manifest_sha256,
            manifest,
        })
    }
}

/// Construct and revalidate a durable record from a successful runner result.
pub fn build_run_record(
    run: &ResolvedRun,
    execution: &RunExecution,
    input: RecordInputV1,
) -> RecordResult<RunRecordV1> {
    build_run_record_with_acquisition(run, execution, input, AcquisitionStatusV1::Complete, None)
}

/// Construct a durable, permanently claim-ineligible record from the verified
/// prefix of an acquisition that failed before its requested repetitions were
/// complete.
pub fn build_censored_run_record(
    run: &ResolvedRun,
    execution: &RunExecution,
    input: RecordInputV1,
    terminal: AcquisitionTerminalV1,
) -> RecordResult<RunRecordV1> {
    build_run_record_with_acquisition(
        run,
        execution,
        input,
        AcquisitionStatusV1::Censored,
        Some(terminal),
    )
}

fn build_run_record_with_acquisition(
    run: &ResolvedRun,
    execution: &RunExecution,
    input: RecordInputV1,
    status: AcquisitionStatusV1,
    terminal: Option<AcquisitionTerminalV1>,
) -> RecordResult<RunRecordV1> {
    validate_resolved_run(run)?;
    validate_execution_binding(run, execution)?;

    let expected_sut = sut_identity_for_execution(execution)?;
    if input.sut != expected_sut {
        return Err(RecordError::new(
            "sut_identity_mismatch",
            "sut",
            "supplied SUT identity does not match the exact worker build attested by the runner",
        ));
    }
    validate_backend_execution_binding(run, execution, &input.backend)?;
    validate_stamped_fixture(&execution.fixture.stamp, &run.case.identity)?;
    if input.fixture != execution.fixture.stamp {
        return Err(RecordError::new(
            "fixture_execution_mismatch",
            "fixture",
            "supplied fixture manifest is not the exact pre-measurement stamp carried by the runner",
        ));
    }

    let mut raw_samples = Vec::with_capacity(execution.samples.len());
    for (index, sample) in execution.samples.iter().enumerate() {
        if sample.logical_store_calls.physical_attempts_observed {
            return Err(RecordError::new(
                "unsupported_physical_attempts",
                format!("execution.samples[{index}].logical_store_calls"),
                "record-v1 has no physical-attempt measurement schema; refusing to discard observed data",
            ));
        }
        raw_samples.push(RawSampleV1::try_from(sample)?);
    }

    let observed_repetitions = u32::try_from(raw_samples.len()).map_err(|_| {
        RecordError::new(
            "repetition_overflow",
            "measurements.raw_samples",
            "sample count does not fit u32",
        )
    })?;
    let record = RunRecordV1 {
        format_version: RUN_RECORD_FORMAT_VERSION,
        invocation: input.invocation,
        run: ResolvedRunIdentityV1 {
            point_identity_version: POINT_IDENTITY_VERSION,
            point_id: run.case.point_id.clone(),
            point_name: run.case.point_name.clone(),
            case_id: run.case.definition.id.clone(),
            case_digest: run.case.case_digest.clone(),
            run_spec: run.case.identity.clone(),
        },
        sut: input.sut,
        machine: execution.machine.clone(),
        backend: input.backend,
        fixture: input.fixture,
        acquisition: AcquisitionV1 {
            status,
            requested_repetitions: run.repetitions,
            observed_repetitions,
            terminal,
        },
        measurements: MeasurementsV1 {
            wall_clock: WallClockSummaryV1 {
                min_us: execution.wall_clock.min_us,
                p50_us: execution.wall_clock.p50_us,
                max_us: execution.wall_clock.max_us,
                p95_us: execution.wall_clock.p95_us,
                p95_supported: execution.wall_clock.p95_supported,
                evidence: evidence_strength(raw_samples.len()),
            },
            raw_samples,
            layer_presence: v1_layer_presence(),
            claim_policy: ClaimPolicyV1 {
                floor_multiplier_millis: DEFAULT_FLOOR_MULTIPLIER_MILLIS,
            },
        },
    };
    validate_run_record(&record)?;
    Ok(record)
}

impl TryFrom<&RepObservation> for RawSampleV1 {
    type Error = RecordError;

    fn try_from(sample: &RepObservation) -> Result<Self, Self::Error> {
        let peak_rss_bytes = sample.peak_rss_bytes.ok_or_else(|| {
            RecordError::new(
                "missing_peak_rss",
                format!("execution.samples[{}].peak_rss_bytes", sample.repetition),
                "durable samples require the supervisor-observed peak resident set size",
            )
        })?;
        Ok(Self {
            repetition: sample.repetition,
            input_physical_digest_sha256: sample.input_physical_digest_sha256.clone(),
            elapsed_us: sample.elapsed_us,
            peak_rss_bytes,
            outcome: sample.outcome.clone(),
            phases: sample.phases.clone(),
            route: sample.route.clone(),
            logical_store_calls: LogicalStoreCallsV1 {
                manifest: sample.logical_store_calls.manifest,
                table: sample.logical_store_calls.table,
            },
            control_store_calls: sample.control_store_calls,
            verification: sample.verification.clone(),
        })
    }
}

/// Recompute every redundant identity and summary; any uncertainty refuses the
/// record instead of degrading to an `unknown` field.
pub fn validate_run_record(record: &RunRecordV1) -> RecordResult<()> {
    if record.format_version != RUN_RECORD_FORMAT_VERSION {
        return Err(RecordError::new(
            "unsupported_record_version",
            "format_version",
            format!(
                "expected {RUN_RECORD_FORMAT_VERSION}, observed {}",
                record.format_version
            ),
        ));
    }
    validate_invocation(&record.invocation)?;
    let sealed = validate_recorded_run_identity(&record.run)?;
    validate_sut(&record.sut)?;
    record.machine.validate().map_err(|error| {
        RecordError::new(
            "invalid_machine_identity",
            format!("machine.{}", error.field),
            error.message,
        )
    })?;
    validate_backend(&sealed.definition.environment.backend, &record.backend)?;
    validate_stamped_fixture(&record.fixture, &record.run.run_spec)?;
    validate_measurements(record, &sealed)?;
    Ok(())
}

/// Canonical compact JSON bytes used both for persistence and content address.
pub fn canonical_record_bytes(record: &RunRecordV1) -> RecordResult<Vec<u8>> {
    validate_run_record(record)?;
    let bytes = serde_json::to_vec(record).map_err(|error| {
        RecordError::new(
            "record_serialization_failed",
            "$",
            format!("could not serialize run record: {error}"),
        )
    })?;
    if bytes.len() > MAX_RECORD_BYTES {
        return Err(RecordError::new(
            "record_too_large",
            "$",
            format!("run record exceeds the {MAX_RECORD_BYTES}-byte limit"),
        ));
    }
    Ok(bytes)
}

/// Parse only the exact bytes this schema emits.  Whitespace, alternate field
/// order, duplicate keys, unknown fields, and non-canonical number spellings
/// are rejected rather than acquiring a second content address.
pub fn parse_canonical_record(bytes: &[u8]) -> RecordResult<RunRecordV1> {
    if bytes.len() > MAX_RECORD_BYTES {
        return Err(RecordError::new(
            "record_too_large",
            "$",
            format!("run record exceeds the {MAX_RECORD_BYTES}-byte limit"),
        ));
    }
    let record: RunRecordV1 = serde_json::from_slice(bytes).map_err(|error| {
        RecordError::new(
            "record_decode_failed",
            "$",
            format!("could not decode run-record-v1 JSON: {error}"),
        )
    })?;
    let canonical = canonical_record_bytes(&record)?;
    if canonical != bytes {
        return Err(RecordError::new(
            "non_canonical_record",
            "$",
            "record bytes are not the canonical compact run-record-v1 serialization",
        ));
    }
    Ok(record)
}

/// SHA-256 of [`canonical_record_bytes`].
pub fn record_content_sha256(record: &RunRecordV1) -> RecordResult<String> {
    let bytes = canonical_record_bytes(record)?;
    Ok(sha256_bytes(&bytes))
}

fn validate_resolved_run(run: &ResolvedRun) -> RecordResult<()> {
    let sealed = validate_case(run.case.definition.clone())
        .into_result()
        .map_err(|diagnostics| {
            RecordError::new(
                "invalid_resolved_run",
                "run.case",
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
        return Err(RecordError::new(
            "tampered_resolved_run",
            "run.case",
            "resolved case does not match its revalidated definition",
        ));
    }
    if !(1..=MAX_REPETITIONS_PER_CASE).contains(&run.repetitions) {
        return Err(RecordError::new(
            "invalid_requested_repetitions",
            "run.repetitions",
            format!("requested repetitions must be in 1..={MAX_REPETITIONS_PER_CASE}"),
        ));
    }
    Ok(())
}

fn validate_execution_binding(run: &ResolvedRun, execution: &RunExecution) -> RecordResult<()> {
    if execution.runner_output_version != crate::RUNNER_OUTPUT_VERSION || execution.durable_record {
        return Err(RecordError::new(
            "unsupported_runner_output",
            "execution.runner_output_version",
            "record-v1 accepts only the current non-durable runner diagnostic projection",
        ));
    }
    if execution.case_id != run.case.definition.id
        || execution.point_id != run.case.point_id
        || execution.point_name != run.case.point_name
        || execution.case_path != run.case_path
        || execution.requested_repetitions != run.repetitions
    {
        return Err(RecordError::new(
            "execution_identity_mismatch",
            "execution",
            "runner output does not match the resolved run supplied for record construction",
        ));
    }
    let observed_repetitions = u32::try_from(execution.samples.len()).map_err(|_| {
        RecordError::new(
            "repetition_overflow",
            "execution.samples",
            "execution sample count does not fit u32",
        )
    })?;
    if execution.wall_clock.observed_repetitions != observed_repetitions {
        return Err(RecordError::new(
            "execution_repetition_mismatch",
            "execution.wall_clock.observed_repetitions",
            "runner wall-clock evidence does not match its raw sample count",
        ));
    }
    Ok(())
}

fn validate_backend_execution_binding(
    run: &ResolvedRun,
    execution: &RunExecution,
    observed: &ObservedBackendV1,
) -> RecordResult<()> {
    match (&run.case.definition.environment.backend, observed) {
        (
            Backend::LocalFs {
                filesystem,
                storage_class,
            },
            ObservedBackendV1::LocalFs {
                filesystem: observed_filesystem,
                storage_class: observed_storage,
                storage_protocol,
                probe,
            },
        ) if filesystem == observed_filesystem
            && storage_class == observed_storage
            && execution.environment.filesystem == local_filesystem_name(*filesystem)
            && execution.environment.storage_class == local_storage_name(*storage_class)
            && execution.environment.storage_protocol == storage_protocol.as_str()
            && execution.environment.probe == probe.as_str() =>
        {
            Ok(())
        }
        (Backend::S3 { .. }, _) => Err(RecordError::new(
            "unsupported_recording_backend",
            "backend",
            "the current RunExecution carries local probe evidence and cannot attest an S3 record",
        )),
        _ => Err(RecordError::new(
            "backend_execution_mismatch",
            "backend",
            "supplied observed backend does not exactly match the runner's environment evidence",
        )),
    }
}

fn local_filesystem_name(filesystem: LocalFilesystem) -> &'static str {
    match filesystem {
        LocalFilesystem::Apfs => "apfs",
        LocalFilesystem::Ext4 => "ext4",
        LocalFilesystem::Xfs => "xfs",
    }
}

fn local_storage_name(storage: LocalStorageClass) -> &'static str {
    match storage {
        LocalStorageClass::NvmeSsd => "nvme-ssd",
        LocalStorageClass::SataSsd => "sata-ssd",
        LocalStorageClass::NetworkBlock => "network-block",
        LocalStorageClass::RamDisk => "ram-disk",
    }
}

fn validate_recorded_run_identity(
    recorded: &ResolvedRunIdentityV1,
) -> RecordResult<crate::case::ValidatedCase> {
    if recorded.point_identity_version != POINT_IDENTITY_VERSION
        || recorded.run_spec.identity_version != POINT_IDENTITY_VERSION
        || recorded.point_identity_version != recorded.run_spec.identity_version
    {
        return Err(RecordError::new(
            "unsupported_point_identity_version",
            "run.point_identity_version",
            format!(
                "record and run spec must both use point identity version {POINT_IDENTITY_VERSION}"
            ),
        ));
    }
    let expected_point_id = typed_sha256(&recorded.run_spec).map_err(|diagnostic| {
        RecordError::new(
            "point_identity_serialization_failed",
            "run.run_spec",
            diagnostic.message,
        )
    })?;
    if recorded.point_id != expected_point_id {
        return Err(RecordError::new(
            "point_id_mismatch",
            "run.point_id",
            "point_id is not the SHA-256 of the persisted canonical run spec",
        ));
    }

    let definition = CaseV1 {
        version: CASE_FORMAT_VERSION,
        id: recorded.case_id.clone(),
        scenario: recorded.run_spec.scenario,
        fixture: recorded.run_spec.fixture.clone(),
        workload: recorded.run_spec.workload.clone(),
        environment: recorded.run_spec.environment.clone(),
        protocol: recorded.run_spec.protocol.clone(),
    };
    let sealed = validate_case(definition)
        .into_result()
        .map_err(|diagnostics| {
            RecordError::new(
                "invalid_recorded_run_spec",
                "run.run_spec",
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
    if sealed.identity != recorded.run_spec
        || sealed.point_id != recorded.point_id
        || sealed.point_name != recorded.point_name
        || sealed.case_digest != recorded.case_digest
    {
        return Err(RecordError::new(
            "recorded_run_identity_mismatch",
            "run",
            "case digest, point name, or normalized spec does not match the persisted run identity",
        ));
    }
    Ok(sealed)
}

fn validate_invocation(invocation: &InvocationIdentityV1) -> RecordResult<()> {
    let invocation_time = ulid_timestamp_ms(&invocation.invocation_id).ok_or_else(|| {
        RecordError::new(
            "invalid_invocation_id",
            "invocation.invocation_id",
            "invocation id must be a canonical 26-character uppercase Crockford ULID",
        )
    })?;
    let session_time = ulid_timestamp_ms(&invocation.session_id).ok_or_else(|| {
        RecordError::new(
            "invalid_session_id",
            "invocation.session_id",
            "session id must be a canonical 26-character uppercase Crockford ULID",
        )
    })?;
    if invocation.invocation_id == invocation.session_id {
        return Err(RecordError::new(
            "invocation_session_collision",
            "invocation",
            "invocation id and session id must be distinct",
        ));
    }
    if invocation_time != invocation.invoked_at_unix_ms {
        return Err(RecordError::new(
            "invocation_timestamp_mismatch",
            "invocation.invoked_at_unix_ms",
            format!(
                "timestamp is {}, but the invocation ULID encodes {invocation_time}",
                invocation.invoked_at_unix_ms
            ),
        ));
    }
    if session_time > invocation_time {
        return Err(RecordError::new(
            "session_after_invocation",
            "invocation.session_id",
            "session ULID timestamp cannot be later than its invocation",
        ));
    }
    Ok(())
}

fn validate_sut(sut: &SutIdentityV1) -> RecordResult<()> {
    validate_text(&sut.package_version, "sut.package_version")?;
    if !valid_lower_hex(&sut.source_commit, &[40, 64]) {
        return Err(RecordError::new(
            "invalid_source_commit",
            "sut.source_commit",
            "source commit must be exactly 40 or 64 lowercase hexadecimal characters",
        ));
    }
    if sut.source_tree_dirty {
        return Err(RecordError::new(
            "dirty_source_tree",
            "sut.source_tree_dirty",
            "source-commit provenance is incomplete for a binary built from an uncommitted tree",
        ));
    }
    if sut.build.profile != "release"
        || sut.build.cargo_opt_level != "2"
        || sut.build.debug_assertions
        || sut.build.declared_release_lto != "thin"
        || sut.build.declared_release_codegen_units != 16
        || !sut.build.declared_release_strip
        || sut.build.cargo_encoded_rustflags_present
        || !sut.build.release_profile_environment_overrides_supported
        || sut.build.effective_codegen_options_proved
    {
        return Err(RecordError::new(
            "non_release_timing_record",
            "sut.build",
            "wall-clock records require Cargo profile=release, Cargo opt-level=2, debug-assertions=false, the checked-in release declaration, no build-script-visible encoded Rust flags, supported release-profile environment overrides, and an explicit false effective-codegen proof until controlled receipts exist",
        ));
    }
    validate_text(&sut.build.target_triple, "sut.build.target_triple")?;
    validate_text(&sut.build.rustc_version, "sut.build.rustc_version")?;
    validate_sha256(
        &sut.build.worker_executable_sha256,
        "sut.build.worker_executable_sha256",
    )?;
    validate_sorted_strings(&sut.engine.feature_flags, "sut.engine.feature_flags")?;
    validate_sorted_strings(
        &sut.engine.enabled_techniques,
        "sut.engine.enabled_techniques",
    )?;
    validate_effective_environment_value(
        &sut.engine.lance_mem_pool_size,
        "sut.engine.lance_mem_pool_size",
    )?;
    let canonical = serde_json::to_vec(sut).map_err(|error| {
        RecordError::new(
            "sut_identity_serialization_failed",
            "sut",
            format!("could not serialize the canonical SUT identity: {error}"),
        )
    })?;
    if canonical.len() > MAX_SUT_IDENTITY_BYTES {
        return Err(RecordError::new(
            "sut_identity_too_large",
            "sut",
            format!(
                "canonical SUT identity is {} bytes; record-v1 permits at most {MAX_SUT_IDENTITY_BYTES} so every valid record remains projectable",
                canonical.len()
            ),
        ));
    }
    Ok(())
}

fn validate_effective_environment_value(
    value: &EffectiveEnvironmentValue,
    _path: &str,
) -> RecordResult<()> {
    match value {
        EffectiveEnvironmentValue::Unset | EffectiveEnvironmentValue::Bytes { .. } => Ok(()),
    }
}

fn validate_backend(declared: &Backend, observed: &ObservedBackendV1) -> RecordResult<()> {
    match (declared, observed) {
        (
            Backend::LocalFs {
                filesystem: declared_filesystem,
                storage_class: declared_storage,
            },
            ObservedBackendV1::LocalFs {
                filesystem,
                storage_class,
                storage_protocol,
                probe,
            },
        ) if declared_filesystem == filesystem && declared_storage == storage_class => {
            validate_text(storage_protocol, "backend.storage_protocol")?;
            validate_text(probe, "backend.probe")?;
            Ok(())
        }
        (
            Backend::S3 {
                implementation: declared_implementation,
                implementation_version: declared_version,
                region: declared_region,
                storage_class: declared_class,
                versioning: declared_versioning,
                image_digest: declared_digest,
            },
            ObservedBackendV1::S3 {
                implementation,
                implementation_version,
                region,
                storage_class,
                versioning,
                image_digest,
                probe,
            },
        ) if declared_implementation == implementation
            && declared_version == implementation_version
            && declared_region == region
            && declared_class == storage_class
            && declared_versioning == versioning
            && declared_digest == image_digest =>
        {
            for (value, path) in [
                (implementation_version, "backend.implementation_version"),
                (region, "backend.region"),
                (storage_class, "backend.storage_class"),
                (probe, "backend.probe"),
            ] {
                validate_text(value, path)?;
            }
            if let Some(digest) = image_digest {
                validate_image_digest(digest, "backend.image_digest")?;
            }
            Ok(())
        }
        _ => Err(RecordError::new(
            "backend_identity_mismatch",
            "backend",
            "observed backend identity does not exactly match the run-spec declaration",
        )),
    }
}

fn validate_stamped_fixture(
    stamped: &StampedFixtureManifestV1,
    run_spec: &PointIdentityV1,
) -> RecordResult<()> {
    validate_sha256(&stamped.manifest_sha256, "fixture.manifest_sha256")?;
    validate_fixture_manifest(&stamped.manifest)?;
    let expected = sha256_json(&stamped.manifest, "fixture_manifest_serialization_failed")?;
    if stamped.manifest_sha256 != expected {
        return Err(RecordError::new(
            "fixture_manifest_digest_mismatch",
            "fixture.manifest_sha256",
            "manifest digest does not match its canonical typed contents",
        ));
    }
    let logical = &stamped.manifest.logical;
    if logical.builder != run_spec.fixture.builder
        || logical.data != run_spec.fixture.data
        || logical.state != run_spec.fixture.state
    {
        return Err(RecordError::new(
            "logical_fixture_identity_mismatch",
            "fixture.manifest.logical",
            "fixture builder, Data, or State differs from the canonical run spec",
        ));
    }
    Ok(())
}

fn validate_fixture_manifest(manifest: &FixtureManifestV1) -> RecordResult<()> {
    if manifest.format_version != FIXTURE_MANIFEST_FORMAT_VERSION {
        return Err(RecordError::new(
            "unsupported_fixture_manifest_version",
            "fixture.manifest.format_version",
            format!(
                "expected {FIXTURE_MANIFEST_FORMAT_VERSION}, observed {}",
                manifest.format_version
            ),
        ));
    }
    validate_sha256(
        &manifest.logical.logical_content_sha256,
        "fixture.manifest.logical.logical_content_sha256",
    )?;
    if manifest.physical.digest_algorithm != PHYSICAL_TREE_DIGEST_ALGORITHM {
        return Err(RecordError::new(
            "unsupported_physical_digest_algorithm",
            "fixture.manifest.physical.digest_algorithm",
            format!("expected {PHYSICAL_TREE_DIGEST_ALGORITHM}"),
        ));
    }
    validate_sha256(
        &manifest.physical.tree_sha256,
        "fixture.manifest.physical.tree_sha256",
    )?;
    if manifest.physical.files == 0 || manifest.physical.bytes == 0 {
        return Err(RecordError::new(
            "empty_physical_fixture",
            "fixture.manifest.physical",
            "a benchmark fixture must contain at least one non-empty regular file",
        ));
    }
    let validation = &manifest.validation;
    if validation.validator != "omnigraph-bench-fixture-validator"
        || validation.validator_version != FIXTURE_VALIDATOR_VERSION
    {
        return Err(RecordError::new(
            "unsupported_fixture_validator",
            "fixture.manifest.validation",
            format!("expected fixture validator version {FIXTURE_VALIDATOR_VERSION}"),
        ));
    }
    if validation.validated_at_unix_ms == 0
        || !validation.logical_content_verified
        || !validation.declared_state_verified
        || !validation.frozen
    {
        return Err(RecordError::new(
            "invalid_fixture_validation_stamp",
            "fixture.manifest.validation",
            "logical content, declared state, and frozen status must all be positively verified",
        ));
    }
    Ok(())
}

fn validate_measurements(
    record: &RunRecordV1,
    sealed: &crate::case::ValidatedCase,
) -> RecordResult<()> {
    let requested = record.acquisition.requested_repetitions;
    let observed = record.acquisition.observed_repetitions;
    if !(1..=MAX_REPETITIONS_PER_CASE).contains(&requested) {
        return Err(RecordError::new(
            "invalid_requested_repetitions",
            "acquisition.requested_repetitions",
            format!("requested repetitions must be in 1..={MAX_REPETITIONS_PER_CASE}"),
        ));
    }
    let sample_count = u32::try_from(record.measurements.raw_samples.len()).map_err(|_| {
        RecordError::new(
            "repetition_overflow",
            "measurements.raw_samples",
            "sample count does not fit u32",
        )
    })?;
    if observed != sample_count {
        return Err(RecordError::new(
            "repetition_count_mismatch",
            "acquisition",
            "observed repetitions must exactly match the raw sample count",
        ));
    }
    match (&record.acquisition.status, &record.acquisition.terminal) {
        (AcquisitionStatusV1::Complete, None) if observed == requested => {}
        (AcquisitionStatusV1::Censored, Some(terminal))
            if observed >= 1 && observed < requested && terminal.failed_repetition == observed =>
        {
            validate_acquisition_error_code(&terminal.code, "acquisition.terminal.code")?;
        }
        (AcquisitionStatusV1::Complete, _) => {
            return Err(RecordError::new(
                "invalid_complete_acquisition",
                "acquisition",
                "a complete acquisition requires requested=observed=samples and no terminal failure",
            ));
        }
        (AcquisitionStatusV1::Censored, _) => {
            return Err(RecordError::new(
                "invalid_censored_acquisition",
                "acquisition",
                "a censored acquisition requires 1 <= observed < requested, a terminal failure, and failed_repetition=observed",
            ));
        }
    }
    let expected_presence = v1_layer_presence();
    if record.measurements.layer_presence != expected_presence {
        return Err(RecordError::new(
            "invalid_measurement_presence",
            "measurements.layer_presence",
            "run-record-v1 observes logical counts only and requires exact typed absence statements for every other layer/operand",
        ));
    }
    if record.measurements.claim_policy.floor_multiplier_millis != DEFAULT_FLOOR_MULTIPLIER_MILLIS {
        return Err(RecordError::new(
            "invalid_claim_policy",
            "measurements.claim_policy.floor_multiplier_millis",
            format!(
                "run-record-v1 requires the protocol default {DEFAULT_FLOOR_MULTIPLIER_MILLIS} (2.000x)"
            ),
        ));
    }

    let physical_sha = &record.fixture.manifest.physical.tree_sha256;
    let plan = BranchMergePlan::try_from(sealed).map_err(|error| {
        RecordError::new(
            "invalid_recorded_run_plan",
            "run.run_spec",
            format!("could not derive the deterministic branch-merge plan: {error}"),
        )
    })?;
    let expected_tables = plan.tables;
    let expected_walks = u64::try_from(plan.diverged_tables).map_err(|_| {
        RecordError::new(
            "table_count_overflow",
            "run.run_spec.workload.diverged_tables",
            "diverged table count does not fit u64",
        )
    })?;
    let expected_rows = plan.expected_merged_rows().map_err(|error| {
        RecordError::new(
            "expected_row_count_failed",
            "run.run_spec",
            format!("could not derive the exact merged row count: {error}"),
        )
    })?;
    for (index, sample) in record.measurements.raw_samples.iter().enumerate() {
        if sample.repetition != index as u32 {
            return Err(RecordError::new(
                "non_contiguous_repetitions",
                format!("measurements.raw_samples[{index}].repetition"),
                "raw samples must be ordered and numbered contiguously from zero",
            ));
        }
        validate_sha256(
            &sample.input_physical_digest_sha256,
            &format!("measurements.raw_samples[{index}].input_physical_digest_sha256"),
        )?;
        if &sample.input_physical_digest_sha256 != physical_sha {
            return Err(RecordError::new(
                "sample_fixture_mismatch",
                format!("measurements.raw_samples[{index}].input_physical_digest_sha256"),
                "sample did not start from the stamped physical fixture",
            ));
        }
        if sample.elapsed_us == 0 || sample.peak_rss_bytes == 0 || sample.outcome != "merged" {
            return Err(RecordError::new(
                "invalid_sample_outcome",
                format!("measurements.raw_samples[{index}]"),
                "a durable branch-merge sample must have nonzero elapsed time, nonzero peak RSS, and outcome `merged`",
            ));
        }
        validate_projected_call_totals(index, sample)?;
        if sample.route.table_walk_intervals != expected_walks {
            return Err(RecordError::new(
                "route_evidence_mismatch",
                format!("measurements.raw_samples[{index}].route.table_walk_intervals"),
                format!("expected exactly {expected_walks} TableWalk intervals"),
            ));
        }
        validate_phases(index, &sample.phases, expected_walks)?;
        let verification = &sample.verification;
        validate_text(
            &verification.branch,
            &format!("measurements.raw_samples[{index}].verification.branch"),
        )?;
        if verification.branch != TARGET_BRANCH
            || verification.tables != expected_tables
            || verification.rows != expected_rows
            || !verification.exact_content
            || !verification.source_exact_content
            || !verification.main_exact_content
            || !verification.protected_heads_unchanged
        {
            return Err(RecordError::new(
                "invalid_sample_verification",
                format!("measurements.raw_samples[{index}].verification"),
                format!(
                    "expected exact verification of target branch {TARGET_BRANCH}, {expected_tables} tables, and {expected_rows} merged rows, plus exact source/main content and unchanged protected heads"
                ),
            ));
        }
    }

    let expected_summary = summarize_wall_clock(&record.measurements.raw_samples)?;
    if record.measurements.wall_clock != expected_summary {
        return Err(RecordError::new(
            "wall_clock_summary_mismatch",
            "measurements.wall_clock",
            "wall-clock summary does not equal nearest-rank statistics recomputed from raw samples",
        ));
    }
    Ok(())
}

/// Revalidate the counter algebra consumed by the V1 projection.
///
/// This keeps archive validity at least as strict as projection input: a
/// canonical record must not overflow either projected per-plane total, and
/// control mutation subcategories must remain a partitioned subset of the
/// aggregate mutation counter emitted by the engine wrapper.
fn validate_projected_call_totals(index: usize, sample: &RawSampleV1) -> RecordResult<()> {
    let sample_path = format!("measurements.raw_samples[{index}]");
    let manifest = checked_lance_call_total(
        sample.logical_store_calls.manifest,
        &format!("{sample_path}.logical_store_calls.manifest"),
    )?;
    let table = checked_lance_call_total(
        sample.logical_store_calls.table,
        &format!("{sample_path}.logical_store_calls.table"),
    )?;
    manifest.checked_add(table).ok_or_else(|| {
        RecordError::new(
            "logical_call_count_overflow",
            format!("{sample_path}.logical_store_calls"),
            "projected Lance data-plane logical call total does not fit u64",
        )
    })?;

    let control = &sample.control_store_calls;
    let classified_mutations = control
        .write_text
        .checked_add(control.delete)
        .ok_or_else(|| {
            RecordError::new(
                "control_call_count_inconsistent",
                format!("{sample_path}.control_store_calls"),
                "write_text + delete mutation subcategories do not fit u64",
            )
        })?;
    if classified_mutations > control.mutation_calls {
        return Err(RecordError::new(
            "control_call_count_inconsistent",
            format!("{sample_path}.control_store_calls"),
            "write_text + delete must not exceed the aggregate mutation_calls counter",
        ));
    }
    checked_call_sum(
        [
            control.read_text,
            control.read_text_if_exists,
            control.read_text_versioned,
            control.exists,
            control.list_dir,
            control.mutation_calls,
        ],
        &format!("{sample_path}.control_store_calls"),
        "projected control-plane logical call total",
    )?;
    Ok(())
}

fn checked_lance_call_total(counts: LogicalCallCounts, path: &str) -> RecordResult<u64> {
    checked_call_sum(
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
        path,
        "projected Lance logical call total",
    )
}

fn checked_call_sum(
    values: impl IntoIterator<Item = u64>,
    path: &str,
    noun: &str,
) -> RecordResult<u64> {
    let mut total = 0_u64;
    for value in values {
        total = total.checked_add(value).ok_or_else(|| {
            RecordError::new(
                "logical_call_count_overflow",
                path,
                format!("{noun} does not fit u64"),
            )
        })?;
    }
    Ok(total)
}

fn validate_phases(
    index: usize,
    phases: &[PhaseObservation],
    expected_walks: u64,
) -> RecordResult<()> {
    if phases.is_empty() {
        return Err(RecordError::new(
            "missing_phase_evidence",
            format!("measurements.raw_samples[{index}].phases"),
            "per-phase attribution requires at least one observed phase",
        ));
    }
    let mut names = BTreeSet::new();
    let mut table_walk = None;
    for (phase_index, phase) in phases.iter().enumerate() {
        validate_text(
            &phase.phase,
            &format!("measurements.raw_samples[{index}].phases[{phase_index}].phase"),
        )?;
        if !names.insert(phase.phase.as_str()) {
            return Err(RecordError::new(
                "duplicate_phase",
                format!("measurements.raw_samples[{index}].phases[{phase_index}].phase"),
                format!("phase `{}` occurs more than once", phase.phase),
            ));
        }
        if phase.interval_count == 0 || phase.max_us > phase.total_us {
            return Err(RecordError::new(
                "invalid_phase_measurement",
                format!("measurements.raw_samples[{index}].phases[{phase_index}]"),
                "phase intervals must be nonzero and max_us cannot exceed total_us",
            ));
        }
        if phase.phase == "TableWalk" {
            table_walk = Some(phase.interval_count);
        }
    }
    if table_walk != Some(expected_walks) {
        return Err(RecordError::new(
            "table_walk_phase_mismatch",
            format!("measurements.raw_samples[{index}].phases"),
            format!("TableWalk must contain exactly {expected_walks} intervals"),
        ));
    }
    Ok(())
}

fn summarize_wall_clock(samples: &[RawSampleV1]) -> RecordResult<WallClockSummaryV1> {
    if samples.is_empty() {
        return Err(RecordError::new(
            "empty_measurements",
            "measurements.raw_samples",
            "a finalized run record must contain at least one sample",
        ));
    }
    let mut durations = samples
        .iter()
        .map(|sample| sample.elapsed_us)
        .collect::<Vec<_>>();
    durations.sort_unstable();
    let p95_supported = durations.len() >= 20;
    Ok(WallClockSummaryV1 {
        min_us: durations[0],
        p50_us: nearest_rank(&durations, 50),
        max_us: durations[durations.len() - 1],
        p95_us: p95_supported.then(|| nearest_rank(&durations, 95)),
        p95_supported,
        evidence: evidence_strength(durations.len()),
    })
}

fn evidence_strength(repetitions: usize) -> EvidenceStrengthV1 {
    if repetitions >= 20 {
        EvidenceStrengthV1::DistributionSupported
    } else {
        EvidenceStrengthV1::Directional
    }
}

fn v1_layer_presence() -> MeasurementLayerPresenceV1 {
    MeasurementLayerPresenceV1 {
        logical: LayerMeasurementsV1 {
            counts: MeasurementPresenceV1::Observed,
            calibration: MeasurementPresenceV1::Absent {
                reason: MeasurementAbsenceReasonV1::LogicalCalibrationNotRun,
            },
            request_timing: MeasurementPresenceV1::Absent {
                reason: MeasurementAbsenceReasonV1::LogicalRequestTimingNotCaptured,
            },
            concurrency_witness: MeasurementPresenceV1::NotApplicable {
                reason: MeasurementAbsenceReasonV1::LogicalConcurrencyWitnessUndefined,
            },
        },
        physical: LayerMeasurementsV1 {
            counts: MeasurementPresenceV1::Absent {
                reason:
                    MeasurementAbsenceReasonV1::PhysicalAttemptsNotObservableAtLogicalWrappingSeam,
            },
            calibration: MeasurementPresenceV1::Absent {
                reason: MeasurementAbsenceReasonV1::PhysicalCalibrationNotRun,
            },
            request_timing: MeasurementPresenceV1::Absent {
                reason: MeasurementAbsenceReasonV1::PhysicalRequestTimingNotCaptured,
            },
            concurrency_witness: MeasurementPresenceV1::Absent {
                reason: MeasurementAbsenceReasonV1::PhysicalConcurrencyWitnessNotCaptured,
            },
        },
    }
}

fn nearest_rank(sorted: &[u64], percentile: usize) -> u64 {
    let rank = percentile
        .checked_mul(sorted.len())
        .expect("record repetition bounds keep percentile rank in usize")
        .div_ceil(100)
        .saturating_sub(1);
    sorted[rank]
}

fn validate_sorted_strings(values: &[String], path: &str) -> RecordResult<()> {
    for (index, value) in values.iter().enumerate() {
        validate_text(value, &format!("{path}[{index}]"))?;
    }
    if values.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(RecordError::new(
            "non_canonical_string_set",
            path,
            "values must be strictly sorted and unique",
        ));
    }
    Ok(())
}

fn validate_text(value: &str, path: &str) -> RecordResult<()> {
    if value.is_empty()
        || value.len() > MAX_TEXT_BYTES
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        return Err(RecordError::new(
            "invalid_record_text",
            path,
            format!(
                "value must be non-empty, trimmed, control-free UTF-8 of at most {MAX_TEXT_BYTES} bytes"
            ),
        ));
    }
    Ok(())
}

fn validate_acquisition_error_code(value: &str, path: &str) -> RecordResult<()> {
    if value.is_empty() || value.len() > MAX_ACQUISITION_ERROR_CODE_BYTES || !value.is_ascii() {
        return Err(RecordError::new(
            "invalid_acquisition_error_code",
            path,
            format!(
                "error code must be a non-empty canonical ASCII token of at most {MAX_ACQUISITION_ERROR_CODE_BYTES} bytes"
            ),
        ));
    }

    let mut segment_start = true;
    for byte in value.bytes() {
        if segment_start {
            if !byte.is_ascii_lowercase() {
                return Err(RecordError::new(
                    "invalid_acquisition_error_code",
                    path,
                    "error-code segments must start with a lowercase ASCII letter",
                ));
            }
            segment_start = false;
        } else if byte == b'_' {
            segment_start = true;
        } else if !byte.is_ascii_lowercase() && !byte.is_ascii_digit() {
            return Err(RecordError::new(
                "invalid_acquisition_error_code",
                path,
                "error codes may contain only lowercase ASCII letters, digits, and single underscores between segments",
            ));
        }
    }
    if segment_start {
        return Err(RecordError::new(
            "invalid_acquisition_error_code",
            path,
            "error codes must not end with an underscore or contain empty segments",
        ));
    }
    Ok(())
}

fn validate_sha256(value: &str, path: &str) -> RecordResult<()> {
    if !valid_lower_hex(value, &[64]) {
        return Err(RecordError::new(
            "invalid_sha256",
            path,
            "digest must be exactly 64 lowercase hexadecimal characters",
        ));
    }
    Ok(())
}

fn validate_image_digest(value: &str, path: &str) -> RecordResult<()> {
    let Some(digest) = value.strip_prefix("sha256:") else {
        return Err(RecordError::new(
            "invalid_image_digest",
            path,
            "image digest must be exactly lowercase sha256:<64hex>",
        ));
    };
    if !valid_lower_hex(digest, &[64]) {
        return Err(RecordError::new(
            "invalid_image_digest",
            path,
            "image digest must be exactly lowercase sha256:<64hex>",
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

fn sha256_json<T: Serialize>(value: &T, code: &'static str) -> RecordResult<String> {
    let bytes = serde_json::to_vec(value).map_err(|error| {
        RecordError::new(
            code,
            "$",
            format!("could not serialize typed benchmark value: {error}"),
        )
    })?;
    Ok(sha256_bytes(&bytes))
}

fn sha256_bytes(bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(bytes);
    format!("{:x}", digest.finalize())
}

fn ulid_timestamp_ms(value: &str) -> Option<u64> {
    if value.len() != 26 || !value.is_ascii() {
        return None;
    }
    let mut timestamp = 0_u64;
    for (index, byte) in value.bytes().enumerate() {
        let digit = crockford_value(byte)?;
        if index == 0 && digit > 7 {
            return None;
        }
        if index < 10 {
            timestamp = timestamp.checked_mul(32)?.checked_add(u64::from(digit))?;
        }
    }
    Some(timestamp)
}

fn crockford_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'A'..=b'H' => Some(byte - b'A' + 10),
        b'J'..=b'K' => Some(byte - b'J' + 18),
        b'M'..=b'N' => Some(byte - b'M' + 20),
        b'P'..=b'T' => Some(byte - b'P' + 22),
        b'V'..=b'Z' => Some(byte - b'V' + 27),
        _ => None,
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::path::PathBuf;

    use crate::branch_merge::FixturePreflight;
    use crate::environment::LocalEnvironmentEvidence;
    use crate::machine::MACHINE_IDENTITY_FORMAT_VERSION;
    use crate::runner::{
        BuildEvidence, FixtureObservation, LogicalStoreCallObservation, WallClockSummary,
    };
    use crate::{RUNNER_OUTPUT_VERSION, parse_case};

    use super::*;

    const CASE: &str = r#"
version: 1
id: durable-record
scenario: branch-merge-v1
fixture:
  builder: { kind: synthetic-branch-merge, version: 2, seed: 0 }
  data: { provenance: synthetic, tables: 8, rows_per_table: 1000, payload_bytes: 64, column_shape: scalars, topology_skew: uniform }
  state: { aging: bulk-loaded, indexes: [], deletion_history: none, compaction_recency: not-optimized, history_depth: 21 }
workload: { delta_rows_per_side: 50, diverged_tables: 4, arrival: unscheduled-single-shot, clients: 1, read_write_mix: write-heavy, contention: distinct-key }
environment:
  backend: { kind: local-fs, filesystem: apfs, storage_class: nvme-ssd }
  network_position: same-host
  execution: embedded
  cache_condition: { process: fresh-per-repetition, engine: warmed-by-program, page_cache: program-conditioned, program: branch-merge-read-set-v1, iterations: 1 }
protocol: { deadline_seconds: 60, attribution: per-phase, schedule: manual, reset: local-clonefile, timer: monotonic }
"#;

    const INVOCATION_TIME_MS: u64 = 1_700_000_000_000;
    const PHYSICAL_SHA: &str = "2222222222222222222222222222222222222222222222222222222222222222";

    fn resolved_run() -> ResolvedRun {
        ResolvedRun {
            case_path: PathBuf::from("/tmp/durable-record.case-v1.yaml"),
            repetitions: 2,
            case: parse_case(CASE).into_result().unwrap(),
        }
    }

    fn sample(repetition: u32, elapsed_us: u64) -> RepObservation {
        RepObservation {
            repetition,
            input_physical_digest_sha256: PHYSICAL_SHA.to_string(),
            elapsed_us,
            peak_rss_bytes: Some(32 * 1024 * 1024),
            outcome: "merged".to_string(),
            phases: vec![PhaseObservation {
                phase: "TableWalk".to_string(),
                total_us: 8,
                max_us: 3,
                interval_count: 4,
            }],
            route: MergeRouteObservation {
                table_walk_intervals: 4,
                stage_merge_insert_calls: 1,
                stage_merge_insert_rows: 10,
                stage_known_present_update_calls: 1,
                stage_known_present_update_rows: 10,
                stage_fenced_insert_calls: 1,
                stage_fenced_insert_rows: 10,
                strict_insert_preflight_calls: 1,
            },
            logical_store_calls: LogicalStoreCallObservation {
                manifest: LogicalCallCounts {
                    get: 2,
                    ..Default::default()
                },
                table: LogicalCallCounts {
                    get: 3,
                    put: 1,
                    ..Default::default()
                },
                physical_attempts_observed: false,
            },
            control_store_calls: ControlCallObservation {
                read_text: 1,
                read_text_if_exists: 0,
                read_text_versioned: 0,
                exists: 0,
                list_dir: 0,
                mutation_calls: 1,
                write_text: 1,
                delete: 0,
            },
            verification: VerificationObservation {
                branch: "bench-target".to_string(),
                tables: 8,
                rows: 7_998,
                exact_content: true,
                source_exact_content: true,
                main_exact_content: true,
                protected_heads_unchanged: true,
            },
        }
    }

    fn execution(run: &ResolvedRun) -> RunExecution {
        RunExecution {
            runner_output_version: RUNNER_OUTPUT_VERSION,
            case_id: run.case.definition.id.clone(),
            case_path: run.case_path.clone(),
            point_id: run.case.point_id.clone(),
            point_name: run.case.point_name.clone(),
            cache_condition: run.case.definition.environment.cache_condition.clone(),
            requested_repetitions: run.repetitions,
            build: BuildEvidence {
                source_commit: "a".repeat(40),
                source_tree_dirty: false,
                cargo_profile: "release".to_string(),
                cargo_opt_level: "2".to_string(),
                debug_assertions: false,
                effective_lance_mem_pool_size: EffectiveEnvironmentValue::Unset,
                target_triple: "aarch64-apple-darwin".to_string(),
                rustc_version: "rustc 1.97.1".to_string(),
                declared_release_lto: "thin".to_string(),
                declared_release_codegen_units: 16,
                declared_release_strip: true,
                cargo_encoded_rustflags_present: false,
                release_profile_environment_overrides_supported: true,
                effective_codegen_options_proved: false,
                engine_feature_flags: Vec::new(),
                enabled_techniques: Vec::new(),
                worker_executable_sha256: Some("b".repeat(64)),
            },
            machine: machine(),
            environment: LocalEnvironmentEvidence {
                filesystem: "apfs".to_string(),
                storage_class: "nvme-ssd".to_string(),
                mount_point: "/".to_string(),
                storage_protocol: "Apple Fabric".to_string(),
                available_bytes: 1_000_000,
                probe: "macos-df-diskutil-v1",
            },
            fixture: FixtureObservation {
                preflight: FixturePreflight {
                    base_rows: 8_000,
                    estimated_generated_bytes: 512_000,
                    base_load_commits: 1,
                    divergence_commits_per_branch: 1,
                    optimize_commits: 0,
                    expected_history_depth: 1,
                    estimated_max_entries: 1_000,
                    required_scratch_bytes: 2_000_000,
                },
                stamp: fixture_stamp(run),
                base_load_commits: 1,
                optimized_user_tables: 0,
                source_history_depth: 1,
                target_history_depth: 1,
            },
            samples: vec![sample(0, 10), sample(1, 20)],
            wall_clock: WallClockSummary {
                observed_repetitions: 2,
                min_us: 10,
                p50_us: 10,
                max_us: 20,
                p95_us: None,
                p95_supported: false,
            },
            durable_record: false,
        }
    }

    fn sut() -> SutIdentityV1 {
        SutIdentityV1 {
            package_version: env!("CARGO_PKG_VERSION").to_string(),
            source_commit: "a".repeat(40),
            source_tree_dirty: false,
            build: BuildIdentityV1 {
                profile: "release".to_string(),
                cargo_opt_level: "2".to_string(),
                debug_assertions: false,
                target_triple: "aarch64-apple-darwin".to_string(),
                rustc_version: "rustc 1.97.1".to_string(),
                declared_release_lto: "thin".to_string(),
                declared_release_codegen_units: 16,
                declared_release_strip: true,
                cargo_encoded_rustflags_present: false,
                release_profile_environment_overrides_supported: true,
                effective_codegen_options_proved: false,
                worker_executable_sha256: "b".repeat(64),
            },
            engine: EngineConfigurationV1 {
                feature_flags: Vec::new(),
                enabled_techniques: Vec::new(),
                lance_mem_pool_size: EffectiveEnvironmentValue::Unset,
            },
        }
    }

    fn machine() -> MachineIdentityV1 {
        MachineIdentityV1 {
            format_version: MACHINE_IDENTITY_FORMAT_VERSION,
            os_name: "macos".to_string(),
            os_version: "15.6".to_string(),
            kernel_version: "24.6.0".to_string(),
            architecture: "aarch64".to_string(),
            cpu_model: "Apple M3".to_string(),
            logical_cores: 8,
            physical_cores: 8,
            total_memory_bytes: 16 * 1024 * 1024 * 1024,
            resource_control: crate::machine::ResourceControlV1::MacosNative,
            scheduling: crate::machine::SchedulingIdentityV1 {
                nice_level: 0,
                policy: crate::machine::SchedulerPolicyV1::Other,
                priority: 31,
                reset_on_fork: false,
            },
            resource_limits: crate::machine::ResourceLimitIdentityV1 {
                scope_version: crate::machine::RESOURCE_LIMIT_SCOPE_VERSION,
                values_sha256: "c".repeat(64),
            },
            machine_label: format!("hostname-sha256:{}", "0".repeat(64)),
        }
    }

    fn fixture_stamp(run: &ResolvedRun) -> StampedFixtureManifestV1 {
        StampedFixtureManifestV1::stamp(FixtureManifestV1 {
            format_version: FIXTURE_MANIFEST_FORMAT_VERSION,
            logical: LogicalFixtureIdentityV1 {
                builder: run.case.definition.fixture.builder.clone(),
                data: run.case.definition.fixture.data.clone(),
                state: run.case.definition.fixture.state.clone(),
                logical_content_sha256: "1".repeat(64),
            },
            physical: PhysicalFixtureIdentityV1 {
                digest_algorithm: PHYSICAL_TREE_DIGEST_ALGORITHM.to_string(),
                tree_sha256: PHYSICAL_SHA.to_string(),
                files: 12,
                bytes: 4_096,
            },
            validation: FixtureValidationStampV1::verified(INVOCATION_TIME_MS + 1),
        })
        .unwrap()
    }

    fn input(_run: &ResolvedRun, execution: &RunExecution) -> RecordInputV1 {
        RecordInputV1 {
            invocation: InvocationIdentityV1 {
                invocation_id: ulid(INVOCATION_TIME_MS, b'A'),
                session_id: ulid(INVOCATION_TIME_MS - 10, b'B'),
                invoked_at_unix_ms: INVOCATION_TIME_MS,
            },
            sut: sut(),
            backend: ObservedBackendV1::LocalFs {
                filesystem: LocalFilesystem::Apfs,
                storage_class: LocalStorageClass::NvmeSsd,
                storage_protocol: "Apple Fabric".to_string(),
                probe: "macos-df-diskutil-v1".to_string(),
            },
            fixture: execution.fixture.stamp.clone(),
        }
    }

    fn valid_record() -> RunRecordV1 {
        let run = resolved_run();
        let execution = execution(&run);
        build_run_record(&run, &execution, input(&run, &execution)).unwrap()
    }

    /// One fully validated durable record for sibling persistence tests.
    pub(crate) fn valid_record_fixture() -> RunRecordV1 {
        valid_record()
    }

    fn ulid(timestamp_ms: u64, entropy: u8) -> String {
        const ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";
        let mut encoded = [b'0'; 26];
        let mut remaining = timestamp_ms;
        for index in (0..10).rev() {
            encoded[index] = ALPHABET[(remaining % 32) as usize];
            remaining /= 32;
        }
        encoded[10..].fill(entropy);
        String::from_utf8(encoded.to_vec()).unwrap()
    }

    #[test]
    fn construction_binds_every_redundant_identity_and_raw_sample() {
        let record = valid_record();
        assert_eq!(record.format_version, RUN_RECORD_FORMAT_VERSION);
        assert_eq!(record.acquisition.requested_repetitions, 2);
        assert_eq!(record.acquisition.observed_repetitions, 2);
        assert!(record.acquisition.is_complete());
        assert!(
            !record.claim_eligible(),
            "complete local records still lack effective-codegen proof"
        );
        assert_eq!(record.measurements.raw_samples.len(), 2);
        assert_eq!(
            record.measurements.layer_presence.logical.counts,
            MeasurementPresenceV1::Observed
        );
        assert_eq!(
            record.measurements.layer_presence.physical.counts,
            MeasurementPresenceV1::Absent {
                reason:
                    MeasurementAbsenceReasonV1::PhysicalAttemptsNotObservableAtLogicalWrappingSeam
            }
        );
        assert_eq!(
            record.measurements.wall_clock.evidence,
            EvidenceStrengthV1::Directional
        );
        assert_eq!(
            record.measurements.claim_policy.floor_multiplier_millis,
            DEFAULT_FLOOR_MULTIPLIER_MILLIS
        );
        assert_eq!(
            record.fixture.manifest.physical.tree_sha256,
            record.measurements.raw_samples[1].input_physical_digest_sha256
        );
        validate_run_record(&record).unwrap();
    }

    #[test]
    fn censored_record_preserves_only_a_verified_prefix_and_is_never_claimable() {
        let run = resolved_run();
        let mut execution = execution(&run);
        execution.samples.truncate(1);
        execution.wall_clock = WallClockSummary {
            observed_repetitions: 1,
            min_us: 10,
            p50_us: 10,
            max_us: 10,
            p95_us: None,
            p95_supported: false,
        };
        let record = build_censored_run_record(
            &run,
            &execution,
            input(&run, &execution),
            AcquisitionTerminalV1::new(1, AcquisitionTerminalStageV1::Measure, "worker_timeout")
                .unwrap(),
        )
        .unwrap();

        assert_eq!(record.acquisition.status, AcquisitionStatusV1::Censored);
        assert!(!record.claim_eligible());
        assert_eq!(record.acquisition.observed_repetitions, 1);
        assert_eq!(record.measurements.raw_samples.len(), 1);
        validate_run_record(&record).unwrap();

        let mut invalid = record;
        invalid
            .acquisition
            .terminal
            .as_mut()
            .unwrap()
            .failed_repetition = 0;
        assert_eq!(
            validate_run_record(&invalid).unwrap_err().code,
            "invalid_censored_acquisition"
        );
    }

    #[test]
    fn censored_terminal_code_is_a_bounded_canonical_token() {
        for valid in ["worker_timeout", "worker_v2_timeout", "a", "a1"] {
            AcquisitionTerminalV1::new(1, AcquisitionTerminalStageV1::Runner, valid).unwrap();
        }

        for invalid in [
            "",
            "Worker_timeout",
            "worker-timeout",
            "worker__timeout",
            "worker_timeout_",
            "worker timeout",
            "worker_2timeout",
            "wörker_timeout",
        ] {
            assert_eq!(
                AcquisitionTerminalV1::new(1, AcquisitionTerminalStageV1::Runner, invalid,)
                    .unwrap_err()
                    .code,
                "invalid_acquisition_error_code",
                "invalid token {invalid:?} must fail closed",
            );
        }

        assert_eq!(
            AcquisitionTerminalV1::new(
                1,
                AcquisitionTerminalStageV1::Runner,
                "a".repeat(MAX_ACQUISITION_ERROR_CODE_BYTES + 1),
            )
            .unwrap_err()
            .code,
            "invalid_acquisition_error_code",
        );
    }

    #[test]
    fn acquisition_terminal_stage_serialization_is_closed_and_canonical() {
        assert_eq!(
            serde_json::to_string(&AcquisitionTerminalStageV1::StructuredFailureReap).unwrap(),
            r#""structured-failure-reap""#,
        );
        assert!(
            serde_json::from_str::<AcquisitionTerminalStageV1>(r#""arbitrary prose""#).is_err()
        );
    }

    #[test]
    fn durable_samples_require_supervisor_peak_rss() {
        let run = resolved_run();
        let mut execution = execution(&run);
        execution.samples[0].peak_rss_bytes = None;
        assert_eq!(
            build_run_record(&run, &execution, input(&run, &execution))
                .unwrap_err()
                .code,
            "missing_peak_rss"
        );
    }

    #[test]
    fn construction_uses_the_worker_build_not_caller_compile_constants() {
        let run = resolved_run();
        let execution = execution(&run);
        let mut supplied = input(&run, &execution);
        supplied.sut.build.target_triple = "x86_64-unknown-linux-gnu".to_string();

        assert_eq!(
            build_run_record(&run, &execution, supplied)
                .unwrap_err()
                .code,
            "sut_identity_mismatch"
        );

        let mut supplied = input(&run, &execution);
        supplied
            .sut
            .engine
            .feature_flags
            .push("invented".to_string());
        assert_eq!(
            build_run_record(&run, &execution, supplied)
                .unwrap_err()
                .code,
            "sut_identity_mismatch"
        );
    }

    #[test]
    fn admitted_lance_memory_setting_is_part_of_sut_identity() {
        let run = resolved_run();
        let baseline_execution = execution(&run);
        let baseline = sut_identity_for_execution(&baseline_execution).unwrap();
        let mut configured_execution = baseline_execution;
        configured_execution.build.effective_lance_mem_pool_size =
            EffectiveEnvironmentValue::Bytes { bytes: 805_306_368 };
        let configured = sut_identity_for_execution(&configured_execution).unwrap();
        assert_ne!(baseline, configured);

        let mut supplied = input(&run, &configured_execution);
        supplied.sut = configured.clone();
        let record = build_run_record(&run, &configured_execution, supplied).unwrap();
        assert_eq!(record.sut, configured);
    }

    #[test]
    fn canonical_bytes_and_content_digest_are_deterministic_and_round_trip() {
        let record = valid_record();
        let first = canonical_record_bytes(&record).unwrap();
        let second = canonical_record_bytes(&record).unwrap();
        assert_eq!(first, second);
        assert_eq!(parse_canonical_record(&first).unwrap(), record);
        assert_eq!(
            record_content_sha256(&record).unwrap(),
            sha256_bytes(&first)
        );

        let mut non_canonical = first.clone();
        non_canonical.push(b'\n');
        assert_eq!(
            parse_canonical_record(&non_canonical).unwrap_err().code,
            "non_canonical_record"
        );
    }

    #[test]
    fn tampered_point_fixture_summary_and_machine_fail_closed() {
        let mut point = valid_record();
        point.run.point_id = "f".repeat(64);
        assert_eq!(
            validate_run_record(&point).unwrap_err().code,
            "point_id_mismatch"
        );

        let mut fixture = valid_record();
        fixture.fixture.manifest.physical.tree_sha256 = "e".repeat(64);
        assert_eq!(
            validate_run_record(&fixture).unwrap_err().code,
            "fixture_manifest_digest_mismatch"
        );

        let mut summary = valid_record();
        summary.measurements.wall_clock.p50_us += 1;
        assert_eq!(
            validate_run_record(&summary).unwrap_err().code,
            "wall_clock_summary_mismatch"
        );

        let mut branch = valid_record();
        branch.measurements.raw_samples[0].verification.branch = "bench-source".to_string();
        assert_eq!(
            validate_run_record(&branch).unwrap_err().code,
            "invalid_sample_verification"
        );

        let mut rows = valid_record();
        rows.measurements.raw_samples[0].verification.rows += 1;
        assert_eq!(
            validate_run_record(&rows).unwrap_err().code,
            "invalid_sample_verification"
        );

        let mut machine = valid_record();
        machine.machine.cpu_model = "unknown".to_string();
        assert_eq!(
            validate_run_record(&machine).unwrap_err().code,
            "invalid_machine_identity"
        );
    }

    #[test]
    fn invocation_ulid_timestamp_and_sorted_configuration_are_enforced() {
        let mut record = valid_record();
        record.invocation.invoked_at_unix_ms += 1;
        assert_eq!(
            validate_run_record(&record).unwrap_err().code,
            "invocation_timestamp_mismatch"
        );

        let mut record = valid_record();
        record.sut.engine.feature_flags = vec!["z".to_string(), "a".to_string()];
        assert_eq!(
            validate_run_record(&record).unwrap_err().code,
            "non_canonical_string_set"
        );

        let mut record = valid_record();
        record.sut.source_tree_dirty = true;
        assert_eq!(
            validate_run_record(&record).unwrap_err().code,
            "dirty_source_tree"
        );

        let mut record = valid_record();
        record.sut.build.cargo_encoded_rustflags_present = true;
        assert_eq!(
            validate_run_record(&record).unwrap_err().code,
            "non_release_timing_record"
        );

        let mut record = valid_record();
        record.sut.build.effective_codegen_options_proved = true;
        assert_eq!(
            validate_run_record(&record).unwrap_err().code,
            "non_release_timing_record"
        );

        let mut record = valid_record();
        record.measurements.layer_presence.physical.counts = MeasurementPresenceV1::Observed;
        assert_eq!(
            validate_run_record(&record).unwrap_err().code,
            "invalid_measurement_presence"
        );

        let mut record = valid_record();
        record.measurements.claim_policy.floor_multiplier_millis = 1_999;
        assert_eq!(
            validate_run_record(&record).unwrap_err().code,
            "invalid_claim_policy"
        );
    }

    #[test]
    fn projected_logical_call_algebra_is_part_of_record_validity() {
        let mut inconsistent = valid_record();
        inconsistent.measurements.raw_samples[0]
            .control_store_calls
            .delete = 1;
        assert_eq!(
            validate_run_record(&inconsistent).unwrap_err().code,
            "control_call_count_inconsistent",
            "write_text and delete are subcategories of mutation_calls"
        );

        let mut lance_overflow = valid_record();
        lance_overflow.measurements.raw_samples[0]
            .logical_store_calls
            .manifest
            .get = u64::MAX;
        lance_overflow.measurements.raw_samples[0]
            .logical_store_calls
            .manifest
            .put = 1;
        assert_eq!(
            validate_run_record(&lance_overflow).unwrap_err().code,
            "logical_call_count_overflow"
        );

        let mut control_overflow = valid_record();
        control_overflow.measurements.raw_samples[0]
            .control_store_calls
            .read_text = u64::MAX;
        assert_eq!(
            validate_run_record(&control_overflow).unwrap_err().code,
            "logical_call_count_overflow"
        );
    }

    #[test]
    fn sut_identity_budget_keeps_every_valid_record_projectable() {
        let mut record = valid_record();
        record.sut.engine.feature_flags = (0..12)
            .map(|index| format!("feature-{index:04}-{}", "x".repeat(980)))
            .collect();
        assert!(serde_json::to_vec(&record.sut).unwrap().len() > MAX_SUT_IDENTITY_BYTES);
        assert_eq!(
            validate_run_record(&record).unwrap_err().code,
            "sut_identity_too_large"
        );
    }

    #[test]
    fn compatible_s3_image_digest_uses_the_case_contract() {
        let digest = format!("sha256:{}", "d".repeat(64));
        let declared = Backend::S3 {
            implementation: S3Implementation::Minio,
            implementation_version: "RELEASE.2026-08-26".to_string(),
            region: "local".to_string(),
            storage_class: "erasure-coded".to_string(),
            versioning: S3Versioning::Enabled,
            image_digest: Some(digest.clone()),
        };
        let observed = ObservedBackendV1::S3 {
            implementation: S3Implementation::Minio,
            implementation_version: "RELEASE.2026-08-26".to_string(),
            region: "local".to_string(),
            storage_class: "erasure-coded".to_string(),
            versioning: S3Versioning::Enabled,
            image_digest: Some(digest),
            probe: "minio-admin-info-v1".to_string(),
        };

        validate_backend(&declared, &observed).unwrap();
    }

    #[tokio::test]
    async fn maximum_escaped_identities_publish_and_rebuild_the_projection() {
        let mut record = valid_record();
        let escaped_fact = "\\".repeat(MAX_TEXT_BYTES);

        // The current runner-v1 accepts only local storage. Maximize its two
        // free-form observed facts; their JSON is escaped again in RunRow.
        let definition = CaseV1 {
            version: CASE_FORMAT_VERSION,
            id: "a".repeat(128),
            scenario: record.run.run_spec.scenario,
            fixture: record.run.run_spec.fixture.clone(),
            workload: record.run.run_spec.workload.clone(),
            environment: record.run.run_spec.environment.clone(),
            protocol: record.run.run_spec.protocol.clone(),
        };
        let sealed = validate_case(definition).into_result().unwrap();
        record.run.point_id.clone_from(&sealed.point_id);
        record.run.point_name.clone_from(&sealed.point_name);
        record.run.case_id.clone_from(&sealed.definition.id);
        record.run.case_digest.clone_from(&sealed.case_digest);
        record.run.run_spec.clone_from(&sealed.identity);
        record.backend = ObservedBackendV1::LocalFs {
            filesystem: LocalFilesystem::Apfs,
            storage_class: LocalStorageClass::NvmeSsd,
            storage_protocol: escaped_fact.clone(),
            probe: escaped_fact.clone(),
        };

        // Fill every repeated SUT string first, then use a feature flag to
        // approach the aggregate cap. Backslashes are the worst JSON case:
        // serde escapes them once in `sut_json` and again in the RunRow.
        record.sut.package_version = escaped_fact.clone();
        record.sut.source_commit = "a".repeat(64);
        record.sut.build.target_triple = escaped_fact.clone();
        record.sut.build.rustc_version = escaped_fact.clone();
        let mut feature = "f".to_string();
        loop {
            let candidate = format!("{feature}\\");
            if candidate.len() > MAX_TEXT_BYTES {
                break;
            }
            record.sut.engine.feature_flags = vec![candidate.clone()];
            if serde_json::to_vec(&record.sut).unwrap().len() > MAX_SUT_IDENTITY_BYTES {
                break;
            }
            feature = candidate;
        }
        record.sut.engine.feature_flags = vec![feature];
        let sut_bytes = serde_json::to_vec(&record.sut).unwrap().len();
        assert!(sut_bytes <= MAX_SUT_IDENTITY_BYTES);
        assert!(
            sut_bytes >= MAX_SUT_IDENTITY_BYTES - 2,
            "adversarial SUT should reach the byte boundary, got {sut_bytes}"
        );

        // Machine facts are projected individually. Maximize every free-form
        // fact that remains semantically valid for the macOS identity shape.
        record.machine.os_version = escaped_fact.clone();
        record.machine.kernel_version = escaped_fact.clone();
        record.machine.architecture = escaped_fact.clone();
        record.machine.cpu_model = escaped_fact;
        record.machine.total_memory_bytes = u64::MAX;

        // Maximize the scalar widths which survive into RunRow summaries. The
        // full sample vector is intentionally not projected.
        for sample in &mut record.measurements.raw_samples {
            sample.elapsed_us = u64::MAX;
            sample.logical_store_calls = LogicalStoreCallsV1 {
                manifest: LogicalCallCounts {
                    get: u64::MAX,
                    ..Default::default()
                },
                table: LogicalCallCounts::default(),
            };
            sample.control_store_calls = ControlCallObservation {
                read_text: u64::MAX,
                read_text_if_exists: 0,
                read_text_versioned: 0,
                exists: 0,
                list_dir: 0,
                mutation_calls: 0,
                write_text: 0,
                delete: 0,
            };
        }
        record.measurements.wall_clock =
            summarize_wall_clock(&record.measurements.raw_samples).unwrap();

        validate_run_record(&record).unwrap();
        let archive = tempfile::tempdir().unwrap();
        crate::archive::publish_record(archive.path(), &record).unwrap();
        let projection_parent = tempfile::tempdir().unwrap();
        let projection = projection_parent.path().join("projection");
        let built = crate::projection::rebuild_projection(archive.path(), &projection)
            .await
            .unwrap();
        assert_eq!(built.record_count, 1);
        assert_eq!(built.point_count, 1);
    }

    #[test]
    fn unknown_json_fields_are_not_accepted_as_a_second_record_shape() {
        let record = valid_record();
        let canonical = canonical_record_bytes(&record).unwrap();
        let mut value: serde_json::Value = serde_json::from_slice(&canonical).unwrap();
        value["unexpected"] = serde_json::json!(true);
        let bytes = serde_json::to_vec(&value).unwrap();
        assert_eq!(
            parse_canonical_record(&bytes).unwrap_err().code,
            "record_decode_failed"
        );

        let mut value: serde_json::Value = serde_json::from_slice(&canonical).unwrap();
        value["measurements"]["raw_samples"][0]["route"]["unexpected"] = serde_json::json!(true);
        let bytes = serde_json::to_vec(&value).unwrap();
        assert_eq!(
            parse_canonical_record(&bytes).unwrap_err().code,
            "non_canonical_record"
        );
    }

    #[test]
    fn twenty_samples_upgrade_tail_and_summary_evidence_together() {
        let mut record = valid_record();
        for repetition in 2..20 {
            let mut sample = record.measurements.raw_samples[0].clone();
            sample.repetition = repetition;
            sample.elapsed_us = u64::from(repetition) + 10;
            record.measurements.raw_samples.push(sample);
        }
        record.acquisition.requested_repetitions = 20;
        record.acquisition.observed_repetitions = 20;
        record.measurements.wall_clock =
            summarize_wall_clock(&record.measurements.raw_samples).unwrap();

        validate_run_record(&record).unwrap();
        assert!(record.measurements.wall_clock.p95_supported);
        assert!(record.measurements.wall_clock.p95_us.is_some());
        assert_eq!(
            record.measurements.wall_clock.evidence,
            EvidenceStrengthV1::DistributionSupported
        );
    }
}
