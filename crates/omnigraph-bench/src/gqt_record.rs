//! Scenario-specific GQT authority records; legacy serializers stay unchanged.
use crate::case::Backend;
use crate::dataset_cache::{DatasetManifestV1, validate_manifest_evidence};
use crate::gqt_case::{GqtPointIdentityV1, MAX_EXPANDED_STEPS, Target, validate_point_spec};
use crate::gqt_evidence::PreparationProofV2;
use crate::gqt_runner::{
    GqtRepObservation, RunExecution, sample_evidence_matches, validate_merge_evidence,
    validate_receipt_treatment, validate_sample,
};
use crate::gqt_served::ServerDeploymentReceiptV1;
use crate::machine::{MachineIdentityV1, validate_machine_identity};
use crate::model::{typed_sha256, valid_kebab_id};
use crate::record::*;
use omnigraph_gqt_core::{Item, Step, StepKind, parse_case};
use serde::{Deserialize, Serialize};

const MAX_SERVED_SUT_BYTES: usize = 8 * 1024;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GqtRunIdentityV1 {
    pub point_identity_version: u32,
    pub point_id: String,
    pub point_name: String,
    pub case_id: String,
    pub case_digest: String,
    pub run_spec: GqtPointIdentityV1,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GqtMeasurementsV1 {
    pub wall_clock: WallClockSummaryV1,
    pub raw_samples: Vec<GqtRepObservation>,
    pub layer_presence: MeasurementLayerPresenceV1,
    pub claim_policy: ClaimPolicyV1,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GqtRunRecordV1 {
    pub format_version: u32,
    pub invocation: InvocationIdentityV1,
    pub run: GqtRunIdentityV1,
    pub sut: GqtSutIdentityV1,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub machine: Option<MachineIdentityV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend: Option<ObservedBackendV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fixture: Option<DatasetManifestV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dataset_cache_hit: Option<bool>,
    pub acquisition: AcquisitionV1,
    pub measurements: GqtMeasurementsV1,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum GqtSutIdentityV1 {
    Embedded(Box<SutIdentityV1>),
    Served(Box<ServedSutIdentityV1>),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServedSutIdentityV1 {
    pub kind: ServedSutKind,
    pub receipt: ServerDeploymentReceiptV1,
    pub client_build: SutIdentityV1,
    pub client_machine: MachineIdentityV1,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ServedSutKind {
    #[serde(rename = "declared-deployment")]
    DeclaredDeployment,
}
impl ServedSutIdentityV1 {
    pub(crate) fn validate(&self) -> RecordResult<()> {
        self.receipt.validate().map_err(error)?;
        validate_sut(&self.client_build)?;
        validate_machine_identity(&self.client_machine).map_err(error)?;
        self.validate_size()
    }

    pub(crate) fn validate_size(&self) -> RecordResult<()> {
        if serde_json::to_vec(self).map_err(error)?.len() > MAX_SERVED_SUT_BYTES {
            return Err(error("served SUT evidence exceeds 8 KiB"));
        }
        Ok(())
    }
}
/// The target-specific evidence of one record, borrowed after its tuple shape is checked.
pub(crate) enum GqtEvidence<'a> {
    Embedded(EmbeddedEvidence<'a>),
    Served(&'a ServedSutIdentityV1),
}
pub(crate) struct EmbeddedEvidence<'a> {
    pub(crate) sut: &'a SutIdentityV1,
    pub(crate) machine: &'a MachineIdentityV1,
    pub(crate) backend: &'a ObservedBackendV1,
    pub(crate) fixture: &'a DatasetManifestV1,
}
impl GqtRunRecordV1 {
    pub fn invocation(&self) -> &InvocationIdentityV1 {
        &self.invocation
    }
    pub fn point_id(&self) -> &str {
        &self.run.point_id
    }
    pub fn claim_eligible(&self) -> bool {
        self.acquisition.is_complete()
            && matches!(&self.sut, GqtSutIdentityV1::Embedded(sut) if sut.build.effective_codegen_options_proved)
    }
    pub(crate) fn evidence(&self) -> RecordResult<GqtEvidence<'_>> {
        match (
            &self.sut,
            self.run.run_spec.environment.target,
            &self.machine,
            &self.backend,
            &self.fixture,
            self.dataset_cache_hit,
        ) {
            (
                GqtSutIdentityV1::Embedded(sut),
                Target::Engine,
                Some(machine),
                Some(backend),
                Some(fixture),
                Some(_),
            ) => Ok(GqtEvidence::Embedded(EmbeddedEvidence {
                sut,
                machine,
                backend,
                fixture,
            })),
            (GqtSutIdentityV1::Served(sut), Target::Server, None, None, None, None) => {
                Ok(GqtEvidence::Served(sut))
            }
            _ => Err(error("target and evidence tuple disagree")),
        }
    }
}
fn error(message: impl std::fmt::Display) -> RecordError {
    RecordError::new("invalid_gqt_record", "$", message.to_string())
}
pub fn build(
    execution: &RunExecution,
    invocation: InvocationIdentityV1,
    terminal: Option<AcquisitionTerminalV1>,
) -> RecordResult<GqtRunRecordV1> {
    execution.bound.revalidate().map_err(error)?;
    let (sut, machine, backend) = match (
        &execution.server_receipt,
        &execution.environment,
        &execution.fixture,
        execution.dataset_cache_hit,
    ) {
        (None, Some(environment), Some(_), Some(_)) => {
            let Backend::LocalFs {
                filesystem,
                storage_class,
            } = execution.bound.identity.environment.backend
            else {
                return Err(error("unsupported backend"));
            };
            (
                GqtSutIdentityV1::Embedded(Box::new(sut_identity_for_build(&execution.build)?)),
                Some(execution.machine.clone()),
                Some(ObservedBackendV1::LocalFs {
                    filesystem,
                    storage_class,
                    storage_protocol: environment.storage_protocol.clone(),
                    probe: environment.probe.into(),
                }),
            )
        }
        (Some(receipt), None, None, None) => {
            if receipt.bind(&execution.bound.plan).map_err(error)? != execution.bound {
                return Err(error("server receipt binding mismatch"));
            }
            (
                GqtSutIdentityV1::Served(Box::new(ServedSutIdentityV1 {
                    kind: ServedSutKind::DeclaredDeployment,
                    receipt: receipt.clone(),
                    client_build: sut_identity_for_build(&execution.build)?,
                    client_machine: execution.machine.clone(),
                })),
                None,
                None,
            )
        }
        _ => return Err(error("inconsistent execution evidence")),
    };
    let record = GqtRunRecordV1 {
        format_version: 1,
        invocation,
        run: GqtRunIdentityV1 {
            point_identity_version: 1,
            point_id: execution.point_id.clone(),
            point_name: execution.point_name.clone(),
            case_id: execution.case_id.clone(),
            case_digest: execution.bound.plan.case_digest.clone(),
            run_spec: execution.bound.identity.clone(),
        },
        sut,
        machine,
        backend,
        fixture: execution.fixture.clone(),
        dataset_cache_hit: execution.dataset_cache_hit,
        acquisition: AcquisitionV1 {
            status: if terminal.is_some() {
                AcquisitionStatusV1::Censored
            } else {
                AcquisitionStatusV1::Complete
            },
            requested_repetitions: execution.requested_repetitions,
            observed_repetitions: execution.samples.len() as u32,
            terminal,
        },
        measurements: GqtMeasurementsV1 {
            wall_clock: summarize(&execution.samples)?,
            raw_samples: execution.samples.clone(),
            layer_presence: layer_presence(execution.bound.identity.environment.target),
            claim_policy: ClaimPolicyV1 {
                floor_multiplier_millis: DEFAULT_FLOOR_MULTIPLIER_MILLIS,
            },
        },
    };
    let proof = validate_with_proof(&record)?;
    for (index, sample) in execution.samples.iter().enumerate() {
        validate_sample(
            sample,
            &execution.bound,
            (index + 1) as u32,
            &proof,
            sample.elapsed_us,
            true,
        )
        .map_err(error)?;
    }
    Ok(record)
}
fn summarize(samples: &[GqtRepObservation]) -> RecordResult<WallClockSummaryV1> {
    if samples.is_empty() {
        return Err(error("empty acquisition"));
    }
    let mut times: Vec<_> = samples.iter().map(|s| s.elapsed_us).collect();
    times.sort_unstable();
    let n = times.len();
    let supported = n >= 20;
    Ok(WallClockSummaryV1 {
        min_us: times[0],
        p50_us: times[(n - 1) / 2],
        max_us: times[n - 1],
        p95_us: supported.then(|| times[(n * 95).div_ceil(100) - 1]),
        p95_supported: supported,
        evidence: if supported {
            EvidenceStrengthV1::DistributionSupported
        } else {
            EvidenceStrengthV1::Directional
        },
    })
}
pub fn validate(r: &GqtRunRecordV1) -> RecordResult<()> {
    validate_with_proof(r).map(drop)
}
fn validate_with_proof(r: &GqtRunRecordV1) -> RecordResult<PreparationProofV2> {
    validate_invocation(&r.invocation)?;
    let proof = validate_target_evidence(r)?;
    let point = typed_sha256(&r.run.run_spec).map_err(error)?;
    let spec = &r.run.run_spec;
    validate_point_spec(spec).map_err(error)?;
    if r.format_version != 1
        || r.run.point_identity_version != 1
        || spec.identity_version != 1
        || point != r.run.point_id
        || r.run.point_name
            != format!(
                "gqt-{}-{}",
                spec.cache_condition.display_label(),
                &point[..12]
            )
    {
        return Err(error("point binding mismatch"));
    }
    for digest in [
        &r.run.case_digest,
        &spec.queries_sha256,
        &spec.dataset_recipe_sha256,
        &spec.dataset_logical_digest,
    ] {
        validate_sha256(digest, "digest")?;
    }
    if !valid_kebab_id(&r.run.case_id)
        || r.run.case_id.len() > 128
        || spec.measured_step.ordinal == 0
        || spec.measured_step.text.trim().is_empty()
    {
        return Err(error("invalid authored identity"));
    }
    let n = r.measurements.raw_samples.len();
    if n == 0
        || n > 10_000
        || r.acquisition.observed_repetitions as usize != n
        || r.acquisition.requested_repetitions == 0
        || r.acquisition.requested_repetitions > 10_000
    {
        return Err(error("invalid acquisition sample count"));
    }
    match (&r.acquisition.status, &r.acquisition.terminal) {
        (AcquisitionStatusV1::Complete, None)
            if n == r.acquisition.requested_repetitions as usize => {}
        (AcquisitionStatusV1::Censored, Some(t))
            if n < r.acquisition.requested_repetitions as usize
                && t.failed_repetition == n as u32 =>
        {
            validate_acquisition_error_code(&t.code, "terminal.code")?
        }
        _ => return Err(error("invalid acquisition terminal")),
    }
    if r.measurements.wall_clock != summarize(&r.measurements.raw_samples)?
        || r.measurements.layer_presence != layer_presence(spec.environment.target)
        || r.measurements.claim_policy.floor_multiplier_millis != DEFAULT_FLOOR_MULTIPLIER_MILLIS
    {
        return Err(error("measurement summary or presence mismatch"));
    }
    for (i, s) in r.measurements.raw_samples.iter().enumerate() {
        if let (Some(logical), Some(control)) = (&s.logical_store_calls, &s.control_store_calls) {
            validate_call_totals(i, logical.manifest, logical.table, control)?;
        }
        let selected_kind = selected_kind(&spec.measured_step.text)?;
        validate_merge_evidence(s.merge.as_ref(), selected_kind, spec.protocol.attribution)
            .map_err(error)?;
        validate_receipt_treatment(&s.steps, spec.measured_step.ordinal, &spec.cache_condition)
            .map_err(error)?;
        let selected: Vec<_> = s
            .steps
            .iter()
            .filter(|step| step.ordinal == spec.measured_step.ordinal)
            .collect();
        if s.repetition != i as u32 + 1
            || !sample_evidence_matches(s, &proof, true)
            || s.outcome != "expectations-passed"
            || selected.len() != 1
            || selected[0].occurrence != 1
            || selected[0].kind != selected_kind.into()
            || selected[0].elapsed_us != s.elapsed_us
            || s.steps.len() > MAX_EXPANDED_STEPS
            || !s.verification.selected_assertion_passed
            || s.verification.following_assertions == 0
            || s.verification.assertions_passed < 2
            || s.verification.assertions_passed as usize > MAX_EXPANDED_STEPS
            || s.verification.following_assertions >= s.verification.assertions_passed
        {
            return Err(error("invalid GQT sample"));
        }
        let mut seen = std::collections::BTreeMap::new();
        for step in &s.steps {
            let occurrence = seen.entry(step.ordinal).or_insert(0);
            *occurrence += 1;
            if step.ordinal == 0 || *occurrence != step.occurrence {
                return Err(error("invalid operation receipt sequence"));
            }
        }
    }
    Ok(proof)
}
fn validate_target_evidence(r: &GqtRunRecordV1) -> RecordResult<PreparationProofV2> {
    let spec = &r.run.run_spec;
    match r.evidence()? {
        GqtEvidence::Embedded(EmbeddedEvidence {
            sut,
            machine,
            backend,
            fixture,
        }) => {
            validate_sut(sut)?;
            validate_machine_identity(machine).map_err(error)?;
            validate_backend(&spec.environment.backend, backend)?;
            validate_manifest_evidence(fixture).map_err(error)?;
            if fixture.recipe_sha256 != spec.dataset_recipe_sha256
                || fixture.handoff.summary.logical_content_sha256 != spec.dataset_logical_digest
                || fixture.handoff.summary.algorithm != spec.dataset_identity_algorithm
                || fixture.reset != spec.protocol.reset
            {
                return Err(error("dataset binding mismatch"));
            }
            Ok(PreparationProofV2::Embedded {
                physical_digest: fixture.handoff.physical.clone(),
                metadata_digest: fixture.handoff.template_metadata.clone(),
            })
        }
        GqtEvidence::Served(sut) => {
            let receipt = &sut.receipt;
            sut.validate()?;
            if receipt.backend != spec.environment.backend
                || receipt.dataset.recipe_sha256 != spec.dataset_recipe_sha256
                || receipt.dataset.logical_content_sha256 != spec.dataset_logical_digest
                || receipt.dataset.algorithm != spec.dataset_identity_algorithm
            {
                return Err(error("declared server dataset binding mismatch"));
            }
            Ok(PreparationProofV2::Served {
                server_receipt_sha256: receipt.digest().map_err(error)?,
            })
        }
    }
}

pub(crate) fn layer_presence(target: Target) -> MeasurementLayerPresenceV1 {
    let mut presence = v1_layer_presence();
    if target == Target::Server {
        let absent = MeasurementPresenceV1::Absent {
            reason: MeasurementAbsenceReasonV1::ServerCountersNotExposed,
        };
        presence.logical.counts = absent;
        presence.logical.request_timing = absent;
        presence.physical.counts = absent;
        presence.physical.request_timing = absent;
        presence.physical.concurrency_witness = absent;
    }
    presence
}

/// Closed archive dispatch keeps the exact historical field ordering intact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum AnyRunRecordV1 {
    Legacy(Box<RunRecordV1>),
    Gqt(Box<GqtRunRecordV1>),
}
impl AnyRunRecordV1 {
    pub fn invocation(&self) -> &InvocationIdentityV1 {
        match self {
            Self::Legacy(r) => &r.invocation,
            Self::Gqt(r) => &r.invocation,
        }
    }
    pub fn point_id(&self) -> &str {
        match self {
            Self::Legacy(r) => &r.run.point_id,
            Self::Gqt(r) => &r.run.point_id,
        }
    }
    pub fn validate(&self) -> RecordResult<()> {
        match self {
            Self::Legacy(r) => validate_run_record(r),
            Self::Gqt(r) => validate(r),
        }
    }
}
pub trait AuthorityRecord: Serialize {
    fn invocation(&self) -> &InvocationIdentityV1;
    fn validate_authority(&self) -> RecordResult<()>;
}
impl AuthorityRecord for RunRecordV1 {
    fn invocation(&self) -> &InvocationIdentityV1 {
        &self.invocation
    }
    fn validate_authority(&self) -> RecordResult<()> {
        validate_run_record(self)
    }
}
impl AuthorityRecord for GqtRunRecordV1 {
    fn invocation(&self) -> &InvocationIdentityV1 {
        &self.invocation
    }
    fn validate_authority(&self) -> RecordResult<()> {
        validate(self)
    }
}
impl AuthorityRecord for AnyRunRecordV1 {
    fn invocation(&self) -> &InvocationIdentityV1 {
        self.invocation()
    }
    fn validate_authority(&self) -> RecordResult<()> {
        self.validate()
    }
}
pub fn canonical_bytes<R: AuthorityRecord>(record: &R) -> RecordResult<Vec<u8>> {
    record.validate_authority()?;
    let bytes = serde_json::to_vec(record).map_err(error)?;
    if bytes.len() > MAX_RECORD_BYTES {
        return Err(error("record exceeds byte budget"));
    }
    Ok(bytes)
}
pub fn parse(bytes: &[u8]) -> RecordResult<AnyRunRecordV1> {
    if bytes.len() > MAX_RECORD_BYTES {
        return Err(error("record exceeds byte budget"));
    }
    let record: AnyRunRecordV1 = serde_json::from_slice(bytes).map_err(error)?;
    if canonical_bytes(&record)? != bytes {
        return Err(error("record is not canonical"));
    }
    Ok(record)
}
impl From<RunRecordV1> for AnyRunRecordV1 {
    fn from(r: RunRecordV1) -> Self {
        Self::Legacy(Box::new(r))
    }
}
impl From<GqtRunRecordV1> for AnyRunRecordV1 {
    fn from(r: GqtRunRecordV1) -> Self {
        Self::Gqt(Box::new(r))
    }
}

fn selected_kind(text: &str) -> RecordResult<StepKind> {
    let suffix = if text.trim() == "--- restart" {
        ""
    } else {
        "\n--- expect error: descriptor-only validation\n"
    };
    let input = format!(
        "# issue: none\n# notes: Durable selected operation syntax.\n--- runner\ntimeout_ms: 10000\nenvironments:\n  - target: omnigraph-engine\n    storage: local-filesystem\n{text}{suffix}"
    );
    let case = parse_case("record_selected_operation", &input).map_err(error)?;
    let descriptors = case.steps();
    let [selected] = descriptors.as_slice() else {
        return Err(error("selected echo must contain one operation"));
    };
    if selected.source != text.trim()
        || selected.in_loop
        || matches!(
            selected.kind,
            StepKind::Show | StepKind::Settings | StepKind::Concurrent
        )
    {
        return Err(error("selected echo is not an admitted engine operation"));
    }
    if let Some(Item::Step(Step::Load(load))) = case.items.first() {
        if load.call_count() != 1 {
            return Err(error("selected load must make one engine call"));
        }
    }
    Ok(selected.kind)
}
