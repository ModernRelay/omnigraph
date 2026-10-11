//! Scenario-specific GQT authority records; legacy serializers stay unchanged.
use crate::gqt_case::GqtPointIdentityV1;
use crate::gqt_runner::{GqtRepObservation, RunExecution};
use crate::record::*;
use serde::{Deserialize, Serialize};
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
    pub sut: SutIdentityV1,
    pub machine: crate::machine::MachineIdentityV1,
    pub backend: ObservedBackendV1,
    pub fixture: crate::dataset_cache::DatasetManifestV1,
    pub dataset_cache_hit: bool,
    pub acquisition: AcquisitionV1,
    pub measurements: GqtMeasurementsV1,
}
impl GqtRunRecordV1 {
    pub fn invocation(&self) -> &InvocationIdentityV1 {
        &self.invocation
    }
    pub fn point_id(&self) -> &str {
        &self.run.point_id
    }
    pub fn claim_eligible(&self) -> bool {
        self.acquisition.is_complete() && self.sut.build.effective_codegen_options_proved
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
    for sample in &execution.samples {
        crate::gqt_runner::validate_sample(
            sample,
            &execution.bound,
            sample.repetition,
            &execution.fixture.handoff.physical,
            sample.elapsed_us,
            true,
        )
        .map_err(error)?;
    }
    let crate::case::Backend::LocalFs {
        filesystem,
        storage_class,
    } = execution.bound.identity.environment.backend
    else {
        return Err(error("unsupported backend"));
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
        sut: sut_identity_for_build(&execution.build)?,
        machine: execution.machine.clone(),
        backend: ObservedBackendV1::LocalFs {
            filesystem,
            storage_class,
            storage_protocol: execution.environment.storage_protocol.clone(),
            probe: execution.environment.probe.into(),
        },
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
            layer_presence: v1_layer_presence(),
            claim_policy: ClaimPolicyV1 {
                floor_multiplier_millis: DEFAULT_FLOOR_MULTIPLIER_MILLIS,
            },
        },
    };
    validate(&record)?;
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
    crate::machine::validate_machine_identity(&r.machine).map_err(error)?;
    validate_invocation(&r.invocation)?;
    validate_sut(&r.sut)?;
    validate_backend(&r.run.run_spec.environment.backend, &r.backend)?;
    let point = crate::model::typed_sha256(&r.run.run_spec).map_err(error)?;
    let spec = &r.run.run_spec;
    crate::gqt_case::validate_point_spec(spec).map_err(error)?;
    crate::dataset_cache::validate_manifest_evidence(&r.fixture).map_err(error)?;
    let fixture = &r.fixture;
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
        || fixture.format_version != 1
        || fixture.recipe_sha256 != spec.dataset_recipe_sha256
        || fixture.handoff.summary.logical_content_sha256 != spec.dataset_logical_digest
        || fixture.handoff.summary.algorithm != spec.dataset_identity_algorithm
        || fixture.reset != spec.protocol.reset
    {
        return Err(error("point or dataset binding mismatch"));
    }
    for digest in [
        &r.run.case_digest,
        &fixture.engine_digest,
        &fixture.key,
        &fixture.handoff.physical.digest_sha256,
        &spec.queries_sha256,
        &spec.dataset_recipe_sha256,
        &spec.dataset_logical_digest,
    ] {
        validate_sha256(digest, "digest")?;
    }
    if !crate::model::valid_kebab_id(&r.run.case_id)
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
        || r.measurements.layer_presence != v1_layer_presence()
        || r.measurements.claim_policy.floor_multiplier_millis != DEFAULT_FLOOR_MULTIPLIER_MILLIS
    {
        return Err(error("measurement summary or presence mismatch"));
    }
    for (i, s) in r.measurements.raw_samples.iter().enumerate() {
        validate_call_totals(
            i,
            s.logical_store_calls.manifest,
            s.logical_store_calls.table,
            &s.control_store_calls,
        )?;
        let selected_kind = selected_kind(&spec.measured_step.text)?;
        crate::gqt_runner::validate_merge_evidence(
            s.merge.as_ref(),
            selected_kind,
            spec.protocol.attribution,
        )
        .map_err(error)?;
        crate::gqt_runner::validate_receipt_treatment(
            &s.steps,
            spec.measured_step.ordinal,
            &spec.cache_condition,
        )
        .map_err(error)?;
        let selected: Vec<_> = s
            .steps
            .iter()
            .filter(|step| step.ordinal == spec.measured_step.ordinal)
            .collect();
        if s.repetition != i as u32 + 1
            || s.input_physical_digest_sha256 != fixture.handoff.physical.digest_sha256
            || s.peak_rss_bytes.is_none_or(|n| n == 0)
            || s.outcome != "expectations-passed"
            || s.logical_store_calls.physical_attempts_observed
            || selected.len() != 1
            || selected[0].occurrence != 1
            || selected[0].kind != selected_kind.into()
            || selected[0].elapsed_us != s.elapsed_us
            || s.steps.len() > crate::gqt_case::MAX_EXPANDED_STEPS
            || !s.verification.selected_assertion_passed
            || s.verification.following_assertions == 0
            || s.verification.assertions_passed < 2
            || s.verification.assertions_passed as usize > crate::gqt_case::MAX_EXPANDED_STEPS
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
    Ok(())
}
/// Closed archive dispatch keeps the exact historical field ordering intact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum AnyRunRecordV1 {
    Legacy(RunRecordV1),
    Gqt(GqtRunRecordV1),
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
        Self::Legacy(r)
    }
}
impl From<GqtRunRecordV1> for AnyRunRecordV1 {
    fn from(r: GqtRunRecordV1) -> Self {
        Self::Gqt(r)
    }
}

fn selected_kind(text: &str) -> RecordResult<omnigraph_gqt_core::StepKind> {
    use omnigraph_gqt_core::{Item, Step, StepKind};
    let suffix = if text.trim() == "--- restart" {
        ""
    } else {
        "\n--- expect error: descriptor-only validation\n"
    };
    let input = format!(
        "# issue: none\n# notes: Durable selected operation syntax.\n--- runner\ntimeout_ms: 10000\nenvironments:\n  - target: omnigraph-engine\n    storage: local-filesystem\n{text}{suffix}"
    );
    let case =
        omnigraph_gqt_core::parse_case("record_selected_operation", &input).map_err(error)?;
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
