//! One engine operation selected from a frozen GQT program. Dataset building,
//! process supervision, result assertions and archive work are outside its clock.
use crate::case::Backend;
use crate::counting::LogicalCallCounter;
use crate::dataset_cache::DatasetManifestV1;
use crate::gqt_case::{
    BoundGqt, DatasetBuildPlan, PlannedGqt, SERVED_READ_KINDS, Target, engine_preparation,
};
use crate::gqt_evidence::{PreparationProofV2, RepetitionInputV2};
use crate::gqt_served::{ServedInput, ServerDeploymentReceiptV1};
use crate::preparation::{PreparationWriteGate, guard_preparation_writes};
use crate::reset::{MetadataDigest, PhysicalDigest, TraversalLimits, verify_metadata_shape};
use crate::runner::{
    BuildEvidence, ControlCallObservation, ControlSnapshot, LogicalStoreCallObservation,
    MeasurementSignals, RunnerError, RunnerResult, WallClockSummary,
};
use futures::{FutureExt, future::BoxFuture};
use lance::io::WrappingObjectStore;
use omnigraph::db::Omnigraph;
use omnigraph::error::OmniError;
use omnigraph::instrumentation::{
    CountingStorageAdapter, MergeWriteProbes, QueryIoProbes, StorageReadCounts,
    with_merge_write_probes, with_query_io_probes,
};
use omnigraph::storage::StorageAdapter;
use omnigraph_compiler::settings::Engine;
use omnigraph_gqt_core::runner_config::SeamDirective;
use omnigraph_gqt_core::{ExecutionHost, Step, StepKind, case_session, execute_steps};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GqtOperationKind {
    Query,
    Mutate,
    Load,
    BranchCreate,
    BranchDelete,
    BranchMerge,
    BranchList,
    Settings,
    Show,
    Restart,
    Concurrent,
}
impl From<StepKind> for GqtOperationKind {
    fn from(kind: StepKind) -> Self {
        match kind {
            StepKind::Query => Self::Query,
            StepKind::Mutate => Self::Mutate,
            StepKind::Load => Self::Load,
            StepKind::BranchCreate => Self::BranchCreate,
            StepKind::BranchDelete => Self::BranchDelete,
            StepKind::BranchMerge => Self::BranchMerge,
            StepKind::BranchList => Self::BranchList,
            StepKind::Settings => Self::Settings,
            StepKind::Show => Self::Show,
            StepKind::Restart => Self::Restart,
            StepKind::Concurrent => Self::Concurrent,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GqtStepObservation {
    pub ordinal: usize,
    pub occurrence: u32,
    pub kind: GqtOperationKind,
    pub elapsed_us: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GqtVerification {
    pub selected_assertion_passed: bool,
    pub assertions_passed: u32,
    pub following_assertions: u32,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GqtMergeEvidence {
    pub phases: Vec<crate::runner::PhaseObservation>,
    pub route: crate::runner::MergeRouteObservation,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GqtRepObservation {
    pub repetition: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_physical_digest_sha256: Option<String>,
    pub elapsed_us: u64,
    pub peak_rss_bytes: Option<u64>,
    pub outcome: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logical_store_calls: Option<LogicalStoreCallObservation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control_store_calls: Option<ControlCallObservation>,
    pub steps: Vec<GqtStepObservation>,
    pub verification: GqtVerification,
    pub merge: Option<GqtMergeEvidence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_receipt_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_peak_rss_bytes: Option<u64>,
}
#[derive(Debug, Clone, Default)]
pub struct RunOptions {
    pub scratch_root: Option<PathBuf>,
    pub worker_executable: Option<PathBuf>,
    pub dataset_cache: Option<PathBuf>,
    pub no_build: bool,
    pub fixture_bindings: Vec<String>,
    pub served: Option<ServedInput>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RunExecution {
    pub runner_output_version: u32,
    pub case_id: String,
    pub case_path: PathBuf,
    pub point_id: String,
    pub point_name: String,
    pub requested_repetitions: u32,
    pub bound: BoundGqt,
    /// The repetition worker build: client evidence for a served target.
    pub build: BuildEvidence,
    pub machine: crate::machine::MachineIdentityV1,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub environment: Option<crate::environment::LocalEnvironmentEvidence>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fixture: Option<DatasetManifestV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dataset_cache_hit: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_receipt: Option<ServerDeploymentReceiptV1>,
    pub samples: Vec<GqtRepObservation>,
    pub wall_clock: WallClockSummary,
    pub durable_record: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SuiteExecution {
    pub runner_output_version: u32,
    pub suite: String,
    pub suite_path: PathBuf,
    pub runs: Vec<RunExecution>,
}
struct Generation {
    storage: Arc<dyn StorageAdapter>,
    counts: Arc<StorageReadCounts>,
    gate: Option<PreparationWriteGate>,
}
fn generation(uri: &str, guarded: bool) -> RunnerResult<Generation> {
    let storage = omnigraph::storage::storage_for_uri(uri).map_err(|e| failure(e.to_string()))?;
    let (storage, gate) = if guarded {
        let (s, g) = guard_preparation_writes(storage, uri);
        (s, Some(g))
    } else {
        (storage, None)
    };
    let (storage, counts) = CountingStorageAdapter::new(storage);
    Ok(Generation {
        storage,
        counts,
        gate,
    })
}
struct LocalInstrumentation {
    root: PathBuf,
    uri: String,
    metadata: MetadataDigest,
    generation: Generation,
    baseline: Option<ControlSnapshot>,
    manifest: LogicalCallCounter,
    table: LogicalCallCounter,
    merge_probes: MergeWriteProbes,
}
struct HostState {
    local: Option<LocalInstrumentation>,
    current: usize,
    started: Option<(usize, Instant)>,
    elapsed: Option<u64>,
    logical: Option<LogicalStoreCallObservation>,
    control: Option<ControlCallObservation>,
    steps: Vec<GqtStepObservation>,
    occurrences: BTreeMap<usize, u32>,
    prefix_reads: u32,
    assertions: u32,
    following: u32,
    selected_assertion: bool,
    error: Option<RunnerError>,
    ready: bool,
    settled: bool,
    merge: Option<GqtMergeEvidence>,
}
impl HostState {
    fn new(local: Option<LocalInstrumentation>) -> Self {
        Self {
            local,
            current: 0,
            started: None,
            elapsed: None,
            logical: None,
            control: None,
            steps: Vec::new(),
            occurrences: BTreeMap::new(),
            prefix_reads: 0,
            assertions: 0,
            following: 0,
            selected_assertion: false,
            error: None,
            ready: false,
            settled: false,
            merge: None,
        }
    }
}
struct BenchHost<'a, S> {
    signals: Mutex<&'a mut S>,
    state: Mutex<HostState>,
    selected: usize,
    kinds: BTreeMap<usize, StepKind>,
    expected_reads: u32,
    attribution: crate::case::Attribution,
}
fn failure(message: impl std::fmt::Display) -> RunnerError {
    RunnerError::new("gqt_prepare_failed", message.to_string())
}
impl<S: MeasurementSignals + Send> BenchHost<'_, S> {
    fn start(&self, ordinal: usize) -> RunnerResult<()> {
        let mut state = self.state.lock().unwrap();
        let kind = *self
            .kinds
            .get(&ordinal)
            .ok_or_else(|| failure("unknown operation ordinal"))?;
        if state.started.is_some() {
            return Err(failure("overlapping operation callbacks"));
        }
        let preparing = state.elapsed.is_none();
        if kind == StepKind::Restart {
            let local = state
                .local
                .as_mut()
                .ok_or_else(|| failure("served restart is unsupported"))?;
            if preparing {
                local
                    .generation
                    .gate
                    .as_ref()
                    .ok_or_else(|| failure("missing preparation gate"))?
                    .validate_preparation()
                    .map_err(failure)?;
            }
            local.generation = generation(&local.uri, preparing)?;
        }
        if ordinal == self.selected {
            if !preparing {
                return Err(failure("selected operation executed more than once"));
            }
            if state.prefix_reads != self.expected_reads {
                return Err(failure(
                    "prefix read did not execute an operation; cache treatment is unproved",
                ));
            }
            if let Some(local) = &state.local {
                verify_metadata_shape(&local.root, &local.metadata, TraversalLimits::default())
                    .map_err(failure)?;
            }
            self.signals.lock().unwrap().ready()?;
            state.ready = true;
            if let Some(local) = &mut state.local {
                if local.manifest.take().has_mutations() || local.table.take().has_mutations() {
                    return Err(failure("Lance mutation occurred before selected operation"));
                }
                local.baseline = Some(ControlSnapshot::read(&local.generation.counts));
                if kind != StepKind::Restart {
                    local
                        .generation
                        .gate
                        .as_ref()
                        .ok_or_else(|| failure("missing preparation gate"))?
                        .begin_measurement()
                        .map_err(failure)?;
                }
            }
        } else if ordinal < self.selected && matches!(kind, StepKind::Query | StepKind::BranchList)
        {
            state.prefix_reads += 1;
        }
        state.started = Some((ordinal, Instant::now()));
        Ok(())
    }
    fn finish(&self, ordinal: usize) -> RunnerResult<()> {
        let ended = Instant::now();
        let mut state = self.state.lock().unwrap();
        let (started_ordinal, started) = state
            .started
            .take()
            .ok_or_else(|| failure("operation finished without starting"))?;
        if started_ordinal != ordinal {
            return Err(failure("operation callback ordinal mismatch"));
        }
        let elapsed = u64::try_from(ended.duration_since(started).as_micros())
            .map_err(|_| failure("operation duration overflow"))?;
        let occurrence = {
            let n = state.occurrences.entry(ordinal).or_default();
            *n += 1;
            *n
        };
        state.steps.push(GqtStepObservation {
            ordinal,
            occurrence,
            kind: self.kinds[&ordinal].into(),
            elapsed_us: elapsed,
        });
        if ordinal == self.selected {
            state.elapsed = Some(elapsed);
            if let Some(local) = &mut state.local {
                let logical = LogicalStoreCallObservation {
                    manifest: local.manifest.take(),
                    table: local.table.take(),
                    physical_attempts_observed: false,
                };
                let control = local
                    .baseline
                    .take()
                    .ok_or_else(|| failure("missing counter baseline"))?
                    .delta(ControlSnapshot::read(&local.generation.counts))?;
                let merge = if self.kinds[&ordinal] == StepKind::BranchMerge
                    && self.attribution == crate::case::Attribution::PerPhase
                {
                    Some(GqtMergeEvidence {
                        phases: crate::runner::phase_observations(
                            local.merge_probes.merge_timing_snapshot(),
                        ),
                        route: crate::runner::MergeRouteObservation::from_probes(
                            &local.merge_probes,
                        ),
                    })
                } else {
                    None
                };
                state.logical = Some(logical);
                state.control = Some(control);
                state.merge = merge;
            }
            self.signals.lock().unwrap().settled(elapsed)?;
            state.settled = true;
            if self.kinds[&ordinal] == StepKind::Restart {
                state
                    .local
                    .as_ref()
                    .ok_or_else(|| failure("served restart is unsupported"))?
                    .generation
                    .gate
                    .as_ref()
                    .ok_or_else(|| failure("missing reopen gate"))?
                    .begin_measurement()
                    .map_err(failure)?;
            }
        }
        Ok(())
    }
    fn callback(&self, result: RunnerResult<()>) -> Result<(), String> {
        result.map_err(|error| {
            let message = error.to_string();
            let mut state = self.state.lock().unwrap();
            state.error = Some(RunnerError::new(stage_code(&state), message.clone()));
            message
        })
    }
}
impl<S: MeasurementSignals + Send> ExecutionHost for BenchHost<'_, S> {
    type StepGuard = ();
    fn arm_seams(&self, seams: &[SeamDirective], _: &Step) -> Result<(), String> {
        if seams.is_empty() {
            Ok(())
        } else {
            Err("benchmark does not support seams".into())
        }
    }
    fn finish_seams(&self, _: ()) -> Result<(), String> {
        Ok(())
    }
    fn operation_started(&self, ordinal: usize) -> Result<(), String> {
        self.callback(self.start(ordinal))
    }
    fn operation_finished(&self, ordinal: usize) -> Result<(), String> {
        self.callback(self.finish(ordinal))
    }
    fn begin_operation(&self, value: impl FnOnce() -> serde_json::Value) {
        self.state.lock().unwrap().current = value()["ordinal"].as_u64().unwrap_or(0) as usize;
    }
    fn record(&self, kind: &str, value: impl FnOnce() -> serde_json::Value) {
        if kind == "assertion" && value()["status"] == "passed" {
            let mut s = self.state.lock().unwrap();
            s.assertions += 1;
            if s.current == self.selected {
                s.selected_assertion = true
            }
            if s.current > self.selected
                && crate::gqt_case::explicit_verification(self.kinds[&s.current])
            {
                s.following += 1
            }
        }
    }
    fn reopen<'a>(
        &'a self,
        uri: &'a str,
        _: Option<Arc<dyn StorageAdapter>>,
    ) -> BoxFuture<'a, Result<Omnigraph, OmniError>> {
        let storage = self
            .state
            .lock()
            .unwrap()
            .local
            .as_ref()
            .map(|l| l.generation.storage.clone());
        async move {
            let storage = storage.ok_or_else(|| {
                OmniError::Io(std::io::Error::other("served restart is unsupported"))
            })?;
            Omnigraph::open_with_storage(uri, storage).await
        }
        .boxed()
    }
}
pub(crate) async fn execute_gqt_rep_signaled<S: MeasurementSignals + Send>(
    repetition: u32,
    root: &Path,
    input: &PhysicalDigest,
    metadata: &MetadataDigest,
    bound: &BoundGqt,
    signals: &mut S,
) -> RunnerResult<GqtRepObservation> {
    bound.revalidate().map_err(failure)?;
    let case = bound.plan.queries.parse().map_err(failure)?;
    let uri = root
        .to_str()
        .ok_or_else(|| failure("non-UTF8 store path"))?;
    let whole_file_started = Instant::now();
    let manifest = LogicalCallCounter::default();
    let table = LogicalCallCounter::default();
    let probes = QueryIoProbes {
        manifest_wrapper: Some(Arc::new(manifest.clone()) as Arc<dyn WrappingObjectStore>),
        table_wrapper: Some(Arc::new(table.clone()) as Arc<dyn WrappingObjectStore>),
        ..Default::default()
    };
    with_query_io_probes(probes, async {
        let generation = generation(uri, true)?;
        let db = tokio::time::timeout(
            Duration::from_millis(case.runner.timeout_ms),
            Omnigraph::open_with_storage(uri, generation.storage.clone()),
        )
        .await
        .map_err(|_| failure("whole-file timeout during open"))?
        .map_err(failure)?;
        let merge_probes = MergeWriteProbes::default();
        let session = case_session(db, &case, Engine::V2).map_err(failure)?;
        let host = BenchHost {
            signals: Mutex::new(signals),
            selected: bound.plan.definition.workload.measured_step.ordinal,
            kinds: case
                .steps()
                .into_iter()
                .map(|s| (s.ordinal, s.kind))
                .collect(),
            expected_reads: bound.plan.cache_condition.iterations,
            attribution: bound.plan.definition.protocol.attribution,
            state: Mutex::new(HostState::new(Some(LocalInstrumentation {
                root: root.into(),
                uri: uri.into(),
                metadata: metadata.clone(),
                generation,
                baseline: None,
                manifest,
                table,
                merge_probes: merge_probes.clone(),
            }))),
        };
        let executed = execute_steps(
            &case,
            Path::new(&bound.plan.queries.stem),
            false,
            session,
            uri,
            None,
            &host,
        );
        let executed = if host.kinds[&host.selected] == StepKind::BranchMerge
            && host.attribution == crate::case::Attribution::PerPhase
        {
            with_merge_write_probes(merge_probes, executed).boxed()
        } else {
            executed
        };
        let result = tokio::time::timeout(
            Duration::from_millis(case.runner.timeout_ms)
                .saturating_sub(whole_file_started.elapsed()),
            executed,
        )
        .await
        .unwrap_or_else(|_| Err("queries whole-file timeout exceeded".into()));

        finish_execution(
            &host,
            result.map(|_| ()),
            bound,
            repetition,
            &PreparationProofV2::Embedded {
                physical_digest: input.clone(),
                metadata_digest: metadata.clone(),
            },
        )
    })
    .await
}

pub(crate) async fn execute_served_rep_signaled<S: MeasurementSignals + Send>(
    repetition: u32,
    bound: &BoundGqt,
    input: &ServedInput,
    signals: &mut S,
) -> RunnerResult<GqtRepObservation> {
    bound.revalidate().map_err(failure)?;
    input.validate(bound).map_err(failure)?;
    if bound.identity.environment.target != Target::Server {
        return Err(failure("served input requires a server point"));
    }
    let case = bound.plan.queries.parse().map_err(failure)?;
    let host = BenchHost {
        signals: Mutex::new(signals),
        selected: bound.plan.definition.workload.measured_step.ordinal,
        kinds: case
            .steps()
            .into_iter()
            .map(|s| (s.ordinal, s.kind))
            .collect(),
        expected_reads: bound.plan.cache_condition.iterations,
        attribution: bound.plan.definition.protocol.attribution,
        state: Mutex::new(HostState::new(None)),
    };
    let result = tokio::time::timeout(
        Duration::from_millis(case.runner.timeout_ms),
        omnigraph_gqt_core::execute_steps_served(&case, &input.target, &host),
    )
    .await
    .unwrap_or_else(|_| Err("queries whole-file timeout exceeded".into()))
    .map_err(|message| crate::gqt_served::redact_error(message, input.target.token.as_deref()));
    let proof = PreparationProofV2::Served {
        server_receipt_sha256: input.receipt.digest().map_err(failure)?,
    };
    finish_execution(&host, result, bound, repetition, &proof)
}

fn finish_execution<S: MeasurementSignals + Send>(
    host: &BenchHost<'_, S>,
    result: Result<(), String>,
    bound: &BoundGqt,
    repetition: u32,
    proof: &PreparationProofV2,
) -> RunnerResult<GqtRepObservation> {
    let mut state = host.state.lock().unwrap();
    let error = state.error.take().or_else(|| {
        result
            .err()
            .map(|message| RunnerError::new(stage_code(&state), message))
    });
    let Some(elapsed_us) = state.elapsed else {
        return Err(error.unwrap_or_else(|| failure("selected operation never executed")));
    };
    let (physical, receipt) = match proof {
        PreparationProofV2::Embedded {
            physical_digest, ..
        } => (Some(physical_digest.digest_sha256.clone()), None),
        PreparationProofV2::Served {
            server_receipt_sha256,
        } => (None, Some(server_receipt_sha256.clone())),
    };
    let mut sample = GqtRepObservation {
        repetition,
        input_physical_digest_sha256: physical,
        elapsed_us,
        peak_rss_bytes: None,
        outcome: if error.is_some() {
            "verification-failed"
        } else {
            "expectations-passed"
        }
        .into(),
        logical_store_calls: state.logical.take(),
        control_store_calls: state.control.take(),
        steps: std::mem::take(&mut state.steps),
        merge: state.merge.take(),
        verification: GqtVerification {
            selected_assertion_passed: state.selected_assertion,
            assertions_passed: state.assertions,
            following_assertions: state.following,
        },
        server_receipt_sha256: receipt,
        client_peak_rss_bytes: None,
    };
    if let Some(error) = error {
        return Err(if state.settled {
            error.with_gqt_settled_sample(sample)
        } else {
            error
        });
    }
    if let Err(error) = validate_sample(&sample, bound, repetition, proof, elapsed_us, false) {
        sample.outcome = "verification-failed".into();
        return Err(
            RunnerError::new("gqt_verification_failed", error.to_string())
                .with_gqt_settled_sample(sample),
        );
    }
    Ok(sample)
}
pub(crate) fn validate_sample(
    sample: &GqtRepObservation,
    bound: &BoundGqt,
    repetition: u32,
    input: &PreparationProofV2,
    elapsed: u64,
    parent: bool,
) -> RunnerResult<()> {
    validate_evidence(sample, bound, repetition, input, elapsed, parent, false)
}
pub(crate) fn validate_failed_sample(
    sample: &GqtRepObservation,
    bound: &BoundGqt,
    repetition: u32,
    input: &PreparationProofV2,
    elapsed: u64,
) -> RunnerResult<()> {
    validate_evidence(sample, bound, repetition, input, elapsed, false, true)
}
fn validate_evidence(
    sample: &GqtRepObservation,
    bound: &BoundGqt,
    repetition: u32,
    input: &PreparationProofV2,
    elapsed: u64,
    parent: bool,
    failed: bool,
) -> RunnerResult<()> {
    if !matches!(
        (input, bound.identity.environment.target),
        (PreparationProofV2::Embedded { .. }, Target::Engine)
            | (PreparationProofV2::Served { .. }, Target::Server)
    ) {
        return Err(failure("sample proof target mismatch"));
    }
    let selected = bound.plan.definition.workload.measured_step.ordinal;
    let receipts: Vec<_> = sample
        .steps
        .iter()
        .filter(|s| s.ordinal == selected)
        .collect();
    if sample.repetition != repetition
        || sample.elapsed_us != elapsed
        || sample.outcome
            != if failed {
                "verification-failed"
            } else {
                "expectations-passed"
            }
        || receipts.len() != 1
        || receipts[0].elapsed_us != elapsed
        || receipts[0].occurrence != 1
        || (!failed
            && (!sample.verification.selected_assertion_passed
                || sample.verification.following_assertions == 0
                || sample.verification.assertions_passed < 2))
        || !sample_evidence_matches(sample, input, parent)
    {
        return Err(failure(
            "GQT sample evidence disagrees with the admitted operation",
        ));
    }
    let case = bound.plan.queries.parse().map_err(failure)?;
    let kinds: BTreeMap<_, _> = case
        .steps()
        .into_iter()
        .map(|s| (s.ordinal, s.kind))
        .collect();
    if let (Some(logical), Some(control)) =
        (&sample.logical_store_calls, &sample.control_store_calls)
    {
        crate::record::validate_call_totals(
            repetition as usize,
            logical.manifest,
            logical.table,
            control,
        )
        .map_err(failure)?;
    }
    validate_merge_evidence(
        sample.merge.as_ref(),
        kinds[&selected],
        bound.plan.definition.protocol.attribution,
    )
    .map_err(failure)?;
    validate_receipt_treatment(&sample.steps, selected, &bound.identity.cache_condition)
        .map_err(failure)?;
    let mut occurrences = BTreeMap::new();
    if sample.steps.len() > crate::gqt_case::MAX_EXPANDED_STEPS {
        return Err(failure("too many operation receipts"));
    }
    for step in &sample.steps {
        if kinds
            .get(&step.ordinal)
            .is_none_or(|kind| GqtOperationKind::from(*kind) != step.kind)
        {
            return Err(failure("unknown receipt ordinal"));
        }
        let n = occurrences.entry(step.ordinal).or_insert(0);
        *n += 1;
        if *n != step.occurrence {
            return Err(failure("receipt occurrence sequence mismatch"));
        }
    }
    Ok(())
}

pub(crate) fn sample_evidence_matches(
    sample: &GqtRepObservation,
    input: &PreparationProofV2,
    parent: bool,
) -> bool {
    let rss_matches = |rss: Option<u64>| {
        if parent {
            rss.is_some_and(|n| n > 0)
        } else {
            rss.is_none()
        }
    };
    match input {
        PreparationProofV2::Embedded {
            physical_digest, ..
        } => {
            sample.input_physical_digest_sha256.as_ref() == Some(&physical_digest.digest_sha256)
                && sample
                    .logical_store_calls
                    .as_ref()
                    .is_some_and(|c| !c.physical_attempts_observed)
                && sample.control_store_calls.is_some()
                && sample.server_receipt_sha256.is_none()
                && sample.client_peak_rss_bytes.is_none()
                && rss_matches(sample.peak_rss_bytes)
        }
        PreparationProofV2::Served {
            server_receipt_sha256,
        } => {
            sample.server_receipt_sha256.as_ref() == Some(server_receipt_sha256)
                && sample.input_physical_digest_sha256.is_none()
                && sample.logical_store_calls.is_none()
                && sample.control_store_calls.is_none()
                && sample.peak_rss_bytes.is_none()
                && sample.merge.is_none()
                && rss_matches(sample.client_peak_rss_bytes)
                && sample.steps.iter().all(|s| {
                    SERVED_READ_KINDS
                        .into_iter()
                        .any(|kind| GqtOperationKind::from(kind) == s.kind)
                })
        }
    }
}

pub async fn execute_suite(
    suite: &crate::suite::ResolvedSuite,
    options: &RunOptions,
) -> RunnerResult<SuiteExecution> {
    for run in &suite.runs {
        validate_run_options(run.case.gqt().map_err(failure)?, options)?;
    }
    let mut runs = Vec::new();
    let mut identities = std::collections::BTreeSet::new();
    for run in &suite.runs {
        let executed = execute_run(run, options).await?;
        if !identities.insert(executed.point_id.clone()) {
            return Err(failure("suite contains duplicate bound point identity"));
        }
        runs.push(executed)
    }
    Ok(SuiteExecution {
        runner_output_version: 1,
        suite: suite.definition.name.clone(),
        suite_path: suite.suite_path.clone(),
        runs,
    })
}
/// Canonical bytes: manifest <=1 MiB, run identity <512 KiB, SUT <=8 KiB,
/// machine <32 KiB, backend <16 KiB and remaining bounded fields <8 KiB.
/// Each component is admitted by record validation; pretty diagnostics are excluded.
pub(crate) const GQT_RECORD_ENVELOPE_RESERVE: usize = 2 * 1024 * 1024;

pub(crate) fn sample_byte_upper_bound(plan: &PlannedGqt) -> RunnerResult<usize> {
    let case = plan.queries.parse().map_err(failure)?;
    let selected = plan.definition.workload.measured_step.ordinal;
    let selected_kind = case
        .steps()
        .into_iter()
        .find(|s| s.ordinal == selected)
        .ok_or_else(|| failure("selected operation is missing"))?
        .kind;
    let mut steps = Vec::new();
    for item in &case.items {
        let (body, times) = match item {
            omnigraph_gqt_core::Item::Step(step) => (std::slice::from_ref(step), 1),
            omnigraph_gqt_core::Item::Loop { steps, values, .. } => {
                (steps.as_slice(), values.len())
            }
        };
        let count = body
            .len()
            .checked_mul(times)
            .and_then(|n| n.checked_add(steps.len()))
            .ok_or_else(|| failure("receipt count overflow"))?;
        if count > crate::gqt_case::MAX_EXPANDED_STEPS {
            return Err(failure("receipt count exceeds admitted program bound"));
        }
        for _ in 0..times {
            for step in body {
                steps.push(GqtStepObservation {
                    ordinal: step.ordinal(),
                    occurrence: u32::MAX,
                    kind: step.operation_kind().into(),
                    elapsed_us: u64::MAX,
                });
            }
        }
    }
    let counts = crate::counting::LogicalCallCounts {
        get: u64::MAX,
        put: u64::MAX,
        put_part: u64::MAX,
        head: u64::MAX,
        list: u64::MAX,
        delete: u64::MAX,
        copy: u64::MAX,
        rename: u64::MAX,
        multipart_complete: u64::MAX,
        multipart_abort: u64::MAX,
    };
    let merge = if selected_kind == StepKind::BranchMerge
        && plan.definition.protocol.attribution == crate::case::Attribution::PerPhase
    {
        let mut phases =
            crate::runner::phase_observations(MergeWriteProbes::default().merge_timing_snapshot());
        for p in &mut phases {
            p.total_us = u64::MAX;
            p.max_us = u64::MAX;
            p.interval_count = u64::MAX;
        }
        Some(GqtMergeEvidence {
            phases,
            route: crate::runner::MergeRouteObservation {
                table_walk_intervals: u64::MAX,
                stage_merge_insert_calls: u64::MAX,
                stage_merge_insert_rows: u64::MAX,
                stage_known_present_update_calls: u64::MAX,
                stage_known_present_update_rows: u64::MAX,
                stage_fenced_insert_calls: u64::MAX,
                stage_fenced_insert_rows: u64::MAX,
                strict_insert_preflight_calls: u64::MAX,
            },
        })
    } else {
        None
    };
    let sample = GqtRepObservation {
        repetition: u32::MAX,
        input_physical_digest_sha256: Some("0".repeat(64)),
        elapsed_us: u64::MAX,
        peak_rss_bytes: Some(u64::MAX),
        outcome: "expectations-passed".into(),
        logical_store_calls: Some(LogicalStoreCallObservation {
            manifest: counts,
            table: counts,
            physical_attempts_observed: false,
        }),
        control_store_calls: Some(ControlCallObservation {
            read_text: u64::MAX,
            read_text_if_exists: u64::MAX,
            read_text_versioned: u64::MAX,
            exists: u64::MAX,
            list_dir: u64::MAX,
            mutation_calls: u64::MAX,
            write_text: u64::MAX,
            delete: u64::MAX,
        }),
        server_receipt_sha256: Some("0".repeat(64)),
        client_peak_rss_bytes: Some(u64::MAX),
        steps,
        verification: GqtVerification {
            selected_assertion_passed: false,
            assertions_passed: u32::MAX,
            following_assertions: u32::MAX,
        },
        merge,
    };
    serde_json::to_vec(&sample)
        .map(|v| v.len())
        .map_err(failure)
}

pub(crate) fn preflight_acquisition_budget(
    plan: &PlannedGqt,
    repetitions: u32,
) -> RunnerResult<()> {
    let sample = sample_byte_upper_bound(plan)?;
    let bytes = sample
        .checked_add(1)
        .and_then(|n| n.checked_mul(repetitions as usize))
        .and_then(|n| n.checked_add(GQT_RECORD_ENVELOPE_RESERVE))
        .ok_or_else(|| {
            RunnerError::new("gqt_record_budget_exceeded", "receipt byte bound overflow")
        })?;
    if bytes > crate::record::MAX_RECORD_BYTES {
        return Err(RunnerError::new(
            "gqt_record_budget_exceeded",
            format!(
                "{repetitions} repetitions require a worst-case canonical receipt budget of {bytes} bytes; limit is {}; reduce repetitions",
                crate::record::MAX_RECORD_BYTES
            ),
        ));
    }
    Ok(())
}

pub async fn execute_run(
    run: &crate::suite::ResolvedRun,
    options: &RunOptions,
) -> RunnerResult<RunExecution> {
    crate::runner::enforce_release_build().map_err(|mut error| {
        error.context.case_id = Some(run.case.id().to_owned());
        error
    })?;
    crate::runner::refuse_unmodeled_runtime_overrides()?;
    let plan = run.case.gqt().map_err(failure)?.clone();
    plan.revalidate().map_err(failure)?;
    validate_run_options(&plan, options)?;
    if run.repetitions == 0 || run.repetitions > crate::suite::MAX_REPETITIONS_PER_CASE {
        return Err(failure("invalid repetition count"));
    }
    preflight_acquisition_budget(&plan, run.repetitions)?;
    let run = run.clone();
    let options = options.clone();
    tokio::task::spawn_blocking(move || {
        std::thread::Builder::new()
            .name("gqt-run-owner".into())
            .stack_size(64 * 1024 * 1024)
            .spawn(move || execute_owned(run, plan, options))
            .map_err(|e| failure(e.to_string()))?
            .join()
            .map_err(|_| failure("run owner panicked"))?
    })
    .await
    .map_err(|e| failure(e.to_string()))?
}
fn execute_owned(
    run: crate::suite::ResolvedRun,
    plan: PlannedGqt,
    options: RunOptions,
) -> RunnerResult<RunExecution> {
    if let Some(input) = &options.served {
        return execute_served_owned(run, plan, &options, input);
    }
    let cache = options
        .dataset_cache
        .clone()
        .or_else(|| options.scratch_root.as_ref().map(|p| p.join("datasets")))
        .ok_or_else(|| failure("--dataset-cache or --scratch-root is required"))?;
    std::fs::create_dir_all(&cache).map_err(|e| failure(e.to_string()))?;
    let Backend::LocalFs {
        filesystem,
        storage_class,
    } = plan.definition.environment.backend
    else {
        return Err(failure("local backend required"));
    };
    let environment =
        crate::environment::verify_local_environment(&cache, filesystem, storage_class)
            .map_err(failure)?;
    let workspace = tempfile::Builder::new()
        .prefix("gqt-run-")
        .tempdir_in(&cache)
        .map_err(|e| failure(e.to_string()))?;
    let source = crate::runner::resolve_bound_worker(
        options
            .worker_executable
            .as_deref()
            .ok_or_else(|| failure("worker executable required"))?,
    )?;
    let worker = crate::runner::stage_bound_worker(source, workspace.path())?;
    crate::gqt_supervisor::preflight_plan(&plan, &cache.canonicalize().map_err(failure)?)?;
    let binding = binding(&plan.dataset, &options.fixture_bindings)?;
    let lease = crate::dataset_cache::acquire(
        &plan.dataset_build_plan().map_err(failure)?,
        &cache,
        &worker.executable,
        options.no_build,
        binding,
    )?;
    let bound = plan
        .bind(
            &lease.manifest.handoff.summary.logical_content_sha256,
            &lease.manifest.handoff.summary.algorithm,
        )
        .map_err(failure)?;
    let mut samples = Vec::new();
    let mut machine = None;
    let mut build = None;
    let acquisition = (|| -> RunnerResult<()> {
        for repetition in 1..=run.repetitions {
            let metadata = lease.restore()?;
            let scratch = lease.root.join(format!("worker-scratch-{repetition:08}"));
            std::fs::create_dir(&scratch).map_err(|e| failure(e.to_string()))?;
            let input = crate::gqt_supervisor::SupervisionInput {
                worker_executable: worker.executable.clone(),
                expected_worker_executable_sha256: worker.executable_sha256.clone(),
                expected_machine: machine.clone(),
                repetition,
                case: bound.clone(),
                worker_scratch_root: scratch.clone(),
                execution: RepetitionInputV2::Embedded {
                    fixture_manifest_sha256: crate::model::typed_sha256(&lease.manifest)
                        .map_err(failure)?,
                    repetition_root: lease.active.clone(),
                    physical_digest: lease.physical().clone(),
                    metadata_digest: metadata.clone(),
                },
                deadline: plan
                    .definition
                    .protocol
                    .deadline_seconds
                    .map(Duration::from_secs),
                #[cfg(test)]
                auxiliary_deadline_override: None,
            };
            match crate::gqt_supervisor::supervise_repetition(input) {
                Ok(observed) => {
                    validate_sample(
                        &observed.sample,
                        &bound,
                        repetition,
                        &PreparationProofV2::Embedded {
                            physical_digest: lease.physical().clone(),
                            metadata_digest: metadata.clone(),
                        },
                        observed.sample.elapsed_us,
                        true,
                    )?;
                    machine = Some(observed.machine);
                    build = Some(observed.worker_build);
                    samples.push(observed.sample);
                    lease.remove_active()?;
                    std::fs::remove_dir_all(&scratch).map_err(|e| failure(e.to_string()))?;
                }
                Err(mut error) => {
                    if crate::dataset_cache::contained(&error) {
                        lease.remove_active()?;
                        let _ = std::fs::remove_dir_all(&scratch);
                    } else {
                        let marker = lease.quarantine(&error.message);
                        let _ = workspace.keep();
                        if let Err(marker) = marker {
                            error.message.push_str(&format!(
                                "; quarantine marker failed: {}",
                                marker.message
                            ));
                        }
                    }
                    return Err(error);
                }
            }
        }
        lease.verify()?;
        Ok(())
    })();
    if samples.is_empty() {
        return Err(acquisition
            .err()
            .unwrap_or_else(|| failure("empty acquisition")));
    }
    let wall_clock = summarize_wall_clock(&samples);
    let execution = RunExecution {
        runner_output_version: 1,
        case_id: plan.definition.id.clone(),
        case_path: run.case_path,
        point_id: bound.point_id.clone(),
        point_name: bound.point_name.clone(),
        requested_repetitions: run.repetitions,
        bound,
        build: crate::runner::build_evidence(build.as_ref())?,
        machine: machine.ok_or_else(|| failure("no worker identity"))?,
        environment: Some(environment),
        fixture: Some(lease.manifest.clone()),
        dataset_cache_hit: Some(lease.cache_hit),
        server_receipt: None,
        samples,
        wall_clock,
        durable_record: false,
    };
    match acquisition {
        Ok(()) => Ok(execution),
        Err(mut error) => {
            error.context.gqt_partial_run = Some(Box::new(execution));
            Err(error)
        }
    }
}

pub fn validate_run_options(plan: &PlannedGqt, options: &RunOptions) -> RunnerResult<()> {
    plan.revalidate().map_err(failure)?;
    match (plan.definition.environment.target, &options.served) {
        (Target::Engine, None) => Ok(()),
        (Target::Server, Some(input)) => {
            if options.no_build || !options.fixture_bindings.is_empty() {
                return Err(failure(
                    "served graphs are pre-provisioned; local dataset acquisition options do not apply",
                ));
            }
            let bound = input.receipt.bind(plan).map_err(failure)?;
            input.validate(&bound).map_err(failure)
        }
        _ => Err(failure(
            "server scenarios require exactly one matching server target and deployment receipt; embedded scenarios refuse them",
        )),
    }
}

fn execute_served_owned(
    run: crate::suite::ResolvedRun,
    plan: PlannedGqt,
    options: &RunOptions,
    input: &ServedInput,
) -> RunnerResult<RunExecution> {
    let bound = input.receipt.bind(&plan).map_err(failure)?;
    input.validate(&bound).map_err(failure)?;
    let scratch = options
        .scratch_root
        .as_ref()
        .or(options.dataset_cache.as_ref())
        .ok_or_else(|| failure("served acquisition requires a client scratch directory"))?;
    std::fs::create_dir_all(scratch).map_err(failure)?;
    let scratch = scratch.canonicalize().map_err(failure)?;
    let workspace = tempfile::Builder::new()
        .prefix("gqt-served-")
        .tempdir_in(scratch)
        .map_err(failure)?;
    let worker = crate::runner::stage_bound_worker(
        crate::runner::resolve_bound_worker(
            options
                .worker_executable
                .as_deref()
                .ok_or_else(|| failure("worker executable required"))?,
        )?,
        workspace.path(),
    )?;
    let execution_input = RepetitionInputV2::Served {
        input: Box::new(input.clone()),
    };
    let proof = execution_input.proof().map_err(failure)?;
    let mut samples = Vec::new();
    let mut machine = None;
    let mut build = None;
    let mut contained = true;
    let acquisition = (|| -> RunnerResult<()> {
        for repetition in 1..=run.repetitions {
            let worker_scratch_root = workspace
                .path()
                .join(format!("worker-scratch-{repetition:08}"));
            std::fs::create_dir(&worker_scratch_root).map_err(failure)?;
            let observed = crate::gqt_supervisor::supervise_repetition(
                crate::gqt_supervisor::SupervisionInput {
                    worker_executable: worker.executable.clone(),
                    expected_worker_executable_sha256: worker.executable_sha256.clone(),
                    expected_machine: machine.clone(),
                    repetition,
                    case: bound.clone(),
                    execution: execution_input.clone(),
                    worker_scratch_root: worker_scratch_root.clone(),
                    deadline: plan
                        .definition
                        .protocol
                        .deadline_seconds
                        .map(Duration::from_secs),
                    #[cfg(test)]
                    auxiliary_deadline_override: None,
                },
            );
            match observed {
                Ok(observed) => {
                    validate_sample(
                        &observed.sample,
                        &bound,
                        repetition,
                        &proof,
                        observed.sample.elapsed_us,
                        true,
                    )?;
                    machine = Some(observed.machine);
                    build = Some(observed.worker_build);
                    samples.push(observed.sample);
                    std::fs::remove_dir_all(worker_scratch_root).map_err(failure)?;
                }
                Err(mut error) => {
                    contained = crate::dataset_cache::contained(&error);
                    if !contained {
                        if let Err(marker) = crate::dataset_cache::quarantine_directory(
                            workspace.path(),
                            &error.message,
                        ) {
                            error.message.push_str(&format!(
                                "; quarantine marker failed: {}",
                                marker.message
                            ));
                        }
                    }
                    return Err(error);
                }
            }
        }
        Ok(())
    })();
    if !contained {
        let _ = workspace.keep();
    }
    if samples.is_empty() {
        return Err(acquisition
            .err()
            .unwrap_or_else(|| failure("empty acquisition")));
    }
    let wall_clock = summarize_wall_clock(&samples);
    let execution = RunExecution {
        runner_output_version: 1,
        case_id: plan.definition.id.clone(),
        case_path: run.case_path,
        point_id: bound.point_id.clone(),
        point_name: bound.point_name.clone(),
        requested_repetitions: run.repetitions,
        bound,
        build: crate::runner::build_evidence(build.as_ref())?,
        machine: machine.ok_or_else(|| failure("no client identity"))?,
        environment: None,
        fixture: None,
        dataset_cache_hit: None,
        server_receipt: Some(input.receipt.clone()),
        samples,
        wall_clock,
        durable_record: false,
    };
    match acquisition {
        Ok(()) => Ok(execution),
        Err(mut error) => {
            error.context.gqt_partial_run = Some(Box::new(execution));
            Err(error)
        }
    }
}

fn summarize_wall_clock(samples: &[GqtRepObservation]) -> WallClockSummary {
    let mut times: Vec<_> = samples.iter().map(|s| s.elapsed_us).collect();
    times.sort_unstable();
    let count = times.len();
    WallClockSummary {
        observed_repetitions: count as u32,
        min_us: times[0],
        p50_us: times[(count - 1) / 2],
        max_us: times[count - 1],
        p95_us: (count >= 20).then(|| times[(count * 95).div_ceil(100) - 1]),
        p95_supported: count >= 20,
    }
}
fn binding<'a>(
    recipe: &crate::gqt_case::DatasetRecipe,
    bindings: &'a [String],
) -> RunnerResult<Option<&'a str>> {
    if let crate::gqt_case::DatasetRecipe::Registered { reference, .. } = recipe {
        let matches: Vec<_> = bindings
            .iter()
            .filter(|b| {
                b.split_once('=')
                    .is_some_and(|(id, _)| id == reference.definition.fixture_id)
            })
            .collect();
        if matches.len() != 1 {
            return Err(failure(
                "registered dataset requires exactly one matching --fixture ID=BUNDLE",
            ));
        }
        Ok(Some(matches[0]))
    } else {
        if !bindings.is_empty() {
            return Err(failure("--fixture supplied for an authored dataset"));
        }
        Ok(None)
    }
}
pub async fn build_dataset(
    plan: &DatasetBuildPlan,
    options: &RunOptions,
) -> RunnerResult<DatasetManifestV1> {
    crate::runner::enforce_release_build()?;
    crate::runner::refuse_unmodeled_runtime_overrides()?;
    plan.revalidate().map_err(failure)?;
    let plan = plan.clone();
    let options = options.clone();
    tokio::task::spawn_blocking(move || {
        let cache = options
            .dataset_cache
            .as_deref()
            .ok_or_else(|| failure("--dataset-cache is required"))?;
        std::fs::create_dir_all(cache).map_err(|e| failure(e.to_string()))?;
        let Backend::LocalFs {
            filesystem,
            storage_class,
        } = plan.environment.backend
        else {
            return Err(failure("local backend required"));
        };
        crate::environment::verify_local_environment(cache, filesystem, storage_class)
            .map_err(failure)?;
        let workspace = tempfile::Builder::new()
            .prefix("gqt-build-")
            .tempdir_in(cache)
            .map_err(|e| failure(e.to_string()))?;
        let worker = crate::runner::stage_bound_worker(
            crate::runner::resolve_bound_worker(
                options
                    .worker_executable
                    .as_deref()
                    .ok_or_else(|| failure("worker executable required"))?,
            )?,
            workspace.path(),
        )?;
        let binding = binding(&plan.dataset, &options.fixture_bindings)?;
        crate::dataset_cache::acquire(&plan, cache, &worker.executable, options.no_build, binding)
            .map(|lease| lease.manifest.clone())
    })
    .await
    .map_err(|e| failure(e.to_string()))?
}

fn stage_code(state: &HostState) -> &'static str {
    if state.settled {
        "gqt_verification_failed"
    } else if state.ready {
        "gqt_measure_failed"
    } else {
        "gqt_prepare_failed"
    }
}
pub(crate) fn validate_merge_evidence(
    evidence: Option<&GqtMergeEvidence>,
    kind: StepKind,
    attribution: crate::case::Attribution,
) -> Result<(), String> {
    if kind != StepKind::BranchMerge || attribution == crate::case::Attribution::Off {
        if evidence.is_some() {
            return Err("unrequested merge phase evidence".into());
        }
        return Ok(());
    }
    let evidence = evidence.ok_or("missing selected merge phase snapshot")?;
    let mut names = std::collections::BTreeSet::new();
    for phase in &evidence.phases {
        if phase.phase.is_empty()
            || phase.phase.len() > 128
            || !names.insert(&phase.phase)
            || phase.max_us > phase.total_us
            || (phase.interval_count == 0 && (phase.total_us != 0 || phase.max_us != 0))
        {
            return Err("invalid merge phase snapshot".into());
        }
    }
    if evidence.phases.len() > 64 {
        return Err("merge phase inventory exceeds bound".into());
    }
    Ok(())
}

/// Durable receipts independently attest the cache preparation actually executed.
pub(crate) fn validate_receipt_treatment(
    steps: &[GqtStepObservation],
    selected: usize,
    condition: &crate::case::CacheCondition,
) -> Result<(), String> {
    let selected_at = steps
        .iter()
        .position(|s| s.ordinal == selected)
        .ok_or("missing selected receipt")?;
    let mut reads = 0u32;
    let mut reopened = false;
    for step in &steps[..selected_at] {
        if step.ordinal >= selected {
            return Err("prefix receipt crosses selected ordinal".into());
        }
        match step.kind {
            GqtOperationKind::Query | GqtOperationKind::BranchList if !reopened => {
                reads = reads
                    .checked_add(1)
                    .ok_or("prefix receipt count overflow")?
            }
            GqtOperationKind::Restart if reads > 0 && !reopened => reopened = true,
            GqtOperationKind::Show | GqtOperationKind::Settings => {}
            _ => {
                return Err("receipt prefix contains an inadmissible preparation operation".into());
            }
        }
    }
    if steps[selected_at + 1..]
        .iter()
        .any(|s| s.ordinal <= selected)
    {
        return Err("suffix receipt precedes or repeats selected ordinal".into());
    }
    let engine = engine_preparation(reopened, reads, condition.process);
    if reads != condition.iterations || engine != condition.engine {
        return Err("operation receipts disagree with derived cache preparation".into());
    }
    Ok(())
}
