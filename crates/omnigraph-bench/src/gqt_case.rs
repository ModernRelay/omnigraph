//! Authored GQT experiments. Recipe identity is resolved before I/O; point
//! identity is bound only after the dataset's logical evidence is verified.
use crate::case::{
    Backend, CacheCondition, EnginePreparation, NetworkPosition, PageCacheCondition,
    ProcessLifecycle, Protocol, ResetMode, Schedule, WarmupProgram,
};
pub use crate::model::{digest, sha256_bytes};
use crate::model::{read_text_file, typed_sha256, valid_kebab_id};
use omnigraph_compiler::query::parser::parse_query;
use omnigraph_gqt_core::{
    Case, Item, MAX_DURATION_SECONDS, MAX_WARMUP_SECONDS, SCRIPTED_HOST_WORKERS, Step,
    StepDescriptor, StepKind, step_kind,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

pub const MAX_SOURCE_BYTES: usize = 512 * 1024;
pub const MAX_EXPANDED_STEPS: usize = 4096;
/// The most workers one label of a workload's measured block declares; any
/// other block of the workload keeps the scripted hosts' bound.
pub const MEASURED_BLOCK_WORKERS: usize = 256;
/// The most sessions a workload's measured block declares against a server,
/// counted over the whole block at the swept value: every session there is
/// an actor with a bearer token of its own.
pub const SERVED_BLOCK_SESSIONS: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GqtScenario {
    #[serde(rename = "gqt-v1")]
    GqtV1,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GqtCaseV1 {
    pub version: u32,
    pub id: String,
    pub scenario: GqtScenario,
    pub fixture: GqtFixture,
    pub workload: GqtWorkload,
    pub environment: GqtEnvironment,
    pub protocol: Protocol,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum GqtFixture {
    Dataset {
        path: PathBuf,
    },
    Registered {
        reference: PathBuf,
        preparation: PathBuf,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GqtWorkload {
    pub queries: PathBuf,
    pub measured_step: MeasuredStep,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MeasuredStep {
    pub ordinal: usize,
    pub text: String,
}

/// A catalog entry expanded for one run, before its workload is read. An
/// entry without `step` measures its workload's `--- concurrent measured`
/// block, whose ordinal and text planning reads off the file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScenarioCase {
    pub id: String,
    pub fixture: GqtFixture,
    pub queries: PathBuf,
    pub step: Option<MeasuredStep>,
    pub environment: GqtEnvironment,
    pub protocol: Protocol,
}

impl ScenarioCase {
    /// The case measuring `step`.
    pub fn measuring(self, step: MeasuredStep) -> GqtCaseV1 {
        GqtCaseV1 {
            version: 1,
            id: self.id,
            scenario: GqtScenario::GqtV1,
            fixture: self.fixture,
            workload: GqtWorkload {
                queries: self.queries,
                measured_step: step,
            },
            environment: self.environment,
            protocol: self.protocol,
        }
    }

    /// What [`validate_definition`] checks of the case, the step when the
    /// entry names one.
    pub fn validate(&self) -> Result<(), String> {
        validate_run(1, &self.id, &self.environment, &self.protocol)?;
        self.step.as_ref().map_or(Ok(()), validate_step)
    }
}

impl From<GqtCaseV1> for ScenarioCase {
    fn from(case: GqtCaseV1) -> Self {
        Self {
            id: case.id,
            fixture: case.fixture,
            queries: case.workload.queries,
            step: Some(case.workload.measured_step),
            environment: case.environment,
            protocol: case.protocol,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "GqtEnvironmentWire", into = "GqtEnvironmentWire")]
pub struct GqtEnvironment {
    pub backend: Backend,
    pub target: Target,
    pub network_position: NetworkPosition,
}
/// Embedded identities omit the implicit target and position; served ones always name the position.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GqtEnvironmentWire {
    backend: Backend,
    #[serde(default, skip_serializing_if = "Target::is_engine")]
    target: Target,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    network_position: Option<NetworkPosition>,
}
impl TryFrom<GqtEnvironmentWire> for GqtEnvironment {
    type Error = String;
    fn try_from(wire: GqtEnvironmentWire) -> Result<Self, String> {
        let network_position = match (wire.target, wire.network_position) {
            (_, Some(position)) => position,
            (Target::Engine, None) => NetworkPosition::SameHost,
            (Target::Server, None) => {
                return Err(
                    "server targets must declare network_position (same-host, same-region or remote)"
                        .into(),
                );
            }
        };
        Ok(Self {
            backend: wire.backend,
            target: wire.target,
            network_position,
        })
    }
}
impl From<GqtEnvironment> for GqtEnvironmentWire {
    fn from(environment: GqtEnvironment) -> Self {
        let network_position = match (environment.target, environment.network_position) {
            (Target::Engine, NetworkPosition::SameHost) => None,
            (_, position) => Some(position),
        };
        Self {
            backend: environment.backend,
            target: environment.target,
            network_position,
        }
    }
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum Target {
    #[default]
    #[serde(rename = "omnigraph-engine")]
    Engine,
    #[serde(rename = "omnigraph-server")]
    Server,
}
impl Target {
    fn is_engine(&self) -> bool {
        *self == Self::Engine
    }
}
impl GqtEnvironment {
    pub fn embedded(backend: Backend) -> Self {
        Self {
            backend,
            target: Target::Engine,
            network_position: NetworkPosition::SameHost,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenSource {
    pub stem: String,
    pub text: String,
    pub sha256: String,
    /// The name the file's `--- bench` section sweeps and the value this
    /// plan reads it as; empty for a file that sweeps nothing.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub sweep: BTreeMap<String, u64>,
}
impl FrozenSource {
    pub fn parse(&self) -> Result<Case, String> {
        let case = self.parse_bound()?;
        if swept_binding(&case) != self.sweep {
            return Err("the frozen sweep binding names another name than the file sweeps".into());
        }
        Ok(case)
    }

    /// The file parsed at the frozen value of its swept name, the sweep's
    /// first when none is frozen yet.
    fn parse_bound(&self) -> Result<Case, String> {
        if self.text.len() > MAX_SOURCE_BYTES || sha256_bytes(self.text.as_bytes()) != self.sha256 {
            return Err("GQT source digest or size does not match frozen input".into());
        }
        let mut values = self.sweep.values();
        let swept = match (values.next(), values.next()) {
            (value, None) => value.copied(),
            _ => return Err("a frozen source binds at most one swept name".into()),
        };
        omnigraph_gqt_core::parse_case_at(&self.stem, &self.text, swept).map_err(String::from)
    }

    /// The same file read at another value of its swept name.
    pub fn bound_to(&self, name: &str, value: u64) -> Self {
        Self {
            sweep: BTreeMap::from([(name.to_string(), value)]),
            ..self.clone()
        }
    }
}

fn swept_binding(case: &Case) -> BTreeMap<String, u64> {
    case.swept()
        .map(|(name, value)| (name.to_string(), value))
        .into_iter()
        .collect()
}

/// How long the bench host loops a workload's measured block. A run setting:
/// the file's `--- bench` section gives the values and the command line may
/// override them, so they stay outside the point's identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlockRun {
    pub warmup_seconds: u32,
    pub duration_seconds: u32,
}

impl BlockRun {
    /// Whether the run is one a `--- bench` section may state: a warmup of
    /// 0 to `MAX_WARMUP_SECONDS`, a duration of 1 to `MAX_DURATION_SECONDS`.
    pub fn in_bounds(self) -> bool {
        self.warmup_seconds <= MAX_WARMUP_SECONDS
            && (1..=MAX_DURATION_SECONDS).contains(&self.duration_seconds)
    }

    /// The warmup, then the measured window: what the block takes by
    /// declaration.
    pub fn declared(self) -> std::time::Duration {
        std::time::Duration::from_secs(
            u64::from(self.warmup_seconds) + u64::from(self.duration_seconds),
        )
    }
}

/// The run of the workload's measured block when `selected` is that block.
fn block_run(case: &Case, selected: usize) -> Option<BlockRun> {
    case.measured_block()
        .filter(|block| block.ordinal == selected)?;
    let bench = case.bench.as_ref()?;
    Some(BlockRun {
        warmup_seconds: bench.warmup_seconds,
        duration_seconds: bench.duration_seconds,
    })
}

/// The admission of a host with neither the DST runner's gate nor the
/// reference engine, at the bench host's worker bounds.
pub(crate) fn admit_case(case: &Case) -> Result<(), String> {
    omnigraph_gqt_core::admit_ungated_case(case, "the bench host", |block| {
        if block.measured {
            MEASURED_BLOCK_WORKERS
        } else {
            SCRIPTED_HOST_WORKERS
        }
    })
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum DatasetRecipe {
    Gqt {
        source: FrozenSource,
    },
    Registered {
        reference: Box<crate::fixture_reference::NormalizedFixtureReferenceV1>,
        preparation: FrozenSource,
    },
}
/// A real dataset build request; optional queries contribute only index requirements.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatasetBuildPlan {
    pub dataset: DatasetRecipe,
    pub index_queries: Option<FrozenSource>,
    pub recipe_sha256: String,
    pub environment: GqtEnvironment,
    pub reset: ResetMode,
    pub needs_indices: bool,
}
impl DatasetBuildPlan {
    pub fn revalidate(&self) -> Result<(), String> {
        if self.environment.target != Target::Engine
            || self.environment.network_position != NetworkPosition::SameHost
        {
            return Err(
                "dataset construction requires an independently declared embedded environment"
                    .into(),
            );
        }
        let dataset = validate_recipe(&self.dataset)?;
        let query_indices = match &self.index_queries {
            Some(source) => {
                let case = source.parse()?;
                admit_case(&case)?;
                if case.fixture.is_some() {
                    return Err("index-requirement queries must be schema-less".into());
                }
                case.needs_indices
            }
            None => false,
        };
        validate_backend_reset(&self.environment.backend, self.reset)?;
        if self.needs_indices != (dataset.needs_indices || query_indices)
            || self.recipe_sha256 != recipe_hash(&self.dataset)?
        {
            return Err("dataset recipe or index requirements differ from frozen input".into());
        }
        if serde_json::to_vec(self).map_err(|e| e.to_string())?.len() > 256 * 1024 {
            return Err("frozen dataset build plan exceeds 256 KiB".into());
        }
        Ok(())
    }
    pub fn request_digest(&self) -> Result<String, String> {
        typed_sha256(self).map_err(|e| e.to_string())
    }
}
fn validate_recipe(recipe: &DatasetRecipe) -> Result<Case, String> {
    let case = match recipe {
        DatasetRecipe::Gqt { source } => {
            let c = source.parse()?;
            if c.fixture.is_none() {
                return Err("dataset requires schema and seed".into());
            }
            c
        }
        DatasetRecipe::Registered {
            reference,
            preparation,
        } => {
            let normalized =
                crate::fixture_reference::normalize_fixture_reference(reference.definition.clone())
                    .into_result()
                    .map_err(|e| format!("invalid registered reference: {e:?}"))?;
            if normalized != **reference {
                return Err("registered reference is not canonical".into());
            }
            let c = preparation.parse()?;
            if c.fixture.is_some() {
                return Err("registered preparation must be schema-less".into());
            }
            c
        }
    };
    admit_environment(&case)?;
    case.admit_lasting_store()?;
    Ok(case)
}
pub fn dataset_file(
    path: &Path,
    queries: Option<&Path>,
    backend: Backend,
    reset: ResetMode,
) -> Result<DatasetBuildPlan, String> {
    let dataset = DatasetRecipe::Gqt {
        source: read_source(path)?,
    };
    let index_queries = queries.map(read_source).transpose()?;
    let needs_indices = validate_recipe(&dataset)?.needs_indices
        || index_queries
            .as_ref()
            .map(|s| s.parse().map(|c| c.needs_indices))
            .transpose()?
            .unwrap_or(false);
    let plan = DatasetBuildPlan {
        recipe_sha256: recipe_hash(&dataset)?,
        dataset,
        index_queries,
        environment: GqtEnvironment::embedded(backend),
        reset,
        needs_indices,
    };
    plan.revalidate()?;
    Ok(plan)
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlannedGqt {
    pub definition: GqtCaseV1,
    pub dataset: DatasetRecipe,
    pub queries: FrozenSource,
    pub recipe_sha256: String,
    pub case_digest: String,
    pub planned_sha256: String,
    pub cache_condition: CacheCondition,
    pub needs_indices: bool,
    /// The run of the measured block, when the measured step is one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block: Option<BlockRun>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GqtPointIdentityV1 {
    pub identity_version: u32,
    pub scenario: GqtScenario,
    pub dataset_recipe_sha256: String,
    pub dataset_logical_digest: String,
    pub dataset_identity_algorithm: String,
    pub queries_sha256: String,
    pub measured_step: MeasuredStep,
    pub cache_condition: CacheCondition,
    pub environment: GqtEnvironment,
    pub protocol: Protocol,
    /// The swept name and the value this point read it as. Absent on a point
    /// whose workload sweeps nothing, so no such point's id moved when the
    /// field arrived.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub sweep: BTreeMap<String, u64>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BoundGqt {
    pub plan: PlannedGqt,
    pub identity: GqtPointIdentityV1,
    pub point_id: String,
    pub point_name: String,
}
impl PlannedGqt {
    pub fn dataset_build_plan(&self) -> Result<DatasetBuildPlan, String> {
        match self.definition.environment.target {
            Target::Engine => Ok(self.build_plan_in(
                self.definition.environment.clone(),
                self.definition.protocol.reset,
            )),
            Target::Server => Err(
                "served scenarios run against a provisioned graph; the dataset cache does not apply"
                    .into(),
            ),
        }
    }

    /// The build of the frozen base a `reset: rebuild` server point binds its
    /// dataset identity to and counts its seeded tables against: the base an
    /// embedded clone point of the same scenario and backend reads.
    pub fn base_build_plan(&self) -> Result<DatasetBuildPlan, String> {
        let environment = &self.definition.environment;
        if environment.target != Target::Server
            || self.definition.protocol.reset != ResetMode::Rebuild
        {
            return Err(
                "only a server point under reset: rebuild binds a base it does not read".into(),
            );
        }
        Ok(self.build_plan_in(
            GqtEnvironment::embedded(environment.backend.clone()),
            ResetMode::Clone,
        ))
    }

    fn build_plan_in(&self, environment: GqtEnvironment, reset: ResetMode) -> DatasetBuildPlan {
        DatasetBuildPlan {
            dataset: self.dataset.clone(),
            index_queries: Some(self.queries.clone()),
            recipe_sha256: self.recipe_sha256.clone(),
            environment,
            reset,
            needs_indices: self.needs_indices,
        }
    }
    /// A workload that sweeps nothing hashes as it always did; a swept value
    /// joins the hash, so every value plans a point of its own.
    pub fn planned_hash(&self) -> Result<String, String> {
        let point = (
            &self.recipe_sha256,
            &self.queries.sha256,
            &self.definition.workload.measured_step,
            &self.cache_condition,
            &self.definition.environment,
            &self.definition.protocol,
        );
        let hashed = if self.queries.sweep.is_empty() {
            typed_sha256(&point)
        } else {
            typed_sha256(&(point, &self.queries.sweep))
        };
        hashed.map_err(|e| e.to_string())
    }

    /// The plan with the measured block's run replaced by the command line's
    /// values, each kept where none is given.
    pub fn with_block_run(
        mut self,
        warmup_seconds: Option<u32>,
        duration_seconds: Option<u32>,
    ) -> Result<Self, String> {
        if warmup_seconds.is_none() && duration_seconds.is_none() {
            return Ok(self);
        }
        let Some(run) = self.block.as_mut() else {
            return Err(format!(
                "scenario '{}' measures no `--- concurrent measured` block, so it takes no warmup or duration",
                self.definition.id
            ));
        };
        run.warmup_seconds = warmup_seconds.unwrap_or(run.warmup_seconds);
        run.duration_seconds = duration_seconds.unwrap_or(run.duration_seconds);
        self.revalidate()?;
        Ok(self)
    }

    /// `budget` for the measured step, counted past what the step takes by
    /// declaration: a measured block's warmup and duration come first, so the
    /// case's `timeout_ms` and the scenario's deadline bound the rest of the
    /// file and the block's drain, whatever run the command line gave it.
    pub fn past_declared_run(&self, budget: std::time::Duration) -> std::time::Duration {
        budget
            + self
                .block
                .map_or(std::time::Duration::ZERO, BlockRun::declared)
    }

    pub fn bind(&self, logical: &str, algorithm: &str) -> Result<BoundGqt, String> {
        if !digest(logical) {
            return Err("invalid dataset logical digest".into());
        }
        let identity = GqtPointIdentityV1 {
            identity_version: 1,
            scenario: GqtScenario::GqtV1,
            dataset_recipe_sha256: self.recipe_sha256.clone(),
            dataset_logical_digest: logical.into(),
            dataset_identity_algorithm: algorithm.into(),
            queries_sha256: self.queries.sha256.clone(),
            measured_step: self.definition.workload.measured_step.clone(),
            cache_condition: self.cache_condition.clone(),
            environment: self.definition.environment.clone(),
            protocol: self.definition.protocol.clone(),
            sweep: self.queries.sweep.clone(),
        };
        validate_point_spec(&identity)?;
        let point_id = typed_sha256(&identity).map_err(|e| e.to_string())?;
        Ok(BoundGqt {
            plan: self.clone(),
            point_name: format!(
                "gqt-{}-{}",
                self.cache_condition.display_label(),
                &point_id[..12]
            ),
            identity,
            point_id,
        })
    }
    pub fn revalidate(&self) -> Result<(), String> {
        validate_definition(&self.definition)?;
        let queries = self.queries.parse()?;
        let dataset = validate_recipe(&self.dataset)?;
        let condition = admit_point_queries(
            &queries,
            &self.definition.workload.measured_step,
            self.definition.environment.target,
            self.definition.protocol.reset,
        )?;
        if self.definition.environment.target == Target::Server {
            if matches!(self.dataset, DatasetRecipe::Registered { .. }) || self.needs_indices {
                return Err(
                    "served benchmarks do not support registered or indexed datasets".into(),
                );
            }
            omnigraph_gqt_core::admit_served(&dataset)?;
        }
        if condition != self.cache_condition
            || (dataset.needs_indices || queries.needs_indices) != self.needs_indices
        {
            return Err("derived GQT treatment differs from frozen plan".into());
        }
        let measured = block_run(&queries, self.definition.workload.measured_step.ordinal);
        if self.block.is_some() != measured.is_some()
            || self.block.is_some_and(|run| !run.in_bounds())
        {
            return Err(format!(
                "the measured block's run is `warmup_seconds` 0 to {MAX_WARMUP_SECONDS} and `duration_seconds` 1 to {MAX_DURATION_SECONDS}, on a plan whose measured step is that block and on no other"
            ));
        }
        if self.planned_hash()? != self.planned_sha256
            || recipe_hash(&self.dataset)? != self.recipe_sha256
            || typed_sha256(&self.definition).map_err(|e| e.to_string())? != self.case_digest
        {
            return Err("frozen case/recipe identity mismatch".into());
        }
        if serde_json::to_vec(self).map_err(|e| e.to_string())?.len() > 256 * 1024 {
            return Err("frozen GQT plan exceeds 256 KiB".into());
        }
        let bound = self.bind(
            &"0".repeat(64),
            crate::dataset_identity::DATASET_LOGICAL_ALGORITHM,
        )?;
        if serde_json::to_vec(&bound).map_err(|e| e.to_string())?.len() > 512 * 1024 {
            return Err("bound GQT request exceeds reserved frame budget".into());
        }
        Ok(())
    }
}
impl BoundGqt {
    pub fn revalidate(&self) -> Result<(), String> {
        self.plan.revalidate()?;
        if self.plan.bind(
            &self.identity.dataset_logical_digest,
            &self.identity.dataset_identity_algorithm,
        )? != *self
        {
            return Err("bound point identity mismatch".into());
        }
        Ok(())
    }
}
pub fn validate_definition(c: &GqtCaseV1) -> Result<(), String> {
    validate_run(c.version, &c.id, &c.environment, &c.protocol)?;
    validate_step(&c.workload.measured_step)
}

/// Everything [`validate_definition`] checks but the measured step.
fn validate_run(
    version: u32,
    id: &str,
    environment: &GqtEnvironment,
    protocol: &Protocol,
) -> Result<(), String> {
    if version != 1 || !valid_kebab_id(id) || id.len() > 128 {
        return Err("gqt-v1 requires version 1 and a bounded kebab-case id".into());
    }
    if protocol.schedule != Schedule::Manual
        || protocol.reset == ResetMode::S3Versioning
        || protocol
            .deadline_seconds
            .is_some_and(|x| x == 0 || x > 3600)
    {
        return Err(
            "gqt-v1 requires a manual local reset and a null or 1..=3600 second deadline".into(),
        );
    }
    if matches!(environment.backend, Backend::S3 { .. })
        && !matches!(
            (environment.target, protocol.reset),
            (Target::Engine, ResetMode::Clone) | (Target::Server, ResetMode::Rebuild)
        )
    {
        return Err(
            "gqt-v1 reaches an S3 store embedded under reset: clone, or through a server started per repetition under reset: rebuild".into(),
        );
    }
    match environment.target {
        Target::Engine => {
            validate_backend_reset(&environment.backend, protocol.reset)?;
            if environment.network_position != NetworkPosition::SameHost {
                return Err("embedded execution requires same-host network position".into());
            }
        }
        Target::Server => {
            if !matches!(protocol.reset, ResetMode::None | ResetMode::Rebuild)
                || protocol.attribution != crate::case::Attribution::Off
            {
                return Err(
                    "served execution requires reset: none (a provisioned graph) or reset: rebuild (a server started per repetition), and attribution: off".into(),
                );
            }
            if protocol.reset == ResetMode::Rebuild
                && environment.network_position != NetworkPosition::SameHost
            {
                return Err(
                    "reset: rebuild starts the server beside the bench and requires network_position: same-host".into(),
                );
            }
        }
    }
    Ok(())
}

fn validate_step(step: &MeasuredStep) -> Result<(), String> {
    if step.ordinal == 0 || step.text.trim().is_empty() || step.text.len() > 16 * 1024 {
        return Err("measured_step requires a positive ordinal and exact operation text".into());
    }
    Ok(())
}
fn recipe_hash(recipe: &DatasetRecipe) -> Result<String, String> {
    let identity = match recipe {
        DatasetRecipe::Gqt { source } => {
            serde_json::json!({"format":"gqt-dataset-recipe-v1","dataset_sha256":source.sha256})
        }
        DatasetRecipe::Registered {
            reference,
            preparation,
        } => {
            serde_json::json!({"format":"gqt-registered-recipe-v1","logical":reference.definition.logical,"expected":reference.definition.expected,"preparation_sha256":preparation.sha256})
        }
    };
    typed_sha256(&identity).map_err(|e| e.to_string())
}
pub fn load(path: &Path, definition: GqtCaseV1) -> Result<PlannedGqt, String> {
    validate_definition(&definition)?;
    let parent = path.parent().ok_or("case path has no parent")?;
    let catalog = parent
        .ancestors()
        .find(|p| p.file_name().is_some_and(|n| n == "cases"))
        .and_then(Path::parent)
        .unwrap_or(parent)
        .canonicalize()
        .map_err(|e| e.to_string())?;
    load_with_root(path, definition, &catalog)
}

/// Resolve config-relative inputs within the caller's explicit catalog boundary.
pub fn load_with_root(
    path: &Path,
    definition: GqtCaseV1,
    root: &Path,
) -> Result<PlannedGqt, String> {
    validate_definition(&definition)?;
    let mut points = load_points(path, definition.into(), root)?.plans;
    let point = points.swap_remove(0);
    admit_one_point(&point.queries.parse()?)?;
    Ok(point)
}

/// Refuses a workload that sweeps several values on a route that plans one
/// point: only a catalog scenario plans a point per value.
fn admit_one_point(queries: &Case) -> Result<(), String> {
    match queries
        .bench
        .as_ref()
        .and_then(|bench| bench.sweep.as_ref())
    {
        Some(sweep) if sweep.values.len() > 1 => Err(format!(
            "the workload sweeps `{}` over {} values and this route plans one point; run it as a catalog scenario, which plans one point per value",
            sweep.name,
            sweep.values.len()
        )),
        _ => Ok(()),
    }
}

/// The points one scenario plans, in the order of the swept values, and the
/// repetitions its workload's `--- bench` section asks for.
#[derive(Debug, Clone)]
pub struct ScenarioPoints {
    pub plans: Vec<PlannedGqt>,
    pub repetitions: Option<u32>,
}

/// The step a workload's `--- concurrent measured` block is, as a catalog
/// entry would echo it: its ordinal and its text as written.
fn measured_block_step(case: &Case) -> Result<MeasuredStep, String> {
    let block = case.measured_block().ok_or(
        "the scenario names no measured_step and its workload holds no `--- concurrent measured` block",
    )?;
    let step = workload_steps(case)?
        .into_iter()
        .find(|step| step.ordinal == block.ordinal)
        .ok_or("the measured block has no step descriptor")?;
    Ok(MeasuredStep {
        ordinal: step.ordinal,
        text: step.source,
    })
}

/// Every point of `case`, its inputs resolved against `root`: one per value
/// its workload sweeps, one for a workload that sweeps nothing.
pub fn load_points(path: &Path, case: ScenarioCase, root: &Path) -> Result<ScenarioPoints, String> {
    case.validate()?;
    let parent = path.parent().ok_or("config path has no parent")?;
    let catalog = root.canonicalize().map_err(|e| e.to_string())?;
    let queries = read_source(&resolve(parent, &case.queries, &catalog)?)?;
    let first = queries.parse()?;
    let step = match case.step.clone() {
        Some(step) => step,
        None => measured_block_step(&first)?,
    };
    let definition = case.measuring(step);
    validate_definition(&definition)?;
    let sweep = first.bench.as_ref().and_then(|bench| bench.sweep.as_ref());
    let sources: Vec<FrozenSource> = match sweep {
        Some(sweep) => sweep
            .values
            .iter()
            .map(|value| queries.bound_to(&sweep.name, *value))
            .collect(),
        None => vec![queries],
    };
    let dataset = match &definition.fixture {
        GqtFixture::Dataset { path } => DatasetRecipe::Gqt {
            source: read_source(&resolve(parent, path, &catalog)?)?,
        },
        GqtFixture::Registered {
            reference,
            preparation,
        } => DatasetRecipe::Registered {
            reference: Box::new(
                crate::fixture_reference::load_fixture_reference(&resolve(
                    parent, reference, &catalog,
                )?)
                .into_result()
                .map_err(|e| format!("{e:?}"))?,
            ),
            preparation: read_source(&resolve(parent, preparation, &catalog)?)?,
        },
    };
    let parsed_dataset = match &dataset {
        DatasetRecipe::Gqt { source } => source.parse()?,
        DatasetRecipe::Registered { preparation, .. } => preparation.parse()?,
    };
    let case_digest = typed_sha256(&definition).map_err(|e| e.to_string())?;
    let recipe_sha256 = recipe_hash(&dataset)?;
    let mut plans = Vec::with_capacity(sources.len());
    for queries in sources {
        let parsed_queries = queries.parse()?;
        let mut plan = PlannedGqt {
            planned_sha256: String::new(),
            case_digest: case_digest.clone(),
            recipe_sha256: recipe_sha256.clone(),
            cache_condition: admit_point_queries(
                &parsed_queries,
                &definition.workload.measured_step,
                definition.environment.target,
                definition.protocol.reset,
            )?,
            needs_indices: parsed_queries.needs_indices || parsed_dataset.needs_indices,
            block: block_run(&parsed_queries, definition.workload.measured_step.ordinal),
            definition: definition.clone(),
            dataset: dataset.clone(),
            queries,
        };
        plan.planned_sha256 = plan.planned_hash()?;
        plan.revalidate()?;
        if serde_json::to_vec(&plan).map_err(|e| e.to_string())?.len() > 256 * 1024 {
            return Err("frozen GQT inputs exceed the bounded worker protocol budget".into());
        }
        plans.push(plan);
    }
    Ok(ScenarioPoints {
        plans,
        repetitions: first.bench.as_ref().and_then(|bench| bench.repetitions),
    })
}
/// The file frozen at the first value of the name its `--- bench` section
/// sweeps, which is what a bare parse reads.
pub fn read_source(path: &Path) -> Result<FrozenSource, String> {
    let text = read_text_file(path, MAX_SOURCE_BYTES, "GQT").map_err(|e| e.message)?;
    let stem = path
        .file_stem()
        .and_then(|x| x.to_str())
        .ok_or("GQT filename must be UTF-8")?
        .into();
    let mut source = FrozenSource {
        stem,
        sha256: sha256_bytes(text.as_bytes()),
        text,
        sweep: BTreeMap::new(),
    };
    source.sweep = swept_binding(&source.parse_bound()?);
    Ok(source)
}
fn resolve(parent: &Path, path: &Path, root: &Path) -> Result<PathBuf, String> {
    if path.is_absolute() {
        return Err("catalog sources must use relative paths".into());
    }
    let resolved = parent
        .join(path)
        .canonicalize()
        .map_err(|e| format!("{}: {e}", path.display()))?;
    if !resolved.starts_with(root) {
        return Err("GQT source escapes the catalog".into());
    }
    Ok(resolved)
}
/// Inspect parser descriptors only after bounding workload expansion.
pub fn workload_steps(case: &Case) -> Result<Vec<StepDescriptor>, String> {
    let count = case.items.iter().try_fold(0usize, |total, item| {
        let count = match item {
            Item::Step(_) => Some(1),
            Item::Loop { steps, values, .. } => steps.len().checked_mul(values.len()),
        }?;
        total.checked_add(count)
    });
    if count.is_none_or(|count| count > MAX_EXPANDED_STEPS) {
        return Err(format!(
            "workload exceeds {MAX_EXPANDED_STEPS} expanded steps"
        ));
    }
    Ok(case.steps())
}
pub fn admit_queries(case: &Case, selected: &MeasuredStep) -> Result<CacheCondition, String> {
    admit_target_queries(case, selected, Target::Engine)
}
pub(crate) fn admit_target_queries(
    case: &Case,
    selected: &MeasuredStep,
    target: Target,
) -> Result<CacheCondition, String> {
    admit_point_queries(case, selected, target, ResetMode::None)
}
/// [`admit_target_queries`] for a point whose reset level is known: under
/// the clone reset every step is its own `omnigraph` process, which the
/// process route's admission and the derived cache condition both state.
pub(crate) fn admit_point_queries(
    case: &Case,
    selected: &MeasuredStep,
    target: Target,
    reset: ResetMode,
) -> Result<CacheCondition, String> {
    admit_target_environment(case, target)?;
    let process = match (target, reset) {
        (Target::Server, ResetMode::Rebuild) => ProcessLifecycle::FreshPerRepetition,
        (Target::Server, _) => ProcessLifecycle::LongRunningServer,
        (Target::Engine, ResetMode::Clone) => ProcessLifecycle::FreshPerStep,
        (Target::Engine, _) => ProcessLifecycle::FreshPerRepetition,
    };
    if process == ProcessLifecycle::FreshPerStep {
        omnigraph_gqt_core::admit_process_workload(case, selected.ordinal)?;
    }
    if target == Target::Server {
        omnigraph_gqt_core::admit_served_workload(case, selected.ordinal)?;
        workload_steps(case)?;
        for step in case.items.iter().flat_map(|item| match item {
            Item::Step(step) => std::slice::from_ref(step),
            Item::Loop { steps, .. } => steps.as_slice(),
        }) {
            let ordinal = step.ordinal();
            if reset != ResetMode::Rebuild
                && let Some(refusal) = served_step_refusal(step, selected.ordinal)
            {
                return Err(format!(
                    "step {ordinal} ({}) {refusal}",
                    served_step_label(step)
                ));
            }
            if let Step::Query(query) = step {
                let file = parse_query(&query.source).map_err(|e| e.to_string())?;
                if !file.settings.is_empty() {
                    return Err(format!(
                        "step {ordinal} (query with a settings prefix) is refused for served acquisition until the served door is proven by a conformance case"
                    ));
                }
            }
        }
    }
    case.admit_lasting_store()?;
    if case.fixture.is_some() {
        return Err("queries must omit schema and seed".into());
    }
    let descriptors = workload_steps(case)?;
    let step = descriptors
        .iter()
        .find(|s| s.ordinal == selected.ordinal)
        .ok_or("measured ordinal does not exist")?;
    if step.in_loop || step.source.trim() != selected.text.trim() {
        return Err("measured step moved, its text changed, or it is inside a loop".into());
    }
    match case.measured_block() {
        Some(block) if block.ordinal != selected.ordinal => {
            return Err(format!(
                "the workload's `--- concurrent measured` block is step {}, and the measured step is step {}; a workload's measured block is its measured step",
                block.ordinal, selected.ordinal
            ));
        }
        Some(_)
            if match target {
                Target::Engine => reset == ResetMode::Clone,
                Target::Server => reset != ResetMode::Rebuild,
            } =>
        {
            return Err(
                "a `--- concurrent measured` block runs embedded on a restored store, or against a server started per repetition (reset: rebuild); a provisioned server and the clone reset level do not run it"
                    .into(),
            );
        }
        Some(block) if target == Target::Server && block.sessions.len() > SERVED_BLOCK_SESSIONS => {
            return Err(format!(
                "a measured block against a server runs at most {SERVED_BLOCK_SESSIONS} sessions, each an actor with a token of its own; step {} declares {}",
                block.ordinal,
                block.sessions.len()
            ));
        }
        Some(_) => {}
        None if matches!(
            step.kind,
            StepKind::Settings | StepKind::Show | StepKind::Concurrent
        ) =>
        {
            return Err(
                "selected step must execute one query, load, or control/mutation operation, or be the workload's `--- concurrent measured` block"
                    .into(),
            );
        }
        None => {}
    }
    let mut reads = 0u32;
    let mut reopened = false;
    let mut suffix = 0usize;
    for item in &case.items {
        let (steps, times) = match item {
            Item::Step(s) => (std::slice::from_ref(s), 1usize),
            Item::Loop { steps, values, .. } => (steps.as_slice(), values.len()),
        };
        for s in steps {
            if s.ordinal() == selected.ordinal {
                if let Step::Load(load) = s {
                    if load.call_count() != 1 {
                        return Err(
                            "measured generated load must contain exactly one loader call".into(),
                        );
                    }
                }
                continue;
            }
            if s.ordinal() > selected.ordinal {
                if explicit_verification(s.operation_kind()) {
                    suffix += times;
                }
                continue;
            }
            match s.operation_kind() {
                StepKind::Query | StepKind::BranchList | StepKind::BranchRead => {
                    if reopened {
                        return Err(
                            "reads after preparation restart are not post-reopen treatment".into(),
                        );
                    }
                    reads = reads
                        .checked_add(u32::try_from(times).map_err(|_| "warmup count overflow")?)
                        .ok_or("warmup count overflow")?;
                }
                StepKind::Settings | StepKind::Show => {}
                StepKind::Restart => {
                    if reopened || reads == 0 || times != 1 {
                        return Err(
                            "post-reopen requires reads followed by exactly one restart".into()
                        );
                    }
                    reopened = true;
                }
                _ => {
                    return Err(
                        "writes before the measured step are forbidden; move them into the dataset"
                            .into(),
                    );
                }
            }
        }
    }
    if suffix == 0 {
        return Err(
            "queries require explicit verification steps after the measured operation".into(),
        );
    }
    Ok(CacheCondition {
        process,
        engine: engine_preparation(reopened, reads, process),
        page_cache: if reads > 0 && target == Target::Engine {
            PageCacheCondition::ProgramConditioned
        } else {
            PageCacheCondition::Uncontrolled
        },
        program: if reads > 0 {
            WarmupProgram::GqtReadSetV1
        } else {
            WarmupProgram::None
        },
        iterations: reads,
    })
}
/// Explicit CLI inputs are frozen by their supplied paths and need not live in a catalog.
pub fn override_sources(
    mut plan: PlannedGqt,
    dataset: Option<&Path>,
    queries: Option<&Path>,
) -> Result<PlannedGqt, String> {
    if let Some(path) = dataset {
        plan.dataset = DatasetRecipe::Gqt {
            source: read_source(path)?,
        };
        plan.definition.fixture = GqtFixture::Dataset {
            path: path.to_path_buf(),
        };
    }
    if let Some(path) = queries {
        plan.queries = read_source(path)?;
        plan.definition.workload.queries = path.to_path_buf();
    }
    let parsed = plan.queries.parse()?;
    admit_one_point(&parsed)?;
    let dataset = match &plan.dataset {
        DatasetRecipe::Gqt { source } => source.parse()?,
        DatasetRecipe::Registered { preparation, .. } => preparation.parse()?,
    };
    plan.cache_condition = admit_point_queries(
        &parsed,
        &plan.definition.workload.measured_step,
        plan.definition.environment.target,
        plan.definition.protocol.reset,
    )?;
    plan.block = block_run(&parsed, plan.definition.workload.measured_step.ordinal);
    plan.needs_indices = dataset.needs_indices || parsed.needs_indices;
    plan.recipe_sha256 = recipe_hash(&plan.dataset)?;
    plan.case_digest = typed_sha256(&plan.definition).map_err(|e| e.to_string())?;
    plan.planned_sha256 = plan.planned_hash()?;
    plan.revalidate()?;
    if serde_json::to_vec(&plan).map_err(|e| e.to_string())?.len() > 256 * 1024 {
        return Err("frozen inputs exceed 256 KiB protocol budget".into());
    }
    Ok(plan)
}

/// The read step kinds a served workload may contain anywhere in its program
/// without further condition; `served_step_refusal` admits the writes.
pub const SERVED_READ_KINDS: [StepKind; 4] = [
    StepKind::Query,
    StepKind::BranchList,
    StepKind::BranchRead,
    StepKind::Show,
];

/// The step's name in a served refusal: the control statement's own two
/// words, since `step_kind` says only `control` for all five of them.
fn served_step_label(step: &Step) -> &'static str {
    match step.operation_kind() {
        StepKind::BranchCreate => "branch create",
        StepKind::BranchDelete => "branch delete",
        StepKind::BranchMerge => "branch merge",
        StepKind::CommitPublish => "commit publish",
        StepKind::CommitDrop => "commit drop",
        _ => step_kind(step),
    }
}

/// Why a step is refused served, or `None` when it is admitted. A served
/// graph is provisioned once and shared by every repetition, so a step may
/// read, write an unpublished commit (`as commit`, which advances no head),
/// or drop a commit this workload bound; anything that moves a head, loads,
/// or publishes would change the graph for the next repetition. A restart
/// after the measured step is the long-running lifecycle's no-op.
pub(crate) fn served_step_refusal(step: &Step, measured: usize) -> Option<String> {
    let kind = step.operation_kind();
    if SERVED_READ_KINDS.contains(&kind) {
        return None;
    }
    match kind {
        StepKind::Restart if step.ordinal() > measured => None,
        StepKind::Mutate | StepKind::BranchMerge if step.as_commit() => None,
        StepKind::Mutate | StepKind::BranchMerge => Some(
            "advances a branch head; reset:none admits a served write only as an unpublished commit (`as commit`), verified at its snapshot and dropped".into(),
        ),
        StepKind::CommitDrop if step.drops_bound_commit() => None,
        StepKind::CommitDrop => Some(
            "drops a commit this workload did not bind; a served drop names a `--- bind commit $name` of this workload".into(),
        ),
        StepKind::Concurrent => Some(
            "writes the graph every repetition shares; a measured block runs against a server started per repetition (reset: rebuild)".into(),
        ),
        _ => Some(
            "is not admissible served; reset:none requires every served workload step, including loops and verification, to be a read, an `as commit` write, or the drop of a bound commit".into(),
        ),
    }
}

/// Engine preparation implied by the executed prefix reads, a restart, and the process lifecycle.
pub(crate) fn engine_preparation(
    reopened: bool,
    reads: u32,
    process: ProcessLifecycle,
) -> EnginePreparation {
    match (reopened, reads, process) {
        (true, _, _) | (false, 1.., ProcessLifecycle::FreshPerStep) => {
            EnginePreparation::ReopenedAfterProgram
        }
        (false, 1.., _) => EnginePreparation::WarmedByProgram,
        (false, 0, ProcessLifecycle::LongRunningServer) => EnginePreparation::Uncontrolled,
        (false, 0, ProcessLifecycle::FreshPerRepetition | ProcessLifecycle::FreshPerStep) => {
            EnginePreparation::PreparationOnly
        }
    }
}

pub fn explicit_verification(kind: StepKind) -> bool {
    !matches!(
        kind,
        StepKind::Restart | StepKind::Settings | StepKind::Concurrent
    )
}
pub(crate) fn admit_environment(case: &Case) -> Result<(), String> {
    admit_target_environment(case, Target::Engine)
}
fn admit_target_environment(case: &Case, target: Target) -> Result<(), String> {
    admit_case(case)?;
    if !case.runner.environments.iter().any(|e| {
        matches!(
            (&e.execution, target),
            (
                omnigraph_gqt_core::Execution::Engine {
                    storage: omnigraph_gqt_core::runner_config::Storage::LocalFilesystem
                },
                Target::Engine
            ) | (
                omnigraph_gqt_core::Execution::Server {
                    storage: omnigraph_gqt_core::runner_config::Storage::LocalFilesystem
                },
                Target::Server
            )
        )
    }) {
        return Err(
            "benchmark target requires a matching admitted local-filesystem environment".into(),
        );
    }
    Ok(())
}
pub fn validate_point_spec(spec: &GqtPointIdentityV1) -> Result<(), String> {
    let synthetic = GqtCaseV1 {
        version: 1,
        id: "point-validation".into(),
        scenario: GqtScenario::GqtV1,
        fixture: GqtFixture::Dataset {
            path: "dataset.gqt".into(),
        },
        workload: GqtWorkload {
            queries: "queries.gqt".into(),
            measured_step: spec.measured_step.clone(),
        },
        environment: spec.environment.clone(),
        protocol: spec.protocol.clone(),
    };
    validate_definition(&synthetic)?;
    let c = &spec.cache_condition;
    let server = spec.environment.target == Target::Server;
    let valid = if server && spec.protocol.reset == ResetMode::Rebuild {
        c.process == ProcessLifecycle::FreshPerRepetition
            && c.page_cache == PageCacheCondition::Uncontrolled
            && matches!(
                (c.engine, c.program, c.iterations),
                (EnginePreparation::PreparationOnly, WarmupProgram::None, 0)
                    | (
                        EnginePreparation::WarmedByProgram,
                        WarmupProgram::GqtReadSetV1,
                        1..=4096
                    )
            )
    } else if server {
        c.process == ProcessLifecycle::LongRunningServer
            && c.page_cache == PageCacheCondition::Uncontrolled
            && matches!(
                (c.engine, c.program, c.iterations),
                (EnginePreparation::Uncontrolled, WarmupProgram::None, 0)
                    | (
                        EnginePreparation::WarmedByProgram,
                        WarmupProgram::GqtReadSetV1,
                        1..=4096
                    )
            )
    } else if spec.protocol.reset == ResetMode::Clone {
        c.process == ProcessLifecycle::FreshPerStep
            && matches!(
                (c.engine, c.page_cache, c.program, c.iterations),
                (
                    EnginePreparation::PreparationOnly,
                    PageCacheCondition::Uncontrolled,
                    WarmupProgram::None,
                    0,
                ) | (
                    EnginePreparation::ReopenedAfterProgram,
                    PageCacheCondition::ProgramConditioned,
                    WarmupProgram::GqtReadSetV1,
                    1..=4096,
                )
            )
    } else {
        c.process == ProcessLifecycle::FreshPerRepetition
            && matches!(
                (c.engine, c.page_cache, c.program, c.iterations),
                (
                    EnginePreparation::PreparationOnly,
                    PageCacheCondition::Uncontrolled,
                    WarmupProgram::None,
                    0,
                ) | (
                    EnginePreparation::WarmedByProgram | EnginePreparation::ReopenedAfterProgram,
                    PageCacheCondition::ProgramConditioned,
                    WarmupProgram::GqtReadSetV1,
                    1..=4096,
                )
            )
    };
    if !valid {
        return Err("invalid GQT cache condition tuple".into());
    }
    for d in [
        &spec.dataset_recipe_sha256,
        &spec.dataset_logical_digest,
        &spec.queries_sha256,
    ] {
        if !digest(d) {
            return Err("invalid point content digest".into());
        }
    }
    if spec.sweep.len() > 1 {
        return Err("a point binds at most one swept name".into());
    }
    if spec.identity_version != 1
        || !matches!(
            spec.dataset_identity_algorithm.as_str(),
            crate::dataset_identity::DATASET_LOGICAL_ALGORITHM
                | crate::dataset_identity::REGISTERED_LOGICAL_ALGORITHM
        )
    {
        return Err("unsupported GQT point identity version or logical domain".into());
    }
    let run_spec_json = serde_json::to_string(spec).map_err(|e| e.to_string())?;
    let projected = serde_json::json!({"point_id":"0".repeat(64),"point_name":"gqt-post-reopen-000000000000","point_identity_version":1,"scenario":"gqt-v1","run_spec_json":run_spec_json});
    if serde_json::to_vec(&projected)
        .map_err(|e| e.to_string())?
        .len()
        > 64 * 1024
    {
        return Err("point identity exceeds projection row budget".into());
    }
    Ok(())
}

pub fn explicit_pair(
    dataset: &Path,
    queries: &Path,
    selected: MeasuredStep,
    backend: Backend,
    reset: ResetMode,
    deadline_seconds: Option<u64>,
) -> Result<PlannedGqt, String> {
    explicit_pair_in_environment(
        dataset,
        queries,
        selected,
        GqtEnvironment::embedded(backend),
        reset,
        deadline_seconds,
    )
}

pub fn explicit_pair_in_environment(
    dataset: &Path,
    queries: &Path,
    selected: MeasuredStep,
    environment: GqtEnvironment,
    reset: ResetMode,
    deadline_seconds: Option<u64>,
) -> Result<PlannedGqt, String> {
    let dataset_source = read_source(dataset)?;
    let query_source = read_source(queries)?;
    let definition = GqtCaseV1 {
        version: 1,
        id: "explicit-gqt-pair".into(),
        scenario: GqtScenario::GqtV1,
        fixture: GqtFixture::Dataset {
            path: dataset.to_path_buf(),
        },
        workload: GqtWorkload {
            queries: queries.to_path_buf(),
            measured_step: selected,
        },
        protocol: Protocol {
            deadline_seconds,
            attribution: if environment.target == Target::Server {
                crate::case::Attribution::Off
            } else {
                crate::case::Attribution::PerPhase
            },
            schedule: Schedule::Manual,
            reset,
            timer: crate::case::Timer::Monotonic,
        },
        environment,
    };
    let query_case = query_source.parse()?;
    admit_one_point(&query_case)?;
    let dataset_case = dataset_source.parse()?;
    let recipe = DatasetRecipe::Gqt {
        source: dataset_source,
    };
    let mut plan = PlannedGqt {
        case_digest: typed_sha256(&definition).map_err(|e| e.to_string())?,
        recipe_sha256: recipe_hash(&recipe)?,
        planned_sha256: String::new(),
        cache_condition: admit_point_queries(
            &query_case,
            &definition.workload.measured_step,
            definition.environment.target,
            definition.protocol.reset,
        )?,
        needs_indices: query_case.needs_indices || dataset_case.needs_indices,
        block: block_run(&query_case, definition.workload.measured_step.ordinal),
        definition,
        dataset: recipe,
        queries: query_source,
    };
    plan.planned_sha256 = plan.planned_hash()?;
    plan.revalidate()?;
    Ok(plan)
}

/// The admitted embedded pairs: the two restore levels on their filesystem,
/// and `clone` on any local filesystem or S3 store.
fn validate_backend_reset(backend: &Backend, reset: ResetMode) -> Result<(), String> {
    use crate::case::{LocalFilesystem, LocalStorageClass};
    if !matches!(
        (backend, reset),
        (
            Backend::LocalFs {
                filesystem: LocalFilesystem::Apfs,
                storage_class: LocalStorageClass::NvmeSsd
            },
            ResetMode::LocalClonefile
        ) | (
            Backend::LocalFs {
                filesystem: LocalFilesystem::Xfs,
                storage_class: LocalStorageClass::NvmeSsd
            },
            ResetMode::PlainCopy
        ) | (
            Backend::LocalFs { .. } | Backend::S3 { .. },
            ResetMode::Clone
        )
    ) {
        return Err(
            "reset and declared backend do not match an admitted embedded environment".into(),
        );
    }
    Ok(())
}
