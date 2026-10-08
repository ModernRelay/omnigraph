//! Authored GQT experiments. Recipe identity is resolved before I/O; point
//! identity is bound only after the dataset's logical evidence is verified.
use crate::case::{
    Backend, CacheCondition, EnginePreparation, PageCacheCondition, ProcessLifecycle, Protocol,
    ResetMode, Schedule, WarmupProgram,
};
use crate::model::{read_text_file, typed_sha256, valid_kebab_id};
use omnigraph_gqt_core::{Case, ExecutionHost, Item, PlainHost, Step, StepDescriptor, StepKind};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

pub const MAX_SOURCE_BYTES: usize = 512 * 1024;
pub const MAX_EXPANDED_STEPS: usize = 4096;

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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GqtEnvironment {
    pub backend: Backend,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenSource {
    pub stem: String,
    pub text: String,
    pub sha256: String,
}
impl FrozenSource {
    pub fn parse(&self) -> Result<Case, String> {
        if self.text.len() > MAX_SOURCE_BYTES || sha256_bytes(self.text.as_bytes()) != self.sha256 {
            return Err("GQT source digest or size does not match frozen input".into());
        }
        omnigraph_gqt_core::parse_case(&self.stem, &self.text)
    }
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
        let dataset = validate_recipe(&self.dataset)?;
        let query_indices = match &self.index_queries {
            Some(source) => {
                let case = source.parse()?;
                admit_environment(&case)?;
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
        environment: GqtEnvironment { backend },
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
    pub fn dataset_build_plan(&self) -> DatasetBuildPlan {
        DatasetBuildPlan {
            dataset: self.dataset.clone(),
            index_queries: Some(self.queries.clone()),
            recipe_sha256: self.recipe_sha256.clone(),
            environment: self.definition.environment.clone(),
            reset: self.definition.protocol.reset,
            needs_indices: self.needs_indices,
        }
    }
    pub fn planned_hash(&self) -> Result<String, String> {
        typed_sha256(&(
            &self.recipe_sha256,
            &self.queries.sha256,
            &self.definition.workload.measured_step,
            &self.cache_condition,
            &self.definition.environment,
            &self.definition.protocol,
        ))
        .map_err(|e| e.to_string())
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
        let condition = admit_queries(&queries, &self.definition.workload.measured_step)?;
        if condition != self.cache_condition
            || (dataset.needs_indices || queries.needs_indices) != self.needs_indices
        {
            return Err("derived GQT treatment differs from frozen plan".into());
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
    if c.version != 1 || !valid_kebab_id(&c.id) || c.id.len() > 128 {
        return Err("gqt-v1 requires version 1 and a bounded kebab-case id".into());
    }
    if c.protocol.schedule != Schedule::Manual
        || c.protocol.reset == ResetMode::S3Versioning
        || c.protocol
            .deadline_seconds
            .is_some_and(|x| x == 0 || x > 3600)
    {
        return Err(
            "gqt-v1 requires a manual local reset and a null or 1..=3600 second deadline".into(),
        );
    }
    if !matches!(c.environment.backend, Backend::LocalFs { .. }) {
        return Err("gqt-v1 supports local-filesystem only".into());
    }
    if c.workload.measured_step.ordinal == 0
        || c.workload.measured_step.text.trim().is_empty()
        || c.workload.measured_step.text.len() > 16 * 1024
    {
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
    let parent = path.parent().ok_or("config path has no parent")?;
    let catalog = root.canonicalize().map_err(|e| e.to_string())?;
    let queries = read_source(&resolve(parent, &definition.workload.queries, &catalog)?)?;
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
    let parsed_queries = queries.parse()?;
    let parsed_dataset = match &dataset {
        DatasetRecipe::Gqt { source } => source.parse()?,
        DatasetRecipe::Registered { preparation, .. } => preparation.parse()?,
    };
    let mut plan = PlannedGqt {
        planned_sha256: String::new(),
        case_digest: typed_sha256(&definition).map_err(|e| e.to_string())?,
        recipe_sha256: recipe_hash(&dataset)?,
        cache_condition: admit_queries(&parsed_queries, &definition.workload.measured_step)?,
        needs_indices: parsed_queries.needs_indices || parsed_dataset.needs_indices,
        definition,
        dataset,
        queries,
    };
    plan.planned_sha256 = plan.planned_hash()?;
    plan.revalidate()?;
    if serde_json::to_vec(&plan).map_err(|e| e.to_string())?.len() > 256 * 1024 {
        return Err("frozen GQT inputs exceed the bounded worker protocol budget".into());
    }
    Ok(plan)
}
pub fn read_source(path: &Path) -> Result<FrozenSource, String> {
    let text = read_text_file(path, MAX_SOURCE_BYTES, "GQT").map_err(|e| e.message)?;
    let stem = path
        .file_stem()
        .and_then(|x| x.to_str())
        .ok_or("GQT filename must be UTF-8")?
        .into();
    let source = FrozenSource {
        stem,
        sha256: sha256_bytes(text.as_bytes()),
        text,
    };
    source.parse()?;
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
    admit_environment(case)?;
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
    if matches!(
        step.kind,
        StepKind::Settings | StepKind::Show | StepKind::Concurrent
    ) {
        return Err(
            "selected step must execute one query, load, or control/mutation operation".into(),
        );
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
                StepKind::Query | StepKind::BranchList => {
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
        process: ProcessLifecycle::FreshPerRepetition,
        engine: if reopened {
            EnginePreparation::ReopenedAfterProgram
        } else if reads > 0 {
            EnginePreparation::WarmedByProgram
        } else {
            EnginePreparation::PreparationOnly
        },
        page_cache: if reads > 0 {
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
pub fn digest(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

pub fn sha256_bytes(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(bytes))
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
    let dataset = match &plan.dataset {
        DatasetRecipe::Gqt { source } => source.parse()?,
        DatasetRecipe::Registered { preparation, .. } => preparation.parse()?,
    };
    plan.cache_condition = admit_queries(&parsed, &plan.definition.workload.measured_step)?;
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

pub fn explicit_verification(kind: StepKind) -> bool {
    !matches!(
        kind,
        StepKind::Restart | StepKind::Settings | StepKind::Concurrent
    )
}
pub fn admit_environment(case: &Case) -> Result<(), String> {
    PlainHost.admit_case(case)?;
    if !case.runner.environments.iter().any(|e| {
        matches!(
            e.execution,
            omnigraph_gqt_core::Execution::Engine {
                storage: omnigraph_gqt_core::runner_config::Storage::LocalFilesystem
            }
        )
    }) {
        return Err(
            "benchmark requires an admitted direct-engine/local-filesystem environment".into(),
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
    validate_backend_reset(&spec.environment.backend, spec.protocol.reset)?;
    let c = &spec.cache_condition;
    let valid = c.process == ProcessLifecycle::FreshPerRepetition
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
        );
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
        environment: GqtEnvironment { backend },
        protocol: Protocol {
            deadline_seconds,
            attribution: crate::case::Attribution::PerPhase,
            schedule: Schedule::Manual,
            reset,
            timer: crate::case::Timer::Monotonic,
        },
    };
    let query_case = query_source.parse()?;
    let dataset_case = dataset_source.parse()?;
    let recipe = DatasetRecipe::Gqt {
        source: dataset_source,
    };
    let mut plan = PlannedGqt {
        case_digest: typed_sha256(&definition).map_err(|e| e.to_string())?,
        recipe_sha256: recipe_hash(&recipe)?,
        planned_sha256: String::new(),
        cache_condition: admit_queries(&query_case, &definition.workload.measured_step)?,
        needs_indices: query_case.needs_indices || dataset_case.needs_indices,
        definition,
        dataset: recipe,
        queries: query_source,
    };
    plan.planned_sha256 = plan.planned_hash()?;
    plan.revalidate()?;
    Ok(plan)
}

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
        )
    ) {
        return Err("reset and declared backend do not match an admitted local environment".into());
    }
    Ok(())
}
