//! One configuration selects GQT starting states and measured workloads.
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Deserializer, Serialize};

use crate::case::{
    Attribution, Backend, LocalFilesystem, LocalStorageClass, Protocol, ResetMode, Schedule, Timer,
    ValidatedCase,
};
use crate::gqt_case::{
    self, GqtCaseV1, GqtEnvironment, GqtFixture, GqtScenario, GqtWorkload, MeasuredStep, PlannedGqt,
};
use crate::model::{Diagnostic, declared_version, read_yaml_file, strict_yaml, valid_kebab_id};
use crate::suite::{
    MAX_REPETITIONS_PER_CASE, MAX_SUITE_RUNS, MAX_TOTAL_REPETITIONS, ResolvedRun, ResolvedSuite,
    SuiteV1,
};

pub const CONFIG_VERSION: u32 = 2;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigV2 {
    pub version: u32,
    #[serde(default, skip_serializing_if = "Defaults::is_empty")]
    pub defaults: Defaults,
    pub scenarios: Vec<Scenario>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub groups: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    pub run: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Defaults {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repetitions: Option<u32>,
    #[serde(
        default,
        deserialize_with = "deadline",
        skip_serializing_if = "Option::is_none"
    )]
    pub deadline_seconds: Option<Option<u64>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub environment: Option<GqtEnvironment>,
    #[serde(default, skip_serializing_if = "ProtocolOverrides::is_empty")]
    pub protocol: ProtocolOverrides,
}

impl Defaults {
    fn is_empty(&self) -> bool {
        self == &Self::default()
    }
}
impl ProtocolOverrides {
    fn is_empty(&self) -> bool {
        self == &Self::default()
    }
}

fn deadline<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<Option<u64>>, D::Error> {
    Option::deserialize(deserializer).map(Some)
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProtocolOverrides {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attribution: Option<Attribution>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule: Option<Schedule>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reset: Option<ResetMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timer: Option<Timer>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FixtureInput {
    Path(PathBuf),
    Explicit(GqtFixture),
}
impl FixtureInput {
    pub fn definition(&self) -> GqtFixture {
        match self {
            Self::Path(path) => GqtFixture::Dataset { path: path.clone() },
            Self::Explicit(fixture) => fixture.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scenario {
    pub id: String,
    pub fixture: FixtureInput,
    pub workload: PathBuf,
    pub measured_step: MeasuredStep,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repetitions: Option<u32>,
    #[serde(
        default,
        deserialize_with = "deadline",
        skip_serializing_if = "Option::is_none"
    )]
    pub deadline_seconds: Option<Option<u64>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub environment: Option<GqtEnvironment>,
    #[serde(default, skip_serializing_if = "ProtocolOverrides::is_empty")]
    pub protocol: ProtocolOverrides,
}
impl Scenario {
    pub fn expand(&self, defaults: &Defaults) -> (GqtCaseV1, u32) {
        let environment = self
            .environment
            .clone()
            .or_else(|| defaults.environment.clone())
            .unwrap_or_else(local_environment);
        let served = environment.target == crate::gqt_case::Target::Server;
        let reset = if served {
            ResetMode::None
        } else {
            match environment.backend {
                Backend::LocalFs {
                    filesystem: LocalFilesystem::Apfs,
                    ..
                } => ResetMode::LocalClonefile,
                _ => ResetMode::PlainCopy,
            }
        };
        let protocol = Protocol {
            deadline_seconds: self
                .deadline_seconds
                .or(defaults.deadline_seconds)
                .unwrap_or(Some(60)),
            attribution: self
                .protocol
                .attribution
                .or(defaults.protocol.attribution)
                .unwrap_or(if served {
                    Attribution::Off
                } else {
                    Attribution::PerPhase
                }),
            schedule: self
                .protocol
                .schedule
                .or(defaults.protocol.schedule)
                .unwrap_or(Schedule::Manual),
            reset: self
                .protocol
                .reset
                .or(defaults.protocol.reset)
                .unwrap_or(reset),
            timer: self
                .protocol
                .timer
                .or(defaults.protocol.timer)
                .unwrap_or(Timer::Monotonic),
        };
        (
            GqtCaseV1 {
                version: 1,
                id: self.id.clone(),
                scenario: GqtScenario::GqtV1,
                fixture: self.fixture.definition(),
                workload: GqtWorkload {
                    queries: self.workload.clone(),
                    measured_step: self.measured_step.clone(),
                },
                environment,
                protocol,
            },
            self.repetitions.or(defaults.repetitions).unwrap_or(5),
        )
    }
}

pub fn local_environment() -> GqtEnvironment {
    GqtEnvironment::embedded(Backend::LocalFs {
        filesystem: if cfg!(target_os = "macos") {
            LocalFilesystem::Apfs
        } else {
            LocalFilesystem::Xfs
        },
        storage_class: LocalStorageClass::NvmeSsd,
    })
}

#[derive(Debug, Clone)]
pub struct Catalog {
    pub path: PathBuf,
    pub root: PathBuf,
    pub definition: ConfigV2,
}
impl Catalog {
    pub fn load(path: &Path) -> Result<Self, Vec<Diagnostic>> {
        let metadata =
            std::fs::metadata(path).map_err(|e| vec![error("config_read_error", e.to_string())])?;
        if !metadata.is_file() {
            return Err(vec![error(
                "invalid_config_file",
                "config must be a regular file",
            )]);
        }
        let source = read_yaml_file(path, "config").map_err(|e| vec![e])?;
        let catalog = Self::from_source(path, &source)?;
        let canonical = path
            .canonicalize()
            .map_err(|e| vec![error("config_path_error", e.to_string())])?;
        if !canonical.starts_with(&catalog.root) {
            return Err(vec![error(
                "config_outside_root",
                "config symlink escapes its declared directory",
            )]);
        }
        Ok(catalog)
    }

    /// Also used before writing a generated config, whose file does not yet exist.
    pub fn from_source(path: &Path, source: &str) -> Result<Self, Vec<Diagnostic>> {
        let version = declared_version(source, "config").map_err(|e| vec![e])?;
        if version != CONFIG_VERSION {
            return Err(vec![error(
                "unsupported_config_version",
                format!("expected config version {CONFIG_VERSION}, found {version}"),
            )]);
        }
        let definition: ConfigV2 = strict_yaml(source, "config").map_err(|e| vec![e])?;
        validate(&definition)?;
        let absolute = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()
                .map_err(|e| vec![error("config_path_error", e.to_string())])?
                .join(path)
        };
        if absolute.to_str().is_none() {
            return Err(vec![error(
                "config_path_error",
                "config path must be UTF-8",
            )]);
        }
        let root = absolute
            .parent()
            .ok_or_else(|| vec![error("config_path_error", "config has no parent")])?
            .canonicalize()
            .map_err(|e| vec![error("config_path_error", e.to_string())])?;
        let path = root.join(
            absolute
                .file_name()
                .ok_or_else(|| vec![error("config_path_error", "config has no filename")])?,
        );
        Ok(Self {
            path,
            root,
            definition,
        })
    }

    pub fn scenario(&self, id: &str) -> Result<&Scenario, Vec<Diagnostic>> {
        self.definition
            .scenarios
            .iter()
            .find(|s| s.id == id)
            .ok_or_else(|| {
                vec![error(
                    "unknown_scenario",
                    format!("unknown scenario '{id}'; use list scenarios"),
                )]
            })
    }

    pub fn plan(&self, id: &str) -> Result<PlannedGqt, Vec<Diagnostic>> {
        let scenario = self.scenario(id)?;
        let (definition, _) = scenario.expand(&self.definition.defaults);
        gqt_case::load_with_root(&self.path, definition, &self.root)
            .map_err(|message| vec![Diagnostic::error("invalid_scenario", id, message)])
    }

    pub fn resolve(
        &self,
        selector: Option<&str>,
        repetitions: Option<u32>,
    ) -> Result<ResolvedSuite, Vec<Diagnostic>> {
        let ids = select(&self.definition, selector)?;
        let configured: Vec<_> = ids
            .iter()
            .map(|id| {
                let scenario = self.scenario(id)?;
                let repetitions =
                    repetitions.unwrap_or(scenario.expand(&self.definition.defaults).1);
                check_repetitions(repetitions)?;
                Ok((id, repetitions))
            })
            .collect::<Result<_, Vec<Diagnostic>>>()?;
        if configured
            .iter()
            .map(|(_, reps)| u64::from(*reps))
            .sum::<u64>()
            > MAX_TOTAL_REPETITIONS
        {
            return Err(vec![error(
                "repetition_budget_exceeded",
                "selection exceeds 100000 repetitions",
            )]);
        }
        let mut runs = Vec::with_capacity(ids.len());
        let mut identities = BTreeSet::new();
        for (id, repetitions) in configured {
            let plan = self.plan(id)?;
            if !identities.insert(plan.planned_sha256.clone()) {
                return Err(vec![error(
                    "duplicate_planned_identity",
                    format!("scenario '{id}' duplicates an experiment in this selection"),
                )]);
            }
            runs.push(ResolvedRun {
                case_path: self.path.clone(),
                repetitions,
                case: ValidatedCase::Gqt(plan),
            });
        }
        Ok(ResolvedSuite {
            definition: SuiteV1 {
                version: 1,
                name: selector.unwrap_or("configured-benchmarks").into(),
                runs: Vec::new(),
            },
            suite_path: self.path.clone(),
            runs,
        })
    }
}

pub fn find_config(explicit: Option<&Path>) -> Result<PathBuf, Vec<Diagnostic>> {
    if let Some(path) = explicit {
        return Ok(path.to_path_buf());
    }
    let cwd =
        std::env::current_dir().map_err(|e| vec![error("config_path_error", e.to_string())])?;
    for directory in cwd.ancestors() {
        for path in [
            directory.join("benchmarks.yaml"),
            directory.join("benchmarks/benchmarks.yaml"),
        ] {
            match path.try_exists() {
                Ok(true) => return Ok(path),
                Ok(false) => {}
                Err(e) => return Err(vec![error("config_path_error", e.to_string())]),
            }
        }
    }
    Err(vec![error(
        "config_not_found",
        "no benchmarks/benchmarks.yaml found; pass --config FILE",
    )])
}

fn check_repetitions(value: u32) -> Result<(), Vec<Diagnostic>> {
    if !(1..=MAX_REPETITIONS_PER_CASE).contains(&value) {
        return Err(vec![error(
            "invalid_repetitions",
            format!("repetitions must be in 1..={MAX_REPETITIONS_PER_CASE}"),
        )]);
    }
    Ok(())
}
fn validate(config: &ConfigV2) -> Result<(), Vec<Diagnostic>> {
    if config.scenarios.is_empty()
        || config.scenarios.len() > MAX_SUITE_RUNS
        || config.groups.len() > MAX_SUITE_RUNS
        || config.run.len() > MAX_SUITE_RUNS
    {
        return Err(vec![error(
            "invalid_catalog_size",
            format!(
                "config requires 1..={MAX_SUITE_RUNS} scenarios and at most {MAX_SUITE_RUNS} groups"
            ),
        )]);
    }
    let mut names = BTreeSet::new();
    for scenario in &config.scenarios {
        if !valid_kebab_id(&scenario.id)
            || scenario.id.len() > 128
            || !names.insert(scenario.id.as_str())
        {
            return Err(vec![error(
                "invalid_scenario_id",
                format!(
                    "scenario ID '{}' must be unique kebab-case, at most 128 characters",
                    scenario.id
                ),
            )]);
        }
        let (definition, repetitions) = scenario.expand(&config.defaults);
        check_repetitions(repetitions)?;
        gqt_case::validate_definition(&definition)
            .map_err(|e| vec![Diagnostic::error("invalid_scenario", &scenario.id, e)])?;
        for path in fixture_paths(&definition.fixture)
            .into_iter()
            .chain([definition.workload.queries.as_path()])
        {
            if path.is_absolute()
                || path.as_os_str().is_empty()
                || path
                    .components()
                    .any(|p| matches!(p, std::path::Component::ParentDir))
            {
                return Err(vec![Diagnostic::error(
                    "invalid_source_path",
                    &scenario.id,
                    "source paths must be relative and stay inside the config directory",
                )]);
            }
        }
    }
    for (name, members) in &config.groups {
        if !valid_kebab_id(name) || name.len() > 128 || names.contains(name.as_str()) {
            return Err(vec![error(
                "invalid_group_id",
                format!("group '{name}' must be unique kebab-case, distinct from scenario IDs"),
            )]);
        }
        if members.is_empty() || members.len() > MAX_SUITE_RUNS {
            return Err(vec![error(
                "invalid_group_size",
                format!("group '{name}' is empty or too large"),
            )]);
        }
        let mut seen = BTreeSet::new();
        for id in members {
            if !names.contains(id.as_str()) || !seen.insert(id) {
                return Err(vec![error(
                    "invalid_group_member",
                    format!("group '{name}' has duplicate or unknown scenario '{id}'"),
                )]);
            }
        }
    }
    if !config.run.is_empty() {
        select(config, None)?;
    }
    Ok(())
}
fn select(config: &ConfigV2, selector: Option<&str>) -> Result<Vec<String>, Vec<Diagnostic>> {
    let selectors: Vec<&str> = match selector {
        Some(name) => vec![name],
        None => config.run.iter().map(String::as_str).collect(),
    };
    if selectors.is_empty() {
        return Err(vec![error(
            "empty_selection",
            "supply a scenario/group name or an explicit run list in the config",
        )]);
    }
    let names: BTreeSet<_> = config.scenarios.iter().map(|s| s.id.as_str()).collect();
    let mut seen = BTreeSet::new();
    let mut result = Vec::new();
    for selector in selectors {
        let members = if let Some(members) = config.groups.get(selector) {
            members.clone()
        } else if names.contains(selector) {
            vec![selector.to_owned()]
        } else {
            return Err(vec![error(
                "unknown_selection",
                format!("unknown scenario or group '{selector}'"),
            )]);
        };
        for id in members {
            if !seen.insert(id.clone()) {
                return Err(vec![error(
                    "duplicate_selection",
                    format!("scenario '{id}' is selected more than once"),
                )]);
            }
            result.push(id);
            if result.len() > MAX_SUITE_RUNS {
                return Err(vec![error(
                    "selection_budget_exceeded",
                    "too many selected scenarios",
                )]);
            }
        }
    }
    Ok(result)
}

pub fn fixture_paths(fixture: &GqtFixture) -> Vec<&Path> {
    match fixture {
        GqtFixture::Dataset { path } => vec![path],
        GqtFixture::Registered {
            reference,
            preparation,
        } => vec![reference, preparation],
    }
}
fn error(code: &str, message: impl Into<String>) -> Diagnostic {
    Diagnostic::error(code, "$", message)
}
