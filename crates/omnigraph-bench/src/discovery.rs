//! Read-only inventory derived from config references and bounded content folders.
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::catalog::{Catalog, FixtureInput, Scenario};
use crate::fixture_reference::load_fixture_reference;
use crate::gqt_case::{GqtFixture, MAX_SOURCE_BYTES};
use crate::model::{Diagnostic, read_text_file};
use omnigraph_gqt_core::parse_case;

const MAX_ENTRIES: usize = 10_000;
const MAX_DEPTH: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum SourceState {
    Available,
    Missing,
    Invalid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum SourceKind {
    Fixture,
    Workload,
    Preparation,
    RegisteredReference,
}
impl std::fmt::Display for SourceKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Fixture => "fixture",
            Self::Workload => "workload",
            Self::Preparation => "preparation",
            Self::RegisteredReference => "registered-reference",
        })
    }
}

#[derive(Debug, Serialize)]
pub struct SourceEntry {
    pub path: PathBuf,
    pub kind: SourceKind,
    pub source: SourceState,
    pub scenarios: Vec<String>,
    pub diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Serialize)]
pub struct SourceInventory {
    pub config: PathBuf,
    pub entries: Vec<SourceEntry>,
}

pub fn sources(catalog: &Catalog, fixtures: bool) -> Result<SourceInventory, Vec<Diagnostic>> {
    let mut paths: BTreeMap<PathBuf, (SourceKind, BTreeSet<String>)> = BTreeMap::new();
    for scenario in &catalog.definition.scenarios {
        let inputs = if fixtures {
            match scenario.fixture.definition() {
                GqtFixture::Dataset { path } => vec![(path, SourceKind::Fixture)],
                GqtFixture::Registered {
                    reference,
                    preparation,
                } => vec![
                    (reference, SourceKind::RegisteredReference),
                    (preparation, SourceKind::Preparation),
                ],
            }
        } else {
            vec![(scenario.workload.clone(), SourceKind::Workload)]
        };
        for (path, kind) in inputs {
            paths
                .entry(path)
                .or_insert_with(|| (kind, BTreeSet::new()))
                .1
                .insert(scenario.id.clone());
        }
    }
    let content = catalog
        .root
        .join(if fixtures { "fixtures" } else { "workloads" });
    let mut remaining = MAX_ENTRIES;
    scan(
        &catalog.root,
        &content,
        0,
        &mut remaining,
        fixtures,
        &mut paths,
    )?;
    if paths.len() > MAX_ENTRIES {
        return Err(vec![diagnostic(
            "inventory_limit",
            "inventory exceeds 10000 sources",
        )]);
    }
    let entries = paths
        .into_iter()
        .map(|(path, (kind, scenarios))| {
            let (kind, source, diagnostics) =
                inspect_source(&catalog.root, &path, kind, scenarios.is_empty());
            SourceEntry {
                path,
                kind,
                source,
                scenarios: scenarios.into_iter().collect(),
                diagnostics,
            }
        })
        .collect();
    Ok(SourceInventory {
        config: catalog.path.clone(),
        entries,
    })
}

type InventoryPaths = BTreeMap<PathBuf, (SourceKind, BTreeSet<String>)>;
fn scan(
    root: &Path,
    directory: &Path,
    depth: usize,
    remaining: &mut usize,
    fixtures: bool,
    paths: &mut InventoryPaths,
) -> Result<(), Vec<Diagnostic>> {
    let metadata = match fs::symlink_metadata(directory) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(vec![diagnostic("inventory_read_error", error.to_string())]),
    };
    if !metadata.is_dir() {
        return Err(vec![diagnostic(
            "invalid_content_root",
            format!("{} must be a directory, not a symlink", directory.display()),
        )]);
    }
    if depth > MAX_DEPTH {
        return Err(vec![diagnostic(
            "inventory_limit",
            "content nesting exceeds 16 directories",
        )]);
    }
    for entry in fs::read_dir(directory)
        .map_err(|e| vec![diagnostic("inventory_read_error", e.to_string())])?
    {
        if *remaining == 0 {
            return Err(vec![diagnostic(
                "inventory_limit",
                "content folders exceed 10000 entries",
            )]);
        }
        *remaining -= 1;
        let entry = entry.map_err(|e| vec![diagnostic("inventory_read_error", e.to_string())])?;
        let path = entry.path();
        let kind = entry
            .file_type()
            .map_err(|e| vec![diagnostic("inventory_read_error", e.to_string())])?;
        if kind.is_dir() {
            scan(root, &path, depth + 1, remaining, fixtures, paths)?;
        } else if path.extension().is_some_and(|e| e == "gqt")
            || (fixtures
                && path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.ends_with(".fixture-reference-v1.yaml")))
        {
            let relative = path
                .strip_prefix(root)
                .map_err(|e| vec![diagnostic("inventory_path_error", e.to_string())])?
                .to_path_buf();
            let kind = if path.extension().is_some_and(|e| e == "yaml") {
                SourceKind::RegisteredReference
            } else if fixtures {
                SourceKind::Fixture
            } else {
                SourceKind::Workload
            };
            paths
                .entry(relative)
                .or_insert_with(|| (kind, BTreeSet::new()));
        }
    }
    Ok(())
}

fn inspect_source(
    root: &Path,
    path: &Path,
    mut kind: SourceKind,
    discover_preparation: bool,
) -> (SourceKind, SourceState, Vec<Diagnostic>) {
    let joined = root.join(path);
    let canonical = match joined.canonicalize() {
        Ok(path) => path,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return (
                kind,
                SourceState::Missing,
                vec![Diagnostic::error(
                    "source_missing",
                    path.display().to_string(),
                    "source file is missing",
                )],
            );
        }
        Err(error) => {
            return (
                kind,
                SourceState::Invalid,
                vec![Diagnostic::error(
                    "source_unreadable",
                    path.display().to_string(),
                    error.to_string(),
                )],
            );
        }
    };
    if !canonical.starts_with(root) {
        return (
            kind,
            SourceState::Invalid,
            vec![Diagnostic::error(
                "source_outside_root",
                path.display().to_string(),
                "source resolves outside the config directory",
            )],
        );
    }
    let result = match kind {
        SourceKind::RegisteredReference => {
            load_fixture_reference(&canonical).into_result().map(|_| ())
        }
        SourceKind::Fixture | SourceKind::Workload | SourceKind::Preparation => {
            let parsed = read_text_file(&canonical, MAX_SOURCE_BYTES, "GQT")
                .map_err(|e| e.message)
                .and_then(|text| {
                    let stem = canonical
                        .file_stem()
                        .and_then(|s| s.to_str())
                        .ok_or("GQT filename must be UTF-8")?;
                    parse_case(stem, &text)
                });
            parsed
                .and_then(|case| {
                    if discover_preparation && kind == SourceKind::Fixture && case.fixture.is_none()
                    {
                        kind = SourceKind::Preparation;
                    }
                    if (kind == SourceKind::Fixture) != case.fixture.is_some() {
                        Err(if kind == SourceKind::Fixture {
                            "fixture requires schema and seed"
                        } else {
                            "workload/preparation must omit schema and seed"
                        }
                        .into())
                    } else {
                        Ok(())
                    }
                })
                .map_err(|e| {
                    vec![Diagnostic::error(
                        "invalid_source",
                        path.display().to_string(),
                        e,
                    )]
                })
        }
    };
    match result {
        Ok(()) => (kind, SourceState::Available, Vec::new()),
        Err(diagnostics) => (kind, SourceState::Invalid, diagnostics),
    }
}

#[derive(Debug, Serialize)]
pub struct ScenarioEntry {
    pub id: String,
    pub fixture: FixtureInput,
    pub workload: PathBuf,
    pub groups: Vec<String>,
    pub repetitions: u32,
    pub source: SourceState,
    pub diagnostics: Vec<Diagnostic>,
}
#[derive(Debug, Serialize)]
pub struct ScenarioInventory {
    pub config: PathBuf,
    pub groups: BTreeMap<String, Vec<String>>,
    pub run: Vec<String>,
    pub entries: Vec<ScenarioEntry>,
}
pub fn scenario(catalog: &Catalog, scenario: &Scenario) -> ScenarioEntry {
    let mut state = SourceState::Available;
    let mut diagnostics = Vec::new();
    let fixture = scenario.fixture.definition();
    let mut sources = match &fixture {
        GqtFixture::Dataset { path } => vec![(path.as_path(), SourceKind::Fixture)],
        GqtFixture::Registered {
            reference,
            preparation,
        } => vec![
            (reference.as_path(), SourceKind::RegisteredReference),
            (preparation.as_path(), SourceKind::Preparation),
        ],
    };
    sources.push((&scenario.workload, SourceKind::Workload));
    for (path, kind) in sources {
        let (_, source, errors) = inspect_source(&catalog.root, path, kind, false);
        if source == SourceState::Invalid
            || (state != SourceState::Invalid && source == SourceState::Missing)
        {
            state = source;
        }
        diagnostics.extend(errors);
    }
    if diagnostics.is_empty()
        && let Err(errors) = catalog.plan(&scenario.id)
    {
        state = SourceState::Invalid;
        diagnostics.extend(errors);
    }
    ScenarioEntry {
        id: scenario.id.clone(),
        fixture: scenario.fixture.clone(),
        workload: scenario.workload.clone(),
        groups: catalog
            .definition
            .groups
            .iter()
            .filter(|(_, members)| members.contains(&scenario.id))
            .map(|(id, _)| id.clone())
            .collect(),
        repetitions: scenario.expand(&catalog.definition.defaults).1,
        source: state,
        diagnostics,
    }
}
pub fn scenarios(catalog: &Catalog) -> ScenarioInventory {
    let mut entries: Vec<_> = catalog
        .definition
        .scenarios
        .iter()
        .map(|entry| scenario(catalog, entry))
        .collect();
    entries.sort_by(|a, b| a.id.cmp(&b.id));
    ScenarioInventory {
        config: catalog.path.clone(),
        groups: catalog.definition.groups.clone(),
        run: catalog.definition.run.clone(),
        entries,
    }
}
fn diagnostic(code: &str, message: impl Into<String>) -> Diagnostic {
    Diagnostic::error(code, "$", message)
}
