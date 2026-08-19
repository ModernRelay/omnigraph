use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::process::Command;

use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;
use walkdir::WalkDir;

mod markdown;
mod public_rust;
mod rust_strings;

pub const INVENTORY_HEADER: &str = "schema_version\toccurrence_id\tsurface\tsource_path\tboundary\tsite_kind\tcurrent_term\ttarget_term\tclassification\tcompatibility_treatment\trollout_phase\ttest_owner\trationale";

// Schema v2 renames the planning field from `rollout_slice` to
// `rollout_phase`; v1 rows are rejected instead of being interpreted under a
// silently different review contract.
const SCHEMA_VERSION: &str = "2";
const SURFACE_OPENAPI: &str = "openapi";
pub(crate) const SURFACE_RUST_STRING: &str = "rust_string";
pub(crate) const SURFACE_USER_DOCS: &str = "user_docs";
pub(crate) const SURFACE_PUBLIC_RUST: &str = "public_rust";
const SURFACES: &[&str] = &[SURFACE_OPENAPI, "rust_string", "user_docs", "public_rust"];
const OPENAPI_SITE_KINDS: &[&str] = &[
    "component_name",
    "header_name",
    "operation_id",
    "parameter_name",
    "path",
    "property_name",
    "required_name",
    "discriminator_name",
    "enum_value",
    "prose",
];
const CLASSIFICATIONS: &[&str] = &[
    "legacy_graph_misuse",
    "compatibility_alias",
    "physical_storage",
    "result_projection",
    "renderer",
    "tabular_presentation",
    "upstream_name",
    "quoted_compatibility",
];
const ROLLOUT_PHASES: &[&str] = &["guard", "presentation", "compatibility", "none"];
const REVIEWED_SEMANTIC_RECLASSIFICATION: &str = "reviewed_semantic_reclassification";

#[derive(Debug, Error)]
pub enum GuardError {
    #[error("cannot read {path}: {source}")]
    Read {
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("cannot parse JSON from {path}: {source}")]
    Json {
        path: String,
        #[source]
        source: serde_json::Error,
    },

    #[error("invalid OpenAPI document: {0}")]
    OpenApi(String),

    #[error("invalid Rust source {path}: {message}")]
    RustSource { path: String, message: String },

    #[error("public Rust API guard error: {0}")]
    PublicRust(String),

    #[error("invalid vocabulary inventory: {0}")]
    Inventory(String),

    #[error("git error: {0}")]
    Git(String),

    #[error("vocabulary check failed:\n{0}")]
    Check(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InventoryRow {
    pub schema_version: String,
    pub occurrence_id: String,
    pub surface: String,
    pub source_path: String,
    pub boundary: String,
    pub site_kind: String,
    pub current_term: String,
    pub target_term: String,
    pub classification: String,
    pub compatibility_treatment: String,
    pub rollout_phase: String,
    pub test_owner: String,
    pub rationale: String,
}

impl InventoryRow {
    fn observation(
        occurrence_id: String,
        surface: &str,
        source_path: &str,
        boundary: &str,
        site_kind: &str,
        current_term: String,
    ) -> Self {
        Self {
            schema_version: SCHEMA_VERSION.to_owned(),
            occurrence_id,
            surface: surface.to_owned(),
            source_path: source_path.to_owned(),
            boundary: boundary.to_owned(),
            site_kind: site_kind.to_owned(),
            current_term,
            target_term: String::new(),
            classification: String::new(),
            compatibility_treatment: String::new(),
            rollout_phase: String::new(),
            test_owner: String::new(),
            rationale: String::new(),
        }
    }

    fn fields(&self) -> [&str; 13] {
        [
            &self.schema_version,
            &self.occurrence_id,
            &self.surface,
            &self.source_path,
            &self.boundary,
            &self.site_kind,
            &self.current_term,
            &self.target_term,
            &self.classification,
            &self.compatibility_treatment,
            &self.rollout_phase,
            &self.test_owner,
            &self.rationale,
        ]
    }

    fn observation_fields(&self) -> [&str; 7] {
        [
            &self.schema_version,
            &self.occurrence_id,
            &self.surface,
            &self.source_path,
            &self.boundary,
            &self.site_kind,
            &self.current_term,
        ]
    }
}

#[derive(Clone, Debug)]
pub struct CheckOptions {
    pub base: String,
    pub inventory: PathBuf,
    pub openapi: PathBuf,
    pub surface: GuardSurface,
    pub public_api_tool: PathBuf,
    pub public_api_target_dir: PathBuf,
    pub presentation_only: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GuardSurface {
    Openapi,
    RustString,
    UserDocs,
    PublicRust,
    All,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CheckReport {
    pub current_occurrences: usize,
    pub base_occurrences: usize,
}

pub fn scan_openapi_file(path: &Path) -> Result<Vec<InventoryRow>, GuardError> {
    let contents = fs::read_to_string(path).map_err(|source| GuardError::Read {
        path: path.display().to_string(),
        source,
    })?;
    scan_openapi(&contents, &path_to_slashes(path))
}

pub fn scan_openapi_at_root(root: &Path, path: &Path) -> Result<Vec<InventoryRow>, GuardError> {
    let repository_path = repository_path(root, path)?;
    let source_path = path_to_slashes(&repository_path);
    let contents =
        fs::read_to_string(root.join(&repository_path)).map_err(|source| GuardError::Read {
            path: source_path.clone(),
            source,
        })?;
    scan_openapi(&contents, &source_path)
}

pub fn scan_openapi(contents: &str, source_path: &str) -> Result<Vec<InventoryRow>, GuardError> {
    let document: Value = serde_json::from_str(contents).map_err(|source| GuardError::Json {
        path: source_path.to_owned(),
        source,
    })?;
    OpenApiCollector::new(&document, source_path).collect()
}

pub fn render_tsv(rows: &[InventoryRow]) -> Result<String, GuardError> {
    let mut rows = rows.to_vec();
    rows.sort_by(|left, right| left.occurrence_id.cmp(&right.occurrence_id));

    let mut seen = HashSet::new();
    let mut output = String::with_capacity(rows.len().saturating_mul(256));
    output.push_str(INVENTORY_HEADER);
    output.push('\n');
    for row in rows {
        if !seen.insert(row.occurrence_id.clone()) {
            return Err(GuardError::Inventory(format!(
                "duplicate occurrence_id {:?}",
                row.occurrence_id
            )));
        }
        let fields = row.fields();
        for field in fields {
            if field.contains(['\t', '\r', '\n']) {
                return Err(GuardError::Inventory(format!(
                    "row {:?} contains a tab or newline",
                    row.occurrence_id
                )));
            }
        }
        output.push_str(&fields.join("\t"));
        output.push('\n');
    }
    Ok(output)
}

pub fn parse_inventory(contents: &str) -> Result<Vec<InventoryRow>, GuardError> {
    let normalized = contents.replace("\r\n", "\n");
    if normalized.contains('\r') {
        return Err(GuardError::Inventory(
            "bare carriage return is not valid TSV".to_owned(),
        ));
    }
    let mut lines = normalized.split('\n');
    let header = lines
        .next()
        .ok_or_else(|| GuardError::Inventory("file is empty".to_owned()))?;
    if header != INVENTORY_HEADER {
        return Err(GuardError::Inventory(format!(
            "header must be exactly {INVENTORY_HEADER:?}"
        )));
    }

    let mut rows = Vec::new();
    let mut seen = HashSet::new();
    let mut previous_id: Option<String> = None;
    for (index, line) in lines.enumerate() {
        let line_number = index + 2;
        if line.is_empty() {
            if line_number == normalized.lines().count() + 1 {
                continue;
            }
            return Err(GuardError::Inventory(format!(
                "blank row at line {line_number}"
            )));
        }
        let fields: Vec<_> = line.split('\t').collect();
        if fields.len() != 13 {
            return Err(GuardError::Inventory(format!(
                "line {line_number} has {} columns, expected 13",
                fields.len()
            )));
        }
        let row = InventoryRow {
            schema_version: fields[0].to_owned(),
            occurrence_id: fields[1].to_owned(),
            surface: fields[2].to_owned(),
            source_path: fields[3].to_owned(),
            boundary: fields[4].to_owned(),
            site_kind: fields[5].to_owned(),
            current_term: fields[6].to_owned(),
            target_term: fields[7].to_owned(),
            classification: fields[8].to_owned(),
            compatibility_treatment: fields[9].to_owned(),
            rollout_phase: fields[10].to_owned(),
            test_owner: fields[11].to_owned(),
            rationale: fields[12].to_owned(),
        };
        validate_inventory_row(&row, line_number)?;
        if !seen.insert(row.occurrence_id.clone()) {
            return Err(GuardError::Inventory(format!(
                "duplicate occurrence_id {:?} at line {line_number}",
                row.occurrence_id
            )));
        }
        if let Some(previous) = &previous_id
            && previous >= &row.occurrence_id
        {
            return Err(GuardError::Inventory(format!(
                "rows are not strictly sorted by occurrence_id at line {line_number}: {:?} follows {:?}",
                row.occurrence_id, previous
            )));
        }
        previous_id = Some(row.occurrence_id.clone());
        rows.push(row);
    }
    Ok(rows)
}

fn validate_inventory_row(row: &InventoryRow, line_number: usize) -> Result<(), GuardError> {
    if row.schema_version != SCHEMA_VERSION {
        return Err(GuardError::Inventory(format!(
            "line {line_number} has unknown schema_version {:?}",
            row.schema_version
        )));
    }
    if row.occurrence_id.is_empty()
        || row.source_path.is_empty()
        || row.boundary.is_empty()
        || row.current_term.is_empty()
    {
        return Err(GuardError::Inventory(format!(
            "line {line_number} is missing an observation identity field"
        )));
    }
    if !SURFACES.contains(&row.surface.as_str()) {
        return Err(GuardError::Inventory(format!(
            "line {line_number} has unknown surface {:?}",
            row.surface
        )));
    }
    if row.site_kind.is_empty() {
        return Err(GuardError::Inventory(format!(
            "line {line_number} has an empty site_kind"
        )));
    }
    let known_site_kinds = match row.surface.as_str() {
        SURFACE_OPENAPI => OPENAPI_SITE_KINDS,
        SURFACE_RUST_STRING => rust_strings::SITE_KINDS,
        SURFACE_USER_DOCS => markdown::SITE_KINDS,
        SURFACE_PUBLIC_RUST => public_rust::SITE_KINDS,
        _ => unreachable!("surface was validated above"),
    };
    if !known_site_kinds.contains(&row.site_kind.as_str()) {
        return Err(GuardError::Inventory(format!(
            "line {line_number} has unknown {} site_kind {:?}",
            row.surface, row.site_kind
        )));
    }
    if !CLASSIFICATIONS.contains(&row.classification.as_str()) {
        return Err(GuardError::Inventory(format!(
            "line {line_number} has unknown or empty classification {:?}",
            row.classification
        )));
    }
    for (name, value) in [
        ("target_term", row.target_term.as_str()),
        (
            "compatibility_treatment",
            row.compatibility_treatment.as_str(),
        ),
        ("rollout_phase", row.rollout_phase.as_str()),
        ("test_owner", row.test_owner.as_str()),
        ("rationale", row.rationale.as_str()),
    ] {
        if value.is_empty() {
            return Err(GuardError::Inventory(format!(
                "line {line_number} has an empty review-owned {name}"
            )));
        }
    }
    if !ROLLOUT_PHASES.contains(&row.rollout_phase.as_str()) {
        return Err(GuardError::Inventory(format!(
            "line {line_number} has unknown rollout_phase {:?}",
            row.rollout_phase
        )));
    }
    if row.compatibility_treatment == REVIEWED_SEMANTIC_RECLASSIFICATION
        && (row.classification == "legacy_graph_misuse" || row.rollout_phase != "presentation")
    {
        return Err(GuardError::Inventory(format!(
            "line {line_number} may use {REVIEWED_SEMANTIC_RECLASSIFICATION:?} only for a non-legacy presentation-phase semantic rewrite"
        )));
    }
    Ok(())
}

pub fn check(options: &CheckOptions) -> Result<CheckReport, GuardError> {
    let root = git_root()?;
    check_at_root(&root, options)
}

pub fn check_in(root: &Path, options: &CheckOptions) -> Result<CheckReport, GuardError> {
    let root = root.canonicalize().map_err(|error| {
        GuardError::Git(format!(
            "cannot canonicalize repository root {}: {error}",
            root.display()
        ))
    })?;
    check_at_root(&root, options)
}

fn check_at_root(root: &Path, options: &CheckOptions) -> Result<CheckReport, GuardError> {
    validate_base_sha(&options.base)?;
    validate_base_commit(root, &options.base)?;
    let inventory_path = repository_path(root, &options.inventory)?;
    let inventory_source_path = path_to_slashes(&inventory_path);

    let current_inventory_text =
        fs::read_to_string(root.join(&inventory_path)).map_err(|source| GuardError::Read {
            path: inventory_source_path.clone(),
            source,
        })?;
    let base_inventory_text = git_show(root, &options.base, &inventory_path)?;

    let current_inventory = parse_inventory(&current_inventory_text)?;
    let base_inventory = parse_inventory(&base_inventory_text)?;

    let mut current_count = 0;
    let mut base_count = 0;
    for &surface in selected_surfaces(options.surface) {
        let (current_observations, base_observations) = match surface {
            SURFACE_OPENAPI => {
                let openapi_path = repository_path(root, &options.openapi)?;
                let openapi_source_path = path_to_slashes(&openapi_path);
                let current = fs::read_to_string(root.join(&openapi_path)).map_err(|source| {
                    GuardError::Read {
                        path: openapi_source_path.clone(),
                        source,
                    }
                })?;
                let base = git_show(root, &options.base, &openapi_path)?;
                if options.presentation_only {
                    ensure_openapi_structure_unchanged(&base, &current)?;
                }
                (
                    scan_openapi(&current, &openapi_source_path)?,
                    scan_openapi(&base, &openapi_source_path)?,
                )
            }
            SURFACE_RUST_STRING => (
                scan_current_rust_strings(root)?,
                scan_base_rust_strings(root, &options.base)?,
            ),
            SURFACE_USER_DOCS => (
                scan_current_user_docs(root)?,
                scan_base_user_docs(root, &options.base)?,
            ),
            SURFACE_PUBLIC_RUST => {
                let public_api_tool = resolve_tool_executable(root, &options.public_api_tool);
                let target_dir = if options.public_api_target_dir.is_absolute() {
                    options.public_api_target_dir.clone()
                } else {
                    root.join(&options.public_api_target_dir)
                };
                let config = public_rust::ToolConfig {
                    executable: public_api_tool.clone(),
                    target_dir: target_dir.join("current"),
                };
                let current = public_rust::scan_worktree(root, &config)?;
                let base_prefix = &options.base[..options.base.len().min(12)];
                let base_config = public_rust::ToolConfig {
                    executable: public_api_tool,
                    target_dir: target_dir.join(format!("base-{base_prefix}")),
                };
                let base = public_rust::scan_git_commit(root, &options.base, &base_config)?;
                (current, base)
            }
            _ => unreachable!("selected_surfaces returns concrete surfaces"),
        };

        compare_surface_inventory("merge base", surface, &base_observations, &base_inventory)?;
        compare_surface_inventory(
            "current tree",
            surface,
            &current_observations,
            &current_inventory,
        )?;
        compare_surface_classification_sets(surface, &base_inventory, &current_inventory)?;
        current_count += current_observations.len();
        base_count += base_observations.len();
    }

    Ok(CheckReport {
        current_occurrences: current_count,
        base_occurrences: base_count,
    })
}

pub fn resolve_tool_executable(root: &Path, executable: &Path) -> PathBuf {
    if executable.is_absolute() || executable.components().count() == 1 {
        executable.to_path_buf()
    } else {
        root.join(executable)
    }
}

fn selected_surfaces(surface: GuardSurface) -> &'static [&'static str] {
    match surface {
        GuardSurface::Openapi => &[SURFACE_OPENAPI],
        GuardSurface::RustString => &[SURFACE_RUST_STRING],
        GuardSurface::UserDocs => &[SURFACE_USER_DOCS],
        GuardSurface::PublicRust => &[SURFACE_PUBLIC_RUST],
        GuardSurface::All => SURFACES,
    }
}

#[cfg(test)]
fn compare_inventory(
    label: &str,
    observations: &[InventoryRow],
    inventory: &[InventoryRow],
) -> Result<(), GuardError> {
    compare_surface_inventory(label, SURFACE_OPENAPI, observations, inventory)
}

fn compare_surface_inventory(
    label: &str,
    surface: &str,
    observations: &[InventoryRow],
    inventory: &[InventoryRow],
) -> Result<(), GuardError> {
    let observed: BTreeMap<_, _> = observations
        .iter()
        .map(|row| (row.occurrence_id.as_str(), row))
        .collect();
    let classified: BTreeMap<_, _> = inventory
        .iter()
        .filter(|row| row.surface == surface)
        .map(|row| (row.occurrence_id.as_str(), row))
        .collect();
    let mut failures = Vec::new();

    for (id, observation) in &observed {
        match classified.get(id) {
            None => failures.push(format!("{label}: unclassified occurrence {id}")),
            Some(row) if row.observation_fields() != observation.observation_fields() => failures
                .push(format!(
                    "{label}: inventory row {id} does not match its observed site"
                )),
            Some(_) => {}
        }
    }
    for id in classified.keys() {
        if !observed.contains_key(id) {
            failures.push(format!("{label}: stale inventory row {id}"));
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(GuardError::Check(failures.join("\n")))
    }
}

fn compare_surface_classification_sets(
    surface: &str,
    base: &[InventoryRow],
    current: &[InventoryRow],
) -> Result<(), GuardError> {
    let ids = |rows: &[InventoryRow], classification: &str| -> BTreeSet<String> {
        rows.iter()
            .filter(|row| row.surface == surface && row.classification == classification)
            .map(|row| row.occurrence_id.clone())
            .collect()
    };
    let term_counts =
        |rows: &[InventoryRow], classification: &str| -> BTreeMap<LegacyTermKey, usize> {
            let mut counts = BTreeMap::new();
            for row in rows
                .iter()
                .filter(|row| row.surface == surface && row.classification == classification)
            {
                *counts.entry(legacy_term_key(row)).or_default() += 1;
            }
            counts
        };
    let base_legacy_terms = term_counts(base, "legacy_graph_misuse");
    let current_legacy_terms = term_counts(current, "legacy_graph_misuse");
    let new_legacy: Vec<_> = current_legacy_terms
        .iter()
        .filter_map(|(key, &current_count)| {
            let base_count = base_legacy_terms.get(key).copied().unwrap_or_default();
            (current_count > base_count).then(|| (key.clone(), current_count - base_count))
        })
        .collect();
    if !new_legacy.is_empty() {
        return Err(GuardError::Check(format!(
            "current tree introduces legacy_graph_misuse stable boundary occurrence(s): {}",
            format_legacy_term_keys(&new_legacy)
        )));
    }

    // A fingerprint-independent boundary deliberately treats a rewritten
    // observation as a possible continuation of a base legacy occurrence.
    // Reserve exact, same-class nonlegacy observations first: they have their
    // own unambiguous base identity and cannot be spent as evidence that an
    // unrelated legacy neighbour survived. Every unmatched row remains in the
    // conservative candidate pool, including text/fingerprint rewrites.
    let current_by_id: BTreeMap<_, _> = current
        .iter()
        .filter(|row| row.surface == surface)
        .map(|row| (row.occurrence_id.as_str(), row))
        .collect();
    let unchanged_nonlegacy_ids: BTreeSet<_> = base
        .iter()
        .filter(|row| row.surface == surface && row.classification != "legacy_graph_misuse")
        .filter(|base_row| {
            current_by_id
                .get(base_row.occurrence_id.as_str())
                .is_some_and(|current_row| {
                    current_row.classification == base_row.classification
                        && current_row.observation_fields() == base_row.observation_fields()
                })
        })
        .map(|row| row.occurrence_id.as_str())
        .collect();

    let base_legacy = legacy_boundary_counts(base.iter(), surface, Some("legacy_graph_misuse"));
    let current_legacy = legacy_boundary_counts(
        current
            .iter()
            .filter(|row| !unchanged_nonlegacy_ids.contains(row.occurrence_id.as_str())),
        surface,
        Some("legacy_graph_misuse"),
    );
    let current_transition_candidates = legacy_boundary_counts(
        current
            .iter()
            .filter(|row| !unchanged_nonlegacy_ids.contains(row.occurrence_id.as_str())),
        surface,
        None,
    );
    let persisted_transition_ids: BTreeSet<_> = base
        .iter()
        .filter(|row| {
            row.surface == surface
                && row.compatibility_treatment == REVIEWED_SEMANTIC_RECLASSIFICATION
        })
        .map(|row| row.occurrence_id.as_str())
        .collect();
    let (reviewed_transitions, candidate_reviewed_transitions): (
        BTreeMap<LegacyBoundaryKey, usize>,
        BTreeMap<LegacyBoundaryKey, usize>,
    ) = {
        let mut all_counts = BTreeMap::new();
        let mut candidate_counts = BTreeMap::new();
        for row in current.iter().filter(|row| {
            row.surface == surface
                && row.compatibility_treatment == REVIEWED_SEMANTIC_RECLASSIFICATION
                && !persisted_transition_ids.contains(row.occurrence_id.as_str())
        }) {
            let key = legacy_boundary_key(row);
            *all_counts.entry(key.clone()).or_default() += 1;
            if !unchanged_nonlegacy_ids.contains(row.occurrence_id.as_str()) {
                *candidate_counts.entry(key).or_default() += 1;
            }
        }
        (all_counts, candidate_counts)
    };
    let laundered_legacy: Vec<_> = base_legacy
        .iter()
        .filter_map(|(key, &base_count)| {
            let surviving_count = base_count.min(
                current_transition_candidates
                    .get(key)
                    .copied()
                    .unwrap_or_default(),
            );
            let current_legacy_count = current_legacy.get(key).copied().unwrap_or_default();
            let transition_count = candidate_reviewed_transitions
                .get(key)
                .copied()
                .unwrap_or_default();
            let unresolved = surviving_count
                .saturating_sub(current_legacy_count)
                .saturating_sub(transition_count);
            (unresolved > 0).then(|| (key.clone(), unresolved))
        })
        .collect();
    if !laundered_legacy.is_empty() {
        return Err(GuardError::Check(format!(
            "current tree reclassifies surviving legacy_graph_misuse stable boundary occurrence(s): {}",
            format_legacy_keys(&laundered_legacy)
        )));
    }
    let unused_transition_markers: Vec<_> = reviewed_transitions
        .iter()
        .filter_map(|(key, &transition_count)| {
            let base_count = base_legacy.get(key).copied().unwrap_or_default();
            let surviving_count = base_count.min(
                current_transition_candidates
                    .get(key)
                    .copied()
                    .unwrap_or_default(),
            );
            let current_legacy_count = current_legacy.get(key).copied().unwrap_or_default();
            let required = surviving_count.saturating_sub(current_legacy_count);
            let usable_transition_count = candidate_reviewed_transitions
                .get(key)
                .copied()
                .unwrap_or_default()
                .min(required);
            (transition_count > usable_transition_count)
                .then(|| (key.clone(), transition_count - usable_transition_count))
        })
        .collect();
    if !unused_transition_markers.is_empty() {
        return Err(GuardError::Check(format!(
            "current tree has unused {REVIEWED_SEMANTIC_RECLASSIFICATION} marker(s): {}",
            format_legacy_keys(&unused_transition_markers)
        )));
    }

    let base_aliases = ids(base, "compatibility_alias");
    let current_aliases = ids(current, "compatibility_alias");
    if base_aliases != current_aliases {
        let added: Vec<_> = current_aliases.difference(&base_aliases).cloned().collect();
        let removed: Vec<_> = base_aliases.difference(&current_aliases).cloned().collect();
        return Err(GuardError::Check(format!(
            "compatibility_alias occurrence set changed; added [{}], removed [{}]",
            added.join(", "),
            removed.join(", ")
        )));
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct LegacyBoundaryKey {
    surface: String,
    source_path: String,
    boundary: String,
    site_kind: String,
    term_ordinal: String,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct LegacyTermKey {
    boundary: LegacyBoundaryKey,
    term_family: String,
}

fn legacy_boundary_counts<'a>(
    rows: impl Iterator<Item = &'a InventoryRow>,
    surface: &str,
    classification: Option<&str>,
) -> BTreeMap<LegacyBoundaryKey, usize> {
    let mut counts = BTreeMap::new();
    for row in rows.filter(|row| {
        row.surface == surface && classification.is_none_or(|value| row.classification == value)
    }) {
        *counts.entry(legacy_boundary_key(row)).or_default() += 1;
    }
    counts
}

fn legacy_boundary_key(row: &InventoryRow) -> LegacyBoundaryKey {
    LegacyBoundaryKey {
        surface: row.surface.clone(),
        source_path: row.source_path.clone(),
        boundary: fingerprint_independent_boundary(&row.boundary),
        site_kind: row.site_kind.clone(),
        term_ordinal: row
            .occurrence_id
            .rsplit(':')
            .next()
            .unwrap_or_default()
            .to_owned(),
    }
}

fn legacy_term_key(row: &InventoryRow) -> LegacyTermKey {
    LegacyTermKey {
        boundary: legacy_boundary_key(row),
        term_family: vocabulary_term_family(&row.current_term),
    }
}

fn fingerprint_independent_boundary(boundary: &str) -> String {
    let Some((prefix, suffix)) = boundary.rsplit_once(" :: ") else {
        return boundary.to_owned();
    };
    let Some((fingerprint, ordinal)) = suffix.rsplit_once(" #") else {
        return boundary.to_owned();
    };
    if fingerprint.len() == 64
        && fingerprint.bytes().all(|byte| byte.is_ascii_hexdigit())
        && !ordinal.is_empty()
        && ordinal.bytes().all(|byte| byte.is_ascii_digit())
    {
        format!("{prefix} :: site #{ordinal}")
    } else {
        boundary.to_owned()
    }
}

fn vocabulary_term_family(term: &str) -> String {
    let folded = term.to_ascii_lowercase();
    match folded.as_str() {
        "table" | "tables" => "table",
        "row" | "rows" => "row",
        "column" | "columns" => "column",
        "sidecar" | "sidecars" => "sidecar",
        "manifest_version" | "manifest version" | "manifest-version" | "manifestversion" => {
            "manifest_version"
        }
        "manifest_branch" | "manifest branch" | "manifest-branch" | "manifestbranch" => {
            "manifest_branch"
        }
        "manifest/head" => "manifest_head",
        _ => term,
    }
    .to_owned()
}

fn format_legacy_term_keys(keys: &[(LegacyTermKey, usize)]) -> String {
    keys.iter()
        .map(|(key, count)| {
            format!(
                "{}:{}:{}:{}:{}#{} (x{count})",
                key.boundary.surface,
                key.boundary.source_path,
                key.boundary.boundary,
                key.boundary.site_kind,
                key.term_family,
                key.boundary.term_ordinal
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_legacy_keys(keys: &[(LegacyBoundaryKey, usize)]) -> String {
    keys.iter()
        .map(|(key, count)| {
            format!(
                "{}:{}:{}:{}#{} (x{count})",
                key.surface, key.source_path, key.boundary, key.site_kind, key.term_ordinal
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

pub fn scan_rust_tree(root: &Path) -> Result<Vec<InventoryRow>, GuardError> {
    scan_current_rust_strings(root)
}

pub fn scan_user_docs_tree(root: &Path) -> Result<Vec<InventoryRow>, GuardError> {
    scan_current_user_docs(root)
}

pub fn scan_public_rust_tree(
    root: &Path,
    executable: PathBuf,
    target_dir: PathBuf,
) -> Result<Vec<InventoryRow>, GuardError> {
    public_rust::scan_worktree(
        root,
        &public_rust::ToolConfig {
            executable,
            target_dir,
        },
    )
}

pub fn public_api_tool_version() -> &'static str {
    public_rust::CARGO_PUBLIC_API_VERSION
}

pub fn public_api_nightly() -> &'static str {
    public_rust::PINNED_NIGHTLY
}

fn scan_current_rust_strings(root: &Path) -> Result<Vec<InventoryRow>, GuardError> {
    let mut rows = Vec::new();
    for path in current_files(root, Path::new("crates"), is_rust_source_path)? {
        let source_path = path_to_slashes(&path);
        let contents = fs::read_to_string(root.join(&path)).map_err(|source| GuardError::Read {
            path: source_path.clone(),
            source,
        })?;
        rows.extend(rust_strings::scan(&contents, &source_path)?);
    }
    sort_unique_observations(rows, SURFACE_RUST_STRING)
}

fn scan_base_rust_strings(root: &Path, base: &str) -> Result<Vec<InventoryRow>, GuardError> {
    let mut rows = Vec::new();
    for path in base_files(root, base, Path::new("crates"), is_rust_source_path)? {
        let source_path = path_to_slashes(&path);
        let contents = git_show(root, base, &path)?;
        rows.extend(rust_strings::scan(&contents, &source_path)?);
    }
    sort_unique_observations(rows, SURFACE_RUST_STRING)
}

fn scan_current_user_docs(root: &Path) -> Result<Vec<InventoryRow>, GuardError> {
    let mut rows = Vec::new();
    for path in current_files(root, Path::new("docs/user"), is_user_doc_path)? {
        let source_path = path_to_slashes(&path);
        let contents = fs::read_to_string(root.join(&path)).map_err(|source| GuardError::Read {
            path: source_path.clone(),
            source,
        })?;
        rows.extend(markdown::scan(&contents, &source_path));
    }
    sort_unique_observations(rows, SURFACE_USER_DOCS)
}

fn scan_base_user_docs(root: &Path, base: &str) -> Result<Vec<InventoryRow>, GuardError> {
    let mut rows = Vec::new();
    for path in base_files(root, base, Path::new("docs/user"), is_user_doc_path)? {
        let source_path = path_to_slashes(&path);
        let contents = git_show(root, base, &path)?;
        rows.extend(markdown::scan(&contents, &source_path));
    }
    sort_unique_observations(rows, SURFACE_USER_DOCS)
}

fn current_files(
    root: &Path,
    directory: &Path,
    include: fn(&Path) -> bool,
) -> Result<Vec<PathBuf>, GuardError> {
    let absolute = root.join(directory);
    let mut paths = Vec::new();
    for entry in WalkDir::new(&absolute).follow_links(false) {
        let entry = entry.map_err(|error| GuardError::Read {
            path: absolute.display().to_string(),
            source: std::io::Error::other(error),
        })?;
        if !entry.file_type().is_file() {
            continue;
        }
        let relative = entry.path().strip_prefix(root).map_err(|_| {
            GuardError::Git(format!(
                "walked path {} is outside repository {}",
                entry.path().display(),
                root.display()
            ))
        })?;
        if include(relative) {
            paths.push(relative.to_path_buf());
        }
    }
    paths.sort();
    Ok(paths)
}

fn base_files(
    root: &Path,
    base: &str,
    directory: &Path,
    include: fn(&Path) -> bool,
) -> Result<Vec<PathBuf>, GuardError> {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["ls-tree", "-r", "--name-only", "-z", base, "--"])
        .arg(directory)
        .output()
        .map_err(|error| GuardError::Git(format!("cannot enumerate {base}: {error}")))?;
    if !output.status.success() {
        return Err(GuardError::Git(format!(
            "cannot enumerate {base}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    let mut paths = Vec::new();
    for raw in output
        .stdout
        .split(|byte| *byte == 0)
        .filter(|path| !path.is_empty())
    {
        let text = std::str::from_utf8(raw)
            .map_err(|error| GuardError::Git(format!("git path is not UTF-8: {error}")))?;
        let path = PathBuf::from(text);
        if include(&path) {
            paths.push(path);
        }
    }
    paths.sort();
    Ok(paths)
}

fn is_rust_source_path(path: &Path) -> bool {
    path.extension().and_then(|value| value.to_str()) == Some("rs")
        && path.components().any(|part| part.as_os_str() == "src")
        && !path.components().any(|part| part.as_os_str() == "tests")
        && !path
            .file_stem()
            .and_then(|value| value.to_str())
            .is_some_and(|stem| stem == "tests" || stem.ends_with("_tests"))
}

fn is_user_doc_path(path: &Path) -> bool {
    path.extension().and_then(|value| value.to_str()) == Some("md")
}

fn sort_unique_observations(
    mut rows: Vec<InventoryRow>,
    surface: &str,
) -> Result<Vec<InventoryRow>, GuardError> {
    rows.sort_by(|left, right| left.occurrence_id.cmp(&right.occurrence_id));
    for pair in rows.windows(2) {
        if pair[0].occurrence_id == pair[1].occurrence_id {
            return Err(GuardError::Check(format!(
                "{surface} occurrence ID collision at {}",
                pair[0].occurrence_id
            )));
        }
    }
    Ok(rows)
}

fn ensure_openapi_structure_unchanged(base: &str, current: &str) -> Result<(), GuardError> {
    let base: Value = serde_json::from_str(base).map_err(|source| GuardError::Json {
        path: "merge-base openapi.json".to_owned(),
        source,
    })?;
    let current: Value = serde_json::from_str(current).map_err(|source| GuardError::Json {
        path: "current openapi.json".to_owned(),
        source,
    })?;
    if normalize_openapi_presentation(&base, JsonContext::Object)
        != normalize_openapi_presentation(&current, JsonContext::Object)
    {
        return Err(GuardError::Check(
            "OpenAPI changed structurally; this vocabulary tranche permits only description/summary presentation changes"
                .to_owned(),
        ));
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum JsonContext {
    Object,
    NamedMap,
    Literal,
}

fn normalize_openapi_presentation(value: &Value, context: JsonContext) -> Value {
    match value {
        Value::Object(object) => {
            let mut normalized = serde_json::Map::new();
            for (key, value) in object {
                if matches!(context, JsonContext::Object)
                    && matches!(key.as_str(), "description" | "summary")
                {
                    continue;
                }
                let child_context = if matches!(context, JsonContext::Literal)
                    || key.starts_with("x-")
                    || matches!(key.as_str(), "example" | "examples" | "default" | "const")
                {
                    JsonContext::Literal
                } else if is_openapi_named_map(key) {
                    JsonContext::NamedMap
                } else {
                    JsonContext::Object
                };
                let child_context = if matches!(context, JsonContext::NamedMap) {
                    JsonContext::Object
                } else {
                    child_context
                };
                normalized.insert(
                    key.clone(),
                    normalize_openapi_presentation(value, child_context),
                );
            }
            Value::Object(normalized)
        }
        Value::Array(values) => Value::Array(
            values
                .iter()
                .map(|value| normalize_openapi_presentation(value, context))
                .collect(),
        ),
        _ => value.clone(),
    }
}

fn is_openapi_named_map(key: &str) -> bool {
    matches!(
        key,
        "paths"
            | "webhooks"
            | "schemas"
            | "properties"
            | "patternProperties"
            | "$defs"
            | "responses"
            | "headers"
            | "requestBodies"
            | "securitySchemes"
            | "links"
            | "callbacks"
            | "pathItems"
            | "content"
            | "encoding"
            | "variables"
    )
}

fn validate_base_sha(base: &str) -> Result<(), GuardError> {
    if !(7..=64).contains(&base.len()) || !base.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(GuardError::Git(format!(
            "--base must be a hexadecimal commit SHA, got {base:?}"
        )));
    }
    Ok(())
}

fn validate_base_commit(root: &Path, base: &str) -> Result<(), GuardError> {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["cat-file", "-t", base])
        .output()
        .map_err(|error| GuardError::Git(format!("cannot inspect --base {base}: {error}")))?;
    if !output.status.success() {
        return Err(GuardError::Git(format!(
            "--base {base} does not resolve to a repository object: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    let object_type = String::from_utf8(output.stdout)
        .map_err(|error| GuardError::Git(format!("git object type is not UTF-8: {error}")))?;
    if object_type.trim() != "commit" {
        return Err(GuardError::Git(format!(
            "--base {base} must identify a commit object, found {}",
            object_type.trim()
        )));
    }
    Ok(())
}

fn git_root() -> Result<PathBuf, GuardError> {
    let output = Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .output()
        .map_err(|error| GuardError::Git(format!("cannot run git rev-parse: {error}")))?;
    if !output.status.success() {
        return Err(GuardError::Git(format!(
            "cannot find repository root: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    let root = String::from_utf8(output.stdout)
        .map_err(|error| GuardError::Git(format!("repository root is not UTF-8: {error}")))?;
    Ok(PathBuf::from(root.trim()))
}

fn repository_path(root: &Path, path: &Path) -> Result<PathBuf, GuardError> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    };
    let root = root.canonicalize().map_err(|error| {
        GuardError::Git(format!(
            "cannot canonicalize repository root {}: {error}",
            root.display()
        ))
    })?;
    let absolute = absolute.canonicalize().map_err(|error| {
        GuardError::Git(format!(
            "cannot canonicalize {}: {error}",
            absolute.display()
        ))
    })?;
    let relative = absolute.strip_prefix(&root).map_err(|_| {
        GuardError::Git(format!(
            "path {} is outside repository {}",
            absolute.display(),
            root.display()
        ))
    })?;
    if relative
        .components()
        .any(|part| !matches!(part, Component::Normal(_)))
    {
        return Err(GuardError::Git(format!(
            "repository path {} is not normalized",
            relative.display()
        )));
    }
    Ok(relative.to_path_buf())
}

fn git_show(root: &Path, base: &str, path: &Path) -> Result<String, GuardError> {
    let repository_path = path_to_slashes(path);
    let object = format!("{base}:{repository_path}");
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["show", "--format=", "--no-ext-diff", "--no-textconv"])
        .arg(&object)
        .output()
        .map_err(|error| GuardError::Git(format!("cannot run git show {object}: {error}")))?;
    if !output.status.success() {
        return Err(GuardError::Git(format!(
            "cannot read required merge-base asset {object}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    String::from_utf8(output.stdout)
        .map_err(|error| GuardError::Git(format!("{object} is not UTF-8: {error}")))
}

fn path_to_slashes(path: &Path) -> String {
    path.components()
        .filter_map(|part| match part {
            Component::Normal(value) => Some(value.to_string_lossy().into_owned()),
            Component::RootDir => Some(String::new()),
            Component::CurDir => None,
            Component::ParentDir => Some("..".to_owned()),
            Component::Prefix(value) => Some(value.as_os_str().to_string_lossy().into_owned()),
        })
        .collect::<Vec<_>>()
        .join("/")
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum WalkContext {
    Structural,
    Link,
}

struct OpenApiCollector<'a> {
    root: &'a Value,
    source_path: &'a str,
    visited: HashSet<(String, WalkContext)>,
    observations: BTreeMap<String, InventoryRow>,
}

impl<'a> OpenApiCollector<'a> {
    fn new(root: &'a Value, source_path: &'a str) -> Self {
        Self {
            root,
            source_path,
            visited: HashSet::new(),
            observations: BTreeMap::new(),
        }
    }

    fn collect(mut self) -> Result<Vec<InventoryRow>, GuardError> {
        let paths = self
            .root
            .get("paths")
            .and_then(Value::as_object)
            .ok_or_else(|| GuardError::OpenApi("top-level paths must be an object".to_owned()))?;
        for (path, item) in paths {
            if path.starts_with("x-") {
                // `paths` is itself an OpenAPI object, so x-* keys here are
                // extension payloads rather than route templates.
                continue;
            }
            let pointer = format!("/paths/{}", escape_json_pointer_segment(path));
            self.observe(&pointer, "path", path)?;
            self.walk(&pointer, item)?;
        }
        Ok(self.observations.into_values().collect())
    }

    fn walk(&mut self, pointer: &str, value: &Value) -> Result<(), GuardError> {
        self.walk_context(pointer, value, WalkContext::Structural)
    }

    fn walk_context(
        &mut self,
        pointer: &str,
        value: &Value,
        context: WalkContext,
    ) -> Result<(), GuardError> {
        if !self.visited.insert((pointer.to_owned(), context)) {
            return Ok(());
        }
        match value {
            Value::Object(object) => {
                if let (Some(name), Some(location)) = (
                    object.get("name").and_then(Value::as_str),
                    object.get("in").and_then(Value::as_str),
                ) && matches!(location, "path" | "query" | "header" | "cookie")
                {
                    self.observe(&format!("{pointer}/name"), "parameter_name", name)?;
                }
                if let Some(reference) = object.get("$ref") {
                    let reference = reference.as_str().ok_or_else(|| {
                        GuardError::OpenApi(format!("$ref at {pointer} must be a string"))
                    })?;
                    let (target_pointer, target) = self.resolve_reference(reference)?;
                    if let Some((component_pointer, component_name)) =
                        schema_component_site(&target_pointer)?
                    {
                        self.observe(&component_pointer, "component_name", &component_name)?;
                    }
                    self.walk_context(&target_pointer, &target, context)?;
                }

                for (key, child) in object {
                    if key == "$ref" {
                        continue;
                    }
                    if context == WalkContext::Link
                        && matches!(key.as_str(), "parameters" | "requestBody")
                    {
                        // Link values can be arbitrary constants or runtime
                        // expressions, not nested OpenAPI objects.
                        continue;
                    }
                    if key.starts_with("x-") {
                        // OpenAPI extensions are arbitrary application data, not
                        // part of the vocabulary boundary owned by this guard.
                        continue;
                    }
                    let child_pointer = format!("{pointer}/{}", escape_json_pointer_segment(key));
                    match key.as_str() {
                        // These JSON Schema/OpenAPI fields contain literal user
                        // values. A `$ref`, `description`, or vocabulary-shaped
                        // key inside them is data rather than schema structure.
                        "example" | "default" | "value" => {}
                        "examples" if child.is_array() => {}
                        "links" if child.is_object() => {
                            self.walk_named_link_map(&child_pointer, child)?;
                        }
                        // Definitions enter the operation closure only when a
                        // schema `$ref` targets them. Do not inventory unused
                        // siblings merely because their parent is reachable.
                        "$defs" if child.is_object() => {}
                        "examples" | "callbacks" | "encoding" | "variables"
                        | "patternProperties" | "dependentSchemas"
                            if child.is_object() =>
                        {
                            self.walk_named_map(&child_pointer, child)?;
                        }
                        "properties" if child.is_object() => {
                            let properties = child.as_object().expect("object checked above");
                            for (name, schema) in properties {
                                let property_pointer = format!(
                                    "{child_pointer}/{}",
                                    escape_json_pointer_segment(name)
                                );
                                self.observe(&property_pointer, "property_name", name)?;
                                self.walk(&property_pointer, schema)?;
                            }
                        }
                        "required" if child.is_array() => {
                            let required = child.as_array().expect("array checked above");
                            for (index, name) in required.iter().enumerate() {
                                let name = name.as_str().ok_or_else(|| {
                                    GuardError::OpenApi(format!(
                                        "required entry at {child_pointer}/{index} must be a string"
                                    ))
                                })?;
                                self.observe(
                                    &format!("{child_pointer}/{index}"),
                                    "required_name",
                                    name,
                                )?;
                            }
                        }
                        "propertyName" if pointer.ends_with("/discriminator") => {
                            let name = child.as_str().ok_or_else(|| {
                                GuardError::OpenApi(format!(
                                    "discriminator propertyName at {child_pointer} must be a string"
                                ))
                            })?;
                            self.observe(&child_pointer, "discriminator_name", name)?;
                        }
                        "mapping" if pointer.ends_with("/discriminator") && child.is_object() => {
                            self.walk_discriminator_mapping(&child_pointer, child)?;
                        }
                        "enum" => {
                            let values = child.as_array().ok_or_else(|| {
                                GuardError::OpenApi(format!(
                                    "enum at {child_pointer} must be an array"
                                ))
                            })?;
                            for (index, enum_value) in values.iter().enumerate() {
                                if let Some(enum_value) = enum_value.as_str() {
                                    self.observe(
                                        &format!("{child_pointer}/{index}"),
                                        "enum_value",
                                        enum_value,
                                    )?;
                                }
                            }
                        }
                        "const" => {
                            if let Some(constant) = child.as_str() {
                                self.observe(&child_pointer, "enum_value", constant)?;
                            }
                        }
                        "description" | "summary" | "title" => {
                            let prose = child.as_str().ok_or_else(|| {
                                GuardError::OpenApi(format!(
                                    "prose at {child_pointer} must be a string"
                                ))
                            })?;
                            self.observe(&child_pointer, "prose", prose)?;
                        }
                        "operationId" => {
                            let operation_id = child.as_str().ok_or_else(|| {
                                GuardError::OpenApi(format!(
                                    "operationId at {child_pointer} must be a string"
                                ))
                            })?;
                            self.observe(&child_pointer, "operation_id", operation_id)?;
                        }
                        "headers" if child.is_object() => {
                            for (name, header) in child.as_object().expect("object checked above") {
                                let header_pointer = format!(
                                    "{child_pointer}/{}",
                                    escape_json_pointer_segment(name)
                                );
                                self.observe(&header_pointer, "header_name", name)?;
                                self.walk(&header_pointer, header)?;
                            }
                        }
                        _ => self.walk(&child_pointer, child)?,
                    }
                }
            }
            Value::Array(values) => {
                for (index, child) in values.iter().enumerate() {
                    self.walk(&format!("{pointer}/{index}"), child)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn walk_named_map(&mut self, pointer: &str, value: &Value) -> Result<(), GuardError> {
        let entries = value.as_object().ok_or_else(|| {
            GuardError::OpenApi(format!("named map at {pointer} must be an object"))
        })?;
        for (name, entry) in entries {
            // These are user-selected map entry names. An x-* name is not a
            // specification extension; extensions inside the entry object are
            // still skipped by `walk`.
            self.walk(
                &format!("{pointer}/{}", escape_json_pointer_segment(name)),
                entry,
            )?;
        }
        Ok(())
    }

    fn walk_discriminator_mapping(
        &mut self,
        pointer: &str,
        value: &Value,
    ) -> Result<(), GuardError> {
        let entries = value.as_object().ok_or_else(|| {
            GuardError::OpenApi(format!(
                "discriminator mapping at {pointer} must be an object"
            ))
        })?;
        for (name, target) in entries {
            let entry_pointer = format!("{pointer}/{}", escape_json_pointer_segment(name));
            self.observe(&entry_pointer, "enum_value", name)?;
            let target = target.as_str().ok_or_else(|| {
                GuardError::OpenApi(format!(
                    "discriminator mapping target at {entry_pointer} must be a string"
                ))
            })?;
            let reference = if target.starts_with('#') {
                target.to_owned()
            } else if target.contains('/') || target.contains(':') {
                return Err(GuardError::OpenApi(format!(
                    "external discriminator mapping target {target:?} at {entry_pointer} is not allowed"
                )));
            } else {
                format!(
                    "#/components/schemas/{}",
                    percent_encode_json_pointer_segment(target)
                )
            };
            let (target_pointer, target_schema) = self.resolve_reference(&reference)?;
            if let Some((component_pointer, component_name)) =
                schema_component_site(&target_pointer)?
            {
                self.observe(&component_pointer, "component_name", &component_name)?;
            }
            self.walk(&target_pointer, &target_schema)?;
        }
        Ok(())
    }

    fn walk_named_link_map(&mut self, pointer: &str, value: &Value) -> Result<(), GuardError> {
        let entries = value.as_object().ok_or_else(|| {
            GuardError::OpenApi(format!("Link map at {pointer} must be an object"))
        })?;
        for (name, entry) in entries {
            let entry_pointer = format!("{pointer}/{}", escape_json_pointer_segment(name));
            if !entry.is_object() {
                return Err(GuardError::OpenApi(format!(
                    "Link or Reference Object at {entry_pointer} must be an object"
                )));
            }
            self.walk_context(&entry_pointer, entry, WalkContext::Link)?;
        }
        Ok(())
    }

    fn resolve_reference(&self, reference: &str) -> Result<(String, Value), GuardError> {
        if !reference.starts_with('#') {
            return Err(GuardError::OpenApi(format!(
                "external $ref {reference:?} is not allowed"
            )));
        }
        let fragment = percent_decode(&reference[1..])?;
        if !fragment.is_empty() && !fragment.starts_with('/') {
            return Err(GuardError::OpenApi(format!(
                "local $ref {reference:?} is not a JSON pointer"
            )));
        }
        let segments = parse_json_pointer(&fragment)?;
        let pointer = canonical_json_pointer(&segments);
        let mut value = self.root;
        for segment in &segments {
            value = match value {
                Value::Object(object) => object
                    .get(segment)
                    .ok_or_else(|| GuardError::OpenApi(format!("dangling $ref {reference:?}")))?,
                Value::Array(values) => {
                    let index = parse_array_index(segment, reference)?;
                    values.get(index).ok_or_else(|| {
                        GuardError::OpenApi(format!("dangling $ref {reference:?}"))
                    })?
                }
                _ => {
                    return Err(GuardError::OpenApi(format!("dangling $ref {reference:?}")));
                }
            };
        }
        Ok((pointer, value.clone()))
    }

    fn observe(
        &mut self,
        boundary: &str,
        site_kind: &str,
        containing_value: &str,
    ) -> Result<(), GuardError> {
        let fingerprint = containing_value_fingerprint(containing_value);
        let matches = vocabulary_matches(containing_value);
        let mut ordinals: BTreeMap<String, usize> = BTreeMap::new();
        for found in matches {
            let ordinal = ordinals.entry(found.term.clone()).or_default();
            *ordinal += 1;
            let occurrence_id = format!(
                "openapi:{}:{site_kind}:{fingerprint}:{}:{}",
                percent_encode(boundary),
                percent_encode(&found.term),
                *ordinal
            );
            let row = InventoryRow {
                schema_version: SCHEMA_VERSION.to_owned(),
                occurrence_id: occurrence_id.clone(),
                surface: SURFACE_OPENAPI.to_owned(),
                source_path: self.source_path.to_owned(),
                boundary: boundary.to_owned(),
                site_kind: site_kind.to_owned(),
                current_term: found.term,
                target_term: String::new(),
                classification: String::new(),
                compatibility_treatment: String::new(),
                rollout_phase: String::new(),
                test_owner: String::new(),
                rationale: String::new(),
            };
            match self.observations.get(&occurrence_id) {
                Some(existing) if existing == &row => {}
                Some(_) => {
                    return Err(GuardError::OpenApi(format!(
                        "occurrence ID collision at {occurrence_id}"
                    )));
                }
                None => {
                    self.observations.insert(occurrence_id, row);
                }
            }
        }
        Ok(())
    }
}

fn parse_array_index(segment: &str, reference: &str) -> Result<usize, GuardError> {
    let valid = segment == "0"
        || (segment
            .as_bytes()
            .first()
            .is_some_and(|byte| matches!(byte, b'1'..=b'9'))
            && segment.as_bytes().iter().all(u8::is_ascii_digit));
    if !valid {
        return Err(GuardError::OpenApi(format!(
            "local $ref {reference:?} has an invalid array index {segment:?}"
        )));
    }
    segment.parse::<usize>().map_err(|_| {
        GuardError::OpenApi(format!(
            "local $ref {reference:?} has an out-of-range array index {segment:?}"
        ))
    })
}

fn schema_component_site(pointer: &str) -> Result<Option<(String, String)>, GuardError> {
    let segments = parse_json_pointer(pointer)?;
    if let Some(definitions_index) = segments.iter().position(|segment| segment == "$defs")
        && let Some(name) = segments.get(definitions_index + 1)
    {
        Ok(Some((
            canonical_json_pointer(&segments[..definitions_index + 2]),
            name.clone(),
        )))
    } else if segments.len() >= 3 && segments[0] == "components" && segments[1] == "schemas" {
        Ok(Some((
            canonical_json_pointer(&segments[..3]),
            segments[2].clone(),
        )))
    } else {
        Ok(None)
    }
}

fn parse_json_pointer(pointer: &str) -> Result<Vec<String>, GuardError> {
    if pointer.is_empty() {
        return Ok(Vec::new());
    }
    pointer[1..]
        .split('/')
        .map(|segment| unescape_json_pointer_segment(segment, pointer))
        .collect()
}

fn unescape_json_pointer_segment(segment: &str, pointer: &str) -> Result<String, GuardError> {
    let mut output = String::with_capacity(segment.len());
    let mut chars = segment.chars();
    while let Some(character) = chars.next() {
        if character != '~' {
            output.push(character);
            continue;
        }
        match chars.next() {
            Some('0') => output.push('~'),
            Some('1') => output.push('/'),
            _ => {
                return Err(GuardError::OpenApi(format!(
                    "invalid JSON pointer escape in {pointer:?}"
                )));
            }
        }
    }
    Ok(output)
}

fn canonical_json_pointer(segments: &[String]) -> String {
    if segments.is_empty() {
        String::new()
    } else {
        format!(
            "/{}",
            segments
                .iter()
                .map(|segment| escape_json_pointer_segment(segment))
                .collect::<Vec<_>>()
                .join("/")
        )
    }
}

fn escape_json_pointer_segment(segment: &str) -> String {
    segment.replace('~', "~0").replace('/', "~1")
}

fn percent_encode_json_pointer_segment(segment: &str) -> String {
    percent_encode(&escape_json_pointer_segment(segment))
}

fn percent_decode(value: &str) -> Result<String, GuardError> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b'%' {
            decoded.push(bytes[index]);
            index += 1;
            continue;
        }
        if index + 2 >= bytes.len() {
            return Err(GuardError::OpenApi(format!(
                "invalid percent escape in local $ref fragment {value:?}"
            )));
        }
        let high = hex_value(bytes[index + 1]);
        let low = hex_value(bytes[index + 2]);
        match (high, low) {
            (Some(high), Some(low)) => decoded.push(high * 16 + low),
            _ => {
                return Err(GuardError::OpenApi(format!(
                    "invalid percent escape in local $ref fragment {value:?}"
                )));
            }
        }
        index += 3;
    }
    String::from_utf8(decoded)
        .map_err(|error| GuardError::OpenApi(format!("local $ref fragment is not UTF-8: {error}")))
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn containing_value_fingerprint(value: &str) -> String {
    let mut normalized = String::with_capacity(value.len());
    let mut pending_space = false;
    for character in value.chars() {
        if character.is_whitespace() {
            if !normalized.is_empty() {
                pending_space = true;
            }
        } else {
            if pending_space {
                normalized.push(' ');
                pending_space = false;
            }
            normalized.push(character);
        }
    }
    let digest = Sha256::digest(normalized.as_bytes());
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write;
        write!(encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
}

fn percent_encode(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut output = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            output.push(char::from(byte));
        } else {
            output.push('%');
            output.push(char::from(HEX[usize::from(byte >> 4)]));
            output.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
    }
    output
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct VocabularyMatch {
    start: usize,
    end: usize,
    term: String,
}

fn vocabulary_matches(value: &str) -> Vec<VocabularyMatch> {
    const TERMS: &[&str] = &[
        "manifest_version",
        "manifest_branch",
        "sidecars",
        "sidecar",
        "columns",
        "column",
        "tables",
        "table",
        "rows",
        "row",
    ];
    let folded = value.to_ascii_lowercase();
    let mut matches = BTreeSet::new();
    for term in TERMS {
        for (start, _) in folded.match_indices(term) {
            let end = start + term.len();
            let valid = if term.starts_with("manifest_") {
                exact_snake_boundary(value.as_bytes(), start, end)
            } else {
                camel_token_boundary(value.as_bytes(), start, end)
            };
            if valid {
                matches.insert(VocabularyMatch {
                    start,
                    end,
                    term: value[start..end].to_owned(),
                });
            }
        }
    }
    for phrase in [
        "manifest version",
        "manifest-version",
        "manifest branch",
        "manifest-branch",
        "manifest/head",
    ] {
        for (start, _) in folded.match_indices(phrase) {
            let end = start + phrase.len();
            if phrase_boundary(value.as_bytes(), start, end) {
                matches.insert(VocabularyMatch {
                    start,
                    end,
                    term: value[start..end].to_owned(),
                });
            }
        }
    }
    for compact in ["manifestversion", "manifestbranch"] {
        for (start, _) in folded.match_indices(compact) {
            let end = start + compact.len();
            if camel_token_boundary(value.as_bytes(), start, end) {
                matches.insert(VocabularyMatch {
                    start,
                    end,
                    term: value[start..end].to_owned(),
                });
            }
        }
    }
    matches.into_iter().collect()
}

fn phrase_boundary(bytes: &[u8], start: usize, end: usize) -> bool {
    (start == 0 || !bytes[start - 1].is_ascii_alphanumeric())
        && (end == bytes.len() || !bytes[end].is_ascii_alphanumeric())
}

fn exact_snake_boundary(bytes: &[u8], start: usize, end: usize) -> bool {
    let is_identifier = |byte: u8| byte.is_ascii_alphanumeric() || byte == b'_';
    (start == 0 || !is_identifier(bytes[start - 1]))
        && (end == bytes.len() || !is_identifier(bytes[end]))
}

fn camel_token_boundary(bytes: &[u8], start: usize, end: usize) -> bool {
    let start_boundary = if start == 0 || !bytes[start - 1].is_ascii_alphanumeric() {
        true
    } else {
        let current = bytes[start];
        let previous = bytes[start - 1];
        current.is_ascii_uppercase()
            && (previous.is_ascii_lowercase()
                || previous.is_ascii_digit()
                || (previous.is_ascii_uppercase()
                    && bytes.get(start + 1).is_some_and(u8::is_ascii_lowercase)))
    };
    let end_boundary = if end == bytes.len() || !bytes[end].is_ascii_alphanumeric() {
        true
    } else {
        let next = bytes[end];
        let previous = bytes[end - 1];
        next.is_ascii_uppercase()
            && (previous.is_ascii_lowercase()
                || previous.is_ascii_digit()
                || bytes.get(end + 1).is_some_and(u8::is_ascii_lowercase))
    };
    start_boundary && end_boundary
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::fs;
    use std::path::Path;
    use std::process::Command;

    use serde_json::{Map, Value, json};
    use tempfile::TempDir;

    use super::*;

    const INVENTORY_PATH: &str = "tools/omnigraph-vocabulary-guard/graph-vocabulary-inventory.tsv";
    const CLOSURE_FIXTURE: &str = include_str!("../tests/fixtures/openapi-closure.json");
    const SHIPPED_INVENTORY: &str =
        include_str!("../../../tools/omnigraph-vocabulary-guard/graph-vocabulary-inventory.tsv");
    const SHIPPED_OPENAPI: &str = include_str!("../../../openapi.json");

    #[test]
    fn openapi_inventory_matches() {
        let rows = scan_openapi(SHIPPED_OPENAPI, "openapi.json").expect("scan shipped spec");
        let inventory = parse_inventory(SHIPPED_INVENTORY).expect("parse shipped inventory");
        compare_inventory("shipped tree", &rows, &inventory)
            .expect("shipped inventory must classify every observation exactly once");
        let classified_openapi_rows = inventory
            .iter()
            .filter(|row| row.surface == SURFACE_OPENAPI)
            .count();
        assert!(!rows.is_empty());
        assert_eq!(rows.len(), classified_openapi_rows);
        let tsv = render_tsv(&rows).expect("render observations");
        assert_eq!(tsv.lines().count(), rows.len() + 1);
        assert!(tsv.starts_with(INVENTORY_HEADER));
        assert!(
            rows.windows(2)
                .all(|pair| pair[0].occurrence_id < pair[1].occurrence_id)
        );
    }

    #[test]
    fn collector_follows_nested_composed_and_cyclic_refs_and_excludes_unreachable_components() {
        let rows = scan_openapi(CLOSURE_FIXTURE, "fixture.json").expect("scan closure fixture");
        let kinds: BTreeSet<_> = rows.iter().map(|row| row.site_kind.as_str()).collect();
        for expected in [
            "component_name",
            "header_name",
            "operation_id",
            "parameter_name",
            "path",
            "property_name",
            "required_name",
            "discriminator_name",
            "enum_value",
            "prose",
        ] {
            assert!(kinds.contains(expected), "missing site kind {expected}");
        }
        assert!(rows.iter().any(|row| {
            row.boundary == "/components/schemas/CycleTable/properties/table~1key~0name"
                && row.current_term == "table"
        }));
        assert!(rows.iter().any(|row| {
            row.boundary == "/components/schemas/Table~1Rows~0Thing" && row.current_term == "Table"
        }));
        assert!(rows.iter().any(|row| {
            row.boundary == "/components/schemas/Table~1Rows~0Thing" && row.current_term == "Rows"
        }));
        assert!(rows.iter().any(|row| {
            row.boundary == "/components/pathItems/Table Path/get/operationId"
                && row.current_term == "Column"
        }));
        assert!(
            rows.iter()
                .all(|row| !row.boundary.contains("UnreachableTable"))
        );
        assert!(
            rows.iter()
                .all(|row| row.current_term != "Arrow" && row.current_term != "narrow")
        );
        assert!(rows.iter().all(|row| {
            !row.boundary.contains("expected_manifest_version")
                || row.current_term != "manifest_version"
        }));
    }

    #[test]
    fn collector_observes_reachable_defs_names_but_not_unreachable_defs() {
        let spec = json!({
            "openapi": "3.1.0",
            "paths": {"/items": {"get": {"responses": {"200": {
                "description": "ok",
                "content": {"application/json": {"schema": {
                    "$ref": "#/components/schemas/Root/$defs/TableRows"
                }}}
            }}}}},
            "components": {"schemas": {
                "Root": {"$defs": {
                    "TableRows": {"type": "string"},
                    "UnusedTable": {"description": "unused rows"}
                }},
                "Unused": {"$defs": {"UnreachableTable": {"type": "string"}}}
            }}
        })
        .to_string();
        let rows = scan_openapi(&spec, "fixture.json").expect("scan local definitions");
        assert!(rows.iter().any(|row| {
            row.site_kind == "component_name"
                && row.boundary == "/components/schemas/Root/$defs/TableRows"
                && row.current_term == "Table"
        }));
        assert!(
            rows.iter()
                .all(|row| !row.boundary.contains("UnreachableTable"))
        );
        assert!(rows.iter().all(|row| !row.boundary.contains("UnusedTable")));
    }

    #[test]
    fn collector_observes_discriminator_mapping_keys_and_follows_mapping_targets() {
        let spec = json!({
            "openapi": "3.1.0",
            "paths": {"/items": {"get": {"responses": {"200": {
                "description": "ok",
                "content": {"application/json": {"schema": {
                    "$ref": "#/components/schemas/Root"
                }}}
            }}}}},
            "components": {"schemas": {
                "Root": {
                    "type": "object",
                    "discriminator": {
                        "propertyName": "kind",
                        "mapping": {"table-row": "#/components/schemas/MappedTableRows"}
                    }
                },
                "MappedTableRows": {"description": "mapped column"},
                "UnreachableTable": {"description": "unreachable rows"}
            }}
        })
        .to_string();
        let rows = scan_openapi(&spec, "fixture.json").expect("scan discriminator mapping");
        assert!(rows.iter().any(|row| {
            row.site_kind == "enum_value"
                && row.boundary == "/components/schemas/Root/discriminator/mapping/table-row"
                && row.current_term == "table"
        }));
        assert!(rows.iter().any(|row| {
            row.site_kind == "component_name"
                && row.boundary == "/components/schemas/MappedTableRows"
                && row.current_term == "Table"
        }));
        assert!(rows.iter().any(|row| {
            row.boundary == "/components/schemas/MappedTableRows/description"
                && row.current_term == "column"
        }));
        assert!(
            rows.iter()
                .all(|row| !row.boundary.contains("UnreachableTable"))
        );
    }

    #[test]
    fn collector_rejects_dangling_external_and_malformed_local_refs() {
        for (reference, expected) in [
            ("#/components/schemas/Missing", "dangling"),
            ("other.json#/components/schemas/Table", "external"),
            ("#not-a-pointer", "not a JSON pointer"),
            ("#/components/schemas/Bad~2Escape", "invalid JSON pointer"),
        ] {
            let spec = json!({
                "paths": {"/x": {"get": {"responses": {"200": {
                    "description": "ok",
                    "content": {"application/json": {"schema": {"$ref": reference}}}
                }}}}},
                "components": {"schemas": {}}
            })
            .to_string();
            let error = scan_openapi(&spec, "fixture.json").expect_err("reference must fail");
            assert!(error.to_string().contains(expected), "{error}");
        }
    }

    #[test]
    fn collector_ignores_literal_example_default_const_and_extension_payloads() {
        let spec = json!({
            "openapi": "3.1.0",
            "paths": {
                "x-table-extension": {"$ref": "https://example.invalid/path-extension"},
                "/items": {
                    "get": {
                        "responses": {
                            "200": {
                                "description": "ok",
                                "content": {
                                    "application/json": {
                                        "schema": {
                                            "type": "object",
                                            "example": {"$ref": "https://example.invalid/example", "table_key": "literal"},
                                            "examples": [{"$ref": "https://example.invalid/examples", "row_id": "literal"}],
                                            "default": {"$ref": "https://example.invalid/default", "column": "literal"},
                                            "const": {"$ref": "https://example.invalid/const", "sidecar": "literal"},
                                            "x-literal": {"$ref": "https://example.invalid/extension", "table": "literal"}
                                        },
                                        "example": {"$ref": "https://example.invalid/media-example", "rows": []},
                                        "examples": {
                                            "x-name": {
                                                "summary": "table example",
                                                "value": {"$ref": "https://example.invalid/value", "columns": []}
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        })
        .to_string();

        let rows = scan_openapi(&spec, "fixture.json").expect("literal payloads are not refs");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].current_term, "table");
        assert_eq!(
            rows[0].boundary,
            "/paths/~1items/get/responses/200/content/application~1json/examples/x-name/summary"
        );
    }

    #[test]
    fn collector_rejects_noncanonical_array_pointer_indices() {
        for segment in ["00", "+0", "-0"] {
            let reference = format!("#/components/schemas/Choice/anyOf/{segment}");
            let spec = json!({
                "openapi": "3.1.0",
                "paths": {"/x": {"get": {"responses": {"200": {
                    "description": "ok",
                    "content": {"application/json": {"schema": {"$ref": reference}}}
                }}}}},
                "components": {"schemas": {"Choice": {"anyOf": [{"type": "string"}]}}}
            })
            .to_string();
            let error = scan_openapi(&spec, "fixture.json").expect_err("index must fail");
            assert!(error.to_string().contains("invalid array index"), "{error}");
        }
    }

    #[test]
    fn collector_treats_link_parameters_and_request_body_as_literal_values() {
        let spec = json!({
            "openapi": "3.1.0",
            "paths": {"/items": {"get": {"responses": {"200": {
                "description": "ok",
                "links": {"x-name": {
                    "description": "row link",
                    "operationId": "getItems",
                    "parameters": {
                        "id": {"$ref": "https://example.invalid/link-parameter", "table": "literal"}
                    },
                    "requestBody": {"$ref": "https://example.invalid/link-body", "column": "literal"}
                }}
            }}}}}
        })
        .to_string();

        let rows = scan_openapi(&spec, "fixture.json").expect("Link constants are not refs");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].current_term, "row");
        assert_eq!(
            rows[0].boundary,
            "/paths/~1items/get/responses/200/links/x-name/description"
        );
    }

    #[test]
    fn collector_preserves_link_literal_context_through_references() {
        let spec = json!({
            "openapi": "3.1.0",
            "paths": {"/items": {"get": {"responses": {"200": {
                "description": "ok",
                "links": {"shared": {"$ref": "#/components/links/Shared"}}
            }}}}},
            "components": {"links": {"Shared": {
                "description": "row link",
                "operationId": "getItems",
                "parameters": {
                    "id": {"$ref": "https://example.invalid/link-parameter", "table": "literal"}
                },
                "requestBody": {"$ref": "https://example.invalid/link-body", "column": "literal"}
            }}}
        })
        .to_string();

        let rows = scan_openapi(&spec, "fixture.json").expect("referenced Link keeps its context");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].current_term, "row");
        assert_eq!(rows[0].boundary, "/components/links/Shared/description");
    }

    #[test]
    fn matcher_is_camel_snake_and_kebab_aware_without_arrow_false_positives() {
        let found: Vec<_> = vocabulary_matches(
            "TableRows table_key row-count SIDECAR_COLUMNS Arrow narrow tablecloth expected_manifest_version manifest_version Manifest Version manifest-branch manifest/head drift ExpectedManifestVersion manifestBranch manifestation versioning MANIFEST_VERSION MANIFEST_BRANCH EXPECTED_MANIFEST_VERSION",
        )
        .into_iter()
        .map(|found| found.term)
        .collect();
        assert_eq!(
            found,
            [
                "Table",
                "Rows",
                "table",
                "row",
                "SIDECAR",
                "COLUMNS",
                "manifest_version",
                "Manifest Version",
                "manifest-branch",
                "manifest/head",
                "ManifestVersion",
                "manifestBranch",
                "MANIFEST_VERSION",
                "MANIFEST_BRANCH",
            ]
        );
    }

    #[test]
    fn occurrence_fingerprint_normalizes_whitespace_but_keeps_exact_term_spelling() {
        let compact = minimal_spec(&[("base", "table rows")]);
        let spaced = minimal_spec(&[("base", "  table\n\t rows  ")]);
        let compact_ids: Vec<_> = scan_openapi(&compact, "openapi.json")
            .expect("compact scan")
            .into_iter()
            .map(|row| row.occurrence_id)
            .collect();
        let spaced_ids: Vec<_> = scan_openapi(&spaced, "openapi.json")
            .expect("spaced scan")
            .into_iter()
            .map(|row| row.occurrence_id)
            .collect();
        assert_eq!(compact_ids, spaced_ids);

        let capitalized = minimal_spec(&[("base", "Table rows")]);
        let capitalized_ids: Vec<_> = scan_openapi(&capitalized, "openapi.json")
            .expect("capitalized scan")
            .into_iter()
            .map(|row| row.occurrence_id)
            .collect();
        assert_ne!(compact_ids, capitalized_ids);
    }

    #[test]
    fn inventory_parser_rejects_duplicate_unsorted_unknown_and_incomplete_rows() {
        let observations = scan_openapi(
            &minimal_spec(&[("a", "table"), ("b", "row")]),
            "openapi.json",
        )
        .expect("scan fixture");
        let reviewed = review_rows(observations, |_| "physical_storage");
        assert_eq!(
            parse_inventory(&render_tsv(&reviewed).unwrap()).unwrap(),
            reviewed
        );

        let duplicate = raw_inventory(&[reviewed[0].clone(), reviewed[0].clone()]);
        assert_error_contains(parse_inventory(&duplicate), "duplicate occurrence_id");

        let unsorted = raw_inventory(&[reviewed[1].clone(), reviewed[0].clone()]);
        assert_error_contains(parse_inventory(&unsorted), "not strictly sorted");

        let mut unknown_surface = reviewed[0].clone();
        unknown_surface.surface = "openpai".to_owned();
        assert_error_contains(
            parse_inventory(&raw_inventory(&[unknown_surface])),
            "unknown surface",
        );

        let mut unknown_classification = reviewed[0].clone();
        unknown_classification.classification = "looks_fine".to_owned();
        assert_error_contains(
            parse_inventory(&raw_inventory(&[unknown_classification])),
            "unknown or empty classification",
        );

        let mut unknown_kind = reviewed[0].clone();
        unknown_kind.site_kind = "property_nmae".to_owned();
        assert_error_contains(
            parse_inventory(&raw_inventory(&[unknown_kind])),
            "unknown openapi site_kind",
        );

        for field in [
            "target_term",
            "compatibility_treatment",
            "rollout_phase",
            "test_owner",
            "rationale",
        ] {
            let mut incomplete = reviewed[0].clone();
            match field {
                "target_term" => incomplete.target_term.clear(),
                "compatibility_treatment" => incomplete.compatibility_treatment.clear(),
                "rollout_phase" => incomplete.rollout_phase.clear(),
                "test_owner" => incomplete.test_owner.clear(),
                "rationale" => incomplete.rationale.clear(),
                _ => unreachable!(),
            }
            assert_error_contains(
                parse_inventory(&raw_inventory(&[incomplete])),
                &format!("empty review-owned {field}"),
            );
        }

        let mut wrong_phase_marker = reviewed[0].clone();
        wrong_phase_marker.compatibility_treatment = REVIEWED_SEMANTIC_RECLASSIFICATION.to_owned();
        assert_error_contains(
            parse_inventory(&raw_inventory(&[wrong_phase_marker])),
            "non-legacy presentation-phase semantic rewrite",
        );

        let mut legacy_marker = reviewed[0].clone();
        legacy_marker.classification = "legacy_graph_misuse".to_owned();
        legacy_marker.rollout_phase = "presentation".to_owned();
        legacy_marker.compatibility_treatment = REVIEWED_SEMANTIC_RECLASSIFICATION.to_owned();
        assert_error_contains(
            parse_inventory(&raw_inventory(&[legacy_marker])),
            "non-legacy presentation-phase semantic rewrite",
        );

        let mut valid_marker = reviewed[0].clone();
        valid_marker.rollout_phase = "presentation".to_owned();
        valid_marker.compatibility_treatment = REVIEWED_SEMANTIC_RECLASSIFICATION.to_owned();
        parse_inventory(&raw_inventory(&[valid_marker]))
            .expect("the exact reviewed transition marker has one closed valid shape");
    }

    #[test]
    fn inventory_parser_enforces_closed_site_kinds_for_every_surface() {
        let observation = scan_openapi(&minimal_spec(&[("base", "table")]), "openapi.json")
            .expect("scan")
            .remove(0);
        for (surface, site_kind) in [
            ("rust_string", "thiserror_attribute"),
            ("user_docs", "paragraph"),
            ("public_rust", "public_signature"),
        ] {
            let mut row = review_rows(vec![observation.clone()], |_| "physical_storage").remove(0);
            row.surface = surface.to_owned();
            row.site_kind = site_kind.to_owned();
            parse_inventory(&raw_inventory(&[row.clone()])).expect("owned site kind must parse");
            row.site_kind = "future_owned_kind".to_owned();
            assert_error_contains(
                parse_inventory(&raw_inventory(&[row])),
                &format!("unknown {surface} site_kind"),
            );
        }
    }

    #[test]
    fn merge_base_check_accepts_reviewed_nonlegacy_classifications_and_legacy_removal() {
        let base_spec = minimal_spec(&[("base", "legacy table")]);
        let repo = TestRepo::with_reviewed_base(&base_spec, |_| "legacy_graph_misuse");
        check_repo(&repo).expect("unchanged baseline must pass");

        let current_spec = minimal_spec(&[
            ("base", "legacy table"),
            ("physical", "physical column"),
            ("projection", "result rows"),
            ("renderer", "renderer table"),
            ("tabular", "tabular rows"),
            ("upstream", "upstream Table"),
            ("quoted", "quoted sidecar"),
        ]);
        repo.write_current(&current_spec, |row| {
            if row.boundary.contains("~1base") {
                "legacy_graph_misuse"
            } else if row.boundary.contains("~1physical") {
                "physical_storage"
            } else if row.boundary.contains("~1projection") {
                "result_projection"
            } else if row.boundary.contains("~1renderer") {
                "renderer"
            } else if row.boundary.contains("~1tabular") {
                "tabular_presentation"
            } else if row.boundary.contains("~1upstream") {
                "upstream_name"
            } else if row.boundary.contains("~1quoted") {
                "quoted_compatibility"
            } else {
                panic!("unexpected row {row:?}")
            }
        });
        check_repo(&repo).expect("reviewed nonlegacy additions must pass");

        let no_vocabulary = minimal_spec(&[("base", "ordinary graph object")]);
        repo.write_current(&no_vocabulary, |_| unreachable!());
        check_repo(&repo).expect("a legacy occurrence may disappear");
    }

    #[test]
    fn merge_base_check_rejects_unclassified_and_stale_current_rows() {
        let base_spec = minimal_spec(&[("base", "legacy table")]);
        let repo = TestRepo::with_reviewed_base(&base_spec, |_| "legacy_graph_misuse");

        fs::write(
            repo.root().join("openapi.json"),
            minimal_spec(&[("base", "legacy table"), ("new", "physical column")]),
        )
        .unwrap();
        assert_error_contains(check_repo(&repo), "unclassified occurrence");

        fs::write(
            repo.root().join("openapi.json"),
            minimal_spec(&[("base", "ordinary graph object")]),
        )
        .unwrap();
        assert_error_contains(check_repo(&repo), "stale inventory row");
    }

    #[test]
    fn merge_base_check_rejects_same_count_legacy_substitution_and_laundering() {
        let base_spec = minimal_spec(&[("base", "legacy table")]);
        let repo = TestRepo::with_reviewed_base(&base_spec, |_| "legacy_graph_misuse");

        let substituted = minimal_spec(&[("base", "legacy row")]);
        repo.write_current(&substituted, |_| "legacy_graph_misuse");
        assert_error_contains(check_repo(&repo), "introduces legacy_graph_misuse");

        repo.write_current(&base_spec, |_| "physical_storage");
        assert_error_contains(
            check_repo(&repo),
            "reclassifies surviving legacy_graph_misuse",
        );
    }

    #[test]
    fn legacy_classification_survives_changed_fingerprint_and_term_at_same_rust_site() {
        let source_path = "crates/example/src/lib.rs";
        let base = review_rows(
            rust_strings::scan(
                r#"fn render() { anyhow::bail!("legacy table wording"); }"#,
                source_path,
            )
            .unwrap(),
            |_| "legacy_graph_misuse",
        );
        let current_physical = review_rows(
            rust_strings::scan(
                r#"fn render() { anyhow::bail!("rewritten row wording"); }"#,
                source_path,
            )
            .unwrap(),
            |_| "physical_storage",
        );
        assert_error_contains(
            compare_surface_classification_sets(SURFACE_RUST_STRING, &base, &current_physical),
            "reclassifies surviving legacy_graph_misuse stable boundary",
        );

        let mut reviewed_reclassification = current_physical.clone();
        for row in &mut reviewed_reclassification {
            row.classification = "quoted_compatibility".to_owned();
            row.compatibility_treatment = REVIEWED_SEMANTIC_RECLASSIFICATION.to_owned();
            row.rollout_phase = "presentation".to_owned();
        }
        compare_surface_classification_sets(SURFACE_RUST_STRING, &base, &reviewed_reclassification)
            .expect("an explicit reviewed semantic rewrite may retain a compatibility noun");
        assert_error_contains(
            compare_surface_classification_sets(
                SURFACE_RUST_STRING,
                &[],
                &reviewed_reclassification,
            ),
            "unused reviewed_semantic_reclassification marker",
        );

        let current_legacy = review_rows(
            rust_strings::scan(
                r#"fn render() { anyhow::bail!("rewritten table wording"); }"#,
                source_path,
            )
            .unwrap(),
            |_| "legacy_graph_misuse",
        );
        compare_surface_classification_sets(SURFACE_RUST_STRING, &base, &current_legacy)
            .expect("a changed fingerprint remains legacy at the stable boundary");
    }

    #[test]
    fn unchanged_nonlegacy_doc_event_does_not_mask_or_inherit_a_legacy_transition() {
        let source_path = "docs/user/example.md";
        let base = review_rows(
            markdown::scan(
                "# Vocabulary\n\n- legacy table wording\n- physical row wording\n",
                source_path,
            ),
            |row| {
                if row.current_term.eq_ignore_ascii_case("table") {
                    "legacy_graph_misuse"
                } else {
                    "physical_storage"
                }
            },
        );

        let removed = review_rows(
            markdown::scan("# Vocabulary\n\n- physical row wording\n", source_path),
            |_| "physical_storage",
        );
        compare_surface_classification_sets(SURFACE_USER_DOCS, &base, &removed)
            .expect("removing a legacy event must not charge its unchanged nonlegacy neighbour");

        let mut false_marker = removed.clone();
        false_marker[0].compatibility_treatment = REVIEWED_SEMANTIC_RECLASSIFICATION.to_owned();
        false_marker[0].rollout_phase = "presentation".to_owned();
        assert_error_contains(
            compare_surface_classification_sets(SURFACE_USER_DOCS, &base, &false_marker),
            "unused reviewed_semantic_reclassification marker",
        );

        let rewritten = review_rows(
            markdown::scan(
                "# Vocabulary\n\n- rewritten column wording\n- physical row wording\n",
                source_path,
            ),
            |_| "physical_storage",
        );
        assert_error_contains(
            compare_surface_classification_sets(SURFACE_USER_DOCS, &base, &rewritten),
            "reclassifies surviving legacy_graph_misuse stable boundary",
        );

        let mut misplaced_marker = rewritten.clone();
        let unchanged_row = misplaced_marker
            .iter_mut()
            .find(|row| row.current_term.eq_ignore_ascii_case("row"))
            .expect("unchanged event must contain its original vocabulary term");
        unchanged_row.compatibility_treatment = REVIEWED_SEMANTIC_RECLASSIFICATION.to_owned();
        unchanged_row.rollout_phase = "presentation".to_owned();
        assert_error_contains(
            compare_surface_classification_sets(SURFACE_USER_DOCS, &base, &misplaced_marker),
            "reclassifies surviving legacy_graph_misuse stable boundary",
        );

        let mut reviewed_rewrite = rewritten.clone();
        let rewritten_row = reviewed_rewrite
            .iter_mut()
            .find(|row| row.current_term.eq_ignore_ascii_case("column"))
            .expect("rewritten event must contain the replacement vocabulary term");
        rewritten_row.compatibility_treatment = REVIEWED_SEMANTIC_RECLASSIFICATION.to_owned();
        rewritten_row.rollout_phase = "presentation".to_owned();
        compare_surface_classification_sets(SURFACE_USER_DOCS, &base, &reviewed_rewrite)
            .expect("only the rewritten legacy event needs an explicit transition marker");
        compare_surface_classification_sets(
            SURFACE_USER_DOCS,
            &reviewed_rewrite,
            &reviewed_rewrite,
        )
        .expect("the accepted marker may persist unchanged for the next generation");

        let retired_marker = review_rows(
            markdown::scan(
                "# Vocabulary\n\n- revised column wording\n- physical row wording\n",
                source_path,
            ),
            |_| "physical_storage",
        );
        compare_surface_classification_sets(SURFACE_USER_DOCS, &reviewed_rewrite, &retired_marker)
            .expect("an accepted marker may retire when its observation changes later");

        let mut rewritten_marker = retired_marker;
        let rewritten_row = rewritten_marker
            .iter_mut()
            .find(|row| row.current_term.eq_ignore_ascii_case("column"))
            .expect("revised event must contain the replacement vocabulary term");
        rewritten_row.compatibility_treatment = REVIEWED_SEMANTIC_RECLASSIFICATION.to_owned();
        rewritten_row.rollout_phase = "presentation".to_owned();
        assert_error_contains(
            compare_surface_classification_sets(
                SURFACE_USER_DOCS,
                &reviewed_rewrite,
                &rewritten_marker,
            ),
            "unused reviewed_semantic_reclassification marker",
        );
    }

    #[test]
    fn reviewed_transition_allows_openapi_compatibility_cleanup_at_same_pointer() {
        let base = review_rows(
            scan_openapi(
                &minimal_spec(&[("change-type", "legacy graph table wording")]),
                "openapi.json",
            )
            .unwrap(),
            |_| "legacy_graph_misuse",
        );
        let mut current = review_rows(
            scan_openapi(
                &minimal_spec(&[("change-type", "the quoted table-key compatibility label")]),
                "openapi.json",
            )
            .unwrap(),
            |_| "quoted_compatibility",
        );
        for row in &mut current {
            row.compatibility_treatment = REVIEWED_SEMANTIC_RECLASSIFICATION.to_owned();
            row.rollout_phase = "presentation".to_owned();
        }

        compare_surface_classification_sets(SURFACE_OPENAPI, &base, &current)
            .expect("review-visible compatibility prose may replace a legacy misuse");
        compare_surface_classification_sets(SURFACE_OPENAPI, &current, &current)
            .expect("an exact accepted transition marker persists across later checks");

        let mut rewritten_again = review_rows(
            scan_openapi(
                &minimal_spec(&[(
                    "change-type",
                    "a revised quoted table-key compatibility label",
                )]),
                "openapi.json",
            )
            .unwrap(),
            |_| "quoted_compatibility",
        );
        for row in &mut rewritten_again {
            row.compatibility_treatment = REVIEWED_SEMANTIC_RECLASSIFICATION.to_owned();
            row.rollout_phase = "presentation".to_owned();
        }
        assert_error_contains(
            compare_surface_classification_sets(SURFACE_OPENAPI, &current, &rewritten_again),
            "unused reviewed_semantic_reclassification marker",
        );
    }

    #[test]
    fn merge_base_check_requires_compatibility_alias_set_equality() {
        let base_spec = minimal_spec(&[("base", "compatibility table")]);
        let repo = TestRepo::with_reviewed_base(&base_spec, |_| "compatibility_alias");
        repo.write_current(
            &minimal_spec(&[("base", "ordinary compatibility object")]),
            |_| unreachable!(),
        );
        assert_error_contains(
            check_repo(&repo),
            "compatibility_alias occurrence set changed",
        );
    }

    #[test]
    fn merge_base_check_rejects_missing_and_stale_base_assets() {
        let mut missing = TestRepo::new();
        let spec = minimal_spec(&[("base", "physical table")]);
        fs::write(missing.root().join("openapi.json"), &spec).unwrap();
        missing.base = missing.commit_all("base without inventory");
        missing.write_current(&spec, |_| "physical_storage");
        assert_error_contains(check_repo(&missing), "required merge-base asset");

        let mut stale = TestRepo::new();
        fs::write(stale.root().join("openapi.json"), &spec).unwrap();
        let wrong_rows =
            scan_openapi(&minimal_spec(&[("base", "physical row")]), "openapi.json").unwrap();
        stale.write_inventory(&review_rows(wrong_rows, |_| "physical_storage"));
        stale.base = stale.commit_all("stale classified base");
        stale.write_current(&spec, |_| "physical_storage");
        assert_error_contains(check_repo(&stale), "merge base:");
    }

    #[test]
    fn merge_base_check_requires_a_commit_object() {
        let spec = minimal_spec(&[("base", "physical table")]);
        let mut repo = TestRepo::with_reviewed_base(&spec, |_| "physical_storage");
        repo.base = git_output(repo.root(), &["rev-parse", "HEAD^{tree}"]);
        assert_error_contains(check_repo(&repo), "must identify a commit object");
    }

    #[test]
    fn base_argument_must_be_a_commit_sha() {
        assert_error_contains(validate_base_sha("origin/main"), "hexadecimal commit SHA");
        validate_base_sha("da466ba").unwrap();
        validate_base_sha("da466ba75bcb9f09a78f75da8e75912bd1d5c1b7").unwrap();
    }

    #[test]
    fn bare_tool_names_stay_on_path_and_explicit_relative_paths_anchor_to_root() {
        let root = Path::new("/repo");
        assert_eq!(
            resolve_tool_executable(root, Path::new("cargo-public-api")),
            PathBuf::from("cargo-public-api")
        );
        assert_eq!(
            resolve_tool_executable(root, Path::new("target/tools/cargo-public-api")),
            root.join("target/tools/cargo-public-api")
        );
        assert_eq!(
            resolve_tool_executable(root, Path::new("./cargo-public-api")),
            root.join("./cargo-public-api")
        );
    }

    #[test]
    fn presentation_only_openapi_check_allows_only_description_and_summary_edits() {
        let base = json!({
            "openapi": "3.1.0",
            "info": {"title": "OmniGraph", "summary": "table service", "description": "row service"},
            "paths": {
                "/items": {"get": {
                    "summary": "list table rows",
                    "description": "returns table rows",
                    "responses": {"200": {
                        "description": "table rows",
                        "headers": {"x-table-key": {"description": "table key", "schema": {"type": "string"}}},
                        "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Item"}}}
                    }}
                }}
            },
            "components": {"schemas": {"Item": {
                "type": "object",
                "description": "table row",
                "properties": {"description": {"type": "string", "description": "row description"}},
                "required": ["description"]
            }}}
        });
        let current = json!({
            "openapi": "3.1.0",
            "info": {"title": "OmniGraph", "summary": "entity service", "description": "record service"},
            "paths": {
                "/items": {"get": {
                    "summary": "list entity records",
                    "description": "returns entity records",
                    "responses": {"200": {
                        "description": "entity records",
                        "headers": {"x-table-key": {"description": "declaration key", "schema": {"type": "string"}}},
                        "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Item"}}}
                    }}
                }}
            },
            "components": {"schemas": {"Item": {
                "type": "object",
                "description": "entity record",
                "properties": {"description": {"type": "string", "description": "record description"}},
                "required": ["description"]
            }}}
        });

        ensure_openapi_structure_unchanged(&base.to_string(), &current.to_string())
            .expect("presentation text may change");

        let mut property_rename = current.clone();
        let properties = property_rename
            .pointer_mut("/components/schemas/Item/properties")
            .and_then(Value::as_object_mut)
            .unwrap();
        let schema = properties.remove("description").unwrap();
        properties.insert("details".to_owned(), schema);
        assert_error_contains(
            ensure_openapi_structure_unchanged(&base.to_string(), &property_rename.to_string()),
            "changed structurally",
        );

        let mut default_change = current.clone();
        default_change
            .pointer_mut("/components/schemas/Item/properties/description")
            .and_then(Value::as_object_mut)
            .unwrap()
            .insert("default".to_owned(), Value::String("row".to_owned()));
        assert_error_contains(
            ensure_openapi_structure_unchanged(&base.to_string(), &default_change.to_string()),
            "changed structurally",
        );
    }

    #[test]
    fn presentation_normalizer_preserves_literal_description_keys() {
        let base = json!({
            "openapi": "3.1.0",
            "paths": {"/items": {"get": {"responses": {"200": {
                "description": "ok",
                "content": {"application/json": {"example": {"description": "table"}}}
            }}}}}
        });
        let current = json!({
            "openapi": "3.1.0",
            "paths": {"/items": {"get": {"responses": {"200": {
                "description": "still ok",
                "content": {"application/json": {"example": {"description": "entity"}}}
            }}}}}
        });
        assert_error_contains(
            ensure_openapi_structure_unchanged(&base.to_string(), &current.to_string()),
            "changed structurally",
        );
    }

    fn minimal_spec(sites: &[(&str, &str)]) -> String {
        let mut paths = Map::new();
        for (name, description) in sites {
            paths.insert(
                format!("/{name}"),
                json!({
                    "get": {
                        "description": description,
                        "responses": {"200": {"description": "ok"}}
                    }
                }),
            );
        }
        json!({"openapi": "3.1.0", "paths": Value::Object(paths)}).to_string()
    }

    fn review_rows<F>(mut rows: Vec<InventoryRow>, classify: F) -> Vec<InventoryRow>
    where
        F: Fn(&InventoryRow) -> &'static str,
    {
        for row in &mut rows {
            row.target_term = "retain".to_owned();
            row.classification = classify(row).to_owned();
            row.compatibility_treatment = "retain_exact_name".to_owned();
            row.rollout_phase = "guard".to_owned();
            row.test_owner = "omnigraph-vocabulary-guard".to_owned();
            row.rationale = "fixture classification".to_owned();
        }
        rows.sort_by(|left, right| left.occurrence_id.cmp(&right.occurrence_id));
        rows
    }

    fn raw_inventory(rows: &[InventoryRow]) -> String {
        let mut output = String::from(INVENTORY_HEADER);
        output.push('\n');
        for row in rows {
            output.push_str(&row.fields().join("\t"));
            output.push('\n');
        }
        output
    }

    fn assert_error_contains<T>(result: Result<T, GuardError>, expected: &str) {
        let error = result.err().expect("operation must fail");
        assert!(error.to_string().contains(expected), "{error}");
    }

    struct TestRepo {
        directory: TempDir,
        base: String,
    }

    impl TestRepo {
        fn new() -> Self {
            let directory = tempfile::tempdir().expect("temp repository");
            git(directory.path(), &["init", "-q"]);
            git(
                directory.path(),
                &["config", "user.name", "Vocabulary Guard"],
            );
            git(
                directory.path(),
                &["config", "user.email", "guard@example.invalid"],
            );
            Self {
                directory,
                base: String::new(),
            }
        }

        fn with_reviewed_base<F>(spec: &str, classify: F) -> Self
        where
            F: Fn(&InventoryRow) -> &'static str,
        {
            let mut repo = Self::new();
            repo.write_current(spec, classify);
            repo.base = repo.commit_all("reviewed base");
            repo
        }

        fn root(&self) -> &Path {
            self.directory.path()
        }

        fn write_current<F>(&self, spec: &str, classify: F)
        where
            F: Fn(&InventoryRow) -> &'static str,
        {
            fs::write(self.root().join("openapi.json"), spec).expect("write OpenAPI");
            let rows = scan_openapi(spec, "openapi.json").expect("scan OpenAPI");
            self.write_inventory(&review_rows(rows, classify));
        }

        fn write_inventory(&self, rows: &[InventoryRow]) {
            let path = self.root().join(INVENTORY_PATH);
            fs::create_dir_all(path.parent().expect("inventory parent"))
                .expect("create inventory parent");
            fs::write(path, render_tsv(rows).expect("render inventory")).expect("write inventory");
        }

        fn commit_all(&self, message: &str) -> String {
            git(self.root(), &["add", "."]);
            git(self.root(), &["commit", "-q", "-m", message]);
            git_output(self.root(), &["rev-parse", "HEAD"])
        }

        fn options(&self) -> CheckOptions {
            CheckOptions {
                base: self.base.clone(),
                inventory: self.root().join(INVENTORY_PATH),
                openapi: self.root().join("openapi.json"),
                surface: GuardSurface::Openapi,
                public_api_tool: PathBuf::from("cargo-public-api"),
                public_api_target_dir: PathBuf::from("target/vocabulary-public-api"),
                presentation_only: false,
            }
        }
    }

    fn check_repo(repo: &TestRepo) -> Result<CheckReport, GuardError> {
        check_at_root(repo.root(), &repo.options())
    }

    fn git(root: &Path, args: &[&str]) {
        let output = Command::new("git")
            .arg("-C")
            .arg(root)
            .args(args)
            .output()
            .expect("run git");
        assert!(
            output.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn git_output(root: &Path, args: &[&str]) -> String {
        let output = Command::new("git")
            .arg("-C")
            .arg(root)
            .args(args)
            .output()
            .expect("run git");
        assert!(
            output.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    }
}
