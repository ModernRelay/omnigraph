use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::SUITE_FORMAT_VERSION;
use crate::case::{ValidatedCase, load_case};
use crate::model::{
    Diagnostic, ValidationOutcome, declared_version, read_yaml_file, strict_yaml, valid_kebab_id,
};

const MAX_SUITE_RUNS: usize = 10_000;
const MAX_REPETITIONS_PER_CASE: u32 = 10_000;
const MAX_TOTAL_REPETITIONS: u64 = 100_000;

/// A V1 suite groups immutable case definitions and owns sample quantity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SuiteV1 {
    pub version: u32,
    pub name: String,
    pub runs: Vec<SuiteRunV1>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SuiteRunV1 {
    pub case: PathBuf,
    pub repetitions: u32,
}

/// One resolved suite entry. Repetitions remain visibly outside the case.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ResolvedRun {
    pub case_path: PathBuf,
    pub repetitions: u32,
    pub case: ValidatedCase,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ResolvedSuite {
    pub definition: SuiteV1,
    pub suite_path: PathBuf,
    pub runs: Vec<ResolvedRun>,
}

/// Parse and structurally validate suite-v1 YAML without reading its cases.
pub fn parse_suite(source: &str) -> ValidationOutcome<SuiteV1> {
    let version = match declared_version(source, "suite") {
        Ok(version) => version,
        Err(diagnostic) => return ValidationOutcome::failure(vec![diagnostic]),
    };
    if version != SUITE_FORMAT_VERSION {
        return ValidationOutcome::failure(vec![Diagnostic::error(
            "unsupported_suite_version",
            "version",
            format!(
                "unsupported suite version {version}; this build supports version {SUITE_FORMAT_VERSION}"
            ),
        )]);
    }
    let suite: SuiteV1 = match strict_yaml(source, "suite") {
        Ok(suite) => suite,
        Err(diagnostic) => return ValidationOutcome::failure(vec![diagnostic]),
    };
    validate_suite(suite)
}

/// Validate suite-owned facts. Cross-case uniqueness is checked by
/// [`load_suite`] after relative references resolve.
pub fn validate_suite(suite: SuiteV1) -> ValidationOutcome<SuiteV1> {
    let mut diagnostics = Vec::new();
    if suite.version != SUITE_FORMAT_VERSION {
        diagnostics.push(Diagnostic::error(
            "unsupported_suite_version",
            "version",
            format!(
                "unsupported suite version {}; this build supports version {SUITE_FORMAT_VERSION}",
                suite.version
            ),
        ));
    }
    if !valid_kebab_id(&suite.name) || suite.name.len() > 128 {
        diagnostics.push(Diagnostic::error(
            "invalid_suite_name",
            "name",
            "suite name must be 1..=128 characters of kebab-case ASCII ([a-z0-9]+(?:-[a-z0-9]+)*)",
        ));
    }
    if suite.runs.is_empty() {
        diagnostics.push(Diagnostic::error(
            "empty_suite",
            "runs",
            "suite must reference at least one case",
        ));
    }
    if suite.runs.len() > MAX_SUITE_RUNS {
        diagnostics.push(Diagnostic::error(
            "suite_run_budget_exceeded",
            "runs",
            format!("suite may contain at most {MAX_SUITE_RUNS} runs"),
        ));
    }
    let mut declared_paths = BTreeSet::new();
    let mut total_repetitions = 0_u64;
    for (index, run) in suite.runs.iter().enumerate() {
        let path = format!("runs[{index}]");
        if run.case.as_os_str().is_empty() {
            diagnostics.push(Diagnostic::error(
                "empty_case_path",
                format!("{path}.case"),
                "case path must not be empty",
            ));
        } else if run.case.is_absolute() {
            diagnostics.push(Diagnostic::error(
                "absolute_case_path",
                format!("{path}.case"),
                "case path must be relative to the suite file",
            ));
        }
        if !declared_paths.insert(run.case.clone()) {
            diagnostics.push(Diagnostic::error(
                "duplicate_case_path",
                format!("{path}.case"),
                format!(
                    "case path '{}' is listed more than once",
                    run.case.display()
                ),
            ));
        }
        if !(1..=MAX_REPETITIONS_PER_CASE).contains(&run.repetitions) {
            diagnostics.push(Diagnostic::error(
                "invalid_repetitions",
                format!("{path}.repetitions"),
                format!("repetitions must be in 1..={MAX_REPETITIONS_PER_CASE}"),
            ));
        }
        total_repetitions += u64::from(run.repetitions);
    }
    if total_repetitions > MAX_TOTAL_REPETITIONS {
        diagnostics.push(Diagnostic::error(
            "suite_repetition_budget_exceeded",
            "runs",
            format!(
                "suite requests {total_repetitions} repetitions; the per-plan limit is {MAX_TOTAL_REPETITIONS}"
            ),
        ));
    }
    if diagnostics.is_empty() {
        ValidationOutcome::success(suite)
    } else {
        ValidationOutcome::failure(diagnostics)
    }
}

/// Load a suite and all referenced case files relative to the suite location.
///
/// Canonical paths, human case ids, and full point ids must each be unique.
pub fn load_suite(path: &Path) -> ValidationOutcome<ResolvedSuite> {
    let source = match read_yaml_file(path, "suite") {
        Ok(source) => source,
        Err(diagnostic) => return ValidationOutcome::failure(vec![diagnostic]),
    };
    let definition = match parse_suite(&source).into_result() {
        Ok(suite) => suite,
        Err(diagnostics) => return ValidationOutcome::failure(diagnostics),
    };
    let suite_path = match fs::canonicalize(path) {
        Ok(path) => path,
        Err(error) => {
            return ValidationOutcome::failure(vec![Diagnostic::error(
                "suite_path_error",
                path.display().to_string(),
                format!("could not resolve suite path: {error}"),
            )]);
        }
    };
    let suite_dir = suite_path
        .parent()
        .expect("a canonical file path has a parent");
    let mut diagnostics = Vec::new();
    let mut runs = Vec::with_capacity(definition.runs.len());
    let mut source_indices = Vec::with_capacity(definition.runs.len());
    let mut canonical_paths: BTreeMap<PathBuf, usize> = BTreeMap::new();

    for (index, run) in definition.runs.iter().enumerate() {
        let joined = suite_dir.join(&run.case);
        let case_path = match fs::canonicalize(&joined) {
            Ok(path) => path,
            Err(error) => {
                diagnostics.push(Diagnostic::error(
                    "case_path_error",
                    format!("runs[{index}].case"),
                    format!("could not resolve '{}': {error}", joined.display()),
                ));
                continue;
            }
        };
        if let Some(first) = canonical_paths.insert(case_path.clone(), index) {
            diagnostics.push(Diagnostic::error(
                "duplicate_case_path",
                format!("runs[{index}].case"),
                format!(
                    "case resolves to the same file as runs[{first}]: {}",
                    case_path.display()
                ),
            ));
            continue;
        }
        match load_case(&case_path).into_result() {
            Ok(case) => {
                source_indices.push(index);
                runs.push(ResolvedRun {
                    case_path,
                    repetitions: run.repetitions,
                    case,
                });
            }
            Err(case_diagnostics) => {
                diagnostics.extend(case_diagnostics.into_iter().map(|mut diagnostic| {
                    diagnostic.path = format!("runs[{index}].case.{}", diagnostic.path);
                    diagnostic
                }));
            }
        }
    }

    reject_duplicate_identity(&runs, &source_indices, &mut diagnostics);
    if !diagnostics.is_empty() {
        return ValidationOutcome::failure(diagnostics);
    }
    ValidationOutcome::success(ResolvedSuite {
        definition,
        suite_path,
        runs,
    })
}

fn reject_duplicate_identity(
    runs: &[ResolvedRun],
    source_indices: &[usize],
    diagnostics: &mut Vec<Diagnostic>,
) {
    debug_assert_eq!(runs.len(), source_indices.len());
    let mut ids: BTreeMap<&str, usize> = BTreeMap::new();
    let mut points: BTreeMap<&str, usize> = BTreeMap::new();
    for (loaded_index, run) in runs.iter().enumerate() {
        let source_index = source_indices[loaded_index];
        if let Some(first) = ids.insert(&run.case.definition.id, source_index) {
            diagnostics.push(Diagnostic::error(
                "duplicate_case_id",
                format!("runs[{source_index}].case"),
                format!(
                    "case id '{}' is already used by runs[{first}]",
                    run.case.definition.id
                ),
            ));
        }
        if let Some(first) = points.insert(&run.case.point_id, source_index) {
            diagnostics.push(Diagnostic::error(
                "duplicate_point_id",
                format!("runs[{source_index}].case"),
                format!(
                    "point id '{}' is already used by runs[{first}]",
                    run.case.point_id
                ),
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CASE: &str = r#"
version: 1
id: base-case
scenario: branch-merge-v1
fixture:
  builder: { kind: synthetic-branch-merge, version: 1, seed: 0 }
  data: { provenance: synthetic, tables: 8, rows_per_table: 1000, payload_bytes: 64, column_shape: scalars, topology_skew: uniform }
  state: { aging: bulk-loaded, indexes: [], deletion_history: none, compaction_recency: optimized, history_depth: 1 }
workload: { delta_rows_per_side: 50, diverged_tables: 4, arrival: unscheduled-single-shot, clients: 1, read_write_mix: write-heavy, contention: distinct-key }
environment:
  backend: { kind: local-fs, filesystem: apfs, storage_class: nvme-ssd }
  network_position: same-host
  execution: embedded
  warmth: { regime: warm, program: branch-merge-read-set-v1, iterations: 1 }
protocol: { deadline_seconds: 60, attribution: per-phase, schedule: manual, reset: plain-copy, timer: monotonic }
"#;

    fn suite(case: &str, repetitions: u32) -> String {
        format!(
            "version: 1\nname: local-smoke\nruns:\n  - case: {case}\n    repetitions: {repetitions}\n"
        )
    }

    #[test]
    fn resolves_relative_cases_and_keeps_quantity_outside_identity() {
        let dir = tempfile::tempdir().unwrap();
        let cases = dir.path().join("cases");
        let suites = dir.path().join("suites");
        fs::create_dir_all(&cases).unwrap();
        fs::create_dir_all(&suites).unwrap();
        fs::write(cases.join("base.yaml"), CASE).unwrap();
        let suite_path = suites.join("smoke.yaml");
        fs::write(&suite_path, suite("../cases/base.yaml", 5)).unwrap();
        let five = load_suite(&suite_path).into_result().unwrap();
        fs::write(&suite_path, suite("../cases/base.yaml", 20)).unwrap();
        let twenty = load_suite(&suite_path).into_result().unwrap();
        assert_eq!(five.runs[0].case.point_id, twenty.runs[0].case.point_id);
        assert_eq!(five.runs[0].repetitions, 5);
        assert_eq!(twenty.runs[0].repetitions, 20);
    }

    #[test]
    fn rejects_duplicate_paths_after_resolution() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("case.yaml"), CASE).unwrap();
        let suite_path = dir.path().join("suite.yaml");
        fs::write(
            &suite_path,
            "version: 1\nname: duplicates\nruns:\n  - { case: case.yaml, repetitions: 5 }\n  - { case: ./case.yaml, repetitions: 5 }\n",
        )
        .unwrap();
        let outcome = load_suite(&suite_path);
        assert!(
            outcome
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "duplicate_case_path")
        );
    }

    #[test]
    fn rejects_duplicate_case_and_point_ids_independently() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("a.yaml"), CASE).unwrap();
        fs::write(
            dir.path().join("same-id.yaml"),
            CASE.replace("delta_rows_per_side: 50", "delta_rows_per_side: 51"),
        )
        .unwrap();
        fs::write(
            dir.path().join("same-point.yaml"),
            CASE.replace("id: base-case", "id: another-case"),
        )
        .unwrap();
        let suite_path = dir.path().join("suite.yaml");
        fs::write(
            &suite_path,
            "version: 1\nname: duplicates\nruns:\n  - { case: a.yaml, repetitions: 5 }\n  - { case: same-id.yaml, repetitions: 5 }\n  - { case: same-point.yaml, repetitions: 5 }\n",
        )
        .unwrap();
        let codes: Vec<_> = load_suite(&suite_path)
            .diagnostics
            .into_iter()
            .map(|diagnostic| diagnostic.code)
            .collect();
        assert!(codes.contains(&"duplicate_case_id".to_string()));
        assert!(codes.contains(&"duplicate_point_id".to_string()));
    }

    #[test]
    fn suite_is_strict_versioned_and_owns_positive_repetitions() {
        let unknown = suite("case.yaml", 5).replace("name:", "unknown: true\nname:");
        assert_eq!(
            parse_suite(&unknown).diagnostics[0].code,
            "invalid_suite_yaml"
        );
        let future = suite("case.yaml", 5).replace("version: 1", "version: 2");
        assert_eq!(
            parse_suite(&future).diagnostics[0].code,
            "unsupported_suite_version"
        );
        assert_eq!(
            parse_suite(&suite("case.yaml", 0)).diagnostics[0].code,
            "invalid_repetitions"
        );
        assert_eq!(
            parse_suite(&suite("case.yaml", MAX_REPETITIONS_PER_CASE + 1)).diagnostics[0].code,
            "invalid_repetitions"
        );
    }

    #[test]
    fn absolute_case_paths_are_refused_before_io() {
        let yaml = suite("/tmp/case.yaml", 5);
        assert_eq!(parse_suite(&yaml).diagnostics[0].code, "absolute_case_path");
    }
}
