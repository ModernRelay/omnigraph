//! The results graph's rows, from a store's records and the catalogs that
//! declared them: graph JSONL for `omnigraph load --mode merge`, so the graph
//! lives wherever `omnigraph` can address and the harness owns no location.
//! A scenario and a suite are their names, a point's spec is its levels and
//! every other id is content-derived, so a re-export of the same store is a
//! no-op under merge, and a run's rows are the projection's rows.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::Path;

use serde::Serialize;

use crate::archive::{ARCHIVE_FORMAT_VERSION, ArchiveReceiptV1};
use crate::catalog::{Catalog, FixtureInput, Scenario};
use crate::gqt_case::{self, GqtFixture, ScenarioPoints};
use crate::gqt_record::{AnyRunRecordV1, GqtRunRecordV1, GqtSutIdentityV1};
use crate::model::sha256_bytes;
use crate::projection::{
    EdgeEnvelope, NodeEnvelope, PROJECTION_SCHEMA, ProjectionError, any_point_row, any_run_row,
    canonical_json, serialize_graph_row,
};
use crate::store::FixtureStore;

/// The results graph's schema: `omnigraph init --schema` takes it as a file.
pub const RESULTS_SCHEMA: &str = PROJECTION_SCHEMA;

/// The stored queries beside the schema, by name: the `.gq` text
/// `omnigraph query --query <file> <name>` runs.
pub const RESULTS_QUERIES: &[(&str, &str)] = &[
    (
        "series_for_scenario",
        include_str!("../projection/queries/series_for_scenario.gq"),
    ),
    (
        "suite_series",
        include_str!("../projection/queries/suite_series.gq"),
    ),
    (
        "runs_for_commit",
        include_str!("../projection/queries/runs_for_commit.gq"),
    ),
    (
        "scenarios_in_suite",
        include_str!("../projection/queries/scenarios_in_suite.gq"),
    ),
    (
        "all_series",
        include_str!("../projection/queries/all_series.gq"),
    ),
    (
        "suite_members",
        include_str!("../projection/queries/suite_members.gq"),
    ),
    (
        "scenario_facts",
        include_str!("../projection/queries/scenario_facts.gq"),
    ),
    (
        "run_hosts",
        include_str!("../projection/queries/run_hosts.gq"),
    ),
    (
        "point_levels",
        include_str!("../projection/queries/point_levels.gq"),
    ),
    (
        "points_at",
        include_str!("../projection/queries/points_at.gq"),
    ),
];

/// Every stored query as one GQ document, the file
/// `omnigraph query <name> --query <file>` selects a declaration from.
pub fn results_queries_document() -> String {
    RESULTS_QUERIES
        .iter()
        .map(|(_, text)| text.trim_end())
        .collect::<Vec<_>>()
        .join("\n\n")
        + "\n"
}

/// What one export wrote: the rows per type, and what it left out. `records`
/// counts every record object of the store: the runs, the legacy records and
/// the ones no record format of this harness parses.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize)]
pub struct ExportSummary {
    pub catalogs: u64,
    pub suites: u64,
    pub scenarios: u64,
    pub fixtures: u64,
    pub workloads: u64,
    pub records: u64,
    pub legacy_records_skipped: u64,
    pub unparsed_records_skipped: u64,
    pub points: u64,
    pub levels: u64,
    pub runs: u64,
    /// Runs whose record names no product commit: they get no `Built` edge,
    /// so the queries that walk `built` do not return them.
    pub runs_without_commit: u64,
    pub revisions: u64,
    pub machines: u64,
    pub edges: u64,
    pub rows: u64,
}

#[derive(Serialize)]
struct CatalogRow<'a> {
    catalog_id: &'a str,
    path: String,
    digest: &'a str,
    scenario_count: u32,
}

#[derive(Serialize)]
struct SuiteRow<'a> {
    name: &'a str,
    catalog_id: &'a str,
}

/// A scenario by its name. The other properties are the declaration this
/// export read from a catalog; a name only the records hold has none.
#[derive(Default, Serialize)]
struct ScenarioRow<'a> {
    name: &'a str,
    measured_step_ordinal: Option<u32>,
    measured_step: Option<&'a str>,
    repetitions: Option<u32>,
    deadline_seconds: Option<u64>,
    protocol_json: Option<String>,
    fixture_kind: Option<&'a str>,
}

/// One value of one field of a point's spec.
#[derive(Serialize)]
struct LevelRow<'a> {
    factor: &'a str,
    value: &'a str,
}

/// An edge the schema keys by its two ends, so the engine derives its id.
#[derive(Serialize)]
struct KeyedEdge<'a> {
    edge: &'a str,
    from: &'a str,
    to: &'a str,
    data: NoProperties,
}

#[derive(Serialize)]
struct FixtureRow<'a> {
    fixture_id: &'a str,
    kind: &'a str,
    path: Option<String>,
    definition_json: String,
}

#[derive(Serialize)]
struct WorkloadRow<'a> {
    workload_id: &'a str,
    path: String,
    sha256: &'a str,
}

#[derive(Serialize)]
struct RevisionRow<'a> {
    sha: &'a str,
}

#[derive(Serialize)]
struct MachineRow {
    machine_fingerprint: String,
    label: Option<String>,
    os_name: Option<String>,
    os_version: Option<String>,
    kernel_version: Option<String>,
    architecture: Option<String>,
    cpu_model: Option<String>,
    logical_cores: Option<u64>,
    physical_cores: Option<u64>,
    total_memory_bytes: Option<u64>,
}

#[derive(Serialize)]
struct NoProperties {}

struct Rows<'w> {
    out: &'w mut dyn Write,
    nodes: Vec<String>,
    edges: Vec<String>,
    edges_written: BTreeSet<String>,
    summary: ExportSummary,
}

impl Rows<'_> {
    fn node(&mut self, node_type: &str, data: &impl Serialize) -> Result<(), ProjectionError> {
        self.nodes
            .push(serialize_graph_row(&NodeEnvelope { node_type, data })?);
        Ok(())
    }

    /// An edge under an id this export supplies, written once per id.
    fn edge(
        &mut self,
        edge: &str,
        id: String,
        from: &str,
        to: &str,
    ) -> Result<(), ProjectionError> {
        if !self.edges_written.insert(id.clone()) {
            return Ok(());
        }
        self.edges.push(serialize_graph_row(&EdgeEnvelope {
            edge,
            id,
            from,
            to,
            data: NoProperties {},
        })?);
        Ok(())
    }

    /// An edge the schema keys by its two ends, written once per pair.
    fn keyed_edge(&mut self, edge: &str, from: &str, to: &str) -> Result<(), ProjectionError> {
        if !self.edges_written.insert(format!("{edge}\n{from}\n{to}")) {
            return Ok(());
        }
        self.edges.push(serialize_graph_row(&KeyedEdge {
            edge,
            from,
            to,
            data: NoProperties {},
        })?);
        Ok(())
    }

    /// The one place a row is written, so an export that fails writes none:
    /// every node, then every edge, so one `load` of the output finds each
    /// endpoint in the same batch.
    fn finish(mut self) -> Result<ExportSummary, ProjectionError> {
        self.summary.edges = self.edges.len() as u64;
        for line in self.nodes.iter().chain(&self.edges) {
            self.out
                .write_all(line.as_bytes())
                .and_then(|()| self.out.write_all(b"\n"))
                .map_err(|error| {
                    ProjectionError::new("export_write_failed", None, error.to_string())
                })?;
            self.summary.rows += 1;
        }
        Ok(self.summary)
    }
}

/// Write the graph's rows: the catalogs' declarations, each node and edge
/// once, then every GQT record of the store as a run on its point, revisions
/// and machine, then the edges. A scenario is a name: a catalog's entry
/// declares it, and a case name only the records hold is a scenario with no
/// declaration, so its runs are in the series all the same. A point's spec is
/// written as levels, one per field, and each run names its own scenario. A
/// record no format of this harness parses is counted and left out. Nothing
/// is written unless the whole export succeeds: a record whose bytes do not
/// hash to its name, two records of one invocation, or one scenario name
/// declared differently by two of the catalogs, stops it with no row out.
pub async fn export(
    store: &FixtureStore,
    catalogs: &[Catalog],
    out: &mut dyn Write,
) -> Result<ExportSummary, ProjectionError> {
    let mut rows = Rows {
        out,
        nodes: Vec::new(),
        edges: Vec::new(),
        edges_written: BTreeSet::new(),
        summary: ExportSummary::default(),
    };
    let mut catalogs_written: BTreeSet<String> = BTreeSet::new();
    let mut suites_written: BTreeSet<String> = BTreeSet::new();
    let mut declarations: BTreeMap<String, String> = BTreeMap::new();
    let mut undeclared: BTreeSet<String> = BTreeSet::new();
    let mut fixtures_written: BTreeSet<String> = BTreeSet::new();
    let mut workloads_written: BTreeSet<String> = BTreeSet::new();
    for catalog in catalogs {
        let digest = sha256_bytes(canonical_json(&catalog.definition, "catalog")?.as_bytes());
        let catalog_id = digest.as_str();
        let scenario_count = u32::try_from(catalog.definition.scenarios.len()).map_err(|_| {
            ProjectionError::new(
                "export_count_overflow",
                Some(&catalog.path),
                "scenario count does not fit u32",
            )
        })?;
        if catalogs_written.insert(digest.clone()) {
            rows.node(
                "Catalog",
                &CatalogRow {
                    catalog_id,
                    path: catalog_path(catalog),
                    digest: catalog_id,
                    scenario_count,
                },
            )?;
            rows.summary.catalogs += 1;
        }
        let mut names_in_catalog: BTreeSet<&str> = BTreeSet::new();
        for scenario in &catalog.definition.scenarios {
            let name = scenario.id.as_str();
            let workload_path = catalog.root.join(&scenario.workload);
            let workload_sha256 = file_sha256(&workload_path, "export_workload_unreadable")?;
            if workloads_written.insert(workload_sha256.clone()) {
                rows.node(
                    "Workload",
                    &WorkloadRow {
                        workload_id: &workload_sha256,
                        path: scenario.workload.display().to_string(),
                        sha256: &workload_sha256,
                    },
                )?;
                rows.summary.workloads += 1;
            }
            let (fixture_id, fixture_kind, fixture_path) = fixture_identity(catalog, scenario)?;
            if fixtures_written.insert(fixture_id.clone()) {
                rows.node(
                    "Fixture",
                    &FixtureRow {
                        fixture_id: &fixture_id,
                        kind: fixture_kind,
                        path: fixture_path,
                        definition_json: canonical_json(&scenario.fixture.definition(), "fixture")?,
                    },
                )?;
                rows.summary.fixtures += 1;
            }
            let row = ScenarioRow {
                name,
                measured_step_ordinal: scenario
                    .measured_step
                    .as_ref()
                    .map(|step| u32::try_from(step.ordinal).unwrap_or(u32::MAX)),
                measured_step: scenario
                    .measured_step
                    .as_ref()
                    .map(|step| step.text.as_str()),
                repetitions: Some(effective_repetitions(catalog, scenario, &workload_path)?),
                deadline_seconds: scenario.deadline_seconds(&catalog.definition.defaults),
                protocol_json: Some(canonical_json(&scenario.protocol, "protocol")?),
                fixture_kind: Some(fixture_kind),
            };
            let declaration = canonical_json(&(&row, &fixture_id, &workload_sha256), "scenario")?;
            match declarations.get(name) {
                None => {
                    rows.node("Scenario", &row)?;
                    rows.summary.scenarios += 1;
                    rows.edge("Uses", format!("uses:{name}"), name, &fixture_id)?;
                    rows.edge(
                        "Executes",
                        format!("executes:{name}"),
                        name,
                        &workload_sha256,
                    )?;
                    declarations.insert(name.to_owned(), declaration);
                }
                Some(first) if *first != declaration => {
                    return Err(ProjectionError::new(
                        "export_scenario_conflict",
                        Some(&catalog.path),
                        format!(
                            "scenario `{name}` is declared differently by an earlier catalog of this export"
                        ),
                    ));
                }
                Some(_) => {}
            }
            rows.keyed_edge("Declares", catalog_id, name)?;
            names_in_catalog.insert(name);
        }
        for (suite, members) in &catalog.definition.suites {
            if suites_written.insert(suite.clone()) {
                rows.node(
                    "Suite",
                    &SuiteRow {
                        name: suite,
                        catalog_id,
                    },
                )?;
                rows.summary.suites += 1;
            }
            for member in members {
                if names_in_catalog.contains(member.as_str()) {
                    rows.keyed_edge("Groups", suite, member)?;
                }
            }
        }
    }

    let mut points_written: BTreeSet<String> = BTreeSet::new();
    let mut levels_written: BTreeSet<String> = BTreeSet::new();
    let mut revisions_written: BTreeSet<String> = BTreeSet::new();
    let mut machines_written: BTreeSet<String> = BTreeSet::new();
    let mut record_of_invocation: BTreeMap<String, String> = BTreeMap::new();
    let uris = store
        .list_records()
        .await
        .map_err(|error| ProjectionError::new("export_store_unreadable", None, error.message))?;
    for uri in uris {
        let bytes = store.read_record(&uri).await.map_err(|error| {
            ProjectionError::new("export_record_unreadable", None, error.message)
        })?;
        rows.summary.records += 1;
        let Ok(record) = crate::gqt_record::parse(&bytes) else {
            rows.summary.unparsed_records_skipped += 1;
            continue;
        };
        let gqt = match &record {
            AnyRunRecordV1::Gqt(record) => &**record,
            AnyRunRecordV1::Concurrent(block) => &block.record,
            AnyRunRecordV1::Legacy(_) => {
                rows.summary.legacy_records_skipped += 1;
                continue;
            }
        };
        let record_sha256 = record_name(&uri)?;
        let invocation_id = record.invocation().invocation_id.clone();
        if let Some(first) =
            record_of_invocation.insert(invocation_id.clone(), record_sha256.clone())
        {
            return Err(ProjectionError::new(
                "export_invocation_conflict",
                None,
                format!(
                    "invocation `{invocation_id}` has two records in the store, records/{first}.json and records/{record_sha256}.json"
                ),
            ));
        }
        let receipt = ArchiveReceiptV1 {
            archive_format_version: ARCHIVE_FORMAT_VERSION,
            invocation_id: invocation_id.clone(),
            record_sha256: record_sha256.clone(),
            object_relative_path: format!("records/{record_sha256}.json"),
            pointer_relative_path: format!("records/{record_sha256}.json"),
            newly_published: false,
        };
        let point = any_point_row(&record)?;
        let point_id = record.point_id().to_string();
        if points_written.insert(point_id.clone()) {
            rows.node("BenchmarkPoint", &point)?;
            rows.summary.points += 1;
            for (factor, value) in spec_levels(&gqt.run.run_spec)? {
                let level_id = canonical_json(&[&factor, &value], "level")?;
                if levels_written.insert(level_id.clone()) {
                    rows.node(
                        "Level",
                        &LevelRow {
                            factor: &factor,
                            value: &value,
                        },
                    )?;
                    rows.summary.levels += 1;
                }
                rows.keyed_edge("Has", &point_id, &level_id)?;
            }
        }
        let case = gqt.run.case_id.as_str();
        if !declarations.contains_key(case) && undeclared.insert(case.to_owned()) {
            rows.node(
                "Scenario",
                &ScenarioRow {
                    name: case,
                    ..ScenarioRow::default()
                },
            )?;
            rows.summary.scenarios += 1;
        }
        rows.keyed_edge("Defines", case, &point_id)?;
        rows.keyed_edge("Ran", &invocation_id, case)?;
        let run = any_run_row(&record, &receipt)?;
        let run_value = serde_json::to_value(&run)
            .map_err(|error| ProjectionError::new("export_row_invalid", None, error.to_string()))?;
        rows.node("BenchmarkRun", &run)?;
        rows.summary.runs += 1;
        rows.edge(
            "Measures",
            format!("measures:{invocation_id}"),
            &invocation_id,
            &point_id,
        )?;
        if let Some(sha) = run_value["source_commit"]
            .as_str()
            .filter(|sha| !sha.is_empty())
        {
            if revisions_written.insert(sha.to_string()) {
                rows.node("Revision", &RevisionRow { sha })?;
                rows.summary.revisions += 1;
            }
            rows.edge(
                "Built",
                format!("built:{invocation_id}"),
                &invocation_id,
                sha,
            )?;
        } else {
            rows.summary.runs_without_commit += 1;
        }
        if let Some(sha) = harness_commit(gqt) {
            if revisions_written.insert(sha.to_string()) {
                rows.node("Revision", &RevisionRow { sha })?;
                rows.summary.revisions += 1;
            }
            rows.edge(
                "MeasuredBy",
                format!("measured_by:{invocation_id}"),
                &invocation_id,
                sha,
            )?;
        }
        if let Some(fingerprint) = run_value["machine_fingerprint"].as_str() {
            if machines_written.insert(fingerprint.to_string()) {
                rows.node("Machine", &machine_row(fingerprint, &run_value))?;
                rows.summary.machines += 1;
            }
            rows.edge(
                "Hosted",
                format!("hosted:{invocation_id}"),
                fingerprint,
                &invocation_id,
            )?;
        }
    }
    rows.finish()
}

/// The commit of the harness that measured: the embedded build itself, a
/// served run's client build, or a product run's harness.
fn harness_commit(record: &GqtRunRecordV1) -> Option<&str> {
    match &record.sut {
        GqtSutIdentityV1::Embedded(sut) => Some(sut.source_commit.as_str()),
        GqtSutIdentityV1::Served(sut) => Some(sut.client_build.source_commit.as_str()),
        GqtSutIdentityV1::Product(sut) => Some(sut.harness.source_commit.as_str()),
    }
    .filter(|sha| !sha.is_empty())
}

fn machine_row(fingerprint: &str, run: &serde_json::Value) -> MachineRow {
    let text = |field: &str| run[field].as_str().map(str::to_owned);
    MachineRow {
        machine_fingerprint: fingerprint.to_owned(),
        label: text("machine_label"),
        os_name: text("machine_os_name"),
        os_version: text("machine_os_version"),
        kernel_version: text("machine_kernel_version"),
        architecture: text("machine_architecture"),
        cpu_model: text("machine_cpu_model"),
        logical_cores: run["machine_logical_cores"].as_u64(),
        physical_cores: run["machine_physical_cores"].as_u64(),
        total_memory_bytes: run["machine_total_memory_bytes"].as_u64(),
    }
}

/// `<root's last component>/<file>`: the two catalogs a machine runs are
/// `benchmarks/benchmarks.yaml` and `private/benchmarks.yaml`.
fn catalog_path(catalog: &Catalog) -> String {
    let directory = catalog
        .root
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    let file = catalog
        .path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    format!("{directory}/{file}")
}

fn file_sha256(path: &Path, code: &'static str) -> Result<String, ProjectionError> {
    let bytes = std::fs::read(path).map_err(|error| ProjectionError::io(code, path, error))?;
    Ok(sha256_bytes(&bytes))
}

/// One level per leaf of a point's spec: the path to the leaf is the factor,
/// a string is its own value and any other leaf its JSON text. A field the
/// harness adds to the spec later is a new factor with no change here.
fn spec_levels(spec: &impl Serialize) -> Result<Vec<(String, String)>, ProjectionError> {
    fn walk(path: &str, value: &serde_json::Value, levels: &mut Vec<(String, String)>) {
        match value {
            serde_json::Value::Object(fields) => {
                for (name, inner) in fields {
                    let inner_path = if path.is_empty() {
                        name.clone()
                    } else {
                        format!("{path}.{name}")
                    };
                    walk(&inner_path, inner, levels);
                }
            }
            serde_json::Value::String(text) => levels.push((path.to_owned(), text.clone())),
            other => levels.push((path.to_owned(), other.to_string())),
        }
    }
    let value = serde_json::to_value(spec)
        .map_err(|error| ProjectionError::new("export_row_invalid", None, error.to_string()))?;
    let mut levels = Vec::new();
    walk("", &value, &mut levels);
    Ok(levels)
}

/// The repetitions the scenario runs when the command line names none, as
/// `Catalog::repetitions` resolves them: the entry's, else its workload's
/// `--- bench` section's, else the defaults'.
fn effective_repetitions(
    catalog: &Catalog,
    scenario: &Scenario,
    workload_path: &Path,
) -> Result<u32, ProjectionError> {
    let invalid = |message: String| {
        ProjectionError::new("export_workload_invalid", Some(workload_path), message)
    };
    let workload = gqt_case::read_source(workload_path)
        .map_err(invalid)?
        .parse()
        .map_err(invalid)?;
    let points = ScenarioPoints {
        plans: Vec::new(),
        repetitions: workload.bench.as_ref().and_then(|bench| bench.repetitions),
    };
    Ok(catalog.repetitions(scenario, &points))
}

/// A fixture's identity: a file's bytes, or a declaration's canonical text.
fn fixture_identity(
    catalog: &Catalog,
    scenario: &Scenario,
) -> Result<(String, &'static str, Option<String>), ProjectionError> {
    match &scenario.fixture {
        FixtureInput::Path(path) => Ok((
            file_sha256(&catalog.root.join(path), "export_fixture_unreadable")?,
            "file",
            Some(path.display().to_string()),
        )),
        FixtureInput::Explicit(fixture) => {
            let kind = match fixture {
                GqtFixture::Dataset { .. } => "dataset",
                GqtFixture::Registered { .. } => "registered",
            };
            Ok((
                sha256_bytes(canonical_json(fixture, "fixture")?.as_bytes()),
                kind,
                None,
            ))
        }
    }
}

/// The store names a record by its bytes' digest, `records/<sha256>.json`.
fn record_name(uri: &str) -> Result<String, ProjectionError> {
    uri.rsplit('/')
        .next()
        .and_then(|name| name.strip_suffix(".json"))
        .filter(|digest| crate::model::digest(digest))
        .map(str::to_owned)
        .ok_or_else(|| {
            ProjectionError::new(
                "export_record_invalid",
                None,
                format!("`{uri}` is not a record object"),
            )
        })
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use omnigraph::Session;
    use omnigraph::db::{Omnigraph, ReadTarget};
    use omnigraph::loader::LoadMode;
    use omnigraph::settings::SessionSettings;
    use omnigraph_compiler::ParamMap;
    use omnigraph_compiler::query::ast::Literal;

    use super::*;

    fn public_catalog() -> Catalog {
        Catalog::load(
            &Path::new(env!("CARGO_MANIFEST_DIR")).join("../../benchmarks/benchmarks.yaml"),
        )
        .unwrap()
    }

    fn query_text(name: &str) -> &'static str {
        RESULTS_QUERIES
            .iter()
            .find(|(candidate, _)| *candidate == name)
            .map(|(_, text)| *text)
            .unwrap()
    }

    async fn rows_of(
        session: &Session,
        query: &str,
        param: (&str, &str),
    ) -> Vec<serde_json::Value> {
        let mut params = ParamMap::new();
        params.insert(param.0.to_string(), Literal::String(param.1.to_string()));
        let (result, _) = session
            .query_with_head(
                ReadTarget::branch("main"),
                query_text(query),
                query,
                &params,
            )
            .await
            .unwrap();
        match result.to_rust_json().unwrap() {
            serde_json::Value::Array(rows) => rows,
            other => panic!("{other}"),
        }
    }

    #[tokio::test]
    async fn export_of_a_store_record_loads_merges_and_answers_the_series() {
        let record = crate::gqt_tests::authority_fixture().await;
        let directory = tempfile::tempdir().unwrap();
        let store_path = directory.path().join("store");
        std::fs::create_dir(&store_path).unwrap();
        let store = FixtureStore::open(store_path.to_str().unwrap()).unwrap();
        let put = store.put_record(&record).await.unwrap();
        let catalog = public_catalog();

        let mut jsonl = Vec::new();
        let summary = export(&store, std::slice::from_ref(&catalog), &mut jsonl)
            .await
            .unwrap();
        let record_object = format!("records/{}.json", put.record_sha256);
        let text = String::from_utf8(jsonl.clone()).unwrap();
        assert!(
            text.contains(&format!("\"archive_object\":\"{record_object}\""))
                && text.contains(&format!("\"archive_pointer\":\"{record_object}\"")),
            "a store record is its own pointer: {text}"
        );
        assert_eq!((summary.records, summary.runs, summary.points), (1, 1, 1));
        assert_eq!(summary.catalogs, 1);
        assert_eq!(
            summary.scenarios as usize,
            catalog.definition.scenarios.len(),
            "{summary:?}"
        );
        assert!(
            summary.revisions >= 1 && summary.machines == 1,
            "{summary:?}"
        );
        let text = String::from_utf8(jsonl).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        let first_edge = lines.iter().position(|line| line.starts_with("{\"edge\""));
        let last_node = lines.iter().rposition(|line| line.starts_with("{\"type\""));
        assert!(last_node < first_edge, "edges come after every node");

        let graph = directory.path().join("results.omni");
        let session = Session::from_defaults(
            Arc::new(
                Omnigraph::init(graph.to_str().unwrap(), RESULTS_SCHEMA)
                    .await
                    .unwrap(),
            ),
            SessionSettings::default(),
        );
        session
            .load_graph_batch("main", &text, LoadMode::Merge)
            .await
            .unwrap();
        let series = rows_of(
            &session,
            "series_for_scenario",
            ("scenario", &record.run.case_id),
        )
        .await;
        assert_eq!(series.len(), 1, "{series:?}");
        assert_eq!(
            series[0]["p50_us"].as_u64(),
            Some(record.measurements.wall_clock.p50_us)
        );
        let GqtSutIdentityV1::Embedded(sut) = &record.sut else {
            panic!("the authority fixture is an embedded run")
        };
        assert_eq!(
            series[0]["commit"].as_str(),
            Some(sut.source_commit.as_str())
        );
        let runs = rows_of(&session, "runs_for_commit", ("sha", &sut.source_commit)).await;
        assert_eq!(runs.len(), 1, "{runs:?}");
        assert_eq!(
            runs[0]["invocation_id"].as_str(),
            Some(record.invocation.invocation_id.as_str())
        );

        session
            .load_graph_batch("main", &text, LoadMode::Merge)
            .await
            .unwrap();
        let again = rows_of(
            &session,
            "series_for_scenario",
            ("scenario", &record.run.case_id),
        )
        .await;
        assert_eq!(
            again, series,
            "the same rows loaded again merge by id, so nothing doubles"
        );
    }

    /// A store's record objects are files, which a `.gqt` case cannot write.
    #[tokio::test]
    async fn export_skips_a_record_it_cannot_parse_and_stops_at_one_that_does_not_hash_to_its_name()
    {
        let record = crate::gqt_tests::authority_fixture().await;
        let directory = tempfile::tempdir().unwrap();
        let store_path = directory.path().join("store");
        std::fs::create_dir(&store_path).unwrap();
        let store = FixtureStore::open(store_path.to_str().unwrap()).unwrap();
        store.put_record(&record).await.unwrap();
        let other_format = br#"{"format_version":1,"run":{}}"#;
        let records = store_path.join("records");
        std::fs::write(
            records.join(format!("{}.json", sha256_bytes(other_format))),
            other_format,
        )
        .unwrap();

        let summary = export(&store, &[], &mut Vec::<u8>::new()).await.unwrap();
        assert_eq!(
            (
                summary.records,
                summary.runs,
                summary.unparsed_records_skipped,
                summary.legacy_records_skipped,
                summary.runs_without_commit,
            ),
            (2, 1, 1, 0, 0),
            "{summary:?}"
        );

        std::fs::write(
            records.join(format!("{}.json", "0".repeat(64))),
            other_format,
        )
        .unwrap();
        let mut written = Vec::new();
        let error = export(&store, &[], &mut written).await.unwrap_err();
        assert_eq!(error.code, "export_record_unreadable", "{error:?}");
        assert!(written.is_empty(), "an export that fails writes no row");
    }

    /// A store's record objects are files, which a `.gqt` case cannot write.
    #[tokio::test]
    async fn export_stops_at_two_records_of_one_invocation_and_names_both() {
        let record = crate::gqt_tests::authority_fixture().await;
        let directory = tempfile::tempdir().unwrap();
        let store_path = directory.path().join("store");
        std::fs::create_dir(&store_path).unwrap();
        let store = FixtureStore::open(store_path.to_str().unwrap()).unwrap();
        let first = store.put_record(&record).await.unwrap();
        let mut renamed = record.clone();
        renamed.run.case_id = format!("{}-again", record.run.case_id);
        let second = store.put_record(&renamed).await.unwrap();

        let mut written = Vec::new();
        let error = export(&store, &[], &mut written).await.unwrap_err();
        assert_eq!(error.code, "export_invocation_conflict", "{error:?}");
        for object in [first.record_sha256, second.record_sha256] {
            assert!(
                error.message.contains(&format!("records/{object}.json")),
                "{error:?}"
            );
        }
        assert!(written.is_empty(), "an export that fails writes no row");
    }

    async fn graph_of(directory: &Path, rows: &str) -> Session {
        let graph = directory.join("results.omni");
        let session = Session::from_defaults(
            Arc::new(
                Omnigraph::init(graph.to_str().unwrap(), RESULTS_SCHEMA)
                    .await
                    .unwrap(),
            ),
            SessionSettings::default(),
        );
        session
            .load_graph_batch("main", rows, LoadMode::Merge)
            .await
            .unwrap();
        session
    }

    /// The queries read the export's own rows, and a `.gqt` case cannot run the export.
    #[tokio::test]
    async fn export_with_no_catalog_still_answers_the_series_and_the_levels_of_its_point() {
        let record = crate::gqt_tests::authority_fixture().await;
        let directory = tempfile::tempdir().unwrap();
        let store_path = directory.path().join("store");
        std::fs::create_dir(&store_path).unwrap();
        let store = FixtureStore::open(store_path.to_str().unwrap()).unwrap();
        store.put_record(&record).await.unwrap();

        let mut jsonl = Vec::new();
        let summary = export(&store, &[], &mut jsonl).await.unwrap();
        assert_eq!(
            (
                summary.catalogs,
                summary.suites,
                summary.scenarios,
                summary.points
            ),
            (0, 0, 1, 1),
            "{summary:?}"
        );
        let session = graph_of(directory.path(), &String::from_utf8(jsonl).unwrap()).await;
        let series = rows_of(
            &session,
            "series_for_scenario",
            ("scenario", &record.run.case_id),
        )
        .await;
        assert_eq!(
            series.len(),
            1,
            "a case name no catalog declares is still a scenario: {series:?}"
        );

        let levels = rows_of(&session, "point_levels", ("point", &record.run.point_id)).await;
        assert_eq!(levels.len() as u64, summary.levels, "{levels:?}");
        let spec = serde_json::to_value(&record.run.run_spec).unwrap();
        let queries = spec["queries_sha256"].as_str().unwrap();
        assert!(
            levels
                .iter()
                .any(|row| row["factor"] == "queries_sha256" && row["value"] == queries),
            "{levels:?}"
        );
    }

    /// The export reads catalog files, which a `.gqt` case cannot name.
    #[tokio::test]
    async fn export_writes_each_declaration_once_with_the_repetitions_the_harness_runs() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::create_dir(directory.path().join("records")).unwrap();
        let store = FixtureStore::open(directory.path().to_str().unwrap()).unwrap();
        let mut once = Vec::new();
        let summary = export(&store, &[public_catalog()], &mut once)
            .await
            .unwrap();
        let mut twice = Vec::new();
        let doubled = export(&store, &[public_catalog(), public_catalog()], &mut twice)
            .await
            .unwrap();
        assert_eq!(doubled, summary, "a catalog given twice adds no row");
        assert_eq!(twice, once);

        let text = String::from_utf8(once).unwrap();
        let repetitions = |name: &str| {
            text.lines()
                .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                .find(|row| row["type"] == "Scenario" && row["data"]["name"] == name)
                .map(|row| row["data"]["repetitions"].clone())
                .unwrap()
        };
        assert_eq!(
            repetitions("branch-merge-d50-process-cold"),
            5,
            "the entry's own"
        );
        assert_eq!(
            repetitions("equal-current-narrow-one-batch"),
            1,
            "the catalog's default"
        );

        let mut other = public_catalog();
        other.definition.scenarios[0].repetitions = Some(7);
        let mut written = Vec::new();
        let error = export(&store, &[public_catalog(), other], &mut written)
            .await
            .unwrap_err();
        assert_eq!(error.code, "export_scenario_conflict", "{error:?}");
        assert!(written.is_empty(), "an export that fails writes no row");
    }

    async fn all_rows(session: &Session, query: &str) -> Vec<serde_json::Value> {
        let (result, _) = session
            .query_with_head(
                ReadTarget::branch("main"),
                query_text(query),
                query,
                &ParamMap::new(),
            )
            .await
            .unwrap();
        match result.to_rust_json().unwrap() {
            serde_json::Value::Array(rows) => rows,
            other => panic!("{other}"),
        }
    }

    /// Two exports with a catalog edit between them, which a `.gqt` case cannot produce.
    #[tokio::test]
    async fn a_second_export_after_a_catalog_edit_merges_to_one_scenario_and_each_run_once() {
        let record = crate::gqt_tests::authority_fixture().await;
        let directory = tempfile::tempdir().unwrap();
        let store_path = directory.path().join("store");
        std::fs::create_dir(&store_path).unwrap();
        let store = FixtureStore::open(store_path.to_str().unwrap()).unwrap();
        store.put_record(&record).await.unwrap();
        let case = record.run.case_id.as_str();

        let mut first = Vec::new();
        export(&store, &[public_catalog()], &mut first)
            .await
            .unwrap();
        let mut edited = public_catalog();
        edited
            .definition
            .scenarios
            .iter_mut()
            .find(|scenario| scenario.id == case)
            .unwrap()
            .repetitions = Some(7);
        let mut second = Vec::new();
        export(&store, &[edited], &mut second).await.unwrap();

        let session = graph_of(directory.path(), &String::from_utf8(first).unwrap()).await;
        session
            .load_graph_batch("main", &String::from_utf8(second).unwrap(), LoadMode::Merge)
            .await
            .unwrap();
        let series = rows_of(&session, "series_for_scenario", ("scenario", case)).await;
        assert_eq!(series.len(), 1, "one run, once: {series:?}");
        let everything = all_rows(&session, "all_series").await;
        assert_eq!(everything.len(), 1, "{everything:?}");
        let facts: Vec<_> = all_rows(&session, "scenario_facts")
            .await
            .into_iter()
            .filter(|row| row["scenario"] == case)
            .collect();
        assert_eq!(facts.len(), 1, "one scenario under its name: {facts:?}");
        assert_eq!(facts[0]["repetitions"], 7, "the edited declaration");
    }
}
