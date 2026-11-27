//! `check`: a scenario's fixture without its data, then its workload, on a
//! temporary graph. The proof that a catalog entry parses, compiles, plans
//! and executes before any fixture is built or any machine is launched.
use std::path::Path;
use std::time::Duration;

use omnigraph::db::Omnigraph;
use omnigraph_compiler::settings::Engine;
use omnigraph_gqt_core::{Case, PlainHost, branch_case, case_session, execute_steps};

use crate::gqt_case::{DatasetRecipe, PlannedGqt};

/// What `check` found for one point of a scenario.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PointCheck {
    /// Every step of the fixture, minus its loads, and of the workload ran;
    /// `steps` is the workload's step count.
    Ok { steps: usize },
    /// A registered fixture: its graph lives in a bundle, so there is no
    /// schema to build an empty graph from.
    Skipped { reason: String },
    /// A parse, build or step failure; `detail` names the step and the error;
    /// `steps` is the workload's step count, 0 when the workload did not
    /// parse, so a fixture failure still says what would have run.
    Failed { steps: usize, detail: String },
}

/// `plan`'s fixture without its seed rows and loads (the schema, the declared
/// branches with their recipes, its steps minus every `load csv`) in a
/// temporary directory, then its workload on it, loads included, the data
/// expectations of both relaxed; each case within its own `timeout_ms`.
/// Nothing is cached, recorded or kept. A step whose success needs the seed
/// (an edge between seeded nodes, an `error:` the data causes, a `bind` on
/// a write that matches nothing) fails here and is the run's to prove.
pub async fn check_point(plan: &PlannedGqt) -> PointCheck {
    let (mut fixture, fixture_stem) = match &plan.dataset {
        DatasetRecipe::Gqt { source } => match source.parse() {
            Ok(case) if case.fixture.is_some() => (case, source.stem.as_str()),
            Ok(_) => {
                return PointCheck::Failed {
                    steps: 0,
                    detail: "fixture: no `--- schema` section".into(),
                };
            }
            Err(e) => {
                return PointCheck::Failed {
                    steps: 0,
                    detail: format!("fixture: {e}"),
                };
            }
        },
        DatasetRecipe::Registered { .. } => {
            return PointCheck::Skipped {
                reason: "a registered fixture carries its graph in a bundle, so there is no schema to build an empty graph from".into(),
            };
        }
    };
    let mut workload = match plan.queries.parse() {
        Ok(case) => case,
        Err(e) => {
            return PointCheck::Failed {
                steps: 0,
                detail: format!("workload: {e}"),
            };
        }
    };
    let steps = workload.steps().len();
    let run = Box::pin(run_without_data(
        &mut fixture,
        fixture_stem,
        &mut workload,
        &plan.queries.stem,
    ));
    match run.await {
        Ok(()) => PointCheck::Ok { steps },
        Err(detail) => PointCheck::Failed { steps, detail },
    }
}

async fn run_without_data(
    fixture: &mut Case,
    fixture_stem: &str,
    workload: &mut Case,
    workload_stem: &str,
) -> Result<(), String> {
    let directory = tempfile::tempdir().map_err(|e| format!("temporary directory: {e}"))?;
    let sources = directory.path().join("sources");
    std::fs::create_dir(&sources).map_err(|e| format!("temporary directory: {e}"))?;
    let graph = directory.path().join("graph");
    let uri = graph.to_str().ok_or("temporary path is not UTF-8")?;
    fixture.drop_loads();
    fixture.relax_data_expectations();
    workload.materialize_csv_fixtures(&sources)?;
    workload.relax_data_expectations();
    let needs_indices = fixture.needs_indices || workload.needs_indices;
    let declared = fixture
        .fixture
        .as_ref()
        .ok_or("fixture: no `--- schema` section")?;
    let fixture_timeout = Duration::from_millis(fixture.runner.timeout_ms);
    let build = async {
        let db = Omnigraph::init(uri, &declared.schema)
            .await
            .map_err(|e| format!("fixture: schema: {e}"))?;
        let session = case_session(db, fixture, Engine::V2).map_err(|e| format!("fixture: {e}"))?;
        branch_case(&session, declared, needs_indices)
            .await
            .map_err(|e| format!("fixture: {e}"))?;
        execute_steps(
            fixture,
            Path::new(fixture_stem),
            false,
            session,
            uri,
            None,
            &PlainHost,
            &(),
        )
        .await
        .map_err(|e| format!("fixture {e}"))
    };
    let session = tokio::time::timeout(fixture_timeout, Box::pin(build))
        .await
        .map_err(|_| {
            format!(
                "fixture: the case's timeout_ms {} elapsed",
                fixture_timeout.as_millis()
            )
        })??;
    drop(session);
    let db = Omnigraph::open(uri)
        .await
        .map_err(|e| format!("reopen: {e}"))?;
    let db = match &workload.external_blob_policy {
        Some(policy) => db
            .with_external_blob_policy(policy.clone())
            .map_err(|e| format!("csv fixture policy: {e}"))?,
        None => db,
    };
    let session = case_session(db, workload, Engine::V2)?;
    let timeout = Duration::from_millis(workload.runner.timeout_ms);
    let run = execute_steps(
        workload,
        Path::new(workload_stem),
        false,
        session,
        uri,
        None,
        &PlainHost,
        &(),
    );
    match tokio::time::timeout(timeout, run).await {
        Ok(result) => result.map(drop),
        Err(_) => Err(format!(
            "the case's timeout_ms {} elapsed",
            timeout.as_millis()
        )),
    }
}
