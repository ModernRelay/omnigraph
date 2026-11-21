//! The run loop of a `reset: rebuild` point, the parent's side of a server
//! the bench starts per repetition: the point's base acquired for its
//! identity and row counts, then per repetition a cluster applied by the
//! product, the server started over it, a worker driving it, and the server
//! stopped for its peak memory. `gqt_server_process` holds the server's
//! process, and the worker's side is `execute_owned_server_rep_signaled` in
//! `gqt_runner`.

use crate::case::Backend;
use crate::gqt_case::PlannedGqt;
use crate::gqt_evidence::{OwnedServerInput, RepetitionInputV2};
use crate::gqt_runner::{
    RunExecution, RunOptions, binding, block_on_own_thread, child_environment, failure,
    resolve_product, summarize_wall_clock, validate_sample,
};
use crate::runner::{RunnerError, RunnerResult};
use crate::store::{FixtureStore, StoreLocation};
use omnigraph_gqt_core::ServerTarget;
use std::path::Path;
use std::time::Duration;

/// The actor that applies the cluster, provisions the graph and runs the
/// workload's own steps against a server the run starts.
const OWNED_SERVER_STEPS_ACTOR: &str = "act-bench-steps";

/// A `--store --server-product` run: the point's base acquired for its
/// identity and row counts, then per repetition a cluster applied, the server
/// started, a worker driving it, and the server stopped for its peak memory.
pub(crate) fn execute_owned_server(
    run: crate::suite::ResolvedRun,
    plan: PlannedGqt,
    options: &RunOptions,
    store_text: &str,
) -> RunnerResult<RunExecution> {
    use crate::gqt_evidence::OWNED_SERVER_GRAPH;
    use crate::gqt_server_process::{
        ServerDeadlines, ServerProcess, apply_cluster, write_cluster_config,
    };

    let scratch = options
        .scratch_root
        .as_ref()
        .or(options.dataset_cache.as_ref())
        .ok_or_else(|| failure("a rebuild-reset run requires a scratch directory"))?;
    std::fs::create_dir_all(scratch).map_err(failure)?;
    let scratch = scratch.canonicalize().map_err(failure)?;
    let workspace = tempfile::Builder::new()
        .prefix("gqt-server-")
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
    let store = FixtureStore::open(store_text)?;
    let (
        Backend::LocalFs {
            filesystem,
            storage_class,
        },
        StoreLocation::Local { path: store_path },
    ) = (&plan.definition.environment.backend, store.location())
    else {
        return Err(failure(
            "a server the run starts serves a cluster root under a local store",
        ));
    };
    let environment =
        crate::environment::verify_local_environment(store_path, *filesystem, *storage_class)
            .map_err(failure)?;
    let inherited = child_environment();
    let with_home = |home: &Path| -> RunnerResult<Vec<(String, String)>> {
        std::fs::create_dir_all(home).map_err(failure)?;
        let home = home
            .to_str()
            .ok_or_else(|| failure("non-UTF8 scratch path"))?;
        let mut environment = inherited.clone();
        environment.push(("OMNIGRAPH_HOME".to_string(), home.to_string()));
        Ok(environment)
    };
    let product = resolve_product(
        options,
        &with_home(&workspace.path().join("operator-home"))?,
    )?;
    let server = product
        .server
        .clone()
        .ok_or_else(|| failure("reset: rebuild requires --server-product"))?;
    let binding = binding(&plan.dataset, &options.fixture_bindings)?;
    let base = block_on_own_thread(store.acquire_base(
        &plan.base_build_plan().map_err(failure)?,
        workspace.path(),
        binding,
        options.no_build,
    ))??;
    let bound = plan
        .bind(
            &base.manifest.summary.logical_content_sha256,
            &base.manifest.summary.algorithm,
        )
        .map_err(failure)?;
    let crate::gqt_case::DatasetRecipe::Gqt { source } = &plan.dataset else {
        return Err(failure(
            "a server started per repetition is provisioned from an authored dataset",
        ));
    };
    let dataset = source.parse().map_err(failure)?;
    let fixture = dataset
        .fixture
        .as_ref()
        .ok_or_else(|| failure("dataset requires schema and seed"))?;
    let sessions = plan
        .queries
        .parse()
        .map_err(failure)?
        .measured_block()
        .map_or(0, |block| block.sessions.len());
    let actors: Vec<String> = std::iter::once(OWNED_SERVER_STEPS_ACTOR.to_string())
        .chain((0..sessions).map(|session| format!("act-bench-s{session:02}")))
        .collect();
    let config = workspace.path().join("cluster-config");
    std::fs::create_dir(&config).map_err(failure)?;
    write_cluster_config(&config, OWNED_SERVER_GRAPH, &fixture.schema, &actors)?;
    let roots = store_path.join("servers");
    std::fs::create_dir_all(&roots).map_err(failure)?;
    let roots = roots.canonicalize().map_err(failure)?;
    type Counts = Vec<crate::fixture_reference::GraphTableCountV1>;
    let seeded = |of: fn(&crate::dataset_identity::BranchLogicalV1) -> &Counts| {
        let branches = base.manifest.summary.branches.iter();
        branches
            .flat_map(|branch| {
                of(branch)
                    .iter()
                    .map(|table| omnigraph_gqt_core::SeededTable {
                        branch: branch.name.clone(),
                        table: table.name.clone(),
                        rows: table.rows,
                    })
            })
            .collect::<Vec<_>>()
    };
    let tables = seeded(|branch| &branch.node_tables);
    let edge_tables = seeded(|branch| &branch.edge_tables);
    let run_id = ulid::Ulid::new().to_string();
    let deadlines = ServerDeadlines::default();
    let mut samples = Vec::new();
    let mut machine = None;
    let mut build = None;
    let mut left = None;
    let acquisition = (|| -> RunnerResult<()> {
        for repetition in 1..=run.repetitions {
            let root = roots.join(format!("{run_id}-{repetition:08}"));
            let tokens: Vec<(String, String)> = actors
                .iter()
                .map(|actor| {
                    let token = format!("{}{}", ulid::Ulid::new(), ulid::Ulid::new());
                    (actor.clone(), token)
                })
                .collect();
            let failed = |mut error: RunnerError| {
                for (_, token) in &tokens {
                    error.message = crate::gqt_served::redact_error(error.message, Some(token));
                }
                error.with_repetition(repetition)
            };
            let unwritable = |error: std::io::Error| failed(failure(error));
            std::fs::create_dir(&root).map_err(unwritable)?;
            left = Some(root.clone());
            let logs = workspace.path().join(format!("server-{repetition:08}"));
            let environment = with_home(&logs.join("home")).map_err(failed)?;
            apply_cluster(
                &product.path,
                &config,
                &root,
                OWNED_SERVER_STEPS_ACTOR,
                &environment,
                deadlines.apply,
            )
            .map_err(failed)?;
            let process = ServerProcess::start(
                &server.path,
                product.measured_commit(),
                &root,
                &tokens,
                &environment,
                &logs,
                deadlines.start,
            )
            .map_err(failed)?;
            let server_logs = process.logs().clone();
            let worker_scratch_root = workspace
                .path()
                .join(format!("worker-scratch-{repetition:08}"));
            std::fs::create_dir(&worker_scratch_root).map_err(unwritable)?;
            let execution = RepetitionInputV2::OwnedServer {
                input: Box::new(OwnedServerInput {
                    target: ServerTarget {
                        url: crate::gqt_served::canonical_endpoint(process.url())
                            .map_err(|message| failed(failure(message)))?,
                        graph: OWNED_SERVER_GRAPH.to_string(),
                        token: Some(tokens[0].1.clone()),
                    },
                    session_tokens: tokens[1..].iter().map(|(_, token)| token.clone()).collect(),
                    server_sha256: server.executable_sha256.clone(),
                    tables: tables.clone(),
                    edge_tables: edge_tables.clone(),
                }),
            };
            let proof = execution
                .proof()
                .map_err(|message| failed(failure(message)))?;
            let observed = crate::gqt_supervisor::supervise_repetition(
                crate::gqt_supervisor::SupervisionInput {
                    worker_executable: worker.executable.clone(),
                    expected_worker_executable_sha256: worker.executable_sha256.clone(),
                    expected_machine: machine.clone(),
                    repetition,
                    case: bound.clone(),
                    execution,
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
            let stopped = process.stop(deadlines.stop);
            let (observed, stopped) = match (observed, stopped) {
                (Ok(observed), Ok(stopped)) => (observed, stopped),
                (Ok(_), Err(stop)) => return Err(failed(stop)),
                (Err(mut error), stopped) => {
                    if let Err(stop) = stopped {
                        error.message.push_str(&format!("; {}", stop.message));
                    }
                    error
                        .message
                        .push_str(&format!("; server {}", server_logs.tails()));
                    if crate::dataset_cache::contained(&error)
                        && std::fs::remove_dir_all(&root).is_ok()
                    {
                        left = None;
                    }
                    return Err(failed(error));
                }
            };
            let mut sample = observed.sample;
            sample.peak_rss_bytes = Some(stopped.peak_rss_bytes);
            validate_sample(&sample, &bound, repetition, &proof, sample.elapsed_us, true)
                .map_err(failed)?;
            machine = Some(observed.machine);
            build = Some(observed.worker_build);
            samples.push(sample);
            std::fs::remove_dir_all(&worker_scratch_root).map_err(unwritable)?;
            std::fs::remove_dir_all(&logs).map_err(unwritable)?;
            std::fs::remove_dir_all(&root).map_err(unwritable)?;
            left = None;
        }
        Ok(())
    })();
    let acquisition = acquisition.map_err(|mut error| {
        if let Some(root) = &left {
            if let Err(marker) =
                crate::dataset_cache::quarantine_directory(workspace.path(), &error.message)
            {
                error
                    .message
                    .push_str(&format!("; quarantine marker failed: {}", marker.message));
            }
            error.message.push_str(&format!(
                "; cluster root {} and {} are left for inspection",
                root.display(),
                workspace.path().display()
            ));
        }
        error
    });
    if left.is_some() {
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
        machine: machine.ok_or_else(|| failure("no worker identity"))?,
        environment: Some(environment),
        fixture: None,
        dataset_cache_hit: Some(base.cache_hit),
        server_receipt: None,
        base: Some(base.manifest),
        product: Some(product),
        observed_backend: None,
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
