mod bench_cli;
use std::collections::BTreeMap;
use std::fs;
use std::path::{self, Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use omnigraph_bench::archive::{
    ArchiveError, ArchivePublicationUnknownV1, ArchiveReceiptV1, ArchiveReconciliationV1,
    iter_archive, preflight_archive_publication, publish_record, reconcile_archive_publication,
};
use omnigraph_bench::fixture_reference::load_fixture_reference;
use omnigraph_bench::projection::{
    DEFAULT_PROJECTION_PAGE_SIZE, ProjectionCursorV1, ProjectionError, ProjectionPageV1,
    list_points_page, list_runs_for_point_page, rebuild_projection,
};
use omnigraph_bench::real_graph::{
    RealGraphObservationV1, observe_real_graph, validate_real_graph_reference,
};
use omnigraph_bench::record::{
    AcquisitionTerminalStageV1, AcquisitionTerminalV1, InvocationIdentityV1,
};
use omnigraph_bench::registered_fixture::{
    FixtureCopyPreflightReceiptV1, fingerprint_registered_fixture, preflight_copy_fixture_bindings,
    stage_registered_fixture_binding, verify_registered_fixture,
};
use omnigraph_bench::{
    Diagnostic, PLAN_FORMAT_VERSION, RUNNER_OUTPUT_VERSION, ResolvedRun, ResolvedSuite,
    RunExecution, RunOptions, RunnerError, ValidatedCase, ValidationOutcome, execute_run,
    load_case, load_suite,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use ulid::Ulid;

const MAX_CASE_FILES: usize = 10_000;
const MAX_DIRECTORY_ENTRIES: usize = 100_000;

#[derive(Debug, Parser)]
#[command(
    name = "omnigraph-bench",
    version,
    about = "Run GQT benchmarks from a named scenario or custom YAML",
    disable_help_subcommand = true,
    after_help = "Start here:\n  omnigraph-bench list scenarios\n  omnigraph-bench show tiny-read\n  omnigraph-bench run tiny-read\n  omnigraph-bench run --config benchmarks/custom.example.yaml\n  omnigraph-bench cache status tiny-read\n\nfixtures/ holds starting states; workloads/ holds operations and checks.\nUse help config, help cache, init --help, or help --json for agents."
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// List fixtures, workloads or named scenarios without building or executing.
    List(bench_cli::ListArgs),
    /// Inspect a scenario's effective settings or a workload's operation ordinals.
    Show(bench_cli::ShowArgs),
    /// Inspect cache contents without creating, restoring or changing them.
    Cache {
        #[command(subcommand)]
        command: bench_cli::CacheCommand,
    },
    /// Generate a custom YAML config from GQT inputs and a selected operation.
    Init(bench_cli::InitArgs),
    /// Explain config/cache topics or describe all commands as JSON.
    Help(bench_cli::HelpArgs),
    /// Inspect one-case experiment definitions.
    Case {
        #[command(subcommand)]
        command: CaseCommand,
    },
    /// Verify registered frozen fixture trees before benchmark preparation.
    Fixture {
        #[command(subcommand)]
        command: FixtureCommand,
    },
    /// Inspect suites of benchmark cases.
    Suite {
        #[command(subcommand)]
        command: SuiteCommand,
    },
    /// Inspect immutable benchmark telemetry archives.
    Archive {
        #[command(subcommand)]
        command: ArchiveCommand,
    },
    /// Rebuild and query the disposable OmniGraph telemetry projection.
    Projection {
        #[command(subcommand)]
        command: ProjectionCommand,
    },
    /// Build, validate and reuse authored GQT datasets.
    Dataset {
        #[command(subcommand)]
        command: DatasetCommand,
    },
    /// Run a named scenario/suite, custom config selection, or a legacy case file.
    Run(Box<RunArgs>),
    /// Print a store's run records of a scenario or suite as a series.
    Report {
        /// Scenario/suite name; the catalog's run list when absent.
        scenario: Option<String>,
        #[arg(long, conflicts_with = "catalog")]
        config: Option<PathBuf>,
        /// A catalog, repeatable: a benchmarks.yaml or the directory holding one; the name resolves in the first that declares it.
        #[arg(long, value_name = "DIR|FILE")]
        catalog: Vec<PathBuf>,
        /// The fixture store whose `records/` are read.
        #[arg(long)]
        store: String,
        #[arg(long)]
        json: bool,
    },
    /// Private one-repetition worker endpoint used by the supervising runner.
    #[command(name = "__gqt-worker-v2", hide = true)]
    WorkerV2,
    /// Private bounded fixture-builder endpoint used by the supervising runner.
    #[command(name = "__dataset-worker-v1", hide = true)]
    FixtureWorkerV1 { request: PathBuf, result: PathBuf },
}

/// The arguments of `run`, boxed in [`Command`] so the subcommand enum stays
/// the size of its smallest variants.
#[derive(Debug, clap::Args)]
struct RunArgs {
    /// Scenario/suite name; a legacy case YAML path is also accepted.
    case: Option<PathBuf>,
    /// Explicit config; paths in it resolve beside the YAML file.
    #[arg(long, conflicts_with_all = ["dataset", "queries", "catalog"])]
    config: Option<PathBuf>,
    /// A catalog, repeatable: a benchmarks.yaml or the directory holding one; the name resolves in the first that declares it.
    #[arg(long, value_name = "DIR|FILE", conflicts_with_all = ["dataset", "queries"])]
    catalog: Vec<PathBuf>,
    #[arg(long)]
    dataset: Option<PathBuf>,
    #[arg(long)]
    queries: Option<PathBuf>,
    #[arg(long)]
    repetitions: Option<u32>,
    #[arg(long, default_value = "target/gqt-datasets")]
    dataset_cache: PathBuf,
    #[arg(long)]
    no_build: bool,
    #[arg(long = "fixture")]
    fixtures: Vec<String>,
    #[command(flatten)]
    server_args: ServerArgs,
    #[arg(long)]
    archive: Option<PathBuf>,
    /// Engine-target route to the fixture: `direct` (a restored copy),
    /// `object-store` (that copy, all I/O through the object store) or
    /// `read-through` (the frozen template, nothing restored; no record).
    #[arg(long, default_value = "direct", conflicts_with = "archive")]
    local_route: omnigraph_bench::gqt_evidence::LocalRouteKind,
    /// Fixture store of a clone-reset run: a directory or an `s3://`
    /// prefix holding frozen bases; every repetition runs the product's
    /// `omnigraph` over an empty clone of the scenario's base.
    #[arg(long, conflicts_with = "server")]
    store: Option<String>,
    /// The declared S3 backend of an `s3://` store: a JSON object or the
    /// path of a file holding one (`implementation`, `implementation_version`,
    /// `region`, `storage_class`, `versioning`, `image_digest`).
    #[arg(long, requires = "store")]
    backend: Option<String>,
    /// The product binary a `--store` run measures; `omnigraph` beside
    /// this executable when unset.
    #[arg(long, requires = "store", conflicts_with = "at")]
    product: Option<PathBuf>,
    /// A commit to measure against the same base, repeatable: `<ref>`
    /// takes the commit's `omnigraph` from the store's `builds/` (built
    /// from an archive on a miss and put there); `<ref>=<path>` names it.
    #[arg(long = "at", requires = "store", conflicts_with = "archive")]
    at: Vec<String>,
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Subcommand)]
enum DatasetCommand {
    /// Build a raw dataset GQT, or the exact dataset needed by a case YAML.
    Build {
        input: PathBuf,
        #[arg(long)]
        queries: Option<PathBuf>,
        #[arg(long, value_parser = ["apfs", "xfs"])]
        filesystem: Option<String>,
        #[arg(long)]
        dataset_cache: PathBuf,
        #[arg(long = "fixture")]
        fixtures: Vec<String>,
        #[arg(long)]
        json: bool,
    },
    /// Verify an existing matching cache entry; never build on a miss.
    Validate {
        input: PathBuf,
        #[arg(long)]
        queries: Option<PathBuf>,
        #[arg(long, value_parser = ["apfs", "xfs"])]
        filesystem: Option<String>,
        #[arg(long)]
        dataset_cache: PathBuf,
        #[arg(long = "fixture")]
        fixtures: Vec<String>,
        #[arg(long)]
        json: bool,
    },
}
fn local_backend(
    filesystem: Option<&str>,
) -> (
    omnigraph_bench::case::Backend,
    omnigraph_bench::case::ResetMode,
) {
    use omnigraph_bench::case::{Backend, LocalFilesystem, LocalStorageClass, ResetMode};
    let selected = filesystem.unwrap_or(if cfg!(target_os = "macos") {
        "apfs"
    } else {
        "xfs"
    });
    let (filesystem, reset) = if selected == "apfs" {
        (LocalFilesystem::Apfs, ResetMode::LocalClonefile)
    } else {
        (LocalFilesystem::Xfs, ResetMode::PlainCopy)
    };
    (
        Backend::LocalFs {
            filesystem,
            storage_class: LocalStorageClass::NvmeSsd,
        },
        reset,
    )
}
async fn run_dataset(command: DatasetCommand) -> ExitCode {
    let (input, queries, filesystem, cache, fixtures, no_build, json) = match command {
        DatasetCommand::Build {
            input,
            queries,
            filesystem,
            dataset_cache,
            fixtures,
            json,
        } => (
            input,
            queries,
            filesystem,
            dataset_cache,
            fixtures,
            false,
            json,
        ),
        DatasetCommand::Validate {
            input,
            queries,
            filesystem,
            dataset_cache,
            fixtures,
            json,
        } => (
            input,
            queries,
            filesystem,
            dataset_cache,
            fixtures,
            true,
            json,
        ),
    };
    let plan = if input.extension().is_some_and(|e| e == "gqt") {
        let (backend, reset) = local_backend(filesystem.as_deref());
        match omnigraph_bench::gqt_case::dataset_file(&input, queries.as_deref(), backend, reset) {
            Ok(p) => p,
            Err(e) => {
                return print_cli_failure(Diagnostic::error("invalid_dataset", "dataset", e), json);
            }
        }
    } else {
        if queries.is_some() || filesystem.is_some() {
            return print_cli_failure(
                Diagnostic::error(
                    "invalid_dataset_options",
                    "dataset",
                    "--queries and --filesystem apply only to a raw dataset .gqt; a case defines both",
                ),
                json,
            );
        }
        match load_case(&input).into_result() {
            Ok(c) => match c.gqt().and_then(|p| p.dataset_build_plan()) {
                Ok(p) => p,
                Err(e) => {
                    return print_cli_failure(
                        Diagnostic::error("invalid_case", "dataset", e),
                        json,
                    );
                }
            },
            Err(e) => return print_cli_failures(e, json),
        }
    };
    match omnigraph_bench::gqt_runner::build_dataset(
        &plan,
        &RunOptions {
            dataset_cache: Some(cache),
            no_build,
            fixture_bindings: fixtures,
            worker_executable: std::env::current_exe().ok(),
            ..Default::default()
        },
    )
    .await
    {
        Ok(manifest) => print_json_success(&manifest),
        Err(e) => print_cli_failure(Diagnostic::error(e.code, "dataset", e.message), json),
    }
}

#[derive(Debug, Subcommand)]
enum CaseCommand {
    /// Strictly parse and validate one case-v1 file.
    Validate {
        file: PathBuf,
        /// Emit a machine-readable validation result.
        #[arg(long)]
        json: bool,
    },
    /// List and validate all case-v1 files directly in a directory.
    List {
        directory: PathBuf,
        /// Emit a machine-readable array.
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
enum FixtureCommand {
    /// Inspect logical references for externally built node-and-edge fixtures.
    Reference {
        #[command(subcommand)]
        command: FixtureReferenceCommand,
    },
    /// Print a location-free JSON manifest for one stable local tree.
    Fingerprint {
        /// Path-free identity assigned to this exact physical snapshot.
        #[arg(long)]
        id: String,
        /// Local graph-root directory to read completely.
        #[arg(long)]
        root: PathBuf,
    },
    /// Verify a local frozen tree against a location-free copy-source descriptor.
    Verify {
        source: PathBuf,
        /// Local graph-root directory whose complete bytes must match.
        #[arg(long)]
        root: PathBuf,
        /// Emit a machine-readable verification result.
        #[arg(long)]
        json: bool,
    },
    /// Verify bundles while copying them through disposable harness scratch.
    PreflightCopy {
        /// Repeatable invocation-local fixture mapping in ID=BUNDLE form.
        #[arg(long = "fixture", value_name = "ID=BUNDLE", required = true)]
        fixtures: Vec<String>,
        /// Create the disposable staging workspace below this existing directory.
        #[arg(long)]
        scratch_root: Option<PathBuf>,
        /// Emit a machine-readable verification result after cleanup.
        #[arg(long)]
        json: bool,
    },
    /// Inspect the logical node-and-edge content of one copied registered fixture.
    ObserveGraph {
        /// Invocation-local fixture mapping in ID=BUNDLE form.
        #[arg(long = "fixture", value_name = "ID=BUNDLE")]
        fixture: String,
        /// Create the disposable staging workspace below this existing directory.
        #[arg(long)]
        scratch_root: Option<PathBuf>,
        /// Emit a machine-readable observation.
        #[arg(long)]
        json: bool,
    },
    /// Validate one copied registered fixture against a logical reference.
    ValidateGraph {
        reference: PathBuf,
        /// Invocation-local fixture mapping in ID=BUNDLE form.
        #[arg(long = "fixture", value_name = "ID=BUNDLE")]
        fixture: String,
        /// Create the disposable staging workspace below this existing directory.
        #[arg(long)]
        scratch_root: Option<PathBuf>,
        /// Emit machine-readable validation evidence.
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
enum FixtureReferenceCommand {
    /// Strictly parse and validate one fixture-reference-v1 YAML file.
    Validate {
        file: PathBuf,
        /// Emit a machine-readable validation result.
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
enum SuiteCommand {
    /// Strictly validate a suite and every referenced case.
    Validate {
        file: PathBuf,
        /// Emit a machine-readable validation result.
        #[arg(long)]
        json: bool,
    },
    /// Resolve a suite into ordered run entries without executing it.
    Plan {
        file: PathBuf,
        /// Select exactly one case id from the suite.
        #[arg(long)]
        case: Option<String>,
        /// Emit a machine-readable execution plan.
        #[arg(long)]
        json: bool,
    },
    /// Execute a validated suite against the supported local runner-v1 envelope.
    Run(Box<SuiteRunArgs>),
}

#[derive(Debug, clap::Args)]
struct ServerArgs {
    /// Address of the already provisioned read-only benchmark server.
    #[arg(long, requires_all = ["graph", "server_receipt"])]
    server: Option<String>,
    /// Graph ID on the server; must equal the deployment receipt's graph.
    #[arg(long, requires = "server")]
    graph: Option<String>,
    /// Bounded deployment receipt with declared build and dataset identity.
    #[arg(long, requires = "server")]
    server_receipt: Option<PathBuf>,
    /// Name of the environment variable containing the bearer token.
    #[arg(long, requires = "server")]
    server_token_env: Option<String>,
    /// Where the client stands to the server; a level of the point.
    #[arg(
        long,
        requires = "server",
        default_value = "same-host",
        value_parser = ["same-host", "same-region", "remote"]
    )]
    network_position: String,
}

impl ServerArgs {
    /// The served input and the declared network position, or `None` for an
    /// embedded run.
    fn resolve(
        self,
    ) -> Result<
        Option<(
            omnigraph_bench::gqt_served::ServedInput,
            omnigraph_bench::case::NetworkPosition,
        )>,
        Diagnostic,
    > {
        use omnigraph_bench::case::NetworkPosition;
        let Some(server) = self.server else {
            return Ok(None);
        };
        let position = match self.network_position.as_str() {
            "same-host" => NetworkPosition::SameHost,
            "same-region" => NetworkPosition::SameRegion,
            _ => NetworkPosition::Remote,
        };
        let graph = self.graph.ok_or_else(|| {
            Diagnostic::error("invalid_server_input", "graph", "--graph is required")
        })?;
        let receipt = self.server_receipt.ok_or_else(|| {
            Diagnostic::error(
                "invalid_server_input",
                "server-receipt",
                "--server-receipt is required",
            )
        })?;
        omnigraph_bench::gqt_served::ServedInput::from_cli(
            &server,
            &graph,
            &receipt,
            self.server_token_env.as_deref(),
        )
        .map(|input| Some((input, position)))
        .map_err(|e| Diagnostic::error("invalid_server_input", e.path, e.message))
    }
}

#[derive(Debug, clap::Args)]
struct SuiteRunArgs {
    file: Option<PathBuf>,
    #[arg(long, requires = "queries", conflicts_with = "file")]
    dataset: Option<PathBuf>,
    #[arg(long, requires = "dataset", conflicts_with = "file")]
    queries: Option<PathBuf>,
    #[arg(long, requires = "dataset")]
    measured_step: Option<usize>,
    #[arg(long, requires = "measured_step", allow_hyphen_values = true)]
    measured_text: Option<String>,
    #[arg(long, requires = "dataset", conflicts_with = "file")]
    repetitions: Option<u32>,
    #[arg(long, requires = "dataset", conflicts_with = "file")]
    deadline_seconds: Option<u64>,
    #[arg(long, requires = "dataset", conflicts_with = "file", value_parser=["apfs","xfs"])]
    filesystem: Option<String>,
    /// Select exactly one case id from the suite.
    #[arg(long)]
    case: Option<String>,
    /// Place disposable fixture trees below this existing directory.
    #[arg(long)]
    scratch_root: Option<PathBuf>,
    #[arg(long)]
    dataset_cache: Option<PathBuf>,
    #[arg(long)]
    no_build: bool,
    #[arg(long = "fixture")]
    fixtures: Vec<String>,
    #[command(flatten)]
    server_args: ServerArgs,
    /// Publish complete immutable run records under this archive root.
    #[arg(long)]
    archive: Option<PathBuf>,
    /// Emit machine-readable diagnostic execution output.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Subcommand)]
enum ArchiveCommand {
    /// Validate every published invocation pointer and canonical run record.
    Verify {
        directory: PathBuf,
        /// Emit a machine-readable verification summary.
        #[arg(long)]
        json: bool,
    },
    /// Resolve one invocation whose pointer durability was previously unknown.
    Reconcile {
        directory: PathBuf,
        #[arg(long)]
        invocation_id: String,
        #[arg(long)]
        record_sha256: String,
        /// Emit a machine-readable reconciliation result.
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
enum ProjectionCommand {
    /// Rebuild an immutable generation from the complete JSON archive.
    Rebuild {
        #[arg(long)]
        archive: PathBuf,
        #[arg(long)]
        root: PathBuf,
        /// Emit a machine-readable build receipt.
        #[arg(long)]
        json: bool,
    },
    /// List one bounded page of benchmark point identities.
    ListPoints {
        #[arg(long)]
        root: PathBuf,
        /// Maximum rows to return (1..=100).
        #[arg(long, default_value_t = DEFAULT_PROJECTION_PAGE_SIZE)]
        limit: u32,
        /// JSON cursor emitted by the preceding page.
        #[arg(long, value_parser = parse_projection_cursor)]
        cursor: Option<ProjectionCursorV1>,
        /// Emit the query result as JSON.
        #[arg(long)]
        json: bool,
    },
    /// List one bounded page of invocations for one full point id.
    ListRuns {
        #[arg(long)]
        root: PathBuf,
        #[arg(long)]
        point_id: String,
        /// Maximum rows to return (1..=100).
        #[arg(long, default_value_t = DEFAULT_PROJECTION_PAGE_SIZE)]
        limit: u32,
        /// JSON cursor emitted by the preceding page.
        #[arg(long, value_parser = parse_projection_cursor)]
        cursor: Option<ProjectionCursorV1>,
        /// Emit the query result as JSON.
        #[arg(long)]
        json: bool,
    },
}

fn parse_projection_cursor(value: &str) -> Result<ProjectionCursorV1, String> {
    serde_json::from_str(value).map_err(|error| format!("invalid projection cursor JSON: {error}"))
}

#[derive(Debug, Serialize)]
struct CaseSummary<'a> {
    id: &'a str,
    path: &'a Path,
    point_id: Option<&'a str>,
    case_digest: &'a str,
}
impl<'a> CaseSummary<'a> {
    fn new(path: &'a Path, case: &'a ValidatedCase) -> Self {
        Self {
            id: case.id(),
            path,
            point_id: case.point_id(),
            case_digest: case.case_digest(),
        }
    }
}
#[derive(Debug, Serialize)]
struct Plan<'a> {
    plan_version: u32,
    suite: &'a str,
    suite_path: &'a Path,
    runs: Vec<&'a ResolvedRun>,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error) => {
            if error.use_stderr() && std::env::args_os().any(|arg| arg == "--json") {
                return bench_cli::failure(
                    vec![Diagnostic::error(
                        "invalid_arguments",
                        "$",
                        error.to_string(),
                    )],
                    true,
                );
            }
            let failed = error.use_stderr();
            let _ = error.print();
            return if failed {
                ExitCode::FAILURE
            } else {
                ExitCode::SUCCESS
            };
        }
    };
    match cli.command {
        Command::List(args) => bench_cli::list(args),
        Command::Show(args) => bench_cli::show(args),
        Command::Cache { command } => bench_cli::cache(command),
        Command::Init(args) => bench_cli::init(args),
        Command::Help(args) => bench_cli::help(args),
        Command::Case { command } => run_case(command),
        Command::Fixture { command } => run_fixture(command).await,
        Command::Suite { command } => run_suite(command).await,
        Command::Archive { command } => run_archive(command),
        Command::Report {
            scenario,
            config,
            catalog,
            store,
            json,
        } => run_report(scenario, config, catalog, store, json).await,
        Command::Projection { command } => run_projection(command).await,
        Command::WorkerV2 => omnigraph_bench::gqt_worker::run_worker_stdio_v2().await,
        Command::FixtureWorkerV1 { request, result } => {
            omnigraph_bench::dataset_worker::run_dataset_worker_files_v1(&request, &result).await
        }
        Command::Dataset { command } => run_dataset(command).await,
        Command::Run(args) => {
            let RunArgs {
                case,
                config,
                catalog,
                dataset,
                queries,
                repetitions,
                dataset_cache,
                no_build,
                fixtures,
                server_args,
                archive,
                local_route,
                store,
                backend,
                product,
                at,
                json,
            } = *args;
            let served = match server_args.resolve() {
                Ok(input) => input,
                Err(diagnostic) => return bench_cli::failure(vec![diagnostic], json),
            };
            let backend = match backend.as_deref().map(declared_backend).transpose() {
                Ok(backend) => backend,
                Err(diagnostic) => return bench_cli::failure(vec![diagnostic], json),
            };
            let store_environment = match (&store, &backend) {
                (Some(store), Some(backend)) if store.starts_with("s3://") => Some(
                    omnigraph_bench::gqt_case::GqtEnvironment::embedded(backend.clone()),
                ),
                (Some(store), None) if store.starts_with("s3://") => {
                    return bench_cli::failure(
                        vec![Diagnostic::error(
                            "invalid_run_input",
                            "backend",
                            "an s3:// store needs --backend, the declared S3 backend the record carries",
                        )],
                        json,
                    );
                }
                (Some(_), Some(_)) => {
                    return bench_cli::failure(
                        vec![Diagnostic::error(
                            "invalid_run_input",
                            "backend",
                            "--backend describes an s3:// store; a local store's backend is probed",
                        )],
                        json,
                    );
                }
                _ => None,
            };
            let run_reset = store
                .is_some()
                .then_some(omnigraph_bench::case::ResetMode::Clone);
            let legacy = if config.is_none() && catalog.is_empty() {
                match case.as_deref().map(bench_cli::legacy_input).transpose() {
                    Ok(legacy) => legacy.unwrap_or(false),
                    Err(e) => return bench_cli::failure(e, json),
                }
            } else {
                false
            };
            if !legacy {
                if dataset.is_some() || queries.is_some() {
                    return bench_cli::failure(
                        vec![Diagnostic::error(
                            "legacy_case_required",
                            "$",
                            "source overrides require a legacy case YAML; use init for a custom config",
                        )],
                        json,
                    );
                }
                let selector = match case.as_deref() {
                    Some(path) => match path.to_str() {
                        Some(name) => Some(name),
                        None => {
                            return bench_cli::failure(
                                vec![Diagnostic::error(
                                    "invalid_selector",
                                    "$",
                                    "scenario ID must be UTF-8",
                                )],
                                json,
                            );
                        }
                    },
                    None => None,
                };
                let catalog = match bench_cli::load_for(
                    &bench_cli::CatalogArgs {
                        config,
                        catalog,
                        json,
                    },
                    selector,
                ) {
                    Ok(c) => c,
                    Err(e) => return bench_cli::failure(e, json),
                };
                let environment = match (&served, store_environment) {
                    (Some((input, position)), _) => omnigraph_bench::catalog::served_environment(
                        input.receipt.backend.clone(),
                        *position,
                    ),
                    (None, Some(environment)) => environment,
                    (None, None) => omnigraph_bench::catalog::local_environment(),
                };
                let suite =
                    match catalog.resolve_with(selector, repetitions, &environment, run_reset) {
                        Ok(s) => s,
                        Err(e) => return bench_cli::failure(e, json),
                    };
                if !at.is_empty() {
                    let store = store.expect("--at requires --store");
                    return run_at(
                        suite,
                        at,
                        store,
                        backend,
                        dataset_cache,
                        no_build,
                        fixtures,
                        json,
                    )
                    .await;
                }
                return run_resolved_suite(
                    suite,
                    None,
                    RunOptions {
                        dataset_cache: Some(dataset_cache),
                        served: served.map(|(input, _)| input),
                        no_build,
                        fixture_bindings: fixtures,
                        worker_executable: std::env::current_exe().ok(),
                        local_route,
                        store,
                        backend,
                        product,
                        ..Default::default()
                    },
                    archive,
                    json,
                )
                .await;
            }
            if !at.is_empty() {
                return bench_cli::failure(
                    vec![Diagnostic::error(
                        "invalid_run_input",
                        "at",
                        "--at runs a catalog scenario or suite, never a legacy case file",
                    )],
                    json,
                );
            }
            let case = match path::absolute(case.expect("legacy route requires a path")) {
                Ok(path) => path,
                Err(e) => {
                    return bench_cli::failure(
                        vec![Diagnostic::error(
                            "case_path_unreadable",
                            "$",
                            e.to_string(),
                        )],
                        json,
                    );
                }
            };
            let repetitions = repetitions.unwrap_or(1);
            let mut loaded = match load_case(&case).into_result() {
                Ok(c) => c,
                Err(e) => return print_cli_failures(e, json),
            };
            if dataset.is_some() || queries.is_some() {
                let plan = match loaded.gqt() {
                    Ok(p) => p.clone(),
                    Err(e) => {
                        return print_cli_failure(
                            Diagnostic::error("invalid_case", "case", e),
                            json,
                        );
                    }
                };
                loaded = match omnigraph_bench::gqt_case::override_sources(
                    plan,
                    dataset.as_deref(),
                    queries.as_deref(),
                ) {
                    Ok(p) => ValidatedCase::Gqt(p),
                    Err(e) => {
                        return print_cli_failure(
                            Diagnostic::error("invalid_case", "sources", e),
                            json,
                        );
                    }
                }
            }
            let suite = ResolvedSuite {
                definition: omnigraph_bench::SuiteV1 {
                    version: 1,
                    name: "command-line".into(),
                    runs: Vec::new(),
                },
                suite_path: case.clone(),
                runs: vec![ResolvedRun {
                    case_path: case,
                    repetitions,
                    case: loaded,
                }],
            };
            run_resolved_suite(
                suite,
                None,
                RunOptions {
                    dataset_cache: Some(dataset_cache),
                    served: served.map(|(input, _)| input),
                    no_build,
                    fixture_bindings: fixtures,
                    worker_executable: std::env::current_exe().ok(),
                    local_route,
                    store,
                    backend,
                    product,
                    ..Default::default()
                },
                archive,
                json,
            )
            .await
        }
    }
}

/// `--backend`: the declared S3 backend as a JSON object, inline or in a file.
fn declared_backend(text: &str) -> Result<omnigraph_bench::case::Backend, Diagnostic> {
    let source = if text.trim_start().starts_with('{') {
        text.to_string()
    } else {
        fs::read_to_string(text).map_err(|e| {
            Diagnostic::error("invalid_run_input", "backend", format!("{text}: {e}"))
        })?
    };
    let backend: omnigraph_bench::case::Backend = serde_json::from_str(&source)
        .map_err(|e| Diagnostic::error("invalid_run_input", "backend", e.to_string()))?;
    if !matches!(backend, omnigraph_bench::case::Backend::S3 { .. }) {
        return Err(Diagnostic::error(
            "invalid_run_input",
            "backend",
            "--backend declares an S3 backend; a local store's backend is probed",
        ));
    }
    Ok(backend)
}

#[derive(Debug, Serialize)]
struct ProjectionFailure {
    ok: bool,
    error: ProjectionError,
}

async fn run_projection(command: ProjectionCommand) -> ExitCode {
    match command {
        ProjectionCommand::Rebuild {
            archive,
            root,
            json,
        } => match rebuild_projection(&archive, &root).await {
            Ok(build) => {
                if json {
                    print_json_success(&build)
                } else {
                    println!(
                        "projection generation {}: {} records, {} points{}",
                        build.generation_id,
                        build.record_count,
                        build.point_count,
                        if build.reused { " (reused)" } else { "" }
                    );
                    ExitCode::SUCCESS
                }
            }
            Err(error) => print_projection_failure(error, json),
        },
        ProjectionCommand::ListPoints {
            root,
            limit,
            cursor,
            json,
        } => match list_points_page(&root, limit, cursor).await {
            Ok(page) => print_projection_page(&page, json),
            Err(error) => print_projection_failure(error, json),
        },
        ProjectionCommand::ListRuns {
            root,
            point_id,
            limit,
            cursor,
            json,
        } => match list_runs_for_point_page(&root, point_id, limit, cursor).await {
            Ok(page) => print_projection_page(&page, json),
            Err(error) => print_projection_failure(error, json),
        },
    }
}

fn print_projection_page(page: &ProjectionPageV1, json: bool) -> ExitCode {
    if json {
        print_json_success(page)
    } else {
        for row in &page.rows {
            match serde_json::to_string(row) {
                Ok(row) => println!("{row}"),
                Err(error) => {
                    eprintln!("could not serialize projection row: {error}");
                    return ExitCode::FAILURE;
                }
            }
        }
        if let Some(cursor) = &page.next_cursor {
            match serde_json::to_string(cursor) {
                Ok(cursor) => eprintln!("next cursor: {cursor}"),
                Err(error) => {
                    eprintln!("could not serialize projection cursor: {error}");
                    return ExitCode::FAILURE;
                }
            }
        }
        ExitCode::SUCCESS
    }
}

fn print_projection_failure(error: ProjectionError, json: bool) -> ExitCode {
    if json {
        let _ = print_json_success(&ProjectionFailure { ok: false, error });
    } else {
        eprintln!("{error}");
    }
    ExitCode::FAILURE
}

#[derive(Debug, Serialize)]
struct ArchiveVerification {
    ok: bool,
    archive_format_version: u32,
    #[serde(serialize_with = "serialize_path_buf_lossy")]
    archive_root: PathBuf,
    record_count: usize,
    authority_inventory_sha256: String,
}

#[derive(Debug, Serialize)]
struct ArchiveVerificationFailure {
    ok: bool,
    archive_format_version: u32,
    #[serde(serialize_with = "serialize_path_buf_lossy")]
    archive_root: PathBuf,
    error: omnigraph_bench::archive::ArchiveError,
}

#[derive(Debug, Serialize)]
struct ArchiveReconciliationFailure {
    ok: bool,
    archive_format_version: u32,
    #[serde(serialize_with = "serialize_path_buf_lossy")]
    archive_root: PathBuf,
    invocation_id: String,
    record_sha256: String,
    error: ArchiveError,
}

fn run_archive(command: ArchiveCommand) -> ExitCode {
    match command {
        ArchiveCommand::Verify { directory, json } => match verify_archive(&directory) {
            Ok(output) => {
                if json {
                    print_json_success(&output)
                } else {
                    println!(
                        "valid archive {} ({} immutable record{})",
                        output.archive_root.display(),
                        output.record_count,
                        if output.record_count == 1 { "" } else { "s" }
                    );
                    ExitCode::SUCCESS
                }
            }
            Err(error) => {
                if json {
                    let _ = print_json_success(&ArchiveVerificationFailure {
                        ok: false,
                        archive_format_version: omnigraph_bench::archive::ARCHIVE_FORMAT_VERSION,
                        archive_root: directory,
                        error,
                    });
                } else {
                    eprintln!("{error}");
                }
                ExitCode::FAILURE
            }
        },
        ArchiveCommand::Reconcile {
            directory,
            invocation_id,
            record_sha256,
            json,
        } => {
            let candidate =
                ArchivePublicationUnknownV1::new(invocation_id.clone(), record_sha256.clone());
            let outcome = candidate
                .and_then(|candidate| reconcile_archive_publication(&directory, &candidate));
            match outcome {
                Ok(outcome) => print_archive_reconciliation(&directory, &outcome, json),
                Err(error) => {
                    if json {
                        let _ = print_json_success(&ArchiveReconciliationFailure {
                            ok: false,
                            archive_format_version:
                                omnigraph_bench::archive::ARCHIVE_FORMAT_VERSION,
                            archive_root: directory,
                            invocation_id,
                            record_sha256,
                            error,
                        });
                    } else {
                        eprintln!("{error}");
                    }
                    ExitCode::FAILURE
                }
            }
        }
    }
}

#[derive(Debug, Serialize)]
struct ArchiveReconciliationOutput<'a> {
    ok: bool,
    archive_format_version: u32,
    #[serde(serialize_with = "serialize_path_ref_lossy")]
    archive_root: &'a Path,
    outcome: &'a ArchiveReconciliationV1,
}

fn print_archive_reconciliation(
    archive_root: &Path,
    outcome: &ArchiveReconciliationV1,
    json: bool,
) -> ExitCode {
    let durable = matches!(outcome, ArchiveReconciliationV1::Durable { .. });
    if json {
        let serialization = print_json_success(&ArchiveReconciliationOutput {
            ok: durable,
            archive_format_version: omnigraph_bench::archive::ARCHIVE_FORMAT_VERSION,
            archive_root,
            outcome,
        });
        if serialization == ExitCode::FAILURE {
            return serialization;
        }
    } else {
        match outcome {
            ArchiveReconciliationV1::Durable { receipt } => println!(
                "durable invocation={} sha256={} pointer={}",
                receipt.invocation_id, receipt.record_sha256, receipt.pointer_relative_path
            ),
            ArchiveReconciliationV1::Absent { candidate } => eprintln!(
                "absent invocation={} sha256={}; the candidate was not published",
                candidate.invocation_id, candidate.record_sha256
            ),
            ArchiveReconciliationV1::Conflict {
                candidate,
                published,
            } => eprintln!(
                "conflict invocation={} candidate_sha256={} published_sha256={}",
                candidate.invocation_id, candidate.record_sha256, published.record_sha256
            ),
        }
    }
    if durable {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

fn verify_archive(directory: &Path) -> Result<ArchiveVerification, ArchiveError> {
    const DOMAIN: &[u8] = b"omnigraph-bench-archive-inventory-v1\0";

    let records = iter_archive(directory)?;
    let mut inventory = Sha256::new();
    inventory.update(DOMAIN);
    let mut record_count = 0usize;
    for archived in records {
        let archived = archived?;
        digest_inventory_field(&mut inventory, archived.receipt.invocation_id.as_bytes());
        digest_inventory_field(&mut inventory, archived.receipt.record_sha256.as_bytes());
        record_count += 1;
    }
    Ok(ArchiveVerification {
        ok: true,
        archive_format_version: omnigraph_bench::archive::ARCHIVE_FORMAT_VERSION,
        archive_root: directory.to_path_buf(),
        record_count,
        authority_inventory_sha256: format!("{:x}", inventory.finalize()),
    })
}

fn digest_inventory_field(digest: &mut Sha256, value: &[u8]) {
    digest.update(
        u64::try_from(value.len())
            .expect("validated archive identity fields fit u64")
            .to_be_bytes(),
    );
    digest.update(value);
}

fn run_case(command: CaseCommand) -> ExitCode {
    match command {
        CaseCommand::Validate { file, json } => {
            let outcome = load_case(&file);
            if json {
                print_json(&outcome)
            } else {
                print_case_validation(&file, outcome)
            }
        }
        CaseCommand::List { directory, json } => list_cases(&directory, json),
    }
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct RealGraphInspectionV1 {
    version: u32,
    fixture: FixtureCopyPreflightReceiptV1,
    reference_sha256: Option<String>,
    implemented_witnesses_match: bool,
    claim_eligible: bool,
    observation: RealGraphObservationV1,
}

async fn run_fixture(command: FixtureCommand) -> ExitCode {
    match command {
        FixtureCommand::Reference { command } => match command {
            FixtureReferenceCommand::Validate { file, json } => {
                let outcome = load_fixture_reference(&file);
                if json {
                    print_json(&outcome)
                } else {
                    match outcome.into_result() {
                        Ok(reference) => {
                            println!(
                                "valid fixture reference {} reference_sha256={}",
                                reference.definition.fixture_id, reference.reference_sha256,
                            );
                            ExitCode::SUCCESS
                        }
                        Err(diagnostics) => print_diagnostics(&diagnostics),
                    }
                }
            }
        },
        FixtureCommand::Fingerprint { id, root } => {
            match fingerprint_registered_fixture(id, &root).into_result() {
                Ok(fixture) => print_json_success(&fixture),
                Err(diagnostics) => print_diagnostics(&diagnostics),
            }
        }
        FixtureCommand::Verify { source, root, json } => {
            let outcome = verify_registered_fixture(&source, &root);
            if json {
                print_json(&outcome)
            } else {
                match outcome.into_result() {
                    Ok(verified) => {
                        println!(
                            "verified fixture {} source_descriptor_sha256={} root={} files={} bytes={} tree_sha256={}",
                            verified.fixture_id,
                            verified.source_descriptor_sha256,
                            verified.canonical_root.display(),
                            verified.physical.files,
                            verified.physical.bytes,
                            verified.physical.tree_sha256,
                        );
                        ExitCode::SUCCESS
                    }
                    Err(diagnostics) => print_diagnostics(&diagnostics),
                }
            }
        }
        FixtureCommand::PreflightCopy {
            fixtures,
            scratch_root,
            json,
        } => {
            let outcome = preflight_copy_fixture_bindings(&fixtures, scratch_root.as_deref());
            if json {
                print_json(&outcome)
            } else {
                match outcome.into_result() {
                    Ok(fixtures) => {
                        for fixture in fixtures {
                            println!(
                                "preflight-copied fixture {} source_descriptor_sha256={} files={} bytes={} tree_sha256={}",
                                fixture.fixture_id,
                                fixture.source_descriptor_sha256,
                                fixture.physical.files,
                                fixture.physical.bytes,
                                fixture.physical.tree_sha256,
                            );
                        }
                        println!("disposable scratch removed");
                        ExitCode::SUCCESS
                    }
                    Err(diagnostics) => print_diagnostics(&diagnostics),
                }
            }
        }
        FixtureCommand::ObserveGraph {
            fixture,
            scratch_root,
            json,
        } => inspect_real_graph_fixture(&fixture, scratch_root.as_deref(), None, json).await,
        FixtureCommand::ValidateGraph {
            reference,
            fixture,
            scratch_root,
            json,
        } => {
            let reference = match load_fixture_reference(&reference).into_result() {
                Ok(reference) => reference,
                Err(diagnostics) => return print_cli_failures(diagnostics, json),
            };
            inspect_real_graph_fixture(&fixture, scratch_root.as_deref(), Some(&reference), json)
                .await
        }
    }
}

async fn inspect_real_graph_fixture(
    fixture: &str,
    scratch_root: Option<&Path>,
    reference: Option<&omnigraph_bench::fixture_reference::NormalizedFixtureReferenceV1>,
    json: bool,
) -> ExitCode {
    let staged = match stage_registered_fixture_binding(fixture, scratch_root).into_result() {
        Ok(staged) => staged,
        Err(diagnostics) => return print_cli_failures(diagnostics, json),
    };
    if let Some(reference) = reference
        && reference.definition.fixture_id != staged.receipt().fixture_id
    {
        let diagnostic = Diagnostic::error(
            "fixture_reference_id_mismatch",
            "--fixture",
            format!(
                "logical reference identifies fixture {:?}, but the registered bundle identifies {:?}",
                reference.definition.fixture_id,
                staged.receipt().fixture_id
            ),
        );
        let cleanup = staged.finish();
        return match cleanup {
            Ok(_) => print_cli_failure(diagnostic, json),
            Err(cleanup) => print_cli_failures(vec![diagnostic, cleanup], json),
        };
    }
    let observation = match observe_real_graph(staged.root()).await {
        Ok(observation) => observation,
        Err(error) => {
            let diagnostic =
                Diagnostic::error("real_graph_observation_failed", "$", error.to_string());
            let cleanup = staged.finish();
            return match cleanup {
                Ok(_) => print_cli_failure(diagnostic, json),
                Err(cleanup) => print_cli_failures(vec![diagnostic, cleanup], json),
            };
        }
    };
    if let Some(reference) = reference
        && let Err(error) = validate_real_graph_reference(reference, &observation)
    {
        let diagnostic = Diagnostic::error(
            "real_graph_reference_mismatch",
            "reference",
            error.to_string(),
        );
        let cleanup = staged.finish();
        return match cleanup {
            Ok(_) => print_cli_failure(diagnostic, json),
            Err(cleanup) => print_cli_failures(vec![diagnostic, cleanup], json),
        };
    }
    if let Err(diagnostic) = staged.verify_unchanged() {
        let cleanup = staged.finish();
        return match cleanup {
            Ok(_) => print_cli_failure(diagnostic, json),
            Err(cleanup) => print_cli_failures(vec![diagnostic, cleanup], json),
        };
    }
    let receipt = match staged.finish() {
        Ok(receipt) => receipt,
        Err(diagnostic) => return print_cli_failure(diagnostic, json),
    };
    let inspection = RealGraphInspectionV1 {
        version: 1,
        fixture: receipt,
        reference_sha256: reference.map(|reference| reference.reference_sha256.clone()),
        implemented_witnesses_match: reference.is_some(),
        claim_eligible: false,
        observation,
    };
    if json {
        print_json_success(&inspection)
    } else {
        println!(
            "observed real graph fixture {}: {} node rows, {} edge rows, history depth {}; claim-eligible=false{}",
            inspection.fixture.fixture_id,
            inspection
                .observation
                .node_tables
                .iter()
                .map(|table| table.rows)
                .sum::<u64>(),
            inspection
                .observation
                .edge_tables
                .iter()
                .map(|table| table.rows)
                .sum::<u64>(),
            inspection.observation.history_depth,
            if inspection.implemented_witnesses_match {
                " (implemented reference witnesses match; declared state remains partially unverified)"
            } else {
                ""
            },
        );
        ExitCode::SUCCESS
    }
}

async fn run_suite(command: SuiteCommand) -> ExitCode {
    match command {
        SuiteCommand::Validate { file, json } => {
            let outcome = load_suite(&file);
            if json {
                print_json(&outcome)
            } else {
                match outcome.into_result() {
                    Ok(suite) => {
                        println!(
                            "valid suite {} ({} cases)",
                            suite.definition.name,
                            suite.runs.len()
                        );
                        ExitCode::SUCCESS
                    }
                    Err(diagnostics) => print_diagnostics(&diagnostics),
                }
            }
        }
        SuiteCommand::Plan { file, case, json } => plan_suite(&file, case.as_deref(), json),
        SuiteCommand::Run(args) => {
            let SuiteRunArgs {
                file,
                case,
                scratch_root,
                dataset_cache,
                no_build,
                fixtures,
                server_args,
                archive,
                json,
                dataset,
                queries,
                measured_step,
                measured_text,
                repetitions,
                deadline_seconds,
                filesystem,
            } = *args;
            let served = match server_args.resolve() {
                Ok(input) => input.map(|(input, _)| input),
                Err(diagnostic) => return bench_cli::failure(vec![diagnostic], json),
            };
            let options = RunOptions {
                served,
                scratch_root,
                dataset_cache: dataset_cache.or_else(|| Some(PathBuf::from("target/gqt-datasets"))),
                no_build,
                fixture_bindings: fixtures,
                worker_executable: std::env::current_exe().ok(),
                local_route: Default::default(),
                store: None,
                backend: None,
                product: None,
                product_commit: None,
            };
            if let Some(file) = file {
                return run_suite_execution(&file, case.as_deref(), options, archive, json).await;
            }
            let (Some(dataset), Some(queries), Some(ordinal), Some(text)) =
                (dataset, queries, measured_step, measured_text)
            else {
                return print_cli_failure(
                    Diagnostic::error(
                        "missing_gqt_pair",
                        "suite run",
                        "supply a suite path or --dataset, --queries, --measured-step and --measured-text",
                    ),
                    json,
                );
            };
            if case.is_some() {
                return print_cli_failure(
                    Diagnostic::error(
                        "invalid_selector",
                        "--case",
                        "case selectors require a suite path",
                    ),
                    json,
                );
            }
            if options.served.is_some() {
                return bench_cli::failure(
                    vec![Diagnostic::error(
                        "invalid_server_input",
                        "server",
                        "served runs select a catalog scenario; explicit pairs are embedded only",
                    )],
                    json,
                );
            }
            let (backend, reset) = local_backend(filesystem.as_deref());
            let plan = match omnigraph_bench::gqt_case::explicit_pair(
                &dataset,
                &queries,
                omnigraph_bench::gqt_case::MeasuredStep { ordinal, text },
                backend,
                reset,
                Some(deadline_seconds.unwrap_or(60)),
            ) {
                Ok(p) => p,
                Err(e) => {
                    return print_cli_failure(
                        Diagnostic::error("invalid_gqt_pair", "suite run", e),
                        json,
                    );
                }
            };
            let suite = ResolvedSuite {
                definition: omnigraph_bench::SuiteV1 {
                    version: 1,
                    name: "explicit-pair".into(),
                    runs: Vec::new(),
                },
                suite_path: queries.clone(),
                runs: vec![ResolvedRun {
                    case_path: queries,
                    repetitions: repetitions.unwrap_or(5),
                    case: ValidatedCase::Gqt(plan),
                }],
            };
            run_resolved_suite(suite, None, options, archive, json).await
        }
    }
}

fn print_case_validation(file: &Path, outcome: ValidationOutcome<ValidatedCase>) -> ExitCode {
    match outcome.into_result() {
        Ok(case) => {
            println!("valid case {} ({})", case.id(), file.display());
            ExitCode::SUCCESS
        }
        Err(e) => print_diagnostics(&e),
    }
}

fn list_cases(directory: &Path, json: bool) -> ExitCode {
    let mut paths = match case_files(directory) {
        Ok(paths) => paths,
        Err(diagnostic) => return print_cli_failure(diagnostic, json),
    };
    paths.sort();

    let loaded: Vec<_> = paths.iter().map(|path| (path, load_case(path))).collect();
    let diagnostics: Vec<_> = loaded
        .iter()
        .flat_map(|(path, outcome)| {
            outcome
                .diagnostics
                .iter()
                .cloned()
                .map(move |mut diagnostic| {
                    diagnostic.path = format!("{}:{}", path.display(), diagnostic.path);
                    diagnostic
                })
        })
        .collect();
    if !diagnostics.is_empty() {
        return print_cli_failures(diagnostics, json);
    }

    let mut identities = Vec::with_capacity(loaded.len());
    for (path, outcome) in &loaded {
        let Some(case) = outcome.value.as_ref() else {
            return print_cli_failure(
                Diagnostic::error(
                    "invalid_validation_outcome",
                    path.display().to_string(),
                    "case validation reported no diagnostics and produced no value",
                ),
                json,
            );
        };
        identities.push((*path, case));
    }
    let diagnostics = duplicate_catalog_diagnostics(&identities);
    if !diagnostics.is_empty() {
        return print_cli_failures(diagnostics, json);
    }

    let cases: Vec<_> = identities
        .iter()
        .map(|(path, case)| CaseSummary::new(path, case))
        .collect();
    if json {
        print_json_success(&cases)
    } else {
        for case in cases {
            println!("{} {:?} {}", case.id, case.point_id, case.path.display());
        }
        ExitCode::SUCCESS
    }
}

fn case_files(directory: &Path) -> Result<Vec<PathBuf>, Diagnostic> {
    let entries = fs::read_dir(directory).map_err(|error| {
        Diagnostic::error(
            "case_directory_read_error",
            directory.display().to_string(),
            format!("could not read case directory: {error}"),
        )
    })?;
    let mut paths = Vec::new();
    for (index, entry) in entries.enumerate() {
        if index >= MAX_DIRECTORY_ENTRIES {
            return Err(Diagnostic::error(
                "case_directory_entry_budget_exceeded",
                directory.display().to_string(),
                format!("case directory may contain at most {MAX_DIRECTORY_ENTRIES} entries"),
            ));
        }
        let entry = entry.map_err(|error| {
            Diagnostic::error(
                "case_directory_entry_error",
                directory.display().to_string(),
                format!("could not read case directory entry: {error}"),
            )
        })?;
        let path = entry.path();
        if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(".case-v1.yaml"))
        {
            paths.push(path);
            if paths.len() > MAX_CASE_FILES {
                return Err(Diagnostic::error(
                    "case_catalog_budget_exceeded",
                    directory.display().to_string(),
                    format!("case catalog may contain at most {MAX_CASE_FILES} case files"),
                ));
            }
        }
    }
    Ok(paths)
}

fn duplicate_catalog_diagnostics(cases: &[(&PathBuf, &ValidatedCase)]) -> Vec<Diagnostic> {
    let mut ids = BTreeMap::new();
    let mut points = BTreeMap::new();
    let mut out = Vec::new();
    for (path, case) in cases {
        if ids.insert(case.id(), *path).is_some() {
            out.push(Diagnostic::error(
                "duplicate_case_id",
                path.display().to_string(),
                "duplicate case id",
            ))
        }
        if points.insert(case.planned_identity(), *path).is_some() {
            out.push(Diagnostic::error(
                "duplicate_point_id",
                path.display().to_string(),
                "duplicate experiment recipe",
            ))
        }
    }
    out
}
fn plan_suite(path: &Path, selector: Option<&str>, json: bool) -> ExitCode {
    let suite = match load_suite(path).into_result() {
        Ok(s) => s,
        Err(e) => return print_cli_failures(e, json),
    };
    let selected = match select_runs(&suite, selector) {
        Ok(r) => r,
        Err(e) => return print_cli_failure(e, json),
    };
    let plan = Plan {
        plan_version: PLAN_FORMAT_VERSION,
        suite: &suite.definition.name,
        suite_path: &suite.suite_path,
        runs: selected,
    };
    if json {
        print_json_success(&plan)
    } else {
        println!("suite {}", plan.suite);
        for run in plan.runs {
            println!(
                "{} repetitions={} dataset-bound-point=pending case={}",
                run.case.id(),
                run.repetitions,
                run.case_path.display()
            )
        }
        ExitCode::SUCCESS
    }
}

fn select_runs<'a>(
    suite: &'a ResolvedSuite,
    selector: Option<&str>,
) -> Result<Vec<&'a ResolvedRun>, Diagnostic> {
    let selected = suite
        .runs
        .iter()
        .filter(|run| selector.is_none_or(|id| run.case.id() == id))
        .collect::<Vec<_>>();
    if let Some(id) = selector
        && selected.is_empty()
    {
        return Err(Diagnostic::error(
            "unknown_case_selector",
            "--case",
            format!("suite '{}' has no case id '{id}'", suite.definition.name),
        ));
    }
    Ok(selected)
}

fn classify_censored_prefix<T>(
    partial: Option<T>,
    observed_repetitions: impl FnOnce(&T) -> usize,
    failure_stage: Option<&str>,
    failure_code: &str,
) -> Result<Option<(T, AcquisitionTerminalV1)>, RecordingError> {
    let Some(partial) = partial else {
        return Ok(None);
    };
    let observed = observed_repetitions(&partial);
    if observed == 0 {
        return Ok(None);
    }
    let observed = u32::try_from(observed).expect("runner repetition bounds fit u32");
    let stage = acquisition_terminal_stage(failure_stage)?;
    let terminal = AcquisitionTerminalV1::new(observed, stage, failure_code)
        .map_err(RecordingError::from_record)?;
    Ok(Some((partial, terminal)))
}

fn acquisition_terminal_stage(
    runner_stage: Option<&str>,
) -> Result<AcquisitionTerminalStageV1, RecordingError> {
    use AcquisitionTerminalStageV1 as Stage;

    let stage = match runner_stage {
        None => Stage::Runner,
        Some("supervisor-panic") => Stage::SupervisorPanic,
        Some("Bootstrap") => Stage::Bootstrap,
        Some("Prepare") => Stage::Prepare,
        Some("Measure") => Stage::Measure,
        Some("Verify") => Stage::Verify,
        Some("Finalize") => Stage::Finalize,
        Some("Protocol") => Stage::Protocol,
        Some("pipe-setup") => Stage::PipeSetup,
        Some("writer-setup") => Stage::WriterSetup,
        Some("reader-setup") => Stage::ReaderSetup,
        Some("request-write") => Stage::RequestWrite,
        Some("prepare-timeout") => Stage::PrepareTimeout,
        Some("prepare-protocol") => Stage::PrepareProtocol,
        Some("begin-write") => Stage::BeginWrite,
        Some("measure-timeout") => Stage::MeasureTimeout,
        Some("measure-protocol") => Stage::MeasureProtocol,
        Some("verify-timeout") => Stage::VerifyTimeout,
        Some("verify-protocol") => Stage::VerifyProtocol,
        Some("finalize-protocol") => Stage::FinalizeProtocol,
        Some("exit-timeout") => Stage::ExitTimeout,
        Some("group-proof") => Stage::GroupProof,
        Some("finalize-exit") => Stage::FinalizeExit,
        Some("structured-failure-reap") => Stage::StructuredFailureReap,
        Some(_) => {
            return Err(RecordingError::new(
                "invalid_acquisition_terminal_stage",
                "runner failure carried a child-process stage outside the closed run-record-v1 terminal-stage registry",
            ));
        }
    };
    Ok(stage)
}

async fn run_suite_execution(
    path: &Path,
    selector: Option<&str>,
    options: RunOptions,
    archive: Option<PathBuf>,
    json: bool,
) -> ExitCode {
    let suite = match load_suite(path).into_result() {
        Ok(suite) => suite,
        Err(diagnostics) => return print_cli_failures(diagnostics, json),
    };
    run_resolved_suite(suite, selector, options, archive, json).await
}
async fn run_resolved_suite(
    suite: ResolvedSuite,
    selector: Option<&str>,
    options: RunOptions,
    archive: Option<PathBuf>,
    json: bool,
) -> ExitCode {
    match execute_resolved_suite(suite, selector, options, archive, None, None, json).await {
        Ok(output) if json => print_json_success(&output),
        Ok(output) => print_execution(&output),
        Err(code) => code,
    }
}

/// Runs the selection and returns its output, or the exit code of a failure
/// already printed; a `--store` run without `--archive` puts every completed
/// record into the store under `session` (minted here when absent).
/// Every selected run's options checked before any run, so a selection with
/// one point that cannot run this way is refused whole and nothing is measured.
fn validate_selected(
    selected: &[&ResolvedRun],
    options: &RunOptions,
    json: bool,
) -> Result<(), ExitCode> {
    for run in selected {
        let validation = run.case.gqt().map_err(|e| e.to_string()).and_then(|plan| {
            omnigraph_bench::gqt_runner::validate_run_options(plan, options)
                .map_err(|e| e.to_string())
        });
        if let Err(message) = validation {
            return Err(bench_cli::failure(
                vec![Diagnostic::error("invalid_run_input", "run", message)],
                json,
            ));
        }
    }
    Ok(())
}

/// `prior`, the session's output so far (a `--at` ref's earlier scenarios), is
/// carried, so the result and a failure document cover every completed run.
async fn execute_resolved_suite(
    suite: ResolvedSuite,
    selector: Option<&str>,
    options: RunOptions,
    archive: Option<PathBuf>,
    session: Option<Ulid>,
    prior: Option<&SuiteRunOutput>,
    json: bool,
) -> Result<SuiteRunOutput, ExitCode> {
    let selected = match select_runs(&suite, selector) {
        Ok(selected) => selected,
        Err(diagnostic) => return Err(print_cli_failure(diagnostic, json)),
    };
    validate_selected(&selected, &options, json)?;
    let recording = match archive {
        Some(root) => match RecordingContext::new(root.clone()) {
            Ok(context) => Some(context),
            Err(error) => {
                return Err(print_recording_failure(
                    &suite,
                    0,
                    None,
                    &[],
                    None,
                    Some(&root),
                    &error,
                    json,
                ));
            }
        },
        None => None,
    };
    let store = match (&options.store, &recording) {
        (Some(uri), None) => match omnigraph_bench::store::FixtureStore::open(uri) {
            Ok(store) => {
                let session = match session {
                    Some(session) => session,
                    None => Ulid::new(),
                };
                Some((store, session))
            }
            Err(error) => {
                return Err(print_cli_failure(
                    Diagnostic::error(&error.code, "store", error.message),
                    json,
                ));
            }
        },
        _ => None,
    };
    let mut store_receipts = prior
        .and_then(|prior| prior.store_records.as_ref())
        .map(|records| records.records.clone())
        .unwrap_or_default();
    // Raw samples already become durable authority one record at a time. In
    // archive mode, retaining them all again in the CLI result would make
    // memory grow with the complete suite and defeat streaming publication.
    let mut diagnostic_runs = recording.is_none().then(|| {
        prior
            .and_then(|prior| prior.runs.clone())
            .unwrap_or_else(|| Vec::with_capacity(selected.len()))
    });
    let mut receipts = Vec::with_capacity(selected.len());
    let mut completed_run_count = prior.map_or(0usize, |prior| prior.completed_run_count);
    let mut bound_points: std::collections::BTreeSet<_> = diagnostic_runs
        .iter()
        .flatten()
        .map(|execution| execution.point_id.clone())
        .collect();
    for run in selected {
        let invocation = recording.as_ref().map(RecordingContext::begin_invocation);
        let execution = match execute_run(run, &options).await {
            Ok(execution) => execution,
            Err(mut error) => {
                let partial_run = error.context.gqt_partial_run.as_deref().cloned();
                let censored = if let Some(recording) = recording.as_ref() {
                    match classify_censored_prefix(
                        partial_run.clone().filter(|partial| {
                            partial.samples.len() < (partial.requested_repetitions as usize)
                        }),
                        |partial| partial.samples.len(),
                        error
                            .context
                            .child_process
                            .as_ref()
                            .map(|evidence| evidence.stage.as_str()),
                        &error.code,
                    ) {
                        Ok(censored) => censored,
                        Err(recording_error) => {
                            return Err(print_recording_failure(
                                &suite,
                                completed_run_count,
                                partial_run.as_ref(),
                                &receipts,
                                Some(recording),
                                Some(&recording.archive_root),
                                &recording_error.with_acquisition_failure(&error),
                                json,
                            ));
                        }
                    }
                } else {
                    None
                };
                if let (Some(recording), Some(invocation), Some((partial, terminal))) =
                    (recording.as_ref(), invocation, censored)
                {
                    match recording.publish_censored(run, &partial, invocation, terminal) {
                        Ok(receipt) => {
                            receipts.push(receipt);
                            error.context.clear_completed_prefix();
                            error.context.settled_sample = None;
                        }
                        Err(recording_error) => {
                            return Err(print_recording_failure(
                                &suite,
                                completed_run_count,
                                Some(&partial),
                                &receipts,
                                Some(recording),
                                Some(&recording.archive_root),
                                &recording_error.with_acquisition_failure(&error),
                                json,
                            ));
                        }
                    }
                }
                return Err(print_runner_failure(
                    &suite,
                    completed_run_count,
                    diagnostic_runs.as_deref(),
                    &receipts,
                    recording.as_ref(),
                    &error,
                    json,
                ));
            }
        };
        if !bound_points.insert(execution.point_id.clone()) {
            return Err(print_cli_failure(
                Diagnostic::error(
                    "duplicate_point_id",
                    "suite",
                    "two planned recipes bound to the same dataset point",
                ),
                json,
            ));
        }
        if let Some(recording) = &recording {
            let invocation = invocation.expect("recording context minted an invocation");
            match recording.publish(run, &execution, invocation) {
                Ok(receipt) => receipts.push(receipt),
                Err(error) => {
                    return Err(print_recording_failure(
                        &suite,
                        completed_run_count.saturating_add(1),
                        Some(&execution),
                        &receipts,
                        Some(recording),
                        Some(&recording.archive_root),
                        &error,
                        json,
                    ));
                }
            }
        } else {
            if let Some((store, session)) = &store {
                let record = match omnigraph_bench::gqt_record::build(
                    &execution,
                    invocation_in_session(*session),
                    None,
                ) {
                    Ok(record) => record,
                    Err(error) => {
                        return Err(print_cli_failure(
                            Diagnostic::error("record_build_failed", "store", format!("{error:?}")),
                            json,
                        ));
                    }
                };
                match store.put_record(&record).await {
                    Ok(receipt) => store_receipts.push(receipt),
                    Err(error) => {
                        return Err(print_cli_failure(
                            Diagnostic::error(&error.code, "store", error.message),
                            json,
                        ));
                    }
                }
            }
            diagnostic_runs
                .as_mut()
                .expect("diagnostic mode retains executions")
                .push(execution);
        }
        completed_run_count = completed_run_count.saturating_add(1);
    }
    Ok(SuiteRunOutput {
        runner_output_version: RUNNER_OUTPUT_VERSION,
        suite: suite.definition.name,
        suite_path: suite.suite_path,
        local_route: (options.local_route != Default::default())
            .then(|| options.local_route.name()),
        completed_run_count,
        runs: diagnostic_runs,
        durable_archive: recording.map(|recording| DurableArchiveOutput {
            archive_format_version: omnigraph_bench::archive::ARCHIVE_FORMAT_VERSION,
            archive_root: recording.archive_root,
            session_id: recording.session_id.to_string(),
            records: receipts,
        }),
        store_records: store.map(|(store, session)| StoreRecordsOutput {
            store: store.uri(),
            session_id: session.to_string(),
            records: store_receipts,
        }),
    })
}

/// One invocation identity inside a session: never below the session's
/// timestamp and never the session id itself.
fn invocation_in_session(session_id: Ulid) -> InvocationIdentityV1 {
    let candidate = Ulid::new();
    let mut invocation = if candidate.timestamp_ms() < session_id.timestamp_ms() {
        Ulid::from_parts(session_id.timestamp_ms(), candidate.random())
    } else {
        candidate
    };
    if invocation == session_id {
        invocation = Ulid::from_parts(invocation.timestamp_ms(), invocation.random() ^ 1);
    }
    InvocationIdentityV1 {
        invocation_id: invocation.to_string(),
        session_id: session_id.to_string(),
        invoked_at_unix_ms: invocation.timestamp_ms(),
    }
}

/// The records a `--store` run put: the store, the session they share and
/// one receipt per completed run.
#[derive(Debug, Serialize)]
struct StoreRecordsOutput {
    store: String,
    session_id: String,
    records: Vec<omnigraph_bench::store::StoreRecordReceiptV1>,
}

/// One `--at` target: the ref as given, the commit it resolved to and the
/// product binary measured for it.
#[derive(Debug, Serialize)]
struct AtOutput {
    reference: String,
    commit: String,
    product: omnigraph_bench::gqt_runner::ProductBinary,
    output: SuiteRunOutput,
}

#[derive(Debug, Serialize)]
struct AtRunOutput {
    runner_output_version: u32,
    store: String,
    at: Vec<AtOutput>,
}

/// One `run --at` invocation's own, absolute staging directory under the
/// dataset cache, removed when the invocation ends (the store holds every
/// product that was put).
struct ProductStaging(PathBuf);

impl Drop for ProductStaging {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// `run --at a --at b`: every product inspected and every scenario's options
/// checked before any run, then every scenario run once per ref, in `--at`
/// order, before the next scenario, so the refs see the same machine state.
#[allow(clippy::too_many_arguments)]
async fn run_at(
    suite: ResolvedSuite,
    at: Vec<String>,
    store: String,
    backend: Option<omnigraph_bench::case::Backend>,
    dataset_cache: PathBuf,
    no_build: bool,
    fixtures: Vec<String>,
    json: bool,
) -> ExitCode {
    let environment = omnigraph_bench::gqt_runner::product_environment();
    let fixture_store = match omnigraph_bench::store::FixtureStore::open(&store) {
        Ok(store) => store,
        Err(error) => {
            return print_cli_failure(Diagnostic::error(&error.code, "store", error.message), json);
        }
    };
    let scratch =
        match std::path::absolute(dataset_cache.join("products").join(Ulid::new().to_string())) {
            Ok(scratch) => scratch,
            Err(error) => {
                return print_cli_failure(
                    Diagnostic::error("invalid_run_input", "dataset-cache", error.to_string()),
                    json,
                );
            }
        };
    if let Err(error) = fs::create_dir_all(&scratch) {
        return print_cli_failure(
            Diagnostic::error("invalid_run_input", "dataset-cache", error.to_string()),
            json,
        );
    }
    let _staging = ProductStaging(scratch.clone());
    let mut references = Vec::new();
    for spec in &at {
        let (reference, path) = match spec.split_once('=') {
            Some((reference, path)) => (reference, Some(PathBuf::from(path))),
            None => (spec.as_str(), None),
        };
        match git_commit(reference) {
            Ok(commit) => references.push((reference, commit, path)),
            Err(diagnostic) => return print_cli_failure(diagnostic, json),
        }
    }
    let mut targets = Vec::new();
    for (reference, commit, path) in references {
        let path = match path {
            Some(path) => path,
            None => match product_at(&fixture_store, &commit, &scratch).await {
                Ok(path) => path,
                Err(diagnostic) => return print_cli_failure(diagnostic, json),
            },
        };
        let product = match omnigraph_bench::gqt_runner::inspect_product(
            &path,
            Some(commit.clone()),
            &environment,
        ) {
            Ok(product) => product,
            Err(error) if error.code == "product_schema_mismatch" => {
                return print_cli_failure(
                    Diagnostic::error(
                        "not_comparable",
                        "at",
                        format!("not comparable: {reference}: {}", error.message),
                    ),
                    json,
                );
            }
            Err(error) => {
                return print_cli_failure(
                    Diagnostic::error(&error.code, "at", error.message),
                    json,
                );
            }
        };
        targets.push((reference.to_string(), commit, product));
    }
    let scenarios: Vec<String> = suite
        .runs
        .iter()
        .map(|run| run.case.id().to_owned())
        .collect();
    let mut outputs: Vec<(Ulid, AtOutput)> = targets
        .into_iter()
        .map(|(reference, commit, product)| {
            let session = Ulid::new();
            let output = SuiteRunOutput {
                runner_output_version: RUNNER_OUTPUT_VERSION,
                suite: suite.definition.name.clone(),
                suite_path: suite.suite_path.clone(),
                local_route: None,
                completed_run_count: 0,
                runs: Some(Vec::new()),
                durable_archive: None,
                store_records: Some(StoreRecordsOutput {
                    store: fixture_store.uri(),
                    session_id: session.to_string(),
                    records: Vec::new(),
                }),
            };
            (
                session,
                AtOutput {
                    reference,
                    commit,
                    product,
                    output,
                },
            )
        })
        .collect();
    let options_for = |target: &AtOutput| RunOptions {
        dataset_cache: Some(dataset_cache.clone()),
        no_build,
        fixture_bindings: fixtures.clone(),
        worker_executable: std::env::current_exe().ok(),
        store: Some(store.clone()),
        backend: backend.clone(),
        product: Some(target.product.path.clone()),
        product_commit: Some(target.commit.clone()),
        ..Default::default()
    };
    let selected = match select_runs(&suite, None) {
        Ok(selected) => selected,
        Err(diagnostic) => return print_cli_failure(diagnostic, json),
    };
    for (_, target) in &outputs {
        if let Err(code) = validate_selected(&selected, &options_for(target), json) {
            return code;
        }
    }
    drop(selected);
    for scenario in &scenarios {
        for index in 0..outputs.len() {
            let (session, target) = &mut outputs[index];
            let run = execute_resolved_suite(
                suite.clone(),
                Some(scenario),
                options_for(target),
                None,
                Some(*session),
                Some(&target.output),
                json,
            )
            .await;
            match run {
                Ok(cumulative) => target.output = cumulative,
                Err(code) => {
                    let reference = target.reference.clone();
                    let completed = AtRunOutput {
                        runner_output_version: RUNNER_OUTPUT_VERSION,
                        store,
                        at: outputs
                            .into_iter()
                            .map(|(_, target)| target)
                            .filter(|target| target.output.completed_run_count > 0)
                            .collect(),
                    };
                    if completed.at.is_empty() {
                        return code;
                    }
                    if json {
                        for target in &completed.at {
                            eprintln!(
                                "completed before `{reference}` failed at `{scenario}`: {} {} ({} record(s) put)",
                                target.reference,
                                &target.commit[..12],
                                target
                                    .output
                                    .store_records
                                    .as_ref()
                                    .map(|records| records.records.len())
                                    .unwrap_or(0)
                            );
                        }
                    } else {
                        eprintln!(
                            "`{reference}` failed at `{scenario}`; the records put before it:"
                        );
                        print_at_table(&completed);
                    }
                    return code;
                }
            }
        }
    }
    let output = AtRunOutput {
        runner_output_version: RUNNER_OUTPUT_VERSION,
        store,
        at: outputs.into_iter().map(|(_, target)| target).collect(),
    };
    if json {
        return print_json_success(&output);
    }
    print_at_table(&output);
    ExitCode::SUCCESS
}

fn print_at_table(output: &AtRunOutput) {
    println!("scenario\tref\tcommit\tversion\tn\tmin_us\tp50_us\tmax_us\tlance_calls_p50\trecords");
    for target in &output.at {
        for run in target.output.runs.as_deref().unwrap_or_default() {
            let mut calls: Vec<u64> = run
                .samples
                .iter()
                .filter_map(|sample| sample.logical_store_calls.as_ref())
                .filter_map(|logical| {
                    let manifest =
                        omnigraph_bench::projection::sum_call_counts(logical.manifest).ok()?;
                    let table = omnigraph_bench::projection::sum_call_counts(logical.table).ok()?;
                    manifest.checked_add(table)
                })
                .collect();
            calls.sort_unstable();
            let lance = calls
                .get((calls.len().max(1) - 1) / 2)
                .map(|n| n.to_string())
                .unwrap_or_else(|| "-".into());
            println!(
                "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                run.case_id,
                target.reference,
                &target.commit[..12],
                target.product.package_version,
                run.samples.len(),
                run.wall_clock.min_us,
                run.wall_clock.p50_us,
                run.wall_clock.max_us,
                lance,
                target
                    .output
                    .store_records
                    .as_ref()
                    .map(|records| records.records.len())
                    .unwrap_or(0)
            );
        }
    }
}

/// The full lowercase commit a ref names in the working directory's repository.
fn git_commit(reference: &str) -> Result<String, Diagnostic> {
    let output = std::process::Command::new("git")
        .args(["rev-parse", "--verify", &format!("{reference}^{{commit}}")])
        .output()
        .map_err(|e| Diagnostic::error("invalid_run_input", "at", format!("git: {e}")))?;
    let commit = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if !output.status.success()
        || commit.len() != 40
        || !commit
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    {
        return Err(Diagnostic::error(
            "invalid_run_input",
            "at",
            format!(
                "`{reference}` names no commit here: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ),
        ));
    }
    Ok(commit)
}

/// The commit's `omnigraph`: the store's cached build, else built from an
/// archive of the commit (a clean tree by construction) with the spelling
/// the release packaging uses, and put under the commit for the next run.
async fn product_at(
    store: &omnigraph_bench::store::FixtureStore,
    commit: &str,
    scratch: &Path,
) -> Result<PathBuf, Diagnostic> {
    let key = omnigraph_bench::store::BuildKey {
        commit,
        target_triple: env!("OMNIGRAPH_BENCH_TARGET_TRIPLE"),
        product: "omnigraph",
    };
    let at = |error: omnigraph_bench::runner::RunnerError| {
        Diagnostic::error(&error.code, "at", error.message)
    };
    let io = |context: &str, error: std::io::Error| {
        Diagnostic::error("invalid_run_input", "at", format!("{context}: {error}"))
    };
    let directory = scratch.join(&commit[..12]);
    fs::create_dir_all(&directory).map_err(|e| io("product scratch", e))?;
    let destination = directory.join("omnigraph");
    if destination.is_file() {
        return Ok(destination);
    }
    if let Some(path) = store.resolve_build(key, &destination).await.map_err(at)? {
        return Ok(path);
    }
    let source = directory.join("src");
    let archive = directory.join("src.tar");
    fs::create_dir_all(&source).map_err(|e| io("source directory", e))?;
    for (program, args) in [
        (
            "git",
            vec![
                "archive",
                "--format=tar",
                "-o",
                &*archive.to_string_lossy(),
                commit,
            ],
        ),
        (
            "tar",
            vec![
                "-xf",
                &*archive.to_string_lossy(),
                "-C",
                &*source.to_string_lossy(),
            ],
        ),
        (
            "cargo",
            vec![
                "build",
                "--release",
                "--locked",
                "-p",
                "omnigraph-cli",
                "--bin",
                "omnigraph",
            ],
        ),
    ] {
        let mut command = std::process::Command::new(program);
        command.args(&args);
        if program == "cargo" {
            command
                .current_dir(&source)
                .env("CARGO_TARGET_DIR", directory.join("target"))
                .env("RUSTFLAGS", "")
                .env_remove("CARGO_ENCODED_RUSTFLAGS");
        }
        let output = command.output().map_err(|e| io(program, e))?;
        if !output.status.success() {
            return Err(Diagnostic::error(
                "product_build_failed",
                "at",
                format!(
                    "`{program} {}` exited {} for {commit}: {}",
                    args.join(" "),
                    output.status,
                    String::from_utf8_lossy(&output.stderr)
                        .lines()
                        .rev()
                        .take(20)
                        .collect::<Vec<_>>()
                        .into_iter()
                        .rev()
                        .collect::<Vec<_>>()
                        .join("\n")
                ),
            ));
        }
    }
    fs::copy(directory.join("target/release/omnigraph"), &destination)
        .map_err(|e| io("built binary", e))?;
    store.put_build(key, &destination, true).await.map_err(at)?;
    Ok(destination)
}

/// One record of a store in the series `report` prints.
#[derive(Debug, Serialize)]
struct ReportRow {
    case_id: String,
    point_id: String,
    point_name: String,
    machine: String,
    invocation_id: String,
    invoked_at_unix_ms: u64,
    sut: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    commit: Option<String>,
    version: String,
    status: String,
    requested_repetitions: u32,
    observed_repetitions: u32,
    min_us: u64,
    p50_us: u64,
    max_us: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    p95_us: Option<u64>,
    record: String,
}

#[derive(Debug, Serialize)]
struct ReportGroup {
    case_id: String,
    point_id: String,
    point_name: String,
    machine: String,
    rows: Vec<ReportRow>,
}

#[derive(Debug, Serialize)]
struct ReportOutput {
    runner_output_version: u32,
    store: String,
    scenarios: Vec<String>,
    groups: Vec<ReportGroup>,
}

/// `report <scenario|suite> --store <uri>`: the store's records of the
/// selection, one row per record, grouped by point and machine; never a
/// factor across groups.
async fn run_report(
    selector: Option<String>,
    config: Option<PathBuf>,
    catalog: Vec<PathBuf>,
    store: String,
    json: bool,
) -> ExitCode {
    let catalog = match bench_cli::load_for(
        &bench_cli::CatalogArgs {
            config,
            catalog,
            json,
        },
        selector.as_deref(),
    ) {
        Ok(catalog) => catalog,
        Err(diagnostics) => return bench_cli::failure(diagnostics, json),
    };
    let scenarios = match catalog.selection(selector.as_deref()) {
        Ok(ids) => ids,
        Err(diagnostics) => return bench_cli::failure(diagnostics, json),
    };
    let fixture_store = match omnigraph_bench::store::FixtureStore::open(&store) {
        Ok(store) => store,
        Err(error) => {
            return print_cli_failure(Diagnostic::error(&error.code, "store", error.message), json);
        }
    };
    let uris = match fixture_store.list_records().await {
        Ok(uris) => uris,
        Err(error) => {
            return print_cli_failure(Diagnostic::error(&error.code, "store", error.message), json);
        }
    };
    let mut rows = Vec::new();
    for uri in uris {
        let bytes = match fixture_store.read_record(&uri).await {
            Ok(bytes) => bytes,
            Err(error) => {
                return print_cli_failure(
                    Diagnostic::error(&error.code, "store", error.message),
                    json,
                );
            }
        };
        let record: omnigraph_bench::gqt_record::AnyRunRecordV1 =
            match serde_json::from_slice(&bytes) {
                Ok(record) => record,
                Err(error) => {
                    return print_cli_failure(
                        Diagnostic::error("record_invalid", "store", format!("{uri}: {error}")),
                        json,
                    );
                }
            };
        let omnigraph_bench::gqt_record::AnyRunRecordV1::Gqt(record) = record else {
            continue;
        };
        if !scenarios.contains(&record.run.case_id) {
            continue;
        }
        rows.push(report_row(&record, uri));
    }
    rows.sort_by(|a, b| {
        (&a.case_id, &a.point_id, &a.machine, a.invoked_at_unix_ms).cmp(&(
            &b.case_id,
            &b.point_id,
            &b.machine,
            b.invoked_at_unix_ms,
        ))
    });
    let mut groups: Vec<ReportGroup> = Vec::new();
    for row in rows {
        match groups.last_mut() {
            Some(group)
                if group.case_id == row.case_id
                    && group.point_id == row.point_id
                    && group.machine == row.machine =>
            {
                group.rows.push(row)
            }
            _ => groups.push(ReportGroup {
                case_id: row.case_id.clone(),
                point_id: row.point_id.clone(),
                point_name: row.point_name.clone(),
                machine: row.machine.clone(),
                rows: vec![row],
            }),
        }
    }
    let output = ReportOutput {
        runner_output_version: RUNNER_OUTPUT_VERSION,
        store,
        scenarios,
        groups,
    };
    if json {
        return print_json_success(&output);
    }
    for group in &output.groups {
        println!(
            "## {} point={} machine={}",
            group.case_id, group.point_name, group.machine
        );
        println!("invoked_at_unix_ms\tcommit\tsut\tversion\tn\tmin_us\tp50_us\tmax_us\tstatus");
        for row in &group.rows {
            println!(
                "{}\t{}\t{}\t{}\t{}/{}\t{}\t{}\t{}\t{}",
                row.invoked_at_unix_ms,
                row.commit
                    .as_deref()
                    .map(|c| &c[..c.len().min(12)])
                    .unwrap_or("-"),
                row.sut,
                row.version,
                row.observed_repetitions,
                row.requested_repetitions,
                row.min_us,
                row.p50_us,
                row.max_us,
                row.status
            );
        }
    }
    if output.groups.is_empty() {
        println!(
            "no records of {:?} under {}",
            output.scenarios, output.store
        );
    }
    ExitCode::SUCCESS
}

fn report_row(record: &omnigraph_bench::gqt_record::GqtRunRecordV1, uri: String) -> ReportRow {
    use omnigraph_bench::gqt_record::GqtSutIdentityV1;
    let (sut, commit, version, declared_machine) = match &record.sut {
        GqtSutIdentityV1::Embedded(sut) => (
            "embedded",
            Some(sut.source_commit.clone()),
            sut.package_version.clone(),
            None,
        ),
        GqtSutIdentityV1::Served(sut) => (
            "declared-deployment",
            Some(sut.receipt.server.source_commit.clone()),
            sut.receipt.server.package_version.clone(),
            sut.receipt
                .machine
                .as_ref()
                .map(|machine| machine.machine_label.clone()),
        ),
        GqtSutIdentityV1::Product(sut) => (
            "product-binary",
            sut.source_commit
                .clone()
                .or_else(|| sut.requested_commit.clone()),
            sut.package_version.clone(),
            None,
        ),
    };
    let wall = &record.measurements.wall_clock;
    ReportRow {
        case_id: record.run.case_id.clone(),
        point_id: record.run.point_id.clone(),
        point_name: record.run.point_name.clone(),
        machine: record
            .machine
            .as_ref()
            .map(|machine| machine.machine_label.clone())
            .or(declared_machine)
            .unwrap_or_else(|| "declared".into()),
        invocation_id: record.invocation.invocation_id.clone(),
        invoked_at_unix_ms: record.invocation.invoked_at_unix_ms,
        sut,
        commit,
        version,
        status: serde_json::to_value(record.acquisition.status)
            .ok()
            .and_then(|value| value.as_str().map(str::to_string))
            .unwrap_or_default(),
        requested_repetitions: record.acquisition.requested_repetitions,
        observed_repetitions: record.acquisition.observed_repetitions,
        min_us: wall.min_us,
        p50_us: wall.p50_us,
        max_us: wall.max_us,
        p95_us: wall.p95_us,
        record: uri,
    }
}

#[derive(Debug, Serialize)]
struct SuiteRunOutput {
    runner_output_version: u32,
    suite: String,
    #[serde(serialize_with = "serialize_path_buf_lossy")]
    suite_path: PathBuf,
    /// Present when the run did not take the direct route to its fixture.
    #[serde(skip_serializing_if = "Option::is_none")]
    local_route: Option<&'static str>,
    completed_run_count: usize,
    /// Present only for diagnostic, non-archive execution. Durable archive
    /// records already own the complete raw repetitions.
    #[serde(skip_serializing_if = "Option::is_none")]
    runs: Option<Vec<RunExecution>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    durable_archive: Option<DurableArchiveOutput>,
    /// Present for a `--store` run: the records put into the store.
    #[serde(skip_serializing_if = "Option::is_none")]
    store_records: Option<StoreRecordsOutput>,
}

#[derive(Debug, Serialize)]
struct DurableArchiveOutput {
    archive_format_version: u32,
    #[serde(serialize_with = "serialize_path_buf_lossy")]
    archive_root: PathBuf,
    session_id: String,
    records: Vec<ArchiveReceiptV1>,
}

fn print_execution(output: &SuiteRunOutput) -> ExitCode {
    println!(
        "completed suite {} ({} run{})",
        output.suite,
        output.completed_run_count,
        if output.completed_run_count == 1 {
            ""
        } else {
            "s"
        }
    );
    if let Some(runs) = &output.runs {
        for run in runs {
            print_run_execution(run);
        }
    }
    if let Some(archive) = &output.durable_archive {
        println!(
            "published {} immutable run record{} to {} (session {})",
            archive.records.len(),
            if archive.records.len() == 1 { "" } else { "s" },
            archive.archive_root.display(),
            archive.session_id
        );
        for record in &archive.records {
            println!(
                "  invocation={} sha256={} object={}",
                record.invocation_id, record.record_sha256, record.object_relative_path
            );
        }
    }
    if let Some(store) = &output.store_records {
        println!(
            "put {} run record{} into {} (session {})",
            store.records.len(),
            if store.records.len() == 1 { "" } else { "s" },
            store.store,
            store.session_id
        );
        for record in &store.records {
            println!("  sha256={} object={}", record.record_sha256, record.uri);
        }
    }
    if output.durable_archive.is_none() {
        println!("diagnostic output only; no durable benchmark record was written");
    }
    ExitCode::SUCCESS
}

fn print_run_execution(run: &RunExecution) {
    println!(
        "{} samples={} p50={}us point={} dataset={} cache_hit={}",
        run.case_id,
        run.samples.len(),
        run.wall_clock.p50_us,
        run.point_id,
        run.fixture
            .as_ref()
            .map(|f| f.handoff.summary.logical_content_sha256.as_str())
            .or_else(|| run
                .server_receipt
                .as_ref()
                .map(|r| r.dataset.logical_content_sha256.as_str()))
            .or_else(|| run
                .base
                .as_ref()
                .map(|b| b.summary.logical_content_sha256.as_str()))
            .unwrap_or("absent"),
        run.dataset_cache_hit
            .map(|hit| hit.to_string())
            .unwrap_or_else(|| "not-applicable".into())
    );
}

#[derive(Debug, Serialize)]
struct RunnerFailure<'a> {
    ok: bool,
    runner_output_version: u32,
    suite: &'a str,
    #[serde(serialize_with = "serialize_path_ref_lossy")]
    suite_path: &'a Path,
    completed_run_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    completed_runs: Option<&'a [RunExecution]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    archive_session_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    archive_root: Option<String>,
    #[serde(skip_serializing_if = "<[ArchiveReceiptV1]>::is_empty")]
    published_records: &'a [ArchiveReceiptV1],
    error: &'a RunnerError,
}

fn print_runner_failure(
    suite: &ResolvedSuite,
    completed_run_count: usize,
    completed_runs: Option<&[RunExecution]>,
    published_records: &[ArchiveReceiptV1],
    recording: Option<&RecordingContext>,
    error: &RunnerError,
    json: bool,
) -> ExitCode {
    let failure = RunnerFailure {
        ok: false,
        runner_output_version: RUNNER_OUTPUT_VERSION,
        suite: &suite.definition.name,
        suite_path: &suite.suite_path,
        completed_run_count,
        completed_runs,
        archive_session_id: recording.map(|context| context.session_id_string.as_str()),
        archive_root: recording.map(|context| context.archive_root.to_string_lossy().into_owned()),
        published_records,
        error,
    };
    if json {
        let _ = print_json_success(&failure);
    } else {
        eprintln!(
            "error[{}] after {} completed suite run(s), {} published record(s): {}",
            error.code,
            completed_run_count,
            published_records.len(),
            error.message
        );
        eprintln!("complete recovery JSON envelope follows:");
        match serde_json::to_string_pretty(&failure) {
            Ok(json) => eprintln!("{json}"),
            Err(serialization_error) => eprintln!(
                "could not serialize runner-failure recovery evidence: {serialization_error}"
            ),
        }
    }
    ExitCode::FAILURE
}

#[derive(Debug)]
struct RecordingContext {
    archive_root: PathBuf,
    session_id: Ulid,
    session_id_string: String,
    source_commit: String,
}

impl RecordingContext {
    fn new(archive_root: PathBuf) -> Result<Self, RecordingError> {
        omnigraph_bench::runner::validate_durable_recording_process()
            .map_err(RecordingError::from_runner)?;
        let source_commit = env!("OMNIGRAPH_BENCH_SOURCE_GIT_COMMIT").to_string();
        if !valid_source_commit(&source_commit) {
            return Err(RecordingError::new(
                "recording_source_commit_unavailable",
                "this benchmark build does not carry a complete lowercase source commit",
            ));
        }
        match env!("OMNIGRAPH_BENCH_SOURCE_WORKTREE_DIRTY") {
            "false" => {}
            "true" => {
                return Err(RecordingError::new(
                    "recording_dirty_source_tree",
                    "durable records require clean source-commit provenance; the exact executable remains identified by its digest and attested build facts",
                ));
            }
            value => {
                return Err(RecordingError::new(
                    "recording_source_state_unavailable",
                    format!("build-time source state is {value:?}, expected true or false"),
                ));
            }
        }
        omnigraph_bench::source_provenance::verify_compiled_source_checkout(&source_commit)
            .map_err(|message| {
                RecordingError::new("recording_source_revalidation_failed", message)
            })?;
        // Eligibility and source provenance are pure checks. Only after both
        // succeed may archive preflight create or synchronize directories.
        preflight_archive_publication(&archive_root).map_err(RecordingError::from_archive)?;
        let session_id = Ulid::new();
        let session_id_string = session_id.to_string();
        Ok(Self {
            archive_root,
            session_id,
            session_id_string,
            source_commit,
        })
    }

    fn begin_invocation(&self) -> InvocationIdentityV1 {
        let candidate = Ulid::new();
        let mut invocation = if candidate.timestamp_ms() < self.session_id.timestamp_ms() {
            Ulid::from_parts(self.session_id.timestamp_ms(), candidate.random())
        } else {
            candidate
        };
        if invocation == self.session_id {
            invocation = Ulid::from_parts(invocation.timestamp_ms(), invocation.random() ^ 1);
        }
        InvocationIdentityV1 {
            invocation_id: invocation.to_string(),
            session_id: self.session_id_string.clone(),
            invoked_at_unix_ms: invocation.timestamp_ms(),
        }
    }

    fn publish(
        &self,
        _run: &ResolvedRun,
        execution: &RunExecution,
        invocation: InvocationIdentityV1,
    ) -> Result<ArchiveReceiptV1, RecordingError> {
        self.validate_worker_source(execution)?;
        let record = omnigraph_bench::gqt_record::build(execution, invocation, None)
            .map_err(RecordingError::from_record)?;
        publish_record(&self.archive_root, &record).map_err(RecordingError::from_archive)
    }
    fn publish_censored(
        &self,
        _run: &ResolvedRun,
        execution: &RunExecution,
        invocation: InvocationIdentityV1,
        terminal: AcquisitionTerminalV1,
    ) -> Result<ArchiveReceiptV1, RecordingError> {
        self.validate_worker_source(execution)?;
        let record = omnigraph_bench::gqt_record::build(execution, invocation, Some(terminal))
            .map_err(RecordingError::from_record)?;
        publish_record(&self.archive_root, &record).map_err(RecordingError::from_archive)
    }

    fn validate_worker_source(&self, execution: &RunExecution) -> Result<(), RecordingError> {
        // A long-running suite can outlive a checkout change. Revalidate at
        // the publication boundary, then tie that checked checkout explicitly
        // to the separately attested measured worker.
        omnigraph_bench::source_provenance::verify_compiled_source_checkout(&self.source_commit)
            .map_err(|message| {
                RecordingError::new("recording_source_revalidation_failed", message)
            })?;
        validate_recording_worker_source(
            &self.source_commit,
            &execution.build.source_commit,
            execution.build.source_tree_dirty,
        )
    }
}

fn validate_recording_worker_source(
    revalidated_source_commit: &str,
    worker_source_commit: &str,
    worker_source_tree_dirty: bool,
) -> Result<(), RecordingError> {
    if worker_source_tree_dirty || worker_source_commit != revalidated_source_commit {
        return Err(RecordingError::new(
            "recording_worker_source_mismatch",
            "measured worker source provenance does not match the clean checkout revalidated by the recording process",
        ));
    }
    Ok(())
}

#[derive(Debug, Serialize)]
struct RecordingError {
    code: String,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    possibly_published: Option<Box<ArchivePublicationUnknownV1>>,
    /// Structured acquisition failure retained when publishing its verified
    /// censored prefix fails as a second, independent operation.
    #[serde(skip_serializing_if = "Option::is_none")]
    acquisition_failure: Option<Box<RunnerError>>,
}

impl RecordingError {
    fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            possibly_published: None,
            acquisition_failure: None,
        }
    }

    fn from_record(error: omnigraph_bench::record::RecordError) -> Self {
        Self::new(error.code, error.to_string())
    }

    fn from_runner(error: RunnerError) -> Self {
        Self::new(error.code, error.message)
    }

    fn from_archive(error: ArchiveError) -> Self {
        Self {
            code: error.code.to_string(),
            message: error.to_string(),
            possibly_published: error.possibly_published,
            acquisition_failure: None,
        }
    }

    fn with_acquisition_failure(mut self, acquisition: &RunnerError) -> Self {
        self.message = format!(
            "benchmark acquisition failed with {}; its verified prefix could not be archived: {}",
            acquisition.code, self.message
        );
        let mut diagnostic = acquisition.clone();
        // `unpublished_run` is the sole recovery copy of the verified prefix.
        // Keep the failed repetition and containment diagnostics here, but do
        // not duplicate raw completed samples or suite runs in the nested
        // acquisition error.
        diagnostic.context.clear_completed_prefix();
        self.acquisition_failure = Some(Box::new(diagnostic));
        self
    }
}

#[derive(Debug, Serialize)]
struct RecordingFailure<'a> {
    ok: bool,
    runner_output_version: u32,
    suite: &'a str,
    #[serde(serialize_with = "serialize_path_ref_lossy")]
    suite_path: &'a Path,
    completed_run_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    unpublished_run: Option<&'a RunExecution>,
    #[serde(skip_serializing_if = "Option::is_none")]
    archive_session_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    archive_root: Option<String>,
    #[serde(skip_serializing_if = "<[ArchiveReceiptV1]>::is_empty")]
    published_records: &'a [ArchiveReceiptV1],
    error: &'a RecordingError,
}

fn print_recording_failure(
    suite: &ResolvedSuite,
    completed_run_count: usize,
    unpublished_run: Option<&RunExecution>,
    published_records: &[ArchiveReceiptV1],
    recording: Option<&RecordingContext>,
    archive_root: Option<&Path>,
    error: &RecordingError,
    json: bool,
) -> ExitCode {
    let failure = RecordingFailure {
        ok: false,
        runner_output_version: RUNNER_OUTPUT_VERSION,
        suite: &suite.definition.name,
        suite_path: &suite.suite_path,
        completed_run_count,
        unpublished_run,
        archive_session_id: recording.map(|context| context.session_id_string.as_str()),
        archive_root: archive_root.map(|root| root.to_string_lossy().into_owned()),
        published_records,
        error,
    };
    if json {
        let _ = print_json_success(&failure);
    } else {
        eprintln!(
            "error[{}] after {} completed suite run(s), {} published record(s): {}",
            error.code,
            completed_run_count,
            published_records.len(),
            error.message
        );
        eprintln!("complete recovery JSON envelope follows:");
        match serde_json::to_string_pretty(&failure) {
            Ok(json) => eprintln!("{json}"),
            Err(serialization_error) => eprintln!(
                "could not serialize recording-failure recovery evidence: {serialization_error}"
            ),
        }
    }
    ExitCode::FAILURE
}

fn valid_source_commit(value: &str) -> bool {
    matches!(value.len(), 40 | 64)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn print_json<T: Serialize>(outcome: &ValidationOutcome<T>) -> ExitCode {
    let success = outcome.ok;
    let code = print_json_success(outcome);
    if success { code } else { ExitCode::FAILURE }
}

fn print_json_success<T: Serialize>(value: &T) -> ExitCode {
    match serde_json::to_string_pretty(value) {
        Ok(json) => {
            println!("{json}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("could not serialize JSON output: {error}");
            ExitCode::FAILURE
        }
    }
}

fn serialize_path_buf_lossy<S>(path: &Path, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&path.to_string_lossy())
}

fn serialize_path_ref_lossy<S>(path: &&Path, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&path.to_string_lossy())
}

fn print_cli_failure(diagnostic: Diagnostic, json: bool) -> ExitCode {
    print_cli_failures(vec![diagnostic], json)
}

fn print_cli_failures(diagnostics: Vec<Diagnostic>, json: bool) -> ExitCode {
    if json {
        print_json(&ValidationOutcome::<()>::failure(diagnostics))
    } else {
        print_diagnostics(&diagnostics)
    }
}

fn print_diagnostics(diagnostics: &[Diagnostic]) -> ExitCode {
    for diagnostic in diagnostics {
        eprintln!(
            "error[{}] {}: {}",
            diagnostic.code, diagnostic.path, diagnostic.message
        );
    }
    ExitCode::FAILURE
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn dataset_and_pair_cli_admit_raw_files_and_refuse_ignored_suite_overrides() {
        assert!(
            Cli::try_parse_from([
                "bench",
                "dataset",
                "build",
                "dataset.gqt",
                "--queries",
                "queries.gqt",
                "--dataset-cache",
                "/cache"
            ])
            .is_ok()
        );
        assert!(
            Cli::try_parse_from([
                "bench",
                "suite",
                "run",
                "--dataset",
                "dataset.gqt",
                "--queries",
                "queries.gqt",
                "--measured-step",
                "1",
                "--measured-text",
                "--- restart"
            ])
            .is_ok()
        );
        for flag in ["--repetitions", "--deadline-seconds", "--filesystem"] {
            let value = if flag == "--filesystem" { "xfs" } else { "2" };
            assert!(
                Cli::try_parse_from([
                    "bench",
                    "suite",
                    "run",
                    "catalog/suites/local-fast",
                    flag,
                    value
                ])
                .is_err()
            );
        }
    }

    fn assert_json_object_keys(value: &serde_json::Value, expected: &[&str]) {
        let actual = value
            .as_object()
            .expect("JSON object")
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        let expected = expected.iter().copied().collect::<BTreeSet<_>>();
        assert_eq!(actual, expected);
    }

    #[cfg(debug_assertions)]
    #[test]
    fn refused_recording_does_not_create_the_archive_root() {
        let holder = tempfile::tempdir().unwrap();
        let archive = holder.path().join("not-created");
        let error = RecordingContext::new(archive.clone()).unwrap_err();
        assert_eq!(error.code, "release_build_required");
        assert!(!archive.exists());
    }

    #[test]
    fn recording_error_preserves_unknown_publication_identity() {
        let unknown = ArchivePublicationUnknownV1 {
            archive_format_version: 1,
            invocation_id: "01K00000000000000000000000".to_string(),
            record_sha256: "a".repeat(64),
            object_relative_path: format!("objects/sha256/{}.json", "a".repeat(64)),
            pointer_relative_path: "invocations/01K00000000000000000000000.json".to_string(),
        };
        let error = RecordingError::from_archive(ArchiveError {
            code: "archive_pointer_publication_unknown",
            path: Some(PathBuf::from("archive/invocations/candidate.json")),
            message: "directory durability could not be proved".to_string(),
            possibly_published: Some(Box::new(unknown.clone())),
        });

        assert_eq!(error.possibly_published, Some(Box::new(unknown)));
        let encoded = serde_json::to_value(error).expect("recording error JSON");
        assert_eq!(
            encoded["possibly_published"]["invocation_id"],
            "01K00000000000000000000000"
        );
    }

    #[test]
    fn durable_recording_requires_worker_source_to_match_revalidated_checkout() {
        let commit = "a".repeat(40);
        validate_recording_worker_source(&commit, &commit, false).unwrap();

        assert_eq!(
            validate_recording_worker_source(&commit, &"b".repeat(40), false)
                .unwrap_err()
                .code,
            "recording_worker_source_mismatch"
        );
        assert_eq!(
            validate_recording_worker_source(&commit, &commit, true)
                .unwrap_err()
                .code,
            "recording_worker_source_mismatch"
        );
    }

    #[test]
    fn human_recovery_envelope_can_carry_reconciliation_identity() {
        let unknown = ArchivePublicationUnknownV1 {
            archive_format_version: 1,
            invocation_id: "01K00000000000000000000000".to_string(),
            record_sha256: "a".repeat(64),
            object_relative_path: format!("objects/sha256/{}.json", "a".repeat(64)),
            pointer_relative_path: "invocations/01K00000000000000000000000.json".to_string(),
        };
        let error = RecordingError {
            code: "archive_pointer_publication_unknown".to_string(),
            message: "directory durability could not be proved".to_string(),
            possibly_published: Some(Box::new(unknown)),
            acquisition_failure: None,
        };
        let failure = RecordingFailure {
            ok: false,
            runner_output_version: RUNNER_OUTPUT_VERSION,
            suite: "suite",
            suite_path: Path::new("suite.yaml"),
            completed_run_count: 1,
            unpublished_run: None,
            archive_session_id: Some("01K00000000000000000000001"),
            archive_root: Some("archive".to_string()),
            published_records: &[],
            error: &error,
        };

        let encoded = serde_json::to_value(failure).expect("complete recovery envelope JSON");
        assert_eq!(
            encoded["error"]["possibly_published"]["invocation_id"],
            "01K00000000000000000000000"
        );
        assert_eq!(encoded["archive_root"], "archive");
    }

    #[test]
    fn double_failure_preserves_recording_and_acquisition_errors_structurally() {
        use omnigraph_bench::counting::LogicalCallCounts;
        use omnigraph_bench::runner::{
            ControlCallObservation, LogicalStoreCallObservation, MergeRouteObservation,
            RepObservation, VerificationObservation,
        };

        let unknown = ArchivePublicationUnknownV1 {
            archive_format_version: 1,
            invocation_id: "01K00000000000000000000000".to_string(),
            record_sha256: "a".repeat(64),
            object_relative_path: format!("objects/sha256/{}.json", "a".repeat(64)),
            pointer_relative_path: "invocations/01K00000000000000000000000.json".to_string(),
        };
        let mut acquisition = RunnerError {
            code: "verification_failed".to_string(),
            message: "rep 1 verification failed".to_string(),
            context: Box::default(),
        };
        acquisition.context.repetition = Some(1);
        acquisition.context.completed_samples.push(RepObservation {
            repetition: 0,
            input_physical_digest_sha256: "d".repeat(64),
            elapsed_us: 1,
            peak_rss_bytes: Some(1),
            outcome: "merged".to_string(),
            phases: Vec::new(),
            route: MergeRouteObservation {
                table_walk_intervals: 1,
                stage_merge_insert_calls: 0,
                stage_merge_insert_rows: 0,
                stage_known_present_update_calls: 0,
                stage_known_present_update_rows: 0,
                stage_fenced_insert_calls: 0,
                stage_fenced_insert_rows: 0,
                strict_insert_preflight_calls: 0,
            },
            logical_store_calls: LogicalStoreCallObservation {
                manifest: LogicalCallCounts::default(),
                table: LogicalCallCounts::default(),
                physical_attempts_observed: false,
            },
            control_store_calls: ControlCallObservation {
                read_text: 0,
                read_text_if_exists: 0,
                read_text_versioned: 0,
                exists: 0,
                list_dir: 0,
                mutation_calls: 0,
                write_text: 0,
                delete: 0,
            },
            verification: VerificationObservation {
                branch: "main".to_string(),
                tables: 2,
                rows: 1,
                exact_content: true,
                source_exact_content: true,
                main_exact_content: true,
                protected_heads_unchanged: true,
            },
        });
        acquisition.context.child_process = Some(omnigraph_bench::runner::ChildProcessEvidence {
            stage: "verify".to_string(),
            stderr_tail: "bounded worker detail".to_string(),
            direct_child_reaped: true,
            process_group_gone: true,
            stdio_closed_cleanly: true,
            ..Default::default()
        });
        let recording = RecordingError::from_archive(ArchiveError {
            code: "archive_pointer_publication_unknown",
            path: Some(PathBuf::from("archive/invocations/candidate.json")),
            message: "could not prove censored-prefix publication".to_string(),
            possibly_published: Some(Box::new(unknown)),
        })
        .with_acquisition_failure(&acquisition);
        let failure = RecordingFailure {
            ok: false,
            runner_output_version: RUNNER_OUTPUT_VERSION,
            suite: "suite",
            suite_path: Path::new("suite.yaml"),
            completed_run_count: 0,
            unpublished_run: None,
            archive_session_id: Some("01K00000000000000000000000"),
            archive_root: Some("archive".to_string()),
            published_records: &[],
            error: &recording,
        };

        let encoded = serde_json::to_value(failure).expect("double-failure recovery JSON");
        assert_eq!(
            encoded["error"]["code"],
            "archive_pointer_publication_unknown"
        );
        assert_eq!(
            encoded["error"]["possibly_published"]["invocation_id"],
            "01K00000000000000000000000"
        );
        assert_eq!(
            encoded["error"]["acquisition_failure"]["code"],
            "verification_failed"
        );
        assert_eq!(encoded["error"]["acquisition_failure"]["repetition"], 1);
        assert_eq!(
            encoded["error"]["acquisition_failure"]["child_process"]["stage"],
            "verify"
        );
        assert_eq!(
            encoded["error"]["acquisition_failure"]["child_process"]["stderr_tail"],
            "bounded worker detail"
        );
        assert!(
            encoded["error"]["acquisition_failure"]
                .get("completed_samples")
                .is_none(),
            "the unpublished prefix must not be duplicated in the nested diagnostic"
        );
    }

    #[test]
    fn archive_mode_summary_does_not_duplicate_raw_runs() {
        let output = SuiteRunOutput {
            runner_output_version: RUNNER_OUTPUT_VERSION,
            suite: "suite".to_string(),
            suite_path: PathBuf::from("suite.yaml"),
            local_route: None,
            completed_run_count: 1,
            runs: None,
            store_records: None,
            durable_archive: Some(DurableArchiveOutput {
                archive_format_version: 1,
                archive_root: PathBuf::from("archive"),
                session_id: "01K00000000000000000000000".to_string(),
                records: vec![ArchiveReceiptV1 {
                    archive_format_version: 1,
                    invocation_id: "01K00000000000000000000001".to_string(),
                    record_sha256: "b".repeat(64),
                    object_relative_path: format!("objects/sha256/{}.json", "b".repeat(64)),
                    pointer_relative_path: "invocations/01K00000000000000000000001.json"
                        .to_string(),
                    newly_published: true,
                }],
            }),
        };

        let encoded = serde_json::to_value(output).expect("suite output JSON");
        assert_eq!(encoded["runner_output_version"], RUNNER_OUTPUT_VERSION);
        assert_json_object_keys(
            &encoded,
            &[
                "completed_run_count",
                "durable_archive",
                "runner_output_version",
                "suite",
                "suite_path",
            ],
        );
        assert_json_object_keys(
            &encoded["durable_archive"],
            &[
                "archive_format_version",
                "archive_root",
                "records",
                "session_id",
            ],
        );
        assert_eq!(encoded["completed_run_count"], 1);
        assert!(encoded.get("runs").is_none());
        assert_eq!(
            encoded["durable_archive"]["records"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn runner_failure_shape_is_bound_to_output_version() {
        let error = RunnerError {
            code: "worker_failed".to_string(),
            message: "worker failed".to_string(),
            context: Box::default(),
        };
        let records = [ArchiveReceiptV1 {
            archive_format_version: 1,
            invocation_id: "01K00000000000000000000001".to_string(),
            record_sha256: "b".repeat(64),
            object_relative_path: format!("objects/sha256/{}.json", "b".repeat(64)),
            pointer_relative_path: "invocations/01K00000000000000000000001.json".to_string(),
            newly_published: true,
        }];
        let failure = RunnerFailure {
            ok: false,
            runner_output_version: RUNNER_OUTPUT_VERSION,
            suite: "suite",
            suite_path: Path::new("suite.yaml"),
            completed_run_count: 1,
            completed_runs: Some(&[]),
            archive_session_id: Some("01K00000000000000000000000"),
            archive_root: Some("archive".to_string()),
            published_records: &records,
            error: &error,
        };

        let encoded = serde_json::to_value(failure).expect("runner failure JSON");
        assert_eq!(encoded["runner_output_version"], RUNNER_OUTPUT_VERSION);
        assert_json_object_keys(
            &encoded,
            &[
                "archive_root",
                "archive_session_id",
                "completed_run_count",
                "completed_runs",
                "error",
                "ok",
                "published_records",
                "runner_output_version",
                "suite",
                "suite_path",
            ],
        );
        assert_json_object_keys(&encoded["error"], &["code", "message"]);
    }

    #[test]
    fn censored_prefix_uses_only_completed_repetitions_and_omits_rep_zero() {
        let settled_but_unverified = "settled-rep-1".to_string();
        let completed = vec!["verified-rep-0".to_string()];
        let (prefix, terminal) = classify_censored_prefix(
            Some(completed.clone()),
            Vec::len,
            Some("Verify"),
            "verification_failed",
        )
        .unwrap()
        .expect("one verified repetition is a censored prefix");

        assert_eq!(prefix, completed);
        assert!(!prefix.contains(&settled_but_unverified));
        assert_eq!(terminal.failed_repetition, 1);
        assert_eq!(terminal.stage, AcquisitionTerminalStageV1::Verify);
        assert_eq!(terminal.code, "verification_failed");
        assert!(
            classify_censored_prefix(
                Some(Vec::<String>::new()),
                Vec::len,
                Some("Measure"),
                "merge_failed",
            )
            .unwrap()
            .is_none()
        );
        assert!(
            classify_censored_prefix::<Vec<String>>(None, Vec::len, None, "worker_failed",)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn censored_terminal_conversion_accepts_only_emitted_runner_stages() {
        use AcquisitionTerminalStageV1 as Stage;

        let emitted = [
            (None, Stage::Runner),
            (Some("supervisor-panic"), Stage::SupervisorPanic),
            (Some("Bootstrap"), Stage::Bootstrap),
            (Some("Prepare"), Stage::Prepare),
            (Some("Measure"), Stage::Measure),
            (Some("Verify"), Stage::Verify),
            (Some("Finalize"), Stage::Finalize),
            (Some("Protocol"), Stage::Protocol),
            (Some("pipe-setup"), Stage::PipeSetup),
            (Some("writer-setup"), Stage::WriterSetup),
            (Some("reader-setup"), Stage::ReaderSetup),
            (Some("request-write"), Stage::RequestWrite),
            (Some("prepare-timeout"), Stage::PrepareTimeout),
            (Some("prepare-protocol"), Stage::PrepareProtocol),
            (Some("begin-write"), Stage::BeginWrite),
            (Some("measure-timeout"), Stage::MeasureTimeout),
            (Some("measure-protocol"), Stage::MeasureProtocol),
            (Some("verify-timeout"), Stage::VerifyTimeout),
            (Some("verify-protocol"), Stage::VerifyProtocol),
            (Some("finalize-protocol"), Stage::FinalizeProtocol),
            (Some("exit-timeout"), Stage::ExitTimeout),
            (Some("group-proof"), Stage::GroupProof),
            (Some("finalize-exit"), Stage::FinalizeExit),
            (
                Some("structured-failure-reap"),
                Stage::StructuredFailureReap,
            ),
        ];
        for (runner_stage, expected) in emitted {
            assert_eq!(acquisition_terminal_stage(runner_stage).unwrap(), expected);
        }

        for invalid in [Some(""), Some("verification"), Some("arbitrary prose")] {
            assert_eq!(
                acquisition_terminal_stage(invalid).unwrap_err().code,
                "invalid_acquisition_terminal_stage",
            );
        }
    }

    #[test]
    fn censored_terminal_conversion_rejects_noncanonical_error_codes() {
        let error = classify_censored_prefix(
            Some(vec!["verified-rep-0".to_string()]),
            Vec::len,
            Some("Verify"),
            "Verification failed",
        )
        .unwrap_err();

        assert_eq!(error.code, "invalid_acquisition_error_code");
    }
}
