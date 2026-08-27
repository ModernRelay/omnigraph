use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use omnigraph_bench::{
    Diagnostic, PLAN_FORMAT_VERSION, RUNNER_OUTPUT_VERSION, ResolvedRun, ResolvedSuite,
    RunExecution, RunOptions, RunnerError, SuiteExecution, ValidatedCase, ValidationOutcome,
    execute_run, load_case, load_suite,
};
use serde::Serialize;

const MAX_CASE_FILES: usize = 10_000;
const MAX_DIRECTORY_ENTRIES: usize = 100_000;

#[derive(Debug, Parser)]
#[command(
    name = "omnigraph-bench",
    version,
    about = "Validate, plan, and run declarative OmniGraph benchmarks"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Inspect one-case experiment definitions.
    Case {
        #[command(subcommand)]
        command: CaseCommand,
    },
    /// Inspect suites of benchmark cases.
    Suite {
        #[command(subcommand)]
        command: SuiteCommand,
    },
    /// Private one-repetition worker endpoint used by the supervising runner.
    #[command(name = "__worker-v1", hide = true)]
    WorkerV1,
    /// Private bounded fixture-builder endpoint used by the supervising runner.
    #[command(name = "__fixture-worker-v1", hide = true)]
    FixtureWorkerV1 { request: PathBuf, result: PathBuf },
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
    Run {
        file: PathBuf,
        /// Select exactly one case id from the suite.
        #[arg(long)]
        case: Option<String>,
        /// Place disposable fixture trees below this existing directory.
        #[arg(long)]
        scratch_root: Option<PathBuf>,
        /// Emit machine-readable diagnostic execution output.
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Serialize)]
struct CaseSummary<'a> {
    id: &'a str,
    path: &'a Path,
    point_id: &'a str,
    point_name: &'a str,
    case_digest: &'a str,
}

impl<'a> CaseSummary<'a> {
    fn new(path: &'a Path, case: &'a ValidatedCase) -> Self {
        Self {
            id: &case.definition.id,
            path,
            point_id: &case.point_id,
            point_name: &case.point_name,
            case_digest: &case.case_digest,
        }
    }
}

#[derive(Debug, Serialize)]
struct Plan<'a> {
    plan_version: u32,
    suite: &'a str,
    suite_path: &'a Path,
    runs: Vec<PlanRun<'a>>,
}

#[derive(Debug, Serialize)]
struct PlanRun<'a> {
    case_id: &'a str,
    case_path: &'a Path,
    repetitions: u32,
    point_id: &'a str,
    point_name: &'a str,
    case_digest: &'a str,
    identity: &'a omnigraph_bench::PointIdentityV1,
}

#[tokio::main]
async fn main() -> ExitCode {
    match Cli::parse().command {
        Command::Case { command } => run_case(command),
        Command::Suite { command } => run_suite(command).await,
        Command::WorkerV1 => omnigraph_bench::worker::run_worker_stdio_v1().await,
        Command::FixtureWorkerV1 { request, result } => {
            omnigraph_bench::fixture_worker::run_fixture_worker_files_v1(&request, &result).await
        }
    }
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
        SuiteCommand::Run {
            file,
            case,
            scratch_root,
            json,
        } => {
            run_suite_execution(
                &file,
                case.as_deref(),
                RunOptions {
                    scratch_root,
                    worker_executable: std::env::current_exe().ok(),
                },
                json,
            )
            .await
        }
    }
}

fn print_case_validation(file: &Path, outcome: ValidationOutcome<ValidatedCase>) -> ExitCode {
    match outcome.into_result() {
        Ok(case) => {
            println!(
                "valid case {} {} ({})",
                case.definition.id, case.point_id, case.point_name
            );
            ExitCode::SUCCESS
        }
        Err(diagnostics) => {
            eprintln!("invalid case {}", file.display());
            print_diagnostics(&diagnostics)
        }
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
            println!("{} {} {}", case.id, case.point_id, case.path.display());
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
    let mut diagnostics = Vec::new();
    let mut ids: BTreeMap<&str, &Path> = BTreeMap::new();
    let mut points: BTreeMap<&str, (&Path, &omnigraph_bench::PointIdentityV1)> = BTreeMap::new();
    for (path, case) in cases {
        if let Some(first) = ids.insert(&case.definition.id, path) {
            diagnostics.push(Diagnostic::error(
                "duplicate_case_id",
                path.display().to_string(),
                format!(
                    "case id '{}' is already declared by {}",
                    case.definition.id,
                    first.display()
                ),
            ));
        }
        if let Some((first_path, first_identity)) =
            points.insert(&case.point_id, (path, &case.identity))
        {
            let (code, message) = if first_identity == &case.identity {
                (
                    "duplicate_point_id",
                    format!(
                        "point id '{}' is already declared by {}",
                        case.point_id,
                        first_path.display()
                    ),
                )
            } else {
                (
                    "point_id_collision",
                    format!(
                        "point id '{}' has unequal identities in {} and {}",
                        case.point_id,
                        first_path.display(),
                        path.display()
                    ),
                )
            };
            diagnostics.push(Diagnostic::error(code, path.display().to_string(), message));
        }
    }
    diagnostics
}

fn plan_suite(path: &Path, selector: Option<&str>, json: bool) -> ExitCode {
    let suite = match load_suite(path).into_result() {
        Ok(suite) => suite,
        Err(diagnostics) => return print_cli_failures(diagnostics, json),
    };
    let selected = match select_runs(&suite, selector) {
        Ok(selected) => selected,
        Err(diagnostic) => return print_cli_failure(diagnostic, json),
    };
    let plan = Plan {
        plan_version: PLAN_FORMAT_VERSION,
        suite: &suite.definition.name,
        suite_path: &suite.suite_path,
        runs: selected
            .into_iter()
            .map(|run| PlanRun {
                case_id: &run.case.definition.id,
                case_path: &run.case_path,
                repetitions: run.repetitions,
                point_id: &run.case.point_id,
                point_name: &run.case.point_name,
                case_digest: &run.case.case_digest,
                identity: &run.case.identity,
            })
            .collect(),
    };
    if json {
        print_json_success(&plan)
    } else {
        println!("suite {}", plan.suite);
        for run in plan.runs {
            println!(
                "{} repetitions={} point_id={} case={}",
                run.case_id,
                run.repetitions,
                run.point_id,
                run.case_path.display()
            );
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
        .filter(|run| selector.is_none_or(|id| run.case.definition.id == id))
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

async fn run_suite_execution(
    path: &Path,
    selector: Option<&str>,
    options: RunOptions,
    json: bool,
) -> ExitCode {
    let suite = match load_suite(path).into_result() {
        Ok(suite) => suite,
        Err(diagnostics) => return print_cli_failures(diagnostics, json),
    };
    let selected = match select_runs(&suite, selector) {
        Ok(selected) => selected,
        Err(diagnostic) => return print_cli_failure(diagnostic, json),
    };
    let mut runs = Vec::with_capacity(selected.len());
    for run in selected {
        match execute_run(run, &options).await {
            Ok(execution) => runs.push(execution),
            Err(error) => return print_runner_failure(&suite, &runs, &error, json),
        }
    }
    let execution = SuiteExecution {
        runner_output_version: RUNNER_OUTPUT_VERSION,
        suite: suite.definition.name,
        suite_path: suite.suite_path,
        runs,
    };
    if json {
        print_json_success(&execution)
    } else {
        print_execution(&execution)
    }
}

fn print_execution(execution: &SuiteExecution) -> ExitCode {
    println!(
        "completed suite {} ({} run{})",
        execution.suite,
        execution.runs.len(),
        if execution.runs.len() == 1 { "" } else { "s" }
    );
    for run in &execution.runs {
        print_run_execution(run);
    }
    println!("diagnostic output only; no durable benchmark record was written");
    ExitCode::SUCCESS
}

fn print_run_execution(run: &RunExecution) {
    let tail = run.wall_clock.p95_us.map_or_else(
        || "p95=unsupported".to_string(),
        |value| format!("p95={value}us"),
    );
    println!(
        "{} repetitions={} p50={}us min={}us max={}us {} build={}/O{} fixture_sha256={}",
        run.case_id,
        run.wall_clock.observed_repetitions,
        run.wall_clock.p50_us,
        run.wall_clock.min_us,
        run.wall_clock.max_us,
        tail,
        run.build.cargo_profile,
        run.build.opt_level,
        run.fixture.physical_digest_sha256
    );
    let cache_condition = serde_json::to_string(&run.cache_condition)
        .unwrap_or_else(|_| "<serialization-failed>".to_string());
    println!("  cache_condition={cache_condition}");
    for sample in &run.samples {
        println!(
            "  rep={} outcome={} elapsed={}us exact_tables={} exact_rows={}",
            sample.repetition,
            sample.outcome,
            sample.elapsed_us,
            sample.verification.tables,
            sample.verification.rows
        );
    }
}

#[derive(Debug, Serialize)]
struct RunnerFailure<'a> {
    ok: bool,
    runner_output_version: u32,
    suite: &'a str,
    suite_path: &'a Path,
    completed_runs: &'a [RunExecution],
    error: &'a RunnerError,
}

fn print_runner_failure(
    suite: &ResolvedSuite,
    completed_runs: &[RunExecution],
    error: &RunnerError,
    json: bool,
) -> ExitCode {
    if json {
        let _ = print_json_success(&RunnerFailure {
            ok: false,
            runner_output_version: RUNNER_OUTPUT_VERSION,
            suite: &suite.definition.name,
            suite_path: &suite.suite_path,
            completed_runs,
            error,
        });
    } else {
        eprintln!(
            "error[{}] after {} completed suite run(s): {}",
            error.code,
            completed_runs.len(),
            error.message
        );
    }
    ExitCode::FAILURE
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
