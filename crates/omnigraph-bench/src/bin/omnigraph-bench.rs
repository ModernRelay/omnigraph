use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use omnigraph_bench::archive::{
    ArchiveError, ArchivePublicationUnknownV1, ArchiveReceiptV1, ArchiveReconciliationV1,
    iter_archive, preflight_archive_publication, publish_record, reconcile_archive_publication,
};
use omnigraph_bench::case::Backend;
use omnigraph_bench::projection::{
    DEFAULT_PROJECTION_PAGE_SIZE, ProjectionCursorV1, ProjectionError, ProjectionPageV1,
    list_points_page, list_runs_for_point_page, rebuild_projection,
};
use omnigraph_bench::record::{
    InvocationIdentityV1, ObservedBackendV1, RecordInputV1, build_run_record,
    sut_identity_for_execution,
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
        /// Publish complete immutable run records under this archive root.
        #[arg(long)]
        archive: Option<PathBuf>,
        /// Emit machine-readable diagnostic execution output.
        #[arg(long)]
        json: bool,
    },
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
        Command::Archive { command } => run_archive(command),
        Command::Projection { command } => run_projection(command).await,
        Command::WorkerV1 => omnigraph_bench::worker::run_worker_stdio_v1().await,
        Command::FixtureWorkerV1 { request, result } => {
            omnigraph_bench::fixture_worker::run_fixture_worker_files_v1(&request, &result).await
        }
    }
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
            archive,
            json,
        } => {
            run_suite_execution(
                &file,
                case.as_deref(),
                RunOptions {
                    scratch_root,
                    worker_executable: std::env::current_exe().ok(),
                },
                archive,
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
    archive: Option<PathBuf>,
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
    let recording = match archive {
        Some(root) => match RecordingContext::new(root.clone()) {
            Ok(context) => Some(context),
            Err(error) => {
                return print_recording_failure(
                    &suite,
                    0,
                    None,
                    &[],
                    None,
                    Some(&root),
                    &error,
                    json,
                );
            }
        },
        None => None,
    };
    // Raw samples already become durable authority one record at a time. In
    // archive mode, retaining them all again in the CLI result would make
    // memory grow with the complete suite and defeat streaming publication.
    let mut diagnostic_runs = recording
        .is_none()
        .then(|| Vec::with_capacity(selected.len()));
    let mut receipts = Vec::with_capacity(selected.len());
    let mut completed_run_count = 0usize;
    for run in selected {
        let invocation = recording.as_ref().map(RecordingContext::begin_invocation);
        let execution = match execute_run(run, &options).await {
            Ok(execution) => execution,
            Err(error) => {
                return print_runner_failure(
                    &suite,
                    completed_run_count,
                    diagnostic_runs.as_deref(),
                    &receipts,
                    recording.as_ref(),
                    &error,
                    json,
                );
            }
        };
        if let Some(recording) = &recording {
            let invocation = invocation.expect("recording context minted an invocation");
            match recording.publish(run, &execution, invocation) {
                Ok(receipt) => receipts.push(receipt),
                Err(error) => {
                    return print_recording_failure(
                        &suite,
                        completed_run_count.saturating_add(1),
                        Some(&execution),
                        &receipts,
                        Some(recording),
                        Some(&recording.archive_root),
                        &error,
                        json,
                    );
                }
            }
        } else {
            diagnostic_runs
                .as_mut()
                .expect("diagnostic mode retains executions")
                .push(execution);
        }
        completed_run_count = completed_run_count.saturating_add(1);
    }
    let output = SuiteRunOutput {
        runner_output_version: RUNNER_OUTPUT_VERSION,
        suite: suite.definition.name,
        suite_path: suite.suite_path,
        completed_run_count,
        runs: diagnostic_runs,
        durable_archive: recording.map(|recording| DurableArchiveOutput {
            archive_format_version: omnigraph_bench::archive::ARCHIVE_FORMAT_VERSION,
            archive_root: recording.archive_root,
            session_id: recording.session_id.to_string(),
            records: receipts,
        }),
    };
    if json {
        print_json_success(&output)
    } else {
        print_execution(&output)
    }
}

#[derive(Debug, Serialize)]
struct SuiteRunOutput {
    runner_output_version: u32,
    suite: String,
    #[serde(serialize_with = "serialize_path_buf_lossy")]
    suite_path: PathBuf,
    completed_run_count: usize,
    /// Present only for diagnostic, non-archive execution. Durable archive
    /// records already own the complete raw repetitions.
    #[serde(skip_serializing_if = "Option::is_none")]
    runs: Option<Vec<RunExecution>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    durable_archive: Option<DurableArchiveOutput>,
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
    } else {
        println!("diagnostic output only; no durable benchmark record was written");
    }
    ExitCode::SUCCESS
}

fn print_run_execution(run: &RunExecution) {
    let tail = run.wall_clock.p95_us.map_or_else(
        || "p95=unsupported".to_string(),
        |value| format!("p95={value}us"),
    );
    println!(
        "{} repetitions={} p50={}us min={}us max={}us {} build={}/cargo-O{} effective-codegen={} fixture_sha256={}",
        run.case_id,
        run.wall_clock.observed_repetitions,
        run.wall_clock.p50_us,
        run.wall_clock.min_us,
        run.wall_clock.max_us,
        tail,
        run.build.cargo_profile,
        run.build.cargo_opt_level,
        if run.build.effective_codegen_options_proved {
            "proved"
        } else {
            "unproved"
        },
        run.fixture.stamp.manifest.physical.tree_sha256
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
}

impl RecordingContext {
    fn new(archive_root: PathBuf) -> Result<Self, RecordingError> {
        preflight_archive_publication(&archive_root).map_err(RecordingError::from_archive)?;
        let source_commit = env!("OMNIGRAPH_BENCH_SOURCE_COMMIT").to_string();
        if !valid_source_commit(&source_commit) {
            return Err(RecordingError::new(
                "recording_source_commit_unavailable",
                "this benchmark build does not carry a complete lowercase source commit",
            ));
        }
        match env!("OMNIGRAPH_BENCH_SOURCE_DIRTY") {
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
        let session_id = Ulid::new();
        let session_id_string = session_id.to_string();
        Ok(Self {
            archive_root,
            session_id,
            session_id_string,
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
        run: &ResolvedRun,
        execution: &RunExecution,
        invocation: InvocationIdentityV1,
    ) -> Result<ArchiveReceiptV1, RecordingError> {
        let fixture = execution.fixture.stamp.clone();
        let backend = match &run.case.definition.environment.backend {
            Backend::LocalFs {
                filesystem,
                storage_class,
            } => ObservedBackendV1::LocalFs {
                filesystem: *filesystem,
                storage_class: *storage_class,
                storage_protocol: execution.environment.storage_protocol.clone(),
                probe: execution.environment.probe.to_string(),
            },
            Backend::S3 { .. } => {
                return Err(RecordingError::new(
                    "recording_backend_unsupported",
                    "the local runner cannot produce observed S3 backend identity",
                ));
            }
        };
        let record = build_run_record(
            run,
            execution,
            RecordInputV1 {
                invocation,
                sut: sut_identity_for_execution(execution).map_err(RecordingError::from_record)?,
                backend,
                fixture,
            },
        )
        .map_err(RecordingError::from_record)?;
        publish_record(&self.archive_root, &record).map_err(RecordingError::from_archive)
    }
}

#[derive(Debug, Serialize)]
struct RecordingError {
    code: String,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    possibly_published: Option<Box<ArchivePublicationUnknownV1>>,
}

impl RecordingError {
    fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            possibly_published: None,
        }
    }

    fn from_record(error: omnigraph_bench::record::RecordError) -> Self {
        Self::new(error.code, error.to_string())
    }

    fn from_archive(error: ArchiveError) -> Self {
        Self {
            code: error.code.to_string(),
            message: error.to_string(),
            possibly_published: error.possibly_published,
        }
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
    completed_run: Option<&'a RunExecution>,
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
    completed_run: Option<&RunExecution>,
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
        completed_run,
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
    use super::*;

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
        };
        let failure = RecordingFailure {
            ok: false,
            runner_output_version: RUNNER_OUTPUT_VERSION,
            suite: "suite",
            suite_path: Path::new("suite.yaml"),
            completed_run_count: 1,
            completed_run: None,
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
    fn archive_mode_summary_does_not_duplicate_raw_runs() {
        let output = SuiteRunOutput {
            runner_output_version: RUNNER_OUTPUT_VERSION,
            suite: "suite".to_string(),
            suite_path: PathBuf::from("suite.yaml"),
            completed_run_count: 1,
            runs: None,
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
}
