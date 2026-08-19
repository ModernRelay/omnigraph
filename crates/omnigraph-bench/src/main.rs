//! omnigraph-bench — the branch-control micro-benchmark under RFC 0039 (the
//! end-to-end benchmark), benchmark v1.
//!
//! Measures `branch_merge` wall-clock, the 14-phase `MergeTimingPhase`
//! breakdown, and per-repetition storage-call counts on a parameterized
//! multi-table fixture, and writes one schema-validated JSON run record per
//! point (run spec + SUT block + machine spec + declared warmth). `diff`
//! compares two records (or directories) matched by their full point name,
//! warns on identity mismatches (rule 4), and labels deltas against the
//! session's A/A noise floor and claim margin (rule 7). `show` renders one
//! record (or directory) as tables. `fixture build` validates and freezes a
//! base store; `run --fixture` refuses an unvalidated or modified fixture.
//! `list` enumerates the runnable points. See this crate's README for the run
//! protocol and the deferrals.

// The merge future is deep and the run body executes under the probe
// task-local scope; the engine's own cost tests raise the same limit.
#![recursion_limit = "512"]

mod counting;
mod diff;
mod fixture;
mod fixture_cmd;
mod groups;
mod list;
mod machine;
mod record;
mod report;
mod run;
mod schema;

use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use clap::{Parser, Subcommand};

use fixture::BenchResult;
use fixture_cmd::FixtureBuildArgs;
use list::ListArgs;
use run::RunArgs;

#[derive(Parser)]
#[command(
    name = "omnigraph-bench",
    about = "Branch-control micro-benchmark (RFC 0039 micro profile): merge wall-clock, phase \
             attribution, storage-call counts (release builds only)"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Acquire a base store (build inline, or copy a validated frozen
    /// fixture), run a merge scenario under one declared warmth regime, write
    /// one schema-validated run record per point.
    Run(Box<RunArgs>),
    /// Compare two run records (files) or two run directories matched by
    /// point name; prints per-phase absolute + percent deltas, warns on
    /// identity mismatches, labels deltas against the floor and margin.
    Diff {
        a: PathBuf,
        b: PathBuf,
        /// noise-floor.json from `run --aa`; deltas below the floor read
        /// "no detected effect" (RFC 0039 rule 7).
        #[arg(long)]
        floor: Option<PathBuf>,
        /// Claim margin (rule 7): an effect must exceed floor x margin to be
        /// claimable. Overrides the floor file's persisted default (2.0).
        #[arg(long)]
        margin: Option<f64>,
        /// Output format: aligned text, or GitHub-flavored markdown tables.
        #[arg(long, value_enum, default_value_t = diff::Format::Text)]
        format: diff::Format,
    },
    /// Render run records (a file or a directory) as one self-contained HTML
    /// page: overview, per-record phase and storage-call charts (inline SVG,
    /// no external requests, no JavaScript), full identity, and — with
    /// --floor — rule-7-labeled A/A pair deltas.
    Report(report::ReportArgs),
    /// Render one run record (or every record of a directory) as tables:
    /// identity, wall-clock, phases, write path, storage calls.
    Show {
        path: PathBuf,
        /// Output format: aligned text, or GitHub-flavored markdown tables.
        #[arg(long, value_enum, default_value_t = diff::Format::Text)]
        format: diff::Format,
    },
    /// Frozen base-store fixtures (build + validate once, run many).
    Fixture {
        #[command(subcommand)]
        cmd: FixtureCmd,
    },
    /// Enumerate the runnable benchmark points (known scenarios x frozen
    /// fixtures x pre-tagged deltas x warmth regimes).
    List(ListArgs),
}

#[derive(Subcommand)]
enum FixtureCmd {
    /// Build a frozen base store from Data+State parameters into
    /// `<fixtures-root>/<derived-name>/`, run the validation pass, and stamp
    /// `fixture-manifest.json`. A fixture that fails validation never
    /// freezes.
    Build(Box<FixtureBuildArgs>),
    /// Run the validation pass on an existing stamp-less fixture (e.g. the
    /// manifest's stamp was deleted to force re-validation) and stamp its
    /// manifest in place. The frozen store is validated on a copy and
    /// digested untouched. An already-stamped fixture is refused.
    Validate { dir: PathBuf },
    /// Print this binary's fixture builder version (an integer): the value a
    /// frozen fixture's manifest must match for `run --fixture` to accept it.
    /// Scripts compare it against a cached fixture's manifest before skipping
    /// a rebuild.
    BuilderVersion,
}

fn main() -> std::process::ExitCode {
    let cli = Cli::parse();
    // The session id (RFC 0039): one harness invocation batch = one CLI run
    // here; minted once and persisted in every record this batch writes.
    let session_id = ulid::Ulid::new().to_string();
    let result = match cli.cmd {
        Cmd::Run(args) => on_big_stack(move || run::run(*args, session_id)),
        Cmd::Diff {
            a,
            b,
            floor,
            margin,
            format,
        } => diff::run_diff(&a, &b, floor.as_deref(), margin, format),
        Cmd::Report(args) => report::run_report(args),
        Cmd::Show { path, format } => diff::run_show(&path, format),
        Cmd::Fixture {
            cmd: FixtureCmd::Build(args),
        } => on_big_stack(move || fixture_cmd::fixture_build(*args)),
        Cmd::Fixture {
            cmd: FixtureCmd::Validate { dir },
        } => on_big_stack(move || fixture_cmd::fixture_validate(dir)),
        Cmd::Fixture {
            cmd: FixtureCmd::BuilderVersion,
        } => {
            println!("{}", fixture::BUILDER_VERSION);
            Ok(())
        }
        Cmd::List(args) => list::list_points(args),
    };
    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {err}");
            std::process::ExitCode::FAILURE
        }
    }
}

/// Run an async body on a dedicated 64 MiB-stack thread with a
/// current-thread runtime — the same shape the engine's merge cost tests use
/// (the merge future is deep, and the timing task-local must stay on the
/// merge's own task).
fn on_big_stack<Fut>(make: impl FnOnce() -> Fut + Send + 'static) -> BenchResult<()>
where
    Fut: std::future::Future<Output = BenchResult<()>>,
{
    std::thread::Builder::new()
        .stack_size(64 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?
                .block_on(make())
        })?
        .join()
        .map_err(|_| "bench thread panicked")?
}

/// Debug-build guard (RFC 0039 rule 2): a debug-build number must be
/// impossible to record, and a debug-built fixture must be impossible to
/// freeze or stamp (its provenance would lie beside release runs). The guard
/// reads `cfg!(debug_assertions)` — an approximation of "release"; the
/// record's `build_opt_level` carries the hard datum.
pub fn refuse_debug_build(what: &str) -> BenchResult<()> {
    if cfg!(debug_assertions) {
        return Err(format!(
            "this is a debug build; {what} from it is refused (rebuild with --release)"
        )
        .into());
    }
    Ok(())
}

pub fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_secs()
}

/// The SUT commit for records and manifests. Primary source: the commit
/// embedded at BUILD time by `build.rs` ("-dirty" appended when the build
/// tree had uncommitted tracked changes) — the value describes the binary,
/// and cold child processes report the same embedded value by construction.
/// The build script's rerun triggers watch `.git/HEAD` and the index only, so
/// an unstaged tracked edit re-links with the OLD embedded values; when
/// run-time git disagrees with them, "-stale-build" is appended (the record
/// still writes, marked) and a one-line warning goes to stderr. Fallback when
/// the build embedded nothing (git unavailable at build time): run-time
/// `git rev-parse`, labeled "unverified" because the tree may have moved
/// since the build; "unknown" when neither source is available.
pub fn source_commit() -> String {
    if let Some(commit) = option_env!("OMNIGRAPH_BENCH_GIT_COMMIT") {
        let embedded_dirty = matches!(option_env!("OMNIGRAPH_BENCH_GIT_DIRTY"), Some("1"));
        let value = compose_source_commit(commit, embedded_dirty, runtime_git_state().as_ref());
        if value.ends_with(STALE_BUILD_SUFFIX) {
            // Once per process: every record of a batch carries the marker,
            // one warning explains it.
            static STALE_WARNING: std::sync::Once = std::sync::Once::new();
            STALE_WARNING.call_once(|| {
                eprintln!(
                    "warning: the tree no longer matches the commit embedded in this binary \
                     (an unstaged edit does not re-trigger the build script) — records are \
                     marked '{STALE_BUILD_SUFFIX}'; rebuild to clear it"
                );
            });
        }
        return value;
    }
    std::process::Command::new("git")
        .args(["-C", env!("CARGO_MANIFEST_DIR"), "rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|out| out.status.success())
        .and_then(|out| String::from_utf8(out.stdout).ok())
        .map(|s| format!("unverified:{}", s.trim()))
        .unwrap_or_else(|| "unknown".to_string())
}

/// Marker appended to `source_commit` when run-time git contradicts the
/// build-time embedding (see [`source_commit`]).
const STALE_BUILD_SUFFIX: &str = "-stale-build";

/// Compose the SUT commit string from the embedded values and the run-time
/// git state (`None` when git is unavailable at run time — the embedded value
/// then stands unchecked, exactly as on a machine without the source tree).
fn compose_source_commit(
    embedded_commit: &str,
    embedded_dirty: bool,
    runtime: Option<&(String, bool)>,
) -> String {
    let mut value = if embedded_dirty {
        format!("{embedded_commit}-dirty")
    } else {
        embedded_commit.to_string()
    };
    if let Some((runtime_commit, runtime_dirty)) = runtime {
        if runtime_commit != embedded_commit || *runtime_dirty != embedded_dirty {
            value.push_str(STALE_BUILD_SUFFIX);
        }
    }
    value
}

/// Run-time (commit, dirty) of the source tree the binary was built in, when
/// git can still see it. The dirty probe uses the same `-uno` flags as
/// `build.rs` so the comparison is like with like.
fn runtime_git_state() -> Option<(String, bool)> {
    let git = |args: &[&str]| -> Option<String> {
        let out = std::process::Command::new("git")
            .args(["-C", env!("CARGO_MANIFEST_DIR")])
            .args(args)
            .output()
            .ok()?;
        if !out.status.success() {
            return None;
        }
        String::from_utf8(out.stdout)
            .ok()
            .map(|s| s.trim().to_string())
    };
    let commit = git(&["rev-parse", "HEAD"])?;
    // A failed status deliberately reads as dirty — the honest direction,
    // and the same policy `build.rs` applies to the embedded flag.
    let dirty = git(&["status", "--porcelain", "-uno"]).is_none_or(|s| !s.is_empty());
    Some((commit, dirty))
}

/// The `OPT_LEVEL` cargo compiled this binary with, embedded by `build.rs`.
pub fn build_opt_level() -> String {
    option_env!("OMNIGRAPH_BENCH_OPT_LEVEL")
        .unwrap_or("unknown")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stale_build_suffix_marks_any_runtime_disagreement() {
        let rt = |commit: &str, dirty: bool| Some((commit.to_string(), dirty));
        // (embedded commit, embedded dirty, runtime state, expected)
        let cases = [
            ("abc", false, rt("abc", false), "abc"),
            ("abc", true, rt("abc", true), "abc-dirty"),
            // Unstaged-edit window: same commit, dirty flipped at run time.
            ("abc", false, rt("abc", true), "abc-stale-build"),
            ("abc", true, rt("abc", false), "abc-dirty-stale-build"),
            // HEAD moved without touching the index (e.g. soft reset).
            ("abc", false, rt("def", false), "abc-stale-build"),
            // No git at run time: the embedded value stands unchecked.
            ("abc", false, None, "abc"),
            ("abc", true, None, "abc-dirty"),
        ];
        for (commit, dirty, runtime, expected) in cases {
            assert_eq!(
                compose_source_commit(commit, dirty, runtime.as_ref()),
                expected,
                "embedded ({commit}, dirty={dirty})"
            );
        }
    }
}
