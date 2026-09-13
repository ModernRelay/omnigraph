#![recursion_limit = "512"]

use std::ffi::OsString;
use std::path::PathBuf;
use std::process::ExitCode;

struct Selection {
    path: PathBuf,
    target: Option<String>,
    storage: Option<String>,
    seed: Option<u64>,
}

enum Invocation {
    Replay(PathBuf),
    Case(Selection),
}

fn parse(args: &[OsString]) -> Result<Invocation, String> {
    let mut args = args.iter();
    let first = args.next().ok_or("usage: omnigraph-gqt <case.gqt> [--target <target>] [--storage <storage>] [--seed <u64>] | --replay <report.json>")?;
    if first == "--replay" {
        let path = args
            .next()
            .map(PathBuf::from)
            .ok_or("--replay requires a report path")?;
        if args.next().is_some() {
            return Err("--replay accepts only its report path".into());
        }
        return Ok(Invocation::Replay(path));
    }
    if first.to_string_lossy().starts_with("--") {
        return Err("expected a case path or --replay".into());
    }
    let mut selection = Selection {
        path: first.into(),
        target: None,
        storage: None,
        seed: None,
    };
    while let Some(arg) = args.next() {
        match arg.to_str() {
            Some("--target") if selection.target.is_none() => {
                selection.target = Some(
                    args.next()
                        .and_then(|v| v.to_str())
                        .ok_or("--target requires a target")?
                        .into(),
                );
            }
            Some("--storage") if selection.storage.is_none() => {
                selection.storage = Some(
                    args.next()
                        .and_then(|v| v.to_str())
                        .ok_or("--storage requires a storage value")?
                        .into(),
                );
            }
            Some("--seed") if selection.seed.is_none() => {
                selection.seed = Some(
                    args.next()
                        .and_then(|v| v.to_str())
                        .ok_or("--seed requires u64")?
                        .parse::<u64>()
                        .map_err(|e| format!("invalid seed: {e}"))?,
                );
            }
            _ => {
                return Err(
                    "unknown or repeated option; configuration belongs in the case file".into(),
                );
            }
        }
    }
    Ok(Invocation::Case(selection))
}

fn run() -> Result<(), String> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    let replay = args.first().is_some_and(|arg| arg == "--replay");
    let case_path = args
        .first()
        .filter(|arg| !arg.to_string_lossy().starts_with("--"))
        .map(PathBuf::from);
    if let Some(path) = &case_path {
        if omnigraph_gqt::run_worker_if_requested(path)? {
            return Ok(());
        }
    }
    let source_report = if replay {
        args.get(1).map(PathBuf::from)
    } else {
        None
    };
    let refusal =
        |error| omnigraph_gqt::report_cli_refusal(case_path.clone(), source_report.clone(), error);
    let invocation = parse(&args).map_err(|error| refusal(format!("invalid_case: {error}")))?;
    let executable = std::env::current_exe().map_err(|error| {
        refusal(format!(
            "environment_changed: locate GQT executable: {error}"
        ))
    })?;
    match invocation {
        Invocation::Replay(path) => omnigraph_gqt::replay_report(&path, &executable),
        Invocation::Case(selection) => {
            let bless = omnigraph_gqt::bless_from_env().map_err(refusal)?;
            let outcome = omnigraph_gqt::run_selected(
                &selection.path,
                &executable,
                bless,
                selection.target.as_deref(),
                selection.storage.as_deref(),
                selection.seed,
            );
            println!(
                "{} {} {:.2}s",
                if outcome.result.is_ok() {
                    if selection.target.is_some()
                        || selection.storage.is_some()
                        || selection.seed.is_some()
                    {
                        "ok (selected execution)"
                    } else {
                        "ok"
                    }
                } else {
                    "FAIL"
                },
                outcome.stem,
                outcome.elapsed.as_secs_f64()
            );
            outcome.result
        }
    }
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}
