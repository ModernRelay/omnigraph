#![recursion_limit = "512"]

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use omnigraph_gqt::MeasureOptions;

struct Selection {
    paths: Vec<PathBuf>,
    target: Option<String>,
    storage: Option<String>,
    store: Option<String>,
    seed: Option<u64>,
    measure: Option<MeasureOptions>,
    artifacts: Option<PathBuf>,
}

enum Invocation {
    Replay(PathBuf),
    Case(Selection),
}

const USAGE: &str = "usage: omnigraph-gqt <case.gqt|dir>... [--store <URI>] [--target <target>] [--storage <storage>] [--seed <u64>] [--artifacts <dir>] [--measure [--model <name>] [--baseline <path>] [--write-baseline]] | --replay <report.json>";

fn parse(args: &[OsString]) -> Result<Invocation, String> {
    let mut args = args.iter().peekable();
    let first = args.peek().ok_or(USAGE)?;
    if *first == "--replay" {
        args.next();
        let path = args
            .next()
            .map(PathBuf::from)
            .ok_or("--replay requires a report path")?;
        if args.next().is_some() {
            return Err("--replay accepts only its report path".into());
        }
        return Ok(Invocation::Replay(path));
    }
    let mut selection = Selection {
        paths: Vec::new(),
        target: None,
        storage: None,
        store: None,
        seed: None,
        measure: None,
        artifacts: None,
    };
    let mut measure = false;
    let mut model: Option<String> = None;
    let mut baseline: Option<PathBuf> = None;
    let mut write_baseline = false;
    while let Some(arg) = args.next() {
        match arg.to_str() {
            Some("--store") if selection.store.is_none() => {
                selection.store = Some(
                    args.next()
                        .and_then(|v| v.to_str())
                        .filter(|v| !v.is_empty() && !v.starts_with("--"))
                        .ok_or("--store requires a URI")?
                        .into(),
                );
            }
            Some("--measure") if !measure => measure = true,
            Some("--artifacts") if selection.artifacts.is_none() => {
                selection.artifacts = Some(
                    args.next()
                        .map(PathBuf::from)
                        .ok_or("--artifacts requires a directory")?,
                );
            }
            Some("--model") if model.is_none() => {
                let name = args
                    .next()
                    .and_then(|v| v.to_str())
                    .ok_or("--model requires a model name")?;
                let names = omnigraph_gqt::measure_model_names();
                if !names.contains(&name) {
                    return Err(format!(
                        "--model takes one of {}, got `{name}`",
                        names.join(", ")
                    ));
                }
                model = Some(name.into());
            }
            Some("--baseline") if baseline.is_none() => {
                baseline = Some(
                    args.next()
                        .map(PathBuf::from)
                        .ok_or("--baseline requires a path")?,
                );
            }
            Some("--write-baseline") if !write_baseline => write_baseline = true,
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
            _ if !arg.to_string_lossy().starts_with("--") => {
                selection.paths.push(PathBuf::from(arg));
            }
            _ => {
                return Err(
                    "unknown or repeated option; configuration belongs in the case file".into(),
                );
            }
        }
    }
    if selection.paths.is_empty() {
        return Err("expected at least one case path".into());
    }
    if !measure && (model.is_some() || baseline.is_some() || write_baseline) {
        return Err("--model, --baseline and --write-baseline need --measure".into());
    }
    if write_baseline && baseline.is_none() {
        return Err("--write-baseline needs --baseline <path>, the file to write".into());
    }
    if measure {
        selection.measure = Some(MeasureOptions {
            model: model.unwrap_or_else(|| "unit".into()),
            baseline,
            write_baseline,
        });
    }
    Ok(Invocation::Case(selection))
}

/// The case files the paths name: a file as itself, a directory as every
/// `.gqt` under it, sorted, so a corpus run reads in one order.
fn case_files(paths: &[PathBuf]) -> Result<Vec<PathBuf>, String> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) -> Result<(), String> {
        let entries = std::fs::read_dir(dir)
            .map_err(|e| format!("cannot read directory {}: {e}", dir.display()))?;
        for entry in entries {
            let path = entry
                .map_err(|e| format!("cannot read directory {}: {e}", dir.display()))?
                .path();
            if path.is_dir() {
                walk(&path, out)?;
            } else if path.extension().is_some_and(|ext| ext == "gqt") {
                out.push(path);
            }
        }
        Ok(())
    }
    let mut files = Vec::new();
    for path in paths {
        if path.is_dir() {
            let mut found = Vec::new();
            walk(path, &mut found)?;
            if found.is_empty() {
                return Err(format!("no .gqt case under {}", path.display()));
            }
            found.sort();
            files.extend(found);
        } else {
            files.push(path.clone());
        }
    }
    Ok(files)
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
            let files = case_files(&selection.paths)
                .map_err(|error| refusal(format!("invalid_case: {error}")))?;
            let selected = selection.target.is_some()
                || selection.storage.is_some()
                || selection.seed.is_some();
            let mut failures = Vec::new();
            for path in &files {
                let outcome = omnigraph_gqt::run_selected(
                    path,
                    &executable,
                    bless,
                    selection.target.as_deref(),
                    selection.storage.as_deref(),
                    selection.seed,
                    selection.measure.clone(),
                    selection.artifacts.clone(),
                    selection.store.as_deref(),
                );
                println!(
                    "{} {} {:.2}s",
                    match (&outcome.result, selected) {
                        (Ok(()), true) => "ok (selected execution)",
                        (Ok(()), false) => "ok",
                        (Err(_), _) => "FAIL",
                    },
                    outcome.stem,
                    outcome.elapsed.as_secs_f64()
                );
                if let Err(error) = outcome.result {
                    failures.push(error);
                }
            }
            if files.len() > 1 {
                println!(
                    "{} of {} cases passed",
                    files.len() - failures.len(),
                    files.len()
                );
            }
            if failures.is_empty() {
                Ok(())
            } else {
                Err(failures.join("\n"))
            }
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
