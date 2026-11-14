//! `omnigraph-gqt --check`: every refusal a case would meet before its first
//! step runs, printed at once, without a store, a build of the engine's
//! world or the corpus step.

use std::path::{Path, PathBuf};

use omnigraph_gqt_core::{CaseError, CaseErrorCode, parse_case};
use serde_json::json;

/// The `timeout_ms` the required corpus admits (`run_corpus_case` refuses
/// above it at run time; `--check` reports it before).
pub const CORPUS_TIMEOUT_MS: u64 = 10_000;

/// One checked file and every error found in it, in source order.
pub struct CheckedFile {
    pub path: PathBuf,
    pub errors: Vec<CaseError>,
}

impl CheckedFile {
    pub fn to_json(&self) -> serde_json::Value {
        json!({
            "path": self.path.display().to_string(),
            "errors": self.errors.iter().map(CaseError::to_json).collect::<Vec<_>>(),
        })
    }
}

pub fn check_file(path: &Path) -> CheckedFile {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) => {
            return CheckedFile {
                path: path.to_path_buf(),
                errors: vec![CaseError::new(
                    CaseErrorCode::Other,
                    format!("cannot read case file: {error}"),
                )],
            };
        }
    };
    let mut errors = match parse_case(&crate::stem_of(path), &text) {
        Ok(_) => Vec::new(),
        Err(errors) => errors.into_vec(),
    };
    errors.extend(corpus_timeout(path, &text));
    errors.sort_by_key(|error| error.line.unwrap_or(usize::MAX));
    CheckedFile {
        path: path.to_path_buf(),
        errors,
    }
}

/// The corpus cap, for a file under the required corpus
/// (`…/crates/omnigraph-gqt/cases/…`, never `cases_slow`), read off the
/// `timeout_ms:` line itself so a case the parser refused still hears it.
fn corpus_timeout(path: &Path, text: &str) -> Option<CaseError> {
    if !under_required_corpus(path) {
        return None;
    }
    let lines: Vec<&str> = text.lines().collect();
    let (idx, timeout_ms) = lines.iter().enumerate().find_map(|(idx, line)| {
        let value = line.trim_start().strip_prefix("timeout_ms:")?.trim();
        Some((idx, value.parse::<u64>().ok()?))
    })?;
    if timeout_ms <= CORPUS_TIMEOUT_MS {
        return None;
    }
    let line = idx + 1;
    Some(
        CaseError::new(
            CaseErrorCode::CorpusTimeout,
            format!(
                "the required corpus admits timeout_ms at most {CORPUS_TIMEOUT_MS}; this case declares {timeout_ms}"
            ),
        )
        .fix(format!(
            "set `timeout_ms: {CORPUS_TIMEOUT_MS}` or lower, or move the case under `cases_slow/`"
        ))
        .located(line, &lines),
    )
}

fn under_required_corpus(path: &Path) -> bool {
    let path = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    let names: Vec<&std::ffi::OsStr> = path.components().map(|c| c.as_os_str()).collect();
    names
        .windows(3)
        .any(|triple| triple[0] == "crates" && triple[1] == "omnigraph-gqt" && triple[2] == "cases")
}

/// `path:line: code: message`, the source line under it, the fix under that.
pub fn render_text(checked: &CheckedFile) -> String {
    let mut out = String::new();
    for error in &checked.errors {
        let path = checked.path.display();
        match error.line {
            Some(line) => out.push_str(&format!("{path}:{line}: ")),
            None => out.push_str(&format!("{path}: ")),
        }
        out.push_str(&format!("{}: {}\n", error.code.name(), error.message));
        if let Some(source) = &error.source {
            out.push_str(&format!("   |  {source}\n"));
        }
        if let Some(fix) = &error.fix {
            out.push_str(&format!("   = fix: {fix}\n"));
        }
    }
    out
}

/// Check every file, print per file (text or one JSON object per line), and
/// answer the error count as the refusal when any file has one.
pub fn check_cases(paths: &[PathBuf], as_json: bool) -> Result<(), String> {
    let mut errors = 0usize;
    let mut failed = 0usize;
    for path in paths {
        let checked = check_file(path);
        if as_json {
            println!("{}", checked.to_json());
        } else if checked.errors.is_empty() {
            println!("ok {}", path.display());
        } else {
            print!("{}", render_text(&checked));
        }
        if !checked.errors.is_empty() {
            errors += checked.errors.len();
            failed += 1;
        }
    }
    if errors == 0 {
        return Ok(());
    }
    let noun = if errors == 1 { "error" } else { "errors" };
    Err(format!(
        "{errors} {noun} in {failed} of {} files",
        paths.len()
    ))
}
