//! `omnigraph-gqt --check`: every refusal a case would meet before its first
//! step runs, printed at once, without a store, a build of the engine's
//! world or the corpus step.

use std::path::{Component, Path, PathBuf};

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
    let text = match crate::dst_runner::read_case_text(path) {
        Ok(text) => text,
        Err(error) => {
            let error = if error.starts_with("invalid_case:") {
                CaseError::from(error)
            } else {
                let reason = error.strip_prefix("report_failed: ").unwrap_or(&error);
                CaseError::new(
                    CaseErrorCode::Other,
                    format!("cannot read case file: {reason}"),
                )
            };
            return CheckedFile {
                path: path.to_path_buf(),
                errors: vec![error],
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

/// The corpus cap, for a file named or resolved under the required corpus
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
    let named = std::path::absolute(path).ok();
    let resolved = path.canonicalize().ok();
    [named, resolved].into_iter().flatten().any(|path| {
        let mut names: Vec<&std::ffi::OsStr> = Vec::new();
        for component in path.components() {
            match component {
                Component::ParentDir => {
                    names.pop();
                }
                other => names.push(other.as_os_str()),
            }
        }
        names.windows(3).any(|triple| {
            triple[0] == "crates" && triple[1] == "omnigraph-gqt" && triple[2] == "cases"
        })
    })
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

/// Check the cases each named path lists, print per file (text or one JSON
/// object per line), and answer the counts as the refusal when any error
/// shows. A path whose listing is refused is reported under its own name,
/// and the paths after it are still checked.
pub fn check_cases(
    named: &[(PathBuf, Result<Vec<PathBuf>, String>)],
    as_json: bool,
) -> Result<(), String> {
    let mut errors = 0usize;
    let mut failed = 0usize;
    let mut files = 0usize;
    let mut refused = 0usize;
    for (path, listed) in named {
        let cases = match listed {
            Ok(cases) => cases,
            Err(error) => {
                refused += 1;
                let refusal = CheckedFile {
                    path: path.clone(),
                    errors: vec![CaseError::new(CaseErrorCode::Other, error.as_str())],
                };
                print_checked(&refusal, as_json);
                continue;
            }
        };
        for case in cases {
            let checked = check_file(case);
            print_checked(&checked, as_json);
            files += 1;
            if !checked.errors.is_empty() {
                errors += checked.errors.len();
                failed += 1;
            }
        }
    }
    let mut summary = Vec::new();
    if errors > 0 {
        let noun = if errors == 1 { "error" } else { "errors" };
        summary.push(format!("{errors} {noun} in {failed} of {files} files"));
    }
    if refused > 0 {
        let subject = if refused == 1 {
            "path names"
        } else {
            "paths name"
        };
        summary.push(format!("{refused} {subject} no case"));
    }
    if summary.is_empty() {
        Ok(())
    } else {
        Err(summary.join("; "))
    }
}

fn print_checked(checked: &CheckedFile, as_json: bool) {
    if as_json {
        println!("{}", checked.to_json());
    } else if checked.errors.is_empty() {
        println!("ok {}", checked.path.display());
    } else {
        print!("{}", render_text(checked));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SLOW: &str = "# issue: none\n--- runner\ntimeout_ms: 20000\n";

    #[test]
    fn a_file_the_run_refuses_to_read_is_one_error_under_its_path() {
        let dir = tempfile::tempdir().unwrap();
        let not_a_file = check_file(dir.path());
        assert_eq!(not_a_file.errors.len(), 1);
        assert_eq!(
            not_a_file.errors[0].message,
            "cannot read case file: input must be a regular file"
        );
        let binary = dir.path().join("binary.gqt");
        std::fs::write(&binary, b"\xff\xfe").unwrap();
        let not_utf8 = check_file(&binary);
        assert_eq!(not_utf8.errors.len(), 1);
        assert!(
            not_utf8.errors[0].message.starts_with("UTF-8: "),
            "{}",
            not_utf8.errors[0].message
        );
    }

    #[cfg(unix)]
    #[test]
    fn the_corpus_cap_holds_for_a_case_named_or_resolved_under_the_corpus() {
        let dir = tempfile::tempdir().unwrap();
        let corpus = dir.path().join("crates/omnigraph-gqt/cases");
        let slow = dir.path().join("crates/omnigraph-gqt/cases_slow");
        let elsewhere = dir.path().join("elsewhere");
        for made in [&corpus, &slow, &elsewhere] {
            std::fs::create_dir_all(made).unwrap();
        }
        std::fs::write(corpus.join("held.gqt"), SLOW).unwrap();
        std::fs::write(slow.join("slow.gqt"), SLOW).unwrap();
        std::os::unix::fs::symlink(slow.join("slow.gqt"), corpus.join("linked_out.gqt")).unwrap();
        std::os::unix::fs::symlink(corpus.join("held.gqt"), elsewhere.join("linked_in.gqt"))
            .unwrap();
        let capped = |path: PathBuf| corpus_timeout(&path, SLOW).is_some();
        assert!(capped(corpus.join("held.gqt")));
        assert!(capped(corpus.join("linked_out.gqt")), "named under it");
        assert!(capped(elsewhere.join("linked_in.gqt")), "resolved under it");
        assert!(capped(slow.join("../cases/linked_out.gqt")), "`..` into it");
        assert!(!capped(slow.join("slow.gqt")));
        assert!(
            !capped(corpus.join("../cases_slow/slow.gqt")),
            "`..` out of it"
        );
    }
}
