//! `queries fmt`: query files in GQ's canonical spelling
//! (docs/rfcs/2026-11-07-gq-3.md, section 6). Local: no target, no
//! credential.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use color_eyre::eyre::{Result, eyre};
use omnigraph_compiler::catalog::Catalog;
use omnigraph_compiler::schema::parser::parse_schema;
use omnigraph_compiler::{QueryDiagnostic, build_catalog, format_source};

use crate::app::Completion;
use crate::output::write_json;

pub(crate) struct FmtArgs {
    pub(crate) paths: Vec<PathBuf>,
    pub(crate) check: bool,
    pub(crate) schema: Option<PathBuf>,
    pub(crate) json: bool,
}

/// One source formatted.
#[derive(serde::Serialize)]
struct FmtFile {
    /// The file, or `-` for stdin.
    path: String,
    /// Whether its canonical spelling differs from the text.
    changed: bool,
    /// Whether the canonical spelling was written to the file.
    written: bool,
    /// Declarations kept as written because a comment stands inside them.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    kept: Vec<String>,
    /// The refusal of a source that does not parse; nothing is written.
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<QueryDiagnostic>,
    /// The canonical text of stdin, outside `--check`.
    #[serde(skip_serializing_if = "Option::is_none")]
    text: Option<String>,
}

#[derive(serde::Serialize)]
struct FmtOutput {
    /// `ok`, or `error` when a source was refused or, under `--check`, is
    /// not in its canonical spelling.
    status: &'static str,
    check: bool,
    changed: usize,
    errors: usize,
    files: Vec<FmtFile>,
}

pub(crate) fn run_queries_fmt(
    args: FmtArgs,
    stdin: &mut dyn Read,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<Completion> {
    let catalog = match &args.schema {
        Some(path) => Some(read_catalog(path)?),
        None => None,
    };
    let stdin_only = args.paths.is_empty() || args.paths.iter().all(|p| p.as_os_str() == "-");
    let mut files = Vec::new();
    if stdin_only {
        let mut source = String::new();
        stdin
            .read_to_string(&mut source)
            .map_err(|e| eyre!("reading stdin: {e}"))?;
        files.push(format_one(
            "-",
            &source,
            catalog.as_ref(),
            args.check,
            None,
        )?);
    } else {
        for path in sources(&args.paths)? {
            let source = std::fs::read_to_string(&path)
                .map_err(|e| eyre!("reading {}: {e}", path.display()))?;
            let label = path.display().to_string();
            files.push(format_one(
                &label,
                &source,
                catalog.as_ref(),
                args.check,
                Some(&path),
            )?);
        }
    }
    let changed = files.iter().filter(|f| f.changed).count();
    let errors = files.iter().filter(|f| f.error.is_some()).count();
    let failed = errors > 0 || (args.check && changed > 0);
    let output = FmtOutput {
        status: if failed { "error" } else { "ok" },
        check: args.check,
        changed,
        errors,
        files,
    };
    if args.json {
        write_json(&output, stdout)?;
    } else {
        write_human(&output, stdin_only, stdout, stderr)?;
    }
    stdout.flush()?;
    Ok(if failed {
        Completion::exit(1)
    } else {
        Completion::success()
    })
}

fn read_catalog(path: &Path) -> Result<Catalog> {
    let source =
        std::fs::read_to_string(path).map_err(|e| eyre!("reading {}: {e}", path.display()))?;
    let schema = parse_schema(&source).map_err(|e| eyre!("{}: {e}", path.display()))?;
    build_catalog(&schema).map_err(|e| eyre!("{}: {e}", path.display()))
}

/// Each named file, and every `.gq` file under each named directory, in
/// name order; a directory named `target` or starting with `.` is skipped.
fn sources(paths: &[PathBuf]) -> Result<Vec<PathBuf>> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
        let mut entries: Vec<PathBuf> = std::fs::read_dir(dir)
            .map_err(|e| eyre!("reading {}: {e}", dir.display()))?
            .map(|e| e.map(|e| e.path()))
            .collect::<std::io::Result<_>>()
            .map_err(|e| eyre!("reading {}: {e}", dir.display()))?;
        entries.sort();
        for path in entries {
            let name = path.file_name().map(|n| n.to_string_lossy().into_owned());
            if path.is_dir() {
                if !name.is_some_and(|n| n == "target" || n.starts_with('.')) {
                    walk(&path, out)?;
                }
            } else if path.extension().is_some_and(|e| e == "gq") {
                out.push(path);
            }
        }
        Ok(())
    }
    let mut out = Vec::new();
    for path in paths {
        if path.as_os_str() == "-" {
            return Err(eyre!("`-` (stdin) cannot be combined with file paths"));
        }
        if path.is_dir() {
            walk(path, &mut out)?;
        } else if path.exists() {
            out.push(path.clone());
        } else {
            return Err(eyre!("{}: no such file or directory", path.display()));
        }
    }
    Ok(out)
}

fn format_one(
    label: &str,
    source: &str,
    catalog: Option<&Catalog>,
    check: bool,
    write_to: Option<&Path>,
) -> Result<FmtFile> {
    let formatted = match format_source(source, catalog) {
        Err(error) => {
            return Ok(FmtFile {
                path: label.to_string(),
                changed: false,
                written: false,
                kept: Vec::new(),
                error: Some(error),
                text: None,
            });
        }
        Ok(formatted) => formatted,
    };
    let mut written = false;
    if formatted.changed
        && !check
        && let Some(path) = write_to
    {
        std::fs::write(path, &formatted.text)
            .map_err(|e| eyre!("writing {}: {e}", path.display()))?;
        written = true;
    }
    Ok(FmtFile {
        path: label.to_string(),
        changed: formatted.changed,
        written,
        kept: formatted.kept,
        error: None,
        text: (write_to.is_none() && !check).then_some(formatted.text),
    })
}

fn write_human(
    output: &FmtOutput,
    stdin_only: bool,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<()> {
    for file in &output.files {
        if let Some(error) = &file.error {
            match error.position {
                Some(at) => writeln!(stderr, "{}:{}:{}: {error}", file.path, at.line, at.column)?,
                None => writeln!(stderr, "{}: {error}", file.path)?,
            }
            continue;
        }
        for name in &file.kept {
            writeln!(
                stderr,
                "{}: `{name}` kept as written: a comment stands inside it",
                file.path
            )?;
        }
        if let Some(text) = &file.text {
            stdout.write_all(text.as_bytes())?;
        } else if output.check && file.changed {
            writeln!(stdout, "would reformat {}", file.path)?;
        } else if file.written {
            writeln!(stdout, "formatted {}", file.path)?;
        }
    }
    if !stdin_only && output.errors == 0 && output.changed == 0 {
        writeln!(
            stdout,
            "{} file(s) already in canonical spelling",
            output.files.len()
        )?;
    }
    Ok(())
}
