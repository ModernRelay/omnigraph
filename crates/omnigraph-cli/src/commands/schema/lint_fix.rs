//! `omnigraph lint --fix`: apply the fixes the compiler prints to `.gq`
//! files (the GQ 3 RFC, section 6, "The respelling map"). The loop is the
//! compiler's [`fix_source`]; this module chooses the files and their
//! schemas, writes the changed ones in place and reports every file.
//!
//! A stored-query registry is fixed where it is declared: `--config` names
//! a cluster configuration directory, each graph's query files are fixed
//! against that graph's schema file, and `cluster apply` registers them,
//! under the deployment's admission and policy, like any other edit.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use color_eyre::eyre::{Result, bail, eyre};
use omnigraph_compiler::catalog::Catalog;
use omnigraph_compiler::{
    FixOptions, FixReport, FixStatus, QueryLintOutput, QueryLintSchemaSource, QueryLintStatus,
    fix_source, lint_query_file,
};
use serde::Serialize;

use super::LintArgs;
use super::lint::{catalog_of_schema_file, lint_catalog};
use crate::app::Completion;
use crate::output::write_json;

/// One file to fix: where it is, the graphs that declare it (a
/// configuration directory's), and the schema it is checked against.
struct Job {
    path: PathBuf,
    source: String,
    graphs: Vec<String>,
    catalog: Arc<Catalog>,
    schema_source: QueryLintSchemaSource,
}

/// What `--fix` did to one file.
#[derive(Debug, Serialize)]
pub(crate) struct FileFix {
    #[serde(flatten)]
    pub(crate) report: FixReport,
    /// Whether the fixed text was written in place: false for a file that
    /// did not change, could not be fixed, or under `--dry-run`.
    pub(crate) written: bool,
    /// The graphs of the configuration directory that declare the file.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) graphs: Vec<String>,
}

/// One file's lint report, of the text after its fixes (the original text
/// where nothing applied), with what the fix did.
#[derive(Debug, Serialize)]
pub(crate) struct FixedFile {
    #[serde(flatten)]
    pub(crate) lint: QueryLintOutput,
    pub(crate) fix: FileFix,
}

/// The report of a `--fix` over a directory or a configuration directory.
#[derive(Debug, Serialize)]
pub(crate) struct FixRun {
    /// `error` when a file holds a refusal with no applicable fix or its
    /// fixes do not converge.
    pub(crate) status: QueryLintStatus,
    pub(crate) dry_run: bool,
    pub(crate) review_fixes: bool,
    pub(crate) files_processed: usize,
    pub(crate) fixed: usize,
    pub(crate) unchanged: usize,
    pub(crate) unfixed: usize,
    pub(crate) does_not_converge: usize,
    pub(crate) written: usize,
    pub(crate) files: Vec<FixedFile>,
}

pub(crate) async fn run(
    args: LintArgs,
    binding: Option<&omnigraph::storage::CapturedStorageBinding>,
    stdout: &mut dyn Write,
) -> Result<Completion> {
    let options = FixOptions {
        review: args.review_fixes,
    };
    // Every file is read before its schema, as `lint` reads its query
    // first, and every file is fixed before any is written.
    let (jobs, single) = match (&args.config, &args.query) {
        (Some(config), _) => (bundle_jobs(config)?, false),
        (None, Some(query)) => {
            let (paths, single) = if query.is_dir() {
                let mut paths = Vec::new();
                gq_files(query, &mut paths)?;
                (paths, false)
            } else {
                (vec![query.clone()], true)
            };
            let sources = paths
                .iter()
                .map(|path| read_source(path))
                .collect::<Result<Vec<_>>>()?;
            let (catalog, schema_source) = lint_catalog(binding, args.schema.as_ref()).await?;
            let jobs = paths
                .into_iter()
                .zip(sources)
                .map(|(path, source)| Job {
                    path,
                    source,
                    graphs: Vec::new(),
                    catalog: Arc::clone(&catalog),
                    schema_source: schema_source.clone(),
                })
                .collect();
            (jobs, single)
        }
        (None, None) => bail!("lint --fix requires --query <file.gq|dir> or --config <dir>"),
    };

    let mut files = Vec::with_capacity(jobs.len());
    let mut writes = Vec::new();
    for job in jobs {
        let outcome = fix_source(&job.catalog, &job.source, options);
        let text = outcome.fixed.as_deref().unwrap_or(&job.source);
        let lint = lint_query_file(
            &job.catalog,
            text,
            job.path.to_string_lossy().into_owned(),
            job.schema_source,
        );
        if let Some(fixed) = &outcome.fixed
            && !args.dry_run
        {
            writes.push((files.len(), job.path.clone(), fixed.clone()));
        }
        files.push(FixedFile {
            lint,
            fix: FileFix {
                report: outcome.report,
                written: false,
                graphs: job.graphs,
            },
        });
    }
    for (index, path, text) in writes {
        write_in_place(&path, &text)?;
        files[index].fix.written = true;
    }

    let count = |status: FixStatus| {
        files
            .iter()
            .filter(|f| f.fix.report.status == status)
            .count()
    };
    let run = FixRun {
        status: if files.iter().any(|f| f.fix.report.failed()) {
            QueryLintStatus::Error
        } else {
            QueryLintStatus::Ok
        },
        dry_run: args.dry_run,
        review_fixes: args.review_fixes,
        files_processed: files.len(),
        fixed: count(FixStatus::Fixed),
        unchanged: count(FixStatus::Unchanged),
        unfixed: count(FixStatus::Unfixed),
        does_not_converge: count(FixStatus::DoesNotConverge),
        written: files.iter().filter(|f| f.fix.written).count(),
        files,
    };
    if args.json {
        if single {
            write_json(&run.files[0], stdout)?;
        } else {
            write_json(&run, stdout)?;
        }
    } else {
        write_human(&run, args.config.is_some(), stdout)?;
    }
    if run.status == QueryLintStatus::Error {
        stdout.flush()?;
        return Ok(Completion::exit(1));
    }
    Ok(Completion::success())
}

/// The jobs of a configuration directory: each stored-query file with the
/// schema of the graph that declares it. A file two graphs declare under
/// different schemas is refused before anything is read or written.
fn bundle_jobs(config: &Path) -> Result<Vec<Job>> {
    let graphs = omnigraph_cluster::stored_query_files(config).map_err(|diagnostics| {
        eyre!(
            "`{}` is not a cluster configuration directory lint can read:\n  {}",
            config.display(),
            diagnostics
                .iter()
                .map(|d| format!("{} at {}: {}", d.code, d.path, d.message))
                .collect::<Vec<_>>()
                .join("\n  ")
        )
    })?;
    let mut schemas: BTreeMap<PathBuf, Arc<Catalog>> = BTreeMap::new();
    let mut by_file: BTreeMap<PathBuf, (Vec<String>, PathBuf)> = BTreeMap::new();
    for graph in graphs {
        if !schemas.contains_key(&graph.schema) {
            let catalog = catalog_of_schema_file(&graph.schema)
                .map_err(|err| eyre!("graph `{}`: {err}", graph.graph_id))?;
            schemas.insert(graph.schema.clone(), Arc::new(catalog));
        }
        for file in graph.files {
            let entry = by_file
                .entry(file.clone())
                .or_insert_with(|| (Vec::new(), graph.schema.clone()));
            if entry.1 != graph.schema
                && std::fs::read(&entry.1).ok() != std::fs::read(&graph.schema).ok()
            {
                bail!(
                    "`{}` is declared by graphs `{}` and `{}`, whose schemas differ; fix it with `--query {} --schema <schema.pg>`",
                    file.display(),
                    entry.0.join("`, `"),
                    graph.graph_id,
                    file.display()
                );
            }
            entry.0.push(graph.graph_id.clone());
        }
    }
    by_file
        .into_iter()
        .map(|(path, (graphs, schema))| {
            Ok(Job {
                source: read_source(&path)?,
                path,
                graphs,
                catalog: Arc::clone(&schemas[&schema]),
                schema_source: QueryLintSchemaSource::file(schema.to_string_lossy().into_owned()),
            })
        })
        .collect()
}

fn read_source(path: &Path) -> Result<String> {
    std::fs::read_to_string(path).map_err(|err| eyre!("could not read `{}`: {err}", path.display()))
}

/// Every `.gq` file below `dir`, in path order; symbolic links are not
/// followed.
fn gq_files(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    let mut entries: Vec<PathBuf> = std::fs::read_dir(dir)
        .map_err(|err| eyre!("could not list `{}`: {err}", dir.display()))?
        .map(|entry| entry.map(|e| e.path()))
        .collect::<std::io::Result<_>>()?;
    entries.sort();
    for path in entries {
        let meta = std::fs::symlink_metadata(&path)?;
        if meta.file_type().is_symlink() {
            continue;
        }
        if meta.is_dir() {
            gq_files(&path, out)?;
        } else if path.extension().is_some_and(|ext| ext == "gq") {
            out.push(path);
        }
    }
    Ok(())
}

/// Replace `path`'s content with `text` by renaming a sibling written
/// first, keeping the file's permissions, so a failed write leaves the
/// original whole.
fn write_in_place(path: &Path, text: &str) -> Result<()> {
    let name = path
        .file_name()
        .ok_or_else(|| eyre!("`{}` names no file", path.display()))?;
    let temporary = path.with_file_name(format!(
        ".{}.lint-fix-{}",
        name.to_string_lossy(),
        std::process::id()
    ));
    let permissions = std::fs::metadata(path)?.permissions();
    let written = std::fs::write(&temporary, text)
        .and_then(|()| std::fs::set_permissions(&temporary, permissions))
        .and_then(|()| std::fs::rename(&temporary, path));
    if let Err(err) = written {
        let _ = std::fs::remove_file(&temporary);
        return Err(eyre!("could not write `{}`: {err}", path.display()));
    }
    Ok(())
}

fn codes(applied: &BTreeMap<String, usize>) -> String {
    applied
        .iter()
        .map(|(code, n)| format!("{code} ×{n}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn write_human(run: &FixRun, bundle: bool, out: &mut dyn Write) -> Result<()> {
    for file in &run.files {
        let path = &file.lint.query_path;
        let graphs = if file.fix.graphs.is_empty() {
            String::new()
        } else {
            format!(" (graph `{}`)", file.fix.graphs.join("`, `"))
        };
        let report = &file.fix.report;
        match report.status {
            FixStatus::Unchanged => writeln!(out, "UNCHANGED {path}{graphs}")?,
            FixStatus::Fixed => {
                let state = if file.fix.written {
                    "written"
                } else {
                    "dry run, not written"
                };
                writeln!(
                    out,
                    "FIXED     {path}{graphs}: {} pass{}; {} [{state}]",
                    report.passes,
                    if report.passes == 1 { "" } else { "es" },
                    codes(&report.applied)
                )?;
                if !report.applied_for_review.is_empty() {
                    writeln!(
                        out,
                        "  applied for review (read these changes): {}",
                        codes(&report.applied_for_review)
                    )?;
                }
            }
            FixStatus::Unfixed | FixStatus::DoesNotConverge => {
                let why = if report.status == FixStatus::Unfixed {
                    "a refusal prints no fix it can apply; nothing written"
                } else {
                    "the fixes do not settle; nothing written"
                };
                writeln!(out, "UNFIXED   {path}{graphs}: {why}")?;
                let pass = report.stopped_at_pass.unwrap_or(0);
                if pass > 0 {
                    writeln!(
                        out,
                        "  after {pass} pass{} of fixes ({}), the text holds:",
                        if pass == 1 { "" } else { "es" },
                        codes(&report.applied)
                    )?;
                }
                for d in &report.unfixed {
                    match d.position {
                        Some(at) => writeln!(
                            out,
                            "  error[{}] at line {}, column {}: {}",
                            d.code, at.line, at.column, d.message
                        )?,
                        None => writeln!(out, "  error[{}]: {}", d.code, d.message)?,
                    }
                    if let Some(fix) = &d.fix {
                        let review = d.suggestion.as_ref().is_some_and(|s| {
                            s.applicability == omnigraph_compiler::Applicability::NeedsReview
                        });
                        let note = if review {
                            " (printed for review; --review-fixes applies it)"
                        } else {
                            ""
                        };
                        writeln!(out, "  fix: {fix}{note}")?;
                    }
                }
            }
        }
    }
    writeln!(
        out,
        "{} file{}: {} fixed, {} unchanged, {} unfixed, {} not converging; {} written",
        run.files_processed,
        if run.files_processed == 1 { "" } else { "s" },
        run.fixed,
        run.unchanged,
        run.unfixed,
        run.does_not_converge,
        run.written
    )?;
    if bundle && run.written > 0 {
        writeln!(
            out,
            "register the fixed queries with `omnigraph cluster apply --target <cluster> --config <dir>`"
        )?;
    }
    Ok(())
}
