//! Fix coverage over edition-2 text: how many queries the printed fixes
//! alone respell into edition 3, and which refusal shapes still stop them
//! (the GQ 3 RFC, section 6, "Stored sources at upgrade": every refused
//! edition-2 form prints its fix).
//!
//! An instrument, not a gate: it reads every `.gqt` case under
//! `OMNIGRAPH_FIX_COVERAGE_DIR` (the corpus of the edition-2 merge base,
//! less the steps edition 2 refused itself; or a
//! directory of stored queries exported beside their schema as
//! `<name>.pg` and `<name>.gq`, or both) and prints counts only, never query
//! text, so it can run over a private registry. Each query goes through the
//! loop `omnigraph lint --fix --review-fixes` runs
//! ([`omnigraph_compiler::fix_source`], every printed suggestion applied,
//! for review included). A query converges when the loop reaches a compiling
//! text, counted by the passes it took; it is stuck at the first refusal
//! that printed no applicable suggestion, counted by code and by the
//! message's leading words, its shape.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use omnigraph_compiler::catalog::build_catalog;
use omnigraph_compiler::schema::parser::parse_schema;
use omnigraph_compiler::{FixOptions, FixStatus, fix_source};

fn files(dir: &Path, ext: &str, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            files(&path, ext, out);
        } else if path.extension().is_some_and(|e| e == ext) {
            out.push(path);
        }
    }
}

/// (schema, query sources) of every case or stored query under `dir`.
fn inputs(dir: &Path) -> Vec<(String, Vec<String>)> {
    let mut out = Vec::new();
    let mut cases = Vec::new();
    files(dir, "gqt", &mut cases);
    cases.sort();
    for path in cases {
        let text = std::fs::read_to_string(&path).unwrap_or_default();
        let mut sections: Vec<(String, String)> = Vec::new();
        for line in text.lines() {
            if let Some(header) = line.strip_prefix("--- ") {
                sections.push((header.trim().to_string(), String::new()));
            } else if let Some((_, body)) = sections.last_mut() {
                body.push_str(line);
                body.push('\n');
            }
        }
        let Some((_, schema)) = sections.iter().find(|(h, _)| h == "schema").cloned() else {
            continue;
        };
        // A step edition 2 refused (`--- expect error`) is not a query to
        // respell: it stays refused in both editions.
        let refused_by_edition_2 = |i: usize| {
            sections[i + 1..]
                .iter()
                .find(|(h, _)| h != "params")
                .is_some_and(|(h, _)| h.starts_with("expect error"))
        };
        let queries = (0..sections.len())
            .filter(|&i| {
                let (h, b) = &sections[i];
                (h == "query"
                    || h.starts_with("query ")
                    || h == "mutate"
                    || h.starts_with("mutate "))
                    && b.trim_start().starts_with("query ")
                    && !refused_by_edition_2(i)
            })
            .map(|i| sections[i].1.clone())
            .collect();
        out.push((schema, queries));
    }
    let mut stored = Vec::new();
    files(dir, "gq", &mut stored);
    stored.sort();
    for path in stored {
        let schema = path.with_extension("pg");
        if let (Ok(schema), Ok(query)) = (
            std::fs::read_to_string(&schema),
            std::fs::read_to_string(&path),
        ) {
            out.push((schema, vec![query]));
        }
    }
    out
}

/// The leading words of a refusal, its shape: up to the first backtick,
/// colon or digit, so the names a message quotes stay out.
fn shape(message: &str) -> String {
    let cut = message
        .find(['`', ':', '\'', '"', '$'])
        .unwrap_or(message.len());
    message[..cut]
        .split_whitespace()
        .take(9)
        .collect::<Vec<_>>()
        .join(" ")
}

#[derive(Default)]
struct Coverage {
    compiled_as_written: usize,
    converged: BTreeMap<usize, usize>,
    /// (code, shape) → queries stuck there.
    stuck: BTreeMap<(String, String), usize>,
    no_schema: usize,
}

#[test]
#[ignore = "instrument: fix coverage over the edition-2 text under OMNIGRAPH_FIX_COVERAGE_DIR"]
fn fix_coverage_over_edition_2_text() {
    let dir = std::env::var_os("OMNIGRAPH_FIX_COVERAGE_DIR")
        .map(PathBuf::from)
        .expect("OMNIGRAPH_FIX_COVERAGE_DIR names the edition-2 text");
    let mut coverage = Coverage::default();
    for (schema, queries) in inputs(&dir) {
        let Ok(catalog) = parse_schema(&schema)
            .map_err(|_| ())
            .and_then(|s| build_catalog(&s).map_err(|_| ()))
        else {
            coverage.no_schema += queries.len();
            continue;
        };
        for query in queries {
            // The loop `omnigraph lint --fix --review-fixes` runs.
            let out = fix_source(&catalog, &query, FixOptions { review: true });
            // `OMNIGRAPH_FIX_COVERAGE_CODES=1` prints the codes applied,
            // never text.
            if std::env::var("OMNIGRAPH_FIX_COVERAGE_CODES").as_deref() == Ok("1") {
                println!("{:?}: {:?}", out.report.status, out.report.applied);
            }
            match out.report.status {
                FixStatus::Unchanged => coverage.compiled_as_written += 1,
                FixStatus::Fixed => *coverage.converged.entry(out.report.passes).or_default() += 1,
                FixStatus::Unfixed | FixStatus::DoesNotConverge => {
                    let (code, message) = match out.report.unfixed.first() {
                        Some(first) => (first.code.to_string(), shape(&first.message)),
                        None => ("none".to_string(), "does not converge".to_string()),
                    };
                    // `OMNIGRAPH_FIX_COVERAGE_SHOW=1` prints the text that
                    // stopped, for a corpus whose text may be shown; never
                    // set it over a private registry.
                    if std::env::var("OMNIGRAPH_FIX_COVERAGE_SHOW").as_deref() == Ok("1") {
                        println!("--- stuck at {code}: {message}\n{query}");
                    }
                    *coverage.stuck.entry((code, message)).or_default() += 1;
                }
            }
        }
    }
    let converged: usize = coverage.converged.values().sum();
    let stuck: usize = coverage.stuck.values().sum();
    println!(
        "compiled as written: {}; respelled by the printed fixes: {converged} (by passes {:?}); stuck: {stuck}; schema refused: {}",
        coverage.compiled_as_written, coverage.converged, coverage.no_schema
    );
    let mut ranked: Vec<_> = coverage.stuck.into_iter().collect();
    ranked.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    for ((code, shape), n) in ranked {
        println!("{n:5}  {code}  {shape}");
    }
}
