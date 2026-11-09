//! The edition line changes nothing for edition-2 text (the GQ 3 RFC,
//! docs/rfcs/2026-11-07-gq-3.md, sections 6 and 7): every edition-2 query
//! text in the repository parses, type-checks and lowers through the front
//! door exactly as through the edition-2 parser that predates the line, and
//! the same texts read as edition 3 are refused only by the rules the RFC's
//! rewriter carries. These walks read every case of this corpus, so they
//! live with it and run whenever a case changes.

use std::collections::BTreeMap;

use omnigraph_compiler::catalog::{Catalog, build_catalog};
use omnigraph_compiler::query::diagnostic::QueryDiagnostic;
use omnigraph_compiler::query::parser::{parse_edition2, parse_query_diagnostic};
use omnigraph_compiler::query::parser3::{EditionLine, edition_line, parse_query3};
use omnigraph_compiler::query::typecheck::{CheckedQuery, typecheck_query_decl};
use omnigraph_compiler::query::typeck3::typecheck_file3;
use omnigraph_compiler::schema::parser::parse_schema;

fn root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn collect_ext(dir: &std::path::Path, ext: &str, out: &mut Vec<std::path::PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if path
                .file_name()
                // The red-team corpus and the GQT cases under `gq3/` are
                // edition-3 text, not edition-2 text.
                .is_some_and(|n| n == "target" || n == ".git" || n == "gq3-redteam" || n == "gq3")
            {
                continue;
            }
            collect_ext(&path, ext, out);
        } else if path.extension().is_some_and(|e| e == ext) {
            out.push(path);
        }
    }
}

/// Every query text the repository holds in edition 2: the `query` and
/// `mutate` sections of the GQT corpus, every `.gq` file under `docs/`, and
/// every fenced `gq` block under `docs/`.
fn edition2_corpus() -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut files = Vec::new();
    collect_ext(
        &root().join("crates/omnigraph-gqt/cases"),
        "gqt",
        &mut files,
    );
    collect_ext(
        &root().join("crates/omnigraph-gqt/cases_slow"),
        "gqt",
        &mut files,
    );
    files.sort();
    for f in &files {
        let text = std::fs::read_to_string(f).unwrap();
        let name = f.strip_prefix(root()).unwrap_or(f).display().to_string();
        let mut section: Option<(String, String)> = None;
        let mut flush = |section: &mut Option<(String, String)>| {
            if let Some((header, body)) = section.take() {
                let kind = header.split_whitespace().next().unwrap_or("");
                if kind == "query" || kind == "mutate" {
                    out.push((format!("{name} ({header})"), body));
                }
            }
        };
        for line in text.lines() {
            if let Some(header) = line.strip_prefix("--- ") {
                flush(&mut section);
                section = Some((header.to_string(), String::new()));
            } else if let Some((_, body)) = section.as_mut() {
                body.push_str(line);
                body.push('\n');
            }
        }
        flush(&mut section);
    }
    let mut gq = Vec::new();
    collect_ext(&root().join("docs"), "gq", &mut gq);
    gq.sort();
    for f in gq {
        out.push((
            f.strip_prefix(root()).unwrap_or(&f).display().to_string(),
            std::fs::read_to_string(&f).unwrap(),
        ));
    }
    let mut md = Vec::new();
    collect_ext(&root().join("docs"), "md", &mut md);
    md.sort();
    for f in md {
        // RFC documents and their assets hold drafts in other editions.
        let rel = f.strip_prefix(root()).unwrap_or(&f).display().to_string();
        if rel.starts_with("docs/rfcs") {
            continue;
        }
        let text = std::fs::read_to_string(&f).unwrap();
        let mut inside = false;
        let mut block = String::new();
        let mut n = 0;
        for line in text.lines() {
            if !inside && line.trim_start().starts_with("```gq") {
                inside = true;
                block.clear();
            } else if inside && line.trim_start().starts_with("```") {
                inside = false;
                n += 1;
                out.push((format!("{rel}#{n}"), block.clone()));
            } else if inside {
                block.push_str(line);
                block.push('\n');
            }
        }
    }
    out
}

/// A source without `gq 3;` parses exactly as the edition-2 front end parses
/// it: the dispatcher changes no outcome, accepted or refused, in the whole
/// corpus.
#[test]
fn every_edition2_text_in_the_repository_parses_as_before() {
    let corpus = edition2_corpus();
    assert!(corpus.len() > 2500, "corpus shrank: {}", corpus.len());
    let mut accepted = 0;
    for (origin, text) in &corpus {
        let through_dispatch = parse_query_diagnostic(text);
        let direct = parse_edition2(text);
        assert_eq!(
            format!("{through_dispatch:?}"),
            format!("{direct:?}"),
            "{origin}"
        );
        accepted += usize::from(direct.is_ok());
    }
    eprintln!(
        "edition-2 corpus: {} texts, {accepted} accepted",
        corpus.len()
    );
    assert!(
        accepted * 10 > corpus.len() * 9,
        "{accepted} of {}",
        corpus.len()
    );
}

/// The same corpus read as edition 3 (section 7): every text either parses
/// or is refused by a rule the rewriter has a row for, a `where` on a
/// statement, or the bare-name rule in the four GQT cases whose `expect
/// error` code moves (the rewriter's fixed map carries them).
#[test]
fn the_edition2_corpus_read_as_edition_3() {
    let corpus = edition2_corpus();
    let mut parses3 = 0;
    let mut refused_by: std::collections::BTreeMap<String, usize> = Default::default();
    let mut bare_names = std::collections::BTreeSet::new();
    let mut unexpected = Vec::new();
    for (origin, text) in &corpus {
        if edition_line(text) != EditionLine::Absent {
            continue;
        }
        if parse_edition2(text).is_err() {
            continue;
        }
        match parse_query3(&format!("gq 3;\n{text}")) {
            Ok(_) => parses3 += 1,
            Err(e) => {
                match e.code.as_str() {
                    "Q008" => {}
                    "Q018" => {
                        bare_names.insert(origin.split(' ').next().unwrap_or("").to_string());
                    }
                    _ => unexpected.push(format!("{origin}: {e}")),
                }
                *refused_by
                    .entry(format!("{} {}", e.code.as_str(), first_words(&e.message)))
                    .or_default() += 1;
            }
        }
    }
    eprintln!("edition-2 corpus read as edition 3: {parses3} parse");
    for (k, v) in &refused_by {
        eprintln!("  refused {v:>5}  {k}");
    }
    assert!(
        unexpected.is_empty(),
        "refused by a rule with no rewriter row:\n{}",
        unexpected.join("\n")
    );
    assert_eq!(
        bare_names.into_iter().collect::<Vec<_>>(),
        [
            "crates/omnigraph-gqt/cases/bare_node_projection_object_fields.gqt",
            "crates/omnigraph-gqt/cases/expression_type_errors.gqt",
            "crates/omnigraph-gqt/cases/issue_640_search_score_projection.gqt",
            "crates/omnigraph-gqt/cases/issue_659_edge_selection_contract.gqt",
        ]
    );
    assert!(parses3 > 2000, "{parses3}");
}

fn first_words(message: &str) -> String {
    message
        .split_whitespace()
        .take(6)
        .collect::<Vec<_>>()
        .join(" ")
}

/// Every edition-2 GQT case's schema with its `query` and `mutate` sections.
fn gqt_corpus() -> Vec<(String, String, Vec<String>)> {
    gqt_files(&[
        "crates/omnigraph-gqt/cases",
        "crates/omnigraph-gqt/cases_slow",
    ])
}

/// The `.gqt` files under `dirs` (relative to the repository root): each
/// file's schema with its `query` and `mutate` sections.
fn gqt_files(dirs: &[&str]) -> Vec<(String, String, Vec<String>)> {
    let root = root();
    let mut files = Vec::new();
    for dir in dirs {
        collect(&root.join(dir), &mut files);
    }
    files.sort();
    let mut out = Vec::new();
    for f in files {
        let text = std::fs::read_to_string(&f).unwrap();
        let name = f.strip_prefix(&root).unwrap_or(&f).display().to_string();
        let mut schema = String::new();
        let mut queries = Vec::new();
        let mut section: Option<(String, String)> = None;
        let flush = |section: &mut Option<(String, String)>,
                     schema: &mut String,
                     queries: &mut Vec<String>| {
            if let Some((header, body)) = section.take() {
                match header.split_whitespace().next().unwrap_or("") {
                    "schema" => *schema = body,
                    "query" | "mutate" => queries.push(body),
                    _ => {}
                }
            }
        };
        for line in text.lines() {
            if let Some(header) = line.strip_prefix("--- ") {
                flush(&mut section, &mut schema, &mut queries);
                section = Some((header.to_string(), String::new()));
            } else if let Some((_, body)) = section.as_mut() {
                body.push_str(line);
                body.push('\n');
            }
        }
        flush(&mut section, &mut schema, &mut queries);
        if !schema.is_empty() && !queries.is_empty() {
            out.push((name, schema, queries));
        }
    }
    out
}

fn collect(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for e in entries.flatten() {
        let p = e.path();
        if p.is_dir() {
            // The edition-3 cases are not edition-2 text.
            if p.file_name().is_some_and(|n| n == "gq3") {
                continue;
            }
            collect(&p, out);
        } else if p.extension().is_some_and(|x| x == "gqt") {
            out.push(p);
        }
    }
}

/// One declaration's verdict: `ok` or its code.
fn verdict<T, E: std::fmt::Display>(r: &Result<T, E>, code: impl Fn(&E) -> String) -> String {
    match r {
        Ok(_) => "ok".into(),
        Err(e) => code(e),
    }
}

/// Edition 2's verdict per declaration of `text`.
fn edition2_verdicts(cat: &Catalog, text: &str) -> Option<Vec<(String, String)>> {
    let file = parse_edition2(text).ok()?;
    let qs = match file.body {
        omnigraph_compiler::query::ast::FileBody::Queries(qs) => qs,
        omnigraph_compiler::query::ast::FileBody::Explain(q) => vec![q],
        _ => return None,
    };
    Some(
        qs.iter()
            .map(|q| {
                let r = typecheck_query_decl(cat, q);
                (
                    q.name.clone(),
                    verdict(&r, |e| {
                        e.diagnostic()
                            .map_or_else(|| "other".to_string(), |d| d.code.as_str().to_string())
                    }),
                )
            })
            .collect(),
    )
}

/// The edition-2 corpus checked by both editions. Without the edition line a
/// text is edition 2 and its result is unchanged (the dispatcher is the only
/// new code on that path, and `every_edition2_text_in_the_repository_parses_as_before`
/// pins it); here every text edition 3 also parses is type-checked by the
/// edition-3 checker too, and the two verdicts are compared per declaration.
/// Every disagreement is either a rule edition 3 changes (each named below)
/// or a defect of this checker; the pinned table is the evidence.
#[test]
fn the_edition2_corpus_type_checks_alike_in_both_editions() {
    let mut agree = 0usize;
    let mut total = 0usize;
    let mut differ: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut e2_ok = 0usize;
    for (case, schema, queries) in gqt_corpus() {
        let Ok(schema) = parse_schema(&schema) else {
            continue;
        };
        let Ok(cat) = build_catalog(&schema) else {
            continue;
        };
        for text in queries {
            if edition_line(&text) != EditionLine::Absent {
                continue;
            }
            let Some(two) = edition2_verdicts(&cat, &text) else {
                continue;
            };
            let Ok(file) = parse_query3(&format!("gq 3;\n{text}")) else {
                continue;
            };
            let three: Vec<(String, String)> = typecheck_file3(&cat, &file)
                .into_iter()
                .map(|(n, r)| (n, verdict(&r, |e| e.code.as_str().to_string())))
                .collect();
            for ((name, a), (_, b)) in two.iter().zip(&three) {
                total += 1;
                e2_ok += usize::from(a == "ok");
                if a == b {
                    agree += 1;
                } else {
                    differ
                        .entry(format!("edition 2 {a:>4} / edition 3 {b:>4}"))
                        .or_default()
                        .push(format!("{case} `{name}`"));
                }
            }
        }
    }
    eprintln!("{total} declarations, {e2_ok} accepted by edition 2, {agree} with the same verdict");
    for (k, v) in &differ {
        eprintln!("  {k}: {}", v.len());
        for x in v.iter().take(4) {
            eprintln!("      {x}");
        }
    }
    // The verdicts that change acceptance, pinned. Every row is a rule the
    // RFC states (its Compatibility section and the rewriter table):
    // - T65 (156), T71 (4), T7 (53): the rewriter's ranking, positional
    //   option and infix `contains`/`starts_with` rows.
    // - T42 (3): an order key over grouped rows that is a meta-field neither
    //   returned nor a key; an order over groups reads keys and aggregates.
    // - edition 3 accepting: T40 lifted (1); a triple with neither end
    //   bound, which starts from a scan of the edge's source type (4); an
    //   order key that is not a return item (1); a Boolean or property item
    //   beside aggregates (2).
    let mut changed: BTreeMap<String, usize> = BTreeMap::new();
    for (k, v) in &differ {
        let ok2 = k.contains("edition 2   ok");
        let ok3 = k.ends_with("edition 3   ok");
        if ok2 != ok3 {
            *changed.entry(k.clone()).or_default() += v.len();
        }
    }
    let want: BTreeMap<String, usize> = [
        ("edition 2   ok / edition 3   T7", 53),
        ("edition 2   ok / edition 3  T42", 3),
        ("edition 2   ok / edition 3  T65", 156),
        ("edition 2   ok / edition 3  T71", 4),
        ("edition 2   T5 / edition 3   ok", 4),
        ("edition 2   T9 / edition 3   ok", 1),
        ("edition 2  T40 / edition 3   ok", 1),
        ("edition 2  T42 / edition 3   ok", 1),
        ("edition 2  T58 / edition 3   ok", 1),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect();
    assert_eq!(changed, want);
    assert!(agree * 100 >= total * 85, "{agree} of {total}");
}

/// One declaration's edition-2 outcome: the lowered IR of a checked read or
/// mutation, or the error, rendered.
fn lowered_ir(cat: &Catalog, decl: &omnigraph_compiler::query::ast::QueryDecl) -> String {
    match typecheck_query_decl(cat, decl) {
        Ok(CheckedQuery::Read(read)) => match omnigraph_compiler::lower_query(cat, &read) {
            Ok(ir) => format!("{ir:?}"),
            Err(e) => format!("lowering: {e}"),
        },
        Ok(CheckedQuery::Mutation(write)) => {
            match omnigraph_compiler::lower_mutation_query(cat, &write) {
                Ok(ir) => format!("{ir:?}"),
                Err(e) => format!("lowering: {e}"),
            }
        }
        Ok(CheckedQuery::Edition3(_)) => "edition 3".to_string(),
        Err(e) => format!("{e}"),
    }
}

/// Every declaration of `text` through `parse`, lowered.
fn lowered_file(
    cat: &Catalog,
    text: &str,
    parse: fn(&str) -> Result<omnigraph_compiler::query::ast::QueryFile, QueryDiagnostic>,
) -> Result<Vec<(String, String)>, String> {
    let file = parse(text).map_err(|e| format!("{e}"))?;
    let decls = match file.body {
        omnigraph_compiler::query::ast::FileBody::Queries(qs) => qs,
        omnigraph_compiler::query::ast::FileBody::Explain(q) => vec![q],
        _ => Vec::new(),
    };
    Ok(decls
        .iter()
        .map(|q| (q.name.clone(), lowered_ir(cat, q)))
        .collect())
}

/// Every edition-2 query text in the repository with a schema beside it (the
/// GQT corpus, the benchmark fixtures and each `.gq` file with a `.pg` of
/// the same stem) through the front door is typed and lowered to the same IR,
/// or refused with the same error, as through the edition-2 parser that
/// predates the edition line.
#[test]
fn every_edition2_text_type_checks_to_the_same_ir_as_before() {
    let mut texts = gqt_files(&[
        "crates/omnigraph-gqt/cases",
        "crates/omnigraph-gqt/cases_slow",
        "benchmarks",
    ]);
    let root = root();
    for (gq, pg) in [
        (
            "crates/omnigraph/tests/fixtures/test.gq",
            "crates/omnigraph/tests/fixtures/test.pg",
        ),
        (
            "crates/omnigraph/tests/fixtures/search.gq",
            "crates/omnigraph/tests/fixtures/search.pg",
        ),
    ] {
        texts.push((
            gq.to_string(),
            std::fs::read_to_string(root.join(pg)).unwrap(),
            vec![std::fs::read_to_string(root.join(gq)).unwrap()],
        ));
    }
    let mut declarations = 0;
    let mut lowered = 0;
    for (case, schema, queries) in texts {
        let Ok(schema) = parse_schema(&schema) else {
            continue;
        };
        let Ok(cat) = build_catalog(&schema) else {
            continue;
        };
        for text in queries {
            if edition_line(&text) != EditionLine::Absent {
                continue;
            }
            let before = lowered_file(&cat, &text, parse_edition2);
            let through_the_door = lowered_file(&cat, &text, parse_query_diagnostic);
            assert_eq!(through_the_door, before, "{case}");
            for (_, ir) in before.iter().flatten() {
                declarations += 1;
                lowered += usize::from(ir.starts_with("QueryIR") || ir.starts_with("MutationIR"));
            }
        }
    }
    assert!(declarations > 1500, "{declarations}");
    assert!(lowered > 1300, "{lowered} of {declarations}");
}
