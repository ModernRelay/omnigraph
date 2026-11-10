//! The edition line changes nothing for edition-2 text (the GQ 3 RFC,
//! docs/rfcs/2026-11-07-gq-3.md, sections 6 and 7): every edition-2 query
//! text in the repository parses, type-checks and lowers through the front
//! door exactly as through the edition-2 parser, and the same texts read as
//! edition 3 are refused only by the rules the RFC's rewriter carries. These
//! walks read every case of this corpus, so they live with it and run
//! whenever a case changes.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use omnigraph_compiler::catalog::{Catalog, build_catalog};
use omnigraph_compiler::query::ast::{FileBody, QueryDecl, QueryFile};
use omnigraph_compiler::query::check::check_file;
use omnigraph_compiler::query::diagnostic::QueryDiagnostic;
use omnigraph_compiler::query::parser::parse_query_diagnostic;
use omnigraph_compiler::query::syntax::edition::{EditionLine, edition_line};
use omnigraph_compiler::query::syntax::edition_window;
use omnigraph_compiler::query::syntax::parse::parse_source;
use omnigraph_compiler::query::typecheck::{CheckedQuery, typecheck_query_decl};
use omnigraph_compiler::schema::parser::parse_schema;

/// One edition-2 query text: where it is, the schema beside it, and the text.
struct Text {
    origin: String,
    schema: Option<String>,
    source: String,
}

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Every file with extension `ext` under `dir`, sorted; edition-3 cases
/// (`cases/diagnostics/`) and build output are not edition-2 text.
fn files(dir: &Path, ext: &str) -> Vec<PathBuf> {
    fn walk(dir: &Path, ext: &str, out: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                let skipped = path.ends_with("cases/diagnostics")
                    || path
                        .file_name()
                        .is_some_and(|n| n == "target" || n == ".git");
                if !skipped {
                    walk(&path, ext, out);
                }
            } else if path.extension().is_some_and(|e| e == ext) {
                out.push(path);
            }
        }
    }
    let mut out = Vec::new();
    walk(dir, ext, &mut out);
    out.sort();
    out
}

fn relative(path: &Path) -> String {
    path.strip_prefix(root())
        .unwrap_or(path)
        .display()
        .to_string()
}

/// Every edition-2 query text the repository holds: the `query` and `mutate`
/// sections of the GQT corpus and the benchmark cases, with their schema;
/// every `.gq` file under `docs/` and the engine's fixtures, with the `.pg`
/// of the same stem when there is one; and every fenced `gq` block of the
/// documentation outside the RFCs, which hold drafts in other editions.
fn corpus() -> Vec<Text> {
    let mut out = Vec::new();
    for dir in [
        "crates/omnigraph-gqt/cases",
        "crates/omnigraph-gqt/cases_slow",
        "benchmarks",
    ] {
        for file in files(&root().join(dir), "gqt") {
            let origin = relative(&file);
            let mut schema = None;
            let mut sections = Vec::new();
            let mut section: Option<(String, String)> = None;
            let text = std::fs::read_to_string(&file).unwrap();
            for line in text.lines().chain(std::iter::once("--- end")) {
                if let Some(header) = line.strip_prefix("--- ") {
                    if let Some((header, body)) = section.take() {
                        match header.split_whitespace().next().unwrap_or("") {
                            "schema" => schema = Some(body),
                            "query" | "mutate" => sections.push((header, body)),
                            _ => {}
                        }
                    }
                    section = Some((header.to_string(), String::new()));
                } else if let Some((_, body)) = section.as_mut() {
                    body.push_str(line);
                    body.push('\n');
                }
            }
            out.extend(sections.into_iter().map(|(header, source)| Text {
                origin: format!("{origin} ({header})"),
                schema: schema.clone(),
                source,
            }));
        }
    }
    for dir in ["docs", "crates/omnigraph/tests/fixtures"] {
        for file in files(&root().join(dir), "gq") {
            out.push(Text {
                origin: relative(&file),
                schema: std::fs::read_to_string(file.with_extension("pg")).ok(),
                source: std::fs::read_to_string(&file).unwrap(),
            });
        }
    }
    for file in files(&root().join("docs"), "md") {
        let origin = relative(&file);
        if origin.starts_with("docs/rfcs") {
            continue;
        }
        let text = std::fs::read_to_string(&file).unwrap();
        let mut block: Option<String> = None;
        for line in text.lines() {
            match block.as_mut() {
                None if line.trim_start().starts_with("```gq") => block = Some(String::new()),
                None => {}
                Some(_) if line.trim_start().starts_with("```") => {
                    let source = block.take().unwrap();
                    out.push(Text {
                        origin: format!("{origin}#{}", out.len()),
                        schema: None,
                        source,
                    });
                }
                Some(body) => {
                    body.push_str(line);
                    body.push('\n');
                }
            }
        }
    }
    out.retain(|text| edition_line(&text.source) == EditionLine::Absent);
    out
}

fn catalog(text: &Text) -> Option<Catalog> {
    build_catalog(&parse_schema(text.schema.as_deref()?).ok()?).ok()
}

fn declarations(file: QueryFile) -> Vec<QueryDecl> {
    match file.body {
        FileBody::Queries(decls) => decls,
        FileBody::Explain(decl) => vec![decl],
        _ => Vec::new(),
    }
}

/// A source without an edition line parses exactly as the edition-2 front
/// end parses it: the dispatcher changes no outcome, accepted or refused.
#[test]
fn every_query_text_in_the_repository_parses_as_before() {
    let corpus = corpus();
    assert!(corpus.len() > 2500, "corpus shrank: {}", corpus.len());
    let mut accepted = 0;
    for text in &corpus {
        let through_the_door = parse_query_diagnostic(&text.source);
        let direct = edition_window::parse(&text.source);
        assert_eq!(
            format!("{through_the_door:?}"),
            format!("{direct:?}"),
            "{}",
            text.origin
        );
        accepted += usize::from(direct.is_ok());
    }
    assert!(
        accepted * 10 > corpus.len() * 9,
        "{accepted} of {}",
        corpus.len()
    );
}

/// One declaration's edition-2 outcome: the lowered IR of a checked read or
/// mutation, or the error, rendered.
fn lowered_ir(cat: &Catalog, decl: &QueryDecl) -> String {
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
        Ok(CheckedQuery::Core(_)) => "edition 3".to_string(),
        Err(e) => format!("{e}"),
    }
}

fn lowered(
    cat: &Catalog,
    source: &str,
    parse: fn(&str) -> Result<QueryFile, QueryDiagnostic>,
) -> Result<Vec<String>, String> {
    let file = parse(source).map_err(|e| format!("{e}"))?;
    Ok(declarations(file)
        .iter()
        .map(|decl| lowered_ir(cat, decl))
        .collect())
}

/// Every edition-2 text with a schema beside it is typed and lowered through
/// the front door to the same IR, or refused with the same error, as through
/// the edition-2 parser.
#[test]
fn every_query_text_type_checks_to_the_same_ir_as_before() {
    let mut declarations = 0;
    let mut lowered_ok = 0;
    for text in corpus() {
        let Some(cat) = catalog(&text) else { continue };
        let before = lowered(&cat, &text.source, edition_window::parse);
        let through_the_door = lowered(&cat, &text.source, parse_query_diagnostic);
        assert_eq!(through_the_door, before, "{}", text.origin);
        for ir in before.iter().flatten() {
            declarations += 1;
            lowered_ok += usize::from(ir.starts_with("QueryIR") || ir.starts_with("MutationIR"));
        }
    }
    assert!(declarations > 1500, "{declarations}");
    assert!(
        lowered_ok > 1300,
        "{lowered_ok} of {declarations} declarations lowered"
    );
}

/// The same texts read as edition 3 (section 7): each parses or is refused
/// by a rule the rewriter has a row for, a `where` on a statement, or the
/// bare-name rule in the four GQT cases whose `expect error` code moves (the
/// rewriter's fixed map carries them).
#[test]
fn the_corpus_read_under_the_edition_line() {
    let mut parses = 0;
    let mut bare_names = BTreeSet::new();
    let mut unexpected = Vec::new();
    for text in corpus() {
        if edition_window::parse(&text.source).is_err() {
            continue;
        }
        match parse_source(&format!("gq 3;\n{}", text.source)) {
            Ok(_) => parses += 1,
            Err(e) => match e.code.as_str() {
                "Q008" => {}
                "Q018" => {
                    bare_names.insert(text.origin.split(' ').next().unwrap_or("").to_string());
                }
                _ => unexpected.push(format!("{}: {e}", text.origin)),
            },
        }
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
    assert!(parses > 2000, "{parses}");
}

/// The GQT corpus checked by both checkers, declaration by declaration:
/// every verdict that changes acceptance is a rule the RFC states (its
/// Compatibility section and the rewriter table), pinned with its count.
#[test]
fn the_corpus_type_checks_alike_under_the_edition_line() {
    let code = |r: Result<(), String>| r.err().unwrap_or_else(|| "ok".to_string());
    let mut agree = 0usize;
    let mut total = 0usize;
    let mut changed: BTreeMap<String, usize> = BTreeMap::new();
    for text in corpus() {
        if !text.origin.starts_with("crates/omnigraph-gqt/") {
            continue;
        }
        let Some(cat) = catalog(&text) else { continue };
        let Ok(file) = edition_window::parse(&text.source) else {
            continue;
        };
        let two: Vec<String> = declarations(file)
            .iter()
            .map(|decl| {
                code(typecheck_query_decl(&cat, decl).map(|_| ()).map_err(|e| {
                    e.diagnostic()
                        .map_or_else(|| "other".to_string(), |d| d.code.as_str().to_string())
                }))
            })
            .collect();
        let Ok(file) = parse_source(&format!("gq 3;\n{}", text.source)) else {
            continue;
        };
        let three: Vec<String> = check_file(&cat, &file)
            .into_iter()
            .map(|(_, r)| code(r.map(|_| ()).map_err(|e| e.code.as_str().to_string())))
            .collect();
        for (a, b) in two.iter().zip(&three) {
            total += 1;
            if a == b {
                agree += 1;
            } else if (a == "ok") != (b == "ok") {
                *changed
                    .entry(format!("edition 2 {a:>4} / edition 3 {b:>4}"))
                    .or_default() += 1;
            }
        }
    }
    // - T65 (156), T71 (4), T7 (53): the rewriter's ranking, positional
    //   option and infix `contains`/`starts_with` rows.
    // - T42 (3): an order key over grouped rows that is a meta-field neither
    //   returned nor a key; an order over groups reads keys and aggregates.
    // - edition 3 accepting: T40 lifted (1); a triple with neither end
    //   bound, which starts from a scan of the edge's source type (4); an
    //   order key that is not a return item (1); a Boolean or property item
    //   beside aggregates (2).
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
