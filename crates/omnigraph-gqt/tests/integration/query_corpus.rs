//! Every query text the repository documents or ships outside the GQT
//! corpus parses: the fenced `gq` blocks of the user and developer guides,
//! the agent skill and the README, and the `.gq` files of the guides and the
//! engine's fixtures; a `.gq` file with a `.pg` beside it also type-checks. The
//! GQT runner holds the corpus's own sections; these texts have no runner, so
//! a stale example would otherwise reach a reader.

use std::path::{Path, PathBuf};

use omnigraph_compiler::catalog::{Catalog, build_catalog};
use omnigraph_compiler::query::check::check_source;
use omnigraph_compiler::query::parser::parse_query_diagnostic;
use omnigraph_compiler::schema::parser::parse_schema;

/// One query text: where it is, the schema beside it, and the text.
struct Text {
    origin: String,
    schema: Option<String>,
    source: String,
}

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Every file with extension `ext` under `dir`, sorted, build output aside.
fn files(dir: &Path, ext: &str) -> Vec<PathBuf> {
    fn walk(dir: &Path, ext: &str, out: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                let skipped = path
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

/// Every documented query text: each `.gq` file under `docs/` and the
/// engine's fixtures, with the `.pg` of the same stem when there is one, and
/// every fenced `gq` block of the guides, the skill and the README, which
/// name types from several schemas and are parsed only; a block fenced
/// `gq fragment` is part of a source (a `match` clause, a write body) and is
/// not read. The RFCs and release notes hold texts of other times.
fn corpus() -> Vec<Text> {
    let mut out = Vec::new();
    for dir in ["docs", "crates/omnigraph/tests/fixtures"] {
        for file in files(&root().join(dir), "gq") {
            // The engine catalogue's declarations, a schema listed for
            // reference, not query text.
            if file.ends_with("docs/user/queries/catalogue.gq") {
                continue;
            }
            out.push(Text {
                origin: relative(&file),
                schema: std::fs::read_to_string(file.with_extension("pg")).ok(),
                source: std::fs::read_to_string(&file).unwrap(),
            });
        }
    }
    let mut pages = files(&root().join("docs"), "md");
    pages.extend(files(&root().join("skills/omnigraph"), "md"));
    pages.push(root().join("README.md"));
    for file in pages {
        let origin = relative(&file);
        if origin.starts_with("docs/rfcs") || origin.starts_with("docs/releases") {
            continue;
        }
        let text = std::fs::read_to_string(&file).unwrap();
        let mut blocks: Vec<(String, String)> = Vec::new();
        let mut block: Option<(String, String)> = None;
        for line in text.lines() {
            let fence = line.trim_start().strip_prefix("```");
            match (block.as_mut(), fence) {
                (None, Some(info)) => {
                    block = Some((info.trim().to_string(), String::new()));
                }
                (Some(_), Some(_)) => blocks.push(block.take().unwrap()),
                (Some((_, body)), None) => {
                    body.push_str(line);
                    body.push('\n');
                }
                (None, None) => {}
            }
        }
        for (n, (info, source)) in blocks.iter().enumerate() {
            if info == "gq" {
                out.push(Text {
                    origin: format!("{origin}#{n}"),
                    schema: None,
                    source: source.clone(),
                });
            }
        }
    }
    out
}

fn catalog(text: &Text) -> Option<Catalog> {
    build_catalog(&parse_schema(text.schema.as_deref()?).ok()?).ok()
}

#[test]
fn every_documented_query_text_parses() {
    let corpus = corpus();
    assert!(corpus.len() > 40, "corpus shrank: {}", corpus.len());
    let refused: Vec<String> = corpus
        .iter()
        .filter_map(|text| {
            parse_query_diagnostic(&text.source)
                .err()
                .map(|e| format!("{}: {e}", text.origin))
        })
        .collect();
    assert!(refused.is_empty(), "{}", refused.join("\n"));
}

/// A text with a schema beside it type-checks against that schema, every
/// declaration of it.
#[test]
fn every_documented_query_text_with_a_schema_type_checks() {
    let mut checked = 0;
    let mut refused = Vec::new();
    for text in corpus() {
        let Some(cat) = catalog(&text) else { continue };
        let Ok(decls) = check_source(&cat, &text.source) else {
            continue;
        };
        for (name, outcome) in decls {
            checked += 1;
            if let Err(e) = outcome {
                refused.push(format!("{} `{name}`: {e}", text.origin));
            }
        }
    }
    assert!(checked > 10, "{checked}");
    assert!(refused.is_empty(), "{}", refused.join("\n"));
}
