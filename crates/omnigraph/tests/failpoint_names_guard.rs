//! Guard: seam decisions must come from the compile-checked `catalog`
//! (`omnigraph::seams::catalog` / `omnigraph_cluster::seams::catalog`), never
//! bare string literals — and the catalog itself must stay consistent.
//!
//! The catalog statics give compile-time typo protection at every helper
//! call; the string-keyed `catalog::decide(name)` lookup keeps a literal path
//! open, so this source-walk closes the gap by construction — the same
//! defense-in-depth shape as `forbidden_apis.rs`. Add a new seam by adding its
//! static to the catalog first; this guard then forces every call site to
//! reference it, and the second test forces it into `ALL`, onto a production
//! crossing, and onto the helper its declared effect pairs with.
//!
//! The walker's grammar is the spelling the tree uses: a helper called by its
//! own name with `&…::IDENT` as the argument, `IDENT` a catalog static (a
//! full path or an imported name). A helper reached through a rename, a
//! binding or a macro is outside the grammar; the helpers' own `assert_eq!`
//! on the declared effect is the backstop that runs at every crossing.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// Call-site prefixes whose first argument must be a catalog static. The check
/// skips whitespace and newlines after the open paren, so wrapping the call
/// across lines cannot hide a literal.
const LITERAL_ARG_PREFIXES: &[&str] = &[
    "seams::fail(",
    "fail(&",
    "skip(&",
    "contention(&",
    "park_first(",
    "catalog::decide(",
];

/// Site helper prefixes paired with the effect the seam they take must declare.
const HELPER_EFFECTS: &[(&str, &str)] = &[
    ("fail(", "Fail"),
    ("skip(", "Skip"),
    ("contention(", "Contention"),
];

/// 1-based line number of `byte_off` within `contents`.
fn line_of(contents: &str, byte_off: usize) -> usize {
    contents[..byte_off].bytes().filter(|&b| b == b'\n').count() + 1
}

fn manifest_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn cluster_dir() -> PathBuf {
    manifest_dir().join("../omnigraph-cluster")
}

fn dst_dir() -> PathBuf {
    manifest_dir().join("../omnigraph-dst")
}

fn gqt_cases_dir() -> PathBuf {
    manifest_dir().join("../omnigraph-gqt/cases")
}

/// Production and test call sites of both crates. This guard file is
/// deliberately not in the set (it names the patterns as literals itself).
fn files_to_scan() -> Vec<PathBuf> {
    let mut out = Vec::new();
    for root in [
        manifest_dir().join("src"),
        manifest_dir().join("tests"),
        cluster_dir().join("src"),
        cluster_dir().join("tests"),
    ] {
        collect_ext(&root, "rs", &mut out);
    }
    out.retain(|file| !is_this_guard(file));
    out
}

fn is_this_guard(file: &Path) -> bool {
    file.file_name()
        .is_some_and(|n| n == "failpoint_names_guard.rs")
}

fn collect_ext(dir: &Path, ext: &str, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_ext(&path, ext, out);
        } else if path.extension().is_some_and(|e| e == ext) {
            out.push(path);
        }
    }
}

#[test]
fn seam_decisions_use_the_compile_checked_catalog() {
    let mut violations = Vec::new();
    for file in files_to_scan() {
        let Ok(contents) = std::fs::read_to_string(&file) else {
            continue;
        };
        for prefix in LITERAL_ARG_PREFIXES {
            let mut from = 0;
            while let Some(rel) = contents[from..].find(prefix) {
                let after_open = from + rel + prefix.len();
                if contents[after_open..].trim_start().starts_with('"') {
                    violations.push(format!(
                        "{}:{}: literal seam name at `{}` — use a `catalog::` static",
                        file.display(),
                        line_of(&contents, from + rel),
                        prefix.trim_end_matches(['(', '&']),
                    ));
                }
                from = after_open;
            }
        }
        if file == engine_failpoints_test() {
            let mut from = 0;
            while let Some(rel) = contents[from..].find("\"branch_merge.") {
                let offset = from + rel;
                violations.push(format!(
                    "{}:{}: literal branch-merge seam name — use a typed selector backed by \
                     `catalog::` statics",
                    file.display(),
                    line_of(&contents, offset),
                ));
                from = offset + 1;
            }
        }
    }
    assert!(
        violations.is_empty(),
        "seam decisions must reference the compile-checked \
         `omnigraph::seams::catalog::*` (or `omnigraph_cluster::seams::catalog::*`) \
         statics, not string literals — a literal typo would silently never fire:\n{}",
        violations.join("\n")
    );
}

fn engine_failpoints_test() -> PathBuf {
    manifest_dir().join("tests/failpoints.rs")
}

/// One declared catalog static.
struct Declared {
    ident: String,
    name: String,
    effect: String,
}

/// One catalog's declared statics plus the identifiers its `ALL` array lists.
struct Catalog {
    label: String,
    declared: Vec<Declared>,
    listed: Vec<String>,
}

/// Parse a catalog source: every `pub static IDENT: … = Seam::decide("name",
/// Op::…, Effect::…, …);` item, whatever its type is spelled as, plus `ALL`.
fn parse_catalog_text(contents: &str, label: &str) -> Catalog {
    let mut declared = Vec::new();
    let mut cursor = 0;
    while let Some(rel) = contents[cursor..].find("pub static ") {
        let at = cursor + rel + "pub static ".len();
        let Some(colon) = contents[at..].find(':') else {
            break;
        };
        let ident = contents[at..at + colon].trim().to_string();
        let item_start = at + colon;
        let Some(item_len) = contents[item_start..].find(';') else {
            break;
        };
        let item = &contents[item_start..item_start + item_len];
        cursor = item_start + item_len;
        let Some(decide) = item.find("Seam::decide(") else {
            continue;
        };
        let after = &item[decide + "Seam::decide(".len()..];
        let name = quoted(after).unwrap_or_default().to_string();
        let effect = after
            .find("Effect::")
            .map(|i| {
                after[i + "Effect::".len()..]
                    .chars()
                    .take_while(|c| c.is_alphanumeric() || *c == '_')
                    .collect::<String>()
            })
            .unwrap_or_default();
        declared.push(Declared {
            ident,
            name,
            effect,
        });
    }

    let all_at = contents
        .find("pub static ALL:")
        .unwrap_or_else(|| panic!("catalog {label} declares no ALL array"));
    let open = contents[all_at..].find("= &[").expect("ALL opens an array") + all_at + 4;
    let close = contents[open..].find("];").expect("ALL closes") + open;
    let listed = contents[open..close]
        .split(',')
        .filter_map(|entry| {
            let entry = entry.trim().trim_start_matches('&').trim();
            (!entry.is_empty()).then(|| entry.to_string())
        })
        .collect();

    Catalog {
        label: label.to_string(),
        declared,
        listed,
    }
}

fn parse_catalog(path: &Path, label: &str) -> Catalog {
    let contents = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("catalog {} is unreadable: {e}", path.display()));
    parse_catalog_text(&contents, label)
}

/// The first double-quoted string in `text`.
fn quoted(text: &str) -> Option<&str> {
    let open = text.find('"')? + 1;
    let close = text[open..].find('"')? + open;
    Some(&text[open..close])
}

/// Whether `ident` occurs in `corpus` as a whole identifier, not as part of
/// a longer one.
fn contains_ident(corpus: &str, ident: &str) -> bool {
    corpus.match_indices(ident).any(|(at, _)| {
        let before = corpus[..at].chars().next_back();
        let after = corpus[at + ident.len()..].chars().next();
        let boundary = |c: Option<char>| !c.is_some_and(|c| c.is_alphanumeric() || c == '_');
        boundary(before) && boundary(after)
    })
}

/// Every text a catalog static may be referenced from: Rust sources in the
/// crates that own or drive the seams, and the `.gqt` case corpus, which names
/// a seam by its string after `at:`.
fn reference_corpus(rust_roots: &[PathBuf], gqt_cases: Option<&Path>, skip: &Path) -> String {
    let mut files = Vec::new();
    for root in rust_roots {
        collect_ext(root, "rs", &mut files);
    }
    if let Some(cases) = gqt_cases {
        collect_ext(cases, "gqt", &mut files);
    }
    let mut corpus = String::new();
    for file in files {
        if file.canonicalize().ok() == skip.canonicalize().ok() || is_this_guard(&file) {
            continue;
        }
        if let Ok(text) = std::fs::read_to_string(&file) {
            corpus.push_str(&text);
            corpus.push('\n');
        }
    }
    corpus
}

/// Every `fail(&…::IDENT)` / `skip(&…)` / `contention(&…)` call in `contents`
/// as (helper effect, IDENT); a call whose argument is a parameter rather than
/// a path is skipped, its effect being pinned where the static was bound.
fn helper_calls(contents: &str) -> Vec<(&'static str, String)> {
    let mut calls = Vec::new();
    for (prefix, effect) in HELPER_EFFECTS {
        let mut from = 0;
        while let Some(rel) = contents[from..].find(prefix) {
            let at = from + rel;
            let after = at + prefix.len();
            from = after;
            let own_word = !contents[..at]
                .chars()
                .next_back()
                .is_some_and(|c| c.is_alphanumeric() || c == '_');
            let Some(rest) = contents[after..].trim_start().strip_prefix('&') else {
                continue;
            };
            let path: String = rest
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_' || *c == ':')
                .collect();
            if own_word
                && let Some(ident) = path.rsplit("::").next()
                && !ident.is_empty()
            {
                calls.push((*effect, ident.to_string()));
            }
        }
    }
    calls
}

/// Production source with comment lines removed: a static counts as crossed
/// only when code under `src/` names it.
fn production_corpus(src_root: &Path, catalog_path: &Path) -> String {
    let mut files = Vec::new();
    collect_ext(src_root, "rs", &mut files);
    let mut corpus = String::new();
    for file in files {
        if file.canonicalize().ok() == catalog_path.canonicalize().ok() {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(&file) else {
            continue;
        };
        for line in text.lines() {
            if !line.trim_start().starts_with("//") {
                corpus.push_str(line);
                corpus.push('\n');
            }
        }
    }
    corpus
}

/// A decision seam declared anywhere under `src/` other than the catalog is
/// invisible to `ALL`, the listing and this guard.
fn check_no_stray_declarations(src_root: &Path, catalog_path: &Path, violations: &mut Vec<String>) {
    let mut files = Vec::new();
    collect_ext(src_root, "rs", &mut files);
    for file in files {
        if file.canonicalize().ok() == catalog_path.canonicalize().ok() {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(&file) else {
            continue;
        };
        if let Some(at) = text.find("Seam::decide(") {
            violations.push(format!(
                "{}:{}: a decision seam declared outside the catalog",
                file.display(),
                line_of(&text, at)
            ));
        }
    }
}

fn check_catalog(
    catalog: &Catalog,
    production: &str,
    references: &str,
    violations: &mut Vec<String>,
) {
    let declared_idents: BTreeSet<&str> =
        catalog.declared.iter().map(|d| d.ident.as_str()).collect();
    let listed: BTreeSet<&str> = catalog.listed.iter().map(String::as_str).collect();

    if listed.len() != catalog.listed.len() {
        let mut seen = BTreeSet::new();
        for ident in &catalog.listed {
            if !seen.insert(ident.as_str()) {
                violations.push(format!(
                    "{}: ALL lists `{ident}` more than once",
                    catalog.label
                ));
            }
        }
    }
    for ident in &declared_idents {
        if !listed.contains(ident) {
            violations.push(format!(
                "{}: `{ident}` is declared but missing from ALL",
                catalog.label
            ));
        }
    }
    for ident in &listed {
        if !declared_idents.contains(ident) {
            violations.push(format!(
                "{}: ALL lists `{ident}`, which is not a declared decision seam",
                catalog.label
            ));
        }
    }

    let mut seen: BTreeSet<&str> = BTreeSet::new();
    for d in &catalog.declared {
        if !seen.insert(d.name.as_str()) {
            violations.push(format!(
                "{}: `{}` reuses the seam name \"{}\" — names are the case-file key \
                 and must be unique",
                catalog.label, d.ident, d.name
            ));
        }
    }

    for d in &catalog.declared {
        if !contains_ident(production, &d.ident) {
            violations.push(format!(
                "{}: `{}` (\"{}\") has no production crossing under src/ — a seam nothing \
                 crosses is dead weight",
                catalog.label, d.ident, d.name
            ));
            continue;
        }
        let quoted = format!("\"{}\"", d.name);
        if !contains_ident(references, &d.ident) && !references.contains(&quoted) {
            violations.push(format!(
                "{}: `{}` (\"{}\") is crossed but never armed by a test or a case",
                catalog.label, d.ident, d.name
            ));
        }
    }
}

/// Every site helper call names a static whose declared effect is the one
/// the helper implements.
fn check_helper_pairing(catalogs: &[&Catalog], files: &[PathBuf], violations: &mut Vec<String>) {
    let effects: std::collections::BTreeMap<&str, &str> = catalogs
        .iter()
        .flat_map(|c| c.declared.iter())
        .map(|d| (d.ident.as_str(), d.effect.as_str()))
        .collect();
    for file in files {
        let Ok(contents) = std::fs::read_to_string(file) else {
            continue;
        };
        for (helper_effect, ident) in helper_calls(&contents) {
            if let Some(declared) = effects.get(ident.as_str())
                && *declared != helper_effect
            {
                violations.push(format!(
                    "{}: `{ident}` declares Effect::{declared} but is passed to the \
                     `{}` helper",
                    file.display(),
                    helper_effect.to_lowercase(),
                ));
            }
        }
    }
}

#[test]
fn catalogs_are_complete_unique_and_used() {
    let engine_catalog_path = manifest_dir().join("src/seams/catalog.rs");
    let cluster_catalog_path = cluster_dir().join("src/seams.rs");

    let engine = parse_catalog(&engine_catalog_path, "omnigraph::seams::catalog");
    let cluster = parse_catalog(&cluster_catalog_path, "omnigraph_cluster::seams::catalog");

    let engine_corpus = reference_corpus(
        &[
            manifest_dir().join("src"),
            manifest_dir().join("tests"),
            dst_dir().join("src"),
        ],
        Some(&gqt_cases_dir()),
        &engine_catalog_path,
    );
    let cluster_corpus = reference_corpus(
        &[cluster_dir().join("src"), cluster_dir().join("tests")],
        None,
        &cluster_catalog_path,
    );

    let engine_src = production_corpus(&manifest_dir().join("src"), &engine_catalog_path);
    let cluster_src = production_corpus(&cluster_dir().join("src"), &cluster_catalog_path);

    let mut violations = Vec::new();
    check_catalog(&engine, &engine_src, &engine_corpus, &mut violations);
    check_catalog(&cluster, &cluster_src, &cluster_corpus, &mut violations);
    check_no_stray_declarations(
        &manifest_dir().join("src"),
        &engine_catalog_path,
        &mut violations,
    );
    check_no_stray_declarations(
        &cluster_dir().join("src"),
        &cluster_catalog_path,
        &mut violations,
    );
    check_helper_pairing(&[&engine, &cluster], &files_to_scan(), &mut violations);

    assert!(
        violations.is_empty(),
        "the seam catalogs must stay complete (every static in ALL and nothing else), \
         uniquely named, used, and paired with the helper their effect names:\n{}",
        violations.join("\n")
    );
}

const FIXTURE_CATALOG: &str = r#"
pub static A: DecideSeam = Seam::decide("x.a", Op::Mutation, Effect::Fail, Global::new());
pub static B: Seam<dyn Decide, Global<dyn Decide>> =
    Seam::decide("x.b", Op::Mutation, Effect::Skip, Global::new());
pub static ALL: &[&'static dyn SeamEntry] = &[&A, &B, &A];
"#;

#[test]
fn guard_refuses_duplicates_and_mispaired_helpers() {
    let catalog = parse_catalog_text(FIXTURE_CATALOG, "fixture");
    assert_eq!(
        catalog
            .declared
            .iter()
            .map(|d| d.ident.as_str())
            .collect::<Vec<_>>(),
        ["A", "B"],
        "a static spelled without the alias is still parsed"
    );
    let mut violations = Vec::new();
    check_catalog(
        &catalog,
        "fail(&catalog::A); AB;",
        "\"x.b\" A",
        &mut violations,
    );
    assert_eq!(
        violations,
        [
            "fixture: ALL lists `A` more than once".to_string(),
            "fixture: `B` (\"x.b\") has no production crossing under src/ — a seam nothing \
             crosses is dead weight"
                .to_string(),
        ],
        "`AB` must not count as a crossing of `A`; a comment or a case is not a crossing"
    );

    let calls = helper_calls(
        "skip(&crate::seams::catalog::A)?; fail(seam)?; fail(\n    &catalog::B,\n)?; \
         use catalog::A; fail(&A)?;",
    );
    assert_eq!(
        calls,
        [
            ("Fail", "B".to_string()),
            ("Fail", "A".to_string()),
            ("Skip", "A".to_string())
        ],
        "a parameter argument is skipped; full paths, wrapped calls and imported names pair"
    );
}
