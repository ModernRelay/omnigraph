//! Guard: seam decisions must come from the compile-checked `catalog`
//! (`omnigraph::seams::catalog` / `omnigraph_cluster::seams::catalog`), never
//! bare string literals — and the catalog itself must stay consistent.
//!
//! The catalog statics give compile-time typo protection at every helper
//! call; the string-keyed `catalog::decide(name)` lookup keeps a literal path
//! open, so this source-walk closes the gap by construction — the same
//! defense-in-depth shape as `forbidden_apis.rs`. A seam is declared beside
//! the site it guards (so the compiler records its location) and indexed by
//! the catalog; this guard forces every call site to reference the static,
//! and the second test forces every declaration into the catalog's `pub use`
//! list and `ALL`, out of the catalog itself and out of test modules, onto a
//! production crossing, and onto the helper its declared effect pairs with.
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
    "guarded(",
    "park_first(",
    "catalog::decide(",
];

/// Site helper prefixes paired with the one effect the seam they take must
/// declare; `guarded(` takes any declared set and is not listed.
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

/// One declared seam static and the file it lives in.
struct Declared {
    ident: String,
    name: String,
    effects: Vec<String>,
    file: PathBuf,
}

/// One crate's declared statics, the identifiers its catalog re-exports, and
/// the identifiers its `ALL` array lists.
struct Catalog {
    label: String,
    declared: Vec<Declared>,
    reexported: Vec<String>,
    listed: Vec<String>,
}

/// Every decision seam declared in `contents`, one grammar: the
/// `decide_seam!` body `pub static IDENT = ("name", Op, [Effect, …]);`.
fn parse_declarations(contents: &str, file: &Path) -> Vec<Declared> {
    let mut declared = Vec::new();
    let mut cursor = 0;
    while let Some(rel) = contents[cursor..].find("pub static ") {
        let at = cursor + rel + "pub static ".len();
        let ident_len = contents[at..]
            .find(|c: char| !(c.is_alphanumeric() || c == '_'))
            .unwrap_or(0);
        let ident = contents[at..at + ident_len].to_string();
        let item_start = at + ident_len;
        let Some(item_len) = contents[item_start..].find(';') else {
            break;
        };
        let item = &contents[item_start..item_start + item_len];
        cursor = item_start + item_len;
        let (name, effects) = if item.trim_start().starts_with("= (") {
            let open = item.find('[').unwrap_or(item.len());
            let close = item[open..].find(']').map_or(item.len(), |i| open + i);
            let effects = item[open + 1..close.max(open + 1)]
                .split(',')
                .map(|e| e.trim().to_string())
                .filter(|e| !e.is_empty())
                .collect();
            (quoted(item).unwrap_or_default().to_string(), effects)
        } else {
            continue;
        };
        declared.push(Declared {
            ident,
            name,
            effects,
            file: file.to_path_buf(),
        });
    }
    declared
}

/// Parse one text holding declarations, `pub use …::IDENT;` re-exports and
/// `ALL` together (the fixture shape).
fn parse_catalog_text(contents: &str, label: &str) -> Catalog {
    let mut catalog = parse_index_text(contents, label);
    catalog.declared = parse_declarations(contents, Path::new(label));
    catalog
}

/// The catalog's index: the paths inside `catalog! { … }`, which are both
/// the re-exports and `ALL`; or, spelled out, its `pub use …::IDENT;` lines
/// and its `ALL` array.
fn parse_index_text(contents: &str, label: &str) -> Catalog {
    if let Some(open) = contents.find("catalog! {") {
        let body_start = open + "catalog! {".len();
        let close = block_end(contents, body_start)
            .unwrap_or_else(|| panic!("catalog {label}: catalog! block never closes"));
        let idents: Vec<String> = contents[body_start..close]
            .split(',')
            .filter_map(|entry| {
                let entry = entry.trim();
                (!entry.is_empty()).then(|| entry.rsplit("::").next().unwrap_or(entry).to_string())
            })
            .collect();
        return Catalog {
            label: label.to_string(),
            declared: Vec::new(),
            reexported: idents.clone(),
            listed: idents,
        };
    }
    let reexported = contents
        .lines()
        .filter_map(|line| {
            line.trim()
                .strip_prefix("pub use ")?
                .strip_suffix(';')?
                .rsplit("::")
                .next()
                .map(str::to_string)
        })
        .collect();
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
        declared: Vec::new(),
        reexported,
        listed,
    }
}

/// A crate's catalog: the index from `catalog_path`, the declarations from
/// every other source file under `src_root`.
fn parse_catalog(catalog_path: &Path, src_root: &Path, label: &str) -> Catalog {
    let contents = std::fs::read_to_string(catalog_path)
        .unwrap_or_else(|e| panic!("catalog {} is unreadable: {e}", catalog_path.display()));
    let mut catalog = parse_index_text(&contents, label);
    let mut files = Vec::new();
    collect_ext(src_root, "rs", &mut files);
    files.sort();
    for file in files {
        if file.canonicalize().ok() == catalog_path.canonicalize().ok() {
            continue;
        }
        if let Ok(text) = std::fs::read_to_string(&file) {
            catalog.declared.extend(parse_declarations(&text, &file));
        }
    }
    catalog
}

/// Whether a source line declares a seam static, in the `decide_seam!`
/// body, by the alias or by the raw type; such a line names the ident
/// without crossing or arming it.
fn is_declaration(line: &str) -> bool {
    let line = line.trim_start();
    line.starts_with("pub static ") && line.contains(" = (")
}

/// 1-based lines of every declaration the index cannot see: a `Seam::decide(`
/// whose item is not a `pub static`, or a `decide_seam!` body without one (a
/// private or crate-visible static is invisible to the listing and teardown).
fn unindexable_declarations(contents: &str) -> Vec<usize> {
    let raw = contents.match_indices("Seam::decide(");
    let bodies = contents.match_indices("decide_seam!").filter(|(at, _)| {
        let end = block_end(contents, *at).unwrap_or(contents.len());
        !contents[*at..end].contains("pub static ")
    });
    let mut lines: Vec<usize> = raw
        .chain(bodies)
        .map(|(at, _)| line_of(contents, at))
        .collect();
    lines.sort_unstable();
    lines
}

/// 1-based lines of every declaration in a catalog text: the catalog only
/// indexes, and discovery skips it, so a seam declared there is checked by
/// nothing else.
fn catalog_declarations(catalog_text: &str) -> Vec<usize> {
    let mut lines: Vec<usize> = catalog_text
        .match_indices("decide_seam!")
        .chain(catalog_text.match_indices("Seam::decide("))
        .map(|(at, _)| line_of(catalog_text, at))
        .collect();
    lines.sort_unstable();
    lines
}

/// Byte offset of the first line after `from` that is only a closing brace,
/// at any indentation: where a macro block invoked inside a module ends.
fn block_end(contents: &str, from: usize) -> Option<usize> {
    let mut at = from;
    for line in contents[from..].split_inclusive('\n') {
        if line.trim() == "}" {
            return Some(at);
        }
        at += line.len();
    }
    None
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
            for line in text.lines().filter(|line| !is_declaration(line)) {
                corpus.push_str(line);
                corpus.push('\n');
            }
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

/// Production source with comment and declaration lines removed: a static
/// counts as crossed only when code under `src/` names it.
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
            if !line.trim_start().starts_with("//") && !is_declaration(line) {
                corpus.push_str(line);
                corpus.push('\n');
            }
        }
    }
    corpus
}

/// A seam is a `pub static` beside its site, only indexed by the catalog:
/// declared in the catalog it records no site, under a test module it is
/// gone from non-test builds, not `pub static` it is invisible to the index.
fn check_declaration_placement(
    catalog: &Catalog,
    catalog_path: &Path,
    src_root: &Path,
    violations: &mut Vec<String>,
) {
    let mut files = Vec::new();
    collect_ext(src_root, "rs", &mut files);
    files.sort();
    for file in files {
        let Ok(text) = std::fs::read_to_string(&file) else {
            continue;
        };
        for line in unindexable_declarations(&text) {
            violations.push(format!(
                "{}:{line}: a decision seam outside the `decide_seam! {{ pub static … }}` grammar; the index, the listing and the catalog teardown cannot see it",
                file.display()
            ));
        }
    }
    let catalog_text = std::fs::read_to_string(catalog_path).unwrap_or_default();
    for line in catalog_declarations(&catalog_text) {
        violations.push(format!(
            "{}:{line}: a decision seam declared in the catalog, where discovery does not look; declare it beside its site and list its path in `catalog!`",
            catalog_path.display()
        ));
    }
    for d in &catalog.declared {
        let under_tests = d
            .file
            .components()
            .any(|c| c.as_os_str() == "tests" || c.as_os_str() == "tests.rs");
        if under_tests {
            violations.push(format!(
                "{}: `{}` is declared under a test module and does not exist in a non-test build",
                d.file.display(),
                d.ident
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
    let reexported: BTreeSet<&str> = catalog.reexported.iter().map(String::as_str).collect();
    for ident in &declared_idents {
        if !reexported.contains(ident) {
            violations.push(format!(
                "{}: `{ident}` is declared but not re-exported by the catalog",
                catalog.label
            ));
        }
    }
    for ident in &reexported {
        if !declared_idents.contains(ident) {
            violations.push(format!(
                "{}: the catalog re-exports `{ident}`, which is not a declared decision seam",
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

/// Every single-effect site helper call names a static whose one declared
/// effect is the one the helper implements.
fn check_helper_pairing(catalogs: &[&Catalog], files: &[PathBuf], violations: &mut Vec<String>) {
    let effects: std::collections::BTreeMap<&str, &[String]> = catalogs
        .iter()
        .flat_map(|c| c.declared.iter())
        .map(|d| (d.ident.as_str(), d.effects.as_slice()))
        .collect();
    for file in files {
        let Ok(contents) = std::fs::read_to_string(file) else {
            continue;
        };
        for (helper_effect, ident) in helper_calls(&contents) {
            if let Some(declared) = effects.get(ident.as_str())
                && *declared != [helper_effect.to_string()]
            {
                violations.push(format!(
                    "{}: `{ident}` declares [{}] but is passed to the `{}` helper, \
                     which takes a seam declaring only Effect::{helper_effect}",
                    file.display(),
                    declared.join(", "),
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

    let engine = parse_catalog(
        &engine_catalog_path,
        &manifest_dir().join("src"),
        "omnigraph::seams::catalog",
    );
    let cluster = parse_catalog(
        &cluster_catalog_path,
        &cluster_dir().join("src"),
        "omnigraph_cluster::seams::catalog",
    );

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
    check_declaration_placement(
        &engine,
        &engine_catalog_path,
        &manifest_dir().join("src"),
        &mut violations,
    );
    check_declaration_placement(
        &cluster,
        &cluster_catalog_path,
        &cluster_dir().join("src"),
        &mut violations,
    );
    check_helper_pairing(&[&engine, &cluster], &files_to_scan(), &mut violations);

    assert!(
        violations.is_empty(),
        "the seam catalogs must stay complete (every static declared beside its site, \
         re-exported and in ALL, nothing else), uniquely named, used, and paired with \
         the helper their effect names:\n{}",
        violations.join("\n")
    );
}

const FIXTURE_CATALOG: &str = r#"
decide_seam! {
    pub static A = ("x.a", Mutation, [Fail]);
}
decide_seam! {
    pub static B = ("x.b", Mutation, [Skip]);
}
decide_seam! {
    /// Two effects.
    pub static C = ("x.c", Mutation, [Fail, Skip]);
}
decide_seam! {
    pub static D = (
        "x.d",
        Mutation,
        [Fail, Skip],
    );
}
omnigraph_seams::catalog! {
    crate::x::A,
    crate::x::B,
    crate::x::D,
    crate::x::A,
}
"#;

#[test]
fn guard_refuses_duplicates_and_mispaired_helpers() {
    let catalog = parse_catalog_text(FIXTURE_CATALOG, "fixture");
    assert_eq!(
        catalog
            .declared
            .iter()
            .map(|d| (d.ident.as_str(), d.effects.join(",")))
            .collect::<Vec<_>>(),
        [
            ("A", "Fail".to_string()),
            ("B", "Skip".to_string()),
            ("C", "Fail,Skip".to_string()),
            ("D", "Fail,Skip".to_string())
        ],
        "a set is parsed whole; a body wrapped across lines too"
    );
    let mut pairing = Vec::new();
    let fixture = std::env::temp_dir().join("failpoint_names_guard_pairing.rs");
    std::fs::write(&fixture, "fail(&catalog::C)?; guarded(&catalog::C, op);").unwrap();
    check_helper_pairing(&[&catalog], std::slice::from_ref(&fixture), &mut pairing);
    assert_eq!(
        pairing,
        [format!(
            "{}: `C` declares [Fail, Skip] but is passed to the `fail` helper, which takes \
             a seam declaring only Effect::Fail",
            fixture.display()
        )],
        "a multi-effect seam reaches a single-effect helper only through `guarded`"
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
            "fixture: `C` is declared but missing from ALL".to_string(),
            "fixture: `C` is declared but not re-exported by the catalog".to_string(),
            "fixture: `B` (\"x.b\") has no production crossing under src/ — a seam nothing \
             crosses is dead weight"
                .to_string(),
            "fixture: `C` (\"x.c\") has no production crossing under src/ — a seam nothing \
             crosses is dead weight"
                .to_string(),
            "fixture: `D` (\"x.d\") has no production crossing under src/ — a seam nothing \
             crosses is dead weight"
                .to_string(),
        ],
        "`AB` must not count as a crossing of `A`; a comment or a case is not a crossing; \
         a declared seam the catalog! list omits is reported twice, as unlisted and as unexported"
    );
    assert!(
        !is_declaration("    fail(&catalog::A)?;")
            && !is_declaration("pub static A: DecideSeam = x;")
            && is_declaration("    pub static D = (\"x.d\", Mutation, [Fail]);")
            && is_declaration("    pub static E = ("),
        "a declaration line is the macro body's `pub static … = (`, wrapped or not; a crossing or a raw static is not one"
    );
    assert_eq!(
        unindexable_declarations(
            "pub static A: DecideSeam = Seam::decide(\"x.a\", Op::Mutation, &[Effect::Fail], Global::new());\n\
             static PRIVATE: DecideSeam = Seam::decide(\"x.p\", Op::Mutation, &[Effect::Fail], Global::new());\n\
             pub(crate) static CRATE: DecideSeam =\n    Seam::decide(\"x.c\", Op::Mutation, &[Effect::Fail], Global::new());\n\
             crate::seams::decide_seam! {\n    pub static D = (\"x.d\", Mutation, [Fail]);\n}\n\
             crate::seams::decide_seam! {\n    static E = (\"x.e\", Mutation, [Fail]);\n}\n"
        ),
        [1, 2, 4, 8],
        "every hand-written `Seam::decide` and every macro body without `pub static` is reported by line; the macro `pub static` is not"
    );
    assert_eq!(
        catalog_declarations(
            "omnigraph_seams::catalog! {\n    crate::x::A,\n}\n\
             decide_seam! {\n    pub static UNINDEXED = (\"x.u\", Mutation, [Fail]);\n}\n"
        ),
        [4],
        "a declaration inside the catalog is reported even when it is a well-formed `pub static`: discovery never reads the catalog"
    );
    assert!(
        catalog_declarations("omnigraph_seams::catalog! {\n    crate::x::A,\n}\n").is_empty(),
        "an index-only catalog is clean"
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
