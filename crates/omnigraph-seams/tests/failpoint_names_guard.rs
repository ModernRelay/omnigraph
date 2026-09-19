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
//! production crossing, under an arming test or case, and onto the helper its
//! declared effect pairs with.
//!
//! The walker's grammar is the spelling the tree uses: a helper called by its
//! own name with `&…::IDENT` as the argument, `IDENT` a catalog static (a
//! full path or an imported name). A helper reached through a rename, a
//! binding or a macro is outside the grammar; the helpers' own `assert_eq!`
//! on the declared effect is the backstop that runs at every crossing.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use syn::visit::Visit;

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

/// The seams crate, where this guard lives: a source walk that links only
/// the crate whose contract it enforces, so it costs a minute wherever it
/// runs and never resolves the engine's feature graph.
fn seams_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// As an engine integration test this guard resolved the
/// engine's feature graph (its dev-dependencies, no `dst`/`failpoints`),
/// a second substrate build in the GQT job that overran its budget. The
/// fix is structural: the crate that hosts the guard names no workspace
/// crate in any dependency table, so no `cargo test` of it can pull the
/// engine in. The manifest is read as text on purpose; a TOML decoder would
/// itself be a dependency this test has to account for.
#[test]
fn guard_host_crate_depends_on_no_workspace_crate_issue_755() {
    let manifest = seams_dir().join("Cargo.toml");
    let text = std::fs::read_to_string(&manifest)
        .unwrap_or_else(|e| panic!("{} is unreadable: {e}", manifest.display()));
    let mut table = String::new();
    let mut offending = Vec::new();
    for (index, raw) in text.lines().enumerate() {
        let line = raw.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        if line.starts_with('[') {
            table = line.trim_matches(|c| c == '[' || c == ']').to_string();
            continue;
        }
        let is_dependency_table = table == "dependencies"
            || table == "dev-dependencies"
            || table == "build-dependencies"
            || table.starts_with("target.");
        let names_workspace_crate = line.starts_with("omnigraph")
            || line.contains("path = \"../")
            || line.contains("package = \"omnigraph");
        if is_dependency_table && (names_workspace_crate || table == "dependencies") {
            offending.push(format!("{}:{}: {raw}", manifest.display(), index + 1));
        }
    }
    assert!(
        offending.is_empty(),
        "the seams crate hosts the seam guard so that the guard links nothing but this crate; \
         a workspace crate in its dependency tables, or any regular dependency at all, hands \
         the guard that crate's feature graph back:\n{}",
        offending.join("\n")
    );
}

fn engine_dir() -> PathBuf {
    seams_dir().join("../omnigraph")
}

fn cluster_dir() -> PathBuf {
    seams_dir().join("../omnigraph-cluster")
}

fn dst_dir() -> PathBuf {
    seams_dir().join("../omnigraph-dst")
}

fn gqt_cases_dir() -> PathBuf {
    seams_dir().join("../omnigraph-gqt/cases")
}

/// Production and test call sites of both crates. This guard file is
/// deliberately not in the set (it names the patterns as literals itself);
/// the seams crate's own sources are the helpers' definitions, not call
/// sites, and are not walked either.
fn files_to_scan() -> Vec<PathBuf> {
    let mut out = Vec::new();
    for root in [
        engine_dir().join("src"),
        engine_dir().join("tests"),
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
    engine_dir().join("tests/failpoints.rs")
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

/// 1-based lines of every declaration the index cannot see: a `Seam::decide(`
/// whose item is not a `pub static`, or a `decide_seam!` body without one (a
/// private or crate-visible static is invisible to the listing and teardown).
fn unindexable_declarations(contents: &str) -> Vec<usize> {
    let raw = contents
        .match_indices("Seam::decide(")
        .chain(contents.match_indices("Seam::decide_with_store("));
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
        .chain(catalog_text.match_indices("Seam::decide_with_store("))
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

/// Whether `file`, relative to `root`, is a test module by path: under a
/// `tests` directory or named `tests.rs` / `…_tests.rs`. Such a file arms
/// seams and never crosses them.
fn is_test_source(root: &Path, file: &Path) -> bool {
    let relative = file.strip_prefix(root).unwrap_or(file);
    relative.components().any(|c| c.as_os_str() == "tests")
        || relative
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n == "tests.rs" || n.ends_with("_tests.rs"))
}

/// What a body of Rust code names, comments and doc attributes excluded:
/// every path segment ident, every string literal, and every method call on
/// a SCREAMING_CASE receiver (a seam static) as `RECEIVER.method`.
#[derive(Default)]
struct Names {
    idents: BTreeSet<String>,
    strings: BTreeSet<String>,
    static_calls: BTreeSet<String>,
}

impl Names {
    fn note_static_call(&mut self, call: &syn::ExprMethodCall) {
        if let syn::Expr::Path(receiver) = &*call.receiver
            && let Some(last) = receiver.path.segments.last()
            && let ident = last.ident.to_string()
            && ident.chars().any(|c| c.is_ascii_uppercase())
            && ident
                .chars()
                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
        {
            self.static_calls.insert(format!("{ident}.{}", call.method));
        }
    }

    fn absorb(&mut self, other: Names) {
        self.idents.extend(other.idents);
        self.strings.extend(other.strings);
        self.static_calls.extend(other.static_calls);
    }
}

impl<'ast> Visit<'ast> for Names {
    fn visit_path_segment(&mut self, segment: &'ast syn::PathSegment) {
        self.idents.insert(segment.ident.to_string());
        syn::visit::visit_path_segment(self, segment);
    }

    fn visit_lit_str(&mut self, lit: &'ast syn::LitStr) {
        self.strings.insert(lit.value());
    }

    fn visit_attribute(&mut self, attr: &'ast syn::Attribute) {
        if !attr.path().is_ident("doc") {
            syn::visit::visit_attribute(self, attr);
        }
    }

    fn visit_use_name(&mut self, name: &'ast syn::UseName) {
        self.idents.insert(name.ident.to_string());
    }

    fn visit_use_rename(&mut self, rename: &'ast syn::UseRename) {
        self.idents.insert(rename.ident.to_string());
        self.idents.insert(rename.rename.to_string());
    }

    fn visit_use_path(&mut self, path: &'ast syn::UsePath) {
        self.idents.insert(path.ident.to_string());
        syn::visit::visit_use_path(self, path);
    }

    fn visit_macro(&mut self, mac: &'ast syn::Macro) {
        let declares = mac
            .path
            .segments
            .last()
            .is_some_and(|s| s.ident == "decide_seam");
        if declares {
            return;
        }
        let exprs = syn::punctuated::Punctuated::<syn::Expr, syn::Token![,]>::parse_terminated;
        if let Ok(exprs) = mac.parse_body_with(exprs) {
            for expr in &exprs {
                self.visit_expr(expr);
            }
        } else if let Ok(MacroNames(names)) = mac.parse_body::<MacroNames>() {
            self.absorb(names);
        }
        syn::visit::visit_macro(self, mac);
    }

    fn visit_expr_method_call(&mut self, call: &'ast syn::ExprMethodCall) {
        self.note_static_call(call);
        syn::visit::visit_expr_method_call(self, call);
    }
}

/// The idents and string literals of a macro body, read token by token.
struct MacroNames(Names);

impl syn::parse::Parse for MacroNames {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        fn walk(names: &mut Names, mut cursor: syn::buffer::Cursor) {
            while !cursor.eof() {
                if let Some((ident, next)) = cursor.ident() {
                    names.idents.insert(ident.to_string());
                    cursor = next;
                } else if let Some((literal, next)) = cursor.literal() {
                    if let Ok(lit) = syn::parse_str::<syn::LitStr>(&literal.to_string()) {
                        names.strings.insert(lit.value());
                    }
                    cursor = next;
                } else if let Some((inside, _, _, next)) = cursor.any_group() {
                    walk(names, inside);
                    cursor = next;
                } else if let Some((_, next)) = cursor.punct() {
                    cursor = next;
                } else if let Some((_, next)) = cursor.lifetime() {
                    cursor = next;
                } else {
                    break;
                }
            }
        }
        let mut names = Names::default();
        input.step(|cursor| {
            walk(&mut names, *cursor);
            Ok(((), syn::buffer::Cursor::empty()))
        })?;
        Ok(MacroNames(names))
    }
}

/// Whether `attrs` carry a `#[cfg(…)]` that holds only under `cfg(test)`:
/// `test` itself, an `all(…)` with such a member, or an `any(…)` whose
/// members all are; `not(…)`, `feature = …` and other predicates never.
fn cfg_requires_test(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|attr| {
        attr.path().is_ident("cfg")
            && attr
                .parse_args::<syn::Meta>()
                .is_ok_and(|meta| meta_requires_test(&meta))
    })
}

fn meta_requires_test(meta: &syn::Meta) -> bool {
    match meta {
        syn::Meta::Path(path) => path.is_ident("test"),
        syn::Meta::List(list) => {
            let Ok(members) = list.parse_args_with(
                syn::punctuated::Punctuated::<syn::Meta, syn::Token![,]>::parse_terminated,
            ) else {
                return false;
            };
            if list.path.is_ident("all") {
                members.iter().any(meta_requires_test)
            } else if list.path.is_ident("any") {
                !members.is_empty() && members.iter().all(meta_requires_test)
            } else {
                false
            }
        }
        syn::Meta::NameValue(_) => false,
    }
}

/// One file's names routed by `cfg`: every item, impl item, trait item,
/// field or `let` gated on `test` goes to `tests`, everything else to
/// `production`.
#[derive(Default)]
struct Split {
    production: Names,
    tests: Names,
}

fn item_attrs(item: &syn::Item) -> &[syn::Attribute] {
    match item {
        syn::Item::Const(i) => &i.attrs,
        syn::Item::Enum(i) => &i.attrs,
        syn::Item::ExternCrate(i) => &i.attrs,
        syn::Item::Fn(i) => &i.attrs,
        syn::Item::ForeignMod(i) => &i.attrs,
        syn::Item::Impl(i) => &i.attrs,
        syn::Item::Macro(i) => &i.attrs,
        syn::Item::Mod(i) => &i.attrs,
        syn::Item::Static(i) => &i.attrs,
        syn::Item::Struct(i) => &i.attrs,
        syn::Item::Trait(i) => &i.attrs,
        syn::Item::TraitAlias(i) => &i.attrs,
        syn::Item::Type(i) => &i.attrs,
        syn::Item::Union(i) => &i.attrs,
        syn::Item::Use(i) => &i.attrs,
        _ => &[],
    }
}

fn impl_item_attrs(item: &syn::ImplItem) -> &[syn::Attribute] {
    match item {
        syn::ImplItem::Const(i) => &i.attrs,
        syn::ImplItem::Fn(i) => &i.attrs,
        syn::ImplItem::Type(i) => &i.attrs,
        syn::ImplItem::Macro(i) => &i.attrs,
        _ => &[],
    }
}

fn trait_item_attrs(item: &syn::TraitItem) -> &[syn::Attribute] {
    match item {
        syn::TraitItem::Const(i) => &i.attrs,
        syn::TraitItem::Fn(i) => &i.attrs,
        syn::TraitItem::Type(i) => &i.attrs,
        syn::TraitItem::Macro(i) => &i.attrs,
        _ => &[],
    }
}

impl<'ast> Visit<'ast> for Split {
    fn visit_item(&mut self, item: &'ast syn::Item) {
        if cfg_requires_test(item_attrs(item)) {
            self.tests.visit_item(item);
        } else {
            syn::visit::visit_item(self, item);
        }
    }

    fn visit_impl_item(&mut self, item: &'ast syn::ImplItem) {
        if cfg_requires_test(impl_item_attrs(item)) {
            self.tests.visit_impl_item(item);
        } else {
            syn::visit::visit_impl_item(self, item);
        }
    }

    fn visit_trait_item(&mut self, item: &'ast syn::TraitItem) {
        if cfg_requires_test(trait_item_attrs(item)) {
            self.tests.visit_trait_item(item);
        } else {
            syn::visit::visit_trait_item(self, item);
        }
    }

    fn visit_field(&mut self, field: &'ast syn::Field) {
        if cfg_requires_test(&field.attrs) {
            self.tests.visit_field(field);
        } else {
            syn::visit::visit_field(self, field);
        }
    }

    fn visit_local(&mut self, local: &'ast syn::Local) {
        if cfg_requires_test(&local.attrs) {
            self.tests.visit_local(local);
        } else {
            syn::visit::visit_local(self, local);
        }
    }

    fn visit_path_segment(&mut self, segment: &'ast syn::PathSegment) {
        self.production.visit_path_segment(segment);
    }

    fn visit_lit_str(&mut self, lit: &'ast syn::LitStr) {
        self.production.visit_lit_str(lit);
    }

    fn visit_attribute(&mut self, attr: &'ast syn::Attribute) {
        self.production.visit_attribute(attr);
    }

    fn visit_macro(&mut self, mac: &'ast syn::Macro) {
        self.production.visit_macro(mac);
    }

    fn visit_expr_method_call(&mut self, call: &'ast syn::ExprMethodCall) {
        self.production.note_static_call(call);
        syn::visit::visit_expr_method_call(self, call);
    }
}

/// `text` parsed as a Rust file and split by `cfg`; a file syn cannot parse
/// is a loud failure naming `label`.
fn split_names(text: &str, label: &str) -> Split {
    let file = syn::parse_file(text).unwrap_or_else(|e| panic!("{label}: not parseable Rust: {e}"));
    let mut split = Split::default();
    split.visit_file(&file);
    split
}

/// Where a catalog's armers live: every file under a harness root, only the
/// test modules (by path, or inline) under a `src/` root.
struct ArmingRoots {
    harness: Vec<PathBuf>,
    src: Vec<PathBuf>,
}

/// Every Rust file that arms seams whole: the harness roots and the test
/// modules under the `src/` roots; never this guard, never a production file.
fn arming_files(roots: &ArmingRoots) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for root in &roots.harness {
        collect_ext(root, "rs", &mut files);
    }
    for root in &roots.src {
        let mut in_source = Vec::new();
        collect_ext(root, "rs", &mut in_source);
        files.extend(
            in_source
                .into_iter()
                .filter(|file| is_test_source(root, file)),
        );
    }
    files.retain(|file| !is_this_guard(file));
    files
}

/// What Rust arms: the arming files whole, plus the `cfg(test)` half of
/// every production file under the `src/` roots.
fn arming_names(roots: &ArmingRoots) -> Names {
    let mut names = Names::default();
    for file in arming_files(roots) {
        if let Ok(text) = std::fs::read_to_string(&file) {
            let mut split = split_names(&text, &file.display().to_string());
            names.absorb(std::mem::take(&mut split.production));
            names.absorb(split.tests);
        }
    }
    for root in &roots.src {
        let mut files = Vec::new();
        collect_ext(root, "rs", &mut files);
        for file in files.into_iter().filter(|file| !is_test_source(root, file)) {
            if let Ok(text) = std::fs::read_to_string(&file) {
                names.absorb(split_names(&text, &file.display().to_string()).tests);
            }
        }
    }
    names
}

/// What arms a seam: Rust test code, which names a static by its ident or,
/// in the DST harness, by its name as a string literal; and the `at` names
/// of the case corpus, matched exactly.
struct Armers {
    rust: Names,
    case_names: BTreeSet<String>,
}

fn armers(roots: &ArmingRoots, gqt_cases: Option<&Path>) -> Armers {
    let mut case_names = BTreeSet::new();
    if let Some(cases) = gqt_cases {
        for file in case_files(cases) {
            if let Ok(text) = std::fs::read_to_string(&file) {
                case_names.extend(seam_at_names(&text));
            }
        }
    }
    Armers {
        rust: arming_names(roots),
        case_names,
    }
}

/// The cases the runner discovers: regular `.gqt` files directly under
/// `cases`, dot-named ones skipped (`omnigraph-gqt/src/lib.rs`, discovery).
fn case_files(cases: &Path) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(cases) else {
        return Vec::new();
    };
    entries
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| {
            path.is_file()
                && path.extension().is_some_and(|e| e == "gqt")
                && path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| !n.starts_with('.'))
        })
        .collect()
}

/// The `at` of every bare `--- seam` section (the one header the runner
/// admits) in one case, its body decoded as the runner's `parse_seam`
/// decodes it, as YAML; a body that does not decode names nothing.
fn seam_at_names(case: &str) -> Vec<String> {
    let lines: Vec<&str> = case.lines().collect();
    let mut starts: Vec<usize> = (0..lines.len())
        .filter(|&i| lines[i].starts_with("--- "))
        .collect();
    starts.push(lines.len());
    let mut names = Vec::new();
    for pair in starts.windows(2) {
        let (start, end) = (pair[0], pair[1]);
        if lines[start]["--- ".len()..].trim_end() != "seam" {
            continue;
        }
        let body = lines[start + 1..end].join("\n");
        if let Ok(serde_yaml::Value::Mapping(fields)) = serde_yaml::from_str(&body)
            && let Some(at) = fields.get("at").and_then(serde_yaml::Value::as_str)
        {
            names.push(at.to_string());
        }
    }
    names
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

/// The files that cross seams: production sources under `src_root`, minus
/// the catalog (index only) and the test modules (`is_test_source`).
fn crossing_files(src_root: &Path, catalog_path: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    collect_ext(src_root, "rs", &mut files);
    files.retain(|file| {
        file.canonicalize().ok() != catalog_path.canonicalize().ok()
            && !is_test_source(src_root, file)
    });
    files
}

/// What production code names, its `cfg(test)` half, comments and
/// declarations excluded: a static counts as crossed only when code under
/// `src/` names it.
fn production_names(src_root: &Path, catalog_path: &Path) -> Names {
    let mut names = Names::default();
    for file in crossing_files(src_root, catalog_path) {
        if let Ok(text) = std::fs::read_to_string(&file) {
            names.absorb(split_names(&text, &file.display().to_string()).production);
        }
    }
    names
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
        if is_test_source(src_root, &d.file) {
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
    production: &Names,
    armers: &Armers,
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
        if !production.idents.contains(&d.ident) {
            violations.push(format!(
                "{}: `{}` (\"{}\") has no production crossing under src/ — a seam nothing \
                 crosses is dead weight",
                catalog.label, d.ident, d.name
            ));
            continue;
        }
        let armed = armers.rust.idents.contains(&d.ident)
            || armers.rust.strings.contains(&d.name)
            || armers.case_names.contains(&d.name);
        if !armed {
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

fn engine_arming_roots() -> ArmingRoots {
    ArmingRoots {
        harness: vec![
            engine_dir().join("tests"),
            dst_dir().join("src"),
            dst_dir().join("tests"),
        ],
        src: vec![engine_dir().join("src")],
    }
}

fn cluster_arming_roots() -> ArmingRoots {
    ArmingRoots {
        harness: vec![cluster_dir().join("tests")],
        src: vec![cluster_dir().join("src")],
    }
}

fn engine_catalog_path() -> PathBuf {
    engine_dir().join("src/seams/catalog.rs")
}

fn cluster_catalog_path() -> PathBuf {
    cluster_dir().join("src/seams.rs")
}

/// The seams crate's decision installers, `.name(` for every `pub fn` taking
/// `&'static self`; only arming text may call one.
fn arming_calls() -> Vec<String> {
    let lib = seams_dir().join("src/lib.rs");
    let text = std::fs::read_to_string(&lib)
        .unwrap_or_else(|e| panic!("seams crate {} is unreadable: {e}", lib.display()));
    let mut calls = Vec::new();
    let mut from = 0;
    while let Some(rel) = text[from..].find("pub fn ") {
        let at = from + rel + "pub fn ".len();
        let name: String = text[at..]
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        let signature_end = text[at..].find('{').map_or(text.len(), |i| at + i);
        if text[at..signature_end].contains("&'static self") {
            calls.push(name);
        }
        from = at;
    }
    calls
}

#[test]
fn catalogs_are_complete_unique_and_used() {
    let engine_catalog_path = engine_catalog_path();
    let cluster_catalog_path = cluster_catalog_path();

    let engine = parse_catalog(
        &engine_catalog_path,
        &engine_dir().join("src"),
        "omnigraph::seams::catalog",
    );
    let cluster = parse_catalog(
        &cluster_catalog_path,
        &cluster_dir().join("src"),
        "omnigraph_cluster::seams::catalog",
    );

    let engine_armers = armers(&engine_arming_roots(), Some(&gqt_cases_dir()));
    let cluster_armers = armers(&cluster_arming_roots(), None);

    let engine_src = production_names(&engine_dir().join("src"), &engine_catalog_path);
    let cluster_src = production_names(&cluster_dir().join("src"), &cluster_catalog_path);

    let mut violations = Vec::new();
    check_catalog(&engine, &engine_src, &engine_armers, &mut violations);
    check_catalog(&cluster, &cluster_src, &cluster_armers, &mut violations);
    check_declaration_placement(
        &engine,
        &engine_catalog_path,
        &engine_dir().join("src"),
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

#[test]
fn arming_files_never_cross() {
    let engine_src = engine_dir().join("src");
    let cluster_src = cluster_dir().join("src");
    let crossing: BTreeSet<PathBuf> = crossing_files(&engine_src, &engine_catalog_path())
        .into_iter()
        .chain(crossing_files(&cluster_src, &cluster_catalog_path()))
        .collect();
    let arming: BTreeSet<PathBuf> = arming_files(&engine_arming_roots())
        .into_iter()
        .chain(arming_files(&cluster_arming_roots()))
        .collect();
    let both: Vec<String> = crossing
        .intersection(&arming)
        .map(|file| file.display().to_string())
        .collect();
    assert!(
        both.is_empty(),
        "a file that crosses a seam must not also count as arming it, or the arming check \
         is satisfied by the crossing itself:\n{}",
        both.join("\n")
    );
    for (root, what) in [
        (engine_src.clone(), "the engine's in-source test modules"),
        (dst_dir(), "the DST harness"),
        (cluster_dir().join("tests"), "the cluster integration tests"),
    ] {
        assert!(
            arming.iter().any(|file| file.starts_with(&root)),
            "{what} ({}) are in the arming set",
            root.display()
        );
    }
    let installers = arming_calls();
    assert!(
        installers.contains(&"fire_always".into()) && installers.contains(&"hold".into()),
        "the seams crate's installers are read from its source: {installers:?}"
    );
    let leaked: Vec<String> = crossing
        .iter()
        .filter_map(|file| {
            let text = std::fs::read_to_string(file).ok()?;
            let production = split_names(&text, &file.display().to_string()).production;
            let call = production.static_calls.into_iter().find(|call| {
                call.rsplit_once('.')
                    .is_some_and(|(_, method)| installers.contains(&method.to_string()))
            })?;
            Some(format!("{}: {call}", file.display()))
        })
        .collect();
    assert!(
        leaked.is_empty(),
        "production code never installs a decision on a seam static; a `cfg(test)` item \
         leaked into the crossing set:\n{}",
        leaked.join("\n")
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
    let names = |rust: &str| split_names(rust, "fixture").production;
    let mut violations = Vec::new();
    check_catalog(
        &catalog,
        &names("fn f() { fail(&catalog::A); AB; }"),
        &Armers {
            rust: names("fn t() { let _ = (\"x.b\", A); }"),
            case_names: BTreeSet::new(),
        },
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
    let unarmed = |armers: &Armers| {
        let mut violations = Vec::new();
        check_catalog(
            &catalog,
            &names(
                "fn f() { fail(&catalog::A); fail(&catalog::B); fail(&catalog::C); fail(&catalog::D); }",
            ),
            armers,
            &mut violations,
        );
        violations.retain(|v| v.contains("never armed"));
        violations
    };
    let never_armed = |ident: &str, name: &str| {
        format!("fixture: `{ident}` (\"{name}\") is crossed but never armed by a test or a case")
    };
    let all_unarmed = [
        never_armed("A", "x.a"),
        never_armed("B", "x.b"),
        never_armed("C", "x.c"),
        never_armed("D", "x.d"),
    ];
    assert_eq!(
        unarmed(&Armers {
            rust: Names::default(),
            case_names: BTreeSet::new(),
        }),
        all_unarmed,
        "a crossing is not arming: with an empty arming corpus every crossed seam is reported"
    );
    assert_eq!(
        unarmed(&Armers {
            rust: names("fn t() { A.fire_always(); let w = \"x.b\"; catalog::C.hold(); }"),
            case_names: ["x.d".to_string()].into(),
        }),
        [] as [String; 0],
        "an ident, a name as a string literal (the DST spelling) and a case `at` name each arm"
    );
    assert_eq!(
        unarmed(&Armers {
            rust: names("/// A and \"x.a\"\nfn t() { AB; let w = \"x.bb\"; /* C */ // \"x.c\"\n }"),
            case_names: ["\"x.d\"".to_string()].into(),
        }),
        all_unarmed,
        "a longer ident, a longer string, a doc comment, a block comment, a line comment and a \
         quoted case name arm nothing"
    );
    assert_eq!(
        seam_at_names(
            "--- seed\nat: not.a.seam\n--- seam\nat: x.a\noccurrence: 1\n\
             --- seam # a header argument the runner refuses\nat: x.b\n\
             --- seam\n\"at\": x.c\n--- seam\n{at: x.d, occurrence: 1, action: fail, scope: next_step}\n\
             --- seam\nat: x.e\t# tab comment\n--- mutate\nquery q() { at: x.f }\n\
             --- seam\nat: [\n--- seam\nat: 'x.g' # quoted\n"
        ),
        ["x.a", "x.c", "x.d", "x.e", "x.g"],
        "a bare `--- seam` section's body is decoded as YAML like the runner's: a quoted key, a \
         flow mapping, a tab before a comment and a quoted scalar all name the seam; a header \
         argument, a query body and an undecodable body name nothing"
    );
    let split = split_names(
        "fn a() { fail(&A); let s = \"F.fire_always()\"; pool.install(|| work()); }\n\
         /// mentions G in a doc comment\n\
         #[cfg(all(\n    test,\n    feature = \"failpoints\",\n    not(target_arch = \"wasm32\")\n))]\n\
         mod tests {\n    /* { */\n    fn arm() { X.fire_always(); assert!(H.hold(), \"x.h\"); }\n}\n\
         fn after() { fail(&A); }\n\
         #[cfg(any(test, feature = \"failpoints\"))]\nmod maybe { fn m() { M.fire_always(); } }\n\
         #[cfg(any(test, all(test, feature = \"x\")))]\nmod only_tests { fn o() { O; } }\n\
         #[cfg(not(test))]\nmod live { fn c() { C; } }\n\
         impl Z {\n    #[cfg(test)]\n    fn helper(&self) { D.panic_at(); }\n    fn keep(&self) { E; }\n}\n\
         struct W {\n    #[cfg(test)]\n    probe: P,\n    keep: K,\n}\n\
         #[cfg(test)]\nuse x::Y;\n\
         decide_seam! {\n    pub static DECLARED = (\"x.declared\", Mutation, [Fail]);\n}\n",
        "fixture",
    );
    let has = |names: &Names, ident: &str| names.idents.contains(ident);
    assert!(
        ["A", "pool", "M", "C", "E", "K", "fail"]
            .iter()
            .all(|i| has(&split.production, i))
            && ["X", "H", "O", "D", "P", "Y"]
                .iter()
                .all(|i| has(&split.tests, i))
            && ["X", "H", "O", "D", "P", "Y", "G", "DECLARED"]
                .iter()
                .all(|i| !has(&split.production, i))
            && ["A", "M", "C", "E", "K"]
                .iter()
                .all(|i| !has(&split.tests, i)),
        "a wrapped `cfg(all(test, …))`, `cfg(any(test, all(test, …)))`, a `cfg(test)` method, \
         field and `use` are test code; `cfg(any(test, feature))`, `cfg(not(test))` and the rest \
         are production; a doc comment, a block comment and a `decide_seam!` declaration name \
         nothing: {:?} / {:?}",
        split.production.idents,
        split.tests.idents
    );
    let calls = |calls: &[&str]| calls.iter().map(|s| s.to_string()).collect::<BTreeSet<_>>();
    assert!(
        split.production.strings.contains("F.fire_always()")
            && split.production.static_calls == calls(&["M.fire_always"])
            && split.tests.strings.contains("x.h")
            && split.tests.static_calls == calls(&["X.fire_always", "H.hold", "D.panic_at"]),
        "a string naming an installer and a lowercase receiver are not installer calls; \
         `X.fire_always()`, `D.panic_at()` and `H.hold()` inside `assert!` are test-side ones, \
         and `M.fire_always()` under `cfg(any(test, feature))` is production's: {:?} / {:?}",
        split.production.static_calls,
        split.tests.static_calls
    );
    let root = Path::new("/x/tests/omnigraph/crates/omnigraph/src");
    assert!(
        is_test_source(root, &root.join("db/manifest/tests.rs"))
            && is_test_source(root, &root.join("db/manifest/upgrade/tests/a.rs"))
            && is_test_source(root, &root.join("table_store/staged_tests.rs"))
            && !is_test_source(root, &root.join("exec/merge.rs"))
            && !is_test_source(root, &root.join("db/contests.rs")),
        "a test module is `tests/…`, `tests.rs` or `…_tests.rs` under the root; an ancestor \
         named `tests` or a `contests.rs` is not one"
    );
    assert_eq!(
        unindexable_declarations(
            "pub static A: DecideSeam = Seam::decide(\"x.a\", Op::Mutation, &[Effect::Fail], Global::new());\n\
             static PRIVATE: DecideSeam = Seam::decide(\"x.p\", Op::Mutation, &[Effect::Fail], Global::new());\n\
             pub(crate) static CRATE: DecideSeam =\n    Seam::decide(\"x.c\", Op::Mutation, &[Effect::Fail], Global::new());\n\
             crate::seams::decide_seam! {\n    pub static D = (\"x.d\", Mutation, [Fail]);\n}\n\
             crate::seams::decide_seam! {\n    static E = (\"x.e\", Mutation, [Fail]);\n}\n\
             pub static S: DecideSeam = Seam::decide_with_store(\"x.s\", Op::Mutation, &[Effect::Fail], &[StoreEffect::Misdirect], \"o/*\", Global::new());\n"
        ),
        [1, 2, 4, 8, 11],
        "every hand-written `Seam::decide` or `Seam::decide_with_store` and every macro body without `pub static` is reported by line; the macro `pub static` is not"
    );
    assert_eq!(
        catalog_declarations(
            "omnigraph_seams::catalog! {\n    crate::x::A,\n}\n\
             decide_seam! {\n    pub static UNINDEXED = (\"x.u\", Mutation, [Fail]);\n}\n\
             pub static WITH_STORE: DecideSeam = Seam::decide_with_store(\"x.w\", Op::Mutation, &[Effect::Fail], &[StoreEffect::Misdirect], \"o/*\", Global::new());\n"
        ),
        [4, 7],
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
