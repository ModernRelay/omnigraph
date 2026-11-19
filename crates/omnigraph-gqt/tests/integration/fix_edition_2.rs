//! `omnigraph lint --fix` over the edition-2 corpus: the printed fixes alone
//! respell edition-2 text into edition 3 (the GQ 3 RFC, section 6, "The
//! respelling map" and "Stored sources at upgrade"), and the respelled text
//! means what the hand rewrite in this tree means.
//!
//! It reads the tree of the pinned edition-2 commit
//! (`EDITION_2_SOURCE_COMMIT` in `ci.yml`) under `OMNIGRAPH_EDITION_2_TREE`:
//! every `.gqt` case of the GQT corpus and of the benchmark catalog (a
//! workload with the schema of the fixture `benchmarks.yaml` pairs it with)
//! and every `.gq` file beside a `.pg` of the same stem. Each query step
//! edition 2 ran (not one it refused) is one text, given to
//! [`fix_source`] with its case's schema and review fixes applied, as
//! `lint --fix --review-fixes` would. A text whose refusals all print fixes
//! must converge to a compiling text; one stuck at a refusal with no fix is
//! counted by code.
//!
//! Every declaration of a text that compiles, as written or fixed, is then
//! paired with the hand-rewritten counterpart in this tree: the case of the
//! same path, or of the same file name when the path moved (`cases/v2/` was
//! folded into `cases/`), and the step declaring the same name at the same
//! occurrence, counting the steps each side ran. Their typed IR, each
//! compiled against its own case's schema, must be equal, or the difference
//! must be a row of `lint-fix-edition-2.tsv` naming one of [`REASONS`]; a
//! row that matches no difference fails too.
//!
//! Without the tree the test prints a skip line; with
//! `OMNIGRAPH_REQUIRE_EDITION_2=1`, as the `Cross-Edition Comparison` job
//! sets, it fails instead.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use omnigraph_compiler::catalog::{Catalog, build_catalog};
use omnigraph_compiler::ir::core::CoreDecl;
use omnigraph_compiler::query::check::{check_source, check_source_all};
use omnigraph_compiler::schema::parser::parse_schema;
use omnigraph_compiler::{FixOptions, FixStatus, fix_source};

/// Why a respelled declaration may compile to another typed IR than its
/// hand rewrite, each naming the RFC rule or the edit behind it. A key
/// starting `open:` names a difference no RFC rule sanctions: the test
/// prints it as a warning rather than failing, and it leaves this list when
/// the fix or the RFC changes.
const REASONS: &[(&str, &str)] = &[
    (
        "map.not-coalesce",
        "Section 6's map writes a filter-only negated block as `not coalesce(φ, false)`, or `not (φ)` where φ cannot be null; the printed fix writes the first, the hand rewrite the second, with the same rows.",
    ),
    (
        "map.ranking-normalized",
        "Section 6's ranking rows hold after its normalisation: N7 makes `k: 60` on `rrf` the default, and the printed fix and the hand rewrite differ only in a rank key that a leading equal key decides.",
    ),
    (
        "hand.renamed-variables",
        "The hand rewrite of a body of several statements reused one variable name in each group, where the group rule's fix binds a fresh one per group; the typed IR differs in the names only.",
    ),
    (
        "open:fix-conjunct-order",
        "Applied pass by pass, the printed fixes of a property brace and of filters among nested blocks write the `where` conjuncts in another order than section 6's map (the brace's conjuncts leading, the rest in written order); the hand rewrite follows the map.",
    ),
];

const TABLE: &str = "lint-fix-edition-2.tsv";

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

/// One `.gqt` file's sections, `(header, body)`, in order.
fn sections(text: &str) -> Vec<(String, String)> {
    let mut sections: Vec<(String, String)> = Vec::new();
    for line in text.lines() {
        if let Some(header) = line.strip_prefix("--- ") {
            sections.push((header.trim().to_string(), String::new()));
        } else if let Some((_, body)) = sections.last_mut() {
            body.push_str(line);
            body.push('\n');
        }
    }
    sections
}

/// The query sources of the steps a case ran: a `query` or `mutate` section
/// holding declarations, whose expectation is not `expect error`.
fn ran_steps(sections: &[(String, String)]) -> Vec<String> {
    let starts_a_declaration = |body: &str| {
        let body = body
            .lines()
            .map(str::trim_start)
            .find(|l| !l.is_empty() && !l.starts_with("//"))
            .unwrap_or_default();
        body.starts_with("query ") || body.starts_with("mutation ")
    };
    (0..sections.len())
        .filter(|&i| {
            let (h, b) = &sections[i];
            let step = h == "query"
                || h.starts_with("query ")
                || h == "mutate"
                || h.starts_with("mutate ");
            let refused = sections[i + 1..]
                .iter()
                .find(|(h, _)| h != "params")
                .is_some_and(|(h, _)| h.starts_with("expect error"));
            step && starts_a_declaration(b) && !refused
        })
        .map(|i| sections[i].1.clone())
        .collect()
}

/// One file of text: its path relative to its tree, its schema and the
/// sources of the steps it ran.
struct Unit {
    path: String,
    schema: Option<String>,
    steps: Vec<String>,
}

fn schema_of(sections: &[(String, String)]) -> Option<String> {
    sections
        .iter()
        .find(|(h, _)| h == "schema")
        .map(|(_, b)| b.clone())
}

/// Every unit of a tree: GQT cases, benchmark workloads with their
/// fixture's schema and fixtures themselves, and `.gq` files beside a `.pg`.
fn units(root: &Path) -> Vec<Unit> {
    let relative = |p: &Path| {
        p.strip_prefix(root)
            .unwrap_or(p)
            .to_string_lossy()
            .into_owned()
    };
    let mut out = Vec::new();
    let mut cases = Vec::new();
    for dir in [
        "crates/omnigraph-gqt/cases",
        "crates/omnigraph-gqt/cases_slow",
        "benchmarks/fixtures",
    ] {
        files(&root.join(dir), "gqt", &mut cases);
    }
    cases.sort();
    for path in cases {
        let text = std::fs::read_to_string(&path).unwrap_or_default();
        let sections = sections(&text);
        out.push(Unit {
            path: relative(&path),
            schema: schema_of(&sections),
            steps: ran_steps(&sections),
        });
    }
    // A workload has no schema; `benchmarks.yaml` pairs it with a fixture.
    let catalog =
        std::fs::read_to_string(root.join("benchmarks/benchmarks.yaml")).unwrap_or_default();
    let mut fixture_of: BTreeMap<String, String> = BTreeMap::new();
    let mut fixture = None;
    for line in catalog.lines() {
        let line = line.trim().trim_start_matches("- ");
        if line.starts_with("id:") {
            fixture = None;
        } else if let Some(f) = line.strip_prefix("fixture:") {
            // A registered fixture is a mapping with no schema in the tree.
            fixture = Some(f.trim().to_string()).filter(|f| !f.is_empty());
        } else if let (Some(w), Some(f)) = (line.strip_prefix("workload: "), &fixture) {
            fixture_of.entry(w.trim().to_string()).or_insert(f.clone());
        }
    }
    let mut workloads = Vec::new();
    files(&root.join("benchmarks/workloads"), "gqt", &mut workloads);
    workloads.sort();
    for path in workloads {
        let name = format!("workloads/{}", path.file_name().unwrap().to_string_lossy());
        let schema = fixture_of.get(&name).and_then(|f| {
            let text = std::fs::read_to_string(root.join("benchmarks").join(f)).ok()?;
            schema_of(&sections(&text))
        });
        let text = std::fs::read_to_string(&path).unwrap_or_default();
        out.push(Unit {
            path: relative(&path),
            schema,
            steps: ran_steps(&sections(&text)),
        });
    }
    let mut stored = Vec::new();
    for dir in ["crates/omnigraph/tests/fixtures", "docs/user"] {
        files(&root.join(dir), "gq", &mut stored);
    }
    stored.sort();
    for path in stored {
        let Ok(schema) = std::fs::read_to_string(path.with_extension("pg")) else {
            continue;
        };
        out.push(Unit {
            path: relative(&path),
            schema: Some(schema),
            steps: vec![std::fs::read_to_string(&path).unwrap_or_default()],
        });
    }
    out
}

fn catalog_of(schema: &str) -> Option<Catalog> {
    build_catalog(&parse_schema(schema).ok()?).ok()
}

/// `source` with section 6's keyed-insert row applied: each `insert T { … }`
/// over a type with `@key` written `replace T { … }`, which means what
/// edition 2's keyed `insert` meant. No refusal prints this fix, since
/// `insert` is edition-3 text too (a strict insert that fails with
/// `key_conflict` on an existing key), so `lint --fix` leaves it; the test
/// applies the row to tell that difference from any other.
fn keyed_insert_as_replace(catalog: &Catalog, source: &str) -> Option<String> {
    let keyed = |name: &str| {
        catalog
            .node_types
            .get(name)
            .is_some_and(|t| t.key.is_some())
            || catalog
                .edge_types
                .get(name)
                .is_some_and(|t| t.key.is_some())
    };
    let mut out = String::with_capacity(source.len());
    let mut rest = source;
    let mut changed = false;
    while let Some(at) = rest.find("insert") {
        let before = rest[..at].chars().next_back();
        let after = &rest[at + "insert".len()..];
        let name_start = after.len() - after.trim_start().len();
        let name: String = after[name_start..]
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        let statement = before.is_none_or(|c| !(c.is_ascii_alphanumeric() || c == '_'))
            && name_start > 0
            && keyed(&name)
            && after[name_start + name.len()..]
                .trim_start()
                .starts_with('{');
        out.push_str(&rest[..at]);
        out.push_str(if statement { "replace" } else { "insert" });
        changed |= statement;
        rest = after;
    }
    out.push_str(rest);
    changed.then_some(out)
}

/// Each declaration of `source` that compiles, by name, in source order.
fn typed(catalog: &Catalog, source: &str) -> Vec<(String, Result<CoreDecl, String>)> {
    match check_source(catalog, source) {
        Ok(checked) => checked
            .into_iter()
            .map(|(name, r)| {
                (
                    name,
                    r.map(|c| c.core)
                        .map_err(|d| format!("{}: {}", d.code, d.message)),
                )
            })
            .collect(),
        Err(d) => vec![(String::new(), Err(format!("{}: {}", d.code, d.message)))],
    }
}

/// The names `source` declares, in order, read from its text so that a
/// step whose fix is stuck keeps its place: each `query` or `mutation`
/// keyword followed by a name and `(`.
fn declared_names(source: &str) -> Vec<String> {
    let mut names = Vec::new();
    let words: Vec<(usize, &str)> = source
        .match_indices(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .scan(0, |start, (at, sep)| {
            let word = (*start, &source[*start..at]);
            *start = at + sep.len();
            Some(word)
        })
        .filter(|(_, w)| !w.is_empty())
        .collect();
    for pair in words.windows(2) {
        let [(_, keyword), (at, name)] = pair else {
            continue;
        };
        let after = source[at + name.len()..].trim_start();
        if matches!(*keyword, "query" | "mutation") && after.starts_with('(') {
            names.push(name.to_string());
        }
    }
    names
}

/// One step's declarations: their names, read from the step's text, and
/// the typed IR of each in the text that was compiled (the respelled one;
/// empty when the fix was stuck).
type Declarations = BTreeMap<(String, usize), (Result<CoreDecl, String>, String)>;

/// The declarations of a unit's steps keyed by `(name, occurrence)`, each
/// with its typed IR against the unit's schema and the text it came from.
fn declarations(catalog: &Catalog, steps: &[(String, String)]) -> Declarations {
    let mut seen: BTreeMap<String, usize> = BTreeMap::new();
    let mut out = BTreeMap::new();
    for (written, compiled) in steps {
        let typed: BTreeMap<String, Result<CoreDecl, String>> =
            typed(catalog, compiled).into_iter().collect();
        for name in declared_names(written) {
            let n = seen.entry(name.clone()).or_default();
            *n += 1;
            let ir = typed
                .get(&name)
                .cloned()
                .unwrap_or_else(|| Err("not compiled".to_string()));
            out.insert((name, *n), (ir, compiled.clone()));
        }
    }
    out
}

/// The counterpart in this tree of an edition-2 path: the same path, else
/// the one file of the same name (preferring the path less a `v2/` part).
fn counterpart<'a>(path: &str, current: &'a BTreeMap<String, Unit>) -> Option<&'a Unit> {
    if let Some(unit) = current.get(path) {
        return Some(unit);
    }
    let unversioned = path.replace("/v2/", "/");
    if let Some(unit) = current.get(&unversioned) {
        return Some(unit);
    }
    let name = Path::new(path).file_name()?;
    let mut same: Vec<&Unit> = current
        .values()
        .filter(|u| Path::new(&u.path).file_name() == Some(name))
        .collect();
    (same.len() == 1).then(|| same.remove(0))
}

/// A table row: case, declaration, occurrence, aspect, reason, why.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Key {
    case: String,
    declaration: String,
    occurrence: usize,
    aspect: String,
}

fn table() -> BTreeMap<Key, String> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(TABLE);
    let text = std::fs::read_to_string(&path).unwrap_or_default();
    let mut rows = BTreeMap::new();
    for (n, line) in text.lines().enumerate() {
        if line.starts_with('#') || line.trim().is_empty() {
            continue;
        }
        let fields: Vec<&str> = line.split('\t').collect();
        assert_eq!(fields.len(), 6, "{TABLE}:{}: six fields: {line}", n + 1);
        let reason = fields[4];
        assert!(
            REASONS.iter().any(|(key, _)| *key == reason),
            "{TABLE}:{}: unknown reason `{reason}`",
            n + 1
        );
        assert!(!fields[5].trim().is_empty(), "{TABLE}:{}: no why", n + 1);
        let key = Key {
            case: fields[0].to_string(),
            declaration: fields[1].to_string(),
            occurrence: fields[2].parse().expect("occurrence is a number"),
            aspect: fields[3].to_string(),
        };
        assert!(
            rows.insert(key, reason.to_string()).is_none(),
            "{TABLE}:{}: repeated row",
            n + 1
        );
    }
    rows
}

#[derive(Default)]
struct Counts {
    texts: usize,
    no_schema: usize,
    compiled_as_written: usize,
    canonical_only: usize,
    fixed: usize,
    fixed_without_review: usize,
    stuck: BTreeMap<String, usize>,
    stuck_without_review: BTreeMap<String, usize>,
    does_not_converge: Vec<String>,
    declarations: usize,
    no_counterpart: usize,
    ir_equal: usize,
    keyed_insert: usize,
    listed: usize,
}

#[test]
fn printed_fixes_respell_the_edition_2_corpus_into_its_hand_rewrite() {
    let Some(tree) = std::env::var_os("OMNIGRAPH_EDITION_2_TREE").map(PathBuf::from) else {
        assert!(
            std::env::var("OMNIGRAPH_REQUIRE_EDITION_2").as_deref() != Ok("1"),
            "OMNIGRAPH_REQUIRE_EDITION_2=1 needs OMNIGRAPH_EDITION_2_TREE, the edition-2 checkout"
        );
        println!("skipped: OMNIGRAPH_EDITION_2_TREE names no edition-2 tree");
        return;
    };
    let here = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let current: BTreeMap<String, Unit> = units(&here)
        .into_iter()
        .map(|u| (u.path.clone(), u))
        .collect();
    let rows = table();
    let mut matched: BTreeSet<Key> = BTreeSet::new();
    let mut unlisted: Vec<String> = Vec::new();
    let mut warnings: Vec<String> = Vec::new();
    let mut counts = Counts::default();
    let edition_2 = units(&tree);
    assert!(
        edition_2.len() > 100,
        "{} holds {} units: not the edition-2 tree",
        tree.display(),
        edition_2.len()
    );
    for unit in edition_2 {
        let catalog = unit.schema.as_deref().and_then(catalog_of);
        let Some(catalog) = catalog else {
            counts.no_schema += unit.steps.len();
            continue;
        };
        let mut respelled = Vec::new();
        for step in &unit.steps {
            counts.texts += 1;
            let as_written = check_source_all(&catalog, step).is_empty();
            let machine = fix_source(&catalog, step, FixOptions::default());
            match machine.report.status {
                FixStatus::Unfixed => {
                    let code = machine.report.unfixed[0].code.to_string();
                    *counts.stuck_without_review.entry(code).or_default() += 1;
                }
                FixStatus::Fixed if !as_written => counts.fixed_without_review += 1,
                _ => {}
            }
            let out = fix_source(&catalog, step, FixOptions { review: true });
            match out.report.status {
                FixStatus::Unchanged => {
                    counts.compiled_as_written += 1;
                    respelled.push(step.clone());
                }
                FixStatus::Fixed => {
                    if as_written {
                        counts.compiled_as_written += 1;
                        counts.canonical_only += 1;
                    } else {
                        counts.fixed += 1;
                    }
                    respelled.push(out.fixed.unwrap());
                }
                FixStatus::Unfixed => {
                    let code = out.report.unfixed[0].code.to_string();
                    if std::env::var("OMNIGRAPH_FIX_EDITION_2_SHOW").as_deref() == Ok("1") {
                        println!(
                            "--- stuck {} at {code}: {}",
                            unit.path, out.report.unfixed[0].message
                        );
                    }
                    *counts.stuck.entry(code).or_default() += 1;
                    // Keep the occurrences of later steps aligned.
                    respelled.push(String::new());
                }
                FixStatus::DoesNotConverge => {
                    counts.does_not_converge.push(format!(
                        "{}: does not converge at pass {:?}",
                        unit.path, out.report.stopped_at_pass
                    ));
                    respelled.push(String::new());
                }
            }
        }
        let Some(other) = counterpart(&unit.path, &current) else {
            counts.no_counterpart += respelled.iter().filter(|s| !s.is_empty()).count();
            continue;
        };
        let Some(other_catalog) = other.schema.as_deref().and_then(catalog_of) else {
            continue;
        };
        let theirs = declarations(
            &other_catalog,
            &other
                .steps
                .iter()
                .map(|s| (s.clone(), s.clone()))
                .collect::<Vec<_>>(),
        );
        let ours = declarations(
            &catalog,
            &unit
                .steps
                .iter()
                .cloned()
                .zip(respelled)
                .collect::<Vec<_>>(),
        );
        for ((name, occurrence), (ir, text)) in ours {
            let Ok(ir) = ir else { continue };
            counts.declarations += 1;
            let Some(hand) = theirs.get(&(name.clone(), occurrence)) else {
                counts.no_counterpart += 1;
                continue;
            };
            let (hand, hand_text) = hand;
            let aspect = match hand {
                Ok(hand) if *hand == ir => {
                    counts.ir_equal += 1;
                    continue;
                }
                Ok(hand) => {
                    // Section 6's keyed-insert row, which no refusal prints.
                    let replaced = keyed_insert_as_replace(&catalog, &text)
                        .map(|t| typed(&catalog, &t))
                        .and_then(|t| t.into_iter().find(|(n, _)| *n == name))
                        .and_then(|(_, ir)| ir.ok());
                    if replaced.as_ref() == Some(hand) {
                        counts.keyed_insert += 1;
                        continue;
                    }
                    "ir"
                }
                Err(_) => "counterpart-refused",
            };
            let key = Key {
                case: unit.path.clone(),
                declaration: name.clone(),
                occurrence,
                aspect: aspect.to_string(),
            };
            if let Some(reason) = rows.get(&key) {
                counts.listed += 1;
                if reason.starts_with("open:") {
                    warnings.push(format!(
                        "{}\t{name}\t{occurrence}\t{aspect}\t{reason}",
                        unit.path
                    ));
                }
                matched.insert(key);
            } else {
                // `OMNIGRAPH_FIX_EDITION_2_SHOW=1` prints both texts.
                if std::env::var("OMNIGRAPH_FIX_EDITION_2_SHOW").as_deref() == Ok("1") {
                    println!(
                        "--- {}\t{name}\t{occurrence}\t{aspect}\n{text}\n--- hand rewrite\n{hand_text}",
                        unit.path
                    );
                }
                unlisted.push(format!("{}\t{name}\t{occurrence}\t{aspect}", unit.path));
            }
        }
    }
    let stale: Vec<&Key> = rows.keys().filter(|k| !matched.contains(k)).collect();
    let stuck: usize = counts.stuck.values().sum();
    let stuck_without_review: usize = counts.stuck_without_review.values().sum();
    println!(
        "edition-2 texts: {}; schema refused: {}; compiled as written: {} (of which canonical forms respelled: {}); fixed and compiled: {} ({} without review fixes); stuck: {stuck} {:?} (without review fixes: {stuck_without_review} {:?}); does not converge: {}",
        counts.texts,
        counts.no_schema,
        counts.compiled_as_written,
        counts.canonical_only,
        counts.fixed,
        counts.fixed_without_review,
        counts.stuck,
        counts.stuck_without_review,
        counts.does_not_converge.len(),
    );
    println!(
        "declarations compiled: {}; IR equal to the hand rewrite: {}; keyed `insert` left as written (the hand rewrite's `replace`): {}; listed differences: {}; no counterpart: {}",
        counts.declarations,
        counts.ir_equal,
        counts.keyed_insert,
        counts.listed,
        counts.no_counterpart
    );
    for warning in &warnings {
        println!("warning: a difference no RFC rule sanctions: {warning}");
    }
    assert!(
        counts.does_not_converge.is_empty(),
        "printed fixes that do not converge:\n{}",
        counts.does_not_converge.join("\n")
    );
    assert!(
        unlisted.is_empty(),
        "respelled declarations whose typed IR differs from the hand rewrite, not in {TABLE}:\n{}",
        unlisted.join("\n")
    );
    assert!(
        stale.is_empty(),
        "{TABLE} rows that match no difference: {stale:?}"
    );
}
