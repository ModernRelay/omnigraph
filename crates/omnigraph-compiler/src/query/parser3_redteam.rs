//! The red-team corpus of the GQ 3 RFC (section 10): agent-written
//! edition-3 texts under `testdata/gq3-redteam/corpus/`, each with its
//! expected outcome in its `// expect:` header.
//!
//! Every text is parsed as edition 3; a text expected to be refused may be
//! refused by the type checker too, where section 5 places some refusals.
//! For every refusal the runner plays the agent: it applies the diagnostic's
//! `suggestion` (byte-range edits), parses the result, and, when the header
//! names the fix it expected, checks that the repaired text is the intended
//! one: the original with one span replaced by the header's fix (ignoring
//! whitespace), or the same tree as the header's fix spliced over the
//! construct the refusal names. The repaired trees are pinned in
//! `fixed-trees.txt` (`redteam_suggestions_reparse_to_their_pinned_trees`;
//! regenerate with `GQ3_REDTEAM_BLESS=1`).
//!
//! `redteam_corpus_matches_its_pinned_mismatches` passes while the set of
//! mismatches equals `known-mismatches.txt`, a list that only shrinks, each
//! line with its reason. `redteam_corpus_strict` is the instrument: it fails
//! on any mismatch. Set `GQ3_REDTEAM_OUT=<dir>` to write the table and the
//! fix audit there.

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use super::ast3::*;
use super::diagnostic::TextEdit;
use super::parser::parse_query_diagnostic;
use super::parser3::{
    EditionLine, Refusal3, apply_edits, edition_line, edition_near_miss, parse_query3_detailed,
};
use super::print3::{self, Style};

/// The RFC's section 8 schema in today's `.pg` spelling (`implements`, no
/// analyzer or distance options), with `SourceRevision` and `Passage` for the
/// section 6 example.
const SCHEMA: &str = r#"
interface Named { name: String }
node Person implements Named { handle: String @key  name: String  age: I32?  city: String? }
node Company implements Named { slug: String @key  name: String }
node Doc { id: String @key  title: String @index  body: String @index  year: I32  tags: [String]  embedding: Vector(3)? }
node Collection { slug: String @key  min_docs: I32 }
node SourceRevision { path: String @key  matter_number: String }
node Passage { locator: String @key  text: String @index }
edge WorksAt: Person -> Company @card(0..1)
edge Knows: Person -> Person
edge Authored: Person -> Doc
edge Cites: Doc -> Doc
edge In: Doc -> Collection
edge HasPassage: SourceRevision -> Passage
"#;

/// What the type checker says about a text the front door lowers: `ok`, or
/// the first refusal's code and message.
fn typecheck(text: &str) -> String {
    use crate::catalog::build_catalog;
    use crate::schema::parser::parse_schema;
    let catalog = build_catalog(&parse_schema(SCHEMA).unwrap()).unwrap();
    let Ok(file) = parse_query_diagnostic(text) else {
        return String::new();
    };
    let crate::query::ast::FileBody::Queries(decls) = &file.body else {
        return "ok".to_string();
    };
    for d in decls {
        if let Err(e) = super::typecheck::typecheck_query_decl(&catalog, d) {
            return match e.diagnostic() {
                Some(d) => format!("{} {}", d.code.as_str(), d.message),
                None => e.to_string(),
            };
        }
    }
    "ok".to_string()
}

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("src/query/testdata/gq3-redteam")
}

#[derive(Debug, Clone, PartialEq)]
enum Expect {
    Accept,
    Refuse,
    RefuseWithFix(String),
}

#[derive(Debug, Clone)]
struct Case {
    name: String,
    kind: String,
    habit: Option<String>,
    expect: Expect,
    shape: Option<String>,
    returns: Option<String>,
    later: Option<String>,
    text: String,
}

fn header(content: &str, key: &str) -> Option<String> {
    let prefix = format!("// {key}: ");
    content
        .lines()
        .take_while(|l| l.starts_with("//"))
        .find_map(|l| l.strip_prefix(&prefix).map(|v| v.trim_end().to_string()))
}

fn load_case(path: &Path) -> Case {
    let content = std::fs::read_to_string(path).unwrap();
    let expect = header(&content, "expect").unwrap_or_else(|| panic!("{path:?}: no expect"));
    let expect = match expect.as_str() {
        "accept" => Expect::Accept,
        "refuse" => Expect::Refuse,
        other => Expect::RefuseWithFix(
            other
                .strip_prefix("refuse-with-fix:")
                .unwrap_or_else(|| panic!("{path:?}: bad expect {other}"))
                .replace("\\n", "\n"),
        ),
    };
    let raw = header(&content, "raw").as_deref() == Some("yes");
    let text = if raw {
        content.clone()
    } else {
        format!("gq 3;\n{content}")
    };
    Case {
        name: path.file_stem().unwrap().to_string_lossy().into_owned(),
        kind: header(&content, "kind").unwrap(),
        habit: header(&content, "habit"),
        expect,
        shape: header(&content, "shape"),
        returns: header(&content, "returns"),
        later: header(&content, "later"),
        text,
    }
}

fn files(dir: &Path) -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "gq"))
        .collect();
    out.sort();
    out
}

/// Parse-level outcome: a malformed edition line is refused first; edition 3
/// through its own parser; any other edition line (or none, for a raw file)
/// through the front door.
fn parse(text: &str) -> Result<Option<File3>, Refusal3> {
    if let Some(r) = edition_near_miss(text) {
        return Err(r);
    }
    match edition_line(text) {
        EditionLine::Present { edition: 3, .. } => parse_query3_detailed(text).map(Some),
        _ => parse_query_diagnostic(text)
            .map(|_| None)
            .map_err(Refusal3::from),
    }
}

fn norm(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn first_decl(file: &File3) -> Option<&Decl3> {
    match &file.body {
        Body3::Decls(d) => d.last(),
        Body3::Explain(d) => Some(d),
        Body3::Statement(_) => None,
    }
}

fn shape_of(file: &File3) -> (String, String) {
    let style = Style::default();
    let Some(d) = first_decl(file) else {
        return (String::new(), String::new());
    };
    match &d.body {
        DeclBody3::Read(r) => (
            r.clauses
                .iter()
                .map(|c| print3::clause(c, style))
                .collect::<Vec<_>>()
                .join(" | "),
            r.projections
                .iter()
                .map(|p| match &p.alias {
                    Some(a) => format!("{} as {a}", print3::expr(&p.expr)),
                    None => print3::expr(&p.expr),
                })
                .collect::<Vec<_>>()
                .join(" | "),
        ),
        DeclBody3::Write(w) => (
            w.groups
                .iter()
                .flat_map(|g| g.clauses.iter())
                .map(|c| print3::clause(c, style))
                .collect::<Vec<_>>()
                .join(" | "),
            String::new(),
        ),
    }
}

#[derive(Debug, Clone, PartialEq)]
enum FixVerdict {
    /// The diagnostic printed no suggestion (only prose, or nothing).
    NoFix,
    /// Applied, the suggestion's text does not parse.
    DoesNotParse(String),
    /// It parses, and it is not the text the header's fix intends.
    ChangesMeaning,
    /// It parses, and either is the intended text or no intended fix was
    /// given.
    Parses,
}

struct Row {
    case: Case,
    actual: Result<(), Refusal3>,
    pipeline: String,
    mismatch: Option<String>,
    fix: Option<FixVerdict>,
    applied: Option<String>,
    tree: Option<String>,
}

fn verdict_name(v: &FixVerdict) -> String {
    match v {
        FixVerdict::NoFix => "no fix".into(),
        FixVerdict::DoesNotParse(c) => format!("fix does not parse ({c})"),
        FixVerdict::ChangesMeaning => "fix changes meaning".into(),
        FixVerdict::Parses => "fix parses".into(),
    }
}

/// `s` without its `//` comment lines (a case's headers name its fix) and
/// without whitespace.
fn strip(s: &str) -> String {
    s.lines()
        .filter(|l| !l.trim_start().starts_with("//"))
        .flat_map(str::chars)
        .filter(|c| !c.is_whitespace())
        .collect()
}

/// Whether `repaired` is `original` with one span replaced by `fix`, ignoring
/// whitespace.
fn is_splice_of(original: &str, repaired: &str, fix: &str) -> bool {
    let (o, r, f) = (strip(original), strip(repaired), strip(fix));
    if r.len() < f.len() {
        return false;
    }
    (0..=r.len() - f.len()).any(|a| {
        r.is_char_boundary(a) && r.is_char_boundary(a + f.len()) && r[a..a + f.len()] == f && {
            let tail = r.len() - a - f.len();
            tail <= o.len()
                && a <= o.len() - tail
                && o.is_char_boundary(a)
                && o.is_char_boundary(o.len() - tail)
                && o[..a] == r[..a]
                && o[o.len() - tail..] == r[a + f.len()..]
        }
    })
}

/// `text` with whitespace removed and every `$name` renamed `$v1`, `$v2`, …
/// in order of first appearance.
fn canonical(text: &str) -> String {
    let mut names: Vec<String> = Vec::new();
    let mut out = String::new();
    let body: String = text
        .lines()
        .filter(|l| !l.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n");
    let mut chars = body.chars().peekable();
    while let Some(c) = chars.next() {
        if c.is_whitespace() {
            continue;
        }
        if c == '$' {
            let mut name = String::new();
            while let Some(&n) = chars.peek() {
                if n.is_ascii_alphanumeric() || n == '_' {
                    name.push(n);
                    chars.next();
                } else {
                    break;
                }
            }
            let i = names.iter().position(|n| *n == name).unwrap_or_else(|| {
                names.push(name.clone());
                names.len() - 1
            });
            out.push_str(&format!("$v{i}"));
        } else {
            out.push(c);
        }
    }
    out
}

fn tree(text: &str) -> Option<Option<File3>> {
    parse(text).ok()
}

/// Whether the suggestion's text is what the header's fix intends: the same
/// text up to whitespace, or the same tree as the fix spliced over the
/// construct (with the suggestion's other edits), or as the construct's text
/// replaced by the fix wherever it occurs.
fn is_intended(case: &Case, r: &Refusal3, repaired: &str, fix: &str) -> bool {
    // A deletion (`<empty>`) is checked by tree below: an empty splice would
    // admit any deletion.
    if !fix.is_empty() && is_splice_of(&case.text, repaired, fix) {
        return true;
    }
    let Some(got) = tree(repaired) else {
        return false;
    };
    let Some(span) = r.span else { return false };
    let mut edits: Vec<TextEdit> = r
        .edits()
        .iter()
        .filter(|e| e.end <= span.0 || e.start >= span.1)
        .cloned()
        .collect();
    edits.push(TextEdit {
        start: span.0,
        end: span.1,
        replacement: fix.to_string(),
    });
    let intended = apply_edits(&case.text, &edits);
    if tree(&intended).is_some_and(|t| t == got) {
        return true;
    }
    // A fix that invents a variable may pick another name than the header:
    // equal up to a renaming of variables is the intended text.
    if canonical(&intended) == canonical(repaired) {
        return true;
    }
    let construct = &case.text[span.0..span.1];
    if !construct.is_empty() && tree(&case.text.replace(construct, fix)).is_some_and(|t| t == got) {
        return true;
    }
    // The fix applied at every site the suggestion edits: each edit widened
    // to the word it sits in (`$P` → `$p` at the binding and in `return`).
    let word = |at: usize, forward: bool| {
        let b = case.text.as_bytes();
        let mut i = at;
        let ok = |c: u8| c.is_ascii_alphanumeric() || c == b'_' || c == b'$' || c == b'?';
        if forward {
            while i < b.len() && ok(b[i]) {
                i += 1;
            }
        } else {
            while i > 0 && ok(b[i - 1]) {
                i -= 1;
            }
        }
        i
    };
    let sites: Vec<TextEdit> = r
        .edits()
        .iter()
        .map(|e| TextEdit {
            start: word(e.start, false),
            end: word(e.end, true),
            replacement: fix.to_string(),
        })
        .collect();
    let overlapping = sites.windows(2).any(|w| w[1].start < w[0].end);
    !overlapping && strip(&apply_edits(&case.text, &sites)) == strip(repaired)
}

fn printed(file: &Option<File3>) -> String {
    match file {
        Some(f) => norm(&print3::print_file(f, Style::default())),
        None => "(edition 2)".to_string(),
    }
}

fn audit_fix(case: &Case, r: &Refusal3) -> (FixVerdict, Option<String>, Option<String>) {
    if r.edits().is_empty() {
        return (FixVerdict::NoFix, None, None);
    }
    let repaired = apply_edits(&case.text, r.edits());
    let got = match parse(&repaired) {
        Err(e) => {
            let code = e.diagnostic.code.as_str().to_string();
            return (
                FixVerdict::DoesNotParse(code.clone()),
                Some(repaired),
                Some(format!("refused {code}")),
            );
        }
        Ok(f) => f,
    };
    let tree = Some(printed(&got));
    if let Expect::RefuseWithFix(expected) = &case.expect {
        let with = if expected == "<empty>" {
            ""
        } else {
            expected.as_str()
        };
        if !is_intended(case, r, &repaired, with) {
            return (FixVerdict::ChangesMeaning, Some(repaired), tree);
        }
    }
    (FixVerdict::Parses, Some(repaired), tree)
}

/// The first type-check refusal of `text` against the corpus schema, for a
/// refusal section 5 places in the type checker (a reserved word as a bare
/// edge, infix `contains` over a list).
fn type_refusal(text: &str) -> Option<super::diagnostic::QueryDiagnostic> {
    use crate::catalog::build_catalog;
    use crate::schema::parser::parse_schema;
    let catalog = build_catalog(&parse_schema(SCHEMA).unwrap()).unwrap();
    let file = parse_query_diagnostic(text).ok()?;
    let crate::query::ast::FileBody::Queries(decls) = &file.body else {
        return None;
    };
    decls.iter().find_map(|d| {
        super::typecheck::typecheck_query_decl(&catalog, d)
            .err()
            .and_then(|e| e.diagnostic().cloned())
    })
}

fn run_case(case: Case) -> Row {
    // A text expected to be refused may be refused by the type checker,
    // where section 5 places some refusals; an accepted text is judged by
    // its parse alone, since the corpus is not written against one schema.
    let parsed = match parse(&case.text) {
        Ok(file) if case.expect != Expect::Accept => match type_refusal(&case.text) {
            // A type refusal's construct is the range its one edit replaces.
            Some(d) => Err(Refusal3 {
                span: match d.suggestion.as_ref().map(|s| s.edits.as_slice()) {
                    Some([e]) => Some((e.start, e.end)),
                    _ => None,
                },
                diagnostic: d,
            }),
            None => Ok(file),
        },
        other => other,
    };
    let pipeline = match parse_query_diagnostic(&case.text) {
        Ok(_) => format!("lowers; typecheck: {}", typecheck(&case.text)),
        Err(e) => format!("{} {}", e.code.as_str(), e.message),
    };
    let mut mismatch = None;
    let mut fix = None;
    let mut applied = None;
    let mut fixed_tree = None;
    match (&case.expect, &parsed) {
        (Expect::Accept, Ok(file)) => {
            if let Some(file) = file {
                let (clauses, returns) = shape_of(file);
                if let Some(want) = &case.shape {
                    if norm(want) != norm(&clauses) {
                        mismatch = Some(format!("shape: expected `{want}`, parsed `{clauses}`"));
                    }
                }
                if let Some(want) = &case.returns {
                    if norm(want) != norm(&returns) {
                        mismatch = Some(format!("returns: expected `{want}`, parsed `{returns}`"));
                    }
                }
            }
        }
        (Expect::Accept, Err(e)) => {
            mismatch = Some(format!(
                "refused, expected accept: {} {}",
                e.diagnostic.code.as_str(),
                e.diagnostic.message
            ))
        }
        (Expect::Refuse | Expect::RefuseWithFix(_), Ok(file)) => {
            let (clauses, returns) = file.as_ref().map(shape_of).unwrap_or_default();
            mismatch = Some(format!(
                "accepted, expected a refusal; parsed `{clauses}` returning `{returns}`"
            ));
        }
        (Expect::Refuse, Err(_)) => {}
        (Expect::RefuseWithFix(_), Err(_)) => {}
    }
    if let Err(d) = &parsed {
        let (v, t, tr) = audit_fix(&case, d);
        if matches!(case.expect, Expect::RefuseWithFix(_)) && v != FixVerdict::Parses {
            mismatch = Some(match &v {
                FixVerdict::NoFix => format!(
                    "no suggestion printed; fix text {:?}",
                    d.diagnostic.fix.as_deref().unwrap_or("")
                ),
                other => format!(
                    "{}: {}",
                    verdict_name(other),
                    t.as_deref()
                        .map(|t| norm(t.trim_start_matches("gq 3;")))
                        .unwrap_or_default()
                ),
            });
        }
        fix = Some(v);
        applied = t;
        fixed_tree = tr;
    }
    Row {
        actual: parsed.map(|_| ()),
        case,
        pipeline,
        mismatch,
        fix,
        applied,
        tree: fixed_tree,
    }
}

fn md_cell(s: &str) -> String {
    s.replace('|', "\\|").replace('\n', "\\n")
}

struct Report {
    rows: Vec<Row>,
    table: String,
    fixes: String,
    trees: String,
}

fn run_all() -> Report {
    let rows: Vec<Row> = files(&root().join("corpus"))
        .iter()
        .map(|p| run_case(load_case(p)))
        .collect();

    let mut table = String::from(
        "| case | kind | habit | expect | parse | code | message | printed fix | fix applied | front door | verdict |\n|---|---|---|---|---|---|---|---|---|---|---|\n",
    );
    for r in &rows {
        let expect = match &r.case.expect {
            Expect::Accept => "accept".to_string(),
            Expect::Refuse => "refuse".to_string(),
            Expect::RefuseWithFix(f) => format!("fix: `{}`", md_cell(f)),
        };
        let (parse, code, message, printed) = match &r.actual {
            Ok(()) => ("accept", String::new(), String::new(), String::new()),
            Err(e) => (
                "refuse",
                e.diagnostic.code.as_str().to_string(),
                md_cell(&e.diagnostic.message),
                e.diagnostic.fix.as_deref().map(md_cell).unwrap_or_default(),
            ),
        };
        let verdict = match &r.mismatch {
            None => "ok".to_string(),
            Some(m) => format!("**MISMATCH**: {}", md_cell(m)),
        };
        let later = r
            .case
            .later
            .as_deref()
            .map(|l| format!(" (RFC later: {})", md_cell(l)))
            .unwrap_or_default();
        let _ = writeln!(
            table,
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {}{} | {} |",
            r.case.name,
            r.case.kind,
            r.case.habit.as_deref().unwrap_or(""),
            expect,
            parse,
            code,
            message,
            if printed.is_empty() {
                String::new()
            } else {
                format!("`{printed}`")
            },
            r.fix.as_ref().map(verdict_name).unwrap_or_default(),
            md_cell(&r.pipeline),
            later,
            verdict,
        );
    }

    // The fix audit: every refusal, counted by verdict.
    let refusals: Vec<&Row> = rows.iter().filter(|r| r.actual.is_err()).collect();
    let count = |f: &dyn Fn(&FixVerdict) -> bool| {
        refusals
            .iter()
            .filter(|r| r.fix.as_ref().is_some_and(f))
            .count()
    };
    let generic = refusals
        .iter()
        .filter(|r| matches!(&r.actual, Err(e) if e.diagnostic.code.as_str() == "Q001"))
        .count();
    let mut fixes = String::new();
    let _ = writeln!(fixes, "refusals: {}", refusals.len());
    let _ = writeln!(
        fixes,
        "  generic Q001 (constructs named, no fix): {generic}"
    );
    let _ = writeln!(
        fixes,
        "  no suggestion printed: {}",
        count(&|v| *v == FixVerdict::NoFix)
    );
    let _ = writeln!(
        fixes,
        "  suggestion printed: {}",
        count(&|v| *v != FixVerdict::NoFix)
    );
    let _ = writeln!(
        fixes,
        "    applied, parses with the intended meaning: {}",
        count(&|v| *v == FixVerdict::Parses)
    );
    let _ = writeln!(
        fixes,
        "    applied, parses but is not the intended text: {}",
        count(&|v| *v == FixVerdict::ChangesMeaning)
    );
    let _ = writeln!(
        fixes,
        "    applied, does not parse: {}",
        count(&|v| matches!(v, FixVerdict::DoesNotParse(_)))
    );
    let _ = writeln!(fixes);
    for r in &refusals {
        let Err(e) = &r.actual else { continue };
        if let Some(v) = &r.fix {
            if matches!(v, FixVerdict::DoesNotParse(_) | FixVerdict::ChangesMeaning) {
                let _ = writeln!(
                    fixes,
                    "{}: {} `{}`\n  fix: {:?}\n  verdict: {}\n  repaired: {}\n",
                    r.case.name,
                    e.diagnostic.code.as_str(),
                    e.diagnostic.message,
                    e.diagnostic.fix,
                    verdict_name(v),
                    r.applied
                        .as_deref()
                        .map(|t| norm(t.trim_start_matches("gq 3;")))
                        .unwrap_or_default()
                );
            }
        }
    }

    // The pinned repaired trees: every refusal's code, its suggestion applied
    // and reparsed, printed.
    let mut trees = String::from(
        "# Each refusal of the red-team corpus: its code, and the tree its suggestion\n# reparses to (printed), pinned by parser3_redteam::redteam_suggestions_reparse_to_their_pinned_trees.\n# Regenerate with GQ3_REDTEAM_BLESS=1.\n",
    );
    for r in &rows {
        let Err(e) = &r.actual else { continue };
        let _ = writeln!(
            trees,
            "{}\t{}\t{}",
            r.case.name,
            e.diagnostic.code.as_str(),
            r.tree.as_deref().unwrap_or("(no suggestion)")
        );
    }

    Report {
        rows,
        table,
        fixes,
        trees,
    }
}

fn write_out(report: &Report) {
    if let Ok(dir) = std::env::var("GQ3_REDTEAM_OUT") {
        let dir = PathBuf::from(dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("results.md"), &report.table).unwrap();
        std::fs::write(dir.join("fix-audit.txt"), &report.fixes).unwrap();
        std::fs::write(dir.join("mismatches.txt"), mismatch_lines(report)).unwrap();
    }
}

fn mismatch_lines(report: &Report) -> String {
    let mut out = String::new();
    for r in &report.rows {
        if let Some(m) = &r.mismatch {
            let _ = writeln!(
                out,
                "{:<45} # {}",
                r.case.name,
                norm(m).chars().take(160).collect::<String>()
            );
        }
    }
    out
}

fn mismatches(report: &Report) -> BTreeSet<String> {
    report
        .rows
        .iter()
        .filter(|r| r.mismatch.is_some())
        .map(|r| r.case.name.clone())
        .collect()
}

#[test]
fn redteam_corpus_matches_its_pinned_mismatches() {
    let report = run_all();
    write_out(&report);
    assert!(
        report.rows.len() >= 200,
        "the corpus holds at least 200 cases"
    );
    let known: BTreeSet<String> = std::fs::read_to_string(root().join("known-mismatches.txt"))
        .unwrap()
        .lines()
        .map(|l| l.split('#').next().unwrap().trim().to_string())
        .filter(|l| !l.is_empty())
        .collect();
    let got = mismatches(&report);
    let new: Vec<_> = got.difference(&known).collect();
    let gone: Vec<_> = known.difference(&got).collect();
    assert!(
        new.is_empty() && gone.is_empty(),
        "mismatches moved: new {new:?}, no longer mismatching {gone:?}"
    );
}

#[test]
#[ignore = "instrument: the GQ 3 red-team corpus; fails on any mismatch with its headers"]
fn redteam_corpus_strict() {
    let report = run_all();
    write_out(&report);
    println!("{}", report.table);
    println!("{}", report.fixes);
    let got = mismatches(&report);
    println!("{} cases, {} mismatches", report.rows.len(), got.len());
    assert!(got.is_empty(), "{} mismatches: {got:?}", got.len());
}

#[test]
fn redteam_suggestions_reparse_to_their_pinned_trees() {
    let report = run_all();
    let path = root().join("fixed-trees.txt");
    if std::env::var("GQ3_REDTEAM_BLESS").is_ok() {
        std::fs::write(&path, &report.trees).unwrap();
        return;
    }
    let pinned = std::fs::read_to_string(&path).unwrap_or_default();
    let pinned: Vec<&str> = pinned.lines().filter(|l| !l.starts_with('#')).collect();
    let got: Vec<&str> = report
        .trees
        .lines()
        .filter(|l| !l.starts_with('#'))
        .collect();
    let moved: Vec<String> = got
        .iter()
        .filter(|l| !pinned.contains(l))
        .map(|l| format!("now:    {l}"))
        .chain(
            pinned
                .iter()
                .filter(|l| !got.contains(l))
                .map(|l| format!("pinned: {l}")),
        )
        .collect();
    assert!(
        moved.is_empty(),
        "repaired trees moved (rerun with GQ3_REDTEAM_BLESS=1 if intended):\n{}",
        moved.join("\n")
    );
}

#[test]
#[ignore = "instrument: one case's verdict, `CASE=<name>`"]
fn debug_one_case() {
    let case = load_case(
        &root()
            .join("corpus")
            .join(std::env::var("CASE").unwrap() + ".gq"),
    );
    let r = parse(&case.text).unwrap_err();
    let repaired = apply_edits(&case.text, r.edits());
    let Expect::RefuseWithFix(fix) = &case.expect else {
        panic!()
    };
    eprintln!("span {:?} edits {:?}", r.span, r.edits());
    eprintln!("splice {}", is_splice_of(&case.text, &repaired, fix));
    eprintln!("intended {}", is_intended(&case, &r, &repaired, fix));
}

/// Every edition-3 refusal code is printed with a suggestion by at least one
/// corpus case, and that suggestion reparses (RFC section 5, as adopted on
/// 2026-11-08: every fix is a suggestion of byte-range edits).
#[test]
fn every_refusal_code_has_a_corpus_case_whose_suggestion_reparses() {
    let report = run_all();
    let mut covered = BTreeSet::new();
    for r in &report.rows {
        if let (Err(e), Some(FixVerdict::Parses)) = (&r.actual, &r.fix) {
            covered.insert(e.diagnostic.code.as_str());
        }
    }
    let wanted = [
        "Q002", "Q005", "Q006", "Q007", "Q008", "Q009", "Q010", "Q011", "Q012", "Q013", "Q014",
        "Q015", "Q016", "Q017", "Q018", "Q019", "Q020", "Q021",
    ];
    let missing: Vec<&&str> = wanted.iter().filter(|c| !covered.contains(**c)).collect();
    assert!(
        missing.is_empty(),
        "codes with no reparsing suggestion in the corpus: {missing:?}"
    );
}
