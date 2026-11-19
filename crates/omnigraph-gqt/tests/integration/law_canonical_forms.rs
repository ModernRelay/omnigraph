//! One plan for one meaning (the GQ 3 RFC, section 6, "Canonical
//! forms").
//!
//! The RFC lists every non-canonical form the grammar admits and says lint's
//! fix for each "never changes the typed IR". This law holds that claim at
//! every level a reader can observe: the two spellings of each listed pair
//! have the same typed IR, the same logical and physical explain plans, the
//! same `logical_hash`, and the same rows.
//!
//! Four parts:
//!
//! - every listed pair, once, over a fixed schema and graph. The pairs are
//!   lint's codes `L203` and `L206` to `L216` (the RFC's list): the eight
//!   kinds of collision the type checker spike pinned and the compiler's
//!   desugaring test re-pins with the parser (the RFC names the five that
//!   the surface removed but not the eight that remain; this law takes them
//!   to be those eight kinds), and the five spellings section 3 admits:
//!   `case` (searched, simple, without `else`, nested in a `where`), `||`,
//!   `<>`, a single-quoted string and a keyword or registry name in another
//!   letter case;
//! - the spellings the grammar refuses with a printed fix (section 5's
//!   habits and the SQL and Cypher forms it does not admit): the fix,
//!   applied, has the canonical spelling's typed IR;
//! - generated reads (p21's generator) with the listed rewrites applied at
//!   random positions, each rewritten read held to its original: the five
//!   admitted spellings are written by `arbitrary::respell_spellings`, which
//!   chooses the keywords and registry names from the parse tree, so
//!   property, edge and alias names keep their spelling;
//! - every read of the GQT corpus that lint flags with a canonical form,
//!   with lint's printed byte-range fix applied, held to the same typed IR
//!   and `logical_hash`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use omnigraph::Session;
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::loader::LoadMode;
use omnigraph::settings::SessionSettings;
use omnigraph_compiler::ParamMap;
use omnigraph_compiler::catalog::{Catalog, build_catalog};
use omnigraph_compiler::ir::core::CoreDecl;
use omnigraph_compiler::query::check::check_source;
use omnigraph_compiler::query::diagnostic::TextEdit;
use omnigraph_compiler::query::lint::{QueryLintSchemaSource, lint_query_file};
use omnigraph_compiler::query::syntax::parse::apply_edits;
use omnigraph_compiler::query_input::{JsonParamMode, find_named_query, json_params_to_param_map};
use omnigraph_compiler::schema::parser::parse_schema;
use proptest::test_runner::TestCaseError;
use serde_json::Value;

use crate::law_engine_reference::{law_runner, load, sample};
use omnigraph_compiler::arbitrary::{ReadOptions, Spelling, choices, respell_spellings};

/// The per-pull-request case count; the nightly test runs `NIGHTLY`.
const CASES: u32 = 200;
const NIGHTLY: u32 = 5_000;

const SCHEMA: &str = r#"
node Person {
    name: String @key
    age: I32?
}
node Company {
    name: String @key
}
node Doc {
    slug: String @key
    text: String @index(fulltext)
    v: Vector(2)
}
edge WorksAt: Person -> Company
edge Knows: Person -> Person
edge Likes: Person -> Person
"#;

const SEED: &str = r#"{"type":"Person","data":{"name":"ada","age":36}}
{"type":"Person","data":{"name":"bob","age":null}}
{"type":"Person","data":{"name":"cy","age":20}}
{"type":"Company","data":{"name":"acme"}}
{"type":"Company","data":{"name":"zeta"}}
{"type":"Doc","data":{"slug":"d1","text":"graph engines answer queries","v":[1.0,0.0]}}
{"type":"Doc","data":{"slug":"d2","text":"a graph of people","v":[0.0,1.0]}}
{"type":"Doc","data":{"slug":"d3","text":"queries over people and companies","v":[1.0,1.0]}}
{"edge":"WorksAt","from":"ada","to":"acme"}
{"edge":"WorksAt","from":"bob","to":"zeta"}
{"edge":"Knows","from":"ada","to":"bob"}
{"edge":"Knows","from":"bob","to":"cy"}
{"edge":"Likes","from":"cy","to":"ada"}
{"edge":"Likes","from":"ada","to":"ada"}
"#;

/// Each listed canonical form: lint's code, the non-canonical spelling, the
/// canonical one, the parameters both take and whether their rows are
/// ordered.
const LISTED: &[(&str, &str, &str)] = &[
    (
        "L203",
        "query q() { match { $p: Person } where count { $f: Person, $p knows $f } > 0 return { $p.name } }",
        "query q() { match { $p: Person } where exists { $f: Person, $p knows $f } return { $p.name } }",
    ),
    (
        "L206",
        "query q() { match { from union(union({ match { $x: Person } where $x.age > 30 return { $x as u } }, { match { $y: Person } where $y.age < 25 return { $y as u } }), { match { $z: Person } where $z.age is null return { $z as u } }) } return { $u.name } }",
        "query q() { match { from union({ match { $x: Person } where $x.age > 30 return { $x as u } }, { match { $y: Person } where $y.age < 25 return { $y as u } }, { match { $z: Person } where $z.age is null return { $z as u } }) } return { $u.name } }",
    ),
    (
        "L207",
        "query q() { match { $p: Person, $p (likes | knows) $f } return { $p.name, $f.name } }",
        "query q() { match { $p: Person, $p (knows | likes) $f } return { $p.name, $f.name } }",
    ),
    (
        "L207",
        "query q() { match { $p: Person, $p <knows> $f } return { $p.name, $f.name } }",
        "query q() { match { $p: Person, $f <knows> $p } return { $p.name, $f.name } }",
    ),
    (
        "L207",
        "query q() { match { $p: Person, $f: Person, $p <knows> $f } return { $p.name, $f.name } }",
        "query q() { match { $p: Person, $f: Person, $f <knows> $p } return { $p.name, $f.name } }",
    ),
    (
        "L208",
        "query q() { match { $p: Person } return { $p.name, $p.age } order { $p.age asc, $p.name } }",
        "query q() { match { $p: Person } return { $p.name, $p.age } order { $p.age, $p.name } }",
    ),
    (
        "L209",
        "query q() { match { $p: Person } return { $p as p } }",
        "query q() { match { $p: Person } return { $p } }",
    ),
    (
        "L210",
        "query q($v: Vector(2), $t: String) { match { let $all = { match { $d: Doc } return { $d } }, from rrf(nearest($all, $d.v, $v, k: 3), bm25($all, $d.text, $t), k: 60) as $s } return { $d.slug, $s } order { $s desc, $d.slug } }",
        "query q($v: Vector(2), $t: String) { match { let $all = { match { $d: Doc } return { $d } }, from rrf(nearest($all, $d.v, $v, k: 3), bm25($all, $d.text, $t)) as $s } return { $d.slug, $s } order { $s desc, $d.slug } }",
    ),
    (
        "L210",
        "query q($t: String) { match { $d: Doc } where match_terms($d.text, terms($t, max_edits: 1, mode: any)) return { $d.slug } }",
        "query q($t: String) { match { $d: Doc } where match_terms($d.text, terms($t, mode: any, max_edits: 1)) return { $d.slug } }",
    ),
    (
        "L210",
        "query q($t: String) { match { $d: Doc } where match_terms($d.text, terms($t, mode: all, max_edits: 0)) return { $d.slug } }",
        "query q($t: String) { match { $d: Doc } where match_terms($d.text, terms($t)) return { $d.slug } }",
    ),
    (
        "L211",
        "query q($t: String) { match { let $all = { match { $d: Doc } return { $d } }, from bm25($all, $d.text, terms($t, mode: any)) as $s } return { $d.slug, $s } order { $s desc, $d.slug } }",
        "query q($t: String) { match { let $all = { match { $d: Doc } return { $d } }, from bm25($all, $d.text, $t) as $s } return { $d.slug, $s } order { $s desc, $d.slug } }",
    ),
    (
        "L212",
        "query q() { match { $p: Person } return { $p.name, case when $p.age > 30 then \"older\" else \"younger\" end as band } }",
        "query q() { match { $p: Person } return { $p.name, if($p.age > 30, \"older\", \"younger\") as band } }",
    ),
    (
        "L212",
        "query q() { match { $p: Person } return { $p.name, case when $p.age > 30 then \"older\" end as band } }",
        "query q() { match { $p: Person } return { $p.name, if($p.age > 30, \"older\", null) as band } }",
    ),
    (
        "L212",
        "query q() { match { $p: Person } return { $p.name, case when $p.age > 30 then 2 when $p.age > 18 then 1 else 0 end as band } }",
        "query q() { match { $p: Person } return { $p.name, if($p.age > 30, 2, if($p.age > 18, 1, 0)) as band } }",
    ),
    (
        "L212",
        "query q() { match { $p: Person } return { $p.name, case $p.name when \"ada\" then 1 when \"bob\" then 2 else 0 end as n } }",
        "query q() { match { $p: Person } return { $p.name, if($p.name = \"ada\", 1, if($p.name = \"bob\", 2, 0)) as n } }",
    ),
    (
        "L212",
        "query q() { match { $p: Person } where case when $p.age is null then false else $p.age > 20 end return { $p.name } }",
        "query q() { match { $p: Person } where if($p.age is null, false, $p.age > 20) return { $p.name } }",
    ),
    (
        "L213",
        "query q() { match { $p: Person } return { $p.name || \"!\" as n } }",
        "query q() { match { $p: Person } return { concat($p.name, \"!\") as n } }",
    ),
    (
        "L213",
        "query q() { match { $p: Person, $p worksAt $c } return { $p.name || \"@\" || $c.name as n } }",
        "query q() { match { $p: Person, $p worksAt $c } return { concat($p.name, \"@\", $c.name) as n } }",
    ),
    (
        "L214",
        "query q() { match { $p: Person } where $p.name <> \"ada\" return { $p.name } }",
        "query q() { match { $p: Person } where $p.name != \"ada\" return { $p.name } }",
    ),
    (
        "L215",
        "query q() { match { $p: Person } where $p.name = 'ada' return { $p.name } }",
        "query q() { match { $p: Person } where $p.name = \"ada\" return { $p.name } }",
    ),
    (
        "L215",
        "query q() { match { $p: Person } where $p.name != 'it\\'s \"x\"' return { $p.name } }",
        "query q() { match { $p: Person } where $p.name != \"it's \\\"x\\\"\" return { $p.name } }",
    ),
    (
        "L216",
        "QUERY q() { MATCH { $p: Person } WHERE $p.age IS NOT NULL AND NOT ($p.name = \"bob\") RETURN { $p.name, $p.age } ORDER { $p.age DESC NULLS LAST } LIMIT 2 }",
        "query q() { match { $p: Person } where $p.age is not null and not ($p.name = \"bob\") return { $p.name, $p.age } order { $p.age desc nulls last } limit 2 }",
    ),
    (
        "L216",
        "query q() { match { $p: Person, Optional { $f: Person, $p knows $f } } where Exists { $c: Company, $p worksAt $c } or $p.age Is Null return Distinct { Upper($p.name) as n, Coalesce($f.name, \"-\") as f } }",
        "query q() { match { $p: Person, optional { $f: Person, $p knows $f } } where exists { $c: Company, $p worksAt $c } or $p.age is null return distinct { upper($p.name) as n, coalesce($f.name, \"-\") as f } }",
    ),
    (
        "L216",
        "query q() { match { $p: Person } return { COUNT($p) as n, Max($p.age) as oldest, CAST(SUM($p.age), String) as total, IF(TRUE, 1, 0) as one } }",
        "query q() { match { $p: Person } return { count($p) as n, max($p.age) as oldest, cast(sum($p.age), String) as total, if(true, 1, 0) as one } }",
    ),
];

/// Spellings the grammar refuses whose refusal prints a fix (section 5),
/// each with the canonical spelling its suggestion must reach. The five
/// spellings of section 6's list it once refused are admitted now and are
/// pairs of [`LISTED`].
const REFUSED: &[(&str, &str, &str)] = &[
    (
        "==",
        "query q() { match { $p: Person } where $p.name == \"ada\" return { $p.name } }",
        "query q() { match { $p: Person } where $p.name = \"ada\" return { $p.name } }",
    ),
    (
        "x not in list",
        "query q() { match { $p: Person } where $p.name not in [\"ada\"] return { $p.name } }",
        "query q() { match { $p: Person } where not $p.name in [\"ada\"] return { $p.name } }",
    ),
    (
        "starts with",
        "query q() { match { $p: Person } where $p.name starts with \"a\" return { $p.name } }",
        "query q() { match { $p: Person } where starts_with($p.name, \"a\") return { $p.name } }",
    ),
    (
        "between",
        "query q() { match { $p: Person } where $p.age between 18 and 40 return { $p.name } }",
        "query q() { match { $p: Person } where $p.age >= 18 and $p.age <= 40 return { $p.name } }",
    ),
    (
        "&&",
        "query q() { match { $p: Person } where $p.age > 18 && $p.age < 40 return { $p.name } }",
        "query q() { match { $p: Person } where $p.age > 18 and $p.age < 40 return { $p.name } }",
    ),
];

/// The parameters `source` declares, bound from `json`.
fn bound(source: &str, query: &str, json: Option<&Value>) -> ParamMap {
    find_named_query(source, query)
        .ok()
        .and_then(|decl| json_params_to_param_map(json, &decl.params, JsonParamMode::Standard).ok())
        .unwrap_or_default()
}

fn fixed_params(source: &str) -> ParamMap {
    bound(
        source,
        "q",
        Some(&serde_json::json!({"v": [1.0, 0.5], "t": "graph people"})),
    )
}

/// Everything a reader can observe of one spelling.
#[derive(Debug, PartialEq)]
struct Observed {
    ir: CoreDecl,
    logical_plan: Value,
    physical_plan: Value,
    logical_hash: Value,
    rows: Vec<String>,
}

fn typed_ir(catalog: &Catalog, source: &str) -> Result<CoreDecl, String> {
    let checked =
        check_source(catalog, source).map_err(|d| format!("{}: {}", d.code, d.message))?;
    match checked.into_iter().find(|(n, _)| n == "q").map(|(_, r)| r) {
        Some(Ok(c)) => Ok(c.core),
        Some(Err(d)) => Err(format!("{}: {}", d.code, d.message)),
        None => Err("no declaration `q`".into()),
    }
}

async fn observe(
    session: &Session,
    source: &str,
    params: &ParamMap,
    ordered: bool,
) -> Result<Observed, String> {
    let ir = typed_ir(&session.catalog(), source)?;
    let explain = session
        .explain_query(ReadTarget::branch("main"), source, "q", params)
        .await
        .map_err(|e| format!("explain: {e}"))?;
    let rows = session
        .query(ReadTarget::branch("main"), source, "q", params)
        .await
        .map_err(|e| format!("query: {e}"))?
        .to_rust_json()
        .map_err(|e| format!("rows: {e}"))?;
    let mut rows: Vec<String> = match rows {
        Value::Array(rows) => rows.iter().map(|r| r.to_string()).collect(),
        other => vec![other.to_string()],
    };
    if !ordered {
        rows.sort();
    }
    Ok(Observed {
        ir,
        logical_plan: explain["logical_plan"].clone(),
        physical_plan: explain["physical_plan"].clone(),
        logical_hash: explain["logical_hash"].clone(),
        rows,
    })
}

/// The first level at which two observations differ.
fn first_difference(a: &Observed, b: &Observed) -> Option<&'static str> {
    if a.ir != b.ir {
        Some("typed IR")
    } else if a.logical_plan != b.logical_plan {
        Some("logical plan")
    } else if a.physical_plan != b.physical_plan {
        Some("physical plan")
    } else if a.logical_hash != b.logical_hash {
        Some("logical_hash")
    } else if a.rows != b.rows {
        Some("rows")
    } else {
        None
    }
}

async fn fixed_session() -> (tempfile::TempDir, Session) {
    let dir = tempfile::tempdir().unwrap();
    let db = Omnigraph::init(dir.path().to_str().unwrap(), SCHEMA)
        .await
        .unwrap();
    let session = Session::from_defaults(Arc::new(db), SessionSettings::default());
    session.load_jsonl(SEED, LoadMode::Overwrite).await.unwrap();
    (dir, session)
}

#[test]
fn every_listed_canonical_form_keeps_the_ir_the_plan_the_hash_and_the_rows() {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let (_dir, session) = fixed_session().await;
        let mut failures = Vec::new();
        for (code, written, canonical) in LISTED {
            let ordered = canonical.contains(" order {");
            let a = observe(&session, written, &fixed_params(written), ordered).await;
            let b = observe(&session, canonical, &fixed_params(canonical), ordered).await;
            match (a, b) {
                (Ok(a), Ok(b)) => {
                    if let Some(level) = first_difference(&a, &b) {
                        failures.push(format!(
                            "{code}: {level} differs\n  {written}\n  {canonical}"
                        ));
                    }
                    if a.rows.is_empty() {
                        failures.push(format!("{code}: no row to compare\n  {canonical}"));
                    }
                }
                (a, b) => failures.push(format!(
                    "{code}: did not run: {:?} / {:?}\n  {written}",
                    a.err(),
                    b.err()
                )),
            }
            // Lint names the written form with its code and no other.
            let findings = canonical_findings(&session.catalog(), written);
            if !findings.iter().any(|(c, _)| c == code) && written != canonical {
                failures.push(format!("{code}: lint does not name\n  {written}"));
            }
            if !canonical_findings(&session.catalog(), canonical).is_empty() {
                failures.push(format!(
                    "{code}: lint names the canonical form\n  {canonical}"
                ));
            }
        }
        assert!(failures.is_empty(), "{}", failures.join("\n"));
    });
}

/// The spellings the grammar refuses with a printed fix: their refusal's
/// suggestion, applied, must have the canonical spelling's typed IR. A
/// refusal without a suggestion is reported with the others.
#[test]
fn refused_spellings_suggest_their_canonical_form() {
    let catalog = build_catalog(&parse_schema(SCHEMA).unwrap()).unwrap();
    let mut failures = Vec::new();
    for (label, written, canonical) in REFUSED {
        let want = typed_ir(&catalog, canonical).unwrap_or_else(|e| panic!("{label}: {e}"));
        let refusal = match check_source(&catalog, written) {
            Err(d) => d,
            Ok(decls) => match decls.into_iter().find_map(|(_, r)| r.err()) {
                Some(d) => d,
                None => {
                    failures.push(format!("{label}: accepted"));
                    continue;
                }
            },
        };
        let Some(suggestion) = refusal.suggestion.as_deref() else {
            failures.push(format!(
                "{label}: {} prints no suggestion: {}",
                refusal.code, refusal.message
            ));
            continue;
        };
        let applied = apply_edits(written, &suggestion.edits);
        match typed_ir(&catalog, &applied) {
            Ok(ir) if ir == want => {}
            Ok(_) => failures.push(format!("{label}: the suggestion's IR differs: {applied}")),
            Err(e) => failures.push(format!(
                "{label}: the suggestion is refused ({e}): {applied}"
            )),
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// lint's codes of section 6's canonical forms.
const CANONICAL_CODES: [&str; 12] = [
    "L203", "L206", "L207", "L208", "L209", "L210", "L211", "L212", "L213", "L214", "L215", "L216",
];

/// Every canonical-form finding lint prints for `source`: its code and its
/// printed byte-range fix, `None` where it prints none.
fn canonical_findings(catalog: &Catalog, source: &str) -> Vec<(String, Option<Vec<TextEdit>>)> {
    lint_query_file(
        catalog,
        source,
        "/tmp/q.gq",
        QueryLintSchemaSource::file("/tmp/s.pg"),
    )
    .findings
    .into_iter()
    .filter(|f| CANONICAL_CODES.contains(&f.code.as_str()))
    .map(|f| (f.code, f.suggestion.map(|s| s.edits)))
    .collect()
}

// ---------------------------------------------------------------------------
// Generated reads with the listed rewrites applied.

/// The index of the `}` closing the `{` at `open`, skipping strings.
fn closing_brace(s: &str, open: usize) -> Option<usize> {
    let bytes = s.as_bytes();
    let mut depth = 0i32;
    let mut in_str = false;
    for (i, &b) in bytes.iter().enumerate().skip(open) {
        match b {
            b'"' if i == 0 || bytes[i - 1] != b'\\' => in_str = !in_str,
            b'{' if !in_str => depth += 1,
            b'}' if !in_str => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

/// One rewrite of the list applied at the `pick`th place it applies, if any.
type Rewrite = fn(&str, usize) -> Option<String>;

fn nth<T>(items: Vec<T>, pick: usize) -> Option<T> {
    if items.is_empty() {
        return None;
    }
    let n = pick % items.len();
    items.into_iter().nth(n)
}

/// `L203`: `(exists { P })` and `(count { P } > 0)`, either way.
fn exists_count(s: &str, pick: usize) -> Option<String> {
    let mut places = Vec::new();
    for (i, _) in s.match_indices("(exists {") {
        if s[..i].ends_with("not ") {
            continue;
        }
        let open = i + "(exists ".len();
        let close = closing_brace(s, open)?;
        if s[close + 1..].starts_with(')') {
            places.push((i, close + 2, format!("(count {} > 0)", &s[open..=close])));
        }
    }
    for (i, _) in s.match_indices("(count {") {
        let open = i + "(count ".len();
        let close = closing_brace(s, open)?;
        if s[close + 1..].starts_with(" > 0)") {
            places.push((i, close + 6, format!("(exists {})", &s[open..=close])));
        }
    }
    let (start, end, with) = nth(places, pick)?;
    Some(format!("{}{with}{}", &s[..start], &s[end..]))
}

/// `L208`: an order key with and without `asc`, before its `nulls first`
/// or `nulls last`.
fn order_asc(s: &str, pick: usize) -> Option<String> {
    let at = s.find("\n  order { ")? + "\n  order { ".len();
    let end = at + s[at..].find(" }")?;
    let keys: Vec<String> = s[at..end].split(", ").map(str::to_string).collect();
    let split = |key: &str| -> (String, String) {
        match key.find(" nulls ") {
            Some(n) => (key[..n].to_string(), key[n..].to_string()),
            None => (key.to_string(), String::new()),
        }
    };
    let candidates: Vec<usize> = (0..keys.len())
        .filter(|&i| !split(&keys[i]).0.ends_with(" desc"))
        .collect();
    let i = nth(candidates, pick)?;
    let mut keys = keys;
    let (key, nulls) = split(&keys[i]);
    keys[i] = match key.strip_suffix(" asc") {
        Some(bare) => format!("{bare}{nulls}"),
        None => format!("{key} asc{nulls}"),
    };
    Some(format!("{}{}{}", &s[..at], keys.join(", "), &s[end..]))
}

/// `L207`: the members of an alternation in either order.
fn alternation(s: &str, pick: usize) -> Option<String> {
    let re = regex::Regex::new(r"\(([a-z][A-Za-z0-9_]*) \| ([a-z][A-Za-z0-9_]*)\)").unwrap();
    let m = nth(re.captures_iter(s).collect(), pick)?;
    let whole = m.get(0)?;
    Some(format!(
        "{}({} | {}){}",
        &s[..whole.start()],
        &m[2],
        &m[1],
        &s[whole.end()..]
    ))
}

/// `L207`: the ends of an undirected triple swapped.
fn undirected_ends(s: &str, pick: usize) -> Option<String> {
    let re = regex::Regex::new(r"(\$[a-z0-9]+)( <[a-z][A-Za-z0-9_]*>(?:\{\d,\d\})? )(\$[a-z0-9]+)")
        .unwrap();
    let m = nth(re.captures_iter(s).collect(), pick)?;
    let whole = m.get(0)?;
    Some(format!(
        "{}{}{}{}{}",
        &s[..whole.start()],
        &m[3],
        &m[2],
        &m[1],
        &s[whole.end()..]
    ))
}

/// `L210`: `rrf`'s `k: 60` dropped, or its options reordered.
fn rrf_options(s: &str, pick: usize) -> Option<String> {
    if let Some(i) = s.find(", k: 60, weights: [") {
        let w_at = i + ", k: 60, ".len();
        let w_end = w_at + s[w_at..].find(']')? + 1;
        let weights = &s[w_at..w_end];
        return Some(if pick.is_multiple_of(2) {
            format!("{}, {weights}{}", &s[..i], &s[w_end..])
        } else {
            format!("{}, {weights}, k: 60{}", &s[..i], &s[w_end..])
        });
    }
    let i = s.find(", k: 60) as ")?;
    Some(format!("{}{}", &s[..i], &s[i + ", k: 60".len()..]))
}

/// `L211`: `bm25` over the String and over `terms(q, mode: any)`.
fn bm25_terms(s: &str, pick: usize) -> Option<String> {
    let re = regex::Regex::new(r#"bm25\((\$[a-z0-9]+), (\$[a-z0-9]+\.t), ("[^"]*")\)"#).unwrap();
    let m = nth(re.captures_iter(s).collect(), pick)?;
    let whole = m.get(0)?;
    Some(format!(
        "{}bm25({}, {}, terms({}, mode: any)){}",
        &s[..whole.start()],
        &m[1],
        &m[2],
        &m[3],
        &s[whole.end()..]
    ))
}

/// One admitted spelling (`L212` to `L216`) written at places `pick`
/// seeds, the next seeds tried where one writes none at a place the read
/// has; `None` where none of four does.
fn spelled(s: &str, pick: usize, form: Spelling) -> Option<String> {
    (0..4).find_map(|i| {
        let (out, written) = respell_spellings(s, pick as u64 + i, &[form]);
        (!written.is_empty()).then_some(out)
    })
}

fn case_spelling(s: &str, pick: usize) -> Option<String> {
    spelled(s, pick, Spelling::Case)
}

fn bars_spelling(s: &str, pick: usize) -> Option<String> {
    spelled(s, pick, Spelling::Bars)
}

fn not_equal_spelling(s: &str, pick: usize) -> Option<String> {
    spelled(s, pick, Spelling::NotEqual)
}

fn single_quote_spelling(s: &str, pick: usize) -> Option<String> {
    spelled(s, pick, Spelling::SingleQuote)
}

fn letter_case_spelling(s: &str, pick: usize) -> Option<String> {
    spelled(s, pick, Spelling::LetterCase)
}

/// The textual rewrites first: the admitted spellings, written from the
/// parse tree, change the text they match.
const REWRITES: &[(&str, Rewrite)] = &[
    ("L203", exists_count),
    ("L208", order_asc),
    ("L207 alternation", alternation),
    ("L207 undirected", undirected_ends),
    ("L210", rrf_options),
    ("L211", bm25_terms),
    ("L212 case", case_spelling),
    ("L213 ||", bars_spelling),
    ("L214 <>", not_equal_spelling),
    ("L215 single quote", single_quote_spelling),
    ("L216 letter case", letter_case_spelling),
];

static APPLIED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
static TOTAL: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
static BY_REWRITE: std::sync::Mutex<BTreeMap<&'static str, usize>> =
    std::sync::Mutex::new(BTreeMap::new());

fn run_rewrite_law(cases: u32) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut runner = law_runner(cases, "tests/regressions/law_canonical_forms.txt");
    let result = runner.run(
        &(choices(64), choices(1024), choices(512), choices(16)),
        |(s, g, q, r)| {
            let sample = sample(&s, &g, &q, ReadOptions::default());
            let source = sample.read.source.clone();
            let mut rewritten = source.clone();
            let mut applied = Vec::new();
            for (i, (label, rewrite)) in REWRITES.iter().enumerate() {
                let pick = r.get(i).copied().unwrap_or(0) as usize;
                if pick % 3 == 2 {
                    continue;
                }
                if let Some(next) = rewrite(&rewritten, pick) {
                    rewritten = next;
                    applied.push(*label);
                }
            }
            TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if applied.is_empty() {
                return Ok(());
            }
            APPLIED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            for label in &applied {
                *BY_REWRITE.lock().unwrap().entry(label).or_default() += 1;
            }
            rt.block_on(async {
                let (_dir, session) = load(&sample).await?;
                let params = sample.params();
                let a = observe(&session, &source, &params, sample.read.ordered).await;
                let b = observe(&session, &rewritten, &params, sample.read.ordered).await;
                let failure = match (&a, &b) {
                    (Ok(a), Ok(b)) => {
                        first_difference(a, b).map(|level| format!("{level} differs"))
                    }
                    (Err(x), Err(y)) if code_of(x) == code_of(y) => None,
                    _ => Some(format!("one side failed: {:?} / {:?}", a.err(), b.err())),
                };
                match failure {
                    None => Ok(()),
                    Some(f) => Err(format!(
                        "{f} under {applied:?}\n{}\n--- rewritten\n{rewritten}",
                        sample.describe()
                    )),
                }
            })
            .map_err(TestCaseError::fail)
        },
    );
    if let Err(e) = result {
        panic!("{e}");
    }
    let applied = APPLIED.load(std::sync::atomic::Ordering::Relaxed);
    let total = TOTAL.load(std::sync::atomic::Ordering::Relaxed);
    eprintln!(
        "{applied} of {total} generated reads took at least one rewrite: {:?}",
        BY_REWRITE.lock().unwrap()
    );
    assert!(
        applied * 4 >= total,
        "only {applied} of {total} reads took a rewrite"
    );
    let by_rewrite = BY_REWRITE.lock().unwrap();
    let untaken: Vec<&str> = REWRITES
        .iter()
        .map(|(label, _)| *label)
        .filter(|label| {
            Spelling::ALL.iter().any(|f| label.starts_with(f.code()))
                && !by_rewrite.contains_key(label)
        })
        .collect();
    assert!(
        untaken.is_empty(),
        "no generated read took the admitted spellings {untaken:?}"
    );
}

fn code_of(error: &str) -> &str {
    error
        .split(|c: char| !c.is_ascii_alphanumeric())
        .find(|w| {
            w.len() >= 2
                && w.len() <= 4
                && w.starts_with(['T', 'Q', 'P', 'E'])
                && w[1..].chars().all(|c| c.is_ascii_digit())
        })
        .unwrap_or("")
}

#[test]
fn generated_reads_keep_one_plan_under_the_listed_rewrites() {
    run_rewrite_law(CASES);
}

#[test]
#[ignore = "nightly: 5,000 generated reads; the gate runs 200"]
fn generated_reads_keep_one_plan_under_the_listed_rewrites_at_nightly_breadth() {
    run_rewrite_law(NIGHTLY);
}

// ---------------------------------------------------------------------------
// Lint's printed fix over the corpus.

fn corpus() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("cases")
}

fn gqt_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            gqt_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "gqt") {
            out.push(path);
        }
    }
}

/// The schema and every read section of a case, each with the parameters of
/// the `--- params` section that follows it.
fn case_reads(text: &str) -> (Option<String>, Vec<(String, Option<String>)>) {
    let mut sections: Vec<(String, String)> = Vec::new();
    for line in text.lines() {
        if let Some(header) = line.strip_prefix("--- ") {
            sections.push((header.trim().to_string(), String::new()));
        } else if let Some((_, body)) = sections.last_mut() {
            body.push_str(line);
            body.push('\n');
        }
    }
    let schema = sections
        .iter()
        .find(|(h, _)| h == "schema")
        .map(|(_, b)| b.clone());
    let mut reads = Vec::new();
    for (i, (header, body)) in sections.iter().enumerate() {
        if header == "query" || header.starts_with("query ") {
            let params = sections
                .get(i + 1)
                .filter(|(h, _)| h == "params")
                .map(|(_, b)| b.clone());
            reads.push((body.clone(), params));
        }
    }
    (schema, reads)
}

#[test]
fn lint_fixes_over_the_corpus_keep_the_typed_ir_and_the_logical_hash() {
    let mut paths = Vec::new();
    gqt_files(&corpus(), &mut paths);
    paths.sort();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut checked = BTreeMap::<String, usize>::new();
    let mut failures = Vec::new();
    for path in paths {
        let text = std::fs::read_to_string(&path).unwrap();
        let (Some(schema), reads) = case_reads(&text) else {
            continue;
        };
        let Ok(catalog) = parse_schema(&schema)
            .map_err(|_| ())
            .and_then(|s| build_catalog(&s).map_err(|_| ()))
        else {
            continue;
        };
        let flagged: Vec<_> = reads
            .iter()
            .flat_map(|(source, params)| {
                canonical_findings(&catalog, source)
                    .into_iter()
                    .map(move |f| (source.clone(), params.clone(), f))
            })
            .collect();
        if flagged.is_empty() {
            continue;
        }
        let name = path.strip_prefix(corpus()).unwrap().display().to_string();
        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let db = match Omnigraph::init(dir.path().to_str().unwrap(), &schema).await {
                Ok(db) => db,
                Err(e) => {
                    failures.push(format!("{name}: init failed: {e}"));
                    return;
                }
            };
            let session = Session::from_defaults(Arc::new(db), SessionSettings::default());
            for (source, params, (code, edits)) in flagged {
                // Every canonical-form finding prints its fix as byte-range
                // edits of the source, which `lint --fix` applies.
                let Some(edits) = edits else {
                    failures.push(format!("{name}: {code} prints no byte-range fix\n{source}"));
                    continue;
                };
                let fixed = apply_edits(&source, &edits);
                let query = last_declaration_name(&source);
                let json = params
                    .as_deref()
                    .and_then(|p| serde_json::from_str::<Value>(p).ok());
                let params = bound(&source, &query, json.as_ref());
                let ir = |src: &str| -> Result<CoreDecl, String> {
                    let checked = check_source(&catalog, src).map_err(|d| d.message)?;
                    match checked.into_iter().find(|(n, _)| *n == query).map(|(_, r)| r) {
                        Some(Ok(c)) => Ok(c.core),
                        Some(Err(d)) => Err(format!("{}: {}", d.code, d.message)),
                        None => Err("missing".into()),
                    }
                };
                let (a, b) = (ir(&source), ir(&fixed));
                if a.is_ok() && a != b {
                    failures.push(format!("{name}: {code} changes the typed IR\n{source}\n--- fixed\n{fixed}"));
                    continue;
                }
                if a.is_err() {
                    continue;
                }
                let hash = |src: String| {
                    let session = &session;
                    let params = &params;
                    let query = query.clone();
                    async move {
                        session
                            .explain_query(ReadTarget::branch("main"), &src, &query, params)
                            .await
                            .map(|e| e["logical_hash"].clone())
                            .map_err(|e| e.to_string())
                    }
                };
                match (hash(source.clone()).await, hash(fixed.clone()).await) {
                    (Ok(x), Ok(y)) if x == y => {
                        *checked.entry(code).or_default() += 1;
                    }
                    (Ok(x), Ok(y)) => failures.push(format!(
                        "{name}: {code} changes the logical_hash ({x} to {y})\n{source}\n--- fixed\n{fixed}"
                    )),
                    // A read the planner cannot plan without its arguments
                    // (or at all) on an empty graph has no hash to compare;
                    // its typed IR was compared above.
                    (Err(_), _) => {
                        *checked.entry(format!("{code} (typed IR only)")).or_default() += 1;
                    }
                    (Ok(_), Err(e)) => failures.push(format!(
                        "{name}: {code} fix does not plan: {e}\n{fixed}"
                    )),
                }
            }
        });
    }
    eprintln!("lint fixes checked: {checked:?}");
    assert!(failures.is_empty(), "{}", failures.join("\n\n"));
    assert!(
        checked.values().sum::<usize>() >= 50,
        "only {checked:?} lint fixes were compared"
    );
}

fn last_declaration_name(source: &str) -> String {
    let re = regex::Regex::new(r"(?m)^\s*query\s+([A-Za-z_][A-Za-z0-9_]*)").unwrap();
    re.captures_iter(source)
        .last()
        .map(|c| c[1].to_string())
        .unwrap_or_default()
}
