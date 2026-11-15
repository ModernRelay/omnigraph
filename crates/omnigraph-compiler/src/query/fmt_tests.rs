//! `queries fmt` (docs/rfcs/2026-11-07-gq-3.md, section 6): one case per
//! rewrite of the formatter's list, the layout, the text around the
//! declarations, and the repository-wide proof that formatting never changes
//! the typed IR and is idempotent.

use std::path::{Path, PathBuf};

use super::*;
use crate::catalog::build_catalog;
use crate::schema::parser::parse_schema;

/// The checker tests' schema, with a declared analyzer on `Doc.title` and
/// a second `Person -> Person` edge for alternations.
fn catalog() -> Catalog {
    let schema = format!(
        "{}\nedge Likes: Person -> Person\nnode Note {{ slug: String @key  text: String @index(fulltext, analyzer = \"english_v1\") }}\n",
        crate::query::check::tests::SCHEMA
    );
    build_catalog(&parse_schema(&schema).unwrap()).unwrap()
}

#[track_caller]
fn fmt(src: &str) -> String {
    format_source(src, Some(&catalog()))
        .unwrap_or_else(|e| panic!("{e}\n{src}"))
        .text
}

#[track_caller]
fn fmt_without_schema(src: &str) -> String {
    format_source(src, None)
        .unwrap_or_else(|e| panic!("{e}\n{src}"))
        .text
}

/// `from` rewritten is `to`, and the two type to one IR.
#[track_caller]
fn rewrites(from: &str, to: &str) {
    let got = fmt(from);
    assert_eq!(got, to, "formatting\n{from}");
    assert_ir_unchanged(&catalog(), from, &got);
    assert_eq!(fmt(&got), got, "not idempotent:\n{got}");
}

#[track_caller]
fn assert_ir_unchanged(catalog: &Catalog, before: &str, after: &str) {
    let typed = |src: &str| {
        let file = parse_source(src).unwrap_or_else(|e| panic!("{e}\n{src}"));
        super::typed(catalog, &file)
    };
    assert_eq!(
        typed(before),
        typed(after),
        "typed IR changed:\n{before}\n--\n{after}"
    );
}

#[test]
fn count_greater_than_zero_is_exists() {
    rewrites(
        "query f() { match { $p: Person } where count { $q: Person, $p knows $q } > 0 return { $p } }",
        "query f() {\n  match { $p: Person }\n  where exists { $q: Person, $p knows $q }\n  return { $p }\n}\n",
    );
    // Not the rows-equal forms that change the IR; those are lint's.
    for kept in [
        "count { $q: Person, $p knows $q } >= 1",
        "count { $q: Person, $p knows $q } = 0",
        "count($q.age) { $q: Person, $p knows $q } > 0",
    ] {
        let src = format!("query f() {{ match {{ $p: Person }} where {kept} return {{ $p }} }}");
        assert!(fmt(&src).contains(kept), "{}", fmt(&src));
    }
}

#[test]
fn a_union_nested_first_is_flat() {
    rewrites(
        "query f() { match { from union(union({ match { $c: Company } return { $c } }, { match { $d: Company } return { $d as c } }), { match { $e: Company } return { $e as c } }) } return { $c } }",
        "query f() {\n  match {\n    from union(\n      { match { $c: Company } return { $c } },\n      { match { $d: Company } return { $d as c } },\n      { match { $e: Company } return { $e as c } }\n    ),\n  }\n  return { $c }\n}\n",
    );
    // A union nested after the first argument associates the other way: its
    // typed IR differs from the flat one's, so it stays as written.
    let right = "query f() { match { from union({ match { $c: Company } return { $c } }, union({ match { $d: Company } return { $d as c } }, { match { $e: Company } return { $e as c } })) } return { $c } }";
    assert!(fmt(right).matches("union(").count() == 2, "{}", fmt(right));
}

#[test]
fn undirected_ends_and_alternation_members_take_a_stable_order() {
    rewrites(
        "query f() { match { $q: Person, $p: Person, $q <knows> $p } return { $q } }",
        "query f() {\n  match { $q: Person, $p: Person, $p <knows> $q }\n  return { $q }\n}\n",
    );
    rewrites(
        "query f() { match { $q <likes | knows> $p } return { $q } }",
        "query f() {\n  match { $p <knows | likes> $q }\n  return { $q }\n}\n",
    );
    rewrites(
        "query f() { match { $p: Person, $p (likes | knows) $q } return { $q } }",
        "query f() {\n  match { $p: Person, $p (knows | likes) $q }\n  return { $q }\n}\n",
    );
    // A directed triple keeps its ends: they say which way the edge points.
    let directed =
        "query f() {\n  match { $q: Person, $p: Person, $q knows $p }\n  return { $q }\n}\n";
    assert_eq!(fmt(directed), directed);
}

#[test]
fn asc_and_an_alias_equal_to_its_column_name_are_dropped() {
    rewrites(
        "query f() { match { $p: Person, $p worksAt $c } return { $p as p, count($c) as c, $p.name as name } order { $p.age asc, name desc } }",
        "query f() {\n  match { $p: Person, $p worksAt $c }\n  return { $p, count($c), $p.name as name }\n  order { $p.age, name desc }\n}\n",
    );
}

#[test]
fn options_take_the_registry_order_and_defaults_are_dropped() {
    rewrites(
        "query f($s: String, $v: Vector(3)) { match { let $docs = { match { $d: Doc } return { $d } }, from rrf(nearest($docs, $d.embedding, $v, recall: 0.9, k: 5), bm25($docs, $d.body, $s), weights: [1, 1.0], k: 60) as $score } return { $d } }",
        "query f($s: String, $v: Vector(3)) {\n  match {\n    let $docs = { match { $d: Doc } return { $d } },\n    from rrf(\n      nearest($docs, $d.embedding, $v, k: 5, recall: 0.9),\n      bm25($docs, $d.body, $s)\n    ) as $score,\n  }\n  return { $d }\n}\n",
    );
    // A weight other than one, and a window that is not the default, stay.
    let kept = "query f($s: String, $v: Vector(3)) {\n  match {\n    let $docs = { match { $d: Doc } return { $d } },\n    from rrf(\n      nearest($docs, $d.embedding, $v, k: 5),\n      bm25($docs, $d.body, $s),\n      k: 10,\n      weights: [2, 1]\n    ) as $score,\n  }\n  return { $d }\n}\n";
    assert_eq!(fmt(kept), kept);
}

#[test]
fn bm25_over_any_terms_is_over_the_string() {
    rewrites(
        "query f($s: String) { match { let $pop = { match { $d: Doc } return { $d } }, from bm25($pop, $d.body, terms($s, max_edits: 0, mode: any)) as $score } return { $d, $score } }",
        "query f($s: String) {\n  match {\n    let $pop = { match { $d: Doc } return { $d } },\n    from bm25($pop, $d.body, $s) as $score,\n  }\n  return { $d, $score }\n}\n",
    );
    // `terms($s)` is `mode: all`, another query.
    let all = "query f($s: String) {\n  match {\n    let $pop = { match { $d: Doc } return { $d } },\n    from bm25($pop, $d.body, terms($s)) as $score,\n  }\n  return { $d, $score }\n}\n";
    assert_eq!(fmt(all), all);
}

#[test]
fn the_older_lexical_calls_are_match_terms_over_a_declared_analyzer() {
    let cases = [
        (
            "search($n.text, $s)",
            "match_terms($n.text, terms($s, mode: any))",
        ),
        (
            "match_text($n.text, $s)",
            "match_terms($n.text, terms($s, mode: any))",
        ),
        (
            "fuzzy($n.text, $s)",
            "match_terms($n.text, terms($s, mode: any, max_edits: 2))",
        ),
        (
            "fuzzy($n.text, $s, max_edits: 1)",
            "match_terms($n.text, terms($s, mode: any, max_edits: 1))",
        ),
        (
            "fuzzy($n.text, \"rust\", max_edits: 0)",
            "match_terms($n.text, terms(\"rust\", mode: any))",
        ),
        (
            "not search($n.text, $s) or fuzzy($n.text, $s)",
            "not match_terms($n.text, terms($s, mode: any))\n    or match_terms($n.text, terms($s, mode: any, max_edits: 2))",
        ),
    ];
    for (from, to) in cases {
        rewrites(
            &format!("query f($s: String) {{ match {{ $n: Note }} where {from} return {{ $n }} }}"),
            &format!(
                "query f($s: String) {{\n  match {{ $n: Note }}\n  where {to}\n  return {{ $n }}\n}}\n"
            ),
        );
    }
    // Over a field whose index declares no analyzer the calls keep today's
    // behaviour: `match_terms` there is another term, so they stay; without a
    // schema nothing says which field declares one, so they stay too.
    let edition_2 = "query f($s: String) {\n  match { $d: Doc }\n  where search($d.body, $s) and fuzzy($d.body, $s)\n  return { $d }\n}\n";
    assert_eq!(fmt(edition_2), edition_2);
    let declared = "query f($s: String) {\n  match { $n: Note }\n  where search($n.text, $s)\n  return { $n }\n}\n";
    assert_eq!(fmt_without_schema(declared), declared);
}

#[test]
fn a_short_match_is_one_line_and_a_long_one_an_element_per_line() {
    rewrites(
        "query cited_near($coll: String, $q: Vector(3)) { match { let $pop = { match { $d: Doc, $x: Collection, $d in $x } where $x.slug = $coll return { $d } }, from nearest($pop, $d.embedding, $q, k: 20) as $distance, $d cites $other } return { $d.title, $distance, count($other) as cites } order { $distance } }",
        "query cited_near($coll: String, $q: Vector(3)) {
  match {
    let $pop = {
      match { $d: Doc, $x: Collection, $d in $x }
      where $x.slug = $coll
      return { $d }
    },
    from nearest($pop, $d.embedding, $q, k: 20) as $distance,
    $d cites $other,
  }
  return { $d.title, $distance, count($other) as cites }
  order { $distance }
}
",
    );
}

#[test]
fn blocks_writes_and_long_lists_break_the_same_way() {
    rewrites(
        "query f($company: String) { match { $p: Person } where not exists { $c: Company, $p worksAt $c where $c.name = $company and $c.slug != \"a-rather-long-slug\" } return { $p.name } }",
        "query f($company: String) {
  match { $p: Person }
  where not exists {
    $c: Company,
    $p worksAt $c,
    where $c.name = $company and $c.slug != \"a-rather-long-slug\"
  }
  return { $p.name }
}
",
    );
    rewrites(
        "query retire($h: String) as commit { match { $p: Person, $p $k:worksAt $c } where $p.handle = $h delete $k }",
        "query retire($h: String) as commit {\n  match { $p: Person, $p $k:worksAt $c } where $p.handle = $h\n  delete $k\n}\n",
    );
    rewrites(
        "query add($h: String, $n: String) { insert Person { handle: $h, name: $n, city: \"a city whose name is long\", age: 30 } }",
        "query add($h: String, $n: String) {
  insert Person {
    handle: $h,
    name: $n,
    city: \"a city whose name is long\",
    age: 30,
  }
}
",
    );
}

#[test]
fn comments_between_declarations_are_kept_and_one_inside_keeps_its_declaration() {
    let src = "// Leading note\n\n// About a\nquery a() { match { $p: Person } return { $p } } // trailing\n\n\n\nquery b() {\n  match { $p: Person } // inside\n  return { $p }\n}\n/* last */\n";
    let out = format_source(src, Some(&catalog())).unwrap();
    assert_eq!(
        out.text,
        "// Leading note\n\n// About a\nquery a() {\n  match { $p: Person }\n  return { $p }\n} // trailing\n\nquery b() {\n  match { $p: Person } // inside\n  return { $p }\n}\n\n/* last */\n"
    );
    assert_eq!(out.kept, vec!["b".to_string()]);
    assert!(out.changed);
    let again = format_source(&out.text, Some(&catalog())).unwrap();
    assert_eq!(again.text, out.text);
    assert!(!again.changed);
    // A `//` inside a string is no comment.
    let url =
        "query f() {\n  match { $p: Person }\n  where $p.city = \"http://x\"\n  return { $p }\n}\n";
    assert_eq!(fmt(url), url);
}

#[test]
fn settings_explain_and_statements() {
    assert_eq!(
        fmt(
            "set engine = v2;\nset ann_nprobes = 4;\nquery f() { match { $p: Person } return { $p } }"
        ),
        "set engine = v2;\nset ann_nprobes = 4;\n\nquery f() {\n  match { $p: Person }\n  return { $p }\n}\n"
    );
    assert_eq!(
        fmt("explain query f() { match { $p: Person } return { $p as p } }"),
        "explain query f() {\n  match { $p: Person }\n  return { $p }\n}\n"
    );
    let statement = "branch create feature from main;\n";
    let out = format_source(statement, None).unwrap();
    assert_eq!(out.text, statement);
    assert!(!out.changed);
}

#[test]
fn a_source_that_does_not_parse_is_refused() {
    let err =
        format_source("query f() { match { $p: Person } where $p.age > 3 }", None).unwrap_err();
    assert!(err.code.as_str().starts_with('Q'), "{err}");
}

// ---------------------------------------------------------------------------
// The repository.

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn files(dir: &Path, ext: &str, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if !path
                .file_name()
                .is_some_and(|n| n == "target" || n == ".git")
            {
                files(&path, ext, out);
            }
        } else if path.extension().is_some_and(|e| e == ext) {
            out.push(path);
        }
    }
}

/// One query text of the repository: where it is, its schema when it has
/// one, and the text.
struct Text {
    origin: String,
    schema: Option<String>,
    source: String,
}

/// Every `.gq` file, with the `.pg` of its stem when there is one, and every
/// `query` and `mutate` section of the GQT corpus with its case's schema.
fn repository_texts() -> Vec<Text> {
    let root = root();
    let mut out = Vec::new();
    let mut gq = Vec::new();
    files(&root, "gq", &mut gq);
    gq.sort();
    for file in gq {
        out.push(Text {
            origin: file.strip_prefix(&root).unwrap().display().to_string(),
            schema: std::fs::read_to_string(file.with_extension("pg")).ok(),
            source: std::fs::read_to_string(&file).unwrap(),
        });
    }
    let mut cases = Vec::new();
    for dir in [
        "crates/omnigraph-gqt/cases",
        "crates/omnigraph-gqt/cases_slow",
    ] {
        files(&root.join(dir), "gqt", &mut cases);
    }
    cases.sort();
    for file in cases {
        let origin = file.strip_prefix(&root).unwrap().display().to_string();
        let text = std::fs::read_to_string(&file).unwrap();
        let mut schema: Option<String> = None;
        let mut section: Option<(String, String)> = None;
        let mut sections: Vec<(String, String)> = Vec::new();
        for line in text.lines() {
            if let Some(header) = line.strip_prefix("--- ") {
                if let Some(done) = section.take() {
                    sections.push(done);
                }
                section = Some((header.trim().to_string(), String::new()));
            } else if let Some((_, body)) = section.as_mut() {
                body.push_str(line);
                body.push('\n');
            }
        }
        sections.extend(section);
        for (n, (header, body)) in sections.into_iter().enumerate() {
            let word = header.split_whitespace().next().unwrap_or("");
            match word {
                "schema" => schema = Some(body),
                "query" | "mutate" => out.push(Text {
                    origin: format!("{origin}#{n}"),
                    schema: schema.clone(),
                    source: body,
                }),
                _ => {}
            }
        }
    }
    out
}

/// Formatting every query text of the repository leaves every declaration's
/// typed IR (or its refusal's code) unchanged, and formatting the result
/// again changes nothing.
#[test]
fn formatting_the_repository_keeps_every_typed_ir_and_is_idempotent() {
    let texts = repository_texts();
    assert!(texts.len() > 2000, "corpus shrank: {}", texts.len());
    let (mut formatted, mut compared, mut changed) = (0, 0, 0);
    let mut failures = Vec::new();
    for text in &texts {
        let catalog = text
            .schema
            .as_deref()
            .and_then(|s| parse_schema(s).ok())
            .and_then(|s| build_catalog(&s).ok());
        let Ok(out) = format_source(&text.source, catalog.as_ref()) else {
            // A text that does not parse: an expect-error case.
            continue;
        };
        formatted += 1;
        changed += usize::from(out.changed);
        match format_source(&out.text, catalog.as_ref()) {
            Ok(again) if again.text == out.text => {}
            Ok(again) => failures.push(format!(
                "{}: not idempotent\n{}\n--\n{}",
                text.origin, out.text, again.text
            )),
            Err(e) => failures.push(format!(
                "{}: does not reparse: {e}\n{}",
                text.origin, out.text
            )),
        }
        let reparsed = parse_source(&out.text);
        if let (Some(catalog), Ok(after)) = (catalog.as_ref(), reparsed) {
            let before = parse_source(&text.source).unwrap();
            compared += 1;
            if super::typed(catalog, &before) != super::typed(catalog, &after) {
                failures.push(format!(
                    "{}: typed IR changed\n{}\n--\n{}",
                    text.origin, text.source, out.text
                ));
            }
        }
    }
    eprintln!("formatted {formatted}, compared {compared}, changed {changed}");
    assert!(compared > 1500, "{compared}");
    assert!(failures.is_empty(), "{}", failures.join("\n\n"));
}
