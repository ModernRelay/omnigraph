//! The rows of the refusal table: one or more per code of
//! `omnigraph_compiler::query::codes::ALL` that a compile pass raises, each
//! with the schema it is typed against, the refusal's message start and
//! printed fix, and the text one pass of its machine-applicable fixes
//! writes. Ordered by code.

use super::Row;

pub(super) const CHECKED: &str = r#"
interface Named { name: String }
node Person implements Named { handle: String @key  name: String  age: I32?  city: String? }
node Company implements Named { slug: String @key  name: String }
node Doc { id: String @key  title: String  body: String @index  year: I32  tags: [String]  embedding: Vector(3)? }
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

pub(super) const COLLECTED: &str = r#"node Person {
    handle: String @key
    embedding: Vector(2)?
}
edge Knows: Person -> Person

"#;

pub(super) const COMMON: &str = r#"interface Named { name: String }
node Person implements Named {
    handle: String @key
    name: String
    age: I32?
    city: String?
}
node Company implements Named {
    slug: String @key
    name: String
}
node Doc {
    id: String @key
    title: String
    body: String @index
    year: I32
    tags: [String]
    embedding: Vector(3)?
    payload: Blob?
}
node Collection {
    slug: String @key
    min_docs: I32
}
edge WorksAt: Person -> Company @card(0..1)
edge Knows: Person -> Person
edge Authored: Person -> Doc
edge Cites: Doc -> Doc
edge In: Doc -> Collection

"#;

pub(super) const DIRECTIONS: &str = r#"node Person {
    handle: String @key
    name: String
}
node Company {
    slug: String @key
    name: String
}
edge WorksAt: Person -> Company @card(0..1)
edge Founded: Person -> Company

"#;

pub(super) const FULL_TEXT_TITLE: &str = r#"node Doc {
    slug: String @key
    title: String @index(fulltext)
}

"#;

pub(super) const LOADED: &str = r#"node Person {
    id: I64 @key
    name: String
}

"#;

pub(super) const ONE_PERSON: &str = r#"node Person {
    handle: String @key
    name: String
}

"#;

pub(super) const OPTIONAL_SCOPES: &str = r#"node Person {
    handle: String @key
    name: String
    age: I32?
}
node Company {
    slug: String @key
    name: String
}
edge WorksAt: Person -> Company @card(0..1)
edge Knows: Person -> Person

"#;

pub(super) const PRODUCTS: &str = r#"node Person {
    handle: String @key
    name: String
}
node Company {
    slug: String @key
    name: String
}
node Collection {
    slug: String @key
}
edge WorksAt: Person -> Company @card(0..1)

"#;

pub(super) const RANKED_DOCS: &str = r#"node Doc {
    slug: String @key
    title: String
    embedding: Vector(3)
}

"#;

pub(super) const UNRANKED_BODY: &str = r#"node Doc {
    slug: String @key
    body: String @index(fulltext, analyzer = "english_v1", scorer = "none")
}

"#;

pub(super) const ROWS: &[Row] = &[
    Row {
        code: "Q001",
        shape: "grammar mismatch",
        schema: COMMON,
        source: "query q() { match { $p: Person } return { $p.name }",
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "Q001",
        shape: "non ascii name",
        schema: COMMON,
        source: "query q() { match { $p: Persön } return { $p.handle } }",
        message: "expected an element (a binding `$x: Type`, a triple `$a edge $b`, `let`, `from` or `optional { }`) or `where <expression>`; found `ö`",
        fix: None,
        intended: None,
    },
    Row {
        code: "Q001",
        shape: "non ascii name (2)",
        schema: COMMON,
        source: "query q() { match { $é: Person } return { $é.handle } limit 1 }",
        message: "expected an element",
        fix: None,
        intended: None,
    },
    Row {
        code: "Q001",
        shape: "non ascii name (3)",
        schema: COMMON,
        source: "query q() { match { $p: Person, $p knöws $q } return { $q.handle } }",
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "Q001",
        shape: "non ascii name (4)",
        schema: COMMON,
        source: r#"query q() { match { $p: Person } where $p.name = "Zoë return { $p.handle } }"#,
        message: r#"expected an expression; found `"`"#,
        fix: None,
        intended: None,
    },
    Row {
        code: "Q001",
        shape: "non ascii name (5)",
        schema: COMMON,
        source: r#"query w() { insert Persön { handle: "a", name: "b" } }"#,
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "Q002",
        shape: "missing parameter list",
        schema: COMMON,
        source: "query q { match { $p: Person } return { $p.name } }",
        message: "",
        fix: Some("query q()"),
        intended: Some("query q() { match { $p: Person } return { $p.name } }"),
    },
    Row {
        code: "Q002",
        shape: "a declaration without its parameter list",
        schema: CHECKED,
        source: r#"query q {
  match { $p: Person }
  return { $p.name }
}"#,
        message: "",
        fix: Some("query q()"),
        intended: Some("query q() { match { $p: Person } return { $p.name } }"),
    },
    Row {
        code: "Q003",
        shape: "refused setting",
        schema: COMMON,
        source: r#"set no_such_setting = 1;
query q() { match { $p: Person } return { $p.name } }"#,
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "Q003",
        shape: "refused setting (2)",
        schema: COMMON,
        source: r#"set engine = v2;
query q() { match { $p: Person } return { $p.name } }"#,
        message: "unknown setting `engine`; it was removed",
        fix: Some("delete the line: the `engine` setting was removed"),
        intended: Some("query q() { match { $p: Person } return { $p.name } }"),
    },
    Row {
        code: "Q004",
        shape: "load mode append and merge",
        schema: LOADED,
        source: r#"load csv "people.csv" into node:Person mode append header"#,
        message: "the load mode `append` was removed; `insert` means what `append` meant",
        fix: Some("write `mode insert`; `upsert` sets only the properties a record names"),
        intended: Some(r#"load csv "people.csv" into node:Person mode insert header"#),
    },
    Row {
        code: "Q004",
        shape: "load mode append and merge (2)",
        schema: LOADED,
        source: r#"load csv "people.csv" into node:Person mode merge header"#,
        message: "the load mode `merge` was removed; `replace` means what `merge` meant",
        fix: Some("write `mode replace`; `upsert` sets only the properties a record names"),
        intended: Some(r#"load csv "people.csv" into node:Person mode replace header"#),
    },
    Row {
        code: "Q004",
        shape: "statement beside declaration",
        schema: COMMON,
        source: r#"show all;
query q() { match { $p: Person } return { $p.name } }"#,
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "Q005",
        shape: "untyped parameter",
        schema: COMMON,
        source: "query q($h) { match { $p: Person } where $p.handle = $h return { $p.name } }",
        message: "",
        fix: Some("$h: String"),
        intended: Some(
            "query q($h: String) { match { $p: Person } where $p.handle = $h return { $p.name } }",
        ),
    },
    Row {
        code: "Q005",
        shape: "an exponent without a fraction",
        schema: CHECKED,
        source: r#"query q() {
  match { $d: Doc }
  where $d.year * 1.0 > 2e3
  return { $d.title }
}"#,
        message: "",
        fix: Some("2.0e3"),
        intended: Some(
            "query q() { match { $d: Doc } where $d.year * 1.0 > 2000.0 return { $d.title } }",
        ),
    },
    Row {
        code: "Q005",
        shape: "a reserved word as an alias",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: Person }
  return { $p.age as in }
  order { in desc }
}"#,
        message: "",
        fix: Some("as in_; and `in_` for `in`"),
        intended: Some(
            "query q() { match { $p: Person } return { $p.age as in_ } order { in_ desc } }",
        ),
    },
    Row {
        code: "Q005",
        shape: "a reserved alias beside a property whose name holds `order`",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: Person }
  return { $p.age as in, $p.order_id }
  order { in desc }
}"#,
        message: "",
        fix: Some("as in_; and `in_` for `in`"),
        intended: Some(
            "query q() { match { $p: Person } return { $p.age as in_, $p.order_id } order { in_ desc } }",
        ),
    },
    Row {
        code: "Q005",
        shape: "a reserved alias with `order { … }` written in a string before the order",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: Person }
  where $p.note = "order { in }"
  return { $p.age as in }
  order { in }
}"#,
        message: "",
        fix: Some("as in_; and `in_` for `in`"),
        intended: Some(
            r#"query q() { match { $p: Person } where $p.note = "order { in }" return { $p.age as in_ } order { in_ } }"#,
        ),
    },
    Row {
        code: "Q005",
        shape: "an option before a positional argument",
        schema: CHECKED,
        source: r#"query q($q: Vector(3)) {
  match { let $pop = { match { $d: Doc } return { $d } }, from nearest($pop, k: 20, $d.embedding, $q) as $distance }
  return { $d.title }
}"#,
        message: "",
        fix: Some("$pop, $d.embedding, $q, k: 20"),
        intended: Some(
            "query q($q: Vector(3)) { match { let $pop = { match { $d: Doc } return { $d } }, from nearest($pop, $d.embedding, $q, k: 20) as $distance } return { $d.title } }",
        ),
    },
    Row {
        code: "Q005",
        shape: "an assignment with =",
        schema: CHECKED,
        source: r#"query w($h: String) {
  match { $p: Person } where $p.handle = $h
  update $p set { age = 3 }
}"#,
        message: "",
        fix: Some("age: 3"),
        intended: Some(
            "query w($h: String) { match { $p: Person } where $p.handle = $h update $p set { age: 3 } }",
        ),
    },
    Row {
        code: "Q005",
        shape: "a qualified assignment target",
        schema: CHECKED,
        source: r#"query w($h: String) {
  match { $p: Person } where $p.handle = $h
  update $p set { $p.age: 3 }
}"#,
        message: "",
        fix: Some("age: 3"),
        intended: Some(
            "query w($h: String) { match { $p: Person } where $p.handle = $h update $p set { age: 3 } }",
        ),
    },
    Row {
        code: "Q005",
        shape: "update without set",
        schema: CHECKED,
        source: r#"query w($h: String) {
  match { $p: Person } where $p.handle = $h
  update $p { age: 3 }
}"#,
        message: "",
        fix: Some("update $p set { age: 3 }"),
        intended: Some(
            "query w($h: String) { match { $p: Person } where $p.handle = $h update $p set { age: 3 } }",
        ),
    },
    Row {
        code: "Q005",
        shape: "an untyped parameter",
        schema: CHECKED,
        source: r#"query q($h) {
  match { $p: Person } where $p.handle = $h
  return { $p.name }
}"#,
        message: "",
        fix: Some("$h: String"),
        intended: Some(
            "query q($h: String) { match { $p: Person } where $p.handle = $h return { $p.name } }",
        ),
    },
    Row {
        code: "Q006",
        shape: "a source nesting deeper than the compiler accepts",
        schema: COMMON,
        source: "query q() { match { $p: Person } where ⟨nest⟩ return { $p.name } }",
        message: "",
        fix: Some(
            "write a long `or` chain over one property as `$p.name in [...]`, and split deep nesting or a long arithmetic chain into simpler predicates",
        ),
        intended: None,
    },
    Row {
        code: "Q007",
        shape: "arrow traversal",
        schema: COMMON,
        source: "query q() { match { $a: Person, $a -knows-> $b } return { $b.name } }",
        message: "",
        fix: Some("$a knows $b"),
        intended: Some("query q() { match { $a: Person, $a knows $b } return { $b.name } }"),
    },
    Row {
        code: "Q007",
        shape: "an outgoing arrow",
        schema: CHECKED,
        source: r#"query q() {
  match { $a: Person, $a -knows-> $b }
  return { $b.name }
}"#,
        message: "",
        fix: Some("$a knows $b"),
        intended: Some("query q() { match { $a: Person, $a knows $b } return { $b.name } }"),
    },
    Row {
        code: "Q007",
        shape: "an incoming arrow",
        schema: CHECKED,
        source: r#"query q() {
  match { $a: Person, $a <-knows- $b }
  return { $b.name }
}"#,
        message: "",
        fix: Some("$b knows $a"),
        intended: Some("query q() { match { $a: Person, $b knows $a } return { $b.name } }"),
    },
    Row {
        code: "Q007",
        shape: "an undirected arrow",
        schema: CHECKED,
        source: r#"query q() {
  match { $a: Person, $a -knows- $b }
  return { $b.name }
}"#,
        message: "",
        fix: Some("$a <knows> $b"),
        intended: Some("query q() { match { $a: Person, $a <knows> $b } return { $b.name } }"),
    },
    Row {
        code: "Q007",
        shape: "an arrow with an edge binding and bounds",
        schema: CHECKED,
        source: r#"query q() {
  match { $a: Person, $a -$k:knows{1,2}-> $b }
  return { $b.name }
}"#,
        message: "",
        fix: Some("$a $k:knows{1,2} $b"),
        intended: Some(
            "query q() { match { $a: Person, $a $k:knows{1,2} $b } return { $b.name } }",
        ),
    },
    Row {
        code: "Q007",
        shape: "Cypher's bracketed edge",
        schema: CHECKED,
        source: r#"query q() {
  match { $a: Person, $a -[:knows]-> $b }
  return { $b.name }
}"#,
        message: "",
        fix: Some("$a knows $b"),
        intended: Some("query q() { match { $a: Person, $a knows $b } return { $b.name } }"),
    },
    Row {
        code: "Q008",
        shape: "where on a statement",
        schema: COMMON,
        source: "query w($h: String) { delete Person where handle = $h }",
        message: "",
        fix: Some("match { $x: Person } where $x.handle = $h  delete $x"),
        intended: Some(
            "query w($h: String) { match { $x: Person } where $x.handle = $h  delete $x }",
        ),
    },
    Row {
        code: "Q008",
        shape: "delete T where",
        schema: CHECKED,
        source: r#"query w($h: String) {
  delete Person where handle = $h
}"#,
        message: "",
        fix: Some("match { $x: Person } where $x.handle = $h  delete $x"),
        intended: Some(
            "query w($h: String) { match { $x: Person } where $x.handle = $h delete $x }",
        ),
    },
    Row {
        code: "Q008",
        shape: "update T set where",
        schema: CHECKED,
        source: r#"query w($h: String) {
  update Person set { age: 3 } where handle = $h
}"#,
        message: "",
        fix: Some("match { $x: Person } where $x.handle = $h  update $x set { age: 3 }"),
        intended: Some(
            "query w($h: String) { match { $x: Person } where $x.handle = $h update $x set { age: 3 } }",
        ),
    },
    Row {
        code: "Q008",
        shape: "delete $x where",
        schema: CHECKED,
        source: r#"query w() {
  match { $p: Person }
  delete $p where $p.age > 90
}"#,
        message: "",
        fix: Some("delete $p; and ` where $p.age > 90` for ``"),
        intended: Some("query w() { match { $p: Person } where $p.age > 90 delete $p }"),
    },
    Row {
        code: "Q008",
        shape: "delete $x where beside the group's where",
        schema: CHECKED,
        source: r#"query w() {
  match { $p: Person } where $p.age < 100 or $p.age > 120
  delete $p where $p.age > 90 or $p.age < 3
}"#,
        message: "",
        fix: Some("delete $p; and `(` for ``; and `) and ($p.age > 90 or $p.age < 3)` for ``"),
        intended: Some(
            "query w() { match { $p: Person } where ($p.age < 100 or $p.age > 120) and ($p.age > 90 or $p.age < 3) delete $p }",
        ),
    },
    Row {
        code: "Q008",
        shape: "delete over an edge type where",
        schema: CHECKED,
        source: r#"query w($h: String) {
  delete WorksAt where @src = $h
}"#,
        message: "",
        fix: Some("match { $x: WorksAt } where $x.@src = $h  delete $x"),
        intended: Some(
            "query w($h: String) { match { $x: WorksAt } where $x.@src = $h delete $x }",
        ),
    },
    Row {
        code: "Q008",
        shape: "a where fix that keeps a parameter",
        schema: CHECKED,
        source: r#"query w($x: String) {
  delete Person where handle = $x
}"#,
        message: "",
        fix: Some("match { $p: Person } where $p.handle = $x  delete $p"),
        intended: Some(
            "query w($x: String) { match { $p: Person } where $p.handle = $x delete $p }",
        ),
    },
    Row {
        code: "Q008",
        shape: "a where fix that qualifies an assignment",
        schema: CHECKED,
        source: r#"query w() {
  update Person set { name: handle } where age > 3
}"#,
        message: "",
        fix: Some("match { $x: Person } where $x.age > 3  update $x set { name: $x.handle }"),
        intended: Some(
            "query w() { match { $x: Person } where $x.age > 3 update $x set { name: $x.handle } }",
        ),
    },
    Row {
        code: "Q009",
        shape: "mutation keyword",
        schema: COMMON,
        source: "mutation w($h: String) { match { $p: Person } where $p.handle = $h delete $p }",
        message: "",
        fix: Some("query"),
        intended: Some(
            "query w($h: String) { match { $p: Person } where $p.handle = $h delete $p }",
        ),
    },
    Row {
        code: "Q009",
        shape: "the mutation keyword",
        schema: CHECKED,
        source: r#"mutation hire($h: String) {
  insert Person { handle: $h, name: $h }
}"#,
        message: "",
        fix: Some("query"),
        intended: Some("query hire($h: String) { insert Person { handle: $h, name: $h } }"),
    },
    Row {
        code: "Q009",
        shape: "the view keyword",
        schema: CHECKED,
        source: r#"view big() {
  match { $c: Company }
  return { $c.name }
}"#,
        message: "",
        fix: Some("query"),
        intended: Some("query big() { match { $c: Company } return { $c.name } }"),
    },
    Row {
        code: "Q010",
        shape: "comparison in binding braces",
        schema: COMMON,
        source: "query q() { match { $d: Doc { year >= 2025 } } return { $d.title } }",
        message: "",
        fix: Some("match { $d: Doc } where $d.year >= 2025"),
        intended: Some("query q() { match { $d: Doc } where $d.year >= 2025 return { $d.title } }"),
    },
    Row {
        code: "Q010",
        shape: "a property map",
        schema: CHECKED,
        source: r#"query q($h: String) {
  match { $p: Person { handle: $h }, $p knows $q }
  return { $q.name }
}"#,
        message: "",
        fix: Some("match { $p: Person, $p knows $q } where $p.handle = $h"),
        intended: Some(
            "query q($h: String) { match { $p: Person, $p knows $q } where $p.handle = $h return { $q.name } }",
        ),
    },
    Row {
        code: "Q010",
        shape: "a comparison inside binding braces",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: Person { age > 30 } }
  return { $p.name }
}"#,
        message: "",
        fix: Some("match { $p: Person } where $p.age > 30"),
        intended: Some("query q() { match { $p: Person } where $p.age > 30 return { $p.name } }"),
    },
    Row {
        code: "Q010",
        shape: "a colon comparison inside binding braces",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: Person { age: > 30 } }
  return { $p.name }
}"#,
        message: "",
        fix: Some("match { $p: Person } where $p.age > 30"),
        intended: Some("query q() { match { $p: Person } where $p.age > 30 return { $p.name } }"),
    },
    Row {
        code: "Q011",
        shape: "as commit on a read",
        schema: COMMON,
        source: "query q() as commit { match { $p: Person } return { $p.name } }",
        message: "",
        fix: Some("remove `as commit`"),
        intended: Some("query q() { match { $p: Person } return { $p.name } }"),
    },
    Row {
        code: "Q011",
        shape: "as commit on a read",
        schema: CHECKED,
        source: r#"query q() as commit {
  match { $p: Person }
  return { $p.name }
}"#,
        message: "",
        fix: Some("remove `as commit`"),
        intended: Some("query q() { match { $p: Person } return { $p.name } }"),
    },
    Row {
        code: "Q012",
        shape: "annotation with empty parentheses",
        schema: COMMON,
        source: "query q() @deprecated() { match { $p: Person } return { $p.name } }",
        message: "`@deprecated()`: an annotation without arguments takes no parentheses",
        fix: Some("@deprecated"),
        intended: Some("query q() @deprecated { match { $p: Person } return { $p.name } }"),
    },
    Row {
        code: "Q012",
        shape: "a chained comparison under `not`",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: Person }
  where not 1 < $p.age < 5
  return { $p.name }
}"#,
        message: "",
        fix: Some("(1 < $p.age and $p.age < 5)"),
        intended: Some(
            "query q() { match { $p: Person } where not (1 < $p.age and $p.age < 5) return { $p.name } }",
        ),
    },
    Row {
        code: "Q012",
        shape: "for over a relation",
        schema: CHECKED,
        source: r#"query q() {
  match { for $x in { match { $p: Person } return { $p } } }
  return { $x.name }
}"#,
        message: "",
        fix: Some("from { match { $p: Person } return { $p } }"),
        intended: Some(
            "query q() { match { from { match { $p: Person } return { $p } } } return { $x.name } }",
        ),
    },
    Row {
        code: "Q012",
        shape: "let … in a relation",
        schema: CHECKED,
        source: r#"query q() {
  match { let $x in { match { $p: Person } return { $p } } }
  return { $x.name }
}"#,
        message: "",
        fix: Some("from { match { $p: Person } return { $p } }"),
        intended: Some(
            "query q() { match { from { match { $p: Person } return { $p } } } return { $x.name } }",
        ),
    },
    Row {
        code: "Q012",
        shape: "let over a tuple",
        schema: CHECKED,
        source: r#"query q() {
  match { let ($a, $b) in { match { $p worksAt $c } return { $p, $c } } }
  return { $a.name }
}"#,
        message: "",
        fix: Some("from { match { $p worksAt $c } return { $p, $c } }"),
        intended: Some(
            "query q() { match { from { match { $p worksAt $c } return { $p, $c } } } return { $a.name } }",
        ),
    },
    Row {
        code: "Q012",
        shape: "a chained comparison",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: Person }
  where 18 <= $p.age < 65
  return { $p.name }
}"#,
        message: "",
        fix: Some("(18 <= $p.age and $p.age < 65)"),
        intended: Some(
            "query q() { match { $p: Person } where 18 <= $p.age and $p.age < 65 return { $p.name } }",
        ),
    },
    Row {
        code: "Q012",
        shape: "not { } among the elements",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: Person, not { $p worksAt $c } }
  return { $p.name }
}"#,
        message: "",
        fix: Some("match { $p: Person } where not exists { $p worksAt $c }"),
        intended: Some(
            "query q() { match { $p: Person } where not exists { $p worksAt $c } return { $p.name } }",
        ),
    },
    Row {
        code: "Q012",
        shape: "not { } in where",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: Person }
  where $p.age > 3 and not { $p worksAt $c }
  return { $p.name }
}"#,
        message: "",
        fix: Some("not exists { $p worksAt $c }"),
        intended: Some(
            "query q() { match { $p: Person } where $p.age > 3 and not exists { $p worksAt $c } return { $p.name } }",
        ),
    },
    Row {
        code: "Q012",
        shape: "a group of one edge name",
        schema: CHECKED,
        source: r#"query q() {
  match { $a: Person, $a (knows) $b }
  return { $b.name }
}"#,
        message: "",
        fix: Some("knows"),
        intended: Some("query q() { match { $a: Person, $a knows $b } return { $b.name } }"),
    },
    Row {
        code: "Q013",
        shape: "element after the last statement",
        schema: COMMON,
        source: "query w($h: String) { match { $p: Person } where $p.handle = $h delete $p  $q: Person }",
        message: "",
        fix: Some(
            "delete $p; and `match { $p: Person, $q: Person } where $p.handle = $h` for `match { $p: Person } where $p.handle = $h`",
        ),
        intended: Some(
            "query w($h: String) { match { $p: Person, $q: Person } where $p.handle = $h delete $p }",
        ),
    },
    Row {
        code: "Q013",
        shape: "an element after the last statement",
        schema: CHECKED,
        source: r#"query w($h: String) {
  delete $p
  $p: Person
}"#,
        message: "",
        fix: Some(
            r#"match { $p: Person }
  delete $p"#,
        ),
        intended: Some("query w($h: String) { match { $p: Person } delete $p }"),
    },
    Row {
        code: "Q013",
        shape: "a match after its statement",
        schema: CHECKED,
        source: r#"query w() {
  delete $p  match { $p: Person } where $p.age > 3
}"#,
        message: "",
        fix: Some("match { $p: Person } where $p.age > 3  delete $p"),
        intended: Some("query w() { match { $p: Person } where $p.age > 3 delete $p }"),
    },
    Row {
        code: "Q013",
        shape: "a write's elements without match",
        schema: CHECKED,
        source: r#"query w($h: String) {
  $p: Person, $p.handle = $h
  delete $p
}"#,
        message: "",
        fix: Some("match { $p: Person } where $p.handle = $h"),
        intended: Some(
            "query w($h: String) { match { $p: Person } where $p.handle = $h delete $p }",
        ),
    },
    Row {
        code: "Q013",
        shape: "a write's elements without match, with a property map",
        schema: CHECKED,
        source: r#"query w($h: String) {
  $p: Person { handle: $h }
  update $p set { age: 3 }
}"#,
        message: "",
        fix: Some("match { $p: Person } where $p.handle = $h"),
        intended: Some(
            "query w($h: String) { match { $p: Person } where $p.handle = $h update $p set { age: 3 } }",
        ),
    },
    Row {
        code: "Q014",
        shape: "`between` under `not`",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: Person }
  where not $p.age between 1 and 5
  return { $p.name }
}"#,
        message: "",
        fix: Some("($p.age >= 1 and $p.age <= 5)"),
        intended: Some(
            "query q() { match { $p: Person } where not ($p.age >= 1 and $p.age <= 5) return { $p.name } }",
        ),
    },
    Row {
        code: "Q014",
        shape: "a Cypher pattern in match",
        schema: CHECKED,
        source: r#"query q() {
  match { (a:Person)-[:KNOWS]->(b) }
  return { $b.name }
}"#,
        message: "",
        fix: Some("$a: Person, $a knows $b"),
        intended: Some("query q() { match { $a: Person, $a knows $b } return { $b.name } }"),
    },
    Row {
        code: "Q014",
        shape: "ORDER BY",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: Person }
  return { $p.name }
  order by $p.age desc
}"#,
        message: "",
        fix: Some("order { $p.age desc }"),
        intended: Some(
            "query q() { match { $p: Person } return { $p.name } order { $p.age desc } }",
        ),
    },
    Row {
        code: "Q014",
        shape: "count(*)",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: Person }
  return { count(*) as n }
}"#,
        message: "",
        fix: Some("count($p)"),
        intended: Some("query q() { match { $p: Person } return { count($p) as n } }"),
    },
    Row {
        code: "Q014",
        shape: "Cypher's WITH",
        schema: CHECKED,
        source: r#"query q() {
  match { $p worksAt $c, with $c, count($p) as n }
  where $n > 3
  return { $c.name }
}"#,
        message: "",
        fix: Some("from { match { $p worksAt $c } return { $c, count($p) as n } }"),
        intended: Some(
            "query q() { match { from { match { $p worksAt $c } return { $c, count($p) as n } } } where $n > 3 return { $c.name } }",
        ),
    },
    Row {
        code: "Q014",
        shape: "STARTS WITH",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: Person }
  where $p.name STARTS WITH "A"
  return { $p.name }
}"#,
        message: "",
        fix: Some(r#"starts_with($p.name, "A")"#),
        intended: Some(
            r#"query q() { match { $p: Person } where starts_with($p.name, "A") return { $p.name } }"#,
        ),
    },
    Row {
        code: "Q014",
        shape: "CREATE",
        schema: CHECKED,
        source: r#"query w() {
  CREATE (p:Person {handle: "x", name: "x"})
}"#,
        message: "",
        fix: Some(r#"insert Person { handle: "x", name: "x" }"#),
        intended: Some(r#"query w() { insert Person { handle: "x", name: "x" } }"#),
    },
    Row {
        code: "Q014",
        shape: "MERGE",
        schema: CHECKED,
        source: r#"query w() {
  MERGE (p:Person {handle: "x", name: "x"})
}"#,
        message: "",
        fix: Some(r#"upsert Person { handle: "x", name: "x" }"#),
        intended: Some(r#"query w() { upsert Person { handle: "x", name: "x" } }"#),
    },
    Row {
        code: "Q014",
        shape: "SET",
        schema: CHECKED,
        source: r#"query w($h: String) {
  match { $p: Person } where $p.handle = $h
  set $p.age = 3
}"#,
        message: "",
        fix: Some("update $p set { age: 3 }"),
        intended: Some(
            "query w($h: String) { match { $p: Person } where $p.handle = $h update $p set { age: 3 } }",
        ),
    },
    Row {
        code: "Q014",
        shape: "DETACH DELETE",
        schema: CHECKED,
        source: r#"query w($h: String) {
  match { $p: Person } where $p.handle = $h
  detach delete $p
}"#,
        message: "",
        fix: Some("remove `detach`"),
        intended: Some(
            "query w($h: String) { match { $p: Person } where $p.handle = $h delete $p }",
        ),
    },
    Row {
        code: "Q014",
        shape: "OPTIONAL MATCH",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: Person, optional match { $c: Company, $p worksAt $c } }
  return { $p.name, $c.name }
}"#,
        message: "",
        fix: Some("optional { $c: Company, $p worksAt $c }"),
        intended: Some(
            "query q() { match { $p: Person, optional { $c: Company, $p worksAt $c } } return { $p.name, $c.name } }",
        ),
    },
    Row {
        code: "Q014",
        shape: "CALL { }",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: Person, call { match { $p knows $q } return { $q } } }
  return { $q.name }
}"#,
        message: "",
        fix: Some("from { match { $p knows $q } return { $q } }"),
        intended: Some(
            "query q() { match { $p: Person, from { match { $p knows $q } return { $q } } } return { $q.name } }",
        ),
    },
    Row {
        code: "Q014",
        shape: "==",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: Person }
  where $p.city == "Berlin"
  return { $p.name }
}"#,
        message: "",
        fix: Some(r#"where $p.city = "Berlin""#),
        intended: Some(
            r#"query q() { match { $p: Person } where $p.city = "Berlin" return { $p.name } }"#,
        ),
    },
    Row {
        code: "Q014",
        shape: "GROUP BY",
        schema: CHECKED,
        source: r#"query q() {
  match { $p worksAt $c }
  return { $c.name, count($p) as n }
  group by $c.name
}"#,
        message: "",
        fix: Some("remove `group by $c.name`"),
        intended: Some("query q() { match { $p worksAt $c } return { $c.name, count($p) as n } }"),
    },
    Row {
        code: "Q014",
        shape: "LIKE",
        schema: CHECKED,
        source: r#"query q() {
  match { $d: Doc }
  where $d.title LIKE '%graph%'
  return { $d.title }
}"#,
        message: "",
        fix: Some(r#"contains($d.title, "graph")"#),
        intended: Some(
            r#"query q() { match { $d: Doc } where contains($d.title, "graph") return { $d.title } }"#,
        ),
    },
    Row {
        code: "Q014",
        shape: "BETWEEN",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: Person }
  where $p.age BETWEEN 18 AND 65
  return { $p.name }
}"#,
        message: "",
        fix: Some("($p.age >= 18 and $p.age <= 65)"),
        intended: Some(
            "query q() { match { $p: Person } where $p.age >= 18 and $p.age <= 65 return { $p.name } }",
        ),
    },
    Row {
        code: "Q014",
        shape: "in over a parenthesised list",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: Person }
  where $p.age in (30, 40)
  return { $p.name }
}"#,
        message: "",
        fix: Some("$p.age in [30, 40]"),
        intended: Some(
            "query q() { match { $p: Person } where $p.age in [30, 40] return { $p.name } }",
        ),
    },
    Row {
        code: "Q014",
        shape: "not in",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: Person }
  where $p.city not in ["Berlin"]
  return { $p.name }
}"#,
        message: "",
        fix: Some(r#"not $p.city in ["Berlin"]"#),
        intended: Some(
            r#"query q() { match { $p: Person } where not $p.city in ["Berlin"] return { $p.name } }"#,
        ),
    },
    Row {
        code: "Q014",
        shape: "&&",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: Person }
  where $p.age >= 18 && $p.age < 65
  return { $p.name }
}"#,
        message: "",
        fix: Some("where $p.age >= 18 and $p.age < 65"),
        intended: Some(
            "query q() { match { $p: Person } where $p.age >= 18 and $p.age < 65 return { $p.name } }",
        ),
    },
    Row {
        code: "Q014",
        shape: "! for not",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: Person }
  where !($p.age > 3)
  return { $p.name }
}"#,
        message: "",
        fix: Some("where not ($p.age > 3)"),
        intended: Some(
            "query q() { match { $p: Person } where not $p.age > 3 return { $p.name } }",
        ),
    },
    Row {
        code: "Q014",
        shape: "a Cypher binding in parentheses",
        schema: CHECKED,
        source: r#"query q($h: String) {
  match { $p: Person(handle: $h) }
  return { $p.name }
}"#,
        message: "",
        fix: Some("match { $p: Person } where $p.handle = $h"),
        intended: Some(
            "query q($h: String) { match { $p: Person } where $p.handle = $h return { $p.name } }",
        ),
    },
    Row {
        code: "Q014",
        shape: "a lowercase type",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: person }
  return { $p.name }
}"#,
        message: "",
        fix: Some("$p: Person"),
        intended: Some("query q() { match { $p: Person } return { $p.name } }"),
    },
    Row {
        code: "Q014",
        shape: "an uppercase variable",
        schema: CHECKED,
        source: r#"query q() {
  match { $P: Person }
  return { $P.name }
}"#,
        message: "",
        fix: Some("$p"),
        intended: Some("query q() { match { $p: Person } return { $p.name } }"),
    },
    Row {
        code: "Q014",
        shape: "a star projection",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: Person }
  return { $p.* }
}"#,
        message: "",
        fix: Some("$p"),
        intended: Some("query q() { match { $p: Person } return { $p } }"),
    },
    Row {
        code: "Q014",
        shape: "distinct inside return",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: Person }
  return { distinct $p.city }
}"#,
        message: "",
        fix: Some("return distinct { $p.city }"),
        intended: Some("query q() { match { $p: Person } return distinct { $p.city } }"),
    },
    Row {
        code: "Q014",
        shape: "a string alias",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: Person }
  return { $p.name as "Name" }
}"#,
        message: "",
        fix: Some("as name"),
        intended: Some("query q() { match { $p: Person } return { $p.name as name } }"),
    },
    Row {
        code: "Q014",
        shape: "a SQL comment",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: Person }
  where -- adults only
  $p.age > 18
  return { $p.name }
}"#,
        message: "",
        fix: Some(
            r#"where // adults only
  $p.age > 18"#,
        ),
        intended: Some("query q() { match { $p: Person } where $p.age > 18 return { $p.name } }"),
    },
    Row {
        code: "Q014",
        shape: "a semicolon after a declaration",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: Person }
  return { $p.name }
};"#,
        message: "",
        fix: Some("remove `;`"),
        intended: Some("query q() { match { $p: Person } return { $p.name } }"),
    },
    Row {
        code: "Q014",
        shape: "optional without braces",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: Person, optional $p worksAt $c }
  return { $p.name, $c.name }
}"#,
        message: "",
        fix: Some("optional { $p worksAt $c }"),
        intended: Some(
            "query q() { match { $p: Person, optional { $p worksAt $c } } return { $p.name, $c.name } }",
        ),
    },
    Row {
        code: "Q014",
        shape: "not without braces",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: Person, $q: Person, not $p knows $q }
  return { $p.name }
}"#,
        message: "",
        fix: Some("match { $p: Person, $q: Person } where not exists { $p knows $q }"),
        intended: Some(
            "query q() { match { $p: Person, $q: Person } where not exists { $p knows $q } return { $p.name } }",
        ),
    },
    Row {
        code: "Q014",
        shape: "delete of two variables",
        schema: CHECKED,
        source: r#"query w() {
  match { $p knows $q }
  delete $p, $q
}"#,
        message: "",
        fix: Some("delete $p  delete $q"),
        intended: Some("query w() { match { $p knows $q } delete $p delete $q }"),
    },
    Row {
        code: "Q014",
        shape: "a direction before its key",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: Person }
  return { $p.name }
  order { desc $p.age }
}"#,
        message: "",
        fix: Some("$p.age desc"),
        intended: Some(
            "query q() { match { $p: Person } return { $p.name } order { $p.age desc } }",
        ),
    },
    Row {
        code: "Q014",
        shape: "a SQL alias = value",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: Person }
  return { n = $p.name }
}"#,
        message: "",
        fix: Some("$p.name as n"),
        intended: Some("query q() { match { $p: Person } return { $p.name as n } }"),
    },
    Row {
        code: "Q014",
        shape: "`&&` in a parenthesized predicate of a `union` arm, which a read that begins with `match` never reads as one without it",
        schema: COMMON,
        source: "query q() { match { $a: Person, from union({ match { $b: Person } where ($b.age = 1 && true) return { $b as u } }, { match { $c: Person } return { $c as u } }) } return { $a.name } }",
        message: "",
        fix: Some("where ($b.age = 1 and true)"),
        intended: Some(
            "query q() { match { $a: Person, from union({ match { $b: Person } where ($b.age = 1 and true) return { $b as u } }, { match { $c: Person } return { $c as u } }) } return { $a.name } }",
        ),
    },
    Row {
        code: "Q014",
        shape: "`starts with` in words",
        schema: COMMON,
        source: r#"query q() { match { $p: Person } where $p.name starts with "A" return { $p.name } }"#,
        message: "",
        fix: Some(r#"starts_with($p.name, "A")"#),
        intended: Some(
            r#"query q() { match { $p: Person } where starts_with($p.name, "A") return { $p.name } }"#,
        ),
    },
    Row {
        code: "Q015",
        shape: "offset is a later stage",
        schema: COMMON,
        source: "query q() { match { $p: Person } return { $p.name } offset 10 }",
        message: "",
        fix: Some("remove `offset 10`"),
        intended: Some("query q() { match { $p: Person } return { $p.name } }"),
    },
    Row {
        code: "Q015",
        shape: "an `assert` inside a `returning`, two overlapping deletions",
        schema: CHECKED,
        source: r#"query w() {
  insert Person { name: "a" }
  returning { assert { match { $p: Person } return { $p } } }
}"#,
        message: "",
        fix: Some("remove `returning { assert { match { $p: Person } return { $p } } }`"),
        intended: Some(r#"query w() { insert Person { name: "a" } }"#),
    },
    Row {
        code: "Q015",
        shape: "returning, a later stage",
        schema: CHECKED,
        source: r#"query w($h: String) {
  match { $p: Person } where $p.handle = $h
  delete $p
  returning { @id }
}"#,
        message: "",
        fix: Some("remove `returning { @id }`"),
        intended: Some(
            "query w($h: String) { match { $p: Person } where $p.handle = $h delete $p }",
        ),
    },
    Row {
        code: "Q015",
        shape: "a namespaced type, a later stage",
        schema: CHECKED,
        source: r#"query q() {
  match { $o: crm::Person }
  return { $o.name }
}"#,
        message: "",
        fix: Some("$o: Person"),
        intended: Some("query q() { match { $o: Person } return { $o.name } }"),
    },
    Row {
        code: "Q015",
        shape: "offset, a later stage",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: Person }
  return { $p.name }
  limit 10 offset 5
}"#,
        message: "",
        fix: Some("remove `offset 5`"),
        intended: Some("query q() { match { $p: Person } return { $p.name } limit 10 }"),
    },
    Row {
        code: "Q016",
        shape: "missing comma",
        schema: COMMON,
        source: "query q() { match { $p: Person $p knows $q } return { $q.name } }",
        message: "",
        fix: Some("match { $p: Person, $p knows $q }"),
        intended: Some("query q() { match { $p: Person, $p knows $q } return { $q.name } }"),
    },
    Row {
        code: "Q016",
        shape: "a missing comma after an element that a comment follows",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: Person // the person
    $q: Person }
  return { $p.name }
}"#,
        message: "",
        fix: Some("match { $p: Person, $q: Person }"),
        intended: Some("query q() { match { $p: Person, $q: Person } return { $p.name } }"),
    },
    Row {
        code: "Q016",
        shape: "a missing comma between elements",
        schema: CHECKED,
        source: r#"query q() {
  match {
    $p: Person
    $p knows $q
  }
  return { $q.name }
}"#,
        message: "",
        fix: Some("match { $p: Person, $p knows $q }"),
        intended: Some("query q() { match { $p: Person, $p knows $q } return { $q.name } }"),
    },
    Row {
        code: "Q016",
        shape: "a missing comma between return items",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: Person }
  return {
    $p.name
    $p.age
  }
}"#,
        message: "",
        fix: Some(
            r#"return {
    $p.name,
    $p.age
  }"#,
        ),
        intended: Some("query q() { match { $p: Person } return { $p.name, $p.age } }"),
    },
    Row {
        code: "Q016",
        shape: "a missing comma between order keys",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: Person }
  return { $p.name }
  order { $p.age desc $p.name }
}"#,
        message: "",
        fix: Some("order { $p.age desc, $p.name }"),
        intended: Some(
            "query q() { match { $p: Person } return { $p.name } order { $p.age desc, $p.name } }",
        ),
    },
    Row {
        code: "Q016",
        shape: "a missing comma between arguments",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: Person }
  where contains($p.name "a")
  return { $p.name }
}"#,
        message: "",
        fix: Some(r#"$p.name, "a""#),
        intended: Some(
            r#"query q() { match { $p: Person } where contains($p.name, "a") return { $p.name } }"#,
        ),
    },
    Row {
        code: "Q016",
        shape: "a missing comma after a string whose later line holds `//`",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: Person }
  return {
    $p.name
    "a
// b" as note
  }
}"#,
        message: "",
        fix: Some(
            r#"return {
    $p.name,
    "a
// b" as note
  }"#,
        ),
        intended: Some(
            r#"query q() { match { $p: Person } return { $p.name, "a
// b" as note } }"#,
        ),
    },
    Row {
        code: "Q016",
        shape: "a missing comma after an element ending in a string whose later line holds `//`",
        schema: CHECKED,
        source: r#"query q() {
  match {
    $p: Person
    let $s = "x
// y"
    $p knows $q
  }
  return { $q.name, $s }
}"#,
        message: "",
        fix: Some(
            r#"match { $p: Person, let $s = "x
// y", $p knows $q }"#,
        ),
        intended: Some(
            r#"query q() { match { $p: Person, let $s = "x
// y", $p knows $q } return { $q.name, $s } }"#,
        ),
    },
    Row {
        code: "Q016",
        shape: "a missing comma after an element followed by a comment",
        schema: CHECKED,
        source: r#"query q() {
  match {
    $p: Person // the person
    $p knows $q /* their friend */
    $q knows $r
  }
  return { $r.name }
}"#,
        message: "",
        fix: Some("match { $p: Person, $p knows $q, $q knows $r }"),
        intended: Some(
            "query q() { match { $p: Person, $p knows $q, $q knows $r } return { $r.name } }",
        ),
    },
    Row {
        code: "Q016",
        shape: "a missing comma between assignments",
        schema: CHECKED,
        source: r#"query w() {
  insert Person { handle: "h" name: "n" }
}"#,
        message: "",
        fix: Some(r#"handle: "h", name: "n""#),
        intended: Some(r#"query w() { insert Person { handle: "h", name: "n" } }"#),
    },
    Row {
        code: "Q017",
        shape: "filter inside match",
        schema: COMMON,
        source: "query q() { match { $p: Person, $p.age > 30 } return { $p.name } }",
        message: "",
        fix: Some("match { $p: Person } where $p.age > 30"),
        intended: Some("query q() { match { $p: Person } where $p.age > 30 return { $p.name } }"),
    },
    Row {
        code: "Q017",
        shape: "an uppercase WHERE inside match",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: Person WHERE $p.age > 30 }
  return { $p.name }
}"#,
        message: "",
        fix: Some("match { $p: Person } where $p.age > 30"),
        intended: Some("query q() { match { $p: Person } where $p.age > 30 return { $p.name } }"),
    },
    Row {
        code: "Q017",
        shape: "a filter among the elements",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: Person, $p.age > 30 }
  return { $p.name }
}"#,
        message: "",
        fix: Some("match { $p: Person } where $p.age > 30"),
        intended: Some("query q() { match { $p: Person } where $p.age > 30 return { $p.name } }"),
    },
    Row {
        code: "Q017",
        shape: "a where inside the match braces",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: Person where $p.age > 30 }
  return { $p.name }
}"#,
        message: "",
        fix: Some("match { $p: Person } where $p.age > 30"),
        intended: Some("query q() { match { $p: Person } where $p.age > 30 return { $p.name } }"),
    },
    Row {
        code: "Q017",
        shape: "a filter among a block's elements",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: Person }
  where exists { $p knows $q, $q.age > 3 }
  return { $p.name }
}"#,
        message: "",
        fix: Some("$p knows $q where $q.age > 3"),
        intended: Some(
            "query q() { match { $p: Person } where exists { $p knows $q where $q.age > 3 } return { $p.name } }",
        ),
    },
    Row {
        code: "Q018",
        shape: "bare name",
        schema: COMMON,
        source: r#"query q() { match { $p: Person } where name = "x" return { $p.name } }"#,
        message: "",
        fix: Some("$p.name"),
        intended: Some(
            r#"query q() { match { $p: Person } where $p.name = "x" return { $p.name } }"#,
        ),
    },
    Row {
        code: "Q018",
        shape: "a bare property name",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: Person }
  where name = "x"
  return { $p.name }
}"#,
        message: "",
        fix: Some("$p.name"),
        intended: Some(
            r#"query q() { match { $p: Person } where $p.name = "x" return { $p.name } }"#,
        ),
    },
    Row {
        code: "Q018",
        shape: "a bare parameter name",
        schema: CHECKED,
        source: r#"query q($h: String) {
  match { $p: Person }
  where $p.handle = h
  return { $p.name }
}"#,
        message: "",
        fix: Some("$h"),
        intended: Some(
            "query q($h: String) { match { $p: Person } where $p.handle = $h return { $p.name } }",
        ),
    },
    Row {
        code: "Q019",
        shape: "element keyword call",
        schema: COMMON,
        source: "query q() { match { $p: Person } where exists($p.city) return { $p.name } }",
        message: "",
        fix: Some("$p.city is not null"),
        intended: Some(
            "query q() { match { $p: Person } where $p.city is not null return { $p.name } }",
        ),
    },
    Row {
        code: "Q019",
        shape: "element keyword call (2)",
        schema: COMMON,
        source: "query q() { match { $p: Person } return { $p.name, where($p.city) as w } }",
        message: "`where` is a keyword, and a call is never named one",
        fix: Some("match { … } where <predicate>"),
        intended: None,
    },
    Row {
        code: "Q019",
        shape: "element keyword call (3)",
        schema: COMMON,
        source: "query q() { match { $p: Person } where and($p.age > 30, $p.city is null) return { $p.name } }",
        message: "`and` is a reserved word, and a call is never named one",
        fix: Some("$p.age > 30 and $p.city is null"),
        intended: Some(
            "query q() { match { $p: Person } where $p.age > 30 and $p.city is null return { $p.name } }",
        ),
    },
    Row {
        code: "Q019",
        shape: "from with parentheses",
        schema: CHECKED,
        source: r#"query q() {
  match { from ({ match { $p: Person } return { $p } }) }
  return { $p.name }
}"#,
        message: "",
        fix: Some("from { match { $p: Person } return { $p } }"),
        intended: Some(
            "query q() { match { from { match { $p: Person } return { $p } } } return { $p.name } }",
        ),
    },
    Row {
        code: "Q019",
        shape: "exists as a function",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: Person }
  where exists($p.city)
  return { $p.name }
}"#,
        message: "",
        fix: Some("$p.city is not null"),
        intended: Some(
            "query q() { match { $p: Person } where $p.city is not null return { $p.name } }",
        ),
    },
    Row {
        code: "Q020",
        shape: "match without braces",
        schema: COMMON,
        source: "query q() { match $p: Person  return { $p.name } }",
        message: "",
        fix: Some("match { $p: Person }"),
        intended: Some("query q() { match { $p: Person }  return { $p.name } }"),
    },
    Row {
        code: "Q020",
        shape: "an inline read without match",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: Person, from { $p knows $q return { $q } order { $q.age } } }
  return { $p.name }
}"#,
        message: "",
        fix: Some("match { $p knows $q } return { $q } order { $q.age }"),
        intended: Some(
            "query q() { match { $p: Person, from { match { $p knows $q } return { $q } order { $q.age } } } return { $p.name } }",
        ),
    },
    Row {
        code: "Q020",
        shape: "return without braces",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: Person }
  return $p.name
}"#,
        message: "",
        fix: Some("return { $p.name }"),
        intended: Some("query q() { match { $p: Person } return { $p.name } }"),
    },
    Row {
        code: "Q020",
        shape: "match without braces",
        schema: CHECKED,
        source: r#"query q() {
  match $p: Person
  return { $p.name }
}"#,
        message: "",
        fix: Some("match { $p: Person }"),
        intended: Some("query q() { match { $p: Person } return { $p.name } }"),
    },
    Row {
        code: "Q020",
        shape: "return before match",
        schema: CHECKED,
        source: r#"query q() {
  return { $p.name }
  match { $p: Person }
}"#,
        message: "",
        fix: Some(
            r#"match { $p: Person }
  return { $p.name }"#,
        ),
        intended: Some("query q() { match { $p: Person } return { $p.name } }"),
    },
    Row {
        code: "Q020",
        shape: "limit before order",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: Person }
  return { $p.name }
  limit 10
  order { $p.age }
}"#,
        message: "",
        fix: Some(
            r#"order { $p.age }
  limit 10"#,
        ),
        intended: Some(
            "query q() { match { $p: Person } return { $p.name } order { $p.age } limit 10 }",
        ),
    },
    Row {
        code: "Q021",
        shape: "annotation argument with colon",
        schema: COMMON,
        source: "query q() @budget(plan: 10) { match { $p: Person } return { $p.name } }",
        message: "",
        fix: Some("@budget(plan = 10)"),
        intended: Some("query q() @budget(plan = 10) { match { $p: Person } return { $p.name } }"),
    },
    Row {
        code: "Q021",
        shape: "an annotation argument with a colon",
        schema: CHECKED,
        source: r#"query q() @budget(plan: 1000) {
  match { $p: Person }
  return { $p.name }
}"#,
        message: "",
        fix: Some("@budget(plan = 1000)"),
        intended: Some(
            "query q() @budget(plan = 1000) { match { $p: Person } return { $p.name } }",
        ),
    },
    Row {
        code: "T1",
        shape: "unknown node type",
        schema: COMMON,
        source: "query q() { match { $p: Persn } return { $p } }",
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "T3",
        shape: "an integer literal outside its property's type",
        schema: COMMON,
        source: "query q() { match { $p: Person } where $p.age = 99999999999 return { $p.name } }",
        message: "",
        fix: Some("2147483647"),
        intended: Some(
            "query q() { match { $p: Person } where $p.age = 2147483647 return { $p.name } }",
        ),
    },
    Row {
        code: "T4",
        shape: "catalogue edge",
        schema: COMMON,
        source: "query q() { match { $a: Person, $a @property $b } return { $b } }",
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "T5",
        shape: "wildcard without bindings",
        schema: COMMON,
        source: "query q() { match { $a * $b } return { $a } }",
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "T6",
        shape: "fuzzy is not a registry row",
        schema: FULL_TEXT_TITLE,
        source: r#"query q() { match { $d: Doc } where fuzzy($d.title, "graf") return { $d.slug } }"#,
        message: "`fuzzy` is not a registry row; the lexical predicate is `match_terms",
        fix: Some(r#"match_terms($d.title, terms("graf", mode: any, max_edits: 2))"#),
        intended: Some(
            r#"query q() { match { $d: Doc } where match_terms($d.title, terms("graf", mode: any, max_edits: 2)) return { $d.slug } }"#,
        ),
    },
    Row {
        code: "T6",
        shape: "match text is not a registry row",
        schema: FULL_TEXT_TITLE,
        source: r#"query q() { match { $d: Doc } where match_text($d.title, "graph") return { $d.slug } }"#,
        message: "`match_text` is not a registry row; the lexical predicate is `match_terms",
        fix: Some(r#"match_terms($d.title, terms("graph", mode: any))"#),
        intended: Some(
            r#"query q() { match { $d: Doc } where match_terms($d.title, terms("graph", mode: any)) return { $d.slug } }"#,
        ),
    },
    Row {
        code: "T6",
        shape: "search is not a registry row",
        schema: FULL_TEXT_TITLE,
        source: r#"query q() { match { $d: Doc } where search($d.title, "graph") return { $d.slug } }"#,
        message: "`search` is not a registry row; the lexical predicate is `match_terms",
        fix: Some(r#"match_terms($d.title, terms("graph", mode: any))"#),
        intended: Some(
            r#"query q() { match { $d: Doc } where match_terms($d.title, terms("graph", mode: any)) return { $d.slug } }"#,
        ),
    },
    Row {
        code: "T6",
        shape: "unbound variable",
        schema: COMMON,
        source: "query q() { match { $p: Person } where $q.age > 1 return { $p.name } }",
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "T6",
        shape: "search",
        schema: CHECKED,
        source: r#"query q($s: String) {
  match { $d: Doc } where search($d.body, $s)
  return { $d.title }
}"#,
        message: "",
        fix: Some("match_terms($d.body, terms($s, mode: any))"),
        intended: Some(
            "query q($s: String) { match { $d: Doc } where match_terms($d.body, terms($s, mode: any)) return { $d.title } }",
        ),
    },
    Row {
        code: "T6",
        shape: "match_text",
        schema: CHECKED,
        source: r#"query q($s: String) {
  match { $d: Doc } where not match_text($d.body, $s)
  return { $d.title }
}"#,
        message: "",
        fix: Some("match_terms($d.body, terms($s, mode: any))"),
        intended: Some(
            "query q($s: String) { match { $d: Doc } where not match_terms($d.body, terms($s, mode: any)) return { $d.title } }",
        ),
    },
    Row {
        code: "T6",
        shape: "fuzzy without a budget",
        schema: CHECKED,
        source: r#"query q($s: String) {
  match { $d: Doc } where fuzzy($d.body, $s)
  return { $d.title }
}"#,
        message: "",
        fix: Some("match_terms($d.body, terms($s, mode: any, max_edits: 2))"),
        intended: Some(
            "query q($s: String) { match { $d: Doc } where match_terms($d.body, terms($s, mode: any, max_edits: 2)) return { $d.title } }",
        ),
    },
    Row {
        code: "T6",
        shape: "fuzzy with a budget",
        schema: CHECKED,
        source: r#"query q($s: String) {
  match { $d: Doc } where fuzzy($d.body, $s, max_edits: 1)
  return { $d.title }
}"#,
        message: "",
        fix: Some("match_terms($d.body, terms($s, mode: any, max_edits: 1))"),
        intended: Some(
            "query q($s: String) { match { $d: Doc } where match_terms($d.body, terms($s, mode: any, max_edits: 1)) return { $d.title } }",
        ),
    },
    Row {
        code: "T6",
        shape: "fuzzy with a positional budget",
        schema: CHECKED,
        source: r#"query q($s: String) {
  match { $d: Doc } where fuzzy($d.body, $s, 1)
  return { $d.title }
}"#,
        message: "",
        fix: Some("match_terms($d.body, terms($s, mode: any, max_edits: 1))"),
        intended: Some(
            "query q($s: String) { match { $d: Doc } where match_terms($d.body, terms($s, mode: any, max_edits: 1)) return { $d.title } }",
        ),
    },
    Row {
        code: "T6",
        shape: "fuzzy with a positional parameter budget",
        schema: CHECKED,
        source: r#"query q($s: String, $n: I64) {
  match { $d: Doc } where fuzzy($d.body, $s, $n)
  return { $d.title }
}"#,
        message: "",
        fix: Some("match_terms($d.body, terms($s, mode: any, max_edits: $n))"),
        intended: Some(
            "query q($s: String, $n: I64) { match { $d: Doc } where match_terms($d.body, terms($s, mode: any, max_edits: $n)) return { $d.title } }",
        ),
    },
    Row {
        code: "T6",
        shape: "fuzzy with a positional budget of zero",
        schema: CHECKED,
        source: r#"query q($s: String) {
  match { $d: Doc } where fuzzy($d.body, $s, 0)
  return { $d.title }
}"#,
        message: "",
        fix: Some("match_terms($d.body, terms($s, mode: any))"),
        intended: Some(
            "query q($s: String) { match { $d: Doc } where match_terms($d.body, terms($s, mode: any)) return { $d.title } }",
        ),
    },
    Row {
        code: "T7",
        shape: "infix contains",
        schema: COMMON,
        source: r#"query q() { match { $d: Doc } where $d.tags contains "rust" return { $d.title } }"#,
        message: "",
        fix: Some(r#""rust" in $d.tags"#),
        intended: Some(
            r#"query q() { match { $d: Doc } where "rust" in $d.tags return { $d.title } }"#,
        ),
    },
    Row {
        code: "T7",
        shape: "in over a list inside match",
        schema: CHECKED,
        source: r#"query q($names: [String]) {
  match { $p: Person, let $n = $p.name, $n in $names }
  return { $p.name }
}"#,
        message: "",
        fix: Some("where $n in $names"),
        intended: Some(
            "query q($names: [String]) { match { $p: Person, let $n = $p.name } where $n in $names return { $p.name } }",
        ),
    },
    Row {
        code: "T7",
        shape: "infix contains over a list",
        schema: CHECKED,
        source: r#"query q() {
  match { $d: Doc } where $d.tags contains "rust"
  return { $d.title }
}"#,
        message: "",
        fix: Some(r#""rust" in $d.tags"#),
        intended: Some(
            r#"query q() { match { $d: Doc } where "rust" in $d.tags return { $d.title } }"#,
        ),
    },
    Row {
        code: "T7",
        shape: "infix contains over a String",
        schema: CHECKED,
        source: r#"query q() {
  match { $d: Doc } where $d.title contains "graph"
  return { $d.title }
}"#,
        message: "",
        fix: Some(r#"contains($d.title, "graph")"#),
        intended: Some(
            r#"query q() { match { $d: Doc } where contains($d.title, "graph") return { $d.title } }"#,
        ),
    },
    Row {
        code: "T7",
        shape: "infix starts_with",
        schema: CHECKED,
        source: r#"query q() {
  match { $p: Person } where $p.name starts_with "A"
  return { $p.name }
}"#,
        message: "",
        fix: Some(r#"starts_with($p.name, "A")"#),
        intended: Some(
            r#"query q() { match { $p: Person } where starts_with($p.name, "A") return { $p.name } }"#,
        ),
    },
    Row {
        code: "T8",
        shape: "collect of a reference (3)",
        schema: COLLECTED,
        source: r#"query embeddings() {
    match { $p: Person }
    return { collect($p.embedding) as embeddings }
}"#,
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "T8",
        shape: "aggregate in a filter",
        schema: COMMON,
        source: "query q() { match { $p: Person } where count($p) > 1 return { $p.name } }",
        message: "",
        fix: Some("return { count($p) as count_p }"),
        intended: None,
    },
    Row {
        code: "T9",
        shape: "uncorrelated not",
        schema: COMMON,
        source: "query q() { match { $p: Person } where not exists { $c: Company } return { $p.name } }",
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "T10",
        shape: "insert into a catalogue type",
        schema: COMMON,
        source: r#"query w() { insert @Type { name: "x" } }"#,
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "T11",
        shape: "edge endpoint written from and to",
        schema: COMMON,
        source: r#"query befriend() { insert Knows { from: "ada", to: "ada" } }"#,
        message: "an edge's endpoints are assigned as `@src` and `@dst`; `from:` is edition 2's spelling of `@src:`",
        fix: Some("@src: <endpoint>"),
        intended: Some(r#"query befriend() { insert Knows { @src: "ada", @dst: "ada" } }"#),
    },
    Row {
        code: "T11",
        shape: "insert of an unknown property",
        schema: COMMON,
        source: r#"query w() { replace Person { handle: "a", name: "b", nickname: "c" } }"#,
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "T12",
        shape: "insert without a required property",
        schema: COMMON,
        source: r#"query w() { replace Person { handle: "a" } }"#,
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "T13",
        shape: "duplicate assignment",
        schema: COMMON,
        source: r#"query w() { replace Person { handle: "a", name: "b", name: "c" } }"#,
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "T15",
        shape: "ranking a non vector",
        schema: COMMON,
        source: "query q($v: Vector(3)) { match { let $pop = { match { $d: Doc } return { $d } }, from nearest($pop, $d.title, $v, k: 3) as $distance } return { $d.title } }",
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "T17",
        shape: "nearest without a window",
        schema: COMMON,
        source: "query q($v: Vector(3)) { match { let $pop = { match { $d: Doc } return { $d } }, from nearest($pop, $d.embedding, $v) as $distance } return { $d.title } }",
        message: "",
        fix: Some("nearest(…, k: n)"),
        intended: None,
    },
    Row {
        code: "T19",
        shape: "terms without text",
        schema: COMMON,
        source: "query q() { match { $d: Doc } where match_terms($d.body, terms()) return { $d.title } }",
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "T20",
        shape: "empty lexical query",
        schema: COMMON,
        source: r#"query q() { match { let $pop = { match { $d: Doc } return { $d } }, from bm25($pop, $d.body, "") as $score } return { $d.title } }"#,
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "T22",
        shape: "an undirected traversal over an edge between two types",
        schema: COMMON,
        source: "query q() { match { $p: Person, $c: Company, $p <worksAt> $c } return { $p.name } }",
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "T23",
        shape: "edge binding reuses an endpoint",
        schema: COMMON,
        source: "query q() { match { $a: Person, $a $a:knows $b } return { $b.name } }",
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "T24",
        shape: "blob as a read value",
        schema: COMMON,
        source: "query q() { match { $d: Doc } return { $d.payload } }",
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "T24",
        shape: "blob compared in where",
        schema: COMMON,
        source: "query q($b: String) { match { $d: Doc } where $d.payload = $b return { $d.title } }",
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "T25",
        shape: "duplicate result column",
        schema: COMMON,
        source: "query q() { match { $p: Person } return { $p.name as n, $p.handle as n } }",
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "T27",
        shape: "bm25 without a full text index",
        schema: COMMON,
        source: "query q($s: String) { match { let $pop = { match { $d: Doc } return { $d } }, from bm25($pop, $d.title, $s) as $score } return { $d.title } }",
        message: "",
        fix: Some("declare `title: String @index(fulltext)` on `Doc`"),
        intended: None,
    },
    Row {
        code: "T39",
        shape: "uncorrelated count block",
        schema: COMMON,
        source: "query q() { match { $p: Person } where count { $c: Company } > 0 return { $p.name } }",
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "T40",
        shape: "sum block without its argument",
        schema: COMMON,
        source: "query q() { match { $p: Person } where sum { $q: Person, $p knows $q } > 0 return { $p.name } }",
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "T41",
        shape: "non boolean filter",
        schema: COMMON,
        source: "query q() { match { $p: Person } where $p.name return { $p.handle } }",
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "T41",
        shape: "non boolean and operand",
        schema: COMMON,
        // A top-level `and` of `where` splits into conjuncts, each a filter;
        // under `or` it is one operand whose own operands must be Booleans.
        source: "query q() { match { $p: Person } where ($p.age > 1 and $p.name) or $p.handle = \"x\" return { $p.handle } }",
        message: "`and` needs a Bool, got String",
        fix: None,
        intended: None,
    },
    Row {
        code: "T42",
        shape: "order alias not returned",
        schema: COMMON,
        source: "query q() { match { $p: Person } return { $p.name } order { missing } }",
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "T43",
        shape: "comparison item without alias",
        schema: COMMON,
        source: "query q() { match { $p: Person } return { $p.age > 3 } }",
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "T44",
        shape: "block as an assignment value",
        schema: COMMON,
        source: "query w() { match { $p: Person }  update $p set { age: count { $q: Person, $p knows $q } } }",
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "T47",
        shape: "a parameter named as the timestamp the engine injects",
        schema: COMMON,
        source: "query q($__nanograph_now: String) { match { $p: Person } where $p.name = $__nanograph_now return { $p.name } }",
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "T48",
        shape: "unknown parameter type",
        schema: COMMON,
        source: "query q($x: Vector(0)) { match { $p: Person } return { $p.name } }",
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "T49",
        shape: "a variable beginning with `__`",
        schema: COMMON,
        source: "query q() { match { $__p: Person } return { $__p.name } }",
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "T50",
        shape: "rebinding to another type",
        schema: COMMON,
        source: "query q() { match { $p: Person, $p: Company } return { $p } }",
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "T52",
        shape: "a nested list literal",
        schema: COMMON,
        source: r#"query q() { match { $p: Person } where $p.name in [["ada"]] return { $p.name } }"#,
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "T53",
        shape: "a list literal of two scalar types",
        schema: COMMON,
        source: r#"query q() { match { $p: Person } where $p.name in ["ada", 1] return { $p.name } }"#,
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "T55",
        shape: "an unattached base parameter that is not a String",
        schema: COMMON,
        source: r#"query w($base: I64) as unattached branch on $base { insert Person { handle: "eve", name: "Eve" } }"#,
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "T56",
        shape: "distinct over aggregates",
        schema: COMMON,
        source: "query q() { match { $p: Person } return distinct { count($p) as n } }",
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "T57",
        shape: "distinct of sum",
        schema: COMMON,
        source: "query q() { match { $p: Person } return { sum(distinct $p.age) as s } }",
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "T65",
        shape: "ranking in order",
        schema: COMMON,
        source: "query q($v: Vector(3)) { match { $d: Doc } return { $d.title } order { nearest($d.embedding, $v) } limit 3 }",
        message: "",
        fix: Some(
            "match { let $pop = { match { $d: Doc } return { $d } }, from nearest($pop, $d.embedding, $v, k: 3) as $distance }  order { $distance }",
        ),
        intended: Some(
            "query q($v: Vector(3)) { match { let $pop = { match { $d: Doc } return { $d } }, from nearest($pop, $d.embedding, $v, k: 3) as $distance } return { $d.title } order { $distance } limit 3 }",
        ),
    },
    Row {
        code: "T68",
        shape: "block binds an outer name",
        schema: OPTIONAL_SCOPES,
        source: r#"query not_bob() { match { $p: Person } where not exists { $p: Person where $p.name = "Bob" } return { $p.name } }"#,
        message: "`$p` is bound inside a block, but the scope where the block is written already holds `$p` (node Person)",
        fix: Some(r#"write `($p.name = "Bob")` in the block's place"#),
        intended: Some(
            r#"query not_bob() { match { $p: Person } where not ($p.name = "Bob") return { $p.name } }"#,
        ),
    },
    Row {
        code: "T68",
        shape: "block binds an outer name (2)",
        schema: OPTIONAL_SCOPES,
        source: "query knows_someone() { match { $p: Person } where exists { $p: Person, $q: Person, $p knows $q } return { $p.name } }",
        message: "`$p` is bound inside a block, but the scope where the block is written already holds `$p` (node Person)",
        fix: Some("read the outer `$p` without binding it: drop `$p: Person`"),
        intended: Some(
            "query knows_someone() { match { $p: Person } where exists { $q: Person, $p knows $q } return { $p.name } }",
        ),
    },
    Row {
        code: "T68",
        shape: "block binds an outer name (3)",
        schema: OPTIONAL_SCOPES,
        source: "query employers() { match { $p: Person, optional { $p: Person, $c: Company, $p worksAt $c } } return { $p.name, $c.name } }",
        message: "`$p` is bound inside an `optional`, but the scope where the `optional` is written already holds `$p` (node Person)",
        fix: Some("read the outer `$p` without binding it: drop `$p: Person`"),
        intended: Some(
            "query employers() { match { $p: Person, optional { $c: Company, $p worksAt $c } } return { $p.name, $c.name } }",
        ),
    },
    Row {
        code: "T68",
        shape: "from column collision",
        schema: COMMON,
        source: "query q() { match { $c: Company, from { match { $x: Company } return { $x as c } } } return { $c.name } }",
        message: "",
        fix: Some("alias it on the inner `return`: `$c as c2`"),
        intended: None,
    },
    Row {
        code: "T69",
        shape: "relation where a scalar is read",
        schema: COMMON,
        source: "query q() { match { let $r = { match { $p: Person } return { $p } } } where $r = 1 return { 1 as one } }",
        message: "",
        fix: Some("draw it with `from $r`, or aggregate over it: `count { from $r }`"),
        intended: None,
    },
    Row {
        code: "T70",
        shape: "recursion without a base arm",
        schema: COMMON,
        source: "query loop($h: String) { match { from loop($h) } return distinct { $q } }",
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "T71",
        shape: "positional option",
        schema: COMMON,
        source: "query q($v: Vector(3)) { match { let $pop = { match { $d: Doc } return { $d } }, from nearest($pop, $d.embedding, $v, 3) as $distance } return { $d.title } }",
        message: "",
        fix: Some("nearest($pop, $d.embedding, $v, k: 3)"),
        intended: Some(
            "query q($v: Vector(3)) { match { let $pop = { match { $d: Doc } return { $d } }, from nearest($pop, $d.embedding, $v, k: 3) as $distance } return { $d.title } }",
        ),
    },
    Row {
        code: "T72",
        shape: "arithmetic over a string",
        schema: COMMON,
        source: "query q() { match { $p: Person } return { $p.name + 1 as n } }",
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "T73",
        shape: "union arms disagree",
        schema: COMMON,
        source: "query q() { match { from union({ match { $p: Person } return { $p.name as v } }, { match { $p: Person } return { $p.age as v } }) } return { $v } }",
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "T74",
        shape: "column not variable shaped",
        schema: COMMON,
        source: "query q() { match { from { match { $p: Person } return { $p.name } } } return { 1 as one } }",
        message: "",
        fix: Some("alias it on the inner `return`: `$p.name as p_name`"),
        intended: None,
    },
    Row {
        code: "T75",
        shape: "rrf with one arm",
        schema: COMMON,
        source: "query q($v: Vector(3)) { match { let $pop = { match { $d: Doc } return { $d } }, from rrf(nearest($pop, $d.embedding, $v, k: 5)) as $score } return { $d.title } }",
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "T76",
        shape: "write mixing kinds",
        schema: COMMON,
        source: r#"query w($h: String) { match { $p: Person, $p $k:worksAt $c } where $p.handle = $h  delete $k  update $p set { city: "x" } }"#,
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "T77",
        shape: "statement over a scalar",
        schema: COMMON,
        source: "query w($h: String) { match { $p: Person, let $n = $p.name } where $p.handle = $h  delete $n }",
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "T78",
        shape: "endpoint of the wrong type",
        schema: COMMON,
        source: "query w() { match { $c: Company }  insert WorksAt { @src: $c, @dst: $c } }",
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "T79",
        shape: "ranked column not a node",
        schema: COMMON,
        source: "query q($v: Vector(3)) { match { let $pop = { match { $d: Doc } return { $d.embedding as e } }, from nearest($pop, $e.x, $v, k: 3) as $distance } return { $e } }",
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "T80",
        shape: "unknown annotation",
        schema: COMMON,
        source: "query q() @cache(ttl = 5) { match { $p: Person } return { $p.name } }",
        message: "",
        fix: None,
        intended: Some("query q() { match { $p: Person } return { $p.name } }"),
    },
    Row {
        code: "T80",
        shape: "an unknown annotation",
        schema: CHECKED,
        source: r#"query q() @owner("search-team") {
  match { $p: Person }
  return { $p.name }
}"#,
        message: "",
        fix: None,
        intended: Some("query q() { match { $p: Person } return { $p.name } }"),
    },
    Row {
        code: "T81",
        shape: "pattern components share no variable",
        schema: PRODUCTS,
        source: "query staff() { match { $c: Company, $p: Person } return { $p.name, $c.name } }",
        message: "`$p: Person` shares no variable with `$c: Company`, so the pattern multiplies their rows by a product; a product is written only when every component but one is key-pinned, one node binding whose every `@key` property, or whose `@id`, `where` equates with a literal or a parameter",
        fix: Some(
            "join them through the edge, `$p worksAt $c`, or write the intended product as a named relation, `let $ps = { match { $p: Person } return { $p } }, from $ps`",
        ),
        intended: Some(
            "query staff() { match { $c: Company, let $ps = { match { $p: Person } return { $p } }, from $ps } return { $p.name, $c.name } }",
        ),
    },
    Row {
        code: "T81",
        shape: "pattern components share no variable (2)",
        schema: PRODUCTS,
        source: r#"query shelved() { match { $p: Person, $s: Collection } where $p.name = "Ada" return { $p.name, $s.slug } }"#,
        message: "`$s: Collection` shares no variable with `$p: Person`, so the pattern multiplies their rows by a product; a product is written only when every component but one is key-pinned, one node binding whose every `@key` property, or whose `@id`, `where` equates with a literal or a parameter",
        fix: Some(
            "write the intended product as a named relation, `let $ss = { match { $s: Collection } return { $s } }, from $ss`, or join the components through a triple",
        ),
        intended: Some(
            r#"query shelved() { match { $p: Person, let $ss = { match { $s: Collection } return { $s } }, from $ss } where $p.name = "Ada" return { $p.name, $s.slug } }"#,
        ),
    },
    Row {
        code: "T81",
        shape: "pattern components share no variable (3)",
        schema: PRODUCTS,
        source: r#"query employer_beside() { match { $p: Person, $p worksAt $w, $s: Collection } where $p.handle = "ada" return { $w.name, $s.slug } }"#,
        message: "`$s: Collection` shares no variable with `$p: Person, $p worksAt $w`, so the pattern multiplies their rows by a product; a product is written only when every component but one is key-pinned, one node binding whose every `@key` property, or whose `@id`, `where` equates with a literal or a parameter",
        fix: Some(
            "write the intended product as a named relation, `let $ss = { match { $s: Collection } return { $s } }, from $ss`, or join the components through a triple",
        ),
        intended: Some(
            r#"query employer_beside() { match { $p: Person, $p worksAt $w, let $ss = { match { $s: Collection } return { $s } }, from $ss } where $p.handle = "ada" return { $w.name, $s.slug } }"#,
        ),
    },
    Row {
        code: "T82",
        shape: "triple written against its edge direction",
        schema: DIRECTIONS,
        source: "query staff($slug: String) { match { $c: Company, $c worksAt $p } where $c.slug = $slug return { $p.name } }",
        message: "`$c worksAt $p` is written against the declared direction of `WorksAt: Person -> Company`",
        fix: Some("write the source first: `$p worksAt $c`"),
        intended: Some(
            "query staff($slug: String) { match { $c: Company, $p worksAt $c } where $c.slug = $slug return { $p.name } }",
        ),
    },
    Row {
        code: "T82",
        shape: "triple written against its edge direction (2)",
        schema: DIRECTIONS,
        source: "query ties() { match { $c: Company } where exists { $p: Person, $c $k:(worksAt | founded) $p } return { $c.name } }",
        message: "no member of `$c $k:(worksAt | founded) $p` points along its written order, which is against the declared direction of `Founded: Person -> Company`, `WorksAt: Person -> Company`",
        fix: Some("write the source first: `$p $k:(worksAt | founded) $c`"),
        intended: Some(
            "query ties() { match { $c: Company } where exists { $p: Person, $p $k:(worksAt | founded) $c } return { $c.name } }",
        ),
    },
    Row {
        code: "T82",
        shape: "triple written against its edge direction (3)",
        schema: DIRECTIONS,
        source: "query everyone() { match { $c: Company, $p: Person, $c * $p } return { $p.name } }",
        message: "no member of `$c * $p` points along its written order, which is against the declared direction of `Founded: Person -> Company`, `WorksAt: Person -> Company`",
        fix: Some("write the source first: `$p * $c`"),
        intended: Some(
            "query everyone() { match { $c: Company, $p: Person, $p * $c } return { $p.name } }",
        ),
    },
    Row {
        code: "T83",
        shape: "duplicate declaration name",
        schema: ONE_PERSON,
        source: r#"query people() { match { $p: Person } return { $p.name } }
query people() { match { $p: Person } return { $p.handle } }"#,
        message: "`people` is declared twice in one source, at line 1, column 7 and at line 2, column 7",
        fix: Some(
            "rename the later `people` (line 2, column 7) to `people_2`, or delete that declaration",
        ),
        intended: Some(
            r#"query people() { match { $p: Person } return { $p.name } }
query people_2() { match { $p: Person } return { $p.handle } }"#,
        ),
    },
    Row {
        code: "T84",
        shape: "triple names a name no binding introduces",
        schema: OPTIONAL_SCOPES,
        source: "query loners() { match { $p: Person } where not exists { $p knows $q } return { $p.name } }",
        message: "`$q` is named in a triple of this block but bound nowhere",
        fix: Some("bind it in the block: `$q: Person`"),
        intended: Some(
            "query loners() { match { $p: Person } where not exists { $q: Person, $p knows $q } return { $p.name } }",
        ),
    },
    Row {
        code: "T84",
        shape: "triple names a name no binding introduces (2)",
        schema: OPTIONAL_SCOPES,
        source: "query headcounts() { match { from { match { $p worksAt $c } return { $c, count($p) as n } } } return { $c.name, $n } }",
        message: "`$p`, `$c` are named in a triple of this inline read but bound nowhere",
        fix: Some("bind them in the inline read: `$p: Person`, `$c: Company`"),
        intended: Some(
            "query headcounts() { match { from { match { $p: Person, $c: Company, $p worksAt $c } return { $c, count($p) as n } } } return { $c.name, $n } }",
        ),
    },
    Row {
        code: "T84",
        shape: "triple names a name no binding introduces (3)",
        schema: OPTIONAL_SCOPES,
        source: "query employers() { match { $p: Person, optional { $p worksAt $c } } return { $p.name, $c.name } }",
        message: "`$c` is named in a triple of this `optional` but bound nowhere",
        fix: Some("bind it in the `optional`: `$c: Company`"),
        intended: Some(
            "query employers() { match { $p: Person, optional { $c: Company, $p worksAt $c } } return { $p.name, $c.name } }",
        ),
    },
    Row {
        code: "T85",
        shape: "ranked from names its score",
        schema: RANKED_DOCS,
        source: "query near($q: Vector(3)) { match { let $pop = { match { $d: Doc } return { $d } }, from nearest($pop, $d.embedding, $q, k: 2) } return { $d.slug, $distance } order { $distance } }",
        message: "`from nearest(…)` ranks without naming its score",
        fix: Some("from nearest(…) as $distance"),
        intended: Some(
            "query near($q: Vector(3)) { match { let $pop = { match { $d: Doc } return { $d } }, from nearest($pop, $d.embedding, $q, k: 2) as $distance } return { $d.slug, $distance } order { $distance } }",
        ),
    },
    Row {
        code: "T85",
        shape: "ranked from names its score (2)",
        schema: RANKED_DOCS,
        source: "query docs() { match { let $docs = { match { $d: Doc } return { $d } }, from $docs as $s } return { $d.slug } }",
        message: "`from $docs as $s`: `as` names an unnamed score, and this `from`'s relation carries none",
        fix: Some("from $docs"),
        intended: Some(
            "query docs() { match { let $docs = { match { $d: Doc } return { $d } }, from $docs } return { $d.slug } }",
        ),
    },
    Row {
        code: "T85",
        shape: "ranked from names its score (3)",
        schema: RANKED_DOCS,
        source: "query near_by_name($q: Vector(3)) { match { let $pop = { match { $d: Doc } return { $d } }, let $near = nearest($pop, $d.embedding, $q, k: 2), from $near } return { $d.slug } }",
        message: "`from $near` ranks without naming its score",
        fix: Some("from $near as $distance"),
        intended: Some(
            "query near_by_name($q: Vector(3)) { match { let $pop = { match { $d: Doc } return { $d } }, let $near = nearest($pop, $d.embedding, $q, k: 2), from $near as $distance } return { $d.slug } }",
        ),
    },
    Row {
        code: "T85",
        shape: "ranked from names its score (4)",
        schema: RANKED_DOCS,
        source: "query wrapped($q: Vector(3)) { match { from { match { let $pop = { match { $d: Doc } return { $d } }, from nearest($pop, $d.embedding, $q, k: 2) as $dist } return { $d, $dist } } as $s } return { $d.slug } }",
        message: "`from { … } as $s`: `as` names an unnamed score, and this `from`'s relation carries none",
        fix: Some("from { … }"),
        intended: Some(
            "query wrapped($q: Vector(3)) { match { from { match { let $pop = { match { $d: Doc } return { $d } }, from nearest($pop, $d.embedding, $q, k: 2) as $dist } return { $d, $dist } } } return { $d.slug } }",
        ),
    },
    Row {
        code: "T85",
        shape: "a ranked from without as",
        schema: CHECKED,
        source: "query near($q: Vector(3)) { match { let $pop = { match { $d: Doc } return { $d } }, from nearest($pop, $d.embedding, $q, k: 2) } return { $d.id, $distance } order { $distance } }",
        message: "",
        fix: Some("from nearest(…) as $distance"),
        intended: Some(
            "query near($q: Vector(3)) { match { let $pop = { match { $d: Doc } return { $d } }, from nearest($pop, $d.embedding, $q, k: 2) as $distance } return { $d.id, $distance } order { $distance } }",
        ),
    },
    Row {
        code: "T85",
        shape: "an as on a from whose relation carries no unnamed score",
        schema: CHECKED,
        source: "query docs() { match { let $docs = { match { $d: Doc } return { $d } }, from $docs as $s } return { $d.id } }",
        message: "",
        fix: Some("from $docs"),
        intended: Some(
            "query docs() { match { let $docs = { match { $d: Doc } return { $d } }, from $docs } return { $d.id } }",
        ),
    },
    Row {
        code: "T85",
        shape: "a let bound to a ranking drawn without as",
        schema: CHECKED,
        source: "query near_by_name($q: Vector(3)) { match { let $pop = { match { $d: Doc } return { $d } }, let $near = nearest($pop, $d.embedding, $q, k: 2), from $near } return { $d.id } }",
        message: "",
        fix: Some("from $near as $distance"),
        intended: Some(
            "query near_by_name($q: Vector(3)) { match { let $pop = { match { $d: Doc } return { $d } }, let $near = nearest($pop, $d.embedding, $q, k: 2), from $near as $distance } return { $d.id } }",
        ),
    },
    Row {
        code: "T85",
        shape: "an as on an inline read returning a named score",
        schema: CHECKED,
        source: "query wrapped($q: Vector(3)) { match { from { match { let $pop = { match { $d: Doc } return { $d } }, from nearest($pop, $d.embedding, $q, k: 2) as $dist } return { $d, $dist } } as $s } return { $d.id } }",
        message: "",
        fix: Some("from { … }"),
        intended: Some(
            "query wrapped($q: Vector(3)) { match { from { match { let $pop = { match { $d: Doc } return { $d } }, from nearest($pop, $d.embedding, $q, k: 2) as $dist } return { $d, $dist } } } return { $d.id } }",
        ),
    },
    Row {
        code: "T86",
        shape: "collect of a reference",
        schema: COLLECTED,
        source: r#"query friends() {
    match { $p: Person, $p knows $f }
    return { $p.handle as handle, collect($f) as friends }
}"#,
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "T86",
        shape: "collect of a reference (2)",
        schema: COLLECTED,
        source: r#"query friend_edges() {
    match { $p: Person, $p $k:knows $f }
    return { $p.handle as handle, collect($k) as edges }
}"#,
        message: "",
        fix: None,
        intended: None,
    },
    Row {
        code: "T86",
        shape: "unlowered read does not run",
        schema: COMMON,
        source: r#"query reachable($from: String) {
    match {
        from union(
            { match { $p: Person } where $p.handle = $from return { $p as q } },
            { match { from reachable($from), $r: Person, $q knows $r } return { $r as q } }),
    }
    return distinct { $q }
}"#,
        message: "`reachable`: this release does not support a read that calls itself (`Fix`) yet",
        fix: None,
        intended: None,
    },
    Row {
        code: "T87",
        shape: "bm25 over an unranked field",
        schema: UNRANKED_BODY,
        source: "query ranked($q: String) { match { let $pop = { match { $d: Doc } return { $d } }, from bm25($pop, $d.body, $q) as $score } return { $d.slug, $score } order { $score desc } limit 2 }",
        message: "",
        fix: Some(
            r#"match without a score, `match_terms($d.body, terms(…))` in `where`, or declare `scorer = "bm25_v1"` on the index"#,
        ),
        intended: None,
    },
    Row {
        code: "T88",
        shape: "defaulted parameter before a required one",
        schema: COMMON,
        source: "query q($min: I32 = 30, $city: String) { match { $p: Person } where $p.age >= $min and $p.city = $city return { $p.name } }",
        message: "`q`'s parameter `$min` has a default and `$city` after it has none",
        fix: Some("($city: String, $min: I32 = 30)"),
        intended: None,
    },
    Row {
        code: "T90",
        shape: "triple reads a reference an optional introduced",
        schema: OPTIONAL_SCOPES,
        source: "query friends_employers() { match { $p: Person, optional { $f: Person, $p knows $f }, $f worksAt $c } return { $p.name, $c.name } }",
        message: "`$f worksAt $c` reads `$f` outside the `optional` that introduces it",
        fix: Some(
            "move `$f worksAt $c` inside the `optional`, or drop the `optional` if only the rows it matches were meant",
        ),
        intended: Some(
            "query friends_employers() { match { $p: Person, optional { $f: Person, $p knows $f, $c: Company, $f worksAt $c } } return { $p.name, $c.name } }",
        ),
    },
    Row {
        code: "T90",
        shape: "triple reads a reference an optional introduced (2)",
        schema: OPTIONAL_SCOPES,
        source: "query admirers_of_staff() { match { $c: Company, optional { $p: Person, $p worksAt $c }, $q knows $p } return { $c.name, $q.name } }",
        message: "`$q knows $p` reads `$p` outside the `optional` that introduces it",
        fix: Some(
            "move `$q knows $p` inside the `optional`, or drop the `optional` if only the rows it matches were meant",
        ),
        intended: Some(
            "query admirers_of_staff() { match { $c: Company, optional { $p: Person, $p worksAt $c, $q: Person, $q knows $p } } return { $c.name, $q.name } }",
        ),
    },
    Row {
        code: "T90",
        shape: "triple reads a reference an optional introduced (3)",
        schema: OPTIONAL_SCOPES,
        source: "query colleagues_of_friends() { match { $p: Person, optional { $f: Person, $p knows $f }, $q: Person, $f knows $q } return { $p.name, $q.name } }",
        message: "`$f knows $q` reads `$f` outside the `optional` that introduces it",
        fix: Some(
            "move `$f knows $q` inside the `optional`, or drop the `optional` if only the rows it matches were meant",
        ),
        intended: None,
    },
    Row {
        code: "T90",
        shape: "triple reads a reference an optional introduced (4)",
        schema: OPTIONAL_SCOPES,
        source: "query friends_employers_drawn() { match { $p: Person, from { match { $f: Person, $p knows $f, optional { $g: Person, $f knows $g } } return { $g.handle as h, $g } }, $g worksAt $c } return { $p.name, $c.name } }",
        message: "`$g worksAt $c` reads `$g` outside the `optional` that introduces it",
        fix: Some(
            "move `$g worksAt $c` inside the `optional`, or drop the `optional` if only the rows it matches were meant",
        ),
        intended: None,
    },
    Row {
        code: "T91",
        shape: "a write group selecting on a type an earlier group changes",
        schema: COMMON,
        source: r#"query w() {
  match { $p: Person } where $p.name = "Ada"
  update $p set { city: "x" }
  match { $q: Person, $q worksAt $c }
  update $c set { name: $q.name }
}"#,
        message: "",
        fix: Some(
            "run the group in a second write after this one publishes, or select every row the statements need in one group and write its statements after that one `match`",
        ),
        intended: None,
    },
    Row {
        code: "T92",
        shape: "upsert or replace without a key",
        schema: COMMON,
        source: r#"query befriend() { replace Knows { @src: "ada", @dst: "ada" } }"#,
        message: "`replace Knows`: `Knows` declares no `@key`",
        fix: Some("insert Knows { … }"),
        intended: Some(r#"query befriend() { insert Knows { @src: "ada", @dst: "ada" } }"#),
    },
    Row {
        code: "T92",
        shape: "upsert or replace without a key (2)",
        schema: COMMON,
        source: r#"query befriend_again() { upsert Knows { @src: "ada", @dst: "ada" } }"#,
        message: "`upsert Knows`: `Knows` declares no `@key`",
        fix: Some("insert Knows { … }"),
        intended: Some(r#"query befriend_again() { insert Knows { @src: "ada", @dst: "ada" } }"#),
    },
    Row {
        code: "T93",
        shape: "update of a key or an endpoint",
        schema: COMMON,
        source: r#"query rename() { match { $p: Person } where $p.handle = "ada"  update $p set { handle: "lovelace" } }"#,
        message: "`update $p` assigns `handle`, a key property of `Person`",
        fix: Some(
            "insert an entity with the new key and delete the old one, or leave `handle` out of the `set`",
        ),
        intended: None,
    },
    Row {
        code: "T93",
        shape: "update of a key or an endpoint (2)",
        schema: COMMON,
        source: r#"query move_edge() { match { $a: Person, $a $k:knows $b }  update $k set { @dst: "ada" } }"#,
        message: "`update $k` assigns `@dst`, an endpoint of `Knows`",
        fix: Some(
            "delete the edge and insert it with its new endpoints, or leave `@dst` out of the `set`",
        ),
        intended: None,
    },
];
