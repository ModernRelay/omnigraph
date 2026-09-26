use super::*;

#[test]
fn test_parse_basic_query() {
    let input = r#"
query get_person($name: String) {
match {
    $p: Person { name: $name }
}
return { $p.name, $p.age }
}
"#;
    let qf = parse_query(input).unwrap();
    let q = qf.single_decl();
    assert_eq!(q.name, "get_person");
    assert_eq!(q.params.len(), 1);
    assert_eq!(q.params[0].name, "name");
    assert_eq!(q.match_clause.len(), 1);
    assert_eq!(q.return_clause.len(), 2);
}

#[test]
fn test_parse_query_metadata_annotations() {
    let input = r#"
query semantic_search($q: String)
@description("Find semantically similar documents.")
@instruction("Use for conceptual search; prefer keyword_search for exact terms.")
{
match {
    $d: Doc
}
return { $d.slug }
}
"#;
    let qf = parse_query(input).unwrap();
    let q = qf.single_decl();
    assert_eq!(
        q.description.as_deref(),
        Some("Find semantically similar documents.")
    );
    assert_eq!(
        q.instruction.as_deref(),
        Some("Use for conceptual search; prefer keyword_search for exact terms.")
    );
}

#[test]
fn test_duplicate_query_description_is_rejected() {
    let input = r#"
query q()
@description("one")
@description("two")
{
match {
    $p: Person
}
return { $p.name }
}
"#;
    let err = parse_query(input).unwrap_err();
    assert!(err.to_string().contains("duplicate @description"));
}

#[test]
fn test_parse_no_params() {
    let input = r#"
query adults() {
match {
    $p: Person
    $p.age > 30
}
return { $p.name, $p.age }
order { $p.age desc }
}
"#;
    let qf = parse_query(input).unwrap();
    let q = qf.single_decl();
    assert_eq!(q.name, "adults");
    assert!(q.params.is_empty());
    assert_eq!(q.match_clause.len(), 2);
    assert_eq!(q.order_clause.len(), 1);
    assert!(q.order_clause[0].descending);
}

#[test]
fn test_parse_undirected_traversal() {
    // `$a <edge> $b`, bare + bounded + inside not{}.
    let input = r#"
query related($name: String) {
match {
    $p: Person { name: $name }
    $p <knows> $f
    $p <knows>{1,3} $g
    not { $f <knows> $g }
}
return { $f.name }
}
"#;
    let qf = parse_query(input).unwrap();
    let q = qf.single_decl();
    match &q.match_clause[1] {
        Clause::Traversal(t) => {
            assert_eq!(t.edge_name, "knows");
            assert!(t.undirected, "bare undirected form");
            assert_eq!((t.min_hops, t.max_hops), (1, Some(1)));
        }
        c => panic!("expected Traversal, got {c:?}"),
    }
    match &q.match_clause[2] {
        Clause::Traversal(t) => {
            assert!(t.undirected, "bounded undirected form");
            assert_eq!((t.min_hops, t.max_hops), (1, Some(3)));
        }
        c => panic!("expected Traversal, got {c:?}"),
    }
    match &q.match_clause[3] {
        Clause::Subquery(block) => match &block.clauses[0] {
            Clause::Traversal(t) => assert!(t.undirected, "undirected inside not{{}}"),
            c => panic!("expected Traversal in not, got {c:?}"),
        },
        c => panic!("expected a not block, got {c:?}"),
    }
}

#[test]
fn test_parse_traversal() {
    let input = r#"
query friends_of($name: String) {
match {
    $p: Person { name: $name }
    $p knows $f
}
return { $f.name, $f.age }
}
"#;
    let qf = parse_query(input).unwrap();
    let q = qf.single_decl();
    assert_eq!(q.match_clause.len(), 2);
    match &q.match_clause[1] {
        Clause::Traversal(t) => {
            assert_eq!(t.src, "p");
            assert_eq!(t.edge_name, "knows");
            assert_eq!(t.dst, "f");
            assert_eq!(t.min_hops, 1);
            assert_eq!(t.max_hops, Some(1));
        }
        _ => panic!("expected Traversal"),
    }
}

#[test]
fn test_parse_negation() {
    let input = r#"
query unemployed() {
match {
    $p: Person
    not { $p worksAt $_ }
}
return { $p.name }
}
"#;
    let qf = parse_query(input).unwrap();
    let q = qf.single_decl();
    assert_eq!(q.match_clause.len(), 2);
    match &q.match_clause[1] {
        Clause::Subquery(block) => {
            assert_eq!(block.keyword, BlockKeyword::Not);
            assert_eq!((block.func, block.op), (AggFunc::Count, CompOp::Eq));
            assert_eq!(block.clauses.len(), 1);
            match &block.clauses[0] {
                Clause::Traversal(t) => {
                    assert_eq!(t.src, "p");
                    assert_eq!(t.edge_name, "worksAt");
                    assert_eq!(t.dst, "_");
                    assert_eq!(t.min_hops, 1);
                    assert_eq!(t.max_hops, Some(1));
                }
                _ => panic!("expected Traversal inside negation"),
            }
        }
        _ => panic!("expected a not block"),
    }
}

#[test]
fn test_parse_aggregation() {
    let input = r#"
query friend_counts() {
match {
    $p: Person
    $p knows $f
}
return {
    $p.name
    count($f) as friends
}
order { friends desc }
limit 20
}
"#;
    let qf = parse_query(input).unwrap();
    let q = qf.single_decl();
    assert_eq!(q.return_clause.len(), 2);
    match &q.return_clause[1].expr {
        Expr::Aggregate { func, .. } => {
            assert_eq!(*func, AggFunc::Count);
        }
        _ => panic!("expected Aggregate"),
    }
    assert_eq!(q.return_clause[1].alias.as_deref(), Some("friends"));
    assert_eq!(q.limit, Some(20));
}

#[test]
fn test_parse_two_hop() {
    let input = r#"
query friends_of_friends($name: String) {
match {
    $p: Person { name: $name }
    $p knows $mid
    $mid knows $fof
}
return { $fof.name }
}
"#;
    let qf = parse_query(input).unwrap();
    let q = qf.single_decl();
    assert_eq!(q.match_clause.len(), 3);
}

#[test]
fn test_parse_reverse_traversal() {
    let input = r#"
query employees_of($company: String) {
match {
    $c: Company { name: $company }
    $p worksAt $c
}
return { $p.name }
}
"#;
    let qf = parse_query(input).unwrap();
    let q = qf.single_decl();
    assert_eq!(q.match_clause.len(), 2);
    match &q.match_clause[1] {
        Clause::Traversal(t) => {
            assert_eq!(t.src, "p");
            assert_eq!(t.edge_name, "worksAt");
            assert_eq!(t.dst, "c");
            assert_eq!(t.min_hops, 1);
            assert_eq!(t.max_hops, Some(1));
        }
        _ => panic!("expected Traversal"),
    }
}

#[test]
fn test_parse_bounded_traversal() {
    let input = r#"
query q() {
match {
    $a: Person
    $a knows{1,3} $b
}
return { $b.name }
}
"#;
    let qf = parse_query(input).unwrap();
    let q = qf.single_decl();
    match &q.match_clause[1] {
        Clause::Traversal(t) => {
            assert_eq!(t.min_hops, 1);
            assert_eq!(t.max_hops, Some(3));
        }
        _ => panic!("expected Traversal"),
    }
}

#[test]
fn test_parse_unbounded_traversal() {
    let input = r#"
query q() {
match {
    $a: Person
    $a knows{1,} $b
}
return { $b.name }
}
"#;
    let qf = parse_query(input).unwrap();
    let q = qf.single_decl();
    match &q.match_clause[1] {
        Clause::Traversal(t) => {
            assert_eq!(t.min_hops, 1);
            assert_eq!(t.max_hops, None);
        }
        _ => panic!("expected Traversal"),
    }
}

#[test]
fn test_parse_multi_query_file() {
    let input = r#"
query q1() {
match { $p: Person }
return { $p.name }
}
query q2() {
match { $c: Company }
return { $c.name }
}
"#;
    let FileBody::Queries(queries) = parse_query(input).unwrap().body else {
        panic!("expected query declarations");
    };
    assert_eq!(queries.len(), 2);
}

#[test]
fn test_parse_complex_negation() {
    let input = r#"
query knows_alice_not_bob() {
match {
    $a: Person { name: "Alice" }
    $b: Person { name: "Bob" }
    $p: Person
    $p knows $a
    not { $p knows $b }
}
return { $p.name }
}
"#;
    let qf = parse_query(input).unwrap();
    let q = qf.single_decl();
    assert_eq!(q.match_clause.len(), 5);
}

#[test]
fn test_parse_filter_string() {
    let input = r#"
query test() {
match {
    $p: Person
    $p.name != "Bob"
}
return { $p.name }
}
"#;
    let qf = parse_query(input).unwrap();
    let q = qf.single_decl();
    match &q.match_clause[1] {
        Clause::Filter(f) => {
            assert_eq!(comparison(f).1, CompOp::Ne);
        }
        _ => panic!("expected Filter"),
    }
}

/// The parts of a comparison-rooted filter clause.
fn comparison(filter: &Expr) -> (&Expr, CompOp, &Expr) {
    filter
        .comparison_parts()
        .unwrap_or_else(|| panic!("expected a comparison, got {filter:?}"))
}

#[test]
fn test_parse_filter_string_decodes_escapes() {
    let input = r#"
query test() {
match {
    $p: Person
    $p.name = "Bob\n\"Builder\"\t\\"
}
return { $p.name }
}
"#;
    let qf = parse_query(input).unwrap();
    let q = qf.single_decl();
    match &q.match_clause[1] {
        Clause::Filter(f) => match comparison(f).2 {
            Expr::Literal(Literal::String(value)) => {
                assert_eq!(value, "Bob\n\"Builder\"\t\\");
            }
            other => panic!("expected string literal, got {:?}", other),
        },
        _ => panic!("expected Filter"),
    }
}

#[test]
fn test_parse_string_literal_rejects_unknown_escape() {
    let input = r#"
query test() {
match {
    $p: Person
    $p.name = "Bob\q"
}
return { $p.name }
}
"#;
    let err = parse_query(input).unwrap_err();
    assert!(err.to_string().contains("unsupported escape sequence"));
}

#[test]
fn test_parse_bool_literals() {
    let input = r#"
query flags() {
match {
    $p: Person
    $p.active = true
    $p.active != false
}
return { $p.name }
}
"#;
    let qf = parse_query(input).unwrap();
    let q = qf.single_decl();
    match &q.match_clause[1] {
        Clause::Filter(f) => match comparison(f).2 {
            Expr::Literal(Literal::Bool(value)) => assert!(*value),
            other => panic!("expected bool literal, got {:?}", other),
        },
        _ => panic!("expected Filter"),
    }
    match &q.match_clause[2] {
        Clause::Filter(f) => match comparison(f).2 {
            Expr::Literal(Literal::Bool(value)) => assert!(!*value),
            other => panic!("expected bool literal, got {:?}", other),
        },
        _ => panic!("expected Filter"),
    }
}

#[test]
fn test_parse_contains_filter() {
    let input = r#"
query tagged($tag: String) {
match {
    $p: Person
    $p.tags contains $tag
}
return { $p.name }
}
"#;
    let qf = parse_query(input).unwrap();
    let q = qf.single_decl();
    match &q.match_clause[1] {
        Clause::Filter(f) => {
            let (left, op, right) = comparison(f);
            assert_eq!(op, CompOp::Contains);
            assert!(matches!(
                left,
                Expr::PropAccess { variable, property } if variable == "p" && property == "tags"
            ));
            assert!(matches!(right, Expr::Variable(v) if v == "tag"));
        }
        _ => panic!("expected Filter"),
    }
}

/// The mutation `where` is the same expression as a read filter, so
/// `contains` and `starts_with` are legal there over the target's property.
#[test]
fn test_parse_contains_and_starts_with_in_mutation_predicate() {
    for (input, op, property) in [
        (
            "query drop_person($tag: String) { delete Person where tags contains $tag }",
            CompOp::Contains,
            "tags",
        ),
        (
            "query drop_person($q: String) { delete Person where name starts_with $q }",
            CompOp::StartsWith,
            "name",
        ),
    ] {
        let qf = parse_query(input).unwrap();
        let Mutation::Delete(delete) = &qf.single_decl().mutations[0] else {
            panic!("expected a delete");
        };
        let (left, parsed_op, right) = comparison(&delete.predicate);
        assert_eq!(parsed_op, op);
        assert_eq!(*left, Expr::mutation_property("Person", property));
        assert!(matches!(right, Expr::Variable(_)));
    }
}

/// `expected` is the filter of `match { $p: Person  $f: Person  <filter> }`.
fn parse_filter_of(filter: &str) -> Expr {
    let input = format!(
        "query q($q: String) {{ match {{ $p: Person  $f: Person  {filter} }} return {{ $p.name }} }}"
    );
    let qf = parse_query(&input).unwrap_or_else(|error| panic!("{filter}: {error}"));
    match &qf.single_decl().match_clause[2] {
        Clause::Filter(expr) => expr.clone(),
        other => panic!("{filter}: expected a filter, got {other:?}"),
    }
}

fn prop(variable: &str, property: &str) -> Expr {
    Expr::PropAccess {
        variable: variable.to_string(),
        property: property.to_string(),
    }
}

fn int(value: i64) -> Expr {
    Expr::Literal(Literal::Integer(value))
}

fn and(left: Expr, right: Expr) -> Expr {
    Expr::Binary {
        left: Box::new(left),
        op: BinaryOp::And,
        right: Box::new(right),
    }
}

fn or(left: Expr, right: Expr) -> Expr {
    Expr::Binary {
        left: Box::new(left),
        op: BinaryOp::Or,
        right: Box::new(right),
    }
}

fn not(inner: Expr) -> Expr {
    Expr::Not(Box::new(inner))
}

#[test]
fn test_parse_precedence_ladder() {
    let a = || Expr::comparison(prop("p", "a"), CompOp::Eq, int(1));
    let b = || Expr::comparison(prop("p", "b"), CompOp::Eq, int(2));
    let c = || Expr::comparison(prop("p", "c"), CompOp::Eq, int(3));
    let null_test = |negated: bool| Expr::IsNull {
        expr: Box::new(prop("p", "email")),
        negated,
    };
    for (filter, expected) in [
        ("$p.a = 1 or $p.b = 2 and $p.c = 3", or(a(), and(b(), c()))),
        (
            "($p.a = 1 or $p.b = 2) and $p.c = 3",
            and(or(a(), b()), c()),
        ),
        (
            "$p.a = 1 and $p.b = 2 and $p.c = 3",
            and(and(a(), b()), c()),
        ),
        ("$p.a = 1 or $p.b = 2 or $p.c = 3", or(or(a(), b()), c())),
        ("not $p.a = 1 and $p.b = 2", and(not(a()), b())),
        ("not ($p.a = 1 and $p.b = 2)", not(and(a(), b()))),
        ("not not $p.a = 1", not(not(a()))),
        (
            "$p.email is not null and $p.a = 1",
            and(null_test(true), a()),
        ),
        ("not $p.email is null", not(null_test(false))),
        (
            "($p.a = 1) = true",
            Expr::comparison(a(), CompOp::Eq, Expr::Literal(Literal::Bool(true))),
        ),
        (
            "($p.a = 1) is null",
            Expr::IsNull {
                expr: Box::new(a()),
                negated: false,
            },
        ),
        ("(($p.a = 1))", a()),
        ("$p.active", prop("p", "active")),
        ("$q", Expr::Variable("q".to_string())),
    ] {
        assert_eq!(parse_filter_of(filter), expected, "{filter}");
    }
}

#[test]
fn test_parse_chained_comparison_is_a_positional_error() {
    let input =
        "query q() { match { $p: Person  $f: Person  $p.age < $f.age < 3 } return { $p.name } }";
    let error = parse_query(input).unwrap_err().to_string();
    assert!(error.starts_with("parse error:"), "{error}");
    assert!(!error.contains("chained"), "{error}");
}

#[test]
fn test_parse_bare_search_conjunct_is_spelled_true() {
    let search = Expr::Search {
        field: Box::new(prop("p", "name")),
        query: Box::new(Expr::Variable("q".to_string())),
    };
    let predicate = Expr::comparison(
        search.clone(),
        CompOp::Eq,
        Expr::Literal(Literal::Bool(true)),
    );
    let age = Expr::comparison(prop("p", "age"), CompOp::Gt, int(3));
    assert_eq!(parse_filter_of("search($p.name, $q)"), predicate);
    assert_eq!(
        parse_filter_of("search($p.name, $q) and $p.age > 3"),
        and(predicate.clone(), age.clone())
    );
    assert_eq!(
        parse_filter_of("$p.age > 3 and search($p.name, $q) = true"),
        and(age.clone(), predicate)
    );
    assert_eq!(
        parse_filter_of("search($p.name, $q) or $p.age > 3"),
        or(search.clone(), age)
    );
    assert_eq!(parse_filter_of("not search($p.name, $q)"), not(search));
}

#[test]
fn test_parse_not_brace_stays_the_pattern_negation() {
    let input = r#"
query q() {
match {
    $p: Person
    not { $p knows $f }
    not $p.active
}
return { $p.name }
}
"#;
    let qf = parse_query(input).unwrap();
    let q = qf.single_decl();
    assert!(
        matches!(&q.match_clause[1], Clause::Subquery(subquery) if subquery.keyword == BlockKeyword::Not)
    );
    assert!(matches!(&q.match_clause[2], Clause::Filter(Expr::Not(_))));
}

#[test]
fn test_parse_boolean_words_are_word_bounded_and_reserved_words_never_edge_names() {
    assert_eq!(
        parse_filter_of("$p.trueish = 1"),
        Expr::comparison(prop("p", "trueish"), CompOp::Eq, int(1))
    );
    let qf = parse_query("query q() { delete Person where falsey = 1 and false_ = true }").unwrap();
    let Mutation::Delete(delete) = &qf.single_decl().mutations[0] else {
        panic!("expected a delete");
    };
    assert_eq!(
        delete.predicate,
        and(
            Expr::comparison(
                Expr::mutation_property("Person", "falsey"),
                CompOp::Eq,
                int(1)
            ),
            Expr::comparison(
                Expr::mutation_property("Person", "false_"),
                CompOp::Eq,
                Expr::Literal(Literal::Bool(true))
            ),
        )
    );
    let param = |name: &str| Expr::Variable(name.to_string());
    assert_eq!(parse_filter_of("$a and $b"), and(param("a"), param("b")));
    assert_eq!(parse_filter_of("$a or $b"), or(param("a"), param("b")));
    let qf =
        parse_query("query q() { match { $p: Person  $p andy $f } return { $p.name } }").unwrap();
    assert!(matches!(
        &qf.single_decl().match_clause[1],
        Clause::Traversal(traversal) if traversal.edge_name == "andy"
    ));
}

#[test]
fn test_parse_reserved_words() {
    let reserved_property = |word: &str| {
        format!(
            "parse error: `{word}` is a reserved word; a property of that name is written `$p.{word}` in a read and cannot be named bare in a mutation `where`"
        )
    };
    for word in ["and", "or", "not", "is", "null"] {
        let read =
            format!("query q() {{ match {{ $p: Person  {word} = 1 }} return {{ $p.name }} }}");
        assert_eq!(
            parse_query(&read).unwrap_err().to_string(),
            reserved_property(word),
            "{read}"
        );
        let mutation = format!("query q() {{ delete Person where {word} = 1 }}");
        assert_eq!(
            parse_query(&mutation).unwrap_err().to_string(),
            reserved_property(word),
            "{mutation}"
        );
        let alias = format!("query q() {{ match {{ $p: Person }} return {{ $p.age as {word} }} }}");
        assert_eq!(
            parse_query(&alias).unwrap_err().to_string(),
            format!("parse error: `{word}` is a reserved word and cannot be a return alias"),
            "{alias}"
        );
    }
    assert_eq!(
        parse_filter_of("$p.and = 1"),
        Expr::comparison(prop("p", "and"), CompOp::Eq, int(1))
    );
    let binding =
        parse_query("query q() { match { $p: Person { and: 1, null: 2 } } return { $p.or } }")
            .unwrap();
    let Clause::Binding(binding) = &binding.single_decl().match_clause[0] else {
        panic!("expected a binding");
    };
    assert_eq!(binding.prop_matches[0].prop_name, "and");
    assert_eq!(binding.prop_matches[1].prop_name, "null");
    let insert = parse_query("query q() { insert Person { and: 1, is: 2 } }").unwrap();
    let Mutation::Insert(insert) = &insert.single_decl().mutations[0] else {
        panic!("expected an insert");
    };
    assert_eq!(insert.assignments[0].property, "and");
    assert_eq!(insert.assignments[1].property, "is");
    let identifiers =
        parse_query("query q() { match { $p: Person  $p.android = nothing } return { $p.age as notnull, $p.name as island } }")
            .unwrap();
    let decl = identifiers.single_decl();
    assert_eq!(
        parse_filter_of("$p.android = nothing"),
        Expr::comparison(
            prop("p", "android"),
            CompOp::Eq,
            Expr::AliasRef("nothing".to_string())
        )
    );
    assert_eq!(decl.return_clause[0].alias.as_deref(), Some("notnull"));
    assert_eq!(decl.return_clause[1].alias.as_deref(), Some("island"));
}

#[test]
fn test_parse_mutation_where_boolean_shapes() {
    let qf = parse_query(
        r#"
query exact() {
delete Knows where @src = "a" and @dst = "b" or not since is null
}
"#,
    )
    .unwrap();
    let Mutation::Delete(delete) = &qf.single_decl().mutations[0] else {
        panic!("expected a delete");
    };
    let endpoint = |field: &str, value: &str| {
        Expr::comparison(
            Expr::mutation_property("Knows", field),
            CompOp::Eq,
            Expr::Literal(Literal::String(value.to_string())),
        )
    };
    assert_eq!(
        delete.predicate,
        or(
            and(endpoint("@src", "a"), endpoint("@dst", "b")),
            not(Expr::IsNull {
                expr: Box::new(Expr::mutation_property("Knows", "since")),
                negated: false,
            })
        )
    );
}

#[test]
fn test_parse_assignment_and_binding_match_values_are_expressions() {
    let qf = parse_query(
        r#"
query q($flag: Bool, $cut: I64) {
match { $p: Person { active: $flag or $cut > 3, name: age } }
return { $p.name }
}
"#,
    )
    .unwrap();
    let Clause::Binding(binding) = &qf.single_decl().match_clause[0] else {
        panic!("expected a binding");
    };
    assert_eq!(
        binding.prop_matches[0].value,
        or(
            Expr::Variable("flag".to_string()),
            Expr::comparison(Expr::Variable("cut".to_string()), CompOp::Gt, int(3))
        )
    );
    assert_eq!(
        binding.prop_matches[1].value,
        Expr::mutation_property("Person", "age")
    );
    let qf =
        parse_query("query q($cut: I64) { insert Person { active: not $cut > 3, name: @id } }")
            .unwrap();
    let Mutation::Insert(insert) = &qf.single_decl().mutations[0] else {
        panic!("expected an insert");
    };
    assert_eq!(
        insert.assignments[0].value,
        not(Expr::comparison(
            Expr::Variable("cut".to_string()),
            CompOp::Gt,
            int(3)
        ))
    );
    assert_eq!(
        insert.assignments[1].value,
        Expr::mutation_property("Person", "@id")
    );
}

#[test]
fn test_parse_boolean_projection_with_alias() {
    let qf = parse_query(
        "query q() { match { $p: Person } return { $p.slug, $p.age > 30 as adult, $p.email is not null as reachable } }",
    )
    .unwrap();
    let decl = qf.single_decl();
    assert_eq!(
        decl.return_clause[1].expr,
        Expr::comparison(prop("p", "age"), CompOp::Gt, int(30))
    );
    assert_eq!(decl.return_clause[1].alias.as_deref(), Some("adult"));
    assert_eq!(
        decl.return_clause[2].expr,
        Expr::IsNull {
            expr: Box::new(prop("p", "email")),
            negated: true,
        }
    );
    assert_eq!(decl.return_clause[2].alias.as_deref(), Some("reachable"));
}

#[test]
fn test_parse_starts_with_filter() {
    let input = r#"
query autocomplete($q: String) {
match {
    $p: Person
    $p.name starts_with $q
}
return { $p.name }
}
"#;
    let qf = parse_query(input).unwrap();
    let q = qf.single_decl();
    match &q.match_clause[1] {
        Clause::Filter(f) => {
            let (left, op, right) = comparison(f);
            assert_eq!(op, CompOp::StartsWith);
            assert!(matches!(
                left,
                Expr::PropAccess { variable, property } if variable == "p" && property == "name"
            ));
            assert!(matches!(right, Expr::Variable(v) if v == "q"));
        }
        _ => panic!("expected Filter"),
    }
}

#[test]
fn test_parse_triangle() {
    let input = r#"
query triangles($name: String) {
match {
    $a: Person { name: $name }
    $a knows $b
    $b knows $c
    $c knows $a
}
return { $b.name, $c.name }
}
"#;
    let qf = parse_query(input).unwrap();
    let q = qf.single_decl();
    assert_eq!(q.match_clause.len(), 4);
}

#[test]
fn test_parse_avg_aggregation() {
    let input = r#"
query avg_age_by_company() {
match {
    $p: Person
    $p worksAt $c
}
return {
    $c.name
    avg($p.age) as avg_age
    count($p) as headcount
}
order { headcount desc }
}
"#;
    let qf = parse_query(input).unwrap();
    let q = qf.single_decl();
    assert_eq!(q.return_clause.len(), 3);
}

#[test]
fn test_parse_insert_mutation() {
    let input = r#"
query add_person($name: String, $age: I32) {
insert Person {
    name: $name
    age: $age
}
}
"#;
    let qf = parse_query(input).unwrap();
    let q = qf.single_decl();
    match q.mutations.first().expect("expected mutation") {
        Mutation::Insert(ins) => {
            assert_eq!(ins.type_name, "Person");
            assert_eq!(ins.assignments.len(), 2);
        }
        _ => panic!("expected Insert mutation"),
    }
}

#[test]
fn test_parse_update_mutation() {
    let input = r#"
query set_age($name: String, $age: I32) {
update Person set {
    age: $age
} where name = $name
}
"#;
    let qf = parse_query(input).unwrap();
    let q = qf.single_decl();
    match q.mutations.first().expect("expected mutation") {
        Mutation::Update(upd) => {
            assert_eq!(upd.type_name, "Person");
            assert_eq!(upd.assignments.len(), 1);
            let (property, op, _) = comparison(&upd.predicate);
            assert_eq!(*property, Expr::mutation_property("Person", "name"));
            assert_eq!(op, CompOp::Eq);
        }
        _ => panic!("expected Update mutation"),
    }
}

#[test]
fn test_parse_delete_mutation() {
    let input = r#"
query drop_person($name: String) {
delete Person where name = $name
}
"#;
    let qf = parse_query(input).unwrap();
    let q = qf.single_decl();
    match q.mutations.first().expect("expected mutation") {
        Mutation::Delete(del) => {
            assert_eq!(del.type_name, "Person");
            let (property, op, _) = comparison(&del.predicate);
            assert_eq!(*property, Expr::mutation_property("Person", "name"));
            assert_eq!(op, CompOp::Eq);
        }
        _ => panic!("expected Delete mutation"),
    }
}

#[test]
fn test_parse_date_and_datetime_literals() {
    let input = r#"
query dated() {
match {
    $e: Event
    $e.on = date("2026-02-14")
    $e.at >= datetime("2026-02-14T10:00:00Z")
}
return { $e.id }
}
"#;
    let qf = parse_query(input).unwrap();
    let q = qf.single_decl();
    match &q.match_clause[1] {
        Clause::Filter(f) => match comparison(f).2 {
            Expr::Literal(Literal::Date(v)) => assert_eq!(v, "2026-02-14"),
            other => panic!("expected date literal, got {:?}", other),
        },
        _ => panic!("expected Filter"),
    }
    match &q.match_clause[2] {
        Clause::Filter(f) => match comparison(f).2 {
            Expr::Literal(Literal::DateTime(v)) => assert_eq!(v, "2026-02-14T10:00:00Z"),
            other => panic!("expected datetime literal, got {:?}", other),
        },
        _ => panic!("expected Filter"),
    }
}

#[test]
fn test_parse_now_expression_and_mutation_value() {
    let input = r#"
query clock() {
match {
    $e: Event
    $e.at <= now()
}
return { now() as ts }
}
"#;
    let qf = parse_query(input).unwrap();
    let q = qf.single_decl();
    match &q.match_clause[1] {
        Clause::Filter(f) => assert!(matches!(comparison(f).2, Expr::Now)),
        _ => panic!("expected Filter"),
    }
    assert!(matches!(q.return_clause[0].expr, Expr::Now));

    let mutation = parse_query(
        r#"
query stamp() {
update Event set { updated_at: now() } where created_at <= now()
}
"#,
    )
    .unwrap();
    match mutation.single_decl().mutations.first().unwrap() {
        Mutation::Update(update) => {
            assert!(matches!(update.assignments[0].value, Expr::Now));
            let (property, op, value) = comparison(&update.predicate);
            assert_eq!(*property, Expr::mutation_property("Event", "created_at"));
            assert_eq!(op, CompOp::Le);
            assert!(matches!(value, Expr::Now));
        }
        _ => panic!("expected update mutation"),
    }
}

#[test]
fn test_parse_multi_mutation() {
    let input = r#"
query add_and_link($name: String, $age: I32, $friend: String) {
insert Person { name: $name, age: $age }
insert Knows { from: $name, to: $friend }
}
"#;
    let qf = parse_query(input).unwrap();
    let q = qf.single_decl();
    assert_eq!(q.mutations.len(), 2);
    assert!(matches!(&q.mutations[0], Mutation::Insert(ins) if ins.type_name == "Person"));
    assert!(matches!(&q.mutations[1], Mutation::Insert(ins) if ins.type_name == "Knows"));
}

#[test]
fn test_parse_multi_mutation_mixed_ops() {
    let input = r#"
query create_and_clean($name: String, $age: I32, $old: String) {
insert Person { name: $name, age: $age }
delete Person where name = $old
}
"#;
    let qf = parse_query(input).unwrap();
    let q = qf.single_decl();
    assert_eq!(q.mutations.len(), 2);
    assert!(matches!(&q.mutations[0], Mutation::Insert(_)));
    assert!(matches!(&q.mutations[1], Mutation::Delete(_)));
}

#[test]
fn test_parse_single_mutation_backward_compat() {
    let input = r#"
query add($name: String, $age: I32) {
insert Person { name: $name, age: $age }
}
"#;
    let qf = parse_query(input).unwrap();
    assert_eq!(qf.single_decl().mutations.len(), 1);
}

#[test]
fn test_parse_list_literal() {
    let input = r#"
query listy() {
match { $p: Person { tags: ["rust", "db"] } }
return { $p.tags }
}
"#;
    let qf = parse_query(input).unwrap();
    let q = qf.single_decl();
    match &q.match_clause[0] {
        Clause::Binding(b) => match &b.prop_matches[0].value {
            Expr::Literal(Literal::List(items)) => {
                assert_eq!(items.len(), 2);
            }
            other => panic!("expected list literal, got {:?}", other),
        },
        _ => panic!("expected Binding"),
    }
}

#[test]
fn test_parse_nearest_ordering_and_vector_param_type() {
    let input = r#"
query similar($q: Vector(3)) {
match { $d: Doc }
return { $d.id }
order { nearest($d.embedding, $q) }
limit 5
}
"#;
    let qf = parse_query(input).unwrap();
    let q = qf.single_decl();
    assert_eq!(q.params[0].type_name, "Vector(3)");
    assert_eq!(q.order_clause.len(), 1);
    assert!(!q.order_clause[0].descending);
    match &q.order_clause[0].expr {
        Expr::Nearest {
            variable,
            property,
            query,
        } => {
            assert_eq!(variable, "d");
            assert_eq!(property, "embedding");
            assert!(matches!(query.as_ref(), Expr::Variable(v) if v == "q"));
        }
        other => panic!("expected nearest ordering, got {:?}", other),
    }
}

#[test]
fn test_parse_nearest_with_spaced_vector_param_type() {
    let input = r#"
query similar($q: Vector( 3 ) ?) {
match { $d: Doc }
return { $d.id }
order { nearest($d.embedding, $q) }
limit 5
}
"#;
    let qf = parse_query(input).unwrap();
    let q = qf.single_decl();
    assert_eq!(q.params[0].type_name, "Vector(3)");
    assert!(q.params[0].nullable);
}

#[test]
fn test_parse_list_and_datetime_param_types() {
    let input = r#"
query tasks($tags: [String], $days: [Date]?, $due_at: DateTime) {
match { $t: Task }
return { $t.slug }
}
"#;
    let qf = parse_query(input).unwrap();
    let q = qf.single_decl();
    assert_eq!(q.params[0].type_name, "[String]");
    assert!(!q.params[0].nullable);
    assert_eq!(q.params[1].type_name, "[Date]");
    assert!(q.params[1].nullable);
    assert_eq!(q.params[2].type_name, "DateTime");
}

#[test]
fn test_parse_nearest_rejects_direction_modifier() {
    let input = r#"
query similar($q: Vector(3)) {
match { $d: Doc }
return { $d.id }
order { nearest($d.embedding, $q) desc }
limit 5
}
"#;
    assert!(parse_query(input).is_err());
}

#[test]
fn test_parse_nearest_expression_in_return_projection() {
    let input = r#"
query similar($q: Vector(3)) {
match { $d: Doc }
return { $d.id, nearest($d.embedding, $q) as score }
order { nearest($d.embedding, $q) }
limit 5
}
"#;
    let qf = parse_query(input).unwrap();
    let q = qf.single_decl();
    assert_eq!(q.return_clause.len(), 2);
    match &q.return_clause[1].expr {
        Expr::Nearest {
            variable,
            property,
            query,
        } => {
            assert_eq!(variable, "d");
            assert_eq!(property, "embedding");
            assert!(matches!(query.as_ref(), Expr::Variable(v) if v == "q"));
        }
        other => panic!(
            "expected nearest expression in return projection, got {:?}",
            other
        ),
    }
    assert_eq!(q.return_clause[1].alias.as_deref(), Some("score"));
}

#[test]
fn test_parse_search_clause_sugar() {
    let input = r#"
query q($q: String) {
match {
    $s: Signal
    search($s.summary, $q)
}
return { $s.slug }
}
"#;
    let qf = parse_query(input).unwrap();
    let q = qf.single_decl();
    assert_eq!(q.match_clause.len(), 2);
    match &q.match_clause[1] {
        Clause::Filter(f) => {
            let (left, op, right) = comparison(f);
            assert_eq!(op, CompOp::Eq);
            assert!(matches!(right, Expr::Literal(Literal::Bool(true))));
            match left {
                Expr::Search { field, query } => {
                    assert!(matches!(
                        field.as_ref(),
                        Expr::PropAccess { variable, property } if variable == "s" && property == "summary"
                    ));
                    assert!(matches!(query.as_ref(), Expr::Variable(v) if v == "q"));
                }
                other => panic!("expected search expression, got {:?}", other),
            }
        }
        other => panic!("expected filter clause, got {:?}", other),
    }
}

#[test]
fn test_parse_fuzzy_clause_with_max_edits() {
    let input = r#"
query q($q: String) {
match {
    $s: Signal
    fuzzy($s.summary, $q, 2)
}
return { $s.slug }
}
"#;
    let qf = parse_query(input).unwrap();
    let q = qf.single_decl();
    assert_eq!(q.match_clause.len(), 2);
    match &q.match_clause[1] {
        Clause::Filter(f) => {
            let (left, op, right) = comparison(f);
            assert_eq!(op, CompOp::Eq);
            assert!(matches!(right, Expr::Literal(Literal::Bool(true))));
            match left {
                Expr::Fuzzy {
                    field,
                    query,
                    max_edits,
                } => {
                    assert!(matches!(
                        field.as_ref(),
                        Expr::PropAccess { variable, property } if variable == "s" && property == "summary"
                    ));
                    assert!(matches!(query.as_ref(), Expr::Variable(v) if v == "q"));
                    assert!(matches!(
                        max_edits.as_deref(),
                        Some(Expr::Literal(Literal::Integer(2)))
                    ));
                }
                other => panic!("expected fuzzy expression, got {:?}", other),
            }
        }
        other => panic!("expected filter clause, got {:?}", other),
    }
}

#[test]
fn test_parse_match_text_clause_sugar() {
    let input = r#"
query q($q: String) {
match {
    $s: Signal
    match_text($s.summary, $q)
}
return { $s.slug }
}
"#;
    let qf = parse_query(input).unwrap();
    let q = qf.single_decl();
    assert_eq!(q.match_clause.len(), 2);
    match &q.match_clause[1] {
        Clause::Filter(f) => {
            let (left, op, right) = comparison(f);
            assert_eq!(op, CompOp::Eq);
            assert!(matches!(right, Expr::Literal(Literal::Bool(true))));
            match left {
                Expr::MatchText { field, query } => {
                    assert!(matches!(
                        field.as_ref(),
                        Expr::PropAccess { variable, property } if variable == "s" && property == "summary"
                    ));
                    assert!(matches!(query.as_ref(), Expr::Variable(v) if v == "q"));
                }
                other => panic!("expected match_text expression, got {:?}", other),
            }
        }
        other => panic!("expected filter clause, got {:?}", other),
    }
}

#[test]
fn test_parse_bm25_expression_in_order() {
    let input = r#"
query q($q: String) {
match { $s: Signal }
return { $s.slug, bm25($s.summary, $q) as score }
order { bm25($s.summary, $q) desc }
limit 5
}
"#;
    let qf = parse_query(input).unwrap();
    let q = qf.single_decl();
    assert_eq!(q.return_clause.len(), 2);
    match &q.return_clause[1].expr {
        Expr::Bm25 { field, query } => {
            assert!(matches!(
                field.as_ref(),
                Expr::PropAccess { variable, property } if variable == "s" && property == "summary"
            ));
            assert!(matches!(query.as_ref(), Expr::Variable(v) if v == "q"));
        }
        other => panic!("expected bm25 expression, got {:?}", other),
    }
    assert_eq!(q.order_clause.len(), 1);
    assert!(q.order_clause[0].descending);
}

#[test]
fn test_parse_rrf_ordering_with_nearest_and_bm25() {
    let input = r#"
query q($vq: Vector(3), $tq: String) {
match { $s: Signal }
return { $s.slug }
order { rrf(nearest($s.embedding, $vq), bm25($s.summary, $tq), 60) desc }
limit 5
}
"#;
    let qf = parse_query(input).unwrap();
    let q = qf.single_decl();
    assert_eq!(q.order_clause.len(), 1);
    assert!(q.order_clause[0].descending);
    match &q.order_clause[0].expr {
        Expr::Rrf {
            primary,
            secondary,
            k,
        } => {
            assert!(matches!(primary.as_ref(), Expr::Nearest { .. }));
            assert!(matches!(secondary.as_ref(), Expr::Bm25 { .. }));
            assert!(matches!(
                k.as_deref(),
                Some(Expr::Literal(Literal::Integer(60)))
            ));
        }
        other => panic!("expected rrf expression, got {:?}", other),
    }
}

#[test]
fn test_parse_error_diagnostic_has_span() {
    let input = r#"
query q() {
match {
    $p: Person
}
return { $p.name
}
"#;
    let err = parse_query_diagnostic(input).unwrap_err();
    assert!(err.position.is_some());
}

#[test]
fn test_parse_traversal_edge_binding() {
    // `$p $w:knows $f` — the optional edge binding names the matched edge
    // so its properties become addressable (`$w.since`).
    let input = r#"
query rated_friends($name: String) {
match {
    $p: Person { name: $name }
    $p $w:knows $f
    $p $x:<knows> $g
    $p knows $h
}
return { $f.name }
}
"#;
    let qf = parse_query(input).unwrap();
    let q = qf.single_decl();
    assert_eq!(q.match_clause.len(), 4);
    match &q.match_clause[1] {
        Clause::Traversal(t) => {
            assert_eq!(t.src, "p");
            assert_eq!(t.edge_name, "knows");
            assert_eq!(t.dst, "f");
            assert!(!t.undirected);
            assert_eq!(t.edge_binding.as_deref(), Some("w"));
        }
        c => panic!("expected Traversal, got {c:?}"),
    }
    match &q.match_clause[2] {
        Clause::Traversal(t) => {
            assert!(t.undirected, "binding composes with undirected form");
            assert_eq!(t.edge_binding.as_deref(), Some("x"));
        }
        c => panic!("expected Traversal, got {c:?}"),
    }
    match &q.match_clause[3] {
        Clause::Traversal(t) => {
            assert_eq!(t.edge_binding, None, "binding stays optional");
        }
        c => panic!("expected Traversal, got {c:?}"),
    }
}

fn parse_branch(input: &str) -> BranchStmt {
    match parse_query(input).unwrap().body {
        FileBody::Branch(stmt) => stmt,
        FileBody::Queries(queries) => panic!(
            "expected a branch statement, got {} declarations",
            queries.len()
        ),
        FileBody::Show(id) => panic!("expected a branch statement, got show {id:?}"),
        FileBody::Explain(_) => panic!("expected a branch statement, got an explain statement"),
    }
}

fn create(name: &str, from: Option<&str>) -> BranchStmt {
    BranchStmt::Write(BranchWrite::Create {
        name: name.to_string(),
        from: from.map(str::to_string),
    })
}

fn delete(name: &str) -> BranchStmt {
    BranchStmt::Write(BranchWrite::Delete {
        name: name.to_string(),
    })
}

fn merge(source: &str, into: Option<&str>) -> BranchStmt {
    BranchStmt::Write(BranchWrite::Merge {
        source: source.to_string(),
        into: into.map(str::to_string),
    })
}

#[test]
fn branch_statements_parse_with_and_without_their_defaults() {
    let created = parse_branch("branch create b0");
    assert_eq!(created, create("b0", None));
    assert!(created.is_write());
    assert_eq!(created.statement_name(), "branch create");
    assert_eq!(
        parse_branch("branch create b0 from main"),
        create("b0", Some("main"))
    );
    let deleted = parse_branch("branch delete b0");
    assert_eq!(deleted, delete("b0"));
    assert!(deleted.is_write());
    assert_eq!(deleted.statement_name(), "branch delete");
    let merged = parse_branch("branch merge b0");
    assert_eq!(merged, merge("b0", None));
    assert!(merged.is_write());
    assert_eq!(merged.statement_name(), "branch merge");
    assert_eq!(
        parse_branch("branch merge b0 into main"),
        merge("b0", Some("main"))
    );
    let listed = parse_branch("branch list");
    assert_eq!(listed, BranchStmt::List);
    assert!(!listed.is_write());
    assert_eq!(listed.statement_name(), "branch list");
    assert_eq!(
        parse_branch("  branch\n  list // trailing comment\n"),
        BranchStmt::List
    );
}

#[test]
fn branch_names_outside_the_identifier_alphabet_are_quoted() {
    assert_eq!(
        parse_branch(r#"branch create "review/add-benchmark""#),
        create("review/add-benchmark", None)
    );
    assert_eq!(
        parse_branch(r#"branch merge "release.1.2" into "Main""#),
        merge("release.1.2", Some("Main"))
    );
    assert_eq!(parse_branch(r#"branch delete "a\"b""#), delete("a\"b"));
    assert_eq!(parse_branch(r#"branch create "b 0""#), create("b 0", None));
    assert!(parse_query("branch create B0").is_err());
    assert!(parse_query("branch create 0b").is_err());
    assert!(parse_query("branch create review/add-benchmark").is_err());
    assert_eq!(parse_branch("branch create list"), create("list", None));
    assert_eq!(parse_branch("branch delete from"), delete("from"));
    assert_eq!(parse_branch("branch merge into"), merge("into", None));
    assert_eq!(
        parse_branch(r#"branch create "from" from main"#),
        create("from", Some("main"))
    );
    assert!(parse_query("branch create from main").is_err());
    assert!(parse_query("branch create b0 from").is_err());
    assert!(parse_query("branch merge b0 into").is_err());
    assert!(parse_query("branch list all").is_err());
}

#[test]
fn branch_keywords_end_at_a_word_boundary() {
    assert!(parse_query("branch create b0").is_ok());
    for input in [
        "branch createb0",
        "branchcreate b0",
        "branch merge b0 intomain",
        "branch listing",
        "branch_list",
        "branch",
    ] {
        assert!(parse_query(input).is_err(), "{input}");
    }
}

#[test]
fn branch_name_refusals_keep_the_spelled_and_acted_on_name_identical() {
    let err = parse_query(r#"branch create """#).unwrap_err();
    assert!(err.to_string().contains("cannot be empty"), "{err}");
    let err = parse_query(r#"branch create " ""#).unwrap_err();
    assert!(
        err.to_string().contains("leading or trailing whitespace"),
        "{err}"
    );
    let err = parse_query(r#"branch merge b0 into "main ""#).unwrap_err();
    assert!(
        err.to_string().contains("leading or trailing whitespace"),
        "{err}"
    );
    let err = parse_query(r#"branch create "b\n0""#).unwrap_err();
    assert!(err.to_string().contains("control character"), "{err}");
}

#[test]
fn branch_statement_never_shares_a_file_with_a_declaration() {
    let decl = "query q() {\n    match { $p: Person }\n    return { $p.name }\n}\n";
    assert!(parse_query(decl).is_ok());
    let err = parse_query(&format!("branch create b0\n{decl}")).unwrap_err();
    assert!(
        err.to_string()
            .contains("a branch statement stands alone in its file"),
        "{err}"
    );
    let err = parse_query("branch create b0\nbranch create b1\n").unwrap_err();
    assert!(
        err.to_string()
            .contains("a branch statement stands alone in its file"),
        "{err}"
    );
    assert!(parse_query(&format!("{decl}branch create b0\n")).is_err());
    assert!(
        parse_query("query m() {\n    insert Person { name: \"a\" }\n    branch create b0\n}\n")
            .is_err()
    );
    let err = parse_query("mutation { insert Person { name: \"a\" } }").unwrap_err();
    assert!(err.to_string().contains("expected query_file"), "{err}");
}

fn parse_explained(input: &str) -> QueryDecl {
    match parse_query(input).unwrap().body {
        FileBody::Explain(decl) => decl,
        other => panic!("expected an explain statement, got {other:?}"),
    }
}

#[test]
fn explain_statement_wraps_one_declaration() {
    let decl = parse_explained(
        "explain query q($n: String) {\n    match { $p: Person { name: $n } }\n    return { $p.name }\n}\n",
    );
    assert_eq!(decl.name, "q");
    assert_eq!(decl.params.len(), 1);
    assert_eq!(decl.return_clause.len(), 1);
    let decl =
        parse_explained("  explain\n  query m() { insert Person { name: \"a\" } } // trailing\n");
    assert_eq!(decl.mutations.len(), 1);
}

#[test]
fn explain_statement_never_shares_a_file_with_a_declaration() {
    let decl = "query q() {\n    match { $p: Person }\n    return { $p.name }\n}\n";
    let err = parse_query(&format!("explain {decl}{decl}")).unwrap_err();
    assert!(
        err.to_string()
            .contains("an `explain` statement stands alone in its file"),
        "{err}"
    );
    let err = parse_query(&format!("explain {decl}explain {decl}")).unwrap_err();
    assert!(
        err.to_string()
            .contains("an `explain` statement stands alone in its file"),
        "{err}"
    );
    for input in [
        format!("{decl}explain {decl}"),
        format!("explain {decl}branch list\n"),
        format!("branch list\nexplain {decl}"),
    ] {
        let error = parse_query(&input).unwrap_err();
        assert!(
            error.diagnostic().is_some_and(|diagnostic| {
                diagnostic.kind == crate::query::diagnostic::QueryDiagnosticKind::Parse
            }),
            "{input}: {error}"
        );
    }
}

#[test]
fn explain_keyword_ends_at_a_word_boundary_and_needs_a_declaration() {
    let decl = "query q() {\n    match { $p: Person }\n    return { $p.name }\n}\n";
    for input in [
        "explainquery q() { match { $p: Person } return { $p.name } }".to_string(),
        format!("explain_ {decl}"),
        "explain".to_string(),
        "explain branch list".to_string(),
        format!("explain explain {decl}"),
    ] {
        let error = parse_query(&input).unwrap_err();
        assert!(
            error.diagnostic().is_some_and(|diagnostic| {
                diagnostic.kind == crate::query::diagnostic::QueryDiagnosticKind::Parse
            }),
            "{input}: {error}"
        );
    }
}

#[test]
fn explain_stays_an_identifier_inside_bodies() {
    let input = r#"
query explain($e: String) {
match {
    $p: Person { explain: $e }
}
return { $p.explain as explain }
}
"#;
    let qf = parse_query(input).unwrap();
    assert_eq!(qf.single_decl().name, "explain");
}

#[test]
fn branch_keywords_stay_identifiers_inside_bodies() {
    let input = r#"
query q($b: String) {
match {
    $p: Person { branch: $b }
    $p merge $q
    $q.list = "x"
}
return { $p.branch, $q.into as create }
}
"#;
    let qf = parse_query(input).unwrap();
    assert_eq!(qf.single_decl().name, "q");
    let insert =
        parse_query("query m() {\n    insert Knows { from: \"a\", to: \"b\" }\n}\n").unwrap();
    assert_eq!(insert.single_decl().mutations.len(), 1);
}

fn set(id: SettingId, value: SettingValue) -> SettingStmt {
    SettingStmt::Set { id, value }
}

fn ident(text: &str) -> SettingValue {
    SettingValue::Ident(text.to_string())
}

#[test]
fn settings_prefix_parses_before_every_body() {
    let decl = "query q() {\n    match { $p: Person }\n    return { $p.name }\n}\n";
    let file = parse_query(&format!(
        "set merge_lineage = off;\nreset merge_lineage;\nset ann_nprobes = 0;\nreset all;\n{decl}"
    ))
    .unwrap();
    assert_eq!(
        file.settings,
        vec![
            set(SettingId::MergeLineage, ident("off")),
            SettingStmt::Reset {
                id: Some(SettingId::MergeLineage)
            },
            set(SettingId::AnnNprobes, SettingValue::Integer(0)),
            SettingStmt::Reset { id: None },
        ]
    );
    assert_eq!(file.single_decl().name, "q");

    let merge = parse_query("set merge_lineage = off;\nbranch merge b0 into main;\n").unwrap();
    assert_eq!(
        merge.settings,
        vec![set(SettingId::MergeLineage, ident("off"))]
    );
    assert!(
        matches!(merge.body, FileBody::Branch(ref stmt) if *stmt == merge_stmt("b0", Some("main")))
    );
    assert!(matches!(
        parse_query("branch list").unwrap().body,
        FileBody::Branch(BranchStmt::List)
    ));

    let only_settings = parse_query("set merge_lineage = on;").unwrap();
    assert_eq!(only_settings.settings.len(), 1);
    assert!(matches!(only_settings.body, FileBody::Queries(ref queries) if queries.is_empty()));
    let empty = parse_query("").unwrap();
    assert!(empty.settings.is_empty());
    assert!(matches!(empty.body, FileBody::Queries(ref queries) if queries.is_empty()));

    let quoted = parse_query("set merge_lineage = \"off\";\nshow all;").unwrap();
    assert_eq!(
        quoted.settings,
        vec![set(
            SettingId::MergeLineage,
            SettingValue::Str("off".to_string())
        )]
    );
    assert!(matches!(quoted.body, FileBody::Show(None)));
    assert!(matches!(
        parse_query("show merge_lineage;").unwrap().body,
        FileBody::Show(Some(SettingId::MergeLineage))
    ));
    assert!(matches!(
        parse_query("  set\n  stage_write_concurrency = 3 ; // width\n show all ;")
            .unwrap()
            .body,
        FileBody::Show(None)
    ));
}

fn merge_stmt(source: &str, into: Option<&str>) -> BranchStmt {
    merge(source, into)
}

#[test]
fn empty_kind_separates_a_settings_only_file_from_no_statement() {
    use crate::query::ast::EmptyFile;

    assert_eq!(
        parse_query("").unwrap().empty_kind(),
        Some(EmptyFile::NoStatement)
    );
    assert_eq!(
        parse_query("set merge_lineage = on;").unwrap().empty_kind(),
        Some(EmptyFile::SettingsOnly)
    );
    assert_eq!(
        parse_query("set merge_lineage = on;\nshow all;")
            .unwrap()
            .empty_kind(),
        None
    );
    assert_eq!(
        parse_query("query q() {\n    match { $p: Person }\n    return { $p.name }\n}\n")
            .unwrap()
            .empty_kind(),
        None
    );
}

#[test]
fn settings_statements_are_checked_against_the_definition() {
    for (input, needle) in [
        (
            "set merge_lineage = v3;",
            "unknown value `v3` for setting `merge_lineage`; expected one of off, on, verify",
        ),
        ("set traversal = v2;", "unknown setting `traversal`"),
        ("reset traversal;", "unknown setting `traversal`"),
        ("show traversal;", "unknown setting `traversal`"),
        (
            "set ann_nprobes = \"many\";",
            "takes an integer of at least 0, got a string `many`",
        ),
        (
            "set stage_write_concurrency = 0;",
            "takes an integer in 1..=64, got 0",
        ),
        (
            "set stage_write_concurrency = 99999999999999999999;",
            "takes an integer in 1..=64, got 99999999999999999999",
        ),
        (
            "set merge_lineage = 5;",
            "unknown value `5` for setting `merge_lineage`",
        ),
        (
            "set search.nprobes = 5;",
            "unknown setting `search.nprobes`",
        ),
    ] {
        let err = parse_query_diagnostic(input).unwrap_err();
        assert!(
            err.message.contains(needle),
            "{input}: {} (the full text is `settings::tests::messages_follow_the_definition`'s)",
            err.message
        );
        assert!(err.position.is_some(), "{input} carries a position");
    }
    let rendered = parse_query("set merge_lineage = v3;")
        .unwrap_err()
        .to_string();
    assert!(
        rendered.starts_with("parse error: unknown value `v3` for setting `merge_lineage`"),
        "{rendered}"
    );
}

#[test]
fn settings_prefix_peek_agrees_with_the_parser() {
    let decl = "query q() {\n    match { $p: Person }\n    return { $p.name }\n}\n";
    for (input, expected) in [
        (format!("set merge_lineage = off;\n{decl}"), true),
        (
            format!("  \t\r\n// pinned\n/* block\n */ reset all;\n{decl}"),
            true,
        ),
        (format!("/* set merge_lineage = off; */\n{decl}"), false),
        (format!("// set merge_lineage = off;\n{decl}"), false),
        (decl.to_string(), false),
        ("show all;".to_string(), false),
        ("setmerge_lineage = off;".to_string(), false),
        ("reset_all;".to_string(), false),
        (
            "/* never closed set merge_lineage = off;".to_string(),
            false,
        ),
        (String::new(), false),
    ] {
        assert_eq!(has_settings_prefix(&input), expected, "{input:?}");
        if expected {
            assert!(
                !parse_query(&input).unwrap().settings.is_empty(),
                "{input:?}"
            );
        } else if let Ok(file) = parse_query(&input) {
            assert!(file.settings.is_empty(), "{input:?}");
        }
    }
}

#[test]
fn settings_keywords_end_at_a_word_boundary_and_stand_at_the_head() {
    for input in [
        "setmerge_lineage = off;",
        "set merge_lineage = off",
        "set merge_lineage off;",
        "set search . nprobes = 5;",
        "resetall;",
        "showall;",
        "show all",
        "show all; show all;",
        "branch list; set merge_lineage = off;",
        "query q() {\n    match { $p: Person }\n    return { $p.name }\n}\nset merge_lineage = off;",
    ] {
        assert!(parse_query(input).is_err(), "{input}");
    }
    let err = parse_query("show all;\nbranch list").unwrap_err();
    assert!(
        err.to_string()
            .contains("a show statement stands alone in its file"),
        "{err}"
    );
    let err = parse_query(
        "branch list;\nquery q() {\n    match { $p: Person }\n    return { $p.name }\n}\n",
    )
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("a branch statement stands alone in its file"),
        "{err}"
    );
}

#[test]
fn settings_keywords_stay_identifiers_inside_bodies() {
    let input = r#"
query q($all: String) {
match {
    $p: Person { set: $all }
    $p show $q
    $q.reset = "x"
}
return { $p.set, $q.traversal as all }
}
"#;
    let file = parse_query(input).unwrap();
    assert!(file.settings.is_empty());
    assert_eq!(file.single_decl().name, "q");
    assert_eq!(parse_branch("branch create set"), create("set", None));
    assert_eq!(parse_branch("branch create all"), create("all", None));
}

#[test]
fn query_without_parameter_list_reports_q002_at_the_name_end_with_fix() {
    let err =
        parse_query_diagnostic("query name {\n  match { $p: Person }\n  return { $p.name }\n}")
            .unwrap_err();
    assert_eq!(err.code.as_str(), "Q002");
    assert_eq!(
        err.message,
        "expected `(`: a query declares its parameters even when it has none"
    );
    assert_eq!(err.fix.as_deref(), Some("query name()"));
    let at = err.position.expect("positioned at the name's end");
    assert_eq!((at.line, at.column, at.byte), (1, 11, 10));
    assert!(err.stage.is_none());
    let rendered = parse_query("query name {").unwrap_err().to_string();
    assert_eq!(
        rendered,
        "parse error: expected `(`: a query declares its parameters even when it has none"
    );
    // A well-formed declaration before the broken one leaves the fix's
    // subject the broken one.
    let err =
        parse_query_diagnostic("query a() { match { $p: Person } return { $p.name } }\nquery b {")
            .unwrap_err();
    assert_eq!(err.fix.as_deref(), Some("query b()"));
    assert_eq!(err.position.unwrap().line, 2);
    // The declaration keeps its parameter list when it has one.
    assert!(parse_query("query a() { match { $p: Person } return { $p.name } }").is_ok());
}

#[test]
fn grammar_mismatch_reports_q001_at_the_deepest_failure() {
    let err = parse_query_diagnostic("mutation { insert Person { name: \"a\" } }").unwrap_err();
    assert_eq!(err.code.as_str(), "Q001");
    assert!(err.message.starts_with("expected "), "{}", err.message);
    assert_eq!(err.position.unwrap().byte, 0);
    assert!(err.fix.is_none());
    // A settings refusal is positioned at the offending token.
    let err = parse_query_diagnostic("set merge_lineage = v3;").unwrap_err();
    assert_eq!(err.code.as_str(), "Q003");
    let at = err.position.unwrap();
    assert_eq!((at.line, at.column), (1, 21));
}
