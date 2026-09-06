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
        Clause::Negation(inner) => match &inner[0] {
            Clause::Traversal(t) => assert!(t.undirected, "undirected inside not{{}}"),
            c => panic!("expected Traversal in not, got {c:?}"),
        },
        c => panic!("expected Negation, got {c:?}"),
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
        Clause::Negation(clauses) => {
            assert_eq!(clauses.len(), 1);
            match &clauses[0] {
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
        _ => panic!("expected Negation"),
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
    let QueryFile::Queries(queries) = parse_query(input).unwrap() else {
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
            assert_eq!(f.op, CompOp::Ne);
        }
        _ => panic!("expected Filter"),
    }
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
        Clause::Filter(f) => match &f.right {
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
        Clause::Filter(f) => match &f.right {
            Expr::Literal(Literal::Bool(value)) => assert!(*value),
            other => panic!("expected bool literal, got {:?}", other),
        },
        _ => panic!("expected Filter"),
    }
    match &q.match_clause[2] {
        Clause::Filter(f) => match &f.right {
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
            assert_eq!(f.op, CompOp::Contains);
            assert!(matches!(
                &f.left,
                Expr::PropAccess { variable, property } if variable == "p" && property == "tags"
            ));
            assert!(matches!(&f.right, Expr::Variable(v) if v == "tag"));
        }
        _ => panic!("expected Filter"),
    }
}

#[test]
fn test_parse_contains_is_rejected_in_mutation_predicate() {
    let input = r#"
query drop_person($tag: String) {
delete Person where tags contains $tag
}
"#;
    assert!(parse_query(input).is_err());
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
            assert_eq!(f.op, CompOp::StartsWith);
            assert!(matches!(
                &f.left,
                Expr::PropAccess { variable, property } if variable == "p" && property == "name"
            ));
            assert!(matches!(&f.right, Expr::Variable(v) if v == "q"));
        }
        _ => panic!("expected Filter"),
    }
}

#[test]
fn test_parse_starts_with_is_rejected_in_mutation_predicate() {
    let input = r#"
query drop_person($q: String) {
delete Person where name starts_with $q
}
"#;
    assert!(parse_query(input).is_err());
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
            assert_eq!(upd.predicate.property, "name");
            assert_eq!(upd.predicate.op, CompOp::Eq);
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
            assert_eq!(del.predicate.property, "name");
            assert_eq!(del.predicate.op, CompOp::Eq);
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
        Clause::Filter(f) => match &f.right {
            Expr::Literal(Literal::Date(v)) => assert_eq!(v, "2026-02-14"),
            other => panic!("expected date literal, got {:?}", other),
        },
        _ => panic!("expected Filter"),
    }
    match &q.match_clause[2] {
        Clause::Filter(f) => match &f.right {
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
        Clause::Filter(f) => assert!(matches!(f.right, Expr::Now)),
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
            assert!(matches!(update.assignments[0].value, MatchValue::Now));
            assert!(matches!(update.predicate.value, MatchValue::Now));
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
            MatchValue::Literal(Literal::List(items)) => {
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
        Clause::Filter(Filter { left, op, right }) => {
            assert_eq!(*op, CompOp::Eq);
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
        Clause::Filter(Filter { left, op, right }) => {
            assert_eq!(*op, CompOp::Eq);
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
        Clause::Filter(Filter { left, op, right }) => {
            assert_eq!(*op, CompOp::Eq);
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
    assert!(err.span.is_some());
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
    match parse_query(input).unwrap() {
        QueryFile::Branch(stmt) => stmt,
        QueryFile::Queries(queries) => panic!(
            "expected a branch statement, got {} declarations",
            queries.len()
        ),
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
