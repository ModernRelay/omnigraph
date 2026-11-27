//! Each acceptance check refuses the plan it exists for. Rust and not
//! `.gqt`: a case only reaches plans the planner builds, and the planner
//! builds none of these; each test plans a query, breaks one thing its query
//! requires, and asserts the check that names it.

use std::sync::Arc;

use crate::read_shape::ReadShape;
use omnigraph_compiler::catalog::{Catalog, build_catalog};
use omnigraph_compiler::ir::{IRExpr, ParamMap};
use omnigraph_compiler::query::value::Literal;
use omnigraph_compiler::schema::parser::parse_schema;

use super::*;
use crate::fixture_bounds::BOUNDS;
use crate::logical::{ColumnRef, IDENTITY_MEMBER};
use crate::operation::TableRef;
use crate::physical::{PhysicalNode, PhysicalPlan};
use crate::source::{MemorySource, NodeTypeSpec};

const SCHEMA: &str = r#"
node Doc {
    slug: String @key
    title: String @index
    year: I64
    open: Bool
    rank: I32?
    score: F64
    born: Date
    tags: [String]
    embedding: Vector(2)
}
edge Cites: Doc -> Doc
"#;

/// The canonical id of a `Doc` by its String key, as a stand-in for the
/// loader's derivation: `id:<value>`.
struct Keys;

impl KeyIds for Keys {
    fn canonical_key_id(
        &self,
        type_key: &str,
        value: &Literal,
        _ty: &omnigraph_compiler::types::ExprType,
    ) -> Option<String> {
        match (type_key, value) {
            ("node:Doc", Literal::String(key)) => Some(format!("id:{key}")),
            _ => None,
        }
    }
}

struct Fixture {
    catalog: Catalog,
    ir: ReadShape,
    /// The checked core read, which the placement check derives from.
    core: omnigraph_compiler::ir::core::CoreRead,
    params: ParamMap,
    source: MemorySource,
}

impl Fixture {
    fn new(query: &str, params: &[(&str, Literal)]) -> Self {
        Self::with_schema(SCHEMA, query, params)
    }

    fn with_schema(schema: &str, query: &str, params: &[(&str, Literal)]) -> Self {
        let catalog = build_catalog(&parse_schema(schema).unwrap()).unwrap();
        let ir = ReadShape::of_source(&catalog, query, "q").unwrap();
        let decl = omnigraph_compiler::find_named_query(query, "q").unwrap();
        let core =
            match omnigraph_compiler::query::check::check_query_decl(&catalog, &decl).unwrap() {
                omnigraph_compiler::ir::core::CoreDecl::Read(read) => *read,
                omnigraph_compiler::ir::core::CoreDecl::Write(_) => panic!("`q` is a read"),
            };
        let mut source = MemorySource::default().with_edge_version("Cites", 1);
        for (name, node_type) in &catalog.node_types {
            source = source.with_node_type(
                name,
                NodeTypeSpec {
                    table: TableRef::dataset(format!("node:{name}"), format!("node/{name}"), None),
                    version: Some(1),
                    columns: catalog.system_columns,
                    schema: Arc::clone(&node_type.arrow_schema),
                    key: node_type.key.clone().unwrap_or_default(),
                    object_columns: node_type
                        .arrow_schema
                        .fields()
                        .iter()
                        .map(|field| field.name().clone())
                        .collect(),
                    object_fields: node_type.node_object_fields().cloned().collect(),
                    row_count: None,
                },
            );
        }
        Self {
            catalog,
            ir,
            core,
            params: params
                .iter()
                .map(|(name, value)| (name.to_string(), value.clone()))
                .collect(),
            source,
        }
    }

    fn traced(&self) -> crate::gate::Traced {
        crate::gate::plan_traced(&self.ir, &self.source, &BOUNDS, false).expect("the query plans")
    }

    fn plan(&self) -> PhysicalPlan {
        self.traced().optimized.physical
    }

    fn derivation(&self) -> Option<Derivation> {
        self.traced().derivation()
    }

    fn accept(&self, plan: PhysicalPlan) -> Result<AcceptedPlan, ValidationError> {
        self.accept_with(plan, self.derivation())
    }

    fn input(&self) -> AcceptInput<'_> {
        AcceptInput {
            core: Some(&self.core),
            catalog: &self.catalog,
            ir: &self.ir,
            params: &self.params,
            keys: &Keys,
            limits: ValidationLimits::DEFAULT,
        }
    }

    fn accept_with(
        &self,
        plan: PhysicalPlan,
        derivation: Option<Derivation>,
    ) -> Result<AcceptedPlan, ValidationError> {
        let input = AcceptInput {
            core: Some(&self.core),
            catalog: &self.catalog,
            ir: &self.ir,
            params: &self.params,
            keys: &Keys,
            limits: ValidationLimits::DEFAULT,
        };
        accept(plan, &input, derivation)
    }

    /// The check that refuses `plan`, which must be refused.
    fn refused(&self, plan: PhysicalPlan) -> (&'static str, String) {
        match self.accept(plan) {
            Err(ValidationError::Violated { check, detail }) => (check, detail),
            other => panic!("the broken plan was not refused by a check: {other:?}"),
        }
    }
}

fn node_ids(plan: &PhysicalPlan, want: impl Fn(&PhysicalNode) -> bool) -> Vec<usize> {
    plan.live()
        .filter(|(_, node)| want(node))
        .map(|(id, _)| id)
        .collect()
}

const FILTERED: &str = r#"query q($min: I64) {
    match { $d: Doc } where $d.open = true and $d.year >= $min
    return { $d.slug, $d.year as year }
    order { $d.year desc }
    limit 2
}"#;

const RANKED: &str = r#"query q($v: Vector(2)) {
    match { let $pop = { match { $d: Doc } where $d.year > 2000 return { $d } }, from nearest($pop, $d.embedding, $v, k: 3) as $score }
    return { $d.slug, $score as score }
    order { $score, $d.year }
    limit 3
}"#;

fn filtered() -> Fixture {
    Fixture::new(FILTERED, &[("min", Literal::Integer(2001))])
}

fn ranked() -> Fixture {
    Fixture::new(
        RANKED,
        &[(
            "v",
            Literal::List(vec![Literal::Float(0.0), Literal::Float(1.0)]),
        )],
    )
}

const DECLARED_SCHEMA: &str = r#"
node Doc {
    slug: String @key
    body: String @index(fulltext, analyzer = "standard_v1")
    title: String @index(fulltext, analyzer = "standard_v1")
    year: I64
}
"#;

const EXACT_BM25: &str = r#"query q($q: String) {
    match { let $pop = { match { $d: Doc } where $d.year > 2000 return { $d } }, from bm25($pop, $d.body, terms($q, mode: all, max_edits: 1)) as $score }
    return { $d.slug, $score as score }
    order { $score desc }
}"#;

fn exact_bm25() -> Fixture {
    Fixture::with_schema(
        DECLARED_SCHEMA,
        EXACT_BM25,
        &[("q", Literal::String("graph".into()))],
    )
}

/// The one exact `bm25` `Score` of `plan`.
fn exact_score(plan: &mut PhysicalPlan) -> &mut PhysicalNode {
    let [id] = node_ids(plan, |node| matches!(node, PhysicalNode::Score { .. }))[..] else {
        panic!("the exact bm25 plans one Score");
    };
    plan.node_mut(id).unwrap()
}

/// An exact `bm25` plans a `Score` of kind `bm25` that retains its
/// `match_terms` predicate; a plan scoring another kind, another predicate
/// or another field is refused (search identity), and so is one whose
/// retained predicate is not its field's `match_terms` (predicate
/// retention), one scoring a field that does not rank (scoring corpus) and a read of an `F64` `_score` no exact `bm25` produces (score
/// origin).
#[test]
fn an_exact_bm25_is_one_score_retaining_its_predicate_over_its_field_corpus() {
    use omnigraph_compiler::ir::core::ScoreKind;
    let fixture = exact_bm25();
    let plan = fixture.plan();
    assert!(
        node_ids(&plan, |node| matches!(
            node,
            PhysicalNode::Scan {
                ranked: Some(_),
                ..
            }
        ))
        .is_empty(),
        "an exact bm25 ranks no scan"
    );
    fixture
        .accept(plan.clone())
        .expect("the planned plan is accepted");

    let mut distance = plan.clone();
    if let PhysicalNode::Score { kind, .. } = exact_score(&mut distance) {
        *kind = ScoreKind::Distance;
    }
    let (check, detail) = fixture.refused(distance);
    assert_eq!(check, "search identity", "{detail}");

    let mut no_edits = plan.clone();
    if let PhysicalNode::Score { query, .. } = exact_score(&mut no_edits)
        && let IRExpr::Call { args, .. } = query
    {
        args[2] = IRExpr::Literal(Literal::Integer(0), args[2].ty().clone());
    }
    let (check, detail) = fixture.refused(no_edits);
    assert_eq!(check, "search identity", "{detail}");

    let mut other_field = plan.clone();
    if let PhysicalNode::Score { property, .. } = exact_score(&mut other_field) {
        *property = "title".to_string();
    }
    let (check, detail) = fixture.refused(other_field);
    assert_eq!(check, "search identity", "{detail}");

    // A shape whose score keeps a constant instead of the field's
    // `match_terms` is refused even where the query's own shape says so.
    let mut dropped = exact_bm25();
    let constant = IRExpr::Literal(
        Literal::Bool(true),
        omnigraph_compiler::types::ExprType::from_prop(
            &omnigraph_compiler::types::PropType::scalar(
                omnigraph_compiler::types::ScalarType::Bool,
                false,
            ),
        ),
    );
    for step in &mut dropped.ir.pipeline {
        if let crate::read_shape::PatternStep::Score { query, .. } = step {
            *query = constant.clone();
        }
    }
    let mut dropped_plan = plan.clone();
    if let PhysicalNode::Score { query, .. } = exact_score(&mut dropped_plan) {
        *query = constant;
    }
    let (check, detail) = dropped.refused(dropped_plan);
    assert_eq!(check, "predicate retention", "{detail}");

    let mut undeclared = exact_bm25();
    undeclared.catalog = build_catalog(
        &parse_schema(&DECLARED_SCHEMA.replace(
            r#"body: String @index(fulltext, analyzer = "standard_v1")"#,
            r#"body: String @index(fulltext, scorer = "none")"#,
        ))
        .unwrap(),
    )
    .unwrap();
    let (check, detail) = undeclared.refused(plan.clone());
    assert_eq!(check, "scoring corpus", "{detail}");

    // The score moved to another binding, predicate and all: the read's
    // `$d._score` is then no exact `bm25`'s.
    let elsewhere = |binding: &mut String, query: &mut IRExpr| {
        *binding = "e".to_string();
        if let IRExpr::Call { args, .. } = query
            && let IRExpr::PropAccess { variable, .. } = &mut args[0]
        {
            *variable = "e".to_string();
        }
    };
    let mut stray = exact_bm25();
    for step in &mut stray.ir.pipeline {
        if let crate::read_shape::PatternStep::Score { binding, query, .. } = step {
            elsewhere(binding, query);
        }
    }
    let mut stray_plan = plan;
    if let PhysicalNode::Score { binding, query, .. } = exact_score(&mut stray_plan) {
        elsewhere(binding, query);
    }
    let (check, detail) = stray.refused(stray_plan);
    assert_eq!(check, "score origin", "{detail}");
}

#[test]
fn the_planned_plans_are_accepted() {
    let fixture = filtered();
    let accepted = fixture
        .accept(fixture.plan())
        .expect("a planned plan is accepted");
    assert_eq!(accepted.scope(), ValidationScope::ExactSubset);
    assert!(accepted.evidence().derivation().is_some());
    for fixture in [ranked(), exact_bm25()] {
        let accepted = fixture
            .accept(fixture.plan())
            .expect("a planned plan is accepted");
        assert_eq!(accepted.scope(), ValidationScope::InvariantsOnly);
    }
    let outside = Fixture::new(
        r#"query q() {
    match { $d: Doc } where contains($d.title, "graph")
    return { $d.slug }
}"#,
        &[],
    );
    let accepted = outside
        .accept(outside.plan())
        .expect("a planned plan is accepted");
    assert_eq!(accepted.scope(), ValidationScope::InvariantsOnly);
    assert!(accepted.evidence().derivation().is_none());
}

/// The rules each fixture's derivation applies, in order.
fn rules(derivation: &Derivation) -> Vec<&'static str> {
    derivation
        .steps
        .iter()
        .map(|step| match step.rule {
            Rule::AbsorbScanFilter { .. } => "absorb",
            Rule::PruneScanColumns { .. } => "prune",
            Rule::Lower => "lower",
            Rule::HydrateReturnColumns { .. } => "hydrate",
            Rule::NarrowKey { .. } => "narrow",
        })
        .collect()
}

#[test]
fn the_derivations_record_every_rule() {
    assert_eq!(
        rules(&filtered().derivation().unwrap()),
        [
            "absorb", "absorb", "prune", "lower", "lower", "lower", "lower"
        ]
    );
}

#[test]
fn a_member_without_its_derivation_is_refused() {
    let fixture = filtered();
    let (check, detail) = match fixture.accept_with(fixture.plan(), None) {
        Err(ValidationError::Violated { check, detail }) => (check, detail),
        other => panic!("{other:?}"),
    };
    assert_eq!(check, "exact subset", "{detail}");
}

#[test]
fn a_dropped_step_fails_the_reconstruction() {
    let fixture = filtered();
    let mut derivation = fixture.derivation().unwrap();
    derivation.steps.remove(0);
    let error = fixture
        .accept_with(fixture.plan(), Some(derivation))
        .unwrap_err();
    assert!(
        matches!(
            &error,
            ValidationError::Violated {
                check: "exact subset",
                ..
            }
        ),
        "{error:?}"
    );
}

#[test]
fn a_scan_pruned_below_its_readers_is_refused() {
    let fixture = filtered();
    let plan = fixture.plan();
    let mut derivation = fixture.derivation().unwrap();
    for step in &mut derivation.steps {
        if let Rule::PruneScanColumns { columns } = &mut step.rule {
            columns.retain(|column| column != "year");
        }
    }
    match fixture.accept_with(plan, Some(derivation)) {
        Err(ValidationError::Violated { check, detail }) => {
            assert_eq!(check, "exact subset");
            assert!(detail.contains("drops `year`"), "{detail}");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn a_dropped_conjunct_fails_predicate_retention() {
    let fixture = filtered();
    let mut plan = fixture.plan();
    for id in node_ids(&plan, |node| matches!(node, PhysicalNode::Scan { .. })) {
        if let Some(PhysicalNode::Scan { spec, .. }) = plan.node_mut(id) {
            spec.filter = None;
        }
    }
    for id in node_ids(&plan, |node| matches!(node, PhysicalNode::Filter { .. })) {
        if let Some(PhysicalNode::Filter { filters, .. }) = plan.node_mut(id) {
            filters.clear();
        }
    }
    let (check, detail) = fixture.refused(plan);
    assert_eq!(check, "predicate retention", "{detail}");
}

/// An `optional` runs as one `OptionalJoin` over its correlation binding,
/// testing its own conjuncts in its inner tree: a join over another binding,
/// or a plan that tests the optional's conjunct after it, is refused.
#[test]
fn an_optional_tests_its_conjuncts_inside_its_inner_tree() {
    let fixture = Fixture::new(
        r#"query q() {
    match { $d: Doc, optional { $e: Doc, $d cites $e where $e.year > 2000 } }
    return { $d.slug, $e.slug }
}"#,
        &[],
    );
    let plan = fixture.plan();
    fixture.accept(plan.clone()).expect("the planned optional");
    let [join] = node_ids(&plan, |node| {
        matches!(node, PhysicalNode::OptionalJoin { .. })
    })[..] else {
        panic!("one optional join");
    };
    // The join correlated on another binding.
    let mut relabelled = plan.clone();
    if let Some(PhysicalNode::OptionalJoin { outer_var, .. }) = relabelled.node_mut(join) {
        *outer_var = "e".to_string();
    }
    let (check, detail) = fixture.refused(relabelled);
    assert_eq!(check, "predicate retention", "{detail}");
    // The optional's conjunct also tested above the join.
    let mut leaked = plan;
    let conjunct = fixture.ir.pipeline.iter().find_map(|step| match step {
        crate::PatternStep::Optional { inner, .. } => inner.iter().find_map(|step| match step {
            crate::PatternStep::Filter { pred: filter, .. } => Some(filter.clone()),
            _ => None,
        }),
        _ => None,
    });
    let conjunct = conjunct.expect("the optional's conjunct");
    let projection = node_ids(&leaked, |node| {
        matches!(node, PhysicalNode::Projection { .. })
    })[0];
    let Some(PhysicalNode::Projection { input, .. }) = leaked.node(projection).cloned() else {
        unreachable!()
    };
    let filter = leaked.add(PhysicalNode::Filter {
        input,
        filters: vec![conjunct],
    });
    if let Some(PhysicalNode::Projection { input, .. }) = leaked.node_mut(projection) {
        *input = filter;
    }
    let (check, detail) = fixture.refused(leaked);
    assert_eq!(check, "predicate origin", "{detail}");
}

/// Two `optional`s on one binding, one testing a conjunct and one testing
/// none, are matched to the plan's two joins whatever order either lists
/// them in: the planned plan is accepted.
#[test]
fn two_optionals_on_one_binding_match_their_own_joins() {
    for query in [
        r#"query q() {
    match { $d: Doc, optional { $e: Doc, $d cites $e }, optional { $f: Doc, $d cites $f where $f.year > 2000 } }
    return { $d.slug, $e.slug, $f.slug }
}"#,
        r#"query q() {
    match { $d: Doc, optional { $f: Doc, $d cites $f where $f.year > 2000 }, optional { $e: Doc, $d cites $e } }
    return { $d.slug, $e.slug, $f.slug }
}"#,
    ] {
        let fixture = Fixture::new(query, &[]);
        fixture
            .accept(fixture.plan())
            .unwrap_or_else(|error| panic!("{query}: {error:?}"));
    }
}

/// An inline read with its own cut runs as one `Correlate` returning what
/// the read returns, testing its own conjuncts in its inner tree: a read
/// with another cut, or a plan that tests the read's conjunct after it,
/// where the cut would already have run, is refused.
#[test]
fn an_inline_read_with_its_own_cut_tests_its_conjuncts_under_the_cut() {
    let fixture = Fixture::new(
        r#"query q() {
    match { $d: Doc, from { match { $e: Doc, $d cites $e } where $e.year > 2000 return { $e } order { $e.year desc } limit 1 } }
    return { $d.slug, $e.slug }
}"#,
        &[],
    );
    let plan = fixture.plan();
    fixture
        .accept(plan.clone())
        .expect("the planned correlated read");
    let [correlate] = node_ids(&plan, |node| matches!(node, PhysicalNode::Correlate { .. }))[..]
    else {
        panic!("one correlated read");
    };
    // The read cut to another count.
    let mut recut = plan.clone();
    if let Some(PhysicalNode::Correlate { read, .. }) = recut.node_mut(correlate) {
        read.limit = Some(2);
    }
    let (check, detail) = fixture.refused(recut);
    assert_eq!(check, "predicate retention", "{detail}");
    // The read's conjunct also tested above it.
    let mut leaked = plan;
    let conjunct = fixture.ir.pipeline.iter().find_map(|step| match step {
        crate::PatternStep::Correlate { inner, .. } => inner.iter().find_map(|step| match step {
            crate::PatternStep::Filter { pred: filter, .. } => Some(filter.clone()),
            _ => None,
        }),
        _ => None,
    });
    let conjunct = conjunct.expect("the read's conjunct");
    let projection = node_ids(&leaked, |node| {
        matches!(node, PhysicalNode::Projection { .. })
    })[0];
    let Some(PhysicalNode::Projection { input, .. }) = leaked.node(projection).cloned() else {
        unreachable!()
    };
    let filter = leaked.add(PhysicalNode::Filter {
        input,
        filters: vec![conjunct],
    });
    if let Some(PhysicalNode::Projection { input, .. }) = leaked.node_mut(projection) {
        *input = filter;
    }
    let (check, detail) = fixture.refused(leaked);
    assert_eq!(check, "predicate origin", "{detail}");
}

/// The strategy of every block of `plan` set to `strategy`.
fn relabel_blocks(plan: &mut PhysicalPlan, strategy: crate::AntiJoinStrategy) {
    for id in node_ids(plan, |node| matches!(node, PhysicalNode::AntiJoin { .. })) {
        if let Some(PhysicalNode::AntiJoin {
            strategy: declared, ..
        }) = plan.node_mut(id)
        {
            *declared = strategy;
        }
    }
}

#[test]
fn a_strategy_its_inner_tree_lacks_fails_block_strategy() {
    use crate::AntiJoinStrategy::{Correlated, KeySet};
    use crate::source::PlanSource;
    use omnigraph_compiler::types::Direction;
    let mut fixture = Fixture::new(
        "query q($s: String) { match { $d: Doc } where not exists { $d cites $e, $e: Doc where $e.slug = $s } return { $d.slug } }",
        &[("s", Literal::String("x".to_string()))],
    );
    let strategies = |plan: &PhysicalPlan| -> Vec<crate::AntiJoinStrategy> {
        plan.live()
            .filter_map(|(_, node)| match node {
                PhysicalNode::AntiJoin { strategy, .. } => Some(*strategy),
                _ => None,
            })
            .collect()
    };
    // No row counts: the block is correlated, its inner tree over the outer rows.
    let correlated = fixture.plan();
    assert_eq!(strategies(&correlated), [Correlated]);
    fixture
        .accept(correlated.clone())
        .expect("the correlated plan is accepted");
    let mut relabelled = correlated;
    relabel_blocks(&mut relabelled, KeySet);
    let (check, detail) = fixture.refused(relabelled);
    assert_eq!(check, "block strategy", "{detail}");

    // With row counts and edge statistics the bound `$e` makes the key set cheaper.
    let mut doc = fixture.source.node_type("Doc").unwrap();
    doc.row_count = Some(100);
    let statistics = crate::ExpandStatistics {
        edge_count: 1_000,
        src_node_count: 100,
        dst_node_count: 100,
        max_frontier_cap: 1024,
        max_hops_cap: 6,
    };
    fixture.source = fixture
        .source
        .clone()
        .with_node_type("Doc", doc)
        .with_expand_statistics("Cites", Direction::Out, statistics)
        .with_expand_statistics("Cites", Direction::In, statistics);
    let key_set = fixture.plan();
    assert_eq!(strategies(&key_set), [KeySet]);
    fixture
        .accept(key_set.clone())
        .expect("the key-set plan is accepted");
    let mut relabelled = key_set;
    relabel_blocks(&mut relabelled, Correlated);
    let (check, detail) = fixture.refused(relabelled);
    assert_eq!(check, "block strategy", "{detail}");
}

fn value_type(
    scalar: omnigraph_compiler::types::ScalarType,
) -> omnigraph_compiler::types::ExprType {
    omnigraph_compiler::types::ExprType::Value {
        scalar,
        list: false,
        nullable: false,
    }
}

/// `conjunct` appended to the filter of every scan of `plan`.
fn place_on_scans(plan: &mut PhysicalPlan, conjunct: &IRExpr) {
    for id in node_ids(plan, |node| matches!(node, PhysicalNode::Scan { .. })) {
        if let Some(PhysicalNode::Scan { spec, .. }) = plan.node_mut(id) {
            let added = crate::optimizer::gq_conjunct(conjunct);
            spec.filter = Some(match spec.filter.take() {
                Some(filter) => filter.and(added),
                None => added,
            });
        }
    }
}

#[test]
fn an_unwritten_conjunct_fails_predicate_origin() {
    let fixture = filtered();
    let mut plan = fixture.plan();
    place_on_scans(
        &mut plan,
        &IRExpr::Literal(
            Literal::Bool(false),
            value_type(omnigraph_compiler::types::ScalarType::Bool),
        ),
    );
    let (check, detail) = fixture.refused(plan);
    assert_eq!(check, "predicate origin", "{detail}");
}

const KEYED: &str = r#"query q($s: String) {
    match { $d: Doc } where $d.slug = $s
    return { $d.title }
}"#;

/// The identity equality a key narrowing adds, with `id`.
fn identity(catalog: &Catalog, id: &str) -> IRExpr {
    let ty = value_type(omnigraph_compiler::types::ScalarType::String);
    IRExpr::comparison(
        IRExpr::PropAccess {
            variable: "d".to_string(),
            property: catalog.system_columns.id.to_string(),
            ty: ty.clone(),
        },
        omnigraph_compiler::query::value::CompOp::Eq,
        IRExpr::Literal(Literal::String(id.to_string()), ty),
    )
}

/// A key narrowing (`scan_access::narrow_key`) is a recorded rule of the
/// exact fragment and a predicate the invariants re-derive: the planned
/// narrowing is accepted, a forged id or a narrowing with no written key
/// equality is refused.
#[test]
fn a_key_narrowing_is_accepted_only_with_the_derived_id() {
    let fixture = Fixture::new(KEYED, &[("s", Literal::String("a".into()))]);
    let id = fixture.catalog.system_columns.id.to_string();
    let source = fixture
        .source
        .clone()
        .with_canonical_key_ids(fixture.params.clone())
        .with_index_facts(
            "node:Doc",
            vec![crate::source::IndexFact {
                name: "id_idx".into(),
                column: id,
                kind: crate::source::IndexKind::Btree { usable: true },
                coverage: Some(crate::source::FragmentCoverage {
                    covered: 1,
                    total: 1,
                }),
            }],
        );
    let accepted = futures::executor::block_on(crate::gate::accept_query(
        &fixture.input(),
        &source,
        &BOUNDS,
    ))
    .expect("the narrowed plan is accepted");
    assert_eq!(accepted.scope(), ValidationScope::ExactSubset);
    let derivation = accepted.evidence().derivation().cloned().unwrap();
    assert!(rules(&derivation).contains(&"narrow"), "{derivation:?}");
    let narrowed = identity(&fixture.catalog, "id:a");
    let placed = node_ids(accepted.plan(), |node| {
        matches!(node, PhysicalNode::Scan { .. })
    })
    .into_iter()
    .any(|id| match accepted.plan().node(id) {
        Some(PhysicalNode::Scan { spec, .. }) => spec
            .filter
            .as_ref()
            .is_some_and(|filter| filter.gq_filters().contains(&narrowed)),
        _ => false,
    });
    assert!(placed, "the plan tests the narrowed identity");

    let mut forged = accepted.plan().clone();
    for id in node_ids(&forged, |node| matches!(node, PhysicalNode::Scan { .. })) {
        if let Some(PhysicalNode::Scan { spec, .. }) = forged.node_mut(id) {
            let conjuncts: Vec<IRExpr> = spec
                .filter
                .as_ref()
                .map(|filter| filter.gq_filters())
                .unwrap_or_default();
            let mut rebuilt: Option<crate::logical::Predicate> = None;
            for conjunct in conjuncts {
                let conjunct = if conjunct == narrowed {
                    identity(&fixture.catalog, "id:b")
                } else {
                    conjunct
                };
                let added = crate::optimizer::gq_conjunct(&conjunct);
                rebuilt = Some(match rebuilt {
                    Some(filter) => filter.and(added),
                    None => added,
                });
            }
            spec.filter = rebuilt;
        }
    }
    match fixture.accept_with(forged, Some(derivation)) {
        Err(ValidationError::Violated { check, detail }) => {
            assert_eq!(check, "predicate origin", "{detail}");
        }
        other => panic!("a forged id was not refused: {other:?}"),
    }

    let keyless = Fixture::new("query q() { match { $d: Doc } return { $d.title } }", &[]);
    let mut plan = keyless.plan();
    place_on_scans(&mut plan, &identity(&keyless.catalog, "id:a"));
    let (check, detail) = keyless.refused(plan);
    assert_eq!(check, "predicate origin", "{detail}");
}

#[test]
fn edited_hop_bounds_fail_traversal_identity() {
    let fixture = Fixture::new(
        "query q() { match { $d: Doc, $d cites{1,2} $e } return { $d.slug, $e.slug } }",
        &[],
    );
    let plan = fixture.plan();
    fixture
        .accept(plan.clone())
        .expect("the planned traversal is accepted");
    let mut plan = plan;
    for id in node_ids(&plan, |node| matches!(node, PhysicalNode::Expand { .. })) {
        if let Some(PhysicalNode::Expand { max_hops, .. }) = plan.node_mut(id) {
            *max_hops = Some(1);
        }
    }
    let (check, detail) = fixture.refused(plan);
    assert_eq!(check, "traversal identity", "{detail}");
}

/// The first key of the final sort, removed: the last sort lowered, above
/// every sort a fusion arm or a window holds.
fn drop_first_sort_key(plan: &mut PhysicalPlan) {
    let sorts = node_ids(plan, |node| matches!(node, PhysicalNode::Sort { .. }));
    if let Some(id) = sorts.into_iter().max()
        && let Some(PhysicalNode::Sort { order_by, .. }) = plan.node_mut(id)
    {
        order_by.remove(0);
    }
}

/// A fused search sorts by `_rrf` descending then its written keys; a
/// search-led aggregate by its written keys then every group key.
#[test]
fn fused_and_grouped_searches_sort_as_the_query_requires() {
    let vectors = [
        (
            "v",
            Literal::List(vec![Literal::Float(0.0), Literal::Float(1.0)]),
        ),
        (
            "w",
            Literal::List(vec![Literal::Float(1.0), Literal::Float(0.0)]),
        ),
    ];
    let fused = Fixture::new(
        "query q($v: Vector(2), $w: Vector(2)) { match { let $all = { match { $d: Doc } return { $d } }, from rrf(nearest($all, $d.embedding, $v, k: 3), nearest($all, $d.embedding, $w, k: 3), k: 60) as $score, $d: Doc } return { $d.slug } order { $score desc, $d.slug desc } limit 2 }",
        &vectors,
    );
    let grouped = Fixture::new(
        "query q($v: Vector(2)) { match { let $pop = { match { $d: Doc } return { $d } }, from nearest($pop, $d.embedding, $v, k: 3) as $score } return { $d.open, count($d) as n } order { n desc } }",
        &vectors[..1],
    );
    for fixture in [fused, grouped] {
        let plan = fixture.plan();
        fixture
            .accept(plan.clone())
            .expect("the planned order is accepted");
        let mut plan = plan;
        drop_first_sort_key(&mut plan);
        let (check, detail) = fixture.refused(plan);
        assert_eq!(check, "order", "{detail}");
    }
}

/// A member whose bare return columns are hydrated by row address keeps its
/// exact subset through the recorded rule; a hydration that places a column
/// elsewhere is refused.
#[test]
fn a_hydrated_member_keeps_its_exact_subset() {
    let fixture = Fixture::new(
        "query q() { match { $d: Doc } return { $d.slug, $d.title } order { $d.year } limit 2 }",
        &[],
    );
    let plan = fixture.plan();
    assert!(
        matches!(
            plan.node(plan.root()),
            Some(PhysicalNode::HydrateColumns { .. })
        ),
        "the fixture hydrates its title"
    );
    let accepted = fixture
        .accept(plan.clone())
        .expect("a hydrated plan is accepted");
    assert_eq!(accepted.scope(), ValidationScope::ExactSubset);
    assert!(rules(accepted.evidence().derivation().unwrap()).contains(&"hydrate"));
    let mut plan = plan;
    let root = plan.root();
    if let Some(PhysicalNode::HydrateColumns { bindings, .. }) = plan.node_mut(root) {
        bindings[0].columns[0].position = 0;
    }
    let (check, detail) = fixture.refused(plan);
    assert_eq!(check, "hydration", "{detail}");
}

#[test]
fn a_scan_of_another_binding_fails_binding_identity() {
    let fixture = filtered();
    let mut plan = fixture.plan();
    for id in node_ids(&plan, |node| matches!(node, PhysicalNode::Scan { .. })) {
        if let Some(PhysicalNode::Scan { spec, .. }) = plan.node_mut(id) {
            spec.binding = Some("e".to_string());
        }
    }
    let (check, _) = fixture.refused(plan);
    assert_eq!(check, "binding identity");
}

#[test]
fn another_query_argument_fails_search_identity() {
    let fixture = ranked();
    let mut plan = fixture.plan();
    for id in node_ids(&plan, |node| node.ranked().is_some()) {
        if let Some(PhysicalNode::Scan {
            ranked: Some(ranked),
            ..
        }) = plan.node_mut(id)
        {
            ranked.query = IRExpr::Literal(
                Literal::List(vec![Literal::Float(1.0), Literal::Float(1.0)]),
                ranked.query.ty().clone(),
            );
        }
    }
    let (check, detail) = fixture.refused(plan);
    assert_eq!(check, "search identity", "{detail}");
}

fn fusion_arms(plan: &mut PhysicalPlan) -> &mut Vec<crate::physical::FuseArm> {
    let id = node_ids(plan, |node| matches!(node, PhysicalNode::Fuse { .. }))[0];
    match plan.node_mut(id) {
        Some(PhysicalNode::Fuse { arms, .. }) => arms,
        _ => unreachable!("a Fuse"),
    }
}

fn fusion_weights(plan: &mut PhysicalPlan) -> &mut Option<IRExpr> {
    let id = node_ids(plan, |node| matches!(node, PhysicalNode::Fuse { .. }))[0];
    match plan.node_mut(id) {
        Some(PhysicalNode::Fuse { weights, .. }) => weights,
        _ => unreachable!("a Fuse"),
    }
}

/// A fusion runs the weights the query writes: dropped, reordered or added
/// weights change every fused score, so each is refused.
#[test]
fn other_fusion_weights_fail_search_identity() {
    let fused = |weights: &str| {
        format!(
            "query q($v: Vector(2), $w: Vector(2)) {{ match {{ let $all = {{ match {{ $d: Doc }} return {{ $d }} }}, from rrf(nearest($all, $d.embedding, $v, k: 3), nearest($all, $d.embedding, $w, k: 3), k: 60{weights}) as $score, $d: Doc }} return {{ $d.slug }} order {{ $score desc }} limit 2 }}"
        )
    };
    let params = [
        (
            "v",
            Literal::List(vec![Literal::Float(0.0), Literal::Float(1.0)]),
        ),
        (
            "w",
            Literal::List(vec![Literal::Float(1.0), Literal::Float(0.0)]),
        ),
    ];
    let weighted = Fixture::new(&fused(", weights: [1.0, 5.0]"), &params);
    let mut plan = weighted.plan();
    let written = fusion_weights(&mut plan).clone();
    assert_eq!(
        written
            .as_ref()
            .and_then(|w| omnigraph_compiler::ir::rrf_weight_values(w, 2)),
        Some(vec![1.0, 5.0])
    );
    weighted.accept(plan.clone()).unwrap();
    for other in [None, omnigraph_compiler::ir::rrf_weights_expr(&[5.0, 1.0])] {
        let mut edited = plan.clone();
        *fusion_weights(&mut edited) = other;
        let (check, detail) = weighted.refused(edited);
        assert_eq!(check, "search identity", "{detail}");
        assert!(detail.contains("`weights`"), "{detail}");
    }
    let unweighted = Fixture::new(&fused(""), &params);
    let mut plan = unweighted.plan();
    assert!(fusion_weights(&mut plan).is_none());
    *fusion_weights(&mut plan) = written;
    let (check, detail) = unweighted.refused(plan);
    assert_eq!(check, "search identity", "{detail}");
}

/// A fusion's plan is the query's arm for arm (RFC 0047, search identity):
/// the planned three-arm fusion, a `nearest` window over a filtered
/// population beside an exact `distance` arm and an ordered read, is
/// accepted, and a saved plan whose arm ranks another way, whose arms are
/// reordered, whose fusion ranks another target, whose window arm lost its
/// population's filter or whose arm's rows no longer arrive in its rank
/// order is refused.
#[test]
fn an_altered_fusion_arm_is_refused() {
    let fixture = Fixture::new(
        "query q($v: Vector(2)) { match { let $recent = { match { $d: Doc } where $d.year > 2000 return { $d } }, let $all = { match { $d: Doc } return { $d } }, from rrf(nearest($recent, $d.embedding, $v, k: 2), distance($all, $d.embedding, [1.0, 0.0]), { match { $d: Doc } return { $d } order { $d.year desc } limit 3 }, weights: [2.0, 1.0, 0.5]) as $score } return { $d.slug } order { $score desc } limit 2 }",
        &[(
            "v",
            Literal::List(vec![Literal::Float(0.0), Literal::Float(1.0)]),
        )],
    );
    let planned = fixture.plan();
    fixture
        .accept(planned.clone())
        .expect("the planned fusion is the query's");
    let fuse = node_ids(&planned, |node| matches!(node, PhysicalNode::Fuse { .. }))[0];
    let edited = |edit: &dyn Fn(&mut PhysicalPlan)| {
        let mut plan = planned.clone();
        edit(&mut plan);
        fixture.refused(plan)
    };
    let (check, detail) = edited(&|plan| {
        fusion_arms(plan)[1].rank = crate::read_shape::ArmRank::Bm25;
    });
    assert_eq!(check, "search identity", "{detail}");
    let (check, detail) = edited(&|plan| fusion_arms(plan).swap(0, 2));
    assert_eq!(check, "search identity", "{detail}");
    let (check, detail) = edited(&|plan| {
        if let Some(PhysicalNode::Fuse { target, .. }) = plan.node_mut(fuse) {
            *target = "e".to_string();
        }
    });
    assert_eq!(check, "search identity", "{detail}");
    let (check, detail) = edited(&|plan| {
        for id in node_ids(plan, |node| matches!(node, PhysicalNode::Scan { .. })) {
            if let Some(PhysicalNode::Scan { spec, .. }) = plan.node_mut(id) {
                spec.filter = None;
            }
        }
        for id in node_ids(plan, |node| matches!(node, PhysicalNode::Filter { .. })) {
            if let Some(PhysicalNode::Filter { filters, .. }) = plan.node_mut(id) {
                filters.clear();
            }
        }
    });
    assert_eq!(check, "predicate retention", "{detail}");
    let (check, detail) = edited(&|plan| {
        let exact = fusion_arms(plan)[1].input;
        if let Some(PhysicalNode::Sort { order_by, .. }) = plan.node_mut(exact) {
            order_by[0].descending = true;
        }
    });
    assert_eq!(check, "search identity", "{detail}");
    assert!(detail.contains("rank order"), "{detail}");
}

/// A `nearest` window drawn as a pattern step is a cut of its `k` targets:
/// the planned plan is accepted, and a `TargetCut` keeping another number of
/// targets, or a window scan fetching another `k`, fails the row cut.
#[test]
fn a_window_cut_of_another_k_fails_the_row_cut() {
    let windowed = Fixture::new(
        "query q() { match { let $pop = { match { $d: Doc } return { $d } }, from nearest($pop, $d.embedding, [0.0, 1.0], k: 2) as $distance } where $d.year > 2000 return { $d.slug } }",
        &[],
    );
    windowed
        .accept(windowed.plan())
        .expect("the planned window is the cut the query requires");
    let mut plan = windowed.plan();
    for id in node_ids(&plan, |node| matches!(node, PhysicalNode::TargetCut { .. })) {
        if let Some(PhysicalNode::TargetCut { targets, .. }) = plan.node_mut(id) {
            *targets = 3;
        }
    }
    let (check, detail) = windowed.refused(plan);
    assert_eq!(check, "row cut", "{detail}");
    let mut plan = windowed.plan();
    for id in node_ids(&plan, |node| node.ranked().is_some()) {
        if let Some(PhysicalNode::Scan {
            ranked: Some(ranked),
            ..
        }) = plan.node_mut(id)
        {
            ranked.fetch = Some(3);
        }
    }
    let (check, detail) = windowed.refused(plan);
    assert_eq!(check, "row cut", "{detail}");
}

/// A conjunct belongs to the scope that writes it: a copy of a conjunct
/// tested in another scope, inside an `optional`'s inner tree, in another
/// arm of a `union` or below a `nearest` window's `TargetCut`, changes which
/// rows the scope keeps and is refused, although the conjunct is required
/// somewhere in the read.
#[test]
fn a_conjunct_tested_in_another_scope_is_refused() {
    let conjunct_of = |fixture: &Fixture, plan: &PhysicalPlan, written: &str| -> IRExpr {
        let _ = fixture;
        super::invariants::conjuncts(plan, &plan.live().map(|(id, _)| id).collect::<Vec<_>>())
            .into_iter()
            .map(|(_, conjunct)| conjunct)
            .find(|conjunct| conjunct.to_string().contains(written))
            .unwrap_or_else(|| panic!("the plan tests `{written}`"))
    };
    // A conjunct written after an `optional` copied into its inner tree.
    let optional = Fixture::new(
        r#"query q() {
    match { $d: Doc, optional { $e: Doc, $d cites $e } }
    where $d.year > 2000
    return { $d.slug, $e.slug }
}"#,
        &[],
    );
    let mut plan = optional.plan();
    optional.accept(plan.clone()).expect("the planned optional");
    let conjunct = conjunct_of(&optional, &plan, "year");
    let [join] = node_ids(&plan, |node| {
        matches!(node, PhysicalNode::OptionalJoin { .. })
    })[..] else {
        panic!("one optional join");
    };
    let Some(PhysicalNode::OptionalJoin { inner, .. }) = plan.node(join).cloned() else {
        unreachable!()
    };
    let filter = plan.add(PhysicalNode::Filter {
        input: inner,
        filters: vec![conjunct],
    });
    if let Some(PhysicalNode::OptionalJoin { inner, .. }) = plan.node_mut(join) {
        *inner = filter;
    }
    let (check, detail) = optional.refused(plan);
    assert_eq!(check, "predicate origin", "{detail}");
    // One arm's conjunct copied into the other arm of a `union`.
    let union = Fixture::new(
        r#"query q() {
    match { from union(
        { match { $d: Doc } where $d.year > 2000 return { $d } },
        { match { $d: Doc } where $d.open = true return { $d } }) }
    return { $d.slug }
}"#,
        &[],
    );
    let mut plan = union.plan();
    union.accept(plan.clone()).expect("the planned union");
    let conjunct = conjunct_of(&union, &plan, "year");
    let [fold] = node_ids(&plan, |node| matches!(node, PhysicalNode::Union { .. }))[..] else {
        panic!("one union");
    };
    let Some(PhysicalNode::Union { right, .. }) = plan.node(fold).cloned() else {
        unreachable!()
    };
    let filter = plan.add(PhysicalNode::Filter {
        input: right,
        filters: vec![conjunct],
    });
    if let Some(PhysicalNode::Union { right, .. }) = plan.node_mut(fold) {
        *right = filter;
    }
    let (check, detail) = union.refused(plan);
    assert_eq!(check, "predicate origin", "{detail}");
    // A conjunct written after a `nearest` window copied below its cut,
    // where it would choose the window's targets.
    let windowed = Fixture::new(
        "query q() { match { let $pop = { match { $d: Doc } return { $d } }, from nearest($pop, $d.embedding, [0.0, 1.0], k: 2) as $distance } where $d.year > 2000 return { $d.slug } }",
        &[],
    );
    let mut plan = windowed.plan();
    windowed.accept(plan.clone()).expect("the planned window");
    let conjunct = conjunct_of(&windowed, &plan, "year");
    let [cut] = node_ids(&plan, |node| matches!(node, PhysicalNode::TargetCut { .. }))[..] else {
        panic!("one window cut");
    };
    let Some(PhysicalNode::TargetCut { input, .. }) = plan.node(cut).cloned() else {
        unreachable!()
    };
    let filter = plan.add(PhysicalNode::Filter {
        input,
        filters: vec![conjunct],
    });
    if let Some(PhysicalNode::TargetCut { input, .. }) = plan.node_mut(cut) {
        *input = filter;
    }
    let (check, detail) = windowed.refused(plan);
    assert_eq!(check, "predicate origin", "{detail}");
    // One of two sibling `optional`s on one binding: its conjunct moved into
    // the other's inner tree, which would keep the first's rows and null the
    // second's instead. The written region no longer tests it.
    let siblings = Fixture::new(
        r#"query q() {
    match { $d: Doc, optional { $e: Doc, $d cites $e where $d.year > 2000 }, optional { $f: Doc, $d cites $f } }
    return { $d.slug, $e.slug, $f.slug }
}"#,
        &[],
    );
    let plan = siblings.plan();
    siblings
        .accept(plan.clone())
        .expect("the planned optionals");
    let joins: Vec<(usize, usize)> = node_ids(&plan, |node| {
        matches!(node, PhysicalNode::OptionalJoin { .. })
    })
    .into_iter()
    .filter_map(|id| match plan.node(id) {
        Some(PhysicalNode::OptionalJoin { inner, .. }) => Some((id, *inner)),
        _ => None,
    })
    .collect();
    let (check, detail) = siblings.refused(move_conjunct(&plan, &joins, "year"));
    assert_eq!(check, "predicate retention", "{detail}");
    // One of two sibling correlated reads on one binding, alike.
    let reads = Fixture::new(
        r#"query q() {
    match { $d: Doc,
        from { match { $e: Doc, $d cites $e } where $d.year > 2000 return { $e } order { $e.year desc } limit 1 },
        from { match { $f: Doc, $d cites $f } return { $f } order { $f.year desc } limit 1 } }
    return { $d.slug, $e.slug, $f.slug }
}"#,
        &[],
    );
    let plan = reads.plan();
    reads.accept(plan.clone()).expect("the planned reads");
    let correlates: Vec<(usize, usize)> =
        node_ids(&plan, |node| matches!(node, PhysicalNode::Correlate { .. }))
            .into_iter()
            .filter_map(|id| match plan.node(id) {
                Some(PhysicalNode::Correlate { inner, .. }) => Some((id, *inner)),
                _ => None,
            })
            .collect();
    let (check, detail) = reads.refused(move_conjunct(&plan, &correlates, "year"));
    assert_eq!(check, "predicate retention", "{detail}");
}

/// `plan` with the conjunct naming `written`, tested in the inner tree of
/// one of two sibling scopes (`scopes`: each scope's node and inner root),
/// tested at the top of the other's inner tree instead.
fn move_conjunct(plan: &PhysicalPlan, scopes: &[(usize, usize)], written: &str) -> PhysicalPlan {
    let [first, second] = scopes else {
        panic!("two sibling scopes: {scopes:?}");
    };
    let mut plan = plan.clone();
    let holds = |plan: &PhysicalPlan, inner: usize| {
        let mut nodes = Vec::new();
        let mut pending = vec![inner];
        while let Some(id) = pending.pop() {
            nodes.push(id);
            pending.extend(plan.node(id).map(PhysicalNode::inputs).unwrap_or_default());
        }
        super::invariants::conjuncts(plan, &nodes)
            .into_iter()
            .find(|(_, conjunct)| conjunct.to_string().contains(written))
    };
    let ((holder, conjunct), to) = match (holds(&plan, first.1), holds(&plan, second.1)) {
        (Some(held), None) => (held, second),
        (None, Some(held)) => (held, first),
        other => panic!("one scope tests `{written}`: {other:?}"),
    };
    match plan.node_mut(holder) {
        Some(PhysicalNode::Filter { filters, .. }) => filters.retain(|own| *own != conjunct),
        other => panic!("`{written}` is tested by a filter: {other:?}"),
    }
    let filter = plan.add(PhysicalNode::Filter {
        input: to.1,
        filters: vec![conjunct],
    });
    match plan.node_mut(to.0) {
        Some(PhysicalNode::OptionalJoin { inner, .. } | PhysicalNode::Correlate { inner, .. }) => {
            *inner = filter
        }
        other => panic!("a scope node: {other:?}"),
    }
    plan
}

/// A `nearest` window drawn as a pattern step keeps its `k` targets with a
/// `TargetCut` over the sort that orders them; a plan without the two keeps
/// every ranked row and is refused.
#[test]
fn a_window_without_its_target_cut_fails_the_row_cut() {
    let windowed = Fixture::new(
        "query q() { match { let $pop = { match { $d: Doc } return { $d } }, from nearest($pop, $d.embedding, [0.0, 1.0], k: 2) as $distance } where $d.year > 2000 return { $d.slug } }",
        &[],
    );
    let mut plan = windowed.plan();
    let [cut] = node_ids(&plan, |node| matches!(node, PhysicalNode::TargetCut { .. }))[..] else {
        panic!("one window cut");
    };
    let Some(PhysicalNode::TargetCut { input: sort, .. }) = plan.node(cut).cloned() else {
        unreachable!()
    };
    // The window's sort goes with its cut, which it orders for.
    let below = match plan.node(sort) {
        Some(PhysicalNode::Sort { input, .. }) => *input,
        _ => sort,
    };
    let parents: Vec<usize> = node_ids(&plan, |node| node.inputs().contains(&cut));
    for parent in parents {
        if let Some(PhysicalNode::Filter { input, .. } | PhysicalNode::Projection { input, .. }) =
            plan.node_mut(parent)
        {
            *input = below;
        }
    }
    let (check, detail) = windowed.refused(plan);
    assert_eq!(check, "row cut", "{detail}");
}

#[test]
fn another_limit_fails_the_row_cut() {
    let fixture = filtered();
    let mut plan = fixture.plan();
    let root = plan.root();
    if let Some(PhysicalNode::Limit { rows, .. }) = plan.node_mut(root) {
        *rows = 3;
    }
    let (check, _) = fixture.refused(plan);
    assert_eq!(check, "row cut");
}

/// Every count cut a plan declares must be one the query requires, where it
/// requires it. Refused: a `Limit` over the root of a query without `limit`,
/// a second `Limit` below a written one, and a capped `Sort` below the final
/// sort, with and without a written `limit`; the final sort's own top-k under
/// a written `limit` stays accepted.
#[test]
fn an_unwritten_row_cut_fails_the_row_cut() {
    let cut_over = |plan: &mut PhysicalPlan, node: PhysicalNode, input: usize| {
        let properties = plan.properties(input).cloned();
        let cut = plan.add(node);
        if let Some(properties) = properties {
            plan.set_properties(cut, properties);
        }
        cut
    };
    let limit_over = |plan: &mut PhysicalPlan, input: usize| {
        cut_over(plan, PhysicalNode::Limit { input, rows: 0 }, input)
    };
    // A capped sort spliced below the plan's final sort, ordered as it is.
    let capped_below_final = |plan: &mut PhysicalPlan| {
        let final_sort = node_ids(plan, |node| matches!(node, PhysicalNode::Sort { .. }))[0];
        let Some(PhysicalNode::Sort {
            input,
            order_by,
            tiebreak,
            ..
        }) = plan.node(final_sort).cloned()
        else {
            unreachable!("the id names a sort");
        };
        let capped = cut_over(
            plan,
            PhysicalNode::Sort {
                input,
                order_by,
                fetch: Some(0),
                tiebreak,
            },
            input,
        );
        if let Some(PhysicalNode::Sort { input, .. }) = plan.node_mut(final_sort) {
            *input = capped;
        }
    };

    let unlimited = Fixture::new(
        "query q() { match { $d: Doc } where contains($d.title, \"graph\") return { $d.slug } }",
        &[],
    );
    let mut plan = unlimited.plan();
    let root = plan.root();
    let cut = limit_over(&mut plan, root);
    plan.set_root(cut);
    let (check, detail) = unlimited.refused(plan);
    assert_eq!(check, "row cut", "{detail}");

    let limited = filtered();
    let mut plan = limited.plan();
    let Some(PhysicalNode::Limit { input, .. }) = plan.node(plan.root()).cloned() else {
        panic!("the limited plan's root is its cut");
    };
    let below = limit_over(&mut plan, input);
    if let Some(PhysicalNode::Limit { input, .. }) = plan.node_mut(plan.root()) {
        *input = below;
    }
    let (check, detail) = limited.refused(plan);
    assert_eq!(check, "row cut", "{detail}");

    let ordered = Fixture::new(
        "query q() { match { $d: Doc } return { $d.slug } order { $d.year } }",
        &[],
    );
    for fixture in [&ordered, &limited] {
        fixture
            .accept(fixture.plan())
            .expect("the planned final sort is the cut the query requires");
        let mut plan = fixture.plan();
        capped_below_final(&mut plan);
        let (check, detail) = fixture.refused(plan);
        assert_eq!(check, "row cut", "{detail}");
    }
}

/// An order key constant for every row (a parameter, `now()`) orders
/// nothing; when `return` projects it under an alias the planner binds the key
/// to that alias, and the order check treats the alias as the constant it
/// projects.
#[test]
fn a_constant_key_bound_to_a_return_alias_is_accepted() {
    for (query, params) in [
        (
            "query q($x: I64) { match { $d: Doc } return { $d.slug, $x as x } order { $x } }",
            vec![("x", Literal::Integer(7))],
        ),
        (
            "query q() { match { $d: Doc } return { $d.slug, now() as t } order { now() } }",
            vec![],
        ),
    ] {
        let fixture = Fixture::new(query, &params);
        fixture
            .accept(fixture.plan())
            .unwrap_or_else(|error| panic!("{query}: {error:?}"));
    }
}

#[test]
fn a_dropped_or_reversed_key_fails_the_order() {
    let fixture = ranked();
    for edit in [
        |order_by: &mut Vec<omnigraph_compiler::ir::IROrdering>| {
            order_by.pop();
        },
        |order_by: &mut Vec<omnigraph_compiler::ir::IROrdering>| {
            let last = order_by.last_mut().unwrap();
            last.descending = !last.descending;
        },
        |order_by: &mut Vec<omnigraph_compiler::ir::IROrdering>| {
            order_by.remove(0);
        },
        |order_by: &mut Vec<omnigraph_compiler::ir::IROrdering>| {
            order_by.last_mut().unwrap().nulls_first = Some(false);
        },
        // The direction's own placement, written, is another key too.
        |order_by: &mut Vec<omnigraph_compiler::ir::IROrdering>| {
            order_by.last_mut().unwrap().nulls_first = Some(true);
        },
    ] {
        let mut plan = fixture.plan();
        for id in node_ids(&plan, |node| matches!(node, PhysicalNode::Sort { .. })) {
            if let Some(PhysicalNode::Sort { order_by, .. }) = plan.node_mut(id) {
                edit(order_by);
            }
        }
        let (check, detail) = fixture.refused(plan);
        assert_eq!(check, "order", "{detail}");
    }
}

/// A written null placement is part of the key the final sort must keep:
/// a plan that drops it, flips it, or keeps it on a key the query placed
/// differently is refused.
#[test]
fn a_dropped_or_moved_null_placement_fails_the_order() {
    let query = r#"query q() {
    match { $d: Doc }
    return { $d.slug, $d.rank as rank }
    order { $d.rank desc nulls first, $d.year nulls last }
    limit 2
}"#;
    let fixture = Fixture::new(query, &[]);
    let plan = fixture.plan();
    let placements: Vec<Vec<Option<bool>>> =
        node_ids(&plan, |node| matches!(node, PhysicalNode::Sort { .. }))
            .into_iter()
            .map(|id| match plan.node(id) {
                Some(PhysicalNode::Sort { order_by, .. }) => {
                    order_by.iter().map(|key| key.nulls_first).collect()
                }
                _ => unreachable!("the id names a sort"),
            })
            .collect();
    assert_eq!(placements, vec![vec![Some(true), Some(false)]]);
    fixture
        .accept(fixture.plan())
        .unwrap_or_else(|error| panic!("{error:?}"));
    for edit in [
        |order_by: &mut Vec<omnigraph_compiler::ir::IROrdering>| {
            order_by[0].nulls_first = None;
        },
        |order_by: &mut Vec<omnigraph_compiler::ir::IROrdering>| {
            order_by[0].nulls_first = Some(false);
        },
        |order_by: &mut Vec<omnigraph_compiler::ir::IROrdering>| {
            order_by[1].nulls_first = None;
        },
        |order_by: &mut Vec<omnigraph_compiler::ir::IROrdering>| {
            order_by.swap(0, 1);
        },
    ] {
        let mut plan = fixture.plan();
        for id in node_ids(&plan, |node| matches!(node, PhysicalNode::Sort { .. })) {
            if let Some(PhysicalNode::Sort { order_by, .. }) = plan.node_mut(id) {
                edit(order_by);
            }
        }
        let (check, detail) = fixture.refused(plan);
        assert_eq!(check, "order", "{detail}");
    }
}

#[test]
fn a_missing_identity_key_fails_the_order() {
    let fixture = filtered();
    let mut plan = fixture.plan();
    for id in node_ids(&plan, |node| matches!(node, PhysicalNode::Sort { .. })) {
        if let Some(PhysicalNode::Sort { tiebreak, .. }) = plan.node_mut(id) {
            assert_eq!(*tiebreak, vec![ColumnRef::property("d", IDENTITY_MEMBER)]);
            tiebreak.clear();
        }
    }
    let (check, detail) = fixture.refused(plan);
    assert_eq!(check, "order", "{detail}");
}

/// An edge binding `$e: Cites` scans the edge type through fresh ends the
/// query never names: the edge's identity leads the tie-break and theirs
/// follow, and a plan that drops the edge's is refused.
#[test]
fn an_edge_scan_ties_by_its_edge_before_its_fresh_ends() {
    let fixture = Fixture::new(
        r#"query q() {
    match { $e: Cites }
    return { $e.@src, $e.@dst }
    order { $e.@src }
}"#,
        &[],
    );
    let plan = fixture.plan();
    fixture.accept(plan.clone()).expect("the planned tie-break");
    let mut dropped = plan;
    for id in node_ids(&dropped, |node| matches!(node, PhysicalNode::Sort { .. })) {
        if let Some(PhysicalNode::Sort { tiebreak, .. }) = dropped.node_mut(id) {
            assert_eq!(
                tiebreak.first(),
                Some(&ColumnRef::property("e", IDENTITY_MEMBER)),
                "{tiebreak:?}"
            );
            tiebreak.remove(0);
        }
    }
    let (check, detail) = fixture.refused(dropped);
    assert_eq!(check, "order", "{detail}");
}

/// A `limit` with no `order` cuts the tie rule's order: the plan sorts by
/// every reference's identity in column order (`$z`, bound first, before
/// `$a`), and a plan that sorts them in name order, or by no key at all
/// (leaving the scan's order), is refused.
#[test]
fn a_limit_with_no_order_cuts_the_tie_rule_order() {
    let fixture = Fixture::new(
        r#"query q() {
    match { $z: Doc, $z cites $a }
    return { $z.slug, $a.slug }
    limit 2
}"#,
        &[],
    );
    let plan = fixture.plan();
    fixture.accept(plan.clone()).expect("the planned tie rule");
    let sorts = node_ids(&plan, |node| matches!(node, PhysicalNode::Sort { .. }));
    let [sort] = sorts[..] else {
        panic!("one sort: {sorts:?}");
    };
    let Some(PhysicalNode::Sort {
        order_by, tiebreak, ..
    }) = plan.node(sort).cloned()
    else {
        unreachable!("the id names a sort");
    };
    assert!(order_by.is_empty(), "{order_by:?}");
    assert_eq!(
        tiebreak,
        [
            ColumnRef::property("z", IDENTITY_MEMBER),
            ColumnRef::property("a", IDENTITY_MEMBER)
        ]
    );

    let mut name_order = plan.clone();
    if let Some(PhysicalNode::Sort { tiebreak, .. }) = name_order.node_mut(sort) {
        tiebreak.reverse();
    }
    let (check, detail) = fixture.refused(name_order);
    assert_eq!(check, "order", "{detail}");

    let mut unsorted = plan;
    if let Some(PhysicalNode::Sort { tiebreak, .. }) = unsorted.node_mut(sort) {
        tiebreak.clear();
    }
    let (check, detail) = fixture.refused(unsorted);
    assert_eq!(check, "order", "{detail}");
}

#[test]
fn another_alias_or_item_fails_the_projection() {
    let fixture = filtered();
    let mut plan = fixture.plan();
    for id in node_ids(&plan, |node| {
        matches!(node, PhysicalNode::Projection { .. })
    }) {
        if let Some(PhysicalNode::Projection { return_exprs, .. }) = plan.node_mut(id) {
            return_exprs[1].alias = Some("age".to_string());
        }
    }
    let (check, _) = fixture.refused(plan);
    assert_eq!(check, "projection");
}

#[test]
fn changed_aggregate_comparison_returns_are_refused() {
    let fixture = Fixture::new(
        "query q() { match { $d: Doc } where $d.open = true return { $d.title as n, count($d.year) > 1 as many } }",
        &[],
    );
    for mutation in ["alias", "constant", "distinct", "group", "unused"] {
        let mut plan = fixture.plan();
        for id in node_ids(&plan, |node| {
            matches!(
                node,
                PhysicalNode::Projection { .. } | PhysicalNode::Aggregate { .. }
            )
        }) {
            match plan.node_mut(id).unwrap() {
                PhysicalNode::Projection { return_exprs, .. } => match mutation {
                    "alias" => return_exprs[1].alias = Some("changed".into()),
                    "constant" => {
                        let IRExpr::Binary { right, .. } = &mut return_exprs[1].expr else {
                            panic!("comparison")
                        };
                        let IRExpr::Literal(value, _) = right.as_mut() else {
                            panic!("literal")
                        };
                        *value = Literal::Integer(2);
                    }
                    _ => {}
                },
                PhysicalNode::Aggregate {
                    return_exprs,
                    aggregates,
                    ..
                } => match mutation {
                    "distinct" => {
                        let IRExpr::Aggregate { distinct, .. } = &mut return_exprs[1].expr else {
                            panic!("aggregate")
                        };
                        *distinct = true;
                    }
                    "group" => {
                        let IRExpr::PropAccess { property, .. } = &mut return_exprs[0].expr else {
                            panic!("group property")
                        };
                        *property = "slug".into();
                    }
                    "unused" => {
                        let mut item = return_exprs[1].clone();
                        item.column = "extra".into();
                        item.alias = Some("extra".into());
                        return_exprs.push(item);
                        aggregates.push(aggregates[1]);
                    }
                    _ => {}
                },
                _ => unreachable!(),
            }
            if let Some(PhysicalNode::Aggregate {
                return_exprs,
                node_objects,
                ..
            }) = plan.node(id)
            {
                let mut properties = plan.properties(id).unwrap().clone();
                properties.schema =
                    crate::output::return_schema(return_exprs, node_objects).unwrap();
                plan.set_properties(id, properties);
            }
        }
        let (check, detail) = fixture.refused(plan);
        assert_eq!(check, "projection", "{mutation}: {detail}");
    }
}

#[test]
fn a_second_projection_cannot_introduce_grouping() {
    let fixture = Fixture::new(
        "query q() { match { $d: Doc } where contains($d.title, \"graph\") return { $d.year } }",
        &[],
    );
    let mut plan = fixture.plan();
    let id = node_ids(&plan, |node| {
        matches!(node, PhysicalNode::Projection { .. })
    })[0];
    let PhysicalNode::Projection {
        input,
        return_exprs,
        node_objects,
    } = plan.node(id).unwrap().clone()
    else {
        panic!("projection")
    };
    let aggregate = plan.add(PhysicalNode::Aggregate {
        input,
        aggregates: vec![None; return_exprs.len()],
        return_exprs: return_exprs.clone(),
        node_objects: node_objects.clone(),
    });
    plan.set_properties(aggregate, plan.properties(id).unwrap().clone());
    *plan.node_mut(id).unwrap() = PhysicalNode::Projection {
        input: aggregate,
        return_exprs: return_exprs
            .into_iter()
            .map(|mut item| {
                item.expr = IRExpr::AliasRef(item.column.clone(), item.ty.clone());
                item
            })
            .collect(),
        node_objects,
    };
    let (check, detail) = fixture.refused(plan);
    assert_eq!(check, "projection", "{detail}");
}

#[test]
fn a_score_read_from_another_column_fails_the_projection() {
    let fixture = ranked();
    let mut plan = fixture.plan();
    for id in node_ids(&plan, |node| {
        matches!(node, PhysicalNode::Projection { .. })
    }) {
        if let Some(PhysicalNode::Projection { return_exprs, .. }) = plan.node_mut(id) {
            return_exprs[1].expr = IRExpr::PropAccess {
                variable: "d".to_string(),
                property: "year".to_string(),
                ty: return_exprs[1].expr.ty().clone(),
            };
        }
    }
    let (check, _) = fixture.refused(plan);
    assert_eq!(check, "projection");
}

#[test]
fn an_exhausted_budget_is_a_resource_outcome() {
    let fixture = ranked();
    let input = AcceptInput {
        core: None,
        catalog: &fixture.catalog,
        ir: &fixture.ir,
        params: &fixture.params,
        keys: &Keys,
        limits: ValidationLimits {
            work: 3,
            ..ValidationLimits::DEFAULT
        },
    };
    assert_eq!(
        accept(fixture.plan(), &input, fixture.derivation()).unwrap_err(),
        ValidationError::Exhausted {
            limit: "work",
            value: 3
        }
    );
}

const NEAREST: &str = r#"query q($v: Vector(2)) {
    match { let $pop = { match { $d: Doc, $e: Doc, $d cites $e } return { $d, $e } }, from nearest($pop, $d.embedding, $v, k: 3) as $distance }
    return { $d.slug }
    order { $distance }
    limit 3
}"#;

fn nearest() -> Fixture {
    Fixture::new(
        NEAREST,
        &[(
            "v",
            Literal::List(vec![Literal::Float(0.0), Literal::Float(1.0)]),
        )],
    )
}

/// The ranked scan of `plan` and its access, mutably.
fn ranked_access(plan: &mut PhysicalPlan) -> &mut crate::physical::RankedAccess {
    let id = node_ids(plan, |node| node.ranked().is_some())[0];
    match plan.node_mut(id) {
        Some(PhysicalNode::Scan {
            ranked: Some(ranked),
            ..
        }) => ranked,
        _ => unreachable!("the id names a ranked scan"),
    }
}

#[test]
fn a_nearest_plan_declares_its_policy_and_prepass() {
    let fixture = nearest();
    let mut plan = fixture.plan();
    let access = ranked_access(&mut plan).clone();
    assert_eq!(access.policy, Some(crate::physical::NearestPolicy::DEFAULT));
    let prefilter = access
        .prefilter
        .expect("the edge from `$d` declares a pre-pass");
    assert_eq!(prefilter.hops.len(), 1);
    fixture
        .accept(plan)
        .expect("the declared policy is accepted");
}

#[test]
fn a_stalling_or_unguarded_ladder_fails_the_declared_policy() {
    let fixture = nearest();
    let breaks: [fn(&mut crate::physical::RankedAccess); 4] = [
        |access| access.policy.as_mut().unwrap().probe_factor = 1,
        |access| access.policy.as_mut().unwrap().flat_rescan_on_unreached = false,
        |access| access.policy.as_mut().unwrap().uncapped_on_missing_counters = false,
        |access| access.policy = None,
    ];
    for edit in breaks {
        let mut plan = fixture.plan();
        edit(ranked_access(&mut plan));
        let (check, detail) = fixture.refused(plan);
        assert_eq!(check, "declared policy", "{detail}");
    }
}

#[test]
fn a_prepass_off_the_required_hops_fails_the_declared_policy() {
    let fixture = nearest();
    let breaks: [fn(&mut crate::physical::Prefilter); 2] = [
        |prefilter| prefilter.hops[0].edge_type = "Other".to_string(),
        |prefilter| prefilter.feeds.push(usize::MAX),
    ];
    for edit in breaks {
        let mut plan = fixture.plan();
        edit(ranked_access(&mut plan).prefilter.as_mut().unwrap());
        let (check, detail) = fixture.refused(plan);
        assert_eq!(check, "declared policy", "{detail}");
    }
}

/// The scope one query is accepted under.
fn scope_of(query: &str, params: &[(&str, Literal)]) -> ValidationScope {
    let fixture = Fixture::new(query, params);
    fixture
        .accept(fixture.plan())
        .unwrap_or_else(|error| panic!("`{query}` is not accepted: {error:?}"))
        .scope()
}

/// Every admitted form of the exact fragment is a member, and every
/// excluded family is not: membership reads the checked declaration only.
#[test]
fn membership_admits_the_fragment_and_nothing_else() {
    let members = [
        "query q() { match { $d: Doc } return { $d.slug } }",
        "query q() { match { $d: Doc } where $d.open = true return { $d.slug } }",
        "query q($y: I64) { match { $d: Doc } where $d.year >= $y return { $d.slug } }",
        "query q() { match { $d: Doc } where $d.year != 3 and not ($d.title < \"m\" or $d.open) return { $d.slug } }",
        "query q() { match { $d: Doc } where ($d.rank is null or $d.rank > 2) return { $d.slug, $d.rank } }",
        "query q() { match { $d: Doc } where $d.open return { $d.title as t } order { $d.year desc, $d.title } limit 4 }",
        "query q($t: String) { match { $d: Doc } where $d.title = $t return { $d.slug } limit 0 }",
    ];
    for query in members {
        let params = [
            ("y", Literal::Integer(1)),
            ("t", Literal::String("x".into())),
            ("q", Literal::String("graph".into())),
        ];
        let used: Vec<(&str, Literal)> = params
            .iter()
            .filter(|(name, _)| query.contains(&format!("${name}:")))
            .cloned()
            .collect();
        assert_eq!(
            scope_of(query, &used),
            ValidationScope::ExactSubset,
            "{query}"
        );
    }
    let outside = [
        "query q() { match { $d: Doc } where contains($d.title, \"g\") return { $d.slug } }",
        "query q() { match { $d: Doc } where $d.score > 1.5 return { $d.slug } }",
        "query q() { match { $d: Doc } where $d.born > date(\"2020-01-01\") return { $d.slug } }",
        "query q() { match { $d: Doc } where \"a\" in $d.tags return { $d.slug } }",
        "query q() { match { $d: Doc, $d cites $e } return { $d.slug } }",
        "query q() { match { $d: Doc, let $es = { match { $e: Doc } return { $e } }, from $es } return { $d.slug } }",
        "query q() { match { $d: Doc } where not exists { $e: Doc, $d cites $e } return { $d.slug } }",
        "query q() { match { $d: Doc } return { count($d) as n } }",
        "query q() { match { $d: Doc } return { $d } }",
        "query q() { match { $d: Doc } return { $d.@id } }",
        "query q() { match { $d: Doc } return { $d.slug } order { $d.score } }",
        "query q() { match { $d: Doc } where match_terms($d.title, terms(\"g\", mode: any)) return { $d.slug } }",
        "query q($v: Vector(2)) { match { let $pop = { match { $d: Doc } return { $d } }, from nearest($pop, $d.embedding, $v, k: 2) as $distance } return { $d.slug } order { $distance } limit 2 }",
        "query q($q: String) { match { let $pop = { match { $d: Doc } return { $d } }, from bm25($pop, $d.title, $q) as $score } return { $score as s, $d.slug } order { $score desc, $d.slug desc } limit 2 }",
        "query q($v: Vector(2), $w: Vector(2)) { match { let $all = { match { $d: Doc } return { $d } }, from rrf(nearest($all, $d.embedding, $v, k: 2), nearest($all, $d.embedding, $w, k: 2), k: 60) as $score, $d: Doc } return { $d.slug } order { $score desc } limit 2 }",
    ];
    for query in outside {
        let params = [
            (
                "v",
                Literal::List(vec![Literal::Float(0.0), Literal::Float(1.0)]),
            ),
            (
                "w",
                Literal::List(vec![Literal::Float(1.0), Literal::Float(0.0)]),
            ),
            ("q", Literal::String("graph".into())),
        ];
        let used: Vec<(&str, Literal)> = params
            .iter()
            .filter(|(name, _)| query.contains(&format!("${name}:")))
            .cloned()
            .collect();
        assert_eq!(
            scope_of(query, &used),
            ValidationScope::InvariantsOnly,
            "{query}"
        );
    }
}

/// The plain and the explained acceptance share one path: the same plan and
/// scope, and the same outcome when a validation limit runs out.
#[test]
fn both_acceptance_entries_agree_even_when_a_limit_runs_out() {
    for limits in [
        ValidationLimits::DEFAULT,
        ValidationLimits {
            work: 5,
            ..ValidationLimits::DEFAULT
        },
        ValidationLimits {
            steps: 2,
            ..ValidationLimits::DEFAULT
        },
    ] {
        let fixture = ranked();
        let input = AcceptInput {
            core: None,
            catalog: &fixture.catalog,
            ir: &fixture.ir,
            params: &fixture.params,
            keys: &Keys,
            limits,
        };
        let plain = futures::executor::block_on(crate::gate::accept_query(
            &input,
            &fixture.source,
            &BOUNDS,
        ));
        let explained = futures::executor::block_on(crate::gate::accept_query_explained(
            &input,
            &fixture.source,
            &BOUNDS,
        ));
        match (plain, explained) {
            (Ok(plain), Ok((explained, explain))) => {
                assert_eq!(plain.plan(), explained.plan());
                assert_eq!(plain.scope(), explained.scope());
                assert_eq!(explain.validation, Some(explained.summary()));
            }
            (Err(plain), Err(explained)) => {
                assert_eq!(plain, explained);
                assert!(
                    matches!(plain, crate::gate::Unrouted::ValidationExhausted { .. }),
                    "{plain:?}"
                );
            }
            (plain, explained) => panic!("the entries disagree: {plain:?} / {explained:?}"),
        }
    }
}

/// Evidence size and checking work as a member query grows: serialized
/// derivation bytes, rule applications, retained nodes and visits, and the
/// checking time. A decision instrument, not a gate.
#[test]
#[ignore = "instrument: prints derivation bytes, steps, nodes, visits and time per conjunct count"]
fn derivation_cost_grows_with_the_query() {
    for conjuncts in [1usize, 4, 16, 64, 256] {
        let filter: Vec<String> = (0..conjuncts)
            .map(|index| format!("$d.year != {index}"))
            .collect();
        let query = format!(
            "query q() {{ match {{ $d: Doc }} where {} return {{ $d.slug }} order {{ $d.year }} limit 10 }}",
            filter.join(" and ")
        );
        let fixture = Fixture::new(&query, &[]);
        let traced = fixture.traced();
        let derivation = traced.derivation().unwrap();
        let bytes = serde_json::to_vec(&derivation).unwrap().len();
        let input = AcceptInput {
            core: None,
            catalog: &fixture.catalog,
            ir: &fixture.ir,
            params: &fixture.params,
            keys: &Keys,
            limits: ValidationLimits::DEFAULT,
        };
        let mut budget = Budget::new(ValidationLimits::DEFAULT);
        let started = std::time::Instant::now();
        let (scope, _) = check(
            &traced.optimized.physical,
            &input,
            Some(derivation),
            &mut budget,
        )
        .unwrap();
        let elapsed = started.elapsed();
        let (nodes, steps, work) = budget.used();
        println!(
            "conjuncts={conjuncts} scope={} bytes={bytes} steps={steps} nodes={nodes} work={work} elapsed={elapsed:?}",
            scope.as_str()
        );
    }
}

/// The checker keeps only each role's current node: absorbing 64 conjuncts
/// one at a time replaces the filter and the scan 64 times, and every
/// replaced node is released, so the arena ends holding no more than the
/// chain's six roles rather than a copy of the predicate per step.
#[test]
fn the_derivation_arena_keeps_only_current_nodes() {
    let filter: Vec<String> = (0..64)
        .map(|index| format!("$d.title != \"{index}\""))
        .collect();
    let query = format!(
        "query q() {{ match {{ $d: Doc }} where {} return {{ $d.slug }} order {{ $d.year }} limit 10 }}",
        filter.join(" and ")
    );
    let fixture = Fixture::new(&query, &[]);
    let derivation = fixture.derivation().expect("an exact-subset member");
    assert!(
        derivation.steps.len() >= 64,
        "every conjunct is absorbed by its own step: {}",
        derivation.steps.len()
    );
    fixture.accept(fixture.plan()).unwrap();
    let retained = super::subset::RETAINED_NODES.with(std::cell::Cell::get);
    assert!(retained <= 6, "the arena retained {retained} nodes");
}

/// A rule may name only a node the derivation has reached and not yet
/// replaced: a forward reference and a superseded node are both refused.
#[test]
fn a_reference_to_an_unreached_or_superseded_node_is_refused() {
    let fixture = filtered();
    let plan = fixture.plan();
    let derivation = fixture.derivation().unwrap();
    let mut forward = derivation.clone();
    forward.steps[0].at = vec![usize::MAX, 0];
    let mut superseded = derivation.clone();
    let first = superseded.steps[0].at.clone();
    superseded.steps[1].at = first;
    for broken in [forward, superseded] {
        match fixture.accept_with(plan.clone(), Some(broken)) {
            Err(ValidationError::Violated { check, detail }) => {
                assert_eq!(check, "exact subset");
                assert!(detail.contains("is not"), "{detail}");
            }
            other => panic!("{other:?}"),
        }
    }
}

/// A return item the lowering folds to a constant (`@type` of a binding
/// with one edge type) cannot tell two rows apart, so ordering by the other
/// returns completes the comparator without identities.
#[test]
fn a_constant_return_needs_no_tiebreak() {
    let fixture = Fixture::new(
        r#"query q() {
    match { $p: Doc, $d: Doc, $p $e:cites $d }
    return { $p.slug as source, $d.slug as target, $e.@type as edge_type }
    order { $p.slug, $d.slug, $e.@type }
}"#,
        &[],
    );
    assert!(
        fixture
            .ir
            .return_exprs
            .iter()
            .any(|projection| matches!(projection.expr, IRExpr::Literal(_, _))),
        "the lowering folds the single edge type"
    );
    fixture
        .accept(fixture.plan())
        .expect("a constant return is covered by the other keys");
}

/// An `order` clause whose every key folds to a constant still orders its
/// rows, by the identity tie-break alone, so the plan's sort is required
/// rather than refused. (Over groups such a key is no return item and is
/// refused at type check, `T42`.)
#[test]
fn a_constant_order_still_sorts() {
    let query = r#"query q() {
    match { $p: Doc, $d: Doc, $p $e:cites $d }
    return { $d.slug as target }
    order { $e.@type }
}"#;
    let fixture = Fixture::new(query, &[]);
    assert!(
        fixture
            .ir
            .order_by
            .iter()
            .all(|key| matches!(key.expr, IRExpr::Literal(_, _))),
        "the binding folds the single edge type"
    );
    fixture
        .accept(fixture.plan())
        .unwrap_or_else(|error| panic!("{error:?}"));
}

/// `return distinct` groups rows like an aggregate: the planner binds its
/// order keys to return items, completes the order with every unwritten
/// group key instead of an identity tie-break, and the query is outside the
/// exact fragment.
#[test]
fn a_distinct_return_sorts_its_groups() {
    for query in [
        r#"query q() {
    match { $d: Doc }
    return distinct { $d.year, $d.title }
    order { $d.year }
    limit 2
}"#,
        r#"query q() {
    match { $d: Doc }
    return distinct { $d.title, 1 as one }
    order { $d.title desc, 1 }
}"#,
    ] {
        let fixture = Fixture::new(query, &[]);
        assert!(fixture.ir.distinct, "{query}");
        let accepted = fixture
            .accept(fixture.plan())
            .unwrap_or_else(|error| panic!("{query}: {error:?}"));
        assert_eq!(accepted.scope(), ValidationScope::InvariantsOnly, "{query}");
    }
}

/// Every full-text field has a profile, so the exact lexical scan answers a
/// lexical predicate whatever the property's index: a source declaring no
/// index plans it, and the plan is accepted (the GQ 3 RFC, section 4: no
/// `FullTextIndexRequired`).
#[test]
fn a_full_text_call_plans_without_an_index() {
    let search = r#"query q($q: String) {
    match { $d: Doc } where match_terms($d.title, terms($q, mode: any))
    return { $d.slug }
}"#;
    let inherited = r#"query q($q: String) {
    match { $d: Doc } where not exists { $x: Doc, $d cites $x where match_terms($d.title, terms($q, mode: any)) }
    return { $d.slug }
}"#;
    let nested = r#"query q($q: String) {
    match { $p: Doc }
    where not exists {
        $d: Doc, $p cites $d
        where not exists { $x: Doc, $d cites $x where match_terms($d.title, terms($q, mode: any, max_edits: 2)) }
    }
    return { $p.slug }
}"#;
    for query in [search, RANKED, inherited, nested] {
        let fixture = Fixture::new(query, &[("q", Literal::String("graph".into()))]);
        fixture
            .accept(fixture.plan())
            .unwrap_or_else(|error| panic!("{query}: {error:?}"));
    }
}

/// A conjunct that can fail written first, then one that cannot: the
/// planner keeps both in written order above the scan.
const FAILING_FIRST: &str = r#"query q() {
    match { $d: Doc } where 10 / $d.year > 1 and $d.open = true
    return { $d.slug }
}"#;

/// The two conjuncts of [`FAILING_FIRST`], as its plan tests them.
fn failing_first_conjuncts(plan: &PhysicalPlan) -> (IRExpr, IRExpr) {
    let tested: Vec<IRExpr> = plan
        .live()
        .flat_map(|(_, node)| match node {
            PhysicalNode::Filter { filters, .. } => filters.clone(),
            _ => Vec::new(),
        })
        .collect();
    let find = |text: &str| {
        tested
            .iter()
            .find(|c| c.to_string().contains(text))
            .cloned()
            .unwrap_or_else(|| panic!("the plan tests no conjunct holding `{text}`: {tested:?}"))
    };
    (find("/"), find("open"))
}

/// The placement law holds the plan to the written order the core term
/// states, independently of the shape and the optimizer: a conjunct written
/// after one that can fail never runs in a scan, nor does the one that can
/// fail, and a later conjunct never runs first. Each broken plan passes
/// every other check of acceptance.
#[test]
fn a_conjunct_at_or_after_the_first_that_can_fail_keeps_its_written_place() {
    let fixture = Fixture::new(FAILING_FIRST, &[]);
    let plan = fixture.plan();
    fixture
        .accept(plan.clone())
        .expect("the planner's plan keeps the written order");
    let (failing, later) = failing_first_conjuncts(&plan);
    for moved in [&later, &failing] {
        let mut broken = plan.clone();
        place_on_scans(&mut broken, moved);
        let (check, detail) = fixture.refused(broken);
        assert_eq!(check, "placement", "{detail}");
    }
    // The later conjunct tested before the one that can fail.
    let mut broken = plan.clone();
    for id in node_ids(&broken, |node| matches!(node, PhysicalNode::Filter { .. })) {
        if let Some(PhysicalNode::Filter { filters, .. }) = broken.node_mut(id)
            && filters.contains(&failing)
        {
            filters.insert(0, later.clone());
        }
    }
    let (check, detail) = fixture.refused(broken);
    assert_eq!(check, "placement", "{detail}");
}

/// A conjunct that cannot fail, written before the first one that can,
/// moves into its binding's scan: the law allows it, and acceptance takes
/// it.
#[test]
fn a_conjunct_before_the_first_that_can_fail_may_run_in_its_scan() {
    let fixture = Fixture::new(
        r#"query q() {
    match { $d: Doc } where $d.open = true and 10 / $d.year > 1
    return { $d.slug }
}"#,
        &[],
    );
    let plan = fixture.plan();
    let scanned = plan.live().any(|(_, node)| {
        matches!(node, PhysicalNode::Scan { spec, .. }
            if spec.filter.as_ref().is_some_and(|f| f.gq_filters().iter().any(|c| c.to_string().contains("open"))))
    });
    assert!(
        scanned,
        "the conjunct before the one that can fail runs in the scan"
    );
    fixture.accept(plan).expect("the law allows it");
}
