//! Each acceptance check refuses the plan it exists for. Rust and not
//! `.gqt`: a case only reaches plans the planner builds, and the planner
//! builds none of these; each test plans a query, breaks one thing its query
//! requires, and asserts the check that names it.

use std::sync::Arc;

use omnigraph_compiler::CheckedQuery;
use omnigraph_compiler::catalog::{Catalog, build_catalog};
use omnigraph_compiler::ir::{IRExpr, ParamMap, QueryIR, fold};
use omnigraph_compiler::query::ast::Literal;
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
    title: String
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

/// The bound-literal evaluator: literals and bound parameters through the
/// compiler's fold rules.
struct Bound<'p>(&'p ParamMap);

impl ConstantEvaluator for Bound<'_> {
    fn evaluate(&self, expr: &IRExpr) -> Option<Literal> {
        match expr {
            IRExpr::Literal(literal) => Some(literal.clone()),
            IRExpr::Param(name) => self.0.get(name).cloned(),
            IRExpr::Binary { left, op, right } => {
                fold::evaluate(*op, &self.evaluate(left)?, &self.evaluate(right)?)
            }
            IRExpr::Not(inner) => match self.evaluate(inner)? {
                Literal::Bool(value) => Some(Literal::Bool(!value)),
                Literal::Null => Some(Literal::Null),
                _ => None,
            },
            IRExpr::IsNull { expr, negated } => Some(Literal::Bool(
                matches!(self.evaluate(expr)?, Literal::Null) != *negated,
            )),
            _ => None,
        }
    }
}

struct Fixture {
    catalog: Catalog,
    checked: CheckedQuery,
    ir: QueryIR,
    params: ParamMap,
    source: MemorySource,
}

impl Fixture {
    fn new(query: &str, params: &[(&str, Literal)]) -> Self {
        let catalog = build_catalog(&parse_schema(SCHEMA).unwrap()).unwrap();
        let decl = omnigraph_compiler::find_named_query(query, "q").unwrap();
        let checked = CheckedQuery::check(&catalog, &decl).unwrap();
        let ir =
            omnigraph_compiler::lower_query(&catalog, checked.decl(), checked.types()).unwrap();
        let mut source = MemorySource::default().with_edge_version("Cites", 1);
        for (name, node_type) in &catalog.node_types {
            source = source.with_node_type(
                name,
                NodeTypeSpec {
                    table: TableRef {
                        type_key: format!("node:{name}"),
                        dataset_path: format!("node/{name}"),
                        native_branch: None,
                    },
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
                    row_count: None,
                },
            );
        }
        Self {
            catalog,
            checked,
            ir,
            params: params
                .iter()
                .map(|(name, value)| (name.to_string(), value.clone()))
                .collect(),
            source,
        }
    }

    fn traced(&self) -> crate::gate::Traced {
        crate::gate::plan_traced(&self.ir, &self.source, &BOUNDS).expect("the query plans")
    }

    fn plan(&self) -> PhysicalPlan {
        self.traced().optimized.physical
    }

    fn derivation(&self) -> Option<Derivation> {
        self.traced().derivation
    }

    fn accept(&self, plan: PhysicalPlan) -> Result<AcceptedPlan, ValidationError> {
        self.accept_with(plan, self.derivation())
    }

    fn accept_with(
        &self,
        plan: PhysicalPlan,
        derivation: Option<Derivation>,
    ) -> Result<AcceptedPlan, ValidationError> {
        let constants = Bound(&self.params);
        let input = AcceptInput {
            checked: &self.checked,
            catalog: &self.catalog,
            ir: &self.ir,
            params: &self.params,
            constants: &constants,
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
    match { $d: Doc { open: true } $d.year >= $min }
    return { $d.slug, $d.year as year }
    order { $d.year desc }
    limit 2
}"#;

const RANKED: &str = r#"query q($q: String) {
    match { $d: Doc $d.year > 2000 }
    return { $d.slug, bm25($d.title, $q) as score }
    order { bm25($d.title, $q), $d.year }
    limit 3
}"#;

fn filtered() -> Fixture {
    Fixture::new(FILTERED, &[("min", Literal::Integer(2001))])
}

fn ranked() -> Fixture {
    Fixture::new(RANKED, &[("q", Literal::String("graph".into()))])
}

#[test]
fn the_planned_plans_are_accepted() {
    for fixture in [filtered(), ranked()] {
        let accepted = fixture
            .accept(fixture.plan())
            .expect("a planned plan is accepted");
        assert_eq!(accepted.scope(), ValidationScope::ExactSubset);
        assert!(accepted.evidence().derivation().is_some());
    }
    let outside = Fixture::new(
        r#"query q() {
    match { $d: Doc $d.title contains "graph" }
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
            Rule::RankBm25Scan { .. } => "rank",
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
    assert_eq!(
        rules(&ranked().derivation().unwrap()),
        [
            "absorb", "prune", "lower", "rank", "lower", "lower", "lower"
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
fn a_bm25_rank_before_scoring_needs_full_coverage() {
    let mut fixture = ranked();
    fixture.source = std::mem::take(&mut fixture.source).with_full_text_coverage(
        "node:Doc",
        "title",
        crate::source::FullTextCoverage::Full,
    );
    let traced = fixture.traced();
    let accepted = fixture
        .accept_with(traced.optimized.physical.clone(), traced.derivation.clone())
        .expect("full coverage admits filtering before scoring");
    assert_eq!(accepted.scope(), ValidationScope::ExactSubset);
    let mut plan = traced.optimized.physical;
    let mut assumptions = plan.assumptions().clone();
    assumptions.full_text.insert(
        crate::physical::Assumptions::full_text_key("node:Doc", "title"),
        crate::source::FullTextCoverage::Partial,
    );
    plan.set_assumptions(assumptions);
    match fixture.accept_with(plan, traced.derivation) {
        Err(ValidationError::Violated { check, detail }) => {
            assert_eq!(check, "prerequisite", "{detail}");
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
            ranked.query = IRExpr::Literal(Literal::String("other".into()));
        }
    }
    let (check, detail) = fixture.refused(plan);
    assert_eq!(check, "search identity", "{detail}");
}

#[test]
fn a_capped_bm25_scan_fails_the_row_cut() {
    let fixture = ranked();
    let mut plan = fixture.plan();
    for id in node_ids(&plan, |node| node.ranked().is_some()) {
        if let Some(PhysicalNode::Scan {
            ranked: Some(ranked),
            ..
        }) = plan.node_mut(id)
        {
            ranked.fetch = Some(3);
        }
    }
    let (check, detail) = fixture.refused(plan);
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

/// A row cut the query does not write is refused wherever it sits: over the
/// root of a query without `limit`, and below the root of one with it.
#[test]
fn an_unwritten_row_cut_fails_the_row_cut() {
    let cut_over = |plan: &mut PhysicalPlan, input: usize| {
        let properties = plan.properties(input).cloned();
        let cut = plan.add(PhysicalNode::Limit { input, rows: 0 });
        if let Some(properties) = properties {
            plan.set_properties(cut, properties);
        }
        cut
    };

    let unlimited = Fixture::new(
        "query q() { match { $d: Doc $d.title contains \"graph\" } return { $d.slug } }",
        &[],
    );
    let mut plan = unlimited.plan();
    let root = plan.root();
    let cut = cut_over(&mut plan, root);
    plan.set_root(cut);
    let (check, detail) = unlimited.refused(plan);
    assert_eq!(check, "row cut", "{detail}");

    let limited = filtered();
    let mut plan = limited.plan();
    let Some(PhysicalNode::Limit { input, .. }) = plan.node(plan.root()).cloned() else {
        panic!("the limited plan's root is its cut");
    };
    let below = cut_over(&mut plan, input);
    if let Some(PhysicalNode::Limit { input, .. }) = plan.node_mut(plan.root()) {
        *input = below;
    }
    let (check, detail) = limited.refused(plan);
    assert_eq!(check, "row cut", "{detail}");
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
fn a_score_read_from_another_column_fails_the_projection() {
    let fixture = ranked();
    let mut plan = fixture.plan();
    for id in node_ids(&plan, |node| {
        matches!(node, PhysicalNode::Projection { .. })
    }) {
        if let Some(PhysicalNode::Projection { return_exprs, .. }) = plan.node_mut(id) {
            return_exprs[1].expr = IRExpr::PropAccess {
                variable: "d".to_string(),
                property: "_distance".to_string(),
            };
        }
    }
    let (check, _) = fixture.refused(plan);
    assert_eq!(check, "projection");
}

#[test]
fn an_exhausted_budget_is_a_resource_outcome() {
    let fixture = ranked();
    let constants = Bound(&fixture.params);
    let input = AcceptInput {
        checked: &fixture.checked,
        catalog: &fixture.catalog,
        ir: &fixture.ir,
        params: &fixture.params,
        constants: &constants,
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
    match { $d: Doc $d cites $e }
    return { $d.slug }
    order { nearest($d.embedding, $v) }
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
    assert_eq!(
        prefilter.on_empty,
        crate::physical::EmptyEligible::ProvenEmpty
    );
    assert!(prefilter.coverage_admits);
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
    let breaks: [fn(&mut crate::physical::Prefilter); 3] = [
        |prefilter| prefilter.hops[0].edge_type = "Other".to_string(),
        |prefilter| prefilter.on_empty = crate::physical::EmptyEligible::Postfilter,
        |prefilter| prefilter.feeds.push(usize::MAX),
    ];
    for edit in breaks {
        let mut plan = fixture.plan();
        edit(ranked_access(&mut plan).prefilter.as_mut().unwrap());
        let (check, detail) = fixture.refused(plan);
        assert_eq!(check, "declared policy", "{detail}");
    }
}

#[test]
fn a_bm25_scan_with_a_nearest_policy_fails_the_declared_policy() {
    let fixture = ranked();
    let mut plan = fixture.plan();
    ranked_access(&mut plan).policy = Some(crate::physical::NearestPolicy::DEFAULT);
    let (check, detail) = fixture.refused(plan);
    assert_eq!(check, "declared policy", "{detail}");
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
        "query q() { match { $d: Doc { open: true } } return { $d.slug } }",
        "query q($y: I64) { match { $d: Doc $d.year >= $y } return { $d.slug } }",
        "query q() { match { $d: Doc $d.year != 3 and not ($d.title < \"m\" or $d.open) } return { $d.slug } }",
        "query q() { match { $d: Doc $d.rank is null or $d.rank > 2 } return { $d.slug, $d.rank } }",
        "query q() { match { $d: Doc $d.open } return { $d.title as t } order { $d.year desc, $d.title } limit 4 }",
        "query q($t: String) { match { $d: Doc $d.title = $t } return { $d.slug } limit 0 }",
        "query q($q: String) { match { $d: Doc } return { bm25($d.title, $q) as s, $d.slug } order { bm25($d.title, $q), $d.slug desc } limit 2 }",
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
        "query q() { match { $d: Doc $d.title contains \"g\" } return { $d.slug } }",
        "query q() { match { $d: Doc $d.score > 1.5 } return { $d.slug } }",
        "query q() { match { $d: Doc $d.born > date(\"2020-01-01\") } return { $d.slug } }",
        "query q() { match { $d: Doc { tags: \"a\" } } return { $d.slug } }",
        "query q() { match { $d: Doc $d cites $e } return { $d.slug } }",
        "query q() { match { $d: Doc $e: Doc } return { $d.slug } }",
        "query q() { match { $d: Doc not { $d cites $e } } return { $d.slug } }",
        "query q() { match { $d: Doc } return { count($d) as n } }",
        "query q() { match { $d: Doc } return { $d } }",
        "query q() { match { $d: Doc } return { $d.@id } }",
        "query q() { match { $d: Doc } return { $d.slug } order { $d.score } }",
        "query q() { match { $d: Doc search($d.title, \"g\") } return { $d.slug } }",
        "query q($v: Vector(2)) { match { $d: Doc } return { $d.slug } order { nearest($d.embedding, $v) } limit 2 }",
        "query q($q: String) { match { $d: Doc } return { $d.slug } order { rrf(bm25($d.title, $q), bm25($d.title, $q)) } limit 2 }",
    ];
    for query in outside {
        let params = [
            (
                "v",
                Literal::List(vec![Literal::Float(0.0), Literal::Float(1.0)]),
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
        let constants = Bound(&fixture.params);
        let input = AcceptInput {
            checked: &fixture.checked,
            catalog: &fixture.catalog,
            ir: &fixture.ir,
            params: &fixture.params,
            constants: &constants,
            limits,
        };
        let plain = crate::gate::accept_query(&input, &fixture.source, &BOUNDS);
        let explained = crate::gate::accept_query_explained(&input, &fixture.source, &BOUNDS);
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
            "query q() {{ match {{ $d: Doc {} }} return {{ $d.slug }} order {{ $d.year }} limit 10 }}",
            filter.join(" ")
        );
        let fixture = Fixture::new(&query, &[]);
        let traced = fixture.traced();
        let derivation = traced.derivation.clone().unwrap();
        let bytes = serde_json::to_vec(&derivation).unwrap().len();
        let constants = Bound(&fixture.params);
        let input = AcceptInput {
            checked: &fixture.checked,
            catalog: &fixture.catalog,
            ir: &fixture.ir,
            params: &fixture.params,
            constants: &constants,
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
        "query q() {{ match {{ $d: Doc {} }} return {{ $d.slug }} order {{ $d.year }} limit 10 }}",
        filter.join(" ")
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
