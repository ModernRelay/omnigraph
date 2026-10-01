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
}
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
        let mut source = MemorySource::default();
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
