//! The ranking an accepted plan orders its rows by (RFC 0047, "The ranking a
//! read reports"): the search function, the returned columns that hold its
//! score, and the `nearest` scans whose run decides whether the search was
//! exhaustive. Derived from the plan alone, so a replay of the plan reports
//! the ranking its own run establishes; nothing is kept beside the plan.

use omnigraph_compiler::ir::{IRExpr, IRProjection};
use omnigraph_compiler::query::ast::RRF_COLUMN;
use omnigraph_compiler::result::{Ranking, SearchFunction};

use crate::error::PlanError;
use crate::physical::{NodeId, PhysicalNode, PhysicalPlan, RankKind, RankScope};

/// What the plan says of its ranking; the run that executes it says whether
/// the search was exhaustive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedRanking {
    by: SearchFunction,
    score_columns: Vec<String>,
    nearest_scans: Vec<NodeId>,
}

impl PlannedRanking {
    pub fn by(&self) -> SearchFunction {
        self.by
    }

    pub fn score_columns(&self) -> &[String] {
        &self.score_columns
    }

    /// The ranked `nearest` scans: the ordered scan of a `nearest` order,
    /// or the `nearest` arms of a fusion, in node order. A `bm25` scan has
    /// no candidate window, so a `bm25` ranking names none.
    pub fn nearest_scans(&self) -> &[NodeId] {
        &self.nearest_scans
    }

    /// The ranking a run reports, given `exact`: whether the last attempt of
    /// each `nearest` scan in the run ranked every row it admitted exactly.
    pub fn ranking(self, exact: impl Fn(NodeId) -> bool) -> Ranking {
        Ranking {
            by: self.by,
            exhaustive: self.nearest_scans.iter().all(|scan| exact(*scan)),
            score_columns: self.score_columns,
        }
    }
}

/// The ranking `plan` orders by, `None` when its order is no search.
/// Acceptance admits one ranked scan in the query's order, or one fusion of
/// two arms, and nothing else.
///
/// # Errors
///
/// An internal error for a plan acceptance should have refused: ranked scans
/// of another shape, or a ranked scan with no binding.
pub fn planned_ranking(plan: &PhysicalPlan) -> Result<Option<PlannedRanking>, PlanError> {
    let mut fusions = Vec::new();
    let mut ordered = Vec::new();
    let mut arms = Vec::new();
    for (id, node) in plan.live() {
        match node {
            PhysicalNode::RankFuse { arms, .. } => fusions.push(arms),
            PhysicalNode::Scan {
                spec,
                ranked: Some(ranked),
                ..
            } => {
                let scan = (id, ranked.kind, spec.binding.as_deref());
                match ranked.scope {
                    RankScope::Order => ordered.push(scan),
                    RankScope::Primary | RankScope::Secondary => arms.push(scan),
                }
            }
            _ => {}
        }
    }
    let (by, binding, column, nearest_scans) =
        match (fusions.as_slice(), ordered.as_slice(), arms.as_slice()) {
            ([], [], []) => return Ok(None),
            ([], [(id, kind, binding)], []) => (
                search_function(*kind),
                binding.ok_or_else(|| {
                    PlanError::Internal(format!("ranked scan {id} has no binding"))
                })?,
                kind.score().0,
                if *kind == RankKind::Nearest {
                    vec![*id]
                } else {
                    Vec::new()
                },
            ),
            ([[primary, _]], [], [_, _]) => (
                SearchFunction::Rrf,
                primary.binding.as_str(),
                RRF_COLUMN,
                arms.iter()
                    .filter(|(_, kind, _)| *kind == RankKind::Nearest)
                    .map(|(id, ..)| *id)
                    .collect(),
            ),
            _ => {
                return Err(PlanError::Internal(format!(
                    "the plan holds {} fusions, {} ordered ranked scans and {} fusion arms; \
                     acceptance admits one ordered scan or one fusion of two arms",
                    fusions.len(),
                    ordered.len(),
                    arms.len()
                )));
            }
        };
    let score_columns = return_list(plan, plan.root())?
        .unwrap_or_default()
        .iter()
        .filter(|projection| {
            matches!(
                &projection.expr,
                IRExpr::PropAccess { variable, property, .. }
                    if variable == binding && property == column
            )
        })
        .map(|projection| projection.column.clone())
        .collect();
    Ok(Some(PlannedRanking {
        by,
        score_columns,
        nearest_scans,
    }))
}

fn search_function(kind: RankKind) -> SearchFunction {
    match kind {
        RankKind::Nearest => SearchFunction::Nearest,
        RankKind::Bm25 => SearchFunction::Bm25,
    }
}

/// The query's return list: the first `Projection` or `Aggregate` in
/// preorder below `id`. A `return distinct` plans an `Aggregate` whose group
/// keys may hold the score; an aggregate's own outputs never do (`T9`). A
/// graph whose schema predates the reservation of `_distance`, `_score` and
/// `_rrf` may store a property of that name: returning it reads the stored
/// value, which is the ranking's score only on the ranked binding.
fn return_list(plan: &PhysicalPlan, id: NodeId) -> Result<Option<&[IRProjection]>, PlanError> {
    let node = plan
        .node(id)
        .ok_or_else(|| PlanError::Internal(format!("physical node {id} is a tombstone")))?;
    match node {
        PhysicalNode::Projection { return_exprs, .. }
        | PhysicalNode::Aggregate { return_exprs, .. } => Ok(Some(return_exprs)),
        node => {
            for input in node.inputs() {
                if let Some(found) = return_list(plan, input)? {
                    return Ok(Some(found));
                }
            }
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use omnigraph_compiler::ir::{IROp, IROrdering, QueryIR};
    use omnigraph_compiler::query::ast::{DISTANCE_COLUMN, Literal, SCORE_COLUMN};
    use omnigraph_compiler::{ExprType, PropType, SYSTEM_COLUMNS_V3, ScalarType};

    use super::*;
    use crate::fixture_bounds::BOUNDS;
    use crate::operation::TableRef;
    use crate::source::{MemorySource, NodeTypeSpec};

    /// A schema older than the reservation of the score column names may
    /// store a property called `_score`. Beside a `nearest` order, returning
    /// it reads the stored value, which is no score of the ranking, while the
    /// returned distance is.
    #[test]
    fn a_stored_property_named_like_a_score_is_no_score_column() {
        let embedding =
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2);
        let fields = |id: &str| {
            vec![
                Field::new(id, DataType::Utf8, false),
                Field::new("slug", DataType::Utf8, true),
                Field::new(SCORE_COLUMN, DataType::Float32, true),
                Field::new("embedding", embedding.clone(), true),
            ]
        };
        let source = MemorySource::default().with_node_type(
            "Doc",
            NodeTypeSpec {
                table: TableRef::dataset("node:Doc", "node/Doc", None),
                version: Some(1),
                columns: SYSTEM_COLUMNS_V3,
                schema: Arc::new(Schema::new(fields(SYSTEM_COLUMNS_V3.id))),
                key: vec!["slug".to_string()],
                object_columns: vec![
                    SYSTEM_COLUMNS_V3.id.to_string(),
                    "slug".to_string(),
                    SCORE_COLUMN.to_string(),
                    "embedding".to_string(),
                ],
                object_fields: fields("@id").into(),
                row_count: Some(3),
            },
        );
        let typed = |scalar, nullable| ExprType::from_prop(&PropType::scalar(scalar, nullable));
        let returned = |property: &str, nullable| IRProjection {
            expr: IRExpr::PropAccess {
                variable: "d".to_string(),
                property: property.to_string(),
                ty: typed(ScalarType::F32, nullable),
            },
            alias: None,
            column: format!("d.{property}"),
            ty: typed(ScalarType::F32, nullable),
        };
        let query = QueryIR {
            name: "q".to_string(),
            params: vec![],
            pipeline: vec![IROp::NodeScan {
                variable: "d".to_string(),
                type_name: "Doc".to_string(),
                filters: vec![],
            }],
            return_exprs: vec![
                returned(SCORE_COLUMN, true),
                returned(DISTANCE_COLUMN, false),
            ],
            distinct: false,
            order_by: vec![IROrdering {
                expr: IRExpr::Nearest {
                    variable: "d".to_string(),
                    property: "embedding".to_string(),
                    query: Box::new(IRExpr::Literal(
                        Literal::List(vec![Literal::Float(1.0), Literal::Float(0.0)]),
                        ExprType::Value {
                            scalar: ScalarType::F64,
                            list: true,
                            nullable: false,
                        },
                    )),
                    ty: typed(ScalarType::F32, false),
                },
                descending: false,
            }],
            limit: Some(2),
        };
        let plan = futures::executor::block_on(crate::plan_query(&query, &source, &BOUNDS))
            .expect("the query plans");
        let planned = planned_ranking(&plan)
            .unwrap()
            .expect("a nearest order ranks");
        assert_eq!(planned.by(), SearchFunction::Nearest);
        assert_eq!(planned.score_columns(), ["d._distance"]);
        let &[scan] = planned.nearest_scans() else {
            panic!("one nearest scan: {:?}", planned.nearest_scans());
        };
        assert!(
            plan.node(scan).and_then(PhysicalNode::ranked).is_some(),
            "the named node is the ranked scan"
        );
        let approximate = planned.clone().ranking(|_| false);
        assert!(!approximate.exhaustive);
        assert!(planned.ranking(|id| id == scan).exhaustive);
    }
}
