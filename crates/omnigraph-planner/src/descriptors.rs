//! The read descriptors of an accepted plan (RFC 0047, "Read descriptors"):
//! one retrieval per ranked `Scan`, per `RankFuse` and per fusion arm, and one
//! metric per return column that holds a retrieval's score. Derived from the
//! plan alone, so a replay of the same plan describes itself with the same
//! ids; nothing is kept beside the plan.

use std::collections::HashMap;

use omnigraph_compiler::ir::IRExpr;
use omnigraph_compiler::query::ast::{DISTANCE_COLUMN, RRF_COLUMN, SCORE_COLUMN};
use omnigraph_compiler::result::{
    EmbeddingCoverage, Metric, ReadDescriptors, Recall, Retrieval, RetrievalKind, RetrievalTarget,
};

use crate::error::PlanError;
use crate::physical::{NodeId, PhysicalNode, PhysicalPlan, RankKind, RankScope};

/// The descriptors of `plan`: retrieval ids in preorder from the root, a
/// fusion before its arms and the arms in query argument order, a node
/// reached twice keeping its first id; metrics in return order.
///
/// # Errors
///
/// An internal error for a plan acceptance should have refused: a fusion arm
/// without exactly its one ranked scan, or a ranked scan with no binding.
pub fn read_descriptors(plan: &PhysicalPlan) -> Result<ReadDescriptors, PlanError> {
    let mut walk = Walk {
        plan,
        produced: HashMap::new(),
        retrievals: Vec::new(),
        ordered: HashMap::new(),
        fused: HashMap::new(),
    };
    walk.visit(plan.root())?;
    let metrics = walk.metrics()?;
    Ok(ReadDescriptors {
        retrievals: walk.retrievals,
        metrics,
    })
}

fn retrieval_kind(kind: RankKind) -> RetrievalKind {
    match kind {
        RankKind::Nearest => RetrievalKind::Nearest,
        RankKind::Bm25 => RetrievalKind::Bm25,
    }
}

struct Walk<'a> {
    plan: &'a PhysicalPlan,
    /// The retrieval each producing node yielded, so a node reached again
    /// reuses its id.
    produced: HashMap<NodeId, usize>,
    retrievals: Vec<Retrieval>,
    /// The query-ordered ranked scans by `(binding, score column)`.
    ordered: HashMap<(String, &'static str), usize>,
    /// The fusions by their primary arm's binding, the binding the fused
    /// score column is written under.
    fused: HashMap<String, usize>,
}

impl Walk<'_> {
    fn node(&self, id: NodeId) -> Result<&PhysicalNode, PlanError> {
        self.plan
            .node(id)
            .ok_or_else(|| PlanError::Internal(format!("physical node {id} is a tombstone")))
    }

    fn push(
        &mut self,
        node: NodeId,
        kind: RetrievalKind,
        target: RetrievalTarget,
        recall: Recall,
    ) -> usize {
        let index = self.retrievals.len();
        self.retrievals.push(Retrieval {
            id: format!("r{index}"),
            kind,
            target,
            recall,
        });
        self.produced.insert(node, index);
        index
    }

    fn visit(&mut self, id: NodeId) -> Result<(), PlanError> {
        if self.produced.contains_key(&id) {
            return Ok(());
        }
        match self.node(id)? {
            PhysicalNode::Scan {
                spec,
                ranked: Some(ranked),
                ..
            } => {
                let binding = spec.binding.clone().ok_or_else(|| {
                    PlanError::Internal(format!("ranked scan {id} has no binding"))
                })?;
                let (recall, embedding_coverage) = match ranked.kind {
                    RankKind::Nearest => {
                        (Recall::Approximate, Some(EmbeddingCoverage::NotComputed))
                    }
                    RankKind::Bm25 => (Recall::Exact, None),
                };
                let scope = ranked.scope;
                let column = ranked.kind.score().0;
                let index = self.push(
                    id,
                    retrieval_kind(ranked.kind),
                    RetrievalTarget::Ranked {
                        binding: binding.clone(),
                        property: ranked.property.clone(),
                        embedding_coverage,
                    },
                    recall,
                );
                if scope == RankScope::Order {
                    self.ordered.insert((binding, column), index);
                }
            }
            PhysicalNode::RankFuse { arms, .. } => {
                let arms = arms.clone();
                let index = self.push(
                    id,
                    RetrievalKind::Rrf,
                    RetrievalTarget::Fused { arms: Vec::new() },
                    Recall::Exact,
                );
                let mut arm_ids = Vec::with_capacity(arms.len());
                let mut recall = Recall::Exact;
                for arm in &arms {
                    let before = self.retrievals.len();
                    self.visit(arm.input)?;
                    let [ranked] = &self.retrievals[before..] else {
                        return Err(PlanError::Internal(format!(
                            "an arm of fusion {id} holds {} retrievals, not its one ranked scan",
                            self.retrievals.len() - before
                        )));
                    };
                    let ranks_its_binding = matches!(
                        &ranked.target,
                        RetrievalTarget::Ranked { binding, .. } if *binding == arm.binding
                    );
                    if ranked.kind != retrieval_kind(arm.kind) || !ranks_its_binding {
                        return Err(PlanError::Internal(format!(
                            "an arm of fusion {id} ranks another binding or kind than it declares"
                        )));
                    }
                    if ranked.recall == Recall::Approximate {
                        recall = Recall::Approximate;
                    }
                    arm_ids.push(ranked.id.clone());
                }
                let fusion = &mut self.retrievals[index];
                fusion.target = RetrievalTarget::Fused { arms: arm_ids };
                fusion.recall = recall;
                self.fused.insert(arms[0].binding.clone(), index);
            }
            node => {
                for input in node.inputs() {
                    self.visit(input)?;
                }
            }
        }
        Ok(())
    }

    /// One metric per column of the query's return projection that reads a
    /// score column a retrieval of the plan writes: the projection nearest
    /// the root. A graph whose schema predates the reservation of `_distance`,
    /// `_score` and `_rrf` may hold a stored property of that name, which no
    /// retrieval writes and which is no metric.
    fn metrics(&self) -> Result<Vec<Metric>, PlanError> {
        let Some(return_exprs) = self.projection(self.plan.root())? else {
            return Ok(Vec::new());
        };
        let mut metrics = Vec::new();
        for projection in return_exprs {
            let IRExpr::PropAccess {
                variable, property, ..
            } = &projection.expr
            else {
                continue;
            };
            let producer = match property.as_str() {
                DISTANCE_COLUMN => self.ordered.get(&(variable.clone(), DISTANCE_COLUMN)),
                SCORE_COLUMN => self.ordered.get(&(variable.clone(), SCORE_COLUMN)),
                RRF_COLUMN => self.fused.get(variable),
                _ => continue,
            };
            let Some(index) = producer else {
                continue;
            };
            metrics.push(Metric {
                column: projection.column.clone(),
                retrieval_id: self.retrievals[*index].id.clone(),
            });
        }
        Ok(metrics)
    }

    /// The return list nearest `id`: the first `Projection` or `Aggregate` in
    /// preorder below it. A `return distinct` plans an `Aggregate` whose group
    /// keys may hold a score; an aggregate's own outputs never do (`T9`).
    fn projection(
        &self,
        id: NodeId,
    ) -> Result<Option<&[omnigraph_compiler::ir::IRProjection]>, PlanError> {
        match self.node(id)? {
            PhysicalNode::Projection { return_exprs, .. }
            | PhysicalNode::Aggregate { return_exprs, .. } => Ok(Some(return_exprs)),
            node => {
                for input in node.inputs() {
                    if let Some(found) = self.projection(input)? {
                        return Ok(Some(found));
                    }
                }
                Ok(None)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use omnigraph_compiler::ir::{IROp, IROrdering, IRProjection, QueryIR};
    use omnigraph_compiler::{ExprType, PropType, SYSTEM_COLUMNS_V3, ScalarType};

    use super::*;
    use crate::fixture_bounds::BOUNDS;
    use crate::operation::TableRef;
    use crate::source::{MemorySource, NodeTypeSpec};

    /// A schema older than the reservation of the score column names may
    /// store a property called `_score`: returning it reads the stored value,
    /// which no retrieval writes, so the read has no metric and still runs.
    #[test]
    fn a_stored_property_named_like_a_score_is_no_metric() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(SYSTEM_COLUMNS_V3.id, DataType::Utf8, false),
            Field::new("slug", DataType::Utf8, true),
            Field::new(SCORE_COLUMN, DataType::Float32, true),
        ]));
        let source = MemorySource::default().with_node_type(
            "Doc",
            NodeTypeSpec {
                table: TableRef::dataset("node:Doc", "node/Doc", None),
                version: Some(1),
                columns: SYSTEM_COLUMNS_V3,
                schema,
                key: vec!["slug".to_string()],
                object_columns: vec![
                    SYSTEM_COLUMNS_V3.id.to_string(),
                    "slug".to_string(),
                    SCORE_COLUMN.to_string(),
                ],
                object_fields: vec![
                    Field::new("@id", DataType::Utf8, false),
                    Field::new("slug", DataType::Utf8, true),
                    Field::new(SCORE_COLUMN, DataType::Float32, true),
                ]
                .into(),
                row_count: Some(3),
            },
        );
        let typed = |scalar| ExprType::from_prop(&PropType::scalar(scalar, true));
        let prop = |property: &str, scalar| IRExpr::PropAccess {
            variable: "d".to_string(),
            property: property.to_string(),
            ty: typed(scalar),
        };
        let query = QueryIR {
            name: "q".to_string(),
            params: vec![],
            pipeline: vec![IROp::NodeScan {
                variable: "d".to_string(),
                type_name: "Doc".to_string(),
                filters: vec![],
            }],
            return_exprs: vec![IRProjection {
                expr: prop(SCORE_COLUMN, ScalarType::F32),
                alias: None,
                column: format!("d.{SCORE_COLUMN}"),
                ty: typed(ScalarType::F32),
            }],
            distinct: false,
            order_by: vec![IROrdering {
                expr: prop("slug", ScalarType::String),
                descending: false,
            }],
            limit: Some(2),
        };
        let plan = futures::executor::block_on(crate::plan_query(&query, &source, &BOUNDS))
            .expect("the query plans");
        assert_eq!(read_descriptors(&plan).unwrap(), ReadDescriptors::default());
    }
}
