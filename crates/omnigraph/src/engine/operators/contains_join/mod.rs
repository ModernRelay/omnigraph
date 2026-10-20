//! `ContainsJoinExec`: the pairs of a `ContainsJoin` node whose right text holds
//! the left needle: the left collected, the right scan's `RuntimeFilterSlot`
//! filled, each right batch paired (`pairing`) through one `PairBuffer`.

mod needle_rows;
mod pairing;

use std::fmt;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::future::BoxFuture;
use omnigraph_compiler::ir::{IRExpr, ParamMap};
use omnigraph_compiler::query::ast::{BinaryOp, CompOp};
use omnigraph_compiler::types::{ExprType, ScalarType};
use omnigraph_planner::ContainsJoinFields;

use self::needle_rows::ContainsColumns;
use self::pairing::Pairing;
use super::memory::WorkMemory;
use super::pair_buffer::PairBuffer;
use super::shape::{BatchSender, CollectedSide, Flow, Streamed, Streaming, work};
use super::{Filled, Needles, RuntimeFilterSlot, external, joined_schema};
use crate::error::{OmniError, Result};

/// The distinct needles the join's fill left for the right scan's filter.
const NEEDLES_METRIC: &str = "runtime_filter_needles";

/// Its explain detail `contains_join=aho_corasick` is the plan's choice; the
/// `contains_join_matcher` counter, set at the first right row to reach the
/// join, says whether the run paired through it.
#[derive(Debug)]
pub(crate) struct ContainsJoinExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    haystack: (String, String),
    needle: (String, String),
    columns: ContainsColumns,
    /// The `contains` conjunct as the plan's fields spell it, then the
    /// residual ones (`residual`): all tested when every pair is taken.
    filters: Vec<IRExpr>,
    params: Arc<ParamMap>,
    runtime_filter: Arc<RuntimeFilterSlot>,
    shape: Streaming,
    metrics: ExecutionPlanMetricsSet,
}

impl ContainsJoinExec {
    /// `runtime_filter` is the slot the right side's scan reads, filled here
    /// from the collected left side before the right side executes.
    pub(crate) fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        fields: ContainsJoinFields<'_>,
        params: Arc<ParamMap>,
        runtime_filter: Arc<RuntimeFilterSlot>,
    ) -> Result<Self> {
        let operand = |expr: &IRExpr, pair: (&str, &str)| {
            matches!(expr, IRExpr::PropAccess {
                variable, property,
                ty: ExprType::Value { scalar: ScalarType::String, list: false, .. },
            } if (variable.as_str(), property.as_str()) == pair)
        };
        let valid = matches!(fields.conjunct,
            IRExpr::Binary { left, op: BinaryOp::Compare(CompOp::StringContains), right, ty: _ }
            if operand(left, fields.haystack) && operand(right, fields.needle));
        if !valid {
            return Err(OmniError::manifest_internal(
                "contains join conjunct disagrees with its matcher columns",
            ));
        }
        let schema = joined_schema(&left.schema(), &right.schema())?;
        let column = |(binding, property): (&str, &str)| format!("{binding}.{property}");
        let columns = ContainsColumns::checked(
            column(fields.haystack),
            column(fields.needle),
            &left.schema(),
            &right.schema(),
        )?;
        let mut filters = Vec::with_capacity(fields.residual.len() + 1);
        filters.push(fields.conjunct());
        filters.extend(fields.residual.iter().cloned());
        Ok(Self {
            left,
            right,
            haystack: (fields.haystack.0.to_string(), fields.haystack.1.to_string()),
            needle: (fields.needle.0.to_string(), fields.needle.1.to_string()),
            columns,
            filters,
            params,
            runtime_filter,
            shape: Streaming::new(schema),
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    fn fields(&self) -> ContainsJoinFields<'_> {
        ContainsJoinFields {
            haystack: (&self.haystack.0, &self.haystack.1),
            needle: (&self.needle.0, &self.needle.1),
            conjunct: &self.filters[0],
            residual: self.residual(),
        }
    }

    /// The conjuncts tested on the pairs the needle rows find.
    fn residual(&self) -> &[IRExpr] {
        &self.filters[1..]
    }
}

impl DisplayAs for ContainsJoinExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let filters: Vec<String> = self.filters.iter().map(ToString::to_string).collect();
        write!(
            f,
            "ContainsJoinExec: {}, runtime_filter={}, contains_join=aho_corasick",
            filters.join(" AND "),
            self.runtime_filter.display()
        )
    }
}

impl ExecutionPlan for ContainsJoinExec {
    fn name(&self) -> &str {
        "ContainsJoinExec"
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.shape.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        assert_eq!(children.len(), 2, "ContainsJoinExec has two children");
        let right = children.pop().expect("right child");
        let left = children.pop().expect("left child");
        Ok(Arc::new(
            Self::try_new(
                left,
                right,
                self.fields(),
                Arc::clone(&self.params),
                Arc::clone(&self.runtime_filter),
            )
            .map_err(external)?,
        ))
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        assert_eq!(partition, 0, "ContainsJoinExec has one partition");
        let memory = work(Arc::clone(&ctx), "ContainsJoinExec", &self.metrics)?;
        memory.metric("input_rows", 0);
        memory.metric("input_batches", 0);
        memory.metric(NEEDLES_METRIC, 0);
        let right = Arc::clone(&self.right);
        let body = Matches {
            left: Arc::clone(&self.left),
            declared: self.schema(),
            filters: self.filters.clone(),
            residual: self.residual().to_vec(),
            params: Arc::clone(&self.params),
            runtime: Arc::clone(&self.runtime_filter),
            columns: self.columns.clone(),
            opened: None,
        };
        Ok(self
            .shape
            .produce(memory, &self.metrics, move || right.execute(0, ctx), body))
    }
}

/// The per-batch body over the right side: the left collected and the right
/// scan's runtime filter filled before the right executes, each right batch
/// paired through the buffer the way the first non-empty one decides.
struct Matches {
    left: Arc<dyn ExecutionPlan>,
    declared: SchemaRef,
    filters: Vec<IRExpr>,
    residual: Vec<IRExpr>,
    params: Arc<ParamMap>,
    runtime: Arc<RuntimeFilterSlot>,
    columns: ContainsColumns,
    opened: Option<Opened>,
}

/// What the open left the body: the collected left under its reservation,
/// the buffer over it, the needles the fill left for the pairing decision,
/// and the pairing once decided.
struct Opened {
    left: RecordBatch,
    _held: WorkMemory,
    buffer: PairBuffer,
    needles: Option<Arc<Needles>>,
    pairing: Option<Pairing>,
}

impl Streamed for Matches {
    fn open<'a>(&'a mut self, memory: &'a Arc<WorkMemory>) -> BoxFuture<'a, DfResult<bool>> {
        Box::pin(async move {
            let side = CollectedSide::collect(&self.left, memory, "contains join left").await?;
            if side.batch.num_rows() == 0 {
                return Ok(false);
            }
            let size = memory.ctx.session_config().batch_size();
            let buffer = PairBuffer::new(
                side.batch.clone(),
                Arc::clone(&self.declared),
                self.filters.clone(),
                Arc::clone(&self.params),
                size,
                memory,
            )?;
            let needles = match self.runtime.fill(&side.batch, memory)? {
                Filled::Needles(needles) => {
                    memory.metric(NEEDLES_METRIC, needles.len());
                    Some(needles)
                }
                Filled::NoNeedles => return Ok(false),
                Filled::Inert(needles) => needles,
            };
            self.opened = Some(Opened {
                left: side.batch,
                _held: side.held,
                buffer,
                needles,
                pairing: None,
            });
            Ok(true)
        })
    }

    fn batch<'a>(
        &'a mut self,
        batch: RecordBatch,
        memory: &'a Arc<WorkMemory>,
        sender: &'a BatchSender,
    ) -> BoxFuture<'a, DfResult<Flow>> {
        Box::pin(async move {
            memory.metric("input_rows", batch.num_rows());
            memory.metric("input_batches", 1);
            if batch.num_rows() == 0 {
                return Ok(Flow::Continue);
            }
            let opened = self.opened.as_mut().ok_or_else(|| {
                DataFusionError::Internal(
                    "contains join paired before its left was collected".into(),
                )
            })?;
            let pairing = match &mut opened.pairing {
                Some(pairing) => pairing,
                slot @ None => {
                    let decided = Pairing::decide(
                        &opened.left,
                        &self.columns,
                        opened.needles.take(),
                        memory,
                    )?;
                    if matches!(decided, Pairing::NeedleRows(_)) {
                        opened.buffer.set_filters(self.residual.clone());
                    }
                    slot.insert(decided)
                }
            };
            opened.buffer.start(&batch)?;
            pairing
                .pair(&batch, &mut opened.buffer, memory, sender)
                .await?;
            opened.buffer.flush(sender).await?;
            Ok(Flow::Continue)
        })
    }

    fn finish<'a>(
        &'a mut self,
        _memory: &'a Arc<WorkMemory>,
        sender: &'a BatchSender,
    ) -> BoxFuture<'a, DfResult<()>> {
        Box::pin(async move {
            match self.opened.as_mut() {
                Some(opened) => opened.buffer.finish(sender).await,
                None => Ok(()),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::operators::fixtures::{context, pairs, text_pairs, texts};

    /// A join of `(mid, number)` matters and `(pid, text)` passage batches on
    /// `$p.text contains $m.number`, with no residual conjunct.
    fn join(matters: &[(&str, &str)], passages: &[&[(&str, &str)]]) -> ContainsJoinExec {
        let filter = Arc::new(RuntimeFilterSlot::new("p", "text", "m.number".into()));
        ContainsJoinExec::try_new(
            texts(["m.mid", "m.number"], &[matters]),
            texts(["p.pid", "p.text"], passages),
            ContainsJoinFields {
                haystack: ("p", "text"),
                needle: ("m", "number"),
                conjunct: &IRExpr::comparison(
                    IRExpr::PropAccess {
                        variable: "p".into(),
                        property: "text".into(),
                        ty: omnigraph_compiler::types::ExprType::from_prop(
                            &omnigraph_compiler::types::PropType::scalar(
                                omnigraph_compiler::types::ScalarType::String,
                                false,
                            ),
                        ),
                    },
                    omnigraph_compiler::query::ast::CompOp::StringContains,
                    IRExpr::PropAccess {
                        variable: "m".into(),
                        property: "number".into(),
                        ty: omnigraph_compiler::types::ExprType::from_prop(
                            &omnigraph_compiler::types::PropType::scalar(
                                omnigraph_compiler::types::ScalarType::String,
                                false,
                            ),
                        ),
                    },
                ),
                residual: &[],
            },
            Arc::new(ParamMap::new()),
            filter,
        )
        .unwrap()
    }

    /// The `(m.mid, p.pid)` pairs of each output of the join, `size` rows a
    /// batch.
    async fn outputs(
        matters: &[(&str, &str)],
        passages: &[&[(&str, &str)]],
        size: usize,
    ) -> Vec<Vec<(String, String)>> {
        let (_, ctx) = context(1 << 20, size);
        let stream = join(matters, passages).execute(0, ctx).unwrap();
        let batches = datafusion::physical_plan::common::collect(stream)
            .await
            .unwrap();
        text_pairs(&batches)
    }

    /// Found pairs flush at the batch size even inside `p0`'s one text: each
    /// output holds `size` pairs in right row order, and the rest leaves last.
    #[tokio::test]
    async fn found_pairs_flush_at_the_batch_size_inside_one_text() {
        let matters = [
            ("a0", "aa"),
            ("a1", "aa"),
            ("a2", "aa"),
            ("a3", "aa"),
            ("c0", "cc"),
            ("c1", "cc"),
            ("c2", "cc"),
            ("b", "bb"),
        ];
        let first = [("p0", "aa"), ("p1", "bb"), ("p2", "bb, then a longer text")];
        let second = [("p3", "cc"), ("p4", "bb"), ("p5", "bb")];
        assert_eq!(
            outputs(&matters, &[&first, &second], 3).await,
            [
                pairs(&[("a0", "p0"), ("a1", "p0"), ("a2", "p0")]),
                pairs(&[("a3", "p0"), ("b", "p1"), ("b", "p2")]),
                pairs(&[("c0", "p3"), ("c1", "p3"), ("c2", "p3")]),
                pairs(&[("b", "p4"), ("b", "p5")]),
            ]
        );
    }
}
