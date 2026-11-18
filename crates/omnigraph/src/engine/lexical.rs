//! `match_terms` as a Boolean operand, and the exact `bm25` of a `Score`
//! node, answered by the exact lexical scan ([`super::lexical_scan`]) inside
//! the operator of their plan node.
//!
//! Every full-text field has a profile, so every lexical call is answered by
//! the exact scan, at every index state, and no full-text index is read
//! (the GQ 3 RFC, section 4). A `Filter` or `Let` reads each
//! call as a Boolean column, the membership of the row's identity in the
//! call's matches, which a null or token-empty document never joins; a
//! `Score` appends each matching row's `bm25_v1` score. The operator runs the
//! scan when its consumer first polls it, before it pulls its input, so a
//! node that never runs (below a `Limit` of 0, say) scans nothing. The scan
//! reads the field's table at the pinned snapshot under the engine's scan
//! tuning, charges what it holds to the query's pool, charges its work to
//! the read's work budget, and records the rows it read and the tokens it
//! analyzed on its node's operator, which the execution report carries.

use std::collections::HashSet;
use std::sync::{Arc, Mutex, OnceLock};

use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::StreamExt;
use omnigraph_compiler::catalog::Catalog;
use omnigraph_compiler::ir::{IRExpr, ParamMap};
use omnigraph_compiler::types::{ExprType, PropType, ScalarType};
use omnigraph_planner::{NodeId, PhysicalNode, PhysicalPlan};

use super::operators::memory::WorkMemory;
use super::{OmniError, Result, Snapshot};

/// The prefix of a membership column an operator adds to its input batch
/// while it evaluates; no GQ alias or `binding.property` name starts with it.
const MEMBERSHIP: &str = "~lexical:";

/// The metric counting the rows an operator's exact lexical scans read.
pub(crate) const SCAN_ROWS_METRIC: &str = "lexical_scan_rows";
/// The metric counting the tokens an operator's exact lexical scans analyzed.
pub(crate) const SCAN_TOKENS_METRIC: &str = "lexical_scan_tokens";

/// What an exact lexical scan reads: the pinned snapshot, the accepted
/// catalog and the bound parameters. `None` snapshot for a lowering with no
/// graph storage, which refuses when the scan runs.
#[derive(Clone)]
pub(crate) struct LexicalSource {
    snapshot: Option<Snapshot>,
    catalog: Arc<Catalog>,
    params: Arc<ParamMap>,
}

impl LexicalSource {
    pub(super) fn new(
        snapshot: Option<&Snapshot>,
        catalog: &Arc<Catalog>,
        params: &Arc<ParamMap>,
    ) -> Self {
        Self {
            snapshot: snapshot.cloned(),
            catalog: Arc::clone(catalog),
            params: Arc::clone(params),
        }
    }

    fn snapshot(&self) -> Result<&Snapshot> {
        self.snapshot
            .as_ref()
            .ok_or_else(|| super::no_graph_storage("a full-text call"))
    }

    /// `exprs` of node `id` with every full-text call read as a membership
    /// column, and the columns the operator adds and answers; `None` when no
    /// expression holds a call.
    pub(super) fn columns(
        &self,
        plan: &PhysicalPlan,
        id: NodeId,
        exprs: &[IRExpr],
    ) -> Result<(Vec<IRExpr>, Option<Arc<LexicalColumns>>)> {
        let mut columns: Vec<LexicalColumn> = Vec::new();
        let mut failed = None;
        let mut rewrite = |call: &IRExpr| -> Option<IRExpr> {
            if failed.is_some() {
                return None;
            }
            let hidden = match columns.iter().position(|column| column.call == *call) {
                Some(position) => columns[position].hidden.clone(),
                None => {
                    let column = (|| {
                        let type_name = call_type(plan, id, call).ok_or_else(|| {
                            OmniError::manifest_internal(format!(
                                "the full-text call `{call}` reads a binding no input of node {id} introduces"
                            ))
                        })?;
                        let binding = call_binding(call).ok_or_else(|| {
                            OmniError::manifest_internal(format!(
                                "the full-text call `{call}` does not search a property of a binding"
                            ))
                        })?;
                        Ok::<_, OmniError>(LexicalColumn {
                            call: call.clone(),
                            type_name,
                            hidden: format!("{MEMBERSHIP}{}", columns.len()),
                            id_column: format!("{binding}.{}", self.catalog.system_columns.id),
                            ids: OnceLock::new(),
                        })
                    })();
                    match column {
                        Ok(column) => {
                            let hidden = column.hidden.clone();
                            columns.push(column);
                            hidden
                        }
                        Err(error) => {
                            failed = Some(error);
                            return None;
                        }
                    }
                }
            };
            Some(IRExpr::AliasRef(hidden, membership_type()))
        };
        let exprs: Vec<IRExpr> = exprs
            .iter()
            .map(|expr| substitute(expr, &mut rewrite))
            .collect();
        if let Some(error) = failed {
            return Err(error);
        }
        let columns = (!columns.is_empty()).then(|| {
            Arc::new(LexicalColumns {
                source: self.clone(),
                columns,
                held: Mutex::new(None),
            })
        });
        Ok((exprs, columns))
    }

    /// The exact `bm25` of the `Score` node of `type_name` whose retained
    /// predicate is `call`, answered when its operator runs.
    pub(super) fn score(&self, type_name: &str, call: &IRExpr) -> Arc<LexicalScore> {
        Arc::new(LexicalScore {
            source: self.clone(),
            type_name: type_name.to_string(),
            call: call.clone(),
            scorer: OnceLock::new(),
        })
    }
}

/// The membership columns one operator reads, answered once when it first
/// runs, added to each input batch while it evaluates and never emitted.
pub(crate) struct LexicalColumns {
    source: LexicalSource,
    columns: Vec<LexicalColumn>,
    /// The charge the answered sets hold for the operator's lifetime.
    held: Mutex<Option<WorkMemory>>,
}

struct LexicalColumn {
    call: IRExpr,
    type_name: String,
    hidden: String,
    /// The `binding.<id>` column of the batch holding the row's identity.
    id_column: String,
    ids: OnceLock<Arc<HashSet<String>>>,
}

impl std::fmt::Debug for LexicalColumns {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(self.columns.iter().map(|column| column.call.to_string()))
            .finish()
    }
}

impl LexicalColumns {
    /// Run the exact lexical scan of every column not yet answered, charging
    /// the match sets to `memory`, which also counts the rows read and the
    /// tokens analyzed.
    pub(crate) async fn answer(&self, memory: WorkMemory) -> Result<()> {
        if self.columns.iter().all(|column| column.ids.get().is_some()) {
            return Ok(());
        }
        let snapshot = self.source.snapshot()?;
        for column in &self.columns {
            if column.ids.get().is_some() {
                continue;
            }
            let (property, query) = super::lexical_scan::query_of_call(
                &self.source.catalog,
                &column.type_name,
                &column.call,
                &self.source.params,
            )?;
            let ids = super::lexical_scan::members(
                snapshot,
                &self.source.catalog,
                &column.type_name,
                &property,
                &query,
                &memory,
            )
            .await?;
            let _ = column.ids.set(Arc::new(ids));
        }
        *super::operators::memory::locked(&self.held) = Some(memory);
        Ok(())
    }

    /// `batch` with each membership column appended: true where the row's
    /// identity is among the call's matches, false elsewhere.
    pub(crate) fn widen(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let schema = batch.schema();
        let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
        let mut arrays: Vec<ArrayRef> = batch.columns().to_vec();
        for column in &self.columns {
            let set = column.ids.get().ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "the full-text call `{}` was evaluated before its scan ran",
                    column.call
                ))
            })?;
            let ids = batch
                .column_by_name(&column.id_column)
                .and_then(|array| array.as_any().downcast_ref::<StringArray>())
                .ok_or_else(|| {
                    OmniError::manifest_internal(format!(
                        "the full-text call `{}` reads `{}`, which the batch does not carry",
                        column.call, column.id_column
                    ))
                })?;
            let member: BooleanArray = (0..ids.len())
                .map(|row| Some(ids.is_valid(row) && set.contains(ids.value(row))))
                .collect();
            fields.push(Field::new(&column.hidden, DataType::Boolean, false));
            arrays.push(Arc::new(member));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .map_err(OmniError::arrow_internal)
    }
}

/// The exact `bm25` of one `Score` node: its field corpus's `bm25_v1`
/// statistics, read once when the operator first runs, which score each row
/// of its population from the row's own value.
pub(crate) struct LexicalScore {
    source: LexicalSource,
    type_name: String,
    call: IRExpr,
    scorer: OnceLock<Arc<super::lexical_scan::Bm25Scorer>>,
}

impl std::fmt::Debug for LexicalScore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "bm25 of {} over `{}`", self.type_name, self.call)
    }
}

impl LexicalScore {
    /// Read the field corpus once, its work charged to `memory`.
    pub(crate) async fn answer(&self, memory: WorkMemory) -> Result<()> {
        if self.scorer.get().is_some() {
            return Ok(());
        }
        let snapshot = self.source.snapshot()?;
        let (property, query) = super::lexical_scan::query_of_call(
            &self.source.catalog,
            &self.type_name,
            &self.call,
            &self.source.params,
        )?;
        let scorer = super::lexical_scan::bm25(
            snapshot,
            &self.source.catalog,
            &self.type_name,
            &property,
            query,
            &memory,
        )
        .await?;
        let _ = self.scorer.set(Arc::new(scorer));
        Ok(())
    }

    /// The corpus's scorer. An error before the corpus was read.
    pub(crate) fn scorer(&self) -> Result<&super::lexical_scan::Bm25Scorer> {
        self.scorer
            .get()
            .map(|scorer| scorer.as_ref())
            .ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "the exact `bm25` over `{}` was read before its scan ran",
                    self.call
                ))
            })
    }
}

/// `input`, preceded by `answer`: the consumer's first poll runs `answer`
/// before the input is pulled, and an error it returns is the stream's.
pub(crate) fn answered_first<F>(
    input: SendableRecordBatchStream,
    answer: F,
) -> SendableRecordBatchStream
where
    F: std::future::Future<Output = Result<()>> + Send + 'static,
{
    let schema = input.schema();
    let first = futures::stream::once(answer).filter_map(|answered| async move {
        answered
            .err()
            .map(|error| Err(super::operators::external(error)))
    });
    Box::pin(RecordBatchStreamAdapter::new(schema, first.chain(input)))
}

fn membership_type() -> ExprType {
    ExprType::from_prop(&PropType::scalar(ScalarType::Bool, false))
}

/// `expr` with every full-text call `rewrite` answers replaced by its answer.
fn substitute(expr: &IRExpr, rewrite: &mut impl FnMut(&IRExpr) -> Option<IRExpr>) -> IRExpr {
    omnigraph_compiler::stack::deep(|| {
        if expr.lexical_predicate_field().is_some() {
            return rewrite(expr).unwrap_or_else(|| expr.clone());
        }
        let mut sub = |e: &IRExpr| Box::new(substitute(e, rewrite));
        match expr {
            IRExpr::Binary {
                left,
                op,
                right,
                ty,
            } => {
                let left = sub(left);
                let right = sub(right);
                IRExpr::Binary {
                    left,
                    op: *op,
                    right,
                    ty: ty.clone(),
                }
            }
            IRExpr::Not(inner, ty) => IRExpr::Not(sub(inner), ty.clone()),
            IRExpr::Cast { expr: inner, ty } => IRExpr::Cast {
                expr: sub(inner),
                ty: ty.clone(),
            },
            IRExpr::Neg { expr: inner, ty } => IRExpr::Neg {
                expr: sub(inner),
                ty: ty.clone(),
            },
            IRExpr::IsNull {
                expr: inner,
                negated,
                ty,
            } => IRExpr::IsNull {
                expr: sub(inner),
                negated: *negated,
                ty: ty.clone(),
            },
            IRExpr::Call { func, args, ty } => IRExpr::Call {
                func: *func,
                args: args.iter().map(|arg| substitute(arg, rewrite)).collect(),
                ty: ty.clone(),
            },
            other => other.clone(),
        }
    })
}

/// The binding whose property a full-text call searches.
fn call_binding(call: &IRExpr) -> Option<&str> {
    match call.lexical_predicate_field()? {
        IRExpr::PropAccess { variable, .. } => Some(variable),
        _ => None,
    }
}

/// The node type of the binding `call` searches, as node `id` sees it: the
/// nearest scan or traversal below `id` that introduces it, else the one
/// node of the plan that does, for a name an inner tree reads from its outer
/// row.
fn call_type(plan: &PhysicalPlan, id: NodeId, call: &IRExpr) -> Option<String> {
    let binding = call_binding(call)?;
    let introduced = |node: &PhysicalNode| -> Option<String> {
        match node {
            PhysicalNode::Scan { spec, .. } if spec.binding.as_deref() == Some(binding) => spec
                .table
                .type_key
                .strip_prefix("node:")
                .map(str::to_string),
            PhysicalNode::Expand { dst, dst_type, .. } if dst == binding => Some(dst_type.clone()),
            _ => None,
        }
    };
    let mut pending = plan.node(id)?.inputs();
    let mut seen = HashSet::new();
    while let Some(next) = pending.pop() {
        if !seen.insert(next) {
            continue;
        }
        let Some(node) = plan.node(next) else {
            continue;
        };
        if let Some(type_name) = introduced(node) {
            return Some(type_name);
        }
        pending.extend(node.inputs());
    }
    let mut found = plan.live().filter_map(|(_, node)| introduced(node));
    let first = found.next()?;
    found.all(|other| other == first).then_some(first)
}
