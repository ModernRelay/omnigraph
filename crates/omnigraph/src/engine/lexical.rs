//! Full-text calls (`search`, `fuzzy`, `match_text`) as Boolean operands.
//!
//! A call standing alone as a conjunct is answered by its binding's scan as
//! membership in the call's matches. Anywhere else, under `not` or `or`,
//! compared with a value or inside `coalesce`, it is the Boolean "this row
//! is among the call's matches", which a `Filter` or a `let` evaluates (the
//! planner keeps such a conjunct off a join's own conjuncts). Before the plan
//! is lowered, each such call is answered once against its node type's table
//! at the pinned snapshot, by the same full-text query the scan runs and with
//! no filter, so the matches are the property's whole match set whatever the
//! index's coverage (Lance scans uncovered fragments with the index's
//! analyzer). The operator then reads each call as a Boolean column, the
//! membership of the row's identity in that set, which a null or token-empty
//! document never joins.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use omnigraph_compiler::catalog::Catalog;
use omnigraph_compiler::ir::{IRExpr, ParamMap};
use omnigraph_compiler::types::{ExprType, PropType, ScalarType};
use omnigraph_planner::{NodeId, PhysicalNode, PhysicalPlan};

use super::operators::memory::WorkMemory;
use super::{OmniError, Result, Snapshot};

/// The prefix of a membership column an operator adds to its input batch
/// while it evaluates; no GQ alias or `binding.property` name starts with it.
const MEMBERSHIP: &str = "~lexical:";

/// Every full-text call the plan evaluates as a Boolean operand, answered:
/// the matching identities of its node type's table, by node type and call.
#[derive(Default)]
pub(super) struct LexicalMembers {
    sets: HashMap<(String, IRExpr), Arc<HashSet<String>>>,
    /// The charge the sets hold for the query's lifetime.
    _memory: Option<WorkMemory>,
}

impl LexicalMembers {
    /// Answer every full-text operand of `plan` at `snapshot`, charging the
    /// match sets to `memory`.
    pub(super) async fn resolve(
        plan: &PhysicalPlan,
        params: &ParamMap,
        snapshot: Option<&Snapshot>,
        catalog: &Catalog,
        memory: WorkMemory,
    ) -> Result<Self> {
        let mut wanted: Vec<(String, IRExpr)> = Vec::new();
        for (id, node) in plan.live() {
            for expr in operand_exprs(node) {
                let mut calls = Vec::new();
                calls_of(expr, &mut calls);
                for call in calls {
                    let Some(type_name) = call_type(plan, id, call) else {
                        return Err(OmniError::manifest_internal(format!(
                            "the full-text call `{call}` reads a binding no input of node {id} introduces"
                        )));
                    };
                    let key = (type_name, call.clone());
                    if !wanted.contains(&key) {
                        wanted.push(key);
                    }
                }
            }
        }
        if wanted.is_empty() {
            return Ok(Self::default());
        }
        let snapshot = snapshot.ok_or_else(|| super::no_graph_storage("a full-text call"))?;
        let mut sets = HashMap::new();
        for (type_name, call) in wanted {
            let query = super::scan::build_fts_query(&call, params)?.ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "the full-text call `{call}` does not search a property"
                ))
            })?;
            let dataset = snapshot
                .open_lance_dataset(&format!("node:{type_name}"))
                .await?;
            let ids = super::scan::search_filter_member_ids(
                &dataset,
                None,
                query,
                catalog.system_columns.id,
                &memory,
            )
            .await?;
            memory
                .entries::<String>(ids.len())
                .map_err(|error| memory.error(error))?;
            sets.insert((type_name, call), Arc::new(ids.into_iter().collect()));
        }
        Ok(Self {
            sets,
            _memory: Some(memory),
        })
    }

    /// `exprs` of node `id` with every full-text call read as its membership
    /// column, and the columns the operator adds to read them; `None` when no
    /// expression holds a call. A call this set did not answer, as in a
    /// lowering for explain alone, is left as written.
    pub(super) fn columns(
        &self,
        plan: &PhysicalPlan,
        id: NodeId,
        exprs: &[IRExpr],
        catalog: &Catalog,
    ) -> (Vec<IRExpr>, Option<Arc<LexicalColumns>>) {
        let mut columns: Vec<LexicalColumn> = Vec::new();
        let mut rewrite = |call: &IRExpr| -> Option<IRExpr> {
            let type_name = call_type(plan, id, call)?;
            let ids = self.sets.get(&(type_name, call.clone()))?;
            let binding = call_binding(call)?;
            let hidden = match columns.iter().position(|column| column.call == *call) {
                Some(position) => columns[position].hidden.clone(),
                None => {
                    let hidden = format!("{MEMBERSHIP}{}", columns.len());
                    columns.push(LexicalColumn {
                        call: call.clone(),
                        hidden: hidden.clone(),
                        id_column: format!("{binding}.{}", catalog.system_columns.id),
                        ids: Arc::clone(ids),
                    });
                    hidden
                }
            };
            Some(IRExpr::AliasRef(hidden, membership_type()))
        };
        let exprs = exprs
            .iter()
            .map(|expr| substitute(expr, &mut rewrite))
            .collect();
        let columns = (!columns.is_empty()).then(|| Arc::new(LexicalColumns { columns }));
        (exprs, columns)
    }
}

/// The membership columns one operator reads, added to each input batch
/// while it evaluates and never emitted.
#[derive(Debug)]
pub(crate) struct LexicalColumns {
    columns: Vec<LexicalColumn>,
}

#[derive(Debug)]
struct LexicalColumn {
    call: IRExpr,
    hidden: String,
    /// The `binding.<id>` column of the batch holding the row's identity.
    id_column: String,
    ids: Arc<HashSet<String>>,
}

impl LexicalColumns {
    /// `batch` with each membership column appended: true where the row's
    /// identity is among the call's matches, false elsewhere.
    pub(crate) fn widen(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let schema = batch.schema();
        let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
        let mut arrays: Vec<ArrayRef> = batch.columns().to_vec();
        for column in &self.columns {
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
                .map(|row| Some(ids.is_valid(row) && column.ids.contains(ids.value(row))))
                .collect();
            fields.push(Field::new(&column.hidden, DataType::Boolean, false));
            arrays.push(Arc::new(member));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .map_err(OmniError::arrow_internal)
    }
}

fn membership_type() -> ExprType {
    ExprType::from_prop(&PropType::scalar(ScalarType::Bool, false))
}

/// The expressions of `node` whose full-text calls an operator evaluates.
fn operand_exprs(node: &PhysicalNode) -> Vec<&IRExpr> {
    match node {
        PhysicalNode::Filter { filters, .. } => filters.iter().collect(),
        PhysicalNode::Let { value, .. } => vec![value],
        _ => Vec::new(),
    }
}

/// Every full-text call in `expr`.
fn calls_of<'e>(expr: &'e IRExpr, out: &mut Vec<&'e IRExpr>) {
    match expr {
        IRExpr::Search { .. } | IRExpr::Fuzzy { .. } | IRExpr::MatchText { .. } => out.push(expr),
        IRExpr::Binary { left, right, .. } => {
            calls_of(left, out);
            calls_of(right, out);
        }
        IRExpr::Not(inner, _)
        | IRExpr::Cast { expr: inner, .. }
        | IRExpr::Neg { expr: inner, .. }
        | IRExpr::IsNull { expr: inner, .. } => calls_of(inner, out),
        IRExpr::Call { args, .. } => args.iter().for_each(|arg| calls_of(arg, out)),
        IRExpr::PropAccess { .. }
        | IRExpr::Nearest { .. }
        | IRExpr::Bm25 { .. }
        | IRExpr::Rrf { .. }
        | IRExpr::Variable(..)
        | IRExpr::Param(..)
        | IRExpr::Literal(..)
        | IRExpr::Aggregate { .. }
        | IRExpr::AliasRef(..) => {}
    }
}

/// `expr` with every full-text call `rewrite` answers replaced by its answer.
fn substitute(expr: &IRExpr, rewrite: &mut impl FnMut(&IRExpr) -> Option<IRExpr>) -> IRExpr {
    let mut sub = |e: &IRExpr| Box::new(substitute(e, rewrite));
    match expr {
        IRExpr::Search { .. } | IRExpr::Fuzzy { .. } | IRExpr::MatchText { .. } => {
            rewrite(expr).unwrap_or_else(|| expr.clone())
        }
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
}

/// The binding whose property a full-text call searches.
fn call_binding(call: &IRExpr) -> Option<&str> {
    match call {
        IRExpr::Search { field, .. }
        | IRExpr::Fuzzy { field, .. }
        | IRExpr::MatchText { field, .. } => match field.as_ref() {
            IRExpr::PropAccess { variable, .. } => Some(variable),
            _ => None,
        },
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
