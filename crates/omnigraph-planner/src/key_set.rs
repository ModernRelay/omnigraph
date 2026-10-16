//! The key-set form of a correlated block (`not { … }`, `count { … } > 2`).
//!
//! The logical inner tree of an `AntiJoin` starts from the outer rows: its
//! leaf is an `OuterReference` and its first `Expand` leaves the correlation
//! binding. When the block reads nothing of the outer rows but that binding's
//! identity, the same matches come from the block's own pattern, rooted at
//! one of its own bindings and ended on the correlation binding: an `Expand`
//! emits one row per distinct endpoint pair (or per edge row, with an edge
//! binding), and the reversed traversal emits the same pairs, so every key
//! meets the same matches in either orientation. The key-set tree is built
//! here, in its own arena, and the physical selection lowers it only when the
//! cost model prices it below the correlated tree; the logical plan keeps the
//! correlated tree as the block's meaning.

use std::collections::{HashMap, HashSet, VecDeque};

use omnigraph_compiler::ir::{IRExpr, SubqueryPredicate};
use omnigraph_compiler::traversal::EdgeSelection;

use crate::cost::scan_row_estimate;
use crate::error::PlanError;
use crate::logical::{ColumnRef, LogicalId, LogicalNode, LogicalPlan, Predicate, ScanSpec};
use crate::optimizer::reads_of_expr;
use crate::source::PlanSource;

/// The re-rooted inner tree of one `AntiJoin`, alone in its arena.
#[derive(Debug)]
pub(crate) struct KeySetTree {
    pub plan: LogicalPlan,
    /// The tree's top: its last `Expand`, which emits the correlation
    /// binding's identity, or the `Filter` of the block's residual conjuncts.
    pub root: LogicalId,
}

/// One `Expand` of the block's pattern as the correlated tree wrote it.
struct PatternEdge {
    src: String,
    dst: String,
    edges: EdgeSelection,
    src_type: String,
    dst_type: String,
    min_hops: u32,
    max_hops: Option<u32>,
    edge_binding: Option<String>,
}

impl PatternEdge {
    /// This edge's `Expand` leaving `from`, reversed when it was written
    /// toward `from`.
    fn leaving(&self, from: &str, input: LogicalId) -> LogicalNode {
        let reversed = self.dst == from;
        LogicalNode::Expand {
            input,
            src: if reversed { &self.dst } else { &self.src }.clone(),
            dst: if reversed { &self.src } else { &self.dst }.clone(),
            edges: if reversed {
                self.edges.reversed()
            } else {
                self.edges.clone()
            },
            src_type: if reversed {
                &self.dst_type
            } else {
                &self.src_type
            }
            .clone(),
            dst_type: if reversed {
                &self.src_type
            } else {
                &self.dst_type
            }
            .clone(),
            min_hops: self.min_hops,
            max_hops: self.max_hops,
            edge_binding: self.edge_binding.clone(),
        }
    }

    fn other(&self, end: &str) -> &str {
        if self.src == end {
            &self.dst
        } else {
            &self.src
        }
    }
}

/// The key-set tree of the `AntiJoin` whose correlated tree is `inner`, when
/// the block qualifies: the inner tree is a chain of `Expand`, dependent
/// `TableScan` and `Filter` nodes over the `OuterReference`, every `Expand`
/// introduces a new binding from a bound one (a pattern tree rooted at
/// `outer_var`), every introduced node binding has its scan, nothing reads
/// an outer binding (`outer_var` included: its identity is the key, never a
/// column read), and one binding's scan is bounded by its own filter (a key
/// or unique equality or a list membership) below its table's row count. The
/// root is the binding with the fewest estimated rows, the first in the
/// correlated tree on a tie. `None` when the block does not qualify.
pub(crate) fn key_set_tree(
    plan: &LogicalPlan,
    inner: LogicalId,
    outer_var: &str,
    predicate: &SubqueryPredicate,
    source: &dyn PlanSource,
) -> Result<Option<KeySetTree>, PlanError> {
    if plan.traversal_work_limit().is_some() {
        return Ok(None);
    }
    let mut chain = Vec::new();
    let mut current = inner;
    loop {
        match plan.node(current) {
            Some(LogicalNode::OuterReference { outer_var: leaf }) if leaf == outer_var => break,
            Some(
                LogicalNode::TableScan {
                    input: Some(input), ..
                }
                | LogicalNode::Expand { input, .. }
                | LogicalNode::Filter { input, .. },
            ) => {
                chain.push(current);
                current = *input;
            }
            _ => return Ok(None),
        }
    }
    chain.reverse();

    let mut edges: Vec<PatternEdge> = Vec::new();
    let mut vertices: Vec<String> = vec![outer_var.to_string()];
    let mut introduced: HashSet<String> = HashSet::new();
    let mut scans: HashMap<String, LogicalId> = HashMap::new();
    let mut residual: Vec<IRExpr> = Vec::new();
    let mut reads: Vec<ColumnRef> = Vec::new();
    let mut last_expand_dst: Option<String> = None;
    for &id in &chain {
        match plan.node(id) {
            Some(LogicalNode::Expand {
                src,
                dst,
                edges: selection,
                src_type,
                dst_type,
                min_hops,
                max_hops,
                edge_binding,
                ..
            }) => {
                let fresh = |name: &str| name != outer_var && !introduced.contains(name);
                if !vertices.contains(src)
                    || !fresh(dst)
                    || edge_binding
                        .as_deref()
                        .is_some_and(|binding| !fresh(binding))
                {
                    return Ok(None);
                }
                vertices.push(dst.clone());
                introduced.insert(dst.clone());
                introduced.extend(edge_binding.iter().cloned());
                edges.push(PatternEdge {
                    src: src.clone(),
                    dst: dst.clone(),
                    edges: selection.clone(),
                    src_type: src_type.clone(),
                    dst_type: dst_type.clone(),
                    min_hops: *min_hops,
                    max_hops: *max_hops,
                    edge_binding: edge_binding.clone(),
                });
                last_expand_dst = Some(dst.clone());
            }
            Some(LogicalNode::TableScan { spec, .. }) => {
                let binding = spec.binding.as_deref();
                if binding.is_none()
                    || binding != last_expand_dst.as_deref()
                    || spec.fragments.is_some()
                    || spec.runtime_filter.is_some()
                {
                    return Ok(None);
                }
                if let Some(filter) = &spec.filter {
                    predicate_reads(filter, &mut reads);
                }
                scans.insert(spec.binding.clone().unwrap_or_default(), id);
                last_expand_dst = None;
            }
            Some(LogicalNode::Filter { conjuncts, .. }) => {
                for conjunct in conjuncts {
                    reads_of_expr(conjunct, &mut reads);
                }
                residual.extend(conjuncts.iter().cloned());
            }
            _ => return Ok(None),
        }
    }
    if let Some(argument) = predicate.left.arg() {
        reads_of_expr(argument, &mut reads);
    }
    if edges.is_empty()
        || reads.iter().any(|read| !introduced.contains(&read.binding))
        || vertices[1..]
            .iter()
            .any(|vertex| !scans.contains_key(vertex))
    {
        return Ok(None);
    }

    let mut root: Option<(u64, &str)> = None;
    for vertex in &vertices[1..] {
        let Some(LogicalNode::TableScan { spec, .. }) = plan.node(scans[vertex]) else {
            return Ok(None);
        };
        let Some(bound) = bounded_rows(spec, source) else {
            continue;
        };
        if root.is_none_or(|(rows, _)| bound < rows) {
            root = Some((bound, vertex.as_str()));
        }
    }
    let Some((_, root)) = root else {
        return Ok(None);
    };

    let schema_of = |id: LogicalId| {
        plan.schema(id)
            .cloned()
            .ok_or_else(|| PlanError::Internal(format!("logical node {id} has no schema")))
    };
    let mut tree = LogicalPlan::new();
    let scan_of = |vertex: &str| match plan.node(scans[vertex]) {
        Some(LogicalNode::TableScan { spec, .. }) => Ok(spec.clone()),
        _ => Err(PlanError::Internal(format!(
            "the key-set scan of `${vertex}` is not a table scan"
        ))),
    };
    let mut top = tree.add(
        LogicalNode::TableScan {
            input: None,
            spec: scan_of(root)?,
        },
        schema_of(scans[root])?,
    );
    let mut used = vec![false; edges.len()];
    let mut reached = HashSet::from([root.to_string()]);
    let mut pending = VecDeque::from([root.to_string()]);
    while let Some(from) = pending.pop_front() {
        for (index, edge) in edges.iter().enumerate() {
            if used[index] || (edge.src != from && edge.dst != from) {
                continue;
            }
            used[index] = true;
            let to = edge.other(&from).to_string();
            let schema = tree
                .schema(top)
                .cloned()
                .ok_or_else(|| PlanError::Internal("key-set node has no schema".to_string()))?;
            top = tree.add(edge.leaving(&from, top), schema.clone());
            if to != outer_var {
                top = tree.add(
                    LogicalNode::TableScan {
                        input: Some(top),
                        spec: scan_of(&to)?,
                    },
                    schema,
                );
            }
            if reached.insert(to.clone()) {
                pending.push_back(to);
            }
        }
    }
    if used.iter().any(|used| !used) || !reached.contains(outer_var) {
        return Err(PlanError::Internal(
            "a correlated block's pattern is not a tree over its bindings".to_string(),
        ));
    }
    if !residual.is_empty() {
        let schema = tree
            .schema(top)
            .cloned()
            .ok_or_else(|| PlanError::Internal("key-set node has no schema".to_string()))?;
        top = tree.add(
            LogicalNode::Filter {
                input: top,
                conjuncts: residual,
            },
            schema,
        );
    }
    tree.set_root(top);
    Ok(Some(KeySetTree {
        plan: tree,
        root: top,
    }))
}

/// The rows `spec` reads as a table scan when its own filter bounds them
/// below the table's row count; `None` when it reads the whole table.
fn bounded_rows(spec: &ScanSpec, source: &dyn PlanSource) -> Option<u64> {
    let table_rows = source
        .node_type(spec.table.node_type_name()?)
        .ok()?
        .row_count?;
    scan_row_estimate(spec, source).filter(|rows| *rows < table_rows)
}

fn predicate_reads(predicate: &Predicate, out: &mut Vec<ColumnRef>) {
    match predicate {
        Predicate::Gq { reads, .. } => out.extend(reads.iter().cloned()),
        Predicate::And { left, right } => {
            predicate_reads(left, out);
            predicate_reads(right, out);
        }
        Predicate::IdAfter { .. } | Predicate::VersionWindow { .. } => {}
    }
}
