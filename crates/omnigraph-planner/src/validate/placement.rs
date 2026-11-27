//! Plan acceptance's placement check, derived from the core term alone.
//!
//! The type checker writes a scope's `where` conjuncts after its elements,
//! in written order, each with whether it can fail. This check reads that
//! order and those facts from the checked core read itself, not from the
//! shape the planner plans nor from any helper of the optimizer, and holds
//! the physical plan to the placement law (the GQ 3 RFC, section 2): in a
//! scope whose conjuncts include one that can fail, that conjunct and every
//! one written after it
//!
//! - runs in no scan: the conjuncts that move are written before it;
//! - runs above every scan of the scope's bindings: it sees every row the
//!   elements produce;
//! - runs before every conjunct written after it, which never sits below it.
//!
//! A conjunct is found in the plan by its skeleton: its operators and the
//! columns it reads, its constant parts (literals, parameters, constant
//! subterms the engine folded) erased. A conjunct whose skeleton another
//! conjunct of its scope shares, one that reads no column, or one the shaper
//! rewrote (a `let` read in place, an inline read's names renamed apart), is
//! not found and not checked. A conjunct reading a `let` counts under its
//! skeleton with the `let` read in place too, since that is how the plan may
//! test it: a conjunct of another scope whose skeleton that rewrite reaches
//! is then not unique, and the rewritten one is not taken for it.

use std::collections::{BTreeSet, HashMap};

use omnigraph_compiler::ir::core::{CoreRead, Fresh, FreshRole, Term};
use omnigraph_compiler::ir::{IRExpr, ParamMap};

use super::ValidationError;
use super::budget::Budget;
use crate::physical::{NodeId, PhysicalNode, PhysicalPlan};

const CHECK: &str = "placement";

/// One scope's conjuncts in written order, each with whether it can fail,
/// and the bindings its elements introduce.
struct Scope {
    conjuncts: Vec<(IRExpr, bool)>,
    bindings: BTreeSet<String>,
    /// The scope lies in a subterm the term reaches twice (an `optional`'s
    /// pattern, which its desugaring both returns and tests for absence):
    /// each of its conjuncts is written twice, so none is unique.
    shared: bool,
}

/// Holds `plan` to the placement of every scope of `core`.
pub(super) fn check(
    plan: &PhysicalPlan,
    core: &CoreRead,
    _params: &ParamMap,
    budget: &mut Budget,
) -> Result<(), ValidationError> {
    let mut scopes = Vec::new();
    let shared = shared_nodes(&core.term);
    collect(
        &core.term,
        &core.fresh,
        &shared,
        false,
        &mut BTreeSet::new(),
        &mut scopes,
    );
    let lets = let_skeletons(&core.term);
    let tested = tested(plan);
    budget.visit(u64::try_from(tested.len() * scopes.len().max(1)).unwrap_or(u64::MAX))?;
    let parents = parents(plan);
    let below = |node: NodeId, above: NodeId| {
        let mut at = node;
        while let Some(parent) = parents.get(&at) {
            if *parent == above {
                return true;
            }
            at = *parent;
        }
        false
    };
    // Each written conjunct as the plan tests it, its top-level `and`
    // operands apart, each with its conjunct's written position and fact. A
    // part is found only by a skeleton no other part of the read shares,
    // in any scope; one that reads no column (a constant) shares the empty
    // skeleton of every constant and is not found.
    let parts: Vec<Vec<(usize, IRExpr, bool, String)>> = scopes
        .iter()
        .map(|scope| {
            scope
                .conjuncts
                .iter()
                .enumerate()
                .flat_map(|(at, (pred, fails))| {
                    pred.clone().into_conjuncts().into_iter().map(move |part| {
                        let shape = skeleton(&part);
                        (at, part, *fails, shape)
                    })
                })
                .collect()
        })
        .collect();
    // Each part counts under its skeleton, and under its skeleton with the
    // `let`s it reads in place where that differs.
    let mut counts: HashMap<String, usize> = HashMap::new();
    for (scope, parts) in scopes.iter().zip(&parts) {
        for (_, part, _, shape) in parts {
            let n = if scope.shared { 2 } else { 1 };
            *counts.entry(shape.clone()).or_default() += n;
            let inlined = skeleton_with(part, Some(&lets));
            if inlined != *shape {
                *counts.entry(inlined).or_default() += n;
            }
        }
    }
    let unique = |shape: &str| shape != "_" && counts.get(shape) == Some(&1);
    let found = |shape: &str| -> Vec<(NodeId, usize)> {
        tested
            .iter()
            .filter(|(_, _, s)| s == shape)
            .map(|(node, at, _)| (*node, *at))
            .collect()
    };
    for (scope, parts) in scopes.iter().zip(&parts) {
        let Some(k) = scope.conjuncts.iter().position(|(_, fails)| *fails) else {
            continue;
        };
        for (written_at, part, _, shape) in parts.iter().filter(|p| p.0 >= k) {
            if !unique(shape) {
                continue;
            }
            for (node, at) in found(shape) {
                if let Some(PhysicalNode::Scan { spec, .. }) = plan.node(node) {
                    return Err(ValidationError::violated(
                        CHECK,
                        format!(
                            "`{part}` is written at or after the scope's first conjunct that can fail, and the plan tests it in the scan of `${}`",
                            spec.binding.as_deref().unwrap_or("?")
                        ),
                    ));
                }
                for (scan, binding) in scans(plan) {
                    if scope.bindings.contains(&binding) && !below(scan, node) {
                        return Err(ValidationError::violated(
                            CHECK,
                            format!(
                                "`{part}` is written at or after the scope's first conjunct that can fail, and the plan tests it below the element that binds `${binding}`"
                            ),
                        ));
                    }
                }
                for (_, later, _, later_shape) in parts.iter().filter(|p| p.0 > *written_at) {
                    if !unique(later_shape) {
                        continue;
                    }
                    for (later_node, later_at) in found(later_shape) {
                        if below(later_node, node) || (later_node == node && later_at < at) {
                            return Err(ValidationError::violated(
                                CHECK,
                                format!(
                                    "`{later}` is written after `{part}`, which can fail or follows one that can, and the plan tests it first"
                                ),
                            ));
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

/// The nodes `t` reaches by two paths: the pattern an `optional`'s
/// desugaring writes twice is stored once and shared.
fn shared_nodes(t: &Term) -> BTreeSet<*const Term> {
    let mut seen = BTreeSet::new();
    let mut shared = BTreeSet::new();
    let mut pending = vec![t];
    while let Some(node) = pending.pop() {
        if !seen.insert(node as *const Term) {
            shared.insert(node as *const Term);
            continue;
        }
        pending.extend(node.children());
    }
    shared
}

/// Every scope of `t`: a run of `where` conjuncts over the elements below
/// them, with the blocks read as values and guard columns among them. A
/// stored node is read once; a scope under a node in `shared` is marked so.
fn collect(
    t: &Term,
    fresh: &Fresh,
    shared: &BTreeSet<*const Term>,
    mut under_shared: bool,
    seen: &mut BTreeSet<*const Term>,
    out: &mut Vec<Scope>,
) {
    if !seen.insert(t as *const Term) {
        return;
    }
    under_shared |= shared.contains(&(t as *const Term));
    let Term::Filter { .. } = t else {
        t.children()
            .into_iter()
            .for_each(|c| collect(c, fresh, shared, under_shared, seen, out));
        return;
    };
    let mut conjuncts = Vec::new();
    let mut at = t;
    loop {
        match at {
            Term::Filter { input, pred, fails } => {
                conjuncts.push((pred.clone(), *fails));
                at = input;
            }
            Term::Correlate { outer, inner, .. } if is_block(inner, fresh) => {
                collect(inner, fresh, shared, under_shared, seen, out);
                at = outer;
            }
            Term::Project {
                input,
                items,
                keep: true,
                ..
            } if items
                .iter()
                .all(|(name, _)| fresh.is(name, FreshRole::Guard)) =>
            {
                at = input;
            }
            _ => break,
        }
    }
    conjuncts.reverse();
    let mut bindings = BTreeSet::new();
    binds(at, &mut bindings);
    out.push(Scope {
        conjuncts,
        bindings,
        shared: under_shared,
    });
    at.children()
        .into_iter()
        .for_each(|c| collect(c, fresh, shared, under_shared, seen, out));
}

/// A block read as a value: a `Group` of one aggregate and no key whose
/// column the type checker made up as a block's.
fn is_block(inner: &Term, fresh: &Fresh) -> bool {
    matches!(inner, Term::Group { keys, aggs, .. }
        if keys.is_empty() && aggs.len() == 1 && fresh.is(&aggs[0].0, FreshRole::Block))
}

/// The bindings `t`'s scans and expansions introduce, each stored node
/// read once.
fn binds(t: &Term, out: &mut BTreeSet<String>) {
    let mut seen: BTreeSet<*const Term> = BTreeSet::new();
    let mut pending = vec![t];
    while let Some(node) = pending.pop() {
        if !seen.insert(node as *const Term) {
            continue;
        }
        match node {
            Term::Scan { var, .. } => {
                out.insert(var.clone());
            }
            Term::Expand { dst, edge_var, .. } => {
                out.insert(dst.clone());
                out.extend(edge_var.iter().cloned());
            }
            _ => {}
        }
        pending.extend(node.children());
    }
}

/// Every conjunct the plan tests: its node, its position there, and its
/// skeleton.
fn tested(plan: &PhysicalPlan) -> Vec<(NodeId, usize, String)> {
    let mut out = Vec::new();
    for (id, node) in plan.live() {
        let conjuncts: Vec<IRExpr> = match node {
            PhysicalNode::Scan { spec, .. } => spec
                .filter
                .iter()
                .flat_map(|filter| filter.gq_filters())
                .flat_map(IRExpr::into_conjuncts)
                .collect(),
            PhysicalNode::Filter { filters, .. } | PhysicalNode::CrossJoin { filters, .. } => {
                filters
                    .iter()
                    .cloned()
                    .flat_map(IRExpr::into_conjuncts)
                    .collect()
            }
            PhysicalNode::ContainsJoin {
                conjunct, residual, ..
            } => std::iter::once(conjunct.clone())
                .chain(residual.iter().cloned().flat_map(IRExpr::into_conjuncts))
                .collect(),
            _ => Vec::new(),
        };
        out.extend(
            conjuncts
                .iter()
                .enumerate()
                .map(|(at, c)| (id, at, skeleton(c))),
        );
    }
    out
}

/// Every scan of the plan with its binding.
fn scans(plan: &PhysicalPlan) -> Vec<(NodeId, String)> {
    plan.live()
        .filter_map(|(id, node)| match node {
            PhysicalNode::Scan { spec, .. } => spec.binding.clone().map(|b| (id, b)),
            _ => None,
        })
        .collect()
}

fn parents(plan: &PhysicalPlan) -> HashMap<NodeId, NodeId> {
    let mut out = HashMap::new();
    for (id, node) in plan.live() {
        for input in node.inputs() {
            out.insert(input, id);
        }
    }
    out
}

/// The skeleton of every value a `Project` of `t` computes, by its column:
/// how a conjunct reading that column reads once the shaper puts the value
/// in place. A value reading an earlier one is written with it in place.
fn let_skeletons(t: &Term) -> HashMap<String, String> {
    let mut order = Vec::new();
    let mut seen: BTreeSet<*const Term> = BTreeSet::new();
    let mut pending = vec![(t, false)];
    while let Some((node, children_done)) = pending.pop() {
        if children_done {
            order.push(node);
        } else if seen.insert(node as *const Term) {
            pending.push((node, true));
            pending.extend(node.children().into_iter().map(|c| (c, false)));
        }
    }
    let mut out = HashMap::new();
    for node in order {
        if let Term::Project { items, .. } = node {
            for (name, value) in items {
                let shape = skeleton_with(value, Some(&out));
                out.insert(name.clone(), shape);
            }
        }
    }
    out
}

/// `e` with its constant parts erased: what stays of a conjunct whatever
/// value its parameters bind and whatever the engine folded.
fn skeleton(e: &IRExpr) -> String {
    skeleton_with(e, None)
}

/// [`skeleton`], with each column of `lets` written as the value it holds.
fn skeleton_with(e: &IRExpr, lets: Option<&HashMap<String, String>>) -> String {
    // Which subtrees read a column, bottom up: one that reads none is a
    // constant, written `_`.
    let mut reads: HashMap<*const IRExpr, bool> = HashMap::new();
    let mut pending = vec![(e, false)];
    while let Some((node, children_done)) = pending.pop() {
        if children_done {
            let own = matches!(
                node,
                IRExpr::PropAccess { .. }
                    | IRExpr::Variable(..)
                    | IRExpr::AliasRef(..)
                    | IRExpr::Nearest { .. }
            );
            let read = own
                || node
                    .children()
                    .iter()
                    .any(|c| reads[&(*c as *const IRExpr)]);
            reads.insert(node as *const IRExpr, read);
        } else {
            pending.push((node, true));
            pending.extend(node.children().into_iter().map(|c| (c, false)));
        }
    }
    let mut out = String::new();
    write_skeleton(e, &reads, lets, &mut out);
    out
}

/// `e`'s skeleton appended to `out`, each level entered through the stack
/// guard.
fn write_skeleton(
    e: &IRExpr,
    reads: &HashMap<*const IRExpr, bool>,
    lets: Option<&HashMap<String, String>>,
    out: &mut String,
) {
    use std::fmt::Write;
    omnigraph_compiler::stack::deep(|| {
        if !reads[&(e as *const IRExpr)] {
            out.push('_');
            return;
        }
        match e {
            IRExpr::PropAccess {
                variable, property, ..
            } => {
                let _ = write!(out, "{variable}.{property}");
            }
            IRExpr::Variable(name, _) | IRExpr::AliasRef(name, _) => {
                match lets.and_then(|lets| lets.get(name)) {
                    Some(value) => out.push_str(value),
                    None => out.push_str(name),
                }
            }
            IRExpr::Param(..) | IRExpr::Literal(..) => out.push('_'),
            IRExpr::Nearest {
                variable,
                property,
                query,
                ..
            } => {
                let _ = write!(out, "nearest({variable}.{property},");
                write_skeleton(query, reads, lets, out);
                out.push(')');
            }
            IRExpr::Aggregate {
                func,
                distinct,
                arg,
                fraction,
                ..
            } => {
                let _ = write!(out, "{func:?}({distinct},");
                write_skeleton(arg, reads, lets, out);
                if let Some(p) = fraction {
                    out.push(',');
                    write_skeleton(p, reads, lets, out);
                }
                out.push(')');
            }
            IRExpr::Binary {
                left, op, right, ..
            } => {
                out.push('(');
                write_skeleton(left, reads, lets, out);
                let _ = write!(out, " {op:?} ");
                write_skeleton(right, reads, lets, out);
                out.push(')');
            }
            IRExpr::Not(inner, _) => {
                out.push_str("not(");
                write_skeleton(inner, reads, lets, out);
                out.push(')');
            }
            IRExpr::Cast { expr, .. } => write_skeleton(expr, reads, lets, out),
            IRExpr::IsNull { expr, negated, .. } => {
                let _ = write!(out, "isnull({negated},");
                write_skeleton(expr, reads, lets, out);
                out.push(')');
            }
            IRExpr::Neg { expr, .. } => {
                out.push_str("neg(");
                write_skeleton(expr, reads, lets, out);
                out.push(')');
            }
            IRExpr::Call { func, args, .. } => {
                let _ = write!(out, "{func:?}(");
                for (i, arg) in args.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    write_skeleton(arg, reads, lets, out);
                }
                out.push(')');
            }
        }
    })
}
