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
//! not found and not checked.

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
}

/// Holds `plan` to the placement of every scope of `core`.
pub(super) fn check(
    plan: &PhysicalPlan,
    core: &CoreRead,
    _params: &ParamMap,
    budget: &mut Budget,
) -> Result<(), ValidationError> {
    let mut scopes = Vec::new();
    collect(&core.term, &core.fresh, &mut scopes);
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
    let mut counts: HashMap<&str, usize> = HashMap::new();
    for (_, _, _, shape) in parts.iter().flatten() {
        *counts.entry(shape.as_str()).or_default() += 1;
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

/// Every scope of `t`: a run of `where` conjuncts over the elements below
/// them, with the blocks read as values and guard columns among them.
fn collect(t: &Term, fresh: &Fresh, out: &mut Vec<Scope>) {
    let Term::Filter { .. } = t else {
        t.children()
            .into_iter()
            .for_each(|c| collect(c, fresh, out));
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
                collect(inner, fresh, out);
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
    });
    at.children()
        .into_iter()
        .for_each(|c| collect(c, fresh, out));
}

/// A block read as a value: a `Group` of one aggregate and no key whose
/// column the type checker made up as a block's.
fn is_block(inner: &Term, fresh: &Fresh) -> bool {
    matches!(inner, Term::Group { keys, aggs, .. }
        if keys.is_empty() && aggs.len() == 1 && fresh.is(&aggs[0].0, FreshRole::Block))
}

/// The bindings `t`'s scans and expansions introduce.
fn binds(t: &Term, out: &mut BTreeSet<String>) {
    match t {
        Term::Scan { var, .. } => {
            out.insert(var.clone());
        }
        Term::Expand { dst, edge_var, .. } => {
            out.insert(dst.clone());
            out.extend(edge_var.iter().cloned());
        }
        _ => {}
    }
    t.children().into_iter().for_each(|c| binds(c, out));
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

/// `e` with its constant parts erased: what stays of a conjunct whatever
/// value its parameters bind and whatever the engine folded.
fn skeleton(e: &IRExpr) -> String {
    let mut read = Vec::new();
    e.columns(&mut read);
    if read.is_empty() {
        return "_".to_string();
    }
    let all = |args: &[&IRExpr]| {
        args.iter()
            .map(|a| skeleton(a))
            .collect::<Vec<_>>()
            .join(",")
    };
    match e {
        IRExpr::PropAccess {
            variable, property, ..
        } => format!("{variable}.{property}"),
        IRExpr::Variable(name, _) | IRExpr::AliasRef(name, _) => name.clone(),
        IRExpr::Param(..) | IRExpr::Literal(..) => "_".to_string(),
        IRExpr::Nearest {
            variable,
            property,
            query,
            ..
        } => format!("nearest({variable}.{property},{})", skeleton(query)),
        IRExpr::Aggregate {
            func,
            distinct,
            arg,
            ..
        } => format!("{func:?}({distinct},{})", skeleton(arg)),
        IRExpr::Binary {
            left, op, right, ..
        } => format!("({} {op:?} {})", skeleton(left), skeleton(right)),
        IRExpr::Not(inner, _) => format!("not({})", skeleton(inner)),
        IRExpr::Cast { expr, .. } => skeleton(expr),
        IRExpr::IsNull { expr, negated, .. } => format!("isnull({negated},{})", skeleton(expr)),
        IRExpr::Neg { expr, .. } => format!("neg({})", skeleton(expr)),
        IRExpr::Call { func, args, .. } => {
            format!("{func:?}({})", all(&args.iter().collect::<Vec<_>>()))
        }
    }
}
