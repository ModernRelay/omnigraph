//! The placement law: where a `where` conjunct runs (the GQ 3 RFC, section
//! 2, after `where`'s written-order passage).
//!
//! A scope's conjuncts filter in written order, each on the rows the ones
//! before it keep. The type checker writes each conjunct after every element
//! of its scope, in written order, with one typed fact computed once: whether
//! it can fail where it is evaluated ([`omnigraph_compiler::ir::core::Term`]'s
//! `fails`). It writes the same fact on every element that can fail: a `let`,
//! a `from`, an `optional` and a block read as a value. The shaper keeps that
//! order, and this module is the one place a conjunct moves:
//!
//! - A conjunct that cannot fail moves down from where it is written, into
//!   the scan of the one binding it reads or to just above the lowest node
//!   whose passing inputs do not carry every binding it reads.
//! - It never moves below a node that can fail. A conjunct that can fail is
//!   such a node, so the conjuncts written before the first one that can
//!   fail are the ones that move, and that one and every one after it keep
//!   their written order above every element. A `let`, a `from`, an
//!   `optional` or a block that can fail runs on every row the elements
//!   before it produce.
//! - A node that selects rows within a group (a `limit`, a ranking's cut, a
//!   window, a score, and later `shortest` per endpoint pair) declares its
//!   group key, and a conjunct crosses it only when every binding it reads
//!   belongs to that key; one that selects over the whole relation declares
//!   an empty key, which nothing crosses.
//! - An inner scope (a block's, an `optional`'s or a correlated read's
//!   pattern, a union's or a fusion's arm) is no node's passing input, so a
//!   conjunct never moves into one; its own conjuncts are placed by the same
//!   law within it.
//!
//! Every node declares these facts itself ([`Declared`], through
//! `LogicalNode::declares`); the law reads only them, never a node's kind,
//! so an operator added later is placed by what it declares.

use std::collections::BTreeSet;

use omnigraph_compiler::ir::IRExpr;

use crate::PlanSource;
use crate::logical::{LogicalId, LogicalNode, LogicalPlan};
use crate::optimizer::{gq_conjunct, has_lexical_call, reads_of_expr};
use crate::validate::subset::Tracer;

/// What a plan node declares to the placement law.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Declared<Id> {
    /// The node can fail where it is evaluated: nothing moves below it.
    pub fails: bool,
    /// The node selects rows within groups of these bindings: a conjunct
    /// crosses it only when every binding it reads belongs to the key, and
    /// an empty key (a selection over the whole relation) lets nothing
    /// cross. `None`: it selects no row of its input by a rank, a count or a
    /// window.
    pub selects: Option<BTreeSet<String>>,
    /// The inputs whose rows reach the node's output with every column they
    /// carry, once or once per match: a conjunct may move into one that
    /// carries every binding it reads.
    pub passes: Vec<Id>,
    /// The binding whose rows the node reads, a scan: a conjunct over that
    /// binding alone runs in it.
    pub absorbs: Option<String>,
}

impl<Id> Declared<Id> {
    /// A node that cannot fail, selects nothing and passes `inputs`.
    pub fn passing(inputs: impl IntoIterator<Item = Id>) -> Self {
        Self {
            fails: false,
            selects: None,
            passes: inputs.into_iter().collect(),
            absorbs: None,
        }
    }

    /// A node that selects rows over the whole relation of `input`: its
    /// group key is empty.
    pub fn selecting_all(input: Id) -> Self {
        Self {
            selects: Some(BTreeSet::new()),
            ..Self::passing([input])
        }
    }
}

/// A plan the law reads.
pub(crate) trait Placed {
    type Id: Copy + Eq;

    /// What the node `id` declares; `None` for no node.
    fn declared(&self, id: Self::Id) -> Option<Declared<Self::Id>>;

    /// The bindings the rows of the subtree at `id` carry.
    fn carries(&self, id: Self::Id) -> BTreeSet<String>;
}

/// Where a conjunct that cannot fail runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Placement<Id> {
    /// Where it is written.
    Stays,
    /// In the scan at this node.
    Absorbed(Id),
    /// Just above this node.
    Above(Id),
}

/// Where a conjunct that cannot fail, reading `reads` and written over the
/// node `input`, runs under the law.
pub(crate) fn place<P: Placed>(
    plan: &P,
    input: P::Id,
    reads: &BTreeSet<String>,
) -> Placement<P::Id> {
    let mut at = input;
    while let Some(node) = plan.declared(at) {
        if node
            .absorbs
            .as_ref()
            .is_some_and(|binding| reads.len() == 1 && reads.contains(binding))
        {
            return Placement::Absorbed(at);
        }
        if node.fails {
            break;
        }
        if node
            .selects
            .as_ref()
            .is_some_and(|key| !reads.is_subset(key))
        {
            break;
        }
        match node
            .passes
            .iter()
            .find(|passing| reads.is_subset(&plan.carries(**passing)))
        {
            Some(next) => at = *next,
            None => break,
        }
    }
    if at == input {
        Placement::Stays
    } else {
        Placement::Above(at)
    }
}

impl Placed for LogicalPlan {
    type Id = LogicalId;

    fn declared(&self, id: LogicalId) -> Option<Declared<LogicalId>> {
        let node = self.node(id)?;
        let mut declared = node.declares();
        // A catalogue table takes no pushed filter.
        if let LogicalNode::TableScan { spec, .. } = node
            && spec.table.is_catalogue()
        {
            declared.absorbs = None;
        }
        Some(declared)
    }

    fn carries(&self, id: LogicalId) -> BTreeSet<String> {
        // A binding's columns reach a row from the scan that reads its rows:
        // an expansion yields its destination's identity alone, which the
        // dependent scan above it hydrates. A correlated read's rows carry
        // its returned references and its value columns, a union's its
        // bindings and a fusion's its target.
        let mut carried = BTreeSet::new();
        let mut stack = vec![id];
        while let Some(at) = stack.pop() {
            let Some(node) = self.node(at) else { continue };
            match node {
                LogicalNode::TableScan { spec, input } => {
                    carried.extend(spec.binding.iter().cloned());
                    stack.extend(input.iter().copied());
                }
                LogicalNode::Expand {
                    input,
                    edge_binding,
                    ..
                } => {
                    carried.extend(edge_binding.iter().cloned());
                    stack.push(*input);
                }
                LogicalNode::Correlate { input, read, .. } => {
                    carried.insert(read.values.clone());
                    carried.extend(read.references().map(str::to_string));
                    stack.extend(input.iter().copied());
                }
                LogicalNode::AntiJoin { input, .. } => stack.push(*input),
                // A `let`'s rows carry its column, under the binding the
                // shaper made up for it.
                LogicalNode::Let { input, name, .. } => {
                    carried.insert(name.clone());
                    stack.push(*input);
                }
                LogicalNode::Union { bindings, .. } => carried.extend(bindings.iter().cloned()),
                LogicalNode::Fuse { target, .. } => {
                    carried.insert(target.clone());
                }
                other => stack.extend(other.inputs()),
            }
        }
        carried
    }
}

/// The bindings `conjunct` reads.
fn reads(conjunct: &IRExpr) -> BTreeSet<String> {
    let mut columns = Vec::new();
    reads_of_expr(conjunct, &mut columns);
    columns.into_iter().map(|column| column.binding).collect()
}

/// Stage 1, pass 2 on a query plan: every conjunct that cannot fail moves
/// where the law places it, into a scan or above the node it stops at; the
/// conjuncts that stay keep their written order. A conjunct holding a
/// lexical call stays where it is written, which the law allows: the exact
/// lexical scan runs over the rows that reach it; so does one that reads no
/// column.
pub(crate) fn place_conjuncts(
    plan: &mut LogicalPlan,
    source: &dyn PlanSource,
    tracer: &mut Tracer,
) -> bool {
    let mut fired = false;
    // Adjacent conjuncts that cannot fail are one node, a repeated one kept
    // once, before any moves.
    coalesce_filters(plan);
    let filters: Vec<LogicalId> = plan
        .live()
        .filter(|(_, node)| matches!(node, LogicalNode::Filter { fails: false, .. }))
        .map(|(id, _)| id)
        .collect();
    for filter_id in filters {
        let Some(LogicalNode::Filter {
            input, conjuncts, ..
        }) = plan.node(filter_id).cloned()
        else {
            continue;
        };
        let mut stays = Vec::with_capacity(conjuncts.len());
        for conjunct in conjuncts {
            // A conjunct holding a lexical call, or reading no column (a
            // constant, the same on every row), stays where it is written,
            // which the law allows.
            let read = reads(&conjunct);
            if has_lexical_call(&conjunct) || read.is_empty() {
                stays.push(conjunct);
                continue;
            }
            let target = match place(&*plan, input, &read) {
                Placement::Absorbed(scan) if !source.filter_pushable(&conjunct) => {
                    if scan == input {
                        Placement::Stays
                    } else {
                        Placement::Above(scan)
                    }
                }
                other => other,
            };
            match target {
                Placement::Stays => stays.push(conjunct),
                Placement::Absorbed(scan) => {
                    if let Some(LogicalNode::TableScan { spec, .. }) = plan.node_mut(scan) {
                        spec.filter = Some(match spec.filter.take() {
                            Some(existing) => existing.and(gq_conjunct(&conjunct)),
                            None => gq_conjunct(&conjunct),
                        });
                        tracer.absorb(&conjunct);
                        fired = true;
                    } else {
                        stays.push(conjunct);
                    }
                }
                Placement::Above(node) => {
                    if filter_above(plan, node, conjunct.clone()) {
                        fired = true;
                    } else {
                        stays.push(conjunct);
                    }
                }
            }
        }
        if stays.is_empty() {
            plan.splice_out(filter_id, input);
        } else if let Some(LogicalNode::Filter { conjuncts, .. }) = plan.node_mut(filter_id) {
            *conjuncts = stays;
        }
    }
    coalesce_filters(plan);
    fired
}

/// `conjunct`, which cannot fail, just above `node`: into the `Filter` of
/// conjuncts that cannot fail already there, else a new one between `node`
/// and its one consumer; `false`, and the plan untouched, when `node` has no
/// single consumer or no schema.
fn filter_above(plan: &mut LogicalPlan, node: LogicalId, conjunct: IRExpr) -> bool {
    let Some(parent) = plan.parent_of(node) else {
        return false;
    };
    if let Some(LogicalNode::Filter {
        conjuncts,
        fails: false,
        ..
    }) = plan.node_mut(parent)
    {
        if !conjuncts.contains(&conjunct) {
            conjuncts.push(conjunct);
        }
        return true;
    }
    let Some(schema) = plan.schema(node).cloned() else {
        return false;
    };
    let filter = plan.add(
        LogicalNode::Filter {
            input: node,
            conjuncts: vec![conjunct],
            fails: false,
        },
        schema,
    );
    plan.redirect_input(parent, node, filter);
    true
}

/// Each chain of `Filter` nodes that agree on whether a conjunct can fail
/// becomes one node, the lower node's conjuncts first, a repeated conjunct
/// kept once: evaluated left to right, each on the rows the ones before it
/// keep, it filters as the chain did.
fn coalesce_filters(plan: &mut LogicalPlan) {
    let filters: Vec<LogicalId> = plan
        .live()
        .filter(|(_, node)| matches!(node, LogicalNode::Filter { .. }))
        .map(|(id, _)| id)
        .collect();
    for filter_id in filters {
        while let Some(LogicalNode::Filter { input: child, .. }) = plan.node(filter_id) {
            let child = *child;
            let Some(LogicalNode::Filter {
                input: grandchild,
                conjuncts: lower,
                fails: lower_fails,
            }) = plan.node(child).cloned()
            else {
                break;
            };
            // A node of conjuncts that can fail keeps to itself, so the
            // ones that cannot keep a node the join may take as its filters.
            if plan.parent_of(child) != Some(filter_id)
                || !matches!(plan.node(filter_id), Some(LogicalNode::Filter { fails, .. }) if *fails == lower_fails)
            {
                break;
            }
            let Some(LogicalNode::Filter {
                input,
                conjuncts,
                fails,
            }) = plan.node_mut(filter_id)
            else {
                break;
            };
            let mut merged = lower;
            for conjunct in std::mem::take(conjuncts) {
                if !merged.contains(&conjunct) {
                    merged.push(conjunct);
                }
            }
            *conjuncts = merged;
            *fails |= lower_fails;
            *input = grandchild;
            plan.splice_out(child, grandchild);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// A plan of stand-in nodes, each declaring its facts as a real node
    /// does, so a test can add a node kind the planner does not have.
    struct Toy {
        nodes: HashMap<u32, (Declared<u32>, BTreeSet<String>)>,
    }

    impl Toy {
        fn new() -> Self {
            Self {
                nodes: HashMap::new(),
            }
        }

        fn add(&mut self, id: u32, declared: Declared<u32>, binds: &[&str]) {
            self.nodes.insert(
                id,
                (declared, binds.iter().map(|b| b.to_string()).collect()),
            );
        }
    }

    impl Placed for Toy {
        type Id = u32;

        fn declared(&self, id: u32) -> Option<Declared<u32>> {
            self.nodes.get(&id).map(|(d, _)| d.clone())
        }

        fn carries(&self, id: u32) -> BTreeSet<String> {
            let mut out = BTreeSet::new();
            let mut stack = vec![id];
            while let Some(at) = stack.pop() {
                if let Some((declared, binds)) = self.nodes.get(&at) {
                    out.extend(binds.iter().cloned());
                    stack.extend(declared.passes.iter().copied());
                }
            }
            out
        }
    }

    fn set(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|n| n.to_string()).collect()
    }

    fn scan(binding: &str) -> Declared<u32> {
        Declared {
            absorbs: Some(binding.to_string()),
            ..Declared::passing([])
        }
    }

    /// `p` scanned, `c` expanded from it, and a node `selector` over both.
    fn chain(selector: Declared<u32>) -> Toy {
        let mut toy = Toy::new();
        toy.add(1, scan("p"), &["p"]);
        toy.add(2, Declared::passing([1]), &["c"]);
        toy.add(3, selector, &[]);
        toy
    }

    #[test]
    fn a_conjunct_moves_into_the_scan_of_the_one_binding_it_reads() {
        let toy = chain(Declared::passing([2]));
        assert_eq!(place(&toy, 3, &set(&["p"])), Placement::Absorbed(1));
        // One reading the expanded binding too stops above the expansion.
        assert_eq!(place(&toy, 3, &set(&["p", "c"])), Placement::Above(2));
    }

    #[test]
    fn nothing_moves_below_a_node_that_can_fail() {
        let toy = chain(Declared {
            fails: true,
            ..Declared::passing([2])
        });
        assert_eq!(place(&toy, 3, &set(&["p"])), Placement::Stays);
    }

    /// A node kind the planner does not have, which selects rows within
    /// groups of `c`: the law needs no change to place a conjunct by it.
    #[test]
    fn a_stand_in_selecting_node_is_crossed_only_by_a_conjunct_over_its_group_key() {
        let within_c = || Declared {
            selects: Some(set(&["c"])),
            ..Declared::passing([2])
        };
        let toy = chain(within_c());
        assert_eq!(place(&toy, 3, &set(&["p"])), Placement::Stays);
        assert_eq!(place(&toy, 3, &set(&["c"])), Placement::Above(2));
        // Over the whole relation: an empty key, which a conjunct reading a
        // column never crosses; one reading none is the same on every row,
        // so it commutes with any selection.
        let toy = chain(Declared::selecting_all(2));
        assert_eq!(place(&toy, 3, &set(&["p"])), Placement::Stays);
        assert_eq!(place(&toy, 3, &set(&[])), Placement::Above(1));
    }

    #[test]
    fn a_conjunct_never_moves_into_an_input_the_node_does_not_pass() {
        // A union's arms, an inner scope: no passing input.
        let mut toy = Toy::new();
        toy.add(1, scan("p"), &["p"]);
        toy.add(2, Declared::passing([]), &["u"]);
        assert_eq!(place(&toy, 2, &set(&["p"])), Placement::Stays);
    }
}
