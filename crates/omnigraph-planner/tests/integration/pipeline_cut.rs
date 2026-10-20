//! The totality of the pipeline cut over the operator catalog: every kind
//! that admits more than one input names the sides it collects, leaving one
//! streamed input for `PhysicalNode::shape` to place, so
//! `PhysicalPlan::pipelines` meets no node it cannot cut. Rust and not
//! `.gqt`: the claim ranges over `PhysicalNodeKind::ALL`, which no one
//! query reaches whole.

use std::collections::BTreeSet;

use omnigraph_compiler::ir::{IRExpr, SubqueryPredicate};
use omnigraph_compiler::query::ast::Literal;
use omnigraph_compiler::{ExprType, PropType, ScalarType};
use omnigraph_planner::{
    AntiJoinStrategy, EmptyEligible, KeyJoinKind, NodeId, PhysicalNode, PhysicalNodeKind,
    PhysicalPlan, Prefilter, RankArm, RankKind,
};

fn leaf(plan: &mut PhysicalPlan) -> NodeId {
    plan.add(PhysicalNode::OuterReference {
        outer_var: "o".to_string(),
    })
}

fn arm(input: NodeId, kind: RankKind) -> RankArm {
    RankArm {
        input,
        binding: "d".to_string(),
        kind,
    }
}

fn truth() -> IRExpr {
    IRExpr::Literal(
        Literal::Bool(true),
        ExprType::from_prop(&PropType::scalar(ScalarType::Bool, false)),
    )
}

/// One node of every kind with more than one input slot.
fn multi_input_samples(plan: &mut PhysicalPlan) -> Vec<PhysicalNode> {
    let [a, b, c, d, e, f, g, h, i, j, k, l] = std::array::from_fn(|_| leaf(plan));
    vec![
        PhysicalNode::HashJoin {
            probe: a,
            build: b,
            binding: "d".to_string(),
            fallback: None,
        },
        PhysicalNode::SortMergeJoin {
            left: c,
            right: d,
            kind: KeyJoinKind::FullOuter,
            on: "__id".to_string(),
            build: true,
            drop_equal_addresses: false,
        },
        PhysicalNode::SortMergeJoin {
            left: e,
            right: f,
            kind: KeyJoinKind::FullOuter,
            on: "__id".to_string(),
            build: false,
            drop_equal_addresses: false,
        },
        PhysicalNode::CrossJoin {
            left: g,
            right: h,
            filters: Vec::new(),
        },
        PhysicalNode::ContainsJoin {
            left: i,
            right: j,
            haystack: ("h".to_string(), "text".to_string()),
            needle: ("n".to_string(), "text".to_string()),
            conjunct: truth(),
            residual: Vec::new(),
        },
        PhysicalNode::AntiJoin {
            input: k,
            inner: l,
            outer_var: "o".to_string(),
            predicate: SubqueryPredicate::not_exists(),
            aggregate: None,
            strategy: AntiJoinStrategy::Correlated,
        },
        PhysicalNode::RankFuse {
            arms: [
                arm(leaf(plan), RankKind::Nearest),
                arm(leaf(plan), RankKind::Bm25),
            ],
            k: None,
            limit: None,
            prefilter: Prefilter {
                ranked_type: "Doc".to_string(),
                hops: Vec::new(),
                feeds: Vec::new(),
                on_empty: EmptyEligible::Postfilter,
                coverage_admits: true,
            },
            row_tiebreak: Vec::new(),
        },
    ]
}

#[test]
fn every_multi_input_kind_collects_all_but_one_side() {
    let mut plan = PhysicalPlan::new();
    let samples = multi_input_samples(&mut plan);
    let sampled: BTreeSet<PhysicalNodeKind> = samples.iter().map(PhysicalNode::kind).collect();
    for kind in PhysicalNodeKind::ALL {
        let most = kind.input_counts().into_iter().max().unwrap_or(0);
        assert!(
            most <= 1 || sampled.contains(kind),
            "`{kind:?}` admits {most} inputs; add a sample of it above so its collected sides are checked"
        );
    }
    for node in &samples {
        let inputs = node.inputs();
        let collected = node.collected_sides();
        for side in &collected {
            assert!(
                inputs.contains(side),
                "`{}` collects {side}, which is not one of its inputs {inputs:?}",
                node.name()
            );
        }
        let streamed = inputs
            .iter()
            .filter(|input| !collected.contains(input))
            .count();
        let expected = match node {
            PhysicalNode::SortMergeJoin { build: false, .. } => 2,
            _ => 1,
        };
        assert_eq!(
            streamed,
            expected,
            "`{}` streams {streamed} of its inputs {inputs:?} after collecting {collected:?}",
            node.name()
        );
    }
}
