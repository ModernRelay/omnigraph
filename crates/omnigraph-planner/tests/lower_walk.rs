//! The order and totality of `PhysicalPlan::lower`. Rust and not `.gqt`: the
//! claim is the call sequence an engine sees, which no query result shows.

use omnigraph_compiler::ir::{IRExpr, IRFilter, IROrdering, IRProjection};
use omnigraph_planner::{
    ExpandFields, HashJoinFields, Lower, NodeId, PhysicalNode, PhysicalPlan, PlanError, Prefilter,
    RankArm, RankKind, RankedAccess, ScanInput, ScanSpec, SideId, SortMergeJoinFields,
};

fn no_prefilter() -> Prefilter {
    Prefilter {
        ranked_type: "Doc".to_string(),
        hops: Vec::new(),
        feeds: Vec::new(),
    }
}

#[derive(Default)]
struct Trace {
    calls: Vec<String>,
}

impl Trace {
    fn call(&mut self, name: &str, id: NodeId, inputs: &[&str]) -> Result<String, PlanError> {
        let op = format!("{name}#{id}({})", inputs.join(","));
        self.calls.push(op.clone());
        Ok(op)
    }
}

impl Lower for Trace {
    type Op = String;
    type Error = PlanError;

    fn metadata_count(
        &mut self,
        id: NodeId,
        _: &ScanSpec,
        _: &[IRProjection],
    ) -> Result<String, PlanError> {
        self.call("metadata_count", id, &[])
    }

    fn scan(
        &mut self,
        id: NodeId,
        _: &ScanInput,
        _: &ScanSpec,
        _: bool,
        _: bool,
        _: Option<&RankedAccess>,
        input: Option<String>,
    ) -> Result<String, PlanError> {
        let inputs: Vec<&str> = input.iter().map(String::as_str).collect();
        self.call("scan", id, &inputs)
    }

    fn sort_merge_join(
        &mut self,
        id: NodeId,
        _: SortMergeJoinFields<'_>,
        left: String,
        right: String,
    ) -> Result<String, PlanError> {
        self.call("sort_merge_join", id, &[&left, &right])
    }

    fn hash_join(
        &mut self,
        id: NodeId,
        _: HashJoinFields<'_>,
        probe: String,
        build: String,
    ) -> Result<String, PlanError> {
        self.call("hash_join", id, &[&probe, &build])
    }

    fn hydrate_by_address(
        &mut self,
        id: NodeId,
        _: SideId,
        input: String,
    ) -> Result<String, PlanError> {
        self.call("hydrate_by_address", id, &[&input])
    }

    fn row_compare(&mut self, id: NodeId, input: String) -> Result<String, PlanError> {
        self.call("row_compare", id, &[&input])
    }

    fn classify_three_way(&mut self, id: NodeId, input: String) -> Result<String, PlanError> {
        self.call("classify_three_way", id, &[&input])
    }

    fn limit(&mut self, id: NodeId, _: usize, input: String) -> Result<String, PlanError> {
        self.call("limit", id, &[&input])
    }

    fn page(
        &mut self,
        id: NodeId,
        _: usize,
        _: u64,
        _: Option<&str>,
        input: String,
    ) -> Result<String, PlanError> {
        self.call("page", id, &[&input])
    }

    fn cross_join(&mut self, id: NodeId, left: String, right: String) -> Result<String, PlanError> {
        self.call("cross_join", id, &[&left, &right])
    }

    fn filter(&mut self, id: NodeId, _: &[IRFilter], input: String) -> Result<String, PlanError> {
        self.call("filter", id, &[&input])
    }

    fn expand(
        &mut self,
        id: NodeId,
        _: ExpandFields<'_>,
        input: String,
    ) -> Result<String, PlanError> {
        self.call("expand", id, &[&input])
    }

    fn anti_join_outer(&mut self, id: NodeId, _: &str, outer: &String) -> Result<(), PlanError> {
        self.call("anti_join_outer", id, &[outer]).map(|_| ())
    }

    fn anti_join(
        &mut self,
        id: NodeId,
        _: &str,
        outer: String,
        inner: String,
    ) -> Result<String, PlanError> {
        self.call("anti_join", id, &[&outer, &inner])
    }

    fn outer_reference(&mut self, id: NodeId, _: &str) -> Result<String, PlanError> {
        self.call("outer_reference", id, &[])
    }

    fn rank_fuse(
        &mut self,
        id: NodeId,
        _: &[RankArm; 2],
        _: Option<&IRExpr>,
        _: Option<usize>,
        primary: String,
        secondary: String,
    ) -> Result<String, PlanError> {
        self.call("rank_fuse", id, &[&primary, &secondary])
    }

    fn projection(
        &mut self,
        id: NodeId,
        _: &[IRProjection],
        input: String,
    ) -> Result<String, PlanError> {
        self.call("projection", id, &[&input])
    }

    fn aggregate(
        &mut self,
        id: NodeId,
        _: &[IRProjection],
        input: String,
    ) -> Result<String, PlanError> {
        self.call("aggregate", id, &[&input])
    }

    fn sort(
        &mut self,
        id: NodeId,
        _: &[IROrdering],
        _: Option<usize>,
        _: &[String],
        input: String,
    ) -> Result<String, PlanError> {
        self.call("sort", id, &[&input])
    }

    fn finish(&mut self, root: NodeId, op: String) -> Result<String, PlanError> {
        self.call("finish", root, &[&op])
    }
}

fn leaf(plan: &mut PhysicalPlan, outer_var: &str) -> NodeId {
    plan.add(PhysicalNode::OuterReference {
        outer_var: outer_var.to_string(),
    })
}

#[test]
fn every_input_is_lowered_before_its_consumer_and_finish_runs_last() {
    let mut plan = PhysicalPlan::new();
    let left = leaf(&mut plan, "a");
    let right = leaf(&mut plan, "b");
    let join = plan.add(PhysicalNode::CrossJoin { left, right });
    let filter = plan.add(PhysicalNode::Filter {
        input: join,
        filters: Vec::new(),
    });
    let limit = plan.add(PhysicalNode::Limit {
        input: filter,
        rows: 3,
    });
    plan.set_root(limit);

    let mut trace = Trace::default();
    plan.lower(&mut trace).expect("the plan lowers");

    assert_eq!(
        trace.calls,
        [
            "outer_reference#0()",
            "outer_reference#1()",
            "cross_join#2(outer_reference#0(),outer_reference#1())",
            "filter#3(cross_join#2(outer_reference#0(),outer_reference#1()))",
            "limit#4(filter#3(cross_join#2(outer_reference#0(),outer_reference#1())))",
            "finish#4(limit#4(filter#3(cross_join#2(outer_reference#0(),outer_reference#1()))))",
        ]
    );
}

#[test]
fn a_hash_join_lowers_its_probe_then_its_build_scan_then_itself() {
    let mut plan = PhysicalPlan::new();
    let probe = leaf(&mut plan, "d");
    let build = plan.add(PhysicalNode::Scan {
        source: ScanInput::Table,
        spec: Box::new(ScanSpec {
            side: SideId::Base,
            table: omnigraph_planner::TableRef {
                type_key: "node:Doc".to_string(),
                dataset_path: "node_Doc".to_string(),
                native_branch: None,
            },
            version: None,
            columns: omnigraph_compiler::SystemColumns {
                id: "__id",
                src: "__src",
                dst: "__dst",
            },
            fragments: None,
            projection: None,
            filter: None,
            binding: Some("d".to_string()),
        }),
        ordered: false,
        keys_only: false,
        ranked: None,
    });
    let join = plan.add(PhysicalNode::HashJoin {
        probe,
        build,
        binding: "d".to_string(),
        fallback: Some(omnigraph_planner::AccessPath::IdLookup),
    });
    plan.set_root(join);

    let mut trace = Trace::default();
    plan.lower(&mut trace).expect("the plan lowers");

    assert_eq!(
        trace.calls,
        [
            "outer_reference#0()",
            "scan#1()",
            "hash_join#2(outer_reference#0(),scan#1())",
            "finish#2(hash_join#2(outer_reference#0(),scan#1()))",
        ]
    );
}

#[test]
fn a_hash_join_whose_build_is_not_a_table_scan_refuses_the_lowering() {
    let mut plan = PhysicalPlan::new();
    let probe = leaf(&mut plan, "d");
    let build = leaf(&mut plan, "d");
    let join = plan.add(PhysicalNode::HashJoin {
        probe,
        build,
        binding: "d".to_string(),
        fallback: None,
    });
    plan.set_root(join);

    let error = plan
        .lower(&mut Trace::default())
        .expect_err("a build that is not a table scan is refused");

    assert_eq!(
        error,
        PlanError::Internal(format!("the build of hash join {join} is not a table scan"))
    );
}

#[test]
fn an_anti_join_hands_over_its_outer_input_before_the_inner_tree_is_lowered() {
    let mut plan = PhysicalPlan::new();
    let outer = leaf(&mut plan, "o");
    let inner_leaf = leaf(&mut plan, "o");
    let inner = plan.add(PhysicalNode::Filter {
        input: inner_leaf,
        filters: Vec::new(),
    });
    let anti = plan.add(PhysicalNode::AntiJoin {
        input: outer,
        inner,
        outer_var: "o".to_string(),
    });
    plan.set_root(anti);

    let mut trace = Trace::default();
    plan.lower(&mut trace).expect("the plan lowers");

    let names: Vec<&str> = trace
        .calls
        .iter()
        .map(|call| call.split('(').next().expect("a call has a name"))
        .collect();
    assert_eq!(
        names,
        [
            "outer_reference#0",
            "anti_join_outer#3",
            "outer_reference#1",
            "filter#2",
            "anti_join#3",
            "finish#3",
        ]
    );
}

#[test]
fn a_rank_fuse_lowers_each_arm_once_primary_first() {
    let mut plan = PhysicalPlan::new();
    let primary = leaf(&mut plan, "d");
    let secondary = leaf(&mut plan, "d");
    let arm = |input: NodeId, kind: RankKind| RankArm {
        input,
        binding: "d".to_string(),
        kind,
    };
    let fuse = plan.add(PhysicalNode::RankFuse {
        arms: [
            arm(primary, RankKind::Nearest),
            arm(secondary, RankKind::Bm25),
        ],
        k: None,
        limit: Some(3),
        prefilter: no_prefilter(),
    });
    plan.set_root(fuse);

    let mut trace = Trace::default();
    plan.lower(&mut trace).expect("the plan lowers");

    assert_eq!(
        trace.calls,
        [
            "outer_reference#0()",
            "outer_reference#1()",
            "rank_fuse#2(outer_reference#0(),outer_reference#1())",
            "finish#2(rank_fuse#2(outer_reference#0(),outer_reference#1()))",
        ]
    );
}

#[test]
fn a_live_node_the_root_does_not_reach_refuses_the_lowering() {
    let mut plan = PhysicalPlan::new();
    let reached = leaf(&mut plan, "a");
    let stranded = leaf(&mut plan, "b");
    plan.set_root(reached);

    let error = plan
        .lower(&mut Trace::default())
        .expect_err("a stranded node is refused");

    assert_eq!(
        error,
        PlanError::Internal(format!(
            "physical nodes [{stranded}] are live and unreachable from the root"
        ))
    );
}

#[test]
fn a_node_with_two_consumers_refuses_the_lowering() {
    let mut plan = PhysicalPlan::new();
    let shared = leaf(&mut plan, "a");
    let join = plan.add(PhysicalNode::CrossJoin {
        left: shared,
        right: shared,
    });
    plan.set_root(join);

    let error = plan
        .lower(&mut Trace::default())
        .expect_err("a shared node is refused");

    assert_eq!(
        error,
        PlanError::Internal(format!(
            "physical nodes [{shared}] are the input of two consumers"
        ))
    );

    let mut plan = PhysicalPlan::new();
    let shared = leaf(&mut plan, "d");
    let arm = || RankArm {
        input: shared,
        binding: "d".to_string(),
        kind: RankKind::Bm25,
    };
    let fuse = plan.add(PhysicalNode::RankFuse {
        arms: [arm(), arm()],
        k: None,
        limit: Some(3),
        prefilter: no_prefilter(),
    });
    plan.set_root(fuse);

    let error = plan
        .lower(&mut Trace::default())
        .expect_err("a shared arm is refused");

    assert_eq!(
        error,
        PlanError::Internal(format!(
            "physical nodes [{shared}] are the input of two consumers"
        ))
    );
}

#[test]
fn parent_of_names_the_consumer_and_the_root_has_none() {
    let mut plan = PhysicalPlan::new();
    let source = leaf(&mut plan, "a");
    let sort = plan.add(PhysicalNode::Sort {
        input: source,
        order_by: Vec::new(),
        fetch: None,
        tiebreak: Vec::new(),
    });
    plan.set_root(sort);

    assert_eq!(plan.parent_of(source), Some(sort));
    assert_eq!(plan.parent_of(sort), None);
}
