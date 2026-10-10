//! GQT cannot suspend finalization, inject source errors or inspect saved access metadata.
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use futures::task::noop_waker;
use omnigraph_compiler::ir::{IRExpr, IROp, IRProjection, QueryIR};
use omnigraph_compiler::query::ast::{CompOp, Literal};
use omnigraph_compiler::{ExprType, SYSTEM_COLUMNS_V3, ScalarType};
use omnigraph_planner::*;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
#[path = "support/bounds.rs"]
mod fixture_bounds;

struct ProbeSource {
    base: MemorySource,
    calls: AtomicUsize,
    dropped: AtomicUsize,
    failure_call: Option<usize>,
    pending: bool,
    runtime: Option<RuntimeInput>,
}
struct DropGuard<'a>(&'a AtomicUsize);
impl Drop for DropGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
impl PlanSource for ProbeSource {
    fn edge_dataset(&self, name: &str) -> Option<DatasetPin> {
        self.base.edge_dataset(name)
    }
    fn schema(&self, side: SideId) -> Result<SchemaRef, PlanError> {
        self.base.schema(side)
    }
    fn fragments(&self, side: SideId) -> Vec<FragmentStat> {
        self.base.fragments(side)
    }
    fn adjacency_proof(&self) -> Option<&AdjacencyProof> {
        None
    }
    fn node_type(&self, name: &str) -> Result<NodeTypeSpec, PlanError> {
        self.base.node_type(name)
    }
    fn index_facts(&self, name: &str) -> Vec<IndexFact> {
        self.base.index_facts(name)
    }
    fn scan_runtime_input(&self, _: &ScanSpec) -> Option<RuntimeInput> {
        self.runtime
    }
    fn index_split<'a>(&'a self, scan: &'a ScanSpec) -> IndexSplitFuture<'a> {
        Box::pin(async move {
            let _guard = DropGuard(&self.dropped);
            let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            assert!(scan.filter.is_some());
            let mut first = true;
            futures::future::poll_fn(|cx| {
                if first || self.pending {
                    first = false;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                } else {
                    Poll::Ready(())
                }
            })
            .await;
            if self.failure_call == Some(call) {
                Err(PlanError::Internal("split failed".into()))
            } else {
                Ok(answer())
            }
        })
    }
}
fn answer() -> ScanAccess {
    ScanAccess::IndexProbe {
        query: IndexQuery::Search {
            index: "identity".into(),
            column: "__id".into(),
            search: "__id = a".into(),
        },
        residual: Some("guard = true".into()),
    }
}
fn source() -> ProbeSource {
    let fields = vec![Field::new("__id", DataType::Utf8, false)];
    let base = MemorySource::default()
        .with_node_type(
            "Doc",
            NodeTypeSpec {
                table: TableRef {
                    type_key: "node:Doc".into(),
                    dataset_path: "node/Doc".into(),
                    native_branch: None,
                },
                version: Some(7),
                columns: SYSTEM_COLUMNS_V3,
                schema: Arc::new(Schema::new(fields.clone())),
                key: vec![],
                object_columns: vec!["__id".into()],
                object_fields: fields.into(),
                row_count: Some(10),
            },
        )
        .with_index_facts(
            "node:Doc",
            vec![IndexFact {
                name: "identity".into(),
                column: "__id".into(),
                kind: IndexKind::Btree { usable: true },
                coverage: Some(FragmentCoverage {
                    covered: 1,
                    total: 1,
                }),
            }],
        );
    ProbeSource {
        base,
        calls: AtomicUsize::new(0),
        dropped: AtomicUsize::new(0),
        failure_call: None,
        pending: false,
        runtime: None,
    }
}
fn prop(binding: &str) -> IRExpr {
    IRExpr::PropAccess {
        variable: binding.into(),
        property: "__id".into(),
        ty: ExprType::Value {
            scalar: ScalarType::String,
            list: false,
            nullable: false,
        },
    }
}
fn query(two: bool) -> QueryIR {
    let scan = |binding: &str| IROp::NodeScan {
        variable: binding.into(),
        type_name: "Doc".into(),
        filters: vec![IRExpr::comparison(
            prop(binding),
            CompOp::Eq,
            IRExpr::Literal(Literal::String("a".into()), prop(binding).ty().clone()),
        )],
    };
    QueryIR {
        name: "probe".into(),
        params: vec![],
        pipeline: if two {
            vec![scan("a"), scan("b")]
        } else {
            vec![scan("a")]
        },
        return_exprs: vec![IRProjection {
            expr: prop("a"),
            alias: None,
            column: "a.id".into(),
            ty: prop("a").ty().clone(),
        }],
        order_by: vec![],
        limit: None,
    }
}
fn prepared(query: &QueryIR, source: &ProbeSource) -> Decision {
    let decision = route(
        &Operation::Query(Box::new(query.clone())),
        source,
        RouteOverride::Registry,
        &fixture_bounds::BOUNDS,
    );
    assert!(matches!(decision, Decision::PendingQuery(_)));
    assert!(decision.explain().is_none());
    decision
}
fn assert_send(_: &impl Send) {}
fn access(plan: &PhysicalPlan) -> Vec<Option<ScanAccess>> {
    plan.live()
        .filter_map(|(_, node)| {
            if let PhysicalNode::Scan { spec, .. } = node {
                Some(spec.access.clone())
            } else {
                None
            }
        })
        .collect()
}
#[test]
fn execution_and_explain_await_the_same_source_answer() {
    let source = source();
    let query = query(false);
    let decision = prepared(&query, &source);
    let mut explain = Box::pin(decision.finalize(&source));
    let mut execution = Box::pin(plan_query(&query, &source, &fixture_bounds::BOUNDS));
    assert_send(&explain);
    assert_send(&execution);
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(explain.as_mut().poll(&mut cx).is_pending());
    assert!(execution.as_mut().poll(&mut cx).is_pending());
    let Decision::Engine { plan, explain, .. } = futures::executor::block_on(explain) else {
        panic!("not finalized")
    };
    let execution = futures::executor::block_on(execution).unwrap();
    assert_eq!(access(&plan), vec![Some(answer())]);
    assert_eq!(plan.to_json(), execution.to_json());
    assert_eq!(source.calls.load(Ordering::SeqCst), 2);
    let facts: Vec<_> = explain
        .statistics
        .as_ref()
        .unwrap()
        .iter()
        .filter(|statistic| statistic.statistic == "index_facts(node:Doc)")
        .collect();
    assert_eq!(facts.len(), 1);
    assert_eq!(
        serde_json::from_str::<Vec<IndexFact>>(&facts[0].value).unwrap(),
        source.index_facts("node:Doc")
    );
    assert_eq!(facts[0].origin, "node/Doc at version 7");
}
#[test]
fn each_access_variant_maps_to_one_scanner_flag() {
    assert_eq!(ScanAccess::Sequential.use_scalar_index(), Some(false));
    assert_eq!(answer().use_scalar_index(), Some(true));
    for runtime in [
        RuntimeInput::Nearest,
        RuntimeInput::FullText,
        RuntimeInput::EligibleIds,
        RuntimeInput::SearchFilter,
        RuntimeInput::JoinFilter,
        RuntimeInput::DynamicExpression,
    ] {
        assert_eq!(
            ScanAccess::Runtime { input: runtime }.use_scalar_index(),
            None
        );
    }
    assert_eq!(
        ScanAccess::IdLookup { index: None }.use_scalar_index(),
        None
    );
    assert_eq!(
        ScanAccess::IdLookup {
            index: Some("identity".into())
        }
        .use_scalar_index(),
        None
    );
}
#[test]
fn failure_and_cancellation_publish_no_partial_plan() {
    let mut source = source();
    source.failure_call = Some(2);
    let query = query(true);
    let result = futures::executor::block_on(prepared(&query, &source).finalize(&source));
    assert!(matches!(result, Decision::Executor { .. }));
    assert!(result.explain().unwrap().physical_plan.is_none());
    source.calls.store(0, Ordering::SeqCst);
    assert!(
        futures::executor::block_on(plan_query(&query, &source, &fixture_bounds::BOUNDS)).is_err()
    );
    source.calls.store(0, Ordering::SeqCst);
    source.dropped.store(0, Ordering::SeqCst);
    source.pending = true;
    let mut future = Box::pin(prepared(&query, &source).finalize(&source));
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(future.as_mut().poll(&mut cx).is_pending());
    drop(future);
    assert_eq!(source.calls.load(Ordering::SeqCst), 1);
    assert_eq!(source.dropped.load(Ordering::SeqCst), 1);
}
#[test]
fn runtime_absent_and_uncovering_catalogs_skip_split_and_replay_keeps_legacy_default() {
    let mut source = source();
    for reason in [
        RuntimeInput::SearchFilter,
        RuntimeInput::EligibleIds,
        RuntimeInput::JoinFilter,
        RuntimeInput::DynamicExpression,
    ] {
        source.runtime = Some(reason);
        let plan = futures::executor::block_on(plan_query(
            &query(false),
            &source,
            &fixture_bounds::BOUNDS,
        ))
        .unwrap();
        assert_eq!(
            access(&plan),
            vec![Some(ScanAccess::Runtime { input: reason })]
        );
    }
    assert_eq!(source.calls.load(Ordering::SeqCst), 0);
    source.runtime = None;
    let plan =
        futures::executor::block_on(plan_query(&query(false), &source, &fixture_bounds::BOUNDS))
            .unwrap();
    let bound = BoundPlan {
        plan,
        values: Default::default(),
    };
    let mut json = serde_json::to_value(&bound).unwrap();
    let restored: BoundPlan = serde_json::from_value(json.clone()).unwrap();
    assert_eq!(access(&restored.plan), vec![Some(answer())]);
    fn remove_access(value: &mut serde_json::Value) {
        match value {
            serde_json::Value::Object(fields) => {
                fields.remove("access");
                for value in fields.values_mut() {
                    remove_access(value);
                }
            }
            serde_json::Value::Array(values) => {
                for value in values {
                    remove_access(value);
                }
            }
            _ => {}
        }
    }
    remove_access(&mut json);
    let restored: BoundPlan = serde_json::from_value(json).unwrap();
    assert_eq!(access(&restored.plan), vec![None]);
    source.base = source.base.with_index_facts("node:Doc", vec![]);
    let calls = source.calls.load(Ordering::SeqCst);
    let plan =
        futures::executor::block_on(plan_query(&query(false), &source, &fixture_bounds::BOUNDS))
            .unwrap();
    assert_eq!(access(&plan), vec![Some(ScanAccess::Sequential)]);
    assert_eq!(source.calls.load(Ordering::SeqCst), calls);
    // An index that covers no current fragment, such as an untrained
    // full-text segment, cannot narrow the scan either; unknown coverage may.
    let fact = |coverage| IndexFact {
        name: "body_idx".into(),
        column: "__id".into(),
        kind: IndexKind::Inverted,
        coverage,
    };
    source.base = source.base.with_index_facts(
        "node:Doc",
        vec![fact(Some(FragmentCoverage {
            covered: 0,
            total: 1,
        }))],
    );
    let plan =
        futures::executor::block_on(plan_query(&query(false), &source, &fixture_bounds::BOUNDS))
            .unwrap();
    assert_eq!(access(&plan), vec![Some(ScanAccess::Sequential)]);
    assert_eq!(source.calls.load(Ordering::SeqCst), calls);
    source.base = source.base.with_index_facts("node:Doc", vec![fact(None)]);
    let plan =
        futures::executor::block_on(plan_query(&query(false), &source, &fixture_bounds::BOUNDS))
            .unwrap();
    assert_eq!(access(&plan), vec![Some(answer())]);
    assert_eq!(source.calls.load(Ordering::SeqCst), calls + 1);
}
