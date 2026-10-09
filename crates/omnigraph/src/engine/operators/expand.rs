//! `ExpandExec` owns the validated traversal step executed by `expand_stream`.

use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use std::fmt;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{Result as DfResult, internal_err};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use omnigraph_compiler::catalog::Catalog;
use omnigraph_compiler::traversal::{EdgeMember, EdgeSelection};
use omnigraph_compiler::types::Direction;
use omnigraph_planner::{ExpandMode, ExpandPolicy};

use super::{breaker_properties, external, joined_schema, polled, streaming_properties};
use crate::db::Snapshot;
use crate::engine::graph::{GraphIndexHandle, ModeOrigin, bound_edge_pair_schema};
use crate::error::{OmniError, Result};

/// What every graph operator of one query shares: the lazy CSR handle, the
/// snapshot and the catalog.
pub(crate) struct GraphEnv {
    pub(crate) graph_index: Arc<GraphIndexHandle>,
    pub(crate) snapshot: Snapshot,
    pub(crate) catalog: Arc<Catalog>,
}

/// A legacy named route retains its declared mode and correction policy.
#[derive(Debug, Clone)]
pub(crate) struct NamedExpand {
    pub(crate) member: EdgeMember,
    pub(crate) mode: ExpandMode,
    pub(crate) origin: ModeOrigin,
}

/// Budgeted execution is always an indexed union. Only a named legacy route
/// can carry a CSR mode or the policy that permits switching to it.
#[derive(Debug, Clone)]
pub(crate) enum ExpandExecution {
    Named(NamedExpand),
    Budgeted(EdgeSelection),
}

impl ExpandExecution {
    pub(crate) fn new(
        edges: EdgeSelection,
        mode: ExpandMode,
        policy: &ExpandPolicy,
    ) -> Result<Self> {
        if matches!(policy, ExpandPolicy::Budgeted) {
            if mode != ExpandMode::IndexedScan {
                return Err(OmniError::manifest_internal(
                    "budgeted expansion requires indexed execution",
                ));
            }
            return Ok(Self::Budgeted(edges));
        }
        let EdgeSelection::Named(member) = edges else {
            return Err(OmniError::manifest_internal(
                "edge selection requires budgeted execution",
            ));
        };
        let origin = match policy {
            ExpandPolicy::Pinned => ModeOrigin::Pinned,
            ExpandPolicy::Costed { inputs } => ModeOrigin::Costed(inputs.clone()),
            ExpandPolicy::Uncosted => ModeOrigin::Uncosted,
            ExpandPolicy::Budgeted => unreachable!("budgeted policy handled above"),
        };
        Ok(Self::Named(NamedExpand {
            member,
            mode,
            origin,
        }))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ExpandStep {
    pub(crate) src: String,
    pub(crate) dst: String,
    pub(crate) execution: ExpandExecution,
    pub(crate) src_type: String,
    pub(crate) dst_type: String,
    pub(crate) min_hops: u32,
    pub(crate) max_hops: u32,
    pub(crate) edge_binding: Option<String>,
    pub(crate) frontier_estimate: Option<u64>,
}

impl ExpandStep {
    pub(crate) fn members(&self) -> &[EdgeMember] {
        match &self.execution {
            ExpandExecution::Named(named) => std::slice::from_ref(&named.member),
            ExpandExecution::Budgeted(edges) => edges.members(),
        }
    }

    pub(crate) fn has_type_column(&self) -> bool {
        matches!(&self.execution, ExpandExecution::Budgeted(edges) if edges.named().is_none())
    }

    /// A one-hop bound; Budgeted multi-hop also streams fixed source windows.
    pub(crate) fn single_hop(&self) -> bool {
        self.max_hops == 1
    }

    pub(crate) fn budgeted(&self) -> bool {
        matches!(self.execution, ExpandExecution::Budgeted(_))
    }

    fn validate(&self, catalog: &Catalog) -> Result<()> {
        validate_expand_structure(
            self.members(),
            matches!(
                &self.execution,
                ExpandExecution::Budgeted(EdgeSelection::Alternation(_))
            ),
            self.src_type != self.dst_type,
            self.min_hops,
            Some(self.max_hops),
            self.edge_binding.is_some(),
        )?;
        for member in self.members() {
            let edge = catalog.edge_types.get(&member.edge_type).ok_or_else(|| {
                OmniError::manifest(format!("unknown edge type '{}'", member.edge_type))
            })?;
            let (src, dst) = match member.direction {
                Direction::Out => (&edge.from_type, &edge.to_type),
                Direction::In => (&edge.to_type, &edge.from_type),
                Direction::Both if edge.from_type == edge.to_type => {
                    (&edge.from_type, &edge.to_type)
                }
                Direction::Both => {
                    return Err(OmniError::manifest_internal(
                        "undirected edge selection has asymmetric endpoints",
                    ));
                }
            };
            if src != &self.src_type || dst != &self.dst_type {
                return Err(OmniError::manifest_internal(
                    "edge selection endpoint types do not match its members",
                ));
            }
        }
        Ok(())
    }
}

pub(crate) fn validate_expand_structure(
    members: &[EdgeMember],
    alternation: bool,
    cross_type: bool,
    min_hops: u32,
    max_hops: Option<u32>,
    bound_edge: bool,
) -> Result<()> {
    if min_hops == 0 {
        return Err(OmniError::manifest_internal(
            "traversal minimum depth must be positive",
        ));
    }
    let max_hops = max_hops
        .ok_or_else(|| OmniError::manifest_internal("traversal requires a finite maximum depth"))?;
    if max_hops < min_hops {
        return Err(OmniError::manifest_internal(
            "traversal maximum depth is below its minimum",
        ));
    }
    if bound_edge && (min_hops != 1 || max_hops != 1) {
        return Err(OmniError::manifest_internal(
            "bound edge traversal requires exactly one hop",
        ));
    }
    // A hop ends on the destination type and the next must start on the
    // source type, so a cross-type path never reaches a second hop.
    if cross_type && max_hops > 1 {
        return Err(OmniError::manifest_internal(
            "a multi-hop traversal requires the same endpoint type",
        ));
    }
    if alternation && members.is_empty() {
        return Err(OmniError::manifest_internal(
            "edge alternation requires at least one member",
        ));
    }
    if members.iter().any(|member| member.edge_type.is_empty()) {
        return Err(OmniError::manifest_internal(
            "traversal member type name must not be empty",
        ));
    }
    for pair in members.windows(2) {
        if pair[0].edge_type == pair[1].edge_type {
            return Err(OmniError::manifest_internal(
                "traversal members contain a duplicate edge type",
            ));
        }
        if pair[0].edge_type > pair[1].edge_type {
            return Err(OmniError::manifest_internal(
                "traversal members must be in canonical type-name order",
            ));
        }
    }
    Ok(())
}

impl fmt::Display for ExpandExecution {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (kind, members) = match self {
            Self::Named(named) => ("named", std::slice::from_ref(&named.member)),
            Self::Budgeted(edges) => (
                match edges {
                    EdgeSelection::Named(_) => "named",
                    EdgeSelection::Alternation(_) => "alternation",
                    EdgeSelection::Wildcard(_) => "wildcard",
                },
                edges.members(),
            ),
        };
        write!(f, "{kind} [")?;
        for (index, member) in members.iter().enumerate() {
            if index > 0 {
                write!(f, ", ")?;
            }
            let direction = match member.direction {
                Direction::Out => "out",
                Direction::In => "in",
                Direction::Both => "both",
            };
            write!(f, "{} {direction}", member.edge_type)?;
        }
        write!(f, "]")
    }
}

pub(crate) struct ExpandExec {
    input: Arc<dyn ExecutionPlan>,
    step: ExpandStep,
    env: Arc<GraphEnv>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl ExpandExec {
    pub(crate) fn try_new(
        input: Arc<dyn ExecutionPlan>,
        step: ExpandStep,
        env: Arc<GraphEnv>,
    ) -> Result<Self> {
        step.validate(&env.catalog)?;
        let schema = expand_output_schema(&input.schema(), &env.catalog, &step)?;
        let properties = if step.single_hop() || step.budgeted() {
            streaming_properties(schema)
        } else {
            breaker_properties(schema)
        };
        Ok(Self {
            input,
            step,
            env,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

/// Source columns, the destination ID, then any bound-edge columns.
fn expand_output_schema(input: &Schema, catalog: &Catalog, step: &ExpandStep) -> Result<SchemaRef> {
    let destination = Schema::new(vec![Field::new(
        format!("{}.{}", step.dst, catalog.system_columns.id),
        DataType::Utf8,
        false,
    )]);
    let mut schema = joined_schema(input, &destination)?;
    if let Some(binding) = &step.edge_binding {
        let pair_schema = bound_edge_pair_schema(catalog, step.members(), step.has_type_column())?;
        let edge_fields: Vec<Field> = pair_schema.fields()[2..]
            .iter()
            .map(|field| {
                Field::new(
                    format!("{binding}.{}", field.name()),
                    field.data_type().clone(),
                    field.is_nullable(),
                )
            })
            .collect();
        schema = joined_schema(&schema, &Schema::new(edge_fields))?;
    }
    Ok(schema)
}

impl fmt::Debug for ExpandExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExpandExec")
            .field("step", &self.step)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for ExpandExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ExpandExec: {}", self.step)
    }
}

impl fmt::Display for ExpandStep {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let step = self;
        write!(
            f,
            "${} {} ${}: {}, hops={}..{}",
            step.src, step.execution, step.dst, step.dst_type, step.min_hops, step.max_hops,
        )?;
        if let Some(binding) = &step.edge_binding {
            write!(f, ", edge_binding=${binding}, batches=bounded")?;
        }
        write!(
            f,
            ", mode={}",
            match &step.execution {
                ExpandExecution::Named(named) => named.mode.word(),
                ExpandExecution::Budgeted(_) => ExpandMode::IndexedScan.word(),
            }
        )?;
        match step.frontier_estimate {
            Some(rows) => write!(f, ", estimate={rows}")?,
            None => write!(f, ", estimate=unknown")?,
        }
        write!(f, ", streaming={}", step.single_hop() || step.budgeted())
    }
}

impl ExecutionPlan for ExpandExec {
    fn name(&self) -> &str {
        "ExpandExec"
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let (Some(input), true) = (children.pop(), children.is_empty()) else {
            return internal_err!("ExpandExec takes one child");
        };
        Ok(Arc::new(
            Self::try_new(input, self.step.clone(), Arc::clone(&self.env)).map_err(external)?,
        ))
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("ExpandExec has one partition, asked for {}", partition);
        }
        let schema: SchemaRef = self.schema();
        let input_schema = self.input.schema();
        let input = self.input.execute(0, Arc::clone(&ctx))?;
        let step = self.step.clone();
        let env = Arc::clone(&self.env);
        super::expand_stream::execute(input, input_schema, schema, step, env, ctx, &self.metrics)
            .map(|stream| polled(&self.metrics, stream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use omnigraph_compiler::catalog::build_catalog;
    use omnigraph_compiler::schema::parser::parse_schema;
    use omnigraph_compiler::traversal::EDGE_TYPE_COLUMN;

    fn catalog() -> Catalog {
        build_catalog(
            &parse_schema(
                r#"
            node Person { name: String }
            node Company { name: String }
            edge Knows: Person -> Person { label: String count: I64 only_knows: String }
            edge Likes: Person -> Person { label: String? count: I64 }
            edge WorksAt: Person -> Company
        "#,
            )
            .unwrap(),
        )
        .unwrap()
    }

    fn step(edges: EdgeSelection) -> ExpandStep {
        ExpandStep {
            src: "p".into(),
            dst: "q".into(),
            execution: ExpandExecution::Budgeted(edges),
            src_type: "Person".into(),
            dst_type: "Person".into(),
            min_hops: 1,
            max_hops: 1,
            edge_binding: Some("e".into()),
            frontier_estimate: None,
        }
    }

    fn member(name: &str) -> EdgeMember {
        EdgeMember {
            edge_type: name.into(),
            direction: Direction::Out,
        }
    }

    #[test]
    fn selected_expand_display_preserves_kind_direction_and_execution_issue_659() {
        let mut selected = step(EdgeSelection::Alternation(vec![
            member("Knows"),
            EdgeMember {
                edge_type: "Likes".into(),
                direction: Direction::In,
            },
        ]));
        assert_eq!(
            selected.to_string(),
            "$p alternation [Knows out, Likes in] $q: Person, hops=1..1, edge_binding=$e, batches=bounded, mode=indexed_scan, estimate=unknown, streaming=true"
        );
        selected.execution = ExpandExecution::Budgeted(EdgeSelection::Wildcard(vec![]));
        assert_eq!(selected.execution.to_string(), "wildcard []");
        selected.execution =
            ExpandExecution::Budgeted(EdgeSelection::Alternation(vec![member("Knows")]));
        assert_eq!(selected.execution.to_string(), "alternation [Knows out]");
        selected.execution = ExpandExecution::new(
            EdgeSelection::Named(EdgeMember {
                edge_type: "Knows".into(),
                direction: Direction::Both,
            }),
            ExpandMode::Csr,
            &ExpandPolicy::Pinned,
        )
        .unwrap();
        assert_eq!(selected.execution.to_string(), "named [Knows both]");
        assert!(selected.to_string().contains("mode=csr"));
    }

    #[test]
    fn traversal_structure_refuses_distinct_causes_issue_659() {
        let members = [member("Knows")];
        for (minimum, maximum, bound, cross_type, cause) in [
            (0, Some(1), false, false, "minimum depth must be positive"),
            (1, None, false, false, "finite maximum depth"),
            (
                2,
                Some(1),
                false,
                false,
                "maximum depth is below its minimum",
            ),
            (
                1,
                Some(2),
                true,
                false,
                "bound edge traversal requires exactly one hop",
            ),
            (1, Some(2), false, true, "same endpoint type"),
        ] {
            let error =
                validate_expand_structure(&members, true, cross_type, minimum, maximum, bound)
                    .unwrap_err();
            assert!(error.to_string().contains(cause), "{error}");
        }
        for (members, cause) in [
            (vec![], "at least one member"),
            (vec![member("")], "type name must not be empty"),
            (
                vec![member("Knows"), member("Knows")],
                "duplicate edge type",
            ),
            (
                vec![member("Likes"), member("Knows")],
                "canonical type-name order",
            ),
        ] {
            let error =
                validate_expand_structure(&members, true, false, 1, Some(1), false).unwrap_err();
            assert!(error.to_string().contains(cause), "{error}");
        }
        validate_expand_structure(&[], false, false, 1, Some(1), true).unwrap();
        validate_expand_structure(&members, false, false, 1, Some(9), false).unwrap();
    }

    #[test]
    fn selected_edge_output_matches_pair_order_and_widens_nullable_properties_issue_659() {
        let catalog = catalog();
        let step = step(EdgeSelection::Alternation(vec![
            member("Knows"),
            member("Likes"),
        ]));
        let pair =
            bound_edge_pair_schema(&catalog, step.members(), step.has_type_column()).unwrap();
        let output = expand_output_schema(&Schema::empty(), &catalog, &step).unwrap();
        for (pair, output) in pair.fields()[2..].iter().zip(&output.fields()[1..]) {
            assert_eq!(output.name(), &format!("e.{}", pair.name()));
            assert_eq!(output.data_type(), pair.data_type());
            assert_eq!(output.is_nullable(), pair.is_nullable());
        }
        assert!(output.field_with_name("e.only_knows").is_err());
        assert!(output.field_with_name("e.label").unwrap().is_nullable());
        assert!(!output.field_with_name("e.count").unwrap().is_nullable());
        assert!(
            !output
                .field_with_name(&format!("e.{EDGE_TYPE_COLUMN}"))
                .unwrap()
                .is_nullable()
        );
    }

    #[test]
    fn empty_wildcard_retains_typed_edge_identity_schema_issue_659() {
        let catalog = catalog();
        let step = step(EdgeSelection::Wildcard(vec![]));
        step.validate(&catalog).unwrap();
        let output = expand_output_schema(&Schema::empty(), &catalog, &step).unwrap();
        assert_eq!(output.fields().len(), 5);
        for field in output.fields() {
            assert_eq!(field.data_type(), &DataType::Utf8);
            assert!(!field.is_nullable());
        }
    }

    #[test]
    fn named_edge_keeps_properties_without_redundant_type_payload_issue_659() {
        let catalog = catalog();
        let mut step = step(EdgeSelection::Named(member("Knows")));
        step.execution = ExpandExecution::new(
            EdgeSelection::Named(member("Knows")),
            ExpandMode::IndexedScan,
            &ExpandPolicy::Pinned,
        )
        .unwrap();
        step.validate(&catalog).unwrap();
        let output = expand_output_schema(&Schema::empty(), &catalog, &step).unwrap();
        assert!(output.field_with_name("e.only_knows").is_ok());
        assert!(
            output
                .field_with_name(&format!("e.{EDGE_TYPE_COLUMN}"))
                .is_err()
        );
    }

    #[test]
    fn replayed_selection_refuses_incompatible_endpoints_and_recursive_edge_binding_issue_659() {
        let catalog = catalog();
        let mut step = step(EdgeSelection::Alternation(vec![member("Knows")]));
        step.dst_type = "Other".into();
        assert!(step.validate(&catalog).is_err());
        step.dst_type = "Person".into();
        step.max_hops = 2;
        assert!(step.validate(&catalog).is_err());
    }

    #[test]
    fn replayed_named_cross_type_step_refuses_a_second_hop() {
        let catalog = catalog();
        for policy in [ExpandPolicy::Pinned, ExpandPolicy::Budgeted] {
            let mut step = step(EdgeSelection::Named(member("WorksAt")));
            step.execution = ExpandExecution::new(
                EdgeSelection::Named(member("WorksAt")),
                ExpandMode::IndexedScan,
                &policy,
            )
            .unwrap();
            step.dst_type = "Company".into();
            step.edge_binding = None;
            step.validate(&catalog).unwrap();
            step.max_hops = 2;
            let error = step.validate(&catalog).unwrap_err();
            assert!(
                error.to_string().contains("same endpoint type"),
                "{policy:?}: {error}"
            );
        }
    }
}
