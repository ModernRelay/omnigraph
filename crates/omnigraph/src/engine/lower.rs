//! Lowers a read `BoundPlan` to one DataFusion physical plan with the plan's
//! shape: every read node builds exactly one operator, and the lowered tree's
//! children are the node's inputs' operators. Relational nodes other than the
//! aggregate use omnigraph's operators too, so every operator counts its polls
//! and charges what it allocates; the aggregate is DataFusion's `AggregateExec`
//! over GQ expressions. A scan's `RankedAccess` and the bound values
//! parameterize its `ScanExec`; the `Pass` says how this pass widens or
//! prefilters it.

use std::collections::HashMap;
use std::sync::Mutex;

use arrow_schema::{Schema, SchemaRef};
use datafusion::functions_aggregate::average::avg_udaf;
use datafusion::functions_aggregate::count::count_udaf;
use datafusion::functions_aggregate::min_max::{max_udaf, min_udaf};
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::aggregate::AggregateExprBuilder;
use datafusion::physical_expr::expressions::{CastExpr, Column};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use omnigraph_compiler::ir::SubqueryPredicate;
use omnigraph_planner::{
    BoundPlan, ExpandFields, HashJoinFields, Lower, NodeId, PhysicalNode, PhysicalPlan, PlanError,
    Predicate, RankArm, RankKind, RankedAccess, ScanInput, ScanSpec, SideId, SortMergeJoinFields,
    ValueTable,
};

use super::adapters::{GqProjectionExpr, LoweringId, Projected};
use super::operators::{
    AntiJoinMaskExec, ArmOrder, CrossJoinExec, ExpandExec, ExpandStep, FilterExec, GraphEnv,
    HashJoinExec, LimitExec, LookupSpec, MetadataCountExec, OuterReferenceExec, OuterSlot,
    ProjectionExec, RankFuseExec, ScanExec, ScanSource, SortExec, SortKey, fresh_tag_column,
    tagged_schema,
};
use super::*;

type Plan = Arc<dyn ExecutionPlan>;

/// The prefix of a column the return projection carries for the sort above
/// it and the sort drops; no GQ alias or `binding.property` name starts with
/// it.
pub(super) const HIDDEN: &str = "~";

/// The rank constant of an `rrf()` that names none.
const RRF_DEFAULT_K: u32 = 60;

pub(super) struct Lowering<'a> {
    pub(super) plan: &'a PhysicalPlan,
    pub(super) values: &'a ValueTable,
    pub(super) snapshot: &'a Snapshot,
    pub(super) graph_index: &'a Arc<GraphIndexHandle>,
    pub(super) catalog: &'a Arc<Catalog>,
}

pub(super) struct Lowered {
    pub(super) root: Plan,
    /// The one operator of every live node, the tree `root` heads.
    pub(super) operators: HashMap<NodeId, Plan>,
    /// What the ranked scan reported in this pass, for the overfetch ladder.
    pub(super) report: Arc<Mutex<ScanReport>>,
    /// The in-memory filter applications of the tree, one per conjunct.
    in_memory_filters: usize,
}

impl Lowered {
    /// Count this tree's in-memory filters, once per execution of it.
    pub(super) fn record_in_memory_filters(&self) {
        for _ in 0..self.in_memory_filters {
            crate::instrumentation::record_in_memory_filter();
        }
    }
}

/// What every operator of one lowering shares.
struct Scope {
    id: LoweringId,
    env: Arc<GraphEnv>,
    ctx: Arc<ProjectionContext>,
}

impl<'a> Lowering<'a> {
    /// `bound` lowered under `context`.
    pub(super) fn new(bound: &'a BoundPlan, context: &'a EngineContext<'a>) -> Self {
        Self {
            plan: &bound.plan,
            values: &bound.values,
            snapshot: context.snapshot,
            graph_index: &context.graph_index,
            catalog: context.catalog,
        }
    }

    fn node(&self, id: NodeId) -> Result<&'a PhysicalNode> {
        self.plan.node(id).ok_or_else(|| {
            OmniError::manifest_internal(format!("physical node {id} is a tombstone"))
        })
    }

    fn params(&self) -> &'a Arc<ParamMap> {
        &self.values.params
    }

    /// The whole tree for one pass, executable under the query's context.
    pub(super) fn lower_query(&self, pass: &Pass) -> Result<Lowered> {
        let mut walk = Walk::new(self, pass);
        let root = self
            .plan
            .lower(&mut walk)
            .map_err(|LowerError(error)| error)?;
        Ok(Lowered {
            root,
            operators: walk.operators,
            report: walk.report,
            in_memory_filters: walk.in_memory_filters,
        })
    }

    /// The `SearchMode` scan `id` runs under in `pass`: the plan's ranking
    /// with its bound query value, widened or prefiltered as the pass says.
    fn search_mode(
        &self,
        id: NodeId,
        spec: &ScanSpec,
        ranked: &RankedAccess,
        pass: &Pass,
    ) -> Result<SearchMode> {
        let mut mode = SearchMode {
            answer_proven_empty: pass.is_proven_empty(id),
            eligible_ids: pass.eligible(id).cloned(),
            ..SearchMode::default()
        };
        match ranked.kind {
            RankKind::Nearest => {
                let vector = self.values.vectors.get(&id).ok_or_else(|| {
                    OmniError::manifest_internal(format!(
                        "the nearest scan {id} of `{}` has no bound query vector",
                        spec.table.type_key
                    ))
                })?;
                let k = ranked.fetch.ok_or_else(|| {
                    OmniError::manifest("nearest() ordering requires a limit clause".to_string())
                })?;
                let rung = pass.rung(id);
                mode.nearest = Some(NearestTarget {
                    property: ranked.property.clone(),
                    vector: vector.clone(),
                    k: rung.map_or(k, |rung| rung.k),
                });
                mode.ann_probe_budget = match rung {
                    Some(rung) => rung.maximum,
                    None => ranked.nprobes,
                };
                mode.nearest_exact = rung.is_some_and(|rung| rung.exact);
            }
            RankKind::Bm25 => {
                let text = resolve_to_string(&ranked.query, self.params()).ok_or_else(|| {
                    OmniError::manifest("bm25 query must resolve to a string".to_string())
                })?;
                mode.bm25 = Some(Bm25Target {
                    property: ranked.property.clone(),
                    text,
                });
            }
        }
        Ok(mode)
    }

    /// What a scan of `spec` reads: the destination type, the binding, the
    /// pushed filters and the projection, on either side of a hash join too.
    fn lookup(&self, spec: &ScanSpec) -> Result<LookupSpec> {
        let binding = spec.binding.as_deref().ok_or_else(|| {
            OmniError::manifest_internal(
                "a read plan's scan is bound to a match variable".to_string(),
            )
        })?;
        let type_name = spec
            .table
            .type_key
            .strip_prefix("node:")
            .unwrap_or(&spec.table.type_key);
        Ok(LookupSpec {
            type_name: type_name.to_string(),
            binding: binding.to_string(),
            filters: spec
                .filter
                .as_ref()
                .map(Predicate::gq_filters)
                .unwrap_or_default(),
            projection: spec
                .projection
                .as_ref()
                .map(|columns| NeededColumns(columns.iter().cloned().collect())),
            params: Arc::clone(self.params()),
            snapshot: self.snapshot.clone(),
            catalog: Arc::clone(self.catalog),
        })
    }

    fn scan(&self, source: ScanSource, spec: &ScanSpec) -> Result<ScanExec> {
        let lookup = self.lookup(spec)?;
        ScanExec::try_new(
            source,
            lookup.type_name,
            lookup.binding,
            lookup.filters,
            lookup.projection,
            lookup.params,
            lookup.snapshot,
            lookup.catalog,
        )
    }

    /// The edge the bulk CSR degree answers for a row-count block: one
    /// single-hop, filter-free, unbound expand from the outer binding (by id
    /// lookup or hash join); an undirected edge only for an existence test.
    fn bulk_row_count(
        &self,
        inner: NodeId,
        outer_var: &str,
        predicate: &SubqueryPredicate,
    ) -> Result<Option<(String, Direction)>> {
        if !predicate.is_row_count() {
            return Ok(None);
        }
        let inner = match self.node(inner)? {
            PhysicalNode::Scan {
                source: ScanInput::Dependent { input },
                spec,
                ..
            } if spec.filter.is_none() => *input,
            PhysicalNode::HashJoin { probe, build, .. }
                if matches!(
                    self.node(*build)?,
                    PhysicalNode::Scan { spec, .. } if spec.filter.is_none()
                ) =>
            {
                *probe
            }
            _ => return Ok(None),
        };
        Ok(match self.node(inner)? {
            PhysicalNode::Expand {
                input,
                src,
                edge_type,
                direction,
                min_hops,
                max_hops,
                edge_binding: None,
                ..
            } if src == outer_var
                && *min_hops == 1
                && max_hops.unwrap_or(1) == 1
                && (*direction != Direction::Both || predicate.is_existence_test())
                && matches!(self.node(*input)?, PhysicalNode::OuterReference { .. }) =>
            {
                Some((edge_type.clone(), *direction))
            }
            _ => None,
        })
    }

    fn projection(&self, projected: Projected, scope: &Scope) -> Arc<dyn PhysicalExpr> {
        Arc::new(GqProjectionExpr::new(
            projected,
            Arc::clone(self.params()),
            Arc::clone(&scope.ctx),
            scope.id,
        ))
    }

    /// The `Sort` that consumes node `id`, when one does: the projection
    /// below it carries the sort's columns hidden.
    fn sort_above(&self, id: NodeId) -> Option<(&'a [IROrdering], &'a [String])> {
        match self
            .plan
            .parent_of(id)
            .and_then(|parent| self.plan.node(parent))
        {
            Some(PhysicalNode::Sort {
                order_by, tiebreak, ..
            }) => Some((order_by.as_slice(), tiebreak.as_slice())),
            _ => None,
        }
    }
}

/// The engine's error with the planner walk's own refusals folded in.
pub(super) struct LowerError(OmniError);

impl From<OmniError> for LowerError {
    fn from(error: OmniError) -> Self {
        Self(error)
    }
}

impl From<PlanError> for LowerError {
    fn from(error: PlanError) -> Self {
        Self(OmniError::manifest_internal(error.to_string()))
    }
}

type Lowers<T> = std::result::Result<T, LowerError>;

/// What an `AntiJoin` inner tree reads while the walk is inside it.
struct OuterScope {
    slot: Arc<OuterSlot>,
    schema: SchemaRef,
    tag_column: String,
}

/// The state of one `PhysicalPlan::lower` call over a read plan.
struct Walk<'l, 'a> {
    lowering: &'l Lowering<'a>,
    pass: &'l Pass,
    scope: Scope,
    report: Arc<Mutex<ScanReport>>,
    outers: Vec<OuterScope>,
    operators: HashMap<NodeId, Plan>,
    in_memory_filters: usize,
    /// A scan below was proven empty by a gate, so the filters above it run
    /// on nothing and are not counted as applied.
    proven_empty: bool,
}

impl<'l, 'a> Walk<'l, 'a> {
    fn new(lowering: &'l Lowering<'a>, pass: &'l Pass) -> Self {
        Self {
            lowering,
            pass,
            scope: Scope {
                id: LoweringId::next(),
                env: Arc::new(GraphEnv {
                    graph_index: Arc::clone(lowering.graph_index),
                    snapshot: lowering.snapshot.clone(),
                    catalog: Arc::clone(lowering.catalog),
                }),
                ctx: Arc::new(ProjectionContext::for_plan(lowering.catalog, lowering.plan)),
            },
            report: Arc::new(Mutex::new(ScanReport::default())),
            outers: Vec::new(),
            operators: HashMap::new(),
            in_memory_filters: 0,
            proven_empty: false,
        }
    }

    fn not_a_pipeline_node(name: &str) -> LowerError {
        OmniError::manifest_internal(format!("`{name}` is not a pipeline node of a read plan"))
            .into()
    }

    /// The one operator of node `id`.
    fn built(&mut self, id: NodeId, operator: impl ExecutionPlan) -> Plan {
        let plan: Plan = Arc::new(operator);
        self.operators.insert(id, Arc::clone(&plan));
        plan
    }
}

impl Lower for Walk<'_, '_> {
    type Op = Plan;
    type Error = LowerError;

    fn metadata_count(
        &mut self,
        id: NodeId,
        spec: &ScanSpec,
        _return_exprs: &[IRProjection],
    ) -> Lowers<Plan> {
        let properties = self.lowering.plan.properties(id).ok_or_else(|| {
            OmniError::manifest_internal("metadata count has no declared schema".to_string())
        })?;
        let count = MetadataCountExec::new(
            spec.table.type_key.clone(),
            self.lowering.snapshot.clone(),
            Arc::clone(&properties.schema),
        );
        Ok(self.built(id, count))
    }

    fn scan(
        &mut self,
        id: NodeId,
        source: &ScanInput,
        spec: &ScanSpec,
        ordered: bool,
        keys_only: bool,
        ranked: Option<&RankedAccess>,
        input: Option<Plan>,
    ) -> Lowers<Plan> {
        if ordered || keys_only {
            return Err(OmniError::manifest_internal(format!(
                "scan {id} declares an ordered or keys-only read the engine does not honour"
            ))
            .into());
        }
        let source = match (input, source) {
            (Some(input), ScanInput::Dependent { .. }) => ScanSource::Dependent { input },
            (None, ScanInput::Table) => {
                let mode = match ranked {
                    Some(ranked) => self.lowering.search_mode(id, spec, ranked, self.pass)?,
                    None => SearchMode::default(),
                };
                self.proven_empty |= mode.answer_proven_empty;
                ScanSource::Table {
                    mode: Box::new(mode),
                    report: Arc::clone(&self.report),
                }
            }
            _ => {
                return Err(OmniError::manifest_internal(format!(
                    "scan {id} was lowered with an input its source does not declare"
                ))
                .into());
            }
        };
        let scan = self.lowering.scan(source, spec)?;
        Ok(self.built(id, scan))
    }

    fn hash_join(
        &mut self,
        id: NodeId,
        fields: HashJoinFields<'_>,
        probe: Plan,
        build: Plan,
    ) -> Lowers<Plan> {
        let lookup = self.lowering.lookup(fields.spec)?;
        if lookup.binding != fields.binding {
            return Err(OmniError::manifest_internal(format!(
                "hash join {id} joins `${}` over a scan of `${}`",
                fields.binding, lookup.binding
            ))
            .into());
        }
        let join = HashJoinExec::try_new(probe, build, fields.fallback, lookup)?;
        Ok(self.built(id, join))
    }

    fn sort_merge_join(
        &mut self,
        _id: NodeId,
        _fields: SortMergeJoinFields<'_>,
        _left: Plan,
        _right: Plan,
    ) -> Lowers<Plan> {
        Err(Self::not_a_pipeline_node("SortMergeJoin"))
    }

    fn hydrate_by_address(&mut self, _id: NodeId, _side: SideId, _input: Plan) -> Lowers<Plan> {
        Err(Self::not_a_pipeline_node("HydrateByAddress"))
    }

    fn row_compare(&mut self, _id: NodeId, _input: Plan) -> Lowers<Plan> {
        Err(Self::not_a_pipeline_node("RowCompare"))
    }

    fn classify_three_way(&mut self, _id: NodeId, _input: Plan) -> Lowers<Plan> {
        Err(Self::not_a_pipeline_node("ClassifyThreeWay"))
    }

    fn limit(&mut self, id: NodeId, rows: usize, input: Plan) -> Lowers<Plan> {
        Ok(self.built(id, LimitExec::new(input, rows)))
    }

    fn page(
        &mut self,
        _id: NodeId,
        _rows: usize,
        _bytes: u64,
        _resume: Option<&str>,
        _input: Plan,
    ) -> Lowers<Plan> {
        Err(Self::not_a_pipeline_node("Page"))
    }

    fn cross_join(&mut self, id: NodeId, left: Plan, right: Plan) -> Lowers<Plan> {
        let join = CrossJoinExec::try_new(left, right)?;
        Ok(self.built(id, join))
    }

    fn filter(&mut self, id: NodeId, filters: &[IRExpr], input: Plan) -> Lowers<Plan> {
        if !self.proven_empty {
            self.in_memory_filters += filters.len();
        }
        let filter = FilterExec::new(input, filters.to_vec(), Arc::clone(self.lowering.params()));
        Ok(self.built(id, filter))
    }

    fn expand(&mut self, id: NodeId, fields: ExpandFields<'_>, input: Plan) -> Lowers<Plan> {
        let step = ExpandStep {
            src: fields.src.to_string(),
            dst: fields.dst.to_string(),
            edge_type: fields.edge_type.to_string(),
            direction: fields.direction,
            dst_type: fields.dst_type.to_string(),
            min_hops: fields.min_hops,
            max_hops: fields.max_hops.ok_or_else(|| {
                OmniError::manifest_internal("the read engine requires a bounded traversal")
            })?,
            edge_binding: fields.edge_binding.map(str::to_string),
            mode: fields.mode,
            frontier_estimate: fields.frontier_estimate,
            origin: ExpandStep::origin(fields.policy),
        };
        let expand = ExpandExec::try_new(input, step, Arc::clone(&self.scope.env))?;
        Ok(self.built(id, expand))
    }

    fn anti_join_outer(&mut self, _id: NodeId, _outer_var: &str, outer: &Plan) -> Lowers<()> {
        let outer_schema = outer.schema();
        let tag_column = fresh_tag_column(&outer_schema);
        self.outers.push(OuterScope {
            slot: Arc::new(OuterSlot::default()),
            schema: tagged_schema(&outer_schema, &tag_column),
            tag_column,
        });
        Ok(())
    }

    fn anti_join(&mut self, id: NodeId, outer_var: &str, outer: Plan, inner: Plan) -> Lowers<Plan> {
        let scope = self.outers.pop().ok_or_else(|| {
            OmniError::manifest_internal("AntiJoin closed with no outer scope open".to_string())
        })?;
        let PhysicalNode::AntiJoin {
            inner: inner_id,
            predicate,
            ..
        } = self.lowering.node(id)?
        else {
            return Err(Self::not_a_pipeline_node("AntiJoin"));
        };
        let bulk = self
            .lowering
            .bulk_row_count(*inner_id, outer_var, predicate)?;
        let mask = AntiJoinMaskExec::new(
            outer,
            inner,
            outer_var.to_string(),
            predicate.clone(),
            Arc::clone(self.lowering.params()),
            scope.tag_column,
            scope.slot,
            bulk,
            Arc::clone(&self.scope.env),
        );
        Ok(self.built(id, mask))
    }

    fn outer_reference(&mut self, id: NodeId, outer_var: &str) -> Lowers<Plan> {
        let scope = self.outers.last().ok_or_else(|| {
            OmniError::manifest_internal(
                "OuterReference outside an AntiJoin inner tree".to_string(),
            )
        })?;
        let leaf = OuterReferenceExec::new(
            outer_var.to_string(),
            Arc::clone(&scope.slot),
            Arc::clone(&scope.schema),
        );
        Ok(self.built(id, leaf))
    }

    fn rank_fuse(
        &mut self,
        id: NodeId,
        arms: &[RankArm; 2],
        k: Option<&IRExpr>,
        limit: Option<usize>,
        primary: Plan,
        secondary: Plan,
    ) -> Lowers<Plan> {
        let limit = limit.ok_or_else(|| {
            OmniError::manifest("rrf() ordering requires a limit clause".to_string())
        })?;
        let k = k
            .and_then(|k| resolve_to_int(k, self.lowering.params()))
            .map(|k| u32::try_from(k).unwrap_or(u32::MAX))
            .unwrap_or(RRF_DEFAULT_K);
        let id_column = format!(
            "{}.{}",
            arms[0].binding, self.lowering.catalog.system_columns.id
        );
        let order = |arm: &RankArm| {
            let (property, descending) = arm.kind.score();
            ArmOrder {
                score_column: format!("{}.{property}", arm.binding),
                descending,
            }
        };
        let fuse = RankFuseExec::new(
            primary,
            secondary,
            RrfMode { k, limit },
            id_column,
            [order(&arms[0]), order(&arms[1])],
        );
        Ok(self.built(id, fuse))
    }

    /// The return expressions, each under its alias or its own name, and
    /// when a `Sort` consumes this node, the sort's columns hidden after them.
    fn projection(
        &mut self,
        id: NodeId,
        return_exprs: &[IRProjection],
        input: Plan,
    ) -> Lowers<Plan> {
        if !self.outers.is_empty() {
            return Err(Self::not_a_pipeline_node("Projection"));
        }
        if return_exprs.is_empty() {
            return Err(OmniError::manifest("query has no return projections".to_string()).into());
        }
        let input_schema = input.schema();
        let mut exprs: Vec<(Arc<dyn PhysicalExpr>, String)> =
            Vec::with_capacity(return_exprs.len());
        for proj in return_exprs {
            let projected = Projected::Expression(proj.expr.clone());
            let name = return_name(proj)?;
            exprs.push((self.lowering.projection(projected, &self.scope), name));
        }
        if let Some((keys, tiebreak)) = self.lowering.sort_above(id) {
            let hidden = hidden_columns(
                keys,
                tiebreak,
                &input_schema,
                self.lowering.catalog.system_columns.id,
            )?;
            for name in hidden {
                let (index, _) = input_schema
                    .column_with_name(&name)
                    .expect("hidden columns come from the input schema");
                exprs.push((
                    Arc::new(Column::new(&name, index)),
                    format!("{HIDDEN}{name}"),
                ));
            }
        }
        let projection = ProjectionExec::try_new(exprs, input).map_err(OmniError::datafusion)?;
        Ok(self.built(id, projection))
    }

    /// `AggregateExec` (`Single`) over GQ group keys and arguments (`sum`/`avg`
    /// inputs cast to `Float64` as v1 computes them); it emits groups first
    /// and `run_plan` restores the return order.
    fn aggregate(
        &mut self,
        id: NodeId,
        return_exprs: &[IRProjection],
        input: Plan,
    ) -> Lowers<Plan> {
        if !self.outers.is_empty() {
            return Err(Self::not_a_pipeline_node("Aggregate"));
        }
        let input_schema = input.schema();
        let mut groups: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::new();
        let mut aggregates = Vec::new();
        for proj in return_exprs {
            let name = return_name(proj)?;
            match &proj.expr {
                IRExpr::Aggregate { func, arg } => {
                    let projected = aggregate_argument(func, arg);
                    let mut argument = self.lowering.projection(projected, &self.scope);
                    let data_type = argument
                        .data_type(&input_schema)
                        .map_err(OmniError::datafusion)?;
                    if matches!(func, AggFunc::Sum | AggFunc::Avg)
                        && data_type.is_numeric()
                        && data_type != DataType::Float64
                    {
                        argument = Arc::new(CastExpr::new(argument, DataType::Float64, None));
                    }
                    let udaf = match func {
                        AggFunc::Count => count_udaf(),
                        AggFunc::Sum => sum_udaf(),
                        AggFunc::Avg => avg_udaf(),
                        AggFunc::Min => min_udaf(),
                        AggFunc::Max => max_udaf(),
                    };
                    let aggregate = AggregateExprBuilder::new(udaf, vec![argument])
                        .schema(Arc::clone(&input_schema))
                        .alias(name)
                        .build()
                        .map(Arc::new)
                        .map_err(OmniError::datafusion)?;
                    aggregates.push(aggregate);
                }
                _ => {
                    let projected = Projected::Expression(proj.expr.clone());
                    groups.push((self.lowering.projection(projected, &self.scope), name));
                }
            }
        }
        let filters = vec![None; aggregates.len()];
        let aggregate = AggregateExec::try_new(
            AggregateMode::Single,
            PhysicalGroupBy::new_single(groups),
            aggregates,
            filters,
            input,
            input_schema,
        )
        .map_err(OmniError::datafusion)?;
        Ok(self.built(id, aggregate))
    }

    /// The node's keys over the columns its input carries (hidden under the
    /// projection below, plain over an aggregate or a count), then the
    /// declared tie-break ids, ascending nulls first.
    fn sort(
        &mut self,
        id: NodeId,
        order_by: &[IROrdering],
        fetch: Option<usize>,
        tiebreak: &[String],
        input: Plan,
    ) -> Lowers<Plan> {
        if !self.outers.is_empty() {
            return Err(Self::not_a_pipeline_node("Sort"));
        }
        let schema = input.schema();
        let carried = |name: &str| -> Option<String> {
            let hidden = format!("{HIDDEN}{name}");
            if schema.column_with_name(&hidden).is_some() {
                Some(hidden)
            } else if schema.column_with_name(name).is_some() {
                Some(name.to_string())
            } else {
                None
            }
        };
        let mut keys = Vec::with_capacity(order_by.len() + tiebreak.len());
        for key in order_by {
            let column = match &key.expr {
                IRExpr::PropAccess { variable, property } => {
                    let name = format!("{variable}.{property}");
                    carried(&name).ok_or_else(|| {
                        OmniError::manifest_internal(format!(
                            "the planned sort key column '{name}' is not in the sort input"
                        ))
                    })?
                }
                IRExpr::AliasRef(alias) => carried(alias).ok_or_else(|| {
                    OmniError::manifest_internal(format!(
                        "the planned sort alias '{alias}' is not in the sort input"
                    ))
                })?,
                _ => {
                    return Err(OmniError::manifest_internal(
                        "the planned sort key is not a property or an alias".to_string(),
                    )
                    .into());
                }
            };
            keys.push(SortKey {
                column,
                descending: key.descending,
                nulls_first: !key.descending,
            });
        }
        for binding in tiebreak {
            let name = format!("{binding}.{}", self.lowering.catalog.system_columns.id);
            let column = carried(&name).ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "the planned tie-break column '{name}' is not in the sort input"
                ))
            })?;
            keys.push(SortKey {
                column,
                descending: false,
                nulls_first: true,
            });
        }
        let sort = SortExec::try_new(input, keys, fetch)?;
        Ok(self.built(id, sort))
    }

    fn finish(&mut self, _root: NodeId, op: Plan) -> Lowers<Plan> {
        Ok(op)
    }
}

/// The argument a GQ aggregate runs over: `count($v)` counts the binding's
/// identity, every other aggregate its expression.
fn aggregate_argument(func: &AggFunc, arg: &IRExpr) -> Projected {
    match (func, arg) {
        (AggFunc::Count, IRExpr::Variable(variable)) => Projected::Identity(variable.clone()),
        _ => Projected::Expression(arg.clone()),
    }
}

/// The output column name of one return projection: its alias, else the
/// name its expression (or aggregate argument) projects under.
fn return_name(proj: &IRProjection) -> Result<String> {
    match &proj.alias {
        Some(alias) => Ok(alias.clone()),
        None => match &proj.expr {
            IRExpr::Aggregate { func, arg } => aggregate_argument(func, arg).name(),
            expr => Projected::Expression(expr.clone()).name(),
        },
    }
}

/// The wide columns the return projection carries for the sort above it, each
/// once: the `PropAccess` keys and the declared tie-break ids, which the same
/// planner pass projects, so a missing one is a planner defect.
fn hidden_columns(
    keys: &[IROrdering],
    tiebreak: &[String],
    input_schema: &Schema,
    id_column: &str,
) -> Result<Vec<String>> {
    let mut hidden: Vec<String> = Vec::new();
    for key in keys {
        match &key.expr {
            IRExpr::PropAccess { variable, property } => {
                let name = format!("{variable}.{property}");
                if input_schema.column_with_name(&name).is_none() {
                    return Err(OmniError::manifest_internal(format!(
                        "the planned sort key column '{name}' is not in the sort input"
                    )));
                }
                if !hidden.contains(&name) {
                    hidden.push(name);
                }
            }
            IRExpr::AliasRef(_) => {}
            _ => {
                return Err(OmniError::manifest_internal(
                    "the planned sort key is not a property or an alias".to_string(),
                ));
            }
        }
    }
    for binding in tiebreak {
        let name = format!("{binding}.{id_column}");
        if input_schema.column_with_name(&name).is_none() {
            return Err(OmniError::manifest_internal(format!(
                "the planned tie-break column '{name}' is not in the sort input"
            )));
        }
        if !hidden.contains(&name) {
            hidden.push(name);
        }
    }
    Ok(hidden)
}

/// The columns of `batch` in `names`' order, for a root whose operator emits
/// them in another: DataFusion's aggregate puts its group keys first.
pub(super) fn in_order(batch: RecordBatch, names: &[String]) -> Result<RecordBatch> {
    let schema = batch.schema();
    let current: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    if current.iter().copied().eq(names.iter().map(String::as_str)) {
        return Ok(batch);
    }
    let indices = names
        .iter()
        .map(|name| {
            schema.index_of(name).map_err(|_| {
                OmniError::manifest_internal(format!(
                    "the result carries no column '{name}' the return declares"
                ))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    batch.project(&indices).map_err(OmniError::arrow_internal)
}

/// The return's column names in return order, for the root of `plan`.
pub(super) fn return_order(plan: &PhysicalPlan) -> Result<Option<Vec<String>>> {
    let Some(returns) = plan.live().find_map(|(_, node)| match node {
        PhysicalNode::Aggregate { return_exprs, .. } => Some(return_exprs),
        _ => None,
    }) else {
        return Ok(None);
    };
    returns
        .iter()
        .map(return_name)
        .collect::<Result<Vec<_>>>()
        .map(Some)
}
