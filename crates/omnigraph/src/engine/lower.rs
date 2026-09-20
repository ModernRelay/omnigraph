//! Lowers a read `PhysicalPlan` to one DataFusion physical plan. Relational
//! nodes use DataFusion operators; scans and graph nodes use omnigraph's
//! operators. Search markers parameterize scans. Filter placement, scan
//! projection and pushed limits come from the physical nodes.

use std::sync::Mutex;

use arrow_schema::{SchemaRef, SortOptions};
use datafusion::common::{JoinType, NullEquality};
use datafusion::functions_aggregate::average::avg_udaf;
use datafusion::functions_aggregate::count::count_udaf;
use datafusion::functions_aggregate::min_max::{max_udaf, min_udaf};
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion::physical_expr::aggregate::AggregateExprBuilder;
use datafusion::physical_expr::expressions::{CastExpr, Column};
use datafusion::physical_expr::{LexOrdering, PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{CrossJoinExec, HashJoinExec, PartitionMode};
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use lance_datafusion::exec::HardCapBatchSizeExec;
use omnigraph_planner::{
    AccessPath, NodeId, PhysicalNode, PhysicalPlan, Predicate, ScanInput, ScanSpec,
};

use super::adapters::{GqFilterExpr, GqProjectionExpr, LoweringId, Projected};
use super::operators::{
    AntiJoinMaskExec, ChargeExec, ExpandExec, ExpandStep, GraphEnv, HashFallbackExec,
    HashProbeExec, MetadataCountExec, OuterReferenceExec, OuterSlot, RankFuseExec, ScanExec,
    ScanSource, fresh_tag_column, joined_schema, tagged_schema,
};
use super::*;

type Plan = Arc<dyn ExecutionPlan>;

fn capped_sort_input(input: Plan, memory_limit: u64) -> Plan {
    let bytes = crate::table_store::sort_input_batch_bytes(memory_limit);
    Arc::new(HardCapBatchSizeExec::new(input, bytes))
}

/// The prefix of a column the return projection carries for the sort above
/// it and the final projection drops; no GQ alias or `binding.property`
/// name starts with it.
const HIDDEN: &str = "~";

pub(super) struct Lowering<'a> {
    pub(super) plan: &'a PhysicalPlan,
    pub(super) ir: &'a QueryIR,
    pub(super) params: &'a ResolvedParams,
    pub(super) snapshot: &'a Snapshot,
    pub(super) graph_index: &'a Arc<GraphIndexHandle>,
    pub(super) catalog: &'a Arc<Catalog>,
    pub(super) settings: &'a SessionSettings,
    /// The query pool's size, which caps a sort's input batches.
    pub(super) memory_limit: u64,
}

/// How one pass runs the tree: under one search mode, or as the two legs of
/// an `rrf()` fusion under the arm modes the prefilter gate settled.
pub(super) enum RunMode<'m> {
    Single(&'m SearchMode),
    Fused {
        rrf: &'m RrfMode,
        primary: &'m SearchMode,
        secondary: &'m SearchMode,
    },
}

pub(super) struct Lowered {
    pub(super) root: Plan,
    /// What the ranked scan reported in this pass, for the overfetch ladder.
    pub(super) report: Arc<Mutex<ScanReport>>,
    /// The `FilterExec`s of the tree, one per in-memory filter application.
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
    in_memory_filters: std::cell::Cell<usize>,
}

#[derive(Clone, Copy)]
struct SortSpec<'p> {
    order_by: &'p [IROrdering],
    fetch: Option<usize>,
}

struct SortKeys {
    keys: Vec<IROrdering>,
    fetch: Option<usize>,
}

/// The tagged outer rows an `AntiJoin` inner tree reads at its leaf.
struct OuterSource {
    slot: Arc<OuterSlot>,
    schema: SchemaRef,
}

/// The plan above the pipeline: `Page` over `Sort` over the return node,
/// with the search markers between the return and the pipeline root.
struct Top<'p> {
    limit: Option<usize>,
    sort: Option<SortSpec<'p>>,
    return_exprs: &'p [IRProjection],
    kind: ReturnKind,
    pipeline: NodeId,
}

#[derive(Clone, Copy)]
enum ReturnKind {
    Projection,
    Aggregate,
    MetadataCount,
}

impl<'a> Lowering<'a> {
    fn node(&self, id: NodeId) -> Result<&'a PhysicalNode> {
        self.plan.node(id).ok_or_else(|| {
            OmniError::manifest_internal(format!("physical node {id} is a tombstone"))
        })
    }

    fn top(&self) -> Result<Top<'a>> {
        let mut id = self.plan.root();
        let mut limit = None;
        if let PhysicalNode::Limit { input, rows } = self.node(id)? {
            limit = Some(*rows);
            id = *input;
        }
        let mut sort = None;
        if let PhysicalNode::Sort {
            input,
            order_by,
            fetch,
        } = self.node(id)?
        {
            sort = Some(SortSpec {
                order_by: order_by.as_slice(),
                fetch: *fetch,
            });
            id = *input;
        }
        let (return_exprs, kind, mut pipeline) = match self.node(id)? {
            PhysicalNode::Projection {
                input,
                return_exprs,
            } => (return_exprs.as_slice(), ReturnKind::Projection, *input),
            PhysicalNode::Aggregate {
                input,
                return_exprs,
            } => (return_exprs.as_slice(), ReturnKind::Aggregate, *input),
            PhysicalNode::MetadataCount { return_exprs, .. } => {
                (return_exprs.as_slice(), ReturnKind::MetadataCount, id)
            }
            other => {
                return Err(OmniError::manifest_internal(format!(
                    "a read plan returns through Projection, Aggregate or MetadataCount, found {}",
                    other.name()
                )));
            }
        };
        while let PhysicalNode::Nearest { input, .. }
        | PhysicalNode::TextSearch { input, .. }
        | PhysicalNode::RankFuse { input, .. } = self.node(pipeline)?
        {
            pipeline = *input;
        }
        Ok(Top {
            limit,
            sort,
            return_exprs,
            kind,
            pipeline,
        })
    }

    /// The whole tree for one pass, executable under the query's context.
    pub(super) fn lower_query(&self, run: &RunMode<'_>) -> Result<Lowered> {
        let report = Arc::new(Mutex::new(ScanReport::default()));
        let scope = Scope {
            id: LoweringId::next(),
            env: Arc::new(GraphEnv {
                graph_index: Arc::clone(self.graph_index),
                snapshot: self.snapshot.clone(),
                catalog: Arc::clone(self.catalog),
            }),
            ctx: Arc::new(ProjectionContext::for_query(self.catalog, self.ir)),
            in_memory_filters: std::cell::Cell::new(0),
        };
        let top = self.top()?;
        let no_search = SearchMode::default();
        let (source, mode, sort, limit) = match run {
            RunMode::Single(mode) => (
                self.pipeline_source(top.pipeline, mode, &scope, &report)?,
                *mode,
                top.sort,
                top.limit,
            ),
            RunMode::Fused {
                rrf,
                primary,
                secondary,
            } => {
                let primary_var = rrf
                    .primary
                    .nearest
                    .as_ref()
                    .map(|(v, ..)| v.as_str())
                    .or_else(|| rrf.primary.bm25.as_ref().map(|(v, ..)| v.as_str()))
                    .ok_or_else(|| {
                        OmniError::manifest("rrf primary must be nearest or bm25".to_string())
                    })?;
                let id_column = format!("{}.{}", primary_var, self.catalog.system_columns.id);
                let primary_plan = self.pipeline_source(top.pipeline, primary, &scope, &report)?;
                let secondary_plan =
                    self.pipeline_source(top.pipeline, secondary, &scope, &report)?;
                let primary = self.ranked_arm(primary_plan, primary, &id_column)?;
                let secondary = self.ranked_arm(secondary_plan, secondary, &id_column)?;
                let fused: Plan = Arc::new(RankFuseExec::new(
                    primary,
                    secondary,
                    (*rrf).clone(),
                    id_column,
                ));
                (fused, &no_search, None, Some(rrf.limit))
            }
        };
        let root = match top.kind {
            ReturnKind::Aggregate => {
                self.aggregate_top(source, top.return_exprs, sort, limit, mode, &scope)?
            }
            ReturnKind::Projection => {
                self.projection_top(source, top.return_exprs, sort, limit, mode, &scope)?
            }
            ReturnKind::MetadataCount => self.aggregate_finish(source, sort, limit, mode)?,
        };
        let no_rows = limit == Some(0) || sort.is_some_and(|sort| sort.fetch == Some(0));
        if no_rows {
            return Ok(Lowered {
                root: Arc::new(EmptyExec::new(root.schema())),
                report,
                in_memory_filters: 0,
            });
        }
        Ok(Lowered {
            root,
            report,
            in_memory_filters: scope.in_memory_filters.get(),
        })
    }

    /// A proven-empty ranked source cannot feed this pipeline. Keep its schema
    /// below the return operators so a global count still produces zero.
    fn pipeline_source(
        &self,
        id: NodeId,
        mode: &SearchMode,
        scope: &Scope,
        report: &Arc<Mutex<ScanReport>>,
    ) -> Result<Plan> {
        let filters_before = scope.in_memory_filters.get();
        let source = self.pipeline(id, mode, None, scope, report)?;
        if mode
            .nearest
            .as_ref()
            .is_some_and(|(variable, ..)| mode.scan_proven_empty(variable))
        {
            scope.in_memory_filters.set(filters_before);
            return Ok(Arc::new(EmptyExec::new(source.schema())));
        }
        Ok(source)
    }

    fn ranked_arm(&self, input: Plan, mode: &SearchMode, id_column: &str) -> Result<Plan> {
        let orderings = search_score_orderings(mode).ok_or_else(|| {
            OmniError::manifest_internal("RRF arm has no ranking expression".to_string())
        })?;
        let schema = input.schema();
        let mut keys = Vec::with_capacity(orderings.len() + 1);
        for ordering in orderings {
            let IRExpr::PropAccess { variable, property } = ordering.expr else {
                return Err(OmniError::manifest_internal(
                    "search ranking must name a score column".to_string(),
                ));
            };
            let name = format!("{variable}.{property}");
            let expr = column(&schema, &name).ok_or_else(|| {
                OmniError::manifest_internal(format!("RRF arm has no ranking column '{name}'"))
            })?;
            keys.push(PhysicalSortExpr::new(
                expr,
                SortOptions {
                    descending: ordering.descending,
                    nulls_first: false,
                },
            ));
        }
        let identity = column(&schema, id_column).ok_or_else(|| {
            OmniError::manifest_internal(format!("RRF arm has no identity column '{id_column}'"))
        })?;
        keys.push(PhysicalSortExpr::new(
            identity,
            SortOptions {
                descending: false,
                nulls_first: false,
            },
        ));
        let id_suffix = format!(".{}", self.catalog.system_columns.id);
        let mut tiebreaks: Vec<&str> = schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .filter(|name| name.ends_with(&id_suffix) && *name != id_column)
            .collect();
        tiebreaks.sort_unstable();
        for name in tiebreaks {
            keys.push(PhysicalSortExpr::new(
                column(&schema, name).expect("tie-break columns come from the schema"),
                SortOptions {
                    descending: false,
                    nulls_first: true,
                },
            ));
        }
        let order = LexOrdering::new(keys).expect("RRF arm has score and identity keys");
        Ok(Arc::new(SortExec::new(
            order,
            capped_sort_input(input, self.memory_limit),
        )))
    }

    /// The pipeline rooted at `id`: a scan starts it, every other node is
    /// an operator over its input.
    fn pipeline(
        &self,
        id: NodeId,
        mode: &SearchMode,
        outer: Option<&OuterSource>,
        scope: &Scope,
        report: &Arc<Mutex<ScanReport>>,
    ) -> Result<Plan> {
        match self.node(id)? {
            PhysicalNode::MetadataCount { spec, .. } => {
                let properties = self.plan.properties(id).ok_or_else(|| {
                    OmniError::manifest_internal(
                        "metadata count has no declared schema".to_string(),
                    )
                })?;
                Ok(Arc::new(MetadataCountExec::new(
                    spec.table.type_key.clone(),
                    self.snapshot.clone(),
                    Arc::clone(&properties.schema),
                )))
            }
            PhysicalNode::Scan { source, spec, .. } => {
                let input = match source {
                    ScanInput::Table => None,
                    ScanInput::Dependent { input, .. } => {
                        Some(self.pipeline(*input, mode, outer, scope, report)?)
                    }
                };
                if let (
                    Some(probe),
                    ScanInput::Dependent {
                        access: AccessPath::HashJoin,
                        ..
                    },
                ) = (&input, source)
                {
                    return self.hash_join_scan(Arc::clone(probe), spec, report);
                }
                self.scan(
                    match input {
                        Some(input) => ScanSource::Dependent { input },
                        None => ScanSource::Table {
                            mode: Box::new(mode.clone()),
                            report: Arc::clone(report),
                        },
                    },
                    spec,
                )
            }
            PhysicalNode::CrossJoin { left, right } => {
                let left = self.pipeline(*left, mode, outer, scope, report)?;
                let right = self.pipeline(*right, mode, outer, scope, report)?;
                joined_schema(&left.schema(), &right.schema())?;
                Ok(Arc::new(ChargeExec::new(
                    Arc::new(CrossJoinExec::new(left, right)),
                    "cross join output",
                )))
            }
            PhysicalNode::Filter { input, filters } => {
                let mut plan = self.pipeline(*input, mode, outer, scope, report)?;
                for filter in filters {
                    scope
                        .in_memory_filters
                        .set(scope.in_memory_filters.get() + 1);
                    let predicate: Arc<dyn PhysicalExpr> = Arc::new(GqFilterExpr::new(
                        filter.clone(),
                        Arc::clone(self.params.shared()),
                        scope.id,
                    ));
                    plan = Arc::new(
                        FilterExec::try_new(predicate, plan).map_err(OmniError::datafusion)?,
                    );
                }
                Ok(plan)
            }
            PhysicalNode::Expand {
                input,
                src,
                dst,
                edge_type,
                direction,
                dst_type,
                min_hops,
                max_hops,
                edge_binding,
                mode: expand_mode,
                frontier_estimate,
                cost,
            } => {
                let input = self.pipeline(*input, mode, outer, scope, report)?;
                let step = ExpandStep {
                    src: src.clone(),
                    dst: dst.clone(),
                    edge_type: edge_type.clone(),
                    direction: *direction,
                    dst_type: dst_type.clone(),
                    min_hops: *min_hops,
                    max_hops: max_hops.ok_or_else(|| {
                        OmniError::manifest_internal("the read engine requires a bounded traversal")
                    })?,
                    edge_binding: edge_binding.clone(),
                    mode: *expand_mode,
                    frontier_estimate: *frontier_estimate,
                    origin: ExpandStep::origin(
                        self.settings.traversal() != Traversal::Auto,
                        cost.clone(),
                    ),
                };
                Ok(Arc::new(ExpandExec::try_new(
                    input,
                    step,
                    Arc::clone(&scope.env),
                )?))
            }
            PhysicalNode::AntiJoin {
                input,
                inner,
                outer_var,
            } => {
                let outer_plan = self.pipeline(*input, mode, outer, scope, report)?;
                let outer_schema = outer_plan.schema();
                let tag_column = fresh_tag_column(&outer_schema);
                let source = OuterSource {
                    slot: Arc::new(OuterSlot::default()),
                    schema: tagged_schema(&outer_schema, &tag_column),
                };
                let inner_report = Arc::new(Mutex::new(ScanReport::default()));
                let inner_plan = self.pipeline(
                    *inner,
                    &SearchMode::default(),
                    Some(&source),
                    scope,
                    &inner_report,
                )?;
                let bulk = self.bulk_negation(*inner, outer_var)?;
                Ok(Arc::new(AntiJoinMaskExec::new(
                    outer_plan,
                    inner_plan,
                    outer_var.clone(),
                    tag_column,
                    source.slot,
                    bulk,
                    Arc::clone(&scope.env),
                )))
            }
            PhysicalNode::OuterReference { outer_var } => {
                let source = outer.ok_or_else(|| {
                    OmniError::manifest_internal(
                        "OuterReference outside an AntiJoin inner tree".to_string(),
                    )
                })?;
                Ok(Arc::new(OuterReferenceExec::new(
                    outer_var.clone(),
                    Arc::clone(&source.slot),
                    Arc::clone(&source.schema),
                )))
            }
            PhysicalNode::Nearest { input, .. }
            | PhysicalNode::TextSearch { input, .. }
            | PhysicalNode::RankFuse { input, .. } => {
                self.pipeline(*input, mode, outer, scope, report)
            }
            other => Err(OmniError::manifest_internal(format!(
                "`{}` is not a pipeline node of a read plan",
                other.name()
            ))),
        }
    }

    fn scan(&self, source: ScanSource, spec: &ScanSpec) -> Result<Plan> {
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
        let filters: Vec<IRFilter> = spec
            .filter
            .as_ref()
            .map(Predicate::gq_filters)
            .unwrap_or_default();
        let projection = spec
            .projection
            .as_ref()
            .map(|columns| NeededColumns(columns.iter().cloned().collect()));
        Ok(Arc::new(ScanExec::try_new(
            source,
            type_name.to_string(),
            binding.to_string(),
            filters,
            projection,
            Arc::clone(self.params.shared()),
            self.snapshot.clone(),
            Arc::clone(self.catalog),
        )?))
    }

    /// The `hash_join` access path of a dependent scan: the destination table
    /// read once as the build side (a root `ScanExec` under no search mode, as
    /// the per-batch read applies none), the traversal as the probe side.
    fn hash_join_scan(
        &self,
        probe: Plan,
        spec: &ScanSpec,
        report: &Arc<Mutex<ScanReport>>,
    ) -> Result<Plan> {
        let probe_schema = probe.schema();
        let lookup = self.scan(
            ScanSource::Dependent {
                input: Arc::clone(&probe),
            },
            spec,
        )?;
        let declared = lookup.schema();
        let build = self.scan(
            ScanSource::Table {
                mode: Box::default(),
                report: Arc::clone(report),
            },
            spec,
        )?;
        let binding = spec
            .binding
            .as_deref()
            .ok_or_else(|| OmniError::manifest_internal("a hash-join scan requires a binding"))?;
        let build_schema = build.schema();
        let id = format!("{binding}.{}", self.catalog.system_columns.id);
        let build_key = column(&build_schema, &id).ok_or_else(|| {
            OmniError::manifest_internal(format!("destination scan projects no '{id}'"))
        })?;
        let probe_key = column(&probe_schema, &id).ok_or_else(|| {
            OmniError::manifest_internal(format!("traversal output carries no '{id}'"))
        })?;
        let joined: Plan = Arc::new(ChargeExec::new(
            Arc::new(
                HashJoinExec::try_new(
                    build,
                    Arc::new(HashProbeExec::new(probe)),
                    vec![(build_key, probe_key)],
                    None,
                    &JoinType::Inner,
                    None,
                    PartitionMode::CollectLeft,
                    NullEquality::NullEqualsNothing,
                    false,
                )
                .map_err(OmniError::datafusion)?,
            ),
            "hash join output",
        ));
        let build_width = build_schema.fields().len();
        let exprs = declared
            .fields()
            .iter()
            .map(|field| {
                let name = field.name();
                let index = match probe_schema.index_of(name) {
                    Ok(index) if name != &id => build_width + index,
                    _ => build_schema
                        .index_of(name)
                        .map_err(|error| OmniError::manifest_internal(error.to_string()))?,
                };
                Ok((
                    Arc::new(Column::new(name, index)) as Arc<dyn PhysicalExpr>,
                    name.clone(),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let projected: Plan =
            Arc::new(ProjectionExec::try_new(exprs, joined).map_err(OmniError::datafusion)?);
        let produced = projected.schema();
        let same = produced.fields().len() == declared.fields().len()
            && produced
                .fields()
                .iter()
                .zip(declared.fields())
                .all(|(p, d)| {
                    p.name() == d.name()
                        && p.data_type() == d.data_type()
                        && p.is_nullable() == d.is_nullable()
                });
        if !same {
            return Err(OmniError::manifest_internal(format!(
                "hash join projection produces {produced} where the dependent scan declares {declared}"
            )));
        }
        Ok(Arc::new(HashFallbackExec::new(projected, lookup)))
    }

    /// The edge of a negation the bulk check can answer: one single-hop,
    /// filter-free expand from the outer binding, directly over the outer
    /// rows.
    fn bulk_negation(&self, inner: NodeId, outer_var: &str) -> Result<Option<(String, Direction)>> {
        let inner = match self.node(inner)? {
            PhysicalNode::Scan {
                source: ScanInput::Dependent { input, .. },
                spec,
                ..
            } if spec.filter.is_none() => *input,
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
                ..
            } if src == outer_var
                && *min_hops == 1
                && max_hops.unwrap_or(1) == 1
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
            Arc::clone(self.params.shared()),
            Arc::clone(&scope.ctx),
            scope.id,
        ))
    }

    /// Non-aggregate return: `ProjectionExec` carrying the sort's wide
    /// columns and every `<binding>.<id>` hidden, `SortExec` in
    /// `apply_ordering`'s total order, the limit, then the hidden columns dropped.
    fn projection_top(
        &self,
        source: Plan,
        return_exprs: &[IRProjection],
        sort: Option<SortSpec<'_>>,
        limit: Option<usize>,
        mode: &SearchMode,
        scope: &Scope,
    ) -> Result<Plan> {
        if return_exprs.is_empty() {
            return Err(OmniError::manifest(
                "query has no return projections".to_string(),
            ));
        }
        let input_schema = source.schema();
        let mut exprs: Vec<(Arc<dyn PhysicalExpr>, String)> =
            Vec::with_capacity(return_exprs.len());
        let mut names: Vec<String> = Vec::with_capacity(return_exprs.len());
        for proj in return_exprs {
            let projected = Projected::Expression(proj.expr.clone());
            let name = match &proj.alias {
                Some(alias) => alias.clone(),
                None => projected.name()?,
            };
            exprs.push((self.projection(projected, scope), name.clone()));
            names.push(name);
        }
        let Some(SortKeys { keys, fetch }) = self.sort_keys(sort, limit, mode, &input_schema)?
        else {
            let plan: Plan = Arc::new(ChargeExec::new(
                Arc::new(ProjectionExec::try_new(exprs, source).map_err(OmniError::datafusion)?),
                "projection output",
            ));
            return Ok(limited(plan, limit));
        };

        let id_suffix = format!(".{}", self.catalog.system_columns.id);
        let (hidden, tiebreaks) = hidden_columns(&keys, &input_schema, &id_suffix)?;
        for name in &hidden {
            let (index, _) = input_schema
                .column_with_name(name)
                .expect("hidden columns come from the input schema");
            exprs.push((
                Arc::new(Column::new(name, index)),
                format!("{HIDDEN}{name}"),
            ));
        }
        let projected: Plan = Arc::new(ChargeExec::new(
            Arc::new(ProjectionExec::try_new(exprs, source).map_err(OmniError::datafusion)?),
            "projection output",
        ));
        let schema = projected.schema();
        let mut sort_exprs = Vec::with_capacity(keys.len() + hidden.len());
        for key in &keys {
            let expr = match &key.expr {
                IRExpr::PropAccess { variable, property } => {
                    column(&schema, &format!("{HIDDEN}{variable}.{property}"))
                        .expect("sort columns were carried hidden")
                }
                IRExpr::AliasRef(alias) => column(&schema, alias).ok_or_else(|| {
                    OmniError::manifest(format!("alias '{}' not found for ordering", alias))
                })?,
                _ => {
                    return Err(OmniError::manifest(
                        "unsupported ordering expression".to_string(),
                    ));
                }
            };
            sort_exprs.push(PhysicalSortExpr::new(
                expr,
                SortOptions {
                    descending: key.descending,
                    nulls_first: !key.descending,
                },
            ));
        }
        for name in &tiebreaks {
            let expr = column(&schema, &format!("{HIDDEN}{name}"))
                .expect("id columns were carried hidden");
            sort_exprs.push(PhysicalSortExpr::new(
                expr,
                SortOptions {
                    descending: false,
                    nulls_first: true,
                },
            ));
        }
        let ordering = LexOrdering::new(sort_exprs).expect("a sort has at least one key");
        let sorted: Plan = Arc::new(
            SortExec::new(ordering, capped_sort_input(projected, self.memory_limit))
                .with_fetch(fetch),
        );
        let limited = limited(sorted, limit);
        let visible: Vec<(Arc<dyn PhysicalExpr>, String)> = names
            .iter()
            .enumerate()
            .map(|(index, name)| {
                (
                    Arc::new(Column::new(name, index)) as Arc<dyn PhysicalExpr>,
                    name.clone(),
                )
            })
            .collect();
        Ok(Arc::new(
            ProjectionExec::try_new(visible, limited).map_err(OmniError::datafusion)?,
        ))
    }

    /// The keys a non-aggregate return sorts by and the sort's fetch: the
    /// user's `order` unless the query is search-ordered, then the scan's
    /// score column followed by the user's plain keys.
    fn sort_keys(
        &self,
        sort: Option<SortSpec<'_>>,
        limit: Option<usize>,
        mode: &SearchMode,
        input_schema: &Schema,
    ) -> Result<Option<SortKeys>> {
        if let Some(sort) = sort.filter(|_| !is_search_ordered(mode)) {
            return Ok(Some(SortKeys {
                keys: sort.order_by.to_vec(),
                fetch: sort.fetch,
            }));
        }
        let Some(mut orderings) = search_score_orderings(mode) else {
            return Ok(None);
        };
        let score_col = match &orderings[0].expr {
            IRExpr::PropAccess { variable, property } => format!("{variable}.{property}"),
            _ => String::new(),
        };
        if input_schema.column_with_name(&score_col).is_none() {
            return Err(OmniError::manifest(format!(
                "search-ordered query produced rows without its '{score_col}' ranking column"
            )));
        }
        let extra: &[IROrdering] = sort.map_or(&[], |sort| sort.order_by);
        let extra = match extra.first() {
            Some(first) if is_search_expr(&first.expr) => &extra[1..],
            _ => extra,
        };
        if extra.iter().any(|ordering| is_search_expr(&ordering.expr)) {
            return Err(OmniError::manifest(
                "search functions must lead the order clause; keys after the \
                 search function must be plain expressions"
                    .to_string(),
            ));
        }
        orderings.extend(extra.iter().cloned());
        let fetch = sort.and_then(|sort| sort.fetch).or(limit);
        Ok(Some(SortKeys {
            keys: orderings,
            fetch,
        }))
    }

    /// Aggregate return: `AggregateExec` (`Single`) over projected keys and
    /// arguments (`sum`/`avg` inputs cast to `Float64` as v1 computes them),
    /// reordered to the return order, sorted with the `.<id>` tie-break, limited.
    fn aggregate_top(
        &self,
        source: Plan,
        return_exprs: &[IRProjection],
        sort: Option<SortSpec<'_>>,
        limit: Option<usize>,
        mode: &SearchMode,
        scope: &Scope,
    ) -> Result<Plan> {
        enum Slot {
            Group(usize),
            Agg(usize),
        }
        let mut pre: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::with_capacity(return_exprs.len());
        let mut slots: Vec<Slot> = Vec::with_capacity(return_exprs.len());
        let mut groups: Vec<(String, String)> = Vec::new();
        let mut aggs: Vec<(String, AggFunc, String)> = Vec::new();
        for (i, proj) in return_exprs.iter().enumerate() {
            match &proj.expr {
                IRExpr::Aggregate { func, arg } => {
                    let projected = match (func, arg.as_ref()) {
                        (AggFunc::Count, IRExpr::Variable(variable)) => {
                            Projected::Identity(variable.clone())
                        }
                        _ => Projected::Expression((**arg).clone()),
                    };
                    let name = match &proj.alias {
                        Some(alias) => alias.clone(),
                        None => projected.name()?,
                    };
                    let pre_name = format!("{HIDDEN}agg{i}");
                    pre.push((self.projection(projected, scope), pre_name.clone()));
                    slots.push(Slot::Agg(aggs.len()));
                    aggs.push((pre_name, *func, name));
                }
                _ => {
                    let projected = Projected::Expression(proj.expr.clone());
                    let name = match &proj.alias {
                        Some(alias) => alias.clone(),
                        None => projected.name()?,
                    };
                    let pre_name = format!("{HIDDEN}key{i}");
                    pre.push((self.projection(projected, scope), pre_name.clone()));
                    slots.push(Slot::Group(groups.len()));
                    groups.push((pre_name, name));
                }
            }
        }
        let pre_plan: Plan = Arc::new(ChargeExec::new(
            Arc::new(ProjectionExec::try_new(pre, source).map_err(OmniError::datafusion)?),
            "aggregate arguments",
        ));
        let pre_schema = pre_plan.schema();
        let group_by = PhysicalGroupBy::new_single(
            groups
                .iter()
                .map(|(pre_name, name)| {
                    (
                        column(&pre_schema, pre_name).expect("group keys are projected"),
                        name.clone(),
                    )
                })
                .collect(),
        );
        let aggr_expr = aggs
            .iter()
            .map(|(pre_name, func, alias)| {
                let (index, field) = pre_schema
                    .column_with_name(pre_name)
                    .expect("aggregate arguments are projected");
                let mut arg: Arc<dyn PhysicalExpr> = Arc::new(Column::new(pre_name, index));
                if matches!(func, AggFunc::Sum | AggFunc::Avg)
                    && field.data_type().is_numeric()
                    && field.data_type() != &DataType::Float64
                {
                    arg = Arc::new(CastExpr::new(arg, DataType::Float64, None));
                }
                let udaf = match func {
                    AggFunc::Count => count_udaf(),
                    AggFunc::Sum => sum_udaf(),
                    AggFunc::Avg => avg_udaf(),
                    AggFunc::Min => min_udaf(),
                    AggFunc::Max => max_udaf(),
                };
                AggregateExprBuilder::new(udaf, vec![arg])
                    .schema(Arc::clone(&pre_schema))
                    .alias(alias.clone())
                    .build()
                    .map(Arc::new)
                    .map_err(OmniError::datafusion)
            })
            .collect::<Result<Vec<_>>>()?;
        let filter_expr = vec![None; aggr_expr.len()];
        let aggregated: Plan = Arc::new(
            AggregateExec::try_new(
                AggregateMode::Single,
                group_by,
                aggr_expr,
                filter_expr,
                pre_plan,
                pre_schema,
            )
            .map_err(OmniError::datafusion)?,
        );
        let aggregated: Plan = Arc::new(ChargeExec::new(aggregated, "aggregate output"));
        let ordered: Vec<(Arc<dyn PhysicalExpr>, String)> = slots
            .iter()
            .map(|slot| {
                let (index, name) = match slot {
                    Slot::Group(group) => (*group, &groups[*group].1),
                    Slot::Agg(agg) => (groups.len() + *agg, &aggs[*agg].2),
                };
                (
                    Arc::new(Column::new(name, index)) as Arc<dyn PhysicalExpr>,
                    name.clone(),
                )
            })
            .collect();
        let plan: Plan =
            Arc::new(ProjectionExec::try_new(ordered, aggregated).map_err(OmniError::datafusion)?);
        self.aggregate_finish(plan, sort, limit, mode)
    }

    fn aggregate_finish(
        &self,
        mut plan: Plan,
        sort: Option<SortSpec<'_>>,
        limit: Option<usize>,
        mode: &SearchMode,
    ) -> Result<Plan> {
        if let Some(SortSpec { order_by, fetch }) = sort.filter(|_| !is_search_ordered(mode)) {
            let schema = plan.schema();
            let mut sort_exprs = Vec::with_capacity(order_by.len());
            for key in order_by {
                let expr = match &key.expr {
                    IRExpr::PropAccess { variable, property } => {
                        let name = format!("{variable}.{property}");
                        column(&schema, &name).ok_or_else(|| {
                            OmniError::manifest(format!("column '{}' not found for ordering", name))
                        })?
                    }
                    IRExpr::AliasRef(alias) => column(&schema, alias).ok_or_else(|| {
                        OmniError::manifest(format!("alias '{}' not found for ordering", alias))
                    })?,
                    _ => {
                        return Err(OmniError::manifest(
                            "unsupported ordering expression".to_string(),
                        ));
                    }
                };
                sort_exprs.push(PhysicalSortExpr::new(
                    expr,
                    SortOptions {
                        descending: key.descending,
                        nulls_first: !key.descending,
                    },
                ));
            }
            let id_suffix = format!(".{}", self.catalog.system_columns.id);
            let mut tiebreaks: Vec<&str> = schema
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .filter(|name| name.ends_with(&id_suffix))
                .collect();
            tiebreaks.sort_unstable();
            for name in tiebreaks {
                sort_exprs.push(PhysicalSortExpr::new(
                    column(&schema, name).expect("tie-break columns come from the schema"),
                    SortOptions {
                        descending: false,
                        nulls_first: true,
                    },
                ));
            }
            let ordering = LexOrdering::new(sort_exprs).expect("a sort has at least one key");
            plan = Arc::new(
                SortExec::new(ordering, capped_sort_input(plan, self.memory_limit))
                    .with_fetch(fetch),
            );
        }
        Ok(limited(plan, limit))
    }
}

fn limited(plan: Plan, limit: Option<usize>) -> Plan {
    match limit {
        Some(rows) => Arc::new(GlobalLimitExec::new(plan, 0, Some(rows))),
        None => plan,
    }
}

fn column(schema: &Schema, name: &str) -> Option<Arc<dyn PhysicalExpr>> {
    schema
        .column_with_name(name)
        .map(|(index, _)| Arc::new(Column::new(name, index)) as Arc<dyn PhysicalExpr>)
}

/// The wide columns the return projection carries for the sort, each once,
/// and the `<binding>.<id>` columns name-sorted: the tie-break of
/// `apply_ordering`.
fn hidden_columns(
    keys: &[IROrdering],
    input_schema: &Schema,
    id_suffix: &str,
) -> Result<(Vec<String>, Vec<String>)> {
    let mut hidden: Vec<String> = Vec::new();
    for key in keys {
        match &key.expr {
            IRExpr::PropAccess { variable, property } => {
                let name = format!("{variable}.{property}");
                if input_schema.column_with_name(&name).is_none() {
                    return Err(OmniError::manifest(format!(
                        "column '{}' not found for ordering",
                        name
                    )));
                }
                if !hidden.contains(&name) {
                    hidden.push(name);
                }
            }
            IRExpr::AliasRef(_) => {}
            _ => {
                return Err(OmniError::manifest(
                    "unsupported ordering expression".to_string(),
                ));
            }
        }
    }
    let mut ids: Vec<String> = input_schema
        .fields()
        .iter()
        .map(|field| field.name().to_string())
        .filter(|name| name.ends_with(id_suffix))
        .collect();
    ids.sort();
    for name in &ids {
        if !hidden.contains(name) {
            hidden.push(name.clone());
        }
    }
    Ok((hidden, ids))
}

fn is_search_expr(expr: &IRExpr) -> bool {
    matches!(
        expr,
        IRExpr::Nearest { .. } | IRExpr::Bm25 { .. } | IRExpr::Rrf { .. }
    )
}
