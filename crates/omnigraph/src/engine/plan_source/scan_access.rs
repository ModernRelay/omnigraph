use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::{Expr, Volatility};
use datafusion::physical_plan::ExecutionPlan;
use lance::Dataset;
use lance::index::DatasetIndexInternalExt;
use lance::io::exec::LanceFilterExec;
use lance::io::exec::filtered_read::FilteredReadExec;
use lance::io::exec::scalar_index::{MaterializeIndexExec, ScalarIndexExec};
use lance_core::datatypes::Projection;
use lance_datafusion::planner::Planner;
use lance_index::scalar::expression::{PlannerIndexExt, ScalarIndexExpr};
use omnigraph_planner::{IndexQuery, Predicate, RuntimeInput, ScanAccess, ScanSpec};

use super::QuerySource;
use crate::engine::scan::NodeRead;
use crate::engine::search::NeededColumns;
use crate::error::{OmniError, Result};

pub(super) async fn index_split(source: &QuerySource<'_>, spec: &ScanSpec) -> Result<ScanAccess> {
    let type_name =
        spec.table.type_key.strip_prefix("node:").ok_or_else(|| {
            OmniError::manifest_internal("query scan access requires a node table")
        })?;
    let dataset = source
        .snapshot
        .open_lance_dataset(&spec.table.type_key)
        .await?;
    let filters = spec
        .filter
        .as_ref()
        .map(Predicate::gq_filters)
        .unwrap_or_default();
    let columns = spec
        .projection
        .as_ref()
        .map(|columns| NeededColumns(columns.iter().cloned().collect()));
    let read = NodeRead::for_index_split(
        dataset.clone(),
        type_name,
        &filters,
        source.params.shared(),
        source.catalog,
        columns.as_ref(),
    )?;
    let (plan, filter) = read.plan_with_filter().await?;
    inspect(&plan, &dataset, filter).await
}

fn mirror(expr: &ScalarIndexExpr) -> IndexQuery {
    match expr {
        ScalarIndexExpr::Query(search) => IndexQuery::Search {
            index: search.index_name.clone(),
            column: search.column.clone(),
            search: search.query.format(&search.column),
        },
        ScalarIndexExpr::And(left, right) => IndexQuery::And {
            left: Box::new(mirror(left)),
            right: Box::new(mirror(right)),
        },
        ScalarIndexExpr::Or(left, right) => IndexQuery::Or {
            left: Box::new(mirror(left)),
            right: Box::new(mirror(right)),
        },
        ScalarIndexExpr::Not(input) => IndexQuery::Not {
            input: Box::new(mirror(input)),
        },
    }
}

fn immutable(filter: &Expr) -> Result<bool> {
    let mut immutable = true;
    filter
        .apply(|expr| {
            if let Expr::ScalarFunction(function) = expr {
                immutable &= function.func.signature().volatility == Volatility::Immutable;
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .map_err(OmniError::datafusion)?;
    Ok(immutable)
}

/// Reconstruct only a legacy node selected by the real scanner.
/// Immutable, unranked filters share its public planner and filter schema.
async fn legacy_query(dataset: &Dataset, filter: Expr) -> Result<ScalarIndexExpr> {
    let dataset = Arc::new(dataset.clone());
    let schema = Projection::full(dataset.clone())
        .with_row_id()
        .with_row_addr()
        .with_row_last_updated_at_version()
        .with_row_created_at_version()
        .to_schema()
        .merge(&Schema::new(vec![Field::new(
            lance_core::ROW_OFFSET,
            DataType::UInt64,
            true,
        )]))
        .map_err(OmniError::storage)?;
    let info = dataset
        .scalar_index_info()
        .await
        .map_err(OmniError::storage)?;
    Planner::new(Arc::new((&schema).into()))
        .create_filter_plan(filter, &info, true)
        .map_err(OmniError::storage)?
        .index_query
        .ok_or_else(|| OmniError::manifest_internal("legacy index node has no scalar index query"))
}

pub(super) async fn inspect(
    plan: &Arc<dyn ExecutionPlan>,
    dataset: &Dataset,
    filter: Option<Expr>,
) -> Result<ScanAccess> {
    if let Some(filter) = &filter {
        if !immutable(filter)? {
            return Ok(ScanAccess::Runtime {
                input: RuntimeInput::DynamicExpression,
            });
        }
    }
    let mut pending = vec![(Arc::clone(plan), Vec::<Expr>::new(), None::<Expr>)];
    let mut probe = None;
    while let Some((node, mut residuals, covered_residual)) = pending.pop() {
        if let Some(filter) = node.downcast_ref::<LanceFilterExec>() {
            residuals.push(filter.expr().clone());
        }
        let query = if let Some(index) = node.downcast_ref::<ScalarIndexExec>() {
            Some(index.expr().clone())
        } else if node.is::<MaterializeIndexExec>() {
            let filter = filter.clone().ok_or_else(|| {
                OmniError::manifest_internal("legacy index node has no configured filter")
            })?;
            Some(legacy_query(dataset, filter).await?)
        } else {
            None
        };
        if let Some(query) = query {
            if probe.is_some() {
                return Err(OmniError::manifest_internal(
                    "static scan contains multiple index probes",
                ));
            }
            residuals.extend(covered_residual);
            let residuals: BTreeSet<_> = residuals.iter().map(|expr| format!("({expr})")).collect();
            let residual = (!residuals.is_empty())
                .then(|| residuals.into_iter().collect::<Vec<_>>().join(" AND "));
            probe = Some(ScanAccess::IndexProbe {
                query: mirror(&query),
                residual,
            });
            continue;
        }
        if let Some(read) = node.downcast_ref::<FilteredReadExec>() {
            if let Some(input) = read.index_input() {
                pending.push((
                    Arc::clone(input),
                    residuals,
                    read.options().refine_filter.clone(),
                ));
                continue;
            }
        }
        for child in node.children() {
            pending.push((
                Arc::clone(child),
                residuals.clone(),
                covered_residual.clone(),
            ));
        }
    }
    Ok(probe.unwrap_or(ScanAccess::Sequential))
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod planning_cost;
