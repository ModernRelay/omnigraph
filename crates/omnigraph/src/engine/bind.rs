//! Values into the plan: the one place the embedding client is used. `bind`
//! fills a `BoundPlan`'s value table from the gathered parameters and, per
//! `nearest` scan, the query vector its `RankedAccess` names, keyed by the
//! scan's node id. It reads the plan and the parameters and nothing else,
//! and refuses a binding that lacks a parameter the plan assumed.

use std::collections::BTreeMap;

use omnigraph_planner::{BoundPlan, PhysicalNode, PhysicalPlan, RankKind, ValueTable};

use super::*;

pub(super) async fn bind(
    plan: PhysicalPlan,
    source: &QuerySource<'_>,
    embedding: &EmbeddingResolver<'_>,
) -> Result<BoundPlan> {
    let params = Arc::clone(source.params.shared());
    let unbound: Vec<&str> = plan
        .assumptions()
        .params
        .iter()
        .filter(|name| !params.contains_key(*name))
        .map(String::as_str)
        .collect();
    if !unbound.is_empty() {
        return Err(OmniError::manifest_internal(format!(
            "the plan was built under parameters the binding lacks: {unbound:?}; a plan is \
             bound only to the parameter set it assumed"
        )));
    }
    let mut vectors = BTreeMap::new();
    for (id, node) in plan.live() {
        let PhysicalNode::Scan { spec, ranked, .. } = node else {
            continue;
        };
        let Some(ranked) = ranked
            .as_ref()
            .filter(|ranked| ranked.kind == RankKind::Nearest)
        else {
            continue;
        };
        let type_name = spec.table.node_type_name().ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "nearest() ranks `{}`, which is no node table",
                spec.table.type_key
            ))
        })?;
        let vector = resolve_nearest_query_vec(
            source.catalog,
            type_name,
            &ranked.property,
            &ranked.query,
            &params,
            embedding,
        )
        .await?;
        vectors.insert(id, vector);
    }
    Ok(BoundPlan {
        plan,
        values: ValueTable { params, vectors },
    })
}
