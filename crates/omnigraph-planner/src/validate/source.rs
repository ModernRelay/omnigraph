//! The source capability check: a catalogue table has rows in memory and no
//! storage, so no node of an accepted plan may ask a catalogue binding for a
//! manifest count, a row-address hydration, an index probe, a ranked scan, a
//! runtime filter, pushed predicate or dependent id lookup. Every catalogue
//! scan records the fingerprint that execution checks. Checked before lowering,
//! on fresh plans and replays alike.

use crate::error::PlanError;
use crate::logical::ScanAccess;
use crate::physical::{PhysicalNode, PhysicalPlan, ScanInput};

/// Every catalogue-bound node of `plan` reads memory only.
pub(crate) fn validate_catalogue_sources(plan: &PhysicalPlan) -> Result<(), PlanError> {
    let refuse = |id: usize, table: &str, what: &str| {
        Err(PlanError::Internal(format!(
            "node {id} asks the catalogue table `{table}` for {what}; catalogue rows are served from memory"
        )))
    };
    for (id, node) in plan.live() {
        match node {
            PhysicalNode::MetadataCount { spec, .. } if spec.table.is_catalogue() => {
                return refuse(id, &spec.table.type_key, "a manifest row count");
            }
            PhysicalNode::HydrateColumns { bindings, .. } => {
                if let Some(binding) = bindings.iter().find(|binding| binding.table.is_catalogue())
                {
                    return refuse(id, &binding.table.type_key, "a row-address hydration");
                }
            }
            PhysicalNode::Scan {
                source,
                spec,
                ranked,
                ..
            } if spec.table.is_catalogue() => {
                let table = &spec.table.type_key;
                if !plan.assumptions().catalogue.contains_key(table) {
                    return Err(PlanError::Internal(format!(
                        "catalogue scan {id} of `{table}` has no recorded catalogue fingerprint"
                    )));
                }
                if spec.filter.is_some() {
                    return refuse(id, table, "a pushed-down predicate");
                }
                if ranked.is_some() {
                    return refuse(id, table, "a ranked read");
                }
                if spec.runtime_filter.is_some() {
                    return refuse(id, table, "a runtime filter");
                }
                if matches!(source, ScanInput::Dependent { .. }) {
                    return refuse(id, table, "a dependent id lookup");
                }
                if spec.fragments.is_some() {
                    return refuse(id, table, "a fragment scope");
                }
                if let Some(access) = &spec.access
                    && !matches!(access, ScanAccess::Sequential)
                {
                    return refuse(id, table, "an index or runtime access path");
                }
            }
            PhysicalNode::ContainsJoin { right, .. } => {
                if let Some(PhysicalNode::Scan { spec, .. }) = plan.node(*right)
                    && spec.table.is_catalogue()
                {
                    return refuse(id, &spec.table.type_key, "a contains-join runtime filter");
                }
            }
            _ => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical::{RuntimeFilterKind, RuntimeFilterSpec, ScanSpec};
    use crate::operation::TableRef;
    use crate::source::SideId;
    use omnigraph_compiler::SYSTEM_COLUMNS_V3;

    fn catalogue_spec() -> Box<ScanSpec> {
        Box::new(ScanSpec {
            access: Some(ScanAccess::Sequential),
            side: SideId::Binding(0),
            table: TableRef::catalogue("node:@Operator"),
            version: None,
            columns: SYSTEM_COLUMNS_V3,
            fragments: None,
            projection: None,
            filter: None,
            binding: Some("o".to_string()),
            runtime_filter: None,
        })
    }

    fn plan_of(node: PhysicalNode) -> PhysicalPlan {
        let mut plan = PhysicalPlan::new();
        plan.set_assumptions(crate::physical::Assumptions {
            catalogue: [("node:@Operator".to_string(), "test".to_string())]
                .into_iter()
                .collect(),
            ..Default::default()
        });
        let id = plan.add(node);
        plan.set_root(id);
        plan
    }

    #[test]
    fn a_sequential_catalogue_scan_is_accepted() {
        let plan = plan_of(PhysicalNode::Scan {
            source: ScanInput::Table,
            spec: catalogue_spec(),
            ordered: false,
            keys_only: false,
            ranked: None,
        });
        validate_catalogue_sources(&plan).unwrap();
    }

    #[test]
    fn storage_requests_on_a_catalogue_table_are_refused() {
        let mut filtered = catalogue_spec();
        filtered.runtime_filter = Some(RuntimeFilterSpec {
            column: "name".to_string(),
            needle: ("j".to_string(), "uses".to_string()),
            kind: RuntimeFilterKind::TextContainsAny,
        });
        let runtime = plan_of(PhysicalNode::Scan {
            source: ScanInput::Table,
            spec: filtered,
            ordered: false,
            keys_only: false,
            ranked: None,
        });
        let count = plan_of(PhysicalNode::MetadataCount {
            spec: catalogue_spec(),
            return_exprs: Vec::new(),
        });
        for (plan, what) in [(runtime, "runtime filter"), (count, "manifest row count")] {
            let error = validate_catalogue_sources(&plan).unwrap_err().to_string();
            assert!(error.contains(what), "{error}");
        }
    }
}
