//! Plan/apply classification: resource diffing, dispositions, approval
//! gating, demotion (moved verbatim from lib.rs in the modularization).

use super::*;

pub(crate) fn diff_resources(
    prior: &BTreeMap<String, String>,
    desired: &BTreeMap<String, String>,
) -> Vec<PlanChange> {
    let mut changes = Vec::new();
    for (address, after) in desired {
        match prior.get(address) {
            None => changes.push(PlanChange {
                resource: address.clone(),
                operation: PlanOperation::Create,
                before_digest: None,
                after_digest: Some(after.clone()),
                disposition: None,
                reason: None,
                binding_change: false,
                migration: None,
            }),
            Some(before) if before != after => changes.push(PlanChange {
                resource: address.clone(),
                operation: PlanOperation::Update,
                before_digest: Some(before.clone()),
                after_digest: Some(after.clone()),
                disposition: None,
                reason: None,
                binding_change: false,
                migration: None,
            }),
            Some(_) => {}
        }
    }
    for (address, before) in prior {
        if !desired.contains_key(address) {
            changes.push(PlanChange {
                resource: address.clone(),
                operation: PlanOperation::Delete,
                before_digest: Some(before.clone()),
                after_digest: None,
                disposition: None,
                reason: None,
                binding_change: false,
                migration: None,
            });
        }
    }
    changes.sort_by(|a, b| a.resource.cmp(&b.resource));
    changes
}

/// Binding-only policy changes: the file digest is unchanged (so
/// `diff_resources` saw nothing) but the applied `applies_to` differs from
/// the desired bindings — including the pre-5A case where the state entry
/// has no bindings recorded yet. These are first-class plan changes: without
/// this pass a binding edit would silently rot or silently converge.
pub(crate) fn append_policy_binding_changes(
    changes: &mut Vec<PlanChange>,
    prior_state: Option<&ClusterState>,
    desired: &DesiredCluster,
) {
    let Some(state) = prior_state else {
        return; // no state: everything is already a Create carrying bindings
    };
    for (address, desired_bindings) in &desired.policy_bindings {
        if changes.iter().any(|change| &change.resource == address) {
            continue; // content change already covers it
        }
        let Some(entry) = state.applied_revision.resources.get(address) else {
            continue; // not applied yet: the Create covers it
        };
        if entry.applies_to.as_ref() == Some(desired_bindings) {
            continue;
        }
        changes.push(PlanChange {
            resource: address.clone(),
            operation: PlanOperation::Update,
            before_digest: Some(entry.digest.clone()),
            after_digest: Some(entry.digest.clone()),
            disposition: None,
            reason: None,
            binding_change: true,
            migration: None,
        });
    }
    changes.sort_by(|a, b| a.resource.cmp(&b.resource));
}

pub(crate) fn compute_blast_radius(
    changes: &[PlanChange],
    dependencies: &[Dependency],
) -> Vec<BlastRadius> {
    changes
        .iter()
        .filter_map(|change| {
            let affected: Vec<_> = dependencies
                .iter()
                .filter_map(|dep| (dep.to == change.resource).then_some(dep.from.clone()))
                .collect();
            (!affected.is_empty()).then(|| BlastRadius {
                resource: change.resource.clone(),
                affected,
            })
        })
        .collect()
}

pub(crate) fn compute_approvals(
    changes: &[PlanChange],
    approved: &BTreeSet<String>,
) -> Vec<ApprovalRequirement> {
    // One gate per subtree: the graph.<id> delete carries its schema and
    // queries, so a schema delete whose graph is also deleted is not listed.
    let graph_deletes: BTreeSet<String> = changes
        .iter()
        .filter(|change| change.operation == PlanOperation::Delete)
        .filter_map(|change| change.resource.strip_prefix("graph.").map(str::to_string))
        .collect();
    changes
        .iter()
        .filter_map(|change| {
            if change.operation != PlanOperation::Delete {
                return None;
            }
            let gated = match resource_kind(&change.resource) {
                ResourceKind::Graph(_) => true,
                ResourceKind::Schema(graph) => !graph_deletes.contains(&graph),
                _ => false,
            };
            gated.then(|| ApprovalRequirement {
                resource: change.resource.clone(),
                reason: "delete may remove deployed graph or schema definition".to_string(),
                satisfied: approved.contains(&change.resource),
            })
        })
        .collect()
}

/// Resources with a valid (digest-matching, unconsumed) pending approval.
/// Near-misses — an artifact for the same resource whose bound digests no
/// longer match — warn as `approval_stale` and never authorize anything.
pub(crate) fn approved_resources(
    artifacts: &[(String, ApprovalArtifact)],
    changes: &[PlanChange],
    config_digest: &str,
    diagnostics: &mut Vec<Diagnostic>,
) -> BTreeSet<String> {
    let mut approved = BTreeSet::new();
    for change in changes {
        let candidates: Vec<&ApprovalArtifact> = artifacts
            .iter()
            .map(|(_, artifact)| artifact)
            .filter(|artifact| {
                artifact.consumed_at.is_none() && artifact.resource == change.resource
            })
            .collect();
        if candidates.is_empty() {
            continue;
        }
        let matched = candidates.iter().any(|artifact| {
            artifact.bound_config_digest == config_digest
                && artifact.bound_before_digest == change.before_digest
                && artifact.bound_after_digest == change.after_digest
        });
        if matched {
            approved.insert(change.resource.clone());
        } else {
            diagnostics.push(Diagnostic::warning(
                "approval_stale",
                change.resource.clone(),
                "an approval artifact exists but its bound digests no longer match the plan; re-run `cluster approve`",
            ));
        }
    }
    approved
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ResourceKind {
    Graph(String),
    Schema(String),
    Query { graph: String, name: String },
    Policy(String),
    EmbeddingProvider(String),
    Unknown,
}

pub(crate) fn resource_kind(address: &str) -> ResourceKind {
    if let Some(graph) = address.strip_prefix("graph.") {
        ResourceKind::Graph(graph.to_string())
    } else if let Some(graph) = address.strip_prefix("schema.") {
        ResourceKind::Schema(graph.to_string())
    } else if let Some(rest) = address.strip_prefix("query.") {
        match rest.split_once('.') {
            Some((graph, name)) => ResourceKind::Query {
                graph: graph.to_string(),
                name: name.to_string(),
            },
            None => ResourceKind::Unknown,
        }
    } else if let Some(name) = address.strip_prefix("policy.") {
        ResourceKind::Policy(name.to_string())
    } else if let Some(name) = address.strip_prefix("provider.embedding.") {
        ResourceKind::EmbeddingProvider(name.to_string())
    } else {
        ResourceKind::Unknown
    }
}

/// Classify every planned change with the disposition config-only apply gives
/// it. Stage 3A executes only query/policy catalog writes; graph/schema
/// movement is a later phase, and `graph.<id>` composite updates whose schema
/// component is unchanged converge automatically once query digests land.
pub(crate) fn classify_changes(
    changes: &mut [PlanChange],
    dependencies: &[Dependency],
    pending_recovery: &BTreeSet<String>,
    approved: &BTreeSet<String>,
) {
    let mut schema_creates = BTreeSet::new();
    let mut schema_pending = BTreeSet::new();
    let mut graph_creates = BTreeSet::new();
    let mut graph_deletes = BTreeSet::new();
    for change in changes.iter() {
        match resource_kind(&change.resource) {
            ResourceKind::Schema(graph) => match change.operation {
                PlanOperation::Create => {
                    schema_creates.insert(graph);
                }
                // Schema updates execute in-run before catalog writes (4B)
                // and no longer block dependents; deletes (4C) still do.
                PlanOperation::Update => {}
                PlanOperation::Delete => {
                    schema_pending.insert(graph);
                }
            },
            ResourceKind::Graph(graph) => match change.operation {
                PlanOperation::Create => {
                    graph_creates.insert(graph);
                }
                PlanOperation::Delete => {
                    graph_deletes.insert(graph);
                }
                PlanOperation::Update => {}
            },
            _ => {}
        }
    }
    // A schema Create is satisfied by its paired graph create (the init
    // carries the schema); a standalone schema Create stays pending.
    for graph in &schema_creates {
        if !graph_creates.contains(graph) {
            schema_pending.insert(graph.clone());
        }
    }
    // Subtree deletes ride the approved graph delete.
    let rides_approved_delete = |graph: &str| {
        graph_deletes.contains(graph)
            && approved.contains(&graph_address(graph))
            && !pending_recovery.contains(graph)
    };

    for change in changes.iter_mut() {
        let (disposition, reason) = match resource_kind(&change.resource) {
            ResourceKind::Schema(graph) => match change.operation {
                PlanOperation::Create
                    if graph_creates.contains(&graph) && !pending_recovery.contains(&graph) =>
                {
                    // Applied with the graph create — the init carries it.
                    (ApplyDisposition::Applied, None)
                }
                PlanOperation::Update if !pending_recovery.contains(&graph) => {
                    // Stage 4B: schema updates execute via the engine's
                    // schema apply (soft drops only; allow_data_loss is 4C).
                    (ApplyDisposition::Applied, None)
                }
                PlanOperation::Create | PlanOperation::Update => {
                    (ApplyDisposition::Blocked, Some("cluster_recovery_pending"))
                }
                PlanOperation::Delete if graph_deletes.contains(&graph) => {
                    if rides_approved_delete(&graph) {
                        (ApplyDisposition::Applied, None)
                    } else if pending_recovery.contains(&graph) {
                        (ApplyDisposition::Blocked, Some("cluster_recovery_pending"))
                    } else {
                        (ApplyDisposition::Blocked, Some("approval_required"))
                    }
                }
                _ => (ApplyDisposition::Deferred, Some("apply_unsupported_kind")),
            },
            ResourceKind::Graph(graph) => match change.operation {
                PlanOperation::Create => {
                    if pending_recovery.contains(&graph) {
                        (ApplyDisposition::Blocked, Some("cluster_recovery_pending"))
                    } else {
                        (ApplyDisposition::Applied, None)
                    }
                }
                PlanOperation::Update if !schema_pending.contains(&graph) => {
                    (ApplyDisposition::Derived, None)
                }
                // Stage 4C: an approved graph delete executes (the
                // irreversible tier — gated by a digest-bound artifact).
                PlanOperation::Delete => {
                    if pending_recovery.contains(&graph) {
                        (ApplyDisposition::Blocked, Some("cluster_recovery_pending"))
                    } else if rides_approved_delete(&graph) {
                        (ApplyDisposition::Applied, None)
                    } else {
                        (ApplyDisposition::Blocked, Some("approval_required"))
                    }
                }
                _ => (ApplyDisposition::Deferred, Some("apply_unsupported_kind")),
            },
            ResourceKind::Query { graph, .. } => match change.operation {
                PlanOperation::Delete => {
                    if rides_approved_delete(&graph) {
                        // Tombstoned with the approved graph delete.
                        (ApplyDisposition::Applied, None)
                    } else if graph_deletes.contains(&graph) {
                        (ApplyDisposition::Blocked, Some("approval_required"))
                    } else {
                        (ApplyDisposition::Applied, None)
                    }
                }
                PlanOperation::Create | PlanOperation::Update => {
                    if pending_recovery.contains(&graph) {
                        (ApplyDisposition::Blocked, Some("cluster_recovery_pending"))
                    } else if schema_pending.contains(&graph) {
                        (ApplyDisposition::Blocked, Some("dependency_not_applied"))
                    } else {
                        // A graph create in the same plan no longer blocks:
                        // creates execute first in the same apply run.
                        (ApplyDisposition::Applied, None)
                    }
                }
            },
            ResourceKind::Policy(_) => match change.operation {
                PlanOperation::Delete => (ApplyDisposition::Applied, None),
                PlanOperation::Create | PlanOperation::Update => {
                    let blocked_pending = dependencies.iter().any(|dep| {
                        dep.from == change.resource
                            && dep
                                .to
                                .strip_prefix("graph.")
                                .is_some_and(|graph| pending_recovery.contains(graph))
                    });
                    if blocked_pending {
                        (ApplyDisposition::Blocked, Some("cluster_recovery_pending"))
                    } else {
                        (ApplyDisposition::Applied, None)
                    }
                }
            },
            ResourceKind::EmbeddingProvider(_) => (ApplyDisposition::Applied, None),
            ResourceKind::Unknown => (ApplyDisposition::Deferred, Some("apply_unsupported_kind")),
        };
        change.disposition = Some(disposition);
        change.reason = reason.map(str::to_string);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FailedGraphOrigin {
    GraphCreate,
    SchemaApply,
    GraphDelete,
}

/// After a graph-moving operation fails mid-run, every change that depended
/// on that graph flips from Applied to Blocked so the output and the
/// persisted statuses tell the truth about what this run actually executed.
/// The originating change carries the failure code; dependents carry
/// `dependency_not_applied`.
pub(crate) fn demote_dependents_of_failed_graphs(
    changes: &mut [PlanChange],
    failed: &BTreeMap<String, FailedGraphOrigin>,
    dependencies: &[Dependency],
) {
    for change in changes.iter_mut() {
        if change.disposition != Some(ApplyDisposition::Applied) {
            continue;
        }
        let demote_reason = match resource_kind(&change.resource) {
            ResourceKind::Graph(graph) => match failed.get(&graph) {
                Some(FailedGraphOrigin::GraphCreate) => Some("graph_create_failed"),
                Some(FailedGraphOrigin::GraphDelete) => Some("graph_delete_failed"),
                Some(FailedGraphOrigin::SchemaApply) => Some("dependency_not_applied"),
                None => None,
            },
            ResourceKind::Schema(graph) => match failed.get(&graph) {
                Some(FailedGraphOrigin::SchemaApply) => Some("schema_apply_failed"),
                Some(FailedGraphOrigin::GraphCreate) | Some(FailedGraphOrigin::GraphDelete) => {
                    Some("dependency_not_applied")
                }
                None => None,
            },
            ResourceKind::Query { graph, .. } if failed.contains_key(&graph) => {
                Some("dependency_not_applied")
            }
            ResourceKind::Policy(_) => {
                let blocked = dependencies.iter().any(|dep| {
                    dep.from == change.resource
                        && dep
                            .to
                            .strip_prefix("graph.")
                            .is_some_and(|graph| failed.contains_key(graph))
                });
                blocked.then_some("dependency_not_applied")
            }
            _ => None,
        };
        if let Some(reason) = demote_reason {
            change.disposition = Some(ApplyDisposition::Blocked);
            change.reason = Some(reason.to_string());
        }
    }
}
