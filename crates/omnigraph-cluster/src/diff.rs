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
                metadata_change: None,
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
                metadata_change: None,
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
                metadata_change: None,
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
            metadata_change: Some(PlanMetadataChange::PolicyBindings),
            migration: None,
        });
    }
    changes.sort_by(|a, b| a.resource.cmp(&b.resource));
}

/// Metadata-only embedding provider changes: the provider digest is unchanged
/// but the applied state predates storing the profile body needed by
/// config-free serving. This mirrors policy binding backfill instead of
/// hiding a serving-time failure behind a no-op plan.
pub(crate) fn append_embedding_profile_changes(
    changes: &mut Vec<PlanChange>,
    prior_state: Option<&ClusterState>,
    desired: &DesiredCluster,
) {
    let Some(state) = prior_state else {
        return; // no state: provider Creates carry profiles already
    };
    for (address, desired_profile) in &desired.embedding_providers {
        if changes
            .iter()
            .any(|change| change.resource.as_str() == address.as_str())
        {
            continue; // content change already covers it
        }
        let Some(entry) = state.applied_revision.resources.get(address) else {
            continue; // not applied yet: the Create covers it
        };
        if entry.embedding_profile.as_ref() == Some(desired_profile) {
            continue;
        }
        changes.push(PlanChange {
            resource: address.clone(),
            operation: PlanOperation::Update,
            before_digest: Some(entry.digest.clone()),
            after_digest: Some(entry.digest.clone()),
            disposition: None,
            reason: None,
            binding_change: false,
            metadata_change: Some(PlanMetadataChange::EmbeddingProfile),
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
