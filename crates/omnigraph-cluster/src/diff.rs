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
                delete_root: None,
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
                delete_root: None,
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
                delete_root: None,
            });
        }
    }
    changes.sort_by(|a, b| a.resource.cmp(&b.resource));
    changes
}

/// The same normalized resource projection drives captured and file plans.
pub(crate) fn diff_state_resources(
    prior: &BTreeMap<String, StateResource>,
    desired: &BTreeMap<String, StateResource>,
) -> Vec<PlanChange> {
    let digests = |resources: &BTreeMap<String, StateResource>| {
        resources
            .iter()
            .map(|(address, resource)| (address.clone(), resource.digest.clone()))
            .collect()
    };
    let mut changes = diff_resources(&digests(prior), &digests(desired));
    for (address, after) in desired {
        let Some(before) = prior.get(address) else {
            continue;
        };
        if changes.iter().any(|change| &change.resource == address) {
            continue;
        }
        let metadata_change = match resource_kind(address) {
            ResourceKind::Policy(_) if before.applies_to != after.applies_to => {
                Some(PlanMetadataChange::PolicyBindings)
            }
            ResourceKind::EmbeddingProvider(_)
                if before.embedding_profile != after.embedding_profile =>
            {
                Some(PlanMetadataChange::EmbeddingProfile)
            }
            _ => None,
        };
        if let Some(metadata_change) = metadata_change {
            changes.push(PlanChange {
                resource: address.clone(),
                operation: PlanOperation::Update,
                before_digest: Some(before.digest.clone()),
                after_digest: Some(after.digest.clone()),
                disposition: None,
                reason: None,
                binding_change: metadata_change == PlanMetadataChange::PolicyBindings,
                metadata_change: Some(metadata_change),
                migration: None,
                delete_root: None,
            });
        }
    }
    changes.sort_by(|a, b| a.resource.cmp(&b.resource));
    changes
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
