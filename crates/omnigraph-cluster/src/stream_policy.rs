//! Cedar policy binding for checked stream-profile apply.
//!
//! A profile transition happens before the cluster state CAS publishes the
//! desired policy revision. Therefore neither the old nor the new policy may
//! be skipped: an existing applied graph policy remains authoritative until
//! that CAS, while a newly desired policy must already authorize the actor
//! before its accompanying profile effect can land. During a policy update we
//! enforce their conjunction. This makes both sides of the state-CAS crash
//! window fail closed.

use std::collections::BTreeSet;
use std::fs;
use std::sync::Arc;

use omnigraph_policy::{
    PolicyAction, PolicyChecker, PolicyEngine, PolicyError, ResourceScope,
};

use crate::diff::{ResourceKind, resource_kind};
use crate::store::ClusterStore;
use crate::types::{ClusterState, DesiredCluster};

struct ConjunctiveGraphPolicies {
    policies: Vec<PolicyEngine>,
}

impl PolicyChecker for ConjunctiveGraphPolicies {
    fn check(
        &self,
        action: PolicyAction,
        scope: &ResourceScope,
        actor: &str,
    ) -> Result<(), PolicyError> {
        for policy in &self.policies {
            policy.check(action, scope, actor)?;
        }
        Ok(())
    }
}

fn binds_graph(bindings: &[String], graph_id: &str) -> bool {
    let graph_binding = format!("graph.{graph_id}");
    bindings.iter().any(|binding| binding == &graph_binding)
}

/// Load the exact policy authority spanning the current and desired cluster
/// revisions for one stream-profile effect.
///
/// Each revision supports at most one graph-scoped bundle, matching the
/// serving contract. An unchanged bundle is compiled once; an update is an
/// intersection, so both old and new must allow `stream_manage`.
pub(crate) async fn stream_profile_policy_checker(
    graph_id: &str,
    state: &ClusterState,
    desired: &DesiredCluster,
    backend: &ClusterStore,
) -> Result<Option<Arc<dyn PolicyChecker>>, String> {
    let mut applied = Vec::new();
    for (address, resource) in &state.applied_revision.resources {
        let kind = resource_kind(address);
        if !matches!(kind, ResourceKind::Policy(_)) {
            continue;
        }
        let Some(bindings) = resource.applies_to.as_deref() else {
            return Err(format!(
                "applied policy '{address}' has no recorded bindings; run cluster refresh/apply before changing the stream profile"
            ));
        };
        if binds_graph(bindings, graph_id) {
            applied.push((address.as_str(), resource.digest.as_str(), kind));
        }
    }
    if applied.len() > 1 {
        return Err(format!(
            "multiple applied policy bundles bind graph '{graph_id}'; profile apply supports the same one-bundle-per-scope contract as serving"
        ));
    }

    let mut desired_bound = desired
        .policy_bindings
        .iter()
        .filter(|(_, bindings)| binds_graph(bindings, graph_id))
        .map(|(address, _)| address.as_str())
        .collect::<Vec<_>>();
    desired_bound.sort_unstable();
    if desired_bound.len() > 1 {
        return Err(format!(
            "multiple desired policy bundles bind graph '{graph_id}'; profile apply supports the same one-bundle-per-scope contract as serving"
        ));
    }

    let mut seen = BTreeSet::new();
    let mut policies = Vec::new();
    for (address, digest, kind) in applied {
        let source = backend
            .read_verified_payload(&kind, digest, address)
            .await
            .map_err(|diagnostic| {
                format!(
                    "could not load applied graph policy '{address}' for profile authorization: [{}] {}",
                    diagnostic.code, diagnostic.message
                )
            })?;
        seen.insert((address.to_string(), digest.to_string()));
        policies.push(
            PolicyEngine::load_graph_from_source(&source, graph_id).map_err(|error| {
                format!(
                    "could not compile applied graph policy '{address}' for profile authorization: {error}"
                )
            })?,
        );
    }

    if let Some(address) = desired_bound.into_iter().next() {
        let digest = desired.resource_digests.get(address).ok_or_else(|| {
            format!("desired graph policy '{address}' has no resource digest")
        })?;
        if !seen.contains(&(address.to_string(), digest.clone())) {
            let path = desired
                .resources
                .iter()
                .find(|resource| resource.address == address)
                .and_then(|resource| resource.path.as_deref())
                .ok_or_else(|| {
                    format!("desired graph policy '{address}' has no source path")
                })?;
            let source = fs::read_to_string(path).map_err(|error| {
                format!(
                    "could not read desired graph policy '{address}' from '{path}': {error}"
                )
            })?;
            let observed = super::sha256_hex(source.as_bytes());
            if &observed != digest {
                return Err(format!(
                    "desired graph policy '{address}' changed while apply was running; expected digest {digest}, observed {observed}"
                ));
            }
            policies.push(
                PolicyEngine::load_graph_from_source(&source, graph_id).map_err(|error| {
                    format!(
                        "could not compile desired graph policy '{address}' for profile authorization: {error}"
                    )
                })?,
            );
        }
    }

    if policies.is_empty() {
        Ok(None)
    } else {
        Ok(Some(Arc::new(ConjunctiveGraphPolicies { policies })))
    }
}
