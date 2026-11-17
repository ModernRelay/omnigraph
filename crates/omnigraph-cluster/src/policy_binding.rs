//! Which applied policy bundle binds each scope: the one rule every reader of
//! applied policy uses (the authorization loaders, the direct writable
//! opener, the serving projection and, through the snapshot, the server,
//! `omnigraph access` and the `policy` commands).
//!
//! It reads only the ledger's binding metadata, never a payload. Every policy
//! entry must record bindings, every binding is `cluster` or `graph.<id>`, a
//! bundle binds the cluster or graphs but not both, and one bundle at most
//! binds each scope. A ledger that breaks any of these
//! is refused as a whole, as no server boots from it: a reader that skipped
//! the entries it does not use would answer for a cluster that serves nothing.

use std::collections::BTreeMap;

use crate::diff::{ResourceKind, resource_kind};
use crate::types::{Diagnostic, StateResource};

/// The binding that names the cluster scope.
pub(crate) const CLUSTER_SCOPE: &str = "cluster";

/// The prefix of a graph scope's binding, `graph.<id>`.
const GRAPH_SCOPE_PREFIX: &str = "graph.";

/// One scope a bundle binds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Scope<'a> {
    Cluster,
    Graph(&'a str),
}

impl<'a> Scope<'a> {
    /// The scope a recorded binding names, or `None` for any other text.
    pub(crate) fn parse(binding: &'a str) -> Option<Self> {
        if binding == CLUSTER_SCOPE {
            return Some(Self::Cluster);
        }
        binding
            .strip_prefix(GRAPH_SCOPE_PREFIX)
            .filter(|graph| !graph.is_empty())
            .map(Self::Graph)
    }
}

/// The policy resource address bound to each scope of an applied revision.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct PolicyBindings {
    cluster: Option<String>,
    graphs: BTreeMap<String, String>,
    bundles: BTreeMap<String, Vec<String>>,
}

impl PolicyBindings {
    /// Resolve the bindings of `resources`, an applied revision's entries.
    ///
    /// # Errors
    /// `policy_bindings_missing` for a policy entry with no recorded bindings,
    /// and `applied_policy_invalid` for a binding that names no scope, a
    /// second bundle binding one scope, or a bundle binding both `cluster`
    /// and a graph (`cluster validate` refuses the last two in a config too).
    pub(crate) fn of(resources: &BTreeMap<String, StateResource>) -> Result<Self, Diagnostic> {
        let mut bindings = Self::default();
        for (address, entry) in resources {
            if !matches!(resource_kind(address), ResourceKind::Policy(_)) {
                continue;
            }
            let recorded = entry
                .applies_to
                .as_ref()
                .filter(|recorded| !recorded.is_empty())
                .ok_or_else(|| {
                    Diagnostic::error(
                        "policy_bindings_missing",
                        address.clone(),
                        "no applied applies_to bindings recorded (ledger predates binding metadata); re-run `cluster apply` to backfill",
                    )
                })?;
            let (mut cluster, mut graph) = (false, false);
            for binding in recorded {
                let slot = match Scope::parse(binding) {
                    Some(Scope::Cluster) => {
                        cluster = true;
                        bindings.cluster.replace(address.clone())
                    }
                    Some(Scope::Graph(id)) => {
                        graph = true;
                        bindings.graphs.insert(id.to_string(), address.clone())
                    }
                    None => {
                        return Err(Diagnostic::error(
                            "applied_policy_invalid",
                            address.clone(),
                            format!("unrecognized applied policy binding `{binding}`"),
                        ));
                    }
                };
                if let Some(first) = slot.filter(|first| first != address) {
                    return Err(Diagnostic::error(
                        "applied_policy_invalid",
                        address.clone(),
                        format!(
                            "`{first}` and `{address}` both bind `{binding}`; the cluster binds one policy bundle per scope"
                        ),
                    ));
                }
            }
            if cluster && graph {
                return Err(Diagnostic::error(
                    "applied_policy_invalid",
                    address.clone(),
                    "one policy bundle binds both `cluster` and graph scopes; the cluster binds server and graph rules in separate bundles",
                ));
            }
            bindings.bundles.insert(address.clone(), recorded.clone());
        }
        Ok(bindings)
    }

    /// The address of the bundle bound to `graph`.
    pub(crate) fn graph(&self, graph: &str) -> Option<&str> {
        self.graphs.get(graph).map(String::as_str)
    }

    /// Every bound bundle's address with the scopes it binds, in address
    /// order; each binding is one [`Scope::parse`] accepts.
    pub(crate) fn bundles(&self) -> impl Iterator<Item = (&str, impl Iterator<Item = Scope<'_>>)> {
        self.bundles.iter().map(|(address, recorded)| {
            (
                address.as_str(),
                recorded.iter().filter_map(|binding| Scope::parse(binding)),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(applies_to: Option<&[&str]>) -> StateResource {
        StateResource {
            digest: "f".repeat(64),
            applies_to: applies_to.map(|all| all.iter().map(|s| s.to_string()).collect()),
            embedding_provider: None,
            embedding_profile: None,
            external_blob_policy: None,
        }
    }

    /// Policy entries by address, with their recorded bindings.
    type Entries<'a> = &'a [(&'a str, Option<&'a [&'a str]>)];

    fn resolve(entries: Entries<'_>) -> Result<PolicyBindings, Diagnostic> {
        PolicyBindings::of(
            &entries
                .iter()
                .map(|(address, applies_to)| (address.to_string(), entry(*applies_to)))
                .collect(),
        )
    }

    #[test]
    fn each_scope_resolves_to_its_one_bundle() {
        let bindings = resolve(&[
            ("graph.knowledge", None),
            ("policy.ops", Some(&["cluster"])),
            ("policy.crm", Some(&["graph.crm", "graph.knowledge"])),
        ])
        .unwrap();
        assert_eq!(bindings.cluster.as_deref(), Some("policy.ops"));
        assert_eq!(bindings.graph("knowledge"), Some("policy.crm"));
        assert_eq!(bindings.graph("crm"), Some("policy.crm"));
        assert_eq!(bindings.graph("other"), None);
        let bundles: Vec<(&str, Vec<Scope<'_>>)> = bindings
            .bundles()
            .map(|(address, scopes)| (address, scopes.collect()))
            .collect();
        assert_eq!(
            bundles,
            vec![
                (
                    "policy.crm",
                    vec![Scope::Graph("crm"), Scope::Graph("knowledge")]
                ),
                ("policy.ops", vec![Scope::Cluster]),
            ]
        );
        assert_eq!(resolve(&[]).unwrap(), PolicyBindings::default());
    }

    /// Each refusal holds wherever the offending entry sits: a reader of one
    /// graph is refused by another graph's entry, as a server is.
    #[test]
    fn a_ledger_that_breaks_a_binding_rule_is_refused_as_a_whole() {
        let cases: [(Entries<'_>, &str, &str); 7] = [
            (
                &[("policy.a", Some(&["graph.a"])), ("policy.b", None)],
                "policy_bindings_missing",
                "policy.b",
            ),
            (
                &[("policy.a", Some(&["graph.a"])), ("policy.b", Some(&[]))],
                "policy_bindings_missing",
                "policy.b",
            ),
            (
                &[("policy.a", Some(&["graph.a", "server"]))],
                "applied_policy_invalid",
                "unrecognized applied policy binding `server`",
            ),
            (
                &[("policy.a", Some(&["graph."]))],
                "applied_policy_invalid",
                "unrecognized applied policy binding `graph.`",
            ),
            (
                &[
                    ("policy.a", Some(&["graph.b"])),
                    ("policy.c", Some(&["graph.b"])),
                ],
                "applied_policy_invalid",
                "`policy.a` and `policy.c` both bind `graph.b`",
            ),
            (
                &[
                    ("policy.a", Some(&["cluster"])),
                    ("policy.c", Some(&["cluster"])),
                ],
                "applied_policy_invalid",
                "`policy.a` and `policy.c` both bind `cluster`",
            ),
            (
                &[
                    ("policy.a", Some(&["graph.a"])),
                    ("policy.m", Some(&["cluster", "graph.b"])),
                ],
                "applied_policy_invalid",
                "binds both `cluster` and graph scopes",
            ),
        ];
        // A bundle that repeats its own binding still binds one scope.
        let repeated = resolve(&[("policy.a", Some(&["graph.a", "graph.a"]))]).unwrap();
        assert_eq!(repeated.graph("a"), Some("policy.a"));
        for (entries, code, said) in cases {
            let refused = resolve(entries).unwrap_err();
            assert_eq!(refused.code, code, "{entries:?}");
            assert!(
                refused.path.contains(said) || refused.message.contains(said),
                "{entries:?}: {refused:?}"
            );
        }
    }
}
