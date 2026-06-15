//! RFC-011 Slice A scope resolution.
//!
//! Translates the new scope inputs (`--profile` / `--store` / operator-config
//! `profiles`/`clusters`/`defaults`) into the SAME effective addressing tuple
//! the existing `GraphClient` factories (`client.rs`) and the maintenance
//! resolver (`helpers::resolve_storage_uri`) already consume. This is a
//! translation layer that sits *in front* of those resolvers — it is purely
//! additive: an explicit legacy address (`--uri`/`--target`/`--server`/
//! `--store`) wins and reproduces today's behavior exactly, so existing
//! invocations are unaffected.
//!
//! The access path (served vs direct) is never chosen here; it falls out of the
//! scope's binding × the verb's capability. The capability→scope check rejects
//! mismatches (e.g. a server scope on a maintenance verb) only on the *new*
//! resolution paths.

use std::env;

use color_eyre::Result;
use color_eyre::eyre::{bail, eyre};

use crate::operator::{OperatorConfig, ScopeBinding};
use crate::planes::Capability;

pub(crate) const PROFILE_ENV: &str = "OMNIGRAPH_PROFILE";

/// The effective addressing a command should use, in the terms the existing
/// resolvers consume. Data/served verbs read `server`/`graph`/`uri`/`target`;
/// maintenance verbs read `cluster`/`cluster_graph`.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct ResolvedScope {
    pub(crate) server: Option<String>,
    pub(crate) graph: Option<String>,
    pub(crate) uri: Option<String>,
    pub(crate) cluster: Option<String>,
    pub(crate) cluster_graph: Option<String>,
}

/// The raw addressing inputs for one command: the global scope flags plus the
/// command's own positional URI.
pub(crate) struct ScopeFlags<'a> {
    pub(crate) profile: Option<&'a str>,
    pub(crate) store: Option<&'a str>,
    pub(crate) server: Option<&'a str>,
    pub(crate) graph: Option<&'a str>,
    pub(crate) uri: Option<String>,
}

/// Resolve the scope for a command with `capability`. Precedence (RFC-011):
/// 1. explicit primitive address (`uri`/`--server`/`--store`) → passthrough;
/// 2. `--profile` / `OMNIGRAPH_PROFILE`;
/// 3. flat `defaults.server` + `defaults.default_graph`;
/// 4. nothing — downstream behaves as today.
pub(crate) fn resolve_scope(
    op: &OperatorConfig,
    capability: Capability,
    flags: ScopeFlags<'_>,
) -> Result<ResolvedScope> {
    // `--store` is its own way to address a graph; combining it with a positional
    // URI or `--server` is a contradiction, not a silent precedence.
    if flags.store.is_some() && (flags.uri.is_some() || flags.server.is_some()) {
        bail!(
            "--store is exclusive with a positional URI and --server — pick one way to \
             address the graph"
        );
    }
    // 1. Any explicit address wins; reproduce today's behavior untouched.
    //    `--store` is an explicit store URI — fold it into `uri`.
    if flags.uri.is_some() || flags.server.is_some() || flags.store.is_some() {
        return Ok(ResolvedScope {
            server: flags.server.map(str::to_string),
            graph: flags.graph.map(str::to_string),
            uri: flags.store.map(str::to_string).or(flags.uri),
            ..Default::default()
        });
    }

    // 2. A named profile (flag, else env).
    let profile_name = flags
        .profile
        .map(str::to_string)
        .or_else(|| env::var(PROFILE_ENV).ok().filter(|s| !s.is_empty()));
    if let Some(name) = profile_name {
        let profile = op.profile(&name).ok_or_else(|| {
            eyre!("unknown profile '{name}' (not defined under `profiles:` in operator config)")
        })?;
        let binding = profile.binding(&name)?;
        let graph = flags
            .graph
            .map(str::to_string)
            .or_else(|| profile.default_graph.clone());
        return scope_from_binding(op, capability, binding, graph, &format!("profile '{name}'"));
    }

    // 3. Flat default server scope.
    if let Some(server) = op.default_server() {
        let graph = flags
            .graph
            .map(str::to_string)
            .or_else(|| op.default_graph().map(str::to_string));
        return scope_from_binding(
            op,
            capability,
            ScopeBinding::Server(server.to_string()),
            graph,
            "operator defaults",
        );
    }

    // 4. Nothing resolved — leave the tuple empty; downstream falls through to
    //    today's behavior (legacy `cli.graph` default or a no-address error).
    Ok(ResolvedScope::default())
}

/// Map a resolved binding to the effective tuple, enforcing scope × capability
/// capability (RFC-011): a server scope is served (data only); a cluster scope
/// is privileged direct (maintenance/control only); a store scope is direct
/// (either).
fn scope_from_binding(
    op: &OperatorConfig,
    capability: Capability,
    binding: ScopeBinding,
    graph: Option<String>,
    source: &str,
) -> Result<ResolvedScope> {
    match binding {
        ScopeBinding::Server(server) => {
            if capability == Capability::Direct {
                bail!(
                    "this command needs direct storage access, but {source} resolves a \
                     server scope; name storage explicitly with --store <uri> (or a \
                     --cluster/--cluster-graph for a managed graph)"
                );
            }
            Ok(ResolvedScope {
                server: Some(server),
                graph,
                ..Default::default()
            })
        }
        ScopeBinding::Cluster(cluster) => {
            if capability == Capability::Any {
                bail!(
                    "{source} resolves a cluster scope, which is maintenance-only; run \
                     data commands through a server, or use --store <uri> for ad-hoc \
                     direct access"
                );
            }
            // A cluster binding is a config name (resolved against `clusters:`)
            // or a literal root URI.
            let root = if let Some(root) = op.cluster_root(&cluster) {
                root.to_string()
            } else if cluster.contains("://") {
                cluster
            } else {
                bail!(
                    "unknown cluster '{cluster}' ({source}); define it under `clusters:` \
                     in operator config, or use a literal root URI"
                );
            };
            Ok(ResolvedScope {
                cluster: Some(root),
                cluster_graph: graph,
                ..Default::default()
            })
        }
        ScopeBinding::Store(uri) => {
            if graph.is_some() {
                bail!(
                    "--graph does not apply to a store scope ({source}): a store is already \
                     a single graph"
                );
            }
            Ok(ResolvedScope {
                uri: Some(uri),
                ..Default::default()
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(yaml: &str) -> OperatorConfig {
        serde_yaml::from_str(yaml).unwrap()
    }

    fn flags<'a>() -> ScopeFlags<'a> {
        ScopeFlags {
            profile: None,
            store: None,
            server: None,
            graph: None,
            uri: None,
        }
    }

    #[test]
    fn explicit_legacy_address_wins_unchanged() {
        let op = cfg("defaults:\n  server: prod\nservers:\n  prod:\n    url: https://x\n");
        // A positional URI given → profile/defaults are ignored entirely.
        let scope = resolve_scope(
            &op,
            Capability::Any,
            ScopeFlags {
                uri: Some("graph.omni".into()),
                ..flags()
            },
        )
        .unwrap();
        assert_eq!(scope.uri.as_deref(), Some("graph.omni"));
        assert_eq!(scope.server, None);
    }

    #[test]
    fn store_flag_folds_into_uri_and_rejects_graph() {
        let op = OperatorConfig::default();
        let scope = resolve_scope(
            &op,
            Capability::Any,
            ScopeFlags {
                store: Some("s3://b/g.omni"),
                ..flags()
            },
        )
        .unwrap();
        assert_eq!(scope.uri.as_deref(), Some("s3://b/g.omni"));
    }

    #[test]
    fn store_is_exclusive_with_positional_uri_and_server() {
        let op = OperatorConfig::default();
        for flags in [
            ScopeFlags {
                store: Some("s3://b/g.omni"),
                uri: Some("file://other.omni".into()),
                ..flags()
            },
            ScopeFlags {
                store: Some("s3://b/g.omni"),
                server: Some("prod"),
                ..flags()
            },
        ] {
            let err = resolve_scope(&op, Capability::Any, flags).unwrap_err().to_string();
            assert!(err.contains("--store is exclusive"), "{err}");
        }
    }

    #[test]
    fn flat_default_server_drives_data_verbs() {
        let op = cfg("defaults:\n  server: prod\n  default_graph: knowledge\nservers:\n  prod:\n    url: https://x\n");
        let scope = resolve_scope(&op, Capability::Any, flags()).unwrap();
        assert_eq!(scope.server.as_deref(), Some("prod"));
        assert_eq!(scope.graph.as_deref(), Some("knowledge"));
    }

    #[test]
    fn profile_server_scope_with_graph_override() {
        let op = cfg(
            "servers:\n  staging:\n    url: https://s\nprofiles:\n  staging:\n    server: staging\n    default_graph: knowledge\n",
        );
        let scope = resolve_scope(
            &op,
            Capability::Any,
            ScopeFlags {
                profile: Some("staging"),
                graph: Some("archive"),
                ..flags()
            },
        )
        .unwrap();
        assert_eq!(scope.server.as_deref(), Some("staging"));
        assert_eq!(scope.graph.as_deref(), Some("archive")); // flag beats profile default
    }

    #[test]
    fn profile_cluster_scope_resolves_root_for_maintenance() {
        let op = cfg(
            "clusters:\n  brain:\n    root: s3://acme/brain\nprofiles:\n  admin:\n    cluster: brain\n    default_graph: knowledge\n",
        );
        let scope = resolve_scope(
            &op,
            Capability::Direct,
            ScopeFlags {
                profile: Some("admin"),
                ..flags()
            },
        )
        .unwrap();
        assert_eq!(scope.cluster.as_deref(), Some("s3://acme/brain"));
        assert_eq!(scope.cluster_graph.as_deref(), Some("knowledge"));
    }

    #[test]
    fn server_scope_on_maintenance_verb_errors() {
        let op = cfg("defaults:\n  server: prod\nservers:\n  prod:\n    url: https://x\n");
        let err = resolve_scope(&op, Capability::Direct, flags()).unwrap_err().to_string();
        assert!(err.contains("direct storage access"), "{err}");
    }

    #[test]
    fn cluster_scope_on_data_verb_errors() {
        let op = cfg(
            "clusters:\n  brain:\n    root: s3://acme/brain\nprofiles:\n  admin:\n    cluster: brain\n",
        );
        let err = resolve_scope(
            &op,
            Capability::Any,
            ScopeFlags {
                profile: Some("admin"),
                ..flags()
            },
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("maintenance-only"), "{err}");
    }

    #[test]
    fn unknown_profile_is_a_loud_error() {
        let op = OperatorConfig::default();
        let err = resolve_scope(
            &op,
            Capability::Any,
            ScopeFlags {
                profile: Some("nope"),
                ..flags()
            },
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("unknown profile 'nope'"), "{err}");
    }

    #[test]
    fn no_address_resolves_empty_for_legacy_fallthrough() {
        let op = OperatorConfig::default();
        let scope = resolve_scope(&op, Capability::Any, flags()).unwrap();
        assert_eq!(scope, ResolvedScope::default());
    }
}
