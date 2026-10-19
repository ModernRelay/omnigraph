//! RFC-011 Slice A scope resolution.
//!
//! Translates the new scope inputs (`--profile` / `--store` / operator-config
//! `profiles`/`clusters`/`defaults`) into the SAME effective addressing tuple
//! the existing `GraphClient` factories (`client.rs`) and the maintenance
//! resolver (`helpers::resolve_storage_uri`) already consume. This is a
//! translation layer that sits *in front* of those resolvers — it is purely
//! additive: an explicit legacy address (a positional URI, `--server` or
//! `--store`) wins and reproduces today's behavior exactly, so existing
//! invocations are unaffected.
//!
//! The access path (served vs direct) is never chosen here; it falls out of the
//! scope's binding × the verb's capability. The capability→scope check rejects
//! mismatches (e.g. a server scope on a maintenance verb) only on the *new*
//! resolution paths.

use color_eyre::Result;
use color_eyre::eyre::{bail, eyre};
use omnigraph_api_types::ErrorCode;

use crate::helpers::CliRefusal;
use crate::operator::{OperatorConfig, ScopeBinding};
use crate::planes::Capability;

pub(crate) const PROFILE_ENV: &str = "OMNIGRAPH_PROFILE";

/// The effective addressing a command should use, in the terms the existing
/// resolvers consume. Data/served verbs read `server`/`graph`/`uri`;
/// maintenance and cluster tooling verbs read `cluster`/`cluster_graph`.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct ResolvedScope {
    pub(crate) server: Option<String>,
    pub(crate) graph: Option<String>,
    pub(crate) uri: Option<String>,
    pub(crate) cluster: Option<String>,
    pub(crate) cluster_graph: Option<String>,
    /// The named target the address came from (`--target` or
    /// `defaults.target`), if one did.
    pub(crate) target: Option<String>,
}

/// The raw addressing inputs for one command: the global scope flags plus the
/// command's own positional URI.
pub(crate) struct ScopeFlags<'a> {
    /// `--target`: one complete named binding, exclusive with every other
    /// address and with `--profile`.
    pub(crate) target: Option<&'a str>,
    pub(crate) profile: Option<&'a str>,
    pub(crate) store: Option<&'a str>,
    pub(crate) server: Option<&'a str>,
    pub(crate) cluster: Option<&'a str>,
    pub(crate) graph: Option<&'a str>,
    pub(crate) uri: Option<String>,
    /// Whether the caller reads a cluster root (`cluster`/`cluster_graph`).
    /// A caller that does not refuses a resolved root as the wrong kind of
    /// address, so a root never reaches a resolver that would drop it.
    pub(crate) reads_root: bool,
}

/// Resolve only from supplied values, without environment or I/O access.
/// Precedence:
/// 1. `--target`, alone;
/// 2. explicit primitive address (`uri`/`--server`/`--store`) → passthrough;
/// 3. `--profile` / `OMNIGRAPH_PROFILE`;
/// 4. `defaults.target`;
/// 5. flat `defaults.server` + `defaults.default_graph`, or `defaults.store`;
/// 6. nothing — downstream behaves as today.
pub(crate) fn resolve_scope(
    op: &OperatorConfig,
    ambient_profile: Option<&str>,
    capability: Capability,
    flags: ScopeFlags<'_>,
) -> Result<ResolvedScope> {
    // At most one explicit scope primitive may address a command — a positional
    // URI, `--store`, `--server`, or `--cluster` are mutually exclusive ways to
    // name the graph. Combining them is a contradiction, not a silent precedence.
    let primitives: Vec<&str> = [
        flags.uri.as_deref().map(|_| "a positional URI"),
        flags.store.map(|_| "--store"),
        flags.server.map(|_| "--server"),
        flags.cluster.map(|_| "--cluster"),
    ]
    .into_iter()
    .flatten()
    .collect();
    if primitives.len() > 1 {
        bail!(
            "{} are mutually exclusive — pick one way to address the graph",
            primitives.join(" and ")
        );
    }

    // 1. A target is a complete address; nothing else may add to it.
    if let Some(name) = flags.target {
        if let Some(other) = primitives
            .first()
            .copied()
            .or(flags.profile.map(|_| "--profile"))
        {
            bail!("--target and {other} are mutually exclusive — a target is a complete address");
        }
        return scope_from_target(op, capability, flags.reads_root, name, flags.graph);
    }

    // 1a. `--cluster` is the cluster scope primitive (maintenance): resolve its
    //     root + select the graph with `--graph`.
    if let Some(cluster) = flags.cluster {
        return scope_from_binding(
            op,
            capability,
            flags.reads_root,
            ScopeBinding::Cluster(cluster.to_string()),
            flags.graph.map(str::to_string),
            "--cluster",
        );
    }

    // 1b. Any other explicit address wins; reproduce today's behavior untouched.
    //     `--store` is an explicit store URI — fold it into `uri`.
    if flags.uri.is_some() || flags.server.is_some() || flags.store.is_some() {
        // `--graph` selects within a multi-graph scope; a bare positional URI /
        // `--store` is already a single graph, so a stray `--graph` is an error
        // rather than a silently-dropped flag.
        if flags.graph.is_some() && flags.server.is_none() {
            bail!(
                "--graph selects a graph within a server or cluster scope; a positional \
                 URI / --store is already a single graph"
            );
        }
        return Ok(ResolvedScope {
            server: flags.server.map(str::to_string),
            graph: flags.graph.map(str::to_string),
            uri: flags.store.map(str::to_string).or(flags.uri),
            ..Default::default()
        });
    }

    // 2. A named profile (flag, else the captured environment value).
    let profile_name = flags.profile.or(ambient_profile.filter(|s| !s.is_empty()));
    if let (None, Some(name), Some(target)) = (flags.profile, profile_name, op.default_target()) {
        bail!(
            "OMNIGRAPH_PROFILE selects profile '{name}' and `defaults.target` selects target \
             '{target}'; unset one, or name the address with --target or --profile"
        );
    }
    if let Some(name) = profile_name {
        let profile = op.profile(name).ok_or_else(|| {
            eyre!("unknown profile '{name}' (not defined under `profiles:` in operator config)")
        })?;
        let binding = profile.binding(name)?;
        let graph = flags
            .graph
            .map(str::to_string)
            .or_else(|| profile.default_graph.clone());
        return scope_from_binding(
            op,
            capability,
            flags.reads_root,
            binding,
            graph,
            &format!("profile '{name}'"),
        );
    }

    // 3. The default target.
    if let Some(name) = op.default_target() {
        return scope_from_target(op, capability, flags.reads_root, name, flags.graph);
    }

    // 4. Flat default server scope.
    if let Some(server) = op.default_server() {
        let graph = flags
            .graph
            .map(str::to_string)
            .or_else(|| op.default_graph().map(str::to_string));
        return scope_from_binding(
            op,
            capability,
            flags.reads_root,
            ScopeBinding::Server(server.to_string()),
            graph,
            "operator defaults",
        );
    }

    // 3b. Flat default store scope — the zero-flag local-dev default (RFC-011).
    //     Mutually exclusive with `defaults.server` (enforced at config load).
    if let Some(store) = op.default_store() {
        return scope_from_binding(
            op,
            capability,
            flags.reads_root,
            ScopeBinding::Store(store.to_string()),
            flags.graph.map(str::to_string),
            "operator defaults",
        );
    }

    // 4. Nothing resolved — leave the tuple empty; downstream falls through to
    //    today's behavior (legacy `cli.graph` default or a no-address error).
    Ok(ResolvedScope::default())
}

/// Where a scope points once every name in it is resolved.
enum Address {
    /// A server name from `servers:` or a literal server URL.
    Server(String),
    /// A cluster's storage root.
    Root(String),
    /// One standalone graph's storage.
    Store(String),
}

/// What supplied an address: a named target, or another scope input
/// (`--cluster`, a profile, operator defaults) labelled for messages.
#[derive(Clone, Copy)]
enum Source<'a> {
    Target(&'a str),
    Other(&'a str),
}

impl std::fmt::Display for Source<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Target(name) => write!(f, "target '{name}'"),
            Self::Other(label) => f.write_str(label),
        }
    }
}

impl Source<'_> {
    /// A refusal of the address's kind for this command. A target's carries
    /// `target_kind_unsupported`; the older inputs keep their untyped refusal.
    fn kind_refusal(self, message: String) -> color_eyre::Report {
        match self {
            Self::Target(_) => CliRefusal::report(ErrorCode::TargetKindUnsupported, message),
            Self::Other(_) => eyre!(message),
        }
    }
}

/// A named target as the address it names, with its default graph. A target
/// is complete: its URI or endpoint is used as written, never looked up as a
/// name under `clusters:` or `servers:`.
fn scope_from_target(
    op: &OperatorConfig,
    capability: Capability,
    reads_root: bool,
    name: &str,
    graph: Option<&str>,
) -> Result<ResolvedScope> {
    use crate::targets::TargetBinding;
    let binding = op.target(name)?.binding(name)?;
    let (address, default_graph) = match binding {
        TargetBinding::DirectGraph { uri } => (Address::Store(uri), None),
        TargetBinding::DirectRoot { uri, default_graph } => (Address::Root(uri), default_graph),
        TargetBinding::Server {
            endpoint,
            default_graph,
            ..
        } => (Address::Server(endpoint), default_graph),
    };
    scope_from_address(
        capability,
        reads_root,
        address,
        graph.map(str::to_string).or(default_graph),
        Source::Target(name),
    )
}

/// A profile's, `--cluster`'s or the operator defaults' binding as the address
/// it names. A cluster value is a config name (resolved against `clusters:`)
/// or a literal root: an `s3://`/`az://`/`file://` URI or a local cluster
/// directory. Only a configured name is rewritten; anything else is passed
/// through to the cluster-state resolver verbatim, so a bare directory path
/// keeps working as it did for per-command `--cluster`.
fn scope_from_binding(
    op: &OperatorConfig,
    capability: Capability,
    reads_root: bool,
    binding: ScopeBinding,
    graph: Option<String>,
    source: &str,
) -> Result<ResolvedScope> {
    let address = match binding {
        ScopeBinding::Server(server) => Address::Server(server),
        ScopeBinding::Cluster(cluster) => Address::Root(
            op.cluster_root(&cluster)
                .map(str::to_string)
                .unwrap_or(cluster),
        ),
        ScopeBinding::Store(uri) => Address::Store(uri),
    };
    scope_from_address(
        capability,
        reads_root,
        address,
        graph,
        Source::Other(source),
    )
}

/// Map a resolved address to the effective tuple, enforcing scope × capability
/// (RFC-011): a server scope is served (data only); a cluster root is
/// privileged direct (maintenance/control only, and only for a caller that
/// reads a root); a store scope is direct (either); cluster tooling (control)
/// reads a root and nothing else.
fn scope_from_address(
    capability: Capability,
    reads_root: bool,
    address: Address,
    graph: Option<String>,
    source: Source<'_>,
) -> Result<ResolvedScope> {
    let not_a_root = |what: &str| {
        source.kind_refusal(format!(
            "this command reads a cluster's applied state, but {source} is not a cluster root: \
             it resolves {what}; pass --cluster <dir|uri>, select a cluster profile, or name a \
             cluster-root target (`kind: direct`, `scope: root`)"
        ))
    };
    let mut scope = match address {
        Address::Server(server) => {
            if capability == Capability::Direct {
                return Err(source.kind_refusal(format!(
                    "this command needs direct storage access, but {source} resolves a \
                     server scope; name storage explicitly with --store <uri> (or \
                     --cluster <dir> --graph <id> for a managed graph)"
                )));
            }
            if capability == Capability::Control {
                return Err(not_a_root("a server scope"));
            }
            ResolvedScope {
                server: Some(server),
                graph,
                ..Default::default()
            }
        }
        Address::Root(root) => {
            // A data command that reads a root (`access`, through a cluster
            // graph's applied policy) takes one; every other refuses it.
            if capability == Capability::Any && !reads_root {
                return Err(source.kind_refusal(format!(
                    "{source} resolves a cluster scope, which is not valid for graph data \
                     commands; run data commands through a server, or use --store <uri> \
                     for ad-hoc direct access"
                )));
            }
            if capability == Capability::Served {
                return Err(source.kind_refusal(format!(
                    "this command requires a server, but {source} resolves a cluster \
                     scope; address the server with --server <name|url> or --profile \
                     <name> (to enumerate a cluster's graphs: `omnigraph cluster status \
                     --config <dir>`)"
                )));
            }
            if !reads_root {
                return Err(source.kind_refusal(format!(
                    "this command addresses one standalone graph, but {source} resolves a \
                     cluster root; name the graph's storage with --store <uri> or a direct \
                     graph target (`kind: direct`, `scope: graph`)"
                )));
            }
            // A cluster holds many graphs; maintenance addresses one at a time.
            // When no `--graph`/`default_graph` is given, leave `cluster_graph`
            // empty and defer to the async storage-URI resolver (RFC-011 D7),
            // which enumerates the catalog: auto-use a sole graph, else error
            // and list the candidates.
            ResolvedScope {
                cluster: Some(root),
                cluster_graph: graph,
                ..Default::default()
            }
        }
        Address::Store(uri) => {
            if capability == Capability::Served {
                return Err(source.kind_refusal(format!(
                    "this command requires a server, but {source} resolves a store scope; \
                     address the server with --server <name|url> or --profile <name>"
                )));
            }
            if capability == Capability::Control {
                return Err(not_a_root("a store scope"));
            }
            if graph.is_some() {
                bail!(
                    "--graph does not apply to a store scope ({source}): a store is already \
                     a single graph"
                );
            }
            ResolvedScope {
                uri: Some(uri),
                ..Default::default()
            }
        }
    };
    if let Source::Target(name) = source {
        scope.target = Some(name.to_string());
    }
    Ok(scope)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(yaml: &str) -> OperatorConfig {
        yaml_serde::from_str(yaml).unwrap()
    }

    fn flags<'a>() -> ScopeFlags<'a> {
        ScopeFlags {
            target: None,
            profile: None,
            store: None,
            server: None,
            cluster: None,
            graph: None,
            uri: None,
            reads_root: true,
        }
    }

    #[test]
    fn explicit_legacy_address_wins_unchanged() {
        let op = cfg("defaults:\n  server: prod\nservers:\n  prod:\n    url: https://x\n");
        // Every explicit primitive ignores both profiles and operator defaults.
        for (capability, explicit, expected) in [
            (
                Capability::Any,
                ScopeFlags {
                    uri: Some("graph.omni".into()),
                    ..flags()
                },
                ResolvedScope {
                    uri: Some("graph.omni".into()),
                    ..Default::default()
                },
            ),
            (
                Capability::Any,
                ScopeFlags {
                    store: Some("graph.omni"),
                    ..flags()
                },
                ResolvedScope {
                    uri: Some("graph.omni".into()),
                    ..Default::default()
                },
            ),
            (
                Capability::Any,
                ScopeFlags {
                    server: Some("explicit"),
                    ..flags()
                },
                ResolvedScope {
                    server: Some("explicit".into()),
                    ..Default::default()
                },
            ),
            (
                Capability::Direct,
                ScopeFlags {
                    cluster: Some("./cluster"),
                    ..flags()
                },
                ResolvedScope {
                    cluster: Some("./cluster".into()),
                    ..Default::default()
                },
            ),
        ] {
            let scope = resolve_scope(
                &op,
                Some("unknown-ambient"),
                capability,
                ScopeFlags {
                    profile: Some("unknown-explicit"),
                    ..explicit
                },
            )
            .unwrap();
            assert_eq!(scope, expected);
        }
    }

    #[test]
    fn store_flag_folds_into_uri_and_rejects_graph() {
        let op = OperatorConfig::default();
        let scope = resolve_scope(
            &op,
            None,
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
    fn scope_primitives_are_mutually_exclusive() {
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
            ScopeFlags {
                cluster: Some("./brain"),
                uri: Some("file://other.omni".into()),
                ..flags()
            },
            ScopeFlags {
                cluster: Some("./brain"),
                server: Some("prod"),
                ..flags()
            },
        ] {
            let err = resolve_scope(&op, Some("unknown-ambient"), Capability::Direct, flags)
                .unwrap_err()
                .to_string();
            assert!(err.contains("mutually exclusive"), "{err}");
        }
    }

    #[test]
    fn cluster_flag_resolves_root_and_graph_for_maintenance() {
        let op = cfg("clusters:\n  brain:\n    root: s3://acme/brain\n");
        let scope = resolve_scope(
            &op,
            None,
            Capability::Direct,
            ScopeFlags {
                cluster: Some("brain"),
                graph: Some("knowledge"),
                ..flags()
            },
        )
        .unwrap();
        assert_eq!(scope.cluster.as_deref(), Some("s3://acme/brain"));
        assert_eq!(scope.cluster_graph.as_deref(), Some("knowledge"));
    }

    #[test]
    fn cluster_flag_accepts_a_literal_root_uri() {
        let op = OperatorConfig::default();
        let scope = resolve_scope(
            &op,
            None,
            Capability::Direct,
            ScopeFlags {
                cluster: Some("s3://bucket/clusters/brain"),
                graph: Some("knowledge"),
                ..flags()
            },
        )
        .unwrap();
        assert_eq!(scope.cluster.as_deref(), Some("s3://bucket/clusters/brain"));
        assert_eq!(scope.cluster_graph.as_deref(), Some("knowledge"));
    }

    #[test]
    fn cluster_scope_without_a_graph_defers_to_catalog_enumeration() {
        // RFC-011 D7: with no `--graph`/`default_graph`, resolution no longer
        // bails here — it resolves the cluster root and leaves `cluster_graph`
        // empty, deferring to the async storage-URI resolver (which enumerates
        // the catalog: auto-use a sole graph, else error listing candidates).
        let op = cfg("clusters:\n  brain:\n    root: s3://acme/brain\n");
        let scope = resolve_scope(
            &op,
            None,
            Capability::Direct,
            ScopeFlags {
                cluster: Some("brain"),
                ..flags()
            },
        )
        .unwrap();
        assert_eq!(scope.cluster.as_deref(), Some("s3://acme/brain"));
        assert_eq!(scope.cluster_graph, None);
    }

    #[test]
    fn graph_on_a_bare_store_or_uri_is_rejected() {
        let op = OperatorConfig::default();
        for flags in [
            ScopeFlags {
                uri: Some("graph.omni".into()),
                graph: Some("knowledge"),
                ..flags()
            },
            ScopeFlags {
                store: Some("s3://b/g.omni"),
                graph: Some("knowledge"),
                ..flags()
            },
        ] {
            let err = resolve_scope(&op, None, Capability::Any, flags)
                .unwrap_err()
                .to_string();
            assert!(err.contains("already a single graph"), "{err}");
        }
    }

    #[test]
    fn flat_default_store_drives_local_verbs() {
        // RFC-011: `defaults.store` is the zero-flag local default — no flags,
        // no profile → the store URI resolves as the (single-graph) store scope.
        let op = cfg("defaults:\n  store: file:///tmp/dev.omni\n");
        for ambient in [None, Some("")] {
            let scope = resolve_scope(&op, ambient, Capability::Any, flags()).unwrap();
            assert_eq!(scope.uri.as_deref(), Some("file:///tmp/dev.omni"));
            assert_eq!(scope.server, None);
        }
    }

    #[test]
    fn flat_default_store_rejects_graph() {
        // A store is already a single graph, so `--graph` against a default
        // store is a loud error.
        let op = cfg("defaults:\n  store: file:///tmp/dev.omni\n");
        let err = resolve_scope(
            &op,
            None,
            Capability::Any,
            ScopeFlags {
                graph: Some("knowledge"),
                ..flags()
            },
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("does not apply to a store scope"), "{err}");
    }

    #[test]
    fn flat_default_server_drives_data_verbs() {
        let op = cfg(
            "defaults:\n  server: prod\n  default_graph: knowledge\nservers:\n  prod:\n    url: https://x\n",
        );
        for ambient in [None, Some("")] {
            let scope = resolve_scope(&op, ambient, Capability::Any, flags()).unwrap();
            assert_eq!(scope.server.as_deref(), Some("prod"));
            assert_eq!(scope.graph.as_deref(), Some("knowledge"));
        }
    }

    #[test]
    fn profile_server_scope_with_graph_override() {
        let op = cfg(
            "defaults:\n  store: file:///default.omni\nservers:\n  staging:\n    url: https://s\nprofiles:\n  staging:\n    server: staging\n    default_graph: knowledge\n",
        );
        for (profile, ambient) in [
            (Some("staging"), Some("unknown-ambient")),
            (None, Some("staging")),
        ] {
            let scope = resolve_scope(
                &op,
                ambient,
                Capability::Any,
                ScopeFlags {
                    profile,
                    graph: Some("archive"),
                    ..flags()
                },
            )
            .unwrap();
            assert_eq!(scope.server.as_deref(), Some("staging"));
            assert_eq!(scope.graph.as_deref(), Some("archive")); // flag beats profile default
        }
    }

    #[test]
    fn profile_cluster_scope_resolves_root_for_maintenance() {
        let op = cfg(
            "clusters:\n  brain:\n    root: s3://acme/brain\nprofiles:\n  admin:\n    cluster: brain\n    default_graph: knowledge\n",
        );
        for (profile, ambient) in [(Some("admin"), None), (None, Some("admin"))] {
            let scope = resolve_scope(
                &op,
                ambient,
                Capability::Direct,
                ScopeFlags { profile, ..flags() },
            )
            .unwrap();
            assert_eq!(scope.cluster.as_deref(), Some("s3://acme/brain"));
            assert_eq!(scope.cluster_graph.as_deref(), Some("knowledge"));
        }
    }

    #[test]
    fn profile_cluster_scope_with_graph_override() {
        // The deferral closed by this slice: a `--graph` flag overrides a
        // profile cluster's default_graph, exactly as it does for a server scope.
        let op = cfg(
            "clusters:\n  brain:\n    root: s3://acme/brain\nprofiles:\n  admin:\n    cluster: brain\n    default_graph: knowledge\n",
        );
        let scope = resolve_scope(
            &op,
            None,
            Capability::Direct,
            ScopeFlags {
                profile: Some("admin"),
                graph: Some("archive"),
                ..flags()
            },
        )
        .unwrap();
        assert_eq!(scope.cluster.as_deref(), Some("s3://acme/brain"));
        assert_eq!(scope.cluster_graph.as_deref(), Some("archive")); // flag beats profile default
    }

    #[test]
    fn server_scope_on_maintenance_verb_errors() {
        let op = cfg("defaults:\n  server: prod\nservers:\n  prod:\n    url: https://x\n");
        let err = resolve_scope(&op, None, Capability::Direct, flags())
            .unwrap_err()
            .to_string();
        assert!(err.contains("direct storage access"), "{err}");
    }

    #[test]
    fn cluster_scope_on_data_verb_errors() {
        let op = cfg(
            "clusters:\n  brain:\n    root: s3://acme/brain\nprofiles:\n  admin:\n    cluster: brain\n",
        );
        for (profile, ambient) in [(Some("admin"), None), (None, Some("admin"))] {
            let err = resolve_scope(
                &op,
                ambient,
                Capability::Any,
                ScopeFlags {
                    profile,
                    reads_root: false,
                    ..flags()
                },
            )
            .unwrap_err()
            .to_string();
            assert!(err.contains("not valid for graph data commands"), "{err}");
        }
        // A data command that reads a root, as `access` does, takes one.
        let scope = resolve_scope(
            &op,
            None,
            Capability::Any,
            ScopeFlags {
                profile: Some("admin"),
                ..flags()
            },
        )
        .unwrap();
        assert_eq!(scope.cluster.as_deref(), Some("s3://acme/brain"));
    }

    #[test]
    fn store_scope_on_served_verb_errors() {
        // The served registry (`graphs list`) is server-scoped: a store-bound
        // profile fails with scope-shaped advice, not a late embedded-arm error.
        let op = cfg("profiles:\n  localdev:\n    store: file:///data/dev.omni\n");
        for (profile, ambient) in [(Some("localdev"), None), (None, Some("localdev"))] {
            let err = resolve_scope(
                &op,
                ambient,
                Capability::Served,
                ScopeFlags { profile, ..flags() },
            )
            .unwrap_err()
            .to_string();
            assert!(
                err.contains("requires a server") && err.contains("resolves a store scope"),
                "{err}"
            );
        }
    }

    #[test]
    fn cluster_scope_on_served_verb_points_at_cluster_status() {
        let op = cfg(
            "clusters:\n  brain:\n    root: s3://acme/brain\nprofiles:\n  admin:\n    cluster: brain\n",
        );
        let err = resolve_scope(
            &op,
            None,
            Capability::Served,
            ScopeFlags {
                profile: Some("admin"),
                ..flags()
            },
        )
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("requires a server")
                && err.contains("resolves a cluster scope")
                && err.contains("cluster status"),
            "{err}"
        );
    }

    #[test]
    fn server_scope_on_served_verb_resolves_with_default_graph_carried() {
        // A served-registry caller resolves the server binding like any served
        // verb; the profile's default_graph rides along in the scope (the
        // registry factory deliberately ignores it — pinned in client.rs).
        let op = cfg(
            "servers:\n  prod:\n    url: https://x\nprofiles:\n  staging:\n    server: prod\n    default_graph: kb\n",
        );
        let scope = resolve_scope(
            &op,
            None,
            Capability::Served,
            ScopeFlags {
                profile: Some("staging"),
                ..flags()
            },
        )
        .unwrap();
        assert_eq!(scope.server.as_deref(), Some("prod"));
        assert_eq!(scope.graph.as_deref(), Some("kb"));
    }

    #[test]
    fn unknown_profile_is_a_loud_error() {
        let op = cfg(
            "defaults:\n  store: file:///default.omni\nprofiles:\n  valid:\n    store: file:///profile.omni\n",
        );
        for (profile, ambient, name) in [
            (Some("nope"), Some("valid"), "nope"),
            (None, Some("nope"), "nope"),
            (Some(""), Some("valid"), ""),
            (None, Some(" "), " "),
        ] {
            let err = resolve_scope(
                &op,
                ambient,
                Capability::Any,
                ScopeFlags { profile, ..flags() },
            )
            .unwrap_err()
            .to_string();
            assert_eq!(
                err,
                format!(
                    "unknown profile '{name}' (not defined under `profiles:` in operator config)"
                )
            );
        }
    }

    #[test]
    fn no_address_resolves_empty_for_legacy_fallthrough() {
        let op = OperatorConfig::default();
        for ambient in [None, Some("")] {
            let scope = resolve_scope(&op, ambient, Capability::Any, flags()).unwrap();
            assert_eq!(scope, ResolvedScope::default());
        }
    }

    fn refusal_code(error: &color_eyre::Report) -> Option<ErrorCode> {
        error
            .downcast_ref::<CliRefusal>()
            .map(|refusal| refusal.code)
    }

    const ROOT_TARGETS: &str = "target_config_version: 1\nclusters:\n  /tmp/x/named:\n    root: /tmp/x/other\n  brain:\n    root: /tmp/x/brain\nservers:\n  prod:\n    url: https://x\nprofiles:\n  admin:\n    cluster: brain\n    default_graph: knowledge\n  served:\n    server: prod\ntargets:\n  ops:\n    kind: direct\n    scope: root\n    uri: /tmp/x/named\n    default_graph: knowledge\n  local:\n    kind: direct\n    scope: graph\n    uri: /tmp/x/graph.omni\n  remote:\n    kind: server\n    endpoint: https://remote.example\n    auth: anonymous\n";

    #[test]
    fn a_target_root_is_used_as_written_while_cluster_names_resolve() {
        // A `clusters:` key spelled like a target's URI does not redirect the
        // target: a target is a complete address. `--cluster` and a profile
        // still name `clusters:` entries.
        let op = cfg(ROOT_TARGETS);
        let target = resolve_scope(
            &op,
            None,
            Capability::Direct,
            ScopeFlags {
                target: Some("ops"),
                ..flags()
            },
        )
        .unwrap();
        assert_eq!(
            target,
            ResolvedScope {
                cluster: Some("/tmp/x/named".into()),
                cluster_graph: Some("knowledge".into()),
                target: Some("ops".into()),
                ..Default::default()
            }
        );
        let named = resolve_scope(
            &op,
            None,
            Capability::Direct,
            ScopeFlags {
                cluster: Some("/tmp/x/named"),
                ..flags()
            },
        )
        .unwrap();
        assert_eq!(named.cluster.as_deref(), Some("/tmp/x/other"));
        assert_eq!(named.target, None);
    }

    #[test]
    fn a_root_refuses_by_kind_where_the_command_reads_no_root() {
        // `schema apply`, `upgrade` and the data commands address one graph:
        // a resolved root refuses rather than being dropped on the way.
        let op = cfg(ROOT_TARGETS);
        let one_graph = |flags: ScopeFlags<'_>| {
            resolve_scope(
                &op,
                None,
                Capability::Direct,
                ScopeFlags {
                    reads_root: false,
                    ..flags
                },
            )
        };
        let refused = one_graph(ScopeFlags {
            target: Some("ops"),
            ..flags()
        })
        .unwrap_err();
        assert_eq!(
            refusal_code(&refused),
            Some(ErrorCode::TargetKindUnsupported)
        );
        assert!(
            refused
                .to_string()
                .contains("target 'ops' resolves a cluster root"),
            "{refused}"
        );
        let refused = one_graph(ScopeFlags {
            profile: Some("admin"),
            ..flags()
        })
        .unwrap_err();
        assert_eq!(refusal_code(&refused), None);
        assert!(
            refused
                .to_string()
                .contains("profile 'admin' resolves a cluster root"),
            "{refused}"
        );
        assert_eq!(
            one_graph(ScopeFlags {
                target: Some("local"),
                ..flags()
            })
            .unwrap()
            .uri
            .as_deref(),
            Some("/tmp/x/graph.omni")
        );
        // A data command refuses a root target by the same code.
        let refused = resolve_scope(
            &op,
            None,
            Capability::Any,
            ScopeFlags {
                target: Some("ops"),
                reads_root: false,
                ..flags()
            },
        )
        .unwrap_err();
        assert_eq!(
            refusal_code(&refused),
            Some(ErrorCode::TargetKindUnsupported)
        );
    }

    #[test]
    fn cluster_tooling_resolves_only_a_root_by_the_same_rules() {
        let op = cfg(&format!("{ROOT_TARGETS}defaults:\n  target: ops\n"));
        let control = |ambient: Option<&str>, flags: ScopeFlags<'_>| {
            resolve_scope(&op, ambient, Capability::Control, flags)
        };
        let default = control(None, flags()).unwrap();
        assert_eq!(default.cluster.as_deref(), Some("/tmp/x/named"));
        assert_eq!(default.target.as_deref(), Some("ops"));
        // An explicit cluster profile wins over the default target, and keeps
        // its root; it is not a target.
        let profile = control(
            None,
            ScopeFlags {
                profile: Some("admin"),
                ..flags()
            },
        )
        .unwrap();
        assert_eq!(profile.cluster.as_deref(), Some("/tmp/x/brain"));
        assert_eq!(profile.target, None);
        // An ambient profile beside `defaults.target` is a conflict, as for
        // every other command.
        let refused = control(Some("admin"), flags()).unwrap_err();
        assert!(
            refused.to_string().contains("OMNIGRAPH_PROFILE"),
            "{refused}"
        );
        // An explicit profile or target that names no root refuses; it never
        // falls through to the default target.
        let refused = control(
            None,
            ScopeFlags {
                profile: Some("served"),
                ..flags()
            },
        )
        .unwrap_err();
        assert!(
            refused
                .to_string()
                .contains("profile 'served' is not a cluster root"),
            "{refused}"
        );
        for name in ["remote", "local"] {
            let refused = control(
                None,
                ScopeFlags {
                    target: Some(name),
                    ..flags()
                },
            )
            .unwrap_err();
            assert_eq!(
                refusal_code(&refused),
                Some(ErrorCode::TargetKindUnsupported),
                "{name}"
            );
            assert!(
                refused.to_string().contains("not a cluster root"),
                "{refused}"
            );
        }
    }
}
