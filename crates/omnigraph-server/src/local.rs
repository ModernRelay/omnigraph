//! Serving one standalone graph or one cluster to clients of the same OS
//! account on loopback.
//!
//! A local server opens what its caller can already open directly, so it
//! grants no more than that caller holds. It is read-only unless asked to
//! write and authenticates every request with a token generated for it. A
//! standalone graph's local server decides every action through a policy
//! generated for it; a cluster's applies the cluster's own policy to its
//! actor. A read-only local server takes no cluster admission; a read-write
//! one of a cluster takes it as a deployment does.

use std::fmt;
use std::path::Path;
use std::sync::Arc;

use base64::Engine as _;
use omnigraph::db::Omnigraph;
use omnigraph::error::OmniError;
use omnigraph_policy::{
    PolicyAction, PolicyActorSelector, PolicyAllowRule, PolicyBranchScope, PolicyConfig,
    PolicyEngine, PolicyRule,
};
use rand::RngCore as _;

use crate::registry::{GraphEntry, GraphHandle};
use crate::{
    AppState, GraphId, GraphKey, GraphOpenAccess, McpSettings, ProcessDefaults, ServerConfigMode,
    workload,
};

/// Whether a local server may change its graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalAccess {
    /// Reads and exports only; every write route refuses before its body.
    ReadOnly,
    /// Also changes, branch controls and unpublished-commit controls.
    ReadWrite,
}

/// The destination a local server opens, captured once by its caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalBinding {
    root: String,
}

impl LocalBinding {
    /// A binding for a graph root the caller resolved, normalized as the
    /// engine normalizes roots.
    pub fn from_root(root: &str) -> Result<Self, LocalOpenError> {
        let root = omnigraph::storage::normalize_root_uri(root)
            .map_err(|error| LocalOpenError::Root(error.to_string()))?;
        Ok(Self { root })
    }

    pub fn root(&self) -> &str {
        &self.root
    }
}

/// What a local server serves.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum LocalScope {
    /// A standalone graph, served under the route id `default`.
    Graph(LocalBinding),
    /// A cluster's storage root, served from its applied state under each
    /// graph's id.
    Cluster(LocalBinding),
}

pub struct LocalOptions {
    pub access: LocalAccess,
    /// The actor every request acts as. Required for `ReadWrite` and for a
    /// cluster; a read-only local server of a standalone graph falls back to
    /// `default`, the actor a single unnamed server token resolves to.
    pub actor: Option<String>,
    pub process_defaults: ProcessDefaults,
}

/// The local server's bearer token. Reveal it once, to deliver it to the
/// local server's clients; it is never printed by `Debug` and the server
/// keeps only its hash.
pub struct LocalCredential(String);

impl LocalCredential {
    pub fn reveal(self) -> String {
        self.0
    }
}

impl fmt::Debug for LocalCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("LocalCredential(<redacted>)")
    }
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum LocalOpenError {
    /// `ReadWrite` needs an actor to attribute writes to, and a cluster's
    /// policy needs one to decide for.
    #[error("this local server needs an actor: pass --as or set operator.actor")]
    ActorRequired,
    /// The root belongs to a cluster, which this local server does not serve.
    #[error(
        "{root} belongs to the cluster at {cluster}; serve the cluster instead of one of its graphs"
    )]
    ClusterGraph { root: String, cluster: String },
    /// The root could not be normalized or classified.
    #[error("graph root: {0}")]
    Root(String),
    #[error(transparent)]
    Open(OmniError),
    #[error("local server policy: {0}")]
    Policy(String),
    /// The cluster refused admission or its applied state, with its code.
    #[error("[{code}] {message}")]
    Cluster { code: String, message: String },
    /// The cluster's applied state could not be served.
    #[error("{0}")]
    Serving(String),
}

impl LocalOpenError {
    fn cluster(diagnostics: Vec<omnigraph_cluster::Diagnostic>) -> Self {
        let message = diagnostics
            .iter()
            .map(|diagnostic| format!("{}: {}", diagnostic.path, diagnostic.message))
            .collect::<Vec<_>>()
            .join("; ");
        Self::Cluster {
            code: diagnostics
                .first()
                .map_or_else(|| "cluster_unavailable".to_string(), |d| d.code.clone()),
            message,
        }
    }

    fn serving(error: color_eyre::Report) -> Self {
        Self::Serving(format!("{error:#}"))
    }
}

/// What a local server grants for an action. Every action is classified here,
/// so an action added later is never granted until it is given a class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalClass {
    /// Granted to every local server, on every branch and graph-wide.
    Read,
    /// Granted to a read-write local server, on every branch.
    Write,
    /// Granted to a read-write local server, with any target branch.
    BranchControl,
    /// Server-scoped listing of the one served graph.
    ServerRead,
    /// Never granted: schema, administration, configuration and stored
    /// queries stay outside a local server.
    Never,
}

fn local_class(action: PolicyAction) -> LocalClass {
    match action {
        PolicyAction::Read | PolicyAction::Export => LocalClass::Read,
        PolicyAction::Change => LocalClass::Write,
        PolicyAction::BranchCreate
        | PolicyAction::BranchDelete
        | PolicyAction::BranchMerge
        | PolicyAction::CommitPublish
        | PolicyAction::CommitDrop => LocalClass::BranchControl,
        PolicyAction::GraphList => LocalClass::ServerRead,
        PolicyAction::SchemaApply
        | PolicyAction::Admin
        | PolicyAction::InvokeQuery
        | PolicyAction::ConfigManage => LocalClass::Never,
    }
}

/// Whether an action can change what a graph or a cluster holds. A read-only
/// local server of a cluster forbids every one of them, whatever the
/// cluster's policy permits.
fn writes(action: PolicyAction) -> bool {
    match action {
        PolicyAction::Read
        | PolicyAction::Export
        | PolicyAction::GraphList
        | PolicyAction::InvokeQuery => false,
        PolicyAction::Change
        | PolicyAction::SchemaApply
        | PolicyAction::BranchCreate
        | PolicyAction::BranchDelete
        | PolicyAction::BranchMerge
        | PolicyAction::CommitPublish
        | PolicyAction::CommitDrop
        | PolicyAction::Admin
        | PolicyAction::ConfigManage => true,
    }
}

/// `policy` with every write action forbidden.
pub(crate) fn forbid_writes(policy: PolicyEngine) -> color_eyre::Result<PolicyEngine> {
    let actions = all_actions()
        .filter(|action| writes(*action))
        .collect::<Vec<_>>();
    policy.forbidding("local-read-only", &actions)
}

/// Every policy action, from the enum's own variant list, so one added later
/// is classified (the matches above are exhaustive) and forbidden when it
/// writes without being listed here.
fn all_actions() -> impl Iterator<Item = PolicyAction> {
    <PolicyAction as clap::ValueEnum>::value_variants()
        .iter()
        .copied()
}

const LOCAL_GROUP: &str = "local";

fn local_rule(
    id: &str,
    actions: Vec<PolicyAction>,
    branch_scope: Option<PolicyBranchScope>,
    target_branch_scope: Option<PolicyBranchScope>,
) -> Option<PolicyRule> {
    (!actions.is_empty()).then(|| PolicyRule {
        id: id.to_string(),
        allow: PolicyAllowRule {
            actors: PolicyActorSelector {
                group: LOCAL_GROUP.to_string(),
            },
            actions,
            branch_scope,
            target_branch_scope,
        },
    })
}

/// The graph and server policies a local server installs for `actor`.
fn local_policies(
    actor: &str,
    access: LocalAccess,
) -> Result<(PolicyEngine, PolicyEngine), LocalOpenError> {
    let granted = |class: LocalClass| -> Vec<PolicyAction> {
        all_actions()
            .filter(|action| local_class(*action) == class)
            .collect()
    };
    let config = |rules: Vec<Option<PolicyRule>>| PolicyConfig {
        version: 1,
        groups: [(LOCAL_GROUP.to_string(), vec![actor.to_string()])].into(),
        protected_branches: Vec::new(),
        rules: rules.into_iter().flatten().collect(),
    };
    let mut graph_rules = vec![local_rule(
        "local-read",
        granted(LocalClass::Read),
        Some(PolicyBranchScope::Any),
        None,
    )];
    if access == LocalAccess::ReadWrite {
        graph_rules.push(local_rule(
            "local-write",
            granted(LocalClass::Write),
            Some(PolicyBranchScope::Any),
            None,
        ));
        graph_rules.push(local_rule(
            "local-branch-control",
            granted(LocalClass::BranchControl),
            None,
            Some(PolicyBranchScope::Any),
        ));
    }
    let graph = PolicyEngine::graph_from_config(&config(graph_rules), "default")
        .map_err(|error| LocalOpenError::Policy(error.to_string()))?;
    let server = PolicyEngine::server_from_config(&config(vec![local_rule(
        "local-graph-list",
        granted(LocalClass::ServerRead),
        None,
        None,
    )]))
    .map_err(|error| LocalOpenError::Policy(error.to_string()))?;
    Ok((graph, server))
}

fn generate_token() -> String {
    let mut bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

impl AppState {
    /// Open `scope` as a local server and return its state with the one
    /// bearer token its clients use.
    ///
    /// For a standalone graph it refuses a root inside a cluster, takes no
    /// admission, and installs a generated policy on the engine as well as at
    /// the HTTP layer. For a cluster the cluster's applied policy decides for
    /// the actor; read-only captures one ledger revision and opens every
    /// graph read-only against it without admission, with every write
    /// forbidden in each policy, while read-write takes the cluster's
    /// serving admission. Read-only holds in ingress, in `/mcp` and in every
    /// engine writer.
    pub async fn local(
        scope: LocalScope,
        options: LocalOptions,
    ) -> Result<(AppState, LocalCredential), LocalOpenError> {
        match scope {
            LocalScope::Graph(binding) => local_graph(binding, options).await,
            LocalScope::Cluster(binding) => local_cluster(binding, options).await,
        }
    }
}

async fn local_graph(
    binding: LocalBinding,
    options: LocalOptions,
) -> Result<(AppState, LocalCredential), LocalOpenError> {
    let actor = match (options.access, options.actor) {
        (_, Some(actor)) => actor,
        (LocalAccess::ReadOnly, None) => "default".to_string(),
        (LocalAccess::ReadWrite, None) => return Err(LocalOpenError::ActorRequired),
    };
    if let Some(cluster) = omnigraph_cluster::cluster_root_for_graph_uri(binding.root())
        .await
        .map_err(|diagnostic| LocalOpenError::Root(diagnostic.message))?
    {
        return Err(LocalOpenError::ClusterGraph {
            root: binding.root().to_string(),
            cluster,
        });
    }
    let (graph_policy, server_policy) = local_policies(&actor, options.access)?;
    let db = match options.access {
        LocalAccess::ReadOnly => Omnigraph::open_read_only(binding.root()).await,
        LocalAccess::ReadWrite => Omnigraph::open(binding.root()).await,
    }
    .map_err(LocalOpenError::Open)?;
    let graph_policy = Arc::new(graph_policy);
    let checker = Arc::clone(&graph_policy) as Arc<dyn omnigraph_policy::PolicyChecker>;
    let handle = Arc::new(GraphHandle {
        key: GraphKey::cluster(GraphId::try_from("default").expect("valid graph id")),
        uri: binding.root().to_string(),
        engine: Arc::new(db.with_policy(checker)),
        policy: Some(graph_policy),
        queries: None,
    });
    let token = generate_token();
    let state = AppState::new_multi_entries(
        vec![GraphEntry::ready(handle)],
        vec![(actor, token.clone())],
        Some(server_policy),
        workload::WorkloadController::from_env(),
        None,
    )
    .map_err(|error| LocalOpenError::Policy(error.to_string()))?
    .with_process_defaults(options.process_defaults)
    .with_mcp_settings(McpSettings {
        // A local graph has no stored queries, so ad-hoc reads are its
        // only MCP data read.
        allow_adhoc: true,
        allow_writes: options.access == LocalAccess::ReadWrite,
        allowed_hosts: Vec::new(),
    })
    .into_local(options.access);
    Ok((state, LocalCredential(token)))
}

/// The storage-root URI cluster readers take: a local root is spelled with
/// `file://`, since a bare path names a configuration directory there.
fn cluster_storage_uri(root: &str) -> String {
    if root.contains("://") {
        root.to_string()
    } else {
        format!("file://{root}")
    }
}

async fn local_cluster(
    binding: LocalBinding,
    options: LocalOptions,
) -> Result<(AppState, LocalCredential), LocalOpenError> {
    let actor = options.actor.ok_or(LocalOpenError::ActorRequired)?;
    let root = cluster_storage_uri(binding.root());
    let token = generate_token();
    let tokens = vec![(actor, token.clone())];
    let state = match options.access {
        LocalAccess::ReadOnly => local_cluster_read_only(&root, binding.root(), tokens).await?,
        LocalAccess::ReadWrite => {
            // The serving owner's admission, held as a deployment holds it
            // and released by `serve_local` after a clean drain.
            let admitted = omnigraph_cluster::admit_serving_snapshot(&root)
                .await
                .map_err(LocalOpenError::cluster)?;
            let (snapshot, _, admission) = admitted.into_parts();
            let config = match crate::settings::settings_from_snapshot(
                Path::new(binding.root()),
                None,
                false,
                false,
                snapshot,
            ) {
                Ok(config) => config,
                Err(error) => {
                    return Err(LocalOpenError::serving(
                        crate::settings::release_settings_refusal(error, admission).await,
                    ));
                }
            };
            let ServerConfigMode::Multi {
                graphs,
                config_path,
                server_policy,
                catalogue_policy,
            } = config.mode;
            let (state, prepared) = crate::prepare_multi_graph_state(
                graphs,
                tokens,
                server_policy.as_ref(),
                config_path,
                admission,
            )
            .await
            .map_err(LocalOpenError::serving)?;
            state.set_catalogue_policy(
                crate::load_catalogue_policy(catalogue_policy.as_ref())
                    .map_err(LocalOpenError::serving)?,
            );
            crate::finish_graph_startup(state.clone(), prepared, false)
                .await
                .map_err(LocalOpenError::serving)?;
            state
        }
    };
    let state = state
        .with_process_defaults(options.process_defaults)
        .with_mcp_settings(McpSettings {
            allow_adhoc: true,
            allow_writes: options.access == LocalAccess::ReadWrite,
            allowed_hosts: Vec::new(),
        })
        .into_local(options.access);
    Ok((state, LocalCredential(token)))
}

/// A cluster's applied state, served without admission: one ledger revision
/// is captured, every graph opens read-only and is validated against it, and
/// every policy forbids writes.
async fn local_cluster_read_only(
    root: &str,
    config_dir: &str,
    tokens: Vec<(String, String)>,
) -> Result<AppState, LocalOpenError> {
    let snapshot = omnigraph_cluster::read_serving_snapshot_from_storage(root)
        .await
        .map_err(LocalOpenError::cluster)?;
    let state_cas = snapshot.state_cas.clone();
    let config = crate::settings::settings_from_snapshot(
        Path::new(config_dir),
        None,
        false,
        false,
        snapshot,
    )
    .map_err(LocalOpenError::serving)?;
    let ServerConfigMode::Multi {
        graphs,
        config_path,
        server_policy,
        catalogue_policy,
    } = config.mode;
    let server_policy = server_policy
        .as_ref()
        .map(|source| {
            let policy = match source {
                crate::PolicySource::File(path) => PolicyEngine::load_cluster(path),
                crate::PolicySource::Inline(text) => PolicyEngine::load_cluster_from_source(text),
            }?;
            forbid_writes(policy)
        })
        .transpose()
        .map_err(|error| LocalOpenError::Policy(error.to_string()))?;
    let mut entries = Vec::with_capacity(graphs.len());
    for cfg in graphs {
        let key = GraphKey::cluster(
            GraphId::try_from(cfg.graph_id.clone())
                .map_err(|error| LocalOpenError::Serving(error.to_string()))?,
        );
        let uri = omnigraph::storage::normalize_root_uri(&cfg.uri)
            .map_err(|error| LocalOpenError::Root(error.to_string()))?;
        let authority = match omnigraph_cluster::GraphReadAuthority::capture(
            &uri,
            state_cas.as_deref(),
        )
        .await
        {
            Ok(Some(authority)) => authority,
            Ok(None) => {
                return Err(LocalOpenError::Serving(format!(
                    "graph '{}' is not inside its cluster's root",
                    cfg.graph_id
                )));
            }
            // A changed revision, an outstanding deployment or a ledger
            // needing conversion refuses the whole server: it serves one
            // revision or none.
            Err(diagnostic) => return Err(LocalOpenError::cluster(vec![diagnostic])),
        };
        let entry =
            match crate::prepare_single_graph(cfg, None, GraphOpenAccess::ReadOnly(authority)) {
                Ok(prepared) => match crate::open_prepared_graph(prepared).await {
                    Ok(opened) => GraphEntry::ready(opened.handle),
                    Err(error) => crate::blocked_startup_graph(key, uri, error),
                },
                Err(error) => crate::blocked_startup_graph(key, uri, error),
            };
        entries.push(entry);
    }
    let state = AppState::new_multi_entries(
        entries,
        tokens,
        server_policy,
        workload::WorkloadController::from_env(),
        Some(config_path),
    )
    .map_err(|error| LocalOpenError::Serving(error.to_string()))?;
    state.set_catalogue_policy(
        crate::load_catalogue_policy(catalogue_policy.as_ref()).map_err(LocalOpenError::serving)?,
    );
    Ok(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use omnigraph_policy::PolicyRequest;

    fn decide(engine: &PolicyEngine, action: PolicyAction, branch: Option<&str>) -> bool {
        let target_scoped = matches!(local_class(action), LocalClass::BranchControl);
        engine
            .authorize(
                "act-me",
                &PolicyRequest {
                    action,
                    branch: (!target_scoped)
                        .then(|| branch.map(str::to_string))
                        .flatten(),
                    target_branch: target_scoped.then(|| branch.map(str::to_string)).flatten(),
                },
            )
            .unwrap()
            .allowed
    }

    #[test]
    fn every_action_has_a_local_class_and_only_its_class_is_granted() {
        for access in [LocalAccess::ReadOnly, LocalAccess::ReadWrite] {
            let (graph, server) = local_policies("act-me", access).unwrap();
            for action in all_actions() {
                let expected = match local_class(action) {
                    LocalClass::Read => true,
                    LocalClass::Write | LocalClass::BranchControl => {
                        access == LocalAccess::ReadWrite
                    }
                    LocalClass::ServerRead | LocalClass::Never => false,
                };
                if local_class(action) == LocalClass::ServerRead {
                    assert!(decide(&server, action, None), "{action}");
                    continue;
                }
                for branch in [None, Some("main"), Some("feature")] {
                    assert_eq!(
                        decide(&graph, action, branch),
                        expected,
                        "{access:?} {action} {branch:?}"
                    );
                }
            }
            // Another actor holds nothing.
            assert!(
                !graph
                    .authorize(
                        "act-other",
                        &PolicyRequest {
                            action: PolicyAction::Read,
                            branch: Some("main".into()),
                            target_branch: None,
                        },
                    )
                    .unwrap()
                    .allowed
            );
        }
    }

    #[test]
    fn what_a_read_write_local_server_adds_is_what_a_read_only_one_forbids() {
        assert_eq!(all_actions().count(), 13);
        for action in all_actions() {
            match local_class(action) {
                LocalClass::Read | LocalClass::ServerRead => assert!(!writes(action), "{action}"),
                LocalClass::Write | LocalClass::BranchControl => {
                    assert!(writes(action), "{action}")
                }
                LocalClass::Never => {}
            }
        }
    }

    #[test]
    fn a_credential_never_prints_its_token() {
        let credential = LocalCredential(generate_token());
        assert_eq!(format!("{credential:?}"), "LocalCredential(<redacted>)");
        let token = credential.reveal();
        assert_eq!(token.len(), 43, "32 bytes, base64url without padding");
        assert_ne!(token, generate_token());
    }
}
