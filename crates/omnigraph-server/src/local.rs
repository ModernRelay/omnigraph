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
use omnigraph::storage::StorageKind;
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
    /// Also changes, branch controls and unpublished-commit controls. Not
    /// available yet: [`AppState::local`] refuses it with
    /// [`LocalOpenError::ReadWriteUnavailable`] while a write whose outcome
    /// is unknown still stops the server that ran it.
    ReadWrite,
}

#[cfg(any(test, feature = "test-util"))]
tokio::task_local! {
    static READ_WRITE_FOR_TESTS: ();
}

/// Run `future` with read-write local servers available to the
/// [`AppState::local`] calls it makes on its own task. Tests only: the
/// read-write path exists before its availability, and a build with this
/// feature still refuses read-write everywhere outside this scope.
#[cfg(any(test, feature = "test-util"))]
pub async fn with_local_read_write<F: std::future::Future>(future: F) -> F::Output {
    READ_WRITE_FOR_TESTS.scope((), future).await
}

/// Whether this call may open a read-write local server.
fn read_write_available() -> bool {
    #[cfg(any(test, feature = "test-util"))]
    {
        READ_WRITE_FOR_TESTS.try_with(|_| ()).is_ok()
    }
    #[cfg(not(any(test, feature = "test-util")))]
    {
        false
    }
}

/// The destination a local server opens, captured once by its caller: a root
/// on this machine's file system.
///
/// A remote root's endpoint and credentials would come from the process
/// environment when it opens, which a captured binding must not depend on,
/// so a local server serves local roots only until a binding carries the
/// storage settings its caller resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalBinding {
    root: String,
}

impl LocalBinding {
    /// A binding for the root at `path`, made absolute and normalized as the
    /// engine normalizes roots. A URI, `file://` included, is refused: pass
    /// the path it names.
    pub fn local(path: impl AsRef<std::path::Path>) -> Result<Self, LocalOpenError> {
        let path = path.as_ref();
        let text = path.to_str().ok_or_else(|| {
            LocalOpenError::Root(format!("{} is not a UTF-8 path", path.display()))
        })?;
        if text.contains("://") {
            return Err(LocalOpenError::Root(format!(
                "a local server serves a root on this machine's file system, not {text}"
            )));
        }
        let absolute = std::path::absolute(path)
            .map_err(|error| LocalOpenError::Root(format!("{}: {error}", path.display())))?;
        let absolute = absolute.to_str().ok_or_else(|| {
            LocalOpenError::Root(format!("{} is not a UTF-8 path", absolute.display()))
        })?;
        let root = omnigraph::storage::normalize_root_uri(absolute)
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
    /// `ReadWrite` is not available yet; serve read-only.
    #[error(
        "a read-write local server is not available yet: a write whose outcome is unknown would stop it; serve read-only"
    )]
    ReadWriteUnavailable,
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
    /// A read-write local server's graphs could not be served after it took
    /// the cluster's serving admission. An open may have reached storage, so
    /// the admission stays held as `lock_id` until it is force-unlocked.
    #[error("{message}; the cluster's serving admission stays held as lock {lock_id}")]
    AdmissionRetained { lock_id: String, message: String },
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
    /// the actor, and a graph it binds no policy to grants `read` alone;
    /// read-only captures one ledger revision and opens every graph
    /// read-only against it without admission, with every write forbidden
    /// in each policy, while read-write takes the cluster's serving
    /// admission. Either refuses when no graph opens. Read-only holds in
    /// ingress, in `/mcp` and in every engine writer.
    pub async fn local(
        scope: LocalScope,
        options: LocalOptions,
    ) -> Result<(AppState, LocalCredential), LocalOpenError> {
        if options.access == LocalAccess::ReadWrite && !read_write_available() {
            return Err(LocalOpenError::ReadWriteUnavailable);
        }
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

/// The storage-root URI cluster readers take: a local root is spelled as a
/// `file://` URL, since a bare path names a configuration directory there.
fn cluster_storage_uri(root: &str) -> Result<String, LocalOpenError> {
    match omnigraph::storage::storage_kind_for_uri(root)
        .map_err(|error| LocalOpenError::Root(error.to_string()))?
    {
        StorageKind::Local => url::Url::from_file_path(root)
            .map(String::from)
            .map_err(|()| LocalOpenError::Root(format!("{root} is not an absolute path"))),
        StorageKind::S3 | StorageKind::Azure => Ok(root.to_string()),
    }
}

async fn local_cluster(
    binding: LocalBinding,
    options: LocalOptions,
) -> Result<(AppState, LocalCredential), LocalOpenError> {
    let actor = options.actor.ok_or(LocalOpenError::ActorRequired)?;
    let root = cluster_storage_uri(binding.root())?;
    let token = generate_token();
    let tokens = vec![(actor.clone(), token.clone())];
    let (state, witness) = match options.access {
        LocalAccess::ReadOnly => {
            local_cluster_read_only(&root, binding.root(), &actor, tokens).await?
        }
        LocalAccess::ReadWrite => {
            local_cluster_read_write(&root, binding.root(), &actor, tokens).await?
        }
    };
    // `/readyz` and `/graphs` report the revision served, as a deployment's do.
    let draining = Arc::clone(&state.draining);
    let grace = state.shutdown_grace;
    let state = state
        .with_boot_witness(witness, draining, grace)
        .with_process_defaults(options.process_defaults)
        .with_mcp_settings(McpSettings {
            allow_adhoc: true,
            allow_writes: options.access == LocalAccess::ReadWrite,
            allowed_hosts: Vec::new(),
        })
        .into_local(options.access);
    Ok((state, LocalCredential(token)))
}

/// What a local server of a cluster serves, projected from one snapshot.
struct LocalClusterSettings {
    graphs: Vec<crate::GraphStartupConfig>,
    config_path: std::path::PathBuf,
    server_policy: Option<crate::PolicySource>,
    catalogue_policy: Option<crate::PolicySource>,
    require_all_graphs: bool,
    witness: crate::BootWitness,
}

/// Project `snapshot` as a deployment's boot does. A graph the cluster binds
/// no policy to gets the grant a deployment without one gives its actors,
/// `read` alone, so its engine refuses every write as its routes do.
fn local_cluster_settings(
    config_dir: &str,
    actor: &str,
    snapshot: omnigraph_cluster::ServingSnapshot,
) -> color_eyre::Result<LocalClusterSettings> {
    let config = crate::settings::settings_from_snapshot(
        Path::new(config_dir),
        None,
        false,
        false,
        snapshot,
    )?;
    let ServerConfigMode::Multi {
        mut graphs,
        config_path,
        server_policy,
        catalogue_policy,
    } = config.mode;
    let unbound = unbound_graph_policy(actor)?;
    for cfg in &mut graphs {
        if cfg.policy.is_none() && cfg.startup_failure.is_none() {
            cfg.policy = Some(crate::PolicySource::Inline(unbound.clone()));
        }
    }
    Ok(LocalClusterSettings {
        graphs,
        config_path,
        server_policy,
        catalogue_policy,
        require_all_graphs: config.require_all_graphs,
        witness: config.witness,
    })
}

/// The policy source of a graph the cluster binds no policy to: `read` on
/// every branch for `actor`, what a deployment's routes grant an
/// authenticated actor without a bundle.
fn unbound_graph_policy(actor: &str) -> color_eyre::Result<String> {
    let config = PolicyConfig {
        version: 1,
        groups: [(LOCAL_GROUP.to_string(), vec![actor.to_string()])].into(),
        protected_branches: Vec::new(),
        rules: local_rule(
            "local-read",
            vec![PolicyAction::Read],
            Some(PolicyBranchScope::Any),
            None,
        )
        .into_iter()
        .collect(),
    };
    Ok(yaml_serde::to_string(&config)?)
}

/// A cluster's applied state, served under its serving admission as a
/// deployment serves it.
async fn local_cluster_read_write(
    root: &str,
    config_dir: &str,
    actor: &str,
    tokens: Vec<(String, String)>,
) -> Result<(AppState, crate::BootWitness), LocalOpenError> {
    // The serving owner's admission, held as a deployment holds it and
    // released after a clean drain by `LocalServing::serve`, or by
    // `LocalServing::release` when it never serves.
    let admitted = omnigraph_cluster::admit_serving_snapshot(root)
        .await
        .map_err(LocalOpenError::cluster)?;
    let (snapshot, _, admission) = admitted.into_parts();
    let lock_id = admission.as_ref().map(|owner| owner.lock_id().to_owned());
    // No graph storage is reached before the first graph opens, so a
    // refusal until then releases the admission, as a refused settings
    // preflight does.
    let held = admission.clone();
    let prepared = async {
        let settings = local_cluster_settings(config_dir, actor, snapshot)?;
        let catalogue_policy = crate::load_catalogue_policy(settings.catalogue_policy.as_ref())?;
        let (state, prepared) = crate::prepare_multi_graph_state(
            settings.graphs,
            tokens,
            settings.server_policy.as_ref(),
            settings.config_path,
            admission,
        )
        .await?;
        state.set_catalogue_policy(catalogue_policy);
        Ok::<_, color_eyre::Report>((
            state,
            prepared,
            settings.require_all_graphs,
            settings.witness,
        ))
    }
    .await;
    let (state, prepared, require_all_graphs, witness) = match prepared {
        Ok(prepared) => prepared,
        Err(error) => {
            return Err(LocalOpenError::serving(
                crate::settings::release_settings_refusal(error, held).await,
            ));
        }
    };
    drop(held);
    // An open may have reached storage, so a failed startup keeps the
    // admission and names its lock.
    crate::finish_graph_startup(state.clone(), prepared, require_all_graphs)
        .await
        .map_err(|error| match lock_id {
            Some(lock_id) => LocalOpenError::AdmissionRetained {
                lock_id,
                message: format!("{error:#}"),
            },
            None => LocalOpenError::serving(error),
        })?;
    Ok((state, witness))
}

/// A cluster's applied state, served without admission: one ledger revision
/// is captured, every graph opens read-only and is validated against it, and
/// every policy forbids writes.
async fn local_cluster_read_only(
    root: &str,
    config_dir: &str,
    actor: &str,
    tokens: Vec<(String, String)>,
) -> Result<(AppState, crate::BootWitness), LocalOpenError> {
    let snapshot = omnigraph_cluster::read_serving_snapshot_from_storage(root)
        .await
        .map_err(LocalOpenError::cluster)?;
    let state_cas = snapshot.state_cas.clone();
    let settings =
        local_cluster_settings(config_dir, actor, snapshot).map_err(LocalOpenError::serving)?;
    let server_policy = settings
        .server_policy
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
    let catalogue_policy = crate::load_catalogue_policy(settings.catalogue_policy.as_ref())
        .and_then(|policy| policy.map(forbid_writes).transpose())
        .map_err(|error| LocalOpenError::Policy(error.to_string()))?;
    let configured = settings.graphs.len();
    let mut entries = Vec::with_capacity(configured);
    let mut failures = Vec::new();
    for cfg in settings.graphs {
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
        let opened =
            match crate::prepare_single_graph(cfg, None, GraphOpenAccess::ReadOnly(authority)) {
                Ok(prepared) => crate::open_prepared_graph(prepared).await,
                Err(error) => Err(error),
            };
        if let Err(error) = &opened {
            failures.push(format!("graph '{}': {error:#}", key.graph_id));
        }
        entries.push(read_only_entry(key, uri, opened)?);
    }
    // As a deployment's startup refuses: when no graph opens, and when any
    // fails while every graph is required.
    if (configured > 0 && failures.len() == configured)
        || (settings.require_all_graphs && !failures.is_empty())
    {
        return Err(LocalOpenError::Serving(format!(
            "{} of {configured} graphs could not be served: {}",
            failures.len(),
            failures.join("; ")
        )));
    }
    let state = AppState::new_multi_entries(
        entries,
        tokens,
        server_policy,
        workload::WorkloadController::from_env(),
        Some(settings.config_path),
    )
    .map_err(|error| LocalOpenError::Serving(error.to_string()))?;
    state.set_catalogue_policy(catalogue_policy);
    Ok((state, settings.witness))
}

/// The entry for a graph opened read-only. A graph that fails to open is
/// blocked, as normal serving blocks it, unless the cluster refused the
/// revision captured for it: then the whole server refuses, since it serves
/// one revision or none. A schema drifted from its achieved contract stays
/// that one graph's block.
fn read_only_entry(
    key: GraphKey,
    uri: String,
    opened: Result<crate::OpenedGraph, crate::GraphOpenFailure>,
) -> Result<GraphEntry, LocalOpenError> {
    match opened {
        Ok(opened) => Ok(GraphEntry::ready(opened.handle)),
        Err(mut error) => match error.cluster_refusal.take() {
            Some(refusal) if refusal.code != "applied_schema_drift" => {
                Err(LocalOpenError::cluster(vec![refusal]))
            }
            _ => Ok(crate::blocked_startup_graph(key, uri, error)),
        },
    }
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
    fn a_local_binding_is_absolute_and_its_cluster_uri_names_it() {
        let cwd = std::env::current_dir().unwrap();
        for spelling in ["relative/cluster", "a local cluster"] {
            let binding = LocalBinding::local(spelling).unwrap();
            assert_eq!(Path::new(binding.root()), cwd.join(spelling));
            let uri = cluster_storage_uri(binding.root()).unwrap();
            assert!(uri.starts_with("file:///"), "{uri}");
            assert_eq!(
                omnigraph::storage::normalize_root_uri(&uri).unwrap(),
                binding.root()
            );
        }
    }

    /// A ledger that moves between a read-only open's capture and its
    /// validation refuses the whole server; a graph whose schema drifted
    /// from its achieved contract is blocked alone.
    #[tokio::test]
    async fn a_read_only_open_refuses_a_moved_revision_and_blocks_a_drifted_graph() {
        let dir = tempfile::tempdir().unwrap();
        let schema = "node Person { name: String @key }\n";
        std::fs::write(dir.path().join("people.pg"), schema).unwrap();
        std::fs::write(
            dir.path().join("cluster.yaml"),
            "version: 1\ngraphs:\n  knowledge:\n    schema: ./people.pg\n",
        )
        .unwrap();
        let caller = omnigraph_cluster::DeploymentCaller::storage_owner(None);
        let applied = omnigraph_cluster::apply_deployment(dir.path(), None, &caller, |_, _, _| {})
            .await
            .unwrap();
        assert!(
            matches!(applied, omnigraph_cluster::DeploymentLookup::Complete { ref result } if result.converged),
            "{applied:?}"
        );
        let root = dir.path().to_str().unwrap();
        let status = omnigraph_cluster::deployment_status(root, None, &caller)
            .await
            .unwrap();
        if let Some(lock_id) = status.lock_id {
            omnigraph_cluster::force_unlock_storage_root(root, &lock_id)
                .await
                .unwrap();
        }
        let snapshot = omnigraph_cluster::read_serving_snapshot_from_storage(
            &cluster_storage_uri(root).unwrap(),
        )
        .await
        .unwrap();
        let state_cas = snapshot.state_cas.clone();
        let ServerConfigMode::Multi { graphs, .. } =
            crate::settings::settings_from_snapshot(dir.path(), None, false, false, snapshot)
                .unwrap()
                .mode;
        let cfg = graphs.into_iter().next().unwrap();
        let key = GraphKey::cluster(GraphId::try_from("knowledge").unwrap());
        let uri = omnigraph::storage::normalize_root_uri(&cfg.uri).unwrap();
        let open = |cfg, authority| async move {
            let prepared =
                crate::prepare_single_graph(cfg, None, GraphOpenAccess::ReadOnly(authority))
                    .unwrap();
            crate::open_prepared_graph(prepared).await
        };

        let authority = omnigraph_cluster::GraphReadAuthority::capture(&uri, state_cas.as_deref())
            .await
            .unwrap()
            .unwrap();
        let state_path = dir.path().join("__cluster/state.json");
        let before = std::fs::read(&state_path).unwrap();
        let mut moved: serde_json::Value = serde_json::from_slice(&before).unwrap();
        moved["state_revision"] = serde_json::json!(moved["state_revision"].as_u64().unwrap() + 1);
        std::fs::write(&state_path, serde_json::to_vec(&moved).unwrap()).unwrap();
        let Err(failure) = open(cfg.clone(), authority).await else {
            panic!("a moved revision must not validate");
        };
        assert_eq!(
            failure.cluster_refusal.as_ref().map(|d| d.code.as_str()),
            Some("cluster_read_revision_changed")
        );
        match read_only_entry(key.clone(), uri.clone(), Err(failure)) {
            Err(LocalOpenError::Cluster { code, .. }) => {
                assert_eq!(code, "cluster_read_revision_changed")
            }
            Err(other) => panic!("{other}"),
            Ok(_) => panic!("a moved revision must refuse the whole server"),
        }

        std::fs::write(&state_path, &before).unwrap();
        std::fs::remove_dir_all(&uri).unwrap();
        drop(Omnigraph::init(&uri, schema).await.unwrap());
        let authority = omnigraph_cluster::GraphReadAuthority::capture(&uri, None)
            .await
            .unwrap()
            .unwrap();
        let Err(failure) = open(cfg, authority).await else {
            panic!("a drifted schema must not validate");
        };
        assert_eq!(
            failure.cluster_refusal.as_ref().map(|d| d.code.as_str()),
            Some("applied_schema_drift")
        );
        assert!(matches!(
            read_only_entry(key, uri, Err(failure)),
            Ok(GraphEntry::Blocked(_))
        ));
    }

    #[test]
    fn a_local_binding_is_an_absolute_path_and_never_a_uri() {
        let binding = LocalBinding::local("relative/graph.omni").unwrap();
        assert!(
            std::path::Path::new(binding.root()).is_absolute(),
            "{binding:?}"
        );
        assert!(
            binding.root().ends_with("relative/graph.omni"),
            "{binding:?}"
        );
        for uri in [
            "s3://bucket/graph",
            "az://container/graph",
            "file:///tmp/graph",
        ] {
            assert!(
                matches!(LocalBinding::local(uri), Err(LocalOpenError::Root(_))),
                "{uri}"
            );
        }
    }

    #[tokio::test]
    async fn read_write_opens_only_inside_the_test_scope() {
        let options = || LocalOptions {
            access: LocalAccess::ReadWrite,
            actor: Some("act-me".into()),
            process_defaults: ProcessDefaults::default(),
        };
        let scope = || LocalScope::Graph(LocalBinding::local("/nonexistent/graph.omni").unwrap());
        assert!(matches!(
            AppState::local(scope(), options()).await,
            Err(LocalOpenError::ReadWriteUnavailable)
        ));
        // Inside the scope the gate passes and the open itself fails.
        let opened = with_local_read_write(AppState::local(scope(), options())).await;
        assert!(
            !matches!(opened, Err(LocalOpenError::ReadWriteUnavailable)),
            "{:?}",
            opened.err()
        );
    }

    /// The engine refuses a write the HTTP layer never sees: an embedder
    /// holding a read-only local server's graph handle cannot change it.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_read_only_local_servers_engine_refuses_a_direct_write() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("graph.omni");
        std::fs::create_dir_all(&root).unwrap();
        Omnigraph::init(
            root.to_str().unwrap(),
            "node Person {\n    name: String @key\n}\n",
        )
        .await
        .unwrap();
        let (state, _) = AppState::local(
            LocalScope::Graph(LocalBinding::local(&root).unwrap()),
            LocalOptions {
                access: LocalAccess::ReadOnly,
                actor: Some("act-me".into()),
                process_defaults: ProcessDefaults::default(),
            },
        )
        .await
        .ok()
        .unwrap();
        let key = GraphKey::cluster(GraphId::try_from("default").unwrap());
        let crate::registry::RegistryLookup::Ready(view) = state.routing.registry.get(&key) else {
            panic!("the local graph is served");
        };
        let session = omnigraph::Session::from_defaults(
            view.handle().engine.clone(),
            omnigraph::settings::SessionSettings::default(),
        );
        let mut params: omnigraph_compiler::ParamMap = Default::default();
        params.insert(
            "name".to_string(),
            omnigraph_compiler::Literal::String("Direct".to_string()),
        );
        let refused = session
            .mutate_as(
                "main",
                "query add($name: String) { insert Person { name: $name } }",
                "add",
                &params,
                Some("act-me"),
            )
            .await;
        assert!(matches!(refused, Err(OmniError::Policy(_))), "{refused:?}");
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
