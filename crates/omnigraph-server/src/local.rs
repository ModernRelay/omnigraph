//! Serving one standalone graph to clients of the same OS account on loopback.
//!
//! A local server opens the graph its caller can already open directly, so it
//! grants no more than that caller holds. It is read-only unless asked to
//! write, authenticates every request with a token generated for it, decides
//! every action through a policy generated for it, and holds no cluster
//! admission.

use std::fmt;
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
use crate::{AppState, GraphId, GraphKey, McpSettings, ProcessDefaults, workload};

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
        // One spelling per directory: `..` folds before the root is stored.
        let absolute = omnigraph::storage::absolutize_lexically(path.to_path_buf())
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
}

pub struct LocalOptions {
    pub access: LocalAccess,
    /// The actor every request acts as. Required for `ReadWrite`; a read-only
    /// local server falls back to `default`, the actor a single unnamed
    /// server token resolves to.
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
    /// `ReadWrite` needs an actor to attribute writes to.
    #[error("a read-write local server needs an actor: pass --as or set operator.actor")]
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

const ALL_ACTIONS: [PolicyAction; 13] = [
    PolicyAction::Read,
    PolicyAction::Export,
    PolicyAction::Change,
    PolicyAction::SchemaApply,
    PolicyAction::BranchCreate,
    PolicyAction::BranchDelete,
    PolicyAction::BranchMerge,
    PolicyAction::CommitPublish,
    PolicyAction::CommitDrop,
    PolicyAction::Admin,
    PolicyAction::GraphList,
    PolicyAction::InvokeQuery,
    PolicyAction::ConfigManage,
];

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
        ALL_ACTIONS
            .into_iter()
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
    /// It never acquires cluster admission and refuses a graph inside a
    /// cluster. A read-only local server opens the graph read-only. The
    /// generated policy is installed on the engine as well as at the HTTP
    /// layer, so read-only holds in ingress, in `/mcp` and in every engine
    /// writer.
    pub async fn local(
        scope: LocalScope,
        options: LocalOptions,
    ) -> Result<(AppState, LocalCredential), LocalOpenError> {
        if options.access == LocalAccess::ReadWrite && !read_write_available() {
            return Err(LocalOpenError::ReadWriteUnavailable);
        }
        let LocalScope::Graph(binding) = scope;
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
            for action in ALL_ACTIONS {
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
        // One directory has one spelling: `.` and `..` fold away.
        let folded = LocalBinding::local("/tmp/a/../b/./graph.omni").unwrap();
        assert_eq!(folded.root(), "/tmp/b/graph.omni");
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
