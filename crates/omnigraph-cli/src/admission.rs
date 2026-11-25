//! Writable embedded commands share one root admission per CLI invocation.
//! Read-only commands observe authority without a lock; HTTP uses its server.

use std::io::Write;
use std::{collections::BTreeMap, future::Future, sync::Arc};

use color_eyre::eyre::{Result, eyre};
use omnigraph::storage::{CapturedStorageBinding, storage_for_binding};
use omnigraph_cluster::{ClusterAdmission, ClusterAdmissionPurpose};
use tokio::sync::Mutex;

use crate::command_outcome::EngineWrite;

type Owners = Arc<Mutex<BTreeMap<String, ClusterAdmission>>>;

tokio::task_local! {
    static COMMAND: Owners;
}

pub(crate) async fn scope<F: Future>(future: F) -> F::Output {
    COMMAND.scope(Owners::default(), future).await
}

/// The storage an embedded command opens its graph with: the captured
/// binding's own, or, under `--base`, read-through storage over `base`
/// (the graph shallow clone RFC), built from the two root URIs; an S3 pair
/// takes its clients from the standard `AWS_*` environment, as the engine's
/// `open_clone` does. A root that holds a clone's `__clone/created` marker
/// is a clone, and opening it without its base is refused by name.
pub(crate) async fn embedded_storage(
    binding: &CapturedStorageBinding,
    base: Option<&str>,
) -> Result<Arc<dyn omnigraph::storage::StorageAdapter>> {
    match base {
        None => {
            let storage = storage_for_binding(binding)?;
            let root = binding.uri();
            if storage.exists(&clone_marker(root)).await? {
                return Err(crate::targets::refusal(
                    "target_invalid",
                    format!(
                        "{root} is a graph shallow clone; open it with --base <the base it was cloned from>"
                    ),
                ));
            }
            Ok(storage)
        }
        Some(base) => {
            // The build writes the marker when the clone root has none,
            // before the engine opens the graph, so the open's own refusal
            // no longer says that the command took no effect.
            let marker = clone_marker(binding.uri());
            if !storage_for_binding(binding)?.exists(&marker).await? {
                crate::command_outcome::storage_wrote();
            }
            Ok(omnigraph::storage::storage_read_through(base, binding.uri()).await?)
        }
    }
}

/// The object whose presence makes `root` a graph shallow clone's root.
fn clone_marker(root: &str) -> String {
    format!("{}/__clone/created", root.trim_end_matches('/'))
}

/// Read commands never acquire writer admission or run the writable capability
/// probe. Cluster authority is observed around the open, not held as a lease.
pub(crate) async fn open_read_only_with_binding(
    binding: &CapturedStorageBinding,
    base: Option<&str>,
    expected_state_cas: Option<&str>,
) -> Result<omnigraph::db::Omnigraph> {
    let authority =
        omnigraph_cluster::GraphReadAuthority::capture_with_binding(binding, expected_state_cas)
            .await
            .map_err(report)?;
    let db = omnigraph::db::Omnigraph::open_read_only_with_storage(
        binding.uri(),
        embedded_storage(binding, base).await?,
    )
    .await?;
    if let Some(authority) = authority {
        authority.validate_opened(&db).await.map_err(report)?;
    }
    Ok(db)
}

/// The one writable opener of an embedded command. A graph inside a cluster
/// is admitted first (before the opener's capability probe), and the policy
/// the applied revision binds to it is installed on the handle, so every
/// `_as` writer judges the declared actor as a server judges a served one. A
/// standalone graph has no policy. `storage` builds the adapter the caller
/// opens the graph with; it runs only once the graph is admitted, since
/// building a read-through adapter under `--base` writes the clone marker.
/// `external_blob_policy`, when given, is installed on the opened handle as
/// part of the open. A base over the storage root that holds the graph is
/// refused first: the cluster's root for a cluster graph, which holds every
/// graph root, the ledger and the policies, as `cluster validate` judges a
/// configured Blob base, or the graph's own root for a standalone graph. The check
/// runs before the command takes a cluster's admission, which it would retain,
/// so that refusal leaves nothing behind; the handle refuses a base over the
/// graph's root again at install. Either refusal, like the open's own, is the
/// engine's typed error, which says the write did not start.
pub(crate) async fn open_writable(
    binding: &CapturedStorageBinding,
    storage: impl Future<Output = Result<Arc<dyn omnigraph::storage::StorageAdapter>>>,
    external_blob_policy: Option<omnigraph::ExternalBlobPolicy>,
    stderr: &mut dyn Write,
) -> Result<omnigraph::db::Omnigraph> {
    let cluster_root = omnigraph_cluster::cluster_root_for_graph_binding(binding)
        .await
        .map_err(report)?;
    if let Some(policy) = &external_blob_policy {
        refuse_source_over_root(policy, cluster_root.as_deref().unwrap_or(binding.uri()))?;
    }
    let policy = match ensure_graph_with_binding(binding, cluster_root, stderr).await? {
        Some(owner) => owner.graph_policy(binding.uri()).await.map_err(report)?,
        None => None,
    };
    crate::command_outcome::writable_open();
    let storage = storage.await?;
    // The open's own error says what the open did: a typed refusal, such as
    // a root that holds no graph, precedes every write the open makes, while
    // an untyped storage failure stays one whose effects are unknown.
    let db = omnigraph::db::Omnigraph::open_with_storage(binding.uri(), storage)
        .await
        .and_then(|db| match external_blob_policy {
            Some(policy) => db.with_external_blob_policy(policy),
            None => Ok(db),
        })
        .engine_write()?;
    Ok(match policy {
        Some(policy) => db.with_policy(policy),
        None => db,
    })
}

/// Refuse an external source policy with a base over the storage root that
/// holds the graph at `binding`: the cluster's root for a cluster graph, or
/// the graph's own root for a standalone graph. `explain load csv` judges its
/// source here, as the load it explains does in [`open_writable`].
pub(crate) async fn refuse_source_over_holding_root(
    binding: &CapturedStorageBinding,
    policy: &omnigraph::ExternalBlobPolicy,
) -> Result<()> {
    let cluster_root = omnigraph_cluster::cluster_root_for_graph_binding(binding)
        .await
        .map_err(report)?;
    refuse_source_over_root(policy, cluster_root.as_deref().unwrap_or(binding.uri()))
}

/// `policy`'s refusal of a base over `root`, as the engine's typed error.
fn refuse_source_over_root(policy: &omnigraph::ExternalBlobPolicy, root: &str) -> Result<()> {
    policy
        .ensure_disjoint_from_storage_root(root)
        .map_err(omnigraph::StorageRootConflict::into_error)
        .engine_write()
}

/// Acquire before the writable opener (including its capability probe), and
/// reuse only the same canonical root's admission during this command. The
/// admission a cluster graph is under, or `None` for a standalone graph;
/// `cluster_root` is the binding's cluster root, as
/// [`omnigraph_cluster::cluster_root_for_graph_binding`] reports it.
async fn ensure_graph_with_binding(
    binding: &CapturedStorageBinding,
    cluster_root: Option<String>,
    stderr: &mut dyn Write,
) -> Result<Option<ClusterAdmission>> {
    let uri = binding.uri();
    let Some(root) = cluster_root else {
        return Ok(None);
    };
    let key = binding.for_uri(&root)?.identity();
    let owners = COMMAND.try_with(Arc::clone).unwrap_or_default();
    let mut owners = owners.lock().await;
    if let Some(owner) = owners.get(&key) {
        owner.validate_graph_uri(uri).await.map_err(report)?;
        return Ok(Some(owner.clone()));
    }
    if let Some(owner) = omnigraph_cluster::acquire_graph_admission_with_binding(
        binding,
        ClusterAdmissionPurpose::GraphOperation,
    )
    .await
    .map_err(report)?
    {
        // Report before dispatch as well as before any command-local exit or
        // lost output. Success of an arbitrary CLI command is not a qualified
        // native settlement proof; its persisted lock deliberately survives.
        let custody = format!(
            "cluster admission retained: root={} lock_id={}. After stopping the owner and establishing graph/control I/O quiescence, use exact-ID cluster force-unlock; command completion alone does not release this lock.",
            omnigraph::storage::redacted_storage_uri(owner.canonical_root()),
            owner.lock_id(),
        );
        writeln!(stderr, "{custody}")
            .and_then(|()| stderr.flush())
            .map_err(|error| color_eyre::Report::from(error).wrap_err(custody))?;
        owners.insert(key, owner.clone());
        return Ok(Some(owner));
    }
    Ok(None)
}

fn report(diagnostic: omnigraph_cluster::Diagnostic) -> color_eyre::Report {
    eyre!(
        "[{}] {}: {}",
        diagnostic.code,
        diagnostic.path,
        diagnostic.message
    )
}
