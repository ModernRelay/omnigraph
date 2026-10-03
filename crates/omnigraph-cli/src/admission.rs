//! One root admission per embedded CLI invocation, shared by every graph open.
//! HTTP commands inherit their server's admission instead.

use std::{collections::BTreeMap, future::Future, sync::Arc};

use color_eyre::eyre::{Result, eyre};
use omnigraph_cluster::{ClusterAdmission, ClusterAdmissionPurpose};
use tokio::sync::Mutex;

type Owners = Arc<Mutex<BTreeMap<String, ClusterAdmission>>>;

tokio::task_local! {
    static COMMAND: Owners;
}

pub(crate) async fn scope<F: Future>(future: F) -> F::Output {
    COMMAND.scope(Owners::default(), future).await
}

/// Acquire before the writable opener (including its capability probe), and
/// reuse only the same canonical root's admission during this command.
pub(crate) async fn ensure_graph(uri: &str) -> Result<()> {
    let Some(root) = omnigraph_cluster::cluster_root_for_graph_uri(uri)
        .await
        .map_err(report)?
    else {
        return Ok(());
    };
    let root = omnigraph::storage::normalize_root_uri(&root)?;
    let key = if omnigraph::storage::storage_kind_for_uri(&root)?
        == omnigraph::storage::StorageKind::Local
    {
        std::fs::canonicalize(&root)?
            .to_str()
            .ok_or_else(|| eyre!("cluster root is not valid UTF-8"))?
            .to_owned()
    } else {
        root
    };
    let owners = COMMAND.try_with(Arc::clone).unwrap_or_default();
    let mut owners = owners.lock().await;
    if let Some(owner) = owners.get(&key) {
        return owner.validate_graph_uri(uri).await.map_err(report);
    }
    if let Some(owner) =
        omnigraph_cluster::acquire_graph_admission(uri, ClusterAdmissionPurpose::GraphOperation)
            .await
            .map_err(report)?
    {
        // Report before dispatch as well as before any command-local exit or
        // lost output. Success of an arbitrary CLI command is not a qualified
        // native settlement proof; its persisted lock deliberately survives.
        eprintln!(
            "cluster admission retained: root={} lock_id={}. After stopping the owner and establishing graph/control I/O quiescence, use exact-ID cluster force-unlock; command completion alone does not release this lock.",
            omnigraph::storage::redacted_storage_uri(owner.canonical_root()),
            owner.lock_id(),
        );
        owners.insert(key, owner);
    }
    Ok(())
}

fn report(diagnostic: omnigraph_cluster::Diagnostic) -> color_eyre::Report {
    eyre!(
        "[{}] {}: {}",
        diagnostic.code,
        diagnostic.path,
        diagnostic.message
    )
}
