//! Writable embedded commands share one root admission per CLI invocation.
//! Read-only commands observe authority without a lock; HTTP uses its server.

use std::io::Write;
use std::{collections::BTreeMap, future::Future, sync::Arc};

use color_eyre::eyre::{Result, eyre};
use omnigraph::storage::{CapturedStorageBinding, storage_for_binding};
use omnigraph_cluster::{ClusterAdmission, ClusterAdmissionPurpose};
use tokio::sync::Mutex;

type Owners = Arc<Mutex<BTreeMap<String, ClusterAdmission>>>;

tokio::task_local! {
    static COMMAND: Owners;
}

pub(crate) async fn scope<F: Future>(future: F) -> F::Output {
    COMMAND.scope(Owners::default(), future).await
}

/// Read commands never acquire writer admission or run the writable capability
/// probe. Cluster authority is observed around the open, not held as a lease.
pub(crate) async fn open_read_only_with_binding(
    binding: &CapturedStorageBinding,
    expected_state_cas: Option<&str>,
) -> Result<omnigraph::db::Omnigraph> {
    let authority =
        omnigraph_cluster::GraphReadAuthority::capture_with_binding(binding, expected_state_cas)
            .await
            .map_err(report)?;
    let db = omnigraph::db::Omnigraph::open_read_only_with_storage(
        binding.uri(),
        storage_for_binding(binding)?,
    )
    .await?;
    if let Some(authority) = authority {
        authority.validate_opened(&db).await.map_err(report)?;
    }
    Ok(db)
}

/// Acquire before the writable opener (including its capability probe), and
/// reuse only the same canonical root's admission during this command.
pub(crate) async fn ensure_graph_with_binding(
    binding: &CapturedStorageBinding,
    stderr: &mut dyn Write,
) -> Result<()> {
    let uri = binding.uri();
    let Some(root) = omnigraph_cluster::cluster_root_for_graph_binding(binding)
        .await
        .map_err(report)?
    else {
        return Ok(());
    };
    let key = binding.for_uri(&root)?.identity();
    let owners = COMMAND.try_with(Arc::clone).unwrap_or_default();
    let mut owners = owners.lock().await;
    if let Some(owner) = owners.get(&key) {
        return owner.validate_graph_uri(uri).await.map_err(report);
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
