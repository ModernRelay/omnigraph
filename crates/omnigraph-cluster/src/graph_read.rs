//! Read-only observations of cluster membership and achieved schema authority.
//!
//! This is not writer admission or a reclamation lease. Capture and validate
//! around a read-only open; subsequent reads keep the engine's snapshot rules.

use omnigraph::db::{Omnigraph, SchemaContractDigest};

use crate::admission::{admitted_graph_id, canonical_graph_uri};
use crate::store::ClusterStore;
use crate::{CLUSTER_STATE_FILE, Diagnostic};

/// One observed ledger revision paired with its exact achieved schema.
/// No lock is acquired, released or interpreted as permission to write.
#[derive(Debug)]
pub struct GraphReadAuthority {
    store: ClusterStore,
    graph_uri: String,
    state_cas: String,
    expected_contract: SchemaContractDigest,
}

impl GraphReadAuthority {
    /// Capture before opening a graph for a genuinely read-only operation.
    /// A stored-query consumer supplies the CAS of its captured query registry.
    /// Standalone reads retain their existing open contract; a v1 cluster must
    /// complete explicit stopped-writer ledger conversion first.
    pub async fn capture(
        graph_uri: &str,
        expected_state_cas: Option<&str>,
    ) -> Result<Option<Self>, Diagnostic> {
        let Some(root) = crate::serve::cluster_root_for_graph_uri(graph_uri).await? else {
            return if expected_state_cas.is_some() {
                Err(revision_changed())
            } else {
                Ok(None)
            };
        };
        let store = ClusterStore::for_storage_root(&root)?;
        let snapshot = store.read_state(&mut store.observations()).await?;
        let state = snapshot.state.ok_or_else(revision_changed)?;
        let state_cas = snapshot.state_cas.ok_or_else(revision_changed)?;
        if expected_state_cas.is_some_and(|expected| expected != state_cas) {
            return Err(revision_changed());
        }
        if state.version != 2 {
            return Err(Diagnostic::error(
                "ledger_upgrade_required",
                CLUSTER_STATE_FILE,
                "convert the stopped cluster ledger to v2 before opening its graphs",
            ));
        }
        if state.outstanding_deployment_id().is_some() {
            return Err(Diagnostic::error(
                "cluster_deployment_outstanding",
                CLUSTER_STATE_FILE,
                "read-only graph opening is unavailable while a deployment is outstanding; reconcile that deployment first",
            ));
        }
        let contracts = state
            .applied_revision
            .schema_contracts
            .expect("validated v2 ledger has exact achieved contracts");
        let graph_id = admitted_graph_id(&store.canonical_root()?, &contracts, graph_uri)?;
        let expected_contract = contracts[&graph_id].clone();
        Ok(Some(Self {
            store,
            graph_uri: canonical_graph_uri(graph_uri)?,
            state_cas,
            expected_contract,
        }))
    }

    /// Validate the opened manifest contract against the unchanged captured
    /// ledger. This performs only bounded control reads, never a writable open.
    /// It does not fence a later deployment or promise native-I/O settlement.
    pub async fn validate_opened(&self, db: &Omnigraph) -> Result<(), Diagnostic> {
        if canonical_graph_uri(db.uri())? != self.graph_uri {
            return Err(Diagnostic::error(
                "cluster_graph_root_mismatch",
                CLUSTER_STATE_FILE,
                "opened graph differs from the captured read-only graph root",
            ));
        }
        let current = self
            .store
            .read_state(&mut self.store.observations())
            .await?;
        if current.state_cas.as_deref() != Some(self.state_cas.as_str()) {
            return Err(revision_changed());
        }
        if self.expected_contract != db.schema_contract_digest() {
            return Err(Diagnostic::error(
                "applied_schema_drift",
                CLUSTER_STATE_FILE,
                "opened graph schema differs from the cluster's achieved contract; inspect and correct the deployment before reading",
            ));
        }
        Ok(())
    }
}

fn revision_changed() -> Diagnostic {
    Diagnostic::error(
        "cluster_read_revision_changed",
        CLUSTER_STATE_FILE,
        "cluster revision changed while capturing a read-only graph; retry from a fresh cluster snapshot",
    )
}
