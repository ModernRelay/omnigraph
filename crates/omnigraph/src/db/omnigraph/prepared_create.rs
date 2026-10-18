//! Exact graph-birth authority for the cluster's durable deployment protocol.

use super::*;
use futures::FutureExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Engine-issued initialization input. Persist before invocation and never replay
/// an invocation whose outcome is unknown. The caller owns authorization,
/// exclusive writer admission, input bounds and prior-owner quiescence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedGraphCreate {
    version: u32,
    root: String,
    source: String,
    contract: SchemaContractDigest,
    genesis: GenesisManifestAttempt,
}

impl PreparedGraphCreate {
    pub fn root(&self) -> &str {
        &self.root
    }

    pub fn desired_contract(&self) -> &SchemaContractDigest {
        &self.contract
    }

    pub fn graph_commit_id(&self) -> &str {
        self.genesis.graph_commit_id()
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    /// Validate serialized inputs without touching storage or claiming authority.
    pub fn validate(&self) -> Result<()> {
        self.validated_schema_ir().map(|_| ())
    }

    pub(super) fn claim_payload(&self) -> Result<String> {
        self.validate()?;
        // A narrow extension of the existing init claim. The deployment ledger
        // already holds the full token; the claim binds its exact bytes.
        let digest = format!(
            "{:x}",
            Sha256::digest(
                serde_json::to_vec(self).map_err(|error| OmniError::manifest(error.to_string()))?
            )
        );
        Ok(serde_json::json!({ "version": 2, "prepared_digest": digest,
            "graph_commit_id": self.graph_commit_id(), "root": self.root() })
        .to_string())
    }

    pub(super) fn genesis(&self) -> &GenesisManifestAttempt {
        &self.genesis
    }

    pub(super) fn validated_schema_ir(&self) -> Result<SchemaIR> {
        let normalized = normalize_root_uri(&self.root)?;
        if self.version != 1
            || self.root != write_queue_root_identity(&normalized)?
            || self.contract.source_hash != format!("{:x}", Sha256::digest(self.source.as_bytes()))
        {
            return Err(OmniError::manifest_conflict(
                "invalid prepared graph creation input",
            ));
        }
        self.genesis
            .validate_for(omnigraph_compiler::SYSTEM_COLUMNS_V3)?;
        let domain = SchemaIdentityDomain::parse(&self.contract.schema_identity_domain)
            .map_err(|error| OmniError::manifest_conflict(error.to_string()))?;
        let schema_ir = initial_schema_ir(
            &self.source,
            omnigraph_compiler::SYSTEM_COLUMNS_V3,
            domain,
            false,
        )?;
        let contract = render_schema_contract(&schema_ir, &self.source)?;
        if contract_digest(&contract) != self.contract {
            return Err(OmniError::manifest_conflict(
                "prepared graph creation contract mismatch",
            ));
        }
        Ok(schema_ir)
    }
}

/// Read-only evidence about one exact initialization. `Absent` is only a current
/// observation: it is terminal only when the caller independently establishes
/// prior graph/control-I/O quiescence. Partial initialization and foreign graph
/// identity remain `Unknown`; neither result permits replay or cleanup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum GraphCreateReconciliation {
    Created {
        graph_manifest_version: u64,
        contract: SchemaContractDigest,
    },
    Absent,
    Unknown,
}

impl Omnigraph {
    /// Capture an exact graph birth before effects. This performs schema
    /// compilation and read-only target checks, not a writable open or probe.
    pub async fn prepare_graph_create(uri: &str, source: &str) -> Result<PreparedGraphCreate> {
        let root = write_queue_root_identity(&normalize_root_uri(uri)?)?;
        let storage = storage_for_uri(&root)?;
        preflight_init_target(&root, storage.as_ref(), InitOptions::default()).await?;
        require_empty_create_target(&root, storage.as_ref()).await?;
        if storage.exists(&init_claim_uri(&root)).await? {
            return Err(OmniError::InitializationClaimed { uri: root });
        }
        let schema_ir = initial_schema_ir(
            source,
            omnigraph_compiler::SYSTEM_COLUMNS_V3,
            SchemaIdentityDomain::from_ulid(crate::dst_ids::new_ulid()),
            false,
        )?;
        let row = render_schema_contract(&schema_ir, source)?;
        Ok(PreparedGraphCreate {
            version: 1,
            root,
            source: source.to_string(),
            contract: contract_digest(&row),
            genesis: GenesisManifestAttempt::mint(omnigraph_compiler::SYSTEM_COLUMNS_V3)?,
        })
    }

    /// Invoke one durably recorded birth exactly once. Existing roots refuse;
    /// callers reconcile an uncertain invocation instead of calling this again.
    pub async fn apply_prepared_graph_create(prepared: &PreparedGraphCreate) -> Result<Self> {
        Self::apply_prepared_graph_create_with_io_scope(prepared, None).await
    }

    /// Invoke a recorded birth using the serving owner's explicit storage lifetime.
    pub async fn apply_prepared_graph_create_with_io_scope(
        prepared: &PreparedGraphCreate,
        scope: Option<crate::storage::StorageIoScope>,
    ) -> Result<Self> {
        prepared.validate()?;
        let storage = match scope {
            Some(scope) => crate::storage::storage_for_uri_scoped(prepared.root(), scope)?,
            None => storage_for_uri(prepared.root())?,
        };
        Self::init_with_storage_for_vintage(
            prepared.root(),
            prepared.source(),
            storage,
            InitOptions::default(),
            false,
            Some(prepared),
        )
        .await
    }

    /// Settle one abandoned, unpublished birth after the caller has established
    /// exclusive writer ownership and prior native/control-I/O quiescence.
    /// This is not replay: a published graph is only observed. Unpublished
    /// cleanup requires this token's exact claim and removes only its derived
    /// empty initial tables and unpublished manifest files, never a graph root.
    /// A foreign claim, published manifest, advanced/nonempty table or an
    /// unbounded/failed inventory remains unknown. The claim is deleted last,
    /// so interrupted cleanup can be repeated under the same attestation.
    pub async fn settle_prepared_graph_create_after_quiescence(
        prepared: &PreparedGraphCreate,
    ) -> Result<GraphCreateReconciliation> {
        let observed = Self::reconcile_prepared_graph_create(prepared).await?;
        if !matches!(observed, GraphCreateReconciliation::Unknown) {
            return Ok(observed);
        }
        let schema = prepared.validated_schema_ir()?;
        let storage = crate::storage::decorate(storage_for_uri(prepared.root())?);
        let claim_uri = init_claim_uri(prepared.root());
        let expected_claim = prepared.claim_payload()?;
        if storage
            .read_text_if_exists_bounded(&claim_uri, 4096)
            .await?
            .as_deref()
            != Some(expected_claim.as_str())
        {
            return Ok(GraphCreateReconciliation::Unknown);
        }
        let manifest = crate::db::manifest::manifest_uri(prepared.root());
        if birth_has_derived_state(&manifest, storage.as_ref()).await?
            || !birth_versions(&manifest, storage.as_ref())
                .await?
                .is_empty()
        {
            return Ok(GraphCreateReconciliation::Unknown);
        }
        let mut tables = Vec::new();
        for (kind, id, incarnation) in schema
            .nodes
            .iter()
            .map(|node| {
                (
                    "node:initial",
                    node.type_id.get(),
                    node.table_incarnation_id.get(),
                )
            })
            .chain(schema.edges.iter().map(|edge| {
                (
                    "edge:initial",
                    edge.type_id.get(),
                    edge.table_incarnation_id.get(),
                )
            }))
        {
            let identity = crate::db::manifest::TableIdentity::new(id, incarnation)?;
            tables.push(join_uri(
                prepared.root(),
                &crate::db::manifest::table_path_for_identity(kind, identity)?,
            ));
        }
        if tables.len() > 4096 {
            return Ok(GraphCreateReconciliation::Unknown);
        }
        let access = crate::lance_access::LanceAccessContext::new();
        let table_store = TableStore::new(prepared.root(), access.control_session());
        // Validate the complete deletion set before removing any object.
        for table in &tables {
            bound_birth_inventory(table, storage.as_ref()).await?;
            if birth_has_derived_state(table, storage.as_ref()).await? {
                return Ok(GraphCreateReconciliation::Unknown);
            }
            let versions = birth_versions(table, storage.as_ref()).await?;
            if !versions.is_empty() {
                if versions.len() != 1 {
                    return Ok(GraphCreateReconciliation::Unknown);
                }
                let original_empty = std::panic::AssertUnwindSafe(async {
                    let dataset = table_store.open_dataset_head(table, None).await?;
                    Ok::<_, OmniError>(
                        dataset.version().version == 1
                            && dataset.count_rows(None).await.map_err(OmniError::storage)? == 0,
                    )
                })
                .catch_unwind()
                .await;
                match original_empty {
                    Ok(Ok(true)) => {}
                    Ok(Err(error)) => return Err(error),
                    Ok(Ok(false)) | Err(_) => return Ok(GraphCreateReconciliation::Unknown),
                }
            }
        }
        bound_birth_inventory(&manifest, storage.as_ref()).await?;
        // Quiescence is an external precondition, but still recheck publication
        // and exact claim immediately before the first destructive operation.
        if !birth_versions(&manifest, storage.as_ref())
            .await?
            .is_empty()
            || storage
                .read_text_if_exists_bounded(&claim_uri, 4096)
                .await?
                .as_deref()
                != Some(expected_claim.as_str())
        {
            return Ok(GraphCreateReconciliation::Unknown);
        }
        for table in tables {
            storage.delete_prefix(&table).await?;
        }
        storage.delete_prefix(&manifest).await?;
        storage.delete(&claim_uri).await?;
        Ok(GraphCreateReconciliation::Absent)
    }

    /// Inspect exact genesis and identity-bearing contract without graph writes,
    /// recovery, claim removal or source-text-only adoption.
    pub async fn reconcile_prepared_graph_create(
        prepared: &PreparedGraphCreate,
    ) -> Result<GraphCreateReconciliation> {
        let schema_ir = prepared.validated_schema_ir()?;
        let storage = crate::storage::decorate(storage_for_uri(prepared.root())?);
        if !storage
            .exists(&crate::db::manifest::manifest_uri(prepared.root()))
            .await?
        {
            return Ok(if storage.exists(&init_claim_uri(prepared.root())).await? {
                GraphCreateReconciliation::Unknown
            } else {
                GraphCreateReconciliation::Absent
            });
        }
        let access = crate::lance_access::LanceAccessContext::new();
        // This is a read-only evidence probe, not an invocation. Malformed
        // native metadata can panic in Lance's footer decoder; it supplies no
        // exact publication proof and must never reach the cleanup path.
        let coordinator =
            match std::panic::AssertUnwindSafe(GraphCoordinator::open_exact_genesis_with_storage(
                prepared.root(),
                prepared.genesis(),
                storage,
                &access.control_session(),
            ))
            .catch_unwind()
            .await
            {
                Ok(Ok(coordinator)) => coordinator,
                Ok(Err(_)) | Err(_) => return Ok(GraphCreateReconciliation::Unknown),
            };
        let contract = coordinator.read_schema_contract().await?;
        if contract_digest(&contract) != *prepared.desired_contract()
            || validate_schema_ir_against_snapshot(&schema_ir, &coordinator.snapshot()).is_err()
            || validate_schema_contract_row(&contract).is_err()
        {
            return Ok(GraphCreateReconciliation::Unknown);
        }
        Ok(GraphCreateReconciliation::Created {
            graph_manifest_version: coordinator.snapshot().graph_manifest_version(),
            contract: prepared.desired_contract().clone(),
        })
    }
}

/// The native empty-target check is bounded even for a foreign prefix. Local
/// empty directory skeletons are harmless; every existing object refuses.
pub(super) async fn require_empty_create_target(
    root: &str,
    storage: &dyn StorageAdapter,
) -> Result<()> {
    storage
        .list_dir_bounded(
            root,
            "",
            omnigraph_storage::ListDirBounds {
                max_matching_entries: 0,
                max_irrelevant_entries: 0,
                max_uri_bytes: 8192,
            },
        )
        .await?;
    Ok(())
}

async fn bound_birth_inventory(uri: &str, storage: &dyn StorageAdapter) -> Result<()> {
    storage
        .list_dir_bounded(
            uri,
            "",
            omnigraph_storage::ListDirBounds {
                max_matching_entries: 32,
                max_irrelevant_entries: 64,
                max_uri_bytes: 256 * 1024,
            },
        )
        .await?;
    Ok(())
}

async fn birth_has_derived_state(uri: &str, storage: &dyn StorageAdapter) -> Result<bool> {
    // Branch trees, refs/tags and indexes are never produced by empty graph
    // birth. Their presence revokes cleanup authority, even without main HEAD.
    for path in ["tree", "_refs", "_indices"] {
        if storage.exists(&join_uri(uri, path)).await? {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn birth_versions(uri: &str, storage: &dyn StorageAdapter) -> Result<Vec<String>> {
    // Lance 11's V1, V2 and detached manifests all live under _versions and
    // end in .manifest. Treat even an unrecognized manifest spelling as a
    // publication; do not mistake damaged evidence for absence.
    storage
        .list_dir_bounded(
            &join_uri(uri, "_versions"),
            ".manifest",
            omnigraph_storage::ListDirBounds {
                max_matching_entries: 2,
                max_irrelevant_entries: 32,
                max_uri_bytes: 64 * 1024,
            },
        )
        .await
}

fn contract_digest(row: &SchemaContractRow) -> SchemaContractDigest {
    SchemaContractDigest {
        source_hash: format!("{:x}", Sha256::digest(row.source.as_bytes())),
        schema_ir_hash: row.head.schema_ir_hash.clone(),
        schema_identity_domain: row.head.schema_identity_domain.clone(),
        schema_identity_version: row.head.schema_identity_version,
    }
}

pub(super) fn initial_schema_ir(
    source: &str,
    system_columns: SystemColumns,
    domain: SchemaIdentityDomain,
    legacy: bool,
) -> Result<SchemaIR> {
    let shape = read_schema_shape_for_vintage(source, system_columns)?;
    let resolution = if legacy {
        let empty = read_schema_shape_from_source("")?;
        let accepted = omnigraph_compiler::into_legacy_vintage(
            initialize_schema_ir(domain, &empty)
                .map_err(|error| OmniError::manifest(error.to_string()))?
                .schema_ir,
        );
        omnigraph_compiler::resolve_schema_ir(&accepted, &shape)
    } else {
        initialize_schema_ir(domain, &shape)
    }
    .map_err(|error| OmniError::manifest(error.to_string()))?;
    for diagnostic in &resolution.diagnostics {
        tracing::warn!(
            target: "omnigraph::schema::identity",
            kind = ?diagnostic.kind,
            entity = %diagnostic.entity,
            hint = %diagnostic.hint,
            "schema identity hint is inert during graph initialization"
        );
    }
    Ok(resolution.schema_ir)
}
