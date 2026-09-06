//! RFC 0054 actor participants in the existing graph publication protocol.
//!
//! This module prepares one bounded row from the accepted schema and pinned
//! branch. It never publishes, resolves credentials, or performs a second write.

use arrow_array::RecordBatch;
use datafusion::prelude::{ident, lit};
use futures::TryStreamExt;
use omnigraph_compiler::catalog::Catalog;

use crate::db::{MutationOpKind, Omnigraph, Snapshot, WriteTxn};
use crate::error::{OmniError, Result};

use super::staging::{MutationStaging, PendingMode};

pub(crate) const MAX_ACTOR_ID_BYTES: usize = 1024;

pub(crate) fn validate_actor_id(actor: Option<&str>) -> Result<()> {
    if actor.is_some_and(|value| {
        value.is_empty()
            || value.len() > MAX_ACTOR_ID_BYTES
            || value.bytes().any(|byte| byte.is_ascii_control())
    }) {
        return Err(OmniError::manifest(
            "actor_provenance_invalid_identity: actor must contain 1–1024 UTF-8 bytes without ASCII control characters",
        ));
    }
    Ok(())
}

/// Protection follows accepted identity, including while creation is disabled.
pub(crate) fn refuse_actor_table_write(catalog: &Catalog, type_name: &str) -> Result<()> {
    if catalog.is_protected_actor_type(type_name) {
        return Err(OmniError::manifest(
            "actor_provenance_protected: actor nodes are maintained by the graph write protocol",
        ));
    }
    Ok(())
}

/// Returns a row only when enabled, attributed and absent from this exact view.
/// Callers invoke it only after establishing that their operation changes data.
pub(crate) async fn prepare_actor_batch(
    snapshot: &Snapshot,
    catalog: &Catalog,
    actor: Option<&str>,
) -> Result<Option<(String, RecordBatch)>> {
    let Some(binding) = catalog.actor_provenance().filter(|binding| binding.enabled) else {
        return Ok(None);
    };
    validate_actor_id(actor)?;
    let Some(actor) = actor else {
        return Ok(None);
    };
    let schema = catalog.bound_schema_ir().ok_or_else(|| {
        OmniError::manifest_internal("actor provenance requires an accepted schema")
    })?;
    let node = schema
        .nodes
        .iter()
        .find(|node| {
            node.type_id == binding.type_id
                && node.table_incarnation_id == binding.table_incarnation_id
        })
        .ok_or_else(|| OmniError::manifest_internal("actor provenance binding has no node"))?;
    let property = node
        .properties
        .iter()
        .find(|property| property.property_id == binding.actor_id_property_id)
        .ok_or_else(|| OmniError::manifest_internal("actor provenance binding has no property"))?;
    let table_key = format!("node:{}", node.name);
    let dataset = snapshot.open_dataset(&table_key).await?;
    let mut scan = dataset.scan();
    scan.project(&[property.name.as_str()])?;
    scan.filter_expr(ident(&property.name).eq(lit(actor)));
    scan.limit(Some(2), None)?;
    scan.batch_size(2).batch_size_bytes(4096);
    let mut stream = scan.try_into_stream().await?;
    let mut found = 0;
    while let Some(batch) = stream.try_next().await.map_err(OmniError::storage)? {
        found += batch.num_rows();
    }
    match found {
        0 => {
            let row = serde_json::Value::Object(serde_json::Map::from_iter([(
                property.name.clone(),
                serde_json::Value::String(actor.to_string()),
            )]));
            let batch = crate::loader::normalize_strict_json_rows(catalog, &table_key, &[row])?;
            Ok(Some((table_key, batch)))
        }
        1 => Ok(None),
        _ => Err(OmniError::manifest_internal(
            "actor provenance contains duplicate identities",
        )),
    }
}

impl Omnigraph {
    pub(crate) async fn stage_actor_provenance(
        &self,
        txn: &WriteTxn,
        staging: &mut MutationStaging,
        actor: Option<&str>,
    ) -> Result<Option<String>> {
        let Some((table_key, batch)) = prepare_actor_batch(&txn.base, &txn.catalog, actor).await?
        else {
            return Ok(None);
        };
        let opened = self
            .open_for_mutation_on_branch(
                txn.branch.as_deref(),
                &table_key,
                MutationOpKind::Insert,
                Some(txn),
            )
            .await?;
        staging.ensure_path(
            &table_key,
            opened.identity,
            opened.full_path,
            opened.table_branch,
            opened.deferred_fork,
            opened.expected_version,
            MutationOpKind::Insert,
        )?;
        // The lookup was pinned to this transaction. Strict insertion plus the
        // complete read-set revalidation makes concurrent first use retry from
        // the newly published view rather than silently overwrite an identity.
        staging.append_batch(&table_key, batch.schema(), PendingMode::StrictInsert, batch)?;
        Ok(Some(table_key))
    }
}
