//! The `__recovery/` directory of graphs written before RFC 0067.
//!
//! No writer arms a recovery sidecar any more: every table effect is a
//! detached commit published as a pin, and a staged schema contract names the
//! graph commit that publishes it. A sidecar can therefore only come from a
//! build that predates detached table commits and stopped mid-write. This
//! binary cannot interpret one, so a read-write open and the storage upgrade
//! refuse the graph until the build that wrote the sidecar has resolved it.
//! Reads stay pinned to published manifest versions and never look here.

use crate::error::{OmniError, Result};
use crate::storage::StorageAdapter;

/// Subdirectory under the graph root that held sidecar files.
pub(crate) const RECOVERY_DIR_NAME: &str = "__recovery";

fn recovery_dir_uri(root_uri: &str) -> String {
    format!("{}/{}", root_uri.trim_end_matches('/'), RECOVERY_DIR_NAME)
}

/// Operation ids of the sidecars under `__recovery/`, sorted. Empty when the
/// directory is absent or holds none, which is every graph this build wrote.
pub(crate) async fn pending_legacy_sidecars(
    root_uri: &str,
    storage: &dyn StorageAdapter,
) -> Result<Vec<String>> {
    let mut ids = storage
        .list_dir(&recovery_dir_uri(root_uri))
        .await?
        .into_iter()
        .filter_map(|uri| {
            uri.rsplit('/')
                .next()
                .and_then(|name| name.strip_suffix(".json"))
                .map(str::to_string)
        })
        .collect::<Vec<_>>();
    ids.sort();
    Ok(ids)
}

/// Refuse a graph that still carries a sidecar. `operation` names the caller
/// for the message.
pub(crate) async fn refuse_legacy_sidecars(
    root_uri: &str,
    storage: &dyn StorageAdapter,
    operation: &str,
) -> Result<()> {
    let ids = pending_legacy_sidecars(root_uri, storage).await?;
    let Some(first) = ids.first() else {
        return Ok(());
    };
    Err(OmniError::recovery_required(
        first.clone(),
        format!(
            "{operation} found {} recovery sidecar(s) under '{RECOVERY_DIR_NAME}/' ({}), written by an OmniGraph \
             build that predates detached table commits; this build cannot interpret them. Open the graph \
             read-write with the build that wrote them so it finishes its recovery, then retry",
            ids.len(),
            ids.join(", "),
        ),
    ))
}

/// Nonmutating absence proof for callers without recovery authority (the
/// cluster admission probe): every JSON object under `__recovery/` blocks,
/// including malformed ones — absence is the only provable state — and so
/// does any staged schema contract file, since only a read-write open may
/// settle one. Listing is bounded; exceeding the bound refuses rather than
/// walking an unbounded directory.
pub(crate) async fn refuse_pending_recovery(
    root_uri: &str,
    storage: &dyn StorageAdapter,
) -> Result<()> {
    let pending = storage
        .list_dir_bounded(
            &recovery_dir_uri(root_uri),
            ".json",
            crate::storage::ListDirBounds {
                max_matching_entries: 1,
                max_irrelevant_entries: 1024,
                max_uri_bytes: 131_072,
            },
        )
        .await?;
    if !pending.is_empty() {
        return Err(OmniError::recovery_required(
            "pending-recovery",
            "graph has pending recovery; resolve it with explicit recovery authority before retrying",
        ));
    }
    for staging in [
        crate::db::schema_state::schema_source_staging_uri(root_uri),
        crate::db::schema_state::schema_ir_staging_uri(root_uri),
        crate::db::schema_state::schema_state_staging_uri(root_uri),
    ] {
        if storage.exists(&staging).await? {
            return Err(OmniError::recovery_required(
                "pending-schema-recovery",
                "graph has staged schema recovery; resolve it with explicit recovery authority before retrying",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::storage_for_uri;

    #[tokio::test]
    async fn an_absent_or_empty_directory_is_clean_and_a_sidecar_refuses() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let storage = storage_for_uri(root).unwrap();
        refuse_legacy_sidecars(root, storage.as_ref(), "open")
            .await
            .unwrap();
        let recovery = dir.path().join(RECOVERY_DIR_NAME);
        std::fs::create_dir(&recovery).unwrap();
        std::fs::write(recovery.join(".DS_Store"), "noise").unwrap();
        refuse_legacy_sidecars(root, storage.as_ref(), "open")
            .await
            .unwrap();
        std::fs::write(recovery.join("01B.json"), "{}").unwrap();
        std::fs::write(recovery.join("01A.json"), "{}").unwrap();
        let error = refuse_legacy_sidecars(root, storage.as_ref(), "open")
            .await
            .unwrap_err();
        assert!(
            matches!(&error, OmniError::RecoveryRequired { operation_id, .. } if operation_id == "01A"),
            "{error}"
        );
        assert!(error.to_string().contains("01A, 01B"), "{error}");
    }
}
