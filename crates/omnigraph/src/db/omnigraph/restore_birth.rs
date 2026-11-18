//! Delivery of a restored graph competes with initialization for the same
//! one-time birth claim. An ordinary graph opener cannot acquire this claim.
use super::*;

fn claim_payload(digest: &str) -> Result<String> {
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(OmniError::manifest("invalid restore descriptor digest"));
    }
    Ok(serde_json::json!({
        "version": 1, "operation": "full_history_restore", "restore_digest": digest
    })
    .to_string())
}

impl Omnigraph {
    /// Reserve an empty destination before copying any history. An uncertain
    /// request may be retried with exactly the same digest. A previously
    /// confirmed reservation is checked separately once staging starts.
    pub async fn reserve_restore_birth(
        uri: &str,
        storage: Arc<dyn StorageAdapter>,
        digest: &str,
    ) -> Result<()> {
        let root = write_queue_root_identity(&normalize_root_uri(uri)?)?;
        let payload = claim_payload(digest)?;
        let key = init_claim_uri(&root);
        if storage
            .read_text_if_exists_bounded(&key, 1024)
            .await?
            .is_none()
        {
            prepared_create::require_empty_create_target(&root, storage.as_ref()).await?;
            verify_local_create_if_absent(&root, storage.as_ref()).await?;
            storage.write_text_if_absent(&key, &payload).await?;
        }
        Self::check_restore_birth_claim(&root, storage.clone(), digest).await?;
        // A competing ordinary birth might have finished between our empty
        // check and conditional claim. Detect its objects before allowing
        // the external staging role to write anything.
        let objects = storage
            .list_dir_bounded(
                &root,
                INIT_CLAIM_FILENAME,
                omnigraph_storage::ListDirBounds {
                    max_matching_entries: 1,
                    max_irrelevant_entries: 0,
                    max_uri_bytes: 8192,
                },
            )
            .await?;
        if objects.as_slice() != [key] {
            return Err(OmniError::manifest(
                "restore reservation requires an empty graph root",
            ));
        }
        Ok(())
    }

    /// Check that only the requested restore owns this graph birth. A stale
    /// normal initializer's claim is never removed or repurposed.
    pub async fn check_restore_birth_claim(
        uri: &str,
        storage: Arc<dyn StorageAdapter>,
        digest: &str,
    ) -> Result<()> {
        let root = write_queue_root_identity(&normalize_root_uri(uri)?)?;
        let expected = claim_payload(digest)?;
        if storage
            .read_text_if_exists_bounded(&init_claim_uri(&root), 1024)
            .await?
            .as_deref()
            != Some(&expected)
        {
            return Err(OmniError::InitializationClaimed { uri: root });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn competing_restore_births_have_one_winner_and_reject_partial_payloads() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("graph.omni");
        let uri = root.to_str().unwrap();
        let adapter = storage_for_uri(uri).unwrap();
        let a = "a".repeat(64);
        let b = "b".repeat(64);
        let (left, right) = tokio::join!(
            Omnigraph::reserve_restore_birth(uri, adapter.clone(), &a),
            Omnigraph::reserve_restore_birth(uri, adapter.clone(), &b),
        );
        assert_ne!(left.is_ok(), right.is_ok(), "{left:?}, {right:?}");
        let winner = if left.is_ok() { &a } else { &b };
        Omnigraph::reserve_restore_birth(uri, adapter.clone(), winner)
            .await
            .unwrap();
        std::fs::write(root.join("unexpected"), b"foreign").unwrap();
        assert!(
            Omnigraph::reserve_restore_birth(uri, adapter.clone(), winner)
                .await
                .is_err()
        );
        Omnigraph::check_restore_birth_claim(uri, adapter, winner)
            .await
            .unwrap();
        assert_eq!(std::fs::read(root.join("unexpected")).unwrap(), b"foreign");
    }

    #[tokio::test]
    async fn preexisting_payload_is_rejected_before_reservation() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("old.omni");
        std::fs::create_dir_all(root.join("nested")).unwrap();
        std::fs::write(root.join("nested/data"), b"retained").unwrap();
        let uri = root.to_str().unwrap();
        let adapter = storage_for_uri(uri).unwrap();
        assert!(
            Omnigraph::reserve_restore_birth(uri, adapter, &"a".repeat(64))
                .await
                .is_err()
        );
        assert!(!root.join(INIT_CLAIM_FILENAME).exists());
        assert_eq!(
            std::fs::read(root.join("nested/data")).unwrap(),
            b"retained"
        );
    }
}
