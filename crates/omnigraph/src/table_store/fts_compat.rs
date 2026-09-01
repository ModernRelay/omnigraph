//! Artifact-scoped full-text analyzer compatibility (RFC 0043).
//!
//! Lance's immutable index UUID and file inventory are the storage identity;
//! neither the table's writer version nor the posting format proves which
//! stemmer built an index. This certificate is derived index state, not a
//! graph migration flag or protection against an out-of-band malicious writer.

use std::{borrow::Cow, sync::Arc};

use lance::{Dataset, index::scalar::IndexDetails};
use lance_core::cache::CacheKey;
use lance_table::format::{IndexFile, IndexMetadata};
use object_store::path::Path;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{OmniError, Result};

pub(crate) const CERTIFICATE_FILE: &str = "omnigraph_fts_compat.json";
pub(crate) const ANALYZER_GENERATION: &str = "lance11.0.0-frostem1.20260821.3-v1";
const FORMAT_VERSION: u32 = 1;
const MAX_CERTIFICATE_BYTES: u64 = 64 * 1024;
const MAX_PAYLOAD_BYTES: usize = 4096;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Certificate {
    format_version: u32,
    index_uuid: String,
    analyzer_generation: String,
    artifact_digest: String,
}

/// Positive evidence only. No codec: proof never outlives the Lance session's
/// bounded in-memory metadata cache, even with a persistent cache backend.
struct VerifiedCertificateKey(String);

impl CacheKey for VerifiedCertificateKey {
    type ValueType = bool;

    fn key(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.0)
    }

    fn type_name() -> &'static str {
        "omnigraph.verified-fts-certificate.v1"
    }
}

/// Certify a newly completed, unpublished full FTS build from source rows.
///
/// The caller must own this fresh index UUID and publish the updated metadata
/// through the existing staged CreateIndex transaction. Do not use this to
/// bless an existing index or an incremental merge/remap of unproven postings.
/// The returned file inventory participates in Lance's native index lifecycle.
pub(crate) async fn write_certificate(dataset: &Dataset, index: &mut IndexMetadata) -> Result<()> {
    let files = inventory(index)?;
    if files.iter().any(|file| file.path == CERTIFICATE_FILE) || index.base_id.is_some() {
        return Err(rebuild_required(
            index,
            "certificate requires a fresh local index artifact",
        ));
    }
    let certificate = Certificate {
        format_version: FORMAT_VERSION,
        index_uuid: index.uuid.to_string(),
        analyzer_generation: ANALYZER_GENERATION.to_owned(),
        artifact_digest: artifact_digest(index, &files)?,
    };
    let payload = serde_json::to_vec(&certificate).map_err(|error| {
        OmniError::manifest_internal(format!("encode FTS certificate: {error}"))
    })?;
    if payload.len() > MAX_PAYLOAD_BYTES {
        return Err(rebuild_required(
            index,
            "new certificate exceeds its size bound",
        ));
    }
    let path = certificate_path(dataset, index)?;
    let store = dataset
        .object_store(None)
        .await
        .map_err(OmniError::storage)?;
    let written = store.put(&path, &payload).await.map_err(|error| {
        OmniError::storage_context("write FTS compatibility certificate", error)
    })?;
    if written.size != payload.len() {
        return Err(rebuild_required(
            index,
            "certificate write reported an unexpected size",
        ));
    }
    // Do not expose a reference until the complete certificate write succeeds.
    index
        .files
        .as_mut()
        .expect("inventory validated")
        .push(IndexFile {
            path: CERTIFICATE_FILE.to_owned(),
            size_bytes: written.size as u64,
        });
    Ok(())
}

/// Verify one exact snapshot-selected FTS index segment before using it.
pub(crate) async fn verify_index(dataset: &Dataset, index: &IndexMetadata) -> Result<()> {
    let files = inventory(index)?;
    let certificate_file = files
        .iter()
        .find(|file| file.path == CERTIFICATE_FILE)
        .ok_or_else(|| rebuild_required(index, "analyzer certificate is missing"))?;
    if certificate_file.size_bytes == 0 || certificate_file.size_bytes > MAX_CERTIFICATE_BYTES {
        return Err(rebuild_required(
            index,
            "analyzer certificate has an invalid size",
        ));
    }
    let digest = artifact_digest(index, &files)?;
    let path = certificate_path(dataset, index)?;
    let store = dataset
        .object_store(index.base_id)
        .await
        .map_err(OmniError::storage)?;
    let key = verification_key(
        dataset.uri(),
        &store.store_prefix,
        &path,
        index,
        certificate_file.size_bytes,
        &digest,
    );
    let session = dataset.session();
    let cache = session.file_metadata_cache();
    if cache.get_with_key(&key).await.is_some() {
        return Ok(());
    }
    // Do not use Lance's index-file parser or open_with_size: malformed Lance
    // footers can allocate before validation, and its cached-size SmallReader
    // downloads the whole object even if the inventory understates its size.
    let reader = store
        .open(&path)
        .await
        .map_err(|error| certificate_read_error(index, error))?;
    let actual_size = reader
        .size()
        .await
        .map_err(|error| certificate_read_error(index, error.into()))?;
    if actual_size == 0
        || actual_size as u64 > MAX_CERTIFICATE_BYTES
        || actual_size as u64 != certificate_file.size_bytes
    {
        return Err(rebuild_required(
            index,
            "analyzer certificate size does not match its bounded inventory",
        ));
    }
    if actual_size > MAX_PAYLOAD_BYTES {
        return Err(rebuild_required(
            index,
            "analyzer certificate payload is oversized",
        ));
    }
    let payload = reader
        .get_range(0..actual_size)
        .await
        .map_err(|error| certificate_read_error(index, error.into()))?;
    if payload.len() != actual_size {
        return Err(rebuild_required(
            index,
            "analyzer certificate read was incomplete",
        ));
    }
    let payload = std::str::from_utf8(&payload)
        .map_err(|_| rebuild_required(index, "analyzer certificate is not UTF-8"))?;
    verify_payload(index, payload, &digest)?;
    cache.insert_with_key(&key, Arc::new(true)).await;
    Ok(())
}

fn verification_key(
    dataset_uri: &str,
    store_prefix: &str,
    path: &Path,
    index: &IndexMetadata,
    certificate_size: u64,
    artifact_digest: &str,
) -> VerifiedCertificateKey {
    let mut digest = Sha256::new();
    // A copied dataset may retain every UUID but have different certificate
    // bytes. Base-path changes and certificate-inventory changes also require
    // fresh evidence. Coverage and names remain outside immutable identity.
    for field in [
        dataset_uri,
        store_prefix,
        path.as_ref(),
        &index.uuid.to_string(),
        ANALYZER_GENERATION,
        artifact_digest,
    ] {
        hash_bytes(&mut digest, field.as_bytes());
    }
    digest.update(FORMAT_VERSION.to_le_bytes());
    digest.update(certificate_size.to_le_bytes());
    VerifiedCertificateKey(format!("{:x}", digest.finalize()))
}

fn certificate_path(dataset: &Dataset, index: &IndexMetadata) -> Result<Path> {
    // Mirror Lance 11 Dataset::indice_files_dir (private): an additional base
    // is either a dataset root or already its index directory. URI parsing,
    // credentials, wrappers, and cross-store resolution remain Lance-owned.
    // https://github.com/lance-format/lance/blob/v11.0.0/rust/lance/src/dataset.rs
    let indices_dir = match index.base_id {
        None => dataset.indices_dir(),
        Some(base_id) => {
            let base = dataset.manifest.base_paths.get(&base_id).ok_or_else(|| {
                rebuild_required(index, "index references an unknown storage base")
            })?;
            let path = base
                .extract_path(dataset.session().store_registry())
                .map_err(OmniError::storage)?;
            if base.is_dataset_root {
                path.join("_indices")
            } else {
                path
            }
        }
    };
    Ok(indices_dir
        .join(index.uuid.to_string())
        .join(CERTIFICATE_FILE))
}

fn certificate_read_error(index: &IndexMetadata, error: lance::Error) -> OmniError {
    if error.is_not_found() {
        rebuild_required(index, "referenced analyzer certificate is missing")
    } else {
        OmniError::storage_context("read FTS compatibility certificate", error)
    }
}

fn verify_payload(index: &IndexMetadata, payload: &str, digest: &str) -> Result<()> {
    let certificate: Certificate = serde_json::from_str(payload)
        .map_err(|_| rebuild_required(index, "analyzer certificate payload is malformed"))?;
    if certificate.format_version != FORMAT_VERSION {
        return Err(rebuild_required(
            index,
            "analyzer certificate format is unsupported",
        ));
    }
    if certificate.index_uuid != index.uuid.to_string() {
        return Err(rebuild_required(
            index,
            "analyzer certificate belongs to a different index UUID",
        ));
    }
    if certificate.analyzer_generation != ANALYZER_GENERATION {
        return Err(rebuild_required(
            index,
            "index analyzer generation differs from this engine",
        ));
    }
    if certificate.artifact_digest != digest {
        return Err(rebuild_required(
            index,
            "analyzer certificate does not match the index artifact",
        ));
    }
    Ok(())
}

fn inventory(index: &IndexMetadata) -> Result<Vec<&IndexFile>> {
    let mut files: Vec<_> = index
        .files
        .as_ref()
        .ok_or_else(|| rebuild_required(index, "index file inventory is missing"))?
        .iter()
        .collect();
    files.sort_unstable_by(|left, right| left.path.cmp(&right.path));
    if files
        .iter()
        .any(|file| file.path.is_empty() || file.size_bytes == 0)
        || files.windows(2).any(|pair| pair[0].path == pair[1].path)
        || !files.iter().any(|file| file.path != CERTIFICATE_FILE)
    {
        return Err(rebuild_required(
            index,
            "index file inventory is incomplete or ambiguous",
        ));
    }
    Ok(files)
}

fn artifact_digest(index: &IndexMetadata, files: &[&IndexFile]) -> Result<String> {
    let details = index
        .index_details
        .as_ref()
        .filter(|details| IndexDetails(Arc::clone(details)).supports_fts())
        .ok_or_else(|| {
            rebuild_required(index, "full-text index details are missing or unsupported")
        })?;
    let mut digest = Sha256::new();
    digest.update(b"omnigraph.fts-artifact.v1\0");
    digest.update(index.index_version.to_le_bytes());
    hash_bytes(&mut digest, details.type_url.as_bytes());
    hash_bytes(&mut digest, &details.value);
    for file in files.iter().filter(|file| file.path != CERTIFICATE_FILE) {
        hash_bytes(&mut digest, file.path.as_bytes());
        digest.update(file.size_bytes.to_le_bytes());
    }
    // Names, table versions, fragment coverage, and base paths can change
    // without changing the immutable postings. They are not analyzer identity.
    Ok(format!("{:x}", digest.finalize()))
}

fn hash_bytes(digest: &mut Sha256, bytes: &[u8]) {
    digest.update((bytes.len() as u64).to_le_bytes());
    digest.update(bytes);
}

fn rebuild_required(index: &IndexMetadata, reason: &str) -> OmniError {
    OmniError::FullTextIndexRebuildRequired {
        index: index.name.clone(),
        reason: format!("{reason} (index UUID {})", index.uuid),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn analyzer_generation_requires_the_audited_resolved_dependency_set() {
        // An audited, behavior-compatible dependency change may keep this
        // generation. Updating this association requires the RFC 0043 audit;
        // neither a manifest edit nor a lockfile refresh grants that approval.
        const AUDITED_GENERATION: &str = "lance11.0.0-frostem1.20260821.3-v1";
        const AUDITED_VERSIONS: &[(&str, &str)] = &[
            ("lance", "11.0.0"),
            ("lance-index", "11.0.0"),
            ("lance-tokenizer", "11.0.0"),
            ("frostem", "1.20260821.3"),
            ("stop-words", "0.10.0"),
            ("unicode-normalization", "0.1.25"),
        ];
        let workspace: toml::Value =
            toml::from_str(include_str!("../../../../Cargo.toml")).unwrap();
        let engine: toml::Value = toml::from_str(include_str!("../../Cargo.toml")).unwrap();
        let lock: toml::Value = toml::from_str(include_str!("../../../../Cargo.lock")).unwrap();
        assert_eq!(ANALYZER_GENERATION, AUDITED_GENERATION);
        for &(name, version) in AUDITED_VERSIONS {
            let dependency = &workspace["workspace"]["dependencies"][name];
            let pin = dependency
                .as_str()
                .or_else(|| dependency.get("version").and_then(toml::Value::as_str));
            assert_eq!(
                pin,
                Some(format!("={version}").as_str()),
                "{name} pin needs an analyzer audit"
            );
            assert_eq!(
                engine["dependencies"][name]["workspace"].as_bool(),
                Some(true),
                "{name} must remain a direct inherited dependency"
            );
            let resolved: Vec<_> = lock["package"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|package| package["name"].as_str() == Some(name))
                .map(|package| package["version"].as_str().unwrap())
                .collect();
            assert_eq!(
                resolved,
                vec![version],
                "{name} resolved versions need an analyzer audit"
            );
        }
    }

    #[tokio::test]
    async fn certificate_file_is_bounded_and_follows_shallow_clone_ownership() {
        use arrow_array::{RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema};
        use lance::{
            dataset::{DEFAULT_INDEX_CACHE_SIZE, DEFAULT_METADATA_CACHE_SIZE},
            index::DatasetIndexExt,
            io::ObjectStoreRegistry,
            session::Session,
        };
        use object_store::ObjectStoreExt;

        use crate::{storage_layer::IndexBuildSpec, table_store::TableStore};

        async fn assert_native_siblings(dataset: &Dataset, index: &IndexMetadata) {
            let parent = certificate_path(dataset, index).unwrap().parent().unwrap();
            let store = dataset.object_store(index.base_id).await.unwrap();
            for file in index.files.as_ref().unwrap() {
                let meta = store
                    .inner
                    .head(&parent.clone().join(file.path.as_str()))
                    .await
                    .unwrap();
                assert_eq!(meta.size, file.size_bytes, "native sibling {}", file.path);
            }
        }

        let directory = tempfile::tempdir().unwrap();
        let uri = directory.path().join("source.lance");
        let uri = uri.to_str().unwrap();
        // Local-file clients (and their counters) are pooled by registry, not
        // dataset path. Isolate I/O accounting while retaining normal caches.
        let session = Arc::new(Session::new(
            DEFAULT_INDEX_CACHE_SIZE,
            DEFAULT_METADATA_CACHE_SIZE,
            Arc::new(ObjectStoreRegistry::default()),
        ));
        let store = TableStore::new(uri, session);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, false)])),
            vec![Arc::new(StringArray::from(vec!["organism"]))],
        )
        .unwrap();
        TableStore::write_dataset(uri, batch).await.unwrap();
        // The low-level bootstrap writer intentionally returns a zero-cache
        // control session. Read through the engine's normal data-table opener
        // and graph-scoped data session before testing warm query behavior.
        let dataset = store.open_dataset_head(uri, None).await.unwrap();
        let staged = store
            .stage_create_indices(
                &dataset,
                &[IndexBuildSpec::FullText {
                    column: "body".into(),
                }],
            )
            .await
            .unwrap();
        let (mut dataset, _) = store
            .commit_staged_exact(Arc::new(dataset), staged)
            .await
            .unwrap();
        let index = dataset.load_indices().await.unwrap()[0].clone();
        // Lance wrote the native files independently of certificate_path. A
        // wrong mirror must not pass just because our writer and reader agree.
        assert_native_siblings(&dataset, &index).await;
        let object_store = dataset.object_store(None).await.unwrap();
        let path = certificate_path(&dataset, &index).unwrap();
        let pooled_reader = TableStore::new(
            uri,
            crate::lance_access::LanceAccessContext::new().data_session(),
        )
        .open_dataset_head(uri, None)
        .await
        .unwrap();
        let pooled_store = pooled_reader.object_store(None).await.unwrap();
        assert!(!Arc::ptr_eq(&object_store, &pooled_store));
        object_store.io_stats_incremental();
        verify_index(&dataset, &index).await.unwrap();
        assert!(object_store.io_stats_incremental().read_iops > 0);
        verify_index(&dataset, &index).await.unwrap();
        // Another graph's pooled client must not contaminate these counters.
        // Keep this deterministic instead of relying on parallel test timing.
        pooled_store.read_one_all(&path).await.unwrap();
        let warm = object_store.io_stats_incremental();
        assert_eq!((warm.read_iops, warm.write_iops), (0, 0));

        // Native local read_iops excludes file metadata/open calls. Lance's
        // test scheme uses CloudObjectReader over the same files, exposing
        // both the HEAD and payload GET through its existing object-store tracker.
        let file_uri = url::Url::from_file_path(uri).unwrap().to_string();
        // forbidden-api-allow: test-only native reader seam to count certificate HEAD and GET separately.
        let cloud_reader = Dataset::open(&file_uri.replacen("file:", "file-object-store:", 1))
            .await
            .unwrap();
        let tracked_store = cloud_reader.object_store(None).await.unwrap();
        assert!(!tracked_store.has_direct_local_paths());
        tracked_store.io_stats_incremental();
        verify_index(&cloud_reader, &index).await.unwrap();
        let cold = tracked_store.io_stats_incremental();
        let methods: Vec<_> = cold.requests.iter().map(|request| request.method).collect();
        assert_eq!(
            methods,
            vec!["get_opts", "get_ranges"],
            "HEAD then payload GET"
        );
        verify_index(&cloud_reader, &index).await.unwrap();
        assert!(tracked_store.io_stats_incremental().requests.is_empty());

        // A changed certificate inventory or artifact cannot borrow the warm
        // proof even when its immutable UUID is unchanged.
        for changed_certificate in [false, true] {
            let mut changed = index.clone();
            changed
                .files
                .as_mut()
                .unwrap()
                .iter_mut()
                .find(|file| (file.path == CERTIFICATE_FILE) == changed_certificate)
                .unwrap()
                .size_bytes += 1;
            assert!(matches!(
                verify_index(&dataset, &changed).await,
                Err(OmniError::FullTextIndexRebuildRequired { .. })
            ));
        }

        let original = object_store.read_one_all(&path).await.unwrap();

        let clone_uri = directory.path().join("clone.lance");
        let version = dataset.version().version;
        dataset
            .shallow_clone(clone_uri.to_str().unwrap(), version, None)
            .await
            .unwrap();
        let mut cloned =
            // forbidden-api-allow: test-only same-session clone proves certificate ownership cannot alias by UUID.
            lance::dataset::builder::DatasetBuilder::from_uri(clone_uri.to_str().unwrap())
                .with_session(dataset.session())
                .load()
                .await
                .unwrap();
        let clone_index = cloned.load_indices().await.unwrap()[0].clone();
        assert_eq!(clone_index.uuid, index.uuid);
        assert!(Arc::ptr_eq(&cloned.session(), &dataset.session()));
        let base_id = clone_index
            .base_id
            .expect("shallow clone must reference its source base");
        assert!(
            !clone_uri
                .join("_indices")
                .join(index.uuid.to_string())
                .join(CERTIFICATE_FILE)
                .exists()
        );
        // A different dataset URI sharing the same session and index UUID
        // must acquire its own proof. Failures must not be cached either.
        object_store.inner.delete(&path).await.unwrap();
        assert!(matches!(
            verify_index(&cloned, &clone_index).await,
            Err(OmniError::FullTextIndexRebuildRequired { .. })
        ));
        object_store.put(&path, &original).await.unwrap();
        verify_index(&cloned, &clone_index).await.unwrap();
        assert_native_siblings(&cloned, &clone_index).await;
        // Both public BasePath shapes must resolve the same immutable file.
        let base = Arc::make_mut(&mut cloned.manifest)
            .base_paths
            .get_mut(&base_id)
            .unwrap();
        assert!(base.is_dataset_root);
        base.path.push_str("/_indices");
        base.is_dataset_root = false;
        verify_index(&cloned, &clone_index).await.unwrap();
        assert_native_siblings(&cloned, &clone_index).await;

        for (case, payload) in [
            ("truncated", original[..original.len() - 1].to_vec()),
            (
                "oversized despite small inventory",
                vec![b' '; MAX_CERTIFICATE_BYTES as usize + 1],
            ),
            (
                "invalid JSON at exact recorded size",
                vec![b'x'; original.len()],
            ),
            (
                "invalid UTF-8 at exact recorded size",
                vec![0xff; original.len()],
            ),
        ] {
            object_store.put(&path, &payload).await.unwrap();
            // These deliberate out-of-band rewrites violate immutable-file
            // ownership. Reopening models their first observation by a reader.
            // forbidden-api-allow: test-only cold reader must revalidate deliberately corrupted certificate bytes.
            let cold = Dataset::open(uri).await.unwrap();
            assert!(
                matches!(
                    verify_index(&cold, &index).await,
                    Err(OmniError::FullTextIndexRebuildRequired { .. })
                ),
                "accepted {case}"
            );
            object_store.put(&path, &original).await.unwrap();
            verify_index(&cold, &index).await.unwrap();
        }
        object_store.put(&path, &original).await.unwrap();
        verify_index(&dataset, &index).await.unwrap();

        let mut missing_reference = index.clone();
        missing_reference
            .files
            .as_mut()
            .unwrap()
            .retain(|file| file.path != CERTIFICATE_FILE);
        assert!(matches!(
            verify_index(&dataset, &missing_reference).await,
            Err(OmniError::FullTextIndexRebuildRequired { .. })
        ));
        object_store.inner.delete(&path).await.unwrap();
        // forbidden-api-allow: test-only cold reader must observe deliberate certificate deletion.
        let cold = Dataset::open(uri).await.unwrap();
        assert!(matches!(
            verify_index(&cold, &index).await,
            Err(OmniError::FullTextIndexRebuildRequired { .. })
        ));
        // forbidden-api-allow: test-only cold clone must observe deletion at its source-owned certificate path.
        let cold_clone = Dataset::open(clone_uri.to_str().unwrap()).await.unwrap();
        assert!(matches!(
            verify_index(&cold_clone, &clone_index).await,
            Err(OmniError::FullTextIndexRebuildRequired { .. })
        ));
    }

    fn index() -> IndexMetadata {
        let mut index = IndexMetadata {
            uuid: "11111111-1111-4111-8111-111111111111".parse().unwrap(),
            fields: vec![1],
            covering_fields: vec![],
            name: "body_fts".into(),
            dataset_version: 1,
            fragment_bitmap: None,
            index_details: Some(Arc::new(Default::default())),
            index_version: 2,
            created_at: None,
            base_id: None,
            files: Some(vec![
                IndexFile {
                    path: "part_0_tokens.lance".into(),
                    size_bytes: 256,
                },
                IndexFile {
                    path: "metadata.lance".into(),
                    size_bytes: 128,
                },
            ]),
        };
        Arc::get_mut(index.index_details.as_mut().unwrap())
            .unwrap()
            .type_url = "type.googleapis.com/lance.index.pb.InvertedIndexDetails".into();
        index
    }

    fn digest(index: &IndexMetadata) -> String {
        artifact_digest(index, &inventory(index).unwrap()).unwrap()
    }

    #[test]
    fn artifact_proof_tracks_immutable_details_not_coverage_or_location() {
        let original = index();
        let expected = digest(&original);
        let path = Path::from("source/_indices/certificate.json");
        let key = verification_key("source", "store-a", &path, &original, 512, &expected).0;
        for (uri, store, path, certificate_size, artifact) in [
            ("copy", "store-a", path.clone(), 512, expected.as_str()),
            ("source", "store-b", path.clone(), 512, expected.as_str()),
            (
                "source",
                "store-a",
                Path::from("other-base/certificate.json"),
                512,
                expected.as_str(),
            ),
            ("source", "store-a", path.clone(), 513, expected.as_str()),
            ("source", "store-a", path.clone(), 512, "changed-artifact"),
        ] {
            assert_ne!(
                verification_key(uri, store, &path, &original, certificate_size, artifact).0,
                key
            );
        }
        let mut other_uuid = original.clone();
        other_uuid.uuid = "22222222-2222-4222-8222-222222222222".parse().unwrap();
        assert_ne!(
            verification_key("source", "store-a", &path, &other_uuid, 512, &expected).0,
            key
        );
        let mut changed = original.clone();
        changed.name = "renamed".into();
        changed.dataset_version = 20;
        changed.fragment_bitmap = Some([42].into_iter().collect());
        changed.base_id = Some(3);
        changed.files.as_mut().unwrap().reverse();
        changed.files.as_mut().unwrap().push(IndexFile {
            path: CERTIFICATE_FILE.into(),
            size_bytes: 512,
        });
        assert_eq!(digest(&changed), expected);

        changed.files.as_mut().unwrap()[0].size_bytes += 1;
        assert_ne!(digest(&changed), expected);
        let mut changed = original.clone();
        Arc::make_mut(changed.index_details.as_mut().unwrap())
            .value
            .push(1);
        assert_ne!(digest(&changed), expected);
        let mut changed = original;
        changed.index_version += 1;
        assert_ne!(digest(&changed), expected);
    }

    #[test]
    fn certificate_requires_exact_uuid_generation_artifact_and_known_format() {
        let index = index();
        let digest = digest(&index);
        let valid = serde_json::to_value(Certificate {
            format_version: FORMAT_VERSION,
            index_uuid: index.uuid.to_string(),
            analyzer_generation: ANALYZER_GENERATION.into(),
            artifact_digest: digest.clone(),
        })
        .unwrap();
        verify_payload(&index, &valid.to_string(), &digest).unwrap();
        for (key, value) in [
            ("format_version", serde_json::json!(2)),
            ("index_uuid", serde_json::json!("another UUID")),
            ("analyzer_generation", serde_json::json!("another analyzer")),
            ("artifact_digest", serde_json::json!("another artifact")),
            ("unknown_field", serde_json::json!(true)),
        ] {
            let mut changed = valid.clone();
            changed[key] = value;
            assert!(
                matches!(
                    verify_payload(&index, &changed.to_string(), &digest),
                    Err(OmniError::FullTextIndexRebuildRequired { .. })
                ),
                "accepted changed {key}"
            );
        }
        for malformed in ["{", "null", "{}"] {
            assert!(matches!(
                verify_payload(&index, malformed, &digest),
                Err(OmniError::FullTextIndexRebuildRequired { .. })
            ));
        }
    }

    #[test]
    fn incomplete_or_ambiguous_inventory_cannot_certify_an_index() {
        let original = index();
        for files in [
            None,
            Some(vec![]),
            Some(vec![IndexFile {
                path: CERTIFICATE_FILE.into(),
                size_bytes: 512,
            }]),
            Some(vec![IndexFile {
                path: "metadata.lance".into(),
                size_bytes: 0,
            }]),
            Some(vec![original.files.as_ref().unwrap()[0].clone(); 2]),
        ] {
            let mut changed = original.clone();
            changed.files = files;
            assert!(matches!(
                inventory(&changed),
                Err(OmniError::FullTextIndexRebuildRequired { .. })
            ));
        }
        let mut changed = original;
        changed.index_details = None;
        assert!(matches!(
            artifact_digest(&changed, &inventory(&changed).unwrap()),
            Err(OmniError::FullTextIndexRebuildRequired { .. })
        ));
    }
}
