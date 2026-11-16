//! Artifact-scoped full-text analyzer compatibility (RFC 0043).
//!
//! Lance's immutable index UUID and file inventory are the storage identity;
//! neither the table's writer version nor the posting format proves which
//! stemmer built an index. This certificate is derived index state, not a
//! graph migration flag or protection against an out-of-band malicious writer.

use std::{borrow::Cow, collections::BTreeMap, sync::Arc};

use lance::{Dataset, index::scalar::IndexDetails};
use lance_core::cache::CacheKey;
use lance_table::format::{IndexFile, IndexMetadata};
use object_store::path::Path;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{OmniError, Result};

pub const CERTIFICATE_FILE: &str = "omnigraph_fts_compat.json";
// This names analyzer semantics, not the running Lance release. The audited
// Lance 13 default analyzer remains compatible with certificates written on 11.
pub const ANALYZER_GENERATION: &str = "lance11.0.0-frostem1.20260821.3-v1";
/// A certificate of Lance's default analysis, written before every
/// full-text field had a profile: the analyzer generation alone.
const FORMAT_VERSION: u32 = 1;
/// A certificate of an analyzer profile: the generation, the field's
/// analyzer fingerprint and the native pipeline the postings were built with.
const FORMAT_VERSION_FINGERPRINT: u32 = 2;
pub const MAX_CERTIFICATE_BYTES: u64 = 64 * 1024;
const MAX_PAYLOAD_BYTES: usize = 4096;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Certificate {
    format_version: u32,
    index_uuid: String,
    analyzer_generation: String,
    artifact_digest: String,
    /// The analyzer fingerprint of the profile the field declared when the
    /// index was built (format 2); absent for Lance's default analysis
    /// (format 1).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    analyzer_fingerprint: Option<String>,
    /// The pipeline Lance built the postings with (format 2), which is not
    /// the profile the fingerprint names: Lance has no Unicode normalization
    /// step, so a profile's NFC is not applied to the postings.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    native: Option<NativeBuild>,
}

/// What a full-text index's postings were built with, as built: recorded in
/// its certificate beside the field's analyzer fingerprint, never in the
/// fingerprint (the GQ 3 RFC, section 4: whether an index
/// stores term positions is a build option of the index). A native path that
/// reads the postings is qualified against these facts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeBuild {
    /// The Unicode normalization applied before tokenization: `none`.
    pub normalization: NativeNormalization,
    /// Lance's posting block size.
    pub block_size: u32,
    /// Lance's inverted-list format version.
    pub posting_format: u32,
    /// Whether the postings store term positions.
    pub positions: bool,
}

/// The Unicode normalization a native full-text build applies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeNormalization {
    None,
}

/// The analyzer fingerprint each full-text column's accepted schema declares
/// (`@index(fulltext, analyzer = ...)`, the GQ 3 RFC, section
/// 4). The accepted schema chooses the analysis; an index proves it only by a
/// certificate naming the same fingerprint, so a certificate of another
/// analysis refuses. A column absent here is read by a caller that holds no
/// schema (a raw scan filter): any certified analysis is accepted there.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeclaredAnalyzers(BTreeMap<String, String>);

impl DeclaredAnalyzers {
    pub fn new(fingerprints: impl IntoIterator<Item = (String, String)>) -> Self {
        Self(fingerprints.into_iter().collect())
    }

    /// The fingerprint `column` declares, `None` when the caller holds none.
    pub fn fingerprint(&self, column: &str) -> Option<&str> {
        self.0.get(column).map(String::as_str)
    }
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

/// Certify a newly completed, unpublished FTS segment the engine's builder
/// wrote from source rows: all of them, or none (an untrained declaration).
///
/// The caller must own this fresh index UUID and publish the updated metadata
/// through the existing staged CreateIndex transaction. Do not use this to
/// bless an existing index or an incremental merge/remap of unproven postings.
/// The returned file inventory participates in Lance's native index lifecycle.
///
/// `fingerprint` is the analyzer fingerprint of the profile the segment was
/// built with, `native` the pipeline Lance built its postings with.
pub async fn write_certificate(
    dataset: &Dataset,
    index: &mut IndexMetadata,
    fingerprint: &str,
    native: NativeBuild,
) -> Result<()> {
    let files = inventory(index)?;
    if files.iter().any(|file| file.path == CERTIFICATE_FILE) || index.base_id.is_some() {
        return Err(rebuild_required(
            index,
            "certificate requires a fresh local index artifact",
        ));
    }
    let certificate = Certificate {
        format_version: FORMAT_VERSION_FINGERPRINT,
        index_uuid: index.uuid.to_string(),
        analyzer_generation: ANALYZER_GENERATION.to_owned(),
        artifact_digest: artifact_digest(index, &files)?,
        analyzer_fingerprint: Some(fingerprint.to_owned()),
        native: Some(native),
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

/// Verify one exact snapshot-selected FTS index segment before using it:
/// its certificate must name this engine's analyzer generation and, when
/// `expected` is the analyzer fingerprint the field's accepted schema
/// declares, that fingerprint. With `expected` `None`, a caller that holds no
/// schema, the certificate may name any analysis.
pub async fn verify_index(
    dataset: &Dataset,
    index: &IndexMetadata,
    expected: Option<&str>,
) -> Result<()> {
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
        expected,
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
    verify_payload(index, payload, &digest, expected)?;
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
    expected: Option<&str>,
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
    // The proof is of one expectation: a field that declares another
    // analysis must read the certificate again and refuse.
    match expected {
        None => digest.update(b"any"),
        Some(fingerprint) => {
            digest.update(FORMAT_VERSION_FINGERPRINT.to_le_bytes());
            hash_bytes(&mut digest, fingerprint.as_bytes());
        }
    }
    digest.update(certificate_size.to_le_bytes());
    VerifiedCertificateKey(format!("{:x}", digest.finalize()))
}

pub fn certificate_path(dataset: &Dataset, index: &IndexMetadata) -> Result<Path> {
    // Mirror Lance 13 Dataset::indice_files_dir (private): an additional base
    // is either a dataset root or already its index directory. URI parsing,
    // credentials, wrappers, and cross-store resolution remain Lance-owned.
    // https://github.com/lance-format/lance/blob/v13.0.0/rust/lance/src/dataset.rs#L2569
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

fn verify_payload(
    index: &IndexMetadata,
    payload: &str,
    digest: &str,
    expected: Option<&str>,
) -> Result<()> {
    let certificate: Certificate = serde_json::from_str(payload)
        .map_err(|_| rebuild_required(index, "analyzer certificate payload is malformed"))?;
    match (
        certificate.format_version,
        &certificate.analyzer_fingerprint,
        &certificate.native,
    ) {
        (FORMAT_VERSION, None, None) | (FORMAT_VERSION_FINGERPRINT, Some(_), Some(_)) => {}
        _ => {
            return Err(rebuild_required(
                index,
                "analyzer certificate format is unsupported",
            ));
        }
    }
    if let Some(expected) = expected
        && certificate.analyzer_fingerprint.as_deref() != Some(expected)
    {
        return Err(rebuild_required(
            index,
            "index was not built with the analyzer the field declares; rebuild it with the declared profile",
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
            ("lance", "13.0.0"),
            ("lance-index", "13.0.0"),
            ("lance-tokenizer", "13.0.0"),
            ("frostem", "1.20260821.3"),
            ("stop-words", "0.10.0"),
            ("unicode-normalization", "0.1.25"),
        ];
        let workspace: toml::Value = toml::from_str(include_str!("../../../Cargo.toml")).unwrap();
        let core: toml::Value = toml::from_str(include_str!("../Cargo.toml")).unwrap();
        let lock: toml::Value = toml::from_str(include_str!("../../../Cargo.lock")).unwrap();
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
                core["dependencies"][name]["workspace"].as_bool(),
                Some(true),
                "{name} must remain a direct inherited dependency of omnigraph-core"
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
        let key = verification_key("source", "store-a", &path, &original, 512, &expected, None).0;
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
                verification_key(
                    uri,
                    store,
                    &path,
                    &original,
                    certificate_size,
                    artifact,
                    None
                )
                .0,
                key
            );
        }
        let mut other_uuid = original.clone();
        other_uuid.uuid = "22222222-2222-4222-8222-222222222222".parse().unwrap();
        assert_ne!(
            verification_key(
                "source",
                "store-a",
                &path,
                &other_uuid,
                512,
                &expected,
                None
            )
            .0,
            key
        );
        // A proof of one analysis is not a proof of another.
        let english = verification_key(
            "source",
            "store-a",
            &path,
            &original,
            512,
            &expected,
            Some("sha256:english"),
        )
        .0;
        assert_ne!(english, key);
        assert_ne!(
            verification_key(
                "source",
                "store-a",
                &path,
                &original,
                512,
                &expected,
                Some("sha256:standard"),
            )
            .0,
            english
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
            analyzer_fingerprint: None,
            native: None,
        })
        .unwrap();
        verify_payload(&index, &valid.to_string(), &digest, None).unwrap();
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
                    verify_payload(&index, &changed.to_string(), &digest, None),
                    Err(OmniError::FullTextIndexRebuildRequired { .. })
                ),
                "accepted changed {key}"
            );
        }
        for malformed in ["{", "null", "{}"] {
            assert!(matches!(
                verify_payload(&index, malformed, &digest, None),
                Err(OmniError::FullTextIndexRebuildRequired { .. })
            ));
        }
    }

    /// A field's declared analyzer is established only by a format-2
    /// certificate naming its fingerprint and the native pipeline built: a
    /// certificate of Lance's default analysis, of another profile, or with
    /// either part missing refuses. A reader that holds no schema accepts
    /// any certified analysis.
    #[test]
    fn certificate_establishes_only_the_declared_analyzer_fingerprint() {
        let index = index();
        let digest = digest(&index);
        let native = NativeBuild {
            normalization: NativeNormalization::None,
            block_size: 128,
            posting_format: 2,
            positions: false,
        };
        let certificate =
            |format_version, fingerprint: Option<&str>, native: Option<NativeBuild>| {
                serde_json::to_string(&Certificate {
                    format_version,
                    index_uuid: index.uuid.to_string(),
                    analyzer_generation: ANALYZER_GENERATION.into(),
                    artifact_digest: digest.clone(),
                    analyzer_fingerprint: fingerprint.map(str::to_owned),
                    native,
                })
                .unwrap()
            };
        let english = certificate(
            FORMAT_VERSION_FINGERPRINT,
            Some("sha256:english"),
            Some(native),
        );
        verify_payload(&index, &english, &digest, Some("sha256:english")).unwrap();
        verify_payload(&index, &english, &digest, None).unwrap();
        let default_analysis = certificate(FORMAT_VERSION, None, None);
        verify_payload(&index, &default_analysis, &digest, None).unwrap();
        for (payload, expected) in [
            (&english, Some("sha256:standard")),
            (&default_analysis, Some("sha256:english")),
            (
                &certificate(FORMAT_VERSION, Some("sha256:english"), None),
                Some("sha256:english"),
            ),
            (&certificate(FORMAT_VERSION_FINGERPRINT, None, None), None),
            (
                &certificate(FORMAT_VERSION_FINGERPRINT, Some("sha256:english"), None),
                Some("sha256:english"),
            ),
            (&certificate(FORMAT_VERSION, None, Some(native)), None),
        ] {
            assert!(
                matches!(
                    verify_payload(&index, payload, &digest, expected),
                    Err(OmniError::FullTextIndexRebuildRequired { .. })
                ),
                "accepted {payload} for {expected:?}"
            );
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
