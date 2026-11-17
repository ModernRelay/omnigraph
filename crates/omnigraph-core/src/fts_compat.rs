//! Artifact-scoped full-text analyzer compatibility (RFC 0043).
//!
//! Lance's immutable index UUID and file inventory are the storage identity;
//! neither the table's writer version nor the posting format proves which
//! stemmer built an index. This certificate is derived index state, not a
//! graph migration flag or protection against an out-of-band malicious writer.
//!
//! A certificate records the field's declared analyzer fingerprint and the
//! measured fingerprint of the native configuration that built the postings
//! ([`crate::fts_analysis`]); qualification compares both with what the
//! running binary measures, so no hand-written version string stands for
//! what the analyzer does.

use std::{borrow::Cow, sync::Arc};

use lance::{Dataset, index::scalar::IndexDetails};
use lance_core::cache::CacheKey;
use lance_table::format::{IndexFile, IndexMetadata};
use object_store::path::Path;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use omnigraph_compiler::schema::analysis::AnalyzerProfile;

use crate::error::{OmniError, Result};
use crate::fts_analysis::{self, MeasuredAnalysis};

pub const CERTIFICATE_FILE: &str = "omnigraph_fts_compat.json";
/// A certificate of edition 2's analysis, Lance's default, written before
/// every full-text field had a profile. It names an analyzer generation and
/// nothing a profile can be checked against, so it never qualifies an index
/// for a profile; only its version is read.
const FORMAT_VERSION_DEFAULT_ANALYSIS: u32 = 1;
/// A certificate of an analyzer profile: the field's declared analyzer
/// fingerprint, the measured fingerprint of the native configuration the
/// postings were built with, and that build's options.
pub const CERTIFICATE_FORMAT_VERSION: u32 = 2;
pub const MAX_CERTIFICATE_BYTES: u64 = 64 * 1024;
const MAX_PAYLOAD_BYTES: usize = 4096;

/// The one field read from a certificate of any format before it is parsed
/// as the format it names.
#[derive(Deserialize)]
struct CertificateFormat {
    format_version: u32,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Certificate {
    format_version: u32,
    index_uuid: String,
    artifact_digest: String,
    /// The declared analyzer fingerprint of the profile the field named when
    /// the index was built.
    analyzer_fingerprint: String,
    /// The measured fingerprint of the native configuration the postings were
    /// built with ([`crate::fts_analysis::measured`]), taken by the building
    /// binary.
    native_fingerprint: String,
    /// The pipeline Lance built the postings with, which is not the profile
    /// the fingerprint names: Lance has no Unicode normalization step, so a
    /// profile's NFC is not applied to the postings.
    native: NativeBuild,
}

/// What a full-text index's postings were built with, as built: recorded in
/// its certificate beside the field's analyzer fingerprint, never in the
/// fingerprint (the GQ 3 RFC, section 4: whether an index stores term
/// positions is a build option of the index). A native path that reads the
/// postings is qualified against these facts.
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
/// `fingerprint` is the declared analyzer fingerprint of the field the
/// segment was built for and `profile` its profile, whose native
/// configuration built the postings; the certificate records that
/// configuration's measured fingerprint in this binary, and `native` the
/// build's options.
pub async fn write_certificate(
    dataset: &Dataset,
    index: &mut IndexMetadata,
    fingerprint: &str,
    profile: AnalyzerProfile,
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
        format_version: CERTIFICATE_FORMAT_VERSION,
        index_uuid: index.uuid.to_string(),
        artifact_digest: artifact_digest(index, &files)?,
        analyzer_fingerprint: fingerprint.to_owned(),
        native_fingerprint: fts_analysis::measured(profile)?.native.clone(),
        native,
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

/// Qualify one exact snapshot-selected FTS index segment for a field before a
/// native path reads it. `expected` is the declared analyzer fingerprint of
/// the field in its accepted schema and `profile` the field's profile; there
/// is no verification without them, so no path qualifies an index for
/// whatever analysis it carries.
///
/// The segment qualifies only when its certificate is a profile certificate
/// of this exact artifact naming `expected`, and
///
/// 1. the native fingerprint it recorded at build time equals the one this
///    binary measures for `profile` (a dependency change under an existing
///    index fails here), and
/// 2. this binary's native fingerprint for `profile` equals its profile
///    analyzer's (parity between the index and the exact scan).
///
/// A certificate of edition 2's analysis (format 1) never qualifies. Every
/// refusal is [`OmniError::FullTextIndexRebuildRequired`], its reason naming
/// the check; a refusal is never cached.
pub async fn verify_index(
    dataset: &Dataset,
    index: &IndexMetadata,
    expected: &str,
    profile: AnalyzerProfile,
) -> Result<()> {
    let running = fts_analysis::measured(profile)?;
    verify(dataset, index, expected, profile, running).await
}

/// [`verify_index`] against measured fingerprints the caller supplies in
/// place of this binary's: lets a test qualify an artifact as a binary
/// whose native configuration has parity would, which no profile has today.
#[cfg(any(test, feature = "test-util"))]
pub async fn verify_index_against(
    dataset: &Dataset,
    index: &IndexMetadata,
    expected: &str,
    profile: AnalyzerProfile,
    running: &MeasuredAnalysis,
) -> Result<()> {
    verify(dataset, index, expected, profile, running).await
}

async fn verify(
    dataset: &Dataset,
    index: &IndexMetadata,
    expected: &str,
    profile: AnalyzerProfile,
    running: &MeasuredAnalysis,
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
        running,
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
    verify_payload(index, payload, &digest, expected, profile, running)?;
    cache.insert_with_key(&key, Arc::new(true)).await;
    Ok(())
}

/// The proof's cache key: one artifact at one place, qualified for one
/// declared fingerprint against one pair of running measurements.
fn verification_key(
    dataset_uri: &str,
    store_prefix: &str,
    path: &Path,
    index: &IndexMetadata,
    certificate_size: u64,
    artifact_digest: &str,
    expected: &str,
    running: &MeasuredAnalysis,
) -> VerifiedCertificateKey {
    let mut digest = Sha256::new();
    digest.update(CERTIFICATE_FORMAT_VERSION.to_le_bytes());
    // A copied dataset may retain every UUID but have different certificate
    // bytes. Base-path changes and certificate-inventory changes also require
    // fresh evidence. Coverage and names remain outside immutable identity.
    // A field that declares another analysis must read the certificate again
    // and refuse.
    for field in [
        dataset_uri,
        store_prefix,
        path.as_ref(),
        &index.uuid.to_string(),
        artifact_digest,
        expected,
        &running.native,
        &running.analyzer,
    ] {
        hash_bytes(&mut digest, field.as_bytes());
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
    expected: &str,
    profile: AnalyzerProfile,
    running: &MeasuredAnalysis,
) -> Result<()> {
    let malformed = || rebuild_required(index, "analyzer certificate payload is malformed");
    let format: CertificateFormat = serde_json::from_str(payload).map_err(|_| malformed())?;
    match format.format_version {
        CERTIFICATE_FORMAT_VERSION => {}
        FORMAT_VERSION_DEFAULT_ANALYSIS => {
            return Err(rebuild_required(
                index,
                "index was built with edition 2's analysis, which is not an analyzer profile; \
                 rebuild it with the field's profile",
            ));
        }
        _ => {
            return Err(rebuild_required(
                index,
                "analyzer certificate format is unsupported",
            ));
        }
    }
    let certificate: Certificate = serde_json::from_str(payload).map_err(|_| malformed())?;
    if certificate.index_uuid != index.uuid.to_string() {
        return Err(rebuild_required(
            index,
            "analyzer certificate belongs to a different index UUID",
        ));
    }
    if certificate.artifact_digest != digest {
        return Err(rebuild_required(
            index,
            "analyzer certificate does not match the index artifact",
        ));
    }
    if certificate.analyzer_fingerprint != expected {
        return Err(rebuild_required(
            index,
            "index was not built with the analyzer the field declares; rebuild it with the declared profile",
        ));
    }
    if certificate.native_fingerprint != running.native {
        return Err(rebuild_required(
            index,
            &format!(
                "index was built by a native {profile} pipeline whose measured fingerprint \
                 differs from this binary's; rebuild it with this binary"
            ),
        ));
    }
    if running.native != running.analyzer {
        return Err(rebuild_required(
            index,
            &format!(
                "this binary's native {profile} pipeline does not emit the profile analyzer's \
                 terms (their measured fingerprints differ), so no native index qualifies for \
                 the profile and a rebuild cannot change that; the exact scan answers"
            ),
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

    /// The analyzer's dependencies are pinned to the audited versions. This
    /// pins versions; `fts_analysis::tests::measured_fingerprints_are_the_golden`
    /// pins behavior. Updating a pin requires the RFC 0043 audit; neither a
    /// manifest edit nor a lockfile refresh grants that approval.
    #[test]
    fn analyzer_dependencies_are_the_audited_resolved_set() {
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

    /// Running measurements with parity, as a binary whose native pipeline
    /// implements the profile would measure.
    fn running(native: &str) -> MeasuredAnalysis {
        MeasuredAnalysis {
            native: native.into(),
            analyzer: native.into(),
        }
    }

    #[test]
    fn artifact_proof_tracks_immutable_details_not_coverage_or_location() {
        let original = index();
        let expected = digest(&original);
        let path = Path::from("source/_indices/certificate.json");
        let measured = running("sha256:native");
        let key_of = |uri: &str,
                      store: &str,
                      path: &Path,
                      index: &IndexMetadata,
                      size: u64,
                      artifact: &str,
                      fingerprint: &str,
                      measured: &MeasuredAnalysis| {
            verification_key(
                uri,
                store,
                path,
                index,
                size,
                artifact,
                fingerprint,
                measured,
            )
            .0
        };
        let key = key_of(
            "source",
            "store-a",
            &path,
            &original,
            512,
            &expected,
            "sha256:english",
            &measured,
        );
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
                key_of(
                    uri,
                    store,
                    &path,
                    &original,
                    certificate_size,
                    artifact,
                    "sha256:english",
                    &measured
                ),
                key
            );
        }
        let mut other_uuid = original.clone();
        other_uuid.uuid = "22222222-2222-4222-8222-222222222222".parse().unwrap();
        assert_ne!(
            key_of(
                "source",
                "store-a",
                &path,
                &other_uuid,
                512,
                &expected,
                "sha256:english",
                &measured
            ),
            key
        );
        // A proof of one analysis, or against another binary's measurements,
        // is not a proof of another.
        for (fingerprint, measured) in [
            ("sha256:standard", running("sha256:native")),
            ("sha256:english", running("sha256:another-native")),
            (
                "sha256:english",
                MeasuredAnalysis {
                    native: "sha256:native".into(),
                    analyzer: "sha256:another-analyzer".into(),
                },
            ),
        ] {
            assert_ne!(
                key_of(
                    "source",
                    "store-a",
                    &path,
                    &original,
                    512,
                    &expected,
                    fingerprint,
                    &measured
                ),
                key
            );
        }
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

    fn native() -> NativeBuild {
        NativeBuild {
            normalization: NativeNormalization::None,
            block_size: 128,
            posting_format: 2,
            positions: false,
        }
    }

    /// A profile certificate of `index` for `fingerprint`, built by a native
    /// pipeline measuring `native_fingerprint`.
    fn certificate(index: &IndexMetadata, fingerprint: &str, native_fingerprint: &str) -> String {
        serde_json::to_string(&Certificate {
            format_version: CERTIFICATE_FORMAT_VERSION,
            index_uuid: index.uuid.to_string(),
            artifact_digest: digest(index),
            analyzer_fingerprint: fingerprint.into(),
            native_fingerprint: native_fingerprint.into(),
            native: native(),
        })
        .unwrap()
    }

    fn refused(result: Result<()>) -> String {
        match result {
            Err(OmniError::FullTextIndexRebuildRequired { reason, .. }) => reason,
            other => panic!("expected FullTextIndexRebuildRequired, got {other:?}"),
        }
    }

    #[test]
    fn certificate_requires_exact_uuid_artifact_and_known_format() {
        let index = index();
        let digest = digest(&index);
        let measured = running("sha256:native");
        let check = |payload: &str| {
            verify_payload(
                &index,
                payload,
                &digest,
                "sha256:english",
                AnalyzerProfile::EnglishV1,
                &measured,
            )
        };
        let valid: serde_json::Value =
            serde_json::from_str(&certificate(&index, "sha256:english", "sha256:native")).unwrap();
        check(&valid.to_string()).unwrap();
        for (key, value) in [
            ("format_version", serde_json::json!(3)),
            ("index_uuid", serde_json::json!("another UUID")),
            ("artifact_digest", serde_json::json!("another artifact")),
            ("analyzer_generation", serde_json::json!("lance11")),
            ("unknown_field", serde_json::json!(true)),
            ("native", serde_json::json!(null)),
        ] {
            let mut changed = valid.clone();
            changed[key] = value;
            refused(check(&changed.to_string()));
        }
        for field in [
            "analyzer_fingerprint",
            "native_fingerprint",
            "native",
            "artifact_digest",
        ] {
            let mut changed = valid.clone();
            changed.as_object_mut().unwrap().remove(field);
            refused(check(&changed.to_string()));
        }
        for malformed in ["{", "null", "{}", "[]", r#"{"format_version":"2"}"#] {
            refused(check(malformed));
        }
    }

    /// A field's profile is established only by a profile certificate of the
    /// field's declared fingerprint, built by a native pipeline this binary
    /// measures the same, in a binary whose native pipeline has parity with
    /// the profile analyzer. Each check refuses alone, with its own reason.
    #[test]
    fn certificate_qualifies_only_the_declared_fingerprint_with_measured_parity() {
        let index = index();
        let digest = digest(&index);
        let english = AnalyzerProfile::EnglishV1;
        let built = certificate(&index, "sha256:english", "sha256:native");
        let check = |payload: &str, expected: &str, measured: &MeasuredAnalysis| {
            verify_payload(&index, payload, &digest, expected, english, measured)
        };
        check(&built, "sha256:english", &running("sha256:native")).unwrap();

        // Another declared analysis.
        let reason = refused(check(&built, "sha256:standard", &running("sha256:native")));
        assert!(
            reason.contains("not built with the analyzer the field declares"),
            "{reason}"
        );
        // A dependency change under the index: this binary measures its
        // native pipeline differently.
        let reason = refused(check(
            &built,
            "sha256:english",
            &running("sha256:changed-native"),
        ));
        assert!(reason.contains("measured fingerprint differs"), "{reason}");
        // No parity in this binary: the native pipeline is not the profile.
        let reason = refused(check(
            &built,
            "sha256:english",
            &MeasuredAnalysis {
                native: "sha256:native".into(),
                analyzer: "sha256:analyzer".into(),
            },
        ));
        assert!(
            reason.contains("does not emit the profile analyzer"),
            "{reason}"
        );
        // Edition 2's analysis is not a profile, whatever its generation.
        let format_1 = serde_json::json!({
            "format_version": 1,
            "index_uuid": index.uuid.to_string(),
            "analyzer_generation": "lance11.0.0-frostem1.20260821.3-v1",
            "artifact_digest": digest,
        })
        .to_string();
        let reason = refused(check(
            &format_1,
            "sha256:english",
            &running("sha256:native"),
        ));
        assert!(reason.contains("edition 2's analysis"), "{reason}");
    }

    /// The certificate this binary writes records its own measured native
    /// fingerprint, so a binary with parity qualifies it, and this binary,
    /// which has no parity for any profile, refuses it.
    #[test]
    fn the_running_binary_refuses_every_profile_for_want_of_parity() {
        let index = index();
        let digest = digest(&index);
        for profile in AnalyzerProfile::ALL {
            let measured = fts_analysis::measured(profile).unwrap();
            let built = certificate(&index, "sha256:declared", &measured.native);
            verify_payload(
                &index,
                &built,
                &digest,
                "sha256:declared",
                profile,
                &running(&measured.native),
            )
            .unwrap();
            let reason = refused(verify_payload(
                &index,
                &built,
                &digest,
                "sha256:declared",
                profile,
                measured,
            ));
            assert!(
                reason.contains("does not emit the profile analyzer"),
                "{reason}"
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
