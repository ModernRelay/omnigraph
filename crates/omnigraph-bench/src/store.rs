//! The benchmark store: frozen bases, per-repetition clone roots, archived
//! records and per-commit binaries under one local path or one `s3://` prefix
//! (RFC 0039 §The store). A base is built where it lives and never written
//! again; a repetition reads it through an empty clone root the engine opens
//! with `Omnigraph::open_clone`; a record is one immutable object; a binary is
//! keyed by the commit it was built from and its target triple.
//!
//! Layout under the store root: `bases/<key>/root/` with `base-manifest.json`
//! beside it (`building.json` while a build holds the entry), `clones/<run>-<rep>/`,
//! `records/<sha256>.json`, `builds/<commit>/<triple>/<product>` with
//! `<product>.sha256` beside it.
use crate::dataset_cache::GQT_ENGINE_DIGEST;
use crate::dataset_identity::DatasetLogicalV1;
use crate::gqt_case::{DatasetBuildPlan, DatasetRecipe};
use crate::gqt_record::{AuthorityRecord, canonical_bytes};
use crate::model::{digest as valid_digest, sha256_bytes, typed_sha256};
use crate::runner::{RunnerError, RunnerResult};
use futures::StreamExt;
use object_store::ObjectStore;
use omnigraph::storage::{StorageAdapter, storage_for_uri};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// The key algorithm; a term of every base key.
pub const BASE_KEY_ALGORITHM: &str = "gqt-base-v1";
/// The manifest object beside a published base root.
pub const BASE_MANIFEST: &str = "base-manifest.json";
/// The lease object a build holds under its entry until the manifest is up.
pub const BASE_LEASE: &str = "building.json";
/// The prefix the engine reserves inside a clone root for its own state.
pub const CLONE_MARKER: &str = "__clone";
const MAX_MANIFEST_BYTES: u64 = 4 * 1024 * 1024;
const MAX_LEASE_BYTES: u64 = 16 * 1024;
const MAX_BINARY_BYTES: u64 = 1024 * 1024 * 1024;
const MAX_LISTED_OBJECTS: usize = 1_000_000;

/// Where a store lives: the one term of a base key that names the place.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum StoreLocation {
    /// A directory on this host, canonical and absolute.
    Local { path: PathBuf },
    /// A prefix of one bucket; empty for the bucket itself.
    S3 { bucket: String, prefix: String },
}

impl StoreLocation {
    /// `s3://bucket/prefix` or a local path; a local path is created and made
    /// canonical, since the key names the place.
    pub fn parse(text: &str) -> RunnerResult<Self> {
        if let Some(rest) = text.strip_prefix("s3://") {
            let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
            let prefix = prefix.trim_matches('/');
            if bucket.is_empty()
                || !bucket
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.')
            {
                return Err(invalid(format!("`{text}` names no S3 bucket")));
            }
            if prefix.split('/').any(|segment| {
                segment.is_empty()
                    || segment == "."
                    || segment == ".."
                    || !segment
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
            }) && !prefix.is_empty()
            {
                return Err(invalid(format!(
                    "`{text}` has a prefix segment outside [A-Za-z0-9-_.]"
                )));
            }
            return Ok(Self::S3 {
                bucket: bucket.to_owned(),
                prefix: prefix.to_owned(),
            });
        }
        if text.contains("://") {
            return Err(invalid(format!(
                "`{text}`: a store is a local path or an s3:// prefix"
            )));
        }
        let path = Path::new(text);
        std::fs::create_dir_all(path).map_err(|e| io_error(text, e))?;
        let path = path.canonicalize().map_err(|e| io_error(text, e))?;
        Ok(Self::Local { path })
    }

    /// The root URI the storage adapter addresses.
    pub fn uri(&self) -> String {
        match self {
            Self::Local { path } => path.to_string_lossy().into_owned(),
            Self::S3 { bucket, prefix } if prefix.is_empty() => format!("s3://{bucket}"),
            Self::S3 { bucket, prefix } => format!("s3://{bucket}/{prefix}"),
        }
    }

    pub fn is_s3(&self) -> bool {
        matches!(self, Self::S3 { .. })
    }

    fn join(&self, parts: &[&str]) -> String {
        format!("{}/{}", self.uri(), parts.join("/"))
    }
}

/// The storage format a base was written in: the terms of its key beyond the
/// recipe and the place. None changes on an ordinary engine commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageFormatV1 {
    /// The `omnigraph:internal_schema_version` stamp this engine writes.
    pub internal_schema_version: u32,
    /// The pinned `lance` crate version (`Cargo.lock`).
    pub lance_version: String,
    /// What this engine's full-text index certificates record, so a change in
    /// what analysis writes always yields a new base.
    pub fts_certificate: FtsCertificateV1,
}

/// The full-text analysis a base's index certificates record: the
/// certificate format and, per analyzer profile, the measured fingerprints of
/// its native configuration and its analyzer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FtsCertificateV1 {
    pub format_version: u32,
    pub profiles: Vec<FtsProfileV1>,
}

/// One analyzer profile's measured fingerprints in the engine that wrote a
/// base.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FtsProfileV1 {
    pub profile: String,
    pub native: String,
    pub analyzer: String,
}

impl StorageFormatV1 {
    pub fn current() -> Self {
        let profiles = omnigraph_compiler::schema::analysis::AnalyzerProfile::ALL
            .into_iter()
            .map(|profile| {
                // The probe corpus and the analyzers are compiled in, so a
                // measurement that fails is a broken binary, not an input.
                let measured = omnigraph::fts_measured_analysis(profile)
                    .expect("the engine measures every analyzer profile");
                FtsProfileV1 {
                    profile: profile.name().to_owned(),
                    native: measured.native.clone(),
                    analyzer: measured.analyzer.clone(),
                }
            })
            .collect();
        Self {
            internal_schema_version: omnigraph::db::manifest::INTERNAL_MANIFEST_SCHEMA_VERSION,
            lance_version: env!("GQT_LANCE_VERSION").to_owned(),
            fts_certificate: FtsCertificateV1 {
                format_version: omnigraph::FTS_CERTIFICATE_FORMAT_VERSION,
                profiles,
            },
        }
    }
}

#[derive(Serialize)]
struct BaseKeyTerms<'a> {
    algorithm: &'static str,
    recipe_sha256: &'a str,
    needs_indices: bool,
    store: &'a StoreLocation,
    format: &'a StorageFormatV1,
    registered_tree_sha256: Option<&'a str>,
}

/// The key of a frozen base: the recipe, its index requirement, the place, the
/// storage format and the registered source's tree; never the engine commit.
pub fn base_key(
    plan: &DatasetBuildPlan,
    store: &StoreLocation,
    format: &StorageFormatV1,
    registered_tree_sha256: Option<&str>,
) -> RunnerResult<String> {
    typed_sha256(&BaseKeyTerms {
        algorithm: BASE_KEY_ALGORITHM,
        recipe_sha256: &plan.recipe_sha256,
        needs_indices: plan.needs_indices,
        store,
        format,
        registered_tree_sha256,
    })
    .map_err(|e| RunnerError::new("dataset_store_key_failed", e.message))
}

/// One object of a base root as listed: its path under the root, its size and
/// the store's version token (the ETag on S3, the content's sha256 locally).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectWitnessV1 {
    pub path: String,
    pub bytes: u64,
    pub version: String,
}

/// The published description of a frozen base; the listing it must match on
/// every later acquisition and the logical digest the base must reproduce.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BaseManifestV1 {
    pub format_version: u32,
    pub key: String,
    pub recipe_sha256: String,
    /// The harness build that constructed the base: recorded, not keyed.
    pub engine_digest: String,
    pub needs_indices: bool,
    pub storage_format: StorageFormatV1,
    pub summary: DatasetLogicalV1,
    pub registered_source_identity: Option<String>,
    pub objects: Vec<ObjectWitnessV1>,
    pub built_at_unix_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BaseLeaseV1 {
    format_version: u32,
    key: String,
    holder_pid: u32,
    started_unix_ms: u64,
}

/// A frozen base this process acquired: verified against its manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FrozenBase {
    pub key: String,
    pub root_uri: String,
    pub manifest: BaseManifestV1,
    pub cache_hit: bool,
}

/// One repetition's clone root: empty when handed out, deleted after.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CloneRoot {
    pub uri: String,
    pub run: String,
    pub repetition: u32,
}

/// The receipt of one record put: its digest names the object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StoreRecordReceiptV1 {
    pub record_sha256: String,
    pub uri: String,
}

/// A binary under `builds/`: the commit it was built from, the target triple
/// it runs on and which product it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BuildKey<'a> {
    pub commit: &'a str,
    pub target_triple: &'a str,
    pub product: &'a str,
}

impl BuildKey<'_> {
    fn validate(&self) -> RunnerResult<()> {
        if self.commit.len() != 40
            || !self
                .commit
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        {
            return Err(invalid(format!(
                "`{}` is not a full lowercase commit sha",
                self.commit
            )));
        }
        for (name, value) in [
            ("target triple", self.target_triple),
            ("product", self.product),
        ] {
            if value.is_empty()
                || !value
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
            {
                return Err(invalid(format!("`{value}` is not a {name}")));
            }
        }
        Ok(())
    }
}

/// The store, open over its storage adapter.
pub struct FixtureStore {
    location: StoreLocation,
    storage: Arc<dyn StorageAdapter>,
}

impl std::fmt::Debug for FixtureStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FixtureStore")
            .field("location", &self.location)
            .finish()
    }
}

impl FixtureStore {
    pub fn open(text: &str) -> RunnerResult<Self> {
        let location = StoreLocation::parse(text)?;
        let storage = storage_for_uri(&location.uri()).map_err(|e| {
            RunnerError::new(
                "dataset_store_unreachable",
                format!("{}: {e}", location.uri()),
            )
        })?;
        Ok(Self { location, storage })
    }

    pub fn location(&self) -> &StoreLocation {
        &self.location
    }

    pub fn uri(&self) -> String {
        self.location.uri()
    }

    pub fn storage(&self) -> &Arc<dyn StorageAdapter> {
        &self.storage
    }

    fn entry_uri(&self, key: &str) -> String {
        self.location.join(&["bases", key])
    }

    /// The base for `plan`: the published one when its manifest is up and the
    /// listing, the version tokens and the logical digest still agree, else
    /// built in place under a lease and published. An entry with objects and
    /// no manifest is refused until it is deleted explicitly; a held lease is
    /// refused as busy.
    pub async fn acquire_base(
        &self,
        plan: &DatasetBuildPlan,
        scratch: &Path,
        binding: Option<&str>,
        no_build: bool,
    ) -> RunnerResult<FrozenBase> {
        plan.revalidate()
            .map_err(|e| RunnerError::new("invalid_case", e))?;
        let registered = crate::dataset_cache::registered_physical(plan, binding)?;
        let format = StorageFormatV1::current();
        let key = base_key(
            plan,
            &self.location,
            &format,
            registered.as_ref().map(|tree| tree.tree_sha256.as_str()),
        )?;
        let entry = self.entry_uri(&key);
        let root_uri = format!("{entry}/root");
        let manifest_uri = format!("{entry}/{BASE_MANIFEST}");
        let lease_uri = format!("{entry}/{BASE_LEASE}");
        if let Some(text) = self
            .storage
            .read_text_if_exists_bounded(&manifest_uri, MAX_MANIFEST_BYTES)
            .await
            .map_err(|e| storage_error(&manifest_uri, e))?
        {
            let manifest: BaseManifestV1 =
                serde_json::from_str(&text).map_err(|e| corrupt(format!("{manifest_uri}: {e}")))?;
            if manifest.format_version != 1 || manifest.key != key {
                return Err(corrupt(format!(
                    "{manifest_uri} describes key {} under entry {key}",
                    manifest.key
                )));
            }
            let base = FrozenBase {
                key,
                root_uri,
                manifest,
                cache_hit: true,
            };
            self.verify_base(&base).await?;
            return Ok(base);
        }
        if no_build {
            return Err(RunnerError::new(
                "dataset_store_miss",
                format!("--no-build requires a published base under {entry}"),
            ));
        }
        if self
            .storage
            .read_text_if_exists_bounded(&lease_uri, MAX_LEASE_BYTES)
            .await
            .map_err(|e| storage_error(&lease_uri, e))?
            .is_some()
        {
            return Err(RunnerError::new(
                "dataset_store_busy",
                format!("another build holds {lease_uri}"),
            ));
        }
        let present = self.witness(&entry).await?;
        if !present.is_empty() {
            return Err(RunnerError::new(
                "dataset_store_unpublished",
                format!(
                    "{} object(s) under {entry} with no {BASE_MANIFEST}: an interrupted build; delete the entry explicitly before building again",
                    present.len()
                ),
            ));
        }
        if matches!(plan.dataset, DatasetRecipe::Registered { .. }) && self.location.is_s3() {
            return Err(RunnerError::new(
                "unsupported_runner_axis",
                "a registered dataset is built on a local store only: its bundle is copied into the base root",
            ));
        }
        let lease = BaseLeaseV1 {
            format_version: 1,
            key: key.clone(),
            holder_pid: std::process::id(),
            started_unix_ms: unix_time_millis()?,
        };
        let lease_text = serde_json::to_string(&lease).map_err(|e| corrupt(e.to_string()))?;
        if !self
            .storage
            .write_text_if_absent(&lease_uri, &lease_text)
            .await
            .map_err(|e| storage_error(&lease_uri, e))?
        {
            return Err(RunnerError::new(
                "dataset_store_busy",
                format!("another build took {lease_uri} first"),
            ));
        }
        if let StoreLocation::Local { .. } = self.location {
            std::fs::create_dir_all(&root_uri).map_err(|e| io_error(&root_uri, e))?;
        }
        let (summary, registered_source_identity) =
            crate::dataset_worker::build_dataset(plan, &root_uri, scratch, binding)
                .await
                .map_err(|e| {
                    RunnerError::new(
                        "fixture_build_failed",
                        format!("{e}; {lease_uri} stays until the entry is deleted explicitly"),
                    )
                })?;
        let objects = self.witness(&root_uri).await?;
        if objects.is_empty() {
            return Err(corrupt(format!(
                "the base built under {root_uri} lists no object"
            )));
        }
        let manifest = BaseManifestV1 {
            format_version: 1,
            key: key.clone(),
            recipe_sha256: plan.recipe_sha256.clone(),
            engine_digest: GQT_ENGINE_DIGEST.to_owned(),
            needs_indices: plan.needs_indices,
            storage_format: format,
            summary,
            registered_source_identity,
            objects,
            built_at_unix_ms: unix_time_millis()?,
        };
        let text = serde_json::to_string(&manifest).map_err(|e| corrupt(e.to_string()))?;
        if text.len() as u64 > MAX_MANIFEST_BYTES {
            return Err(corrupt(format!(
                "the base manifest is {} bytes; the bound is {MAX_MANIFEST_BYTES}",
                text.len()
            )));
        }
        if !self
            .storage
            .write_text_if_absent(&manifest_uri, &text)
            .await
            .map_err(|e| storage_error(&manifest_uri, e))?
        {
            return Err(corrupt(format!(
                "{manifest_uri} appeared while this process held the lease"
            )));
        }
        self.storage
            .delete(&lease_uri)
            .await
            .map_err(|e| storage_error(&lease_uri, e))?;
        Ok(FrozenBase {
            key,
            root_uri,
            manifest,
            cache_hit: false,
        })
    }

    /// The base as it stands against its manifest: the listing with its
    /// version tokens, then the logical digest recomputed from a read-only
    /// open. Any difference is corruption, never silently rebuilt.
    pub async fn verify_base(&self, base: &FrozenBase) -> RunnerResult<()> {
        let listed = self.witness(&base.root_uri).await?;
        if listed != base.manifest.objects {
            let differing = listed
                .iter()
                .zip(base.manifest.objects.iter())
                .find(|(now, then)| now != then)
                .map(|(now, _)| now.path.clone())
                .or_else(|| {
                    listed
                        .get(base.manifest.objects.len())
                        .map(|o| o.path.clone())
                })
                .or_else(|| {
                    base.manifest
                        .objects
                        .get(listed.len())
                        .map(|o| o.path.clone())
                })
                .unwrap_or_default();
            return Err(corrupt(format!(
                "{} lists {} object(s) against {} in its manifest; first difference at `{differing}`",
                base.root_uri,
                listed.len(),
                base.manifest.objects.len()
            )));
        }
        let observed = crate::dataset_identity::observe_uri(&base.root_uri)
            .await
            .map_err(|e| corrupt(format!("{}: {e}", base.root_uri)))?;
        let mut expected = base.manifest.summary.clone();
        if base.manifest.registered_source_identity.is_some() {
            expected.logical_content_sha256 = observed.logical_content_sha256.clone();
            expected.algorithm = observed.algorithm.clone();
            if base.manifest.summary.branches != observed.branches {
                return Err(corrupt(format!(
                    "{} reproduces another branch inventory than its manifest",
                    base.root_uri
                )));
            }
        } else if observed.logical_content_sha256 != expected.logical_content_sha256 {
            return Err(corrupt(format!(
                "{} reproduces logical digest {} against {} in its manifest",
                base.root_uri, observed.logical_content_sha256, expected.logical_content_sha256
            )));
        }
        crate::dataset_identity::validate(
            &base.manifest.summary,
            base.manifest.registered_source_identity.as_deref(),
        )
        .map_err(corrupt)?;
        Ok(())
    }

    /// Every object under `uri`, sorted by path, with its version token; empty
    /// when nothing is there. Bounded, and a symlink under a local root refuses.
    pub async fn witness(&self, uri: &str) -> RunnerResult<Vec<ObjectWitnessV1>> {
        let mut objects = match &self.location {
            StoreLocation::Local { .. } => {
                let root = Path::new(uri);
                let mut objects = Vec::new();
                if root.exists() {
                    walk_local(root, root, &mut objects)?;
                }
                objects
            }
            StoreLocation::S3 { bucket, .. } => {
                let client = s3_client(bucket)?;
                let prefix = uri
                    .strip_prefix(&format!("s3://{bucket}/"))
                    .ok_or_else(|| invalid(format!("`{uri}` is outside bucket {bucket}")))?;
                let prefix_path = object_store::path::Path::from(prefix);
                let mut stream = client.list(Some(&prefix_path));
                let mut objects = Vec::new();
                while let Some(meta) = stream.next().await {
                    let meta = meta.map_err(|e| storage_error(uri, e))?;
                    let location = meta.location.as_ref();
                    let path = location
                        .strip_prefix(prefix)
                        .and_then(|rest| rest.strip_prefix('/'))
                        .ok_or_else(|| corrupt(format!("`{location}` listed outside {uri}")))?;
                    let version = meta
                        .e_tag
                        .clone()
                        .ok_or_else(|| corrupt(format!("`{location}` listed without an ETag")))?;
                    objects.push(ObjectWitnessV1 {
                        path: path.to_owned(),
                        bytes: meta.size,
                        version,
                    });
                    if objects.len() > MAX_LISTED_OBJECTS {
                        return Err(corrupt(format!(
                            "{uri} lists more than {MAX_LISTED_OBJECTS} objects"
                        )));
                    }
                }
                objects
            }
        };
        objects.sort();
        Ok(objects)
    }

    /// An empty clone root for one repetition, its emptiness witnessed by a
    /// listing before it is handed out.
    pub async fn empty_clone(&self, run: &str, repetition: u32) -> RunnerResult<CloneRoot> {
        if run.is_empty()
            || !run
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        {
            return Err(invalid(format!("`{run}` is not a run id")));
        }
        let uri = self
            .location
            .join(&["clones", &format!("{run}-{repetition:08}")]);
        if let StoreLocation::Local { .. } = self.location {
            std::fs::create_dir_all(&uri).map_err(|e| io_error(&uri, e))?;
        }
        let present = self.witness(&uri).await?;
        if !present.is_empty() {
            return Err(RunnerError::new(
                "clone_root_not_empty",
                format!(
                    "{} object(s) under {uri} before the repetition",
                    present.len()
                ),
            ));
        }
        Ok(CloneRoot {
            uri,
            run: run.to_owned(),
            repetition,
        })
    }

    /// The paths under a clone root outside the engine's reserved marker:
    /// empty before the measured step, or the preparation wrote where it must not.
    pub async fn clone_writes_outside_marker(
        &self,
        clone: &CloneRoot,
    ) -> RunnerResult<Vec<String>> {
        Ok(self
            .witness(&clone.uri)
            .await?
            .into_iter()
            .map(|object| object.path)
            .filter(|path| path != CLONE_MARKER && !path.starts_with(&format!("{CLONE_MARKER}/")))
            .collect())
    }

    /// Delete the clone root and witness that nothing is left under it.
    pub async fn delete_clone(&self, clone: &CloneRoot) -> RunnerResult<()> {
        self.storage
            .delete_prefix(&clone.uri)
            .await
            .map_err(|e| storage_error(&clone.uri, e))?;
        let left = self.witness(&clone.uri).await?;
        if !left.is_empty() {
            return Err(RunnerError::new(
                "clone_root_not_deleted",
                format!(
                    "{} object(s) under {} after its deletion",
                    left.len(),
                    clone.uri
                ),
            ));
        }
        Ok(())
    }

    /// Put one validated record as the immutable object its digest names; a
    /// second put of the same record is refused, not repeated: an existing
    /// object is that record already put, and the conditional put only
    /// settles a race (its lost-answer read-back calls equal bytes applied).
    pub async fn put_record<R: AuthorityRecord>(
        &self,
        record: &R,
    ) -> RunnerResult<StoreRecordReceiptV1> {
        let bytes = canonical_bytes(record)
            .map_err(|e| RunnerError::new("archive_record_invalid", format!("{e:?}")))?;
        let record_sha256 = sha256_bytes(&bytes);
        let uri = self
            .location
            .join(&["records", &format!("{record_sha256}.json")]);
        let text = String::from_utf8(bytes)
            .map_err(|e| RunnerError::new("archive_record_invalid", e.to_string()))?;
        let exists = RunnerError::new(
            "record_exists",
            format!("{uri} already holds this record (an object is named by its bytes)"),
        );
        if self
            .storage
            .exists(&uri)
            .await
            .map_err(|e| storage_error(&uri, e))?
        {
            return Err(exists);
        }
        if !self
            .storage
            .write_text_if_absent(&uri, &text)
            .await
            .map_err(|e| storage_error(&uri, e))?
        {
            return Err(exists);
        }
        Ok(StoreRecordReceiptV1 { record_sha256, uri })
    }

    /// The URIs of every record object, sorted.
    pub async fn list_records(&self) -> RunnerResult<Vec<String>> {
        let records = self.location.join(&["records"]);
        let mut uris = self
            .storage
            .list_dir(&records)
            .await
            .map_err(|e| storage_error(&records, e))?
            .into_iter()
            .filter(|uri| uri.ends_with(".json"))
            .collect::<Vec<_>>();
        uris.sort();
        Ok(uris)
    }

    /// One record's bytes, refused when they do not hash to the name.
    pub async fn read_record(&self, uri: &str) -> RunnerResult<Vec<u8>> {
        let named = uri
            .rsplit('/')
            .next()
            .and_then(|name| name.strip_suffix(".json"))
            .filter(|digest| valid_digest(digest))
            .ok_or_else(|| corrupt(format!("`{uri}` is not a record object")))?;
        let bytes = self
            .storage
            .read_bytes_if_exists_bounded(uri, crate::archive::MAX_ARCHIVE_RECORD_BYTES)
            .await
            .map_err(|e| storage_error(uri, e))?
            .ok_or_else(|| corrupt(format!("{uri} disappeared after its listing")))?;
        if sha256_bytes(&bytes) != named {
            return Err(corrupt(format!("{uri} does not hash to its name")));
        }
        Ok(bytes)
    }

    fn build_uris(&self, key: BuildKey<'_>) -> (String, String) {
        let product = self
            .location
            .join(&["builds", key.commit, key.target_triple, key.product]);
        (format!("{product}.sha256"), product)
    }

    /// The cached binary of a commit, written to `destination` executable,
    /// or `None` when the store has none or holds its `.sha256` without the
    /// binary yet (a put in flight); a binary whose bytes do not hash to its
    /// `.sha256` is refused.
    pub async fn resolve_build(
        &self,
        key: BuildKey<'_>,
        destination: &Path,
    ) -> RunnerResult<Option<PathBuf>> {
        key.validate()?;
        let (digest_uri, product_uri) = self.build_uris(key);
        let Some(expected) = self
            .storage
            .read_text_if_exists_bounded(&digest_uri, MAX_LEASE_BYTES)
            .await
            .map_err(|e| storage_error(&digest_uri, e))?
        else {
            return Ok(None);
        };
        let expected = expected.trim();
        if !valid_digest(expected) {
            return Err(corrupt(format!("{digest_uri} holds no sha256")));
        }
        let Some(bytes) = self
            .storage
            .read_bytes_if_exists_bounded(&product_uri, MAX_BINARY_BYTES)
            .await
            .map_err(|e| storage_error(&product_uri, e))?
        else {
            return Ok(None);
        };
        if sha256_bytes(&bytes) != expected {
            return Err(corrupt(format!(
                "{product_uri} does not hash to {digest_uri}"
            )));
        }
        write_executable(destination, &bytes)?;
        Ok(Some(destination.to_path_buf()))
    }

    /// Put a binary built from a clean tree at `commit`; a dirty tree's binary
    /// is never cached. The `.sha256` is claimed if-absent first, so of two
    /// concurrent puts one names the product; the other is refused when its
    /// bytes differ, and writes the same bytes when the product is missing
    /// (an if-absent write of equal bytes answers as the writer's own on S3,
    /// so the product's presence, not the claim, says whether it was there).
    /// `Ok(false)` when the store already holds that commit's product.
    pub async fn put_build(
        &self,
        key: BuildKey<'_>,
        binary: &Path,
        clean_tree: bool,
    ) -> RunnerResult<bool> {
        key.validate()?;
        if !clean_tree {
            return Err(RunnerError::new(
                "dirty_tree_not_cached",
                format!(
                    "{} was built from a dirty tree; only a clean tree's binary is cached under {}",
                    binary.display(),
                    key.commit
                ),
            ));
        }
        let bytes =
            std::fs::read(binary).map_err(|e| io_error(&binary.display().to_string(), e))?;
        let digest = sha256_bytes(&bytes);
        let (digest_uri, product_uri) = self.build_uris(key);
        let created = self
            .storage
            .write_text_if_absent(&digest_uri, &digest)
            .await
            .map_err(|e| storage_error(&digest_uri, e))?;
        if !created {
            let existing = self
                .storage
                .read_text_if_exists_bounded(&digest_uri, MAX_LEASE_BYTES)
                .await
                .map_err(|e| storage_error(&digest_uri, e))?
                .unwrap_or_default();
            if existing.trim() != digest {
                return Err(corrupt(format!(
                    "{digest_uri} holds {} for a binary hashing to {digest}",
                    existing.trim()
                )));
            }
        }
        if self
            .storage
            .exists(&product_uri)
            .await
            .map_err(|e| storage_error(&product_uri, e))?
        {
            return Ok(false);
        }
        self.storage
            .write_bytes(&product_uri, &bytes)
            .await
            .map_err(|e| storage_error(&product_uri, e))?;
        Ok(created)
    }
}

fn walk_local(
    root: &Path,
    directory: &Path,
    objects: &mut Vec<ObjectWitnessV1>,
) -> RunnerResult<()> {
    let mut entries = std::fs::read_dir(directory)
        .map_err(|e| io_error(&directory.display().to_string(), e))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| io_error(&directory.display().to_string(), e))?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let path = entry.path();
        let metadata = std::fs::symlink_metadata(&path)
            .map_err(|e| io_error(&path.display().to_string(), e))?;
        if metadata.file_type().is_symlink() {
            return Err(corrupt(format!("{} is a symlink", path.display())));
        }
        if metadata.is_dir() {
            walk_local(root, &path, objects)?;
            continue;
        }
        let bytes = std::fs::read(&path).map_err(|e| io_error(&path.display().to_string(), e))?;
        let relative = path
            .strip_prefix(root)
            .map_err(|e| corrupt(e.to_string()))?
            .components()
            .map(|component| component.as_os_str().to_string_lossy().into_owned())
            .collect::<Vec<_>>()
            .join("/");
        objects.push(ObjectWitnessV1 {
            path: relative,
            bytes: metadata.len(),
            version: sha256_bytes(&bytes),
        });
        if objects.len() > MAX_LISTED_OBJECTS {
            return Err(corrupt(format!(
                "{} lists more than {MAX_LISTED_OBJECTS} objects",
                root.display()
            )));
        }
    }
    Ok(())
}

fn unix_time_millis() -> RunnerResult<u64> {
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| RunnerError::new("system_clock_invalid", e.to_string()))?;
    u64::try_from(elapsed.as_millis())
        .map_err(|e| RunnerError::new("system_clock_invalid", e.to_string()))
}

fn env_var_truthy(name: &str) -> bool {
    std::env::var(name)
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes"
            )
        })
        .unwrap_or(false)
}

/// The listing client of one bucket from the `AWS_*` environment the engine's
/// own S3 storage reads: the endpoint override, plain HTTP where it asks for it,
/// path-style requests under `AWS_S3_FORCE_PATH_STYLE`.
fn s3_client(bucket: &str) -> RunnerResult<object_store::aws::AmazonS3> {
    let mut builder = object_store::aws::AmazonS3Builder::from_env().with_bucket_name(bucket);
    if let Some(endpoint) = std::env::var("AWS_ENDPOINT_URL_S3")
        .ok()
        .or_else(|| std::env::var("AWS_ENDPOINT_URL").ok())
    {
        builder = builder.with_endpoint(&endpoint);
        if endpoint.starts_with("http://") || env_var_truthy("AWS_ALLOW_HTTP") {
            builder = builder.with_allow_http(true);
        }
    }
    if env_var_truthy("AWS_S3_FORCE_PATH_STYLE") {
        builder = builder.with_virtual_hosted_style_request(false);
    }
    builder
        .build()
        .map_err(|e| RunnerError::new("dataset_store_unreachable", format!("s3://{bucket}: {e}")))
}

fn write_executable(destination: &Path, bytes: &[u8]) -> RunnerResult<()> {
    use std::io::Write;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o755);
    }
    let mut file = options
        .open(destination)
        .map_err(|e| io_error(&destination.display().to_string(), e))?;
    file.write_all(bytes)
        .and_then(|_| file.sync_all())
        .map_err(|e| io_error(&destination.display().to_string(), e))
}

fn invalid(message: String) -> RunnerError {
    RunnerError::new("invalid_store", message)
}

fn corrupt(message: String) -> RunnerError {
    RunnerError::new("dataset_store_corrupt", message)
}

fn storage_error(uri: &str, error: impl std::fmt::Display) -> RunnerError {
    RunnerError::new("dataset_store_io", format!("{uri}: {error}"))
}

fn io_error(path: &str, error: std::io::Error) -> RunnerError {
    RunnerError::new("dataset_store_io", format!("{path}: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use omnigraph::db::Omnigraph;

    fn tiny_plan() -> DatasetBuildPlan {
        crate::gqt_case::dataset_file(
            &Path::new(env!("CARGO_MANIFEST_DIR")).join("../../benchmarks/fixtures/tiny_graph.gqt"),
            None,
            crate::case::Backend::LocalFs {
                filesystem: crate::case::LocalFilesystem::Apfs,
                storage_class: crate::case::LocalStorageClass::NvmeSsd,
            },
            crate::case::ResetMode::Clone,
        )
        .unwrap()
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    #[test]
    fn a_store_location_is_a_path_or_a_bucket_prefix() {
        let directory = tempfile::tempdir().unwrap();
        let local = StoreLocation::parse(directory.path().join("store").to_str().unwrap()).unwrap();
        assert!(matches!(&local, StoreLocation::Local { path } if path.ends_with("store")));
        assert!(directory.path().join("store").is_dir());
        let s3 = StoreLocation::parse("s3://bucket-1/bench/").unwrap();
        assert_eq!(
            s3,
            StoreLocation::S3 {
                bucket: "bucket-1".into(),
                prefix: "bench".into()
            }
        );
        assert_eq!(s3.uri(), "s3://bucket-1/bench");
        assert_eq!(
            StoreLocation::parse("s3://bucket-1").unwrap().uri(),
            "s3://bucket-1"
        );
        for text in ["s3://", "s3://Bucket/x", "s3://b/a//b", "gs://bucket/x"] {
            assert_eq!(
                StoreLocation::parse(text).unwrap_err().code,
                "invalid_store",
                "{text}"
            );
        }
    }

    #[test]
    fn the_base_key_names_the_recipe_the_place_and_the_format_never_the_commit() {
        let plan = tiny_plan();
        let here = StoreLocation::S3 {
            bucket: "b".into(),
            prefix: "bench".into(),
        };
        let format = StorageFormatV1::current();
        let key = base_key(&plan, &here, &format, None).unwrap();
        assert_eq!(key, base_key(&plan, &here, &format, None).unwrap());
        let mut other_stamp = format.clone();
        other_stamp.internal_schema_version += 1;
        assert_ne!(key, base_key(&plan, &here, &other_stamp, None).unwrap());
        let mut other_lance = format.clone();
        other_lance.lance_version.push_str("-next");
        assert_ne!(key, base_key(&plan, &here, &other_lance, None).unwrap());
        // A change in what analysis writes, in either measured fingerprint
        // of any profile or in the certificate format, is a new base.
        assert_eq!(format.fts_certificate.profiles.len(), 3);
        for profile in 0..format.fts_certificate.profiles.len() {
            for native in [true, false] {
                let mut other_analysis = format.clone();
                let measured = &mut other_analysis.fts_certificate.profiles[profile];
                if native {
                    measured.native.push_str("-next");
                } else {
                    measured.analyzer.push_str("-next");
                }
                assert_ne!(key, base_key(&plan, &here, &other_analysis, None).unwrap());
            }
        }
        let mut other_certificate = format.clone();
        other_certificate.fts_certificate.format_version += 1;
        assert_ne!(
            key,
            base_key(&plan, &here, &other_certificate, None).unwrap()
        );
        let elsewhere = StoreLocation::S3 {
            bucket: "b".into(),
            prefix: "bench-2".into(),
        };
        assert_ne!(key, base_key(&plan, &elsewhere, &format, None).unwrap());
        assert_ne!(
            key,
            base_key(&plan, &here, &format, Some(&"c".repeat(64))).unwrap()
        );
        assert!(
            !serde_json::to_string(&BaseKeyTerms {
                algorithm: BASE_KEY_ALGORITHM,
                recipe_sha256: &plan.recipe_sha256,
                needs_indices: plan.needs_indices,
                store: &here,
                format: &format,
                registered_tree_sha256: None,
            })
            .unwrap()
            .contains(GQT_ENGINE_DIGEST)
        );
    }

    #[test]
    fn build_keys_are_full_shas_and_plain_names() {
        let sha = "a".repeat(40);
        BuildKey {
            commit: &sha,
            target_triple: "aarch64-apple-darwin",
            product: "omnigraph",
        }
        .validate()
        .unwrap();
        for (commit, triple, product) in [
            ("abc", "aarch64-apple-darwin", "omnigraph"),
            (sha.as_str(), "x86_64/linux", "omnigraph"),
            (sha.as_str(), "aarch64-apple-darwin", "../omnigraph"),
        ] {
            assert_eq!(
                BuildKey {
                    commit,
                    target_triple: triple,
                    product
                }
                .validate()
                .unwrap_err()
                .code,
                "invalid_store"
            );
        }
    }

    /// The store's whole contract over one location, every §Phases step 5 row
    /// in one sequence, the destructive rows (a changed object, a dropped
    /// manifest, a held lease) last.
    fn exercise(location: &str) {
        let runtime = runtime();
        let scratch = tempfile::tempdir().unwrap();
        let store = FixtureStore::open(location).unwrap();
        let plan = tiny_plan();
        let base = runtime
            .block_on(store.acquire_base(&plan, scratch.path(), None, false))
            .unwrap();
        assert!(!base.cache_hit);
        assert!(!base.manifest.objects.is_empty());
        assert_eq!(base.manifest.engine_digest, GQT_ENGINE_DIGEST);
        assert_eq!(base.manifest.storage_format, StorageFormatV1::current());
        assert!(
            runtime
                .block_on(store.witness(&format!("{}/{BASE_LEASE}", store.entry_uri(&base.key))))
                .unwrap()
                .is_empty(),
            "the lease is gone once the manifest is up"
        );
        let again = runtime
            .block_on(store.acquire_base(&plan, scratch.path(), None, true))
            .unwrap();
        assert!(again.cache_hit);
        assert_eq!(again.manifest, base.manifest);
        assert_eq!(again.root_uri, base.root_uri);

        let run = "01JRUN";
        for repetition in 1..=10 {
            let clone = runtime
                .block_on(store.empty_clone(run, repetition))
                .unwrap();
            assert!(clone.uri.ends_with(&format!("{run}-{repetition:08}")));
            assert!(
                runtime
                    .block_on(store.witness(&clone.uri))
                    .unwrap()
                    .is_empty()
            );
            runtime.block_on(async {
                let db = Omnigraph::open_clone(&base.root_uri, &clone.uri)
                    .await
                    .unwrap();
                db.branch_create(&format!("rep-{repetition}"))
                    .await
                    .unwrap();
                assert!(db.branch_list().await.unwrap().len() >= 2);
            });
            assert!(
                !runtime
                    .block_on(store.witness(&clone.uri))
                    .unwrap()
                    .is_empty(),
                "the write landed under the clone"
            );
            assert_eq!(
                runtime
                    .block_on(store.empty_clone(run, repetition))
                    .unwrap_err()
                    .code,
                "clone_root_not_empty"
            );
            runtime.block_on(store.delete_clone(&clone)).unwrap();
            assert!(
                runtime
                    .block_on(store.witness(&clone.uri))
                    .unwrap()
                    .is_empty()
            );
        }
        runtime.block_on(store.verify_base(&base)).unwrap();
        let after = runtime
            .block_on(store.acquire_base(&plan, scratch.path(), None, true))
            .unwrap();
        assert_eq!(after.manifest.objects, base.manifest.objects);
        assert_eq!(
            runtime
                .block_on(crate::dataset_identity::observe_uri(&base.root_uri))
                .unwrap()
                .branches
                .iter()
                .map(|branch| branch.name.as_str())
                .collect::<Vec<_>>(),
            base.manifest
                .summary
                .branches
                .iter()
                .map(|branch| branch.name.as_str())
                .collect::<Vec<_>>(),
            "no repetition's branch reached the base"
        );

        let entry = store.entry_uri(&base.key);
        let manifest_uri = format!("{entry}/{BASE_MANIFEST}");
        let mut other_harness = base.manifest.clone();
        other_harness.engine_digest = "b".repeat(64);
        runtime
            .block_on(store.storage.write_text(
                &manifest_uri,
                &serde_json::to_string(&other_harness).unwrap(),
            ))
            .unwrap();
        let built_elsewhere = runtime
            .block_on(store.acquire_base(&plan, scratch.path(), None, true))
            .unwrap();
        assert!(
            built_elsewhere.cache_hit,
            "the harness build is recorded, not keyed"
        );
        assert_eq!(built_elsewhere.manifest.engine_digest, "b".repeat(64));

        let record = runtime.block_on(crate::gqt_tests::authority_fixture());
        let receipt = runtime.block_on(store.put_record(&record)).unwrap();
        assert!(
            receipt
                .uri
                .ends_with(&format!("{}.json", receipt.record_sha256))
        );
        assert_eq!(
            runtime
                .block_on(store.put_record(&record))
                .unwrap_err()
                .code,
            "record_exists"
        );
        let listed = runtime.block_on(store.list_records()).unwrap();
        assert_eq!(listed.len(), 1);
        let bytes = runtime.block_on(store.read_record(&listed[0])).unwrap();
        assert_eq!(sha256_bytes(&bytes), receipt.record_sha256);

        let binary = scratch.path().join("omnigraph");
        std::fs::write(&binary, b"#!/bin/sh\nexit 0\n").unwrap();
        let key = BuildKey {
            commit: &"f".repeat(40),
            target_triple: "aarch64-apple-darwin",
            product: "omnigraph",
        };
        let resolved = scratch.path().join("resolved");
        assert_eq!(
            runtime
                .block_on(store.put_build(key, &binary, false))
                .unwrap_err()
                .code,
            "dirty_tree_not_cached"
        );
        assert_eq!(
            runtime
                .block_on(store.resolve_build(key, &resolved))
                .unwrap(),
            None
        );
        assert!(
            runtime
                .block_on(store.put_build(key, &binary, true))
                .unwrap()
        );
        assert!(
            !runtime
                .block_on(store.put_build(key, &binary, true))
                .unwrap()
        );
        assert_eq!(
            runtime
                .block_on(store.resolve_build(key, &resolved))
                .unwrap(),
            Some(resolved.clone())
        );
        assert_eq!(std::fs::read(&resolved).unwrap(), b"#!/bin/sh\nexit 0\n");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&resolved).unwrap().permissions().mode() & 0o111,
                0o111
            );
        }

        let changed = &base.manifest.objects[0];
        runtime
            .block_on(store.storage.write_bytes(
                &format!("{}/{}", base.root_uri, changed.path),
                b"not the base",
            ))
            .unwrap();
        let refused = runtime
            .block_on(store.acquire_base(&plan, scratch.path(), None, true))
            .unwrap_err();
        assert_eq!(refused.code, "dataset_store_corrupt", "{}", refused.message);
        assert!(
            refused.message.contains(&changed.path),
            "{}",
            refused.message
        );

        runtime
            .block_on(store.storage.delete(&manifest_uri))
            .unwrap();
        let unpublished = runtime
            .block_on(store.acquire_base(&plan, scratch.path(), None, false))
            .unwrap_err();
        assert_eq!(
            unpublished.code, "dataset_store_unpublished",
            "{}",
            unpublished.message
        );
        assert_eq!(
            runtime
                .block_on(store.acquire_base(&plan, scratch.path(), None, true))
                .unwrap_err()
                .code,
            "dataset_store_miss"
        );
        runtime
            .block_on(store.storage.delete_prefix(&entry))
            .unwrap();
        runtime
            .block_on(
                store
                    .storage
                    .write_text(&format!("{entry}/{BASE_LEASE}"), "{\"held\":true}"),
            )
            .unwrap();
        assert_eq!(
            runtime
                .block_on(store.acquire_base(&plan, scratch.path(), None, false))
                .unwrap_err()
                .code,
            "dataset_store_busy"
        );
    }

    #[test]
    fn a_local_store_keeps_bases_clones_records_and_builds() {
        let directory = tempfile::tempdir().unwrap();
        exercise(directory.path().join("store").to_str().unwrap());
    }

    #[test]
    fn rustfs_integration_a_bucket_store_keeps_bases_clones_records_and_builds() {
        let Ok(bucket) = std::env::var("OMNIGRAPH_S3_TEST_BUCKET") else {
            eprintln!("skipping s3 store: OMNIGRAPH_S3_TEST_BUCKET is not set");
            return;
        };
        let prefix = std::env::var("OMNIGRAPH_S3_TEST_PREFIX")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "omnigraph-itests".to_string());
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let location = format!("s3://{bucket}/{prefix}/bench-store/{unique}");
        exercise(&location);
        runtime().block_on(async {
            storage_for_uri(&location)
                .unwrap()
                .delete_prefix(&location)
                .await
                .unwrap();
        });
    }
}
