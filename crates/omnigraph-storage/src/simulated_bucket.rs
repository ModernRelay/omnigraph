//! A cloud bucket in memory, for tests and the DST harness. It holds the
//! objects of [`InMemory`] and, beside them, raw keys no [`Path`] names (a
//! folder marker `graph/`): a listing returns such a key as the S3 client
//! parses it, and no other request reaches it. A bucket answers to names of
//! one [`Provider`], and this crate's clients of that provider look a name up
//! here before they build a client; so does Lance's store of the scheme.
//!
//! The provider decides the conditional surface, the one way the two differ
//! here. An S3 bucket conditions a write on the ETag. A Google Cloud Storage
//! bucket conditions it on the object generation, reported in
//! [`ObjectMeta::version`] and [`PutResult::version`] by every write, a
//! completed multipart upload included. `object_store`'s GCS client sends
//! `x-goog-if-generation-match` from [`UpdateVersion::version`] alone and
//! ignores an ETag beside it, so a replace naming no generation is refused
//! and one naming both compares the generation. Its ETag is unrelated to the
//! generation, and a read conditioned on the ETag a GCS bucket reported
//! (`If-Match`, `If-None-Match`) compares that ETag.

use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::sync::{Arc, LazyLock, Mutex};

use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::memory::InMemory;
use object_store::path::{DELIMITER, Path};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt as _, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
    UpdateVersion,
};

/// The entity tag S3 gives an empty object: the MD5 of no bytes, quoted.
const EMPTY_OBJECT_ETAG: &str = "\"d41d8cd98f00b204e9800998ecf8427e\"";

/// The object store a bucket simulates: what its names are looked up by, and
/// its conditional surface (module description).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Provider {
    S3,
    Gcs,
}

impl Provider {
    /// The URI scheme the provider's roots use.
    pub fn scheme(self) -> &'static str {
        match self {
            Self::S3 => "s3",
            Self::Gcs => "gs",
        }
    }

    /// The provider whose roots use `scheme`, when one does.
    pub fn for_scheme(scheme: &str) -> Option<Self> {
        [Self::S3, Self::Gcs]
            .into_iter()
            .find(|provider| provider.scheme() == scheme)
    }
}

/// The buckets that answer, by provider and name.
type Named = HashMap<(Provider, String), Arc<SimulatedBucket>>;

static NAMED: LazyLock<Mutex<Named>> = LazyLock::new(Mutex::default);

/// The bucket of `provider` that answers to `name`, when one does.
pub fn bucket(provider: Provider, name: &str) -> Option<Arc<SimulatedBucket>> {
    NAMED
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(&(provider, name.to_owned()))
        .cloned()
}

/// A bucket in memory: see the module's description.
#[derive(Debug)]
pub struct SimulatedBucket {
    provider: Provider,
    objects: InMemory,
    raw: Mutex<BTreeSet<String>>,
}

/// The guard of one [`SimulatedBucket::answer_to`]: dropping it frees the names.
#[derive(Debug)]
pub struct BucketNames(Provider, Vec<String>);

impl Drop for BucketNames {
    fn drop(&mut self) {
        let mut named = NAMED
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for name in &self.1 {
            named.remove(&(self.0, name.clone()));
        }
    }
}

impl SimulatedBucket {
    pub fn new(provider: Provider) -> Arc<Self> {
        Arc::new(Self {
            provider,
            objects: InMemory::new(),
            raw: Mutex::default(),
        })
    }

    pub fn provider(&self) -> Provider {
        self.provider
    }

    /// Answer to each of `names` until the guard is dropped. One bucket under
    /// two names is how two bucket names address one bucket.
    ///
    /// # Errors
    /// When a bucket already answers to one of the names; none is then taken.
    pub fn answer_to(self: &Arc<Self>, names: &[&str]) -> std::result::Result<BucketNames, String> {
        let mut named = NAMED
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let key = |name: &str| (self.provider, name.to_owned());
        if let Some(taken) = names.iter().find(|name| named.contains_key(&key(name))) {
            return Err(format!(
                "a simulated {}:// bucket already answers to '{taken}'",
                self.provider.scheme()
            ));
        }
        for name in names {
            named.insert(key(name), Arc::clone(self));
        }
        Ok(BucketNames(
            self.provider,
            names.iter().map(|name| (*name).to_owned()).collect(),
        ))
    }

    /// Put an empty object at the raw key `key`: an object like any other when
    /// a [`Path`] names the key, and otherwise one a listing alone returns.
    pub async fn put_empty(&self, key: &str) -> Result<()> {
        match Path::parse(key) {
            Ok(path) if path.as_ref() == key => {
                self.objects.put(&path, PutPayload::default()).await?;
            }
            _ => {
                self.raw
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .insert(key.to_owned());
            }
        }
        Ok(())
    }

    /// The raw keys an S3 listing of `prefix` returns, each beside what the
    /// client makes of it.
    fn raw_under(&self, prefix: Option<&Path>) -> Vec<(String, Result<ObjectMeta>)> {
        let under = listed_prefix(prefix);
        self.raw
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .filter(|key| key.starts_with(&under))
            .map(|key| (key.clone(), listed(key).map(|meta| self.reported(meta))))
            .collect()
    }

    fn reported(&self, meta: ObjectMeta) -> ObjectMeta {
        reported_by(self.provider, meta)
    }

    /// The precondition `options` names, translated to the token the store
    /// compares; a GCS replace naming no generation is refused as the GCS
    /// client refuses it, and an ETag beside a generation is ignored, as
    /// that client ignores it.
    fn compared(&self, mut options: PutOptions) -> Result<PutOptions> {
        if self.provider != Provider::Gcs {
            return Ok(options);
        }
        if let PutMode::Update(version) = &options.mode {
            let Some(generation) = version.version.clone() else {
                return Err(object_store::Error::NotSupported {
                    source: "Google Cloud Storage conditions a replace on the object \
                             generation (UpdateVersion::version)"
                        .into(),
                });
            };
            options.mode = PutMode::Update(UpdateVersion {
                e_tag: Some(generation),
                version: None,
            });
        }
        Ok(options)
    }
}

/// `meta` as `provider` reports it: on GCS the store's own token is the
/// generation, and the ETag is another value.
fn reported_by(provider: Provider, mut meta: ObjectMeta) -> ObjectMeta {
    if provider == Provider::Gcs {
        meta.version = meta.e_tag.take();
        meta.e_tag = meta.version.as_deref().map(gcs_etag);
    }
    meta
}

/// A write's result as `provider` reports it, as [`reported_by`] reports
/// metadata.
fn reported_write(provider: Provider, mut result: PutResult) -> PutResult {
    if provider == Provider::Gcs {
        result.version = result.e_tag.take();
        result.e_tag = result.version.as_deref().map(gcs_etag);
    }
    result
}

const GCS_ETAG_PREFIX: &str = "\"gcs-etag-";

/// The ETag a simulated GCS bucket reports for the object at `generation`:
/// a value of its own, so a client that conditions a write on it cannot pass.
fn gcs_etag(generation: &str) -> String {
    format!("{GCS_ETAG_PREFIX}{}\"", generation.trim_matches('"'))
}

/// The generation a reported GCS ETag stands for; any other value is left
/// as it is, and matches no object.
fn generation_of(e_tag: &str) -> String {
    match e_tag
        .strip_prefix(GCS_ETAG_PREFIX)
        .and_then(|rest| rest.strip_suffix('"'))
    {
        Some(generation) => format!("\"{generation}\""),
        None => e_tag.to_owned(),
    }
}

/// Every ETag a read precondition lists, translated to the token the store
/// compares (`*` and unknown values unchanged).
fn compared_etags(list: &str) -> String {
    list.split(',')
        .map(|e_tag| generation_of(e_tag.trim()))
        .collect::<Vec<_>>()
        .join(", ")
}

/// A multipart upload to a GCS bucket, whose completion reports the
/// generation and ETag a single-request write reports.
#[derive(Debug)]
struct ReportedUpload {
    provider: Provider,
    upload: Box<dyn MultipartUpload>,
}

#[async_trait]
impl MultipartUpload for ReportedUpload {
    fn put_part(&mut self, data: PutPayload) -> object_store::UploadPart {
        self.upload.put_part(data)
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let result = self.upload.complete().await?;
        Ok(reported_write(self.provider, result))
    }

    async fn abort(&mut self) -> Result<()> {
        self.upload.abort().await
    }
}

/// The key prefix the S3 client sends for a listing of `prefix`.
fn listed_prefix(prefix: Option<&Path>) -> String {
    prefix
        .filter(|prefix| !prefix.as_ref().is_empty())
        .map(|prefix| format!("{prefix}{DELIMITER}"))
        .unwrap_or_default()
}

/// The metadata the S3 client makes of a listed empty object at `key`, or the
/// error with which it fails the listing.
fn listed(key: &str) -> Result<ObjectMeta> {
    Ok(ObjectMeta {
        location: Path::parse(key)?,
        last_modified: Default::default(),
        size: 0,
        e_tag: Some(EMPTY_OBJECT_ETAG.to_owned()),
        version: None,
    })
}

impl fmt::Display for SimulatedBucket {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "SimulatedBucket({})", self.objects)
    }
}

#[async_trait]
impl ObjectStore for SimulatedBucket {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult> {
        let options = self.compared(options)?;
        let result = self.objects.put_opts(location, payload, options).await?;
        Ok(reported_write(self.provider, result))
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let upload = self.objects.put_multipart_opts(location, options).await?;
        Ok(Box::new(ReportedUpload {
            provider: self.provider,
            upload,
        }))
    }

    async fn get_opts(&self, location: &Path, mut options: GetOptions) -> Result<GetResult> {
        if self.provider == Provider::Gcs {
            options.if_match = options.if_match.as_deref().map(compared_etags);
            options.if_none_match = options.if_none_match.as_deref().map(compared_etags);
        }
        let mut result = self.objects.get_opts(location, options).await?;
        result.meta = self.reported(result.meta);
        Ok(result)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.objects.delete_stream(locations)
    }

    /// In key order, as S3 lists: a client may rely on it.
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let raw = self.raw_under(prefix);
        let provider = self.provider;
        let named = self
            .objects
            .list(prefix)
            .map_ok(move |meta| reported_by(provider, meta))
            .boxed();
        if raw.is_empty() {
            return named;
        }
        futures::stream::once(async move {
            let mut entries: Vec<(String, Result<ObjectMeta>)> = named
                .map_ok(|meta| (meta.location.to_string(), Ok(meta)))
                .try_collect()
                .await?;
            entries.extend(raw);
            entries.sort_by(|one, other| one.0.cmp(&other.0));
            Ok::<_, object_store::Error>(futures::stream::iter(
                entries.into_iter().map(|(_, entry)| entry),
            ))
        })
        .try_flatten()
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let mut level = self.objects.list_with_delimiter(prefix).await?;
        level.objects = level
            .objects
            .into_iter()
            .map(|meta| self.reported(meta))
            .collect();
        let under = listed_prefix(prefix).len();
        for (key, entry) in self.raw_under(prefix) {
            match key[under..].find(DELIMITER) {
                Some(delimiter) => {
                    let common = Path::parse(&key[..under + delimiter])?;
                    if !level.common_prefixes.contains(&common) {
                        level.common_prefixes.push(common);
                    }
                }
                None => level.objects.push(entry?),
            }
        }
        level.common_prefixes.sort();
        level
            .objects
            .sort_by(|one, other| one.location.cmp(&other.location));
        Ok(level)
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.objects.copy_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn locations(listed: &[ObjectMeta]) -> Vec<&str> {
        listed.iter().map(|meta| meta.location.as_ref()).collect()
    }

    /// What an S3 listing makes of a folder marker, a key `InMemory` cannot hold.
    #[tokio::test]
    async fn a_folder_marker_is_listed_at_its_parsed_key_and_never_read() {
        let bucket = SimulatedBucket::new(Provider::S3);
        bucket.put_empty("graph/").await.unwrap();
        bucket.put_empty("graph/object").await.unwrap();

        let all: Vec<ObjectMeta> = bucket.list(None).try_collect().await.unwrap();
        assert_eq!(locations(&all), ["graph", "graph/object"]);
        let graph = Path::from("graph");
        let under: Vec<ObjectMeta> = bucket.list(Some(&graph)).try_collect().await.unwrap();
        assert_eq!(locations(&under), ["graph", "graph/object"]);

        let read = bucket.head(&graph).await;
        assert!(
            matches!(read, Err(object_store::Error::NotFound { .. })),
            "{read:?}"
        );
        bucket.head(&Path::from("graph/object")).await.unwrap();

        let level = bucket.list_with_delimiter(Some(&graph)).await.unwrap();
        assert_eq!(locations(&level.objects), ["graph", "graph/object"]);
        let top = bucket.list_with_delimiter(None).await.unwrap();
        assert!(top.objects.is_empty());
        assert_eq!(top.common_prefixes, [graph]);
    }

    #[tokio::test]
    async fn a_key_no_path_parses_fails_the_listing() {
        let bucket = SimulatedBucket::new(Provider::S3);
        bucket.put_empty("a//b").await.unwrap();
        bucket.put_empty("z").await.unwrap();
        let listed: Result<Vec<ObjectMeta>> = bucket.list(None).try_collect().await;
        assert!(
            matches!(listed, Err(object_store::Error::InvalidPath { .. })),
            "{listed:?}"
        );
    }

    #[tokio::test]
    async fn one_bucket_answers_to_its_names_until_the_guard_drops() {
        let held = SimulatedBucket::new(Provider::S3);
        let names = held
            .answer_to(&["simulated-names-a", "simulated-names-b"])
            .unwrap();
        for name in ["simulated-names-a", "simulated-names-b"] {
            assert!(
                Arc::ptr_eq(&bucket(Provider::S3, name).unwrap(), &held),
                "{name}"
            );
            assert!(bucket(Provider::Gcs, name).is_none(), "{name}");
        }
        let refused = SimulatedBucket::new(Provider::S3)
            .answer_to(&["simulated-names-c", "simulated-names-a"]);
        assert!(refused.is_err());
        assert!(
            bucket(Provider::S3, "simulated-names-c").is_none(),
            "a refused call takes no name"
        );
        drop(names);
        assert!(bucket(Provider::S3, "simulated-names-a").is_none());
    }

    fn replace(e_tag: Option<&str>, version: Option<&str>) -> PutOptions {
        PutMode::Update(UpdateVersion {
            e_tag: e_tag.map(str::to_owned),
            version: version.map(str::to_owned),
        })
        .into()
    }

    /// A GCS bucket conditions a replace on the generation alone: an ETag
    /// beside it is ignored, and a replace naming no generation is refused,
    /// as the GCS client sends only `x-goog-if-generation-match`.
    #[tokio::test]
    async fn a_gcs_bucket_conditions_a_replace_on_the_generation() {
        let bucket = SimulatedBucket::new(Provider::Gcs);
        let path = Path::from("graph/state.json");
        let first = bucket.put(&path, PutPayload::from("s1")).await.unwrap();
        let generation = first
            .version
            .clone()
            .expect("a write reports its generation");
        let e_tag = first.e_tag.clone().expect("a write reports its ETag");
        assert_ne!(e_tag, generation);

        let head = bucket.head(&path).await.unwrap();
        assert_eq!(head.version.as_ref(), Some(&generation));
        assert_eq!(head.e_tag.as_ref(), Some(&e_tag));
        let listed: Vec<ObjectMeta> = bucket.list(None).try_collect().await.unwrap();
        assert_eq!(listed[0].version.as_ref(), Some(&generation));

        for absent in [replace(Some(&e_tag), None), replace(None, None)] {
            let refused = bucket.put_opts(&path, PutPayload::from("s2"), absent).await;
            assert!(
                matches!(refused, Err(object_store::Error::NotSupported { .. })),
                "{refused:?}"
            );
        }
        let second = bucket
            .put_opts(
                &path,
                PutPayload::from("s2"),
                replace(Some(&e_tag), Some(&generation)),
            )
            .await
            .unwrap();
        assert_ne!(second.version, Some(generation.clone()));
        let current = second.version.clone().unwrap();
        for stale in [
            replace(None, Some(&generation)),
            replace(second.e_tag.as_deref(), Some(&generation)),
        ] {
            let refused = bucket.put_opts(&path, PutPayload::from("s3"), stale).await;
            assert!(
                matches!(refused, Err(object_store::Error::Precondition { .. })),
                "{refused:?}"
            );
        }
        bucket
            .put_opts(
                &path,
                PutPayload::from("s3"),
                replace(Some("\"unrelated\""), Some(&current)),
            )
            .await
            .unwrap();
    }

    /// A read conditioned on the ETag a GCS bucket reported passes while the
    /// object is unchanged and fails once it changes.
    #[tokio::test]
    async fn a_gcs_read_compares_the_reported_etag() {
        let bucket = SimulatedBucket::new(Provider::Gcs);
        let path = Path::from("graph/table.lance");
        bucket.put(&path, PutPayload::from("abcdef")).await.unwrap();
        let head = bucket.head(&path).await.unwrap();
        let e_tag = head.e_tag.clone().unwrap();

        let range = GetOptions {
            if_match: Some(e_tag.clone()),
            range: Some((1..3).into()),
            ..Default::default()
        };
        let read = bucket.get_opts(&path, range.clone()).await.unwrap();
        assert_eq!(read.meta.e_tag.as_ref(), Some(&e_tag));
        assert_eq!(read.bytes().await.unwrap().as_ref(), b"bc");
        let unchanged = GetOptions {
            if_none_match: Some(format!("\"other\", {e_tag}")),
            ..Default::default()
        };
        let not_modified = bucket.get_opts(&path, unchanged).await;
        assert!(
            matches!(not_modified, Err(object_store::Error::NotModified { .. })),
            "{not_modified:?}"
        );

        bucket.put(&path, PutPayload::from("ghijkl")).await.unwrap();
        let changed = bucket.get_opts(&path, range).await;
        assert!(
            matches!(changed, Err(object_store::Error::Precondition { .. })),
            "{changed:?}"
        );
    }

    /// A completed multipart upload reports what a single write reports: the
    /// generation, and the ETag the next read of the object gives.
    #[tokio::test]
    async fn a_gcs_multipart_upload_reports_like_a_single_write() {
        let bucket = SimulatedBucket::new(Provider::Gcs);
        let path = Path::from("graph/data/part.lance");
        let mut upload = bucket.put_multipart(&path).await.unwrap();
        upload.put_part(PutPayload::from("ab")).await.unwrap();
        upload.put_part(PutPayload::from("cd")).await.unwrap();
        let completed = upload.complete().await.unwrap();
        let generation = completed
            .version
            .clone()
            .expect("a completed upload reports its generation");

        let head = bucket.head(&path).await.unwrap();
        assert_eq!(head.version.as_ref(), Some(&generation));
        assert_eq!(head.e_tag, completed.e_tag);
        bucket
            .put_opts(
                &path,
                PutPayload::from("ef"),
                replace(None, Some(&generation)),
            )
            .await
            .unwrap();
    }
}
