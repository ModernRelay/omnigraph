//! An S3 bucket in memory, for tests and the DST harness. It holds the objects
//! of [`InMemory`] and, beside them, raw keys no [`Path`] names (a folder
//! marker `graph/`): a listing returns such a key as the S3 client parses it,
//! and no other request reaches it. A bucket answers to names, and this
//! crate's S3 clients look a name up here before they build a client.

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
    ObjectStoreExt as _, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};

/// The entity tag S3 gives an empty object: the MD5 of no bytes, quoted.
const EMPTY_OBJECT_ETAG: &str = "\"d41d8cd98f00b204e9800998ecf8427e\"";

static NAMED: LazyLock<Mutex<HashMap<String, Arc<SimulatedBucket>>>> =
    LazyLock::new(Mutex::default);

/// The bucket that answers to `name`, when one does.
pub fn bucket(name: &str) -> Option<Arc<SimulatedBucket>> {
    NAMED
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(name)
        .cloned()
}

/// An S3 bucket in memory: see the module's description.
#[derive(Debug, Default)]
pub struct SimulatedBucket {
    objects: InMemory,
    raw: Mutex<BTreeSet<String>>,
}

/// The guard of one [`SimulatedBucket::answer_to`]: dropping it frees the names.
#[derive(Debug)]
pub struct BucketNames(Vec<String>);

impl Drop for BucketNames {
    fn drop(&mut self) {
        let mut named = NAMED
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for name in &self.0 {
            named.remove(name);
        }
    }
}

impl SimulatedBucket {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
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
        if let Some(taken) = names.iter().find(|name| named.contains_key(**name)) {
            return Err(format!("a simulated bucket already answers to '{taken}'"));
        }
        for name in names {
            named.insert((*name).to_owned(), Arc::clone(self));
        }
        Ok(BucketNames(
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
            .map(|key| (key.clone(), listed(key)))
            .collect()
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
        self.objects.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.objects.put_multipart_opts(location, options).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.objects.get_opts(location, options).await
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
        let named = self.objects.list(prefix);
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
        let bucket = SimulatedBucket::new();
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
        let bucket = SimulatedBucket::new();
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
        let held = SimulatedBucket::new();
        let names = held
            .answer_to(&["simulated-names-a", "simulated-names-b"])
            .unwrap();
        for name in ["simulated-names-a", "simulated-names-b"] {
            assert!(Arc::ptr_eq(&bucket(name).unwrap(), &held), "{name}");
        }
        let refused = SimulatedBucket::new().answer_to(&["simulated-names-c", "simulated-names-a"]);
        assert!(refused.is_err());
        assert!(
            bucket("simulated-names-c").is_none(),
            "a refused call takes no name"
        );
        drop(names);
        assert!(bucket("simulated-names-a").is_none());
    }
}
