//! A store over a frozen directory: reads fall through to the directory,
//! every write stays in a memory layer above it, and the directory is never
//! opened for writing. `omnigraph-gqt --store <dir>` under DST runs on one of
//! these in each of the engine's realms, so `--store` names the directory in
//! every mode and no copy of it is made.
//!
//! The layer is any [`ObjectStore`]; the conditional puts keep their meaning
//! over an object only the directory holds: a create refuses it, an update
//! compares the caller's token against the directory object's ETag and lands
//! in the layer as a create, so of two racing updates exactly one wins. A
//! delete masks the directory's object for the rest of the run. A listing is
//! the union of the layer and the unmasked directory, sorted by key, so two
//! runs of one seed list in one order. The directory's metadata is indexed
//! once at open: a head, an existence check or a listing never touches the
//! disk, only an object's bytes do, so under the simulation's paused clock a
//! directory object costs what a memory object costs until its body is read.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::ops::Range;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{
    CopyMode, CopyOptions, Error, GetOptions, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt as _, PutMode, PutMultipartOptions,
    PutOptions, PutPayload, PutResult, Result,
};

/// A directory opened for reading only. The file-system store is private and
/// only its read methods are called, so no write reaches the directory;
/// `objects` is its metadata, indexed once at open, and `deleted` holds every
/// directory key a store above deleted.
#[derive(Debug)]
pub struct FrozenDirectory {
    files: LocalFileSystem,
    objects: BTreeMap<Path, ObjectMeta>,
    deleted: Mutex<BTreeSet<Path>>,
}

impl FrozenDirectory {
    /// Open `root` for reading and index its objects; it must exist and hold
    /// plain files and directories only, so a symlink anywhere under it is
    /// refused by name before anything is read through.
    pub async fn open(root: &std::path::Path) -> Result<Arc<Self>> {
        refuse_symlinks(root)?;
        let files = LocalFileSystem::new_with_prefix(root)?;
        let objects = files
            .list(None)
            .map_ok(|meta| (meta.location.clone(), meta))
            .try_collect()
            .await?;
        Ok(Arc::new(Self {
            files,
            objects,
            deleted: Mutex::new(BTreeSet::new()),
        }))
    }

    fn is_deleted(&self, key: &Path) -> bool {
        self.deleted
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains(key)
    }

    fn mark_deleted(&self, key: Path) {
        self.deleted
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(key);
    }

    /// The directory's object at `key`, `None` once deleted or never there.
    fn head(&self, key: &Path) -> Option<&ObjectMeta> {
        if self.is_deleted(key) {
            return None;
        }
        self.objects.get(key)
    }

    async fn get_opts(&self, key: &Path, options: GetOptions) -> Result<Option<GetResult>> {
        let Some(meta) = self.head(key) else {
            return Ok(None);
        };
        if options.head {
            options.check_preconditions(meta)?;
            return Ok(Some(GetResult {
                payload: GetResultPayload::Stream(futures::stream::empty().boxed()),
                meta: meta.clone(),
                range: 0..meta.size,
                attributes: Default::default(),
                extensions: Default::default(),
            }));
        }
        match self.files.get_opts(key, options).await {
            Ok(result) => Ok(Some(result)),
            Err(Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(error),
        }
    }

    async fn get_ranges(&self, key: &Path, ranges: &[Range<u64>]) -> Result<Option<Vec<Bytes>>> {
        if self.head(key).is_none() {
            return Ok(None);
        }
        match self.files.get_ranges(key, ranges).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Every unmasked object under `prefix`, the exact key excluded, sorted.
    fn list(&self, prefix: Option<&Path>) -> Vec<ObjectMeta> {
        let root = Path::default();
        let prefix = prefix.unwrap_or(&root);
        self.objects
            .range(prefix.clone()..)
            .take_while(|(key, _)| key.as_ref().starts_with(prefix.as_ref()))
            .filter(|(key, _)| {
                key.prefix_match(prefix)
                    .is_some_and(|mut parts| parts.next().is_some())
                    && !self.is_deleted(key)
            })
            .map(|(_, meta)| meta.clone())
            .collect()
    }

    fn list_with_delimiter(&self, prefix: Option<&Path>) -> ListResult {
        let root = Path::default();
        let prefix = prefix.unwrap_or(&root);
        let mut common_prefixes = BTreeSet::new();
        let mut objects = Vec::new();
        for (key, meta) in self.objects.range(prefix.clone()..) {
            if !key.as_ref().starts_with(prefix.as_ref()) {
                break;
            }
            let Some(mut parts) = key.prefix_match(prefix) else {
                continue;
            };
            let Some(first) = parts.next() else {
                continue;
            };
            if self.is_deleted(key) {
                continue;
            }
            if parts.next().is_some() {
                common_prefixes.insert(prefix.clone().join(first));
            } else {
                objects.push(meta.clone());
            }
        }
        ListResult {
            common_prefixes: common_prefixes.into_iter().collect(),
            objects,
            extensions: Default::default(),
        }
    }
}

/// Walk `dir` without following links and refuse the first symlink by name:
/// a read-through store must never read outside its root.
fn refuse_symlinks(dir: &std::path::Path) -> Result<()> {
    let generic = |source: std::io::Error| Error::Generic {
        store: "FrozenDirectory",
        source: Box::new(source),
    };
    for entry in std::fs::read_dir(dir).map_err(generic)? {
        let entry = entry.map_err(generic)?;
        let path = entry.path();
        let file_type = entry.file_type().map_err(generic)?;
        if file_type.is_symlink() {
            return Err(Error::Generic {
                store: "FrozenDirectory",
                source: format!(
                    "prepared store holds a symlink: {}; the root is read from plain files and directories only",
                    path.display()
                )
                .into(),
            });
        }
        if file_type.is_dir() {
            refuse_symlinks(&path)?;
        }
    }
    Ok(())
}

/// Where a store's key space meets the directory's: the requested prefix is
/// under the mount (its rest names a directory prefix), above it (the mount
/// is one of its children), or apart from it.
enum Mount {
    Under(Path),
    Above,
    Apart,
}

/// An [`ObjectStore`] whose reads fall through to a [`FrozenDirectory`]
/// mounted at `prefix` and whose writes land in `layer`.
#[derive(Debug)]
pub struct ReadThroughStore {
    layer: Arc<dyn ObjectStore>,
    frozen: Arc<FrozenDirectory>,
    prefix: Path,
}

impl ReadThroughStore {
    /// `frozen`'s objects appear under `prefix`; `layer` takes every write.
    pub fn new(layer: Arc<dyn ObjectStore>, frozen: Arc<FrozenDirectory>, prefix: Path) -> Self {
        Self {
            layer,
            frozen,
            prefix,
        }
    }

    /// The directory key `location` names, when it lies under the mount.
    fn directory_key(prefix: &Path, location: &Path) -> Option<Path> {
        location.prefix_match(prefix).map(Iterator::collect)
    }

    /// The store key of a directory key.
    fn mounted(prefix: &Path, key: &Path) -> Path {
        prefix.parts().chain(key.parts()).collect()
    }

    fn mount(&self, prefix: Option<&Path>) -> Mount {
        let requested = prefix.cloned().unwrap_or_default();
        if let Some(rest) = Self::directory_key(&self.prefix, &requested) {
            Mount::Under(rest)
        } else if self.prefix.prefix_matches(&requested) {
            Mount::Above
        } else {
            Mount::Apart
        }
    }

    /// The directory's object at `location` as the store presents it.
    fn directory_head(&self, location: &Path) -> Option<ObjectMeta> {
        let key = Self::directory_key(&self.prefix, location)?;
        self.frozen.head(&key).map(|meta| ObjectMeta {
            location: location.clone(),
            ..meta.clone()
        })
    }

    /// Every unmasked directory object the mount shows under a listing,
    /// keyed as the store presents it.
    fn directory_objects(&self, prefix: Option<&Path>) -> Vec<ObjectMeta> {
        let rest = match self.mount(prefix) {
            Mount::Under(rest) => Some(rest),
            Mount::Above => None,
            Mount::Apart => return Vec::new(),
        };
        self.frozen
            .list(rest.as_ref().filter(|rest| !rest.as_ref().is_empty()))
            .into_iter()
            .map(|meta| ObjectMeta {
                location: Self::mounted(&self.prefix, &meta.location),
                ..meta
            })
            .collect()
    }
}

fn not_found(location: &Path) -> Error {
    Error::NotFound {
        path: location.to_string(),
        source: "no such object in the layer or the directory".into(),
    }
}

fn precondition(location: &Path, reason: impl Into<String>) -> Error {
    Error::Precondition {
        path: location.to_string(),
        source: reason.into().into(),
    }
}

impl fmt::Display for ReadThroughStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ReadThrough({} over {} at {})",
            self.layer, self.frozen.files, self.prefix
        )
    }
}

#[async_trait]
impl ObjectStore for ReadThroughStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let PutOptions {
            mode,
            tags,
            attributes,
            extensions,
        } = opts;
        let layer_put = |mode: PutMode| PutOptions {
            mode,
            tags,
            attributes,
            extensions,
        };
        match mode {
            PutMode::Overwrite => {
                self.layer
                    .put_opts(location, payload, layer_put(PutMode::Overwrite))
                    .await
            }
            PutMode::Create => {
                if self.directory_head(location).is_some() {
                    return Err(Error::AlreadyExists {
                        path: location.to_string(),
                        source: "the directory holds this object".into(),
                    });
                }
                self.layer
                    .put_opts(location, payload, layer_put(PutMode::Create))
                    .await
            }
            PutMode::Update(version) => {
                match self.layer.head(location).await {
                    Ok(_) => {
                        return self
                            .layer
                            .put_opts(location, payload, layer_put(PutMode::Update(version)))
                            .await;
                    }
                    Err(Error::NotFound { .. }) => {}
                    Err(error) => return Err(error),
                }
                let Some(current) = self.directory_head(location) else {
                    return Err(precondition(location, "no object to update"));
                };
                let Some(expected) = version.e_tag else {
                    return Err(precondition(
                        location,
                        "an update names the ETag it expects",
                    ));
                };
                if current.e_tag.as_deref() != Some(expected.as_str()) {
                    return Err(precondition(
                        location,
                        format!("the directory object's ETag is not {expected}"),
                    ));
                }
                match self
                    .layer
                    .put_opts(location, payload, layer_put(PutMode::Create))
                    .await
                {
                    Err(Error::AlreadyExists { path, source }) => {
                        Err(Error::Precondition { path, source })
                    }
                    other => other,
                }
            }
        }
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.layer.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        match self.layer.get_opts(location, options.clone()).await {
            Err(Error::NotFound { .. }) => {}
            other => return other,
        }
        let Some(key) = Self::directory_key(&self.prefix, location) else {
            return Err(not_found(location));
        };
        let mut result = self
            .frozen
            .get_opts(&key, options)
            .await?
            .ok_or_else(|| not_found(location))?;
        result.meta.location = location.clone();
        Ok(result)
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        match self.layer.get_ranges(location, ranges).await {
            Err(Error::NotFound { .. }) => {}
            other => return other,
        }
        let Some(key) = Self::directory_key(&self.prefix, location) else {
            return Err(not_found(location));
        };
        self.frozen
            .get_ranges(&key, ranges)
            .await?
            .ok_or_else(|| not_found(location))
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let layer = Arc::clone(&self.layer);
        let frozen = Arc::clone(&self.frozen);
        let prefix = self.prefix.clone();
        locations
            .and_then(move |location| {
                let layer = Arc::clone(&layer);
                let frozen = Arc::clone(&frozen);
                let prefix = prefix.clone();
                async move {
                    match layer.delete(&location).await {
                        Ok(()) | Err(Error::NotFound { .. }) => {}
                        Err(error) => return Err(error),
                    }
                    if let Some(key) = Self::directory_key(&prefix, &location) {
                        frozen.mark_deleted(key);
                    }
                    Ok(location)
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let layer = self.layer.list(prefix);
        let mut merged: BTreeMap<Path, ObjectMeta> = self
            .directory_objects(prefix)
            .into_iter()
            .map(|meta| (meta.location.clone(), meta))
            .collect();
        futures::stream::once(async move {
            let mut layer = layer;
            while let Some(meta) = layer.try_next().await? {
                merged.insert(meta.location.clone(), meta);
            }
            Ok::<_, Error>(futures::stream::iter(merged.into_values().map(Ok)))
        })
        .try_flatten()
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let layer = self.layer.list_with_delimiter(prefix).await?;
        let mut objects: BTreeMap<Path, ObjectMeta> = BTreeMap::new();
        let mut common_prefixes: BTreeSet<Path> = layer.common_prefixes.into_iter().collect();
        match self.mount(prefix) {
            Mount::Apart => {}
            Mount::Above => {
                let requested = prefix.cloned().unwrap_or_default();
                let next = self
                    .prefix
                    .prefix_match(&requested)
                    .and_then(|mut parts| parts.next());
                if let Some(next) = next {
                    common_prefixes.insert(requested.join(next));
                }
            }
            Mount::Under(rest) => {
                let directory = self
                    .frozen
                    .list_with_delimiter(Some(&rest).filter(|rest| !rest.as_ref().is_empty()));
                for meta in directory.objects {
                    let location = Self::mounted(&self.prefix, &meta.location);
                    objects.insert(location.clone(), ObjectMeta { location, ..meta });
                }
                for common in directory.common_prefixes {
                    common_prefixes.insert(Self::mounted(&self.prefix, &common));
                }
            }
        }
        for meta in layer.objects {
            objects.insert(meta.location.clone(), meta);
        }
        Ok(ListResult {
            common_prefixes: common_prefixes.into_iter().collect(),
            objects: objects.into_values().collect(),
            extensions: layer.extensions,
        })
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        let source = self.get(from).await?;
        let attributes = source.attributes.clone();
        let bytes = source.bytes().await?;
        let mode = match options.mode {
            CopyMode::Overwrite => PutMode::Overwrite,
            CopyMode::Create => PutMode::Create,
        };
        self.put_opts(
            to,
            PutPayload::from(bytes),
            PutOptions {
                mode,
                attributes,
                ..PutOptions::default()
            },
        )
        .await
        .map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;
    use object_store::{GetRange, UpdateVersion};

    use super::*;

    async fn directory(files: &[(&str, &str)]) -> (tempfile::TempDir, Arc<FrozenDirectory>) {
        let dir = tempfile::tempdir().unwrap();
        for (name, contents) in files {
            let path = dir.path().join(name);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, contents).unwrap();
        }
        let frozen = FrozenDirectory::open(dir.path()).await.unwrap();
        (dir, frozen)
    }

    fn store(frozen: &Arc<FrozenDirectory>, prefix: &str) -> ReadThroughStore {
        ReadThroughStore::new(
            Arc::new(InMemory::new()),
            Arc::clone(frozen),
            Path::from(prefix),
        )
    }

    async fn text(store: &ReadThroughStore, key: &str) -> Option<String> {
        match store.get(&Path::from(key)).await {
            Ok(result) => Some(String::from_utf8(result.bytes().await.unwrap().to_vec()).unwrap()),
            Err(Error::NotFound { .. }) => None,
            Err(error) => panic!("{error}"),
        }
    }

    async fn keys(store: &ReadThroughStore, prefix: Option<&str>) -> Vec<String> {
        store
            .list(prefix.map(Path::from).as_ref())
            .map_ok(|meta| meta.location.to_string())
            .try_collect()
            .await
            .unwrap()
    }

    fn directory_files(dir: &tempfile::TempDir) -> Vec<(String, String)> {
        let mut out = Vec::new();
        for entry in walk(dir.path()) {
            let relative = entry
                .strip_prefix(dir.path())
                .unwrap()
                .to_string_lossy()
                .into_owned();
            out.push((relative, std::fs::read_to_string(&entry).unwrap()));
        }
        out.sort();
        out
    }

    fn walk(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
        let mut out = Vec::new();
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                out.extend(walk(&path));
            } else {
                out.push(path);
            }
        }
        out
    }

    #[tokio::test]
    async fn reads_fall_through_and_writes_stay_above() {
        let (dir, frozen) = directory(&[("a.txt", "base a"), ("sub/b.txt", "base b")]).await;
        let store = store(&frozen, "graph");
        assert_eq!(text(&store, "graph/a.txt").await.as_deref(), Some("base a"));
        assert_eq!(
            text(&store, "graph/sub/b.txt").await.as_deref(),
            Some("base b")
        );
        assert_eq!(text(&store, "graph/missing").await, None);
        assert_eq!(text(&store, "elsewhere/a.txt").await, None);

        store
            .put(
                &Path::from("graph/a.txt"),
                PutPayload::from_static(b"layer a"),
            )
            .await
            .unwrap();
        store
            .put(
                &Path::from("graph/c.txt"),
                PutPayload::from_static(b"layer c"),
            )
            .await
            .unwrap();
        assert_eq!(
            text(&store, "graph/a.txt").await.as_deref(),
            Some("layer a")
        );
        assert_eq!(
            text(&store, "graph/c.txt").await.as_deref(),
            Some("layer c")
        );
        assert_eq!(
            directory_files(&dir),
            vec![
                ("a.txt".to_string(), "base a".to_string()),
                ("sub/b.txt".to_string(), "base b".to_string()),
            ]
        );
    }

    #[tokio::test]
    async fn open_refuses_a_symlink_anywhere_under_the_root() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("sub")).unwrap();
        std::fs::write(dir.path().join("a.txt"), b"a").unwrap();
        std::os::unix::fs::symlink("../a.txt", dir.path().join("sub/alias")).unwrap();
        let error = FrozenDirectory::open(dir.path()).await.err().unwrap();
        let text = error.to_string();
        assert!(text.contains("prepared store holds a symlink"), "{text}");
        assert!(text.contains("sub/alias"), "{text}");
        std::fs::remove_file(dir.path().join("sub/alias")).unwrap();
        std::os::unix::fs::symlink(".", dir.path().join("loop")).unwrap();
        let text = FrozenDirectory::open(dir.path())
            .await
            .err()
            .unwrap()
            .to_string();
        assert!(text.contains("prepared store holds a symlink"), "{text}");
        std::fs::remove_file(dir.path().join("loop")).unwrap();
        assert!(FrozenDirectory::open(dir.path()).await.is_ok());
    }

    #[tokio::test]
    async fn ranges_and_heads_read_the_directory() {
        let (_dir, frozen) = directory(&[("a.txt", "0123456789")]).await;
        let store = store(&frozen, "graph");
        let location = Path::from("graph/a.txt");
        let head = store.head(&location).await.unwrap();
        assert_eq!(head.location, location);
        assert_eq!(head.size, 10);
        assert!(head.e_tag.is_some());
        let whole = store.get(&location).await.unwrap();
        assert_eq!(whole.meta.e_tag, head.e_tag, "the index and the file agree");
        let ranges = store.get_ranges(&location, &[0..2, 8..10]).await.unwrap();
        assert_eq!(
            ranges,
            vec![Bytes::from_static(b"01"), Bytes::from_static(b"89")]
        );
        let ranged = store
            .get_opts(
                &location,
                GetOptions::new().with_range(Some(GetRange::Bounded(3..6))),
            )
            .await
            .unwrap();
        assert_eq!(ranged.range, 3..6);
        assert_eq!(ranged.bytes().await.unwrap(), Bytes::from_static(b"345"));
        let unchanged = store
            .get_opts(
                &location,
                GetOptions::new()
                    .with_head(true)
                    .with_if_none_match(head.e_tag.clone()),
            )
            .await;
        assert!(matches!(unchanged, Err(Error::NotModified { .. })));
    }

    #[tokio::test]
    async fn delete_masks_the_directory_object() {
        let (dir, frozen) = directory(&[("a.txt", "base a"), ("b.txt", "base b")]).await;
        let store = store(&frozen, "graph");
        store.delete(&Path::from("graph/a.txt")).await.unwrap();
        assert_eq!(text(&store, "graph/a.txt").await, None);
        assert!(matches!(
            store.head(&Path::from("graph/a.txt")).await,
            Err(Error::NotFound { .. })
        ));
        assert_eq!(keys(&store, None).await, vec!["graph/b.txt"]);
        store
            .put(
                &Path::from("graph/a.txt"),
                PutPayload::from_static(b"again"),
            )
            .await
            .unwrap();
        assert_eq!(text(&store, "graph/a.txt").await.as_deref(), Some("again"));
        store.delete(&Path::from("graph/a.txt")).await.unwrap();
        assert_eq!(text(&store, "graph/a.txt").await, None);
        assert_eq!(directory_files(&dir).len(), 2);
    }

    #[tokio::test]
    async fn listings_are_the_sorted_union() {
        let (_dir, frozen) =
            directory(&[("a.txt", "a"), ("sub/b.txt", "b"), ("sub/d.txt", "d")]).await;
        let store = store(&frozen, "graph");
        store
            .put(
                &Path::from("graph/sub/c.txt"),
                PutPayload::from_static(b"c"),
            )
            .await
            .unwrap();
        store
            .put(
                &Path::from("graph/sub/b.txt"),
                PutPayload::from_static(b"layer b"),
            )
            .await
            .unwrap();
        store
            .put(&Path::from("other/e.txt"), PutPayload::from_static(b"e"))
            .await
            .unwrap();
        assert_eq!(
            keys(&store, None).await,
            vec![
                "graph/a.txt",
                "graph/sub/b.txt",
                "graph/sub/c.txt",
                "graph/sub/d.txt",
                "other/e.txt"
            ]
        );
        assert_eq!(
            keys(&store, Some("graph/sub")).await,
            vec!["graph/sub/b.txt", "graph/sub/c.txt", "graph/sub/d.txt"]
        );
        assert_eq!(keys(&store, Some("graph/none")).await, Vec::<String>::new());
        assert_eq!(
            keys(&store, Some("graph/a.txt")).await,
            Vec::<String>::new()
        );
        assert_eq!(keys(&store, Some("other")).await, vec!["other/e.txt"]);
        let listed = store
            .list(Some(&Path::from("graph/sub")))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(listed[0].size, 7, "the layer's object wins the union");

        let root = store.list_with_delimiter(None).await.unwrap();
        assert_eq!(
            root.common_prefixes,
            vec![Path::from("graph"), Path::from("other")]
        );
        assert!(root.objects.is_empty());
        let graph = store
            .list_with_delimiter(Some(&Path::from("graph")))
            .await
            .unwrap();
        assert_eq!(graph.common_prefixes, vec![Path::from("graph/sub")]);
        assert_eq!(
            graph
                .objects
                .iter()
                .map(|m| m.location.to_string())
                .collect::<Vec<_>>(),
            vec!["graph/a.txt"]
        );
        let sub = store
            .list_with_delimiter(Some(&Path::from("graph/sub")))
            .await
            .unwrap();
        assert_eq!(
            sub.objects
                .iter()
                .map(|m| (m.location.to_string(), m.size))
                .collect::<Vec<_>>(),
            vec![
                ("graph/sub/b.txt".to_string(), 7),
                ("graph/sub/c.txt".to_string(), 1),
                ("graph/sub/d.txt".to_string(), 1),
            ]
        );
        let offset = store
            .list_with_offset(Some(&Path::from("graph")), &Path::from("graph/sub/b.txt"))
            .map_ok(|meta| meta.location.to_string())
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(offset, vec!["graph/sub/c.txt", "graph/sub/d.txt"]);

        for key in ["graph/sub/b.txt", "graph/sub/c.txt", "graph/sub/d.txt"] {
            store.delete(&Path::from(key)).await.unwrap();
        }
        let graph = store
            .list_with_delimiter(Some(&Path::from("graph")))
            .await
            .unwrap();
        assert!(
            graph.common_prefixes.is_empty(),
            "a prefix whose directory objects are all masked no longer lists"
        );
        assert_eq!(keys(&store, Some("graph/sub")).await, Vec::<String>::new());
    }

    #[tokio::test]
    async fn create_refuses_a_directory_object_until_it_is_deleted() {
        let (_dir, frozen) = directory(&[("a.txt", "base a")]).await;
        let store = store(&frozen, "graph");
        let location = Path::from("graph/a.txt");
        let create = PutOptions::from(PutMode::Create);
        assert!(matches!(
            store
                .put_opts(&location, PutPayload::from_static(b"x"), create.clone())
                .await,
            Err(Error::AlreadyExists { .. })
        ));
        store.delete(&location).await.unwrap();
        store
            .put_opts(&location, PutPayload::from_static(b"x"), create.clone())
            .await
            .unwrap();
        assert!(matches!(
            store
                .put_opts(&location, PutPayload::from_static(b"y"), create)
                .await,
            Err(Error::AlreadyExists { .. })
        ));
        assert_eq!(text(&store, "graph/a.txt").await.as_deref(), Some("x"));
    }

    #[tokio::test]
    async fn update_against_a_directory_object_is_a_cas() {
        let (_dir, frozen) = directory(&[("a.txt", "base a")]).await;
        let store = store(&frozen, "graph");
        let location = Path::from("graph/a.txt");
        let token = store.head(&location).await.unwrap().e_tag.unwrap();
        let update = |e_tag: &str| {
            PutOptions::from(PutMode::Update(UpdateVersion {
                e_tag: Some(e_tag.to_string()),
                version: None,
            }))
        };
        assert!(matches!(
            store
                .put_opts(
                    &location,
                    PutPayload::from_static(b"stale"),
                    update("other")
                )
                .await,
            Err(Error::Precondition { .. })
        ));
        let first = store
            .put_opts(&location, PutPayload::from_static(b"first"), update(&token))
            .await
            .unwrap();
        assert!(matches!(
            store
                .put_opts(
                    &location,
                    PutPayload::from_static(b"second"),
                    update(&token)
                )
                .await,
            Err(Error::Precondition { .. })
        ));
        assert_eq!(text(&store, "graph/a.txt").await.as_deref(), Some("first"));
        let next = first.e_tag.unwrap();
        assert_eq!(
            store.head(&location).await.unwrap().e_tag.as_deref(),
            Some(next.as_str())
        );
        store
            .put_opts(&location, PutPayload::from_static(b"third"), update(&next))
            .await
            .unwrap();
        assert_eq!(text(&store, "graph/a.txt").await.as_deref(), Some("third"));
        assert!(matches!(
            store
                .put_opts(
                    &Path::from("graph/none"),
                    PutPayload::from_static(b"z"),
                    update(&token)
                )
                .await,
            Err(Error::Precondition { .. })
        ));
    }

    #[tokio::test]
    async fn racing_updates_of_a_directory_object_admit_one() {
        let (_dir, frozen) = directory(&[("a.txt", "base a")]).await;
        let store = Arc::new(store(&frozen, "graph"));
        let location = Path::from("graph/a.txt");
        let token = store.head(&location).await.unwrap().e_tag.unwrap();
        let attempts = (0..8).map(|i| {
            let store = Arc::clone(&store);
            let location = location.clone();
            let token = token.clone();
            tokio::spawn(async move {
                store
                    .put_opts(
                        &location,
                        PutPayload::from(format!("writer {i}").into_bytes()),
                        PutOptions::from(PutMode::Update(UpdateVersion {
                            e_tag: Some(token),
                            version: None,
                        })),
                    )
                    .await
                    .is_ok()
            })
        });
        let mut won = 0;
        for attempt in attempts {
            if attempt.await.unwrap() {
                won += 1;
            }
        }
        assert_eq!(won, 1);
    }

    #[tokio::test]
    async fn copies_and_renames_read_the_directory_and_write_the_layer() {
        let (dir, frozen) = directory(&[("a.txt", "base a")]).await;
        let store = store(&frozen, "graph");
        store
            .copy(&Path::from("graph/a.txt"), &Path::from("graph/copy.txt"))
            .await
            .unwrap();
        assert_eq!(
            text(&store, "graph/copy.txt").await.as_deref(),
            Some("base a")
        );
        assert!(matches!(
            store
                .copy_if_not_exists(&Path::from("graph/copy.txt"), &Path::from("graph/a.txt"))
                .await,
            Err(Error::AlreadyExists { .. })
        ));
        store
            .rename(&Path::from("graph/a.txt"), &Path::from("graph/moved.txt"))
            .await
            .unwrap();
        assert_eq!(text(&store, "graph/a.txt").await, None);
        assert_eq!(
            text(&store, "graph/moved.txt").await.as_deref(),
            Some("base a")
        );
        assert_eq!(
            directory_files(&dir),
            vec![("a.txt".to_string(), "base a".to_string())]
        );
    }

    #[tokio::test]
    async fn multipart_uploads_land_in_the_layer() {
        let (dir, frozen) = directory(&[("a.txt", "base a")]).await;
        let store = store(&frozen, "graph");
        let mut upload = store
            .put_multipart(&Path::from("graph/a.txt"))
            .await
            .unwrap();
        upload
            .put_part(PutPayload::from_static(b"part one "))
            .await
            .unwrap();
        upload
            .put_part(PutPayload::from_static(b"part two"))
            .await
            .unwrap();
        upload.complete().await.unwrap();
        assert_eq!(
            text(&store, "graph/a.txt").await.as_deref(),
            Some("part one part two")
        );
        assert_eq!(
            directory_files(&dir),
            vec![("a.txt".to_string(), "base a".to_string())]
        );
    }

    #[tokio::test]
    async fn two_mounts_share_one_deletion_view() {
        let (_dir, frozen) = directory(&[("a.txt", "base a")]).await;
        let lance = store(&frozen, "graph");
        let control = store(&frozen, "authority/graph");
        assert_eq!(
            text(&control, "authority/graph/a.txt").await.as_deref(),
            Some("base a")
        );
        lance.delete(&Path::from("graph/a.txt")).await.unwrap();
        assert_eq!(text(&control, "authority/graph/a.txt").await, None);
        assert_eq!(keys(&control, None).await, Vec::<String>::new());
    }
}
