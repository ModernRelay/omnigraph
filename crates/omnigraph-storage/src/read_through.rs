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
//!
//! [`ReadThroughStore::durable`] is the same store over a layer that outlives
//! the process. Each delete of a directory object first writes an empty
//! marker under the mount's reserved `__clone/deleted/`, and the next store
//! opened over that layer masks what the markers name. The reserved directory
//! never shows in a read or a listing. A [`ReadThroughMount`] is one such
//! layer over one directory: it takes its [`FrozenDirectory`] by value, so two
//! layers never share one set of deletes, while every store built from the
//! one mount does. The directory may be any [`ObjectStore`]
//! ([`FrozenDirectory::over`]).

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
    PutOptions, PutPayload, PutResult, RenameOptions, RenameTargetMode, Result,
};

/// A directory opened for reading only. The store that reads it is private
/// and only its read methods are called, so no write reaches the directory;
/// `objects` is its metadata, indexed once at open, and `deleted` holds every
/// directory key a store above deleted. A `turns` entry is held by a write
/// or a delete of its directory key from the decision to the landing, so two
/// of them on one key take turns and neither undoes the other unseen; other
/// keys are not waited for.
pub struct FrozenDirectory {
    files: Arc<dyn ObjectStore>,
    objects: BTreeMap<Path, ObjectMeta>,
    deleted: Mutex<BTreeSet<Path>>,
    turns: Mutex<BTreeMap<Path, Arc<tokio::sync::Mutex<()>>>>,
}

impl fmt::Debug for FrozenDirectory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FrozenDirectory")
            .field("files", &self.files)
            .field("objects", &self.objects.len())
            .field(
                "deleted",
                &self
                    .deleted
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .len(),
            )
            .finish()
    }
}

impl FrozenDirectory {
    /// Open `root` for reading and index its objects; it must exist and hold
    /// plain files and directories only, so a symlink anywhere under it is
    /// refused by name before anything is read through.
    pub async fn open(root: &std::path::Path) -> Result<Arc<Self>> {
        Ok(Arc::new(Self::local(root).await?))
    }

    /// [`Self::open`] by value, for a [`ReadThroughMount`].
    pub async fn local(root: &std::path::Path) -> Result<Self> {
        refuse_symlinks(root)?;
        let files = LocalFileSystem::new_with_prefix(root)?;
        Self::over(Arc::new(files)).await
    }

    /// Index every object of `files`, which may be any store whose keys start
    /// at the directory's root: a local directory, a bucket prefix.
    pub async fn over(files: Arc<dyn ObjectStore>) -> Result<Self> {
        let objects = files
            .list(None)
            .map_ok(|meta| (meta.location.clone(), meta))
            .try_collect()
            .await?;
        Ok(Self {
            files,
            objects,
            deleted: Mutex::new(BTreeSet::new()),
            turns: Mutex::new(BTreeMap::new()),
        })
    }

    /// Whether `key` is one of the directory's, deleted or not.
    fn holds(&self, key: &Path) -> bool {
        self.objects.contains_key(key)
    }

    /// The turn of a conditional write or a delete of `key`, when the
    /// directory holds it; a key it never held needs none.
    async fn turn(&self, key: Option<&Path>) -> Option<tokio::sync::OwnedMutexGuard<()>> {
        let key = key.filter(|key| self.holds(key))?;
        let turn = Arc::clone(
            self.turns
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .entry(key.clone())
                .or_default(),
        );
        Some(turn.lock_owned().await)
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
                    "the base holds a symlink: {}; the base is read from plain files and directories only",
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

/// The directory under a durable store's mount that holds the store's own
/// objects, and under it the one that holds a marker per deleted key.
const RESERVED: &str = "__clone";
const DELETED: &str = "deleted";
const CREATED: &str = "created";

/// An [`ObjectStore`] whose reads fall through to a [`FrozenDirectory`]
/// mounted at `prefix` and whose writes land in `layer`. `reserved` is the
/// durable form's own directory under the mount; without it a delete masks
/// in memory only.
#[derive(Debug)]
pub struct ReadThroughStore {
    layer: Arc<dyn ObjectStore>,
    frozen: Arc<FrozenDirectory>,
    prefix: Path,
    reserved: Option<Path>,
}

impl ReadThroughStore {
    /// `frozen`'s objects appear under `prefix`; `layer` takes every write.
    pub fn new(layer: Arc<dyn ObjectStore>, frozen: Arc<FrozenDirectory>, prefix: Path) -> Self {
        Self {
            layer,
            frozen,
            prefix,
            reserved: None,
        }
    }

    /// The durable form over a `layer` that outlives this store: the deletes
    /// an earlier store recorded in `layer` are masked again, and every new
    /// delete of a directory object is recorded before it takes effect.
    ///
    /// # Errors
    /// When the layer's listing fails, or when `frozen` holds the reserved
    /// directory itself: it is then the layer of another durable store, and
    /// reading through a layer would lose that store's directory and deletes.
    pub async fn durable(
        layer: Arc<dyn ObjectStore>,
        frozen: FrozenDirectory,
        prefix: Path,
    ) -> Result<Self> {
        let mount = ReadThroughMount::open(layer.as_ref(), frozen, prefix).await?;
        Ok(mount.store(layer))
    }

    fn is_reserved(&self, location: &Path) -> bool {
        under(self.reserved.as_ref(), location)
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

/// One durable layer over one frozen directory mounted at one prefix. Every
/// store built from a mount shares the directory's index and its deletes, so
/// two clients of the layer (Lance's store and the control adapter's) agree
/// on what a delete masked.
#[derive(Debug, Clone)]
pub struct ReadThroughMount {
    frozen: Arc<FrozenDirectory>,
    prefix: Path,
    reserved: Path,
    bucket: Option<String>,
}

impl ReadThroughMount {
    /// The name of the directory under a clone root that holds the storage's
    /// own objects: the harness leaves it out of a clone root's shape proof.
    pub const RESERVED_DIRECTORY: &'static str = RESERVED;

    /// Mask the deletes an earlier mount recorded in `layer`, and give a
    /// layer that has no reserved directory yet its `__clone/created` object,
    /// so every clone root carries the directory from its first build.
    ///
    /// # Errors
    /// When the layer's listing or that first write fails, or when `frozen`
    /// holds the reserved directory itself: it is then a clone root, and
    /// reading through a clone root would lose its base and its whiteouts.
    /// Also when `frozen` holds an object at the directory's name, which no
    /// read or listing of a clone could show.
    pub async fn open(
        layer: &dyn ObjectStore,
        frozen: FrozenDirectory,
        prefix: Path,
    ) -> Result<Self> {
        if let Some(found) = frozen.list(Some(&Path::from(RESERVED))).first() {
            return Err(Error::Generic {
                store: "ReadThroughStore",
                source: format!(
                    "the base holds {}: it is a clone root, and reading through a clone root is not supported",
                    found.location
                )
                .into(),
            });
        }
        if frozen.head(&Path::from(RESERVED)).is_some() {
            return Err(Error::Generic {
                store: "ReadThroughStore",
                source: format!(
                    "the base holds an object named {RESERVED}, the name of a clone root's reserved directory"
                )
                .into(),
            });
        }
        let reserved = prefix.clone().join(RESERVED);
        let deleted = reserved.clone().join(DELETED);
        let mut markers = layer.list(Some(&deleted));
        while let Some(marker) = markers.try_next().await? {
            if let Some(key) = marker.location.prefix_match(&deleted) {
                frozen.mark_deleted(key.collect());
            }
        }
        let created = reserved.clone().join(CREATED);
        match layer.head(&created).await {
            Ok(_) => {}
            Err(Error::NotFound { .. }) => {
                layer.put(&created, PutPayload::new()).await?;
            }
            Err(error) => return Err(error),
        }
        Ok(Self {
            frozen: Arc::new(frozen),
            prefix,
            reserved,
            bucket: None,
        })
    }

    /// The mount of a clone root that is a key prefix of `bucket`: a local
    /// clone root's prefix is an absolute path and names one location by
    /// itself, a key prefix names one only with its bucket.
    #[must_use]
    pub fn in_bucket(mut self, bucket: impl Into<String>) -> Self {
        self.bucket = Some(bucket.into());
        self
    }

    /// The bucket the clone root's prefix is in, when it is a key prefix.
    pub fn bucket(&self) -> Option<&str> {
        self.bucket.as_deref()
    }

    /// A store of this mount over `layer`, which must hold the objects the
    /// mount was opened over: another client of the same directory or bucket.
    pub fn store(&self, layer: Arc<dyn ObjectStore>) -> ReadThroughStore {
        ReadThroughStore {
            layer,
            frozen: Arc::clone(&self.frozen),
            prefix: self.prefix.clone(),
            reserved: Some(self.reserved.clone()),
        }
    }

    /// The refusal a store of this mount gives a write or a delete at
    /// `location`, for a caller that lists before it deletes and so would
    /// otherwise find nothing to refuse.
    ///
    /// # Errors
    /// When `location` is the reserved directory or lies inside it.
    pub fn refuse_reserved(&self, location: &Path) -> Result<()> {
        if under(Some(&self.reserved), location) {
            return Err(reserved_write(location));
        }
        Ok(())
    }
}

fn not_found(location: &Path) -> Error {
    Error::NotFound {
        path: location.to_string(),
        source: "no such object in the clone root or the base".into(),
    }
}

fn precondition(location: &Path, reason: impl Into<String>) -> Error {
    Error::Precondition {
        path: location.to_string(),
        source: reason.into().into(),
    }
}

fn under(reserved: Option<&Path>, location: &Path) -> bool {
    reserved.is_some_and(|reserved| location.prefix_match(reserved).is_some())
}

fn reserved_write(location: &Path) -> Error {
    Error::Generic {
        store: "ReadThroughStore",
        source: format!("{location} is inside the store's reserved directory {RESERVED}").into(),
    }
}

fn rename_of_base(location: &Path) -> Error {
    Error::Generic {
        store: "ReadThroughStore",
        source: format!(
            "{location} is an object the base holds, and a rename of it is refused: the rename's read takes no turn against a concurrent write of the object"
        )
        .into(),
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
        if self.is_reserved(location) {
            return Err(reserved_write(location));
        }
        let key = Self::directory_key(&self.prefix, location);
        let _turn = self.frozen.turn(key.as_ref()).await;
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
                        source: "the base holds this object".into(),
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
                        format!("the base object's ETag is not {expected}"),
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
        if self.is_reserved(location) {
            return Err(reserved_write(location));
        }
        self.layer.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        if self.is_reserved(location) {
            return Err(not_found(location));
        }
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
        if self.is_reserved(location) {
            return Err(not_found(location));
        }
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
        let reserved = self.reserved.clone();
        locations
            .and_then(move |location| {
                let layer = Arc::clone(&layer);
                let frozen = Arc::clone(&frozen);
                let prefix = prefix.clone();
                let reserved = reserved.clone();
                async move {
                    let key = Self::directory_key(&prefix, &location);
                    let _turn = frozen.turn(key.as_ref()).await;
                    if let Some(reserved) = &reserved {
                        if under(Some(reserved), &location) {
                            return Err(reserved_write(&location));
                        }
                        if let Some(key) = key.as_ref().filter(|key| frozen.head(key).is_some()) {
                            let marker: Path = reserved
                                .clone()
                                .join(DELETED)
                                .parts()
                                .chain(key.parts())
                                .collect();
                            layer.put(&marker, PutPayload::new()).await?;
                        }
                    }
                    if let Some(key) = key {
                        frozen.mark_deleted(key);
                    }
                    match layer.delete(&location).await {
                        Ok(()) | Err(Error::NotFound { .. }) => {}
                        Err(error) => return Err(error),
                    }
                    Ok(location)
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let layer = self.layer.list(prefix);
        let reserved = self.reserved.clone();
        let mut merged: BTreeMap<Path, ObjectMeta> = self
            .directory_objects(prefix)
            .into_iter()
            .map(|meta| (meta.location.clone(), meta))
            .collect();
        futures::stream::once(async move {
            let mut layer = layer;
            while let Some(meta) = layer.try_next().await? {
                if under(reserved.as_ref(), &meta.location) {
                    continue;
                }
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
        let mut common_prefixes: BTreeSet<Path> = layer
            .common_prefixes
            .into_iter()
            .filter(|common| !self.is_reserved(common))
            .collect();
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
            if self.is_reserved(&meta.location) {
                continue;
            }
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

    /// The copy and the delete of the default, once the source is not an
    /// object the base holds: that rename's read would take no turn.
    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        if Self::directory_key(&self.prefix, from).is_some_and(|key| self.frozen.holds(&key)) {
            return Err(rename_of_base(from));
        }
        let RenameOptions {
            target_mode,
            extensions,
        } = options;
        let mode = match target_mode {
            RenameTargetMode::Overwrite => CopyMode::Overwrite,
            RenameTargetMode::Create => CopyMode::Create,
        };
        self.copy_opts(from, to, CopyOptions { mode, extensions })
            .await?;
        self.delete(from).await
    }
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;
    use object_store::{GetRange, UpdateVersion};

    use crate::read_through_contract;

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
    async fn reads_fall_through_and_writes_stay_in_the_clone_root() {
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
        assert!(text.contains("the base holds a symlink"), "{text}");
        assert!(text.contains("sub/alias"), "{text}");
        std::fs::remove_file(dir.path().join("sub/alias")).unwrap();
        std::os::unix::fs::symlink(".", dir.path().join("loop")).unwrap();
        let text = FrozenDirectory::open(dir.path())
            .await
            .err()
            .unwrap()
            .to_string();
        assert!(text.contains("the base holds a symlink"), "{text}");
        std::fs::remove_file(dir.path().join("loop")).unwrap();
        assert!(FrozenDirectory::open(dir.path()).await.is_ok());
    }

    #[tokio::test]
    async fn ranges_and_heads_read_the_base() {
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
    async fn delete_masks_the_base_object() {
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
    async fn create_refuses_a_base_object_until_it_is_deleted() {
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
    async fn update_against_a_base_object_is_a_cas() {
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
    async fn racing_updates_of_a_base_object_admit_one() {
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
    async fn copies_and_renames_read_the_base_and_write_the_clone_root() {
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
            .rename(
                &Path::from("graph/copy.txt"),
                &Path::from("graph/moved.txt"),
            )
            .await
            .unwrap();
        assert_eq!(text(&store, "graph/copy.txt").await, None);
        assert_eq!(
            text(&store, "graph/moved.txt").await.as_deref(),
            Some("base a")
        );
        assert_eq!(text(&store, "graph/a.txt").await.as_deref(), Some("base a"));
        assert_eq!(
            directory_files(&dir),
            vec![("a.txt".to_string(), "base a".to_string())]
        );
    }

    #[tokio::test]
    async fn multipart_uploads_land_in_the_clone_root() {
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

    fn local(dir: &std::path::Path) -> Arc<dyn ObjectStore> {
        Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap())
    }

    /// The durable contract's stores on local directories: a base directory
    /// holding the named files and one fresh directory per clone root.
    struct LocalFixture {
        dirs: std::sync::Mutex<Vec<tempfile::TempDir>>,
    }

    impl LocalFixture {
        fn new() -> Self {
            Self {
                dirs: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn keep(&self, dir: tempfile::TempDir) -> Arc<dyn ObjectStore> {
            let store = local(dir.path());
            self.dirs.lock().unwrap().push(dir);
            store
        }
    }

    #[async_trait]
    impl read_through_contract::Fixture for LocalFixture {
        async fn base(&self, files: &[(&str, &str)]) -> Arc<dyn ObjectStore> {
            let (dir, _) = directory(files).await;
            self.keep(dir)
        }

        async fn clone_root(&self) -> Arc<dyn ObjectStore> {
            self.keep(tempfile::tempdir().unwrap())
        }
    }

    macro_rules! local_contract_rows {
        ($($name:ident),* $(,)?) => {
            $(
                #[tokio::test]
                async fn $name() {
                    read_through_contract::$name(&LocalFixture::new()).await;
                }
            )*
        };
    }

    local_contract_rows!(
        durable_deletes_survive_a_reopen_and_stay_in_their_own_clone_root,
        a_durable_store_keeps_its_whiteouts_out_of_sight,
        durable_refuses_a_base_that_is_itself_a_clone_root,
        durable_refuses_a_base_that_holds_an_object_at_the_reserved_name,
        a_clone_root_carries_the_reserved_directory_from_its_first_build,
        a_stop_between_the_whiteout_and_the_delete_leaves_the_key_deleted_or_untouched,
        an_update_and_a_delete_of_one_base_object_take_turns,
        a_create_and_a_delete_of_one_base_object_take_turns,
        a_rename_of_a_base_object_is_refused_and_one_of_a_clone_object_runs,
        create_after_a_durable_delete_holds_across_reopens,
        racing_updates_of_a_base_object_over_a_clone_root_admit_one,
    );

    mod differential_probe {
        use std::collections::{BTreeSet, HashMap, HashSet};
        use std::path::PathBuf;

        use object_store::memory::InMemory;
        use object_store::{ObjectStoreExt, UpdateVersion};

        use super::*;

        const BASE: &[(&str, &str)] = &[
            ("a.txt", "base a"),
            ("sub/b.txt", "base b"),
            ("sub/c.txt", "base c"),
            ("sub/deep/d.txt", "base d"),
        ];
        const KEYS: [&str; 4] = [
            "graph/a.txt",
            "graph/sub/b.txt",
            "graph/x.txt",
            "graph/sub/new/n.txt",
        ];
        const NAMES: [&str; 4] = ["a", "b", "x", "n"];
        const BASE_KEYS: [usize; 2] = [0, 1];

        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        enum Op {
            Put(usize),
            PutIfAbsent(usize),
            PimLast(usize),
            PimBogus(usize),
            Delete(usize),
            Get(usize),
            GetRange(usize),
            Head(usize),
            Multipart(usize),
            List,
            ListSub,
            Lwd,
            LwdSub,
            Copy(usize, usize),
            CopyIfNotExists(usize, usize),
            Rename(usize, usize),
            RenameIfNotExists(usize, usize),
        }

        impl fmt::Display for Op {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let n = |k: &usize| NAMES[*k];
                match self {
                    Op::Put(k) => write!(f, "put({})", n(k)),
                    Op::PutIfAbsent(k) => write!(f, "put_if_absent({})", n(k)),
                    Op::PimLast(k) => write!(f, "put_if_match_last({})", n(k)),
                    Op::PimBogus(k) => write!(f, "put_if_match_bogus({})", n(k)),
                    Op::Delete(k) => write!(f, "delete({})", n(k)),
                    Op::Get(k) => write!(f, "get({})", n(k)),
                    Op::GetRange(k) => write!(f, "get_range({})", n(k)),
                    Op::Head(k) => write!(f, "head({})", n(k)),
                    Op::Multipart(k) => write!(f, "multipart({})", n(k)),
                    Op::List => write!(f, "list()"),
                    Op::ListSub => write!(f, "list(sub)"),
                    Op::Lwd => write!(f, "list_with_delimiter()"),
                    Op::LwdSub => write!(f, "list_with_delimiter(sub)"),
                    Op::Copy(s, d) => write!(f, "copy({}->{})", n(s), n(d)),
                    Op::CopyIfNotExists(s, d) => {
                        write!(f, "copy_if_not_exists({}->{})", n(s), n(d))
                    }
                    Op::Rename(s, d) => write!(f, "rename({}->{})", n(s), n(d)),
                    Op::RenameIfNotExists(s, d) => {
                        write!(f, "rename_if_not_exists({}->{})", n(s), n(d))
                    }
                }
            }
        }

        fn full_alphabet() -> Vec<Op> {
            let mut ops = Vec::new();
            for k in 0..4 {
                ops.extend([
                    Op::Put(k),
                    Op::PutIfAbsent(k),
                    Op::PimLast(k),
                    Op::PimBogus(k),
                    Op::Delete(k),
                    Op::Get(k),
                    Op::GetRange(k),
                    Op::Head(k),
                    Op::Multipart(k),
                ]);
            }
            ops.extend([Op::List, Op::ListSub, Op::Lwd, Op::LwdSub]);
            for s in 0..4 {
                for d in 0..4 {
                    if s != d {
                        ops.extend([
                            Op::Copy(s, d),
                            Op::CopyIfNotExists(s, d),
                            Op::Rename(s, d),
                            Op::RenameIfNotExists(s, d),
                        ]);
                    }
                }
            }
            ops
        }

        fn class(error: &Error) -> &'static str {
            match error {
                Error::NotFound { .. } => "not_found",
                Error::AlreadyExists { .. } => "already_exists",
                Error::Precondition { .. } => "precondition",
                Error::NotModified { .. } => "not_modified",
                Error::NotImplemented { .. } => "not_implemented",
                _ => "other",
            }
        }

        fn err(error: Error) -> String {
            format!("Err({})", class(&error))
        }

        struct Side {
            store: Arc<dyn ObjectStore>,
            etags: HashMap<usize, String>,
            sort_lists: bool,
            /// The local reference copies by hard link, and rename(2) between two links of one
            /// inode is a no-op that keeps the source; it copies by read and write instead.
            copy_by_bytes: bool,
        }

        async fn copy_by_bytes(
            side: &Side,
            s: usize,
            d: usize,
            create: bool,
            rename: bool,
        ) -> String {
            let bytes = match side.store.get(&key(s)).await {
                Ok(result) => match result.bytes().await {
                    Ok(bytes) => bytes,
                    Err(e) => return err(e),
                },
                Err(e) => return err(e),
            };
            let mode = if create {
                PutMode::Create
            } else {
                PutMode::Overwrite
            };
            if let Err(e) = side
                .store
                .put_opts(&key(d), PutPayload::from(bytes), PutOptions::from(mode))
                .await
            {
                return err(e);
            }
            if rename {
                if let Err(e) = side.store.delete(&key(s)).await {
                    return err(e);
                }
            }
            "Ok(())".into()
        }

        fn key(k: usize) -> Path {
            Path::from(KEYS[k])
        }

        fn sub() -> Path {
            Path::from("graph/sub")
        }

        async fn list_text(side: &Side, prefix: Option<&Path>) -> String {
            match side
                .store
                .list(prefix)
                .try_collect::<Vec<ObjectMeta>>()
                .await
            {
                Ok(metas) => {
                    let mut items: Vec<String> = metas
                        .iter()
                        .map(|m| format!("{}:{}", m.location, m.size))
                        .collect();
                    if side.sort_lists {
                        items.sort();
                    }
                    format!("Ok([{}])", items.join(", "))
                }
                Err(e) => err(e),
            }
        }

        async fn lwd_text(side: &Side, prefix: Option<&Path>) -> String {
            match side.store.list_with_delimiter(prefix).await {
                Ok(result) => {
                    let objects: BTreeSet<String> = result
                        .objects
                        .iter()
                        .map(|m| format!("{}:{}", m.location, m.size))
                        .collect();
                    let prefixes: BTreeSet<String> = result
                        .common_prefixes
                        .iter()
                        .map(|p| p.to_string())
                        .collect();
                    format!(
                        "Ok(objects=[{}] prefixes=[{}])",
                        objects.into_iter().collect::<Vec<_>>().join(", "),
                        prefixes.into_iter().collect::<Vec<_>>().join(", ")
                    )
                }
                Err(e) => err(e),
            }
        }

        async fn get_text(store: &Arc<dyn ObjectStore>, k: usize) -> (String, Option<String>) {
            match store.get(&key(k)).await {
                Ok(result) => {
                    let etag = result.meta.e_tag.clone();
                    match result.bytes().await {
                        Ok(bytes) => (format!("Ok({:?})", String::from_utf8_lossy(&bytes)), etag),
                        Err(e) => (err(e), None),
                    }
                }
                Err(e) => (err(e), None),
            }
        }

        async fn observe(side: &Side) -> String {
            let mut out = format!("list={}", list_text(side, None).await);
            for (k, name) in NAMES.iter().enumerate() {
                let (text, _) = get_text(&side.store, k).await;
                out.push_str(&format!(" {name}={text}"));
            }
            out
        }

        fn put_result(side: &mut Side, k: usize, result: Result<PutResult>) -> String {
            match result {
                Ok(put) => {
                    if let Some(etag) = put.e_tag {
                        side.etags.insert(k, etag);
                    }
                    "Ok(put)".to_string()
                }
                Err(e) => err(e),
            }
        }

        async fn token(side: &mut Side, k: usize) -> Option<String> {
            if let Some(etag) = side.etags.get(&k) {
                return Some(etag.clone());
            }
            side.store.head(&key(k)).await.ok().and_then(|m| m.e_tag)
        }

        async fn put_if_match(
            side: &mut Side,
            k: usize,
            etag: Option<String>,
            bytes: &str,
        ) -> String {
            let mode = PutMode::Update(UpdateVersion {
                e_tag: etag,
                version: None,
            });
            let result = side
                .store
                .put_opts(
                    &key(k),
                    PutPayload::from(bytes.to_string()),
                    PutOptions::from(mode),
                )
                .await;
            put_result(side, k, result)
        }

        async fn apply(side: &mut Side, op: Op, bytes: &str) -> String {
            let payload = || PutPayload::from(bytes.to_string());
            match op {
                Op::Put(k) => {
                    let r = side.store.put(&key(k), payload()).await;
                    put_result(side, k, r)
                }
                Op::PutIfAbsent(k) => {
                    let r = side
                        .store
                        .put_opts(&key(k), payload(), PutOptions::from(PutMode::Create))
                        .await;
                    put_result(side, k, r)
                }
                Op::PimLast(k) => {
                    let etag = token(side, k).await;
                    put_if_match(side, k, etag, bytes).await
                }
                Op::PimBogus(k) => {
                    put_if_match(side, k, Some("\"made-up-etag\"".into()), bytes).await
                }
                Op::Delete(k) => match side.store.delete(&key(k)).await {
                    Ok(()) => "Ok(deleted)".into(),
                    Err(e) => err(e),
                },
                Op::Get(k) => {
                    let (text, etag) = get_text(&side.store, k).await;
                    if let Some(etag) = etag {
                        side.etags.insert(k, etag);
                    }
                    text
                }
                Op::GetRange(k) => {
                    let options = GetOptions::new().with_range(Some(1..3));
                    match side.store.get_opts(&key(k), options).await {
                        Ok(result) => {
                            if let Some(etag) = result.meta.e_tag.clone() {
                                side.etags.insert(k, etag);
                            }
                            match result.bytes().await {
                                Ok(b) => format!("Ok({:?})", String::from_utf8_lossy(&b)),
                                Err(e) => err(e),
                            }
                        }
                        Err(e) => err(e),
                    }
                }
                Op::Head(k) => match side.store.head(&key(k)).await {
                    Ok(meta) => {
                        if let Some(etag) = meta.e_tag {
                            side.etags.insert(k, etag);
                        }
                        format!("Ok(size={})", meta.size)
                    }
                    Err(e) => err(e),
                },
                Op::Multipart(k) => {
                    let mut upload = match side.store.put_multipart(&key(k)).await {
                        Ok(upload) => upload,
                        Err(e) => return err(e),
                    };
                    if let Err(e) = upload.put_part(payload()).await {
                        return err(e);
                    }
                    let r = upload.complete().await;
                    put_result(side, k, r)
                }
                Op::List => list_text(side, None).await,
                Op::ListSub => list_text(side, Some(&sub())).await,
                Op::Lwd => lwd_text(side, None).await,
                Op::LwdSub => lwd_text(side, Some(&sub())).await,
                Op::Copy(s, d) if side.copy_by_bytes => {
                    copy_by_bytes(side, s, d, false, false).await
                }
                Op::CopyIfNotExists(s, d) if side.copy_by_bytes => {
                    copy_by_bytes(side, s, d, true, false).await
                }
                Op::Rename(s, d) if side.copy_by_bytes => {
                    copy_by_bytes(side, s, d, false, true).await
                }
                Op::RenameIfNotExists(s, d) if side.copy_by_bytes => {
                    copy_by_bytes(side, s, d, true, true).await
                }
                Op::Copy(s, d) => unit(side.store.copy(&key(s), &key(d)).await),
                Op::CopyIfNotExists(s, d) => {
                    unit(side.store.copy_if_not_exists(&key(s), &key(d)).await)
                }
                Op::Rename(s, d) => unit(side.store.rename(&key(s), &key(d)).await),
                Op::RenameIfNotExists(s, d) => {
                    unit(side.store.rename_if_not_exists(&key(s), &key(d)).await)
                }
            }
        }

        fn unit(r: Result<()>) -> String {
            match r {
                Ok(()) => "Ok(())".into(),
                Err(e) => err(e),
            }
        }

        #[derive(Default)]
        struct Model {
            written: HashSet<usize>,
            hidden: HashSet<usize>,
        }

        impl Model {
            fn in_base(k: usize) -> bool {
                BASE_KEYS.contains(&k)
            }

            fn base_only(&self, k: usize) -> bool {
                Self::in_base(k) && !self.written.contains(&k) && !self.hidden.contains(&k)
            }

            fn present(&self, k: usize) -> bool {
                self.written.contains(&k) || self.base_only(k)
            }

            fn delete(&mut self, k: usize) {
                self.written.remove(&k);
                if Self::in_base(k) {
                    self.hidden.insert(k);
                }
            }

            fn after(&mut self, op: Op, expected: &str) {
                if !expected.starts_with("Ok") {
                    return;
                }
                match op {
                    Op::Put(k)
                    | Op::PutIfAbsent(k)
                    | Op::PimLast(k)
                    | Op::PimBogus(k)
                    | Op::Multipart(k) => {
                        self.written.insert(k);
                    }
                    Op::Delete(k) => self.delete(k),
                    Op::Copy(_, d) | Op::CopyIfNotExists(_, d) => {
                        self.written.insert(d);
                    }
                    Op::Rename(s, d) | Op::RenameIfNotExists(s, d) => {
                        self.written.insert(d);
                        self.delete(s);
                    }
                    _ => {}
                }
            }
        }

        /// The reference's answer, except where the RFC says otherwise: a rename of a base key is
        /// refused (both forms); durable only: put-if-match on a base-only key is a compare-and-swap on
        /// the base ETag, on a clone-held key the clone root's answer; a delete of an absent key is Ok.
        async fn expect(
            durable: bool,
            model: &Model,
            reference: &mut Side,
            op: Op,
            bytes: &str,
        ) -> String {
            if let Op::Rename(s, _) | Op::RenameIfNotExists(s, _) = op {
                if Model::in_base(s) {
                    return "Err(other)".into();
                }
            }
            if durable {
                match op {
                    Op::PimLast(k) | Op::PimBogus(k) if model.written.contains(&k) => {}
                    Op::PimLast(k) if model.base_only(k) => {
                        let r = reference
                            .store
                            .put(&key(k), PutPayload::from(bytes.to_string()))
                            .await;
                        return put_result(reference, k, r);
                    }
                    Op::PimLast(_) | Op::PimBogus(_) => return "Err(precondition)".into(),
                    Op::Delete(k) if !model.present(k) => return "Ok(deleted)".into(),
                    _ => {}
                }
            }
            apply(reference, op, bytes).await
        }

        struct Divergence {
            step: usize,
            expected: String,
            got: String,
        }

        fn diverge(step: usize, expected: String, got: String) -> Option<Divergence> {
            Some(Divergence {
                step,
                expected,
                got,
            })
        }

        async fn run_steps(
            durable: bool,
            seq: &[Op],
            store: &mut Side,
            reference: &mut Side,
        ) -> Option<Divergence> {
            let mut model = Model::default();
            for (i, op) in seq.iter().enumerate() {
                let bytes = format!("v{i}-{op}");
                let expected = expect(durable, &model, reference, *op, &bytes).await;
                let got = apply(store, *op, &bytes).await;
                if expected != got {
                    return diverge(i, expected, got);
                }
                model.after(*op, &expected);
                let expected = observe(reference).await;
                let got = observe(store).await;
                if expected != got {
                    return diverge(i, format!("state {expected}"), format!("state {got}"));
                }
            }
            None
        }

        fn base_files(root: &std::path::Path) -> Vec<(String, String)> {
            fn walk(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
                for entry in std::fs::read_dir(dir).unwrap() {
                    let path = entry.unwrap().path();
                    if path.is_dir() {
                        walk(&path, out);
                    } else {
                        out.push(path);
                    }
                }
            }
            let mut paths = Vec::new();
            walk(root, &mut paths);
            let mut out: Vec<(String, String)> = paths
                .iter()
                .map(|p| {
                    (
                        p.strip_prefix(root).unwrap().to_string_lossy().into_owned(),
                        std::fs::read_to_string(p).unwrap(),
                    )
                })
                .collect();
            out.sort();
            out
        }

        /// A clone never writes its base.
        fn base_unchanged(base: &std::path::Path, step: usize) -> Option<Divergence> {
            let expected: Vec<(String, String)> = BASE
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            let got = base_files(base);
            if got != expected {
                return diverge(step, format!("base {expected:?}"), format!("base {got:?}"));
            }
            None
        }

        fn local(dir: &std::path::Path) -> Arc<dyn ObjectStore> {
            Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap())
        }

        /// One durable sequence against the reference, then the end checks: the base's bytes,
        /// a reopened store's view, the reserved directory out of every listing, not found on a
        /// head of it, and its refusal of a delete and a put.
        async fn run_durable(base: PathBuf, seq: Vec<Op>) -> Option<Divergence> {
            let clone = tempfile::tempdir().unwrap();
            let refdir = tempfile::tempdir().unwrap();
            for (name, contents) in BASE {
                let path = refdir.path().join("graph").join(name);
                std::fs::create_dir_all(path.parent().unwrap()).unwrap();
                std::fs::write(path, contents).unwrap();
            }
            let layer = local(clone.path());
            let frozen = FrozenDirectory::local(&base).await.unwrap();
            let store = ReadThroughStore::durable(Arc::clone(&layer), frozen, Path::from("graph"))
                .await
                .unwrap();
            let mut store = Side {
                store: Arc::new(store),
                etags: HashMap::new(),
                sort_lists: false,
                copy_by_bytes: false,
            };
            let mut reference = Side {
                store: local(refdir.path()),
                etags: HashMap::new(),
                sort_lists: true,
                copy_by_bytes: true,
            };
            if let Some(d) = run_steps(true, &seq, &mut store, &mut reference).await {
                return Some(d);
            }
            let end = seq.len();
            if let Some(d) = base_unchanged(&base, end) {
                return Some(d);
            }
            let frozen = FrozenDirectory::local(&base).await.unwrap();
            let reopened =
                ReadThroughStore::durable(local(clone.path()), frozen, Path::from("graph"))
                    .await
                    .unwrap();
            let reopened = Side {
                store: Arc::new(reopened),
                etags: HashMap::new(),
                sort_lists: false,
                copy_by_bytes: false,
            };
            let expected = format!(
                "{} lwd={} lwd_sub={}",
                observe(&reference).await,
                lwd_text(&reference, None).await,
                lwd_text(&reference, Some(&sub())).await
            );
            let got = format!(
                "{} lwd={} lwd_sub={}",
                observe(&reopened).await,
                lwd_text(&reopened, None).await,
                lwd_text(&reopened, Some(&sub())).await
            );
            if expected != got {
                return diverge(end, format!("reopen {expected}"), format!("reopen {got}"));
            }
            for side in [&store, &reopened] {
                for text in [
                    list_text(side, None).await,
                    list_text(side, Some(&Path::from("graph"))).await,
                    lwd_text(side, None).await,
                    lwd_text(side, Some(&Path::from("graph"))).await,
                    list_text(side, Some(&Path::from("graph/__clone"))).await,
                ] {
                    if text.contains("__clone") {
                        return diverge(
                            end,
                            "reserved listing without __clone".into(),
                            format!("reserved {text}"),
                        );
                    }
                }
            }
            let created = Path::from("graph/__clone/created");
            let got = match reopened.store.head(&created).await {
                Ok(_) => "Ok(head)".to_string(),
                Err(e) => err(e),
            };
            if got != "Err(not_found)" {
                return diverge(
                    end,
                    "reserved head Err(not_found)".into(),
                    format!("reserved head {got}"),
                );
            }
            let got = unit(reopened.store.delete(&created).await);
            if got != "Err(other)" {
                return diverge(
                    end,
                    "reserved delete Err(other)".into(),
                    format!("reserved delete {got}"),
                );
            }
            let got = match reopened
                .store
                .put(&Path::from("graph/__clone/probe"), PutPayload::from("p"))
                .await
            {
                Ok(_) => "Ok(put)".to_string(),
                Err(e) => err(e),
            };
            if got != "Err(other)" {
                return diverge(
                    end,
                    "reserved put Err(other)".into(),
                    format!("reserved put {got}"),
                );
            }
            None
        }

        async fn run_memory(base: PathBuf, seq: Vec<Op>) -> Option<Divergence> {
            let frozen = FrozenDirectory::open(&base).await.unwrap();
            let store =
                ReadThroughStore::new(Arc::new(InMemory::new()), frozen, Path::from("graph"));
            let reference = InMemory::new();
            for (name, contents) in BASE {
                reference
                    .put(
                        &Path::from(format!("graph/{name}")),
                        PutPayload::from(contents.to_string()),
                    )
                    .await
                    .unwrap();
            }
            let mut store = Side {
                store: Arc::new(store),
                etags: HashMap::new(),
                sort_lists: false,
                copy_by_bytes: false,
            };
            let mut reference = Side {
                store: Arc::new(reference),
                etags: HashMap::new(),
                sort_lists: true,
                copy_by_bytes: false,
            };
            if let Some(d) = run_steps(false, &seq, &mut store, &mut reference).await {
                return Some(d);
            }
            base_unchanged(&base, seq.len())
        }

        fn label(seq: &[Op]) -> String {
            seq.iter()
                .map(|op| op.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        }

        /// Every sequence of one or two store calls over four keys (two the base holds, two it does
        /// not), in both forms, against a plain store holding a full copy of the base, the listings
        /// of the reference sorted; a sequence stops at its first divergence.
        #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
        async fn differential_probe_read_through_against_a_plain_store() {
            let started = std::time::Instant::now();
            let base = tempfile::tempdir().unwrap();
            for (name, contents) in BASE {
                let path = base.path().join(name);
                std::fs::create_dir_all(path.parent().unwrap()).unwrap();
                std::fs::write(path, contents).unwrap();
            }
            let full = full_alphabet();
            let levels: Vec<Vec<Vec<Op>>> = vec![
                full.iter().map(|op| vec![*op]).collect(),
                full.iter()
                    .flat_map(|a| full.iter().map(move |b| vec![*a, *b]))
                    .collect(),
            ];

            let mut lines = Vec::new();
            let mut shapes: HashSet<(&str, String, String, String)> = HashSet::new();
            let mut total = 0usize;
            for durable in [true, false] {
                let arm = if durable { "durable" } else { "memory" };
                let mut diverged: HashSet<String> = HashSet::new();
                let mut run = 0usize;
                let mut skipped = 0usize;
                for level in &levels {
                    let todo: Vec<Vec<Op>> = level
                        .iter()
                        .filter(|seq| {
                            let skip = (1..seq.len()).any(|n| diverged.contains(&label(&seq[..n])));
                            if skip {
                                skipped += 1;
                            }
                            !skip
                        })
                        .cloned()
                        .collect();
                    run += todo.len();
                    let results: Vec<(Vec<Op>, Option<Divergence>)> = futures::stream::iter(todo)
                        .map(|seq| {
                            let base = base.path().to_path_buf();
                            async move {
                                let found = if durable {
                                    run_durable(base, seq.clone()).await
                                } else {
                                    run_memory(base, seq.clone()).await
                                };
                                (seq, found)
                            }
                        })
                        .buffer_unordered(32)
                        .collect()
                        .await;
                    let mut found: Vec<(Vec<Op>, Divergence)> = results
                        .into_iter()
                        .filter_map(|(seq, d)| d.map(|d| (seq, d)))
                        .collect();
                    found.sort_by_key(|(seq, _)| label(seq));
                    for (seq, d) in found {
                        diverged.insert(label(&seq));
                        total += 1;
                        let class_of =
                            |s: &str| s.split(['(', ' ']).next().unwrap_or("").to_string();
                        let shape = (
                            arm,
                            seq[d.step.min(seq.len() - 1)].to_string(),
                            class_of(&d.expected),
                            class_of(&d.got),
                        );
                        let fresh = shapes.insert(shape);
                        if fresh && lines.len() < 40 {
                            lines.push(format!(
                                "DIVERGE arm={arm} seq=[{}] step={} expected={} got={}",
                                label(&seq),
                                d.step,
                                d.expected,
                                d.got
                            ));
                        }
                    }
                }
                println!(
                    "arm={arm} sequences_run={run} skipped_after_prefix_divergence={skipped} diverged={}",
                    diverged.len()
                );
            }
            for line in &lines {
                println!("{line}");
            }
            println!(
                "total_diverged={total} shapes={} printed={} wall_ms={}",
                shapes.len(),
                lines.len(),
                started.elapsed().as_millis()
            );
            assert_eq!(total, 0, "{total} sequences diverged");
        }
    }
}
