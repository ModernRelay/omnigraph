//! The durable read-through contract as test bodies over a caller's stores:
//! the rows of the graph shallow clone RFC's storage contract that hold on
//! every instance, written once and run by the local tests of this crate and
//! by the engine's S3 integration tests (`s3_storage::`) over a bucket. A
//! [`Fixture`] gives each body a fresh base holding named objects and a fresh,
//! empty clone root; the mount sits at `graph` inside the clone root.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures::TryStreamExt;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::prefix::PrefixStore;
use object_store::{
    CopyOptions, Error, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreExt as _, PutMode, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, Result, UpdateVersion,
};

use crate::{FrozenDirectory, ReadThroughStore};

/// A store keyed at the prefix of an `s3://bucket/prefix` URI, built from
/// the standard `AWS_*` environment as the adapter builds its own: the
/// S3 fixture's base and clone roots.
pub fn s3_prefix_store(uri: &str) -> crate::Result<Arc<dyn ObjectStore>> {
    let location = crate::parse_s3_uri(uri)?;
    let bucket = crate::build_s3_store(crate::s3_bucket_builder(&location.bucket), uri)?;
    Ok(Arc::new(PrefixStore::new(
        bucket,
        Path::from(location.key.trim_end_matches('/')),
    )))
}

/// Fresh stores for one body. `base` returns a store keyed at the base's
/// root that holds `files`; `clone_root` returns an empty store keyed at a
/// clone root's root. Each call returns a new place: two clone roots of one
/// body never share objects.
#[async_trait]
pub trait Fixture: Send + Sync {
    async fn base(&self, files: &[(&str, &str)]) -> Arc<dyn ObjectStore>;
    async fn clone_root(&self) -> Arc<dyn ObjectStore>;
}

/// The mount's prefix inside every clone root the bodies use.
pub const PREFIX: &str = "graph";

/// A durable store of `layer` over `base`, the mount at [`PREFIX`].
pub async fn durable(
    layer: &Arc<dyn ObjectStore>,
    base: &Arc<dyn ObjectStore>,
) -> ReadThroughStore {
    let frozen = FrozenDirectory::over(Arc::clone(base)).await.unwrap();
    ReadThroughStore::durable(Arc::clone(layer), frozen, Path::from(PREFIX))
        .await
        .unwrap()
}

async fn text(store: &dyn ObjectStore, key: &str) -> Option<String> {
    match store.get(&Path::from(key)).await {
        Ok(result) => Some(String::from_utf8(result.bytes().await.unwrap().to_vec()).unwrap()),
        Err(Error::NotFound { .. }) => None,
        Err(error) => panic!("{error}"),
    }
}

async fn keys(store: &dyn ObjectStore, prefix: Option<&str>) -> Vec<String> {
    store
        .list(prefix.map(Path::from).as_ref())
        .map_ok(|meta| meta.location.to_string())
        .try_collect()
        .await
        .unwrap()
}

/// Every object of `store` with its text, sorted by key: the base's bytes.
pub async fn files(store: &dyn ObjectStore) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for key in keys(store, None).await {
        out.push((key.clone(), text(store, &key).await.unwrap()));
    }
    out.sort();
    out
}

fn graph(key: &str) -> Path {
    Path::from(format!("{PREFIX}/{key}"))
}

/// A delete in one clone root leaves another clone root's view of the base
/// alone, survives a reopen of its own clone root, and never writes the base.
pub async fn durable_deletes_survive_a_reopen_and_stay_in_their_own_clone_root(
    fixture: &dyn Fixture,
) {
    let base = fixture
        .base(&[("a.txt", "base a"), ("sub/b.txt", "base b")])
        .await;
    let one = fixture.clone_root().await;
    let two = fixture.clone_root().await;
    let first = durable(&one, &base).await;
    let second = durable(&two, &base).await;
    first.delete(&graph("a.txt")).await.unwrap();
    first
        .put(&graph("c.txt"), PutPayload::from_static(b"layer c"))
        .await
        .unwrap();
    assert_eq!(text(&first, "graph/a.txt").await, None);
    assert_eq!(
        text(&second, "graph/a.txt").await.as_deref(),
        Some("base a"),
        "a delete in one layer leaves the other layer's view alone"
    );
    assert_eq!(text(&second, "graph/c.txt").await, None);
    drop(first);
    let reopened = durable(&one, &base).await;
    assert_eq!(text(&reopened, "graph/a.txt").await, None);
    assert_eq!(
        keys(&reopened, None).await,
        vec!["graph/c.txt", "graph/sub/b.txt"]
    );
    assert_eq!(
        keys(&second, None).await,
        vec!["graph/a.txt", "graph/sub/b.txt"]
    );
    assert_eq!(
        files(base.as_ref()).await,
        vec![
            ("a.txt".to_string(), "base a".to_string()),
            ("sub/b.txt".to_string(), "base b".to_string()),
        ]
    );
}

/// The reserved directory is hidden from every read and listing and refuses
/// writes; only a delete of a base object leaves a whiteout.
pub async fn a_durable_store_keeps_its_whiteouts_out_of_sight(fixture: &dyn Fixture) {
    let base = fixture
        .base(&[("a.txt", "base a"), ("b.txt", "base b")])
        .await;
    let layer = fixture.clone_root().await;
    let store = durable(&layer, &base).await;
    store.delete(&graph("a.txt")).await.unwrap();
    let marker = graph("__clone/deleted/a.txt");
    assert!(
        layer.head(&marker).await.is_ok(),
        "the layer holds the marker"
    );
    assert!(matches!(
        store.head(&marker).await,
        Err(Error::NotFound { .. })
    ));
    assert_eq!(keys(&store, None).await, vec!["graph/b.txt"]);
    assert_eq!(
        keys(&store, Some("graph/__clone")).await,
        Vec::<String>::new()
    );
    let listed = store
        .list_with_delimiter(Some(&Path::from(PREFIX)))
        .await
        .unwrap();
    assert!(listed.common_prefixes.is_empty());
    assert!(
        store
            .put(&graph("__clone/x"), PutPayload::from_static(b"x"))
            .await
            .is_err()
    );
    assert!(store.delete(&marker).await.is_err());

    store
        .put(&graph("c.txt"), PutPayload::from_static(b"c"))
        .await
        .unwrap();
    store.delete(&graph("c.txt")).await.unwrap();
    store.delete(&graph("a.txt")).await.unwrap();
    assert_eq!(
        keys(layer.as_ref(), Some("graph/__clone")).await,
        vec!["graph/__clone/created", "graph/__clone/deleted/a.txt"],
        "the created object and one whiteout per deleted base object, nothing for a clone-root key"
    );
}

/// A base that is itself a clone root is refused by the whiteout it holds.
pub async fn durable_refuses_a_base_that_is_itself_a_clone_root(fixture: &dyn Fixture) {
    let base = fixture
        .base(&[("__clone/deleted/a.txt", ""), ("b.txt", "base b")])
        .await;
    let layer = fixture.clone_root().await;
    let frozen = FrozenDirectory::over(base).await.unwrap();
    let error = ReadThroughStore::durable(layer, frozen, Path::from(PREFIX))
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("__clone/deleted/a.txt"), "{error}");
    assert!(error.contains("it is a clone root"), "{error}");
}

/// A base holding an object at the reserved directory's name is refused.
pub async fn durable_refuses_a_base_that_holds_an_object_at_the_reserved_name(
    fixture: &dyn Fixture,
) {
    let base = fixture
        .base(&[("__clone", "not a directory"), ("b.txt", "base b")])
        .await;
    let layer = fixture.clone_root().await;
    let frozen = FrozenDirectory::over(base).await.unwrap();
    let error = ReadThroughStore::durable(layer, frozen, Path::from(PREFIX))
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(
        error.contains("the base holds an object named __clone"),
        "{error}"
    );
}

/// A clone root carries `__clone/created` from its first build, a later
/// build over it finds the same objects, and it is refused as a base.
pub async fn a_clone_root_carries_the_reserved_directory_from_its_first_build(
    fixture: &dyn Fixture,
) {
    let base = fixture.base(&[("a.txt", "base a")]).await;
    let layer = fixture.clone_root().await;
    let store = durable(&layer, &base).await;
    assert!(layer.head(&graph("__clone/created")).await.is_ok());
    store
        .put(&graph("c.txt"), PutPayload::from_static(b"c"))
        .await
        .unwrap();
    assert_eq!(keys(&store, None).await, vec!["graph/a.txt", "graph/c.txt"]);
    drop(store);
    let again = durable(&layer, &base).await;
    assert_eq!(keys(&again, None).await, vec!["graph/a.txt", "graph/c.txt"]);

    let as_base: Arc<dyn ObjectStore> =
        Arc::new(PrefixStore::new(Arc::clone(&layer), Path::from(PREFIX)));
    let as_base = FrozenDirectory::over(as_base).await.unwrap();
    let other = fixture.clone_root().await;
    let error = ReadThroughStore::durable(other, as_base, Path::from("other"))
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(
        error.contains("it is a clone root"),
        "a clone root that never deleted is refused as a base: {error}"
    );
}

/// A layer whose delete fails after the whiteout landed, as a process stop
/// between the two steps would leave the clone root.
#[derive(Debug)]
pub struct DeleteFails(pub Arc<dyn ObjectStore>);

impl fmt::Display for DeleteFails {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DeleteFails({})", self.0)
    }
}

#[async_trait]
impl ObjectStore for DeleteFails {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.0.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.0.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.0.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        locations
            .and_then(|location| async move {
                Err(Error::Generic {
                    store: "DeleteFails",
                    source: format!("the process stopped before {location} was deleted").into(),
                })
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.0.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.0.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.0.copy_opts(from, to, options).await
    }
}

/// A stop between the whiteout and the delete leaves a base-only key deleted
/// and a clone-held key untouched, as a reopen over the plain layer shows.
pub async fn a_stop_between_the_whiteout_and_the_delete_leaves_the_key_deleted_or_untouched(
    fixture: &dyn Fixture,
) {
    let base = fixture
        .base(&[("a.txt", "base a"), ("b.txt", "base b")])
        .await;
    let plain = fixture.clone_root().await;
    let stopping: Arc<dyn ObjectStore> = Arc::new(DeleteFails(Arc::clone(&plain)));

    let store = durable(&stopping, &base).await;
    store
        .put(&graph("b.txt"), PutPayload::from_static(b"clone b"))
        .await
        .unwrap();
    assert!(store.delete(&graph("a.txt")).await.is_err());
    assert!(store.delete(&graph("b.txt")).await.is_err());
    assert_eq!(text(&store, "graph/a.txt").await, None);
    assert_eq!(
        text(&store, "graph/b.txt").await.as_deref(),
        Some("clone b"),
        "the clone root's own object still answers while its delete has not happened"
    );
    drop(store);

    let reopened = durable(&plain, &base).await;
    assert_eq!(
        text(&reopened, "graph/a.txt").await,
        None,
        "a key only the base held is deleted once its whiteout landed"
    );
    assert_eq!(
        text(&reopened, "graph/b.txt").await.as_deref(),
        Some("clone b"),
        "a key the clone root held is untouched until its own object is deleted"
    );
    reopened.delete(&graph("b.txt")).await.unwrap();
    assert_eq!(text(&reopened, "graph/b.txt").await, None);
    let again = durable(&plain, &base).await;
    assert_eq!(text(&again, "graph/b.txt").await, None);
    assert_eq!(keys(&again, None).await, Vec::<String>::new());
}

/// A layer that parks one call at `key` until the test lets it go, its
/// create or its delete, so a second call can start inside the first.
#[derive(Debug)]
pub struct Parks {
    layer: Arc<dyn ObjectStore>,
    key: Path,
    the_create: bool,
    pub parked: Arc<tokio::sync::Semaphore>,
    pub go: Arc<tokio::sync::Semaphore>,
}

impl Parks {
    pub fn over(layer: Arc<dyn ObjectStore>, key: &Path, the_create: bool) -> Self {
        Self {
            layer,
            key: key.clone(),
            the_create,
            parked: Arc::new(tokio::sync::Semaphore::new(0)),
            go: Arc::new(tokio::sync::Semaphore::new(0)),
        }
    }
}

impl fmt::Display for Parks {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Parks({})", self.layer)
    }
}

#[async_trait]
impl ObjectStore for Parks {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        if self.the_create && *location == self.key && matches!(opts.mode, PutMode::Create) {
            self.parked.add_permits(1);
            self.go.acquire().await.unwrap().forget();
        }
        self.layer.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.layer.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.layer.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let layer = Arc::clone(&self.layer);
        let key = (!self.the_create).then(|| self.key.clone());
        let (parked, go) = (Arc::clone(&self.parked), Arc::clone(&self.go));
        locations
            .and_then(move |location| {
                let layer = Arc::clone(&layer);
                let held = key.as_ref() == Some(&location);
                let (parked, go) = (Arc::clone(&parked), Arc::clone(&go));
                async move {
                    if held {
                        parked.add_permits(1);
                        go.acquire().await.unwrap().forget();
                    }
                    layer.delete(&location).await?;
                    Ok(location)
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.layer.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.layer.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.layer.copy_opts(from, to, options).await
    }
}

/// An update admitted on the base ETag before a delete began lands, and the
/// delete that began after it removes the object.
pub async fn an_update_and_a_delete_of_one_base_object_take_turns(fixture: &dyn Fixture) {
    let base = fixture.base(&[("a.txt", "base a")]).await;
    let location = graph("a.txt");
    let parks = Parks::over(fixture.clone_root().await, &location, true);
    let (parked, go) = (Arc::clone(&parks.parked), Arc::clone(&parks.go));
    let layer: Arc<dyn ObjectStore> = Arc::new(parks);
    let store = Arc::new(durable(&layer, &base).await);
    let token = store.head(&location).await.unwrap().e_tag.unwrap();

    let update = tokio::spawn({
        let (store, location) = (Arc::clone(&store), location.clone());
        async move {
            store
                .put_opts(
                    &location,
                    PutPayload::from_static(b"updated"),
                    PutOptions::from(PutMode::Update(UpdateVersion {
                        e_tag: Some(token),
                        version: None,
                    })),
                )
                .await
        }
    });
    parked.acquire().await.unwrap().forget();
    let delete = tokio::spawn({
        let (store, location) = (Arc::clone(&store), location.clone());
        async move { store.delete(&location).await }
    });
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    go.add_permits(1);

    let updated = update.await.unwrap();
    delete.await.unwrap().unwrap();
    assert!(
        updated.is_ok(),
        "an update admitted on the base ETag before the delete began lands: {updated:?}"
    );
    assert_eq!(
        text(store.as_ref(), "graph/a.txt").await,
        None,
        "the delete that began after the update ran after it and removed the object"
    );
}

/// A create acknowledged after a delete stands; one refused before it leaves
/// the key deleted.
pub async fn a_create_and_a_delete_of_one_base_object_take_turns(fixture: &dyn Fixture) {
    let base = fixture.base(&[("a.txt", "base a")]).await;
    let location = graph("a.txt");
    let parks = Parks::over(fixture.clone_root().await, &location, false);
    let (parked, go) = (Arc::clone(&parks.parked), Arc::clone(&parks.go));
    let layer: Arc<dyn ObjectStore> = Arc::new(parks);
    let store = Arc::new(durable(&layer, &base).await);

    let delete = tokio::spawn({
        let (store, location) = (Arc::clone(&store), location.clone());
        async move { store.delete(&location).await }
    });
    parked.acquire().await.unwrap().forget();
    let create = tokio::spawn({
        let (store, location) = (Arc::clone(&store), location.clone());
        async move {
            store
                .put_opts(
                    &location,
                    PutPayload::from_static(b"created"),
                    PutOptions::from(PutMode::Create),
                )
                .await
        }
    });
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    go.add_permits(1);

    delete.await.unwrap().unwrap();
    let created = create.await.unwrap();
    let left = text(store.as_ref(), "graph/a.txt").await;
    match &created {
        Ok(_) => assert_eq!(
            left.as_deref(),
            Some("created"),
            "a create the store acknowledged after the delete stands"
        ),
        Err(Error::AlreadyExists { .. }) => assert_eq!(
            left, None,
            "a create refused before the delete leaves the key deleted"
        ),
        Err(error) => panic!("{error}"),
    }
}

/// A rename of a base object is refused and writes nothing; a rename of a
/// clone-held object runs.
pub async fn a_rename_of_a_base_object_is_refused_and_one_of_a_clone_object_runs(
    fixture: &dyn Fixture,
) {
    let base = fixture.base(&[("a.txt", "base a")]).await;
    let layer = fixture.clone_root().await;
    let store = durable(&layer, &base).await;
    let (a, r, c) = (graph("a.txt"), graph("r.txt"), graph("c.txt"));

    let refused = store.rename(&a, &r).await;
    assert!(matches!(refused, Err(Error::Generic { .. })), "{refused:?}");
    assert_eq!(text(&store, "graph/a.txt").await.as_deref(), Some("base a"));
    assert_eq!(text(&store, "graph/r.txt").await, None);
    let layer_keys = keys(layer.as_ref(), None).await;
    assert!(
        layer_keys
            .iter()
            .all(|key| !key.contains("deleted") && key != "graph/r.txt"),
        "a refused rename writes nothing: {layer_keys:?}"
    );

    store
        .put(&c, PutPayload::from_static(b"clone c"))
        .await
        .unwrap();
    store.rename(&c, &r).await.unwrap();
    assert_eq!(
        text(&store, "graph/r.txt").await.as_deref(),
        Some("clone c")
    );
    assert_eq!(text(&store, "graph/c.txt").await, None);
}

/// A create after a durable delete lands in the clone root and holds across
/// reopens; the base keeps its object throughout.
pub async fn create_after_a_durable_delete_holds_across_reopens(fixture: &dyn Fixture) {
    let base = fixture.base(&[("a.txt", "base a")]).await;
    let layer = fixture.clone_root().await;
    let location = graph("a.txt");
    let create = PutOptions::from(PutMode::Create);

    let store = durable(&layer, &base).await;
    assert!(matches!(
        store
            .put_opts(&location, PutPayload::from_static(b"x"), create.clone())
            .await,
        Err(Error::AlreadyExists { .. })
    ));
    store.delete(&location).await.unwrap();

    let store = durable(&layer, &base).await;
    store
        .put_opts(&location, PutPayload::from_static(b"x"), create.clone())
        .await
        .unwrap();

    let store = durable(&layer, &base).await;
    assert_eq!(text(&store, "graph/a.txt").await.as_deref(), Some("x"));
    assert!(matches!(
        store
            .put_opts(&location, PutPayload::from_static(b"y"), create)
            .await,
        Err(Error::AlreadyExists { .. })
    ));
    store.delete(&location).await.unwrap();

    let store = durable(&layer, &base).await;
    assert_eq!(text(&store, "graph/a.txt").await, None);
    let kept = base.get(&Path::from("a.txt")).await.unwrap();
    assert_eq!(kept.bytes().await.unwrap(), Bytes::from_static(b"base a"));
    assert_eq!(keys(base.as_ref(), None).await, vec!["a.txt"]);
}

/// Of eight racing updates of one base object, each on the ETag the mount
/// reports, exactly one is admitted. The frozen directory's turn puts them in
/// a sequence: the first finds no copy in the clone root and lands by the
/// clone root's uncontested put-if-absent, each later one finds that copy and
/// fails its put-if-match against the copy's ETag. The row holds the mount's
/// ordering and the layer's put-if-match; the layer's put-if-absent under
/// contention is `s3_adapter_conditional_writes_contract`'s.
pub async fn racing_updates_of_a_base_object_over_a_clone_root_admit_one(fixture: &dyn Fixture) {
    let base = fixture.base(&[("a.txt", "base a")]).await;
    let layer = fixture.clone_root().await;
    let store = Arc::new(durable(&layer, &base).await);
    let location = graph("a.txt");
    let token = store.head(&location).await.unwrap().e_tag.unwrap();
    let attempts = (0..8).map(|i| {
        let store = Arc::clone(&store);
        let location = location.clone();
        let token = token.clone();
        async move {
            store
                .put_opts(
                    &location,
                    PutPayload::from(format!("w{i}").into_bytes()),
                    PutOptions::from(PutMode::Update(UpdateVersion {
                        e_tag: Some(token),
                        version: None,
                    })),
                )
                .await
                .is_ok()
        }
    });
    let won = futures::future::join_all(attempts)
        .await
        .into_iter()
        .filter(|admitted| *admitted)
        .count();
    assert_eq!(won, 1);
    assert_eq!(
        files(base.as_ref()).await,
        vec![("a.txt".to_string(), "base a".to_string())]
    );
}
