//! Graph-internal branch bases follow the opened root (the relocated graph
//! roots RFC, <https://github.com/ModernRelay/omnigraph/pull/1234>, until it
//! lands under `docs/rfcs/`).
//!
//! Lance 13 `Dataset::create_branch` is a shallow clone. It records the
//! source branch's absolute URI as a dataset-root `BasePath` named for that
//! branch (`Operation::Clone.ref_path`, then `Manifest::shallow_clone`), sets
//! `base_id` on every inherited data, deletion and index file, and copies the
//! source's own bases forward. Lance resolves a `base_id` file through
//! `Dataset::manifest.base_paths`, so a root copied to a new location would
//! read inherited files at the location each branch was created at.
//!
//! Every base of a graph dataset comes from that clone, of a branch of the
//! same dataset (`lance_clone::create_branch`, the one call
//! `forbidden_apis.rs` pins). Its location is therefore fixed by the opened
//! location: `find_branch(name)` of the dataset's main location.
//! [`follow_opened_root`] replaces each such base in the handle's in-memory
//! manifest with that location and refuses a handle carrying any other base.
//! Persisted bytes are never rewritten. A table pin's `manifest_path`, the
//! other absolute location a graph records, is Omnigraph's own and is rebuilt
//! from the opened root by `metadata::full_manifest_object_store_path`; this
//! module owns only Lance bases.
//!
//! A base is graph-internal when it is a dataset root and its recorded path
//! ends with the suffix its own name selects: `/<dir>` for main (no name, or
//! `main`) and `/<dir>/tree/<name>` for a branch, `<dir>` being the last
//! segment of the dataset's main location (`__manifest` or a table's
//! directory). A recorded path loses its query and trailing `/` first, and
//! on Windows `\` reads as `/`. A rebound file missing under the opened root
//! fails there, naming that path; nothing falls back to the recorded path.
//!
//! Every open of a graph dataset passes through this module: the
//! instrumented opener applies [`follow_opened_root`], and [`checkout`] /
//! [`checkout_native_branch`] are the only checkouts (`forbidden_apis.rs`
//! refuses a bare `checkout_branch`, `checkout_version`, `checkout_latest` or
//! `checkout_tag` elsewhere, an assignment to `Dataset::manifest` outside this
//! module, and a read of `base_paths` outside it).
//!
//! **The base-store cache.** A base file's path is computed from
//! `manifest.base_paths` on every call, but its store is built from the base's
//! URI on first use and kept in Lance's `base_object_stores`, which clones
//! share and which only Lance's private `set_manifest` clears. Replacing the
//! public manifest `Arc` would leave a store opened at the recorded URI in
//! place, and a handle would read the rebound path through it (on S3 or Azure,
//! another bucket or container). So the rebound map is installed on a fresh
//! clone with an empty cache (`Dataset::with_object_store` of the handle's own
//! primary store), never on the handle that came in. A handle whose bases
//! already name the opened root is returned as it is: its cache can hold only
//! stores opened at those URIs, because this module is the only writer of the
//! map.

use std::collections::HashMap;
use std::sync::Arc;

use lance::Dataset;
use lance::dataset::refs::Ref;
use lance_core::{Error, Result};
use lance_table::format::BasePath;
use object_store::path::Path;

const MAIN_BRANCH: &str = "main";
const BRANCH_DIR: &str = "tree";

/// Rebind every graph-internal branch base of `dataset` to the opened root and
/// refuse any other base. A dataset without bases, or whose bases already name
/// the opened root, is returned unchanged; otherwise the result is a fresh
/// handle with an empty base-store cache.
pub async fn follow_opened_root(dataset: Dataset) -> Result<Dataset> {
    let Some(base_paths) = rebound_bases(&dataset)? else {
        return Ok(dataset);
    };
    let primary = dataset.object_store(None).await?;
    let mut fresh = dataset.with_object_store(primary, None);
    let mut manifest = fresh.manifest.as_ref().clone();
    manifest.base_paths = base_paths;
    fresh.manifest = Arc::new(manifest);
    Ok(fresh)
}

/// The rebound map, `None` when every base already names the opened root, or
/// the refusal of a base that is not graph-internal.
fn rebound_bases(dataset: &Dataset) -> Result<Option<HashMap<u32, BasePath>>> {
    if dataset.manifest.base_paths.is_empty() {
        return Ok(None);
    }
    let main = dataset.branch_location().find_main()?;
    let dir = dataset_dir(&main.uri);
    let mut rebased = None;
    for (id, base) in dataset.manifest.base_paths.iter() {
        let opened = match internal_branch(&main.uri, dir.as_deref(), base)? {
            None => main.uri.clone(),
            Some(name) => main.find_branch(Some(name))?.uri,
        };
        if opened == base.path {
            continue;
        }
        let mut rebound = base.clone();
        rebound.path = opened;
        rebased
            .get_or_insert_with(|| dataset.manifest.base_paths.clone())
            .insert(*id, rebound);
    }
    Ok(rebased)
}

/// Check out `target` of `dataset`, then [`follow_opened_root`]. The one
/// checkout of a graph dataset.
pub async fn checkout(dataset: &Dataset, target: impl Into<Ref>) -> Result<Dataset> {
    let checked_out = dataset.checkout_version(target).await?;
    follow_opened_root(checked_out).await
}

/// The latest version of native `branch`, through [`checkout`]; Lance's
/// `checkout_branch` is the same reference.
pub async fn checkout_native_branch(dataset: &Dataset, branch: &str) -> Result<Dataset> {
    checkout(dataset, Ref::Version(Some(branch.to_string()), None)).await
}

/// Whether `dataset` carries any Lance base at all, whatever its location.
/// Callers that bound I/O to the primary store ask this.
pub fn has_bases(dataset: &Dataset) -> bool {
    !dataset.manifest.base_paths.is_empty()
}

/// Whether `dataset` carries a base that is not a graph-internal branch base:
/// the relocation bit. A copy of the whole root serves every other base from
/// the root it is opened at, and the open door refuses a dataset with such a
/// base, so an opened graph dataset answers `false`.
pub fn has_external_bases(dataset: &Dataset) -> bool {
    rebound_bases(dataset).is_err()
}

/// One base of a handle that came through the door: its URI and whether it is
/// a dataset root. `None` for an id the manifest does not name.
pub fn base(dataset: &Dataset, id: u32) -> Option<(&str, bool)> {
    dataset
        .manifest
        .base_paths
        .get(&id)
        .map(|base| (base.path.as_str(), base.is_dataset_root))
}

/// Every base of a handle that came through the door, as [`base`] returns it.
pub fn bases(dataset: &Dataset) -> impl Iterator<Item = (&str, bool)> {
    dataset
        .manifest
        .base_paths
        .values()
        .map(|base| (base.path.as_str(), base.is_dataset_root))
}

/// The object path of base `id` under the dataset's store registry and whether
/// it is a dataset root. The collector and the full-text certificate resolve
/// base files through this. It classifies nothing: the door already refused a
/// handle with a base that is not graph-internal and rebound every other one,
/// so the map it reads names locations under the opened root.
pub fn base_object_path(dataset: &Dataset, id: u32) -> Result<Option<(Path, bool)>> {
    let Some(base) = dataset.manifest.base_paths.get(&id) else {
        return Ok(None);
    };
    let path = base.extract_path(dataset.session().store_registry())?;
    Ok(Some((path, base.is_dataset_root)))
}

/// The branch `base` is graph-internal for (`None` for main), or the refusal
/// of a base that is not graph-internal. `main` is the dataset's main URI.
fn internal_branch<'a>(
    main: &str,
    dir: Option<&str>,
    base: &'a BasePath,
) -> Result<Option<&'a str>> {
    let refuse = |reason: &str| {
        Error::internal(format!(
            "graph dataset {main} names base {} ({}), which {reason}; Omnigraph reads only \
             objects under the opened root, so the dataset is refused",
            base.id,
            base.name
                .as_deref()
                .map_or_else(|| "main".to_string(), |name| format!("branch {name}")),
        ))
    };
    if !base.is_dataset_root {
        return Err(refuse("is not a dataset root"));
    }
    let Some(dir) = dir else {
        return Err(refuse("cannot be a branch of a dataset with no directory"));
    };
    let branch = base.name.as_deref().filter(|name| *name != MAIN_BRANCH);
    let suffix = match branch {
        None => format!("/{dir}"),
        Some(name) => format!("/{dir}/{BRANCH_DIR}/{name}"),
    };
    if !normalized(path_part(&base.path)).ends_with(&suffix) {
        return Err(refuse(
            "does not record the location its name selects in this dataset",
        ));
    }
    Ok(branch)
}

/// The last path segment of the dataset's main location.
fn dataset_dir(main: &str) -> Option<String> {
    let path = normalized(path_part(main));
    path.rsplit_once('/')
        .map(|(_, dir)| dir.to_string())
        .filter(|dir| !dir.is_empty())
}

fn path_part(uri: &str) -> &str {
    uri.split_once('?').map_or(uri, |(path, _)| path)
}

/// Forward slashes, no trailing slash. Lance spells a local Windows base with
/// backslashes (`BranchLocation::get_root_path` accepts both).
fn normalized(path: &str) -> String {
    let path = if cfg!(windows) {
        path.replace('\\', "/")
    } else {
        path.to_string()
    };
    path.trim_end_matches('/').to_string()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::lance_clone::fresh_dataset;

    /// A real dataset at `<root>/<dir>` whose manifest carries `bases`.
    async fn with_bases(root: &std::path::Path, dir: &str, bases: Vec<BasePath>) -> Dataset {
        let uri = root.join(dir);
        let mut dataset = fresh_dataset(uri.to_str().unwrap()).await;
        let mut manifest = dataset.manifest.as_ref().clone();
        manifest.base_paths = bases.into_iter().map(|base| (base.id, base)).collect();
        dataset.manifest = Arc::new(manifest);
        dataset
    }

    fn paths(dataset: &Dataset) -> HashMap<u32, String> {
        dataset
            .manifest
            .base_paths
            .iter()
            .map(|(id, base)| (*id, base.path.clone()))
            .collect()
    }

    fn base(id: u32, path: &str, name: Option<&str>, root: bool) -> BasePath {
        BasePath::new(id, path.into(), name.map(str::to_string), root)
    }

    #[tokio::test]
    async fn graph_internal_bases_follow_the_opened_root() {
        let dir = tempfile::tempdir().unwrap();
        let opened = dir.path().join("copy.omni");
        let manifest = format!("{}/__manifest", opened.to_str().unwrap());
        let dataset = with_bases(
            &opened,
            "__manifest",
            vec![
                base(1, "/old/root/__manifest", None, true),
                // Lance accepts `main` for the default branch in a reference.
                base(2, "s3://bucket/old/__manifest/", Some("main"), true),
                base(
                    3,
                    "/old/root/__manifest/tree/parent.01J",
                    Some("parent.01J"),
                    true,
                ),
                // A `/` in a branch name is a nested directory under `tree/`.
                base(
                    4,
                    "/old/root/__manifest/tree/team/review.01K",
                    Some("team/review.01K"),
                    true,
                ),
            ],
        )
        .await;
        let rebound = follow_opened_root(dataset).await.unwrap();
        let paths = paths(&rebound);
        assert_eq!(paths[&1], manifest);
        assert_eq!(paths[&2], manifest);
        assert_eq!(paths[&3], format!("{manifest}/tree/parent.01J"));
        assert_eq!(paths[&4], format!("{manifest}/tree/team/review.01K"));

        // Idempotent: a rebound handle is returned as it is.
        let again = follow_opened_root(rebound.clone()).await.unwrap();
        assert!(Arc::ptr_eq(&again.manifest, &rebound.manifest));
    }

    #[tokio::test]
    async fn a_table_fork_base_follows_its_own_table_directory() {
        let dir = tempfile::tempdir().unwrap();
        let opened = dir.path().join("copy.omni").join("nodes");
        let table = format!("{}/0001-0002", opened.to_str().unwrap());
        let dataset = with_bases(
            &opened,
            "0001-0002",
            vec![
                base(1, "/old/root/nodes/0001-0002", None, true),
                base(
                    2,
                    "/old/root/nodes/0001-0002/tree/legacy",
                    Some("legacy"),
                    true,
                ),
            ],
        )
        .await;
        let paths = paths(&follow_opened_root(dataset).await.unwrap());
        assert_eq!(paths[&1], table);
        assert_eq!(paths[&2], format!("{table}/tree/legacy"));
    }

    #[tokio::test]
    async fn every_other_base_refuses_the_dataset() {
        let dir = tempfile::tempdir().unwrap();
        let opened = dir.path().join("copy.omni");
        let refused = [
            // The name selects one tree, the path records another.
            base(
                1,
                "/old/root/__manifest/tree/other",
                Some("parent.01J"),
                true,
            ),
            // A branch name recorded at the main location.
            base(1, "/old/root/__manifest", Some("parent.01J"), true),
            // Main recorded as a branch tree.
            base(1, "/old/root/__manifest/tree/parent.01J", None, true),
            // Another dataset, and a data-only base.
            base(1, "s3://bucket/elsewhere/table.lance", None, true),
            base(1, "/old/root/__manifest", None, false),
            // A sibling dataset whose directory merely ends the same way.
            base(1, "/old/root/x__manifest", None, true),
        ];
        for (case, refused) in refused.into_iter().enumerate() {
            let described = format!("{refused:?}");
            let root = opened.join(format!("case-{case}"));
            let dataset = with_bases(&root, "__manifest", vec![refused]).await;
            let error = follow_opened_root(dataset).await.expect_err(&described);
            assert!(
                matches!(error, Error::Internal { .. })
                    && error.to_string().contains("so the dataset is refused")
                    && !error.to_string().contains("/old/root"),
                "{described}: {error}"
            );
        }
    }

    #[tokio::test]
    async fn checkout_applies_the_rule() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = fresh_dataset(dir.path().join("__manifest").to_str().unwrap()).await;
        let version = dataset.version().version;
        crate::lance_clone::create_branch(&mut dataset, "feature", version, None)
            .await
            .unwrap();
        let copy = tempfile::tempdir().unwrap();
        copy_tree(dir.path(), copy.path());
        // The original no longer holds the inherited file: only the copy can
        // serve the read.
        std::fs::remove_dir_all(dir.path().join("__manifest").join("data")).unwrap();
        let opened = fresh_open(copy.path().join("__manifest").to_str().unwrap()).await;
        let branch = checkout_native_branch(&opened, "feature").await.unwrap();
        let recorded: Vec<_> = branch.manifest.base_paths.values().collect();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].path, opened.uri());
        assert_eq!(branch.count_rows(None).await.unwrap(), 2);
    }

    /// A handle that already resolved a base's store at the recorded URI must
    /// not read the rebound path through it: across buckets that store would
    /// read the wrong bucket. `shared-memory://<authority>` buckets are
    /// isolated, like S3 buckets.
    #[tokio::test]
    async fn a_base_store_resolved_before_the_rebind_is_not_read_through() {
        use futures::TryStreamExt;
        use object_store::ObjectStoreExt;

        let run = ulid::Ulid::new().to_string().to_lowercase();
        let source = format!("shared-memory://source-{run}/root/__manifest");
        let copy = format!("shared-memory://copy-{run}/root/__manifest");
        let mut dataset = fresh_dataset(&source).await;
        let version = dataset.version().version;
        dataset
            .create_branch("feature", version, None)
            .await
            .unwrap();

        // Copy every object to the other bucket, then empty the source bucket.
        let (from, from_root) = lance::io::ObjectStore::from_uri(&source).await.unwrap();
        let (to, to_root) = lance::io::ObjectStore::from_uri(&copy).await.unwrap();
        let objects: Vec<_> = from
            .read_dir_all(&from_root, None)
            .try_collect()
            .await
            .unwrap();
        for object in &objects {
            let relative: Vec<_> = object.location.prefix_match(&from_root).unwrap().collect();
            let target: Path = to_root.parts().chain(relative).collect();
            let bytes = from
                .inner
                .get(&object.location)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            to.put(&target, &bytes).await.unwrap();
            from.inner.delete(&object.location).await.unwrap();
        }

        // A stock handle on the copy resolves its base store at the recorded
        // (source-bucket) URI before any rebind.
        // forbidden-api-allow: test-only stock open that bypasses the door, the handle under test
        let stale = lance::dataset::builder::DatasetBuilder::from_uri(&copy)
            .with_branch("feature", None)
            .load()
            .await
            .unwrap();
        let id = *stale.manifest.base_paths.keys().next().unwrap();
        assert_eq!(stale.manifest.base_paths[&id].path, source);
        stale.object_store(Some(id)).await.unwrap();

        let rebound = follow_opened_root(stale.clone()).await.unwrap();
        assert_eq!(rebound.manifest.base_paths[&id].path, copy);
        let rows = rebound.scan().try_into_batch().await.unwrap();
        assert_eq!(rows.num_rows(), 2, "reads the copy's bucket");
    }

    async fn fresh_open(uri: &str) -> Dataset {
        crate::instrumentation::open_dataset(
            uri,
            crate::instrumentation::VersionResolution::Latest,
            None,
            None,
        )
        .await
        .unwrap()
    }

    fn copy_tree(from: &std::path::Path, to: &std::path::Path) {
        std::fs::create_dir_all(to).unwrap();
        for entry in std::fs::read_dir(from).unwrap() {
            let entry = entry.unwrap();
            let target = to.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy_tree(&entry.path(), &target);
            } else {
                std::fs::copy(entry.path(), &target).unwrap();
            }
        }
    }
}
