//! `Omnigraph::open_clone`: a handle over an empty root that reads the base
//! and writes only its own root, the handle form of read-through storage.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use omnigraph::db::Omnigraph;

use crate::helpers::*;

/// Every file under `dir` with its length and modification time.
fn files_of(dir: &Path) -> BTreeMap<PathBuf, (u64, SystemTime)> {
    let mut files = BTreeMap::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(next) = pending.pop() {
        for entry in std::fs::read_dir(&next).unwrap() {
            let entry = entry.unwrap();
            let metadata = entry.metadata().unwrap();
            if metadata.is_dir() {
                pending.push(entry.path());
            } else {
                files.insert(entry.path(), (metadata.len(), metadata.modified().unwrap()));
            }
        }
    }
    files
}

#[tokio::test]
async fn a_clone_reads_the_base_and_writes_only_its_own_root() {
    let base_dir = tempfile::tempdir().unwrap();
    let base_uri = base_dir.path().to_str().unwrap();
    let persons = {
        let base = init_and_load(&base_dir).await;
        count_rows(&base, "node:Person").await
    };
    let base_files = files_of(base_dir.path());

    let clone_dir = tempfile::tempdir().unwrap();
    let clone_uri = clone_dir.path().to_str().unwrap();
    let clone = session(Omnigraph::open_clone(base_uri, clone_uri).await.unwrap());
    assert_eq!(count_rows(&clone, "node:Person").await, persons);
    mutate_main(
        &clone,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Cloned")], &[("$age", 1)]),
    )
    .await
    .unwrap();
    assert_eq!(count_rows(&clone, "node:Person").await, persons + 1);

    assert_eq!(files_of(base_dir.path()), base_files);
    assert!(clone_dir.path().join("__clone").join("created").is_file());
    let base = session(Omnigraph::open(base_uri).await.unwrap());
    assert_eq!(count_rows(&base, "node:Person").await, persons);

    let reopened = session(Omnigraph::open_clone(base_uri, clone_uri).await.unwrap());
    assert_eq!(count_rows(&reopened, "node:Person").await, persons + 1);
}

#[tokio::test]
async fn a_clone_root_inside_its_base_is_refused() {
    let base_dir = tempfile::tempdir().unwrap();
    let base_uri = base_dir.path().to_str().unwrap();
    drop(init_and_load(&base_dir).await);
    let inside = base_dir.path().join("clone");
    std::fs::create_dir(&inside).unwrap();
    let error = Omnigraph::open_clone(base_uri, inside.to_str().unwrap())
        .await
        .err()
        .expect("a clone root inside its base is refused");
    assert!(
        error.to_string().contains("do not contain each other"),
        "{error}"
    );
}
