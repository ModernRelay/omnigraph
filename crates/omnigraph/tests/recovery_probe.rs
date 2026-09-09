//! Recovery inspection is bounded and never repairs the inspected graph.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use omnigraph::db::Omnigraph;

fn files(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn visit(root: &Path, dir: &Path, found: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for entry in fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                visit(root, &path, found);
            } else {
                found.insert(
                    path.strip_prefix(root).unwrap().to_path_buf(),
                    fs::read(path).unwrap(),
                );
            }
        }
    }
    let mut found = BTreeMap::new();
    visit(root, root, &mut found);
    found
}

#[tokio::test]
async fn recovery_probe_refuses_unknown_sidecars_and_staging_without_writes() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    Omnigraph::init(uri, "node Person { name: String @key }")
        .await
        .unwrap();
    let before = files(dir.path());
    Omnigraph::ensure_no_pending_recovery(uri).await.unwrap();
    assert_eq!(files(dir.path()), before);

    let recovery = dir.path().join("__recovery");
    fs::create_dir_all(&recovery).unwrap();
    let pending = recovery.join("unknown.json");
    fs::write(&pending, "malformed future recovery data").unwrap();
    let before = files(dir.path());
    let err = Omnigraph::ensure_no_pending_recovery(uri)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("pending recovery"), "{err}");
    assert_eq!(files(dir.path()), before);
    fs::remove_file(pending).unwrap();

    for name in [
        "_schema.pg.staging",
        "_schema.ir.json.staging",
        "__schema_state.json.staging",
    ] {
        let pending = dir.path().join(name);
        fs::write(&pending, "uncertain staging").unwrap();
        let before = files(dir.path());
        let err = Omnigraph::ensure_no_pending_recovery(uri)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("staged schema recovery"),
            "{name}: {err}"
        );
        assert_eq!(files(dir.path()), before);
        fs::remove_file(pending).unwrap();
    }
    // Existing inventory semantics ignore non-JSON residue within the bound.
    fs::write(recovery.join("note.txt"), "not a recovery sidecar").unwrap();
    let before = files(dir.path());
    Omnigraph::ensure_no_pending_recovery(uri).await.unwrap();
    assert_eq!(files(dir.path()), before);
}

#[tokio::test]
async fn recovery_probe_refuses_inventory_uncertainty_at_the_bound() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let recovery = dir.path().join("__recovery");
    fs::create_dir(&recovery).unwrap();
    for index in 0..1025 {
        fs::write(recovery.join(format!("residue-{index}.txt")), []).unwrap();
    }
    let before = files(dir.path());
    let err = Omnigraph::ensure_no_pending_recovery(uri)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("limit"), "{err}");
    assert_eq!(files(dir.path()), before);
}
