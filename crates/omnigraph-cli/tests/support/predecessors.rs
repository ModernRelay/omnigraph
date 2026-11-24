//! Predecessor binaries: older Omnigraph builds a cross-version test runs
//! beside the current tree. `.github/predecessors.toml` holds one row per
//! predecessor; `scripts/predecessors.py provision <row>...` installs or
//! builds them and writes the manifest this module reads, the file
//! `OMNIGRAPH_PREDECESSORS` names, else `target/predecessors/manifest.json`
//! (docs/dev/testing.md, Predecessor binaries).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

use serde_json::Value;

use super::HERMETIC_OPERATOR_HOME;

/// The manifest's variable; unset reads the default store's manifest.
const MANIFEST_VARIABLE: &str = "OMNIGRAPH_PREDECESSORS";

/// One provisioned registry row.
pub struct Predecessor {
    name: String,
    binaries: BTreeMap<String, PathBuf>,
}

impl Predecessor {
    /// The row's `omnigraph` CLI, which every row provides.
    pub fn omnigraph(&self) -> PathBuf {
        self.binary("omnigraph")
    }

    /// One of the executables the row's `binaries` lists.
    pub fn binary(&self, binary: &str) -> PathBuf {
        self.binaries.get(binary).cloned().unwrap_or_else(|| {
            panic!(
                "predecessor {} provides no {binary}; add it to the row's binaries",
                self.name
            )
        })
    }
}

struct Manifest {
    required: bool,
    rows: BTreeMap<String, Entry>,
}

/// A manifest row: the version it reports and its binaries' paths.
struct Entry {
    reports: String,
    binaries: BTreeMap<String, PathBuf>,
}

fn manifest() -> &'static Manifest {
    static MANIFEST: OnceLock<Manifest> = OnceLock::new();
    MANIFEST.get_or_init(|| {
        let (path, named) = match std::env::var_os(MANIFEST_VARIABLE) {
            Some(path) => (PathBuf::from(path), true),
            None => (
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../../target/predecessors/manifest.json"),
                false,
            ),
        };
        let text = match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(error) if !named && error.kind() == std::io::ErrorKind::NotFound => {
                return Manifest {
                    required: false,
                    rows: BTreeMap::new(),
                };
            }
            Err(error) => panic!(
                "cannot read the predecessor manifest {}: {error}",
                path.display()
            ),
        };
        parse(&text).unwrap_or_else(|error| {
            panic!("malformed predecessor manifest {}: {error}", path.display())
        })
    })
}

fn parse(text: &str) -> Result<Manifest, String> {
    let value: Value = serde_json::from_str(text).map_err(|error| error.to_string())?;
    let required = value["required"]
        .as_bool()
        .ok_or("`required` is not a boolean")?;
    let entries = value["predecessors"]
        .as_object()
        .ok_or("`predecessors` is not an object")?;
    let mut rows = BTreeMap::new();
    for (name, entry) in entries {
        let reports = entry["reports"]
            .as_str()
            .ok_or_else(|| format!("row {name} has no `reports`"))?;
        let binaries = entry["binaries"]
            .as_object()
            .ok_or_else(|| format!("row {name} has no `binaries`"))?
            .iter()
            .map(|(binary, path)| {
                path.as_str()
                    .map(|path| (binary.clone(), PathBuf::from(path)))
                    .ok_or_else(|| format!("row {name} binary {binary} is not a path"))
            })
            .collect::<Result<_, _>>()?;
        rows.insert(
            name.clone(),
            Entry {
                reports: reports.to_owned(),
                binaries,
            },
        );
    }
    Ok(Manifest { required, rows })
}

/// The registry row `name`, once its binaries exist and its `omnigraph`
/// reports the version the row records. A row the manifest lacks skips with
/// a line, or fails when the manifest was provisioned with `--require`.
pub fn predecessor(name: &str) -> Option<Predecessor> {
    let manifest = manifest();
    let Some(Entry { reports, binaries }) = manifest.rows.get(name) else {
        assert!(
            !manifest.required,
            "predecessor {name} is required but not provisioned: \
             run `python3 scripts/predecessors.py provision {name}` and name it in the job"
        );
        eprintln!(
            "skipping: predecessor {name} is not provisioned; \
             run `python3 scripts/predecessors.py provision {name}`"
        );
        return None;
    };
    for (binary, path) in binaries {
        assert!(
            path.is_file(),
            "predecessor {name} {binary} is not a file: {}; provision it again",
            path.display()
        );
    }
    let predecessor = Predecessor {
        name: name.to_owned(),
        binaries: binaries.clone(),
    };
    let output = Command::new(predecessor.omnigraph())
        .arg("--version")
        .env("OMNIGRAPH_HOME", HERMETIC_OPERATOR_HOME)
        .env_remove("OMNIGRAPH_CONFIG")
        .output()
        .unwrap_or_else(|error| panic!("cannot run predecessor {name}: {error}"));
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim(),
        reports,
        "predecessor {name} must report the version its registry row records"
    );
    Some(predecessor)
}
