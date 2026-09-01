use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

use serde_json::Value;
use tempfile::TempDir;

use crate::{
    GuardError, InventoryRow, SURFACE_PUBLIC_RUST, containing_value_fingerprint, path_to_slashes,
    percent_encode, vocabulary_matches,
};

pub(crate) const CARGO_PUBLIC_API_VERSION: &str = "0.52.0";
pub(crate) const PINNED_NIGHTLY: &str = "nightly-2026-08-01";
pub(crate) const SITE_KINDS: &[&str] = &["public_signature"];
const PUBLIC_LIBRARY_PACKAGES: &[&str] = &[
    "omnigraph-api-types",
    "omnigraph-cluster",
    "omnigraph-compiler",
    "omnigraph-engine",
    "omnigraph-policy",
    "omnigraph-server",
    "omnigraph-storage",
];
const INTERNAL_LIBRARY_PACKAGES: &[&str] =
    &["omnigraph-azure-admission", "omnigraph-vocabulary-guard"];

#[derive(Clone, Debug)]
pub(crate) struct ToolConfig {
    pub executable: PathBuf,
    pub target_dir: PathBuf,
}

#[derive(Debug)]
struct Package {
    name: String,
    manifest_path: PathBuf,
    has_non_default_features: bool,
}

pub(crate) fn scan_worktree(
    root: &Path,
    config: &ToolConfig,
) -> Result<Vec<InventoryRow>, GuardError> {
    let root = root.canonicalize().map_err(|error| {
        GuardError::PublicRust(format!(
            "cannot canonicalize public Rust workspace {}: {error}",
            root.display()
        ))
    })?;
    validate_tool(&config.executable)?;
    let packages = workspace_library_packages(&root)?;
    let mut rows: BTreeMap<String, InventoryRow> = BTreeMap::new();
    for package in packages {
        let manifest_path = workspace_relative_manifest_path(&root, &package.manifest_path)?;
        for &feature_mode in feature_modes(&package) {
            let started = Instant::now();
            eprintln!(
                "Graph vocabulary guard: scanning public Rust package {} ({})",
                package.name,
                feature_mode.label()
            );
            let mut command = Command::new(&config.executable);
            command
                .current_dir(&root)
                .env("RUSTUP_TOOLCHAIN", PINNED_NIGHTLY)
                .arg("--manifest-path")
                .arg(root.join("Cargo.toml"))
                .args(["--package", &package.name])
                .args([
                    "--omit",
                    "blanket-impls,auto-trait-impls,auto-derived-impls",
                ])
                .args(["--include", "function-parameter-names"])
                .args(["--color", "never"])
                .arg("--target-dir")
                .arg(&config.target_dir);
            if matches!(feature_mode, FeatureMode::All) {
                command.arg("--all-features");
            }
            let output = command.output().map_err(|error| {
                GuardError::PublicRust(format!(
                    "cannot execute {} for package {} ({}): {error}",
                    config.executable.display(),
                    package.name,
                    feature_mode.label()
                ))
            })?;
            if !output.status.success() {
                return Err(GuardError::PublicRust(format!(
                    "cargo-public-api failed for package {} ({}): {}",
                    package.name,
                    feature_mode.label(),
                    String::from_utf8_lossy(&output.stderr).trim()
                )));
            }
            let stdout = String::from_utf8(output.stdout).map_err(|error| {
                GuardError::PublicRust(format!(
                    "cargo-public-api output for package {} ({}) is not UTF-8: {error}",
                    package.name,
                    feature_mode.label()
                ))
            })?;
            merge_feature_rows(
                &mut rows,
                scan_output(&stdout, &package.name, &manifest_path),
            )?;
            eprintln!(
                "Graph vocabulary guard: scanned public Rust package {} ({}) in {:.1}s",
                package.name,
                feature_mode.label(),
                started.elapsed().as_secs_f64()
            );
        }
    }
    let rows: Vec<_> = rows.into_values().collect();
    ensure_unique(&rows)?;
    Ok(rows)
}

fn workspace_relative_manifest_path(
    canonical_root: &Path,
    manifest_path: &Path,
) -> Result<String, GuardError> {
    let canonical_manifest = manifest_path.canonicalize().map_err(|error| {
        GuardError::PublicRust(format!(
            "cannot canonicalize workspace manifest {}: {error}",
            manifest_path.display()
        ))
    })?;
    canonical_manifest
        .strip_prefix(canonical_root)
        .map(path_to_slashes)
        .map_err(|_| {
            GuardError::PublicRust(format!(
                "workspace manifest {} is outside {}",
                canonical_manifest.display(),
                canonical_root.display()
            ))
        })
}

fn merge_feature_rows(
    rows: &mut BTreeMap<String, InventoryRow>,
    additions: Vec<InventoryRow>,
) -> Result<(), GuardError> {
    for row in additions {
        match rows.get(&row.occurrence_id) {
            Some(existing) if existing != &row => {
                return Err(GuardError::PublicRust(format!(
                    "feature-union occurrence ID collision at {}",
                    row.occurrence_id
                )));
            }
            Some(_) => {}
            None => {
                rows.insert(row.occurrence_id.clone(), row);
            }
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum FeatureMode {
    Default,
    All,
}

fn feature_modes(package: &Package) -> &'static [FeatureMode] {
    const DEFAULT_ONLY: &[FeatureMode] = &[FeatureMode::Default];
    const DEFAULT_AND_ALL: &[FeatureMode] = &[FeatureMode::Default, FeatureMode::All];

    if package.has_non_default_features {
        DEFAULT_AND_ALL
    } else {
        // `--all-features` is byte-for-byte the default feature set when a
        // package declares no non-default features. Running it again produces
        // no additional public surface and needlessly repeats rustdoc work.
        DEFAULT_ONLY
    }
}

impl FeatureMode {
    fn label(self) -> &'static str {
        match self {
            Self::Default => "default features",
            Self::All => "all features",
        }
    }
}

pub(crate) fn scan_git_commit(
    repository_root: &Path,
    commit: &str,
    config: &ToolConfig,
) -> Result<Vec<InventoryRow>, GuardError> {
    let archive = TempDir::new().map_err(|error| {
        GuardError::PublicRust(format!(
            "cannot create merge-base archive directory: {error}"
        ))
    })?;
    let tar_path = archive.path().join("tree.tar");
    let tree_path = archive.path().join("tree");
    fs::create_dir(&tree_path).map_err(|error| {
        GuardError::PublicRust(format!("cannot create merge-base tree directory: {error}"))
    })?;

    let output = Command::new("git")
        .arg("-C")
        .arg(repository_root)
        .args(["archive", "--format=tar"])
        .arg(format!("--output={}", tar_path.display()))
        .arg(commit)
        .output()
        .map_err(|error| GuardError::Git(format!("cannot archive merge base {commit}: {error}")))?;
    if !output.status.success() {
        return Err(GuardError::Git(format!(
            "cannot archive merge base {commit}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    let output = Command::new("tar")
        .args(["-xf"])
        .arg(&tar_path)
        .args(["-C"])
        .arg(&tree_path)
        .output()
        .map_err(|error| GuardError::PublicRust(format!("cannot extract merge base: {error}")))?;
    if !output.status.success() {
        return Err(GuardError::PublicRust(format!(
            "cannot extract merge base: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }

    scan_worktree(&tree_path, config)
}

pub(crate) fn scan_output(output: &str, package: &str, manifest_path: &str) -> Vec<InventoryRow> {
    let mut rows = Vec::new();
    let mut duplicate_signatures: BTreeMap<String, usize> = BTreeMap::new();
    for raw_line in output.lines() {
        let signature = normalize_signature(raw_line);
        if signature.is_empty() {
            continue;
        }
        let fingerprint = containing_value_fingerprint(&signature);
        let signature_ordinal = duplicate_signatures.entry(fingerprint.clone()).or_default();
        *signature_ordinal += 1;
        let mut term_ordinals: BTreeMap<String, usize> = BTreeMap::new();
        for found in vocabulary_matches(&signature) {
            let term_ordinal = term_ordinals.entry(found.term.clone()).or_default();
            *term_ordinal += 1;
            rows.push(InventoryRow::observation(
                format!(
                    "{SURFACE_PUBLIC_RUST}:{}:{fingerprint}:{}:{}:{}",
                    percent_encode(package),
                    percent_encode(&found.term),
                    signature_ordinal,
                    term_ordinal
                ),
                SURFACE_PUBLIC_RUST,
                manifest_path,
                &signature,
                "public_signature",
                found.term,
            ));
        }
    }
    rows.sort_by(|left, right| left.occurrence_id.cmp(&right.occurrence_id));
    rows
}

fn validate_tool(executable: &Path) -> Result<(), GuardError> {
    let output = Command::new(executable)
        .arg("--version")
        .output()
        .map_err(|error| {
            GuardError::PublicRust(format!(
                "cannot execute pinned cargo-public-api at {}: {error}",
                executable.display()
            ))
        })?;
    if !output.status.success() {
        return Err(GuardError::PublicRust(format!(
            "cannot read cargo-public-api version at {}: {}",
            executable.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    let version = String::from_utf8_lossy(&output.stdout);
    let expected = format!("cargo-public-api {CARGO_PUBLIC_API_VERSION}");
    if version.trim() != expected {
        return Err(GuardError::PublicRust(format!(
            "cargo-public-api must be exactly {CARGO_PUBLIC_API_VERSION}, found {:?}",
            version.trim()
        )));
    }
    Ok(())
}

fn workspace_library_packages(root: &Path) -> Result<Vec<Package>, GuardError> {
    let output = Command::new("cargo")
        .current_dir(root)
        .args(["metadata", "--format-version", "1", "--no-deps", "--locked"])
        .output()
        .map_err(|error| GuardError::PublicRust(format!("cannot run cargo metadata: {error}")))?;
    if !output.status.success() {
        return Err(GuardError::PublicRust(format!(
            "cargo metadata failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    let metadata: Value = serde_json::from_slice(&output.stdout).map_err(|error| {
        GuardError::PublicRust(format!("cannot parse cargo metadata JSON: {error}"))
    })?;
    let workspace_members: BTreeSet<_> = metadata
        .get("workspace_members")
        .and_then(Value::as_array)
        .ok_or_else(|| GuardError::PublicRust("cargo metadata lacks workspace_members".to_owned()))?
        .iter()
        .filter_map(Value::as_str)
        .collect();
    let packages = metadata
        .get("packages")
        .and_then(Value::as_array)
        .ok_or_else(|| GuardError::PublicRust("cargo metadata lacks packages".to_owned()))?;
    let mut by_name = BTreeMap::new();
    for package in packages {
        let id = package
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if !workspace_members.contains(id) {
            continue;
        }
        let has_library = package
            .get("targets")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .any(|target| {
                target
                    .get("kind")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .any(|kind| kind.as_str() == Some("lib"))
            });
        if !has_library {
            continue;
        }
        let name = package
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| GuardError::PublicRust("package lacks name".to_owned()))?;
        let manifest_path = package
            .get("manifest_path")
            .and_then(Value::as_str)
            .ok_or_else(|| GuardError::PublicRust(format!("package {name} lacks manifest_path")))?;
        let features = package
            .get("features")
            .and_then(Value::as_object)
            .ok_or_else(|| GuardError::PublicRust(format!("package {name} lacks features")))?;
        by_name.insert(
            name.to_owned(),
            Package {
                name: name.to_owned(),
                manifest_path: PathBuf::from(manifest_path),
                has_non_default_features: features.keys().any(|feature| feature != "default"),
            },
        );
    }
    let discovered: BTreeSet<_> = by_name.keys().map(String::as_str).collect();
    let expected: BTreeSet<_> = PUBLIC_LIBRARY_PACKAGES.iter().copied().collect();
    let missing: Vec<_> = expected.difference(&discovered).copied().collect();
    if !missing.is_empty() {
        return Err(GuardError::PublicRust(format!(
            "expected public library package(s) missing from cargo metadata: {}",
            missing.join(", ")
        )));
    }
    let internal: BTreeSet<_> = INTERNAL_LIBRARY_PACKAGES.iter().copied().collect();
    let unexpected: Vec<_> = discovered
        .difference(&expected)
        .copied()
        .filter(|name| !internal.contains(name))
        .collect();
    if !unexpected.is_empty() {
        return Err(GuardError::PublicRust(format!(
            "unclassified workspace library package(s); review public reachability before updating the allowlist: {}",
            unexpected.join(", ")
        )));
    }
    let mut output: Vec<_> = PUBLIC_LIBRARY_PACKAGES
        .iter()
        .map(|name| by_name.remove(*name).expect("presence checked above"))
        .collect();
    output.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(output)
}

fn ensure_unique(rows: &[InventoryRow]) -> Result<(), GuardError> {
    let mut ids = BTreeSet::new();
    for row in rows {
        if !ids.insert(&row.occurrence_id) {
            return Err(GuardError::PublicRust(format!(
                "public API occurrence ID collision at {}",
                row.occurrence_id
            )));
        }
    }
    Ok(())
}

fn normalize_signature(value: &str) -> String {
    let mut value = value.split_whitespace().collect::<Vec<_>>().join(" ");
    for (from, to) in [
        ("( ", "("),
        (" )", ")"),
        ("[ ", "["),
        (" ]", "]"),
        ("< ", "<"),
        (" >", ">"),
        (" ,", ","),
        (" :", ":"),
        (":: ", "::"),
        (" ::", "::"),
    ] {
        value = value.replace(from, to);
    }
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    use std::os::unix::fs::symlink;

    #[test]
    fn public_api_output_covers_symbols_fields_and_parameter_names() {
        let fixture = r#"
pub struct omnigraph::SnapshotTable
pub omnigraph::SnapshotTable::table_key: alloc::string::String
pub fn omnigraph::Omnigraph::entity_at_target(&self, table_key: &str, row_id: &str) -> Result
pub struct omnigraph::ManifestVersion
pub fn omnigraph::ordinary(value: usize)
"#;
        let rows = scan_output(fixture, "omnigraph-engine", "crates/omnigraph/Cargo.toml");
        assert!(rows.iter().any(|row| row.current_term == "Table"));
        assert!(rows.iter().any(|row| row.current_term == "table"));
        assert!(rows.iter().any(|row| row.current_term == "row"));
        assert!(rows.iter().any(|row| row.current_term == "ManifestVersion"));
        assert!(rows.iter().all(|row| row.site_kind == "public_signature"));
        assert!(
            rows.iter()
                .all(|row| row.source_path == "crates/omnigraph/Cargo.toml")
        );
    }

    #[test]
    fn signature_whitespace_does_not_change_identity() {
        let compact = scan_output("pub fn a::table(row_id: &str)", "a", "crates/a/Cargo.toml");
        let spaced = scan_output(
            " pub   fn a::table( row_id: &str ) ",
            "a",
            "crates/a/Cargo.toml",
        );
        assert_eq!(compact, spaced);
    }

    #[test]
    fn expected_package_set_is_explicit() {
        assert_eq!(PUBLIC_LIBRARY_PACKAGES.len(), 7);
        assert!(PUBLIC_LIBRARY_PACKAGES.contains(&"omnigraph-engine"));
        assert!(!PUBLIC_LIBRARY_PACKAGES.contains(&"omnigraph-azure-admission"));
        assert!(!PUBLIC_LIBRARY_PACKAGES.contains(&"omnigraph-vocabulary-guard"));
        assert_eq!(
            INTERNAL_LIBRARY_PACKAGES,
            &["omnigraph-azure-admission", "omnigraph-vocabulary-guard",]
        );
    }

    #[test]
    fn feature_union_deduplicates_default_and_keeps_all_features_only_symbols() {
        let default = scan_output("pub fn a::table(row_id: &str)", "a", "crates/a/Cargo.toml");
        let all_features = scan_output(
            "pub fn a::table(row_id: &str)\npub fn a::feature_column()",
            "a",
            "crates/a/Cargo.toml",
        );
        let mut union = BTreeMap::new();
        merge_feature_rows(&mut union, default.clone()).unwrap();
        merge_feature_rows(&mut union, all_features).unwrap();
        assert_eq!(union.len(), default.len() + 1);
        assert!(union.values().any(|row| row.current_term == "column"));
    }

    #[test]
    fn feature_modes_skip_only_a_provably_identical_all_features_pass() {
        let package = |has_non_default_features| Package {
            name: "example".to_owned(),
            manifest_path: PathBuf::from("crates/example/Cargo.toml"),
            has_non_default_features,
        };

        assert!(matches!(
            feature_modes(&package(false)),
            [FeatureMode::Default]
        ));
        assert!(matches!(
            feature_modes(&package(true)),
            [FeatureMode::Default, FeatureMode::All]
        ));
    }

    #[cfg(unix)]
    #[test]
    fn workspace_alias_and_manifest_use_the_same_canonical_root() {
        let temp = TempDir::new().unwrap();
        let real_root = temp.path().join("real");
        let manifest = real_root.join("crates/example/Cargo.toml");
        fs::create_dir_all(manifest.parent().unwrap()).unwrap();
        fs::write(
            &manifest,
            "[package]\nname = \"example\"\nversion = \"0.0.0\"\n",
        )
        .unwrap();
        let alias_root = temp.path().join("alias");
        symlink(&real_root, &alias_root).unwrap();

        let canonical_root = alias_root.canonicalize().unwrap();
        let canonical_manifest = manifest.canonicalize().unwrap();
        assert_eq!(
            workspace_relative_manifest_path(&canonical_root, &canonical_manifest).unwrap(),
            "crates/example/Cargo.toml"
        );
    }

    #[cfg(unix)]
    #[test]
    fn manifest_symlink_outside_workspace_is_rejected_after_canonicalization() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("workspace");
        let linked_manifest = root.join("crates/example/Cargo.toml");
        fs::create_dir_all(linked_manifest.parent().unwrap()).unwrap();
        let outside_manifest = temp.path().join("outside/Cargo.toml");
        fs::create_dir_all(outside_manifest.parent().unwrap()).unwrap();
        fs::write(
            &outside_manifest,
            "[package]\nname = \"outside\"\nversion = \"0.0.0\"\n",
        )
        .unwrap();
        symlink(&outside_manifest, &linked_manifest).unwrap();

        let canonical_root = root.canonicalize().unwrap();
        let error = workspace_relative_manifest_path(&canonical_root, &linked_manifest)
            .expect_err("an escaped manifest must fail closed");
        assert!(error.to_string().contains("is outside"));
    }
}
