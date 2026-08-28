//! Physical registration, binding, and disposable staging of frozen fixtures.
//!
//! This deliberately does not open an OmniGraph graph, download from object
//! storage, validate logical Data/State, prepare a workload, or make a fixture
//! executable by CaseV1. Physical identity is audit/reset evidence; RFC 0039
//! keeps it out of benchmark point identity.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::Read;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::model::{Diagnostic, ValidationOutcome, typed_sha256, valid_kebab_id};
use crate::reset::{
    PHYSICAL_TREE_DIGEST_ALGORITHM, PhysicalDigest, TraversalLimits, copy_verified,
    digest_physical_tree, verify_physical_tree,
};

pub const REGISTERED_FIXTURE_SOURCE_FORMAT_VERSION: u32 = 1;
const MAX_SOURCE_DESCRIPTOR_BYTES: u64 = 1024 * 1024;
const MAX_FIXTURE_ID_BYTES: usize = 128;
const MAX_FIXTURE_BINDINGS: usize = 64;

/// A location-free declaration of one exact physical tree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegisteredFixtureSourceV1 {
    pub format_version: u32,
    pub fixture_id: String,
    pub physical: RegisteredPhysicalTreeV1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegisteredPhysicalTreeV1 {
    pub digest_algorithm: String,
    pub tree_sha256: String,
    pub files: u64,
    pub bytes: u64,
}

#[derive(Deserialize)]
struct RegisteredFixtureSourceVersionHeader {
    format_version: u32,
    #[serde(flatten)]
    _remaining: BTreeMap<String, serde::de::IgnoredAny>,
}

/// One invocation-local path proven to contain the registered bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VerifiedRegisteredFixtureSourceV1 {
    pub fixture_id: String,
    pub source_descriptor_sha256: String,
    pub canonical_root: PathBuf,
    pub physical: RegisteredPhysicalTreeV1,
}

/// One invocation-local `ID=BUNDLE` mapping.
///
/// `bundle` is transport/configuration, never fixture or benchmark identity.
#[derive(Debug, Clone, PartialEq, Eq)]
struct FixtureBundleBinding {
    fixture_id: String,
    bundle: PathBuf,
}

/// A bundle whose required direct entries and typed source were resolved.
///
/// The source tree is not trusted after this step. The copy preflight
/// re-reads and verifies every source byte while copying it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ResolvedFixtureBundleV1 {
    fixture_id: String,
    source_descriptor_sha256: String,
    canonical_bundle: PathBuf,
    canonical_root: PathBuf,
    source: RegisteredFixtureSourceV1,
}

/// Path-free evidence that one bundle was copied and re-verified in scratch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureCopyPreflightReceiptV1 {
    pub fixture_id: String,
    pub source_descriptor_sha256: String,
    pub physical: RegisteredPhysicalTreeV1,
}

/// Harness-owned disposable staging workspace.
///
/// Dropping this value attempts best-effort deletion. Call [`Self::finish`] when
/// cleanup success must be reported to a user or orchestration layer.
#[derive(Debug)]
struct StagedFixtureBundlesV1 {
    workspace: Option<tempfile::TempDir>,
    fixtures: BTreeMap<String, FixtureCopyPreflightReceiptV1>,
}

impl StagedFixtureBundlesV1 {
    /// Delete the disposable workspace and return path-free staging evidence.
    fn finish(mut self) -> Result<Vec<FixtureCopyPreflightReceiptV1>, Diagnostic> {
        let fixtures = self.fixtures.values().cloned().collect();
        let workspace = self
            .workspace
            .take()
            .expect("staging workspace is present until finish");
        workspace.close().map_err(|error| {
            Diagnostic::error(
                "fixture_preflight_cleanup_failed",
                "$",
                format!("could not remove disposable fixture staging workspace: {error}"),
            )
        })?;
        Ok(fixtures)
    }
}

/// Read one stable local tree and return its location-free copy-source descriptor.
pub fn fingerprint_registered_fixture(
    fixture_id: String,
    root: &Path,
) -> ValidationOutcome<RegisteredFixtureSourceV1> {
    if !valid_kebab_id(&fixture_id) || fixture_id.len() > MAX_FIXTURE_ID_BYTES {
        return ValidationOutcome::failure(vec![Diagnostic::error(
            "invalid_registered_fixture_id",
            "fixture_id",
            "fixture_id must be 1..=128 characters of path-free kebab-case ASCII",
        )]);
    }
    let canonical_root = match canonical_fixture_root(root) {
        Ok(root) => root,
        Err(diagnostic) => return ValidationOutcome::failure(vec![diagnostic]),
    };
    let observed = match digest_physical_tree(&canonical_root, TraversalLimits::default()) {
        Ok(observed) => observed,
        Err(error) => {
            return ValidationOutcome::failure(vec![Diagnostic::error(
                "registered_fixture_fingerprint_failed",
                root.to_string_lossy(),
                format!("could not fingerprint fixture bytes: {error}"),
            )]);
        }
    };
    let source = RegisteredFixtureSourceV1 {
        format_version: REGISTERED_FIXTURE_SOURCE_FORMAT_VERSION,
        fixture_id,
        physical: registered_physical(&observed),
    };
    match seal_source_descriptor(&source) {
        Ok(_) => ValidationOutcome::success(source),
        Err(diagnostics) => ValidationOutcome::failure(diagnostics),
    }
}

/// Re-read every byte and bind a local directory to one registered source.
pub fn verify_registered_fixture(
    source_path: &Path,
    root: &Path,
) -> ValidationOutcome<VerifiedRegisteredFixtureSourceV1> {
    let (source, source_descriptor_sha256) = match load_source_descriptor(source_path) {
        Ok(loaded) => loaded,
        Err(diagnostics) => return ValidationOutcome::failure(diagnostics),
    };
    let canonical_root = match canonical_fixture_root(root) {
        Ok(root) => root,
        Err(diagnostic) => return ValidationOutcome::failure(vec![diagnostic]),
    };
    let canonical_source = match fs::canonicalize(source_path) {
        Ok(path) => path,
        Err(error) => {
            return ValidationOutcome::failure(vec![Diagnostic::error(
                "fixture_source_read_error",
                source_path.to_string_lossy(),
                format!("could not canonicalize fixture source descriptor: {error}"),
            )]);
        }
    };
    if canonical_source.starts_with(&canonical_root) {
        return ValidationOutcome::failure(vec![Diagnostic::error(
            "fixture_source_inside_root",
            source_path.to_string_lossy(),
            "fixture source descriptor must live outside the graph root it identifies",
        )]);
    }

    let expected = PhysicalDigest {
        files: source.physical.files,
        bytes: source.physical.bytes,
        digest_sha256: source.physical.tree_sha256.clone(),
    };
    if let Err(error) = verify_physical_tree(&canonical_root, &expected, TraversalLimits::default())
    {
        return ValidationOutcome::failure(vec![Diagnostic::error(
            "registered_fixture_verification_failed",
            root.to_string_lossy(),
            format!("could not verify registered fixture bytes: {error}"),
        )]);
    }

    ValidationOutcome::success(VerifiedRegisteredFixtureSourceV1 {
        fixture_id: source.fixture_id,
        source_descriptor_sha256,
        canonical_root,
        physical: source.physical,
    })
}

/// Resolve `ID=BUNDLE` mappings, verify each source while copying it through
/// harness-owned scratch, verify each copy, and remove the workspace.
///
/// Success is physical preflight evidence only. It does not claim that a tree
/// is an executable benchmark fixture or that its observed descriptor digest
/// matches an independently registered expectation.
pub fn preflight_copy_fixture_bindings(
    values: &[String],
    scratch_root: Option<&Path>,
) -> ValidationOutcome<Vec<FixtureCopyPreflightReceiptV1>> {
    let resolved = match resolve_fixture_bundle_bindings(values).into_result() {
        Ok(resolved) => resolved,
        Err(diagnostics) => return ValidationOutcome::failure(diagnostics),
    };
    let staged = match stage_fixture_bundles(&resolved, scratch_root).into_result() {
        Ok(staged) => staged,
        Err(diagnostics) => return ValidationOutcome::failure(diagnostics),
    };
    match staged.finish() {
        Ok(receipts) => ValidationOutcome::success(receipts),
        Err(diagnostic) => ValidationOutcome::failure(vec![diagnostic]),
    }
}

/// Parse one invocation-local `ID=BUNDLE` argument.
///
/// Only the first `=` is structural, so bundle paths may themselves contain
/// `=`. The bundle path is deliberately not canonicalized until resolution.
fn parse_fixture_bundle_binding(value: &str) -> Result<FixtureBundleBinding, Diagnostic> {
    let Some((fixture_id, bundle)) = value.split_once('=') else {
        return Err(Diagnostic::error(
            "invalid_fixture_binding",
            "--fixture",
            "fixture binding must have the form ID=BUNDLE",
        ));
    };
    if !valid_kebab_id(fixture_id) || fixture_id.len() > MAX_FIXTURE_ID_BYTES {
        return Err(Diagnostic::error(
            "invalid_fixture_binding",
            "--fixture",
            "fixture binding ID must be 1..=128 characters of path-free kebab-case ASCII",
        ));
    }
    if bundle.is_empty() {
        return Err(Diagnostic::error(
            "invalid_fixture_binding",
            "--fixture",
            "fixture binding BUNDLE path must not be empty",
        ));
    }
    Ok(FixtureBundleBinding {
        fixture_id: fixture_id.to_string(),
        bundle: PathBuf::from(bundle),
    })
}

/// Parse and resolve a complete set of invocation-local fixture bindings.
fn resolve_fixture_bundle_bindings(
    values: &[String],
) -> ValidationOutcome<Vec<ResolvedFixtureBundleV1>> {
    if values.is_empty() {
        return ValidationOutcome::failure(vec![Diagnostic::error(
            "missing_fixture_binding",
            "--fixture",
            "at least one ID=BUNDLE fixture binding is required",
        )]);
    }
    if values.len() > MAX_FIXTURE_BINDINGS {
        return ValidationOutcome::failure(vec![Diagnostic::error(
            "fixture_binding_budget_exceeded",
            "--fixture",
            format!("at most {MAX_FIXTURE_BINDINGS} fixture bindings are allowed"),
        )]);
    }

    let mut diagnostics = Vec::new();
    let mut bindings = Vec::with_capacity(values.len());
    for (index, value) in values.iter().enumerate() {
        match parse_fixture_bundle_binding(value) {
            Ok(binding) => bindings.push(binding),
            Err(mut diagnostic) => {
                diagnostic.path = format!("--fixture[{index}]");
                diagnostics.push(diagnostic);
            }
        }
    }
    if !diagnostics.is_empty() {
        return ValidationOutcome::failure(diagnostics);
    }
    resolve_fixture_bundles(&bindings)
}

/// Resolve bundle layout and source identity without hashing the graph root.
///
/// A bundle has two required direct entries: `fixture-source.json` and `root/`.
/// Unrelated siblings are ignored. This phase is cheap enough to run before
/// release-build guards, scratch creation, or archive publication.
fn resolve_fixture_bundles(
    bindings: &[FixtureBundleBinding],
) -> ValidationOutcome<Vec<ResolvedFixtureBundleV1>> {
    if bindings.is_empty() {
        return ValidationOutcome::failure(vec![Diagnostic::error(
            "missing_fixture_binding",
            "--fixture",
            "at least one ID=BUNDLE fixture binding is required",
        )]);
    }
    if bindings.len() > MAX_FIXTURE_BINDINGS {
        return ValidationOutcome::failure(vec![Diagnostic::error(
            "fixture_binding_budget_exceeded",
            "--fixture",
            format!("at most {MAX_FIXTURE_BINDINGS} fixture bindings are allowed"),
        )]);
    }

    let mut diagnostics = Vec::new();
    let mut seen = BTreeSet::new();
    for (index, binding) in bindings.iter().enumerate() {
        let path = format!("--fixture[{index}]");
        if !valid_kebab_id(&binding.fixture_id)
            || binding.fixture_id.len() > MAX_FIXTURE_ID_BYTES
            || binding.bundle.as_os_str().is_empty()
        {
            diagnostics.push(Diagnostic::error(
                "invalid_fixture_binding",
                &path,
                "fixture binding must contain a valid path-free kebab-case ID and a non-empty BUNDLE path",
            ));
            continue;
        }
        if !seen.insert(binding.fixture_id.clone()) {
            diagnostics.push(Diagnostic::error(
                "duplicate_fixture_binding",
                &path,
                format!(
                    "fixture id '{}' is bound more than once",
                    binding.fixture_id
                ),
            ));
            continue;
        }
    }
    if !diagnostics.is_empty() {
        return ValidationOutcome::failure(diagnostics);
    }

    let mut resolved = Vec::with_capacity(bindings.len());
    for (index, binding) in bindings.iter().enumerate() {
        let path = format!("--fixture[{index}]");
        match resolve_fixture_bundle(binding) {
            Ok(bundle) => resolved.push(bundle),
            Err(mut bundle_diagnostics) => {
                for diagnostic in &mut bundle_diagnostics {
                    diagnostic.path = format!("{path}.{}", diagnostic.path);
                }
                diagnostics.extend(bundle_diagnostics);
            }
        }
    }
    if diagnostics.is_empty() {
        resolved.sort_by(|left, right| left.fixture_id.cmp(&right.fixture_id));
        ValidationOutcome::success(resolved)
    } else {
        ValidationOutcome::failure(diagnostics)
    }
}

/// Copy resolved sources into one harness-owned disposable workspace.
///
/// The source is verified against its descriptor before any destination bytes
/// are written, and the completed destination is re-digested before success.
/// Neither source nor destination is opened as an OmniGraph database here.
fn stage_fixture_bundles(
    bundles: &[ResolvedFixtureBundleV1],
    scratch_root: Option<&Path>,
) -> ValidationOutcome<StagedFixtureBundlesV1> {
    if bundles.is_empty() {
        return ValidationOutcome::failure(vec![Diagnostic::error(
            "missing_fixture_binding",
            "fixtures",
            "at least one resolved fixture bundle is required for staging",
        )]);
    }
    if bundles.len() > MAX_FIXTURE_BINDINGS {
        return ValidationOutcome::failure(vec![Diagnostic::error(
            "fixture_binding_budget_exceeded",
            "fixtures",
            format!("at most {MAX_FIXTURE_BINDINGS} fixture bundles may be staged"),
        )]);
    }
    let mut ids = BTreeSet::new();
    if let Some(duplicate) = bundles
        .iter()
        .find(|bundle| !ids.insert(bundle.fixture_id.as_str()))
    {
        return ValidationOutcome::failure(vec![Diagnostic::error(
            "duplicate_fixture_binding",
            "fixtures",
            format!(
                "resolved fixture id '{}' appears more than once",
                duplicate.fixture_id
            ),
        )]);
    }

    let staging_base = match canonical_staging_base(scratch_root) {
        Ok(base) => base,
        Err(diagnostic) => return ValidationOutcome::failure(vec![diagnostic]),
    };
    if let Some(bundle) = bundles
        .iter()
        .find(|bundle| staging_base.starts_with(&bundle.canonical_bundle))
    {
        return ValidationOutcome::failure(vec![Diagnostic::error(
            "fixture_preflight_scratch_inside_bundle",
            "--scratch-root",
            format!(
                "fixture staging scratch must not equal or lie below bundle for '{}'",
                bundle.fixture_id
            ),
        )]);
    }

    let workspace = match staging_workspace(&staging_base) {
        Ok(workspace) => workspace,
        Err(diagnostic) => return ValidationOutcome::failure(vec![diagnostic]),
    };

    let limits = TraversalLimits::default();
    let mut staged = BTreeMap::new();
    for bundle in bundles {
        let fixture_directory = workspace.path().join(&bundle.fixture_id);
        if let Err(error) = fs::create_dir(&fixture_directory) {
            return stage_failure(
                workspace,
                vec![Diagnostic::error(
                    "fixture_preflight_copy_failed",
                    &bundle.fixture_id,
                    format!("could not create private fixture staging directory: {error}"),
                )],
            );
        }
        let staged_root = fixture_directory.join("root");
        let expected = physical_digest(&bundle.source.physical);
        let copied = match copy_verified(&bundle.canonical_root, &staged_root, &expected, limits) {
            Ok(copied) => copied,
            Err(error) => {
                return stage_failure(
                    workspace,
                    vec![Diagnostic::error(
                        "fixture_preflight_copy_failed",
                        &bundle.fixture_id,
                        format!(
                            "could not verify and copy registered fixture into disposable scratch: {error}"
                        ),
                    )],
                );
            }
        };
        if let Err(error) = fs::remove_dir_all(&fixture_directory) {
            return stage_failure(
                workspace,
                vec![Diagnostic::error(
                    "fixture_preflight_cleanup_failed",
                    &bundle.fixture_id,
                    format!(
                        "could not remove verified fixture copy before processing the next binding: {error}"
                    ),
                )],
            );
        }
        staged.insert(
            bundle.fixture_id.clone(),
            FixtureCopyPreflightReceiptV1 {
                fixture_id: bundle.fixture_id.clone(),
                source_descriptor_sha256: bundle.source_descriptor_sha256.clone(),
                physical: registered_physical(&copied),
            },
        );
    }

    ValidationOutcome::success(StagedFixtureBundlesV1 {
        workspace: Some(workspace),
        fixtures: staged,
    })
}

fn resolve_fixture_bundle(
    binding: &FixtureBundleBinding,
) -> Result<ResolvedFixtureBundleV1, Vec<Diagnostic>> {
    let metadata = fs::symlink_metadata(&binding.bundle).map_err(|error| {
        vec![Diagnostic::error(
            "invalid_fixture_bundle_layout",
            "bundle",
            format!("could not inspect fixture bundle: {error}"),
        )]
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(vec![Diagnostic::error(
            "invalid_fixture_bundle_layout",
            "bundle",
            "fixture bundle must be a real directory, not a symlink or special file",
        )]);
    }
    let canonical_bundle = fs::canonicalize(&binding.bundle).map_err(|error| {
        vec![Diagnostic::error(
            "invalid_fixture_bundle_layout",
            "bundle",
            format!("could not canonicalize fixture bundle: {error}"),
        )]
    })?;
    if canonical_bundle.to_str().is_none() {
        return Err(vec![Diagnostic::error(
            "invalid_fixture_bundle_layout",
            "bundle",
            "fixture bundle must have a canonical UTF-8 path",
        )]);
    }

    let source_path = canonical_bundle.join("fixture-source.json");
    let root_path = canonical_bundle.join("root");
    let canonical_root = canonical_fixture_root(&root_path).map_err(|diagnostic| {
        vec![Diagnostic::error(
            "invalid_fixture_bundle_layout",
            "bundle.root",
            diagnostic.message,
        )]
    })?;
    if canonical_root.parent() != Some(canonical_bundle.as_path()) {
        return Err(vec![Diagnostic::error(
            "invalid_fixture_bundle_layout",
            "bundle.root",
            "bundle root must be a real direct child named 'root'",
        )]);
    }

    let (source, source_descriptor_sha256) = load_source_descriptor(&source_path)?;
    let canonical_source = fs::canonicalize(&source_path).map_err(|error| {
        vec![Diagnostic::error(
            "invalid_fixture_bundle_layout",
            "bundle.fixture-source.json",
            format!("could not canonicalize fixture source descriptor: {error}"),
        )]
    })?;
    if canonical_source.parent() != Some(canonical_bundle.as_path()) {
        return Err(vec![Diagnostic::error(
            "invalid_fixture_bundle_layout",
            "bundle.fixture-source.json",
            "fixture source descriptor must be a real direct child named 'fixture-source.json'",
        )]);
    }
    if source.fixture_id != binding.fixture_id {
        return Err(vec![Diagnostic::error(
            "fixture_binding_id_mismatch",
            "fixture_id",
            format!(
                "binding id '{}' does not match source fixture_id '{}'",
                binding.fixture_id, source.fixture_id
            ),
        )]);
    }

    Ok(ResolvedFixtureBundleV1 {
        fixture_id: binding.fixture_id.clone(),
        source_descriptor_sha256,
        canonical_bundle,
        canonical_root,
        source,
    })
}

fn canonical_staging_base(scratch_root: Option<&Path>) -> Result<PathBuf, Diagnostic> {
    let root = scratch_root
        .map(Path::to_path_buf)
        .unwrap_or_else(std::env::temp_dir);
    let metadata = fs::symlink_metadata(&root).map_err(|error| {
        Diagnostic::error(
            "fixture_preflight_scratch_error",
            "--scratch-root",
            format!("could not inspect fixture staging scratch root: {error}"),
        )
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(Diagnostic::error(
            "fixture_preflight_scratch_error",
            "--scratch-root",
            "fixture staging scratch root must be a real existing directory",
        ));
    }
    fs::canonicalize(&root).map_err(|error| {
        Diagnostic::error(
            "fixture_preflight_scratch_error",
            "--scratch-root",
            format!("could not canonicalize fixture staging scratch root: {error}"),
        )
    })
}

fn staging_workspace(staging_base: &Path) -> Result<tempfile::TempDir, Diagnostic> {
    let mut builder = tempfile::Builder::new();
    builder.prefix("omnigraph-bench-fixture-stage-");
    builder.tempdir_in(staging_base).map_err(|error| {
        Diagnostic::error(
            "fixture_preflight_scratch_error",
            "--scratch-root",
            format!("could not create disposable fixture staging workspace: {error}"),
        )
    })
}

fn stage_failure(
    workspace: tempfile::TempDir,
    mut diagnostics: Vec<Diagnostic>,
) -> ValidationOutcome<StagedFixtureBundlesV1> {
    if let Err(error) = workspace.close() {
        diagnostics.push(Diagnostic::error(
            "fixture_preflight_cleanup_failed",
            "$",
            format!("could not remove failed fixture staging workspace: {error}"),
        ));
    }
    ValidationOutcome::failure(diagnostics)
}

fn load_source_descriptor(
    path: &Path,
) -> Result<(RegisteredFixtureSourceV1, String), Vec<Diagnostic>> {
    let source = read_source_descriptor(path).map_err(|diagnostic| vec![diagnostic])?;
    let header: RegisteredFixtureSourceVersionHeader =
        serde_json::from_str(&source).map_err(|error| {
            vec![Diagnostic::error(
                "invalid_fixture_source_json",
                path.to_string_lossy(),
                format!("could not parse fixture source JSON header: {error}"),
            )]
        })?;
    if header.format_version != REGISTERED_FIXTURE_SOURCE_FORMAT_VERSION {
        return Err(vec![Diagnostic::error(
            "unsupported_fixture_source_version",
            "format_version",
            format!(
                "unsupported fixture source version {}; this build supports version {REGISTERED_FIXTURE_SOURCE_FORMAT_VERSION}",
                header.format_version
            ),
        )]);
    }
    let descriptor: RegisteredFixtureSourceV1 = serde_json::from_str(&source).map_err(|error| {
        vec![Diagnostic::error(
            "invalid_fixture_source_json",
            path.to_string_lossy(),
            format!("could not parse fixture source JSON: {error}"),
        )]
    })?;
    let digest = seal_source_descriptor(&descriptor)?;
    Ok((descriptor, digest))
}

fn seal_source_descriptor(
    descriptor: &RegisteredFixtureSourceV1,
) -> Result<String, Vec<Diagnostic>> {
    let mut diagnostics = Vec::new();
    if descriptor.format_version != REGISTERED_FIXTURE_SOURCE_FORMAT_VERSION {
        diagnostics.push(Diagnostic::error(
            "unsupported_fixture_source_version",
            "format_version",
            format!(
                "unsupported fixture source version {}; this build supports version {REGISTERED_FIXTURE_SOURCE_FORMAT_VERSION}",
                descriptor.format_version
            ),
        ));
    }
    if !valid_kebab_id(&descriptor.fixture_id) || descriptor.fixture_id.len() > MAX_FIXTURE_ID_BYTES
    {
        diagnostics.push(Diagnostic::error(
            "invalid_registered_fixture_id",
            "fixture_id",
            "fixture_id must be 1..=128 characters of path-free kebab-case ASCII",
        ));
    }
    if descriptor.physical.digest_algorithm != PHYSICAL_TREE_DIGEST_ALGORITHM {
        diagnostics.push(Diagnostic::error(
            "unsupported_physical_digest_algorithm",
            "physical.digest_algorithm",
            format!("expected '{PHYSICAL_TREE_DIGEST_ALGORITHM}'"),
        ));
    }
    if !is_lowercase_sha256(&descriptor.physical.tree_sha256) {
        diagnostics.push(Diagnostic::error(
            "invalid_physical_tree_sha256",
            "physical.tree_sha256",
            "tree_sha256 must be exactly 64 lowercase hexadecimal characters",
        ));
    }
    if descriptor.physical.files == 0 || descriptor.physical.bytes == 0 {
        diagnostics.push(Diagnostic::error(
            "empty_registered_fixture",
            "physical",
            "a registered benchmark fixture must contain at least one non-empty regular file",
        ));
    }
    let limits = TraversalLimits::default();
    if descriptor.physical.files > limits.max_entries {
        diagnostics.push(Diagnostic::error(
            "registered_fixture_entry_budget_exceeded",
            "physical.files",
            format!("files must be <= {}", limits.max_entries),
        ));
    }
    if descriptor.physical.bytes > limits.max_bytes {
        diagnostics.push(Diagnostic::error(
            "registered_fixture_byte_budget_exceeded",
            "physical.bytes",
            format!("bytes must be <= {}", limits.max_bytes),
        ));
    }
    if diagnostics.is_empty() {
        typed_sha256(descriptor).map_err(|diagnostic| vec![diagnostic])
    } else {
        Err(diagnostics)
    }
}

fn canonical_fixture_root(root: &Path) -> Result<PathBuf, Diagnostic> {
    let metadata = fs::symlink_metadata(root).map_err(|error| {
        Diagnostic::error(
            "registered_fixture_root_error",
            root.to_string_lossy(),
            format!("could not inspect fixture root: {error}"),
        )
    })?;
    if !metadata.file_type().is_dir() {
        return Err(Diagnostic::error(
            "registered_fixture_root_error",
            root.to_string_lossy(),
            "fixture root must be a real directory, not a symlink or special file",
        ));
    }
    let canonical = fs::canonicalize(root).map_err(|error| {
        Diagnostic::error(
            "registered_fixture_root_error",
            root.to_string_lossy(),
            format!("could not canonicalize fixture root: {error}"),
        )
    })?;
    if canonical.to_str().is_none() {
        return Err(Diagnostic::error(
            "registered_fixture_root_error",
            root.to_string_lossy(),
            "fixture root must have a UTF-8 canonical path for machine-readable binding",
        ));
    }
    Ok(canonical)
}

fn read_source_descriptor(path: &Path) -> Result<String, Diagnostic> {
    let metadata = fs::symlink_metadata(path).map_err(|error| source_descriptor_io(path, error))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(Diagnostic::error(
            "fixture_source_read_error",
            path.to_string_lossy(),
            "fixture source descriptor must be a regular file, not a symlink or special file",
        ));
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_NONBLOCK);
    }
    let mut file = options
        .open(path)
        .map_err(|error| source_descriptor_io(path, error))?;
    let metadata = file
        .metadata()
        .map_err(|error| source_descriptor_io(path, error))?;
    if !metadata.is_file() || metadata.len() > MAX_SOURCE_DESCRIPTOR_BYTES {
        return Err(Diagnostic::error(
            "fixture_source_read_error",
            path.to_string_lossy(),
            format!(
                "fixture source descriptor must be a regular file no larger than {MAX_SOURCE_DESCRIPTOR_BYTES} bytes"
            ),
        ));
    }
    let mut source = String::new();
    Read::by_ref(&mut file)
        .take(MAX_SOURCE_DESCRIPTOR_BYTES + 1)
        .read_to_string(&mut source)
        .map_err(|error| source_descriptor_io(path, error))?;
    if source.len() as u64 > MAX_SOURCE_DESCRIPTOR_BYTES {
        return Err(Diagnostic::error(
            "fixture_source_read_error",
            path.to_string_lossy(),
            format!(
                "fixture source descriptor grew beyond {MAX_SOURCE_DESCRIPTOR_BYTES} bytes while being read"
            ),
        ));
    }
    Ok(source)
}

fn source_descriptor_io(path: &Path, error: std::io::Error) -> Diagnostic {
    Diagnostic::error(
        "fixture_source_read_error",
        path.to_string_lossy(),
        format!("could not read fixture source descriptor: {error}"),
    )
}

fn registered_physical(observed: &PhysicalDigest) -> RegisteredPhysicalTreeV1 {
    RegisteredPhysicalTreeV1 {
        digest_algorithm: PHYSICAL_TREE_DIGEST_ALGORITHM.to_string(),
        tree_sha256: observed.digest_sha256.clone(),
        files: observed.files,
        bytes: observed.bytes,
    }
}

fn physical_digest(registered: &RegisteredPhysicalTreeV1) -> PhysicalDigest {
    PhysicalDigest {
        files: registered.files,
        bytes: registered.bytes,
        digest_sha256: registered.tree_sha256.clone(),
    }
}

fn is_lowercase_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(root: &Path) {
        fs::create_dir_all(root.join("tables")).unwrap();
        fs::write(root.join("manifest"), b"graph-head").unwrap();
        fs::write(root.join("tables/nodes.lance"), b"node-bytes").unwrap();
    }

    fn bundle(parent: &Path, id: &str) -> PathBuf {
        let bundle = parent.join(id);
        let root = bundle.join("root");
        fixture(&root);
        let source = fingerprint_registered_fixture(id.to_string(), &root)
            .into_result()
            .unwrap();
        fs::write(
            bundle.join("fixture-source.json"),
            serde_json::to_vec(&source).unwrap(),
        )
        .unwrap();
        bundle
    }

    #[test]
    fn source_descriptor_is_strict_and_canonically_hashed() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("root");
        fixture(&root);
        let source = fingerprint_registered_fixture("monarch-main-20260829".into(), &root)
            .into_result()
            .unwrap();
        let reordered = format!(
            "{{\"physical\":{},\"fixture_id\":\"{}\",\"format_version\":1}}",
            serde_json::to_string(&source.physical).unwrap(),
            source.fixture_id
        );
        let path = directory.path().join("fixture-source.json");
        fs::write(&path, serde_json::to_vec(&source).unwrap()).unwrap();
        let (_, first) = load_source_descriptor(&path).unwrap();
        fs::write(&path, reordered).unwrap();
        let (_, second) = load_source_descriptor(&path).unwrap();
        assert_eq!(first, second);

        let unknown = serde_json::json!({
            "format_version": 1,
            "fixture_id": "monarch-main-20260829",
            "physical": source.physical,
            "unknown": true
        });
        fs::write(&path, serde_json::to_vec(&unknown).unwrap()).unwrap();
        assert_eq!(
            load_source_descriptor(&path).unwrap_err()[0].code,
            "invalid_fixture_source_json"
        );

        fs::write(&path, r#"{"format_version":2,"future_shape":true}"#).unwrap();
        assert_eq!(
            load_source_descriptor(&path).unwrap_err()[0].code,
            "unsupported_fixture_source_version"
        );
        fs::write(&path, r#"{"format_version":1,"format_version":1}"#).unwrap();
        assert_eq!(
            load_source_descriptor(&path).unwrap_err()[0].code,
            "invalid_fixture_source_json"
        );
    }

    #[test]
    fn exact_tree_verifies_and_same_length_drift_fails() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("root");
        fixture(&root);
        let source = fingerprint_registered_fixture("monarch-main-20260829".into(), &root)
            .into_result()
            .unwrap();
        let path = directory.path().join("fixture-source.json");
        fs::write(&path, serde_json::to_vec(&source).unwrap()).unwrap();
        let verified = verify_registered_fixture(&path, &root)
            .into_result()
            .unwrap();
        assert_eq!(verified.physical, source.physical);

        fs::write(root.join("manifest"), b"other-head").unwrap();
        assert_eq!(
            verify_registered_fixture(&path, &root).diagnostics[0].code,
            "registered_fixture_verification_failed"
        );
    }

    #[test]
    fn binding_parser_and_resolver_are_strict_location_only_configuration() {
        let directory = tempfile::tempdir().unwrap();
        let bundle = bundle(directory.path(), "monarch-main-20260829");
        let path_with_equals = directory.path().join("copy=one");
        fs::rename(&bundle, &path_with_equals).unwrap();
        let value = format!("monarch-main-20260829={}", path_with_equals.display());
        let parsed = parse_fixture_bundle_binding(&value).unwrap();
        assert_eq!(parsed.fixture_id, "monarch-main-20260829");
        assert_eq!(parsed.bundle, path_with_equals);

        let resolved = resolve_fixture_bundle_bindings(std::slice::from_ref(&value))
            .into_result()
            .unwrap();
        assert_eq!(resolved[0].fixture_id, "monarch-main-20260829");
        assert_eq!(resolved[0].source_descriptor_sha256.len(), 64);

        for invalid in ["missing-separator", "=bundle", "bad/id=bundle", "id="] {
            assert_eq!(
                parse_fixture_bundle_binding(invalid).unwrap_err().code,
                "invalid_fixture_binding"
            );
        }
        let duplicate = resolve_fixture_bundle_bindings(&[value.clone(), value]);
        assert!(
            duplicate
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "duplicate_fixture_binding")
        );
        let duplicate_before_io = resolve_fixture_bundle_bindings(&[
            "same-id=/definitely/missing/first".to_string(),
            "same-id=/definitely/missing/second".to_string(),
        ]);
        assert_eq!(
            duplicate_before_io.diagnostics[0].code,
            "duplicate_fixture_binding"
        );
    }

    #[test]
    fn bundle_id_and_required_layout_are_bound_before_tree_hashing() {
        let directory = tempfile::tempdir().unwrap();
        let bundle = bundle(directory.path(), "monarch-main-20260829");
        let mismatch = FixtureBundleBinding {
            fixture_id: "fin-graph-main-20260829".to_string(),
            bundle: bundle.clone(),
        };
        assert_eq!(
            resolve_fixture_bundles(&[mismatch]).diagnostics[0].code,
            "fixture_binding_id_mismatch"
        );

        fs::remove_file(bundle.join("fixture-source.json")).unwrap();
        let missing = FixtureBundleBinding {
            fixture_id: "monarch-main-20260829".to_string(),
            bundle,
        };
        assert!(
            resolve_fixture_bundles(&[missing])
                .diagnostics
                .iter()
                .any(|diagnostic| { diagnostic.code == "fixture_source_read_error" })
        );
    }

    #[test]
    fn staging_verifies_both_sides_and_removes_owned_scratch() {
        let directory = tempfile::tempdir().unwrap();
        let bundle = bundle(directory.path(), "monarch-main-20260829");
        let scratch = directory.path().join("scratch");
        fs::create_dir(&scratch).unwrap();
        let source_before =
            digest_physical_tree(&bundle.join("root"), TraversalLimits::default()).unwrap();
        let binding = format!("monarch-main-20260829={}", bundle.display());
        let resolved = resolve_fixture_bundle_bindings(&[binding])
            .into_result()
            .unwrap();
        let staged = stage_fixture_bundles(&resolved, Some(&scratch))
            .into_result()
            .unwrap();
        assert_eq!(
            fs::read_dir(staged.workspace.as_ref().unwrap().path())
                .unwrap()
                .count(),
            0
        );
        assert_eq!(
            fs::read(bundle.join("root/manifest")).unwrap(),
            b"graph-head"
        );
        let receipts = staged.finish().unwrap();
        assert_eq!(receipts.len(), 1);
        assert_eq!(
            digest_physical_tree(&bundle.join("root"), TraversalLimits::default()).unwrap(),
            source_before
        );
        assert_eq!(fs::read_dir(&scratch).unwrap().count(), 0);
    }

    #[test]
    fn source_drift_after_resolution_fails_and_cleans_partial_scratch() {
        let directory = tempfile::tempdir().unwrap();
        let bundle = bundle(directory.path(), "monarch-main-20260829");
        let scratch = directory.path().join("scratch");
        fs::create_dir(&scratch).unwrap();
        let binding = format!("monarch-main-20260829={}", bundle.display());
        let resolved = resolve_fixture_bundle_bindings(&[binding])
            .into_result()
            .unwrap();
        fs::write(bundle.join("root/manifest"), b"other-head").unwrap();

        let outcome = stage_fixture_bundles(&resolved, Some(&scratch));

        assert_eq!(outcome.diagnostics[0].code, "fixture_preflight_copy_failed");
        assert_eq!(fs::read_dir(&scratch).unwrap().count(), 0);
    }

    #[test]
    fn later_bundle_failure_removes_an_earlier_completed_copy() {
        let directory = tempfile::tempdir().unwrap();
        let first = bundle(directory.path(), "a-graph-main-20260829");
        let second = bundle(directory.path(), "z-graph-main-20260829");
        let scratch = directory.path().join("scratch");
        fs::create_dir(&scratch).unwrap();
        let resolved = resolve_fixture_bundle_bindings(&[
            format!("a-graph-main-20260829={}", first.display()),
            format!("z-graph-main-20260829={}", second.display()),
        ])
        .into_result()
        .unwrap();
        fs::write(second.join("root/manifest"), b"other-head").unwrap();

        let outcome = stage_fixture_bundles(&resolved, Some(&scratch));

        assert_eq!(outcome.diagnostics[0].code, "fixture_preflight_copy_failed");
        assert_eq!(fs::read_dir(&scratch).unwrap().count(), 0);
    }

    #[test]
    fn staging_never_creates_scratch_inside_the_registered_bundle() {
        let directory = tempfile::tempdir().unwrap();
        let bundle = bundle(directory.path(), "monarch-main-20260829");
        let binding = format!("monarch-main-20260829={}", bundle.display());
        let resolved = resolve_fixture_bundle_bindings(&[binding])
            .into_result()
            .unwrap();
        let descendant = bundle.join("scratch");
        fs::create_dir(&descendant).unwrap();

        for scratch in [&bundle, &descendant] {
            let outcome = stage_fixture_bundles(&resolved, Some(scratch));
            assert_eq!(
                outcome.diagnostics[0].code,
                "fixture_preflight_scratch_inside_bundle"
            );
        }
        assert_eq!(fs::read_dir(&descendant).unwrap().count(), 0);
        assert_eq!(fs::read_dir(bundle.join("root")).unwrap().count(), 2);
    }

    #[cfg(unix)]
    #[test]
    fn aliases_and_invalid_semantics_fail_closed() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("root");
        fixture(&root);
        let source = fingerprint_registered_fixture("monarch-main-20260829".into(), &root)
            .into_result()
            .unwrap();
        let path = directory.path().join("fixture-source.json");
        fs::write(&path, serde_json::to_vec(&source).unwrap()).unwrap();

        let link = directory.path().join("fixture-link.json");
        symlink(&path, &link).unwrap();
        assert_eq!(
            verify_registered_fixture(&link, &root).diagnostics[0].code,
            "fixture_source_read_error"
        );
        let root_link = directory.path().join("root-link");
        symlink(&root, &root_link).unwrap();
        assert_eq!(
            verify_registered_fixture(&path, &root_link).diagnostics[0].code,
            "registered_fixture_root_error"
        );
        let inside = root.join("fixture-source.json");
        fs::write(&inside, serde_json::to_vec(&source).unwrap()).unwrap();
        assert_eq!(
            verify_registered_fixture(&inside, &root).diagnostics[0].code,
            "fixture_source_inside_root"
        );

        let mut invalid = source;
        invalid.format_version = 2;
        invalid.fixture_id = "../monarch".into();
        invalid.physical.digest_algorithm = "sha256".into();
        invalid.physical.tree_sha256 = "A".repeat(64);
        invalid.physical.files = 0;
        assert!(seal_source_descriptor(&invalid).is_err());

        let root_link_bundle = directory.path().join("bundle");
        fs::create_dir(&root_link_bundle).unwrap();
        symlink(&root, root_link_bundle.join("root")).unwrap();
        symlink(&path, root_link_bundle.join("fixture-source.json")).unwrap();
        let outcome = resolve_fixture_bundles(&[FixtureBundleBinding {
            fixture_id: "monarch-main-20260829".to_string(),
            bundle: root_link_bundle,
        }]);
        assert_eq!(outcome.diagnostics[0].code, "invalid_fixture_bundle_layout");

        let source_link_bundle = directory.path().join("source-link-bundle");
        fixture(&source_link_bundle.join("root"));
        symlink(&path, source_link_bundle.join("fixture-source.json")).unwrap();
        let outcome = resolve_fixture_bundles(&[FixtureBundleBinding {
            fixture_id: "monarch-main-20260829".to_string(),
            bundle: source_link_bundle,
        }]);
        assert_eq!(outcome.diagnostics[0].code, "fixture_source_read_error");

        let real_scratch = directory.path().join("real-scratch");
        fs::create_dir(&real_scratch).unwrap();
        let scratch_link = directory.path().join("scratch-link");
        symlink(&real_scratch, &scratch_link).unwrap();
        let real_bundle = bundle(directory.path(), "fin-graph-main-20260829");
        let resolved = resolve_fixture_bundle_bindings(&[format!(
            "fin-graph-main-20260829={}",
            real_bundle.display()
        )])
        .into_result()
        .unwrap();
        assert_eq!(
            stage_fixture_bundles(&resolved, Some(&scratch_link)).diagnostics[0].code,
            "fixture_preflight_scratch_error"
        );
    }
}
