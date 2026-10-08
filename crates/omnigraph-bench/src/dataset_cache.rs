//! Frozen dataset cache. The lock owns the stable active path through builder
//! and repetition quiescence; the template is never opened by an engine.
use crate::case::ResetMode;
use crate::dataset_worker::FixtureBuildHandoff;
use crate::gqt_case::{DatasetBuildPlan, DatasetRecipe, digest as valid_digest};
use crate::registered_fixture::{RegisteredPhysicalTreeV1, load_source_descriptor};
use crate::reset::{self, MetadataDigest, PhysicalDigest, TraversalLimits};
use crate::runner::{RunnerError, RunnerResult};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
pub(crate) const MAX_DATASET_MANIFEST_BYTES: usize = 1024 * 1024;
pub const GQT_ENGINE_DIGEST: &str = env!("GQT_ENGINE_DIGEST");
const MAX_INSPECTION_ENTRIES: usize = 100_000;
const MAX_INSPECTION_PAGE: usize = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceAvailability {
    Available,
    Missing,
    Invalid,
    Unbound,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheState {
    Missing,
    Present,
    Cached,
    Busy,
    Invalid,
    Incomplete,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CacheIdentity {
    pub recipe_sha256: String,
    pub engine_digest: String,
    pub needs_indices: bool,
    pub reset: ResetMode,
    pub logical_content_sha256: String,
    pub tree_sha256: String,
    pub files: u64,
    pub bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CacheInspection {
    pub inspection_version: u32,
    pub source: Option<SourceAvailability>,
    pub cache: CacheState,
    pub key: Option<String>,
    pub path: PathBuf,
    pub identity: Option<CacheIdentity>,
    pub diagnostic: Option<RunnerError>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CachePage {
    pub inspection_version: u32,
    pub cache_root: PathBuf,
    pub root_state: CacheState,
    pub entries: Vec<CacheInspection>,
    pub next_cursor: Option<String>,
}

impl CacheInspection {
    fn new(path: &Path, key: Option<String>) -> Self {
        Self {
            inspection_version: 1,
            source: None,
            cache: CacheState::Unknown,
            key,
            path: path.into(),
            identity: None,
            diagnostic: None,
        }
    }

    fn fail(mut self, state: CacheState, code: &str, message: impl Into<String>) -> Self {
        self.cache = state;
        self.diagnostic = Some(RunnerError::new(code, message));
        self
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatasetManifestV1 {
    pub format_version: u32,
    pub key: String,
    pub recipe_sha256: String,
    pub engine_digest: String,
    pub needs_indices: bool,
    pub reset: ResetMode,
    pub handoff: FixtureBuildHandoff,
}
pub struct DatasetLease {
    pub root: PathBuf,
    pub active: PathBuf,
    pub template: PathBuf,
    pub manifest: DatasetManifestV1,
    pub cache_hit: bool,
    _directory: File,
    #[cfg(unix)]
    _lock: nix::fcntl::Flock<File>,
}
impl DatasetLease {
    pub fn restore(&self) -> RunnerResult<MetadataDigest> {
        self.verify()?;
        let h = &self.manifest.handoff;
        let limits = TraversalLimits::default();
        let prepared = match self.manifest.reset {
            ResetMode::LocalClonefile => reset::accept_clonefile_template_handoff(
                &self.active,
                &self.template,
                h.physical.clone(),
                h.template_metadata.clone(),
                limits,
            )
            .and_then(|t| t.restore_active()),
            ResetMode::PlainCopy => reset::accept_plain_copy_template_handoff(
                &self.active,
                &self.template,
                h.physical.clone(),
                h.template_metadata.clone(),
                limits,
            )
            .and_then(|t| t.restore_active()),
            ResetMode::S3Versioning => return Err(error("unsupported dataset reset")),
        }
        .map_err(|e| error(e.to_string()))?;
        prepared
            .verify_unchanged()
            .map_err(|e| error(e.to_string()))
    }
    pub fn verify(&self) -> RunnerResult<()> {
        self.validate_ownership()?;
        validate_manifest_evidence(&self.manifest).map_err(error)?;

        if is_quarantined(&self.root)? {
            return Err(error(
                "dataset entry is quarantined after uncertain worker containment",
            ));
        }
        reset::verify_metadata_tree(
            &self.template,
            &self.manifest.handoff.template_metadata,
            TraversalLimits::default(),
        )
        .map_err(|e| error(format!("template metadata changed: {e}")))?;
        Ok(())
    }
    pub fn quarantine(&self, why: &str) -> RunnerResult<()> {
        self.validate_ownership()?;
        write_quarantine(&self.root, &self._directory, why)
    }
    pub fn remove_active(&self) -> RunnerResult<()> {
        self.validate_ownership()?;
        if self.active.exists() {
            std::fs::remove_dir_all(&self.active).map_err(|e| error(e.to_string()))?;
        }
        Ok(())
    }
    pub fn validate_ownership(&self) -> RunnerResult<()> {
        #[cfg(unix)]
        {
            validate_entry(&self.root, &self._directory, &self._lock)?;
        }
        Ok(())
    }
    pub fn physical(&self) -> &PhysicalDigest {
        &self.manifest.handoff.physical
    }
}
fn error(message: impl Into<String>) -> RunnerError {
    RunnerError::new("dataset_cache_corrupt", message)
}
fn is_quarantined(root: &Path) -> RunnerResult<bool> {
    match std::fs::symlink_metadata(root.join("quarantined")) {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(error(e.to_string())),
    }
}
fn write_quarantine(root: &Path, directory: &File, why: &str) -> RunnerResult<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_NONBLOCK);
    }
    match options.open(root.join("quarantined")) {
        Ok(mut file) => file
            .write_all(why.as_bytes())
            .and_then(|_| file.sync_all())
            .map_err(|e| error(e.to_string()))?,
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(error(e.to_string())),
    }
    directory.sync_all().map_err(|e| error(e.to_string()))
}

/// Inspect the exact frozen input's cache variant without creating or executing anything.
#[cfg(unix)]
pub fn inspect(
    plan: &DatasetBuildPlan,
    cache: &Path,
    binding: Option<&str>,
    verify: bool,
) -> CacheInspection {
    let mut result = CacheInspection::new(cache, None);
    result.source = Some(SourceAvailability::Available);
    if let Err(message) = plan.revalidate() {
        result.source = Some(SourceAvailability::Invalid);
        return result.fail(CacheState::Unknown, "invalid_dataset", message);
    }
    if let DatasetRecipe::Registered { reference, .. } = &plan.dataset {
        let Some(binding) = binding else {
            result.source = Some(SourceAvailability::Unbound);
            return result.fail(
                CacheState::Unknown,
                "fixture_binding_missing",
                "registered dataset requires --fixture ID=BUNDLE",
            );
        };
        if binding
            .split_once('=')
            .is_none_or(|(id, _)| id != reference.definition.fixture_id)
        {
            result.source = Some(SourceAvailability::Invalid);
            return result.fail(
                CacheState::Unknown,
                "fixture_binding_invalid",
                "expected the registered fixture's ID=BUNDLE binding",
            );
        }
        if let Some((_, bundle)) = binding.split_once('=') {
            for path in [
                PathBuf::from(bundle),
                Path::new(bundle).join("fixture-source.json"),
                Path::new(bundle).join("root"),
            ] {
                match std::fs::symlink_metadata(&path) {
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        result.source = Some(SourceAvailability::Missing);
                        return result.fail(
                            CacheState::Unknown,
                            "fixture_source_missing",
                            e.to_string(),
                        );
                    }
                    Ok(m) if m.file_type().is_symlink() => {
                        result.source = Some(SourceAvailability::Invalid);
                        return result.fail(
                            CacheState::Unknown,
                            "fixture_binding_invalid",
                            "registered source must not be a symlink",
                        );
                    }
                    Err(e) => {
                        result.source = Some(SourceAvailability::Invalid);
                        return result.fail(
                            CacheState::Unknown,
                            "fixture_source_unreadable",
                            e.to_string(),
                        );
                    }
                    _ => {}
                }
            }
        }
    }
    let registered = match registered_physical(plan, binding) {
        Ok(value) => value,
        Err(e) => {
            result.source = Some(SourceAvailability::Invalid);
            return result.fail(CacheState::Unknown, &e.code, e.message);
        }
    };
    let (cache, directory) = match inspection_root(cache) {
        Ok(Some(value)) => value,
        Ok(None) => {
            result.cache = CacheState::Missing;
            return result;
        }
        Err(e) => return result.fail(CacheState::Unknown, &e.code, e.message),
    };
    let key = match cache_key(plan, &cache, registered.as_ref()) {
        Ok(value) => value,
        Err(e) => return result.fail(CacheState::Unknown, &e.code, e.message),
    };
    result = inspect_entry(&cache.join(&key), Some(plan), verify);
    result.source = Some(SourceAvailability::Available);
    if let Err(e) = validate_path(&cache, &directory) {
        return result.fail(CacheState::Unknown, "dataset_cache_changed", e.message);
    }
    result
}

/// List a bounded page of historical entries; no current source or engine is required.
#[cfg(unix)]
pub fn list(
    cache: &Path,
    limit: usize,
    after: Option<&str>,
    verify: bool,
) -> RunnerResult<CachePage> {
    if !(1..=MAX_INSPECTION_PAGE).contains(&limit) {
        return Err(RunnerError::new(
            "invalid_cache_page_limit",
            "cache page limit must be 1..=100",
        ));
    }
    let mut page = CachePage {
        inspection_version: 1,
        cache_root: cache.into(),
        root_state: CacheState::Missing,
        entries: Vec::new(),
        next_cursor: None,
    };
    let Some((cache, directory)) = inspection_root(cache)? else {
        return Ok(page);
    };
    page.cache_root = cache.clone();
    page.root_state = CacheState::Present;
    let names = bounded_names(&cache)?;
    let mut selected = names
        .iter()
        .filter(|name| after.is_none_or(|cursor| name.as_str() > cursor));
    for name in selected.by_ref().take(limit) {
        page.entries
            .push(inspect_entry(&cache.join(name), None, verify));
    }
    if selected.next().is_some() {
        page.next_cursor = page
            .entries
            .last()
            .and_then(|entry| entry.path.file_name())
            .and_then(|name| name.to_str())
            .map(str::to_owned);
    }
    validate_path(&cache, &directory)?;
    Ok(page)
}

#[cfg(unix)]
fn inspection_root(cache: &Path) -> RunnerResult<Option<(PathBuf, File)>> {
    let canonical = match cache.canonicalize() {
        Ok(path) => path,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return match std::fs::symlink_metadata(cache) {
                Err(missing) if missing.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(other) => Err(RunnerError::new(
                    "dataset_cache_unreadable",
                    other.to_string(),
                )),
                Ok(_) => Err(RunnerError::new("dataset_cache_unreadable", e.to_string())),
            };
        }
        Err(e) => return Err(RunnerError::new("dataset_cache_unreadable", e.to_string())),
    };
    match std::fs::symlink_metadata(&canonical) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(RunnerError::new("dataset_cache_unreadable", e.to_string())),
        Ok(m) if !m.is_dir() => {
            return Err(RunnerError::new(
                "dataset_cache_invalid_root",
                "cache root must be a real directory",
            ));
        }
        Ok(_) => {}
    }
    let directory = open_inspection_directory(&canonical)
        .map_err(|e| RunnerError::new("dataset_cache_unreadable", e.to_string()))?;
    validate_path(&canonical, &directory)?;
    Ok(Some((canonical, directory)))
}

#[cfg(unix)]
fn open_inspection_directory(path: &Path) -> std::io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt;
    OpenOptions::new()
        .read(true)
        .custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_DIRECTORY)
        .open(path)
}

fn bounded_names(root: &Path) -> RunnerResult<Vec<String>> {
    let mut names = Vec::new();
    for (index, entry) in std::fs::read_dir(root)
        .map_err(|e| error(e.to_string()))?
        .enumerate()
    {
        if index >= MAX_INSPECTION_ENTRIES {
            return Err(RunnerError::new(
                "dataset_cache_entry_budget",
                "cache directory exceeds 100000 entries",
            ));
        }
        let name = entry
            .map_err(|e| error(e.to_string()))?
            .file_name()
            .into_string()
            .map_err(|_| error("cache entry name is not UTF-8"))?;
        names.push(name);
    }
    names.sort();
    Ok(names)
}

#[cfg(unix)]
fn inspect_entry(
    root: &Path,
    expected: Option<&DatasetBuildPlan>,
    verify: bool,
) -> CacheInspection {
    use nix::fcntl::{Flock, FlockArg};
    use std::os::unix::fs::OpenOptionsExt;
    let key = root
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| valid_digest(name))
        .map(str::to_owned);
    let mut result = CacheInspection::new(root, key);
    if result.key.is_none() {
        return result.fail(
            CacheState::Unknown,
            "dataset_cache_unrecognized_entry",
            "entry name is not a dataset cache key",
        );
    }
    match std::fs::symlink_metadata(root) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            result.cache = CacheState::Missing;
            return result;
        }
        Err(e) => {
            return result.fail(
                CacheState::Unknown,
                "dataset_cache_unreadable",
                e.to_string(),
            );
        }
        Ok(m) if !m.is_dir() => {
            return result.fail(
                CacheState::Invalid,
                "dataset_cache_invalid_entry",
                "cache entry must be a real directory",
            );
        }
        Ok(_) => {}
    }
    let directory = match open_inspection_directory(root) {
        Ok(value) => value,
        Err(e) => return result.fail(CacheState::Unknown, "dataset_cache_changed", e.to_string()),
    };
    let file = match OpenOptions::new()
        .read(true)
        .custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_NONBLOCK)
        .open(root.join("lock"))
    {
        Ok(value) => value,
        Err(e) => {
            return result.fail(
                CacheState::Unknown,
                "dataset_cache_lock_unavailable",
                e.to_string(),
            );
        }
    };
    match file.metadata() {
        Ok(m) if m.is_file() => {}
        _ => {
            return result.fail(
                CacheState::Invalid,
                "dataset_cache_invalid_lock",
                "cache lock must be a regular file",
            );
        }
    }
    let lock = match Flock::lock(file, FlockArg::LockSharedNonblock) {
        Ok(value) => value,
        Err((_, e)) if e == nix::errno::Errno::EWOULDBLOCK => {
            return result.fail(
                CacheState::Busy,
                "dataset_cache_busy",
                "dataset cache entry has an exclusive owner",
            );
        }
        Err((_, e)) => {
            return result.fail(
                CacheState::Unknown,
                "dataset_cache_lock_unavailable",
                e.to_string(),
            );
        }
    };
    if let Err(e) = validate_entry(root, &directory, &lock) {
        return result.fail(CacheState::Unknown, "dataset_cache_changed", e.message);
    }
    result = inspect_owned_entry(result, expected, verify);
    if let Err(e) = validate_entry(root, &directory, &lock) {
        return result.fail(CacheState::Unknown, "dataset_cache_changed", e.message);
    }
    result
}

fn inspect_owned_entry(
    mut result: CacheInspection,
    expected: Option<&DatasetBuildPlan>,
    verify: bool,
) -> CacheInspection {
    let root = result.path.clone();
    match is_quarantined(&root) {
        Ok(true) => {
            return result.fail(
                CacheState::Invalid,
                "dataset_cache_quarantined",
                "cache entry is quarantined",
            );
        }
        Err(e) => return result.fail(CacheState::Unknown, "dataset_cache_unreadable", e.message),
        Ok(false) => {}
    }
    match std::fs::symlink_metadata(root.join("dataset-build.json")) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return match bounded_names(&root) {
                Ok(names) if names.iter().all(|name| name == "lock") => {
                    result.cache = CacheState::Missing;
                    result
                }
                Ok(_) => result.fail(
                    CacheState::Incomplete,
                    "dataset_cache_unpublished",
                    "entry contains unpublished state",
                ),
                Err(e) => result.fail(CacheState::Unknown, "dataset_cache_unreadable", e.message),
            };
        }
        Err(e) => {
            return result.fail(
                CacheState::Unknown,
                "dataset_cache_unreadable",
                e.to_string(),
            );
        }
        Ok(_) => {}
    }
    let manifest: DatasetManifestV1 = match read_json(&root.join("dataset-build.json")) {
        Ok(value) => value,
        Err(e) => return result.fail(CacheState::Invalid, &e.code, e.message),
    };
    let validated = (|| {
        validate_manifest_identity(
            &manifest,
            result.key.as_deref().unwrap_or_default(),
            expected,
        )?;
        validate_manifest_evidence(&manifest).map_err(error)?;
        validate_published_paths(&root)?;
        validate_descriptor(&root, &manifest)?;
        if verify {
            reset::verify_metadata_tree(
                &root.join("root"),
                &manifest.handoff.template_metadata,
                TraversalLimits::default(),
            )
            .map_err(|e| error(e.to_string()))?;
            reset::verify_physical_tree(
                &root.join("root"),
                &manifest.handoff.physical,
                TraversalLimits::default(),
            )
            .map_err(|e| error(e.to_string()))?;
        }
        Ok::<(), RunnerError>(())
    })();
    if let Err(e) = validated {
        return result.fail(CacheState::Invalid, &e.code, e.message);
    }
    result.identity = Some(CacheIdentity {
        recipe_sha256: manifest.recipe_sha256,
        engine_digest: manifest.engine_digest,
        needs_indices: manifest.needs_indices,
        reset: manifest.reset,
        logical_content_sha256: manifest.handoff.summary.logical_content_sha256,
        tree_sha256: manifest.handoff.physical.digest_sha256,
        files: manifest.handoff.physical.files,
        bytes: manifest.handoff.physical.bytes,
    });
    result.cache = if verify {
        CacheState::Cached
    } else {
        CacheState::Present
    };
    result
}

fn validate_manifest_identity(
    manifest: &DatasetManifestV1,
    key: &str,
    plan: Option<&DatasetBuildPlan>,
) -> RunnerResult<()> {
    if manifest.format_version != 1
        || manifest.key != key
        || !valid_digest(&manifest.recipe_sha256)
        || !valid_digest(&manifest.engine_digest)
        || plan.is_some_and(|plan| {
            manifest.recipe_sha256 != plan.recipe_sha256
                || manifest.engine_digest != GQT_ENGINE_DIGEST
                || manifest.needs_indices != plan.needs_indices
                || manifest.reset != plan.reset
        })
    {
        return Err(error("published dataset manifest identity mismatch"));
    }
    Ok(())
}

fn validate_published_paths(root: &Path) -> RunnerResult<()> {
    match std::fs::symlink_metadata(root.join("active")) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(error(e.to_string())),
        Ok(_) => return Err(error("published cache entry has an unretired active store")),
    }
    if !std::fs::symlink_metadata(root.join("root"))
        .map_err(|e| error(e.to_string()))?
        .is_dir()
    {
        return Err(error("published cache template must be a real directory"));
    }
    Ok(())
}

fn validate_descriptor(root: &Path, manifest: &DatasetManifestV1) -> RunnerResult<()> {
    let (descriptor, _) = load_source_descriptor(&root.join("fixture-source.json"))
        .map_err(|e| error(format!("{e:?}")))?;
    let physical = &manifest.handoff.physical;
    if descriptor.fixture_id != format!("gqt-{}", manifest.key)
        || descriptor.physical.tree_sha256 != physical.digest_sha256
        || descriptor.physical.files != physical.files
        || descriptor.physical.bytes != physical.bytes
    {
        return Err(error(
            "registered cache descriptor disagrees with dataset handoff",
        ));
    }
    Ok(())
}

fn registered_physical(
    plan: &DatasetBuildPlan,
    binding: Option<&str>,
) -> RunnerResult<Option<RegisteredPhysicalTreeV1>> {
    let DatasetRecipe::Registered { reference, .. } = &plan.dataset else {
        return Ok(None);
    };
    let binding = binding.ok_or_else(|| {
        RunnerError::new(
            "fixture_binding_missing",
            "registered dataset requires --fixture ID=BUNDLE",
        )
    })?;
    let (id, bundle) = binding
        .split_once('=')
        .ok_or_else(|| RunnerError::new("fixture_binding_invalid", "expected ID=BUNDLE"))?;
    if id != reference.definition.fixture_id {
        return Err(RunnerError::new(
            "fixture_binding_invalid",
            "fixture id differs from case reference",
        ));
    }
    let (descriptor, _) = load_source_descriptor(&Path::new(bundle).join("fixture-source.json"))
        .map_err(|e| RunnerError::new("fixture_binding_invalid", format!("{e:?}")))?;
    if descriptor.fixture_id != reference.definition.fixture_id {
        return Err(RunnerError::new(
            "fixture_binding_invalid",
            "source descriptor fixture ID differs from the reference",
        ));
    }
    let root = std::fs::symlink_metadata(Path::new(bundle).join("root"))
        .map_err(|e| RunnerError::new("fixture_binding_invalid", e.to_string()))?;
    if !root.is_dir() {
        return Err(RunnerError::new(
            "fixture_binding_invalid",
            "registered root must be a real directory",
        ));
    }
    Ok(Some(descriptor.physical))
}

#[cfg(unix)]
pub fn acquire(
    plan: &DatasetBuildPlan,
    cache: &Path,
    executable: &Path,
    no_build: bool,
    binding: Option<&str>,
) -> RunnerResult<DatasetLease> {
    plan.revalidate()
        .map_err(|e| RunnerError::new("invalid_case", e))?;
    let registered_physical = registered_physical(plan, binding)?;
    std::fs::create_dir_all(cache).map_err(|e| error(e.to_string()))?;
    let cache = cache.canonicalize().map_err(|e| error(e.to_string()))?;
    let key = cache_key(plan, &cache, registered_physical.as_ref())?;
    let root = cache.join(&key);
    match std::fs::create_dir(&root) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(error(e.to_string())),
    }
    if !std::fs::symlink_metadata(&root)
        .map_err(|e| error(e.to_string()))?
        .is_dir()
    {
        return Err(error("cache entry is not a real directory"));
    }
    use std::os::unix::fs::OpenOptionsExt;
    let directory = OpenOptions::new()
        .read(true)
        .custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_DIRECTORY)
        .open(&root)
        .map_err(|e| error(e.to_string()))?;
    let lock = lock(&root)?;
    validate_entry(&root, &directory, &lock)?;
    let active = root.join("active");
    let template = root.join("root");
    let stamp = root.join("dataset-build.json");
    if is_quarantined(&root)? {
        return Err(error(format!(
            "{} is quarantined; inspect and remove explicitly",
            root.display()
        )));
    }
    let cache_hit = stamp.exists();
    let manifest = if cache_hit {
        let m: DatasetManifestV1 = read_json(&stamp)?;
        validate_manifest_identity(&m, &key, Some(plan))?;
        validate_published_paths(&root)?;
        m
    } else {
        if no_build {
            return Err(RunnerError::new(
                "dataset_cache_miss",
                "--no-build requires a published matching dataset",
            ));
        }
        let leftovers = std::fs::read_dir(&root)
            .map_err(|e| error(e.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| error(e.to_string()))?;
        if leftovers.iter().any(|entry| entry.file_name() != "lock") {
            validate_entry(&root, &directory, &lock)?;
            write_quarantine(
                &root,
                &directory,
                "unpublished state has no proven process containment",
            )?;
            return Err(error(
                "unpublished dataset state requires explicit inspection and cleanup",
            ));
        }
        validate_entry(&root, &directory, &lock)?;
        std::fs::create_dir(&active).map_err(|e| error(e.to_string()))?;
        validate_entry(&root, &directory, &lock)?;
        let handoff = match crate::dataset_worker::supervise_fixture_build(
            executable,
            plan,
            binding,
            &active,
            &template,
            &root,
            Duration::from_secs(3600),
        ) {
            Ok(h) => h,
            Err(mut e) => {
                if !contained(&e)
                    && let Err(marker) = validate_entry(&root, &directory, &lock)
                        .and_then(|_| write_quarantine(&root, &directory, &e.message))
                {
                    e.message
                        .push_str(&format!("; quarantine marker failed: {}", marker.message));
                }
                return Err(e);
            }
        };
        validate_entry(&root, &directory, &lock)?;
        if active.exists() {
            return Err(error("dataset worker did not retire its active store"));
        }
        reset::verify_physical_tree(&template, &handoff.physical, TraversalLimits::default())
            .map_err(|e| error(e.to_string()))?;
        let descriptor = crate::registered_fixture::RegisteredFixtureSourceV1 {
            format_version: 1,
            fixture_id: format!("gqt-{key}"),
            physical: RegisteredPhysicalTreeV1 {
                digest_algorithm: crate::reset::PHYSICAL_TREE_DIGEST_ALGORITHM.into(),
                tree_sha256: handoff.physical.digest_sha256.clone(),
                files: handoff.physical.files,
                bytes: handoff.physical.bytes,
            },
        };
        validate_entry(&root, &directory, &lock)?;
        let mut descriptor_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(root.join("fixture-source.json"))
            .map_err(|e| error(e.to_string()))?;
        descriptor_file
            .write_all(&serde_json::to_vec(&descriptor).map_err(|e| error(e.to_string()))?)
            .and_then(|_| descriptor_file.sync_all())
            .map_err(|e| error(e.to_string()))?;
        let m = DatasetManifestV1 {
            format_version: 1,
            key: key.clone(),
            recipe_sha256: plan.recipe_sha256.clone(),
            engine_digest: GQT_ENGINE_DIGEST.into(),
            needs_indices: plan.needs_indices,
            reset: plan.reset,
            handoff,
        };
        let bytes = serde_json::to_vec(&m).map_err(|e| error(e.to_string()))?;
        if bytes.len() > MAX_DATASET_MANIFEST_BYTES {
            return Err(error("dataset manifest exceeds 1 MiB"));
        }
        validate_entry(&root, &directory, &lock)?;
        let pending = root.join("dataset-build.pending");
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&pending)
            .map_err(|e| error(e.to_string()))?;
        file.write_all(&bytes)
            .and_then(|_| file.sync_all())
            .map_err(|e| error(e.to_string()))?;
        validate_entry(&root, &directory, &lock)?;
        std::fs::rename(&pending, &stamp).map_err(|e| error(e.to_string()))?;
        File::open(&root)
            .and_then(|f| f.sync_all())
            .map_err(|e| error(e.to_string()))?;
        m
    };
    let lease = DatasetLease {
        root,
        active,
        template,
        manifest,
        cache_hit,
        _directory: directory,
        _lock: lock,
    };
    lease.verify()?;
    validate_descriptor(&lease.root, &lease.manifest)?;
    reset::verify_physical_tree(
        &lease.template,
        &lease.manifest.handoff.physical,
        TraversalLimits::default(),
    )
    .map_err(|e| error(e.to_string()))?;
    Ok(lease)
}
fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> RunnerResult<T> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_NONBLOCK);
    }
    let f = options.open(path).map_err(|e| error(e.to_string()))?;
    let m = f.metadata().map_err(|e| error(e.to_string()))?;
    if !m.is_file() || m.len() > MAX_DATASET_MANIFEST_BYTES as u64 {
        return Err(error("dataset manifest is not a bounded regular file"));
    }
    let mut bytes = Vec::new();
    f.take(MAX_DATASET_MANIFEST_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| error(e.to_string()))?;
    serde_json::from_slice(&bytes).map_err(|e| error(e.to_string()))
}
#[cfg(unix)]
fn lock(root: &Path) -> RunnerResult<nix::fcntl::Flock<File>> {
    use nix::fcntl::{Flock, FlockArg};
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    let path = root.join("lock");
    let started = Instant::now();
    loop {
        if started.elapsed() > Duration::from_secs(3600) {
            return Err(error("cache lock path changed until timeout"));
        }
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_NONBLOCK)
            .open(&path)
            .map_err(|e| error(e.to_string()))?;
        loop {
            match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
                Ok(lock) => {
                    let a = lock.metadata().map_err(|e| error(e.to_string()))?;
                    let b = std::fs::symlink_metadata(&path).map_err(|e| error(e.to_string()))?;
                    if a.is_file()
                        && !b.file_type().is_symlink()
                        && a.dev() == b.dev()
                        && a.ino() == b.ino()
                    {
                        return Ok(lock);
                    }
                    drop(lock);
                    break;
                }
                Err((returned, e)) if e == nix::errno::Errno::EWOULDBLOCK => file = returned,
                Err((_, e)) => return Err(error(e.to_string())),
            }
            if started.elapsed() > Duration::from_secs(3600) {
                return Err(RunnerError::new(
                    "dataset_lock_timeout",
                    "dataset cache lock exceeded one hour",
                ));
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}
pub fn contained(error: &RunnerError) -> bool {
    error
        .context
        .child_process
        .as_ref()
        .is_some_and(|e| e.direct_child_reaped && e.process_group_gone && e.stdio_closed_cleanly)
}

#[cfg(unix)]
fn validate_entry(root: &Path, directory: &File, lock: &File) -> RunnerResult<()> {
    validate_path(root, directory)?;
    validate_path(&root.join("lock"), lock)
}

#[cfg(unix)]
fn validate_path(path: &Path, file: &File) -> RunnerResult<()> {
    use std::os::unix::fs::MetadataExt;
    let a = file.metadata().map_err(|e| error(e.to_string()))?;
    let b = std::fs::symlink_metadata(path).map_err(|e| error(e.to_string()))?;
    if b.file_type().is_symlink() || a.dev() != b.dev() || a.ino() != b.ino() {
        return Err(error(
            "cache directory or lock pathname changed while owned",
        ));
    }
    Ok(())
}

fn cache_key(
    plan: &DatasetBuildPlan,
    cache: &Path,
    registered: Option<&RegisteredPhysicalTreeV1>,
) -> RunnerResult<String> {
    crate::model::typed_sha256(&(
        "gqt-dataset-cache-v1",
        &plan.recipe_sha256,
        GQT_ENGINE_DIGEST,
        plan.needs_indices,
        &plan.environment.backend,
        plan.reset,
        cache,
        registered,
    ))
    .map_err(|e| error(e.to_string()))
}
/// Structural handoff checks also apply when validating a durable archive record.
pub(crate) fn validate_manifest_evidence(manifest: &DatasetManifestV1) -> Result<(), String> {
    if serde_json::to_vec(manifest)
        .map_err(|e| e.to_string())?
        .len()
        > MAX_DATASET_MANIFEST_BYTES
    {
        return Err("dataset manifest exceeds 1 MiB".into());
    }
    let h = &manifest.handoff;
    crate::dataset_identity::validate(&h.summary, h.registered_source_identity.as_deref())?;
    for digest in [
        &manifest.key,
        &manifest.recipe_sha256,
        &manifest.engine_digest,
        &h.physical.digest_sha256,
        &h.template_metadata.shape_sha256,
        &h.template_metadata.state_sha256,
    ] {
        if !valid_digest(digest) {
            return Err("invalid dataset manifest digest".into());
        }
    }
    let m = &h.template_metadata;
    if manifest.format_version != 1
        || manifest.reset == ResetMode::S3Versioning
        || h.physical.files == 0
        || h.physical.bytes == 0
        || h.physical.files != m.files
        || h.physical.bytes != m.bytes
        || m.files.checked_add(m.directories) != Some(m.entries)
    {
        return Err("invalid dataset physical/metadata handoff".into());
    }
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    fn plan() -> DatasetBuildPlan {
        let dataset =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../benchmarks/fixtures/tiny_graph.gqt");
        crate::gqt_case::dataset_file(
            &dataset,
            None,
            crate::case::Backend::LocalFs {
                filesystem: crate::case::LocalFilesystem::Xfs,
                storage_class: crate::case::LocalStorageClass::NvmeSsd,
            },
            ResetMode::PlainCopy,
        )
        .unwrap()
    }
    fn refused(plan: &DatasetBuildPlan, cache: &Path, no_build: bool) -> RunnerError {
        match acquire(
            plan,
            cache,
            Path::new("/does-not-exist-worker"),
            no_build,
            None,
        ) {
            Ok(_) => panic!("unexpected cache admission"),
            Err(e) => e,
        }
    }
    async fn published(plan: &DatasetBuildPlan, cache: &Path) -> PathBuf {
        assert_eq!(refused(plan, cache, true).code, "dataset_cache_miss");
        let root = cache.join(cache_key(plan, cache, None).unwrap());
        let active = root.join("active");
        let scratch = root.join("scratch");
        std::fs::create_dir(&scratch).unwrap();
        let (summary, registered_source_identity) =
            crate::dataset_worker::build_dataset(plan, active.to_str().unwrap(), &scratch, None)
                .await
                .unwrap();
        let frozen = reset::freeze_plain_copy_template(
            &active,
            &root.join("root"),
            TraversalLimits::default(),
        )
        .unwrap();
        let handoff = FixtureBuildHandoff {
            summary,
            registered_source_identity,
            physical: frozen.physical_digest().clone(),
            template_metadata: frozen.metadata_digest().clone(),
        };
        std::fs::remove_dir_all(active).unwrap();
        let manifest = DatasetManifestV1 {
            format_version: 1,
            key: root.file_name().unwrap().to_str().unwrap().into(),
            recipe_sha256: plan.recipe_sha256.clone(),
            engine_digest: GQT_ENGINE_DIGEST.into(),
            needs_indices: plan.needs_indices,
            reset: plan.reset,
            handoff,
        };
        validate_manifest_evidence(&manifest).unwrap();
        let descriptor = crate::registered_fixture::RegisteredFixtureSourceV1 {
            format_version: 1,
            fixture_id: format!("gqt-{}", manifest.key),
            physical: RegisteredPhysicalTreeV1 {
                digest_algorithm: reset::PHYSICAL_TREE_DIGEST_ALGORITHM.into(),
                tree_sha256: manifest.handoff.physical.digest_sha256.clone(),
                files: manifest.handoff.physical.files,
                bytes: manifest.handoff.physical.bytes,
            },
        };
        std::fs::write(
            root.join("fixture-source.json"),
            serde_json::to_vec(&descriptor).unwrap(),
        )
        .unwrap();
        std::fs::write(
            root.join("dataset-build.json"),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        root
    }
    #[test]
    fn miss_and_unknown_build_state_never_spawn_or_remove_unproved_work() {
        let directory = tempfile::tempdir().unwrap();
        let cache = directory.path().canonicalize().unwrap();
        let plan = plan();
        assert_eq!(refused(&plan, &cache, true).code, "dataset_cache_miss");
        let root = cache.join(cache_key(&plan, &cache, None).unwrap());
        assert_eq!(std::fs::read_dir(&root).unwrap().count(), 1);
        std::fs::create_dir(root.join("active")).unwrap();
        std::fs::write(root.join("active/owner"), b"possibly live child").unwrap();
        assert_eq!(refused(&plan, &cache, false).code, "dataset_cache_corrupt");
        assert_eq!(
            std::fs::read(root.join("active/owner")).unwrap(),
            b"possibly live child"
        );
        assert!(root.join("quarantined").is_file());
    }

    #[test]
    fn inspection_preserves_missing_roots_entries_locks_and_unpublished_state() {
        let directory = tempfile::tempdir().unwrap();
        let cache = directory.path().join("absent");
        let plan = plan();
        assert_eq!(
            inspect(&plan, &cache, None, false).cache,
            CacheState::Missing
        );
        assert_eq!(
            list(&cache, 10, None, false).unwrap().root_state,
            CacheState::Missing
        );
        assert!(!cache.exists());
        std::fs::create_dir(&cache).unwrap();
        let cache = cache.canonicalize().unwrap();
        let status = inspect(&plan, &cache, None, true);
        assert_eq!(status.source, Some(SourceAvailability::Available));
        assert_eq!(status.cache, CacheState::Missing);
        assert_eq!(std::fs::read_dir(&cache).unwrap().count(), 0);
        let root = status.path;
        std::fs::create_dir(&root).unwrap();
        assert_eq!(
            inspect(&plan, &cache, None, false).cache,
            CacheState::Unknown
        );
        assert!(!root.join("lock").exists());
        std::fs::write(root.join("lock"), b"").unwrap();
        std::fs::create_dir(root.join("active")).unwrap();
        std::fs::write(root.join("active/owner"), b"possibly live child").unwrap();
        assert_eq!(
            inspect(&plan, &cache, None, true).cache,
            CacheState::Incomplete
        );
        assert_eq!(
            std::fs::read(root.join("active/owner")).unwrap(),
            b"possibly live child"
        );
        assert!(!root.join("quarantined").exists());
        std::fs::write(root.join("quarantined"), b"keep this reason").unwrap();
        assert_eq!(
            inspect(&plan, &cache, None, false).cache,
            CacheState::Invalid
        );
        assert_eq!(
            std::fs::read(root.join("quarantined")).unwrap(),
            b"keep this reason"
        );
    }

    #[test]
    fn inspection_reports_busy_without_waiting_for_an_exclusive_owner() {
        let directory = tempfile::tempdir().unwrap();
        let cache = directory.path().canonicalize().unwrap();
        let plan = plan();
        let root = cache.join(cache_key(&plan, &cache, None).unwrap());
        std::fs::create_dir(&root).unwrap();
        let owner = lock(&root).unwrap();
        let (sender, receiver) = std::sync::mpsc::channel();
        let inspector = std::thread::spawn(move || {
            sender.send(inspect(&plan, &cache, None, false)).unwrap();
        });
        let result = receiver.recv_timeout(Duration::from_secs(5));
        drop(owner);
        inspector.join().unwrap();
        assert_eq!(result.unwrap().cache, CacheState::Busy);
    }

    #[test]
    fn inspection_refuses_symlink_entries_and_locks_without_touching_targets() {
        let directory = tempfile::tempdir().unwrap();
        let cache = directory.path().join("cache");
        std::fs::create_dir(&cache).unwrap();
        let cache = cache.canonicalize().unwrap();
        let outside = directory.path().join("outside");
        std::fs::create_dir(&outside).unwrap();
        std::fs::write(outside.join("sentinel"), b"untouched").unwrap();
        let plan = plan();
        let root = cache.join(cache_key(&plan, &cache, None).unwrap());
        let alias = directory.path().join("alias");
        std::os::unix::fs::symlink(&cache, &alias).unwrap();
        for path in [&alias, &alias.join(".")] {
            assert_eq!(
                inspect(&plan, path, None, false),
                inspect(&plan, &cache, None, false)
            );
            assert_eq!(
                list(path, 10, None, false).unwrap(),
                list(&cache, 10, None, false).unwrap()
            );
        }
        std::os::unix::fs::symlink(&outside, &root).unwrap();
        assert_eq!(
            inspect(&plan, &cache, None, false).cache,
            CacheState::Invalid
        );
        std::fs::remove_file(&root).unwrap();
        std::fs::create_dir(&root).unwrap();
        std::os::unix::fs::symlink(outside.join("absent"), root.join("lock")).unwrap();
        assert_eq!(
            inspect(&plan, &cache, None, false).cache,
            CacheState::Unknown
        );
        assert_eq!(std::fs::read_dir(&outside).unwrap().count(), 1);
        assert_eq!(
            std::fs::read(outside.join("sentinel")).unwrap(),
            b"untouched"
        );
        for (name, target) in [
            ("dangling", outside.join("absent")),
            ("file", outside.join("sentinel")),
            ("loop", directory.path().join("loop")),
        ] {
            let alias = directory.path().join(name);
            std::os::unix::fs::symlink(target, &alias).unwrap();
            assert!(list(&alias, 10, None, false).is_err());
            assert_eq!(
                inspect(&plan, &alias, None, false).cache,
                CacheState::Unknown
            );
        }
        assert!(!outside.join("absent").exists());
    }

    #[tokio::test]
    async fn inspection_separates_presence_verification_and_historical_identity() {
        let directory = tempfile::tempdir().unwrap();
        let cache = directory.path().canonicalize().unwrap();
        let plan = plan();
        let root = published(&plan, &cache).await;
        let before = reset::digest_metadata_tree(&root, TraversalLimits::default()).unwrap();
        assert_eq!(
            inspect(&plan, &cache, None, false).cache,
            CacheState::Present
        );
        assert_eq!(inspect(&plan, &cache, None, true).cache, CacheState::Cached);
        let aliases = tempfile::tempdir().unwrap();
        let alias = aliases.path().join("cache");
        std::os::unix::fs::symlink(&cache, &alias).unwrap();
        for path in [&alias, &alias.join(".")] {
            assert_eq!(
                inspect(&plan, path, None, true),
                inspect(&plan, &cache, None, true)
            );
            assert_eq!(
                list(path, 10, None, true).unwrap(),
                list(&cache, 10, None, true).unwrap()
            );
        }
        assert_eq!(
            before,
            reset::digest_metadata_tree(&root, TraversalLimits::default()).unwrap()
        );
        let stamp = root.join("dataset-build.json");
        let mut historical: DatasetManifestV1 = read_json(&stamp).unwrap();
        historical.engine_digest = "0".repeat(64);
        std::fs::write(&stamp, serde_json::to_vec(&historical).unwrap()).unwrap();
        assert_eq!(
            inspect(&plan, &cache, None, false).cache,
            CacheState::Invalid
        );
        assert_eq!(
            list(&cache, 10, None, true).unwrap().entries[0].cache,
            CacheState::Cached
        );
        std::fs::write(root.join("root/unexpected"), b"changed").unwrap();
        assert_eq!(
            list(&cache, 10, None, true).unwrap().entries[0].cache,
            CacheState::Invalid
        );
        std::fs::remove_file(root.join("fixture-source.json")).unwrap();
        assert_eq!(
            list(&cache, 10, None, false).unwrap().entries[0].cache,
            CacheState::Invalid
        );
        assert!(!root.join("quarantined").exists());
    }

    #[test]
    fn inspection_listing_is_bounded_sorted_and_preserves_unknown_entries() {
        let directory = tempfile::tempdir().unwrap();
        for name in ["f".repeat(64), "1".repeat(64), "unrecognized".into()] {
            std::fs::create_dir(directory.path().join(name)).unwrap();
        }
        let first = list(directory.path(), 1, None, false).unwrap();
        assert_eq!(
            first.entries[0].key.as_deref(),
            Some("1".repeat(64).as_str())
        );
        assert_eq!(first.entries[0].cache, CacheState::Unknown);
        let second = list(directory.path(), 1, first.next_cursor.as_deref(), false).unwrap();
        assert_eq!(
            second.entries[0].key.as_deref(),
            Some("f".repeat(64).as_str())
        );
        let third = list(directory.path(), 1, second.next_cursor.as_deref(), false).unwrap();
        assert_eq!(third.entries[0].key, None);
        assert_eq!(third.entries[0].cache, CacheState::Unknown);
        assert_eq!(third.next_cursor, None);
        assert!(list(directory.path(), 0, None, false).is_err());
        assert!(list(directory.path(), MAX_INSPECTION_PAGE + 1, None, false).is_err());
        for entry in std::fs::read_dir(directory.path()).unwrap() {
            assert_eq!(std::fs::read_dir(entry.unwrap().path()).unwrap().count(), 0);
        }
    }

    #[test]
    fn invalid_frozen_source_never_becomes_a_cache_miss() {
        let directory = tempfile::tempdir().unwrap();
        let cache = directory.path().join("absent");
        let mut plan = plan();
        plan.needs_indices = !plan.needs_indices;
        let result = inspect(&plan, &cache, None, false);
        assert_eq!(result.source, Some(SourceAvailability::Invalid));
        assert_eq!(result.cache, CacheState::Unknown);
        assert_eq!(result.key, None);
        assert!(!cache.exists());
    }

    #[test]
    fn registered_source_availability_is_independent_of_cache_availability() {
        let directory = tempfile::tempdir().unwrap();
        let cache = directory.path().join("absent");
        let config = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../benchmarks/benchmarks.yaml");
        let plan = crate::catalog::Catalog::load(&config)
            .unwrap()
            .plan("finbench-disjoint-merge")
            .unwrap()
            .dataset_build_plan();
        let DatasetRecipe::Registered { reference, .. } = &plan.dataset else {
            panic!("registered fixture required");
        };
        let unbound = inspect(&plan, &cache, None, false);
        assert_eq!(unbound.source, Some(SourceAvailability::Unbound));
        assert_eq!(unbound.cache, CacheState::Unknown);
        let binding = format!("{}={}", reference.definition.fixture_id, cache.display());
        let missing = inspect(&plan, &cache, Some(&binding), false);
        assert_eq!(missing.source, Some(SourceAvailability::Missing));
        assert_eq!(missing.cache, CacheState::Unknown);
        let invalid = inspect(&plan, &cache, Some("different-id=missing"), false);
        assert_eq!(invalid.source, Some(SourceAvailability::Invalid));
        assert_eq!(invalid.cache, CacheState::Unknown);
        assert!(!cache.exists());
        let bundle = directory.path().join("bundle");
        std::fs::create_dir(&bundle).unwrap();
        let source = bundle.join("root");
        std::fs::create_dir(&source).unwrap();
        std::fs::write(source.join("data"), b"source").unwrap();
        let descriptor = crate::registered_fixture::fingerprint_registered_fixture(
            reference.definition.fixture_id.clone(),
            &source,
        )
        .into_result()
        .unwrap();
        std::fs::write(
            bundle.join("fixture-source.json"),
            serde_json::to_vec(&descriptor).unwrap(),
        )
        .unwrap();
        let binding = format!("{}={}", reference.definition.fixture_id, bundle.display());
        assert_eq!(
            inspect(&plan, &cache, Some(&binding), false).source,
            Some(SourceAvailability::Available)
        );
        std::fs::rename(&source, bundle.join("saved-root")).unwrap();
        assert_eq!(
            inspect(&plan, &cache, Some(&binding), false).source,
            Some(SourceAvailability::Missing)
        );
        std::os::unix::fs::symlink(bundle.join("saved-root"), &source).unwrap();
        assert_eq!(
            inspect(&plan, &cache, Some(&binding), false).source,
            Some(SourceAvailability::Invalid)
        );
        std::fs::remove_file(&source).unwrap();
        std::fs::rename(bundle.join("saved-root"), &source).unwrap();
        let mut wrong = descriptor;
        wrong.fixture_id = "wrong-id".into();
        std::fs::write(
            bundle.join("fixture-source.json"),
            serde_json::to_vec(&wrong).unwrap(),
        )
        .unwrap();
        assert_eq!(
            inspect(&plan, &cache, Some(&binding), false).source,
            Some(SourceAvailability::Invalid)
        );
        assert!(!cache.exists());
    }
    #[test]
    fn cache_entry_symlink_is_refused_without_touching_its_target() {
        let directory = tempfile::tempdir().unwrap();
        let cache = directory.path().join("cache");
        std::fs::create_dir(&cache).unwrap();
        let cache = cache.canonicalize().unwrap();
        let outside = directory.path().join("outside");
        std::fs::create_dir(&outside).unwrap();
        std::fs::write(outside.join("sentinel"), b"untouched").unwrap();
        let plan = plan();
        std::os::unix::fs::symlink(
            &outside,
            cache.join(cache_key(&plan, &cache, None).unwrap()),
        )
        .unwrap();
        assert_eq!(refused(&plan, &cache, false).code, "dataset_cache_corrupt");
        assert_eq!(std::fs::read_dir(outside).unwrap().count(), 1);
    }
    #[test]
    fn quarantine_markers_refuse_reuse_without_following_symlinks() {
        for no_build in [false, true] {
            for live_target in [false, true] {
                let directory = tempfile::tempdir().unwrap();
                let cache = directory.path().join("cache");
                std::fs::create_dir(&cache).unwrap();
                let cache = cache.canonicalize().unwrap();
                let plan = plan();
                assert_eq!(refused(&plan, &cache, true).code, "dataset_cache_miss");
                let root = cache.join(cache_key(&plan, &cache, None).unwrap());
                let outside = directory.path().join("outside");
                if live_target {
                    std::fs::write(&outside, b"untouched").unwrap();
                }
                std::os::unix::fs::symlink(&outside, root.join("quarantined")).unwrap();
                assert_eq!(
                    refused(&plan, &cache, no_build).code,
                    "dataset_cache_corrupt"
                );
                if live_target {
                    assert_eq!(std::fs::read(&outside).unwrap(), b"untouched");
                } else {
                    assert!(!outside.exists());
                }
            }
        }
    }
    #[tokio::test]
    async fn published_hit_requires_descriptor_bytes_and_the_original_lock_inode() {
        let directory = tempfile::tempdir().unwrap();
        let cache = directory.path().canonicalize().unwrap();
        let plan = plan();
        let root = published(&plan, &cache).await;
        let lease = acquire(&plan, &cache, Path::new("/unused-worker"), true, None).unwrap();
        assert!(lease.cache_hit);
        lease.restore().unwrap();
        lease.remove_active().unwrap();
        let outside = directory.path().join("outside-marker");
        std::os::unix::fs::symlink(&outside, root.join("quarantined")).unwrap();
        assert!(lease.verify().is_err());
        lease.quarantine("uncertain child").unwrap();
        assert!(!outside.exists());
        std::fs::remove_file(root.join("quarantined")).unwrap();
        lease.quarantine("first reason").unwrap();
        lease.quarantine("second reason").unwrap();
        assert_eq!(
            std::fs::read(root.join("quarantined")).unwrap(),
            b"first reason"
        );
        std::fs::remove_file(root.join("quarantined")).unwrap();
        std::fs::rename(root.join("lock"), root.join("old-lock")).unwrap();
        std::fs::write(root.join("lock"), b"").unwrap();
        assert!(lease.verify().is_err());
        drop(lease);
        std::fs::remove_file(root.join("fixture-source.json")).unwrap();
        assert_eq!(refused(&plan, &cache, true).code, "dataset_cache_corrupt");
    }
    #[tokio::test]
    async fn published_tree_or_logical_receipt_tampering_is_corruption_not_a_miss() {
        let directory = tempfile::tempdir().unwrap();
        let cache = directory.path().canonicalize().unwrap();
        let plan = plan();
        let root = published(&plan, &cache).await;
        let stamp = root.join("dataset-build.json");
        let original = std::fs::read(&stamp).unwrap();
        let mut oversized: DatasetManifestV1 = serde_json::from_slice(&original).unwrap();
        let logical = &mut oversized.handoff.summary;
        let main = logical.branches[0].clone();
        for index in 0..1023 {
            let mut branch = main.clone();
            branch.name = format!("{index:04}{}", "x".repeat(1020));
            logical.branches.push(branch);
        }
        logical.branches.sort_by(|a, b| a.name.cmp(&b.name));
        logical.logical_content_sha256 = crate::model::typed_sha256(&(
            crate::dataset_identity::DATASET_LOGICAL_ALGORITHM,
            &logical.branches,
        ))
        .unwrap();
        crate::dataset_identity::validate(logical, None).unwrap();
        assert_eq!(
            validate_manifest_evidence(&oversized).unwrap_err(),
            "dataset manifest exceeds 1 MiB"
        );
        let mut manifest: DatasetManifestV1 = serde_json::from_slice(&original).unwrap();
        manifest.handoff.summary.branches.clear();
        std::fs::write(&stamp, serde_json::to_vec(&manifest).unwrap()).unwrap();
        assert_eq!(refused(&plan, &cache, false).code, "dataset_cache_corrupt");
        std::fs::write(&stamp, original).unwrap();
        std::fs::write(root.join("root/unexpected"), b"changed").unwrap();
        assert_eq!(refused(&plan, &cache, false).code, "dataset_cache_corrupt");
    }
}
