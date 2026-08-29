//! Local fixture freezing and reset primitives.
//!
//! The caller must quiesce the frozen fixture before calling this module. The
//! physical digest reads every byte once while the fixture is frozen. Measured
//! APFS resets use forced `clonefileat(2)` calls and metadata-only witnesses, so
//! no reset or pre-timer proof reads file contents or silently falls back to a
//! byte copy.
//!
//! Lance shallow branches can retain absolute base paths. Callers therefore
//! build at one stable absolute `active_root`, clone that tree to a template
//! which is never opened as a database, remove `active_root`, and restore every
//! repetition to that exact same path. Recreating the lexical path, rather than
//! opening the template in place, keeps every embedded absolute base path
//! inside the disposable active tree.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Stable algorithm identifier for [`digest_physical_tree`].
pub(crate) const PHYSICAL_TREE_DIGEST_ALGORITHM: &str = "omnigraph-bench-physical-tree-v1";
const DIGEST_DOMAIN: &[u8] = b"omnigraph-bench-physical-tree-v1\0";
const METADATA_SHAPE_DOMAIN: &[u8] = b"omnigraph-bench-metadata-shape-v1\0";
const METADATA_STATE_DOMAIN: &[u8] = b"omnigraph-bench-metadata-state-v1\0";
const COPY_BUFFER_BYTES: usize = 1024 * 1024;

/// Explicit resource bounds for one physical fixture traversal.
///
/// `max_entries` counts every file and directory below the root. The root
/// itself is not counted. `max_depth` uses the same convention: direct
/// children are at depth one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TraversalLimits {
    pub max_entries: u64,
    pub max_bytes: u64,
    pub max_depth: u32,
}

impl Default for TraversalLimits {
    fn default() -> Self {
        Self {
            max_entries: 1_000_000,
            max_bytes: 8 * 1024 * 1024 * 1024 * 1024,
            max_depth: 128,
        }
    }
}

/// Stable physical identity and size of one fixture tree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PhysicalDigest {
    pub files: u64,
    pub bytes: u64,
    pub digest_sha256: String,
}

/// Content-free identity of one observed tree state.
///
/// `shape_sha256` contains sorted relative paths, entry kinds, and regular-file
/// lengths, and can therefore be compared across a clone. `state_sha256` also
/// contains stable Unix stat fields (including device, inode, mode, link count,
/// mtime, and ctime) and is only compared against a later observation of the
/// same tree. Access time is deliberately excluded because read-only cache preparation and
/// metadata traversal may update it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetadataDigest {
    pub entries: u64,
    pub files: u64,
    pub directories: u64,
    pub bytes: u64,
    pub shape_sha256: String,
    pub state_sha256: String,
}

/// A byte-identified APFS template that must never be opened as a database.
///
/// The template remembers the exact absolute path at which the source fixture
/// was built. [`Self::restore_active`] can restore only that path, which keeps
/// absolute Lance base paths valid without rewriting manifests.
#[derive(Debug, Clone)]
pub struct ClonefileTemplate {
    template_root: PathBuf,
    active_root: PathBuf,
    physical: PhysicalDigest,
    metadata: MetadataDigest,
    limits: TraversalLimits,
}

/// One clonefile-restored active tree and its pre-open metadata witness.
#[derive(Debug, Clone)]
pub struct PreparedClone {
    root: PathBuf,
    metadata: MetadataDigest,
    limits: TraversalLimits,
}

/// Compute the physical identity of a quiescent fixture root.
///
/// The SHA-256 input is domain-separated and contains every descendant in
/// sorted, slash-separated UTF-8 relative-path order. Each entry contributes
/// its type, path length, path bytes, and length; regular files additionally
/// contribute their complete contents. Including directories makes empty
/// directories part of the identity. Symlinks, non-file/non-directory entries,
/// non-UTF-8 names, and trees outside `limits` are refused.
pub fn digest_physical_tree(root: &Path, limits: TraversalLimits) -> io::Result<PhysicalDigest> {
    capture_stable_tree(root, limits).map(|(physical, _)| physical)
}

/// Re-digest `root` and fail unless it exactly matches `expected`.
pub fn verify_physical_tree(
    root: &Path,
    expected: &PhysicalDigest,
    limits: TraversalLimits,
) -> io::Result<PhysicalDigest> {
    let observed = digest_physical_tree(root, limits)?;
    require_digest("fixture tree", expected, &observed)?;
    Ok(observed)
}

/// Compute a content-free metadata witness for one tree.
///
/// This traverses and stats the complete tree but never opens a regular file.
/// The witness is suitable for proving that a quiescent tree did not change
/// between two points in the benchmark protocol. It is not a replacement for
/// [`PhysicalDigest`]: clone identity additionally relies on a successful
/// forced `clonefileat(2)` call for every file.
pub fn digest_metadata_tree(root: &Path, limits: TraversalLimits) -> io::Result<MetadataDigest> {
    let entries = inventory(root, limits)?;
    digest_metadata_inventory(root, &entries, limits)
}

/// Re-stat `root` and fail unless its complete metadata witness is unchanged.
pub fn verify_metadata_tree(
    root: &Path,
    expected: &MetadataDigest,
    limits: TraversalLimits,
) -> io::Result<MetadataDigest> {
    let observed = digest_metadata_tree(root, limits)?;
    require_metadata(
        "fixture tree",
        expected,
        &observed,
        MetadataComparison::State,
    )?;
    Ok(observed)
}

/// Re-stat `root` and require the same paths, entry kinds, and file lengths.
///
/// Unlike [`verify_metadata_tree`], this intentionally ignores inode and
/// timestamp state. The measured runner pairs this shape proof with a storage
/// write firewall: the read-write bind's balanced create-if-absent capability
/// probe changes directory timestamps even though it leaves no fixture object.
pub fn verify_metadata_shape(
    root: &Path,
    expected: &MetadataDigest,
    limits: TraversalLimits,
) -> io::Result<MetadataDigest> {
    let observed = digest_metadata_tree(root, limits)?;
    require_metadata(
        "fixture tree shape",
        expected,
        &observed,
        MetadataComparison::Shape,
    )?;
    Ok(observed)
}

/// Freeze a quiescent active fixture into a never-opened APFS clone template.
///
/// Both paths must be stable absolute paths without parent traversal.
/// `active_root` must be the exact path used to build the database; the
/// returned template restores only that same path. The caller must remove
/// `active_root` after this function succeeds and before the first restore.
///
/// This performs the run's one full byte digest against `active_root`, proves
/// that its metadata stayed stable across that read, then clones every regular
/// file with forced `clonefileat(2)`. There is no byte-copy fallback. The
/// returned template's physical identity follows from the verified source
/// digest, the stable source witness, and the kernel clone contract.
pub fn freeze_clonefile_template(
    active_root: &Path,
    template_root: &Path,
    limits: TraversalLimits,
) -> io::Result<ClonefileTemplate> {
    require_clonefile_platform()?;
    let active_root = stable_absolute_path("active fixture root", active_root)?;
    let template_root = stable_absolute_path("clonefile template root", template_root)?;
    refuse_destination_below_source(&active_root, &template_root)?;
    let (physical, active_metadata) = capture_stable_tree(&active_root, limits)?;
    let template_metadata =
        clonefile_tree_from_witness(&active_root, &template_root, &active_metadata, limits)?;

    Ok(ClonefileTemplate {
        template_root,
        active_root,
        physical,
        metadata: template_metadata,
        limits,
    })
}

/// Accept a clonefile template produced by the contained fixture worker.
///
/// The worker owns the only full byte read and clone operation. The parent
/// reconstructs the typed handle only after the child is reaped, checks that
/// the active path is absent, and independently re-stats the complete template
/// against the handed-off metadata witness without reopening file contents.
pub(crate) fn accept_clonefile_template_handoff(
    active_root: &Path,
    template_root: &Path,
    physical: PhysicalDigest,
    metadata: MetadataDigest,
    limits: TraversalLimits,
) -> io::Result<ClonefileTemplate> {
    require_clonefile_platform()?;
    let active_root = stable_absolute_path("active fixture root", active_root)?;
    let template_root = stable_absolute_path("clonefile template root", template_root)?;
    refuse_destination_below_retired_source(&active_root, &template_root)?;
    match fs::symlink_metadata(&active_root) {
        Ok(_) => {
            return Err(invalid_data(format!(
                "contained fixture worker left the active path behind: {}",
                active_root.display()
            )));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(contextual(
                error,
                format!("checking active fixture path {}", active_root.display()),
            ));
        }
    }
    if physical.files != metadata.files || physical.bytes != metadata.bytes {
        return Err(invalid_data(format!(
            "fixture handoff disagrees on template totals: physical files={} bytes={}, metadata files={} bytes={}",
            physical.files, physical.bytes, metadata.files, metadata.bytes
        )));
    }
    if !is_lowercase_sha256(&physical.digest_sha256) {
        return Err(invalid_data(
            "fixture handoff physical digest must be exactly 64 lowercase hexadecimal characters",
        ));
    }
    verify_metadata_tree(&template_root, &metadata, limits)?;
    Ok(ClonefileTemplate {
        template_root,
        active_root,
        physical,
        metadata,
        limits,
    })
}

fn is_lowercase_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(target_os = "macos")]
fn require_clonefile_platform() -> io::Result<()> {
    Ok(())
}

#[cfg(not(target_os = "macos"))]
fn require_clonefile_platform() -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "forced clonefile reset is available only on macOS/APFS; no byte-copy fallback is permitted",
    ))
}

impl ClonefileTemplate {
    /// Exact path of the never-opened template tree.
    pub fn template_root(&self) -> &Path {
        &self.template_root
    }

    /// Exact stable path at which every repetition must run.
    pub fn active_root(&self) -> &Path {
        &self.active_root
    }

    /// Full byte identity captured once before the template was made.
    pub fn physical_digest(&self) -> &PhysicalDigest {
        &self.physical
    }

    /// Metadata witness for proving that the template remains immutable.
    pub fn metadata_digest(&self) -> &MetadataDigest {
        &self.metadata
    }

    /// Prove without reading file contents that the template is unchanged.
    pub fn verify_unchanged(&self) -> io::Result<MetadataDigest> {
        verify_metadata_tree(&self.template_root, &self.metadata, self.limits)
    }

    /// Restore one repetition to the exact path used to build the fixture.
    ///
    /// The active path may be absent or an existing empty directory. An error
    /// can leave a partial active tree, which must not be opened. The caller
    /// owns deletion of the previous repetition after all engine handles have
    /// quiesced.
    pub fn restore_active(&self) -> io::Result<PreparedClone> {
        let metadata = clonefile_tree_from_witness(
            &self.template_root,
            &self.active_root,
            &self.metadata,
            self.limits,
        )?;
        Ok(PreparedClone {
            root: self.active_root.clone(),
            metadata,
            limits: self.limits,
        })
    }
}

impl PreparedClone {
    /// Exact active path to pass to the engine.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Pre-open metadata witness for this repetition.
    pub fn metadata_digest(&self) -> &MetadataDigest {
        &self.metadata
    }

    /// Prove without reading contents that open and cache preparation did not mutate the
    /// measured input before the timer starts.
    pub fn verify_unchanged(&self) -> io::Result<MetadataDigest> {
        verify_metadata_tree(&self.root, &self.metadata, self.limits)
    }
}

/// Copy a frozen fixture root into an empty destination and verify the copy.
///
/// The destination may be absent (its parent must exist) or an existing empty
/// directory. It must not be the source or lie below it. The source is checked
/// against `frozen` before any destination bytes are written, and the completed
/// destination is checked again before success is returned. An error can leave
/// a partial destination; callers should use a disposable per-repetition
/// directory and must never open it after an error.
pub fn copy_verified(
    frozen_root: &Path,
    destination: &Path,
    frozen: &PhysicalDigest,
    limits: TraversalLimits,
) -> io::Result<PhysicalDigest> {
    let entries = inventory(frozen_root, limits)?;
    let observed_frozen = digest_inventory(&entries, limits)?;
    require_digest("frozen fixture", frozen, &observed_frozen)?;

    refuse_destination_below_source(frozen_root, destination)?;
    prepare_empty_destination(destination)?;
    copy_inventory(&entries, destination, limits)?;

    let copied = digest_physical_tree(destination, limits)?;
    require_digest("copied fixture", frozen, &copied)?;
    Ok(copied)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EntryKind {
    Directory,
    File,
}

impl EntryKind {
    fn digest_tag(self) -> u8 {
        match self {
            Self::Directory => b'd',
            Self::File => b'f',
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EntryType {
    Directory,
    File,
    Symlink,
    Special,
}

#[cfg(unix)]
fn entry_type(metadata: &fs::Metadata) -> EntryType {
    use std::os::unix::fs::MetadataExt;

    classify_unix_mode(metadata.mode())
}

#[cfg(unix)]
fn classify_unix_mode(mode: u32) -> EntryType {
    let (file_type, directory, file, symlink) = unix_file_type_bits(mode);
    if file_type == directory {
        EntryType::Directory
    } else if file_type == file {
        EntryType::File
    } else if file_type == symlink {
        EntryType::Symlink
    } else {
        EntryType::Special
    }
}

// Darwin exposes these `mode_t` constants as `u16`; Linux exposes them as
// `u32`. Keep the platform-specific widening at this seam so the shared
// classifier is warning-free on both CI and the supported macOS runner.
#[cfg(target_os = "macos")]
fn unix_file_type_bits(mode: u32) -> (u32, u32, u32, u32) {
    (
        mode & u32::from(nix::libc::S_IFMT),
        u32::from(nix::libc::S_IFDIR),
        u32::from(nix::libc::S_IFREG),
        u32::from(nix::libc::S_IFLNK),
    )
}

#[cfg(all(unix, not(target_os = "macos")))]
fn unix_file_type_bits(mode: u32) -> (u32, u32, u32, u32) {
    (
        mode & nix::libc::S_IFMT,
        nix::libc::S_IFDIR,
        nix::libc::S_IFREG,
        nix::libc::S_IFLNK,
    )
}

fn supported_inventory_kind(entry_type: EntryType, source: &Path) -> io::Result<EntryKind> {
    match entry_type {
        EntryType::Directory => Ok(EntryKind::Directory),
        EntryType::File => Ok(EntryKind::File),
        EntryType::Symlink | EntryType::Special => Err(invalid_data(format!(
            "unsupported fixture entry (symlinks and special files are refused): {}",
            source.display()
        ))),
    }
}

#[cfg(not(unix))]
fn entry_type(metadata: &fs::Metadata) -> EntryType {
    let file_type = metadata.file_type();
    if file_type.is_dir() {
        EntryType::Directory
    } else if file_type.is_file() {
        EntryType::File
    } else if file_type.is_symlink() {
        EntryType::Symlink
    } else {
        EntryType::Special
    }
}

#[derive(Debug)]
struct TreeEntry {
    source: PathBuf,
    relative: PathBuf,
    portable_path: String,
    kind: EntryKind,
    len: u64,
}

#[derive(Debug, Clone, Copy)]
enum MetadataComparison {
    Shape,
    State,
}

fn capture_stable_tree(
    root: &Path,
    limits: TraversalLimits,
) -> io::Result<(PhysicalDigest, MetadataDigest)> {
    let entries = inventory(root, limits)?;
    let before = digest_metadata_inventory(root, &entries, limits)?;
    let physical = digest_inventory(&entries, limits)?;
    let after = digest_metadata_tree(root, limits)?;
    require_metadata(
        "fixture changed while its physical digest was captured",
        &before,
        &after,
        MetadataComparison::State,
    )?;
    Ok((physical, after))
}

fn clonefile_tree_from_witness(
    source: &Path,
    destination: &Path,
    expected_source: &MetadataDigest,
    limits: TraversalLimits,
) -> io::Result<MetadataDigest> {
    let entries = inventory(source, limits)?;
    let source_before = digest_metadata_inventory(source, &entries, limits)?;
    require_metadata(
        "clonefile source",
        expected_source,
        &source_before,
        MetadataComparison::State,
    )?;

    refuse_destination_below_source(source, destination)?;
    prepare_empty_destination(destination)?;
    clone_inventory_forced(&entries, source, destination)?;

    let source_after = digest_metadata_tree(source, limits)?;
    require_metadata(
        "clonefile source changed while it was cloned",
        &source_before,
        &source_after,
        MetadataComparison::State,
    )?;
    let destination_metadata = digest_metadata_tree(destination, limits)?;
    require_metadata(
        "clonefile destination shape",
        &source_before,
        &destination_metadata,
        MetadataComparison::Shape,
    )?;
    Ok(destination_metadata)
}

fn stable_absolute_path(label: &str, path: &Path) -> io::Result<PathBuf> {
    if !path.is_absolute() {
        return Err(invalid_input(format!(
            "{label} must be an absolute path: {}",
            path.display()
        )));
    }
    if path
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(invalid_input(format!(
            "{label} must not contain `..`: {}",
            path.display()
        )));
    }
    if path.file_name().is_none() {
        return Err(invalid_input(format!(
            "{label} must name a directory below a filesystem root: {}",
            path.display()
        )));
    }
    Ok(path.to_path_buf())
}

fn inventory(root: &Path, limits: TraversalLimits) -> io::Result<Vec<TreeEntry>> {
    let root_metadata = fs::symlink_metadata(root).map_err(|error| {
        contextual(error, format!("inspecting fixture root {}", root.display()))
    })?;
    if !root_metadata.file_type().is_dir() {
        return Err(invalid_input(format!(
            "fixture root must be a directory and not a symlink: {}",
            root.display()
        )));
    }

    let mut pending = vec![(root.to_path_buf(), PathBuf::new(), 0_u32)];
    let mut entries = Vec::new();
    let mut total_bytes = 0_u64;

    while let Some((directory, relative_directory, depth)) = pending.pop() {
        let children = fs::read_dir(&directory).map_err(|error| {
            contextual(
                error,
                format!("reading fixture directory {}", directory.display()),
            )
        })?;
        for child in children {
            let child = child.map_err(|error| {
                contextual(
                    error,
                    format!("reading an entry below {}", directory.display()),
                )
            })?;
            let name = child.file_name();
            if name.to_str().is_none() {
                return Err(invalid_data(format!(
                    "fixture path is not valid UTF-8: {:?}",
                    child.path()
                )));
            }

            let child_depth = depth
                .checked_add(1)
                .ok_or_else(|| invalid_data("fixture depth counter overflowed"))?;
            if child_depth > limits.max_depth {
                return Err(invalid_data(format!(
                    "fixture tree exceeds max_depth {} at {}",
                    limits.max_depth,
                    child.path().display()
                )));
            }

            let relative = relative_directory.join(&name);
            let portable_path = portable_relative_path(&relative)?;
            let source = child.path();
            let metadata = fs::symlink_metadata(&source).map_err(|error| {
                contextual(
                    error,
                    format!("inspecting fixture entry {}", source.display()),
                )
            })?;
            let kind = supported_inventory_kind(entry_type(&metadata), &source)?;
            let len = match kind {
                EntryKind::Directory => {
                    pending.push((source.clone(), relative.clone(), child_depth));
                    0
                }
                EntryKind::File => {
                    total_bytes = total_bytes.checked_add(metadata.len()).ok_or_else(|| {
                        invalid_data("fixture byte counter overflowed while inventorying")
                    })?;
                    if total_bytes > limits.max_bytes {
                        return Err(invalid_data(format!(
                            "fixture tree exceeds max_bytes {} at {}",
                            limits.max_bytes,
                            source.display()
                        )));
                    }
                    metadata.len()
                }
            };

            entries.push(TreeEntry {
                source,
                relative,
                portable_path,
                kind,
                len,
            });
            if entries.len() as u64 > limits.max_entries {
                return Err(invalid_data(format!(
                    "fixture tree exceeds max_entries {}",
                    limits.max_entries
                )));
            }
        }
    }

    entries.sort_by(|left, right| left.portable_path.cmp(&right.portable_path));
    if entries
        .windows(2)
        .any(|pair| pair[0].portable_path == pair[1].portable_path)
    {
        return Err(invalid_data(
            "fixture tree has duplicate portable relative paths",
        ));
    }
    Ok(entries)
}

fn portable_relative_path(relative: &Path) -> io::Result<String> {
    let mut parts = Vec::new();
    for component in relative.components() {
        match component {
            Component::Normal(part) => {
                let part = part.to_str().ok_or_else(|| {
                    invalid_data(format!("fixture path is not valid UTF-8: {relative:?}"))
                })?;
                parts.push(part);
            }
            _ => {
                return Err(invalid_data(format!(
                    "fixture path is not a strict relative path: {}",
                    relative.display()
                )));
            }
        }
    }
    if parts.is_empty() {
        return Err(invalid_data("fixture entry has an empty relative path"));
    }
    Ok(parts.join("/"))
}

fn digest_metadata_inventory(
    root: &Path,
    entries: &[TreeEntry],
    limits: TraversalLimits,
) -> io::Result<MetadataDigest> {
    let root_metadata = fs::symlink_metadata(root).map_err(|error| {
        contextual(error, format!("rechecking fixture root {}", root.display()))
    })?;
    if !root_metadata.file_type().is_dir() {
        return Err(invalid_data(format!(
            "fixture root changed to a non-directory or symlink: {}",
            root.display()
        )));
    }

    let mut shape = Sha256::new();
    shape.update(METADATA_SHAPE_DOMAIN);
    let mut state = Sha256::new();
    state.update(METADATA_STATE_DOMAIN);
    state.update(b"root\0");
    update_stat_digest(&mut state, &root_metadata);

    let mut files = 0_u64;
    let mut directories = 0_u64;
    let mut bytes = 0_u64;
    for entry in entries {
        let metadata = fs::symlink_metadata(&entry.source).map_err(|error| {
            contextual(
                error,
                format!("rechecking fixture entry {}", entry.source.display()),
            )
        })?;
        let observed_kind = match entry_type(&metadata) {
            EntryType::Directory => EntryKind::Directory,
            EntryType::File => EntryKind::File,
            EntryType::Symlink | EntryType::Special => {
                return Err(invalid_data(format!(
                    "fixture entry changed to a symlink or special file: {}",
                    entry.source.display()
                )));
            }
        };
        if observed_kind != entry.kind
            || (entry.kind == EntryKind::File && metadata.len() != entry.len)
        {
            return Err(invalid_data(format!(
                "fixture entry changed while its metadata was captured: {}",
                entry.source.display()
            )));
        }
        refuse_hard_link(entry, &metadata)?;

        update_shape_digest(&mut shape, entry);
        update_shape_digest(&mut state, entry);
        update_stat_digest(&mut state, &metadata);

        match entry.kind {
            EntryKind::Directory => {
                directories = directories
                    .checked_add(1)
                    .ok_or_else(|| invalid_data("fixture directory counter overflowed"))?;
            }
            EntryKind::File => {
                files = files
                    .checked_add(1)
                    .ok_or_else(|| invalid_data("fixture file counter overflowed"))?;
                bytes = bytes
                    .checked_add(entry.len)
                    .ok_or_else(|| invalid_data("fixture byte counter overflowed"))?;
                if bytes > limits.max_bytes {
                    return Err(invalid_data(format!(
                        "fixture tree exceeds max_bytes {}",
                        limits.max_bytes
                    )));
                }
            }
        }
    }

    Ok(MetadataDigest {
        entries: u64::try_from(entries.len())
            .map_err(|_| invalid_data("fixture entry count does not fit u64"))?,
        files,
        directories,
        bytes,
        shape_sha256: format!("{:x}", shape.finalize()),
        state_sha256: format!("{:x}", state.finalize()),
    })
}

fn update_shape_digest(hasher: &mut Sha256, entry: &TreeEntry) {
    hasher.update([entry.kind.digest_tag()]);
    hasher.update((entry.portable_path.len() as u64).to_le_bytes());
    hasher.update(entry.portable_path.as_bytes());
    hasher.update(entry.len.to_le_bytes());
}

#[cfg(unix)]
fn update_stat_digest(hasher: &mut Sha256, metadata: &fs::Metadata) {
    use std::os::unix::fs::MetadataExt;

    for value in [
        metadata.dev(),
        metadata.ino(),
        u64::from(metadata.mode()),
        metadata.nlink(),
        u64::from(metadata.uid()),
        u64::from(metadata.gid()),
        metadata.rdev(),
        metadata.size(),
        metadata.blksize(),
        metadata.blocks(),
    ] {
        hasher.update(value.to_le_bytes());
    }
    for value in [
        metadata.mtime(),
        metadata.mtime_nsec(),
        metadata.ctime(),
        metadata.ctime_nsec(),
    ] {
        hasher.update(value.to_le_bytes());
    }
}

#[cfg(not(unix))]
fn update_stat_digest(hasher: &mut Sha256, metadata: &fs::Metadata) {
    use std::time::UNIX_EPOCH;

    hasher.update(metadata.len().to_le_bytes());
    hasher.update([u8::from(metadata.permissions().readonly())]);
    match metadata.modified().and_then(|time| {
        time.duration_since(UNIX_EPOCH)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    }) {
        Ok(duration) => {
            hasher.update(duration.as_secs().to_le_bytes());
            hasher.update(duration.subsec_nanos().to_le_bytes());
        }
        Err(_) => hasher.update([0_u8; 12]),
    }
}

#[cfg(unix)]
fn refuse_hard_link(entry: &TreeEntry, metadata: &fs::Metadata) -> io::Result<()> {
    use std::os::unix::fs::MetadataExt;

    if entry.kind == EntryKind::File && metadata.nlink() != 1 {
        return Err(invalid_data(format!(
            "fixture regular files must not have aliases outside the witnessed tree (link count {}): {}",
            metadata.nlink(),
            entry.source.display()
        )));
    }
    Ok(())
}

#[cfg(not(unix))]
fn refuse_hard_link(_entry: &TreeEntry, _metadata: &fs::Metadata) -> io::Result<()> {
    Ok(())
}

fn digest_inventory(entries: &[TreeEntry], limits: TraversalLimits) -> io::Result<PhysicalDigest> {
    let mut hasher = Sha256::new();
    hasher.update(DIGEST_DOMAIN);
    let mut files = 0_u64;
    let mut bytes = 0_u64;
    let mut buffer = vec![0_u8; COPY_BUFFER_BYTES];

    for entry in entries {
        let metadata = fs::symlink_metadata(&entry.source).map_err(|error| {
            contextual(
                error,
                format!("rechecking fixture entry {}", entry.source.display()),
            )
        })?;
        let observed_kind = match entry_type(&metadata) {
            EntryType::Directory => EntryKind::Directory,
            EntryType::File => EntryKind::File,
            EntryType::Symlink | EntryType::Special => {
                return Err(invalid_data(format!(
                    "fixture entry changed to a symlink or special file: {}",
                    entry.source.display()
                )));
            }
        };
        if observed_kind != entry.kind
            || (entry.kind == EntryKind::File && metadata.len() != entry.len)
        {
            return Err(invalid_data(format!(
                "fixture entry changed while being digested: {}",
                entry.source.display()
            )));
        }
        hasher.update([entry.kind.digest_tag()]);
        hasher.update((entry.portable_path.len() as u64).to_le_bytes());
        hasher.update(entry.portable_path.as_bytes());
        hasher.update(entry.len.to_le_bytes());

        if entry.kind == EntryKind::File {
            let mut source = BufReader::new(File::open(&entry.source).map_err(|error| {
                contextual(
                    error,
                    format!("opening fixture file {}", entry.source.display()),
                )
            })?);
            let mut file_bytes = 0_u64;
            loop {
                let read = source.read(&mut buffer).map_err(|error| {
                    contextual(
                        error,
                        format!("reading fixture file {}", entry.source.display()),
                    )
                })?;
                if read == 0 {
                    break;
                }
                file_bytes = file_bytes
                    .checked_add(read as u64)
                    .ok_or_else(|| invalid_data("fixture file byte counter overflowed"))?;
                if file_bytes > entry.len {
                    return Err(invalid_data(format!(
                        "fixture file grew while being digested: {}",
                        entry.source.display()
                    )));
                }
                hasher.update(&buffer[..read]);
            }
            if file_bytes != entry.len {
                return Err(invalid_data(format!(
                    "fixture file changed length while being digested: {}",
                    entry.source.display()
                )));
            }
            files = files
                .checked_add(1)
                .ok_or_else(|| invalid_data("fixture file counter overflowed"))?;
            bytes = bytes
                .checked_add(file_bytes)
                .ok_or_else(|| invalid_data("fixture byte counter overflowed"))?;
            if bytes > limits.max_bytes {
                return Err(invalid_data(format!(
                    "fixture tree exceeds max_bytes {}",
                    limits.max_bytes
                )));
            }
        }
    }

    Ok(PhysicalDigest {
        files,
        bytes,
        digest_sha256: format!("{:x}", hasher.finalize()),
    })
}

#[cfg(target_os = "macos")]
fn clone_inventory_forced(
    entries: &[TreeEntry],
    source_root: &Path,
    destination_root: &Path,
) -> io::Result<()> {
    use std::ffi::CString;
    use std::os::fd::AsRawFd;

    // Values from <sys/clonefile.h>. libc exposes clonefileat but not these
    // newer flags. Both are required: a kernel that does not support them must
    // fail closed rather than weaken path resolution or fall back to copying.
    const CLONE_NOFOLLOW_ANY: u32 = 0x0008;
    const CLONE_RESOLVE_BENEATH: u32 = 0x0010;
    const FLAGS: u32 = CLONE_NOFOLLOW_ANY | CLONE_RESOLVE_BENEATH;

    // Directory clonefile is explicitly discouraged by clonefile(2). Create
    // the witnessed directory shape first, then force one atomic clone syscall
    // for each regular file.
    for entry in entries
        .iter()
        .filter(|entry| entry.kind == EntryKind::Directory)
    {
        let target = destination_root.join(&entry.relative);
        fs::create_dir(&target).map_err(|error| {
            contextual(
                error,
                format!("creating clonefile directory {}", target.display()),
            )
        })?;
    }

    let source_directory = File::open(source_root).map_err(|error| {
        contextual(
            error,
            format!("opening clonefile source root {}", source_root.display()),
        )
    })?;
    let destination_directory = File::open(destination_root).map_err(|error| {
        contextual(
            error,
            format!(
                "opening clonefile destination root {}",
                destination_root.display()
            ),
        )
    })?;

    for entry in entries.iter().filter(|entry| entry.kind == EntryKind::File) {
        let relative = CString::new(entry.portable_path.as_bytes()).map_err(|_| {
            invalid_data(format!(
                "fixture path contains a NUL byte: {}",
                entry.portable_path
            ))
        })?;
        // SAFETY: both descriptors remain open directory descriptors for the
        // duration of the call; `relative` is a live NUL-terminated string;
        // the inventory admits only strict relative paths; and the flags make
        // the kernel reject symlinks or escapes in either hierarchy.
        let result = unsafe {
            libc::clonefileat(
                source_directory.as_raw_fd(),
                relative.as_ptr(),
                destination_directory.as_raw_fd(),
                relative.as_ptr(),
                FLAGS,
            )
        };
        if result != 0 {
            let error = io::Error::last_os_error();
            return Err(contextual(
                error,
                format!(
                    "forced clonefileat of {} to {}",
                    entry.source.display(),
                    destination_root.join(&entry.relative).display()
                ),
            ));
        }
    }
    Ok(())
}

#[cfg(not(target_os = "macos"))]
fn clone_inventory_forced(
    _entries: &[TreeEntry],
    _source_root: &Path,
    _destination_root: &Path,
) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "forced clonefile reset is available only on macOS/APFS; no byte-copy fallback is permitted",
    ))
}

fn copy_inventory(
    entries: &[TreeEntry],
    destination: &Path,
    limits: TraversalLimits,
) -> io::Result<()> {
    let mut copied_bytes = 0_u64;
    let mut buffer = vec![0_u8; COPY_BUFFER_BYTES];
    for entry in entries {
        let target = destination.join(&entry.relative);
        match entry.kind {
            EntryKind::Directory => fs::create_dir(&target).map_err(|error| {
                contextual(
                    error,
                    format!("creating fixture-copy directory {}", target.display()),
                )
            })?,
            EntryKind::File => {
                copied_bytes = copied_bytes
                    .checked_add(copy_regular_file(entry, &target, &mut buffer)?)
                    .ok_or_else(|| invalid_data("copied fixture byte counter overflowed"))?;
                if copied_bytes > limits.max_bytes {
                    return Err(invalid_data(format!(
                        "copied fixture exceeds max_bytes {}",
                        limits.max_bytes
                    )));
                }
            }
        }
    }
    Ok(())
}

fn copy_regular_file(entry: &TreeEntry, target: &Path, buffer: &mut [u8]) -> io::Result<u64> {
    let metadata = fs::symlink_metadata(&entry.source).map_err(|error| {
        contextual(
            error,
            format!("rechecking fixture file {}", entry.source.display()),
        )
    })?;
    if !metadata.file_type().is_file() || metadata.len() != entry.len {
        return Err(invalid_data(format!(
            "fixture file changed before copy: {}",
            entry.source.display()
        )));
    }
    let mut source = BufReader::new(File::open(&entry.source).map_err(|error| {
        contextual(
            error,
            format!("opening fixture file {}", entry.source.display()),
        )
    })?);
    let destination_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(target)
        .map_err(|error| {
            contextual(
                error,
                format!("creating fixture-copy file {}", target.display()),
            )
        })?;
    let mut destination_file = BufWriter::new(destination_file);
    let mut copied = 0_u64;
    loop {
        let read = source.read(buffer).map_err(|error| {
            contextual(
                error,
                format!("reading fixture file {}", entry.source.display()),
            )
        })?;
        if read == 0 {
            break;
        }
        copied = copied
            .checked_add(read as u64)
            .ok_or_else(|| invalid_data("copied fixture file byte counter overflowed"))?;
        if copied > entry.len {
            return Err(invalid_data(format!(
                "fixture file grew while being copied: {}",
                entry.source.display()
            )));
        }
        destination_file
            .write_all(&buffer[..read])
            .map_err(|error| {
                contextual(
                    error,
                    format!("writing fixture-copy file {}", target.display()),
                )
            })?;
    }
    destination_file.flush().map_err(|error| {
        contextual(
            error,
            format!("flushing fixture-copy file {}", target.display()),
        )
    })?;
    if copied != entry.len {
        return Err(invalid_data(format!(
            "fixture file changed length while being copied: {}",
            entry.source.display()
        )));
    }
    Ok(copied)
}

fn refuse_destination_below_source(source: &Path, destination: &Path) -> io::Result<()> {
    let canonical_source = fs::canonicalize(source).map_err(|error| {
        contextual(
            error,
            format!("canonicalizing fixture root {}", source.display()),
        )
    })?;
    refuse_resolved_destination_below(&canonical_source, destination)
}

/// Containment for the contained-fixture handoff, where the worker has
/// already retired the active path: resolve the absent fixture root through
/// its canonical parent, then refuse a template that equals or lies below it.
fn refuse_destination_below_retired_source(source: &Path, destination: &Path) -> io::Result<()> {
    let parent = source
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| {
            invalid_input(format!(
                "fixture root has no parent directory: {}",
                source.display()
            ))
        })?;
    let name = source.file_name().ok_or_else(|| {
        invalid_input(format!(
            "fixture root has no final component: {}",
            source.display()
        ))
    })?;
    let canonical_source = fs::canonicalize(parent)
        .map_err(|error| {
            contextual(
                error,
                format!("canonicalizing fixture-root parent {}", parent.display()),
            )
        })?
        .join(name);
    refuse_resolved_destination_below(&canonical_source, destination)
}

fn refuse_resolved_destination_below(
    canonical_source: &Path,
    destination: &Path,
) -> io::Result<()> {
    let canonical_destination = match fs::symlink_metadata(destination) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() {
                return Err(invalid_input(format!(
                    "fixture-copy destination must not be a symlink: {}",
                    destination.display()
                )));
            }
            fs::canonicalize(destination).map_err(|error| {
                contextual(
                    error,
                    format!("canonicalizing destination {}", destination.display()),
                )
            })?
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let parent = destination
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."));
            let name = destination.file_name().ok_or_else(|| {
                invalid_input(format!(
                    "fixture-copy destination has no final component: {}",
                    destination.display()
                ))
            })?;
            fs::canonicalize(parent)
                .map_err(|error| {
                    contextual(
                        error,
                        format!("canonicalizing destination parent {}", parent.display()),
                    )
                })?
                .join(name)
        }
        Err(error) => {
            return Err(contextual(
                error,
                format!("inspecting destination {}", destination.display()),
            ));
        }
    };

    if canonical_destination.starts_with(canonical_source) {
        return Err(invalid_input(format!(
            "fixture-copy destination must not equal or lie below the fixture root: {}",
            destination.display()
        )));
    }
    Ok(())
}

fn prepare_empty_destination(destination: &Path) -> io::Result<()> {
    match fs::symlink_metadata(destination) {
        Ok(metadata) => {
            if !metadata.file_type().is_dir() {
                return Err(invalid_input(format!(
                    "fixture-copy destination must be an empty directory: {}",
                    destination.display()
                )));
            }
            if fs::read_dir(destination)
                .map_err(|error| {
                    contextual(
                        error,
                        format!("reading destination {}", destination.display()),
                    )
                })?
                .next()
                .transpose()?
                .is_some()
            {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!(
                        "fixture-copy destination is not empty: {}",
                        destination.display()
                    ),
                ));
            }
            Ok(())
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => fs::create_dir(destination)
            .map_err(|error| {
                contextual(
                    error,
                    format!("creating destination {}", destination.display()),
                )
            }),
        Err(error) => Err(contextual(
            error,
            format!("inspecting destination {}", destination.display()),
        )),
    }
}

fn require_digest(
    label: &str,
    expected: &PhysicalDigest,
    observed: &PhysicalDigest,
) -> io::Result<()> {
    if expected == observed {
        return Ok(());
    }
    Err(invalid_data(format!(
        "{label} does not match frozen physical identity: expected {} files / {} bytes / {}, observed {} files / {} bytes / {}",
        expected.files,
        expected.bytes,
        expected.digest_sha256,
        observed.files,
        observed.bytes,
        observed.digest_sha256
    )))
}

fn require_metadata(
    label: &str,
    expected: &MetadataDigest,
    observed: &MetadataDigest,
    comparison: MetadataComparison,
) -> io::Result<()> {
    let shape_matches = expected.entries == observed.entries
        && expected.files == observed.files
        && expected.directories == observed.directories
        && expected.bytes == observed.bytes
        && expected.shape_sha256 == observed.shape_sha256;
    let matches = match comparison {
        MetadataComparison::Shape => shape_matches,
        MetadataComparison::State => {
            shape_matches && expected.state_sha256 == observed.state_sha256
        }
    };
    if matches {
        return Ok(());
    }
    Err(invalid_data(format!(
        "{label} does not match its metadata witness: expected {} entries / {} files / {} directories / {} bytes / shape {} / state {}, observed {} entries / {} files / {} directories / {} bytes / shape {} / state {}",
        expected.entries,
        expected.files,
        expected.directories,
        expected.bytes,
        expected.shape_sha256,
        expected.state_sha256,
        observed.entries,
        observed.files,
        observed.directories,
        observed.bytes,
        observed.shape_sha256,
        observed.state_sha256,
    )))
}

fn invalid_input(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn contextual(error: io::Error, context: impl Into<String>) -> io::Error {
    io::Error::new(error.kind(), format!("{}: {error}", context.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handoff_physical_digest_requires_canonical_sha256() {
        assert!(is_lowercase_sha256(&"0".repeat(64)));
        assert!(is_lowercase_sha256(
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        ));
        assert!(!is_lowercase_sha256("junk"));
        assert!(!is_lowercase_sha256(&"A".repeat(64)));
        assert!(!is_lowercase_sha256(&"0".repeat(63)));
        assert!(!is_lowercase_sha256(&"0".repeat(65)));
    }

    #[cfg(target_os = "macos")]
    fn apfs_tempdir() -> Option<tempfile::TempDir> {
        use std::os::fd::AsRawFd;

        let workspace = tempfile::tempdir().unwrap();
        let directory = File::open(workspace.path()).unwrap();
        // SAFETY: `stats` is initialized storage for `fstatfs`, and the open
        // directory descriptor remains valid for the duration of the call.
        let mut stats = unsafe { std::mem::zeroed::<libc::statfs>() };
        let result = unsafe { libc::fstatfs(directory.as_raw_fd(), &mut stats) };
        assert_eq!(result, 0, "fstatfs failed: {}", io::Error::last_os_error());
        let filesystem_bytes = stats
            .f_fstypename
            .iter()
            .map(|&byte| byte as u8)
            .take_while(|&byte| byte != 0)
            .collect::<Vec<_>>();
        let filesystem = String::from_utf8_lossy(&filesystem_bytes);
        if filesystem != "apfs" {
            eprintln!(
                "SKIP clonefile proof: temporary directory {} is on {filesystem}, not APFS",
                workspace.path().display()
            );
            return None;
        }
        Some(workspace)
    }

    fn fixture_tree(root: &Path) {
        fs::create_dir(root.join("data")).unwrap();
        fs::create_dir(root.join("empty")).unwrap();
        fs::write(root.join("a.txt"), b"alpha").unwrap();
        fs::write(root.join("data/b.bin"), b"beta").unwrap();
    }

    #[test]
    fn digest_formula_is_stable_and_covers_empty_directories() {
        let fixture = tempfile::tempdir().unwrap();
        fixture_tree(fixture.path());

        let observed = digest_physical_tree(fixture.path(), TraversalLimits::default()).unwrap();
        assert_eq!(observed.files, 2);
        assert_eq!(observed.bytes, 9);

        let mut expected = Sha256::new();
        expected.update(DIGEST_DOMAIN);
        for (kind, path, length, contents) in [
            (b'f', "a.txt", 5_u64, b"alpha" as &[u8]),
            (b'd', "data", 0_u64, b"" as &[u8]),
            (b'f', "data/b.bin", 4_u64, b"beta" as &[u8]),
            (b'd', "empty", 0_u64, b"" as &[u8]),
        ] {
            expected.update([kind]);
            expected.update((path.len() as u64).to_le_bytes());
            expected.update(path.as_bytes());
            expected.update(length.to_le_bytes());
            expected.update(contents);
        }
        assert_eq!(observed.digest_sha256, format!("{:x}", expected.finalize()));

        let repeated = digest_physical_tree(fixture.path(), TraversalLimits::default()).unwrap();
        assert_eq!(observed, repeated);
        fs::remove_dir(fixture.path().join("empty")).unwrap();
        let without_empty =
            digest_physical_tree(fixture.path(), TraversalLimits::default()).unwrap();
        assert_ne!(observed.digest_sha256, without_empty.digest_sha256);
    }

    #[test]
    fn metadata_witness_detects_same_length_changes_without_reading_contents() {
        let fixture = tempfile::tempdir().unwrap();
        fixture_tree(fixture.path());
        let limits = TraversalLimits::default();
        let frozen = digest_metadata_tree(fixture.path(), limits).unwrap();

        assert_eq!(frozen.entries, 4);
        assert_eq!(frozen.files, 2);
        assert_eq!(frozen.directories, 2);
        assert_eq!(frozen.bytes, 9);
        verify_metadata_tree(fixture.path(), &frozen, limits).unwrap();

        fs::write(fixture.path().join("a.txt"), b"ALPHA").unwrap();
        let changed = digest_metadata_tree(fixture.path(), limits).unwrap();
        assert_eq!(changed.shape_sha256, frozen.shape_sha256);
        assert_ne!(changed.state_sha256, frozen.state_sha256);
        let error = verify_metadata_tree(fixture.path(), &frozen, limits).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("metadata witness"));
    }

    #[cfg(unix)]
    #[test]
    fn metadata_witness_refuses_regular_file_aliases() {
        let fixture = tempfile::tempdir().unwrap();
        fs::write(fixture.path().join("first"), b"shared").unwrap();
        fs::hard_link(fixture.path().join("first"), fixture.path().join("second")).unwrap();

        let error = digest_metadata_tree(fixture.path(), TraversalLimits::default()).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("link count"));
    }

    #[test]
    fn verified_copy_preserves_the_complete_tree() {
        let fixture = tempfile::tempdir().unwrap();
        fixture_tree(fixture.path());
        let destination_parent = tempfile::tempdir().unwrap();
        let destination = destination_parent.path().join("rep");
        let limits = TraversalLimits::default();
        let frozen = digest_physical_tree(fixture.path(), limits).unwrap();

        let copied = copy_verified(fixture.path(), &destination, &frozen, limits).unwrap();

        assert_eq!(copied, frozen);
        assert_eq!(fs::read(destination.join("a.txt")).unwrap(), b"alpha");
        assert_eq!(fs::read(destination.join("data/b.bin")).unwrap(), b"beta");
        assert!(destination.join("empty").is_dir());
    }

    #[test]
    fn changed_frozen_tree_is_refused_before_copy() {
        let fixture = tempfile::tempdir().unwrap();
        fixture_tree(fixture.path());
        let limits = TraversalLimits::default();
        let frozen = digest_physical_tree(fixture.path(), limits).unwrap();
        fs::write(fixture.path().join("a.txt"), b"changed").unwrap();
        let destination_parent = tempfile::tempdir().unwrap();
        let destination = destination_parent.path().join("rep");

        let error = copy_verified(fixture.path(), &destination, &frozen, limits).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("frozen fixture"));
        assert!(!destination.exists());
    }

    #[test]
    fn nonempty_and_nested_destinations_are_refused() {
        let fixture = tempfile::tempdir().unwrap();
        fixture_tree(fixture.path());
        let limits = TraversalLimits::default();
        let frozen = digest_physical_tree(fixture.path(), limits).unwrap();

        let destination_parent = tempfile::tempdir().unwrap();
        let nonempty = destination_parent.path().join("nonempty");
        fs::create_dir(&nonempty).unwrap();
        fs::write(nonempty.join("resident"), b"keep").unwrap();
        let error = copy_verified(fixture.path(), &nonempty, &frozen, limits).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(fs::read(nonempty.join("resident")).unwrap(), b"keep");

        let nested = fixture.path().join("copy");
        let error = copy_verified(fixture.path(), &nested, &frozen, limits).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(!nested.exists());
    }

    #[test]
    fn traversal_limits_fail_closed() {
        let fixture = tempfile::tempdir().unwrap();
        fixture_tree(fixture.path());

        let error = digest_physical_tree(
            fixture.path(),
            TraversalLimits {
                max_entries: 3,
                ..TraversalLimits::default()
            },
        )
        .unwrap_err();
        assert!(error.to_string().contains("max_entries"));

        let error = digest_physical_tree(
            fixture.path(),
            TraversalLimits {
                max_bytes: 8,
                ..TraversalLimits::default()
            },
        )
        .unwrap_err();
        assert!(error.to_string().contains("max_bytes"));

        let error = digest_physical_tree(
            fixture.path(),
            TraversalLimits {
                max_depth: 1,
                ..TraversalLimits::default()
            },
        )
        .unwrap_err();
        assert!(error.to_string().contains("max_depth"));
    }

    #[cfg(unix)]
    #[test]
    fn symlinks_and_special_files_are_refused_without_following() {
        use std::os::unix::fs::symlink;

        let fixture = tempfile::tempdir().unwrap();
        fs::write(fixture.path().join("target"), b"outside").unwrap();
        symlink(fixture.path().join("target"), fixture.path().join("link")).unwrap();
        let error = digest_physical_tree(fixture.path(), TraversalLimits::default()).unwrap_err();
        assert!(error.to_string().contains("symlinks"));

        fs::remove_file(fixture.path().join("link")).unwrap();
        #[cfg(target_os = "macos")]
        let socket_mode = u32::from(nix::libc::S_IFSOCK | 0o600);
        #[cfg(not(target_os = "macos"))]
        let socket_mode = nix::libc::S_IFSOCK | 0o600;
        let socket = classify_unix_mode(socket_mode);
        assert_eq!(socket, EntryType::Special);
        let error = supported_inventory_kind(socket, Path::new("socket")).unwrap_err();
        assert!(error.to_string().contains("special files"));
    }

    #[test]
    fn retired_source_containment_accepts_a_sibling_but_refuses_the_source_path() {
        let workspace = tempfile::tempdir().unwrap();
        let active = workspace.path().join("active");
        let template = workspace.path().join("template");
        fs::create_dir(&template).unwrap();
        assert!(!active.exists());

        refuse_destination_below_retired_source(&active, &template).unwrap();

        let error = refuse_destination_below_retired_source(&active, &active).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(
            error
                .to_string()
                .contains("must not equal or lie below the fixture root"),
            "{error}"
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn clonefile_template_restores_exact_active_path_with_cow_isolation() {
        let Some(workspace) = apfs_tempdir() else {
            return;
        };
        let active = workspace.path().join("active");
        let template = workspace.path().join("template");
        fs::create_dir(&active).unwrap();
        fixture_tree(&active);
        let limits = TraversalLimits::default();

        let frozen = freeze_clonefile_template(&active, &template, limits).unwrap();
        assert_eq!(frozen.active_root(), active);
        assert_eq!(frozen.template_root(), template);
        // Full content reads are test-only. Production reset proof is the
        // clonefile syscall plus metadata witnesses.
        assert_eq!(
            digest_physical_tree(&template, limits).unwrap(),
            *frozen.physical_digest()
        );
        frozen.verify_unchanged().unwrap();

        fs::remove_dir_all(&active).unwrap();
        let prepared = frozen.restore_active().unwrap();
        assert_eq!(prepared.root(), active);
        assert_eq!(
            digest_physical_tree(&active, limits).unwrap(),
            *frozen.physical_digest()
        );
        prepared.verify_unchanged().unwrap();

        fs::write(active.join("a.txt"), b"ALPHA").unwrap();
        assert!(prepared.verify_unchanged().is_err());
        assert_eq!(fs::read(template.join("a.txt")).unwrap(), b"alpha");
        frozen.verify_unchanged().unwrap();

        fs::remove_dir_all(&active).unwrap();
        let second = frozen.restore_active().unwrap();
        assert_eq!(fs::read(active.join("a.txt")).unwrap(), b"alpha");
        second.verify_unchanged().unwrap();
        frozen.verify_unchanged().unwrap();
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn changed_template_is_refused_before_restoring_active() {
        let Some(workspace) = apfs_tempdir() else {
            return;
        };
        let active = workspace.path().join("active");
        let template = workspace.path().join("template");
        fs::create_dir(&active).unwrap();
        fixture_tree(&active);
        let limits = TraversalLimits::default();
        let frozen = freeze_clonefile_template(&active, &template, limits).unwrap();
        fs::remove_dir_all(&active).unwrap();
        fs::write(template.join("a.txt"), b"ALPHA").unwrap();

        let error = frozen.restore_active().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(!active.exists());
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn contained_handoff_accepts_only_a_retired_active_path() {
        let Some(workspace) = apfs_tempdir() else {
            return;
        };
        let active = workspace.path().join("active");
        let template = workspace.path().join("template");
        fs::create_dir(&active).unwrap();
        fixture_tree(&active);
        let limits = TraversalLimits::default();
        let frozen = freeze_clonefile_template(&active, &template, limits).unwrap();
        let physical = frozen.physical_digest().clone();
        let metadata = digest_metadata_tree(&template, limits).unwrap();

        let left_behind = accept_clonefile_template_handoff(
            &active,
            &template,
            physical.clone(),
            metadata.clone(),
            limits,
        )
        .unwrap_err();
        assert!(
            left_behind
                .to_string()
                .contains("left the active path behind"),
            "{left_behind}"
        );

        // The contained worker retires the active tree before handing off;
        // the parent must accept exactly that state.
        fs::remove_dir_all(&active).unwrap();
        let accepted =
            accept_clonefile_template_handoff(&active, &template, physical, metadata, limits)
                .unwrap();
        assert_eq!(accepted.active_root(), active);
        assert_eq!(accepted.template_root(), template);
        let prepared = accepted.restore_active().unwrap();
        assert_eq!(
            digest_physical_tree(&active, limits).unwrap(),
            *accepted.physical_digest()
        );
        prepared.verify_unchanged().unwrap();
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn clonefile_template_requires_stable_absolute_paths() {
        let workspace = tempfile::tempdir().unwrap();
        let active = workspace.path().join("active");
        fs::create_dir(&active).unwrap();
        fixture_tree(&active);

        let error = freeze_clonefile_template(
            Path::new("relative-active"),
            &workspace.path().join("template"),
            TraversalLimits::default(),
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("absolute path"));
    }

    #[cfg(not(target_os = "macos"))]
    #[test]
    fn clonefile_template_has_no_non_macos_copy_fallback() {
        let workspace = tempfile::tempdir().unwrap();
        let active = workspace.path().join("active");
        let template = workspace.path().join("template");
        fs::create_dir(&active).unwrap();
        fixture_tree(&active);

        let error =
            freeze_clonefile_template(&active, &template, TraversalLimits::default()).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Unsupported);
        assert!(error.to_string().contains("no byte-copy fallback"));
        assert!(!template.exists());
    }

    #[cfg(all(unix, not(target_vendor = "apple")))]
    #[test]
    fn non_utf8_paths_are_refused() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let fixture = tempfile::tempdir().unwrap();
        let invalid_name = OsString::from_vec(vec![b'n', 0x80]);
        fs::write(fixture.path().join(invalid_name), b"bytes").unwrap();

        let error = digest_physical_tree(fixture.path(), TraversalLimits::default()).unwrap_err();
        assert!(error.to_string().contains("not valid UTF-8"));
    }
}
