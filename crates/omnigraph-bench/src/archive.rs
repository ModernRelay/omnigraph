//! Immutable, content-addressed storage for benchmark run records.
//!
//! JSON records are the telemetry authority. Publication first installs a
//! content-addressed object and then atomically claims the invocation id with
//! an immutable pointer. A crash before the pointer can leave an unreachable
//! object, but can never expose a partial record. Database projections consume
//! only validated invocation pointers and are therefore always rebuildable.
//! Pointer publication and inventory capture coordinate through bounded
//! exclusive/shared locks on the archive root directory inode.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::fs::{self, File, OpenOptions};
#[cfg(any(unix, test))]
use std::io::Read;
use std::io::Write;
#[cfg(unix)]
use std::io::{Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

#[cfg(unix)]
use nix::errno::Errno;
#[cfg(unix)]
use nix::fcntl::{AtFlags, Flock, FlockArg, OFlag, openat};
#[cfg(unix)]
use nix::sys::stat::{Mode, SFlag, fchmod, fstat, fstatat, mkdirat};
#[cfg(unix)]
use nix::unistd::{UnlinkatFlags, linkat, unlinkat};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
#[cfg(unix)]
use std::os::fd::{IntoRawFd, OwnedFd};
#[cfg(unix)]
use std::sync::Arc;
#[cfg(unix)]
use std::time::Instant;

use crate::record::{RunRecordV1, canonical_record_bytes, parse_canonical_record};

/// Version of the archive pointer and receipt contract.
pub const ARCHIVE_FORMAT_VERSION: u32 = 1;
/// Maximum canonical run-record size accepted by both archive publication and
/// the run-record-v1 parser.
pub const MAX_ARCHIVE_RECORD_BYTES: u64 = crate::record::MAX_RECORD_BYTES as u64;
/// Aggregate serialized-record budget for the compatibility materializer.
/// Callers processing larger archives must use [`ArchiveRecordIter`].
pub const MAX_MATERIALIZED_ARCHIVE_BYTES: u64 = 256 * 1024 * 1024;

const MAX_POINTER_BYTES: u64 = 16 * 1024;
const MAX_ARCHIVE_POINTERS: usize = 1_000_000;
const MAX_ARCHIVE_SHARDS: usize = 1_024;
const MAX_DIRECTORY_ENTRIES: usize = 1_100_000;
const OBJECT_DIRECTORY: &str = "objects";
const INVOCATION_DIRECTORY: &str = "invocations";
const SHA256_DIRECTORY: &str = "sha256";
const RECORD_SUFFIX: &str = ".run-record-v1.json";
const POINTER_SUFFIX: &str = ".pointer-v1.json";
const PREFLIGHT_PROBE_BYTES: &[u8] = b"omnigraph-bench-archive-preflight-v1\n";
const ARCHIVE_LOCK_TIMEOUT: Duration = Duration::from_secs(30);
const POST_LINK_DIRECTORY_SYNC_ATTEMPTS: usize = 3;
const LINK_VISIBLE_SYNC_FAILURE_CODE: &str = "archive_link_visible_directory_sync_failed";
#[cfg(unix)]
const ARCHIVE_LOCK_RETRY_INTERVAL: Duration = Duration::from_millis(25);

static STAGING_COUNTER: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
type PublicationRootSwapHook = Box<dyn FnOnce()>;
#[cfg(test)]
type PublicationRootSwapHooks = (
    Option<PublicationRootSwapHook>,
    Option<PublicationRootSwapHook>,
);
#[cfg(test)]
type PointerPostSyncHook = Box<dyn FnOnce()>;
#[cfg(test)]
type PointerAncestorChainPostSyncHook = Box<dyn FnOnce()>;
#[cfg(test)]
type ImmutableParentPostEnsureHook = Box<dyn FnOnce()>;

#[cfg(test)]
std::thread_local! {
    static INJECT_POINTER_DIRECTORY_SYNC_FAILURES:
        std::cell::RefCell<std::collections::VecDeque<i32>> =
        const { std::cell::RefCell::new(std::collections::VecDeque::new()) };
    static INJECT_PUBLICATION_ROOT_SWAP_HOOKS: std::cell::RefCell<Option<PublicationRootSwapHooks>> =
        const { std::cell::RefCell::new(None) };
    static INJECT_POINTER_POST_SYNC_HOOK: std::cell::RefCell<Option<PointerPostSyncHook>> =
        const { std::cell::RefCell::new(None) };
    static INJECT_POINTER_ANCESTOR_CHAIN_POST_SYNC_HOOK:
        std::cell::RefCell<Option<PointerAncestorChainPostSyncHook>> =
        const { std::cell::RefCell::new(None) };
    static INJECT_IMMUTABLE_PARENT_POST_ENSURE_HOOK:
        std::cell::RefCell<Option<ImmutableParentPostEnsureHook>> =
        const { std::cell::RefCell::new(None) };
}

fn injected_pointer_directory_sync_failure(path: &Path) -> Option<std::io::Error> {
    #[cfg(test)]
    {
        if is_invocation_pointer_path(path) {
            return INJECT_POINTER_DIRECTORY_SYNC_FAILURES.with(|failures| {
                failures
                    .borrow_mut()
                    .pop_front()
                    .map(std::io::Error::from_raw_os_error)
            });
        }
    }
    #[cfg(not(test))]
    let _ = path;
    None
}

fn is_invocation_pointer_path(path: &Path) -> bool {
    path.components()
        .any(|component| component.as_os_str() == std::ffi::OsStr::new(INVOCATION_DIRECTORY))
        && path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(POINTER_SUFFIX))
}

/// Stable reference returned only after an invocation pointer is durable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveReceiptV1 {
    pub archive_format_version: u32,
    pub invocation_id: String,
    pub record_sha256: String,
    pub object_relative_path: String,
    pub pointer_relative_path: String,
    /// False means an identical invocation pointer was already present.
    pub newly_published: bool,
}

/// One fully validated authority record discovered through its invocation
/// pointer. Unreachable content objects are deliberately not records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchivedRecord {
    pub record: RunRecordV1,
    pub receipt: ArchiveReceiptV1,
}

/// Streaming view of the canonical invocation inventory.
///
/// Opening the iterator retains only fixed-size 26-byte invocation ids. Each
/// call validates and materializes exactly one pointed-to record. The first
/// invalid record terminates the iterator, so an error cannot be skipped by
/// continuing iteration.
#[derive(Debug, Clone)]
pub struct ArchiveRecordIter {
    archive_root: PathBuf,
    archive_root_identity: ArchiveRootIdentity,
    anchored_root: AnchoredArchiveRoot,
    invocation_ids: Vec<InvocationId>,
    next_index: usize,
    failed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct InvocationId([u8; 26]);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InvocationPointerV1 {
    archive_format_version: u32,
    invocation_id: String,
    record_sha256: String,
    object_relative_path: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArchiveLockMode {
    Shared,
    Exclusive,
}

/// Advisory lock over the archive root directory inode.
///
/// Locking the directory itself keeps read-only verification read-only and
/// gives an empty archive a coordination inode without introducing a mutable
/// lock file into the archive layout.
#[derive(Debug)]
struct ArchiveLock {
    #[cfg(unix)]
    _lock: Flock<File>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ArchiveRootIdentity {
    device: u64,
    inode: u64,
}

/// Descriptor-rooted view of one archive inode.
///
/// The descriptor is opened relative to the already-locked root descriptor,
/// rather than by repeating the caller-facing path lookup. It deliberately
/// has its own open-file description so dropping the inventory lock does not
/// keep the shared flock held while records are streamed.
#[derive(Debug, Clone)]
struct AnchoredArchiveRoot {
    display_path: PathBuf,
    identity: ArchiveRootIdentity,
    #[cfg(unix)]
    directory: Arc<File>,
}

#[cfg(unix)]
struct CapturedArchiveDirectory {
    descriptor: File,
    display_path: PathBuf,
    name_from_parent: Option<String>,
}

impl ArchiveLock {
    fn validate_root(&self, archive_root: &Path) -> Result<(), ArchiveError> {
        #[cfg(unix)]
        {
            validate_archive_lock_target(archive_root, &self._lock)
        }
        #[cfg(not(unix))]
        {
            Err(ArchiveError::new(
                "archive_lock_unsupported",
                Some(archive_root),
                "archive root validation requires Unix descriptor identity",
            ))
        }
    }

    fn root_identity(&self) -> Result<ArchiveRootIdentity, ArchiveError> {
        #[cfg(unix)]
        {
            archive_root_identity(&self._lock)
        }
        #[cfg(not(unix))]
        {
            Err(ArchiveError::new(
                "archive_lock_unsupported",
                None,
                "archive root identity requires Unix descriptor metadata",
            ))
        }
    }

    fn anchored_root(&self, archive_root: &Path) -> Result<AnchoredArchiveRoot, ArchiveError> {
        #[cfg(unix)]
        {
            self.validate_root(archive_root)?;
            let descriptor = openat(&*self._lock, ".", directory_open_flags(), Mode::empty())
                .map_err(|error| {
                    anchored_errno(
                        "archive_root_open_failed",
                        archive_root,
                        error,
                        "could not open the locked archive root descriptor",
                    )
                })?;
            let directory = File::from(descriptor);
            let identity = archive_root_identity(&directory)?;
            let locked_identity = self.root_identity()?;
            if identity != locked_identity {
                return Err(ArchiveError::new(
                    "archive_lock_invalid",
                    Some(archive_root),
                    "descriptor-relative archive root does not match the locked root inode",
                ));
            }
            self.validate_root(archive_root)?;
            Ok(AnchoredArchiveRoot {
                display_path: archive_root.to_path_buf(),
                identity,
                directory: Arc::new(directory),
            })
        }
        #[cfg(not(unix))]
        {
            Err(ArchiveError::new(
                "archive_lock_unsupported",
                Some(archive_root),
                "descriptor-rooted archive traversal requires Unix openat semantics",
            ))
        }
    }
}

#[cfg(unix)]
fn directory_open_flags() -> OFlag {
    OFlag::O_RDONLY | OFlag::O_CLOEXEC | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_NONBLOCK
}

#[cfg(unix)]
fn regular_file_open_flags() -> OFlag {
    OFlag::O_RDONLY | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW | OFlag::O_NONBLOCK
}

#[cfg(unix)]
fn anchored_errno(code: &'static str, path: &Path, error: Errno, context: &str) -> ArchiveError {
    ArchiveError::new(code, Some(path), format!("{context}: {error}"))
}

#[cfg(unix)]
fn openat_directory(parent: &File, name: &str, path: &Path) -> Result<File, ArchiveError> {
    openat(parent, name, directory_open_flags(), Mode::empty())
        .map(File::from)
        .map_err(|error| match error {
            Errno::ELOOP | Errno::ENOTDIR => ArchiveError::new(
                "archive_layout_invalid",
                Some(path),
                "archive ancestors must be real directories, not symlinks or special files",
            ),
            _ => anchored_errno(
                "archive_directory_open_failed",
                path,
                error,
                "could not open descriptor-relative archive directory",
            ),
        })
}

#[cfg(unix)]
fn openat_directory_optional(
    parent: &File,
    name: &str,
    path: &Path,
) -> Result<Option<File>, ArchiveError> {
    match openat(parent, name, directory_open_flags(), Mode::empty()) {
        Ok(descriptor) => Ok(Some(File::from(descriptor))),
        Err(Errno::ENOENT) => Ok(None),
        Err(Errno::ELOOP | Errno::ENOTDIR) => Err(ArchiveError::new(
            "archive_layout_invalid",
            Some(path),
            "archive ancestors must be real directories, not symlinks or special files",
        )),
        Err(error) => Err(anchored_errno(
            "archive_directory_open_failed",
            path,
            error,
            "could not open descriptor-relative archive directory",
        )),
    }
}

#[cfg(unix)]
fn anchored_entry_type(parent: &File, name: &str, path: &Path) -> Result<SFlag, ArchiveError> {
    let metadata = fstatat(parent, name, AtFlags::AT_SYMLINK_NOFOLLOW).map_err(|error| {
        anchored_errno(
            "archive_entry_inspection_failed",
            path,
            error,
            "could not inspect descriptor-relative archive entry",
        )
    })?;
    Ok(SFlag::from_bits_truncate(metadata.st_mode))
}

#[cfg(unix)]
struct AnchoredDirectoryStream {
    directory: *mut nix::libc::DIR,
    display_path: PathBuf,
}

#[cfg(unix)]
impl AnchoredDirectoryStream {
    fn from_fd(descriptor: OwnedFd, display_path: &Path) -> Result<Self, ArchiveError> {
        let raw_descriptor = descriptor.into_raw_fd();
        // SAFETY: `raw_descriptor` is exclusively owned here. On success,
        // fdopendir takes ownership; on failure we close it below.
        let directory = unsafe { nix::libc::fdopendir(raw_descriptor) };
        if directory.is_null() {
            let error = Errno::last();
            // SAFETY: fdopendir did not take ownership on failure.
            unsafe {
                nix::libc::close(raw_descriptor);
            }
            return Err(anchored_errno(
                "archive_directory_open_failed",
                display_path,
                error,
                "could not create descriptor-relative directory stream",
            ));
        }
        Ok(Self {
            directory,
            display_path: display_path.to_path_buf(),
        })
    }
}

#[cfg(unix)]
impl Iterator for AnchoredDirectoryStream {
    type Item = Result<String, ArchiveError>;

    fn next(&mut self) -> Option<Self::Item> {
        Errno::clear();
        // SAFETY: `directory` is a live DIR owned exclusively by this iterator.
        let entry = unsafe { nix::libc::readdir(self.directory) };
        if entry.is_null() {
            let raw_error = Errno::last_raw();
            return (raw_error != 0).then(|| {
                Err(anchored_errno(
                    "archive_directory_entry_failed",
                    &self.display_path,
                    Errno::from_raw(raw_error),
                    "could not enumerate descriptor-relative archive directory",
                ))
            });
        }
        // SAFETY: readdir returned a live entry whose nul-terminated d_name is
        // valid until the next call; it is copied into an owned String now.
        let name = unsafe { std::ffi::CStr::from_ptr((*entry).d_name.as_ptr()) };
        Some(name.to_str().map(str::to_owned).map_err(|_| {
            ArchiveError::new(
                "archive_layout_invalid",
                Some(&self.display_path),
                "archive entry name is not valid UTF-8",
            )
        }))
    }
}

#[cfg(unix)]
impl Drop for AnchoredDirectoryStream {
    fn drop(&mut self) {
        // SAFETY: this iterator exclusively owns the live DIR and closes it
        // exactly once here, including its underlying descriptor.
        unsafe {
            nix::libc::closedir(self.directory);
        }
    }
}

#[cfg(unix)]
fn anchored_directory_stream(
    directory: &File,
    display_path: &Path,
) -> Result<AnchoredDirectoryStream, ArchiveError> {
    let descriptor =
        openat(directory, ".", directory_open_flags(), Mode::empty()).map_err(|error| {
            anchored_errno(
                "archive_directory_open_failed",
                display_path,
                error,
                "could not duplicate descriptor-relative archive directory for enumeration",
            )
        })?;
    AnchoredDirectoryStream::from_fd(descriptor, display_path)
}

impl AnchoredArchiveRoot {
    fn display_path(&self, relative: &str) -> PathBuf {
        join_relative(&self.display_path, relative)
    }

    #[cfg(unix)]
    fn relative_components<'a>(&self, relative: &'a str) -> Result<Vec<&'a str>, ArchiveError> {
        let components = relative.split('/').collect::<Vec<_>>();
        if components.is_empty()
            || components
                .iter()
                .any(|component| component.is_empty() || matches!(*component, "." | ".."))
        {
            return Err(ArchiveError::new(
                "archive_path_invalid",
                Some(&self.display_path(relative)),
                "archive-relative paths must contain only nonempty normal components",
            ));
        }
        Ok(components)
    }

    #[cfg(unix)]
    fn root_descriptor(&self) -> Result<File, ArchiveError> {
        openat(
            self.directory.as_ref(),
            ".",
            directory_open_flags(),
            Mode::empty(),
        )
        .map(File::from)
        .map_err(|error| {
            anchored_errno(
                "archive_root_open_failed",
                &self.display_path,
                error,
                "could not duplicate captured archive root descriptor",
            )
        })
    }

    #[cfg(unix)]
    fn open_relative_directory_optional(
        &self,
        relative: &str,
    ) -> Result<Option<File>, ArchiveError> {
        let components = self.relative_components(relative)?;
        let mut directory = self.root_descriptor()?;
        let mut display = self.display_path.clone();
        for component in components {
            display.push(component);
            let Some(next) = openat_directory_optional(&directory, component, &display)? else {
                return Ok(None);
            };
            directory = next;
        }
        Ok(Some(directory))
    }

    #[cfg(unix)]
    fn open_relative_parent(
        &self,
        relative: &str,
    ) -> Result<(File, String, PathBuf), ArchiveError> {
        let mut components = self.relative_components(relative)?;
        let file_name = components
            .pop()
            .expect("relative component validation requires one component")
            .to_string();
        let mut directory = self.root_descriptor()?;
        let mut display = self.display_path.clone();
        for component in components {
            display.push(component);
            directory = openat_directory(&directory, component, &display)?;
        }
        Ok((directory, file_name, self.display_path(relative)))
    }

    #[cfg(unix)]
    fn relative_path_exists(&self, relative: &str) -> Result<bool, ArchiveError> {
        let (parent, name, display) = match self.open_relative_parent(relative) {
            Ok(opened) => opened,
            Err(error) if error.code == "archive_directory_open_failed" => {
                // A missing authority parent is a definite absence. Other
                // directory errors remain fail-closed.
                if !self.relative_parent_exists(relative)? {
                    return Ok(false);
                }
                return Err(error);
            }
            Err(error) => return Err(error),
        };
        match fstatat(&parent, name.as_str(), AtFlags::AT_SYMLINK_NOFOLLOW) {
            Ok(metadata) if SFlag::from_bits_truncate(metadata.st_mode) == SFlag::S_IFLNK => {
                Err(ArchiveError::new(
                    "archive_layout_invalid",
                    Some(&display),
                    "archive entries may not be symlinks",
                ))
            }
            Ok(_) => Ok(true),
            Err(Errno::ENOENT) => Ok(false),
            Err(error) => Err(anchored_errno(
                "archive_file_inspection_failed",
                &display,
                error,
                "could not inspect descriptor-relative archive entry",
            )),
        }
    }

    #[cfg(not(unix))]
    fn relative_path_exists(&self, _relative: &str) -> Result<bool, ArchiveError> {
        Err(ArchiveError::new(
            "archive_lock_unsupported",
            Some(&self.display_path),
            "descriptor-rooted archive traversal requires Unix openat semantics",
        ))
    }

    #[cfg(unix)]
    fn relative_parent_exists(&self, relative: &str) -> Result<bool, ArchiveError> {
        let mut components = self.relative_components(relative)?;
        components.pop();
        if components.is_empty() {
            return Ok(true);
        }
        self.open_relative_directory_optional(&components.join("/"))
            .map(|directory| directory.is_some())
    }

    #[cfg(unix)]
    fn open_relative_regular_file(
        &self,
        relative: &str,
    ) -> Result<(File, File, String, PathBuf), ArchiveError> {
        let (parent, name, display) = self.open_relative_parent(relative)?;
        let descriptor = openat(
            &parent,
            name.as_str(),
            regular_file_open_flags(),
            Mode::empty(),
        )
        .map_err(|error| match error {
            Errno::ELOOP => ArchiveError::new(
                "archive_file_invalid",
                Some(&display),
                "archive file must be a regular file, not a symlink or special file",
            ),
            _ => anchored_errno(
                "archive_file_open_failed",
                &display,
                error,
                "could not open descriptor-relative archive file",
            ),
        })?;
        let file = File::from(descriptor);
        validate_anchored_regular_file(&parent, &name, &display, &file)?;
        Ok((file, parent, name, display))
    }

    #[cfg(unix)]
    fn read_bounded_relative_regular_file(
        &self,
        relative: &str,
        limit: u64,
    ) -> Result<Vec<u8>, ArchiveError> {
        let (mut file, parent, name, display) = self.open_relative_regular_file(relative)?;
        let metadata = fstat(&file).map_err(|error| {
            anchored_errno(
                "archive_file_inspection_failed",
                &display,
                error,
                "could not inspect descriptor-relative archive file",
            )
        })?;
        if metadata.st_size < 0 || u64::try_from(metadata.st_size).unwrap_or(u64::MAX) > limit {
            return Err(ArchiveError::new(
                "archive_file_too_large",
                Some(&display),
                format!("file exceeds the {limit}-byte read bound"),
            ));
        }
        let capacity = usize::try_from(metadata.st_size).map_err(|_| {
            ArchiveError::new(
                "archive_file_too_large",
                Some(&display),
                "file length does not fit this host's address space",
            )
        })?;
        let mut bytes = Vec::with_capacity(capacity);
        Read::by_ref(&mut file)
            .take(limit.saturating_add(1))
            .read_to_end(&mut bytes)
            .map_err(|error| ArchiveError::io("archive_file_read_failed", &display, error))?;
        if u64::try_from(bytes.len())
            .ok()
            .is_none_or(|length| length > limit)
        {
            return Err(ArchiveError::new(
                "archive_file_too_large",
                Some(&display),
                format!("file exceeded the {limit}-byte read bound"),
            ));
        }
        validate_anchored_regular_file(&parent, &name, &display, &file)?;
        Ok(bytes)
    }

    #[cfg(not(unix))]
    fn read_bounded_relative_regular_file(
        &self,
        _relative: &str,
        _limit: u64,
    ) -> Result<Vec<u8>, ArchiveError> {
        Err(ArchiveError::new(
            "archive_lock_unsupported",
            Some(&self.display_path),
            "descriptor-rooted archive traversal requires Unix openat semantics",
        ))
    }

    #[cfg(unix)]
    fn ensure_relative_directory(&self, relative: &str) -> Result<(), ArchiveError> {
        let components = self.relative_components(relative)?;
        let mut parent = self.root_descriptor()?;
        let mut display = self.display_path.clone();
        for component in components {
            display.push(component);
            let directory = match openat_directory_optional(&parent, component, &display)? {
                Some(directory) => directory,
                None => {
                    match mkdirat(&parent, component, Mode::from_bits_truncate(0o777)) {
                        Ok(()) | Err(Errno::EEXIST) => {}
                        Err(error) => {
                            return Err(anchored_errno(
                                "archive_directory_create_failed",
                                &display,
                                error,
                                "could not create descriptor-relative archive directory",
                            ));
                        }
                    }
                    openat_directory(&parent, component, &display)?
                }
            };
            directory.sync_all().map_err(|error| {
                ArchiveError::io("archive_directory_sync_failed", &display, error)
            })?;
            parent.sync_all().map_err(|error| {
                ArchiveError::io(
                    "archive_directory_sync_failed",
                    display.parent().unwrap_or(&self.display_path),
                    error,
                )
            })?;
            parent = directory;
        }
        Ok(())
    }

    #[cfg(unix)]
    fn ensure_archive_layout(&self) -> Result<(), ArchiveError> {
        self.ensure_relative_directory(OBJECT_DIRECTORY)?;
        self.ensure_relative_directory(&format!("{OBJECT_DIRECTORY}/{SHA256_DIRECTORY}"))?;
        self.ensure_relative_directory(INVOCATION_DIRECTORY)
    }

    #[cfg(not(unix))]
    fn ensure_archive_layout(&self) -> Result<(), ArchiveError> {
        Err(ArchiveError::new(
            "archive_lock_unsupported",
            Some(&self.display_path),
            "descriptor-rooted archive publication requires Unix openat semantics",
        ))
    }

    #[cfg(unix)]
    fn install_immutable(
        &self,
        relative: &str,
        bytes: &[u8],
        limit: u64,
    ) -> Result<bool, ArchiveError> {
        let mut components = self.relative_components(relative)?;
        let name = components
            .pop()
            .expect("relative component validation requires one component")
            .to_string();
        if components.is_empty() {
            return Err(ArchiveError::new(
                "archive_path_invalid",
                Some(&self.display_path(relative)),
                "immutable archive entries require a parent below the archive root",
            ));
        }
        let parent_relative = components.join("/");
        self.ensure_relative_directory(&parent_relative)?;
        run_immutable_parent_post_ensure_hook();
        let parent = self
            .open_relative_directory_optional(&parent_relative)?
            .ok_or_else(|| {
                ArchiveError::new(
                    "archive_directory_replaced",
                    Some(&self.display_path(&parent_relative)),
                    "immutable-entry parent disappeared after descriptor-relative creation",
                )
            })?;
        let display = self.display_path(relative);

        match fstatat(&parent, name.as_str(), AtFlags::AT_SYMLINK_NOFOLLOW) {
            Ok(_) => {
                return self
                    .require_identical_relative_file(relative, bytes, limit)
                    .and_then(|()| self.sync_relative_regular_file(relative))
                    .and_then(|()| {
                        self.sync_visible_relative_entry_directory_on(
                            relative, &parent, bytes, limit,
                        )
                    })
                    .map(|()| false)
                    .map_err(|error| link_visible_failure(&display, error));
            }
            Err(Errno::ENOENT) => {}
            Err(error) => {
                return Err(anchored_errno(
                    "archive_file_inspection_failed",
                    &display,
                    error,
                    "could not inspect descriptor-relative immutable entry",
                ));
            }
        }

        let (staging_name, _staging_file) =
            create_anchored_staging_file(&parent, &self.display_path(&parent_relative), bytes)?;
        let link_result = linkat(
            &parent,
            staging_name.as_str(),
            &parent,
            name.as_str(),
            AtFlags::empty(),
        );
        let result = match link_result {
            Ok(()) => self
                .require_identical_relative_file(relative, bytes, limit)
                .and_then(|()| self.sync_relative_regular_file(relative))
                .and_then(|()| {
                    self.sync_visible_relative_entry_directory_on(relative, &parent, bytes, limit)
                })
                .map(|()| true)
                .map_err(|error| link_visible_failure(&display, error)),
            Err(Errno::EEXIST) => self
                .require_identical_relative_file(relative, bytes, limit)
                .and_then(|()| self.sync_relative_regular_file(relative))
                .and_then(|()| {
                    self.sync_visible_relative_entry_directory_on(relative, &parent, bytes, limit)
                })
                .map(|()| false)
                .map_err(|error| link_visible_failure(&display, error)),
            Err(error) => Err(anchored_errno(
                "archive_publish_failed",
                &display,
                error,
                "could not link descriptor-relative immutable entry",
            )),
        };
        let _ = unlinkat(&parent, staging_name.as_str(), UnlinkatFlags::NoRemoveDir);
        result
    }

    #[cfg(not(unix))]
    fn install_immutable(
        &self,
        _relative: &str,
        _bytes: &[u8],
        _limit: u64,
    ) -> Result<bool, ArchiveError> {
        Err(ArchiveError::new(
            "archive_lock_unsupported",
            Some(&self.display_path),
            "descriptor-rooted archive publication requires Unix openat semantics",
        ))
    }

    #[cfg(unix)]
    fn require_identical_relative_file(
        &self,
        relative: &str,
        expected: &[u8],
        limit: u64,
    ) -> Result<(), ArchiveError> {
        let observed = self.read_bounded_relative_regular_file(relative, limit)?;
        if observed != expected {
            return Err(ArchiveError::new(
                "archive_immutable_conflict",
                Some(&self.display_path(relative)),
                "immutable archive path already contains different bytes",
            ));
        }
        Ok(())
    }

    #[cfg(unix)]
    fn sync_relative_regular_file(&self, relative: &str) -> Result<(), ArchiveError> {
        let (file, parent, name, display) = self.open_relative_regular_file(relative)?;
        file.sync_all()
            .map_err(|error| ArchiveError::io("archive_file_sync_failed", &display, error))?;
        validate_anchored_regular_file(&parent, &name, &display, &file)
    }

    #[cfg(unix)]
    fn sync_relative_file_and_ancestor_chain(
        &self,
        relative: &str,
        expected: &[u8],
        limit: u64,
    ) -> Result<(), ArchiveError> {
        let mut components = self.relative_components(relative)?;
        let name = components
            .pop()
            .expect("relative component validation requires one component")
            .to_string();
        if components.is_empty() {
            return Err(ArchiveError::new(
                "archive_path_invalid",
                Some(&self.display_path(relative)),
                "durable archive entries require a parent below the archive root",
            ));
        }

        let mut directories = Vec::with_capacity(components.len().saturating_add(1));
        directories.push(CapturedArchiveDirectory {
            descriptor: self.root_descriptor()?,
            display_path: self.display_path.clone(),
            name_from_parent: None,
        });
        let mut display = self.display_path.clone();
        for component in components {
            display.push(component);
            let descriptor = openat_directory(
                &directories
                    .last()
                    .expect("captured directory chain starts at the archive root")
                    .descriptor,
                component,
                &display,
            )?;
            directories.push(CapturedArchiveDirectory {
                descriptor,
                display_path: display.clone(),
                name_from_parent: Some(component.to_string()),
            });
        }

        let entry_display = self.display_path(relative);
        let leaf = directories
            .last()
            .expect("captured directory chain includes the archive root");
        let mut file = self.open_expected_relative_entry_on(
            &leaf.descriptor,
            &name,
            &entry_display,
            expected,
            limit,
        )?;
        file.sync_all()
            .map_err(|error| ArchiveError::io("archive_file_sync_failed", &entry_display, error))?;
        self.require_expected_open_entry(
            &mut file,
            &leaf.descriptor,
            &name,
            &entry_display,
            expected,
            limit,
        )?;

        // Sync from the file's containing directory back through the captured
        // archive root. Every step is re-opened through its held parent and
        // compared by device/inode after fsync, so syncing a detached or
        // replaced shard can never be mistaken for durable authority.
        for index in (0..directories.len()).rev() {
            let directory = &directories[index];
            let injected_pointer_path = (index + 1 == directories.len()
                && is_invocation_pointer_path(&entry_display))
            .then_some(entry_display.as_path());
            self.sync_captured_directory(
                &directory.descriptor,
                &directory.display_path,
                injected_pointer_path,
            )?;
            self.validate_captured_directory_chain(&directories)?;
            self.require_expected_open_entry(
                &mut file,
                &leaf.descriptor,
                &name,
                &entry_display,
                expected,
                limit,
            )?;
        }
        Ok(())
    }

    #[cfg(unix)]
    fn sync_captured_directory(
        &self,
        directory: &File,
        display: &Path,
        injected_pointer_path: Option<&Path>,
    ) -> Result<(), ArchiveError> {
        for attempt in 1..=POST_LINK_DIRECTORY_SYNC_ATTEMPTS {
            let result = injected_pointer_path
                .and_then(injected_pointer_directory_sync_failure)
                .map_or_else(|| directory.sync_all(), Err);
            match result {
                Ok(()) => {
                    if injected_pointer_path.is_some() {
                        run_pointer_ancestor_chain_post_sync_hook();
                    }
                    return Ok(());
                }
                Err(error)
                    if error.kind() == std::io::ErrorKind::Interrupted
                        && attempt < POST_LINK_DIRECTORY_SYNC_ATTEMPTS =>
                {
                    continue;
                }
                Err(error) => {
                    return Err(ArchiveError::new(
                        "archive_directory_sync_failed",
                        Some(display),
                        format!(
                            "could not durably sync captured archive directory on attempt {attempt}: {error}"
                        ),
                    ));
                }
            }
        }
        unreachable!("the bounded directory-sync loop always returns")
    }

    #[cfg(unix)]
    fn validate_captured_directory_chain(
        &self,
        directories: &[CapturedArchiveDirectory],
    ) -> Result<(), ArchiveError> {
        let root = directories.first().ok_or_else(|| {
            ArchiveError::new(
                "archive_directory_replaced",
                Some(&self.display_path),
                "captured archive directory chain is empty",
            )
        })?;
        let root_metadata = fstat(&root.descriptor).map_err(|error| {
            anchored_errno(
                "archive_directory_inspection_failed",
                &root.display_path,
                error,
                "could not inspect the captured archive root",
            )
        })?;
        if SFlag::from_bits_truncate(root_metadata.st_mode) != SFlag::S_IFDIR
            || root_metadata.st_dev as u64 != self.identity.device
            || root_metadata.st_ino as u64 != self.identity.inode
        {
            return Err(ArchiveError::new(
                "archive_root_replaced",
                Some(&self.display_path),
                "captured durability chain no longer starts at the coordinated archive root inode",
            ));
        }
        validate_archive_root_identity(&self.display_path, self.identity)?;

        for pair in directories.windows(2) {
            let parent = &pair[0];
            let child = &pair[1];
            let name = child.name_from_parent.as_deref().ok_or_else(|| {
                ArchiveError::new(
                    "archive_directory_replaced",
                    Some(&child.display_path),
                    "captured non-root archive directory has no parent-relative name",
                )
            })?;
            let reachable = openat_directory_optional(
                &parent.descriptor,
                name,
                &child.display_path,
            )?
            .ok_or_else(|| {
                ArchiveError::new(
                    "archive_directory_replaced",
                    Some(&child.display_path),
                    "captured archive directory is no longer reachable through its held parent",
                )
            })?;
            let captured_metadata = fstat(&child.descriptor).map_err(|error| {
                anchored_errno(
                    "archive_directory_inspection_failed",
                    &child.display_path,
                    error,
                    "could not inspect captured archive directory",
                )
            })?;
            let reachable_metadata = fstat(&reachable).map_err(|error| {
                anchored_errno(
                    "archive_directory_inspection_failed",
                    &child.display_path,
                    error,
                    "could not inspect reachable archive directory",
                )
            })?;
            if SFlag::from_bits_truncate(captured_metadata.st_mode) != SFlag::S_IFDIR
                || SFlag::from_bits_truncate(reachable_metadata.st_mode) != SFlag::S_IFDIR
                || captured_metadata.st_dev != reachable_metadata.st_dev
                || captured_metadata.st_ino != reachable_metadata.st_ino
            {
                return Err(ArchiveError::new(
                    "archive_directory_replaced",
                    Some(&child.display_path),
                    "captured archive directory was replaced at its canonical descriptor-relative path",
                ));
            }
        }
        Ok(())
    }

    #[cfg(unix)]
    fn sync_visible_relative_entry_directory_on(
        &self,
        relative: &str,
        parent: &File,
        expected: &[u8],
        limit: u64,
    ) -> Result<(), ArchiveError> {
        let mut components = self.relative_components(relative)?;
        let name = components
            .pop()
            .expect("relative component validation requires one component");
        let parent_relative = components.join("/");
        let display = self.display_path(relative);
        let parent_display = display.parent().unwrap_or(&self.display_path);
        let mut linked_file = self
            .open_expected_relative_entry_on(parent, name, &display, expected, limit)
            .map_err(|error| link_visible_validation_failure(&display, error))?;
        // `parent` is the descriptor used by `linkat`. Keep that exact inode
        // across the durability loop: reopening by path could sync a replaced
        // directory. Only EINTR is retryable; any other result leaves a
        // visible link whose crash durability is unknown.
        for attempt in 1..=POST_LINK_DIRECTORY_SYNC_ATTEMPTS {
            let result = injected_pointer_directory_sync_failure(&display)
                .map_or_else(|| parent.sync_all(), Err);
            match result {
                Ok(()) => {
                    run_pointer_post_sync_hook(&display);
                    self.validate_relative_parent_descriptor(
                        &parent_relative,
                        parent,
                        parent_display,
                    )
                    .and_then(|()| {
                        self.require_expected_open_entry(
                            &mut linked_file,
                            parent,
                            name,
                            &display,
                            expected,
                            limit,
                        )
                    })
                    .and_then(|()| {
                        self.validate_relative_parent_descriptor(
                            &parent_relative,
                            parent,
                            parent_display,
                        )
                    })
                    .map_err(|error| link_visible_validation_failure(&display, error))?;
                    return Ok(());
                }
                Err(error)
                    if error.kind() == std::io::ErrorKind::Interrupted
                        && attempt < POST_LINK_DIRECTORY_SYNC_ATTEMPTS =>
                {
                    continue;
                }
                Err(error) => {
                    return Err(ArchiveError::new(
                        LINK_VISIBLE_SYNC_FAILURE_CODE,
                        Some(&display),
                        format!(
                            "linked entry remained visible, but syncing its already-open parent directory failed on attempt {attempt}; crash durability is unknown: {error} (parent: {})",
                            parent_display.display()
                        ),
                    ));
                }
            }
        }
        unreachable!("the bounded directory-sync loop always returns")
    }

    #[cfg(unix)]
    fn open_expected_relative_entry_on(
        &self,
        parent: &File,
        name: &str,
        display: &Path,
        expected: &[u8],
        limit: u64,
    ) -> Result<File, ArchiveError> {
        let descriptor =
            openat(parent, name, regular_file_open_flags(), Mode::empty()).map_err(|error| {
                match error {
                    Errno::ELOOP => ArchiveError::new(
                        "archive_file_invalid",
                        Some(display),
                        "archive file must be a regular file, not a symlink or special file",
                    ),
                    _ => anchored_errno(
                        "archive_file_open_failed",
                        display,
                        error,
                        "could not reopen linked immutable entry",
                    ),
                }
            })?;
        let mut file = File::from(descriptor);
        self.require_expected_open_entry(&mut file, parent, name, display, expected, limit)?;
        Ok(file)
    }

    #[cfg(unix)]
    fn require_expected_open_entry(
        &self,
        file: &mut File,
        parent: &File,
        name: &str,
        display: &Path,
        expected: &[u8],
        limit: u64,
    ) -> Result<(), ArchiveError> {
        let expected_len = u64::try_from(expected.len()).map_err(|_| {
            ArchiveError::new(
                "archive_file_too_large",
                Some(display),
                "expected immutable content length does not fit u64",
            )
        })?;
        if expected_len > limit {
            return Err(ArchiveError::new(
                "archive_file_too_large",
                Some(display),
                format!("expected immutable content exceeds the {limit}-byte read bound"),
            ));
        }
        let metadata = fstat(&*file).map_err(|error| {
            anchored_errno(
                "archive_file_inspection_failed",
                display,
                error,
                "could not inspect linked immutable entry",
            )
        })?;
        if metadata.st_size < 0 || u64::try_from(metadata.st_size).ok() != Some(expected_len) {
            return Err(ArchiveError::new(
                "archive_immutable_conflict",
                Some(display),
                "linked immutable entry length changed before durable acknowledgement",
            ));
        }
        file.seek(SeekFrom::Start(0))
            .map_err(|error| ArchiveError::io("archive_file_read_failed", display, error))?;
        let mut observed = Vec::with_capacity(expected.len());
        Read::by_ref(file)
            .take(limit.saturating_add(1))
            .read_to_end(&mut observed)
            .map_err(|error| ArchiveError::io("archive_file_read_failed", display, error))?;
        if observed != expected {
            return Err(ArchiveError::new(
                "archive_immutable_conflict",
                Some(display),
                "linked immutable entry content changed before durable acknowledgement",
            ));
        }
        validate_anchored_regular_file(parent, name, display, file)
    }

    #[cfg(unix)]
    fn validate_relative_parent_descriptor(
        &self,
        parent_relative: &str,
        linked_parent: &File,
        display: &Path,
    ) -> Result<(), ArchiveError> {
        let reachable_parent = self
            .open_relative_directory_optional(parent_relative)?
            .ok_or_else(|| {
                ArchiveError::new(
                    "archive_directory_replaced",
                    Some(display),
                    "linked entry parent is no longer reachable below the captured archive root",
                )
            })?;
        let linked = fstat(linked_parent).map_err(|error| {
            anchored_errno(
                "archive_directory_inspection_failed",
                display,
                error,
                "could not inspect the parent directory used by linkat",
            )
        })?;
        let reachable = fstat(&reachable_parent).map_err(|error| {
            anchored_errno(
                "archive_directory_inspection_failed",
                display,
                error,
                "could not inspect the canonical linked-entry parent",
            )
        })?;
        if SFlag::from_bits_truncate(linked.st_mode) != SFlag::S_IFDIR
            || SFlag::from_bits_truncate(reachable.st_mode) != SFlag::S_IFDIR
            || linked.st_dev != reachable.st_dev
            || linked.st_ino != reachable.st_ino
        {
            return Err(ArchiveError::new(
                "archive_directory_replaced",
                Some(display),
                "the parent directory synced after linkat is no longer reachable at its canonical archive-relative path",
            ));
        }
        Ok(())
    }

    #[cfg(not(unix))]
    fn sync_relative_file_and_ancestor_chain(
        &self,
        _relative: &str,
        _expected: &[u8],
        _limit: u64,
    ) -> Result<(), ArchiveError> {
        Err(ArchiveError::new(
            "archive_lock_unsupported",
            Some(&self.display_path),
            "descriptor-rooted archive synchronization requires Unix openat semantics",
        ))
    }
}

#[cfg(unix)]
fn validate_anchored_regular_file(
    parent: &File,
    name: &str,
    display: &Path,
    file: &File,
) -> Result<(), ArchiveError> {
    let descriptor = fstat(file).map_err(|error| {
        anchored_errno(
            "archive_file_inspection_failed",
            display,
            error,
            "could not inspect opened archive descriptor",
        )
    })?;
    if SFlag::from_bits_truncate(descriptor.st_mode) != SFlag::S_IFREG {
        return Err(ArchiveError::new(
            "archive_file_invalid",
            Some(display),
            "opened archive descriptor must refer to a regular file",
        ));
    }
    let path = fstatat(parent, name, AtFlags::AT_SYMLINK_NOFOLLOW).map_err(|error| {
        anchored_errno(
            "archive_file_inspection_failed",
            display,
            error,
            "could not reinspect descriptor-relative archive entry",
        )
    })?;
    if SFlag::from_bits_truncate(path.st_mode) != SFlag::S_IFREG {
        return Err(ArchiveError::new(
            "archive_file_invalid",
            Some(display),
            "archive path changed to a symlink or special file while its descriptor was open",
        ));
    }
    if descriptor.st_dev != path.st_dev || descriptor.st_ino != path.st_ino {
        return Err(ArchiveError::new(
            "archive_file_replaced",
            Some(display),
            "archive path was replaced while its descriptor was open",
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn create_anchored_staging_file(
    parent: &File,
    display_parent: &Path,
    bytes: &[u8],
) -> Result<(String, File), ArchiveError> {
    for _ in 0..128 {
        let sequence = STAGING_COUNTER.fetch_add(1, Ordering::Relaxed);
        let name = format!(".staging-{}-{sequence:016x}", std::process::id());
        let display = display_parent.join(&name);
        let descriptor = match openat(
            parent,
            name.as_str(),
            OFlag::O_WRONLY
                | OFlag::O_CLOEXEC
                | OFlag::O_CREAT
                | OFlag::O_EXCL
                | OFlag::O_NOFOLLOW
                | OFlag::O_NONBLOCK,
            Mode::from_bits_truncate(0o666),
        ) {
            Ok(descriptor) => descriptor,
            Err(Errno::EEXIST) => continue,
            Err(error) => {
                return Err(anchored_errno(
                    "archive_staging_create_failed",
                    &display,
                    error,
                    "could not create descriptor-relative staging file",
                ));
            }
        };
        let mut file = File::from(descriptor);
        let result = (|| {
            file.write_all(bytes).map_err(|error| {
                ArchiveError::io("archive_staging_write_failed", &display, error)
            })?;
            file.sync_all().map_err(|error| {
                ArchiveError::io("archive_staging_sync_failed", &display, error)
            })?;
            let metadata = fstat(&file).map_err(|error| {
                anchored_errno(
                    "archive_staging_inspection_failed",
                    &display,
                    error,
                    "could not inspect descriptor-relative staging file",
                )
            })?;
            let readonly_mode = Mode::from_bits_truncate(metadata.st_mode & !0o222);
            fchmod(&file, readonly_mode).map_err(|error| {
                anchored_errno(
                    "archive_staging_permissions_failed",
                    &display,
                    error,
                    "could not make descriptor-relative staging file read-only",
                )
            })?;
            file.sync_all().map_err(|error| {
                ArchiveError::io("archive_staging_sync_failed", &display, error)
            })?;
            validate_anchored_regular_file(parent, &name, &display, &file)?;
            Ok((name.clone(), file))
        })();
        if result.is_err() {
            let _ = unlinkat(parent, name.as_str(), UnlinkatFlags::NoRemoveDir);
        }
        return result;
    }
    Err(ArchiveError::new(
        "archive_staging_create_failed",
        Some(display_parent),
        "could not reserve a unique bounded descriptor-relative staging name",
    ))
}

/// Identity needed to reconcile a publication whose pointer link became
/// visible but whose directory durability could not be proved.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchivePublicationUnknownV1 {
    pub archive_format_version: u32,
    pub invocation_id: String,
    pub record_sha256: String,
    pub object_relative_path: String,
    pub pointer_relative_path: String,
}

impl ArchivePublicationUnknownV1 {
    /// Construct a canonical reconciliation candidate from its public identity.
    ///
    /// Object and pointer paths are derived rather than accepted from a CLI or
    /// another untrusted caller, so internal archive layout cannot be confused
    /// with user-supplied path data.
    pub fn new(
        invocation_id: impl Into<String>,
        record_sha256: impl Into<String>,
    ) -> Result<Self, ArchiveError> {
        let invocation_id = invocation_id.into();
        let record_sha256 = record_sha256.into();
        require_sha256(&record_sha256, "record digest")?;
        let pointer_relative_path = pointer_relative_path(&invocation_id)?;
        let object_relative_path = object_relative_path(&record_sha256);
        Ok(Self {
            archive_format_version: ARCHIVE_FORMAT_VERSION,
            invocation_id,
            record_sha256,
            object_relative_path,
            pointer_relative_path,
        })
    }
}

/// Candidate-specific result from closing a publication durability window.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "kebab-case", deny_unknown_fields)]
pub enum ArchiveReconciliationV1 {
    /// The exact pointer and record were validated and durably synced.
    Durable { receipt: ArchiveReceiptV1 },
    /// No pointer for this invocation existed at the coordinated boundary.
    Absent {
        candidate: ArchivePublicationUnknownV1,
    },
    /// The invocation is durably claimed by a different valid record.
    Conflict {
        candidate: ArchivePublicationUnknownV1,
        published: ArchiveReceiptV1,
    },
}

/// Stable, machine-readable archive failure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ArchiveError {
    pub code: &'static str,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_path_lossy"
    )]
    pub path: Option<PathBuf>,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub possibly_published: Option<Box<ArchivePublicationUnknownV1>>,
}

impl ArchiveError {
    fn new(code: &'static str, path: Option<&Path>, message: impl Into<String>) -> Self {
        Self {
            code,
            path: path.map(Path::to_path_buf),
            message: message.into(),
            possibly_published: None,
        }
    }

    fn io(code: &'static str, path: &Path, error: std::io::Error) -> Self {
        Self::new(code, Some(path), error.to_string())
    }

    fn pointer_publication_unknown(
        pointer_path: &Path,
        identity: ArchivePublicationUnknownV1,
        cause: Self,
    ) -> Self {
        Self {
            code: "archive_pointer_publication_unknown",
            path: Some(pointer_path.to_path_buf()),
            message: format!(
                "invocation pointer is visible, but durable publication could not be proved after bounded recovery: {}",
                cause.message
            ),
            possibly_published: Some(Box::new(identity)),
        }
    }
}

fn serialize_optional_path_lossy<S>(
    path: &Option<PathBuf>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match path {
        Some(path) => serializer.serialize_some(path.to_string_lossy().as_ref()),
        None => serializer.serialize_none(),
    }
}

impl Display for ArchiveError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(path) = &self.path {
            write!(
                formatter,
                "{} at {}: {}",
                self.code,
                path.display(),
                self.message
            )
        } else {
            write!(formatter, "{}: {}", self.code, self.message)
        }
    }
}

impl Error for ArchiveError {}

/// Prepare an archive root for crash-durable publication.
///
/// Missing root components are created one at a time and made durable by
/// syncing each new directory and its parent. The preflight then durably
/// creates the versioned archive layout and exercises the exact create/write/
/// file-fsync/hard-link/directory-fsync/delete mechanism used by publication
/// in both authority trees. A crash during the probe can leave only empty
/// layout directories or `.staging-*` files, neither of which is archive
/// authority; readers ignore the staging files.
pub fn preflight_archive_publication(archive_root: &Path) -> Result<(), ArchiveError> {
    ensure_durable_directory_tree(archive_root)?;
    let archive_root = canonical_archive_root(archive_root)?;
    let lock = acquire_archive_lock(
        &archive_root,
        ArchiveLockMode::Exclusive,
        ARCHIVE_LOCK_TIMEOUT,
    )?;
    lock.validate_root(&archive_root)?;
    ensure_archive_layout(&archive_root)?;
    validate_archive_directory_chain(&archive_root, &archive_root.join(OBJECT_DIRECTORY))?;
    validate_archive_directory_chain(&archive_root, &archive_root.join(INVOCATION_DIRECTORY))?;
    let object_probe = archive_root
        .join(OBJECT_DIRECTORY)
        .join(SHA256_DIRECTORY)
        .join("00");
    let invocation_probe = archive_root.join(INVOCATION_DIRECTORY).join("00");
    ensure_real_directory(&object_probe)?;
    ensure_real_directory(&invocation_probe)?;
    validate_archive_directory_chain(&archive_root, &object_probe)?;
    validate_archive_directory_chain(&archive_root, &invocation_probe)?;
    run_publication_probe(&archive_root, &object_probe)?;
    lock.validate_root(&archive_root)?;
    run_publication_probe(&archive_root, &invocation_probe)?;
    lock.validate_root(&archive_root)?;
    Ok(())
}

/// Validate and atomically publish one complete run record.
///
/// Re-publishing byte-identical content for the same invocation is
/// idempotent. Reusing an invocation id for different bytes fails closed.
pub fn publish_record(
    archive_root: &Path,
    record: &RunRecordV1,
) -> Result<ArchiveReceiptV1, ArchiveError> {
    let bytes = canonical_record_bytes(record).map_err(|error| {
        ArchiveError::new(
            "archive_record_invalid",
            None,
            format!("record could not be canonicalized: {error}"),
        )
    })?;
    let byte_len = u64::try_from(bytes.len()).map_err(|_| {
        ArchiveError::new(
            "archive_record_too_large",
            None,
            "record byte length does not fit u64",
        )
    })?;
    if byte_len > MAX_ARCHIVE_RECORD_BYTES {
        return Err(ArchiveError::new(
            "archive_record_too_large",
            None,
            format!("record is {byte_len} bytes; maximum is {MAX_ARCHIVE_RECORD_BYTES}"),
        ));
    }
    let record_sha256 = sha256_bytes(&bytes);
    require_sha256(&record_sha256, "record digest")?;
    let invocation_id = &record.invocation.invocation_id;
    let object_relative_path = object_relative_path(&record_sha256);
    let pointer_relative_path = pointer_relative_path(invocation_id)?;

    // Direct publication is a complete durable entry point: callers do not
    // have to remember to run the optional capability probe first. Persisting
    // every missing root component also makes the archive root's entry durable
    // in its parent before it becomes the inode used for coordination.
    ensure_durable_directory_tree(archive_root)?;
    let archive_root = canonical_archive_root(archive_root)?;
    let publication_lock = acquire_archive_lock(
        &archive_root,
        ArchiveLockMode::Exclusive,
        ARCHIVE_LOCK_TIMEOUT,
    )?;
    publication_lock.validate_root(&archive_root)?;
    let anchored_root = publication_lock.anchored_root(&archive_root)?;
    anchored_root.ensure_archive_layout()?;
    publication_lock.validate_root(&archive_root)?;
    #[cfg(test)]
    run_publication_root_swap_hook(true);
    let object_result =
        anchored_root.install_immutable(&object_relative_path, &bytes, MAX_ARCHIVE_RECORD_BYTES);
    #[cfg(test)]
    run_publication_root_swap_hook(false);
    let _object_new = object_result?;
    anchored_root.sync_relative_file_and_ancestor_chain(
        &object_relative_path,
        &bytes,
        MAX_ARCHIVE_RECORD_BYTES,
    )?;
    publication_lock.validate_root(&archive_root)?;

    let pointer = InvocationPointerV1 {
        archive_format_version: ARCHIVE_FORMAT_VERSION,
        invocation_id: invocation_id.clone(),
        record_sha256: record_sha256.clone(),
        object_relative_path: object_relative_path.clone(),
    };
    let pointer_bytes = serde_json::to_vec(&pointer).map_err(|error| {
        ArchiveError::new(
            "archive_pointer_serialization_failed",
            None,
            error.to_string(),
        )
    })?;
    let pointer_path = anchored_root.display_path(&pointer_relative_path);
    // The immutable pointer is the sole publication boundary. Inventory
    // readers hold the shared form of this same cross-process lock while they
    // enumerate pointers, so each captured inventory is either wholly before
    // or wholly after this publication.
    let pointer_new = match anchored_root.install_immutable(
        &pointer_relative_path,
        &pointer_bytes,
        MAX_POINTER_BYTES,
    ) {
        Ok(created) => created,
        Err(error) if error.code == LINK_VISIBLE_SYNC_FAILURE_CODE => {
            let candidate =
                ArchivePublicationUnknownV1::new(invocation_id.clone(), record_sha256.clone())?;
            return Err(ArchiveError::pointer_publication_unknown(
                &pointer_path,
                candidate,
                error,
            ));
        }
        Err(error) => {
            // The object is content addressed and harmless if this invocation
            // was already claimed by different content.
            return Err(error);
        }
    };
    if let Err(error) = anchored_root.sync_relative_file_and_ancestor_chain(
        &pointer_relative_path,
        &pointer_bytes,
        MAX_POINTER_BYTES,
    ) {
        let candidate =
            ArchivePublicationUnknownV1::new(invocation_id.clone(), record_sha256.clone())?;
        return Err(ArchiveError::pointer_publication_unknown(
            &pointer_path,
            candidate,
            error,
        ));
    }
    if let Err(error) = publication_lock.validate_root(&archive_root) {
        let candidate =
            ArchivePublicationUnknownV1::new(invocation_id.clone(), record_sha256.clone())?;
        return Err(ArchiveError::pointer_publication_unknown(
            &pointer_path,
            candidate,
            error,
        ));
    }

    Ok(ArchiveReceiptV1 {
        archive_format_version: ARCHIVE_FORMAT_VERSION,
        invocation_id: invocation_id.clone(),
        record_sha256,
        object_relative_path,
        pointer_relative_path,
        newly_published: pointer_new,
    })
}

/// Resolve one publication whose pointer durability was previously unknown.
///
/// Reconciliation is serialized with publication. It never infers success
/// from archive-wide validity: the exact invocation pointer and content digest
/// are matched, the referenced canonical record is fully validated, and the
/// visible immutable files and their containing directories are synced before
/// a durable or conflict outcome is returned.
pub fn reconcile_archive_publication(
    archive_root: &Path,
    candidate: &ArchivePublicationUnknownV1,
) -> Result<ArchiveReconciliationV1, ArchiveError> {
    let invocation_id = validate_reconciliation_candidate(candidate)?;
    let archive_root = canonical_archive_root(archive_root)?;
    let publication_lock = acquire_archive_lock(
        &archive_root,
        ArchiveLockMode::Exclusive,
        ARCHIVE_LOCK_TIMEOUT,
    )?;
    publication_lock.validate_root(&archive_root)?;
    let anchored_root = publication_lock.anchored_root(&archive_root)?;

    let pointer_path = anchored_root.display_path(&candidate.pointer_relative_path);
    if !anchored_root.relative_path_exists(&candidate.pointer_relative_path)? {
        publication_lock.validate_root(&archive_root)?;
        return Ok(ArchiveReconciliationV1::Absent {
            candidate: candidate.clone(),
        });
    }

    let loaded = load_invocation(&anchored_root, invocation_id);
    if let Err(error) = publication_lock.validate_root(&archive_root) {
        return Err(ArchiveError::pointer_publication_unknown(
            &pointer_path,
            candidate.clone(),
            error,
        ));
    }
    let (archived, _serialized_bytes) = loaded?;
    let durability = make_archived_record_durable(&anchored_root, &archived);
    if let Err(error) = publication_lock.validate_root(&archive_root) {
        let observed = ArchivePublicationUnknownV1::new(
            archived.receipt.invocation_id.clone(),
            archived.receipt.record_sha256.clone(),
        )?;
        return Err(ArchiveError::pointer_publication_unknown(
            &pointer_path,
            observed,
            error,
        ));
    }
    durability?;

    if archived.receipt.record_sha256 != candidate.record_sha256 {
        return Ok(ArchiveReconciliationV1::Conflict {
            candidate: candidate.clone(),
            published: archived.receipt,
        });
    }
    if archived.receipt.object_relative_path != candidate.object_relative_path
        || archived.receipt.pointer_relative_path != candidate.pointer_relative_path
        || archived.receipt.invocation_id != candidate.invocation_id
    {
        return Err(ArchiveError::new(
            "archive_reconciliation_mismatch",
            Some(&pointer_path),
            "validated archive receipt does not exactly match the canonical reconciliation candidate",
        ));
    }
    Ok(ArchiveReconciliationV1::Durable {
        receipt: archived.receipt,
    })
}

fn validate_reconciliation_candidate(
    candidate: &ArchivePublicationUnknownV1,
) -> Result<InvocationId, ArchiveError> {
    if candidate.archive_format_version != ARCHIVE_FORMAT_VERSION {
        return Err(ArchiveError::new(
            "archive_reconciliation_candidate_invalid",
            None,
            format!(
                "candidate archive format {} is unsupported; expected {ARCHIVE_FORMAT_VERSION}",
                candidate.archive_format_version
            ),
        ));
    }
    let canonical = ArchivePublicationUnknownV1::new(
        candidate.invocation_id.clone(),
        candidate.record_sha256.clone(),
    )?;
    if candidate != &canonical {
        return Err(ArchiveError::new(
            "archive_reconciliation_candidate_invalid",
            None,
            "candidate object and pointer paths must equal the canonical paths derived from invocation_id and record_sha256",
        ));
    }
    InvocationId::parse(&candidate.invocation_id)
}

fn make_archived_record_durable(
    archive_root: &AnchoredArchiveRoot,
    archived: &ArchivedRecord,
) -> Result<(), ArchiveError> {
    let receipt = &archived.receipt;
    let record_bytes = canonical_record_bytes(&archived.record).map_err(|error| {
        ArchiveError::new(
            "archive_record_invalid",
            Some(&archive_root.display_path(&receipt.object_relative_path)),
            format!("validated record could not be canonicalized for durability closure: {error}"),
        )
    })?;
    let observed_record_sha256 = sha256_bytes(&record_bytes);
    if observed_record_sha256 != receipt.record_sha256 {
        return Err(ArchiveError::new(
            "archive_record_digest_mismatch",
            Some(&archive_root.display_path(&receipt.object_relative_path)),
            format!(
                "receipt expects {}, canonical record hashes to {observed_record_sha256}",
                receipt.record_sha256
            ),
        ));
    }
    let pointer_bytes = serde_json::to_vec(&InvocationPointerV1 {
        archive_format_version: ARCHIVE_FORMAT_VERSION,
        invocation_id: receipt.invocation_id.clone(),
        record_sha256: receipt.record_sha256.clone(),
        object_relative_path: receipt.object_relative_path.clone(),
    })
    .map_err(|error| {
        ArchiveError::new(
            "archive_pointer_serialization_failed",
            None,
            error.to_string(),
        )
    })?;
    let pointer_path = archive_root.display_path(&receipt.pointer_relative_path);
    let observed = ArchivePublicationUnknownV1::new(
        receipt.invocation_id.clone(),
        receipt.record_sha256.clone(),
    )?;
    let durability = (|| {
        archive_root.sync_relative_file_and_ancestor_chain(
            &receipt.object_relative_path,
            &record_bytes,
            MAX_ARCHIVE_RECORD_BYTES,
        )?;
        archive_root.sync_relative_file_and_ancestor_chain(
            &receipt.pointer_relative_path,
            &pointer_bytes,
            MAX_POINTER_BYTES,
        )
    })();
    durability
        .map_err(|error| ArchiveError::pointer_publication_unknown(&pointer_path, observed, error))
}

/// Open the archive's invocation inventory without materializing its records.
///
/// Inventory memory is bounded by the record-count ceiling times a fixed
/// 26-byte id. Records are yielded in invocation-id order.
pub fn iter_archive(archive_root: &Path) -> Result<ArchiveRecordIter, ArchiveError> {
    ArchiveRecordIter::open(archive_root)
}

impl ArchiveRecordIter {
    /// Open a streaming archive iterator.
    pub fn open(archive_root: &Path) -> Result<Self, ArchiveError> {
        let archive_root = canonical_archive_root(archive_root)?;
        // Only pointer discovery needs the lock. Once captured, every pointer
        // is immutable, so each record can be loaded and durability-closed
        // exactly once on yield without holding up future publishers.
        let inventory_lock =
            acquire_archive_lock(&archive_root, ArchiveLockMode::Shared, ARCHIVE_LOCK_TIMEOUT)?;
        inventory_lock.validate_root(&archive_root)?;
        let anchored_root = inventory_lock.anchored_root(&archive_root)?;
        let archive_root_identity = anchored_root.identity;
        let invocation_ids = invocation_inventory(&anchored_root);
        inventory_lock.validate_root(&archive_root)?;
        let invocation_ids = invocation_ids?;
        Ok(Self {
            archive_root,
            archive_root_identity,
            anchored_root,
            invocation_ids,
            next_index: 0,
            failed: false,
        })
    }

    /// Number of records not yet visited.
    pub fn remaining(&self) -> usize {
        self.invocation_ids.len().saturating_sub(self.next_index)
    }

    fn next_with_size(&mut self) -> Option<Result<(ArchivedRecord, u64), ArchiveError>> {
        if self.failed || self.next_index >= self.invocation_ids.len() {
            return None;
        }
        let invocation_id = self.invocation_ids[self.next_index];
        self.next_index += 1;
        let loaded = validate_archive_root_identity(&self.archive_root, self.archive_root_identity)
            .and_then(|()| load_invocation(&self.anchored_root, invocation_id))
            .and_then(|(archived, serialized_bytes)| {
                make_archived_record_durable(&self.anchored_root, &archived)
                    .map(|()| (archived, serialized_bytes))
            });
        let loaded =
            match validate_archive_root_identity(&self.archive_root, self.archive_root_identity) {
                Err(root_error) => Err(root_error),
                Ok(()) => loaded,
            };
        if loaded.is_err() {
            self.failed = true;
            self.next_index = self.invocation_ids.len();
        }
        Some(loaded)
    }
}

impl Iterator for ArchiveRecordIter {
    type Item = Result<ArchivedRecord, ArchiveError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_with_size()
            .map(|result| result.map(|(record, _serialized_bytes)| record))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.remaining();
        (0, Some(remaining))
    }
}

/// Materialize a small archive as a compatibility convenience.
///
/// Larger archives fail with `archive_materialization_limit_exceeded` and must
/// be processed through [`iter_archive`]. Results are ordered by invocation id.
pub fn load_archive(archive_root: &Path) -> Result<Vec<ArchivedRecord>, ArchiveError> {
    let mut iterator = iter_archive(archive_root)?;
    let mut materialized_bytes = 0_u64;
    let mut records = Vec::with_capacity(iterator.remaining().min(4_096));
    while let Some(result) = iterator.next_with_size() {
        let (record, serialized_bytes) = result?;
        materialized_bytes = checked_materialized_bytes(materialized_bytes, serialized_bytes)?;
        records.push(record);
    }
    Ok(records)
}

#[cfg(not(unix))]
fn invocation_inventory(
    archive_root: &AnchoredArchiveRoot,
) -> Result<Vec<InvocationId>, ArchiveError> {
    let _ = archive_root;
    Err(ArchiveError::new(
        "archive_lock_unsupported",
        None,
        "descriptor-rooted archive inventory requires Unix openat semantics",
    ))
}

#[cfg(unix)]
fn invocation_inventory(
    archive_root: &AnchoredArchiveRoot,
) -> Result<Vec<InvocationId>, ArchiveError> {
    let Some(invocation_root) =
        archive_root.open_relative_directory_optional(INVOCATION_DIRECTORY)?
    else {
        return Ok(Vec::new());
    };
    let invocation_root_path = archive_root.display_path(INVOCATION_DIRECTORY);
    let mut entries_seen = 0usize;
    let mut shard_count = 0usize;
    let mut invocation_ids = Vec::new();
    for shard_name in anchored_directory_stream(&invocation_root, &invocation_root_path)? {
        let shard_name = shard_name?;
        if matches!(shard_name.as_str(), "." | "..") {
            continue;
        }
        entries_seen = checked_entry_budget(entries_seen)?;
        let shard_path = invocation_root_path.join(&shard_name);
        if !valid_id_shard(&shard_name) {
            return Err(ArchiveError::new(
                "archive_layout_invalid",
                Some(&shard_path),
                "invocation shard must be exactly two uppercase Crockford characters",
            ));
        }
        shard_count = shard_count.checked_add(1).ok_or_else(|| {
            ArchiveError::new("archive_budget_exceeded", None, "shard count overflowed")
        })?;
        if shard_count > MAX_ARCHIVE_SHARDS {
            return Err(ArchiveError::new(
                "archive_budget_exceeded",
                Some(&invocation_root_path),
                format!("archive has more than {MAX_ARCHIVE_SHARDS} invocation shards"),
            ));
        }
        let shard_directory = openat_directory(&invocation_root, &shard_name, &shard_path)?;
        for name in anchored_directory_stream(&shard_directory, &shard_path)? {
            let name = name?;
            if matches!(name.as_str(), "." | "..") {
                continue;
            }
            entries_seen = checked_entry_budget(entries_seen)?;
            let pointer_path = shard_path.join(&name);
            let file_type = anchored_entry_type(&shard_directory, &name, &pointer_path)?;
            if name.starts_with(".staging-") && file_type == SFlag::S_IFREG {
                continue;
            }
            if file_type != SFlag::S_IFREG {
                return Err(ArchiveError::new(
                    "archive_layout_invalid",
                    Some(&pointer_path),
                    "shards may contain only regular pointer-v1 JSON files",
                ));
            }
            let invocation_id = invocation_id_from_pointer_name(&name).ok_or_else(|| {
                ArchiveError::new(
                    "archive_layout_invalid",
                    Some(&pointer_path),
                    "pointer filename must contain one canonical 26-character invocation id",
                )
            })?;
            if invocation_id.shard() != shard_name {
                return Err(ArchiveError::new(
                    "archive_pointer_path_mismatch",
                    Some(&pointer_path),
                    format!(
                        "invocation {} belongs in shard {}",
                        invocation_id.as_str(),
                        invocation_id.shard()
                    ),
                ));
            }
            invocation_ids.push(invocation_id);
            if invocation_ids.len() > MAX_ARCHIVE_POINTERS {
                return Err(ArchiveError::new(
                    "archive_budget_exceeded",
                    Some(&invocation_root_path),
                    format!("archive has more than {MAX_ARCHIVE_POINTERS} records"),
                ));
            }
        }
    }
    invocation_ids.sort_unstable();
    if invocation_ids.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(ArchiveError::new(
            "archive_duplicate_invocation",
            Some(&archive_root.display_path(INVOCATION_DIRECTORY)),
            "one invocation id is claimed by more than one pointer",
        ));
    }
    Ok(invocation_ids)
}

fn load_invocation(
    archive_root: &AnchoredArchiveRoot,
    invocation_id: InvocationId,
) -> Result<(ArchivedRecord, u64), ArchiveError> {
    let pointer_relative_path = pointer_relative_path(invocation_id.as_str())?;
    let pointer_path = archive_root.display_path(&pointer_relative_path);
    let pointer_bytes = archive_root
        .read_bounded_relative_regular_file(&pointer_relative_path, MAX_POINTER_BYTES)?;
    let pointer: InvocationPointerV1 = serde_json::from_slice(&pointer_bytes).map_err(|error| {
        ArchiveError::new(
            "archive_pointer_invalid",
            Some(&pointer_path),
            error.to_string(),
        )
    })?;
    let canonical_pointer = serde_json::to_vec(&pointer).map_err(|error| {
        ArchiveError::new(
            "archive_pointer_invalid",
            Some(&pointer_path),
            error.to_string(),
        )
    })?;
    if canonical_pointer != pointer_bytes {
        return Err(ArchiveError::new(
            "archive_pointer_non_canonical",
            Some(&pointer_path),
            "pointer bytes are not canonical compact pointer-v1 JSON",
        ));
    }
    validate_pointer(&pointer_path, &pointer_relative_path, &pointer)?;

    let object_relative_path = object_relative_path(&pointer.record_sha256);
    let object_path = archive_root.display_path(&object_relative_path);
    let record_bytes = archive_root
        .read_bounded_relative_regular_file(&object_relative_path, MAX_ARCHIVE_RECORD_BYTES)?;
    let serialized_bytes = u64::try_from(record_bytes.len()).map_err(|_| {
        ArchiveError::new(
            "archive_record_too_large",
            Some(&object_path),
            "record length does not fit u64",
        )
    })?;
    let record = parse_canonical_record(&record_bytes).map_err(|error| {
        ArchiveError::new(
            "archive_record_invalid",
            Some(&object_path),
            error.to_string(),
        )
    })?;
    // `parse_canonical_record` accepted this exact byte slice. Hash it
    // directly instead of serializing the typed record a second time.
    let observed_sha256 = sha256_bytes(&record_bytes);
    if observed_sha256 != pointer.record_sha256 {
        return Err(ArchiveError::new(
            "archive_record_digest_mismatch",
            Some(&object_path),
            format!(
                "pointer expects {}, canonical record hashes to {observed_sha256}",
                pointer.record_sha256
            ),
        ));
    }
    if record.invocation.invocation_id != pointer.invocation_id {
        return Err(ArchiveError::new(
            "archive_invocation_mismatch",
            Some(&object_path),
            format!(
                "pointer claims invocation {}, record contains {}",
                pointer.invocation_id, record.invocation.invocation_id
            ),
        ));
    }
    Ok((
        ArchivedRecord {
            record,
            receipt: ArchiveReceiptV1 {
                archive_format_version: ARCHIVE_FORMAT_VERSION,
                invocation_id: pointer.invocation_id,
                record_sha256: pointer.record_sha256,
                object_relative_path: pointer.object_relative_path,
                pointer_relative_path,
                newly_published: false,
            },
        },
        serialized_bytes,
    ))
}

fn validate_pointer(
    pointer_path: &Path,
    actual_pointer_relative_path: &str,
    pointer: &InvocationPointerV1,
) -> Result<(), ArchiveError> {
    if pointer.archive_format_version != ARCHIVE_FORMAT_VERSION {
        return Err(ArchiveError::new(
            "archive_version_unsupported",
            Some(pointer_path),
            format!(
                "pointer version {} is unsupported; expected {ARCHIVE_FORMAT_VERSION}",
                pointer.archive_format_version
            ),
        ));
    }
    require_sha256(&pointer.record_sha256, "pointer record digest")?;
    let expected_pointer = pointer_relative_path(&pointer.invocation_id)?;
    if actual_pointer_relative_path != expected_pointer {
        return Err(ArchiveError::new(
            "archive_pointer_path_mismatch",
            Some(pointer_path),
            format!("expected pointer path {expected_pointer}"),
        ));
    }
    let expected_object = object_relative_path(&pointer.record_sha256);
    if pointer.object_relative_path != expected_object {
        return Err(ArchiveError::new(
            "archive_object_path_mismatch",
            Some(pointer_path),
            format!("expected object path {expected_object}"),
        ));
    }
    Ok(())
}

fn checked_materialized_bytes(total: u64, next: u64) -> Result<u64, ArchiveError> {
    let combined = total.checked_add(next).ok_or_else(|| {
        ArchiveError::new(
            "archive_materialization_limit_exceeded",
            None,
            "materialized archive byte count overflowed u64",
        )
    })?;
    if combined > MAX_MATERIALIZED_ARCHIVE_BYTES {
        return Err(ArchiveError::new(
            "archive_materialization_limit_exceeded",
            None,
            format!(
                "materializing this archive would exceed {MAX_MATERIALIZED_ARCHIVE_BYTES} serialized bytes; use iter_archive"
            ),
        ));
    }
    Ok(combined)
}

fn ensure_archive_layout(root: &Path) -> Result<(), ArchiveError> {
    require_existing_directory(root, "archive_root_invalid")?;
    ensure_real_directory(&root.join(OBJECT_DIRECTORY))?;
    ensure_real_directory(&root.join(OBJECT_DIRECTORY).join(SHA256_DIRECTORY))?;
    ensure_real_directory(&root.join(INVOCATION_DIRECTORY))?;
    Ok(())
}

fn canonical_archive_root(path: &Path) -> Result<PathBuf, ArchiveError> {
    require_existing_directory(path, "archive_root_invalid")?;
    let canonical = fs::canonicalize(path)
        .map_err(|error| ArchiveError::io("archive_root_invalid", path, error))?;
    require_existing_directory(path, "archive_root_invalid")?;
    require_existing_directory(&canonical, "archive_root_invalid")?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        let requested_metadata = fs::symlink_metadata(path)
            .map_err(|error| ArchiveError::io("archive_root_invalid", path, error))?;
        let canonical_metadata = fs::symlink_metadata(&canonical)
            .map_err(|error| ArchiveError::io("archive_root_invalid", &canonical, error))?;
        if requested_metadata.dev() != canonical_metadata.dev()
            || requested_metadata.ino() != canonical_metadata.ino()
        {
            return Err(ArchiveError::new(
                "archive_root_invalid",
                Some(path),
                "archive root changed while resolving its canonical directory",
            ));
        }
    }

    Ok(canonical)
}

#[cfg(test)]
fn install_immutable(
    archive_root: &Path,
    path: &Path,
    bytes: &[u8],
    limit: u64,
) -> Result<bool, ArchiveError> {
    let parent = path.parent().ok_or_else(|| {
        ArchiveError::new(
            "archive_path_invalid",
            Some(path),
            "archive object has no parent directory",
        )
    })?;
    ensure_real_directory(parent)?;
    validate_archive_directory_chain(archive_root, parent)?;
    match fs::symlink_metadata(path) {
        Ok(_) => {
            // Another publisher may have linked this entry but not yet synced
            // the directory. Idempotent acknowledgement must close that crash
            // window itself rather than depend on the winning process.
            return require_identical_file(archive_root, path, bytes, limit)
                .and_then(|()| sync_regular_file(archive_root, path))
                .and_then(|()| sync_visible_entry_directory(archive_root, path, parent))
                .map(|()| false)
                .map_err(|error| link_visible_failure(path, error));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(ArchiveError::io(
                "archive_file_inspection_failed",
                path,
                error,
            ));
        }
    }

    let staging = create_staging_file(parent, bytes)?;
    let link_result = fs::hard_link(&staging, path);
    let result = match link_result {
        Ok(()) => validate_archive_directory_chain(archive_root, parent)
            .and_then(|()| require_identical_file(archive_root, path, bytes, limit))
            .and_then(|()| sync_regular_file(archive_root, path))
            .and_then(|()| sync_visible_entry_directory(archive_root, path, parent))
            .map(|()| true)
            .map_err(|error| link_visible_failure(path, error)),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            require_identical_file(archive_root, path, bytes, limit)
                .and_then(|()| sync_regular_file(archive_root, path))
                .and_then(|()| sync_visible_entry_directory(archive_root, path, parent))
                .map(|()| false)
                .map_err(|error| link_visible_failure(path, error))
        }
        Err(error) => Err(ArchiveError::io("archive_publish_failed", path, error)),
    };
    // A staging file is never authoritative. Cleanup failure cannot make a
    // successfully linked immutable object ambiguous.
    let _ = fs::remove_file(staging);
    result
}

fn link_visible_failure(path: &Path, cause: ArchiveError) -> ArchiveError {
    if matches!(
        cause.code,
        LINK_VISIBLE_SYNC_FAILURE_CODE | "archive_immutable_conflict"
    ) {
        cause
    } else {
        ArchiveError::new(
            LINK_VISIBLE_SYNC_FAILURE_CODE,
            Some(path),
            format!(
                "linked entry became visible, but post-link validation or durability failed: {cause}"
            ),
        )
    }
}

fn link_visible_validation_failure(path: &Path, cause: ArchiveError) -> ArchiveError {
    if cause.code == LINK_VISIBLE_SYNC_FAILURE_CODE {
        cause
    } else {
        ArchiveError::new(
            LINK_VISIBLE_SYNC_FAILURE_CODE,
            Some(path),
            format!(
                "linked entry became visible, but its post-sync identity could not be revalidated: {cause}"
            ),
        )
    }
}

#[cfg(test)]
fn sync_visible_entry_directory(
    archive_root: &Path,
    path: &Path,
    parent: &Path,
) -> Result<(), ArchiveError> {
    validate_archive_directory_chain(archive_root, parent)?;
    let directory = open_directory_no_follow(parent)?;
    for attempt in 1..=POST_LINK_DIRECTORY_SYNC_ATTEMPTS {
        match directory.sync_all() {
            Ok(()) => {
                return validate_open_directory_target(parent, &directory)
                    .and_then(|()| validate_archive_directory_chain(archive_root, parent))
                    .map_err(|error| link_visible_failure(path, error));
            }
            Err(error)
                if error.kind() == std::io::ErrorKind::Interrupted
                    && attempt < POST_LINK_DIRECTORY_SYNC_ATTEMPTS =>
            {
                continue;
            }
            Err(error) => {
                return Err(ArchiveError::new(
                    LINK_VISIBLE_SYNC_FAILURE_CODE,
                    Some(path),
                    format!(
                        "linked entry remained visible, but syncing its already-open parent directory failed on attempt {attempt}; crash durability is unknown: {error} (parent: {})",
                        parent.display()
                    ),
                ));
            }
        }
    }
    unreachable!("the bounded directory-sync loop always returns")
}

#[cfg(test)]
fn inject_pointer_directory_sync_failures(failures: impl IntoIterator<Item = i32>) {
    INJECT_POINTER_DIRECTORY_SYNC_FAILURES.with(|injected| {
        *injected.borrow_mut() = failures.into_iter().collect();
    });
}

#[cfg(test)]
fn inject_pointer_post_sync_hook(hook: impl FnOnce() + 'static) {
    INJECT_POINTER_POST_SYNC_HOOK.with(|injected| {
        *injected.borrow_mut() = Some(Box::new(hook));
    });
}

#[cfg(test)]
fn inject_pointer_ancestor_chain_post_sync_hook(hook: impl FnOnce() + 'static) {
    INJECT_POINTER_ANCESTOR_CHAIN_POST_SYNC_HOOK.with(|injected| {
        *injected.borrow_mut() = Some(Box::new(hook));
    });
}

#[cfg(test)]
fn inject_immutable_parent_post_ensure_hook(hook: impl FnOnce() + 'static) {
    INJECT_IMMUTABLE_PARENT_POST_ENSURE_HOOK.with(|injected| {
        *injected.borrow_mut() = Some(Box::new(hook));
    });
}

fn run_immutable_parent_post_ensure_hook() {
    #[cfg(test)]
    INJECT_IMMUTABLE_PARENT_POST_ENSURE_HOOK.with(|injected| {
        if let Some(hook) = injected.borrow_mut().take() {
            hook();
        }
    });
}

fn run_pointer_post_sync_hook(path: &Path) {
    #[cfg(test)]
    {
        if path
            .components()
            .any(|component| component.as_os_str() == std::ffi::OsStr::new(INVOCATION_DIRECTORY))
            && path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(POINTER_SUFFIX))
        {
            INJECT_POINTER_POST_SYNC_HOOK.with(|injected| {
                if let Some(hook) = injected.borrow_mut().take() {
                    hook();
                }
            });
        }
    }
    #[cfg(not(test))]
    let _ = path;
}

fn run_pointer_ancestor_chain_post_sync_hook() {
    #[cfg(test)]
    INJECT_POINTER_ANCESTOR_CHAIN_POST_SYNC_HOOK.with(|injected| {
        if let Some(hook) = injected.borrow_mut().take() {
            hook();
        }
    });
}

#[cfg(test)]
fn remaining_pointer_directory_sync_failures() -> usize {
    INJECT_POINTER_DIRECTORY_SYNC_FAILURES.with(|injected| injected.borrow().len())
}

#[cfg(test)]
fn inject_publication_root_swap_hooks(
    before_object_install: impl FnOnce() + 'static,
    after_object_install: impl FnOnce() + 'static,
) {
    INJECT_PUBLICATION_ROOT_SWAP_HOOKS.with(|hooks| {
        *hooks.borrow_mut() = Some((
            Some(Box::new(before_object_install)),
            Some(Box::new(after_object_install)),
        ));
    });
}

#[cfg(test)]
fn run_publication_root_swap_hook(before: bool) {
    let hook = INJECT_PUBLICATION_ROOT_SWAP_HOOKS.with(|hooks| {
        let mut hooks = hooks.borrow_mut();
        let pair = hooks.as_mut()?;
        let hook = if before { pair.0.take() } else { pair.1.take() };
        if pair.0.is_none() && pair.1.is_none() {
            *hooks = None;
        }
        hook
    });
    if let Some(hook) = hook {
        hook();
    }
}

fn run_publication_probe(archive_root: &Path, parent: &Path) -> Result<(), ArchiveError> {
    validate_archive_directory_chain(archive_root, parent)?;
    let staging = create_staging_file(parent, PREFLIGHT_PROBE_BYTES)?;
    let linked = match create_probe_link(parent, &staging) {
        Ok(linked) => linked,
        Err(error) => {
            let _ = remove_probe_files(archive_root, parent, &[staging]);
            return Err(error);
        }
    };
    let probe_result = sync_archive_directory(archive_root, parent);
    let cleanup_result = remove_probe_files(archive_root, parent, &[linked, staging]);
    probe_result?;
    cleanup_result
}

fn create_probe_link(parent: &Path, staging: &Path) -> Result<PathBuf, ArchiveError> {
    for _ in 0..128 {
        let sequence = STAGING_COUNTER.fetch_add(1, Ordering::Relaxed);
        let linked = parent.join(format!(
            ".staging-preflight-link-{}-{sequence:016x}",
            std::process::id()
        ));
        match fs::hard_link(staging, &linked) {
            Ok(()) => return Ok(linked),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(ArchiveError::io(
                    "archive_preflight_hard_link_failed",
                    &linked,
                    error,
                ));
            }
        }
    }
    Err(ArchiveError::new(
        "archive_preflight_hard_link_failed",
        Some(parent),
        "could not reserve a unique bounded preflight link name",
    ))
}

fn remove_probe_files(
    archive_root: &Path,
    parent: &Path,
    paths: &[PathBuf],
) -> Result<(), ArchiveError> {
    validate_archive_directory_chain(archive_root, parent)?;
    if let Some(first) = paths.first() {
        make_probe_writable(archive_root, first)?;
    }
    for path in paths {
        fs::remove_file(path)
            .map_err(|error| ArchiveError::io("archive_preflight_cleanup_failed", path, error))?;
    }
    sync_archive_directory(archive_root, parent)
}

#[cfg(unix)]
fn make_probe_writable(archive_root: &Path, path: &Path) -> Result<(), ArchiveError> {
    use std::os::unix::fs::PermissionsExt;

    let file = open_regular_file_no_follow(archive_root, path)?;
    let mut permissions = file
        .metadata()
        .map_err(|error| ArchiveError::io("archive_preflight_cleanup_failed", path, error))?
        .permissions();
    permissions.set_mode(permissions.mode() | 0o200);
    file.set_permissions(permissions)
        .map_err(|error| ArchiveError::io("archive_preflight_cleanup_failed", path, error))?;
    validate_open_regular_file_target(archive_root, path, &file)?;
    Ok(())
}

#[cfg(not(unix))]
// The portable standard-library permission type exposes only the Windows-style
// read-only bit here; clearing it does not broaden executable or ACL rights.
#[allow(clippy::permissions_set_readonly_false)]
fn make_probe_writable(archive_root: &Path, path: &Path) -> Result<(), ArchiveError> {
    let file = open_regular_file_no_follow(archive_root, path)?;
    let mut permissions = file
        .metadata()
        .map_err(|error| ArchiveError::io("archive_preflight_cleanup_failed", path, error))?
        .permissions();
    permissions.set_readonly(false);
    file.set_permissions(permissions)
        .map_err(|error| ArchiveError::io("archive_preflight_cleanup_failed", path, error))?;
    validate_open_regular_file_target(archive_root, path, &file)?;
    Ok(())
}

fn create_staging_file(parent: &Path, bytes: &[u8]) -> Result<PathBuf, ArchiveError> {
    for _ in 0..128 {
        let sequence = STAGING_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = parent.join(format!(".staging-{}-{sequence:016x}", std::process::id()));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;

            options.custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_NONBLOCK);
        }
        let mut file = match options.open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(ArchiveError::io(
                    "archive_staging_create_failed",
                    &path,
                    error,
                ));
            }
        };
        let result = (|| {
            file.write_all(bytes)
                .map_err(|error| ArchiveError::io("archive_staging_write_failed", &path, error))?;
            file.sync_all()
                .map_err(|error| ArchiveError::io("archive_staging_sync_failed", &path, error))?;
            let mut permissions = file
                .metadata()
                .map_err(|error| {
                    ArchiveError::io("archive_staging_inspection_failed", &path, error)
                })?
                .permissions();
            permissions.set_readonly(true);
            file.set_permissions(permissions).map_err(|error| {
                ArchiveError::io("archive_staging_permissions_failed", &path, error)
            })?;
            file.sync_all()
                .map_err(|error| ArchiveError::io("archive_staging_sync_failed", &path, error))?;
            Ok(path.clone())
        })();
        if result.is_err() {
            drop(file);
            let _ = fs::remove_file(&path);
        }
        return result;
    }
    Err(ArchiveError::new(
        "archive_staging_create_failed",
        Some(parent),
        "could not reserve a unique bounded staging name",
    ))
}

#[cfg(test)]
fn require_identical_file(
    archive_root: &Path,
    path: &Path,
    expected: &[u8],
    limit: u64,
) -> Result<(), ArchiveError> {
    let observed = read_bounded_regular_file(archive_root, path, limit)?;
    if observed != expected {
        return Err(ArchiveError::new(
            "archive_immutable_conflict",
            Some(path),
            "immutable archive path already contains different bytes",
        ));
    }
    Ok(())
}

#[cfg(test)]
fn read_bounded_regular_file(
    archive_root: &Path,
    path: &Path,
    limit: u64,
) -> Result<Vec<u8>, ArchiveError> {
    let mut file = open_regular_file_no_follow(archive_root, path)?;
    let metadata = validate_open_regular_file_target(archive_root, path, &file)?;
    if metadata.len() > limit {
        return Err(ArchiveError::new(
            "archive_file_too_large",
            Some(path),
            format!("file is {} bytes; maximum is {limit}", metadata.len()),
        ));
    }
    let capacity = usize::try_from(metadata.len()).map_err(|_| {
        ArchiveError::new(
            "archive_file_too_large",
            Some(path),
            "file length does not fit this host's address space",
        )
    })?;
    let mut bytes = Vec::with_capacity(capacity);
    Read::by_ref(&mut file)
        .take(limit.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|error| ArchiveError::io("archive_file_read_failed", path, error))?;
    if u64::try_from(bytes.len())
        .ok()
        .is_none_or(|length| length > limit)
    {
        return Err(ArchiveError::new(
            "archive_file_too_large",
            Some(path),
            format!("file exceeded the {limit}-byte read bound"),
        ));
    }
    validate_open_regular_file_target(archive_root, path, &file)?;
    Ok(bytes)
}

fn open_regular_file_no_follow(archive_root: &Path, path: &Path) -> Result<File, ArchiveError> {
    validate_archive_file_parent(archive_root, path)?;
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            return Err(ArchiveError::new(
                "archive_file_invalid",
                Some(path),
                "archive file must be a regular file, not a symlink or special file",
            ));
        }
        Ok(_) => {}
        Err(error) => {
            return Err(ArchiveError::io(
                "archive_file_inspection_failed",
                path,
                error,
            ));
        }
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        // O_NONBLOCK is set before fstat so an attacker cannot replace a
        // checked regular file with a FIFO or blocking device between the path
        // inspection and open.
        options.custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_NONBLOCK);
    }
    let file = options
        .open(path)
        .map_err(|error| ArchiveError::io("archive_file_open_failed", path, error))?;
    validate_open_regular_file_target(archive_root, path, &file)?;
    Ok(file)
}

fn validate_open_regular_file_target(
    archive_root: &Path,
    path: &Path,
    file: &File,
) -> Result<fs::Metadata, ArchiveError> {
    validate_archive_file_parent(archive_root, path)?;
    let descriptor_metadata = file
        .metadata()
        .map_err(|error| ArchiveError::io("archive_file_inspection_failed", path, error))?;
    if !descriptor_metadata.is_file() {
        return Err(ArchiveError::new(
            "archive_file_invalid",
            Some(path),
            "opened archive descriptor must refer to a regular file",
        ));
    }
    let path_metadata = fs::symlink_metadata(path)
        .map_err(|error| ArchiveError::io("archive_file_inspection_failed", path, error))?;
    if path_metadata.file_type().is_symlink() || !path_metadata.is_file() {
        return Err(ArchiveError::new(
            "archive_file_invalid",
            Some(path),
            "archive path changed to a symlink or special file while its descriptor was open",
        ));
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        if descriptor_metadata.dev() != path_metadata.dev()
            || descriptor_metadata.ino() != path_metadata.ino()
        {
            return Err(ArchiveError::new(
                "archive_file_replaced",
                Some(path),
                "archive path was replaced while its descriptor was open",
            ));
        }
    }

    validate_archive_file_parent(archive_root, path)?;
    Ok(descriptor_metadata)
}

fn ensure_real_directory(path: &Path) -> Result<(), ArchiveError> {
    let parent = parent_directory(path);
    match fs::create_dir(path) {
        Ok(()) => {
            // Persist the new inode before making its directory entry durable.
            sync_directory(path)?;
            sync_directory(parent)?;
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            require_existing_directory(path, "archive_layout_invalid")?;
            // Close the same concurrent-creator crash window as immutable
            // files: the process that first linked this directory entry may
            // not have synced its parent yet.
            sync_directory(path)?;
            sync_directory(parent)
        }
        Err(error) => Err(ArchiveError::io(
            "archive_directory_create_failed",
            path,
            error,
        )),
    }
}

fn ensure_durable_directory_tree(path: &Path) -> Result<(), ArchiveError> {
    if path.as_os_str().is_empty() {
        return Err(ArchiveError::new(
            "archive_root_invalid",
            Some(path),
            "archive root must not be an empty path",
        ));
    }

    let mut missing = Vec::new();
    let mut cursor = path;
    loop {
        match fs::symlink_metadata(cursor) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() || !metadata.is_dir() {
                    return Err(ArchiveError::new(
                        "archive_root_invalid",
                        Some(cursor),
                        "archive root components must be real directories, not symlinks or special files",
                    ));
                }
                // The existing anchor may itself have just been created by a
                // caller or concurrent preflight. Close its durability window
                // before building below it.
                sync_directory(cursor)?;
                sync_directory(parent_directory(cursor))?;
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                missing.push(cursor.to_path_buf());
                let parent = parent_directory(cursor);
                if parent == cursor {
                    return Err(ArchiveError::new(
                        "archive_root_invalid",
                        Some(path),
                        "archive root has no existing directory ancestor",
                    ));
                }
                cursor = parent;
            }
            Err(error) => {
                return Err(ArchiveError::io("archive_root_invalid", cursor, error));
            }
        }
    }

    for directory in missing.into_iter().rev() {
        ensure_real_directory(&directory)?;
    }
    Ok(())
}

fn acquire_archive_lock(
    archive_root: &Path,
    mode: ArchiveLockMode,
    timeout: Duration,
) -> Result<ArchiveLock, ArchiveError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        require_existing_directory(archive_root, "archive_root_invalid")?;
        let mut options = OpenOptions::new();
        options
            .read(true)
            .custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_DIRECTORY | nix::libc::O_NONBLOCK);
        let mut directory = options
            .open(archive_root)
            .map_err(|error| ArchiveError::io("archive_lock_open_failed", archive_root, error))?;
        validate_archive_lock_target(archive_root, &directory)?;

        let operation = match mode {
            ArchiveLockMode::Shared => FlockArg::LockSharedNonblock,
            ArchiveLockMode::Exclusive => FlockArg::LockExclusiveNonblock,
        };
        let started = Instant::now();
        loop {
            match Flock::lock(directory, operation) {
                Ok(lock) => {
                    // A path replacement while waiting would strand this lock
                    // on an inode that no longer coordinates the archive.
                    validate_archive_lock_target(archive_root, &lock)?;
                    return Ok(ArchiveLock { _lock: lock });
                }
                Err((returned, error)) if error == Errno::EWOULDBLOCK => {
                    directory = returned;
                    let elapsed = started.elapsed();
                    if elapsed >= timeout {
                        let requested = match mode {
                            ArchiveLockMode::Shared => "shared inventory",
                            ArchiveLockMode::Exclusive => "exclusive publication",
                        };
                        return Err(ArchiveError::new(
                            "archive_lock_timeout",
                            Some(archive_root),
                            format!(
                                "timed out after {} ms waiting for {requested} lock",
                                timeout.as_millis()
                            ),
                        ));
                    }
                    std::thread::sleep(
                        timeout
                            .saturating_sub(elapsed)
                            .min(ARCHIVE_LOCK_RETRY_INTERVAL),
                    );
                }
                Err((_returned, error)) => {
                    return Err(ArchiveError::new(
                        "archive_lock_failed",
                        Some(archive_root),
                        error.to_string(),
                    ));
                }
            }
        }
    }

    #[cfg(not(unix))]
    {
        let _ = (mode, timeout);
        Err(ArchiveError::new(
            "archive_lock_unsupported",
            Some(archive_root),
            "coherent archive publication and inventory capture require Unix flock semantics",
        ))
    }
}

#[cfg(unix)]
fn validate_archive_lock_target(archive_root: &Path, directory: &File) -> Result<(), ArchiveError> {
    use std::os::unix::fs::MetadataExt;

    let descriptor_metadata = directory
        .metadata()
        .map_err(|error| ArchiveError::io("archive_lock_inspection_failed", archive_root, error))?;
    if !descriptor_metadata.is_dir() {
        return Err(ArchiveError::new(
            "archive_lock_invalid",
            Some(archive_root),
            "archive lock descriptor must refer to the archive root directory",
        ));
    }
    let path_metadata = fs::symlink_metadata(archive_root)
        .map_err(|error| ArchiveError::io("archive_lock_inspection_failed", archive_root, error))?;
    if path_metadata.file_type().is_symlink() || !path_metadata.is_dir() {
        return Err(ArchiveError::new(
            "archive_lock_invalid",
            Some(archive_root),
            "archive root changed to a symlink or special file while acquiring its lock",
        ));
    }
    if descriptor_metadata.dev() != path_metadata.dev()
        || descriptor_metadata.ino() != path_metadata.ino()
    {
        return Err(ArchiveError::new(
            "archive_lock_invalid",
            Some(archive_root),
            "archive root directory was replaced while acquiring its lock",
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn archive_root_identity(directory: &File) -> Result<ArchiveRootIdentity, ArchiveError> {
    use std::os::unix::fs::MetadataExt;

    let metadata = directory.metadata().map_err(|error| {
        ArchiveError::new(
            "archive_lock_inspection_failed",
            None,
            format!("could not inspect locked archive root: {error}"),
        )
    })?;
    if !metadata.is_dir() {
        return Err(ArchiveError::new(
            "archive_lock_invalid",
            None,
            "archive root identity descriptor must refer to a directory",
        ));
    }
    Ok(ArchiveRootIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}

fn validate_archive_root_identity(
    archive_root: &Path,
    expected: ArchiveRootIdentity,
) -> Result<(), ArchiveError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        let metadata = fs::symlink_metadata(archive_root)
            .map_err(|error| ArchiveError::io("archive_root_replaced", archive_root, error))?;
        if metadata.file_type().is_symlink()
            || !metadata.is_dir()
            || metadata.dev() != expected.device
            || metadata.ino() != expected.inode
        {
            return Err(ArchiveError::new(
                "archive_root_replaced",
                Some(archive_root),
                "archive root no longer names the inode captured at the coordinated inventory boundary",
            ));
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let _ = expected;
        Err(ArchiveError::new(
            "archive_lock_unsupported",
            Some(archive_root),
            "archive root identity validation requires Unix metadata",
        ))
    }
}

fn parent_directory(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| {
            if path.has_root() {
                path
            } else {
                Path::new(".")
            }
        })
}

fn validate_archive_directory_chain(
    archive_root: &Path,
    directory: &Path,
) -> Result<(), ArchiveError> {
    if archive_directory_chain_exists(archive_root, directory)? {
        Ok(())
    } else {
        Err(ArchiveError::new(
            "archive_layout_invalid",
            Some(directory),
            "archive directory chain is incomplete",
        ))
    }
}

fn archive_directory_chain_exists(
    archive_root: &Path,
    directory: &Path,
) -> Result<bool, ArchiveError> {
    require_existing_directory(archive_root, "archive_root_invalid")?;
    let relative = directory.strip_prefix(archive_root).map_err(|_| {
        ArchiveError::new(
            "archive_path_invalid",
            Some(directory),
            format!("archive path is not below root {}", archive_root.display()),
        )
    })?;
    let mut current = archive_root.to_path_buf();
    for component in relative.components() {
        let std::path::Component::Normal(name) = component else {
            return Err(ArchiveError::new(
                "archive_path_invalid",
                Some(directory),
                "archive-relative paths may contain only normal components",
            ));
        };
        current.push(name);
        let metadata = match fs::symlink_metadata(&current) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => {
                return Err(ArchiveError::io(
                    "archive_directory_inspection_failed",
                    &current,
                    error,
                ));
            }
        };
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(ArchiveError::new(
                "archive_layout_invalid",
                Some(&current),
                "archive ancestors must be real directories, not symlinks or special files",
            ));
        }
    }
    Ok(true)
}

fn validate_archive_file_parent(archive_root: &Path, path: &Path) -> Result<(), ArchiveError> {
    let parent = path.parent().ok_or_else(|| {
        ArchiveError::new(
            "archive_path_invalid",
            Some(path),
            "archive file has no parent directory",
        )
    })?;
    validate_archive_directory_chain(archive_root, parent)
}

fn require_existing_directory(path: &Path, code: &'static str) -> Result<(), ArchiveError> {
    let metadata =
        fs::symlink_metadata(path).map_err(|error| ArchiveError::io(code, path, error))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(ArchiveError::new(
            code,
            Some(path),
            "path must be a real directory, not a symlink or special file",
        ));
    }
    Ok(())
}

fn checked_entry_budget(entries_seen: usize) -> Result<usize, ArchiveError> {
    let next = entries_seen.checked_add(1).ok_or_else(|| {
        ArchiveError::new("archive_budget_exceeded", None, "entry count overflowed")
    })?;
    if next > MAX_DIRECTORY_ENTRIES {
        return Err(ArchiveError::new(
            "archive_budget_exceeded",
            None,
            format!("archive traversal exceeded {MAX_DIRECTORY_ENTRIES} entries"),
        ));
    }
    Ok(next)
}

fn sync_directory(path: &Path) -> Result<(), ArchiveError> {
    let directory = open_directory_no_follow(path)?;
    directory
        .sync_all()
        .map_err(|error| ArchiveError::io("archive_directory_sync_failed", path, error))?;
    validate_open_directory_target(path, &directory)
}

fn sync_archive_directory(archive_root: &Path, path: &Path) -> Result<(), ArchiveError> {
    validate_archive_directory_chain(archive_root, path)?;
    sync_directory(path)?;
    validate_archive_directory_chain(archive_root, path)
}

#[cfg(test)]
fn sync_regular_file(archive_root: &Path, path: &Path) -> Result<(), ArchiveError> {
    let file = open_regular_file_no_follow(archive_root, path)?;
    file.sync_all()
        .map_err(|error| ArchiveError::io("archive_file_sync_failed", path, error))?;
    validate_open_regular_file_target(archive_root, path, &file)?;
    Ok(())
}

fn open_directory_no_follow(path: &Path) -> Result<File, ArchiveError> {
    require_existing_directory(path, "archive_directory_invalid")?;
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options
            .custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_DIRECTORY | nix::libc::O_NONBLOCK);
    }
    let directory = options
        .open(path)
        .map_err(|error| ArchiveError::io("archive_directory_open_failed", path, error))?;
    validate_open_directory_target(path, &directory)?;
    Ok(directory)
}

fn validate_open_directory_target(path: &Path, directory: &File) -> Result<(), ArchiveError> {
    let descriptor_metadata = directory
        .metadata()
        .map_err(|error| ArchiveError::io("archive_directory_inspection_failed", path, error))?;
    if !descriptor_metadata.is_dir() {
        return Err(ArchiveError::new(
            "archive_directory_invalid",
            Some(path),
            "opened archive descriptor must refer to a directory",
        ));
    }
    let path_metadata = fs::symlink_metadata(path)
        .map_err(|error| ArchiveError::io("archive_directory_inspection_failed", path, error))?;
    if path_metadata.file_type().is_symlink() || !path_metadata.is_dir() {
        return Err(ArchiveError::new(
            "archive_directory_invalid",
            Some(path),
            "archive directory path changed to a symlink or special file while open",
        ));
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        if descriptor_metadata.dev() != path_metadata.dev()
            || descriptor_metadata.ino() != path_metadata.ino()
        {
            return Err(ArchiveError::new(
                "archive_directory_replaced",
                Some(path),
                "archive directory path was replaced while its descriptor was open",
            ));
        }
    }
    Ok(())
}

fn object_relative_path(digest: &str) -> String {
    format!(
        "{OBJECT_DIRECTORY}/{SHA256_DIRECTORY}/{}/{}{}",
        &digest[..2],
        digest,
        RECORD_SUFFIX
    )
}

fn pointer_relative_path(invocation_id: &str) -> Result<String, ArchiveError> {
    let invocation_id = InvocationId::parse(invocation_id)?;
    Ok(format!(
        "{INVOCATION_DIRECTORY}/{}/{}{}",
        invocation_id.shard(),
        invocation_id.as_str(),
        POINTER_SUFFIX
    ))
}

fn invocation_id_from_pointer_name(name: &str) -> Option<InvocationId> {
    let value = name.strip_suffix(POINTER_SUFFIX)?;
    InvocationId::parse(value).ok()
}

impl InvocationId {
    fn parse(value: &str) -> Result<Self, ArchiveError> {
        if value.len() != 26
            || !value.bytes().all(valid_crockford_byte)
            || !matches!(value.as_bytes().first(), Some(b'0'..=b'7'))
        {
            return Err(ArchiveError::new(
                "archive_invocation_id_invalid",
                None,
                "invocation id must be a 26-character uppercase Crockford ULID",
            ));
        }
        let mut bytes = [0_u8; 26];
        bytes.copy_from_slice(value.as_bytes());
        Ok(Self(bytes))
    }

    fn as_str(&self) -> &str {
        // Construction accepts ASCII Crockford bytes only.
        std::str::from_utf8(&self.0).expect("validated invocation ids are ASCII")
    }

    fn shard(&self) -> &str {
        // The final two ULID characters are entropy, unlike the timestamp
        // prefix, and distribute concurrent invocations across all shards.
        std::str::from_utf8(&self.0[24..]).expect("validated invocation ids are ASCII")
    }
}

fn valid_id_shard(value: &str) -> bool {
    value.len() == 2 && value.bytes().all(valid_crockford_byte)
}

fn valid_crockford_byte(byte: u8) -> bool {
    byte.is_ascii_digit()
        || matches!(
            byte,
            b'A'..=b'H' | b'J'..=b'K' | b'M'..=b'N' | b'P'..=b'T' | b'V'..=b'Z'
        )
}

fn require_sha256(value: &str, label: &str) -> Result<(), ArchiveError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ArchiveError::new(
            "archive_digest_invalid",
            None,
            format!("{label} must be exactly 64 lowercase hexadecimal characters"),
        ));
    }
    Ok(())
}

fn sha256_bytes(bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(bytes);
    format!("{:x}", digest.finalize())
}

fn join_relative(root: &Path, relative: &str) -> PathBuf {
    relative
        .split('/')
        .fold(root.to_path_buf(), |path, part| path.join(part))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn canonical_record_publication_load_idempotence_and_tamper_refusal_compose() {
        use std::os::unix::fs::PermissionsExt;

        let archive = tempfile::tempdir().unwrap();
        let record = crate::record::tests::valid_record_fixture();
        let canonical = canonical_record_bytes(&record).unwrap();

        let first = publish_record(archive.path(), &record).unwrap();
        assert!(first.newly_published);
        assert_eq!(first.record_sha256, sha256_bytes(&canonical));
        assert!(archive.path().join(&first.pointer_relative_path).is_file());
        let object_path = archive.path().join(&first.object_relative_path);
        assert_eq!(fs::read(&object_path).unwrap(), canonical);

        let loaded = load_archive(archive.path()).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].record, record);
        assert_eq!(loaded[0].receipt.record_sha256, first.record_sha256);
        assert!(!loaded[0].receipt.newly_published);

        let second = publish_record(archive.path(), &record).unwrap();
        assert!(!second.newly_published);
        assert_eq!(second.record_sha256, first.record_sha256);
        assert_eq!(load_archive(archive.path()).unwrap().len(), 1);

        let mut permissions = fs::metadata(&object_path).unwrap().permissions();
        permissions.set_mode(0o600);
        fs::set_permissions(&object_path, permissions).unwrap();
        fs::write(&object_path, b"{}\n").unwrap();
        let error = load_archive(archive.path()).unwrap_err();
        assert_eq!(error.code, "archive_record_invalid");
    }

    #[cfg(unix)]
    #[test]
    fn publication_cannot_escape_during_root_swap_and_restore() {
        let parent = tempfile::tempdir().unwrap();
        let archive = parent.path().join("archive");
        let replacement = parent.path().join("replacement");
        let displaced = parent.path().join("displaced");
        fs::create_dir(&archive).unwrap();
        fs::create_dir(&replacement).unwrap();

        let before_archive = archive.clone();
        let before_replacement = replacement.clone();
        let before_displaced = displaced.clone();
        let after_archive = archive.clone();
        let after_replacement = replacement.clone();
        let after_displaced = displaced.clone();
        inject_publication_root_swap_hooks(
            move || {
                fs::rename(&before_archive, &before_displaced).unwrap();
                fs::rename(&before_replacement, &before_archive).unwrap();
            },
            move || {
                fs::rename(&after_archive, &after_replacement).unwrap();
                fs::rename(&after_displaced, &after_archive).unwrap();
            },
        );

        let record = crate::record::tests::valid_record_fixture();
        let receipt = publish_record(&archive, &record).unwrap();

        assert_eq!(load_archive(&archive).unwrap()[0].record, record);
        assert!(archive.join(receipt.object_relative_path).is_file());
        assert!(archive.join(receipt.pointer_relative_path).is_file());
        assert_eq!(iter_archive(&replacement).unwrap().remaining(), 0);
        assert!(fs::read_dir(&replacement).unwrap().next().is_none());
    }

    #[test]
    fn paths_are_content_addressed_and_invocation_sharded() {
        let digest = "a".repeat(64);
        assert_eq!(
            object_relative_path(&digest),
            format!("objects/sha256/aa/{digest}.run-record-v1.json")
        );
        assert_eq!(
            pointer_relative_path("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
            "invocations/AV/01ARZ3NDEKTSV4RRFFQ69G5FAV.pointer-v1.json"
        );
        assert_eq!(
            pointer_relative_path("01ARZ3NDEKTSV4RRFFQ69G5FAA").unwrap(),
            "invocations/AA/01ARZ3NDEKTSV4RRFFQ69G5FAA.pointer-v1.json"
        );
        assert!(pointer_relative_path("../escape").is_err());

        let candidate =
            ArchivePublicationUnknownV1::new("01ARZ3NDEKTSV4RRFFQ69G5FAV", digest.clone()).unwrap();
        assert_eq!(
            candidate.object_relative_path,
            object_relative_path(&digest)
        );
        assert_eq!(
            candidate.pointer_relative_path,
            pointer_relative_path(&candidate.invocation_id).unwrap()
        );
    }

    #[test]
    fn direct_publication_durably_creates_the_root_and_preflight_leaves_only_layout() {
        let parent = tempfile::tempdir().unwrap();
        let missing = parent.path().join("missing-parent").join("missing-archive");
        let receipt = publish_record(&missing, &crate::record::tests::valid_record_fixture())
            .expect("direct publication must create and durably sync its archive root");

        assert!(missing.is_dir());
        assert!(missing.join(receipt.object_relative_path).is_file());
        assert!(missing.join(receipt.pointer_relative_path).is_file());

        preflight_archive_publication(&missing).unwrap();
        assert!(
            missing
                .join(OBJECT_DIRECTORY)
                .join(SHA256_DIRECTORY)
                .join("00")
                .is_dir()
        );
        let invocation_probe = missing.join(INVOCATION_DIRECTORY).join("00");
        assert!(invocation_probe.is_dir());
        assert!(fs::read_dir(&invocation_probe).unwrap().next().is_none());
        fs::write(
            invocation_probe.join(".staging-simulated-crash"),
            PREFLIGHT_PROBE_BYTES,
        )
        .unwrap();
        assert_eq!(load_archive(&missing).unwrap().len(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn immutable_parent_disappearance_is_a_typed_failure() {
        let archive = tempfile::tempdir().unwrap();
        let record = crate::record::tests::valid_record_fixture();
        let bytes = canonical_record_bytes(&record).unwrap();
        let digest = sha256_bytes(&bytes);
        let object_relative = object_relative_path(&digest);
        let object_parent = archive
            .path()
            .join(&object_relative)
            .parent()
            .unwrap()
            .to_path_buf();
        let lock = acquire_archive_lock(
            archive.path(),
            ArchiveLockMode::Exclusive,
            Duration::from_secs(1),
        )
        .unwrap();
        let anchored = lock.anchored_root(archive.path()).unwrap();
        anchored.ensure_archive_layout().unwrap();
        inject_immutable_parent_post_ensure_hook(move || {
            fs::remove_dir(&object_parent).unwrap();
        });

        let error = anchored
            .install_immutable(&object_relative, &bytes, MAX_ARCHIVE_RECORD_BYTES)
            .unwrap_err();

        assert_eq!(error.code, "archive_directory_replaced");
    }

    #[cfg(unix)]
    #[test]
    fn orphaned_content_is_not_inventory_and_retry_reuses_it_before_publishing_pointer() {
        use std::os::unix::fs::MetadataExt;

        let archive = tempfile::tempdir().unwrap();
        let record = crate::record::tests::valid_record_fixture();
        let bytes = canonical_record_bytes(&record).unwrap();
        let digest = sha256_bytes(&bytes);
        let object_relative = object_relative_path(&digest);
        let pointer_relative = pointer_relative_path(&record.invocation.invocation_id).unwrap();

        let original_inode = {
            let lock = acquire_archive_lock(
                archive.path(),
                ArchiveLockMode::Exclusive,
                Duration::from_secs(1),
            )
            .unwrap();
            let anchored = lock.anchored_root(archive.path()).unwrap();
            anchored.ensure_archive_layout().unwrap();
            assert!(
                anchored
                    .install_immutable(&object_relative, &bytes, MAX_ARCHIVE_RECORD_BYTES)
                    .unwrap(),
                "the crash boundary begins after installing the content object"
            );
            fs::metadata(archive.path().join(&object_relative))
                .unwrap()
                .ino()
        };

        assert!(!archive.path().join(&pointer_relative).exists());
        assert_eq!(iter_archive(archive.path()).unwrap().remaining(), 0);

        let receipt = publish_record(archive.path(), &record).unwrap();

        assert!(receipt.newly_published);
        assert_eq!(receipt.object_relative_path, object_relative);
        assert_eq!(receipt.pointer_relative_path, pointer_relative);
        assert_eq!(
            fs::metadata(archive.path().join(&receipt.object_relative_path))
                .unwrap()
                .ino(),
            original_inode,
            "retry must reuse the immutable content object rather than replace it"
        );
        assert_eq!(
            fs::read_dir(
                archive
                    .path()
                    .join(&receipt.object_relative_path)
                    .parent()
                    .unwrap()
            )
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.ends_with(RECORD_SUFFIX))
            })
            .count(),
            1
        );
        assert_eq!(
            fs::read_dir(
                archive
                    .path()
                    .join(&receipt.pointer_relative_path)
                    .parent()
                    .unwrap()
            )
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.ends_with(POINTER_SUFFIX))
            })
            .count(),
            1
        );
        assert_eq!(load_archive(archive.path()).unwrap().len(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn archive_lock_wait_is_bounded_and_fail_closed() {
        let archive = tempfile::tempdir().unwrap();
        let held = acquire_archive_lock(
            archive.path(),
            ArchiveLockMode::Exclusive,
            Duration::from_secs(1),
        )
        .unwrap();

        let started = Instant::now();
        let error = acquire_archive_lock(
            archive.path(),
            ArchiveLockMode::Shared,
            Duration::from_millis(40),
        )
        .unwrap_err();
        assert_eq!(error.code, "archive_lock_timeout");
        assert!(started.elapsed() < Duration::from_secs(1));
        drop(held);
    }

    #[cfg(unix)]
    #[test]
    fn interrupted_pointer_directory_sync_is_retried_before_acknowledgement() {
        let archive = tempfile::tempdir().unwrap();
        let record = crate::record::tests::valid_record_fixture();
        assert_eq!(
            std::io::Error::from_raw_os_error(nix::libc::EINTR).kind(),
            std::io::ErrorKind::Interrupted
        );
        inject_pointer_directory_sync_failures([nix::libc::EINTR]);

        let receipt = publish_record(archive.path(), &record).unwrap();

        assert!(receipt.newly_published);
        assert_eq!(remaining_pointer_directory_sync_failures(), 0);
        assert_eq!(load_archive(archive.path()).unwrap()[0].record, record);
    }

    #[cfg(unix)]
    #[test]
    fn eio_after_pointer_visibility_is_not_retried_or_acknowledged() {
        let archive = tempfile::tempdir().unwrap();
        let record = crate::record::tests::valid_record_fixture();
        assert_ne!(
            std::io::Error::from_raw_os_error(nix::libc::EIO).kind(),
            std::io::ErrorKind::Interrupted
        );
        inject_pointer_directory_sync_failures([nix::libc::EIO, nix::libc::EINTR]);

        let error = publish_record(archive.path(), &record).unwrap_err();

        assert_eq!(error.code, "archive_pointer_publication_unknown");
        assert!(error.possibly_published.is_some());
        assert_eq!(
            remaining_pointer_directory_sync_failures(),
            1,
            "a substantive EIO must stop immediately instead of consuming a retry"
        );

        inject_pointer_directory_sync_failures([]);
        let candidate = error.possibly_published.as_deref().unwrap();
        assert!(matches!(
            reconcile_archive_publication(archive.path(), candidate).unwrap(),
            ArchiveReconciliationV1::Durable { .. }
        ));
    }

    #[cfg(unix)]
    #[test]
    fn replaced_pointer_parent_after_sync_is_possibly_published_even_with_same_entry() {
        use std::os::unix::fs::MetadataExt;

        let holder = tempfile::tempdir().unwrap();
        let archive = holder.path().join("archive");
        fs::create_dir(&archive).unwrap();
        let record = crate::record::tests::valid_record_fixture();
        let pointer_relative = pointer_relative_path(&record.invocation.invocation_id).unwrap();
        let pointer_path = archive.join(&pointer_relative);
        let pointer_parent = pointer_path.parent().unwrap().to_path_buf();
        let pointer_name = pointer_path.file_name().unwrap().to_owned();
        let displaced_parent = holder.path().join("displaced-pointer-parent");
        let hook_parent = pointer_parent.clone();
        let hook_displaced = displaced_parent.clone();
        inject_pointer_post_sync_hook(move || {
            fs::rename(&hook_parent, &hook_displaced).unwrap();
            fs::create_dir(&hook_parent).unwrap();
            fs::hard_link(
                hook_displaced.join(&pointer_name),
                hook_parent.join(&pointer_name),
            )
            .unwrap();
        });

        let error = publish_record(&archive, &record).unwrap_err();

        assert_eq!(error.code, "archive_pointer_publication_unknown");
        assert!(error.possibly_published.is_some());
        let canonical = fs::metadata(&pointer_path).unwrap();
        let displaced =
            fs::metadata(displaced_parent.join(pointer_path.file_name().unwrap())).unwrap();
        assert_eq!(
            (canonical.dev(), canonical.ino()),
            (displaced.dev(), displaced.ino())
        );
    }

    #[cfg(unix)]
    #[test]
    fn replaced_pointer_ancestor_is_detected_before_durable_acknowledgement() {
        use std::os::unix::fs::MetadataExt;

        let holder = tempfile::tempdir().unwrap();
        let archive = holder.path().join("archive");
        fs::create_dir(&archive).unwrap();
        let record = crate::record::tests::valid_record_fixture();
        let pointer_relative = pointer_relative_path(&record.invocation.invocation_id).unwrap();
        let pointer_path = archive.join(&pointer_relative);
        let shard_path = pointer_path.parent().unwrap().to_path_buf();
        let shard_name = shard_path.file_name().unwrap().to_owned();
        let invocation_root = shard_path.parent().unwrap().to_path_buf();
        let displaced_invocation_root = holder.path().join("displaced-invocations");
        let hook_invocation_root = invocation_root.clone();
        let hook_displaced_root = displaced_invocation_root.clone();
        inject_pointer_ancestor_chain_post_sync_hook(move || {
            fs::rename(&hook_invocation_root, &hook_displaced_root).unwrap();
            fs::create_dir(&hook_invocation_root).unwrap();
            fs::rename(
                hook_displaced_root.join(&shard_name),
                hook_invocation_root.join(&shard_name),
            )
            .unwrap();
        });

        let error = publish_record(&archive, &record).unwrap_err();

        assert_eq!(error.code, "archive_pointer_publication_unknown");
        assert!(error.possibly_published.is_some());
        assert!(pointer_path.is_file());
        assert_ne!(
            fs::metadata(&invocation_root).unwrap().ino(),
            fs::metadata(&displaced_invocation_root).unwrap().ino(),
            "the pointer shard must remain reachable through a newly installed ancestor"
        );
        assert_eq!(load_archive(&archive).unwrap()[0].record, record);
    }

    #[cfg(unix)]
    #[test]
    fn replaced_pointer_entry_after_sync_is_possibly_published_even_with_same_bytes() {
        use std::os::unix::fs::MetadataExt;

        let archive = tempfile::tempdir().unwrap();
        let record = crate::record::tests::valid_record_fixture();
        let record_sha256 = sha256_bytes(&canonical_record_bytes(&record).unwrap());
        let pointer_relative = pointer_relative_path(&record.invocation.invocation_id).unwrap();
        let pointer_path = archive.path().join(&pointer_relative);
        let displaced_pointer = archive.path().join("displaced-pointer");
        let expected_pointer = serde_json::to_vec(&InvocationPointerV1 {
            archive_format_version: ARCHIVE_FORMAT_VERSION,
            invocation_id: record.invocation.invocation_id.clone(),
            record_sha256: record_sha256.clone(),
            object_relative_path: object_relative_path(&record_sha256),
        })
        .unwrap();
        let hook_pointer = pointer_path.clone();
        let hook_displaced = displaced_pointer.clone();
        let replacement_bytes = expected_pointer.clone();
        inject_pointer_post_sync_hook(move || {
            fs::rename(&hook_pointer, &hook_displaced).unwrap();
            fs::write(&hook_pointer, replacement_bytes).unwrap();
        });

        let error = publish_record(archive.path(), &record).unwrap_err();

        assert_eq!(error.code, "archive_pointer_publication_unknown");
        assert!(error.possibly_published.is_some());
        assert_eq!(fs::read(&pointer_path).unwrap(), expected_pointer);
        assert_ne!(
            fs::metadata(&pointer_path).unwrap().ino(),
            fs::metadata(&displaced_pointer).unwrap().ino(),
            "byte-identical replacement must still be detected by inode identity"
        );
    }

    #[cfg(unix)]
    #[test]
    fn reader_rejects_ambiguous_pointer_until_durability_closure_succeeds() {
        let archive = tempfile::tempdir().unwrap();
        let record = crate::record::tests::valid_record_fixture();
        let expected_digest = sha256_bytes(&canonical_record_bytes(&record).unwrap());
        inject_pointer_directory_sync_failures(std::iter::repeat_n(
            nix::libc::EINTR,
            POST_LINK_DIRECTORY_SYNC_ATTEMPTS * 2,
        ));

        let error = publish_record(archive.path(), &record).unwrap_err();

        assert_eq!(error.code, "archive_pointer_publication_unknown");
        let possible = error.possibly_published.as_ref().unwrap();
        assert_eq!(possible.archive_format_version, ARCHIVE_FORMAT_VERSION);
        assert_eq!(possible.invocation_id, record.invocation.invocation_id);
        assert_eq!(possible.record_sha256, expected_digest);
        assert_eq!(
            possible.object_relative_path,
            object_relative_path(&expected_digest)
        );
        assert_eq!(
            possible.pointer_relative_path,
            pointer_relative_path(&record.invocation.invocation_id).unwrap()
        );
        assert!(
            archive
                .path()
                .join(&possible.pointer_relative_path)
                .is_file(),
            "the injected failure occurs only after the hard link is visible"
        );
        let serialized = serde_json::to_value(&error).unwrap();
        assert_eq!(
            serialized["possibly_published"]["invocation_id"],
            record.invocation.invocation_id
        );
        assert_eq!(
            remaining_pointer_directory_sync_failures(),
            POST_LINK_DIRECTORY_SYNC_ATTEMPTS,
            "publication must leave the separately injected reader-healing failure queued"
        );
        let reader_error = load_archive(archive.path()).unwrap_err();
        assert_eq!(reader_error.code, "archive_pointer_publication_unknown");
        assert_eq!(
            reader_error
                .possibly_published
                .as_deref()
                .expect("reader retains the exact ambiguous publication identity"),
            possible.as_ref()
        );
        assert_eq!(remaining_pointer_directory_sync_failures(), 0);
        assert_eq!(load_archive(archive.path()).unwrap()[0].record, record);

        inject_pointer_directory_sync_failures(std::iter::repeat_n(
            nix::libc::EINTR,
            POST_LINK_DIRECTORY_SYNC_ATTEMPTS,
        ));
        let still_unknown = reconcile_archive_publication(archive.path(), possible).unwrap_err();
        assert_eq!(still_unknown.code, "archive_pointer_publication_unknown");
        assert_eq!(
            still_unknown
                .possibly_published
                .as_deref()
                .expect("candidate remains typed"),
            possible.as_ref()
        );

        let reconciled = reconcile_archive_publication(archive.path(), possible).unwrap();
        let ArchiveReconciliationV1::Durable { receipt } = reconciled else {
            panic!("visible matching pointer must reconcile as durable");
        };
        assert!(!receipt.newly_published);
        assert_eq!(receipt.record_sha256, expected_digest);
    }

    #[test]
    fn reconciliation_distinguishes_definite_absence_and_durable_conflict() {
        let archive = tempfile::tempdir().unwrap();
        let record = crate::record::tests::valid_record_fixture();
        let actual = publish_record(archive.path(), &record).unwrap();

        let absent =
            ArchivePublicationUnknownV1::new("01ARZ3NDEKTSV4RRFFQ69G5FAA", "a".repeat(64)).unwrap();
        assert_eq!(
            reconcile_archive_publication(archive.path(), &absent).unwrap(),
            ArchiveReconciliationV1::Absent {
                candidate: absent.clone()
            }
        );

        let conflicting = ArchivePublicationUnknownV1::new(
            record.invocation.invocation_id.clone(),
            "b".repeat(64),
        )
        .unwrap();
        let outcome = reconcile_archive_publication(archive.path(), &conflicting).unwrap();
        let ArchiveReconciliationV1::Conflict {
            candidate,
            published,
        } = outcome
        else {
            panic!("a different durable record must be a typed conflict");
        };
        assert_eq!(candidate, conflicting);
        assert_eq!(published.record_sha256, actual.record_sha256);
        assert!(!published.newly_published);
    }

    #[test]
    fn reconciliation_rejects_noncanonical_candidate_paths() {
        let archive = tempfile::tempdir().unwrap();
        let mut candidate =
            ArchivePublicationUnknownV1::new("01ARZ3NDEKTSV4RRFFQ69G5FAV", "a".repeat(64)).unwrap();
        candidate.pointer_relative_path.push_str(".forged");

        let error = reconcile_archive_publication(archive.path(), &candidate).unwrap_err();
        assert_eq!(error.code, "archive_reconciliation_candidate_invalid");
        assert!(fs::read_dir(archive.path()).unwrap().next().is_none());
    }

    #[cfg(unix)]
    #[test]
    fn empty_inventory_capture_does_not_create_coordination_state() {
        let archive = tempfile::tempdir().unwrap();
        assert!(fs::read_dir(archive.path()).unwrap().next().is_none());

        assert_eq!(iter_archive(archive.path()).unwrap().remaining(), 0);

        assert!(fs::read_dir(archive.path()).unwrap().next().is_none());
    }

    #[cfg(unix)]
    #[test]
    fn archive_root_lock_never_follows_a_symlink() {
        use std::os::unix::fs::symlink;

        let holder = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let archive_link = holder.path().join("archive-link");
        symlink(outside.path(), &archive_link).unwrap();

        assert_eq!(
            publish_record(&archive_link, &crate::record::tests::valid_record_fixture())
                .unwrap_err()
                .code,
            "archive_root_invalid"
        );
        assert_eq!(
            iter_archive(&archive_link).unwrap_err().code,
            "archive_root_invalid"
        );
        assert!(fs::read_dir(outside.path()).unwrap().next().is_none());
    }

    #[cfg(unix)]
    #[test]
    fn post_acquisition_root_replacement_is_detected_by_lock_and_iterator() {
        let parent = tempfile::tempdir().unwrap();
        let archive = parent.path().join("archive");
        fs::create_dir(&archive).unwrap();
        let record = crate::record::tests::valid_record_fixture();
        publish_record(&archive, &record).unwrap();

        let canonical = canonical_archive_root(&archive).unwrap();
        let lock =
            acquire_archive_lock(&canonical, ArchiveLockMode::Shared, Duration::from_secs(1))
                .unwrap();
        let mut inventory = iter_archive(&canonical).unwrap();
        let displaced = parent.path().join("displaced");
        fs::rename(&canonical, &displaced).unwrap();
        fs::create_dir(&canonical).unwrap();

        assert_eq!(
            lock.validate_root(&canonical).unwrap_err().code,
            "archive_lock_invalid"
        );
        assert_eq!(
            inventory.next().unwrap().unwrap_err().code,
            "archive_root_replaced"
        );
    }

    #[cfg(unix)]
    #[test]
    fn captured_root_descriptor_cannot_escape_during_swap_and_restore() {
        let parent = tempfile::tempdir().unwrap();
        let archive = parent.path().join("archive");
        let replacement = parent.path().join("replacement");
        fs::create_dir(&archive).unwrap();
        fs::create_dir(&replacement).unwrap();

        let original = crate::record::tests::valid_record_fixture();
        publish_record(&archive, &original).unwrap();
        let mut forged_external_root = original.clone();
        forged_external_root.machine.machine_label = format!("hostname-sha256:{}", "b".repeat(64));
        publish_record(&replacement, &forged_external_root).unwrap();

        let mut inventory = iter_archive(&archive).unwrap();
        assert_eq!(inventory.remaining(), 1);
        let invocation_id = inventory.invocation_ids[0];
        let displaced = parent.path().join("displaced");
        fs::rename(&archive, &displaced).unwrap();
        fs::rename(&replacement, &archive).unwrap();

        // The caller-facing path now names a complete, independently valid
        // archive with the same invocation and different record bytes. The
        // captured descriptor must still resolve both pointer and object from
        // the original coordinated root inode.
        let anchored = load_invocation(&inventory.anchored_root, invocation_id)
            .unwrap()
            .0;
        assert_eq!(anchored.record, original);
        assert_ne!(anchored.record, forged_external_root);

        fs::rename(&archive, &replacement).unwrap();
        fs::rename(&displaced, &archive).unwrap();
        assert_eq!(inventory.next().unwrap().unwrap().record, original);
    }

    #[cfg(unix)]
    #[test]
    fn regular_file_reads_and_syncs_reject_symlinks_and_replacements() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("target");
        let link = directory.path().join("link");
        fs::write(&target, b"authority").unwrap();
        symlink(&target, &link).unwrap();
        assert_eq!(
            read_bounded_regular_file(directory.path(), &link, 100)
                .unwrap_err()
                .code,
            "archive_file_invalid"
        );
        assert_eq!(
            sync_regular_file(directory.path(), &link).unwrap_err().code,
            "archive_file_invalid"
        );

        let path = directory.path().join("replaceable");
        let old = directory.path().join("old");
        fs::write(&path, b"old").unwrap();
        let descriptor = open_regular_file_no_follow(directory.path(), &path).unwrap();
        fs::rename(&path, &old).unwrap();
        fs::write(&path, b"replacement").unwrap();
        assert_eq!(
            validate_open_regular_file_target(directory.path(), &path, &descriptor)
                .unwrap_err()
                .code,
            "archive_file_replaced"
        );
    }

    #[cfg(unix)]
    #[test]
    fn bounded_file_open_is_nonblocking_and_rejects_symlink_ancestors() {
        use std::os::fd::AsRawFd;
        use std::os::unix::fs::symlink;

        let archive = tempfile::tempdir().unwrap();
        let regular = archive.path().join("regular");
        fs::write(&regular, b"record").unwrap();
        let descriptor = open_regular_file_no_follow(archive.path(), &regular).unwrap();
        // SAFETY: F_GETFL reads flags from a live descriptor and writes no memory.
        let flags = unsafe { nix::libc::fcntl(descriptor.as_raw_fd(), nix::libc::F_GETFL) };
        assert!(flags >= 0);
        assert_ne!(flags & nix::libc::O_NONBLOCK, 0);

        let outside = tempfile::tempdir().unwrap();
        fs::write(outside.path().join("record"), b"outside").unwrap();
        let redirected = archive.path().join("redirected");
        symlink(outside.path(), &redirected).unwrap();
        let redirected_file = redirected.join("record");
        assert_eq!(
            read_bounded_regular_file(archive.path(), &redirected_file, 100)
                .unwrap_err()
                .code,
            "archive_layout_invalid"
        );
    }

    #[cfg(unix)]
    #[test]
    fn archive_error_path_serialization_is_lossy_but_infallible() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let path = PathBuf::from(OsString::from_vec(b"archive-\xff".to_vec()));
        let error = ArchiveError::new("archive_test", Some(&path), "failure");

        let json = serde_json::to_value(&error).unwrap();

        assert_eq!(json["code"], "archive_test");
        assert_eq!(json["path"], path.to_string_lossy().as_ref());
        assert!(error.to_string().contains(path.to_string_lossy().as_ref()));
    }

    #[cfg(unix)]
    #[test]
    fn inventory_capture_waits_for_pointer_publication_and_then_is_fixed() {
        use std::sync::mpsc;

        let archive = tempfile::tempdir().unwrap();
        let first = crate::record::tests::valid_record_fixture();
        publish_record(archive.path(), &first).unwrap();

        let before_publication = iter_archive(archive.path()).unwrap();
        assert_eq!(before_publication.remaining(), 1);

        let mut second = first.clone();
        second.invocation.invocation_id.replace_range(25..26, "B");
        let bytes = canonical_record_bytes(&second).unwrap();
        let digest = sha256_bytes(&bytes);
        let object_relative_path = object_relative_path(&digest);
        install_immutable(
            archive.path(),
            &archive.path().join(&object_relative_path),
            &bytes,
            MAX_ARCHIVE_RECORD_BYTES,
        )
        .unwrap();
        let pointer_relative_path =
            pointer_relative_path(&second.invocation.invocation_id).unwrap();
        let pointer_bytes = serde_json::to_vec(&InvocationPointerV1 {
            archive_format_version: ARCHIVE_FORMAT_VERSION,
            invocation_id: second.invocation.invocation_id,
            record_sha256: digest,
            object_relative_path,
        })
        .unwrap();

        let publication_lock = acquire_archive_lock(
            archive.path(),
            ArchiveLockMode::Exclusive,
            Duration::from_secs(1),
        )
        .unwrap();
        let (started_tx, started_rx) = mpsc::sync_channel(0);
        let (result_tx, result_rx) = mpsc::sync_channel(0);
        let archive_root = archive.path().to_path_buf();
        let reader = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            result_tx.send(iter_archive(&archive_root)).unwrap();
        });
        started_rx.recv().unwrap();
        assert!(result_rx.recv_timeout(Duration::from_millis(100)).is_err());

        install_immutable(
            archive.path(),
            &archive.path().join(pointer_relative_path),
            &pointer_bytes,
            MAX_POINTER_BYTES,
        )
        .unwrap();
        drop(publication_lock);

        let after_publication = result_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap();
        assert_eq!(after_publication.remaining(), 2);
        assert_eq!(before_publication.remaining(), 1);
        reader.join().unwrap();
    }

    #[test]
    fn streaming_iterator_orders_fixed_invocation_inventory() {
        let archive = tempfile::tempdir().unwrap();
        let first = crate::record::tests::valid_record_fixture();
        let mut second = first.clone();
        second.invocation.invocation_id.replace_range(25..26, "B");

        let first_receipt = publish_record(archive.path(), &first).unwrap();
        let second_receipt = publish_record(archive.path(), &second).unwrap();
        assert_ne!(
            Path::new(&first_receipt.pointer_relative_path)
                .components()
                .nth(1),
            Path::new(&second_receipt.pointer_relative_path)
                .components()
                .nth(1),
            "entropy suffixes must select different shards"
        );

        let mut records = iter_archive(archive.path()).unwrap();
        let mut second_pass = records.clone();
        assert_eq!(records.remaining(), 2);
        assert_eq!(
            records
                .next()
                .unwrap()
                .unwrap()
                .record
                .invocation
                .invocation_id,
            first.invocation.invocation_id
        );
        assert_eq!(records.remaining(), 1);
        assert_eq!(
            records
                .next()
                .unwrap()
                .unwrap()
                .record
                .invocation
                .invocation_id,
            second.invocation.invocation_id
        );
        assert_eq!(records.remaining(), 0);
        assert!(records.next().is_none());
        assert_eq!(
            second_pass
                .by_ref()
                .map(|result| result.unwrap().record.invocation.invocation_id)
                .collect::<Vec<_>>(),
            vec![
                first.invocation.invocation_id,
                second.invocation.invocation_id,
            ]
        );
        assert_eq!(second_pass.remaining(), 0);
    }

    #[test]
    fn materializer_enforces_an_aggregate_serialized_byte_budget() {
        assert_eq!(
            checked_materialized_bytes(MAX_MATERIALIZED_ARCHIVE_BYTES - 1, 1).unwrap(),
            MAX_MATERIALIZED_ARCHIVE_BYTES
        );
        assert_eq!(
            checked_materialized_bytes(MAX_MATERIALIZED_ARCHIVE_BYTES, 1)
                .unwrap_err()
                .code,
            "archive_materialization_limit_exceeded"
        );
        assert_eq!(MAX_ARCHIVE_RECORD_BYTES, 64 * 1024 * 1024);
    }

    #[test]
    fn immutable_install_is_idempotent_and_rejects_conflicts() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("nested").join("record.json");
        assert!(install_immutable(directory.path(), &path, b"first", 100).unwrap());
        assert!(!install_immutable(directory.path(), &path, b"first", 100).unwrap());
        let error = install_immutable(directory.path(), &path, b"second", 100).unwrap_err();
        assert_eq!(error.code, "archive_immutable_conflict");
    }

    #[cfg(unix)]
    #[test]
    fn archive_loader_rejects_symlinked_shards() {
        use std::os::unix::fs::symlink;

        let archive = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        fs::create_dir(archive.path().join(INVOCATION_DIRECTORY)).unwrap();
        symlink(
            outside.path(),
            archive.path().join(INVOCATION_DIRECTORY).join("01"),
        )
        .unwrap();
        let error = load_archive(archive.path()).unwrap_err();
        assert_eq!(error.code, "archive_layout_invalid");
    }

    #[cfg(unix)]
    #[test]
    fn archive_loader_rejects_symlinked_object_ancestors() {
        use std::os::unix::fs::symlink;

        let archive = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let record = crate::record::tests::valid_record_fixture();
        let receipt = publish_record(archive.path(), &record).unwrap();
        let object_path = archive.path().join(receipt.object_relative_path);
        let shard = object_path.parent().unwrap();
        let moved_shard = outside.path().join("object-shard");
        fs::rename(shard, &moved_shard).unwrap();
        symlink(&moved_shard, shard).unwrap();

        let error = load_archive(archive.path()).unwrap_err();
        assert_eq!(error.code, "archive_layout_invalid");
    }

    #[cfg(unix)]
    #[test]
    fn archive_publisher_rejects_symlinked_internal_ancestors() {
        use std::os::unix::fs::symlink;

        let archive = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        preflight_archive_publication(archive.path()).unwrap();
        let sha256_root = archive.path().join(OBJECT_DIRECTORY).join(SHA256_DIRECTORY);
        let displaced = outside.path().join("sha256");
        fs::rename(&sha256_root, &displaced).unwrap();
        symlink(&displaced, &sha256_root).unwrap();
        let before = fs::read_dir(&displaced).unwrap().count();

        let error = publish_record(
            archive.path(),
            &crate::record::tests::valid_record_fixture(),
        )
        .unwrap_err();

        assert_eq!(error.code, "archive_layout_invalid");
        assert_eq!(fs::read_dir(&displaced).unwrap().count(), before);
    }
}
