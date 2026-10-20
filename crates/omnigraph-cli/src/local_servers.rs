//! Local server records: one directory per OS account, holding one record
//! per live local server, so a terminal, an agent and a GUI find the same
//! local servers.
//!
//! A record's `.lock` is held by its local server for its whole life; a reader
//! that can take it has found a dead server. Liveness never consults the
//! process ID. A record from another machine (a home shared over a network) is
//! ignored and never touched, so a machine whose host identifier cannot be
//! read refuses its records. A destination's `.start` lock orders local server
//! starts for that destination on one machine; it is not writer fencing.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use color_eyre::Result;
use omnigraph_api_types::ErrorCode;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::commands::local::output::InternalSchemaOutput;
use crate::helpers::CliRefusal;

/// The records format this CLI reads and writes. A reader ignores a record of
/// another format, which hides that live server from it, and ignores fields
/// it does not know; so a field a reader can do without joins the current
/// format, and only a change an old reader would misread takes a new one.
pub(crate) const RECORD_FORMAT: u32 = 1;

/// Overrides the records directory for an isolated environment, such as a
/// test. A client that sets it sees only the local servers started with the
/// same value.
pub(crate) const RECORDS_DIR_ENV: &str = "OMNIGRAPH_LOCAL_SERVERS_DIR";

fn unsafe_records(why: impl std::fmt::Display) -> color_eyre::Report {
    CliRefusal::report(
        ErrorCode::LocalServerRecordsUnsafe,
        format!("the local server records directory is not safe to use: {why}"),
    )
}

/// The account's home directory from the account database, never `HOME`.
#[cfg(unix)]
fn account_home() -> Result<PathBuf> {
    use std::ffi::CStr;
    use std::os::unix::ffi::OsStrExt;
    let mut record = std::mem::MaybeUninit::<libc::passwd>::uninit();
    let mut result = std::ptr::null_mut();
    let mut buffer = vec![0u8; 128 * 1024];
    // SAFETY: the bounded buffer and output record remain alive until pw_dir is copied.
    let status = unsafe {
        libc::getpwuid_r(
            libc::geteuid(),
            record.as_mut_ptr(),
            buffer.as_mut_ptr().cast(),
            buffer.len(),
            &mut result,
        )
    };
    if status != 0 || result.is_null() {
        return Err(unsafe_records("the account database has no home directory"));
    }
    // SAFETY: getpwuid_r succeeded and returned the initialized record above.
    let record = unsafe { record.assume_init() };
    if record.pw_dir.is_null() {
        return Err(unsafe_records("the account database has no home directory"));
    }
    // SAFETY: pw_dir is a NUL-terminated string inside the live getpwuid_r buffer.
    let path = unsafe { CStr::from_ptr(record.pw_dir) };
    Ok(PathBuf::from(std::ffi::OsStr::from_bytes(path.to_bytes())))
}

/// The per-account records directory.
pub(crate) fn records_dir() -> Result<PathBuf> {
    if let Some(dir) = std::env::var_os(RECORDS_DIR_ENV).filter(|dir| !dir.is_empty()) {
        return Ok(PathBuf::from(dir));
    }
    #[cfg(target_os = "macos")]
    {
        Ok(account_home()?.join("Library/Application Support/OmniGraph/servers"))
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        Ok(account_home()?.join(".local/state/omnigraph/servers"))
    }
    #[cfg(windows)]
    {
        let base = std::env::var_os("LOCALAPPDATA")
            .ok_or_else(|| unsafe_records("the local application data folder is unknown"))?;
        Ok(PathBuf::from(base).join("OmniGraph").join("servers"))
    }
}

/// Only Unix permissions are checked, so every other platform refuses: a
/// record holds a bearer token.
#[cfg(not(unix))]
fn ensure_dir(dir: &Path) -> Result<()> {
    Err(unsafe_records(format!(
        "{}: this platform cannot yet restrict local server records to this account",
        dir.display()
    )))
}

/// Create the directory owner-only, and refuse one another account owns or
/// that grants group or other access.
#[cfg(unix)]
fn ensure_dir(dir: &Path) -> Result<()> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder
        .create(dir)
        .map_err(|error| unsafe_records(format!("{}: {error}", dir.display())))?;
    let metadata = std::fs::symlink_metadata(dir)
        .map_err(|error| unsafe_records(format!("{}: {error}", dir.display())))?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(unsafe_records(format!(
            "{} is not a directory",
            dir.display()
        )));
    }
    {
        use std::os::unix::fs::MetadataExt;
        // SAFETY: geteuid has no preconditions.
        if metadata.uid() != unsafe { libc::geteuid() } || metadata.mode() & 0o077 != 0 {
            return Err(unsafe_records(format!(
                "{} must be owned by this account with no group or other access",
                dir.display()
            )));
        }
    }
    Ok(())
}

/// Open a file in the records directory without following a symlink, owner
/// only.
fn open_file(path: &Path, create: bool) -> Result<File> {
    if std::fs::symlink_metadata(path).is_ok_and(|m| !m.is_file() || m.file_type().is_symlink()) {
        return Err(unsafe_records(format!(
            "{} is not a regular file",
            path.display()
        )));
    }
    let mut options = OpenOptions::new();
    options
        .read(true)
        .write(true)
        .create(create)
        .truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        options.custom_flags(0x0020_0000); // FILE_FLAG_OPEN_REPARSE_POINT
    }
    let file = options
        .open(path)
        .map_err(|error| unsafe_records(format!("{}: {error}", path.display())))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let metadata = file.metadata()?;
        // SAFETY: geteuid has no preconditions.
        if metadata.uid() != unsafe { libc::geteuid() } || metadata.mode() & 0o077 != 0 {
            return Err(unsafe_records(format!(
                "{} must be owned by this account and readable by it alone",
                path.display()
            )));
        }
    }
    Ok(file)
}

/// The platform's host identifier, so a record written on another machine
/// sharing this home is recognised and left alone; `None` when the platform
/// reports none.
fn host_id() -> Option<String> {
    #[cfg(target_os = "macos")]
    {
        let mut id = [0u8; 16];
        let timeout = libc::timespec {
            tv_sec: 1,
            tv_nsec: 0,
        };
        // SAFETY: gethostuuid writes exactly 16 bytes into `id`.
        if unsafe { libc::gethostuuid(id.as_mut_ptr(), &timeout) } == 0 {
            return Some(id.iter().map(|byte| format!("{byte:02X}")).collect());
        }
    }
    #[cfg(target_os = "linux")]
    {
        for path in ["/etc/machine-id", "/var/lib/dbus/machine-id"] {
            if let Ok(id) = std::fs::read_to_string(path) {
                let id = id.trim();
                if !id.is_empty() {
                    return Some(id.to_string());
                }
            }
        }
    }
    None
}

/// The machine whose records a reader or writer may touch. Without a host
/// identifier, this machine's records cannot be told from another machine's
/// in a shared home, which would then be reaped or reused, so records refuse.
/// A valid identifier is 32 hex digits and not all zeros, which both
/// sources produce: a Linux machine-id (machine-id(5)) and macOS's 16-byte
/// host UUID. Anything else, such as systemd's `uninitialized` first-boot
/// marker, could be shared by two machines, so it refuses too.
fn this_machine(host: Option<String>) -> Result<String> {
    host.map(|id| id.trim().to_ascii_lowercase())
        .filter(|id| {
            id.len() == 32
                && id.bytes().all(|byte| byte.is_ascii_hexdigit())
                && id.bytes().any(|byte| byte != b'0')
        })
        .ok_or_else(|| {
            unsafe_records(
                "this machine's host identifier is missing or is not a machine id, so its records cannot be told from another machine's",
            )
        })
}

/// What a local server serves, identified by everything that selects its
/// storage. Two targets naming the same storage are one destination whatever
/// their names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Destination {
    pub(crate) scope: &'static str,
    /// The canonical root, for display and for the record.
    pub(crate) root: String,
    /// The same root as a path, for opening.
    pub(crate) path: PathBuf,
    pub(crate) digest: String,
}

impl Destination {
    /// The destination of a direct target's root. Only a local root names its
    /// storage completely today; a cloud root's store settings still come from
    /// the environment, so it refuses.
    pub(crate) fn of(scope: &'static str, uri: &str) -> Result<Self> {
        let path = match uri.split_once("://") {
            Some(("file", rest)) => PathBuf::from(rest),
            Some(_) => {
                return Err(CliRefusal::report(
                    ErrorCode::LocalServerDestinationUnknown,
                    format!(
                        "the target's binding does not name every setting that selects the storage of {uri}; a local server serves a local root"
                    ),
                ));
            }
            None => PathBuf::from(uri),
        };
        let canonical = std::fs::canonicalize(&path).map_err(|error| {
            CliRefusal::report(
                ErrorCode::LocalServerDestinationUnknown,
                format!(
                    "cannot resolve the target's root {}: {error}",
                    path.display()
                ),
            )
        })?;
        let root = format!("file://{}", canonical.display());
        let identity = serde_json::json!({"scope": scope, "root": root});
        let digest = format!(
            "sha256:{:x}",
            Sha256::digest(identity.to_string().as_bytes())
        );
        Ok(Self {
            scope,
            root,
            path: canonical,
            digest,
        })
    }

    fn file_stem(&self) -> String {
        self.digest.trim_start_matches("sha256:").to_string()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CliIdentity {
    pub(crate) path: String,
    pub(crate) version: String,
    pub(crate) channel: Option<String>,
    pub(crate) commit: Option<String>,
    pub(crate) http_contract: String,
    /// The storage formats it writes and serves, as `version --json` reports
    /// them. A record without them, which a reader of the same format still
    /// counts as live, names no formats a reuse could match.
    #[serde(default)]
    pub(crate) internal_schema: Option<InternalSchemaOutput>,
}

impl CliIdentity {
    pub(crate) fn current() -> Self {
        Self {
            path: std::env::current_exe()
                .map(|path| path.display().to_string())
                .unwrap_or_default(),
            version: crate::commands::local::output::PACKAGE_VERSION.clone(),
            channel: omnigraph_api_types::build::RELEASE_CHANNEL.map(str::to_string),
            commit: omnigraph_api_types::build::SOURCE_COMMIT.map(str::to_string),
            http_contract: omnigraph_api_types::HTTP_API_CONTRACT.to_string(),
            internal_schema: Some(InternalSchemaOutput::current()),
        }
    }
}

/// One live local server, as its record holds it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Record {
    pub(crate) format: u32,
    pub(crate) id: String,
    pub(crate) machine: String,
    pub(crate) root: String,
    pub(crate) destination: String,
    pub(crate) scope: String,
    pub(crate) binding: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) graph_id: Option<String>,
    pub(crate) url: String,
    pub(crate) token: String,
    pub(crate) access: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) actor: Option<String>,
    pub(crate) pid: u32,
    pub(crate) started_at: String,
    pub(crate) cli: CliIdentity,
}

/// The records directory, created and checked.
pub(crate) struct Records {
    dir: PathBuf,
    machine: String,
}

/// A held lock file: the election lock while a start runs, or a record's
/// liveness lock for the local server's life. Released on drop.
pub(crate) struct HeldLock {
    _file: File,
}

impl Records {
    pub(crate) fn open() -> Result<Self> {
        Self::open_at(records_dir()?, host_id())
    }

    /// The records directory `dir` for a start on the machine `host` names,
    /// refused before anything is created when `host` names none.
    fn open_at(dir: PathBuf, host: Option<String>) -> Result<Self> {
        let machine = this_machine(host)?;
        ensure_dir(&dir)?;
        Ok(Self { dir, machine })
    }

    /// The records directory for a reader, which creates nothing: `None`
    /// when no local server has written it yet.
    pub(crate) fn existing() -> Result<Option<Self>> {
        Self::existing_at(records_dir()?, host_id())
    }

    /// A missing directory holds no record to tell apart, so only a directory
    /// that exists refuses a missing host identifier.
    fn existing_at(dir: PathBuf, host: Option<String>) -> Result<Option<Self>> {
        match std::fs::symlink_metadata(&dir) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            _ => {}
        }
        ensure_dir(&dir)?;
        let machine = this_machine(host)?;
        Ok(Some(Self { dir, machine }))
    }

    pub(crate) fn machine(&self) -> &str {
        &self.machine
    }

    pub(crate) fn record_path(&self, id: &str) -> PathBuf {
        self.dir.join(format!("{id}.json"))
    }

    fn lock_path(&self, id: &str) -> PathBuf {
        self.dir.join(format!("{id}.lock"))
    }

    /// Take a new record's liveness lock, before anything announces it.
    pub(crate) fn hold(&self, id: &str) -> Result<HeldLock> {
        let file = open_file(&self.lock_path(id), true)?;
        match file.try_lock() {
            Ok(()) => Ok(HeldLock { _file: file }),
            Err(_) => Err(unsafe_records(format!("{id}.lock is already held"))),
        }
    }

    /// Take the destination's election lock, waiting at most `wait`. While
    /// waiting, a live record for the destination ends the wait: the start
    /// that held the lock published it.
    pub(crate) fn elect(
        &self,
        destination: &Destination,
        wait: Duration,
    ) -> Result<std::result::Result<HeldLock, Record>> {
        let file = open_file(
            &self.dir.join(format!("{}.start", destination.file_stem())),
            true,
        )?;
        let deadline = Instant::now() + wait;
        loop {
            match file.try_lock() {
                Ok(()) => return Ok(Ok(HeldLock { _file: file })),
                Err(std::fs::TryLockError::WouldBlock) => {
                    if let Some(record) = self.live(destination)? {
                        return Ok(Err(record));
                    }
                    if Instant::now() >= deadline {
                        return Err(CliRefusal::report(
                            ErrorCode::LocalServerStarting,
                            "another local server for this destination is still starting; retry when it is ready",
                        ));
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
                Err(std::fs::TryLockError::Error(error)) => {
                    return Err(unsafe_records(error));
                }
            }
        }
    }

    /// Every live record of this machine. A dead record (its lock free) is
    /// removed; a record from another machine, of an unknown format or
    /// without its lock file is ignored and kept.
    pub(crate) fn live_records(&self) -> Result<Vec<Record>> {
        let mut live = Vec::new();
        let entries = std::fs::read_dir(&self.dir)
            .map_err(|error| unsafe_records(format!("{}: {error}", self.dir.display())))?;
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let Ok(file) = open_file(&path, false) else {
                continue;
            };
            let Ok(record) = serde_json::from_reader::<_, Record>(file) else {
                continue;
            };
            if record.format != RECORD_FORMAT || record.machine != self.machine {
                continue;
            }
            // Only a record's owner creates its lock. A lock created here
            // would be a new file whose lock proves nothing, while the owner
            // may still hold the old one under a path since unlinked.
            let Ok(lock) = open_file(&self.lock_path(&record.id), false) else {
                continue;
            };
            match lock.try_lock() {
                Ok(()) => {
                    // Its owner is gone: no process holds the lock.
                    let _ = std::fs::remove_file(&path);
                    let _ = std::fs::remove_file(self.lock_path(&record.id));
                }
                Err(std::fs::TryLockError::WouldBlock) => live.push(record),
                Err(std::fs::TryLockError::Error(_)) => {}
            }
        }
        live.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(live)
    }

    /// The live local server of a destination, if any.
    pub(crate) fn live(&self, destination: &Destination) -> Result<Option<Record>> {
        Ok(self
            .live_records()?
            .into_iter()
            .find(|record| record.destination == destination.digest))
    }

    /// Publish a record: a sibling written owner-only, then renamed over.
    pub(crate) fn publish(&self, record: &Record) -> Result<()> {
        let path = self.record_path(&record.id);
        let tmp = self.dir.join(format!("{}.json.tmp", record.id));
        let mut file = open_file(&tmp, true)?;
        file.set_len(0)?;
        file.write_all(&serde_json::to_vec_pretty(record)?)?;
        file.sync_all()?;
        std::fs::rename(&tmp, &path)?;
        crate::sync_dir(&self.dir)?;
        Ok(())
    }

    /// Remove a record and its lock file, when its local server stops.
    pub(crate) fn remove(&self, id: &str) {
        let _ = std::fs::remove_file(self.record_path(id));
        let _ = std::fs::remove_file(self.lock_path(id));
    }
}

#[cfg(test)]
mod tests {
    /// A host identifier in the shape `host_id` reads.
    const THIS_MACHINE: &str = "0123456789abcdef0123456789abcdef";
    const ANOTHER_MACHINE: &str = "fedcba9876543210fedcba9876543210";
    use super::*;

    #[test]
    fn a_record_is_live_while_its_lock_is_held_and_removed_after() {
        let dir = tempfile::tempdir().unwrap();
        let records_dir = dir.path().join("servers");
        let records = Records::open_at(records_dir, Some(THIS_MACHINE.into())).unwrap();
        let graph = dir.path().join("g.omni");
        std::fs::create_dir(&graph).unwrap();
        let destination = Destination::of("graph", graph.to_str().unwrap()).unwrap();
        assert_eq!(
            Destination::of("graph", &format!("file://{}", graph.display()))
                .unwrap()
                .digest,
            destination.digest,
            "a path and its file:// URI are one destination"
        );
        let record = Record {
            format: RECORD_FORMAT,
            id: "01TEST".into(),
            machine: records.machine().to_string(),
            root: destination.root.clone(),
            destination: destination.digest.clone(),
            scope: "graph".into(),
            binding: "sha256:b".into(),
            graph_id: Some("default".into()),
            url: "http://127.0.0.1:1".into(),
            token: "t".into(),
            access: "read_only".into(),
            actor: Some("default".into()),
            pid: std::process::id(),
            started_at: "now".into(),
            cli: CliIdentity::current(),
        };
        let held = records.hold(&record.id).unwrap();
        records.publish(&record).unwrap();
        assert_eq!(records.live(&destination).unwrap().unwrap().id, "01TEST");
        // A record from another machine is ignored and kept.
        let mut foreign = record.clone();
        foreign.id = "01FOREIGN".into();
        foreign.machine = "elsewhere".into();
        records.publish(&foreign).unwrap();
        assert_eq!(records.live_records().unwrap().len(), 1);
        assert!(records.record_path("01FOREIGN").exists());
        // A record without its lock file is not live. A reader neither
        // creates the lock nor removes the record: its owner may still hold
        // a lock whose path was unlinked.
        let mut unlocked = record.clone();
        unlocked.id = "01UNLOCKED".into();
        records.publish(&unlocked).unwrap();
        assert_eq!(records.live_records().unwrap().len(), 1);
        assert!(records.record_path("01UNLOCKED").exists());
        assert!(!records.lock_path("01UNLOCKED").exists());
        // Released, the record is dead and removed by the next reader.
        drop(held);
        assert!(records.live(&destination).unwrap().is_none());
        assert!(!records.record_path("01TEST").exists());

        // Cloud roots do not name their whole store yet.
        let refused = Destination::of("graph", "s3://bucket/g").unwrap_err();
        assert_eq!(
            refused.downcast_ref::<CliRefusal>().unwrap().code,
            ErrorCode::LocalServerDestinationUnknown
        );
    }

    #[cfg(unix)]
    #[test]
    fn a_records_directory_others_can_read_is_refused() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let records = dir.path().join("servers");
        std::fs::create_dir(&records).unwrap();
        std::fs::set_permissions(&records, std::fs::Permissions::from_mode(0o755)).unwrap();
        let refused = ensure_dir(&records).unwrap_err();
        assert_eq!(
            refused.downcast_ref::<CliRefusal>().unwrap().code,
            ErrorCode::LocalServerRecordsUnsafe
        );
    }

    /// Without a host identifier this machine's records cannot be told from
    /// another machine's in a shared home, so records refuse rather than reap
    /// or reuse one.
    #[test]
    fn records_refuse_a_machine_without_a_host_identifier() {
        let dir = tempfile::tempdir().unwrap();
        let records_dir = dir.path().join("servers");
        for host in [
            None,
            Some(String::new()),
            Some(" \n".to_string()),
            Some("uninitialized".to_string()),
            Some("0".repeat(32)),
            Some("0123456789abcdef".to_string()),
            Some("g123456789abcdef0123456789abcdef".to_string()),
        ] {
            let refused = Records::open_at(records_dir.clone(), host.clone())
                .err()
                .expect("a start without a host identifier refuses");
            assert_eq!(
                refused.downcast_ref::<CliRefusal>().unwrap().code,
                ErrorCode::LocalServerRecordsUnsafe
            );
            assert!(!records_dir.exists(), "a refused start creates nothing");
            // A missing directory holds no record to tell apart.
            assert!(
                Records::existing_at(records_dir.clone(), host)
                    .unwrap()
                    .is_none()
            );
        }
        // A directory a shared home holds: here, a record whose lock this
        // machine can take, as another machine's lock is invisible to it.
        let elsewhere =
            Records::open_at(records_dir.clone(), Some(ANOTHER_MACHINE.into())).unwrap();
        let graph = dir.path().join("g.omni");
        std::fs::create_dir(&graph).unwrap();
        let destination = Destination::of("graph", graph.to_str().unwrap()).unwrap();
        let record = Record {
            format: RECORD_FORMAT,
            id: "01ELSEWHERE".into(),
            machine: elsewhere.machine().to_string(),
            root: destination.root.clone(),
            destination: destination.digest.clone(),
            scope: "graph".into(),
            binding: "sha256:b".into(),
            graph_id: Some("default".into()),
            url: "http://127.0.0.1:1".into(),
            token: "t".into(),
            access: "read_only".into(),
            actor: Some("default".into()),
            pid: 1,
            started_at: "now".into(),
            cli: CliIdentity::current(),
        };
        drop(elsewhere.hold(&record.id).unwrap());
        elsewhere.publish(&record).unwrap();
        for host in [None, Some(String::new())] {
            for refused in [
                Records::existing_at(records_dir.clone(), host.clone())
                    .err()
                    .expect("a reader without a host identifier refuses"),
                Records::open_at(records_dir.clone(), host)
                    .err()
                    .expect("a start without a host identifier refuses"),
            ] {
                assert_eq!(
                    refused.downcast_ref::<CliRefusal>().unwrap().code,
                    ErrorCode::LocalServerRecordsUnsafe
                );
            }
        }
        assert!(elsewhere.record_path(&record.id).exists());
        assert!(elsewhere.lock_path(&record.id).exists());
    }

    #[test]
    fn a_second_start_waits_for_the_first_and_finds_its_record() {
        let dir = tempfile::tempdir().unwrap();
        let records =
            Records::open_at(dir.path().join("servers"), Some(THIS_MACHINE.into())).unwrap();
        let graph = dir.path().join("g.omni");
        std::fs::create_dir(&graph).unwrap();
        let destination = Destination::of("graph", graph.to_str().unwrap()).unwrap();
        let first = records.elect(&destination, Duration::from_secs(1)).unwrap();
        assert!(first.is_ok());
        // The second start times out while the first holds the election and
        // has published nothing.
        let refused = records
            .elect(&destination, Duration::from_millis(200))
            .err()
            .expect("a held election refuses after its wait");
        assert_eq!(
            refused.downcast_ref::<CliRefusal>().unwrap().code,
            ErrorCode::LocalServerStarting
        );
    }
}
