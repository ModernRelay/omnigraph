//! Local credential rotation coordination; the API owns session authority.
use super::*;
use sha2::{Digest as _, Sha256};
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

fn failed() -> Failure {
    Failure::refused(
        "session_lock_unavailable",
        "could not safely coordinate the OS session cache",
    )
}

#[cfg(unix)]
fn home() -> Result<PathBuf> {
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
        return Err(failed());
    }
    // SAFETY: getpwuid_r succeeded and returned the initialized record above.
    let record = unsafe { record.assume_init() };
    if record.pw_dir.is_null() {
        return Err(failed());
    }
    // SAFETY: pw_dir is a NUL-terminated string inside the live getpwuid_r buffer.
    let path = unsafe { CStr::from_ptr(record.pw_dir) };
    Ok(PathBuf::from(std::ffi::OsStr::from_bytes(path.to_bytes())))
}

#[cfg(not(unix))]
fn home() -> Result<PathBuf> {
    std::env::home_dir().ok_or_else(failed)
}

fn open(directory: &Path, origin: &str) -> Result<File> {
    let mut builder = std::fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    match builder.create(directory) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(_) => return Err(failed()),
    }
    let metadata = std::fs::symlink_metadata(directory).map_err(|_| failed())?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(failed());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        // SAFETY: geteuid has no preconditions.
        if metadata.uid() != unsafe { libc::geteuid() } || metadata.mode() & 0o077 != 0 {
            return Err(failed());
        }
    }
    let path = directory.join(format!("{:x}.lock", Sha256::digest(origin.as_bytes())));
    if std::fs::symlink_metadata(&path).is_ok_and(|m| !m.is_file() || m.file_type().is_symlink()) {
        return Err(failed());
    }
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        options.custom_flags(0x0020_0000); // FILE_FLAG_OPEN_REPARSE_POINT
    }
    let file = options.open(path).map_err(|_| failed())?;
    let metadata = file.metadata().map_err(|_| failed())?;
    if !metadata.is_file() {
        return Err(failed());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        // SAFETY: geteuid has no preconditions.
        if metadata.uid() != unsafe { libc::geteuid() } || metadata.mode() & 0o077 != 0 {
            return Err(failed());
        }
    }
    Ok(file)
}

async fn lock_at(directory: &Path, origin: &str, wait: Duration) -> Result<File> {
    let file = open(directory, origin)?;
    let deadline = Instant::now() + wait;
    loop {
        match file.try_lock() {
            Ok(()) => return Ok(file),
            Err(std::fs::TryLockError::WouldBlock) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(std::fs::TryLockError::WouldBlock) => {
                return Err(Failure::refused(
                    "session_busy",
                    "another process is updating this API session; retry when it finishes",
                ));
            }
            Err(_) => return Err(failed()),
        }
    }
}

pub(super) async fn lock(origin: &str) -> Result<File> {
    // Stable per-account location, independent of working folder and OMNIGRAPH_HOME.
    lock_at(
        &home()?.join(".omnigraph-session-locks"),
        origin,
        Duration::from_secs(40),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn same_origin_wait_is_bounded_and_other_origins_are_independent() {
        let root = tempfile::tempdir().unwrap();
        let directory = root.path().join("locks");
        let first = lock_at(&directory, "https://one.example", Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(
            lock_at(&directory, "https://one.example", Duration::from_millis(20))
                .await
                .unwrap_err()
                .body["type"],
            "session_busy"
        );
        let _other = lock_at(&directory, "https://two.example", Duration::ZERO)
            .await
            .unwrap();
        drop(first);
        let _recovered = lock_at(&directory, "https://one.example", Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(std::fs::read_dir(directory).unwrap().count(), 2);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn unsafe_cache_coordination_paths_fail_closed() {
        use std::os::unix::fs::{PermissionsExt, symlink};
        let root = tempfile::tempdir().unwrap();
        let directory = root.path().join("locks");
        symlink(root.path(), &directory).unwrap();
        assert!(
            lock_at(&directory, "https://one.example", Duration::ZERO)
                .await
                .is_err()
        );
        std::fs::remove_file(&directory).unwrap();
        std::fs::create_dir(&directory).unwrap();
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(
            lock_at(&directory, "https://one.example", Duration::ZERO)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn concurrent_refresh_waiter_rereads_the_published_rotation() {
        use crate::managed::auth::tests::MemoryStore;
        use crate::managed_http_fixture::{IntentApiFixture, IntentReply};
        let deadline = (OffsetDateTime::now_utc() + time::Duration::hours(1))
            .format(&Rfc3339)
            .unwrap();
        let access_expiry = (OffsetDateTime::now_utc() - time::Duration::seconds(1))
            .format(&Rfc3339)
            .unwrap();
        let api = IntentApiFixture::with_response_delay(
            vec![IntentReply::json(
                200,
                json!({"data":{"access_token":"new-access","token_type":"Bearer","expires_at":(OffsetDateTime::now_utc()+time::Duration::minutes(2)).format(&Rfc3339).unwrap(),"refresh_token":"new-refresh","refresh_expires_at":deadline,"principal_id":"p","subject":"s","account_id":"a","scopes":{}},"meta":{}}),
            )],
            Duration::from_millis(100),
        );
        let root = tempfile::tempdir().unwrap();
        let directory = root.path().join("locks");
        let current = MemoryStore::default();
        let legacy = MemoryStore::default();
        current.put(&api.origin, &json!({"version":2,"access_token":"old-access","expires_at":access_expiry,"refresh_token":"old-refresh","refresh_expires_at":deadline,"identity":{"principal_id":"p","subject":"s","account_id":"a"},"state":"ready"}).to_string()).unwrap();
        let acquire = || async {
            let _lock = lock_at(&directory, &api.origin, Duration::from_secs(2))
                .await
                .unwrap();
            renewal::credential(&legacy, &current, &api.origin)
                .await
                .unwrap()
        };
        let (first, second) = tokio::join!(acquire(), acquire());
        assert_eq!(first, "new-access");
        assert_eq!(second, first);
        assert_eq!(api.requests().len(), 1);
        api.assert_complete();
    }

    #[test]
    fn another_process_observes_the_same_origin_lock() {
        const CHILD_ROOT: &str = "OMNIGRAPH_TEST_SESSION_LOCK_ROOT";
        if let Some(root) = std::env::var_os(CHILD_ROOT) {
            let file = open(Path::new(&root), "https://one.example").unwrap();
            assert!(matches!(
                file.try_lock(),
                Err(std::fs::TryLockError::WouldBlock)
            ));
            return;
        }
        let root = tempfile::tempdir().unwrap();
        let directory = root.path().join("locks");
        let first = open(&directory, "https://one.example").unwrap();
        first.try_lock().unwrap();
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "managed::auth::coordination::tests::another_process_observes_the_same_origin_lock",
            ])
            .env(CHILD_ROOT, &directory)
            .output()
            .unwrap();
        assert!(
            child.status.success(),
            "{}",
            String::from_utf8_lossy(&child.stderr)
        );
    }
}
