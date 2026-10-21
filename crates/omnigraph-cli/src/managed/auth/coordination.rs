//! Local credential rotation coordination; the provider owns session authority.
use super::*;
use sha2::{Digest as _, Sha256};
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub(crate) struct LockGuard {
    file: File,
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

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

pub(super) async fn lock_at(directory: &Path, origin: &str, wait: Duration) -> Result<LockGuard> {
    let file = open(directory, origin)?;
    let deadline = Instant::now() + wait;
    loop {
        match file.try_lock() {
            Ok(()) => return Ok(LockGuard { file }),
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

pub(super) async fn lock(origin: &str) -> Result<LockGuard> {
    // Stable per-account location, independent of working folder and OMNIGRAPH_HOME.
    lock_at(
        &home()?.join(".omnigraph-session-locks"),
        &crate::operator_routing::namespace(origin),
        Duration::from_secs(40),
    )
    .await
}

pub(super) fn cache_index_lock() -> Result<LockGuard> {
    cache_index_lock_at(&home()?.join(".omnigraph-session-locks"))
}

fn cache_index_lock_at(directory: &Path) -> Result<LockGuard> {
    let file = open(
        directory,
        &format!(
            "{}:data-cache-index",
            crate::operator_routing::data_profile_namespace().ok_or_else(failed)?
        ),
    )?;
    file.try_lock().map_err(|_| failed())?;
    Ok(LockGuard { file })
}

pub(super) async fn global_session_lock() -> Result<LockGuard> {
    lock_at(
        &home()?.join(".omnigraph-session-locks"),
        "provider-session-claims",
        Duration::from_secs(40),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cache_index_lock_is_profile_wide_across_clusters_and_generations() {
        let root = tempfile::tempdir().unwrap();
        let input = crate::operator::InvocationConfig::at(Some(root.path().to_owned()), None);
        let first =
            crate::operator_routing::registration_auth(&input, "first", "https://api.example")
                .unwrap();
        let other =
            crate::operator_routing::registration_auth(&input, "other", "https://api.example")
                .unwrap();
        let directory = root.path().join("locks");
        let held = crate::operator_routing::AUTH
            .scope(Some(first.clone()), async {
                cache_index_lock_at(&directory).unwrap()
            })
            .await;
        #[cfg(unix)]
        let inherited = held.file.try_clone().unwrap();
        first.invalidate().unwrap();
        let renewed = crate::operator_routing::registration_auth(
            &input,
            "first",
            "https://replacement.example",
        )
        .unwrap();
        crate::operator_routing::AUTH
            .scope(Some(renewed), async {
                assert!(cache_index_lock_at(&directory).is_err());
            })
            .await;
        crate::operator_routing::AUTH
            .scope(Some(other), async {
                assert!(cache_index_lock_at(&directory).is_ok());
            })
            .await;
        drop(held);
        crate::operator_routing::AUTH
            .scope(Some(first), async {
                assert!(cache_index_lock_at(&directory).is_ok());
            })
            .await;
        #[cfg(unix)]
        drop(inherited);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn guard_drop_unlocks_while_a_duplicate_description_remains_open() {
        let root = tempfile::tempdir().unwrap();
        let directory = root.path().join("locks");
        let held = lock_at(&directory, "origin", Duration::ZERO).await.unwrap();
        let inherited = held.file.try_clone().unwrap();
        assert!(lock_at(&directory, "origin", Duration::ZERO).await.is_err());
        drop(held);
        let successor = lock_at(&directory, "origin", Duration::ZERO).await.unwrap();
        drop(inherited);
        assert!(lock_at(&directory, "origin", Duration::ZERO).await.is_err());
        drop(successor);
        assert!(lock_at(&directory, "origin", Duration::ZERO).await.is_ok());
    }

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
