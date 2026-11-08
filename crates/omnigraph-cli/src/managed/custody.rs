//! Private, bounded, atomic operator-home files. These are local custody, not authority.
use serde::{Serialize, de::DeserializeOwned};
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::Path;

pub(super) fn ensure_directory(path: &Path) -> io::Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => Ok(()),
        Ok(_) => Err(io::Error::other("custody path is not a directory")),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if let Some(parent) = path.parent() {
                ensure_directory(parent)?;
            }
            match std::fs::create_dir(path) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error),
            }
            let metadata = std::fs::symlink_metadata(path)?;
            if !metadata.is_dir() || metadata.file_type().is_symlink() {
                return Err(io::Error::other("custody path is not a directory"));
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
            }
            if let Some(parent) = path.parent() {
                crate::sync_dir(parent)?;
            }
            Ok(())
        }
        Err(error) => Err(error),
    }
}

pub(super) fn open(path: &Path, write: bool) -> io::Result<File> {
    let mut options = std::fs::OpenOptions::new();
    options
        .read(true)
        .write(write)
        .create(write)
        .truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .mode(0o600);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        options.custom_flags(0x0020_0000);
    }
    if std::fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_symlink() || !m.is_file()) {
        return Err(io::Error::other("custody file is not regular"));
    }
    let file = options.open(path)?;
    if !file.metadata()?.is_file() {
        return Err(io::Error::other("custody file is not regular"));
    }
    Ok(file)
}

pub(super) fn read<T: DeserializeOwned>(
    dir: &Path,
    name: &str,
    limit: u64,
) -> io::Result<Option<T>> {
    let path = dir.join(name);
    match std::fs::symlink_metadata(&path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
        Ok(_) => {}
    }
    let mut bytes = Vec::new();
    open(&path, false)?
        .take(limit + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        return Err(io::Error::other("custody size limit exceeded"));
    }
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(io::Error::other)
}

pub(super) fn lock(dir: &Path, name: &str) -> io::Result<File> {
    ensure_directory(dir)?;
    let file = open(&dir.join(name), true)?;
    file.try_lock().map_err(io::Error::from)?;
    Ok(file)
}

pub(super) fn save(dir: &Path, name: &str, value: &impl Serialize, limit: u64) -> io::Result<()> {
    let bytes = serde_json::to_vec(value).map_err(io::Error::other)?;
    if bytes.len() as u64 > limit {
        return Err(io::Error::other("custody size limit exceeded"));
    }
    let mut temporary = tempfile::NamedTempFile::new_in(dir)?;
    temporary.write_all(&bytes)?;
    temporary.as_file().sync_all()?;
    temporary
        .persist(dir.join(name))
        .map_err(io::Error::other)?;
    crate::sync_dir(dir)
}
