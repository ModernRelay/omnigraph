//! Read-only source checkout. Publication is all-or-nothing into a new folder.
use super::*;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

const MAX_FILES: usize = 4096;
const MAX_FILE: usize = 1024 * 1024;
const MAX_SOURCE: usize = 8 * 1024 * 1024;
const MAX_EXPORT: usize = 64 * 1024 * 1024;
const EXPORT_TIMEOUT: Duration = Duration::from_secs(120);

pub(super) async fn list(binding: &Binding) -> Result<(Value, i32)> {
    let body = binding
        .client()
        .await?
        .request(Method::GET, "/v1/clusters", None, None)
        .await?;
    let clusters = body["data"]["clusters"]
        .as_array()
        .ok_or_else(Failure::protocol)?;
    let mut seen = BTreeSet::new();
    for cluster in clusters {
        let id = cluster["cluster_id"]
            .as_str()
            .ok_or_else(Failure::protocol)?;
        let incarnation = cluster["incarnation"]
            .as_str()
            .ok_or_else(Failure::protocol)?;
        if identifier(id).is_err() || identifier(incarnation).is_err() || !seen.insert(id) {
            return Err(Failure::protocol());
        }
    }
    Ok((body, 0))
}

fn full_revision(revision: &str) -> bool {
    revision.len() == 40
        && revision
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

pub(super) async fn pull(
    context: &Context,
    out: &Path,
    revision: Option<&str>,
) -> Result<(Value, i32)> {
    if revision.is_some_and(|rev| !full_revision(rev)) {
        return Err(Failure::refused(
            "revision_invalid",
            "--rev requires a full lowercase Git commit ID",
        ));
    }
    // Refuse unsupported platforms and existing destinations before authentication or HTTP.
    let destination = destination(out)?;
    let api = context.client().await?;
    let mut path = format!("/v1/clusters/{}/config/export", context.cluster);
    if let Some(revision) = revision {
        path.push_str(&format!("?revision={revision}"));
    }
    let response = api
        .raw_bounded(
            Method::GET,
            &path,
            None,
            None,
            ResponseLimits {
                timeout: EXPORT_TIMEOUT,
                max_body: MAX_EXPORT,
                // Preserve the immutable source bytes. They are written privately and
                // removed from output; unsuccessful responses are still scrubbed.
                preserve_contents: true,
            },
        )
        .await?;
    if !response.status.is_success() {
        return Err(Failure {
            body: response.body,
            response_status: Some(response.status),
            exit: if response.status.is_client_error() {
                2
            } else {
                1
            },
        });
    }
    let mut body = response.body;
    context.verify(&body)?;
    let actual_revision = body["data"]["revision"]
        .as_str()
        .ok_or_else(Failure::protocol)?;
    let incarnation = body["meta"]["incarnation"]
        .as_str()
        .ok_or_else(Failure::protocol)?;
    if body["data"]["cluster_id"].as_str() != Some(&context.cluster)
        || identifier(incarnation).is_err()
        || !full_revision(actual_revision)
        || revision.is_some_and(|expected| actual_revision != expected)
    {
        return Err(Failure::protocol());
    }
    let files = validate_files(&body["data"]["files"])?;
    let count = files.len();
    publish(&destination, &files)?;
    // Contents belong in the destination, not terminal output or captured logs.
    body["data"]
        .as_object_mut()
        .ok_or_else(Failure::protocol)?
        .remove("files");
    body["data"]["output"] = json!(destination);
    body["data"]["file_count"] = json!(count);
    Ok((body, 0))
}

fn invalid_export() -> Failure {
    Failure::refused(
        "config_export_invalid",
        "export must contain a complete UTF-8 configuration with normalized, nonconflicting relative paths and no reserved directories; limits are 4096 files, 1 MiB per file and 8 MiB total",
    )
}

fn validate_files(value: &Value) -> Result<BTreeMap<&str, &str>> {
    let files = value.as_object().ok_or_else(invalid_export)?;
    if files.is_empty() || files.len() > MAX_FILES {
        return Err(invalid_export());
    }
    let mut result = BTreeMap::new();
    let mut directories = BTreeSet::new();
    let mut spellings = BTreeMap::new();
    let mut bytes = 0usize;
    for (path, value) in files {
        let content = value.as_str().ok_or_else(invalid_export)?;
        bytes = bytes
            .checked_add(content.len())
            .ok_or_else(invalid_export)?;
        if path.is_empty()
            || path.len() > 512
            || path.contains(['\\', ':'])
            || path.chars().any(char::is_control)
            || content.len() > MAX_FILE
            || bytes > MAX_SOURCE
        {
            return Err(invalid_export());
        }
        let components: Vec<_> = path.split('/').collect();
        for (index, component) in components.iter().enumerate() {
            if component.is_empty()
                || matches!(*component, "." | "..")
                || [".git", ".omnigraph", "__cluster"]
                    .iter()
                    .any(|reserved| component.eq_ignore_ascii_case(reserved))
            {
                return Err(invalid_export());
            }
            let prefix = components[..=index].join("/");
            if spellings
                .insert(prefix.to_lowercase(), prefix.clone())
                .is_some_and(|old| old != prefix)
            {
                return Err(invalid_export());
            }
            if index + 1 < components.len() {
                directories.insert(prefix);
            }
        }
        result.insert(path.as_str(), content);
    }
    if result.keys().any(|path| directories.contains(*path)) {
        return Err(invalid_export());
    }
    let config = result.get("cluster.yaml").ok_or_else(invalid_export)?;
    if !yaml_serde::from_str::<yaml_serde::Value>(config).is_ok_and(|yaml| yaml.is_mapping()) {
        return Err(invalid_export());
    }
    Ok(result)
}

fn destination(path: &Path) -> Result<PathBuf> {
    if !cfg!(any(target_os = "linux", target_os = "macos")) {
        return Err(Failure::refused(
            "config_pull_unsupported",
            "atomic configuration download currently requires Linux or macOS",
        ));
    }
    // Test the supplied path before lexical decomposition: trailing `/.` must
    // not be reinterpreted as a different, new child of an existing directory.
    if std::fs::symlink_metadata(path).is_ok() {
        return Err(Failure::refused(
            "config_destination_exists",
            "--out must be new; existing directories, files and symlinks are never replaced",
        ));
    }
    let filename = path
        .file_name()
        .filter(|name| !name.is_empty())
        .ok_or_else(|| {
            Failure::refused(
                "config_destination_invalid",
                "--out must name a new directory beneath an existing parent",
            )
        })?;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let parent = parent.canonicalize().map_err(|_| {
        Failure::refused(
            "config_destination_invalid",
            "--out parent must be an accessible existing directory",
        )
    })?;
    let destination = parent.join(filename);
    if destination.to_str().is_none() {
        return Err(Failure::refused(
            "config_destination_invalid",
            "--out must have a UTF-8 path so the result can report its exact destination",
        ));
    }
    match std::fs::symlink_metadata(&destination) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(destination),
        _ => Err(Failure::refused(
            "config_destination_exists",
            "--out must be new; existing directories, files and symlinks are never replaced",
        )),
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn write_failed() -> Failure {
    Failure::new(
        "config_pull_write_failed",
        "could not publish the complete configuration into the new directory; existing destinations are never replaced",
        1,
    )
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn publish(destination: &Path, files: &BTreeMap<&str, &str>) -> Result<()> {
    use std::ffi::CString;
    use std::fs::{File, OpenOptions};
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

    fn name(value: &std::ffi::OsStr) -> Result<CString> {
        CString::new(value.as_bytes()).map_err(|_| write_failed())
    }
    fn directory(parent: &File, name: &str) -> Result<File> {
        let name = CString::new(name).map_err(|_| write_failed())?;
        // Every ancestor is opened relative to an already held directory. A
        // link or special file cannot redirect writes outside the private tree.
        unsafe {
            if libc::mkdirat(parent.as_raw_fd(), name.as_ptr(), 0o700) != 0
                && std::io::Error::last_os_error().kind() != std::io::ErrorKind::AlreadyExists
            {
                return Err(write_failed());
            }
            let fd = libc::openat(
                parent.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            );
            if fd < 0 {
                return Err(write_failed());
            }
            Ok(File::from_raw_fd(fd))
        }
    }
    fn write(parent: &File, filename: &str, content: &[u8]) -> Result<()> {
        let filename = CString::new(filename).map_err(|_| write_failed())?;
        let fd = unsafe {
            libc::openat(
                parent.as_raw_fd(),
                filename.as_ptr(),
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                0o600,
            )
        };
        if fd < 0 {
            return Err(write_failed());
        }
        let mut file = unsafe { File::from_raw_fd(fd) };
        file.write_all(content)
            .and_then(|()| file.sync_all())
            .map_err(|_| write_failed())
    }

    let parent_path = destination.parent().ok_or_else(write_failed)?;
    let parent = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(parent_path)
        .map_err(|_| write_failed())?;
    let staging = tempfile::Builder::new()
        .prefix(".omnigraph-pull-")
        .permissions(std::fs::Permissions::from_mode(0o700))
        .tempdir_in(parent_path)
        .map_err(|_| write_failed())?;
    let staging_name = name(staging.path().file_name().ok_or_else(write_failed)?)?;
    let root_fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            staging_name.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if root_fd < 0 {
        return Err(write_failed());
    }
    let root = unsafe { File::from_raw_fd(root_fd) };
    // Catch platform-specific path aliases (including Unicode normalization).
    let mut directory_names = BTreeMap::new();
    for (path, contents) in files {
        let mut current = root.try_clone().map_err(|_| write_failed())?;
        let components: Vec<_> = path.split('/').collect();
        for (index, component) in components[..components.len() - 1].iter().enumerate() {
            let child = directory(&current, component)?;
            current.sync_all().map_err(|_| write_failed())?;
            let metadata = child.metadata().map_err(|_| write_failed())?;
            let spelling = components[..=index].join("/");
            if directory_names
                .insert((metadata.dev(), metadata.ino()), spelling.clone())
                .is_some_and(|old| old != spelling)
            {
                return Err(invalid_export());
            }
            current = child;
        }
        write(
            &current,
            components[components.len() - 1],
            contents.as_bytes(),
        )?;
        current.sync_all().map_err(|_| write_failed())?;
    }
    root.sync_all().map_err(|_| write_failed())?;
    let destination_name = name(destination.file_name().ok_or_else(write_failed)?)?;
    // A concurrent creator wins. Never use ordinary rename, which can replace
    // an existing empty directory after the preflight existence check.
    #[cfg(target_os = "macos")]
    let status = unsafe {
        libc::renameatx_np(
            parent.as_raw_fd(),
            staging_name.as_ptr(),
            parent.as_raw_fd(),
            destination_name.as_ptr(),
            libc::RENAME_EXCL,
        )
    };
    #[cfg(target_os = "linux")]
    let status = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            parent.as_raw_fd(),
            staging_name.as_ptr(),
            parent.as_raw_fd(),
            destination_name.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if status != 0 {
        return Err(write_failed());
    }
    // Publication succeeded. Do not remove it if syncing its parent fails.
    let _ = staging.keep();
    parent.sync_all().map_err(|_| Failure::new("config_pull_sync_failed", "configuration was published at --out but directory durability could not be confirmed; inspect that directory before retrying", 1))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn publish(_: &Path, _: &BTreeMap<&str, &str>) -> Result<()> {
    Err(Failure::refused(
        "config_pull_unsupported",
        "atomic configuration download currently requires Linux or macOS",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn export_limits_cover_raw_bytes_file_count_and_json_expansion() {
        let mut files = serde_json::Map::new();
        files.insert("cluster.yaml".into(), json!("version: 1\n"));
        for i in 0..8 {
            // Control bytes cost six JSON bytes; the HTTP cap must not reject
            // a valid source whose decoded content fits the raw-byte bound.
            let size = if i == 7 {
                MAX_FILE - "version: 1\n".len()
            } else {
                MAX_FILE
            };
            files.insert(format!("file-{i}.pg"), json!("\u{0001}".repeat(size)));
        }
        let value = Value::Object(files);
        assert_eq!(validate_files(&value).unwrap().len(), 9);
        let encoded = serde_json::to_vec(&json!({"data":{"files":value}})).unwrap();
        assert!(encoded.len() > 32 * 1024 * 1024 && encoded.len() < MAX_EXPORT);
        let mut files = value;
        files["excess.pg"] = json!("x");
        assert!(validate_files(&files).is_err());

        let mut files = serde_json::Map::new();
        files.insert("cluster.yaml".into(), json!("version: 1"));
        for i in 1..MAX_FILES {
            files.insert(format!("file-{i}.pg"), json!(""));
        }
        assert!(validate_files(&Value::Object(files.clone())).is_ok());
        files.insert("extra.pg".into(), json!(""));
        assert!(validate_files(&Value::Object(files)).is_err());
    }

    #[test]
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn atomic_publication_preserves_a_destination_created_after_preflight() {
        let temp = tempfile::tempdir().unwrap();
        let destination = destination(&temp.path().join("checkout")).unwrap();
        std::fs::create_dir(&destination).unwrap();
        let files = BTreeMap::from([("cluster.yaml", "version: 1\n")]);
        assert!(publish(&destination, &files).is_err());
        assert_eq!(std::fs::read_dir(&destination).unwrap().count(), 0);
        assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 1);
    }
}
