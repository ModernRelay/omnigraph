//! Bounded referenced-file capture; the service owns configuration validation.
use super::*;
use std::collections::BTreeMap;
use std::fs::File;
use std::path::PathBuf;

const MAX_FILE: u64 = 2 * 1024 * 1024;
const MAX_UPLOAD: usize = 32 * 1024 * 1024;
const MAX_FILES: usize = 4096;

fn invalid(detail: &str) -> Failure {
    Failure::refused("config_capture_failed", detail)
}

fn logical(path: &str) -> Result<String> {
    let path = path.trim_start_matches("./").trim_end_matches('/');
    if path.is_empty()
        || path.len() > 512
        || path.starts_with('/')
        || path.contains(['\\', ':'])
        || path.chars().any(char::is_control)
        || path
            .split('/')
            .any(|p| p.is_empty() || p == "." || p == "..")
        || matches!(
            path.split('/').next(),
            Some(".omnigraph" | ".git" | "__cluster")
        )
    {
        return Err(invalid(
            "references must be bounded relative paths inside the config directory, outside private context and storage directories",
        ));
    }
    Ok(path.into())
}

struct Capture {
    root: PathBuf,
    #[cfg(unix)]
    directory: File,
    started: std::time::Instant,
    files: BTreeMap<String, String>,
    total: usize,
}

impl Capture {
    fn check(&self) -> Result<()> {
        if self.started.elapsed() > Duration::from_secs(120) {
            return Err(invalid("config capture exceeded 120 seconds"));
        }
        Ok(())
    }

    #[cfg(unix)]
    fn open(&self, path: &str) -> Result<File> {
        use std::os::fd::{AsRawFd as _, FromRawFd as _};
        let mut directory = self
            .directory
            .try_clone()
            .map_err(|_| invalid("cannot read config directory"))?;
        let parts: Vec<_> = path.split('/').collect();
        for (index, part) in parts.iter().enumerate() {
            let name = std::ffi::CString::new(*part).map_err(|_| invalid("invalid path"))?;
            let flags = libc::O_RDONLY
                | libc::O_CLOEXEC
                | libc::O_NOFOLLOW
                | libc::O_NONBLOCK
                | if index + 1 < parts.len() {
                    libc::O_DIRECTORY
                } else {
                    0
                };
            // Each open is relative to an already-open directory. Renames or
            // symlink replacement cannot redirect a read outside that root.
            let fd = unsafe { libc::openat(directory.as_raw_fd(), name.as_ptr(), flags) };
            if fd < 0 {
                return Err(invalid(
                    "a referenced path is missing, unreadable or a symbolic link",
                ));
            }
            // openat returned a new owned descriptor on success.
            directory = unsafe { File::from_raw_fd(fd) };
        }
        Ok(directory)
    }

    #[cfg(not(unix))]
    fn open(&self, _path: &str) -> Result<File> {
        Err(invalid(
            "managed upload requires Unix descriptor-relative capture on this release",
        ))
    }

    fn file(&mut self, path: &str) -> Result<()> {
        self.check()?;
        let path = logical(path)?;
        if self.files.contains_key(&path) {
            return Ok(());
        }
        if self.files.len() >= MAX_FILES {
            return Err(invalid("config exceeds 4096 files"));
        }
        let file = self.open(&path)?;
        let metadata = file
            .metadata()
            .map_err(|_| invalid("file metadata unavailable"))?;
        if !metadata.is_file() || metadata.len() > MAX_FILE {
            return Err(invalid(
                "references must be regular files at most 2 MiB each",
            ));
        }
        let mut bytes = Vec::new();
        file.take(MAX_FILE + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| invalid("referenced file unreadable"))?;
        self.total = self.total.saturating_add(bytes.len());
        if bytes.len() as u64 > MAX_FILE || self.total > MAX_UPLOAD {
            return Err(invalid("config exceeds its byte bounds"));
        }
        let text =
            String::from_utf8(bytes).map_err(|_| invalid("referenced files must be UTF-8"))?;
        self.files.insert(path, text);
        self.check()
    }

    fn discover(&mut self, path: &str) -> Result<()> {
        let path = logical(path)?;
        if !self
            .open(&path)?
            .metadata()
            .map_err(|_| invalid("query path unreadable"))?
            .is_dir()
        {
            return self.file(&path);
        }
        let mut names = Vec::new();
        for (count, entry) in std::fs::read_dir(self.root.join(&path))
            .map_err(|_| invalid("query directory unreadable"))?
            .enumerate()
        {
            self.check()?;
            if count >= MAX_FILES {
                return Err(invalid("query directory exceeds 4096 entries"));
            }
            let name = entry
                .map_err(|_| invalid("query directory entry unreadable"))?
                .file_name();
            let name = name
                .to_str()
                .ok_or_else(|| invalid("query filenames must be UTF-8"))?;
            if name.ends_with(".gq") {
                names.push(format!("{path}/{name}"));
            }
        }
        names.sort();
        for name in names {
            self.file(&name)?;
        }
        Ok(())
    }

    fn references(&mut self, yaml: &serde_yaml::Value) -> Result<()> {
        use serde_yaml::Value as Yaml;
        if let Some(graphs) = yaml.get("graphs").and_then(Yaml::as_mapping) {
            for graph in graphs.values() {
                if let Some(path) = graph.get("schema").and_then(Yaml::as_str) {
                    self.file(path)?;
                }
                match graph.get("queries") {
                    Some(Yaml::String(path)) => self.discover(path)?,
                    Some(Yaml::Sequence(paths)) => {
                        for path in paths {
                            self.discover(
                                path.as_str()
                                    .ok_or_else(|| invalid("query references must be paths"))?,
                            )?;
                        }
                    }
                    Some(Yaml::Mapping(queries)) => {
                        for query in queries.values() {
                            if let Some(path) = query.get("file").and_then(Yaml::as_str) {
                                self.file(path)?;
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        if let Some(policies) = yaml.get("policies").and_then(Yaml::as_mapping) {
            for policy in policies.values() {
                if let Some(path) = policy.get("file").and_then(Yaml::as_str) {
                    self.file(path)?;
                }
            }
        }
        Ok(())
    }
}

pub(super) fn request(config: &Path, revision: &str, message: &str) -> Result<Value> {
    let root = config
        .canonicalize()
        .map_err(|_| invalid("config directory missing"))?;
    if !root.is_dir() {
        return Err(invalid("config directory required"));
    }
    #[cfg(unix)]
    let directory = File::open(&root).map_err(|_| invalid("config directory unreadable"))?;
    let mut capture = Capture {
        root,
        #[cfg(unix)]
        directory,
        started: std::time::Instant::now(),
        files: BTreeMap::new(),
        total: 0,
    };
    capture.file("cluster.yaml")?;
    let yaml = serde_yaml::from_str(&capture.files["cluster.yaml"])
        .map_err(|_| invalid("cluster.yaml is not valid YAML"))?;
    capture.references(&yaml)?;
    let request = json!({"expected_revision":revision,"message":message,"files":capture.files});
    if serde_json::to_vec(&request)
        .map_err(|_| invalid("cannot encode upload"))?
        .len()
        > MAX_UPLOAD
    {
        return Err(invalid("encoded upload exceeds 32 MiB"));
    }
    Ok(request)
}
