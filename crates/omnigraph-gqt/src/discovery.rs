use std::path::{Path, PathBuf};

/// Recursively lists visible regular `.gqt` files and entries the test
/// target would silently skip. Hidden non-case files and directories are
/// ignored; hidden case files, symlinks, non-UTF-8 names and unreadable
/// directories are reported as foreign. The target's harness pattern and
/// `scripts/check-fix-regression.py` mirror these path rules.
pub fn list_cases(root: &Path) -> (Vec<PathBuf>, Vec<String>) {
    if !std::fs::symlink_metadata(root).is_ok_and(|metadata| metadata.is_dir()) {
        return (
            Vec::new(),
            vec![format!(
                "{}: case root is not a regular directory",
                root.display()
            )],
        );
    }
    let mut files = Vec::new();
    let mut foreign = Vec::new();
    let mut directories = vec![root.to_path_buf()];
    while let Some(directory) = directories.pop() {
        let entries = match std::fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) => {
                foreign.push(format!("{}: {error}", directory.display()));
                continue;
            }
        };
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(error) => {
                    foreign.push(format!("{}: {error}", directory.display()));
                    continue;
                }
            };
            let path = entry.path();
            let relative = path.strip_prefix(root).unwrap_or(&path);
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                foreign.push(relative.to_string_lossy().into_owned());
                continue;
            };
            let kind = match entry.file_type() {
                Ok(kind) => kind,
                Err(error) => {
                    foreign.push(format!("{}: {error}", relative.display()));
                    continue;
                }
            };
            let is_gqt = path.extension().and_then(|s| s.to_str()) == Some("gqt");
            if name.starts_with('.') && (kind.is_dir() || !is_gqt) {
                continue;
            }
            if kind.is_dir() {
                directories.push(path);
            } else if kind.is_file() && is_gqt && !name.starts_with('.') {
                files.push(path);
            } else {
                foreign.push(relative.to_string_lossy().into_owned());
            }
        }
    }
    files.sort();
    foreign.sort();
    (files, foreign)
}
