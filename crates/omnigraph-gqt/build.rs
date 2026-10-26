use sha2::{Digest, Sha256};
use std::process::Command;

fn main() {
    println!("cargo::rustc-check-cfg=cfg(tokio_unstable)");
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let revision = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(&root)
        .output()
        .expect("read source revision");
    assert!(revision.status.success(), "source revision unavailable");
    let revision = String::from_utf8(revision.stdout).expect("UTF-8 revision");
    let inventory = Command::new("git")
        .args([
            "ls-files",
            "--cached",
            "--others",
            "--exclude-standard",
            "-z",
            "--",
            "crates",
            "Cargo.toml",
            "Cargo.lock",
            "rust-toolchain.toml",
            ".cargo",
        ])
        .current_dir(&root)
        .output()
        .expect("read source inventory");
    assert!(inventory.status.success(), "source inventory unavailable");
    let mut paths = inventory
        .stdout
        .split(|b| *b == 0)
        .filter(|p| !p.is_empty())
        .collect::<Vec<_>>();
    paths.sort();
    paths.dedup();
    let mut hash = Sha256::new();
    for bytes in paths {
        let name = std::str::from_utf8(bytes).expect("UTF-8 source path");
        let path = root.join(name);
        if !path.is_file() {
            continue;
        }
        let contents = std::fs::read(&path).expect("read source snapshot");
        hash.update((bytes.len() as u64).to_le_bytes());
        hash.update(bytes);
        hash.update((contents.len() as u64).to_le_bytes());
        hash.update(contents);
        println!("cargo::rerun-if-changed={}", path.display());
    }
    // The Git control files that move the revision without touching source
    // bytes. Git names them, because a worktree's `.git` is a file and its
    // index and HEAD live under the common directory's `worktrees/`. Only an
    // existing file is watched: Cargo reruns a script whose watched path is
    // missing on every build, which relinks this crate's binaries each time.
    let mut controls = vec![
        "HEAD".to_string(),
        "index".to_string(),
        "packed-refs".to_string(),
    ];
    if let Some(reference) = git(&root, &["symbolic-ref", "-q", "HEAD"]) {
        controls.push(reference);
    }
    for control in controls {
        if let Some(path) = git(&root, &["rev-parse", "--git-path", &control])
            .map(|path| root.join(path))
            .filter(|path| path.is_file())
        {
            println!("cargo::rerun-if-changed={}", path.display());
        }
    }
    println!("cargo::rustc-env=GQT_SOURCE_REVISION={}", revision.trim());
    println!("cargo::rustc-env=GQT_SOURCE_DIGEST={:x}", hash.finalize());
}

/// One line of `git <args>` run at `root`, or `None` when it fails or prints
/// nothing.
fn git(root: &std::path::Path, args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(root)
        .output()
        .ok()?;
    let line = String::from_utf8(output.stdout).ok()?;
    let line = line.trim();
    (output.status.success() && !line.is_empty()).then(|| line.to_string())
}
