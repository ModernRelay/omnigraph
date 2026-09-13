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
    println!(
        "cargo::rerun-if-changed={}",
        root.join(".git/index").display()
    );
    println!(
        "cargo::rerun-if-changed={}",
        root.join(".git/HEAD").display()
    );
    println!("cargo::rustc-env=GQT_SOURCE_REVISION={}", revision.trim());
    println!("cargo::rustc-env=GQT_SOURCE_DIGEST={:x}", hash.finalize());
}
