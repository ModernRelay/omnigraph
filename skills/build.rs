//! Embeds the skill tree beside this manifest: every file below `omnigraph/`
//! becomes one row of the generated `FILES` table, so adding or removing a
//! skill file needs no code change. Names starting with `.` are skipped; a
//! name that is not ASCII, or an entry that is neither a file nor a directory,
//! fails the build. ASCII names keep case-insensitive and normalizing file
//! systems from aliasing two skill paths that an export compares.

use std::fmt::Write as _;
use std::path::Path;

fn main() {
    let manifest = std::env::var_os("CARGO_MANIFEST_DIR")
        .expect("Cargo sets CARGO_MANIFEST_DIR for build scripts");
    let root = Path::new(&manifest).join("omnigraph");
    // Keep both watching and generated includes relative to the compiling package:
    // a shared target directory can reuse this executable from another checkout.
    println!("cargo::rerun-if-changed=omnigraph");
    let mut files = Vec::new();
    collect(&root, None, &mut files);
    files.sort();
    assert!(
        files.iter().any(|path| path == "SKILL.md"),
        "{} has no SKILL.md",
        root.display()
    );
    let mut table = String::from("&[\n");
    for path in &files {
        writeln!(
            table,
            "    File {{ path: {path:?}, contents: include_bytes!(concat!(env!(\"CARGO_MANIFEST_DIR\"), \"/omnigraph/\", {path:?})) }},"
        )
        .expect("writing to a String cannot fail");
    }
    table.push(']');
    let out = std::env::var_os("OUT_DIR").expect("Cargo sets OUT_DIR for build scripts");
    std::fs::write(Path::new(&out).join("files.rs"), table).expect("write the skill table");
}

fn collect(dir: &Path, prefix: Option<&str>, files: &mut Vec<String>) {
    let entries = std::fs::read_dir(dir)
        .unwrap_or_else(|error| panic!("read skill directory {}: {error}", dir.display()));
    for entry in entries {
        let entry = entry.unwrap_or_else(|error| panic!("list {}: {error}", dir.display()));
        let name = entry
            .file_name()
            .into_string()
            .unwrap_or_else(|name| panic!("skill path {name:?} is not UTF-8"));
        if name.starts_with('.') {
            continue;
        }
        assert!(name.is_ascii(), "skill path name {name:?} is not ASCII");
        let path = match prefix {
            Some(prefix) => format!("{prefix}/{name}"),
            None => name,
        };
        let kind = entry
            .file_type()
            .unwrap_or_else(|error| panic!("inspect skill path {path}: {error}"));
        if kind.is_dir() {
            collect(&entry.path(), Some(&path), files);
        } else if kind.is_file() {
            files.push(path);
        } else {
            panic!("skill path {path} is neither a file nor a directory");
        }
    }
}
