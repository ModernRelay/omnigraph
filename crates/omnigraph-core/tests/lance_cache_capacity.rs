//! Process environment sizes the data session's caches, so the case runs alone
//! in a child process.

use omnigraph_core::lance_access::LanceAccessContext;
use std::process::Command;

const CASE: &str = "OMNIGRAPH_LANCE_CACHE_CAPACITY_CASE";
const METADATA_BYTES: usize = 64 * 1024 * 1024;

#[test]
fn metadata_cache_capacity_follows_the_environment_issue_702() {
    if std::env::var_os(CASE).is_some() {
        let session = LanceAccessContext::new().data_session();
        assert_eq!(
            session.file_metadata_cache().capacity_bytes(),
            Some(METADATA_BYTES)
        );
        return;
    }
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "metadata_cache_capacity_follows_the_environment_issue_702",
            "--nocapture",
        ])
        .env(CASE, "1")
        .env(
            "OMNIGRAPH_LANCE_METADATA_CACHE_BYTES",
            METADATA_BYTES.to_string(),
        )
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("1 passed"),
        "the child ran no test"
    );
}
