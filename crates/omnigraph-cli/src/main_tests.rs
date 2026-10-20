//! Shared CLI contract tests, compiled once in the application library.

use super::{
    MissingCredential, RemoteCredential, apply_bearer_token, branch_statement_change_request,
    branch_statement_query_request, normalize_bearer_token, resolve_remote_credential_with_input,
    sync_dir,
};
use reqwest::header::AUTHORIZATION;
use serde_json::json;

#[test]
fn embedded_cli_preserves_complete_storage_diagnostic_without_reprefixing() {
    let diagnostic = "storage read failed for 's3://bucket/key': request timed out".to_string();
    let error = omnigraph::error::OmniError::Storage(omnigraph::error::StorageFailure::new(
        omnigraph::error::StorageFailureKind::Transient,
        diagnostic.clone(),
    ));
    let report = color_eyre::eyre::Report::from(error);

    assert_eq!(report.to_string(), diagnostic);
    assert!(!report.to_string().contains("storage: storage"));
}

#[test]
fn branch_statement_requests_carry_the_source_alone() {
    assert_eq!(
        serde_json::to_value(branch_statement_query_request("branch list")).unwrap(),
        json!({
            "query": "branch list",
            "name": null,
            "params": null,
            "branch": null,
            "snapshot": null,
        })
    );
    assert_eq!(
        serde_json::to_value(branch_statement_change_request("branch merge b0 into main")).unwrap(),
        json!({
            "query": "branch merge b0 into main",
            "name": null,
            "params": null,
            "branch": null,
        })
    );
}

#[test]
fn branch_merge_as_commit_excludes_delete_branch() {
    use clap::Parser;
    let error = crate::cli::Cli::try_parse_from([
        "omnigraph",
        "branch",
        "merge",
        "feature",
        "--as-commit",
        "--delete-branch",
    ])
    .unwrap_err();
    assert_eq!(error.kind(), clap::error::ErrorKind::ArgumentConflict);
    assert!(
        crate::cli::Cli::try_parse_from(["omnigraph", "branch", "merge", "feature", "--as-commit"])
            .is_ok()
    );
}

#[test]
fn commit_list_unattached_excludes_unpublished() {
    use clap::Parser;
    let error = crate::cli::Cli::try_parse_from([
        "omnigraph",
        "commit",
        "list",
        "--unattached",
        "--unpublished",
    ])
    .unwrap_err();
    assert_eq!(error.kind(), clap::error::ErrorKind::ArgumentConflict);
    assert!(
        crate::cli::Cli::try_parse_from(["omnigraph", "commit", "list", "--unattached"]).is_ok()
    );
}

#[test]
fn commit_list_pages_only_a_branch_history() {
    use clap::Parser;
    for listing in ["--unattached", "--unpublished"] {
        for page in [["--limit", "2"], ["--page-token", "token"]] {
            let error = crate::cli::Cli::try_parse_from(
                ["omnigraph", "commit", "list", listing]
                    .into_iter()
                    .chain(page),
            )
            .unwrap_err();
            assert_eq!(
                error.kind(),
                clap::error::ErrorKind::ArgumentConflict,
                "{listing} {page:?}"
            );
        }
    }
    assert!(
        crate::cli::Cli::try_parse_from([
            "omnigraph",
            "commit",
            "list",
            "--limit",
            "2",
            "--page-token",
            "token",
        ])
        .is_ok()
    );
}

#[test]
fn apply_bearer_token_adds_header_when_configured() {
    let client = reqwest::Client::new();
    let request = apply_bearer_token(client.get("http://example.com"), Some("demo-token"))
        .build()
        .unwrap();
    assert_eq!(
        request
            .headers()
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok()),
        Some("Bearer demo-token")
    );
}

#[test]
fn apply_bearer_token_leaves_request_unchanged_when_not_configured() {
    let client = reqwest::Client::new();
    let request = apply_bearer_token(client.get("http://example.com"), None)
        .build()
        .unwrap();
    assert!(request.headers().get(AUTHORIZATION).is_none());
}

#[test]
fn normalize_bearer_token_trims_and_filters_blank_values() {
    assert_eq!(normalize_bearer_token(None), None);
    assert_eq!(normalize_bearer_token(Some("   ".to_string())), None);
    assert_eq!(
        normalize_bearer_token(Some(" demo-token ".to_string())).as_deref(),
        Some("demo-token")
    );
}

/// Sets process environment variables for one test and restores the previous
/// values when dropped, including on a failed assertion.
struct ScopedEnv(Vec<(&'static str, Option<std::ffi::OsString>)>);

impl ScopedEnv {
    fn set(vars: &[(&'static str, Option<&str>)]) -> Self {
        let saved = vars
            .iter()
            .map(|(name, _)| (*name, std::env::var_os(name)))
            .collect();
        for (name, value) in vars {
            unsafe {
                match value {
                    Some(value) => std::env::set_var(name, value),
                    None => std::env::remove_var(name),
                }
            }
        }
        Self(saved)
    }
}

impl Drop for ScopedEnv {
    fn drop(&mut self) {
        for (name, value) in &self.0 {
            unsafe {
                match value {
                    Some(value) => std::env::set_var(name, value),
                    None => std::env::remove_var(name),
                }
            }
        }
    }
}

#[test]
fn resolve_remote_credential_sends_no_unkeyed_token_issue_1022() {
    // A token reaches only the operator server it is keyed to. The unkeyed
    // `OMNIGRAPH_BEARER_TOKEN` is never a credential: a URL that matches no
    // `servers:` entry, and a declared server with no keyed token, both get
    // none.
    let home = tempfile::tempdir().unwrap();
    std::fs::write(
        home.path().join("config.yaml"),
        "servers:\n  \
           unkeyed:\n    url: https://unkeyed.example.com\n  \
           keyed:\n    url: https://keyed.example.com\n",
    )
    .unwrap();
    let _env = ScopedEnv::set(&[
        ("OMNIGRAPH_BEARER_TOKEN", Some("global-token")),
        ("OMNIGRAPH_TOKEN_KEYED", Some("keyed-token")),
        ("OMNIGRAPH_TOKEN_UNKEYED", None),
    ]);

    // The whole URL is the address here; served clients pass the server root
    // and a graph URL under it.
    let input = crate::operator::InvocationConfig::at(Some(home.path().to_path_buf()), None);
    let config = input.get(&mut Vec::new()).unwrap();
    let credential =
        |url: &str| match resolve_remote_credential_with_input(&input, config, url, url).unwrap() {
            RemoteCredential::Keyed(token) => Ok(token),
            RemoteCredential::Missing(missing) => Err(missing),
        };
    let unbound = |server_root: &str| {
        Err(MissingCredential::Unbound {
            server_root: server_root.to_string(),
            config: Some(home.path().join("config.yaml")),
        })
    };
    assert_eq!(
        credential("https://override.example.com"),
        unbound("https://override.example.com")
    );
    assert_eq!(
        credential("https://unkeyed.example.com/graphs/knowledge"),
        Err(MissingCredential::Server("unkeyed".to_string()))
    );
    assert_eq!(
        credential("https://keyed.example.com-evil/graphs/knowledge"),
        unbound("https://keyed.example.com-evil/graphs/knowledge")
    );
    assert_eq!(
        credential("https://keyed.example.com/graphs/knowledge"),
        Ok("keyed-token".to_string())
    );
    // A refusal names the address without credentials written into its URL.
    assert_eq!(
        credential("https://user:secret@override.example.com"),
        unbound("https://override.example.com/")
    );
}

#[cfg(unix)]
#[test]
fn sync_dir_propagates_open_failure_instead_of_swallowing_it() {
    // The F5 defect was `if let Ok(dir) = File::open(..)` / `let _ =
    // dir.sync_all()` — both errors were dropped, so the baseline resume
    // cursor could print for a rename that never reached disk. `sync_dir`
    // must instead return the error so the caller's `?` aborts before the
    // cursor is emitted.
    let missing = std::path::Path::new("/nonexistent-omnigraph-sync-dir/xyz");
    assert!(
        sync_dir(missing).is_err(),
        "sync_dir must propagate a directory-open failure, not swallow it"
    );

    // A real directory fsyncs cleanly.
    let dir = tempfile::tempdir().expect("tempdir");
    assert!(
        sync_dir(dir.path()).is_ok(),
        "sync_dir must succeed on an existing directory"
    );
}
