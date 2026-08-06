//! In-source test suite for the CLI binary (moved verbatim from
//! main.rs; `use super::*` resolves through the #[path] declaration).

use super::{
    DEFAULT_BEARER_TOKEN_ENV, apply_bearer_token, legacy_change_request_body,
    normalize_bearer_token, resolve_remote_bearer_token,
};
use reqwest::header::AUTHORIZATION;
use serde_json::json;

#[test]
fn legacy_change_request_body_uses_legacy_field_names() {
    // `mutate`'s remote arm hits `POST /change`, which old
    // `omnigraph-server` builds deserialize as `ChangeRequest` with
    // **required** `query_source` and optional `query_name` keys.
    // Newer servers accept both spellings via serde alias, but a
    // newer CLI must still emit the legacy keys on the wire so it
    // can talk to an old server during a rolling upgrade.
    let body = legacy_change_request_body(
        "query insert_person($n: String) { insert Person { name: $n } }",
        Some("insert_person"),
        "main",
        Some(&json!({ "n": "Alice" })),
    );
    assert_eq!(
        body["query_source"].as_str(),
        Some("query insert_person($n: String) { insert Person { name: $n } }"),
    );
    assert_eq!(body["query_name"].as_str(), Some("insert_person"));
    assert_eq!(body["branch"].as_str(), Some("main"));
    assert_eq!(body["params"]["n"].as_str(), Some("Alice"));
    // Crucially, the **new** field names must NOT appear -- old
    // servers would silently treat them as unknown fields and then
    // fail on missing required `query_source`.
    assert!(
        body.get("query").is_none(),
        "legacy /change body must not carry the renamed `query` key; got {body}"
    );
    assert!(
        body.get("name").is_none(),
        "legacy /change body must not carry the renamed `name` key; got {body}"
    );
}

#[test]
fn legacy_change_request_body_omits_optional_fields_when_unset() {
    let body = legacy_change_request_body(
        "query find() { match { $p: Person } return { $p.name } }",
        None,
        "main",
        None,
    );
    assert_eq!(body["branch"].as_str(), Some("main"));
    assert!(body.get("query_name").is_none());
    assert!(body.get("params").is_none());
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

#[test]
fn resolve_remote_bearer_token_falls_back_to_default_env() {
    // RFC-011: with no operator server matching the URL, the only chain
    // left is the default `OMNIGRAPH_BEARER_TOKEN` env (no omnigraph.yaml
    // scoped chain). Hermetic: no operator config is read for a literal URL
    // that matches no `servers:` entry.
    let previous = std::env::var_os(DEFAULT_BEARER_TOKEN_ENV);
    let previous_home = std::env::var_os("OMNIGRAPH_HOME");
    unsafe {
        std::env::set_var(DEFAULT_BEARER_TOKEN_ENV, "global-token");
        std::env::set_var("OMNIGRAPH_HOME", "/nonexistent/omnigraph-test-home");
    }

    assert_eq!(
        resolve_remote_bearer_token(Some("https://override.example.com"))
            .unwrap()
            .as_deref(),
        Some("global-token")
    );

    unsafe {
        if let Some(value) = previous {
            std::env::set_var(DEFAULT_BEARER_TOKEN_ENV, value);
        } else {
            std::env::remove_var(DEFAULT_BEARER_TOKEN_ENV);
        }
        if let Some(value) = previous_home {
            std::env::set_var("OMNIGRAPH_HOME", value);
        } else {
            std::env::remove_var("OMNIGRAPH_HOME");
        }
    }
}
