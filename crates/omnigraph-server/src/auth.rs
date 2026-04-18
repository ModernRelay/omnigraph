//! Bearer token sources.
//!
//! A `TokenSource` loads `(actor_id, token)` pairs that the server uses to
//! authenticate incoming bearer tokens. Plaintext tokens returned here are
//! hashed immediately by `AppState` on ingest — see `hash_bearer_token` —
//! and never persist past startup/refresh.
//!
//! The trait exists so that additional backends (AWS Secrets Manager,
//! HashiCorp Vault, etc.) can plug in behind feature flags without
//! touching the server wiring.

use async_trait::async_trait;
use color_eyre::eyre::Result;

use crate::server_bearer_tokens_from_env;

/// A source of bearer tokens, returned as `(actor_id, token)` pairs in
/// plaintext. The caller is expected to hash tokens before storing them.
#[async_trait]
pub trait TokenSource: Send + Sync {
    /// Fetch the current set of actor → token pairs.
    ///
    /// Called once at startup. Implementations that support rotation may
    /// also be polled periodically.
    async fn load(&self) -> Result<Vec<(String, String)>>;

    /// Whether this source can be re-fetched for rotation without restart.
    /// Default: false (one-shot sources).
    fn supports_refresh(&self) -> bool {
        false
    }

    /// Human-readable name for logs and error messages.
    fn name(&self) -> &'static str;
}

/// Reads bearer tokens from environment variables and / or files, matching
/// the long-standing server configuration:
///
/// - `OMNIGRAPH_SERVER_BEARER_TOKEN` — a single token assigned to the
///   implicit actor `default`.
/// - `OMNIGRAPH_SERVER_BEARER_TOKENS_JSON` — a JSON object of
///   `{"actor_id": "token", …}`.
/// - `OMNIGRAPH_SERVER_BEARER_TOKENS_FILE` — a path to a JSON file of the
///   same shape.
///
/// Does not support refresh — reloading means restarting the process.
#[derive(Debug, Default, Clone)]
pub struct EnvOrFileTokenSource;

#[async_trait]
impl TokenSource for EnvOrFileTokenSource {
    async fn load(&self) -> Result<Vec<(String, String)>> {
        server_bearer_tokens_from_env()
    }

    fn name(&self) -> &'static str {
        "env-or-file"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use serial_test::serial;

    fn clear_env() {
        unsafe {
            env::remove_var("OMNIGRAPH_SERVER_BEARER_TOKEN");
            env::remove_var("OMNIGRAPH_SERVER_BEARER_TOKENS_JSON");
            env::remove_var("OMNIGRAPH_SERVER_BEARER_TOKENS_FILE");
        }
    }

    #[tokio::test]
    #[serial]
    async fn env_or_file_source_returns_empty_when_nothing_configured() {
        clear_env();
        let source = EnvOrFileTokenSource;
        let tokens = source.load().await.unwrap();
        assert!(tokens.is_empty());
    }

    #[tokio::test]
    #[serial]
    async fn env_or_file_source_reads_single_token_as_default_actor() {
        clear_env();
        unsafe {
            env::set_var("OMNIGRAPH_SERVER_BEARER_TOKEN", "some-token");
        }
        let source = EnvOrFileTokenSource;
        let tokens = source.load().await.unwrap();
        unsafe {
            env::remove_var("OMNIGRAPH_SERVER_BEARER_TOKEN");
        }
        assert_eq!(tokens, vec![("default".to_string(), "some-token".to_string())]);
    }

    #[tokio::test]
    async fn env_or_file_source_does_not_support_refresh() {
        let source = EnvOrFileTokenSource;
        assert!(!source.supports_refresh());
        assert_eq!(source.name(), "env-or-file");
    }
}
