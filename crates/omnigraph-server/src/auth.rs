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
use color_eyre::eyre::{Result, bail};

use crate::server_bearer_tokens_from_env;

/// Environment variable that, when set, selects AWS Secrets Manager as the
/// token source. Its value is the secret ID or ARN. Only honored when the
/// binary is compiled with `--features aws`.
pub const AWS_SECRET_ENV: &str = "OMNIGRAPH_SERVER_BEARER_TOKENS_AWS_SECRET";

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

/// Pick the token source based on configuration.
///
/// Preference order:
/// 1. If `OMNIGRAPH_SERVER_BEARER_TOKENS_AWS_SECRET` is set AND the binary was
///    built with `--features aws`, returns an AWS Secrets Manager source.
/// 2. If that env var is set but the binary was built without the feature,
///    errors with a clear rebuild instruction rather than silently falling
///    back to the env/file source (which would hide the misconfiguration).
/// 3. Otherwise, returns `EnvOrFileTokenSource`.
pub async fn resolve_token_source() -> Result<Box<dyn TokenSource>> {
    if let Ok(secret_id) = std::env::var(AWS_SECRET_ENV) {
        let secret_id = secret_id.trim().to_string();
        if !secret_id.is_empty() {
            #[cfg(feature = "aws")]
            {
                let source = aws::SecretsManagerTokenSource::new(secret_id).await?;
                return Ok(Box::new(source));
            }
            #[cfg(not(feature = "aws"))]
            {
                bail!(
                    "{} is set but this binary was not built with --features aws. \
                     Rebuild: cargo build --release --features aws",
                    AWS_SECRET_ENV
                );
            }
        }
    }
    Ok(Box::new(EnvOrFileTokenSource))
}

/// Parse a JSON secret payload (from AWS Secrets Manager or any equivalent
/// source) into actor → token pairs.
///
/// Payload shape: `{"actor_id_1": "token_1", "actor_id_2": "token_2", ...}`.
/// Extracted as a free function so it can be unit-tested without the AWS SDK.
#[cfg(any(test, feature = "aws"))]
pub(crate) fn parse_json_secret_payload(payload: &str) -> Result<Vec<(String, String)>> {
    use std::collections::HashMap;

    let map: HashMap<String, String> = serde_json::from_str(payload).map_err(|err| {
        color_eyre::eyre::eyre!(
            "bearer-token secret payload is not a JSON object of actor→token: {}",
            err
        )
    })?;

    let mut pairs: Vec<(String, String)> = Vec::with_capacity(map.len());
    for (actor, token) in map {
        let actor = actor.trim().to_string();
        let token = token.trim().to_string();
        if actor.is_empty() {
            bail!("bearer-token secret contains a blank actor id");
        }
        if token.is_empty() {
            bail!("bearer-token secret has a blank token for actor '{}'", actor);
        }
        pairs.push((actor, token));
    }
    pairs.sort_by(|(a, _), (b, _)| a.cmp(b));
    Ok(pairs)
}

#[cfg(feature = "aws")]
pub mod aws {
    //! AWS Secrets Manager bearer-token backend.
    //!
    //! Fetches a JSON payload from a named secret on startup. Credentials are
    //! resolved via the AWS default chain — env vars, shared config, IMDSv2
    //! instance role, or ECS task role — so no explicit credential plumbing
    //! is needed when running under an IAM role.
    //!
    //! Background refresh for rotation is a follow-up.
    use super::TokenSource;
    use async_trait::async_trait;
    use color_eyre::eyre::{Result, WrapErr, eyre};

    /// Loads bearer tokens from a named AWS Secrets Manager secret.
    pub struct SecretsManagerTokenSource {
        client: aws_sdk_secretsmanager::Client,
        secret_id: String,
    }

    impl SecretsManagerTokenSource {
        /// Construct a new source. Resolves AWS credentials + region via the
        /// default chain — no explicit configuration needed on EC2/ECS/EKS.
        pub async fn new(secret_id: impl Into<String>) -> Result<Self> {
            let config =
                aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
            let client = aws_sdk_secretsmanager::Client::new(&config);
            Ok(Self {
                client,
                secret_id: secret_id.into(),
            })
        }
    }

    #[async_trait]
    impl TokenSource for SecretsManagerTokenSource {
        async fn load(&self) -> Result<Vec<(String, String)>> {
            let output = self
                .client
                .get_secret_value()
                .secret_id(&self.secret_id)
                .send()
                .await
                .wrap_err_with(|| {
                    format!("fetch AWS Secrets Manager secret '{}'", self.secret_id)
                })?;

            let payload = output.secret_string().ok_or_else(|| {
                eyre!(
                    "secret '{}' has no SecretString — binary secrets are not supported",
                    self.secret_id
                )
            })?;

            super::parse_json_secret_payload(payload)
        }

        fn supports_refresh(&self) -> bool {
            true
        }

        fn name(&self) -> &'static str {
            "aws-secrets-manager"
        }
    }
}

#[cfg(feature = "aws")]
pub use aws::SecretsManagerTokenSource;

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

    #[test]
    fn parse_json_secret_payload_reads_actor_token_map() {
        let pairs = parse_json_secret_payload(r#"{"alice": "tok-a", "bob": "tok-b"}"#).unwrap();
        assert_eq!(
            pairs,
            vec![
                ("alice".to_string(), "tok-a".to_string()),
                ("bob".to_string(), "tok-b".to_string()),
            ]
        );
    }

    #[test]
    fn parse_json_secret_payload_trims_whitespace() {
        let pairs = parse_json_secret_payload(r#"{"  alice  ": "  tok-a  "}"#).unwrap();
        assert_eq!(pairs, vec![("alice".to_string(), "tok-a".to_string())]);
    }

    #[test]
    fn parse_json_secret_payload_rejects_blank_actor() {
        let err = parse_json_secret_payload(r#"{"   ": "tok"}"#).unwrap_err();
        assert!(err.to_string().contains("blank actor"));
    }

    #[test]
    fn parse_json_secret_payload_rejects_blank_token() {
        let err = parse_json_secret_payload(r#"{"alice": "  "}"#).unwrap_err();
        assert!(err.to_string().contains("blank token"));
    }

    #[test]
    fn parse_json_secret_payload_rejects_non_object() {
        let err = parse_json_secret_payload("[1, 2, 3]").unwrap_err();
        assert!(err.to_string().contains("not a JSON object"));
    }

    #[tokio::test]
    #[serial]
    async fn resolve_token_source_falls_back_to_env_or_file_when_aws_var_unset() {
        clear_env();
        unsafe {
            env::remove_var(AWS_SECRET_ENV);
        }
        let source = resolve_token_source().await.unwrap();
        assert_eq!(source.name(), "env-or-file");
    }

    #[cfg(not(feature = "aws"))]
    #[tokio::test]
    #[serial]
    async fn resolve_token_source_errors_when_aws_var_set_without_feature() {
        clear_env();
        unsafe {
            env::set_var(AWS_SECRET_ENV, "some-secret-id");
        }
        let result = resolve_token_source().await;
        unsafe {
            env::remove_var(AWS_SECRET_ENV);
        }
        let err = match result {
            Ok(_) => panic!("expected resolve_token_source to error without aws feature"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("--features aws"));
    }
}
