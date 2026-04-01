use std::future::Future;
use std::time::Duration;

use reqwest::Client;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::time::sleep;

use crate::error::{OmniError, Result};

const GEMINI_EMBED_MODEL: &str = "gemini-embedding-2-preview";
const DEFAULT_GEMINI_BASE_URL: &str = "https://generativelanguage.googleapis.com/v1beta";
const DEFAULT_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_RETRY_ATTEMPTS: usize = 4;
const DEFAULT_RETRY_BACKOFF_MS: u64 = 200;

#[derive(Clone, Debug)]
enum EmbeddingTransport {
    Mock,
    Gemini {
        api_key: String,
        base_url: String,
        http: Client,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct EmbeddingClient {
    retry_attempts: usize,
    retry_backoff_ms: u64,
    transport: EmbeddingTransport,
}

struct EmbedCallError {
    message: String,
    retryable: bool,
}

#[derive(Debug, Deserialize)]
struct GeminiEmbedResponse {
    embedding: GeminiContentEmbedding,
}

#[derive(Debug, Deserialize)]
struct GeminiContentEmbedding {
    values: Vec<f32>,
}

#[derive(Debug, Deserialize)]
struct GoogleErrorEnvelope {
    error: GoogleErrorBody,
}

#[derive(Debug, Deserialize)]
struct GoogleErrorBody {
    message: String,
}

impl EmbeddingClient {
    pub(crate) fn from_env() -> Result<Self> {
        let retry_attempts =
            parse_env_usize("OMNIGRAPH_EMBED_RETRY_ATTEMPTS", DEFAULT_RETRY_ATTEMPTS);
        let retry_backoff_ms =
            parse_env_u64("OMNIGRAPH_EMBED_RETRY_BACKOFF_MS", DEFAULT_RETRY_BACKOFF_MS);

        if env_flag("OMNIGRAPH_EMBEDDINGS_MOCK") {
            return Ok(Self {
                retry_attempts,
                retry_backoff_ms,
                transport: EmbeddingTransport::Mock,
            });
        }

        let api_key = std::env::var("GEMINI_API_KEY")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .ok_or_else(|| {
                OmniError::manifest_internal(
                    "GEMINI_API_KEY is required when nearest() needs a string embedding",
                )
            })?;
        let base_url = std::env::var("OMNIGRAPH_GEMINI_BASE_URL")
            .ok()
            .map(|v| v.trim_end_matches('/').to_string())
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| DEFAULT_GEMINI_BASE_URL.to_string());
        let timeout_ms = parse_env_u64("OMNIGRAPH_EMBED_TIMEOUT_MS", DEFAULT_TIMEOUT_MS);
        let http = Client::builder()
            .timeout(Duration::from_millis(timeout_ms))
            .build()
            .map_err(|e| {
                OmniError::manifest_internal(format!("failed to initialize HTTP client: {}", e))
            })?;

        Ok(Self {
            retry_attempts,
            retry_backoff_ms,
            transport: EmbeddingTransport::Gemini {
                api_key,
                base_url,
                http,
            },
        })
    }

    #[cfg(test)]
    fn mock_for_tests() -> Self {
        Self {
            retry_attempts: DEFAULT_RETRY_ATTEMPTS,
            retry_backoff_ms: DEFAULT_RETRY_BACKOFF_MS,
            transport: EmbeddingTransport::Mock,
        }
    }

    pub(crate) async fn embed_query_text(
        &self,
        input: &str,
        expected_dim: usize,
    ) -> Result<Vec<f32>> {
        if expected_dim == 0 {
            return Err(OmniError::manifest_internal(
                "embedding dimension must be greater than zero",
            ));
        }

        match &self.transport {
            EmbeddingTransport::Mock => Ok(mock_embedding(input, expected_dim)),
            EmbeddingTransport::Gemini { .. } => {
                self.with_retry(|| self.embed_query_text_gemini_once(input, expected_dim))
                    .await
            }
        }
    }

    async fn with_retry<T, F, Fut>(&self, mut operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = std::result::Result<T, EmbedCallError>>,
    {
        let max_attempt = self.retry_attempts.max(1);
        let mut attempt = 0usize;
        loop {
            attempt += 1;
            match operation().await {
                Ok(value) => return Ok(value),
                Err(err) => {
                    if !err.retryable || attempt >= max_attempt {
                        return Err(OmniError::manifest_internal(err.message));
                    }
                    let shift = (attempt - 1).min(10) as u32;
                    let delay = self.retry_backoff_ms.saturating_mul(1u64 << shift);
                    sleep(Duration::from_millis(delay)).await;
                }
            }
        }
    }

    async fn embed_query_text_gemini_once(
        &self,
        input: &str,
        expected_dim: usize,
    ) -> std::result::Result<Vec<f32>, EmbedCallError> {
        let (api_key, base_url, http) = match &self.transport {
            EmbeddingTransport::Gemini {
                api_key,
                base_url,
                http,
            } => (api_key, base_url, http),
            EmbeddingTransport::Mock => unreachable!("mock transport should not call Gemini"),
        };

        let response = http
            .post(gemini_endpoint(base_url))
            .header("x-goog-api-key", api_key)
            .json(&build_gemini_request(input, expected_dim))
            .send()
            .await;
        let response = match response {
            Ok(response) => response,
            Err(err) => {
                let retryable = err.is_timeout() || err.is_connect() || err.is_request();
                return Err(EmbedCallError {
                    message: format!("embedding request failed: {}", err),
                    retryable,
                });
            }
        };

        let status = response.status();
        let body = match response.text().await {
            Ok(body) => body,
            Err(err) => {
                return Err(EmbedCallError {
                    message: format!(
                        "embedding response read failed (status {}): {}",
                        status, err
                    ),
                    retryable: status.is_server_error() || status.as_u16() == 429,
                });
            }
        };

        if !status.is_success() {
            let message = parse_google_error_message(&body).unwrap_or(body);
            return Err(EmbedCallError {
                message: format!(
                    "embedding request failed with status {}: {}",
                    status, message
                ),
                retryable: status.is_server_error() || status.as_u16() == 429,
            });
        }

        let parsed: GeminiEmbedResponse =
            serde_json::from_str(&body).map_err(|err| EmbedCallError {
                message: format!("embedding response decode failed: {}", err),
                retryable: false,
            })?;

        validate_and_normalize_embedding(parsed.embedding.values, expected_dim).map_err(|message| {
            EmbedCallError {
                message,
                retryable: false,
            }
        })
    }
}

fn gemini_endpoint(base_url: &str) -> String {
    format!(
        "{}/models/{}:embedContent",
        base_url.trim_end_matches('/'),
        GEMINI_EMBED_MODEL
    )
}

fn build_gemini_request(input: &str, expected_dim: usize) -> Value {
    json!({
        "model": format!("models/{}", GEMINI_EMBED_MODEL),
        "content": {
            "parts": [
                {
                    "text": input
                }
            ]
        },
        "taskType": "RETRIEVAL_QUERY",
        "outputDimensionality": expected_dim,
    })
}

fn validate_and_normalize_embedding(
    values: Vec<f32>,
    expected_dim: usize,
) -> std::result::Result<Vec<f32>, String> {
    if values.len() != expected_dim {
        return Err(format!(
            "embedding dimension mismatch: expected {}, got {}",
            expected_dim,
            values.len()
        ));
    }
    Ok(normalize_vector(values))
}

fn normalize_vector(mut values: Vec<f32>) -> Vec<f32> {
    let norm = values
        .iter()
        .map(|v| (*v as f64) * (*v as f64))
        .sum::<f64>()
        .sqrt() as f32;
    if norm > f32::EPSILON {
        for value in &mut values {
            *value /= norm;
        }
    }
    values
}

fn parse_google_error_message(body: &str) -> Option<String> {
    serde_json::from_str::<GoogleErrorEnvelope>(body)
        .ok()
        .map(|e| e.error.message)
        .filter(|msg| !msg.trim().is_empty())
}

fn parse_env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

fn parse_env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

fn env_flag(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .map(|v| {
            let s = v.trim().to_ascii_lowercase();
            s == "1" || s == "true" || s == "yes" || s == "on"
        })
        .unwrap_or(false)
}

fn mock_embedding(input: &str, dim: usize) -> Vec<f32> {
    let mut seed = fnv1a64(input.as_bytes());
    let mut out = Vec::with_capacity(dim);
    for _ in 0..dim {
        seed = xorshift64(seed);
        let ratio = (seed as f64 / u64::MAX as f64) as f32;
        out.push((ratio * 2.0) - 1.0);
    }
    normalize_vector(out)
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 14695981039346656037u64;
    for byte in bytes {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(1099511628211u64);
    }
    hash
}

fn xorshift64(mut x: u64) -> u64 {
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    x
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use serial_test::serial;

    use super::*;

    struct EnvGuard {
        saved: Vec<(&'static str, Option<String>)>,
    }

    impl EnvGuard {
        fn set(vars: &[(&'static str, Option<&str>)]) -> Self {
            let saved = vars
                .iter()
                .map(|(name, _)| (*name, std::env::var(name).ok()))
                .collect::<Vec<_>>();
            for (name, value) in vars {
                unsafe {
                    match value {
                        Some(value) => std::env::set_var(name, value),
                        None => std::env::remove_var(name),
                    }
                }
            }
            Self { saved }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (name, value) in self.saved.drain(..) {
                unsafe {
                    match value {
                        Some(value) => std::env::set_var(name, value),
                        None => std::env::remove_var(name),
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn mock_embeddings_are_deterministic() {
        let client = EmbeddingClient::mock_for_tests();
        let a = client.embed_query_text("alpha", 8).await.unwrap();
        let b = client.embed_query_text("alpha", 8).await.unwrap();
        let c = client.embed_query_text("beta", 8).await.unwrap();
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_eq!(a.len(), 8);
    }

    #[test]
    fn gemini_request_uses_preview_model_retrieval_query_and_dimension() {
        let request = build_gemini_request("alpha", 4);
        assert_eq!(request["model"], "models/gemini-embedding-2-preview");
        assert_eq!(request["taskType"], "RETRIEVAL_QUERY");
        assert_eq!(request["outputDimensionality"], 4);
        assert_eq!(request["content"]["parts"][0]["text"], "alpha");
    }

    #[test]
    fn validate_and_normalize_embedding_enforces_dimension() {
        let normalized = validate_and_normalize_embedding(vec![3.0, 4.0], 2).unwrap();
        assert!((normalized[0] - 0.6).abs() < 1e-6);
        assert!((normalized[1] - 0.8).abs() < 1e-6);

        let err = validate_and_normalize_embedding(vec![1.0, 2.0], 3).unwrap_err();
        assert!(err.contains("expected 3, got 2"));
    }

    #[tokio::test]
    async fn with_retry_retries_retryable_failures() {
        let client = EmbeddingClient::mock_for_tests();
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_call = Arc::clone(&attempts);

        let value = client
            .with_retry(|| {
                let attempts_for_call = Arc::clone(&attempts_for_call);
                async move {
                    let attempt = attempts_for_call.fetch_add(1, Ordering::SeqCst);
                    if attempt == 0 {
                        Err(EmbedCallError {
                            message: "retry me".to_string(),
                            retryable: true,
                        })
                    } else {
                        Ok("ok")
                    }
                }
            })
            .await
            .unwrap();

        assert_eq!(value, "ok");
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn with_retry_stops_on_non_retryable_failures() {
        let client = EmbeddingClient::mock_for_tests();
        let err = client
            .with_retry(|| async {
                Err::<(), _>(EmbedCallError {
                    message: "do not retry".to_string(),
                    retryable: false,
                })
            })
            .await
            .unwrap_err();

        assert!(err.to_string().contains("do not retry"));
    }

    #[test]
    #[serial]
    fn from_env_requires_gemini_api_key_when_not_mocking() {
        let _guard = EnvGuard::set(&[
            ("OMNIGRAPH_EMBEDDINGS_MOCK", None),
            ("GEMINI_API_KEY", None),
        ]);

        let err = EmbeddingClient::from_env().unwrap_err();
        assert!(err.to_string().contains("GEMINI_API_KEY"));
    }
}
