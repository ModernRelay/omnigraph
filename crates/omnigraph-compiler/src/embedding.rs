use std::time::Duration;

use reqwest::Client;
use serde::Deserialize;
use tokio::time::sleep;

use crate::error::{NanoError, Result};

const DEFAULT_EMBED_MODEL: &str = "text-embedding-3-small";
const DEFAULT_OPENAI_BASE_URL: &str = "https://api.openai.com/v1";
const DEFAULT_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_RETRY_ATTEMPTS: usize = 4;
const DEFAULT_RETRY_BACKOFF_MS: u64 = 200;

#[derive(Clone)]
enum EmbeddingTransport {
    Mock,
    OpenAi {
        api_key: String,
        base_url: String,
        http: Client,
    },
}

#[derive(Clone)]
pub(crate) struct EmbeddingClient {
    model: String,
    retry_attempts: usize,
    retry_backoff_ms: u64,
    transport: EmbeddingTransport,
}

struct EmbedCallError {
    message: String,
    retryable: bool,
}

#[derive(Debug, Deserialize)]
struct OpenAiEmbeddingResponse {
    data: Vec<OpenAiEmbeddingDatum>,
}

#[derive(Debug, Deserialize)]
struct OpenAiEmbeddingDatum {
    index: usize,
    embedding: Vec<f32>,
}

#[derive(Debug, Deserialize)]
struct OpenAiErrorEnvelope {
    error: OpenAiErrorBody,
}

#[derive(Debug, Deserialize)]
struct OpenAiErrorBody {
    message: String,
}

impl EmbeddingClient {
    pub(crate) fn from_env() -> Result<Self> {
        let model = std::env::var("NANOGRAPH_EMBED_MODEL")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| DEFAULT_EMBED_MODEL.to_string());
        let retry_attempts =
            parse_env_usize("NANOGRAPH_EMBED_RETRY_ATTEMPTS", DEFAULT_RETRY_ATTEMPTS);
        let retry_backoff_ms =
            parse_env_u64("NANOGRAPH_EMBED_RETRY_BACKOFF_MS", DEFAULT_RETRY_BACKOFF_MS);

        if env_flag("NANOGRAPH_EMBEDDINGS_MOCK") {
            return Ok(Self {
                model,
                retry_attempts,
                retry_backoff_ms,
                transport: EmbeddingTransport::Mock,
            });
        }

        let api_key = std::env::var("OPENAI_API_KEY")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .ok_or_else(|| {
                NanoError::Execution(
                    "OPENAI_API_KEY is required when an embedding call is needed".to_string(),
                )
            })?;
        let base_url = std::env::var("OPENAI_BASE_URL")
            .ok()
            .map(|v| v.trim_end_matches('/').to_string())
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| DEFAULT_OPENAI_BASE_URL.to_string());
        let timeout_ms = parse_env_u64("NANOGRAPH_EMBED_TIMEOUT_MS", DEFAULT_TIMEOUT_MS);
        let http = Client::builder()
            .timeout(Duration::from_millis(timeout_ms))
            .build()
            .map_err(|e| {
                NanoError::Execution(format!("failed to initialize HTTP client: {}", e))
            })?;

        Ok(Self {
            model,
            retry_attempts,
            retry_backoff_ms,
            transport: EmbeddingTransport::OpenAi {
                api_key,
                base_url,
                http,
            },
        })
    }

    #[cfg(test)]
    pub(crate) fn mock_for_tests() -> Self {
        Self {
            model: DEFAULT_EMBED_MODEL.to_string(),
            retry_attempts: DEFAULT_RETRY_ATTEMPTS,
            retry_backoff_ms: DEFAULT_RETRY_BACKOFF_MS,
            transport: EmbeddingTransport::Mock,
        }
    }

    pub(crate) fn model(&self) -> &str {
        &self.model
    }

    pub(crate) async fn embed_text(&self, input: &str, expected_dim: usize) -> Result<Vec<f32>> {
        let mut vectors = self.embed_texts(&[input.to_string()], expected_dim).await?;
        vectors.pop().ok_or_else(|| {
            NanoError::Execution("embedding provider returned no vector".to_string())
        })
    }

    pub(crate) async fn embed_texts(
        &self,
        inputs: &[String],
        expected_dim: usize,
    ) -> Result<Vec<Vec<f32>>> {
        if expected_dim == 0 {
            return Err(NanoError::Execution(
                "embedding dimension must be greater than zero".to_string(),
            ));
        }
        if inputs.is_empty() {
            return Ok(Vec::new());
        }

        match &self.transport {
            EmbeddingTransport::Mock => Ok(inputs
                .iter()
                .map(|input| mock_embedding(input, expected_dim))
                .collect()),
            EmbeddingTransport::OpenAi { .. } => {
                self.embed_texts_openai_with_retry(inputs, expected_dim)
                    .await
            }
        }
    }

    async fn embed_texts_openai_with_retry(
        &self,
        inputs: &[String],
        expected_dim: usize,
    ) -> Result<Vec<Vec<f32>>> {
        let max_attempt = self.retry_attempts.max(1);
        let mut attempt = 0usize;
        loop {
            attempt += 1;
            match self.embed_texts_openai_once(inputs, expected_dim).await {
                Ok(vectors) => return Ok(vectors),
                Err(err) => {
                    if !err.retryable || attempt >= max_attempt {
                        return Err(NanoError::Execution(err.message));
                    }
                    let shift = (attempt - 1).min(10) as u32;
                    let delay = self.retry_backoff_ms.saturating_mul(1u64 << shift);
                    sleep(Duration::from_millis(delay)).await;
                }
            }
        }
    }

    async fn embed_texts_openai_once(
        &self,
        inputs: &[String],
        expected_dim: usize,
    ) -> std::result::Result<Vec<Vec<f32>>, EmbedCallError> {
        let (api_key, base_url, http) = match &self.transport {
            EmbeddingTransport::OpenAi {
                api_key,
                base_url,
                http,
            } => (api_key, base_url, http),
            EmbeddingTransport::Mock => unreachable!("mock transport should not call OpenAI"),
        };

        let request = serde_json::json!({
            "model": self.model,
            "input": inputs,
            "dimensions": expected_dim,
        });
        let url = format!("{}/embeddings", base_url);
        let response = http
            .post(&url)
            .bearer_auth(api_key)
            .json(&request)
            .send()
            .await;

        let response = match response {
            Ok(resp) => resp,
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
            let message = parse_openai_error_message(&body).unwrap_or_else(|| body.clone());
            return Err(EmbedCallError {
                message: format!(
                    "embedding request failed with status {}: {}",
                    status, message
                ),
                retryable: status.is_server_error() || status.as_u16() == 429,
            });
        }

        let mut parsed: OpenAiEmbeddingResponse =
            serde_json::from_str(&body).map_err(|err| EmbedCallError {
                message: format!("embedding response decode failed: {}", err),
                retryable: false,
            })?;

        if parsed.data.len() != inputs.len() {
            return Err(EmbedCallError {
                message: format!(
                    "embedding response size mismatch: expected {}, got {}",
                    inputs.len(),
                    parsed.data.len()
                ),
                retryable: false,
            });
        }

        parsed.data.sort_by_key(|item| item.index);
        let mut vectors = Vec::with_capacity(parsed.data.len());
        for (idx, item) in parsed.data.into_iter().enumerate() {
            if item.index != idx {
                return Err(EmbedCallError {
                    message: format!(
                        "embedding response index mismatch at position {}: got {}",
                        idx, item.index
                    ),
                    retryable: false,
                });
            }
            if item.embedding.len() != expected_dim {
                return Err(EmbedCallError {
                    message: format!(
                        "embedding dimension mismatch: expected {}, got {}",
                        expected_dim,
                        item.embedding.len()
                    ),
                    retryable: false,
                });
            }
            vectors.push(item.embedding);
        }
        Ok(vectors)
    }
}

fn parse_openai_error_message(body: &str) -> Option<String> {
    serde_json::from_str::<OpenAiErrorEnvelope>(body)
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

    let norm = out
        .iter()
        .map(|v| (*v as f64) * (*v as f64))
        .sum::<f64>()
        .sqrt() as f32;
    if norm > f32::EPSILON {
        for value in &mut out {
            *value /= norm;
        }
    }
    out
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
    use super::*;

    #[tokio::test]
    async fn mock_embeddings_are_deterministic() {
        let client = EmbeddingClient::mock_for_tests();
        let a = client.embed_text("alpha", 8).await.unwrap();
        let b = client.embed_text("alpha", 8).await.unwrap();
        let c = client.embed_text("beta", 8).await.unwrap();
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_eq!(a.len(), 8);
    }
}
