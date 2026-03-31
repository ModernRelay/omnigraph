---
title: Embeddings Developer Reference
slug: embeddings-dev
---

# Embeddings Developer Reference

Internals of the embedding subsystem: client architecture, provider implementations, caching, chunking, and media handling.

## Module Map

| File | Role |
|------|------|
| `embedding.rs` | Provider client: OpenAI, Gemini, Mock transports. Retry, validation, normalization |
| `store/loader/embeddings.rs` | Load-time materialization, caching, chunking, streaming batch resolution |
| `store/media.rs` | Media URI resolution, `@media_uri(...)` property handling |
| `store/blob_store.rs` | Managed blob storage, byte reads for `lanceblob://` URIs |
| `plan/planner.rs` | Query-time embedding (string params in `nearest(...)` calls) |

## Core Structs

### EmbeddingClient (`embedding.rs`)

```rust
pub(crate) struct EmbeddingClient {
    model: String,
    retry_attempts: usize,       // default 4
    retry_backoff_ms: u64,       // default 200, exponential up to 2^10
    transport: EmbeddingTransport,
}

enum EmbeddingTransport {
    Mock,
    OpenAi { api_key: String, base_url: String, http: Client },
    Gemini { api_key: String, base_url: String, http: Client },
}
```

Initialized via `EmbeddingClient::from_env()`. Mock mode via `NANOGRAPH_EMBEDDINGS_MOCK=1`. Provider inferred by `infer_embedding_provider()`.

### EmbedRole

```rust
pub(crate) enum EmbedRole {
    Document,   // stored rows, materialized at load time
    Query,      // query-time string inputs in nearest(...)
}
```

Maps to Gemini `taskType`: `RETRIEVAL_DOCUMENT` / `RETRIEVAL_QUERY`. OpenAI has no role concept; role is ignored. Mock blends a role-specific vector component (2% weight).

### MediaSource

```rust
pub(crate) enum MediaSource {
    LocalFile { path: PathBuf, mime_type: String, size_bytes: u64 },
    ManagedBlob { db_path: PathBuf, blob_id: String, mime_type: String },
    RemoteUri { uri: String, mime_type: String },
}
```

## Provider Details

### Provider Auto-Detection (`infer_embedding_provider`)

Priority order:
1. `NANOGRAPH_EMBED_PROVIDER` env var (`"openai"` or `"gemini"`)
2. Model name heuristic: starts with `gemini-` -> Gemini
3. Key availability: `GEMINI_API_KEY` present and `OPENAI_API_KEY` absent -> Gemini
4. Default: OpenAI

### OpenAI

- Default model: `text-embedding-3-small`
- Default base URL: `https://api.openai.com/v1`
- Endpoint: `POST {base_url}/embeddings`
- Auth: `Authorization: Bearer {api_key}`
- Request body: `{ "model": "...", "input": [...], "dimensions": dim }`
- Response: `{ "data": [{ "index": 0, "embedding": [...] }] }` — sorted by index
- Text only. `embed_media_many` returns an error.
- Vectors returned pre-normalized; no post-processing applied.

### Gemini

- Default model: `gemini-embedding-2-preview`
- Default base URL: `https://generativelanguage.googleapis.com/v1beta`
- Endpoint: `POST {base_url}/models/{model}:batchEmbedContents`
- Auth: `x-goog-api-key: {api_key}` header
- Supports both text and media (images, audio, video, PDF).
- All returned vectors are L2-normalized post-response (`normalize_embedding`).

#### Gemini Text Request Shape

```json
{
  "requests": [{
    "model": "models/gemini-embedding-2-preview",
    "content": { "parts": [{ "text": "..." }] },
    "taskType": "RETRIEVAL_DOCUMENT",
    "outputDimensionality": 768
  }]
}
```

Response: `{ "embeddings": [{ "values": [0.1, 0.2, ...] }] }`

#### Gemini Media Request Shape

Media inputs map to `GeminiPart` variants:

| Source | Part type | How bytes arrive |
|--------|-----------|-----------------|
| `LocalFile` | `inlineData` (base64) | `tokio::fs::read` |
| `ManagedBlob` | `inlineData` (base64) | `read_managed_blob_bytes` |
| `RemoteUri` (http/https) | `inlineData` (base64) | HTTP fetch, validate, encode |
| `RemoteUri` (gs://, s3://, etc.) — image/audio | `fileData` (URI passthrough) | Provider fetches directly |
| `RemoteUri` (gs://, s3://, etc.) — video/PDF | **rejected** | Cannot validate duration/pages without bytes |

Media batch limit: 6 items per API call (`GEMINI_MEDIA_BATCH_MAX`). Larger batches are chunked internally.

#### Gemini Media Validation (Pre-API)

All validation runs locally before the provider call:

| Media kind | MIME types | Constraint | How checked |
|-----------|-----------|-----------|-------------|
| Image | `image/png`, `image/jpeg` | None beyond MIME | `classify_gemini_media_kind` |
| Audio | `audio/*` | None | MIME prefix check |
| Video | `video/mp4`, `video/quicktime` | ≤120 seconds | `parse_mp4_like_duration_seconds` — parses `moov/mvhd` atom |
| Document | `application/pdf` | ≤6 pages | `count_pdf_pages` — scans `/Type /Page` markers, falls back to `/Type /Pages` + `/Count` |
| Text | — | ≤8192 tokens (conservative) | `estimate_gemini_text_tokens` — max(word_count*2, byte_len/3) |

Unsupported MIME types (e.g. `image/webp`, `video/webm`) fail at `classify_gemini_media_kind`.

### Retry Logic (Both Providers)

Exponential backoff: `retry_backoff_ms * 2^(attempt-1)`, capped at `2^10` multiplier.

Retryable conditions:
- Timeouts, connection errors, request errors
- Server errors (5xx)
- Rate limits (429)

Non-retryable: dimension mismatches, response parse failures, validation failures, missing API keys.

## Load-Time Materialization (`store/loader/embeddings.rs`)

### Entry Points

```rust
// String-based: reads JSONL from &str, returns materialized JSONL string
pub(crate) async fn materialize_embeddings_for_load(
    db_path: &Path, source_base: Option<&Path>,
    schema_ir: &SchemaIR, data_source: &str,
) -> Result<String>

// Streaming: reads from BufRead, writes to temp file, returns path
pub(crate) async fn materialize_embeddings_for_load_to_tempfile<R: BufRead>(
    db_path: &Path, source_base: Option<&Path>,
    schema_ir: &SchemaIR, reader: R,
) -> Result<PathBuf>

// Standalone: resolves a batch of requests (used by `nanograph embed`)
pub(crate) async fn resolve_embedding_requests(
    db_path: &Path, requests: &[EmbedValueRequest],
) -> Result<Vec<Vec<f32>>>
```

### Schema Scanning

`collect_embed_specs(schema_ir)` finds all `@embed(source_prop)` annotations and builds `EmbedSpec` entries:

```rust
pub(crate) struct EmbedSpec {
    pub target_prop: String,       // Vector property name
    pub source_prop: String,       // source text or media property
    pub source_kind: EmbedSourceKind,  // Text or Media { mime_prop }
    pub dim: usize,
}

pub(crate) enum EmbedSourceKind {
    Text,
    Media { mime_prop: String },   // name of the property holding the MIME type
}
```

`EmbedSourceKind::Media` is set when the source property has a `@media_uri(mime_prop)` annotation.

### Streaming Materialization Flow

1. **Init**: Load cache, create `EmbeddingClient`, collect embed specs
2. **Per JSONL line**: Parse JSON, match type against embed specs, check if target already has a vector
3. **Queue**: Build `StreamPendingAssignment` for each missing vector, track in pending queue
4. **Batch**: When queue reaches `batch_size` or EOF, group by `(dim, kind)`, resolve via cache or API
5. **Write**: Inject vectors into JSON objects, write completed lines to output
6. **Finalize**: Flush remaining pending, append new entries to cache

Queue key is `(dim, kind)` where kind is text(0) or media(1). Text and media are batched separately because they hit different API paths.

### Embedding Cache

File: `{db_path}/_embedding_cache.jsonl`

#### Cache Key

```rust
struct CacheKey {
    model: String,          // e.g. "text-embedding-3-small"
    dim: usize,             // declared Vector dimension
    content_hash: String,   // SHA256 of source text, or "media:{mime}:{uri}"
    chunk_chars: usize,     // chunking config (0 if disabled or media)
    chunk_overlap_chars: usize,
}
```

Cache key includes chunking params so changing chunk settings invalidates stale entries.

#### Cache Record (JSONL)

```json
{"model":"text-embedding-3-small","dim":768,"content_hash":"a1b2c3...","vector":[0.1,0.2,...],"chunk_chars":1500,"chunk_overlap_chars":200}
```

#### Cache Locking

Lock file: `{cache_path}.lock`. Acquired with 200 retries at 10ms intervals. Stale lock timeout: 60s (configurable via `NANOGRAPH_EMBED_CACHE_LOCK_STALE_SECS`). Prevents corruption during concurrent `load` or `embed` processes.

#### Cache Compaction

Max entries: 50,000 (`NANOGRAPH_EMBED_CACHE_MAX_ENTRIES`). When exceeded during append, the cache is compacted: deduplicated by key (last write wins), oldest entries dropped.

### Text Chunking

Config from env:
- `NANOGRAPH_EMBED_CHUNK_CHARS` (default 0 = disabled)
- `NANOGRAPH_EMBED_CHUNK_OVERLAP_CHARS` (default 128)

Algorithm (`split_text_into_chunks`): character-boundary-aware sliding window. Step size: `chunk_chars - overlap_chars` (minimum 1).

Example with chunk_chars=1500, overlap=200:
- Chunk 1: chars 0..1500
- Chunk 2: chars 1300..2800
- Chunk 3: chars 2600..4100

Aggregation (`average_pool_embeddings`): element-wise mean across chunk vectors, then L2-normalize. Keeps output dimension constant regardless of chunk count.

Chunking applies to text only. Media inputs are never chunked.

## Query-Time Embedding (`plan/planner.rs`)

When a query uses `nearest($var.embedding, $query_string)`:
1. The planner collects string parameters that need embedding
2. Calls `client.embed_texts_with_role(&[query_string], dim, EmbedRole::Query)`
3. The resulting vector feeds into the Lance KNN scan

ANN fast path (`try_execute_single_node_nearest_ann_fast_path`): for simple `match { $v: Type } order { nearest(...) } limit N` patterns, uses Lance vector index directly instead of full-scan + sort.

Query-time embeddings are NOT cached in the embedding cache. They are computed fresh each call.

## Mock Provider

Deterministic, content-based embeddings for tests and examples. No API key required.

### Algorithm (`mock_semantic_embedding`)

1. Base vector from `fnv1a64(semantic_key)` + `xorshift64` PRNG, mapped to [-1, 1], L2-normalized
2. Role vector from `fnv1a64("role:{document|query}")`
3. Modality vector from `fnv1a64("modality:{text|media}")`
4. Blend: `base * 0.92 + role * 0.02 + modality * 0.06`
5. L2-normalize

### Semantic Key Normalization

- Text: `input.trim().to_ascii_lowercase()`
- Media: extract filename stem (LocalFile), blob ID (ManagedBlob), or last URL path segment before first dot (RemoteUri)

This enables cross-modal matching: a text query `"space"` is similar to a media file `space.jpg` because both normalize to `"space"`.

## Normalization

```rust
fn normalize_embedding(mut input: Vec<f32>) -> Vec<f32> {
    let norm = input.iter().map(|v| (*v as f64) * (*v as f64)).sum::<f64>().sqrt() as f32;
    if norm > f32::EPSILON {
        for value in &mut input { *value /= norm; }
    }
    input
}
```

Applied to: all Gemini outputs, all Mock outputs. NOT applied to OpenAI (pre-normalized by provider). Ensures unit-norm vectors for cosine distance: `distance = 1 - dot_product`.

## Environment Variables

### Provider Selection

| Variable | Default | Notes |
|----------|---------|-------|
| `NANOGRAPH_EMBED_PROVIDER` | (inferred) | `"openai"`, `"gemini"`, or `"mock"` |
| `NANOGRAPH_EMBED_MODEL` | per provider | OpenAI: `text-embedding-3-small`, Gemini: `gemini-embedding-2-preview` |
| `NANOGRAPH_EMBEDDINGS_MOCK` | false | `1`/`true`/`yes`/`on` enables mock, overrides everything |
| `OPENAI_API_KEY` | — | required when OpenAI provider active |
| `OPENAI_BASE_URL` | `https://api.openai.com/v1` | trailing slash stripped |
| `GEMINI_API_KEY` | — | required when Gemini provider active |
| `GEMINI_BASE_URL` | `https://generativelanguage.googleapis.com/v1beta` | trailing slash stripped |

### Client Tuning

| Variable | Default | Notes |
|----------|---------|-------|
| `NANOGRAPH_EMBED_TIMEOUT_MS` | 30000 | HTTP request timeout |
| `NANOGRAPH_EMBED_RETRY_ATTEMPTS` | 4 | max retry count |
| `NANOGRAPH_EMBED_RETRY_BACKOFF_MS` | 200 | initial backoff, exponential |

### Batching & Chunking

| Variable | Default | Notes |
|----------|---------|-------|
| `NANOGRAPH_EMBED_BATCH_SIZE` | 64 | texts/media per provider call |
| `NANOGRAPH_EMBED_CHUNK_CHARS` | 0 | 0 = disabled. Character-based chunk size |
| `NANOGRAPH_EMBED_CHUNK_OVERLAP_CHARS` | 128 | overlap between chunks |

### Cache

| Variable | Default | Notes |
|----------|---------|-------|
| `NANOGRAPH_EMBED_CACHE_MAX_ENTRIES` | 50000 | compaction threshold |
| `NANOGRAPH_EMBED_CACHE_LOCK_STALE_SECS` | 60 | lock file timeout |

## Config Flow (CLI)

```
nanograph.toml [embedding] section
    -> apply_embedding_env_for_process()
        -> sets env vars if not already present
        -> handles api_key_env indirection (reads custom var, writes standard var)
        -> maps provider-specific base_url

.env.nano / .env
    -> loaded by load_dotenv_for_process() before embedding init

Process env takes precedence over .env.nano, which takes precedence over .env,
which takes precedence over nanograph.toml defaults.
```

## Constants

```rust
// embedding.rs
DEFAULT_OPENAI_EMBED_MODEL = "text-embedding-3-small"
DEFAULT_GEMINI_EMBED_MODEL = "gemini-embedding-2-preview"
DEFAULT_TIMEOUT_MS = 30_000
DEFAULT_RETRY_ATTEMPTS = 4
DEFAULT_RETRY_BACKOFF_MS = 200
GEMINI_MEDIA_BATCH_MAX = 6
GEMINI_TEXT_TOKEN_LIMIT = 8192
GEMINI_VIDEO_MAX_SECONDS = 120.0
GEMINI_PDF_MAX_PAGES = 6

// store/loader/embeddings.rs
DEFAULT_EMBED_BATCH_SIZE = 64
DEFAULT_EMBED_CHUNK_CHARS = 0
DEFAULT_EMBED_CHUNK_OVERLAP_CHARS = 128
DEFAULT_EMBED_CACHE_MAX_ENTRIES = 50_000
DEFAULT_EMBED_CACHE_LOCK_STALE_SECS = 60
EMBEDDING_CACHE_LOCK_RETRIES = 200
EMBEDDING_CACHE_LOCK_RETRY_DELAY_MS = 10
```

## Adding a New Provider

1. Add variant to `EmbeddingProvider` enum and `EmbeddingTransport` enum
2. Add case to `infer_embedding_provider()` and `default_model_for_provider()`
3. Add API key env var handling in `require_api_key` call
4. Implement `embed_texts_{provider}_with_retry` and `embed_texts_{provider}_once`
5. If media supported: implement `embed_media_{provider}_with_retry` / `_once`, update `supports_media_embeddings()`
6. Decide on normalization: if provider returns non-normalized vectors, apply `normalize_embedding`
7. Update `apply_embedding_env_for_process()` in CLI config for base URL mapping
8. Add tests for request shape, error handling, and provider inference
