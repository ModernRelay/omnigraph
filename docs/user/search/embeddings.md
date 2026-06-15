# Embeddings

OmniGraph embeds text through a **single, provider-independent client** resolved from one
`EmbeddingConfig { provider, model, base_url, api_key }`. The same resolved config is used by the query-time
auto-embed of a string in `nearest($v, "string")` and by the offline `omnigraph embed` file pipeline, so
query vectors and document vectors share one model and one vector space.

## Providers

| `provider` | Wire shape | Use it for |
|---|---|---|
| `openai-compatible` (default) | `POST {base}/embeddings`, bearer auth, `{model, input, dimensions}` | **OpenRouter** (the default gateway — one key for many models), OpenAI direct, or a self-hosted endpoint (vLLM / Ollama / LM Studio) |
| `gemini` | `POST {base}/models/{model}:embedContent`, `x-goog-api-key`, with `RETRIEVAL_QUERY` / `RETRIEVAL_DOCUMENT` task types | Reaching Google's `generativelanguage` API directly |
| `mock` | none — deterministic offline vectors | Tests and local dev without a key |

Vectors are stored L2-normalized as `FixedSizeList(Float32, dim)`; the requested output dimension is driven by
the target column width and sent as Gemini `outputDimensionality` / OpenAI `dimensions`.

## Configuration (environment)

| Variable | Meaning |
|---|---|
| `OMNIGRAPH_EMBED_PROVIDER` | `openai-compatible` (default, → OpenRouter) \| `openai` (→ OpenAI's own host) \| `gemini` \| `mock` |
| `OMNIGRAPH_EMBED_BASE_URL` | endpoint base; defaults `https://openrouter.ai/api/v1` (`openai-compatible`/unset), `https://api.openai.com/v1` (`openai`), `https://generativelanguage.googleapis.com/v1beta` (`gemini`) |
| `OMNIGRAPH_EMBED_MODEL` | model id; defaults `openai/text-embedding-3-large` (OpenRouter), `text-embedding-3-large` (`openai`), `gemini-embedding-2` (`gemini`) |
| `OPENROUTER_API_KEY` / `OPENAI_API_KEY` | api key for `openai-compatible` (OpenRouter preferred) |
| `GEMINI_API_KEY` | api key for `gemini` |
| `OMNIGRAPH_EMBED_QUERY_DEADLINE_MS` | total wall-clock budget for one embed call across all retries (default `60000`; `0` = unbounded) |
| `OMNIGRAPH_EMBED_TIMEOUT_MS` | per-request HTTP timeout (default `30000`) |
| `OMNIGRAPH_EMBED_RETRY_ATTEMPTS` / `OMNIGRAPH_EMBED_RETRY_BACKOFF_MS` | retry policy (defaults `4` / `200`) |
| `OMNIGRAPH_EMBEDDINGS_MOCK` | set truthy to force the deterministic mock provider |

The default zero-config path is OpenRouter: set `OPENROUTER_API_KEY` and run. Reaching Gemini takes
`OMNIGRAPH_EMBED_PROVIDER=gemini` plus `GEMINI_API_KEY`.

### Behavior notes

- **Bounded latency.** Each embed call is wrapped in `OMNIGRAPH_EMBED_QUERY_DEADLINE_MS`, so a degraded
  provider cannot hang a read for the full retry envelope.
- **Reuse.** The query path builds the client once per graph handle (on the first `nearest($v, "string")`
  that needs embedding) and reuses it, keeping the provider connection pool warm. A graph that never embeds
  needs no provider key.
- **Observability.** Embed calls emit `tracing` events under `target = "omnigraph::embedding"` (provider,
  model, dim, attempt, elapsed, outcome).

## `@embed` schema annotation

Mark a Vector property with `@embed("source_text_property")`. This is a **catalog annotation** consumed by the
query typechecker and linter: it records which String property is the embedding source and lets
`nearest($v, "string")` auto-embed a query string for comparison against that vector column.

Optionally record the model that produced the stored vectors:
`@embed("source_text_property", model="openai/text-embedding-3-large")`. When a model is recorded, a
`nearest($v, "string")` query is **rejected with a typed error** unless the resolved query embedder uses the
same model — so stored and query vectors are guaranteed same-space instead of silently ranking across spaces.
To fix a mismatch, set `OMNIGRAPH_EMBED_MODEL` (and the matching provider) to the recorded model, or re-embed.
The recorded model is the literal string, so `openai/text-embedding-3-large` (via OpenRouter) and
`text-embedding-3-large` (OpenAI direct) are distinct identities; use the matching string. Changing a recorded
model is a loud `schema apply` refusal (treat it as a re-embed migration). `@embed` without a model keeps
working with no validation. `model` is the only supported `@embed` argument; any other is a parse error.

**It does not embed at ingest.** Stored vectors are supplied directly in your load data, or pre-filled by the
offline `omnigraph embed` pipeline below. (Ingest-time execution of `@embed` is a planned enhancement.)

## CLI `omnigraph embed` (offline file pipeline)

Operates on **JSONL files** (not on a graph), using the same resolved provider config. Three modes (mutually
exclusive):

- (default) `fill_missing` — only embed rows whose target field is empty
- `--reembed-all` — overwrite all
- `--clean` — strip embeddings

Inputs are either a single seed manifest YAML or `--input/--output/--spec`. Selectors `--type T`, `--select T:field=value` filter rows. Streams JSONL → JSONL.

## Migration

This release has no backwards-compatibility shim (pre-release). The default provider is now OpenRouter, and
the legacy `OMNIGRAPH_GEMINI_BASE_URL` is removed. A graph whose vectors were produced with
`gemini-embedding-2-preview` should either re-embed, or pin the query-time embedder to match by setting
`OMNIGRAPH_EMBED_PROVIDER=gemini` and `OMNIGRAPH_EMBED_MODEL=gemini-embedding-2-preview` (the stored and query
vectors must come from the same model to be comparable).
