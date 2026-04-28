# Embeddings

OmniGraph has **two** embedding clients with different defaults and purposes.

## Compiler-side client (`omnigraph-compiler/src/embedding.rs`) — query-time normalization

- Default model: `text-embedding-3-small` (OpenAI-style schema)
- Env: `NANOGRAPH_EMBED_MODEL`, `OPENAI_API_KEY`, `OPENAI_BASE_URL` (default `https://api.openai.com/v1`), `NANOGRAPH_EMBEDDINGS_MOCK`, `NANOGRAPH_EMBED_TIMEOUT_MS=30000`, `NANOGRAPH_EMBED_RETRY_ATTEMPTS=4`, `NANOGRAPH_EMBED_RETRY_BACKOFF_MS=200`
- Methods: `embed_text(input, expected_dim)`, `embed_texts(inputs, expected_dim)`
- Mock mode: deterministic FNV-1a + xorshift64 → L2-normalized vectors

## Engine-side client (`omnigraph/src/embedding.rs`) — runtime ingest

- Model: `gemini-embedding-2-preview`
- Env: `GEMINI_API_KEY`, `OMNIGRAPH_GEMINI_BASE_URL` (default Google generativelanguage v1beta), `OMNIGRAPH_EMBED_TIMEOUT_MS=30000`, `OMNIGRAPH_EMBED_RETRY_ATTEMPTS=4`, `OMNIGRAPH_EMBED_RETRY_BACKOFF_MS=200`, `OMNIGRAPH_EMBEDDINGS_MOCK`
- Two task types: `embed_query_text` (RETRIEVAL_QUERY) and `embed_document_text` (RETRIEVAL_DOCUMENT)
- Exponential backoff with retryable detection (timeouts, 429, 5xx)

## Schema integration

Mark a Vector property with `@embed("source_text_property")`. At ingest, the engine pulls the source text and writes the embedding into the vector column. Stored as L2-normalized FixedSizeList(Float32, dim).

## CLI `omnigraph embed` (offline file pipeline)

Operates on **JSONL files** (not on a repo). Three modes (mutually exclusive):

- (default) `fill_missing` — only embed rows whose target field is empty
- `--reembed-all` — overwrite all
- `--clean` — strip embeddings

Inputs are either a single seed manifest YAML or `--input/--output/--spec`. Selectors `--type T`, `--select T:field=value` filter rows. Streams JSONL → JSONL.
