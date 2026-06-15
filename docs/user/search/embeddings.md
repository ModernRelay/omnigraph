# Embeddings

OmniGraph embeds text through a **single engine-side client** (Google Gemini). It is used in two places: the
query-time auto-embed of a string passed to `nearest($v, "string")`, and the offline `omnigraph embed` file
pipeline. Both paths use the same client, so query vectors and CLI-produced document vectors share one model
and one vector space.

## Engine embedding client

- Model: `gemini-embedding-2-preview`
- Env: `GEMINI_API_KEY`, `OMNIGRAPH_GEMINI_BASE_URL` (default Google generativelanguage v1beta), `OMNIGRAPH_EMBED_TIMEOUT_MS=30000`, `OMNIGRAPH_EMBED_RETRY_ATTEMPTS=4`, `OMNIGRAPH_EMBED_RETRY_BACKOFF_MS=200`, `OMNIGRAPH_EMBEDDINGS_MOCK`
- Two task types: `embed_query_text` (RETRIEVAL_QUERY) for query strings and `embed_document_text` (RETRIEVAL_DOCUMENT) for stored documents
- Exponential backoff with retryable detection (timeouts, 429, 5xx)
- Vectors are stored as L2-normalized `FixedSizeList(Float32, dim)`; the requested dimension is driven by the target column width

## `@embed` schema annotation

Mark a Vector property with `@embed("source_text_property")`. Today this is a **catalog annotation** consumed
by the query typechecker and linter: it records which String property is the embedding source and lets
`nearest($v, "string")` auto-embed a query string for comparison against that vector column.

**It does not embed at ingest.** Stored vectors are supplied directly in your load data, or pre-filled by the
offline `omnigraph embed` pipeline below. (Ingest-time execution of `@embed` is a planned enhancement; until
it ships, populate the vector column yourself.)

## CLI `omnigraph embed` (offline file pipeline)

Operates on **JSONL files** (not on a graph), using the same engine client. Three modes (mutually exclusive):

- (default) `fill_missing` — only embed rows whose target field is empty
- `--reembed-all` — overwrite all
- `--clean` — strip embeddings

Inputs are either a single seed manifest YAML or `--input/--output/--spec`. Selectors `--type T`, `--select T:field=value` filter rows. Streams JSONL → JSONL.
