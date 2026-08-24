# Embeddings

OmniGraph can turn query text into a vector for
`nearest($document.embedding, $text)`. Use the same provider and model that
produced the stored document vectors; vectors from different models are not
comparable.

Generated vectors are finite, nonzero, and L2-normalized before use. The target
`Vector(N)` property determines their required dimension.

## Providers

| Provider | Configuration |
|---|---|
| `openai-compatible` | Default. Uses an OpenAI-compatible `/embeddings` endpoint; defaults to OpenRouter. |
| `openai` | Uses OpenAI directly. |
| `gemini` | Uses Google's embedding API. |
| `mock` | Deterministic local vectors for tests and development. |

For direct or embedded use, configure the provider with environment variables:

| Variable | Meaning |
|---|---|
| `OMNIGRAPH_EMBED_PROVIDER` | `openai-compatible`, `openai`, `gemini`, or `mock` |
| `OMNIGRAPH_EMBED_BASE_URL` | Override the provider endpoint |
| `OMNIGRAPH_EMBED_MODEL` | Override the model id |
| `OPENROUTER_API_KEY`, `OPENAI_API_KEY`, `GEMINI_API_KEY` | Provider credential |
| `OMNIGRAPH_EMBED_DEADLINE_MS` | Total call deadline; default 60,000 ms |
| `OMNIGRAPH_EMBED_TIMEOUT_MS` | Per-request timeout; default 30,000 ms |
| `OMNIGRAPH_EMBED_RETRY_ATTEMPTS` | Maximum attempts; default 4 |
| `OMNIGRAPH_EMBED_RETRY_BACKOFF_MS` | Initial retry backoff; default 200 ms |
| `OMNIGRAPH_EMBEDDINGS_MOCK` | Force the mock provider |

The default OpenRouter model is `openai/text-embedding-3-large`. The direct
OpenAI default is `text-embedding-3-large`; Gemini defaults to
`gemini-embedding-2`.

## Cluster configuration

Cluster-served graphs select a named provider in `cluster.yaml`:

```yaml
providers:
  embedding:
    default:
      kind: openai-compatible
      base_url: https://openrouter.ai/api/v1
      model: openai/text-embedding-3-large
      api_key: ${OPENROUTER_API_KEY}

graphs:
  knowledge:
    schema: knowledge.pg
    embedding_provider: default
```

Inline API keys are rejected. `${ENV_VAR}` references are resolved when the
server starts, not when the cluster configuration is planned or applied.

## Schema annotation

Associate a vector with its source text:

```pg
node Document {
  slug: String @key
  body: String
  embedding: Vector(1536) @embed("body", model="openai/text-embedding-3-large") @index
}
```

When `model` is recorded, a text `nearest` query is rejected unless the active
provider resolves to exactly that model id. Changing the recorded source or
model is not an in-place schema migration; rebuild or re-embed the data instead.

`@embed` does not populate the property during a load. Supply vectors in input
data or prepare seed files with the offline command.

## Offline file pipeline

`omnigraph embed` reads and writes JSONL files; it does not mutate a graph.

```bash
omnigraph embed --input raw.jsonl --output embedded.jsonl --spec embeddings.json
```

By default it fills missing vectors. Use `--reembed-all` to replace selected
vectors or `--clean` to remove them. `--type` and `--select` restrict the records
processed. A seed manifest can be supplied with `--seed` instead of separate
input, output, and spec paths.
