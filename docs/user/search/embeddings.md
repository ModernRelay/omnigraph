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

The vector is the embedding of the record's `body` value exactly as stored,
and a text `nearest($d.embedding, $q)` embeds `$q` exactly as given. Neither
side adds a type name, a field label or any other template, and neither trims
whitespace, so a record whose `body` equals the query text embeds the same
input. The one difference a provider can see is the retrieval role: Gemini
receives `RETRIEVAL_DOCUMENT` for stored text and `RETRIEVAL_QUERY` for a
query, and the other providers receive identical requests. Vectors you compute
yourself must embed the source value the same way to be comparable with text
queries.

When `model` is recorded, a text `nearest` query is rejected unless the active
provider resolves to exactly that model id, and so is an
[`omnigraph embed`](#offline-file-pipeline) run that fills the property.
Changing the recorded source or model is not an in-place schema migration;
rebuild or re-embed the data instead.

`@embed` does not populate the property during a load, even with an embedding
provider configured. Supplied vectors are preserved, omitted nullable vectors
remain null, and missing required vectors refuse the load. Supply vectors in
input data or prepare seed files with the offline command.

Successful HTTP and CLI JSON load results include `embedding_generation`:
`"unsupported"` means the load touched a node type with an `@embed` property;
`null` means it did not. This describes the load capability, including when
every vector was supplied. Human CLI output prints the corresponding guidance.

## Offline file pipeline

`omnigraph embed` reads and writes JSONL files; it does not mutate a graph.

```bash
omnigraph embed --schema schema.pg --input raw.jsonl --output embedded.jsonl
```

The schema is the only statement of what to embed. Each `@embed` declaration is
a vector the command fills: a record of the declaring node type gets that
property set to the embedding of its source property, at the dimension of the
property's `Vector(N)`. A type may declare several such vectors. Pass the schema
the graph was initialized or last applied with; `omnigraph schema show` prints a
graph's accepted schema. A schema without an `@embed` declaration is refused.

When an `@embed` records a `model`, the command refuses to run unless the
embedding provider resolves to exactly that model id, the check a text
`nearest` query applies to the same property. The refusal comes before the
input is read or the output is written. Use `--type` to leave out a type whose
recorded model the provider does not serve.

The source's string value is embedded unchanged, as described under
[Schema annotation](#schema-annotation). A processed record whose source is
absent, null, or blank gets no vector for it: whatever the vector property
held, including a null or an empty list, is removed and counted as cleaned.
When that property is a required `Vector(N)` (no `?`), the record is refused
instead, with its line number, since a load would refuse it without the vector.
A source value that is not a string is refused.

By default it fills missing vectors. Use `--reembed-all` to replace selected
vectors or `--clean` to remove them. `--type` restricts the run to the named
node types, each of which must declare an `@embed` vector, and `--select`
restricts the records processed.

`--json` prints a summary: the `input`, `output` and `schema` paths, the
`records` read, the `selected_records`, and the `mode`. A record counts once in
`embedded_records` when any of its vectors was embedded and once in
`cleaned_records` when any was removed. `vectors` lists each processed `@embed`
vector with its `type`, `property`, `source` and `dimension`, and `model` is the
model the provider resolved (empty with `--clean`, which calls no provider).

### Seed manifests

A seed manifest names the three files in one place, each path relative to the
manifest:

```yaml
schema: ./schema.pg
sources:
  raw_seed: ./data/seed.jsonl
artifacts:
  embedded_seed: ./build/seed.embedded.jsonl
```

```bash
omnigraph embed --seed seed.yaml
```

`schema` and `sources.raw_seed` are required; `artifacts.embedded_seed` defaults
to `./build/seed.embedded.jsonl`. A key the command does not read is refused.
