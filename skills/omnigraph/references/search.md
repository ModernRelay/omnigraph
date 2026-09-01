# Search & Embeddings

## Contents
- Embeddings are schema-declared
- Offline embedding pipeline
- Search functions in queries
- The key pattern: scope first, rank second
- Model / config

Vector embeddings and text search in Omnigraph.

## Embeddings are Schema-Declared

```pg
node Chunk {
    text: String
    chunk_index: I32
    embedding: Vector(1536) @embed("text", model="openai/text-embedding-3-large") @index
    createdAt: DateTime
}
```

- `Vector(N)` — fixed-size float vector
- `@embed("source_prop", model="model-id")` — associates the vector with its
  String source and, optionally, the exact model space
- `@index` — declares derived index intent and can accelerate vector search;
  correctness falls back to an exact scan when coverage is missing

The schema says **where** embeddings live and **what** they come from. It does
not populate vectors during a load. Supply vectors in input or prepare JSONL
with the offline command.

## Offline Embedding Pipeline

`omnigraph embed` transforms JSONL files; it does **not** mutate a graph:

```bash
omnigraph embed --input raw.jsonl --output embedded.jsonl --spec embeddings.json
```

By default it fills missing vectors. Load the output explicitly afterward.

Use the same file/spec form with `--reembed-all` to replace selected vectors,
or `--clean` to remove them. `--type` and `--select` narrow the records. A seed
manifest is an alternative:

```bash
omnigraph embed --seed embed-config.yaml --reembed-all
omnigraph embed --seed embed-config.yaml --clean
omnigraph embed --seed embed-config.yaml --select "Chunk:chunk_index=42"
```

Changing source text, source-property metadata, or model requires generating
replacement vectors; neither `merge` nor `overwrite` does that automatically.

## Search Functions in Queries

Ranking functions are order operators, not filters. `nearest` and `rrf` require
`limit N`; BM25 alone does not, though a limit keeps output bounded.

### Vector similarity

```gq
query nearest_chunks($q: Vector(1536)) {
    match { $c: Chunk }
    return { $c.text }
    order { nearest($c.embedding, $q) }
    limit 10
}
```

### BM25 text ranking

```gq
query top_titles($q: String) {
    match { $d: Doc }
    return { $d.slug, $d.title }
    order { bm25($d.title, $q) }
    limit 10
}
```

### Hybrid (Reciprocal Rank Fusion)

```gq
query hybrid($vq: Vector(1536), $tq: String) {
    match { $d: Doc }
    return { $d.slug, $d.title }
    order { rrf(nearest($d.embedding, $vq), bm25($d.title, $tq)) }
    limit 10
}
```

### Text filter (not ranking — no `limit` required)

```gq
match {
    $d: Doc
    search($d.title, $q)          // full-text filter
    fuzzy($d.title, $q, 2)        // fuzzy filter, max 2 edits
    match_text($d.body, $q)       // regular full-text filter (not phrase search)
}
```

## The Key Pattern: Scope First, Rank Second

Filter with graph traversal before invoking vector or text ranking. Ranking over a narrow set is both cheaper and more relevant.

```gq
query related_chunks($artifact_slug: String, $q: Vector(1536)) {
    match {
        $a: InformationArtifact { slug: $artifact_slug }
        $c partOfArtifact $a                      // scope: only this artifact's chunks
    }
    return { $c.text }
    order { nearest($c.embedding, $q) }           // rank: vector similarity within scope
    limit 10
}
```

Don't rank over the entire chunk set if you know a traversal can narrow it first.

## Model / Config

The offline command and a served graph have separate configuration surfaces,
but must resolve to the same provider/model space. Stored and query vectors
must match the schema's `Vector(N)` dimension; recording `model=` on `@embed`
makes that contract explicit.

| Provider | Default model | Credential |
|---|---|---|
| `openai-compatible` (default, OpenRouter endpoint) | `openai/text-embedding-3-large` | `OPENROUTER_API_KEY` |
| `openai` | `text-embedding-3-large` | `OPENAI_API_KEY` |
| `gemini` | `gemini-embedding-2` | `GEMINI_API_KEY` |
| `mock` | deterministic test vectors | none |

Configure direct/offline use with `OMNIGRAPH_EMBED_PROVIDER`,
`OMNIGRAPH_EMBED_BASE_URL`, and `OMNIGRAPH_EMBED_MODEL`. Deadline/retry controls
are `OMNIGRAPH_EMBED_DEADLINE_MS`, `OMNIGRAPH_EMBED_TIMEOUT_MS`,
`OMNIGRAPH_EMBED_RETRY_ATTEMPTS`, and `OMNIGRAPH_EMBED_RETRY_BACKOFF_MS`;
`OMNIGRAPH_EMBEDDINGS_MOCK` forces the mock provider.

For a served graph, declare a named provider under `providers.embedding` in
`cluster.yaml` and bind it with `graphs.<id>.embedding_provider`. API keys must
be `${ENV_VAR}` references and are resolved by the server at startup. Generated
vectors are finite, nonzero, and L2-normalized.

After upgrading a Lance 9/10 store, full-text queries can require
`rebuild-full-text-indexes` on each live branch. Ordinary reads and vector
search do not depend on that rebuild; see [`commands.md`](commands.md#rebuild-full-text-indexes--explicit-analyzer-upgrade).
