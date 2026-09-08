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

### Return the ranking score

Project the same `nearest(...)` or `bm25(...)` expression that leads `order`:

```gq
query scored_titles($q: String) {
    match { $d: Doc }
    return { $d.slug, bm25($d.title, $q) as score }
    order { bm25($d.title, $q) desc }
    limit 10
}
```

The result is F32: BM25 relevance or nearest squared L2 distance, computed by
the ordering. Without an alias its column is `d._score` or `d._distance`.
T33 refuses a different/missing leading order expression. An RRF arm is not
a leading order expression; `rrf(...)` itself cannot be returned (T37), and
scores cannot appear under aggregates. Search predicates belong in `match`.

### Filtered nearest results and cost

A standalone nearest ordering widens candidates when later filters/traversals
leave `limit` short: four times, then sixteen times, then one exact pass over
the whole type if needed. Fewer survivors than `limit` can therefore mean a
whole-type pass on every execution. Selective traversal scopes can prefilter
the search. The default IVF partition cap is 20 per index delta
(`OMNIGRAPH_ANN_NPROBES`; `0` removes it); a short scan can widen that cap.
A full candidate count does not make ANN ranking exact.

An RRF nearest arm keeps its top-k window and widens only its own probe cap;
later traversal filtering can shorten the fused answer. Do not assume the
standalone nearest fill guarantee applies to RRF. Rank within a known scope
when possible, and distinguish fewer eligible rows from approximate ranking.

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

For the earlier v0.9→v0.10 upgrade, full-text queries require compatible
rebuilt indexes on each branch that uses them. Current v7 binaries first require
a v6 store to be exported/rebuilt at a new root; an FTS rebuild cannot bypass
that format boundary. See [migrations](migrations.md).
