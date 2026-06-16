# Search & Embeddings

## Contents
- Embeddings are schema-declared
- Generating embeddings
- Embeddings + `load --mode merge` interaction
- Search functions in queries
- The key pattern: scope first, rank second
- Model / config

Vector embeddings and text search in Omnigraph.

## Embeddings are Schema-Declared

```pg
node Chunk {
    text: String
    chunk_index: I32
    embedding: Vector(3072) @embed("text") @index
    createdAt: DateTime
}
```

- `Vector(N)` — fixed-size float vector
- `@embed("source_prop")` — what text field to embed from (quoted string)
- `@index` — enables vector search on this field

The schema says **where** embeddings live and **what** they come from. Queries don't recompute; they read.

## Generating Embeddings

### First time / refresh missing

```bash
omnigraph embed --seed embed-config.yaml
```

Default mode is `fill_missing` — only generates embeddings for rows without one.

### Re-embed everything

```bash
omnigraph embed --seed embed-config.yaml --reembed_all
```

Use when:
- You changed the source field: `@embed("body")` → `@embed("title")`
- You mutated text at scale and need fresh embeddings
- You switched embedding models (rare)

### Selective refresh

```bash
omnigraph embed --seed embed-config.yaml --select "Chunk:chunk_index=42"
```

Regenerate only rows matching the selector.

### Clean (delete) embeddings

```bash
omnigraph embed --seed embed-config.yaml --clean
```

## Embeddings + `load --mode merge` Interaction

**`load --mode merge` does NOT recompute embeddings.**

If you update rows whose source fields feed into `@embed(...)`, the source updates but the embedding stays stale.

Two fixes:
1. Run `omnigraph embed --reembed_all` after the merge
2. Use `load --mode overwrite` instead, which re-triggers embedding on load

## Search Functions in Queries

All ranking functions require `limit N` — they're order operators, not filters.

### Vector similarity

```gq
query nearest_chunks($q: Vector(3072)) {
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
query hybrid($vq: Vector(3072), $tq: String) {
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
    match_text($d.body, $q)       // phrase filter
}
```

## The Key Pattern: Scope First, Rank Second

Filter with graph traversal before invoking vector or text ranking. Ranking over a narrow set is both cheaper and more relevant.

```gq
query related_chunks($artifact_slug: String, $q: Vector(3072)) {
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

Omnigraph uses **two distinct embedding clients** — don't conflate them:

| Client | When it runs | Default model | Configured via |
|--------|--------------|---------------|----------------|
| **Engine / load-time** | At load, when an `@embed("source")` field is populated (and `omnigraph embed`) | `gemini-embedding-2-preview` (3072-dim) | `GEMINI_API_KEY`, `OMNIGRAPH_GEMINI_BASE_URL`, `OMNIGRAPH_EMBED_*`, `OMNIGRAPH_EMBEDDINGS_MOCK` |
| **Compiler / query-time** | When a query passes a *string* to a ranking op (e.g. `nearest($c.embedding, "some text")`) and the server auto-embeds it | `text-embedding-3-small` (OpenAI-style) | `NANOGRAPH_EMBED_MODEL`, `OPENAI_API_KEY`, `OPENAI_BASE_URL`, `NANOGRAPH_EMBEDDINGS_MOCK` |

The vector stored in the schema is produced by the **load-time (engine)** client, so `Vector(N)` must match that model's output dimension — `Vector(3072)` for `gemini-embedding-2-preview`. If you point the query-time client at a model with a different dimension than your stored vectors, similarity search returns garbage or errors — keep both sides on the same dimension. Vectors are stored L2-normalized.
