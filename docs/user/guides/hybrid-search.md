# Hybrid Search End to End

This guide builds a small document graph and runs a **hybrid** query that fuses
full-text (BM25) and vector (k-NN) rankings with Reciprocal Rank Fusion. You do
not build indexes by hand — the engine maintains them; a freshly loaded row is
searchable immediately.

See [search](../search/index.md) for the function reference and
[embeddings](../search/embeddings.md) for the full provider/env matrix.

## 1. Schema

A document with a text body for full-text search and a vector for similarity.
`@embed("body")` tells the engine to embed the `body` text into `embedding` at
load time:

```
node Document {
  title: String,
  body: String,
  embedding: Vector(768) @embed("body"),
}
```

```bash
omnigraph init --schema schema.pg docs.omni
```

## 2. Configure embeddings

Ingest-time embedding uses the engine's embedding client. Point it at your
provider (see [embeddings](../search/embeddings.md) for every variable):

```bash
export GEMINI_API_KEY=...        # ingest-time document embeddings
# For local experimentation without a provider, deterministic mock vectors:
# export OMNIGRAPH_EMBEDDINGS_MOCK=1
```

If you would rather supply vectors yourself, drop `@embed` and include the
`embedding` array in each input record instead.

## 3. Load

```bash
omnigraph load --data docs.jsonl --mode overwrite docs.omni
```

Each row's `body` is embedded into `embedding` as it loads. The BM25 (full-text)
and vector indexes are maintained by the engine — there is no separate build step.

## 4. Query — full-text, vector, then hybrid

Full-text only:

```gq
query text_search($q: String) {
  match { $d: Document { } }
  return { $d.title, bm25($d.body, $q) as score }
  order { score desc }
  limit 10
}
```

Vector only (the query text is embedded at query time; `nearest` requires a
`limit`):

```gq
query vector_search($q: String) {
  match { $d: Document { } }
  return { $d.title, nearest($d.embedding, $q) as score }
  order { score desc }
  limit 10
}
```

Hybrid — fuse both rankings with `rrf`:

```gq
query hybrid($q: String) {
  match { $d: Document { } }
  return {
    $d.title,
    rrf( nearest($d.embedding, $q), bm25($d.body, $q) ) as score
  }
  order { score desc }
  limit 10
}
```

Run it:

```bash
omnigraph query --query queries.gq hybrid \
  --params '{"q":"trends in AI safety"}' --format table --store docs.omni
```

`rrf` combines the two rankings without needing their score scales to match, so
you get a single fused ordering from a lexical signal and a semantic one.
