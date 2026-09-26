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
not populate vectors during mutation or load. Supply required vectors in input
or prepare JSONL with the offline command. A nullable `Vector(N)?` target may
remain null even when its source text is supplied.

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

Ranking functions lead the `order` clause. `nearest` and `rrf` require `limit N`;
BM25 alone does not, though a limit keeps output bounded. `nearest` sorts by
ascending distance and `bm25` by descending relevance; secondary keys follow
the score, then entity IDs break ties. This order also holds through traversals.
A bounded BM25 scan with no secondary keys may select equal-score rows at its
cutoff by scan order before the final sort.

Omit a direction on a search key: `bm25(...)` always ranks by descending score
(`asc`/`desc` are ignored), and a direction after `nearest(...)` is a parse
error. Only one search function may lead `order`; a second one fails when the
query runs ("search functions must lead the order clause"), even though `lint`
passes.

### Vector similarity

```gq
query nearest_chunks($q: Vector(1536)) {
    match { $c: Chunk }
    return { $c.text }
    order { nearest($c.embedding, $q) }
    limit 10
}
```

The query value may also be a `String` (`$q: String`): the configured embedding
provider embeds it at query time. When `@embed(..., model=...)` records a
model, the resolved provider model must match it exactly.

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

### Projecting scores

Repeat the leading order expression to return its score:

```gq
query scored_titles($q: String) {
    match { $d: Doc }
    return { $d.slug, bm25($d.title, $q) as score }
    order { bm25($d.title, $q) }
    limit 10
}
```

`nearest(...) as distance` works the same way and returns squared L2 distance.
Without an alias, the result column is `d._score` or `d._distance`. A different
expression or one without the matching leading order key is refused (`T33`).
Ranks under aggregates (`T32`), `rrf(...)` (`T37`), and full-text predicates
(`T35`) cannot be projected; an expression used only as an `rrf` arm is not a
projectable score either.

A search ordering may accompany aggregates, but it only selects the rows that
are aggregated: the top-`limit` window under `nearest`, every text match under
`bm25`. Groups are not score-ranked, and projecting a score beside an aggregate
is refused (`T9`).

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

A standalone `nearest` ordering widens an underfilled candidate set, finally
using an exact scan if needed to fill the limit with available survivors.
An `rrf` vector arm retains a top-k window: an entity outside that window has
no vector contribution, so filtering through a traversal can shorten or change
the fused answer. Full-text arms in `rrf` remain unbounded over their eligible
matches.

An indexed `nearest` scan reads a bounded number of partitions per index delta
(`OMNIGRAPH_ANN_NPROBES`, default 20; `0` removes the cap) and widens only to
fill the limit, so a filled limit does not make the ANN ranking exact. A scoped
`nearest` whose survivors are always fewer than `limit` pays an exact
whole-type pass on every execution. `OMNIGRAPH_RRF_GATE_RATIO` and
`OMNIGRAPH_RRF_GATE_MAX_IDS` tune the prefilter that a selective traversal
pushes into a `nearest` or `rrf` scan; leave them unset in normal operation.

`OMNIGRAPH_ANN_NPROBES` and `OMNIGRAPH_RRF_PLAN` are not free tunables. Each is
the process default of a `process`-scope session setting (`ann_nprobes`,
`rrf_plan`): the server reads it once at startup and the CLI once per run, a
value outside the setting's row refuses that start instead of running a
default, and `show all;` reports the setting with its value and source.

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
