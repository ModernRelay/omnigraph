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

The spec maps each type to its vector and the one source property its
`@embed` names; `fields` must hold exactly that property:

```json
{ "dimension": 1536, "types": { "Chunk": { "target": "embedding", "fields": ["text"] } } }
```

The source value is embedded exactly as stored, with no type or field-name
template, which is also how a text `nearest(rel, $c.embedding, $q, k: n)` embeds `$q`; a
chunk whose text equals the query lies at distance zero under a
role-independent model. A record with an absent, null or blank source gets no
vector. Vectors from older `omnigraph embed` builds embedded a
`type: <T>\n<field>: <value>` template; regenerate them with `--reembed-all`.

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

A ranking is a relation drawn into `match` with `from`:
`from nearest(rel, $c.embedding, $q, k: n)`, `from bm25(rel, $d.title, $q)`,
`from rrf(arm, arm, k: 60)`. `rel` is the population, an inline read
`{ match { … } where … return { $c } }` whose `return` lists every binding the rest of
the query reads, or a relation named with `let`. `from` adds the score column:
`$distance` for `nearest`, `$score` for `bm25` and `rrf`. A ranking written as a
call in `order` or `return` (`order { nearest($c.embedding, $q) }`) is refused
(`T65`), and the diagnostic prints the `from` form as a machine-applicable fix.

Order by the score as the leading key: `order { $distance }` (ascending
distance) or `order { $score desc }` (descending relevance); secondary keys
follow the score, then entity IDs break ties. This order also holds through
traversals. `nearest` requires its window `k` (`T17`) and a `limit N`; give
them the same value. BM25 alone needs neither, though a limit keeps output
bounded. A `bm25` ranking reads every text match before the final sort, so
equal-score rows at a `limit` cutoff are chosen by the secondary keys and then
entity ID, not by scan order.

### Vector similarity

```gq
query nearest_chunks($q: Vector(1536)) {
    match { from nearest({ match { $c: Chunk } return { $c } }, $c.embedding, $q, k: 10) }
    return { $c.text }
    order { $distance }
    limit 10
}
```

The query value may also be a `String` (`$q: String`): the configured embedding
provider embeds it at query time. When `@embed(..., model=...)` records a
model, the resolved provider model must match it exactly.

### BM25 text ranking

```gq
query top_titles($q: String) {
    match { from bm25({ match { $d: Doc } return { $d } }, $d.title, $q) }
    return { $d.slug, $d.title }
    order { $score desc }
    limit 10
}
```

### Hybrid (Reciprocal Rank Fusion)

`let` names the population once and both arms rank it. `rrf` takes two arms;
identical arms, or arms that rank two different bindings, are refused (`T75`):

```gq
query hybrid($vq: Vector(1536), $tq: String) {
    match {
        let $all = { match { $d: Doc } return { $d } },
        from rrf(nearest($all, $d.embedding, $vq, k: 10), bm25($all, $d.title, $tq), k: 60),
    }
    return { $d.slug, $d.title }
    order { $score desc }
    limit 10
}
```

### Projecting scores

Return the score column, aliased or not:

```gq
query scored_titles($q: String) {
    match { from bm25({ match { $d: Doc } return { $d } }, $d.title, $q) }
    return { $d.slug, $score as score }
    order { $score desc }
    limit 10
}
```

`$distance as distance` works the same way and returns squared L2
distance, and an `rrf` `$score` is the fused score as `F64`. Without an alias,
the result column is `score` or `distance`; `$score as _score` and
`$distance as _distance` give those names. Full-text predicates are not return
items (`T38`).

A ranking may feed aggregates, but it only selects the rows that are
aggregated: the top-`limit` window under `nearest`, every text match under
`bm25`. Groups are not score-ranked, and the score is not an order key over
groups (`T42`): the written keys order the groups (or `return distinct` rows),
then every group key ascending; with none, output is unordered.

### What a ranked read reports

A read that draws from `nearest`, `bm25` or `rrf` carries a `ranking` object in its
JSON envelope (`query --json`, `--format toon`, the `--format jsonl` metadata
line, `POST /query`, stored-query calls):

```json
"ranking": { "by": "rrf", "order": "descending", "exhaustive": true, "score_columns": ["score"] }
```

`order` is `ascending` for a `nearest` distance and `descending` for a `bm25`
or fused score. `exhaustive: false` means the `ann_nprobes` cap left
partitions unsearched, so a nearer row may exist (raise `ann_nprobes`); `bm25`
is always exhaustive, and `rrf` is when every `nearest` arm is. `score_columns`
lists the returned columns that hold the ranking's own score. A read that
draws from no ranking has no `ranking`.

### Text filter (not ranking — no `limit` required)

```gq fragment
match { $d: Doc }
where search($d.title, $q)                    // full-text filter
  and fuzzy($d.title, $q, max_edits: 2)       // fuzzy filter, max 2 edits
  and match_text($d.body, $q)                 // regular full-text filter (not phrase search)
```

A full-text call (`search`, `fuzzy`, `match_text`, `bm25`) needs a full-text
index on its property: a one-column `@index` or `@key` on a free-text String
node property. A call on any other property, an enum included, is refused at
type checking (`T27`); compare enums with `=`. The analyzer folds case and
stems, so `"deep"` matches `"Deep Learning"`. `search`, `fuzzy` and
`match_text` must stand alone as a conjunct of `where` or be compared
`= true`, joined only by `and`; `= false`, `not search(...)` and `or` are
refused (`T38`). Negate with a block that re-mentions the binding:
`not exists { $d: Doc where search($d.title, $q) }`.

## The Key Pattern: Scope First, Rank Second

Filter with graph traversal inside the ranking's population. Constrain the ranked binding with a traversal or filter there so the ranking answers within that scope: `limit 10` then means the top ten that satisfy the pattern.

```gq
query related_chunks($artifact_slug: String, $q: Vector(1536)) {
    match {
      from nearest({ match { $c: Chunk, $c partOfArtifact $a } where $a.slug = $artifact_slug return { $c, $a } }, $c.embedding, $q, k: 10),
    }
    return { $c.text }
    order { $distance }           // rank: vector similarity within scope
    limit 10
}
```

Scoping fixes the answer, not always the cost: a `nearest` or `rrf` scan
prefilters to the traversal's eligible entities only when they are few:
within `OMNIGRAPH_RRF_GATE_RATIO` of the ranked type (default 10%) and at most
`OMNIGRAPH_RRF_GATE_MAX_IDS` (default 100000). Otherwise, and
always for a standalone `bm25`, the ranked type is scanned and the traversal
keeps the rows it reaches.

The ranked binding starts the traversal whichever binding the population
declares first, and whether or not it is declared: with
`$a: InformationArtifact` first, then `$c partOfArtifact $a`, ranked on
`$c`, the query ranks `Chunk` and keeps the chunks the traversal reaches.

A standalone `nearest` ranking widens an underfilled candidate set, finally
using an exact scan if needed to fill the limit with available survivors.
An `rrf` vector arm retains a top-k window: an entity outside that window has
no vector contribution, so filtering through a traversal can shorten or change
the fused answer. Full-text arms in `rrf` remain unbounded over their eligible
matches.

A `nearest` scan uses only the IVF_FLAT, L2 vector index Omnigraph builds; an
index written another way (another metric, quantized vectors, HNSW) is
skipped and every row is ranked flat.

An indexed `nearest` scan reads a bounded number of partitions per index delta
(`OMNIGRAPH_ANN_NPROBES`, default 20; `0` removes the cap) and widens only to
fill the limit, so a filled limit does not make the ANN ranking exact. A scoped
`nearest` whose survivors are always fewer than `limit` pays an exact
whole-type pass on every execution. `OMNIGRAPH_RRF_GATE_RATIO` and
`OMNIGRAPH_RRF_GATE_MAX_IDS` tune the prefilter that a selective traversal
pushes into a `nearest` or `rrf` scan; leave them unset in normal operation.

`OMNIGRAPH_ANN_NPROBES` and `OMNIGRAPH_RRF_PLAN` are not free tunables. Each is
the process default of a session setting (`ann_nprobes`, `request` scope;
`rrf_plan`, `process` scope): the server reads it once at startup and the CLI once per run, a
value outside the setting's row refuses that start instead of running a
default, and `show all;` reports the setting with its value and source. An
ad-hoc query may set `ann_nprobes` for itself (`set ann_nprobes = 40;` before
the declaration, `--set ann_nprobes=40`, or the `settings` field of
`POST /query`); `rrf_plan` is refused in a served request.

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

A full-text call can fail with HTTP `409` before any scan.
`full_text_index_required` (a table version holds rows but no part of the
index) clears with
`omnigraph index build --target <target> --branch <branch>`.
`full_text_index_rebuild_required` (an index without a certified analyzer,
such as one built before Lance 11) clears with `index rebuild --kind full-text`
on each live branch that needs search; see
[`commands.md`](commands.md#index-rebuild--explicit-analyzer-upgrade). Retrying
clears neither, and historical snapshots of the old version stay refused.
Ordinary reads and vector search depend on neither.
