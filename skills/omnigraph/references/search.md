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

- `Vector(N)` — fixed-size float vector; `Vector(N, distance = "cosine")`
  declares its distance (`l2`, the default, `cosine` or `dot`), which
  `distance` and `nearest` read. A `cosine` field refuses a zero vector:
  writing one fails validation and a zero query vector is refused
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

A ranking is a relation drawn into `match` with `from`, which names its score
with `as`: `from nearest(rel, $c.embedding, $q, k: n) as $distance`,
`from bm25(rel, $d.title, $q) as $score` (any term of the String `$q`; `terms($q)`
asks for every term, `terms($q, max_edits: n)` an edit budget),
`from rrf(arm, arm, k: 60, weights: [1.0, 1.0]) as $score`. `rel` is the population, a relation
named with `let` before the ranking, `let $pop = { match { … } where … return { $c } }`,
whose `return` lists every binding the rest of the query reads (an inline read
in its place also works). `from` adds the population's columns and the score
under the name `as` writes; a `from` over a ranking (or a `let` name bound to
one) without `as`, or an `as` on any other `from`, is refused (`T85`) with the
fix printed. Inside an
`rrf` arm a score has no name. A ranking written as a call in `order` or
`return` (`order { nearest($c.embedding, $q) }`) is refused (`T65`), and the
diagnostic prints the let-first `from` form as a machine-applicable fix.

Order by the score: `order { $distance }` (ascending distance) or
`order { $score desc }` (descending relevance), or put another key first;
entity IDs break the remaining ties. This order also holds through
traversals. `nearest` requires its window `k` (`T17`): the `k` nearest
targets, every row of each; `limit` is a separate cut, and elements or a
`where` after the `from` remove rows without a refill. BM25 alone needs neither, though a limit keeps output
bounded. A `bm25` ranking reads every text match before the final sort, so
equal-score rows at a `limit` cutoff are chosen by the secondary keys and then
entity ID, not by scan order.

### Vector similarity

```gq
query nearest_chunks($q: Vector(1536)) {
    match { let $pop = { match { $c: Chunk } return { $c } }, from nearest($pop, $c.embedding, $q, k: 10) as $distance }
    return { $c.text }
    order { $distance }
    limit 10
}
```

The query value may also be a `String` (`$q: String`): the configured embedding
provider embeds it at query time. When `@embed(..., model=...)` records a
model, the resolved provider model must match it exactly.

### Exact distance

`nearest` is the answer to an unqualified request for the most similar
targets. `distance(rel, $c.embedding, $q)` is for an exact answer or a radius,
and it scans the population: every row of `rel` with its exact distance to
`$q` under the field's declared distance (`l2`: sum of squared differences;
`cosine`: `1 - cos`; `dot`: `1 - dot`, can be negative), whatever the index
covers; a row whose vector is null (or a zero vector on a `cosine` field) is
dropped, and a non-finite component or distance is refused (`E3`).
`order { $distance }` with `limit n` is the exact window, ties to the smaller
ID; `where $distance < $r` is a radius. It is not an `rrf` arm (`T86`).

```gq
query within($q: Vector(1536), $r: F64) {
    match { let $pop = { match { $c: Chunk } return { $c } }, from distance($pop, $c.embedding, $q) as $distance }
    where $distance < $r
    return { $c.text, $distance }
    order { $distance }
}
```

### BM25 text ranking

```gq
query top_titles($q: String) {
    match { let $pop = { match { $d: Doc } return { $d } }, from bm25($pop, $d.title, $q) as $score }
    return { $d.slug, $d.title }
    order { $score desc }
    limit 10
}
```

### Hybrid (Reciprocal Rank Fusion)

`let` names the population once and both arms rank it. `rrf` takes two arms;
identical arms, or arms that rank two different bindings, are refused (`T75`).
An arm over `bm25` is not run (`T86`): fuse two `nearest` arms.
A target scores the sum over the arms that selected it of
`weight / (k + rank)`, ranks counted from 1; `weights: [w, w]` (one number in
`(0, 1000000]` per arm, default 1.0, another length `T75`) gives an arm more
say, so `weights: [1.0, 3.0]` lets the second arm lead.
It yields one row per target with the fused score and no other population
column; a population that reaches a target several times adds no row, and a
read that needs a population column draws it again after the `from`
(`$t: Tag, $d tagged $t`), correlated on the target:

```gq
query hybrid($vq: Vector(1536), $tq: String) {
    match {
        let $all = { match { $d: Doc } return { $d } },
        from rrf(nearest($all, $d.embedding, $vq, k: 10), nearest($all, $d.embedding, $tq, k: 10), k: 60) as $score,
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
    match { let $pop = { match { $d: Doc } return { $d } }, from bm25($pop, $d.title, $q) as $score }
    return { $d.slug, $score as score }
    order { $score desc }
    limit 10
}
```

A distance drawn `as $distance` works the same way and returns the exact
distance under the field's distance, ascending, and an `rrf` score is the fused score as `F64`. No score column has
a fixed name: it is the name the `from`'s `as` writes (`as $relevance`
returns the column `relevance`), and an alias in `return` renames it like any
column. A `let` name bound to a ranking carries an unnamed score too, so its
`from` takes `as`; an inline read that returns a named score takes none
(`T85`). Full-text predicates are not return
items (`T86`).

A ranking may feed aggregates, but it only selects the rows that are
aggregated: the `k` window under `nearest`, every text match under
`bm25`. Groups are not score-ranked, and the score is not an order key over
groups (`T42`): the written keys order the groups (or `return distinct` rows),
then every group key ascending; with neither keys nor a `limit`, output is
unordered.

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
lists the returned columns that hold the ranking's own score, by the names
the read returns them under (`["score"]` above for `$score as score`). A read that
draws from no ranking has no `ranking`.

### Text filter (not ranking — no `limit` required)

```gq fragment
match { $d: Doc }
where match_terms($d.title, terms($q, mode: any))                 // any analyzed term of $q
  and match_terms($d.title, terms($q, mode: any, max_edits: 2))   // within 2 edits
  and match_terms($d.body, terms($q))                             // every analyzed term (mode: all)
```

`match_terms` is the one lexical predicate. `search`, `match_text` and `fuzzy`
are refused (`T6`) with their fix: `match_terms(f, terms(q, mode: any))`, and
`max_edits: 2` for a `fuzzy` without a budget. A full-text call (`match_terms`,
`bm25`) needs a full-text index on its property: a one-column `@index` or
`@key` on a free-text String node property. A call on any other property, an
enum included, is refused at type checking (`T27`); compare enums with `=`.
The field's analyzer decides what matches (next section). `match_terms` is a
Boolean operand like any other in a `where`: true where the row is among the
call's matches, false elsewhere, never null. Write `not match_terms(...)`,
`match_terms(...) or ...` or `match_terms(...) = false`; the rows are the same
whether or not the index covers them yet. Read as a value in `return`,
`order` or a block's aggregate, a call is `T86`.

### Analyzers

The schema, never the query, chooses a field's analysis:
`body: String @index(fulltext, analyzer = "english_v1")`. The profiles:

| Profile | Lowercase | Stemming and stop words | ASCII folding |
|---|---|---|---|
| `standard_v1` | yes | no | no |
| `standard_folded_v1` | yes | no | yes |
| `english_v1` | yes | Snowball English stemmer, then the profile's stop list | yes |

A field written `@index` or `@index(fulltext)` without `analyzer` is
`english_v1`. All split on every non-alphanumeric character (Lance's `simple`
tokenizer) and keep tokens of any length. `"run"` matches
`"Runners were RUNNING"` under `english_v1` only; `"cafe"` matches `"Café"`
under `standard_folded_v1` and `english_v1`. `english_v1`'s stop list holds
NLTK's 198 English stop words and each one's stem, so `only` and `very` match
nothing and `"human beings"` analyzes to `human`; a query of only stop words is
`T20` when the read runs. `scorer = "none"` keeps the field matchable and
makes `bm25` over it `T87`, also as an `rrf` arm (`T87` comes before `T86`);
use a `where` filter there. No call takes an analyzer (`terms(…)` with one is
`T71`).

Every full-text call is exact and reads no index: the exact lexical scan
normalizes each value to NFC, analyzes it with the profile and decides each
row, so built, partly built and unbuilt indexes give the same rows.
`match_terms($d.f, terms($q, mode: all | any, max_edits: 0..=2))` matches a
query term to a stored term within the budget (one edit per inserted, deleted
or substituted character; a swap costs two). `bm25` is `bm25_v1`, an `F64`
scored against the whole field (every entity whose value has at least one
analyzed term, before filters), so a filter never changes a surviving row's
score; the plan shows a `Score` node, not a ranked scan. `rrf` with a `bm25`
arm is `T86`. A query over 64 KiB or 1,024 distinct terms, or a value over
16 MiB, fails with a resource-limit error (HTTP 413, detail `resource_limit`
with `resource` `lexical_query_bytes`, `lexical_query_terms` or
`lexical_value_bytes`); the scan's terms and comparisons count against
`read_work_limit`. Changing a field's profile is a schema apply that replaces
its index with an untrained one; `index build` builds the postings, which no
query reads.

## The Key Pattern: Scope First, Rank Second

Filter with graph traversal inside the ranking's population. Constrain the ranked binding with a traversal or filter there so the ranking answers within that scope: `limit 10` then means the top ten that satisfy the pattern.

```gq
query related_chunks($artifact_slug: String, $q: Vector(1536)) {
    match {
      let $pop = { match { $c: Chunk, $a: InformationArtifact, $c partOfArtifact $a } where $a.slug = $artifact_slug return { $c, $a } },
      from nearest($pop, $c.embedding, $q, k: 10) as $distance,
    }
    return { $c.text }
    order { $distance }           // rank: vector similarity within scope
    limit 10
}
```

Scoping fixes the answer, not always the cost: a `nearest` or `rrf` scan
prefilters to the traversal's eligible entities only when they are few:
within `OMNIGRAPH_RRF_GATE_RATIO` of the ranked type (default 10%) and at most
`OMNIGRAPH_RRF_GATE_MAX_IDS` (default 100000). Otherwise the ranked type is
scanned and the traversal keeps the rows it reaches. A `bm25` scores the
population's rows, but its statistics come from the whole field, so the exact
lexical scan reads that field across the whole type.

The ranked binding starts the traversal whichever binding the population
declares first, and whether or not it is declared: with
`$a: InformationArtifact` first, then `$c partOfArtifact $a`, ranked on
`$c`, the query ranks `Chunk` and keeps the chunks the traversal reaches.

A standalone `nearest` ranking widens an underfilled candidate set, finally
using an exact scan if needed to fill the limit with available survivors.
An `rrf` vector arm retains a top-k window: an entity outside that window has
no vector contribution, so filtering through a traversal can shorten or change
the fused answer.

A `nearest` scan uses only the IVF_FLAT vector index Omnigraph builds for the
field's declared distance; an index built for another distance or written
another way (quantized vectors, HNSW) is skipped and every row is ranked flat.
Changing a field's distance is an in-place `schema apply` with no reload:
reads answer exactly at once, and `omnigraph index build` (or `optimize`)
rebuilds the index for the new distance.

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

No GQ query reads a full-text index, so none fails for one. A raw
`contains_tokens` scan filter (an SDK read, not GQ) reads the postings and fails
with HTTP `409` `full_text_index_rebuild_required` on an index without a valid
certificate, such as one built before Lance 11; it clears with
`index rebuild --kind full-text` on the branch; see
[`commands.md`](commands.md#index-rebuild--explicit-analyzer-upgrade).
