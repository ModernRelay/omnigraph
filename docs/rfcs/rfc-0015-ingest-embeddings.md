# RFC-015: Ingest-time `@embed` via the embedding reconciler

**Status:** Proposed
**Date:** 2026-06-21
**Tickets:** `iss-ingest-embed` (umbrella); relates to RFC-012 (embedding client),
invariant 7 (indexes are derived state)
**Author track:** maintainer-internal RFC

> Evidence tags: **[S]** verified in v0.7.1 source (`file:line`), **[U]** verified
> against an external system, **[D]** documented behavior. Unmarked = design intent.

## Summary

`@embed("source_text", model="…")` records *which* String property seeds a
Vector column and with *which* model, but **does not embed at write time** — the
loader and mutation paths never call the embedder, and stored vectors must be
supplied in the load data or pre-filled by the offline `omnigraph embed` file
pipeline ([embeddings.md:92](../user/search/embeddings.md) [D]).

This RFC closes that gap **without putting a network call on the write path.**
An embedding is *derived physical state* — the same class as an index — so it is
materialized by a **reconciler**, not inline: `load`/`mutate` store the source
text and leave the dependent vector **pending** (NULL); an `ensure_embeddings`
reconciler (sibling of today's `ensure_indices`) embeds the pending rows in
batches from manifest state, with bounded retry. A not-yet-embedded row is
queryable by traversal/filter immediately and becomes a `nearest()` hit once
materialized — documented partial coverage, exactly as for a deferred index.

This is invariant 7 applied to embeddings, and it is the design the Postgres
ecosystem converged on (Timescale pgai Vectorizer, Supabase pgmq) [U].

## Motivation

The natural request — "embed automatically when I add data" — has one obvious
implementation (embed inline during `mutate`/`load`) that is **wrong for this
codebase on three independent counts**:

1. **It is on the deny-list.** [invariants.md](invariants.md) forbids
   "synchronous inline vector/FTS index rebuilds on the write path." An
   embedding is the same shape (expensive, network-bound, derivable). The
   deny-list does not distinguish "build the index" from "compute the value the
   index covers."
2. **It blows the write-path cost contract.** RFC-013 (and step 3b,
   [rfc-013-step-3b-writetxn.md](rfc-013-step-3b-writetxn.md)) is making the
   write path O(1) and failure-bounded. A per-row embedding call is
   O(network-latency) and fallible — a single-row `mutate` would block on a
   model round-trip (the latency that makes this unacceptable for interactive
   mutations).
3. **It couples a logical write to a flaky external service.** Per pgai's stated
   rule: *"primary data-modification operations (INSERT/UPDATE/DELETE) should not
   be dependent on the embedding operation, otherwise the application is down
   every time the endpoint is slow or fails"* [U]. That is invariant 7's "a
   physical operation must never fail a logical one," verbatim from the field.

### Prior art (the three patterns)

| Pattern | Who | Fit |
|---|---|---|
| **Pre-embed / BYO vector** | raw pgvector, Qdrant, Milvus (classic), Atlas, Cloudflare Vectorize | omnigraph already has this — the `omnigraph embed` *file* pipeline. Keep it. |
| **Synchronous-inline at write** | **HelixDB** (`AddV(Embed(content), …)`) [U], Weaviate vectorizers, Milvus 2.5 functions, Pinecone integrated inference, LanceDB embedding registry | Blocks the write; fine for bulk-batched, bad for single-row mutations. **HelixDB — the closest analog — took this path and inherited the latency.** Forbidden here by the deny-list. |
| **Async materialization / reconciler** | **Timescale pgai Vectorizer**, Supabase (trigger → `pgmq` → worker) [U] | The write enqueues; a batched, retrying worker embeds and writes back; "a delay may occur before [vectors] become available." **= invariant 7. Recommended.** |

"Lazy on read" is not a fourth option: the ANN index needs the vector *present*
to index it, so you cannot defer the vector itself to query time without giving
up the index. The reconciler is "lazy relative to the write, eager relative to
search" — which is precisely invariant 7's partial-coverage licence.

## Design

### Embeddings are derived state, keyed off `@embed`

`@embed("source", model)` declares a derivation: `Vector_col = embed(source_col)`
[D]. That makes the vector column *physical, rebuildable, source-derived* state —
identical in kind to an index. The whole design follows from treating it that way.

### Write path: store source, leave the vector pending (NULL)

`load`/`mutate` are **unchanged in cost**: they write the source text and any
caller-supplied vector. They do **not** call the embedder. A row whose `@embed`
source is present but whose vector is NULL is *pending*. The write path's only
new behavior is **staleness invalidation** (see §"Staleness"): when a write
touches an `@embed` *source* column, it clears (NULLs) the dependent vector so
the reconciler re-embeds. Clearing a column is write-local and cheap (no
network).

This reuses the exact shape already in `table_ops.rs`: an untrainable Vector
column (all-NULL today) is isolated as a `PendingIndex` rather than aborting the
build [S]; here the *rows* are pending, tracked the same way.

### The reconciler: `ensure_embeddings`

A sibling of `ensure_indices`
(`db/omnigraph/table_ops.rs`, materialized in `optimize.rs` [S]):

1. For each `@embed`-annotated `(table, vector_col, source_col, model)`, scan for
   **pending rows**: `vector_col IS NULL AND source_col IS NOT NULL`. Cost is
   bounded by *pending rows*, not history (invariant 15).
2. Embed the source texts **in batches** via a new
   `EmbeddingClient::embed_documents(&[&str]) -> Vec<Vec<f32>>` (see §"Required
   new mechanics"), honoring the existing retry/backoff
   (`OMNIGRAPH_EMBED_RETRY_*`, `embedding.rs:18` [S]) and the recorded model.
3. Write the vectors back via the normal staged-write + manifest publish path
   (an `update` keyed on row id). Then fold the now-trainable vector column into
   its index via the existing `ensure_indices` pass — embeddings unblock the
   deferred vector index automatically.
4. A model mismatch (recorded model ≠ resolved client model) **refuses loudly**,
   reusing the query-path same-space validation that already exists [D].

**Trigger (decision §A):** start **explicit** — `ensure_embeddings` runs inside
`optimize` and as a new `omnigraph embed --graph <scope>` mode (the file
pipeline gains a graph target). This matches how indexes already reconcile
(operator/cron-triggered, no background loop). A **background loop** (pgai's
every-5-min worker [U]) is the eventual convergence, specified as a follow-up,
not built first.

### Staleness (decision §C)

A source-text `update` must re-embed. Two options:

- **Recommended — NULL-on-source-change.** When a write touches an `@embed`
  source column, clear the dependent vector. Then *one* universal pending
  predicate (`vector IS NULL AND source IS NOT NULL`) covers both first-insert
  and post-update, with no per-row bookkeeping. Cost: the write path must know
  the `@embed` dependency (a catalog lookup it already has) and clear a column.
- *Alternative — embedded-from marker.* Store the source version/hash the vector
  was derived from; the reconciler re-embeds rows whose
  `_row_last_updated_at_version` (`lance_version_columns` [S]) exceeds it. No
  write-path coupling, but adds a per-row marker column.

### Read semantics (decision §B)

A pending row is **present and queryable** by traversal/filter/scalar predicates
immediately; it is simply **not yet a `nearest()` hit.** This is invariant 7
partial coverage, surfaced — not silent:

- Expose a **pending-embedding count** (per table/column) via `status`/`optimize`
  output, so operators see the backlog.
- Provide an **explicit synchronous opt-in** for the rare read-after-write-on-
  vectors case — e.g. `omnigraph embed --graph --wait` or a `mutate` flag that
  runs `ensure_embeddings` for the touched rows before returning (opt-in only;
  never the default, never on the hot path).

## Required new mechanics

| Mechanic | Today | Needed |
|---|---|---|
| **Batch embed** | `embed_document_text(&str)` / `embed_query_text(&str)` are one-string-per-call (`embedding.rs:248,252` [S]) | Add `embed_documents(&[&str]) -> Result<Vec<Vec<f32>>>` (provider batch endpoint where available; chunked sequential fallback). pgai treats batch as load-bearing for throughput/rate-limits [U]. |
| **Pending-row scan** | `PendingIndex` tracks declared-but-unbuilt *indexes* (`table_ops.rs` [S]) | A row-level pending scan (`vector IS NULL AND source IS NOT NULL`) per `@embed` column. |
| **`@embed` dependency in the write path** | `@embed` is a catalog annotation consumed at query typecheck/lint [D] | The loader/mutation must read it to NULL-on-source-change (decision §C). |
| **Graph-target embed** | `omnigraph embed` is file→file [D] | `omnigraph embed --graph <scope>` runs the reconciler against a live graph. |

The provider client, model-identity recording, same-space validation, and
retry/backoff already exist (RFC-012) — this is write-path + reconciler
integration, not a new subsystem.

## Invariants & deny-list check

- **7 (indexes are derived state):** this *is* an instance — embeddings converge
  from manifest/source state, never extend the critical write path; reads are
  correct (just not vector-ranked) under partial coverage.
- **Deny-list "synchronous inline vector/FTS index rebuilds on the write path":**
  obeyed by construction — embedding is off the write path.
- **9 (integrity failures are loud):** a model mismatch or an embed failure on
  the reconciler is surfaced (pending count, typed error), not a placeholder
  vector.
- **13 (operational failures bounded/observable):** the reconciler's embed calls
  are batched, retry-bounded, and report a pending backlog; a flaky provider
  degrades vector-search freshness, never write availability.
- **2/3 (manifest-atomic, one snapshot):** unchanged — the reconciler's write-back
  is an ordinary staged write + manifest publish.

## Cost contract

- **Write path:** unchanged — O(1), no embedder call (the NULL-on-source-change
  clear is a local column write). Assert in `write_cost.rs` that a `mutate`
  touching an `@embed` source performs **zero** embedding round-trips.
- **Reconciler:** O(pending rows), batched; flat in history depth (it scans by
  the pending predicate, not the version chain).

## Testing

- **Write-path-clean:** a `mutate`/`load` into an `@embed` table issues no
  embedder call and leaves the vector NULL (mock client asserts zero invocations).
- **Reconcile fills:** `ensure_embeddings` embeds the pending rows, the vector
  column becomes non-NULL, and the deferred vector index then builds.
- **Staleness:** updating the source NULLs the vector; the next reconcile
  re-embeds; updating a *non-source* property does **not**.
- **Read partial coverage:** a pending row is returned by a traversal/filter
  query but not by `nearest()`; the pending count is reported.
- **Model mismatch refuses** on the reconciler (reuse the query-path validation).
- **Batch:** `embed_documents` over N texts maps 1:1 to N output vectors in order;
  a mid-batch provider error is retry-bounded and reported, not silently dropped.
- Mock provider throughout (no network in CI), mirroring the existing embedding
  tests.

## Rollout

1. **PR 1 — `embed_documents` batch method** + reconciler-internal use. Pure
   addition to the client.
2. **PR 2 — `ensure_embeddings` reconciler** + `optimize` integration + the
   pending-row scan. Read-only effect until something is pending.
3. **PR 3 — `omnigraph embed --graph`** (graph target for the existing CLI) +
   the pending-count surface in `status`/`optimize`.
4. **PR 4 — write-path staleness** (NULL-on-source-change) + the
   `--wait`/synchronous opt-in. This is the only write-path touch; lands last,
   behind the reconciler that consumes its output.
5. *(follow-up)* background reconciler loop (pgai-style), if/when operators want
   automatic freshness without a cron.

PRs 1–3 deliver "add data, then reconcile, then it's searchable" with **no
write-path change at all**; PR 4 adds automatic staleness + the opt-in.

## Drawbacks & alternatives

- **Read-after-write delay on vectors.** The cost of keeping writes fast: a
  freshly-mutated node is not a `nearest()` hit until reconciled. Mitigated by
  the pending-count surface and the synchronous opt-in. This is the same
  trade-off pgai ships and documents [U].
- **Explicit-trigger first** means freshness depends on `optimize`/cron cadence,
  not seconds. Accepted as the lower-liability start (matches index reconcile);
  the background loop is the convergence.
- **Rejected — synchronous inline (HelixDB regime).** The latency and
  failure-coupling this RFC exists to avoid; on the deny-list.
- **Rejected — embed-on-read (lazy).** Doesn't fit a vector column the ANN index
  must contain.

## Reversibility

High. The vector column, `@embed` annotation, and on-disk format are unchanged;
the reconciler is additive. The one observable contract change (a pending window
before vectors are searchable) is the documented partial-coverage semantics, and
the synchronous opt-in covers callers that cannot tolerate it.

## Open questions / decisions to ratify

- **§A Trigger model:** explicit (`optimize` + `omnigraph embed --graph`) first,
  background loop later — confirm the staged approach.
- **§B Read-after-write:** ratify the "present but not vector-searchable until
  reconciled" contract + the synchronous opt-in shape.
- **§C Staleness signal:** NULL-on-source-change (recommended) vs embedded-from
  marker.
- **Batching granularity / provider limits:** max batch size + handling a
  provider's per-request token cap (chunk within `embed_documents`).
- **Chunking:** pgai also *chunks* long source text before embedding [U]. Out of
  scope here (one source property → one vector), but note it as a future
  extension if long-text columns appear.

## Relationship to prior work

- **RFC-012 (embedding provider config):** provides the client, model identity,
  same-space validation, retry/backoff this RFC consumes. RFC-015 is the
  write-path/reconciler integration RFC-012 deferred.
- **Invariant 7 / the deferred-vector-index path** (`table_ops.rs`
  `PendingIndex`, `optimize.rs` [S]): the mechanism this extends from
  index-coverage to row-level embedding coverage.
- **RFC-013 (write-path latency) / step 3b:** the cost-and-failure-bounding
  contract that makes synchronous inline embedding unacceptable and the
  reconciler necessary.
