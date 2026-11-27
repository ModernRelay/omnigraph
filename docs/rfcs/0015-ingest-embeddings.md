---
rfc: "0015"
title: "Ingest-time `@embed` reconciliation"
track: maintainer
status: draft
implementation: not-started
authors:
  - OmniGraph maintainers
created: 2026-06-21
updated: 2026-11-21
discussion: null
supersedes: []
superseded_by: []
blocked_on:
  - "GQ 3 (PR #1302, implementation PR #1316): the encoding recipe descriptor and its hash, which the row fingerprint uses"
---

# RFC 0015: Ingest-time `@embed` reconciliation

Contract: keeps E1, E2, E4, E7, read:11, write:3; changes Boundary
("Outside the commit"); bears on D17.

## Summary

`@embed("source", model = "…")` declares a derivation: the vector is the
embedding of the record's source text in one embedding space. Nothing in the
engine computes it. Vectors arrive in load data, usually from the offline
`omnigraph embed` file pipeline, and nothing notices when the source text
changes afterwards ([embeddings](../user/search/embeddings.md)).

This RFC makes the engine converge every `@embed` vector to its declaration
without putting a provider call on the write path. Each row records a
fingerprint of what its vector was computed from: the field's encoding recipe
hash (GQ 3) plus a digest of the source value. A row is **current** when its
fingerprint equals the one its present source and recipe would produce, and
**stale** otherwise, which covers a first insert, a source edit, a supplied
vector of unknown origin and a recipe change with one predicate. An explicit
`embed --graph` operation embeds stale rows in batches and publishes them as
ordinary commits.

## Motivation

1. **Silent staleness.** Updating a source property leaves its vector in
   place, and a `nearest` then ranks the row by text it no longer contains.
   Fill-missing in `omnigraph embed` cannot see it, because a stale vector is
   not missing.
2. **Generation lives outside the graph.** Backfill is export, embed a file,
   reload: it bypasses branch policy, the actor on the commit and the head
   the caller read.
3. **Inline embedding is the wrong fix.** Embedding during `mutate` or `load`
   is a synchronous, network-bound, fallible step on the write path, which
   the deny-list forbids in the same terms as inline index rebuilds, and it
   makes write availability depend on the provider's.

Prior art splits three ways: bring your own vector (pgvector, Qdrant, today's
`omnigraph embed`), synchronous inline (HelixDB, Weaviate vectorizers, LanceDB
embedding registry) and asynchronous materialization (Timescale pgai
Vectorizer, Supabase with pgmq). Omnigraph keeps the first and adopts the
third without its queue: the work is derivable from accepted state, so a
reconciler suffices (deny-list: no job queue for manifest-derived work).

## User and operational behavior

- `load` and `mutate` are unchanged in cost and never call a provider. They
  store the source and any supplied vector, and they write the row's
  fingerprint (below). A supplied vector is the writer's claim that it is in
  the field's space; the fingerprint records which source and recipe it claims.
- A new operation, `omnigraph embed --graph <uri> [--branch b] [--type T]
  [--vector T.p] [--limit n]`, embeds stale rows and publishes one commit per
  batch on the branch, with its actor, under the `change` policy action. A
  served cluster exposes it as a recorded operation, like remote `optimize`.
- `index status` (and its served equivalent) reports, per `@embed` field, the
  count of current, stale and missing vectors.
- A stale row keeps its old vector and stays a `nearest` candidate until it
  is re-embedded. A row with no vector is not a `nearest` candidate, as today.
- The offline file pipeline stays for seeding. It writes fingerprints into its
  output so a loaded file is current.

## Design

### The fingerprint

GQ 3 defines three typed descriptors whose hashes are derived views: the
encoding recipe (the space: model revision plus record and query encoding
choices), the field representation, and the query encoding. Hashes are per
field only.

The row fingerprint is `recipe_hash || sha256(source value bytes)`. It uses
the recipe hash, not the field-representation hash, because a distance or
analyzer change rebuilds an index but leaves vectors current. A null or empty
source has no fingerprint, and its vector must be null.

Storage: one system column per `@embed` target, `__embed_fp_<property id>`,
`FixedSizeBinary(64)`, nullable, keyed by stable property identity so a rename
keeps it (invariant 6). It is written by every door that writes the target or
its source:

| Writer | Fingerprint written |
|---|---|
| `load` or `mutate` setting the source and the vector | the supplied pair's fingerprint |
| `load` or `mutate` setting the source only | null (stale) |
| `mutate` setting the vector only | the fingerprint for the row's stored source |
| `embed --graph` | the fingerprint it embedded |

The engine never clears a vector on a source edit. Clearing would remove the
row from `nearest` until the next run; keeping the old vector degrades more
gently and the stale count makes the lag visible.

### The operation

1. Capture one snapshot of the branch (invariant 3). For each selected field,
   resolve the recipe from the accepted schema and refuse when the bound
   provider does not resolve it (cluster apply already refuses a model or
   dimension mismatch).
2. Scan for rows whose fingerprint is null or differs from
   `recipe_hash || sha256(source)`, bounded by `--limit` and a per-run memory
   ceiling. The cost is the stale rows plus one pass over the fingerprint and
   source columns, never history.
3. Embed in batches with the provider's batch endpoint, outside every writer
   gate, under the existing deadline, retry and backoff settings.
4. Publish each batch as one commit. Before writing a row, recheck its source
   digest against the head at publish. A row whose source changed since the
   snapshot is skipped, stays stale and is picked up by the next run. The
   batch never overwrites a concurrent edit (`write:3`), and it never holds a
   gate while waiting on the provider.
5. Report per field: embedded, skipped as concurrently changed, failed, and
   still stale.

Only `embed --graph` calls a provider. A provider failure ends the run with
the committed batches kept and an exact count of what remains stale; reads
and writes are unaffected.

### Kind of operation

The operation changes property values, so it is a `write` under the
contract, not `maintain`: its commits carry an actor, appear in the change
feed, merge like other writes, and are checked against policy (E6). It is not
physical state. The vector it writes changes `nearest` answers, which `E7`
forbids for `maintain`.

### Model or recipe change

Changing a field's recipe makes every row stale at once. Rows still carrying
the old recipe's vectors are in another space, so a text `nearest` on that
field skips rows whose fingerprint names another recipe, and reports
`exhaustive: false` while any are skipped. A vector `nearest` is the caller's
claim about the space and is unaffected.

## Invariants

- **2 and 4:** each batch is one manifest publication.
- **3:** one snapshot per run, revalidated per batch at publish.
- **7:** vectors are not physical state, so this is not index reconciliation.
  Freshness is timeliness, as the contract classifies values derived by a
  pipeline.
- **11:** batches, retries, the deadline, memory and the scan are bounded.
  Provider failure is an explicit, counted outcome.
- **Deny-list:** no provider call on a content write, no queue (the stale
  predicate over accepted state is the work list), no background scheduler
  (`maintain:1`; a managed scheduler, if any, calls the explicit operation).

## Compatibility and reversibility

The fingerprint column is a storage-format addition behind a format bump:
older binaries refuse the graph, as for any newer format. Existing rows start
with a null fingerprint, so a graph upgraded from an older format reports
every vector stale until it is re-embedded or a fingerprint backfill
(`embed --graph --adopt`) records the current recipe for vectors the operator
asserts are current. Removing the feature later drops one column per field.

## Alternatives

- **NULL on source change** (this RFC's earlier draft). One predicate,
  `vector IS NULL AND source IS NOT NULL`, with no new column, but every
  source edit removes the row from `nearest` until the next run, a recipe
  change cannot be detected per row, and a supplied vector's origin is never
  checked.
- **Lance `_row_last_updated_at_version` against a stored embed version.**
  Needs no hash, but any update to the row makes it stale, and it reads
  linear Lance history, which `__manifest` owns (invariant 12).
- **Inline synchronous embedding.** On the deny-list.
- **Embed on read.** A vector index needs the vector present.

## Evidence and tests

- A `mutate` or `load` into an `@embed` type makes zero provider calls (mock
  provider counter), and the DST storage-cost golden records the added
  fingerprint write.
- Source edit: the fingerprint goes null, the old vector stays, the run
  re-embeds it; an edit to another property leaves it current.
- A concurrent source edit during a run is skipped and stays stale.
- A recipe change makes all rows stale; text `nearest` skips them and reports
  not exhaustive.
- A supplied matching pair loads current; a supplied vector without its
  source digest loads stale.
- Batching: N texts map to N vectors in order; a mid-batch provider error is
  retried within bounds, then reported, with earlier batches committed.
- Policy: the run is refused without `change` on the branch.

## Rollout

1. Batch embedding in the client (`embed_documents`), used by the file
   pipeline (after PR #952).
2. GQ 3's recipe descriptor and hash (PR #1316).
3. The fingerprint column, the format bump and the write-path rules.
4. `embed --graph`, the stale counts in `index status`, the served operation.
5. Text `nearest` skipping other-recipe rows.

## Unresolved questions

- Should a served cluster schedule `embed --graph` (control-plane DEC-08-76
  scheduling), or stay operator-triggered like `optimize`?
- A per-tenant budget for provider tokens beside CPU, memory and storage.
- Chunking long sources into several vectors is out of scope: one source
  property, one vector.

## Decision log

- 2026-06-21: drafted with NULL-on-source-change as the recommended staleness
  signal and the reconciler inside `optimize`.
- 2026-11-21: revised. A per-row fingerprint of the GQ 3 recipe hash plus a
  source digest replaces NULL-on-source-change, as agreed with the GQ 3 RFC
  (per-field hashes stay per field; the row record belongs here). The
  operation is a `write`, not part of `optimize`, because it changes
  answers. Blocked on GQ 3's recipe descriptor.
