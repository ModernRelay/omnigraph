# RFC: Provider-Independent Embedding Configuration

**Status:** Proposed
**Date:** 2026-06-15
**Builds on:** the engine embedding client (`crates/omnigraph/src/embedding.rs`), the `@embed` catalog
annotation (`omnigraph-compiler/src/catalog`), the reserved cluster `embeddings`/`providers` fields
([cluster-config-specs.md](cluster-config-specs.md), [rfc-007-operator-config.md](rfc-007-operator-config.md)
for the secret-resolution pattern).
**Target release:** staged — NFR floor first, then the provider-independent config core; ingest-time `@embed`
execution is a separate later phase.

## Summary

OmniGraph's embedding subsystem is **hardwired to a single provider (Google Gemini)** and has no recorded
link between the model that produced a stored vector and the model that embeds a query string. Today that
happens to be self-consistent (one live client embeds both sides), but it is consistent by accident, not by
construction: the provider is hardcoded, the model is a moving `-preview` target, nothing validates that a
query vector and a stored vector share a space, and the one configurable knob (key + base URL) cannot change
the provider or model.

This RFC makes embedding **provider-independent**: one resolved `EmbeddingConfig { provider, model, base_url,
api_key, dim, normalize }` behind a sealed provider abstraction, resolved once and shared by every embedder.
The **primary variant is OpenAI-compatible** — a single request/response shape (`POST {base}/embeddings`,
`{model, input, dimensions}`) that covers **OpenRouter** (the recommended default gateway, one key for Gemini,
OpenAI, Mistral, BGE, Qwen, sentence-transformers, …), OpenAI direct, and any self-hosted OpenAI-compatible
endpoint (vLLM, Ollama, LM Studio, Together). A native **Gemini** (`generativelanguage`) variant is retained
for shops that want to hit Google directly with its `RETRIEVAL_QUERY`/`RETRIEVAL_DOCUMENT` task-type
asymmetry, plus a deterministic **Mock**. The embedding *identity* (provider + model + dim) is recorded in the
schema IR so it travels with the data, and a query whose resolved embedder cannot match the stored vectors'
recorded identity is **rejected with a typed error instead of silently ranking across vector spaces.**
Provider/endpoint wiring lands on the already-reserved cluster `providers.embedding` field; secrets follow the
existing operator-credential pattern; no secret ever enters the schema.

This RFC supersedes the framing in `docs/user/search/embeddings.md` that described "two embedding clients
with different defaults" — one of those clients was dead code with zero callers and has been removed (see
Phase 1); the OpenAI request shape returns as a first-class *provider variant* of the one client, not as a
second parallel client.

## Motivation

This work originated in an external handoff that reported a live cross-provider bug: gemini-3072 stored
vectors compared against OpenAI-1536 query vectors, silently. Investigation against the current source showed
the reported mechanism is **inaccurate** — the OpenAI client it blamed (`omnigraph-compiler/src/embedding.rs`)
was `pub(crate)`, `#![allow(dead_code)]`, and had **zero callers**; the live `nearest("string")` path and the
offline `omnigraph embed` CLI both use the engine **Gemini** client; and `@embed` does no ingest-time
embedding at all. So the documented happy path is self-consistent. But the investigation surfaced four real
problems the handoff's instincts correctly smelled:

- **P1 — Provider is hardwired.** The one live client builds Google `generativelanguage` requests; only key +
  base URL are configurable, not the provider or model. A non-Gemini shop cannot use `nearest("string")`
  without a Gemini key, and cannot make it produce non-Gemini vectors. If they store their own vectors and
  query with `nearest("string")`, the query is embedded with Gemini → a silent cross-space ranking. This is
  the handoff's failure, reached by a different cause.
- **P2 — A dead, divergent second client + stale docs** invited exactly the misdiagnosis the handoff made.
- **P3 — No same-space guarantee recorded with the data.** Nothing stamps which model/dim produced a stored
  vector, so write-side and read-side embedders can drift with no validation.
- **P4 — `@embed` is declarative-in-name-only.** It records a source property for the typechecker but never
  embeds at ingest; the docs claimed otherwise.

Per the project's first principle, the lower-liability shape is **one provider-independent client with the
identity recorded next to the data**, not N independently-defaulted clients kept in lockstep by discipline.
Hardcoding one provider mortgages every future "we need OpenAI / a local model / Vertex" against a rewrite;
recording identity once closes the silent-wrong-results class by construction.

## Current state — which API we actually use

| | Live engine client (`crates/omnigraph/src/embedding.rs`) | Deleted dead client (was `omnigraph-compiler/src/embedding.rs`) |
|---|---|---|
| Provider | **Google Gemini Developer API** (`generativelanguage`, *not* Vertex AI) | OpenAI |
| Endpoint | `POST {base}/models/{model}:embedContent` | `POST {base}/embeddings` |
| Auth | header `x-goog-api-key`, env `GEMINI_API_KEY` | `Authorization: Bearer`, env `OPENAI_API_KEY` |
| Model | `gemini-embedding-2-preview` (hardcoded) | `text-embedding-3-small` (env `NANOGRAPH_EMBED_MODEL`) |
| Base default | `https://generativelanguage.googleapis.com/v1beta` | `https://api.openai.com/v1` |
| Request body | `{model, content:{parts:[{text}]}, taskType, outputDimensionality}` | `{model, input:[…], dimensions}` |
| Response | `{embedding:{values:[f32]}}` | `{data:[{index, embedding:[f32]}]}` |
| Task types | `RETRIEVAL_QUERY` / `RETRIEVAL_DOCUMENT` | none |
| Status | **live** — used by `nearest("string")` and `omnigraph embed` | **removed in Phase 1** (zero callers) |

Both shapes honour a requested output dimensionality (Gemini `outputDimensionality`, OpenAI `dimensions`)
driven by the target column width, so dimension is already schema-driven. The two known shapes are exactly the
two initial provider variants this RFC defines — the OpenAI shape returns from git history as a `Provider`
variant of the single client.

## Guide-level explanation

### Configuring a provider (operator view)

Pick a provider for the graph in `cluster.yaml` (the team-owned surface), referencing a secret by name. The
recommended default routes through OpenRouter (OpenAI-compatible, one key for many models):

```yaml
providers:
  embedding:
    default:
      kind: openai-compatible           # openai-compatible | gemini | mock
      base_url: https://openrouter.ai/api/v1
      model: google/gemini-embedding-2  # or openai/text-embedding-3-large, mistralai/mistral-embed, …
      dimension: 3072
      api_key: ${OPENROUTER_API_KEY}
```

The same `openai-compatible` kind points at OpenAI direct (`base_url: https://api.openai.com/v1`,
`model: text-embedding-3-large`) or a self-hosted endpoint (vLLM/Ollama/LM Studio) by changing `base_url`. Use
`kind: gemini` only to reach Google's `generativelanguage` API directly (it keeps the query/document
task-type asymmetry that the OpenAI-compatible shape does not expose).

The zero-config tier keeps working with env only (`OMNIGRAPH_EMBED_PROVIDER`, `OMNIGRAPH_EMBED_BASE_URL`,
`OMNIGRAPH_EMBED_MODEL`, and the provider api-key env — `OPENROUTER_API_KEY` / `OPENAI_API_KEY` /
`GEMINI_API_KEY`), so no cluster file is required for a single-graph setup.

### Recording identity in the schema

`@embed` grows optional arguments that pin the embedding identity to the vector column:

```pg
node Doc {
  slug: String @key
  text: String
  v: Vector(3072) @embed("text", model="gemini-embedding-2", dim=3072) @index
}
```

The single-argument form `@embed("text")` keeps working unchanged. The recorded identity persists in the
schema IR (`_schema.ir.json`) and so travels with `schema apply` and `schema show`.

### What a mismatch looks like

If the resolved read-side embedder cannot produce the recorded identity (wrong model, wrong dim, wrong
provider), `nearest($v, "string")` fails with a typed error naming both sides, instead of returning a
plausible-but-meaningless ranking. Changing the recorded identity on an existing column is a loud schema-apply
refusal (it is a re-embed, a deliberate migration step), reusing the migration planner's existing
annotation-change rejection.

## Reference-level design

### One client, sealed provider abstraction

Replace the two-variant `EmbeddingTransport` with a resolved config plus a sealed provider enum:

```text
EmbeddingConfig { provider: Provider, model, base_url, api_key, dim, normalize }
enum Provider {
  OpenAiCompatible,   // POST {base}/embeddings, Bearer auth, {model, input, dimensions} → {data:[{embedding,index}]}
                      //   covers OpenRouter (default gateway), OpenAI direct, vLLM/Ollama/LM Studio/Together
  Gemini,             // POST {base}/models/{model}:embedContent, x-goog-api-key, with RETRIEVAL_QUERY/DOCUMENT task types
  Mock,               // deterministic, offline
}
struct EmbeddingClient { config, http, retry, deadline }
```

`Provider` owns the per-API differences (endpoint suffix, auth header, request JSON, response JSON, task-type
support); the client owns retry/backoff, the deadline, normalization, and tracing — all provider-independent.
**OpenRouter is not a distinct variant** — it is `OpenAiCompatible` with `base_url =
https://openrouter.ai/api/v1`, which is the point: one OpenAI-compatible shape gives provider-independence
across every model OpenRouter fronts, so the gateway does the multi-provider fan-out and OmniGraph carries one
request shape. The native `Gemini` variant exists only for direct-to-Google with task-type asymmetry. An enum
(not a trait) is the earned complexity for this small, first-party set; if third-party plug-in providers are
ever needed, the enum becomes a trait behind the same `EmbeddingConfig` surface without touching callers.

The OpenAI-compatible `input` accepts an **array**, giving batch embedding for free — which the later
ingest phase needs for throughput, and which removes the open dependency on Gemini's native
`batchEmbedContents`.

### Config resolution (resolved once, shared)

Precedence, highest first: cluster `providers.embedding.<name>` profile → env (`OMNIGRAPH_EMBED_*`, provider
api-key env) → built-in defaults. The api-key is resolved through the existing operator credential chain
(`${NAME}` → env / `~/.omnigraph/credentials` / server `TokenSource`); it never lives in the schema or any
checked-in file. Resolution happens once; the resolved client is shared by `nearest("string")` and the
offline CLI (replacing the per-query `EmbeddingClient::from_env()` rebuild at `exec/query.rs:238`).

### Identity recorded in the schema IR (not a new store)

The `@embed` args serialize into `PropertyIR.annotations` → `_schema.ir.json`, which `schema apply` already
persists atomically and which the catalog (the one thing `nearest()` reads at query time) is built from. No
new metadata store, no manifest column, no extra read on the query path. The migration planner already rejects
non-description annotation changes as `UnsupportedChange`, so "recorded identity is immutable without a
deliberate re-embed migration" is the default behaviour, not new code. (A second, optional copy in Lance
field metadata — co-located with the vectors — is available later by activating the currently no-op
`UpdatePropertyMetadata` migration step; out of scope here.)

### Query-time validation

`resolve_nearest_query_vec` compares the resolved read-side identity against the column's recorded identity
before embedding; on mismatch it returns a typed `OmniError` naming recorded vs resolved (model, dim,
provider). This is the only behaviour that closes P3 by construction.

### NFR floor (independent of the provider work)

- **Deadline:** wrap the query-time embed in a total-operation deadline (`OMNIGRAPH_EMBED_QUERY_DEADLINE_MS`)
  so a degraded provider cannot hang a read for the current ~121 s worst case (4 × 30 s timeout + backoff).
- **Observability:** `tracing` span per embed call (provider, model, dim, attempts, outcome, elapsed; `warn!`
  per retry; token usage when the provider returns it). The subsystem has zero instrumentation today.
- **Single normalization:** one `normalize_vector` (the dead client carried a divergent second copy; removed
  in Phase 1).
- **Stable model:** make the model configurable and default to a stable (non-`-preview`) model once the GA
  name is confirmed.

### Ingest-time `@embed` (later phase, not this RFC's core)

Making `@embed` embed at ingest is a separate phase with a hard constraint: embedding is a slow, external,
**non-idempotent** side effect, so it must run **entirely before staging** — in the pure in-memory phase,
before any `stage_*`/Lance HEAD move, alongside the existing constraint validation — so a mid-load provider
failure aborts with zero drift. It must never sit inside or after the commit protocol, because the recovery
sweep cannot re-run or undo an external embedding. It also needs a content-hash skip (so `load --mode
overwrite` does not re-bill every row), batching, and a bounded-concurrency stage. Specified here only to fix
the design constraint; deferred to its own RFC/phase.

### Phasing (implementation order)

| Phase | Scope | Demo |
|---|---|---|
| **1 — NFR floor + dead-client removal** | deadline, observability, single normalize, configurable model, delete dead client + `NANOGRAPH_*` | a hung provider fails at the deadline; embed calls traced; `rg NANOGRAPH_` empty |
| **2 — Provider-independent config** | `EmbeddingConfig` + `Provider` enum (OpenAiCompatible covering OpenRouter/OpenAI/local, Gemini, Mock); env-first resolution; client reuse | point `base_url` at OpenRouter, run `nearest("string")`, get correct neighbours vs OpenRouter-stored vectors; CLI shares the config |
| **3 — Record identity in schema IR** | `@embed` args grammar + catalog + IR persistence | `schema show` reflects recorded model/dim |
| **4 — Query-time validation** | compare resolved vs recorded; typed error; planner refusal on identity change | stored model A vs read model B → loud error, never silent garbage |
| **5 — Cluster provider wiring** | un-reserve `providers.embedding`; `${NAME}` resolution | provider profile resolved from `cluster.yaml`; legacy `omnigraph.yaml` untouched |
| later | ingest-time `@embed` (Shape C) | separate RFC |

## Invariants & deny-list check

- **Invariant 9 (integrity failures are loud):** strengthened — query-time identity mismatch becomes a typed
  error instead of silent wrong results.
- **Invariant 10 (query semantics are first-class IR concepts):** embedding identity becomes IR/catalog data,
  not an out-of-band env guess.
- **Invariant 11 (transport stays at the boundary):** strengthened — Phase 1 removes the HTTP client + async
  runtime (`reqwest`, `tokio`) from `omnigraph-compiler`, whose own manifest advertises "Zero Lance
  dependency"; the embedding HTTP client lives only in the engine.
- **Invariant 12 / secret handling:** api-keys resolve through the existing credential chain; never in schema
  or checked-in config.
- **Invariant 13 (bounded & observable):** addressed — the deadline bounds latency; tracing makes the
  subsystem observable.
- **Deny-list — "silent fallback / dropped rows":** the cross-space ranking is exactly a silent-wrong-result;
  this RFC closes it.
- **Deny-list — "new write paths that advance Lance HEAD before manifest publish without a recovery
  sidecar":** the ingest phase (deferred) explicitly keeps embedding *before* staging, so it does not create a
  new HEAD-advancing write path. No invariant is weakened.

## Drawbacks & alternatives

- **Do nothing.** The happy path works today, so the live risk is narrow (P1 + P3). But the provider hardwiring
  and missing validation are a latent silent-wrong-results class that bites the first non-Gemini user.
- **Interim env-only provider switch (no schema record).** Cheaper, but leaves the same-space guarantee to
  operator discipline (fails P3). Folded in as Phase 2's env-first resolution, with Phases 3–4 adding the
  record/validate guarantee.
- **Trait-based provider plug-ins now.** Rejected as unearned complexity for two first-party providers; the
  enum upgrades to a trait behind the same surface if needed.
- **Stamp identity in the manifest or Lance field metadata instead of the IR.** The manifest is the wrong
  granularity; field metadata needs net-new wiring and a query-path dataset open. The IR is where `@embed`
  already lives and is already read at query time (see spike).

## Reversibility

Mostly reversible. Phases 1–2 and 5 are code/config (env, CLI, cluster keys) and cheap to undo. Phase 3
(recording identity in the schema IR) is **near-permanent** — it changes the on-disk `_schema.ir.json` shape
and the schema hash — so it earns the most scrutiny: the single-arg `@embed` form stays byte-compatible, and
recorded identity is additive (absent identity = today's behaviour). Provider request/response shapes are
external API contracts, not our format, so adding providers is reversible.

## Gateway tradeoff (OpenRouter)

Routing through OpenRouter (the default) buys provider-independence with one key and one billing relationship,
batch input, and access to the GA `google/gemini-embedding-2`. Costs to accept, all controllable:

- **Extra network hop** → more query-path latency. The Phase-1 deadline bounds it; the cache mitigates repeats.
- **Text transits a third party.** OpenRouter's `provider: { data_collection }` routing preference controls
  retention; shops with strict residency requirements use `kind: gemini`/`openai-compatible` pointed at the
  provider (or a self-hosted endpoint) directly instead of the gateway. Provider-independence means this is a
  config change, not a code change.
- **Loses Gemini's task-type asymmetry** when Gemini is reached via the OpenAI-compatible gateway (both sides
  embed symmetrically). This is a retrieval-quality cost, **not** a same-space correctness cost — both stored
  and query vectors take the identical path, so they stay in one space by construction. Shops that want the
  asymmetry use `kind: gemini`.

## Unresolved questions

- GA Gemini model name — **resolved:** `google/gemini-embedding-2` (via OpenRouter) / `gemini-embedding-2`
  (direct), 128–3072 dims (recommended 768/1536/3072). Default flips off `-preview` in Phase 2.
- Gemini `batchEmbedContents` availability — **moot** when going through the OpenAI-compatible gateway (its
  `input` array batches); still relevant only for the direct `kind: gemini` path.
- Identity granularity: per-vector-property args vs one graph-level default profile referenced by name.
- Whether to backfill recorded identity for existing graphs, or treat absent-identity as "unvalidated, legacy"
  permanently.
- Default model for the zero-config tier: `google/gemini-embedding-2` vs `openai/text-embedding-3-large`
  (both 3072-capable) — pick the project default.
