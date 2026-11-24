# Provider-independent embedding configuration

**Decides:** one resolved `EmbeddingConfig` over a closed `Provider` enum embeds both stored and query text, and a model recorded on a vector property with `@embed("…", model="…")` is checked against the query embedder before `nearest()` ranks.
**Contract:** keeps E3, E7, read:4
**Code:** `crates/omnigraph/src/embedding.rs`, `crates/omnigraph/src/engine/search.rs`, `crates/omnigraph-compiler/src/schema/parser.rs`, `crates/omnigraph-compiler/src/catalog/schema_plan.rs`, `crates/omnigraph-cluster/src/types.rs`
**Cited as:** RFC 0012

## Context

`nearest($v, "text")` embeds the text and compares it with stored vectors. The
comparison means something only when both vectors come from one model in one
space. With one hardwired provider, a graph whose vectors come from another
model is ranked against a query vector from a different space, and the result
looks plausible. Unless the model that produced a stored vector is recorded,
nothing can refuse the mismatch.

## Decision

- `EmbeddingConfig { provider, model, base_url, api_key }` is resolved once and
  shared by `nearest()` and the offline `embed` command, so
  stored and query vectors take the same path.
- `Provider` is a closed enum: `OpenAiCompatible` (`POST {base}/embeddings`,
  bearer; covers OpenRouter, OpenAI and self-hosted endpoints), `Gemini` (direct
  `generativelanguage`, with query and document task types) and `Mock`. The
  client owns retry, the total deadline (`OMNIGRAPH_EMBED_DEADLINE_MS`),
  normalization and tracing; the variant owns only the request and response
  shape. OpenRouter is a base URL, not a variant.
- Without a cluster profile the config comes from the environment
  (`OMNIGRAPH_EMBED_PROVIDER`, `_BASE_URL`, `_MODEL` and the provider's key
  variable); unset, it is `openai-compatible`. `OMNIGRAPH_EMBEDDINGS_MOCK` wins
  and warns when it overrides an explicit provider.
- The vector width comes from the target `Vector(N)` column, not the profile.
- `@embed` accepts one keyword, `model`; any other keyword is a parse error.
  The model is stored in the schema IR with the property.
- When a property has a recorded model and the resolved embedder's model
  differs, `nearest()` refuses with an error naming both. An `@embed` without a
  model is not checked.
- Schema apply refuses a change of a property's `@embed` source or model: that
  is a re-embed, not a migration.
- A cluster declares named `providers.embedding` profiles that graphs bind.
  `api_key` must be a `${NAME}` environment reference resolved at serving boot;
  an inline secret is refused. No secret enters the schema or the ledger.
- `@embed` does no embedding at load time.

## Rejected

- **Do nothing:** the hardwired provider and the missing check leave silent
  cross-space ranking for the first non-Gemini user.
- **An environment-only provider switch with no schema record:** leaves the
  same-space guarantee to operator discipline.
- **A provider trait for plug-ins:** unearned for a small first-party set; the
  enum can become a trait behind the same config.
- **Record the identity in the manifest or Lance field metadata:** the
  manifest is the wrong granularity, and field metadata needs new wiring and a
  dataset open on the query path; the IR is already read at query time.
