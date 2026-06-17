# Omnigraph 0.7 — Essence

The core points of the 0.7 release. Full notes: [v0.7.0.md](v0.7.0.md).

**In one line:** 0.7 makes Omnigraph a **cluster-first server**. A deployment is a
declarative cluster on your own object storage, config collapses to two
single-owner surfaces, the CLI is unified and capability-aware, and
`omnigraph.yaml` is gone.

## Five core changes

1. **Cluster-only server.** `omnigraph-server` boots only from
   `--cluster <dir | s3://bucket/prefix>` and serves every graph under
   `/graphs/{id}/…`. No positional-URI boot, no single-graph flat mode. A cluster
   can live entirely on object storage (`storage:` in `cluster.yaml`), so a
   serving box needs only the bucket URI plus credentials (config-free).

2. **Config is two single-owner surfaces.** The team owns `cluster.yaml` (graphs,
   schemas, stored queries, policies); each operator owns
   `~/.omnigraph/config.yaml` (identity, named servers, aliases, defaults).
   Credentials live in `~/.omnigraph/credentials`, never in a config file.
   **`omnigraph.yaml` is removed** and is read by neither the CLI nor the server.

3. **Unified, capability-aware CLI.** One bulk-write command, `omnigraph load`,
   with a required `--mode merge|append|overwrite`. Honest addressing via
   `--store` / `--server` / `--cluster` / `--profile` / `--graph` (a remote is
   reached only with `--server`). Stored queries and mutations are invoked **by
   name**; operator aliases get their own namespace (`omnigraph alias <name>`);
   destructive writes against a non-local target require `--yes`.

4. **Engine & substrate.** Lance 6.0.1 → 7.0.0; indexed graph traversal; index
   materialization as derived state (`schema apply` records intent, `optimize`
   reconciles); recovery heals on the next write rather than only at restart;
   composite `@unique(a, b)` enforced as a true key.

5. **Provider-independent embeddings.** One embedding client behind a sealed
   provider enum (OpenAI-compatible / native Gemini / mock), defaulting to
   **OpenRouter**. `@embed("source", model="…")` records the embedding identity,
   and a query whose resolved model differs from the recorded one is rejected
   (no silent cross-space ranking).

## Must-know breaking changes

- **`omnigraph.yaml` is removed.** No migrate command; recreate config as a team
  `cluster.yaml` plus a per-operator `~/.omnigraph/config.yaml`.
- **`omnigraph-server` requires `--cluster`.** No positional-URI / single-graph boot.
- **Default embedding provider flips to OpenRouter.** Gemini-direct users set
  `OMNIGRAPH_EMBED_PROVIDER=gemini`.
- **`query --alias` is removed** in favor of `omnigraph alias <name>`.
- **`query`/`mutate` positional is a stored-query *name*,** not a graph URI;
  `--target`, the positional `http(s)://` → remote dispatch, and `--as` on served
  writes are gone.
- **`omnigraph load` `--mode` is required**, `omnigraph ingest` is deprecated, and
  loading into a missing branch errors without `--from`.
