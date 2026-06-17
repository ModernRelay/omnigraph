:rocket: *Omnigraph 0.7: the essence*

0.7 makes Omnigraph a *cluster-first server*. A deployment is a declarative cluster on your own object storage, config collapses to two single-owner surfaces, the CLI is unified, and `omnigraph.yaml` is gone.

*Five core changes*

1. *Cluster-only server.* `omnigraph-server` boots only from `--cluster <dir | s3://bucket/prefix>` and serves every graph under `/graphs/{id}/…`. A cluster can live entirely on object storage, so a serving box needs only the bucket URI + credentials (config-free).

2. *Config = two single-owner surfaces.* The team owns `cluster.yaml` (graphs, schemas, stored queries, policies); each operator owns `~/.omnigraph/config.yaml` (identity, named servers, aliases, defaults). Credentials live in `~/.omnigraph/credentials`, never in a config file. `omnigraph.yaml` is removed.

3. *Unified, capability-aware CLI.* One bulk-write command `omnigraph load` (required `--mode merge|append|overwrite`); address via `--store` / `--server` / `--cluster` / `--profile` / `--graph` (a remote is reached only with `--server`); stored queries run by name; operator aliases via `omnigraph alias <name>`; destructive non-local writes require `--yes`.

4. *Engine & substrate.* Lance 6.0.1 → 7.0.0, indexed graph traversal, index materialization as derived state (`schema apply` records intent, `optimize` reconciles), recovery heals on the next write (not just at restart), composite `@unique(a, b)` as a true key.

5. *Provider-independent embeddings.* One client behind a sealed provider enum (OpenAI-compatible / native Gemini / mock), default OpenRouter. `@embed("source", model="…")` records the embedding identity; a query whose model differs is rejected (no silent cross-space ranking).

*Must-know breaking changes*
• `omnigraph.yaml` removed (no migrate command; use `cluster.yaml` + `~/.omnigraph/config.yaml`)
• `omnigraph-server` requires `--cluster` (no positional-URI / single-graph boot)
• default embedding provider flips to OpenRouter (Gemini-direct users: `OMNIGRAPH_EMBED_PROVIDER=gemini`)
• `query --alias` removed → `omnigraph alias <name>`
• `query` / `mutate` positional is a stored-query name, not a graph URI; `--target` and positional `http(s)://`→remote are gone
• `omnigraph load` needs `--mode`; `ingest` deprecated; loading a missing branch errors without `--from`
