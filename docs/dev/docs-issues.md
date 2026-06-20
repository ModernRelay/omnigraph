# User Docs Coherence Ledger

**Last review:** 2026-06-20 (against 0.7.1)
**Status:** all open findings resolved — living ledger for future audits.

This page tracks stale or incoherent user-doc claims found during broad docs
reviews. Findings are validated against current **code/behavior**, not just
cross-doc consistency. Record new findings as they surface; mark them resolved
(with the fixing commit) once the public pages are corrected.

## Resolved — 2026-06-20 docs/user coherence sweep

Every finding from the 2026-06-20 review was validated (all reproduced) and
fixed. Branch `docs/user-coherence-0-7-1`.

| Pri | Finding | Resolution |
|---|---|---|
| P1 | `cluster apply` documented as catalog-only / "Stage 3A" with graph+schema deferred — in both `cli/reference.md` and the shipped CLI help (`cli.rs`) | Rewrote both to describe the real converge behavior (creates graphs, applies schema with soft drops, writes catalog, executes approved deletes in one ordered run); `deferred` now means the genuinely-unsupported case (standalone schema delete). |
| P1 | Stored-query exposure had two contracts: `server.md` documented a per-query `mcp:{expose:false}` knob; cluster docs said all queries are listed | Confirmed in code: cluster registry has no expose field (`QueryConfig`), boot bridge hardcodes `expose: true` (`omnigraph-server` settings), no GQ-level annotation. Removed the knob from `server.md`; documented "every applied query is listed; per-query exposure may become a Cedar-policy decision later". |
| P1 | The same stale "`mcp.expose == true` subset" contract lived in the **OpenAPI surface**: utoipa annotations (`handlers.rs:1029,1037`, `omnigraph-api-types/src/lib.rs:404`) drove `openapi.json` (Greptile catch on #293) | Updated the three Rust doc-comment/annotation strings to "every stored query" and regenerated `openapi.json` (`OMNIGRAPH_UPDATE_OPENAPI=1`); drift test green. Same-change per AGENTS.md rule 4. |
| P2 | `schema/index.md` claimed `allow_data_loss` honored "uniformly across transports" incl. HTTP `POST /schema/apply` | Scoped to the direct/embedded path; added that cluster-managed graphs evolve via `cluster apply` (soft drops only) and the HTTP route is 409-disabled for cluster serving. |
| P2 | `/load` missing from admission / body-limit / rate-limit / manifest-conflict prose (named `/ingest` only); constants called it "Ingest body limit" | Documented `/load` as canonical everywhere with `/ingest` as the deprecated alias; renamed the constant to "Load (bulk-write) body limit". |
| P2 | CLI "Bearer token resolution" section listed removed `omnigraph.yaml` keys (`graphs.<name>.bearer_token_env`, `auth.env_file`) | Replaced with a pointer to the keyed-credential model (`OMNIGRAPH_TOKEN_<NAME>` → `~/.omnigraph/credentials` → `OMNIGRAPH_BEARER_TOKEN`); no plaintext-in-config path. |
| P2 | Flat route names in a cluster-only server (`POST /query`, `POST /mutate`, `GET /queries`, `POST /queries/{name}`) | Added a one-line note that the per-graph subsections use shorthand under `/graphs/{id}/…`; the endpoint table is already fully qualified. |
| — | `version` printed `omnigraph 0.3.x` | → `0.7.x`. |
| — | `search/indexes.md` used deprecated `ingest --mode merge` | → `load --mode merge`. |
| — | `config.md` `deferred` disposition described as "graph/schema change, later phase" | → "an unsupported change (e.g. standalone schema delete)". |
| — | Stale stage labels (`Stage 3A`, `Stage 2C`, `Stage 1`) in active reference docs | Removed / reworded to plain language; release notes keep history. |

## Open — surfaced 2026-06-20, not yet fixed

- **Stale "config-only apply" / "Stage 3A" comments in `omnigraph-cluster`
  source** (internal rustdoc, not user docs — out of scope for the docs sweep
  above): `src/types.rs:147` ("Applied changes execute (config-only query/policy
  catalog writes)"), `src/types.rs:265` ("Output of config-only cluster apply"),
  `src/diff.rs:256`, and `src/tests.rs:1129` ("config-only apply (Stage 3A)").
  Apply now also runs graph creates, schema applies, and approved deletes
  (`diff.rs:411` `GraphCreate` / `SchemaApply`; the Stage-4 create/schema/delete
  executors + tests `apply_creates_graph_and_unblocks_dependents`,
  `apply_schema_update_and_dependent_query_in_one_run`,
  `apply_blocks_graph_delete_without_approval`). Update these comments in a
  cluster-crate change.
- **Cross-repo drift from this sweep** (separate repos — track here, fix in a
  follow-up in each repo):
  - `omnigraph-ts` SDK ships a stale generated `spec/openapi.json` +
    `packages/sdk/src/generated/types.gen.ts` still describing the `GET /queries`
    catalog as the `mcp.expose` subset. Regenerate from this repo's
    `openapi.json` once the SDK's deferred refresh happens (the SDK is known to
    lag the API by design).
  - `omnigraph-cookbooks/docs/best-practices.md` (~line 372) still describes
    client-side auth as resolving through the removed `bearer_token_env` chain.
    Update to the keyed-credential model (`OMNIGRAPH_TOKEN_<NAME>` →
    credentials file → `OMNIGRAPH_BEARER_TOKEN`).

## Verification checklist (re-run on the next docs audit)

```bash
rg -n "Stage [0-9]|graph/schema changes are deferred|reserved for later stages" docs/user crates/omnigraph-cli/src/cli.rs
rg -n "POST /query|POST /mutate|GET /queries|POST /queries/\{name\}|POST /schema/apply" docs/user
rg -n "ingest --mode|Ingest body limit|/ingest" docs/user
rg -n "0\.3\.x|bearer_token_env|auth\.env_file" docs/user
rg -n "expose: false|mcp\.expose" docs/user
```

Expected: active user docs have no matches for stale phrases, or the remaining
matches are explicitly marked as deprecated aliases, "no longer exist" notes, or
route shorthand disclaimed relative to `/graphs/{id}`. Release notes are allowed
to preserve historical behavior.
