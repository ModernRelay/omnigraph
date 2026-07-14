# RFC: Console-Grade Read Surface — Changes Feed, Merge Dry-Run, Branch Divergence, Commits Pagination, Graph Status

**Status:** Proposed
**Date:** 2026-07-13
**Tickets:** unfiled as of this draft — one per phase below (§Phasing)
**Builds on:** the shipped HTTP endpoint inventory ([server.md](../user/operations/server.md)), the engine change-detection module (`crates/omnigraph/src/changes/`), the merge executor (`crates/omnigraph/src/exec/merge.rs`), the manifest-projected commit graph (`crates/omnigraph/src/db/commit_graph.rs`, RFC-013 Phase 7), and the cluster serving snapshot ([rfc-005](rfc-005-server-cluster-boot.md), `crates/omnigraph-cluster/src/serve.rs`).

## Summary

Five **additive, read-side** HTTP surfaces, motivated by the first external console and SDK
consumers built against a live 0.8 server. Every one is a projection of state the engine already
computes — no new write paths, no storage changes, no new invariants:

1. **Changes feed** — `GET /graphs/{id}/changes?from=&to=` exposing the engine's
   `diff_between`/`diff_commits` (currently CLI/SDK-embedded only).
2. **Merge dry-run** — a `dry_run` mode on `POST /graphs/{id}/branches/merge` that runs the
   existing pre-commit conflict pass and reports, without publishing.
3. **Branch divergence summary** — `GET /graphs/{id}/branches?verbose=true` returning per-branch
   `{head, ahead, behind, merged, actor, updated_at}` computed server-side.
4. **Commits pagination** — `limit`/`before` on `GET /graphs/{id}/commits`, with the response
   ordering finally pinned in the contract.
5. **Graph status** — `GET /graphs/{id}/status` projecting the boot-time serving snapshot
   (applied revision digests, policy bindings, quarantine diagnostics).
6. **Blob content** — `GET /graphs/{id}/blob` streaming one row's `Blob` property with
   `Content-Length`, `Accept-Ranges`, and HTTP Range support — exposing the engine's existing
   SDK-only `read_blob` over the wire (Lance's `BlobFile::read_range` makes partial reads
   zero-copy against the object store).
7. **Structured schema catalog** — a `catalog` representation on `GET /graphs/{id}/schema`
   (types, properties, keys, edges as JSON), so clients stop re-parsing `.pg` source with
   approximations of the grammar.

Explicit non-goals, with rationale: push/streaming change subscription, a metrics/stats surface,
maintenance operations over HTTP, and runtime graph add/remove (§Non-goals).

## Motivation — validated consumer pain

Each gap below was hit while building a real console against the served API, and each has a
shipped workaround whose cost demonstrates the missing surface (validated 2026-07-13 against a
live 0.8.1 server and this branch's source):

- **No diff over HTTP.** A branch-review UI cannot show what a branch changes (per-type
  added/updated/deleted, per-row change lists). The engine has exactly this —
  `Omnigraph::diff_between` / `diff_commits` over `ChangeFilter` → `ChangeSet`
  (`db/omnigraph.rs:1189–1239`) with a manifest fast path and ID streaming
  ([changes.md](../user/branching/changes.md)) — but no server route reaches it. Consumers see
  branch *names* and commit *ids*, never contents-at-a-glance.
- **Conflicts are post-attempt only.** `POST /branches/merge` reports conflicts as a 409 with
  structured `merge_conflicts` — after attempting the merge. A console cannot render a
  "Conflicts" state before the user clicks Merge, and a CI gate cannot check mergeability
  without mutating on success.
- **Divergence requires client-side archaeology.** `GET /commits?branch=X` returns one branch's
  lineage projection (its own commits plus shared main ancestry — commits on other branches are
  never included; a merged branch's commits are never copied into main's projection, only
  pointed at via `merged_parent_commit_id`). Ahead/behind/merged for a branches page therefore
  costs the client **one full-lineage fetch per branch plus main's**, then an ancestry walk over
  the union so merge-parent pointers resolve. That is O(branches) requests of O(history) payload
  each, re-implemented per client — and capped clients degrade to "≥" lower bounds.
- **Commits are unbounded.** The full lineage returns in one response ("Pagination — none" is
  documented). A 633-commit graph ships ~130 KB per read today and grows linearly with history —
  invariant 15's cost face, surfaced at the API boundary. The response ordering (ascending
  `manifest_version`) was also never part of the documented contract, and the endpoint's
  original doc-comment claimed both "most recent first" *and* "omit to list across all branches"
  — both wrong against source, both corrected 2026-07-13. Pagination is the occasion to pin the
  contract deliberately (Hyrum's Law is already in effect).
- **Graph status is invisible.** `GET /graphs` returns `{graph_id, uri}` and nothing else
  (`api-types/src/lib.rs:703` — intentionally minimal for routing). Applied revision, policy
  bindings, quarantine state, and schema digest all exist in the cluster ledger and the boot
  snapshot, but are only reachable via `omnigraph cluster status` against the cluster directory
  — which a remote console, by design, cannot address.
- **Clients re-parse `.pg` source because the catalog isn't served.** `GET /schema` returns
  `{schema_source}` only (`handlers.rs:1114`), so a console that needs the type list, key
  properties, or edge endpoints must re-derive them from source text with its own parser — a
  second implementation of the grammar that WILL drift (observed 2026-07-14: a client regex
  parser silently dropped every type annotated inline with `@instruction("…")`, rendering a
  whole graph empty). The authoritative structure already exists: the compiler's `Catalog`
  (`omnigraph-compiler/src/catalog/mod.rs:14` — `node_types`/`edge_types`/`interfaces`, each
  with properties, keys, unique/index/range/check constraints, embed sources, blob properties)
  is built for every serve and never leaves the process.
- **Blob content is unreachable over HTTP.** `Blob` properties come back as `null` from `/query`
  (the executor deliberately excludes blob columns from scans and re-adds them as null — partly
  a Lance workaround: `BlobsDescriptions` + filter trips a projection assertion,
  `exec/query.rs:1904`), and `entity_at_target` never materializes them. The ONLY wire path that
  carries blob bytes is `POST /export`, which inlines `base64:`-prefixed strings into a bulk
  NDJSON stream of the whole table — O(table) transfer to preview one image. Meanwhile the
  engine already ships `Omnigraph::read_blob(type, id, prop) → lance::BlobFile`
  (`db/omnigraph.rs:1377`) — SDK-only, main-snapshot-only, and never routed.

## Validated current state

Source-checked claims this design rests on:

| Fact | Where |
|---|---|
| `diff_between(from, to, filter)` / `diff_commits(from_id, to_id, filter)` take `ReadTarget` (branch **or** snapshot/commit id) and return `ChangeSet { from_version, to_version, branch, changes: Vec<EntityChange>, stats: ChangeStats }`; `EntityChange { table_key, kind, type_name, id, op, manifest_version, endpoints }`; `ChangeFilter { kinds, type_names, ops }` | `db/omnigraph.rs:1189`, `changes/mod.rs:35–66` |
| Merge conflict detection completes **before** any commit: `stage_streaming_table_merge` collects `MergeConflict`s per table, and `if !conflicts.is_empty() { return Err(OmniError::MergeConflicts(..)) }` fires before the recovery sidecar is written and before any `commit_staged` (constraint validation — `validate_merge_candidates` — also runs pre-commit) | `exec/merge.rs:1402–1451` |
| The 409 wire shape for conflicts (`merge_conflicts: [{kind, table_key, row_id, message}]`) already exists and round-trips through the SDK | `api-types`, server merge handler |
| The commit graph is a per-branch pure projection of `__manifest` lineage rows; `CommitGraph::merge_base` already computes cross-branch merge bases via ancestor-distance walks over the union of two projections | `db/commit_graph.rs:99–160` |
| `list_commits` returns ascending `manifest_version` order (tie-broken by `created_at`, then id); the handler preserves it | `db/commit_graph.rs:99`, `handlers.rs:1652` |
| `PolicyRequest { action, branch, target_branch }` already carries two branch scopes (the merge handler uses both), so no Cedar vocabulary change is needed for two-branch reads | `handlers.rs`, `omnigraph-policy` |
| The server boots from `ServingSnapshot { graphs, queries, policies, diagnostics }`; policies carry `applies_to` bindings; the applied revision records `config_digest` plus a per-resource `digest`; serving deliberately never re-reads mutable cluster state after boot | `omnigraph-cluster/src/serve.rs:1–52`, `types.rs:568–597` |
| Read endpoints are not admission-gated; mutating endpoints are | [server.md](../user/operations/server.md) |
| `Omnigraph::read_blob(type, id, prop)` exists: snapshot → SQL row lookup → `take_blobs` → `lance::BlobFile`; it pins the MAIN snapshot only (no branch/target param) and reaches Lance through the `pub(crate)` `SnapshotHandle::dataset()` accessor — deliberately NOT on the sealed `TableStorage` trait | `db/omnigraph.rs:1377–1422`, `export.rs:179` |
| Lance 7.0.0 `BlobFile` supports true partial reads: `read_range(Range<u64>) → Bytes` and `read_ranges(&[Range])` translate blob-local to physical offsets and issue bounded object-store `get_opts` — no whole-blob materialization; plus `size()`, `seek/tell`, and `uri()` for external blobs. Storage classes: Inline ≤64KB (in data file), Packed ≤4MB / Dedicated >4MB (sidecar `.blob` files), External (URI) | `lance-7.0.0/src/dataset/blob.rs:1185–1298` |
| Blob reads are version-pinned (a dataset handle checked out at version V reads that version's descriptors/sidecars) and require stable row ids — which every OmniGraph table has (`_rowid` is already load-bearing for export) | `lance-7.0.0/src/dataset.rs:1460`, `export.rs:162` |
| Lance stores NO content-type/MIME anywhere in the blob descriptor or field metadata — media type is entirely the application's concern | `lance-arrow-7.0.0/src/lib.rs:49–54` |

## Design

### 1. Changes feed — `GET /graphs/{id}/changes`

```
GET /graphs/{id}/changes?from=<ref>&to=<ref>[&summary=true][&kind=node|edge][&type=T][&op=insert|update|delete][&limit=N][&cursor=…]
```

- `from`/`to` are **refs**: a branch name or a graph-commit id. Resolution mirrors the engine —
  a value matching an existing commit id resolves as `ReadTarget::Snapshot`, otherwise as
  `ReadTarget::Branch` (the SDK can expose them as separate typed fields). This one endpoint
  covers branch-vs-branch (branch review), commit-vs-commit (audit), and branch-vs-commit
  (what changed since I last looked).
- **`summary=true` returns only `ChangeStats` per type** — `{type_name, inserts, updates,
  deletes}` rows. This is the cheap diffstat a console's branch list and branch-detail header
  need, and it is Phase 1a: the full row listing (1b) can follow.
- Full mode returns camelized `ChangeSet`: `{fromVersion, toVersion, branch, changes: [...],
  stats}`. `changes` is paginated (`limit` + opaque `cursor` — a `(table_key, id)` watermark,
  matching the engine's per-table ordered streaming) because a bulk-load delta is O(rows).
- **Auth:** bearer + `read`, enforced on **both** resolved branches via the existing
  `PolicyRequest { branch: to_branch, target_branch: from_branch }` shape. Not admission-gated
  (read-only), consistent with `/snapshot` and `/commits`.
- **Invariants:** each request resolves two snapshots once and reads only those (invariant 3);
  the diff is a pure projection (invariant 15 — the manifest fast path bounds cost to the
  touched tables, not history).

### 2. Merge dry-run — `dry_run` on `POST /graphs/{id}/branches/merge`

```
POST /graphs/{id}/branches/merge   { "source": "...", "target": "...", "dry_run": true }
```

- Runs the existing pipeline **through conflict detection and constraint validation, then stops
  before the recovery sidecar** — the seam already exists: conflicts and
  `validate_merge_candidates` both complete before any `commit_staged` or manifest publish
  (`exec/merge.rs:1446`).
- **Response is 200 in both directions** — for a dry run, finding conflicts is the *successful*
  result of the check, not a failure:
  `{ "would_merge": true, "predicted_outcome": "fast_forward" | "merge" | "already_up_to_date" }`
  or `{ "would_merge": false, "conflicts": [ …the existing merge_conflicts rows… ] }`.
  The real merge's 409 contract is unchanged.
- **Auth:** same `branch_merge` action and admission gate as the real merge. Rationale: the
  dry run reveals merge-internal validation state and exercises the same heavy three-way
  cursor machinery, so both the permission question ("who may dry-run") and the cost question
  ("how many concurrent dry-runs") should answer exactly like the merge itself. A weaker
  `read`-scoped variant was considered and rejected — an actor who can read both branches could
  in principle compute the merge client-side, but the *server cost* is the merge's, and
  admission accounting keys off the action. (Open question 1 keeps this revisitable.)
- **Implementation note (the one real decision):** today's conflict pass *stages uncommitted
  Lance writes while it classifies* — a naive early-return leaves orphaned staged fragments
  (harmless, crash-equivalent, reclaimed by `cleanup`, but noisy under a polling console). The
  clean shape is a mode flag on `stage_streaming_table_merge` that runs the three-way cursor
  and `classify_merge_conflict` while **skipping the output writer** — classification is
  per-row and independent of the written output. Ship the flag with the endpoint; do not ship
  the orphan-generating variant behind a poll-shaped API.

### 3. Branch divergence summary — `GET /graphs/{id}/branches?verbose=true`

```
GET /graphs/{id}/branches?verbose=true[&base=main]
→ { "branches": [ { "name": "...", "head_commit_id": "...", "ahead": 3, "behind": 13,
                    "merged": false, "actor_id": "act-...", "updated_at": 1783938854410972 } ] }
```

- Plain `GET /branches` is byte-unchanged (`BranchListOutput { branches: Vec<String> }` —
  Hyrum). `verbose=true` is the additive opt-in.
- Semantics (documented in the contract, matching the engine's own `merge_base` walk):
  - a branch's **own head** = its newest commit stamped with the branch name (`manifest_branch`);
  - `ahead`/`behind` = ancestor-set differences between the branch head and the base head,
    walking `parent_commit_id` **and** `merged_parent_commit_id` over the union of both
    projections (the union is load-bearing: base merge commits point at rows that exist only in
    the source branch's projection);
  - `merged` = the branch's own head is reachable from the base head. A fresh fork with no own
    commits reports `ahead: 0, behind: N, merged: false, head_commit_id: null` — not "merged".
- Server-side this is **exact** (full projections, no client cap) and computed once per request
  instead of N times per client; the `CommitGraph` projections it walks are already
  warm-cached per coordinator. Cost is bounded by lineage size — acceptable now, and the same
  work N client round-trips currently force; if it ever needs to be cheaper, per-branch head
  metadata in `__manifest` (`graph_head` rows already exist) gives an incremental path without
  changing this contract.
- **Auth:** bearer + `read` (branch scope: the listed branch set). Not admission-gated.

### 4. Commits pagination — `limit`/`before` on `GET /graphs/{id}/commits`

```
GET /graphs/{id}/commits?branch=X&limit=100[&before=<manifest_version>]
→ { "commits": [ …newest-first… ], "next_before": 412 }   // absent on the last page
```

- **Without `limit`, behavior is byte-unchanged**: the full lineage, ascending
  `manifest_version` — now *documented* as such rather than accidentally shipped (the previous
  doc-comment's "most recent first" claim was wrong against source).
- With `limit`: the newest `limit` commits **descending**, plus `next_before` — the
  `manifest_version` watermark for the next page. `before=<version>` pages older. Version is
  the natural cursor: it is unique per branch projection, totally ordered, and already the
  projection's sort key.
- `CommitListOutput` gains an optional `next_before` field — additive, absent today and on
  unpaginated responses.
- The endpoint doc keeps the branch-scoping contract explicit (one branch's lineage; other
  branches' commits never included — the corrected 2026-07-13 wording).

### 5. Graph status — `GET /graphs/{id}/status`

```
GET /graphs/{id}/status
→ { "graph_id": "...", "uri": "...", "serving": true,
    "quarantined": null | { "reason": "..." },
    "applied_revision": { "config_digest": "sha256:...", "graph_digest": "sha256:..." },
    "policies": [ { "name": "...", "applies_to": ["cluster", "graph.knowledge"] } ],
    "embedding_provider": "gemini-prod" | null }
```

- **Data source is the boot-time `ServingSnapshot` + `AppliedRevisionState`, held in process
  since boot** — the server does *not* re-read mutable cluster state per request, preserving
  serve.rs's explicit doctrine. Status therefore reflects the **booted revision**; after
  `cluster apply`, it updates on restart. That staleness is by design and documented: the
  cluster mutation path *is* apply + restart (RFC-011), so "what this server is serving" is
  precisely what a console should render, and it is immune to showing a half-applied ledger.
- Per-graph quarantine diagnostics (the boot loader already quarantines a graph with pending
  recovery sidecars while siblings serve) surface as `quarantined.reason` — today they exist
  only in startup logs.
- **The convergence pairing:** the booted `config_digest` here is the *serving-side* half of
  the applied-vs-serving check; the *ledger-side* half (the currently applied digest + run
  history) is the control-plane service's `GET /clusters/{id}/status|runs`
  ([rfc-016](rfc-016-control-plane-service.md)). A console compares the two digests — mismatch
  means "applied but not yet serving; restart pending." Neither side reads the other's source.
- **Auth:** bearer + `read` **on the graph** — deliberately per-graph, not the server-scoped
  `graph_list` action. A console holding a graph-scoped token can render its own graph's status
  without the cluster-management policy bundle that `GET /graphs` requires. `GET /graphs` itself
  stays minimal (`{graph_id, uri}`) — it is a routing surface, and enriching it would drag
  status data behind the wrong (server-scoped) auth boundary.

### 6. Blob content — `GET /graphs/{id}/blob`

```
GET  /graphs/{id}/blob?type=Artifact&id=art-…&prop=blob[&branch=main]
HEAD /graphs/{id}/blob?…                                  → Content-Length + Accept-Ranges only
```

- **The engine primitive already exists**: `Omnigraph::read_blob(type, id, prop)` resolves a
  snapshot, finds the row by key, and returns a `lance::BlobFile`
  (`db/omnigraph.rs:1377–1422`). This surface is an HTTP exposure plus two upgrades — it is NOT
  a new read path:
  1. **Branch/target parameter**: `read_blob` pins the main snapshot today; extend it to
     `read_blob_at(target: ReadTarget, …)` so the endpoint reads the requested branch at one
     pinned snapshot (invariant 3), exactly like `/snapshot` and `/query`.
  2. **Streamed, range-capable body**: never call `BlobFile::read()` (whole-blob
     materialization — fine for export's small artifacts, wrong for video). Lance 7.0.0's
     `BlobFile::read_range(Range<u64>)` does bounded object-store reads with zero whole-blob
     buffering, so the handler maps HTTP `Range` → `206 Partial Content` directly, and
     full-body GETs stream fixed-size chunks (e.g. 4 MB) through the same `Body::from_stream`
     pattern `/export` already uses. `Content-Length` comes free from `BlobFile::size()`;
     `Accept-Ranges: bytes` is advertised. Storage classes make this efficient by construction:
     Lance keeps blobs >4 MB in dedicated sidecar files, so a range read touches only that blob.
- **External-URI blobs**: `BlobFile::uri()` is `Some` for `External` blobs (the loader accepts
  `s3://…` references as well as `base64:` payloads). The endpoint answers
  `302 Found → Location: <uri>` rather than proxying bytes it does not hold — the descriptor is
  the source of truth, the content deliberately lives elsewhere.
- **Content-Type**: Lance stores no MIME anywhere (validated — descriptor and field metadata
  carry only position/size/kind/uri). The handler sniffs magic bytes from the first chunk
  (`infer`-style) and falls back to `application/octet-stream`; a schema-level MIME property
  convention stays an application concern. `ETag` derives from the pinned
  `(table version, row id)` pair — stable, content-addressed enough for caching, and free.
- **Errors**: 404 unknown row / null blob, 400 non-Blob property, 416 unsatisfiable range —
  all typed (invariant 13).
- **Auth:** bearer + `read` with branch scope — a blob is row data, same class as `/query` and
  `/snapshot`. Not admission-gated (read-only); large-transfer rate control, if ever needed, is
  a reverse-proxy concern like `/export`'s today.
- **Boundary note:** `take_blobs` stays OFF the sealed `TableStorage` trait (it is a
  Lance-specific descriptor-materialization API; both existing call sites — export and
  `read_blob` — reach the inner dataset through the `pub(crate)` accessor, and the
  `forbidden_apis` source-walk guard already sanctions this shape). No upstream Lance change is
  needed for any of this — everything required is public in 7.0.0. The only blob item still
  Lance-blocked is compaction (`LANCE_SUPPORTS_BLOB_COMPACTION`, unrelated to reads).

### 7. Structured schema catalog — `format=catalog` on `GET /graphs/{id}/schema`

```
GET /graphs/{id}/schema?format=catalog
→ { "types": [ { "name": "Person", "kind": "node", "implements": [],
                 "key": ["slug"],
                 "properties": [ { "name": "slug", "type": "String", "nullable": false,
                                   "key": true, "unique": false, "indexed": true,
                                   "blob": false, "embedSource": null } ],
                 ... } ],
    "edges": [ { "name": "Knows", "from": "Person", "to": "Person",
                 "card": null, "properties": [...] } ],
    "interfaces": [ ... ] }
```

- **A pure projection of the compiler's `Catalog`** — the struct the engine already builds and
  validates on every boot (`node_types`/`edge_types`/`interfaces` with properties, keys,
  unique/index constraints, embed sources, blob properties). No new computation; the handler
  serializes what's in memory.
- **Why it belongs in this RFC:** every schema-aware client currently re-parses `.pg` source
  text, i.e. maintains a second implementation of the grammar. That is the "shadow copy that
  drifts" failure shape of invariant 15, exported to consumers — and it has already bitten
  (silent type loss on annotated schemas). One source of truth, cheaply projected, lets clients
  delete their parsers; the raw source stays the default response for display/editing.
- **Scope discipline:** the catalog response is a *read model for consumers* (form generation,
  browse projections, graph topology), not a schema-migration surface — `schema plan/apply`
  semantics are untouched, and internal fields with no consumer meaning (e.g. Arrow schemas)
  are not serialized. Property types use the `.pg` spellings (`String`, `Vector(3072)`,
  `[String]`, `enum(a, b)`) so clients never re-derive them.
- **Auth:** bearer + `read`, same as the source form. Additive: `format` defaults to the
  current `{schema_source}` response, byte-stable.

## Non-goals

- **Push/streaming (SSE, webhooks, change subscriptions).** Deferred, not rejected. The changes
  feed's `(from, to)` ref pair is the primitive a subscription would replay; shipping the poll
  form first validates the payload shape without committing to a delivery contract. Consoles
  poll with short stale-times today and that is adequate at current scale.
- **Metrics/stats surface** (query latency, table/fragment sizes, index coverage, admission
  counters). Real need, different design space (observability trait surfaces, invariant 13) —
  deserves its own RFC rather than a rider on this one.
- **Maintenance over HTTP** (`optimize`/`repair`/`cleanup`). These address cluster storage
  directly by design (RFC-011); exposing them through the serving path needs the Admin action
  vocabulary (MR-724 Option A reserves it) and a two-person-rule story first.
- **Runtime graph add/remove.** Unchanged: `cluster apply` + restart ([server.md](../user/operations/server.md)).
- **Ledger-side status and cluster operations.** Applied-revision state, run history, and
  remote `plan`/`apply` belong to the control-plane service track
  ([rfc-016](rfc-016-control-plane-service.md)), never the serving path — this RFC's surfaces
  stay pure projections of what the *server* holds.

## Compatibility & rollout

- Every change is **additive**: new route (`/changes`, `/status`), new optional params
  (`verbose`, `limit`/`before`, `dry_run`), new optional response fields (`next_before`).
  Existing responses stay byte-stable without the new params.
- Each phase regenerates `openapi.json` (`OMNIGRAPH_UPDATE_OPENAPI=1`, per
  [testing.md](testing.md)) and the TypeScript SDK re-syncs its vendored spec copy in its own
  release.
- CLI follow-ups ride each phase: `omnigraph changes --from --to`, `branch list --verbose`,
  `commit list --limit/--before`, `branch merge --dry-run`, `graphs status` — all thin wrappers
  over the same routes, keeping served and embedded surfaces in parity (RFC-009's referee
  covers the forked verbs).
- Contract discipline (deny-list: "shipping observable behavior as if it weren't part of the
  contract"): ordering, cursor opacity, and the dry-run response shape are documented from day
  one, because this RFC exists partly *because* two undocumented behaviors (commit ordering,
  branch scoping) were mis-documented and got depended on.

## Invariants check

- No new write paths; the dry-run stops before the sidecar boundary, so invariant 5 (recovery
  coverage) is untouched — provided the write-skipping mode ships (§2), not the orphaning
  variant.
- Every new read resolves one snapshot (or one snapshot pair) per request — invariant 3.
- Everything served is a projection of Lance/`__manifest`/the boot snapshot — invariant 15; no
  parallel state, no cold re-derivation on a hot path (divergence walks warm projections;
  status reads boot-held state).
- Failures stay typed and bounded: changes/status reuse the existing error taxonomy; dry-run
  conflicts are a 200-shaped *result*, never a silent degradation (invariant 13).

## Phasing

| Phase | Surface | Size | Unblocks |
|---|---|---|---|
| 1a | `/changes?summary=true` | S — route + camelize `ChangeStats` | branch diffstat, review UX |
| 1b | `/changes` row listing + cursor | M — pagination watermark | per-row diff view, audit |
| 2 | commits `limit`/`before` | S — projection slice + one field | deep-history graphs stop shipping full lineage |
| 3 | branches `verbose=true` | M — server-side divergence walk | branches page in one request, exact counts |
| 4 | merge `dry_run` | M — write-skipping cursor mode | pre-merge conflict UX, CI mergeability gates |
| 5 | `/graphs/{id}/status` | S — project boot snapshot | console graph/deployment status |
| 6 | `/graphs/{id}/blob` | M — `read_blob_at` target param + Range/streaming handler | image/file preview + download in consoles; media-bearing agent tools |
| 7 | schema `format=catalog` | S — serialize the in-memory `Catalog` | clients delete their `.pg` parsers (drift class closed at the source) |

Ordered by consumer pain over implementation order; 1a and 2 are each afternoon-sized and
independently shippable.

## Open questions

1. **Dry-run action scope** — `branch_merge` (proposed: permission + admission symmetry with the
   real merge) vs `read` (weaker; the result is derivable from two readable branches). Revisit
   if read-only consoles without merge rights need mergeability signals.
2. **Changes cursor** — `(table_key, id)` watermark (proposed; matches per-table ordered
   streaming) vs a version-window token. Decide when 1b lands; 1a doesn't need it.
3. **Divergence cost at pathological branch counts** — if a graph accumulates hundreds of
   branches, `verbose=true` walks hundreds of projections per request. `graph_head` manifest
   rows give an incremental head-only fast path; measure before building it.
4. **Should `/status` include stored-query registry state** (names + validation advisories from
   boot)? Cheap to add from `ServingSnapshot.queries`; deferred until a consumer asks.
