# RFC 0001: Bulk data plane: canonical load/export interfaces and scale path

| | |
|---|---|
| **Status** | Proposed |
| **Author(s)** | Ragnor Comerford |
| **Discussion** | — |
| **Implementation** | — |

> Status is maintained by maintainers: `Proposed` while the PR is open,
> `Accepted` on merge, `Declined` on close, `Superseded by NNNN` later.

## Summary

Canonicalize the bulk write surface as `load` on every layer (engine, CLI, HTTP, docs), restructure the loader around a transport-agnostic batch core, and stage the scale path in two phases: phase 1 replaces the buffered 32 MB JSON-envelope ingest body with a streamed NDJSON body and adds Arrow IPC content negotiation plus snapshot pinning on exports; phase 2 adds incremental staging for bounded-memory loads, multipart Arrow IPC ingest, load-by-reference from object storage, idempotency keys, and presigned-URL export jobs.

> **Status note (2026-06-13).** The engine and CLI half of the naming work has already shipped on `main` (v0.7.0): `load_as` is canonical with an `Option` fork base, `ingest_as` is a deprecated shim, the CLI `load` is canonical with `ingest` a deprecated alias, fork-if-missing is opt-in, and overwrite is fully staged. What remains for this RFC is the HTTP layer (still `POST /ingest`, still a buffered 32 MB `Json<IngestRequest>`), the streamed transport, the response/export field additions, and all of phase 2. Two things also moved underneath the RFC: the recovery-sweep liveness gap is now partly closed by a write-queue-acquisition guard (#203), which replaces this RFC's earlier grace-window proposal; and the shared DTOs moved to a new `omnigraph-api-types` crate (RFC-009), which is now where the type changes below land. Separately, Lance's proposed native Multi-Table Transactions ([lance#7264](https://github.com/lance-format/lance/discussions/7264)) put the entire manifest-CAS + recovery-sidecar machinery on a path to removal; see "Substrate trajectory" below. The body of this RFC is updated to reflect all of this.

## Motivation

Four concrete defects, each a growing liability:

1. **Naming drift, now isolated to the HTTP layer.** The engine (`load_as`) and CLI (`load`) canonicalization shipped in commits `e676c15`/`fa6af77`, with `ingest_as`/`ingest` kept as deprecated shims. The one remaining holdout is the HTTP route: the only bulk-write endpoint is still `POST /ingest` (`crates/omnigraph-server` route table), so the deprecated name is the single thing every remote client binds to (Hyrum's Law: the route name is contract). The drift is no longer cross-layer; it's the last unmigrated surface.

2. **A hard scale ceiling on remote loads.** The endpoint takes NDJSON as a string field inside a JSON body, fully buffered, capped at 32 MB (`INGEST_REQUEST_BODY_LIMIT_BYTES`, `lib.rs:132`). The CLI reads the whole file with `fs::read_to_string` and sends one request (`crates/omnigraph-cli/src/main.rs:2716`); anything larger fails with 413. There is no chunking anywhere, which preserves atomicity and also means remote loads simply cannot exceed 32 MB. Payloads are also double-encoded (JSON string inside JSON), which roughly doubles parse cost and forbids streaming decode.

3. **Interface gaps that block downstream workflows.** Write responses omit the published commit id even though `CommitOutput.graph_commit_id` exists (`crates/omnigraph-server/src/api.rs:222`), so clients cannot pin, diff, or audit the commit they just created. `ExportRequest` (`api.rs:517-526`) has no commit/version parameter, so exports are unrepeatable. There is no idempotency mechanism, and the recovery protocol makes that gap real rather than theoretical: a crash after all per-table `commit_staged` but before manifest publish is rolled *forward* on the next open, so a client that saw an error and retried an append-mode load double-applies.

4. **A multi-writer hazard, now half-closed.** For `s3://` and local URIs the CLI embeds the engine and writes around any live server. Manifest CAS makes concurrent publishes safe, but the per-(table, branch) write queues are in-process only. The open-time recovery sweep used to act with no liveness guard at all, so an embedded `ReadWrite` open while a server was mid-publish could classify the live write as crash residue and roll it back via `Dataset::restore`. Commit `446b46d` (#203) closed the **in-process** half: write entry points now run `heal_pending_recovery_sidecars` under the same per-(table, branch) write queues a live writer holds, then re-check the sidecar's existence, so a long-lived server no longer wedges or self-rolls-back on its own handle. The **cross-process** half remains: a second process (embedded CLI, or a second server) opening the same storage root has its own in-memory queues, so it can still read and roll back the first process's in-flight sidecar. The residual is a single-coordinator assumption that is now documented but not enforced.

The long-run cost of doing nothing: every external client binds to the deprecated route name, bulk users hit the 32 MB wall and work around it with scripted multi-request loads (silently giving up single-commit atomicity), and the cross-process embedded-vs-served topology hazard waits for a production incident.

## Guide-level explanation

### The canonical pair: `load` and `export`

One verb per direction, on every surface:

```bash
# CLI (unchanged names; new behavior in italics below)
omnigraph load   --data ./batch.jsonl --branch review/x --from main --mode merge s3://bucket/g.omni
omnigraph export --branch main --at 01JX… --format arrow s3://bucket/g.omni
```

```
POST /load?branch=review/x&from=main&mode=merge     # body: application/x-ndjson, streamed
GET  /export?branch=main&at=<commit_id>             # Accept: application/x-ndjson | application/vnd.apache.arrow.stream
```

`POST /ingest` and `POST /export` remain as deprecated aliases using the exact machinery that migrated `/read` → `/query` and `/change` → `/mutate` (`#[deprecated]` handlers, OpenAPI deprecation flags, RFC 9745 Deprecation + RFC 8288 Link headers, `lib.rs:1222-1235`).

### Large loads go to a branch

For data large enough that you would not want it racing live writers on `main`, the pattern is load onto a private branch, validate, then merge:

```bash
omnigraph load --from main --branch ingest/2026-06 --mode append --data ./big.ndjson s3://bucket/g.omni
omnigraph export --branch ingest/2026-06 ...        # validate / spot-check in isolation
omnigraph branch merge ingest/2026-06 --into main s3://bucket/g.omni
```

The load is a sole-writer workload with no contention; the merge is the single retryable step that publishes to `main`, and it fast-forwards when `main` has not moved. The mechanics and why this relaxes the load's requirements are in "Large-scale loads" in the reference design.

### What users gain per phase

**Phase 1**

- Remote loads stream: the CLI streams the file as the request body; the 32 MB buffer cap is replaced by an explicit, documented per-load row/byte budget enforced incrementally.
- Load and mutate responses include `graph_commit_id`.
- `GET /export` accepts `at=<commit_id>` for reproducible, snapshot-pinned exports.
- Query results and single-type exports can be returned as Arrow IPC streams via the `Accept` header (zero new dependencies; `arrow-ipc` is already in the tree via Lance).
- The cross-process write-rollback hazard is closed by a documented single-coordinator topology rule (the in-process half already landed in #203; see reference design).

**Phase 2**

- Loads of arbitrary size with bounded server memory (incremental staging).
- `POST /load` with `Content-Type: multipart/mixed`, one Arrow IPC stream per table part: Arrow-native clients skip JSON entirely (and embedding vectors stop costing ~4× on the wire).
- Load-by-reference: `POST /load` with `{"uri": "s3://…"}`; the server pulls the artifact from object storage, so very large loads need no long-lived HTTP connection and client retries are free.
- `Idempotency-Key` header: retried loads with the same key return the original commit instead of double-applying.
- Export jobs: `POST /exports` materializes snapshot-pinned Arrow/Parquet part-files to object storage and returns presigned GET URLs; clients fetch parts in parallel without proxying bytes through the server.

## Reference-level design

### Engine: transport-agnostic batch core (phase 1, prerequisite for everything else)

Split `load_jsonl_reader` (`crates/omnigraph/src/loader/mod.rs:305-570`) into:

```rust
pub async fn load_batches_as(
    &self,
    branch: &str,
    base: Option<&str>,
    batches: impl Stream<Item = Result<(TableKey, RecordBatch)>>,
    mode: LoadMode,
    actor_id: Option<&str>,
) -> Result<LoadResult>
```

- NDJSON parsing becomes one front-end decoder feeding this core; `build_node_batch` (`loader/mod.rs:573`) moves behind it.
- ID resolution (`@key` extraction, ULID generation) moves to batch level: fill or rewrite the `id` column on a `RecordBatch`. This is the main new code; the constraint validators already operate on `RecordBatch` (`validate_value_constraints`, `loader/mod.rs:1137`) and are reused unchanged.
- Edge referential integrity keeps its current shape: node-ID sets accumulate during decode; edge batches validate against committed + pending IDs before staging.
- The commit tail is untouched: per-table `commit_staged`, one recovery sidecar, one manifest CAS publish (`loader/mod.rs:540-568`). Every transport variant in this RFC funnels into this single boundary.

### Server: phase 1 transport

- `POST /load`: raw `application/x-ndjson` body, parameters in the query string. Chunks feed the decoder as they arrive. Budget enforcement is incremental and loud (typed error naming the exceeded budget), replacing the silent-by-comparison 413.
- Admission control gains a concurrent-streaming-loads-per-actor cap alongside the existing per-actor workload caps.
- `LoadOutput` (renamed from `IngestOutput`, old name kept as an OpenAPI alias) gains `graph_commit_id`, threaded from the publish result. These DTOs now live in the `omnigraph-api-types` crate (extracted in RFC-009 Phase 2, `4821e72`), so the field additions land there and are consumed by both the server handler and the CLI's `GraphClient` arms, ending the triple DTO construction.
- `GET /export`: query-string parameters (`branch`, `at`, `types`), `Accept`-negotiated body. `application/vnd.apache.arrow.stream` is produced per table schema; phase 1 limits IPC output to single-schema responses (query results, single-type exports) and keeps NDJSON for mixed whole-branch exports. `POST /export` stays as the compatibility alias.
- The CLI `load` remote path switches from `fs::read_to_string` + JSON envelope to streaming the file as the request body. The CLI `export` gains `--at` and `--format arrow`.

### Multi-writer safety (phase 1)

The in-process half landed in #203 (`heal_pending_recovery_sidecars` acquires the per-(table, branch) write queues a live writer holds, then re-checks the sidecar before acting). This RFC's earlier grace-window proposal is **withdrawn**: a ULID-age threshold is a heuristic, while the queue-acquisition guard is exact, and shipping both would be two mechanisms for one property. What remains is the cross-process gap, addressed without new coordination state:

1. **Single-coordinator topology rule, documented loudly** in `docs/user/cli.md` and `docs/user/storage.md`: a storage root has at most one writer at a time. A graph served by a running `omnigraph-server` is written through that server; embedded CLI writes are for graphs no server is serving. The CLI cannot detect a server, and the in-memory write queues do not span processes, so this is an operational contract stated where operators will read it.
2. **Don't reintroduce a lease.** A storage-backed lease or heartbeat would close the cross-process gap but adds exactly the kind of drift-prone coordination state the deny-list warns against, and it is short-lived: Lance's native Multi-Table Transactions (see "Substrate trajectory") ship a leased write fence as a first-class primitive, at which point a hand-rolled omnigraph lease would be scaffolding to remove. The topology rule holds the line until the substrate closes the gap properly.

### Policy on the load surface

Load and export sit behind the same Cedar gate as every `_as` writer: `load_as` enforces `PolicyAction::Change`, and fork-if-missing additionally enforces `BranchCreate`. Two gaps surfaced that are worth closing as the surface formalizes. Load mode is invisible to Cedar (overwrite enforces plain `Change`, so a policy cannot allow merge while forbidding the destructive overwrite), tracked as `iss-cedar-context-load-mode`. And embedded-CLI policy attachment depends on a configured policy file rather than being a property of the graph, which is the same single-coordinator territory as the multi-writer rule above.

### Phase 2: scale machinery

- **Incremental staging.** `stage_append` already chains via `prior_stages: &[StagedWrite]` (`crates/omnigraph/src/table_store.rs:856-860`). The loader stages decoded batches as they arrive instead of accumulating them in `MutationStaging`; resident memory drops to the current batch plus unique-key and node-ID hash sets. Commit semantics are unchanged: one `commit_staged` per table, one sidecar, one publish. An aborted stream leaves unreferenced staged data files; `cleanup` garbage-collects them (state derived from observable storage, reconciler-shaped). Merge mode's cross-batch upsert semantics under repeated `stage_merge_insert` need verification; if they conflict, streaming ships for append/overwrite first and merge keeps the accumulate path.

  One isolation interaction must land with this item. Today's loads exhibit a snapshot-isolation write skew on edge referential integrity: edge endpoints are validated against a snapshot plus pending batches, but the publish-time expected-version CAS covers only the tables this load writes (`crates/omnigraph/src/exec/staging.rs:501-514`, `db/manifest/publisher.rs:353-368`), so a concurrent mutation of a node table this load only reads can land between validation and publish, committing edges to nodes that no longer exist. Incremental staging lengthens exactly that window, so phase 2 must pair it with a remedy rather than widening the anomaly: either include the RI-read node tables' versions in the expected map (turning the race into a loud publish conflict, at the cost of false conflicts on unrelated node writes) or re-validate RI at commit time under the touched-table queues. See unresolved question 7.
- **Multipart Arrow IPC load.** `Content-Type: multipart/mixed`, one part per table, each part one Arrow IPC stream (IPC streams are single-schema by spec, so multi-table loads need framing; multipart keeps one request = one `load_as` = one publish, with no cross-call state). Schema compatibility is checked against the catalog per part; ID resolution and all validators run on the decoded batches. Note `allow_64bit` defaults to false in IPC writers, so individual field lengths cap at signed 32-bit, relevant for `Blob` properties and documented as a limit.
- **Load-by-reference.** `POST /load` with a JSON body `{"uri": …}` instead of inline data; the server streams the artifact from object storage through the same decoder. Fetch scope is restricted to the graph's own bucket or a server-issued presigned upload prefix; arbitrary URIs are rejected (SSRF).
- **Idempotency keys.** Optional `Idempotency-Key` request header, recorded in the published commit's metadata in `_graph_commits`. On a retry, the server scans recent commits on the target branch for the key and returns the original `LoadOutput` instead of re-applying. Derived from the commit graph: no side table, no drift. This directly closes the retry-after-roll-forward double-apply window.
- **Export jobs.** `POST /exports {branch, at, types, format}` pins a snapshot, materializes Arrow IPC or Parquet part-files under a graph-owned export prefix on object storage, and returns `{job_id, parts: [presigned GET URLs], schema, expires_at}`; `GET /exports/{id}` re-reads job state. The pinned snapshot is protected from `cleanup` version GC for the job's TTL; expired job artifacts are reclaimed by a TTL sweep folded into the existing `cleanup` reconciler. Noun-resource routes (`/exports`, `/exports/{id}`) match the `/branches`–`/commits` style.
- **Optional Flight read adapter (explicitly out of scope for writes).** If a client ecosystem (ADBC etc.) demands it, `GetFlightInfo`/`DoGet` become a thin adapter over export jobs: tickets map to parts, extended-location URIs return the same presigned URLs. Write-side Flight is rejected in this RFC: DoPut is single-schema per stream, and Flight's multi-table answer (N DoPuts plus a "seal" action) requires server-held cross-call transaction state, which the deny-list bans; under single-publish atomicity the only legal `PutResult` is the final one, reducing DoPut to a request/response call that plain HTTP already provides.

### Compression

Compression is negotiated, never forced, because it trades CPU for bandwidth: a clear win when the client is far from the server (cross-region, on-prem to cloud), close to a wash when client, server, and object store share a region. Three layers, by phase:

- **Transport (`Content-Encoding` gzip/zstd), phase 1.** Works on any body, including the streamed NDJSON load. Text NDJSON compresses roughly 5-10x, and the middleware is standard on both client and server. This is the cheapest win and applies to the first thing built.
- **Arrow IPC buffer compression (LZ4_FRAME / ZSTD), phase 2.** The IPC format compresses record-batch buffers internally. It rides the IPC load and export path for free, and compresses better than gzipped NDJSON because the data is columnar (dictionary, run-length, and delta encoding on contiguous same-type values).
- **Parquet via load-by-reference, phase 2.** For genuinely large compressed data, the densest route places a Parquet (or compressed NDJSON/Arrow) artifact in object storage and points the server at it. No wire-compression machinery, and Parquet is smaller than Arrow IPC because it optimizes for storage rather than zero-copy speed.

The spectrum, cheapest to densest: gzipped NDJSON (phase 1) → Arrow IPC with a codec (phase 2) → Parquet via load-by-reference (phase 2).

### Large-scale loads: decouple, isolate, and the merge mechanic

A TB-scale load does not become a TB-scale transaction. The architecture already separates the expensive immutable data write (staged Lance fragments) from the cheap atomic visibility commit (one manifest flip), so the commit is O(touched tables), not O(rows), at any data size. This is the same shape Snowflake (`COPY` over immutable micro-partitions), BigQuery (load jobs, and the Storage Write API's independently-committed pending streams), Delta Lake (`_delta_log` put-if-absent), and Iceberg (snapshot-pointer CAS) all use: make the data write cheap to abort, make the commit a metadata flip. What makes a load "large" is memory, duration under contention, and crash recovery, and each has its own lever:

- **Memory** is bounded by incremental staging (above): stream batches to fragments, keep only the unique-key and node-ID hash sets resident. This is the only lever that touches the commit tail, and it is independent of the substrate.
- **Contention and the end-of-load abort risk** are removed by staging the load on its own branch. A load onto a private branch is a sole-writer workload: no competition for the per-(table, branch) write queue, no manifest-CAS conflict, no risk that a concurrent commit forces an hours-long load to rebase or abort at the very end. The cross-table RI write skew (unresolved Q7) also cannot occur during a sole-writer branch load, since it requires a concurrent node mutation; the RI check concentrates at merge instead of racing.
- **Crash recovery during the data write** is already all-or-nothing: an aborted load leaves unreferenced staged fragments that `cleanup` reclaims, never a partial commit. Resuming a partly-done load is a separate, missing capability (see the gap below).

Branches share the commit mechanism. The protocol is identical (stage, `commit_staged`, manifest flip, sidecar) with the branch as a parameter, and `main` is the canonical branch by convention, so branch-load keeps every per-commit guarantee intact. What it relaxes is contention and blast radius, and it converts the canonical-side publish into a merge:

- When `main` has not advanced since the fork, the merge fast-forwards: a metadata operation that adopts the branch's staged fragments by reference rather than rewriting rows, O(tables) not O(rows). (Observed today as `merged ... into main: fast_forward`.)
- When `main` has diverged, the merge is a real three-way merge whose cost scales with the divergence, and it is itself one atomic operation. So branch-load is a large win when loading into a relatively quiet `main` and a smaller win against a hot `main`, where the conflict cost relocates to the merge rather than disappearing.

This makes branch-load the documented pattern for large ingest: the hard part moves to a single, retryable merge instead of a long optimistic load racing live writers. The upstream primitive this pattern leans on is branch merge/rebase ([#7263](https://github.com/lance-format/lance/issues/7263)), which is a different and more relevant piece than multi-table transactions ([#7264](https://github.com/lance-format/lance/discussions/7264)); see Substrate trajectory.

**Resumability gap.** A load that dies partway restarts from zero; there is no load history or offset checkpoint, and the dead attempt's staged fragments are GC'd rather than resumed. The reference systems close this (Snowflake dedups already-loaded files; BigQuery commits write streams independently), and it is the one large-load capability that neither the current design nor the Lance MTT migration provides for free, since MTT's crash default is to discard the transaction branch. It is a phase-2 follow-up and sits outside the atomicity guarantee, which holds regardless. See unresolved question 9.

### Substrate trajectory: Lance native Multi-Table Transactions

Everything this RFC builds on in the commit tail (the `__manifest` row-level CAS via `ManifestBatchPublisher`, the `__recovery/{ulid}.json` sidecars, and the open-time recovery sweep) exists because Lance has no native multi-table atomic commit. [lance#7264](https://github.com/lance-format/lance/discussions/7264) ("Multi-Table Transactions for the Directory Catalog via Branching") proposes exactly that primitive, and the project's intent is to follow it and delete the omnigraph-side machinery. This RFC treats the commit tail as **interim** accordingly.

The Lance design: `BEGIN` allocates one transaction branch name; the first write to each table lazily shallow-clones a branch off its current tip; `COMMIT` repoints every involved table's catalog `location` to its branch tip in one atomic `__manifest` rewrite. Lost-update detection compares each table's `parent_version` at branch creation against its tip at commit; on advance it rebases the transaction branch onto the new tip (built on the branch-merge/rebase primitive [#7263](https://github.com/lance-format/lance/issues/7263) and UUID branch paths [#7185](https://github.com/lance-format/lance/issues/7185)). A leased, heartbeat-renewed **barrier commit** fences the fast path on the involved tables for the prepare+commit window, closing the time-of-check-to-time-of-use race that a catalog-only CAS cannot.

What this means for omnigraph, mapped to existing tracking:

- **The manifest-CAS + sidecar + sweep stack is removable** once MTT lands. This is the natural closure of the known gap that `invariants.md` and `AGENTS.md` already document ("multi-table commit = manifest CAS + recovery sidecars; *not* a single Lance primitive"). It is tracked by `iss-863` ("Track Lance namespace batch commit API and retire recovery sidecar when available"); that issue should be re-pointed at #7264 as the concrete upstream design. The per-table staging primitive (`stage_append` and its siblings) survives the swap; what MTT replaces is the multi-table coordination above it, since MTT itself stages each table before its atomic flip. So the loader and batch core, which call through the staging trait, are insulated from the change.
- **The cross-process write fence comes for free.** MTT's leased barrier is the proper version of the lease this RFC deliberately declines to hand-roll. The single-coordinator topology rule is the bridge until then.
- **Write skew does not go away.** MTT's stated isolation level is snapshot isolation with write skew *explicitly permitted* ("Disjoint concurrent transactions may both commit"). So the edge-RI write-skew anomaly (phase 2 / unresolved Q7, `iss-ri-write-skew-dangling-edges`) is an omnigraph application-level integrity concern that survives the migration; it is not a sidecar-era artifact and must be solved in omnigraph regardless of which substrate carries the commit.
- **MTT addresses commit coordination; large-load scale is separate.** Its scaling win keeps the catalog out of the per-commit hot path, which matters for commit *frequency* (many small transactions serializing on the catalog) rather than commit *size* (one large load is a single cheap commit either way, even under the removed `table_version_storage` approach). MTT replaces the cheapest part of a large load (the O(tables) flip) and leaves the expensive parts (fragment writes, validation, RI) untouched. The large-load levers all live above it (incremental staging, branch isolation, resumability) and are substrate-independent. So large-load work should not block on #7264.
- **Two MTT open questions bear on omnigraph directly.** Pointer-swap vs fast-forward-main (lance#7264 open question 1) decides whether a table's physical identity stays stable: pointer-swap means the catalog becomes the sole resolver and any omnigraph code that opens a dataset by physical path must re-resolve after every commit; fast-forward-main keeps identity stable at the cost of a per-transaction intent record (structurally the sidecar we already have). And the barrier's safety needs a generation/epoch, not a TTL alone, or a slow rebase that outruns the lease can still lose an update. Both are flagged in the upstream discussion.

Until #7264 ships, the interim machinery stays exactly as today; this RFC adds no new sidecar kind and no new writer that advances Lance HEAD before manifest publish.

A scope note to prevent a common confusion: Lance's MemTable/WAL (MemWAL) LSM is a separate substrate primitive on a different axis (single-table streaming write latency), not multi-table commit. It does not apply to bulk loads, which write large immutable fragments directly, and is tracked separately for hot-table write latency (`iss-681`). Do not conflate it with MTT.

### Error behavior

All new failure modes are typed and loud: budget exceeded (names the budget and observed value), unsupported media type (406/415 with the supported list), multipart part schema mismatch (names the table and field), idempotency-key replay (200 with the original output, flagged in the body), export job expiry (410 with the job id). Streamed-load failures before the publish leave the manifest untouched, as today; aborted streams surface the abort reason in the response trailer or the connection error, never a partial commit.

## Guarantees and known correctness gaps

What a load promises today, stated per mode so the contract is explicit:

- **Atomicity (all modes).** A load publishes one manifest commit; readers see all of it or none. Merge, append, and overwrite all run the same stage → `commit_staged` → manifest-flip tail. Two caveats: a fork-if-missing load is two commits (the branch creation publishes before the load, so a failed load leaves an empty created branch), and a crash after the per-table commits but before the manifest publish is rolled forward on the next open, so a client that saw an error may find the load applied.
- **Durability (all modes).** Lance fragments and the manifest are persisted before the response is sent. The roll-forward above is durability-preserving recovery, not loss.
- **Isolation (snapshot).** Readers hold one snapshot for a query; writers get first-committer-wins per table via the expected-version CAS. The gap is cross-table write skew on edge RI (below), which snapshot isolation permits and which survives the Lance MTT migration.
- **Consistency (validated at write, with mode-specific gaps).** Value, enum, intra-batch unique, edge-endpoint, and edge-cardinality checks run identically for all three modes before any commit. The remaining gaps are mode-specific and listed below.

Known correctness gaps on the load path, each tracked:

| Gap | Modes | Tracking |
|---|---|---|
| Merge replaces the whole matched row; omitted fields become null (silent data loss) | merge | `iss-986`, Q1 |
| Edge-RI write skew: a concurrent node mutation between validation and publish commits dangling edges | all | `iss-ri-write-skew-dangling-edges`, Q7 |
| Overwrite of a node table orphans committed edges in edge tables the load did not touch | overwrite | `iss-overwrite-orphans-committed-edges` |
| Append does not check incoming ids against committed rows, so it can create duplicate keys | append | `iss-714` |
| `@unique` / `@key` enforced intra-batch only, not against already-committed rows | all | `iss-714` |

These are application-level integrity concerns; none is fixed by the substrate. The Lance MTT migration permits write skew explicitly, and the rest are omnigraph's validation surface. The RFC's position is that the bulk data plane closes these as it formalizes the load contract rather than inheriting them silently: merge semantics (`iss-986`) gate streaming merge (Q1), the write-skew remedy gates incremental staging (Q7), and the remaining three are independent and can land on the current path.

## Invariants & deny-list check

- **§2 manifest-atomic visibility, §4 one publish boundary:** preserved by construction: every transport variant funnels into the unchanged `stage_* → commit_staged → sidecar → publish` tail. No per-table publishing is introduced.
- **§5 recovery coverage:** incremental staging introduces no new writer kind; the existing `SidecarKind::Load` coverage applies. The #203 queue-acquisition guard changes *when* the sweep acts on a live writer's sidecar, never *whether*; recovery still converges all-or-nothing. When Lance MTT replaces the sidecar stack, this invariant moves to the substrate (the leased barrier + atomic catalog flip).
- **§6 strong consistency / no early acks:** streamed loads return exactly one response after the publish. The resumable-write patterns of streaming protocols are deliberately unused.
- **§13 bounded and observable:** explicit per-load budgets replace an implicit transport cap; export jobs carry TTLs; all skips and refusals are typed.
- **Deny-list:** no cross-query transactions (multipart keeps multi-table loads inside one request); no acks before durable persistence; no state that drifts from the manifest (idempotency keys live in the commit graph, export GC derives from storage); no wire-protocol code in engine crates (decoders live in the server; the engine takes `RecordBatch` streams). Shipped observable behavior is treated as contract: `/ingest` and `POST /export` remain as deprecated aliases indefinitely, and new media types are versioned by content type.
- **Known Gaps:** the manifest→commit-graph two-write gap is unchanged (idempotency-key lookup tolerates a missing commit row by falling back to re-apply-safe merge semantics). The recovery-sweep liveness gap is now in-process-closed (#203) and cross-process-documented (topology rule), pending the Lance MTT barrier as the durable fix. The snapshot-isolation write skew on edge referential integrity predates this RFC and survives the MTT migration (MTT permits write skew); phase 2's incremental staging is gated on the remedy in unresolved question 7 so the gap is left no worse.

## Drawbacks & alternatives

**Costs.** A second body format on `/load` (raw NDJSON now, multipart IPC later) widens the test matrix; content negotiation on `/export` does the same. Export jobs add a small state surface (job manifests on object storage) and a GC obligation. The single-coordinator topology rule is an unenforced operational contract until the Lance MTT barrier lands.

**Alternatives rejected:**

- *Raise the 32 MB limit only.* Keeps the buffered double-encoded envelope; the ceiling moves rather than disappearing, and server memory scales with payload size.
- *Chunked multi-request CLI loads.* Breaks single-commit atomicity, the property this whole design exists to preserve.
- *Arrow Flight as the bulk transport.* Adds a gRPC server, duplicated auth, and an OpenAPI-invisible surface; its write primitive fights the multi-table atomic unit (see phase 2 notes) and its read-side strengths (multi-endpoint scatter, presigned extended locations) are captured here over plain HTTP. Reconsider read-side Flight as a thin adapter once export jobs exist and a concrete client demands it.
- *A lease/lock file for multi-writer safety.* New coordination state the substrate doesn't own, with its own staleness problem; the deny-list's "state that drifts from the manifest" applies. It is also redundant with the leased barrier Lance MTT ships natively. The #203 in-process guard plus the documented single-coordinator rule holds the line with zero new state until then.
- *Do nothing.* The 413 wall is already the effective ceiling for remote bulk loads, and the cross-process rollback hazard is a standing production risk.

## Reversibility

Mixed. Route names, headers, and CLI flags are cheap to revise behind the existing deprecation machinery. Media types, the multipart framing, the export-job manifest shape, and the `Idempotency-Key` semantics are wire contract, near-permanent once a client ships against them, which is why phase 2 items should land only when a consumer exists. Engine internals (batch core, incremental staging) are invisible at the boundary and freely reversible. The interim commit tail (manifest CAS + sidecars) is internal and is expected to be removed wholesale when Lance MTT lands, so it carries no reversibility cost; the wire contract above is independent of which substrate carries the commit.

## Unresolved questions

1. Merge-mode streaming: do repeated `stage_merge_insert` calls against one snapshot compose to the same result as one accumulated merge? If they conflict, does merge stay on the accumulate path permanently or fold in once Lance's staged surface allows it? Upstream of streaming, merge's row semantics need pinning first: the current upsert replaces the whole matched row, so fields omitted from the payload come out null. The batch core must define (and document) field-level vs row-level merge before streaming merge ships, or it inherits the data-loss behavior at higher volume.
2. Cross-process write safety in the interim window: is the documented single-coordinator topology rule sufficient until Lance MTT lands, or does the embedded CLI need an active refusal (e.g. detect a server-held marker and decline embedded writes) rather than only documentation? This trades against RFC-009's deliberate choice to keep the embedded path first-class.
3. Export job artifact default: Arrow IPC vs. Parquet (Parquet is friendlier to generic tooling; IPC is zero-conversion for Arrow clients). Possibly both behind `format`.
4. Multipart framing: standard `multipart/mixed` vs. a length-prefixed binary envelope (multipart is tooling-friendly; length-prefix is simpler to parse and harder to mis-buffer).
5. Whether `POST /load` should also accept the legacy JSON envelope for one release as a migration bridge, or whether `/ingest` covers that alone.
6. Idempotency-key scan depth (how far back in the commit graph a replay is detectable) and whether the key should also be exposed in `commit show`.
7. RI write-skew remedy for incremental staging: expected-map inclusion of RI-read node tables (loud conflict, false-positive cost) vs commit-time re-validation (extra scan under the write queues). Whichever lands should state whether it also retrofits the non-streaming load path, where the same anomaly exists with a shorter window. This is independent of the Lance MTT migration, since MTT permits write skew at the substrate.
8. Lance MTT coupling: should the batch core (`load_batches_as`) be written now in terms of a transaction abstraction that maps cleanly onto MTT's `begin/table/commit`, so the eventual substrate swap is a backend change rather than a loader rewrite? And does the pointer-swap vs fast-forward-main decision upstream (lance#7264 OQ1) change whether omnigraph can keep opening datasets by physical path anywhere?
9. Resumability for large loads: load-history dedup (Snowflake-style), offset checkpointing, or a resumable staging branch. Which model, does it live in the batch core or above it, and does it interact with merge-mode dedup? This is substrate-independent (MTT does not provide it) and is the one large-load capability not otherwise covered here.
