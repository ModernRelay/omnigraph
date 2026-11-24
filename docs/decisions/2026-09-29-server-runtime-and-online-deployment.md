# Server runtime and online deployment

**Decides:** one exclusive owner per cluster root, the stopped-cluster CLI or the running server, executes one captured deployment at a time through ledger v2 under an exact identity that is never replayed, and the server activates the result in place while unaffected graphs keep serving.
**Contract:** keeps E2, E4, E5, E6, write:7, reclaim:3
**Code:** `crates/omnigraph-cluster/src/deployment.rs`, `crates/omnigraph-cluster/src/deployment`, `crates/omnigraph-cluster/src/admission.rs`, `crates/omnigraph-cluster/src/lease.rs`, `crates/omnigraph-server/src/deployment.rs`, `crates/omnigraph-server/src/serving.rs`

## Context

A deployment changes schemas, stored queries, policies, bindings and the graph inventory. Each graph's schema publishes atomically, but no deployment across graphs is atomic, and a lost acknowledgement leaves an outcome unknown. A second process applying beside a serving one has no fence against the server's writes. A restart per change interrupts unrelated graphs, and a retry under a fresh identity can apply a change twice.

## Decision

- `__cluster/state.json` version 2 is the only operational ledger. There is no import, refresh, approval or sweep executor. A version 1 ledger is read only by stopped-writer conversion (`cluster upgrade-ledger --writers-stopped`), which preserves graphs and resources and fabricates no receipts.
- One exclusive admission covers the whole canonical root: the server for its lifetime, or one direct operation. Every supported writer, writable opener, cleanup and native control acquires it before opening a graph. Lock age or a timeout never grants takeover; an operator unlocks an exact lock id after quiescence. The S3 serving lease is [a separate decision](2026-10-19-storage-outcomes-and-restartable-ownership.md).
- One deployment is outstanding per cluster. A competing id refuses; there is no queue or supersession.
- A deployment id is `<ledger_id>:<sequence>:<nonce>`. Acceptance consumes exactly the next sequence. Resubmitting a full id with the same input returns the original record; another input refuses. A consumed sequence never runs again; an evicted result is `result_expired`, outcome unknown.
- Before the first effect, every schema intent is prepared, every query validated, and completion capacity reserved within fixed limits.
- Each graph advances `NotStarted`, `Started`, `Settled`. `Started` is durable before invocation. A schema intent may publish only manifest version `M + 1`; an uncertain intent is reconciled from evidence (committed, no-op, not published, or unknown) and never invoked again. A neutral lineage fence at `M + 1` can prove non-publication.
- Graphs converge independently; a partial result is a valid base for a correcting deployment.
- Removing a graph declaration deletes its exact managed root and all retained history. `Started` precedes any removal; only verified absence completes it. Accepted deletion never ends as refused.
- Creation reconciles only its exact prepared birth, never matching text. Adoption, root recreation and catalog repair refuse.
- Each graph's schema contract identity is held in the applied revision and compared before serving or changing it, so a recreated graph with identical text is refused.
- `POST /cluster/deployments` returns 202 after the acceptance CAS. The server's owned executor keeps preparation, execution and activation after the caller leaves. `POST /cluster/plan` observes without admission, a schema gate or a ledger write.
- Online activation closes only affected graphs, drains them, applies with the existing engines, then installs contracts, queries, bindings, management policy and inventory in one registry snapshot. A pre-effect refusal restores the predecessor under a fresh epoch; uncertainty keeps the graph closed.
- Applied and active are distinct: activation identity lives only in the registry snapshot.
- Authority is `storage_owner` or `authenticated_identity`. Identity callers need current `ConfigManage`, and graph `Read` and `SchemaApply` for schema effects and deletion; candidate policy never authorizes its own deployment.

## Rejected

- **Restart for every schema deployment:** interrupts unrelated graphs.
- **Refresh a handle or swap only queries:** establishes no coherent bindings and does not settle old users.
- **A separate apply process beside the server:** schema publication fences nothing for native controls, reclamation or deployment ownership.
- **Queue and supersede deployments:** ordering states with no requirement for concurrent submission.
- **A deployment service or job store:** an endpoint in the one writer process needs no second authority.

## Not built

- Bounded retry of transient graph startup failures. A graph that fails to open stays blocked until restart.
- Settlement of native accepted I/O and protected completion memory and local-I/O reserves, which generic engine disposal or reuse would require.
