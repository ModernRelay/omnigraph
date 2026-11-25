# Recorded operations

A cluster server can run `optimize` on a graph's `main` itself, so a scheduler
or an operator starts it with one HTTP request and looks the result up later
by an id it chose. The run uses the same engine as
[`omnigraph optimize`](maintenance.md#optimize) and publishes one graph commit.
Every request and every outcome is kept in the cluster's durable operation
record, so a caller that lost a response learns what happened instead of
guessing. `optimize` is the first recorded kind; lookup and cancel serve every
kind.

The routes are off by default. Without them, stop the server and run
`omnigraph optimize` on the cluster root directly.

## Enable it

| Variable | Default | Meaning |
|---|---|---|
| `OMNIGRAPH_OPERATION_ROUTES` | `false` | `true` serves the routes; otherwise they answer 503. |
| `OMNIGRAPH_OPERATION_RESOLUTION_WALK_LIMIT` | `256` | Commits one lookup or submission reads of a branch's history to settle a publication in doubt. |
| `OMNIGRAPH_OPTIMIZE_GATE_HOLD_SECONDS` | `60` | The gate hold of a request that names none. |
| `OMNIGRAPH_OPTIMIZE_MAX_GATE_HOLD_SECONDS` | `300` | The largest gate hold a request may ask for. |
| `OMNIGRAPH_OPTIMIZE_ARTIFACT_WRITE_LIMIT_BYTES` | `536870912` | The largest adjacency artifact a served run writes. |
| `OMNIGRAPH_OPTIMIZE_PLANNING_CONCURRENCY` | `8` | Tables a served run plans ahead of the one it rewrites; it rewrites one at a time. |
| `OMNIGRAPH_OPTIMIZE_MAX_SOURCE_FRAGMENTS`, `_MAX_SOURCE_ROWS`, `_MAX_SOURCE_BYTES`, `_MAX_BLOB_BYTES`, `_MAX_INDEX_ROWS` | unset | The per-table ceilings every served run applies ([table ceilings and the byte budget](#table-ceilings-and-the-byte-budget)). |
| `OMNIGRAPH_OPTIMIZE_RUN_SOURCE_BYTES` | unset | The run byte budget, and the table byte ceiling when `_MAX_SOURCE_BYTES` is unset. |

A value that does not parse, a default above its ceiling, or a table byte
ceiling above the run budget stops the server at startup. The ceilings are the
server's: a request names only its tables and how long writers wait. A table
whose plan passes a ceiling is `deferred` with reason `resource_limit`, and
nothing is written for it. A size the run could not read from storage while
measuring is not a deferral: that table is `failed` with the storage error,
and the run goes on to the next. Every
route needs the [`maintain`](policy.md#actions) action on the graph's `main`
branch, for the actor the bearer token resolves to.

### Table ceilings and the byte budget

A per-table ceiling is checked before any of that table's work starts. The
fragment, row, byte and blob ceilings are measured from the table's compaction
plan; `_MAX_INDEX_ROWS` counts the table's live rows, and only when the table
has index work. A table over one is `deferred` with reason `resource_limit`:
the run does not compact it, rebuild its lagging indexes or rewrite its dropped
properties, and goes on to the next table. This does not move the resume
position (`resume_at`), so every later run defers that table again until a
ceiling is raised or a direct run optimizes it. After a direct run, the next
write that inserts rows puts them in a fragment that the index on the table's
id does not cover, so the table has index work again. An update can do the
same, because it can move the updated rows to a new fragment: a `load --mode
merge` of existing keys always does. A table with more live rows than
`_MAX_INDEX_ROWS` is therefore deferred again after any such write, even one
that adds no rows, and while it gets writes only a direct run optimizes it.
The run byte budget defers, with reason `byte_budget`, the first table that
would take the run past it and every table after it, and the run resumes
there.

The budget is also the table byte ceiling when
`OMNIGRAPH_OPTIMIZE_MAX_SOURCE_BYTES` is unset. A table whose compaction reads
more bytes than the budget is therefore never optimized by a served run. To
optimize it, raise the budget, or stop the server and run `omnigraph optimize`
on the cluster root directly, which applies neither. Give that run
`--datasets <type key>` (for example `--datasets node:Document`) to optimize only
the oversized table, so the server restarts sooner. A direct run keeps the
root's admission lock: release it with `cluster force-unlock`, as after a
[direct apply](../clusters/index.md#bootstrap-a-cluster), before the server
starts again.

## Submit, look up, cancel

```bash
curl -X POST "$SERVER/graphs/knowledge/operations/optimize" \
  -H "Authorization: Bearer $TOKEN" -H 'Omnigraph-Http-Api: 0.14' \
  -H 'Content-Type: application/json' \
  -d '{"operation_id": "01JC0000000000000000000000", "max_gate_hold_ms": 30000}'
curl "$SERVER/graphs/knowledge/operations/01JC0000000000000000000000" \
  -H "Authorization: Bearer $TOKEN" -H 'Omnigraph-Http-Api: 0.14'
curl -X POST "$SERVER/graphs/knowledge/operations/01JC0000000000000000000000/cancel" \
  -H "Authorization: Bearer $TOKEN" -H 'Omnigraph-Http-Api: 0.14'
```

`operation_id` is a canonical ULID the caller generates. The body also takes
`datasets` (run only those table keys, in that order) and `max_gate_hold_ms`,
at or below the server's ceiling; nothing else.

A submission answers 202 with the record once it is durably accepted, and 200
once the operation is terminal. Submitting the same id with the same request
returns the existing record without starting work again. A lookup answers
200 with the record, or `"lookup": "unknown"` when the record does not hold
the id. `unknown` does not prove the operation never ran, and it is never a
reason to resubmit the same work under the same id.

A record that holds a `cancellation_request` is refused by a server older
than the release that added the field (409 `operation_record_invalid`), so
upgrade every server on a cluster together.

Earlier edge builds wrote `tables` and `more_tables`. If the cluster's
operation record contains either field, submit, lookup and cancel return
HTTP 409 with `operation_record_invalid` after upgrading. No copy of that
record is valid for this build, so restoring one does not help. To recover,
stop the server, move `__cluster/operations.json` out of the cluster root and
start the server again. The next submission creates a new record. The old
record's results, resume position and eviction mark are lost: an earlier
operation id answers `unknown` and is accepted as a new operation if it is
submitted again, and the next full-catalog run starts at the first table.

| Refusal | Status |
|---|---|
| A cancel of a `publishing` operation that no run on this server holds (`[operation_publication_in_doubt]`; nothing changed, look it up until it settles); the same id with a different request (message prefix `[operation_request_mismatch]`); an id at or before the record's eviction mark (`[operation_id_expired]`, mint a new one); an outstanding deployment; four unresolved publications; a record from another ledger at this root (`[operation_record_foreign]`); a cluster state file whose text is not valid cluster state (`[invalid_state_json]`) or an operation record whose text is not a valid record (`[operation_record_invalid]`; restore the object from a trusted copy, while bytes that are not text answer 500) | 409 `conflict` |
| Another operation is active in the maintenance scope (its id is in the message), or the scope is admitting one | 429 `too_many_requests`, with `Retry-After` |
| A gate hold above the server's ceiling, an `operation_id` that is not a canonical ULID, or one dated more than a minute ahead of the server's clock | 400 `bad_request` |
| The routes are off, the server is draining, the graph is closing for a deployment, a storage read of the cluster state file, its lock or the operation record failed temporarily or for an unknown reason (message prefix `[state_read_error]`, `[state_lock_read_error]` or `[operation_record_read_error]`; send the same request again), or the record write was refused and nothing was accepted (`effects: none`). A refusal that names an earlier request of unknown outcome clears once a read sees the record change; for an unconditional write, or a create whose record never appeared, it holds until the server restarts | 503 `service_unavailable` |
| A record write without a definite answer | 500 `outcome_unknown`; look the id up |
| A storage read of the cluster state file, its lock or the operation record failed in a way that repeats on every read: bytes that are not text, an object over its size bound, or access the store denies (same message prefixes; `effects: none`, nothing changed). Sending the same request again fails the same way until the object or the storage configuration is repaired | 500 `internal` |

## From the CLI

`omnigraph optimize --target <server>` runs the same route. It prints the id
it minted before it sends, waits for the terminal record, and after a lost
response looks the id up instead of submitting again. `--no-wait` returns
once the run is accepted, `--operation <id>` observes an existing operation,
and `--timeout` bounds the wait, not the operation. `--datasets` and
`--max-gate-hold` map to the request of a new operation and are refused beside
`--operation`, which observes what that operation was submitted with.
`--start-at` is refused: the server keeps the resume position. Either refusal
exits 1 before any request is sent.

| Outcome | Exit |
|---|---|
| `succeeded`, or accepted or observed with `--no-wait` | 0 |
| Refused before acceptance, or `failed`, `cancelled` or `interrupted` | 1 |
| Still `publishing` or running when the wait ended, `unsettled`, `unknown` after a lost response, a lookup refused after acceptance, or five lookups in a row without a definite answer | 2 |
| The same id with a different request | 4 |
| Another operation is active in the scope | 75 |

## States

An operation moves through `accepted`, `running` and `publishing` to one
terminal state:

- `succeeded`: it published a commit (`published_commit`), or found nothing to
  publish. A succeeded run may still have deferred or failed tables; read each
  table's outcome under `outcome.datasets`. An `optimized` table with
  `reason: index_failed` published the compaction or index work it completed
  before a later index step or coverage check failed. Its `detail` gives
  the error. The next run checks the published version for remaining work. A
  record lists at most 1,024 tables; `more_datasets` counts the rest per
  outcome and reason (`optimized:index_failed`).
- `failed`, `cancelled` and `interrupted`: nothing was published, and the
  staged work is reclaimed by a later `cleanup`.
- `unsettled`: the graph was deleted while the publication was in doubt
  (`reason: graph_deleted`), so whether it published is unknown and nothing
  can answer it any more.

`publishing` means a commit may have landed. When the server cannot tell, the
operation stays `publishing` and leaves the active slot. Once that graph's
`main` has a newer commit, the next submission or lookup settles it from
`main`'s history: `succeeded` if the run's commit is there, otherwise
`failed`. Each attempt reads one page of that history; a record whose walk is
still open shows `resolution` with the commit the next page starts at and how
many commits it has covered. An operation that was `accepted` or `running`
when its server stopped is `interrupted` at the next submission or lookup.

A full-catalog run that a gate hold or the byte budget stopped records the
table to start at; the next full-catalog submission for that graph starts
there, so successive bounded runs reach every table that fits the server's
per-table ceilings; a table over one never moves that position
([table ceilings](#table-ceilings-and-the-byte-budget)).

Cancellation stops the run at its next safe boundary. A cancel of an
operation this server is running is written to the operation's record as
`cancellation_request` (who asked, and when) before the run is told and
before the answer, 202. From then on every state of that record keeps the
request, whatever ends the run, and across a restart of the server. The
terminal state says whether it took effect:

- `cancelled`: the run stopped before its publication and published nothing.
- `succeeded`: the run had already started its publication, or had finished
  its tables with nothing to publish, and `reason` says the cancellation
  arrived late. A run still in its publication finishes it; an adjacency
  artifact refresh whose write has not started is skipped
  (`outcome.artifact` is `skipped` with detail `cancelled`). A publication
  that ended in doubt and settled from `main`'s history as `succeeded` has no
  such reason; the request is still on the record.
- `failed`, `interrupted` or `unsettled`: the run ended for its own reason
  first, which `reason` gives; `failed` and `interrupted` published nothing.

A cancel that arrives after the record is terminal answers 200 with the record
unchanged. A cancel of a `publishing` operation that no run on this server
holds, a publication in doubt, is refused with 409 and changes nothing: its
commit may still land, so nothing can stop it, and it settles as described
above. A cancel whose record write fails is answered as the refusal table
says, and the run is not told. A shutdown and a deployment that affects the
graph cancel the running operation the same way, without writing a
`cancellation_request`, and the record names the reason.

The record keeps the 32 newest terminal operations and the time of the newest
id it evicted: an older id answers `unknown`, and a submission under an id
minted at or before that time is refused as expired, so an evicted operation
never runs again. A retained id always answers its record first, whatever the
mark says. Because the mark is a caller's time, an id dated more than a minute
ahead of the server's clock is refused before it is accepted; otherwise one
such id, once evicted, would refuse every honest id after it.
