# Recorded operations

A cluster server can run `optimize` on a graph's `main` itself, so a scheduler
or an operator starts it with one HTTP request and looks the result up later
by an id it chose. The run uses the same engine as
[`omnigraph optimize`](maintenance.md#optimize) and publishes one graph commit.
Every request and every outcome is kept in the cluster's durable operation
record, so a caller that lost a response learns what happened instead of
guessing. `optimize` is the first recorded kind; lookup and cancel serve every
kind.

The routes are off by default. A standalone server, which has no cluster
record, refuses them; direct `omnigraph optimize` stays available there.

## Enable it

| Variable | Default | Meaning |
|---|---|---|
| `OMNIGRAPH_OPERATION_ROUTES` | `false` | `true` serves the routes; otherwise they answer 503. |
| `OMNIGRAPH_OPERATION_RESOLUTION_WALK_LIMIT` | `256` | Commits one lookup or submission reads of a branch's history to settle a publication in doubt. |
| `OMNIGRAPH_OPTIMIZE_GATE_HOLD_SECONDS` | `60` | The gate hold of a request that names none. |
| `OMNIGRAPH_OPTIMIZE_MAX_GATE_HOLD_SECONDS` | `300` | The largest gate hold a request may ask for. |
| `OMNIGRAPH_OPTIMIZE_ARTIFACT_WRITE_LIMIT_BYTES` | `536870912` | The largest adjacency artifact a served run writes. |
| `OMNIGRAPH_OPTIMIZE_PLANNING_CONCURRENCY` | `8` | Tables a served run plans ahead of the one it rewrites; it rewrites one at a time. |
| `OMNIGRAPH_OPTIMIZE_MAX_SOURCE_FRAGMENTS`, `_MAX_SOURCE_ROWS`, `_MAX_SOURCE_BYTES`, `_MAX_BLOB_BYTES`, `_MAX_INDEX_ROWS` | unset | The per-table ceilings every served run applies ([bounding a run](maintenance.md#bounding-a-run)). |
| `OMNIGRAPH_OPTIMIZE_RUN_SOURCE_BYTES` | unset | The run byte budget. |

A value that does not parse, a default above its ceiling, or a table byte
ceiling above the run budget stops the server at startup. The ceilings are the
server's: a request names only its tables and how long writers wait. Every
route needs the [`maintain`](policy.md#actions) action on the graph's `main`
branch, for the actor the bearer token resolves to.

## Submit, look up, cancel

```bash
curl -X POST "$SERVER/graphs/knowledge/operations/optimize" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"operation_id": "01JC0000000000000000000000", "max_gate_hold_ms": 30000}'
curl "$SERVER/graphs/knowledge/operations/01JC0000000000000000000000" \
  -H "Authorization: Bearer $TOKEN"
curl -X POST "$SERVER/graphs/knowledge/operations/01JC0000000000000000000000/cancel" \
  -H "Authorization: Bearer $TOKEN"
```

`operation_id` is a canonical ULID the caller generates. The body also takes
`tables` (run only those table keys, in that order) and `max_gate_hold_ms`,
at or below the server's ceiling; nothing else.

A submission answers 202 with the record once it is durably accepted, and 200
once the operation is terminal. Submitting the same id with the same request
answers the existing record; it never starts the work again. A lookup answers
200 with the record, or `"lookup": "unknown"` when the record does not hold
the id. `unknown` does not prove the operation never ran, and it is never a
reason to resubmit the same work under the same id.

| Refusal | Status |
|---|---|
| The same id with a different request (message prefix `[operation_request_mismatch]`); an id at or before the record's eviction mark (`[operation_id_expired]`, mint a new one); an outstanding deployment; four unresolved publications; a record from another ledger at this root (`[operation_record_foreign]`) | 409 `conflict` |
| Another operation is active in the maintenance scope (its id is in the message), or the scope is admitting one | 429 `too_many_requests`, with `Retry-After` |
| A gate hold above the server's ceiling, an `operation_id` that is not a canonical ULID, or one dated more than a minute ahead of the server's clock | 400 `bad_request` |
| The routes are off, or the server is draining | 503 `service_unavailable` |
| A record write without a definite answer | 500 `outcome_unknown`; look the id up |

## From the CLI

`omnigraph optimize --target <server>` runs the same route. It prints the id
it minted before it sends, waits for the terminal record, and after a lost
response looks the id up instead of submitting again. `--no-wait` returns
once the run is accepted, `--operation <id>` observes an existing operation,
and `--timeout` bounds the wait, not the operation. `--tables` and
`--max-gate-hold` map to the request.

| Outcome | Exit |
|---|---|
| `succeeded`, or accepted or observed with `--no-wait` | 0 |
| Refused before acceptance, or `failed`, `cancelled` or `interrupted` | 1 |
| Still `publishing` or running when the wait ended, `unsettled`, `unknown` after a lost response, or five lookups in a row without a definite answer | 2 |
| The same id with a different request | 4 |
| Another operation is active in the scope | 75 |

## States

An operation moves through `accepted`, `running` and `publishing` to one
terminal state:

- `succeeded`: it published a commit (`published_commit`), or found nothing to
  publish. A succeeded run may still have deferred or failed tables; read each
  table's outcome under `outcome`.
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
there, so successive bounded runs reach every table.

Cancellation stops the run at its next safe boundary. A run that already
started its publication finishes it and reports that the cancellation arrived
late. A shutdown and a deployment that affects the graph cancel the running
operation the same way, and the record names the reason.

The record keeps the 32 newest terminal operations and the time of the newest
id it evicted: an older id answers `unknown`, and a submission under an id
minted at or before that time is refused as expired, so an evicted operation
never runs again. A retained id always answers its record first, whatever the
mark says. Because the mark is a caller's time, an id dated more than a minute
ahead of the server's clock is refused before it is accepted; otherwise one
such id, once evicted, would refuse every honest id after it.
