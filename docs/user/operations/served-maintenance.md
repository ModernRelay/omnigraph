# Served maintenance

A cluster server can run `optimize` on a graph's `main` itself, so a scheduler
or an operator can start it with one HTTP request and look the result up later
by an id it chose. The run uses the same engine as
[`omnigraph optimize`](maintenance.md#optimize) and publishes one graph commit.
Every request and every outcome is kept in a durable operation record, so a
caller that lost a response learns what happened instead of guessing.

The routes are off by default. A standalone server, which has no cluster
record, refuses them; direct `omnigraph optimize` stays available there.

## Enable it

| Variable | Default | Meaning |
|---|---|---|
| `OMNIGRAPH_MAINTENANCE_ROUTES` | `false` | `true` serves the routes; otherwise they answer 503. |
| `OMNIGRAPH_MAINTENANCE_GATE_HOLD_SECONDS` | `60` | The gate hold of a request that names none. |
| `OMNIGRAPH_MAINTENANCE_MAX_GATE_HOLD_SECONDS` | `300` | The largest gate hold a request may ask for. |
| `OMNIGRAPH_MAINTENANCE_ARTIFACT_WRITE_LIMIT_BYTES` | `536870912` | The largest adjacency artifact a served run writes. |

A value that does not parse, or a default above its ceiling, stops the server
at startup. Every route needs the [`maintain`](policy.md#actions) action on the
graph's `main` branch, for the actor the bearer token resolves to.

## Submit, look up, cancel

```bash
curl -X POST "$SERVER/graphs/knowledge/maintenance/optimize" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"maintenance_id": "01JC0000000000000000000000", "max_gate_hold_ms": 30000}'
curl "$SERVER/graphs/knowledge/maintenance/01JC0000000000000000000000" \
  -H "Authorization: Bearer $TOKEN"
curl -X POST "$SERVER/graphs/knowledge/maintenance/01JC0000000000000000000000/cancel" \
  -H "Authorization: Bearer $TOKEN"
```

`maintenance_id` is a canonical ULID the caller generates. The body also takes
`tables` (run only those table keys, in that order) and the per-table ceilings
`max_source_fragments`, `max_source_rows`, `max_source_bytes`,
`max_blob_bytes` and `max_index_rows`, and the run budget `run_source_bytes`;
see [bounding a run](maintenance.md#bounding-a-run).

A submission answers 202 with the record once it is durably accepted, and 200
once the operation is terminal. Submitting the same id with the same request
answers the existing record; it never starts the work again. A lookup answers
200 with the record, or `"lookup": "unknown"` when the record does not hold
the id. `unknown` does not prove the operation never ran, and it is never a
reason to resubmit the same work under the same id.

| Refusal | Status |
|---|---|
| The same id with a different request; an outstanding deployment; four unresolved publications; a record from another ledger at this root | 409 `conflict` |
| Another maintenance operation is active in the cluster (its id is in the message) | 429 `too_many_requests` |
| A gate hold above the server's ceiling | 413 `payload_too_large` |
| The routes are off, or the server is draining | 503 `service_unavailable` |
| A record write without a definite answer | 500 `outcome_unknown`; look the id up |

## States

An operation moves through `accepted`, `running` and `publishing` to one
terminal state:

- `succeeded`: it published a commit (`published_commit`), or found nothing to
  publish. A succeeded run may still have deferred or failed tables; read each
  table's outcome.
- `failed`, `cancelled` and `interrupted`: nothing was published, and the
  staged work is reclaimed by a later `cleanup`.

`publishing` means a commit may have landed. When the server cannot tell, the
operation stays `publishing` and leaves the active slot. Once that graph's
`main` has a newer commit, the next submission or lookup settles it from
`main`'s history: `succeeded` if the run's commit is there, otherwise
`failed`. An operation that was `accepted` or `running` when its server
stopped is `interrupted` at the next submission or lookup.

A full-catalog run that a gate hold or the byte budget stopped records the
table to start at; the next full-catalog submission for that graph starts
there, so successive bounded runs reach every table.

Cancellation stops the run at its next safe boundary. A run that already
started its publication finishes it and reports that the cancellation arrived
late. A shutdown and a deployment that affects the graph cancel the running
operation the same way, and the record names the reason.

The record keeps the 32 newest terminal operations; an older id answers
`unknown`.
