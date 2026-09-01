# Troubleshooting

CLI failures include a human-readable message. Add `--json` where supported to
receive structured fields suitable for automation. Application-generated HTTP
errors use a JSON body with `error`, an optional broad `code`, and optional
details for conflicts, limits, Blob ranges, external sources, or recovery.
Router, method, media-type, malformed-body, and extractor rejections may be
plain responses, including 404, 405, 415, or 422.

Do not parse human-readable error text when a structured field is present.

## HTTP errors

| Status | Meaning | Usual action |
|---:|---|---|
| 400 | Invalid request, query, schema, configuration, or external-Blob policy | Correct the request; retrying unchanged will fail again |
| 401 | Missing or invalid bearer token | Supply a token configured by the server |
| 403 | The resolved actor is not authorized | Change policy or use an authorized identity |
| 404 | Graph, query, branch, entity, or route is unavailable | Check the name and applied cluster revision; stored-query denials may also appear as 404 |
| 409 | Concurrent change, duplicate ID, merge conflict, existing resource, or incompatible full-text index | Inspect structured details; not every conflict is retryable |
| 410 | Required change-feed history was reclaimed | Capture and durably install a new baseline, then resume from its terminal cursor |
| 412 | Blob entity-tag or graph-commit precondition failed | Refresh the Blob ETag, or re-read the branch and retry the mutation with its current graph commit |
| 413 | Request or operation exceeded a bounded resource limit | Split or reduce the operation using the reported limit |
| 416 | Blob byte range is outside the value | Use the returned length to choose a valid range |
| 424 | An allowed external Blob source could not be read | Restore source availability or correct its URI/credentials |
| 429 | Per-actor admission limit reached | Honor `Retry-After` and retry later |
| 500 | Server or stored-data integrity failure | Check server logs; do not assume partial success |
| 503 | An interrupted write requires recovery | Reopen read-write or restart the server, then retry |

A graph-head `412` includes `precondition_failure` with `expected` and, when
available, `actual`. A change-feed `410` includes `change_feed_gap`; retrying
the same cursor cannot recover its missing history.

## Conflicts

A `409` is not one universal retry signal:

- A read-set or version conflict means another writer changed an input. Start
  the operation again from a fresh read.
- A key conflict means strict insertion found an existing ID. Change the ID or
  use merge/upsert semantics; repeating the strict insert is not useful.
- A merge conflict requires an explicit resolution on one branch before
  merging again.
- A full-text incompatibility includes
  `full_text_index_rebuild_required: { "index": "…", "reason": "…" }` with
  `code: "conflict"`. This condition persists until an operator rebuilds the
  affected branch's indexes; do not automatically retry the same query. Follow
  the [full-text upgrade procedure](upgrade.md#full-text-index-upgrade).
- “Already initialized” means the target already contains a graph. Choose a new
  root or deliberately use the command's destructive option when appropriate.

Writes are atomic at the graph-commit boundary. A normal validation, conflict,
or limit error does not mean that a subset became visible. A recovery-required
error is different: durable effects may exist but remain hidden until recovery
finishes, so do not work around it with repair or cleanup.

## Storage-format mismatch

If a graph was written by a different storage-format generation, the binary
refuses to open it and names the required release line. Follow
[Upgrading](upgrade.md): export with a compatible old binary, then initialize
and load a new graph with the current binary.

Do not edit internal metadata or copy files from individual backing datasets
between graph roots.

## Cluster failures

- Run `cluster validate` before `plan` or `apply`.
- A blocked graph deletion needs an approval for the exact current plan.
- A stale lock may be removed only after proving no cluster operation is
  running and supplying the exact lock ID to `cluster force-unlock`.
- Directory boot reads `cluster.yaml` to resolve storage, but served graph,
  query, and policy resources come from applied state; apply changes and
  restart.
- By default one graph that cannot open is quarantined while healthy graphs
  serve. Use `--require-all-graphs` when partial startup is unacceptable.

See [Operating a cluster](../clusters/index.md).

## Maintenance failures

- Pending recovery: reopen the graph read-write or restart its server.
- Uncovered drift: preview with `repair`; publish only classifications you have
  verified.
- Cleanup refusal: resolve recovery/drift and verify all live branches before
  retrying.
- Azure admission failure: inspect the lease owner before using the admission
  tool's break-glass flow.

See [Maintenance](maintenance.md) and [Deployment](../deployment.md).

## Useful diagnostics

```bash
omnigraph version
omnigraph snapshot --store ./graph.omni --json
omnigraph commit list --store ./graph.omni --json
omnigraph cluster status --config ./company-brain --json
omnigraph repair ./graph.omni --json
```

For server requests, retain the HTTP status, response body, request path,
timestamp, and server logs. Redact bearer tokens and storage credentials before
sharing diagnostics.
