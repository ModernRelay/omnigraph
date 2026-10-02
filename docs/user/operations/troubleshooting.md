# Troubleshooting

CLI failures include a human-readable message. Add `--json` where supported to
receive structured fields suitable for automation. Application-generated HTTP
errors use a JSON body with `error`, an optional broad `code`, and optional
details for conflicts, limits, Blob ranges, external sources, or recovery.
Router, method, media-type, malformed-body, and extractor rejections may be
plain responses, including 404, 405, 415, or 422.

Do not parse human-readable error text when a structured field is present.

## Server discovery

Remote graph commands first make an anonymous `HEAD /healthz` request to the
configured service root. Discovery sends neither the data bearer token nor URL
Basic-auth credentials; reverse proxies must allow that public probe. A timeout
or connection failure is reported with a credential-safe cause category and
means the data request was not sent. HTTP discovery refusals retain their status.

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
| 429 | Server or per-actor admission limit reached | Use the whole-command outcome below before retrying; preserve `Retry-After` |
| 500 | Server or stored-data integrity failure | Check server logs; do not assume partial success |
| 503 | Admission is closed, or a published schema change requires completion | Inspect the structured error; generic 503 is not permission to repeat a write |

A graph-head `412` includes `precondition_failure` with `expected` and, when
available, `actual`. A change-feed `410` includes `change_feed_gap`; retrying
the same cursor cannot recover its missing history.

A `400` that refuses a query includes `diagnostic`: the stable code (`Q…`
parse, `T…` typecheck), `position` (`line`, `column`, `byte`) for a parse
refusal or `stage` (and `expression` when known) for a later one, `expected`,
and `fix` when one exists. Act on the fix; a retry of the same source fails
the same way. See [Diagnostics](../queries/diagnostics.md).

## Failed data-write commands

CLI data-write failures preserve structured details and add `command_outcome`:
`execution` is `not_started` or `unknown`, `effects` is `none` or `unknown`, and
`action` is `retry`, `refresh`, `recover` or `reconcile`. JSON mode writes these
fields to stdout; human mode prints the action on stderr. HTTP failures also
retain `http_status` and, when present, the original `retry_after` string.

| CLI exit | What to do |
|---:|---|
| 0 | The operation succeeded or was a no-op. A merge can separately report a source-deletion failure without losing its successful result. |
| 4 | The server verified the requested write precondition failed with no earlier command effects. Re-read the branch and choose a new precondition deliberately. |
| 75 | The server's typed admission refusal proves this whole command did not start. Honor `Retry-After` and bound any caller retry. |
| 1 | Inspect the action and details. A conflict, resource limit, unavailable server, incomplete response or lost connection does not by itself permit repeating the write. |

`refresh` means inspect current inputs and correct the command; it does not grant
automatic replay. `recover` means finish the reported recovery obligation.
`reconcile` means establish the original operation's outcome before another
attempt. Only `retry` with exit 75 grants the bounded retry described above; the
CLI performs no automatic retry.

The outcome covers the entire command. A later refused subrequest cannot undo
an earlier branch creation, load or publication. Embedded load conflicts and
conditional mismatches remain exit 1: writable open can complete earlier work.
Disconnecting an admitted HTTP write does not cancel it: the server keeps
running the original operation, and a lost response can leave the caller's
outcome unknown. There is no durable request-result lookup in this release.

## Conflicts

A `409` is not one universal retry signal:

- A read-set or version conflict means another writer changed an input. Inspect
  the whole-command outcome and re-read before deciding on a new operation.
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
error is different: a schema change is already published and only its schema
files remain to be installed, so reopen read-write rather than retrying it.

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
- `external_blob_base_overlaps_storage_root`: an `external_blobs` base lies
  inside, or contains, the cluster storage root that holds every graph and the
  applied state. Move the base to a sibling prefix, run `cluster apply`, and
  restart the server; until then the server quarantines the graph, and fails
  to start with `cluster_no_healthy_graphs` if every applied graph is
  quarantined. See
  [External Blob references](../clusters/config.md#external-blob-references).

See [Operating a cluster](../clusters/index.md).

## Maintenance failures

- Recovery required: reopen the graph read-write or restart its server. A
  graph carrying a sidecar from a release before 0.12 must first be opened
  read-write with that release.
- Foreign drift: `repair` reports Lance commits above a table's last linear
  version as `foreign_drift`; no read or write uses them and no command
  adopts them (below).
- Cleanup row with `error`: the collector's trace of that table did not
  finish, so nothing of that table was deleted; fix the named cause and
  rerun. Other tables are collected and the command exits 0.
- Azure admission failure: inspect the lease owner before using the admission
  tool's break-glass flow.

### Foreign drift

`omnigraph repair` classifies a graph table as `foreign_drift` when its
Lance linear history carries commits above the table's recorded last linear
version (`omnigraph.last_linear_version` on the registration). Every
OmniGraph write is a detached commit that a graph commit pins, so such a
commit came from something else writing the table directory directly. The
condition is per table.

- Queries, mutations, loads, merges, index builds, schema apply, optimize
  and cleanup are unaffected: none of them resolves the linear HEAD.
- `repair` prints the last linear version, the HEAD and the count of foreign
  versions, takes no action and exits 0. `--confirm` and `--force --confirm`
  never adopt the foreign commit.
- `cleanup` never deletes a foreign version or its files; the table's result
  row lists them under `foreign_versions`.

There is no command to run before writing: the write path has no
precondition on a table's linear history. To discard the foreign commits,
export the graph and load it into a new one.

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
