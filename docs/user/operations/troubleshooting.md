# Troubleshooting

CLI failures include a human-readable message. Add `--json` where supported to
receive structured fields suitable for automation. Every HTTP error the server
answers, an unknown route, a method the route does not serve and a malformed
body, query string or path parameter included, uses a JSON body with `error`,
`code`, `disposition` and optional details for conflicts, limits, Blob ranges,
external sources, or recovery. A malformed request is `400`, a body without a
JSON content type `415`. MCP tool calls report the same body beside the HTTP
status ([MCP](mcp.md#results-and-errors)).

Do not parse human-readable error text when a structured field is present.

## Server discovery

Remote graph commands first make an anonymous `HEAD /healthz` request to the
configured service root. Discovery sends neither the data bearer token nor URL
Basic-auth credentials; reverse proxies must allow that public probe. A timeout
or connection failure is reported with a credential-safe cause category and
means the data request was not sent. HTTP discovery refusals retain their status.

## HTTP errors

Each `code` is sent under the statuses `ErrorCode` in `openapi.json` names
beside its meaning: one, except `branch_start_refusal`, which is `409`, or `410`
for `not_retained`.

| Status | Meaning | Usual action |
|---:|---|---|
| 400 | Invalid request, query, schema, configuration, or external-Blob policy, including an update that must carry a stored external Blob reference the policy does not admit | Correct the request; retrying unchanged will fail again. For a stored reference, assign that property in the same update (a new value or null), or admit its base in the policy |
| 401 | Missing or invalid bearer token | Supply a token configured by the server. The CLI sends a token only to the server it is keyed to; see [server credentials](../cli/index.md#use-a-server) |
| 403 | The resolved actor is not authorized | Change policy or use an authorized identity |
| 404 | Graph, query, branch, entity, or route is unavailable | Check the name and applied cluster revision; stored-query denials may also appear as 404 |
| 405 | The path is served, but not with this method (`method_not_allowed`) | Use a method the `Allow` header lists |
| 408 | The request body did not arrive before its deadline (`request_timeout`) | Send the request again; nothing was admitted |
| 409 | Concurrent change, duplicate ID, merge conflict, existing resource, incompatible full-text index, or a commit no branch can start at (`branch_start_refusal`) | Inspect structured details; not every conflict is retryable |
| 410 | Required change-feed history was reclaimed, or the commit a branch start names (`branch_start_refusal`, `not_retained`) | Capture and durably install a new baseline, then resume from its terminal cursor; start the branch at a retained commit |
| 412 | Blob entity-tag or graph-commit precondition failed (`precondition_failed`), or `actor_mismatch` | Refresh the Blob ETag (a Blob write's `blob_precondition_failure` names the current one), or re-read the branch and retry the mutation with its current graph commit |
| 413 | Request or operation exceeded a bounded resource limit (`payload_too_large`) | Split or reduce the operation using the reported limit. Blob limits are listed in [Blob limits](../blobs.md#limits) |
| 415 | The body's content type is not one the route accepts (`unsupported_media_type`) | Send JSON with `Content-Type: application/json`, or the type the route names |
| 416 | Blob byte range is outside the value (`range_not_satisfiable`) | Use the returned length to choose a valid range |
| 424 | An allowed external Blob source could not be read | Restore source availability or correct its URI/credentials |
| 429 | Server or per-actor admission limit reached | Use the whole-command disposition below before retrying; preserve `Retry-After` |
| 500 | Server or stored-data integrity failure | Check server logs; do not assume partial success. A Blob delivery, change-route or streamed-response failure logs its error class (`error_variant`, and `storage_kind` for a storage failure), never its storage path; see below |
| 503 | Admission is closed or the graph is not served (`service_unavailable`, `graph_unavailable`, both with `Retry-After`), or a published schema change requires completion (`recovery_required`) | Inspect the structured error; generic 503 is not permission to repeat a write |

A `GET` or `HEAD /blob` 500 logs `error_kind="blob_pre_header_internal"` with a
`stage`. `target` is a failure resolving a snapshot target for a policy-gated
request. `cell` is any failure of the engine's Blob read, including the branch
or snapshot resolution the engine does itself. `transport` is the server's own
refusal before headers, logged with `error_variant="unclassified"`. A managed
Blob body that fails after the headers logs the byte range under one of
`blob_payload_read` (with the error class), `blob_payload_short_read` (with
the returned and expected byte counts) or `blob_payload_permit_closed`.

A change route (`GET /changes`, `POST /changes/baseline`,
`GET /commits/{commit_id}/changes`) answers an internal failure with a fixed
500 and logs `error_kind="change_route_internal"` with `error_variant`,
`storage_kind` and `manifest_kind`. A change route that requires recovery logs
the `operation_id` the 503 reports. An export or change-baseline response that
fails after its 200 headers ends with an incomplete body and logs
`error_kind="served_stream_failed"` with `stream` (`export` or
`change_baseline`) and the same error class. None of these logs carries the
error's message, which can hold object URIs or credentials.

A graph-head `412` includes `precondition_failure` with `expected` and, when
available, `actual`. A change-feed `410` includes `change_feed_gap`; retrying
the same cursor cannot recover its missing history.

A `400` that refuses a query includes `diagnostic`: the stable code (`Q…`
parse, `T…` typecheck), `position` (`line`, `column`, `byte`) for a parse
refusal or `stage` (and `expression` when known) for a later one, `expected`,
and `fix` when one exists. Act on the fix; a retry of the same source fails
the same way. See [Diagnostics](../queries/diagnostics.md).

## Failed data-write commands

Every server error body, and every CLI data-write and read failure, carries a
`disposition`: `effects` is `none`, `partial` or `unknown`, and `action` is
`fix`, `refresh`, `retry`, `recover` or `reconcile`. The server decides it for
the request it answered by the first row that matches:

| Error | `effects` | `action` |
|---|---|---|
| Any error before the request submitted a write | `none` | the refusal's own |
| A write whose completion the server could not settle; `code` becomes `outcome_unknown` unless the action is `recover` | `unknown` | `reconcile`, or `recover` |
| A refusal before the write started (`401`, `403`, `412`, `429`, `api_contract_mismatch`, `actor_mismatch`), a typed refusal of its one publication (a merge, key, read-set or dataset-version conflict, a resource limit, an external Blob source, a query diagnostic), or a failure the engine proved published nothing, such as a storage failure before publication; `code` is kept | `none`, or `partial` when an earlier step of the write published, such as a load that forked its missing branch | the refusal's own |
| Any other error after a write, which may follow its publication; `code` is kept | `unknown` | `reconcile`, or `recover` |

A refusal's own action follows its code: `recover` for a recovery intent or a
full-text index to build, `retry` for `too_many_requests`,
`service_unavailable`, `graph_unavailable` and `request_timeout`, `fix` for
`unauthorized`, `bad_request`, `api_contract_mismatch`, `actor_mismatch`,
`not_found`, `method_not_allowed`, `unattached_cannot_publish`,
`branch_start_refusal`, `payload_too_large`, `unsupported_media_type` and
`range_not_satisfiable`, and
`refresh` for the rest: `forbidden`, `precondition_failed`, `conflict`,
`external_blob_source`, `change_feed_gap` and `internal`. A write the server
could not settle answers `500`.

The CLI reports the server's disposition when that request was the command's
only effectful one, and otherwise its own account of the whole command, so a
lost response is `unknown` with `reconcile`. A read command changes nothing, so
its failure always has `effects: none`. JSON mode writes the failure to stdout;
human mode prints the `disposition` line on stderr. HTTP failures also retain
`http_status` and, when present, the original `retry_after` string.

This includes `commit publish` and `commit drop`, as well as their `mutate`
statement forms. If a response or result output is lost, inspect the commit's
current state before another attempt; an output failure does not undo the write.
The same applies to `load`, `blob put`, `blob clear` and `schema apply`: a closed
output pipe or failed result flush can follow a successful write. The CLI exits with a failure
and does not repeat the operation. If stdout cannot carry a JSON failure, stderr
reports the undelivered diagnostic and its disposition, and the exit still
follows it.

For the data commands the CLI classifies (queries, reads, `load`, `mutate`,
branch, commit, schema and Blob commands, aliases and target commands), the
exit follows from the disposition:

| CLI exit | What to do |
|---:|---|
| 0 | The operation succeeded or was a no-op. A merge can separately report a source-deletion failure without losing its successful result. |
| 1 | `effects: none`: the command changed nothing. Inspect the action and details, correct the cause and run it again. |
| 2 | `effects: unknown` or `partial`: the command may have had effects, or an earlier step of it did. Read the target, and reconcile or recover as `action` says, before another attempt; a conflict, resource limit, incomplete response or lost connection does not by itself permit repeating the write. |
| 4 | A condition you set did not hold, with no earlier command effects: a verified write precondition or an `--as` assertion. Re-read the branch and choose a new precondition deliberately. |
| 75 | `effects: none` with `retry`: the server refused before starting, as when the actor is busy. Honor `Retry-After` and bound any caller retry. |
| 64 | The argument parser refused the command line before anything ran (`EX_USAGE`); an addressing refusal inside a command, such as two scope flags, exits 1. |

Maintenance commands (`init`, `upgrade`, `optimize`, `rebuild-full-text-indexes`,
`repair`, `cleanup`, `schema upgrade-system-columns`), `cluster`, `policy` and the
managed commands keep their own exits for now: their exit 1 does not by itself
say that nothing changed, so read their report.

A served write refused with `401` `unauthorized`, `403` `forbidden` or `400`
`api_contract_mismatch` exits 1 with `effects: none`, and `action: refresh` for
the policy denial and `fix` for the others. The server answers these before it admits the write:
bearer authentication, then the API contract header, then policy. Fix the
credential, the policy grant or the client before running the command again.
When the CLI sent no credential, the refusal also carries `missing_credential`:
`server` names a declared server with no stored token, or `unbound` names an
address under no declared server, and `hint` says how to bind one. The server's
`error` text is unchanged.
The status counts only with its matching `code` in a response that carries the
server's API contract header: a bare `403` from a proxy stays `unknown` with
`reconcile`, and a refusal that also reports recovery work stays `recover`.

`fix` means correct the credential, input, path or client; sent unchanged, the
command fails the same way. `refresh` means read the current state and decide
again; it does not grant automatic replay. `partial` effects mean an earlier
step stands although the failing one wrote nothing; read the target before
deciding. `recover` means finish the reported recovery obligation.
`reconcile` means establish the original operation's outcome before another
attempt. Only `retry` with exit 75 grants the bounded retry described above; the
CLI performs no automatic retry.

The disposition covers the entire command. A later refused subrequest cannot
undo an earlier branch creation, load or publication. An embedded write's own
refusal says what the engine did, projected as a server would project it: a
policy refusal precedes every publication, a `load --from` fork included, so it
has `effects: none` on both planes. The engine marks a refusal that follows a
fork which did publish, and that refusal has `effects: partial` on both planes.
A `load --from` onto a branch that already exists forks nothing. Any later
failure, such as rendering a committed receipt, stays `unknown`.
Disconnecting an admitted HTTP write does not cancel it: the server keeps
running the original operation, and a lost response can leave the caller's
outcome unknown. There is no durable request-result lookup in this release.

## Conflicts

A `409` is not one universal retry signal:

- A read-set or version conflict means another writer changed an input. Inspect
  the whole-command disposition and re-read before deciding on a new operation.
- A key conflict means strict insertion found an existing ID. Change the ID or
  use merge/upsert semantics; repeating the strict insert is not useful.
- A merge conflict requires an explicit resolution on one branch before
  merging again.
- A full-text incompatibility includes
  `full_text_index_rebuild_required: { "index": "…", "reason": "…" }` with
  `code: "conflict"`. This condition persists until an operator rebuilds the
  affected branch's indexes; do not automatically retry the same query. Follow
  the [full-text upgrade procedure](upgrade.md#full-text-index-upgrade).
- A full-text index with no part at the query's snapshot includes
  `full_text_index_required: { "index": "Type.property" }` with
  `code: "conflict"`. It persists until the branch's index is built
  (`omnigraph optimize` on `main`, `omnigraph rebuild-full-text-indexes
  --branch <branch>` elsewhere); do not retry the same query. See
  [Search indexes](../search/index.md#indexes).
- “Already initialized” means the target already contains a graph. Choose a new
  root or deliberately use the command's destructive option when appropriate.

Writes are atomic at the graph-commit boundary. A normal validation, conflict,
or limit error does not mean that a subset became visible. A recovery-required
error is different: a schema change is already published and only its schema
files remain to be installed, so reopen read-write rather than retrying it.

## Storage-format mismatch

If a graph was written by a different storage-format generation, the binary
refuses to open it and names the required release line. Follow the qualified
explicit routes in [Upgrading](upgrade.md) to preserve graph identity and history;
normal open does not migrate or reset the graph.

Do not edit internal metadata or copy files from individual backing datasets
between graph roots.

## Cluster failures

- Run `cluster validate` before `plan` or `apply`.
- Apply refuses existing unmanaged roots and missing managed graphs that remain
  declared. Restore authoritative data to keep a missing graph, or remove its
  declaration to delete it; see [deployment boundaries](../clusters/index.md#deployment-boundaries).
- A retained lock requires prior-owner and accepted-I/O quiescence, exclusion
  of other admissions/unlocks, and its exact ID; follow
  [ownership transfer](../deployment.md#writer-topology).
- Directory boot reads `cluster.yaml` to resolve storage, but served graph,
  query, and policy resources come from applied state. Submit configuration
  changes with `cluster apply --server` for live activation.
- By default one graph that cannot open is quarantined while healthy graphs
  serve. Use `--require-all-graphs` when partial startup is unacceptable.
- `external_blob_base_overlaps_storage_root`: an `external_blobs` base lies
  inside, or contains, the cluster storage root that holds every graph and the
  applied state. For a new declaration, move the base to a sibling prefix before
  applying. Correct existing external-Blob bindings in the desired configuration
  and apply them. An invalid applied binding keeps its graph blocked;
  if every graph is blocked, startup fails with `cluster_no_healthy_graphs`. See
  [External Blob references](../clusters/config.md#external-blob-references).
- `external_blob_storage_root_uncomparable`: the cluster storage root is
  spelled with a path component an external Blob base cannot express (an empty
  component, or a percent sign in a local path), so a base of the same storage
  kind cannot be proven to lie outside it. Moving the base does not help:
  correct the declaration or remove the allow-list before applying. Graph roots
  remain fixed. Validation refuses the base, and an invalid applied
  binding keeps the graph blocked, as for an overlap.

See [Operating a cluster](../clusters/index.md).

## Maintenance failures

If `optimize`, `repair`, `cleanup`, `rebuild-full-text-indexes` or `upgrade` cannot
write or flush its output, the CLI returns a failure without repeating the
operation. An output failure does not undo completed work. Inspect the graph's
state before another attempt; for an interrupted upgrade, follow the
[upgrade recovery procedure](upgrade.md).

- Recovery required: reopen the graph read-write or restart its server. A
  graph carrying a sidecar from a release before 0.12 must first be opened
  read-write with that release (`omnigraph snapshot <graph>` is such an open).
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
  versions, takes no action and exits 0. The removed confirmation flags are rejected; repair never adopts the foreign commit.
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
