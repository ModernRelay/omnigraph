- Isolate read and write admission, retain request tracing across disconnect,
  and keep proven pre-effect storage refusals nonfatal. Uncertain writes close
  admission across the process and exit nonzero after other logical owners drain.
  Remote discovery reports credential-safe transport causes; dedicated merge
  commands canonicalize branch names before dispatch and receipt validation.
  Malformed branch-create names return HTTP 400 and leave admission open.

- **One v0.12 HTTP contract.** Protected graph and registry requests require
  `Omnigraph-Http-Api: 0.12`; missing or unsupported headers refuse before graph
  access with `api_contract_mismatch`. Upgrade CLI, server and HTTP integrations
  together. The CLI probes public health before each request and validates
  response headers before consuming JSON or streamed bodies, with redirects and
  automatic retries disabled. A failed preflight sends no data request; an
  incompatible response after dispatch leaves effects unknown. MCP and OAuth
  retain their separate standard protocols. Server URLs must identify the service
  root; replace graph-qualified URLs with the root and explicit graph selection. See
  [HTTP server guidance][v0-12-0-highlights-1].

- **Exact merge receipts.** Branch merge HTTP responses, GQ merge statements and
  CLI JSON now return the commit published by that merge, even when another
  writer advances the target before delivery. Fast-forward returns its own
  target commit; an already-up-to-date merge returns `commit: null`. Optional
  source deletion failure preserves the successful receipt and exit 0, with
  `branch_deleted: false` and structured `branch_delete_error_details` replacing
  the old string field. Embedded callers receive `MergeResult` instead of only
  `MergeOutcome`. See [merge outcomes][v0-12-0-highlights-2].

- **Typed edge selection (GQ 2.1).** `(knows | likes)` selects compatible named
  edge types and `*` selects compatible types from the captured schema. Every
  omitted bound means exactly `{1,1}`; explicit finite ranges combine shortest
  distances across types. Bound edges retain concrete type and identity through
  `$e.@type` and `$e.@id`. Common properties must have compatible types on every
  member. A shared finite `traversal_work_limit` fails explicitly on exhaustion.
  Historical wildcard targets and internal forced CSR selection are refused. Explain 4
  records complete member lists, directions, version maps and typed sort ties;
  serialized physical plans use a versioned envelope. Regenerate older saved
  plans, including those without traversal nodes. Scan admission charges full
  selected-table sizes, even for small neighborhoods. See
  [traversal and match patterns][v0-12-0-highlights-3].

- **Admitted writes outlive their HTTP callers.** The server owns mutations,
  loads and branch operations through completion, including optional merge-source
  deletion. Disconnect no longer frees their admission capacity. Aggregate input
  and operation limits bound retained server work; shutdown closes admission and
  waits for owned writes, read bodies and server producers against one deadline.
  An owner panic or indeterminate completion closes admission and stops the
  process. This does not qualify every native storage-I/O lifetime. See
  [deployment][v0-12-0-highlights-4].

- **Whole-command write outcomes.** CLI data-write errors include
  `command_outcome` with execution/effect evidence and a supported action,
  preserving structured errors and `Retry-After`. Exit 75 permits a bounded caller
  retry only for a verified typed admission refusal before the whole command's
  effects. Conflicts and uncertain delivery remain exit 1; verified HTTP
  conditional refusals retain exit 4. Embedded mismatches remain exit 1 because
  writable open may complete earlier work. The CLI never automatically repeats a write. See
  [failure outcomes][v0-12-0-highlights-5].

- **`cleanup` is a tracing collector over table storage, and `--keep`
  counts graph commits.** `--keep N` retains the newest `N` graph commits on
  every live branch and the table versions they pin; `--older-than` also
  retains commits newer than its cutoff. Branch creation bases, selected
  merge bases and exact snapshots protected by native tags are retained too.
  Other published versions are collected; unpublished staging is collected only
  after its recorded publication authority is provably gone. Missing or
  malformed ownership evidence is retained. Native tree candidates are frozen
  before graph capture; complete branch, head and tag views are revalidated
  after all table inventories. A change refuses the plan before deletion.
  Retirement archives exact branch-ref contents before unlinking active refs;
  cleanup reclaims unneeded trees and archives after checking dependencies.
  Branch creation does not scan accumulated retirement history. Accepted
  merge inputs remain protected through cancellation or an in-doubt
  publication until their target publication authority is provably gone.
  Ordinary writes and merges may overlap cleanup; branch creation/deletion
  and cleanup keep the existing single-writer-process control boundary. Dropped table lifetimes,
  base and overlay data files, borrowed origins and Blob sidecars participate
  in retention. Orphan objects and temporary manifests require their own
  seven-day age. Nothing defers a table: the `deferred` result field is removed,
  and each `--json` row carries `manifests_removed`, `unpublished_manifests`,
  `unpublished_bytes` and `foreign_versions` (`old_versions_removed` stays
  for one release with the value of `manifests_removed`). Exit 0 means every
  table was visited and no retained table version or file was removed; a table
  whose trace did not finish is left whole and its row says why.
- Lost acknowledgements resolve against the attempted graph commit even when
  another writer advances HEAD before readback. A publish that failed before
  any object-store request, with the attempted `__manifest` version absent and
  the branch head below it, returns the original error. A storage error stays
  in doubt, because its request may still land. The in-doubt error names the
  `graph_commit_id`.
- **`omnigraph repair` reports foreign drift instead of healing drift.** A
  graph table's Lance linear history ends at its creation version, recorded
  on the registration as `omnigraph.last_linear_version`. Linear commits
  above it came from outside OmniGraph and are resolved by no read or write;
  `repair` classifies such a table `foreign_drift`, prints the last linear
  version and the HEAD, takes no action and exits 0. `--confirm` and
  `--force --confirm` publish nothing on a v14 graph, and `cleanup` never
  deletes a foreign version.
- Inserting into a node type with a camelCase `@unique` property no longer
  fails with `No field named <lowercased name>`: the committed-uniqueness probe
  now reads the column by its exact name, for `mutate … insert` and for
  `append`/`merge` loads. Fixes #765.
- A property named like a SQL prefix keyword (`interval`, `exists`, `trim`)
  can now be the column of a `delete`/`update … where` predicate and of an
  indexed `Vector`: the predicate and the index-training filter reach the
  storage layer with the column quoted.
- **Correlated blocks in `match`: `exists { … }`, `count { … } > 2`,
  `sum($d.size) { … } > 100` (also `min`, `max`, `avg`).** A block holds a
  pattern matched once per outer row and keeps the row by a comparison on the
  aggregate of its matches; `not { … }` is the `count = 0` case of the same
  operator. The rows are narrowed before `order` and `limit`, so a paged
  listing filtered by a relationship count is exact. A binding declared inside
  a block (`not { $p worksAt $c; $c: Company { name: "Acme" } }`) is now
  reached through the block's traversal from the outer row instead of scanned
  as a whole table and cross-joined with the outer rows. A single-hop,
  filter-free `count { … }` over a directed, unbound
  edge is answered from the graph index's degree without reading the edge's
  rows. The bare `count($d) > 2` in `match` stays refused (T7). Fixes #764
  and the correlated-product half of #763.


- **Writes no longer use recovery sidecars, and the recovery machinery is
  gone (RFC 0067).** Every writer stages its table effects as detached Lance
  commits that nothing references until the one manifest commit publishes
  them, so an interrupted write leaves the graph unchanged and there is
  nothing to recover. The `__recovery` sidecars, the reopen-time recovery
  sweep with its roll-forward and rollback, the write-entry recovery barrier
  and the recovery audit table no longer exist. The failure classes they
  caused go with them: a stale or mis-named sidecar can no longer make every
  read-write open fail, a direct `optimize` beside a live server no longer
  leaves a barrier that refuses writes until a reopen, and a sidecar read can
  no longer race its own cleanup into a transient error. Fixes #602; removes
  the cause of #554, #601 and #330.
- **Optional OIDC identity and MCP access.** `omnigraph-server
  --oidc-identity-trust FILE` verifies resource-bound human identities locally
  against public keys and explicit principal admissions. Applied Cedar retains
  all graph permissions. Public admission updates have bounded expiry and do
  not restart serving. The option exposes standard protected-resource metadata
  and MCP tools for minimal discovery and stored reads, using the maintained
  protocol SDK and the existing authorization handlers. Direct deployments
  without the option retain their existing routes. See
  [OIDC resource identities and MCP][v0-12-0-highlights-6].

- **Managed login uses public AuthKit authentication and cached credentials.** Repeated `login --api`
  calls return fresh identity metadata without another browser flow while the
  session remains usable. The WorkOS SDK obtains and rotates credentials directly,
  without an embedded client secret or an API device/refresh broker, for
  up to eight hours from sign-in; each access credential remains short-lived.
  Metadata validation tolerates a bounded clock difference without extending
  either expiry timestamp.
  Credentials stay in the OS keychain, uncertain exchanges are never replayed,
  and logout clears local custody while separately reporting provider revocation.
  This profile requires an API supporting direct AuthKit access; old opaque
  sessions must sign in again. That service issues version-2 identity credentials
  only; retained `--actions` syntax remains an explicit restricted request for
  older compatible issuers and is never silently widened. Named-server login
  is unchanged. See the
  [managed CLI reference][v0-12-0-highlights-7].

- **Managed bulk loading uses the native CLI.** Implicit `load --graph`
  reuses the selected folder's data credential; applied policy supplies
  change and branch-creation permissions. Loads use the existing NDJSON
  endpoint with 32 MiB input, 8 MiB response and a 300-second deadline.
  Redirects and automatic retries are disabled; a lost response requires
  branch reconciliation. Explicit targets, positional storage URIs, profiles
  and `--direct` retain ordinary loading. See
  [managed bulk loading][v0-12-0-highlights-8].

- **Managed commit reads inspect native lineage.** `commit list` and
  `commit show` use the selected folder's identity and applied `read`
  permission to inspect graph history after a lost write response. They share
  the 30-second request deadline and never replay the uncertain write.

- **Managed queries and mutations allow 30 seconds to finish.** The total
  deadline includes connecting and reading the complete response, with at most
  10 seconds to connect. Redirects and automatic retries remain disabled, and
  responses remain limited to 8 MiB. A timed-out mutation may have committed;
  inspect the graph before resubmitting it. See
  [managed data access][v0-12-0-highlights-9].

- **CLI graph errors retain their structured JSON.** With `--json` or a read
  command's `--format json`, server refusals preserve their error code and
  typed details on stdout. Policy denials and resource limits exit 1;
  qualified HTTP conditional mutation mismatches retain exit 4. Data-write
  diagnostics additionally report the whole-command outcome described above.

- **Identity credentials leave permissions in applied policy.** Graph commands
  acquire and cache a cluster-bound identity credential with no graph or
  action grants. Applied Cedar policy decides protected graph and schema
  operations. Minimal graph discovery shows every effective graph ID/name
  without exposing schema, roots or other operational metadata. Explicit
  server addressing selects it with `graphs list --discovery`. The older
  restricted credential remains available only through explicit `--actions`
  requests. See [managed data access][v0-12-0-highlights-9].

- **Session settings.** A `.gq` file may open with `set <name> = <value>;`
  and `reset <name>;` lines, and `show <name>;` or `show all;` is a read
  statement returning `name`, `value`, `default`, `source` and `scope`. A
  value applies to that invocation or request and is never persisted. Seven
  settings: `engine`, `rrf_plan`, `merge_lineage`, `ann_nprobes`,
  `stage_write_concurrency`, `traversal_work_limit` and `history_release_bytes`.
  `history_release_bytes` is the byte budget of a branch's buffer of unreleased
  commits: a mutate, load or branch merge of the session that reaches it closes
  a history block, and the next publish writes the block under `__history`;
  production keeps the default, and a caller may only lower it. The CLI verbs `query`, `mutate`, `branch merge`,
  `commit changes`, `changes poll`, `load` and `ingest` take a repeatable
  `--set NAME=VALUE`;
  `POST /query`, `POST /mutate`, `POST /mutate/if-graph-commit` and
  `POST /branches/merge` take an optional `settings` object; `GET /changes`
  and `GET /commits/{id}/changes` take a repeatable `set=<name>=<value>`
  parameter. `omnigraph lint` reports a `set` line's error with its position.
  See [Session settings][v0-12-0-highlights-10].

- **Every read query runs from a plan (engine v2).** Engine v2 runs the plan as
  one DataFusion physical plan under omnigraph's own memory pool:
  DataFusion's operators for filter, join, projection, sort, limit and
  aggregate, omnigraph's own for the scan, the traversal, the negation and
  the rank fusion. Lance scans and graph hydration execute under the same
  query context, and shared Arrow buffers are charged once across custom
  operators. DataFusion operators that support spilling can use bounded
  scratch space; custom graph breakers return a typed resource-limit error
  when their memory reservation is refused. These query budgets do not bound
  shared caches or total process memory. Ordinary v2 reads build only the
  executable plan; explain JSON and structural hashes are generated only by
  explicit explain requests. `explain` describes the v2 plan the query runs
  and lists its lowered tree as `datafusion`
  rows when it can be built without running the query. The
  planner answers an unfiltered bare-variable count from the pinned dataset's
  live-row metadata through `aggregate_pushdown`, without scanning IDs or
  properties. Counts with filters, grouping, traversal or search retain their
  scans. The columns each remaining scan reads are decided before execution:
  the identity and key for a bare-variable count, the projected node
  object's members for `return { $var }`, and each property an expression
  names; a `Vector` or `Blob` column is read only when an expression names
  it, so a plain `count($s)` over a type with a `Vector` column no longer
  materializes every embedding (#704: 5.27 GiB peak RSS over 213,109 rows of
  `Vector(3072)` on the executor). Engine v2 is the engine for every read at
  every door: the embedded `Session`, the CLI, the server, stored queries
  and DST. The session setting `engine` keeps the one value `v2`, its
  default, so `set engine = v2;` is accepted and changes nothing. Engine v1,
  the executor, is retired from production: it is kept, frozen and
  hash-pinned, as the test reference `omnigraph-reference-engine`, a crate
  that is never published and that only the GQ logic tests depend on, where
  a query step's `--- expect same as v1` compares v2's rows with v1's.
  Traversal destination
  hydration also uses the planned projection before copying rows per edge,
  avoiding unused text and vector payloads in grouped counts (#703). GQT
  `expect plan` sections assert destination columns on the dependent scan.

- **Engine v2 decides the traversal mode in the planner.** The cost model
  that picks per-hop BTREE scans or the in-memory CSR for an unbound
  traversal moves from the executor into `omnigraph-planner`, decided at plan
  time from a row-count estimate (the pinned snapshot's entity counts,
  key-equality selectivity and the edge type's average fanout) and recorded on the
  physical `Expand` as `mode` and `frontier_estimate`; `explain` prints both
  and lists the pass `expand_mode`, and a GQ logic test asserts the mode with
  `expand $s Knows $d: mode indexed_scan`. The engine keeps two runtime
  corrections and can reconsider a CSR choice for a smaller observed first
  frontier. A degraded BTREE (or a CSR an earlier operator already built)
  re-runs the model before an indexed start, and the mid-flight switch on the
  observed frontier stays.

- **Engine v2 reads a traversal's destination table once when the frontier
  is dense.** The planner chooses an access path for every traversal
  destination: a `HashJoin` node reads the destination table once, with its
  pushed filters and projection, as the build side the traversal probes in
  its own order; a dependent scan (`access` `id_lookup`) keeps the per-batch
  `id IN (...)` read in slices of at most 256 input rows. The rule checks the
  table's row count against eight times the frontier estimate and requires
  its estimated projected bytes to fit one quarter of the query pool;
  `explain` prints the `HashJoin` node with its `fallback` and lists the pass
  `access_path`, and a GQ logic test asserts it with `hash join $d`.

- **`ann_nprobes` is a `request` setting.** A query sets it with
  `set ann_nprobes = N;` or the `settings` field of `POST /query`, and `show`
  prints its scope as `request`.

- **The `profile` row carries `attempts[].drained`.** It is `true` or `false`
  for an omnigraph operator (its stream reached its end or not) and `null`
  for a DataFusion operator, whose end no counter observes. An attempt's
  `actual_rows` is complete only when `drained` is `true`.

- **Engine v2 streams a single-hop traversal.** An unbound one-hop `Expand`
  no longer retains its frontier or its pairs: per input batch it walks the
  CSR adjacency into dense arrays and emits slices of at most 256 pairs,
  copying only those destination ids from the CSR dictionary; a
  bound-edge `Expand` runs its pair producer and sort per input batch.
  Rows, multiplicity and order are unchanged; `explain` prints
  `streaming=true` on the operator. The `HashJoin` node's operator charges
  its build and its output batches to the query pool (`hash join output`).
  Multi-hop traversal keeps the BFS breaker.

- **Engine v2 emits a multi-hop traversal's pairs a chunk at a time.** An
  unbound `knows{1,2}`-style `Expand` still drains its frontier before it
  emits, but its pairs now leave in batches of at most 256, each charged to
  the query pool on its own and released once hydrated, so the walk no longer
  retains every pair and a trailing `limit` ends it once the limit has its
  rows; a second multi-hop `Expand` between the two still drains the first
  one whole before it walks. A 1,024 → hub → 1,024 fixture (1,050,624 one-or-two-hop pairs) under
  `limit 1` refused with `resource limit exceeded for query_memory_bytes` and
  now answers. Rows, multiplicity and order are unchanged; `explain` still
  prints `streaming=false` on the operator, and its `expand_pairs` counter
  reports the pairs the walk handed on. Fixes
  [issue 782](https://github.com/ModernRelay/omnigraph/issues/782).

- **Engine v2 bounds bound-edge traversal before grouped aggregation.**
  Matched edges pass through bounded producer queues, native spillable ordering
  and incremental destination hydration. Counts and sums can consume expanded
  rows without retaining the complete expansion. Source-row and edge-identity
  ordering, parallel edges, destination filters and null amounts are preserved.
  The source frontier is still retained and charged to the query pool;
  unbound traversal keeps its existing execution path. Fixes the bound-edge
  materialization in [issue 723](https://github.com/ModernRelay/omnigraph/issues/723).

- **One expression language for filters, mutation `where`, `return`, `order`
  and assignments (RFC 2026-09-24, shared expression model).** Conditions
  compose with `and`, `or`, `not`, parentheses and `is null` / `is not null`,
  with three-valued null rules: a comparison with a null operand is null, a
  filter or a mutation `where` keeps the rows whose expression is true. A
  mutation `where` takes the same expression over the target's properties,
  `@id`, `@src`, `@dst`, parameters and `now()`, so `delete Knows where @src =
  "a" and @dst = "b"` removes exactly the intersected edges (issue 660) and
  `$p.name = "Alice" or $p.name = "Bob"` selects either (issue 651). A
  comparison or Boolean expression in `return` is a `Bool` column under its
  alias. An `order` key that is not a property or a system field binds to a
  return alias or to an expression written in `return`, so `order { count($d)
  desc }` sorts by the returned count (issue 566 part 3); a key that matches
  nothing is refused with `T42`. Assignment values and inline binding matches
  are constants over literals, parameters and `now()`, evaluated once per
  invocation. `GQ_LANGUAGE_VERSION` is 2.1. `LOGICAL_PLAN_VERSION` remains 2;
  logical `Filter` stores `conjuncts`. Explain version 3 added
  physical `ContainsJoin` and `CrossJoin` `filters`; `EXPLAIN_VERSION` is now 4.
  Physical `Expand` records all selected members and versions, and `Sort` and
  `RankFuse` record typed ties. Adding or removing a node kind bumps the explain
  version.

[v0-12-0-highlights-1]: ../docs/user/operations/server.md
[v0-12-0-highlights-2]: ../docs/user/branching/merge.md#outcomes
[v0-12-0-highlights-3]: ../docs/user/queries/traversal.md
[v0-12-0-highlights-4]: ../docs/user/deployment.md
[v0-12-0-highlights-5]: ../docs/user/operations/troubleshooting.md#failed-data-write-commands
[v0-12-0-highlights-6]: ../docs/user/operations/server.md#oidc-resource-identities-and-mcp
[v0-12-0-highlights-7]: ../docs/user/cli/reference.md#managed-cluster-commands
[v0-12-0-highlights-8]: ../docs/user/cli/managed-data.md#bulk-loading
[v0-12-0-highlights-9]: ../docs/user/cli/managed-data.md
[v0-12-0-highlights-10]: ../docs/user/queries/index.md#session-settings
