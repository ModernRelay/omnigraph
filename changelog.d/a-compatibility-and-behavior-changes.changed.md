- **Named traversal correctness.** Disconnected traversal patterns without an
  executable endpoint now fail type checking instead of disappearing from the
  query. Named reverse traversals in `not`, `exists` and block aggregates now
  use their checked direction and lexical expression scope. Later outer
  declarations no longer capture earlier block-local variables. Missing edge
  datasets on historical targets fail consistently when named, bound or
  explicit alternative traversals open them.

- **Writers stop serializing on the process-local schema gate (RFC
  2026-09-18, shared schema gate).** Mutations, loads, merges, index
  maintenance, optimize, cleanup, repair, branch delete, the system-column
  upgrade's `--check` and read-view captures take a shared permit on the
  schema gate; schema apply, the system-column upgrade, open, refresh,
  reload, `sync_branch` and branch create take it exclusive. A
  writer on one branch no longer waits for a writer on another, and a read
  no longer waits behind another handle's publish hold (a read and a write
  on the same handle and branch still wait for each other while it
  publishes); same-branch writers still serialize on the branch gate, and
  a schema apply still excludes every writer for its whole pass. No flag;
  per-operation object-store request counts on the success path are
  unchanged.
- **A branch merge finds what changed without requiring either side to
  descend from the merge base's table version.** With `merge_lineage` at
  `on` (the release default), candidate discovery compares the table
  versions the merge base, the source and the target pin by file identity.
  A merge whose side does not descend from the base's, such as merging
  main back into a branch after a three-way merge, now discovers
  candidates from version metadata where it fell back to the full
  three-way scan. A merge that succeeds produces the same result in every
  mode.
- **A write on a branch other than the handle's bound one reads that
  branch's `__manifest` once less.** Before its table effects the write
  compares the branch's latest version and identifier with the ones it
  captured; when both are unchanged it keeps the captured state and skips
  the reopen and scan. A branch that moved, or was deleted and recreated,
  is read again and the write is prepared again or refused as before. No
  flag; writes on the bound branch are unchanged.
- **Every table write is a detached Lance commit, published once and never
  promoted (RFC 0067 and RFC "Detached-only tables").** Each table effect
  is a detached commit of the pinned base, published as the pin
  `(published_dataset_version, staged_version, transaction_uuid)` in the one
  `__manifest` CAS, and that detached commit is the table's version for its
  whole life: readers open `staged_version`, nothing replays it onto the
  table's linear history, and a graph table's linear HEAD stays at its `v1`
  create. `published_dataset_version` keeps its number, `base + 1`, as the
  table's logical version and names no Lance version. Handle and topology
  cache identities include the detached pin, including on backends without
  e-tags. Persisted topology format v3 carries that pin; older artifacts are
  cache misses and rebuild. A mutation or load
  that fails before publication leaves the graph unchanged with nothing to
  recover, and one that publishes is complete at the CAS; a
  `RecoveryRequired` from a mutation or load, the `__recovery` sidecar it
  used to write, and the reopen-time recovery of that sidecar are gone. If
  the manifest CAS lands durably but its
  acknowledgement is lost, the write is already graph-visible, so the publisher
  reads the commit back and returns success rather than an opaque failure a
  non-idempotent retry could double-apply; only an outcome it genuinely cannot
  confirm surfaces as an in-doubt error. A failed operation never wedges its
  own live handle: once the fault source stops, the same handle's next write
  succeeds without reopening. Writes to registered tables do not read their
  linear HEAD, so a foreign linear commit does not block those writes;
  `omnigraph repair` reports it as `foreign_drift`. Schema apply checks an
  unregistered added-type path before reuse and requires its original empty
  version 1. `ensure_indices` and the explicit full-text rebuild
  follow: each index batch is a detached commit published as a pin, a
  rebuild interrupted after publication keeps serving search through the
  staged batch, and no `EnsureIndices` sidecar is written. Branch merge
  follows: a merge into a named branch chains every chunk of a merged table
  as detached commits and publishes one pin per table, a merge onto main is
  a pointer switch in which main's registration takes the source's pin with
  no row copy, and a merge that fails before publication leaves the target
  untouched; no `BranchMerge` sidecar or `RecoveryRequired` outcome remains.
  No per-table fork is created for a graph branch: a branch write stages
  detached on the dataset its inherited registration names, two branches
  writing one table produce two detached chains in one dataset, and forks
  created by earlier builds stay valid and are reclaimed by `cleanup` once
  unreferenced. Schema apply follows: existing-table rewrites are detached
  commits published as pins. An added type's original empty `v1` Create can
  be reused after a failed attempt; a different qualified original schema
  uses a detached replacement, preserving the path and its versions. The
  table delta and replacement schema-contract row publish together. No
  `SchemaApply` sidecar or root-contract installation remains. A failure
  before publication leaves the previous graph visible; a post-publication
  error identifies a complete published outcome, including its contract.
  Optimize follows: each table's compaction is one detached rewrite
  of its pin, a scalar or vector index whose coverage lags is rebuilt whole
  as a detached commit chained on it, the batch publishes once with an exact
  check on every pin it planned from, and no `Optimize` sidecar is written;
  a run that loses a table to a concurrent write fails with a read-set
  conflict and the next run re-plans. Optimize no longer strips a stale
  `lance.auto_cleanup` configuration from data tables: every engine commit
  skips Lance's auto-cleanup, so the key is inert. Every detached commit
  records the branch incarnation and graph head it was staged against in its
  transaction properties (`omnigraph.staged_against_branch_incarnation`,
  `omnigraph.staged_against_graph_head`), and a delete records the ids it
  removed (`omnigraph.deleted_ids`, or `omnigraph.deleted_ids_path` above
  64 KiB); the collector and the change feed read them. The optimization
  admits at most 16 MiB of encoded JSON; larger records take exact discovery
  without downloading an oversized spill. An external Lance
  reader pointed at a table directory opens the table's last linear version
  and sees stale rows; `omnigraph export` is the supported external route.
  Test seams `mutation.post_arm_pre_effect`, `mutation.post_sidecar_pre_fork`,
  `mutation.sidecar_confirm_put`, `mutation.sidecar_post_publish_delete`,
  `ensure_indices.post_sidecar_pre_fork`,
  `ensure_indices.post_effects_pre_confirm`, `branch_merge.post_sidecar_pre_fork`,
  `branch_merge.post_effects_pre_confirm`, `branch_merge.pre_error_recovery`
  and `optimize.post_compact_pre_reindex` are gone, and so are the promotion
  and fork windows that never shipped in a release
  (`mutation.post_publish_pre_promotion`,
  `ensure_indices.post_publish_pre_promotion`,
  `branch_merge.post_publish_pre_promotion`,
  `schema_apply.post_publish_pre_promotion`,
  `optimize.post_publish_pre_promotion`, `promotion.pre_replay`,
  `fork.post_create_pre_open`, `cleanup.pre_reap`, `cleanup.reap_delete`);
  `mutation.post_table_commit`, `publish.pre_merge`,
  `branch_merge.post_table_effect`, `optimize.post_table_effect`,
  `cleanup.collector_post_snapshot`, `cleanup.sweep_pre_manifest_delete`,
  `cleanup.sweep_between_manifests` and `cleanup.sweep_pre_file_delete` name
  the windows that exist, and the GQT cases that armed the retired seams are
  retired or repointed with them.

- **Graphs that still carry a recovery sidecar.** No 0.12 build reads or
  writes one. A read-write open refuses a graph
  whose `__recovery/` directory holds a sidecar, naming it, until the 0.11.x
  binary that wrote it has opened the graph read-write and finished its own
  recovery; a read-only open is unaffected. Refresh validates the published
  schema-contract row and updates the in-memory view; it does not install
  root schema files. A schema or system-column operation can still report
  `RecoveryRequired` after a proven publication, but the named commit already
  contains its tables and contract.
  Test seams `recovery.*`, `optimize.post_recovery_check_pre_main_gate`,
  `cleanup.post_recovery_check_pre_gates` and
  `system_column_upgrade.after_lock_reclaim` are gone, and
  `branch_control.post_recovery_barrier` is now `branch_control.pre_gates`.
  The GQ logic test format loses its `--- known_failure` section, which
  existed only to pin known recovery-sidecar defects.

- **Storage format v14; a standalone v8, v9 or v13 graph is converted in
  place, any other format is rebuilt.** Normal open serves v14 only and never
  migrates. Fresh graphs of either system-column vintage start at v14. A
  standalone graph at v8 or v9 (release 0.11.x) or at v13 is converted in
  place by the offline `omnigraph upgrade`, which keeps its branches, commit
  ids, commit history and table data. A graph at any other format, and every
  cluster-managed graph below v14, is refused, and the path is
  `omnigraph export` with the binary of the release that wrote the graph,
  then `omnigraph init` and `omnigraph load --mode overwrite`. That rebuild
  preserves data, vectors and blobs; commit history and branches are not
  kept.

  `omnigraph upgrade <graph>` converts a standalone v8, v9 or v13 graph to
  v14 offline: stop every process using the graph, retain a verified backup
  of the whole root, run `omnigraph upgrade <graph> --check`, which writes
  nothing, then `omnigraph upgrade <graph>`. It reports `check_passed` or
  `completed` for a convertible graph, `already_current` for a v14 graph, the
  finding `unsupported_source` for any other format below 14,
  `newer_than_binary` above 14, and `unsupported_target` for a `--to-format`
  other than 14. A graph carrying a pending conversion marker reports
  `recovery_required` and stays refused by ordinary open; run the same
  command again with the executable that started the conversion.
  Cluster-managed roots are refused. See the
  [upgrade guide][v0-12-0-compat-1].

  `omnigraph schema upgrade-system-columns` separately respells a served
  graph without changing v14. Its detached table renames and replacement
  contract publish together. An interruption before publication leaves the
  graph unchanged; afterward both contract and table references are complete.
  Test seam `schema_apply.post_sidecar_pre_effect` is now
  `schema_apply.post_lock_pre_effect`, and
  `system_column_upgrade.after_stamp_advance` is gone. The
  `V5 ↔ V9 Format Fence` CI job is now `V5 ↔ V10 Format Fence`.
  Historical queries retain current-contract semantics; older contract rows
  do not add a schema history API. Export with this binary followed by `init`
  and `load` with an older one is a separate logical rebuild; it loses history
  and branches.
- **A published commit id is `hb1.<block>.<slot>.<nonce>`.** Every commit
  after genesis is published under an id of that shape, so every `commit`,
  `snapshot` and `changes` output and every `/commits/{id}` path carries it;
  the genesis commit keeps its ULID, and on a graph converted by
  `omnigraph upgrade` the commits from before the conversion keep the ids
  they had. A prepared schema apply's commit is addressed by its published
  id: the intent id no longer resolves through
  `GET /commits/{id}`, and `RecoveryRequired.operation_id` names the
  published id.
- **Engine v1 is retired from production; engine v2 answers every read.**
  The session setting `engine` has the one value `v2`, its default:
  `set engine = v1;`, `--set engine=v1`, `{"settings": {"engine": "v1"}}` and
  `OMNIGRAPH_ENGINE=v1` are refused as an unknown value
  (``unknown value `v1` for setting `engine`; expected one of v2``), and the
  process default refuses startup like any other invalid setting value.
  Engine v1 survives only as the frozen test reference `omnigraph-reference-engine`, which no production
  door reaches. Three answers differ from v0.11.0, whose reads all ran on
  engine v1:
  - A `bm25()` or `nearest()` order on a traversal destination, a shape
    engine v1 answered, is refused with a bad-request user error that names
    the function and the binding, says engine v2 does not support it, and
    suggests ordering on the traversal's source binding or matching the
    destination with `search()`.
  - BM25 ties order by identity under a limit. Full-text candidates stay
    uncapped until the final score and identity ordering, so an equal-score
    entity outside an initial search window can still win, where engine v1
    cut the candidates first.
  - Several text predicates (`search`, `fuzzy`, `match_text`) on one scan
    are conjoined, where engine v1 applied only the last one.
- **Engine v2 no longer materializes the product of two unconnected bindings
  under a `contains` filter (issue 775).** A `$p.text contains $m.number`
  between bindings no traversal connects runs on their join, below any
  traversal, and, when the text binding's scan sits on the join's right
  (written second, or moved there when both row counts are known and the
  needles are no more than the texts), as a
  `ContainsJoin` that sieves the text scan through one Aho-Corasick automaton
  over the needles and pairs each text only with the needles it holds. Rows
  are unchanged; a query that refused under the memory cap now answers.
  `explain` shows the `ContainsJoin` node, the conjuncts a `CrossJoin` tests
  (`CrossJoin.filters`) and the scan's `runtime_filter`.

- **Search predicates:** `search`, `match_text`, and `fuzzy` filters accept
  a standalone call or `= true`. Other comparisons now fail type checking
  (`T38`) instead of being silently interpreted as a positive search. A
  text filter on a BM25-ranked binding is preserved by applying its
  membership set without changing the BM25 scoring query.

- **A refused query carries its diagnostic.** Every parse or type refusal
  now reports a stable code (`Q…`, `T…`), where the failure is (line and
  column, or the compiler stage), what was expected, and one fix. The HTTP
  `400` body gains an additive `diagnostic` object; the CLI prints the four
  fields on stderr for human formats and the API's error body for `--json`
  and `--format jsonl`, with no colour codes and no backtrace footer.
  `queries validate --json` and `cluster plan --json` carry the same detail
  beside their messages. A declaration without its parameter list, `query
  name {`, is refused at the name's end with the fix `query name()` where it
  was reported at the file's first position; the one-line `error` text of a
  parse refusal no longer embeds the parser's multi-line rendering. See
  [Diagnostics][v0-12-0-compat-2].

- **Setting environment variables are process defaults, and an invalid value
  refuses startup.** `OMNIGRAPH_RRF_PLAN`,
  `OMNIGRAPH_MERGE_LINEAGE`, `OMNIGRAPH_ANN_NPROBES` and
  `OMNIGRAPH_LOAD_CONCURRENCY` keep their names and meaning as the default
  of the setting each one seeds; they are read once, by the server at startup
  and by the CLI per run, no longer by the engine. A value outside the
  setting's row refuses the server start or the CLI invocation with the
  setting's message instead of warning and running a default:
  `OMNIGRAPH_LOAD_CONCURRENCY` must be in `1..=64` (`0` and `128` are
  refused), `OMNIGRAPH_MERGE_LINEAGE` must be `off`, `on` or `verify`.
  `OMNIGRAPH_TRAVERSAL_MODE` is no longer read: the expand path is no session
  setting, so nothing seeds it from the environment. A `process` setting
  (`rrf_plan`, `stage_write_concurrency`) is refused in a request's
  `settings` field, `set` parameter or text; a request may set
  `merge_lineage`, `ann_nprobes`, `traversal_work_limit` and
  `history_release_bytes`. `traversal_work_limit` defaults
  from `OMNIGRAPH_TRAVERSAL_WORK_LIMIT`. The GQ logic-test runner refuses to run
  while a settings-default variable or retired `OMNIGRAPH_TRAVERSAL_MODE` is set.

- **Malformed request JSON answers the API's own error body.** A body that
  does not parse on `POST /query`, `POST /mutate`,
  `POST /mutate/if-graph-commit`, `POST /branches/merge` or `POST /change` is
  refused with the JSON `ErrorOutput` body under the code `bad_request`,
  where earlier releases answered axum's bare extractor rejection.

- **Policy embedder source compatibility.** Cluster configuration authorization
  adds `PolicyAction::ConfigManage`, `PolicyResourceKind::Cluster`, and
  `PolicyEngineKind::Cluster`. Exhaustive matches on these public enums need
  the new variants. Existing static credentials, direct storage-holder APIs
  and version 1 token structs keep their behavior and construction contracts.
  An existing cluster needs an explicitly applied management policy before
  activating identity-authorized configuration execution; proposed policies
  cannot grant permission to install themselves.

- **GQ:** a new file-level statement, `explain` before a read declaration
  (`explain query q() { … }`), describes the v2 plan the query runs,
  instead of returning query results. Its table
  has one row per plan node (`tree`, `depth`, `node`, `detail`): the logical
  and physical trees, then the available DataFusion tree, each in pre-order,
  followed by `plan` rows for fired passes and document fields (`route`,
  `logical_hash`, …). Served by `omnigraph query` and
  `POST /query` under the query's own target, parameters and `read` policy
  decision; refused at `mutate`, at the deprecated routes, and over a mutation
  declaration. One statement per file, like the branch statements of RFC
  0055.

- **GQ logic tests:** a query step may carry an `--- expect plan` section
  after its `--- expect shape`, asserting facts of the query's v2 plan.
  Forms include `scan <Type>[ as $var]: columns [..]`,
  `scan <Type>[ as $var]: not columns [..]`, `scan <Type>[ as $var]: filter
  reads [..]`, `scan <Type>[ as $var]: no filter`, `filter reads [..]`,
  `pass <name>`, `not pass <name>`. The facts are matched against the
  engine's explain document, never against rendered text. See
  `crates/omnigraph-gqt/README.md`.

- **GQ logic tests:** a query step may end with `--- expect same as v1`,
  after its shape or plan section. The runner runs the query again on the
  frozen engine v1 (`omnigraph-reference-engine`) and fails the step on a v1
  error or a row difference; the step's own rows expect still applies. The
  section is refused on mutate, error-expect, `show` and `branch list`
  steps and skipped under DST. The `engine-v2` CI mode is gone, and
  `OMNIGRAPH_GQ_ENGINE` accepts only `v2` or empty.

- **GQ logic tests:** a `--- concurrent` section runs two to four labeled
  GQ statements at the same time on the case's one handle, under the DST
  runner, in the order its `order:` line names over store requests
  (`<label> [park] <verb> <key-suffix>`), session starts and completions;
  a `park` entry holds a session inside whatever the engine holds at that
  request. The block is one step, its bare `--- expect` names each
  session's outcome, a starved script fails with the entry it stopped at,
  and under `--measure` each session is its own cost row (RFC 0045
  §Concurrent block).

- **GQ logic tests:** several `--- seam` blocks may precede one mutate step
  when they name distinct seams; each is armed and recorded on its own, and
  the same seam twice before one step is refused.

- **GQ logic tests:** a fault injected at the object store is a `--- seam`,
  not a `--- fault` (RFC 0066, RFC 0045): `at` may name a store place from
  `STORE_PLACES` in `omnigraph-dst` (`storage.put` first) with a `subject`
  glob over the object's root-relative name, or a decision seam that declares
  a store effect; the site passes and the storage decoration acts on the next
  put. The DST target installs that decoration on every run. The delivery
  record carries the store's own hit (`method`, `requested`, `stored`). At most
  one store action precedes one step.
- **GQ logic tests:** the `--- fault` block that named a code hook is replaced
  by `--- seam`. Rename the section, keep `at`, `occurrence` and `scope`, and
  write `action: fail` (was `return_error`) or `action: skip`; a seam precedes
  a mutate step and must name a seam in the engine catalog with a matching
  effect. An old `--- fault` block is refused with that instruction. Delivery
  is proved by the runner's own decision (`seam_delivered`, `seam_unobserved`)
  instead of the injected error text. See `crates/omnigraph-gqt/README.md`.
- **Rust consumers of the engine's test seams:** the feature-gated
  `omnigraph::failpoints` module (`registry`, `set_registry`,
  `ScopedFailPoint`, `names`) and the `dst_clock`/`dst_ids`/`dst_gate`
  install and uninstall functions are removed. Seams are statics in
  `omnigraph::seams::catalog` armed through the `omnigraph-seams` crate
  (`SEAM.fire_always()`, `.fire_once_at(n)`, `.panic_at()`, `.hold()`,
  `.observe(f)`; `CLOCK.install(..)` and friends for the thread-local seams),
  each returning a guard that uninstalls on drop. `fail-parallel` is no longer
  a dependency.
- **Rust consumers of the engine workspace:** two new crates hold code moved
  out of the `omnigraph-engine` package with no behavior change:
  `omnigraph-core` (`OmniError`, Lance dataset access, request
  instrumentation, Lance native ref control (`branch_control`), dataset
  addressing) and `omnigraph-catalog` (the `__manifest` catalog: graph branch
  registrations and lineage, the publish). Both are internal API with no
  compatibility promise. The documented engine paths are unchanged:
  `omnigraph::db::{Snapshot, SnapshotDataset, SnapshotScanner}` (engine-owned
  types), `omnigraph::db::manifest::*`, `omnigraph::db::commit_graph::*`,
  `omnigraph::error::*`, `omnigraph::storage::*` and
  `omnigraph::instrumentation::*`. Code that was private or crate-private in
  the engine and is now called across the crate boundary is `pub` in
  `omnigraph-core` and `omnigraph-catalog` (about 440 items, among them the
  `ManifestCoordinator` commit and branch methods, `CommitGraph` in-memory
  methods, `OmniError` constructors and `DatasetEntry`/`DatasetUpdate`
  fields). The engine does not re-export `ManifestCoordinator` or the other
  catalog writers, so their methods are reachable only by depending on the
  internal crates directly. Items on types the engine does re-export are now
  callable through `omnigraph`: the `OmniError` constructors, the `CommitGraph`
  in-memory methods, `QueryMemoryProbes::record_refusal` and the
  `DatasetEntry`/`DatasetUpdate` fields. None of them writes to the object
  store.
- **GQ reserves `and`, `or`, `not`, `is`, `null` and `in`; GQ language 2.0.**
  Before upgrading, an operator checks every schema and every stored query for
  three spellings that no longer parse, since the server quarantines a graph
  whose stored query fails to parse at boot: a property named one of the six
  words written bare as an operand of a mutation `where` (`$p.and` after a dot
  in a read, and `and: 1` in an assignment or binding match, stay legal), a
  return alias spelled as one of them (`as and`), and an edge type named one of
  them traversed bare (`$a and $b` is now a Boolean filter; `not` was already
  excluded). A property named `true` or `false` cannot be named bare in a
  mutation `where` either: the parser reads the literal, and the type checker
  refuses the ambiguous spelling with `T46` instead of selecting rows by a
  constant. The fix for all four is a schema rename with `@rename_from`, which
  keeps the data, plus a rewrite of the stored queries that name the old
  spelling. Two grammar rules, `match_value` and `text_search_clause`, are
  deprecated in this release and removed in the next
  (`crates/omnigraph-compiler/compat/gq_language.deprecated.txt`).

[v0-12-0-compat-1]: ../docs/user/operations/upgrade.md#storage-upgrade
[v0-12-0-compat-2]: ../docs/user/queries/diagnostics.md
