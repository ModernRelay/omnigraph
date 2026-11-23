# Session settings

**Decides:** every engine knob a caller can choose is a row of one `const` definition table in `omnigraph-compiler`, set per file or request with GQ `set`/`reset`/`show` or the doors' equivalents, carried on a `Session` value, and read by the engine from that session and nowhere else.
**Contract:** keeps E5, E7, read:4
**Code:** `crates/omnigraph-compiler/src/settings.rs`, `crates/omnigraph/src/session.rs`, `crates/omnigraph-api-types/src/lib.rs`, `crates/omnigraph-cluster/src/config.rs`, `crates/omnigraph-cli/src/client.rs`

## Context

An engine knob (the merge lineage mode, the staged-write width, the vector
probe cap, the fusion plan) read from the process environment or a test
task-local is invisible in every signature, and each such read grows its own
fallback rule. An environment variable is per process, so a server cannot let
one request choose differently from another, and a `.gqt` case cannot say in
GQ what it needs. A list of knobs known partly to the engine, the docs, the
CLI and the server has nothing holding the parts equal.

## Decision

- `DEFINITIONS` is the one list: name, kind (enum, bounded integer or text),
  default, scope, environment variable and description. `SettingId` has one
  variant and `SessionSettings` one field per row, and `SettingsRequest` one
  field per `request` row; unit tests hold them equal.
- A file may begin with `set NAME = VALUE;` and `reset NAME|all;`, and may be
  `show NAME|all;`. These are legal only before the first statement, so no
  existing file changes meaning. The parser checks names, kinds and ranges, so
  `omnigraph lint` reports errors offline with a position.
- A prefix applies to the call it heads, on a copy of the session. Only
  `Session::set` and `Session::reset` change a session. The last `set` of a
  name wins.
- `show` returns `name`, `value`, `default`, `source` (`default`, `env`,
  `request`, `file`) and `scope` per row, through the read door with the
  scope-free read authorization.
- A `process` setting is set only by the process hosting the engine: its
  environment, an embedded caller, or the embedded CLI. From a request field,
  a `set=` query parameter, request text or a `.gqt` case body it is refused.
  `SettingsRequest` denies unknown fields, so a `process` name in it is a 400.
- Every `request` setting preserves answers except `ann_nprobes`, whose cap the
  plan records so the answer is reproducible from it; a resource limit refuses
  rather than changes an answer. `rrf_plan`, which changes answers and leaves
  no record, is `process` scope.
- The operations that consult a setting are `Session` methods; everything else
  is reached through `Deref<Target = Arc<Omnigraph>>`. A session is one `Arc`
  clone and two small structs, never shared between requests and never
  persisted.
- `from_env()` reads each row's variable once, at server start or CLI
  invocation, into the baseline that `reset` returns to. An invalid or
  out-of-range value refuses startup; it is never defaulted.
- A stored query source carries no settings prefix; validation refuses one.
- `engine` has one value, `v2`; the row exists so a later engine is a new
  value, not a new setting.
- The traversal mode is a `SessionSettings` field no row names, set only by
  tests, DST and the `.gqt` `# traversal:` header.

## Rejected

- **Keep the environment variable and task-local:** one engine per server
  process, and no spelling in GQ.
- **A settings struct on a task-local scope:** the same struct read through a
  scope no signature shows.
- **Settings on the `Omnigraph` handle:** leaks across concurrent requests on
  the shared handle.
- **A per-query prefix (Neo4j `CYPHER runtime=`):** merge and the change feed
  are not queries; a session option covers every statement with one rule.
- **The definition in `omnigraph-api-types`:** that crate depends on the
  engine and the compiler, so both would form a dependency cycle.
- **Server-side narrowing of a `request` setting:** a second scope authority
  makes the `scope` column depend on the deployment.

## Not built

- `engine` does not route the change feed or merge; the planner's
  `RouteOverride` and `Route::PlannerBehindFlag` remain for those operations.
