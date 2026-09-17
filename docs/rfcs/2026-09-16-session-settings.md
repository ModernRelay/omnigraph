---
rfc: "2026-09-16-session-settings"
title: "Session settings"
track: maintainer
status: draft
implementation: in-progress
authors:
  - azimafroozeh
created: 2026-09-16
updated: 2026-09-17
discussion: null
supersedes: []
superseded_by: []
blocked_on: ["the engine version 2 planner RFC (PR #711) merging, for step 2 of the rollout"]
---

# RFC: Session settings

> A term set in ***bold italics*** is being defined at that exact spot; it is
> used plain everywhere after.

Every `file:line` anchor into omnigraph is at upstream commit `a8f95907` plus
the engine version 2 working tree (the planner crate, `omnigraph-exec`, and
the two gate calls); line anchors into `omnigraph-gqt` are at that engine
version 2 base, not today's `main`; anchors into DuckDB are at the local clone
of its `main`.

## Summary

GQ gains three statements that stand at the start of a file: `set <name> =
<value>;`, `reset <name>;` and `show <name>;`. A ***setting*** is a named,
typed value with a declared set of legal values, a default, and a scope,
listed in one ***settings definition***: a `const` table in
`omnigraph-compiler`, module `settings`, one row per setting, that the
compiler, the engine, the server, the CLI, the docs and `show` all read, so
the list exists in one place; `omnigraph-api-types` re-exports it for the
wire struct. `set` selects a value for the statements that follow it in the
same file; `reset` restores a setting's process default; `show` reads the
current value back, and `show all;` reads every setting with its value, its
default, where the value came from, and its scope.

The first setting is `engine`, values `v1` and `v2`, default `v1`. Under
`v1` every operation runs the existing code. Under `v2` the operations that
***engine version 2*** (the engine version 2 planner RFC, PR #711, which
names the planner, the execution engine and memory management as engine
version 2's components; the planner and `omnigraph-exec` are the two with
code today) has registered run through it, and every other operation runs
the existing code. Four knobs the engine reads today from the process
environment or a test task-local become the other four settings: the merge
lineage mode and the staged-write concurrency, and two diagnostics, the
reciprocal rank fusion plan and the vector probe count. The traversal mode is
not a setting: it stays a field of `SessionSettings` that no definition row
names, reached only through `SessionSettings::with_traversal` by tests, DST
and the `.gqt` `# traversal:` header pin.

The values live in one place: a ***session***, a small value holding one
`Arc<Omnigraph>` and one `SessionSettings`, created by
`Omnigraph::session(settings, sources)`. The operations that consult a
setting are the session's methods and exist nowhere else; every other
operation the handle exposes is reachable through the session. The engine
reads a setting from the session it was called on and from nowhere else: the
four task-locals and their `with_*` test seams are deleted, and no engine
function reads a setting from the process environment. Each of the three
ways into the engine builds one session: the CLI one per invocation, the
HTTP server one per request, an embedded caller its own. A setting is
therefore scoped to one file or one request, is never persisted, and never
crosses requests. A setting marked `process` in the definition is refused
when it arrives in a request, so a remote caller cannot widen a resource
knob or switch a diagnostic on.

Every `request` setting is answer-preserving: the same statement under any
assignment of them returns the same result rows in the same order where the
statement orders them, the same row count, the same typed error outcome, and
for a merge the same conflict list or the same published content. The two
`process` diagnostics that change answers, `rrf_plan` and `ann_nprobes`, are
set only by the process that hosts the engine and never by a remote caller
(the `process` scope rule, The settings). Running one
`.gqt` case under both engines on that promise is a follow-up RFC on the
logic-test runner; this document gives it the statement and nothing else.

The boundary that does not change: nothing is written to `__manifest` or
anywhere in the store; the `/branches` routes and every response type keep
their fields; three request types gain one optional field, `settings`,
honored at four routes and refused at the two deprecated ones, and the two
`GET` change routes one optional `set` query parameter; there is still no
current branch, checkout or identity carried
between requests, since every request keeps naming its branch and its actor;
`set`, `reset` and `show` appear only at the start of a file, so no file
that parses today changes meaning; the other `OMNIGRAPH_*` variables are
untouched: the four expand and gate cost knobs (`OMNIGRAPH_RRF_GATE_RATIO`,
`OMNIGRAPH_RRF_GATE_MAX_IDS`, `OMNIGRAPH_EXPAND_INDEXED_MAX_FRONTIER`,
`OMNIGRAPH_EXPAND_INDEXED_MAX_HOPS`) stay environment reads for now and
are candidates for the next definition rows, and the deployment
configuration (`OMNIGRAPH_MERGE_STAGING_DIR`, `OMNIGRAPH_MAINTENANCE_CONCURRENCY`,
the embedding provider variables, paths, tokens, limits) stays as it is.

## Motivation

Engine version 2 exists in the tree as a planner crate with a routing
registry and an execution crate, and has no switch of its own. What selects
it today is `OMNIGRAPH_PLANNER_ROUTE`, an override read from the environment
or a test task-local (`crates/omnigraph/src/instrumentation.rs:439`,
`planner_route()`: task-local, then `std::env::var`, then the registry
default) at three production sites: `commit_changes_page`
(`db/omnigraph.rs:2756`), `poll_change_feed` (`db/omnigraph.rs:2883`) and the
merge walk (`exec/merge.rs:2057`). All three registry entries declare
`Route::PlannerBehindFlag` (`crates/omnigraph-planner/src/registry.rs:119`,
`:137`, `:154`), so the existing code runs by default and the new engine is
reached only by setting `force_planner`.

Three things are wrong with that as the switch for a whole engine.

First, it is per operation and hidden. A reader of a test or a deployment
cannot see from any signature which operations consult the override; the
answer is a grep for `planner_route()`. The same is true of the four other
task-locals in `instrumentation.rs` (`TRAVERSAL_MODE_OVERRIDE` `:367`,
`RRF_PLAN_OVERRIDE` `:393`, `PLANNER_ROUTE_OVERRIDE` `:419`,
`STAGE_WRITE_CONCURRENCY_OVERRIDE` `:481`) and of the two knobs read from the
environment alone (`OMNIGRAPH_MERGE_LINEAGE`, `exec/merge.rs:2186`;
`OMNIGRAPH_ANN_NPROBES`, `exec/query.rs:2587`), each with its own fallback
rule: the lineage mode defaults to `verify` under `debug_assertions` and `on`
otherwise (`merge.rs:2208`), an invalid probe count is replaced by the default
with a warning (`query.rs:2578`), an invalid route falls back to the registry
with a warning (`instrumentation.rs:445`). Six knobs, three fallback
policies, four places (the engine's readers, the docs tables, the CLI, the
server) that each know part of the list, and no place a reader can see them
together.

Second, it does not fit the server. The server holds one `Arc<Omnigraph>`
per graph in its registry and dispatches concurrent requests on `&self`
methods (`crates/omnigraph-server/src/registry.rs:46`). An environment
variable is per process, so one deployment runs one engine for every
request; a task-local set by a request handler would work, but only as one
more invisible scope.

Third, it cannot be written in GQ. RFC 0045's runner refuses to run while
`OMNIGRAPH_TRAVERSAL_MODE` is set (`crates/omnigraph-gqt/src/lib.rs:2590`) so
that logic cases exercise the production path, and it has no reading of
`OMNIGRAPH_PLANNER_ROUTE` at all; a case that needs a mode pins it with a
file header (`# traversal:`, `lib.rs:375`) that the runner turns into the
task-local (`lib.rs:1705`). Every mode a case can need is one more header.

The direction was decided while engine version 2's planner work (PR #711;
not yet merged) was under way: the switch should be the whole of
engine version 2 as one unit, selected in GQ
the way SQL engines select session options. A per-query prefix in the style
of Neo4j's `CYPHER runtime=` does not fit, because the operations that route
today (`commit changes`, the change feed, `branch merge`) are not queries; a
session option in the style of Postgres and DuckDB `SET` does. DuckDB's shape
is the one followed here: one definition file (`src/common/settings.json`,
one object per setting with name, description, type and scope), a generated
registry array (`src/main/config.cpp:71`, `internal_options[]`), typed reads
(`Settings::Get<ThreadsSetting>(context)`, `src/include/duckdb/main/settings.hpp:16`),
per-connection values (`ClientConfig`) under process values (`DBConfig`),
`RESET`, and a table function `duckdb_settings()` that lists name, value,
description, input type and scope (`src/function/table/system/duckdb_settings.cpp:36-48`).

## User and operational behavior

### The statements

```
set engine = v2;
branch merge feature into main;
```

runs the merge through engine version 2. The `set` and `reset` lines come
first, then the one statement or the query declarations the file holds
today, at a fixed position at the head of the file, as RFC 0055 placed a
branch statement. A file of only `set` and `reset` lines parses as
`FileBody::Queries` with no declaration; `QueryFile::empty_kind()` is the one
reading of an empty body every door shares and names it
`EmptyFile::SettingsOnly`, against `EmptyFile::NoStatement` for a source
holding nothing at all. Such a file is legal only as a `.gqt` step; every
HTTP route and the CLI refuse it with HTTP 400 `a file of only settings lines
carries no statement` (`query_file_refusals::ONLY_SETTINGS`), since nothing
follows it in the same request or invocation. The same name set twice in one
file takes the last value, as SQL does.

```
reset engine;
```

restores the process default of `engine` and its source (`default` or
`env`); a value that came from the request field, a `--set` or a `set` line
is discarded. `reset all;` does that for every setting: `Session::reset`
walks every definition row on `None`. At a remote door and in a `.gqt` case
body a `process` row already sits at its baseline, since neither can move
one, so what is observable there is the `request` settings returning to the
process defaults. A `reset` of a `process` setting by name in a request is
refused like a `set` of one (the fifth message below).

```
show engine;
```

returns one row with the columns `name`, `value`, `default`, `source`,
`scope`; `show all;` returns one such row per setting. `source` is where the
current value came from: `default`, `env` (a process default read from the
environment), `request` (the HTTP `settings` field, a `set` query parameter
or a CLI `--set`), or `file` (a `set` line). `show` enters through the read
door (defined below: RFC 0055's word for the route or CLI verb a statement
enters through), which is `POST /query` and `omnigraph query`, and takes the
scope-free `read` authorization `GET /branches` and `branch list` take
(`authorize_scope_free_read`, `omnigraph-server/src/handlers.rs`: the
graph's Cedar `read` gate with no branch in the request). The deprecated
`POST /read` and `POST /change` serve no statement and refuse a `settings`
field and any `set` or `reset` prefix alike, with HTTP 400 `the deprecated
/read and /change routes take no settings, neither a settings field nor a
set or reset prefix; use POST /query or POST /mutate`
(`query_file_refusals::SETTINGS_AT_DEPRECATED_ROUTE`); a `show` at `/read`
is RFC 0055's `DEPRECATED_ROUTE` refusal. At the write door (below), `/change`
included, `show` is refused with the same HTTP 400 that RFC 0055 gives
`branch list` at `/mutate`, the statement in the message being `show engine`
or `show all`.

A name outside the definition, a value of the wrong type, a value outside the
declared values or range, and a `process` setting arriving in a request are
each refused where they arrive, with the definition's row in the message:

```
set engine = v3;
error: unknown value `v3` for setting `engine`; expected one of v1, v2
set enigne = v2;                          (likewise reset enigne; and show enigne;)
error: unknown setting `enigne`; expected one of rrf_plan, merge_lineage, ann_nprobes, stage_write_concurrency
set ann_nprobes = "many";
error: setting `ann_nprobes` takes an integer of at least 0, got a string `many`
set stage_write_concurrency = 0;
error: setting `stage_write_concurrency` takes an integer in 1..=64, got 0
set stage_write_concurrency = 64;        (in an HTTP request)
error: setting `stage_write_concurrency` is a process setting; it is read from the server's environment, not from a request
```

The unknown-setting message lists the definition's rows as they stand (the
four of rollout step 1; `engine` joins at step 2). The wrong-kind message
names the offending spelling. A `--set`, `set=` or environment value that is
a digit run too large for `i64` is refused as out of range, the message
naming the spelling (`SettingValue::from_spelling` keeps it as a string, and
the integer rows refuse a string that parses as digits as
`SessionSettingsError::OutOfRange`).

Because the compiler carries the definition, every message but the last is
reported by `omnigraph lint` on the file, with a position, before any server
is involved.

### The settings

| Name | Type and values | Default | Scope | Answer-preserving | Replaces | What it chooses |
|---|---|---|---|---|---|---|
| `engine` | enum `v1`, `v2` | `v1` | request | yes | nothing (new) | whether a registered operation runs through engine version 2 |
| `rrf_plan` | enum `auto`, `force_prefilter`, `force_postfilter` | `auto` | process | no (`docs/user/search/index.md:83`; `docs/dev/execution.md:137-139`) | `OMNIGRAPH_RRF_PLAN` | the reciprocal rank fusion plan on a traversal-constrained `nearest`, for diagnosis (`docs/user/search/index.md:83`) |
| `merge_lineage` | enum `off`, `on`, `verify` | `on`; a debug build defaults to `verify` | request | yes | `OMNIGRAPH_MERGE_LINEAGE` | how a merge finds the entities it classifies: the full-scan walk, the lineage path, or both compared (`docs/user/branching/merge.md:124`) |
| `ann_nprobes` | integer, at least `0`; `0` is no cap | `20` | process | no (`docs/user/search/index.md:80`; `docs/dev/execution.md:122-133`) | `OMNIGRAPH_ANN_NPROBES` | the partition cap per index delta of a `nearest` scan (`search/index.md:80`) |
| `stage_write_concurrency` | integer `1..=64` | `8` | process | yes | `OMNIGRAPH_LOAD_CONCURRENCY` | the width of the staged-write fan-out for `load` and `mutate` (`docs/user/mutations/index.md:116`) |

The defaults are today's defaults; nothing changes for a caller that sets
nothing. The `merge_lineage` row says `on`; `SessionSettings::default()`
applies `cfg!(debug_assertions)` on top and yields `verify` in a debug build,
as `lineage_merge_mode()` does today (`merge.rs:2208`), and the user-docs
table says `on` and names the debug-build `verify` beside it, so it reads the
same in every build. The traversal mode has no row: `SessionSettings`
carries it as a field with no name, no `set`, no `show` and no environment
variable, set through `SessionSettings::with_traversal` by tests, DST and
the `.gqt` `# traversal:` header pin; `OMNIGRAPH_TRAVERSAL_MODE` is retired
(Compatibility, Environment). Every `request` setting is
answer-preserving in the Summary's sense. `rrf_plan` and `ann_nprobes` are
not, by the docs they cite: a forced plan can leave `limit` unfilled or rank
differently, and a probe cap decides which approximate neighbours fill `k`;
so both are `process` scope. The `process` scope rule, stated once here and
referred to everywhere else: a `process` setting is set only by the process
that hosts the engine, which is the server from its environment, the
embedded CLI from its environment, its `--set` values and the file's text
(it is the process), and an embedded caller through the builder; it is
refused from a remote caller (the HTTP `settings` field, the `set` query
parameter, or the text of a request) and from a `.gqt` case body at every
target.
`OMNIGRAPH_PLANNER_ROUTE` is not carried over: after this document every
registry entry declares `Planner`, so forcing the planner has nothing left to
force and forcing the executor under `v2` is `v1` (Alternatives 8).

This table is written by hand into `docs/user/queries/index.md`, one row per
definition row, and a new row in the definition brings a new row there. A
generated block with a `scripts/check-docs.py` staleness check was the first
shape and was dropped (Decision log, 2026-09-17); once `show all` carries the
`doc` and `env` columns, the table becomes one example output.

### The three doors

***Door*** is RFC 0055's word for the route or CLI verb a statement enters
through. A setting reaches the engine through three of them.

**CLI.** `omnigraph query` and `omnigraph mutate` parse the source before
sending, as they do today for a branch statement (`omnigraph-cli/src/main.rs:1155`,
`:1221`). Both, the verbs that run a routed operation without GQ text
(`omnigraph branch merge`, the commit-changes and change-feed verbs), and
`omnigraph load` and `omnigraph ingest` gain a repeatable `--set name=value`;
the value is written in GQ spelling, the same as after `=` in a `set` line
(`--set engine=v2`, `--set merge_lineage=verify`), and every `--set` goes
through `SettingId::parse_assignment`, the one `name=value` door, shared
with the `set=` query parameter (HTTP). Embedded (`GraphClient::Embedded`,
`client.rs:68`), the CLI is the process: it builds one session from the
environment and the `--set` values and calls it with the file, whose `set`
lines apply to that call (Prefix application, Design); a `process` setting
is accepted from all three, by the `process` scope rule (The settings), and
an embedded `load` or `ingest` applies its `--set` to the session it loads
through. Remote (`GraphClient::Remote`, `client.rs:71`), the CLI sends the
GQ text unchanged and the `--set` values in the request's `settings` field,
so the server sees the same two inputs an embedded run does, and applies the
scope rule; a remote `--set` of a `process` setting is refused by the CLI
before anything is sent, with the fifth message, since the typed field
cannot carry it. A served `load` or `ingest` refuses any `--set` with `load
and ingest take --set only on an embedded store; the served load and ingest
routes carry no settings field` (`SETTINGS_AT_SERVED_LOAD`,
`omnigraph-cli/src/client.rs`), since neither route's request type has a
field to carry it and a value could only be dropped. On the commit-changes
and change-feed verbs a `--set` is validated and sent as `set=`; nothing
consumes it until `engine` exists (HTTP). `omnigraph lint` prints a `set` error as
`ERROR line <n>, column <c>: <message>`, the whole-file parse failure with the
line's position, since a `set` line has no declaration name for the
per-declaration line `output.rs:145-150` prints today.

**HTTP.** Three request types carry the field to a settings-aware
operation: `POST /query` (`QueryRequest`), `POST /mutate` and
`POST /mutate/if-graph-commit` (`ChangeRequest`; the field is honored alike
on both), and `POST /branches/merge` (`BranchMergeRequest`). The deprecated
`POST /change` carries `ChangeRequest` too, and the deprecated `POST /read`
(`ReadRequest`) carries `settings` as raw JSON only so the refusal can name
the field; both serve no statement and refuse a present `settings` field and
any `set` or `reset` prefix in the source with the
`SETTINGS_AT_DEPRECATED_ROUTE` message (The statements;
`omnigraph-server/src/handlers.rs`, `server_read` and `server_change`;
`handlers/dispatch.rs`, `refuse_settings_at_deprecated_route`), running
their legacy bodies under the process defaults alone. The three types gain
one optional field, `settings`: `SettingsRequest`,
hand-written in `omnigraph-api-types` with one `Option<T>` field per
`request` row, `#[serde(deny_unknown_fields, rename_all = "snake_case")]`,
an enum serialized as its GQ spelling and an integer as a JSON number;
absent means empty. Rendered:

```json
{"query": "query q() { match { $p: Person } return { $p.name } }",
 "settings": {"engine": "v2", "merge_lineage": "verify"}}
```

An unknown key in `settings`, a `process` name included, is HTTP 400 with
serde's message, so `"settings": {"stage_write_concurrency": 64}` is
refused, not dropped. The two routes that reach one and have no body,
`GET /commits/{commit_id}/changes` and `GET /changes`, take a repeatable
query parameter `set=<name>=<value>` (`?set=engine=v2`), parsed with the
same definition and the same messages, beside the change-surface parameters
they parse today (`omnigraph-server/src/handlers.rs:2799`); `set` joins the
repeatable parameter group (`kind`, `type`, `op`), and each value goes
through `SettingId::parse_assignment` (CLI). In rollout step 1 a `set=`
value is validated and consumed by nothing: the two change surfaces read no
`request` setting until `engine` exists (step 2). A handler builds one
session per request: `session(process_defaults, sources)`, then `set(..,
Source::Request)` per field entry or `set` parameter, then calls the
session's method with the text, whose `set` lines apply to that call
(Prefix application, Design), the later assignment winning, so a `set` in
the file overrides the field the way a `SET` in a Postgres query overrides
the connection's options; `baseline` is therefore always the process
defaults. A `process`
setting in the text of a request is refused (the fifth message above).
`show` dispatches like `branch list`: the same envelope refusal
(`refuse_statement_envelope`), the same scope-free `read` authorization
(`authorize_scope_free_read`, The statements), a
`ReadDispatch::Show(Vec<SettingRow>)` variant rendered as `ReadOutput` the
way `branch_list_read_output` renders `branch list`, rows of five string
columns, `query_name` = `show`, no `graph_commit_id`; the prefix of a `show`
file is applied on a copy of the request's session (`Session::with_prefix`,
Design) before the rows are read. Every settings refusal is `ApiError::bad_request`:
`{"error": <message>, "code": "bad_request"}` and nothing else. The response
types do not change.

**Embedded.** A caller holding an `Arc<Omnigraph>` writes
`Session::from_defaults(db, SessionSettings::default().with("engine", "v2")?)`,
which makes `v2` the session's baseline with every source `Default`: `show
engine` reports source `default`, and `reset engine` leaves `v2`.
`session.set(SettingId::Engine, &SettingValue::Ident("v2".into()),
Source::Request)?` on a default session is not the same: it is an override
over the baseline `v1`, `show engine` reports source `request`, and `reset
engine` returns `v1`. `SessionSettings` has no public field;
`try_from_values(..)` and the checked builder `with(..)` validate every
value against the definition, so `from_defaults` cannot admit a value
outside it, and an embedded caller gets the refusal a door gives
(`stage_write_concurrency = 0` is refused, where today the reader replaces
it with the default; the floor at `exec/staging.rs:609`, `.min(tables).max(1)`,
guards an empty table set, not a zero setting, and stays). `Session::set` is
the only mutator of a session. A caller that does
not care writes `Session::from_defaults(db, SessionSettings::default())`; one
that wants the environment honored calls `settings::from_env()` and passes
both values it returns to `db.session(settings, sources)`; a `process`
setting is accepted from both, by the `process` scope rule (The settings).
`Omnigraph::open` and `init*`
return a bare `Omnigraph` (`db/omnigraph.rs:382`, `:686`, `:710`), so an
embedded caller wraps it in `Arc` once.

**Process defaults.** The four environment variables in the table keep their
names, and `OMNIGRAPH_ENGINE` joins them at rollout step 2. `from_env()`, in
`omnigraph-compiler::settings` beside the definition since it only parses
strings, iterates the definition (`SettingId::ALL`) and reads each row's
`env` once, by the server at startup and by the CLI when it builds a
session, into the process defaults every session starts from and `reset`
returns to, with each value's source (`default` or `env`); the engine crate
reads no environment variable for a setting. An invalid value in any row's
variable is refused at startup, not defaulted (Compatibility names today's
policies and the two new ranges). A deployment that sets
`OMNIGRAPH_MERGE_LINEAGE=verify` today keeps that behavior. The logic-test
runner refuses to run while any row's variable is set, and while the retired
`OMNIGRAPH_TRAVERSAL_MODE` is set (`RETIRED_SETTING_ENVIRONMENT` and
`settings_override_refusal`, `omnigraph-gqt/src/lib.rs`), so a case's
settings are always the case's own and a stale variable in a CI environment
is never mistaken for a live control.

### Logic tests

A `.gqt` case owns one session for its lifetime. RFC 0045's format changes
in exactly these ways, and nothing else. A new step kind, `Step::Settings`,
is a `--- mutate` step whose body is only `set` and `reset` lines; it
expects `ok` and changes the case session for the steps that follow, across
a `--- restart` (the runner takes the settings off the session with
`Session::detach`, which drops the old `Arc<Omnigraph>` before the store is
reopened, reopens the handle, wraps it in `Arc`, and gives the `Detached`
value the new handle with `Detached::attach`; `Session::rebind` is the two
in one call; `settings`, `sources`, `baseline` and `baseline_sources`
survive as they are, so a `file` value keeps its source and `reset` still
returns to the case's baseline). `show` is a rows step under `--- query`
with five `String` columns, `name`, `value`, `default`, `source`, `scope`,
in a declared total order (definition order), so `--- expect ordered`
applies to `show all`. A `set` prefix is legal before any body in any step
and applies to that step only (Prefix application, Design), as at HTTP. A
`process` setting in a case body is refused at every target, embedded and
server alike, by the `process` scope rule (The settings), so a case has one
expectation everywhere; `reset all` in a case body returns every row to the
case's baseline, where a `process` row already sits since a case body cannot
move one, as at HTTP. The `# traversal:` header pin (`lib.rs:375`) keeps
working and is the only per-case traversal control: the runner sets the
case session's traversal field through `SessionSettings::with_traversal`,
and there is no `set traversal`, since no row names it.
Running one case under several settings assignments from its `--- runner`
section, and the evidence that a routed operation actually routed, are the
follow-up runner RFC.

### Operators

Nothing to migrate. A server that receives no `settings` and no `set`
behaves as today. `show` is the only new read, and it reads the session, not
the store. `engine = v2` is a request, not a guarantee: an unregistered
shape runs the existing code under both values. Under `v2` the two planner
decisions record their outcome in the planner decision probes
(`record_planner_decision`, `changes/planned.rs:77`;
`record_planner_merge_decision`, `exec/merge/planned.rs:135`), which only a
test installs; under `v1` no decision is recorded, and that absence is the
record. No log line, header or response field says which engine ran a
request in this RFC; that witness is the runner RFC's job. No server-side
narrowing in this RFC: `OMNIGRAPH_ENGINE` sets the default only, and any
caller may `set engine = v2`.

**Stored queries.** A stored `.gq` source carries no settings prefix:
discovery and validation (`omnigraph-cluster/src/config.rs:147-167`,
`:1083-1128`) and registry loading refuse a `QueryFile` whose `settings` is
non-empty with `a stored query carries no settings; it runs under the
process defaults` (`STORED_QUERY_CARRIES_NO_SETTINGS`,
`omnigraph-compiler/src/settings.rs`), so the source that invocation hands to `run_query` or
`run_mutate` (`omnigraph-server/src/handlers.rs:1592-1657`) never carries
one. Whether the caller may pass a `settings` field on those routes is
Unresolved question 1.

## Design

### The definition

In `omnigraph-compiler`, module `settings`, re-exported by
`omnigraph-api-types`:

```rust
pub struct SettingSpec {
    pub name: &'static str,                 // "ann_nprobes"
    pub kind: SettingKind,                  // Enum(&[..]) | Integer { min, max: Option<..> }
    pub default: &'static str,              // "20"
    pub scope: SettingScope,                // Request | Process
    pub env: &'static str,                  // "OMNIGRAPH_ANN_NPROBES", the variable from_env reads
    pub doc: &'static str,                  // the "What it chooses" cell
}
pub const DEFINITIONS: &[SettingSpec] = &[ /* the table's rows in its order: four at rollout step 1, engine joining at step 2 */ ];
pub enum SettingValue { Integer(i64), Ident(String), Str(String) }
pub enum Engine { V1, V2 }                  // and RrfPlan, MergeLineage; Traversal exists and names no row
pub enum SettingId { Engine, RrfPlan, MergeLineage, AnnNprobes, StageWriteConcurrency }  // one variant per row
impl SettingId {
    pub fn parse(name: &str) -> Result<SettingId, SessionSettingsError>;
    pub fn spec(self) -> &'static SettingSpec;
    pub fn parse_assignment(name: &str, value: &str) -> Result<(SettingId, SettingValue), SessionSettingsError>;  // the one name=value door: --set, set=, with(..)
}
```

The compiler is the home because it is the lowest crate every reader
already depends on (`crates/omnigraph/Cargo.toml:32`, the server, the CLI,
the planner); `omnigraph-api-types` depends on the engine and the compiler
(`crates/omnigraph-api-types/Cargo.toml:12-13`), so it can re-export the
module but not own it (Alternatives 13). The table is a `const`: no
address, no identity, no initialization, no mutable state; it is the same
kind of item as an enum's variant list. Every other part of this design is
derived from it and holds no copy of the list: the compiler validates names,
types, values and ranges against it; `SettingId` has one variant and
`SessionSettings` one field per row, and a unit test asserts the three
match in both directions;
`SettingsRequest`, the server's typed request field, is hand-written in
`omnigraph-api-types` with one `Option<T>` field per `request` row, and the
same unit test asserts its field names equal the `request` rows in order;
`show all` prints it; the user-docs table is written by hand from it. Adding
a setting is one row, one field, one read site, one row in the user-docs
table, and for a `request` row one field on `SettingsRequest`.

### Grammar

`query.pest` gains three statements and one prefix position:

```
query_file    = { SOI ~ setting_stmt* ~ (show_stmt ~ statement_trailer? | branch_stmt ~ ";"? ~ statement_trailer? | query_decl*) ~ EOI }

setting_stmt  = { set_stmt | reset_stmt }
set_stmt      = { kw_set ~ setting_name ~ "=" ~ setting_value ~ ";" }
reset_stmt    = { kw_reset ~ (kw_all | setting_name) ~ ";" }
show_stmt     = { kw_show ~ (kw_all | setting_name) ~ ";" }
setting_name  = @{ ident ~ ("." ~ ident)* }
setting_value = { integer | ident | string_lit }

kw_set        = @{ "set" ~ !(ASCII_ALPHANUMERIC | "_") }     // likewise kw_reset, kw_show, kw_all
```

The keywords are atomic rules closed by a word boundary, as `kw_branch` is
(`query.pest:23`), so `setengine = v2;` is a parse error. A trailing
semicolon becomes optional on a branch statement (`";"?`), so the example
above parses and the corpus's bare `branch list` keeps parsing. `set`,
`reset` and `show` disambiguate at the one position where they can appear: a
file today begins with `query` or `branch` (`query.pest:8`), so no file that
parses today changes meaning, and inside bodies the three words remain
ordinary identifiers wherever one is legal, by the same argument RFC 0055
made for its seven branch keywords. Setting names are not keywords at all:
`engine` is looked up only after `set`, `reset` or `show`, so a type, a
property or a parameter named `engine` is unaffected. A name may be dotted
(`search.nprobes`) so a namespace can be introduced later without a grammar
change, and the rule is atomic, so `set search . nprobes` is a parse error;
every name in this document is flat. A value has one of three spellings and
the definition's kind, enum or integer, decides which is legal: a bare
integer (`integer`, `query.pest:144`) for an integer, a bare identifier for
an enum, and a string literal where the identifier alphabet does not reach;
`set engine = "v2"` is the same as `set engine = v2`.

### The compiler's part

The AST becomes

```rust
pub struct QueryFile { pub settings: Vec<SettingStmt>, pub body: FileBody }
pub enum SettingStmt { Set { id: SettingId, value: SettingValue }, Reset { id: Option<SettingId> } }
pub enum FileBody { Queries(Vec<QueryDecl>), Branch(BranchStmt), Show(Option<SettingId>) }  // Queries is empty for a file with no statement, prefix or not
pub enum EmptyFile { NoStatement, SettingsOnly }
impl QueryFile { pub fn empty_kind(&self) -> Option<EmptyFile>; }  // None when the body carries a statement; the one reading every door shares
```

with `SettingValue` and `SettingId` from module `settings`, replacing
today's two-variant `QueryFile` (`ast.rs:6`). The parser validates every
named statement against `DEFINITIONS`: the name of a `set`, a `reset
<name>` or a `show <name>` exists, and for a `set` the value's spelling
matches the kind, an enum value is one of the declared ones, an integer is
in range; the AST carries the checked `SettingId`, never the string, so
`reset enigne;` and `show enigne;` are the unknown-setting error like `set
enigne = v2;`. A failure is a parse error with the statement's position, which is
what `omnigraph lint` reports offline (`omnigraph-cli/src/helpers.rs:947`).
Scope is not the compiler's business: whether a `process` setting may be set
depends on the door, so the door checks it.

### The session

The value struct and the environment reader sit in the compiler's
`settings` module beside the definition (they only parse strings and hold
typed values); the session itself is one new module `session.rs` in the
engine crate:

```rust
// omnigraph-compiler, module settings
pub struct SessionSettings {                // no public field: every constructor validates against DEFINITIONS
    engine: Engine,                         // V1
    traversal: Traversal,                   // Auto; names no row: set only by with_traversal, never by set, show or the environment
    rrf_plan: RrfPlan,                      // Auto
    merge_lineage: MergeLineage,            // On; Verify under cfg!(debug_assertions)
    ann_nprobes: Option<usize>,             // Some(20); None is no cap, spelled 0
    stage_write_concurrency: usize,         // 8
}
pub enum Source { Default, Env, Request, File }

impl SessionSettings {
    pub fn try_from_values(values: &[(&str, &str)]) -> Result<SessionSettings, SessionSettingsError>;  // the defaults, then each pair in GQ spelling, validated
    pub fn with(self, name: &str, value: &str) -> Result<SessionSettings, SessionSettingsError>;        // checked builder, the same validation
    pub fn set(&mut self, id: SettingId, value: &SettingValue) -> Result<(), SessionSettingsError>;
    pub fn get(&self, id: SettingId) -> String;
    pub fn engine(&self) -> Engine;         // one typed getter per row, and traversal()
    pub fn with_traversal(self, traversal: Traversal) -> Self;  // the harness field's one door
}
pub fn from_env() -> Result<(SessionSettings, [Source; DEFINITIONS.len()]), SessionSettingsError>;

// omnigraph-engine, module session
pub struct Session {
    db: Arc<Omnigraph>,
    settings: SessionSettings, sources: [Source; DEFINITIONS.len()],
    baseline: SessionSettings, baseline_sources: [Source; DEFINITIONS.len()],
}
impl Omnigraph {
    pub fn session(self: &Arc<Self>, settings: SessionSettings, sources: [Source; DEFINITIONS.len()]) -> Session;
}
impl Deref for Session { type Target = Arc<Omnigraph>; }
impl Session {
    pub fn from_defaults(db: Arc<Omnigraph>, settings: SessionSettings) -> Session;   // every source Default; settings is already validated
    pub fn detach(self) -> Detached;                                                  // the .gqt runner's --- restart: keep the settings, drop the handle
    pub fn rebind(self, db: Arc<Omnigraph>) -> Session;                               // detach().attach(db)
    pub fn set(&mut self, id: SettingId, value: &SettingValue, source: Source) -> Result<(), SessionSettingsError>;  // the only mutator
    pub fn reset(&mut self, id: Option<SettingId>);    // back to baseline and baseline_sources; every row on None
    pub fn apply(&mut self, stmt: &SettingStmt) -> Result<(), SessionSettingsError>;  // one prefix line: set with Source::File, or reset
    pub fn with_prefix(&self, stmts: &[SettingStmt]) -> Result<Session, SessionSettingsError>;  // the one prefix-on-a-copy door; self is unchanged
    pub fn effective(&self, source: &str) -> Result<SessionSettings>;  // with_prefix over the source's parsed prefix; no parse when has_settings_prefix says there is none
    pub fn settings(&self) -> &SessionSettings;
    pub fn show(&self, id: Option<SettingId>) -> Vec<SettingRow>;   // name, value, default, source, scope
    // the operations that consult a setting, moved here from Omnigraph with their signatures
}
pub struct Detached { /* settings, sources, baseline, baseline_sources; no handle */ }
impl Detached { pub fn attach(self, db: Arc<Omnigraph>) -> Session; }
```

The operations that consult a setting move from `Omnigraph` to `Session`
and exist nowhere else. Today those are `query`, `query_with_head` and
`run_query_at` (traversal, RRF plan, probe count; `run_query_at` calls the
same `execute_query`, `exec/query.rs:109-151`); the six `mutate*` variants, `mutate`,
`mutate_with_receipt`, `mutate_as`, `mutate_as_with_receipt`,
`mutate_as_with_expected_head` and `mutate_as_with_expected_head_receipt`
(stage-write concurrency; `exec/mutation.rs:678-812`); the `load` and
`ingest` family, `load`, `load_as`, `load_file`, `load_graph_batch` with
their `_as` and `_with_receipt` twins and `ingest`, `ingest_as`,
`ingest_file`, `ingest_file_as` (`loader/mod.rs:131-408`; the loader reads
`stage_write_concurrency()` at `loader/mod.rs:836`) and the two free
functions `load_jsonl` and `load_jsonl_file` (`loader/mod.rs:89-99`), whose
first parameter is a bare `&Omnigraph` today and which move onto `Session`
with the rest; `commit_changes_page`
and `poll_change_feed` (engine); `branch_merge` and `branch_merge_as`
(engine, lineage mode). A caller that still writes `db.query(..)` on the
handle gets a compile error, which is the migration's checklist. `Session`
implements `Deref<Target = Arc<Omnigraph>>`, so every method that stays on
the handle in its `impl Omnigraph` blocks (seven today: `db/omnigraph.rs:331`;
`exec/query.rs:36`; `exec/mutation.rs:652`; `exec/merge.rs:5148`;
`blob.rs:972`; `loader/mod.rs:101`; `db/omnigraph/export.rs:89`) is
reachable through auto-deref, the two `self: &Arc<Self>` export methods
`capture_served_export_cut` and `capture_served_change_baseline_cut`
(`export.rs:96`, `:128`) included. An operation that starts to consult a
setting later moves to `Session`, and every caller of it is found by the
compiler. Open, init, schema apply and the read-only introspection stay on
the handle only. The handle stays shared and immutable; a session is one
`Arc` clone and two small structs, built per request without a lock; `set`
takes `&mut self`, so two tasks cannot share a session and both set it
without a `Mutex`, which is the right refusal. `reset` restores `baseline`
and `baseline_sources` for the id, or for every setting on `None`; every
door passes `reset all` through as `reset(None)`, and at a remote door and
in a case body a `process` row already sits at its baseline, so nothing else
is observable there (The statements).

**Prefix application.** Every `Session` method that takes GQ text (`query`,
`query_with_head`, `run_query_at` and the six `mutate*` variants) derives
the effective settings per call:

1. parse the text into a `QueryFile`;
2. copy the session's `settings` and `sources`;
3. apply the file's `settings` prefix to the copy in file order, a `set`
   with `Source::File`, a `reset` from `baseline` and `baseline_sources`;
4. execute the body under the copy;
5. return; the session's `settings` and `sources` are as they were.

`Session::with_prefix` is the one door that applies a prefix, always on a
copy: `Session::effective(source)` calls it with the source's parsed prefix
for the methods that take GQ text, and skips the parse when
`has_settings_prefix` says the source has none, so a warm query reaches its
compiled-query cache without a parse; the server's `session_with_prefix`
calls it for the statements the engine does not parse itself, `branch merge`
and `show`. The prefix is read from the parsed `QueryFile` on every call
that has one, never only on a cache miss: `compile_named_query` caches the
declaration's `QueryIR` and nothing else (`exec/query.rs:157-175`), so a
cache hit applies the prefix the same way. The methods that take no GQ text
have no prefix: `commit_changes_page`, `poll_change_feed`, `branch_merge`,
`branch_merge_as`, and the `load`, `ingest`, `load_jsonl` and
`load_jsonl_file` family, whose text is JSONL. This is the one rule the
three doors share: a prefix is scoped to the call it heads, and only
`Session::set` and `Session::reset`, which `Session::apply` calls for a
`Step::Settings` step, change a session.

Inside the engine the value travels as a parameter, the pattern the actor
already follows on every `_as` entry point (`exec/mutation.rs:818`;
`exec/merge.rs:5381`). The six functions that resolve a knob today,
`planner_route()` (`instrumentation.rs:439`), `traversal_indexed_override()`
(`exec/query.rs:2449`), `rrf_plan_force()` (`:1100`), `lineage_merge_mode()`
(`exec/merge.rs:2186`), `ann_nprobes()` (`exec/query.rs:2587`) and
`stage_write_concurrency()` (`exec/staging.rs:178`), are deleted together
with the four task-locals and their seams `with_traversal_mode`,
`with_rrf_plan`, `with_planner_route` and `with_stage_write_concurrency`
(`STAGE_WRITE_CONCURRENCY_OVERRIDE` shares its `task_local!` block with
`STAGE_WRITE_PROBES`, `instrumentation.rs:481-484`, so that block is edited,
not deleted). The probe task-locals (`with_query_io_probes`,
`with_stage_write_probes`, `with_merge_write_probes`) and the debug red
control `with_rrf_gate_subset_drop` are not settings and stay. The 37 call
sites of the four seams across 15 files (the engine's integration tests and
benches, two unit tests in `exec/staging.rs:1742` and `:1748`,
`omnigraph-dst/src/fixtures.rs:351`, `omnigraph-gqt/src/lib.rs:1705`), and
the tests that set a setting's variable directly
(`EnvGuard::set("OMNIGRAPH_ANN_NPROBES", ..)` at `tests/search.rs:880`,
`:1036`, `:1098`, `:1158`, `:1240`, `:1392`, `:1473`;
`examples/bench_expand.rs:257`, `:275`), become `db.session(..)` calls;
left as they are, the latter would compile and pass vacuously once the
engine stops reading the variable.

### The engine gate

`Engine::V1`: the two planner decisions, `changes::planned::decide`
(`changes/planned.rs:51`) and `merge::planned::decide`
(`exec/merge/planned.rs:118`), are not called; the existing code at
`changes/enumerate.rs:630` and `exec/merge.rs:2086` runs directly, as it does
today under the default.

`Engine::V2`: the two decisions run, and the three registry entries
`scoped_commit_diff`, `unscoped_commit_diff` and `three_way_merge_classify`
(`registry.rs:112`) declare `Route::Planner`. `Route` keeps two variants,
`Executor` and `Planner`; `PlannerBehindFlag` is deleted, since the flag it
named is the `engine` setting now. An entry demoted to `Executor` runs the
existing code under both values with the reason
`Unrouted::RegistryRouteExecutor`, which stays: `gate.rs:154-157` produces
it for a `Route::Executor` entry, the registry fallback, which deleting the
override does not remove; an unregistered shape runs the existing code
under both values, as today. `RouteOverride`, its `OMNIGRAPH_PLANNER_ROUTE`
variable and `with_planner_route` are deleted with the task-local, and with
them the planner-crate surface that carried the override: the `route`
parameter of `omnigraph_planner::route(..)` (`route.rs:12-17`),
`Explain.override_`, the `Unrouted::Override` reason (the forced executor,
`gate.rs:31`), the behind-flag clause of `RegistryRouteExecutor`'s doc
comment (`gate.rs:25-27`), the three `gate.rs` arms that match the flag or
the override (`gate.rs:158-166`; the `Route::Planner` arm stays and pairs
with no override), the third string of `Route::as_str` (the deleted
variant's), and the `route` parameter of
`enumerate_commit_changes` and `open_routed` (`changes/enumerate.rs:167`,
`:553`, `:625`; `changes/planned.rs:75`, `:83`). The registry's `evidence`
strings that name the `planner_route_*` engine tests (`registry.rs:121-122`,
`:139`) are renamed with those tests. A diagnosis that wants the existing
code for one file writes `set engine = v1;`.

Inherited from Lance: nothing. Every part of this design is above the
storage boundary; a session never touches a dataset.

## Invariants

- **9, query semantics are typed structures.** The invariant's body forbids
  global state. Today the route, the traversal mode and the RRF plan are
  global state in a task-local and the process environment. This design
  moves them into one typed struct on a value the caller holds, and deletes
  the task-locals. The two classes argue separately. Every `request`
  setting is answer-preserving in the Summary's sense, so none is a
  semantic smuggled through a transport flag. The two `process` diagnostics,
  `rrf_plan` and `ann_nprobes`, do change search results (the table's
  citations); that is why they are `process` scope: they are set only by the
  process that hosts the engine, never by a remote caller's text, field or
  parameter (the `process` scope rule, The settings), so no request carries
  a semantic through them, and `show` names them. The
  definition is a `const`, not a singleton: it holds no value and nothing
  writes to it.
- **10, trust is established at the boundary and enforced at the engine.**
  The `settings` field and the `set` prefix are caller data, parsed and
  refused at the door; neither names an actor, a policy or a branch, and the
  `_as` entry points keep their action, scope and actor gate unchanged. A
  `process` setting cannot arrive from a remote caller. Whether choosing
  `engine` should itself be policy-gated is Unresolved question 2.
- **11, failures and resource use are bounded and observable.** Every
  setting has a declared type, value set or range, and default; an
  undeclared name or value is refused, not defaulted, at a door and at
  process startup alike; a resource width is `process` scope. `show all`
  makes every effective value and its source observable at any door.
  `engine = v2` is a request, not a guarantee: an unregistered shape runs
  the existing code, and the door-visible witness of which engine ran is the
  runner RFC's job (Operators).
- **13, evidence matches the boundary.** The statements are visible in rows
  and errors, so their evidence is `.gqt` cases; the deletion of the
  task-locals and the definition's completeness are mechanism claims and get
  one Rust check each.

Deny-list items engaged: "side channels for query semantics or discarded
retrieval rank" (a `request` setting is answer-preserving, and a `process`
diagnostic is set only by the process that hosts the engine, the `process`
scope rule in The settings and Invariant 9 above); "cost-blind plan choice or planner decisions based on
hidden statistics" (the choice is explicit, named, and readable back). No
known gap changes.

## Compatibility and reversibility

**Wire.** Within the Summary's boundary, the wire change is one optional
field, `settings`, on three request types, honored at four routes and
refused at the deprecated `/read` and `/change`, and one optional repeatable
query parameter, `set`, on the two `GET` change routes; absent means the
process defaults. Additive: a client that never sends them sees no change.
An older server that does not know the field drops it silently (the three
request types carry no `deny_unknown_fields`, `api-types/src/lib.rs:191`,
`:708`, `:734`, `:889`) and answers 200 under `v1`. That is accepted: every
`request` setting is answer-preserving, so the rows are the same, and a
client that must know sends a `show` first (`show merge_lineage;` at rollout
step 1, `show engine;` after step 2), which an older server answers with a
parse-error 400. The parameter
an older server refuses the way it refuses any unknown change-surface
parameter (`handlers.rs:2840`). The field is a typed struct, so the OpenAPI
golden (`crates/omnigraph-server/tests/openapi.rs`) shows each `request`
setting's type and values, and a future `request` setting is a visible
schema change. No response type changes. The routes that reach no `request`
setting take nothing: `/schema`, `/schema/apply`, `/snapshot`, `/commits`,
`/commits/{commit_id}`, `/blob`, `/export`, `/changes/baseline`, `/load`,
`/load/ndjson` and `/ingest` (these three reach only the `process` setting
`stage_write_concurrency`), `/branches` create, delete and list (`GET
/branches` included), `/graphs`, `/queries`, `/openapi.json`, the health
routes; `POST /queries/{name}` and `POST /queries/{name}/if-graph-commit`
are Unresolved question 1.

**Language.** `set`, `reset` and `show` are legal only before the first
statement of a file, so every file that parses today keeps its meaning; a
trailing semicolon becomes optional on a branch statement. A stored source
that begins with `set` does not exist today, since `set` does not parse
today, so refusing one (Operators) changes nothing for an existing
registry. RFC 0045's
format changes as Logic tests lists, and nothing else. Documentation:
`docs/user/queries/index.md` gains a section for the three statements and
the hand-maintained settings table; `docs/dev/execution.md:66` and `:121`,
`docs/dev/merge.md:78`, `docs/dev/writes.md:118`,
`docs/user/search/index.md:80-83`, `docs/user/branching/merge.md:124` and
`docs/user/mutations/index.md:116` move from the environment variable to the
setting, naming the variable as the process default; the
`OMNIGRAPH_PLANNER_ROUTE` row (`execution.md:174` at the engine version 2
base) is deleted.

**Environment.** The four existing variables in the table keep their names
and their meaning as process defaults; `OMNIGRAPH_ENGINE` joins them at
rollout step 2; `OMNIGRAPH_PLANNER_ROUTE` is removed (it was introduced with
engine version 2's planner work and has no release behind it);
`OMNIGRAPH_TRAVERSAL_MODE` is retired: no row names it, neither the engine
nor `from_env()` reads it, and the logic-test runner refuses to run while it
is set (Process defaults). What changes is where they
are read: `from_env()` in the compiler crate, called by the two process
doors, not the engine. What also changes is what an invalid value does:
today an invalid probe count or RRF plan warns and runs the default
(`exec/query.rs:2578`, `:1112`), an invalid lineage value warns and runs
`off` (`exec/merge.rs:2192`), and an invalid or `0` concurrency runs the
default (`exec/staging.rs:189-193`); after this document any invalid or
out-of-range value in a row's variable refuses server startup and refuses
the CLI invocation, with the definition's message. Two ranges are new:
`stage_write_concurrency` is `1..=64`, so `OMNIGRAPH_LOAD_CONCURRENCY=0`
(today the default) and `=128` (today accepted) are refused; `ann_nprobes`
keeps today's unbounded parse with `0` as no cap from any source, so
`OMNIGRAPH_ANN_NPROBES=100000` (`tests/search.rs:1392`) stays legal. The
four expand and gate cost knobs and the deployment configuration named in
the Summary are out of scope for now; the four are candidates for the next
definition rows. An embedded caller that relied on the engine reading
`OMNIGRAPH_TRAVERSAL_MODE` for it now sets the field itself with
`SessionSettings::with_traversal`.

**Storage.** Nothing is persisted; no manifest, schema or dataset changes.

**Reverting.** Removing the three grammar rules, the AST field and the
request field restores today's language and wire. The session is the costly
part to revert: the 37 migrated call sites and the six parameterized read
sites would have to return to task-locals. The rule this document sets, one
definition and one place to read a value from, is meant to outlive any one
setting.

## Alternatives

1. **Do nothing: keep `OMNIGRAPH_PLANNER_ROUTE` and `with_planner_route`.**
   Fails the server (one engine per process, `registry.rs:46`) and GQ (no
   spelling; `lib.rs:2590` refuses the environment). Rejected.

2. **A `SessionSettings` struct on the task-local scope,
   `with_settings(settings, fut)`.** The first shape considered. Same struct, but read through a
   scope invisible in every signature; the doors would wrap a future rather
   than build a value, and an embedded caller would have to know which
   operations consult the scope. Rejected for the visible shape.

3. **A setting on the `Omnigraph` handle.** Leaks across the server's
   concurrent requests on the one shared handle. Rejected.

4. **A per-query prefix, Neo4j's `CYPHER runtime=parallel` shape.** The
   routed operations are `commit changes`, the change feed and `branch merge`,
   none of which is a query, so the prefix would have to be invented for each
   statement family. A session option covers every statement with one rule.
   Rejected.

5. **`pragma <name> = <value>;`, DuckDB's and SQLite's other spelling (from
   their documentation, not a local tree).** Same
   mechanism, but the word suggests an implementation hint the engine may
   ignore. Here an unknown name is an error and every name is readable with
   `show`, which is `SET` semantics. Rejected on the word only.

6. **The list in the engine only; the compiler carries strings; a string map
   on the wire.** The first draft of this document. The list then exists in
   four places (engine, docs, CLI, server) with nothing holding them equal,
   an unknown name is found only at run time against a server, and the
   OpenAPI schema says `map<string, string>` about a typed thing. Rejected
   for the definition in the compiler crate.

7. **The definition as a JSON file with generated code, DuckDB's shape.**
   Right for C++, where the header, the registry array and the validation
   bodies are three generated artifacts. In Rust a `const` array in the
   compiler crate is already data every crate can read, the wire struct is
   one hand-written struct checked against it by a unit test, and the
   user-docs table is written by hand (Decision log, 2026-09-17). Rejected
   as a mechanism, kept as the model.

8. **Keep `planner_route` as a second setting inside `v2`.** After this
   document every entry declares `Planner`: `force_planner` has nothing to
   force, and `force_executor` under `v2` is `v1`. One switch. Rejected.

9. **Sixty forwarding methods on `Session` instead of `Deref`.** Explicit,
   and six hundred lines that say nothing about the methods that read no
   setting. Rejected for `Deref<Target = Arc<Omnigraph>>` on those; the
   settings-aware operations are not forwarded but moved, so the compiler
   finds every caller.

10. **Refuse a duplicate `set` of one name in a file.** Stricter than SQL
    for no defect it would catch that `show` does not show. Rejected;
    last assignment wins.

11. **Only `show <name>`, no `show all` and no source column.** Cheaper, and
    the first question after a surprising plan ("which engine ran, and who
    set it") would have no answer at the door. Rejected.

12. **No session type: `&SessionSettings` as a parameter on the routed
    methods (`query_with(settings, ..)`).** The design minus `Session`. It
    works, and the engine internals use exactly this shape below the handle.
    The difference at the handle is not method count, since the
    settings-aware methods move rather than double either way: it is two
    values at every call site, the handle and the settings, against one,
    carried by the three doors through every call. The session is the pair
    (handle, settings) named once. Kept as the internal shape, rejected as
    the public one.

13. **The definition in `omnigraph-api-types`.** The second draft's home,
    chosen so the wire struct and the table sit together.
    `omnigraph-api-types` depends on `omnigraph-engine` and
    `omnigraph-compiler` (`crates/omnigraph-api-types/Cargo.toml:12-13`), so
    the compiler validating against it and the engine reading its enums are
    two dependency cycles. Rejected; the compiler crate is the lowest crate
    every reader already depends on, and api-types re-exports the module.

14. **Retire the environment variables.** The two process doors would read
    no `OMNIGRAPH_*` setting; a deployment moves to `settings` in the
    request for the `request` rows and to server configuration for the
    `process` rows. Rejected: `OMNIGRAPH_MERGE_LINEAGE=verify` in CI keeps
    working with no change, and the `process` rows need a seed the operator
    owns.

15. **Server-side narrowing of a `request` setting.** Server configuration
    would narrow a `request` setting to `process` for one deployment (a
    server that refuses `engine = v2` from callers). Rejected: one scope per
    row, fixed in the definition; a second scope authority would make the
    `scope` column of `show` deployment-dependent.

Precedent audit. The engine already threads a per-call value as a parameter
where it is one value: the actor on every `_as` entry point. This design
keeps that shape inside the engine and diverges at the public surface, where
the value is six fields and growing, and where three doors would otherwise
each repeat the threading. RFC 0055 and RFC 0056 state that there is no
session, current branch or checkout; that boundary stands. It names a
session that carries a target or an identity, and this session carries
neither: every request still names its branch, its snapshot and its actor,
and the word is used in its Postgres sense, the scope of `SET`.

A pre-mortem, not a record: four weeks later this design lost a side-by-side;
the winner put the settings on a request context the server already carried.
There is none today (`run_query` and `run_mutate`, `handlers.rs:1170` and
`:1078`, take the handle, the actor and the door as separate parameters), so
a session is the first such value; if one appears, it should own the session
rather than sit beside it.

## Evidence and tests

Cases in `crates/omnigraph-gqt/cases/`, RFC 0045's corpus, own the visible
contract:

- `show engine;` on a fresh session returns the row `engine`, `v1`, `v1`,
  `default`, `request`; after a `Step::Settings` step `set engine = v2;` the
  value is `v2` and the source `file`; after `reset engine;` it is `v1`
  again with source `default`; after a `--- restart` a set value survives
  with source `file`, and `set engine = v2;` then `--- restart` then `reset
  engine;` then `show engine;` returns `v1`, `default`, so the restart kept
  the baseline.
- `show all;` returns one row per definition row, in the definition's
  order, under `--- expect ordered`.
- `set engine = v3;`, `set enigne = v2;`, `reset enigne;`, `show enigne;`,
  `set ann_nprobes = "many";` and
  `set stage_write_concurrency = 0;` are refused with the messages above;
  `set rrf_plan = force_prefilter;` in a case body is refused as a `process`
  setting at every target; a `set engine = v2;` prefix before a query body
  under `--- query` applies to that step only, and the next step's `show
  engine;` returns `v1`; a `Step::Settings` step `set engine = v2; set
  engine = v1;` followed by `show engine;` returns `v1`.
- `set traversal = csr;` is the unknown-setting refusal: no row names the
  traversal field, and the `# traversal: csr` header pin stays the only
  per-case traversal control.

Server tests: a `process` setting in a request's text or `set` parameter is
refused with HTTP 400 and the fifth message, and in the `settings` field
with HTTP 400 and serde's unknown-field message; the typed `settings` field
round-trips each `request` setting and its `show` source is `request`; text
overrides the field; `POST /mutate/if-graph-commit` honors the field like
`/mutate`; `show engine;` at `/mutate` is the wrong-door 400 and at `/read`
the `DEPRECATED_ROUTE` refusal; a `settings` field, and a `set` or `reset`
prefix in the source, at `/read` and at `/change` is 400
`SETTINGS_AT_DEPRECATED_ROUTE`; a file of only `set` lines is 400 `a file of
only settings lines carries no statement` at `/query` and `/mutate`;
`GET /changes?set=engine=v2` records a planner decision (rollout step 2; at
step 1 `?set=merge_lineage=off` is accepted and selects nothing) and
`?set=engine=v3` is refused with the first message; with
`OMNIGRAPH_ANN_NPROBES=5` in the server's environment, `set engine = v2;
reset all; show all;` at `/query` reports `engine` `v1` `default` and
`ann_nprobes` `5` `env`, so `reset all` at the HTTP door returned every row
to the process defaults, the `env` value included; every refusal body is
`{"error", "code": "bad_request"}`.

Rust, for mechanism only: one unit test that `DEFINITIONS`, `SettingId`,
`SessionSettings` and `SettingsRequest` agree (the same names in the same
order for the first three, the `request` rows in order for the fourth) and
every default parses; `from_env()` refusing each invalid spelling and each
out-of-range value with the definition's message; `try_from_values` and
`with` refusing `stage_write_concurrency` `0` and `65` with the same
message; one test that `Session::from_defaults(db,
SessionSettings::default().with("engine", "v2")?)` and
`Session::set(SettingId::Engine, .., Source::Request)` on a default session
differ in `show` (`default` against `request`) and in `reset` (`v2` against
`v1`); one test that a prefixed file passed twice to `session.query(..)`
applies its prefix on the second, cached, call and leaves `show engine`
unchanged after both; an `omnigraph-cluster` test that a stored source
beginning `set engine = v2;` is refused at validation with `a stored query
carries no settings; it runs under the process defaults`; the engine crate's
integration suites that use the four seams today (`traversal_adaptive`,
`traversal_indexed`, `search`, `rrf_prefilter_gate`, `warm_read_cost`,
`changes`, `merge_fast_forward`, `merge_truth_table`, `writes`,
`s3_storage`, `proptest_equivalence`, the benches), and the `EnvGuard::set`
sites in `search.rs` and `examples/bench_expand.rs`, migrated to
`db.session(..)`; the two planner registry tests that assert the fallback
reason `RegistryRouteExecutor` under the default override flip to asserting
the planner route under `v2` and no decision under `v1`; one check that
`crates/omnigraph/src/` reads none of the setting variables
(`OMNIGRAPH_ENGINE`, `OMNIGRAPH_RRF_PLAN`, `OMNIGRAPH_MERGE_LINEAGE`,
`OMNIGRAPH_ANN_NPROBES`, `OMNIGRAPH_LOAD_CONCURRENCY`) nor the retired
`OMNIGRAPH_TRAVERSAL_MODE` through `std::env::var`, that
`crates/omnigraph/tests/` and `examples/` hold no `EnvGuard::set` or
`set_var` on them, and that `instrumentation.rs` holds no `task_local!`
outside the probe and red-control scopes (`CLONE_CONTEXT` at
`storage_layer/lance_clone.rs:22` is a production task-local outside that
file and is untouched), in `scripts/check-agents-md.sh`'s style.

Cost goldens (`changes_cost`, `merge_cost`) keep running on the existing code
under `v1`.

Upstream surfaces surveyed: DuckDB (anchors in Motivation) for the
definition, scope, `RESET` and the settings table function; Postgres `SET`,
`RESET` and `SHOW` (session scope; an unknown name is an error;
`pg_settings.source`) and Neo4j's `CYPHER runtime=` prefix for the scope
rule, both from their public documentation and unverified against a local
tree. None is depended on.

## Rollout

1. **Definition, session and statements, on today's `main`.** `DEFINITIONS`
   with four rows (no `engine`; the traversal mode is the harness-only
   field, not a row), `SessionSettings`, `Session` and
   `SettingsRequest`, the five read sites on `main` parameterized, the
   three task-locals on `main` and their seams deleted
   (`TRAVERSAL_MODE_OVERRIDE`, `RRF_PLAN_OVERRIDE`,
   `STAGE_WRITE_CONCURRENCY_OVERRIDE`; `instrumentation.rs:348`, `:374`,
   `:429` on `main`), the 34 seam call sites across 12 files on `main` and
   the `EnvGuard` sites migrated, a caller census after the move (every
   call site of each moved method, `run_query_at`, `load_jsonl` and
   `load_jsonl_file` included), `from_env()` and its refusal, the grammar
   and AST, the three doors, `Step::Settings` and the `show` step, the
   runner refusal, the hand-maintained docs table. Ships alone: the four
   settings are usable; the `# traversal:` header pin stays the only
   per-case traversal control; `set=` on the two `GET` change routes and
   `--set` on the change verbs are validated and consumed by nothing until
   step 2. `implementation: in-progress`.
2. **The engine setting.** After engine version 2's planner PR merges (the
   frontmatter's `blocked_on`): the `engine` row and field, the two gate
   calls read it, `planner_route()`, `PLANNER_ROUTE_OVERRIDE` and the three
   `with_planner_route` call sites go with `Route::PlannerBehindFlag` and
   `RouteOverride`, the three entries declare `Planner`.
   `implementation: complete`.

Each step is independently safe: step 1 changes no default, and step 2
changes no behavior for a caller that never sets `engine`. If that PR merges
first, the two steps are one PR with the same content. The runner RFC
(several settings assignments per case; evidence that a routed operation
routed) follows step 2.

## Unresolved questions

1. **Stored queries.** Whether `InvokeStoredQueryRequest`
   (`omnigraph-api-types/src/lib.rs:918`) gets the `settings` field now, on
   `POST /queries/{name}` and `POST /queries/{name}/if-graph-commit`. The
   text leaves it out: a stored query is the governed path, and a
   caller-chosen engine there is a policy question first. Decider: the
   maintainers at acceptance review; the text's default holds until then.
2. **Policy.** Whether choosing `engine = v2` should be a Cedar-gated action
   while engine version 2 is new, or is a cost choice any caller may make like
   `merge_lineage` today. The text treats it as the latter. Decider: the
   maintainers at acceptance review; the text's default holds until then.

## Decision log

- 2026-09-16: initial draft.
- 2026-09-16: the definition lives in `omnigraph-compiler`, module
  `settings`, re-exported by `omnigraph-api-types`; `rrf_plan` and
  `ann_nprobes` are `process` diagnostics; the settings-aware operations
  live on `Session` and nowhere else; an invalid or out-of-range `process`
  value is refused at startup; the two `GET` change routes take the `set`
  parameter; a settings prefix applies to the call it heads; a stored
  source carries no settings; `reset all` at the HTTP door and in a `.gqt`
  body resets the `request` settings only.
- 2026-09-17, from the implementation: the traversal mode is out of the
  definition, a field of `SessionSettings` that no row names, set only
  through `SessionSettings::with_traversal` by tests, DST and the `.gqt`
  `# traversal:` header pin; `OMNIGRAPH_TRAVERSAL_MODE` is retired and the
  logic-test runner refuses it beside the rows' variables; the deprecated
  `/read` and `/change` refuse a `settings` field and any `set` or `reset`
  prefix with `SETTINGS_AT_DEPRECATED_ROUTE` instead of ignoring them;
  `show` takes the scope-free `read` authorization `GET /branches` and
  `branch list` take; `reset all` resets every row, a `process` row sitting
  at its baseline at every remote door; the empty file is
  `FileBody::Queries` with no declaration, named by
  `QueryFile::empty_kind()` as `SettingsOnly` or `NoStatement`, and
  `FileBody::Empty` is gone; `set=` on the two `GET` change routes and
  `--set` on the change verbs are validated in step 1 and consumed by
  nothing until `engine`; the served `load` and `ingest` refuse `--set` and
  the embedded ones apply it; the generated user-docs table and its
  `check-docs.py` staleness check are dropped for a hand-maintained table;
  `Session::detach` and `Detached::attach` carry a session across the
  runner's restart; `Session::with_prefix` is the one prefix-on-a-copy door
  and `SettingId::parse_assignment` the one `name=value` door; the
  wrong-kind message names the offending spelling and an overflowing digit
  run is refused as out of range naming it; the stored-query refusal reads
  `a stored query carries no settings; it runs under the process defaults`.
