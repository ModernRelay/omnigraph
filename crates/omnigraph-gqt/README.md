# omnigraph-gqt

GQ describes operations; GQT describes the scenario and expectations; DST
controls execution. This private test package is not shipped in release builds.
Every `.gqt` file under `cases/`, including subdirectories, is one test
in the complete corpus. Test names retain the path relative to `cases/`.
Shared cases live directly under `cases/`; v2-specific cases live under
`cases/v2/`, with plan assertions under `cases/v2/planner/`.
These directories organize the corpus. Every case runs on engine v2, the one
engine; a case's `set engine = v2;` line changes nothing, and discovery never
selects an engine.
The format contract and future extensions live in
[RFC 0045](../../docs/rfcs/0045-gq-logic-tests.md).

## Explicit execution

Every case starts with its existing issue header, followed by required runner,
schema and seed sections. Configuration has no default target, storage, seed
or timeout:

```yaml
--- runner
timeout_ms: 10000
environments:
  - target: omnigraph-engine
    storage: local-filesystem
  - target: omnigraph-engine-dst
    storage: in-memory-object-store
    seeds: [0, 42]
```

The complete scenario executes against a fresh graph for each environment.
These two target/storage combinations are implemented. Direct engine execution
currently refuses seams. Server targets, direct-engine memory storage, cloud
storage and other combinations fail admission explicitly; their names do not
imply implementation or qualification.

One engine instance survives ordinary steps and expected errors. Only
`--- restart` drops the engine and reopens the same storage. A case owns one
session ([Session settings](../../docs/rfcs/2026-09-16-session-settings.md)) for
its lifetime: a `--- mutate` step of only `set` and
`reset` lines expects `ok` and changes that session for the steps that follow,
across a restart; a `set` prefix before any other body applies to that step
only; `show <name>` and `show all` are `--- query` rows steps with the five
`String` columns `name`, `value`, `default`, `source`, `scope` in definition
order, and that shape is derived, so a `show` step writes no `--- expect shape`
section; a `process` setting in a case body refuses the case at parse time. GQ operations
and GQT rows, shape, affected counts, errors and loop semantics are shared by
both paths. Unordered row comparison preserves duplicate counts.

The configuration accepts 1–16 distinct environment parameter sets,
1–600000 milliseconds, and 1–64 distinct unsigned 64-bit seeds per DST
environment. The required corpus refuses budgets above 10000 milliseconds;
longer standalone reproductions must be selected deliberately. Unknown, duplicate, missing and inapplicable YAML fields are
refused, as are aliases, anchors, merge keys and tags. Each DST seed runs twice
in fresh worker processes; completed assertion failures also replay and later
seeds still run within the file budget. Matching replay of a failing graph
assertion remains a failure unless it meets the explicit known-failure contract below.

## Seam placement

Place a seam directly before its mutate operation, a GQ mutation or a branch
statement; no seam is crossed by a query step yet. Several seam blocks may
precede one operation when they name distinct seams (contention at
publication and a lost acknowledgement on one mutation,
`cases/mutation_contention_and_lost_ack_survive_reopen.gqt`); each carries
its own delivery record, and the same seam twice before one operation is
refused. The one exception is the store: at most one directive per step may
act on the store, as a store place or as a store action on a decision seam; a
second is refused at admission with `unsupported_environment: one store
action per step`:

```yaml
--- seam
at: branch_merge.post_authority_capture
occurrence: 1
action: fail
scope: next_step
```

The four fields `at`, `occurrence`, `action` and `scope` are required, and
`subject` is optional. `at` names a decision seam in the engine's catalog
(`omnigraph::seams::catalog`, RFC 0066) or a store place in `STORE_PLACES`
(`omnigraph-dst`); a store place requires `subject`, a glob over the object's
root-relative name (RFC 0066 §Design Subjects), and a block whose catalog
entry or row declares no subject refuses it; a decision seam that declares a
store effect carries its own subject, which a case does not restate. Quote a
subject that begins with
`*`: the case reader refuses an unquoted leading `*` as a YAML alias; a
quoted subject is read as one scalar, so globset's `{a,b}` and `[!x]` forms
are fine inside the quotes. A store
place is admitted before a mutate or branch step (a branch create writes
nothing through the adapter, so a store place before it is `seam_unobserved`). `action` is `fail`,
`contention`, `skip`, or a store action, the first being `misdirect`, with
`lose`, `error`, `corrupt` and `delay` spellable and refused at admission
naming the table row until admitted. `fail` selects the fail effect when declared,
otherwise contention for compatibility with existing cases. `contention`
selects only contention, so a seam declaring both failure effects lets a
case choose either. `skip` selects only skip; an undeclared effect is
refused. `hold` is refused until concurrent steps exist. A seam declares one
effect when it sits between two steps and several when it wraps one
operation and can declare both fail and skip. A `fail` action on a
contention-only seam, or an explicit `contention` action, injects a retryable
error that the publisher retries, so the step succeeds and the `seam_delivered` record is its only
proof; on a seam declaring fail the step states the injected error in its
`--- expect error:` row, unless the site swallows the failure by design (the
lost acknowledgement after a publication, `publish.post_merge_pre_ack`, which
the publisher's read-back recognizes as success, so the step succeeds and the
delivery record is the proof,
`cases/mutation_contention_and_lost_ack_survive_reopen.gqt`). A `skip` action carries
the healthy expectation the skipped path produces; a lost durable write is
then proven healed by a `--- restart` and the query after it. The occurrence counts
crossings inside that operation, including production retries; setup and
preceding operations cannot consume it. The installed decision is removed
before the next operation or restart. Seam directives inside loops are
refused. GQT does not add retries.

A seam is admitted when its catalog operation matches the step it precedes:
`mutation` before a mutate, `branch_merge`/`branch_create`/`branch_delete`
before the matching branch statement, `any_write` before either; a seam of
operation `unreachable` is refused. Occurrences must be 1–1000000. Delivery
is proven by the runner's own decision: it counts crossings, fires the
admitted effect on the declared occurrence, and the report carries
`seam_delivered` with the seam, the occurrence, the crossings, the effect,
`declared_at` (the file and line of the seam's static, beside the code it
guards) and `fired_at` (the helper call whose crossing fired); an unfired seam fails
with `seam_unobserved`. Text in data or an error cannot satisfy this check.

There is no `--- fault` section: a fault injected at the object store is a
`--- seam` naming a store place or a decision seam that declares a store
effect, and a `--- fault` section is refused with a pointer to `--- seam`.
The store places, their methods and the actions each honors are RFC 0066's
table; a place-and-action pair outside it, or listed but not implemented, is
refused at admission naming the table row. An old `--- fault` block
converts by renaming the section and its `return_error` action to
`action: fail`; `at`, `occurrence` and `scope` keep their names. Process crashes,
server/CLI sessions and network simulation are future extensions.

## Concurrent block

A `--- concurrent` section runs two to four labeled GQ statements at the
same time on the case's one handle, in the order its `order:` line names;
it is one step of the case and runs under the DST runner only.

```text
--- concurrent
w1: query add_bob() { insert Person { name: "bob" } }
r1: query all() { match { $p: Person } return { $p.name } }
order: w1 park put __manifest/_versions/, r1 start, r1, w1 put __manifest/_versions/
--- expect
w1: ok
r1: ok
```

A session is `<label>[ on <branch>]: <statement>` at column 0, continued on
the lines that follow: one query or mutation declaration with no parameters,
on `main` unless `on <branch>` says otherwise. Every session is a `Session`
over the case handle, so the sessions share its in-process locks the way a
server's requests do. `order:` closes the block: comma-separated entries,
each `<label> start` (the session's statement begins; the runner holds it
before that), `<label>` (the session's completion, reported as `done`),
`<label> <verb> <key-suffix>` (the session's next store request of that
verb whose key contains the suffix, run in turn; the cursor moves past the
entry when the request completes: a multipart write when its upload
completes or aborts, a listing when its first item or end arrives, a `copy`
is named by its destination key) or `<label> park <verb> <key-suffix>` (the
session arrives at that request and is held there, inside whatever the
engine holds at that point, until its next entry is due; arrival moves the
cursor; the entry after a park for its label is that request without
`park`, or the label's completion). Verbs are `get`, `head`, `put`, `list`,
`delete` and `copy`. Labels are `[a-z][a-z0-9]*`; `setup`, `runner`, `step`
and `order` are reserved. A request no entry names runs at once, and a
request made outside any session's future (a Lance pool thread, a task the
engine spawned) can never be named. The `--- expect` after the block is
bare, one `<label>: ok` or `<label>: error: <needle>` line per session;
rows are not compared inside a block.

While a session waits on the script the block drives the paused clock
itself (RFC 0045 §Concurrent block says why) up to ten virtual seconds past
the last cursor move or request; past that the clock stands still and the
block fails as starved when neither an entry nor a request arrives for half
the case's `timeout_ms` of wall time, at most ten seconds, or when a
session finishes without a request entry of its own. A session blocked in
the engine while no session waits on the script ends as the case's
timeout. The example above is the read-during-publish case: on an engine
whose publish holds the schema gate and the handle's coordinator lock
across the manifest commit, `r1` waits behind `w1` (at the schema gate,
before it reaches the coordinator lock) and the block starves at entry 3;
on an engine that installs the published state after the commit, it
passes. An entry naming a request the engine makes while holding a lock
another session needs starves the block too: the writer's manifest `put` is
safe to park at, a read's `__manifest` listing is under the coordinator read
lock and is not, which is what `start` is for. The evidence row
`concurrent_block` carries each session's outcome, the entry the cursor
stopped at and the block's failure, replay-compared; the grant sequence
with its wall times and the count of requests made outside any session are
in the report's measurements, not the cost table. Under `--measure` each
session is its own row, labeled by its session label, and the block's own
row holds the requests made outside any session, so a read's virtual time
beside a write is a number. Once a block is starved its sessions drain, and
the requests they make from then on are in phase `after_abort`: a starved
block's rows are the drain, not the interleaving the script named. Store
requests are the only points an entry can name in this version; seams and
in-process gates as entries, control statements as sessions and sessions on
separate handles are later extensions.

## Plan expectations

A query step may carry an `--- expect plan` section directly after its
`--- expect shape`. Each line asserts one fact of the query's v2
plan, obtained from the engine's explain document before row
comparison, without executing the query a second time:

```text
--- expect plan
scan Doc as $d: columns [__id, slug, state]
scan Doc: not columns [embedding]
scan Doc as $d: filter reads [d.state]
scan Doc as $e: no filter
hash join $e
expand $d Knows $e: mode indexed_scan
filter reads [d.rank, e.rank]
sort tiebreak [$d, $e]
pass projection_pushdown
not pass aggregate_pushdown
```

A `scan <Type>[ as $var]:` line selects the scans of that type (or the one
bound to that binding) and claims one fact of each: `columns [..]` the exact
columns it projects; `not columns [..]` columns it must not read; `filter
reads [..]` a pushed filter reading exactly those columns (`binding.property`);
`no filter` no pushed filter at all. `filter reads [..]` on its own states that
an in-memory `Filter` node stays in the plan reading exactly those columns.
`sort tiebreak [$a.@id, $e.@type, $e.@id]` states the exact ordered metadata
keys a physical `Sort` appends after user order keys. `$a` abbreviates `$a.@id`;
`sort no tiebreak` requires an empty list. `rank fuse row tiebreak [...]` checks
the exact downstream keys of `RankFuse`, and `rank fuse no row tiebreak` requires
none. Dropping a type key or swapping key order fails these assertions.
`pass <name>` states that a named optimizer pass fired, `not pass <name>` that
it did not. Projection/read lists are sets; identity keys and selection members
are ordered lists. A mismatch prints the whole explain document.
Pass names must be registered optimizer passes. Excluded columns must
exist in the selected type's catalog schema. Unknown names fail even in
negative assertions. Assert destination projection on the dependent scan;
`Expand` carries topology alone.
`expand $a $b: selection alternation [Knows out, Likes in]` checks the exact
resolved member list and per-member directions. Selection kinds are `named`,
`alternation` and `wildcard`; `wildcard []` checks an empty selection. Types use
canonical catalog names, with JSON quotes available for a member name. An
endpoint-only `expand $a $b: mode indexed_scan` applies to every Expand between
those bindings, including selections.
An `expand $src <Edge> $dst:` line selects every matching named-edge physical `Expand` between
those bindings over that edge type and claims `mode csr` or `mode
indexed_scan`, the traversal mode the planner recorded (pass `expand_mode`
when the cost model chose it); it fails when no such expand is in the physical
plan or its mode differs. A `scan <Type>[ as $var]: access id_lookup` line
selects the physical scans of that type (or the one bound to that binding)
and claims each is a traversal's destination read once per slice of at most
256 input rows; it fails when no such scan is in the physical plan, the scan
is a table scan (no access path) or the build side of a hash join. A `hash
join $var` line claims that a physical `HashJoin` reaches `$var`'s rows by
reading its table once as the build side the traversal probes (pass
`access_path` when the cost model decided); it fails when the plan holds no
hash join over that binding. The `hash join` and `expand` lines take a
trailing `ran <side>` (`hash join $e ran id_lookup`, `mode indexed_scan ran
csr`): the side of the node's declared switch the run took, read from the
execution report of the same run (the last attempt of the node's row, joined
to the explain row by the node's `id`); it fails when the run recorded no
side on that node or another side. A `scan <Type>[ as $var]: ranked
<nearest|bm25>[ fetch <n>][ nprobes <n>]` line claims the ranked access path
of the scan, the candidates it asks the index for and, on a `nearest` scan,
the probe cap the plan carries (`0` spells no cap, as the `ann_nprobes`
setting does). Nothing is compared as rendered text, so a planner that
reaches the same facts by another route keeps the case green.

## Reference comparison

A query step may end with `--- expect same as v1`, directly after its
`--- expect shape` or `--- expect plan`; the section has no body. The runner
runs the step's query again on a copy of the case session carrying the frozen
engine v1 (`omnigraph_reference_engine::ReferenceEngine`, the crate
`omnigraph-reference-engine`, installed through `Session::with_read_executor`)
and compares its rows with v2's, ordered or unordered as the step's
`--- expect` says. The step's own rows expect still applies; the comparison is
added to it.

```text
--- expect unordered
--- expect shape
p.name: String
--- expect same as v1
```

A v1 error, a v1 gate refusal included, or a row difference fails the step.
The section is refused on a mutate step, an error expect, `show` and
`branch list`. The DST runner skips the comparison. The reference answers
`not { ... }` blocks only among the correlated blocks, and refuses count
predicates and a string `nearest` argument, so a step using those carries no
`expect same as v1`.

## Run and reproduce

Run the complete package (the workspace Cargo configuration enables seeded
Tokio from any directory; the commands below use crate-relative paths):

```bash
cd crates/omnigraph-gqt
cargo test -p omnigraph-gqt --locked
cargo test -p omnigraph-gqt --test gq_logic_tests -- --list
cargo test -p omnigraph-gqt --test gq_logic_tests v2/planner/
cargo test -p omnigraph-gqt --test gq_logic_tests -- --test-threads=2
cargo run --bin omnigraph-gqt -- cases/dst_restart_preserves_rows.gqt
cargo run --bin omnigraph-gqt -- cases/dst_restart_preserves_rows.gqt --target omnigraph-engine-dst --storage in-memory-object-store --seed 42
cargo run --bin omnigraph-gqt -- --replay ../../target/gqt-artifacts/invocation-EXAMPLE.json
cargo run --bin omnigraph-gqt -- cases/dst_restart_preserves_rows.gqt --measure
```

`--measure` records, for every step of each DST environment, the object-store
requests made while the step ran (the engine's, and under a `--- store` rule
the fault wrapper's own), in both of the engine's realms. The Lance
realm: the measuring store is a decorator on the engine's
`object_store_seam`, the seam the DST fault decorator uses, so every store the
registry builds is wrapped, `__manifest` and table traffic alike. The control
realm: the schema contract (`_schema.pg`, `_schema.ir.json`,
`__schema_state.json` and their `.staging` twins), the init claim and probe,
the legacy `__recovery/` listing and the graph-index artifact go through the
engine's `StorageAdapter`, whose DST store is a second in-memory object store
the registry never builds; the worker wraps the adapter it hands the engine
and logs each call as the requests the in-memory adapter makes for it: a text
read one `get`, a bounded read one `get` of `0-(max+1)`, a write one `put`,
a conditional write the store refused `put_failed`, an `exists` one `head` (a
miss `head_failed` and the `list` of the prefix that follows), a rename a
`copy` and a `delete`, a directory listing one `list` per page of the entries
it returned (a bounded listing walks nested and unmatched entries it does not
return; those are not paged). The engine is not edited. The ledger keeps every request of the run, tagged with the label
current when it was made: `setup` before the first step, `step` N while step
N runs, `runner` N from its end to the next step (the runner's own checks);
the runner only moves the label, and the report is the ledger grouped by it,
so the rows add up to every request either store saw (`slot` column; the
control realm's limits below are the exceptions). Per group it reports:

- `requests`, the work, and `repeat_reads`, the `get`s and `head`s of an
  object and byte range the group had already read (the same bytes paid for
  twice; objects are told apart by their real names, uuids included, and a
  control object never meets a Lance object of the same name);
- `after_publish`, the requests of either realm after the group's last
  `__manifest` version put, the publish CAS: the crash window, where a crash
  leaves a published operation unfinished (a schema apply's contract write
  after its publish is in it); absent when the group published nothing;
- a count per `<realm>_<kind>.<verb>` class (`manifest_meta.put`,
  `table_data.get`, `control_schema.head`, …) where the realm is the dataset
  (`__manifest`, the table, the recovery root) and the kind its Lance
  directory, or `control` and the kind the object's role (`schema`,
  `recovery`, `graph_index`, `claim`, `probe`, `manifest` for the adapter's
  probe of the `__manifest` root, `other`); a request the store refused
  counts under `<verb>_failed`, for every verb (`get`, `head`, `put`,
  `put_part`, `put_multipart`, `put_complete`, `put_abort`, `copy`,
  `delete`, `list`); a `list` counts one request per 1,000 keys, a multipart
  upload the create, one request per part and the complete or abort, the
  shapes S3 bills;
- the schedule in the work-span model: the measuring store sleeps the
  request's cost on the paused DST clock, so requests the engine issues
  together overlap and a group's `makespan` (ticks of one millisecond, from
  its first request's start to its last request's end) is the time its
  schedule took under the model; the in-memory store alone answers
  synchronously and would make every request its own tick;
- its phases, from the engine's own decision seams: measure mode installs a
  pass-through observer on every empty seam of the engine catalog, and a
  request's phase is the last seam the engine crossed in the step (`start`
  before the first crossing), so the report names `mutation.post_stage_pre_effect_gate`,
  `fork.before_classify`, `mutation.post_finalize_pre_publisher`,
  `publish.load_state`, `publish.pre_merge`, … in the order they were crossed;
  nothing is inferred from the log's shape. A `--- seam` directive takes its
  seam back for its step and the observer returns after it. Each phase has
  its makespan, its `span` (the critical path with the phase's tables side by
  side: the phase's non-table requests in series plus the longest single
  table's time; a table request is one in a table's realm, so a branch's own
  `__manifest` lineage under `__manifest/tree/` is never a table), its
  requests and tables; the step's `span` is the sum of the phases' spans,
  the critical path under "phases in sequence, tables independent within a
  phase", so `waiting` (makespan minus span) is the time tables spent in
  series that the model says they need not; every schedule column is the
  model's time in ticks, a request spanning its start to its end, so they
  share one unit under every model, and the printout shows the parallelism
  achieved (requests per makespan tick) beside the parallelism available
  (requests per span tick);
- `sim ms`, the group's virtual time under the latency model, and `u$`, its
  requests at S3 list prices in microdollars (PUT, COPY and LIST 5.0, GET and
  HEAD 0.4, DELETE free).

Every label the runner set is a row, so a step that made no request (a
settings step, a no-op mutation before its first table) reports zero, and
zero stays distinct from missing.

`--model <name>` picks the latency model, what one request costs on the
virtual clock: `unit` (the default: one tick per request, the model every
count is stated under), `s3-like` (17 ms per request, the slope measured on
the branch-age chart, plus the bytes at 50 MiB/s, Durner et al., until a
calibration run on the real store replaces both) or `local` (0.1 ms plus the
bytes at 1 GiB/s). The bytes are charged when they are known: a write's and
a bounded read's before the call, a whole-object or offset read's after it,
once the result says how much came back. Requests still overlap under every
model; there is no per-device queue, since a serialized queue would make the
makespan equal the request count and hide the overlap the schedule columns
exist to show.

The request counts (`requests`, `repeat_reads`, `after_publish`, the class
counts) are an `io` evidence row per group, so the two runs of one seed must
agree on them; the schedule-derived numbers (`makespan`, `span`, the phases,
`sim ms`, `u$`) are measurement-only, since a detached commit's overlap moved
by one tick between two runs of one seed (in the seed load and in a step), a
determinism gap of the write path under DST, not of the counting; bytes and
the uuid-redacted request log (each request with its start tick, so requests
sharing a tick ran together) go to a `measurements` field the replay
comparison skips. The invocation prints one ASCII table per environment, a
row per step and per gap and a row per phase, and writes the long-form TSV
under `target/gqt-artifacts/cost/` (phase rows as `phase.<name>.<field>`).
Direct-engine environments record nothing: on a `file` root Lance bypasses
the wrapped store for data files, so only the DST in-memory object store
sees every request. The control realm's counts are the in-memory adapter's:
under DST its store never holds a Lance object, so the engine's probes of a
dataset root through the adapter (init's `__manifest` preflight, schema
apply's leftover-dataset probe) always miss, `head_failed` then `list`, and
the `delete_prefix` reclaim behind the second is never reached (it is logged
as its listing alone, the deletes that follow being as many as it found); an
`exists` the store refused is `head_failed` whether the head or the list
after it failed. An adapter the engine builds for itself instead of using
the handle's (`ensure_no_pending_recovery`, the storage upgrade, the
graph-index load of a historical read) is outside the wrapped one; no gqt
step reaches one today. A contract file probed then read in one step is a repeat
read, `head` and whole-object `get` sharing a key, and so is every load of
the contract after a step's first (a write's revalidation after its capture).
Every case measures the same way; nothing in a case
declares it, and the invocation takes several case paths or directories,
anywhere on disk. `--artifacts <dir>` puts the report and the TSV in that
directory instead of the build tree's `target/gqt-artifacts/`, so a suite
kept outside this repository keeps its outputs beside its cases.

**Baseline.** `--baseline <path>` names a TSV, anywhere on disk, with one
row per case, environment, seed, slot and step and the three counts
`requests`, `repeat_reads` and `after_publish`; the repository commits no
such file, and without the flag a measured run prints its table and
writes its TSV without a delta. A case is named by its path under the
corpus whichever spelling the invocation used (a case outside the corpus
by its absolute path). `--write-baseline` (with `--baseline`) replaces,
for each measured case, the rows of the environments and seeds the run
measured and keeps every other row, so a `--target` or `--seed` selection
rewrites only what it ran; a run that failed writes nothing and says so;
rows of a case the run did not measure stay, so after a case is renamed or
removed, delete the file first and let a run over the directory rebuild
it. With `--baseline` alone the run prints each case's delta against the
file: a mutate, control, settings or restart row at any change, a query,
show or list row past two requests or five percent, whichever is more, or
at a changed crash window, plus the rows the baseline lacks and, within
the measured environments and seeds, the rows the run lacks. The delta is
a report, never a failure. A baseline that cannot be read or parsed fails
the invocation, and its saved report says so.

`--target`, `--storage` and `--seed` only narrow declared executions; all supplied
filters must match. There are no environment IDs or runner format versions.
Reports identify each environment by its full declared parameters. Unknown selections
fail; a selected subset is recorded explicitly. A workspace-root build without
seeded Tokio refuses any selected DST environment. The required GQT CI context
owns both this unavailable-build check and the entire configured corpus; Test
Workspace explicitly excludes this separately tested package.

The supervisor freezes the case bytes and selection before graph setup. Workers
consume that input rather than reopening the case. The executable digest and a
build-time source revision/content digest bind reproduction to the tested build.
Each worker verifies those identities. Workers start with a cleared environment
and explicit pool/entropy settings, preserving no inherited fault configuration.

Every file invocation retains a JSON summary under workspace
`target/gqt-artifacts/` and prints its path and replay command. Reports include
frozen inputs, environment/seed/attempt attribution, assertion evidence,
executed Arrow column types and rows, mutation counts, fault delivery, lifetime
events and DST main snapshots. Failed assertions retain actual evidence too.
Replay compares the recorded observations; it does not claim identical internal
I/O or complete unqueried graph state. A changed executable or mismatched report
fails. Direct-engine execution has no deterministic replay guarantee.

Case input, worker reports and the terminal summary each have a 16 MiB limit.
Observation collection also has a 100000-event limit; exceeding a bound fails
explicitly. Missing or unwritable reports fail the invocation. The file timeout
covers preflight and all selected worker attempts; overdue workers are killed
and reaped within a separate 250 ms containment allowance. Failure to confirm
containment stops further dispatch and retains the worker context. Remaining
attempts are accounted for when execution cannot continue. The supervisor uses
synchronous local-file reads and report writes; these host I/O operations are
not interruptible by its worker deadline.

For case invocations, `OMNIGRAPH_GQ_BLESS` accepts `1` to enable rewriting;
unset, empty, or `0` leaves it disabled. Other values, including non-UTF-8
values, produce an `invalid_case` report before execution or rewriting.
Replay ignores this variable and refuses saved blessing invocations.

`OMNIGRAPH_GQ_ENGINE` names the initial `engine` setting for direct and
DST case sessions, the baseline `reset engine` restores. It accepts only `v2`,
empty or unset, all three meaning engine v2, the setting's one value; engine
v1 is reached only through `--- expect same as v1`, never through this
variable.

Invocation reports freeze the engine in the worker input before clearing the
worker environment, and replay uses the recorded value. An input without the
field means `v2`. Engine input is covered by the report's input digest, and
the executable and source identity checks still apply. Any other value, `v1`
and non-UTF-8 values included, produces an `invalid_case` report before
workers start, including on replay.

`OMNIGRAPH_GQ_BLESS=1` is supported only for a case declaring one direct-engine
environment. A subset selection cannot bless a multi-environment case. It rewrites a failing row or shape expectation and still returns
failure until a subsequent run confirms it. DST cannot bless. The legacy
`OMNIGRAPH_GQ_CASE_TIMEOUT_SECS` helper applies to library mechanism tests;
file invocations refuse that ambient override and take their timeout from the
runner section. Ambient fault, entropy and pool overrides also refuse
admission, including replay, as does a set settings variable
(`OMNIGRAPH_ENGINE`, `OMNIGRAPH_RRF_PLAN`, `OMNIGRAPH_MERGE_LINEAGE`,
`OMNIGRAPH_ANN_NPROBES`, `OMNIGRAPH_LOAD_CONCURRENCY`,
`OMNIGRAPH_TRAVERSAL_WORK_LIMIT`) and the retired
`OMNIGRAPH_TRAVERSAL_MODE`, which names no setting any more. A case session
never reads the environment (the runner's own `OMNIGRAPH_GQ_ENGINE` above is
the one seed), so neither variable decides anything; the refusal keeps a stale
one in a CI environment from being mistaken for a live control, and keeps the
retired name from lingering. A case that must run one value writes it in a
`set` step.
