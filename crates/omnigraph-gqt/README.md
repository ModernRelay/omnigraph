# omnigraph-gqt

GQ describes operations; GQT describes the scenario and expectations; DST
controls execution. This private test package is not shipped in release builds.
Every `.gqt` file under `cases/`, including subdirectories, is one test
in the complete corpus. Test names retain the path relative to `cases/`.
Shared cases live directly under `cases/`; v2-specific cases live under
`cases/v2/`, with plan assertions under `cases/v2/planner/`.
These directories organize the corpus. Cases that require v2 still select it
explicitly with `set engine = v2;`; discovery never changes the engine.
The format contract and future extensions live in
[RFC 0045](../../docs/rfcs/0045-gq-logic-tests.md).

## Explicit execution

Every case starts with its existing issue header, followed by required runner,
schema and seed sections, with an optional `known_failure` section after runner.
Configuration has no default target, storage, seed
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
precede one operation when they name distinct seams (two lost writes on one
mutation, `cases/issue_602_stale_sidecar_heals_on_reopen.gqt`); each carries
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
operation, so `mutation.sidecar_confirm_put` (effects fail and skip) takes
either action from a case with no engine change
(`cases/mutation_sidecar_confirm_put_failure_rolls_back.gqt`,
`cases/issue_602_stale_sidecar_heals_on_reopen.gqt`). A `fail` action on a
contention-only seam, or an explicit `contention` action, injects a retryable
error that the publisher retries, so the step succeeds and the `seam_delivered` record is its only
proof; on a seam declaring fail the step states the injected error in its
`--- expect error:` row. A `skip` action carries the healthy expectation the
skipped path produces; a lost durable write is then proven healed by a
`--- restart` and the query after it
(`cases/issue_602_stale_sidecar_heals_on_reopen.gqt`), or, while the defect
stands, pinned by a `--- known_failure` marker on that restart. The occurrence counts
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
concurrent steps, server/CLI sessions and network simulation are future
extensions.

## Known recovery failures

An optional `--- known_failure` section directly after `--- runner` and before
`--- schema` can retain a known recovery defect in the corpus:

```yaml
--- known_failure
step: 4
match:
  error: RecoveryRequired
  reason: "the exact OmniError::RecoveryRequired reason"
```

`step` and `match` are required. The step is a positive operation ordinal.
The typed matcher supports `error: RecoveryRequired`, with a nonempty exact
`reason` of at most 2048 bytes, on a mutate step, and `error: Internal`,
with a nonempty `reason_prefix` of at most 2048 bytes, on a `--- restart`
step whose reopen is refused by an `Internal` manifest error (the prefix,
because the refusal embeds a per-run operation id). Unknown error names,
including `Unknown`, and extra fields are refused. The old `--- fixme`
syntax is refused. Use the existing `# issue` and `# notes` headers for
issue identity and context; `# issue: none` remains valid when no issue has
been assigned. This marker admits only engine-DST/in-memory cases without
loops, targeting an ordinary mutate with its healthy `ok` or `affected`
expectation or a restart. At least one seam must precede that step; no seam
may occur at or after it.

Every preceding assertion must pass, and each declared seam must have its
delivery record at its declared operation. The marked assertion must fail
because that mutate returned the typed `OmniError::RecoveryRequired` variant
with exactly the declared reason, or that reopen returned an `Internal`
manifest error opening with the declared prefix. An operation ID is retained
in raw evidence but is not part of the marker. Text in rows, another error
variant, another step, a changed reason, or missing seam evidence cannot
match.

The runner keeps the healthy assertions unchanged and stops at the failing
step; later steps remain unexecuted. Every selected seed and its mandatory
fresh replay must satisfy the marker and match the complete raw reports.
Accepted attempts carry `known_failure: true`; the summary code is
`known_failure`, and stdout prints `KNOWN_FAILURE`. This is an accepted known defect,
not a genuine passing scenario. Partial selection remains `scope: partial`.
An unexpectedly passing execution fails with `unexpected_pass`, requiring the
stale marker to be removed. Setup, panic, timeout, cleanup and report failures
are never waived. Replay re-derives marker acceptance and refuses forged
status. Blessing a marked case is refused.

## Plan expectations

A query step may carry an `--- expect plan` section directly after its
`--- expect shape`. The query's effective engine must be `v2`, selected by
the runner baseline, a case settings step, or the query's `set engine = v2;`
prefix. Under `v1` the harness fails before executing or explaining the
query: v1 produces no plan. Each line asserts one fact of the selected v2
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
`sort tiebreak [$a, $b]` states that a physical `Sort` declares exactly those
bindings' ids as the keys it appends after the order keys, `sort no tiebreak`
that a `Sort` declares none.
`pass <name>` states that a named optimizer pass fired, `not pass <name>` that
it did not. Every list is a set. A mismatch prints the whole explain document.
Pass names must be registered optimizer passes. Excluded columns must
exist in the selected type's catalog schema. Unknown names fail even in
negative assertions. Assert destination projection on the dependent scan;
`Expand` carries topology alone.
An `expand $src <Edge> $dst:` line selects every matching physical `Expand` between
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
```

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

`OMNIGRAPH_GQ_ENGINE` selects the initial `engine` setting for direct and
DST case sessions, and the baseline `reset engine` restores. Unset, empty,
or `v1` selects the executor; `v2` selects the plan route. CI runs the corpus
under both routes. A case's `set engine = …;` overrides this baseline like
any other setting. `--- expect plan` requires the query's effective engine
to be `v2`; a query prefix or case setting can override the runner baseline.
Plan-specific corpus cases explicitly select v2, while shared row cases
inherit the baseline and run on both routes.

Invocation reports freeze the selected engine in the worker input before
clearing the worker environment. Replay uses that recorded engine even if
`OMNIGRAPH_GQ_ENGINE` now selects the other route. Reports whose input has no
engine field mean `v1`; v1 reports continue to omit the field. Engine input
is covered by the report's input digest, and the executable and source
identity checks still apply. Any other value, including non-UTF-8 values,
produces an `invalid_case` report before workers start, including on replay.

`OMNIGRAPH_GQ_BLESS=1` is supported only for a case declaring one direct-engine
environment. A subset selection cannot bless a multi-environment case. It rewrites a failing row or shape expectation and still returns
failure until a subsequent run confirms it. DST cannot bless. The legacy
`OMNIGRAPH_GQ_CASE_TIMEOUT_SECS` helper applies to library mechanism tests;
file invocations refuse that ambient override and take their timeout from the
runner section. Ambient fault, entropy and pool overrides also refuse
admission, including replay, as does a set settings variable
(`OMNIGRAPH_ENGINE`, `OMNIGRAPH_RRF_PLAN`, `OMNIGRAPH_MERGE_LINEAGE`,
`OMNIGRAPH_ANN_NPROBES`, `OMNIGRAPH_LOAD_CONCURRENCY`) and the retired
`OMNIGRAPH_TRAVERSAL_MODE`, which names no setting any more. A case session
never reads the environment (the runner's own `OMNIGRAPH_GQ_ENGINE` above is
the one seed), so neither variable decides anything; the refusal keeps a stale
one in a CI environment from being mistaken for a live control, and keeps the
retired name from lingering. A case that must run one value writes it in a
`set` step.
