# omnigraph-gqt

GQ describes operations; GQT describes the scenario and expectations; DST
controls execution. This private test package is not shipped in release builds.
Each top-level `cases/*.gqt` file is one test in the complete corpus.
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
`--- restart` replaces it, reopening the same storage. GQ operations and GQT
rows, shape, affected counts, errors and loop semantics are shared by both paths.
Unordered row comparison preserves duplicate counts.

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
publication and a failure before promotion on one mutation,
`cases/mutation_pending_pin_survives_reopen.gqt`); each carries
its own delivery record, and the same seam twice before one operation is
refused:

```yaml
--- seam
at: branch_merge.post_authority_capture
occurrence: 1
action: fail
scope: next_step
```

All four fields are required. `at` names a decision seam in the engine's
catalog (`omnigraph::seams::catalog`, RFC 0066); `action` is `fail`,
`contention`, or `skip`. `fail` selects the fail effect when declared,
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
promotion after a mutation's publication, `mutation.post_publish_pre_promotion`,
where the step succeeds and the delivery record is the proof,
`cases/mutation_pending_pin_survives_reopen.gqt`). A `skip` action carries
the healthy expectation the skipped path produces; a lost durable write is
then proven healed by a `--- restart` and the query after it, or, while the
defect stands, pinned by a `--- known_failure` marker on that restart. The occurrence counts
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

`--- fault` is reserved for storage-boundary faults (a separate amendment)
and is refused today with a pointer to `--- seam`. An old `--- fault` block
converts by renaming the section and its `return_error` action to
`action: fail`; `at`, `occurrence` and `scope` keep their names. Process crashes,
concurrent steps, server/CLI sessions and network simulation are future
extensions.

## Known recovery failures

Since RFC 0067 no corpus writer arms a recovery intent, so no case can
record a known recovery failure; the marker stays parsed and refused as
documented here until the recovery classifier retires with the last sidecar
writer.

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

## Run and reproduce

Run the complete package (the workspace Cargo configuration enables seeded
Tokio from any directory; the commands below use crate-relative paths):

```bash
cd crates/omnigraph-gqt
cargo test -p omnigraph-gqt --locked
cargo test -p omnigraph-gqt --test gq_logic_tests -- --list
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

`OMNIGRAPH_GQ_BLESS=1` is supported only for a case declaring one direct-engine
environment. A subset selection cannot bless a multi-environment case. It rewrites a failing row or shape expectation and still returns
failure until a subsequent run confirms it. DST cannot bless. The legacy
`OMNIGRAPH_GQ_CASE_TIMEOUT_SECS` helper applies to library mechanism tests;
file invocations refuse that ambient override and take their timeout from the
runner section. Ambient fault, entropy, pool and traversal overrides also refuse
admission, including replay.
