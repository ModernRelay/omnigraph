# Explicit storage upgrades

**Decides:** a graph changes storage format only through the offline `omnigraph upgrade` command, which runs a compiled conversion route chosen from the source stamp, fences the root with an intent on main before any effect, and activates main last.
**Contract:** keeps E2
**Code:** `crates/omnigraph/src/db/upgrade.rs`, `crates/omnigraph/src/db/upgrade/root_schema.rs`, `crates/omnigraph-catalog/src/migrations.rs`, `crates/omnigraph-cli/src/upgrade.rs`
**Cited as:** RFC 0064

## Context

Normal open refuses a format it does not serve. The other way forward, export
and load, keeps entities but restarts commit history, branches and their
shared ancestry. A conversion keeps them, but it scans retained history and
writes durable state that must survive a crash. Run at open, that cost and
that recovery become startup behavior of every process. The conversion is
therefore an explicit operator step with its own admission, fence and
recovery.

## Decision

- Normal open never converts. It serves the stamps in
  `MIN_SUPPORTED..=CURRENT` and refuses a root that carries a pending
  upgrade intent.
- The binary registers compiled routes and selects one from the source
  stamp: `history-lance-files-v{8,9,13}-to-v18` for those standalone roots,
  and a stamp-only route that raises a served v14 to v17 graph's live refs.
  Any other stamp refuses before effects and names export and load.
  `--to-format` sets a target; it never names a handler.
- `--check` is read-only and reports the route, blocking findings with
  stable codes, and work. Outcomes are `check_passed`, `already_current`,
  `completed`, `check_failed`, `interrupted` and `recovery_required`. A
  check never returns `completed`.
- Execution requires every server, writer and maintenance process stopped;
  a local lock is not proof of exclusion. Preflight runs again under that
  control.
- Before any other effect, an intent on main records the attempt, source
  and target formats, schema contract and every live ref's source version
  and identity, and stamps main with the target so an older executable
  refuses the root.
- Each live ref is converted from its pinned source version and records a
  receipt. Writes are create-only objects and commits with no retry; a
  moved head refuses.
- Every converted ref is validated against its source before main clears
  the pending marker. Clearing it is activation.
- A rerun inspects durable state, resumes its own attempt, reuses finished
  refs and refuses another attempt or unknown ownership. A rerun after
  activation reports `already_current`.
- A stamp 8 or 9 root's contract is read from its root schema files,
  compared with the Lance columns of every table main pins, and pinned by
  the fence. The
  files are left in place.
- Supporting a format as conversion input never widens the serving range.
  There is no reverse route; rollback restores the whole-root backup taken
  before the upgrade.

## Rejected

- **Conversion at open:** turns expensive effects and their recovery into
  startup behavior of every process.
- **Export and load only:** loses commit history, branches and ancestry. It
  stays the route for stamps no handler converts.
- **A separate command per conversion:** duplicates admission and reporting
  and hides the order of prerequisites.
- **A generic migration ledger or scheduler:** resumable chains become a
  second authority, where each handler's own intent and receipts suffice.
- **Converting into a separate destination root:** adds relocation, storage
  and cutover cost, and shared files must outlive the source.

## Not built

- An upgrade of a graph inside a cluster; the CLI refuses one and names a
  qualified cluster operation.
