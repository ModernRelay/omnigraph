---
rfc: "2026-11-21-cross-version-compatibility-matrix"
title: "Cross-version compatibility as a matrix over a support window"
track: maintainer
status: draft
implementation: not-started
authors:
  - ragnorc
created: 2026-11-21
updated: 2026-11-22
discussion: null
supersedes: []
superseded_by: []
blocked_on:
  - "PR 1507 (the predecessor registry this RFC builds on) merged"
---

# RFC: Cross-version compatibility as a matrix over a support window

> A term set in ***bold italics*** is being defined at that exact spot.

Contract: changes Boundary, Versions; keeps `E1`, `E5`; leaves `D23` and `D26`
open.

## Summary

The current binary serves storage formats 14 to 18. Formats 14 to 17 were
written by the releases from 0.12.0 to 0.16.0, and no test runs a genuine
writer of any of them. The genuine cross-version tests that do run cover
formats the current binary refuses (v3 to v6), routes into the window from
below it (stamps 8, 9 and 13), and one dependency transition (Lance 11). Each
is a separate journey with its own workload, chosen when its format was new.

This RFC makes cross-version coverage one ***matrix***: the rows of the
predecessor registry (`.github/predecessors.toml`, PR 1507) against the routes
the current binary takes, with one workload and one set of checks per route.
A row declares its binary's version on each compatibility surface it is
tested on, and provisioning proves each one. The current
binary's own support range selects the route, so no table copies it. A checker
requires a row for every release that writes a format in the window, so a
missing writer is a red check, not a gap found by reading.

The GQ language is a second surface of the same matrix, with the same shape:
a row says which GQ language version its binary parses, the candidate's own
parser and lint pick the route, and a difference passes only when a
sanctioned-difference table names it. It adds no other mechanism.

The RFC also narrows the window's lower edge from a conversion route to a
bridge. Graphs below format 14 are refused with the name of the ***bridge
release***, the newest release whose `omnigraph upgrade` converts them (0.16.x
today). The in-place 8, 9 and 13 conversions retire from the current binary.
Reads of history that a past conversion left under `__history/legacy/` stay.

Unchanged: the storage format, every route inside the window, the refusal of
formats outside it, the rebuild path, and the explicit-upgrade protocol of
[RFC 0064](0064-explicit-storage-upgrades.md), whose required compatibility CI
this RFC specifies.

## Motivation

Facts measured on main at `291214830`, 2026-11-21:

| Release | Writes | Genuine writer in CI |
|---|---|---|
| 0.12.0, 0.13.0 | v14 | none (0.12.0 is a row only for an ignored ledger journey) |
| 0.14.0, 0.15.0 | v16 | none (v16 is reached only by the `lance-11` main build) |
| 0.16.0 | v17 | none |
| current (0.16.1 workspace) | v18, serves v14 to v18 | (the candidate) |

The support matrix in [versioning](../dev/versioning.md) covers v14, v15 and
v17 with fixtures in `db/upgrade/tests.rs` only. Those fixtures are written by
the same tree they test, so a change that alters both writer and reader
together cannot fail them. Meanwhile CI builds three formats from source
(`stamp-5`, `stamp-13`, `lance-11`) because no release wrote them, at about 25
minutes each on a cold cache, and the module that runs them,
`crossversion_upgrade.rs`, is 3,600 lines of about ten journeys, each with a
different workload:

- the v3 to v6 cases are refuse-and-rebuild fences over the search fixture;
- the v0.9 journey adds edges and a Blob;
- the stamp-13 and 0.11 journeys add branches, merges, recreated branches, a
  schema apply and `optimize`;
- the 0.12 cluster-ledger journey was `#[ignore]`d and drifted through three
  product changes before anyone ran it again (exit codes in #1078, the
  `published` commit-record default in #942, the release of the cluster lock
  on a clean stop); PR 1507 repaired it.

What a source format is tested with depends on who wrote its journey. Names
and lists repeat the facts the tests own: the Test Workspace owner check names
two journeys and their output, `check-storage-upgrade-ci.py` lists the
storage-upgrade cases, test names say `current_v10` while the current format
is v18, and the stamp a predecessor writes is a literal in each test.

Few users run graphs below format 14: the formats were written by 0.11.x and
earlier, or by `main` builds that lived for days. The code that converts them
in place is the largest part of the upgrade path, and every change to the
catalog must keep it working.

## User and operational behavior

A graph at format 14 to 18 behaves as today. A graph below 14 is refused on
open and by `omnigraph upgrade`, before any read of its data, with a refusal
that names the bridge:

```text
error[unsupported_source]: this graph is storage format 9; this binary serves 14 to 18.
  Convert it with omnigraph 0.16.x: `omnigraph upgrade <graph>`, then run this
  binary's `omnigraph upgrade <graph>`. Or export it with the binary that
  wrote it and rebuild.
```

The JSON report of `upgrade --check` carries the same facts as fields:
`source_format`, `serves`, and `bridge_release`. A graph that a past `upgrade`
converted, which carries `__history/legacy/`, still opens, reads its retained
history and is cleaned by `cleanup` as today.

Operators affected: those with a standalone graph at v8 or v9 (0.11.x) or v13
(`main` from 2026-10-01 to 2026-10-04) that has not been upgraded. Their path
is one extra command with the 0.16.x binary, which `scripts/install.sh`
installs with `VERSION=v0.16.0`. Cluster-managed graphs below v14 are already
refused today.

## Design

### Rows say what they speak

Each registry row gains a `surfaces` table: the row's version on each
compatibility surface the matrix tests, keyed by the names the
[compatibility surfaces RFC](2026-09-14-compatibility-surfaces.md) (draft)
gives its record. A new surface under test is a new key, never a new field:

```toml
[predecessors."release-0.14.0"]
release = "v0.14.0"
sha256 = { … }
binaries = ["omnigraph", "omnigraph-server"]
reports = "omnigraph 0.14.0"
surfaces = { storage_format = 16, gq_language = "2.3" }
lance = "13.0.0"
dialect = "targets"
retire_when = "format 16 leaves the window"
```

`storage_format` is the stamp a graph this binary creates carries
(`INTERNAL_MANIFEST_SCHEMA_VERSION`). Provisioning proves it: it creates an
empty graph with the row's binary and requires `snapshot --json` to report
`internal_schema_version` equal to it, as the stamp-13 journey does by hand
today.

`gq_language` is the GQ language version the binary parses
(`GQ_LANGUAGE_VERSION`, major and minor). No released binary reports it, so
provisioning reads the constant at the row's source,
`git show <release or commit>:crates/omnigraph-compiler/src/lib.rs`, and
requires it to equal the row's value: `2.1` for 0.12 and 0.13, `2.3` for 0.14
to 0.16. Once the compatibility surfaces record ships, `omnigraph version
--json` reports `surfaces.gq_language`, and a row whose binary reports it is
proven by that report instead.

Two fields are not surfaces. `lance` is the manifest writer version the Lance
journey checks today; it is a dependency's version, read from the manifest a
row's binary writes. `dialect` names the argv shape of the binary's CLI
(below), which no surface versions. A row whose surfaces or `lance` cannot be
proven is not provisioned.

### The route comes from the candidate

The candidate already decides what it does with a format: `version --json`
reports `internal_schema.serves`, and `upgrade --check` reports the route.
The matrix asks it, for each row's `storage_format`, and runs the route's checks.
No table of routes exists outside the binary:

| Route | When | Checks |
|---|---|---|
| ***accepted*** | `storage_format` in `serves` | the canonical workload's rows, history and branches read the same through the candidate; the candidate writes, merges and runs `cleanup`; the row's binary then opens the graph and either reads it (same stamp) or refuses it before any read (higher stamp), never misreads it (Boundary, Versions) |
| ***stamped*** | `storage_format` in `serves` and below the stamp the candidate writes | as accepted, after `upgrade`; `upgrade` is a stamp-only commit per live branch and rewrites no table row |
| ***bridged*** | `storage_format` below `serves` | the candidate refuses naming `bridge_release`; the bridge row's `upgrade` converts; the candidate's `upgrade` stamps; then the accepted checks |

The bridged route is where induction is sound: it is the user's real path for
an old graph, and the only one that produces a graph with retained
`__history/legacy/`, the input the legacy readers serve.

### One workload, written in each row's dialect

The workload is a list of steps, not a test per row: initialize from a schema
with nodes, edges, vectors, full-text and Blobs; load rows; create, write,
merge, delete and recreate branches; apply an additive schema change;
`optimize`; `cleanup`. Steps a row's binary does not have (a feature newer than
it) are skipped by that row's surface versions, and each skip is counted
in the run's report.

The row's binary runs each step through its ***dialect***, the argv shape of
its CLI. Two exist in the window: before and after the explicit targets of
#1090 (0.14). A dialect is a table row that maps a step to arguments; a new
dialect is a new row, never a branch in a test.

Wire answers compare by one rule: every field the row's binary reported is
reported by the candidate with the same value; fields the contract has added
since are allowed. This is the rule the 0.12 ledger journey now uses.

### The GQT corpus as a second workload

DuckDB runs its ordinary SQL test corpus against a database an old binary
created. Omnigraph can do the same with GQT: 423 cases carry a schema, 372 a
seed in the `load` envelope. For each case and accepted row, the row's
binary runs `init` with the case's schema and `load` with its seed, and the
candidate's runner runs the case's steps against that store.

This needs one runner mode. Today `--store` refuses a case with schema and
seed. The mode admits one when the store's schema equals the case's, skips
seed loading, and runs each case over a shallow clone (`--base`) so the
predecessor's graph is never written. A case whose schema the row's binary
refuses is skipped as unsupported by that row and counted; a case the row's
binary loads and the candidate then fails is a red cell.

### The GQ language axis

The route on this surface follows from `GQ_LANGUAGE_VERSION`'s rule: a minor
only accepts more, a major accepts less or changes a produced shape. A row
with the candidate's major and a lower minor takes only the as-written route
below, and every one of its texts must take it. A row with another major is a
language break.

A break is not a format. The GQ 3 break (PR 1316,
[the GQ 3 RFC](https://github.com/ragnorc/omnigraph/blob/rfc/gq-3/docs/rfcs/2026-11-07-gq-3.md),
"The break"; that RFC calls a major an *edition*) replaces major 2 with no
way to declare the version of a text and no compatibility path: the candidate
parses one version, never major-2 text on request. Its parser and
`omnigraph lint` decide one of three routes for each text a major-2 row
parses:

| Route | When | Checks |
|---|---|---|
| ***as written*** | the text is valid under both majors (1,463 of the 2,504 texts in the repository, measured by #1316) | the row's runner and the candidate's run the case; every step's columns, types, rows, affected counts and error match, apart from the sanctioned differences |
| ***fixed*** | the candidate refuses the text and `omnigraph lint --fix` applies the fix each refusal prints (1,000 more) | as written, on the rewritten text |
| ***refused*** | no machine fix exists (41 texts) | the refusal and its code are listed; no comparison runs |

The comparison is #1316's `scripts/cross-edition.py`: steps pair by ordinal,
binding and session, and a difference passes only when
`crates/omnigraph-gqt/cross-edition.tsv` lists it with the bullet of the GQ 3
RFC that sanctions it. A listed difference that no longer occurs fails, so the
table never outlives what it excuses. The storage axis uses the same form for
its sanctioned differences. The script takes the row's runner from the
manifest (`OMNIGRAPH_PREDECESSORS`) and the row's cases from
`git show <commit>:<path>`, since provisioning keeps no checkout; the table
changes only in the pull request that changes the rows.

Rows: `edge-20261118-fd4d61f` (the published major-2 CLI) is already a
release row for the storage rollback checks, which are this matrix's accepted
route run backwards, the major-2 binary reading graphs the candidate wrote.
The GQ axis adds one source row, `42013036`, the merge base the break was
measured against, with `binaries = ["omnigraph", "omnigraph-gqt"]`, because
releases ship no GQT runner.

Lifetime: a source row on this axis serves one break. The comparison gates #1316, and the
`42013036` row and `cross-edition.tsv` retire together once 0.17.0 ships,
since no case written after the break has a major-2 form. The rollback
check on `edge-20261118-fd4d61f` is a storage row and follows the storage
window.

### Coverage is a check, not a reading

`scripts/predecessors.py --self-test` gains four requirements:

1. Every format in the candidate's `serves` range that a release wrote has a
   release row writing it, newest patch of each minor.
2. The newest release below the workspace version is a row (Lance derives the
   same from its package index; here the release list is the registry and the
   workspace version is the clock).
3. Each bridge release named by the candidate is a row, and so is one release
   below the window to start the bridged route from.
4. A sanctioned-difference table names the row it compares against; the
   table and the row exist together.

A format that only `main` builds wrote (v15) has no release to row; the
fixtures of `db/upgrade/tests.rs` stay its owner, and the support matrix says
so.

### CI

Pull requests run the oldest and newest rows of the window and the bridged
route (RocksDB's `SHORT_TEST`: the oldest of each kind finds most breaks).
The nightly job runs every row and the GQT corpus mode. No cross-version case
is `#[ignore]`d: a case too slow for pull requests is a nightly row.

## Invariants

No engine invariant changes. The checks are the ones the contract already
states: one coherent view per read (`E1`), refusal before any read of an
unsupported format (Boundary, Versions), and no silent partial result (`E5`).
The bridged route keeps RFC 0064's rule that an incompatible format is refused
before effects. Retiring the 8, 9 and 13 conversions removes writers; the
readers of converted history stay, so no retained snapshot becomes unreadable
(invariant 5, safety of every retained pin).

## Compatibility and reversibility

The change for users is the bridge: one release's `upgrade` before the
current one. That is a breaking support change and carries a breaking release
note. It is reversible by restoring the conversion routes from history; the
cost of keeping them instead is every catalog change keeping three legacy
source layouts working.

The registry, the matrix and the runner mode are test infrastructure and can
be reverted freely.

## Alternatives

| Alternative | Disposition |
|---|---|
| Keep the bespoke journeys | The status quo above: coverage chosen by history, formats in the window untested by a genuine writer, copies of test facts in CI. |
| Pairwise induction (each release upgrades its predecessor) | Re-runs frozen code: an old binary's upgrade step was tested when that release shipped and no pull request can change it. What a pull request changes is the candidate, so the matrix is the candidate × every format it claims. Induction is kept where it is sound, in the bridged route. |
| Rebuild every row from its commit | Uniform, but a rebuild is not the artifact users ran (release profile and flags, version stamp, toolchain), and each row would cost a cold build. Release rows stay artifacts; source rows only where no artifact wrote a format. |
| Checked-in historical datasets only (Lance's `historical_datasets/`) | Cheap and deterministic, and right for formats below the window, where a small root suffices as refusal input. Inside the window it misses what a live writer does over time (branches, merges, maintenance) unless the fixture records all of it; the rows do that by running. A checked-in root may replace the below-window row once the bridge is proven. |
| Keep the 8, 9 and 13 routes | Possible; this RFC's matrix works either way. Keeping them keeps the source census and root-schema reader (`legacy/census.rs` and `upgrade/root_schema.rs`, about 1,700 lines), the source plans in `legacy.rs`, the in-place arm of `upgrade.rs`, most of `upgrade/tests.rs` (4,410 lines) and their genuine journeys alive for a small population. |

Prior art, read from each project's repository: Lance's `@compat_test`
(create, read and write across the three most recent majors from PyPI, both
directions, per-test minimum version and skip hooks, `historical_datasets/`,
and a workflow that builds any two refs from source); DuckDB's
`scripts/test_storage_compatibility.py` (the old CLI runs each SQL test's
`CREATE` statements, the new runner runs the same file); RocksDB's
`tools/check_format_compatible.sh` (release-branch lists by kind of check,
`SHORT_TEST` for pull requests); SlateDB's compatibility page (forward and
backward between adjacent releases, "move one release at a time", snapshot
tests of encoded bytes).

## Evidence and tests

- Owner: a new module `crossversion_matrix` of the CLI's `integration`
  binary replaces the bespoke journeys of `crossversion_upgrade.rs`; the
  cluster-path refusal and the fresh-graph report stay where they are.
- The matrix is itself tested: a planted row whose binary writes another
  stamp than its `storage_format` says, or whose source holds another
  `GQ_LANGUAGE_VERSION` than its `gq_language` says, fails provisioning; a planted candidate that
  misreads a higher stamp fails the accepted route; a removed release row
  fails the coverage check.
- Each phase below shows the matrix passing against every row it adds before
  the journeys it replaces are deleted, so coverage never drops between
  pull requests.
- The GQT corpus mode reports, per row, cases run, cases skipped as
  unsupported, and failures; the first nightly run records the baseline in
  this RFC.

## Rollout

1. **Rows and proofs.** Add `surfaces` and its proofs; add rows for 0.13.0,
   0.14.0, 0.15.0 and 0.16.0; run the 0.12 ledger journey in Storage Upgrade
   Compatibility instead of `#[ignore]`.
2. **The matrix.** Land `crossversion_matrix` with the accepted and stamped
   routes for the release rows, and the coverage checks. The bespoke journeys
   still run beside it.
3. **The bridge.** The candidate's refusal and `upgrade --check` name the
   bridge release; the bridged route runs from `release-0.11.0` through
   `release-0.16.0`. Contract, versioning, upgrade guide and skill change in
   the same pull request, with the breaking note.
4. **Retire.** Delete the 8, 9 and 13 conversion code and its tests, the
   bespoke journeys, the rows `release-0.7.2`, `release-0.8.1`, `stamp-5`,
   `release-0.9.0`, `release-0.10.0`, `stamp-13` and `lance-11`, the V5 ↔ V10
   Format Fence job, the owner-check names in Test Workspace and the case
   list in `check-storage-upgrade-ci.py`.
5. **The corpus.** Add the runner mode and the nightly GQT corpus run.

The GQ axis has no phase here: #1316 adds the `42013036` row and points
`cross-edition.py` at the manifest once PR 1507 merges, and retires both with
0.17.0.

`implementation` moves to `in-progress` with phase 1 and `complete` with
phase 5.

## Unresolved questions

- **The window's lower edge from now on.** This RFC moves it once, to a
  bridge. Is the rule "raise it only by an amendment that names the bridge
  release", as RFC 0064's support-policy row says, or a standing policy such
  as "the formats of the last N minor releases" (Lance: three majors)?
- **The readers of converted history.** They stay here. Retiring them needs
  a rule for graphs that still hold `__history/legacy/` (for example: refuse
  until `cleanup` has removed every legacy version, naming that).
- **Object stores.** The matrix runs on the local filesystem, as the
  journeys do today. Running rows against RustFS or Azurite is a separate
  qualification.

## Decision log

- 2026-11-21: draft opened. Facts above measured on `291214830`; the bridge
  claim checked against `docs/user/operations/upgrade.md` at `v0.16.0`
  (converts v8, v9 and v13 to v17 in place, serves v14 to v17).
- 2026-11-21: the query edition joins the matrix as a second axis, with the
  owner of the GQ 3 break (the "Query language evaluation" session working
  on #1316). Three routes, decided by the candidate: as written, fixed by
  `lint --fix`, refused. No declared-edition route exists, since GQ 3 has
  none. The counts are #1316's. Removed the open question on the edition axis.
- 2026-11-22: the axis is named by its surface, the GQ language version
  (`GQ_LANGUAGE_VERSION`), not "edition". An edition, in Rust's sense, is a
  version a text declares and that coexists with the others; GQ 3 has
  neither. A row's `writes` table becomes `surfaces`, keyed by the compatibility
  surfaces RFC's record names (`storage_format`, `gq_language`), so a new
  surface under test is a key; `lance` and `dialect` stay separate fields
  because they version no surface. The GQ 3 RFC's own wording, and #1316's
  `cross-edition.*` files, are that RFC's to name.
