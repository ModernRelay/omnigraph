# Autoresearch: warm read on main, 2026-11-02

**Audience:** Ragnor and the orchestrator recording this run
**Status:** pilot run report; not a design document

## Run

| Field | Value |
|---|---|
| Operation group | warm read on main: a keyed lookup and a one-hop traversal, through the engine and the served HTTP path |
| Base commit | `3e0e467e39756355c981c2f598f63d92a394529f` (origin/main on 2026-11-02) |
| Branch | `autoresearch/warm-read-main-20261102` |
| Executor | Modal dev box (the session itself ran in it), Claude Opus 5.5 at high effort |
| Budget | about 4 hours, at most 6 hypotheses |
| Prior runs | none (pilot) |
| Graph writes | none (pilot has no graph write access) |

Commits on the branch, one change each:

1. `test(dst): warm-read cost rows and a call-attribution instrument`:
   the measurement, at base behaviour.
2. `perf(catalog): refresh captures the schema contract in the open's scan`:
   H1.
3. `perf(engine): a stale read refreshes on its first probe's verdict`:
   H2, with the `changelog.d` note that covers H1 and H2.
4. This report.

## Answer to the production question

On main, a warm read does not reload `__manifest`. The server keeps one
`Arc<Omnigraph>` per graph (`crates/omnigraph-server/src/registry.rs`), builds
a cheap `Session` per request (`AppState::session`), resolves a branch target
with a policy check only (`resolve_authorized_read_target`), and runs the same
`Session::query_with_head` the engine path runs. That read probes freshness
with one HEAD of the held `__manifest` version (plus a HEAD of the next version,
which misses) and reads no manifest body.

Counts are flat in commit history: 0, 2, 32 and 128 commits (DST) and 0, 32
and 256 commits (GQT) give the same numbers. The most expensive read in the
group is the first read after another process has published a commit (the
stale path): 20 to 22 counted calls at base, 11 after this run. I could not
reproduce anything near 800 GETs per request on main. The figure probably
comes from an older build, or from a graph whose reads touch many tables (see
"Not verified").

## How it was measured

Two exact counters. Neither covers the HTTP layer itself.

- **DST cost ledger** (`crates/omnigraph-dst/src/cost.rs`), deterministic
  in-memory object store. New labeled scenarios (`WarmReadCost` in
  `crates/omnigraph-dst/tests/scenarios.rs`): set up N small update commits on
  main, then reopen a handle (`ReadOpen`), then do a lookup and a one-hop,
  cold and warm. Another handle then publishes one commit, and the first
  handle does a lookup (`ReadStaleLookup`) and a one-hop (`ReadStaleHop`).
  The golden pins the rows at N = 2 and N = 32 (`cost_table.txt`). The new
  ignored instrument `dst_warm_read_cost_paths` logs every call's path, so
  each call can be attributed to a realm and Lance directory.
  This ledger counts **successful** calls only: a HEAD that misses (the
  probe's "next version not found") is not counted.
- **GQT `--measure`** (DST environment, single handle), which also counts
  failed requests (`head_failed`). Cases: `warm-read-main-20261102/warm_read_h{0,32,256}.gqt`
  next to this report. GQT runs every session on one handle, so it cannot
  express the external-writer case.

Realms: `manifest` = the `__manifest` dataset, `table` = a node or edge table,
`control` = the `StorageAdapter`. No read in this group made a control-realm
call, and none touched `__history`.

## Baseline (base commit)

DST ledger, successful calls per operation. The columns are the commit history
depth before the reopen.

| Label | h0 | h2 | h32 | h128 | Where the calls go (h32) |
|---|---|---|---|---|---|
| ReadOpen (reopen handle) | 5 | 5 | 5 | 5 | manifest: 2 list `_versions`, 2 get manifest, 1 get data file |
| ReadColdLookup | 4 | 8 | 8 | 8 | manifest: 1 head; table: 2 head + 2 get table manifest, 1 deletion file, 2 data files |
| ReadWarmLookup | 2 | 2 | 2 | 2 | manifest: 1 head; table: 1 data |
| ReadColdHop | 8 | 9 | 9 | 9 | manifest: 1 head; table: 1 head + 1 get edge-table manifest, 6 data |
| ReadWarmHop | 6 | 6 | 6 | 6 | manifest: 1 head; table: 5 data |
| ReadStaleLookup | 21 | 20 | 22 | 22 | manifest: 4 head, 1 list, 1 get manifest, **10 get data (one file opened and scanned twice)**; table: 2 head + 2 get, 2 data |
| ReadStaleHop | 9 | 9 | 9 | 9 | manifest: 1 head; table: 1 head + 1 get (unchanged edge table reopened), 6 data |

GQT `--measure`, all requests including failed HEADs (a loop is one step):

| Step | h0 | h32 | h256 |
|---|---|---|---|
| restart (reopen) | 8 | 8 | 8 |
| lookup, first after reopen | 5 | 9 | 9 |
| one-hop, first | 10 | 11 | 11 |
| lookup, warm | 3 | 3 | 3 |
| one-hop, warm | 8 | 8 | 8 |
| own one-row update | 19 | 18 | 20 |
| lookup after own write | 9 | 8 | 8 |
| one-hop after own write | 9 | 9 | 9 |

Determinism: I ran each baseline twice. The DST instrument output and the GQT
step table were byte-identical between runs, and the golden test's own a/b
replay passed.

## Where the stale-path calls came from

`Omnigraph::resolve_target_inner` (`crates/omnigraph/src/db/omnigraph.rs`):

1. It probes under the coordinator read lock: HEAD of v, HEAD of v+1 (found),
   HEAD of v+2 (missed).
2. It takes the write lock and probes again: the same three HEADs.
3. `ManifestCoordinator::refresh` lists `_versions/`, gets the manifest, and
   scans the hot fragment **without the schema contract**
   (`read_manifest_rows_projected`): 5 range reads of the one data file.
   `install_head` then drops the held captured contract.
4. `accepted_schema_for_snapshot` misses its cache (new manifest image) and
   calls `read_schema_contract`. With no captured contract, that call opens
   and scans the same version again: 5 more range reads of the same file.
5. `invalidate_read_caches` clears every held table handle. The next read of
   an unchanged table reopens it (2 calls per table).

## Hypotheses

| # | What | Where | Targeted counts before → after | Other rows | Gates | Outcome |
|---|---|---|---|---|---|---|
| H1 | The refresh takes the scan an open takes: contract included, small manifest files read whole (`read_manifest_rows_with_contract`), so the contract read that follows reuses it. | `crates/omnigraph-catalog/src/lib.rs` `refresh_inner` | ReadStaleLookup h0/h2/h32/h128: 21/20/22/22 → 14/13/13/13 (manifest data gets 10 → 1). Golden `l.get`: History2 19 → 12, History32 21 → 12 | Harness `_verify` phase 1488 → 1432 `l.get`. No op row rose. | See gates table | kept |
| H2 | Under the write lock, skip the second probe when the held incarnation is still the one the first probe found stale; refresh directly. A changed incarnation (another task refreshed or published) is probed again, as before. | `crates/omnigraph/src/db/omnigraph.rs` `resolve_target_inner` | ReadStaleLookup 14/13/13/13 → 12/11/11/11 (manifest heads 4 → 2; one more missed HEAD saved that this ledger does not count). Golden `l.get`: 12 → 10 at both depths | Harness `_verify` 1432 → 1415 `l.get`, 254 → 253 `l.list`. No op row rose. | See gates table | kept |

Result for the targeted scenario (read after an external commit), counted
calls: **22 → 11** at h32 and h128, 20 → 11 at h2, 21 → 12 at h0. On a store
that bills the missed HEADs, the GQT-style count drops by 12 at h32: 9 data
reads, 2 found HEADs and 1 missed HEAD. Warm and cold reads are unchanged:
they were already at their floor for this design.

H2 changes four exact-count assertions in `integration/warm_read_cost.rs`
from `version_probes == 2` to `== 1`. They still pin an exact count, now the
lower one. The row and head correctness assertions beside them are unchanged.

### Considered, not attempted (budget)

- **H3: open the refreshed `__manifest` from the probe's HEAD.** The probe
  already holds v+1's location and size. Opening from that location would
  skip the `_versions/` LIST, saving 1 LIST per stale read. A LIST also costs
  more than a GET. It needs `ManifestLocation` plumbing from the probe into
  the refresh, and an argument that a version that was latest at probe time
  is linearizable for the read, which it is because the probe is part of the
  read.
- **H4: keep unchanged tables' handles across a refresh.** This saves 2 calls
  per unchanged table touched after each external commit, so it grows with
  tables, not history. `invalidate_read_caches` clears all handles on purpose,
  as hygiene for e_tag-less stores, recreated branches and replaced roots
  (keys may collide on legacy linear versions). A safe version invalidates
  only when the refresh reports a new lifetime. That needs a refresh outcome
  type and a review of the replaced-root case at a different version.
- **H5: warm one-hop repeats.** The warm hop reads the same small data files
  more than once: Person twice before the traversal, the edge table twice,
  Person again for hydration. On these tiny files every read is a whole-file
  GET. One pair comes from the planner sizing the frontier from the anchor's
  rows before execution reads them again. Changing that touches the
  bound-plan replay contract and needs a fixture with realistic file sizes
  first. Not a quick win.

## Gates

H1 alone (before H2 was applied):

| Gate | Result |
|---|---|
| `omnigraph-catalog` tests | 214 passed, 0 failed |
| DST suite (`cargo test` in `crates/omnigraph-dst`: crash, concurrency, faults, golden) | 54 + 2 + 56 passed, 0 failed (ignored instruments excluded) |
| `cargo fmt --all --check` | clean |

H1 + H2 together (the final tree; engine and GQT gates ran only here):

| Gate | Result |
|---|---|
| DST suite (crash, concurrency, faults, golden) | 54 + 2 + 56 passed, 0 failed |
| Engine `--test integration` (`warm_read_cost`, `recovery`, `branching`, `point_in_time`, `consistency`, `lifecycle`, `changes`), failpoints feature | 216 passed, 0 failed |
| Engine `--test failpoints` | 142 passed, 0 failed, 2 ignored (pre-existing) |
| Engine `--test schema_apply` | 42 passed, 0 failed |
| Engine `--lib`, failpoints feature | 630 passed as an unprivileged user. As root, `db::upgrade::tests::interrupted::unreadable_legacy_object_asks_for_a_rerun` fails: it makes a file unreadable with `chmod 000`, and root still reads it. It fails the same way with H2 removed and passes as `nobody` on both trees. Environment, not this change. |
| GQT, complete corpus (`cargo test -p omnigraph-gqt`: 432 cases, 15 with frozen-reference-engine parity steps, plus harness tests) | 432 + 50 + 14 + 27 passed, 0 failed |
| GQT `--measure` warm-read cases, before vs after | identical at h0, h32 and h256: no single-handle step's count changed |
| `cargo clippy -p omnigraph-engine -p omnigraph-catalog -p omnigraph-dst --all-targets -- -D warnings -W clippy::dbg_macro` | clean |
| `cargo fmt --all --check` | clean |

Not run: the canonical workspace test graph (server, CLI and cluster crates),
the S3 and Azure suites, and the nightly and ignored tiers. Neither change
touches the server, CLI or cluster crates directly. They reach the changed
code only through the engine read path the gates above cover.

## Wall clock

Not measured. The qualified benchmark environment is a release build with no
`--cfg tokio_unstable`, run on a qualified XFS or APFS dataset cache
(`omnigraph-bench run`). It was not available in the box, and a debug build
in gVisor is not evidence. As the brief notes, fewer logical calls do not by
themselves prove lower latency. In this change the removed calls are serial
on the stale read's critical path: the second probe and the second
manifest open both run under the coordinator write lock. So I expect lower
latency on S3, but I have not measured it.

## Not verified

- **Served HTTP path, counted.** No storage counter exists below a server
  request. The bench harness's served mode declares server storage counts
  absent, and GQT `--measure` refuses server targets. The served numbers
  above are inferred: the code shows the handlers call
  `Session::query_with_head` on the shared handle, and I measured that call.
  I did not count bytes on the wire or check the HTTP layer itself.
- **Real object stores.** Counts come from the DST in-memory store. On S3 the
  missed HEADs are billed requests and a LIST may page, but the call
  structure is the same.
- **Larger graphs.** The fixtures have 2 to 4 tables and a few rows. With
  many tables, each stale read reopens every table it touches (H4), and the
  cold registry fragments of a long-lived `__manifest` were not exercised
  past 256 commits.
- **The 800-GET production figure.** I did not see the deployment's build
  or its request logs.

## Workflow feedback

- **Bench harness vs the asked-for split.** The brief asks for counts by
  manifest, table and control realm "from the bench harness". The bench
  harness counts by verb only (`LogicalCallCounts`), needs a qualified release
  build and dataset cache, and has no server-side storage counts. GQT
  `--measure` gives the realm split but only for one handle. The DST ledger
  had no realm split until this run added a path log. Name the counter to
  use.
- **The two ledgers count differently.** The DST ledger counts only
  successful calls; GQT counts failed HEADs too. Both are deterministic. Say
  which one is "the score".
- **External writers.** The stale path, which is the expensive one, cannot be
  written as a GQT case. The brief should expect a Rust scenario for it.
- **Served path.** The brief assumes the served path can be counted. Nothing
  in the repository counts storage under an HTTP request. Either add that
  instrument as a prerequisite or accept a code-path argument.
- **Isolated checkout.** A fresh worktree on Linux starts cold:
  `scripts/seed-target.sh` only clones on APFS, and a cold build is hours on
  this box. I used the box's existing checkout on a new branch.
- **Dev box.** The session was started inside a dev box, so "execute in its
  own box and stop it" is circular: stopping the box ends the session.
  Say whether the orchestrator owns starting and stopping.
- **Skill reading.** "Read every linked file" was about 3,700 lines, mostly
  about schema design, Blob values and cluster operations, which don't bear
  on engine read cost. For engine work, the repository's invariants, Lance
  and testing guides were what mattered.
- **Where to push.** The brief says to push the branch, but
  `ModernRelay/omnigraph` has a `fork-only` ruleset that refuses every branch
  creation, with no bypass. The branch is on Ragnor's fork,
  `ragnorc/omnigraph`. Name the fork in the brief.
- **Determinism.** The counts were deterministic run to run: two baseline
  runs of each instrument were identical, and the golden's built-in replay
  check passed.
