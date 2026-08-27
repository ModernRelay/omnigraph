# Benchmark catalog

This directory contains the checked-in inputs for the end-to-end benchmark
harness. A case describes one benchmark point. A suite selects cases and says
how many samples to collect. These files are experiment definitions, not run
records, fixtures, or result storage.

## Layout

- `cases/*.case-v1.yaml` assigns the fixture, workload, environment, and
  protocol for one point.
- `suites/*.suite-v1.yaml` lists cases to run. Suite files live directly in
  `suites/`, whose parent is the authoritative catalog root. A `case` path is
  resolved relative to the suite file that contains it, and its canonical
  target must remain under the same catalog's `cases/` directory. Suite and
  referenced-case symlinks cannot escape the catalog.

The `version` field selects the document schema. Keep identifiers and enum
values in kebab-case. The scenario and fixture-builder versions identify the
code contracts that interpret the remaining typed fields.

Index state is an inventory rather than a global label. Use `indexes: []` for
an unindexed fixture; each indexed entry names its `table`, `column`, `kind`,
and `freshness`. Synthetic branch-merge builder v1 supports only
`compaction_recency: not-optimized`, because OmniGraph optimization
materializes physical indexes outside this builder's exact inventory contract.

`protocol.deadline_seconds` is required. Set it to an integer from 1 through
3600, or to YAML `null` when the measured operation has no deadline; the
executor must still enforce a separate bounded safety watchdog. Leading and
trailing whitespace in S3 identity fields and index table/column names is
normalized before validation and hashing.

`environment.cache_condition` is structured, not a single cold/warm label.
Every current point uses `process: fresh-per-repetition`. Select one of these
exact engine/page-cache/program combinations:

```yaml
# Process-cold: fresh worker, no declared warm-up, OS page cache uncontrolled.
cache_condition:
  process: fresh-per-repetition
  engine: preparation-only
  page_cache: uncontrolled
  program: none
  iterations: 0

# Warm: named program, then measurement on the same handle.
cache_condition:
  process: fresh-per-repetition
  engine: warmed-by-program
  page_cache: program-conditioned
  program: branch-merge-read-set-v1
  iterations: 1

# Post-reopen: named program, then a new engine handle; no invalidation claim.
cache_condition:
  process: fresh-per-repetition
  engine: reopened-after-program
  page_cache: program-conditioned
  program: branch-merge-read-set-v1
  iterations: 1
```

All five fields enter point identity. `program-conditioned` says the named
reads ran immediately before measurement; it is not a page-residency proof.
Page-cache-cold is deliberately absent: it requires both a named
platform/backend eviction control and a post-control witness. Storage-cold is
neither supported nor representable because controlling the OS page cache says
nothing about device, object-store, or remote-service caches.
Case definitions deliberately contain no source branch, system-under-test
build, machine identity, AWS account, bucket URI, result location, or
credentials. Runner-v1 binds the supported local host facts at execution; the
durable telemetry slice will bind the complete invocation identity to its run
record. Repetition count is run quantity, so it belongs to the suite rather
than the case's experiment identity.

`fixture.state.history_depth` is the exact number of reachable OmniGraph graph
commits on **each already-diverged frozen branch**, including the history
shared before the branch was created. It is not merely the number of commits
made after branching. Builder v1 measures both frozen branches and refuses a
case whose declaration does not match; it does not silently pad or squash
history. For the checked case, the reachable depth is 214: one genesis commit,
200 base-load publications (25 chunks for each of eight tables), one optimize
publication, and 12 branch-local divergence publications.

## Validate and inspect

From the repository root:

```bash
cargo run --locked -p omnigraph-bench -- \
  case validate benchmarks/cases/branch-merge-d50-warm.case-v1.yaml

cargo run --locked -p omnigraph-bench -- \
  case list benchmarks/cases

cargo run --locked -p omnigraph-bench -- \
  suite validate benchmarks/suites/local-smoke.suite-v1.yaml

cargo run --locked -p omnigraph-bench -- \
  suite plan benchmarks/suites/local-smoke.suite-v1.yaml
```

Each command accepts `--json` for machine-readable output. `suite plan` also
accepts `--case <ID>` to select one case from a suite.

Validation loads every referenced case and checks cross-field rules, including
checked scale budgets, table bounds, cache-condition declarations, and
reset/backend compatibility. Planning expands the suite into ordered run
entries; it does not execute a benchmark.

The checked-in smoke point declares APFS on local NVMe storage. Validation is
host-independent; runner-v1 probes the actual scratch volume on macOS and
refuses to measure when its APFS and internal-storage evidence does not match
the declaration. S3-compatible cases likewise carry region, storage class,
implementation/version, bucket-versioning state, and a digest pin for MinIO or
RustFS images in their point identity, but they are not executable by this
runner slice.

## Run locally

Wall-clock execution is available only from a release-profile binary:

```bash
cargo run --release --locked -p omnigraph-bench -- \
  suite run benchmarks/suites/local-smoke.suite-v1.yaml
```

Use `--case <ID>` to select one suite entry, `--scratch-root <EXISTING-DIR>` to
place disposable fixture trees on a particular volume, and `--json` for
machine-readable diagnostic output. The host probe examines the created
scratch tree, not merely the path supplied on the command line.

Runner-v1 constructs and verifies one fixture whose `bench-source` and
`bench-target` branches are already diverged, closes it, and freezes the
complete directory by physical SHA-256. The fixture is built at a stable
`active` path because Lance shallow-branch manifests can retain absolute base
paths. The runner makes a never-opened APFS clonefile template, removes
`active`, and clone-restores every repetition to that exact path. Forced
clonefile reset has no byte-copy fallback. Reset and the pre-timer witness walk
metadata but do not read file contents, so they do not prewarm the fixture's
data pages merely to prove identity.

Every repetition uses a fresh worker process and starts from the same frozen
state. The parent pins and records the worker executable SHA-256 and requires
matching release-profile attestation in the private handshake. The complete
cache condition is point identity. A declared read-only warm-up program runs
inside each repetition before measurement. A storage firewall allows
only each read-write open's one balanced, empty create-if-absent capability
probe and rejects any other preparation write; a complete metadata-shape
witness independently catches path, kind, or length drift. Measured counters
are then cleared and exactly one branch merge is timed. The repetition's writes
disappear when `active` is removed rather than aging the next sample, and the
never-opened template is checked after every worker exits.

Before it initializes a fixture, the runner derives the exact builder-v1
publication count, rejects a mismatched history declaration, applies explicit
local row/byte/entry/history limits, and proves that the scratch volume has its
conservative frozen-copy capacity allowance. The concrete runner limits are
listed in `crates/omnigraph-bench/README.md`; they are deliberately narrower
than the host-independent case schema.

The supervisor starts the declared hard deadline immediately before releasing
the prepared worker with `Begin`. The worker must send `Settled` after its merge
future returns. On timeout the supervisor kills the worker's complete process
group without a grace period, waits for and reaps it, and proves the group is
gone before cleanup. No killed or partial operation becomes a sample.
Preparation and exact verification have separate bounded watchdogs with a
300-second minimum allowance. Successful finalization additionally requires
clean, bounded EOF on both captured pipes and no frame after `Complete`; an
unproved containment state quarantines the disposable workspace.

After timing and call counters stop, the runner verifies exact IDs, values,
cohorts, payloads, insertions, and deletions across every target table,
including the tables that should remain untouched. Source and main must retain
their exact frozen content and unchanged branch heads. The runner also requires
the general three-way route and exactly one `TableWalk` interval per diverged
table. The reported storage counts are logical engine calls, not physical
requests or cloud-cost estimates.

The warm-up program `branch-merge-read-set-v1` reads the reachable commit list and
a coherent snapshot for `main`, `bench-source`, and `bench-target`, then fully
consumes the projected benchmark columns of every diverged table. A
`reopened-after-program` point runs that same program and reopens the engine
handle before measurement; it does not claim invalidation. A process-cold point
uses `preparation-only`, `program: none`, and `iterations: 0`: the worker is
fresh and no workload-shaped warm-up runs, but ordinary open and harness
preparation reads still occur and `page_cache: uncontrolled` states the OS
cache limitation explicitly. A true page-cache-cold point remains refused
until a named platform/backend eviction control has an observed witness.

## Delivery boundary

The configuration, planning, and identical-state local runner slices now sit
behind one typed plan contract. Runner-v1 deliberately stops at versioned
diagnostic output with `durable_record: false`; even its JSON output is not an
immutable benchmark record.

The next telemetry slice writes immutable JSON records to a content-addressed
archive and builds a query database as a disposable projection. A later AWS
adapter binds declared S3 and machine facts, applies budget and lifecycle
controls, executes the same plans, and uploads that same record format. S3
reset, fixture caching, server-mode execution, and proved operating-system
page-cache eviction remain outside the current local runner.

## Add a case

1. Add a versioned case file with explicit levels for every typed factor.
2. Reference it from a suite with a path relative to that suite.
3. Run both `case validate` and `suite validate`.
4. Inspect `suite plan` before executing the suite.
5. Run from a release build on a host that can prove every declared environment
   fact; an unsupported factor is a refusal, not permission to approximate it.

Do not encode a profile such as `micro` or `realistic` in a case. Profiles are
derived from the declared factor levels. Benchmark timing is evidence and does
not gate CI; deterministic count contracts remain in their existing owning
tests.
