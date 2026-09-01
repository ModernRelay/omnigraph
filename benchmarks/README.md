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
- `fixtures/*.fixture-reference-v1.yaml` is the path-free logical contract for
  an imported node-and-edge graph: builder provenance, logical data shape,
  declared physical state, and expected schema/content digests.
- `real-graph/*.run-v1.yaml` selects one registered fixture, the fixed
  real-graph workload, repetition count, and operation deadline. This narrow
  diagnostic path is separate from CaseV1 suites and durable run records.

The `version` field selects the document schema. Keep identifiers and enum
values in kebab-case. The scenario and fixture-builder versions identify the
code contracts that interpret the remaining typed fields.

Index state is an inventory rather than a global label. Use `indexes: []` for
an unindexed fixture; each indexed entry names its `table`, `column`, `kind`,
and `freshness`. Synthetic branch-merge builder v2 supports only
`compaction_recency: not-optimized`, because OmniGraph optimization
materializes physical indexes outside this builder's exact inventory contract.

Builder v2 interprets `fixture.data.tables` as the total user-table count and
requires an even value: half are immutable node endpoint tables and half are
edge tables in a uniform type ring. Every edge row connects equal ordinals in
adjacent node types. `workload.diverged_tables` selects edge tables only, so it
must not exceed half of `fixture.data.tables`. Source and target update
disjoint stable edge IDs, delete disjoint edge cohorts, and append distinct
edge IDs while retaining valid endpoints.

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
credentials. Record finalization binds the clean source/build, backend,
complete typed process observations, session, and invocation to each completed
run. Its hostname-derived label is only a non-secret, non-stable correlation
hint—not a privacy boundary or proof of machine identity.
Repetition count is run quantity, so it belongs to the suite rather than the
case's experiment identity.

`fixture.state.history_depth` is the exact number of reachable OmniGraph graph
commits on **each already-diverged frozen branch**, including the history
shared before the branch was created. It is not merely the number of commits
made after branching. Builder v2 measures both frozen branches and refuses a
case whose declaration does not match; it does not silently pad or squash
history. For the checked case, the reachable depth is 213: one genesis commit,
200 base-load publications (25 chunks for each of four node and four edge
tables), and 12 branch-local edge-divergence publications on each branch.

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

cargo run --locked -p omnigraph-bench -- \
  fixture reference validate /path/to/graph.fixture-reference-v1.yaml
```

Each command accepts `--json` for machine-readable output. `suite plan` also
accepts `--case <ID>` to select one case from a suite.

An operator-quiesced external snapshot tree uses a location-free two-entry
bundle:

```text
BUNDLE/
  fixture-source.json
  root/
```

Create the physical copy-source descriptor from a quiescent local copy, then
verify any later copy:

```bash
target/release/omnigraph-bench fixture fingerprint \
  --id monarch-main-20260829 --root /mnt/nvme/fixtures/monarch/root \
  > /path/to/fixture-source.json
target/release/omnigraph-bench fixture verify /path/to/fixture-source.json \
  --root /mnt/nvme/fixtures/monarch/root --json
```

The harness can also resolve `ID=BUNDLE`, copy the source into private
disposable scratch, verify the copy, and clean it in the same invocation:

```bash
target/release/omnigraph-bench fixture preflight-copy \
  --fixture monarch-main-20260829=/mnt/nvme/fixtures/monarch \
  --scratch-root /mnt/nvme/omnigraph-bench-scratch --json
```

These commands prove only physical byte identity and copy preflight. Physical
identity is audit/reset evidence, not `point_id` input. They do not add the
fixture to a CaseV1 suite or run a benchmark; CaseV1 remains the synthetic
builder contract. No copied tree remains after preflight success.

### Observe and validate a registered graph

`fixture observe-graph` copies and byte-verifies the bundle into disposable
scratch, opens only that copy read-only, and computes the implemented logical
witnesses:

```bash
target/release/omnigraph-bench fixture observe-graph \
  --fixture finbench-2026-08-21-sf10-v1=/path/to/finbench-2026-08-21-sf10-v1 \
  --scratch-root /path/to/existing-apfs-scratch --json
```

The observation includes accepted schema shape, complete per-type node and
edge counts, a canonical logical-content digest, logical payload bytes,
engine-managed index observations, main history depth, branch inventory, and a
relocation-self-contained witness. The command rechecks the copied physical
tree after observation and removes it. It never opens the registered source as
an OmniGraph database and does not mutate that source.

The declarative expectations for the frozen FinGraph fixture live in
`benchmarks/fixtures/finbench-2026-08-21-sf10-v1.fixture-reference-v1.yaml`.
Validate the YAML structure first, then recompute its implemented witnesses
against the registered bytes:

```bash
target/release/omnigraph-bench fixture reference validate \
  benchmarks/fixtures/finbench-2026-08-21-sf10-v1.fixture-reference-v1.yaml \
  --json

target/release/omnigraph-bench fixture validate-graph \
  benchmarks/fixtures/finbench-2026-08-21-sf10-v1.fixture-reference-v1.yaml \
  --fixture finbench-2026-08-21-sf10-v1=/path/to/finbench-2026-08-21-sf10-v1 \
  --scratch-root /path/to/existing-apfs-scratch --json
```

`fixture reference validate` parses and normalizes the strict document only;
it does not inspect a graph or prove a supplied digest. `fixture
validate-graph` binds that declaration to one byte-verified disposable copy and
fails when an implemented witness differs. Aging, deletion-history,
compaction-recency, unknown raw Lance indexes, and per-index FTS/ANN freshness
still lack exact substrate-owned witnesses and remain explicit in
`unverified_state_fields`. Accordingly, graph inspection and validation always
report `claim_eligible: false`.

The logical reference and run file deliberately contain no local path, S3 URI,
account, or credentials. The `ID=BUNDLE` argument is invocation-local transport
configuration; replacing both bundle entries changes the observed physical
receipt and the logical validation must still pass.

Validation loads every referenced case and checks cross-field rules, including
checked scale budgets, table bounds, cache-condition declarations, and
reset/backend compatibility. Planning expands the suite into ordered run
entries; it does not execute a benchmark.

The checked-in smoke point declares APFS on local NVMe storage. The AWS point
declares XFS on EC2 instance-store NVMe and a fresh process with an uncontrolled
page cache. Validation is host-independent; runner-v1 probes the actual scratch
volume and refuses declarations that do not match. S3-compatible cases carry region, storage class,
implementation/version, bucket-versioning state, and a digest pin for MinIO or
RustFS images in their point identity, but they are not executable by this
runner slice.

## Run locally

### CaseV1 suite runner

Wall-clock execution is available only from a release-profile binary:

```bash
cargo run --release --locked -p omnigraph-bench -- \
  suite run benchmarks/suites/local-smoke.suite-v1.yaml \
  --archive .bench/archive

# On the AWS lab's XFS instance-store mount:
target/release/omnigraph-bench suite run \
  benchmarks/suites/aws-xfs-process-cold.suite-v1.yaml \
  --scratch-root /mnt/nvme --archive /mnt/nvme/omnigraph-bench-archive
```

Use `--case <ID>` to select one suite entry, `--scratch-root <EXISTING-DIR>` to
place disposable fixture trees on a particular volume, `--archive <DIR>` to
publish immutable canonical records, and `--json` for machine-readable output.
The host probe examines the created scratch tree, not merely the path supplied
on the command line. Durable publication requires a release binary built from
a clean committed tree so the record has honest source provenance. The exact
system under test is bound separately by the executable SHA-256, Cargo/compiler
observations, checked-in release-profile declarations, and engine feature
inventory. The inventory includes Cargo's `default` feature and execution
refuses manifest/registry drift or an unsuppressed implicit optional-dependency
feature. Effective LTO/codegen/strip settings remain explicitly unproved
until controlled benchmark infrastructure supplies a digest-bound receipt.

Runner-v1 constructs and verifies one fixture whose `bench-source` and
`bench-target` branches are already diverged, closes it, and freezes the
complete directory by physical SHA-256. The fixture is built at a stable
`active` path because Lance shallow-branch manifests can retain absolute base
paths. Public execution constructs it in a dedicated process group under a
bounded watchdog. The child freezes a byte-digested template and removes
`active` before returning an identity-checked handoff. APFS uses forced
clonefile with no byte-copy fallback. Linux XFS uses verified copies only for
the process-fresh point: reset reads bytes outside timing, so its page cache is
truthfully declared uncontrolled. Every repetition restores the exact stable
`active` path; failed construction quarantines the workspace.

Every repetition uses a fresh worker process and starts from the same frozen
state. The parent pins and records the worker executable SHA-256 and requires
matching source commit/dirty state, release-profile, target/compiler,
engine-feature, and effective engine-environment evidence in the private
handshake. Fixture and repetition children clear inherited environment. The
fixture child receives an empty protocol-owned scratch sibling as `TMPDIR`; a
measured worker receives a fresh per-repetition scratch sibling as both
`TMPDIR` and `OMNIGRAPH_MERGE_STAGING_DIR`. Each child validates the exact real,
absolute path and uses it as its current directory before work, so relative
dependency spill cannot escape the verified disposable workspace. Cleanup
occurs only after containment is proved.
`LANCE_MEM_POOL_SIZE` is admitted only as the canonical decimal `u64` byte
count Lance actually applies and is recorded as typed SUT identity. Unknown
`LANCE_*`, engine-facing `OMNIGRAPH_*`, or process-runtime thread-count
overrides fail closed without recording their values. Credentials and unrelated
host knobs never reach the child or enter a record. The supervisor also records
the repetition worker's peak RSS. The
complete cache condition is part of point identity. A declared read-only
warm-up program runs inside each repetition before measurement. A storage
firewall allows only each read-write open's one balanced, empty
create-if-absent capability probe and rejects any other preparation write; a
complete metadata-shape witness independently catches path, kind, or length
drift. Measured counters are then cleared and exactly one branch merge is
timed. The repetition's writes disappear when `active` is removed rather than
aging the next sample, and the never-opened template is checked after every
worker exits.

Before it initializes a fixture, the runner derives the exact builder-v2
publication count, rejects a mismatched history declaration, applies explicit
local row/byte/entry/history limits, and proves that the scratch volume has its
conservative frozen-copy capacity allowance plus space for the staged,
descriptor-bound worker executable. The concrete runner limits are listed in
`crates/omnigraph-bench/README.md`; they are deliberately narrower than the
host-independent case schema.

The supervisor starts the declared hard deadline immediately before releasing
the prepared worker with `Begin`. The worker must send `Settled` after its merge
future returns. On timeout the supervisor kills the worker's complete process
group without a grace period, waits for and reaps it, and proves the group is
gone. Every repetition failure, including a frame after `Complete`, rejects the
sample. Cleanup is allowed only after the direct child is reaped, its process
group is gone, and bounded capture has observed clean EOF on both pipes;
otherwise the disposable workspace is quarantined. A failure with complete
containment may therefore clean up safely, but no killed or partial operation
becomes a sample. Preparation and exact verification have separate bounded
watchdogs with a 300-second minimum allowance.

After timing and call counters stop, the worker reads and verifies exact IDs,
values, cohorts, payloads, insertions, and deletions across every table on
target, source, and main, including tables that should remain untouched, and
requires unchanged source/main branch heads. The parent does not reread those
content bytes; it independently derives and validates the point/case identity,
general three-way route, and declared table/total-row count attestations
returned by the worker. Exactly one `TableWalk` interval is required per
diverged edge table. This
full runner-v1 verification is O(store), deliberately outside timing.
Receipt-based O(delta) certification starts with the future versioned-S3 reset
slice. A future DST oracle adds independent evidence but never replaces these
per-repetition probes. Reported storage counts are logical engine calls, not
physical requests or cloud-cost estimates.

The warm-up program `branch-merge-read-set-v1` reads the reachable commit list
and a coherent snapshot for `main`, `bench-source`, and `bench-target`, then
fully consumes every diverged edge table plus the union of its endpoint node
tables using type-appropriate projections. A
`reopened-after-program` point runs that same program and reopens the engine
handle before measurement; it does not claim invalidation. A process-cold point
uses `preparation-only`, `program: none`, and `iterations: 0`: the worker is
fresh and no declared warm-up program runs, but ordinary engine open and
protected-head capture still occur and `page_cache: uncontrolled` states the
OS cache limitation explicitly. A true page-cache-cold point remains refused
until a named platform/backend eviction control has a post-control witness;
storage-cold is also unsupported and unrepresentable.

### FinGraph diagnostic runner

The real-graph run is declared independently from the synthetic CaseV1 suite.
The checked-in run file has this strict shape:

```yaml
version: 1
fixture_id: finbench-2026-08-21-sf10-v1
workload: finbench-disjoint-insert-merge
repetitions: 5
operation_deadline_seconds: 600
```

`repetitions` must be from 1 through 20 and
`operation_deadline_seconds` from 1 through 3600. Unknown fields are refused.
The logical reference path and local bundle path stay outside the run file so
neither a checkout location nor an operator's storage location becomes
experiment identity.

Build the CLI in release mode and run the checked-in FinGraph declaration and
logical reference against a local registered bundle:

```bash
cargo build --release --locked -p omnigraph-bench

target/release/omnigraph-bench fixture run-graph \
  benchmarks/real-graph/finbench-2026-08-21-sf10-v1.run-v1.yaml \
  --reference \
    benchmarks/fixtures/finbench-2026-08-21-sf10-v1.fixture-reference-v1.yaml \
  --fixture finbench-2026-08-21-sf10-v1=/path/to/finbench-2026-08-21-sf10-v1 \
  --scratch-root /path/to/existing-qualified-scratch --json
```

`run-graph` refuses a debug build. The harness copies and validates the
registered graph, prepares two branches whose disjoint changes each contain
two `Account` nodes and one `AccountTransferAccount` edge, and freezes that
prepared input. Each repetition restores the same path and runs only the branch
merge in a fresh worker process. macOS requires qualified local APFS and uses
forced clonefiles. Linux requires XFS on a directly mounted EC2 instance-store
NVMe namespace, refuses EBS, and uses a complete verified plain copy outside
the timer. Use a dedicated benchmark mount: after freezing and after each
restore, Linux `syncfs` waits for all writeback on that filesystem so reset I/O
does not overlap the merge. The recorded reset is
`xfs-plain-copy-syncfs-same-active-path`. Before making that template, the Linux
path requires free space for one prepared-tree copy plus 1 GiB. The registered
source is never opened as an OmniGraph database and remains unchanged.
Before returning either success or failure, the command explicitly attempts to
remove its disposable workspace. A cleanup failure is reported, and a run plus
cleanup double failure retains both causes.
The result includes raw elapsed samples, p50, merge route/phases, the prepared
physical digest, per-worker machine and backend evidence, and verification evidence for
the inserted delta, protected heads, and untouched tables. Pre-existing rows
in the two changed tables are not yet fully re-read and are reported as
unverified rather than implied to be proved. Machine identity is captured just
before timing in every worker; differing identities fail the run.

This path deliberately does not make a publishable performance claim. Every
result says `claim_eligible: false` and `durable_record: false`; it has no
warm-up program, and although each measurement uses a fresh process, the
operating-system page cache is uncontrolled; Linux plain-copy reset reads every
fixture byte outside timing and can warm it. `run-graph` does not accept
`--archive`, publish durable telemetry, calculate a noise floor, download an
S3 fixture, or dispatch work through the AWS benchmark infrastructure. AWS may
hold the source snapshot, but an operator must first provide the verified local
two-entry bundle used above. AWS orchestration and durable-record integration
are later connector work, not behavior hidden behind this command.

## Telemetry and delivery boundary

The archive stores canonical run-record-v1 JSON by content digest and publishes
it through one immutable pointer per invocation. Check it independently with:

```bash
target/release/omnigraph-bench archive verify .bench/archive
```

The JSON archive is authority. Its team-facing OmniGraph database is a
disposable, inventory-verified projection:

```bash
target/release/omnigraph-bench projection rebuild \
  --archive .bench/archive --root .bench/projection
target/release/omnigraph-bench projection list-points \
  --root .bench/projection --limit 100
```

Projection responses are bounded pages. When `next_cursor` is present, pass
that JSON value to the next command with `--cursor`; the cursor remains pinned
to the immutable generation from which the first page was read. The archive
verifier likewise captures one publication-coherent invocation inventory,
durability-closes every visible record through the captured archive-root
directory chain, streams the immutable records, and emits only a count plus an
inventory digest. A reader refuses an inventory whose durability proof still
fails; the publisher must still use candidate-specific `archive reconcile`
before retrying an invocation reported as `possibly_published`.

Without `--archive`, runner-v1 retains its versioned diagnostic output with
`durable_record: false`; copying that output into the archive is invalid. With
`--archive`, the CLI publishes and releases each complete raw run in turn; its
bounded summary contains the completed count and immutable record receipts,
not a duplicate raw `runs` array. If a later repetition fails, one or more
already verified repetitions publish as a permanently claim-ineligible
`censored` record and the command still fails; rep-zero failures publish
nothing, and `Settled` evidence is never promoted to a sample. A failed
publication retains only its current complete execution or censored verified
prefix as state-neutral `unpublished_run` recovery evidence. Resolve any `possibly_published`
identity with `archive reconcile` before minting a replacement invocation. A
later controlled-cloud adapter binds declared S3 facts, applies budget and lifecycle
controls, executes the same typed plans, and uploads the same record format.
S3 reset, fixture caching, server-mode execution, comparison/noise-floor
reports, and proved operating-system page-cache eviction remain outside this
slice.

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
