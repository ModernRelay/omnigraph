# Benchmark catalog

Start with a named scenario or your own YAML:

```bash
omnigraph-bench list scenarios
omnigraph-bench show tiny-read
omnigraph-bench run tiny-read
omnigraph-bench run --config benchmarks/custom.example.yaml
```

The [shared config](benchmarks.yaml) defines all scenarios and groups. GQT files
hold the data, operations and expected results:

```text
benchmarks/
  fixtures/            # starting states; registered references/preparation together
  workloads/           # operations and verification
  deferred/            # inactive sources awaiting language/executor support
  benchmarks.yaml      # scenario names, groups and run settings
  custom.example.yaml  # optional example to copy
  README.md
```

Use `omnigraph-bench --help`, `help config` or `help cache` for the complete
workflow. `help --json` describes this binary's commands for agents. Discovery
works with a debug build; measurements require the qualified release build
[described below](#run-locally).

## End-to-end workloads

All active benchmarks are fixture/workload GQT pairs in the shared catalog.
The `end-to-end`, `query-shapes` and `traversal` groups contain bounded graph
operation representatives. Run `omnigraph-bench list scenarios` for the exact
inventory. The default remains `local-fast`.

The representatives have independent expected rows and new experiment identities.
Their fixture sizes, cache preparation and repetition boundaries differ from
the historical Rust instruments; their timing series must not be combined.

Unsupported workloads are preserved temporarily in [deferred/](deferred/README.md).
Cleanup and optimization require GQ support. Concurrent and streaming instruments require an executor that preserves their
scheduling and transport. Their preserved sources are outside Cargo discovery
and are not active scenarios. Read-only HTTP acquisition is available through
the served variants below.
Raw Lance and Rust collection comparisons are retired from the benchmark suite.

### Served query and traversal variants

`query-shapes-served` lists 13 query scenarios and `traversal-served` lists
three traversal scenarios. Their schema-less workloads contain a warm-up read,
the measured read, and explicit result verification. Embedded workload files
and default selections are unchanged. Six embedded scenarios have no served
twin because the server target has no door for them: `e2e-query-destination-search`,
`e2e-nearest-prefilter`, `e2e-nearest-nprobes-one` and `e2e-rrf-traversal` need
indexes (search, nearest and RRF), and `e2e-traversal-selective-csr` and
`e2e-traversal-selective-indexed` pin a traversal path.

These scenarios require an externally provisioned graph and a typed deployment
receipt. One invocation binds one graph and dataset recipe, so run different
fixture recipes separately. The stock definitions declare server-local
APFS/NVMe and same-host network position; other deployments need explicit
custom environment settings. Server build and dataset facts remain declared,
server counters remain absent, and these records are claim-ineligible. See the
[served acquisition guide](../crates/omnigraph-bench/README.md#served-read-only-acquisition)
for the command, receipt fields, cache semantics and evidence boundaries.

## Configuration

One [version-2 YAML file](benchmarks.yaml) defines named scenarios. A scenario
pairs a fixture with a workload and selects one measured operation. Groups
select scenario IDs without duplicating their definitions. The `run` list is
the explicit default selection: the repository selects only `local-fast`.
Large histories and registered FinBench require explicit selection.

- `fixtures/*.gqt` contains schema, seed and preparation/history steps.
- `fixtures/finbench/` keeps a registered graph's logical reference and GQT
  preparation together. The invocation supplies its physical source bundle.
- `workloads/*.gqt` omits schema/seed and contains optional preparation reads,
  the selected operation, then explicit verification.
- `defaults` supplies repetitions, deadline, environment and protocol settings;
  a scenario may override them. `--repetitions` changes only sample quantity.

Use the complete [custom example](custom.example.yaml), or generate a config
from the parser's exact selected operation:

```bash
omnigraph-bench show --workload benchmarks/workloads/tiny_read.gqt
omnigraph-bench init --fixture benchmarks/fixtures/tiny_graph.gqt \
  --workload benchmarks/workloads/tiny_read.gqt --step 1 --output custom.yaml
omnigraph-bench show custom --config custom.yaml
omnigraph-bench run --config custom.yaml
```

`init` refuses existing files. Put the output YAML in a directory containing
both sources. YAML paths resolve relative to its directory and stay within it;
absolute paths, parent traversal and symlink escapes are refused. CLI file
arguments are relative to the working directory. An explicit `--config` never
falls back to another config. With no `--config`, the CLI searches the working
directory and its ancestors for `benchmarks.yaml` or `benchmarks/benchmarks.yaml`.

A minimal config inherits five repetitions, a 60-second deadline, per-phase
attribution, manual scheduling and a monotonic timer. Local defaults are
APFS/clonefile on macOS or qualified XFS/plain-copy elsewhere. The repository
config retains each scenario's original repetitions and leaves `environment`
and `reset` out of its defaults, so a scenario without its own environment
takes that host default and the one file runs on a macOS workstation and on a
qualified Linux XFS volume alike; `show` reports the resolved value and the
record carries it. A scenario whose volume is part of its meaning declares it
explicitly, as `branch-merge-d50-process-cold-xfs` and
`branch-merge-d50-history64-xfs` do. The resolved backend is part of a
scenario's point identity, so APFS and XFS runs of one id are distinct points.
`show` reports all effective settings before a run. `deadline_seconds: null`
removes the measurement deadline; the supervisor remains bounded. Protocol
fields are `attribution`, `schedule`, `reset` and `timer`.

The internal validated case remains one experiment. Legacy case/suite readers
and explicit `suite run --dataset ... --queries ...` remain available for
existing callers; the authored catalog needs no separate case or suite files.

The echo includes the complete operation header and body, including branch
selection. It excludes expectations and separate parameter sections; the full
queries SHA-256 binds those too. Moving a step or changing its text without
updating the selector is refused. The selected step cannot occur in a loop.
A generated load is selectable only when its recipe makes exactly one loader
call; generation happens before its engine timer. Multi-call loads remain
valid dataset construction steps.

For embedded execution, cache treatment is derived, never authored as a label.
No prefix reads means
process-cold. Prefix reads on the same handle mean warmed-by-program. Prefix
reads followed by one restart mean reopened-after-program. A read after that
restart is refused. Settings and show steps are neutral; prefix writes belong
in the dataset and are refused. Only reads that actually reach an engine
operation prove the preparation. A prefix expected parameter error that never
calls the engine cannot establish a warm treatment. Every repetition is a
fresh process; the OS page cache remains uncontrolled or conditioned by the
named GQT read program. No page-cache-cold or storage-cold claim is implied.
[Served acquisition](#served-query-and-traversal-variants) uses a long-running
server with uncontrolled OS page-cache state.

### Preparing additional write history

History is ordinary generated loads or GQ mutations in the dataset. The
retired `fixture.preparation` Rust recipe is retained only for historical
archive validation and parity tests. The checked-in D50 dataset reproduces
its base load/divergence recipe, while the core parity tests cover reversible
aging. Current-content and normalized lineage witnesses distinguish different
histories without hashing random commit IDs or physical Lance versions.

The D50 warm prefix is 24 authored aggregate queries. This is the new
`gqt-read-set-v1` program, distinct from the retired Rust `warm_read_set`
scans. Fixture parity does not make their wall-clock values or point IDs
interchangeable.

## Bounded scenario pairs

The `local-regression` suite selects the following sequential cases, each with
one measured engine operation. Dataset assertions check the prepared state;
query suffixes check exact resulting rows and repeat those checks after restart.
These inputs establish correctness conditions, not performance results.

| Scenario | Cases and treatment | Verification and scope |
| --- | --- | --- |
| Very long history | `very-long-history-32-{read,write,reopen}` | One fixed live row, 32 real updates alternating 1/0, then a selected read, next write, or reopen. The build witness checks 34 reachable commits including genesis and seed. |
| Hot table with idle registrations | `hot-table-idle-{base,tables-4,branches-4,branches-4-aged}` | Separate controls add four idle tables or four live branches. The aged branch case adds eight updates only on an idle branch. Every sentinel and unchanged branch is checked. |
| Parallel tables | `parallel-tables-{1,2,4}` | One mutation touches one, two, or four of the same four populated tables. All touched and untouched rows are checked. This tests fan-out shape; it makes no actor-overlap or internal parallel-span claim. |
| Identity lifecycle | `identity-lifecycle` | Delete and recreate a branch name, then measure its first write. Exact current views prove that the recreated branch inherits main while an old child retains its original parent view. Table-schema lifecycle and held snapshots are outside this subset. |
| Repeated deletion/recreation | `repeated-deletion-recreation-{rows,branches}` | Four bounded cycles followed by the next insertion or branch creation. Every cycle verifies exact rows; the final branch inventory and reopened views are checked. Row churn and branch churn are separate treatments. |
| Threshold crossings | `threshold-crossings-{before,at,after,production-control}` | A configured history-release boundary: two, three, or four updates at `history_release_bytes = 2048`, plus a three-update control at the production budget 262144, followed by an insert. Exact main/child contents are checked. |
| Equal current state, different history | `equal-current-{narrow-one-batch,wide-one-batch,wide-four-batches}` | Four identical final rows after eight past/restore cycles. Prior payload width varies from 8 to 4096 bytes; a separate treatment publishes each load in four batches. Build witnesses check equal current contents and 18/18/66 reachable commits. |

The threshold fixtures preserve the configured shapes from the engine's
history-release correctness cases. Their row assertions do not prove that a
release counter fired, and they do not model an optimize, index-build, or
cache-capacity threshold. There is no maintenance or cleanup step in these
pairs. Concurrent interleavings, held-snapshot lifetimes, and historical row
images need their own observations; these sequential cases do not establish
those properties.

Dataset and query basenames use the same scenario names with underscores.
The history operations share `workloads/very_long_history_{read,write,reopen}.gqt`
across depths, and equal-current cases share
`workloads/equal_current_different_history.gqt`. The owning benchmark catalog
test executes all 20 bounded pairs through dataset construction and the
measured adapter, including their explicit restart verification. Parsing and
suite admission also cover the larger recipes without executing them.

### Opt-in history scale

The `history-1000`, `history-10000` and `history-100000` groups select read,
next-write and reopen operations over fixed live contents with the named
number of real updates. Their recipes live in `fixtures/`; nightly names its
21 ordinary recipes explicitly and does not select these larger inputs.

Each fixture retains one `HistoryRow` at `key = "hot"`, `value = 0`,
`payload = "fixed"`. Updates alternate value 1 and 0, producing 1,002, 10,002
or 100,002 reachable commits including genesis and seed publication. The
100,000-update recipe uses five loops of 10,000 iterations, with two updates
per iteration. Every workload verifies rows and repeats verification after
restart. Their 600,000 ms whole-file budget may expire on a slow host.

```bash
target/debug/omnigraph-gqt benchmarks/fixtures/very_long_history_1000.gqt
omnigraph-bench run history-1000 --dataset-cache /qualified/cache --json
```

These inputs have not been run as a scale campaign. Their authored scale is
not a timing claim; no one-million-update recipe is included. The groups use
one repetition by default. Cache construction and verification remain outside
the selected operation timer.

## Validate and inspect

From the repository root, use these read-only commands:

```bash
omnigraph-bench list fixtures --json
omnigraph-bench list workloads --json
omnigraph-bench list scenarios --json
omnigraph-bench show tiny-read --json
omnigraph-bench cache status tiny-read --dataset-cache /qualified/cache --json
omnigraph-bench cache status tiny-read --dataset-cache /qualified/cache --verify --json
omnigraph-bench cache list --dataset-cache /qualified/cache --limit 100 --json
```

Listings include unselected local inputs and keep missing or invalid references
visible with diagnostics. They scan only config-relative content folders and
references, never the workstation. `show --workload FILE` lists parser ordinals
and exact operation text; `init` validates whether the selected operation is measurable.

Cache status reports source availability separately from the matching cache
variant. Source states are `available`, `missing`, `invalid`, `unbound`,
`not_applicable`; cache states are `missing`, `present`, `cached`, `busy`,
`invalid`, `incomplete`, `unknown`. A served scenario reports `not_applicable`
with the diagnostic `served_scenario_has_no_dataset_cache` and exits zero: it
reads a provisioned graph and uses no dataset cache.
`present` validates the published evidence; `--verify` also audits metadata and
physical bytes before reporting `cached`. A busy entry returns immediately.
Inspection never builds, creates directories/locks, restores, cleans or quarantines.
A missing/unbound source gives an unknown current key, not a cache miss. A known
miss is a successful observation; invalid or unreadable evidence exits nonzero.

The default cache path is `target/gqt-datasets` relative to cwd. Use the same
`--dataset-cache` for inspection and execution. Keys bind recipe, engine/builder,
backend/reset, cache location, index needs and registered source identity. An
older variant in `cache list` may coexist with a current miss. Listing is paged
at 1..100 entries; continue with `--after` and the returned `next_cursor`.
Inspection is a snapshot, not a reservation for execution.

Discovery success/error JSON carries `cli_output_version: 1`, `ok`, `value`
(on success or status inspection), and `diagnostics`. Diagnostics have severity,
code, path and message. JSON goes to stdout; progress goes to stderr. Execution
retains `runner_output_version: 2` and its complete archive/failure evidence.
`help --json` includes command syntax, defaults, effects and examples without
reading any catalog or cache. Legacy `dataset validate` acquires a lease and
stages worker files; it is not a read-only inspection command.

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
identity is audit/reset evidence, not `point_id` input. A registered `gqt-v1` case separately binds the logical reference and its
preparation GQT. No copied tree remains after preflight success.

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
`benchmarks/fixtures/finbench/finbench-2026-08-21-sf10-v1.fixture-reference-v1.yaml`.
Validate the YAML structure first, then recompute its implemented witnesses
against the registered bytes:

```bash
target/release/omnigraph-bench fixture reference validate \
  benchmarks/fixtures/finbench/finbench-2026-08-21-sf10-v1.fixture-reference-v1.yaml \
  --json

target/release/omnigraph-bench fixture validate-graph \
  benchmarks/fixtures/finbench/finbench-2026-08-21-sf10-v1.fixture-reference-v1.yaml \
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

The checked-in smoke point declares no volume and takes the host default, APFS
on a macOS workstation or XFS on a qualified Linux volume. The AWS point
declares XFS on EC2 instance-store NVMe and a fresh process with an uncontrolled
page cache. Validation is host-independent; runner-v1 probes the actual scratch
volume and refuses a resolved or declared backend that does not match. S3-compatible cases carry region, storage class,
implementation/version, bucket-versioning state, and a digest pin for MinIO or
RustFS images in their point identity, but they are not executable by this
runner slice.

## Run locally

Wall-clock execution requires a flag-free release-profile binary. The
workspace development configuration injects `--cfg tokio_unstable`; the
existing documented release build uses `RUSTFLAGS= cargo build --release
--locked -p omnigraph-bench`. The guard refuses builds that retain encoded
Rust flags, debug assertions, unsupported engine features, or unmodeled
runtime overrides. Durable publication additionally requires a clean,
committed source tree matching that executable's provenance.

Start with the small correctness-oriented catalog:

```bash
target/release/omnigraph-bench dataset build   benchmarks/fixtures/tiny_graph.gqt --dataset-cache /qualified/cache
target/release/omnigraph-bench run local-fast   --dataset-cache /qualified/cache --json
```

`dataset build <case.yaml>` prepares the exact cache entry needed by that
case. `dataset build <dataset.gqt> --queries <queries.gqt>` includes the
queries' index requirements without selecting or measuring an operation.
Without `--queries`, only dataset index needs apply. The union of dataset and
query requirements is part of the cache key, and indexes are built with the
seed **before** authored updates/deletes. This preserves stale-index and
fallback scenarios. Building an unindexed raw dataset cannot satisfy a later
indexed case's `--no-build` request.

`dataset validate` takes the same inputs and verifies a published matching
entry without building. `suite run --no-build` refuses a miss. `--filesystem
apfs|xfs` applies to raw dataset and explicit-pair commands; case/suite files
supply their own backend or inherit the host default. APFS requires forced clonefile, and qualified Linux
XFS uses verified plain copies. The host probe must establish the declared
backend. Clonefile has no byte-copy fallback; copies condition the OS page
cache outside measurement.

An explicit pair uses the same resolver, cache, worker, and archive pipeline:

```bash
target/release/omnigraph-bench suite run   --dataset benchmarks/fixtures/tiny_graph.gqt   --queries benchmarks/workloads/tiny_restart.gqt   --measured-step 1 --measured-text '--- restart'   --repetitions 5 --deadline-seconds 60 --dataset-cache /qualified/cache
```

The full D50 suites are `local-smoke` (warm, using the host-local default),
`aws-xfs-process-cold` (qualified XFS) and `aws-xfs-history64` (qualified XFS,
fixture aged by 64 reversible single-row update commits before the branches
exist). They build 800,000 base rows. The small `local-fast` suite is a separate
smoke test, not proof of full-scale performance or correctness.

Every build runs in a contained child, at its cache entry's final stable
`active` path. The engine closes before freezing the never-opened `root/`
template. Publication writes `fixture-source.json` and then
`dataset-build.json` last. The parent accepts the handoff only after child
reap, process-group exit, and clean bounded output capture. The exclusive
cache lock remains held through every repetition's verification and cleanup.
All entries restore at their original absolute path, including relocatable
ones; this conservative policy keeps Lance branch base paths valid.

A cache key includes recipe contents, production engine/builder digest,
backend/reset, index requirements, canonical cache location, and registered
physical source identity when applicable. `GQT_ENGINE_DIGEST` excludes authored
case/data files, which have their own content digests. A hit verifies the
registered descriptor, logical receipt, and full template bytes once. Each
repetition uses metadata and forced-clone/copy witnesses without rereading
all APFS template bytes before its timer. Changed published evidence is
`dataset_cache_corrupt`, never an implicit rebuild. Unknown unpublished state
or uncertain process containment is preserved/quarantined for explicit
inspection; do not remove it while a child could still be alive.

The selected engine operation alone owns its monotonic interval and logical
store-call deltas. Query results are materialized before the interval closes.
Parsing, generated-row preparation, cache work, assertions, verification,
recording, and cleanup are outside it. Selected restart measures the fresh
engine open, using fresh adapter counters; it does not include preparation
checks. Optional merge-phase probes are installed only for a selected merge
with per-phase attribution. Logical calls do not measure retries, pagination,
multipart fan-out, physical network attempts, or cloud cost.

A successful record requires the selected expectation and explicit following
verification expectations. Restart alone is not verification. Any failure
rejects the repetition. After `Settled`, a later assertion/protocol/exit
failure may retain closed timing as diagnostic evidence, never as a passing
sample. A declared deadline is measured separately by the parent from Begin;
whole-file GQT budgets also apply. Process containment is required before any
active tree or scratch directory can be removed.

### Registered FinBench merge

The old `fixture run-graph` command and `.run-v1.yaml` format are retired.
The ordinary case and suite now select a registered reference plus
[`fixtures/finbench/finbench_disjoint.gqt`](fixtures/finbench/finbench_disjoint.gqt) and
[`workloads/finbench_disjoint_merge.gqt`](workloads/finbench_disjoint_merge.gqt):

```bash
target/release/omnigraph-bench run finbench   --fixture finbench-2026-08-21-sf10-v1=/path/to/bundle   --dataset-cache /qualified/cache --json
```

This shipped case declares no volume: it runs on APFS on a macOS workstation
and on XFS on a qualified Linux volume, and the two are distinct points. The
source bundle remains quiescent and is
never opened as an engine store; a byte-verified disposable copy is validated
against its logical reference before preparation. Registered imports currently
require a main-only, relocation-self-contained source and deterministic node
`@key` columns. Unsupported unkeyed node identity or named source branches is
refused explicitly.

Preparation publishes two accounts and one transfer atomically on each side.
Post-merge GQT asserts exact reserved account/transfer properties, endpoints,
and table counts on target, source, and main. Transfer IDs are engine-generated
and can differ from the retired Rust recipe. The prepared graph uses logical
equivalence, while an additional identity-aware export digest binds the fixed
registered source's original IDs. The source reference and preparation text
also enter recipe identity. This is a documented equivalence migration, not
an assertion that the old physical bytes or experiment point IDs survive.

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
S3 reset, served writes, comparison/noise-floor reports, and proved operating-system page-cache eviction remain outside this
slice.

## Add a scenario

1. Add a fixture GQT and schema-less workload GQT with explicit expectations.
2. Use `show --workload FILE` and `init` to generate the exact measured selector.
3. Add the scenario to `benchmarks.yaml`, or keep the custom config separately.
4. Inspect it with `show NAME --config FILE`; optionally add its ID to a group.
5. Run the explicit selection on a qualified release host.

The final point ID binds a dataset logical witness, verified by embedded
acquisition or declared in the served deployment receipt, plus recipe/query
contents, the selected operation, cache treatment, target, network position,
backend and protocol.
Planning can report only a pre-build experiment digest until the dataset is
bound. Paths, case display IDs, repetitions, cache-hit status, and physical
tree bytes do not enter the point ID. Historic `branch-merge-v1` records retain
their exact canonical encoding and remain queryable beside GQT records, but
old authored execution is refused.

Catalog parsing and GQT result assertions are correctness gates. Benchmark
wall-clock and unpinned measured counters remain report-only. The slow nightly
GQT route explicitly selects 21 ordinary recipes from `benchmarks/fixtures`, including full-size D50 and its
64-commit history variant; reduced parity
tests independently check the generator against the retained legacy test oracle.
