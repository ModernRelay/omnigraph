---
type: spec
title: "RFC-031 — Comparative cost harness"
description: A checked-in harness that measures time, storage requests, bytes, and memory for the same logical work across two builds and two access paths, so release-gating cost regressions are caught by evidence instead of noticed by accident.
status: draft
tags: [eng, rfc, benchmark, performance, cost, release-gate, tooling, omnigraph]
timestamp: 2026-08-05
owner: OmniGraph maintainers
---

# RFC-031: Comparative cost harness

**Status:** Draft
**Date:** 2026-08-05
**Author track:** Maintainer design series
**Depends on:** nothing. Deliberately additive tooling; no product behavior,
format, or protocol change. A test-only metrics feature/recorder is in scope.
**Complements, does not replace:** `crates/omnigraph/tests/helpers/cost.rs` and
its consumers (`warm_read_cost`, `write_cost`, `merge_cost`,
`branch_control_cost`, …) — in-process local/post-merge flat-cost gates; and
`crates/omnigraph/benches/scenarios.rs` — the subprocess wall-clock/RSS
decision instrument for one build.
**Audience:** engine, CLI, server, and release maintainers.

---

## 0. Decision summary

On 2026-08-03 an ad-hoc measurement found that every CLI operation against a
current-format graph costs ~55 more S3 requests than against 0.8.1 — a fixed
toll paid at graph open, introduced somewhere across fifteen format strands,
noticed by nobody. A companion run through the cluster server showed the same
toll amortized to zero, and isolated a separate 4× request increase confined to
export. That measured edge build belonged to the subsequently removed RFC-026
lineage; 0.9.0 returned to manifest schema v6. The numbers are historical
evidence of an instrumentation gap, not a claim about current-main cost.

Two facts about that discovery matter more than the numbers:

1. **The existing instruments could not have caught it.** `helpers::cost` gates
   warm operations and selected cold Lance-open/request/byte terms;
   `scenarios.rs` measures one build. Nothing compares the complete public
   OmniGraph cold lifecycle across two builds and processes, and nothing can
   observe a *server's* requests from a separate CLI invocation.
2. **The measurement that did catch it is not in the repository.** It exists as
   two markdown reports produced by a script in a personal working directory.
   It cannot be re-run by anyone else, cannot gate a release, and will rot.

This RFC proposes a bounded first harness for both gaps: a checked-in
driver that runs the same logical work against **two builds** across **two
access paths**, measures **four dimensions** (time, storage actions, bytes,
peak memory), and emits versioned, environment-stamped JSONL from which a
reporter produces a comparison and a pass/fail. Timing/RSS and storage traffic
are collected in separate matched runs because the counting proxy materially
perturbs timing (§4.2). V1 is a release gate and an on-demand tool, not a
per-PR performance gate — with one deliberate bounded regression guard (§6.3)
that closes the specific hole above. A shared historical
service or database is deliberately deferred until local records plus release
artifacts prove insufficient (§4.4).

## 1. Why the existing instruments do not cover this

| Instrument | Measures | Cannot |
|---|---|---|
| `helpers::cost` + consumers | in-process object-store ops, selected cold Lance-open/request/byte terms, and flat-cost assertions across commit depth | compare two binaries; observe a separate server process; measure a full public cold lifecycle, RSS, or wall time; measure a released build |
| `benches/scenarios.rs` | wall time + peak RSS + scenario metrics for one build, subprocess-isolated | compare builds; count storage requests; vary access path |
| The 2026-08-03 reports | exactly the missing axes | be re-run, be reviewed, gate anything |

The scoped gap is **comparative, cross-process, cost-dimensional** measurement;
this RFC does not claim to cover throughput under concurrency or every
performance question.

## 2. What it measures

Four measurement families per (operation, build, access path, fixture) cell.
The direct/proxied split and server-lifetime limitation below determine which
families are available and release-gated for a given cell; a missing measurement
is explicit, never zero:

- **Wall time** — collected only on the direct, unproxied path. Five matched
  pairs report median and min/max; p95 is reported only with at least 20
  samples. Never infer a tail percentile from five observations.
- **Storage actions** — collected in a separate proxied pass and split at two
  layers. Instrumented control builds expose Lance's optional logical operations
  (`get`, `put`, `put_part`, `head`, `list`, `delete`, `copy`, `rename`,
  multipart complete/abort); associated metrics expose failures, bytes,
  duration, in-flight work, and native-cloud retryable/throttle attempts. The
  physical layer records HTTP attempts and classifies GET/HEAD/LIST/PUT/POST/
  DELETE plus multipart initiation, part upload, completion, abort, and copy.
  Logical operations and physical attempts are not interchangeable: retries
  may make one logical action several billable requests. Deterministic RustFS
  structure gates use physical counts; logical metrics qualify and diagnose the
  proxy. Real-S3 proxy counts remain diagnostic until a provider-side or other
  direct-attempt oracle reconciles them.
- **Bytes** — read and written at both layers when available. Absent from the
  2026-08-03 reports, and the reason its export finding could not be diagnosed:
  request count alone cannot distinguish *more data* from *the same data in
  smaller slices*.
- **Peak RSS** — a process-lifetime high-water mark from `wait4`/`ru_maxrss`,
  collected only when the measured process exits. Direct embedded invocations
  and `server-startup` receive paired release thresholds. A warm long-lived
  server has one advisory run-level high-water mark; `served-first-request` and
  `served-warm` therefore have no per-operation RSS threshold because there is
  no truthful `ru_maxrss` delta.

Two derived values, computed by the reporter, that make results decidable
rather than debatable:

- **Payload-request density** — reported separately by layer and direction:
  data-bearing read actions per payload MiB read, and data-bearing write actions
  per payload MiB written. HEAD/LIST/delete and other metadata-only actions stay
  as absolute counts; a zero-byte denominator reports `n/a`. There is no blended
  "requests per MB" number that hides action class or direction.
- **Estimated cost** in currency, from a checked-in price table stamped with
  publication date, provider, region, storage class, and transfer assumptions
  (per-1,000 requests by class, per-GB transfer). This is what turns "4× the
  requests but 58× faster" from an argument into an arithmetic result. It is an
  estimate, never a substitute for the raw action and byte counts.

The logical-action vocabulary is versioned against Lance's
[observability guide](https://lance.org/guide/observability/). The checked-in
price table records its source URL (for AWS, the
[S3 pricing page](https://aws.amazon.com/s3/pricing/)) so later reports can
reproduce the estimate instead of silently applying today's prices to an old
run.

## 3. Parameters

The axes a run may vary. A single comparison varies **one**; the rest are held
and recorded.

| Axis | Values |
|---|---|
| Build | `stable` (a released tag), `edge` (a commit), or an **ablation variant** (edge with one change reverted) |
| Access path | `embedded` (fresh CLI process and graph open), `server-startup` (spawn to readiness), `served-first-request` (first request after readiness), `served-warm` (after deterministic priming) |
| Fixture size | at least two (small / large); a third middle point when a slope is suspected |
| History depth | shallow vs deep at equal row count — separates history-scaling cost from data-scaling cost |
| Backend | RustFS for deterministic request structure; real S3 for direct latency confirmation; real-S3 physical counts only after the TLS/SigV4 proxy gate passes |
| Operation knobs | operation-specific sweeps (e.g. export chunk size) |

Two axes deserve their rationale recorded, because omitting them is how the
2026-08-03 reports became hard to interpret:

**Server lifecycle windows.** Startup, first-request, and warm work are three
different questions. `server-startup` begins immediately before spawn and ends
only after a `--require-all-graphs` server answers `/healthz` **and** an
authenticated, policy-authorized `GET /graphs` returns the exact sorted expected
inventory. `/healthz` alone is only liveness and quarantine mode is ineligible.
The startup cell uses a fresh server and contains no measured graph operation.
`served-first-request` starts a fresh strict server and waits for that same
verified inventory. In the
proxied pass it then drains prior proxy work, resets counters, and measures
exactly one request; in the direct pass it has no proxy counter step.
`served-warm` performs one declared, unmeasured priming request and fully reads
its response. The proxied pass then waits for zero proxy in-flight work before
resetting counters; the direct pass proceeds after response completion alone. A
sample never reuses a server from another lifecycle cell.
Folding these windows together makes a fixed toll look like a per-request cost
or vanish entirely.

**Local vs real S3.** A local proxy answers much faster than real S3. Local
measurement therefore *systematically understates* the latency cost of any
"more, smaller requests" design. Request-shape conclusions drawn only from a
local backend are provisional and must be labeled so. Wall-time conclusions
come from the direct pass on the named backend, never from timing through the
counting proxy.

## 4. Design

### 4.1 Every sample receives an equivalent fresh state

Two builds may speak different storage formats, so they cannot open the same
graph. Each arm therefore **builds its own fixture from the same logical seed**
(schema source + seed JSONL + branch/history recipe). Equivalence is proved,
not assumed, but the external harness never parses historical manifests. A
checked-in versioned adapter drives only public outputs from each supported
binary: normalized `schema show`, canonical whole-graph `export` for every
declared branch, `branch list`, `commit list --branch`, and the measured
operation's JSON result. The adapter projects version-specific fields into an
explicit common schema; opaque text is not compared. The pre-state fingerprint is those common
observables: schema source, logical row multisets, branch names, and commit-DAG
topology/depth. Commit IDs, timestamps, manifest versions, root URIs, and other
build-minted identities are alpha-normalized while parent/branch shape is
preserved.

V1 supports exactly the previous release and edge adapter named by the run
manifest. Adding another historical build requires an adapter contract fixture;
an absent public observable makes that comparison explicitly unsupported rather
than licensing internal-format decoding or weakening the fingerprint.

Every mutating sample receives a fresh deterministic root with that fingerprint
and declares an expected normalized post-state. The reporter admits the sample
only if both pre-state and post-state match across arms. Read-only samples also
receive separate roots when backend caches or server state could cross-contaminate
them. An immutable base artifact may be cached by seed digest, fixture-recipe
version, and building-binary SHA-256, but each sample clones or rebuilds from it;
the measured operation never mutates the cache. A divergent pre-state, result,
or post-state fails the cell and is excluded from performance aggregates.

### 4.2 Components

1. **Counting proxy** — an in-repo HTTP proxy between the measured process and
   the storage endpoint. It counts physical attempts by action, bytes each way,
   and response outcomes, and exposes a control endpoint that resets counters
   only after all prior requests drain. HTTPS interception is accepted only
   after a spike proves the client trusts the test CA, SigV4's original Host and
   signed request remain unchanged, streaming bodies are counted without
   buffering them whole, and the backend cannot be reached through an
   unmeasured route. Until that gate passes, real-S3 proxy results are not
   accepted as exact physical counts.
2. **Driver** — takes a run manifest (builds, fixtures, operations, paths,
   repeats), provisions and verifies fresh sample roots, starts proxy and
   server, executes each operation as a fresh subprocess where the lifecycle
   requires it, and emits one JSON record per invocation. It runs two matched
   passes: direct for wall/RSS and proxied for actions/bytes. The logical result
   and pre/post fingerprints must agree across the passes.
3. **Reporter** — aggregates records into the comparison table, computes derived
   metrics, evaluates thresholds, and renders the markdown. The two 2026-08-03
   reports are the target output shape; they were legible and should stay so.
4. **Logical-metrics control** — separate current-source CLI and server control
   builds enable a non-default `bench-metrics` feature. The CLI and server
   features forward it into the engine and `omnigraph-storage`, and both install
   Lance's process-global metrics recorder before the first graph or cluster
   open. Feature-gated counters live *inside* the concrete
   `omnigraph_storage::ObjectStorageAdapter`; the harness does not wrap or
   replace that authority-bearing type. The driver supplies an inherited,
   harness-only IPC channel. A one-shot CLI child resets at startup and emits a
   final snapshot; the long-lived server accepts bounded reset/snapshot commands
   around each proxy window. The server channel is not an HTTP route, and every
   response is one bounded (≤64 KiB) JSON control record. Proxy records are
   attributed to known Lance-dataset versus sidecar/control prefixes; every
   request must land in exactly one class. Lance metrics qualify only Lance-
   attributed traffic, while the internal adapter counters explain the separate
   residual. This build exists only to qualify the action mapping; arbitrary
   released binaries are not retro-instrumented, and its timings/RSS never enter
   release comparisons.

The split is mandatory, not theoretical hygiene: the 2026-08-03 proxy
validation perturbed throughput by roughly 6.4×. Proxied wall time may be
recorded only as diagnostic calibration and is never release evidence.

### 4.3 Rules that make a result trustworthy

- **Use matched pairs.** Each pair uses the same seed and equivalent fresh
  prestates. A mandatory direct timing/RSS cell runs five pairs in order AB, BA,
  AB, BA, AB — never all of A then all of B. A deterministic RustFS proxied cell
  runs two pairs, AB then BA: both must have identical physical action/byte
  counts within each build or the cell fails instead of averaging noise. Report
  every signed pair delta and ratio plus median and min/max. A p95 requires at
  least 20 samples; a single sample is directional evidence only.
- **Stamp the environment.** Record manifest schema/version/digest; logical
  fixture and history-recipe versions; binary SHA-256, git commit/tree SHA and
  dirty flag; Lance and `object_store` versions; CPU, RAM, OS, thread counts,
  cache/session settings, I/O buffer/chunk settings, retry/AIMD settings;
  backend endpoint kind, region and storage class; and direct/proxied
  observation mode. `scenarios.rs` already owns part of this shape; extend one
  shared vocabulary so records stay interpretable a year on.
- **One variable per comparison.** The two 2026-08-03 reports each changed build
  *and* access path relative to each other, which is why attribution needed
  inference. Hold everything but one axis.
- **Ablation for bounded attribution.** A build variant with one change
  reverted estimates that change's marginal effect in the measured fixture;
  interacting effects are not additive. `edge`, `edge−open-gate`, and
  `edge−hidden-column` narrow a hypothesis but do not prove a decomposition.
- **Fail cells, not records.** A refusal, crash, timeout, result mismatch, or
  state mismatch still emits a typed record, then fails the required cell. It
  is excluded from aggregates, is never replaced, and cannot be waived as a
  performance observation.

### 4.4 Output and retention

V1 writes JSON Lines, one envelope per invocation, to a local results log
(`--out`, else an env var, else a gitignored default) — the same convention
`scenarios.rs` uses. The reporter is a pure function of the records supplied to
it. A release run uploads the exact JSONL, run manifest, threshold manifest,
and rendered report as one checksummed CI bundle, then attaches the accepted
bundle to the release and links it from the release notes. That is sufficient
to make the evidence reviewable and repeatable without operating a new database.

The envelope is versioned and contains at least: `record_schema_version`,
`instrument_kind`, a unique-per-invocation `run_id`, `sample_id`,
`coordinate_key`, `pair_id`, `logical_trial_id`, observation mode (`direct` or
`proxied`), run-manifest digest, operation/path/fixture/history/backend/runtime
keys, build and binary identities, pre/result/post fingerprints, typed outcome,
raw measurements, and threshold/waiver identities. `coordinate_key` is the
deterministic manifest-plus-sample coordinate used to compare independent runs;
`sample_id` derives from `run_id + coordinate_key`, so a legitimate rerun cannot
collide with different timing/RSS. `pair_id` groups A/B samples within one mode,
and `logical_trial_id` joins corresponding direct/proxied samples. Merging the
same run deduplicates exact records and rejects conflicting bodies for one
`sample_id`.
Per-request details are optional versioned sidecars keyed by `sample_id`.

A future `sync` command may copy immutable per-run objects to a shared prefix,
but it is not part of V1. It must use create-only object names, treat an
identical existing digest as success, reject conflicting reuse, and fetch
incrementally from an explicit cursor/index rather than list an unbounded
history on every report. SQLite or hosted telemetry may later be a rebuildable
query view over those objects; neither is an authority or a dependency of the
release gate. Lance's process-local metrics are likewise a measurement oracle,
not the historical record store. RFC-032 may reuse the envelope library for its
own local records without depending on shared storage.

## 5. Where it lives

A new binary crate, `crates/omnigraph-bench`, containing driver, proxy, and
reporter. Rationale: it must drive *independently built released binaries*,
which a `cargo bench` target cannot; it drives only binaries with the §4.1
public-output adapter. It needs the workspace's serde/JSON/ULID
dependencies; and a Rust crate keeps environment stamping and the record schema
honest in a way shell scripts do not. It is an ordinary workspace member with
`publish = false`; its fast parser/reporter/proxy/record-contract tests run
under the canonical `cargo test --workspace --locked --features
omnigraph-engine/failpoints,omnigraph-cluster/failpoints` graph. Only the
external measurement grids are excluded from that command. Subprocess, `wait4`, binary-
identity, clean-tree, stamping, and JSONL primitives are shared with
`benches/scenarios.rs` through a dependency-neutral library surface rather than
copied. The landing change adds the crate to `AGENTS.md`'s workspace list and
to `docs/dev/testing.md`'s ownership map. The `bench-metrics` control feature is
non-default, test/tooling-only, and structurally guarded so product binaries do
not install or expose the recorder.

## 6. CI posture

Per the standing CI budget, the release measurements are **not** a per-PR gate.
The implementation adds one always-starting (no path filter), required reporting
job named `Check benchmark harness contracts` for the crate's fast unit/contract
tests, run as the lightweight default-feature `cargo test --locked -p
omnigraph-bench`, with a 5 min timeout: manifest and record parsing, deterministic
IDs/deduplication, reporting and threshold evaluation, failure exclusion, proxy
action classification/window draining, and the synthetic accounting oracle. Its
landing change adds that exact context to `.github/branch-protection.json`,
updates `docs/dev/branch-protection.md` and `docs/dev/ci.md`, and assigns a CI
maintainer to run `scripts/apply-branch-protection.sh` after merge and verify the
context reports on a pull request. The workflow alone does not make the check a
gate. Heavy diagnostic sweeps remain `workflow_dispatch`/local and are never
silently added to the release gate.

### 6.1 Mandatory V1 manifest and budget

V1 is one checked-in executable manifest, not a Cartesian product of §3. Its
landing change creates `crates/omnigraph-bench/manifests/v1-release-smoke.yaml`
and pins the digest of every schema, query, generator, command template,
threshold, and price input. The prose names below are explanatory aliases; the
manifest is the executable identity.

The fixture recipe is exact and deliberately modest:

- The schema is a harness-owned copy of the four declarations in
  `crates/omnigraph/tests/fixtures/test.pg`: `Person`, `Company`, `Knows`, and
  `WorksAt`, with no vector, blob, or index build.
- `small-shallow` has 128 people (`p000000` onward), 16 companies (`c0000`
  onward), one `WorksAt` edge from person `i` to company `i mod 16`, and four
  `Knows` edges from person `i` to people `(i + 1)..=(i + 4) mod 128`: exactly
  128 + 16 nodes and 640 edges. Age is `20 + (i mod 50)`. One overwrite load
  follows init; there is no later history.
- `small-deep` starts from that byte-identical logical seed, then makes exactly
  64 one-row update commits to `p000000`, alternating age 21 and 20. The 64th
  commit restores the shallow fixture's exact final rows, so only history depth
  differs.
- `gate-large-shallow` uses the same formula with 4,096 people, 256 companies,
  4,096 `WorksAt` edges, and 16,384 `Knows` edges. It has the same one-load
  history as `small-shallow`. Generator seed, JSONL ordering, load chunk size,
  branch (`main` only), and the single-graph server config (`bench`) are literal
  manifest fields.

Fixture generation records the exact JSONL line count, byte length, and SHA-256
in the manifest; setup refuses a mismatch before either build is measured. Each
adapter also pins the expected public commit-DAG depth produced by the recipe.

The read is the exact `total_people` query body from
`crates/omnigraph/tests/fixtures/test.gq` with `{}` parameters. The write is:

```gq
query set_age($name: String, $age: I32) {
    update Person set { age: $age } where name = $name
}
```

with `{"name":"p000000","age":71}`. Cold embedded work is exactly
`schema show --json`; whole-graph export is `export --branch main` with stdout
fully drained, hashed, and semantically checked. Version-specific adapters may
spell flags differently but must project these same operations.

The mandatory coordinates are exactly:

| ID | Backend / observation | Access path and operation | Fixture | Matched pairs |
|---|---|---|---|---:|
| R1 | RustFS direct + qualified proxy | embedded-cold schema show | small-shallow | 5 direct / 2 proxied |
| R2 | RustFS direct + qualified proxy | embedded-cold schema show | small-deep | 5 direct / 2 proxied |
| R3 | RustFS direct + qualified proxy | server startup to verified inventory | small-shallow | 5 direct / 2 proxied |
| R4 | RustFS direct + qualified proxy | served first `total_people` | small-shallow | 5 direct / 2 proxied |
| R5 | RustFS direct + qualified proxy | served warm `total_people` | small-deep | 5 direct / 2 proxied |
| R6 | RustFS direct + qualified proxy | embedded `set_age` | small-shallow | 5 direct / 2 proxied |
| R7 | RustFS direct + qualified proxy | served warm `set_age` | small-shallow | 5 direct / 2 proxied |
| R8 | RustFS direct + qualified proxy | embedded whole-graph export | gate-large-shallow | 5 direct / 2 proxied |
| R9 | RustFS direct + qualified proxy | served warm whole-graph export | gate-large-shallow | 5 direct / 2 proxied |
| C1 | real S3 direct | embedded-cold schema show | small-deep | 5 |
| C2 | real S3 direct | served warm whole-graph export | gate-large-shallow | 5 |

The complete release-smoke parent has a 45 min wall-clock deadline allocated as
5 min fixture provision/equivalence, 15 min R1–R7, 10 min R8–R9, 10 min C1–C2,
and 5 min reporting/teardown. A qualification run on the declared Linux runner
must finish every allocation with at least 20% headroom before the manifest can
become required; the pilot record ships with the manifest. If it does not fit,
the proposal is amended or the fixture is deliberately re-versioned and
re-baselined—CI time is not silently expanded. This avoids repeating the
historical 643-second stable export five times inside a 45-minute claim.

The same manifest has a US$5 modeled worst-case request+transfer ceiling. For
every invocation it declares an upper bound for each *physical billable request
class*, after expanding logical operations into LIST pages, multipart initiate /
part / complete / abort, batched-delete requests, and copy/rename source and
destination requests. Each expansion records its pagination, part, batch,
fan-out, and SDK retry-attempt maxima plus aggregate request-body and response-
body byte ceilings. Preflight sums those expanded maxima across both builds and
every repeat, then applies the dated price table. Qualified proxied invocations
also enforce the physical-attempt and aggregate-byte ceilings at runtime; a
lower retry count does not license extra unmodeled actions. A direct real-S3
pass has no exact live attempt oracle, so its US$5 figure is this conservative
fully expanded admission model plus the wall deadline, not a claim that AWS
offers an instantaneous billing fuse. The run refuses if any expansion or input
is unpinned or if the modeled upper bound does not fit; crossing an observable
time or resource allocation fails closed. Additional operations, sizes, history
points, ablations, real-S3 proxy diagnostics, or ≥20-sample percentile runs are
on-demand evidence outside the release gate.

The historical-scale sensitivity manifest is separately checked in as
`manifests/v1-historical-sensitivity.yaml`; it never borrows time from the
release-smoke gate. It adds `small-history-512` (the 128-person seed plus 512
alternating/restoring commits) and `historical-large-shallow` (65,536 people,
4,096 companies, 65,536 `WorksAt`, and 262,144 `Knows`, using the same formula).
Its exact coordinates are the embedded-cold and served-warm read on
`small-history-512`, plus served whole-graph export on
`historical-large-shallow`: two RustFS proxied pairs for request structure and
five real-S3 direct pairs for the export's directional latency. It receives a
3 h / US$10 on-demand ceiling and requires the same 20% qualification headroom.
The 2026-08-03 source fixture was not preserved in this repository, so V1 must
reproduce the three defect *classes* under this recipe and seeded ablations, not
pretend it can reproduce the exact historical latencies or row shape (§9).

Released CLIs expose no open-only command. The embedded cold cell therefore
names and verifies the minimal adapter-owned `schema show --json` sentinel; its
cost includes that small public operation and is never labeled isolated open
time. The in-process §6.3 guard separately owns any engine-open-only claim.

### 6.2 Pre-release identity and handoff

A protected `benchmark-s3` GitHub environment owns the real-S3 credentials and
requires release-operator approval. A pre-tag workflow builds one Linux x86_64
release-profile candidate archive containing both `omnigraph` and
`omnigraph-server`. It measures those exact executables and records source
commit, `Cargo.lock` digest, exact Rust toolchain, Cargo profile, Cargo feature
set, target triple, and the separate SHA-256 of each binary, then emits the
candidate archive, one checksummed evidence bundle, and workflow-run ID. The
tag/release workflow verifies the source and all build inputs, then promotes
that exact archive as the Linux release artifact; it does not rebuild the
qualified Linux executables. Any packaging step must prove the extracted binary
digests unchanged. It does **not** claim that other post-tag platform binaries
were measured or share either Linux digest. V1 cost qualification is explicitly
Linux x86_64. The stable comparison archive likewise records separate CLI and
server digests rather than assuming one version string identifies both.

The bundle must pass every required cell, correctness oracle, budget, and §9
harness-evidence gate. A machine-readable waiver may disposition only a
performance threshold after those non-waivable gates pass. The release workflow
attaches the accepted bundle and comparison to the release notes. This requires
a matching update to `docs/dev/ci.md`; until enforcement lands, call the run an
operator preflight, not an automated gate.

### 6.3 First landing: close the known hole

Do not wait for the whole harness to add the focused cold-open guard. Extend the
existing `helpers::cost` owner first. The landing target is
`crates/omnigraph/tests/cold_open_cost.rs`: its local cell gates only the full-
graph manifest/scan terms the tracker demonstrably observes across history, and
its `s3_` cell gates latest-version/open terms that local `read_dir` can hide.
These are fixed structural request-count invariants, not latency or comparative
performance thresholds. Before claiming the guard closes the motivating hole,
a clean historical revert or isolated seeded perturbation must turn the relevant
cell red and current code must turn it green. The local cell adds at most 5 s to
the canonical workspace test body; the RustFS cell adds at most 10 s by adding
that exact target to the existing post-merge `rustfs_integration` default shard.
Both run on every push to `main`; a failure makes main stop-the-line until fixed
or reverted. The landing change updates `testing.md`, `ci.md`, and the workflow's
existing “cost gates are on demand” comment together to record this narrow
structural exception; broader cost and benchmark instruments remain on demand.
The RustFS cell is not moved onto PRs, so its existing cold engine compilation
is not duplicated there. An on-demand run alone would be a diagnostic, not the
regression guard claimed here. This lands independently of, and before, the
comparative harness.

## 7. Thresholds

The first full run establishes a candidate comparative baseline; it still must
satisfy correctness and any predeclared structural request ceilings, but it has
no relative-regression threshold to compare against. Thereafter:

- **Threshold keys are complete:** operation, access/lifecycle path, fixture
  recipe and size, history depth, backend, operation knobs, observation mode,
  and relevant runtime configuration. A baseline from another key is never
  silently substituted.
- **Per-operation physical-request ceilings** protect absolute cost on
  deterministic RustFS (and on any future backend that passes the direct-attempt
  gate), while edge/stable paired ratios and deltas protect regressions. Timing
  gates use only direct matched pairs; request and byte gates use only qualified
  proxied matched pairs.
- **Peak-RSS thresholds** use the direct paired process-lifetime HWM for every
  embedded coordinate and `server-startup`, keyed and baseline-reviewed like
  timing. `served-first-request` and `served-warm` retain only the explicitly
  advisory whole-server HWM and cannot fail a per-operation memory threshold.
- **A regression rule:** any operation beyond its budget must ship with a
  machine-readable waiver naming owner, cause, exact measured trade, and expiry
  or review release (for example "+455 GETs, −632 s wall, +$0.0002 per
  export").
- **A release rule:** no unexplained regression against the previous released
  version.

Thresholds and waivers are recorded in the harness's own manifest, not only in
prose, so a failing run points at the exact number and decision it violated.

## 8. Out of scope

- **Replacing `helpers::cost` or `scenarios.rs`.** Both remain the right tools
  for their jobs; this harness explicitly does not absorb them.
- **General per-PR performance gating.** Beyond the focused structural guard in
  §6.3 (which is mandatory post-merge), the CI budget forbids it and the value
  does not justify it.
- **Micro-benchmarks / Criterion.** Already rejected in `testing.md` for the
  right reasons: statistics over warm in-process iterations is the wrong model
  for multi-second stateful operations, and it measures no memory.
- **Telemetry services.** Historical trend storage itself is in scope — §4.4's
  versioned records and checksummed release assets are the V1 persistence
  story — but an operated OTLP collector, shared database, dashboard, and alert
  stack are not. Any future emitter or query database is a derived view.
- **Multi-machine or concurrent-load testing.** Single-client cost measurement
  only; throughput-under-concurrency is a separate instrument with a separate
  RFC if it is ever needed.

## 9. Evidence gates

The harness must prove itself before its numbers are trusted:

- **Proxy accounting is exact** — a synthetic client issuing a known request
  sequence reproduces exactly, including LIST-vs-GET, multipart, copy,
  conditional-write, repeated attempts, response outcomes, and byte-direction
  classification. Counter reset refuses while requests remain in flight.
- **No bypass / TLS correctness** — local backend logs reconcile with the
  proxy; the HTTPS spike proves the trusted-CA and unchanged SigV4 Host path
  before real-S3 physical counts are even recorded. They remain diagnostic
  until provider-side logs or another direct-attempt oracle reconcile them.
- **Logical cross-check** — on an instrumented local build, path-attributed Lance
  proxy traffic reconciles with Lance's process-local metrics at the documented
  logical-versus-physical boundary. Separately attributed sidecar/control
  traffic reconciles with the test-only `omnigraph-storage` counters below the
  engine facade. Their union must equal total proxy traffic with no unclassified
  or double-counted request; adapter traffic is never mislabeled as a
  Lance-metrics residual.
- **The proxy is not the clock** — direct and proxied passes produce identical
  logical results, while a calibration records the proxy's perturbation and
  proves proxied wall time is excluded from thresholds. Every gated RustFS
  sample also proves the proxy induced no timeout, retryable response, or
  throttle event that would change its request shape.
- **Fixture equivalence is enforced** — deliberately divergent schema, content,
  branch/history, pre-state, result, and post-state cases each fail the cell.
- **Released-build adapter is public-only** — golden fixtures for the previous
  release and edge normalize the exact supported CLI outputs; a removed command
  or unprojectable field marks the build unsupported rather than invoking an
  internal manifest decoder.
- **Lifecycle windows are real** — startup, first-request, and warm records show
  strict boot, exact `GET /graphs` inventory, warmup, counter reset, and
  in-flight-drain transitions; `/healthz` alone fails the gate.
- **Interleaving and repeats are real** — a recorded run shows stable pair IDs,
  fresh roots, alternating order, and per-sample values, not just aggregates.
- **Determinism is scoped** — two deterministic local-backend runs must agree in
  exact action counts and fingerprints. Real-S3 physical attempts may vary due
  to retries and remain diagnostic until the direct-attempt gate above exists;
  direct real-S3 timing still uses its declared paired thresholds.
- **Reproduction and sensitivity** — the bounded release-smoke manifest plus
  isolated seeded ablations must rediscover all three motivating classes: a
  fixed embedded cold-open toll, its amortization on the warm served path, and
  an export request multiplier. The separate 3 h historical-scale manifest must
  show deterministic request-shape deltas on RustFS and directional direct
  latency effects on the named backend before V1 is accepted. Because the
  original 2026-08-03 fixture was not preserved, the historical values (~55
  requests, +455 export requests, 643 s) are provenance, not exact acceptance
  thresholds. Claiming exact reproduction would be false; failing to rediscover
  the defect classes is still a failed harness.

## 10. Open decisions

1. **Shared history trigger** — local JSONL plus checksummed release assets are
   the accepted V1. Define the concrete query/retention need that justifies a
   shared prefix or derived SQLite index before adding either.
