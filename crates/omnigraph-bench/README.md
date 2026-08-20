# omnigraph-bench

The **end-to-end benchmark**'s harness (RFC 0039), currently implementing the
**micro profile** for branch-control workloads: one instrument measuring both
elapsed time (wall-clock + the 14-phase `MergeTimingPhase` attribution) and
storage calls per run, driving `branch_merge` on a parameterized multi-table
fixture. It is *not* the logical-cost comparator (DST counting golden) or the
real-backend qualifier — those are RFC-031's counting instruments; this one
adds time on top and names which tool a number came from. Per RFC 0039 rule 6
**nothing here gates anything, at any CI stage**.

Internal instrument: workspace member, `publish = false`, plain binary.

Profile note: the record declares `profile: "micro"`, and per-phase
attribution is served **in-process** while the engine's phase-timing exposure
is unshipped — an *implementation interim* per the RFC, named as such in every
record's `instrument_access` field, not a different instrument.

## Runs, points, and the run spec

A **run** applies a workload to a fixture under stated conditions, measuring
one SUT. Its **run spec** (Data / State / Workload / Environment / Protocol —
one level per factor) is the record's natural key; flattened it is the
**point name**:

```
m3-t8-n100k-btree-fresh-d50-warm
^scenario ^data      ^state  ^delta ^warmth   (+ -divD off-default, -p<bytes> off-default, -s3)
```

`list` enumerates every runnable point (known scenarios x frozen fixtures x
pre-tagged deltas x warmth regimes):

```bash
cargo run --release -p omnigraph-bench -- list --fixtures-root /path/to/fixtures
```

## Running (release only)

A debug build refuses to run, record, or freeze — a debug-build wall-clock
number must be impossible to write (rule 2). Storage-call counts would be
build-profile-independent, but the record carries both, so the guard covers
the record.

```bash
cargo run --release -p omnigraph-bench -- run --scenario m3 --warmth warm --out /tmp/bench-out
cargo run --release -p omnigraph-bench -- run --scenario m5 --warmth warm --out /tmp/bench-out
```

Defaults are the small dev center (T=12 tables, N=10k rows/table, 5 measured
reps, 1 discarded warm-up). The large center is reachable by flags
(`--tables 140 --rows 100000`).

Scenarios:

| Id | Shape | Defaults |
|---|---|---|
| `m3` | Three-way diverged mixed merge (updates+deletes+inserts on both sides, disjoint rows, overlapping tables), delta sweep at fixed N | d ∈ {1, 50, 5000}, 4 diverged tables |
| `m5` | Composite headline: every table diverged, small delta, one merge | d = 50, all T tables diverged |

The delta split per side is updates-first (`ceil(d/3)` updates, then deletes,
then inserts), so every d ≥ 1 carries at least one update per side and the
merge can never classify onto the proven-pure-insert shortcut.

Non-vacuous guards (every rep, warm-ups included): the outcome must be
`Merged` (not `FastForward` / `AlreadyUpToDate`), the phase timings must be
nonzero, and the post-merge row count of the first diverged table must equal
the planned `N - deletes + inserts`.

## Warmth regimes (rule 3: one per cell, declared)

`--warmth` declares the cell's regime; the record carries regime, discarded
warm-up count, and exactly what the regime did. Mixing regimes within a cell
invalidates it — records from early development builds did exactly that (rep 1 partly cold)
and read `"uncontrolled-v2"` after upgrade.

| Regime | Mechanism here |
|---|---|
| `warm` (default) | `--warmup-reps` (default 1) full divergence+merge repetitions run and are discarded, then measurement |
| `cold` | one **fresh process per measured repetition** (this binary re-execs itself; each process copies or rebuilds its own store); the parent folds the per-process rows into one record — one invocation, repetitions as rows |
| `post-invalidation` | warm-up, then the engine handle is dropped and reopened (engine + Lance session caches invalidated), then measurement. The OS page cache stays warm: the engine exposes no finer invalidation door at this commit, and the record's `warmth.detail` says so |

## Frozen fixtures: build, validate, refuse

`fixture build` builds a base store, runs the **validation pass**, and only
then freezes (RFC 0039: a fixture is validated once, before anything is ever
measured against it; failing validation removes the partial fixture — it
never freezes):

- row counts per table equal the spec's N;
- for `--index` builds: `ensure_indices` + `optimize`, then the id BTREE must
  be `Indexed` on every table with no unindexed fragments (the condition the
  indexed merge arms need);
- nothing is fetched (generated data), recorded as such — a fetched artifact
  would be digest-pinned here;
- a SHA-256 content digest of the frozen `store/` goes into the
  `fixture-manifest.json` **validation stamp**.

`run --fixture <dir>` **refuses** a fixture whose manifest lacks the stamp,
and re-digests the frozen store, refusing on mismatch (a mutated frozen store
cannot be measured against). A stamp-less fixture is stamped in place with
`fixture validate <dir>` (validates a copy, digests the untouched original);
an already-stamped fixture is refused — delete the manifest's `validation`
block (or rebuild) to force re-validation. Manifests below v3 are refused
outright: fixtures are disposable, rebuild them. `fixture builder-version`
prints the binary's builder version so scripts can compare it against a
cached fixture's manifest before skipping a rebuild.

```bash
cargo run --release -p omnigraph-bench -- fixture build \
    --fixtures-root /path/to/fixtures --tables 8 --rows 100000 --index
cargo run --release -p omnigraph-bench -- fixture validate /path/to/fixtures/fx-t8-n4k-scalars-noindex
cargo run --release -p omnigraph-bench -- run --scenario m3 \
    --fixture /path/to/fixtures/fx-t8-n100k-scalars-btree-fresh \
    --delta 50,5000 --warmth warm --out /tmp/bench-out
```

Fixture names derive from the tuple (`fx-t8-n100k-scalars-btree-fresh`);
cohort tags are delta-namespaced (`d50_src_upd`, ...) so one frozen base
serves the whole `--deltas` sweep; a delta outside the list is refused. The
frozen bytes are never mutated: runs copy `store/` to a per-point tempdir.

`scripts/generate_merge_bench_fixtures.py` (repo root) generates the
branch-merge micro-benchmark's v1 fixtures — this crate's three, not the
whole benchmark program's data: it builds the release binary and the
fixtures under `target/bench/fixtures`, generation only, no measurements. It skips a cached
fixture only when the manifest's builder version matches the binary's
(`fixture builder-version`), deleting and rebuilding on mismatch.

## The run record (schema v3, validated on write and read)

One JSON file per point invocation:
`<point_name>[_aa1|_aa2]_<invocation_id>.json` — the full point name keeps
distinct points from colliding, the invocation id keeps re-runs of one point
from colliding, and the file is opened `create_new` (a residual collision is
a hard error, never a silent overwrite; no code path rewrites a record in
place — RFC 0039: append-only until first cited). **One record = one
invocation**; repetitions are rows (per-rep arrays) inside it. A record is
identified uniquely by (spec, SUT, **invocation id**) — `invocation_id` is a
caller-minted ULID generated at invocation start, so identity never rests on
clock resolution; `invocation_unix_seconds` is persisted too but carries
ordering only. Every record also carries the **session id** (one ULID per
CLI invocation batch, RFC 0039) and the **point-name format version** (a
name is decodable only with its format).

The normative field list is `schema/run-record-v3.schema.json`, shipped with
the crate: the harness refuses to **write** a record that fails it, `diff`
refuses to **read** one. Top-level blocks:

- **`run_spec`** — the five classes: `data` (provenance, T, N, column shape,
  payload), `state` (F1–F5, the dataset-builder identity — `builder_version`
  + `generation` parameters, recorded on every run, inline builds included —
  and frozen-fixture provenance incl. the embedded validated manifest),
  `workload` (scenario, merge kind, arrival, d, split, diverged tables),
  `environment` (backend id, `s3_endpoint` when S3, and the **warmth
  declaration**), `protocol` (instrument, attribution, reps, timer, rep
  independence). The `profile` field is **derived** from the three
  profile-deciding levels (arrival, provenance, attribution — RFC 0039:
  decidable from a spec's levels alone), never asserted independently.
- **`sut`** — the system under test, deliberately outside the spec: the
  source commit (**embedded at build time** by `build.rs`, `-dirty` appended
  when the build tree had uncommitted **tracked** changes — `git status -uno`,
  so an untracked `.idea/` never marks a build dirty; a run-time git fallback
  is labeled `unverified:`), build profile plus the embedded
  `build_opt_level`, and **engine configuration as data** (every `OMNIGRAPH_*`
  environment
  variable at run time — a feature flag like `OMNIGRAPH_MERGE_LINEAGE` is a
  record field, never a prose label; values of secret-looking names
  (TOKEN/SECRET/PASSWORD/CREDENTIAL/KEY, case-insensitive) are stored
  `<redacted>` — the name persists, the value never does).
- **`machine`** — the auto-captured machine specification (cpu model, cores,
  memory, storage class), **record-level identity beside the SUT** (RFC
  0039: not a factor, auto-captured, so it sits outside the run spec; rule 4
  still forbids silent cross-machine comparison).
- **`results`** — wall-clock (p50/p95/min/max/mean + raw per-rep array +
  the persisted **`tail_support`** marker: `supported` needs >= 20 reps for
  p95, else `directional`, rule 3), per-phase totals (p50/max + raw
  arrays), the non-vacuous row check, write-path counters, and
  **`storage_calls`**: per-rep counts per RFC-031 operation class (get,
  put, put_part, head, list, delete, copy, rename, and the multipart
  operations — unobserved classes appear as zero), split into
  `manifest_store` / `table_store` (Lance object stores, counted via the
  engine's public `QueryIoProbes` wrapper seam through the `open_dataset`
  chokepoint) and `control_plane` (the engine's public
  `CountingStorageAdapter`). The counting wrapper forwards `rename_opts` to
  the target store (counting one `rename`) instead of inheriting the
  copy+delete default, so atomic renames stay atomic and land in the
  `rename` class. RFC-031 counts at two layers; the layer observed here is
  **logical operations** (`layer` field), and presence is stated per layer
  for every conditional column (RFC 0039): `physical_attempts`, the
  `concurrency_witness` (physical-layer, per-repetition span grain), the
  per-layer `cumulative_request_time_*`, and the `latency_calibration` are
  each explicitly `null` with the reason in their `_note` field — this seam
  counts requests but neither times them nor observes the physical layer,
  so the attempts x latency cross-check and elapsed reconciliation wait on
  a seam that does (latency calibration is the next measurement to wire).
  Counts cover the measured merge windows only and never gate anything
  (rule 6).

The build script re-embeds the SUT identity only when `.git/HEAD` or the git
index changes, so the standard dev loop — edit a tracked file without
staging, rebuild, run — re-links the binary with the **old** embedded commit
and dirty flag. `source_commit` closes that window at run time: when git can
still see the source tree and disagrees with the embedded values, every
record's commit gets a `-stale-build` suffix (marked, never refused) and a
one-line stderr warning says a rebuild clears it.

v1/v2 records stay readable: `diff` upgrades them on load, labeling their
warmth `"uncontrolled-v2"` and their machine/engine-config as not captured.

## The A/A noise floor (rule 7), diff, and show

```bash
cargo run --release -p omnigraph-bench -- run --scenario m3 --fixture F --aa --out /tmp/floor
cargo run --release -p omnigraph-bench -- diff DIR_A DIR_B --floor /tmp/floor/noise-floor.json
cargo run --release -p omnigraph-bench -- diff DIR_A DIR_B --format md
cargo run --release -p omnigraph-bench -- show /tmp/bench-out --format md
```

`--aa` runs every point twice at equal spec and SUT and writes
`noise-floor.json` (wall-clock and per-phase pair deltas per point, plus the
session id, the SUT commit, and the persisted default **claim margin** 2.0)
beside the `_aa1`/`_aa2` records; an existing `noise-floor.json` in the out
directory is refused before any measurement, never overwritten. `diff --floor` labels every wall or phase
delta against three bands: below the floor reads **"no detected effect"** —
never a small effect; between the floor and floor x margin is named the
in-between band (not a claimable effect); above clears. `--margin` overrides
the persisted default per invocation. A floor licenses only its own cell:
`diff` prints a loud **extrapolation** warning when the floor's SUT commit or
session differs from either compared record's.

`diff` matches records by their full **point name** (the spec flattened —
different specs never pair), prints both point names, wall-clock and
per-phase deltas plus write-path and storage-call sums, reports points
present in only one directory (both directions), and **warns loudly** when
identities differ (rule 4: backend or machine spec; rule 2: build profile;
rule 3: warmth), when any run-spec field differs, and when the two SUT
engine configurations differ (a systems comparison, shown as data).
Unsupported tails print the **directional** marker from the record.

`show` renders one record (or every record of a directory) as tables:
identity, wall-clock, the grouped phase view, write path, storage calls. The
14 raw phases render under seven plain-language groups (setup and refresh /
discover changes / table walk / validate changes / write merged data /
publish new state / crash-safety bookkeeping), each group a row with total ms
and its share of the wall-clock median, the raw phase names and µs indented
beneath — grouping adds a layer, it never replaces the data. `TableWalk` is
the general route's three-way row walk plus merged-row staging (one interval
per table; zero on the insert-only fast route). `KeyedStage`/`KeyedCommit`
are per-chunk sub-buckets of the write step (inside `PhysicalPublish` on the
insert-only route, recorded bare on the general route), so the write group
counts them once, never twice. Wall-clock the 14 phases do not cover prints
as an explicit **not phase-attributed** row (on records written before the
`TableWalk` phase existed, that remainder includes the then-uninstrumented
walk), never silently normalized away. Both `diff` and `show` take `--format md` for
GitHub-flavored markdown tables (paste-ready for PRs); the JSON record stays
the single source of truth, the tables are views.

## HTML report

```bash
cargo run --release -p omnigraph-bench -- report /tmp/bench-out --floor /tmp/bench-out/noise-floor.json
```

`report` renders every record of a directory (or one file) as a single
**self-contained** HTML page (default `report.html` beside the records): no
external requests of any kind, inline CSS and inline-SVG charts only, no
JavaScript (collapsed sections are `<details>`), light theme with dark via
`prefers-color-scheme`. Each record section leads with **metric cards**
(median merge, p95, storage requests, biggest cost), then the seven grouped
phase bars (share of wall-clock p50, one-line annotation, raw phases in a
`<details>` fold — same grouping and unattributed-remainder rules as `show`),
then the storage-call total and per-class split (with the S3 note: every
request is one network round trip, so the count predicts cloud cost),
write-path counters, and the full identity block collapsed — with the same
honesty as the terminal views (full SUT commit incl. `-dirty`/`-stale-build`
markers, directional tail badges). With `--floor`,
A/A pairs in the directory (two records per point name) get a rule-7 delta
section — below-floor deltas read "no detected effect". General two-directory
diff pairing stays with `diff`; the report does not duplicate it.

## Backend seam (MinIO later)

Default backend is a fresh local tempdir per run (backend id
`local-fs-tempdir` — the same local-FS backend the engine's own integration
tests use; the engine has no whole-graph in-memory backend, the `memory://`
adapter covers control objects only). `--root-uri s3://bucket/prefix`
switches the entire store to any S3-compatible backend through the engine's
normal `AWS_*` environment plumbing (`AWS_ENDPOINT_URL_S3`, ...), which is
where a digest-pinned MinIO/RustFS slots in; the endpoint lands in the
record's environment block. Nothing in this crate is MinIO-specific; a unique
sub-prefix is appended per run.

**S3 sub-prefixes accumulate.** Each run writes under a fresh
`bench-<scenario>-d<delta>-<nanos>/` sub-prefix and the harness never deletes
anything (deletion is an operator action). After a bench campaign, clean up
with a command of this shape (operator-executed; `--dryrun` first):

```bash
aws s3 rm --recursive --dryrun s3://bucket/prefix/
aws s3 rm --recursive s3://bucket/prefix/bench-m3-d50-1755600000000000000/
```

## Baseline records (v1/v2)

Earlier baseline runs exist as v1/v2 records (`five_tuple`, uncontrolled
warmth) from before this schema; `diff` still reads them, upgrades on load,
and their per-rep arrays keep the warmth drift visible.

## What benchmark v1 defers

- `run --point <name>` (point names exist and `list` enumerates them; the
  reverse parse is not wired), declarative case files, and the `Scenario`
  trait for new scenario kinds.
- Presentation exports beyond markdown (CSV) and the trend/progress view
  (`diff`/`show --format md` exist).
- State axes F1/F4 as real sweeps; scenarios M1/M2/M4, C family, D family.
- The realistic profile (open-loop driver, on-time validity, quality floor,
  cost) — rule 1 has no purchase on the micro profile's single-operation
  workload.
- Vector/blob column shapes, edge tables, topology; MinIO digest pinning and
  any real-S3 run (operator-executed); S3-hosted fixture archives.
- Branch cleanup between warm reps (cold reps are fully independent
  processes; warm reps accumulate 2 branches each and deeper `__manifest`
  journal history — the record's `rep_independence` note carries it).
