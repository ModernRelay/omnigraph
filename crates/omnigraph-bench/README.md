# OmniGraph benchmark harness

`omnigraph-bench run tiny-read` runs a named GQT benchmark in a supervised
release worker. One [`benchmarks.yaml`](../../benchmarks/benchmarks.yaml)
selects fixtures, workloads and run settings. `run --config FILE` accepts the
same format for custom experiments; [custom.example.yaml](../../benchmarks/custom.example.yaml)
is a complete starting point. The measured operation is selected by ordinal
plus exact header/body echo. GQT remains the language for data,
operations, and expectations; it has no benchmark timing or cache labels.
The production parser/executor comes from `omnigraph-gqt-core`, which has no
build script or normal `test-util` requirement. The GQT binary separately owns
correctness/DST execution and never supplies these wall-clock measurements.

The [catalog guide](../../benchmarks/README.md) contains complete examples. The
[`local-regression` group](../../benchmarks/benchmarks.yaml)
contains bounded history, idle-registration, multi-table, branch-identity,
churn, configured history-release, and equal-current/different-history pairs.
The larger [history recipes](../../benchmarks/README.md#opt-in-history-scale) are opt-in.
For a qualified flag-free release build, the existing build command is
`RUSTFLAGS= cargo build --release --locked -p omnigraph-bench`; this clears the
workspace's development `--cfg tokio_unstable`. Neither that flag nor debug
assertions may remain in a measured worker. Then run a small suite:

```bash
target/release/omnigraph-bench run local-fast \
  --dataset-cache /qualified/cache --json
```

Use `list fixtures`, `list workloads` and `list scenarios` to discover inputs;
`show NAME` reports effective settings and `show --workload FILE` lists operation
ordinals. `init --help` explains config generation. All support JSON output.
`cache status NAME` and paginated `cache list` are read-only: `present` validates
published evidence, while `--verify` audits bytes before reporting `cached`.
Missing sources and absent cache entries remain distinct. See `help config`,
`help cache`, or the versioned `help --json` schema. Inspection/help do not need
a release build or a dataset cache.

## Execution and identity boundaries

A scenario is one selected operation on a fixture. Groups select scenario
IDs, while the config's explicit `run` list supplies default selections. The
runner expands these into the existing validated-case representation before
execution; legacy case/suite file readers remain supported. File paths and source text are resolved/frozen before workers start.
Workers receive bounded immutable text and never reread authored files.
Selected ordinals must have matching exact echoes and cannot occur in loops.
Generated selected loads must make one loader call. Prefix writes are refused,
and at least one following step must carry an explicit verification
expectation. A restart alone does not meet that requirement.

For embedded execution, the prefix derives process-cold, warmed-by-program,
or reopened-after-program treatment. Settings/show are neutral. Warming requires observed engine-read
callbacks, not merely a syntactic query whose parameters fail before execution.
Every repetition uses a fresh process. OS page-cache state is uncontrolled or
conditioned by the named `gqt-read-set-v1` program; neither page-cache-cold nor
storage-cold is representable.

For embedded execution, the engine operation timer and logical counters close
before expectations and verification. Reads materialize results within the interval. A selected
restart installs fresh counting storage before timing, measures the engine
open, and validates its preparation gate after the interval. Generated load
rows are prepared outside a single-call measurement. Optional merge probes
exist only for selected merges with per-phase attribution. The result carries
operation-kind/ordinal/occurrence receipts, selected elapsed time, verification
facts, and optional applicable merge evidence. Physical retries, pagination,
multipart fan-out, cloud costs, calibration, and concurrency witnesses remain
absent unless the record explicitly says otherwise.

A later assertion, timeout, protocol, or process-exit failure rejects the
repetition. Accepted Settled timing can survive as diagnostic evidence without
becoming a valid sample. The parent enforces a separate measured-operation
watchdog and preparation/verification bounds; the authored whole-file GQT
budget also applies. The operation alone owns the benchmark clock. Parsing,
building, restore, warm-up, assertions, archival work, and cleanup are excluded.

Embedded dataset construction runs in a bounded child process. The cache builds at its
final `<key>/active` path, closes every engine handle, freezes a never-opened
`root/` template, retires active, and publishes `fixture-source.json` followed
by `dataset-build.json`. The parent accepts only after reap, process-group
exit, and clean bounded output capture. The exclusive cache lease remains
held through all repetitions and final cleanup. APFS requires forced clonefile;
qualified XFS uses verified plain copies. Both restore the exact original
active path so Lance shallow-branch references remain valid. Relocatable
entries also use this conservative stable-path policy.

`dataset build <dataset.gqt>` builds without a measured workload. Optional
`--queries <queries.gqt>` contributes index requirements; `dataset build
<case.yaml>` uses that case's exact union. Indexes are prepared with the seed
before authored updates/deletes. Dataset-only and query-required index variants
use different cache keys. `dataset validate` and `run --no-build` require
a published hit. A hit validates descriptor, logical receipt, and template
bytes once; APFS repetition preflight uses metadata/clone witnesses rather
than rehashing the whole template. Plain-copy reset necessarily reads bytes
outside timing.

Cache keys bind recipe, engine/builder source digest, backend/reset, index
needs, canonical cache location, and registered physical source identity where
applicable. `GQT_ENGINE_DIGEST` covers production engine/core dependencies and
benchmark build/observer/reset code, plus manifests, lockfile, toolchain and
configuration. Authored inputs use separate text digests. Cache hit status and
physical tree digest are durable audit facts outside point identity. Missing
or changed published evidence is corruption, never a silent rebuild. Unknown
unpublished state and uncertain process containment are preserved/quarantined;
operator inspection must establish quiescence before removing such state.

Final `point_id` binds a dataset logical witness, verified by embedded acquisition
or declared in the served deployment receipt, plus recipe/query contents,
the selected ordinal/echo, cache treatment, target, network position, backend
and protocol. It is
unavailable during syntax-only planning; `planned_sha256` detects duplicate
content experiments before build. Paths, human case IDs, repetitions, physical
tree bytes, and cache hits do not change the point. For embedded acquisition,
dataset identity is computed from all live branches' keyed-node properties,
edge endpoints/properties and
multiplicity, schema/index inventory, and normalized two-parent commit DAGs.
Generated unkeyed edge IDs, commit ULIDs/timestamps, and physical manifest
versions are excluded. Explicit IDs authored in a recipe remain bound by its
text digest. Historical row images are not attested. Both parent links must
resolve within bounded traversal, and parent generations must be smaller.

Historical `branch-merge-v1` records retain their original field ordering and
canonical bytes and remain readable beside GQT records. Their old authored
execution and Rust fixture builder are retired; the legacy builder is a test
oracle only. D50 fixture parity covers construction equivalence. Its new warm
prefix consists of 24 authored aggregate queries and is not comparable as the
same cache program to the retired Rust scans.

Children use a cleared environment, fixed locale, and protocol-owned scratch
siblings for `TMPDIR`; measured workers also use that scratch for
`OMNIGRAPH_MERGE_STAGING_DIR`. Only modeled canonical `LANCE_MEM_POOL_SIZE`
values are inherited. Unknown engine/Lance settings and Tokio/Rayon thread
count overrides are refused. Each child attests executable digest, source,
release/compiler facts, engine features, and machine identity. Effective
LTO/codegen/strip options remain unproved without a controlled build receipt.
Cleanup requires direct-child reap, process-group exit, and clean stdio;
uncertain containment cannot release a cache entry for safe reuse.

## Served read-only acquisition

`query-shapes-served` and `traversal-served` add server-target variants with
separate workload sources and point identities. Select one scenario against
an already provisioned graph:

```bash
target/release/omnigraph-bench run e2e-query-count-served \
  --server http://127.0.0.1:8080 --graph bench \
  --server-receipt /path/to/deployment.json \
  --server-token-env BENCH_SERVER_TOKEN \
  --dataset-cache /path/to/client-scratch --repetitions 5 --json
```

The token option names an existing environment variable; it never takes the
credential itself on the command line. Choose a separate name outside the
`LANCE_` and `OMNIGRAPH_` runtime namespaces. The endpoint must use HTTP(S), with no
URL credentials, query or fragment, and name the server base rather than a
`/graphs/...` path. Credentials travel to each client worker
only in its bounded private stdin frame. Durable evidence stores the SHA-256
of the normalized endpoint, not its raw URL. URL normalization canonicalizes
the host and default port, then trailing slashes are removed; for example,
`http://LOCALHOST:80/` becomes `http://localhost`.

Provision and validate the graph once outside acquisition. Supply a JSON
receipt declaring the deployed server and that graph's dataset. The receipt
file is at most 8 KiB, and the receipt together with the observed client build
and machine identity must fit 8 KiB:

```json
{
  "format_version": 1,
  "endpoint_sha256": "<SHA-256 of normalized endpoint>",
  "graph": "bench",
  "server": {
    "package_version": "<deployed version>",
    "source_commit": "<full lowercase commit hash>",
    "source_tree_dirty": false,
    "profile": "release",
    "cargo_opt_level": "2",
    "debug_assertions": false,
    "artifact": {"kind": "executable", "sha256": "<executable SHA-256>"},
    "target_triple": null,
    "rustc_version": null,
    "engine": null
  },
  "backend": {"kind": "local-fs", "filesystem": "apfs", "storage_class": "nvme-ssd"},
  "dataset": {
    "recipe_sha256": "<selected dataset recipe SHA-256>",
    "logical_content_sha256": "<validated deployment dataset SHA-256>",
    "algorithm": "omnigraph-gqt-branches-lineage-equivalence-v1"
  },
  "machine": null
}
```

Replace every placeholder with deployment evidence. An image digest may use
`artifact.kind: image` instead. The optional machine, engine, target and
compiler fields remain absent when unproved; supplied values use the same
strict types as embedded records. The receipt binds declared facts, not remote
attestation: the public version API cannot prove the server's source, profile,
artifact or dataset content. GQT row expectations check the workload results,
but do not independently verify the declaration. Every served record therefore
has `claim_eligible: false` and `sut.kind: declared-deployment`.

One invocation accepts one receipt, endpoint and graph. All selected scenarios
must use that receipt's dataset recipe and backend; incompatible groups fail
before archive creation or network activity. The groups are discovery sets:
run their different fixture recipes in separate invocations against matching
provisioned graphs. A served definition must declare `network_position`; a
server target without it is refused. For another deployment, author an explicit
server backend and `same-host`, `same-region`, or `remote` network position in
custom YAML;
these declarations enter point identity and are never inferred from the client.
Explicit `--dataset`/`--queries` pairs are embedded only and refuse `--server`.

The entire schema-less query program must be read-only, including loop bodies
and verification suffixes. Mutations, loads, restarts, index requirements and
registered-fixture recipes are refused. Settings prefixes on read steps are
refused for served acquisition. Acquisition does not seed, restore,
restart or clean up the server. It creates a fresh client worker per repetition
and sends only the admitted GQT reads. Timing uses the shared BenchHost boundary
and includes the selected request and result materialization; warm-up and
assertions stay outside. The server lifecycle is `long-running-server`, with
`reset: none` and `attribution: off`. Zero warm-up means uncontrolled server
preparation; an executed read prefix proves only warmed-by-program. Server OS
page-cache state always remains uncontrolled. With zero warm-up reads the
measured request also carries the client's connection setup (TCP, and TLS over
HTTPS).

The served interval includes request serialization on the client, the server's
response encoding and the row decoding on the client; the embedded interval
ends at the engine's Arrow batches, before any JSON rendering. Served and
embedded points are never pooled, and their timings are not comparable beyond
that difference.

Client executable and machine evidence are distinct from the declared server
SUT. Samples report `client_peak_rss_bytes`; server RSS, logical storage calls,
physical attempts, per-storage-request timing and concurrency witnesses are absent. The
projection preserves those absences as nulls and marks the SUT evidence
`declared-deployment`. Killing or reaping a timed-out client proves only client
containment, not server-side cancellation. Existing clean-client release and
archive publication guards still apply.

## Logical references for real graphs

An externally built graph first needs a strict logical declaration. A
`fixture-reference-v1.yaml` records only rebuild-stable identity:

- the imported-graph builder version, digest-pinned recipe and inputs, and
  typed parameters;
- Data as canonical schema shape, non-empty node and edge inventories with
  exact row counts, payload/column shape, provenance, and topology skew;
- State as aging, structured index inventory, deletion history, compaction
  recency, and history depth; and
- the full logical-content digest that `fixture validate-graph` recomputes
  against a byte-verified disposable copy.

The checked-in FinGraph declaration is
`benchmarks/fixtures/finbench/finbench-2026-08-21-sf10-v1.fixture-reference-v1.yaml`.
Its implemented algorithms are `omnigraph-schema-shape-v1`,
`omnigraph-logical-properties-v1`, and
`omnigraph-logical-graph-multiset-v1`. The document remains deliberately
location-free and scalar.

Builder parameters are limited to `null`, booleans, and unsigned integers;
arbitrary strings cannot store literal paths, URIs, or credentials. Parameter
names and numeric meanings are still trusted builder-contract input that a
builder contract must define. Input roles are path-free semantic labels and
each input's content is pinned by SHA-256. The current validator verifies the
resulting graph facts; it does not replay the import recipe or resolve its
inputs.

Validate the declaration with:

```bash
target/release/omnigraph-bench fixture reference validate \
  benchmarks/fixtures/finbench/finbench-2026-08-21-sf10-v1.fixture-reference-v1.yaml \
  --json
```

The result reports `reference_sha256`, an audit hash of the complete normalized,
versioned declaration. It is not a point id. This command validates and
normalizes the document only; it does not open a store or prove a supplied
digest. `fixture observe-graph` derives the implemented witnesses, and `fixture
validate-graph` compares all of them with the declaration while also requiring
a relocation-self-contained graph. Aging, deletion history, compaction
recency, unknown raw Lance index metadata, and per-index FTS/ANN freshness
remain explicitly unverified, so the result is claim-ineligible. See the
[benchmark catalog](../../benchmarks/README.md#observe-and-validate-a-registered-graph)
for the exact observation and validation commands.

## Registered fixture byte verification

Large graph snapshots can be registered without putting a machine path, S3
URI, account, or credentials into their identity. A strict
`fixture-source.json` describes only a format version, a path-free id, and the
physical tree identity:

```json
{
  "format_version": 1,
  "fixture_id": "monarch-main-20260829",
  "physical": {
    "digest_algorithm": "omnigraph-bench-physical-tree-v1",
    "tree_sha256": "<64 lowercase hex characters>",
    "files": 16279,
    "bytes": 3893489669
  }
}
```

Keep the descriptor beside, not inside, the graph root. The local bundle
convention is deliberately small:

```text
BUNDLE/
  fixture-source.json
  root/
```

After an operator has quiesced a local snapshot tree, fingerprint its complete
bytes into the small descriptor:

```bash
target/release/omnigraph-bench fixture fingerprint \
  --id monarch-main-20260829 \
  --root /mnt/nvme/fixtures/monarch/root \
  > /mnt/nvme/fixtures/monarch/fixture-source.json
```

The command only prints JSON; it does not modify the tree or write a registry.
Bind any later local copy to that descriptor and re-read its complete bytes with:

```bash
target/release/omnigraph-bench fixture verify fixture-source.json \
  --root /mnt/nvme/fixtures/monarch/root --json
```

Success returns the canonical typed `source_descriptor_sha256`, the canonical
local path, and the observed physical identity. The local path and descriptor
digest are transport/audit evidence, not the harness's stamped logical fixture
manifest and, per RFC 0039, must not become `point_id` input.

Exercise the same-invocation binding and copy seam with a repeatable
`ID=BUNDLE` mapping:

```bash
target/release/omnigraph-bench fixture preflight-copy \
  --fixture monarch-main-20260829=/mnt/nvme/fixtures/monarch \
  --scratch-root /mnt/nvme/omnigraph-bench-scratch \
  --json
```

The command resolves the two required direct bundle entries, checks that the
binding id matches the typed source descriptor, verifies the source while
copying it into a private disposable workspace, re-digests the copy, and
explicitly removes the workspace. Duplicate/malformed mappings, symlinked required
entries, source drift, copy failure or drift, and cleanup failure all fail
closed. Paths are invocation-only and do not appear in the success result.
Unrelated bundle siblings are ignored. Bindings are copied and removed one at
a time, so peak scratch usage is bounded by the largest fixture rather than
their sum. Because this is a copy preflight, no staged tree remains after
success.

`fixture preflight-copy` remains physical-only: it has no independent logical
expectation, so replacing both `fixture-source.json` and `root/` under the same
id produces a different successful physical receipt. `fixture observe-graph`
and `fixture validate-graph` build on this seam. They retain the verified
disposable copy, open only that copy read-only, compute the canonical schema,
payload, and full node/edge content witnesses, check table/index/history
observations and relocation safety, recheck its physical bytes, and then clean
it up. They neither download from S3 nor open or mutate the registered source
as an OmniGraph database.

Registered datasets use the same `gqt-v1` case/suite/cache/archive path. A
logical reference and schema-less GQT preparation are part of the recipe;
`--fixture ID=BUNDLE` supplies only the current source location. The source is
never opened directly as a database. Current imports require a main-only,
relocation-self-contained graph with deterministic node `@key` columns;
unkeyed nodes and named source branches are explicit scope refusals. An
identity-aware export digest binds the fixed source's original IDs before
preparation. Prepared generated edge IDs use the documented logical-equivalence
domain. See the [registered FinBench recipe](../../benchmarks/README.md#registered-finbench-merge).
The former `fixture run-graph` command and run YAML are retired.

The local source namespace is a trusted-input boundary: the operator must keep
it quiescent for the command. Digest/copy verification detects ordinary drift
and prevents a mismatching copy from succeeding, but this path traversal is
not a sandbox for an adversary racing file types or path entries.

## Durable records and archive

Passing `--archive <DIR>` changes successful `suite run` finalization from a
diagnostic-only run into durable telemetry publication. The harness mints one
session ULID for the command and one invocation ULID per suite entry. Each
record contains the complete typed run spec, exact point identity, raw repetition
rows, dispersion, and explicit evidence-presence facts. Embedded records also
contain clean source and release-build evidence, an executable digest,
process-effective machine and backend evidence, a stamped fixture manifest,
and logical calls. [Served records](#served-read-only-acquisition) keep the
declared deployment receipt and observed client evidence separate; unavailable
server measurements remain absent.

A dirty or unproved source tree cannot publish a record because the source
commit would not honestly describe its provenance. The local CLI rechecks that
its build checkout still names that exact commit and has neither tracked nor
untracked changes before archive preflight and again at every publication
boundary. That proof uses a pinned system Git, binds the exact git directory
and worktree, disables replacement objects and hostile stat-cache settings,
compares tracked source as raw bytes without clean filters, and refuses
assume-unchanged or skip-worktree entries. Ignored files in workspace
source/config roots are still source and therefore make the build dirty. The
measured worker must independently attest the same clean commit.
The exact executable is identified by its digest and normalized
compiler/build/engine facts. Build after committing the intended source, then
verify the resulting archive:

```bash
target/release/omnigraph-bench run local-smoke \
  --archive .bench/archive

target/release/omnigraph-bench archive verify .bench/archive
```

If publication reports `possibly_published`, reconcile that exact candidate
before minting a replacement invocation:

```bash
target/release/omnigraph-bench archive reconcile .bench/archive \
  --invocation-id <INVOCATION_ULID> \
  --record-sha256 <RECORD_SHA256> --json
```

Reconciliation holds the publication lock, validates the exact immutable
pointer and canonical record, and retries the required file/directory syncs.
It returns `durable`, `absent`, or `conflict`; only `durable` exits successfully.

Canonical compact JSON objects live below `objects/sha256/`. Publication makes
the object durable first, then atomically installs an immutable invocation
pointer below `invocations/`. Only reachable, fully validated pointers are
records; a crash can leave an unreferenced content object but cannot expose a
partial record. Reusing an invocation for unequal bytes fails closed, and
publishing identical bytes is idempotent. The JSON archive is the only result
authority. `archive verify` streams a fixed invocation inventory and returns a
compact count and inventory digest; it does not retain or print the complete
record set.

A run that verifies every requested repetition publishes a `complete` record.
If a later repetition fails after at least one earlier repetition was fully
measured and verified, archive mode publishes exactly that verified prefix as
a `censored` record, persists the failed repetition index plus a closed stable
stage and canonical error-code token,
and still exits nonzero. A repetition that merely reached `Settled` is never
included. A failure before the first verified sample publishes no record.
Censored records are permanently ineligible for performance claims. A complete
acquisition is necessary but not sufficient: `claim_eligible` also requires
every record-level proof gate, including effective-codegen proof. The current
local publisher deliberately lacks that proof, so its complete records remain
useful evidence with `claim_eligible: false`. The projection exposes the
status, eligibility, and nullable terminal fields.
Every durable embedded raw sample includes supervisor-observed peak RSS; served
samples label that measurement as client peak RSS.

The current archive durability contract is local Unix filesystem durability.
The content object and immutable pointer are synced through descriptor-rooted
directory chains from their containing shards back through the captured
archive root, with path/inode revalidation after each directory fsync. Only
`EINTR` is retried, on the same already-open descriptor. Inventory readers fix
the pointer inventory under the shared publication lock, then durability-close
each visible immutable record exactly once before yielding it; they reject the
stream if that proof still fails. Any publication failure after pointer visibility is reported as
`possibly_published` and the publisher must reconcile that exact identity
before retrying; an orphaned content object is not inventory.

Machine evidence records OS/kernel/CPU/memory facts, the worker-inherited nice
level and scheduler policy/priority, and a versioned digest of a fixed common
set of soft/hard process resource limits. The hostname-derived label omits the
raw hostname but is only a non-secret, non-stable correlation hint: it is not
anonymization, a privacy boundary, or proof of machine identity. On Linux the
record also includes process CPU affinity and the effective cgroup-v2
CPU/memory limits plus a bounded fingerprint of every stable controller
setting across the inherited hierarchy; cgroup-v1, hybrid control, and
scheduler policies whose canonical parameters are not fully represented are
refused rather than published with incomplete identity.
Every repetition worker captures this identity immediately before `Ready`;
the parent refuses a run if any repetition differs, and record finalization
uses that worker-attested identity rather than a long-lived CLI snapshot.

## Rebuildable query projection

The team-facing query database is an OmniGraph read model generated from the
complete archive. It is never written in a measured window and has no
incremental mutation API:

```bash
target/release/omnigraph-bench projection rebuild \
  --archive .bench/archive \
  --root .bench/projection

target/release/omnigraph-bench projection list-points \
  --root .bench/projection --limit 100

target/release/omnigraph-bench projection list-runs \
  --root .bench/projection \
  --point-id <FULL_SHA256_POINT_ID> --limit 100
```

Rebuild validates every archive record, collision-checks point and invocation
identity, loads a fresh bounded generation through the public engine surface,
verifies its complete inventory and a canonical digest over every projected
point and run field. Only then does it atomically replace `CURRENT`. Public and
internal queries are bounded and use exclusive cursors pinned to one immutable
generation. Pass the JSON `next_cursor` from one page back through `--cursor`
to continue. A generation id is derived from the projection schema,
source-to-row transform contract, sorted archive inventory, and complete
projected-row digest, so neither stale formulas nor bad field mappings can be
reused silently. Rebuilds serialize across processes with a bounded lock wait,
clean abandoned staging directories, and retain at most eight published
generations; reaching that ceiling requires deleting the disposable projection
root and rebuilding it.
Query callers choose fixed, parameterized names; arbitrary GQ text is not
accepted. The projection may be deleted at any time and rebuilt without losing
evidence.
For `declared-deployment` rows (`sut_evidence` and `backend_evidence`), the
build, machine and backend columns hold the receipt's declared values, and the
observed client is in `client_build_json` and `client_machine_json`.

## Local support envelope

The adapter accepts direct-engine, local-filesystem GQT environments without
DST seams or concurrent actor blocks. The admitted tuples are APFS with
local-clonefile or qualified XFS with plain-copy, on the declared NVMe SSD
backend. The platform probe remains authoritative. Cloud stores, unproved embedded backend declarations, and OS page-cache
eviction claims are refused; the served path uses the explicit deployment
declarations described above; unsupported diagnostic sources are retained under `benchmarks/deferred/`.

A frozen source is bounded at 512 KiB, the combined plan at 256 KiB, the bound
request reservation at 512 KiB, and the actual framed request at 1 MiB. The
selected echo is at most 16 KiB and must fit the projection's serialized row
budget. Query programs are limited to 4096 expanded steps. Before acquiring or
building a dataset, the runner bounds all repetitions' canonical receipts plus
2 MiB reserved for the record envelope against the 64 MiB archive limit. Reduce
repetitions when this bound is exceeded; the final record size is also checked.
Dataset manifests are bounded at 1 MiB. Dataset history
observation is bounded at 1024 live branches and one million reachable commits
per branch; both parent links must be available. Generator-specific bounds
remain in the core GQT grammar. These limits are explicit refusals, not silent
truncation or fallback execution.

Without `--archive`, `suite run --json` still emits a versioned diagnostic
execution projection whose runs have `durable_record: false`; it must not be
copied into the archive. With `--archive`, each complete run is canonically
encoded, published, and then dropped from CLI memory. A failed run may publish
one censored verified prefix under the rules above, while the command remains a
failure. The success output omits
the raw `runs` array and carries only `completed_run_count` plus authoritative
content addresses and invocation pointers. Archive-mode failures likewise
report the completed count and known receipts without duplicating previously
published raw samples. A complete execution or censored verified prefix whose
record could not be published is retained once as state-neutral
`unpublished_run` recovery evidence, because an indeterminate pointer sync may
already be authoritative. That sync also carries
`possibly_published` for candidate-specific reconciliation. Human-mode failures
print the same complete recovery JSON envelope, not only a timing summary. If
acquisition fails and publishing its censored prefix also fails, that envelope
retains the recording error and the complete structured acquisition error as
separate fields.
Controlled cloud orchestration remains a later, separately reviewed slice.

Machine-readable diagnostic-mode failures keep the suite/case/point identity
and all completed runs or repetitions. A worker killed at its hard deadline
contributes structured process-containment evidence, but never a partial
sample.

## End-to-end query and traversal workloads

The shared [catalog](../../benchmarks/benchmarks.yaml) owns the `end-to-end`,
`query-shapes` and `traversal` groups. They execute complete public graph
operations with expected rows authored in GQT. Each case identifies its own
fixture size and cache treatment; bounded replacements have new experiment
identities and do not preserve the historical Rust timing series.

The [deferred inventory](../../benchmarks/deferred/README.md) preserves unsupported
maintenance, concurrency, transport and scale experiments outside Cargo target
discovery. GQ must expose maintenance operations before those scenarios return;
GQT adds no separate cleanup or optimization command.
