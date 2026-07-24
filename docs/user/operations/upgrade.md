# Upgrading across a storage-format change (export / import)

Omnigraph storage is **strict-single-version**: a binary reads exactly one
internal-schema (storage-format) version. There is no in-place migration. When a
release changes the internal schema, a graph created by an older release is
**refused on open** with a message that points here, and you move it forward by
rebuilding it: export with the old binary, then `init` + `load` with the new one.

This is a deliberate pre-release design choice. The rationale (lower long-term
liability than carrying in-place migration code for a format that is still
changing) is in [docs/dev/versioning.md](../../dev/versioning.md).

## How you know you need this

Opening a graph whose stamp is below the binary's version fails with a
message that **names the release line that wrote it** and the exact commands —
so you can fetch the right old binary without guessing:

```
__manifest is stamped at internal schema v8, but this omnigraph reads only v9.
This graph was created by omnigraph 0.12.x. Rebuild it: with an omnigraph
0.12.x binary run `omnigraph export <graph> > graph.jsonl`, then with this
binary run `omnigraph init --schema <schema.pg> <new-graph>` and `omnigraph load
--mode overwrite --data graph.jsonl <new-graph>`. (Data, vectors, and blobs are
preserved; commit history and branches are not.) See docs/user/operations/upgrade.md.
```

### Which old binary do I need?

The on-disk stamp maps to the release line that wrote it. Export with any binary
from that line (the latest is safest):

| On-disk stamp | Written by | Export with |
|---|---|---|
| internal schema v1 | omnigraph ≤ 0.3.1 | any 0.3.1-or-earlier binary |
| internal schema v2 | omnigraph 0.4.1–0.6.1 | the latest 0.6.x (e.g. 0.6.1) |
| internal schema v3 | omnigraph 0.6.2–0.7.2 | the latest 0.7.x (e.g. 0.7.2) |
| internal schema v4 | omnigraph 0.8.x | the latest 0.8.x (e.g. 0.8.1) |
| internal schema v5 | omnigraph 0.9.x | the latest 0.9.x |
| internal schema v6 | omnigraph 0.10.x | the latest 0.10.x |
| internal schema v7 | omnigraph 0.11.x | the latest 0.11.x |
| internal schema v8 | omnigraph 0.12.x | the latest 0.12.x |
| internal schema v9 | omnigraph 0.13.x | — current format; no rebuild needed |

You can also check versions before you hit a refusal:

- `omnigraph version` — the binary's served version (the `internal-schema <N>` line).
- `omnigraph snapshot <graph>` — the graph's on-disk `internal_schema_version`.

If the graph's stamp is **higher** than the binary's, the binary is too old —
upgrade omnigraph rather than rebuilding the graph.

## What is preserved (and what is not)

| Preserved | Not preserved |
|---|---|
| All node and edge rows | Commit history (the graph DAG starts fresh) |
| Vector columns (embeddings round-trip verbatim) | Branches (export is a single-branch snapshot) |
| Blob columns | Snapshot/time-travel history of the old graph |
| The schema (re-applied at `init`) | |

The rebuilt graph is a faithful copy of the exported branch's **current state**.
Commit history cannot be carried forward. To preserve the current states of
multiple branches, export each branch you care about separately and rebuild
each export into a different graph root. Every result is a main-only graph with
a newly minted identity domain and self-contained history; the old branch
topology and shared ancestry are not reconstructed.

## The recipe

Use the **old** binary for the export steps and the **new** binary for init/load.
Keep them as separate executables (for example a downloaded release archive) so you
can run both.

```bash
# 1. With the OLD binary — capture the schema and the data.
old-omnigraph schema show   s3://bucket/graph.omni > schema.pg
old-omnigraph export         s3://bucket/graph.omni > graph.jsonl

# 2. With the NEW binary — create a fresh graph and load the data.
omnigraph init --schema schema.pg s3://bucket/graph-v2.omni
omnigraph load --mode overwrite --data graph.jsonl s3://bucket/graph-v2.omni

# 3. With the NEW binary — verify.
omnigraph snapshot s3://bucket/graph-v2.omni     # internal_schema_version is current
omnigraph version                                 # confirms the binary's served version
```

`omnigraph export` writes a full JSONL snapshot (one row per node/edge, all
columns including vectors and blobs) of the chosen branch (default `main`; pass
`--branch` for another) to stdout. `omnigraph load --mode overwrite` replaces the
target graph's contents with that snapshot.

Once you have verified the rebuilt graph, retire the old one. If you rebuilt
through a storage-format boundary, the target must be a different URI: keep the
source root intact until row/vector/blob verification and fleet cutover are
complete. Do not use force-init to turn the old root into the new format.

## Notes

- **Upgrade the whole fleet together.** A mixed fleet where an old binary still
  writes a graph a newer binary has stamped is unsupported, as with any
  internal-schema bump.
- **Embeddings are not recomputed.** Export carries the stored vectors verbatim, so
  a load does not re-run the embedding pipeline. If you changed the embedding model,
  re-embed after loading.
- **External Blob URIs remain references during rebuild.** Export preserves the
  URI and the documented rebuild uses `--mode overwrite`, so verify that the new
  fleet can still read the referenced object before cutover. Later keyed
  `append`/`merge` writes copy external payloads instead, as described below.
- **Server deployments**: take the graph out of the serving set, rebuild it offline
  with the CLI, then point the cluster at the rebuilt graph (`cluster apply`).

## Migrating from internal schema v8 to v9

Internal schema v9 activates RFC-026's private common-B2 storage contract:
stream-config v3, lifecycle state v2, a manifest-selected
`_stream_tokens.lance` authority, trusted per-row attribution, and
recovery-v12's atomic base-plus-token publication. These are physical
correctness foundations; v9 does not by itself expose a public streaming API.

A v8 graph must use the standard rebuild recipe above. Quiesce every v8 writer,
export with the latest 0.12.x binary, initialize a **different** root with the
0.13.x binary, load the export, and verify the v9 stamp plus row/vector/blob
fidelity before fleet cutover. Keep the v8 root unchanged through the rollback
window. A v9 binary refuses v8, and a v8 binary refuses v9.

V9's physical attribution field is named `__omnigraph_stream_v1$`. The trailing
`$` is deliberately outside the `.pg` property-name grammar, so it cannot
collide with a user property. In particular, v8 legitimately allowed a user
property named `__omnigraph_stream_v1` (without `$`); export/init/load preserves
that property and its values unchanged. Do not rename or delete it as protocol
metadata. Conversely, v9 export omits only the exact physical `$`-suffixed
field and never transfers token authority into the rebuilt logical snapshot.

## Migrating to v0.8.0

v0.8.0 is the first release with a storage-format change since v0.4.0. Any graph
created by an earlier release must be rebuilt with the recipe above. Beyond the
rebuild, v0.8.0 changes two things to plan for: the on-disk layout, and
write-time validation strictness.

### What changed on disk (internal schema v4)

- **Graph commit lineage now lives in the `__manifest` table.** Commits, parents,
  merge parents, per-branch heads, and the authoring actor are stored as
  `graph_commit` / `graph_head` rows, written in the **same atomic commit** as the
  table-version rows of a graph publish. Previously a crash in a narrow window
  could leave a published version with no matching history entry; that window no
  longer exists.
- **Two internal datasets are retired.** `_graph_commits.lance` and
  `_graph_commit_actors.lance` are no longer created, read, or written — a graph
  created by v0.8.0 has neither. If backup scripts, disk-usage tooling, or
  monitoring reference those paths inside a graph directory, update them.
- **The version gate is enforced in both directions, including read-only opens.**
  A v0.8.0 binary refuses a pre-v0.8.0 graph with the rebuild message above; a
  pre-v0.8.0 binary refuses a v0.8.0 graph with an
  `upgrade omnigraph before opening this graph` error. There is no mixed-version
  window: upgrade every binary that touches a graph together, then rebuild.

If you have tooling that inspects `__manifest` directly, note that it now holds
three kinds of rows (table versions, commits, branch heads) rather than one —
filter by row kind instead of assuming every row is a table version.

### Stricter validation — pre-flight your pipelines

Independently of the storage change, v0.8.0 unifies constraint validation across
all three write surfaces (load, mutation, branch merge). Every change is stricter;
none relaxes an existing check. A pipeline that unknowingly relied on one of these
gaps will now fail loudly at write time:

- **Enum constraints are enforced on branch merge** (previously only on load and
  mutation).
- **Cross-version uniqueness**: inserting a `@unique` value that collides with a
  different, already-committed row is rejected on load and mutation (previously
  only merges caught it). Re-upserting the *same* row — same key — is still an
  update, not a violation.
- **Duplicate keys within one input batch are rejected**: the same `@key` value
  twice in one load file is an error. The same id across *separate* batches or
  statements still coalesces (last write wins).
- **Typed node IDs are canonicalized during rebuild**: a keyed node is stored
  under the canonical ID derived from its complete `@key` tuple. Legacy scalar
  spellings that denote the same typed value are accepted, and edge endpoints
  are rewritten by their declared node endpoint type. An ambiguous old-ID
  mapping is refused rather than guessed.
- **Overwrite loads validate the new image per table**: an edges-only overwrite
  resolves referential integrity against the retained node tables, and orphan
  edges are rejected.

Pre-flight recipe: before upgrading a production writer, run your ingest with a
v0.8.0 binary against a **branch** of a rebuilt copy, using the **same `--mode`
your pipeline uses in production** (`--mode` is always required; `overwrite` is
the mode whose validation changed most):

```bash
omnigraph load --data batch.jsonl --mode merge \
  --branch preflight --from main s3://bucket/graph-v2.omni
```

Rows violating the stricter checks fail the load with a typed error naming the
constraint; fix the data (or the constraint) and re-run. Nothing is partially
applied — a failed load publishes no commit.

### Verifying versions

The two CLI checks are listed in
[How you know you need this](#how-you-know-you-need-this) (`omnigraph version`,
`omnigraph snapshot`). New in v0.8.0, the server's `GET /healthz` response also
reports `internal_schema_version`.

## Migrating to internal schema v5

Internal schema v5 activates RFC-028 stable schema identity. Accepted type and
property IDs are allocated inside one graph identity domain and survive
supported renames; dropping and later recreating a declaration starts a new
logical lifetime. The `__manifest` journal keys table registrations, versions,
and tombstones by stable table ID plus incarnation, and initial table paths are
derived from that pair rather than a mutable type name.

This is why a v4 graph must be rebuilt instead of opened in place: v4 has only
name-derived SchemaIR IDs and name-keyed manifest history, so there is no safe,
unambiguous identity to backfill after renames or drop/recreate events. Export
with the latest v0.8.x binary, initialize a different root with the v5 binary,
load the export, verify it, and then cut clients over. The new root deliberately
mints a new identity domain; identity continuity across export/import is not
claimed.

Tooling that reads `__manifest` directly must treat `stable_table_id` and
`table_incarnation_id` as the table coordinate. `table_key` remains the current
human-readable alias and can change during a type rename. A pure rename keeps
the same physical dataset path and Lance version.

## Migrating to internal schema v6

Internal schema v6 activates RFC-023 key-conflict fencing while preserving the
stable identity model introduced by v5. Every newly created node and edge
dataset declares exactly its non-null physical `id` field as Lance's
unenforced primary key. Production graph inserts and upserts then use an
exact-`id` filter-bearing transaction route; a bare Lance Append is not used for
keyed graph rows.

The user-visible load modes are now deliberately distinct:

- `--mode append` means **strict insert**. If an `id` already exists, the load
  returns `KeyConflict` and does not update that row. An effect-free concurrent
  same-key winner has the same result.
- `--mode merge` means **upsert**. Existing IDs are updated; new IDs are
  inserted. If a retryable conflict occurs before any effect, the engine
  discards the complete attempt, rereads current authority, reruns validation,
  and stages a fresh attempt.
- `--mode overwrite` replaces the target image as before; the replacement
  dataset still carries the exact-`id` PK metadata.

For Blob values supplied as external URIs, `append` and `merge` copy referenced
payload bytes after enforcing a 32 MiB aggregate pre-read ceiling. `overwrite`
retains Lance's external-reference behavior. This is intentional: Lance's
merge-insert builder has no `WriteParams` hook, while Overwrite does.

This format cannot be obtained by adding metadata to a live v5 root. Lance's
filtered/unfiltered conflict behavior is directional, so every table image and
every writer must cross the boundary together. Quiesce writers, export with the
latest 0.9.x binary, initialize a **different** root with the 0.10.x binary,
load the export, verify the v6 stamp and data, then cut the whole fleet over.
The 0.10.x binary refuses the v5 source root, and the 0.9.x binary refuses the
new v6 root.

The v6 load checks the export for duplicate logical IDs before any table effect.
Older bare-Append workloads could contain a committed collision; do not resolve
one by silently choosing a winner during upgrade. A duplicate-bearing load
fails atomically and leaves the separately initialized v6 target as a valid
empty graph; do not serve that target, discard it, repair the source/export,
and restart the rebuild into another clean root. Keep the source root intact
until the rebuilt graph has passed row/vector/blob checks.

### Repairing duplicate IDs found during the v6 rebuild

There is no `repair duplicates` command, and the importer never chooses a
winner from export order. Repair the **exported snapshot**, not Lance files or
the old graph root. Run this procedure independently for every branch snapshot
you intend to preserve.

A duplicate is scoped to one logical table:

- node key: `["node", <node type>, <data.id>]`;
- edge key: `["edge", <edge type>, <data.id>]`.

The same ID in two different node types, two different edge types, or a node
and an edge is not a collision. Do not deduplicate IDs globally.

#### 1. Quiesce and preserve the evidence

Stop every writer and drain or stop the servers that expose the graph before
the final export. Do not remove a cluster resource if that would trigger graph
deletion. Keep the old fleet quiesced through cutover so the repaired snapshot
cannot miss a later write. Export with the old-format binary and make the raw
artifacts read-only:

```bash
set -euo pipefail

OLD_ROOT=s3://bucket/graph.omni
BRANCH=main

old-omnigraph schema show "$OLD_ROOT" > schema.pg
old-omnigraph export --branch "$BRANCH" "$OLD_ROOT" > graph.raw.jsonl
shasum -a 256 schema.pg graph.raw.jsonl > rebuild-inputs.sha256
chmod a-w schema.pg graph.raw.jsonl rebuild-inputs.sha256
```

Use `sha256sum` instead of `shasum -a 256` on systems that provide only the
former. Preserve the source root, raw export, checksum, old/new binary versions,
and branch name until the rollback window has closed. Do not run `repair`,
`cleanup`, or a hand-written Lance mutation against the old root.

#### 2. Detect every duplicate table key

The following scan requires `jq` and an external `sort`. It streams the export;
only `sort` needs spill space. The encoded keys keep arbitrary UTF-8 IDs and
control characters safe as one line:

```bash
set -euo pipefail

jq -rc '
  def logical_key:
    if ((.type? | type) == "string" and (.data.id? | type) == "string") then
      ["node", .type, .data.id]
    elif ((.edge? | type) == "string" and (.data.id? | type) == "string") then
      ["edge", .edge, .data.id]
    else
      error("invalid export row: expected a node or edge with string data.id")
    end;
  logical_key | @base64
' graph.raw.jsonl |
  LC_ALL=C sort |
  uniq -d > duplicate-keys.b64
```

If the pipeline exits nonzero, discard its partial output and stop; malformed
JSON or a missing/string-invalid `data.id` is not a clean duplicate scan.
An empty `duplicate-keys.b64` means this snapshot has no duplicate table keys.
If it is non-empty, generate a reviewable report containing every physical row
for every affected key:

```bash
set -euo pipefail

jq --rawfile duplicates duplicate-keys.b64 -c '
  def logical_key:
    if ((.type? | type) == "string" and (.data.id? | type) == "string") then
      ["node", .type, .data.id]
    elif ((.edge? | type) == "string" and (.data.id? | type) == "string") then
      ["edge", .edge, .data.id]
    else
      error("invalid export row")
    end;
  ($duplicates | split("\n") | map(select(length > 0)) |
    reduce .[] as $key ({}; .[$key] = true)) as $duplicate_keys
  | (logical_key | @base64) as $encoded_key
  | select($duplicate_keys[$encoded_key] == true)
  | {key: logical_key, row: .}
' graph.raw.jsonl |
  LC_ALL=C sort > duplicate-report.jsonl
```

Do not stop after fixing the first ID named by the importer. This scan is the
complete inventory for the exported snapshot.

#### 3. Record an application-aware decision for each key

Create `graph.repaired.jsonl` from the immutable raw export with a reviewed,
deterministic transformation. Keep a decision ledger beside it containing the
key, evidence, chosen action, resulting IDs, every edge rewrite, and reviewer.
For each group, choose one of these domain decisions:

- **One real entity or relationship:** consolidate the rows into exactly one
  canonical row. Resolve every differing property explicitly. Byte-identical
  rows still require a recorded “keep one” decision.
- **Multiple real entities:** keep one ID and mint stable new application IDs
  for the others. Before remapping a node, inspect `schema.pg` for that node
  type. If it declares `@key`, choose replacement values for the complete key
  tuple and write every component to its property. The safest repaired row
  omits `data.id` and lets the loader derive the canonical physical ID; any
  incident edge must then use that derived ID. If the repair tooling writes
  `data.id` explicitly, use the exact encoding documented under
  [schema table layout](../schema/index.md#table-layout): the canonical scalar
  string for one key component, or the JSON-array string for a composite key.
  If the node has no declared `@key`, update only `data.id`. Then
  update every incident edge row's `from` or `to` whose edge schema points to
  that node type. A matching text ID in another node type must not be rewritten
  automatically.
- **Erroneous row:** remove it, and remove or redirect incident edges only when
  the application contract says they belonged to that erroneous entity.
- **Duplicate edge ID:** consolidate the relationship or mint new edge IDs.
  Edge endpoint IDs change only if the corresponding node decision requires it.

Never use “first row”, “last row”, lexical order, `sort -u`, or an old commit's
apparent arrival order as evidence. The export is a snapshot, not an authority
for which racing write was intended. Prefer the upstream system of record,
domain timestamps with documented semantics, or human review. If the graph is
derived from another system of record, make the repair there and generate the
candidate export from that corrected source instead of mutating the old graph.

Rerun the exact detector from step 2 against `graph.repaired.jsonl`, writing to
`repaired-duplicate-keys.b64` so the original evidence is not overwritten; the
new file must be empty. Checksum the repaired export and decision ledger. If a
repaired node ID leaves an edge endpoint ambiguous, stop—the v6 schema/RI check
must not be used to guess the intended endpoint.

#### 4. Validate in a disposable v6 root

Use a new URI that has never been initialized, is outside any managed cluster
graph prefix, and will never be served:

```bash
set -euo pipefail

TICKET=change-1234
VALIDATION_ROOT="s3://bucket/graph-v6-validation-${TICKET}.omni"

omnigraph init --schema schema.pg "$VALIDATION_ROOT"
omnigraph --yes load --mode overwrite --data graph.repaired.jsonl "$VALIDATION_ROOT"
omnigraph export "$VALIDATION_ROOT" > graph.canonical.jsonl
```

`--yes` is a global CLI flag. It is required for a non-interactive overwrite
of a non-local root, as in this S3 example. At an interactive terminal, omit
it and answer the overwrite prompt with `y` or `yes`; a local root needs
neither. `init` does not use `--yes`: the target must be genuinely new. Never
substitute `init --force`, because reusing a root would invalidate this clean-
rebuild procedure.

If `init` or `load` fails, preserve the error, discard that target with your
storage tooling, repair the export, and retry at another clean URI. Do not try
to heal the initialized empty target with `append` or `merge`.

Compare the repaired input with the v6 re-export semantically. This helper
preserves arbitrary-size JSON integers, canonicalizes object-key order, and
lets external `sort` compare the complete row multisets:

```bash
set -euo pipefail

canonicalize_jsonl() {
  python3 -c '
import json, sys
for line in sys.stdin:
    if line.strip():
        print(json.dumps(json.loads(line), sort_keys=True,
                         separators=(",", ":"), ensure_ascii=False))
' < "$1" | LC_ALL=C sort
}

canonicalize_jsonl graph.repaired.jsonl > repaired.semantic.jsonl
canonicalize_jsonl graph.canonical.jsonl > canonical.semantic.jsonl
cmp repaired.semantic.jsonl canonical.semantic.jsonl
```

Also rerun the duplicate detector against `graph.canonical.jsonl` into a new
`canonical-duplicate-keys.b64`, execute the application's integrity queries,
compare per-table row counts, check vectors, and verify blob content. A matching
external Blob URI proves only that the reference round-tripped; read or checksum
the referenced object from the new fleet's credentials before cutover.

#### 5. Build the production candidate and cut over

Treat `graph.canonical.jsonl`—the successful v6 re-export—as the audited input
to one final clean root for a standalone graph:

```bash
set -euo pipefail

TICKET=change-1234
NEW_ROOT="s3://bucket/graph-v6-${TICKET}.omni"

canonicalize_jsonl() {
  python3 -c '
import json, sys
for line in sys.stdin:
    if line.strip():
        print(json.dumps(json.loads(line), sort_keys=True,
                         separators=(",", ":"), ensure_ascii=False))
' < "$1" | LC_ALL=C sort
}

omnigraph init --schema schema.pg "$NEW_ROOT"
omnigraph --yes load --mode overwrite --data graph.canonical.jsonl "$NEW_ROOT"
omnigraph export "$NEW_ROOT" > graph.final.jsonl
canonicalize_jsonl graph.canonical.jsonl > canonical.production.semantic.jsonl
canonicalize_jsonl graph.final.jsonl > final.production.semantic.jsonl
cmp canonical.production.semantic.jsonl final.production.semantic.jsonl
```

The last comparison is semantic and does not assume that scan/export row order
is a storage contract. Cut the whole fleet or cluster configuration over only
after it and the application checks pass. Never serve the failed empty target
or the disposable validation root. Keep the old root and all repair evidence
through the rollback window; if any check fails, leave production on the old
root and start again at a new target URI.

#### 6. Rebuild and cut over a cluster as one unit

A cluster-managed upgrade is not step 5 repeated as independent pointer swaps.
The cluster storage root contains one ledger and catalog for the complete
declared graph set. Build a parallel cluster for **all** graphs, validate it as
one fleet, and switch the whole fleet together.

Start from a fresh version-control checkout of the authored cluster config.
Do not clone generated `__cluster/` state or a local `graphs/` directory. Edit
`cluster.yaml` so `storage:` names an unused parallel cluster prefix, prove that
prefix is empty with the object-store tooling, and retain the old config and
root unchanged. Then prepare a reviewed plan:

```bash
set -euo pipefail

NEW_CONFIG=/absolute/path/to/company-brain-v6
NEW_CLUSTER_ROOT=s3://bucket/clusters/company-brain-v6-change-1234

test -d "$NEW_CONFIG"
test ! -e "$NEW_CONFIG/__cluster"
test ! -e "$NEW_CONFIG/graphs"

# Set `storage:` to exactly $NEW_CLUSTER_ROOT before saving.
"${EDITOR:-vi}" "$NEW_CONFIG/cluster.yaml"
omnigraph cluster validate --config "$NEW_CONFIG"
omnigraph cluster import --config "$NEW_CONFIG"
omnigraph cluster plan --config "$NEW_CONFIG" --json > parallel-cluster-plan.json
```

Review `parallel-cluster-plan.json`. Confirm that it describes the fresh
parallel deployment, includes every graph declared in `cluster.yaml`, and has
no unexpected delete or approval gate. Only then apply it; `cluster apply`
creates every derived graph root and publishes the parallel ledger and catalog:

```bash
set -euo pipefail

NEW_CONFIG=/absolute/path/to/company-brain-v6
omnigraph cluster apply --config "$NEW_CONFIG"
```

Complete steps 1–4 for every declared graph and create a reviewed
`rebuilt-graphs.tsv` with exactly one tab-separated row per `graphs:` key:
`<graph-id><TAB><canonical-export-path>`. Check set equality between its graph
IDs and the config; missing, extra, or repeated IDs are a stop condition. Load
and semantically re-export every row into the roots created by `cluster apply`:

```bash
set -euo pipefail

NEW_CLUSTER_ROOT=s3://bucket/clusters/company-brain-v6-change-1234
REPORT_DIR=parallel-cluster-validation
test -s rebuilt-graphs.tsv
test ! -e "$REPORT_DIR"
mkdir "$REPORT_DIR"

awk -F '\t' 'NF != 2 || $1 == "" || $2 == "" { exit 1 }' rebuilt-graphs.tsv
cut -f1 rebuilt-graphs.tsv | LC_ALL=C sort | uniq -d > \
  "$REPORT_DIR/duplicate-graph-ids.txt"
test ! -s "$REPORT_DIR/duplicate-graph-ids.txt"

canonicalize_jsonl() {
  python3 -c '
import json, sys
for line in sys.stdin:
    if line.strip():
        print(json.dumps(json.loads(line), sort_keys=True,
                         separators=(",", ":"), ensure_ascii=False))
' < "$1" | LC_ALL=C sort
}

graph_id=
canonical_export=
while IFS=$'\t' read -r graph_id canonical_export ||
    test -n "${graph_id}${canonical_export}"; do
  test -n "$graph_id"
  test -n "$canonical_export"
  test -f "$canonical_export"

  graph_root="${NEW_CLUSTER_ROOT%/}/graphs/${graph_id}.omni"
  final_export="$REPORT_DIR/${graph_id}.final.jsonl"
  omnigraph --yes load --mode overwrite --data "$canonical_export" "$graph_root"
  omnigraph export "$graph_root" > "$final_export"
  canonicalize_jsonl "$canonical_export" > \
    "$REPORT_DIR/${graph_id}.input.semantic.jsonl"
  canonicalize_jsonl "$final_export" > \
    "$REPORT_DIR/${graph_id}.final.semantic.jsonl"
  cmp "$REPORT_DIR/${graph_id}.input.semantic.jsonl" \
    "$REPORT_DIR/${graph_id}.final.semantic.jsonl"
done < rebuilt-graphs.tsv
```

For every graph, also rerun the duplicate detector, compare per-table counts,
read vectors and blobs, and run its application integrity queries. One graph
passing does not qualify the cluster. After every graph and catalog resource
passes, refresh the ledger from live observations and capture the final state
and plan:

```bash
set -euo pipefail

NEW_CONFIG=/absolute/path/to/company-brain-v6
omnigraph cluster refresh --config "$NEW_CONFIG"
omnigraph cluster status --config "$NEW_CONFIG" --json > parallel-cluster-status.json
omnigraph cluster plan --config "$NEW_CONFIG" --json > parallel-cluster-final-plan.json
```

Require a healthy status and a final plan with no changes. For cutover, stop
every old-cluster replica, change every replica's boot source to the same new
config checkout or `NEW_CLUSTER_ROOT`, and then restart the whole fleet. Verify
graph enumeration and health before reopening writes. Never swap one graph
pointer, mix replicas on old and new cluster roots, or delete the old root. A
rollback likewise stops the whole new fleet and switches every replica back;
retain both roots and all repair evidence through the rollback window.

## Migrating to internal schema v7

Internal schema v7 preserves v5 stable identity and v6 exact-`id` key fencing,
then activates RFC-026 Phase A's stream-format foundation: identity-keyed
lifecycle authority, a recoverable empty main-only/unsharded Lance MemWAL
enrollment, process-local writer exclusion, and strict partial-format refusal.
It does **not** expose streaming ingestion. There is no `@stream`, production
enrollment command or API, WAL row acknowledgement, fold, drain/resume, or
fresh-read surface in this format slice.

Move a v6 graph to v7 with the ordinary recipe at the top of this page: export
with the latest 0.10.x binary, initialize a **different** root with the 0.11.x
binary, load the export, verify the v7 stamp and logical data, and cut the whole
fleet over together. The 0.11.x binary refuses the v6 source root, and the
0.10.x binary refuses the new v7 root. Genuine cross-version tests pin both
directions.

V6 has no acknowledged MemWAL rows, so there is no stream backlog to drain
before this particular rebuild. Export transfers only manifest-visible logical
rows; it does not copy MemWAL indexes, shard manifests, lifecycle rows, recovery
sidecars, or epochs. The new v7 root therefore starts with no physical stream
enrollment. Keep the old root intact until row, vector, blob, policy, and
application-integrity verification passes and the fleet cutover is complete.

## Migrating to internal schema v8

Internal schema v8 activates RFC-026 Phase B1's private data-bearing MemWAL
core: exact persisted stream-config v2, one hard-bounded generation, durable
watcher acknowledgement, conservative restart/replay, explicit seal/drain,
and schema-v11 `StreamFold` recovery through the unified write path. It still
does **not** expose public stream schema, SDK, HTTP, CLI, or operator controls;
those remain Phase B2 gates.

Move a v7 graph to v8 with the ordinary recipe at the top of this page: export
with the latest 0.11.x binary, initialize a different root with the 0.12.x
binary, load the export, and verify the v8 stamp and logical data. V7's
config-v1 enrollment is never reinterpreted as data-bearing config-v2 state.
The rebuild copies manifest-visible rows only, not MemWAL indexes, shard state,
recovery intents, or epochs, so the new root starts unenrolled.
