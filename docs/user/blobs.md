# Blobs

Use `Blob` properties for bytes that should not be projected through `.gq`.
Blob values can contain graph-managed bytes, an external URI reference, or null.
A valid empty managed Blob is distinct from null.

```pg
node Document {
  slug: String @key
  content: Blob?
}
```

## Writing Blob values

Load and mutation input use one String representation:

- `base64:<payload>` supplies managed bytes owned by the graph;
- any other String requests an external URI reference;
- `null` stores a null value when the property is nullable.

```jsonl
{"type":"Document","data":{"slug":"manual","content":"base64:SGVsbG8="}}
```

New external references are denied by default. A cluster-served graph must list
allowed URI bases in its graph configuration. Direct `--store` CLI access has no
external-source allowlist, so it accepts managed `base64:` input but rejects new
external references. Credentials must not appear in stored URIs. An allowed
base must lie outside the cluster's storage root and every graph root; see
[External Blob references](clusters/config.md#external-blob-references).

Write mode determines ownership:

- `load --mode overwrite` preserves an allowed external URI as an external
  reference;
- incremental inserts, upserts, updates, append/merge loads, and branch merges
  that write entities copy allowed source bytes into graph-managed storage;
- an existing external reference remains readable even when new external
  ingress is disabled, and exportable when it names its whole object (see
  below).

An `update` never reads the old value of a Blob it assigns. It carries every
other Blob cell of a matched row: it reads the cell and rewrites it as managed
bytes. Carrying a stored external reference therefore needs the graph's
external Blob policy to admit the reference's source. Otherwise the update
fails with a 400 that names the type, id, and property; assign that property in
the same update, to a new value or to null, to replace or clear the reference
without reading it.

Only a graph written outside OmniGraph can hold a stored reference to a byte
range of an object. Export writes an external reference as a bare URI, which
reloads as the whole object, so export refuses a ranged reference instead of
widening it. Change-feed images, the change-feed baseline and entity reads by
id describe it exactly, as `{"uri": …, "offset": …, "length": …}` with a
positive `length`, without reading the object. The feed passes the commit that
holds it like any other, and a baseline taken while the row exists succeeds;
that baseline does not reload with `load`. A stored descriptor with an offset
but no length is refused as a Blob integrity error.

OmniGraph never deletes the object named by an external reference.

## Query behavior

Blob properties are not ordinary `.gq` read values. They cannot be projected,
filtered, ordered, or aggregated. Write them through load or mutation
assignment, then read an individual Blob value through the dedicated CLI or
HTTP surface.

There are no `blob put` or `blob clear` commands. Use the normal graph write
path so Blob changes remain part of an atomic graph commit.

## CLI reads

The selector is `ENTITY TYPE ID PROPERTY`, where `ENTITY` is `node` or `edge`.
Blob commands address the graph with `--store`, `--server`, or a matching
profile; they do not take a positional graph URI.

```bash
# Stream managed bytes to a file.
omnigraph blob get node Document manual content \
  --out manual.bin --store graph.omni

# Inspect the value without reading payload bytes.
omnigraph blob stat node Document manual content \
  --json --store graph.omni
```

Reads default to `main`. `--branch <name>` and `--snapshot <commit-id>` are
mutually exclusive.

`blob get` supports `--offset`, `--length`, and `--out`. With no range flags it
streams the complete value. `--offset N` reads from `N` to the end;
`--length M` reads the first `M` bytes; together they read `N..N+M`. A length of
zero is rejected. An end beyond the value is clamped to EOF, while a start at or
beyond the end of a non-empty value is unsatisfiable.

If transfer fails after output begins, stdout or `--out` may contain the prefix
already written. For atomic file replacement, write to a temporary path, check
the exit status, then rename it.

`blob stat --json` reports the selector, value kind, and exact resolved snapshot.
A managed value also reports `size` and `etag`; a whole-object external value
reports its stored `uri`. Fields that do not apply are omitted rather than set
to null.

The CLI never follows an external URI. `blob get` refuses a whole-object
reference and points to `blob stat`, which returns it without opening the
target. A persisted ranged external descriptor is rejected by both commands;
OmniGraph never widens it to the complete target object.

## HTTP reads

Servers expose the same logical selector with GET and HEAD:

```http
GET /graphs/knowledge/blob?entity=node&type=Document&id=manual&property=content&branch=main
Omnigraph-Http-Api: 0.12
```

Both methods follow the [HTTP contract](operations/server.md#http-contract),
including checking the response header before consuming bytes.
Use `snapshot=<commit-id>` instead of `branch` for an immutable historical read.

For managed values:

- GET returns bytes; HEAD returns the same metadata without a body;
- a single standard `Range` request returns `206`, or `416` when unsatisfiable;
- `ETag`, `If-Match`, and `If-None-Match` support conditional delivery;
- the response identifies the exact resolved graph snapshot.

Treat ETags as opaque validators for the selected graph representation, not as
content hashes. An unrelated change to the same entity type can produce a new
ETag even when this cell's bytes are unchanged.

For a whole-object external value, GET and HEAD return `302` with the stored URI
in `Location`. The server does not fetch, sign, authorize, or proxy that object
and does not claim its size or ETag. A persisted ranged external descriptor
fails loudly instead of redirecting to a wider value.

## Limits

Blob limits bound the memory one operation needs. An operation over a limit
fails before it changes the graph. Over HTTP it returns `413` with a
`resource_limit` detail naming the `resource`, its `limit` and the `actual`
value observed; the CLI reports the same three fields. Split the work into
smaller operations and retry.

| Limit | Applies to | Reported resource |
|---|---|---|
| 32 MiB of decoded `base64:` bytes | Each node or edge type in one load, in every mode, including `overwrite` | `decoded blob input bytes for <table>` |
| 32 MiB per touched type, and 32 MiB across all touched types, Blob bytes included | Incremental writes: `append` and `merge` loads, inserts and updates. External bytes copied in and Blob values carried unchanged by an update count | `keyed write bytes for <table>`, `keyed entity bytes for <table>`, `retained keyed batch bytes per operation` |
| 32 MiB of external payload copied into managed storage | One incremental write operation across all its types, and each type within it: two types copying 20 MiB each exceed it although each fits its per-type limit | `materialized external blob payload bytes` |
| 32 MiB of Blob payload | One branch merge that writes rows, across all types, managed and external bytes together | `materialized blob payload bytes` |
| 8,192 external references | One write operation or merge | `external Blob reference cells` |
| 32 MiB of retained URI metadata | One write operation or merge. Every copy of a URI the operation keeps counts, plus 24 bytes per copy: admission keeps each reference's text twice and each distinct object's normalized URI twice, so distinct URIs reach the limit at about 8 MiB of text | `external Blob URI metadata bytes` |
| 64 KiB | One external URI | `external Blob URI bytes` |
| 4 MiB | One embedded managed range read | `Blob read range bytes` |

`<table>` names the type as `node:<Type>` or `edge:<Type>`, for example
`keyed entity bytes for node:Document`.

The HTTP load request body is also capped at 32 MiB. That cap counts the
encoded request, so one request carries about 24 MiB of decoded `base64:`
data.

Values larger than these limits stay readable. The CLI and the HTTP server
read managed values in 4 MiB ranges, so a large value streams without a
whole-value buffer, and one HTTP response holds at most two ranges at a time.
`omnigraph optimize` bounds the Blob payload of each compaction batch
separately; see [Optimize](operations/maintenance.md#optimize).

## Lifecycle

Blob readers stay pinned to the snapshot selected when they were opened. They
never switch to newer bytes when a branch advances. Branch deletion and
destructive cleanup can remove storage needed by a long-running reader; quiesce
those readers first when they must finish reliably.

Historical reads are fail-closed. Type renames remain addressable through the
current type name, but a historical property rename, drop/re-add, or branch
incarnation may be refused when the selected snapshot does not carry enough
identity information to prove it is the same logical Blob property. OmniGraph
returns an error rather than guessing from a reused name or physical field
position.
