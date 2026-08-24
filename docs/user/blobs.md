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
external references. Credentials must not appear in stored URIs.

Write mode determines ownership:

- `load --mode overwrite` preserves an allowed external URI as an external
  reference;
- incremental inserts, upserts, updates, append/merge loads, and branch merges
  that write entities copy allowed source bytes into graph-managed storage;
- an existing external reference remains readable and exportable even when new
  external ingress is disabled.

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
```

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

## Limits and lifecycle

One embedded managed range read returns at most 4 MiB. Read larger values in
consecutive ranges; the CLI and HTTP server stream them without requiring one
whole-value buffer.

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
