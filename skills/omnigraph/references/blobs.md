# Blob Values

Read this when a schema or workflow uses `Blob`. Blob values are intentionally
outside ordinary `.gq` projection: write them through normal mutations or
loads, then read one cell through the dedicated CLI or HTTP surface.

## Schema and values

```pg
node Document {
    slug: String @key
    content: Blob?
}
```

`Blob` cannot be a key, unique member, index, embed source, or list member.
Schema parsing rejects those combinations. Blob properties also cannot be
projected, filtered, ordered, or aggregated in `.gq`.

Write input uses one String representation:

- `base64:<payload>` stores graph-managed bytes;
- another String requests an external object URI;
- `null` stores null when the property is optional.

There is no `blob put` or `blob clear`; use the normal atomic graph write path.
Blob payloads count toward write limits.

## External-reference policy and ownership

New external references are denied by default. A cluster graph may declare
normalized allowed bases under `external_blobs`; direct CLI access remains
deny-only. Never put credentials in a stored URI.

Ownership depends on the write:

- `load --mode overwrite` preserves an allowed external reference;
- insert, update, append/merge load, and branch merge copy allowed source bytes
  into graph-managed storage;
- a previously stored external reference remains readable/exportable if new
  external ingress is later disabled.

OmniGraph never deletes the external source object.

## CLI reads

Select one cell as `<node|edge> <TYPE> <ID> <PROPERTY>` and address the graph
with `--store`, `--server`, or a profile:

```bash
omnigraph blob stat node Document manual content --store graph.omni --json
omnigraph blob get node Document manual content \
  --store graph.omni --out manual.bin
```

Use `--branch` or the mutually exclusive `--snapshot`. `get` supports
`--offset` and `--length`; one embedded managed range is limited to 4 MiB, so
read larger values in consecutive ranges. The CLI and HTTP server stream bytes.

A failed transfer can leave a prefix in stdout or `--out`. For atomic file
installation, write a temporary file, check the exit status, then rename it.

`stat` reports the exact resolved snapshot and value kind. Managed values also
have size and ETag; an external whole-object value reports its stored URI. The
CLI never follows an external URI: `get` refuses it and directs the caller to
`stat`.

## HTTP reads

`GET` and `HEAD /graphs/{id}/blob` take `entity`, `type`, `id`, `property`, and
either `branch` or `snapshot`. Managed values support one standard `Range`,
`ETag`, `If-Match`, and `If-None-Match`. Treat an ETag as an opaque validator of
that graph representation, not a content hash.

For a whole-object external value, the server returns `302` with the stored URI
in `Location`; it does not fetch, sign, authorize, or proxy the object.

## Lifecycle

A reader stays pinned to the snapshot selected when it opens. Branch deletion
or destructive cleanup can reclaim bytes needed by a long read, so quiesce such
readers first. Blob-aware compaction is supported.

Historical identity fails closed: if a rename, drop/re-add, or branch lifetime
does not prove that a historical property is the same logical Blob property,
OmniGraph returns an error rather than guessing.

Canonical user contract: [Blob values](../../../docs/user/blobs.md).
