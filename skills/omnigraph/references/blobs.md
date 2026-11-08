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
projected, filtered, ordered, or aggregated in `.gq` (`T24: Blob property … is
not available as a .gq read value`), and a bare node projection
(`return { $p }`) silently omits them.

Write input uses one String representation:

- `base64:<payload>` stores graph-managed bytes;
- another String requests an external object URI;
- `null` stores null when the property is optional.

To replace or clear one cell of an existing node or edge without a load or
mutation, use `blob put` (raw bytes from `--file` or stdin) or `blob clear`:

```bash
omnigraph blob put node Document manual content --file manual.pdf \
  --if-match '"<current ETag>"' --target ./graph.omni --json
omnigraph blob clear node Document manual content --target prod --graph knowledge
```

Each write is one graph commit and needs `change` on its `--branch` (default
`main`). Neither inserts a row (a missing entity is `404`), and clearing a cell
that is already null publishes nothing (`commit: null`). Put input is at most
32 MiB and must fit `write_max_bytes`. `--if-match` takes entity tags or `*`
(any non-null value); a failed precondition exits 4 (HTTP `412
precondition_failed` with the current ETag). Each write is sent once: after an
unknown outcome, `blob stat` the cell before writing again.

Blob payloads count toward write limits. `export` writes Blob cells in the same
`base64:`/URI spelling, so exported lines reload as-is. The exception is a
stored reference to a byte range of an object, which only a writer outside
Omnigraph creates: a bare URI would reload as the whole object, so export
refuses it. Change-feed images, the change-feed baseline and entity reads by id
describe it as `{"uri", "offset", "length"}` instead; such a baseline does not
reload with `load`.

An `update` never reads the old value of a Blob it assigns, and a null assigned
to a nullable Blob clears it. Every other Blob of a matched row is carried
(read and rewritten as managed bytes), so carrying a stored external reference
needs the graph's external Blob policy to admit its source; otherwise the
update fails with a 400 naming the type, id, and property. Assign that property
in the same update to replace or clear the reference without reading it.

## External-reference policy and ownership

New external references are denied by default. A cluster graph admits them
under declared bases:

```yaml
graphs:
  knowledge:
    external_blobs:
      allow:
        - base: s3://company-assets/knowledge/
          scope: server_safe
```

`scope` is required (`server_safe` for a served graph; `embedded_only` only for
an embedded host). A base inside or above the cluster storage root is refused
with `external_blob_base_overlaps_storage_root`. The same bases admit
`load csv` sources. Direct CLI access remains deny-only. Never put credentials
in a stored URI.

Ownership depends on the write:

- `load --mode overwrite` preserves an allowed external reference;
- insert, update, append/merge load, and branch merge copy allowed source bytes
  into graph-managed storage;
- a previously stored external reference remains readable if new external
  ingress is later disabled, and exportable when it names its whole object.

Omnigraph never deletes the external source object.

## CLI reads

Select one cell as `<node|edge> <TYPE> <ID> <PROPERTY>` and address the graph
with `--target NAME|ADDRESS`. A keyed edge's `<ID>` is its derived
JSON-array id (for example `'["alice","bob"]'`):

```bash
omnigraph blob stat node Document manual content --target ./graph.omni --json
omnigraph blob get node Document manual content \
  --target ./graph.omni --out manual.bin
```

Use `--branch` or the mutually exclusive `--snapshot`. With no range flags
`get` streams the complete value; `--offset N` reads to the end, `--length M`
the first `M` bytes, together `N..N+M`. The CLI and HTTP server read managed
values in 4 MiB pieces themselves, so no manual chunking is needed; the 4 MiB
limit binds one range read of an embedded `Session`.

A failed transfer can leave a prefix in stdout or `--out`. For atomic file
installation, write a temporary file, check the exit status, then rename it.

`stat` reports the exact resolved snapshot and value kind. Managed values also
have size and ETag; an external whole-object value reports its stored URI. The
CLI never follows an external URI: `get` refuses it and directs the caller to
`stat`.

## HTTP reads

`GET` and `HEAD /graphs/{graph_id}/blob` take `entity`, `type`, `id`, `property`, and
either `branch` or `snapshot`, plus the `Omnigraph-Http-Api: 0.14` header every
protected graph route requires (`api_contract_mismatch` without it). Managed
values support one standard `Range`,
`ETag`, `If-Match`, and `If-None-Match`. Treat an ETag as an opaque validator of
that graph representation, not a content hash.

For a whole-object external value, the server returns `302` with the stored URI
in `Location`; it does not fetch, sign, authorize, or proxy the object.

`PUT` on the same route stores a raw `Content-Type: application/octet-stream`
body (at most the server's `write_max_bytes`, never above 32 MiB) and `DELETE`
clears the cell. Both take `branch` (default `main`), refuse `snapshot`, honor
`If-Match`, and answer a receipt whose `commit` is `null` only for a clear of a
null cell. An admitted write finishes even if the client disconnects.

## Lifecycle

A reader stays pinned to the snapshot selected when it opens. Explicit
`cleanup` (including table versions only a deleted branch kept) can reclaim
bytes needed by a long read, so quiesce such readers first; branch deletion and
schema drops alone defer reclamation to `cleanup`. Blob-aware compaction is
supported.

Historical identity fails closed: if a rename, drop/re-add, or branch lifetime
does not prove that a historical property is the same logical Blob property,
Omnigraph returns an error rather than guessing.

Canonical user contract: [Blob values](https://github.com/ModernRelay/omnigraph/blob/main/docs/user/blobs.md).
