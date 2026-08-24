# Schema Language (`.pg`)

## Top-level declarations

- `interface <Name> { property* }` — reusable property contracts.
- `node <Name> [implements <Iface>, ...] { property* | constraint* }`
- `edge <Name>: <FromType> -> <ToType> [@card(min..max)] { property* | constraint* }`
- Comments: line `//` and block `/* … */`.

## Property declarations

`<ident>: <TypeRef> [annotation*]`

Property names starting with `_` are reserved for system columns and are
rejected when a schema is admitted. The reserved namespace covers Lance's
virtual system columns (`_rowid`, `_rowaddr`, `_rowoffset`,
`_row_created_at_version`, `_row_last_updated_at_version`) and OmniGraph's
own implicit stored columns, spelled `__id` on nodes and edges and
`__src`/`__dst` on edges for graphs created at the current schema version.
Graphs created before that version keep the earlier spellings `id`, `src`,
and `dst` for their implicit columns and continue to reserve those three
property names; on current graphs `id`, `src`, and `dst` are ordinary
property names. Result payloads carry each graph's own implicit column
names, and the schema endpoint (`GET /schema`) reports them in its
`system_columns` field so clients never have to guess.

## Built-in scalar types

| Scalar | Logical / physical representation |
|---|---|
| `String` | Utf8 |
| `Blob` | Compiler placeholder: LargeBinary; persisted graph column: Lance Blob-v2 (`lance.blob.v2`) on file format V2_2 |
| `Bool` | Boolean |
| `I32` / `I64` | Int32 / Int64 |
| `U32` / `U64` | UInt32 / UInt64 |
| `F32` / `F64` | Float32 / Float64 |
| `Date` | Date32 |
| `DateTime` | Date64 |
| `Vector(<dim>)` | FixedSizeList(Float32, dim), `1 ≤ dim ≤ i32::MAX` |
| `[<scalar>]` | List(scalar) |
| `enum(v1, v2, …)` | Utf8 with sorted/dedup'd set of allowed string values |
| `<scalar>?` | Same as scalar but `nullable: true` |

## Constraints (body level)

| Constraint | On | Effect |
|---|---|---|
| `@key(p, …)` | node | Primary key; the complete ordered tuple identifies the node and implies indexes on its properties |
| `@unique(p, …)` | node, edge | Uniqueness across listed non-Blob properties |
| `@index(p, …)` | node, edge | Declare indexes on the properties; each eligible property's type selects BTREE, full-text, or vector indexing |
| `@range(p, min..max)` | node | Numeric range validation (open ranges allowed) |
| `@check(p, "regex")` | node | Regex pattern validation |
| `@card(min..max?)` | edge | Edge multiplicity — default `0..*`; `0..1`, `1..1`, `1..*`, etc. |

Edge bodies only allow `@unique` and `@index`. Blob properties are not eligible
for `@key`, `@unique`, `@index`, or `@embed`, whether the constraint is written
on the property or in the type body.

Compatibility note: a v6 graph whose persisted accepted schema predates v0.10
and contains the formerly accepted body-level `@unique(BlobProperty)` shape can
still be opened for inspection and export. New init and schema-apply input
reject that shape; the compatibility reader does not make the constraint
enforceable. Rebuild the graph with the invalid constraint removed.

## Annotations

- `@<ident>` or `@<ident>(<literal>)` on any declaration or property.
- Known annotations:
  - `@embed("source_property")` on a Vector property — records which String property is the embedding source for query-time `nearest($v, "string")` auto-embedding. It is a catalog annotation; it does **not** populate the vector at ingest (supply vectors in load data, or pre-fill via the offline `omnigraph embed` pipeline). An optional `model="…"` kwarg (`@embed("source_property", model="openai/text-embedding-3-large")`) records the embedding model so a `nearest()` query whose embedder uses a different model is rejected loudly; `model` is the only supported kwarg. See [search/embeddings.md](../search/embeddings.md).
  - `@description("…")`, `@instruction("…")` on query declarations (carried through to clients).
- Custom annotations are accepted by the parser and surfaced in catalog metadata; unrecognized annotations don't fail compilation.

<a id="table-layout"></a>

## Type and dataset layout

- Each node type compiles to a Lance dataset with a physical `id: Utf8` column plus all declared properties; `implements` clauses expand the interface's properties into the node.
- Each edge type compiles to a Lance dataset with physical `id: Utf8, src: Utf8, dst: Utf8` columns plus the edge's own properties. Edge endpoint types (`from`/`to`) must exist, and edge names are matched case-insensitively.

The compiler uses LargeBinary only as its dependency-free logical placeholder
for `Blob`. When the engine creates a physical node or edge dataset, it replaces
that placeholder with Lance Blob-v2 on explicit file format V2_2. Blob-v2 is a
descriptor-backed extension column: null is the Arrow parent validity, while a
non-null zero-length value is a valid empty Blob. Physical placement is
Lance-owned and is not part of the `.pg` schema contract.

Blob input keeps one JSON spelling across nodes and edges:

- `base64:<payload>` supplies managed bytes owned by the graph;
- any other string requests an external URI reference.

New external URI ingress is denied by default. Embedded callers must install a
graph-level `ExternalBlobPolicy` with an exact allowed base; cluster-served
graphs declare the equivalent per-graph policy in `cluster.yaml`. The policy is
an additional resource boundary, not a per-request option. It compares
normalized URI components rather than string prefixes, never permits
`file://` in server execution, and probes each distinct approved source before
the write's first durable effect.

The direct-store CLI has no allow-policy source in this phase, so a bare
`--store`/positional graph open admits managed `base64:` Blob input only. To
write a new external reference from the CLI, target a cluster server whose
graph has configured bases; an embedded application can instead install the
same graph-level policy on its engine builder. There is no command flag that
weakens the policy for one request.

Write mode determines ownership. Overwrite preserves an approved external URI
as a caller-owned reference. Strict insert, upsert, append/merge load,
mutation-update carry, and a HEAD-advancing branch merge that writes entities copy
approved source bytes into managed Blob storage under the existing 32 MiB
keyed-write ceiling. A pointer-only branch fast-forward preserves the existing
descriptor without policy approval or source I/O. Existing stored external
references remain readable and exportable when new ingress is denied;
OmniGraph never deletes their target objects.

## Embedded Blob reads

Blob delivery is deliberately separate from `.gq` projection. Embedded Rust
callers use `Omnigraph::read_blob_at(ReadTarget, BlobCell)` for either a node or
an edge cell. The selector names the logical entity kind, current accepted type
and property aliases, and exact logical entity `id`; caller text is matched
through a typed exact-ID expression, never flattened into SQL. The engine
resolves one branch or snapshot, the handle's current accepted catalog, and one
exact published dataset version for the complete call.

For the fail-closed compatibility checks below, the engine may compare current
branch authority, but only as an admission witness. Row, descriptor, ETag, and
payload data remain on the immutable selected target and never retarget to live
branch data.

The result is one of two non-null states:

- managed content exposes its logical length, a strong engine-owned ETag, and a
  `Send + Sync` `BlobReader`; or
- external content exposes the stored absolute URI, offset, and optional length
  without opening, probing, signing, or reading that caller-owned object.

A null cell is typed `NotFound`, not a zero-length payload. A valid empty managed
Blob has length zero and `read_range(0..0)` succeeds with empty bytes. Managed
reader ranges are half-open and valid exactly when
`start <= end <= logical_length`; an empty `logical_length..logical_length` is
valid. Reversed or out-of-bounds ranges return
`BlobRangeNotSatisfiable { start, end, length }`. One read returns at most
`BLOB_READ_RANGE_MAX_BYTES` (4 MiB). A wider in-bounds request returns typed
`ResourceLimitExceeded` for `Blob read range bytes` before payload I/O, so larger
values are consumed through consecutive bounded ranges. Malformed persisted
descriptors return `BlobIntegrity { reason }`, never null or plausible bytes.

Managed ETags are quoted lowercase hex over a stable identity tuple at the exact
published dataset version plus the exact non-empty `transaction_file` identity stored in
that immutable opened Lance manifest. They are deliberately
dataset-version-granular, so an unrelated write to the same dataset can change a
token even when this cell's bytes do not. Exact numeric version plus immutable
manifest identity prevents branch deletion/recreation from reusing a token
without widening it to graph-snapshot granularity. A missing or empty witness
is `BlobIntegrity { reason }`, never a weaker token. External references have no
ETag because their current bytes are not owned by the graph.

After a pure type rename, the current type alias can still address pre-rename
dataset history because stable table/incarnation identity crosses the rename. The
retired type alias is not accepted. This dataset-identity binding remains subject
to the independent physical-name, property-lifetime, and branch-incarnation
checks below. Property renames are deliberately narrower in Phase 1: a
pre-rename dataset version does not expose the current property
spelling, while the retired spelling is absent from the current catalog. Such a
historical property read returns `BadRequest`; the engine does not infer by
column position. Historical property-field crossing is deferred.

Physical user fields newly initialized, added, or schema-rebuilt by 0.10 persist
their authoritative graph property lifetime as `omnigraph.stable_property_id`
metadata. Blob reads compare that marker with the current accepted catalog. A
soft-dropped property re-added under the same name has a different stable ID,
so its old snapshot is refused with `BadRequest`; Lance field IDs and positions
are never identity. A
malformed marker is `BlobIntegrity`.

Existing pre-0.10 v6 fields lack the marker. Schema-preserving Append, Merge,
and mutation writes retain that unmarked schema. Full-dataset Overwrite instead
carries the 0.10 catalog schema and adopts the marker on its replacement fields;
it does not rewrite older versions. An unmarked field remains readable when the
selected snapshot points at the exact current physical dataset entry. An older
snapshot without the marker fails `BadRequest` with `no persisted
property-lifetime witness`, because OmniGraph cannot prove that the same
spelling did not cross a drop/re-add lifetime. This refusal applies even when no
rename occurred. This is additive field metadata, not a manifest-format bump.

Named-branch reads have a separate incarnation fence. An explicit snapshot's
reopened graph manifest must still carry the resolved graph commit, so deleting and
recreating a named graph ref cannot retarget even an inherited-main dataset. V6
entries do not persist Lance's native `BranchIdentifier`, and a Lance dataset-manifest e-tag
is not a sufficient substitute. After opening a dataset stored on a named native
branch, OmniGraph cold-rechecks that the selected graph ref still has the
captured effective head. A raced live read and an older branch-owned snapshot
therefore return `BadRequest` with `no persisted native-branch incarnation
witness` rather than retargeting. Genuine main/inherited-main dataset history
remains eligible after the graph-snapshot proof; the independent property/schema
checks still apply.

The returned reader stays on its captured version if a branch advances.
Deletion of that branch and physical tree reclamation are destructive
boundaries, like `cleanup`: Phase 1 adds no durable or cross-process live-reader
lease. A reader never redirects to newer bytes, but an uncached later range may
fail loudly after reclamation. Quiesce readers before deleting their branch or
running version GC when they must finish. HTTP Blob delivery and CLI
`blob get`/`blob stat` now expose this bounded facade without leaking physical
placement. The pre-1.0 Lance-returning `Omnigraph::read_blob` method has been
removed; there is no compatibility wrapper that leaks `BlobFile`.

For a keyed node, `id` is derived from the complete typed `@key` tuple. A
single-property key keeps its canonical scalar spelling. A composite key is an
unambiguous JSON array of those canonical scalar strings, ordered by stable
property identity so renaming a key property cannot change existing node IDs.
Integers use decimal spelling, booleans use `true`/`false`, Date and DateTime
use their stored epoch-day and epoch-millisecond values, finite floats use
their stored-width spelling, and both signed zeros become `0`; non-finite
floats are not valid keys. Load and mutation use this same derivation. Exported
keyed entities include the physical `data.id`; on rebuild, a legacy scalar spelling
that is typed-equivalent is accepted and rewritten to the canonical ID, with
typed edge endpoints rewritten in the same import. New hand-authored entities may
omit `data.id` and let the loader derive it.

## Schema migration planning

A migration plan compares the accepted schema against the desired one and reports whether the change is supported plus the ordered steps it requires:

- Add a type
- Rename a type
- Add a property
- Rename a property
- Add a constraint
- Extend an enum (pure widening: add variants to an existing `enum(...)` property — same base type and nullability, every existing value retained; metadata-only at apply time, no dataset data touched, and the new variants are accepted immediately on every write surface. Narrowing, renaming a variant, or converting between an enum and a free `String` still plan as unsupported, `OG-MF-106`. Value *order* is not significant — the schema IR normalizes enum values, so a reorder is not a change at all.)
- Update type or property metadata (annotations)
- Unsupported change (reports the entity and reason; forces the plan to unsupported)

Applying a plan reports whether it was supported, the steps applied, and the resulting graph manifest version. Concurrent schema applies serialize so they can't interleave.

## Destructive drops — `--allow-data-loss`

`DropProperty` and `DropType` steps default to `Soft` mode: the catalog tombstones the entry but the prior physical property field or dataset remains time-travel-reachable via `snapshot_at_graph_manifest_version(prev)` until `omnigraph cleanup` runs. Soft drops are reversible.

Pass `--allow-data-loss` (CLI `schema apply`) or `allow_data_loss: true` (SDK `SchemaApplyOptions`) to promote every drop in the plan to `Hard` mode. Hard drops run `cleanup_old_versions` on the affected dataset immediately after the graph-manifest publish, making the prior physical property field or dataset unreachable. **Irreversible.**

This is the **direct/embedded** schema-apply path — `omnigraph schema apply --store …` and the embedded SDK `apply_schema_with_options(.., SchemaApplyOptions { allow_data_loss: true })` produce identical plans and identical effects.

**Cluster-managed graphs are different.** A graph served from a cluster evolves only through `omnigraph cluster apply`, which performs **soft drops only** (no `allow_data_loss` path), and the HTTP `POST /schema/apply` route is **disabled (returns 409) for cluster-backed serving** — see [server](../operations/server.md) and [cluster-config](../clusters/config.md). Direct `schema apply` against a cluster-managed storage path is likewise refused.
