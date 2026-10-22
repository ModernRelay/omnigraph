# Mutations and Loads

Mutation statements live inside a named `.gq` query. Run them with
`omnigraph mutate`.

```gq
query hire($person_id: String, $company_id: String, $role: String) {
  insert WorksAt {
    from: $person_id,
    to: $company_id,
    role: $role
  }
}
```

Edge endpoints use the reserved assignments `from` and `to`; their values are
the logical ids of existing endpoint nodes.

## Statements

```gq
insert Person { email: $email, display_name: $name }
update Person set { display_name: $name } where email = $email
delete Person where email = $email
```

A `where` on `update` or `delete` takes the same expressions as a read filter
(see [Boolean expressions and nulls](../queries/index.md#boolean-expressions-and-nulls)):
`and`, `or`, `not`, parentheses, the six comparisons, `starts_with`,
`contains`, `in`, `is null` and `is not null`. Its operands are the target type's
properties, named bare, the system fields `@id`, `@src` and `@dst`, literals,
parameters, and `now()`; `from` and `to` stay accepted for `@src` and `@dst`.
It keeps only the rows whose expression is true. On four `Knows` edges, `ab1`
and `ab2` from `a` to `b`, `ac`, and `db`:

```gq
query exact() { delete Knows where @src = "a" and @dst = "b" }
```

removes `ab1` and `ab2` and nothing else: `affected: nodes=0 edges=2`. A
binding variable in a `where` is refused at compile time (`T14`); an
aggregate, search, or ranking call is `T44`, for example ``T44: `count` cannot
appear in a mutation where; a where compares the row's own properties,
parameters and now()``. A compound `where` runs under either `engine` setting.

Assignment values are constants: literals, parameters, `now()`, and
comparisons or Boolean operators over them, such as `adult: true or $flag`.
A constant's value is fixed per invocation: parameters and `now()` are bound
once before any retry, so a retried mutation computes the same value. It follows the read rules
for null: with `$flag` null, `true or $flag` is `true` and `false and $flag`
is `false`, and `$age > 30` with `$age` null is null. A null result assigned
to a nullable property writes null; on a `Blob` property it clears the cell.
`null` is a reserved word, so clear a Blob through a nullable parameter such
as `$content: Blob?`. Assigned to a non-nullable property a
null result is refused with a typed error that names the property, never
written as a default. A
property or system field in a value is refused, for example ``T45: `age`
cannot appear in an assignment value; assignments and binding matches are
constants per invocation``.

A mutation query may contain several inserts and updates, or several deletes,
but it cannot mix inserts or updates with deletes. Split that workflow into two
queries, or run the queries on a branch and merge when the combined result is
ready.

## Atomicity

An effectful mutation query publishes one graph commit. All of its statements
become visible together, or none do. Separate mutation commands are separate
commits, even when they run consecutively.

With `--json`, a successful effectful mutation returns `commit` with the
exact `graph_commit_id` and commit metadata published by that attempt. A
successful mutation that matches no entities publishes no commit and returns
`"commit": null`. Load JSON responses use the same exact receipt.

For a multi-command workflow, use a branch as isolated staging. Earlier changes
remain committed on that branch; merging makes the resulting branch state
visible on the target in one atomic step. See
[Branches, Commits, and History](../branching/index.md).

## Insert and update identity

- Inserting a node or edge with `@key` is an upsert by its derived id.
  Inserting the same key again updates the existing row.
- Inserting a node without a key is a strict insert with a generated or supplied
  id.
- Inserting an edge without a key is a strict insert with a generated or
  supplied id; the same pair inserted twice is two edges.
- Key properties cannot be changed by an update. Edge types do not support
  update at all: change a keyed edge by inserting its key again with the new
  values (an upsert), and an unkeyed edge by delete and re-insert.

All declared value, uniqueness, endpoint, and cardinality constraints are
checked before publication.

## Bulk loading

`omnigraph load` accepts newline-delimited JSON. One file can contain nodes and
edges of several types:

```jsonl
{"type":"Person","data":{"email":"ada@example.com","display_name":"Ada"}}
{"type":"Company","data":{"slug":"acme","name":"Acme"}}
{"edge":"WorksAt","from":"ada@example.com","to":"acme","data":{"role":"Engineer"}}
```

Here, `Person.email` and `Company.slug` are single-property String keys, so their
derived ids are exactly `ada@example.com` and `acme`. The edge uses those ids in
`from` and `to`.

A `Date` value is an integer day count since 1970-01-01 or a `YYYY-MM-DD`
string; a `DateTime` value is an integer millisecond count since the Unix epoch
or an ISO 8601 string. Any other JSON type (a float such as `19723.0`, a
boolean, an object) fails the load with `invalid Date value` or `invalid
DateTime value` naming the property. A `Date` string carries no time of day:
`"2024-01-01T02:00:00+05:00"` fails the load with `invalid Date literal`, as it
does in a mutation param, a `date(...)` literal, or a read filter; an instant
belongs in a `DateTime` property. A `DateTime` holds milliseconds: a string with
a non-zero digit past the third fractional digit, such as
`"2024-01-01T00:00:00.123456Z"`, fails the load with `invalid DateTime literal`,
as it does in a mutation param, a `datetime(...)` literal, or a read filter.
Trailing zeros, as in `.123000`, are accepted.

Choose the mode explicitly:

| Mode | Existing id | Use |
|---|---|---|
| `append` | Fails with `key_conflict` | Add entities without replacing anything. |
| `merge` | Updates the existing entity | Upsert a batch. |
| `overwrite` | Replaces every node or edge type represented in the batch; types absent from the batch remain unchanged | Rebuild from a complete export or seed. |

For a keyed edge the existing-id column applies to its derived id: `append`
reports `key_conflict` on an already-committed pair, `merge` upserts it, and
a supplied top-level `id` must equal the derivation exactly.

```bash
omnigraph load --data batch.jsonl --mode merge --target ./graph.omni
```

One load request is one graph commit. Use `--branch <name> --from <base>` to
create a missing review branch and load onto it in the same workflow.

Loads preserve supplied embeddings and do not generate them. Results report
`embedding_generation: "unsupported"` when a loaded node type declares
`@embed`, or `null` otherwise; see [Embeddings](../search/embeddings.md).

### CSV loading

The `load csv` statement reads one CSV object into one declared node or edge
type, with every cell typed by the `.pg` declaration. It is a GQ statement, so
it runs wherever a `branch create` statement runs: `omnigraph mutate -e`, a
`.gq` file, or `POST /mutate` with the request `branch` as its target (`main`
when omitted). It is the one CSV surface: `omnigraph load` reads JSONL only.

```gq
load csv "s3://imports/person.csv" into node:Person mode append delimiter "|" header
load csv "s3://imports/knows.csv" into edge:Knows mode append delimiter "|" header columns (from, to, creationDate)
```

- `mode` is required: `overwrite`, `append`, or `merge`, with the meanings of
  the table above.
- `delimiter "<c>"` is one printable ASCII character; `,` when unspelled.
- `header` makes the first record name the columns: each name is the property
  it feeds, and an edge file's first two columns are its endpoints whatever
  the header calls them (`Person.id|Person.id` in an LDBC file).
- `columns (a, b, …)` names the property each column feeds, by position. It
  is required without `header` and overrides a header's names. `from` and
  `to` name an edge file's endpoint columns and are refused on a node file.
- An empty field is null; a null for a non-nullable property, a column the
  type does not declare, a count of `columns` that differs from the source, a
  malformed number, a repeated name, and an edge file without `from` or `to`
  are refused before anything is staged.

Numbers are typed by the reader. `Bool` takes exactly `true` or `false`.
`Date` and `DateTime` cells are read as text and pass the checks above
before conversion: `2024-01-01T02:00:00+05:00` in a `Date` column and
`2024-01-01T00:00:00.123456Z` in a `DateTime` column fail the load exactly
as they fail a JSONL load, and a numeric offset without a colon, as LDBC
writes it (`2010-02-14T15:32:10.447+0000`), stores the instant. List, Vector
and Blob properties have no CSV encoding: a column mapped to one is refused
with the kind named; an unmapped one stores null when nullable.

Float values must be finite and representable by their declared `F32` or
`F64` type; NaN, infinity and overflow refuse the load. Each decoded batch
is held against the statement's `query_memory_bytes` allowance before it
is retained, and each staged-write input against `write_max_bytes`.

Node ids derive from the `@key` as in a JSONL load; an edge's `from` and `to`
are the canonical ids of its endpoints, so a single-property integer key is
written as its decimal value. The statement never forks a branch.

The source is a URI the engine reads under the graph's external Blob policy
([External Blob references](../clusters/config.md#external-blob-references)):
`s3://` objects beneath a configured
base, and for an embedded run `file://` paths beneath an `embedded_only`
base. A graph with no policy denies every source; a URI outside every base,
an unsupported scheme, and an object over the
[`load_source_max_bytes`](../queries/settings.md) setting (default 32 MiB)
are refused before any byte is read, and the read is bounded by a 120-second
deadline. A refused source names `csv_source_bytes`; raising the setting in
the statement's `set` prefix (or the request's `settings` field) admits a
larger object. A direct `omnigraph mutate`
run installs an embedded-only base over a `file://` source's directory for
that statement, so the file is admitted by the same rule; a served run
refuses a `file://` source, since the server reads only what it may reach.

`explain` before a `load csv` statement prints the load's plan, the same
document a query's explain prints: the source, the one validation with each
check and its access path, and the write.
An overwrite runs that plan; append and merge run the keyed adapters.
It reads the source's metadata and no payload byte.
See [Explaining a load statement](../queries/explain-load.md).

#### CLI and HTTP requests

```bash
omnigraph mutate --target ./graph.omni -e 'load csv "file:///data/person_0_0.csv" into node:Person mode append delimiter "|" header'
omnigraph mutate --target dev -e 'load csv "s3://imports/knows.csv" into edge:Knows mode append delimiter "|" header'
```

The CLI uses `--branch`; HTTP uses the request `branch`, with `main` as the
omitted default. The statement accepts no `name`, `params` or commit
precondition (`--if-commit` or `Omnigraph-If-Graph-Commit`). It requires
`change` on the branch. A source that holds no records is refused. The
response is a mutation's `ChangeOutput`: the
branch, `load csv` as `query_name`, loaded node and edge counts, the commit,
and for `mode overwrite` an `executed` object, what the load's plan measured
as it ran ([explaining a load statement](../queries/explain-load.md)).

```bash
curl -sS http://localhost:8080/graphs/knowledge/mutate \
  -H 'Omnigraph-Http-Api: 0.15' \
  -H 'authorization: Bearer secret-a' \
  -H 'content-type: application/json' \
  -d '{"query":"load csv \"s3://imports/person.csv\" into node:Person mode append delimiter \"|\" header","branch":"main"}'
```

The HTTP body carries the statement; the server reads its source under the
graph's `external_blobs.allow` policy, with server-safe bases only. Before
fetching, workload admission reserves the request's `load_source_max_bytes`
(default 32 MiB) plus the statement bytes against both global and per-actor
budgets, so a raised setting must still fit the server's own limits. This reservation lasts until the write
settles, including when the HTTP caller disconnects.

## Limits and conflicts

Insert/update mutations and incremental keyed loads are bounded to 8,192
entities per touched type. The [`write_max_bytes`](../queries/settings.md)
setting (default 32 MiB) bounds retained row data per type and across all
touched types. Blob payloads have a separate allowance of the same size,
including inline bytes, copied external bytes and carried update values.
Every version of a key written in one mutation counts, so an insert and an
update of the same row charge its Blob twice.
Keyed loads also bound their parser estimate independently. A strict JSONL
load retains its projected input check, `strict_input_arrow_bytes`.
A CSV load has no whole-statement input check.
It applies `write_max_bytes` to each staged-write input instead.
An overwrite stages the reader's 8,192-row batches, and a batch over the
allowance refuses as `staged_write_input_bytes`.
An append or merge keeps the keyed adapter's ceilings
(`retained keyed batch bytes per operation`, `keyed entities for <table>`,
`keyed entity bytes for <table>`).
The [`query_memory_bytes`](../queries/settings.md) setting bounds one
`load csv` statement's tracked memory as a whole: the retained source, every
decoded batch, validation state and the staging working set.
A statement the allowance cannot hold refuses with the `query_memory_bytes`
resource error before any fragment write.
A CSV load needs roughly the source plus twice its typed input under that
allowance.
The source object is bounded by `load_source_max_bytes` before it is read
(refused as `csv_source_bytes`).
The loader reads the object once per statement, under the size, etag and
version its metadata probe captured, and every reprepare attempt reuses those
bytes.
An object replaced between the probe and the read refuses as an
external-source error, and nothing is staged.
Raising `write_max_bytes` lifts `strict_input_arrow_bytes` and
`staged_write_input_bytes`; raising `query_memory_bytes` admits a larger
statement; raising `load_source_max_bytes` lifts `csv_source_bytes`. See
[Blob limits](../blobs.md#limits).

Deletes, including cascades, and overwrite loads collecting replaced IDs have
a separate `write_max_bytes` allowance per operation for those IDs, summed over all
touched types. Each removed ID is charged its UTF-8 length plus 24 bytes, so
the default allowance holds 671,088 IDs of 26 bytes, the length of a generated ID. A
delete and the edges it cascades to draw on the same allowance. An overwrite of
entities loaded without a `@key` and without an explicit `id` removes every
committed ID of that type, because those IDs are generated again on each load.

Oversized work returns a resource-limit error before its data is staged or
published. These checks do not bound total engine memory. The setting can
change each allowance from its 32 MiB default; a CSV overwrite stages its
input as a vector of reader batches, each bounded by the allowance, under
the statement's `query_memory_bytes`. Split larger inserts, updates, keyed
loads and deletes into explicit commits. An overwrite replaces each
represented type as one image and cannot be split: it keeps its bulk-input
behavior and remains subject to its removed-ID check.

Independent existing constructive datasets stage concurrently. The
`stage_write_concurrency` [session setting](../queries/index.md#session-settings)
controls that width for both Load and insert/update mutations (default 8,
range `1..=64`); it is `process` scope, so the server takes it from
`OMNIGRAPH_LOAD_CONCURRENCY` and a direct CLI run from that variable or from
`--set` on `load`, `ingest` or `mutate` (a JSONL input carries no `set` line),
and an invalid or `0` value refuses startup instead of running the default.
The same width is the number of Lance writers a `load csv` overwrite runs at
once: its batches are cut into runs of at most one Lance file (1 M rows), each
run written by its own writer, and the files are published in input order, so
the table's files are the ones a single writer would have produced. It is also
the most streams that parse the overwrite's source: the source is cut on
record ends into 8 MiB slices, a stream claims the next slice as it finishes
one, and the validation puts the slices back in source order, so the rows and
their order are those of a single parser. Delete staging remains serial. This affects preparation only—one request still
publishes exactly one graph commit.

A stale strict update, delete, or overwrite can return `read_set_conflict`.
Refresh the branch and retry deliberately. A `key_conflict` means an append or
strict insert found an existing id; it never silently becomes an upsert.

If a write returns `recovery_required`, do not immediately resubmit it. Reopen
the graph read-write or restart the server, then retry from a fresh branch head.

## Blobs

Blob assignments accept managed `base64:` data and, when allowed by graph
policy, external URI references. Ownership differs by load mode, and Blob bytes
count toward write limits. See the canonical [Blob guide](../blobs.md) before
loading them, and its [limits](../blobs.md#limits) for the bounds that apply.

## Conditional mutations

A mutation can be made to apply only to the graph state the caller read, with
`--if-commit`; see [conditional mutations](conditional.md).
