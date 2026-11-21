# Mutations and Loads

Mutation statements live inside a named `.gq` query; run it with `omnigraph mutate`.

```gq
query hire($person_id: String, $company_id: String, $role: String) {
  replace WorksAt {
    @src: $person_id,
    @dst: $company_id,
    role: $role
  }
}
```

An edge write assigns its endpoints as `@src` and `@dst`, the logical ids of existing nodes or bound references; edition 2's `from:` and `to:` are refused (`T11`) with that fix. Where a node type and an edge type share a name, a statement assigning `@src` or `@dst` writes the edge type and one assigning neither the node type.

## Statements

Five statements write, one per intent:

| Statement | Key absent | Key present | Names its target by |
|---|---|---|---|
| `insert T { ... }` | creates | fails with `key_conflict`; nothing publishes | key |
| `upsert T { ... }` | creates | sets the named properties; the others, Blob and vector included, keep their values | key |
| `replace T { ... }` | creates | the row becomes exactly the assignments: an omitted nullable property becomes null, Blob and vector included | key |
| `update $x set { ... }` | selects nothing | sets the named properties of every selected node or edge | selection |
| `delete $x` | selects nothing | removes every selected node or edge, and a node's incident edges | selection |

```gq fragment
insert Person { email: $email, display_name: $name }
upsert Person { email: $email, display_name: $name }
replace Person { email: $email, display_name: $name }

match { $x: Person } where $x.email = $email
update $x set { display_name: $name }

match { $x: Person } where $x.email = $email
delete $x
```

A write body is one or more groups. A group selects its rows with a read's
`match { ... }` and `where`, and its statements run once per row: `update $x
set { ... }` and `delete $x` name a bound node or edge variable (another is
`T77`), and `insert`, `upsert` and `replace` write one entity per row, once with no `match`.
A later group opens with its own `match`, and every group selects on the graph
the write started from, never on an earlier group's effects, so a group whose
selection reads a type an earlier group of the write changes is refused
(`T91`): write it as a second write, or as statements after one `match`, which
each see the statements before them. A statement takes no `where`: `delete T
where ...` is refused (`Q008`) with the group form as the fix, and elements
written without `match { }` are refused (`Q013`).

The selection takes everything a [read's match](../queries/index.md#match-patterns)
does: traversals and edge bindings (`$p $k:worksAt $c` then `delete $k`),
`exists`, `not exists` and `count { ... }` blocks, `let` and `from`, and runs
once, before its group's first statement; `optional` is not supported yet
(`T86`). An unbound variable is `T6` and an aggregate `T8`; a filter on one
binding testing over four properties twice each with `=`, `!=` or `in` fails.

Assignment values are literals, parameters, `now()`, expressions over them,
and values read from the row (`city: $c.city`, `@dst: $c` for an endpoint);
constants are bound once before any retry. They follow the read rules for
null: a null result, or `null` written as the value, clears a nullable
property, and on a non-nullable property is refused with a typed error naming
it. A value read from the row may not be a `Blob` (`T86`). Rows of one
statement that give one entity's property two values are refused (`E4`); a
keyed insert whose rows repeat a key with equal values writes it once. A
write whose selection matches nothing publishes no commit. A write mixes no
`delete` with the other four (`T76`): split it into two queries, or run them on
a branch and merge the result.

## Atomicity

An effectful mutation query publishes one graph commit: all of its statements
become visible together, or none do. With `--json` it returns `commit`, the exact
`graph_commit_id` and metadata that attempt published, or `"commit": null` when
nothing changed; loads return the same receipt. For a multi-command workflow,
stage the commits on a branch and merge them in one atomic step ([Branches,
Commits, and History](../branching/index.md)).

## Keys and identity

- `null` clears a property in every statement; only `replace` clears what it omits.
- Creating without a required property, an `@embed` vector included (no write
  embeds), is an error (`T12` for `insert` and `replace`, at run time for an
  `upsert` of a new key); an `upsert` of an existing key needs none.
- `insert` fails with `key_conflict` on a committed key or one the write
  staged earlier, naming `upsert` and `replace`. A job that retries after an
  uncertain outcome uses those two, which are idempotent.
- A type without `@key` has no key to match: `insert` creates with a generated
  or supplied id (the same pair twice is two edges); `upsert` or `replace` on
  it is `T92`, fixed by `insert`.
- Keys and endpoints are identity: an `update` assigning one is `T93`; move an
  edge by a delete and an insert, in two writes.
- `upsert` reads the stored values of the properties a row omits, Blob bytes
  included; a row naming every property reads nothing more than `replace`.
- An `update` that changes no stored value publishes no commit and still
  reports the entities it selected; an unchanged `upsert` or `replace` still
  publishes one, since proving it unchanged would need a read they do not make.
- `upsert` and `replace` are statement words only where a statement starts.

All declared value, uniqueness, endpoint, and cardinality constraints are
checked before publication; a node a `@card` minimum needs edges for arrives with them in one query or load ([edge cardinality](../schema/index.md#edge-cardinality)).

## Bulk loading

`omnigraph load` accepts newline-delimited JSON. One file can contain nodes and
edges of several types:

```jsonl
{"type":"Person","data":{"email":"ada@example.com","display_name":"Ada"}}
{"type":"Company","data":{"slug":"acme","name":"Acme"}}
{"edge":"WorksAt","from":"ada@example.com","to":"acme","data":{"role":"Engineer"}}
```

An edge record names its endpoints' derived ids as `from` and `to`: here the
String keys `ada@example.com` and `acme` themselves.

A `Date` is an integer day count since 1970-01-01 or a `YYYY-MM-DD` string; a
`DateTime` an integer millisecond count since the Unix epoch or an ISO 8601
string. Any other JSON type fails with `invalid Date value` or `invalid DateTime
value`; a `Date` string with a time of day and a `DateTime` with more than
millisecond precision fail as their literals do in a mutation param or filter.

Choose the mode explicitly; each is the statement of its name:

| Mode | Existing id | Use |
|---|---|---|
| `insert` | Fails with `key_conflict`; nothing publishes | Add entities without replacing anything. |
| `upsert` | Sets the properties the record names; the others keep their values | Patch existing entities from partial records. |
| `replace` | Writes the whole entity; a property the record omits becomes null | Reload complete records. |
| `overwrite` | Replaces every node or edge type represented in the batch; types absent from the batch remain unchanged | Rebuild from a complete export or seed. |

Under `upsert` a record's absent property key keeps the stored value and an
explicit `null` clears it. `append` and `merge` are refused with `insert` and
`replace`, which mean what they meant. For a keyed edge the existing-id
column applies to its derived id, and a supplied top-level `id` must equal the
derivation exactly.

```bash
omnigraph load --data batch.jsonl --mode replace --target ./graph.omni
```

One load request is one graph commit; `--branch <name> --from <base>` creates a missing branch and loads onto it.

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
load csv "s3://imports/person.csv" into node:Person mode insert delimiter "|" header
```

```gq
load csv "s3://imports/knows.csv" into edge:Knows mode insert delimiter "|" header columns (from, to, creationDate)
```

- `mode` is required: `insert`, `upsert`, `replace` or `overwrite`, with the
  meanings of the table above (`append` and `merge` are `Q004`, with the new
  word as the fix). Under `upsert` a property with no column keeps its stored
  value and an empty field clears it.
- `delimiter "<c>"` is one printable ASCII character; `,` when unspelled.
- `header` makes the first record name the columns: each name is the property
  it feeds, and an edge file's first two columns are its endpoints whatever
  the header calls them (`Person.id|Person.id` in an LDBC file). In mode
  `overwrite` the first record is read from the source's first 8 MiB, with or
  without `header`; a first record longer than that is refused.
- `columns (a, b, …)` names the property each column feeds, by position. It
  is required without `header` and overrides a header's names. `from` and
  `to` name an edge file's endpoint columns and are refused on a node file.
- An empty field and `""` are null (there is no empty-string spelling); an empty line is not a
  record, so a one-column file skips its null rows. A null for a non-nullable property, a column
  the type does not declare, a count of `columns` that differs from the source, a malformed
  number, a repeated name, and an edge file without `from` or `to` are refused before anything is staged.

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
`s3://` objects beneath a configured base, and for an embedded run `file://`
paths beneath an `embedded_only` base. A graph with no policy denies every
source; a URI outside every base, an unsupported scheme, and an object over the
[`load_source_max_bytes`](../queries/settings.md) setting (default 32 MiB) are
refused before any byte is read, and every read of the object (each range an
overwrite's partition reads, or the one read of a keyed mode) is bounded
by a 120-second deadline. A refused source names `csv_source_bytes`; raising the
setting in the statement's `set` prefix (or the request's `settings` field)
admits a larger object. A direct `omnigraph mutate` run installs an embedded-
only base over a `file://` source's directory for that statement, so the file is
admitted by the same rule; a served run refuses a `file://` source, since the
server reads only what it may reach.

`explain` before a `load csv` statement prints the load's plan, the same
document a query's explain prints: the source, the one validation with each
check and its access path, and the write. An overwrite runs that plan; the
keyed modes run the keyed adapters. It reads the source's metadata and no payload
byte. See [Explaining a load statement](../queries/explain-load.md).

#### CLI and HTTP requests

```bash
omnigraph mutate --target ./graph.omni -e 'load csv "file:///data/person_0_0.csv" into node:Person mode insert delimiter "|" header'
omnigraph mutate --target dev -e 'load csv "s3://imports/knows.csv" into edge:Knows mode insert delimiter "|" header'
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
  -H 'Omnigraph-Http-Api: 0.14' \
  -H 'authorization: Bearer secret-a' \
  -H 'content-type: application/json' \
  -d '{"query":"load csv \"s3://imports/person.csv\" into node:Person mode insert delimiter \"|\" header","branch":"main"}'
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
including inline bytes, copied external bytes and carried update values. The
8,192 entities count each key once however many statements write it, since a
later statement's version replaces the earlier one. The bytes retained across
all touched types and the Blob allowance count every version, so an insert and
an update of the same row charge its Blob twice. Keyed loads also bound their
parser estimate independently. A strict JSONL load retains its projected input
check, `strict_input_arrow_bytes`. A vector counts its full width (dimension × 4
bytes), present or null: 12 KiB for `Vector(3072)`. A CSV load has no whole-
statement input check; it applies `write_max_bytes` to each staged-write input
instead. An overwrite stages the reader's 8,192-row batches, and a batch over
the allowance refuses as `staged_write_input_bytes`. An overwrite writes a
fragment per parse partition, up to 1 Mi rows each, as the rows drain; nothing
commits before the publish, a refusal deletes them all. A keyed mode keeps
the keyed adapter's ceilings (`retained keyed batch bytes per operation`, `keyed
entities for <table>`, `keyed entity bytes for <table>`). The
[`query_memory_bytes`](../queries/settings.md) setting bounds one `load csv`
statement's tracked memory as a whole: the retained source, every decoded batch,
validation state and the staging working set; a streaming overwrite's working
set is its open fragments and the writers in flight. An overwrite admits that
memory once, before any partition reads, one share per parse partition that only
its own reader charges, so a refusal and its `actual` (the smallest admitting
limit) are the same on every run (the shares: the settings reference); a
statement it cannot hold refuses before any fragment write. The source object is
bounded by `load_source_max_bytes` before it is read (refused as
`csv_source_bytes`). An overwrite probes the object's metadata once, then each
parse partition reads its own byte range in 8 MiB pieces, every read conditional
on the etag and size the probe captured; a refusal of the partitioned run reads
the ranges once more as one partition. A keyed mode reads the object once
per statement under the same validators, and every reprepare attempt reuses
those bytes. An object replaced between the probe and the read refuses as an
external-source error, and nothing is staged. Raising `write_max_bytes` lifts
`strict_input_arrow_bytes` and `staged_write_input_bytes`; raising
`query_memory_bytes` admits a larger statement; raising `load_source_max_bytes`
lifts `csv_source_bytes`. No setting lifts a merge's [validation
limit](../branching/merge.md#validation-limit), which counts a branch's commits
together. See [Blob limits](../blobs.md#limits).

Deletes, including cascades, and overwrite loads collecting replaced IDs have
a separate `write_max_bytes` allowance per operation for those IDs, summed over all
touched types. Each removed ID is charged its UTF-8 length plus 24 bytes, so
the default allowance holds 671,088 IDs of 26 bytes, the length of a generated ID. A
delete and the edges it cascades to draw on the same allowance. An overwrite of
entities loaded without a `@key` and without an explicit `id` removes every
committed ID of that type, because those IDs are generated again on each load.
An overwrite collects replaced IDs only when its plan's `removal` is `scan`, a
node target with committed rows that an edge type references; an edge target,
a node target no edge type references and an empty table collect none and draw
nothing from this allowance.

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
controls that width for loads and write statements (default 8, range `1..=64`);
it is `process` scope, so the server takes it from `OMNIGRAPH_LOAD_CONCURRENCY`
and a direct CLI run from that variable or from `--set` on `load`, `ingest` or
`mutate`, and an invalid or `0` value refuses startup. The same width bounds
the Lance writers a `load csv` overwrite runs at once (each writes runs of at
most one Lance file, published in input order) and the streams that parse its
source: each parse partition owns an equal byte range, read in 8 MiB pieces and
cut on record ends, and the slices are validated back in source order, so rows
and their order are those of a single parser. A boundary neighbors cannot agree
on (inside a quoted field, or after an empty line) reruns the plan as one
partition. Delete staging remains serial. One request still publishes exactly
one graph commit.

A write overtaken before it takes effect, an update or delete included, reruns on the new head; it returns
`read_set_conflict` only if the branch was recreated, the schema changed, or another process kept overtaking
it. A `key_conflict` means an `insert` statement or load found an existing id; it never silently becomes an `upsert` or `replace`.

If a write returns `recovery_required`, do not immediately resubmit it. Reopen
the graph read-write or restart the server, then retry from a fresh branch head.

## Blobs

Blob assignments accept managed `base64:` data and, when allowed by graph
policy, external URI references. Ownership differs by load mode, and Blob bytes
count toward write limits. See the canonical [Blob guide](../blobs.md) before
loading them, and its [limits](../blobs.md#limits) for the bounds that apply.

## Conditional mutations

`--if-commit` applies a mutation only to the graph state the caller read; see
[conditional mutations](conditional.md).
