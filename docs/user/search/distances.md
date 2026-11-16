# Vector distances

A vector's distance comes from the schema, never from a query. Declare it on
the property's type with `Vector(N, distance = ...)`:

```pg
node Doc {
  slug: String @key
  embedding: Vector(1536, distance = "cosine")? @embed("body") @index
  signal: Vector(8, distance = "dot")
  raw: Vector(3)
}
```

`distance` names one of three distances. Every value is ascending: a smaller
distance is nearer.

| Distance | Value |
|---|---|
| `l2` | sum of squared component differences, no square root |
| `cosine` | `1 - dot(x, q) / (norm(x) · norm(q))`, from 0 to 2 |
| `dot` | `1 - dot(x, q)`; can be negative |

An omitted distance is `l2`, which is what `Vector(N)` has always meant, so
`Vector(3)` and `Vector(3, distance = "l2")` are the same type and no existing
schema changes meaning. An unknown distance or option is a schema error that
lists the admitted names. A node that implements an interface declares the
interface property's distance too.

## What reads it

`distance(...)` and `nearest(...)` both read the field's declared distance; no
query names one. A returned distance is always the exact value under the
field's distance. An index affects only which targets `nearest` finds, never
the value it reports or the rows `distance` returns.

The vector index Omnigraph builds is an IVF_FLAT index built for the field's
distance. A `nearest` scan uses an index only when it was built for that
distance; any other vector index, such as one built for another distance, a
quantized one or an HNSW graph, is not used, and the scan ranks every row flat,
which is exact and reads the whole column.

Stored and query components must be finite. A distance that overflows `F32`
from finite components, such as `l2` between vectors near `3e38` of opposite
sign, fails the read with `E3`: `distance` fails on any such row, and `nearest`
fails when such a row is among the targets it returns. A `cosine` or `dot`
distance that overflows to an undefined value (infinity minus infinity) is
dropped by `nearest`'s scan, as a zero vector under `cosine` is, while
`distance` fails on it with `E3`.

## Zero vectors and `cosine`

Cosine is undefined at a zero vector, so a `cosine` field refuses one:

- A write that stores a zero vector in a `cosine` field (every component `0` or
  `-0`) fails validation and changes nothing. This holds for loads, inserts,
  updates and edge properties. A null is not a vector and is accepted.
- A zero query vector for a `cosine` field is refused before the read runs,
  under `distance` and `nearest` alike.
- A zero vector stored before the field became `cosine` drops its row from
  `distance` and `nearest`, as a null does, so the exact scan and the index
  agree.

`l2` and `dot` accept a zero vector, stored or queried.

## Changing a distance

Changing a field's distance is a schema change applied in place: no vector is
rewritten and no export or reload is needed. Until the index is rebuilt, the old
index was built for another distance, so it is not used: `distance` and
`nearest` answer from the exact scan, with the new distance's values. Run
[`index build` or `optimize`](../operations/maintenance.md#build-indexes) to
replace the index with one built for the new distance. Vectors already stored
are not checked again: a zero vector in a field that becomes `cosine` stays
stored and drops from the field's results, as described above.

A changed distance also changes scores. When the stored vectors and the query
both come from Omnigraph's embedding client, they are normalized, so `l2`,
`cosine` and `dot` rank them in the same order, but the values differ.
