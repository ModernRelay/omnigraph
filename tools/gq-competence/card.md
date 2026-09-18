# GQ in one page (OmniGraph 0.11)

A `.gq` source holds named, typed read queries. You run one query at a time.

```gq
query people_at($org: String) {
  match {
    $p: Person                   // bind every node of a type
    $p belongsTo $o              // follow a directed edge (declared BelongsTo)
    $o: Organization { slug: $org }   // inline equality constraints on a binding
    $p.relation != "family"      // scalar filter; operators = != < <= > >= starts_with contains
    not { $p livesIn $_ }        // keep rows where the inner pattern has no match
  }
  return { $p.slug, $p.name as name, count($o) as orgs }   // aggregates group by the other columns
  order { orgs desc, $p.slug asc }                          // total order; ids break ties
  limit 20                                                  // integer literal only
}
```

Rules that differ from SQL and Cypher:

- Blocks are `match { }` then `return { }`, then optional `order { }` and `limit N`. There is no `WHERE`, `WITH`, `GROUP BY`, `DISTINCT`, `UNION`, `OPTIONAL MATCH`, `UNWIND`, `CASE` or arithmetic. Filters go inside `match { }` as their own lines.
- Parameters: `$name: Type` in the signature, passed as JSON. Types: `String Bool I32 I64 U32 U64 F32 F64 Date DateTime Blob`, `T?` nullable, `[T]` list, `Vector(N)`. There is no `Int`; use `I64`. Never inline a value you could pass as a parameter.
- Variables start with `$`. Node types are `UpperCamel`. An edge declared `BelongsTo` is traversed as `belongsTo` (lowercase first letter). `$_` is an anonymous node, distinct each time it appears.
- Node identity is `$p.@id`; `$e.@src` / `$e.@dst` are a bound edge's endpoints. A bare `id` is a user property and usually does not exist.
- Traversal forms: `$a knows $b` (directed), `$a <knows> $b` (either direction, same node type both ends), `$a knows{1,3} $b` (1–3 hops, shortest-path distance, finite bounds required), `$a $e:knows $b` (bind the edge to read `$e.property`).
- Filters compare one expression to one expression: `$t.status = "next"`, `$e.date >= datetime("2026-01-01T00:00:00Z")`, `$x.tags contains "rust"`, `$x.name starts_with "Om"`. String comparisons are exact and case-sensitive. Literals: `"text"`, `42`, `1.5`, `true`, `date("2026-01-01")`, `datetime("2026-01-01T00:00:00Z")`, `[…]`, `now()`.
- `return { }` projects properties, parameters, literals, and aggregates `count sum avg min max`; `count($p)` counts rows. Any non-aggregate projection beside an aggregate becomes a group key. Alias with `as name`; every result column name must be distinct. `return { $p }` returns the node as one object.
- `order { }` keys are properties or aliases with `asc`/`desc` (default asc). `limit` takes only an integer literal.
- Full-text: `search($n.content, $q)` (token match), `fuzzy($n.content, $q, 1)` (edit tolerance), `match_text(...)` are predicates inside `match`. Ranking goes in `order`: `order { bm25($n.content, $q) desc } limit 10`, `order { nearest($t.embedding, $q) } limit 10` (vector; `$q` may be text), `order { rrf(nearest(...), bm25(...)) } limit 10`. `nearest` and `rrf` require `limit`. Exact substring is `contains`, not search.
- Comments are `//`. Enum values are plain strings: `{ status: "next" }`.

Reading results: JSON `rows` is a list of objects keyed by column name (`p.slug`, or your alias); a null cell's key is omitted. `graph_commit_id` identifies the snapshot the rows came from.

Errors name a code and a position (`Q000` parse error, `T…` type errors). Fix the named construct; do not retry the same text.
