# GQ before and after: edition 2 against edition 3, by kind of query

**Status:** evidence asset of the [GQ 3 RFC](../2026-11-07-gq-3.md) (2026-11-07).
Every kind of read and write the language serves, written once in edition 2
(what every graph runs today) and once in edition 3 (the draft). Where
edition 2 has no way to write the query, the left side says **Not
supported** and only the edition 3 form is shown. Where the two are the
same, the text is shown once and marked unchanged.

Edition 2 text is checked against today's `query.pest`. Edition 3 text is
the RFC's grammar as of its 2026-11-11 decision log; edition 3 has no
edition line.

## The schema, shared by every example

```pg
interface Named { name: String }

node Person implements Named { handle: String @key  name: String  age: I32?  city: String? }
node Company implements Named { slug: String @key  name: String }
node Doc { id: String @key  title: String  body: String @index(fulltext)  year: I32  tags: [String]  embedding: Vector(3)? }
// `@index(fulltext, …)` is edition-3 schema syntax, additive; edition 2 spells a String's full-text index `@index`.
node Collection { slug: String @key  min_docs: I32 }

edge WorksAt: Person -> Company @card(0..1)
edge Knows: Person -> Person
edge Authored: Person -> Doc
edge Cites: Doc -> Doc
edge In: Doc -> Collection        // edition 2 quotes it, "in"; edition 3 writes in
```

The schema is `.pg` text and is the same in both editions; the `analyzer`
and `distance` options are edition 3's additive declarations.

## 1. Lookups and filters

**Key lookup.** Edition 3 has no property map: the key becomes an equality
in `where`.

```gq
// edition 2
query person($h: String) {
  match { $p: Person { handle: $h } }
  return { $p.name, $p.city }
}
```

```gq
// edition 3
query person($h: String) {
  match { $p: Person }
  where $p.handle = $h
  return { $p.name, $p.city }
}
```

**Comparison filter.** The comparison is the same; edition 3 moves it out
of `match` into `where`.

```gq
// edition 2
query adults($min: I32) {
  match { $p: Person  $p.age >= $min }
  return { $p.name }
  order { $p.age desc }
}
```

```gq
// edition 3
query adults($min: I32) {
  match { $p: Person }
  where $p.age >= $min
  return { $p.name }
  order { $p.age desc }
}
```

**String predicate.** Infix in edition 2, a call in edition 3.

```gq
// edition 2
query titled() {
  match { $d: Doc  $d.title contains "graph"  $d.title starts_with "The" }
  return { $d.title }
}
```

```gq
// edition 3
query titled() {
  match { $d: Doc }
  where contains($d.title, "graph") and starts_with($d.title, "The")
  return { $d.title }
}
```

**Membership in a list literal.** The `in` test is the same; edition 3
moves it to `where`.

```gq
// edition 2
query in_cities() {
  match { $p: Person  $p.city in ["Berlin", "Paris"] }
  return { $p.name }
}
```

```gq
// edition 3
query in_cities() {
  match { $p: Person }
  where $p.city in ["Berlin", "Paris"]
  return { $p.name }
}
```

**Membership in a list property.** Edition 2 overloaded `contains`; edition 3
uses `in`.

```gq
// edition 2
query tagged() {
  match { $d: Doc  $d.tags contains "rust" }
  return { $d.title }
}
```

```gq
// edition 3
query tagged() {
  match { $d: Doc }
  where "rust" in $d.tags
  return { $d.title }
}
```

**Null test.** The `is null` test is the same; edition 3 moves it to
`where`.

```gq
// edition 2
query homeless() {
  match { $p: Person  $p.city is null }
  return { $p.name }
}
```

```gq
// edition 3
query homeless() {
  match { $p: Person }
  where $p.city is null
  return { $p.name }
}
```

**Interface binding.** Edition 2: **Not supported** (`T1`, an interface is
not a node type), and edition 3 refuses it with the same code until stage
Poly, which adds the polymorphic `Scan`.

```gq
// edition 3, stage Poly
query names() {
  match { $n: Named }
  return { $n.name }
}
```

**Arithmetic.** Edition 2: **Not supported.** The expression language has no
operators.

```gq
// edition 3
query doubled($min: I32) {
  match { $p: Person }
  where $p.age * 2 >= $min
  return { $p.name, $p.age * 2 as twice, (2026 - $p.age) as born_about }
}
```

**Conditional and default values.** Edition 2: **Not supported.**

```gq
// edition 3
query with_city() {
  match { $p: Person }
  return { $p.name, coalesce($p.city, "unknown") as city, if($p.age >= 65, "senior", "active") as band }
}
```

## 2. Traversal

**One hop, directed, with the edge bound.** The triple is the same;
edition 3 separates it from the binding with a comma and moves the key
equality to `where`.

```gq
// edition 2
query employer($h: String) {
  match { $p: Person { handle: $h }  $p $k:worksAt $c }
  return { $c.name, $k.@src, $k.@dst }
}
```

```gq
// edition 3
query employer($h: String) {
  match { $p: Person, $p $k:worksAt $c }
  where $p.handle = $h
  return { $c.name, $k.@src, $k.@dst }
}
```

**Either orientation, alternation, bounded hops, any edge.** The triples
are the same; edition 3 separates them with commas and moves the key
equality to `where`.

```gq
// edition 2
query around($h: String) {
  match {
    $p: Person { handle: $h }
    $p <knows> $friend
    $p knows{2,2} $second
    $near: Person
    $p *{1,3} $near
  }
  return { $friend.name, count($second) as seconds, count($near) as near }
}
```

```gq
// edition 3
query around($h: String) {
  match {
    $p: Person,
    $p <knows> $friend,
    $p knows{2,2} $second,
    $near: Person,
    $p *{1,3} $near,
  }
  where $p.handle = $h
  return { $friend.name, count($second) as seconds, count($near) as near }
}
```

**Unbounded reachability.** Edition 2: **Not supported.** Every traversal
needs a bound.

```gq
// edition 3
query reachable($from: String) {
  match {
    from union(
      { match { $p: Person, $q: Person, $p knows $q } where $p.handle = $from return { $q } },
      { match { from reachable($from), $r: Person, $q knows $r } return { $r as q } }),
  }
  return distinct { $q }
}
```

**Edge whose name is a reserved word.** Edition 2 quotes it; edition 3
writes the bare name, since no expression is written inside `match`, and
moves the key equality to `where`.

```gq
// edition 2
query in_collection($slug: String) {
  match { $x: Collection { slug: $slug }  $d "in" $x }
  return { $d.title }
}
```

```gq
// edition 3
query in_collection($slug: String) {
  match { $x: Collection, $d in $x }
  where $x.slug = $slug
  return { $d.title }
}
```

**Written against schema order.** Edition 2 accepts it and reads the
schema's direction; edition 3 refuses it (`T82`) and prints the swapped
triple, so the written order is always the direction.

```gq
// edition 2
query boss($slug: String) {
  match { $c: Company { slug: $slug }  $c worksAt $p }   // fmt: $p worksAt $c
  return { $p.name }
}
```

```gq
// edition 3
query boss($slug: String) {
  match { $c: Company, $p worksAt $c }   // `$c worksAt $p` is T82
  where $c.slug = $slug
  return { $p.name }
}
```

## 3. Aggregation

**Count per group.** The non-aggregated items are the keys in both editions.
Edition 2 needs an end of the triple typed (`T5`); edition 3 starts an
unanchored triple from the edge's declared source type.

```gq
// edition 2
query headcount() {
  match { $c: Company  $p worksAt $c }
  return { $c.name, count($p) as n }
  order { n desc }
}
```

```gq
// edition 3
query headcount() {
  match { $p worksAt $c }
  return { $c.name, count($p) as n }
  order { n desc }
}
```

**Distinct rows and distinct counts.** `return distinct` and
`count(distinct …)` are the same; edition 3 moves the key equality to
`where`.

```gq
// edition 2
query reach($h: String) {
  match { $p: Person { handle: $h }  $p knows{1,2} $q }
  return distinct { $q.name }
}

query distinct_cities() {
  match { $p: Person }
  return { count(distinct $p.city) as cities }
}
```

```gq
// edition 3
query reach($h: String) {
  match { $p: Person, $p knows{1,2} $q }
  where $p.handle = $h
  return distinct { $q.name }
}

query distinct_cities() {
  match { $p: Person }
  return { count(distinct $p.city) as cities }
}
```

**Filter on an aggregate.** Edition 2 writes a literal or parameter bound
with a count block, `$c: Company  count { $p worksAt $c } >= 100`, and
edition 3 keeps the block as an expression in `where`, `where count { $p:
Person, $p worksAt $c } >= 100`. New in edition 3 is a threshold over a
grouped column, which a count block cannot express: no `having`, and a
filter cannot see a `return` column.

```gq
// edition 3
query big_employers() {
  match { from { match { $p: Person, $c: Company, $p worksAt $c } return { $c, count($p) as n } } }
  where $n >= 100
  return { $c.name, $n }
  order { $n desc }
}
```

**Aggregate of an aggregate.** Edition 2: **Not supported.** The block form
keeps a company with no employees at zero; the grouped form (`from { match {
$p worksAt $c } return { $c, count($p) as n } }`) drops it. They are
different terms, not two spellings of one.

```gq
// edition 3
query average_headcount() {
  match { $c: Company, let $n = count { $p: Person, $p worksAt $c } }
  return { avg($n) as mean, max($n) as largest }
}
```

**Count block as a predicate with a literal or parameter bound.** The block
is the same; edition 3 writes it as an expression in `where`.

```gq
// edition 2
query well_connected($min: I64) {
  match { $p: Person  count { $p knows $q } >= $min }
  return { $p.name }
}
```

```gq
// edition 3
query well_connected($min: I64) {
  match { $p: Person }
  where count { $q: Person, $p knows $q } >= $min
  return { $p.name }
}
```

**Count block compared with another binding's value.** Edition 2: **Not
supported** (`T40`: a block compares with a literal, `now()` or a
parameter).

```gq
// edition 3
query full_collections() {
  match { $x: Collection }
  where count { $d: Doc, $d in $x } >= $x.min_docs
  return { $x.slug }
}
```

**A block as a value, computed once and reused.** Edition 2: **Not
supported.** A block is a predicate only; the count cannot be returned or
ordered on.

```gq
// edition 3
query degrees($min: I64) {
  match { $p: Person, let $degree = count { $q: Person, $p knows $q } }
  where $degree >= $min
  return { $p.name, $degree }
  order { $degree desc }
}
```

**Existence and absence.** Edition 3 writes both as expressions in
`where`, and absence as `not exists { }`.

```gq
// edition 2
query authors_without_citations() {
  match {
    $p: Person
    exists { $p authored $d }
    not { $p authored $d2  $other cites $d2 }
  }
  return { $p.name }
}
```

```gq
// edition 3
query authors_without_citations() {
  match { $p: Person }
  where exists { $d: Doc, $p authored $d }
    and not exists { $d2: Doc, $other: Doc, $p authored $d2, $other cites $d2 }
  return { $p.name }
}
```

## 4. Shapes of a result

**Outer join.** Edition 2: **Not supported.** A person without an employer
is dropped.

```gq
// edition 3
query people_and_employers() {
  match { $p: Person, optional { $c: Company, $p worksAt $c } }
  return { $p.name, $c.name }
}
```

**Union of two reads.** Edition 2: **Not supported.**

```gq
// edition 3
query people_and_companies() {
  match {
    from union(
      { match { $p: Person } return { $p.name as name, "person" as kind } },
      { match { $c: Company } return { $c.name as name, "company" as kind } }),
  }
  return { $name, $kind }
}
```

**Reuse of a declared read with arguments.** Edition 2: **Not supported.**
Each stored query is self-contained.

```gq
// edition 3
query colleagues($of: String) {
  match { $p: Person, $p worksAt $c, $q worksAt $c }
  where $p.handle = $of and $q != $p
  return { $q }
}

query colleagues_of_colleagues($of: String) {
  match {
    from { match { from colleagues($of) } return { $q.handle as h1 } },   // the call's $q enters scope; a second call needs it renamed (T68)
    from colleagues($h1),
  }
  return distinct { $q.name }
}
```

**Correlated subquery: the newest three documents per author.** Edition 2:
**Not supported.** A `limit` applies to the whole result, never per group.

```gq
// edition 3
query latest_per_author() {
  match {
    $p: Person,
    from { match { $d: Doc, $p authored $d } return { $d } order { $d.year desc } limit 3 },
  }
  return { $p.name, $d.title, $d.year }
}
```

**Parameterised limit and a default.** Edition 2: **Not supported.** `limit`
takes an integer literal and a parameter has no default.

```gq
// edition 3
query top_people($k: I64 = 10) {
  match { $p: Person }
  return { $p.name }
  order { $p.age desc }
  limit $k
}
```

## 5. Ranking and search

**Nearest window.** The ranking moves out of `order` into `from`; the cut
becomes `k`.

```gq
// edition 2
query similar($q: Vector(3)) {
  match { $d: Doc  $d.year >= 2025 }
  return { $d.title }
  order { nearest($d.embedding, $q) }
  limit 20
}
```

```gq
// edition 3
query similar($q: Vector(3)) {
  match {
    let $pop = { match { $d: Doc } where $d.year >= 2025 return { $d } },
    from nearest($pop, $d.embedding, $q, k: 20) as $distance,
  }
  return { $d.title }
  order { $distance }
}
```

**Nearest with the distance returned.** The score is a column, not a call.

```gq
// edition 2
query similar_scored($q: Vector(3)) {
  match { $d: Doc }
  return { $d.title, nearest($d.embedding, $q) as s }
  order { nearest($d.embedding, $q) }
  limit 20
}
```

```gq
// edition 3
query similar_scored($q: Vector(3)) {
  match {
    let $docs = { match { $d: Doc } return { $d } },
    from nearest($docs, $d.embedding, $q, k: 20) as $distance,
  }
  return { $d.title, $distance as s }
  order { $distance }
}
```

**A window over documents, then a traversal from the hits.** Edition 2:
**Not supported** as a window over documents: with the traversal in the
same `match`, the `limit` cuts the joined document-citation rows, not the
documents.

```gq
// edition 3
query cited_near($coll: String, $q: Vector(3)) {
  match {
    let $pop = { match { $d: Doc, $x: Collection, $d in $x } where $x.slug = $coll return { $d } },
    from nearest($pop, $d.embedding, $q, k: 20) as $distance,
    $d cites $other,
  }
  return { $d.title, $distance, count($other) as cites }
  order { $distance }
}
```

**Every document within a distance.** Edition 2: **Not supported.** The
distance exists only as a ranking; no filter can read it, and the window
form answers a different question.

```gq
// edition 3
query within_radius($q: Vector(3), $r: F32) {
  match {
    let $pop = { match { $d: Doc } where $d.year >= 2025 return { $d } },
    from distance($pop, $d.embedding, $q) as $distance,
  }
  where $distance <= $r
  return { $d.title, $distance }
  order { $distance, $d.id }
  limit 10
}
```

**Lexical ranking.**

```gq
// edition 2
query about($q: String) {
  match { $d: Doc }
  return { $d.title, bm25($d.body, $q) as s }
  order { bm25($d.body, $q) desc }
  limit 10
}
```

```gq
// edition 3
query about($q: String) {
  match {
    let $docs = { match { $d: Doc } return { $d } },
    from bm25($docs, $d.body, $q) as $score,
  }
  return { $d.title, $score as s }
  order { $score desc }
  limit 10
}
```

**Lexical filter without a score.** The predicates stay registry calls;
edition 3 moves them to `where` and names the positional option.

```gq
// edition 2
query mentions($q: String) {
  match { $d: Doc  search($d.body, $q)  fuzzy($d.body, $q, 2) }   // over the indexed property; `title` has no index
  return { $d.title }
}
```

```gq
// edition 3
query mentions($q: String) {
  match { $d: Doc }
  where search($d.body, $q) and fuzzy($d.body, $q, max_edits: 2)
  return { $d.title }
}
```

**Hybrid, fused.** Both arms over one population; the population is named
once.

```gq
// edition 2
query hybrid($q: String, $v: Vector(3)) {
  match { $d: Doc }
  return { $d.title }
  order { rrf(nearest($d.embedding, $v), bm25($d.body, $q), 60) }
  limit 10
}
```

```gq
// edition 3
query hybrid($q: String, $v: Vector(3)) {
  match {
    let $docs = { match { $d: Doc } return { $d } },
    from rrf(nearest($docs, $d.embedding, $v, k: 50), bm25($docs, $d.body, $q), k: 60) as $score,
  }
  return { $d.title, $score }
  order { $score desc }
  limit 10
}
```

**Hybrid, weighted.** Edition 2: **Not supported.** Scores are not values
and there is no arithmetic.

```gq
// edition 3
query weighted($q: String, $v: Vector(3)) {
  match {
    let $docs = { match { $d: Doc } return { $d } },
    let $docs2 = { match { from $docs } return { $d as d2 } },              // the second arm's column renamed before it is ranked (T68)
    from distance($docs, $d.embedding, $v) as $vec,
    from bm25($docs2, $d2.body, $q) as $lex,
  }
  where $d = $d2
  return { $d.title, 0.7 * (1.0 / (1.0 + $vec)) + 0.3 * $lex as s }
  order { s desc }
  limit 10
}
```

**Declared recall.** Edition 2: **Not supported** in the text; the probe
budget is a session setting.

```gq
// edition 3
query similar_fast($q: Vector(3)) {
  match {
    let $docs = { match { $d: Doc } return { $d } },
    from nearest($docs, $d.embedding, $q, k: 20, recall: 0.9) as $distance,
  }
  return { $d.title, $distance }
}
```

## 6. Writes

Both editions declare a write with `query`; the body's shape makes it one.

**Insert a node and a keyed edge.** Unchanged.

```gq
query hire($h: String, $slug: String) {
  insert Person { handle: $h, name: $h }
  insert WorksAt { from: $h, to: $slug }
}
```

**Update and delete by predicate.** Edition 2 names the type and a `where`;
edition 3 binds the entity in a `match`, filters it in that `match`'s
`where`, and names it. Neither edition mixes a `delete` with an `update` in
one write, and edition 2 cannot assign `null`, so clearing a property is
edition 3 only.

```gq
// edition 2
query retire($h: String) as commit {
  delete WorksAt where @src = $h
}

query relocate($h: String) {
  update Person set { city: "unknown" } where handle = $h
}
```

```gq
// edition 3
query retire($h: String) as commit {
  match { $p: Person, $p $k:worksAt $c } where $p.handle = $h
  delete $k
}

query relocate($h: String) {
  match { $p: Person } where $p.handle = $h
  update $p set { city: null }
}
```

**A write whose targets come from a traversal.** Edition 2 reaches an edge
by its own endpoint, `delete WorksAt where @dst = $slug`; a selection that
needs a neighbour's property is **Not supported** there, and the agent reads
the ids first and writes `in [ids]` on a second snapshot.

```gq
// edition 3
query close_office($slug: String, $city: String) {
  match { $c: Company, $p: Person, $p $k:worksAt $c }
  where $c.slug = $slug and $p.city = $city
  delete $k
}
```

**A write with a computed value.** Edition 2: **Not supported.**

```gq
// edition 3
query birthday($h: String) {
  match { $p: Person } where $p.handle = $h
  update $p set { age: $p.age + 1 }
}
```

**A write whose targets come from a ranked read.** Edition 2: **Not
supported.**

```gq
// edition 3
query tag_near($q: Vector(3)) {
  match {
    let $docs = { match { $d: Doc } return { $d } },
    from nearest($docs, $d.embedding, $q, k: 20) as $distance,
  }
  update $d set { tags: ["near"] }
}
```

**An unpublished proposal or an unattached snapshot.** Unchanged.

```gq
query propose($h: String) as unattached branch {
  insert Person { handle: $h, name: $h }
}
```

## 7. Catalogue and declarations

**The engine catalogue.** Unchanged: the five `@` types exist in both
editions.

```gq
query operators() {
  match { $o: @Operator }
  return { $o.name, $o.inputs }
}
```

**Annotations on a declaration.** `@description` and `@instruction` are
unchanged; edition 3 admits `@name(args)` for the annotations it reads and
the metadata allow-list, and refuses any other name (`T80`). The body's
filter moves to `where`.

```gq
// edition 2
query people_over($cutoff: I32)
  @description("People over a given age")
  @instruction("Pass the age as an integer") {
  match { $p: Person  $p.age > $cutoff }
  return { $p.name }
}
```

```gq
// edition 3
query people_over($cutoff: I32)
  @description("People over a given age")
  @instruction("Pass the age as an integer") {
  match { $p: Person }
  where $p.age > $cutoff
  return { $p.name }
}
```

**Added by later stages.** Edition 2: **Not supported** and not parseable.
Each is a parse error today and additive when its stage lands; `@if_commit`
alone is refused by name, so that it is never silently kept as metadata.

```gq
// edition 3, later stages
query schema_types() {
  match { $t: @Type, $p: @Property, $t @property $p }       // the schema catalogue
  return { $t.name, $p.name }
}

query retract($h: String, $c: Commit) @if_commit($c) {      // the writes RFC
  match { $p: Person } where $p.handle = $h
  delete $p
  returning { @id }
}

query bounded() @budget(rows = 1000, on_exceed = refuse) {     // the agent-interaction stage
  match { $p: Person }
  return { $p.handle }
}

query namespaced() {
  match { $o: crm::Person }                                 // the schema namespaces RFC
  return { $o.name }
}
```

## 8. Statements outside a declaration

Unchanged in both editions: `show`, `branch`, `commit`, `load`, `explain`,
and the `set` settings. Edition 3 adds nothing at this level and has no
edition line.

## What a rewrite does to the edition 2 column

Every difference between the columns is a row of the respelling map. Most
examples differ only by the respelling rows: commas between elements, items
and assignments; filters and property maps moved into one `where`; `not { }`
written `not exists { }`; a block's filters in its own `where`; a write's
selection in a `match`; and the bare name of a reserved edge. The others are
the infix string predicates, the list `contains`, the positional `fuzzy`
option, `nearest` and `bm25` out of `order` and `return`, `rrf` out of
`order`, and the `where` on `update` and `delete`. No rewriter is built and
no shipped command converts text: agents apply the rows to this repository's
text in the break, and a stored source in the old syntax fails with a
diagnostic whose fix applies the row. The GQT corpus keeping its rows,
shapes and plans is the proof. Every "Not supported" above is new text that
edition 2 never parsed, so it rewrites nothing.
