# GQ before and after: edition 2 against edition 3, by kind of query

**Status:** evidence asset of the [GQ 3 RFC](../2026-11-07-gq-3.md) (2026-11-07).
Every kind of read and write the language serves, written once in edition 2
(what every graph runs today) and once in edition 3 (the draft). Where
edition 2 has no way to write the query, the left side says **Not
supported** and only the edition 3 form is shown. Where the two are the
same, the text is shown once and marked unchanged.

Edition 2 text is checked against today's `query.pest`. Edition 3 text is
the RFC's grammar as of its 2026-11-07 decision log; every edition 3 file
begins with `gq 3;`, omitted below.

## The schema, shared by every example

```gq
interface Named { name: String }

node Person: Named { handle: String @key  name: String  age: I32?  city: String? }
node Company: Named { slug: String @key  name: String }
node Doc { id: String @key  title: String  body: String @index(fulltext)  year: I32  tags: [String]  embedding: Vector(3)? }
node Collection { slug: String @key  min_docs: I32 }

edge WorksAt: Person -> Company @card(0..1)
edge Knows: Person -> Person
edge Authored: Person -> Doc
edge Cites: Doc -> Doc
edge In: Doc -> Collection        // traversed as "in": the bare name is reserved
```

Edition 2 declares `node Person implements Named`; edition 3 writes the
interface after a colon. Nothing else in the schema changes.

## 1. Lookups and filters

**Key lookup.** Unchanged.

```gq
query person($h: String) {
  match { $p: Person { handle: $h } }
  return { $p.name, $p.city }
}
```

**Comparison filter.** Unchanged.

```gq
query adults($min: I32) {
  match { $p: Person  $p.age >= $min }
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
  match { $d: Doc  contains($d.title, "graph")  starts_with($d.title, "The") }
  return { $d.title }
}
```

**Membership in a list literal.** Unchanged.

```gq
query in_cities() {
  match { $p: Person  $p.city in ["Berlin", "Paris"] }
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
  match { $d: Doc  "rust" in $d.tags }
  return { $d.title }
}
```

**Null test.** Unchanged.

```gq
query homeless() {
  match { $p: Person  $p.city is null }
  return { $p.name }
}
```

**Interface binding.** Unchanged: every implementor of `Named`.

```gq
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
  match { $p: Person  $p.age * 2 >= $min }
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

**One hop, directed, with the edge bound.** Unchanged.

```gq
query employer($h: String) {
  match { $p: Person { handle: $h }  $p $k:worksAt $c }
  return { $c.name, $k.@src, $k.@dst }
}
```

**Either orientation, alternation, bounded hops, any edge.** Unchanged.

```gq
query around($h: String) {
  match {
    $p: Person { handle: $h }
    $p <knows> $friend
    $p (knows | authored) $thing
    $p knows{2,2} $second
    $p *{1,3} $near
  }
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
      { match { $p: Person { handle: $from }  $p knows $q } return { $q } },
      { match { from reachable($from)  $q knows $r } return { $r as q } })
  }
  return distinct { $q }
}
```

**Edge whose name is a reserved word.** Unchanged: quoted in both editions.

```gq
query in_collection($slug: String) {
  match { $x: Collection { slug: $slug }  $d "in" $x }
  return { $d.title }
}
```

**Written against schema order.** Unchanged in meaning; edition 3's `fmt`
rewrites it to schema order.

```gq
query boss($slug: String) {
  match { $c: Company { slug: $slug }  $c worksAt $p }   // fmt: $p worksAt $c
  return { $p.name }
}
```

## 3. Aggregation

**Count per group.** Unchanged: the non-aggregated items are the keys.

```gq
query headcount() {
  match { $p worksAt $c }
  return { $c.name, count($p) as n }
  order { n desc }
}
```

**Distinct rows and distinct counts.** Unchanged.

```gq
query reach($h: String) {
  match { $p: Person { handle: $h }  $p knows{1,2} $q }
  return distinct { $q.name }
}

query distinct_cities() {
  match { $p: Person }
  return { count(distinct $p.city) as cities }
}
```

**Filter on an aggregate.** Edition 2: **Not supported.** No `having`, and a
filter cannot see a `return` column.

```gq
// edition 3
query big_employers() {
  match {
    from { match { $p worksAt $c } return { $c, count($p) as n } }
    $n >= 100
  }
  return { $c.name, $n }
  order { $n desc }
}
```

**Aggregate of an aggregate.** Edition 2: **Not supported.**

```gq
// edition 3
query average_headcount() {
  match { from { match { $p worksAt $c } return { $c, count($p) as n } } }
  return { avg($n) as mean, max($n) as largest }
}
```

**Count block as a predicate with a literal or parameter bound.** Unchanged.

```gq
query well_connected($min: I64) {
  match { $p: Person  count { $p knows $q } >= $min }
  return { $p.name }
}
```

**Count block compared with another binding's value.** Edition 2: **Not
supported** (`T40`: a block compares with a literal, `now()` or a
parameter).

```gq
// edition 3
query full_collections() {
  match { $x: Collection  count { $d "in" $x } >= $x.min_docs }
  return { $x.slug }
}
```

**A block as a value, computed once and reused.** Edition 2: **Not
supported.** A block is a predicate only; the count cannot be returned or
ordered on.

```gq
// edition 3
query degrees($min: I64) {
  match {
    $p: Person
    let $degree = count { $p knows $q }
    $degree >= $min
  }
  return { $p.name, $degree }
  order { $degree desc }
}
```

**Existence and absence.** Unchanged.

```gq
query authors_without_citations() {
  match {
    $p: Person
    exists { $p authored $d }
    not { $p authored $d2  $other cites $d2 }
  }
  return { $p.name }
}
```

## 4. Shapes of a result

**Outer join.** Edition 2: **Not supported.** A person without an employer
is dropped.

```gq
// edition 3
query people_and_employers() {
  match { $p: Person  optional { $p worksAt $c } }
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
      { match { $c: Company } return { $c.name as name, "company" as kind } })
  }
  return { $name, $kind }
}
```

**Reuse of a declared read with arguments.** Edition 2: **Not supported.**
Each stored query is self-contained.

```gq
// edition 3
query colleagues($of: String) {
  match { $p: Person { handle: $of }  $p worksAt $c  $q worksAt $c  $q != $p }
  return { $q }
}

query colleagues_of_colleagues($of: String) {
  match {
    from colleagues($of)
    from { from colleagues($q.handle) return { $q as r } }
  }
  return distinct { $r.name }
}
```

**Correlated subquery: the newest three documents per author.** Edition 2:
**Not supported.** A `limit` applies to the whole result, never per group.

```gq
// edition 3
query latest_per_author() {
  match {
    $p: Person
    from { match { $p authored $d } return { $d } order { $d.year desc } limit 3 }
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
  match { from nearest({ match { $d: Doc  $d.year >= 2025 } return { $d } }, embedding, $q, k: 20) }
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
  match { from nearest({ match { $d: Doc } return { $d } }, embedding, $q, k: 20) }
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
    from nearest({ match { $d "in" $x  $x: Collection { slug: $coll } } return { $d } }, embedding, $q, k: 20)
    $d cites $other
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
    from l2({ match { $d: Doc  $d.year >= 2025 } return { $d } }, embedding, $q)
    $distance <= $r
  }
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
  match { from bm25({ match { $d: Doc } return { $d } }, body, $q) }
  return { $d.title, $score as s }
  order { $score desc }
  limit 10
}
```

**Lexical filter without a score.** Unchanged: the predicates stay registry
calls; only the positional option moves.

```gq
// edition 2
query mentions($q: String) {
  match { $d: Doc  search($d.body, $q)  fuzzy($d.title, $q, 2) }
  return { $d.title }
}
```

```gq
// edition 3
query mentions($q: String) {
  match { $d: Doc  search($d.body, $q)  fuzzy($d.title, $q, max_edits: 2) }
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
    let $docs = { match { $d: Doc } return { $d } }
    from rrf(nearest($docs, embedding, $v, k: 50), bm25($docs, body, $q), k: 60)
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
    let $docs = { match { $d: Doc } return { $d } }
    from { from l2($docs, embedding, $v) return { $d, $distance as vec } }
    from { from bm25($docs, body, $q) return { $d as d2, $score as lex } }
    $d = $d2
  }
  return { $d.title, 0.7 * (1.0 / (1.0 + $vec)) + 0.3 * $lex as s }
  order { $s desc }
  limit 10
}
```

**Declared recall.** Edition 2: **Not supported** in the text; the probe
budget is a session setting.

```gq
// edition 3
query similar_fast($q: Vector(3)) {
  match { from nearest({ match { $d: Doc } return { $d } }, embedding, $q, k: 20, recall: 0.9) }
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
edition 3 binds the entity and names it.

```gq
// edition 2
query retire($h: String) as commit {
  delete WorksAt where @src = $h
  update Person set { city: null } where handle = $h
}
```

```gq
// edition 3
query retire($h: String) as commit {
  $p: Person { handle: $h }
  $p $k:worksAt $c
  delete $k
  update $p set { city: null }
}
```

**A write whose targets come from a traversal.** Edition 2: **Not
supported.** The agent reads the ids first and writes `in [ids]` on a second
snapshot.

```gq
// edition 3
query close_company($slug: String) {
  $c: Company { slug: $slug }
  $p $k:worksAt $c
  delete $k
  update $p set { city: null }
}
```

**A write with a computed value.** Edition 2: **Not supported.**

```gq
// edition 3
query birthday($h: String) {
  $p: Person { handle: $h }
  update $p set { age: $p.age + 1 }
}
```

**A write whose targets come from a ranked read.** Edition 2: **Not
supported.**

```gq
// edition 3
query tag_near($q: Vector(3)) {
  from nearest({ match { $d: Doc } return { $d } }, embedding, $q, k: 20)
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
unchanged; edition 3 admits any `@name(args)` and keeps unknown names as
metadata.

```gq
query people_over($cutoff: I32)
  @description("People over a given age")
  @instruction("Pass the age as an integer") {
  match { $p: Person  $p.age > $cutoff }
  return { $p.name }
}
```

**Added by later stages.** Edition 2: **Not supported** and not parseable.
Each is a parse error today and additive when its stage lands; `@if_commit`
alone is refused by name, so that it is never silently kept as metadata.

```gq
// edition 3, later stages
query schema_types() {
  match { $t: @Type  $p: @Property  $t @property $p }       // the schema catalogue
  return { $t.name, $p.name }
}

query retract($h: String, $c: Commit) @if_commit($c) {      // the writes RFC
  $p: Person { handle: $h }
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
and the `set` settings. Edition 3 adds the edition line `gq 3;` at the top
of a file and nothing else at this level.

## What the rewriter does to the edition 2 column

Every edition 2 example above that differs from its edition 3 form is one of
the seven rewriter rows: the infix string predicates, the list `contains`,
the positional `fuzzy` option, `nearest` and `bm25` out of `order` and
`return`, `rrf` out of `order`, and the `where` on `update` and `delete`.
`queries migrate` applies all seven; six are proved IR-equal and the `rrf`
row rows-equal. Every "Not supported" above is new text that
edition 2 never parsed, so it rewrites nothing.
