# GQ reference card (edition 3)

## The file and the declaration

- A file begins with the edition line `gq 3;`, then one or more declarations.
- Every declaration is a `query`, read or write. There is no `mutation`
  keyword: the body's shape says which it is. An empty parameter list still
  needs parentheses: `query all() { … }`.
- **Parameters**: `$name: Type`, optionally with a default, `$k: I64 = 10`.
  Types are `String Bool I32 I64 U32 U64 F32 F64 Date DateTime Blob`, `T?`
  (nullable), `[T]` (list) and `Vector(N)`. There is no `Int`; use `I64`.
- **Annotations** follow the parameter list: `@description("…")`,
  `@instruction("…")`. Any other annotation name is refused.
- **Comments** are `//` and `/* */`, never `#`.
- **Casing**: types are uppercase-initial (`Person`); an edge declared
  `WorksAt` is traversed lowerCamelCase (`worksAt`); variables start with `$`.
  An edge whose lowerCamelCase name is a reserved word (`in`, `is`, `null`,
  `let`, `from`, `true`, `false`, `insert`, `update`, `delete`) is written
  quoted: `$d "in" $x`.

## A read

```
query name($p: T) {
  match { <clauses> }
  return { <items> }          // or: return distinct { <items> }
  order { <keys> }            // optional; `desc` per key, ascending by default
  limit 20                    // optional; an integer or a parameter: limit $k
}
```

Clauses in `match` are written one after another with no separator. Each
clause is one of:

| Clause | Meaning |
|---|---|
| `$p: Person` | bind every node of a type |
| `$p: Person { handle: $h }` | the same, keeping rows whose properties equal the values; braces hold equalities only |
| `$k: WorksAt` | bind every edge of an edge type (`$k.@src`, `$k.@dst`, `$k.@id`) |
| `$p worksAt $c` | a triple: one hop along an edge, in the schema's direction; neither end needs to be bound first |
| `$p $k:worksAt $c` | the triple, binding the edge as `$k`: one row per edge |
| `$p <knows> $f` | either direction (same node type at both ends only) |
| `$p (knows \| authored) $x`, `$p *{1,3} $x` | any listed edge type; any edge type |
| `$p knows{1,3} $f` | 1 to 3 hops, shortest distance; bounds are finite, and above one hop need the same node type at both ends |
| a Boolean expression | a filter: keeps rows where it is true (null is not true) |
| `exists { … }`, `not { … }` | a pattern matches at least once / never; it correlates on the variables bound before it that it mentions |
| `optional { … }` | an outer join: keeps the row, with nulls, when the pattern does not match |
| `let $v = <expression>` | a named value computed per row; adds and removes no rows |
| `let $r = { <read> }` | a name for a relation, to pass to a ranking or draw from twice |
| `from <relation>` | joins the relation's rows in: one output row per inner row |

Comparisons are filter clauses, never inside braces: write
`$d: Doc  $d.year >= 2025`, not `$d: Doc { year >= 2025 }`.

Order matters only for `let`, `from` and the blocks: each sees the variables
bound in the text before it, and nothing after it.

## Expressions

- Comparison `= != < <= > >=` (they do not chain: `$a < $b and $b < $c`),
  logic `and or not` with parentheses, `is null` / `is not null`.
- Membership: `$p.city in ["Berlin", "Paris"]`, `$x.slug in $list_param`,
  and `"rust" in $d.tags` for a list property.
- Arithmetic: `+ - * /` and `%`, unary `-`, the usual precedence:
  `$p.age * 2 >= $min`, `2026 - $p.age`. A binary minus has spaces on both
  sides or on neither: `$n - 1`, never `$n -1`. Integer overflow and a zero
  divisor are errors, never null. A float literal needs a fraction:
  `1.5e3`, never `1e3`.
- Calls: `name(arg, …, option: value)`; the name touches its `(`. String
  predicates are calls: `contains($d.title, "graph")`,
  `starts_with($d.title, "The")` (exact, case-sensitive). Conditional and
  default values: `if($p.age >= 65, "senior", "active")`,
  `coalesce($p.city, "unknown")`, `concat($a, $b)`. Literals and calls:
  `now()`, `date("2026-04-29")`, `datetime("…")`, `[…]`.
- There is no `CASE`, no `||`, no `null` literal in a read, and a bare word
  is never a value: every value is a literal, a `$` name, a property or a
  call.
- Meta-fields: `$p.@id`, `$k.@src`, `$k.@dst`, `$k.@type`.

## Aggregates and blocks

- `return { $c.name, count($p) as n }`: the non-aggregated items are the
  group keys. `count sum avg min max`, `count(distinct $p.city)`.
  `return distinct { … }` keeps one row per distinct tuple.
- A block is a value computed per row: `count { $p knows $q }`,
  `sum($d.year) { $p authored $d }`, `min`, `max`, `avg`, `exists { … }`.
  Use it in a filter and compare it with anything in scope
  (`count { $d "in" $x } >= $x.min_docs`), or name it with `let` to return
  and order by it: `let $degree = count { $p knows $q }`.
- **Grouping drops empty groups; a block does not.** For "each company and
  its number of employees", `$c: Company  let $n = count { $p worksAt $c }`
  keeps a company with no employees at 0, while
  `return { $c.name, count($p) as n }` over `$p worksAt $c` leaves it out.
  Choose by whether the empty groups belong in the answer.
- Alias with `as`; result column names must be distinct. `order` keys may be
  expressions, `$` variables, or bare `return` aliases (`order { n desc }`).

## Relations: `from` and `let`

A relation is one of:

| Form | Adds |
|---|---|
| `{ match { … } return { … } order { … } limit n }` | an inline read; its `return` items are its columns |
| `name(args)` | a call of a declared `query` of the same file, inlined; scalar arguments |
| `$r` | a relation named by `let $r = { … }` |
| `nearest(rel, $d.embedding, $q, k: 20)` | the `k` nearest targets, approximate; adds `$distance`; `k` is required; `recall: 0.9` declares the accepted recall |
| `distance(rel, $d.embedding, $q)` | every row with its exact distance; adds `$distance`; scans the whole population |
| `bm25(rel, $d.body, $q)` | every text match with its exact BM25 score; adds `$score` |
| `rrf(rel1, rel2, …, k: 60, weights: [w1, w2])` | reciprocal rank fusion of 2 to 16 ranked relations over one node column; yields that column and `$score` only |
| `union(rel1, rel2, …)` | a bag union; columns agree by position, names from the first |

`from rel` joins every row of the relation into the current rows. Its
columns enter scope under their own names: a column written `$c` arrives as
`$c`, and `count($p) as n` arrives as `$n`. So every column of a relation you
draw from is a bare variable or has an alias.

- **A name already in scope is an error (`T68`).** Rename the column with
  `as` in the inner `return`. The same holds inside an inline read: a
  binding in it, including the columns of a `from` in it, may not reuse a
  name the outer scope holds.
- **An inline read may read the variables written before it**, which
  correlates it: its `order` and `limit` then apply per outer row, and an
  outer filter never widens its `limit`.
- **A helper called twice**: the first call's columns would collide with the
  second's, so wrap the first and rename its column:
  `from { match { from colleagues($of) } return { $q.handle as h } }  from colleagues($h)`.

### Ranking and search

- A ranking is always a relation in `from`: never write `nearest`, `bm25`,
  `distance` or `rrf` in `order` or `return` (`T65`). Order by the column it
  adds: `order { $distance }`, `order { $score desc }`.
- The first argument is the **population**: the rows eligible to be ranked.
  The second names the ranked column as a property of one of its node
  columns (`$d.embedding`, `$d.body`). The population's other columns ride
  along.
- **Use `nearest` for "the `k` most similar".** Use `distance` only when the
  task asks for an exact answer or for every row within a radius
  (`from distance(…)` then `$distance <= $r`), since it scans every row of
  the population.
- **Window versus population.** A filter inside the population restricts who
  is eligible before ranking; a clause after `from nearest(…)` filters the
  `k` selected rows afterwards, and nothing refills them. `k` is the window;
  the outer `limit` never sizes it.
- `bm25(rel, $d.body, $q)` with a String `$q` matches any of its terms. It
  has no window: for the top `n`, wrap it in an inline read with
  `order { $score desc }  limit n`.
- Lexical predicates without a score stay calls in `match`:
  `search($d.body, $q)`, `match_text($d.body, $q)`,
  `fuzzy($d.body, $q, max_edits: 2)`; the general form is
  `match_terms($d.body, terms($q, mode: any, max_edits: 1))`.
- Fusion: name the population once, then fuse two rankings of it:
  `let $docs = { match { $d: Doc } return { $d } }` and
  `from rrf(nearest($docs, $d.embedding, $v, k: 50), bm25($docs, $d.body, $q), k: 60)`.
  The fused rows carry `$d` and `$score` only; draw anything else again
  after it. To combine scores yourself, draw each ranking through a wrapper
  read that renames its columns, then join them with `$d = $d2`.

### Reuse and recursion

Several declarations may share a file; one may call another in `from`:
`from colleagues($p.handle)`. A declared read may call itself through a
`union` of a base case and a step, passing its parameters through
unchanged; this computes the unbounded reachable set. Its `return` must be
`distinct` (or `min`/`max`), and its columns must be variables, so project
names in a second declaration:

```
query reachable($from: String) {
  match {
    from union(
      { match { $p: Person { handle: $from }  $p knows $q } return { $q } },
      { match { from reachable($from)  $q knows $r } return { $r as q } })
  }
  return distinct { $q }
}
```

## A write

A write body has no `match { }` and no `return`: it is clauses written bare,
exactly a read's clauses, followed by one or more statements. Each statement
runs once per row the clauses produce, in order, each seeing the ones before
it; the write commits once.

| Statement | Meaning |
|---|---|
| `insert Person { handle: $h, name: $h }` | a keyed insert (upsert); with no clauses before it, it runs once |
| `insert WorksAt { from: $h, to: $c }` | an edge; each endpoint is a key value or a bound node |
| `update $p set { city: null, age: $p.age + 1 }` | assign properties of a bound node; `null` clears one |
| `delete $k` | delete the bound node or edge |

- There is no `where` on a statement: the clauses select the rows.
  `delete WorksAt where @src = $h` is refused; write
  `$p: Person { handle: $h }  $p $k:worksAt $c  delete $k`.
- Every non-nullable property must be given on insert. An `update` cannot
  assign a `@key` property; edges cannot be updated.
- One write is constructive (insert, update) **or** destructive (delete),
  never both; split them into two queries.
- After the parameter list, `as commit` writes an unpublished proposal
  commit and `as unattached branch` writes an unattached branch snapshot.

## Refused forms

| Written | Write instead |
|---|---|
| `$d.title contains "x"`, `$d.title starts_with "T"` | `contains($d.title, "x")`, `starts_with($d.title, "T")` |
| `$d.tags contains "rust"` | `"rust" in $d.tags` |
| `fuzzy($d.body, $q, 2)` | `fuzzy($d.body, $q, max_edits: 2)` |
| `order { nearest($d.embedding, $q) }` | `from nearest(…, k: n)` and `order { $distance }` |
| `return { bm25($d.body, $q) as s }` | `from bm25(…)` and `$score as s` |
| `update T set { … } where …`, `delete T where …` | bind, filter, then `update $x` / `delete $x` |
| `mutation name(…)`, a write body in `match { }` | `query name(…)`, clauses written bare |
| `having`, `where`, `with`, `call`, `unwind`, `group by`, `offset` | a filter after `from { … }`; a filter clause; `from` |
| `CASE … END`, `a \|\| b` | `if(c, a, b)`, `concat(a, b)` |
| `$n -1`, `colleagues ($h)`, `$a < $b < $c` | `$n - 1`, `colleagues($h)`, two comparisons |

## Also

- The engine catalogue is queryable: `$o: @Operator` (fields `name: String`,
  `inputs: [I64]`), and likewise `@Pass`, `@Setting`, `@Engine`,
  `@LogicalKind`.

## Three worked examples

**A grouped result, filtered.** The inner read groups; the outer filters its
rows like any others.

```gq
gq 3;
query big_employers() {
  match {
    from { match { $p worksAt $c } return { $c, count($p) as headcount } }
    $headcount >= 100
  }
  return { $c.name, $headcount }
  order { $headcount desc }
}
```

**A ranked window scoped by the graph, then a traversal from the hits.** The
population is the documents of one collection; `k: 20` is the window; the
traversal and the count run on those twenty rows.

```gq
gq 3;
query cited_near($coll: String, $q: Vector(3)) {
  match {
    from nearest(
      { match { $d "in" $x  $x: Collection { slug: $coll } } return { $d } },
      $d.embedding, $q, k: 20
    )
    $d cites $other
  }
  return { $d.title, $distance, count($other) as cites }
  order { $distance }
}
```

**A write fed by a read.** The clauses choose the rows; each statement names
the bound entity it writes. A delete and an update are two declarations.

```gq
gq 3;
query retire($h: String) as commit {
  $p: Person { handle: $h }
  $p $k:worksAt $c
  delete $k
}

query relocate($h: String) {
  $p: Person { handle: $h }
  update $p set { city: null }
}
```
