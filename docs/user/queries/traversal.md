# Traversal and match patterns

Inside `match { ... }` the elements are separated by commas; after the
braces, `where` holds one Boolean expression:

| Pattern | Meaning |
|---|---|
| `$p: Person` | Bind nodes of a type. |
| `$person worksAt $company` | Follow a directed edge, written in its declared order, source first. |
| `$a knows{1,3} $b` | Follow a path from one to three hops. |
| `$a <related> $b` | Match the edge in either direction. The edge must connect the same node type at both ends. |
| `$a $rel:related $b` | Bind a single-hop edge instance so its properties can be used. |
| `$a (knows \| likes) $b` | Follow one hop across either concrete edge type. |
| `$a *{1,3} $b` | Follow one through three hops across compatible edge types; declare both endpoint node types. |
| `$a <knows \| likes> $b` or `$a <*> $b` | Follow the selected edges in either direction. |
| `$a $e:(knows \| likes) $b` or `$a $e:* $b` | Bind each concrete selected edge. |
| `where $p.age >= 18` | Apply a filter expression. |
| `where not exists { $o: Person, $p blocked $o }` | Keep rows for which the inner pattern has no match. |
| `where exists { $o: Person, $p blocked $o }` | Keep rows for which the inner pattern has at least one match. |
| `where count { $p: Post, $a authored $p } > 2` | Keep rows whose inner pattern matches more than two times. |
| `where sum($d.size) { $d: File, $p owns $d } > 100` | Aggregate a property over the inner matches, then compare. |

```gq
query colleagues($h: String) {
  match { $p: Person, $p worksAt $c, $q worksAt $c }
  where $p.handle = $h and $q != $p and not exists { $q blocked $p }
  return { $q.name }
}
```

The elements are bindings `$x: Type`, triples, `optional { ... }`, `let` and
`from`, separated by commas with a trailing comma allowed; a newline is not a
separator. `where` sees every variable the elements bind and is evaluated
after all of them, so a `let`, a `from` or an `optional` whose expression can
fail runs on every row the elements before it produce, rows `where` then
removes included; guard it with `if(...)` or compute it in `return`. `and` and `or` guard left to right: `where`'s conjuncts filter in
written order, each on the rows the ones before it keep, so `$n != 0 and
$t / $n > 1` never divides by zero. A block such as `exists { ... }` is a
conjunct like any other: written first, it guards the conjuncts after it.
Under a guard (the later operand of `and` or `or`, an `if` branch, a later
`coalesce` argument), a block runs only on the rows that guard selects, its
own `where`, a `let` inside it and its aggregate's argument included. A filter, a `where` or a
comparison inside the braces is refused (`Q017`), and so is a missing comma
(`Q016`); a property map `$p: Person { handle: $h }` is refused (`Q010`) with
the binding and the conjunct `$p.handle = $h`, and `not { ... }` with
`not exists { ... }` (`Q012`). Each refusal prints the corrected text.

Omitted bounds always mean exactly `{1,1}`. Explicit `{1,1}` is equivalent.
Multiple hops require a finite `{min,max}` with `1 <= min <= max`; `*` selects
edge types and never means repetition. Duplicate alternatives select a type once.

All members must connect the same source and destination node types, with
direction resolved separately for each member. Wildcards require explicit node
declarations for both endpoint variables in the same lexical block or a visible
outer scope. A declaration in the same block may follow the traversal. A block
in `where` sees every element of the enclosing `match`; a block in a `let` sees
the elements before it. Undirected
traversal and any hop bound above one, on a named edge or a selection, require
the same node type at both ends: a hop ends on the destination type and the
next one must start on the source type, so `$p worksAt{1,2} $c` over
`WorksAt: Person -> Company` is refused with `T5` rather than run as one hop.
A traversal on a named edge needs no bound end: `$a knows $b` alone matches
every `Knows` edge. Mixed-orientation alternatives
need a declared endpoint type when their orientation would otherwise be ambiguous.

A triple is written in its edge's declared order, source first, so the written
order is the direction whatever the end types. Over `WorksAt: Person ->
Company`, `$c: Company, $c worksAt $p` is refused with `T82`, in every scope,
and its fix swaps the ends, `$p worksAt $c`, keeping an edge binding, bounds
and an alternation as written; the swapped triple reads the same edges from the
same bound end. Between ends of one type, `Knows: Person -> Person`, the written
order decides which end is the source. `<e>` reads either direction. A
selection, `(a | b)` or `*`, matches each member in the direction its end types
fix, in written order where both ends have one type, so an alternation whose
members run in opposite directions between the written ends (`$c (worksAt |
employs) $p` with `Employs: Company -> Person`) reads each member its own way.
A wildcard's members are every edge between its two end types, either way. A
selection is `T82` only when no member points along the written order, `$c *
$p` where every edge between the two types is declared `Person -> Company`;
the fix swaps the ends as for a single edge.

Hop counts are shortest-path distances from the start node: `{2,2}` returns the
nodes exactly two hops away. A node is never re-reached through its own
self-loop or through a cycle back to it. The single hop returns the start
node through its own self-loop; a range such as `{1,3}` never returns it.

An unbound traversal has set semantics for endpoint pairs. Binding the edge
returns one result per matching edge, so parallel edges remain distinct. An
edge-type binding, `$k: Knows`, binds every edge of the type, one row per edge
with neither end named: `match { $k: Knows } where $k.since > 2020 return {
$k.@src, $k.@dst }`. Edge
bindings are available only for a single hop. Alternatives share the same
deduplication and shortest-distance search across all members. Bound edges are
identified by canonical type plus ID, so equal IDs in different types remain
distinct: `count(distinct $k)` counts them apart, and `order { $k }` sorts by
type, then ID. An undirected self-loop appears once. A bound edge is read
through its fields (`$k.@id`, `$k.@src`, `$k.@dst`, `$k.@type` and its
properties); a result has no edge object, so `return { $k }` is not lowered in
this release (`T86`).

A bound edge property must exist on every member with the same base value type.
Scalar/list shape and Vector dimensions must agree. It is nullable if any
member allows null. Enum domains are combined, and an enum
combined with plain String becomes String. Other conflicting types or missing
properties are static errors. Blob values retain their dedicated read API;
selecting a Blob-bearing edge does not make Blob properties projectable. An empty wildcard returns no rows with the declared node and
edge-metadata columns; ordinary edge properties cannot be inferred.

Wildcard membership is fixed by the invocation's captured catalog and graph
snapshot. Compatible schema additions join later wildcard invocations. An
addition without a referenced common property invalidates that query. Stored-query
validation refuses a schema change that would invalidate a registered query;
with embedded ad-hoc queries, the schema change can succeed and the next query
fails type checking. Existing graph, branch and stored-query permissions apply.
Explicit historical targets refuse wildcard traversal. When traversal opens a
selected edge dataset, absence at that snapshot is an error for named edges and
explicit alternatives alike. Empty-input paths need not open a dataset.

Statements using alternatives or wildcard share a finite
[`traversal_work_limit`](settings.md) across all traversals, members and hops.
Exhaustion terminates the query with an error, possibly after rows were delivered.
Those rows do not constitute a successful complete result. Result `limit` is
optional and does not replace this work budget. Admission includes every selected
table's physical row count before each scan, even for a selective neighborhood;
see the setting for the cost and batching rules.

Each `$_` is a distinct anonymous node: two anonymous traversals from one
variable are independent, so a source with two neighbors matches two by two
rows. Binding a variable a second time in `match` (`$p: Person` after `$p` is
already bound there) constrains the same rows; it never introduces a second
`$p`. Inside a block, an `optional` or an inline read, binding a name the
enclosing scope holds is `T68` ([inner scopes](#inner-scopes)): `not exists {
$p: Person where $p.name = "bob" }` is refused with the fix `not ($p.name =
"bob")`, or `not coalesce(…, false)` when the test can be null. Variable names
beginning with `__` are reserved.

Bare traversal names begin with a lowercase letter (`worksAt` for the declared
edge `WorksAt`); edge lookup itself is case-insensitive. Any word is an edge
name inside `match`, keywords included (`$d in $c` follows an edge `In`), and a
quoted name is accepted too.

## Patterns that share no variable

The elements form components: a binding, a triple, an `optional` or a `from`
joins the component of every variable it binds or reads, and a `let` value
counts as the variables it reads. Two components that share no variable
multiply their rows, every row of one with every row of the other. A pattern
holds such a product only when every component but one is key-pinned: a
single node binding whose `@key` (every column of a composite key), or whose
`@id`, a top-level conjunct of `where` equates with a literal or a parameter.
A component of two or more elements is never pinned, nor is a triple between
two names of the enclosing scope. Otherwise the type checker refuses it with
`T81` at the second component. The
`where` and its blocks do not join components, so `where $p.name = $c.name`
is a filtered product and is refused too. The rule holds in every scope: a
read's or a write group's `match`, an inline read, a block and an `optional`.

```gq
query staff_beside($slug: String) {
  match { $p: Person, $c: Company }
  where $c.slug = $slug
  return { $p.name, $c.name }
}
```

The fix joins the components through a triple; when exactly one edge type
connects a variable of one with a variable of the other, the printed fix names
it, source first (`$p worksAt $c`). An intended product is written as a named
relation drawn with `from`, which reads nothing of the scope. That is the form
the suggestion writes, for review, since it keeps the product's rows:

```gq
query every_pair() {
  match { $p: Person, let $cs = { match { $c: Company } return { $c } }, from $cs }
  return { $p.name, $c.name }
}
```

## Inner scopes

A block (`exists`, `not exists`, `count` and the other aggregates), an
`optional { ... }` and an inline read are inner scopes, and all follow one
rule. A name the inner
scope reads from the scope where it is written correlates: it is the outer
row's value. A name new to it is introduced by a binding element inside it,
anywhere in the scope: `not exists { $m: Person, $m manages $p }`. A triple
naming a name that is neither in scope nor bound there is refused with `T84`;
the fix inserts the binding before the triple, with the type the edge
declares for that end. An anonymous end, `$_`, and an edge binding,
`$k:knows`, need none. A binding of a name already in scope is `T68`: beside
other elements the fix drops it, so the scope reads the outer name. A block
that only binds an outer name to filter its row, `not exists { $p: Person
where $p.name = "bob" }`, is that filter, and the fix writes it in the
block's place, `not ($p.name = "bob")`, or `not coalesce($p.age > 30, false)`
where the predicate can be null, which keeps the rows whose test is null. A
text search is written the same way, `not (match_terms($d.title, terms($q, mode: any)))`: a
full-text call is a Boolean operand, never null. An `optional` whose only
element re-mentions an outer name, `optional { $p: Person where ... }`, keeps
every row, matched or not, and introduces no name: it is `T68` with no edit.
The names a block or an inline read introduces stay inside it; those an
`optional` introduces enter the enclosing scope, nullable. `match` itself is
not an inner scope: a triple there binds its ends.

## Optional elements

`optional { ... }` keeps every row of the elements before it: a row with
matches once per match, and a row with none once, with every name the
optional introduces null on it. The names it introduces are in scope after it
and nullable, so `$c.name` read from `optional { $c: Company, $p worksAt $c }` is
`String?`, `$c is null` tests whether the row had a match, and `count($c)`
counts the matches. A `let` inside the braces is a column of the matches,
null on a row with none whatever it reads (`let $one = 1` too). A `where`
inside the braces decides which rows match; a `where` after `match` filters
the joined rows:

```gq
query employers() {
  match { $p: Person, optional { $c: Company, $p worksAt $c where $c.slug = "acme" } }
  return { $p.handle, $c.name }
}
```

A triple written after the optional that reads a name it introduces, at
either end, is `T90`: it would drop every row where the name is null and turn
the outer join into an inner one. The fix moves the triple inside the
optional; drop the optional instead where only its matches were meant. A
later optional, a block, an inline read and a `where` read the name and keep
the row. Returning a node the optional introduces whole, `return { $c }`, is not lowered
(`T86`). It prints no fix: returning the node's properties instead runs, but
with other result columns.

A read's plan budget bounds the work of compiling and planning it: every
node of its plan built, every expression typed, each scope's copy of the
names around it, and each step of lowering and planning, in one count. It is
32768 unless the declaration writes `@budget(plan = n)`. A read whose work
passes it is refused with `T70` as soon as it does, naming the construct
whose work grew and how much it charged. Each optional adds work in
proportion to its pattern and to the names around it, so nesting or a long
run of them grows faster than the text. A declared budget above the
session's ceiling, [`plan_budget_limit`](settings.md) (65536 by default; on a
server, the operator's), is refused with `T89` before the read is compiled,
never clamped.

## Values and inline reads

`let $v = <expression>` names a value computed for every row of the elements
before it; `where`, later elements, the blocks of `where`, `return` and
`order` read it as `$v`. A value whose expression cannot fail is read where
its name is. One that can fail (arithmetic: overflow `E1`, a zero divisor
`E2`) runs on every row the elements before it produce, as the core states:
it fails the read on a row that `where`, or an element written after it,
would remove. No conjunct of `where` runs below it. To keep such a row from
failing, guard the value with `if`, whose branch not taken is never
evaluated (`let $r = if($n != 0, $t / $n, 0)`), or write the expression in
`return`, which runs after `where`.

Arithmetic is `+ - * / %` and unary `-` over numbers. Operands of two numeric
types meet in their common type through an explicit widening: a narrower
integer widens to the wider, `U32` with a signed integer is `I64`, and an
integer or an `F32` with a float is `F64`; `if` and `coalesce` widen their
arguments the same way. An integer literal, a negated one such as `-3`
included, takes the other operand's integer type when it fits, so
`$p.age - -3` over an `I32` is `I32` and overflows at `I32`; a float literal
is `F64`, and `(0 - 3)` is an `I64` operand. `U64` with a signed integer and
unary `-` over an unsigned value are refused (`T72`). The result is nullable
when an operand is; division truncates toward zero, and `%` takes the sign of
its left operand. A comparison and the zero-divisor test treat `-0.0` and
`0.0` as equal; a sort puts `-0.0` first and `distinct` keeps both.

`from { match … return { … } }` joins the rows of an inline read, one outer
row per inner row, correlated on the outer variables it reads. Its returned
references bind their names and its returned values read by name; a name it
binds without returning stays its own variable.

An inline read with its own aggregate, `distinct`, `order` or `limit` keeps
them: its rows are grouped, ordered and cut as written, per outer row when it
reads a name of the enclosing scope and once otherwise, before they are
joined. An outer `where` never widens the inner cut and an outer `limit`
never becomes it. A read grouped by no key is one row per outer row, its
`count` 0 where nothing matches:

```gq
query friends_and_oldest() {
  match {
    $p: Person,
    from { match { $q: Person, $p knows $q } return { count($q) as friends } },
    from { match { $o: Person, $p knows $o } return { $o } order { $o.age desc } limit 1 },
  }
  return { $p.handle, $friends, $o.handle }
}
```

A ranking (`nearest`, `bm25`, `rrf`, `distance`) inside such a read is not
lowered (`T86`).

`from union(a, b, …)` joins the bag union of its arms, each an inline read:
every row of every arm, duplicates kept (`return distinct` removes them). The
arms match by column name: each returns the same set of names, a column's base
type agrees in every arm, and the columns come in the first arm's order. A
later arm names its column with `as`, and an arm missing a name or holding
another is `T73`, whose fix aliases it:

```gq
query older_or_known() {
  match {
    from union(
      { match { $p: Person } where $p.age > 30 return { $p } },
      { match { $a: Person, $q: Person, $a knows $q } return { $q as p } }),
  }
  return { $p.handle }
}
```

An arm may return values, `return { $p, 1 as d }`, which match by name and
are nullable where any arm's is, and may group, order and cut its own
rows (`order { $p.age } limit 3` belongs to the arm). An arm may read a name
bound before the union, `$p worksAt $c`: the union then runs for each outer
row. In this release a node column is one type in every arm, no arm returns
an edge, and an arm returning a value or keeping its own cut reads no outer
name (`T86`).

`from name(args)` calls a read declared in the same source: its body runs as
an inline read, each parameter bound to its argument evaluated in the
caller's scope, and its returned columns enter the caller's scope. An argument
is a binding, not text: `$p.handle` below reads the caller's `$p`, never the
`$p` the called read binds for itself. Arguments bind by position, and a
call may leave out the trailing parameters that have defaults; leaving out
one without a default is `T7`. The called read's own aggregate, `order` and
`limit` belong to it, as an inline read's do:

```gq
query colleagues($h: String) {
  match { $p: Person, $p worksAt $c, $q worksAt $c }
  where $p.handle = $h
  return { $q }
}

query everyones_colleagues() {
  match { $p: Person, from colleagues($p.handle) }
  return { $p.handle, $q.handle }
}
```

Comparison operators are `=`, `!=`, `<`, `<=`, `>`, and `>=`.

See [Query language](index.md) for expressions, projection and ordering.
