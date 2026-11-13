# Diagnostics

Every refusal of a query carries four things: a stable code, where the failure
is, what was expected or violated, and one concrete fix. The reader is often
an agent that treats an error as the documentation it acts on, so the fix
names the construct to use, not the rule that was broken. A refusal with no
fix names the decision instead.

| Field | Meaning |
|---|---|
| `code` | Stable identifier: `Q…` from the parser, `T…` from the type checker, `P…` from the planner, `E…` from execution. A code's meaning is frozen; its message text may improve. |
| `position` | For a parse refusal: `line` and `column` (1-based, in characters) and the `byte` offset. |
| `stage` and `expression` | For a refusal after parsing: the stage (`typecheck` or `plan`) and, when the site can render it, the expression it refused. |
| `expected` | What was expected or violated, one line, without a position. |
| `fix` | One concrete fix, absent when `expected` names the decision. |
| `suggestion` | Optional source edits with `applicability`, plus `start`, `end` and `replacement` for each edit. |

The one-line `error` text is the code and the expectation in the legacy form
(`parse error: …`, `type error: T33: …`, `plan error: P002: …`); the other
fields travel beside it.

## The measured case

A declaration without its parameter list:

```text
query name {
```

```text
error[Q002]: parse error: expected `(`: a query declares its parameters even when it has none
  --> line 1, column 11
  fix: query name()
```

Earlier releases reported this at the file's first position as `expected
query_file`.

## Typed suggestions

The `fix` remains human-readable guidance. When the compiler can propose a
specific edit, `suggestion` carries it as data. For example, this source:

```text
query name { match { $p: Person } return { $p.name } }
```

receives this suggestion alongside Q002:

```json
{
  "applicability": "machine_applicable",
  "edits": [{ "start": 10, "end": 10, "replacement": "()" }]
}
```

Ranges use zero-based UTF-8 byte offsets in the exact original request source,
with an exclusive `end`. Equal offsets insert text. Ranges do not overlap;
apply multiple edits from the highest offset to the lowest. Do not apply an
edit to a source that has changed since the request.

`machine_applicable` means the compiler has enough evidence for this local
correction. `needs_review` means the caller must review the proposed change.
Neither value guarantees that the corrected query will pass every check;
validate it again. The compiler does not apply suggestions automatically.

Q002 offers the `()` insertion only when the edited file matches the grammar
without another missing-parameter recovery. Incomplete or ambiguous input can
still receive textual guidance while omitting `suggestion`. Missing suggestions
do not mean there is no possible fix.

## Checked literals

A literal compared with a property, or listed in an `in` against one, is
checked against the property's type wherever it is written: a `where`, a
block's or an `optional`'s `where`, a write group's `where` and a `let`, in
either operand order. An enum value outside the declared set, an integer
outside the property's range, a float outside the `F32` range and a `date(…)`
or `datetime(…)` that does not parse are `T3`, so a misspelled enum value is
refused instead of matching no rows. A float against an integer property is
admitted: it compares through a cast and keeps its fraction. `contains(…)` and
`starts_with(…)` over an enum property refuse a literal that no declared value
passes. A parameter keeps its declared type and is not checked against the
property's domain.

The fix names the nearest allowed value: the closest enum value by edit
distance, the range bound or the nearest calendar day. Where the literal was
written in a comparison or an `in`, `suggestion` writes that value in its place
as `needs_review`, since the intended value is a guess. This source:

```text
query open_prs() { match { $pr: PullRequest } where $pr.status = "Closed" return { $pr.title } }
```

against `status: enum(closed, open)` is refused with `T3`, the expectation
``property `status` expects one of [closed, open], got 'Closed'``, the fix
`"closed"` and this suggestion:

```json
{
  "applicability": "needs_review",
  "edits": [{ "start": 65, "end": 73, "replacement": "\"closed\"" }]
}
```

A date with no nearest calendar day and a malformed `datetime(…)` print the
expected format instead, without an edit.

## Where the fields appear

- **CLI, human formats** (`table`, `kv`, `csv`): the form above on stderr,
  then `HTTP 400` when served and the `disposition` line, exit status 1, with no
  colour codes and no backtrace footer.
- **CLI, machine formats**: `--json` and `--format json` print the API's error
  body pretty with its `disposition`, `--format jsonl` prints it as one line, and
  `--format toon` prints that same body as TOON. All of them go on stdout with
  exit status 1. A served refusal keeps the server's `code` field
  (`bad_request`); an embedded one has none.
- **HTTP**: a `400` that refuses a query carries the additive `diagnostic`
  object in its error body. See
  [HTTP errors](../operations/troubleshooting.md#http-errors).
- **`omnigraph lint`**: a `Q000` finding whose message is the expectation at
  `line <n>, column <c>`; type errors report their `T…` code, and `T83` (two
  declarations of one name) is reported for the whole file at the later
  declaration's `line <n>, column <c>`. See
  [Linting](index.md#linting).
- **`queries validate --json` and `cluster plan --json`**: each breakage or
  query parse/typecheck failure carries the same diagnostic information as
  `diagnostic` or `detail`, so a stored query the next release would refuse
  is a pre-upgrade finding. Cluster `detail` uses the compiler shape: `message`
  and a nested `stage` object. API and validation output use `expected` and
  separate `stage`/`expression` fields.

```json
{
  "error": "parse error: expected `(`: a query declares its parameters even when it has none",
  "code": "bad_request",
  "diagnostic": {
    "code": "Q002",
    "position": { "line": 1, "column": 11, "byte": 10 },
    "expected": "expected `(`: a query declares its parameters even when it has none",
    "fix": "query name()"
  }
}
```

## Parse codes

| Code | Meaning |
|---|---|
| `Q001` | The source does not match the grammar at the reported position; `expected` names the grammar rules the parser could accept there. |
| `Q002` | A query declaration is missing its parameter list. |
| `Q003` | A settings statement is refused: unknown setting, value outside its row, or a process setting in a request. |
| `Q004` | A branch, commit, show or explain statement is misplaced or malformed. |
| `Q005` | A declaration body is refused; `expected` names the construct. |
| `Q006` | The source nests too deeply, checked before the grammar runs: more than 64 levels of brackets and adjacent `not`s, or an expression deeper than 256 levels when each `and`/`or` of a chain also counts one. Write a long `or` chain over one property as `$p.name in [...]`. |
| `Q007` | A traversal is written with an arrow; the fix is the triple. |
| `Q008` | An `update` or `delete` statement carries a `where`; the fix is the group's `match` and `where`, as in `match { $x: Person } where $x.email = $e  delete $x`. |
| `Q009` | `mutation` is not a declaration keyword; the fix is `query`. |
| `Q010` | A binding carries a property map or braces; the fix is the binding and the conjunct in `where`, `match { $p: Person } where $p.handle = $h`. |
| `Q011` | A read body carries `as commit` or `as unattached branch`. |
| `Q012` | A respelling the language refuses (`not { … }`, `\|\|`, `CASE`, a chained comparison, a one-name edge group `(knows)`); the fix prints the legal form. |
| `Q013` | A write's elements are written without `match { }`, or after a statement; the fix is the group's `match`. |
| `Q014` | A spelling from another language (`==`, `&&`, `WHERE`, `count(*)`, …); the fix prints the GQ form. |
| `Q015` | A form the language does not have yet; the message names it. |
| `Q016` | Two elements, return items, order keys, call arguments or assignments are separated by whitespace alone; the fix is the comma. |
| `Q017` | A filter, a `where` or a comparison inside the `match` braces; the fix moves it to the `where` after them, joined with `and`. |
| `Q018` | A bare name where a value is read; a bare name is a return alias in `order` only. |
| `Q019` | A call named by an element keyword (`exists(x)`), or `from` over a parenthesized relation. |
| `Q020` | A read body's parts are missing, misplaced or unbraced. |
| `Q021` | An annotation's named argument is written `name: value`; the fix is `name = value`. |

Type codes are listed where the construct they guard is described, in
[Query language](index.md).

## Patterns that share no variable

Two components of a pattern that share no variable multiply their rows. The
type checker refuses that product with `T81` at the second component unless
every component but one is pinned to one row by an equality of its `@key` with
a literal or a parameter in `where`; see
[Traversal and match patterns](traversal.md#patterns-that-share-no-variable).

```text
query staff() { match { $c: Company, $p: Person } return { $p.name, $c.name } }
```

```text
error[T81]: type error: T81: `$p: Person` shares no variable with `$c: Company`, so the pattern multiplies their rows by a product; a product is written only when every component but one is pinned to one row by a `@key` equality in `where`
  --> line 1, column 38
  fix: join them through the edge, `$p worksAt $c`, or write the intended product as a named relation, `let $ps = { match { $p: Person } return { $p } }, from $ps`
```

When exactly one edge type connects a variable of one component with a
variable of the other, as `WorksAt: Person -> Company` does here, the
suggestion is machine-applicable and appends the triple to the elements,
source first: `, $p worksAt $c`. Otherwise the fix lists the triples, if any,
and the suggestion, which needs review, writes the second component as the
named relation drawn with `from`.

## Triples against their edge's direction

A triple is written in its edge's declared order, source first; see
[Traversal and match patterns](traversal.md). A triple written against it is
`T82`, at the triple:

```text
query staff() { match { $c: Company, $c worksAt $p } return { $p.name } }
```

```text
error[T82]: type error: T82: `$c worksAt $p` is written against the declared direction of `WorksAt: Person -> Company`: a triple is written in its edge's declared order, source first, so its written order is its direction
  --> line 1, column 38
  fix: write the source first: `$p worksAt $c`
```

The machine-applicable suggestion swaps the two ends and leaves the edge, its
binding, bounds and alternation as written. The swapped triple reads the same
edges from the same bound end.

## Inner scopes

A block and an inline read bind every name new to them and read an outer
name to correlate on it; see
[Traversal and match patterns](traversal.md#inner-scopes). A triple naming a
name that is neither in scope nor bound there is `T84`:

```text
query loners() { match { $p: Person } where not exists { $p knows $q } return { $p.name } }
```

```text
error[T84]: type error: T84: `$q` is named in a triple of this block but bound nowhere: a name new to a block or an inline read is introduced by a binding element inside it, and a name read from the enclosing scope correlates
  --> line 1, column 58
  fix: bind it in the block: `$q: Person`
```

The machine-applicable suggestion inserts `$q: Person, ` before the triple,
with the type the edge declares for that end, and binds every other name the
scope's triples introduce in the same edit. A binding inside a block of a name
the enclosing scope holds is `T68`. Its suggestion drops the binding, so the
block reads the outer name; a block that only binds the name to filter its
row, `not exists { $p: Person where $p.name = "Bob" }`, becomes that filter,
`not ($p.name = "Bob")`, or `not coalesce(…, false)` around a predicate that
can be null. A binding of another type has no suggestion: name the block's
own variable differently.

## A ranked `from` names its score

A `from` whose relation is a ranking, a ranking function or a `let` bound to
one, names the score with `as`; see [Search](../search/index.md). Without it
the `from` is `T85`:

```text
query near($q: Vector(3)) { match { let $pop = { match { $d: Doc } return { $d } }, from nearest($pop, $d.embedding, $q, k: 2) } return { $d.slug, $distance } }
```

```text
error[T85]: type error: T85: `from nearest(…)` ranks without naming its score: a ranked `from` names its score with `as`, and the score enters scope only under that name
  --> line 1, column 85
  fix: from nearest(…) as $distance
```

The machine-applicable suggestion writes `as $distance` after the ranking
(`as $score` for `bm25` and `rrf`), the name the score column had before it
was written, or a fresh variant when that name is taken. An `as` on a `from`
that ranks nothing, `from $docs as $s`, is `T85` too, and the suggestion
removes it: the relation names its own columns.

## Terms this release does not lower

A read or write the type checker accepts, the planner runs. A term that
type-checks but that this release's planner has no plan for is refused at
type check with `T86`, at the construct it was written as (the `let`, the
`optional`, the `from`, the `limit`, the order key, the block's comparison,
the write group's `match`, the statement), before any storage is read. The
message names the stage of the language programme that adds it: `B` for a
construct of edition 3 that this release does not run yet, `Rec` for a read
that calls itself.

| Refused | Stage | Rewrite this release runs |
|---|---|---|
| `optional { … }`, `from union(…)`, a declared read's call, `distance`, an edge-type binding in a read, `limit $param` | B | none |
| A `let` whose value can fail (arithmetic) | B | write the value where the name is read, in `where` or `return` |
| An order key no return item computes | B | return the key under an alias; the edit inserts it |
| An inline read with an aggregate, `distinct`, `order` or `limit` | B | none |
| A block compared with a column, a block of several aggregates | B | none |
| An aggregate over a ranking's score; a `nearest` window read from a parameter outside the read's `limit`; two `rrf` arm windows that differ | B | none |
| A write group whose selection reads a type an earlier group of the write changes; a `Blob` read from a row in an assignment | B | none |
| A read that calls itself (`Fix`) | Rec | none |

```text
query q() { match { $p: Person } return { $p.handle } order { $p.age > 30 } }
```

```text
error[T86]: type error: T86: `q`: this release's planner does not lower an order key computed from columns no return item computes; stage B (the break) adds it
  --> line 1, column 63
  fix: return the key under an alias, which the sort reads: `$p.age > 30 as key`
```

Its suggestion inserts `$p.age > 30 as key, ` before the first return item.
`P006` is no longer a refusal: see [Planner codes](#planner-codes).

## Planner codes

A planner code refuses a well-formed, type-checked query shape by design. It
is the caller's error, answered as a bad request with the diagnostic, never as
an internal error; `P006` is the exception, the planner's assertion that the
type checker let through a term it does not lower.

| Code | Meaning |
|---|---|
| `P002` | An edge alternation or wildcard traversal has no finite traversal work limit. The fix sets `traversal_work_limit` before the query. |
| `P003` | The traversal work limit is outside `1..=9223372036854775807`, or a plan records it twice. |
| `P004` | An edge alternation or wildcard traversal was asked to run in CSR traversal mode; it runs in `auto` or `indexed` mode. |
| `P006` | Not a refusal: the planner's assertion that a term the type checker accepted arrived unlowered. The type checker refuses every such term (`T86`), so `P006` is an internal error and a defect to report. |

`P001` and `P005` are withdrawn. A ranking ranks the binding it names
whatever its population declares first
([Search](../search/index.md#ranking-a-traversal-reached-binding)), and an
`rrf` whose arms rank two bindings is the type error `T75`.

## Execution codes

An execution code refuses a row while the query or write runs
(`execution error: E2: …`). A refused write publishes nothing.

| Code | Meaning |
|---|---|
| `E1` | Integer arithmetic overflowed its type. |
| `E2` | A divisor is zero. |
| `E3` | A float result is not finite. |
| `E4` | The rows of one write statement give one entity's property two values, which row order would otherwise decide; make the selection reach each entity once, or agree on the value. |
