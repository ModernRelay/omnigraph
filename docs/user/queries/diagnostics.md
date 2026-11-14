# Diagnostics

Every refusal of a query carries four things: a stable code, where the failure
is, what was expected or violated, and one concrete fix. The reader is often
an agent that treats an error as the documentation it acts on, so the fix
names the construct to use, not the rule that was broken. A refusal with no
fix names the decision instead.

| Field | Meaning |
|---|---|
| `code` | Stable identifier: `Q…` from the parser, `T…` from the type checker, `P…` from the planner, `E…` from execution. A code's meaning is frozen; its message text may improve. |
| `position` | Where the refused construct starts: `line` and `column` (1-based, in characters) and the `byte` offset. A parse refusal has one, and so does a type refusal of written text. |
| `end` | Where the refused construct ends, exclusive, in the same form: `position` to `end` is its span. |
| `stage` and `expression` | For a refusal after parsing: the stage (`typecheck` or `plan`) and, when the site can render it, the expression it refused. |
| `expected` | What was expected or violated, one line, without a position. |
| `fix` | One concrete fix, absent when `expected` names the decision. |
| `suggestion` | Optional source edits with `applicability`, plus `start`, `end` and `replacement` for each edit. |
| `related` | Other spans the refusal names, each a `position`, an `end` and a `message`: `T83` names the first declaration of the repeated name. |
| `further` | The other refusals of the same compile pass, in source order, each in this shape. |

The one-line `error` text is the code and the expectation in the legacy form
(`parse error: …`, `type error: T33: …`, `plan error: P002: …`); the other
fields travel beside it.

## Every refusal in one pass

A compile returns every independent refusal in one pass, in source order,
each with its span; the first is the diagnostic and the `error` line, the
others its `further` list. A grammar refusal stops the pass, since the text
after it has no tree. Otherwise the pass holds a `T83` at each later
declaration of a repeated name and every declaration's own refusals: each
element of `match`, top-level conjunct of `where`, return item, order key and
write statement is checked by itself. A construct naming a variable that a
refused one would bind is skipped, and so are the order keys after a refused
return item; a block, an `optional` or an inline read stops at its first
refusal. `T81` and `T86` are checked only when nothing else in the
declaration was refused.

```text
query q() { match { $c: Company } where $c.names = "x" and $c.slug = 3 return { $c } }
```

```text
error[T6]: type error: T6: type `Company` has no property `names`; nearest: name (String), slug (String)
  --> line 1, column 41
error[T7]: cannot compare String with I64
  --> line 1, column 60
```

An unknown type, property, edge, variable, return alias, option or parameter
type lists up to five names of its kind after `nearest:`, nearest first, each
with its type. An enum value outside its set lists the declared values, the
five nearest when there are more.

Applying every `machine_applicable` suggestion of one pass at once (they never
overlap) gives a source that parses, holds fewer refusals and undoes no
earlier edit; repairing the grammar can reveal type refusals, and those
`T86`. Every expect-error case of the logic tests reaches a source with no such
suggestion left within three passes.

## Typed suggestions

A declaration without its parameter list:

```text
query name { match { $p: Person } return { $p.name } }
```

```text
error[Q002]: parse error: expected `(`: a query declares its parameters even when it has none
  --> line 1, column 11
  fix: query name()
```

The `fix` is human-readable guidance. When the compiler can propose a specific
edit, `suggestion` carries it as data:

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

Incomplete or ambiguous input can receive textual guidance without
`suggestion`; a missing suggestion does not mean there is no fix.

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
  `line <n>, column <c>`; `T83` (two declarations of one name) is a finding
  for the whole file at each later declaration's `line <n>, column <c>`. A
  refused query prints every refusal of its pass, each at its position with
  its fix, and `--json` lists them in its `diagnostics`. See
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

Two components of a pattern that share no variable are `T81` at the second
component unless every component but one is pinned by a `@key` equality; see
[Traversal and match patterns](traversal.md#patterns-that-share-no-variable).

```text
query staff() { match { $c: Company, $p: Person } return { $p.name, $c.name } }
```

```text
error[T81]: type error: T81: `$p: Person` shares no variable with `$c: Company`, so the pattern multiplies their rows by a product; a product is written only when every component but one is pinned to one row by a `@key` equality in `where`
  --> line 1, column 38
  fix: join them through the edge, `$p worksAt $c`, or write the intended product as a named relation, `let $ps = { match { $p: Person } return { $p } }, from $ps`
```

The suggestion appends the triple, machine-applicable, when exactly one edge
type joins the components; otherwise it writes the named relation and needs
review.

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

The machine-applicable suggestion swaps the two ends and leaves the rest as
written.

## Inner scopes

A triple in a block or an inline read naming a name that is neither in scope
nor bound there is `T84`; see
[Traversal and match patterns](traversal.md#inner-scopes).

```text
query loners() { match { $p: Person } where not exists { $p knows $q } return { $p.name } }
```

```text
error[T84]: type error: T84: `$q` is named in a triple of this block but bound nowhere: a name new to a block or an inline read is introduced by a binding element inside it, and a name read from the enclosing scope correlates
  --> line 1, column 58
  fix: bind it in the block: `$q: Person`
```

The machine-applicable suggestion inserts `$q: Person, ` before the triple and
binds every other name the scope's triples introduce in the same edit. The
fixes of `T68`, a binding of a name the enclosing scope holds, are described
there too.

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
(`as $score` for `bm25` and `rrf`), or a fresh variant when the name is taken.
An `as` on a `from` that ranks nothing is `T85` too, and the suggestion
removes it.

## Terms this release does not lower

A read or write the type checker accepts, the planner runs. A term that
type-checks but that this release's planner has no plan for is refused at
type check with `T86`, at the construct it was written as (the `let`, the
`optional`, the `from`, the `limit`, the order key, the block's comparison,
the write group's `match`, the statement), before any storage is read. The
message names the stage of the language programme that adds it: `B` for a
construct of edition 3 that this release does not run yet, `Rec` for a read
that calls itself.

| Refused | Stage |
|---|---|
| `distance` as an `rrf` arm | B |
| A `union` arm returning an edge, or one returning a value or keeping its own cut that reads a name of the enclosing scope | B |
| A node an `optional` introduces, returned whole | B |
| A ranking inside an inline read with its own aggregate, `distinct`, `order` or `limit` | B |
| A block compared with another block's value, a block of several aggregates | B |
| Two `rrf` arm windows that differ | B |
| A full-text call (`search`, `fuzzy`, `match_text`) read as a value in `return`, `order` or a block's aggregate | B |
| A write group whose selection reads a type an earlier group of the write changes; a `Blob` read from a row in an assignment | B |
| A read that calls itself (`Fix`) | Rec |

```text
query q($v: Vector(3), $s: String) { match { let $all = { match { $d: Doc } return { $d } }, from rrf(distance($all, $d.embedding, $v), bm25($all, $d.body, $s)) as $score } return { $d.title } }
```

```text
error[T86]: type error: T86: `q`: this release's planner does not lower an `rrf` arm over `distance`; stage B (the break) adds it
  --> line 1, column 94
```

`T86` prints a fix only where a form this release runs has the same rows and
result columns, and none above has one: returning an `optional` node's
properties instead of the node, for example, changes the result columns.
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
