# Diagnostics

Every refusal of a query carries four things: a stable code, where the failure
is, what was expected or violated, and one concrete fix. The reader is often
an agent that treats an error as the documentation it acts on, so the fix
names the construct to use, not the rule that was broken. A refusal with no
fix names the decision instead.

| Field | Meaning |
|---|---|
| `code` | Stable identifier: `Q…` from the parser, `T…` from the type checker, `P…` from the planner. A code's meaning is frozen; its message text may improve. |
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

## Planner codes

A planner code refuses a well-formed, type-checked query shape by design. It
is the caller's error, answered as a bad request with the diagnostic, never as
an internal error.

| Code | Meaning |
|---|---|
| `P002` | An edge alternation or wildcard traversal has no finite traversal work limit. The fix sets `traversal_work_limit` before the query. |
| `P003` | The traversal work limit is outside `1..=9223372036854775807`, or a plan records it twice. |
| `P004` | An edge alternation or wildcard traversal was asked to run in CSR traversal mode; it runs in `auto` or `indexed` mode. |
| `P006` | A type-checked shape the planner does not run, such as a block compared with a column, or a write group whose selection is more than one filtered binding of its target; `expected` names it. |

`P001` and `P005` are withdrawn. A ranking ranks the binding it names
whatever its population declares first
([Search](../search/index.md#ranking-a-traversal-reached-binding)), and an
`rrf` whose arms rank two bindings is the type error `T75`.
