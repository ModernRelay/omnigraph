# Diagnostics

Every refusal of a query carries four things: a stable code, where the failure
is, what was expected or violated, and one concrete fix. The reader is often
an agent that treats an error as the documentation it acts on, so the fix
names the construct to use, not the rule that was broken. A refusal with no
fix names the decision instead.

| Field | Meaning |
|---|---|
| `code` | Stable identifier: `Q…` from the parser, `T…` from the type checker. A code's meaning is frozen; its message text may improve. |
| `position` | For a parse refusal: `line` and `column` (1-based, in characters) and the `byte` offset. |
| `stage` and `expression` | For a refusal after parsing: the compiler stage (`typecheck`) and, when the site can render it, the expression it refused. |
| `expected` | What was expected or violated, one line, without a position. |
| `fix` | One concrete fix, absent when `expected` names the decision. |

The one-line `error` text is the code and the expectation in the legacy form
(`parse error: …`, `type error: T33: …`); the other fields travel beside it.

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

## Where the fields appear

- **CLI, human formats** (`table`, `kv`, `csv`): the form above on stderr,
  exit status 1, with no colour codes and no backtrace footer.
- **CLI, machine formats**: `--json` and `--format json` print the API's error
  body pretty, `--format jsonl` prints it as one line, both on stdout with
  exit status 1. A served refusal keeps the server's `code` field
  (`bad_request`); an embedded one has none.
- **HTTP**: a `400` that refuses a query carries the additive `diagnostic`
  object in its error body. See
  [HTTP errors](../operations/troubleshooting.md#http-errors).
- **`omnigraph lint`**: a `Q000` finding whose message is the expectation at
  `line <n>, column <c>`; type errors report their `T…` code. See
  [Linting](index.md#linting).
- **`queries validate --json` and `cluster plan --json`**: each breakage or
  `query_typecheck_error` diagnostic carries the same object as `diagnostic`
  or `detail`, so a stored query the next release would refuse is a
  pre-upgrade finding.

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
| `Q004` | A branch or show statement is misplaced or malformed. |
| `Q005` | A declaration body is refused; `expected` names the construct. |

Type codes are listed where the construct they guard is described, in
[Query language](index.md).
