# Case study: preserving camelCase filters

A property name that survived schema parsing, lint, and lowering once changed case at the engine/storage boundary. The result was a valid query failing only at runtime:

```text
No field named reponame. Column names are case sensitive.
```

The fix is small; the useful lesson is that two superficially similar filter consumers had different identifier rules.

## Reproduction

```pg
node SourceDocument {
  repoName: String @index
}
```

```gq
query find($repoName: String) {
  match { $d: SourceDocument { repoName: $repoName } }
  return { $d.repoName }
}
```

The compiler correctly preserved `repoName`. The corruption happened in execution, after static validation had finished.

## Two boundaries, two parsers

### Structured read pushdown

The read path built a DataFusion expression with `col(property)`. `col(&str)` parses and normalizes a SQL-style identifier, so the unquoted `repoName` became `reponame`. Lance stores the Arrow field case-preservingly and could not resolve the changed name.

The fix uses the structured, case-preserving constructor:

```rust
datafusion::prelude::ident(property)
```

The IR property is already one unqualified field name, so parsing a qualified SQL name was unnecessary. `ident` also preserves scalar-index eligibility for the real field.

### Pending mutation scan

Mutation predicates are also rendered as an unquoted string such as:

```text
repoName = 'acme'
```

That string has two consumers:

- Lance's committed-row scanner preserves the case of the unquoted identifier.
- DataFusion's SQL parser for the pending in-memory `MemTable` normalized it to lowercase.

Quoting the column was not a shared fix: at this Lance boundary, double quotes were interpreted as a string literal and could silently match no committed rows. The predicate therefore remains unquoted, while the pending DataFusion context disables normalization:

```rust
config.options_mut().sql_parser.enable_ident_normalization = false;
```

This matters only when a multi-statement mutation re-reads a pending row, which explains why ordinary single-statement mutation tests did not expose the second bug.

## Regression ownership

The protection is intentionally layered:

- the expression unit test proves `repoName` reaches DataFusion unchanged;
- `literal_filters.rs` proves a camelCase indexed match returns the right row;
- `writes.rs` proves update/delete and a chained pending-row update preserve case;
- `lance_surface_guards.rs` proves the camelCase equality still plans through the scalar index and records why `col()` is forbidden here.

Testing only the returned row would miss an accidental full-scan fallback. Testing only the plan would miss pending mutation semantics.

## General rule

Use structured column expressions when the input is already a resolved schema field. If one predicate string must cross multiple parsers, verify each parser's identifier and quoting rules independently; do not assume SQL-looking syntax has one meaning across libraries.
