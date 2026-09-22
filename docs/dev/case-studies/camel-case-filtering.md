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

Mutation predicates are rendered as a SQL string with two consumers:

- Lance's committed-row scanner, which preserves the case of an unquoted identifier.
- DataFusion's SQL parser for the pending in-memory `MemTable`, which normalized an unquoted identifier to lowercase.

Double-quoting the column was not a shared fix: at the Lance boundary, `"repoName"` is a string literal and silently matches no committed rows. The first fix therefore left the predicate unquoted and disabled normalization on the pending DataFusion context. That form had a second hole: a property named like a SQL keyword construct (`interval`, `exists`, `trim`) is a parse error when bare, in both consumers, so `delete Span where interval = 1` failed while reads on `interval` worked.

The predicate is now rendered with backticks:

```text
`repoName` = 'acme'
`interval` = 1
```

Lance reads a backtick-quoted identifier as a case-preserving column (its documented escape for non-standard names), and the generic SQL dialect DataFusion parses the pending query with delimits identifiers with backticks as well, so a quoted identifier is exempt from normalization and the override is gone. The pending side matters only when a multi-statement mutation re-reads a pending row, which explains why ordinary single-statement mutation tests did not expose the second consumer.

## Regression ownership

The protection is intentionally layered:

- the expression unit test proves `repoName` reaches DataFusion unchanged;
- `literal_filters.rs` proves a camelCase indexed match returns the right row;
- `writes.rs` proves update/delete and a chained pending-row update preserve case;
- `lance_surface_guards.rs` proves the camelCase equality still plans through the scalar index and records why `col()` is forbidden here.

Testing only the returned row would miss an accidental full-scan fallback. Testing only the plan would miss pending mutation semantics.

## General rule

Use structured column expressions when the input is already a resolved schema field. If one predicate string must cross multiple parsers, verify each parser's identifier and quoting rules independently; do not assume SQL-looking syntax has one meaning across libraries.
