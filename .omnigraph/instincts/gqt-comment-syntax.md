---
id: gqt-comment-syntax
trigger: "when writing, editing, or linting .gq queries or .pg schemas"
confidence: 0.95
domain: query
source: repo-rule
action: "Always use // for comments in .gq and .pg files; never use #"
---

# GQ and PG Comment Syntax

## Action
Always use double slashes `//` for comments in `.gq` query and `.pg` schema files. Never use hash `#`.

## Explanation & Invariant
Using `#` for comments is the single most common parse error encountered by agents in Omnigraph.
The GQ compiler grammar and PG schema parser strictly require C-style line comments (`//`).
A query or schema containing `#` fails at parse time with an unexpected character token error before typechecking.

## Examples
### Correct
```gq
// Look up person by slug
query get_person($slug: String) {
    match {
        $p: Person { slug: $slug }
    }
    return { $p.name }
}
```

### Incorrect
```gq
# This will fail to parse!
query get_person($slug: String) {
    match {
        $p: Person { slug: $slug }
    }
    return { $p.name }
}
```
