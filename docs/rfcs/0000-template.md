# RFC NNNN: <title>

| | |
|---|---|
| **Status** | Proposed |
| **Author(s)** | <your name / handle> |
| **Discussion** | <link to the originating Discussion, if any> |
| **Implementation** | <issue/PR links, filled in as work lands> |

> Status is maintained by maintainers: `Proposed` while the PR is open,
> `Accepted` on merge, `Declined` on close, `Superseded by NNNN` later.

## Summary

One paragraph: what this changes, in plain terms.

## Motivation

What problem does this solve, and why is it worth the ongoing cost? Tie it to a
concrete need (a Discussion, a recurring issue, a user request). Per the
project's first principle, argue the *long-run liability*, not just the
short-term convenience.

## Guide-level explanation

Explain the change as you'd teach it to a user or contributor: new commands,
syntax, API shapes, behavior. Examples first.

## Reference-level design

The precise design: data structures, IR/AST/planner changes, storage/format
impact, migration path, error behavior. Enough that a reviewer can find the
holes.

## Invariants & deny-list check

Which Hard Invariants in [../dev/invariants.md](../dev/invariants.md) does this
touch? Does it brush against any deny-list item — and if so, why is this the
justified exception? State explicitly that no invariant is weakened, or which
Known Gap moves.

## Drawbacks & alternatives

What does this cost, what did you reject, and why. "Do nothing" is a valid
alternative to weigh.

## Reversibility

Is this reversible? On-disk/wire/format and substrate choices are near-permanent
and demand more evidence; a CLI flag or doc is cheap to undo. Say which this is.

## Unresolved questions

What's deliberately left open for review to settle.
