# Decisions

**Audience:** anyone who is about to change a design that someone chose on
purpose
**Authority:** why the code has the shape it has, and what was rejected; what
a caller is promised is in [the contract](../dev/contract.md), and how the
code works is in the code

A decision record is one page. It says what was chosen, why, and what was
turned down. It is loaded on demand, when a change touches the decision.
[The documentation guide](../dev/documentation.md#decisions) owns the rules
for when a record is written and removed.

## Format

A record is `docs/decisions/<id>.md`, at most 5,000 bytes, in this shape:

```markdown
# Title, as a noun phrase

**Decides:** the choice, in one sentence.
**Contract:** keeps E2, write:3; closes G2
**Code:** `crates/omnigraph/src/exec/merge.rs`, `crates/omnigraph-catalog`
**Cited as:** RFC 0067

## Context

The problem, in three to six sentences: what a caller or an operator could
observe, and what made the obvious design wrong.

## Decision

- Each rule the design fixes, one per item, in the present tense.

## Rejected

- **The alternative:** the one reason it lost.

## Not built

- What the decision includes and the code does not do yet.
```

- **Decides** is one sentence a reader can act on without the rest.
- **Contract** is a [contract line](../dev/contract.md#citing). A decision
  that no caller can observe says `none` and names the invariant it serves.
- **Code** names the crates, modules or tests that own the decision. Every
  path exists.
- **Cited as** is present only when code comments cite the decision by an
  older label, such as `RFC 0067`.
- **Rejected** is the part no other document holds. Each item is an
  alternative a competent engineer would propose, with the reason it lost.
- **Not built** is present only while the code is behind the decision, and
  is deleted when the code reaches it.

## What a record never holds

- How the code works. That is the module's own documentation.
- A plan, a phase list, a rollout order or a status. Open work is an issue.
- History: dates, pull request numbers, review rounds, who proposed what.
- Measurements. A cost claim is a checked-in test or benchmark, and the
  record names it.
- A proposal. A proposal is a pull request; nothing on `main` is a draft.
