# RFCs

Substantial changes to OmniGraph — new user-facing surface, format or protocol
changes, anything irreversible or cross-cutting — go through a lightweight RFC
so the design is agreed *as reviewable code* before implementation starts. This
is the public RFC track, open to **anyone, including external contributors**.

This complements the always-on review bar in
[../dev/invariants.md](../dev/invariants.md): the invariants say *what every
change must respect*; an RFC says *why this particular change is worth making and
how*.

> **Two tracks, don't conflate them.** This `docs/rfcs/` directory is the
> **public contribution** track (anyone authors; maintainers accept). The
> maintainer-internal RFCs under `docs/dev/rfc-00N-*.md` are a separate,
> team-owned track for in-flight internal work. If you're an outside
> contributor, you're in the right place here.

## When you need one

- **RFC required:** new query/schema/CLI/HTTP surface; on-disk or wire-format
  changes; a new substrate dependency; anything the deny-list in
  [../dev/invariants.md](../dev/invariants.md) flags; anything irreversible
  ("reversibility shapes evidence demand").
- **RFC not required:** bug fixes for an `accepted` issue, and the trivial
  fast-lane (typos, docs, deps) — see [../../CONTRIBUTING.md](../../CONTRIBUTING.md).

If you're unsure, start a [Discussion](../../../discussions); a maintainer will
tell you whether it needs an RFC.

## Lifecycle

```
Discussion (incubate, get rough consensus)
      │ graduate
      ▼
RFC pull request  →  adds docs/rfcs/NNNN-title.md  (Status: Proposed)
      │
maintainer review  ──▶  changes requested / declined (PR closed, with rationale)
      │
      ▼
merged  ==  Accepted   (the merged file is the durable decision record)
      │
      ▼
Implementation PR(s)  reference the accepted RFC
```

- **Author:** anyone. **Acceptance:** a maintainer decision, performed by
  merging the RFC PR. Declining is closing it with rationale.
- The merged RFC *is* the accepted record — there is no separate sign-off step.
- Later reversals don't edit history: supersede with a new RFC that links back
  and flip the old one's `Status` to `Superseded`.

## Numbering & naming

- File: `docs/rfcs/NNNN-kebab-title.md`, where `NNNN` is the next free
  zero-padded integer (`0001`, `0002`, …). `0000-template.md` is reserved.
- Pick the number when you open the PR; if it collides with another in-flight
  RFC, the second to merge bumps theirs.

## Status values

`Proposed` (open PR) · `Accepted` (merged) · `Declined` (closed) ·
`Superseded by NNNN` · `Implemented` (set once the work lands, optional).

Copy [0000-template.md](0000-template.md) to start.
