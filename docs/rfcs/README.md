# RFCs

Substantial changes to OmniGraph — new user-facing surface, format or protocol
changes, anything irreversible or cross-cutting — go through a lightweight RFC
so the design is agreed *as reviewable code* before implementation starts.
`docs/rfcs/` is the durable home for both externally authored proposals and
maintainer design series. Every formal RFC uses the same four-digit filename
namespace; explicit metadata selects the applicable lifecycle.

This complements the always-on review bar in
[../dev/invariants.md](../dev/invariants.md): the invariants say *what every
change must respect*; an RFC says *why this particular change is worth making and
how*.

> **Two tracks, don't conflate their lifecycle.** Every RFC must declare an
> `Author track` of either `Public contribution` or
> `Maintainer design series`, plus an explicit `Status`. Those fields—not the
> filename—select the lifecycle. Anyone may author a public contribution RFC,
> and merge means maintainer acceptance. Maintainer design series may be merged
> and revised while still draft so design, review findings, and implementation
> evidence remain visible together. Existing `docs/dev/rfc-00N-*.md` files are
> legacy internal design and implementation records that retain their existing
> lifecycle; new formal RFCs live here.

## When you need one

- **RFC required:** new query/schema/CLI/HTTP surface; on-disk or wire-format
  changes; a new substrate dependency; anything the deny-list in
  [../dev/invariants.md](../dev/invariants.md) flags; anything irreversible
  ("reversibility shapes evidence demand").
- **RFC not required:** bug fixes for an `accepted` issue, and the trivial
  fast-lane (typos, docs, deps) — see [../../CONTRIBUTING.md](../../CONTRIBUTING.md).

If you're unsure, start a [Discussion](../../../discussions); a maintainer will
tell you whether it needs an RFC.

## Public contribution lifecycle

```
Discussion (incubate, get rough consensus)
      │ graduate
      ▼
RFC pull request  →  adds docs/rfcs/NNNN-title.md
                         (Author track: Public contribution; Status: Proposed)
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

- Every formal RFC file is `docs/rfcs/NNNN-kebab-title.md`, where `NNNN` is
  the next free zero-padded integer. `0000-template.md` is reserved.
- Pick the number when you open the PR; if it collides with another in-flight
  RFC, the second to merge bumps theirs.
- Numbers are never reused. A maintainer-series or superseded record still
  reserves its number.
- Every RFC declares exactly one `Author track` and an explicit `Status`. If a
  file has both YAML frontmatter and rendered metadata, the values must agree.

### Public status values

`Proposed` (open PR) · `Accepted` (merged) · `Declined` (closed) ·
`Superseded by NNNN` · `Implemented` (set once the work lands, optional).

Copy [0000-template.md](0000-template.md) to start a public contribution RFC.

## Maintainer design-series lifecycle

Maintainers use this lane for a related architecture series that benefits from
durable in-repository review before every decision or implementation dependency
is ready. Merge publishes the current reviewed design state; it does **not** by
itself mean acceptance. Each file names `Maintainer design series` as its author
track, identifies an owner, links its review ledger when one exists, and records
support boundaries alongside implementation evidence. This metadata is required
for new or materially revised series; older records are not silently reclassified.

When YAML frontmatter is present, its machine-readable status is `draft`,
`research-blocked`, `accepted`, `implemented`, or `superseded`; headings render
these as `Draft`, `Research blocked`, `Accepted`, `Implemented`, and
`Superseded by RFC-<number>`.
Moving a file to `accepted` or `implemented` requires dispositioning the blockers
owned by that RFC. Open findings in a sibling RFC do not prevent an independently
reviewable member of the series from advancing.

The explicit author track and status are a process boundary, not an authority
distinction: both tracks are reviewed by maintainers, and neither may bypass
the invariants, compatibility gates, or evidence required by the change.
