# Governance

This document describes how **external contributions** to OmniGraph are
proposed, accepted, and merged. It exists so an outside contributor can answer,
without asking: *where does my report/idea/change go, who decides, and what has
to happen before code lands?*

> **Scope.** This governs the public contribution surface — Issues,
> Discussions, RFCs, and pull requests from people outside the ModernRelay
> team. **Maintainers operate under a separate internal process** and are not
> bound by the intake gates below. Everyone, maintainer or not, is still bound
> by the universal gates: branch protection on `main` and CI
> (see [docs/dev/branch-protection.md](docs/dev/branch-protection.md)).

## Roles

| Role | Who | Authority |
|---|---|---|
| **Maintainer** | The ModernRelay team (repository admins) | Validate issues, accept/reject RFCs, review and merge PRs, set direction. Final decision authority. |
| **Contributor** | Anyone else | Report problems (Issues), propose ideas (Discussions), author RFCs, and open pull requests. |

Decision authority rests with the maintainers (the ModernRelay team holding
repository-admin access).

## The three channels

Each channel has one job. Using the right one is the first thing we ask of a
contribution.

| Channel | Purpose | Not for |
|---|---|---|
| **[Issues](../../issues)** | **Report a problem** — a bug, a regression, a documented behavior that's wrong. Something concrete and reproducible. | Feature requests, ideas, questions, or design proposals (→ Discussions). |
| **[Discussions](../../discussions)** | **Propose and explore** — new ideas, feature requests, questions, and the incubation of RFCs. | Bug reports (→ Issues). |
| **Pull requests** | **Land a sanctioned change** — a fix for a *validated* issue, an *accepted* RFC, or a trivial change (see fast-lane). | Substantive change with no backing issue/RFC — it will be redirected. |

## How a change becomes mergeable

```
            ┌─────────── bug ───────────┐        ┌──────── idea / feature ────────┐
            ▼                            │        ▼                                │
        Issue (problem report)           │   Discussion (idea / RFC incubation)    │
            │                            │        │                                │
   maintainer triage                     │   rough consensus                        │
            │                            │        │ graduate                         │
            ▼                            │        ▼                                  │
  label: accepted  ──────────┐          │   RFC PR  (docs/rfcs/NNNN-*.md)           │
            │                 │          │        │                                  │
            │                 │          │   maintainer review                       │
            ▼                 ▼          │        ▼                                  │
     Pull request  ◀──────────┴──────────│──  merged == accepted                     │
   (links the issue or the accepted RFC) ◀───────┘ (implementation PRs reference it) │
            │
   review + branch protection + CI
            ▼
         merged
```

### Issues → validated
A new issue starts unlabeled. A maintainer triages it and, if it's a real,
in-scope problem, applies the **`accepted`** label. **Only `accepted` issues are
open for a contributor PR.** This prevents the "I fixed an issue you hadn't
agreed was a problem" rejection. Want to fix something? Get the issue accepted
first, or pick one already labelled `accepted` / `help wanted`.

### Discussions → RFCs → accepted
Ideas and feature requests start in **Discussions**. Anyone — including external
contributors — may then **author an RFC** by opening a pull request that adds
`docs/rfcs/NNNN-title.md` (see [docs/rfcs/README.md](docs/rfcs/README.md)). The
RFC is reviewed as code; **a maintainer merging it is the act of acceptance**
(it becomes the durable decision record). Implementation PRs then reference the
accepted RFC.

Authoring an RFC is open to everyone; **accepting one is a maintainer
decision.** Maintainers may also decline an RFC, with rationale, by closing it.

### Pull requests → sanctioned
A contributor PR must do one of:
1. link a maintainer-**`accepted`** issue it fixes, or
2. be (or reference) an **accepted RFC**, or
3. qualify for the **trivial fast-lane**.

**Trivial fast-lane** — these may be opened directly, no prior issue/RFC:
typo and wording fixes, documentation corrections, dependency bumps, comment
fixes, and obviously-correct one-line CI tweaks. When in doubt, open an Issue or
Discussion first; a PR that turns out to be non-trivial will be asked to.

A substantive PR with no backing issue/RFC will be closed with a pointer to the
right channel — not as a judgment of the idea, but to keep design discussion
where it's reviewable.

## What maintainers do *not* gate
Maintainers' own changes do not pass through the intake gates above — the team
runs a separate internal process. The universal gates (review, branch
protection, CI) apply to everyone. Enforcement of the intake rules is, to
start, **by convention and review** (PR template + labels); an automated check
keyed to author association may be added later if volume warrants.

## Code of conduct & security
- Conduct: [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).
- Security issues are **not** public Issues — see [SECURITY.md](SECURITY.md).

## Changing this document
Governance changes the same way code does: a pull request, reviewed by
maintainers. This file describes the external surface; the internal maintainer
process is intentionally out of scope here.
