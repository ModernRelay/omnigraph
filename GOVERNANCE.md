# Governance

How change enters OmniGraph: who decides, what each change needs before code
lands, and what reviewers and CI enforce. It exists so an outside contributor
can answer, without asking: *where does my report or idea go, who decides, and
what has to happen before code lands?* The practical how-to lives in
[CONTRIBUTING.md](CONTRIBUTING.md); this file is the rules behind it.

## Roles

- **Maintainers** — triage issues, accept or decline proposals and RFCs,
  review and merge PRs, and set direction. Final decision authority.
- **Contributors** — everyone else. Report bugs, propose features, author
  RFCs, and open pull requests.

The intake gates below govern contributions. The universal gates — review,
branch protection on `main`, and CI (see
[docs/dev/branch-protection.md](docs/dev/branch-protection.md)) — apply to
everyone, maintainers included.

## One channel in

**Issues are the only inbound channel.** Bugs and feature proposals each have
an issue form; GitHub Discussions are not used. The one exception is
security: report vulnerabilities privately per [SECURITY.md](SECURITY.md),
never as a public issue — a public security issue is closed on sight and
re-routed.

## Change sizes

The lane is set by **blast radius and design impact**, not line count.

- **S — trivial.** Clearly broken, small blast radius, no design impact.
  Open a PR directly; no issue required.
- **M — feature or fix.** A real change, consistent with the existing
  design. Path: **issue → `accepted` → PR**.
- **L — design.** Creates or changes a design: new user-facing surface
  (query/schema/CLI/HTTP), on-disk or wire formats, a new substrate
  dependency, anything irreversible, or anything touching an accepted RFC.
  Path: **issue → `accepted` → RFC PR → merged → code PRs**.

If you cannot tell S from M, it is M. A reviewer who reclassifies a change
upward closes the PR and restarts it on the correct path — that is about
process, not the merit of the change.

## Issue lifecycle

Labels are the state machine; an issue has exactly one status label.

- `needs-triage` — default on arrival. No work should start.
- `accepted` — a maintainer agreed the change should exist; a code PR may
  follow. For size L, this is also the state after the RFC merges.
- `needs-rfc` — size L, accepted with the design outstanding. It replaces
  `accepted` until the RFC PR merges (writing the RFC is authorized); the
  issue then returns to `accepted`, and code may start.
- `blocked` — accepted, but waiting on something named in the thread.
- `declined` — closed, with a reason.

`accepted` precedes implementation effort — including the effort of writing
an RFC. Its purpose is that nobody invests before a maintainer has agreed the
change should exist. An accepted issue with no assignee is open to anyone.

## RFCs

An RFC lives in two vessels: its **issue** (discussion, then implementation
tracking) and its **document** (`docs/rfcs/NNNN-title.md`). Numbering,
statuses, and the template live in
[docs/rfcs/README.md](docs/rfcs/README.md).

- **Merging the RFC PR is the green light to write code.** An accepted issue
  alone is not enough for an L change — it only authorizes writing the RFC.
- **A merged RFC sanctions its implementation PRs directly**: code PRs
  reference the RFC, and the RFC's issue is the umbrella that tracks them.
- **Amending an accepted RFC is itself a size-L change**: issue → `accepted`
  → amendment PR → merged → code. Discovering mid-implementation that the
  design is wrong is normal and welcome — stop, amend, then continue. A code
  PR that rewrites an accepted RFC on the way through is rejected regardless
  of the code's quality.

## Pull requests

### Draft vs ready

A **draft** PR is work shared for visibility: nobody is expected to review
it, gates need not be green, and no backing issue is required. Use drafts
freely — prototyping an approach, exploring a design space, cross-checking a
measurement, sharing direction before it is right. A **ready** PR requests
review and must meet everything below. Do not leave finished work in draft:
draft means unfinished, not unreviewed.

### A ready PR must

- reference its `accepted` issue or a merged RFC — or be size S;
- keep to one kind of content: an RFC PR touches only `docs/rfcs/`; a
  contributor code PR does not modify RFC documents (amend first, then code);
- be green on CI (build, `fmt`, `clippy`, tests, and the repo's drift
  checks);
- describe its blast radius and what was tested beyond the happy path.

Review and merge are maintainer decisions.

## Enforcement

A PR that skips this process is closed plainly, with a link to this document
— not as a judgment of the idea, but to keep design discussion where it is
reviewable. Reclassified changes restart on the correct path rather than
being renegotiated in the thread. Low-effort PRs with no evident mental model
and no testing beyond the happy path may be closed without detailed review.

Enforcement is by convention and review to start; automated checks (an
RFC-path guard, an issue-link check) may be added as volume warrants.

## Code of conduct & security

- Conduct: [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).
- Security issues are **not** public Issues — see [SECURITY.md](SECURITY.md).

## Changing this document

Governance changes the same way code does: a pull request, reviewed by
maintainers.
