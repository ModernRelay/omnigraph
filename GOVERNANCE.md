# Governance

How change enters OmniGraph: who decides, what each change needs before code
lands, and what reviewers and CI enforce. It exists so an outside contributor
can answer, without asking: *where does my report or idea go, who decides, and
what has to happen before code lands?* The practical how-to lives in
[CONTRIBUTING.md](CONTRIBUTING.md); this file is the rules behind it.

## Roles

OmniGraph is maintained by EQTR, Inc.

- **Maintainers** — triage issues, accept or decline proposals and RFCs,
  review and merge PRs, and set direction. Final decision authority.
- **Contributors** — everyone else. Report bugs, propose features, author
  RFCs, and open pull requests.

The intake gates below govern contributions. The universal gates — review,
branch protection on `main`, and CI (see
[docs/dev/branch-protection.md](docs/dev/branch-protection.md)) — apply to
everyone, maintainers included.

## One channel in

**Issues are the only inbound channel.** Use the bug form for broken existing
functionality and the feature or design proposal form for new capabilities or
changes to intended behavior or architecture. Maintainers decide whether the
same issue needs an RFC; there is no separate RFC intake form.
GitHub Discussions are not used. The one exception is
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
  dependency, anything irreversible, or anything that changes a
  [decision record](docs/decisions/README.md) or a statement of
  [the contract](docs/dev/contract.md).
  Path: **issue → `accepted` → RFC pull request merged → code PRs**.

If you cannot tell S from M, it is M. A reviewer who reclassifies a change
upward closes the PR and restarts it on the correct path — that is about
process, not the merit of the change.

## Issue lifecycle

Labels are the state machine; an issue has exactly one status label.

- `needs-triage` — default on arrival. No work should start.
- `accepted` — a maintainer agreed the change should exist; a code PR may
  follow for size M. Size L still requires a merged RFC.
- `needs-rfc` — size L, accepted with the design outstanding. It replaces
  `accepted` while the RFC is written and reviewed; the issue returns to
  `accepted` only when the RFC merges.
- `blocked` — accepted, but waiting on something named in the thread.
- `declined` — closed, with a reason.

`accepted` precedes implementation effort — including the effort of writing
an RFC. Its purpose is that nobody invests before a maintainer has agreed the
change should exist. An accepted issue with no assignee is open to anyone.

## RFCs

An RFC is a pull request, not a file. It proposes a design before the code
exists, and its title has the type `rfc`. Its description carries the
problem, the alternatives and the argument; its thread carries the review.
The description opens with a short summary a person can read in a minute;
the exact statements for agents follow it.

What it merges is small:

- the statements of [the contract](docs/dev/contract.md) that the design
  adds or changes, each with a row under Gaps until the code follows;
- one [decision record](docs/decisions/README.md) of one page: what was
  chosen, why, and what was rejected;
- an entry of the [deny-list](docs/dev/invariants.md#deny-list), when the
  design rules a shape out.

[The documentation guide](docs/dev/documentation.md#decisions) owns the
rules for records.

- **Merge is acceptance.** Nothing on `main` is a draft. An RFC that is not
  accepted stays an open pull request or is closed.
- **A merged RFC sanctions its implementation PRs directly**: code PRs
  reference the decision record, and the RFC's issue is the umbrella that
  tracks them.
- **Changing a decision is itself a size-L change**: issue → `accepted` →
  RFC pull request → code. Discovering mid-implementation that the design is
  wrong is normal and welcome: stop, change the record and the contract in
  their own pull request, then continue.
- **A record follows the code.** The pull request that completes or removes
  a design updates or deletes its record in the same change.

## Pull requests

### Draft vs ready

A **draft** PR is work shared for visibility: nobody is expected to review
it, gates need not be green, and no backing issue is required. Use drafts
freely — prototyping an approach, exploring a design space, cross-checking a
measurement, sharing direction before it is right. A **ready** PR requests
review and must meet everything below. Do not leave finished work in draft:
draft means unfinished, not unreviewed.

### A ready PR must

- reference its `accepted` issue or a decision record — or be size S;
- keep to one kind of content: an RFC PR touches only the contract, a
  decision record and the deny-list; a contributor code PR does not change
  what a decision record chose (change the decision first, then the code);
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

Review enforces issue linkage. CI also validates decision records, page budgets,
metadata, lifecycle values, registry entries, and documentation links with
`scripts/check-docs.py`.

## Code of conduct & security

- Conduct: [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).
- Security issues are **not** public Issues — see [SECURITY.md](SECURITY.md).

## Changing this document

Governance changes the same way code does: a pull request, reviewed by
maintainers.
