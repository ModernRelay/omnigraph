<!--
  Thanks for contributing! See CONTRIBUTING.md and GOVERNANCE.md.
  A substantive PR needs a backing accepted issue or accepted RFC.
  Title: `type(scope)!: lowercase description` (types and examples in
  CONTRIBUTING.md, Pull Requests); the `PR Title` check refuses anything else.

  The description is written for people: the reviewer now, and whoever follows
  a commit back here in a year. Prose, one short paragraph per question, no
  headings, lists or checkboxes. Like this (#1141):

  Closes #1083. Every `load csv` overwrite runs a removal scan that only a
  retained-edge check reads, so on a fresh graph it is an unnecessary check on
  every statement: 88 s of SF30's 261 s load after #1134, collecting an empty
  set.

  The planner now decides it per statement: `scan` where a retained-edge check
  consumes the removed ids, `none` where none does, and no retained-edge check
  on a target with no committed rows. The engine refuses a `none` plan whose
  target holds rows.

  Measured on the five large SF30 statements, release builds of `60bfd19d` and
  this branch, one round each: load 155.4 → 104.5 s, validate 82.5 → 35.2 s;
  parse, write and capture unchanged.

  For review: the decision to challenge is the guard, a `none` plan on a
  target that holds rows fails as `manifest_internal` rather than falling
  back to a scan; a target emptied by a branch or a restart is the input that
  would expose a wrong count. Under `none` nothing is charged to the
  removed-id allowance of `write_max_bytes`, so an edge overwrite the
  allowance refused before is admitted; docs and release note say so. Explain
  JSON gains `removal` on the physical `Validate` row, additive, same
  `explain_version`; store format unchanged.

  The four questions, in this order:

  1. What was wrong or missing, and what it cost; open with `Closes #N` or the
     RFC path. A trivial change (typo, docs, comment, dependency bump,
     one-line CI; CONTRIBUTING.md) has neither and deletes the `Closes #`
     line. A feature: what could not be done. A fix: what failed, on which
     input. A refactor: what made the next change hard. Docs or CI: what a
     reader or contributor ran into.
  2. What changes, where, and the one guard that keeps it correct.
  3. How you know: a measurement with its protocol, the test that fails
     without the change, the behaviour a refactor's existing tests still pin,
     or the command or page that now behaves as described.
  4. "For review:" the decision to challenge and what would make it wrong;
     what you are least sure of and the input that exposes it; what the checks
     cannot judge: an existing store still reads, openapi.json or cluster.yaml
     shape, a changed default, a run CI does not do and its result, a check
     that could not run and why. Skip only when there is nothing.

  Precise terms. Name no command the checks tab runs. A user-visible change
  carries a changelog.d/<slug>.<category>.md note; the `PR Title` check asks
  one of every `feat`, `fix` and `perf` title unless the pull request carries
  the `skip-changelog` label.
-->

Closes #
