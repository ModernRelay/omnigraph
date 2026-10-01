# Documentation guide

Documentation is part of OmniGraph's public contract. Keep each fact in one
place, at the narrowest audience that needs it.

## Content ownership

| Location | Owns | Does not own |
|---|---|---|
| `docs/user/` | Supported behavior, concepts, workflows, configuration, limits, and operator action | Code structure, recovery protocols, test evidence, design history |
| `docs/dev/` | Current architecture, invariants, support boundaries, and how to change/test the system | Roadmaps, stale implementation plans, release history |
| `docs/rfcs/` | Proposals, decisions, rationale, alternatives, evidence, and disposition | The current user manual or a second implementation reference |
| `changelog.d/` | Permanent notes written with each user-visible change | Current user instructions |
| `docs/releases/` | Published release history and generated version snapshots | Evergreen instructions |
| Code and tests | Exact types, fields, constants, and executable assertions | Long-form product guidance |
| Issue tracker | Open work, sequencing, and ownership | Current architecture |

Git history is the archive for superseded drafts. Do not keep a stale design in
`docs/dev/` merely because it may be interesting later.

## User documentation

Write for someone using the CLI, HTTP API, or deployment—not for a contributor
reading the implementation.

- Lead with the task or guarantee.
- Show one canonical command or request, then link to the reference.
- Explain public consequences of atomicity, recovery, and indexing without
  naming internal tables, structs, sidecars, or protocol generations.
- Document supported behavior only. Put planned work in an issue or draft RFC.
- State limits when they change what a user must do; omit internal tuning and
  implementation evidence.
- Give a concept one owner. Other pages use a sentence and a link rather than a
  second explanation.
- Prefer current canonical names. Mention deprecated aliases only in a compact
  compatibility section.

Most authored user pages should fit in 80–220 lines. Longer reference material
must earn its size and should be generated from the defining schema when
practical.

## Developer documentation

Developer docs answer three questions: what is true now, why must it remain
true, and where should a change be made and tested?

- Describe stable components and flows, not mutable source line numbers.
- Link to code for exact serialized shapes and constants.
- Link to an RFC for rationale, rejected alternatives, or historical evidence.
- Keep benchmarks and migration evidence with the release or RFC that used
  them; do not grow an evergreen ledger.
- Remove a plan after it lands. Transfer only the durable outcome into current
  architecture or invariants.
- Name unsupported boundaries plainly without presenting a speculative design
  as current architecture.

## RFCs

Every project RFC lives directly under `docs/rfcs/` and follows the metadata,
filename, lifecycle, and template in [the RFC guide](../rfcs/README.md). There
is one namespace and one lifecycle for public and maintainer-authored RFCs.

An RFC remains a decision record after implementation. Update its disposition,
but keep day-to-day instructions in user or developer docs.

## Release notes

Add one permanent `changelog.d/<descriptive-slug>.<category>.md` file with each
user-visible change. No PR number or release number is needed. Internal-only
changes need no note. Write the outcome in one to three sentences and link to
the guide for detail; keep necessary migration instructions even when longer.
Review the note with its code. There is no separate release-note approver.

| Suffix | Section |
|---|---|
| `breaking` | Upgrade actions: who is affected and what to do |
| `added` | Features |
| `changed` | Behavior changes |
| `fixed` | Fixes |
| `performance` | Performance |
| `deprecated` | Deprecations |
| `removed` | Removals |

Start the file with a Markdown bullet and end it with a newline. Examples,
nested lists and code spans stay intact. Local links use reference definitions
at column zero, with a relative destination and optional heading anchor:

```markdown
- Queries accept the new predicate. See the [query guide][predicate-guide].

[predicate-guide]: ../docs/user/queries/index.md
```

Give reference labels names unique to the note; labels are case-insensitive.
Definitions occupy one line and have no optional title. Local inline links,
fragment-only links and URL query parameters on local links are refused; name
the destination document explicitly. Inline external URLs are allowed. Escape
a literal backtick or close its code span. The composer only rewrites link
definitions outside code, leaving the rest of each fragment intact. Do not
add release headings to fragments.

Preview local edits and untracked notes from anywhere inside the checkout:

```bash
python3 scripts/release_notes.py preview --working-tree
python3 scripts/check-docs.py
```

For a committed preview, use `preview --target HEAD`. The existing documentation
CI job uploads this output as `release-notes-preview`; no generated preview is
checked in and no bot comment is needed.

`changelog.d/release.json` names the next `version`, the previous release `base`
for that maintenance line, and an optional `legacy` baseline SHA. These are
explicit inputs, not inferred from the newest tag. Both Git revisions must be
available with enough history to prove ancestry. The selected notes are paths
present in the target tree and absent from the base tree. Content comes from
the target; category and filename ordering are deterministic.

An unreleased note can be edited or removed with a reverted change. Once a
release includes it, retain its path and bytes: a correction or reversal gets
a new note. A backport carries the same path and uses that branch's previous
release. The checker validates selected notes' links against the selected tree;
historical raw notes are not checked against today's moving documentation.

Release preparation creates a versioned snapshot from committed inputs and an
explicit date. For the first v0.12.0 snapshot only:

```bash
python3 scripts/release_notes.py snapshot --target HEAD --date 2026-10-01 --replace-legacy
```

For later releases omit `--replace-legacy`. `--replace` regenerates an existing
generated snapshot before its tag exists. Nothing moves, deletes or stages
fragments. `--base REF` and `--version vX.Y.Z` explicitly override configuration
for a selected run; `--initial-release` is the only mode without a previous
release. Configuration itself remains part of the recorded inputs.

The snapshot records the selected target SHA, base SHA and note digests. It need
not name the commit that later includes the snapshot itself. Validation requires
that recorded target to be an ancestor of the audited release source, with the
same note inputs and configuration. Any later note/configuration change requires
regeneration. Verify a prepared committed snapshot with:

```bash
python3 scripts/release_notes.py verify --version v0.12.0 --target HEAD
```

The stable publisher validates the snapshot from its audited checkout and renders
its local links at the release tag. Versions before v0.12.0 retain asset-only
backfills; v0.12.0 onward require a valid snapshot. Edge releases are unchanged.
After publication, update the configuration for the next release: set its base
to the release just published, advance the version and set `legacy` to `null`.

The existing v0.12.0 document remains a frozen, one-time migration baseline,
pinned to its original Git source. New entries go in `changelog.d/`. Its original
body precedes new sections in the first snapshot; later releases list upgrade
actions first. Existing published documents and URLs stay unchanged.

## Review checklist

Before merging documentation:

1. Verify behavior against current code, CLI help, OpenAPI, and existing tests.
2. Check whether another page already owns the concept.
3. Remove future promises and internal detail from user docs.
4. Remove stale plans, evidence ledgers, and duplicated RFC content from
   developer docs.
5. Use relative Markdown links and run the documentation checks.
6. Read the rendered diff for examples, headings, and scanability.

```bash
bash scripts/check-agents-md.sh
python3 scripts/check-docs.py
typos   # from the repository root; exemptions in .typos.toml
```
