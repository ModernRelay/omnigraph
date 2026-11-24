# Documentation guide

**Audience:** anyone, an agent included, who writes or changes a document
**Authority:** which documents exist, what each may hold, and the checks that
keep them small

Agents write the code here and read the documents before every change. A
person reads little of either. So a document earns its place only by holding
what the code cannot say, and a long or partly stale page is worse than no
page: an agent takes all of it as true.

## The test

Before writing a sentence, ask: **can the code say this itself?** If it can,
the sentence is a second copy that goes stale. Put it in the code, as a type,
a test or a doc comment, and leave it out of `docs/`.

The code cannot say four things:

1. What a caller is promised, before the code keeps the promise.
2. What the system is optimized for.
3. What an implementation must never do, when nothing in the code refuses it.
4. Why a design was chosen, and what was turned down.

Each has one home below. A fact has one owner; every other page cites it.

## Kinds of document

| Kind | Where | Holds | Loaded | Budget |
|---|---|---|---|---|
| Contract | [contract.md](contract.md) | What each operation promises its caller: the target, the gaps and the open decisions | Every session | 53,000 bytes |
| Work | [workload.md](workload.md) | What the system is optimized for, and the evidence | On demand | 36,000 bytes |
| Rules | [invariants.md](invariants.md) | What an implementation must never do, each with its reason | Every session | 11,000 bytes |
| Map | [`AGENTS.md`](../../AGENTS.md) | Which crate owns what, which page to read, the commands and the change discipline | Every session | 15,000 bytes |
| Decision | [`docs/decisions/`](../decisions/README.md) | Why one design was chosen and what was rejected | On demand | 5,000 bytes each |
| Testing rules | [testing.md](testing.md) | Where a test goes and the rules for adding one | Every session | 7,500 bytes |
| Guide | the rest of `docs/dev/` | What spans more than one module and so has no single doc comment to live in | On demand | 60,000 bytes each |
| User guide | `docs/user/` | Supported behavior, for someone who uses the CLI, the HTTP API or a deployment | By users | 350 lines each |
| Release note | `changelog.d/`, `docs/releases/` | What changed for a user, written with the change | By users | see [Release notes](#release-notes) |

`scripts/check-docs.py` holds the budgets and fails a page that passes its
own. A page over budget is split by owner or cut; a budget is raised only in
a pull request whose description says what the page could not lose.

Nothing else is kept:

- **How a mechanism works** is the module's own doc comment, beside the code
  it describes, reviewed in the same diff. A guide keeps only what crosses
  modules: an order of steps, an authority boundary, a protocol.
- **History** is git. No "previously", no account of how a design changed.
- **Plans and status** are issues. No phase list, no "not yet", no "next".
- **Evidence** is a checked-in test or benchmark. A page names it and repeats
  none of its numbers.
- **Proposals** are pull requests. Nothing on `main` is a draft.

## Two readers

Every page is written for an agent first: exact, complete, one term for one
meaning. Precision comes before ease of reading.

A page that every session loads also opens with a section for a person, in
[ASD-STE100 Simplified Technical English](contract.md#editing-this-page):
sentences of 25 words or fewer, active voice, simple tenses, each term
defined at its first use. It adds no fact. It changes in the same pull
request as the statement it summarizes.

## Writing a page

- Open with **Audience** and **Authority**: who reads the page, and which
  facts it owns and which it leaves to another page.
- State what is true now, in the present tense. A statement about the code
  is checked against the code before it is written.
- Name a thing by what it is. A file path, a type or a test name is better
  than a description of it.
- Cite [the contract](contract.md#citing) by address (`E2`, `write:3`,
  `G2`) wherever a page relies on a promise. Do not restate the promise.
- Do not cite a pull request, an issue, a date or a person as the reason for
  a statement. Give the reason.
- Do not write a source line number. It is stale after the next edit.
- Spell the product `Omnigraph`.

## Decisions

A decision record answers one question for an agent about to change a
design: was this shape chosen on purpose, and what was already tried? The
format is in [`docs/decisions/README.md`](../decisions/README.md).

- **When one is needed.** A change that is costly to reverse: a storage or
  wire format, a recovery protocol, a query-language or API contract, a new
  substrate dependency, or an exception to the
  [deny-list](invariants.md#deny-list). The record lands in the pull request
  that makes the decision, with the contract statements it changes. An
  irreversible storage, protocol or substrate decision also lands its
  compatibility, refusal, crash and rebuild evidence as tests
  ([invariant 13](invariants.md#hard-invariants)).
- **A proposal is a pull request**, an RFC, titled `rfc: …`
  ([GOVERNANCE.md](../../GOVERNANCE.md#rfcs)). Its description carries the problem, the
  alternatives and the argument, and its thread carries the debate. To agree
  on a design before the code exists, open a pull request that changes the
  contract and adds the record: the contract is the target, so its statement
  may land first, with a row under [Gaps](contract.md#gaps) until the code
  follows.
- **One decision, one record.** A record that a later decision replaces is
  deleted in the same pull request; the later record's **Rejected** section
  says what the earlier design was and why it lost.
- **A rejected idea is one line.** When a design is turned down and likely
  to be proposed again, it becomes an entry of the
  [deny-list](invariants.md#deny-list): what not to build, and why.
- **A record follows the code.** A change that makes a record's statement
  false changes the record in the same pull request. A record whose design
  is fully removed is deleted.
- **Code cites a record by its file name.** A comment that cites `RFC NNNN`
  names the record whose **Cited as** line carries that label. Many older
  comments cite a number that has no record, in a dashed three-digit form,
  because the design was replaced: when you change such code, replace the citation
  with the reason in words.

The design files that came before this system, under `docs/rfcs/`, are in
git history. The last commit that holds them is `65e38056c`:

```bash
git show 65e38056c:docs/rfcs/README.md
```

They are history, not guidance: many describe designs that were never built
or were replaced.

## User documentation

Write for someone using the CLI, HTTP API, or deployment, not for a
contributor reading the implementation.

- Lead with the task or guarantee.
- Show one canonical command or request, then link to the reference.
- Explain public consequences of atomicity, recovery, and indexing without
  naming internal tables, structs, sidecars, or protocol generations.
- Document supported behavior only. Planned work is an issue.
- State limits when they change what a user must do; omit internal tuning and
  implementation evidence.
- Give a concept one owner. Other pages use a sentence and a link rather than a
  second explanation.
- Prefer current canonical names. Mention deprecated aliases only in a compact
  compatibility section.
- Do not link to a decision record. A user page states the behavior itself.

Reference material should be generated from the defining schema when
practical.

## Where a fact goes

| The fact | Goes in |
|---|---|
| A caller can observe it | [The contract](contract.md), then the user guide |
| The code must never do it | [Invariants](invariants.md) |
| Why this design, and not that one | A [decision record](../decisions/README.md) |
| How one module works | That module's doc comment |
| How several modules work together | The guide that owns the flow ([index](index.md)) |
| Which suite tests it | [Test ownership](test-ownership.md) |
| A Lance behavior relied on | [Lance compatibility fences](lance-fences.md) |
| A measurement | A checked-in test or benchmark |
| Work that is not done | An issue |
| What changed for a user | A `changelog.d` note |

Do not create a new page for a fact this table places. A new guide needs a
flow that no existing guide owns.

## Documentation tools

Use Python 3.11 or newer. From the repository root, create and activate a local
environment before running the documentation checker or release-note commands:

```bash
python3 -m venv .venv-docs
source .venv-docs/bin/activate
python3 -m pip install -r scripts/requirements-docs.txt
```

The requirements pin `markdown-it-py` and its `mdurl` dependency. The checker and
composer share its CommonMark parser, with table and strikethrough support, so
links in nested lists and code examples have the same meaning in both tools.
CI installs these same dependencies in a temporary environment. Reuse the local
environment on later runs; reinstall when the requirements change.

## Release notes

Add one permanent `changelog.d/<descriptive-slug>.<category>.md` file with each
user-visible change. No PR number or release number is needed: the release
page links each note to the pull request whose squash commit added it.
Internal-only changes need no note. A note is one bullet with one sentence or
two, at most 200 characters of visible text (code spans count, link
destinations and definitions do not); move detail into the guide it links.
A breaking note may use 400 characters, says who is affected and what to do,
and links the guide that holds the steps, usually the
[upgrade guide](../user/operations/upgrade.md). Review the note with its code.
There is no separate release-note approver.

The suffix names the release-page section, as `CATEGORIES` in
`scripts/release_notes.py` maps it (`breaking` prints as Upgrade actions).

Local links use reference definitions; `scripts/release_notes.py` refuses
every other note shape with a message that names the fix:

```markdown
- Queries accept the new predicate. See the [query guide][predicate-guide].

[predicate-guide]: ../docs/user/queries/index.md
```

Inline external URLs are allowed.

### The release file

The release-prep pull request adds `changelog.d/<version>.md`, named for the
`version` in `release.json`. `scripts/release_notes.py` refuses a release
file whose headings, word counts or highlight count are out of shape, and its
message names the limit. It opens with an intro that
says what users get from the release and, when it breaks something, what
they must do before they upgrade. Write the intro in
[ASD-STE100](https://www.asd-ste100.org/) Simplified Technical English: short
sentences (at most 25 words), active voice, simple present, one statement per
sentence and no slogans. When the release has any breaking note, a
`## Why these changes` section says why the release asks
users to act; it is printed at the top of the upgrade actions.

A `## Highlights` section holds `### ` highlights. The heading names the change and what it gives users in a few words, with a
measured number when there is one (`CSV import: 630 million rows in under
five minutes`, not `CSV loads go through the planner`). The body opens with
what changes for the reader: what was slow, refused, wrong or impossible
before, who noticed, and what they can do now. Then it names the mechanism in
database terms (projection pushdown, copy-on-write branches, compare-and-swap
publication), names the benchmark behind any number and the machine and
storage it ran on, says when a number was not measured on a cloud store, and
lists its pull requests:

```markdown
### Counting no longer loads every embedding

A `count` over a type with embeddings used to read every vector, so it slowed
down as the vectors grew. It now reads none. Projection pushdown: a `Vector`
or `Blob` column is read only when the query names it. #760, #795
```

A note follows the same order in fewer words: say what the reader notices
(`a read no longer waits for a write to commit`), not only what the code does
(`branch state installs by pointer swap`). When the notes of a release are
still hard to act on after it is tagged, add its
[overview](#release-overviews) rather than editing the snapshot.

The release-prep pull request may leave `## Highlights` empty for someone else
to write: the preview shows them as still to be written and CI passes, but the
snapshot refuses a minor or major release until three to five are in.

Before the snapshot, the release-prep pull request also reads the cycle's
notes as one page. Notes stay editable until their release is tagged, so when
several notes describe one upgrade step (a saved-plan version, a storage
format), keep one that states the final value and delete the others, and say
each fact once.

Notes print as one list per section, in file-name order. To put the most
important ones first, add a `## Note order` section listing note file names
(`- storage-format-14.breaking.md`); listed notes lead their
sections in that order, the rest follow by file name, and the section itself
is not printed. A `## Contributors` section lists the GitHub handles to thank,
and the page prints them as one thank-you line. List the pull request authors
since the previous release with
`gh api repos/ModernRelay/omnigraph/compare/<previous>...<target> --paginate --jq '.commits[].author.login' | sort -u`,
drop bots, and add anyone else worth thanking.
Links follow the note rules. The file is immutable once its release is tagged.

Wrap notes and the release file as you like: the page joins each paragraph
into one line, because GitHub shows every newline in a release body as a line
break. Code blocks and hard line breaks keep their lines.

### Pull request titles

The `PR Title` check holds the title to `type(scope)!: description`
([CONTRIBUTING.md](../../CONTRIBUTING.md), Pull Requests);
`scripts/check-release-note.py` refuses a typed change without the note its
type needs, and says which. A maintainer
adds the `skip-changelog` label when a typed change has nothing users can see.
The squash commit keeps the title and its `(#NNN)`, which is where the release
page's pull request links come from.

### Preview, snapshot and publication

`python3 scripts/release_notes.py --help`, and each command's `--help`, say
what the script selects, checks and records. Preview local edits and
untracked notes from the repository root after
[activating the documentation environment](#documentation-tools):

```bash
python3 scripts/release_notes.py preview --working-tree
python3 scripts/check-docs.py
```

For a committed preview, use `preview --target HEAD`. The existing
documentation CI job uploads this output as `release-notes-preview`; no
generated preview is checked in and no bot comment is needed.

An unreleased note can be edited or removed with a reverted change. Once a
release includes it, a correction or reversal gets a new note. A backport
carries the same path and uses that branch's previous release.

With `changelog.d/<version>.md` written, release preparation creates the
versioned snapshot and updates `docs/releases/README.md`. Replace the example
date with the intended release date:

```bash
python3 scripts/release_notes.py snapshot --target HEAD --date 2026-10-01
```

Verify a prepared committed snapshot, and repair only the index (also after a
version is tagged):

```bash
python3 scripts/release_notes.py verify --version v0.12.0 --target HEAD
python3 scripts/release_notes.py index --write
```

The stable publisher validates the snapshot from its audited checkout and renders
its local links at the release tag. Versions before v0.12.0 retain asset-only
backfills; v0.12.0 onward require a valid snapshot. Edge releases need no
snapshot: each shows the unreleased preview at its commit
([release workflows](ci.md#release-workflows)).
After publication, update the configuration for the next release: set its base
to the release just published, advance the version, set `legacy` to `null` and
keep `"format": 2`.
Advance the workspace package version too: edge builds are refused while it is
not above the latest stable release ([release workflows](ci.md#release-workflows)).

### Release overviews

A release may also have a curated `docs/releases/vX.Y.Z-overview.md`, linked
from the generated release index. Reconcile superseded fragments against the
code at the release tag and explain the final shipped behavior and upgrade
actions. Write it for a reader deciding whether and how to upgrade: for each
upgrade action, who is affected, what changes for them and why, and what to do;
for each other change, what was slow, refused or wrong before and what works
now. Keep the release page's sections, order and pull request links, so a
reader finds each change where earlier releases put it. Link the complete
immutable `vX.Y.Z.md` snapshot from the overview;
do not edit the published snapshot or source notes to resolve contradictions.
The overview is a maintained guide to that release, not a replacement for its
recorded history. After adding it, regenerate the index with
`python3 scripts/release_notes.py index --write`.

### Release checklist

Every release runs these steps in order.

1. Confirm the cycle: `changelog.d/release.json` names the version, its `base`
   and `"format": 2`.
2. Smoke-test the candidate: install the edge build of the commit to be
   released ([release channels](../user/install.md#release-channels)) and run
   the [quickstart](../user/quickstart.md) against a fresh local graph.
3. Open the release-prep pull request. Run
   `python3 scripts/release_notes.py preview --working-tree`, read the page
   against the docs at that commit, fix each stale note in its own file, and
   merge notes that repeat one upgrade step.
4. Add `changelog.d/<version>.md` to that pull request: the intro, the why
   when anything breaks, the highlights, the contributors and, if useful, the
   note order. An agent can draft the highlights from the cycle's notes; run
   `preview` again and read the rendered page.
5. Review the pull request against the rendered preview, not only the
   files.
6. After the review, run
   `python3 scripts/release_notes.py snapshot --target HEAD --date <release date>`,
   commit the snapshot and the index to the same pull request, and merge it.
7. Tag the merged commit `<version>` and push the tag. Tag CI audits it and
   `release.yml` publishes the archives and the GitHub release from the
   snapshot. Do not edit the published page or its notes; correct a mistake
   with a new note or in the release's overview.
8. Open the next cycle: update `release.json` and the workspace package
   version as described above.

## Review checklist

Before merging documentation:

1. Could the code say this itself? Then it does not belong in `docs/`.
2. Verify behavior against current code, CLI help, OpenAPI, and existing tests.
3. Check whether another page already owns the fact; cite it, do not repeat it.
4. Remove plans, status, history, evidence ledgers and internal detail.
5. Check the page against its budget.
6. Use relative Markdown links and run the documentation checks.

After [activating the documentation environment](#documentation-tools):

```bash
bash scripts/check-agents-md.sh
python3 scripts/check-docs.py
typos   # from the repository root; exemptions in .typos.toml
```
