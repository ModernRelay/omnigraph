#!/usr/bin/env python3
"""Every pull request title is a Conventional Commits subject.

Every merge into `main` is a squash through the merge queue, and the squash
subject is the pull request title, so the title is the one line `git log`
and the release tooling keep for the change. This check holds it to
`type(scope)!: description`: one type from TYPES, an optional lowercase
scope list in parentheses, `!` for a breaking change, a colon and one space,
then a description that starts with a non-capital, carries no trailing
period, and keeps the whole title within MAX_LEN characters; the only
whitespace a title may contain is the plain space. GitHub's own
`Revert "…"` title and the `docs(rfc):` spellings are refused with the one
accepted form in the message, so `git log` greps as one history.

`--title TITLE` checks a pull request title. `--squash-subject MESSAGE`
checks the commit message the merge queue is about to land: the first line,
with GitHub's ` (#N)` suffix removed, is the title. A failure prints one
line naming what failed, the accepted shape and two examples, and exits 1.
`--self-test` runs the accepted and refused fixtures, including the untyped
titles that reached `main` before this check existed.
"""

from __future__ import annotations

import argparse
import os
import re
import sys

TYPES = (
    "feat",
    "fix",
    "perf",
    "refactor",
    "docs",
    "test",
    "ci",
    "build",
    "revert",
    "rfc",
    "release",
)
MAX_LEN = 100
SHAPE = "type(scope)!: description"
EXAMPLES = (
    "feat(server): return exact merge publication receipts",
    "fix(merge): decide fast_forward by the target's state",
)

TITLE = re.compile(r"^(?P<type>[A-Za-z]+)(?:\((?P<scope>[^()]*)\))?(?P<bang>!?)(?P<colon>:?)(?P<rest>.*)$")
SCOPE = re.compile(r"^[a-z0-9_-]+(?:,[a-z0-9_-]+)*$")
PR_SUFFIX = re.compile(r" \(#\d+\)$")
GITHUB_REVERT = re.compile(r'^Revert\s+"')


def failure(title: str) -> str | None:
    """The reason `title` is refused, or None when it is accepted."""
    if title != title.strip():
        return "leading or trailing whitespace"
    if not title:
        return "empty title"
    if any(c.isspace() and c != " " for c in title):
        return "a tab, line break or other non-space whitespace in the title"
    if len(title) > MAX_LEN:
        return f"{len(title)} characters; at most {MAX_LEN}"
    if GITHUB_REVERT.match(title):
        return 'GitHub\'s `Revert "…"` title; use `revert: <what is undone>`'
    m = TITLE.match(title)
    if m is None or not m.group("colon"):
        return f"no `type:` prefix; the shape is `{SHAPE}`"
    type_ = m.group("type")
    if type_ not in TYPES:
        if type_.lower() in TYPES:
            return f"type `{type_}` is spelled in capitals; use `{type_.lower()}`"
        return f"unknown type `{type_}`; one of {', '.join(TYPES)}"
    scope = m.group("scope")
    if scope is not None:
        if type_ == "docs" and scope in ("rfc", "rfcs"):
            return f"`docs({scope}):` for an RFC; use `rfc:`"
        if not SCOPE.fullmatch(scope):
            return f"scope `({scope})` is not lowercase `a-z0-9_-` names separated by commas"
    rest = m.group("rest")
    if not rest.strip():
        return "empty description"
    if not rest.startswith(" ") or rest[1:2].isspace():
        return "exactly one space after the colon"
    desc = rest[1:]
    if desc[0].isupper():
        return f"description starts with a capital `{desc[0]}`; start lowercase (a backtick or digit also passes)"
    if desc.endswith("."):
        return "description ends with a period"
    return None


def squash_title(message: str) -> str:
    """The pull request title inside a merge-queue commit message."""
    subject = message.splitlines()[0] if message.strip() else ""
    return PR_SUFFIX.sub("", subject.strip())


def message(title: str, reason: str) -> str:
    return (
        f"PR title refused: {reason}.\n"
        f"  title:    {title!r}\n"
        f"  shape:    `{SHAPE}` with type one of {', '.join(TYPES)}; `(scope)` and `!` optional;\n"
        f"            description lowercase-first, no trailing period, at most {MAX_LEN} characters\n"
        f"  examples: {EXAMPLES[0]}\n"
        f"            {EXAMPLES[1]}\n"
        f"  rule:     CONTRIBUTING.md, Pull Requests"
    )


def check(title: str) -> int:
    reason = failure(title)
    if reason is None:
        print(f"PR title accepted: {title}")
        return 0
    text = message(title, reason)
    print(text)
    if os.environ.get("GITHUB_ACTIONS") == "true":
        print(f"::error title=PR Title::{reason}; the shape is `{SHAPE}` (CONTRIBUTING.md, Pull Requests)")
    return 1


ACCEPTED = (
    "feat(server): return exact merge publication receipts",
    "fix(merge): decide fast_forward by the target's state",
    "refactor(blob,schema)!: remove --allow-data-loss",
    "docs: correct the ledger v2 upgrade note",
    "rfc: search plan truth on engine v2",
    "fix(server): follow RFC 9110 for 503 retries",
    "release: v0.12.0",
    "revert: drop list membership",
    "docs: fix a broken link in the upgrade guide",
    "test(gq): cover inherited indexes after fast-forward",
    "ci: run slow GQT cases in a nightly workflow",
    "build: bump lance to 11.0.1",
    "fix: reject `DateTime` digits",
    "build: bump Lance to 11.0.1",
    "perf(catalog): release settled commits to immutable `__history` extents",
    "feat(gqt): add `--- concurrent` block, ordered sessions on one handle under DST",
    "fix!: 2-phase cursor keeps wide rows out of SortExec",
    "feat(engine): " + "x" * (MAX_LEN - len("feat(engine): ")),
)

REFUSED = (
    # Untyped titles that reached `main` before the check.
    ("fix (#898)", "no `type:` prefix"),
    ("Update Readme", "no `type:` prefix"),
    ("Enable server-owned v2 deployments without restart", "no `type:` prefix"),
    ("detached table commits replace recovery sidecars", "no `type:` prefix"),
    ("imp", "no `type:` prefix"),
    ("review", "no `type:` prefix"),
    ("Check docs conflict markers", "no `type:` prefix"),
    ("Clarify infrastructure support in README", "no `type:` prefix"),
    ("Merge pull request #680 from ModernRelay/codex/self-service-cluster-lifecycle", "no `type:` prefix"),
    ("Merge remote-tracking branch 'origin/main' into codex/revert-663-actor-provenance", "no `type:` prefix"),
    ("RFC 0066: Server lifecycle and online deployment", "no `type:` prefix"),
    ("compiler: the diagnostics contract for refused queries", "unknown type `compiler`"),
    ("engine: shared schema gate and the write critical section", "unknown type `engine`"),
    ("snapshot: read the storage-format stamp from the snapshot it returns", "unknown type `snapshot`"),
    ("cli: extend managed deadlines and prepare bounded transports", "unknown type `cli`"),
    ("auth: use cluster identities and applied policy for authorization", "unknown type `auth`"),
    ("bench: concurrent-writes throughput diagnostic", "unknown type `bench`"),
    ("merge: integrate main and allocate managed lifecycle RFC 0061", "unknown type `merge`"),
    ("chore(release): prepare notes for v0.13.1", "unknown type `chore`"),
    ("Governance: issue-first intake, Discussions retired", "unknown type `Governance`"),
    ("GQ: add edge alternation and wildcard traversal", "unknown type `GQ`"),
    ("RFC: Compatibility surfaces", "spelled in capitals"),
    ("docs(rfc): align RFC 0068 with detached-only tables", "use `rfc:`"),
    ("docs(rfcs): name new RFCs by creation date and slug", "use `rfc:`"),
    ("rfc: RFC 0047, search plan truth on engine v2", "starts with a capital"),
    # Shapes the rule refuses by construction.
    ("", "empty title"),
    (" feat: leading space", "whitespace"),
    ("feat: trailing space ", "whitespace"),
    ("feat:", "empty description"),
    ("feat(server)!:", "empty description"),
    ("feat:no space", "exactly one space"),
    ("feat:  two spaces", "exactly one space"),
    ("fix: \tCapital description", "non-space whitespace"),
    ("fix:  Capital description", "non-space whitespace"),
    ("fix: \rCapital description", "non-space whitespace"),
    ("fix:\ttab after the colon", "non-space whitespace"),
    ("feat: two\nlines", "non-space whitespace"),
    ("feat(server\n): newline in the scope", "non-space whitespace"),
    ("feat(server ): no-break space in the scope", "non-space whitespace"),
    ("feat(): empty scope", "scope `()`"),
    ("feat(Server): capital scope", "scope `(Server)`"),
    ("feat(server, api): space in scope", "scope `(server, api)`"),
    ("feat(server)!!: double bang", "no `type:` prefix"),
    ("feat(server) : space before colon", "no `type:` prefix"),
    ("Feat: capital type", "spelled in capitals"),
    ("FIX: capital type", "spelled in capitals"),
    ("feat: Capital description", "starts with a capital `C`"),
    ("feat: ends with a period.", "ends with a period"),
    ("chore: tidy", "unknown type `chore`"),
    ("style: reformat", "unknown type `style`"),
    ("WIP", "no `type:` prefix"),
    ("[WIP] feat: draft", "no `type:` prefix"),
    ('Revert "feat(gq): add list membership"', "use `revert: <what is undone>`"),
    ("feat(engine): " + "x" * (MAX_LEN + 1 - len("feat(engine): ")), f"at most {MAX_LEN}"),
)

SQUASH_SUBJECTS = (
    ("feat(server): own writes, bound admission, and coordinate shutdown (#824)\n\n* body line\n", "feat(server): own writes, bound admission, and coordinate shutdown"),
    ("release: v0.12.0 (#877)", "release: v0.12.0"),
    ("fix (#898)", "fix"),
    ("fix: keep a (#12) inside the text (#900)", "fix: keep a (#12) inside the text"),
    ("Update Readme (#881)\n", "Update Readme"),
    ("", ""),
)


def self_test() -> int:
    failures: list[str] = []
    for title in ACCEPTED:
        reason = failure(title)
        if reason is not None:
            failures.append(f"accepted fixture refused: {title!r}: {reason}")
    for title, expected in REFUSED:
        reason = failure(title)
        if reason is None:
            failures.append(f"refused fixture accepted: {title!r}")
        elif expected is not None and expected not in reason:
            failures.append(f"refused fixture {title!r}: expected {expected!r} in {reason!r}")
    for text, expected in SQUASH_SUBJECTS:
        got = squash_title(text)
        if got != expected:
            failures.append(f"squash subject {text!r}: expected {expected!r}, got {got!r}")
    if failure(squash_title(SQUASH_SUBJECTS[0][0])) is not None:
        failures.append("squash subject of an accepted title is not itself accepted")
    if SCOPE.fullmatch("server\n") is not None or SCOPE.fullmatch("server") is None:
        failures.append("scope grammar must match the whole scope and nothing else")
    sample = message("WIP", "no `type:` prefix")
    for needle in (SHAPE, EXAMPLES[0], EXAMPLES[1], "CONTRIBUTING.md"):
        if needle not in sample:
            failures.append(f"failure message lacks {needle!r}")
    if failures:
        print("\n".join(failures))
        return 1
    print(f"self-test ok ({len(ACCEPTED)} accepted, {len(REFUSED)} refused, {len(SQUASH_SUBJECTS)} squash subjects)")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--title", help="a pull request title")
    parser.add_argument("--squash-subject", help="a merge-queue commit message; its first line minus ` (#N)` is the title")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        return self_test()
    if (args.title is None) == (args.squash_subject is None):
        parser.error("exactly one of --title or --squash-subject is required unless --self-test")
    title = args.title if args.title is not None else squash_title(args.squash_subject)
    return check(title)


if __name__ == "__main__":
    sys.exit(main())
