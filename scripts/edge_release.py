#!/usr/bin/env python3
"""Decisions and text of an edge build, for `.github/workflows/release-edge.yml`.

An edge build is published twice: as its own prerelease, `edge-<date>-<commit>`,
and as the rolling `edge` release.

- `check-version` refuses a build whose package version is not above the
  latest stable release, since its `X.Y.Z-edge+<commit>` would sort before
  that release.
- `body` composes the release text: what a deployer needs before running the
  build against data (the commit, the storage format it writes and serves next
  to the latest stable release's, the HTTP contract, how to pin the build) and
  the release notes recorded since the last stable release, which
  `release_notes.py preview` renders. A stable release before 0.14 does not
  report its HTTP contract, so it is read from that release's source. A marker
  line records the build's commit and storage format for `prune`.
- `prune` lists the edge builds to delete: everything beyond the newest
  retained count, except the newest build of every storage format, which is
  the build that can still export a graph at that format.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass, replace
from pathlib import Path

# GitHub refuses a release body above 125,000 characters; stay clear of it.
BODY_LIMIT = 120_000
BUILD_TAG = re.compile(r"edge-[0-9]{8}-[0-9a-f]{7}")
CORE_VERSION = re.compile(r"v?([0-9]+)\.([0-9]+)\.([0-9]+)(?:[-+].*)?")
MARKER = re.compile(r"^<!-- edge-build: (\{.*\}) -->$", re.MULTILINE)
COMMIT = re.compile(r"[0-9a-f]{40}")
SCHEMA_LINE = re.compile(r"internal-schema ([0-9]+) \(serves v([0-9]+) to v([0-9]+)\)")
# Format-1 previews spell the name `OmniGraph`, format 2 `Omnigraph`.
PREVIEW_HEADING = re.compile(r"# Omni[Gg]raph (v[0-9]+\.[0-9]+\.[0-9]+)")
# The lines `release_notes.render_footer` writes after a format-2 preview.
FOOTER = ("**Full changelog:** ", "[Upgrade guide](")
SOURCE_CONTRACT = re.compile(r'^pub const HTTP_API_CONTRACT: &str = "([^"\n]+)";$', re.MULTILINE)


class BodyError(Exception):
    pass


@dataclass(frozen=True)
class Facts:
    """What `omnigraph version` reports about one binary."""

    version: str
    writes: int
    serves: tuple[int, int]
    http_contract: str | None


def parse_facts(raw: str) -> Facts:
    """Read `omnigraph version --json`, or the text of a release without it."""
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        data = None
    if isinstance(data, dict):
        try:
            serves = data["internal_schema"]["serves"]
            return Facts(
                version=str(data["version"]),
                writes=int(data["internal_schema"]["writes"]),
                serves=(int(serves[0]), int(serves[1])),
                http_contract=data.get("http_contract"),
            )
        except (KeyError, IndexError, TypeError, ValueError) as error:
            raise BodyError(f"unexpected `omnigraph version --json` output: {error}") from error
    lines = raw.strip().splitlines()
    if not lines or not lines[0].startswith("omnigraph "):
        raise BodyError("`omnigraph version` output does not start with `omnigraph <version>`")
    schema = next(filter(None, (SCHEMA_LINE.fullmatch(line) for line in lines)), None)
    if schema is None:
        raise BodyError("`omnigraph version` output has no internal-schema line")
    contract = next((line.removeprefix("http-contract ") for line in lines if line.startswith("http-contract ")), None)
    return Facts(
        version=lines[0].removeprefix("omnigraph "),
        writes=int(schema.group(1)),
        serves=(int(schema.group(2)), int(schema.group(3))),
        http_contract=contract,
    )


def with_source_contract(facts: Facts, source: str) -> Facts:
    """Complete `facts` with the `HTTP_API_CONTRACT` that the release's
    `crates/omnigraph-api-types/src/lib.rs` defines. A release before 0.14
    sends that value but does not report it; a release that reports it must
    agree with its source."""
    found = SOURCE_CONTRACT.findall(source)
    if len(found) > 1:
        raise BodyError("the stable release's source defines HTTP_API_CONTRACT more than once")
    if not found:
        return facts
    if facts.http_contract is None:
        return replace(facts, http_contract=found[0])
    if facts.http_contract != found[0]:
        raise BodyError(
            f"the stable binary reports HTTP contract {facts.http_contract} but its source "
            f"defines {found[0]}"
        )
    return facts


def core_version(version: str) -> tuple[int, int, int]:
    match = CORE_VERSION.fullmatch(version)
    if match is None:
        raise BodyError(f"not a version: {version!r}")
    return tuple(int(part) for part in match.groups())


def require_newer_than_stable(package_version: str, stable_tag: str | None) -> None:
    """An edge build of `X.Y.Z` reports `X.Y.Z-edge+<commit>`, which SemVer ranks
    below `X.Y.Z`; it is published only while `X.Y.Z` is above every stable
    release."""
    if stable_tag and core_version(package_version) <= core_version(stable_tag):
        raise BodyError(
            f"the package version {package_version} is not above the latest stable release "
            f"{stable_tag}, so its edge build would sort before {stable_tag}; advance the "
            f"workspace version past {stable_tag} to publish edge builds"
        )


def compatibility(build: Facts, stable_tag: str | None, stable: Facts | None, upgrade_guide: str) -> str:
    def served(facts: Facts) -> str:
        return f"v{facts.serves[0]} to v{facts.serves[1]}"

    parts = ["## Compatibility\n\n"]
    if stable_tag and stable:
        rows = [
            ("Version", f"`{build.version}`", f"`{stable.version}`"),
            ("Storage format written", str(build.writes), str(stable.writes)),
            ("Storage formats served", served(build), served(stable)),
            ("HTTP contract", build.http_contract or "not reported", stable.http_contract or "not reported"),
        ]
        parts.append(f"| | This build | {stable_tag} |\n|---|---|---|\n")
        parts.extend(f"| {name} | {ours} | {theirs} |\n" for name, ours, theirs in rows)
        parts.append("\n")
        storage = []
        if build.writes > stable.serves[1]:
            storage.append(
                f"A branch this build writes to is left at storage format {build.writes}, which "
                f"{stable_tag} cannot open. Test on a copy of the graph, or back it up, before pointing "
                f"this build at data that {stable_tag} must still read."
            )
        if stable.writes < build.serves[0]:
            storage.append(
                f"This build does not open storage format {stable.writes}, which {stable_tag} writes; "
                f"[convert or rebuild]({upgrade_guide}) such a graph first."
            )
        if not storage:
            storage.append(
                f"This build writes storage format {build.writes}, as {stable_tag} does."
                if build.writes == stable.writes
                else f"This build writes storage format {build.writes} and serves {served(build)}."
            )
        parts.append("**Storage.** " + " ".join(storage) + "\n\n")
        if build.http_contract and stable.http_contract and build.http_contract != stable.http_contract:
            parts.append(
                f"**HTTP.** A client and a server must send the same HTTP contract. This build speaks "
                f"{build.http_contract} and {stable_tag} speaks {stable.http_contract}: upgrade the CLI, "
                "the server and every HTTP integration together.\n\n"
            )
        elif build.http_contract and build.http_contract == stable.http_contract:
            parts.append(f"**HTTP.** This build speaks HTTP contract {build.http_contract}, as {stable_tag} does.\n\n")
        elif build.http_contract:
            parts.append(
                f"**HTTP.** This build speaks HTTP contract {build.http_contract}. {stable_tag} does not "
                "report its contract; its release notes name it.\n\n"
            )
    else:
        parts.append(
            f"No stable release is published to compare with. This build is `{build.version}`, writes "
            f"storage format {build.writes}, serves {served(build)} and speaks HTTP contract "
            f"{build.http_contract or 'not reported'}.\n\n"
        )
    return "".join(parts)


def unreleased_notes(preview: str) -> tuple[str, str, list[str]]:
    """Split `release_notes.py preview` output into its version, preamble and sections."""
    lines = preview.splitlines(keepends=True)
    heading = PREVIEW_HEADING.fullmatch(lines[0].rstrip("\n")) if lines else None
    if heading is None:
        raise BodyError("release-note preview does not start with `# Omnigraph vX.Y.Z`")
    preamble: list[str] = []
    sections: list[list[str]] = []
    for line in lines[1:]:
        if line.startswith(FOOTER):
            # The format-2 footer compares with a tag this build precedes.
            continue
        if line.startswith("## "):
            sections.append(["#" + line])
        elif sections:
            sections[-1].append(line)
        elif line.startswith("<!-- release-notes:"):
            preamble.append(line)
    if not sections:
        return heading.group(1), "".join(preamble), ["No user-visible changes are recorded since the last stable release.\n"]
    return heading.group(1), "".join(preamble), ["".join(section).rstrip("\n") + "\n" for section in sections]


def compose(
    *,
    repository: str,
    commit: str,
    build_tag: str,
    build: Facts,
    stable_tag: str | None,
    stable: Facts | None,
    preview: str,
    rolling: bool,
) -> str:
    if not COMMIT.fullmatch(commit):
        raise BodyError("commit must be a full lowercase commit id")
    if not BUILD_TAG.fullmatch(build_tag) or not build_tag.endswith(commit[:7]):
        raise BodyError("build tag must be edge-<YYYYMMDD>-<first seven characters of the commit>")
    require_newer_than_stable(build.version, stable_tag if stable else None)
    site = f"https://github.com/{repository}"
    raw = f"https://raw.githubusercontent.com/{repository}/main/scripts"
    marker = json.dumps({"format": 1, "commit": commit, "writes": build.writes}, sort_keys=True)
    intro = [
        f"<!-- edge-build: {marker} -->\n",
        f"Edge build of [`{commit[:7]}`]({site}/commit/{commit}) from `main`, published as "
        f"[`{build_tag}`]({site}/releases/tag/{build_tag}). Edge builds are prereleases built once a day "
        "from a commit on `main` whose CI passed; they do not receive the qualification a stable release does."
    ]
    if stable_tag:
        intro.append(f" The latest stable release is [{stable_tag}]({site}/releases/tag/{stable_tag}).")
    intro.append("\n\n")
    if rolling:
        intro.append(
            f"The rolling `edge` release always holds the newest edge build, now `{build_tag}`, and its "
            "files are replaced by the next one. To keep the same binaries, install the build by its tag.\n\n"
        )
    install = (
        "## Install this build\n\n"
        f"```bash\ncurl -fsSL {raw}/install.sh | VERSION={build_tag} bash\n```\n\n"
        "```powershell\n"
        f"iwr -UseBasicParsing {raw}/install.ps1 -OutFile install.ps1\n"
        f"powershell -NoProfile -ExecutionPolicy Bypass -File .\\install.ps1 -Version {build_tag}\n"
        "```\n\n"
        "Every archive carries a build provenance attestation: "
        f"`gh attestation verify <archive> --repo {repository}` checks that this repository's release "
        f"workflow built it from `{commit[:7]}`.\n\n"
    )
    version, preamble, sections = unreleased_notes(preview)
    notes_heading = (
        "## Unreleased changes\n\n"
        f"Release notes recorded since the last stable release, as {version} will publish them.\n\n"
    )
    upgrade_guide = f"{site}/blob/{commit}/docs/user/operations/upgrade.md"
    head = "".join(intro) + compatibility(build, stable_tag, stable, upgrade_guide) + install + notes_heading + preamble
    if preamble:
        head += "\n"
    body = head + "\n".join(sections)
    if len(body) > BODY_LIMIT:
        # Upgrade actions come first; keep them and point at the rest.
        omitted = (
            f"\nThe remaining notes do not fit in a release description; read them in "
            f"[`changelog.d` at `{commit[:7]}`]({site}/tree/{commit}/changelog.d).\n"
        )
        body = head + sections[0] + omitted
        if not sections[0].startswith("### Upgrade actions") or len(body) > BODY_LIMIT:
            body = head + omitted.lstrip("\n")
    return body.rstrip("\n") + "\n"


def build_writes(body: str | None) -> int | None:
    """The storage format a build release's marker records, if it has one."""
    match = MARKER.search(body or "")
    if match is None:
        return None
    try:
        writes = json.loads(match.group(1)).get("writes")
    except (json.JSONDecodeError, AttributeError):
        return None
    return writes if isinstance(writes, int) else None


def releases_to_delete(releases: list[dict], retain: int, current: str) -> list[str]:
    """Edge build tags beyond the newest `retain`, keeping the newest build of
    every storage format and `current`. `releases` are GitHub release objects
    (`tag_name`, `created_at`, `draft`, `body`); other tags and drafts are
    never listed."""
    if retain < 1:
        raise BodyError("retain at least one edge build")
    builds = sorted(
        (release for release in releases if not release.get("draft") and BUILD_TAG.fullmatch(release.get("tag_name", ""))),
        key=lambda release: release["created_at"],
        reverse=True,
    )
    keep = {release["tag_name"] for release in builds[:retain]} | {current}
    newest_of_format: dict[int, str] = {}
    for release in builds:
        writes = build_writes(release.get("body"))
        if writes is not None:
            newest_of_format.setdefault(writes, release["tag_name"])
    keep |= set(newest_of_format.values())
    return [release["tag_name"] for release in builds if release["tag_name"] not in keep]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    body = commands.add_parser("body", help="compose a build's release text")
    body.add_argument("--repository", required=True, help="owner/name")
    body.add_argument("--commit", required=True)
    body.add_argument("--build-tag", required=True)
    body.add_argument("--build-version", required=True, type=Path, help="`omnigraph version --json` of this build")
    body.add_argument("--stable-tag", default="", help="latest stable release tag, empty when none exists")
    body.add_argument("--stable-version", type=Path, help="`omnigraph version` output of that release")
    body.add_argument(
        "--stable-source",
        type=Path,
        help="that release's crates/omnigraph-api-types/src/lib.rs, for a release that does not report its HTTP contract",
    )
    body.add_argument("--notes", required=True, type=Path, help="`release_notes.py preview` output")
    body.add_argument("--rolling", action="store_true", help="compose the rolling `edge` release text")
    check = commands.add_parser("check-version", help="refuse a package version not above the latest stable")
    check.add_argument("--package-version", required=True)
    check.add_argument("--stable-tag", default="", help="latest stable release tag, empty when none exists")
    prune = commands.add_parser("prune", help="list edge build tags to delete, one per line")
    prune.add_argument("--releases", required=True, type=Path, help="GitHub release objects, one JSON object per line")
    prune.add_argument("--retain", required=True, type=int)
    prune.add_argument("--current", required=True, help="the tag of the build this run published")
    args = parser.parse_args(argv)
    try:
        if args.command == "check-version":
            require_newer_than_stable(args.package_version, args.stable_tag or None)
            return 0
        if args.command == "prune":
            lines = args.releases.read_text(encoding="utf-8").splitlines()
            releases = [json.loads(line) for line in lines if line.strip()]
            for tag in releases_to_delete(releases, args.retain, args.current):
                print(tag)
            return 0
        stable_tag = args.stable_tag or None
        if bool(stable_tag) != bool(args.stable_version):
            raise BodyError("--stable-tag and --stable-version go together")
        if args.stable_source and not stable_tag:
            raise BodyError("--stable-source needs --stable-tag")
        stable = parse_facts(args.stable_version.read_text(encoding="utf-8")) if args.stable_version else None
        if stable and args.stable_source:
            stable = with_source_contract(stable, args.stable_source.read_text(encoding="utf-8"))
        text = compose(
            repository=args.repository,
            commit=args.commit,
            build_tag=args.build_tag,
            build=parse_facts(args.build_version.read_text(encoding="utf-8")),
            stable_tag=stable_tag,
            stable=stable,
            preview=args.notes.read_text(encoding="utf-8"),
            rolling=args.rolling,
        )
    except (BodyError, OSError, json.JSONDecodeError) as error:
        print(f"edge release: {error}", file=sys.stderr)
        return 1
    sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
