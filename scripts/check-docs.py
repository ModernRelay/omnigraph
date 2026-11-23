#!/usr/bin/env python3
"""Validate Omnigraph documentation structure, budgets and local links."""

from __future__ import annotations

import html
import re
import subprocess
import sys
from pathlib import Path
from urllib.parse import unquote

from markdown_links import link_targets

ROOT = Path(__file__).resolve().parent.parent
RFC_DIR = ROOT / "docs" / "rfcs"
SKILL_DIR = ROOT / "skills" / "omnigraph"
SKILL_PATH = SKILL_DIR / "SKILL.md"

DEV_DIR = ROOT / "docs" / "dev"
DECISION_DIR = ROOT / "docs" / "decisions"
CONTRACT_PATH = DEV_DIR / "contract.md"

# Byte budgets (docs/dev/documentation.md, Kinds of document). A page that
# every agent session loads is listed by path; every other page of docs/dev/
# takes GUIDE_BUDGET. A guide listed with a number above GUIDE_BUDGET is over
# that budget: its number is a ceiling that only goes down, and the row is
# deleted when the page fits.
PAGE_BUDGETS = {
    "AGENTS.md": 14_000,
    "docs/dev/contract.md": 53_000,
    "docs/dev/workload.md": 36_000,
    "docs/dev/invariants.md": 11_500,
    "docs/dev/testing.md": 7_500,
    "docs/dev/execution.md": 88_500,
    "docs/dev/writes.md": 70_500,
}
GUIDE_BUDGET = 60_000
DECISION_BUDGET = 5_000

DECISION_FILENAME = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*\.md")
DECISION_FIELDS = ("Decides", "Contract", "Code")
DECISION_SECTIONS = ("Context", "Decision", "Rejected")
DECISION_FORBIDDEN = {
    r"(?<![\w/])#\d{2,}\b": "a decision record gives the reason, not an issue or pull request number",
    r"\b(?:Phase|Step) \d": "a decision record holds no plan; open work is an issue",
    r"\b20\d\d-\d\d-\d\d(?![\w-])": "a decision record holds no dates; history is git",
    r"\bOmniGraph\b": "spell the product Omnigraph",
}
CONTRACT_KINDS = ("read", "write", "merge", "schema", "branch", "maintain", "reclaim")
CONTRACT_COLUMNS = ("sees", "changes", "refuses", "cost")

# Leftover merge-conflict markers at line start. git always labels them; the
# `\s|$` arm also catches hand-mangled label-less leftovers. A lone `=======`
# is excluded (legal setext heading underline). CI's diff-scoped companion
# (the marker step in .github/workflows/ci.yml) uses `git diff --check`. A
# doc quoting a conflict block indents the markers one space; no exemption.
CONFLICT_MARKER = re.compile(r"^(?:<{7,}|>{7,}|\|{7,})(?:\s|$)")

USER_FORBIDDEN = {
    r"\b__manifest\b": "internal table names belong in developer docs",
    r"\bManifestCoordinator\b": "code symbols belong in developer docs",
    r"\bSidecarKind\b": "code symbols belong in developer docs",
    r"\bprotocol_v\d+\b": "recovery protocol versions belong in developer docs",
    r"\brecovery-v\d+\b": "recovery protocol versions belong in developer docs",
    r"\bReserveFragments\b": "substrate operations belong in developer docs",
    r"\bRFC[- ]0\d{3}\b": "a user page states the behavior; design history is not user documentation",
    r"\bPhase \d+[A-Z]?\b": "implementation phases do not belong in user docs",
}


def tracked_markdown() -> list[Path]:
    result = subprocess.run(
        ["git", "ls-files", "-co", "--exclude-standard", "--", "*.md"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return sorted(
        {
            ROOT / line
            for line in result.stdout.splitlines()
            if line and (ROOT / line).is_file() and not line.startswith("vendor/lance/")
        }
    )


def strip_fenced_code(text: str) -> str:
    lines: list[str] = []
    fence: str | None = None
    for line in text.splitlines():
        match = re.match(r"^\s*(```+|~~~+)", line)
        if match:
            marker = match.group(1)[0]
            if fence is None:
                fence = marker
            elif marker == fence:
                fence = None
            lines.append("")
        elif fence is None:
            lines.append(line)
        else:
            lines.append("")
    return "\n".join(lines)


def local_link_targets(text: str) -> list[tuple[int, str]]:
    return link_targets(text)


def markdown_destination(raw: str) -> str:
    target = raw
    if target.startswith("<") and ">" in target:
        target = target[1 : target.index(">")]
    else:
        # Drop an optional Markdown title after the destination.
        target = re.split(r"\s+[\"']", target, maxsplit=1)[0]
    return unquote(target)


def normalize_link(raw: str) -> str | None:
    target = markdown_destination(raw).split("#", 1)[0].split("?", 1)[0]
    if not target:
        return None
    if "NNNN" in target or "<" in target or ">" in target:
        return None
    if target.startswith("//") or re.match(r"^[A-Za-z][A-Za-z0-9+.-]*:", target):
        return None
    return target


def heading_anchors(path: Path) -> set[str]:
    return heading_anchors_text(path.read_text(encoding="utf-8"))


def heading_anchors_text(text: str) -> set[str]:
    anchors: set[str] = set()
    counts: dict[str, int] = {}
    for line in text.splitlines():
        match = re.match(r"^#{1,6}\s+(.+?)\s*#*\s*$", line)
        if not match:
            continue
        heading = re.sub(r"\[([^]]+)\]\([^)]+\)", r"\1", match.group(1))
        heading = re.sub(r"<[^>]+>", "", heading)
        heading = re.sub(r"[`*_~]", "", heading)
        heading = html.unescape(heading).lower().strip()
        heading = re.sub(r"[^\w\- ]", "", heading)
        heading = re.sub(r"\s", "-", heading)
        duplicate = counts.get(heading, 0)
        counts[heading] = duplicate + 1
        anchors.add(heading if duplicate == 0 else f"{heading}-{duplicate}")
    for match in re.finditer(r"<(?:a|span)\s+(?:id|name)=[\"']([^\"']+)[\"']", text):
        anchors.add(match.group(1).lower())
    return anchors


def check_links(files: list[Path], errors: list[str]) -> None:
    anchor_cache: dict[Path, set[str]] = {}
    for source in files:
        text = source.read_text(encoding="utf-8")
        for line, raw in local_link_targets(text):
            destination = markdown_destination(raw)
            if "NNNN" in destination or "<" in destination or ">" in destination:
                continue
            if destination.startswith("//") or re.match(
                r"^[A-Za-z][A-Za-z0-9+.-]*:", destination
            ):
                continue
            path_part, separator, fragment = destination.partition("#")
            target = normalize_link(raw)
            if target is not None and target.startswith("/"):
                errors.append(
                    f"{source.relative_to(ROOT)}:{line}: local documentation links must be relative: {raw}"
                )
                continue
            resolved = source.resolve() if target is None else (source.parent / target).resolve()
            try:
                resolved.relative_to(ROOT)
            except ValueError:
                errors.append(
                    f"{source.relative_to(ROOT)}:{line}: local link escapes the repository: {raw}"
                )
                continue
            if not resolved.exists():
                errors.append(
                    f"{source.relative_to(ROOT)}:{line}: broken local link: {raw}"
                )
                continue
            if separator and fragment and resolved.suffix.lower() == ".md":
                anchors = anchor_cache.setdefault(resolved, heading_anchors(resolved))
                if fragment.lower() not in anchors:
                    errors.append(
                        f"{source.relative_to(ROOT)}:{line}: broken heading anchor '#{fragment}' in {resolved.relative_to(ROOT)}"
                    )


def defined_contract_addresses() -> set[str]:
    """Addresses `docs/dev/contract.md` defines, for a record's Contract line."""
    text = CONTRACT_PATH.read_text(encoding="utf-8")
    addresses = set(re.findall(r"(?m)^\*\*(E\d+)\.", text))
    addresses |= set(re.findall(r"(?m)^\| `([GD]\d+)` ", text))
    addresses |= set(re.findall(r"(?m)^\d+\. `([a-z]+:\d+)` ", text))
    for kind in CONTRACT_KINDS:
        addresses |= {f"{kind}.{column}" for column in CONTRACT_COLUMNS}
        addresses |= {f"{kind}+{other}" for other in CONTRACT_KINDS}
    return addresses


CONTRACT_ADDRESS = re.compile(
    r"\b(?:[EGD]\d+|(?:%s)(?::\d+|\.[a-z]+|\+[a-z]+))(?![\w-])" % "|".join(CONTRACT_KINDS)
)


def check_budgets(files: list[Path], errors: list[str]) -> None:
    """Fail a page that passes the byte budget of its kind."""
    for path in files:
        relative = path.relative_to(ROOT).as_posix()
        budget = PAGE_BUDGETS.get(relative)
        if budget is None and path.parent == DEV_DIR:
            budget = GUIDE_BUDGET
        if budget is None and path.parent == DECISION_DIR:
            budget = DECISION_BUDGET
        if budget is None:
            continue
        size = path.stat().st_size
        if size > budget:
            errors.append(
                f"{relative}: {size} bytes is over its budget of {budget}; "
                "cut or split the page (docs/dev/documentation.md, Kinds of document)"
            )


def check_decisions(errors: list[str]) -> None:
    if RFC_DIR.exists():
        errors.append(
            "docs/rfcs/: a proposal is a pull request and a decision is a record "
            "under docs/decisions/ (docs/dev/documentation.md, Decisions)"
        )
    addresses = defined_contract_addresses()
    for path in sorted(DECISION_DIR.glob("*.md")):
        if path.name == "README.md":
            continue
        relative = path.relative_to(ROOT)
        if not DECISION_FILENAME.fullmatch(path.name):
            errors.append(f"{relative}: a decision record is named kebab-title.md")
        text = path.read_text(encoding="utf-8")
        lines = text.splitlines()
        if not lines or not lines[0].startswith("# ") or lines[0].startswith("# RFC"):
            errors.append(f"{relative}: must open with '# Title', a noun phrase")
        fields: dict[str, str] = {}
        for label in DECISION_FIELDS:
            match = re.search(
                rf"(?ms)^\*\*{label}:\*\* (.+?)(?=^\*\*[A-Z][a-z ]+:\*\* |^\s*$)", text
            )
            if match is None:
                errors.append(f"{relative}: missing the '**{label}:**' line")
            else:
                fields[label] = " ".join(match.group(1).split())
        for heading in DECISION_SECTIONS:
            if f"\n## {heading}\n" not in text:
                errors.append(f"{relative}: missing the '## {heading}' section")
        allowed = {*DECISION_SECTIONS, "Not built"}
        for heading in re.findall(r"(?m)^#{2,} (.+)$", strip_fenced_code(text)):
            if heading not in allowed:
                errors.append(f"{relative}: '{heading}' is not a section of a decision record")

        contract = fields.get("Contract", "")
        cited = CONTRACT_ADDRESS.findall(contract)
        for address in cited:
            if address not in addresses:
                errors.append(
                    f"{relative}: Contract line cites {address}, which docs/dev/contract.md does not define"
                )
        if contract and not cited and not contract.startswith("none"):
            errors.append(
                f"{relative}: Contract line cites no address; write 'none' and the invariant it serves"
            )

        for raw in re.findall(r"`([^`]+)`", fields.get("Code", "")):
            target = raw.split("::", 1)[0]
            if "/" in target and not (ROOT / target).exists():
                errors.append(f"{relative}: Code line names {raw}, which does not exist")

        clean = strip_fenced_code(text)
        for pattern, reason in DECISION_FORBIDDEN.items():
            match = re.search(pattern, clean)
            if match:
                line = clean.count("\n", 0, match.start()) + 1
                errors.append(f"{relative}:{line}: {reason}")


def check_locations(files: list[Path], errors: list[str]) -> None:
    for path in files:
        relative = path.relative_to(ROOT)
        if relative.parts and relative.parts[0] == "docs" and "internal" not in relative.parts:
            text = path.read_text(encoding="utf-8")
            if re.search(r"\bRFC-\d{1,4}\b", text) or re.search(
                r"\bRFC \d{1,3}\b", text
            ):
                errors.append(
                    f"{relative}: RFC references must use the four-digit 'RFC NNNN' form"
                )


def check_conflict_markers(files: list[Path], errors: list[str]) -> None:
    for path in files:
        text = path.read_text(encoding="utf-8")
        for number, line in enumerate(text.splitlines(), start=1):
            if CONFLICT_MARKER.match(line):
                errors.append(
                    f"{path.relative_to(ROOT)}:{number}: committed merge-conflict marker"
                )


def check_user_boundary(files: list[Path], errors: list[str]) -> None:
    for path in files:
        try:
            relative = path.relative_to(ROOT / "docs" / "user")
        except ValueError:
            continue
        text = path.read_text(encoding="utf-8")
        if len(text.splitlines()) > 350 and "docs-check: allow-long-page" not in text:
            errors.append(
                f"docs/user/{relative}: authored user page exceeds 350 lines; consolidate or add a reviewed exemption"
            )
        clean = strip_fenced_code(text)
        for pattern, reason in USER_FORBIDDEN.items():
            match = re.search(pattern, clean)
            if match:
                line = clean.count("\n", 0, match.start()) + 1
                errors.append(f"docs/user/{relative}:{line}: {reason}")


def check_skill_version(errors: list[str]) -> None:
    if not SKILL_PATH.exists():
        errors.append("skills/omnigraph/SKILL.md: missing OmniGraph skill entrypoint")
        return

    text = SKILL_PATH.read_text(encoding="utf-8")
    if not text.startswith("---\n") or (end := text.find("\n---\n", 4)) < 0:
        errors.append("skills/omnigraph/SKILL.md: malformed YAML frontmatter")
        return
    metadata = re.search(
        r"(?ms)^metadata:\s*\n(?P<body>(?:^[ \t]+.*(?:\n|$))+)", text[4:end]
    )
    version = (
        re.search(r'(?m)^\s+version:\s*["\']?([^"\'\s]+)', metadata.group("body"))
        if metadata
        else None
    )
    if version is None:
        errors.append("skills/omnigraph/SKILL.md: metadata.version is required")
        return

    cli_manifest = ROOT / "crates" / "omnigraph-cli" / "Cargo.toml"
    cli_text = cli_manifest.read_text(encoding="utf-8")
    package = re.search(r"(?ms)^\[package\]\s*(.*?)(?=^\[|\Z)", cli_text)
    cli_version = (
        re.search(r'(?m)^version\s*=\s*["\']([^"\']+)["\']', package.group(1))
        if package
        else None
    )
    if cli_version is None:
        errors.append(f"{cli_manifest.relative_to(ROOT)}: package.version is required")
    elif version.group(1) != cli_version.group(1):
        errors.append(
            "skills/omnigraph/SKILL.md: metadata.version "
            f"{version.group(1)!r} != omnigraph-cli {cli_version.group(1)!r}"
        )

    # The CLI reports its HTTP contract beside the skill it exports, so the
    # skill's declared contract must be the one the binary speaks.
    api_types = ROOT / "crates" / "omnigraph-api-types" / "src" / "lib.rs"
    contract = re.search(
        r'(?m)^pub const HTTP_API_CONTRACT: &str = "([^"]+)";',
        api_types.read_text(encoding="utf-8"),
    )
    compatibility = re.search(r"(?m)^compatibility:\s*(.*)$", text[4:end])
    declared = (
        re.search(r"\bHTTP contract (\S+?)[.,;]?(?:\s|$)", compatibility.group(1))
        if compatibility
        else None
    )
    if contract is None:
        errors.append(f"{api_types.relative_to(ROOT)}: HTTP_API_CONTRACT is required")
    elif declared is None:
        errors.append(
            "skills/omnigraph/SKILL.md: compatibility must name the 'HTTP contract' it covers"
        )
    elif declared.group(1) != contract.group(1):
        errors.append(
            "skills/omnigraph/SKILL.md: compatibility names HTTP contract "
            f"{declared.group(1)!r} != omnigraph-api-types {contract.group(1)!r}"
        )


def main() -> int:
    from release_notes import check_working_notes

    errors: list[str] = []
    files = tracked_markdown()
    check_locations(files, errors)
    link_files = [
        path
        for path in files
        if path.is_relative_to(ROOT / "docs")
        or path.is_relative_to(SKILL_DIR)
        or path in {ROOT / "AGENTS.md", ROOT / "README.md"}
    ]
    check_links(link_files, errors)
    check_decisions(errors)
    check_budgets(files, errors)
    check_conflict_markers(files, errors)
    check_user_boundary(files, errors)
    check_skill_version(errors)
    check_working_notes(ROOT, errors)

    if errors:
        for error in errors:
            print(f"error: {error}", file=sys.stderr)
        print(f"\ndocumentation checks failed ({len(errors)} errors)", file=sys.stderr)
        return 1
    print(f"Documentation OK ({len(files)} Markdown files checked).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
