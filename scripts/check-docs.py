#!/usr/bin/env python3
"""Validate OmniGraph documentation structure and local links."""

from __future__ import annotations

import datetime as dt
import html
import re
import subprocess
import sys
from pathlib import Path
from urllib.parse import unquote


ROOT = Path(__file__).resolve().parent.parent
RFC_DIR = ROOT / "docs" / "rfcs"
SKILL_DIR = ROOT / "skills" / "omnigraph"
SKILL_PATH = SKILL_DIR / "SKILL.md"

RFC_KEYS = (
    "rfc",
    "title",
    "track",
    "status",
    "implementation",
    "authors",
    "created",
    "updated",
    "discussion",
    "supersedes",
    "superseded_by",
    "blocked_on",
)
RFC_TRACKS = {"public", "maintainer"}
RFC_STATUSES = {"draft", "accepted", "rejected", "superseded"}
RFC_IMPLEMENTATION = {
    "not-started",
    "in-progress",
    "partial",
    "complete",
    "removed",
    "n/a",
}

# Leftover merge-conflict markers at line start. git always labels them; the
# `\s|$` arm also catches hand-mangled label-less leftovers. A lone `=======`
# is excluded (legal setext heading underline). Twin pattern: the marker step
# in .github/workflows/ci.yml — keep the two in sync. A doc quoting a
# conflict block indents the markers one space; there is no exemption.
CONFLICT_MARKER = re.compile(r"^(?:<{7,}|>{7,}|\|{7,})(?:\s|$)")

USER_FORBIDDEN = {
    r"\b__manifest\b": "internal table names belong in developer docs",
    r"\bManifestCoordinator\b": "code symbols belong in developer docs",
    r"\bSidecarKind\b": "code symbols belong in developer docs",
    r"\bprotocol_v\d+\b": "recovery protocol versions belong in developer docs",
    r"\brecovery-v\d+\b": "recovery protocol versions belong in developer docs",
    r"\bReserveFragments\b": "substrate operations belong in developer docs",
    r"\bRFC[- ]0\d{3}\b": "project design history belongs in RFCs, not user docs",
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
            if line and (ROOT / line).is_file()
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
    clean = strip_fenced_code(text)
    found: list[tuple[int, str]] = []
    patterns = (
        re.compile(r"!?\[[^\]]*\]\(([^)]+)\)"),
        re.compile(r"^\s*\[[^\]]+\]:\s*(\S+)", re.MULTILINE),
    )
    for pattern in patterns:
        for match in pattern.finditer(clean):
            raw = match.group(1).strip()
            line = clean.count("\n", 0, match.start()) + 1
            found.append((line, raw))
    return found


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
    anchors: set[str] = set()
    counts: dict[str, int] = {}
    text = path.read_text(encoding="utf-8")
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


def frontmatter(text: str, path: Path, errors: list[str]) -> tuple[dict[str, str], str]:
    if not text.startswith("---\n"):
        errors.append(f"{path.relative_to(ROOT)}: RFC must start with YAML frontmatter")
        return {}, text
    end = text.find("\n---\n", 4)
    if end < 0:
        errors.append(f"{path.relative_to(ROOT)}: RFC frontmatter has no closing ---")
        return {}, text
    block = text[4:end]
    values: dict[str, str] = {}
    order: list[str] = []
    for line in block.splitlines():
        match = re.match(r"^([a-z_]+):(?:\s*(.*))?$", line)
        if match:
            key, value = match.group(1), (match.group(2) or "").strip()
            values[key] = value
            order.append(key)
    missing = [key for key in RFC_KEYS if key not in values]
    if missing:
        errors.append(
            f"{path.relative_to(ROOT)}: missing RFC metadata: {', '.join(missing)}"
        )
    known_order = [key for key in order if key in RFC_KEYS]
    expected_order = [key for key in RFC_KEYS if key in known_order]
    if known_order != expected_order:
        errors.append(
            f"{path.relative_to(ROOT)}: RFC metadata keys are not in template order"
        )
    extra = [key for key in order if key not in RFC_KEYS]
    if extra:
        errors.append(
            f"{path.relative_to(ROOT)}: unsupported RFC metadata: {', '.join(extra)}"
        )
    return values, text[end + 5 :]


def scalar(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        return value[1:-1]
    return value


def list_ids(value: str) -> list[str]:
    return re.findall(r"[\"'](\d{4})[\"']", value)


def valid_date(value: str) -> bool:
    try:
        dt.date.fromisoformat(scalar(value))
    except ValueError:
        return False
    return True


def check_rfcs(errors: list[str]) -> None:
    readme = RFC_DIR / "README.md"
    registry = readme.read_text(encoding="utf-8") if readme.exists() else ""
    seen: dict[str, Path] = {}
    records: dict[str, tuple[str, str, str, str]] = {}
    references: list[tuple[Path, str, str]] = []

    for path in sorted(RFC_DIR.glob("*.md")):
        if path.name in {"README.md", "0000-template.md"}:
            continue
        name = re.fullmatch(r"(\d{4})-([a-z0-9]+(?:-[a-z0-9]+)*)\.md", path.name)
        if not name:
            errors.append(
                f"{path.relative_to(ROOT)}: RFC filename must be NNNN-kebab-title.md"
            )
            continue
        number = name.group(1)
        if number in seen:
            errors.append(
                f"{path.relative_to(ROOT)}: duplicate RFC {number}; also {seen[number].relative_to(ROOT)}"
            )
        seen[number] = path

        text = path.read_text(encoding="utf-8")
        values, body = frontmatter(text, path, errors)
        metadata_number = scalar(values.get("rfc", ""))
        if metadata_number != number:
            errors.append(
                f"{path.relative_to(ROOT)}: filename RFC {number} != frontmatter {metadata_number!r}"
            )
        title = scalar(values.get("title", ""))
        heading = re.search(r"^# RFC (\d{4}): (.+)$", body, re.MULTILINE)
        if not heading:
            errors.append(
                f"{path.relative_to(ROOT)}: first H1 must use '# RFC {number}: Title'"
            )
        else:
            heading_title = re.sub(r"[`*_]", "", heading.group(2)).strip()
            metadata_title = re.sub(r"[`*_]", "", title).strip()
            if heading.group(1) != number or heading_title != metadata_title:
                errors.append(
                    f"{path.relative_to(ROOT)}: H1 must match frontmatter id and title"
                )

        track = scalar(values.get("track", ""))
        status = scalar(values.get("status", ""))
        implementation = scalar(values.get("implementation", ""))
        if track not in RFC_TRACKS:
            errors.append(f"{path.relative_to(ROOT)}: invalid RFC track {track!r}")
        if status not in RFC_STATUSES:
            errors.append(f"{path.relative_to(ROOT)}: invalid RFC status {status!r}")
        if implementation not in RFC_IMPLEMENTATION:
            errors.append(
                f"{path.relative_to(ROOT)}: invalid implementation state {implementation!r}"
            )
        records[number] = (title, track, status, implementation)
        if status == "superseded" and values.get("superseded_by", "") == "[]":
            errors.append(
                f"{path.relative_to(ROOT)}: superseded RFC must name superseded_by"
            )
        if status == "draft" and implementation == "complete":
            errors.append(
                f"{path.relative_to(ROOT)}: completed implementation cannot remain draft"
            )
        for key in ("created", "updated"):
            if key in values and not valid_date(values[key]):
                errors.append(
                    f"{path.relative_to(ROOT)}: {key} must be an ISO date"
                )
        if values.get("authors", None) != "":
            errors.append(
                f"{path.relative_to(ROOT)}: authors must use the template's YAML list form"
            )
        for key in ("supersedes", "superseded_by"):
            for target in list_ids(values.get(key, "")):
                references.append((path, key, target))

        if f"]({path.name})" not in registry:
            errors.append(
                f"{path.relative_to(ROOT)}: RFC is missing from docs/rfcs/README.md registry"
            )

    for path, key, target in references:
        if target not in seen:
            errors.append(
                f"{path.relative_to(ROOT)}: {key} references missing RFC {target}"
            )

    registry_rows: dict[str, tuple[str, str, str, str, str]] = {}
    row_pattern = re.compile(
        r"^\| \[(\d{4})\]\(([^)]+)\) \| (.*?) \| "
        r"(public|maintainer) \| (draft|accepted|rejected|superseded) \| "
        r"([^|]+?) \|$",
        re.MULTILINE,
    )
    for match in row_pattern.finditer(registry):
        number, filename, title, track, status, implementation = match.groups()
        registry_rows[number] = (
            filename,
            re.sub(r"[`*_]", "", title).strip(),
            track,
            status,
            implementation.strip(),
        )
    for number, path in seen.items():
        row = registry_rows.get(number)
        if row is None:
            errors.append(f"docs/rfcs/README.md: registry row missing RFC {number}")
            continue
        filename, title, track, status, implementation = row
        expected_title, expected_track, expected_status, expected_implementation = records[number]
        expected_title = re.sub(r"[`*_]", "", expected_title).strip()
        if filename != path.name or (
            title,
            track,
            status,
            implementation,
        ) != (
            expected_title,
            expected_track,
            expected_status,
            expected_implementation,
        ):
            errors.append(
                f"docs/rfcs/README.md: registry row for RFC {number} disagrees with its file"
            )
    for number in registry_rows.keys() - seen.keys():
        errors.append(f"docs/rfcs/README.md: registry references missing RFC {number}")


def check_locations(files: list[Path], errors: list[str]) -> None:
    for path in files:
        relative = path.relative_to(ROOT)
        name = path.name.lower()
        if ".pre-merge-draft." in name:
            errors.append(f"{relative}: duplicate RFC draft snapshots are not allowed")
        if re.search(r"(?:^|[-_])rfc[-_]?\d", name) and path.parent != RFC_DIR:
            errors.append(f"{relative}: project RFCs must live directly in docs/rfcs/")
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


def main() -> int:
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
    check_rfcs(errors)
    check_conflict_markers(files, errors)
    check_user_boundary(files, errors)
    check_skill_version(errors)

    if errors:
        for error in errors:
            print(f"error: {error}", file=sys.stderr)
        print(f"\ndocumentation checks failed ({len(errors)} errors)", file=sys.stderr)
        return 1
    print(f"Documentation OK ({len(files)} Markdown files checked).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
