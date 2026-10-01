#!/usr/bin/env python3
"""Compose permanent release-note fragments from explicit Git trees."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import importlib.util
import json
import os
import posixpath
import re
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from urllib.parse import quote, unquote, urlsplit

ROOT = Path(__file__).resolve().parent.parent
CONFIG = "changelog.d/release.json"
LEGACY_PATH = "docs/releases/v0.12.0.md"
REPOSITORY = "https://github.com/ModernRelay/omnigraph"
CATEGORIES = {
    "breaking": "Upgrade actions",
    "added": "Features",
    "changed": "Behavior changes",
    "fixed": "Fixes",
    "performance": "Performance",
    "deprecated": "Deprecations",
    "removed": "Removals",
}
NOTE_NAME = re.compile(r"changelog\.d/[a-z0-9]+(?:-[a-z0-9]+)*\.([a-z]+)\.md")
VERSION = re.compile(r"v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)")
SHA = re.compile(r"[0-9a-f]{40}(?:[0-9a-f]{24})?")
PROVENANCE = re.compile(r"^<!-- release-notes: (.+) -->$", re.MULTILINE)
DEFINITION = re.compile(r"^\[([^\]]+)\]: (\S+)$")
INLINE_LINK = re.compile(r"!?\[[^\]\n]*\]\(([^)\n]+)\)")


class NotesError(Exception):
    pass


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@cache
def docs_checker():
    spec = importlib.util.spec_from_file_location("check_docs", ROOT / "scripts/check-docs.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class Repository:
    def __init__(self, root: Path):
        self.root = root

    def git(self, *args: str, optional: bool = False) -> bytes | None:
        result = subprocess.run(
            ["git", "-C", str(self.root), *args], capture_output=True, check=False
        )
        if result.returncode:
            if optional and result.returncode in {1, 128}:
                return None
            raise NotesError(result.stderr.decode("utf-8", errors="replace").strip())
        return result.stdout

    def resolve(self, ref: str) -> str:
        value = self.git("rev-parse", "--verify", "--end-of-options", f"{ref}^{{commit}}")
        sha = value.decode().strip()
        if not SHA.fullmatch(sha):
            raise NotesError(f"not a commit: {ref}")
        return sha

    def require_ancestor(self, base: str, target: str) -> None:
        if self.git("merge-base", "--is-ancestor", base, target, optional=True) is None:
            raise NotesError(f"{base} is not a known ancestor of {target}; fetch complete history")

    def read(self, revision: str | None, path: str) -> bytes:
        if revision is None:
            file = self.root / path
            if file.is_symlink() or not file.resolve().is_relative_to(self.root.resolve()):
                raise NotesError(f"unsupported symlink or escaping path: {path}")
            return file.read_bytes()
        return self.git("show", f"{revision}:{path}")

    def notes(self, revision: str | None) -> dict[str, bytes]:
        if revision is None:
            paths = [p.relative_to(self.root).as_posix() for p in (self.root / "changelog.d").rglob("*") if p.is_symlink() or not p.is_dir()]
        else:
            entries = self.git("ls-tree", "-rz", revision, "--", "changelog.d/")
            paths = []
            for entry in entries.split(b"\0"):
                if not entry:
                    continue
                fields, path = entry.split(b"\t", 1)
                if fields.split()[0] != b"100644":
                    raise NotesError(f"release inputs must be regular non-executable files: {path!r}")
                paths.append(path.decode("utf-8"))
        return {path: self.read(revision, path) for path in sorted(paths) if path != CONFIG}

    def object_kind(self, revision: str | None, path: str) -> str | None:
        if revision is None:
            file = self.root / path
            if not file.resolve().is_relative_to(self.root.resolve()) or not file.exists():
                return None
            return "tree" if file.is_dir() else "blob"
        result = self.git("cat-file", "-t", f"{revision}:{path}", optional=True)
        return result.decode().strip() if result else None

    def released(self, version: str) -> bool:
        return self.git("rev-parse", "--verify", "--end-of-options", f"refs/tags/{version}^{{commit}}", optional=True) is not None


def prose_lines(text: str):
    """Yield original lines and a code-masked view; only link definitions are rewritten."""
    fence: tuple[str, int] | None = None
    inline: int | None = None
    for line in text.splitlines(keepends=True):
        marker = re.match(r"^\s*(`{3,}|~{3,})", line)
        if marker and inline is None:
            token = marker.group(1)
            if fence is None:
                fence = (token[0], len(token))
            elif token[0] == fence[0] and len(token) >= fence[1] and not line[marker.end():].strip():
                fence = None
            yield line, ""
        elif fence is not None or (inline is None and (line.startswith("    ") or line.startswith("\t"))):
            yield line, ""
        else:
            masked = list(line)
            start = 0
            for token in re.finditer(r"(?<!\\)`+", line):
                if inline is None:
                    inline, start = len(token.group()), token.start()
                elif len(token.group()) == inline:
                    masked[start:token.end()] = " " * (token.end() - start)
                    inline = None
            if inline is not None:
                masked[start:] = " " * (len(line) - start)
            yield line, "".join(masked)
    if fence is not None:
        raise NotesError("unclosed Markdown fence")
    if inline is not None:
        raise NotesError("unclosed Markdown code span; escape a literal backtick")


def is_external(destination: str) -> bool:
    return bool(urlsplit(destination).scheme) or destination.startswith("//")


def local_destination(source: str, destination: str) -> tuple[str, str]:
    url = urlsplit(destination)
    if not url.path or url.query or url.path.startswith("/") or "\\" in url.path:
        raise NotesError(f"{source}: use a relative file path, optionally with an anchor: {destination}")
    path = posixpath.normpath(posixpath.join(posixpath.dirname(source), unquote(url.path)))
    if path == ".." or path.startswith("../") or path.startswith("/"):
        raise NotesError(f"{source}: link escapes the repository: {destination}")
    return path, url.fragment


def link_destination(repo: Repository, revision: str | None, source: str, destination: str, publication_ref: str | None) -> str:
    if is_external(destination):
        return destination
    path, anchor = local_destination(source, destination)
    kind = repo.object_kind(revision, path)
    if kind not in {"blob", "tree"}:
        raise NotesError(f"{source}: missing link target at selected revision: {destination}")
    if anchor and path.endswith(".md"):
        content = repo.read(revision, path).decode("utf-8")
        if unquote(anchor).lower() not in docs_checker().heading_anchors_text(content):
            raise NotesError(f"{source}: missing heading anchor at selected revision: {destination}")
    suffix = f"#{anchor}" if anchor else ""
    if publication_ref is not None:
        return f"{REPOSITORY}/{kind}/{quote(publication_ref, safe='')}/{quote(path, safe='/')}{suffix}"
    return quote(posixpath.relpath(path, "docs/releases"), safe="/.-") + suffix


def note_text(path: str, raw: bytes) -> tuple[str, str]:
    match = NOTE_NAME.fullmatch(path)
    if not match or match.group(1) not in CATEGORIES:
        raise NotesError(f"{path}: expected changelog.d/<slug>.<category>.md; categories: {', '.join(CATEGORIES)}")
    text = raw.decode("utf-8")
    if not text.startswith("- ") or not text[2:].strip() or not text.endswith("\n"):
        raise NotesError(f"{path}: write a nonempty Markdown bullet ending with a newline")
    return match.group(1), text


def render_note(repo: Repository, revision: str | None, path: str, raw: bytes, publication_ref: str | None, labels: set[str]) -> str:
    _, text = note_text(path, raw)
    result = []
    for line, prose in prose_lines(text):
        definition = DEFINITION.fullmatch(prose.rstrip("\n"))
        if definition:
            label, destination = definition.groups()
            normalized = " ".join(label.casefold().split())
            if normalized in labels:
                raise NotesError(f"{path}: repeated reference label {label!r}; use a label unique to this note")
            labels.add(normalized)
            destination = link_destination(repo, revision, path, destination, publication_ref)
            result.append(f"[{label}]: {destination}\n")
            continue
        if re.match(r"^\s*\[[^\]]+\]:", prose):
            raise NotesError(f"{path}: link definitions must be unindented '[label]: destination', without a title")
        if re.match(r"^#{1,2}\s", prose):
            raise NotesError(f"{path}: release headings belong to the renderer")
        for match in INLINE_LINK.finditer(prose):
            if not is_external(match.group(1)):
                raise NotesError(f"{path}: put local links in '[label]: ../docs/...' reference definitions")
        result.append(line)
    return "".join(result)


def select_notes(base: dict[str, bytes], target: dict[str, bytes]) -> dict[str, bytes]:
    changed = [path for path, raw in base.items() if target.get(path) != raw]
    if changed:
        raise NotesError("published notes were changed, renamed or removed: " + ", ".join(sorted(changed)))
    for path, raw in target.items():
        note_text(path, raw)
    return {path: target[path] for path in sorted(target.keys() - base.keys())}


@dataclass
class Selection:
    base: str | None
    target: str
    notes: dict[str, bytes]
    legacy: str | None = None
    baseline: str = ""
    working_tree: bool = False
    config: str = ""

    def inputs(self) -> dict[str, str]:
        return {path: digest(raw) for path, raw in self.notes.items()}


def select(repo: Repository, base: str | None, target: str, legacy: str | None = None, working_tree: bool = False) -> Selection:
    target_sha = repo.resolve(target)
    base_sha = repo.resolve(base) if base is not None else None
    if base_sha is not None:
        repo.require_ancestor(base_sha, target_sha)
    notes = select_notes(repo.notes(base_sha) if base_sha else {}, repo.notes(None if working_tree else target_sha))
    legacy_sha, baseline = None, ""
    if legacy is not None:
        if not SHA.fullmatch(legacy):
            raise NotesError("the legacy baseline must use a full commit SHA")
        legacy_sha = repo.resolve(legacy)
        repo.require_ancestor(legacy_sha, target_sha)
        original = repo.read(legacy_sha, LEGACY_PATH).decode("utf-8")
        prefix = "# OmniGraph v0.12.0\n\nUnreleased.\n\n"
        if not original.startswith(prefix) or PROVENANCE.search(original):
            raise NotesError("the legacy revision must contain the original unreleased v0.12.0 document")
        current = repo.read(None if working_tree else target_sha, LEGACY_PATH).decode("utf-8")
        if current != original and not PROVENANCE.search(current):
            raise NotesError("the v0.12.0 baseline is frozen; put new entries in changelog.d/")
        baseline = original[len(prefix):]
    return Selection(base_sha, target_sha, notes, legacy_sha, baseline, working_tree,
                     digest(repo.read(None if working_tree else target_sha, CONFIG)))


def metadata(selection: Selection, version: str, date: str | None) -> dict:
    if not isinstance(version, str) or not VERSION.fullmatch(version):
        raise NotesError("version must be vMAJOR.MINOR.PATCH")
    if date is not None:
        try:
            if not isinstance(date, str):
                raise ValueError
            if dt.date.fromisoformat(date).isoformat() != date:
                raise ValueError
        except ValueError as error:
            raise NotesError("release date must be YYYY-MM-DD") from error
    if selection.legacy and version != "v0.12.0":
        raise NotesError("the legacy baseline applies only to v0.12.0")
    return {"format": 1, "version": version, "date": date, "base": selection.base,
            "target": selection.target, "notes": selection.inputs(), "legacy": selection.legacy,
            "working_tree": selection.working_tree, "config": selection.config}


def render(repo: Repository, selection: Selection, info: dict, publication_ref: str | None = None) -> str:
    revision = None if selection.working_tree else selection.target
    status = f"Released {info['date']}." if info["date"] else "Unreleased preview."
    if selection.working_tree:
        status = f"Working-tree preview based on `{selection.target}`; includes local and untracked notes."
    parts = [f"# OmniGraph {info['version']}\n\n{status}\n\n",
             "<!-- release-notes: " + json.dumps(info, sort_keys=True, separators=(",", ":")) + " -->\n\n"]
    if selection.baseline:
        baseline = selection.baseline
        # The pinned migration input uses only simple inline local links.
        lines = []
        for line, prose in prose_lines(baseline):
            for match in reversed(list(INLINE_LINK.finditer(prose))):
                destination = match.group(1)
                if is_external(destination):
                    continue
                replacement = link_destination(repo, revision, LEGACY_PATH, destination, publication_ref)
                if publication_ref:
                    start, end = match.span(1)
                    line = line[:start] + replacement + line[end:]
            lines.append(line)
        baseline = "".join(lines)
        parts.append(baseline.rstrip("\n") + "\n\n")
    labels: set[str] = set()
    for category, title in CATEGORIES.items():
        notes = [(path, raw) for path, raw in selection.notes.items() if NOTE_NAME.fullmatch(path).group(1) == category]
        if notes:
            parts.append(f"## {title}\n\n")
            for path, raw in notes:
                parts.append(render_note(repo, revision, path, raw, publication_ref, labels).rstrip("\n") + "\n\n")
    if not selection.notes and not selection.baseline:
        parts.append("No user-visible changes recorded.\n\n")
    return "".join(parts).rstrip("\n") + "\n"


def read_config(repo: Repository, revision: str | None) -> dict:
    config = json.loads(repo.read(revision, CONFIG))
    if not isinstance(config, dict) or set(config) != {"version", "base", "legacy"}:
        raise NotesError(f"{CONFIG}: expected exactly version, base and legacy")
    if not isinstance(config["version"], str) or not VERSION.fullmatch(config["version"]):
        raise NotesError(f"{CONFIG}: invalid version")
    if not isinstance(config["base"], str) or not config["base"]:
        raise NotesError(f"{CONFIG}: base must name the previous release explicitly")
    if config["legacy"] is not None and (not isinstance(config["legacy"], str) or not SHA.fullmatch(config["legacy"])):
        raise NotesError(f"{CONFIG}: legacy must be null or a full commit SHA")
    return config


def verify_snapshot(repo: Repository, content: str, audited: str | None = None) -> tuple[Selection, dict]:
    matches = PROVENANCE.findall(content)
    if len(matches) != 1:
        raise NotesError("release snapshot must contain exactly one provenance record")
    info = json.loads(matches[0])
    if not isinstance(info, dict) or set(info) != {"format", "version", "date", "base", "target", "notes", "legacy", "working_tree", "config"}:
        raise NotesError("invalid release snapshot provenance")
    if info["format"] != 1 or info["working_tree"] is not False or not info["date"]:
        raise NotesError("publication requires a dated snapshot of committed inputs")
    for name in ("base", "target", "legacy"):
        value = info[name]
        if (value is None and name != "target") or (isinstance(value, str) and SHA.fullmatch(value)):
            continue
        raise NotesError(f"snapshot {name} must be a full commit SHA")
    selected = select(repo, info["base"], info["target"], info["legacy"])
    expected_info = metadata(selected, info["version"], info["date"])
    if info != expected_info or content != render(repo, selected, expected_info):
        raise NotesError("snapshot differs from its recorded inputs; regenerate it")
    if audited is not None:
        source = repo.resolve(audited)
        repo.require_ancestor(selected.target, source)
        current = select(repo, selected.base, source, selected.legacy)
        if current.config != selected.config:
            raise NotesError("release configuration changed after snapshot generation; regenerate it")
        if current.inputs() != selected.inputs():
            raise NotesError("release notes changed after snapshot generation; regenerate it")
        # Check the destinations against the release tree, not just its older input tree.
        render(repo, current, expected_info)
    return selected, expected_info


def write_snapshot(repo: Repository, selection: Selection, info: dict, replace: bool, replace_legacy: bool) -> Path:
    path = repo.root / "docs/releases" / f"{info['version']}.md"
    content = render(repo, selection, info)
    if repo.released(info["version"]):
        raise NotesError("a tag already exists for this version; published notes are immutable")
    if path.exists():
        existing = path.read_text(encoding="utf-8")
        original = repo.read(selection.legacy, LEGACY_PATH).decode("utf-8") if selection.legacy else None
        migration = replace_legacy and info["version"] == "v0.12.0" and existing == original
        generated = replace and PROVENANCE.search(existing) is not None
        if not migration and not generated:
            raise NotesError(f"{path}: already exists; use --replace for a generated snapshot or --replace-legacy for the pinned baseline")
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, prefix=".release-notes-", delete=False) as temporary:
        temporary.write(content)
        temporary_path = Path(temporary.name)
    try:
        os.replace(temporary_path, path)
    finally:
        temporary_path.unlink(missing_ok=True)
    return path


def check_working_notes(root: Path, errors: list[str]) -> None:
    try:
        repo = Repository(root)
        config = read_config(repo, None)
        selected = select(repo, config["base"], "HEAD", config["legacy"], working_tree=True)
        render(repo, selected, metadata(selected, config["version"], None))
        if selected.legacy:
            current = repo.read(None, LEGACY_PATH).decode("utf-8")
            original = repo.read(selected.legacy, LEGACY_PATH).decode("utf-8")
            if current != original:
                if not PROVENANCE.search(current):
                    raise NotesError("the v0.12.0 baseline is frozen; put new entries in changelog.d/")
                recorded, _ = verify_snapshot(repo, current, "HEAD")
                if recorded.inputs() != selected.inputs() or recorded.config != selected.config:
                    raise NotesError("working notes or release configuration changed after snapshot generation")
    except (NotesError, OSError, ValueError) as error:
        errors.append(f"release notes: {error}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for command in ("preview", "snapshot"):
        child = sub.add_parser(command)
        target = child.add_mutually_exclusive_group(required=True)
        target.add_argument("--target", help="committed revision")
        if command == "preview":
            target.add_argument("--working-tree", action="store_true", help="include tracked edits and untracked notes")
        base = child.add_mutually_exclusive_group()
        base.add_argument("--base", help="override the explicit previous release in changelog.d/release.json")
        base.add_argument("--initial-release", action="store_true", help="explicitly select all notes without a previous release")
        child.add_argument("--version", help="override the configured version")
        child.add_argument("--date", required=command == "snapshot")
        if command == "snapshot":
            child.add_argument("--replace", action="store_true")
            child.add_argument("--replace-legacy", action="store_true")
    verify = sub.add_parser("verify")
    verify.add_argument("--version", required=True)
    verify.add_argument("--target", required=True, help="audited release revision")
    body = sub.add_parser("body")
    body.add_argument("--tag", required=True)
    body.add_argument("--target", required=True, help="audited release revision")
    args = parser.parse_args(argv)
    try:
        repo = Repository(ROOT)
        if args.command in {"verify", "body"}:
            version = args.tag if args.command == "body" else args.version
            if not VERSION.fullmatch(version):
                raise NotesError("version must be vMAJOR.MINOR.PATCH")
            target = repo.resolve(args.target)
            if args.command == "body" and repo.resolve(f"refs/tags/{version}") != target:
                raise NotesError("release tag does not select the audited source")
            selected, info = verify_snapshot(repo, repo.read(target, f"docs/releases/{version}.md").decode("utf-8"), target)
            if info["version"] != version:
                raise NotesError("snapshot version does not match its filename")
            if args.command == "body":
                print(render(repo, selected, info, publication_ref=version), end="")
            else:
                print(f"Release notes OK: {version}, {selected.base}..{selected.target}")
            return 0
        working = getattr(args, "working_tree", False)
        target = repo.resolve(args.target or "HEAD")
        config = read_config(repo, None if working else target)
        selected = select(repo, None if args.initial_release else (args.base or config["base"]), target, config["legacy"], working)
        info = metadata(selected, args.version or config["version"], args.date)
        if args.command == "preview":
            print(render(repo, selected, info), end="")
        else:
            print(write_snapshot(repo, selected, info, args.replace, args.replace_legacy))
        return 0
    except (NotesError, OSError, ValueError) as error:
        print(f"release notes: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
