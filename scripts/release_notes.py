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
from dataclasses import dataclass, field
from functools import cache
from pathlib import Path
from urllib.parse import quote, unquote, urlsplit

from markdown_links import descendants, parse as parse_markdown, preserves_boundary

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
PROVENANCE_MARKER = re.compile(r"<!--\s*release-notes\b", re.IGNORECASE)
LEGACY_PREFIX = "# OmniGraph v0.12.0\n\nUnreleased.\n\n"
DEFINITION = re.compile(r"^\[([^\]]+)\]: (\S+)$")
RELEASE_FILE = re.compile(r"changelog\.d/(v(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*))\.md")


class NotesError(Exception):
    pass


def canonical(data: bytes) -> bytes:
    return data.replace(b"\r\n", b"\n")


def digest(data: bytes) -> str:
    return hashlib.sha256(canonical(data)).hexdigest()


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
            return canonical(file.read_bytes())
        return canonical(self.git("show", f"{revision}:{path}"))

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
    text = canonical(raw).decode("utf-8")
    if not text.startswith("- ") or not text[2:].strip() or not text.endswith("\n"):
        raise NotesError(f"{path}: write a nonempty Markdown bullet ending with a newline")
    return match.group(1), text


def render_note(repo: Repository, revision: str | None, path: str, raw: bytes, publication_ref: str | None, labels: set[str]) -> str:
    _, text = note_text(path, raw)
    document = parse_markdown(text)
    if not preserves_boundary(text):
        raise NotesError(f"{path}: unclosed Markdown block would consume the following note")
    if document.environment.get("duplicate_refs"):
        raise NotesError(f"{path}: repeated reference label")
    for token in descendants(document.tokens):
        if token.type == "heading_open" and token.tag in {"h1", "h2"}:
            raise NotesError(f"{path}: release headings belong to the renderer")
        if token.type in {"html_inline", "html_block"}:
            raise NotesError(f"{path}: raw HTML is unsupported outside code; use Markdown")
    for link in document.links:
        if link.label is None and not is_external(link.destination):
            raise NotesError(f"{path}: put local links in '[label]: ../docs/...' reference definitions")
    lines = text.splitlines(keepends=True)
    for token in document.definitions:
        start, end = token.map
        definition = DEFINITION.fullmatch(lines[start].rstrip("\n"))
        if end != start + 1 or not definition or token.meta["title"]:
            raise NotesError(f"{path}: link definitions must be unindented '[label]: destination', without a title")
        normalized = token.meta["id"]
        if normalized in labels:
            raise NotesError(f"{path}: repeated reference label {normalized!r}; use a label unique to this note")
        labels.add(normalized)
        destination = link_destination(repo, revision, path, token.meta["url"], publication_ref)
        lines[start] = f"[{definition.group(1)}]: {destination}\n"
    return "".join(lines)


def is_release_file(path: str) -> bool:
    return RELEASE_FILE.fullmatch(path) is not None


def select_notes(base: dict[str, bytes], target: dict[str, bytes]) -> dict[str, bytes]:
    base = {path: canonical(raw) for path, raw in base.items()}
    target = {path: canonical(raw) for path, raw in target.items()}
    # Earlier releases' files sit in the base tree, so this also freezes them.
    changed = [path for path, raw in base.items() if target.get(path) != raw]
    if changed:
        raise NotesError("published notes were changed, renamed or removed: " + ", ".join(sorted(changed)))
    for path, raw in target.items():
        if not is_release_file(path):
            note_text(path, raw)
    return {path: target[path] for path in sorted(target.keys() - base.keys()) if not is_release_file(path)}


def select_release(base: dict[str, bytes], target: dict[str, bytes], version: str) -> dict[str, bytes]:
    expected = f"changelog.d/{version}.md"
    new = sorted(path for path in target.keys() - base.keys() if is_release_file(path))
    stray = [path for path in new if path != expected]
    if stray:
        raise NotesError(f"release files are named for the configured version ({expected}): {', '.join(stray)}")
    return {expected: canonical(target[expected])} if expected in new else {}


@dataclass
class Selection:
    base: str | None
    target: str
    notes: dict[str, bytes]
    legacy: str | None = None
    baseline: str = ""
    working_tree: bool = False
    config: str = ""
    release: dict[str, bytes] = field(default_factory=dict)
    links: dict[str, int] = field(default_factory=dict)

    def inputs(self) -> dict[str, str]:
        return {path: digest(raw) for path, raw in self.notes.items()}

    def release_inputs(self) -> dict[str, str]:
        return {path: digest(raw) for path, raw in self.release.items()}


def unreleased_legacy_body(content: str) -> str:
    if not content.startswith(LEGACY_PREFIX) or PROVENANCE_MARKER.search(content) or not content[len(LEGACY_PREFIX):].strip():
        raise NotesError("the migration baseline must be a nonempty, ungenerated v0.12.0 document headed 'Unreleased.'")
    return content[len(LEGACY_PREFIX):]


def migration_baseline(repo: Repository, revision: str | None, legacy: str, freeze: bool = False) -> str:
    original = canonical(repo.read(legacy, LEGACY_PATH)).decode("utf-8")
    pinned_body = unreleased_legacy_body(original)
    current = canonical(repo.read(revision, LEGACY_PATH)).decode("utf-8")
    if PROVENANCE_MARKER.search(current):
        info = snapshot_info(current)
        if info["version"] != "v0.12.0" or not current.startswith(f"# OmniGraph v0.12.0\n\nReleased {info['date']}.\n"):
            raise NotesError("migration provenance requires a dated generated v0.12.0 snapshot")
        return pinned_body
    current_body = unreleased_legacy_body(current)
    if freeze and current != original:
        raise NotesError("the v0.12.0 migration baseline changed; refresh release.json legacy to a durable, already-landed ancestor containing the current unreleased document before preparing the snapshot")
    return pinned_body if freeze else current_body


def select(repo: Repository, base: str | None, target: str, legacy: str | None = None, working_tree: bool = False,
           freeze_legacy: bool = False, release_version: str | None = None) -> Selection:
    target_sha = repo.resolve(target)
    base_sha = repo.resolve(base) if base is not None else None
    if base_sha is not None:
        repo.require_ancestor(base_sha, target_sha)
    base_files = repo.notes(base_sha) if base_sha else {}
    target_files = repo.notes(None if working_tree else target_sha)
    notes = select_notes(base_files, target_files)
    release = select_release(base_files, target_files, release_version) if release_version else {}
    legacy_sha, baseline = None, ""
    if legacy is not None:
        if not SHA.fullmatch(legacy):
            raise NotesError("the legacy baseline must use a full commit SHA")
        legacy_sha = repo.resolve(legacy)
        repo.require_ancestor(legacy_sha, target_sha)
        baseline = migration_baseline(repo, None if working_tree else target_sha, legacy_sha, freeze_legacy)
    return Selection(base_sha, target_sha, notes, legacy_sha, baseline, working_tree,
                     digest(repo.read(None if working_tree else target_sha, CONFIG)), release)


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
        # Only the migration document uses inline local destinations.
        destinations = [link.destination for link in parse_markdown(baseline).links if not is_external(link.destination)]
        for destination in sorted(set(destinations)):
            replacement = link_destination(repo, revision, LEGACY_PATH, destination, publication_ref)
            original = f"]({destination})"
            if baseline.count(original) != destinations.count(destination):
                raise NotesError("legacy links no longer match the pinned simple-inline format")
            if publication_ref:
                baseline = baseline.replace(original, f"]({replacement})")
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
    if config["base"] is not None and (not isinstance(config["base"], str) or not config["base"]):
        raise NotesError(f"{CONFIG}: base must name the previous release, or be null for an initial release")
    if config["legacy"] is not None and (not isinstance(config["legacy"], str) or not SHA.fullmatch(config["legacy"])):
        raise NotesError(f"{CONFIG}: legacy must be null or a full commit SHA")
    return config


def require_configured_selection(repo: Repository, selected: Selection, version: str) -> None:
    config = read_config(repo, None if selected.working_tree else selected.target)
    base = repo.resolve(config["base"]) if config["base"] is not None else None
    if config["version"] != version or base != selected.base or config["legacy"] != selected.legacy:
        raise NotesError("snapshot base, version and legacy source must match changelog.d/release.json")


def snapshot_info(content: str) -> dict:
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
    if not isinstance(info["notes"], dict) or any(
        not isinstance(path, str) or not NOTE_NAME.fullmatch(path) or not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value)
        for path, value in info["notes"].items()
    ) or not isinstance(info["config"], str) or not re.fullmatch(r"[0-9a-f]{64}", info["config"]):
        raise NotesError("snapshot must record note and configuration SHA-256 digests")
    metadata(Selection(info["base"], info["target"], {}, info["legacy"]), info["version"], info["date"])
    return info


def verify_snapshot(repo: Repository, content: str, audited: str | None = None) -> tuple[Selection, dict]:
    content = canonical(content.encode("utf-8")).decode("utf-8")
    info = snapshot_info(content)
    # The input commit can disappear after squash. Its complete input manifest,
    # checked against the audited tree, is the proof; target is provenance only.
    source = repo.resolve(audited or "HEAD")
    selected = select(repo, info["base"], source, info["legacy"], freeze_legacy=True)
    require_configured_selection(repo, selected, info["version"])
    expected_info = metadata(selected, info["version"], info["date"])
    expected_info["target"] = info["target"]
    if info["config"] != expected_info["config"]:
        raise NotesError("release configuration changed after snapshot generation; regenerate it")
    if info["notes"] != expected_info["notes"]:
        raise NotesError("release notes changed after snapshot generation; regenerate it")
    if info != expected_info or content != render(repo, selected, expected_info):
        raise NotesError("snapshot differs from its recorded inputs; regenerate it")
    return selected, expected_info


def render_index(repo: Repository, pending: dict[str, str] | None = None, directory: Path | None = None) -> str:
    directory = directory or repo.root / "docs/releases"
    documents = {}
    for path in directory.glob("v*.md"):
        if not VERSION.fullmatch(path.stem):
            continue
        if path.is_symlink():
            raise NotesError(f"release documents must not be symlinks: {path}")
        documents[path.stem] = path.read_text(encoding="utf-8")
    documents.update(pending or {})
    entries = []
    for version in sorted(documents, key=lambda value: tuple(map(int, VERSION.fullmatch(value).groups())), reverse=True):
        content = documents[version]
        status = ""
        if PROVENANCE_MARKER.search(content):
            info = snapshot_info(content)
            if info["version"] != version or not content.startswith(f"# OmniGraph {version}\n\nReleased {info['date']}.\n"):
                raise NotesError(f"{version}: snapshot filename, heading and provenance disagree")
            status = f": released {info['date']}"
        elif tuple(map(int, VERSION.fullmatch(version).groups())) >= (0, 12, 0):
            config = read_config(repo, None)
            if version != "v0.12.0" or config["version"] != version or not config["legacy"]:
                raise NotesError(f"{version}: expected a generated snapshot or the pinned migration baseline")
            unreleased_legacy_body(canonical(repo.read(config["legacy"], LEGACY_PATH)).decode("utf-8"))
            unreleased_legacy_body(content)
            status = ": unreleased migration baseline; new changes are collected\n  in the documentation CI job's `release-notes-preview` artifact."
        entries.append(f"- [{version}]({version}.md){status}\n")
    return ("# Release notes\n\n"
            "Release documents describe user-visible changes and actions needed before\n"
            "upgrading. The [upgrade guide](../user/operations/upgrade.md) owns the current\n"
            "upgrade procedure.\n\n" + "".join(entries) +
            "\nContributors add individual notes using the\n"
            "[release-note authoring guide](../dev/documentation.md#release-notes).\n")


def validate_output(repo: Repository, output: Path) -> None:
    if output.is_symlink() or not output.resolve().is_relative_to(repo.root.resolve()):
        raise NotesError(f"unsupported output symlink or escaping path: {output}")
    if output.exists() and not output.is_file():
        raise NotesError(f"release outputs must be regular files: {output}")


def write_index(repo: Repository) -> Path:
    path = repo.root / "docs/releases/README.md"
    validate_output(repo, path)
    content = render_index(repo)
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", newline="\n", dir=path.parent, prefix=".release-notes-", delete=False) as temporary:
        temporary_path = Path(temporary.name)
    try:
        temporary_path.write_text(content, encoding="utf-8", newline="\n")
        os.replace(temporary_path, path)
    finally:
        temporary_path.unlink(missing_ok=True)
    return path


def write_snapshot(repo: Repository, selection: Selection, info: dict, replace: bool, replace_legacy: bool) -> Path:
    if selection.working_tree or not info["date"]:
        raise NotesError("snapshots require a date and committed inputs")
    require_configured_selection(repo, selection, info["version"])
    if selection.legacy and selection.baseline != migration_baseline(repo, selection.target, selection.legacy, freeze=True):
        raise NotesError("snapshot baseline differs from its pinned legacy source; select the inputs again")
    path = repo.root / "docs/releases" / f"{info['version']}.md"
    index = path.parent / "README.md"
    for output in (path, index):
        validate_output(repo, output)
    content = render(repo, selection, info)
    if repo.released(info["version"]):
        raise NotesError("a tag already exists for this version; published notes are immutable")
    if path.exists():
        existing = path.read_text(encoding="utf-8")
        original = canonical(repo.read(selection.legacy, LEGACY_PATH)).decode("utf-8") if selection.legacy else None
        migration = replace_legacy and info["version"] == "v0.12.0" and existing == original
        generated = replace and PROVENANCE.search(existing) is not None
        if not migration and not generated:
            raise NotesError(f"{path}: already exists; use --replace for a generated snapshot or --replace-legacy for the pinned baseline")
    index_content = render_index(repo, {info["version"]: content})
    path.parent.mkdir(parents=True, exist_ok=True)
    staged: list[tuple[Path, Path]] = []
    written = []
    try:
        for output, text in ((path, content), (index, index_content)):
            with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", newline="\n", dir=path.parent, prefix=".release-notes-", delete=False) as temporary:
                staged.append((Path(temporary.name), output))
                temporary.write(text)
        for temporary_path, output in staged:
            os.replace(temporary_path, output)
            written.append(output)
    except OSError as error:
        if written:
            raise NotesError("snapshot written but index update failed; retry snapshot with --replace, or run 'python3 scripts/release_notes.py index --write'") from error
        raise
    finally:
        for temporary_path, _ in staged:
            temporary_path.unlink(missing_ok=True)
    return path


def check_working_notes(root: Path, errors: list[str]) -> None:
    try:
        repo = Repository(root)
        config = read_config(repo, None)
        selected = select(repo, config["base"], "HEAD", config["legacy"], working_tree=True)
        render(repo, selected, metadata(selected, config["version"], None))
        path = root / "docs/releases" / f"{config['version']}.md"
        if path.exists():
            current = path.read_text(encoding="utf-8")
            if selected.legacy and not PROVENANCE_MARKER.search(current):
                unreleased_legacy_body(current)
            else:
                recorded, _ = verify_snapshot(repo, current, "HEAD")
                if recorded.inputs() != selected.inputs() or recorded.config != selected.config:
                    raise NotesError("working notes or release configuration changed after snapshot generation")
        index = root / "docs/releases/README.md"
        if not index.exists() or index.read_text(encoding="utf-8") != render_index(repo, directory=index.parent):
            raise NotesError("release index is stale; run 'python3 scripts/release_notes.py index --write'")
    except (NotesError, OSError, ValueError) as error:
        errors.append(f"release notes: {error}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    index = sub.add_parser("index", help="print the release-document index")
    index.add_argument("--write", action="store_true", help="atomically update docs/releases/README.md")
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
        if args.command == "index":
            if args.write:
                print(write_index(repo))
            else:
                print(render_index(repo), end="")
            return 0
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
            print(render(repo, selected, info, publication_ref=None if working else selected.target), end="")
        else:
            print(write_snapshot(repo, selected, info, args.replace, args.replace_legacy))
        return 0
    except (NotesError, OSError, ValueError) as error:
        print(f"release notes: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
