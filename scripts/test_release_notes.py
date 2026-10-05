#!/usr/bin/env python3
"""Release-note contracts, using in-memory Git trees and read-only integration."""

import copy
import hashlib
import importlib.util
import io
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import release_notes as notes

BASE, LEGACY, TARGET, AUDITED, OTHER = (letter * 40 for letter in "abcde")
OLD = "changelog.d/already-shipped.fixed.md"
NEW = "changelog.d/new-predicate.added.md"
GUIDE = "docs/user/queries/index.md"
ORIGINAL = b"# OmniGraph v0.12.0\n\nUnreleased.\n\n## Highlights\n\n- Original [guide](../user/queries/index.md).\n"
ADOPTED = ORIGINAL.replace(b"Original [guide]", b"Updated CLI [guide]") + b"\n- Landed feature one.\n- Landed feature two.\n- Landed fix one.\n- Landed fix two.\n"
CONFIG = json.dumps({"version": "v0.13.0", "base": "previous", "legacy": None}).encode()
RELEASE = "changelog.d/v0.13.0.md"


def release_text(intro="OmniGraph 0.13 makes reads cheaper.", why=None, highlights=3, body="Scans read only named columns."):
    parts = [intro, ""]
    if why is not None:
        parts += ["## Why these changes", "", why, ""]
    if highlights:
        parts += ["## Highlights", ""]
        for number in range(highlights):
            parts += [f"### Highlight {number + 1}", "", body, ""]
    return ("\n".join(parts).rstrip("\n") + "\n").encode()


RELEASE_TEXT = release_text()
V0_12_0 = "a9aa28502a2217aefdf3464dbc6c39fb58d23d65"
V0_12_0_BODY_SHA256 = "60f73177ee77bc39aa7f04da88e833a4957a9f332a764b5487da6f12df4db752"


class MemoryRepository(notes.Repository):
    def __init__(self):
        self.root = Path("/unused")
        self.refs = {"previous": BASE, "HEAD": TARGET, "audited": AUDITED}
        base = {notes.CONFIG: CONFIG, OLD: b"- Already released.\n", GUIDE: b"# Queries\n\n## Predicates\n"}
        self.trees = {BASE: base, LEGACY: {**base, notes.LEGACY_PATH: ORIGINAL}}
        self.trees[TARGET] = {**self.trees[LEGACY], NEW: b"- A new predicate.\n"}
        self.trees[AUDITED] = dict(self.trees[TARGET])
        self.trees[OTHER] = dict(base)
        self.working = dict(self.trees[TARGET])
        self.tags = set()
        self.subjects = {}

    def resolve(self, ref):
        sha = self.refs.get(ref, ref)
        if sha not in self.trees:
            raise notes.NotesError(f"missing revision: {ref}")
        return sha

    def require_ancestor(self, base, target):
        chain = [BASE, LEGACY, TARGET, AUDITED]
        if base not in chain or target not in chain or chain.index(base) > chain.index(target):
            raise notes.NotesError("not a known ancestor")

    def read(self, revision, path):
        try:
            return (self.working if revision is None else self.trees[revision])[path]
        except KeyError as error:
            raise notes.NotesError(f"missing path: {path}") from error

    def notes(self, revision):
        tree = self.working if revision is None else self.trees[revision]
        return {path: raw for path, raw in tree.items() if path.startswith("changelog.d/") and path != notes.CONFIG}

    def object_kind(self, revision, path):
        tree = self.working if revision is None else self.trees[revision]
        if path in tree:
            return "blob"
        return "tree" if any(name.startswith(path.rstrip("/") + "/") for name in tree) else None

    def released(self, version):
        return version in self.tags

    def added_by(self, base, target, path):
        return self.subjects.get(path)


class ReleaseNotesTests(unittest.TestCase):
    def setUp(self):
        self.repo = MemoryRepository()

    def snapshot(self, legacy=None, version="v0.13.0"):
        config = json.dumps({"version": version, "base": "previous", "legacy": legacy}).encode()
        for tree in (self.repo.trees[TARGET], self.repo.trees[AUDITED], self.repo.working):
            tree[notes.CONFIG] = config
        selected = notes.select(self.repo, "previous", "HEAD", legacy)
        info = notes.metadata(selected, version, "2026-10-01")
        return selected, info, notes.render(self.repo, selected, info)

    def test_selects_target_paths_absent_from_base(self):
        self.assertEqual(list(notes.select(self.repo, "previous", "HEAD").notes), [NEW])

    def test_release_file_is_selected_separately_from_notes(self):
        self.repo.trees[TARGET][RELEASE] = RELEASE_TEXT
        selected = notes.select(self.repo, "previous", "HEAD", release_version="v0.13.0")
        self.assertEqual(list(selected.notes), [NEW])
        self.assertEqual(selected.release, {RELEASE: RELEASE_TEXT})
        self.assertEqual(selected.release_inputs(), {RELEASE: notes.digest(RELEASE_TEXT)})

    def test_release_file_for_another_version_is_refused(self):
        self.repo.trees[TARGET]["changelog.d/v0.14.0.md"] = RELEASE_TEXT
        with self.assertRaisesRegex(notes.NotesError, "named for the configured version"):
            notes.select(self.repo, "previous", "HEAD", release_version="v0.13.0")

    def test_published_release_file_is_immutable(self):
        old = "changelog.d/v0.12.0.md"
        self.repo.trees[BASE][old] = RELEASE_TEXT
        self.repo.trees[TARGET][old] = RELEASE_TEXT + b"\nEdited.\n"
        with self.assertRaisesRegex(notes.NotesError, "published notes"):
            notes.select(self.repo, "previous", "HEAD", release_version="v0.13.0")
        self.repo.trees[TARGET][old] = RELEASE_TEXT
        self.assertEqual(notes.select(self.repo, "previous", "HEAD", release_version="v0.13.0").release, {})

    def test_format_one_selection_ignores_release_files(self):
        self.repo.trees[TARGET][RELEASE] = RELEASE_TEXT
        selected = notes.select(self.repo, "previous", "HEAD")
        self.assertEqual(list(selected.notes), [NEW])
        self.assertEqual(selected.release, {})

    def test_pull_request_number_comes_from_the_squash_subject(self):
        for subject, number in (("feat(gq): add list membership (#803)", 803), ("release: v0.12.0 (#877)", 877),
                                ("Merge pull request #680 from x/y", None), ("fix: refer to #12 in text", None),
                                ("fix: zero (#0)", None), (None, None)):
            with self.subTest(subject=subject):
                self.assertEqual(notes.pr_number(subject), number)

    def test_selection_links_notes_to_their_pull_requests(self):
        self.repo.subjects[NEW] = "feat: add predicate (#803)"
        self.repo.trees[TARGET]["changelog.d/direct.fixed.md"] = b"- Direct push.\n"
        selected = notes.select(self.repo, "previous", "HEAD", release_version="v0.13.0")
        self.assertEqual(selected.links, {NEW: 803})
        self.assertEqual(notes.select(self.repo, "previous", "HEAD").links, {})

    def test_note_caps_accept_one_short_paragraph(self):
        notes.check_note_caps(NEW, b"- Add `in` list membership to GQ, pushed into the scan.\n")
        notes.check_note_caps(NEW, b"- Short note.\r\n")
        notes.check_note_caps(NEW, ("- " + "é" * 200 + "\n").encode())
        notes.check_note_caps("changelog.d/upgrade.breaking.md",
                              b"- Upgrade the CLI and server together. See the [upgrade guide][up-guide].\n\n"
                              b"[up-guide]: ../docs/user/operations/upgrade.md\n")

    def test_note_caps_count_visible_text_not_markup_or_definitions(self):
        text = "- `code` [link][caps-link] " + "x" * 180 + "\n\n[caps-link]: ../docs/user/operations/upgrade.md\n"
        notes.check_note_caps(NEW, text.encode())

    def test_note_caps_refuse_long_or_structured_notes(self):
        breaking = "changelog.d/upgrade.breaking.md"
        for path, raw, message in (
            (NEW, b"- " + b"word " * 50 + b"\n", "over the 200-character limit"),
            (NEW, ("- " + "é" * 201 + "\n").encode(), "over the 200-character limit"),
            (NEW, b"- First paragraph.\n\n  Second paragraph.\n", "one bullet with one paragraph"),
            (NEW, b"- Feature:\n  - nested detail\n", "one bullet with one paragraph"),
            (NEW, b"- Example:\n\n  ```text\n  code\n  ```\n", "one bullet with one paragraph"),
            (breaking, b"- Upgrade everything together.\n", "link the guide"),
            (breaking, b"- " + b"word " * 90 + b"[g][g-up].\n\n[g-up]: https://example.com\n", "over the 400-character limit"),
        ):
            with self.subTest(raw=raw[:40]), self.assertRaisesRegex(notes.NotesError, message):
                notes.check_note_caps(path, raw)

    def test_v0_12_0_oversized_notes_fail_the_caps(self):
        repo = notes.Repository(notes.ROOT)
        for path in ("changelog.d/a-release-highlights.added.md",
                     "changelog.d/a-compatibility-and-behavior-changes.changed.md"):
            with self.subTest(path=path), self.assertRaises(notes.NotesError):
                notes.check_note_caps(path, repo.read(V0_12_0, path))

    def test_release_file_splits_intro_why_and_highlights(self):
        release = notes.split_release_file(RELEASE, release_text(why="The write path changed.").decode())
        self.assertEqual(release.intro, "OmniGraph 0.13 makes reads cheaper.")
        self.assertEqual(release.why, "The write path changed.")
        self.assertEqual([title for title, _ in release.highlights], ["Highlight 1", "Highlight 2", "Highlight 3"])
        self.assertTrue(release.highlights_text.startswith("### Highlight 1"))

    def test_release_file_accepts_the_documented_shapes(self):
        notes.check_release_file(RELEASE, release_text(), "v0.13.0", has_breaking=False)
        notes.check_release_file(RELEASE, release_text(why="Why."), "v0.13.0", has_breaking=True)
        notes.check_release_file("changelog.d/v0.13.1.md", release_text(highlights=0), "v0.13.1", has_breaking=False)
        crlf = notes.check_release_file(RELEASE, release_text().replace(b"\n", b"\r\n"), "v0.13.0", has_breaking=False)
        self.assertEqual(crlf, notes.check_release_file(RELEASE, release_text(), "v0.13.0", has_breaking=False))

    def test_release_file_refusals(self):
        many = " ".join(["word"] * 81)
        for raw, version, breaking, message in (
            (release_text(intro=""), "v0.13.0", False, "start with an intro"),
            (release_text(intro=many), "v0.13.0", False, "the intro has 81 words"),
            (release_text(), "v0.13.0", True, "add '## Why these changes'"),
            (release_text(why=many), "v0.13.0", True, "has 81 words"),
            (release_text(highlights=2), "v0.13.0", False, "2 highlights; write 3 to 5"),
            (release_text(highlights=6), "v0.13.1", False, "6 highlights; write 0 to 5"),
            (release_text(body=" ".join(["word"] * 151)), "v0.13.0", False, "highlight 'Highlight 1' has 151 words"),
            (release_text() + b"\n## Roadmap\n\nLater.\n", "v0.13.0", False, "not '## Roadmap'"),
            (b"# OmniGraph\n\n" + release_text(), "v0.13.0", False, "allowed headings"),
            (release_text(intro="Intro.\n\n### Stray"), "v0.13.0", False, "belong under"),
            (release_text().replace(b"## Highlights\n\n", b"## Highlights\n\nLoose text.\n\n"), "v0.13.0", False, "start every highlight"),
            (release_text(why="One.") + b"\n## Why these changes\n\nTwo.\n", "v0.13.0", False, "appears twice"),
            (release_text().replace(b"## Highlights", b"Highlights\n----------"), "v0.13.0", False, "use '## ' headings"),
        ):
            with self.subTest(message=message), self.assertRaisesRegex(notes.NotesError, message):
                notes.check_release_file(RELEASE, raw, version, breaking)

    def test_format_follows_the_version(self):
        self.assertEqual(notes.format_for("v0.12.0"), 1)
        for version in ("v0.13.0", "v0.12.1", "v1.0.0"):
            self.assertEqual(notes.format_for(version), 2)

    def test_previous_tag_reads_only_version_tags(self):
        for base, expected in (("refs/tags/v0.12.0", "v0.12.0"), ("v0.12.0", "v0.12.0"), ("previous", None),
                               (None, None), (BASE, None), ("refs/heads/v0.12.0", None)):
            with self.subTest(base=base):
                self.assertEqual(notes.previous_tag(base), expected)

    def test_format_two_metadata_records_release_links_and_previous(self):
        self.repo.trees[TARGET][RELEASE] = RELEASE_TEXT
        self.repo.subjects[NEW] = "feat: predicate (#803)"
        selected = notes.select(self.repo, "previous", "HEAD", release_version="v0.13.0")
        info = notes.metadata(selected, "v0.13.0", "2026-10-20", 2, "v0.12.0")
        self.assertEqual(info["format"], 2)
        self.assertEqual(info["release"], {RELEASE: notes.digest(RELEASE_TEXT)})
        self.assertEqual(info["links"], {NEW: 803})
        self.assertEqual(info["previous"], "v0.12.0")
        self.assertEqual(set(notes.metadata(selected, "v0.13.0", "2026-10-20")), notes.FORMAT1_KEYS)

    def test_format_two_refuses_a_legacy_baseline(self):
        selected = notes.select(self.repo, BASE, TARGET, LEGACY)
        with self.assertRaisesRegex(notes.NotesError, "no legacy baseline"):
            notes.metadata(selected, "v0.12.0", "2026-10-20", 2)

    def test_snapshot_info_validates_format_two_records(self):
        self.repo.trees[TARGET][RELEASE] = RELEASE_TEXT
        self.repo.subjects[NEW] = "feat: predicate (#803)"
        selected = notes.select(self.repo, "previous", "HEAD", release_version="v0.13.0")
        info = notes.metadata(selected, "v0.13.0", "2026-10-20", 2, "v0.12.0")

        def record(value):
            return "<!-- release-notes: " + json.dumps(value, sort_keys=True, separators=(",", ":")) + " -->\n"

        self.assertEqual(notes.snapshot_info(record(info)), info)
        for forged, message in (
            (dict(info, release={}), "SHA-256 digest of changelog.d/v0.13.0.md"),
            (dict(info, links={OLD: 5}), "pull request numbers"),
            (dict(info, links={NEW: "803"}), "pull request numbers"),
            (dict(info, previous="previous"), "previous release"),
            ({key: value for key, value in info.items() if key != "links"}, "invalid release snapshot provenance"),
        ):
            with self.subTest(message=message), self.assertRaisesRegex(notes.NotesError, message):
                notes.snapshot_info(record(forged))

    def put(self, path, raw):
        for tree in (self.repo.trees[TARGET], self.repo.trees[AUDITED], self.repo.working):
            tree[path] = raw

    def snapshot2(self, version="v0.13.0"):
        self.repo.refs["refs/tags/v0.12.0"] = BASE
        config = json.dumps({"version": version, "base": "refs/tags/v0.12.0", "legacy": None}).encode()
        for tree in (self.repo.trees[TARGET], self.repo.trees[AUDITED], self.repo.working):
            tree[notes.CONFIG] = config
        selected = notes.select(self.repo, "refs/tags/v0.12.0", "HEAD", release_version=version)
        info = notes.metadata(selected, version, "2026-10-20", 2, notes.previous_tag("refs/tags/v0.12.0"))
        return selected, info, notes.render(self.repo, selected, info)

    def test_v0_12_0_body_is_byte_identical(self):
        output = io.StringIO()
        with patch("sys.stdout", output):
            self.assertEqual(notes.main(["body", "--tag", "v0.12.0", "--target", "v0.12.0"]), 0)
        self.assertEqual(hashlib.sha256(output.getvalue().encode("utf-8")).hexdigest(), V0_12_0_BODY_SHA256)

    def test_format_two_page_order_links_and_footer(self):
        self.put(RELEASE, release_text(why="The write path changed."))
        self.put(notes.UPGRADE_GUIDE, b"# Upgrading\n")
        self.put("changelog.d/upgrade.breaking.md",
                 b"- Upgrade together. See the [guide][up-guide].\n\n[up-guide]: ../docs/user/operations/upgrade.md\n")
        self.put("changelog.d/crash.fixed.md", b"- Fix a crash.\n")
        self.repo.subjects.update({NEW: "feat: predicate (#803)", "changelog.d/upgrade.breaking.md": "feat!: contract (#814)"})
        _, _, content = self.snapshot2()
        order = ["OmniGraph 0.13 makes reads cheaper.", "## Highlights", "### Highlight 1", "## Upgrade actions",
                 "The write path changed.", "- Upgrade together.", "## Features", "- A new predicate.",
                 "## Fixes", "- Fix a crash.", "**Full changelog:**"]
        positions = [content.index(text) for text in order]
        self.assertEqual(positions, sorted(positions))
        self.assertIn(f"- A new predicate. ([#803]({notes.REPOSITORY}/pull/803))", content)
        self.assertIn(f"[guide][up-guide]. ([#814]({notes.REPOSITORY}/pull/814))", content)
        self.assertIn("- Fix a crash.\n", content)
        self.assertIn(f"[v0.12.0...v0.13.0]({notes.REPOSITORY}/compare/v0.12.0...v0.13.0)", content)
        self.assertIn("[Upgrade guide](../user/operations/upgrade.md)", content)

    def test_format_two_preview_without_release_file_shows_placeholder(self):
        _, _, content = self.snapshot2()
        self.assertIn(f"_{notes.PENDING_RELEASE_FILE}_", content)
        self.assertNotIn("## Highlights", content)
        self.assertNotIn("Upgrade guide", content)

    def test_patch_release_with_no_notes_renders_intro_and_says_so(self):
        for tree in (self.repo.trees[TARGET], self.repo.trees[AUDITED], self.repo.working):
            del tree[NEW]
        self.put("changelog.d/v0.13.1.md", release_text(highlights=0))
        _, _, content = self.snapshot2("v0.13.1")
        self.assertIn("OmniGraph 0.13 makes reads cheaper.\n\nNo user-visible changes recorded.", content)
        self.assertNotIn("## Highlights", content)
        self.assertNotIn("## Upgrade actions", content)

    def test_format_two_body_drops_header_and_provenance(self):
        self.put(RELEASE, RELEASE_TEXT)
        selected, info, content = self.snapshot2()
        body = notes.render(self.repo, selected, info, "v0.13.0", header=False)
        self.assertTrue(content.startswith("# OmniGraph v0.13.0\n\nReleased 2026-10-20.\n\n<!-- release-notes: "))
        self.assertTrue(body.startswith("OmniGraph 0.13 makes reads cheaper."))
        self.assertNotIn("<!-- release-notes", body)

    def test_release_file_links_follow_the_note_link_rules(self):
        self.put(RELEASE, release_text(intro="See the [query guide][rel-guide].\n\n[rel-guide]: ../docs/user/queries/index.md#predicates"))
        selected, info, content = self.snapshot2()
        self.assertIn("[rel-guide]: ../user/queries/index.md#predicates", content)
        published = notes.render(self.repo, selected, info, "v0.13.0", header=False)
        self.assertIn(f"[rel-guide]: {notes.REPOSITORY}/blob/v0.13.0/docs/user/queries/index.md#predicates", published)
        self.put(RELEASE, release_text(intro="See [guide](../docs/user/queries/index.md)."))
        with self.assertRaisesRegex(notes.NotesError, "reference definitions"):
            self.snapshot2()

    def test_format_two_snapshot_requires_the_release_file(self):
        selected, info, _ = self.snapshot2()
        with tempfile.TemporaryDirectory() as directory:
            self.repo.root = Path(directory)
            with self.assertRaisesRegex(notes.NotesError, "write changelog.d/v0.13.0.md"):
                notes.write_snapshot(self.repo, selected, info, False, False)
            self.assertFalse((self.repo.root / "docs/releases").exists())

    def test_format_two_snapshot_round_trips_through_verify_and_body(self):
        self.put(RELEASE, RELEASE_TEXT)
        self.repo.subjects[NEW] = "feat: predicate (#803)"
        selected, info, content = self.snapshot2()
        with tempfile.TemporaryDirectory() as directory:
            self.repo.root = Path(directory)
            path = notes.write_snapshot(self.repo, selected, info, False, False)
            self.assertEqual(path.read_text(), content)
            self.repo.trees[AUDITED]["docs/releases/v0.13.0.md"] = path.read_bytes()
        self.assertEqual(notes.verify_snapshot(self.repo, content, AUDITED)[1], info)
        self.repo.refs["refs/tags/v0.13.0"] = AUDITED
        output = io.StringIO()
        with patch.object(notes, "Repository", return_value=self.repo), patch("sys.stdout", output):
            self.assertEqual(notes.main(["body", "--tag", "v0.13.0", "--target", AUDITED]), 0)
        body = output.getvalue()
        self.assertTrue(body.startswith("OmniGraph 0.13 makes reads cheaper."))
        self.assertNotIn("<!-- release-notes", body)
        self.assertIn(f"([#803]({notes.REPOSITORY}/pull/803))", body)

    def test_verify_tolerates_links_that_appear_after_the_squash(self):
        self.put(RELEASE, RELEASE_TEXT)
        _, info, content = self.snapshot2()
        self.assertEqual(info["links"], {})
        self.repo.subjects[NEW] = "release: prepare v0.13.0 (#900)"
        self.assertEqual(notes.verify_snapshot(self.repo, content, AUDITED)[1]["links"], {})

    def test_verify_refuses_forged_links_and_late_release_edits(self):
        self.put(RELEASE, RELEASE_TEXT)
        self.repo.subjects[NEW] = "feat: predicate (#803)"
        _, info, content = self.snapshot2()

        def encode(value):
            return json.dumps(value, sort_keys=True, separators=(",", ":"))

        forged = content.replace(encode(info), encode(dict(info, links={NEW: 999})))
        with self.assertRaisesRegex(notes.NotesError, "disagree with history"):
            notes.verify_snapshot(self.repo, forged, AUDITED)
        self.repo.trees[AUDITED][RELEASE] = RELEASE_TEXT + b"\nLate edit.\n"
        with self.assertRaisesRegex(notes.NotesError, "release file changed after snapshot"):
            notes.verify_snapshot(self.repo, content, AUDITED)

    def test_docs_check_applies_caps_before_the_snapshot_exists(self):
        self.snapshot2()
        self.repo.working["changelog.d/wordy.fixed.md"] = b"- " + b"word " * 50 + b"\n"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "docs/releases").mkdir(parents=True)
            errors = []
            with patch.object(notes, "Repository", return_value=self.repo):
                notes.check_working_notes(root, errors)
        self.assertTrue(any("over the 200-character limit" in error for error in errors), errors)

    def test_preview_cli_uses_format_two_for_new_versions(self):
        self.snapshot2()
        output = io.StringIO()
        with patch.object(notes, "Repository", return_value=self.repo), patch("sys.stdout", output):
            self.assertEqual(notes.main(["preview", "--target", TARGET]), 0)
        self.assertIn(f"_{notes.PENDING_RELEASE_FILE}_", output.getvalue())
        self.assertIn('"format":2', output.getvalue())

    def test_release_note_gate_matches_the_composer_note_names(self):
        spec = importlib.util.spec_from_file_location("check_pr_title", notes.ROOT / "scripts/check-pr-title.py")
        gate = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(gate)
        self.assertEqual(set(gate.CATEGORIES), set(notes.CATEGORIES))
        for path in ("changelog.d/x.added.md", "changelog.d/a-b.breaking.md", "changelog.d/v0.13.0.md",
                     "changelog.d/x.other.md", "changelog.d/sub/x.fixed.md", "docs/x.added.md"):
            match = notes.NOTE_NAME.fullmatch(path)
            with self.subTest(path=path):
                self.assertEqual(bool(gate.NOTE_PATH.fullmatch(path)), bool(match) and match.group(1) in notes.CATEGORIES)

    def test_unreleased_edits_and_reverts_use_final_tree(self):
        self.repo.trees[TARGET][NEW] = b"- Final wording.\n"
        self.assertEqual(notes.select(self.repo, BASE, TARGET).notes[NEW], b"- Final wording.\n")
        del self.repo.trees[TARGET][NEW]
        self.assertEqual(notes.select(self.repo, BASE, TARGET).notes, {})

    def test_published_edit_removal_and_rename_are_refused(self):
        for changed in ({OLD: b"- Changed.\n"}, {}, {"changelog.d/renamed.fixed.md": b"- Already released.\n"}):
            with self.subTest(changed=changed), self.assertRaisesRegex(notes.NotesError, "published notes"):
                notes.select_notes({OLD: b"- Already released.\n"}, changed)

    def test_backport_identity_is_branch_local(self):
        fragment = {NEW: b"- A new predicate.\n"}
        self.assertEqual(notes.select_notes({}, fragment), fragment)
        self.assertEqual(notes.select_notes(fragment, fragment), {})

    def test_missing_revision_and_unrelated_base_fail(self):
        for base in ("missing", OTHER):
            with self.subTest(base=base), self.assertRaises(notes.NotesError):
                notes.select(self.repo, base, TARGET)

    def test_initial_mode_is_explicit(self):
        selected = notes.select(self.repo, None, TARGET)
        self.assertEqual(set(selected.notes), {OLD, NEW})
        self.assertIsNone(selected.base)

    def test_working_preview_includes_untracked_edits_and_says_so(self):
        local = "changelog.d/local.fixed.md"
        self.repo.working[local] = b"- Local change.\n"
        selected = notes.select(self.repo, BASE, TARGET, working_tree=True)
        content = notes.render(self.repo, selected, notes.metadata(selected, "v0.13.0", None))
        self.assertIn("Working-tree preview", content)
        self.assertIn(local, selected.notes)
        self.assertNotIn(local, notes.select(self.repo, BASE, TARGET).notes)

    def test_note_shape_validation(self):
        for path, raw in (("changelog.d/x.other.md", b"- Text.\n"), (NEW, b"\n"),
                          (NEW, b"- \n"), (NEW, b"- Missing newline."), ("changelog.d/nested/x.fixed.md", b"- Text.\n")):
            with self.subTest(path=path, raw=raw), self.assertRaises(notes.NotesError):
                notes.note_text(path, raw)

    def test_order_is_category_then_filename(self):
        self.repo.trees[TARGET].update({"changelog.d/z.fixed.md": b"- Last fix.\n", "changelog.d/a.breaking.md": b"- Upgrade first.\n"})
        _, _, content = self.snapshot()
        self.assertLess(content.index("## Upgrade actions"), content.index("## Features"))
        self.assertLess(content.index("## Features"), content.index("## Fixes"))

    def test_no_change_release_is_explicit(self):
        del self.repo.trees[TARGET][NEW]
        self.assertIn("No user-visible changes recorded.", self.snapshot()[2])

    def test_local_reference_rebases_for_snapshot_and_release_tag(self):
        self.repo.trees[TARGET][NEW] = b"- See [the guide][new-guide].\n\n[new-guide]: ../docs/user/queries/index.md#predicates\n"
        selected, info, content = self.snapshot()
        self.assertIn("[new-guide]: ../user/queries/index.md#predicates", content)
        published = notes.render(self.repo, selected, info, "v0.13.0")
        self.assertIn("[new-guide]: https://github.com/ModernRelay/omnigraph/blob/v0.13.0/docs/user/queries/index.md#predicates", published)

    def test_missing_target_anchor_and_escaping_destinations_fail(self):
        for destination in ("../missing.md", "../docs/user/queries/index.md#missing", "../../outside.md", "#predicates", "../docs/user/queries/index.md?q=1"):
            self.repo.trees[TARGET][NEW] = f"- See [guide][new].\n\n[new]: {destination}\n".encode()
            with self.subTest(destination=destination), self.assertRaises(notes.NotesError):
                self.snapshot()

    def test_reference_labels_share_case_and_whitespace_normalization(self):
        self.repo.trees[TARGET][NEW] = b"- One.\n\n[A  Guide]: https://example.com/a\n"
        self.repo.trees[TARGET]["changelog.d/second.fixed.md"] = b"- Two.\n\n[a guide]: https://example.com/b\n"
        with self.assertRaisesRegex(notes.NotesError, "repeated reference label"):
            self.snapshot()

    def test_inline_local_links_are_refused_but_external_links_stay(self):
        self.repo.trees[TARGET][NEW] = b"- [guide](../docs/user/queries/index.md).\n"
        with self.assertRaisesRegex(notes.NotesError, "reference definitions"):
            self.snapshot()
        self.repo.trees[TARGET][NEW] = b"- [website](https://example.com/a).\n"
        self.assertIn("[website](https://example.com/a)", self.snapshot()[2])

    def test_code_examples_are_byte_preserved(self):
        for code in (
            "- Example: `[x](../missing.md)`.\n",
            "- Example: `first\n[x]: ../missing.md\nlast`.\n",
            "- Example:\n\n  ```markdown\n  [x]: ../missing.md\n  ```\n",
            "- Example:\n\n  ````markdown\n  ```\n  [x]: ../missing.md\n  ````\n",
            "- Example:\n\n  ```markdown\n  ```not-a-close\n  [x]: ../missing.md\n  ```\n",
        ):
            with self.subTest(code=code):
                self.repo.trees[TARGET][NEW] = code.encode()
                self.assertIn(code, self.snapshot()[2])

    def test_bad_definition_and_unclosed_examples_fail(self):
        for text in ('- See guide.\n\n [x]: ../docs/user/queries/index.md\n',
                     '- See guide.\n\n[x]: ../docs/user/queries/index.md "title"\n',
                     '- Example:\n\n```text\nunfinished\n'):
            self.repo.trees[TARGET][NEW] = text.encode()
            with self.subTest(text=text), self.assertRaises(notes.NotesError):
                self.snapshot()

    def test_commonmark_code_spans_include_literal_backslash_and_unmatched_ticks(self):
        for text in ('- A backslash is `\\`.\n', '- A Windows path is `C:\\temp\\`.\n',
                     '- An unmatched ` is literal prose.\n'):
            self.repo.trees[TARGET][NEW] = text.encode()
            self.assertIn(text, self.snapshot()[2])

    def test_real_links_in_nested_lists_and_multiline_syntax_are_refused(self):
        for text in ('- Feature:\n  - Details:\n    [guide](../docs/user/queries/index.md)\n',
                     '- See [guide](\n../docs/user/queries/index.md\n).\n'):
            self.repo.trees[TARGET][NEW] = text.encode()
            with self.subTest(text=text), self.assertRaisesRegex(notes.NotesError, "reference definitions"):
                selected = notes.select(self.repo, BASE, TARGET)
                notes.render(self.repo, selected, notes.metadata(selected, "v0.13.0", "2026-10-01"), "v0.13.0")

    def test_generated_code_examples_pass_the_same_documentation_checker(self):
        self.repo.trees[TARGET][NEW] = b"- Example: `[x](../missing.md)`.\n\n  ```markdown\n  [x](../also-missing.md)\n  ```\n"
        _, _, content = self.snapshot()
        checker = notes.docs_checker()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "docs/releases/v0.13.0.md"
            path.parent.mkdir(parents=True)
            path.write_text(content)
            errors = []
            with patch.object(checker, "ROOT", root):
                checker.check_links([path], errors)
            self.assertEqual(errors, [])

    def test_doc_link_scanner_checks_unused_references_and_skips_image_alt_links(self):
        checker = notes.docs_checker()
        self.assertEqual(checker.local_link_targets("[unused]: ../missing.md\n"), [(1, "../missing.md")])
        links = checker.local_link_targets("![an [alt link](../not-outgoing.md)](https://example.com/image.png)\n")
        self.assertEqual([destination for _, destination in links], ["https://example.com/image.png"])

    def test_raw_html_attributes_are_refused_only_outside_code(self):
        for text in ('- <a href="../docs/user/queries/index.md">guide</a>.\n',
                     '- <img\nsrc="../missing.png">\n',
                     '- <a HREF = "https://example.com">site</a>.\n'):
            self.repo.trees[TARGET][NEW] = text.encode()
            with self.subTest(text=text), self.assertRaisesRegex(notes.NotesError, "raw HTML"):
                self.snapshot()
        for text in ('- Example: `<a href="../missing.md">guide</a>`.\n',
                     '- Example:\n\n```html\n<img src="../missing.png">\n```\n',
                     '- See [site](https://example.com/?src=notes).\n'):
            self.repo.trees[TARGET][NEW] = text.encode()
            self.assertIn(text, self.snapshot()[2])

    def test_unclosed_html_container_cannot_hide_following_notes(self):
        self.repo.trees[TARGET][NEW] = b"- Fix.\n\n<details><summary>Details</summary>\n\nA detail.\n"
        with self.assertRaisesRegex(notes.NotesError, "raw HTML"):
            self.snapshot()
        self.repo.trees[TARGET][NEW] = b"- Example: `<details><summary>Details</summary>`.\n"
        self.assertIn("`<details><summary>Details</summary>`", self.snapshot()[2])

    def test_deterministic_dated_snapshot_and_audited_descendant(self):
        selected, info, content = self.snapshot()
        self.assertEqual(content, notes.render(self.repo, selected, info))
        checked, checked_info = notes.verify_snapshot(self.repo, content, AUDITED)
        self.assertEqual(checked_info, info)
        self.assertEqual(checked.inputs(), selected.inputs())

    def test_squash_keeps_manifest_valid_without_the_old_input_commit(self):
        _, info, content = self.snapshot()
        del self.repo.trees[TARGET]
        self.repo.refs["HEAD"] = AUDITED
        ancestor = self.repo.require_ancestor

        def only_durable_ancestors(base, target):
            if base == TARGET:
                raise notes.NotesError("squash removed this ancestry")
            ancestor(base, target)

        with patch.object(self.repo, "require_ancestor", side_effect=only_durable_ancestors):
            selected, verified = notes.verify_snapshot(self.repo, content, AUDITED)
        self.assertEqual(selected.target, AUDITED)
        self.assertEqual(verified["target"], TARGET)
        self.assertEqual(verified, info)

    def test_squash_does_not_hide_omitted_or_changed_manifest_inputs(self):
        _, info, content = self.snapshot()
        del self.repo.trees[TARGET]
        self.repo.refs["HEAD"] = AUDITED
        for path, value in ((NEW, b"- Different.\n"), ("changelog.d/extra.fixed.md", b"- Omitted note.\n")):
            previous = dict(self.repo.trees[AUDITED])
            self.repo.trees[AUDITED][path] = value
            with self.subTest(path=path), self.assertRaisesRegex(notes.NotesError, "after snapshot"):
                notes.verify_snapshot(self.repo, content, AUDITED)
            self.repo.trees[AUDITED] = previous
        forged = dict(info, notes={})
        malformed = content.replace(json.dumps(info, sort_keys=True, separators=(",", ":")), json.dumps(forged, sort_keys=True, separators=(",", ":")))
        with self.assertRaisesRegex(notes.NotesError, "after snapshot"):
            notes.verify_snapshot(self.repo, malformed, AUDITED)

    def test_snapshot_configuration_is_semantically_bound(self):
        selected, info, content = self.snapshot()
        with tempfile.TemporaryDirectory() as directory:
            self.repo.root = Path(directory)
            for version, base in (("v0.14.0", selected.base), ("v0.13.0", None)):
                candidate = copy.copy(selected)
                candidate.base = base
                with self.subTest(version=version, base=base), self.assertRaisesRegex(notes.NotesError, "must match"):
                    notes.write_snapshot(self.repo, candidate, notes.metadata(candidate, version, "2026-10-01"), False, False)
            self.assertFalse((self.repo.root / "docs/releases").exists())
        config = json.loads(CONFIG)
        config["base"] = LEGACY
        self.repo.trees[AUDITED][notes.CONFIG] = json.dumps(config).encode()
        forged = dict(info, config=notes.digest(self.repo.trees[AUDITED][notes.CONFIG]))
        edited = content.replace(json.dumps(info, sort_keys=True, separators=(",", ":")), json.dumps(forged, sort_keys=True, separators=(",", ":")))
        with self.assertRaisesRegex(notes.NotesError, "must match"):
            notes.verify_snapshot(self.repo, edited, AUDITED)

    def test_crlf_inputs_have_the_same_meaning_and_hashes(self):
        _, info, content = self.snapshot(LEGACY, "v0.12.0")
        config = json.dumps(json.loads(self.repo.working[notes.CONFIG]), indent=2).encode() + b"\n"
        for tree in (self.repo.trees[TARGET], self.repo.trees[AUDITED], self.repo.working):
            tree[notes.CONFIG] = config
        selected = notes.select(self.repo, BASE, TARGET, LEGACY)
        info = notes.metadata(selected, "v0.12.0", "2026-10-01")
        content = notes.render(self.repo, selected, info)
        self.repo.working = {path: raw.replace(b"\n", b"\r\n") for path, raw in self.repo.working.items()}
        local = notes.select(self.repo, BASE, TARGET, LEGACY, working_tree=True)
        self.assertEqual(local.inputs(), selected.inputs())
        self.assertEqual(local.config, selected.config)
        self.assertEqual(local.baseline, selected.baseline)
        self.assertEqual(notes.verify_snapshot(self.repo, content.replace("\n", "\r\n"), AUDITED)[1], info)
        self.repo.working[OLD] = b"- Real content change.\r\n"
        with self.assertRaisesRegex(notes.NotesError, "published notes"):
            notes.select(self.repo, BASE, TARGET, LEGACY, working_tree=True)

    def test_committed_preview_links_include_the_selected_sha(self):
        self.repo.trees[TARGET][NEW] = b"- See [guide][new].\n\n[new]: ../docs/user/queries/index.md#predicates\n"
        self.snapshot(LEGACY, "v0.12.0")
        output = io.StringIO()
        with patch.object(notes, "Repository", return_value=self.repo), patch("sys.stdout", output):
            self.assertEqual(notes.main(["preview", "--target", TARGET]), 0)
        self.assertIn(f"/blob/{TARGET}/docs/user/queries/index.md#predicates", output.getvalue())
        self.assertIn(f"Original [guide]({notes.REPOSITORY}/blob/{TARGET}/docs/user/queries/index.md)", output.getvalue())

    def test_initial_release_configuration_works_through_snapshot_check_and_body(self):
        config = json.dumps({"version": "v0.13.0", "base": None, "legacy": None}).encode()
        for tree in (self.repo.trees[TARGET], self.repo.trees[AUDITED], self.repo.working):
            tree[notes.CONFIG] = config
        selected = notes.select(self.repo, None, TARGET)
        info = notes.metadata(selected, "v0.13.0", "2026-10-01")
        content = notes.render(self.repo, selected, info)
        self.assertEqual(set(info["notes"]), {OLD, NEW})
        with tempfile.TemporaryDirectory() as directory:
            self.repo.root = Path(directory)
            path = notes.write_snapshot(self.repo, selected, info, False, False)
            errors = []
            with patch.object(notes, "Repository", return_value=self.repo):
                notes.check_working_notes(self.repo.root, errors)
            self.assertEqual(errors, [])
            self.repo.trees[AUDITED]["docs/releases/v0.13.0.md"] = path.read_bytes()
        self.repo.refs["refs/tags/v0.13.0"] = AUDITED
        with patch.object(notes, "Repository", return_value=self.repo), patch("sys.stdout", io.StringIO()):
            self.assertEqual(notes.main(["body", "--tag", "v0.13.0", "--target", AUDITED]), 0)

    def test_initial_flag_cannot_override_noninitial_snapshot_configuration(self):
        with patch.object(notes, "Repository", return_value=self.repo), patch("sys.stderr", io.StringIO()) as output:
            self.assertEqual(notes.main(["snapshot", "--target", TARGET, "--date", "2026-10-01", "--initial-release"]), 1)
        self.assertIn("must match", output.getvalue())

    def test_provenance_or_body_edits_fail(self):
        _, _, content = self.snapshot()
        for edited in (content.replace("A new predicate.", "Different."), content.replace('"format":1', '"format":2'), content.replace('"date":"2026-10-01"', '"date":null')):
            with self.subTest(edited=edited), self.assertRaises(notes.NotesError):
                notes.verify_snapshot(self.repo, edited, AUDITED)

    def test_drift_in_notes_or_release_config_fails_before_publication(self):
        _, _, content = self.snapshot()
        for path, raw in ((NEW, b"- Later wording.\n"), ("changelog.d/later.fixed.md", b"- Later change.\n"), (notes.CONFIG, CONFIG + b"\n")):
            before = dict(self.repo.trees[AUDITED])
            self.repo.trees[AUDITED][path] = raw
            with self.subTest(path=path), self.assertRaisesRegex(notes.NotesError, "after snapshot"):
                notes.verify_snapshot(self.repo, content, AUDITED)
            self.repo.trees[AUDITED] = before

    def test_audited_document_links_are_validated(self):
        self.repo.trees[TARGET][NEW] = b"- See [guide][new].\n\n[new]: ../docs/user/queries/index.md\n"
        self.repo.trees[AUDITED][NEW] = self.repo.trees[TARGET][NEW]
        _, _, content = self.snapshot()
        del self.repo.trees[AUDITED][GUIDE]
        with self.assertRaisesRegex(notes.NotesError, "missing link target"):
            notes.verify_snapshot(self.repo, content, AUDITED)

    def test_legacy_body_is_preserved_and_tag_links_use_same_content(self):
        selected, info, content = self.snapshot(LEGACY, "v0.12.0")
        self.assertIn(ORIGINAL.decode().split("\n", 4)[4], content)
        self.assertEqual(content.count("Original [guide]"), 1)
        published = notes.render(self.repo, selected, info, "v0.12.0")
        self.assertIn("Original [guide](https://github.com/ModernRelay/omnigraph/blob/v0.12.0/docs/user/queries/index.md)", published)
        self.repo.trees[AUDITED][notes.LEGACY_PATH] = content.encode()
        del self.repo.trees[AUDITED][GUIDE]
        with self.assertRaisesRegex(notes.NotesError, "missing link target"):
            notes.verify_snapshot(self.repo, content, AUDITED)

    def test_adoption_preview_preserves_selected_tree_legacy_changes(self):
        self.snapshot(LEGACY, "v0.12.0")
        self.repo.trees[TARGET][notes.LEGACY_PATH] = ADOPTED
        output = io.StringIO()
        with patch.object(notes, "Repository", return_value=self.repo), patch("sys.stdout", output):
            self.assertEqual(notes.main(["preview", "--target", TARGET]), 0)
        preview = output.getvalue()
        self.assertIn("Updated CLI [guide]", preview)
        self.assertIn(f"/blob/{TARGET}/docs/user/queries/index.md", preview)
        for number in ("feature one", "feature two", "fix one", "fix two"):
            self.assertIn(f"Landed {number}.", preview)
        self.assertNotIn("Original [guide]", preview)
        self.assertEqual(self.repo.trees[LEGACY][notes.LEGACY_PATH], ORIGINAL)
        self.assertEqual(self.repo.working[notes.LEGACY_PATH], ORIGINAL)

    def test_adoption_working_preview_preserves_local_legacy_changes(self):
        self.snapshot(LEGACY, "v0.12.0")
        self.repo.working[notes.LEGACY_PATH] = ADOPTED
        selected = notes.select(self.repo, BASE, TARGET, LEGACY, working_tree=True)
        preview = notes.render(self.repo, selected, notes.metadata(selected, "v0.12.0", None))
        self.assertIn(ADOPTED.decode().removeprefix(notes.LEGACY_PREFIX), preview)
        self.assertEqual(self.repo.trees[TARGET][notes.LEGACY_PATH], ORIGINAL)

    def test_adoption_docs_check_and_index_accept_current_unreleased_body(self):
        self.snapshot(LEGACY, "v0.12.0")
        for tree in (self.repo.trees[TARGET], self.repo.working):
            tree[notes.LEGACY_PATH] = ADOPTED
        with tempfile.TemporaryDirectory() as directory:
            self.repo.root = Path(directory)
            path = self.repo.root / notes.LEGACY_PATH
            path.parent.mkdir(parents=True)
            path.write_bytes(ADOPTED)
            index = notes.render_index(self.repo)
            self.assertIn("unreleased migration baseline", index)
            (path.parent / "README.md").write_text(index)
            errors = []
            with patch.object(notes, "Repository", return_value=self.repo):
                notes.check_working_notes(self.repo.root, errors)
            self.assertEqual(errors, [])

    def test_adoption_with_stale_pin_cannot_create_or_verify_snapshot(self):
        self.snapshot(LEGACY, "v0.12.0")
        for tree in (self.repo.trees[TARGET], self.repo.trees[AUDITED]):
            tree[notes.LEGACY_PATH] = ADOPTED
        selected = notes.select(self.repo, BASE, TARGET, LEGACY)
        info = notes.metadata(selected, "v0.12.0", "2026-10-01")
        content = notes.render(self.repo, selected, info)
        with tempfile.TemporaryDirectory() as directory:
            self.repo.root = Path(directory)
            path = self.repo.root / notes.LEGACY_PATH
            path.parent.mkdir(parents=True)
            path.write_bytes(ADOPTED)
            with self.assertRaisesRegex(notes.NotesError, "refresh release.json legacy"):
                notes.write_snapshot(self.repo, selected, info, False, True)
            self.assertEqual(path.read_bytes(), ADOPTED)
            self.assertFalse((path.parent / "README.md").exists())
        with self.assertRaisesRegex(notes.NotesError, "refresh release.json legacy"):
            notes.verify_snapshot(self.repo, content, AUDITED)
        self.repo.trees[AUDITED][notes.LEGACY_PATH] = content.encode()
        with self.assertRaisesRegex(notes.NotesError, "differs from its recorded inputs"):
            notes.verify_snapshot(self.repo, content, AUDITED)

    def test_refreshing_pin_to_landed_ancestor_allows_snapshot_and_verification(self):
        config = json.dumps({"version": "v0.12.0", "base": "previous", "legacy": TARGET}).encode()
        self.repo.trees[TARGET][notes.LEGACY_PATH] = ADOPTED
        self.repo.trees[AUDITED][notes.LEGACY_PATH] = ADOPTED
        self.repo.trees[AUDITED][notes.CONFIG] = config
        self.repo.working[notes.CONFIG] = config
        selected = notes.select(self.repo, BASE, AUDITED, TARGET)
        info = notes.metadata(selected, "v0.12.0", "2026-10-01")
        with tempfile.TemporaryDirectory() as directory:
            self.repo.root = Path(directory)
            path = self.repo.root / notes.LEGACY_PATH
            path.parent.mkdir(parents=True)
            path.write_bytes(ADOPTED)
            notes.write_snapshot(self.repo, selected, info, False, True)
            content = path.read_text()
        self.repo.trees[AUDITED][notes.LEGACY_PATH] = content.encode()
        verified, _ = notes.verify_snapshot(self.repo, content, AUDITED)
        self.assertEqual(verified.baseline, ADOPTED.decode().removeprefix(notes.LEGACY_PREFIX))
        self.assertEqual(self.repo.trees[LEGACY][notes.LEGACY_PATH], ORIGINAL)

    def test_malformed_provenance_cannot_turn_raw_legacy_into_generated_snapshot(self):
        _, _, generated = self.snapshot(LEGACY, "v0.12.0")
        copied_provenance = notes.PROVENANCE.search(generated).group(0).encode()
        for suffix in (b"\n<!-- release-notes: broken -->\n", b"\n<!-- release-notes: missing end\n",
                       b"\n<!--  Release-Notes: {} -->\n", b"\n" + copied_provenance + b"\n"):
            self.repo.trees[TARGET][notes.LEGACY_PATH] = ADOPTED + suffix
            with self.subTest(suffix=suffix), self.assertRaises((notes.NotesError, ValueError)):
                notes.select(self.repo, BASE, TARGET, LEGACY)

    def test_generated_legacy_append_and_tamper_still_fail_docs_check(self):
        _, _, content = self.snapshot(LEGACY, "v0.12.0")
        self.repo.trees[TARGET][notes.LEGACY_PATH] = content.encode()
        for edited in (content + "\n- Late addition.\n", content.replace("Original [guide]", "Tampered [guide]")):
            self.repo.working[notes.LEGACY_PATH] = edited.encode()
            with self.subTest(edited=edited), tempfile.TemporaryDirectory() as directory:
                self.repo.root = Path(directory)
                path = self.repo.root / notes.LEGACY_PATH
                path.parent.mkdir(parents=True)
                path.write_text(edited)
                errors = []
                with patch.object(notes, "Repository", return_value=self.repo):
                    notes.check_working_notes(self.repo.root, errors)
                self.assertTrue(any("differs from its recorded inputs" in error for error in errors), errors)

    def test_snapshot_writes_only_output_and_refuses_unsafe_replacement(self):
        selected, info, _ = self.snapshot()
        before = copy.deepcopy(self.repo.trees)
        with tempfile.TemporaryDirectory() as directory:
            self.repo.root = Path(directory)
            path = notes.write_snapshot(self.repo, selected, info, False, False)
            self.assertTrue(path.is_file())
            with self.assertRaisesRegex(notes.NotesError, "already exists"):
                notes.write_snapshot(self.repo, selected, info, False, False)
            notes.write_snapshot(self.repo, selected, info, True, False)
            self.repo.tags.add("v0.13.0")
            with self.assertRaisesRegex(notes.NotesError, "tag already exists"):
                notes.write_snapshot(self.repo, selected, info, True, False)
        self.assertEqual(self.repo.trees, before)

    def test_legacy_replacement_requires_explicit_option_and_exact_original(self):
        selected, info, _ = self.snapshot(LEGACY, "v0.12.0")
        with tempfile.TemporaryDirectory() as directory:
            self.repo.root = Path(directory)
            path = self.repo.root / notes.LEGACY_PATH
            path.parent.mkdir(parents=True)
            path.write_bytes(ORIGINAL)
            with self.assertRaises(notes.NotesError):
                notes.write_snapshot(self.repo, selected, info, False, False)
            path.write_bytes(ORIGINAL + b"\n- Uncollected addition.\n")
            with self.assertRaises(notes.NotesError):
                notes.write_snapshot(self.repo, selected, info, False, True)
            path.write_bytes(ORIGINAL)
            notes.write_snapshot(self.repo, selected, info, False, True)
            self.assertIn("Original [guide]", path.read_text())

    def test_post_migration_working_check_catches_stale_snapshot(self):
        _, _, content = self.snapshot()
        self.repo.working[NEW] = b"- Changed after snapshot.\n"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "docs/releases/v0.13.0.md"
            path.parent.mkdir(parents=True)
            path.write_text(content)
            errors = []
            with patch.object(notes, "Repository", return_value=self.repo):
                notes.check_working_notes(root, errors)
            self.assertTrue(any("after snapshot" in error for error in errors), errors)

    def test_snapshot_replacement_failure_preserves_existing_output(self):
        selected, info, content = self.snapshot()
        with tempfile.TemporaryDirectory() as directory:
            self.repo.root = Path(directory)
            path = notes.write_snapshot(self.repo, selected, info, False, False)
            with patch.object(notes.os, "replace", side_effect=OSError("interrupted")):
                with self.assertRaises(OSError):
                    notes.write_snapshot(self.repo, selected, info, True, False)
            self.assertEqual(path.read_text(), content)
            self.assertEqual(list(path.parent.glob(".release-notes-*")), [])

    def test_snapshot_updates_index_and_orders_versions_numerically(self):
        selected, info, _ = self.snapshot()
        with tempfile.TemporaryDirectory() as directory:
            self.repo.root = Path(directory)
            releases = self.repo.root / "docs/releases"
            releases.mkdir(parents=True)
            (releases / "v0.9.0.md").write_text("# Old release\n")
            (releases / "v0.10.0.md").write_text("# Old release\n")
            notes.write_snapshot(self.repo, selected, info, False, False)
            index = (releases / "README.md").read_text()
            self.assertIn("[v0.13.0](v0.13.0.md): released 2026-10-01", index)
            self.assertLess(index.index("[v0.13.0]"), index.index("[v0.10.0]"))
            self.assertLess(index.index("[v0.10.0]"), index.index("[v0.9.0]"))
            output = io.StringIO()
            with patch.object(notes, "Repository", return_value=self.repo), patch("sys.stdout", output):
                self.assertEqual(notes.main(["index"]), 0)
            self.assertEqual(output.getvalue(), index)

    def test_index_refuses_invalid_snapshot_before_writing_either_output(self):
        selected, info, content = self.snapshot()
        with tempfile.TemporaryDirectory() as directory:
            self.repo.root = Path(directory)
            releases = self.repo.root / "docs/releases"
            releases.mkdir(parents=True)
            (releases / "v0.14.0.md").write_text(content)
            index = releases / "README.md"
            index.write_text("Existing index.\n")
            with self.assertRaisesRegex(notes.NotesError, "disagree"):
                notes.write_snapshot(self.repo, selected, info, False, False)
            self.assertFalse((releases / "v0.13.0.md").exists())
            self.assertEqual(index.read_text(), "Existing index.\n")

    def test_index_directory_is_refused_before_snapshot_write(self):
        selected, info, _ = self.snapshot()
        with tempfile.TemporaryDirectory() as directory:
            self.repo.root = Path(directory)
            releases = self.repo.root / "docs/releases"
            (releases / "README.md").mkdir(parents=True)
            with self.assertRaisesRegex(notes.NotesError, "regular files"):
                notes.write_snapshot(self.repo, selected, info, False, False)
            self.assertFalse((releases / "v0.13.0.md").exists())

    def test_interrupted_index_update_has_explicit_repair_after_tagging(self):
        selected, info, content = self.snapshot()
        with tempfile.TemporaryDirectory() as directory:
            self.repo.root = Path(directory)
            releases = self.repo.root / "docs/releases"
            releases.mkdir(parents=True)
            index = releases / "README.md"
            index.write_text("Old index.\n")
            replace = notes.os.replace
            def fail_index(source, destination):
                if destination == index:
                    raise OSError("interrupted index update")
                replace(source, destination)
            with patch.object(notes.os, "replace", side_effect=fail_index):
                with self.assertRaisesRegex(notes.NotesError, "snapshot written but index update failed"):
                    notes.write_snapshot(self.repo, selected, info, False, False)
            self.assertEqual((releases / "v0.13.0.md").read_text(), content)
            self.assertEqual(index.read_text(), "Old index.\n")
            self.assertEqual(list(releases.glob(".release-notes-*")), [])
            self.repo.tags.add("v0.13.0")
            with patch.object(notes, "Repository", return_value=self.repo), patch("sys.stdout", io.StringIO()):
                self.assertEqual(notes.main(["index", "--write"]), 0)
            self.assertIn("[v0.13.0]", index.read_text())

    def test_index_repair_preserves_old_index_on_validation_or_replace_failure(self):
        _, _, content = self.snapshot()
        with tempfile.TemporaryDirectory() as directory:
            self.repo.root = Path(directory)
            releases = self.repo.root / "docs/releases"
            releases.mkdir(parents=True)
            index = releases / "README.md"
            index.write_text("Keep this index.\n")
            bad = releases / "v0.14.0.md"
            bad.write_text(content)
            with self.assertRaises(notes.NotesError):
                notes.write_index(self.repo)
            self.assertEqual(index.read_text(), "Keep this index.\n")
            bad.unlink()
            with patch.object(notes.os, "replace", side_effect=OSError("interrupted")):
                with self.assertRaises(OSError):
                    notes.write_index(self.repo)
            self.assertEqual(index.read_text(), "Keep this index.\n")
            self.assertEqual(list(releases.glob(".release-notes-*")), [])

    def test_index_marks_frozen_baseline_unreleased_then_dated_snapshot(self):
        selected, info, _ = self.snapshot(LEGACY, "v0.12.0")
        with tempfile.TemporaryDirectory() as directory:
            self.repo.root = Path(directory)
            path = self.repo.root / notes.LEGACY_PATH
            path.parent.mkdir(parents=True)
            path.write_bytes(ORIGINAL)
            self.assertIn("unreleased migration baseline", notes.render_index(self.repo))
            notes.write_snapshot(self.repo, selected, info, False, True)
            index = (path.parent / "README.md").read_text()
            self.assertNotIn("unreleased", index)
            self.assertIn("released 2026-10-01", index)

    def test_body_requires_release_tag_to_select_audited_source(self):
        self.repo.refs["refs/tags/v0.13.0"] = TARGET
        output = io.StringIO()
        with patch.object(notes, "Repository", return_value=self.repo), patch("sys.stderr", output):
            self.assertEqual(notes.main(["body", "--tag", "v0.13.0", "--target", AUDITED]), 1)
        self.assertIn("does not select the audited source", output.getvalue())

    def test_body_cli_publishes_validated_audited_snapshot(self):
        self.repo.trees[TARGET][NEW] = b"- See [guide][new].\n\n[new]: ../docs/user/queries/index.md#predicates\n"
        self.repo.trees[AUDITED][NEW] = self.repo.trees[TARGET][NEW]
        _, info, content = self.snapshot()
        self.repo.trees[AUDITED]["docs/releases/v0.13.0.md"] = content.encode()
        self.repo.refs["refs/tags/v0.13.0"] = AUDITED
        output = io.StringIO()
        with patch.object(notes, "Repository", return_value=self.repo), patch("sys.stdout", output):
            self.assertEqual(notes.main(["body", "--tag", "v0.13.0", "--target", AUDITED]), 0)
        published = output.getvalue()
        self.assertIn("Released 2026-10-01.", published)
        self.assertIn("/blob/v0.13.0/docs/user/queries/index.md#predicates", published)
        self.assertEqual(json.loads(notes.PROVENANCE.search(published).group(1)), info)

    def test_real_pinned_migration_source_is_complete(self):
        repo = notes.Repository(notes.ROOT)
        raw = repo.read("d0bbe07fc666d1bf8e89815d39f4dfe20be18d24", notes.LEGACY_PATH)
        self.assertEqual(notes.digest(raw), "de05c5362a0acc9c942324e5a753932b6d229627e96bf9f4fb43b6e5f0b88f77")
        self.assertEqual(sum(line.startswith(b"- ") for line in raw.splitlines()), 50)


class AddedByGitTests(unittest.TestCase):
    def test_added_by_reads_the_adding_commit_subject(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            def git(*args):
                subprocess.run(["git", "-C", directory, "-c", "user.name=t", "-c", "user.email=t@example.com",
                                "-c", "commit.gpgsign=false", *args], check=True, capture_output=True)

            git("init", "-q", "-b", "main")
            git("config", "diff.renames", "copies")
            git("config", "log.follow", "true")
            (root / "changelog.d").mkdir()
            (root / "changelog.d/first.added.md").write_text("- First.\n")
            git("add", "-A")
            git("commit", "-q", "-m", "feat: first (#5)")
            repo = notes.Repository(root)
            base = repo.resolve("HEAD")
            git("mv", "changelog.d/first.added.md", "changelog.d/moved.added.md")
            (root / "changelog.d/second.added.md").write_text("- Second.\n")
            git("add", "-A")
            git("commit", "-q", "-m", "feat: second (#6)")
            self.assertEqual(repo.added_by(None, "HEAD", "changelog.d/second.added.md"), "feat: second (#6)")
            self.assertEqual(repo.added_by(None, "HEAD", "changelog.d/moved.added.md"), "feat: second (#6)")
            self.assertEqual(repo.added_by(None, base, "changelog.d/first.added.md"), "feat: first (#5)")
            self.assertIsNone(repo.added_by(base, "HEAD", "changelog.d/first.added.md"))
            self.assertIsNone(repo.added_by(None, "HEAD", "changelog.d/untracked.added.md"))


if __name__ == "__main__":
    unittest.main()
