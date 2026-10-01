#!/usr/bin/env python3
"""Release-note contracts, using in-memory Git trees and read-only integration."""

import copy
import io
import json
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
CONFIG = json.dumps({"version": "v0.13.0", "base": "previous", "legacy": None}).encode()


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


class ReleaseNotesTests(unittest.TestCase):
    def setUp(self):
        self.repo = MemoryRepository()

    def snapshot(self, legacy=None, version="v0.13.0"):
        selected = notes.select(self.repo, "previous", "HEAD", legacy)
        info = notes.metadata(selected, version, "2026-10-01")
        return selected, info, notes.render(self.repo, selected, info)

    def test_selects_target_paths_absent_from_base(self):
        self.assertEqual(list(notes.select(self.repo, "previous", "HEAD").notes), [NEW])

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
            "- Example:\n\n  ```markdown\n[x]: ../missing.md\n  ```\n",
            "- Example:\n\n  ````markdown\n  ```\n[x]: ../missing.md\n  ````\n",
            "- Example:\n\n  ```markdown\n  ```not-a-close\n[x]: ../missing.md\n  ```\n",
        ):
            with self.subTest(code=code):
                self.repo.trees[TARGET][NEW] = code.encode()
                self.assertIn(code, self.snapshot()[2])

    def test_bad_definition_and_unclosed_examples_fail(self):
        for text in ('- See guide.\n\n [x]: ../docs/user/queries/index.md\n',
                     '- See guide.\n\n[x]: ../docs/user/queries/index.md "title"\n',
                     '- Example: `unfinished.\n', '- Example:\n\n```text\nunfinished\n'):
            self.repo.trees[TARGET][NEW] = text.encode()
            with self.subTest(text=text), self.assertRaises(notes.NotesError):
                self.snapshot()

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

    def test_deterministic_dated_snapshot_and_audited_descendant(self):
        selected, info, content = self.snapshot()
        self.assertEqual(content, notes.render(self.repo, selected, info))
        checked, checked_info = notes.verify_snapshot(self.repo, content, AUDITED)
        self.assertEqual(checked_info, info)
        self.assertEqual(checked.inputs(), selected.inputs())

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

    def test_direct_legacy_addition_fails_working_preview(self):
        self.repo.working[notes.LEGACY_PATH] += b"\n- Uncollected change.\n"
        with self.assertRaisesRegex(notes.NotesError, "baseline is frozen"):
            notes.select(self.repo, BASE, TARGET, LEGACY, working_tree=True)

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


if __name__ == "__main__":
    unittest.main()
