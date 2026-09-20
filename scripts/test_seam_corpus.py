import contextlib
import io
import os
import pathlib
import sys
import tempfile
import unittest
from unittest.mock import patch

import seam_corpus


class SeamCorpusTests(unittest.TestCase):
    def test_catalog_reports_each_macro_invocation(self):
        source = '''// A preceding comment is not the invocation.
decide_seam! {
    /// A declaration with documentation.
    /// A second documentation line.
    pub static FIRST = (
        "test.first",
        Mutation,
        [Fail, Skip],
    );
}

omnigraph_seams::decide_seam! {
    pub static SECOND = ("test.second", AnyWrite, [Contention]);
}

crate::seams::
    decide_seam! {
    /** Documentation may mention { braces }. */
    pub static THIRD = ("test.third", Mutation, [Fail]);
}
'''
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            src = root / "src"
            src.mkdir()
            (src / "seams.rs").write_text(source)
            store_places = root / "store_places.rs"
            store_places.write_text("pub static STORE_PLACES = [];\n")
            with (
                patch.object(seam_corpus, "ROOT", root),
                patch.object(seam_corpus, "ENGINE_SRC", src),
                patch.object(seam_corpus, "STORE_PLACES", store_places),
            ):
                seams, places, problems = seam_corpus.catalog()
                self.assertEqual(places, set())
                self.assertEqual(problems, [])
                self.assertEqual(seams, {
                    "test.first": ("src/seams.rs:2", "Mutation", ("Fail", "Skip")),
                    "test.second": ("src/seams.rs:12", "AnyWrite", ("Contention",)),
                    "test.third": ("src/seams.rs:16", "Mutation", ("Fail",)),
                })

    def test_nested_cases_exclude_hidden_paths_and_symlinks(self):
        fixture = "--- seam\nat: test.site\naction: fail\n"
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            nested = root / "v2" / "planner"
            nested.mkdir(parents=True)
            (root / "case.gqt").write_text(fixture)
            (nested / "case.gqt").write_text(fixture)
            (nested / ".hidden.gqt").write_text(fixture)
            (nested / "other.txt").write_text(fixture)
            hidden = root / ".hidden"
            hidden.mkdir()
            (hidden / "case.gqt").write_text(fixture)
            (root / "linked.gqt").symlink_to(nested / "case.gqt")
            (root / "linked_directory").symlink_to(nested, target_is_directory=True)
            if sys.platform == "linux":
                (root / os.fsdecode(b"invalid_\xff.gqt")).write_text(fixture)
            with patch.object(seam_corpus, "CASES", root):
                self.assertEqual(seam_corpus.corpus(), [
                    ("case.gqt", "test.site", "fail", "", "case.gqt:1"),
                    ("v2/planner/case.gqt", "test.site", "fail", "", "v2/planner/case.gqt:1"),
                ])
            self.assertEqual(seam_corpus.case_files(root / "linked_directory"), [])

    def test_contention_requires_contention_and_keeps_legacy_fail(self):
        for effects, action, expected in [
            (("Fail",), "contention", 1),
            (("Skip",), "contention", 1),
            (("Contention",), "contention", 0),
            (("Fail", "Contention"), "contention", 0),
            (("Fail",), "fail", 0),
            (("Contention",), "fail", 0),
            (("Skip",), "fail", 1),
        ]:
            with self.subTest(effects=effects, action=action):
                with (
                    patch.object(seam_corpus, "catalog", return_value=({"test.site": ("src/site.rs:1", "Mutation", effects)}, set(), [])),
                    patch.object(seam_corpus, "corpus", return_value=[("case.gqt", "test.site", action, "", "case.gqt:1")]),
                    patch("sys.argv", ["seam_corpus.py", "--check"]),
                    contextlib.redirect_stdout(io.StringIO()),
                    contextlib.redirect_stderr(io.StringIO()),
                ):
                    self.assertEqual(seam_corpus.main(), expected)


if __name__ == "__main__":
    unittest.main()
