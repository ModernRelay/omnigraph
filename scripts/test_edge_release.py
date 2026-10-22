#!/usr/bin/env python3
"""Edge release decisions and text: version order, compatibility facts,
pinning, the unreleased notes and which old builds are kept."""

import json
import tempfile
import unittest
import unittest.mock
from pathlib import Path

import edge_release as body

COMMIT = "9c2b3679e8f66062252e7ed6270a9478b4b77413"
TAG = "edge-20261016-9c2b367"
PROVENANCE = '<!-- release-notes: {"format":1} -->\n'
PREVIEW = (
    "# OmniGraph v0.14.0\n\nUnreleased preview.\n\n" + PROVENANCE + "\n"
    "## Upgrade actions\n\n- Regenerate saved plans.\n\n"
    "## Features\n\n- A new predicate.\n"
)
EDGE = body.Facts("0.14.0-edge+9c2b367", 16, (14, 16), "0.13")
STABLE_TEXT = "omnigraph 0.13.0\ninternal-schema 14 (serves v14 to v14)\n"
# The lines of v0.13.0's crates/omnigraph-api-types/src/lib.rs that name the contract.
STABLE_SOURCE = (
    "/// The single request/response discriminator for the v0.13 HTTP contract.\n"
    'pub const HTTP_API_CONTRACT_HEADER: &str = "omnigraph-http-api";\n'
    "/// Exact header value; consumers must reject missing or repeated values.\n"
    'pub const HTTP_API_CONTRACT: &str = "0.13";\n'
)


def compose(**overrides):
    arguments = dict(
        repository="ModernRelay/omnigraph",
        commit=COMMIT,
        build_tag=TAG,
        build=EDGE,
        stable_tag="v0.13.0",
        stable=body.parse_facts(STABLE_TEXT),
        preview=PREVIEW,
        rolling=False,
    )
    arguments.update(overrides)
    return body.compose(**arguments)


class EdgeReleaseBodyTests(unittest.TestCase):
    def test_reads_json_and_the_text_of_releases_without_it(self):
        json_facts = body.parse_facts(json.dumps({
            "version": "0.14.0-edge+9c2b367", "channel": "edge", "commit": COMMIT,
            "http_contract": "0.13", "internal_schema": {"writes": 16, "serves": [14, 16]},
        }))
        self.assertEqual(json_facts, EDGE)
        self.assertEqual(body.parse_facts(STABLE_TEXT), body.Facts("0.13.0", 14, (14, 14), None))
        text = STABLE_TEXT + "http-contract 0.13\ncommit " + COMMIT + "\n"
        self.assertEqual(body.parse_facts(text).http_contract, "0.13")
        for raw in ("", "omnigraph 0.13.0\n", '{"version": "0.14.0"}'):
            with self.subTest(raw=raw), self.assertRaises(body.BodyError):
                body.parse_facts(raw)

    def test_reads_the_contract_of_a_release_that_does_not_report_it_from_its_source(self):
        stable = body.parse_facts(STABLE_TEXT)
        self.assertEqual(body.with_source_contract(stable, STABLE_SOURCE).http_contract, "0.13")
        reported = body.Facts("0.14.0", 16, (14, 16), "0.14")
        self.assertEqual(body.with_source_contract(reported, STABLE_SOURCE.replace('"0.13"', '"0.14"')), reported)
        self.assertEqual(body.with_source_contract(stable, "pub const OTHER: &str = \"0.13\";\n"), stable)
        with self.assertRaisesRegex(body.BodyError, "reports HTTP contract 0.14 but its source defines 0.13"):
            body.with_source_contract(reported, STABLE_SOURCE)
        with self.assertRaisesRegex(body.BodyError, "more than once"):
            body.with_source_contract(stable, STABLE_SOURCE * 2)

    def test_body_says_the_contract_changed_from_the_stable_source(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            files = {
                "build.json": json.dumps({
                    "version": "0.14.0-edge+9c2b367", "http_contract": "0.14",
                    "internal_schema": {"writes": 16, "serves": [14, 16]},
                }),
                "stable.out": STABLE_TEXT,
                "lib.rs": STABLE_SOURCE,
                "notes.md": PREVIEW,
            }
            for name, text in files.items():
                (root / name).write_text(text, encoding="utf-8")
            arguments = [
                "body", "--repository", "ModernRelay/omnigraph", "--commit", COMMIT, "--build-tag", TAG,
                "--build-version", str(root / "build.json"), "--notes", str(root / "notes.md"),
            ]
            stable = ["--stable-tag", "v0.13.0", "--stable-version", str(root / "stable.out")]
            with unittest.mock.patch("sys.stdout") as stdout:
                code = body.main(arguments + stable + ["--stable-source", str(root / "lib.rs")])
            self.assertEqual(code, 0)
            text = "".join(call.args[0] for call in stdout.write.call_args_list)
            self.assertIn("| HTTP contract | 0.14 | 0.13 |", text)
            self.assertIn("This build speaks 0.14 and v0.13.0 speaks 0.13", text)
            with unittest.mock.patch("sys.stderr"):
                self.assertEqual(body.main(arguments + ["--stable-source", str(root / "lib.rs")]), 1)

    def test_warns_when_stable_cannot_open_what_the_build_writes(self):
        text = compose()
        self.assertIn("| Storage format written | 16 | 14 |", text)
        self.assertIn("left at storage format 16, which v0.13.0 cannot open", text)
        self.assertIn("v0.13.0 does not report its contract", text)

    def test_states_an_unchanged_format_and_contract(self):
        stable = body.Facts("0.13.0", 16, (14, 16), "0.13")
        text = compose(stable=stable)
        self.assertIn("This build writes storage format 16, as v0.13.0 does.", text)
        self.assertIn("This build speaks HTTP contract 0.13, as v0.13.0 does.", text)
        self.assertNotIn("cannot open", text)

    def test_names_formats_the_build_no_longer_opens_and_contract_changes(self):
        build = body.Facts("0.15.0-edge+9c2b367", 18, (17, 18), "0.14")
        text = compose(build=build, stable_tag="v0.14.0", stable=body.Facts("0.14.0", 16, (14, 16), "0.13"))
        self.assertIn("left at storage format 18, which v0.14.0 cannot open", text)
        self.assertIn("does not open storage format 16, which v0.14.0 writes", text)
        self.assertIn(f"/blob/{COMMIT}/docs/user/operations/upgrade.md", text)
        self.assertIn("This build speaks 0.14 and v0.14.0 speaks 0.13", text)

    def test_describes_the_build_alone_without_a_stable_release(self):
        text = compose(stable_tag=None, stable=None)
        self.assertIn("No stable release is published to compare with.", text)
        self.assertNotIn("latest stable release", text)

    def test_pins_the_build_and_only_the_rolling_text_says_it_moves(self):
        pinned = compose()
        self.assertIn(f"VERSION={TAG} bash", pinned)
        self.assertIn(f"-Version {TAG}", pinned)
        self.assertIn("gh attestation verify <archive> --repo ModernRelay/omnigraph", pinned)
        self.assertNotIn("rolling `edge` release", pinned)
        self.assertIn(f"always holds the newest edge build, now `{TAG}`", compose(rolling=True))

    def test_refuses_a_tag_that_does_not_name_the_commit(self):
        for tag in ("edge-20261016-aaaaaaa", "edge-2026-10-16-9c2b367", "v0.14.0"):
            with self.subTest(tag=tag), self.assertRaises(body.BodyError):
                compose(build_tag=tag)
        with self.assertRaises(body.BodyError):
            compose(commit=COMMIT[:7])

    def test_keeps_notes_in_order_under_one_heading(self):
        text = compose()
        self.assertIn("as v0.14.0 will publish them", text)
        self.assertIn(PROVENANCE, text)
        self.assertNotIn("# OmniGraph v0.14.0", text)
        self.assertNotIn("\n## Upgrade actions", text)
        self.assertLess(text.index("### Upgrade actions"), text.index("### Features"))
        empty = compose(preview="# OmniGraph v0.14.0\n\nUnreleased preview.\n\nNo user-visible changes recorded.\n")
        self.assertIn("No user-visible changes are recorded since the last stable release.", empty)
        with self.assertRaises(body.BodyError):
            compose(preview="## Features\n")

    def test_drops_the_format_two_footer_that_links_an_unpublished_tag(self):
        footer = ("**Full changelog:** [v0.13.0...v0.14.0](https://github.com/ModernRelay/omnigraph/compare/v0.13.0...v0.14.0)"
                  " · [Upgrade guide](https://github.com/ModernRelay/omnigraph/blob/abc/docs/user/operations/upgrade.md)\n")
        preview = PREVIEW.replace('"format":1', '"format":2') + "\n" + footer
        text = compose(preview=preview)
        self.assertNotIn("Full changelog", text)
        self.assertNotIn("/compare/v0.13.0...v0.14.0", text)
        self.assertTrue(text.endswith("- A new predicate.\n"), text[-200:])
        alone = compose(preview=PREVIEW + "\n[Upgrade guide](https://example.invalid/upgrade.md)\n")
        self.assertNotIn("example.invalid", alone)

    def test_a_long_release_keeps_upgrade_actions_and_links_the_rest(self):
        features = "".join(f"- Feature {index} " + "x" * 200 + ".\n\n" for index in range(1000))
        preview = PREVIEW.replace("- A new predicate.\n", features)
        text = compose(preview=preview)
        self.assertLessEqual(len(text), body.BODY_LIMIT)
        self.assertIn("- Regenerate saved plans.", text)
        self.assertNotIn("Feature 0 ", text)
        self.assertIn(f"/tree/{COMMIT}/changelog.d", text)

    def test_refuses_a_package_version_not_above_the_latest_stable(self):
        for package, stable in (("0.13.0-edge+9c2b367", "v0.13.0"), ("0.12.9", "v0.13.0"), ("0.13.0", "v0.13.0")):
            with self.subTest(package=package, stable=stable), self.assertRaisesRegex(body.BodyError, "advance the workspace version"):
                body.require_newer_than_stable(package, stable)
        body.require_newer_than_stable("0.14.0-edge+9c2b367", "v0.13.0")
        body.require_newer_than_stable("0.13.1", "v0.13.0")
        body.require_newer_than_stable("0.13.0", None)
        with self.assertRaisesRegex(body.BodyError, "would sort before v0.13.0"):
            compose(build=body.Facts("0.13.0-edge+9c2b367", 14, (14, 14), "0.13"))

    def test_check_version_exits_by_the_same_rule(self):
        self.assertEqual(body.main(["check-version", "--package-version", "0.14.0", "--stable-tag", "v0.13.0"]), 0)
        self.assertEqual(body.main(["check-version", "--package-version", "0.13.0", "--stable-tag", "v0.13.0"]), 1)
        self.assertEqual(body.main(["check-version", "--package-version", "0.13.0"]), 0)

    def test_marks_the_build_with_its_commit_and_storage_format(self):
        text = compose()
        self.assertTrue(text.startswith("<!-- edge-build: "))
        self.assertEqual(body.build_writes(text), 16)
        self.assertIn(f'"commit": "{COMMIT}"', text.splitlines()[0])
        for unmarked in (None, "", "Edge build of `9c2b367`.", '<!-- edge-build: {"writes": "16"} -->'):
            with self.subTest(body=unmarked):
                self.assertIsNone(body.build_writes(unmarked))


def release(tag, day, writes=None, draft=False):
    text = "" if writes is None else f'<!-- edge-build: {{"commit": "x", "format": 1, "writes": {writes}}} -->\nText.'
    return {"tag_name": tag, "created_at": f"2026-10-{day:02d}T04:30:00Z", "draft": draft, "body": text}


class EdgeBuildRetentionTests(unittest.TestCase):
    def test_keeps_the_newest_builds_and_the_newest_of_each_storage_format(self):
        releases = [
            release("edge-20261001-aaaaaaa", 1, writes=13),
            release("edge-20261002-bbbbbbb", 2, writes=13),
            release("edge-20261003-ccccccc", 3, writes=14),
            release("edge-20261004-ddddddd", 4, writes=16),
            release("edge-20261005-eeeeeee", 5, writes=16),
            release("edge-20261006-fffffff", 6, writes=16),
        ]
        deleted = body.releases_to_delete(releases, retain=2, current="edge-20261006-fffffff")
        # Kept: the two newest (05, 06), and the newest build that wrote 13 (02) and 14 (03).
        self.assertEqual(deleted, ["edge-20261004-ddddddd", "edge-20261001-aaaaaaa"])

    def test_never_lists_the_current_build_other_tags_or_drafts(self):
        releases = [
            release("v0.13.0", 1),
            release("edge", 2),
            release("edge-20261003-ccccccc", 3, draft=True),
            release("edge-20261004-ddddddd", 4),
            release("edge-20261005-eeeeeee", 5),
        ]
        self.assertEqual(body.releases_to_delete(releases, retain=1, current="edge-20261004-ddddddd"), [])
        self.assertEqual(body.releases_to_delete(releases, retain=1, current="edge-20261005-eeeeeee"), ["edge-20261004-ddddddd"])
        with self.assertRaises(body.BodyError):
            body.releases_to_delete(releases, retain=0, current="edge-20261005-eeeeeee")

    def test_prune_reads_one_release_per_line(self):
        releases = [release("edge-20261004-ddddddd", 4, writes=16), release("edge-20261005-eeeeeee", 5, writes=16)]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "releases.jsonl"
            path.write_text("".join(json.dumps(item) + "\n" for item in releases), encoding="utf-8")
            with unittest.mock.patch("sys.stdout") as stdout:
                code = body.main(["prune", "--releases", str(path), "--retain", "1", "--current", "edge-20261005-eeeeeee"])
            self.assertEqual(code, 0)
            self.assertEqual("".join(call.args[0] for call in stdout.write.call_args_list), "edge-20261004-ddddddd\n")


if __name__ == "__main__":
    unittest.main()
