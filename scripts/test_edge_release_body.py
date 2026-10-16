#!/usr/bin/env python3
"""Edge release text: compatibility facts, pinning and the unreleased notes."""

import json
import unittest

import edge_release_body as body

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

    def test_a_long_release_keeps_upgrade_actions_and_links_the_rest(self):
        features = "".join(f"- Feature {index} " + "x" * 200 + ".\n\n" for index in range(1000))
        preview = PREVIEW.replace("- A new predicate.\n", features)
        text = compose(preview=preview)
        self.assertLessEqual(len(text), body.BODY_LIMIT)
        self.assertIn("- Regenerate saved plans.", text)
        self.assertNotIn("Feature 0 ", text)
        self.assertIn(f"/tree/{COMMIT}/changelog.d", text)


if __name__ == "__main__":
    unittest.main()
