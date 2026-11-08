"""Shared CommonMark classification; source Markdown remains the authored text."""

from dataclasses import dataclass

try:
    from markdown_it import MarkdownIt
except ModuleNotFoundError as error:
    raise SystemExit(
        "Documentation tools require Python 3.10+ and the pinned packages in "
        "scripts/requirements-docs.txt. Install them with your virtual environment's "
        "python -m pip install -r scripts/requirements-docs.txt."
    ) from error


@dataclass
class Link:
    line: int
    destination: str
    label: str | None


def descendants(tokens):
    for token in tokens:
        yield token
        # Image children become plain alt text, not outgoing links.
        if token.children and token.type != "image":
            yield from descendants(token.children)


@dataclass
class Document:
    tokens: list
    environment: dict

    @property
    def definitions(self):
        return [token for token in self.tokens if token.type == "definition"]

    @property
    def links(self):
        found = []
        for block in self.tokens:
            line = block.map[0] + 1 if block.map else 1
            for token in descendants(block.children or []):
                if token.type in {"link_open", "image"}:
                    destination = token.attrGet("href" if token.type == "link_open" else "src")
                    found.append(Link(line, destination, token.meta.get("label")))
        return found


def parse(text: str) -> Document:
    parser = MarkdownIt("commonmark", {"store_labels": True, "inline_definitions": True})
    parser.enable(["table", "strikethrough"])
    environment = {}
    return Document(parser.parse(text, environment), environment)


def link_targets(text: str) -> list[tuple[int, str]]:
    document = parse(text)
    # Definitions include unused links. Used reference links need no second check.
    return [(token.map[0] + 1, token.meta["url"]) for token in document.definitions] + [
        (link.line, link.destination) for link in document.links if link.label is None
    ]


def preserves_boundary(text: str) -> bool:
    sentinel = "omnigraph-release-note-boundary-check"
    tokens = parse(text + "\n\n" + sentinel + "\n").tokens
    return len(tokens) >= 3 and [token.type for token in tokens[-3:]] == [
        "paragraph_open", "inline", "paragraph_close"
    ] and tokens[-2].content == sentinel and tokens[-3].level == 0
