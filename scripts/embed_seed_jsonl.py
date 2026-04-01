#!/usr/bin/env python3

import argparse
import json
import math
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path


DEFAULT_BASE_URL = "https://generativelanguage.googleapis.com/v1beta"


def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def render_value(value) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        rendered = [render_value(item) for item in value]
        rendered = [item for item in rendered if item]
        return ", ".join(rendered)
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value).strip()


def build_embedding_text(type_name: str, data: dict, fields: list[str]) -> str:
    parts: list[str] = [f"type: {type_name}"]
    for field in fields:
        value = render_value(data.get(field))
        if value:
            parts.append(f"{field}: {value}")
    return "\n".join(parts)


def normalize(values: list[float]) -> list[float]:
    norm = math.sqrt(sum(value * value for value in values))
    if norm == 0:
        return values
    return [value / norm for value in values]


def embed_text(
    *,
    api_key: str,
    base_url: str,
    model: str,
    dimension: int,
    text: str,
    retries: int,
    backoff_ms: int,
) -> list[float]:
    payload = json.dumps(
        {
            "model": f"models/{model}",
            "content": {"parts": [{"text": text}]},
            "taskType": "RETRIEVAL_DOCUMENT",
            "outputDimensionality": dimension,
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        f"{base_url.rstrip('/')}/models/{model}:embedContent",
        data=payload,
        headers={
            "Content-Type": "application/json",
            "x-goog-api-key": api_key,
        },
        method="POST",
    )

    attempt = 0
    while True:
        attempt += 1
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                body = response.read().decode("utf-8")
                parsed = json.loads(body)
                values = parsed["embedding"]["values"]
                if len(values) != dimension:
                    raise RuntimeError(
                        f"expected {dimension} embedding dimensions, got {len(values)}"
                    )
                return normalize([float(value) for value in values])
        except urllib.error.HTTPError as exc:
            status = exc.code
            body = exc.read().decode("utf-8", errors="replace")
            retryable = status in {408, 429, 500, 502, 503, 504}
            if retryable and attempt <= retries:
                time.sleep(backoff_ms / 1000.0)
                continue
            raise RuntimeError(
                f"embedding request failed with status {status}: {body}"
            ) from exc
        except urllib.error.URLError as exc:
            if attempt <= retries:
                time.sleep(backoff_ms / 1000.0)
                continue
            raise RuntimeError(f"embedding request failed: {exc}") from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate explicit embedding vectors for Omnigraph seed JSONL rows."
    )
    parser.add_argument("--input", required=True, help="Path to the source seed JSONL.")
    parser.add_argument("--output", required=True, help="Path to write the embedded JSONL.")
    parser.add_argument(
        "--spec",
        required=True,
        help="Path to the embeddings JSON spec describing types, fields, and dimension.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing embedding fields instead of preserving them.",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="Retry count for retryable Gemini failures.",
    )
    parser.add_argument(
        "--backoff-ms",
        type=int,
        default=500,
        help="Sleep duration between retry attempts.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not api_key:
        print("GEMINI_API_KEY is required", file=sys.stderr)
        return 1

    spec = load_json(Path(args.spec))
    model = spec.get("model", "gemini-embedding-2-preview")
    dimension = int(spec["dimension"])
    types = spec["types"]
    base_url = os.environ.get("OMNIGRAPH_GEMINI_BASE_URL", DEFAULT_BASE_URL).strip()

    input_path = Path(args.input)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    transformed = 0
    total = 0
    with input_path.open("r", encoding="utf-8") as source, output_path.open(
        "w", encoding="utf-8"
    ) as dest:
        for raw_line in source:
            line = raw_line.strip()
            if not line:
                continue
            total += 1
            row = json.loads(line)
            type_name = row.get("type")
            data = row.get("data")
            config = types.get(type_name)
            if config and isinstance(data, dict):
                target = config["target"]
                if args.force or data.get(target) in (None, []):
                    text = build_embedding_text(type_name, data, config["fields"])
                    if text.strip():
                        data[target] = embed_text(
                            api_key=api_key,
                            base_url=base_url,
                            model=model,
                            dimension=dimension,
                            text=text,
                            retries=args.retries,
                            backoff_ms=args.backoff_ms,
                        )
                        transformed += 1
            dest.write(json.dumps(row, ensure_ascii=True))
            dest.write("\n")

    print(
        json.dumps(
            {
                "input": str(input_path),
                "output": str(output_path),
                "rows": total,
                "embedded_rows": transformed,
                "dimension": dimension,
                "model": model,
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
