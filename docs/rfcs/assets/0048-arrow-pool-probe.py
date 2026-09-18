#!/usr/bin/env python3
"""Run the optional Arrow-pool qualification outside the production workspace.

Requires Python 3.11+, Cargo, and the registry dependencies cached by the
workspace build. Offline resolution must retain versions/checksums from the
workspace lockfile. The temporary crate and output remain available for audit.
"""

import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import tomllib


def main():
    assets = Path(__file__).resolve().parent
    root = assets.parents[2]
    lock_bytes = (root / "Cargo.lock").read_bytes()
    baseline = tomllib.loads(lock_bytes.decode())
    allowed = {
        (p["name"], p["version"], p.get("source"), p.get("checksum"))
        for p in baseline["package"]
    }
    directory = Path(tempfile.mkdtemp(prefix="omnigraph-0048-arrow-pool-"))
    print(f"Probe directory: {directory}", flush=True)
    (directory / "Cargo.toml").write_text('''[package]
name = "omnigraph-0048-arrow-pool-probe"
version = "0.0.0"
edition = "2024"

[workspace]

[dependencies]
arrow-array = { version = "=58.3.0", features = ["pool"] }
arrow-buffer = { version = "=58.3.0", features = ["pool"] }
datafusion-execution = { version = "=54.0.0", features = ["arrow_buffer_pool"] }

[profile.dev]
debug = 0
''')
    (directory / "Cargo.lock").write_bytes(lock_bytes)
    (directory / "src").mkdir()
    source = assets / "0048-arrow-pool-probe.rs"
    shutil.copyfile(source, directory / "src/main.rs")
    subprocess.run(
        ["cargo", "metadata", "--offline", "--format-version", "1"],
        cwd=directory, check=True, stdout=subprocess.DEVNULL,
    )
    resolved = tomllib.loads((directory / "Cargo.lock").read_text())
    for package in resolved["package"]:
        if package["name"] == "omnigraph-0048-arrow-pool-probe":
            continue
        identity = (package["name"], package["version"], package.get("source"), package.get("checksum"))
        if identity not in allowed:
            raise RuntimeError(f"Dependency differs from workspace lock: {identity}")
    output = subprocess.check_output(
        ["cargo", "run", "--offline", "--locked", "--target-dir", str(directory / "target")],
        cwd=directory, text=True,
    )
    result = {
        "workspace_lock_sha256": hashlib.sha256(lock_bytes).hexdigest(),
        "probe_source_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
        "resolved_lock_sha256": hashlib.sha256((directory / "Cargo.lock").read_bytes()).hexdigest(),
        "registry_dependencies_match_workspace": True,
        "observations": json.loads(output),
    }
    (directory / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
