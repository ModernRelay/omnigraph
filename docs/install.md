# Install

## Quick Install

```bash
curl -fsSL https://raw.githubusercontent.com/ModernRelay/omnigraph-public/main/scripts/install.sh | bash
```

By default the installer places:

- `omnigraph`
- `omnigraph-server`

in `~/.local/bin`.

If a matching release asset exists for your platform, the installer downloads
and unpacks it. Otherwise it falls back to cloning `ModernRelay/omnigraph-public`
and building from source.

## Useful Overrides

Install to a different directory:

```bash
curl -fsSL https://raw.githubusercontent.com/ModernRelay/omnigraph-public/main/scripts/install.sh | INSTALL_DIR="$HOME/bin" bash
```

Force a source build even if a release asset exists:

```bash
curl -fsSL https://raw.githubusercontent.com/ModernRelay/omnigraph-public/main/scripts/install.sh | FORCE_BUILD=1 bash
```

Build from a specific git ref:

```bash
curl -fsSL https://raw.githubusercontent.com/ModernRelay/omnigraph-public/main/scripts/install.sh | SOURCE_REF=main bash
```

## Manual Source Build

```bash
cargo build --release --locked -p omnigraph-cli -p omnigraph-server
install -m 0755 target/release/omnigraph ~/.local/bin/omnigraph
install -m 0755 target/release/omnigraph-server ~/.local/bin/omnigraph-server
```

## Release Assets

Tagged releases are expected to publish:

- `omnigraph-linux-x86_64.tar.gz`
- `omnigraph-macos-x86_64.tar.gz`
- `omnigraph-macos-arm64.tar.gz`

Each archive contains both binaries:

- `omnigraph`
- `omnigraph-server`

## Verify The Install

```bash
omnigraph version
omnigraph-server --help
```
