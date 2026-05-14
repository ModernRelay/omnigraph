# Install

## Quick Install

```bash
curl -fsSL https://raw.githubusercontent.com/ModernRelay/omnigraph/main/scripts/install.sh | bash
```

By default the installer places:

- `omnigraph`
- `omnigraph-server`

in `~/.local/bin`.

The default installer is binary-only. It downloads a published release asset,
verifies the SHA256 checksum, and unpacks it. It does not build from source.
If no stable tag is published yet, the installer automatically falls back to
the rolling `edge` release.

## Homebrew

```bash
brew tap ModernRelay/tap
brew install ModernRelay/tap/omnigraph
```

## Channels

Stable binaries:

```bash
curl -fsSL https://raw.githubusercontent.com/ModernRelay/omnigraph/main/scripts/install.sh | bash
```

Rolling edge binaries from `main`:

```bash
curl -fsSL https://raw.githubusercontent.com/ModernRelay/omnigraph/main/scripts/install.sh | RELEASE_CHANNEL=edge bash
```

Install from source:

```bash
curl -fsSL https://raw.githubusercontent.com/ModernRelay/omnigraph/main/scripts/install-source.sh | bash
```

## Useful Overrides

Install to a different directory:

```bash
curl -fsSL https://raw.githubusercontent.com/ModernRelay/omnigraph/main/scripts/install.sh | INSTALL_DIR="$HOME/bin" bash
```

Install a specific tag:

```bash
curl -fsSL https://raw.githubusercontent.com/ModernRelay/omnigraph/main/scripts/install.sh | VERSION=v0.1.0 bash
```

Build from a specific git ref:

```bash
curl -fsSL https://raw.githubusercontent.com/ModernRelay/omnigraph/main/scripts/install-source.sh | SOURCE_REF=main bash
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
