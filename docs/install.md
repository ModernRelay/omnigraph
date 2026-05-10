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

## Updating

After installing via the script (or a manual binary install) you can update both
`omnigraph` and `omnigraph-server` in place:

```bash
omnigraph update            # update from the latest stable release
omnigraph update --check    # only check for a newer version
omnigraph update --yes      # skip the confirmation prompt
omnigraph update --channel edge  # follow the rolling `edge` channel
```

`omnigraph update`:

- detects the platform automatically (Linux x86_64 / macOS arm64),
- downloads the matching `omnigraph-<platform>.tar.gz` and `.sha256`,
- verifies the SHA256 digest before touching anything,
- replaces both binaries in the install directory atomically (POSIX rename), and
- detects Homebrew installs and asks you to run `brew upgrade ModernRelay/tap/omnigraph` instead.

Each invocation of `omnigraph` also performs a best-effort, cached check
(once every 24 hours) for newer stable releases and prints a one-line stderr
notice if one is available. The notice is suppressed when:

- `OMNIGRAPH_NO_UPDATE_CHECK=1` is set,
- `CI` is set,
- stdout is not a TTY (pipes, scripts), or
- the running subcommand is `version` or `update`.

The cache lives at `$XDG_CACHE_HOME/omnigraph/update-check.json` (default
`~/.cache/omnigraph/update-check.json`).
