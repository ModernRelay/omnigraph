# Install

## Quick Install

macOS / Linux:

```bash
curl -fsSL https://raw.githubusercontent.com/ModernRelay/omnigraph/main/scripts/install.sh | bash
```

Windows PowerShell:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -Command "iwr -UseBasicParsing https://raw.githubusercontent.com/ModernRelay/omnigraph/main/scripts/install.ps1 | iex"
```

By default the installer places:

- `omnigraph`
- `omnigraph-server`

in `~/.local/bin` on macOS / Linux, or:

- `omnigraph.exe`
- `omnigraph-server.exe`

in `%USERPROFILE%\.local\bin` on Windows.

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

Windows rolling edge binaries:

```powershell
iwr -UseBasicParsing https://raw.githubusercontent.com/ModernRelay/omnigraph/main/scripts/install.ps1 -OutFile install.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File .\install.ps1 -ReleaseChannel edge
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

Windows:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\install.ps1 -InstallDir "$env:USERPROFILE\bin"
```

Install a specific tag:

```bash
curl -fsSL https://raw.githubusercontent.com/ModernRelay/omnigraph/main/scripts/install.sh | VERSION=v0.1.0 bash
```

Windows:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\install.ps1 -Version v0.1.0
```

Build from a specific git ref:

```bash
curl -fsSL https://raw.githubusercontent.com/ModernRelay/omnigraph/main/scripts/install-source.sh | SOURCE_REF=main bash
```

## Manual Source Build

macOS / Linux:

```bash
cargo build --release --locked -p omnigraph-cli -p omnigraph-server
install -m 0755 target/release/omnigraph ~/.local/bin/omnigraph
install -m 0755 target/release/omnigraph-server ~/.local/bin/omnigraph-server
```

Windows:

```powershell
cargo build --release --locked -p omnigraph-cli -p omnigraph-server
New-Item -ItemType Directory -Force "$env:USERPROFILE\.local\bin" | Out-Null
Copy-Item target\release\omnigraph.exe "$env:USERPROFILE\.local\bin\omnigraph.exe"
Copy-Item target\release\omnigraph-server.exe "$env:USERPROFILE\.local\bin\omnigraph-server.exe"
```

## Release Assets

Tagged releases are expected to publish:

- `omnigraph-linux-x86_64.tar.gz`
- `omnigraph-macos-arm64.tar.gz`
- `omnigraph-windows-x86_64.zip`

The macOS / Linux archives contain both binaries:

- `omnigraph`
- `omnigraph-server`

The Windows archive contains:

- `omnigraph.exe`
- `omnigraph-server.exe`

## Verify The Install

macOS / Linux:

```bash
omnigraph version
omnigraph-server --help
```

Windows:

```powershell
omnigraph.exe version
omnigraph-server.exe --help
```
