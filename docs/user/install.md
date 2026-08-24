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
- `omnigraph-azure-admission`

in `~/.local/bin` on macOS / Linux, or:

- `omnigraph.exe`
- `omnigraph-server.exe`
- `omnigraph-azure-admission.exe`

in `%USERPROFILE%\.local\bin` on Windows.

Starting with v0.10.0, the admission binary is shipped beside the two core
binaries. Its `run` child-supervision mode is supported only on the Unix
deployment images used by the Azure reference topology. The Windows executable
can run `inspect`/`break` and display help; it is not a Windows process
supervisor.

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

Install from source (requires the Rust stable toolchain and `protoc` — install
the Protocol Buffers compiler first with `brew install protobuf` or
`apt-get install -y protobuf-compiler libprotobuf-dev`):

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
curl -fsSL https://raw.githubusercontent.com/ModernRelay/omnigraph/main/scripts/install.sh | VERSION=v0.9.0 bash
```

Windows:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\install.ps1 -Version v0.9.0
```

Build from a specific git ref:

```bash
curl -fsSL https://raw.githubusercontent.com/ModernRelay/omnigraph/main/scripts/install-source.sh | SOURCE_REF=main bash
```

## Manual Source Build

Requires the Rust stable toolchain and `protoc` (`brew install protobuf` /
`apt-get install -y protobuf-compiler libprotobuf-dev`).

macOS / Linux:

```bash
cargo build --release --locked -p omnigraph-cli -p omnigraph-server -p omnigraph-azure-admission
install -m 0755 target/release/omnigraph ~/.local/bin/omnigraph
install -m 0755 target/release/omnigraph-server ~/.local/bin/omnigraph-server
install -m 0755 target/release/omnigraph-azure-admission ~/.local/bin/omnigraph-azure-admission
```

Windows:

```powershell
cargo build --release --locked -p omnigraph-cli -p omnigraph-server -p omnigraph-azure-admission
New-Item -ItemType Directory -Force "$env:USERPROFILE\.local\bin" | Out-Null
Copy-Item target\release\omnigraph.exe "$env:USERPROFILE\.local\bin\omnigraph.exe"
Copy-Item target\release\omnigraph-server.exe "$env:USERPROFILE\.local\bin\omnigraph-server.exe"
Copy-Item target\release\omnigraph-azure-admission.exe "$env:USERPROFILE\.local\bin\omnigraph-azure-admission.exe"
```

## Release Assets

Tagged releases are expected to publish:

- `omnigraph-linux-x86_64.tar.gz`
- `omnigraph-linux-arm64.tar.gz`
- `omnigraph-macos-arm64.tar.gz`
- `omnigraph-windows-x86_64.zip`

Starting with v0.10.0, the macOS / Linux archives contain:

- `omnigraph`
- `omnigraph-server`
- `omnigraph-azure-admission`

Starting with v0.10.0, the Windows archive contains:

- `omnigraph.exe`
- `omnigraph-server.exe`
- `omnigraph-azure-admission.exe`

## Verify The Install

macOS / Linux:

```bash
omnigraph version
omnigraph-server --help
omnigraph-azure-admission --help
```

Windows:

```powershell
omnigraph.exe version
omnigraph-server.exe --help
omnigraph-azure-admission.exe --help
```
