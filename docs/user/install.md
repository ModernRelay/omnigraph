# Install

## macOS and Linux

```bash
curl -fsSL https://raw.githubusercontent.com/ModernRelay/omnigraph/main/scripts/install.sh | bash
```

The installer downloads a release archive, verifies its SHA-256 checksum, and
installs the binaries in `~/.local/bin`.

Prebuilt archives support Linux x86_64 and arm64, macOS arm64, and Windows
x86_64. Build from source on other platforms, including Intel macOS.

Homebrew is also supported:

```bash
brew tap ModernRelay/tap
brew install ModernRelay/tap/omnigraph
```

## Windows

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -Command "iwr -UseBasicParsing https://raw.githubusercontent.com/ModernRelay/omnigraph/main/scripts/install.ps1 | iex"
```

Binaries are installed in `%USERPROFILE%\.local\bin`.

## Release channels

The default is the latest stable release; if none exists, the installer falls
back to `edge`. To request the rolling build explicitly:

```bash
curl -fsSL https://raw.githubusercontent.com/ModernRelay/omnigraph/main/scripts/install.sh \
  | RELEASE_CHANNEL=edge bash
```

```powershell
iwr -UseBasicParsing https://raw.githubusercontent.com/ModernRelay/omnigraph/main/scripts/install.ps1 -OutFile install.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File .\install.ps1 -ReleaseChannel edge
```

Install a specific tag with `VERSION=v0.10.0` on macOS/Linux or
`-Version v0.10.0` on Windows. Set `INSTALL_DIR` or `-InstallDir` to choose a
different destination.

Documentation on `main` may describe behavior newer than the latest stable
binary. Check `omnigraph version` and the [release notes](../releases/).

## Build from source

Install the Rust stable toolchain and the Protocol Buffers compiler (`protoc`),
then:

```bash
cargo build --release --locked \
  -p omnigraph-cli \
  -p omnigraph-server \
  -p omnigraph-azure-admission
```

Or use the source installer:

```bash
curl -fsSL https://raw.githubusercontent.com/ModernRelay/omnigraph/main/scripts/install-source.sh | bash
```

## Installed programs

Release archives contain:

- `omnigraph` — CLI;
- `omnigraph-server` — HTTP server;
- `omnigraph-azure-admission` — Azure writer-admission utility.

The admission utility's `inspect`, `break`, and help commands are available on
Windows. Its child-supervision `run` mode is supported only by the Unix
deployment images used for the Azure reference topology.

## Verify

```bash
omnigraph version
omnigraph-server --help
omnigraph-azure-admission --help
```

Continue with the [quickstart](quickstart.md).
