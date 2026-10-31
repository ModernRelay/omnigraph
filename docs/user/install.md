# Install

## macOS and Linux

```bash
curl -fsSL https://raw.githubusercontent.com/ModernRelay/omnigraph/main/scripts/install.sh | bash
```

The installer downloads a release archive, verifies its SHA-256 checksum, and
installs the binaries in `~/.local/bin`.

Prebuilt archives support Linux x86_64 and arm64, macOS arm64, and Windows
x86_64. The Linux archives need glibc 2.39 or newer, as on Ubuntu 24.04 and
Debian 13. Build from source on other platforms, including Intel macOS.

Homebrew is also supported:

```bash
brew tap ModernRelay/tap
brew install ModernRelay/tap/omnigraph
```

## Nix

The repository's flake builds the three release programs from source with the
pinned Rust toolchain, the same packages and features as the release archives,
on Linux x86_64 and arm64 and macOS arm64:

```bash
nix build github:ModernRelay/omnigraph
nix run github:ModernRelay/omnigraph -- version
```

Pin a commit or a release tag that carries the flake with
`github:ModernRelay/omnigraph/<ref>`. A build of a clean
checkout reports its commit; it reports no release channel.

## Windows

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -Command "iwr -UseBasicParsing https://raw.githubusercontent.com/ModernRelay/omnigraph/main/scripts/install.ps1 | iex"
```

Binaries are installed in `%USERPROFILE%\.local\bin`.

## Release channels

The default is the latest stable release; if none exists, the installer falls
back to `edge`.

Edge builds are prereleases built once a day from the tip of `main`, after its
whole CI run passes, when `main` has moved since the last one. Each is
published as its own release, `edge-<commit date>-<commit>` (for example
`edge-20261016-9c2b367`), whose files never change; the rolling `edge` release
always holds the newest one. The 30 newest edge builds are kept, and so is the
newest build of every storage format they no longer cover, since that build can
still export a graph written at that format. To install the newest:

```bash
curl -fsSL https://raw.githubusercontent.com/ModernRelay/omnigraph/main/scripts/install.sh \
  | RELEASE_CHANNEL=edge bash
```

```powershell
iwr -UseBasicParsing https://raw.githubusercontent.com/ModernRelay/omnigraph/main/scripts/install.ps1 -OutFile install.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File .\install.ps1 -ReleaseChannel edge
```

To deploy an edge build, pin it by its tag, so every machine installs the same
binaries and you can return to them: `VERSION=edge-20261016-9c2b367` on
macOS/Linux or `-Version edge-20261016-9c2b367` on Windows.

Each edge build is also a [container image](deployment.md#container) with the
same tag, `modernrelay/omnigraph-server:edge-20261016-9c2b367` on Docker Hub
and `ghcr.io/modernrelay/omnigraph-server:edge-20261016-9c2b367`, holding that
build's Linux x86_64 binaries on Debian trixie. The `edge` image tag follows the
rolling `edge` release; `latest` stays on the newest stable release. Pin a
deployment to the image's digest, which the run's summary lists. Provenance is
attached to the GHCR digest; do not use a Docker Hub manifest digest to verify
that attestation. GHCR access may require registry authentication; Docker Hub
is the public pull channel. See [container verification](#verify).

Read the build's release description before pointing it at a graph that a
stable release must still read. It lists the storage format the build writes
and the HTTP contract it speaks next to the latest stable release's. A branch
the build writes to is left at the build's storage format, and a stable binary
refuses a format newer than its own, so test on a copy of the graph or back it
up first ([upgrades](operations/upgrade.md)). A client and a server must speak
the same HTTP contract.

Install a specific stable tag with `VERSION=v0.14.0` on macOS/Linux or
`-Version v0.14.0` on Windows. Set `INSTALL_DIR` or `-InstallDir` to choose a
different destination.

Documentation on `main` may describe behavior newer than the latest stable
binary. Check `omnigraph version` and the [release notes](../releases/).

## Client compatibility

Upgrade the CLI, server, and HTTP integrations together. OmniGraph 0.14 requires
HTTP contract `0.14`; the npm `0.13.0` clients are not compatible with it.
Both `@modernrelay/omnigraph@0.14.0` and
`@modernrelay/omnigraph-mcp@0.14.0` are published on npm. Install the 0.14.0
clients alongside the 0.14 server; see the
[SDK release](https://github.com/ModernRelay/omnigraph-ts/releases/tag/v0.14.0).
For an existing deployment, follow the
[0.13 → 0.14 checklist](operations/upgrade-0.14.md), including configuration
and command replacements.

You can use the released CLI, the [HTTP API](operations/server.md), or the
server's built-in [MCP endpoint and CLI stdio bridge](operations/mcp.md)
without either npm package. The MCP protocol has its own negotiation; refresh
cached tool schemas when upgrading a client.

## Build from source

Install the Rust stable toolchain and the Protocol Buffers compiler (`protoc`),
then:

```bash
RUSTFLAGS= cargo build --release --locked \
  -p omnigraph-cli \
  -p omnigraph-server \
  -p omnigraph-azure-admission
```

The empty `RUSTFLAGS` replaces the workspace `.cargo/config.toml` build flags,
which exist for the development test suites; release binaries build without
them.

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

`omnigraph version` reports what the binary itself speaks, without opening a
graph or contacting a server. For example, building the `v0.14.0` source tag
without release metadata prints:

```text
omnigraph 0.14.0
internal-schema 16 (serves v14 to v16)
http-contract 0.14
```

`omnigraph version --json` also reports the machine-readable CLI protocols:

```json
{
  "version": "0.14.0",
  "channel": null,
  "commit": null,
  "http_contract": "0.14",
  "protocols": { "studio": 1, "target": 2, "credential": 2, "access": 2 },
  "internal_schema": {
    "writes": 16,
    "serves": [14, 16]
  }
}
```

An edge build reports a SemVer pre-release of the package version, such as
`0.14.1-edge+9c2b367`. Edge builds are published only while the package version
is above the latest stable release, so an edge version sorts after every
published stable release and before the release it precedes. `channel` is `stable`
or `edge` for a GitHub release archive and null for any other build. `commit`
is the commit a packaged build was compiled from; a plain source build reports
it as null and prints no `commit` line. The server reports the same commit as
`source_version` on `GET /healthz`.

`http_contract` is the `Omnigraph-Http-Api` value the CLI sends. A server
accepts only an exact match and returns its own value on every response,
including `HEAD /healthz` ([HTTP contract](operations/server.md#http-contract)).
When several CLIs are installed, for example one in `~/.local/bin` and an
older one from Homebrew, choose by this field, not by the package version.
`protocols.target` and `protocols.credential` identify compatible generations of
operator configuration/target administration and credential commands. Both are
2 for profiles, targets with one inline `auth` provider, and credential sources.
Match these numbers exactly when selecting a CLI for those contracts; they are
independent of the operator document's `version: 1`. Protocol 1 was published
with multiple incompatible 0.14 edge grammars, so that number alone cannot
select one of them. Use protocol 2 for the [current format](cli/targets.md).
An incompatible change to either contract requires its discovery number to change.
`protocols.access` identifies the `omnigraph access --json` shape: protocol 2
uses `actor_id` in place of protocol 1's `actor`. `protocols.studio` identifies
the [Studio link](cli/studio.md#the-link), `studio --json`, and the `home` and
`binding` digests `target inspect --json` reports for checking a link.
`internal_schema.writes` is the storage-format stamp this binary's commits
write, and `internal_schema.serves` is the inclusive range, lowest first, that
normal open accepts; `snapshot --json` reports a graph's own stamp as
`internal_schema_version`. `omnigraph --version` (`-v`) prints only the first
line.

Release archives from v0.14.0 on, and every `edge-<date>-<commit>` build, carry
a build provenance attestation. Check that an archive was built by this
repository's release workflow, and from which commit:

```bash
gh attestation verify omnigraph-linux-x86_64.tar.gz --repo ModernRelay/omnigraph
```

The [container image](deployment.md#container) carries the same stamp from
v0.14.0, and its GHCR digest carries the attestation:

```bash
docker run --rm --entrypoint /usr/local/bin/omnigraph \
  modernrelay/omnigraph-server:<tag> version
gh attestation verify oci://ghcr.io/modernrelay/omnigraph-server:<tag> \
  --repo ModernRelay/omnigraph
```

Continue with the [quickstart](quickstart.md).
