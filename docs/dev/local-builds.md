# Local builds

**Audience:** anyone building or testing OmniGraph locally, often in many
worktrees at once
**Authority:** how the workspace's build configuration is meant to be used;
`.cargo/config.toml` and the scripts it and this page name are the source
of truth

The workspace links the Lance, DataFusion, Arrow and object-store stack into
every test binary, so most of a build is dependency compilation and linking.
Three things keep that cost paid once: one feature set for every loop, a warm
`target/` for every new worktree, and a fast linker on Linux.

## One feature set

Cargo builds a separate copy of every crate whose features differ, so
alternating `cargo test -p omnigraph-engine` with the failpoint graph rebuilds
the engine and everything above it each time. The aliases in
`.cargo/config.toml` all build the canonical CI graph from
[testing.md](testing.md), with `omnigraph-engine/failpoints` and
`omnigraph-cluster/failpoints`:

```bash
cargo dev-build-tests                      # compile every test binary, run nothing
cargo dev-test                             # the canonical workspace graph
cargo dev-nextest -E 'package(omnigraph-engine)'           # run a subset, no rebuild
cargo dev-nextest -E 'binary(=integration) & test(/^branching::/)'
cargo dev-clippy
```

Narrow a run with a nextest filterset or a libtest name filter, not with `-p`:
a package selection resolves its own features, which is the rebuild these
aliases exist to avoid. `cargo dev-test` keeps doctests, which nextest does not
run. A plain `cargo test -p <crate>` still works and is the cheaper choice when
only that crate is built in this `target/`.

## Warm worktrees

A cold `target/` costs a full dependency build and about 20 GB. sccache does
not remove that cost across worktrees: its Rust cache key includes the
compile's working directory (`src/compiler/rust.rs`, step 9, unchanged on
sccache's main branch as of 0.19), and Cargo runs every rustc, registry crates
included, from the workspace root, so a compile in one worktree never hits an
entry another worktree wrote. `SCCACHE_BASEDIRS` normalizes C and C++ only.

`scripts/seed-target.sh`, run once in a new worktree, instead seeds its
`target/` as an APFS copy-on-write clone (`cp -c -R -p`) of a warm checkout's
`target/`: by default the repository's main worktree (the first
`git worktree list` entry), or `$OMNIGRAPH_WARM_TARGET` when set. The clone
shares disk blocks with its source until either side rewrites a file. The
script does nothing when `target/` already exists, when no source exists, off
macOS, or when the source is on another or a non-APFS volume, where `cp -c`
would fall back to a full copy:

```bash
sh scripts/seed-target.sh
OMNIGRAPH_WARM_TARGET=/path/to/warm/target sh scripts/seed-target.sh
```

The clone keeps the source's mtimes. Cargo decides freshness by comparing an
artifact's recorded time with its sources' mtimes; the new worktree's sources
were just checked out, so every workspace crate rebuilds once and every
registry dependency stays fresh. Cargo rewrites the build-script output paths
recorded under the old `target/` itself. Keep the source warm by running
`cargo dev-build-tests` on `main` in the main worktree; a source built with
other features or another toolchain seeds less.

## Shared sccache backend (optional)

A remote sccache backend shares compilations between machines and CI runners
whose checkout path, toolchain and flags match, such as cloud dev boxes that all
check out to the same path. It does not help between local worktrees (see
above). No bucket or credential is part of this repository; to use one, create
an S3 or R2 bucket and credentials scoped to it, then configure sccache through
its environment:

```bash
export RUSTC_WRAPPER=sccache
export SCCACHE_BUCKET=<bucket>
export SCCACHE_REGION=auto                         # R2; an AWS region for S3
export SCCACHE_ENDPOINT=https://<account>.r2.cloudflarestorage.com   # R2 only
export SCCACHE_S3_KEY_PREFIX=omnigraph/
export AWS_ACCESS_KEY_ID=<key id>                   # from a secret store, never committed
export AWS_SECRET_ACCESS_KEY=<secret>
# Give read-only credentials to untrusted machines:
# export SCCACHE_S3_NO_CREDENTIALS=true             # anonymous reads of a public bucket
```

Restart the server after changing its configuration
(`sccache --stop-server`), and read hits with `sccache --show-stats`. sccache
caches compilations, not links: test executables and proc-macro crates always
link locally. CI uses the GitHub Actions backend instead
([ci.md](ci.md#full-correctness-graphs)).

## Linker

On Linux, `.cargo/config.toml` links through `scripts/cargo-linker.sh`, which
adds `-fuse-ld=mold` when `mold` is on `PATH` (GCC 12.1 or later, or clang)
and otherwise runs `cc` unchanged. Install it with the system package manager
(`apt-get install mold`). The linker is not part of a compile fingerprint, so
installing or removing mold relinks without recompiling. CI's test and lint jobs
install it; release builds do not, and link exactly as before. macOS uses the
system linker.
