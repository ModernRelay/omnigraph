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

A cold `target/` costs a full dependency build and about 10 GB for the engine's
test binaries alone. sccache removes only part of that cost across worktrees.
Its Rust cache key includes the compile's working directory
(`src/compiler/rust.rs`, step 9, unchanged on sccache's main branch as of
0.19). Cargo runs a registry crate's rustc from that crate's own directory, so
those compiles can hit, but runs every workspace crate from the workspace root,
so the workspace crates, and everything they feed, miss in every new worktree.
`SCCACHE_BASEDIRS` normalizes C and C++ only. sccache also never caches a
proc-macro, a link or a build-script run, and those repeat in full.

`scripts/seed-target.sh`, which Claude Code runs at session start through the
checked-in `.claude/settings.json` hook and any other agent runs once in a new
worktree before its first build, instead seeds its `target/` as an APFS
copy-on-write clone (`cp -c -R -p`) of a warm checkout's `target/`. The clone
shares disk blocks with its source until either side rewrites a file. The
script does nothing when `target/` already exists, when no source qualifies,
off macOS, or when the source is on another or a non-APFS volume, where `cp -c`
would fall back to a full copy:

```bash
sh scripts/seed-target.sh
OMNIGRAPH_WARM_TARGET=/path/to/warm/target sh scripts/seed-target.sh
```

Without `$OMNIGRAPH_WARM_TARGET` the source is the most recently built
`target/` among the repository's other worktrees whose ancestor Cargo configs
are identical. Cargo joins array values such as `build.rustflags` from every
`.cargo/config.toml` above a checkout, so a worktree nested inside another
checkout, as `.claude/worktrees/*` is inside the main checkout, builds with
`--cfg tokio_unstable` twice. Its artifact hashes then differ from the main
checkout's, and a clone of the main checkout's `target/` rebuilds everything;
a sibling nested worktree's `target/` matches. Spelling the flags as a string,
which Cargo does not join, would break every checkout still holding the array
form, because Cargo refuses to merge a string with an array.

The clone keeps the source's mtimes. Cargo decides freshness by comparing an
artifact's recorded time with its sources' mtimes; the new worktree's sources
were just checked out, so every workspace crate rebuilds once and every
registry dependency stays fresh. Cargo rewrites the build-script output paths
recorded under the old `target/` itself. A source built with other features or
another toolchain seeds less, so keep one worktree warm with
`cargo dev-build-tests`.

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
