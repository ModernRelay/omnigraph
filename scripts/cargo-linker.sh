#!/bin/sh
# Linker driver for the Linux targets in .cargo/config.toml: links with mold
# when it is on PATH, with the default `cc` linker otherwise. `-fuse-ld=mold`
# needs GCC 12.1+ or clang. The flag goes last because rustc may already pass
# `-fuse-ld=lld` (its self-contained lld default on x86_64-unknown-linux-gnu),
# and the last `-fuse-ld` wins.
if command -v mold >/dev/null 2>&1; then
  exec cc "$@" -fuse-ld=mold
fi
exec cc "$@"
