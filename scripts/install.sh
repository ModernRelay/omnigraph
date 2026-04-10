#!/usr/bin/env bash
set -euo pipefail

REPO_SLUG="${REPO_SLUG:-ModernRelay/omnigraph-public}"
SOURCE_REF="${SOURCE_REF:-main}"
INSTALL_DIR="${INSTALL_DIR:-$HOME/.local/bin}"
FORCE_BUILD="${FORCE_BUILD:-0}"
TMP_ROOT="${TMPDIR:-/tmp}"
WORKDIR=""

log() {
  printf '==> %s\n' "$*"
}

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

cleanup() {
  if [ -n "${WORKDIR:-}" ] && [ -d "$WORKDIR" ]; then
    rm -rf "$WORKDIR"
  fi
}

trap cleanup EXIT

repo_root_from_shell() {
  if [ -f "$PWD/Cargo.toml" ] && [ -d "$PWD/crates" ]; then
    printf '%s\n' "$PWD"
    return 0
  fi

  if [ -n "${BASH_SOURCE[0]:-}" ] && [ -f "${BASH_SOURCE[0]}" ]; then
    local candidate
    candidate="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
    if [ -f "$candidate/Cargo.toml" ] && [ -d "$candidate/crates" ]; then
      printf '%s\n' "$candidate"
      return 0
    fi
  fi

  return 1
}

latest_release_tag() {
  local json
  json="$(curl -fsSL "https://api.github.com/repos/$REPO_SLUG/releases/latest" 2>/dev/null || true)"
  printf '%s' "$json" | sed -n 's/.*"tag_name":[[:space:]]*"\([^"]*\)".*/\1/p' | head -n 1
}

platform_asset_name() {
  local os arch
  os="$(uname -s)"
  arch="$(uname -m)"

  case "$os/$arch" in
    Linux/x86_64)
      printf 'omnigraph-linux-x86_64.tar.gz\n'
      ;;
    Darwin/x86_64)
      printf 'omnigraph-macos-x86_64.tar.gz\n'
      ;;
    Darwin/arm64)
      printf 'omnigraph-macos-arm64.tar.gz\n'
      ;;
    *)
      return 1
      ;;
  esac
}

install_from_dir() {
  mkdir -p "$INSTALL_DIR"
  install -m 0755 "$1/omnigraph" "$INSTALL_DIR/omnigraph"
  install -m 0755 "$1/omnigraph-server" "$INSTALL_DIR/omnigraph-server"
}

install_from_release() {
  local tag asset archive

  [ "$FORCE_BUILD" = "1" ] && return 1

  tag="$(latest_release_tag)"
  [ -n "$tag" ] || return 1

  asset="$(platform_asset_name)" || return 1
  WORKDIR="$(mktemp -d "$TMP_ROOT/omnigraph-install.XXXXXX")"
  archive="$WORKDIR/$asset"

  log "Downloading $asset from $tag"
  curl -fsSL \
    "https://github.com/$REPO_SLUG/releases/download/$tag/$asset" \
    -o "$archive" || return 1

  tar -C "$WORKDIR" -xzf "$archive" || return 1
  install_from_dir "$WORKDIR"
  return 0
}

build_from_source() {
  local repo_root
  repo_root="${1:-}"

  if [ -z "$repo_root" ]; then
    need_cmd git
    need_cmd cargo

    WORKDIR="$(mktemp -d "$TMP_ROOT/omnigraph-install.XXXXXX")"
    repo_root="$WORKDIR/source"
    log "Cloning $REPO_SLUG at $SOURCE_REF"
    git clone --depth 1 --branch "$SOURCE_REF" "https://github.com/$REPO_SLUG.git" "$repo_root"
  fi

  need_cmd cargo
  log "Building omnigraph binaries from source"
  (
    cd "$repo_root"
    cargo build --release --locked -p omnigraph-cli -p omnigraph-server
  )

  install_from_dir "$repo_root/target/release"
}

print_summary() {
  cat <<EOF

Installed:
  $INSTALL_DIR/omnigraph
  $INSTALL_DIR/omnigraph-server

Verify:
  $INSTALL_DIR/omnigraph version
  $INSTALL_DIR/omnigraph-server --help

EOF

  case ":$PATH:" in
    *":$INSTALL_DIR:"*)
      ;;
    *)
      printf 'Add %s to PATH if needed.\n' "$INSTALL_DIR"
      ;;
  esac
}

main() {
  local repo_root

  need_cmd curl
  repo_root="$(repo_root_from_shell || true)"

  if ! install_from_release; then
    build_from_source "$repo_root"
  fi

  print_summary
}

main "$@"
