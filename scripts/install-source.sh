#!/usr/bin/env bash
set -euo pipefail

REPO_SLUG="${REPO_SLUG:-ModernRelay/omnigraph-public}"
SOURCE_REF="${SOURCE_REF:-main}"
INSTALL_DIR="${INSTALL_DIR:-$HOME/.local/bin}"
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

install_from_dir() {
  mkdir -p "$INSTALL_DIR"
  install -m 0755 "$1/omnigraph" "$INSTALL_DIR/omnigraph"
  install -m 0755 "$1/omnigraph-server" "$INSTALL_DIR/omnigraph-server"
}

build_from_source() {
  local repo_root
  repo_root="${1:-}"

  if [ -z "$repo_root" ]; then
    need_cmd git
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

  repo_root="$(repo_root_from_shell || true)"
  build_from_source "$repo_root"
  print_summary
}

main "$@"
