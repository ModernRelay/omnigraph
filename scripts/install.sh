#!/usr/bin/env bash
set -euo pipefail

REPO_SLUG="${REPO_SLUG:-ModernRelay/omnigraph}"
INSTALL_DIR="${INSTALL_DIR:-$HOME/.local/bin}"
RELEASE_CHANNEL="${RELEASE_CHANNEL:-stable}"
VERSION="${VERSION:-}"
TMP_ROOT="${TMPDIR:-/tmp}"
WORKDIR=""
SELECTED_CHANNEL=""

log() {
  printf '==> %s\n' "$*"
}

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

cleanup() {
  if [ -n "${WORKDIR:-}" ] && [ -d "$WORKDIR" ]; then
    rm -rf "$WORKDIR"
  fi
}

trap cleanup EXIT

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

checksum_command() {
  if command -v shasum >/dev/null 2>&1; then
    printf 'shasum -a 256'
    return
  fi

  if command -v sha256sum >/dev/null 2>&1; then
    printf 'sha256sum'
    return
  fi

  die "missing checksum tool: expected shasum or sha256sum"
}

release_base_url() {
  local channel="${1:-$RELEASE_CHANNEL}"

  if [ -n "$VERSION" ]; then
    printf 'https://github.com/%s/releases/download/%s\n' "$REPO_SLUG" "$VERSION"
    return
  fi

  case "$channel" in
    stable)
      printf 'https://github.com/%s/releases/latest/download\n' "$REPO_SLUG"
      ;;
    edge)
      printf 'https://github.com/%s/releases/download/edge\n' "$REPO_SLUG"
      ;;
    *)
      die "unsupported RELEASE_CHANNEL '$channel' (expected stable or edge)"
      ;;
  esac
}

install_from_dir() {
  mkdir -p "$INSTALL_DIR"
  install -m 0755 "$1/omnigraph" "$INSTALL_DIR/omnigraph"
  install -m 0755 "$1/omnigraph-server" "$INSTALL_DIR/omnigraph-server"
}

verify_checksum() {
  local archive="$1"
  local checksum_file="$2"
  local expected actual tool

  expected="$(awk '{print $1}' "$checksum_file")"
  [ -n "$expected" ] || die "checksum file did not contain a SHA256 digest"

  tool="$(checksum_command)"
  actual="$($tool "$archive" | awk '{print $1}')"

  [ "$actual" = "$expected" ] || die "checksum verification failed for $(basename "$archive")"
}

download_release_files() {
  local base_url="$1"
  local asset_name="$2"
  local checksum_name="$3"
  local archive="$4"
  local checksum="$5"

  curl -fsSL "$base_url/$asset_name" -o "$archive" || return 1
  curl -fsSL "$base_url/$checksum_name" -o "$checksum" || return 1
}

install_from_release() {
  local asset asset_stem archive checksum base_url

  asset="$(platform_asset_name)" || die "no prebuilt binary is available for $(uname -s)/$(uname -m)"
  asset_stem="${asset%.tar.gz}"
  WORKDIR="$(mktemp -d "$TMP_ROOT/omnigraph-install.XXXXXX")"
  archive="$WORKDIR/$asset"
  checksum="$WORKDIR/$asset_stem.sha256"

  if [ -n "$VERSION" ]; then
    SELECTED_CHANNEL="$VERSION"
    base_url="$(release_base_url)"
    log "Downloading $asset from $VERSION"
    download_release_files "$base_url" "$asset" "$asset_stem.sha256" "$archive" "$checksum" || die "no published binary found for $asset at release $VERSION"
  else
    SELECTED_CHANNEL="$RELEASE_CHANNEL"
    base_url="$(release_base_url "$SELECTED_CHANNEL")"
    log "Downloading $asset from $SELECTED_CHANNEL"
    if ! download_release_files "$base_url" "$asset" "$asset_stem.sha256" "$archive" "$checksum"; then
      if [ "$RELEASE_CHANNEL" != "stable" ]; then
        die "no published binary found for $asset on channel $RELEASE_CHANNEL"
      fi

      log "Stable release binaries are not published yet; falling back to edge"
      SELECTED_CHANNEL="edge"
      base_url="$(release_base_url "$SELECTED_CHANNEL")"
      download_release_files "$base_url" "$asset" "$asset_stem.sha256" "$archive" "$checksum" || die "no published binary found for $asset on stable or edge; use scripts/install-source.sh or build from source"
    fi
  fi

  verify_checksum "$archive" "$checksum"
  tar -C "$WORKDIR" -xzf "$archive" || die "failed to unpack $asset"
  install_from_dir "$WORKDIR"
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

  if [ -n "$SELECTED_CHANNEL" ]; then
    printf 'Installed from release channel: %s\n' "$SELECTED_CHANNEL"
  fi

  case ":$PATH:" in
    *":$INSTALL_DIR:"*)
      ;;
    *)
      printf 'Add %s to PATH if needed.\n' "$INSTALL_DIR"
      ;;
  esac
}

main() {
  command -v curl >/dev/null 2>&1 || die "missing required command: curl"
  install_from_release
  print_summary
}

main "$@"
