#!/usr/bin/env bash
set -euo pipefail

REPO_SLUG="${REPO_SLUG:-ModernRelay/omnigraph}"
INSTALL_DIR="${INSTALL_DIR:-$HOME/.local/bin}"
RELEASE_CHANNEL="${RELEASE_CHANNEL:-stable}"
VERSION="${VERSION:-}"
# auto: verify the archive's build provenance with `gh attestation verify`
# when the GitHub CLI is installed and signed in; require: refuse without it;
# off: skip. Releases before v0.14 carry no attestation.
VERIFY_ATTESTATION="${VERIFY_ATTESTATION:-auto}"
TMP_ROOT="${TMPDIR:-/tmp}"
WORKDIR=""
SELECTED_CHANNEL=""
ADMISSION_INSTALLED=0

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
    Linux/aarch64)
      printf 'omnigraph-linux-arm64.tar.gz\n'
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
  if [ -f "$1/omnigraph-azure-admission" ]; then
    install -m 0755 "$1/omnigraph-azure-admission" "$INSTALL_DIR/omnigraph-azure-admission"
    ADMISSION_INSTALLED=1
  else
    # Releases before native Azure support contain only the CLI and server.
    rm -f "$INSTALL_DIR/omnigraph-azure-admission"
    ADMISSION_INSTALLED=0
  fi
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

# Download one release file. Returns 2 when the release or the file is not
# published (HTTP 404), 1 on any other failure.
download_file() {
  local url="$1"
  local output="$2"
  local status

  status="$(curl -sSL -w '%{http_code}' -o "$output" "$url")" || return 1
  case "$status" in
    200) return 0 ;;
    404) return 2 ;;
    *) return 1 ;;
  esac
}

download_release_files() {
  local base_url="$1"
  local asset_name="$2"
  local checksum_name="$3"
  local archive="$4"
  local checksum="$5"

  download_file "$base_url/$asset_name" "$archive" || return $?
  download_file "$base_url/$checksum_name" "$checksum" || return $?
}

# The tag the stable channel serves, from the redirect of releases/latest.
stable_release_tag() {
  local url
  url="$(curl -sSLI -o /dev/null -w '%{url_effective}' "https://github.com/$REPO_SLUG/releases/latest" 2>/dev/null)" || return 1
  case "$url" in
    */releases/tag/*) printf '%s\n' "${url##*/}" ;;
    *) return 1 ;;
  esac
}

# Whether `$1` names a stable release before v0.14.0, the first one published
# with a build attestation. Edge tags and later releases do not.
version_predates_attestations() {
  local version="${1#v}" major minor
  case "$version" in
    [0-9]*.[0-9]*.[0-9]*) ;;
    *) return 1 ;;
  esac
  major="${version%%.*}"
  minor="${version#*.}"
  minor="${minor%%.*}"
  case "$major$minor" in
    *[!0-9]*) return 1 ;;
  esac
  [ "$major" -eq 0 ] && [ "$minor" -lt 14 ]
}

verify_attestation() {
  local archive="$1"

  case "$VERIFY_ATTESTATION" in
    off)
      log "Skipping build provenance verification (VERIFY_ATTESTATION=off)"
      return
      ;;
    auto|require) ;;
    *) die "unsupported VERIFY_ATTESTATION '$VERIFY_ATTESTATION' (expected auto, require or off)" ;;
  esac

  if ! command -v gh >/dev/null 2>&1 \
    || ! gh attestation verify --help >/dev/null 2>&1 \
    || ! gh auth status >/dev/null 2>&1; then
    if [ "$VERIFY_ATTESTATION" = "require" ]; then
      die "VERIFY_ATTESTATION=require needs the GitHub CLI (gh) with gh attestation, signed in"
    fi
    log "Build provenance not verified: install and sign in to a GitHub CLI (gh) with gh attestation to check it"
    return
  fi

  if gh attestation verify "$archive" --repo "$REPO_SLUG" >/dev/null; then
    log "Verified build provenance of $(basename "$archive") with gh attestation verify"
    return
  fi
  if [ "$VERIFY_ATTESTATION" = "auto" ] && version_predates_attestations "$VERSION"; then
    # Releases before v0.14.0 were published without attestations. Any
    # other release, a pinned edge build included, carries one, so a failed
    # check there refuses the archive.
    log "warning: $VERSION predates build attestations; its provenance was not verified. Set VERIFY_ATTESTATION=off to silence this, or require to refuse it"
    return
  fi
  die "build provenance verification failed for $(basename "$archive"); refusing to install. Set VERIFY_ATTESTATION=off only if you trust this archive"
}

install_from_release() {
  local asset asset_stem archive checksum base_url

  asset="$(platform_asset_name)" || die "no prebuilt binary is available for $(uname -s)/$(uname -m)"
  asset_stem="${asset%.tar.gz}"
  WORKDIR="$(mktemp -d "$TMP_ROOT/omnigraph-install.XXXXXX")"
  archive="$WORKDIR/$asset"
  checksum="$WORKDIR/$asset_stem.sha256"

  local status=0 tag
  if [ -n "$VERSION" ]; then
    SELECTED_CHANNEL="$VERSION"
    base_url="$(release_base_url)"
    log "Downloading $asset from release $VERSION"
    download_release_files "$base_url" "$asset" "$asset_stem.sha256" "$archive" "$checksum" || die "no published binary found for $asset at release $VERSION"
  else
    SELECTED_CHANNEL="$RELEASE_CHANNEL"
    base_url="$(release_base_url "$SELECTED_CHANNEL")"
    if [ "$SELECTED_CHANNEL" = "stable" ] && tag="$(stable_release_tag)"; then
      SELECTED_CHANNEL="stable ($tag)"
    fi
    log "Downloading $asset from $SELECTED_CHANNEL"
    download_release_files "$base_url" "$asset" "$asset_stem.sha256" "$archive" "$checksum" || status=$?
    if [ "$status" -ne 0 ]; then
      if [ "$RELEASE_CHANNEL" != "stable" ]; then
        die "no published binary found for $asset on channel $RELEASE_CHANNEL"
      fi
      if [ "$status" -ne 2 ]; then
        # Only a stable release that is not published falls back; a network
        # or server failure must not switch the channel.
        die "download of $asset from the stable channel failed; retry, or set RELEASE_CHANNEL=edge to install the edge build"
      fi

      log "No stable release publishes $asset (HTTP 404); installing the edge build instead"
      SELECTED_CHANNEL="edge"
      base_url="$(release_base_url "$SELECTED_CHANNEL")"
      download_release_files "$base_url" "$asset" "$asset_stem.sha256" "$archive" "$checksum" || die "no published binary found for $asset on stable or edge; use scripts/install-source.sh or build from source"
    fi
  fi

  verify_checksum "$archive" "$checksum"
  verify_attestation "$archive"
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

  if [ "$ADMISSION_INSTALLED" -eq 1 ]; then
    cat <<EOF
  $INSTALL_DIR/omnigraph-azure-admission

Verify Azure admission:
  $INSTALL_DIR/omnigraph-azure-admission --help

EOF
  fi

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
