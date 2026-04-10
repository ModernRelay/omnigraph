#!/usr/bin/env bash
set -euo pipefail

REPO_SLUG="${REPO_SLUG:-ModernRelay/omnigraph-public}"
SOURCE_REF="${SOURCE_REF:-main}"
WORKDIR="${WORKDIR:-$PWD/.omnigraph-rustfs-demo}"
RUSTFS_CONTAINER_NAME="${RUSTFS_CONTAINER_NAME:-omnigraph-rustfs-demo}"
RUSTFS_IMAGE="${RUSTFS_IMAGE:-rustfs/rustfs:latest}"
RUSTFS_DATA_DIR="${RUSTFS_DATA_DIR:-$WORKDIR/rustfs-data}"
BUCKET="${BUCKET:-omnigraph-local}"
PREFIX="${PREFIX:-repos/context}"
BIND="${BIND:-127.0.0.1:8080}"
AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-rustfsadmin}"
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-rustfsadmin}"
AWS_REGION="${AWS_REGION:-us-east-1}"
AWS_ENDPOINT_URL="${AWS_ENDPOINT_URL:-http://127.0.0.1:9000}"
AWS_ENDPOINT_URL_S3="${AWS_ENDPOINT_URL_S3:-$AWS_ENDPOINT_URL}"
AWS_ALLOW_HTTP="${AWS_ALLOW_HTTP:-true}"
AWS_S3_FORCE_PATH_STYLE="${AWS_S3_FORCE_PATH_STYLE:-true}"
FORCE_BUILD="${FORCE_BUILD:-0}"

REPO_URI="s3://$BUCKET/$PREFIX"
SERVER_LOG="$WORKDIR/omnigraph-server.log"
SERVER_PID_FILE="$WORKDIR/omnigraph-server.pid"
BIN_DIR=""
FIXTURE_DIR=""
AWS_BIN=""

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

repo_root_from_shell() {
  if [ -f "$PWD/Cargo.toml" ] && [ -f "$PWD/crates/omnigraph/tests/fixtures/context.pg" ]; then
    printf '%s\n' "$PWD"
    return 0
  fi

  if [ -n "${BASH_SOURCE[0]:-}" ] && [ -f "${BASH_SOURCE[0]}" ]; then
    local candidate
    candidate="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
    if [ -f "$candidate/Cargo.toml" ] && [ -f "$candidate/crates/omnigraph/tests/fixtures/context.pg" ]; then
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

ensure_aws_cli() {
  if command -v aws >/dev/null 2>&1; then
    AWS_BIN="$(command -v aws)"
    return
  fi

  need_cmd python3

  if ! python3 -m pip --version >/dev/null 2>&1; then
    python3 -m ensurepip --upgrade --user >/dev/null 2>&1 || die "aws cli not found and python3 pip bootstrap failed"
  fi

  log "Installing a user-local AWS CLI"
  python3 -m pip install --user awscli >/dev/null
  export PATH="$HOME/.local/bin:$PATH"

  command -v aws >/dev/null 2>&1 || die "aws cli installation succeeded but aws was not found on PATH"
  AWS_BIN="$(command -v aws)"
}

download_fixture_files() {
  local ref="$1"
  local fixture_target="$WORKDIR/fixtures"
  mkdir -p "$fixture_target"

  for file in context.pg context.jsonl; do
    curl -fsSL \
      "https://raw.githubusercontent.com/$REPO_SLUG/$ref/crates/omnigraph/tests/fixtures/$file" \
      -o "$fixture_target/$file" || return 1
  done

  FIXTURE_DIR="$fixture_target"
}

download_release_binaries() {
  local tag asset archive_dir archive_path

  [ "$FORCE_BUILD" = "1" ] && return 1

  tag="$(latest_release_tag)"
  [ -n "$tag" ] || return 1

  asset="$(platform_asset_name)" || return 1
  archive_dir="$WORKDIR/release"
  archive_path="$archive_dir/$asset"
  mkdir -p "$archive_dir" "$WORKDIR/bin"

  log "Downloading release asset $asset from $tag"
  curl -fsSL \
    "https://github.com/$REPO_SLUG/releases/download/$tag/$asset" \
    -o "$archive_path" || return 1
  tar -C "$WORKDIR/bin" -xzf "$archive_path" || return 1

  BIN_DIR="$WORKDIR/bin"
  download_fixture_files "$tag" || return 1
}

build_from_source() {
  local repo_root
  repo_root="${1:-}"

  if [ -z "$repo_root" ]; then
    need_cmd git
    need_cmd cargo

    repo_root="$WORKDIR/source"
    if [ ! -d "$repo_root/.git" ]; then
      log "Cloning $REPO_SLUG at $SOURCE_REF"
      git clone --depth 1 --branch "$SOURCE_REF" "https://github.com/$REPO_SLUG.git" "$repo_root"
    fi
  fi

  need_cmd cargo
  log "Building omnigraph binaries from source"
  (
    cd "$repo_root"
    cargo build --release --locked -p omnigraph-cli -p omnigraph-server
  )

  BIN_DIR="$repo_root/target/release"
  FIXTURE_DIR="$repo_root/crates/omnigraph/tests/fixtures"
}

setup_binaries() {
  local repo_root
  repo_root="$(repo_root_from_shell || true)"

  if [ -n "${OMNIGRAPH_BIN_DIR:-}" ]; then
    BIN_DIR="$OMNIGRAPH_BIN_DIR"
    if [ -n "${OMNIGRAPH_FIXTURE_DIR:-}" ]; then
      FIXTURE_DIR="$OMNIGRAPH_FIXTURE_DIR"
    elif [ -n "$repo_root" ]; then
      FIXTURE_DIR="$repo_root/crates/omnigraph/tests/fixtures"
    fi
  elif [ -n "$repo_root" ]; then
    build_from_source "$repo_root"
  elif ! download_release_binaries; then
    build_from_source
  fi

  [ -x "$BIN_DIR/omnigraph" ] || die "omnigraph binary not found in $BIN_DIR"
  [ -x "$BIN_DIR/omnigraph-server" ] || die "omnigraph-server binary not found in $BIN_DIR"
  [ -f "$FIXTURE_DIR/context.pg" ] || die "context fixture schema not found in $FIXTURE_DIR"
  [ -f "$FIXTURE_DIR/context.jsonl" ] || die "context fixture data not found in $FIXTURE_DIR"
}

start_rustfs() {
  mkdir -p "$RUSTFS_DATA_DIR"

  if docker ps --format '{{.Names}}' | grep -qx "$RUSTFS_CONTAINER_NAME"; then
    log "Reusing existing RustFS container $RUSTFS_CONTAINER_NAME"
    return
  fi

  if docker ps -a --format '{{.Names}}' | grep -qx "$RUSTFS_CONTAINER_NAME"; then
    log "Removing stopped RustFS container $RUSTFS_CONTAINER_NAME"
    docker rm -f "$RUSTFS_CONTAINER_NAME" >/dev/null
  fi

  log "Starting RustFS on $AWS_ENDPOINT_URL_S3"
  docker run -d \
    --name "$RUSTFS_CONTAINER_NAME" \
    -p 9000:9000 \
    -p 9001:9001 \
    -v "$RUSTFS_DATA_DIR:/data" \
    -e RUSTFS_ACCESS_KEY="$AWS_ACCESS_KEY_ID" \
    -e RUSTFS_SECRET_KEY="$AWS_SECRET_ACCESS_KEY" \
    "$RUSTFS_IMAGE" \
    /data >/dev/null
}

wait_for_rustfs() {
  local attempt
  for attempt in $(seq 1 30); do
    if "$AWS_BIN" --endpoint-url "$AWS_ENDPOINT_URL_S3" s3api list-buckets >/dev/null 2>&1; then
      return
    fi
    sleep 2
  done

  docker logs "$RUSTFS_CONTAINER_NAME" || true
  die "RustFS did not become ready"
}

ensure_bucket() {
  log "Ensuring bucket $BUCKET exists"
  "$AWS_BIN" --endpoint-url "$AWS_ENDPOINT_URL_S3" \
    s3api create-bucket --bucket "$BUCKET" >/dev/null 2>&1 || true
}

initialize_repo() {
  if "$BIN_DIR/omnigraph" snapshot "$REPO_URI" --json >/dev/null 2>&1; then
    log "Reusing existing repo at $REPO_URI"
    return
  fi

  log "Initializing repo at $REPO_URI"
  "$BIN_DIR/omnigraph" init --schema "$FIXTURE_DIR/context.pg" "$REPO_URI"

  log "Loading context fixture into $REPO_URI"
  "$BIN_DIR/omnigraph" load --data "$FIXTURE_DIR/context.jsonl" "$REPO_URI"
}

start_server() {
  mkdir -p "$WORKDIR"

  if [ -f "$SERVER_PID_FILE" ] && kill -0 "$(cat "$SERVER_PID_FILE")" >/dev/null 2>&1; then
    log "Stopping existing server process $(cat "$SERVER_PID_FILE")"
    kill "$(cat "$SERVER_PID_FILE")" >/dev/null 2>&1 || true
    sleep 1
  fi

  log "Starting omnigraph-server on $BIND"
  nohup "$BIN_DIR/omnigraph-server" "$REPO_URI" --bind "$BIND" >"$SERVER_LOG" 2>&1 &
  echo "$!" > "$SERVER_PID_FILE"
}

wait_for_server() {
  local bind_host bind_port health_host base_url
  bind_host="${BIND%:*}"
  bind_port="${BIND##*:}"
  health_host="$bind_host"
  if [ "$health_host" = "0.0.0.0" ]; then
    health_host="127.0.0.1"
  fi
  base_url="http://$health_host:$bind_port"

  for _ in $(seq 1 30); do
    if curl -fsSL "$base_url/healthz" >/dev/null 2>&1; then
      printf '%s\n' "$base_url"
      return
    fi
    sleep 1
  done

  cat "$SERVER_LOG" >&2 || true
  die "omnigraph-server did not pass /healthz"
}

print_summary() {
  local base_url="$1"

  cat <<EOF

Omnigraph local RustFS demo is up.

Server:
  $base_url

Repo URI:
  $REPO_URI

RustFS console:
  http://127.0.0.1:9001

Useful commands:
  curl -fsSL "$base_url/healthz"
  curl -fsSL "$base_url/snapshot?branch=main"
  "$BIN_DIR/omnigraph" snapshot "$REPO_URI" --json
  tail -f "$SERVER_LOG"
  kill \$(cat "$SERVER_PID_FILE")
  docker logs -f "$RUSTFS_CONTAINER_NAME"

EOF
}

main() {
  need_cmd docker
  need_cmd curl
  docker info >/dev/null 2>&1 || die "docker is installed but the daemon is not reachable; start Docker Desktop or another daemon and rerun"

  export AWS_ACCESS_KEY_ID
  export AWS_SECRET_ACCESS_KEY
  export AWS_REGION
  export AWS_ENDPOINT_URL
  export AWS_ENDPOINT_URL_S3
  export AWS_ALLOW_HTTP
  export AWS_S3_FORCE_PATH_STYLE

  mkdir -p "$WORKDIR"

  setup_binaries
  ensure_aws_cli
  start_rustfs
  wait_for_rustfs
  ensure_bucket
  initialize_repo
  start_server
  print_summary "$(wait_for_server)"
}

main "$@"
