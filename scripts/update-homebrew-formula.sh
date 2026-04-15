#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: update-homebrew-formula.sh <tag> [formula_path]

Environment:
  REPO_SLUG     GitHub repo that owns the Omnigraph release
                default: ModernRelay/omnigraph
EOF
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    printf 'error: missing required command: %s\n' "$1" >&2
    exit 1
  }
}

asset_digest() {
  local release_json="$1"
  local asset_name="$2"

  jq -r --arg name "$asset_name" '
    .assets[]
    | select(.name == $name)
    | (.digest // "")
    | sub("^sha256:"; "")
  ' <<<"$release_json"
}

TAG="${1:-}"
FORMULA_PATH="${2:-Formula/omnigraph.rb}"
REPO_SLUG="${REPO_SLUG:-ModernRelay/omnigraph}"

if [[ -z "$TAG" ]]; then
  usage
  exit 1
fi

need_cmd gh
need_cmd jq

VERSION="${TAG#v}"
RELEASE_JSON="$(gh release view "$TAG" --repo "$REPO_SLUG" --json assets)"

MACOS_ARM_URL="https://github.com/${REPO_SLUG}/releases/download/${TAG}/omnigraph-macos-arm64.tar.gz"
MACOS_X86_URL="https://github.com/${REPO_SLUG}/releases/download/${TAG}/omnigraph-macos-x86_64.tar.gz"
LINUX_X86_URL="https://github.com/${REPO_SLUG}/releases/download/${TAG}/omnigraph-linux-x86_64.tar.gz"

MACOS_ARM_SHA="$(asset_digest "$RELEASE_JSON" "omnigraph-macos-arm64.tar.gz")"
MACOS_X86_SHA="$(asset_digest "$RELEASE_JSON" "omnigraph-macos-x86_64.tar.gz")"
LINUX_X86_SHA="$(asset_digest "$RELEASE_JSON" "omnigraph-linux-x86_64.tar.gz")"

for value in "$MACOS_ARM_SHA" "$MACOS_X86_SHA" "$LINUX_X86_SHA"; do
  if [[ -z "$value" ]]; then
    printf 'error: failed to resolve one or more release asset digests for %s\n' "$TAG" >&2
    exit 1
  fi
done

mkdir -p "$(dirname "$FORMULA_PATH")"

cat >"$FORMULA_PATH" <<EOF
class Omnigraph < Formula
  desc "Typed property graph database with Git-style workflows"
  homepage "https://github.com/${REPO_SLUG}"
  license "MIT"
  version "${VERSION}"

  on_macos do
    if Hardware::CPU.arm?
      url "${MACOS_ARM_URL}"
      sha256 "${MACOS_ARM_SHA}"
    else
      url "${MACOS_X86_URL}"
      sha256 "${MACOS_X86_SHA}"
    end
  end

  on_linux do
    url "${LINUX_X86_URL}"
    sha256 "${LINUX_X86_SHA}"
  end

  head "https://github.com/${REPO_SLUG}.git", branch: "main"

  livecheck do
    url :stable
    regex(/^v?(\\d+(?:\\.\\d+)+)$/i)
  end

  def install
    bin.install "omnigraph", "omnigraph-server"
  end

  test do
    assert_match "omnigraph ", shell_output("#{bin}/omnigraph version")
    assert_match "HTTP server for the Omnigraph graph database", shell_output("#{bin}/omnigraph-server --help")
  end
end
EOF
