#!/usr/bin/env bash
# Apply branch protection rules to the main branch.
#
# Requires:
#   - `gh` CLI authenticated.
#   - Repository-admin or org-admin permissions on ModernRelay/omnigraph.
#
# This script is idempotent: re-running applies whatever is currently
# declared in .github/branch-protection.json. The JSON file is the
# source of truth; this script is the apply mechanism.
#
# Usage:
#   ./scripts/apply-branch-protection.sh                    # apply to main
#   REPO=ModernRelay/omnigraph BRANCH=main ./scripts/...    # explicit
#   DRY_RUN=1 ./scripts/apply-branch-protection.sh          # show what would apply

set -euo pipefail

REPO="${REPO:-ModernRelay/omnigraph}"
BRANCH="${BRANCH:-main}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POLICY_FILE="${POLICY_FILE:-$HERE/../.github/branch-protection.json}"

if [ ! -f "$POLICY_FILE" ]; then
    echo "error: policy file not found: $POLICY_FILE" >&2
    exit 1
fi

# Strip the _comment field — the GitHub API rejects unknown keys.
PAYLOAD="$(mktemp)"
trap 'rm -f "$PAYLOAD"' EXIT
jq 'del(._comment)' "$POLICY_FILE" > "$PAYLOAD"

if [ "${DRY_RUN:-0}" = "1" ]; then
    echo "DRY RUN — would apply to $REPO/$BRANCH:"
    echo
    cat "$PAYLOAD"
    exit 0
fi

echo "Applying branch protection to $REPO/$BRANCH from $POLICY_FILE"
gh api -X PUT "repos/$REPO/branches/$BRANCH/protection" \
    --input "$PAYLOAD" \
    -H "Accept: application/vnd.github+json" \
    > /dev/null
echo "OK"

# Verify by reading back.
echo
echo "Current policy on $REPO/$BRANCH:"
gh api "repos/$REPO/branches/$BRANCH/protection" \
    --jq '{
        required_status_checks: .required_status_checks.contexts,
        strict_status_checks: .required_status_checks.strict,
        required_approvals: .required_pull_request_reviews.required_approving_review_count,
        require_code_owner_reviews: .required_pull_request_reviews.require_code_owner_reviews,
        enforce_admins: .enforce_admins.enabled,
        linear_history: .required_linear_history.enabled,
        force_pushes_allowed: .allow_force_pushes.enabled,
        deletions_allowed: .allow_deletions.enabled,
        conversation_resolution_required: .required_conversation_resolution.enabled
    }'
