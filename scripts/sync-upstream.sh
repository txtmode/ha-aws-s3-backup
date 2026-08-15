#!/usr/bin/env bash
# Refresh the `upstream` vendor branch from home-assistant/core.
#
# The `upstream` branch holds a pristine copy of
# homeassistant/components/aws_s3 — no local modifications, ever. `main` is
# that branch plus our IAM-role patch, so updating is a normal 3-way merge:
#
#   scripts/sync-upstream.sh 2026.9.0
#   git checkout main && git merge upstream
#
# Conflicts can only appear where upstream touched the ~60 patched lines.
set -euo pipefail

TAG="${1:-}"
if [ -z "$TAG" ]; then
  echo "usage: $0 <core-tag>            e.g. $0 2026.9.0" >&2
  echo "       $0 dev                   vendor the current dev branch" >&2
  exit 1
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPONENT_PATH="homeassistant/components/aws_s3"
VENDOR_PATH="custom_components/aws_s3"
cd "$REPO_ROOT"

if [ -n "$(git status --porcelain)" ]; then
  echo "error: working tree is dirty — commit or stash first" >&2
  exit 1
fi
START_BRANCH="$(git rev-parse --abbrev-ref HEAD)"

TMP="$(mktemp -d)"
cleanup() { rm -rf "$TMP"; }
trap cleanup EXIT

echo ">> fetching $COMPONENT_PATH from home-assistant/core@$TAG"
# Blobless + sparse: pulls only this one directory's blobs, a few hundred KB.
git clone --quiet --filter=blob:none --no-checkout --depth=1 \
  --branch "$TAG" https://github.com/home-assistant/core.git "$TMP/core"
git -C "$TMP/core" sparse-checkout set --no-cone "$COMPONENT_PATH" >/dev/null
git -C "$TMP/core" checkout --quiet
CORE_SHA="$(git -C "$TMP/core" rev-parse HEAD)"

if [ ! -d "$TMP/core/$COMPONENT_PATH" ]; then
  echo "error: $COMPONENT_PATH not found at $TAG" >&2
  exit 1
fi

if git show-ref --verify --quiet refs/heads/upstream; then
  git checkout --quiet upstream
else
  echo ">> creating orphan branch 'upstream'"
  git checkout --quiet --orphan upstream
  git rm -rqf --ignore-unmatch . >/dev/null
fi

rm -rf "$VENDOR_PATH"
mkdir -p "$(dirname "$VENDOR_PATH")"
cp -R "$TMP/core/$COMPONENT_PATH" "$VENDOR_PATH"

cat > UPSTREAM <<EOF
# Pristine vendor drop of home-assistant/core:$COMPONENT_PATH
# Refresh with scripts/sync-upstream.sh <core-tag>; never edit files here by hand.
tag=$TAG
sha=$CORE_SHA
EOF

git add -A
if git diff --cached --quiet; then
  echo ">> already at $TAG — nothing to commit"
else
  git commit --quiet -m "vendor: home-assistant/core $TAG" -m "$COMPONENT_PATH @ $CORE_SHA"
  echo ">> committed vendor drop for $TAG"
fi

git checkout --quiet "$START_BRANCH"
echo
echo "Next:  git checkout main && git merge upstream"
echo "Then:  push both branches — git push origin main upstream"
