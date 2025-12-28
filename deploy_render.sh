#!/usr/bin/env bash
set -euo pipefail

# Usage: ./deploy_render.sh <git-remote-url> [remote-name] [branch]
# Example: ./deploy_render.sh git@github.com:me/erp-repo.git render main

REMOTE_URL="$1"
REMOTE_NAME="${2:-render}"
BRANCH="${3:-main}"

if [ -z "$REMOTE_URL" ]; then
  echo "Error: remote URL required."
  echo "Usage: $0 <git-remote-url> [remote-name] [branch]"
  exit 1
fi

echo "Deploy script starting:"
echo "  Remote URL: $REMOTE_URL"
echo "  Remote name: $REMOTE_NAME"
echo "  Branch: $BRANCH"

# Ensure we're in repo root (script location)
cd "$(dirname "$0")/" || exit 1

# Initialize git if needed
if [ ! -d .git ]; then
  echo "No .git found — initializing repository"
  git init
fi

# Add or update remote
if git remote get-url "$REMOTE_NAME" >/dev/null 2>&1; then
  echo "Remote '$REMOTE_NAME' exists — updating URL"
  git remote set-url "$REMOTE_NAME" "$REMOTE_URL"
else
  echo "Adding remote '$REMOTE_NAME' -> $REMOTE_URL"
  git remote add "$REMOTE_NAME" "$REMOTE_URL"
fi

# Ensure we have a commit to push
git add -A
if git diff --cached --quiet; then
  echo "No changes to commit."
else
  git commit -m "Prepare repo for Render deployment"
fi

# Push current HEAD to remote branch
echo "Pushing to $REMOTE_NAME/$BRANCH..."
git push -u "$REMOTE_NAME" HEAD:"$BRANCH"

echo "Push complete. Your repo is on $REMOTE_NAME/$BRANCH"
