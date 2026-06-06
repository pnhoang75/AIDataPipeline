#!/bin/bash
set -e

REMOTE="https://github.com/pnhoang75/AIDataPipeline.git"

echo "→ Initialising git repo..."
git init
git branch -M main

echo "→ Staging files..."
git add .

echo "→ Creating initial commit..."
git commit -m "Initial design docs: pipeline, multi-tenancy, UI, operators"

echo "→ Setting remote..."
git remote add origin "$REMOTE" 2>/dev/null || git remote set-url origin "$REMOTE"

echo "→ Pushing to GitHub..."
git push -u origin main

echo ""
echo "Done! View at: https://github.com/pnhoang75/AIDataPipeline"
