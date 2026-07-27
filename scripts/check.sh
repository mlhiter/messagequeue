#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

required_files=(
  AGENTS.md
  DESIGN.md
  PRODUCT.md
  README.md
  ROADMAP.md
  docs/architecture.md
  docs/ia.md
  docs/references.md
  docs/runbook.md
)

for file in "${required_files[@]}"; do
  if [[ ! -s "$file" ]]; then
    echo "missing required file: $file" >&2
    exit 1
  fi
done

if find . -type f -name '*.md' -not -path './.git/*' -print0 \
  | xargs -0 grep -nE 'TODO|TBD|Lorem ipsum'; then
  echo "placeholder text found" >&2
  exit 1
fi

if find . -type f -not -path './.git/*' -print0 \
  | xargs -0 grep -nE '[[:blank:]]$'; then
  echo "trailing whitespace found" >&2
  exit 1
fi

git diff --check
git diff --cached --check

echo "repository checks passed"
