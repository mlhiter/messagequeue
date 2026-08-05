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

repo_files() {
  git ls-files --cached --others --exclude-standard -z "$@"
}

if repo_files -- '*.md' \
  | xargs -0 grep -nE 'TODO|TBD|Lorem ipsum'; then
  echo "placeholder text found" >&2
  exit 1
fi

if repo_files \
  | xargs -0 grep -nIE '[[:blank:]]$'; then
  echo "trailing whitespace found" >&2
  exit 1
fi

git diff --check
git diff --cached --check

echo "repository checks passed"
