#!/bin/sh
set -eu

api_base_url="${API_BASE_URL:-/api}"
escaped=$(printf '%s' "$api_base_url" | sed 's|\\|\\\\|g; s|"|\\"|g')
{
  printf 'window.MESSAGEQUEUE_API_BASE = "%s";\n' "$escaped"
} > /usr/share/nginx/html/config.js
