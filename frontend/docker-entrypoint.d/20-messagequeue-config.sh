#!/bin/sh
set -eu

api_base_url="${API_BASE_URL:-/api}"
escaped=$(printf '%s' "$api_base_url" | sed 's|\\|\\\\|g; s|"|\\"|g')
case "${CREATE_ENABLED:-false}" in
  1|true|TRUE|True|yes|YES|Yes)
    create_enabled=true
    ;;
  *)
    create_enabled=false
    ;;
esac
{
  printf 'window.MESSAGEQUEUE_API_BASE = "%s";\n' "$escaped"
  printf 'window.MESSAGEQUEUE_CREATE_ENABLED = %s;\n' "$create_enabled"
} > /usr/share/nginx/html/config.js
