#!/bin/sh
set -eu

lock_hash="$(sha256sum package-lock.json | cut -d ' ' -f 1)"
installed_hash="$(cat node_modules/.lock-hash 2>/dev/null || true)"

if [ "$lock_hash" != "$installed_hash" ]; then
  npm ci
  printf '%s\n' "$lock_hash" > node_modules/.lock-hash
fi

exec npm run dev
