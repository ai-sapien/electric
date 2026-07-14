#!/usr/bin/env bash

set -euo pipefail

sync_service_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
bandit_dir="${sync_service_dir}/deps/bandit"
patch_file="${sync_service_dir}/patches/bandit-http1-max-requests-close.patch"

if [[ ! -d "${bandit_dir}" ]]; then
  printf 'Bandit dependency is missing; run mix deps.get before applying the HTTP/1 patch.\n' >&2
  exit 1
fi

if patch --batch --forward --dry-run -p1 --directory "${bandit_dir}" \
  --input "${patch_file}" >/dev/null; then
  patch --batch --forward -p1 --directory "${bandit_dir}" \
    --input "${patch_file}"
  printf 'Applied Bandit HTTP/1 max-requests close patch.\n'
elif patch --batch --reverse --dry-run -p1 --directory "${bandit_dir}" \
  --input "${patch_file}" >/dev/null; then
  printf 'Bandit HTTP/1 max-requests close patch is already applied.\n'
else
  printf 'Bandit dependency does not match the reviewed HTTP/1 patch baseline.\n' >&2
  exit 1
fi
