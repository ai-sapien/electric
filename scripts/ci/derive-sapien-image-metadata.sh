#!/usr/bin/env bash

set -euo pipefail

event_name="${1:?GitHub event name is required}"
revision="${2:?Git revision is required}"
candidate="${3:-}"
image="${IMAGE:?IMAGE is required}"
output_file="${GITHUB_OUTPUT:?GITHUB_OUTPUT is required}"

if [[ ! "${revision}" =~ ^[0-9a-f]{40}$ ]]; then
  printf 'Git revision must be a lowercase 40-character SHA.\n' >&2
  exit 1
fi

short_sha="${revision:0:12}"
version="1.7.7-sapien.${short_sha}"

case "${event_name}" in
  push)
    tags="${image}:sapien-${short_sha},${image}:sapien-latest"
    ;;
  workflow_dispatch)
    if [[ ! "${candidate}" =~ ^pr-[1-9][0-9]*$ ]]; then
      printf 'Manual image candidates must match ^pr-[1-9][0-9]*$.\n' >&2
      exit 1
    fi

    tags="${image}:sapien-${candidate}-${short_sha}"
    ;;
  *)
    printf 'Unsupported image publication event: %s\n' "${event_name}" >&2
    exit 1
    ;;
esac

{
  printf 'version=%s\n' "${version}"
  printf 'tags=%s\n' "${tags}"
} >> "${output_file}"
