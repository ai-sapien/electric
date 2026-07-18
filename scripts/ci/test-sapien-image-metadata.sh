#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
metadata_script="${repo_root}/scripts/ci/derive-sapien-image-metadata.sh"
test_root="$(mktemp -d)"
trap 'rm -rf "${test_root}"' EXIT

sha="4cf6fa64eda40ce2eb06962007147b975f6ed34d"
short_sha="${sha:0:12}"
image="ghcr.io/ai-sapien/electric"

read_output() {
  local key="$1"
  local output_file="$2"

  sed -n "s/^${key}=//p" "${output_file}"
}

assert_dispatch_metadata() {
  local output_file="${test_root}/dispatch-output"

  GITHUB_OUTPUT="${output_file}" IMAGE="${image}" \
    "${metadata_script}" workflow_dispatch "${sha}" pr-2

  test "$(read_output version "${output_file}")" = "1.7.7-sapien.${short_sha}"
  test "$(read_output tags "${output_file}")" = "${image}:sapien-pr-2-${short_sha}"
  ! grep -q 'sapien-latest' "${output_file}"
}

assert_push_metadata() {
  local output_file="${test_root}/push-output"

  GITHUB_OUTPUT="${output_file}" IMAGE="${image}" \
    "${metadata_script}" push "${sha}"

  test "$(read_output version "${output_file}")" = "1.7.7-sapien.${short_sha}"
  test "$(read_output tags "${output_file}")" = \
    "${image}:sapien-${short_sha},${image}:sapien-latest"
}

assert_invalid_dispatch_candidate_is_rejected() {
  local output_file="${test_root}/invalid-output"

  if GITHUB_OUTPUT="${output_file}" IMAGE="${image}" \
    "${metadata_script}" workflow_dispatch "${sha}" sapien-latest; then
    printf 'Expected reserved candidate name to be rejected.\n' >&2
    return 1
  fi
}

assert_dispatch_metadata
assert_push_metadata
assert_invalid_dispatch_candidate_is_rejected
