#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
export MIX_ENV=test
cd "${repo_root}/packages/elixir-client"
mix deps.get
mix compile --warnings-as-errors
mix test --timeout 30000
cd "${repo_root}/packages/sync-service"
mix deps.get
mix compile --warnings-as-errors
mix format --check-formatted
mix test --max-cases 2 --seed 424242 --timeout 300000
mix test --include oracle --max-cases 1 --seed 424242 --timeout 300000 \
  test/integration/sapien_recovery_compatibility_test.exs
RESTART_TYPE=brutal mix test --include oracle --max-cases 1 --seed 424242 --timeout 300000 \
  test/integration/sapien_recovery_compatibility_test.exs
