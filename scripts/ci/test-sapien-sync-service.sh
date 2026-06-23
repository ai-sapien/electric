#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
sync_service_dir="${repo_root}/packages/sync-service"

cd "${sync_service_dir}"

mix deps.get

mix format --check-formatted \
  lib/electric/connection/manager/pool.ex \
  lib/electric/postgres/configuration.ex \
  lib/electric/postgres/inspector/direct_inspector.ex \
  lib/electric/postgres/inspector/ets_inspector.ex \
  lib/electric/postgres/snapshot_query.ex \
  lib/electric/replication/publication_manager.ex \
  lib/electric/replication/publication_manager/configurator.ex \
  lib/electric/replication/publication_manager/relation_tracker.ex \
  lib/electric/shape_cache.ex \
  lib/electric/shape_cache/shape_status.ex \
  lib/electric/shape_cache/shape_status/shape_db.ex \
  lib/electric/shapes/consumer/materializer.ex \
  lib/electric/shapes/consumer/snapshotter.ex \
  lib/electric/shapes/querying.ex \
  test/electric/connection/manager/pool_test.exs \
  test/electric/postgres/configuration_test.exs \
  test/electric/postgres/inspector/ets_inspector_test.exs \
  test/electric/replication/publication_manager_test.exs \
  test/electric/shape_cache/shape_status_test.exs \
  test/electric/shape_cache_test.exs \
  test/electric/shapes/consumer/materializer_test.exs \
  test/integration/streaming_test.exs \
  test/support/component_setup.ex

mix test \
  test/electric/connection/manager/pool_test.exs \
  test/electric/postgres/configuration_test.exs \
  test/electric/postgres/inspector/ets_inspector_test.exs \
  test/electric/replication/publication_manager_test.exs \
  test/electric/shape_cache/shape_status_test.exs \
  test/electric/shape_cache_test.exs \
  test/electric/shapes/consumer/materializer_test.exs \
  test/integration/streaming_test.exs
