#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
sync_service_dir="${repo_root}/packages/sync-service"
elixir_client_dir="${repo_root}/packages/elixir-client"

export MIX_ENV=test

(
  cd "${elixir_client_dir}"

  mix deps.get

  mix compile --warnings-as-errors

  mix format --check-formatted \
    lib/electric/client.ex \
    lib/electric/client/fetch.ex \
    lib/electric/client/fetch/monitor.ex \
    lib/electric/client/fetch/pool.ex \
    lib/electric/client/poll.ex \
    test/electric/client/fetch/pool_test.exs \
    test/electric/client_test.exs

  mix test --timeout 30000
)

cd "${sync_service_dir}"

mix deps.get

mix compile --warnings-as-errors

mix format --check-formatted \
  config/runtime.exs \
  lib/electric/application.ex \
  lib/electric/config.ex \
  lib/electric/connection/manager.ex \
  lib/electric/connection/manager/pool.ex \
  lib/electric/postgres/configuration.ex \
  lib/electric/postgres/causal_marker.ex \
  lib/electric/postgres/inspector/direct_inspector.ex \
  lib/electric/postgres/inspector/ets_inspector.ex \
  lib/electric/postgres/replication_client.ex \
  lib/electric/postgres/replication_client/connection_setup.ex \
  lib/electric/postgres/replication_client/message_converter.ex \
  lib/electric/postgres/snapshot_query.ex \
  lib/electric/replication/shape_log_collector.ex \
  lib/electric/replication/publication_manager.ex \
  lib/electric/replication/publication_manager/configurator.ex \
  lib/electric/replication/publication_manager/relation_tracker.ex \
  lib/electric/shape_cache.ex \
  lib/electric/shape_cache/in_memory_storage.ex \
  lib/electric/shape_cache/pure_file_storage.ex \
  lib/electric/shape_cache/pure_file_storage/chunk_index.ex \
  lib/electric/shape_cache/pure_file_storage/log_file.ex \
  lib/electric/shape_cache/pure_file_storage/shared_records.ex \
  lib/electric/shape_cache/pure_file_storage/write_loop.ex \
  lib/electric/shape_cache/shape_cleaner.ex \
  lib/electric/shape_cache/shape_status.ex \
  lib/electric/shape_cache/shape_status/shape_db.ex \
  lib/electric/shape_cache/storage.ex \
  lib/electric/shapes/api.ex \
  lib/electric/shapes/consumer.ex \
  lib/electric/shapes/consumer/effects.ex \
  lib/electric/shapes/consumer/event_handler/subqueries/buffering.ex \
  lib/electric/shapes/consumer/initial_snapshot.ex \
  lib/electric/shapes/consumer/materializer.ex \
  lib/electric/shapes/consumer/materializer/replay_coordinator.ex \
  lib/electric/shapes/consumer/pending_txn.ex \
  lib/electric/shapes/consumer/snapshotter.ex \
  lib/electric/shapes/consumer/state.ex \
  lib/electric/shapes/consumer/subqueries/active_move.ex \
  lib/electric/shapes/consumer_registry.ex \
  lib/electric/shapes/querying.ex \
  lib/electric/shapes/shape.ex \
  lib/electric/stack_config.ex \
  lib/electric/stack_supervisor.ex \
  test/electric/config_test.exs \
  test/electric/connection/manager/pool_test.exs \
  test/electric/connection/manager_test.exs \
  test/electric/postgres/configuration_test.exs \
  test/electric/postgres/causal_marker_test.exs \
  test/electric/postgres/inspector/ets_inspector_test.exs \
  test/electric/postgres/replication_client_test.exs \
  test/electric/postgres/replication_client/connection_setup_test.exs \
  test/electric/postgres/replication_client/message_converter_test.exs \
  test/electric/plug/router_test.exs \
  test/electric/replication/publication_manager_test.exs \
  test/electric/replication/shape_log_collector_test.exs \
  test/electric/shape_cleaner_test.exs \
  test/electric/shape_cache/pure_file_storage/chunk_index_test.exs \
  test/electric/shape_cache/pure_file_storage/log_file_test.exs \
  test/electric/shape_cache/pure_file_storage_test.exs \
  test/electric/shape_cache/storage_implementations_test.exs \
  test/electric/shape_cache/shape_status_test.exs \
  test/electric/shape_cache_test.exs \
  test/electric/shapes/api_test.exs \
  test/electric/shapes/consumer_test.exs \
  test/electric/shapes/consumer/effects_test.exs \
  test/electric/shapes/consumer/event_handler/subqueries_test.exs \
  test/electric/shapes/consumer_registry_test.exs \
  test/electric/shapes/consumer/materializer_test.exs \
  test/electric/shapes/consumer/state_test.exs \
  test/electric/shapes/shape_test.exs \
  test/electric/stack_supervisor_test.exs \
  test/integration/oracle_causal_order_test.exs \
  test/integration/oracle_property_test.exs \
  test/integration/oracle_restore_test.exs \
  test/integration/oracle_shape_checker_timeout_test.exs \
  test/integration/streaming_test.exs \
  test/support/component_setup.ex \
  test/support/integration_setup.ex \
  test/support/oracle_harness.ex \
  test/support/oracle_harness/shape_checker.ex \
  test/support/oracle_harness/shape_checker_diagnostics_test.exs \
  test/support/test_storage.ex \
  test/test_helper.exs

sync_test_files=(
  test/electric/config_test.exs
  test/electric/connection/manager/pool_test.exs
  test/electric/connection/manager_test.exs
  test/electric/postgres/configuration_test.exs
  test/electric/postgres/causal_marker_test.exs
  test/electric/postgres/inspector/ets_inspector_test.exs
  test/electric/postgres/replication_client_test.exs
  test/electric/postgres/replication_client/connection_setup_test.exs
  test/electric/postgres/replication_client/message_converter_test.exs
  test/electric/plug/router_test.exs
  test/electric/replication/publication_manager_test.exs
  test/electric/replication/shape_log_collector_test.exs
  test/electric/shape_cleaner_test.exs
  test/electric/shape_cache/pure_file_storage/chunk_index_test.exs
  test/electric/shape_cache/pure_file_storage/log_file_test.exs
  test/electric/shape_cache/pure_file_storage_test.exs
  test/electric/shape_cache/storage_implementations_test.exs
  test/electric/shape_cache/shape_status_test.exs
  test/electric/shape_cache_test.exs
  test/electric/shapes/api_test.exs
  test/electric/shapes/consumer_test.exs
  test/electric/shapes/consumer/effects_test.exs
  test/electric/shapes/consumer/event_handler/subqueries_test.exs
  test/electric/shapes/consumer_registry_test.exs
  test/electric/shapes/consumer/materializer_test.exs
  test/electric/shapes/consumer/state_test.exs
  test/electric/shapes/shape_test.exs
  test/electric/stack_supervisor_test.exs
  test/integration/oracle_causal_order_test.exs
  test/integration/oracle_restore_test.exs
  test/integration/oracle_shape_checker_timeout_test.exs
  test/support/oracle_harness/shape_checker_diagnostics_test.exs
  test/integration/streaming_test.exs
)

# Stateful sync-service tests share global process registries and PostgreSQL
# lifecycle state. Run each file in a fresh BEAM so a deliberately crashed
# component cannot poison an otherwise unrelated test module.
for test_file in "${sync_test_files[@]}"; do
  mix test --include oracle --max-cases 1 --seed 424242 --timeout 300000 "${test_file}"
done

SKIP_REPATCH_PREWARM=true \
CHECK_TIMEOUT=180000 \
SHAPE_COUNT=100 \
BATCH_COUNT=10 \
BATCH_LIMIT=2 \
TXNS_PER_BATCH=10 \
MUTATIONS_PER_TXN=5 \
RUN_COUNT=1 \
LONG_POLL_TIMEOUT=100 \
RESTART_SERVER_EVERY=1 \
RESTART_CLIENT_EVERY=0 \
ORACLE_POOL_SIZE=50 \
mix test --include oracle --seed 424242 --timeout 300000 \
  test/integration/oracle_property_test.exs
