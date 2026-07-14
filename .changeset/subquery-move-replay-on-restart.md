---
'@core/elixir-client': patch
'@core/sync-service': patch
---

Fix optimized streaming subquery shapes losing, duplicating, or reordering
dependency move-ins/move-outs across server restarts and same-transaction
updates. Restored dependency graphs now initialize in two phases, replay each
outer consumer's exact persisted source position through a bounded pull worker,
and become externally ready only after their root and generated work is
consistent. Root replication runs before same-transaction materializer output,
with multi-hop output ordered by causal depth.

Elixir client polls now accept an explicit timeout that is enforced as one
absolute deadline across request coalescing, monitor startup, registration,
retries, and response delivery. Timed-out or dead subscribers are removed
without cancelling a request still used by another subscriber, orphaned
requests are stopped after their final subscriber leaves, and revocable reply
aliases prevent late large responses from remaining in caller mailboxes.

Dependency logs now keep uncompacted history, and storage compaction is part of
shape identity so a compacted shape cannot be reused as a replay dependency.
The persistent shape-cache version is now 11; caches written with the previous
identity are discarded and rebuilt instead of restoring incompatible history.
Generated-shape storage-range handoffs also retain the transaction ids already
persisted on rows and move controls, including move-in controls whose snapshot
rows are appended later in the same logical batch. Persisted transaction
metadata is validated and unioned with any live notification xid instead of
being replaced by an empty generated-move xid.

Startup now emits an exact transactional logical marker immediately before
replication begins and publishes external readiness only after that marker's
commit has passed through the ShapeLogCollector, its real replication flush
boundary is durable, and every cached Consumer has drained causal work at or
below the target. The target-scoped fixed-point drain closes atomically against
new causal reservations and topology changes, uses bounded concurrency, ignores
later live traffic, and fails closed on one configurable absolute deadline
instead of leaving startup stuck forever. PostgreSQL keepalive `wal_end` values
are transport hints only and never prove logical delivery or causal progress.

Pure-file storage now exposes one atomic, versioned read generation for log
bytes and dependency positions. Exact replay streams defer opening their
generation files until enumeration, own those descriptors within the stream,
and close all of them on early halt, failed initialization, or retry. Replay,
live fan-out, and deferred Consumer queues are bounded and fail closed by
invalidating the rebuildable outer shape. Generated-column invalidation also
propagates through normal shape cleanup instead of leaving a failed graph live.
Materializer link-value snapshots cached in stack ETS are removed on graceful
Materializer termination and during destructive shape cleanup, including when
the ShapeStatus row is already gone. Repeated purge and refetch cycles therefore
cannot retain citation-sized link-value sets until the whole stack restarts.
Completed chunk-index entries stay staged until their root or dependency-move
cursor commits, preventing rolling legacy readers from observing an
uncommitted transaction.

When storage coalesces a flush past one or more mapped shape writes, Consumer
acknowledgements now advance to the latest covered logical transaction boundary
while preserving mappings for later writes. This prevents startup readiness
from waiting forever on transaction bytes that are already durable.

Custom storage adapters remain compatible with ordinary shapes. Subquery shapes
now validate their stronger storage contract before cache creation and require
`get_log_stream_with_offsets/3`, `begin_move_transaction!/1`, and
`commit_move_transaction!/2`; an adapter missing any of them receives an
actionable error naming the callbacks. `get_log_replay_safe_cursor/1` remains
optional and falls back conservatively to the adapter's latest offset. Internal
callers of `Materializer.subscribe/2` must now handle `{:pending, offset}` for a
stale cursor and pull bounded replay work through `Materializer.next_replay/2`.

Relevant resource and startup controls are positive-integer environment values:

- `ELECTRIC_MATERIALIZER_REPLAY_MEMORY_LIMIT_BYTES` (default 8 MiB)
- `ELECTRIC_MATERIALIZER_REPLAY_MAX_PENDING` (default 100)
- `ELECTRIC_MATERIALIZER_REPLAY_IDLE_TIMEOUT_MS` (default 30,000)
- `ELECTRIC_MATERIALIZER_LIVE_MAX_SUBSCRIBERS` (default 1,000)
- `ELECTRIC_MATERIALIZER_LIVE_BACKLOG_MEMORY_LIMIT_BYTES` (default 8 MiB)
- `ELECTRIC_MATERIALIZER_CAUSAL_CALL_TIMEOUT_MS` (default 30,000)
- `ELECTRIC_CAUSAL_DRAIN_MAX_CONCURRENCY` (default 32)
- `ELECTRIC_CAUSAL_DRAIN_TIMEOUT_MS` (default 600,000)
- `ELECTRIC_SUBQUERY_BUFFER_MAX_TRANSACTIONS` (default 1,000)
- `ELECTRIC_SUBQUERY_DEFERRED_EVENT_MEMORY_LIMIT_BYTES` (default 1 MiB)

Exceeding a replay, fan-out, or deferred-work bound invalidates the rebuildable
outer shape instead of retaining unbounded memory. Exceeding the startup drain
deadline keeps external readiness closed and fails the catch-up attempt.

This release intentionally rebuilds persisted caches. Shape-cache metadata uses
version 11 because storage compaction joined shape identity, and pure-file shape
storage uses version 2 for its atomic durability contract. Older metadata and
shape-log directories are not migrated in place: they are discarded and
reconstructed from Postgres. Clients holding a discarded handle resync through
the normal shape-refetch path. A rollback likewise rejects version-2 pure-file
storage and rebuilds it under the older format rather than reading mixed
durability metadata.
