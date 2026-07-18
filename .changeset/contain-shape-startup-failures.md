---
'@core/sync-service': patch
---

Contain shape-consumer initialization races and corrupt persisted replay to the
affected shape. Global LSN broadcasts now wait until initialization completes,
and replay corruption stops with an attributable reason so the shape is purged
and rebuilt instead of cascading through the Electric process. Health checks
now fail closed while replication is disconnected or waiting on its lock, and
repeated corrupt materializer replays latch the stack unhealthy for clean
replacement.
