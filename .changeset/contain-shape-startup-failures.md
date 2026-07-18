---
'@core/sync-service': patch
---

Contain shape-consumer initialization races and corrupt persisted replay to the
affected shape. Global LSN broadcasts now wait until initialization completes,
and replay corruption stops with an attributable reason so the shape is purged
and rebuilt instead of cascading through the Electric process.
