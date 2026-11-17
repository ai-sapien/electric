---
'@core/sync-service': patch
---

Subset snapshots now compute subquery row tags from the shape's resolved handle instead of the handle the client sent. A client's first subset request carries no handle, so rows it loaded were tagged `md5(stack_id || '' || value)` and no later move-out pattern could match them: those rows stayed on the client after they left the shape.
