---
"@core/sync-service": patch
---

Align the Sapien server with upstream 1.8.1, retaining bounded identifiers,
cold-start inspector/publication optimizations, typed publication errors and
nullable-permission move-in correctness. Use upstream resnapshot recovery with
a new metadata generation and verify continuous clients across graceful and
crash restarts. Publish versioned AMD64/ARM64 images through the existing Sapien
attestation workflow.
