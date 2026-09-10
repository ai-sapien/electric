# Sapien upstream alignment

The server is based on upstream `bb397424db0e1c153dc356713fd3dfd40315470c`
(Electric 1.8.1). The deployment branch remains `sapien/shape-startup` so existing
GHCR signer and source-ref verification continues to bind images to that branch.
The merge records the previous deployment ancestry while rebuilding from the
upstream tree and reapplying the patches below.

## Retained patches

- Bound generated PostgreSQL identifiers to 63 bytes with stable hash suffixes.
- Coalesce and batch cold inspector lookups, with lookup timeouts and bounded
  negative-cache lifetime. Keep the worker supervisor visible in the stack tree.
- Batch publication/replica-identity updates without starving queued changes.
- Preserve typed publication failures when removing affected shapes.
- Use `IS NOT TRUE` for the old predicate during move-in selection so NULL to
  authorized transitions are included.
- Keep db_connection 2.10.2.
- Build and attest AMD64 and ARM64 images with the actual package version.

Upstream owns replication acknowledgments, storage, materializers and recovery.
The previous custom replay engine and Bandit patch are retired. Bandit 1.12.0
includes the HTTP connection lifecycle correction.

## Recovery contract

Metadata generation 12 intentionally differs from upstream 10 and the previous
Sapien generation 11. The first upgrade creates fresh shape metadata. Upstream
drops subquery graphs on subsequent restart, so existing clients receive 409,
replace their snapshot and resume updates. The required guarantee is correct
permission-filtered state without an application reload. Stable shape handles
and exact historical replay across restart are not required.

This trades additional snapshot work after restart for substantially less
custom storage and replication code. Validate workload/resource limits in the
consumer repository before rolling out. A clean build is not deployment proof.

## Verification

Run `scripts/ci/test-sapien-sync-service.sh` from the repository root with the
Postgres services in `packages/sync-service/dev` available. It runs the Elixir
client suite, full server suite and retained-client permission recovery cases
under graceful and brutal restart. Router tests wait for observable replication
and compaction completion while retaining exact sequence assertions.

The consumer repository also verifies the JavaScript client's quiet completion
and handle replacement behavior, real old/new-image compatibility, canonical
citation predicates, incident-scale recovery, resource limits and provenance.
