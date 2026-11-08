---
"@core/sync-service": patch
---

Lock Bandit 1.14.1 so the last HTTP/1 response before `max_requests` carries `connection: close` (Bandit 1.12.2+) and the Bandit 1.12.x HTTP/1 and HTTP/2 advisories are fixed, and log the formatted error when an initial snapshot fails.
