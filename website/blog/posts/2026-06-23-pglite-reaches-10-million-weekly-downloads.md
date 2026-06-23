---
title: '...'
description: >-
  ...
excerpt: >-
  ...
authors: [samwillis, tdrz]
image: /img/blog/pglite-reaches-10-million-weekly-downloads/header.jpg
tags: [PGlite, Postgres]
outline: [2, 3]
post: true
published: false
---

PGlite has reached 10 million weekly npm downloads. Thank you to everyone who has tried it, built with it, opened issues, contributed extensions, or told us where it broke.

We want to mark the milestone by looking back at how a small Postgres-in-WASM experiment became a widely used embedded Postgres project, and by sharing where we want to take it next.



> [!WARNING] 🪧  Quicklinks
> [PGlite](https://pglite.dev/) is Postgres compiled to WASM, packaged for JavaScript environments including browsers, Node.js, Bun, and Deno.
>
> - [PGlite docs](https://pglite.dev/)
> - [GitHub](https://github.com/electric-sql/pglite/)
> - [Discord](https://discord.gg/pVASdMED)

## Postgres is showing up in smaller places



- Developers increasingly need Postgres in places that used to be too small, too temporary, or too user-controlled for a database server.
- AI sandboxes need a database inside the runtime so generated apps can run immediately.
- CI pipelines need fast, isolated databases that can be created, reset, and thrown away cheaply.
- Local-first apps need durable local storage with rich query semantics.
- Sync systems need a local target that can preserve the same schema, types, and behavior as remote Postgres.
- These use cases look different, but they point at the same need: real Postgres closer to the application.

## The closer you get to production, the less glue you need



- The teams that benefit are the ones that can keep development, testing, local state, and production on the same database model.
- They avoid rewriting schemas, test fixtures, migrations, and query behavior for a weaker local substitute.
- They can use Postgres features earlier: types, indexes, constraints, full-text search, `pgvector`, PostGIS, and other extensions.
- The teams that struggle are the ones forced to choose between a heavy server setup and a lightweight environment that does not behave like production.

## The same database model everywhere



- 
- It is Postgres semantics wherever the application needs them: in a browser tab, a local dev process, a CI worker, an AI sandbox, or a sync-enabled client.
- The schema should travel with the app.
- Queries should behave the same way they do against server Postgres.
- Extensions should be available when the application needs them.
- Developers should not have to choose between "easy to embed" and "actually Postgres."

## How PGlite got here



- It started with a proof of concept: Stas Kelvich had shown Postgres could run in WASM, and Jarred Sumner's "PostgresLite" tweet gave the idea a public spark.
- Sam picked it up in February 2024, got it building, wrapped it as an npm package, and posted the first "got it working" announcement.
- The first version was basic: single-user mode, hacked JSON output, in-memory or filesystem-backed persistence.
- The project became useful by adding the Postgres wire protocol, parameterized queries, proper type handling, live queries, `pg_notify`, and extension support.
- Adoption followed when people could use it for real workflows: [Supabase `database.build](https://database.build/)`, [Prisma local development](https://www.prisma.io/docs/postgres/database/local-development), CI, AI sandboxes, local-first apps, and sync with Electric.
- The next phase is about making embedded Postgres broader: more extensions, true multi-connection work, replication, and eventually `libpglite`.

## The first hack: Postgres in WASM



- In January 2024, Jarred Sumner asked when "PostgresLite" would become a thing.
- Electric had already been thinking about a related problem: syncing Postgres on the server into a local database on the client.
- The hard part was fidelity. Postgres has rich types, strict semantics, and extensions; translating that into a different local database creates friction.
- Nikita Shamgunov, then advising Electric, pointed the team at an earlier Neon proof of concept by Stas Kelvich: Postgres compiled to WASM.
- Stas had cracked the first problem: getting Postgres running in single-user mode inside WASM.
- Sam picked it up in February 2024, spent a few long evenings getting it to build, and wrapped it into an npm package.
- The first release was tiny and rough: Postgres single-user mode, JSON output hacked out of the CLI path, persistence through Node/Bun filesystems or IndexedDB in the browser.
- It was enough to show that the idea had legs.

## Making it feel like Postgres



- The first version could run queries, but it did not yet feel like Postgres from JavaScript.
- Adding the Postgres wire protocol changed that: parameterized queries, type metadata, protocol behavior, and the developer experience people expect from Postgres clients.
- Refactoring the main loop removed the need for `Asyncify` on the hot path and made query execution much faster.
- `pg_notify` unlocked local live queries: SQL queries that could re-run reactively when underlying tables changed.
- Extension support brought PGlite closer to the real Postgres platform, starting with contrib extensions and `pgvector`, then expanding toward PostGIS and community-built extensions.
- Later architecture work reduced the amount of custom Postgres code PGlite has to carry, making upstream Postgres upgrades and future ports more realistic.

### Basic usage



```typescript
import { PGlite } from '@electric-sql/pglite'

const db = new PGlite()
await db.exec('CREATE TABLE test (id serial PRIMARY KEY, name text)')
await db.exec("INSERT INTO test (name) VALUES ('hello')")
const result = await db.query('SELECT * FROM test')
```

### PostGIS in PGlite



```bash
npm install @electric-sql/pglite-postgis
```

```typescript
import { PGlite } from '@electric-sql/pglite'
import { postgis } from '@electric-sql/pglite-postgis'

const pg = new PGlite({
  extensions: {
    postgis,
  },
})

await pg.exec('CREATE EXTENSION IF NOT EXISTS postgis;')
```

## The community made it real



- PGlite started as an Electric experiment, but adoption came from people trying it in real workflows.
- Supabase used it for `[database.build](https://database.build/)`, an AI database design tool that runs locally in the browser.
- [Prisma](https://www.prisma.io/docs/postgres/database/local-development) bundled PGlite into its CLI for local development, bringing it to a much wider developer audience.
- Electric built a sync adapter so remote Postgres data can sync into local PGlite while preserving the Postgres data model.
- Community contributors helped bring extensions such as Apache AGE, `pg_uuidv7`, `pgTAP`, `pg_hashids`, `pgcrypto`, and PostGIS to PGlite.
- The project became more interesting each time someone used it somewhere we had not expected.

## What 10 million downloads looks like



- PGlite passed 1 million weekly downloads a little over a year ago.
- It has now reached 10 million weekly npm downloads.
- That growth came from several directions at once: developer CLIs, CI pipelines, browser apps, AI sandboxes, local-first apps, and sync use cases.
- Prisma helped bring PGlite into everyday local development flows.
- Supabase `database.build` showed what browser-native Postgres can enable for AI-assisted database design.
- The number is worth celebrating because it means embedded Postgres is no longer just an interesting demo; it is now being distributed through real tools people use.
- The best part is still the project stories: people using PGlite in ways that stretch what "local Postgres" can mean.

## Where PGlite goes next



- The next phase is about making PGlite feel less like "Postgres squeezed into WASM" and more like embedded Postgres as a platform.
- More extensions are a major part of that: Postgres is powerful because of its extension ecosystem, and PGlite needs to make extension building and porting easier.
- True multi-connection support remains a north star. PGlite currently works around Postgres single-user mode; future work is exploring multi-instance and multi-threaded approaches.
- Replication is another frontier: logical replication into and out of PGlite would make it a more powerful participant in Postgres systems, not just a local runtime.
- `libpglite` is the longer-term ambition: a native embeddable Postgres library for mobile, desktop, and non-JavaScript environments.
- The goal is embeddable Postgres that is as broadly adoptable as SQLite and brings Postgres semantics, tooling, and extensions with it.

## Try it, build with it, tell us what you make



- Try PGlite with `npm install @electric-sql/pglite`.
- Star the project on [GitHub](https://github.com/electric-sql/pglite/).
- Join the [Discord](https://discord.gg/pVASdMED).
- Tell us what you have built, or what you want to build, with embedded Postgres.
- Thank you to everyone who helped get PGlite here: users, contributors, extension authors, maintainers, and the teams that bet on it early.

---

