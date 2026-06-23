---
title: 'PGlite reaches 10 million weekly downloads'
description: >-
  PGlite has reached 10 million weekly npm downloads. This post looks back at
  how Postgres in WASM became a widely adopted embedded Postgres project, and
  where PGlite is going next.
excerpt: >-
  PGlite has reached 10 million weekly npm downloads. Here's how a small
  Postgres-in-WASM experiment became a widely used embedded Postgres project,
  and where it goes next.
authors: [samwillis, tdrz]
image: /img/blog/pglite-reaches-10-million-weekly-downloads/header.jpg
tags: [PGlite, Postgres]
outline: [2, 3]
post: true
published: false
---

PGlite has reached 10&nbsp;million weekly npm downloads. Thank you to everyone who has tried it, built with it, opened issues, contributed extensions, or told us where it broke.

We want to mark the milestone by looking back at how a small Postgres-in-WASM experiment became a widely used embedded Postgres project, and by sharing where we want to take it next.

> [!WARNING] 🪧&nbsp; Quicklinks
> [PGlite](https://pglite.dev/) is Postgres compiled to WASM, packaged for JavaScript environments including browsers, Node.js, Bun, and Deno.
>
> - [PGlite docs](https://pglite.dev/)
> - [GitHub](https://github.com/electric-sql/pglite/)
> - [Discord](https://discord.gg/pVASdMED)

## Postgres is showing up in smaller places

Developers increasingly need Postgres in places that used to be too small, too temporary, or too user-controlled for a database server.

AI sandboxes need a database inside the runtime so generated apps can run immediately. CI pipelines need fast, isolated databases that can be created, reset, and thrown away cheaply. Local-first apps need durable local storage with rich query semantics. Sync systems need a local target that can preserve the same schema, types, and behavior as remote Postgres.

These use cases look different, but they point at the same need: real Postgres closer to the application.

## The closer you get to production, the less glue you need

The teams that benefit are the ones that can keep development, testing, local state, and production on the same database model.

That means less translation between environments. Fewer alternate schemas. Fewer test fixtures that only exist because the local database behaves differently. Fewer bugs where a query works in one place and fails in another.

It also means developers can use Postgres features earlier: types, indexes, constraints, full-text search, `pgvector`, PostGIS, and other extensions. The alternative is a familiar tradeoff: a full server setup that is powerful but heavy, or a lightweight local environment that is easy to start but does not behave like production.

## The same database model everywhere

The ideal is not a toy database in the browser. It is Postgres semantics wherever the application needs them: in a browser tab, a local dev process, a CI worker, an AI sandbox, or a sync-enabled client.

The schema should travel with the app. Queries should behave the same way they do against server Postgres. Extensions should be available when the application needs them. Developers should not have to choose between "easy to embed" and "actually Postgres."

That is the idea behind PGlite.

## How PGlite got here

PGlite started with a proof of concept. Stas Kelvich at Neon had shown that Postgres could run in WASM. In January 2024, Jarred Sumner asked publicly when "PostgresLite" would become a thing. That tweet gave the idea a public spark.

At Electric, we were already thinking about a related problem. We were syncing Postgres on the server into local databases on clients, and the hard part was fidelity. Postgres has rich types, strict semantics, extensions, and a lot of behavior that developers rely on. Translating that into a different local database creates friction.

So Sam picked up Stas's proof of concept in February 2024, got it building, wrapped it as an npm package, and posted the first "got it working" announcement. The first version was basic: Postgres single-user mode, hacked JSON output, in-memory or filesystem-backed persistence. But it was enough to show that the idea had legs.

From there, the work became about making it useful. PGlite gained the Postgres wire protocol, parameterized queries, proper type handling, live queries, `pg_notify`, and extension support. Adoption followed when people could use it for real workflows: [Supabase `database.build`](https://database.build/), [Prisma local development](https://www.prisma.io/docs/postgres/database/local-development), CI, AI sandboxes, local-first apps, and sync with Electric.

## The first hack: Postgres in WASM

The first milestone was getting from "Postgres can technically run in WASM" to "you can install this package and build something with it."

Stas had cracked the first problem by getting Postgres running in single-user mode inside WASM. That mattered because Postgres normally uses a multi-process architecture: a postmaster accepts connections and forks backend processes to handle them. WASM does not map cleanly to that model.

Single-user mode gave us a path through that. It runs Postgres as a single process, which made it possible to package a working database into a JavaScript environment. The first PGlite release used that path. It was rough, but it was real: import PGlite, create an instance, run a query, and get Postgres inside Node.js, Bun, or the browser.

## Making it feel like Postgres

The early version could run queries, but it did not yet feel like Postgres from JavaScript.

The wire protocol changed that. It brought parameterized queries, type metadata, protocol behavior, and the developer experience people expect from Postgres clients. Refactoring the main loop removed `Asyncify` from the hot path and made query execution much faster.

Then came features that made PGlite more than a query runner. `pg_notify` unlocked local live queries: SQL queries that can re-run reactively when underlying tables change. Extension support brought PGlite closer to the real Postgres platform, starting with contrib extensions and `pgvector`, then expanding toward PostGIS and community-built extensions.

Later architecture work reduced the amount of custom Postgres code PGlite has to carry. That makes upstream Postgres upgrades easier, helps contributors understand the codebase, and sets the project up for future ports.

### Basic usage

Install PGlite:

```bash
npm install @electric-sql/pglite
```

Create a database and run a query:

```typescript
import { PGlite } from '@electric-sql/pglite'

const db = new PGlite()
await db.exec('CREATE TABLE test (id serial PRIMARY KEY, name text)')
await db.exec("INSERT INTO test (name) VALUES ('hello')")
const result = await db.query('SELECT * FROM test')
```

### PostGIS in PGlite

PostGIS was one of the most requested extensions. You can install it as a PGlite extension package:

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

PGlite started as an Electric experiment, but it became real when people pulled it into workflows we could not have planned alone.

Supabase used it for [`database.build`](https://database.build/), an AI database design tool that runs locally in the browser. [Prisma](https://www.prisma.io/docs/postgres/database/local-development) bundled PGlite into its CLI for local development, bringing it to a much wider developer audience.

Electric built a sync adapter so remote Postgres data can sync into local PGlite while preserving the Postgres data model. Community contributors helped bring extensions such as Apache AGE, `pg_uuidv7`, `pgTAP`, `pg_hashids`, `pgcrypto`, and PostGIS to PGlite.

The project became more interesting each time someone used it somewhere we had not expected.

## What 10 million downloads looks like

PGlite passed 1&nbsp;million weekly downloads a little over a year ago. It has now reached 10&nbsp;million weekly npm downloads.

That growth came from several directions at once: developer CLIs, CI pipelines, browser apps, AI sandboxes, local-first apps, and sync use cases. Prisma helped bring PGlite into everyday local development flows. Supabase `database.build` showed what browser-native Postgres can enable for AI-assisted database design.

The number is worth celebrating because it means embedded Postgres is no longer just an interesting demo. It is being distributed through real tools people use. The best part is still the project stories: people using PGlite in ways that stretch what "local Postgres" can mean.

## Where PGlite goes next

The next phase is about making PGlite feel less like "Postgres squeezed into WASM" and more like embedded Postgres as a platform.

More extensions are a major part of that. Postgres is powerful because of its extension ecosystem, and PGlite needs to make extension building and porting easier.

True multi-connection support remains a north star. PGlite currently works around Postgres single-user mode; future work is exploring multi-instance and multi-threaded approaches. Replication is another frontier: logical replication into and out of PGlite would make it a more powerful participant in Postgres systems, not just a local runtime.

`libpglite` is the longer-term ambition: a native embeddable Postgres library for mobile, desktop, and non-JavaScript environments. The goal is embeddable Postgres that is as broadly adoptable as SQLite and brings Postgres semantics, tooling, and extensions with it.

## Try it, build with it, tell us what you make

Try PGlite with `npm install @electric-sql/pglite`, star the project on [GitHub](https://github.com/electric-sql/pglite/), and join the [Discord](https://discord.gg/pVASdMED).

Most of all, tell us what you have built, or what you want to build, with embedded Postgres. Thank you to everyone who helped get PGlite here: users, contributors, extension authors, maintainers, and the teams that bet on it early.
