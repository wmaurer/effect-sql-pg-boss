# Problem: `@effect/sql-pg` and pg-boss in one transaction

## Context

An application keeps its own tables and a pg-boss job queue in one Postgres database. It writes its
own SQL with `@effect/sql` and `@effect/sql-pg` (tagged templates, `SqlSchema`, `Migrator`,
`SqlClient.withTransaction`). It needs two operations that span both:

1. **Enqueue inside the application's transaction.** One unit of work inserts rows into the
   application's tables and enqueues a pg-boss flow, for example a `parent` job and a dependent
   `child` job. All of it must commit or roll back together. pg-boss supports this through its `db`
   option, `boss.flow(jobs, { db })`. Here `db` is any object with
   `executeSql(text: string, values?: unknown[]): Promise<{ rows: any[] }>`. The application wants
   `db` to run on the connection of the current `@effect/sql` transaction.
2. **Write inside pg-boss's transaction.** pg-boss 12 has transactional workers,
   `boss.work("jobs", { transactional: true }, (jobs, tx) => …)`. pg-boss opens the transaction,
   the handler writes through `tx`, and those writes commit together with the job's completion. The
   application wants to write its own tables through `@effect/sql` inside that transaction.

This note asks whether `@effect/sql` can support both.

Versions: `effect` / `@effect/sql-pg` 4.0.5, pg-boss 12.38.0, `pg` 8.23.1.

## What blocks it

### 1. `flow({ db })` sends a parameter-less, multi-statement string

`pg-boss/dist/manager.js:1793-1843` builds the flow as several statements. There is one guarded
`INSERT` per queue, the `job_dependency` insert, and an optional `pg_notify`. When the caller
passes `db`, pg-boss joins them with `';\n'` and calls `db.executeSql(sql)` once. It passes no
`values` and sends no `BEGIN` or `COMMIT`, because the caller owns the transaction:

```js
const db = options.db ?? this.db;
const sql = options.db ? statements.join(';\n') : plans.transaction(statements);
await db.executeSql(sql);
```

node-postgres runs a query without values on the **simple query protocol** (the `Query` message).
Postgres accepts several statements in one `Query` message.

`@effect/sql-pg` 4.0.5 does not wrap node-postgres. It implements the wire protocol itself, in
`packages/sql/pg/src/PgProtocol.ts` and `packages/sql/pg/src/PgConnection.ts`. `encodeQuery`
(`PgConnection.ts`, around line 1084) encodes every query as a `Parse` / `Bind` / `Execute` plan.
The plan is named and cached when `prepare` is on. It is unnamed (`encodeUnnamed`) when `prepare`
is off. `executeUnprepared` and `sql.unsafe` with no params also go through `Parse`. There appears
to be no encoder for the simple-protocol `Query` message.

Postgres rejects a `Parse` whose text holds more than one command, with the error
`cannot insert multiple commands into a prepared statement`. So an `IDatabase` adapter over an
`@effect/sql-pg` transaction connection cannot run `flow`.

**To verify first.** Confirm that `executeUnprepared` and `prepare: false` still send `Parse`, and
that Postgres returns the error above. The repro sketch is at the end.

### 2. Transactional workers hand over a node-postgres connection

The `tx` that a transactional worker receives is pg-boss's `beginTransaction()` handle
(`pg-boss/dist/db.js:300-345`). It wraps a checked-out `pg` client on which pg-boss has already
sent `BEGIN`. To write the application's tables through `@effect/sql` inside that transaction,
`@effect/sql` would have to run statements on a connection it did not open. It must not commit,
roll back, or release that connection.

### 3. Two pools

Suppose blockers 1 and 2 are solved another way. `@effect/sql-pg` and pg-boss still each open their
own pool to the same database. pg-boss accepts a custom `db: IDatabase` in its constructor. That
adapter must then implement the optional `beginTransaction()` for transactional workers, and the
optional `listen()` for `useListenNotify`. `pg-boss/dist/types.d.ts:18-59` defines the `IDatabase`
contract.

## Directions to investigate

- **A simple-query path in `@effect/sql-pg`.** For example, `executeRaw` or `unsafe` with no params
  could send a `Query` message. Another option is an explicit `sql.simple(text)` or `executeMulti`.
  With that path, an `IDatabase` adapter over the current fiber's transaction connection is small:
  `executeSql: (text, values) => runPromise(values?.length ? sql.unsafe(text, values) : sql.simple(text))`.
  The adapter runs with the fiber's context, so it uses the `withTransaction` connection.
  Open questions: does upstream want this, and does `multiplex` mode or pipelining make a simple
  query on a pinned transaction connection awkward?
- **Splitting pg-boss's SQL in the adapter.** pg-boss joins the statements with `';\n'`. It inlines
  job `data` into the text as a literal, `'${JSON.stringify(value)}'` with single quotes doubled
  (`serializeJsonParam`, `pg-boss/dist/plans.js:4523`). `JSON.stringify` escapes newlines, so job
  data cannot produce a literal `;\n`. Two risks remain. pg-boss's own SQL templates may contain
  `;\n`, and the split depends on a pg-boss internal that may change in any release. This is
  probably a dead end, but it is cheap to check.
- **An `@effect/sql` client over an existing node-postgres connection.** Is there, or could there
  be, a `SqlClient` implementation that wraps a `pg` `Pool` or `PoolClient`, as `@effect/sql-pg` did
  in Effect v3? Then pg-boss's `getDb()` pool, or the `tx` of a transactional worker, could back a
  `SqlClient` for the length of one handler. `@effect/sql` templates and `SqlSchema` would work on
  top of it. Check how `SqlClient.make` (`packages/effect/src/sql/SqlClient.ts`) takes an
  `acquirer` and a `transactionAcquirer`. Check whether a borrowed-connection mode, which never
  commits or releases, fits that shape.
- **Upstream pg-boss.** Would pg-boss accept sending the flow as separate `executeSql` calls when
  the caller passes its own `db`? Each statement already stands alone, and the caller owns the
  transaction, so atomicity does not change.

## Repro sketch

```ts
import { PgClient } from "@effect/sql-pg"
import { Effect } from "effect"
import { SqlClient } from "effect/sql"

const program = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  yield* sql.withTransaction(
    sql.unsafe("select 1; select 2")
  )
})
// Expect: SqlError wrapping "cannot insert multiple commands into a prepared statement".
// Then try the same through executeUnprepared / prepare: false.
```

Then run the real case. Start pg-boss 12.38.0 against the same database and create the queues
`parent` and `child`. Call `boss.flow([...], { db: adapter })`, where `adapter.executeSql` runs the
text on the current `withTransaction` connection.

## Done when

There is a yes or no, with evidence, for this question. Can an application use `@effect/sql` for
its own tables and still (1) enqueue a pg-boss flow inside the same `@effect/sql` transaction and
(2) write through `@effect/sql` inside a pg-boss transactional worker's transaction? If yes, name
the smallest change that makes it work, and say whether it belongs in `@effect/sql-pg`, in a new
client, or in pg-boss.
