# Grounding: `@wmaurer/effect-sql-pg-boss`

Phase A output for the architect run. Every fact below was measured at runtime or read in source during this session. File:line refs are to the effect repo worktree `/home/wayne/projects/effect-ts/effect/.worktrees/feat-sql-pg-pgboss-transactions` (effect 4.0.5, HEAD 1b740862e3) unless they say `pg-boss/dist/...` (pg-boss 12.38.0) or `pg/...` (pg 8.23.1).

## The user's goal

An application keeps its own tables and a pg-boss queue in one Postgres database. It wants all of the Effect SQL stack (`sql` tagged templates, `SqlSchema`, `Model`, `Migrator`, `withTransaction`, spans, Reactivity) and pg-boss, with two cross-cutting operations:

1. **Enqueue inside the app's transaction.** Insert app rows and `boss.flow([...])` / `boss.send(...)` in one `sql.withTransaction`; all commit or roll back together.
2. **Write inside pg-boss's transaction.** `boss.work(q, { transactional: true }, handler)`: the handler writes app tables through `@effect/sql`, and those writes commit together with the job's completion.

Package name is decided: `@wmaurer/effect-sql-pg-boss`, one package, pg-boss as a peer dependency, the node-postgres `SqlClient` as a module inside it. It mirrors v3's `@effect/sql-drizzle` / `@effect/sql-kysely` ("X integration for @effect/sql"). Expect it to be permanent userland, not merged upstream. Its sibling, `/home/wayne/projects/wmaurer/effect-ai-amazon-bedrock`, is a pnpm monorepo (`packages/`, effect-tsgo, vitest, dprint, oxlint) and shows the user's packaging conventions.

A competing package `@shivaedev/effect-pg-boss` exists and deliberately does NOT share transactions ("Treat enqueueing and an application database transaction as separate operations... The package supplies no application transaction adapter").

## Hard facts

### `@effect/sql-pg` (native client) cannot run multi-statement text

- Every query path sends `Parse` (named or unnamed). No simple-protocol `Query` ('Q') encoder exists (`packages/sql/pg/src/PgProtocol.ts:340-351`, `:1086-1110`). Measured: 9 of 9 modes fail with `cannot insert multiple commands into a prepared statement`, including `prepare: false`, `.unprepared`, `.raw`, `multiplex: true`.
- Deliberate: #7426 (`534b8b9dba`, 2026-08-25) replaced `pg` with the native client and lists "Query strings must contain one statement" as a breaking change. It removed `fromPool`, `fromClient`, `makeWith`. `PgClient` can only use connections it opens itself.
- Native decoding: `int8` -> bigint, `date` -> string, timestamps -> epoch ms, `bytea` -> Uint8Array. node-postgres decodes `int8` -> string, timestamps -> Date, bytea -> Buffer. A `SqlSchema` written for one can fail on the other.
- Adding `PgClient.simple(text)` upstream is ~150-250 lines + tests; acceptance unknown (guess: low to medium).

### pg-boss 12.38.0 surface

- `IDatabase` (`pg-boss/dist/types.d.ts:18-59`): `executeSql(text, values?) => Promise<{ rows }>` required; optional `beginTransaction()`, `listen()`, `setSessionStatements`, `poolCounts`. `TransactionHandle` (`types.d.ts:70-84`): `{ db, commit(), rollback() }`; `commit` must reject on failure; `rollback` must never reject, be safe to call twice, and be time-bounded.
- Every API accepting `{ db }` (send, insert, update, upsert, publish, fetch, complete, fail, touch, cancel, resume, retry, getJobById, findJobs, deleteJob, schedule, ...) sends ONE statement with values and works on the extended protocol. Only `flow` sends multi-statement text without values (`manager.js:1840-1843`, joined with `';\n'`). `fetch` passes `values = []`.
- The constructor `db` must support multi-statement text (maintenance, migrations, BAM, `flow` without `db` uses `plans.transaction`). Docs: constructor.md lines 69-86; asserts at `bam.js:320`, `boss.js:500`. So pg-boss's own `db` must be node-postgres-like (simple protocol when no values).
- Transactional workers: `work()` asserts `this.db.beginTransaction` exists (`manager.js:1169`). Per batch: `beginTransaction()` -> `tx.db.executeSql(set_config($1..), values)` -> `handler(jobs, untracked(transaction.db))` -> on success `complete(..., { db: transaction.db })` then `commit()`; on throw `rollback()` then `fail()` on the pool (`manager.js:666-734`).
- The handler's `tx` is exactly `beginTransaction().db` (`===`, measured with a custom constructor adapter). With the built-in `Db`, `tx` is a plain `{ executeSql }` closure over a checked-out pg client (`db.js:300-379`); no client is exposed.
- **Settle fencing by identity**: pg-boss tracks a handler's settles by the `options.db` object (`manager.js:671-674, 958-963, 990, 1007`). Calling `boss.complete/fail/cancel/deleteJob` with a different wrapper over the same connection looks like a lost claim and rolls back. Effect code must pass the exact `tx` object.
- **Error fields**: pg-boss reads `err.code` (22012 flow/insert guard `manager.js:172`, 23505 `:1905`, 42P01/23514 `:1076`, 25P02 `:730`) and `err.constraint`/`err.detail` (`:180-186`). An adapter that throws must preserve these from the driver error.
- A custom constructor `db` loses `_pgbdb` special handling: pg-boss will not open/close it (`index.js:207, 277, 364`), no pool-headroom warning (`manager.js:1322`).
- Shipped adapters (`pg-boss/dist/adapters/{bun,drizzle,knex,kysely,prisma,pglite}.js`) document the convention "parameterless call = simple protocol" (`bun.js:35-39`).

### effect/sql `SqlClient` layer

- `SqlClient.make(options)` is public (`packages/effect/src/sql/SqlClient.ts:192-261`): `acquirer`, `transactionAcquirer`, `compiler`, `spanAttributes`, `transactionService?`, `beginTransaction`/`commit`/`rollback` strings (commit may be an Effect), `savepoint`/`releaseSavepoint`/`rollbackSavepoint`, `prepareTransactionControls`, `onCommitFailure`.
- `Connection` interface (`packages/effect/src/sql/SqlConnection.ts:28-65`): `execute`, `executeRaw`, `executeStream`, `executeValues`, `executeValuesUnprepared`, `executeUnprepared`.
- The fiber's transaction connection lives in the context under `sql.transactionService` as `[conn, depth]` (`SqlClient.ts:263-269, 709, 720-723`). Nested `withTransaction` uses SAVEPOINT `effect_sql_<id>`.
- **Defect**: providing `sql.transactionService` with `[conn, 0]` yourself ("join an existing transaction") makes plain statements work, but a nested `withTransaction` dies with `Service not found: effect/sql/SqlClient/TransactionSemaphore/<n>` (`SqlClient.ts:564-566, 616, 676`). `afterCommit` also runs immediately for such a level (`:504-509`). A 1-2 line upstream fix at `:676` would make this route work; do not depend on it landing.
- `PgClient.makeCompiler` is exported (`packages/sql/pg/src/PgClient.ts:417-457`), but `sql.json` compiles to a `PgTypes` parameter object (`{[ParameterTypeId], oid, value}`, `PgTypes.ts:1798-1811`) that node-postgres cannot send. A pg-based client needs its own compiler or must unwrap with `PgTypes.isParameter`. The pre-#7426 compiler (`git show 534b8b9dba^:packages/sql/pg/src/PgClient.ts`, ~837-877) passed JSON raw; its `ConnectionImpl` (~674-765) is a template for a node-postgres `Connection`.
- Thin client references: `packages/sql/pglite/src/PgliteClient.ts:205-368` (`fromClient` over a caller-owned driver, semaphore acquirer, ~70-line Connection class). Driver clients total 339-832 lines.

## Runtime prototypes (all passed, Postgres 17)

Scripts in `prototypes/investigation-runtime/` (their node_modules has pg-boss 12.38.0, pg 8.23.1, and `effect` + `@effect/sql-pg` symlinked to the repo sources; scripts run with `node file.ts`):

- `borrowed.ts`: `SqlClient.make` over `tx.executeSql`, `acquirer: Effect.succeed(conn)`, begin/commit/rollback mapped to `SAVEPOINT effect_borrowed` / `RELEASE` / `ROLLBACK TO`. E4: app rows commit/roll back with the job; effect sent no BEGIN/COMMIT.
- `e5.ts`: `SqlClient.make` over a `pg.Pool` (`acquireRelease(pool.connect, release)`), `client.query(text, values-or-undefined)`. Unmodified `boss.flow(..., { db: adapter })` inside `withTransaction`: commit -> app row + 2 jobs; rollback -> nothing.
- `e2e3.ts`: native `PgClient` + adapter splitting `flow` text on `";\n"`: commit and rollback atomic. Stopgap only (pg-boss internal). Templates `flow` uses contain no inner `";\n"`; JSON data cannot produce one.

## Known caveats any design must address or consciously accept

1. `afterCommit` / Reactivity invalidation inside a worker fires at RELEASE SAVEPOINT, before pg-boss's real COMMIT.
2. Concurrent top-level `withTransaction` on one borrowed connection would interleave savepoints; a semaphore (as `PgliteClient.fromClient`) serializes them.
3. `tx` identity must be preserved when the handler calls pg-boss settle APIs.
4. Driver errors must keep `code`/`constraint`/`detail` for pg-boss.
5. One pool vs two: pg-boss can take the same `pg.Pool` via a custom constructor `db` (that adapter then owns `beginTransaction`, optionally `listen`, and pg-boss will not close the pool).
6. Lifecycle: `boss.start()`/`stop()` as a scoped Layer; pg-boss schema install/migrations.
7. pg-boss's JS API is Promise-based and untyped job data (`unknown`/generic); Effect users want Schema-typed queues and Effect handlers.
