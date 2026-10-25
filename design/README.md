# How effect-sql-pg-boss works, and why

This document explains the design of `@wmaurer/effect-sql-pg-boss`: the problem it solves, what the investigation found, how the library works inside, and the decisions behind the API. For how to use the library, read the [package README](../packages/effect-sql-pg-boss/README.md).

The design was made in a session on 2026-10-20. That session produced a problem statement, investigation experiments, three competing API sketches with a judge's rubric, and a prototype of the chosen sketch. They are not in the working tree any more. The tag `design-record` points at the last commit that has them:

```sh
git show design-record:design/README.md
git checkout design-record -- design/
```

## The goal

An application keeps its own tables and its pg-boss queues in one Postgres database. It uses the Effect SQL stack (`sql` templates, `SqlSchema`, `Model`, `Migrator`, `withTransaction`) on the native `@effect/sql-pg` client. It needs two things:

1. Enqueue pg-boss jobs and flows inside `sql.withTransaction`, so they commit or roll back with the app's rows.
2. Write app tables through `effect/sql` inside a pg-boss transactional worker (`boss.work(q, { transactional: true }, ...)`), so the writes commit together with the job's completion.

## What the investigation found

The answer is yes for both, with a small library of your own and no change in Effect or pg-boss.

- **The native client cannot run multi-statement SQL.** Every query path in `@effect/sql-pg` sends a `Parse` message. There is no encoder for the simple-query message. An experiment tried 9 query modes, including `prepare: false`, `.unprepared` and `multiplex: true`. All failed with `cannot insert multiple commands into a prepared statement`. This is deliberate. Effect PR #7426 (commit `534b8b9dba`, 2026-08-25) replaced node-postgres with the native client. Its changeset lists "Query strings must contain one statement" as a breaking change, and it removed `fromPool`, `fromClient` and `makeWith`.
- **Only `boss.flow` needs multi-statement SQL on a `{ db }` path.** Every other pg-boss API that takes `{ db }` sends one statement with parameters, and all of them work on the native client. `flow` joins its statements with `";\n"` and sends them without parameters (`pg-boss/dist/manager.js:1840-1843`).
- **pg-boss's own constructor `db` must run multi-statement SQL.** Migrations, maintenance and BAM need it, so pg-boss's own statements must run on node-postgres.
- **A pg-boss worker's `tx` is only `{ executeSql }`.** It does not expose the node-postgres client. pg-boss fences a handler's settle calls by the identity of that object, so app code must pass the exact `tx` object to pg-boss.
- **Joining an external transaction by hand is broken in Effect.** If you provide `sql.transactionService` yourself, a nested `withTransaction` dies with `Service not found: effect/sql/SqlClient/TransactionSemaphore/<n>` (`packages/effect/src/sql/SqlClient.ts:676`).
- **The parked transaction works.** This was the key finding of the design phase. pg-boss lets a custom constructor `db` implement `beginTransaction()`. The library's `beginTransaction()` forks a fiber that runs a real `sql.withTransaction` on the app's native client and waits on a `Deferred`. pg-boss's `commit()` and `rollback()` settle the `Deferred`. The worker transaction is therefore a normal Effect transaction. Nested `withTransaction` calls become savepoints, `afterCommit` runs after the real COMMIT, and rows decode the native way.

The experiments ran against a real Postgres with pg-boss 12.38.0:

| Experiment                                                            | Result                                                                    |
| --------------------------------------------------------------------- | ------------------------------------------------------------------------- |
| Multi-statement text through native `PgClient`                        | Fails in all 9 modes.                                                     |
| `boss.flow` with a direct adapter on the `withTransaction` connection | Fails with the same error.                                                |
| An adapter that splits `flow`'s text on `";\n"`                       | Commit and rollback are atomic.                                           |
| A `SqlClient.make` over the worker's `tx.executeSql`, with savepoints | Works, but `afterCommit` fires before the real COMMIT. Rejected later.    |
| A `SqlClient.make` over a node-postgres pool                          | `flow` works without a split. Rejected later because it changes decoding. |

## Decisions

| Decision            | Choice                                                                                             | Reason                                                                                                                                                                                                                                                       |
| ------------------- | -------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Package name        | `@wmaurer/effect-sql-pg-boss`                                                                      | It follows Effect v3's `@effect/sql-drizzle` and `@effect/sql-kysely` ("X integration for @effect/sql"). It still fits if the node-postgres parts go away. It is different from `@shivaedev/effect-pg-boss`, which deliberately does not share transactions. |
| Packaging           | One package. `pg-boss`, `effect` and `@effect/sql-pg` are peer dependencies. `pg` is a dependency. | Nothing else needs to be split out today.                                                                                                                                                                                                                    |
| App's SQL client    | The native `@effect/sql-pg` `PgClient`                                                             | The app already uses it. Switching to node-postgres would change every row decode in the app (`int8` from `bigint` to string, timestamps from epoch ms to `Date`).                                                                                           |
| Worker transactions | The parked transaction                                                                             | Real Effect transaction semantics inside the worker.                                                                                                                                                                                                         |
| Expected lifetime   | Permanent userland package                                                                         | Effect removed the node-postgres client on purpose, and v4 dropped the drizzle and kysely integrations.                                                                                                                                                      |

## How it works inside

The modules are in `packages/effect-sql-pg-boss/src/`.

- **One routing rule.** `internal/connection.ts` derives the pg-boss `Db` for a call from the fiber's current Effect transaction connection. It caches the `Db` in a `WeakMap<Connection, Db>`. The `Db` calls `conn.executeUnprepared(text, values ?? [], undefined)` directly, not `sql.unsafe`. So pg-boss gets raw rows even when the app sets `transformResultNames`. Outside a transaction, `send` and `flow` open their own `withTransaction`.
- **The `flow` split.** A call with `values === undefined` is a multi-statement script, and only `flow` sends one on a `{ db }` path. The library splits it on `";\n"` and runs each part on the same connection. This is the only part that depends on a pg-boss internal. It is the reason for the `~12.38` peer pin.
- **Errors.** The `Db` rejects with the driver's own `Error`, so pg-boss still sees the SQLSTATE `code`, `constraint` and `detail`. A `WeakMap` maps that error back to the original `SqlError`, so the caller's Effect fails with the typed error.
- **The parked worker transaction** (`internal/workerTransaction.ts`).
    1. `beginTransaction()` forks `sql.withTransaction(body)` into the `PgBoss` layer's `FiberSet`.
    2. The body opens the transaction and hands pg-boss the handle.
    3. The handler runs as a child fiber of the parked fiber. The parked transaction's services win over the worker layer's services.
    4. `rollback()` interrupts the handler before the ROLLBACK runs, so no handler statement can run after the connection goes back to the pool.
    5. Before it hands pg-boss the handle, the body registers its own `sql.afterCommit` callback, the committed signal. It is the transaction's first callback, so it runs first, right after the real COMMIT and the connection release. `commit()` resolves on that signal and does not wait for the app's own `afterCommit` callbacks. If the parked fiber fails first, as when COMMIT fails, `commit()` rejects with the driver error.
    6. `rollback()` is idempotent, never rejects, and gives up after `rollbackTimeout`.
    7. After the transaction settles, its `Db` rejects every statement.
- **The maintenance pool** (`internal/housekeeping.ts`). This is a small node-postgres pool built from `PgClient.config`. It runs pg-boss's own statements: migrations, maintenance, BAM, fetch, and settles outside a transaction. App rows never use it. It has `'error'` listeners on the pool and on each checked-out client.
- **The listener** (`internal/listener.ts`). With `useListenNotify` on, it opens one `PgConnection` of its own for `LISTEN`. Decision 5 below explains why.
- **Pool headroom** (`internal/runtime.ts`). Each transactional worker layer reserves `localConcurrency` connections from a budget. The layer fails to build when fewer than `connectionReserve` connections would stay free.
- **Lifecycle.** The order on release is: `boss.stop({ graceful: true, close: false })`, then the `FiberSet` interrupts leftover worker transactions (they roll back), then the maintenance pool ends. A worker layer releases before `PgBoss.layer` and before any layer built after it, such as an app service its handler uses. On release it calls `offWork(name, { id, wait: true })`, so it returns only after pg-boss has settled the batch in flight, and every service in the handler's context outlives the handler. That wait is bounded by the stop timeout, which the internal `Runtime` carries as `stopTimeout` (0 when the stop is not graceful). Past it, the worker interrupts its own handlers, which run in a `FiberSet` the worker owns, and waits for `offWork` again. A transactional handler is a child of its parked fiber, so the worker's fiber interrupts that child and waits for it. pg-boss then rolls back and fails the batch as a handler failure, with the interruption as its error. With `wait: false` the release returned at once, and the services provided to the worker closed while its handler still ran.

## What the tests prove

The design prototype ran these checks once, on Postgres 17.11 and 18.6. The package's suite now runs them through the queue classes against Postgres 18 (`pnpm test`).

`test/workerTransaction.test.ts` runs P1 to P4, and `test/connection.test.ts` runs P5:

| Check                   | What it shows                                                                                                                                                                       |
| ----------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| P1. Commit and failure  | The commit path keeps the app rows, a nested savepoint row and a follow-up job, and the job ends `completed`. The failure path leaves nothing, and an app `afterCommit` never runs. |
| P2. Abandoned handler   | A handler in `pg_sleep` is interrupted when pg-boss calls `rollback()`. No handler statement runs after the ROLLBACK, and the connection is reusable.                               |
| P2b. Bounded rollback   | With an uninterruptible handler, `rollback()` resolves after `rollbackTimeout`. The handler's later insert never runs.                                                              |
| P3. Commit timing       | `commit()` resolves within 1 s and at least 2.5 s before the app's 3 s `afterCommit` finishes.                                                                                      |
| P4. Callback order      | `commit()` has already resolved 50 ms into the app's first `afterCommit`.                                                                                                           |
| P5. Raw rows and errors | With camelCase `transformResultNames`, pg-boss gets `created_on`. A unique violation reaches pg-boss as `code=23505` and the Effect as `SqlError/UniqueViolation`.                  |

Resolving `commit()` on the parked fiber's exit instead fails P3 (3053 ms) and P4, and registering the committed signal after the handler fails P3.

`test/layer.test.ts` runs the paths the prototype did not:

- `PgBoss.use` gets `{}` outside a transaction and the transaction's `db` inside one. A `send` through it commits and rolls back with the app's rows.
- The headroom budget comes from config. `maxConnections: 5` with `connectionReserve: 2` gives 3, and an unset pool with the default reserve gives 8.
- A COMMIT that fails on a deferred unique constraint rolls back the handler's rows. The job ends `failed`, and its stored error carries `code: "23505"`.
- Closing the layer while a handler sleeps past the stop timeout takes that timeout and little more (2 s configured, about 2020 ms measured), in both modes. The worker interrupts the handler, and the job ends `failed` with the interruption as its error. A transactional handler's row rolls back, and a non-transactional handler's row stays. No connection of either pool remains.
- A handler outlives the services provided to its worker. It sleeps 1.5 s while the layer closes, still sees the app's service open, and its job ends `completed`, in both modes.
- Closing a worker layer while `PgBoss.layer` stays up waits for its handler to finish, and the job ends `completed`, in both modes.
- A worker transaction that cannot start fails the job without running the handler. The test forces it with a `PgClient` whose startup parameters the server rejects (`22023`), so the failure is at connect, not at the `BEGIN` statement.
- An error on a maintenance connection reaches the app's logger.

Not run yet: shutdown while a handler waits to start.

`test/connection.test.ts` and `test/jobQueue.test.ts` run the `JobQueue` paths:

- The flow script pg-boss sends on a caller's `db` is one `executeSql` call without values, of three statements split on `";\n"`. A pg-boss upgrade that changes the count or the separator fails this test.
- A flow keeps a caller-chosen id on its parent step, and the app stores that id in its own row before the commit.
- A flow whose second statement fails rolls back the app's row and the first statement. The Effect fails with `PgBossError`, because pg-boss wraps the driver error (22012) in its own.
- A queue layer updates `retryLimit` from 1 to 5 on the next build, and fails to build when the stored policy differs from the declared one.
- With `onDecodeError: "drop"`, a batch of three with one malformed payload completes all three, and the handler sees the two good ones. With `"fail"`, all three fail and the handler never runs. A batch where nothing decodes completes without running the handler.
- A job that fails past its retries reaches the dead-letter queue. Its handler sees the original payload and the failed job's id, queue, output and retry count.

`test/listener.test.ts` and `test/listenNotify.test.ts` run `useListenNotify`, with the numbers in decision 5. `test/usage.ts` holds the README's call sites and the mistakes that must not compile, each with a `@ts-expect-error`. `pnpm typecheck` checks it, and vitest never runs it.

## Versions

- **pg-boss 12.38.0.** This was the latest release on npm (published 2026-10-13, checked on 2026-10-20), and pg-boss master has the same `flow` code. The peer range is `~12.38` because the `flow` split depends on how pg-boss joins statements, and 12.38 is the only version tested at runtime. pg-boss 12.37.1 has the same join (`statements.join(';\n')` at `dist/manager.js:1686`; source checked, not run). So the range could probably widen to `>=12.37.1 <12.39` after one test run on 12.37.1.
- **Postgres.** Nothing in the design depends on a Postgres version. The prototype passed on 17.11 and 18.6, and the suite runs on 18. pg-boss requires PostgreSQL 13 or higher.
- **effect and @effect/sql-pg 4.0.6.** The package's peer range is `^4.0.6`. `SqlClient.afterCommit`, which the committed signal needs, arrived in Effect PR #9289, after the 4.0.5 tag. The package first shipped on 4.0.5 without the committed signal, with P1's `afterCommit` check, P3 and P4 deferred. Issue #1 tracked their return, which came with the move to 4.0.6.
- **pg 8.23.1.** This version is used for the maintenance pool and in pg-boss.

## Rejected alternatives

Three agents sketched the API independently, and a judge on a different model scored them. The library is the third sketch's native-client direction with the first sketch's queue API, plus the judge's required fixes: the handler as a child of the parked fiber, the committed signal, the bounded rollback, the released guard, the headroom check, and the `'error'` listeners.

- **A node-postgres client as the app's main client** (sketches 1 and 2). One object would be both the Effect `Connection` and the pg-boss `Db`, so `tx` identity would hold by construction. The judge preferred it. It was rejected because it changes the app's decoders and adds a permanent driver client of 300 to 800 lines. Choose it only if one pool and no `flow` split matter more to you than native decoding.
- **A borrowed per-batch `SqlClient` in workers** (sketch 2). Repositories built at layer time write outside the transaction, and `afterCommit` fires before the real COMMIT. Two of its ideas were kept: the bounded rollback race, and eager queue creation at layer build.
- **Providing `transactionService` as `[conn, 0]` by hand.** It dies at `SqlClient.ts:676`, and `afterCommit` runs immediately.
- **Lazy queue creation on the first send** (sketch 3). DDL on another connection can wait on locks that the caller's own transaction holds.
- **Splitting every multi-statement pg-boss script on `;`.** pg-boss's `dist/plans.js` has 76 `$$` dollar-quote markers around function bodies that contain `;`, so the split would corrupt them.
- **A full Effect wrapper over about 80 pg-boss methods.** `PgBoss.use` covers them.

## API decisions

1. **Transactional workers are not supported on a `PgClient.makeClient` client, and `PgBoss.layer` does not refuse them.** `PgClient.make` and `PgClient.makeClient` both store the raw options as `config` (`PgClient.ts:186`, `:212`), so nothing public tells the two apart. The headroom check reads `config.maxConnections` through one cast and falls back to 10, the `PgPool` default (`PgPool.ts:162`), so the budget is exact for every pooled client. On `makeClient` a transactional batch would pin the only connection for the handler's run (`PgClient.ts:209`) and stall every other query of the app, and the docs say so.
2. **`transactional` is required on every `worker` call.** The two modes differ in what they hold (a connection and an open transaction for the whole handler run) and in what commits together, and the right one depends on the handler. pg-boss defaults to `false` (`src/attorney.ts:480`), so a `true` default would silently diverge from it. A required field ties the headroom build failure to a visible choice, at the cost of one field per worker.
3. **A worker chooses with `onDecodeError: "fail" | "drop"`, and `"fail"` is the default.** A best-effort queue must not let one malformed message retry a whole batch, and a queue with a retry policy must treat a decode failure like any other failure. `"deadletter"` is not offered: it needs `perJobResults`, which pg-boss refuses together with `transactional`.
4. **A queue layer always writes the options it declares with `updateQueue`.** A declarative layer should bring the stored queue in line with its declaration, and `updateQueue` is idempotent. An option removed from the declaration keeps its stored value, and a changed `policy` or `partition` fails the build, because pg-boss cannot change either.
5. **`useListenNotify` works, on a connection the library owns instead of through `PgClient.listen`.** With the option on, `PgBoss.layer` gives pg-boss a `listen` (`internal/listener.ts`) that opens one `PgConnection` from `PgClient.config`, pins it, and runs pg-boss's own heartbeat, a `pg_listening_channels()` query, on that session every 10 s. The listener logs a lost session and replaces it with pg-boss's backoff, and each new subscribe makes every notify-queue worker fetch once. `PgClient.listen` cannot carry this. It hides the session it pins, so nothing can run the heartbeat there, and its scope sends `UNLISTEN` before the socket closes. On a silently dead socket that `UNLISTEN` did not return within 8 s, and `boss.stop()` waits for it. The library's session closes its socket first. Closing the layer over a frozen socket took 4 ms, and with the order swapped the frozen-socket test in `test/listenNotify.test.ts` fails at its 8 s limit. In the same suite, a worker that polls every 30 s ran a job about 50 ms after its send, and a job sent in the gap after `pg_terminate_backend` ran about 1 s later. In `test/listener.test.ts` the heartbeat replaced a frozen session 6.6 s after the freeze with a 300 ms interval and timeout. With the defaults it took 21 s in one run. The heartbeat's timeout also bounds the wait for PostgreSQL to confirm each LISTEN, so a socket that goes silent between connect and that reply fails a first subscribe, which pg-boss treats as unavailable and polls, or counts as a lost session on a reconnect. Through a proxy that froze the LISTEN, the first subscribe rejected 5.3 s after the freeze, the driver's 5 s abort drain included, and a reconnect came back 8.4 s after the loss. The connection is outside the PgClient pool, the maintenance connections and the `connectionReserve` budget, so the listener never waits for an app connection and also works on a `makeClient` client. An app that does not set the option opens no connection.
6. **Deferred: handlers do not store a typed result or error in the job's output, because no consumer needs it.** In bgs-videostreaming the media service is the only package on this library. Its `transcode-succeeded` handler reads `result.json` from S3, not the finished job's output, because pg-boss archives and deletes completed jobs. Its `transcode-failed` handler decodes `DeadLetterJob.source.output` with the contract's own schema, which works today. The transcoder does return typed results and failures, but it uses pg-boss directly. Reopen this if the transcoder moves onto the library. What it needs is `perJobResults`, so that each job can return `failed` or `deadletter` with a typed output. pg-boss refuses `perJobResults` together with `transactional` (decision 3), so the feature would cover non-transactional workers only.

## Upstream opportunities

None of these is needed. Each one would make the package smaller.

- **`PgClient.simple(text)` in `@effect/sql-pg`.** It would send the simple-query message, in about 150 to 250 lines of source plus tests. With it, the maintenance pool, the `flow` split, the `pg` dependency and the version pin all go away. My guess for the chance of acceptance is low to medium, because multi-statement support was removed on purpose. A similar Bun SQLite request, Effect-TS/effect#5930, was closed as not planned on 2026-10-13. Open an issue before writing a PR. Base the pitch on the convention that Bun, postgres.js, pg and pg-boss all follow: no parameters means the simple protocol.
- **A one-line fix at `packages/effect/src/sql/SqlClient.ts:676`.** It would make "join an existing transaction" work by providing `transactionService`. This design does not need it.
- **pg-boss sends `flow` as one `executeSql` call per statement when you pass `db`.** It is a two-line change at `manager.js:1840-1843`. The maintainer is responsive (#945 was fixed in a day), but a code comment says the single round trip is intended "regardless of db adapter".

## Gotchas for contributors

- Pass pg-boss the exact `tx` object for settle calls. A wrapper object looks like a lost claim, and pg-boss rolls back.
- Keep the SQLSTATE `code`, `constraint` and `detail` on errors that go back to pg-boss. pg-boss checks codes 22012, 23505, 42P01, 23514 and 25P02.
- Inside a transaction, pg-boss read APIs used through `PgBoss.use` return natively decoded rows, so timestamps arrive as epoch milliseconds. Read outside transactions.
- Each in-flight transactional batch holds one `PgClient` connection for the handler's whole run. Use `transactional: false` for slow handlers.
- A handler stuck inside an uninterruptible statement can hold its connection until the native client's cancel timeout. This was inferred from the code, not measured.
- `pg-boss`'s `TestClock` wraps `tx` (`activity.js:7-9`). It looks unreachable with this `Db`, but check it when you add tests.
