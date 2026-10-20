# @wmaurer/effect-sql-pg-boss: design handoff

This document holds everything from the session of 2026-10-20 that designed `@wmaurer/effect-sql-pg-boss`. You can start a new session from this file alone. All supporting files are in this folder, and the paths below are relative to it.

Status: the investigation is finished, and the API sketch is finished and verified. The sketch is ported into `packages/effect-sql-pg-boss/`. `PgBoss.layer`, `PgBoss.layerConfig` and `PgBoss.use` are implemented. The integration suite runs on `effect` 4.0.5 and builds everything through `PgBoss.layer`. `JobQueue` still has TODO bodies.

## Contents of the folder

- `problem.md` is the original problem statement.
- `arena/grounding.md` lists every fact the design rests on, with file and line references into the Effect repo and pg-boss 12.38.0.
- `arena/synthesis/` is **the chosen API sketch**. Start here.
    - `README.md` is the user-facing usage.
    - `rationale.md` is the design rationale, the synthesis decision, accepted trade-offs, deviations, and open questions.
    - `src/` has the public modules (`JobQueue.ts`, `PgBoss.ts`) with real signatures and TODO bodies, and the internal modules (`internal/connection.ts`, `internal/workerTransaction.ts`, `internal/housekeeping.ts`, `internal/runtime.ts`) with real code.
    - `examples/usage.ts` compiles the README call sites and holds type-level assertions.
    - `prototype/synth.ts` runs the internal modules against a real Postgres. `synth.out` is the Postgres 17 output, and `synth-pg18.out` is the Postgres 18 output.
- `arena/synthesis-spec.md` is the spec I gave to the agent that built the synthesis.
- `arena/rubric.md` is the rubric used to judge the three candidates.
- `arena/candidate-1/`, `arena/candidate-2/` and `arena/candidate-3/` are the three competing sketches, kept for reference.
- `prototypes/investigation-runtime/` has the investigation experiments E1 to E5 (see "What the investigation found").
- `prototypes/wire/probe.mjs` is a raw Postgres wire-protocol probe. `prototypes/probe/probe.ts` probes `SqlClient.make` with a fake connection. `prototypes/scripts/` has probes of every pg-boss API that takes `{ db }`.
- `references/effect-sql-pg-PgClient-before-7426.ts` is the old node-postgres based `PgClient` from the Effect repo (`git show 534b8b9dba^:packages/sql/pg/src/PgClient.ts`). The pg-boss source at 12.38.0 is in `.repos/deps/pg-boss` at the repository root (`pnpm deps:fetch`).

## The goal

An application keeps its own tables and a pg-boss queue in one Postgres database. It uses the Effect SQL stack (`sql` templates, `SqlSchema`, `Model`, `Migrator`, `withTransaction`) on the native `@effect/sql-pg` 4.0.5 client. It needs two things:

1. Enqueue pg-boss jobs and flows inside `sql.withTransaction`, so they commit or roll back with the app's rows.
2. Write app tables through `@effect/sql` inside a pg-boss transactional worker (`boss.work(q, { transactional: true }, ...)`), so the writes commit together with the job's completion.

## What the investigation found

The answer is yes for both, with a small library of your own and no change in Effect or pg-boss.

- **The native client cannot run multi-statement SQL.** Every query path in `@effect/sql-pg` 4.0.5 sends a `Parse` message. There is no encoder for the simple-query message. Experiment E1 tried 9 query modes, including `prepare: false`, `.unprepared` and `multiplex: true`. All failed with `cannot insert multiple commands into a prepared statement`. This is deliberate. Effect PR #7426 (commit `534b8b9dba`, 2026-08-25) replaced node-postgres with the native client. Its changeset lists "Query strings must contain one statement" as a breaking change, and it removed `fromPool`, `fromClient` and `makeWith`.
- **Only `boss.flow` needs multi-statement SQL on a `{ db }` path.** Every other pg-boss API that takes `{ db }` sends one statement with parameters, and all of them work on the native client. `flow` joins its statements with `";\n"` and sends them without parameters (`pg-boss/dist/manager.js:1840-1843`).
- **pg-boss's own constructor `db` must run multi-statement SQL.** Migrations, maintenance and BAM need it, so pg-boss's own statements must run on node-postgres.
- **A pg-boss worker's `tx` is only `{ executeSql }`.** It does not expose the node-postgres client. pg-boss fences a handler's settle calls by the identity of that object, so app code must pass the exact `tx` object to pg-boss.
- **Joining an external transaction by hand is broken in Effect.** If you provide `sql.transactionService` yourself, a nested `withTransaction` dies with `Service not found: effect/sql/SqlClient/TransactionSemaphore/<n>` (`packages/effect/src/sql/SqlClient.ts:676`).
- **The parked transaction works.** This was the key finding of the design phase. pg-boss lets a custom constructor `db` implement `beginTransaction()`. The library's `beginTransaction()` forks a fiber that runs a real `sql.withTransaction` on the app's native client and waits on a `Deferred`. pg-boss's `commit()` and `rollback()` settle the `Deferred`. The worker transaction is therefore a normal Effect transaction. Nested `withTransaction` calls become savepoints, `afterCommit` runs after the real COMMIT, and rows decode the native way.

The investigation experiments, all against a real Postgres with pg-boss 12.38.0:

| Experiment                                                                | Result                                                                    |
| ------------------------------------------------------------------------- | ------------------------------------------------------------------------- |
| E1. Multi-statement text through native `PgClient`                        | Fails in all 9 modes.                                                     |
| E2. `boss.flow` with a direct adapter on the `withTransaction` connection | Fails with the same error.                                                |
| E3. Adapter that splits `flow`'s text on `";\n"`                          | Commit and rollback are atomic.                                           |
| E4. A `SqlClient.make` over the worker's `tx.executeSql`, with savepoints | Works, but `afterCommit` fires before the real COMMIT. Rejected later.    |
| E5. A `SqlClient.make` over a node-postgres pool                          | `flow` works without a split. Rejected later because it changes decoding. |

## Decisions

| Decision            | Choice                                                                                             | Reason                                                                                                                                                                                                                                                       |
| ------------------- | -------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Package name        | `@wmaurer/effect-sql-pg-boss`                                                                      | It follows Effect v3's `@effect/sql-drizzle` and `@effect/sql-kysely` ("X integration for @effect/sql"). It still fits if the node-postgres parts go away. It is different from `@shivaedev/effect-pg-boss`, which deliberately does not share transactions. |
| Packaging           | One package. `pg-boss`, `effect` and `@effect/sql-pg` are peer dependencies. `pg` is a dependency. | Nothing else needs to be split out today.                                                                                                                                                                                                                    |
| App's SQL client    | The native `@effect/sql-pg` `PgClient`                                                             | The app already uses it. Switching to node-postgres would change every row decode in the app (`int8` from `bigint` to string, timestamps from epoch ms to `Date`).                                                                                           |
| Worker transactions | The parked transaction                                                                             | Real Effect transaction semantics inside the worker.                                                                                                                                                                                                         |
| Expected lifetime   | Permanent userland package                                                                         | Effect removed the node-postgres client on purpose, and v4 dropped the drizzle and kysely integrations.                                                                                                                                                      |

## The API

This is a summary. The full usage is in `arena/synthesis/README.md`.

```ts
import { PgClient } from "@effect/sql-pg"
import { JobQueue, PgBoss } from "@wmaurer/effect-sql-pg-boss"
import { Config, Effect, Layer, Schema } from "effect"
import { SqlClient } from "effect/sql"

class ChargeCard extends JobQueue.Service<ChargeCard>()("charge-card", {
	payload: Schema.Struct({ orderId: Schema.BigInt, amountCents: Schema.Int }),
	options: { retryLimit: 5, retryBackoff: true }
}) {}
class ShipOrder extends JobQueue.Service<ShipOrder>()("ship-order", {
	payload: Schema.Struct({ orderId: Schema.BigInt })
}) {}
class AuditLog extends JobQueue.Service<AuditLog>()("audit-log", {
	payload: Schema.Struct({ orderId: Schema.BigInt, event: Schema.String })
}) {}

// Direction 1: enqueue inside the app's transaction.
const placeOrder = Effect.fn("placeOrder")(function*(cart: Cart) {
	const sql = yield* SqlClient.SqlClient
	return yield* sql.withTransaction(Effect.gen(function*() {
		const [order] = yield* sql<{ id: bigint }>`insert into orders ${sql.insert(cart)} returning id`
		const jobs = yield* JobQueue.flow({
			charge: ChargeCard.step({ orderId: order.id, amountCents: cart.totalCents }),
			ship: ShipOrder.step({ orderId: order.id }, { dependsOn: ["charge"] })
		})
		yield* sql`update orders set charge_job_id = ${jobs.charge} where id = ${order.id}`
		return order.id
	}))
})

// Direction 2: write app tables inside pg-boss's transaction.
const ChargeCardWorker = ChargeCard.worker(
	Effect.fn("ChargeCardWorker")(function*(jobs) {
		const sql = yield* SqlClient.SqlClient
		for (const job of jobs) {
			yield* sql`update orders set status = 'charged' where id = ${job.data.orderId}`
			yield* sql.withTransaction(sql`insert into ledger ${sql.insert({ orderId: job.data.orderId })}`)
			yield* AuditLog.send({ orderId: job.data.orderId, event: "charged" })
		}
	}),
	{ transactional: true, batchSize: 10, localConcurrency: 2 }
)

// Wiring.
const AppLive = ChargeCardWorker.pipe(
	Layer.provideMerge(Layer.mergeAll(ChargeCard.layer, ShipOrder.layer, AuditLog.layer)),
	Layer.provideMerge(PgBoss.layer({ schema: "pgboss" })),
	Layer.provideMerge(PgClient.layerConfig({ url: Config.Redacted("DATABASE_URL") }))
)
```

`ChargeCardWorker` only writes the database, so it is transactional. A handler that calls an external service, such as a card processor, would usually be `transactional: false`, so it does not hold a connection and an open transaction while it waits.

The public surface:

- `JobQueue.Service<Self>()(name, { payload, options? })` declares a queue. The class has these statics:
    - `queueName`, `payload` and `options`.
    - `layer` creates the queue if it is missing, when the layer builds.
    - `send(payload, options?)` returns `Effect<Option<JobId>, SqlError | PgBossError | SchemaError, Self | ...>`. It is `None` when a singleton or throttle policy drops the job.
    - `step(payload, { dependsOn?, job? })` builds one node of a flow.
    - `worker(handler, options)` returns a `Layer`. The handler gets a non-empty batch of `Job<A>`. `transactional` is required and has no default. `transactionTimeoutSeconds` exists only when `transactional` is `true`.
- `JobQueue.flow(record)` inserts a graph of jobs and returns `{ [ref]: JobId }`. `dependsOn` accepts only the other keys of the same record.
- `JobQueue.Job<A>` has `id`, `queue`, `data`, `retryCount` and `expireInSeconds`. `JobQueue.JobId` is a branded string with a `Schema`.
- `PgBoss.layer(options?)` and `PgBoss.layerConfig(...)` need `PgClient.PgClient`. Options are pg-boss constructor options without the connection fields, plus these:
    - `maintenanceConnections` sizes the node-postgres pool for pg-boss's own statements. The default is 4.
    - `connectionReserve` is the number of `PgClient` connections that transactional workers must leave free. The default is 2.
    - `rollbackTimeout` bounds a worker rollback. The default is 5 seconds.
    - `stop` sets the options for `boss.stop()`.
- `PgBoss.use((boss, connection) => Promise<A>)` is the escape hatch for the rest of pg-boss's API. `connection` is `{ db }` inside a transaction and `{}` outside one.
- `PgBossError` covers pg-boss failures that are not SQL errors. SQL failures surface as the original typed `SqlError`, for example `UniqueViolation`.

These mistakes are compile errors, and `packages/effect-sql-pg-boss/test/usage.ts` proves each one with a `@ts-expect-error`:

- a typo in a `dependsOn` ref, and a step that depends on itself;
- a payload that does not match the schema;
- `transactionTimeoutSeconds` on a non-transactional worker;
- a worker that does not state `transactional`, or has no options at all;
- using a queue whose layer is not provided.

## How it works inside

- **One routing rule.** `internal/connection.ts` derives the pg-boss `Db` for a call from the fiber's current Effect transaction connection. It caches the `Db` in a `WeakMap<Connection, Db>`. The `Db` calls `conn.executeUnprepared(text, values ?? [], undefined)` directly, not `sql.unsafe`. So pg-boss gets raw rows even when the app sets `transformResultNames`. Outside a transaction, `send` and `flow` open their own `withTransaction`.
- **The `flow` split.** A call with `values === undefined` is a multi-statement script, and only `flow` sends one on a `{ db }` path. The library splits it on `";\n"` and runs each part on the same connection. This is the only part that depends on a pg-boss internal. It is the reason for the `~12.38` peer pin.
- **Errors.** The `Db` rejects with the driver's own `Error`, so pg-boss still sees the SQLSTATE `code`, `constraint` and `detail`. A `WeakMap` maps that error back to the original `SqlError`, so the caller's Effect fails with the typed error.
- **The parked worker transaction** (`internal/workerTransaction.ts`).
    1. `beginTransaction()` forks `sql.withTransaction(body)` into the `PgBoss` layer's `FiberSet`.
    2. The body opens the transaction and hands pg-boss the handle.
    3. The handler runs as a child fiber of the parked fiber. The parked transaction's services win over the worker layer's services.
    4. `rollback()` interrupts the handler before the ROLLBACK runs, so no handler statement can run after the connection goes back to the pool.
    5. `commit()` resolves when the parked fiber exits after the real COMMIT. The design resolved it earlier, on a committed signal, but that needs `SqlClient.afterCommit` (see "Versions").
    6. `rollback()` is idempotent, never rejects, and gives up after `rollbackTimeout`.
    7. After the transaction settles, its `Db` rejects every statement.
- **The maintenance pool** (`internal/housekeeping.ts`). This is a small node-postgres pool built from `PgClient.config`. It runs pg-boss's own statements: migrations, maintenance, BAM, fetch, and settles outside a transaction. App rows never use it. It has `'error'` listeners on the pool and on each checked-out client.
- **Pool headroom.** Each transactional worker layer reserves `localConcurrency` connections from a budget. The layer fails to build when fewer than `connectionReserve` connections would stay free.
- **Lifecycle.** The order on release is: `boss.stop({ graceful: true, close: false })`, then the `FiberSet` interrupts leftover worker transactions (they roll back), then the maintenance pool ends. Worker layers call `offWork(name, { id, wait: true })` on release.

## Runtime evidence for the sketch

`prototype/synth.ts` imports the real internal modules. It ran on Postgres 17.11 and again on Postgres 18.6, with pg-boss 12.38.0 and `effect` and `@effect/sql-pg` from the Effect repo sources at `1b740862e3` (see "Versions"). Both runs gave the same results, apart from timings and ids.

| Check                   | Result                                                                                                                                                                                                                                      |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| P1. Commit and failure  | The commit path kept the app rows and a nested savepoint row. The job ended `completed`, and the follow-up job was `created`. The failure path left nothing, and the job ended `failed`. `currentDb === pg-boss tx` was true.               |
| P2. Abandoned handler   | A handler in `pg_sleep(10)` was interrupted 15 ms after pg-boss called `rollback()`. The Postgres statement log shows no handler statement after the ROLLBACK. The connection was idle and reusable.                                        |
| P2b. Bounded rollback   | With an uninterruptible 4 s handler and `rollbackTimeout` 1 s, `rollback()` resolved after 1001 ms. The handler's later insert never ran.                                                                                                   |
| P3. Commit timing       | `commit()` resolved after 1 ms. An app `afterCommit` that sleeps 3 s finished 3051 ms later.                                                                                                                                                |
| P4. Callback order      | The library's committed signal ran before the app's first `afterCommit`.                                                                                                                                                                    |
| P5. Raw rows and errors | With camelCase `transformResultNames`, pg-boss got `created_on`. `send` and `flow` inside a transaction committed and rolled back with it. A unique violation reached pg-boss as `code=23505` and the Effect as `SqlError/UniqueViolation`. |
| Headroom                | Reserving past the budget failed at layer build with a clear message.                                                                                                                                                                       |

`test/layer.test.ts` in the package runs the paths the prototype did not:

- `PgBoss.use` gets `{}` outside a transaction and the transaction's `db` inside one. A `send` through it commits and rolls back with the app's rows.
- The headroom budget comes from config. `maxConnections: 5` with `connectionReserve: 2` gives 3, and an unset pool with the default reserve gives 8.
- A COMMIT that fails on a deferred unique constraint rolls back the handler's rows. The job ends `failed`, and its stored error carries `code: "23505"`.
- Closing the layer while a transactional handler sleeps takes the stop timeout and little more (2 s configured). The handler is interrupted, its row rolls back, the job ends `failed` with `pg-boss shut down while active`, and no connection of either pool remains.
- A worker transaction that cannot start fails the job without running the handler. The test forces it with a `PgClient` whose startup parameters the server rejects (`22023`), so the failure is at connect, not at the `BEGIN` statement.
- An error on a maintenance connection reaches the app's logger.

Not run yet: shutdown while a handler waits to start.

## Versions

- **pg-boss 12.38.0.** This is the latest release on npm (published 2026-10-13, checked on 2026-10-20), and pg-boss master has the same `flow` code. All runtime checks used it. The peer range is `~12.38` because the `flow` split depends on how pg-boss joins statements, and 12.38 is the only version tested at runtime. pg-boss 12.37.1 has the same join (`statements.join(';\n')` at `dist/manager.js:1686`; source checked, not run). So the range could probably widen to `>=12.37.1 <12.39` after one test run on 12.37.1.
- **Postgres 17.** Nothing in the design depends on Postgres 17. It was the Docker image used for the tests. The full prototype also passed on Postgres 18.6 (`synth-pg18.out`). pg-boss's README requires PostgreSQL 13 or higher.
- **effect and @effect/sql-pg 4.0.5.** These are the versions in `problem.md` and the current npm release. The prototypes used the Effect repo sources at commit `1b740862e3`. That commit is not 4.0.5. It is `4.0.5-233-g1b740862e3`, 233 commits after the 4.0.5 tag.
- **No `SqlClient.afterCommit` in 4.0.5.** `afterCommit` arrived in Effect PR #9289, after the 4.0.5 tag. The package targets 4.0.5 anyway, without the committed signal. `commit()` resolves when the parked fiber exits after the COMMIT. P1, P2, P2b, P5 and the headroom check ran again on the real 4.0.5 and passed. P1's `afterCommit` check, P3 and P4 need `afterCommit`, so they are deferred. The committed signal, P3 and P4 return when the peer range moves to the first release that includes #9289. Issue #1 tracks this.
- **pg 8.23.1.** This version was used for the maintenance pool and in pg-boss.

## The three candidate designs

Three agents sketched the API independently. One was assigned the native-client direction. A judge on a different model scored them.

|                         | Candidate 1                                               | Candidate 2                                                      | Candidate 3                                          |
| ----------------------- | --------------------------------------------------------- | ---------------------------------------------------------------- | ---------------------------------------------------- |
| App's SQL client        | A node-postgres client from this package (`PgNodeClient`) | A node-postgres client from this package (`NodePgClient`)        | Native `PgClient` (assigned)                         |
| Worker transaction      | Parked transaction                                        | A borrowed `SqlClient` per batch, with BEGIN mapped to SAVEPOINT | Parked transaction                                   |
| Pools                   | One                                                       | One                                                              | Two (native, plus a maintenance pool)                |
| `flow` in a transaction | Works natively (simple protocol)                          | Works natively                                                   | Split on `";\n"`                                     |
| Queue API               | One class per queue (`JobQueue.Service`)                  | `Queue.make` values, plus a `queues: [...]` list on the layer    | `JobQueue.make` values, created lazily on first send |
| Judge score (of 30)     | 24                                                        | 20                                                               | 22                                                   |

- **Candidate 1, "one transaction engine".** One object, `NodePgConnection`, is both the Effect `Connection` and the pg-boss `Db`, so `tx` identity holds by construction. It had the best ergonomics and type-level checks. The judge picked it. I did not, because it forces the app onto node-postgres decoding and leaves you maintaining a 300 to 800 line driver client.
- **Candidate 2, "ambient connection".** Every `Boss` operation runs on the fiber's connection. Workers get a borrowed client over `tx.executeSql`. It was rejected for two reasons. Repositories built at layer time write outside the worker transaction, and `afterCommit` fires before the real COMMIT. Two of its ideas were kept: the bounded rollback race, and eager queue creation at layer build.
- **Candidate 3, "native client stays".** It found the parked transaction independently of candidate 1 and proved it on the native client. It is the base of the synthesis.

The synthesis is candidate 3's native direction with candidate 1's queue API. It adds the judge's required fixes: the handler as a child of the parked fiber, the committed signal, the bounded rollback, the released guard, the headroom check, and the `'error'` listeners. `arena/synthesis/rationale.md` lists every graft with its source.

## Rejected alternatives

- **A node-postgres client as the app's main client** (candidates 1 and 2). It changes the app's decoders and adds a permanent driver client. Choose it only if one pool and no `flow` split matter more to you than native decoding. Candidate 1 is ready for that and typechecks.
- **A borrowed per-batch `SqlClient` in workers** (candidate 2). Repositories write outside the transaction, and `afterCommit` fires too early.
- **Providing `transactionService` as `[conn, 0]` by hand.** It dies at `SqlClient.ts:676`, and `afterCommit` runs immediately.
- **Lazy queue creation on the first send** (candidate 3). DDL on another connection can wait on locks that the caller's own transaction holds.
- **Splitting every multi-statement pg-boss script on `;`.** pg-boss's `dist/plans.js` has 76 `$$` dollar-quote markers around function bodies that contain `;`, so the split would corrupt them.
- **A full Effect wrapper over about 80 pg-boss methods.** `PgBoss.use` covers them.

## Open decisions for you

1. **Decided: transactional workers are not supported on a `PgClient.makeClient` client, and `PgBoss.layer` does not refuse them.** `PgClient.make` and `PgClient.makeClient` both store the raw options as `config` (`PgClient.ts:186`, `:212`), so nothing public tells the two apart. The headroom check reads `config.maxConnections` through one cast and falls back to 10, the `PgPool` default (`PgPool.ts:162`), so the budget is exact for every pooled client. On `makeClient` a transactional batch would pin the only connection for the handler's run (`PgClient.ts:209`) and stall every other query of the app, and the docs say so.
2. **Decided: `transactional` is required on every `worker` call.** The two modes differ in what they hold (a connection and an open transaction for the whole handler run) and in what commits together, and the right one depends on the handler. pg-boss defaults to `false` (`src/attorney.ts:480`), so a `true` default would silently diverge from it. A required field ties the headroom build failure to a visible choice, at the cost of one field per worker.
3. Should a payload that fails to decode go to the dead-letter queue instead of being retried?
4. Should a queue layer also update the options of an existing queue (`updateQueue`)?
5. Is polling acceptable for v0.1, or should `useListenNotify` go through `PgClient.listen` now?
6. Should the handler be able to store a typed result or error in the job's output?

## Upstream opportunities

None of these is needed. Each one would make the package smaller.

- **`PgClient.simple(text)` in `@effect/sql-pg`.** It would send the simple-query message, in about 150 to 250 lines of source plus tests. With it, the maintenance pool, the `flow` split, the `pg` dependency and the version pin all go away. My guess for the chance of acceptance is low to medium, because multi-statement support was removed on purpose. A similar Bun SQLite request, Effect-TS/effect#5930, was closed as not planned on 2026-10-13. Open an issue before writing a PR. Base the pitch on the convention that Bun, postgres.js, pg and pg-boss all follow: no parameters means the simple protocol.
- **A one-line fix at `packages/effect/src/sql/SqlClient.ts:676`.** It would make "join an existing transaction" work by providing `transactionService`. This design does not need it.
- **pg-boss sends `flow` as one `executeSql` call per statement when you pass `db`.** It is a two-line change at `manager.js:1840-1843`. The maintainer is responsive (#945 was fixed in a day), but a code comment says the single round trip is intended "regardless of db adapter".

## Gotchas for the implementation

- Pass pg-boss the exact `tx` object for settle calls. A wrapper object looks like a lost claim, and pg-boss rolls back.
- Keep the SQLSTATE `code`, `constraint` and `detail` on errors that go back to pg-boss. pg-boss checks codes 22012, 23505, 42P01, 23514 and 25P02.
- Inside a transaction, pg-boss read APIs used through `PgBoss.use` return natively decoded rows, so timestamps arrive as epoch milliseconds. Read outside transactions.
- Each in-flight transactional batch holds one `PgClient` connection for the handler's whole run. Use `transactional: false` for slow handlers.
- A handler stuck inside an uninterruptible statement can hold its connection until the native client's cancel timeout. This was inferred from the code, not measured.
- `pg-boss`'s `TestClock` wraps `tx` (`activity.js:7-9`). It looks unreachable with this `Db`, but check it when you add tests.

## Run the prototypes again

The prototypes ran with Node 26 type stripping (`node file.ts`). The prototype directories used a `node_modules` with these packages:

- `pg-boss@12.38.0` and `pg@8.23.1`, from npm.
- `effect` and `@effect/sql-pg`, as symlinks to copies of the Effect repo sources (`packages/effect/src` and `packages/sql/pg/src` at `1b740862e3`, which is `4.0.5-233-g1b740862e3`). In a new repo, install `effect@4.0.5` and `@effect/sql-pg@4.0.5` from npm instead.

`types/pg/index.d.ts` is a small stand-in for `@types/pg`. Install `@types/pg` in the real package instead.

To run the synthesis prototype:

1. Start a throwaway Postgres 18. The script reads the server log from the container named `synth-pg`, so keep that name.

    ```sh
    docker run -d --rm --name synth-pg -e POSTGRES_PASSWORD=probe -p 127.0.0.1:55511:5432 \
    	postgres:18 -c log_statement=all -c "log_line_prefix=[%p] "
    ```

2. Run the prototype from `arena/synthesis/`.

    ```sh
    node prototype/synth.ts > prototype/synth.out
    ```

3. Stop the container.

    ```sh
    docker stop synth-pg
    ```

To typecheck a sketch, run `tsc -p tsconfig.json --noEmit` in its directory. TypeScript 7.0.2 was used.

## Next step

Steps 1 to 4 are done. `packages/effect-sql-pg-boss/` has the dependencies, the ported `src/`, `PgBoss.layer`, and `test/usage.ts` with the type-level assertions. Its integration suite runs P1, P2, P2b, P5 and the headroom check through `PgBoss.layer` against Postgres 18 (`pnpm test`). The tests register transactional work with `boss.work` and `WorkerTransaction.workCallback`, as `JobQueue.worker` will.

Next, implement `JobQueue.Service` and `JobQueue.flow`, and run the suite again through `ChargeCard.worker` and `ChargeCard.send` instead of the internal helpers.
