# @wmaurer/effect-sql-pg-boss

pg-boss for Effect apps that use the native `@effect/sql-pg` client. Your app tables and your pg-boss queues live in one Postgres database and share transactions in both directions:

- A job you send inside `sql.withTransaction` commits or rolls back with your rows.
- Rows you write in a transactional worker commit or roll back with the job's completion.

You never pass a `db` or a `tx`. The library finds the transaction the same way `sql` statements do, from the fiber. Your rows decode the same way everywhere (`int8` as `bigint`, timestamps as epoch milliseconds), in request code and in workers, because both run on your one `PgClient`.

## Install

```sh
pnpm add @wmaurer/effect-sql-pg-boss effect @effect/sql-pg pg-boss
```

`effect`, `@effect/sql-pg` and `pg-boss` (`~12.38`) are peer dependencies.

## Quickstart

```ts
import { PgClient } from "@effect/sql-pg"
import { JobQueue, PgBoss } from "@wmaurer/effect-sql-pg-boss"
import { Config, Effect, Layer, Schema } from "effect"
import { SqlClient } from "effect/sql"

// 1. Declare a queue. The class holds the queue's name, its payload schema and
//    its pg-boss options. It is also a service: code that sends to the queue or
//    works it needs `SendWelcomeEmail` in its context, so a queue you forgot to
//    register is a type error, not a "queue not found" at run time.
class SendWelcomeEmail extends JobQueue.Service<SendWelcomeEmail>()("send-welcome-email", {
  payload: Schema.Struct({ userId: Schema.BigInt, email: Schema.String }),
  options: { retryLimit: 5, retryBackoff: true }
}) {}

// 2. Send inside a transaction. The job is inserted on the transaction's connection.
const signUp = Effect.fn("signUp")(function*(email: string) {
  const sql = yield* SqlClient.SqlClient
  return yield* sql.withTransaction(Effect.gen(function*() {
    const [user] = yield* sql<{ id: bigint }>`insert into users ${sql.insert({ email })} returning id`
    yield* SendWelcomeEmail.send({ userId: user.id, email })
    return user.id
  }))
})

// 3. Work the queue. By default each batch runs inside pg-boss's transaction, so
//    the handler's `sql` writes commit together with the job's completion.
const WelcomeEmailWorker = SendWelcomeEmail.worker((jobs) =>
  Effect.gen(function*() {
    const sql = yield* SqlClient.SqlClient
    for (const job of jobs) {
      yield* sql`insert into email_outbox ${sql.insert({ userId: job.data.userId, kind: "welcome" })}`
    }
  })
)

// 4. Wire it. Workers need their queue's layer, queue layers need PgBoss, and
//    PgBoss needs your PgClient.
export const AppLive = WelcomeEmailWorker.pipe(
  Layer.provideMerge(SendWelcomeEmail.layer),
  Layer.provideMerge(PgBoss.layer({ schema: "pgboss" })),
  Layer.provideMerge(PgClient.layerConfig({ url: Config.Redacted("DATABASE_URL") }))
)
```

`PgBoss.layer` installs or migrates the pg-boss schema and starts pg-boss. Each queue's `layer` creates the queue if it is missing, when the layer builds. Closing the app layer stops pg-boss gracefully: running handlers finish their batches first.

## Enqueue a flow inside your transaction

A flow is a graph of jobs that pg-boss inserts in one go. The keys of the record are the refs. `dependsOn` accepts only the other keys of the same record, so a typo in a ref is a type error.

```ts
class ChargeCard extends JobQueue.Service<ChargeCard>()("charge-card", {
  payload: Schema.Struct({ orderId: Schema.BigInt, amountCents: Schema.Int }),
  options: { retryLimit: 5, retryBackoff: true }
}) {}
class ShipOrder extends JobQueue.Service<ShipOrder>()("ship-order", {
  payload: Schema.Struct({ orderId: Schema.BigInt })
}) {}
class SendReceipt extends JobQueue.Service<SendReceipt>()("send-receipt", {
  payload: Schema.Struct({ orderId: Schema.BigInt })
}) {}

export const placeOrder = Effect.fn("placeOrder")(function*(cart: Cart) {
  const sql = yield* SqlClient.SqlClient
  return yield* sql.withTransaction(Effect.gen(function*() {
    const [order] = yield* sql<{ id: bigint }>`insert into orders ${sql.insert(cart)} returning id`
    const jobs = yield* JobQueue.flow({
      charge: ChargeCard.step({ orderId: order.id, amountCents: cart.totalCents }),
      ship: ShipOrder.step({ orderId: order.id }, { dependsOn: ["charge"] }),
      receipt: SendReceipt.step({ orderId: order.id }, { dependsOn: ["charge"], job: { startAfter: 30 } })
    })
    // jobs: { charge: JobId; ship: JobId; receipt: JobId }
    yield* sql`update orders set charge_job_id = ${jobs.charge} where id = ${order.id}`
    return order.id
  }))
})
// Effect<bigint, SqlError | PgBossError | SchemaError, SqlClient | ChargeCard | ShipOrder | SendReceipt>
```

If the `update` fails, the order row and all three jobs roll back together. Outside a transaction, `send` and `flow` open one of their own, so a flow is all-or-nothing there too.

`send` returns `Option<JobId>`. It is `None` when pg-boss drops the job because of a singleton or throttle option, or a `singleton` or `stately` queue policy.

## Write app tables in a transactional worker

```ts
export const ChargeCardWorker = ChargeCard.worker(
  Effect.fn("ChargeCardWorker")(function*(jobs) {
    const sql = yield* SqlClient.SqlClient
    const payments = yield* Payments
    for (const job of jobs) {
      const receipt = yield* payments.charge(job.data.orderId, job.data.amountCents, { idempotencyKey: job.id })
      yield* sql`update orders set status = 'charged', payment_ref = ${receipt.ref} where id = ${job.data.orderId}`
      // A nested transaction is a SAVEPOINT inside pg-boss's transaction.
      yield* sql.withTransaction(sql`insert into ledger ${sql.insert({ orderId: job.data.orderId, ref: receipt.ref })}`)
      // The follow-up job joins the same transaction.
      yield* AuditLog.send({ orderId: job.data.orderId, event: "charged" })
      // Runs after pg-boss's real COMMIT, when the job is already completed.
      yield* sql.afterCommit(Effect.logInfo("order charged", job.data.orderId))
    }
  }),
  { batchSize: 10, localConcurrency: 2 }
)
// Layer<never, PgBossError, ChargeCard | AuditLog | Payments | SqlClient>
```

The handler gets a non-empty batch of jobs with decoded payloads. The batch fails or succeeds as a whole. If the handler fails, dies or is interrupted, the order update, the ledger row, the audit job and the completion all roll back, and pg-boss retries the batch. If the job expires or pg-boss stops while the handler is still running, the library interrupts the handler first and only then rolls back, so no handler statement runs after the rollback.

The stored error of a failed job is the handler's `Cause`, pretty-printed.

## Work without a transaction

A handler that calls slow external services should not hold a database connection open. Opt out per worker:

```ts
export const RenderVideoWorker = RenderVideo.worker(
  Effect.fn("RenderVideoWorker")(function*(jobs) {
    const sql = yield* SqlClient.SqlClient
    for (const job of jobs) {
      const url = yield* renderer.render(job.data.videoId) // minutes
      // Its own short transaction. Not atomic with the job's completion.
      yield* sql.withTransaction(sql`update videos set url = ${url} where id = ${job.data.videoId}`)
    }
  }),
  { transactional: false }
)
```

`transactionTimeoutSeconds` exists only on transactional workers. Passing it with `transactional: false` is a type error.

## Use the rest of pg-boss

The queue classes cover sending and working. For everything else (cancel, retry, stats, schedules), `PgBoss.use` gives you the pg-boss instance and the connection options for the fiber's transaction. Pass the options on, and the call joins the transaction.

```ts
const stats = PgBoss.use((boss) => boss.getQueueStats(ChargeCard.queueName))
const cancel = (jobId: JobQueue.JobId) =>
  PgBoss.use((boss, connection) => boss.cancel(ChargeCard.queueName, jobId, connection))
```

Outside a transaction `connection` is `{}`, and pg-boss runs the call on its own connections. Inside a transaction, pg-boss read APIs return rows that the native client decoded, so timestamps are epoch milliseconds, not `Date`. Prefer to read outside a transaction.

## Requirements and limits

- Your app's `SqlClient` must be the one `PgClient.layer` provides. The library needs `PgClient.PgClient` in the context.
- Each in-flight transactional batch holds one connection from your `PgClient` pool for the handler's whole run. A transactional worker layer fails to build when its `localConcurrency` would leave fewer than `connectionReserve` (default 2) free connections in the pool.
- `@effect/sql-pg` cannot yet run a script with more than one statement. pg-boss needs that for its migrations and maintenance, so the library keeps a small node-postgres pool for pg-boss's own statements, built from your `PgClient` settings. Your rows never use it. Size it with `PgBoss.layer({ maintenanceConnections })` (default 4).
- `PgBoss.layer` fails when your `PgClient` config has a custom `stream`, because node-postgres cannot reuse it.
- A transactional worker's rollback gives up after `rollbackTimeout` (default 5 seconds), as pg-boss requires.
- pg-boss's `useListenNotify` is not supported yet. Workers poll.
- `pg-boss` is pinned to `~12.38`. `flow` inside your transaction depends on how pg-boss 12.38 joins its statements.
