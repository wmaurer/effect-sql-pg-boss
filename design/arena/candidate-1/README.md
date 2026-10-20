# @wmaurer/effect-sql-pg-boss

pg-boss integration for `effect/sql`. Your app tables and your pg-boss queues
live in one Postgres database, use one connection pool, and share
transactions in both directions:

- A job you send inside `sql.withTransaction` commits or rolls back with your rows.
- Rows you write in a transactional worker commit or roll back with the job's completion.

You do not pass a `db` or a `tx` anywhere. The package reads the transaction
the same way `sql` statements do: from the fiber.

## Install

```sh
pnpm add @wmaurer/effect-sql-pg-boss effect pg pg-boss
```

`pg` and `pg-boss` are peer dependencies.

## Quickstart

```ts
import { PgBoss, PgNodeClient, JobQueue } from "@wmaurer/effect-sql-pg-boss"
import { Config, Effect, Layer, Schema } from "effect"
import { SqlClient } from "effect/sql"

// 1. Define a queue. The class is the queue's name, its payload schema and its
//    pg-boss queue options. It is also a service: code that sends to it or
//    works it needs `SendWelcomeEmail` in its context, so a queue you forgot
//    to register is a type error, not a "queue not found" at runtime.
class SendWelcomeEmail extends JobQueue.Service<SendWelcomeEmail>()("send-welcome-email", {
  payload: Schema.Struct({ userId: Schema.String, email: Schema.String }),
  options: { retryLimit: 5, retryBackoff: true }
}) {}

// 2. Send inside a transaction. The job is inserted on the transaction's connection.
const signUp = Effect.fn("signUp")(function*(email: string) {
  const sql = yield* SqlClient.SqlClient
  return yield* sql.withTransaction(Effect.gen(function*() {
    const [user] = yield* sql<{ id: string }>`insert into users ${sql.insert({ email })} returning id`
    yield* SendWelcomeEmail.send({ userId: user.id, email })
    return user.id
  }))
})

// 3. Work the queue. With `transactional: true` every `sql` statement in the
//    handler, and every job it sends, commits with the job's completion.
const WelcomeEmailWorker = SendWelcomeEmail.worker(
  Effect.fn("WelcomeEmailWorker")(function*(jobs) {
    const sql = yield* SqlClient.SqlClient
    for (const job of jobs) {
      yield* sql`insert into email_outbox ${sql.insert({ userId: job.data.userId, kind: "welcome" })}`
    }
  }),
  { transactional: true }
)

// 4. Wire it. One pool, one pg-boss instance, started and stopped with the layer.
//    Workers need their queue's layer; queue layers need PgBoss; PgBoss needs PgNodeClient.
export const AppLive = WelcomeEmailWorker.pipe(
  Layer.provideMerge(SendWelcomeEmail.layer),
  Layer.provideMerge(PgBoss.layer({ schema: "pgboss" })),
  Layer.provideMerge(PgNodeClient.layerConfig({ url: Config.Redacted("DATABASE_URL") }))
)
```

## Call sites

### Place an order and start a flow, atomically

A flow is a DAG of jobs. The record keys are the refs. `dependsOn` only
accepts the other keys of the same record, so a typo in a ref is a type error.

```ts
class ChargeCard extends JobQueue.Service<ChargeCard>()("charge-card", {
  payload: Schema.Struct({ orderId: Schema.String, amountCents: Schema.Int })
}) {}
class ShipOrder extends JobQueue.Service<ShipOrder>()("ship-order", {
  payload: Schema.Struct({ orderId: Schema.String })
}) {}
class SendReceipt extends JobQueue.Service<SendReceipt>()("send-receipt", {
  payload: Schema.Struct({ orderId: Schema.String })
}) {}

export const placeOrder = Effect.fn("placeOrder")(function*(cart: Cart) {
  const sql = yield* SqlClient.SqlClient
  return yield* sql.withTransaction(Effect.gen(function*() {
    const [order] = yield* sql<{ id: string }>`insert into orders ${sql.insert(cart.toRow())} returning id`
    const jobs = yield* JobQueue.flow({
      charge: ChargeCard.step({ orderId: order.id, amountCents: cart.totalCents }),
      ship: ShipOrder.step({ orderId: order.id }, { dependsOn: ["charge"] }),
      receipt: SendReceipt.step({ orderId: order.id }, { dependsOn: ["charge"] })
    })
    // jobs: { charge: JobId; ship: JobId; receipt: JobId }
    yield* sql`update orders set charge_job_id = ${jobs.charge} where id = ${order.id}`
    return order.id
  }))
})
// Effect<string, SqlError | PgBossError | SchemaError, SqlClient | ChargeCard | ShipOrder | SendReceipt>
```

If the `update` fails, the order row and all three jobs roll back together.

### A transactional worker that writes app tables and sends the next job

```ts
export const ChargeCardWorker = ChargeCard.worker(
  Effect.fn("ChargeCardWorker")(function*(jobs) {
    const sql = yield* SqlClient.SqlClient
    const payments = yield* Payments
    for (const job of jobs) {
      const receipt = yield* payments.charge(job.data.orderId, job.data.amountCents, {
        idempotencyKey: job.id
      })
      yield* sql`update orders set status = 'charged', payment_ref = ${receipt.ref} where id = ${job.data.orderId}`
      // Nested transactions work: this is a SAVEPOINT inside pg-boss's transaction.
      yield* sql.withTransaction(sql`insert into ledger ${sql.insert(receipt.toLedgerRow())}`)
      // Joins the worker's transaction too.
      yield* AuditLog.send({ orderId: job.data.orderId, event: "charged" })
      // Runs after pg-boss's real COMMIT, not before.
      yield* sql.afterCommit(Effect.logInfo("order charged", job.data.orderId))
    }
  }),
  { transactional: true, batchSize: 10, localConcurrency: 2 }
)
// Layer<never, PgBossError, ChargeCard | AuditLog | Payments | SqlClient>
```

If the handler fails, is interrupted, or pg-boss's completion fails, the order
update, the ledger row, the audit job and the completion all roll back, and
pg-boss retries the batch.

### A long-running worker that must not hold a connection

```ts
export const RenderVideoWorker = RenderVideo.worker(
  Effect.fn("RenderVideoWorker")(function*(jobs) {
    const sql = yield* SqlClient.SqlClient
    for (const job of jobs) {
      const url = yield* renderer.render(job.data.videoId) // minutes
      // Own short transaction. Not atomic with the job's completion.
      yield* sql.withTransaction(sql`update videos set url = ${url} where id = ${job.data.videoId}`)
    }
  }),
  { transactional: false }
)
```

### Anything else pg-boss can do

`PgBoss.use` hands you the pg-boss instance and the connection options for the
current transaction, if any. Pass them on so the call joins the transaction.

```ts
const stats = PgBoss.use((boss) => boss.getQueueStats(ChargeCard.queueName))
const cancel = (jobId: JobQueue.JobId) => PgBoss.use((boss, conn) => boss.cancel(ChargeCard.queueName, jobId, conn))
```

## What you can rely on

- One `pg.Pool`. `PgNodeClient` owns it. pg-boss runs on it through an adapter and never opens a pool of its own.
- Rows decode the node-postgres way: `int8` as string, `timestamptz` as `Date`, `bytea` as `Buffer`.
- In a transactional worker the transaction is a real `sql.withTransaction`: nested `withTransaction` uses savepoints, `afterCommit` and Reactivity invalidation run after the real `COMMIT`.
- `send` returns `Option<JobId>`: `None` when a singleton option or the queue's policy suppressed the job.
- Job payloads are encoded with the queue's schema on send and decoded on work. A payload that does not decode fails the batch with `JobQueue.PayloadDecodeError`.
- Closing the layer stops pg-boss gracefully, then interrupts what is still running and rolls back its transactions.
