# @wmaurer/effect-sql-pg-boss

pg-boss for Effect apps that use the native `@effect/sql-pg` client.

- Enqueue jobs and flows inside `sql.withTransaction`. They commit or roll back with your rows.
- Run transactional workers. Your `@effect/sql` writes in the handler commit together with the job's completion.
- Your rows decode the same everywhere (`int8` as `bigint`, timestamps as epoch ms), in request code and in workers, because both run on your one `PgClient`.

## Quickstart

```ts
import { PgClient } from "@effect/sql-pg"
import { JobQueue, JobWorker, PgBoss } from "@wmaurer/effect-sql-pg-boss"
import { Effect, Layer, Redacted, Schema } from "effect"
import { SqlClient } from "effect/sql"

// 1. Declare each queue once. The payload schema is the queue's contract:
//    `send` encodes with it, the worker decodes with it.
export const ShipOrder = JobQueue.make("ship-order", {
  payload: Schema.Struct({ orderId: Schema.BigInt }),
  queue: { retryLimit: 5, retryBackoff: true }
})

// 2. Your SQL client stays the native one. PgBoss reads its connection
//    settings from it; there is nothing else to configure.
const SqlLive = PgClient.layer({ url: Redacted.make(process.env.DATABASE_URL!) })
const BossLive = PgBoss.layer({ schema: "pgboss" }).pipe(Layer.provide(SqlLive))
```

`PgBoss.layer` installs or migrates the pg-boss schema and starts pg-boss. Closing the layer stops pg-boss gracefully: running handlers finish first, then it releases its connections.

## Enqueue inside your transaction

`JobQueue.send` and `JobQueue.flow` run on the connection your `sql` statements would run on at that point. Inside `sql.withTransaction` that is the transaction. Outside one, the job is inserted on its own.

```ts
export const placeOrder = Effect.fn("placeOrder")(function*(customerId: bigint, items: ReadonlyArray<Item>) {
  const sql = yield* SqlClient.SqlClient
  return yield* sql.withTransaction(Effect.gen(function*() {
    const [order] = yield* sql<{ id: bigint }>`
      insert into orders ${sql.insert({ customer_id: customerId })} returning id`
    yield* sql`insert into order_items ${sql.insert(items.map((i) => ({ order_id: order.id, ...i })))}`

    // Same transaction: if anything below fails, the order and both jobs vanish together.
    const jobs = yield* JobQueue.flow([
      JobQueue.step("charge", ChargeCard, { orderId: order.id }),
      JobQueue.step("ship", ShipOrder, { orderId: order.id }, { dependsOn: ["charge"] })
    ])
    yield* sql`update orders set charge_job = ${jobs.charge} where id = ${order.id}`
    return order.id
  }))
})
```

A single job is the same call shape:

```ts
yield* JobQueue.send(WelcomeEmail, { userId: user.id, email }, { startAfter: 60 })
```

`send` returns `Option<JobId>`: pg-boss returns no id when a singleton or throttle policy drops the job.

## Transactional workers

A worker is a layer. By default each job runs inside a real `sql.withTransaction` on your `PgClient`, and pg-boss marks the job completed in that same transaction.

```ts
export const ShipOrderWorker = JobWorker.layer(
  ShipOrder,
  Effect.fn("ShipOrder")(function*(job) {
    const sql = yield* SqlClient.SqlClient
    const [order] = yield* sql<{ status: string }>`select status from orders where id = ${job.data.orderId} for update`
    if (order.status !== "charged") return yield* new NotReady({ orderId: job.data.orderId })

    yield* sql`update inventory set reserved = reserved + 1 where sku in (
      select sku from order_items where order_id = ${job.data.orderId})`
    yield* sql`update orders set status = 'shipping' where id = ${job.data.orderId}`

    // Joins the worker's transaction too: no notification without the shipment.
    yield* JobQueue.send(NotifyCustomer, { orderId: job.data.orderId, kind: "shipped" })

    // Runs after the real COMMIT, when the job is already completed.
    yield* sql.afterCommit(Effect.logInfo("order shipped"))
  }),
  { localConcurrency: 4 }
)
```

If the handler fails, dies, or is interrupted (job timeout, shutdown), the transaction rolls back and pg-boss fails the job, which retries per the queue's policy. Nested `sql.withTransaction` calls become savepoints, as usual.

A handler that calls slow external services should not hold a transaction open. Opt out per worker:

```ts
export const WelcomeEmailWorker = JobWorker.layer(
  WelcomeEmail,
  (job) => Mailer.send(job.data.email, "Welcome!"),
  { transactional: false }
)
```

Wire everything:

```ts
const WorkersLive = Layer.mergeAll(ShipOrderWorker, WelcomeEmailWorker).pipe(
  Layer.provide(BossLive),
  Layer.provide(SqlLive)
)
```

## The rest of pg-boss

Typed queues cover sending and working. For everything else (cancel, retry, stats, schedules) use the pg-boss instance directly. `db` is the connection your `sql` would use right now, so these calls also join your transaction:

```ts
yield* PgBoss.use((boss, { db }) => boss.cancel(ShipOrder.name, jobId, { db }))
```

## Requirements and limits

- The app's `SqlClient` must be the one `PgClient.layer` provides. The library needs `PgClient.PgClient` in context.
- `@effect/sql-pg` cannot yet run multi-statement SQL. pg-boss needs that for its schema migrations and maintenance, so the library keeps a small node-postgres pool for pg-boss's own housekeeping, built from your `PgClient` settings. Your rows never travel through it.
- Each in-flight transactional batch holds one connection from your `PgClient` pool. Size `maxConnections` for app traffic plus the sum of `localConcurrency` over transactional workers.
- A custom `stream` in the `PgClient` config cannot be copied to the housekeeping pool. `PgBoss.layer` fails with a clear error then.
