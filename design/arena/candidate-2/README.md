# @wmaurer/effect-sql-pg-boss

pg-boss for Effect v4. Jobs and your own tables share one database and one connection pool, so:

- A job you enqueue inside `sql.withTransaction` commits or rolls back with your rows.
- A worker's `SqlClient` writes inside the transaction pg-boss uses to complete the job. Your rows and the job's completion commit together.

You keep writing `sql` templates, `SqlSchema`, `Model`, `Migrator` and `withTransaction` as before. The package adds a node-postgres backed `SqlClient`, typed queues, and three functions: `Boss.send`, `Boss.flow` and `Boss.work`.

## Install

```sh
pnpm add @wmaurer/effect-sql-pg-boss effect pg pg-boss
```

`effect`, `pg` and `pg-boss` are peer dependencies.

## Quickstart

```ts
import { Config, Effect, Layer, Schema } from "effect"
import { NodeRuntime } from "@effect/platform-node"
import { SqlClient } from "effect/sql"
import { Boss, NodePgClient, Queue } from "@wmaurer/effect-sql-pg-boss"

// 1. Queues are values. The schema is the payload type everywhere.
export const OrderId = Schema.String.pipe(Schema.brand("OrderId"))

export const ChargeCard = Queue.make("charge-card", Schema.Struct({ orderId: OrderId }), {
  retry: { limit: 5, delay: "10 seconds", backoff: true }
})
export const SendReceipt = Queue.make("send-receipt", Schema.Struct({ orderId: OrderId, email: Schema.String }))

// 2. One pool. This layer provides NodePgClient and SqlClient (same value).
const AppDb = NodePgClient.layerConfig(Config.all({
  url: Config.Redacted("DATABASE_URL"),
  pool: Config.succeed({ max: 20 })
}))

// 3. Start pg-boss on that pool. List every queue the app uses.
const Queues = Boss.layer({ queues: [ChargeCard, SendReceipt] })

// 4. Workers are scoped effects, so they are layers.
const Workers = Layer.mergeAll(
  Layer.effectDiscard(chargeWorker),
  Layer.effectDiscard(receiptWorker)
)

const Main = Workers.pipe(
  Layer.provide(Queues),
  Layer.provideMerge(AppDb)
)

Layer.launch(Main).pipe(NodeRuntime.runMain)
```

`Boss.layer` starts pg-boss (installing or migrating its schema) and creates the listed queues. On shutdown, workers finish their current batch, pg-boss stops, then the pool closes.

If you forget to list a queue, the program does not compile where that queue is used. `Boss.send(SendReceipt, ...)` requires `Boss.Registered<"send-receipt">`, and only `Boss.layer` provides it.

## Call site 1: enqueue atomically with your own rows

```ts
import { Effect } from "effect"
import { SqlClient } from "effect/sql"
import { Boss, Flow } from "@wmaurer/effect-sql-pg-boss"

export const placeOrder = Effect.fn("placeOrder")(function*(input: { readonly email: string }) {
  const sql = yield* SqlClient.SqlClient

  return yield* sql.withTransaction(Effect.gen(function*() {
    const [order] = yield* sql<{ id: string }>`
      insert into orders ${sql.insert({ email: input.email, status: "new" })} returning id`
    const orderId = OrderId.make(order.id)

    // A job graph: send the receipt only after the charge completes.
    const charge = Flow.node("charge", ChargeCard, { orderId })
    const receipt = Flow.node("receipt", SendReceipt, { orderId, email: input.email }, { dependsOn: [charge] })

    const ids = yield* Boss.flow(receipt)   // { charge: JobId, receipt: JobId }
    return { orderId, chargeJobId: ids.charge }
  }))
})
```

If the insert fails, or anything after `Boss.flow` fails, no order row and no job exist. If the transaction commits, both do. `Boss.flow` and `Boss.send` have no `db` or `tx` argument. They run on the connection of the calling fiber, which inside `withTransaction` is the transaction.

Outside a transaction the same calls autocommit:

```ts
yield* Boss.send(SendReceipt, { orderId, email }, { startAfter: "5 minutes", singletonKey: String(orderId) })
```

## Call site 2: write your own tables atomically with job completion

```ts
import { Effect } from "effect"
import { SqlClient } from "effect/sql"
import { Boss } from "@wmaurer/effect-sql-pg-boss"

export const chargeWorker = Boss.work(
  ChargeCard,
  { batchSize: 1, concurrency: 4 },
  Effect.fn("chargeWorker")(function*([job]) {
    // This SqlClient is bound to the transaction pg-boss opened for this batch.
    const sql = yield* SqlClient.SqlClient
    const gateway = yield* PaymentGateway

    const receiptRef = yield* gateway.charge(job.data.orderId)   // outside the database: at-least-once
    yield* sql`insert into payments ${sql.insert({ order_id: job.data.orderId, ref: receiptRef })}`
    yield* sql`update orders set status = 'paid' where id = ${job.data.orderId}`

    // Enqueued in the same transaction: exists only if the batch commits.
    yield* Boss.send(SendReceipt, { orderId: job.data.orderId, email: "..." })
  })
)
```

When the handler succeeds, pg-boss marks the job completed in the same transaction and commits. The payment row, the order update, the receipt job and the completion are one commit.

When the handler fails or dies, the transaction rolls back and pg-boss fails the job. It retries per the queue's `retry` policy. None of the handler's writes persist.

If the claim was lost (the job expired and another worker took it), the completion is a no-op, pg-boss rolls back, and your writes are discarded too.

`sql.withTransaction` inside the handler works. It becomes a savepoint:

```ts
yield* sql.withTransaction(riskyInsert).pipe(Effect.catchTag("SqlError", () => Effect.log("skipped")))
```

A failed statement inside a savepoint rolls back to it and the batch continues.

A handler that only calls an API and touches no tables should not hold a connection while it waits:

```ts
export const receiptWorker = Boss.work(SendReceipt, { atomic: false }, ([job]) => Mailer.send(job.data))
```

## Call site 3: what is left of pg-boss

Everything this package does not wrap is reachable with the matching connection:

```ts
// Cron, cancel, lookups. `db` is the caller's current connection.
yield* Boss.use((boss, db) => boss.schedule("charge-card", "0 * * * *", { orderId: "..." }))
yield* Boss.use((boss, db) => boss.cancel("charge-card", jobId, { db }))
```

`Boss.use` is the only place pg-boss types appear. Inside a worker, `db` is the exact transaction object pg-boss gave the handler, which settle calls such as `cancel` need.

## Errors

- A failing statement fails with `SqlError`, same as any other `sql` call. Typed reasons such as `UniqueViolation` are preserved.
- `BossError` has a `kind`: `Encode`, `Decode`, `InvalidFlow`, `Driver`, `Lifecycle`.
- A job whose stored payload does not decode is failed without running your handler.

## What to know

- `NodePgClient` replaces `@effect/sql-pg`'s `PgClient` for this app. The native client sends every statement through the extended protocol, cannot run pg-boss's multi-statement SQL, and cannot share a pool. Application SQL does not change. Decoding follows node-postgres: `int8` and `numeric` arrive as strings, timestamps as `Date`. Decode with `Schema`.
- Size `pool.max` for app transactions plus transactional workers. Each running batch holds one connection until its handler ends.
- `afterCommit` hooks and Reactivity invalidation inside a worker run when the handler's savepoint is released, which is just before pg-boss commits.
- A worker with `batchSize > 1` fails the whole batch if one job fails to decode or the handler fails. Use `batchSize: 1` unless you want batch semantics.
- pg-boss polls. LISTEN/NOTIFY wake-ups are off in this version.

## Module map

| Module         | Purpose                                                            |
| -------------- | ------------------------------------------------------------------ |
| `Queue`        | `Queue.make(name, schema, options)`, `Job`, `JobId`, `SendOptions` |
| `Flow`         | `Flow.node` for job graphs. Nodes depend on nodes by value         |
| `Boss`         | `layer`, `send`, `flow`, `work`, `use`, `BossError`, `Registered`  |
| `NodePgClient` | `layer`, `layerConfig`, `layerFromPool`, the `SqlClient` over `pg` |
