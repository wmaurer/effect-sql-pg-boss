# @wmaurer/effect-sql-pg-boss

Use [pg-boss](https://github.com/timgit/pg-boss), the Postgres job queue for Node, from an Effect app that uses the native `@effect/sql-pg` client. Your tables and your queues are in the same Postgres database, and they share transactions:

- A job that you send inside `sql.withTransaction` commits or rolls back with your rows.
- In a transactional worker, the rows that the handler writes commit or roll back with the job's completion.

You do not pass a `db` or a `tx` to any call. The library finds the transaction from the fiber, the same way `sql` statements do.

The package is at version 0.x. A minor release can change the API.

## Install

```sh
pnpm add @wmaurer/effect-sql-pg-boss effect @effect/sql-pg pg-boss
```

`effect`, `@effect/sql-pg` and `pg-boss` are peer dependencies:

| Package          | Version  |
| ---------------- | -------- |
| `effect`         | `^4.0.6` |
| `@effect/sql-pg` | `^4.0.6` |
| `pg-boss`        | `~12.38` |

The range for pg-boss is narrow on purpose. `JobQueue.flow` inside your transaction depends on how pg-boss 12.38 joins its SQL statements. The test suite fails if a pg-boss release changes this.

pg-boss needs PostgreSQL 13 or later. The tests run on PostgreSQL 18.

The type declarations of pg-boss 12.38.0 need `@types/pg`, and some of them do not compile. Keep `skipLibCheck: true` in your `tsconfig.json`.

## Get started

These steps add one queue and one worker to an app.

### Declare a queue

A queue is a class. It holds the queue's name, the schema of its payload, and its pg-boss options:

```ts
import { JobQueue } from "@wmaurer/effect-sql-pg-boss";
import { Schema } from "effect";

class SendWelcomeEmail extends JobQueue.Service<SendWelcomeEmail>()("send-welcome-email", {
	payload: Schema.Struct({ userId: Schema.BigInt, email: Schema.String }),
	options: { retryLimit: 5, retryBackoff: true },
}) {}
```

The class is also an Effect service. Code that sends to the queue or works it needs `SendWelcomeEmail` in its context. If you forget to add the queue's layer to the app, you get a type error, not a "queue not found" error at run time.

The payload schema must encode to an object, because pg-boss stores a job's data as a JSON object. The library encodes the payload with `Schema.toCodecJson`, so a `Schema.BigInt` field is stored as a string and decodes back to a `bigint`.

### Send a job inside a transaction

```ts
import { Effect } from "effect";
import { SqlClient } from "effect/sql";

const signUp = Effect.fn("signUp")(function* (email: string) {
	const sql = yield* SqlClient.SqlClient;
	return yield* sql.withTransaction(
		Effect.gen(function* () {
			const [user] = yield* sql<{ readonly id: bigint }>`insert into users ${sql.insert({ email })} returning id`;
			yield* SendWelcomeEmail.send({ userId: user.id, email });
			return user.id;
		}),
	);
});
```

pg-boss inserts the job on the transaction's connection. If the transaction rolls back, the job does not exist. Outside a transaction, `send` opens a transaction of its own.

`send` returns `Option<JobId>`. The result is `None` when pg-boss did not create the job. This happens because of a `singleton` or throttle option on the send, or because of a `singleton` or `stately` queue policy.

### Work the queue

```ts
const WelcomeEmailWorker = SendWelcomeEmail.worker(
	(jobs) =>
		Effect.gen(function* () {
			const sql = yield* SqlClient.SqlClient;
			for (const job of jobs) {
				yield* sql`insert into email_outbox ${sql.insert({ userId: job.data.userId, kind: "welcome" })}`;
			}
		}),
	{ transactional: true },
);
```

The handler gets a non-empty batch of jobs. Each `job.data` is the decoded payload. The whole batch fails or succeeds together.

Every worker must set `transactional`. This worker only writes to the database, so it uses `transactional: true`. Its `insert` commits together with the job's completion. To choose the mode for your own worker, see [Choose a worker mode](#choose-a-worker-mode).

### Build the app layer

Each worker needs its queue's layer. Each queue's layer needs `PgBoss.layer`, and `PgBoss.layer` needs your `PgClient`:

```ts
import { PgClient } from "@effect/sql-pg";
import { PgBoss } from "@wmaurer/effect-sql-pg-boss";
import { Config, Layer, String } from "effect";

const SqlLive = PgClient.layerConfig({
	url: Config.Redacted("DATABASE_URL"),
	transformQueryNames: Config.succeed(String.camelToSnake),
	transformResultNames: Config.succeed(String.snakeToCamel),
});

const AppLive = WelcomeEmailWorker.pipe(
	Layer.provideMerge(SendWelcomeEmail.layer),
	Layer.provideMerge(PgBoss.layer()),
	Layer.provideMerge(SqlLive),
);
```

The examples in this README write camelCase keys, such as `userId`, into snake_case columns, such as `user_id`. `transformQueryNames` changes the names. If your column names are the same as your keys, leave out the two transforms.

When the layer builds, `PgBoss.layer` installs or migrates the pg-boss schema and starts pg-boss. `SendWelcomeEmail.layer` creates the queue if it does not exist. Then the worker starts to fetch jobs.

When the layer closes, the finalizers run in the opposite order. The worker stops fetching and waits for its current batch. Then pg-boss stops, and then the connections close.

Your handlers must use the `SqlClient` that `PgClient.layer` or `PgClient.layerConfig` provides. A transactional worker finds its transaction through that client.

## Send several jobs as a flow

A flow is a group of jobs that pg-boss inserts in one step. A job in a flow can depend on other jobs in the same flow. pg-boss starts a dependent job only after the jobs it depends on complete.

This example and the next ones use these queues:

```ts
class ChargeCard extends JobQueue.Service<ChargeCard>()("charge-card", {
	payload: Schema.Struct({ orderId: Schema.BigInt, amountCents: Schema.Int }),
	options: { retryLimit: 5, retryBackoff: true },
}) {}

class ShipOrder extends JobQueue.Service<ShipOrder>()("ship-order", {
	payload: Schema.Struct({ orderId: Schema.BigInt }),
}) {}

class SendReceipt extends JobQueue.Service<SendReceipt>()("send-receipt", {
	payload: Schema.Struct({ orderId: Schema.BigInt }),
}) {}

class AuditLog extends JobQueue.Service<AuditLog>()("audit-log", {
	payload: Schema.Struct({ orderId: Schema.BigInt, event: Schema.String }),
}) {}
```

This function places an order and sends three jobs in the same transaction:

```ts
const placeOrder = Effect.fn("placeOrder")(function* (cart: { readonly email: string; readonly totalCents: number }) {
	const sql = yield* SqlClient.SqlClient;
	return yield* sql.withTransaction(
		Effect.gen(function* () {
			const [order] = yield* sql<{ readonly id: bigint }>`insert into orders ${sql.insert(cart)} returning id`;
			const jobs = yield* JobQueue.flow({
				charge: ChargeCard.step({ orderId: order.id, amountCents: cart.totalCents }),
				ship: ShipOrder.step({ orderId: order.id }, { dependsOn: ["charge"] }),
				receipt: SendReceipt.step({ orderId: order.id }, { dependsOn: ["charge"], job: { startAfter: 30 } }),
			});
			yield* sql`update orders set charge_job_id = ${jobs.charge} where id = ${order.id}`;
			return order.id;
		}),
	);
});
```

The keys of the record are the names that `dependsOn` refers to. `dependsOn` accepts only the other keys of the same record, so a wrong name is a type error. `flow` returns a record with the same keys, and each value is that job's `JobId`.

If the `update` fails, the order row and all three jobs roll back. Outside a transaction, `flow` opens a transaction of its own, so a flow is always all or nothing.

## Choose a worker mode

The `transactional` option decides what a batch holds while the handler runs, and what commits together.

With `transactional: true`, pg-boss opens a transaction for the batch. The handler runs inside it, and pg-boss commits it together with the batch's completion. These all join that transaction:

- `SqlClient` statements.
- A nested `sql.withTransaction`, which becomes a savepoint.
- `send`, `flow` and `PgBoss.use`.

`sql.afterCommit` callbacks run after the real commit. If the handler fails, dies or is interrupted, everything rolls back, and every job in the batch fails. A job with retries left runs again. The batch holds one connection from your `PgClient` pool for the handler's whole run.

With `transactional: false`, the handler's writes commit on their own. pg-boss completes the jobs afterwards, on its own connections. The batch holds no connection while the handler runs. A failed batch does not undo the writes that the handler already committed.

Use `transactional: true` for handlers that only write to the database. Use `transactional: false` for handlers that call slow external services. A transactional handler that waits a minute for an HTTP call also holds a pool connection for that minute.

In this transactional worker, `Payments` is a service of the app:

```ts
const ChargeCardWorker = ChargeCard.worker(
	Effect.fn("ChargeCardWorker")(function* (jobs) {
		const sql = yield* SqlClient.SqlClient;
		const payments = yield* Payments;
		for (const job of jobs) {
			const receipt = yield* payments.charge(job.data.orderId, job.data.amountCents, { idempotencyKey: job.id });
			yield* sql`update orders set status = 'charged', payment_ref = ${receipt.ref} where id = ${job.data.orderId}`;
			yield* sql.withTransaction(sql`insert into ledger ${sql.insert({ orderId: job.data.orderId, ref: receipt.ref })}`);
			yield* AuditLog.send({ orderId: job.data.orderId, event: "charged" });
			yield* sql.afterCommit(Effect.logInfo("order charged", job.data.orderId));
		}
	}),
	{ transactional: true, batchSize: 10, localConcurrency: 2 },
);
```

In this worker, the order update, the ledger row, the `AuditLog` job and the completion of the batch commit in one transaction. The log line runs only after that commit.

`transactionTimeoutSeconds` is available only with `transactional: true`. With `transactional: false` it is a type error.

The other worker options come from pg-boss's `work`: `batchSize`, `pollingIntervalSeconds`, `localConcurrency`, `localGroupConcurrency`, `groupConcurrency`, `priority`, `orderByCreatedOn` and `heartbeatRefreshSeconds`.

## Drop payloads that do not decode

By default, a job whose payload does not decode fails its whole batch before the handler runs. Every job in the batch fails, and a job with retries left runs again. This is right for a queue with a retry policy.

For a best-effort queue, set `onDecodeError: "drop"`:

```ts
const AuditLogWorker = AuditLog.worker((jobs) => Effect.logInfo("audit events", jobs.length), {
	transactional: false,
	batchSize: 50,
	onDecodeError: "drop",
});
```

The worker logs each job that does not decode at warning level, with its `SchemaError`. The handler gets the jobs that did decode. The dropped jobs complete with the batch. If no job in the batch decodes, the handler does not run, and the batch completes.

## Add a dead-letter queue

pg-boss moves a job to its queue's dead-letter queue when the job fails after its last retry. Declare the dead-letter queue with `deadLetter: true`. Then name its class in the source queue's `deadLetter` option:

```ts
const WorkPayload = Schema.Struct({ workId: Schema.String });

class WorkFailed extends JobQueue.Service<WorkFailed>()("work-failed", {
	payload: WorkPayload,
	deadLetter: true,
}) {}

class Work extends JobQueue.Service<Work>()("work", {
	payload: WorkPayload,
	options: { deadLetter: WorkFailed, retryLimit: 3 },
}) {}
```

pg-boss copies the failed job's payload unchanged, so both queues must use the same payload schema. A different schema is a type error, and so is a class that was not declared with `deadLetter: true`.

`Work.layer` needs `WorkFailed.layer`, because pg-boss requires that the dead-letter queue exists first.

The handler of a dead-letter queue gets a `DeadLetterJob`. Its `source` field describes the job that failed:

| Field        | Type      | Description                                                                                                |
| ------------ | --------- | ---------------------------------------------------------------------------------------------------------- |
| `id`         | `JobId`   | The id of the job that failed.                                                                             |
| `queue`      | `string`  | The queue that the job failed on.                                                                          |
| `output`     | `unknown` | The stored error. Decode it with your own schema.                                                          |
| `retryCount` | `number`  | The number of retries that the job used.                                                                   |
| `rootId`     | `JobId`   | The first job in the dead-letter chain. It is `id` unless the job was sent again from a dead-letter queue. |
| `createdOn`  | `Date`    | When the job that failed was created.                                                                      |

For a handler failure, `output` holds the error that the worker stored, with the pretty-printed `Cause` as its `message`. For an expired job, a lost heartbeat or a shutdown, `output` has the shape that pg-boss uses.

## Change the options of a queue

A queue's layer writes the declared options to the stored queue with pg-boss's `updateQueue` every time it builds. To change a retry limit, change the declaration and restart the app.

If you remove an option from the declaration, the stored queue keeps its old value. pg-boss cannot change `policy` or `partition` after it creates a queue. If the stored value is different from the declaration, the layer fails to build with a `PgBossError`. Without a declaration, the expected values are `"standard"` and `false`.

The layer creates and updates the queue when it builds, never when you send a job. DDL on another connection could wait for locks that your own transaction holds.

## Use other pg-boss APIs

The queue classes cover sending, flows and workers. For the rest of pg-boss, such as cancel, retry, stats and schedules, use `PgBoss.use`. It gives you the pg-boss instance and the connection options for the fiber's transaction:

```ts
const stats = PgBoss.use((boss) => boss.getQueueStats(ChargeCard.queueName));

const cancelCharge = (id: JobQueue.JobId) =>
	PgBoss.use((boss, connection) => boss.cancel(ChargeCard.queueName, id, connection));
```

Pass `connection` as the options of the call. Inside a transaction, `connection` is `{ db }`, and the call joins the transaction. Outside a transaction, it is `{}`, and pg-boss uses its own connections.

Read data outside a transaction when you can. Inside a transaction, pg-boss's read APIs return rows that the native client decoded. Timestamps then arrive as epoch milliseconds, not as `Date` objects.

## Wake workers when a job is sent

By default, a worker finds new jobs when it polls. To wake a worker when a job commits, add `notify: true` to its queue's options, and start pg-boss with `useListenNotify: true`:

```ts
class ShipOrder extends JobQueue.Service<ShipOrder>()("ship-order", {
	payload: Schema.Struct({ orderId: Schema.BigInt }),
	options: { notify: true },
}) {}

const BossLive = PgBoss.layer({ useListenNotify: true });
```

The library opens one extra connection for `LISTEN`. It checks the connection every 10 seconds and replaces it when it is lost. While the wake-ups work, workers of notify queues poll only every 30 seconds, or at their polling interval when that is longer. If the first connection fails, pg-boss logs `listen_notify_unavailable` and polls as usual.

## `PgBoss.layer` options

`PgBoss.layer(options)` accepts the pg-boss constructor options except the connection options. You can set `schema`, which defaults to `pgboss`. pg-boss reaches the database through your `PgClient`, so you do not configure a connection.

These options have a special meaning in this library. `useListenNotify` is a pg-boss option, and the library provides the connection that it needs. The library adds the other four:

| Option                   | Default                               | Description                                                                                                                   |
| ------------------------ | ------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `useListenNotify`        | `false`                               | Wakes notify-queue workers when a job commits. See [Wake workers when a job is sent](#wake-workers-when-a-job-is-sent).       |
| `maintenanceConnections` | `4`                                   | The size of the pool that pg-boss uses for polling, maintenance, and the completions of non-transactional workers.            |
| `connectionReserve`      | `2`                                   | The number of `PgClient` connections that transactional workers cannot use.                                                   |
| `rollbackTimeout`        | `"5 seconds"`                         | How long the rollback of a transactional batch can take before the library gives up on the connection.                        |
| `stop`                   | `{ graceful: true, timeout: 30_000 }` | How pg-boss stops when the layer closes. Handlers can finish their batches until the timeout. After it, they are interrupted. |

`PgBoss.layerConfig` takes the same options as Effect `Config` values.

## Connections

The library uses these database connections:

- One `PgClient` connection for each transactional batch in progress, for the handler's whole run. A transactional worker can run `localConcurrency` batches at once.
- A separate node-postgres pool of `maintenanceConnections` connections for pg-boss's own statements. `@effect/sql-pg` cannot run a script with more than one statement, and pg-boss needs that for its migrations and maintenance. The library builds the pool from your `PgClient` settings. Your app's rows never use it.
- One more connection for `LISTEN`, only when `useListenNotify` is on.

All transactional workers together can use the pool's `maxConnections` minus `connectionReserve`. When `maxConnections` is not set, the pool has 10. A transactional worker layer fails to build when its `localConcurrency` goes over what is left. With the defaults, all transactional workers together can run 8 batches at once.

`ChargeCardWorker` above has `localConcurrency: 2`, so it uses 2 of those 8 connections.

## Errors

- A SQL failure fails with the original `SqlError` from `effect/sql`.
- Any other pg-boss failure fails with `PgBoss.PgBossError`. Its `operation` field names the call, and its `cause` field holds the error. Examples are a bad argument, a failed start, and a worker that does not fit in the pool.
- A payload that does not encode fails `send` or `flow` with a `SchemaError`.

pg-boss's `error` events go to `Effect.logError`, and its `warning` events go to `Effect.logWarning`.

## Limits

- Transactional workers do not work on a `PgClient.makeClient` client. A batch would hold its only connection for the handler's whole run, and every other query of the app would wait. `PgBoss.layer` does not check for this.
- `PgBoss.layer` fails when your `PgClient` config has a custom `stream`, because node-postgres cannot use it.
- A handler cannot store a typed result in the job's output. The worker options do not include `perJobResults` or `includeMetadata`.
- A handler that is stuck in a statement that cannot be interrupted can hold its connection until the native client's cancel timeout.

## Learn more

- [The orders example](https://github.com/wmaurer/effect-sql-pg-boss/tree/main/examples/orders) is a tutorial. It runs a small order pipeline against Postgres in Docker, with a flow, retries and a dead-letter queue.
- [The design record](https://github.com/wmaurer/effect-sql-pg-boss/blob/main/design/README.md) explains how the library works and why. It covers the transaction that pg-boss opens for a worker, the alternatives that were rejected, and the decisions behind the API.
