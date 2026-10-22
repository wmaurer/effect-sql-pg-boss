# Orders example

A small order pipeline on `@wmaurer/effect-sql-pg-boss`. A CLI places orders, and a worker charges and ships them. It shows the two things the library is for:

1. Jobs are enqueued inside the app's `sql.withTransaction`, so they commit or roll back with the app's rows.
2. A transactional worker's app writes commit in the same transaction as the job's completion.

## The parts

- `src/queues.ts` declares three queues. `charge-card` retries twice, one second apart, and then moves the job to the dead-letter queue `charge-failed`. `ship-order` has no options.
- `src/app.ts` builds the `PgClient`, creates the `orders` table if it is missing, starts pg-boss and creates the queues. There is no separate setup step.
- `src/workers.ts` has one transactional worker per queue. Each one updates the order's `status`.
- `src/main.ts` is the CLI, with the commands `worker`, `order` and `status`.

## Run it

You need Docker and Node 22.18 or later. Node runs the TypeScript files directly. Install the workspace from the repository root with `pnpm install`, then work in this folder. The outputs below leave out the two lines pnpm prints before each command.

Start Postgres 18 on port 54320:

```sh
pnpm db:up
```

The CLI connects to `postgres://postgres:postgres@localhost:54320/orders`. Set `DATABASE_URL` to use a different database.

In terminal A, start the worker:

```sh
pnpm orders worker
```

In terminal B, place an order:

```sh
pnpm orders order 1999
```

```
order 1 inserted
  charge-card job 4c952168-1576-41ec-8022-d872e72efbf7
  ship-order job  43c65b5f-22ee-4414-8e59-1aefde67d348 (runs after charge-card completes)
committed: the order and both jobs are now visible to the worker
```

The order row and the two jobs are inserted in one transaction. `JobQueue.flow` creates the `ship-order` job blocked until the `charge-card` job completes. Terminal A shows the charge, and then the shipment a few seconds later:

```
[21:38:34.963] INFO (#17): charge-card: order 1 set to charged, not committed yet
[21:38:34.970] INFO (#15): charge-card: committed order 1 together with job 4c952168-1576-41ec-8022-d872e72efbf7
[21:38:36.974] INFO (#37): ship-order: order 1 shipped
```

The second line comes from `sql.afterCommit`. It runs only after pg-boss commits the worker's transaction, which holds both the `update orders` and the completion of the job.

Place an order whose card is declined:

```sh
pnpm orders order 500 --decline
```

The charge worker fails with a `CardDeclined` error, and the transaction rolls back. pg-boss tries the job three times. Then it moves the job to `charge-failed`, whose worker marks the order `payment_failed`:

```
[21:38:36.966] WARN (#30): charge-card: card declined for order 2 (attempt 1)
[21:38:38.962] WARN (#45): charge-card: card declined for order 2 (attempt 2)
[21:38:40.964] WARN (#54): charge-card: card declined for order 2 (attempt 3)
[21:38:42.965] ERROR (#63): charge-failed: order 2 set to payment_failed; charge-card job b609ff3b-8ed4-4b29-b770-704dcd3f5538 failed after 2 retries with "CardDeclined: card declined for order 2"
```

Place an order and roll it back:

```sh
pnpm orders order 700 --rollback
```

```
order 3 inserted
  charge-card job 88cc287f-582a-403b-91a1-ef8feff92b99
  ship-order job  0b952c23-0986-41e8-9dc9-4ecd04a01f23 (runs after charge-card completes)
rolled back: orders has 0 rows for order 3, pgboss.job has 0 rows for its jobs
```

The command inserts the order and enqueues both jobs, and then fails the transaction. Neither the order nor its jobs exist afterwards, and the worker never sees them. The id 3 is used up, because Postgres does not roll back a sequence.

Look at the result:

```sh
pnpm orders status
```

```
order           amount_cents    status
1               1999            shipped
2               500             payment_failed

queue           state           jobs
charge-card     completed       1
charge-card     failed          1
charge-failed   completed       1
ship-order      blocked         1
ship-order      completed       1
```

The blocked `ship-order` job belongs to order 2. pg-boss leaves a dependent job blocked when its parent fails. Cancel it with `boss.cancel` through `PgBoss.use` if it should not stay.

Stop the worker with Ctrl-C in terminal A. The workers stop fetching, pg-boss stops, and the pool closes, in that order, through the layers' finalizers:

```
[21:38:47.345] INFO (#2): worker: stopped, every layer closed
 ELIFECYCLE  Command failed with exit code 130.
```

The last line is pnpm's. An interrupted process exits with code 130, and pnpm reports any nonzero exit code as a failure.

Remove the container and its data:

```sh
pnpm db:down
```
