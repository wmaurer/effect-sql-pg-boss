# Rationale: candidate 1, "one transaction engine"

## Problem

An app keeps its tables and its pg-boss queues in one Postgres database. It wants two atomic operations: send jobs (including `flow`) inside `sql.withTransaction`, and write app tables inside a pg-boss transactional worker so they commit with the job's completion. Phase A fixed the constraints. pg-boss's constructor `db` must run multi-statement text, so it must be node-postgres-like. `boss.flow(..., { db })` sends `;\n`-joined text without values. pg-boss fences a handler's settles by the identity of the `db` object. Driver errors must keep `code` and `constraint`. The native `@effect/sql-pg` client cannot run multi-statement text, cannot share a pool, and decodes differently from node-postgres. Providing `sql.transactionService` as `[conn, 0]` by hand breaks nested `withTransaction` and runs `afterCommit` too early. What makes the shape non-obvious is that pg-boss opens and settles worker transactions from promise code outside any fiber, while `effect/sql` only knows transactions that `withTransaction` opened.

## Usage (caller's view)

See `README.md`. The call sites there are compiled in `examples/usage.ts`, with type-level assertions on the inferred requirements. In short:

```ts
class ChargeCard extends JobQueue.Service<ChargeCard>()("charge-card", {
  payload: Schema.Struct({ orderId: Schema.String, amountCents: Schema.Int })
}) {}

// direction 1
sql.withTransaction(Effect.gen(function*() {
  const [order] = yield* sql`insert into orders ... returning id`
  yield* JobQueue.flow({
    charge: ChargeCard.step({ orderId: order.id, amountCents }),
    ship: ShipOrder.step({ orderId: order.id }, { dependsOn: ["charge"] })
  })
}))

// direction 2
ChargeCard.worker(Effect.fn(function*(jobs) {
  const sql = yield* SqlClient.SqlClient
  yield* sql`update orders set status = 'charged' where id = ${jobs[0].data.orderId}`
  yield* AuditLog.send({ ... })
}), { transactional: true })

// wiring
WorkersLive.pipe(Layer.provideMerge(QueuesLive), Layer.provideMerge(PgBoss.layer()), Layer.provideMerge(PgNodeClient.layerConfig(...)))
```

No call site passes a `db`, a `tx`, or a client. The transaction comes from the fiber, the same way it does for `sql` statements.

## Shape

**Data structures first.** There is one object: `NodePgConnection` (`src/internal/connection.ts`). It is the `effect/sql` `Connection` and the pg-boss `Db` at the same time. Every other decision follows from that.

- "The pg-boss `db` for the current transaction" is the connection in `sql.transactionService`. You find it with an `instanceof`, not a map lookup. `PgBoss.use` and every `JobQueue` operation get it through one function, so the joining policy has one owner (per minimize-reader-load).
- The `tx` pg-boss gives a transactional handler is that same object. A settle the handler makes stays fenced by identity, by construction. There is no wrapper cache to keep in sync (per separate-before-serializing-shared-state: no second copy of the state exists).

**One transaction engine.** pg-boss's constructor `db` is an adapter over the `PgNodeClient` pool (`src/internal/bossDatabase.ts`). Its `beginTransaction()` forks a fiber into the PgBoss layer's `FiberSet`. That fiber runs `sql.withTransaction` and parks inside it on a `Deferred<"commit" | "rollback">`. `commit()` and `rollback()` complete the deferred and wait for the fiber to exit. The first write wins, so `rollback()` is idempotent and `commit()` after a settle rejects, as pg-boss's `TransactionHandle` contract asks (per make-operations-idempotent). The handler fiber joins with a context delta: the services the transaction fiber has that the layer's base context lacks. These are the transaction level, its savepoint semaphore, the deferring Reactivity, and the span. They are merged over the handler's own context. The consequences:

- Nested `withTransaction` in a handler is a SAVEPOINT, because the semaphore is in the delta. Join-by-hand breaks here.
- `afterCommit` and Reactivity invalidation run after pg-boss's real COMMIT.
- The handler keeps the app's `SqlClient` instance. Repositories that captured `SqlClient` when their layer was built join the worker's transaction automatically. A per-batch client does not give this.

**Measured, not assumed.** `prototype/bridge.ts` runs this mechanism against Postgres 17 with pg-boss 12.38.0 and `effect` from the repo sources. A transactional handler wrote an outer row, a nested savepoint row, and a nested savepoint that rolled back, then sent a job on `tx`. The commit kept `["nested","outer"]` and the sent job, and the batch's job ended `completed`. `afterCommit` read the job from another pool connection and saw `completed`. The failing handler left 0 rows and 0 sent jobs, and the job ended `failed`. `handler conn === tx` printed `true` both times. Direction 1, `boss.flow(..., { db: conn })` inside a rolled-back `withTransaction`, left 0 rows and 0 jobs.

**Typed queues as services.** `JobQueue.Service<Self>()(name, { payload, options })` declares name, schema and pg-boss queue options in one place. The class is a `Context` service whose `layer` converges the queue: it creates the queue if missing and updates it if changed. `send`, `step` and `worker` require the class, so code that uses a queue whose layer is missing fails to compile (asserted in `examples/usage.ts`). This encodes the invariant in types, not in a hand-synced `queues: [...]` list (per type-system-discipline). Payloads are encoded on send and decoded on work with the queue's schema. That parsing happens at the boundary, and handlers get `Job<A>` with typed `data` (per boundary-discipline). `PayloadSchema` requires an object-encoded, service-free schema, because pg-boss requires object `data`.

**Illegal states made unrepresentable.** `WorkerOptions.transactional` is required. `transactionTimeoutSeconds` exists only on its `true` branch, and the `@ts-expect-error` in the examples proves it. `perJobResults` and `includeMetadata` are not offered: the first cannot combine with `transactional`, the second changes `Job`'s shape. In `flow`, `dependsOn` accepts only the other keys of the same record, which rejects a typo and a self-reference at compile time (both asserted). The handler takes a non-empty tuple. `send` returns `Option<JobId>`, because pg-boss returns `null` when a singleton option or queue policy suppresses the job.

**Interface depth.** The public surface is `PgNodeClient.{layer, layerConfig, make, makeCompiler}`, `PgBoss.{layer, use, PgBossError}`, and `JobQueue.{Service, flow, Job, JobId, WorkerOptions, ...}`. Behind it sit the pool sharing, the identity fencing, the fiber bridge for pg-boss transactions, the context join, encoding and decoding, worker lifecycle (`offWork` on scope close, abort signal to interruption), and error preservation. The caller learns none of it. The pool and the bridge are in `src/internal/`, which is blocked from outside imports (`"./internal/*": null` in `package.json`).

**Why the main `SqlClient` is node-postgres, from this package.** pg-boss needs a node-postgres-like `db`, and `flow` needs the simple protocol on the transaction's connection. A shared pool needs a client that accepts a pool, and the native client cannot. With one driver, a row decodes the same way in a request handler and in a worker. `PgBoss.layer` requires `PgNodeClient`, not `SqlClient`, so wiring pg-boss to a client that cannot share its pool does not compile.

**Lifecycle.** `PgBoss.layer` starts pg-boss in `acquireRelease` and stops it gracefully on release. Worker layers are built after the boss layer, so they are released first: `offWork({ wait: true })`, then their handler fibers are interrupted. Then pg-boss stops, then the transaction fibers are interrupted and roll back, then `PgNodeClient` ends the pool. A handler fiber never outlives the transaction it joined.

**What it does not do.** It does not wrap pg-boss's whole API. Schedules, stats, cancel and the rest go through `PgBoss.use`. It does not support the native `@effect/sql-pg` client. It does not make `afterCommit` durable.

## Synthesis decision

Left for arena.

## Tradeoffs accepted

- We accept one class per queue in exchange for compile-time proof that every queue used was registered.
- We accept that `transactional` must be stated at every `worker` call in exchange for no silent non-atomic default. pg-boss defaults to `false`, and this package exists for `true`.
- We accept `PgBoss.use` exposing pg-boss's own types as an escape hatch in exchange for not mirroring a large, fast-moving API. It still joins the transaction for you.
- We accept a batch-level handler in exchange for honesty about the commit unit. One failing job rolls back its whole batch.
- We accept node-postgres decoding (`int8` as string, `Date`, `Buffer`) in exchange for one decoder across both directions. This looks like a regression next to the native client, and it is deliberate.
- We accept reading `Context.mapUnsafe` to compute the join delta in exchange for not depending on `effect/sql`'s unexported semaphore key or on the upstream join fix.
- We accept pg-boss's custom-`db` limits in exchange for one pool. pg-boss gives no pool-headroom warning, and `useListenNotify` needs the adapter's `listen`, which is still a TODO.
- We accept that a transactional batch holds a pool connection for the handler's whole run. `transactional: false` exists for long handlers.

## Alternatives considered

- **Borrowed per-batch `SqlClient`** (`SqlClient.make` over `tx.executeSql`, with BEGIN mapped to SAVEPOINT, as in the grounding's `borrowed.ts`). It exposes less in the API, but it hides a correctness gap. `afterCommit` and invalidation fire at RELEASE SAVEPOINT, before the real COMMIT. A service that captured the app's `SqlClient` at layer build does not join the transaction, so a repository silently writes outside it. It also builds a client per batch.
- **Own the transaction ourselves.** A non-transactional pg-boss worker would run `withTransaction(handler *> boss.complete(..., { db }))`. The bridge would go away, but pg-boss's claim fencing, timeout GUC and shutdown-abort rules would be copied into this package. That is the same policy in two places, and it drifts as pg-boss changes. pg-boss would also run a second `complete` on the pool after every batch.
- **Native `@effect/sql-pg` as the app client.** Here `flow` is either split on `;\n` (this depends on pg-boss internals) or waits for an upstream `PgClient.simple`. You get two pools and two decoders, so a `SqlSchema` can pass in one direction and fail in the other.
- **An explicit `tx` / `db` parameter** (`send(data, { tx })`, `handler(jobs, tx)`). Every caller must thread it, and a forgotten one is a silent loss of atomicity. Repositories do not take it.
- **Join by providing `[conn, 0]` as `transactionService`.** This is the smallest change, but nested `withTransaction` dies on the missing semaphore and `afterCommit` runs immediately (`SqlClient.ts:504-509, 676`). It works only after an upstream fix.
- **Queues as plain values plus `PgBoss.layer({ queues: [...] })`.** The surface is smaller, but the list must be kept in sync by hand. A forgotten queue becomes pg-boss's runtime "queue not found".

## Implementation reconciliation

None yet.

## Open questions and risks

- Should `transactional` default to `true` instead of being required?
- Is `Option<JobId>` from `send` worth its friction, or should the queue's policy pick the return type at the type level?
- Should handlers be able to return an output that pg-boss stores, so flow children can read a parent's result?
- Should `PgNodeClient` accept a caller-owned `pg.Pool` (`fromPool`)?
- Should `JobQueue` offer `schedule(cron, data)` as a first-class method instead of `PgBoss.use`?
- Risk: a handler that forks a fiber which outlives the handler can run statements on the connection after pg-boss commits. This is the same hazard as with any `withTransaction`. Should the worker run the handler in a child scope that interrupts such fibers?
- Risk: a transactional worker whose handler is given a different `SqlClient` would silently not join. The sketch dies at layer build. Is that strict enough?
- Risk: the prototype checked commit, rollback, a nested savepoint, `afterCommit` order, and identity. It did not check a hanging rollback (`destroy`), shutdown mid-handler, or `localConcurrency > 1` against the pool size.

## Next implementation step

Build `NodePgConnection` and `PgNodeClient.make` from the pre-#7426 node-postgres `ConnectionImpl`. Then move `prototype/bridge.ts` into `BossDatabase.make`, behind a vitest suite that asserts the prototype's five outcomes against Postgres.
