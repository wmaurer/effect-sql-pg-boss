# Rationale: candidate 3 (native `PgClient` stays the app's client)

## Problem

The app keeps its tables and a pg-boss queue in one Postgres database and wants two atomic operations: enqueue jobs or flows inside `sql.withTransaction`, and write app tables inside a pg-boss transactional worker so the writes commit with the job's completion. The assigned direction is that the app's only `SqlClient` is the native `@effect/sql-pg` `PgClient`, so rows decode with native types (`int8` as `bigint`, timestamps as epoch ms) in request code and in workers alike.

The constraints that shape the design (grounding plus this session's reading):

- The native client cannot run multi-statement text (`PgProtocol.ts` has no simple-protocol encoder). pg-boss needs that on its constructor `db` for migrations, maintenance and BAM, and on `{ db }` paths only for `flow`.
- `PgClient.make` builds its pool privately. There is no `fromPool`, so the library cannot hand pg-boss a native pool, and pg-boss cannot use its built-in node-postgres `Db` for worker transactions on the native client.
- pg-boss calls `this.db.beginTransaction()` on the constructor `db` for transactional workers (`manager.js:666`). The handler gets that handle's `db` (`untracked` returns the same object for a custom db, `activity.js:7`). Settles are fenced by that object's identity (`manager.js:671`).
- `withTransaction` puts three things into the body's context: the `[conn, depth]` level, a per-client savepoint semaphore under a private key, and a deferred `Reactivity` (`SqlClient.ts:640-648`). If you provide the level yourself, you cannot supply the semaphore. That is the `:676` defect.
- Native driver errors keep SQLSTATE fields on the `SqlError` reason's `cause` (`PgConnection.ts:2466-2472`, `Object.assign(new Error(..), fields)`). pg-boss reads `code`, `constraint` and `detail` from them.

## Usage (caller's view)

See `README.md`, and `example/app.ts`, which typechecks the same call sites. In short:

```ts
const ShipOrder = JobQueue.make("ship-order", { payload: Schema.Struct({ orderId: Schema.BigInt }) })
const BossLive = PgBoss.layer({ schema: "pgboss" }).pipe(Layer.provide(SqlLive))   // needs PgClient

// direction 1: inside the app's transaction
sql.withTransaction(Effect.gen(function*() {
  const [order] = yield* sql<{ id: bigint }>`insert into orders ... returning id`
  const jobs = yield* JobQueue.flow([
    JobQueue.step("charge", ChargeCard, { orderId: order.id }),
    JobQueue.step("ship", ShipOrder, { orderId: order.id }, { dependsOn: ["charge"] })
  ])                                   // jobs.charge, jobs.ship : JobId
}))

// direction 2: inside pg-boss's transaction
JobWorker.layer(ShipOrder, (job) => Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient   // the same native client, now in the job's transaction
  yield* sql`update orders set status = 'shipping' where id = ${job.data.orderId}`
  yield* JobQueue.send(NotifyCustomer, { ... })   // joins the job's transaction
  yield* sql.afterCommit(...)                     // runs after the real COMMIT
}))

// everything else in pg-boss
PgBoss.use((boss, { db }) => boss.cancel(ShipOrder.name, id, { db }))
```

## Shape

**Data structures first.** There are three runtime structures. Everything else is derived from them.

1. `JobQueue<Name, S>`: `{ name, payload: Schema, queue: QueueOptions }`. It is the single source of truth for a queue. `send` encodes with `Schema.toCodecJson(payload)` and the worker decodes with the same codec, so `bigint` and `Date` fields survive the JSON column. The handler sees `Job<A>` with the decoded `data` and a branded `JobId`. pg-boss's raw `Job` never reaches the caller (per boundary-discipline).
2. `byConnection: WeakMap<Connection, Db>` is the one owner of the rule "pg-boss runs where `sql` would run" (`Runtime.currentDb`). Inside a transaction, the key is the transaction's `Connection` object. The native client creates a fresh one per transaction (`Effect.map(pool.reserve, makeConnection)`), and all savepoint levels share it. Outside a transaction, the db is built over the fiber's context and runs on the pool. A worker's parked transaction registers its db first, so every library call made in the handler passes pg-boss the exact object it fences on. No caller ever threads a `tx` or a `db` (per separate-before-serializing-shared-state: one writer per key, no shared mutable connection state).
3. The **parked transaction** (`internal/database.ts`). `beginTransaction()` forks `sql.withTransaction(capture context; signal ready; await outcome)` into the layer's FiberSet. `commit` resolves the outcome and awaits the fiber. `rollback` fails it once and never rejects. The worker runs the handler with the worker layer's context merged with the captured transaction context.

The parked transaction is the load-bearing decision. Because the context comes from `withTransaction` itself, the handler's nested `withTransaction` finds its semaphore, `afterCommit` and `Reactivity` invalidation run after the real COMMIT, and the transaction is native. No borrowed second `SqlClient` exists anywhere. Measured in `proto/parked.ts` (output in `proto/parked.out`, Postgres 17.11, pg-boss 12.38.0). The handler wrote an `int8` row that came back as `bigint` and ran a nested `withTransaction` as a savepoint. Its `send` joined the job's transaction. pg-boss's `complete` ran on the native connection with `uuid[]` parameters. The `afterCommit` callback, querying from another connection, saw the job already `completed`. A failing handler left no app row, no follow-up job, and the job `failed`. Enqueue in an app transaction (send plus flow) committed and rolled back atomically, and a bare flow inserted both jobs.

**The constructor `db`** routes pg-boss's own statements (migrations, maintenance, fetch, non-transactional settles) to a small node-postgres **housekeeping** pool, and `beginTransaction` to the parked transaction. The pool is built from `PgClient.config`, so the caller configures nothing. App rows never travel through it. pg-boss's internals keep the node-postgres row types they were written against.

**Errors.** The library has one Promise boundary, `Runtime.call`. The adapter rejects with the driver `Error` (`SqlError.reason.cause`), so pg-boss's SQLSTATE branches work. It remembers the original `SqlError` in a WeakMap, so the caller's Effect fails with the native `SqlError` (`UniqueViolation` stays matchable) rather than a re-wrapped copy. Non-SQL failures become `PgBossError`. Encode and decode failures are `SchemaError`.

**Types that carry invariants.** `PgBoss.layer` requires `PgClient.PgClient`, not `SqlClient`: the direction is in the type, and `config` is needed to build the housekeeping pool. `SendOptions` omits `db`, so a caller cannot pass a different wrapper and break fencing. `flow` returns `{ [ref]: JobId }` computed from the steps' literal refs. An unknown ref and a payload that does not match the schema are both compile errors (`example/app.ts`, `@ts-expect-error` checks).

**Interface depth.** The public surface is `JobQueue.make/send/step/flow`, `JobWorker.layer`, and `PgBoss.layer/layerConfig/use`. Behind it the library hides context-based connection routing, the parked transaction lifecycle, settle-identity fencing, the flow-splitting stopgap, the housekeeping pool and its config mapping, SQLSTATE error preservation, schema encode and decode, lazy queue creation, and graceful shutdown ordering. The caller learns none of it. `PgBoss.use` is the one deliberate leak. It exposes the raw pg-boss instance so the library does not wrap about 80 methods as pass-throughs (per laziness-protocol). It still hands over the correct `db`.

**What it deliberately does not do.** It does not implement a `SqlClient`. It does not provide `transactionService` itself. It does not support `perJobResults`. It does not update options of an existing queue. It does not wrap pg-boss admin APIs.

**Shrink path.** Upstream may add `PgClient.simple`. Then `runParameterless` calls it instead of splitting on `";\n"`, `bossDb.executeSql` goes native, and `internal/housekeeping.ts` and the `pg` dependency are deleted. `Options.housekeeping` becomes a documented no-op, which does not break callers. Upstream may also fix the `SqlClient.ts:676` semaphore. Nothing changes then, because this design never provides the level itself. The parked transaction is better than the join route even after the fix, since the join route still fires `afterCommit` immediately (`:504-509`).

## Synthesis decision

_Filled in by arena._

## Tradeoffs accepted

- We accept two pools and two drivers in the process (native for the app, plus a 4-connection node-postgres pool for pg-boss housekeeping). In exchange, app rows decode one way everywhere and the library owns no `SqlClient` implementation. A node-postgres main client would need one pool and no stopgap, but the library would then ship and maintain a 300 to 800 line driver client (its own compiler for `sql.json`, error mapping, streams), and the app would lose the native decoders and multiplexing.
- We accept that each in-flight transactional batch holds one connection from the app's `PgClient` pool for the handler's duration. That couples worker concurrency to app pool sizing, which is documented. In exchange, the worker's transaction is a real native `withTransaction`.
- We accept the `flow` stopgap, which splits on pg-boss's `";\n"` join (internal to pg-boss 12.38, `manager.js:1840-1843`), pinned by a `~12.38` peer range and a test. In exchange, flows work on the native client today. It is deleted when `PgClient.simple` lands.
- We accept losing pg-boss's built-in `Db` features for a custom db: no pool-headroom warning, and pg-boss does not open or close the pool. The layer scope owns both pools.
- We accept handler code running in a fiber other than the one that owns the transaction, because pg-boss's Promise callbacks sit in between. The context is identical, so `sql`, savepoints and `afterCommit` behave as in-fiber. Tracing nests the handler under the `sql.transaction` span, not the other way round.
- We accept lazy queue creation on first `send` or worker start, memoized per layer instance, in place of a hand-maintained `queues: [...]` list in the layer options.
- We accept `batchSize > 1` as a unit of atomicity, so one failing job fails the batch. This matches pg-boss's transactional semantics without adding `perJobResults` surface.

## Alternatives considered

- **Node-postgres `SqlClient` as the app's main client** (the opposite direction). The interface is just as small and it needs one shared pool. But the library hides less: it must own a driver client, and callers inherit node-postgres row types (`int8` as string, `Date`, `Buffer`) that native-typed `SqlSchema` code rejects. It also still needs either the parked-transaction trick or a borrowed client for workers. It lost on the assigned direction and on maintenance weight.
- **Borrowed second `SqlClient` inside the worker** (`SqlClient.make` over pg-boss's `tx.executeSql`, transactions mapped to `SAVEPOINT`, as in grounding `borrowed.ts`). The handler sees a different client whose rows decode as node-postgres types, and `afterCommit` fires at `RELEASE SAVEPOINT` before pg-boss's real COMMIT. Repositories built on the app's `PgClient` tag would need re-providing. It exposes more and breaks more invariants than the parked transaction.
- **Joining via a self-provided `transactionService` `[conn, 0]`** on a `sql.reserve`d native connection with manual BEGIN and COMMIT. It dies on nested `withTransaction` today (`:676`), and even after the fix it runs `afterCommit` immediately. The parked transaction dominates it, so the upstream fix buys nothing here.
- **Native client for every pg-boss statement, splitting all parameterless scripts on `;`.** Migrations contain dollar-quoted function bodies with inner `;`, so a split corrupts them. The only safe split is `flow`'s fixed separator.
- **A full Effect façade over pg-boss** (`PgBoss.cancel`, `.retry`, `.getQueueStats`, ...). That would be about 80 pass-through methods, a shallow module, and a hand-synced list against pg-boss's API. It was replaced by typed `send`/`flow`/`work` plus `use`.

## Implementation reconciliation

No deviations yet.

## Open questions and risks

- Should a payload that fails to decode go to the dead-letter queue instead of failing and retrying? Retrying a poison payload is wasted work. The sketch fails the job, which follows the queue's retry policy.
- Should `ensureQueue` call `updateQueue` when a definition's `queue` options differ from the database's? The sketch only creates. Changing the options silently on deploy could surprise operators.
- After the shrink, pg-boss's own `fetch` would return natively decoded rows (timestamps as epoch ms). Does any pg-boss internal do date math on fetched job fields? This was not audited, and it decides whether the shrink also moves housekeeping to native, or only the scripts.
- Should `listen` (pg-boss `useListenNotify`) be implemented over `PgClient.listen` now, so polling latency drops without a housekeeping connection?
- How should a typed handler error be recorded in the job's output? The sketch stores the pretty-printed `Cause`. An `errorSchema` on the queue could store it as data, at the cost of more surface.
- Risk: the parked fiber must never outlive `boss.stop()`. The FiberSet closes after `stop`, which interrupts leftovers and rolls them back. Is that ordering right under `graceful: false`?

## Next implementation step

Port `proto/parked.ts` into `internal/database.ts` (`connectionDb`, `runParameterless`, `parkedTransaction`, `toDriverError`) and turn the prototype's eight observations into a vitest integration suite against Postgres 17 before filling in the public modules.
