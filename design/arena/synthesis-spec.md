# Synthesis spec: `@wmaurer/effect-sql-pg-boss`

Arena result: three candidates under `arena/candidate-{1,2,3}`, cross-judge scores A=24, B=20, C=22. This spec is the synthesized design. It is the contract for the synthesized sketch in `arena/synthesis/`.

## Base and why

**Base: candidate 3 (native `@effect/sql-pg` `PgClient` stays the app's only client)**, with candidate 1's public queue API.

The judge recommended candidate 1 (node-postgres main client). I overrode it on one criterion the rubric did not price: the user's app already runs on native `PgClient` 4.0.5 (problem.md). Candidate 1 would change every row decode in the existing app (`int8` bigint -> string, timestamps epoch ms -> `Date`, `bytea` Uint8Array -> Buffer) and make the user maintain a 300-800 line driver client forever with no upstream shrink. The parked transaction (found independently by candidates 1 and 3) removed the only reason the investigation recommended a node-postgres client: worker-side decoding now stays native too. Candidate 3's remaining costs (a small node-postgres sidecar pool for pg-boss's own statements, a `flow` split stopgap) are isolated in two internal files and shrink away if `PgClient.simple` lands. The judge also noted the sidecar keeps pg-boss polling/maintenance from being starved by handlers holding app connections.

## Public API (the contract)

Modules: `JobQueue`, `PgBoss`. (Candidate 3's separate `JobWorker` module folds into the queue class, per candidate 1.)

```ts
class ChargeCard extends JobQueue.Service<ChargeCard>()("charge-card", {
  payload: Schema.Struct({ orderId: Schema.BigInt, amountCents: Schema.Int }),
  options: { retryLimit: 5, retryBackoff: true }      // pg-boss queue options minus name
}) {}

ChargeCard.layer                 // Layer<ChargeCard, PgBossError | SqlError, PgBoss>: creates queue if missing at layer build (eager; never DDL in a request path, per candidate 2's deadlock note)
ChargeCard.send(payload, opts?)  // Effect<Option<JobId>, SqlError | PgBossError | SchemaError, ChargeCard | EncodingServices>
ChargeCard.step(payload, { dependsOn?: [...refs] , job?: JobInsertOptions })   // flow node
JobQueue.flow({ charge: ChargeCard.step(...), ship: ShipOrder.step(..., { dependsOn: ["charge"] }) })
                                 // Effect<{ charge: JobId; ship: JobId }, ..., ChargeCard | ShipOrder | ...>; dependsOn only accepts other keys of the record
ChargeCard.worker(handler, options?)  // Layer<never, PgBossError, ChargeCard | R | DecodingServices>
   // handler: (jobs: NonEmptyReadonlyArray<Job<A>>) => Effect<void, E, R>   (batch = unit of atomicity)
   // options.transactional defaults to true; transactionTimeoutSeconds only on the true branch
PgBoss.layer(options?)           // Layer<PgBoss, PgBossError | SqlError, PgClient.PgClient>
PgBoss.layerConfig(Config.Wrap<options>)
PgBoss.use((boss, conn: { db }) => Promise<A>)  // escape hatch; conn joins the fiber's transaction
JobQueue.Job<A> { id: JobId; queue; data: A; retryCount; expireInSeconds }   // pg-boss raw Job never public
JobQueue.JobId                   // branded string + Schema
PgBossError                      // non-SQL pg-boss failures; SQL failures surface as the original SqlError
```

`send`/`step`/`flow`/`worker` require the queue's service, so an unregistered queue is a type error (candidate 1). Queue identity: `X.queueName`, `X.payload`, `X.options` statics. Keep statics to what callers need (judge noted 7 statics is wide; drop anything internal).

## Internal structure (decisions)

1. **Routing rule, one owner** (`internal/connection.ts`). The pg-boss `Db` for a call is derived from the fiber's current `effect/sql` transaction `Connection` (`Effect.serviceOption(sql.transactionService)`), cached in a `WeakMap<Connection, Db>`. The `Db` is stateless over the Connection: `executeSql(text, values)` calls `conn.executeUnprepared(text, values ?? [], undefined)` directly, NOT `sql.unsafe`. This fixes two judge findings: no `transformRows`/`transformResultNames` applied to pg-boss rows, and no stale captured context/savepoint depth in the cache. Outside a transaction, `send` runs inside its own `sql.withTransaction` (so `flow` stays all-or-nothing) and uses that transaction's Db. One Db per Connection gives the parked worker transaction's Db identity for pg-boss settle fencing.
2. **Flow stopgap** (`internal/connection.ts`): `values === undefined` means a parameterless script; only `flow` sends one on a `{ db }` path. Split on `FLOW_SEPARATOR = ";\n"` and run each part in order on the connection. Pin `pg-boss` peer to `~12.38` and ship a test that fails if the separator count changes. Deleted when `PgClient.simple` exists.
3. **Errors** (candidate 3): the Db rejects with the driver `Error` from the `SqlError` reason cause (keeps SQLSTATE `code`, `constraint`, `detail` for pg-boss); `originalSqlError: WeakMap<Error, SqlError>` lets the single Promise boundary fail the caller's Effect with the original typed `SqlError` (e.g. `UniqueViolation` stays matchable). Non-SQL rejections become `PgBossError { operation, cause }`.
4. **Parked worker transaction** (`internal/workerTransaction.ts`), with the judge's required fixes:
    - `beginTransaction()` forks into the PgBoss layer's FiberSet: `sql.withTransaction(body)`. The body registers `sql.afterCommit(signal committed)` FIRST, captures its context, resolves the handle, then serves the handler.
    - **The handler runs as a child of the parked fiber, not in pg-boss's frame.** The worker callback submits `handlerEffect` to the parked fiber through a Deferred/Queue; the parked fiber runs it in its own context with the worker layer's context underneath (context delta per candidate 1: transaction-owned services win, everything else from the worker layer; no full merge as in candidate 3). The callback awaits the child's exit. Consequence: `rollback()` interrupts the handler child (structured) BEFORE ROLLBACK runs and before the connection is released, so no late statement can run in another session (judge risk 1).
    - Settlement `Deferred<"commit" | "rollback">`, first write wins: `rollback` idempotent and never rejects; `commit` after settle rejects.
    - **`commit()` resolves on the committed signal** (the first afterCommit callback, runs right after the real COMMIT) or rejects on the fiber's failure; it does not wait for app `afterCommit` callbacks (judge risk 3). The fiber keeps running remaining afterCommit callbacks in the FiberSet.
    - **`rollback()` is time-bounded** (option `rollbackTimeout`, default 5s): on timeout, interrupt the parked fiber and resolve anyway; never awaits an exit that may not come (judge risk 2).
    - Released guard: the parked Db refuses statements once settled (mirrors pg-boss `db.js:299-379`).
    - The handler's pg-boss `tx` must be the Db registered for the parked connection; worker callback looks up the parked transaction by `tx` and fails loudly if absent.
5. **Housekeeping pool** (`internal/housekeeping.ts`, candidate 3): node-postgres pool built from `PgClient.config` (no caller config; small `maxConnections`, default 4). Runs pg-boss's own constructor-db statements (migrations, maintenance, BAM, fetch, non-transactional settles). Per-checkout `'error'` listener on any checked-out client, and a pool `'error'` listener (judge risk 7). Option is named for its effect, not the mechanism: `PgBoss.layer({ maintenanceConnections })` (judge: "housekeeping" name leaks the implementation). Deleted, with the `pg` dependency, once `PgClient.simple` lands; option then becomes a documented no-op.
6. **Pool headroom**: each transactional worker layer reserves `localConcurrency` from a budget kept in the PgBoss runtime and fails at layer build when the total reaches the PgClient pool's `maxConnections` minus a reserve (option, default 2) (judge risk 3/4).
7. **Lifecycle** (finalizers reverse): sidecar pool, FiberSet, `new PgBoss({ ...options, db })` with `error`/`warning` listeners logging through Effect, `boss.start()`; release `boss.stop({ graceful: true, ... , close: false })`, then FiberSet interrupts leftovers (rolling them back), then sidecar ends. Worker layers: `offWork(name, { id, wait: true })` on release.
8. **Runtime handle**: hidden on the `PgBoss` service through a module-private (non-`Symbol.for`) symbol so callers truly cannot name it (judge red flag on candidate 3).
9. **Documented, not fixed**: in a transaction, pg-boss read APIs used through `PgBoss.use` return natively decoded rows (timestamps as epoch ms) because they run on the native connection; advise reads outside transactions. Each in-flight transactional batch holds one PgClient connection.

## Rejected (with source)

- Node-postgres main client (candidates 1, 2): app-wide decoder change and a permanent driver client; see "Base and why".
- Borrowed per-batch SqlClient (candidate 2): repositories captured at layer build write outside the transaction; afterCommit fires before the real COMMIT.
- Self-provided `transactionService` `[conn, 0]`: dies at `SqlClient.ts:676`; afterCommit runs immediately.
- Lazy queue creation on first send (candidate 3): DDL on another connection can wait on the caller's own locks.
- Full merge of the parked context over the worker context (candidate 3): overrides worker-layer services.
- Flow graph by value (candidate 2): record-keyed refs are simpler; pg-boss rejects cycles at runtime. Revisit if users hit it.
- Full Effect facade over pg-boss's ~80 methods: `PgBoss.use` instead.

## Things the prototype must prove (Phase F)

P1. Handler-as-child-of-parked-fiber: commit path (app row + nested savepoint + `send` joined + job completed), failure path (nothing persisted, job failed).
P2. Abandon path: a handler still running a slow statement (`pg_sleep`) when pg-boss rolls back (short `expireInSeconds` or `boss.stop` with short timeout): the handler child is interrupted, no statement runs after ROLLBACK, the connection returns to the pool healthy.
P3. Commit signal: an app `afterCommit` that sleeps 3s does not delay `commit()` resolution (measure).
P4. afterCommit callback order: the library's committed-signal callback registered first runs first.
P5. `executeUnprepared` with `transformResultNames` set on PgClient leaves pg-boss rows untransformed, and `send` inside a transaction returns an id.
