# Rationale: `@wmaurer/effect-sql-pg-boss` (synthesis)

## Problem

An Effect app keeps its own tables and a pg-boss queue in one Postgres database and wants two things. First, a job sent inside `sql.withTransaction` commits or rolls back with the app's rows. Second, a transactional worker's `@effect/sql` writes commit together with the job's completion. The app already runs on the native `@effect/sql-pg` `PgClient` 4.0.5, and that client constrains the shape in three ways. It cannot run a script with more than one statement (every path sends `Parse`, `PgProtocol.ts:340-351`), and pg-boss's migrations, maintenance and `flow` send such scripts. It can only use connections it opens itself, so pg-boss's built-in `Db` cannot share its pool. Its transaction context cannot be joined from outside: providing `transactionService` as `[conn, 0]` yourself breaks nested `withTransaction` (`SqlClient.ts:676`) and runs `afterCommit` early. On the pg-boss side, settles are fenced by the identity of the `db` object (`manager.js:671-674`), errors must keep SQLSTATE fields (`manager.js:172, 1905, 730`), and `rollback()` must never reject and must be time-bounded (`types.d.ts:70-84`).

## Usage (caller's view)

`README.md` is the spec. Its call sites compile in `examples/usage.ts` under `strict` and `exactOptionalPropertyTypes`, with type-level assertions on what the compiler infers. The core of it:

```ts
class ChargeCard extends JobQueue.Service<ChargeCard>()("charge-card", {
  payload: Schema.Struct({ orderId: Schema.BigInt, amountCents: Schema.Int }),
  options: { retryLimit: 5, retryBackoff: true }
}) {}

yield* ChargeCard.send({ orderId, amountCents })                      // joins the fiber's transaction
yield* JobQueue.flow({ charge: ChargeCard.step(p), ship: ShipOrder.step(q, { dependsOn: ["charge"] }) })
ChargeCard.worker((jobs) => /* sql writes, nested withTransaction, AuditLog.send, afterCommit */)
PgBoss.layer({ schema: "pgboss" })                                   // needs PgClient.PgClient
PgBoss.use((boss, connection) => boss.cancel(ChargeCard.queueName, id, connection))
```

`examples/usage.ts` rejects five misuses, each confirmed by deleting the `@ts-expect-error` and reading the error: a ref typo in `dependsOn` (`'"chrage"' is not assignable to '"charge"'`), a self-reference, a wrong payload type (`'string' is not assignable to 'bigint'`), `transactionTimeoutSeconds` with `transactional: false`, and running a send to a queue whose layer is missing (`'Unregistered' is not assignable to type 'never'`).

## Shape

**Data.** A queue is a class (`JobQueue.Service`) whose statics are the queue's identity (`queueName`, `payload`, `options`) and its operations (`layer`, `send`, `step`, `worker`). The class is a `Context` service. Its value is `Registered<Name>`, which is proof that `layer` created the queue plus the `PgBoss` service it was created on. `send`, `step` and `worker` require the class, so an unregistered queue is a missing requirement. A flow is a record of `Step<Self, Refs, R>`, and `Step.ValidRefs` restricts each step's `dependsOn` to the other keys. Payloads go through `Schema.toCodecJson`, so `bigint` survives the `jsonb` column. `Job<A>` is `{ id, queue, data, retryCount, expireInSeconds }`. pg-boss's raw job never reaches a handler.

**One routing rule, one owner** (`internal/connection.ts`). A pg-boss call runs on the fiber's current `effect/sql` transaction connection. `dbFor(conn)` caches one `Db` per `Connection` in a `WeakMap`. Native `PgClient` hands each transaction a fresh `Connection` (`PgClient.ts:184`), so the cache never carries a `Db` across transactions, and one transaction always presents one object to pg-boss's identity fence. The `Db` calls `conn.executeUnprepared(text, values, undefined)` directly, so pg-boss gets raw rows and the `Db` holds no fiber context. `values === undefined` marks the one parameterless script (`flow`), which `runScript` splits on `";\n"` and runs part by part. Outside a transaction, `withDb` opens one, so `send` and `flow` keep their atomicity. Errors cross one boundary. A `Db` rejects with the driver `Error` carried on the `SqlError` reason (SQLSTATE fields intact), and `call` maps the rejection back to the original `SqlError`, or to `PgBossError` for anything else.

**Parked worker transaction** (`internal/workerTransaction.ts`). `beginTransaction()` forks `sql.withTransaction(body)` into the PgBoss layer's FiberSet. The body registers the committed signal with `afterCommit` first, forks a handler child, resolves the handle, and waits for pg-boss's settlement. The worker callback submits the handler to that child. The child runs it with the transaction's own services (the parked context minus the layer's base context) over the worker layer's context. On rollback the body interrupts and awaits the child before `withTransaction` sends `ROLLBACK`. `commit()` resolves on the committed signal, not on the fiber's exit. `rollback()` resolves on the fiber's exit or after `rollbackTimeout`, whichever comes first. The `Db` refuses statements once the transaction is settled.

**pg-boss's own statements** (`internal/housekeeping.ts`) go to a small node-postgres pool built from `PgClient.config`, so app rows and pg-boss rows never share a decoder. The pool, the `pg` dependency and `runScript`'s split are the only parts that exist because `PgClient` lacks a simple-protocol call. All three are deleted when `PgClient.simple` lands.

**Depth.** The public surface is 2 modules. `JobQueue` has `Service`, `flow`, `JobId`, `Job`, the option types and `Step`. `PgBoss` has `layer`, `layerConfig`, `use`, `PgBossError` and `Options`. The surface hides the transaction routing, the identity fence, the parked fiber, the error mapping, the sidecar pool and the flow split. The one leak is `PgBoss.use`, which hands out pg-boss's own API. That is deliberate, per laziness-protocol: about 80 pg-boss methods stay one escape hatch instead of a facade.

## Synthesis decision

**Base: candidate 3** (the native `PgClient` stays the app's only client), with candidate 1's public queue API. The cross-judge recommended candidate 1 (scores A=24, B=20, C=22). The parent overrode it on one criterion the rubric did not price. The user's app already runs on native `PgClient` 4.0.5. Candidate 1 would change every row decode in the existing app (`int8` from `bigint` to string, timestamps from epoch ms to `Date`, `bytea` to `Buffer`) and leave the user maintaining a 300-800 line driver client with no upstream shrink. The parked transaction, which candidates 1 and 3 found independently, removed the only reason to want a node-postgres client: worker-side decoding stays native too. Candidate 3's remaining costs, the sidecar pool and the `flow` split, sit in two internal files and shrink away if `PgClient.simple` lands. The sidecar also keeps pg-boss polling from being starved by handlers that hold app connections.

**Adapted from candidate 1.** The queue `Service` class with `send`, `step` and `worker` as statics, and the requirement it carries. The record-keyed `flow` with `Step.ValidRefs`. `WorkerOptions` as a union where `transactionTimeoutSeconds` exists only on the transactional branch. The batch as the handler's unit. The context delta (transaction-owned services over the worker layer's context). The `Equals` type assertions and the `@ts-expect-error` cases in `examples/usage.ts`. Candidate 3's separate `JobWorker` module folded into the class.

**Adapted from candidate 3.** The native-client direction. The parked-transaction prototype (`proto/parked.ts`). The housekeeping pool built from `PgClient.config`. The `originalSqlError` mapping. The `flow` split. Payloads through `Schema.toCodecJson`.

**Adapted from candidate 2.** The time-bounded rollback race from `PoolDb.ts`. Eager queue creation at layer build, from candidate 2's note that DDL on another connection can wait on the caller's own locks.

**Judge fixes folded in.** The handler runs as a child of the parked fiber, so a rollback cannot leave a late statement running (risk 1). The rollback is bounded (risk 2). `commit()` waits only for the committed signal (risk 3). Transactional workers reserve pool headroom (risks 3 and 4). The `Db` uses `executeUnprepared`, with no row transforms and no stale context. The sidecar has per-client and pool `'error'` listeners (risk 7). The runtime handle hides behind a non-`Symbol.for` symbol. The sidecar option is named for its effect (`maintenanceConnections`).

**Rejected.**

- A node-postgres main client (candidates 1 and 2). It changes the app's decoders and adds a driver client to maintain forever.
- A borrowed per-batch `SqlClient` (candidate 2). Repositories captured at layer build write outside the transaction, and `afterCommit` fires before the real `COMMIT`.
- Providing `transactionService` as `[conn, 0]`. A nested transaction dies at `SqlClient.ts:676`, and `afterCommit` runs immediately.
- Lazy queue creation on first send (candidate 3). DDL on another connection can wait on the caller's own locks.
- A full merge of the parked context over the worker context (candidate 3). It overrides worker-layer services.
- A flow graph by value (candidate 2). Record-keyed refs are simpler, and pg-boss rejects cycles at run time.
- A full Effect facade over pg-boss's methods. `PgBoss.use` covers them.

## Tradeoffs accepted

- We accept a second, small node-postgres pool (default 4 connections) in exchange for keeping every app row on the native decoder. It disappears with `PgClient.simple`.
- We accept depending on pg-boss's internal `";\n"` join for `flow` inside a transaction, in exchange for unmodified `boss.flow` calls. The peer range is `~12.38`, and a test must fail when the statement count of a known flow changes.
- We accept that pg-boss statements on the app's connection carry no spans (they skip `sql.unsafe`), in exchange for raw rows and a stateless `Db`.
- We accept that `send` outside a transaction costs a `BEGIN` and a `COMMIT`, in exchange for one routing rule and an atomic `flow` everywhere.
- We accept that each in-flight transactional batch holds a `PgClient` connection for the handler's whole run. `transactional: false` exists for slow handlers.
- We accept that a timed-out rollback interrupts the parked fiber and can leave a stuck connection to the native client's own cancel-and-drain timeout, in exchange for pg-boss's error path always making progress.
- We accept that pg-boss read APIs used through `PgBoss.use` inside a transaction return natively decoded rows. The README says to read outside transactions.

## Alternatives considered

- **The app keeps native `PgClient`, and pg-boss gets a node-postgres pool for everything, transactions included.** The interface stays the same, but a transactional worker's writes would run on a node-postgres connection with node-postgres decoders and no `effect/sql` transaction semantics. The handler would need a second `SqlClient`, which is the hidden complexity candidate 2 exposed.
- **Upstream first: add `PgClient.simple` and a public "join this connection" API, then build on them.** This shape is cleaner, but it blocks the package on two upstream changes of unknown acceptance. The chosen shape isolates both gaps in internal files that shrink when either change lands.
- **Handler in pg-boss's call frame, with the transaction context merged in** (candidate 3's prototype). It is simpler, but a rollback after a timeout leaves the handler running on a connection that is already back in the pool. Making the handler a child of the parked fiber removes that with no change to the public API.

## Implementation reconciliation

Phase F ran `prototype/synth.ts` against a throwaway Postgres 17.11 with pg-boss 12.38.0 and `effect` and `@effect/sql-pg` from the repo sources. It imports the real `src/internal/*.ts`. Output is in `prototype/synth.out`. Every spec decision held. The deviations below are signature or placement changes.

1. **`PgBoss.use`'s second argument is `Boss.ConnectionOptions` (`{ db?: Db }`), and it is `{}` outside a transaction.** The spec wrote `conn: { db }`. Outside a transaction there is no connection to join. Always giving a `db` would mean opening an app transaction for every `use` call and decoding pg-boss reads natively for no reason. Proposed: accept. The spec's "conn joins the fiber's transaction" still holds.
2. **`internal/runtime.ts` was added.** It holds `RuntimeId` (`Symbol(...)`, not `Symbol.for`), the `Runtime` type, and `connectionBudget`. `JobQueue` and `PgBoss` both need the hidden handle, and a symbol exported from a public module would be nameable. Proposed: accept.
3. **The worker's pg-boss callback (`workCallback`) lives in `internal/workerTransaction.ts` as real code.** `prototype/synth.ts` runs it. `JobQueue.Service`'s `worker` pseudocode calls it. Proposed: accept.
4. **`rollbackTimeout` and `connectionReserve` are `PgBoss.layer` options, not worker options.** pg-boss calls `this.db.beginTransaction()` with no arguments (`manager.js:667`), so the transaction cannot know its worker. Proposed: accept.
5. **`runScript` uses no savepoint.** A failing part aborts the enclosing transaction, and `PgClient`'s `commit` fails on an aborted transaction (`PgClient.ts`, "COMMIT rolled back an aborted transaction"), so the flow stays atomic. Measured: a flow in a transaction that rolls back leaves `rows=[] jobs=[]`. Not measured: a failure in the second part of a script (guess: same result).
6. **Open type gap in decision 6.** `PgClient.config` is typed `PgClientConfig`, which has no `maxConnections`. The headroom check must cast to `PgPoolConfig` and fall back to `PgPool`'s default of 10 (`PgPool.ts:162`). For `PgClient.makeClient`, a single connection, the number means nothing. Proposed fix: read `maxConnections` through the cast, and fail `PgBoss.layer` when a transactional worker is registered on a client without a pool. This needs the operator's call (see open questions).

Phase F evidence, quoted from `prototype/synth.out`:

- **P1 commit and failure paths.** `p1-ok: app rows=["nested-savepoint","outer"] jobs=["p1:completed","p1-follow:created"]`, `p1-bad: app rows=[] jobs=["p1:failed"]`, `p1-ok: currentDb === pg-boss tx: true`, `p1-ok: afterCommit sees p1:completed, p1-follow:created`, and the released guard: `statement on p1-ok's tx after commit: rejected: pg-boss used a worker transaction after it was settled`.
- **P2 abandon path.** `2044ms p2 handler started on backend 88, running pg_sleep(10)`, then `4043ms pg-boss called rollback()`, `4058ms p2 handler interrupted`, `4059ms rollback() resolved after 16ms`. The server log for backend 88 reads `BEGIN`, pg-boss's `set_config(transaction_timeout)`, `select pg_sleep(10)`, `ERROR: canceling statement due to user request`, `ROLLBACK`, and then only the next pool user's statements. The handler's insert after the sleep never ran (`p2 app rows=[]`). The backend is `"state":"idle"`, and six concurrent transactions afterwards all succeeded, on backends `[88,88,88,88,88,88]`.
- **P2b bounded rollback (extra).** A handler that sleeps 4 s uninterruptibly, with `rollbackTimeout` 1 s: `6523ms pg-boss called rollback()`, `7524ms rollback() resolved after 1001ms`, `p2b job failed while the handler still sleeps`. The handler was interrupted when it left the uninterruptible region (`9523ms p2b handler interrupted`), and its insert never ran (`p2b app rows after the handler woke up=[]`).
- **P3 committed signal.** `commit() resolved after 1ms`, and `app afterCommit #1 finished 3051ms after commit() resolved`.
- **P4 order.** `app afterCommit #1: commit() already resolved when it ran: true`. Callbacks run one after another, and #1 sleeps 3 s. So the library's signal, registered first, ran before #1 started.
- **P5 raw rows and send in a transaction.** With `transformResultNames` set to camelCase: `sql=[{"createdOn":1}] sql.unsafe=[{"createdOn":1}] Db.executeSql=[{"created_on":1}]`. Also `send inside sql.withTransaction returned "47deaa64-..."`, flow commit and rollback in a transaction, a flow outside a transaction, and `unique violation through a Db: pg-boss sees Error code=23505 constraint=synth_unique_pkey; the Effect fails with SqlError/UniqueViolation`.

## Open questions and risks

- Should `PgBoss.layer` refuse transactional workers on a `PgClient.makeClient` client (one connection), or allow them with a budget of 1 and warn? Every transactional batch would pin the app's only connection.
- Phase F did not exercise three paths: `commit()` rejecting on a failed `COMMIT`, a failing `BEGIN`, and layer shutdown while a handler waits for submission. It also did not exercise abandonment through `boss.stop()` (P2 used job expiry, which takes the same rollback path). Each has code, but none has a run. Should the first implementation unit add a test for each before the public layers?
- In P2b the timed-out rollback interrupted the parked fiber. By the code path (inferred, not measured), the fiber then sent `ROLLBACK` and released the connection while the handler child still slept. The child was interrupted when it became interruptible, before any statement. A handler stuck inside an uninterruptible statement would instead hold the connection until the native client's cancel-and-drain timeout. Is that acceptable for v0.1?
- pg-boss passes `untracked(transaction.db)` to the handler. That is the same object unless a pg-boss `TestClock` is attached (`activity.js:7-9`), and then `run` finds no transaction and rejects. pg-boss's `start()` refuses a `TestClock` on a `Db` without `setSessionStatements`, which ours lacks, so the path looks unreachable. Is that enough, or should `run` also look up `untracked` originals?
- `useListenNotify` is unsupported. Is polling acceptable for v0.1, or should `Db.listen` go through `PgClient.listen` now?
- The queue layer does not update the options of an existing queue. Should it converge options (`updateQueue`) at build, as candidate 1 proposed?

## Next implementation step

Implement `PgBoss.layer` by moving `prototype/synth.ts`'s wiring (housekeeping pool, FiberSet, `beginTransaction`, start and stop) into it, then rerun P1-P5 through `ChargeCard.worker` and `ChargeCard.send` instead of the internal helpers.
