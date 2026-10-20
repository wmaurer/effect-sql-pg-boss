# Rationale: `@wmaurer/effect-sql-pg-boss` (candidate 2)

## Problem

An app keeps its tables and a pg-boss queue in one Postgres database. It wants two atomic operations. First, enqueue jobs inside `sql.withTransaction`. Second, write app tables from a pg-boss transactional worker so the writes commit with the job's completion.

Phase A found the constraints that fix the shape.

- The native `@effect/sql-pg` client cannot run multi-statement text (9 of 9 modes fail) and cannot be handed a pool or a connection (`fromPool` was removed in #7426). `boss.flow` sends multi-statement text, and pg-boss's own `db` needs it too.
- pg-boss takes an `IDatabase` (`executeSql`, `beginTransaction`). Almost every API accepts `{ db }` and sends one statement. A transactional worker hands the handler the exact `tx` object. pg-boss matches settle calls to a claim by that object's identity.
- `SqlClient.make` is public. A client over a borrowed connection works if `begin` is mapped to a savepoint. Providing `transactionService` by hand breaks nested `withTransaction`.
- pg-boss reads `err.code`, `err.constraint` and `err.detail`, so an adapter must keep the driver error.

## Usage (caller's view)

See `README.md`. The three facts a caller learns:

```ts
yield* sql.withTransaction(Effect.gen(function*() {
  yield* sql`insert into orders ...`
  yield* Boss.flow(receipt)                 // no tx argument, joins the transaction
}))

Boss.work(ChargeCard, { concurrency: 4 }, ([job]) =>
  Effect.gen(function*() {
    const sql = yield* SqlClient.SqlClient   // already the batch's transaction
    yield* sql`update orders set status = 'paid' where id = ${job.data.orderId}`
  }))

Boss.layer({ queues: [ChargeCard, SendReceipt] })   // start/stop, on the app's pool
```

`src/` and `typecheck/usage.ts` were reconciled to this. `usage.ts` compiles the call sites against the signatures.

## Shape

**One idea carries both directions: every operation runs on the connection of the calling fiber.** Nothing takes a `tx` or `db` argument. The package never asks the caller to pass a transaction around.

- **App to queue.** `internal/AmbientDb` builds a pg-boss `IDatabase` whose `executeSql` runs `sql.unsafe(text, values)` in the Effect context captured at the call. Inside `withTransaction` the context holds the transaction connection, so pg-boss's single statements, and `flow`'s multi-statement text, run in the app's transaction. This is the E5 prototype. Outside a transaction it autocommits (a multi-statement simple query is one implicit transaction, so `flow` stays atomic).
- **Queue to app.** `Boss.work` registers a `transactional: true` pg-boss worker. For each batch it takes pg-boss's `tx` and builds a borrowed `NodePgClient` with `root.borrow({ executeSql: tx.executeSql })`. It runs the Effect handler with `SqlClient` and `NodePgClient` replaced by that client. `withTransaction` inside becomes a savepoint, so the handler uses normal Effect SQL code. This is the `borrowed.ts` prototype. `Boss.send` inside the handler is the same ambient path, so chained jobs join the batch's transaction.
- **Main client.** The app's `SqlClient` comes from this package: `NodePgClient`, a node-postgres client over a `pg.Pool`. Reasons, in order. (1) pg-boss's constructor `db` must run multi-statement text, and native `PgClient` cannot. (2) The app and pg-boss must draw from one pool, and native `PgClient` cannot take one. (3) The ambient adapter needs a client that sends parameterless text as simple protocol. Upstream `PgClient.simple` would fix only (3), has unknown acceptance, and this is permanent userland. The cost is real. Decoding follows node-postgres, not the native client. The README says so.
- **One pool.** `NodePgClient` owns the pool (or wraps a caller's pool with `layerFromPool`). `Boss.layer` requires `NodePgClient` and builds `internal/PoolDb`, an `IDatabase` over `client.pool`. It adds `beginTransaction` (a checked-out client, `BEGIN`, a handle whose `db` is created once) and a time-bounded, idempotent `rollback`. pg-boss does not close a constructor `db`. Layer release order (workers, `boss.stop({ close: false })`, `pool.end()`) follows from layer dependencies, not from code.
- **Data first: queues are values.** `Queue<Name, A>` carries the name, a JSON codec from `Schema.toCodecJson`, and the policy. `send`, `work`, and `Boss.layer` all read it. `Flow.node(ref, queue, data, { dependsOn: [parentNode] })` makes a flow by value. A dangling parent and a cycle cannot be written, and the result type is `{ charge: JobId, receipt: JobId }` derived from the node graph.
- **Types carry the hand-synced list.** `Boss.layer({ queues })` provides `Registered<"a" | "b">`. `send`, `flow` and `work` require `Registered<Name>`. A queue missing from the list is a compile error at the use site. Creating queues eagerly at layer start is deliberate (see Alternatives).
- **Errors.** A failed statement comes back as the `SqlError` that the ambient client produced. The adapter keeps the typed error aside and rejects pg-boss with the unwrapped driver error, so pg-boss sees `code`/`constraint`. Everything else is one `BossError` with a `kind`.

**Interface depth.** The public surface is `Queue.make`, `Flow.node`, `Boss.{layer, send, flow, work, use}` and the client layers. It hides the pg-boss `IDatabase` contract, the `tx` identity rule, savepoint mapping, the pool and the rollback timeout, driver-error unwrapping, queue creation and ordering, graceful stop, payload encoding, and flow topological order. The caller does not learn any of it. The one leak is `Boss.use`, which takes pg-boss types on purpose, for the long tail (`schedule`, `cancel`, `findJobs`).

**Principles applied.**

- `boundary-discipline`: payloads are encoded and decoded at the pg-boss edge only. Inside, handlers see decoded `Job<A>`.
- `type-system-discipline`: `Registered<Name>`, branded `JobId`, flow refs derived from nodes, `NodePgClient` marked by `TypeId`.
- `separate-before-serializing-shared-state`: the batch gets its own borrowed client. The only serialisation left is the one-permit semaphore around one physical connection, which is a real invariant (savepoints must not interleave).
- `make-operations-idempotent`: queue creation at start is idempotent. `rollback` is safe twice. A retried batch rewrites the same rows inside a fresh transaction.
- `laziness-protocol`: no wrapper for each pg-boss API, one escape hatch instead.

## Synthesis decision

Not filled in. This file is one runner's candidate. The arena records the decision.

## Tradeoffs accepted

- We accept a second Postgres client for the app (`NodePgClient`) in exchange for sharing a pool and transactions with pg-boss without touching pg-boss internals or upstream.
- We accept that `afterCommit` and Reactivity invalidation inside a worker fire at `RELEASE SAVEPOINT`, before pg-boss's real commit, in exchange for using the stock `SqlClient` transaction machinery. A reactive reader on another connection may see stale data for a moment.
- We accept an extra connection held for the whole handler in atomic mode, in exchange for exactly-once database effects. `atomic: false` exists for handlers that do no database work.
- We accept that every Boss call allocates a small ambient adapter, in exchange for capturing the right transaction without global state.
- We accept the `Registered<Name>` type machinery in exchange for removing a hand-synced list from the failure modes. The runtime then never checks queue membership.
- We accept no LISTEN/NOTIFY in v1 (polling only). The `IDatabase.listen` hook needs a dedicated connection and reconnect logic that does not belong in the first cut.
- `NodePgClient extends SqlClient`, as other drivers do. TypeScript then merges `NodePgClient | SqlClient` into `SqlClient`, so a program that also requires plain `SqlClient` does not show `NodePgClient` as its own requirement. We accept this and compare `client.pool` with Boss's pool at runtime to catch a wrong client.
- `batchSize > 1` fails the whole batch on one bad job. This follows pg-boss's transactional contract (no per-job results with `transactional`).

## Alternatives considered

1. **Keep native `PgClient` as the app client and split `flow`'s text on `";\n"` in an adapter.** Works in the E2/E3 prototypes. Rejected. It depends on pg-boss's internal join string. It leaves two pools. It still cannot give `Boss.layer` a shared connection source. The adapter is small but the knowledge it needs is not stable.
2. **Add `PgClient.simple` / `fromPool` upstream and use `PgClient` everywhere.** Best for users if it lands. Rejected as the base because it blocks on an unknown review and this package is permanent userland. The design keeps the seam: `Boss` needs only "a client whose ambient connection speaks pg-boss's text". If `PgClient` gains it, `NodePgClient` can be replaced without changing the public Boss API.
3. **Explicit `{ db }` / `Tx` argument (`Boss.send(queue, data, { tx })`, handler receives `tx`).** Honest and simple to build. Rejected on interface depth. Callers must thread a value that Effect already carries in its context, and forgetting it silently gives a non-atomic enqueue. The ambient design makes the wrong thing impossible to write.
4. **Join a worker's transaction by providing `sql.transactionService` as `[conn, 0]`.** Plain statements work, but nested `withTransaction` dies with "Service not found: TransactionSemaphore" and `afterCommit` runs immediately. It needs an upstream fix. The borrowed client over `SqlClient.make` with savepoint controls works today.
5. **Methods on the service (`boss.send(...)`) plus module-level helpers.** Two ways to do one task. Rejected. Module functions only. The service handle is opaque.
6. **Create queues lazily on first send.** Removes the queue list. Rejected. First send inside a caller's transaction would run `createQueue` DDL on another connection while the caller's transaction may hold locks on `pgboss.job`. That can wait on the caller itself. Eager creation at layer start avoids it, and `Registered` removes the cost of the list.
7. **Untyped `send(name, data)` and a separate typed layer on top.** A flat pg-boss port. Rejected. Payload type safety is the main reason to wrap pg-boss for Effect users, so it belongs in the core data structure.

## Implementation reconciliation

Empty. Fill during implementation. Record accepted deviations and who accepted them.

## Open questions and risks

- Should `Registered<Name>` stay? It makes a forgotten queue a compile error, but it adds a type to every signature. Would you prefer a runtime `BossError` instead?
- Should the worker handler get a per-job result type (output schema on the `Queue`)? pg-boss stores `output`. The sketch returns `void`.
- Can a handler mark a failure as non-retryable (`Boss.NonRetryable`)? pg-boss supports `deadletter` only through `perJobResults`, which cannot combine with `transactional`. This may need a thrown marker plus a post-rollback `fail` call.
- Pool sizing. `N` transactional workers hold `N` connections. Should the layer compute a minimum from the registered workers and warn, as pg-boss does for its own pool?
- `afterCommit` timing in workers. Is a small upstream hook ("run after outer commit") worth proposing, or is the documented limit enough?
- `rollback` after a handler error: pg-boss rolls back and then calls `fail` on the pool. If the handler recovered from a failed statement without a savepoint, the transaction is aborted (`25P02`). Does the borrowed client need to detect an aborted transaction and fail the batch early with a clear message?
- LISTEN/NOTIFY: `IDatabase.listen` on a dedicated `pg.Client` built from `pool.options`. Is polling latency acceptable for the first release?
- Decoding differences. Apps moving from native `PgClient` get strings for `int8` and `numeric`. Is a `transformRows` helper for bigint worth shipping with the client?

## Next implementation step

Implement `internal/PoolDb.ts` and `internal/AmbientDb.ts`, then run the E4/E5 prototype scenarios (commit and rollback, both directions) against them as the first integration test.
