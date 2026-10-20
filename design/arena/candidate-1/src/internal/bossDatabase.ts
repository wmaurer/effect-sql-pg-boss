import type * as Cause from "effect/Cause";
import type * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Fiber from "effect/Fiber";
import type * as SqlClient from "effect/sql/SqlClient";
import type * as Pg from "pg";
/**
 * The `db` pg-boss is constructed with.
 *
 * pg-boss runs everything it does on its own through this adapter: polling,
 * maintenance, migrations, and the transactions of transactional workers.
 * It runs on the `PgNodeClient` pool, so pg-boss never opens a pool.
 *
 * The one non-obvious part is `beginTransaction`. pg-boss calls it from its
 * worker loop, outside any fiber, and later settles the handle from another
 * call frame. Instead of driving `BEGIN`/`COMMIT` by hand, the adapter forks
 * a fiber that runs `sql.withTransaction` and parks inside it until pg-boss
 * decides. So the transaction a transactional worker runs in is an ordinary
 * `effect/sql` transaction:
 *
 * - nested `sql.withTransaction` in the handler becomes a SAVEPOINT, because
 *   the handler sees the same transaction level (and semaphore) the fiber has,
 * - `afterCommit` and Reactivity invalidation run after the real COMMIT,
 * - the `db` pg-boss gets is the `NodePgConnection` itself, so the settle
 *   fencing by identity holds.
 *
 * Not exported from the package.
 */
import type { Db } from "pg-boss";

/** Forks an effect into the PgBoss layer's scope; interrupted when it closes. */
export type RunFork = <A, E>(effect: Effect.Effect<A, E>) => Fiber.Fiber<A, E>;

/** How pg-boss settled a transaction it opened. First write wins. */
export type Settlement = "commit" | "rollback";

/**
 * Transaction contexts by the `tx` pg-boss hands a transactional handler.
 * Module-level because the keys are connection objects, which are unique;
 * an entry lives from `beginTransaction` until its fiber exits.
 */
const contexts = new WeakMap<Db, Context.Context<never>>();

/**
 * The context a handler must run in to join the transaction pg-boss opened
 * and passed to it as `tx`. Contains only what `sql.withTransaction` added:
 * the transaction level, its savepoint semaphore, the deferring Reactivity,
 * and the transaction span.
 *
 * Undefined when `tx` was not opened by this adapter, which cannot happen
 * for a worker registered through `JobQueue.worker`.
 */
export const transactionContext = (tx: Db): Context.Context<never> | undefined => contexts.get(tx);

export const make = (options: {
    readonly pool: Pg.Pool;
    readonly sql: SqlClient.SqlClient;
    readonly runFork: RunFork;
    /** The context the PgBoss layer was built in, to diff the transaction's context against. */
    readonly base: Context.Context<never>;
    /**
     * Bound on `rollback()`. pg-boss requires it to make progress; past this
     * the connection is destroyed instead of returned to the pool.
     */
    readonly rollbackTimeoutMillis: number;
}): Db => {
    // TODO:
    // executeSql: (text, values) => options.pool.query(text, values)
    //   Raw pool query; simple protocol without values, so migrations, BAM and
    //   flow-without-db work. Driver errors pass through untouched.
    //
    // poolCounts: () => ({ max: pool.options.max, total: pool.totalCount, idle: pool.idleCount, waiting: pool.waitingCount })
    //
    // listen: (channel, onNotification, onReconnect) => dedicated pool.connect(),
    //   LISTEN, reconnect loop calling onReconnect. Only needed for useListenNotify.
    //
    // beginTransaction: () => new Promise((resolve, reject) => {
    //   const settlement = Deferred.makeUnsafe<Settlement>()
    //   const fiber = options.runFork(options.sql.withTransaction(Effect.gen(function*() {
    //     const context = yield* Effect.context<never>()
    //     const [conn] = Context.get(context, options.sql.transactionService)
    //     // isNodePgConnection(conn) holds: the client is a PgNodeClient.
    //     contexts.set(conn, contextDelta(context, options.base))
    //     resolve(makeHandle(conn, settlement, fiber))
    //     if ((yield* Deferred.await(settlement)) === "rollback") {
    //       return yield* Effect.fail(rollbackRequested)   // makes withTransaction ROLLBACK
    //     }
    //   })))
    //   fiber.addObserver((exit) => { if begin never resolved: reject(driverError(exit.cause)) })
    // })
    //
    // makeHandle(conn, settlement, fiber): TransactionHandle
    //   db: conn
    //   commit: () => Deferred.succeed(settlement, "commit") was false
    //             ? reject(new Error("transaction already settled"))
    //             : await fiber exit; success -> resolve; failure -> reject(driverError(cause))
    //   rollback: () => Deferred.succeed(settlement, "rollback") (ignored if already decided);
    //             await fiber exit raced with rollbackTimeoutMillis;
    //             on timeout conn.destroy(...) then await exit; never rejects.
    //   Both delete conn from `contexts` once the fiber exits.
    void options;
    throw new Error("not implemented");
};

/**
 * The services `inner` has that `outer` lacks or holds a different value for.
 * Pure. Lets the handler fiber join a transaction another fiber owns without
 * this package knowing `effect/sql`'s private keys (the transaction semaphore
 * key is generated per client and not exported).
 */
export const contextDelta = (inner: Context.Context<never>, outer: Context.Context<never>): Context.Context<never> => {
    // TODO: Context.makeUnsafe(new Map([...inner.mapUnsafe].filter(([k, v]) => outer.mapUnsafe.get(k) !== v)))
    (void inner, outer);
    throw new Error("not implemented");
};

/**
 * The error a pg-boss promise rejects with: the `pg.DatabaseError` under a
 * `SqlError`, so `code`, `constraint` and `detail` reach pg-boss, or the
 * squashed cause when there is no driver error.
 */
export const driverError = (cause: Cause.Cause<unknown>): unknown => {
    void cause;
    throw new Error("not implemented");
};
