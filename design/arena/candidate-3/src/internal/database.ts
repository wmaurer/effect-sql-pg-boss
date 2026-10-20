import * as Effect from "effect/Effect";

import type { Housekeeping } from "./housekeeping.ts";
/**
 * The three pg-boss `Db` objects this library hands pg-boss, all over the
 * app's native `PgClient`:
 *
 * - {@link connectionDb}: passed as `{ db }` on every call the library makes.
 *   Runs pg-boss's statement in a captured fiber context, so it lands on
 *   whatever connection `sql` would use there (a transaction, or the pool).
 * - {@link parkedTransaction}: what `beginTransaction()` returns for a
 *   transactional worker. The transaction is a real `sql.withTransaction`
 *   whose body is parked on a Deferred until pg-boss commits or rolls back.
 * - {@link bossDb}: the constructor `db`. Housekeeping on the node-postgres
 *   sidecar, worker transactions on the native client.
 *
 * Shrink plan: when `@effect/sql-pg` gains a simple-protocol call
 * (`PgClient.simple`), `runParameterless` calls it instead of splitting, and
 * `bossDb.executeSql` drops the sidecar. Nothing outside this file and
 * `housekeeping.ts` changes.
 *
 * @internal
 */
import type * as Context from "effect/Context";
import type * as Fiber from "effect/Fiber";
import type * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";
import type * as Boss from "pg-boss";

/** pg-boss's `IDatabase`, exported by pg-boss as `Db`. */
export type Db = Boss.Db;

/** pg-boss's transaction handle, inferred from `Db` (pg-boss does not export the name). */
export type TransactionHandle = Awaited<ReturnType<NonNullable<Db["beginTransaction"]>>>;

/**
 * pg-boss joins a flow's statements with exactly this separator and binds no
 * values (pg-boss 12.38 `manager.js:1840-1843`). The text contains no other
 * occurrence: the templates have none and JSON data cannot produce a raw
 * newline. Covered by a test pinned to the peer range.
 */
export const FLOW_SEPARATOR = ";\n";

/**
 * Builds the per-connection db for a fiber context.
 *
 * Routing (the pg-boss adapter convention, `adapters/bun.js:35-39`):
 * - `values !== undefined`: one extended-protocol statement,
 *   `sql.unsafe(text, values)`. Every `{ db }` API except `flow` lands here.
 * - `values === undefined`: a parameterless multi-statement script. Only
 *   `flow` sends one on a `{ db }` path; see {@link runParameterless}.
 *
 * Errors: a failing statement rejects with the driver's own `Error` (the
 * `cause` of the `SqlError` reason, carrying SQLSTATE `code`, `constraint`,
 * `detail`), because pg-boss branches on those fields (23505, 22012, 25P02).
 * The `SqlError` is remembered in {@link originalSqlError} so the caller's
 * Effect fails with it, not with a re-wrapped copy.
 */
export const connectionDb = (_context: Context.Context<SqlClient.SqlClient>): Db => {
    // TODO
    // const sql = Context.get(context, SqlClient.SqlClient)
    // const run = Effect.runPromiseWith(context)
    // return {
    //   executeSql: (text, values) =>
    //     run(values === undefined ? runParameterless(sql, text) : Effect.map(sql.unsafe(text, values), (rows) => ({ rows })))
    //       .catch((e) => Promise.reject(toDriverError(e)))
    // }
    throw new Error("not implemented");
};

/**
 * Runs a parameterless script on the native client.
 *
 * Stopgap until `PgClient.simple` exists: split on {@link FLOW_SEPARATOR} and
 * run each part in `sql.withTransaction`, which is a savepoint inside the
 * caller's transaction and a transaction of its own outside one, so the flow
 * is all-or-nothing either way. Rows come from the last statement, as
 * node-postgres' multi-result unwrapping does for pg-boss.
 *
 * Measured (candidate-3/proto/parked.ts): flow inside an app transaction
 * commits and rolls back with it; flow outside one inserts both jobs.
 */
export const runParameterless = (
    _sql: SqlClient.SqlClient,
    _text: string,
): Effect.Effect<{ rows: Array<any> }, SqlError> => Effect.die(new Error("not implemented"));

/**
 * A worker transaction that is a real `sql.withTransaction` on the native
 * client.
 *
 * ```
 * beginTransaction()
 *   fork (into the PgBoss layer's FiberSet):
 *     sql.withTransaction(
 *       ctx <- Effect.context()          // has the transaction connection,
 *       ready.succeed(ctx)               // its savepoint semaphore, deferred
 *       outcome.await                    // Reactivity, afterCommit buffer
 *     )
 *   ctx <- ready.await                   // BEGIN failed -> reject
 *   db  = connectionDb(ctx); register(db, ctx)
 *   return { db,
 *     commit:   outcome.succeed(); exit <- fiber.await; failure -> reject(driver error)
 *     rollback: outcome.fail(Rollback) once; fiber.await; never rejects }
 * ```
 *
 * Why this shape, not providing `transactionService` ourselves: the context
 * comes from `withTransaction`, so a nested `withTransaction` in the handler
 * finds its savepoint semaphore (no `SqlClient.ts:676` defect) and
 * `afterCommit` runs after the real COMMIT. Measured: nested savepoint works,
 * an `afterCommit` callback sees the job already `completed` from another
 * connection, and a failing handler leaves no app rows.
 *
 * `rollback` is bounded by the fiber's own ROLLBACK; if the fiber is wedged,
 * interrupting it releases the connection (pg-boss requires a time-bounded,
 * never-rejecting rollback, `types.d.ts:70-84`).
 */
export const parkedTransaction = (_options: {
    readonly sql: SqlClient.SqlClient;
    readonly fork: <A, E>(effect: Effect.Effect<A, E, SqlClient.SqlClient>) => Fiber.Fiber<A, E>;
    readonly register: (db: Db, context: Context.Context<SqlClient.SqlClient>) => void;
}): Promise<TransactionHandle> => Promise.reject(new Error("not implemented"));

/**
 * The constructor `db` for `new PgBoss({ db })`.
 *
 * - `executeSql`: pg-boss's own statements (migrations, maintenance, fetch,
 *   non-transactional settles) go to the node-postgres sidecar. They need the
 *   simple protocol, and pg-boss's internals were written against
 *   node-postgres row types; keeping them there means app rows and pg-boss
 *   rows never share a decoder.
 * - `beginTransaction`: {@link parkedTransaction} on the native client.
 * - `listen`: TODO via `PgClient.listen` (native LISTEN), so
 *   `useListenNotify` needs no sidecar connection.
 *
 * pg-boss never opens or closes a custom db (`index.js:207, 277, 364`); the
 * layer's scope owns both pools.
 */
export const bossDb = (options: {
    readonly housekeeping: Housekeeping;
    readonly beginTransaction: () => Promise<TransactionHandle>;
}): Db => ({
    executeSql: options.housekeeping.executeSql,
    beginTransaction: options.beginTransaction,
});

/**
 * Remembers which `SqlError` a rejected driver error came from, so the
 * library's `Effect.tryPromise` boundary can fail with the original.
 */
export const originalSqlError: WeakMap<object, SqlError> = new WeakMap();

/** `SqlError` -> the driver error pg-boss expects; anything else unchanged. */
export const toDriverError = (error: unknown): unknown => {
    // TODO
    // if (!isSqlError(error)) return error
    // const cause = error.reason.cause  // Error & ErrorFields from PgConnection.classifyFields
    // if (cause instanceof Error) { originalSqlError.set(cause, error); return cause }
    // return error
    return error;
};
