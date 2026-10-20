import type { BossError } from "../Boss.ts";
import type { NodePgClient } from "../NodePgClient.ts";
/**
 * pg-boss' view of "whatever connection this fiber is using right now".
 *
 * `executeSql` runs `sql.unsafe(text, values)` on the `NodePgClient` found in
 * the context of the calling fiber. Inside `sql.withTransaction` that is the
 * transaction's connection, so pg-boss statements join the app's transaction.
 * Outside a transaction it is a pooled connection and each call autocommits.
 * Inside a transactional worker the client is the borrowed one, so it is the
 * worker's transaction.
 *
 * Invariants:
 * - The Effect context is captured when the adapter is built, not when the
 *   promise runs, so a transaction opened by the caller is the one used.
 * - A failing statement rejects with the driver error (`SqlError.reason.cause`
 *   when it is one), keeping `code`, `constraint` and `detail`. The typed
 *   `SqlError` is kept aside and returned by `failure()` so the Effect caller
 *   sees the same typed error as for any other statement.
 * - `flow` without a transaction is still atomic: PostgreSQL runs one
 *   multi-statement simple query as an implicit transaction.
 *
 * @internal
 */
import type * as Effect from "effect/Effect";
import type { SqlError } from "effect/sql/SqlError";
import type * as Pg from "pg";
import type { Db } from "pg-boss";

/** @internal */
export interface Ambient {
    readonly db: Db;
    /** The typed error of the last failed statement, if any. */
    readonly failure: () => SqlError | undefined;
}

/** @internal */
export const make = (_pool: Pg.Pool): Effect.Effect<Ambient, BossError, NodePgClient> => {
    // gen:
    //   sql = yield* NodePgClient
    //   if (sql.pool !== pool) fail BossError Driver "the ambient NodePgClient is not on this Boss's pool"
    //   ctx = yield* Effect.context<never>()
    //   let failure: SqlError | undefined
    //   db = { executeSql: (text, values) =>
    //     Effect.runPromiseWith(ctx)(values?.length ? sql.unsafe(text, values) : sql.unsafe(text))
    //       .then(rows => ({ rows }), (e) => { failure = e; throw driverError(e) }) }
    //   return { db, failure: () => failure }
    throw new Error("not implemented");
};
