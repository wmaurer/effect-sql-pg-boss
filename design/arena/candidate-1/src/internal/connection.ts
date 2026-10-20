import type * as Effect from "effect/Effect";
import type { Connection } from "effect/sql/SqlConnection";
import type { SqlError } from "effect/sql/SqlError";
import type * as Stream from "effect/Stream";
import type * as Pg from "pg";
/**
 * One node-postgres connection, seen two ways.
 *
 * `NodePgConnection` is both the `effect/sql` `Connection` that `SqlClient`
 * runs statements on and the pg-boss `Db` that pg-boss runs its own SQL on.
 * Making them the same object is the load-bearing decision of the package:
 *
 * - pg-boss fences a transactional handler's settles by the identity of the
 *   `db` it was given (`manager.js:671-674`). Every path in this package that
 *   hands pg-boss a `db` for a transaction hands it this object, so identity
 *   holds by construction. There is no wrapper to cache and no map to keep in
 *   sync.
 * - "The pg-boss `db` for the current `effect/sql` transaction" is a cast
 *   after an `instanceof` check, not a lookup.
 *
 * Not exported from the package.
 */
import type { Db } from "pg-boss";

type TransformRows = (<A extends object>(row: ReadonlyArray<A>) => ReadonlyArray<A>) | undefined;

export class NodePgConnection implements Connection, Db {
    /** A pooled client leased for a statement, a stream, or a transaction. */
    readonly client: Pg.PoolClient;

    constructor(client: Pg.PoolClient) {
        this.client = client;
    }

    // ---------------------------------------------------------------------------
    // effect/sql side. Every failure is a `SqlError` whose `reason.cause` is the
    // untouched `pg.DatabaseError`, so `code`, `constraint` and `detail` survive
    // for anyone (pg-boss included) who unwraps it.
    // ---------------------------------------------------------------------------

    execute(
        sql: string,
        params: ReadonlyArray<unknown>,
        transformRows: TransformRows,
    ): Effect.Effect<ReadonlyArray<any>, SqlError> {
        // TODO: client.query({ text: sql, values: params }) -> rows, transformRows?.(rows)
        (void sql, params, transformRows);
        throw new Error("not implemented");
    }

    executeRaw(sql: string, params: ReadonlyArray<unknown>): Effect.Effect<unknown, SqlError> {
        (void sql, params);
        throw new Error("not implemented");
    }

    executeStream(
        sql: string,
        params: ReadonlyArray<unknown>,
        transformRows: TransformRows,
    ): Stream.Stream<any, SqlError> {
        // TODO: pg-cursor (optional peer). Without it, fall back to one query
        // emitted as a single chunk, like the pre-#7426 pg client did.
        (void sql, params, transformRows);
        throw new Error("not implemented");
    }

    executeValues(
        sql: string,
        params: ReadonlyArray<unknown>,
    ): Effect.Effect<ReadonlyArray<ReadonlyArray<unknown>>, SqlError> {
        // TODO: client.query({ text, values, rowMode: "array" })
        (void sql, params);
        throw new Error("not implemented");
    }

    executeValuesUnprepared(
        sql: string,
        params: ReadonlyArray<unknown>,
    ): Effect.Effect<ReadonlyArray<ReadonlyArray<unknown>>, SqlError> {
        return this.executeValues(sql, params);
    }

    /**
     * With no params this sends the text through the simple query protocol, so
     * it may hold several statements. `sql.unsafe` migrations, the transaction
     * control statements, and `Migrator` files rely on that.
     */
    executeUnprepared(
        sql: string,
        params: ReadonlyArray<unknown>,
        transformRows: TransformRows,
    ): Effect.Effect<ReadonlyArray<any>, SqlError> {
        // TODO: client.query(sql, params.length === 0 ? undefined : params)
        (void sql, params, transformRows);
        throw new Error("not implemented");
    }

    // ---------------------------------------------------------------------------
    // pg-boss side. Plain promises, no Effect, no spans, no row transforms:
    // pg-boss expects raw node-postgres rows and raw driver errors.
    // ---------------------------------------------------------------------------

    /**
     * pg-boss's `Db.executeSql`. node-postgres sends a query with no values, or
     * an empty array, through the simple protocol (`Query.requiresPreparation`),
     * which is what `boss.flow(..., { db })` needs for its `;\n`-joined
     * statements. Errors are rethrown as node-postgres raised them, so pg-boss
     * can read `err.code` (22012, 23505, 25P02, ...) and `err.constraint`.
     */
    executeSql(text: string, values?: Array<unknown>): Promise<{ rows: Array<any> }> {
        return this.client.query(text, values);
    }

    /**
     * Drops the connection instead of returning it to the pool. Used when a
     * rollback does not get through in time: pg-boss requires `rollback()` to
     * make progress, and a destroyed socket ends the transaction server-side.
     */
    destroy(cause: Error): void {
        this.client.release(cause);
    }
}

/** Narrows a connection read from `sql.transactionService`. */
export const isNodePgConnection = (u: unknown): u is NodePgConnection => u instanceof NodePgConnection;

/**
 * The pool behind each `PgNodeClient`. Written once by `PgNodeClient.make`,
 * read only by `PgBoss.layer`. Kept off the public interface so app code has
 * no second way to reach a connection.
 */
const pools = new WeakMap<object, Pg.Pool>();

export const registerPool = (client: object, pool: Pg.Pool): void => {
    pools.set(client, pool);
};

export const poolOf = (client: object): Pg.Pool => {
    const pool = pools.get(client);
    if (pool === undefined) throw new Error("PgNodeClient has no pool: it was not built by PgNodeClient.make");
    return pool;
};
