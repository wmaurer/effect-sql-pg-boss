/**
 * Where a pg-boss call runs, and how its failure comes back.
 *
 * One rule: a pg-boss call made from a fiber runs on that fiber's current
 * `effect/sql` transaction connection. Outside a transaction, {@link withDb}
 * opens one, so a `flow` stays all-or-nothing there too.
 *
 * The pg-boss `Db` for a connection talks to the `Connection` directly
 * (`executeUnprepared`), not through `sql.unsafe`: pg-boss gets raw rows (no
 * `transformResultNames`, no `transformJson`), and the `Db` holds no captured
 * fiber context that could go stale.
 *
 * Shrink plan: {@link runScript} becomes one `PgClient.simple` call once that
 * exists upstream; nothing else here changes.
 *
 * @internal
 */
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import { isSqlError, type SqlError } from "effect/sql/SqlError";

import { PgBossError } from "../PgBoss.ts";

import type * as SqlClient from "effect/sql/SqlClient";
import type { Connection } from "effect/sql/SqlConnection";
import type * as Boss from "pg-boss";

/** pg-boss's `IDatabase`, which pg-boss exports as `Db`. */
export type Db = Boss.Db;

/** pg-boss's transaction handle. pg-boss does not export the name. */
export type TransactionHandle = Awaited<ReturnType<NonNullable<Db["beginTransaction"]>>>;

/**
 * pg-boss joins a flow's statements with exactly this separator and binds no
 * values (pg-boss 12.38 `manager.js:1840-1843`). No statement contains it: the
 * templates have none, and JSON data cannot produce a raw newline. The peer
 * range is pinned to `~12.38` and a test fails if the statement count of a
 * known flow changes.
 */
export const FLOW_SEPARATOR = ";\n";

const dbs = new WeakMap<Connection, Db>();

/**
 * The `Db` for `conn`: created on first use, the same object after that.
 *
 * pg-boss fences a transactional handler's settles by the identity of its
 * `db` (`manager.js:671-674`), so every call on one transaction must pass one
 * object. Native `PgClient` hands each transaction a fresh `Connection`, so
 * the cache never carries a `Db` across transactions.
 *
 * `isReleased` is the worker transaction's released guard. Only the worker
 * transaction passes it, and it calls this right after `BEGIN`, before any
 * other caller can see the connection.
 */
export const dbFor = (conn: Connection, isReleased?: () => boolean): Db => {
    let db = dbs.get(conn);
    if (db === undefined) {
        db = makeDb(conn, isReleased);
        dbs.set(conn, db);
    }
    return db;
};

const makeDb = (conn: Connection, isReleased: (() => boolean) | undefined): Db => ({
    executeSql: (text, values) => {
        if (isReleased?.() === true) {
            return Promise.reject(new Error("pg-boss used a worker transaction after it was settled"));
        }
        const statement =
            values === undefined ? runScript(conn, text) : conn.executeUnprepared(text, values, undefined);
        return Effect.runPromiseExit(statement).then((exit) =>
            Exit.isSuccess(exit) ? { rows: [...exit.value] } : Promise.reject(toDriverError(Cause.squash(exit.cause))),
        );
    },
});

/**
 * A parameterless script. Only `flow` sends one on a `{ db }` path. Runs each
 * statement in order on the connection and returns the last one's rows.
 *
 * A failing part aborts the surrounding transaction, which the caller can no
 * longer commit, so the flow is all-or-nothing without a savepoint.
 */
export const runScript = (conn: Connection, text: string): Effect.Effect<ReadonlyArray<any>, SqlError> =>
    Effect.map(
        Effect.forEach(
            text.split(FLOW_SEPARATOR).filter((part) => part.trim() !== ""),
            (part) => conn.executeUnprepared(part, [], undefined),
        ),
        (results) => results.at(-1) ?? [],
    );

/** The `Db` of the fiber's current transaction, if it is in one. */
export const currentDb = (sql: SqlClient.SqlClient): Effect.Effect<Db | undefined> =>
    Effect.map(
        Effect.serviceOption(sql.transactionService),
        Option.match({ onNone: () => undefined, onSome: ([conn]) => dbFor(conn) }),
    );

/**
 * Runs `f` with the `Db` of the fiber's transaction, inside a transaction of
 * its own when the fiber is outside one.
 */
export const withDb = <A, E, R>(
    sql: SqlClient.SqlClient,
    f: (db: Db) => Effect.Effect<A, E, R>,
): Effect.Effect<A, E | SqlError, R> => {
    const joined = Effect.flatMap(currentDb(sql), (db) =>
        db === undefined ? Effect.die(new Error("withDb: no transaction connection after withTransaction")) : f(db),
    );
    return Effect.flatMap(Effect.serviceOption(sql.transactionService), (level) =>
        Option.isSome(level) ? joined : sql.withTransaction(joined),
    );
};

/**
 * Which `SqlError` a rejected driver error came from, so {@link call} can fail
 * with the original rather than a copy.
 */
const originalSqlError = new WeakMap<object, SqlError>();

/**
 * What a `Db` rejects with. pg-boss branches on the driver error's SQLSTATE
 * `code`, `constraint` and `detail` (23505, 22012, 25P02, ...), which the
 * native client keeps on the `SqlError` reason's `cause`.
 */
export const toDriverError = (error: unknown): unknown => {
    if (isSqlError(error)) {
        const cause = error.reason.cause;
        if (cause instanceof Error) {
            originalSqlError.set(cause, error);
            return cause;
        }
    }
    return error;
};

/**
 * The one Promise boundary. A rejection that came from a `Db` fails with the
 * original `SqlError` (so `UniqueViolation` stays matchable); anything else is
 * a `PgBossError` naming `operation`.
 */
export const call = <A>(operation: string, f: () => Promise<A>): Effect.Effect<A, PgBossError | SqlError> =>
    Effect.tryPromise({
        try: f,
        catch: (cause) => {
            if (isSqlError(cause)) return cause;
            const original = typeof cause === "object" && cause !== null ? originalSqlError.get(cause) : undefined;
            return original ?? new PgBossError({ operation, cause });
        },
    });
