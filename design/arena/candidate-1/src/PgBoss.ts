import * as Context from "effect/Context";
import * as Schema from "effect/Schema";

import type { PgNodeClient } from "./PgNodeClient.ts";
import type * as Effect from "effect/Effect";
import type * as Layer from "effect/Layer";
/**
 * The pg-boss instance as a service: started when the layer builds, stopped
 * when it closes, and running on the `PgNodeClient` pool.
 *
 * Most code never touches this module beyond `PgBoss.layer`. Sending, flows
 * and workers live on `JobQueue`. `PgBoss.use` is the escape hatch for the
 * rest of pg-boss's API.
 *
 * @since 0.1.0
 */
import type { ConnectionOptions, ConstructorOptions, PgBoss as PgBossJs, StopOptions } from "pg-boss";

/**
 * @since 0.1.0
 * @category models
 */
export interface PgBoss {
    /**
     * Runs one pg-boss call.
     *
     * `connection` is `{ db }` for the `effect/sql` transaction the calling
     * fiber is in, whether the app opened it with `sql.withTransaction` or
     * pg-boss opened it for a transactional worker, and `{}` outside one. Pass
     * it as the call's `ConnectionOptions` so the call joins the transaction. In
     * a transactional worker `db` is the very object pg-boss gave the handler,
     * so `boss.complete` / `boss.cancel` / `boss.fail` on the batch's own jobs
     * stay fenced.
     *
     * A rejection becomes `PgBossError`, with the driver error as `cause`.
     */
    readonly use: <A>(
        f: (boss: PgBossJs, connection: ConnectionOptions) => Promise<A>,
    ) => Effect.Effect<A, PgBossError>;
}

/**
 * @since 0.1.0
 * @category services
 */
export const PgBoss = Context.Service<PgBoss>("@wmaurer/effect-sql-pg-boss/PgBoss");

/**
 * Accessor for `PgBoss.use`.
 *
 * @since 0.1.0
 * @category accessors
 */
export const use = <A>(
    f: (boss: PgBossJs, connection: ConnectionOptions) => Promise<A>,
): Effect.Effect<A, PgBossError, PgBoss> => {
    void f;
    throw new Error("not implemented");
};

/**
 * A pg-boss call rejected.
 *
 * `cause` is what pg-boss threw. For SQL failures it is the node-postgres
 * `DatabaseError`, with `code` and `constraint`.
 *
 * @since 0.1.0
 * @category errors
 */
export class PgBossError extends Schema.TaggedError<PgBossError>("@wmaurer/effect-sql-pg-boss/PgBossError")(
    "PgBossError",
    {
        operation: Schema.String,
        message: Schema.String,
        cause: Schema.Defect(),
    },
) {}

/**
 * Options for the pg-boss instance.
 *
 * Connection options (`connectionString`, `host`, `max`, `db`, ...) are not
 * here: pg-boss always runs on the `PgNodeClient` pool.
 *
 * @since 0.1.0
 * @category models
 */
export interface PgBossOptions extends Omit<
    ConstructorOptions,
    | "db"
    | "connectionString"
    | "host"
    | "port"
    | "database"
    | "user"
    | "password"
    | "ssl"
    | "max"
    | "connectionTimeoutMillis"
    | "application_name"
    | "options"
> {
    /**
     * How the layer stops pg-boss when it closes. Defaults to
     * `{ graceful: true, timeout: 30_000 }`: workers finish their batches, then
     * whatever is still running is interrupted and its transactions roll back.
     * `close` is ignored; the pool belongs to `PgNodeClient`.
     */
    readonly stop?: Omit<StopOptions, "close"> | undefined;
    /** Bound on a transactional worker's rollback. Defaults to 5 seconds. */
    readonly rollbackTimeoutMillis?: number | undefined;
}

/**
 * Starts pg-boss on the `PgNodeClient` pool and stops it with the scope.
 *
 * `start()` installs or migrates the pg-boss schema unless `migrate: false`.
 * Queues are not created here: each `JobQueue` class has its own `layer`, so a
 * queue's existence and the code that uses it cannot drift apart.
 *
 * Finalization order: pg-boss stops first (graceful), then the fibers that
 * hold worker transactions are interrupted, then the pool closes (it is
 * `PgNodeClient`'s).
 *
 * @since 0.1.0
 * @category layers
 */
export const layer = (options?: PgBossOptions | undefined): Layer.Layer<PgBoss, PgBossError, PgNodeClient> => {
    // TODO:
    // const client = yield* PgNodeClient
    // const runFork = yield* FiberSet.makeRuntime<never>()        // finalized after boss.stop below
    // const base = yield* Effect.context<never>()
    // const db = BossDatabase.make({ pool: poolOf(client), sql: client, runFork, base, rollbackTimeoutMillis })
    // const boss = new PgBossJs({ ...options, db })
    // boss.on("error", (e) => runFork(Effect.logError("pg-boss", e)))
    // boss.on("warning", (w) => runFork(Effect.logWarning("pg-boss", w.message, w.data)))
    // yield* Effect.acquireRelease(tryPromise(() => boss.start()), () => promise(() => boss.stop({ graceful: true, timeout: 30_000, ...options?.stop, close: false })))
    //
    // const connection = Effect.map(Effect.serviceOption(client.transactionService), Option.match({
    //   onNone: () => ({}),
    //   onSome: ([conn]) => ({ db: conn })      // NodePgConnection is a pg-boss Db
    // }))
    // return { use: (f) => connection.pipe(flatMap((c) => tryPromise(() => f(boss, c)))) }
    void options;
    throw new Error("not implemented");
};
