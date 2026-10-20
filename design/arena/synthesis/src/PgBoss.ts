import * as Config from "effect/Config";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import type { Runtime, RuntimeId } from "./internal/runtime.ts";
/**
 * The running pg-boss instance, as a scoped layer over the app's native
 * `PgClient`.
 *
 * Most code touches this module only for `PgBoss.layer`. Sending, flows and
 * workers live on the queue classes (`JobQueue`); {@link use} is the escape
 * hatch for the rest of pg-boss's API.
 *
 * @since 0.1.0
 */
import type { PgClient } from "@effect/sql-pg/PgClient";
import type * as Duration from "effect/Duration";
import type { SqlError } from "effect/sql/SqlError";
import type * as Boss from "pg-boss";

/**
 * A pg-boss call failed for a reason other than SQL: argument validation, a
 * lost claim, start-up, shutdown, or a worker that does not fit the pool.
 * SQL failures surface as the original `SqlError` instead.
 *
 * @since 0.1.0
 * @category errors
 */
export class PgBossError extends Schema.TaggedError<PgBossError>("@wmaurer/effect-sql-pg-boss/PgBossError")(
    "PgBossError",
    {
        operation: Schema.String,
        cause: Schema.Defect(),
    },
) {}

/**
 * pg-boss constructor options the caller may set. Connection fields and `db`
 * are excluded (only `schema` stays): pg-boss reaches the database through
 * the app's `PgClient`.
 *
 * @since 0.1.0
 * @category models
 */
export interface Options extends Omit<Boss.ConstructorOptions, Exclude<keyof Boss.DatabaseOptions, "schema">> {
    /**
     * Connections pg-boss uses for polling, maintenance and the settles of
     * non-transactional workers. Separate from the PgClient pool, so handlers
     * holding app connections cannot starve them. Defaults to 4.
     */
    readonly maintenanceConnections?: number | undefined;
    /**
     * PgClient connections kept free of transactional workers for the rest of
     * the app. A worker layer whose `localConcurrency` would eat into them fails
     * at build. Defaults to 2.
     */
    readonly connectionReserve?: number | undefined;
    /**
     * How long a transactional worker's rollback may take before it gives up
     * on the connection. pg-boss requires a bounded rollback. Defaults to
     * 5 seconds.
     */
    readonly rollbackTimeout?: Duration.Input | undefined;
    /**
     * How the layer stops pg-boss when it closes. Defaults to
     * `{ graceful: true, timeout: 30_000 }`: handlers finish their batches, then
     * whatever is still running is interrupted and its transactions roll back.
     */
    readonly stop?: Omit<Boss.StopOptions, "close"> | undefined;
}

/**
 * The running pg-boss for this app.
 *
 * @since 0.1.0
 * @category services
 */
export class PgBoss extends Context.Service<
    PgBoss,
    {
        /** See {@link use}. */
        readonly use: <A>(
            f: (boss: Boss.PgBoss, connection: Boss.ConnectionOptions) => Promise<A>,
        ) => Effect.Effect<A, PgBossError | SqlError>;
        readonly [RuntimeId]: Runtime;
    }
>()("@wmaurer/effect-sql-pg-boss/PgBoss") {}

/**
 * Escape hatch for the pg-boss API the queue classes do not cover (cancel,
 * retry, stats, schedules, ...).
 *
 * Pass `connection` as the call's options. Inside `sql.withTransaction`, or
 * in a transactional worker, it is `{ db }` for that transaction: the call
 * joins it, and in a worker `db` is the object pg-boss fences the batch's
 * settles on. Outside a transaction it is `{}`, and pg-boss runs the call on
 * its own connections.
 *
 * Prefer reads outside a transaction: inside one, pg-boss's read APIs return
 * rows decoded by the native client (timestamps as epoch milliseconds, not
 * `Date`).
 *
 * @since 0.1.0
 * @category accessors
 */
export const use = <A>(
    f: (boss: Boss.PgBoss, connection: Boss.ConnectionOptions) => Promise<A>,
): Effect.Effect<A, PgBossError | SqlError, PgBoss> => PgBoss.use((service) => service.use(f));

/**
 * Starts pg-boss over the app's native `PgClient`.
 *
 * `start()` installs or migrates the pg-boss schema unless `migrate: false`.
 * Queues are created by each queue class's own `layer`.
 *
 * Requirement: the `SqlClient` the app's handlers use is this `PgClient`.
 * `PgClient.layer` provides both from one instance; transactional workers
 * find their transaction through it.
 *
 * Finalizers run in reverse: `boss.stop()` (graceful, `close: false`), then
 * the worker transactions still open are interrupted and roll back, then the
 * maintenance connections close.
 *
 * @since 0.1.0
 * @category layers
 */
export const layer = (_options?: Options | undefined): Layer.Layer<PgBoss, PgBossError | SqlError, PgClient> =>
    // TODO, every step below runs for real in prototype/synth.ts:
    // const sql = yield* PgClient
    // const housekeeping = yield* Housekeeping.make(sql.config, options.maintenanceConnections ?? 4)
    // const fork = yield* FiberSet.makeRuntime<never>()
    // const base = yield* Effect.context<never>()
    // const db: Db = {
    //   executeSql: housekeeping.executeSql,
    //   beginTransaction: () => WorkerTransaction.begin({ sql, fork, base, rollbackTimeout: options.rollbackTimeout ?? "5 seconds" })
    // }
    // const boss = new Boss.PgBoss({ ...bossOptions, db })
    // boss.on("error", (e) => fork(Effect.logError("pg-boss", e)))
    // boss.on("warning", (w) => fork(Effect.logWarning("pg-boss", w.message, w.data)))
    // yield* Effect.acquireRelease(
    //   Connection.call("start", () => boss.start()),
    //   () => Effect.promise(() => boss.stop({ graceful: true, timeout: 30_000, ...options.stop, close: false })))
    // const reserveConnections = connectionBudget(((sql.config as PgPoolConfig).maxConnections ?? 10) - (options.connectionReserve ?? 2))
    // return PgBoss.of({
    //   use: (f) => Effect.flatMap(Connection.currentDb(sql), (db) => Connection.call("use", () => f(boss, db === undefined ? {} : { db }))),
    //   [RuntimeId]: { boss, sql, reserveConnections }
    // })
    Layer.effect(PgBoss, Effect.die(new Error("not implemented")));

/**
 * {@link layer} with options read from `Config`.
 *
 * @since 0.1.0
 * @category layers
 */
export const layerConfig = (
    options: Config.Wrap<Options>,
): Layer.Layer<PgBoss, Config.ConfigError | PgBossError | SqlError, PgClient> =>
    Layer.unwrap(Effect.map(Config.unwrap(options), layer));
