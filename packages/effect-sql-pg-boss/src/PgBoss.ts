import * as PgClient from "@effect/sql-pg/PgClient";
import * as Config from "effect/Config";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FiberSet from "effect/FiberSet";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Boss from "pg-boss";

import * as Connection from "./internal/connection.ts";
import * as Housekeeping from "./internal/housekeeping.ts";
import { connectionBudget, type Runtime, RuntimeId } from "./internal/runtime.ts";
import * as WorkerTransaction from "./internal/workerTransaction.ts";

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
import type * as Duration from "effect/Duration";
import type { SqlError } from "effect/sql/SqlError";

/**
 * A pg-boss call failed for a reason other than SQL: argument validation, a
 * lost claim, start-up, shutdown, or a worker that does not fit the pool.
 * SQL failures surface as the original `SqlError` instead.
 *
 * @since 0.1.0
 * @category errors
 */
export class PgBossError extends Schema.TaggedError<PgBossError, { readonly brand: unique symbol }>(
    "@wmaurer/effect-sql-pg-boss/PgBossError",
)("PgBossError", {
    operation: Schema.String,
    cause: Schema.Defect(),
}) {}

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
 * Transactional workers may hold the PgClient pool's `maxConnections`
 * (10 when unset) minus `connectionReserve`. They are not supported on a
 * `PgClient.makeClient` client, where a batch would pin the only connection.
 *
 * Finalizers run in reverse: `boss.stop()` (graceful, `close: false`), then
 * the worker transactions still open are interrupted and roll back, then the
 * maintenance connections close.
 *
 * @since 0.1.0
 * @category layers
 */
export const layer = (options: Options = {}): Layer.Layer<PgBoss, PgBossError | SqlError, PgClient.PgClient> =>
    Layer.effect(
        PgBoss,
        Effect.gen(function* () {
            const { maintenanceConnections, connectionReserve, rollbackTimeout, stop, ...bossOptions } = options;
            const sql = yield* PgClient.PgClient;
            const housekeeping = yield* Housekeeping.make(sql.config, maintenanceConnections ?? 4);
            const fork = yield* FiberSet.makeRuntime<never>();
            const base = yield* Effect.context<never>();
            const boss = new Boss.PgBoss({
                ...bossOptions,
                db: {
                    executeSql: housekeeping.executeSql,
                    beginTransaction: () =>
                        WorkerTransaction.begin({ sql, fork, base, rollbackTimeout: rollbackTimeout ?? "5 seconds" }),
                },
            });
            boss.on("error", (error) => fork(Effect.logError("pg-boss", error)));
            boss.on("warning", (warning) => fork(Effect.logWarning("pg-boss", warning.message, warning.data)));
            yield* Effect.acquireRelease(
                Connection.call("start", () => boss.start()),
                () => Effect.promise(() => boss.stop({ graceful: true, timeout: 30_000, ...stop, close: false })),
            );
            // SAFETY: `PgClient.make` stores its `PgPoolConfig` as `config`. A `makeClient` config has
            // no `maxConnections` and gets the pool default; transactional workers are unsupported there.
            const maxConnections = (sql.config as PgClient.PgPoolConfig).maxConnections ?? 10;
            return PgBoss.of({
                use: (f) =>
                    Effect.flatMap(Connection.currentDb(sql), (db) =>
                        Connection.call("use", () => f(boss, db === undefined ? {} : { db })),
                    ),
                [RuntimeId]: {
                    boss,
                    sql,
                    reserveConnections: connectionBudget(maxConnections - (connectionReserve ?? 2)),
                },
            });
        }),
    );

/**
 * {@link layer} with options read from `Config`.
 *
 * @since 0.1.0
 * @category layers
 */
export const layerConfig = (
    options: Config.Wrap<Options>,
): Layer.Layer<PgBoss, Config.ConfigError | PgBossError | SqlError, PgClient.PgClient> =>
    Layer.unwrap(Effect.map(Config.unwrap(options), layer));
