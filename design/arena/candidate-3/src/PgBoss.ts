/**
 * The running pg-boss instance, as a scoped layer over the app's native
 * `PgClient`.
 *
 * @since 0.1.0
 */
import * as Config from "effect/Config";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import type { Db } from "./internal/database.ts";
import type { HousekeepingOptions } from "./internal/housekeeping.ts";
import type { Runtime, RuntimeId } from "./internal/runtime.ts";
import type { PgClient } from "@effect/sql-pg/PgClient";
import type { SqlError } from "effect/sql/SqlError";
import type * as Boss from "pg-boss";

/**
 * A pg-boss call failed for a reason other than SQL: argument validation, a
 * lost claim, start-up or shutdown. SQL failures surface as the original
 * `SqlError` instead.
 *
 * @since 0.1.0
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
 * are excluded (only `schema` stays): the library derives the connection
 * from the `PgClient`.
 *
 * @since 0.1.0
 */
export interface Options extends Omit<Boss.ConstructorOptions, Exclude<keyof Boss.DatabaseOptions, "schema"> | "db"> {
    /**
     * The pool pg-boss uses for its own housekeeping. Ignored, without breaking
     * callers, once `@effect/sql-pg` can run pg-boss's scripts natively.
     */
    readonly housekeeping?: HousekeepingOptions | undefined;
    /** Passed to `boss.stop()`. Defaults to graceful, waiting for running handlers. */
    readonly stop?: Boss.StopOptions | undefined;
}

/**
 * The running pg-boss for this app.
 *
 * Its only public member is {@link use}; typed sending and working go through
 * `JobQueue` and `JobWorker`, which read the hidden runtime.
 *
 * @since 0.1.0
 */
export class PgBoss extends Context.Service<
    PgBoss,
    {
        readonly use: <A>(
            f: (boss: Boss.PgBoss, connection: { readonly db: Db }) => Promise<A>,
        ) => Effect.Effect<A, PgBossError | SqlError>;
        readonly [RuntimeId]: Runtime;
    }
>()("@wmaurer/effect-sql-pg-boss/PgBoss") {}

/**
 * Escape hatch for the pg-boss API the typed modules do not cover (cancel,
 * retry, stats, schedules). `db` follows the same rule as `JobQueue.send`:
 * pass it as `{ db }` and the call joins the fiber's transaction, or a
 * transactional worker's, by the exact object pg-boss fences settles on.
 *
 * @since 0.1.0
 */
export const use = <A>(
    f: (boss: Boss.PgBoss, connection: { readonly db: Db }) => Promise<A>,
): Effect.Effect<A, PgBossError | SqlError, PgBoss> =>
    Effect.gen(function* () {
        const service = yield* PgBoss;
        return yield* service.use(f);
    });

/**
 * Starts pg-boss over the app's native `PgClient`.
 *
 * Acquisition, in order (finalizers run in reverse):
 * 1. Housekeeping pool from `PgClient.config`.
 * 2. A FiberSet that owns parked worker transactions; closing it interrupts
 *    any still open, which rolls them back.
 * 3. `new PgBoss({ ...options, db: bossDb })`, an `error` listener that logs
 *    through Effect (an unhandled `error` event would crash the process),
 *    `boss.start()` (installs or migrates the schema; idempotent across
 *    instances).
 * 4. Finalizer: `boss.stop(options.stop ?? { graceful: true, wait: true })`.
 *    Graceful stop lets in-flight handlers commit before step 2 interrupts
 *    whatever is left.
 *
 * Invariant: the `SqlClient` the app's handlers use is this `PgClient`.
 * `PgClient.layer` provides both from one instance; the worker relies on it
 * to find the parked transaction under `sql.transactionService`.
 *
 * @since 0.1.0
 */
export const layer = (_options?: Options): Layer.Layer<PgBoss, PgBossError | SqlError, PgClient> =>
    // TODO
    // Layer.effect(PgBoss, Effect.gen(function*() {
    //   const sql = yield* PgClient
    //   const housekeeping = yield* Housekeeping.make(sql.config, options.housekeeping ?? {})
    //   const fork = yield* FiberSet.makeRuntime<SqlClient>()
    //   const contexts = new WeakMap<Db, Context<SqlClient>>()      // parked tx db -> its context
    //   const byConnection = new WeakMap<Connection, Db>()         // see Runtime.currentDb
    //   const db = Database.bossDb({ housekeeping, beginTransaction: () =>
    //     Database.parkedTransaction({ sql, fork, register: (db, ctx) => { contexts.set(db, ctx); byConnection.set(connOf(ctx), db) } }) })
    //   const boss = new Boss.PgBoss({ ...options, db })
    //   boss.on("error", (e) => fork(Effect.logError("pg-boss", e)))
    //   yield* Effect.acquireRelease(call("start", () => boss.start()), () => Effect.promise(() => boss.stop(stopOptions)))
    //   return PgBoss.of({ use, [RuntimeId]: runtime })
    // }))
    Layer.effect(PgBoss, Effect.die(new Error("not implemented")));

/**
 * {@link layer} with options read from `Config`.
 *
 * @since 0.1.0
 */
export const layerConfig = (
    options: Config.Wrap<Options>,
): Layer.Layer<PgBoss, Config.ConfigError | PgBossError | SqlError, PgClient> =>
    Layer.unwrap(Effect.map(Config.unwrap(options), layer));
