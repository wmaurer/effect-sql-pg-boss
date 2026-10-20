import * as Effect from "effect/Effect";
import * as Redacted from "effect/Redacted";
import pg, { type PoolConfig } from "pg";

import { PgBossError } from "../PgBoss.ts";

import type { Db } from "./connection.ts";
/**
 * The node-postgres pool pg-boss runs its own statements on: migrations,
 * maintenance, BAM, fetch, and the settles of non-transactional workers.
 *
 * It exists because `@effect/sql-pg` has no simple-protocol call, and those
 * statements are multi-statement scripts without values. This file and the
 * `pg` dependency are deleted once `PgClient.simple` lands upstream.
 *
 * Invariant: app rows never travel through this pool.
 *
 * @internal
 */
import type { PgClientConfig } from "@effect/sql-pg/PgClient";
import type * as Scope from "effect/Scope";

export interface Housekeeping {
    readonly executeSql: Db["executeSql"];
}

/**
 * Builds the pool from the app's `PgClient.config`, so both pools reach the
 * same database with the same credentials and the caller configures nothing.
 * A `password` Effect runs per connection, as on the native client.
 *
 * The pool ends when the scope closes. The PgBoss layer acquires it first,
 * so it ends after `boss.stop()`.
 */
export const make = (
    config: PgClientConfig,
    maxConnections: number,
): Effect.Effect<Housekeeping, PgBossError, Scope.Scope> =>
    Effect.gen(function* () {
        if (config.stream !== undefined) {
            return yield* new PgBossError({
                operation: "maintenanceConnections",
                cause: new Error("a PgClient with a custom stream cannot be reached from node-postgres"),
            });
        }
        const pool = yield* Effect.acquireRelease(
            Effect.sync(() => new pg.Pool(poolConfig(config, maxConnections))),
            (pool) => Effect.promise(() => pool.end()),
        );
        // node-postgres calls this outside any fiber. It logs with the default logger until PgBoss.layer passes its context.
        // @effect-diagnostics-next-line runEffectInsideEffect:off
        const logError = (error: Error) => Effect.runFork(Effect.logWarning("pg-boss maintenance connection", error));
        pool.on("error", logError);
        pool.on("connect", (client) => client.on("error", logError));
        return {
            executeSql: (text, values) =>
                // A multi-statement script returns one result per statement; keep every
                // statement's rows so a RETURNING before COMMIT is not lost.
                pool.query(text, values).then((result) => ({
                    rows: Array.isArray(result) ? result.flatMap((r) => r.rows) : result.rows,
                })),
        };
    });

/**
 * Fields the app's config leaves unset stay `undefined`, which node-postgres
 * treats as absent and fills from `PG*` environment variables and defaults.
 */
const poolConfig = (config: PgClientConfig, max: number): PoolConfig => ({
    max,
    connectionString: config.url === undefined ? undefined : Redacted.value(config.url),
    host: config.path ?? config.host,
    port: config.port,
    database: config.database,
    user: config.username,
    ssl: config.ssl,
    application_name: config.applicationName ?? "@wmaurer/effect-sql-pg-boss",
    password: passwordOf(config.password),
});

const passwordOf = (password: PgClientConfig["password"]): PoolConfig["password"] => {
    if (password === undefined) return undefined;
    if (Redacted.isRedacted(password)) return Redacted.value(password);
    return () => Effect.runPromise(Effect.map(password, Redacted.value));
};
