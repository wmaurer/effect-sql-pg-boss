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
        const logError = (error: Error) => Effect.runFork(Effect.logWarning("pg-boss maintenance connection", error));
        pool.on("error", logError);
        pool.on("connect", (client) => client.on("error", logError));
        return {
            executeSql: async (text, values) => {
                const result = await pool.query(text, values);
                // A multi-statement script returns one result per statement; keep every
                // statement's rows so a RETURNING before COMMIT is not lost.
                return { rows: Array.isArray(result) ? result.flatMap((r) => r.rows) : result.rows };
            },
        };
    });

const poolConfig = (config: PgClientConfig, max: number): PoolConfig => {
    const password = config.password;
    return {
        max,
        ...(config.url !== undefined ? { connectionString: Redacted.value(config.url) } : {}),
        ...(config.path !== undefined ? { host: config.path } : config.host !== undefined ? { host: config.host } : {}),
        ...(config.port !== undefined ? { port: config.port } : {}),
        ...(config.database !== undefined ? { database: config.database } : {}),
        ...(config.username !== undefined ? { user: config.username } : {}),
        ...(config.ssl !== undefined ? { ssl: config.ssl } : {}),
        application_name: config.applicationName ?? "@wmaurer/effect-sql-pg-boss",
        ...(password === undefined
            ? {}
            : Redacted.isRedacted(password)
              ? { password: Redacted.value(password) }
              : { password: () => Effect.runPromise(Effect.map(password, Redacted.value)) }),
    };
};
