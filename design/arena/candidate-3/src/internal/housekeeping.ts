/**
 * The node-postgres pool pg-boss uses for its own statements.
 *
 * Exists for one reason: `@effect/sql-pg` has no simple-protocol call, and
 * pg-boss's migrations, maintenance and BAM send multi-statement scripts
 * without values. This file, and the `pg` dependency, are deleted when
 * `PgClient.simple` lands upstream.
 *
 * Invariant: app rows never travel through this pool. Only pg-boss's own
 * constructor-db statements do.
 *
 * @internal
 */
import * as Effect from "effect/Effect";

import type { PgBossError } from "../PgBoss.ts";
import type { Db } from "./database.ts";
import type { PgClientConfig } from "@effect/sql-pg/PgClient";
import type * as Scope from "effect/Scope";

export interface Housekeeping {
    readonly executeSql: Db["executeSql"];
}

export interface HousekeepingOptions {
    /** Defaults to 4: pg-boss's polling, maintenance and non-transactional settles. */
    readonly maxConnections?: number | undefined;
}

/**
 * Builds the pool from the app's `PgClient.config`, so the two pools reach the
 * same database with the same credentials and the caller configures nothing.
 *
 * Mapping: `url` -> `connectionString`; `host`/`port`/`path`/`database`/
 * `username`/`ssl`/`applicationName` field by field; a `password` Effect
 * becomes node-postgres' `password: () => Promise<string>`, run per
 * connection like the native client does. Fails with `PgBossError` when the
 * config has a custom `stream` (not portable to node-postgres).
 *
 * Results: node-postgres returns an array of results for a multi-statement
 * script; rows are flattened so a `RETURNING` before `COMMIT` is not lost
 * (pg-boss's own `unwrapSQLResult` does the same).
 *
 * The pool is ended when the scope closes, after `boss.stop()` (finalizers
 * run in reverse, and the layer acquires this first).
 */
export const make = (
    _config: PgClientConfig,
    _options: HousekeepingOptions,
): Effect.Effect<Housekeeping, PgBossError, Scope.Scope> =>
    // TODO
    // const pool = new pg.Pool(toPgPoolConfig(config, options))
    // pool.on("error", log)           // idle-client errors must not crash the process
    // yield* Effect.addFinalizer(() => Effect.promise(() => pool.end()))
    // return { executeSql: async (text, values) => flatten(await pool.query(text, values)) }
    Effect.die(new Error("not implemented"));
