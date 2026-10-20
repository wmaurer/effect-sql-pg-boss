import * as Context from "effect/Context";
/**
 * An `effect/sql` client over node-postgres (`pg.Pool`).
 *
 * This is the app's main `SqlClient`, not a side client for pg-boss. It exists
 * because the native `@effect/sql-pg` client cannot be the one:
 *
 * - It sends every statement through `Parse`, so it cannot run the
 *   multi-statement text `boss.flow` sends (`PgProtocol.ts:340-351`).
 * - It opens its own sockets and has no `fromPool`, so pg-boss cannot share
 *   its pool.
 * - It decodes `int8`, timestamps and `bytea` differently from node-postgres.
 *   A handler's rows would decode one way inside a transactional worker
 *   (pg-boss's connection) and another way in a request handler.
 *
 * With one node-postgres pool for both, there is one driver, one decoder, and
 * one place a connection comes from.
 *
 * @since 0.1.0
 */
import * as Pg from "pg";

import type * as Config from "effect/Config";
import type * as Duration from "effect/Duration";
import type * as Effect from "effect/Effect";
import type * as Layer from "effect/Layer";
import type * as Reactivity from "effect/reactivity/Reactivity";
import type * as Redacted from "effect/Redacted";
import type * as Scope from "effect/Scope";
import type * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";
import type * as Statement from "effect/sql/Statement";

/**
 * @since 0.1.0
 * @category type IDs
 */
export const TypeId: TypeId = "~@wmaurer/effect-sql-pg-boss/PgNodeClient";

/**
 * @since 0.1.0
 * @category type IDs
 */
export type TypeId = "~@wmaurer/effect-sql-pg-boss/PgNodeClient";

/**
 * A `SqlClient` whose connections are node-postgres pool clients.
 *
 * `driver` is `"pg-node"`. The pool itself is not on the interface: only
 * `PgBoss.layer` needs it, and it reaches it through an internal accessor.
 *
 * @since 0.1.0
 * @category models
 */
export interface PgNodeClient extends SqlClient.SqlClient {
    readonly [TypeId]: TypeId;
    readonly config: PgNodeClientConfig;
    /** A `jsonb` parameter. node-postgres sends it as text; the server casts it. */
    readonly json: (value: unknown) => Statement.Fragment;
}

/**
 * Provided by every `PgNodeClient` layer, next to `SqlClient`.
 *
 * `PgBoss.layer` requires this tag rather than `SqlClient`, so wiring pg-boss
 * to a client that cannot share its pool, or cannot run `flow`, does not
 * compile.
 *
 * @since 0.1.0
 * @category services
 */
export const PgNodeClient = Context.Service<PgNodeClient>("@wmaurer/effect-sql-pg-boss/PgNodeClient");

/**
 * @since 0.1.0
 * @category models
 */
export interface PgNodeClientConfig {
    readonly url?: Redacted.Redacted | undefined;
    readonly host?: string | undefined;
    readonly port?: number | undefined;
    readonly database?: string | undefined;
    readonly username?: string | undefined;
    readonly password?: Redacted.Redacted | undefined;
    readonly ssl?: boolean | Record<string, unknown> | undefined;
    readonly applicationName?: string | undefined;
    /**
     * Pool size, shared by the app and pg-boss. A transactional worker holds one
     * connection per batch in flight, and pg-boss's own polling and maintenance
     * need a few more, so size it for `sum(localConcurrency)` plus your request
     * load plus about three.
     */
    readonly maxConnections?: number | undefined;
    readonly idleTimeout?: Duration.Input | undefined;
    readonly connectTimeout?: Duration.Input | undefined;
    readonly spanAttributes?: Record<string, unknown> | undefined;
    readonly transformResultNames?: ((str: string) => string) | undefined;
    readonly transformQueryNames?: ((str: string) => string) | undefined;
    readonly transformJson?: boolean | undefined;
}

/**
 * Opens the pool, checks one connection, and closes the pool with the scope.
 *
 * @since 0.1.0
 * @category constructors
 */
export const make = (
    config: PgNodeClientConfig,
): Effect.Effect<PgNodeClient, SqlError, Scope.Scope | Reactivity.Reactivity> => {
    // TODO:
    // const pool = yield* Effect.acquireRelease(sync(() => new Pg.Pool(toPoolConfig(config))), (p) => promise(() => p.end()))
    // yield* ping (SELECT 1) mapped to ConnectionError
    // const acquirer = acquireRelease(tryPromise(() => pool.connect()), (c) => sync(() => c.release()))
    //                   |> map((c) => new NodePgConnection(c))
    // const client = yield* SqlClient.make({
    //   driver: "pg-node",
    //   acquirer,
    //   compiler: makeCompiler(config.transformQueryNames, config.transformJson),
    //   spanAttributes: [...config.spanAttributes, ["db.system.name", "postgresql"]],
    //   transformRows: config.transformResultNames && Statement.defaultTransforms(...).array,
    //   releaseSavepoint: (name) => `RELEASE SAVEPOINT ${name}`
    // })
    // internal.registerPool(client, pool)   // read only by PgBoss.layer
    // return Object.assign(client, { [TypeId]: TypeId, config, json })
    void config;
    void Pg.Pool;
    throw new Error("not implemented");
};

/**
 * Provides `PgNodeClient` and `SqlClient` (the same value).
 *
 * @since 0.1.0
 * @category layers
 */
export const layer = (config: PgNodeClientConfig): Layer.Layer<PgNodeClient | SqlClient.SqlClient, SqlError> => {
    void config;
    throw new Error("not implemented");
};

/**
 * @since 0.1.0
 * @category layers
 */
export const layerConfig = (
    config: Config.Wrap<PgNodeClientConfig>,
): Layer.Layer<PgNodeClient | SqlClient.SqlClient, Config.ConfigError | SqlError> => {
    void config;
    throw new Error("not implemented");
};

/**
 * The PostgreSQL compiler for node-postgres: `$n` placeholders, double-quoted
 * identifiers, and `json` fragments passed as plain values. The native
 * client's compiler cannot be reused because it wraps `json` in a `PgTypes`
 * parameter object that node-postgres cannot send.
 *
 * @since 0.1.0
 * @category constructors
 */
export const makeCompiler = (transform?: (_: string) => string, transformJson?: boolean): Statement.Compiler => {
    (void transform, transformJson);
    throw new Error("not implemented");
};
