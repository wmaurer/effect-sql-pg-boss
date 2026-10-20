import * as Context from "effect/Context";

/**
 * An `@effect/sql` client over node-postgres (`pg`), built so that other
 * libraries can share its pool and its connections.
 *
 * Why this exists: the native `@effect/sql-pg` client sends every statement
 * through the extended protocol and cannot be handed a `pg.Pool`. pg-boss needs
 * both (multi-statement text without parameters, and one pool for the app and
 * the queue). This client has the same `SqlClient` surface, so app code written
 * against `SqlClient` does not change.
 *
 * Invariants:
 * - A call with no parameters is sent as one simple-protocol text, so
 *   multi-statement text works. A call with parameters uses the extended
 *   protocol.
 * - The pool is created with node-postgres' default type parsers and is not
 *   configurable here. pg-boss shares the pool and its internal SQL depends on
 *   them (int8 as string, timestamptz as Date). Row transforms for the app go in
 *   `transformResultNames` or in `Schema`, never in pool-level parsers.
 * - `SqlClient` is provided together with `NodePgClient`: both tags resolve to
 *   the same value. `NodePgClient` is a subtype of `SqlClient`, so a program
 *   that also requires plain `SqlClient` does not show `NodePgClient` as a
 *   separate requirement; `Boss` therefore also checks `pool` at runtime.
 *
 * @since 0.1.0
 */
import type * as Config from "effect/Config";
import type * as Effect from "effect/Effect";
import type * as Layer from "effect/Layer";
import type * as Reactivity from "effect/reactivity/Reactivity";
import type * as Redacted from "effect/Redacted";
import type * as Scope from "effect/Scope";
import type * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";
import type * as Statement from "effect/sql/Statement";
import type * as Pg from "pg";

/** @since 0.1.0 */
export const TypeId: TypeId = "~@wmaurer/effect-sql-pg-boss/NodePgClient";

/** @since 0.1.0 */
export type TypeId = "~@wmaurer/effect-sql-pg-boss/NodePgClient";

/**
 * The smallest thing a connection owned by somebody else must offer. It is
 * structurally the same as pg-boss' `IDatabase.executeSql`, but this module
 * does not import pg-boss: the sketch keeps the client usable without it.
 *
 * Contract: `values` is `undefined` for a parameterless call, and the executor
 * must then use the simple protocol. It must reject with the driver's own error
 * object so that `code`, `constraint` and `detail` survive.
 *
 * @since 0.1.0
 */
export interface Executor {
    readonly executeSql: (text: string, values?: Array<unknown>) => Promise<{ readonly rows: Array<any> }>;
}

/**
 * A node-postgres backed `SqlClient`.
 *
 * Two kinds of values implement it. The root client owns a pool and acquires
 * a connection per statement or transaction. A borrowed client (from
 * {@link NodePgClient.borrow}) runs on one connection that someone else
 * opened and will commit, which is how a pg-boss transactional worker hands
 * its transaction to the handler.
 *
 * @since 0.1.0
 */
export interface NodePgClient extends SqlClient.SqlClient {
    readonly [TypeId]: TypeId;
    /**
     * The pool every connection of this client (and of clients borrowed from it)
     * belongs to. `Boss` compares it with its own pool to refuse a client that
     * points at another database.
     */
    readonly pool: Pg.Pool;
    /** JSON parameter fragment, same as `PgClient.json`. */
    readonly json: (value: unknown) => Statement.Fragment;
    /**
     * Builds a client that runs every statement on `executor`'s connection.
     *
     * - Statements are serialised by a one-permit semaphore (as
     *   `PgliteClient.fromClient`), so concurrent fibers cannot interleave
     *   savepoints.
     * - The executor's connection is already inside a transaction. The first
     *   `withTransaction` becomes `SAVEPOINT`/`RELEASE`/`ROLLBACK TO`, deeper ones
     *   nest as usual. Nothing here issues `BEGIN` or `COMMIT`.
     * - `afterCommit` hooks fire at `RELEASE SAVEPOINT`, which is before the real
     *   commit. Callers who need "after commit" must not rely on them here.
     * - The result shares compiler, row transforms and Reactivity with `this`.
     */
    readonly borrow: (executor: Executor) => Effect.Effect<NodePgClient>;
}

/** @since 0.1.0 */
export const NodePgClient = Context.Service<NodePgClient>("@wmaurer/effect-sql-pg-boss/NodePgClient");

/** @since 0.1.0 */
export interface NodePgClientConfig {
    /** Connection string. Redacted so it never reaches a log. */
    readonly url?: Redacted.Redacted | undefined;
    /**
     * Pool sizing and timeouts. `types` is omitted on purpose, see the module
     * invariants. Size `max` for app transactions plus the transactional workers
     * (each holds one connection for the length of its handler) plus pg-boss'
     * own maintenance statements.
     */
    readonly pool?: Omit<Pg.PoolConfig, "types" | "connectionString"> | undefined;
    readonly transformQueryNames?: ((str: string) => string) | undefined;
    readonly transformResultNames?: ((str: string) => string) | undefined;
    readonly spanAttributes?: Record<string, unknown> | undefined;
}

/**
 * Builds the root client over a pool that the caller owns. The pool is not
 * closed when the scope ends.
 *
 * @since 0.1.0
 */
export const fromPool = (
    _pool: Pg.Pool,
    _options?: Omit<NodePgClientConfig, "url" | "pool">,
): Effect.Effect<NodePgClient, SqlError, Reactivity.Reactivity> => {
    // TODO
    // 1. compiler = makeCompiler(transformQueryNames)  (PgJson custom, `$n` placeholders)
    // 2. acquirer = acquireRelease(pool.connect(), c => c.release()) mapped to a
    //    NodePgConnection over `c.query(text, values?)`; rejections go through
    //    classifyError, keeping the original error as `cause`
    // 3. client = SqlClient.make({ driver: "pg-node", acquirer, compiler,
    //    releaseSavepoint, spanAttributes, transformRows })
    // 4. borrow(executor) = SqlClient.make with acquirer = semaphore over a
    //    NodePgConnection(executor), beginTransaction = "SAVEPOINT effect_borrowed",
    //    commit = "RELEASE SAVEPOINT effect_borrowed",
    //    rollback = "ROLLBACK TO SAVEPOINT effect_borrowed"
    throw new Error("not implemented");
};

/**
 * Creates the pool, attaches an `error` listener that logs through Effect (an
 * unhandled pool `error` would crash the process), builds the client and ends
 * the pool when the scope closes.
 *
 * @since 0.1.0
 */
export const make = (
    _config: NodePgClientConfig,
): Effect.Effect<NodePgClient, SqlError, Scope.Scope | Reactivity.Reactivity> => {
    throw new Error("not implemented");
};

/**
 * Provides `NodePgClient` and `SqlClient` (same value).
 *
 * @since 0.1.0
 */
export const layer = (_config: NodePgClientConfig): Layer.Layer<NodePgClient | SqlClient.SqlClient, SqlError> => {
    throw new Error("not implemented");
};

/** @since 0.1.0 */
export const layerConfig = (
    _config: Config.Config<NodePgClientConfig>,
): Layer.Layer<NodePgClient | SqlClient.SqlClient, Config.ConfigError | SqlError> => {
    throw new Error("not implemented");
};

/**
 * Same as {@link layer} for a pool the caller already created and will end.
 *
 * @since 0.1.0
 */
export const layerFromPool = (
    _pool: Pg.Pool,
    _options?: Omit<NodePgClientConfig, "url" | "pool">,
): Layer.Layer<NodePgClient | SqlClient.SqlClient, SqlError> => {
    throw new Error("not implemented");
};

/**
 * Statement compiler: `$n` placeholders, double-quoted identifiers, and
 * `sql.json` unwrapped to a JSON string. Copy of `PgliteClient.makeCompiler`.
 * It cannot reuse `PgClient.makeCompiler`, whose JSON parameter is an object
 * that node-postgres cannot send.
 *
 * @since 0.1.0
 */
export const makeCompiler = (_transform?: (_: string) => string): Statement.Compiler => {
    throw new Error("not implemented");
};
