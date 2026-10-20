/**
 * What `JobQueue` needs from a running `PgBoss` layer. Held on the service
 * under {@link RuntimeId}, a symbol no caller can name: it is not registered
 * with `Symbol.for`, and `package.json` blocks `./internal/*`.
 *
 * @internal
 */
import * as Effect from "effect/Effect";

import { PgBossError } from "../PgBoss.ts";

import type * as Scope from "effect/Scope";
import type * as SqlClient from "effect/sql/SqlClient";
import type * as Boss from "pg-boss";

export const RuntimeId: unique symbol = Symbol("@wmaurer/effect-sql-pg-boss/Runtime");

export interface Runtime {
    readonly boss: Boss.PgBoss;
    /** The app's `PgClient`, as the `SqlClient` the routing rule reads the transaction from. */
    readonly sql: SqlClient.SqlClient;
    /**
     * Holds `connections` of the PgClient pool for one transactional worker
     * until the scope closes. Each in-flight transactional batch holds one
     * connection for the handler's whole run.
     */
    readonly reserveConnections: (
        queueName: string,
        connections: number,
    ) => Effect.Effect<void, PgBossError, Scope.Scope>;
}

/**
 * The pool headroom check. `available` is the PgClient pool's
 * `maxConnections` minus the reserve kept for request traffic. A worker layer
 * whose `localConcurrency` would exceed what is left fails at build, instead
 * of starving requests at run time.
 */
export const connectionBudget = (available: number): Runtime["reserveConnections"] => {
    let reserved = 0;
    return (queueName, connections) =>
        Effect.acquireRelease(
            Effect.suspend(() => {
                if (reserved + connections > available) {
                    return Effect.fail(
                        new PgBossError({
                            operation: "work",
                            cause: new Error(
                                `transactional worker "${queueName}" needs ${connections} connection(s); ` +
                                    `${available - reserved} of the PgClient pool's ${available} left after the reserve`,
                            ),
                        }),
                    );
                }
                reserved += connections;
                return Effect.void;
            }),
            () => Effect.sync(() => (reserved -= connections)),
        );
};
