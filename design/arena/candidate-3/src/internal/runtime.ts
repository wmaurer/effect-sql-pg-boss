import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import type { Job, JobId, JobQueue } from "../JobQueue.ts";
import type { PgBossError } from "../PgBoss.ts";
import type { Db } from "./database.ts";
/**
 * What the public modules need from a running `PgBoss` layer. Held on the
 * service under a symbol callers cannot name.
 *
 * @internal
 */
import type * as Context from "effect/Context";
import type * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";
import type * as Boss from "pg-boss";

export const RuntimeId: unique symbol = Symbol.for("@wmaurer/effect-sql-pg-boss/Runtime");

export interface Runtime {
    readonly boss: Boss.PgBoss;

    /**
     * The db for the fiber's current connection. Single owner of the rule
     * "pg-boss runs where `sql` would run":
     *
     * - The fiber is inside `sql.withTransaction`: the db registered for that
     *   transaction's connection, creating it from the fiber's context on first
     *   use. Keyed by the connection object, which is fresh per transaction and
     *   shared by its savepoint levels.
     * - Otherwise: a db over the fiber's context, which runs on the pool.
     *
     * A transactional worker registers its parked transaction's db before
     * pg-boss calls the handler, so every call the handler makes through the
     * library passes pg-boss the exact object `beginTransaction()` returned.
     * pg-boss fences a handler's settles by that identity (`manager.js:671`).
     */
    readonly currentDb: Effect.Effect<Db>;

    /**
     * Creates the queue if it is missing, once per layer instance per name.
     * Runs on the housekeeping pool, never inside the caller's transaction, so
     * a rollback cannot undo queue creation that a committed sibling relied on.
     * Does not update options of an existing queue (see rationale, open
     * questions).
     */
    readonly ensureQueue: (queue: JobQueue.Any) => Effect.Effect<void, PgBossError>;

    /**
     * The context of the parked transaction behind a handler's `tx`, or
     * `undefined` if pg-boss handed us a db we did not create.
     */
    readonly transactionContext: (tx: Db) => Context.Context<SqlClient.SqlClient> | undefined;

    /**
     * The one Promise boundary. Rejections map to `SqlError` when they came
     * from our db adapter (the original error, via `originalSqlError`),
     * otherwise to `PgBossError` tagged with `operation`.
     */
    readonly call: <A>(operation: string, f: () => Promise<A>) => Effect.Effect<A, PgBossError | SqlError>;
}

/**
 * Encodes a payload to the JSON value pg-boss stores, through
 * `Schema.toCodecJson`, so `bigint`, `Date` and branded fields survive the
 * JSON column.
 */
export const encodePayload = <Q extends JobQueue.Any>(
    queue: Q,
    payload: JobQueue.Payload<Q>,
): Effect.Effect<unknown, Schema.SchemaError, Q["payload"]["EncodingServices"]> =>
    Schema.encodeUnknownEffect(Schema.toCodecJson(queue.payload))(payload);

/**
 * Parses a fetched pg-boss job into the library's `Job`. Boundary parse: the
 * handler only ever sees decoded data.
 */
export const decodeJob = <Q extends JobQueue.Any>(
    queue: Q,
    job: Boss.Job<unknown>,
): Effect.Effect<Job<JobQueue.Payload<Q>>, Schema.SchemaError, Q["payload"]["DecodingServices"]> =>
    Effect.map(
        Schema.decodeUnknownEffect(Schema.toCodecJson(queue.payload))(job.data),
        (data): Job<JobQueue.Payload<Q>> => ({
            id: job.id as JobId,
            queue: job.name,
            data,
            retryCount: job.retryCount,
            expireInSeconds: job.expireInSeconds,
        }),
    );
