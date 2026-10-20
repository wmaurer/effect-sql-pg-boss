/**
 * pg-boss for Effect, sharing the app's database connection.
 *
 * Every operation in this module runs on the connection of the fiber that
 * calls it. That is the whole mechanism for both directions of atomicity:
 *
 * - App to queue. `Boss.send` / `Boss.flow` inside `sql.withTransaction`
 *   write the job rows on the transaction's connection. They commit or roll
 *   back with the app rows. Outside a transaction they autocommit.
 * - Queue to app. A handler passed to `Boss.work` runs with `SqlClient`
 *   (and `NodePgClient`) bound to the transaction that pg-boss opened for the
 *   batch. App writes, further `Boss.send` calls, and the job's completion
 *   commit together, or the whole batch rolls back and the job is retried.
 *
 * This is why the operations require `NodePgClient` in their context: the
 * type says "this joins the ambient SQL transaction", and only a client that
 * shares the pool with pg-boss can honour it.
 *
 * @since 0.1.0
 */
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import * as AmbientDb from "./internal/AmbientDb.ts";

import type { FlowNode, QueueNamesOf, RefsOf } from "./Flow.ts";
import type { NodePgClient } from "./NodePgClient.ts";
import type { Job, JobId, Queue, SendOptions } from "./Queue.ts";
import type * as Duration from "effect/Duration";
import type * as Layer from "effect/Layer";
import type * as Scope from "effect/Scope";
import type { SqlClient } from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";
import type * as Pg from "pg";
import type { Db, PgBoss } from "pg-boss";

/** @since 0.1.0 */
export const TypeId: TypeId = "~@wmaurer/effect-sql-pg-boss/Boss";

/** @since 0.1.0 */
export type TypeId = "~@wmaurer/effect-sql-pg-boss/Boss";

/**
 * Failure of a pg-boss operation that is not a SQL failure. A failed statement
 * is reported as the `SqlError` it already is, so callers handle one error type
 * for SQL wherever it comes from.
 *
 * @since 0.1.0
 */
export class BossError extends Schema.TaggedError<BossError>("@wmaurer/effect-sql-pg-boss/BossError")("BossError", {
    kind: Schema.Literals([
        /** The payload did not encode. */
        "Encode",
        /** A stored payload did not decode. The job is failed without running the handler. */
        "Decode",
        /** Duplicate refs in a flow. Nothing was sent. */
        "InvalidFlow",
        /** pg-boss rejected the call for a reason that is not SQL. */
        "Driver",
        /** `start` or `stop` failed, or two queues in `Boss.layer({ queues })` share a name. */
        "Lifecycle",
    ]),
    message: Schema.String,
    cause: Schema.optional(Schema.Defect()),
}) {}

/**
 * The running pg-boss instance. Opaque: use the functions in this module.
 *
 * @since 0.1.0
 */
export interface BossHandle {
    readonly [TypeId]: TypeId;
}

/**
 * Type-level proof that `Boss.layer` creates the queue called `Name`. `send`,
 * `flow` and `work` require it, so a queue missing from `Boss.layer({ queues })`
 * is a compile error at the call site, not a runtime failure.
 *
 * At runtime the key holds the set of created queue names.
 *
 * @since 0.1.0
 */
export interface Registered<out Name extends string> {
    readonly _Registered: () => Name;
}

/** @since 0.1.0 */
export const Registered = Context.Service<Registered<string>>("@wmaurer/effect-sql-pg-boss/Registered");

/**
 * Service tag. Provided by {@link layer}.
 *
 * @since 0.1.0
 */
export class Boss extends Context.Service<Boss, BossHandle>()("@wmaurer/effect-sql-pg-boss/Boss") {}

/**
 * Not exported. What the functions in this module see behind {@link BossHandle}.
 * Only this file downcasts, so nothing outside can reach the raw instance
 * except through {@link use}.
 */
interface BossImpl extends BossHandle {
    readonly raw: PgBoss;
    /** The pool pg-boss runs on. Ambient clients must belong to it. */
    readonly pool: Pg.Pool;
}

/**
 * Set by `work` around the handler: the exact `IDatabase` object pg-boss gave
 * the transactional handler. `use` hands it back so that settle calls
 * (`complete`, `fail`, `cancel`, `deleteJob`) keep the identity pg-boss matches
 * on. Not exported.
 */
const CurrentTx = Context.Service<Db>("@wmaurer/effect-sql-pg-boss/CurrentTx");

const asImpl = (handle: BossHandle): BossImpl => handle as BossImpl;

/**
 * Runs one pg-boss call. A rejection becomes the `SqlError` the ambient adapter
 * recorded if there is one, otherwise a `BossError` of kind `Driver`.
 */
const attempt = <A>(ambient: AmbientDb.Ambient, f: () => Promise<A>): Effect.Effect<A, BossError | SqlError> =>
    Effect.tryPromise({
        try: f,
        catch: (cause) =>
            ambient.failure() ??
            new BossError({
                kind: "Driver",
                message: cause instanceof Error ? cause.message : String(cause),
                cause,
            }),
    });

// ---------------------------------------------------------------------------
// Enqueue
// ---------------------------------------------------------------------------

/**
 * Enqueues one job on the connection of the calling fiber.
 *
 * Succeeds with the job id, or `undefined` when pg-boss declined to create the
 * job (a singleton or throttle key already exists), same as pg-boss' `null`.
 *
 * @since 0.1.0
 */
export const send = <Name extends string, A extends object>(
    _queue: Queue<Name, A>,
    _data: A,
    _options?: SendOptions,
): Effect.Effect<JobId | undefined, BossError | SqlError, Boss | NodePgClient | Registered<Name>> => {
    // gen:
    //   impl = asImpl(yield* Boss)
    //   body = yield* Schema.encodeEffect(queue.codec)(data)           (BossError Encode)
    //   ambient = yield* AmbientDb.make(impl.pool)
    //   id = yield* attempt(() => impl.raw.send(queue.name, body, { ...toPgBossOptions(options), db: ambient.db }), ambient)
    //   return id ?? undefined
    throw new Error("not implemented");
};

/**
 * Enqueues a graph of jobs, parents before children, in one statement batch on
 * the connection of the calling fiber. Succeeds with the id of every node in the
 * graph, keyed by ref.
 *
 * Takes the leaves; everything they depend on is sent too.
 *
 * @since 0.1.0
 */
export const flow = <const Leaves extends readonly [FlowNode<string, string>, ...Array<FlowNode<string, string>>]>(
    ..._leaves: Leaves
): Effect.Effect<
    { readonly [R in RefsOf<Leaves[number]>]: JobId },
    BossError | SqlError,
    Boss | NodePgClient | Registered<QueueNamesOf<Leaves[number]>>
> => {
    // gen:
    //   nodes = Flow.collect(leaves)                                   (BossError InvalidFlow)
    //   rows  = yield* Effect.forEach(nodes, n => encode + toFlowJob(n))
    //   ids   = yield* attempt(() => impl.raw.flow(rows, { db: ambient.db }), ambient)
    //   return ids as Record<ref, JobId>
    throw new Error("not implemented");
};

// ---------------------------------------------------------------------------
// Work
// ---------------------------------------------------------------------------

/** @since 0.1.0 */
export interface WorkOptions {
    /** Jobs per handler call. Default 1. */
    readonly batchSize?: number | undefined;
    /** Parallel handler calls for this queue in this process. Default 1. */
    readonly concurrency?: number | undefined;
    readonly pollInterval?: Duration.Input | undefined;
}

/**
 * What a transactional handler may require without the caller providing it.
 * Both resolve to the batch's transaction.
 *
 * @since 0.1.0
 */
export type TransactionServices = SqlClient | NodePgClient;

/**
 * Registers a worker for the lifetime of the scope. Each batch runs in one
 * database transaction that pg-boss opens and commits.
 *
 * - The handler sees `SqlClient` and `NodePgClient` bound to that transaction,
 *   so `yield* SqlClient` gives the transactional client with no extra code.
 * - Handler success: pg-boss completes the jobs in the same transaction and
 *   commits. Failure or defect: the transaction rolls back, nothing the handler
 *   wrote persists, and the jobs are failed and retried per queue policy.
 * - `sql.withTransaction` inside the handler nests as a savepoint, so a handler
 *   can recover from a failed statement without poisoning the batch.
 * - Side effects outside the database (HTTP, email) are at-least-once.
 * - Closing the scope stops fetching and waits for running handlers.
 * - A job that cannot decode is failed without calling the handler. With
 *   `batchSize > 1` that fails the whole batch, so keep `batchSize` 1 for
 *   untrusted payloads.
 *
 * @since 0.1.0
 */
export const work: {
    <Name extends string, A extends object, E, R>(
        queue: Queue<Name, A>,
        options: WorkOptions,
        handler: (jobs: readonly [Job<A>, ...Array<Job<A>>]) => Effect.Effect<void, E, R>,
    ): Effect.Effect<
        void,
        BossError,
        Scope.Scope | Boss | NodePgClient | Registered<Name> | Exclude<R, TransactionServices>
    >;
    /**
     * Same, without a transaction. The handler runs on whatever `SqlClient` the
     * calling context holds and pg-boss completes the jobs separately. Use it for
     * long handlers that do no database work, so no connection is held while they
     * run.
     */
    <Name extends string, A extends object, E, R>(
        queue: Queue<Name, A>,
        options: WorkOptions & { readonly atomic: false },
        handler: (jobs: readonly [Job<A>, ...Array<Job<A>>]) => Effect.Effect<void, E, R>,
    ): Effect.Effect<void, BossError, Scope.Scope | Boss | Registered<Name> | R>;
} = (..._args: Array<any>): any => {
    // gen:
    //   root = yield* NodePgClient; ctx = yield* Effect.context<...>(); impl = yield* Boss
    //   run = (jobs, tx) => Effect.gen(function*() {
    //     decoded = yield* decode each job with queue.codec        (BossError Decode)
    //     txClient = yield* root.borrow({ executeSql: tx.executeSql })
    //     return yield* handler(decoded).pipe(
    //       provideService(NodePgClient, txClient), provideService(SqlClient, txClient), provideService(CurrentTx, tx))
    //   })
    //   promiseHandler = (jobs, tx) => Effect.runPromiseExit(run(jobs, tx).pipe(Effect.provideContext(ctx)), { signal })
    //                                    .then(exit => exit succeeds ? undefined : throw toError(exit.cause))
    //   id = yield* tryPromise(() => impl.raw.work(queue.name, { transactional: true, batchSize, localConcurrency, pollingIntervalSeconds }, promiseHandler))
    //   yield* Effect.addFinalizer(() => impl.raw.offWork(queue.name, { id, wait: true }))
    throw new Error("not implemented");
};

// ---------------------------------------------------------------------------
// Escape hatch
// ---------------------------------------------------------------------------

/**
 * Runs `f` with the raw pg-boss instance and the database handle to pass as
 * `{ db }`. Use it for what this module does not wrap (`schedule`, `cancel`,
 * `getJobById`, `findJobs`, ...).
 *
 * `db` is the connection of the calling fiber: the app's transaction, a worker's
 * transaction (the exact object pg-boss gave the handler, which settle calls
 * require), or a pooled connection. Calls that omit `{ db }` use pg-boss' own
 * path, which is separate from the ambient transaction.
 *
 * This is the only function here that exposes pg-boss types.
 *
 * @since 0.1.0
 */
export const use = <A>(
    f: (boss: PgBoss, db: Db) => Promise<A>,
): Effect.Effect<A, BossError | SqlError, Boss | NodePgClient> =>
    Effect.gen(function* () {
        const impl = asImpl(yield* Boss);
        const ambient = yield* AmbientDb.make(impl.pool);
        const tx = yield* Effect.serviceOption(CurrentTx);
        const db = Option.getOrElse(tx, () => ambient.db);
        return yield* attempt(ambient, () => f(impl.raw, db));
    });

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

/** @since 0.1.0 */
export interface BossConfig<Queues extends ReadonlyArray<Queue<string, any>>> {
    /**
     * Every queue the app sends to or works on. Created (idempotently, dead
     * letter queues first) when the layer starts, never from a request path: a
     * DDL statement on another connection could wait on locks held by the
     * caller's own transaction. The layer provides `Registered` for exactly these
     * names, so a queue left out of this list does not type check where it is used.
     */
    readonly queues: Queues;
    /** pg-boss schema name. Default `"pgboss"`. */
    readonly schema?: string | undefined;
    /** Install and migrate the pg-boss schema on start. Default `true`. */
    readonly migrate?: boolean | undefined;
    /** Run pg-boss maintenance (expiry, retention, monitoring). Default `true`. */
    readonly supervise?: boolean | undefined;
    /** Run the cron scheduler. Default `false`. */
    readonly schedule?: boolean | undefined;
    /** How long `stop` waits for running handlers. Default 30 seconds. */
    readonly shutdownTimeout?: Duration.Input | undefined;
}

/**
 * Starts pg-boss on the shared pool and stops it when the layer is released.
 *
 * Requires `NodePgClient` and runs on its pool, not on a second connection
 * string: the app and the queue use one pool by construction. Because the
 * layer depends on the pool layer, release order is right: workers (scoped
 * under this layer) drain, pg-boss stops with `close: false`, then the pool
 * ends.
 *
 * Startup: build `PoolDb` over the pool, `new PgBoss({ db, schema, migrate,
 * supervise, schedule, useListenNotify: false })`, route its `error` event to
 * `Effect.logError`, `await start()`, then create the queues.
 *
 * @since 0.1.0
 */
export const layer = <const Queues extends ReadonlyArray<Queue<string, any>>>(
    _config: BossConfig<Queues>,
): Layer.Layer<Boss | Registered<Queues[number]["name"]>, BossError, NodePgClient> => {
    throw new Error("not implemented");
};
