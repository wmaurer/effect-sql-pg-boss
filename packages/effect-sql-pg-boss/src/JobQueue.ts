import * as Schema from "effect/Schema";

import type { PgBoss, PgBossError } from "./PgBoss.ts";
/**
 * Typed pg-boss queues.
 *
 * A queue is a class: its name, its payload schema and its pg-boss queue
 * options in one declaration. The class is also a `Context` service whose
 * layer creates the queue, so `send`, `step` and `worker` require the queue
 * in their context. Using a queue the app never registered is a type error.
 *
 * Every operation joins the `effect/sql` transaction the calling fiber is in,
 * whether the app opened it or pg-boss opened it for a transactional worker.
 * There is no `db` / `tx` parameter to forget.
 *
 * @since 0.1.0
 */
import type * as Arr from "effect/Array";
import type * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Layer from "effect/Layer";
import type * as Option from "effect/Option";
import type { SqlError } from "effect/sql/SqlError";
import type * as Boss from "pg-boss";

/**
 * A pg-boss job id (a UUID string).
 *
 * @since 0.1.0
 * @category models
 */
export const JobId = Schema.String.pipe(Schema.brand("@wmaurer/effect-sql-pg-boss/JobId"));

/**
 * @since 0.1.0
 * @category models
 */
export type JobId = typeof JobId.Type;

/**
 * What a queue's payload schema must be: one that encodes to an object,
 * because pg-boss stores `data` as `jsonb` and requires an object. It travels
 * through `Schema.toCodecJson`, so a `Schema.BigInt` field is stored as a
 * string and comes back as a `bigint`.
 *
 * @since 0.1.0
 * @category models
 */
export interface PayloadSchema extends Schema.Top {
    readonly Encoded: object;
}

/**
 * A job as a handler sees it: the payload decoded with the queue's schema.
 * pg-boss's raw `Job` stays internal; its abort signal interrupts the handler.
 *
 * @since 0.1.0
 * @category models
 */
export interface Job<A> {
    readonly id: JobId;
    readonly queue: string;
    readonly data: A;
    /** 0 on the first attempt. */
    readonly retryCount: number;
    readonly expireInSeconds: number;
}

/**
 * pg-boss queue options (retry, expiry, retention, policy, dead letter, ...).
 *
 * @since 0.1.0
 * @category models
 */
export type QueueOptions = Omit<Boss.Queue, "name">;

/**
 * Per-job options for `send`. `db` is absent: the transaction comes from the
 * fiber.
 *
 * @since 0.1.0
 * @category models
 */
export type SendOptions = Omit<Boss.SendOptions, "db">;

/**
 * Per-job options for one step of a flow.
 *
 * @since 0.1.0
 * @category models
 */
export type JobInsertOptions = Omit<Boss.JobInsert, "data">;

/**
 * Worker options.
 *
 * `transactional` is required, because the right mode depends on the handler.
 * `true`: the handler and the batch's completion commit in one transaction on
 * one PgClient connection, held for the handler's whole run. Fits handlers
 * that only write the database. `false`: the handler's writes commit on their
 * own and pg-boss completes the jobs afterwards on its own connections. Fits
 * handlers that call slow external services. pg-boss itself defaults to
 * `false`.
 *
 * Transactional workers are not supported on a `PgClient.makeClient` client:
 * a batch would pin its only connection for the handler's run and stall every
 * other query of the app.
 *
 * `transactionTimeoutSeconds` exists only on the transactional branch.
 * `perJobResults` and `includeMetadata` are not offered: the first cannot
 * combine with `transactional`, the second would change `Job`'s shape.
 *
 * @since 0.1.0
 * @category models
 */
export type WorkerOptions = Pick<
    Boss.WorkOptions,
    | "batchSize"
    | "pollingIntervalSeconds"
    | "localConcurrency"
    | "localGroupConcurrency"
    | "groupConcurrency"
    | "priority"
    | "orderByCreatedOn"
    | "heartbeatRefreshSeconds"
> &
    (
        | { readonly transactional: true; readonly transactionTimeoutSeconds?: number | undefined }
        | { readonly transactional: false; readonly transactionTimeoutSeconds?: never }
    );

/**
 * The service a queue class provides: proof the queue exists, plus the
 * pg-boss it was created on.
 *
 * @since 0.1.0
 * @category models
 */
export interface Registered<Name extends string> {
    readonly queueName: Name;
    readonly boss: PgBoss;
}

const StepTypeId: unique symbol = Symbol.for("@wmaurer/effect-sql-pg-boss/JobQueue/Step");

/**
 * One node of a flow, made by `Queue.step(payload, options)`.
 *
 * `Self` is the queue (so the flow requires it), `Refs` the refs it depends
 * on, kept as a literal tuple so `flow` can check them against the other keys
 * of the record, and `R` what its payload needs to encode.
 *
 * @since 0.1.0
 * @category models
 */
export interface Step<out Self, out Refs extends ReadonlyArray<string>, out R = never> {
    readonly [StepTypeId]: (_: never) => Self;
    readonly queueName: string;
    readonly dependsOn: Refs;
    readonly job: JobInsertOptions | undefined;
    /** The payload, encoded when the flow runs. */
    readonly encoded: Effect.Effect<object, Schema.SchemaError, R>;
}

/**
 * @since 0.1.0
 * @category models
 */
export declare namespace Step {
    /** The queues a record of steps uses. */
    export type Queues<Steps> = {
        [K in keyof Steps]: Steps[K] extends Step<infer Self, any, any> ? Self : never;
    }[keyof Steps];

    /** What the steps' payloads need to encode. */
    export type Services<Steps> = {
        [K in keyof Steps]: Steps[K] extends Step<any, any, infer R> ? R : never;
    }[keyof Steps];

    /** Each step may depend only on the other keys of the record. */
    export type ValidRefs<Steps> = {
        readonly [K in keyof Steps]: Step<unknown, ReadonlyArray<Exclude<keyof Steps & string, K>>, unknown>;
    };
}

/**
 * Inserts a graph of jobs atomically. The record's keys are the refs, and the
 * result maps each key to its job's id.
 *
 * Inside `sql.withTransaction` (or a transactional worker) the jobs commit or
 * roll back with the transaction. Outside one, the library opens a
 * transaction for the flow. A cycle fails with `PgBossError` (pg-boss checks
 * it).
 *
 * @since 0.1.0
 * @category flows
 */
export const flow = <const Steps extends Record<string, Step<any, ReadonlyArray<string>, any>>>(
    _steps: Steps & Step.ValidRefs<Steps>,
): Effect.Effect<
    { readonly [K in keyof Steps]: JobId },
    SqlError | PgBossError | Schema.SchemaError,
    Step.Queues<Steps> | Step.Services<Steps>
> => {
    // TODO: implement as below. pg-boss returns exactly one id per ref or rejects.
    // ```ts
    // const runtime = (yield* first step's queue service).boss[RuntimeId]
    // const jobs = yield* Effect.forEach(Object.entries(steps), ([ref, step]) =>
    //   Effect.map(step.encoded, (data) => ({ ref, name: step.queueName, data, options: step.job, dependsOn: [...step.dependsOn] })))
    // return yield* Connection.withDb(runtime.sql, (db) => Connection.call("flow", () => runtime.boss.flow(jobs, { db })))
    // ```
    throw new Error("not implemented");
};

/**
 * The class `JobQueue.Service` returns.
 *
 * @since 0.1.0
 * @category models
 */
export interface Class<Self, Name extends string, S extends PayloadSchema> extends Context.ServiceClass<
    Self,
    `@wmaurer/effect-sql-pg-boss/JobQueue/${Name}`,
    Registered<Name>
> {
    readonly queueName: Name;
    readonly payload: S;
    readonly options: QueueOptions;

    /**
     * Creates the queue if it is missing, when the layer builds. Never in a
     * request path: DDL on another connection could wait on the caller's own
     * locks. Does not change the options of an existing queue.
     */
    readonly layer: Layer.Layer<Self, PgBossError | SqlError, PgBoss>;

    /**
     * Encodes `payload` and sends one job, joining the fiber's transaction.
     *
     * `None` when pg-boss suppressed the job: a singleton or throttle option on
     * the send, or a `singleton` / `stately` queue policy, found an existing job.
     */
    readonly send: (
        payload: S["Type"],
        options?: SendOptions,
    ) => Effect.Effect<Option.Option<JobId>, SqlError | PgBossError | Schema.SchemaError, Self | S["EncodingServices"]>;

    /** One node of a `JobQueue.flow`. Encodes nothing until the flow runs. */
    readonly step: <const Refs extends ReadonlyArray<string> = readonly []>(
        payload: S["Type"],
        options?: {
            readonly dependsOn?: Refs | undefined;
            readonly job?: JobInsertOptions | undefined;
        },
    ) => Step<Self, NoInfer<Refs>, S["EncodingServices"]>;

    /**
     * Works the queue while the layer is alive. Closing the layer calls
     * `offWork` and waits for batches in flight.
     *
     * The handler gets a non-empty batch with decoded payloads, and fails or
     * succeeds as a whole. A payload that does not decode fails the batch.
     *
     * With `transactional: true`, the handler runs inside the transaction
     * pg-boss opened for the batch. `SqlClient` statements, nested
     * `withTransaction` (a savepoint), `afterCommit`, Reactivity, `send`, `flow`
     * and `PgBoss.use` all join it, and pg-boss commits it together with the
     * batch's completion. `afterCommit` callbacks run after that real commit.
     * A failure, defect or interruption rolls back and fails the batch, with
     * `Cause.pretty` as the stored error. Expiry or `boss.stop()` interrupts the
     * handler before the rollback runs.
     *
     * Fails to build when `localConcurrency` would leave the PgClient pool with
     * fewer free connections than `PgBoss.layer`'s `connectionReserve`.
     */
    readonly worker: <E, R>(
        handler: (jobs: Arr.NonEmptyReadonlyArray<Job<S["Type"]>>) => Effect.Effect<void, E, R>,
        options: WorkerOptions,
    ) => Layer.Layer<never, PgBossError, Self | R | S["DecodingServices"]>;
}

/**
 * Declares a queue.
 *
 * ```ts
 * class ChargeCard extends JobQueue.Service<ChargeCard>()("charge-card", {
 *   payload: Schema.Struct({ orderId: Schema.BigInt, amountCents: Schema.Int }),
 *   options: { retryLimit: 5, retryBackoff: true }
 * }) {}
 * ```
 *
 * The queue name is the pg-boss queue name and part of the service key, so
 * two classes for one queue name collide in the context, as two queues of
 * one name collide in pg-boss.
 *
 * @since 0.1.0
 * @category constructors
 */
export const Service =
    <Self>() =>
    <const Name extends string, S extends PayloadSchema>(
        _name: Name,
        _definition: {
            readonly payload: S;
            readonly options?: QueueOptions | undefined;
        },
    ): Class<Self, Name, S> => {
        // TODO: implement as below.
        // ```ts
        // const Key = Context.Service<Self, Registered<Name>>()(`@wmaurer/effect-sql-pg-boss/JobQueue/${name}`)
        // const codec = Schema.toCodecJson(definition.payload)
        // return class extends Key {
        //   static queueName = name; static payload = definition.payload; static options = definition.options ?? {}
        //   static layer = Layer.effect(Key, gen:
        //     const boss = yield* PgBoss; const { boss: js } = boss[RuntimeId]
        //     yield* Connection.call("createQueue", async () => { if (!(await js.getQueue(name))) await js.createQueue(name, options) })
        //     return { queueName: name, boss })
        //   static send = (payload, options) => gen:
        //     const runtime = (yield* Key).boss[RuntimeId]
        //     const data = yield* Schema.encodeEffect(codec)(payload)
        //     const id = yield* Connection.withDb(runtime.sql, (db) => Connection.call("send", () => runtime.boss.send(name, data, { ...options, db })))
        //     return Option.fromNullishOr(id)
        //   static step = (payload, options) => ({ [StepTypeId]: identity, queueName: name, dependsOn: options?.dependsOn ?? [], job: options?.job, encoded: Schema.encodeEffect(codec)(payload) })
        //   static worker = (handler, options) => Layer.effectDiscard(gen:
        //     const runtime = (yield* Key).boss[RuntimeId]
        //     const { transactional, ...work } = options
        //     if (transactional) yield* runtime.reserveConnections(name, work.localConcurrency ?? 1)
        //     const context = yield* Effect.context<R | DecodingServices>()
        //     const decodeBatch = (jobs) => Effect.forEach(jobs, (job) => Effect.map(Schema.decodeUnknownEffect(codec)(job.data), (data) => toJob(job, data)))
        //     const id = yield* Connection.call("work", () => runtime.boss.work(name, { ...work, transactional },
        //       WorkerTransaction.workCallback({ transactional, context, handler: (jobs) => Effect.flatMap(decodeBatch(jobs), handler) })))
        //     yield* Effect.addFinalizer(() => Effect.promise(() => runtime.boss.offWork(name, { id, wait: true })))
        //   )
        // }
        // ```
        throw new Error("not implemented");
    };
