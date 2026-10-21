import * as Arr from "effect/Array";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Record from "effect/Record";
import * as Schema from "effect/Schema";

import * as Connection from "./internal/connection.ts";
import { RuntimeId } from "./internal/runtime.ts";
import * as WorkerTransaction from "./internal/workerTransaction.ts";
import { PgBoss, PgBossError } from "./PgBoss.ts";

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
 * `onDecodeError` decides what a payload that does not decode does to its
 * batch. `"fail"` (the default): the batch fails before the handler runs,
 * and pg-boss retries every job in it. `"drop"`: each such job is logged at
 * warning level with its `SchemaError`, the handler gets the jobs that did
 * decode, and the dropped jobs complete with the batch. When none decode,
 * the handler does not run and the batch completes.
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
        | { readonly transactional: true; readonly transactionTimeoutSeconds?: number }
        | { readonly transactional: false; readonly transactionTimeoutSeconds?: never }
    ) & { readonly onDecodeError?: DecodeErrorPolicy };

/**
 * What a worker does with a job whose payload does not decode. See
 * {@link WorkerOptions}.
 *
 * @since 0.1.0
 * @category models
 */
export type DecodeErrorPolicy = "fail" | "drop";

/**
 * The service a queue class provides: proof the queue exists, plus the
 * pg-boss it was created on.
 *
 * @since 0.1.0
 * @category models
 */
export interface Registered<Name extends string> {
    readonly queueName: Name;
    readonly boss: PgBoss["Service"];
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
    /** The queue's service, which holds the pg-boss the flow runs on. */
    readonly queue: Effect.Effect<Registered<string>, never, Self>;
    readonly queueName: string;
    readonly dependsOn: ReadonlyArray<Refs[number]>;
    readonly job: JobInsertOptions;
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
    steps: Steps & Step.ValidRefs<Steps>,
): Effect.Effect<
    { readonly [K in keyof Steps]: JobId },
    SqlError | PgBossError | Schema.SchemaError,
    Step.Queues<Steps> | Step.Services<Steps>
> =>
    // SAFETY: pg-boss returns exactly one id for each ref, and the refs are the keys of `steps`.
    insertFlow<Step.Queues<Steps>, Step.Services<Steps>>(steps) as Effect.Effect<
        { readonly [K in keyof Steps]: JobId },
        SqlError | PgBossError | Schema.SchemaError,
        Step.Queues<Steps> | Step.Services<Steps>
    >;

const insertFlow = <Q, R>(
    steps: Record<string, Step<Q, ReadonlyArray<string>, R>>,
): Effect.Effect<Record<string, JobId>, SqlError | PgBossError | Schema.SchemaError, Q | R> =>
    Effect.gen(function* () {
        const prepared = yield* Effect.forEach(Record.toEntries(steps), ([ref, step]) =>
            Effect.zipWith(step.queue, step.encoded, (queue, data) => ({
                runtime: queue.boss[RuntimeId],
                job: { ref, name: step.queueName, data, options: step.job, dependsOn: [...step.dependsOn] },
            })),
        );
        if (!Arr.isReadonlyArrayNonEmpty(prepared)) return {};
        const { runtime } = Arr.headNonEmpty(prepared);
        const jobs = Arr.map(prepared, ({ job }) => job);
        const ids = yield* Connection.withDb(runtime.sql, (db) =>
            Connection.call("flow", () => runtime.boss.flow(jobs, { db })),
        );
        return Record.map(ids, (id) => JobId.make(id));
    });

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
     * Creates the queue if it is missing and converges an existing one to the
     * declared options, when the layer builds. Never in a request path: DDL on
     * another connection could wait on the caller's own locks.
     *
     * Each declared option is written with pg-boss's `updateQueue` on every
     * build. An option removed from the declaration keeps its stored value,
     * because `updateQueue` leaves the fields it is not given alone. `policy`
     * and `partition` cannot change after creation: the build fails with a
     * `PgBossError` when the stored value differs from the declared one
     * (`"standard"` and `false` when not declared).
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
     * `offWork`: the worker stops fetching and does not wait for the batch in
     * flight. When the app closes, `PgBoss.layer`'s stop waits for that batch
     * up to its `stop.timeout`, then interrupts the handler and fails the
     * batch. Waiting here would hold the app's shutdown for as long as a
     * handler runs.
     *
     * The handler gets a non-empty batch with decoded payloads, and fails or
     * succeeds as a whole. A payload that does not decode fails the batch,
     * or is dropped with `onDecodeError: "drop"`.
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
        name: Name,
        definition: {
            readonly payload: S;
            readonly options?: QueueOptions | undefined;
        },
    ): Class<Self, Name, S> => {
        const Key = Context.Service<Self, Registered<Name>>()(`@wmaurer/effect-sql-pg-boss/JobQueue/${name}`);
        const codec = Schema.toCodecJson(definition.payload);
        const encode = (payload: S["Type"]) => encodeObject(codec, payload);
        const decode = Schema.decodeUnknownEffect(codec);
        const options = definition.options ?? {};
        const runtimeOf = Effect.map(Key, (queue) => queue.boss[RuntimeId]);

        const decodeJob = (job: Boss.Job<unknown>) =>
            Effect.map(decode(job.data), (data): Job<S["Type"]> => ({
                id: JobId.make(job.id),
                queue: job.name,
                data,
                retryCount: job.retryCount,
                expireInSeconds: job.expireInSeconds,
            }));
        const decodeBatchWith: Record<
            DecodeErrorPolicy,
            (
                jobs: ReadonlyArray<Boss.Job<unknown>>,
            ) => Effect.Effect<ReadonlyArray<Job<S["Type"]>>, Schema.SchemaError, S["DecodingServices"]>
        > = {
            fail: (jobs) => Effect.forEach(jobs, decodeJob),
            drop: (jobs) =>
                Effect.map(
                    Effect.forEach(jobs, (job) =>
                        decodeJob(job).pipe(
                            Effect.asSome,
                            Effect.catchTag("SchemaError", (error) =>
                                Effect.as(
                                    Effect.logWarning(
                                        "dropped a job whose payload does not decode",
                                        { queue: name, id: job.id },
                                        error,
                                    ),
                                    Option.none(),
                                ),
                            ),
                        ),
                    ),
                    Arr.getSomes,
                ),
        };

        return class extends Key {
            static readonly queueName = name;
            static readonly payload = definition.payload;
            static readonly options = options;

            static readonly layer = Layer.effect(
                Key,
                Effect.gen(function* () {
                    const boss = yield* PgBoss;
                    yield* convergeQueue(boss[RuntimeId].boss, name, options);
                    return { queueName: name, boss };
                }),
            );

            static readonly send = (payload: S["Type"], sendOptions?: SendOptions) =>
                Effect.gen(function* () {
                    const runtime = yield* runtimeOf;
                    const data = yield* encode(payload);
                    const id = yield* Connection.withDb(runtime.sql, (db) =>
                        Connection.call("send", () => runtime.boss.send(name, data, { ...sendOptions, db })),
                    );
                    return Option.map(Option.fromNullishOr(id), (sent) => JobId.make(sent));
                });

            static readonly step = <const Refs extends ReadonlyArray<string> = readonly []>(
                payload: S["Type"],
                stepOptions?: {
                    readonly dependsOn?: Refs | undefined;
                    readonly job?: JobInsertOptions | undefined;
                },
            ): Step<Self, NoInfer<Refs>, S["EncodingServices"]> => ({
                [StepTypeId]: (_: never) => _,
                queue: Key,
                queueName: name,
                dependsOn: stepOptions?.dependsOn ?? [],
                job: stepOptions?.job ?? {},
                encoded: encode(payload),
            });

            static readonly worker = <E, R>(
                handler: (jobs: Arr.NonEmptyReadonlyArray<Job<S["Type"]>>) => Effect.Effect<void, E, R>,
                workerOptions: WorkerOptions,
            ) =>
                Layer.effectDiscard(
                    Effect.gen(function* () {
                        const runtime = yield* runtimeOf;
                        const { transactional, onDecodeError = "fail", ...work } = workerOptions;
                        const decodeBatch = decodeBatchWith[onDecodeError];
                        if (transactional) {
                            yield* runtime.reserveConnections(name, work.localConcurrency ?? 1);
                        }
                        const context = yield* Effect.context<R | S["DecodingServices"]>();
                        const id = yield* Effect.tryPromise({
                            try: () =>
                                runtime.boss.work(
                                    name,
                                    { ...work, transactional },
                                    WorkerTransaction.workCallback({
                                        transactional,
                                        context,
                                        handler: (jobs) =>
                                            Effect.flatMap(decodeBatch(jobs), (decoded) =>
                                                Arr.isReadonlyArrayNonEmpty(decoded) ? handler(decoded) : Effect.void,
                                            ),
                                    }),
                                ),
                            catch: (cause) => new PgBossError({ operation: "work", cause }),
                        });
                        yield* Effect.addFinalizer(() =>
                            Effect.promise(() => runtime.boss.offWork(name, { id, wait: false })),
                        );
                    }),
                );
        };
    };

/**
 * Brings the stored queue to `options`. `createQueue` leaves an existing
 * queue alone, so creating first and updating after holds when another
 * instance creates the same queue at the same moment.
 */
const convergeQueue = (boss: Boss.PgBoss, name: string, options: QueueOptions) =>
    Effect.gen(function* () {
        yield* Connection.call("createQueue", () => boss.createQueue(name, options));
        const stored = yield* Connection.call("getQueue", () => boss.getQueue(name));
        const { policy = "standard", partition = false, ...updatable } = options;
        for (const [field, declared, actual] of [
            ["policy", policy, stored?.policy],
            ["partition", partition, stored?.partition],
        ] as const) {
            if (declared !== actual) {
                return yield* new PgBossError({
                    operation: "updateQueue",
                    cause: new Error(
                        `queue "${name}" has ${field} ${String(actual)}, but its declaration says ${String(declared)}; pg-boss cannot change ${field} after creation`,
                    ),
                });
            }
        }
        if (Arr.isReadonlyArrayNonEmpty(Object.keys(updatable))) {
            yield* Connection.call("updateQueue", () => boss.updateQueue(name, updatable));
        }
    });

/**
 * Encodes a payload for pg-boss's `data` column.
 *
 * `PayloadSchema` requires an `Encoded` that is an object, and `toCodecJson`
 * keeps that shape. Its type widens the result to any `Json`.
 */
const encodeObject = <S extends PayloadSchema>(
    codec: Schema.toCodecJson<S>,
    payload: S["Type"],
): Effect.Effect<object, Schema.SchemaError, S["EncodingServices"]> =>
    // SAFETY: the schema's `Encoded` is an object (`PayloadSchema`), and its JSON codec keeps the shape.
    Effect.map(Schema.encodeEffect(codec)(payload), (json) => json as object);
