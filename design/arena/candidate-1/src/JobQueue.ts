import * as Schema from "effect/Schema";

import type { PgBoss, PgBossError } from "./PgBoss.ts";
import type * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Layer from "effect/Layer";
import type * as Option from "effect/Option";
import type * as Scope from "effect/Scope";
/**
 * Typed pg-boss queues.
 *
 * A queue is a class: its name, its payload schema, and its pg-boss queue
 * options in one declaration. The class is also a `Context` service whose
 * layer creates the queue, so `send`, `step` and `worker` require the queue
 * in their context. Using a queue the app never registered is a type error.
 *
 * Every operation here joins the `effect/sql` transaction the calling fiber
 * is in, whether the app opened it or pg-boss opened it for a transactional
 * worker. There is no `db` / `tx` parameter to forget.
 *
 * @since 0.1.0
 */
import type {
    JobInsert,
    Queue as PgBossQueue,
    SendOptions as PgBossSendOptions,
    WorkOptions as PgBossWorkOptions,
} from "pg-boss";

// -----------------------------------------------------------------------------
// Data
// -----------------------------------------------------------------------------

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
 * What a queue's payload schema must be.
 *
 * - It encodes to an object, because pg-boss stores `data` as `jsonb` and
 *   requires an object.
 * - It needs no services to encode or decode, so `send` and `worker` carry
 *   no hidden requirements.
 *
 * @since 0.1.0
 * @category models
 */
export interface PayloadSchema extends Schema.Top {
    readonly Encoded: object;
    readonly DecodingServices: never;
    readonly EncodingServices: never;
}

/**
 * A job as a handler sees it: the payload already decoded with the queue's
 * schema. pg-boss's `signal` is not here; its abort interrupts the handler.
 *
 * @since 0.1.0
 * @category models
 */
export interface Job<A> {
    readonly id: JobId;
    readonly data: A;
    /** 0 on the first attempt. With `id`, identifies the attempt for fenced settles. */
    readonly retryCount: number;
    readonly groupId: string | undefined;
}

/**
 * pg-boss queue options (retry, expiry, retention, policy, dead letter, ...).
 *
 * @since 0.1.0
 * @category models
 */
export type QueueOptions = Omit<PgBossQueue, "name">;

/**
 * Per-job options for `send`. Queue-level defaults come from the queue class.
 * `db` is absent: the transaction comes from the fiber.
 *
 * @since 0.1.0
 * @category models
 */
export type SendOptions = Omit<PgBossSendOptions, "db">;

/**
 * Worker options.
 *
 * `transactional` has no default. `true` runs the handler and the batch's
 * completion in one transaction on one pooled connection, held for the
 * handler's whole run. `false` holds nothing, and the handler's writes commit
 * on their own. The choice is a correctness one, so the call site states it.
 *
 * `transactionTimeoutSeconds` only exists on the transactional branch.
 * `perJobResults` and `includeMetadata` are not offered: the first cannot
 * combine with `transactional`, the second would change `Job`'s shape.
 *
 * @since 0.1.0
 * @category models
 */
export type WorkerOptions = Pick<
    PgBossWorkOptions,
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
        | { readonly transactional: false }
    );

/**
 * The service a queue class provides: proof the queue exists, plus the
 * pg-boss instance it was created on.
 *
 * @since 0.1.0
 * @category models
 */
export interface Registered<Name extends string> {
    readonly queueName: Name;
    readonly boss: PgBoss;
}

// -----------------------------------------------------------------------------
// Flows
// -----------------------------------------------------------------------------

const StepTypeId: unique symbol = Symbol.for("@wmaurer/effect-sql-pg-boss/JobQueue/Step") as never;

/**
 * One node of a flow, made by `Queue.step(data, options)`.
 *
 * `Self` is the queue (so the flow requires it) and `Refs` the refs it
 * depends on, kept as a literal tuple so `flow` can check them against the
 * other keys of the record.
 *
 * @since 0.1.0
 * @category models
 */
export interface Step<out Self, out Refs extends ReadonlyArray<string>> {
    readonly [StepTypeId]: (_: never) => Self;
    readonly queueName: string;
    /** The payload, encoded with the queue's schema when the flow runs. */
    readonly encoded: Effect.Effect<object, Schema.SchemaError>;
    readonly options: StepOptions<Refs>;
}

/**
 * @since 0.1.0
 * @category models
 */
export type StepOptions<Refs extends ReadonlyArray<string>> = Omit<JobInsert, "data"> & {
    readonly dependsOn?: Refs | undefined;
};

/**
 * @since 0.1.0
 * @category models
 */
export declare namespace Step {
    /** The queues a record of steps uses. */
    export type Queues<Steps> = {
        [K in keyof Steps]: Steps[K] extends Step<infer Self, any> ? Self : never;
    }[keyof Steps];

    /** Each step may depend only on the other keys of the record. */
    export type ValidRefs<Steps> = {
        readonly [K in keyof Steps]: Step<unknown, ReadonlyArray<Exclude<keyof Steps & string, K>>>;
    };
}

/**
 * Inserts a DAG of jobs in one statement batch. Record keys are the refs.
 *
 * Inside `sql.withTransaction` (or a transactional worker) the jobs commit or
 * roll back with the transaction. Outside one, pg-boss wraps the batch in its
 * own transaction. A cycle fails with `PgBossError` (pg-boss checks it).
 *
 * @since 0.1.0
 * @category flows
 */
export const flow = <const Steps extends Record<string, Step<any, ReadonlyArray<string>>>>(
    steps: Steps & Step.ValidRefs<Steps>,
): Effect.Effect<{ readonly [K in keyof Steps]: JobId }, PgBossError | Schema.SchemaError, Step.Queues<Steps>> => {
    // TODO:
    // const boss = (yield* first step's queue service).boss
    // const jobs = yield* Effect.forEach(Object.entries(steps), ([ref, step]) =>
    //   Effect.map(step.encoded, (data) => ({ ref, name: step.queueName, data, options: omit(step.options, "dependsOn"), dependsOn: step.options.dependsOn })))
    // const ids = yield* boss.use((b, connection) => b.flow(jobs, connection))
    // return ids as branded record
    void steps;
    throw new Error("not implemented");
};

// -----------------------------------------------------------------------------
// Queue classes
// -----------------------------------------------------------------------------

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
    readonly queueOptions: QueueOptions;

    /**
     * Creates the queue if it is missing and updates its options if they
     * changed, so it converges no matter what a previous deploy left behind.
     * Fails if a deployed queue's `policy` or `partition` differs, which pg-boss
     * cannot change in place.
     */
    readonly layer: Layer.Layer<Self, PgBossError, PgBoss>;

    /**
     * Encodes `data` and sends one job. Joins the fiber's transaction.
     *
     * `None` when pg-boss suppressed the job: a singleton or throttle option on
     * the send, or a `singleton` / `stately` queue policy, found an existing job.
     */
    readonly send: (
        data: S["Type"],
        options?: SendOptions | undefined,
    ) => Effect.Effect<Option.Option<JobId>, PgBossError | Schema.SchemaError, Self>;

    /** One node of a `JobQueue.flow`. Encodes nothing until the flow runs. */
    readonly step: <const Refs extends ReadonlyArray<string> = readonly []>(
        data: S["Type"],
        options?: StepOptions<Refs> | undefined,
    ) => Step<Self, NoInfer<Refs>>;

    /**
     * Works the queue while the layer is alive. Closing the layer calls
     * `offWork` and waits for batches in flight.
     *
     * The handler gets a non-empty batch with decoded payloads. It fails or
     * succeeds as a whole, because with `transactional: true` the batch is the
     * unit that commits. A payload that does not decode fails the batch with
     * `PayloadDecodeError` before the handler runs.
     *
     * With `transactional: true`, the handler runs inside the transaction
     * pg-boss opened for the batch: `SqlClient` statements, nested
     * `withTransaction`, `send`, `flow` and `PgBoss.use` all join it, and pg-boss
     * commits it together with the batch's completion. The `SqlClient` the
     * handler uses must be the `PgNodeClient` pg-boss runs on; building the
     * layer dies otherwise.
     *
     * A handler failure, defect or interruption makes pg-boss roll back and fail
     * the batch, with `Cause.pretty` as the stored error. pg-boss's abort signal
     * (expiry, shutdown) interrupts the handler.
     */
    readonly worker: <E, R>(
        handler: (jobs: readonly [Job<S["Type"]>, ...Array<Job<S["Type"]>>]) => Effect.Effect<void, E, R>,
        options: WorkerOptions,
    ) => Layer.Layer<never, PgBossError, Self | Exclude<R, Scope.Scope>>;
}

/**
 * Declares a queue.
 *
 * ```ts
 * class SendWelcomeEmail extends JobQueue.Service<SendWelcomeEmail>()("send-welcome-email", {
 *   payload: Schema.Struct({ userId: Schema.String, email: Schema.String }),
 *   options: { retryLimit: 5 }
 * }) {}
 * ```
 *
 * The queue name is the pg-boss queue name and part of the service key, so
 * two classes for one queue name collide in the context, as two queues of one
 * name collide in pg-boss.
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
        // TODO:
        // const Key = Context.Service<Self, Registered<Name>>()(`@wmaurer/effect-sql-pg-boss/JobQueue/${name}`)
        // const encode = Schema.encodeEffect(definition.payload)
        // const decode = Schema.decodeUnknownEffect(definition.payload)
        // return class extends Key {
        //   static queueName = name; static payload = definition.payload; static queueOptions = definition.options ?? {}
        //   static layer = Layer.effect(Key, gen: boss = yield* PgBoss; yield* converge(boss, name, options); return { queueName: name, boss })
        //   static send = (data, options) => gen: { boss } = yield* Key; encoded = yield* encode(data)
        //     id = yield* boss.use((b, connection) => b.send(name, encoded, { ...options, ...connection })); Option.fromNullishOr(id)
        //   static step = (data, options) => ({ [StepTypeId]: identity, queueName: name, encoded: encode(data), options: options ?? {} })
        //   static worker = (handler, options) => Layer.effectDiscard(gen:
        //     { boss } = yield* Key
        //     runPromise = yield* FiberSet.makeRuntimePromise<R>()       // captures the handler's context
        //     if (options.transactional) assert (yield* SqlClient) is the PgNodeClient pg-boss runs on
        //     id = yield* boss.use((b) => b.work(name, options, async (jobs, tx) => {
        //       const joined = tx === undefined ? undefined : BossDatabase.transactionContext(tx)
        //       const program = Effect.flatMap(decodeBatch(jobs), handler)
        //         |> joined ? Effect.updateContext(Context.merge(joined)) : identity
        //       await runPromise(program, { signal: jobs[0].signal })   // rejects with Cause.pretty as Error
        //     }))
        //     yield* Effect.addFinalizer(() => boss.use((b) => b.offWork(name, { id, wait: true })).pipe(Effect.ignore))
        //   )
        // }
        (void name, definition);
        throw new Error("not implemented");
    };

/**
 * A job's payload did not decode with its queue's schema. The batch fails
 * and pg-boss retries it; once the producer and consumer schemas agree again,
 * the retry succeeds.
 *
 * @since 0.1.0
 * @category errors
 */
export class PayloadDecodeError extends Schema.TaggedError<PayloadDecodeError>(
    "@wmaurer/effect-sql-pg-boss/PayloadDecodeError",
)("PayloadDecodeError", {
    queueName: Schema.String,
    jobId: JobId,
    message: Schema.String,
}) {}
