import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import type { PgBoss, PgBossError } from "./PgBoss.ts";
/**
 * Typed pg-boss queues: definitions, `send`, and `flow`.
 *
 * A queue definition is the single source of truth for a queue's name, its
 * payload schema, and the options it is created with. `send` encodes with the
 * schema; `JobWorker.layer` decodes with the same one.
 *
 * @since 0.1.0
 */
import type * as Arr from "effect/Array";
import type * as Brand from "effect/Brand";
import type * as Option from "effect/Option";
import type { SqlError } from "effect/sql/SqlError";
import type * as Boss from "pg-boss";

/**
 * A pg-boss job id (a UUID string).
 *
 * @since 0.1.0
 */
export type JobId = string & Brand.Brand<"@wmaurer/effect-sql-pg-boss/JobId">;

/**
 * @since 0.1.0
 */
export const JobId: Schema.brand<Schema.String, "@wmaurer/effect-sql-pg-boss/JobId"> = Schema.String.pipe(
    Schema.brand("@wmaurer/effect-sql-pg-boss/JobId"),
);

/**
 * Options a queue is created with. pg-boss's own `Queue` minus `name`, which
 * the definition owns.
 *
 * @since 0.1.0
 */
export type QueueOptions = Omit<Boss.Queue, "name">;

/**
 * Per-job options. pg-boss's `SendOptions` minus `db`: the library chooses the
 * connection (see {@link send}).
 *
 * @since 0.1.0
 */
export type SendOptions = Omit<Boss.SendOptions, "db">;

/**
 * A queue definition.
 *
 * Invariant: `name` and `payload` never change for a live queue. The payload
 * travels as JSON, so the schema is applied through `Schema.toCodecJson`:
 * a `Schema.BigInt` field is stored as a string and comes back as `bigint`.
 *
 * @since 0.1.0
 */
export interface JobQueue<Name extends string, S extends Schema.Top> {
    readonly name: Name;
    readonly payload: S;
    readonly queue: QueueOptions;
}

/**
 * @since 0.1.0
 */
export declare namespace JobQueue {
    /**
     * @since 0.1.0
     */
    export type Any = JobQueue<string, Schema.Top>;
    /**
     * The payload type a caller sends and a handler receives.
     *
     * @since 0.1.0
     */
    export type Payload<Q extends Any> = Q["payload"]["Type"];
}

/**
 * Declares a queue. Pure: nothing touches the database until the first
 * `send` or worker for it, which creates the queue if it is missing.
 *
 * @since 0.1.0
 */
export const make = <const Name extends string, S extends Schema.Top>(
    name: Name,
    options: { readonly payload: S; readonly queue?: QueueOptions | undefined },
): JobQueue<Name, S> => ({ name, payload: options.payload, queue: options.queue ?? {} });

/**
 * A job as a handler sees it: the decoded payload plus the fields a handler
 * needs to reason about retries. pg-boss's raw `Job` stays internal.
 *
 * @since 0.1.0
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
 * Inserts one job.
 *
 * **Where it runs.** On the connection the fiber's `sql` statements would use
 * right now: inside `sql.withTransaction` (including a transactional worker's
 * handler) the job commits or rolls back with that transaction; outside one it
 * is inserted on its own.
 *
 * Returns `Option.none()` when pg-boss drops the job (singleton or throttle
 * policies). Fails with `SqlError` when the insert itself fails (the original
 * driver error, so `UniqueViolation` etc. stay matchable), `PgBossError` for
 * pg-boss's own validation, and `SchemaError` if the payload does not encode.
 *
 * @since 0.1.0
 */
export const send = <Q extends JobQueue.Any>(
    _queue: Q,
    _payload: JobQueue.Payload<Q>,
    _options?: SendOptions,
): Effect.Effect<
    Option.Option<JobId>,
    PgBossError | SqlError | Schema.SchemaError,
    PgBoss | Q["payload"]["EncodingServices"]
> =>
    // TODO
    // const data = yield* internal.encodePayload(queue, payload)
    // const runtime = (yield* PgBoss)[internal.RuntimeId]
    // yield* runtime.ensureQueue(queue)
    // const db = yield* runtime.currentDb
    // const id = yield* runtime.call("send", () => runtime.boss.send(queue.name, data, { ...options, db }))
    // return Option.fromNullishOr(id) as Option<JobId>
    Effect.die(new Error("not implemented"));

/**
 * One job in a flow. `ref` names it inside the flow; `dependsOn` lists refs of
 * other steps that must complete first. pg-boss validates the graph.
 *
 * @since 0.1.0
 */
export interface FlowStep<Ref extends string, Q extends JobQueue.Any> {
    readonly ref: Ref;
    readonly queue: Q;
    readonly payload: JobQueue.Payload<Q>;
    readonly dependsOn: ReadonlyArray<string>;
    readonly options: Omit<Boss.JobInsert, "data" | "name"> | undefined;
}

/**
 * @since 0.1.0
 */
export const step = <const Ref extends string, Q extends JobQueue.Any>(
    ref: Ref,
    queue: Q,
    payload: JobQueue.Payload<Q>,
    options?: {
        readonly dependsOn?: ReadonlyArray<string> | undefined;
        readonly job?: Omit<Boss.JobInsert, "data" | "name"> | undefined;
    },
): FlowStep<Ref, Q> => ({
    ref,
    queue,
    payload,
    dependsOn: options?.dependsOn ?? [],
    options: options?.job,
});

/**
 * Inserts a dependency graph of jobs atomically and returns each step's id by
 * its ref.
 *
 * Same "where it runs" rule as {@link send}. Outside a transaction the flow
 * still lands all-or-nothing: the library wraps it in its own transaction.
 *
 * @since 0.1.0
 */
export const flow: <const Steps extends Arr.NonEmptyReadonlyArray<FlowStep<string, JobQueue.Any>>>(
    steps: Steps,
) => Effect.Effect<
    { readonly [K in Steps[number]["ref"]]: JobId },
    PgBossError | SqlError | Schema.SchemaError,
    PgBoss | Steps[number]["queue"]["payload"]["EncodingServices"]
> = (_steps) =>
    // TODO
    // encode every payload, ensure every distinct queue, then
    // runtime.call("flow", () => boss.flow(steps.map(toFlowJob), { db: yield* runtime.currentDb }))
    // The returned Record<ref, id> is cast to the mapped type: pg-boss returns
    // exactly one id per ref or rejects.
    Effect.die(new Error("not implemented"));
