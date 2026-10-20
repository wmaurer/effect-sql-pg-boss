import * as Schema from "effect/Schema";

/**
 * A queue definition: its name, its payload schema and its policy, in one
 * value. Send and work are typed from it, and `Boss.layer` creates the queue
 * from it. This is the single source of truth for a queue's payload type.
 *
 * Invariants:
 * - The payload is the schema's `Type`. The stored JSON is `toCodecJson` of the
 *   schema, so `Schema.Date`, `Schema.BigInt` and friends round-trip.
 * - The schema may not need services, so encoding and decoding cannot fail
 *   because a layer is missing.
 * - A queue value is plain data. Creating one does not touch the database.
 *
 * @since 0.1.0
 */
import type * as Duration from "effect/Duration";

/** @since 0.1.0 */
export const TypeId: TypeId = "~@wmaurer/effect-sql-pg-boss/Queue";

/** @since 0.1.0 */
export type TypeId = "~@wmaurer/effect-sql-pg-boss/Queue";

/**
 * Id of a job, as pg-boss assigns it (a UUID string).
 *
 * @since 0.1.0
 */
export const JobId = Schema.String.pipe(Schema.brand("@wmaurer/effect-sql-pg-boss/JobId"));

/** @since 0.1.0 */
export type JobId = typeof JobId.Type;

/**
 * Retry policy of the jobs in a queue. Maps to pg-boss `retryLimit`,
 * `retryDelay`, `retryBackoff` and `retryDelayMax`.
 *
 * @since 0.1.0
 */
export interface RetryPolicy {
    readonly limit: number;
    readonly delay?: Duration.Input | undefined;
    readonly backoff?: boolean | undefined;
    readonly maxDelay?: Duration.Input | undefined;
}

/** @since 0.1.0 */
export interface QueueOptions {
    readonly policy?: "standard" | "short" | "singleton" | "stately" | "exclusive" | undefined;
    readonly retry?: RetryPolicy | undefined;
    /** How long a worker may hold a job before pg-boss expires it. */
    readonly expireIn?: Duration.Input | undefined;
    /** How long finished jobs are kept. */
    readonly keepFor?: Duration.Input | undefined;
    /**
     * Queue that receives jobs that fail for good. Given as a queue value so a
     * dead letter queue cannot be misspelled. It must also be listed in
     * `Boss.layer({ queues })`, which creates it first.
     */
    readonly deadLetter?: Queue<string, any> | undefined;
}

/**
 * @since 0.1.0
 */
export interface Queue<out Name extends string, out A extends object> {
    readonly [TypeId]: TypeId;
    readonly name: Name;
    /** JSON codec derived from the schema given to {@link make}. */
    readonly codec: Schema.Codec<A, unknown>;
    readonly options: QueueOptions;
}

/**
 * A job as a handler sees it. `data` is already decoded.
 *
 * @since 0.1.0
 */
export interface Job<out A> {
    readonly id: JobId;
    readonly data: A;
    /** 0 on the first attempt. */
    readonly retryCount: number;
}

/**
 * Per-job options for `Boss.send` and `Flow.node`. A deliberately small subset
 * of pg-boss' `SendOptions`, in Effect types. Anything else goes through
 * `Boss.use`.
 *
 * @since 0.1.0
 */
export interface SendOptions {
    readonly priority?: number | undefined;
    /** Delay before the job becomes available, or an absolute time. */
    readonly startAfter?: Duration.Input | Date | undefined;
    /** At most one job with this key per queue while the queue policy says so. */
    readonly singletonKey?: string | undefined;
    readonly retry?: RetryPolicy | undefined;
    readonly expireIn?: Duration.Input | undefined;
}

/**
 * Defines a queue.
 *
 * @example
 * const SendReceipt = Queue.make("send-receipt", Schema.Struct({ orderId: OrderId }))
 *
 * @since 0.1.0
 */
export const make = <
    const Name extends string,
    S extends Schema.Constraint & {
        readonly Type: object;
        readonly DecodingServices: never;
        readonly EncodingServices: never;
    },
>(
    name: Name,
    schema: S,
    options: QueueOptions = {},
): Queue<Name, S["Type"]> => {
    // TODO: return { [TypeId]: TypeId, name, codec: Schema.toCodecJson(schema), options }
    void name;
    void schema;
    void options;
    throw new Error("not implemented");
};
