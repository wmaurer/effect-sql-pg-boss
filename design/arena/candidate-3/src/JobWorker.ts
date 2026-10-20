/**
 * Workers as layers. A transactional worker runs each batch inside a real
 * `sql.withTransaction` on the app's `PgClient`; pg-boss completes the jobs in
 * that same transaction.
 *
 * @since 0.1.0
 */
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import type { Job, JobQueue } from "./JobQueue.ts";
import type { PgBoss, PgBossError } from "./PgBoss.ts";
import type * as Boss from "pg-boss";

/**
 * pg-boss work options the caller may set, plus `transactional`.
 *
 * Excluded: `includeMetadata` (the handler gets the library's `Job`), and
 * `perJobResults` (one outcome per batch; see {@link layer}).
 *
 * @since 0.1.0
 */
export interface Options extends Omit<Boss.WorkOptions, "includeMetadata" | "perJobResults" | "transactional"> {
    /**
     * Run the handler inside the job's transaction. Defaults to `true`.
     *
     * Set `false` for handlers that call slow external services: the handler
     * then runs with no transaction, and pg-boss completes the job on its own
     * pool after the handler succeeds.
     */
    readonly transactional?: boolean | undefined;
}

/**
 * Registers a worker for `queue` for the lifetime of the layer.
 *
 * Per batch (`batchSize`, default 1):
 * 1. pg-boss fetches jobs on its housekeeping pool, then (transactional)
 *    calls `beginTransaction()`, which parks a `sql.withTransaction` on a
 *    native connection.
 * 2. The library decodes each job's payload with the queue's schema and runs
 *    `handler` for each job in order, with the worker layer's context plus
 *    the parked transaction's (so `SqlClient`, nested `withTransaction`,
 *    `afterCommit`, `Reactivity`, and `JobQueue.send` all see the
 *    transaction).
 * 3. Success: pg-boss completes the batch through the transaction, then
 *    commits. Any failure, defect, decode error, or interruption: rollback,
 *    then pg-boss fails the batch, which retries per the queue's policy.
 *    A batch is the unit of atomicity: with `batchSize > 1`, one failing job
 *    fails all of them.
 *
 * Interruption: each job's `AbortSignal` (job expiry, `boss.stop()`)
 * interrupts the handler fiber.
 *
 * Handler errors become the job's recorded failure as the pretty-printed
 * `Cause`. The typed `E` is not stored as data.
 *
 * Release: `boss.offWork(queue.name, { id, wait: true })`, so the layer
 * closes only after its in-flight batches settled.
 *
 * @since 0.1.0
 */
export const layer = <Q extends JobQueue.Any, E, R>(
    _queue: Q,
    _handler: (job: Job<JobQueue.Payload<Q>>) => Effect.Effect<void, E, R>,
    _options?: Options,
): Layer.Layer<never, PgBossError, PgBoss | R | Q["payload"]["DecodingServices"]> =>
    // TODO
    // Layer.effectDiscard(Effect.gen(function*() {
    //   const runtime = (yield* PgBoss)[RuntimeId]
    //   const context = yield* Effect.context<R | DecodingServices>()
    //   yield* runtime.ensureQueue(queue)
    //   const runJob = (job, txContext) =>
    //     Effect.runPromiseWith(txContext ? Context.merge(context, txContext) : context)(
    //       Effect.flatMap(decodeJob(queue, job), handler), { signal: job.signal })
    //   const callback = transactional
    //     ? async (jobs, tx) => { const tc = runtime.transactionContext(tx); if (!tc) throw lostIdentity; for (job of jobs) await runJob(job, tc) }
    //     : async (jobs) => { for (job of jobs) await runJob(job, undefined) }
    //   const id = yield* runtime.call("work", () => runtime.boss.work(queue.name, { ...options, transactional }, callback))
    //   yield* Effect.addFinalizer(() => Effect.promise(() => runtime.boss.offWork(queue.name, { id, wait: true })))
    // }))
    Layer.effectDiscard(Effect.die(new Error("not implemented")));
