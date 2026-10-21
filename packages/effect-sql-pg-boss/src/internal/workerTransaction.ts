/**
 * pg-boss's `beginTransaction()` for transactional workers: a real
 * `sql.withTransaction` on the app's `PgClient`, parked until pg-boss settles
 * it from another call frame.
 *
 * ```
 * parked fiber (forked into the PgBoss layer's FiberSet)
 *   sql.withTransaction(body)
 *     body: resolve { db, commit, rollback }
 *           await settlement
 *           interrupt and await the handler child
 *           settlement = rollback -> fail   (withTransaction runs ROLLBACK)
 *     handler child: await the submitted handler, run it with the
 *                    transaction's services over the worker layer's context
 * ```
 *
 * Invariants:
 * - The handler is a child of the parked fiber. A rollback interrupts it and
 *   waits for it before `ROLLBACK` runs and before the connection returns to
 *   the pool, so no late handler statement can land in another session.
 * - Interrupting the fiber that awaits {@link run} interrupts the handler
 *   child and waits for it.
 * - `commit()` resolves when the parked fiber exits after the real `COMMIT`,
 *   which includes running the app's own `afterCommit` callbacks.
 * - `rollback()` never rejects, is safe to call twice, and gives up after
 *   `rollbackTimeout` by interrupting the parked fiber.
 * - The handle's `db` refuses statements once the transaction is settled.
 *
 * @internal
 */
import * as Arr from "effect/Array";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import { isSqlError, type SqlError } from "effect/sql/SqlError";

import { type Db, dbFor, toDriverError, type TransactionHandle } from "./connection.ts";

import type * as SqlClient from "effect/sql/SqlClient";
import type * as Boss from "pg-boss";

/** Forks into a FiberSet; closing its scope interrupts what is left. */
export type RunFork<R = never> = <A, E>(
    effect: Effect.Effect<A, E, R>,
    options?: Effect.RunOptions,
) => Fiber.Fiber<A, E>;

export interface Options {
    readonly sql: SqlClient.SqlClient;
    /** The PgBoss layer's FiberSet. */
    readonly fork: RunFork;
    /**
     * The context `fork` runs effects in. The handler gets the parked fiber's
     * context minus this one (the transaction's own services) on top of the
     * worker layer's context, so worker-layer services are never overridden.
     */
    readonly base: Context.Context<never>;
    readonly rollbackTimeout: Duration.Input;
}

type Settlement = "commit" | "rollback";

type Submit = <A, E, R>(effect: Effect.Effect<A, E, R>, context: Context.Context<R>) => Effect.Effect<A, E>;

/** A submitted handler, ready to run once it gets the transaction's services. */
type Submission = (delta: Context.Context<never>) => Effect.Effect<void>;

const submitters = new WeakMap<Db, Submit>();

class RollbackRequested {
    readonly _tag = "RollbackRequested";
}

/** The services `inner` has that `outer` lacks or holds a different value for. */
const contextDelta = (inner: Context.Context<never>, outer: Context.Context<never>): Context.Context<never> =>
    Context.makeUnsafe(new Map(Arr.filter(inner.mapUnsafe, ([key, value]) => outer.mapUnsafe.get(key) !== value)));

const closedTransaction = Exit.die(new Error("the worker transaction closed before the handler ran"));

/**
 * The parked fiber's outcome as pg-boss sees it: a SQL failure rejects with
 * the driver error, so pg-boss can read its SQLSTATE fields.
 */
const toPromise = <A>(effect: Effect.Effect<A, SqlError | RollbackRequested>): Promise<A> =>
    Effect.runPromise(Effect.mapError(effect, (error) => (isSqlError(error) ? toDriverError(error) : error)));

export const begin = (options: Options): Promise<TransactionHandle> => {
    const opened = Deferred.makeUnsafe<TransactionHandle, SqlError | RollbackRequested>();
    const settlement = Deferred.makeUnsafe<Settlement>();
    const submitted = Deferred.makeUnsafe<Submission>();
    let settled = false;
    let exited = false;
    let abandon: (() => void) | undefined;

    const submitTo =
        (handler: Fiber.Fiber<void>): Submit =>
        <A, E, R>(effect: Effect.Effect<A, E, R>, context: Context.Context<R>) =>
            Effect.suspend(() => {
                const reply = Deferred.makeUnsafe<A, E>();
                const submission: Submission = (delta) =>
                    Effect.asVoid(
                        Effect.exit(
                            Effect.onExit(Effect.provideContext(effect, Context.merge(context, delta)), (exit) =>
                                Deferred.done(reply, exit),
                            ),
                        ),
                    );
                if (exited || !Deferred.doneUnsafe(submitted, Exit.succeed(submission))) {
                    return closedTransaction;
                }
                abandon = () => Deferred.doneUnsafe(reply, closedTransaction);
                return Effect.onInterrupt(Deferred.await(reply), () => Fiber.interrupt(handler));
            });

    const body = Effect.gen(function* () {
        const context = yield* Effect.context<never>();
        const [conn] = Context.getUnsafe(context, options.sql.transactionService);
        const handler = yield* Effect.forkChild(
            Effect.flatMap(Deferred.await(submitted), (submission) => submission(contextDelta(context, options.base))),
        );
        const db = dbFor(conn, () => settled);
        submitters.set(db, submitTo(handler));
        yield* Deferred.succeed(opened, { db, commit, rollback });
        const decision = yield* Deferred.await(settlement);
        yield* Fiber.interrupt(handler);
        if (decision === "rollback") {
            return yield* Effect.fail(new RollbackRequested());
        }
    });

    const fiber = options.fork(options.sql.withTransaction(body));

    fiber.addObserver((exit) => {
        exited = true;
        if (Exit.isFailure(exit)) {
            Deferred.doneUnsafe(opened, Exit.failCause(exit.cause));
        }
        abandon?.();
    });

    function commit(): Promise<void> {
        if (!Deferred.doneUnsafe(settlement, Exit.succeed("commit"))) {
            return Promise.reject(new Error("the worker transaction is already settled"));
        }
        settled = true;
        return toPromise(Fiber.join(fiber));
    }

    function rollback(): Promise<void> {
        Deferred.doneUnsafe(settlement, Exit.succeed("rollback"));
        settled = true;
        return Effect.runPromise(
            Effect.flatMap(Effect.timeoutOption(Fiber.await(fiber), options.rollbackTimeout), (exit) =>
                Option.isNone(exit) ? Effect.sync(() => fiber.interruptUnsafe()) : Effect.void,
            ),
        );
    }

    return toPromise(Deferred.await(opened));
};

/**
 * Runs a transactional handler inside the worker transaction pg-boss handed
 * it as `tx`, as a child of that transaction's parked fiber, with the caller's
 * context underneath the transaction's services.
 *
 * Dies when `tx` is not a transaction this module opened: pg-boss handed the
 * handler something else, and running it outside the transaction would commit
 * its writes apart from the job.
 */
export const run = <A, E, R>(tx: Db, effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
    Effect.suspend(() => {
        const submit = submitters.get(tx);
        return submit === undefined
            ? Effect.die(new Error("pg-boss handed a transactional handler a db this library did not open"))
            : Effect.flatMap(Effect.context<R>(), (context) => submit(effect, context));
    });

/**
 * The callback a worker registers with `boss.work`. A batch is the unit of
 * atomicity: the handler sees every job of the batch and fails or succeeds as
 * a whole. Any failure, defect or interruption rejects with `Cause.pretty`,
 * which pg-boss records as the jobs' error.
 *
 * Each batch runs in a fiber forked with `fork`, the worker's own, so the
 * worker can interrupt its handlers. Transactional: the fiber runs the
 * handler through {@link run}; pg-boss's abort (expiry, `stop()`) reaches it
 * as the rollback that follows. Otherwise the fiber runs the handler,
 * interrupted by the batch's abort signal.
 */
export const workCallback =
    <E, R>(options: {
        readonly transactional: boolean;
        readonly fork: RunFork<R>;
        readonly handler: (jobs: ReadonlyArray<Boss.Job<unknown>>) => Effect.Effect<void, E, R>;
    }) =>
    (jobs: Array<Boss.Job<unknown>>, tx?: Db): Promise<void> => {
        const program = options.handler(jobs);
        const fiber = !options.transactional
            ? options.fork(program, { signal: jobs[0]?.signal })
            : options.fork(
                  tx === undefined
                      ? Effect.die(new Error("pg-boss called a transactional handler without its transaction"))
                      : run(tx, program),
              );
        return Effect.runPromise(Fiber.await(fiber)).then((exit) =>
            Exit.isFailure(exit) ? Promise.reject(new Error(Cause.pretty(exit.cause))) : undefined,
        );
    };
