/**
 * pg-boss's `beginTransaction()` for transactional workers: a real
 * `sql.withTransaction` on the app's `PgClient`, parked until pg-boss settles
 * it from another call frame.
 *
 * ```
 * parked fiber (forked into the PgBoss layer's FiberSet)
 *   sql.withTransaction(body)
 *     body: afterCommit(committed)          registered first, so it runs first
 *           resolve { db, commit, rollback }
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
 * - `commit()` resolves on the committed signal, which runs right after the
 *   real `COMMIT` and the connection release, and does not wait for the app's
 *   own `afterCommit` callbacks.
 * - `rollback()` never rejects, is safe to call twice, and gives up after
 *   `rollbackTimeout` by interrupting the parked fiber.
 * - The handle's `db` refuses statements once the transaction is settled.
 *
 * @internal
 */
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";

import { type Db, dbFor, toDriverError, type TransactionHandle } from "./connection.ts";

import type * as SqlClient from "effect/sql/SqlClient";
import type * as Boss from "pg-boss";

/** Forks into the PgBoss layer's FiberSet; closing the layer interrupts what is left. */
export type RunFork = <A, E>(effect: Effect.Effect<A, E>) => Fiber.Fiber<A, E>;

export interface Options {
    readonly sql: SqlClient.SqlClient;
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

type Submit = <A, E>(effect: Effect.Effect<A, E>, context: Context.Context<never>) => Promise<Exit.Exit<A, E>>;

interface Submission {
    readonly effect: Effect.Effect<unknown, unknown>;
    readonly context: Context.Context<never>;
    readonly reply: Deferred.Deferred<Exit.Exit<unknown, unknown>>;
}

const submitters = new WeakMap<Db, Submit>();

class RollbackRequested {
    readonly _tag = "RollbackRequested";
}

/** The services `inner` has that `outer` lacks or holds a different value for. */
const contextDelta = (inner: Context.Context<never>, outer: Context.Context<never>): Context.Context<never> =>
    Context.makeUnsafe(new Map([...inner.mapUnsafe].filter(([key, value]) => outer.mapUnsafe.get(key) !== value)));

const closedTransaction = Exit.die(new Error("the worker transaction closed before the handler ran"));

export const begin = (options: Options): Promise<TransactionHandle> =>
    new Promise((resolve, reject) => {
        const settlement = Deferred.makeUnsafe<Settlement>();
        const submitted = Deferred.makeUnsafe<Submission>();
        const committed = Promise.withResolvers<void>();
        let settled = false;
        let begun = false;
        let exited = false;

        const serve = (delta: Context.Context<never>) =>
            Effect.flatMap(Deferred.await(submitted), ({ context, effect, reply }) =>
                Effect.onExit(Effect.provideContext(effect, Context.merge(context, delta)), (exit) =>
                    Deferred.succeed(reply, exit),
                ),
            );

        let pendingReply: Deferred.Deferred<Exit.Exit<unknown, unknown>> | undefined;
        const submit: Submit = (effect, context) => {
            const reply = Deferred.makeUnsafe<Exit.Exit<unknown, unknown>>();
            if (exited || !Deferred.doneUnsafe(submitted, Exit.succeed({ effect, context, reply }))) {
                return Promise.resolve(closedTransaction);
            }
            pendingReply = reply;
            return Effect.runPromise(Deferred.await(reply)) as Promise<Exit.Exit<any, any>>;
        };

        const body = Effect.gen(function* () {
            yield* options.sql.afterCommit(Effect.sync(() => committed.resolve()));
            const context = yield* Effect.context<never>();
            const [conn] = Context.getUnsafe(context, options.sql.transactionService);
            const handler = yield* Effect.forkChild(serve(contextDelta(context, options.base)));
            const db = dbFor(conn, () => settled);
            submitters.set(db, submit);
            begun = true;
            resolve({ db, commit, rollback });
            const decision = yield* Deferred.await(settlement);
            yield* Fiber.interrupt(handler);
            if (decision === "rollback") {
                return yield* Effect.fail(new RollbackRequested());
            }
        });

        const fiber = options.fork(options.sql.withTransaction(body));

        fiber.addObserver((exit) => {
            exited = true;
            if (!begun && Exit.isFailure(exit)) {
                reject(toDriverError(Cause.squash(exit.cause)));
            }
            if (pendingReply !== undefined) {
                Deferred.doneUnsafe(pendingReply, Exit.succeed(closedTransaction));
            }
        });

        function commit(): Promise<void> {
            if (!Deferred.doneUnsafe(settlement, Exit.succeed("commit"))) {
                return Promise.reject(new Error("the worker transaction is already settled"));
            }
            settled = true;
            return new Promise((resolveCommit, rejectCommit) => {
                committed.promise.then(resolveCommit);
                fiber.addObserver((exit) =>
                    Exit.isFailure(exit) ? rejectCommit(toDriverError(Cause.squash(exit.cause))) : resolveCommit(),
                );
            });
        }

        function rollback(): Promise<void> {
            Deferred.doneUnsafe(settlement, Exit.succeed("rollback"));
            settled = true;
            return new Promise((resolveRollback) => {
                const timer = setTimeout(() => {
                    fiber.interruptUnsafe();
                    resolveRollback();
                }, Duration.toMillis(options.rollbackTimeout));
                fiber.addObserver(() => {
                    clearTimeout(timer);
                    resolveRollback();
                });
            });
        }
    });

/**
 * Runs a transactional handler inside the worker transaction pg-boss handed
 * it as `tx`, as a child of that transaction's parked fiber, with `context`
 * (the worker layer's) underneath the transaction's services.
 *
 * Rejects when `tx` is not a transaction this module opened: pg-boss handed
 * the handler something else, and running it outside the transaction would
 * commit its writes apart from the job.
 */
export const run = <A, E, R>(
    tx: Db,
    effect: Effect.Effect<A, E, R>,
    context: Context.Context<R>,
): Promise<Exit.Exit<A, E>> => {
    const submit = submitters.get(tx);
    if (submit === undefined) {
        return Promise.reject(new Error("pg-boss handed a transactional handler a db this library did not open"));
    }
    return submit(effect as Effect.Effect<A, E>, context as Context.Context<never>);
};

/**
 * The callback a worker registers with `boss.work`. A batch is the unit of
 * atomicity: the handler sees every job of the batch and fails or succeeds as
 * a whole. Any failure, defect or interruption rejects with `Cause.pretty`,
 * which pg-boss records as the jobs' error.
 *
 * Transactional: the handler runs through {@link run}; pg-boss's abort
 * (expiry, `stop()`) reaches it as the rollback that follows. Otherwise it
 * runs on its own, interrupted by the batch's abort signal.
 */
export const workCallback =
    <R>(options: {
        readonly transactional: boolean;
        readonly context: Context.Context<R>;
        readonly handler: (jobs: ReadonlyArray<Boss.Job<unknown>>) => Effect.Effect<unknown, unknown, R>;
    }) =>
    async (jobs: Array<Boss.Job<unknown>>, tx?: Db): Promise<void> => {
        const program = options.handler(jobs);
        let exit: Exit.Exit<unknown, unknown>;
        if (options.transactional) {
            if (tx === undefined) throw new Error("pg-boss called a transactional handler without its transaction");
            exit = await run(tx, program, options.context);
        } else {
            const signal = jobs[0]?.signal;
            exit = await Effect.runPromiseExitWith(options.context)(
                program,
                signal === undefined ? undefined : { signal },
            );
        }
        if (Exit.isFailure(exit)) throw new Error(Cause.pretty(exit.cause));
    };
