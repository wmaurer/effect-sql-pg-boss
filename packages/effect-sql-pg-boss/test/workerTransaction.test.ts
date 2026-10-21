import { assert, layer } from "@effect/vitest";
import { Array as Arr, Deferred, Effect, Layer, Option, Record, Ref } from "effect";

import { JobQueue } from "../src/index.ts";
import * as Connection from "../src/internal/connection.ts";
import {
    appNotes,
    failure,
    Harness,
    JobData,
    jobStates,
    layer as harness,
    maintenanceQuery,
    sendOutside,
    timeline,
    transactionOf,
    UUID,
    waitFor,
} from "./fixture.ts";

class P1 extends JobQueue.Service<P1>()("p1", { payload: JobData, options: { retryLimit: 0 } }) {}
class P1Follow extends JobQueue.Service<P1Follow>()("p1-follow", { payload: JobData }) {}
class P2 extends JobQueue.Service<P2>()("p2", { payload: JobData, options: { retryLimit: 0, expireInSeconds: 2 } }) {}
class P2b extends JobQueue.Service<P2b>()("p2b", {
    payload: JobData,
    options: { retryLimit: 0, expireInSeconds: 1 },
}) {}

const polled = { transactional: true, pollingIntervalSeconds: 0.5 } as const;

/** A server log line without its pid prefix or the client's prepared statement name. */
const normalize = (line: string) =>
    line
        .replace(/^\[\d+\] /, "")
        .replace(/execute effect_\w+:/, "execute <statement>:")
        .replace(/(SELECT set_config)\(.*/, "$1(...)");

/** The lines from the worker transaction's BEGIN to its ROLLBACK: the transaction that ran pg_sleep(10). */
const workerTransaction = (lines: ReadonlyArray<string>) => {
    const sleepAt = Option.getOrElse(
        Arr.findFirstIndex(lines, (line) => line.includes("execute") && line.includes("pg_sleep(10)")),
        () => 0,
    );
    const beginAt = Option.getOrElse(
        Arr.findLastIndex(Arr.take(lines, sleepAt), (line) => line.includes(": BEGIN")),
        () => 0,
    );
    const rollbackAt = Option.getOrElse(
        Arr.findFirstIndex(lines, (line, index) => index > sleepAt && line.includes(": ROLLBACK")),
        () => lines.length,
    );
    return Arr.take(Arr.drop(lines, beginAt), rollbackAt + 1 - beginAt);
};

/** How the transaction that first bound `tag` as a parameter ended: `COMMIT` or `ROLLBACK`. */
const transactionEnd = (lines: ReadonlyArray<string>, tag: string) =>
    Option.flatMap(
        Arr.findFirstIndex(lines, (line) => line.includes("Parameters:") && line.includes(`'${tag}'`)),
        (boundAt) =>
            Option.map(
                Arr.findFirst(lines, (line, index) => index > boundAt && /: (COMMIT|ROLLBACK)$/.test(line)),
                (line) => line.replace(/^.*: /, ""),
            ),
    );

layer(harness({ database: "worker_transaction" }, Layer.mergeAll(P1.layer, P1Follow.layer, P2.layer)), {
    excludeTestServices: true,
    timeout: "60 seconds",
})("the parked worker transaction", (it) => {
    it.effect("P1: the handler's writes, savepoints and sends commit with the job, or roll back with it", () =>
        Effect.gen(function* () {
            const { sql, serverLog } = yield* Harness;
            const handler = yield* timeline;
            const seen = yield* Ref.make<
                Record<string, { readonly sameTx: boolean; readonly followId: string; readonly pid: number }>
            >({});
            yield* Layer.build(
                P1.worker(
                    (jobs) =>
                        Effect.forEach(
                            jobs,
                            ({ data: job }) =>
                                Effect.gen(function* () {
                                    const [row] = yield* sql<{ readonly pid: number }>`select pg_backend_pid() as pid`;
                                    yield* sql`insert into app ${sql.insert({ tag: job.tag, note: "outer" })}`;
                                    yield* sql.withTransaction(
                                        sql`insert into app ${sql.insert({ tag: job.tag, note: "nested-savepoint" })}`,
                                    );
                                    yield* sql
                                        .withTransaction(
                                            Effect.andThen(
                                                sql`insert into app ${sql.insert({ tag: job.tag, note: "nested-rolled-back" })}`,
                                                Effect.fail("rolled back on purpose"),
                                            ),
                                        )
                                        .pipe(Effect.ignore);
                                    const handlerDb = yield* Connection.currentDb(sql);
                                    const pgBossTx = yield* transactionOf(job.tag);
                                    const followId = yield* P1Follow.send({ tag: job.tag });
                                    yield* Ref.update(
                                        seen,
                                        Record.set(job.tag, {
                                            sameTx: Option.contains(pgBossTx, handlerDb),
                                            followId: Option.getOrElse(followId, () => ""),
                                            pid: row?.pid ?? 0,
                                        }),
                                    );
                                    if (job.fail === true) {
                                        yield* handler.mark("failing");
                                        return yield* Effect.die(new Error("handler failed on purpose"));
                                    }
                                }),
                            { discard: true },
                        ),
                    polled,
                ),
            );
            yield* sendOutside("p1", { tag: "p1-ok", fail: false });
            yield* sendOutside("p1", { tag: "p1-bad", fail: true });
            yield* waitFor("p1-ok", "p1", "completed");
            yield* waitFor("p1-bad", "p1", "failed");

            assert.deepStrictEqual(yield* appNotes("p1-ok"), ["nested-savepoint", "outer"]);
            assert.deepStrictEqual(yield* jobStates("p1-ok"), ["p1:completed", "p1-follow:created"]);
            assert.deepStrictEqual(yield* appNotes("p1-bad"), []);
            assert.deepStrictEqual(yield* jobStates("p1-bad"), ["p1:failed"]);

            const observed = yield* Ref.get(seen);
            for (const tag of ["p1-ok", "p1-bad"]) {
                assert.strictEqual(
                    observed[tag]?.sameTx,
                    true,
                    `${tag}: currentDb is the tx pg-boss handed the handler`,
                );
                assert.match(
                    observed[tag]?.followId ?? "",
                    UUID,
                    `${tag}: send in the worker transaction returned an id`,
                );
            }

            const failed = yield* failure("p1-bad", "p1");
            assert.isBelow(
                failed.failedAt - (yield* handler.at("failing")),
                2500,
                "rollback() did not wait for its timeout: pg-boss fails the job once it resolves",
            );
            assert.strictEqual(String(failed.output?.message).split("\n")[0], "Error: handler failed on purpose");

            if (Option.isSome(serverLog)) {
                for (const [tag, end] of [
                    ["p1-ok", "COMMIT"],
                    ["p1-bad", "ROLLBACK"],
                ] as const) {
                    const lines = yield* serverLog.value(observed[tag]?.pid ?? 0);
                    assert.deepStrictEqual(
                        transactionEnd(lines, tag),
                        Option.some(end),
                        `${tag}: pg-boss settled with ${end}`,
                    );
                }
            }

            const settledTx = yield* transactionOf("p1-ok");
            assert.isTrue(Option.isSome(settledTx));
            if (Option.isNone(settledTx)) return;
            const afterSettle = yield* Effect.promise(() =>
                settledTx.value.executeSql("select 1", []).then(
                    () => "ran",
                    (error: Error) => `rejected: ${error.message}`,
                ),
            );
            assert.strictEqual(afterSettle, "rejected: pg-boss used a worker transaction after it was settled");
        }),
    );

    it.effect("P2: an abandoned handler is interrupted before ROLLBACK, and its connection goes back to the pool", () =>
        Effect.gen(function* () {
            const { sql, serverLog } = yield* Harness;
            const handler = yield* timeline;
            const backend = yield* Deferred.make<number>();
            yield* Layer.build(
                P2.worker(
                    (jobs) =>
                        Effect.forEach(
                            jobs,
                            ({ data: job }) =>
                                Effect.gen(function* () {
                                    const [row] = yield* sql<{ readonly pid: number }>`select pg_backend_pid() as pid`;
                                    yield* Deferred.succeed(backend, row?.pid ?? 0);
                                    yield* handler.mark("started");
                                    yield* sql`select pg_sleep(10)`;
                                    yield* sql`insert into app ${sql.insert({ tag: job.tag, note: "after-sleep" })}`;
                                    yield* handler.mark("finished");
                                }).pipe(Effect.onInterrupt(() => handler.mark("interrupted"))),
                            { discard: true },
                        ),
                    polled,
                ),
            );
            yield* sendOutside("p2", { tag: "p2-abandon" });
            yield* waitFor("p2-abandon", "p2", "failed");

            const pid = yield* Deferred.await(backend);
            const startedAt = yield* handler.at("started");
            const interruptedAt = yield* handler.at("interrupted");
            const failed = yield* failure("p2-abandon", "p2");
            assert.strictEqual(failed.output?.message, "handler execution exceeded 2s", "pg-boss gave up on expiry");
            assert.isAtLeast(interruptedAt - startedAt, 1000, "pg-boss gave up on expiry, not at once");
            assert.isBelow(interruptedAt - startedAt, 9000, "while the handler was still in pg_sleep(10)");
            assert.isAtMost(interruptedAt, failed.failedAt, "interrupted before rollback() resolved");
            assert.isBelow(failed.failedAt - interruptedAt, 2500, "rollback() did not wait for its timeout");
            assert.isFalse(yield* handler.reached("finished"));

            assert.deepStrictEqual(yield* appNotes("p2-abandon"), []);
            assert.deepStrictEqual(yield* jobStates("p2-abandon"), ["p2:failed"]);

            assert.deepStrictEqual(
                yield* maintenanceQuery("select pid, state from pg_stat_activity where pid = $1", [pid]),
                [{ pid, state: "idle" }],
            );
            const held = yield* Effect.all(
                Arr.makeBy(6, () =>
                    sql.withTransaction(sql<{ readonly pid: number }>`select pg_backend_pid() as pid, pg_sleep(0.2)`),
                ),
                { concurrency: "unbounded" },
            );
            const pids = new Set(Arr.map(held, ([row]) => row?.pid));
            assert.strictEqual(pids.size, 6, "the pool's 6 connections all held a transaction at once");
            assert.isTrue(pids.has(pid), "one of them on the abandoned handler's backend");

            if (Option.isSome(serverLog)) {
                const lines = yield* serverLog.value(pid);
                assert.deepStrictEqual(Arr.map(workerTransaction(lines), normalize), [
                    "LOG:  execute <statement>: BEGIN",
                    "LOG:  execute <unnamed>: SELECT set_config(...)",
                    "DETAIL:  Parameters: $1 = 'transaction_timeout', $2 = '7000'",
                    "LOG:  execute <statement>: select pg_backend_pid() as pid",
                    "LOG:  execute <statement>: select pg_sleep(10)",
                    "ERROR:  canceling statement due to user request",
                    "STATEMENT:  select pg_sleep(10)",
                    "LOG:  execute <statement>: ROLLBACK",
                ]);
                assert.deepStrictEqual(
                    Arr.filter(lines, (line) => line.includes("after-sleep")),
                    [],
                    "the handler's insert never reached the server",
                );
            }
        }),
    );
});

layer(harness({ database: "worker_transaction_bounded", rollbackTimeout: "1 second" }, P2b.layer), {
    excludeTestServices: true,
    timeout: "60 seconds",
})("the bounded rollback", (it) => {
    it.effect("P2b: rollback() gives up after rollbackTimeout on a handler that ignores interruption", () =>
        Effect.gen(function* () {
            const { sql } = yield* Harness;
            const handler = yield* timeline;
            yield* Layer.build(
                P2b.worker(
                    (jobs) =>
                        Effect.forEach(
                            jobs,
                            ({ data: job }) =>
                                Effect.gen(function* () {
                                    yield* handler.mark("started");
                                    yield* Effect.uninterruptible(Effect.sleep("4 seconds"));
                                    yield* handler.mark("woke up");
                                    yield* sql`insert into app ${sql.insert({ tag: job.tag, note: "after-wedge" })}`;
                                }).pipe(Effect.onInterrupt(() => handler.mark("interrupted"))),
                            { discard: true },
                        ),
                    polled,
                ),
            );
            yield* sendOutside("p2b", { tag: "p2b-wedged" });
            yield* waitFor("p2b-wedged", "p2b", "failed");
            assert.isFalse(yield* handler.reached("interrupted"), "the job failed while the handler still slept");
            assert.deepStrictEqual(yield* jobStates("p2b-wedged"), ["p2b:failed"]);

            const startedAt = yield* handler.at("started");
            const failed = yield* failure("p2b-wedged", "p2b");
            assert.strictEqual(failed.output?.message, "handler execution exceeded 1s", "pg-boss gave up on expiry");
            assert.isAtLeast(
                failed.failedAt - startedAt,
                1900,
                "rollback() waited for rollbackTimeout (1 s) after the expiry (1 s) before pg-boss failed the job",
            );
            assert.isBelow(failed.failedAt - startedAt, 3000, "and no longer");

            yield* Effect.sleep("4 seconds");
            const interruptedAt = yield* handler.at("interrupted");
            assert.isAtLeast(interruptedAt - startedAt, 3500, "interrupted once it left the uninterruptible sleep");
            assert.isFalse(yield* handler.reached("woke up"));
            assert.deepStrictEqual(yield* appNotes("p2b-wedged"), []);
        }),
    );
});
