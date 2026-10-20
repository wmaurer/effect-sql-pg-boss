import { assert, layer } from "@effect/vitest";
import { Array as Arr, Deferred, Effect, Option, Record, Ref } from "effect";

import * as Connection from "../src/internal/connection.ts";
import {
    appNotes,
    Harness,
    jobStates,
    layer as harness,
    send,
    sendOutside,
    settled,
    timeline,
    transactionOf,
    UUID,
    waitFor,
    work,
} from "./fixture.ts";

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

layer(
    harness({
        database: "worker_transaction",
        queues: [
            ["p1", { retryLimit: 0 }],
            ["p1-follow", {}],
            ["p2", { retryLimit: 0, expireInSeconds: 2 }],
        ],
    }),
    { excludeTestServices: true, timeout: "60 seconds" },
)("the parked worker transaction", (it) => {
    it.effect("P1: the handler's writes, savepoints and sends commit with the job, or roll back with it", () =>
        Effect.gen(function* () {
            const { sql, housekeeping } = yield* Harness;
            const seen = yield* Ref.make<Record<string, { readonly sameTx: boolean; readonly followId: string }>>({});
            yield* work("p1", (job) =>
                Effect.gen(function* () {
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
                    const followId = yield* send("p1-follow", { tag: job.tag });
                    yield* Ref.update(
                        seen,
                        Record.set(job.tag, {
                            sameTx: Option.contains(pgBossTx, handlerDb),
                            followId: String(followId),
                        }),
                    );
                    if (job.fail === true) return yield* Effect.die(new Error("handler failed on purpose"));
                }),
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

            assert.deepStrictEqual(
                Arr.map(yield* settled("p1-ok"), (settle) => settle.kind),
                ["commit"],
            );
            const rollbacks = yield* settled("p1-bad");
            assert.deepStrictEqual(
                Arr.map(rollbacks, (settle) => settle.kind),
                ["rollback"],
            );
            for (const rollback of rollbacks) {
                assert.isBelow(
                    rollback.resolvedAt - rollback.calledAt,
                    2500,
                    "rollback() did not wait for its timeout",
                );
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

            const stored = yield* Effect.promise(() =>
                housekeeping.executeSql(
                    "select output from pgboss.job where name = 'p1' and data->>'tag' = 'p1-bad'",
                    [],
                ),
            );
            assert.strictEqual(
                String(stored.rows[0]?.output?.message).split("\n")[0],
                "Error: handler failed on purpose",
            );
        }),
    );

    it.effect("P2: an abandoned handler is interrupted before ROLLBACK, and its connection goes back to the pool", () =>
        Effect.gen(function* () {
            const { sql, housekeeping, serverLog } = yield* Harness;
            const handler = yield* timeline;
            const backend = yield* Deferred.make<number>();
            yield* work("p2", (job) =>
                Effect.gen(function* () {
                    const [row] = yield* sql<{ readonly pid: number }>`select pg_backend_pid() as pid`;
                    yield* Deferred.succeed(backend, row?.pid ?? 0);
                    yield* handler.mark("started");
                    yield* sql`select pg_sleep(10)`;
                    yield* sql`insert into app ${sql.insert({ tag: job.tag, note: "after-sleep" })}`;
                    yield* handler.mark("finished");
                }).pipe(Effect.onInterrupt(() => handler.mark("interrupted"))),
            );
            yield* sendOutside("p2", { tag: "p2-abandon" });
            yield* waitFor("p2-abandon", "p2", "failed");

            const pid = yield* Deferred.await(backend);
            const startedAt = yield* handler.at("started");
            const interruptedAt = yield* handler.at("interrupted");
            const rollbacks = yield* settled("p2-abandon");
            assert.deepStrictEqual(
                Arr.map(rollbacks, (settle) => settle.kind),
                ["rollback"],
            );
            for (const rollback of rollbacks) {
                assert.isAtLeast(rollback.calledAt - startedAt, 1000, "pg-boss gave up on expiry, not at once");
                assert.isBelow(rollback.calledAt - startedAt, 9000, "while the handler was still in pg_sleep(10)");
                assert.isAtLeast(interruptedAt, rollback.calledAt, "interrupted after pg-boss called rollback()");
                assert.isAtMost(interruptedAt, rollback.resolvedAt, "interrupted before rollback() resolved");
                assert.isBelow(
                    rollback.resolvedAt - rollback.calledAt,
                    2500,
                    "rollback() did not wait for its timeout",
                );
            }
            assert.isFalse(yield* handler.reached("finished"));

            assert.deepStrictEqual(yield* appNotes("p2-abandon"), []);
            assert.deepStrictEqual(yield* jobStates("p2-abandon"), ["p2:failed"]);

            const activity = yield* Effect.promise(() =>
                housekeeping.executeSql("select pid, state from pg_stat_activity where pid = $1", [pid]),
            );
            assert.deepStrictEqual(activity.rows, [{ pid, state: "idle" }]);
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

layer(
    harness({
        database: "worker_transaction_bounded",
        queues: [["p2b", { retryLimit: 0, expireInSeconds: 1 }]],
        rollbackTimeout: "1 second",
    }),
    { excludeTestServices: true, timeout: "60 seconds" },
)("the bounded rollback", (it) => {
    it.effect("P2b: rollback() gives up after rollbackTimeout on a handler that ignores interruption", () =>
        Effect.gen(function* () {
            const { sql } = yield* Harness;
            const handler = yield* timeline;
            yield* work("p2b", (job) =>
                Effect.gen(function* () {
                    yield* handler.mark("started");
                    yield* Effect.uninterruptible(Effect.sleep("4 seconds"));
                    yield* handler.mark("woke up");
                    yield* sql`insert into app ${sql.insert({ tag: job.tag, note: "after-wedge" })}`;
                }).pipe(Effect.onInterrupt(() => handler.mark("interrupted"))),
            );
            yield* sendOutside("p2b", { tag: "p2b-wedged" });
            yield* waitFor("p2b-wedged", "p2b", "failed");
            assert.isFalse(yield* handler.reached("interrupted"), "the job failed while the handler still slept");
            assert.deepStrictEqual(yield* jobStates("p2b-wedged"), ["p2b:failed"]);

            const rollbacks = yield* settled("p2b-wedged");
            assert.deepStrictEqual(
                Arr.map(rollbacks, (settle) => settle.kind),
                ["rollback"],
            );
            for (const rollback of rollbacks) {
                assert.isAtLeast(rollback.resolvedAt - rollback.calledAt, 950, "rollback() waited for rollbackTimeout");
                assert.isBelow(rollback.resolvedAt - rollback.calledAt, 2500, "and no longer");
            }

            yield* Effect.sleep("4 seconds");
            const startedAt = yield* handler.at("started");
            const interruptedAt = yield* handler.at("interrupted");
            assert.isAtLeast(interruptedAt - startedAt, 3500, "interrupted once it left the uninterruptible sleep");
            assert.isFalse(yield* handler.reached("woke up"));
            assert.deepStrictEqual(yield* appNotes("p2b-wedged"), []);
        }),
    );
});
