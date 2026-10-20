import { execSync } from "node:child_process";

// Runtime proof of the synthesis's internal mechanisms (P1-P5 in
// synthesis-spec.md). It imports the real src/internal modules and wires them
// the way PgBoss.layer and Queue.worker describe.
//
// Run (from the synthesis dir):
//   docker run -d --rm --name synth-pg -e POSTGRES_PASSWORD=probe -p 127.0.0.1:55511:5432 \
//     postgres:17 -c log_statement=all -c "log_line_prefix=[%p] "
//   node prototype/synth.ts > prototype/synth.out
import { PgClient } from "@effect/sql-pg";
import { type Duration, Effect, Exit, FiberSet, Redacted } from "effect";
import { PgBoss } from "pg-boss";

import * as Connection from "../src/internal/connection.ts";
import * as Housekeeping from "../src/internal/housekeeping.ts";
import { connectionBudget } from "../src/internal/runtime.ts";
import * as WorkerTransaction from "../src/internal/workerTransaction.ts";

const URL = "postgres://postgres:probe@127.0.0.1:55511/postgres";
const t0 = Date.now();
const ms = () => `${String(Date.now() - t0).padStart(6)}ms`;
const log = (...parts: Array<unknown>) => console.log(ms(), ...parts);
const json = (u: unknown) => JSON.stringify(u, (_, v) => (typeof v === "bigint" ? `${v}n` : v));

const program = Effect.gen(function* () {
    const sql = yield* PgClient.PgClient;
    const housekeeping = yield* Housekeeping.make(sql.config, 3);
    const fork = yield* FiberSet.makeRuntime<never>();
    const base = yield* Effect.context<never>();

    const settleTimes: Array<string> = [];
    let commitResolvedAt: number | undefined;
    let rollbackTimeout: Duration.Input = "5 seconds";
    const db = {
        executeSql: housekeeping.executeSql,
        beginTransaction: async () => {
            const handle = await WorkerTransaction.begin({ sql, fork, base, rollbackTimeout });
            return {
                db: handle.db,
                commit: async () => {
                    const start = Date.now();
                    await handle.commit();
                    commitResolvedAt = Date.now();
                    settleTimes.push(`commit() resolved after ${commitResolvedAt - start}ms`);
                },
                rollback: async () => {
                    const start = Date.now();
                    log("pg-boss called rollback()");
                    await handle.rollback();
                    settleTimes.push(`rollback() resolved after ${Date.now() - start}ms`);
                    log(`rollback() resolved after ${Date.now() - start}ms`);
                },
            };
        },
    };

    const boss = new PgBoss({ db, schema: "pgboss" });
    boss.on("error", (e: unknown) => log("[pg-boss error]", (e as Error)?.message));
    yield* Effect.acquireRelease(
        Connection.call("start", () => boss.start()),
        () => Effect.promise(() => boss.stop({ graceful: true, timeout: 5000, close: false })),
    );
    for (const [name, options] of [
        ["p1", { retryLimit: 0 }],
        ["p1-follow", {}],
        ["p2", { retryLimit: 0, expireInSeconds: 2 }],
        ["p2b", { retryLimit: 0, expireInSeconds: 1 }],
        ["p3", { retryLimit: 0 }],
        ["flow-a", {}],
        ["flow-b", {}],
    ] as const) {
        yield* Connection.call("createQueue", async () => {
            if (!(await boss.getQueue(name))) await boss.createQueue(name, options);
        });
    }
    yield* sql`drop table if exists synth_app`;
    yield* sql`drop table if exists synth_unique`;
    yield* sql`create table synth_app (tag text, note text, created_on int8 default 1)`;

    const jobStates = (tag: string) =>
        Effect.promise(() =>
            housekeeping.executeSql(`select name, state from pgboss.job where data->>'tag' = $1 order by name`, [tag]),
        ).pipe(Effect.map((r) => r.rows.map((row) => `${row.name}:${row.state}`)));
    const appNotes = (tag: string) =>
        Effect.map(sql<{ note: string }>`select note from synth_app where tag = ${tag} order by note`, (rows) =>
            rows.map((r) => r.note),
        );
    const waitFor = (tag: string, name: string, states: ReadonlyArray<string>) =>
        Effect.gen(function* () {
            for (let i = 0; i < 100; i++) {
                const now = yield* jobStates(tag);
                if (states.some((s) => now.includes(`${name}:${s}`))) return now;
                yield* Effect.sleep("100 millis");
            }
            return yield* Effect.die(new Error(`timed out waiting for ${name} of ${tag}`));
        });

    const txSeen = new Map<string, Connection.Db>();
    const work = <R>(
        queue: string,
        handler: (job: { readonly id: string; readonly data: any }) => Effect.Effect<void, unknown, R>,
    ) =>
        Effect.gen(function* () {
            const context = yield* Effect.context<R>();
            const callback = WorkerTransaction.workCallback({
                transactional: true,
                context,
                handler: (jobs) => Effect.forEach(jobs, handler, { discard: true }),
            });
            yield* Connection.call("work", () =>
                boss.work(
                    queue,
                    { transactional: true, pollingIntervalSeconds: 0.5 },
                    async (jobs: Array<any>, tx: Connection.Db) => {
                        txSeen.set(jobs[0].data.tag, tx);
                        return callback(jobs, tx);
                    },
                ),
            );
        });
    const send = (name: string, data: object) =>
        Connection.withDb(sql, (db) => Connection.call("send", () => boss.send(name, data, { db })));

    // ---------------------------------------------------------------------------
    console.log("\n== P1 handler as child of the parked fiber: commit and failure paths");
    const p1Events: Array<string> = [];
    yield* work("p1", (job) =>
        Effect.gen(function* () {
            const tag: string = job.data.tag;
            yield* sql`insert into synth_app ${sql.insert({ tag, note: "outer" })}`;
            yield* sql.withTransaction(sql`insert into synth_app ${sql.insert({ tag, note: "nested-savepoint" })}`);
            yield* sql
                .withTransaction(
                    Effect.andThen(
                        sql`insert into synth_app ${sql.insert({ tag, note: "nested-rolled-back" })}`,
                        Effect.fail("x"),
                    ),
                )
                .pipe(Effect.ignore);
            const handlerDb = yield* Connection.currentDb(sql);
            p1Events.push(`${tag}: currentDb === pg-boss tx: ${handlerDb === txSeen.get(tag)}`);
            const followId = yield* send("p1-follow", { tag });
            p1Events.push(`${tag}: send inside the worker transaction returned an id: ${typeof followId === "string"}`);
            yield* sql.afterCommit(
                Effect.map(jobStates(tag), (states) => p1Events.push(`${tag}: afterCommit sees ${states.join(", ")}`)),
            );
            if (job.data.fail) return yield* Effect.fail(new Error("handler failed on purpose"));
        }),
    );
    yield* Connection.call("send", () => boss.send("p1", { tag: "p1-ok", fail: false }));
    yield* Connection.call("send", () => boss.send("p1", { tag: "p1-bad", fail: true }));
    yield* waitFor("p1-ok", "p1", ["completed"]);
    yield* waitFor("p1-bad", "p1", ["failed"]);
    yield* Effect.sleep("300 millis");
    for (const tag of ["p1-ok", "p1-bad"]) {
        log(`${tag}: app rows=${json(yield* appNotes(tag))} jobs=${json(yield* jobStates(tag))}`);
    }
    for (const e of p1Events) log(e);
    const settledTx = txSeen.get("p1-ok")!;
    const afterSettle = yield* Effect.promise(() =>
        settledTx.executeSql("select 1", []).then(
            () => "ran",
            (e: Error) => `rejected: ${e.message}`,
        ),
    );
    log(`released guard, statement on p1-ok's tx after commit: ${afterSettle}`);
    const badFail = yield* Effect.promise(() =>
        housekeeping.executeSql(`select output from pgboss.job where name = 'p1' and data->>'tag' = 'p1-bad'`, []),
    );
    log(`p1-bad stored error starts: ${json(String(badFail.rows[0]?.output?.message ?? "").split("\n")[0])}`);

    // ---------------------------------------------------------------------------
    console.log("\n== P2 abandon: handler still in pg_sleep when pg-boss gives up (expireInSeconds 2)");
    let p2Pid: number | undefined;
    yield* work("p2", (job) =>
        Effect.gen(function* () {
            const [{ pid }] = yield* sql<{ pid: number }>`select pg_backend_pid() as pid`;
            p2Pid = pid;
            log(`p2 handler started on backend ${pid}, running pg_sleep(10)`);
            yield* sql`select pg_sleep(10)`;
            yield* sql`insert into synth_app ${sql.insert({ tag: job.data.tag, note: "after-sleep" })}`;
            log("p2 handler finished pg_sleep (should not happen)");
        }).pipe(Effect.onInterrupt(() => Effect.sync(() => log("p2 handler interrupted")))),
    );
    yield* Connection.call("send", () => boss.send("p2", { tag: "p2-abandon" }));
    yield* waitFor("p2-abandon", "p2", ["failed"]);
    yield* Effect.sleep("500 millis");
    log(`p2 app rows=${json(yield* appNotes("p2-abandon"))} jobs=${json(yield* jobStates("p2-abandon"))}`);
    const activity = yield* Effect.promise(() =>
        housekeeping.executeSql(`select pid, state, query from pg_stat_activity where pid = $1`, [p2Pid]),
    );
    log(`p2 backend ${p2Pid} now: ${json(activity.rows)}`);
    const pids = yield* Effect.all(
        Array.from({ length: 6 }, () => sql.withTransaction(sql<{ pid: number }>`select pg_backend_pid() as pid`)),
        { concurrency: "unbounded" },
    );
    log(`pool after P2, 6 concurrent transactions ok on backends ${json(pids.map(([r]) => r.pid))}`);
    yield* Effect.sleep("300 millis");
    const serverLog = execSync("docker logs synth-pg 2>&1")
        .toString()
        .split("\n")
        .filter((line) => line.startsWith(`[${p2Pid}] `))
        .filter((line) => !line.includes("pg_backend_pid"))
        .map((line) => line.slice(0, 140));
    const sleepAt = serverLog.findIndex((line) => line.includes("execute") && line.includes("pg_sleep"));
    const beginAt = serverLog.slice(0, sleepAt).findLastIndex((line) => line.includes(": BEGIN"));
    log(`server log for backend ${p2Pid} (log_statement=all), from the worker transaction's BEGIN:`);
    for (const line of serverLog.slice(beginAt, sleepAt + 7)) console.log("         ", line);

    // ---------------------------------------------------------------------------
    console.log(
        "\n== P2b bounded rollback: the handler ignores interruption (uninterruptible 4s sleep), rollbackTimeout 1s",
    );
    rollbackTimeout = "1 second";
    yield* work("p2b", (job) =>
        Effect.gen(function* () {
            log("p2b handler started, sleeping 4s uninterruptibly");
            yield* Effect.uninterruptible(Effect.sleep("4 seconds"));
            log("p2b handler woke up");
            yield* sql`insert into synth_app ${sql.insert({ tag: job.data.tag, note: "after-wedge" })}`;
            log("p2b handler inserted after the rollback (must not happen)");
        }).pipe(Effect.onInterrupt(() => Effect.sync(() => log("p2b handler interrupted")))),
    );
    yield* Connection.call("send", () => boss.send("p2b", { tag: "p2b-wedged" }));
    yield* waitFor("p2b-wedged", "p2b", ["failed"]);
    log(`p2b job failed while the handler still sleeps; jobs=${json(yield* jobStates("p2b-wedged"))}`);
    yield* Effect.sleep("4 seconds");
    log(`p2b app rows after the handler woke up=${json(yield* appNotes("p2b-wedged"))}`);
    rollbackTimeout = "5 seconds";

    // ---------------------------------------------------------------------------
    console.log("\n== P3 + P4 committed signal: an app afterCommit sleeping 3s does not delay commit()");
    const p3Events: Array<string> = [];
    commitResolvedAt = undefined;
    settleTimes.length = 0;
    yield* work("p3", (job) =>
        Effect.gen(function* () {
            yield* sql`insert into synth_app ${sql.insert({ tag: job.data.tag, note: "p3" })}`;
            yield* sql.afterCommit(
                Effect.gen(function* () {
                    const start = Date.now();
                    yield* Effect.sleep("50 millis");
                    p3Events.push(
                        `app afterCommit #1: commit() already resolved when it ran: ${commitResolvedAt !== undefined && commitResolvedAt <= start + 50}`,
                    );
                    yield* Effect.sleep("3 seconds");
                    p3Events.push(
                        `app afterCommit #1 finished ${Date.now() - commitResolvedAt!}ms after commit() resolved`,
                    );
                }),
            );
            yield* sql.withTransaction(
                sql.afterCommit(Effect.sync(() => p3Events.push("app afterCommit #2 (from a savepoint) ran after #1"))),
            );
        }),
    );
    yield* Connection.call("send", () => boss.send("p3", { tag: "p3-commit" }));
    yield* waitFor("p3-commit", "p3", ["completed"]);
    log(`p3 job completed (seen from another connection); ${settleTimes.join("; ")}`);
    yield* Effect.sleep("3500 millis");
    for (const e of p3Events) log(e);

    // ---------------------------------------------------------------------------
    console.log("\n== P5 executeUnprepared leaves pg-boss rows untransformed; send and flow in a transaction");
    const viaSql = yield* sql<Record<string, unknown>>`select 1 as created_on`;
    const viaUnsafe = yield* sql.unsafe<Record<string, unknown>>("select 1 as created_on");
    const viaDb = yield* sql.withTransaction(
        Connection.withDb(sql, (db) => Effect.promise(() => db.executeSql("select 1 as created_on", []))),
    );
    log(
        `PgClient transformResultNames=camelCase: sql=${json(viaSql)} sql.unsafe=${json(viaUnsafe)} Db.executeSql=${json(viaDb.rows)}`,
    );
    const sentInTx = yield* sql.withTransaction(
        Effect.gen(function* () {
            yield* sql`insert into synth_app ${sql.insert({ tag: "p5-send", note: "app" })}`;
            return yield* send("p1-follow", { tag: "p5-send" });
        }),
    );
    log(
        `send inside sql.withTransaction returned ${json(sentInTx)}; rows=${json(yield* appNotes("p5-send"))} jobs=${json(yield* jobStates("p5-send"))}`,
    );
    const flowIn = (tag: string, fail: boolean) =>
        sql
            .withTransaction(
                Effect.gen(function* () {
                    yield* sql`insert into synth_app ${sql.insert({ tag, note: "app" })}`;
                    const ids = yield* Connection.withDb(sql, (db) =>
                        Connection.call("flow", () =>
                            boss.flow(
                                [
                                    { ref: "a", name: "flow-a", data: { tag, note: "x;\ny" } },
                                    { ref: "b", name: "flow-b", data: { tag }, dependsOn: ["a"] },
                                ],
                                { db },
                            ),
                        ),
                    );
                    if (fail) return yield* Effect.fail("app rollback");
                    return ids;
                }),
            )
            .pipe(Effect.exit);
    const flowOk = yield* flowIn("p5-flow-ok", false);
    yield* flowIn("p5-flow-bad", true);
    const flowBare = yield* Connection.withDb(sql, (db) =>
        Connection.call("flow", () =>
            boss.flow(
                [
                    { ref: "a", name: "flow-a", data: { tag: "p5-flow-bare" } },
                    { ref: "b", name: "flow-b", data: { tag: "p5-flow-bare" }, dependsOn: ["a"] },
                ],
                { db },
            ),
        ),
    );
    log(
        `flow in tx: ${Exit.isSuccess(flowOk) ? "ids " + Object.keys(flowOk.value).join(",") : "failed"}; rows=${json(yield* appNotes("p5-flow-ok"))} jobs=${json(yield* jobStates("p5-flow-ok"))}`,
    );
    log(
        `flow in tx that rolls back: rows=${json(yield* appNotes("p5-flow-bad"))} jobs=${json(yield* jobStates("p5-flow-bad"))}`,
    );
    log(
        `flow outside a tx (withDb opens one): ids ${Object.keys(flowBare).join(",")}; jobs=${json(yield* jobStates("p5-flow-bare"))}`,
    );
    yield* sql`create table synth_unique (id int primary key)`;
    yield* sql`insert into synth_unique values (1)`;
    let seenByPgBoss: unknown;
    const dup = yield* sql
        .withTransaction(
            Connection.withDb(sql, (db) =>
                Connection.call("executeSql", () =>
                    db.executeSql("insert into synth_unique values ($1)", [1]).catch((e) => {
                        seenByPgBoss = e;
                        throw e;
                    }),
                ),
            ),
        )
        .pipe(Effect.flip);
    const driver = seenByPgBoss as { code?: string; constraint?: string; constructor: { name: string } };
    log(
        `unique violation through a Db: pg-boss sees ${driver.constructor.name} code=${driver.code} constraint=${driver.constraint}; the Effect fails with ${dup._tag}/${dup._tag === "SqlError" ? dup.reason._tag : "-"}`,
    );

    // ---------------------------------------------------------------------------
    console.log("\n== Pool headroom budget (maxConnections 6, reserve 2)");
    const reserve = connectionBudget(6 - 2);
    const first = yield* Effect.exit(reserve("a", 3));
    const second = yield* Effect.exit(reserve("b", 2));
    log(
        `reserve 3: ${first._tag}; reserve 2 more: ${second._tag} ${Exit.isFailure(second) ? json(String((second.cause as any).reasons?.[0]?.error?.cause?.message)) : ""}`,
    );
});

const SqlLive = PgClient.layer({
    url: Redacted.make(URL),
    maxConnections: 6,
    transformResultNames: (s) => s.replace(/_(\w)/g, (_, c: string) => c.toUpperCase()),
});

await Effect.runPromise(
    Effect.scoped(program).pipe(
        Effect.provide(SqlLive),
        Effect.catchCause((cause) => Effect.sync(() => console.log("FAILED", cause))),
    ) as Effect.Effect<void>,
);
log("scope closed: pg-boss stopped, worker fibers interrupted, maintenance pool ended");
