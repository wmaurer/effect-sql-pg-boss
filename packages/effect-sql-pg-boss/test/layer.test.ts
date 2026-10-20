import { PgClient } from "@effect/sql-pg";
import { assert, layer } from "@effect/vitest";
import { Array as Arr, Clock, Context, Deferred, Effect, Exit, Layer, Logger, MutableRef, Scope } from "effect";

import { PgBoss } from "../src/index.ts";
import * as Connection from "../src/internal/connection.ts";
import { RuntimeId } from "../src/internal/runtime.ts";
import * as WorkerTransaction from "../src/internal/workerTransaction.ts";
import {
    appNotes,
    databaseUrl,
    failure,
    Harness,
    jobStates,
    layer as harness,
    sendOutside,
    timeline,
    UUID,
    waitFor,
    work,
} from "./fixture.ts";

const DATABASE = "pg_boss_layer";

/** A second `PgBoss.layer` on this file's database, over a `PgClient` of its own. */
const bossOn = (client: Omit<PgClient.PgPoolConfig, "url">, options: PgBoss.Options = {}) =>
    PgBoss.layer({ schema: "pgboss", ...options }).pipe(
        Layer.provideMerge(PgClient.layer({ url: databaseUrl(DATABASE), ...client })),
    );

/** The backends a client with this `application_name` holds, read on the harness's own client. */
const backendsOf = (applicationName: string) =>
    Effect.gen(function* () {
        const { sql } = yield* Harness;
        return yield* sql<{
            readonly pid: number;
        }>`select pid from pg_stat_activity where application_name = ${applicationName}`;
    });

layer(
    harness({
        database: DATABASE,
        queues: [
            ["use-send", {}],
            ["commit-fails", { retryLimit: 0 }],
            ["shutdown", { retryLimit: 0 }],
            ["begin-fails", { retryLimit: 0 }],
        ],
    }),
    { excludeTestServices: true, timeout: "60 seconds" },
)("PgBoss.layer", (it) => {
    it.effect("use passes {} outside a transaction and the transaction's db inside one", () =>
        Effect.gen(function* () {
            const { sql } = yield* Harness;
            const connectionOf = PgBoss.use((_, connection) => Promise.resolve(connection));
            assert.deepStrictEqual(yield* connectionOf, {});
            const [inside, transactionDb] = yield* sql.withTransaction(
                Effect.all([connectionOf, Connection.currentDb(sql)]),
            );
            assert.deepStrictEqual(Object.keys(inside), ["db"]);
            assert.strictEqual(inside.db, transactionDb);
        }),
    );

    it.effect("a send through use commits and rolls back with the app's rows", () =>
        Effect.gen(function* () {
            const { sql } = yield* Harness;
            const sendIn = (tag: string, fail: boolean) =>
                sql.withTransaction(
                    Effect.gen(function* () {
                        yield* sql`insert into app ${sql.insert({ tag, note: "app" })}`;
                        const id = yield* PgBoss.use((boss, connection) => boss.send("use-send", { tag }, connection));
                        if (fail) return yield* Effect.fail("app rollback");
                        return id;
                    }),
                );

            assert.match((yield* sendIn("use-ok", false)) ?? "", UUID);
            assert.deepStrictEqual(yield* appNotes("use-ok"), ["app"]);
            assert.deepStrictEqual(yield* jobStates("use-ok"), ["use-send:created"]);

            assert.deepStrictEqual(yield* Effect.exit(sendIn("use-bad", true)), Exit.fail("app rollback"));
            assert.deepStrictEqual(yield* appNotes("use-bad"), []);
            assert.deepStrictEqual(yield* jobStates("use-bad"), []);
        }),
    );

    it.effect("the headroom budget is the PgClient's maxConnections minus connectionReserve", () =>
        Effect.gen(function* () {
            const budgetOf = (client: Omit<PgClient.PgPoolConfig, "url">, options: PgBoss.Options) =>
                Effect.scoped(
                    Effect.gen(function* () {
                        const context = yield* Layer.build(bossOn(client, options));
                        const { reserveConnections } = Context.get(context, PgBoss.PgBoss)[RuntimeId];
                        const exits = yield* Effect.forEach(Arr.range(1, 10), (n) =>
                            Effect.exit(reserveConnections(`w${n}`, 1)),
                        );
                        const taken = Arr.takeWhile(exits, Exit.isSuccess).length;
                        const error = yield* Effect.flip(reserveConnections("last", 1));
                        return { taken, message: error.cause instanceof Error ? error.cause.message : undefined };
                    }),
                );

            assert.deepStrictEqual(yield* budgetOf({ maxConnections: 5 }, { connectionReserve: 2 }), {
                taken: 3,
                message: `transactional worker "last" needs 1 connection(s); 0 of the PgClient pool's 3 left after the reserve`,
            });
            assert.deepStrictEqual(yield* budgetOf({}, {}), {
                taken: 8,
                message: `transactional worker "last" needs 1 connection(s); 0 of the PgClient pool's 8 left after the reserve`,
            });
        }),
    );

    it.effect("a COMMIT that fails rolls back the handler's writes and fails the job with the SQLSTATE", () =>
        Effect.gen(function* () {
            const { sql } = yield* Harness;
            yield* sql`create table deferred_unique (id int unique deferrable initially deferred)`;
            const handler = yield* timeline;
            yield* work("commit-fails", (job) =>
                Effect.gen(function* () {
                    yield* sql`insert into app ${sql.insert({ tag: job.tag, note: "app" })}`;
                    yield* sql`insert into deferred_unique values (1), (1)`;
                    yield* handler.mark("returned");
                }),
            );
            yield* sendOutside("commit-fails", { tag: "commit-fails" });
            yield* waitFor("commit-fails", "commit-fails", "failed");

            assert.isTrue(yield* handler.reached("returned"), "the handler succeeded; only the COMMIT failed");
            assert.deepStrictEqual(yield* appNotes("commit-fails"), []);
            assert.deepStrictEqual(yield* jobStates("commit-fails"), ["commit-fails:failed"]);
            const { output } = yield* failure("commit-fails", "commit-fails");
            assert.strictEqual(output?.code, "23505");
            assert.strictEqual(output?.constraint, "deferred_unique_id_key");
        }),
    );

    it.effect("closing the layer while a transactional handler runs rolls it back and closes every connection", () =>
        Effect.gen(function* () {
            const applicationName = "pg-boss-shutdown-test";
            const scope = yield* Scope.make();
            const context = yield* Layer.buildWithScope(
                bossOn({ applicationName }, { stop: { timeout: 2000 } }),
                scope,
            );
            const sql = Context.get(context, PgClient.PgClient);
            const { boss } = Context.get(context, PgBoss.PgBoss)[RuntimeId];
            const handler = yield* timeline;
            const started = yield* Deferred.make<void>();
            yield* Connection.call("work", () =>
                boss.work(
                    "shutdown",
                    { transactional: true, pollingIntervalSeconds: 0.5 },
                    WorkerTransaction.workCallback({
                        transactional: true,
                        context: Context.empty(),
                        handler: () =>
                            Effect.gen(function* () {
                                yield* sql`insert into app ${sql.insert({ tag: "shutdown", note: "before-stop" })}`;
                                yield* Deferred.succeed(started, undefined);
                                yield* Effect.sleep("60 seconds");
                                yield* handler.mark("woke up");
                            }).pipe(Effect.onInterrupt(() => handler.mark("interrupted"))),
                    }),
                ),
            );
            yield* sendOutside("shutdown", { tag: "shutdown" });
            yield* Deferred.await(started);
            assert.isAbove((yield* backendsOf(applicationName)).length, 1, "both pools hold connections");

            const closing = yield* Clock.currentTimeMillis;
            yield* Scope.close(scope, Exit.void);
            const closed = (yield* Clock.currentTimeMillis) - closing;

            assert.isAtLeast(closed, 2000, "the graceful stop gave the handler its timeout");
            assert.isBelow(closed, 3000, "and closed soon after it");
            assert.isTrue(yield* handler.reached("interrupted"));
            assert.isFalse(yield* handler.reached("woke up"));
            assert.deepStrictEqual(yield* appNotes("shutdown"), []);
            assert.deepStrictEqual(yield* jobStates("shutdown"), ["shutdown:failed"]);
            assert.deepStrictEqual((yield* failure("shutdown", "shutdown")).output, {
                value: "pg-boss shut down while active",
            });
            assert.deepStrictEqual(yield* backendsOf(applicationName), [], "no connection of either pool remains");
        }),
    );

    it.effect("a worker transaction that cannot start fails the job without running the handler", () =>
        Effect.gen(function* () {
            const handler = yield* timeline;
            yield* Effect.scoped(
                Effect.gen(function* () {
                    const context = yield* Layer.build(bossOn({ startupParameters: { statement_timeout: "never" } }));
                    const { boss } = Context.get(context, PgBoss.PgBoss)[RuntimeId];
                    const id = yield* Connection.call("work", () =>
                        boss.work(
                            "begin-fails",
                            { transactional: true, pollingIntervalSeconds: 0.5 },
                            WorkerTransaction.workCallback({
                                transactional: true,
                                context: Context.empty(),
                                handler: () => handler.mark("ran"),
                            }),
                        ),
                    );
                    yield* sendOutside("begin-fails", { tag: "begin-fails" });
                    yield* waitFor("begin-fails", "begin-fails", "failed");
                    yield* Effect.promise(() => boss.offWork("begin-fails", { id, wait: true }));
                }),
            );
            assert.isFalse(yield* handler.reached("ran"));
            const { output } = yield* failure("begin-fails", "begin-fails");
            assert.strictEqual(output?.code, "22023");
        }),
    );

    it.effect("a maintenance connection's error reaches the app's logger", () =>
        Effect.gen(function* () {
            const applicationName = "pg-boss-logger-test";
            const logged = MutableRef.make<ReadonlyArray<ReadonlyArray<unknown>>>([]);
            const capture = Logger.make((options) => {
                MutableRef.update(logged, Arr.append(Arr.ensure(options.message)));
            });
            const { sql } = yield* Harness;
            yield* Effect.scoped(
                Effect.gen(function* () {
                    yield* Layer.build(bossOn({ applicationName })).pipe(Effect.provide(Logger.layer([capture])));
                    yield* sql`select pg_terminate_backend(pid) from pg_stat_activity where application_name = ${applicationName}`;
                    yield* Effect.sleep("500 millis");
                }),
            );
            const warnings = Arr.filter(
                MutableRef.get(logged),
                (message) => message[0] === "pg-boss maintenance connection",
            );
            assert.isAbove(warnings.length, 0);
            for (const [, error] of warnings) {
                assert.propertyVal(error, "code", "57P01");
            }
        }),
    );
});
