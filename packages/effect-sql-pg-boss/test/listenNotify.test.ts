/**
 * `useListenNotify` through `PgBoss.layer`. Each test builds a pg-boss of its
 * own on this file's database, over a `PgClient` with its own
 * `application_name`, which the listener inherits, so `listenerPids` finds
 * that test's listener and no other.
 */
import { PgClient } from "@effect/sql-pg";
import { assert, layer } from "@effect/vitest";
import { Array as Arr, Clock, Context, Effect, Exit, Fiber, Layer, Predicate, Scope } from "effect";

import { JobQueue, PgBoss } from "../src/index.ts";
import {
    captureLogs,
    databaseUrl,
    eventually,
    Harness,
    JobData,
    layer as harness,
    listenerPids,
    timeline,
} from "./fixture.ts";
import * as Proxy from "./proxy.ts";

const DATABASE = "pg_boss_listen";

class Wake extends JobQueue.Service<Wake>()("wake", { payload: JobData, options: { notify: true } }) {}

/** A `PgBoss.layer` on this file's database, over a `PgClient` of its own. */
const bossOn = (client: PgClient.PgPoolConfig, options: PgBoss.Options = {}) =>
    PgBoss.layer({ schema: "pgboss", maintenanceConnections: 2, ...options }).pipe(
        Layer.provideMerge(PgClient.layer({ url: databaseUrl(DATABASE), ...client })),
    );

const listening = (client: PgClient.PgPoolConfig, options: PgBoss.Options = {}) =>
    bossOn(client, { useListenNotify: true, ...options });

type Marks = Effect.Success<typeof timeline>;

/** Marks each job's tag when its handler runs. */
const markEach = (marks: Marks) => (jobs: Arr.NonEmptyReadonlyArray<JobQueue.Job<JobData>>) =>
    Effect.forEach(jobs, (job) => marks.mark(job.data.tag), { discard: true });

/** A worker that polls every 30 s, so only a wake-up runs a job within seconds of its send. */
const slowPoller = (marks: Marks) => Wake.worker(markEach(marks), { transactional: false, pollingIntervalSeconds: 30 });

/** Waits past a new worker's first fetch, which finds nothing; its next poll is 30 s away. */
const pastFirstFetch = Effect.sleep("1 second");

/** Sends one job and waits for its handler. Returns how many milliseconds after the send it ran. */
const latencyOf = (context: Context.Context<Wake>, marks: Marks, tag: string) =>
    Effect.gen(function* () {
        const sentAt = yield* Clock.currentTimeMillis;
        yield* Effect.provideContext(Wake.send({ tag }), context);
        yield* eventually(`the handler of ${tag}`, marks.reached(tag), (ran) => ran);
        return (yield* marks.at(tag)) - sentAt;
    });

/** Whether a backend with this pid still exists. */
const alive = (pid: number) =>
    Effect.gen(function* () {
        const { sql } = yield* Harness;
        return (yield* sql`select 1 from pg_stat_activity where pid = ${pid}`).length > 0;
    });

/** Closes `scope` and returns how many milliseconds it took. A close past `limit` fails the test but keeps running. */
const timeClose = (scope: Scope.Closeable, limit: `${number} seconds`) =>
    Effect.gen(function* () {
        const closingAt = yield* Clock.currentTimeMillis;
        const closing = yield* Effect.forkDetach(Scope.close(scope, Exit.void));
        yield* Fiber.await(closing).pipe(
            Effect.timeoutOrElse({
                duration: limit,
                orElse: () => Effect.die(new Error(`closing the layer took longer than ${limit}`)),
            }),
        );
        return (yield* Clock.currentTimeMillis) - closingAt;
    });

const exitsWithinTwoSeconds = (pid: number) =>
    eventually(`backend ${pid} to exit`, alive(pid), (exists) => !exists).pipe(
        Effect.timeoutOrElse({
            duration: "2 seconds",
            orElse: () => Effect.die(new Error(`backend ${pid} still runs 2 s after the close`)),
        }),
    );

layer(harness({ database: DATABASE }, Layer.empty), { excludeTestServices: true, timeout: "60 seconds" })(
    "useListenNotify",
    (it) => {
        it.effect("a notify queue's worker runs a job within 3 s of its send while it polls every 30 s", () =>
            Effect.gen(function* () {
                const app = "listen-latency";
                const marks = yield* timeline;
                const context = yield* Layer.build(
                    slowPoller(marks).pipe(
                        Layer.provideMerge(Layer.fresh(Wake.layer)),
                        Layer.provideMerge(listening({ applicationName: app })),
                    ),
                );
                assert.lengthOf(yield* listenerPids(app), 1);
                yield* pastFirstFetch;

                const latency = yield* latencyOf(context, marks, "latency");
                assert.isBelow(latency, 3000, `ran ${latency} ms after the send`);
            }).pipe(Effect.scoped),
        );

        it.effect(
            "after pg_terminate_backend the listener comes back, and its reconnect makes notify workers fetch",
            () =>
                Effect.gen(function* () {
                    const app = "listen-heal";
                    const { sql } = yield* Harness;
                    const logs = captureLogs();
                    const marks = yield* timeline;
                    const context = yield* Layer.build(
                        slowPoller(marks).pipe(
                            Layer.provideMerge(Layer.fresh(Wake.layer)),
                            Layer.provideMerge(listening({ applicationName: app })),
                        ),
                    ).pipe(Effect.provide(logs.layer));
                    const [old] = yield* listenerPids(app);
                    assert.isDefined(old);
                    yield* pastFirstFetch;

                    yield* sql`select pg_terminate_backend(${old})`;
                    yield* eventually("the old listener to exit", alive(old), (exists) => !exists);
                    assert.deepStrictEqual(yield* listenerPids(app), [], "the reconnect waits out a 1 s backoff");
                    // Its NOTIFY reaches no listener: only the reconnect's forced fetch runs it before the next poll.
                    const gap = yield* latencyOf(context, marks, "heal-gap");
                    assert.isBelow(gap, 5000, `the job sent in the gap ran ${gap} ms after the send`);

                    const pids = yield* listenerPids(app);
                    assert.lengthOf(pids, 1);
                    assert.notStrictEqual(pids[0], old);
                    const after = yield* latencyOf(context, marks, "heal-after");
                    assert.isBelow(after, 3000, `ran ${after} ms after the send`);
                    const losses = logs.logged("pg-boss listener");
                    assert.lengthOf(losses, 1);
                    assert.nestedPropertyVal(losses[0]?.[1], "reason.cause.code", "57P01");
                }).pipe(Effect.scoped),
        );

        it.effect("closing the layer closes the listener's backend and logs no listener warning", () =>
            Effect.gen(function* () {
                const app = "listen-close";
                const logs = captureLogs();
                const scope = yield* Scope.make();
                yield* Layer.buildWithScope(listening({ applicationName: app }), scope).pipe(
                    Effect.provide(logs.layer),
                );
                const [pid] = yield* listenerPids(app);
                assert.isDefined(pid);

                const closed = yield* timeClose(scope, "5 seconds");
                assert.isBelow(closed, 2000, `closed in ${closed} ms`);
                yield* exitsWithinTwoSeconds(pid);
                assert.lengthOf(logs.logged("pg-boss listener"), 0);
            }),
        );

        it.effect("the listener takes no PgClient connection and no headroom", () =>
            Effect.gen(function* () {
                const app = "listen-pool";
                yield* Effect.scoped(
                    Effect.gen(function* () {
                        const context = yield* Layer.build(listening({ applicationName: app, maxConnections: 1 }));
                        assert.lengthOf(yield* listenerPids(app), 1);
                        const sql = Context.get(context, PgClient.PgClient);
                        yield* sql`select 1`.pipe(
                            Effect.timeoutOrElse({
                                duration: "2 seconds",
                                orElse: () => Effect.die(new Error("the PgClient's only connection is taken")),
                            }),
                        );
                    }),
                );

                // 6 - connectionReserve 2 leaves 4, all of them for this worker.
                const marks = yield* timeline;
                const context = yield* Layer.build(
                    Wake.worker(markEach(marks), {
                        transactional: true,
                        localConcurrency: 4,
                        pollingIntervalSeconds: 30,
                    }).pipe(
                        Layer.provideMerge(Layer.fresh(Wake.layer)),
                        Layer.provideMerge(listening({ maxConnections: 6 }, { connectionReserve: 2 })),
                    ),
                );
                yield* pastFirstFetch;
                const latency = yield* latencyOf(context, marks, "headroom");
                assert.isBelow(latency, 3000, `ran ${latency} ms after the send`);
            }).pipe(Effect.scoped),
        );

        it.effect(
            "on a makeClient client the listener leaves the client's connection free and still wakes workers",
            () =>
                Effect.gen(function* () {
                    const app = "listen-make-client";
                    const marks = yield* timeline;
                    const context = yield* Layer.build(
                        slowPoller(marks).pipe(
                            Layer.provideMerge(Layer.fresh(Wake.layer)),
                            Layer.provideMerge(
                                PgBoss.layer({ schema: "pgboss", maintenanceConnections: 2, useListenNotify: true }),
                            ),
                            Layer.provideMerge(
                                PgClient.layerFrom(
                                    PgClient.makeClient({ url: databaseUrl(DATABASE), applicationName: app }),
                                ),
                            ),
                        ),
                    );
                    assert.lengthOf(yield* listenerPids(app), 1);
                    const sql = Context.get(context, PgClient.PgClient);
                    yield* sql`select 1`.pipe(
                        Effect.timeoutOrElse({
                            duration: "2 seconds",
                            orElse: () => Effect.die(new Error("the client's connection is taken")),
                        }),
                    );
                    yield* pastFirstFetch;

                    const latency = yield* latencyOf(context, marks, "make-client");
                    assert.isBelow(latency, 3000, `ran ${latency} ms after the send`);
                }).pipe(Effect.scoped),
        );

        it.effect("without useListenNotify no backend listens and nothing about a listener is logged", () =>
            Effect.gen(function* () {
                const app = "listen-off";
                const logs = captureLogs();
                const marks = yield* timeline;
                const context = yield* Layer.build(
                    Wake.worker(markEach(marks), { transactional: false, pollingIntervalSeconds: 0.5 }).pipe(
                        Layer.provideMerge(Layer.fresh(Wake.layer)),
                        Layer.provideMerge(bossOn({ applicationName: app })),
                    ),
                ).pipe(Effect.provide(logs.layer));
                // The queue has `notify: true`, so the send does emit a NOTIFY; polling runs the job.
                yield* latencyOf(context, marks, "off");
                yield* Effect.sleep("1 second");

                assert.deepStrictEqual(yield* listenerPids(app), []);
                assert.lengthOf(logs.logged("pg-boss listener"), 0);
                assert.isFalse(
                    Arr.some(logs.logged("pg-boss"), ([, message]) => String(message).includes("LISTEN/NOTIFY")),
                    "pg-boss warned about LISTEN/NOTIFY",
                );
            }).pipe(Effect.scoped),
        );

        it.effect("closing the layer while the listener's socket is silently dead returns within 8 s", () =>
            Effect.gen(function* () {
                const app = "listen-frozen-close";
                const proxy = yield* Proxy.make(databaseUrl(DATABASE));
                const scope = yield* Scope.make();
                yield* Layer.buildWithScope(listening({ url: proxy.url, applicationName: app }), scope);
                assert.lengthOf(yield* listenerPids(app), 1);
                proxy.freezeListeners();

                // The worst case is a close that lands on a heartbeat in flight: the driver's 5 s drain.
                const closed = yield* timeClose(scope, "8 seconds");
                assert.isBelow(closed, 8000, `closed in ${closed} ms`);
            }).pipe(Effect.scoped),
        );

        it.effect("a failed first LISTEN logs listen_notify_unavailable, keeps polling, and is not retried", () =>
            Effect.gen(function* () {
                const app = "listen-unavailable";
                const proxy = yield* Proxy.make(databaseUrl(DATABASE), "destroy");
                const logs = captureLogs();
                const marks = yield* timeline;
                const context = yield* Layer.build(
                    Wake.worker(markEach(marks), { transactional: false }).pipe(
                        Layer.provideMerge(Layer.fresh(Wake.layer)),
                        Layer.provideMerge(listening({ url: proxy.url, applicationName: app })),
                    ),
                ).pipe(Effect.provide(logs.layer));
                const unavailable = () =>
                    Arr.filter(
                        logs.logged("pg-boss"),
                        ([, message]) =>
                            message === "Failed to start LISTEN/NOTIFY listener. Continuing with polling only.",
                    );
                yield* eventually("pg-boss's warning", Effect.sync(unavailable), (warnings) => warnings.length === 1);
                const [, , data] = unavailable()[0] ?? [];
                assert.match(
                    Predicate.hasProperty(data, "error") ? String(data.error) : "",
                    /^PgConnection: \S/,
                    "the rejection's message, the only part of it pg-boss persists",
                );

                const latency = yield* latencyOf(context, marks, "unavailable");
                assert.isBelow(latency, 5000, `polling ran it ${latency} ms after the send`);
                yield* Effect.sleep("3 seconds");
                assert.strictEqual(proxy.listens(), 1, "no LISTEN after the 1 s and 2 s backoffs");
                assert.deepStrictEqual(yield* listenerPids(app), []);
            }).pipe(Effect.scoped),
        );
    },
);
