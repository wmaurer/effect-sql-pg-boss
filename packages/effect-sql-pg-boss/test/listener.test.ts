/**
 * The listener on its own, driven the way pg-boss drives it, with short
 * heartbeat timings where the defaults would make a test take tens of
 * seconds.
 */
import { PgClient } from "@effect/sql-pg";
import { assert, it, layer } from "@effect/vitest";
import { Array as Arr, Clock, Duration, Effect, Exit, Fiber, MutableRef, Predicate, Redacted, Scope } from "effect";
import { inject } from "vitest";

import * as Listener from "../src/internal/listener.ts";
import { captureLogs, eventually, listenerPids } from "./fixture.ts";
import * as Proxy from "./proxy.ts";

const server = Redacted.make(inject("databaseUrl"));
const CHANNEL = "listener_test";

/** Stands in for pg-boss: records what its two callbacks receive, and what the listener logs. */
const pgBossStandIn = () => {
    const payloads = MutableRef.make<ReadonlyArray<string>>([]);
    const reconnects = MutableRef.make(0);
    return {
        ...captureLogs(),
        onNotification: (payload: string) => MutableRef.update(payloads, Arr.append(payload)),
        onReconnect: () => MutableRef.update(reconnects, (n) => n + 1),
        payloads: () => MutableRef.get(payloads),
        reconnects: () => MutableRef.get(reconnects),
    };
};

it("the reconnect delay doubles from 1 s and stops at 30 s, as pg-boss's own listener", () => {
    assert.deepStrictEqual(
        Arr.map(Arr.range(0, 8), (attempt) => Duration.toMillis(Listener.reconnectDelay(attempt))),
        [1000, 2000, 4000, 8000, 16000, 30000, 30000, 30000, 30000],
    );
});

layer(PgClient.layer({ url: server }), { excludeTestServices: true, timeout: "60 seconds" })("Listener", (it) => {
    it.effect("a silently dropped session is found by the heartbeat and replaced", () =>
        Effect.gen(function* () {
            const sql = yield* PgClient.PgClient;
            const applicationName = "listener-silent-drop";
            const proxy = yield* Proxy.make(server);
            const pgBoss = pgBossStandIn();
            const listen = yield* Listener.make(
                { url: proxy.url, applicationName },
                { interval: "300 millis", timeout: "300 millis" },
            ).pipe(Effect.provide(pgBoss.layer));
            yield* Effect.promise(() => listen(CHANNEL, pgBoss.onNotification, pgBoss.onReconnect));
            const [frozen] = yield* listenerPids(applicationName);
            assert.isDefined(frozen);

            proxy.freezeListeners();
            const frozenAt = yield* Clock.currentTimeMillis;
            yield* eventually("a second subscribe", Effect.sync(pgBoss.reconnects), (n) => n === 2);
            const replacedAfter = (yield* Clock.currentTimeMillis) - frozenAt;

            // 300 ms interval + 300 ms timeout + the driver's 5 s abort drain + the 1 s backoff.
            assert.isBelow(replacedAfter, 9000, `replaced ${replacedAfter} ms after the freeze`);
            const pids = yield* listenerPids(applicationName);
            assert.isTrue(
                Arr.some(pids, (pid) => pid !== frozen),
                "a new backend listens; the frozen one lives on behind the proxy",
            );
            assert.lengthOf(pgBoss.logged("pg-boss listener"), 1);
            yield* sql`select pg_notify(${CHANNEL}, 'after-drop')`;
            yield* eventually("the payload", Effect.sync(pgBoss.payloads), Arr.contains("after-drop"));
        }).pipe(Effect.scoped),
    );

    it.effect("a callback that throws costs neither the subscription nor a reconnect", () =>
        Effect.gen(function* () {
            const sql = yield* PgClient.PgClient;
            const pgBoss = pgBossStandIn();
            const listen = yield* Listener.make({ url: server }).pipe(Effect.provide(pgBoss.layer));
            yield* Effect.promise(() =>
                listen(
                    CHANNEL,
                    (payload) => {
                        pgBoss.onNotification(payload);
                        throw new Error("notifyQueue threw");
                    },
                    () => {
                        pgBoss.onReconnect();
                        throw new Error("forceFetchLnWorkers threw");
                    },
                ),
            );
            yield* sql`select pg_notify(${CHANNEL}, 'one')`;
            yield* sql`select pg_notify(${CHANNEL}, 'two')`;
            yield* eventually("both payloads", Effect.sync(pgBoss.payloads), (all) => all.length === 2);

            assert.deepStrictEqual(pgBoss.payloads(), ["one", "two"]);
            assert.strictEqual(pgBoss.reconnects(), 1);
            assert.lengthOf(pgBoss.logged("pg-boss listener"), 0);
            assert.lengthOf(pgBoss.logged("pg-boss listener callback"), 3);
        }).pipe(Effect.scoped),
    );

    it.effect("a first LISTEN that PostgreSQL never confirms rejects listen and hangs up", () =>
        Effect.gen(function* () {
            const proxy = yield* Proxy.make(server, "freeze");
            const pgBoss = pgBossStandIn();
            const listen = yield* Listener.make({ url: proxy.url }, { interval: "300 millis", timeout: "300 millis" });
            const rejection = yield* Effect.forkChild(
                Effect.flip(
                    Effect.tryPromise({
                        try: () => listen(CHANNEL, pgBoss.onNotification, pgBoss.onReconnect),
                        // pg-boss persists only the rejection's message.
                        catch: (error) => String(Predicate.hasProperty(error, "message") ? error.message : error),
                    }),
                ),
            );
            yield* eventually("the LISTEN to reach the proxy", Effect.sync(proxy.listens), (n) => n === 1);
            const frozenAt = yield* Clock.currentTimeMillis;

            const message = yield* Fiber.join(rejection).pipe(
                Effect.timeoutOrElse({
                    duration: "10 seconds",
                    orElse: () => Effect.die(new Error("listen never settled while its LISTEN went unanswered")),
                }),
            );
            const rejectedAfter = (yield* Clock.currentTimeMillis) - frozenAt;

            // 300 ms timeout + the driver's 5 s abort drain.
            assert.isBelow(rejectedAfter, 6500, `rejected ${rejectedAfter} ms after the freeze`);
            assert.strictEqual(message, "PostgreSQL did not confirm LISTEN in time");
            yield* eventually("the listener to hang up", Effect.sync(proxy.openListeners), (n) => n === 0);
            assert.strictEqual(pgBoss.reconnects(), 0);
        }).pipe(Effect.scoped),
    );

    it.effect("a reconnect whose LISTEN PostgreSQL never confirms is logged and retried", () =>
        Effect.gen(function* () {
            const sql = yield* PgClient.PgClient;
            const applicationName = "listener-unconfirmed-reconnect";
            const proxy = yield* Proxy.make(server);
            const pgBoss = pgBossStandIn();
            const listen = yield* Listener.make(
                { url: proxy.url, applicationName },
                { interval: "300 millis", timeout: "300 millis" },
            ).pipe(Effect.provide(pgBoss.layer));
            yield* Effect.promise(() => listen(CHANNEL, pgBoss.onNotification, pgBoss.onReconnect));
            const [first] = yield* listenerPids(applicationName);
            assert.isDefined(first);

            proxy.setOnListen("freeze");
            yield* sql`select pg_terminate_backend(${first})`;
            const lostAt = yield* Clock.currentTimeMillis;
            yield* eventually("the reconnect's LISTEN to reach the proxy", Effect.sync(proxy.listens), (n) => n === 2);
            proxy.setOnListen("forward");
            yield* eventually("a second subscribe", Effect.sync(pgBoss.reconnects), (n) => n === 2);
            const backAfter = (yield* Clock.currentTimeMillis) - lostAt;

            // 1 s backoff + 300 ms timeout + the driver's 5 s abort drain + 2 s backoff.
            assert.isBelow(backAfter, 10_500, `subscribed again ${backAfter} ms after the loss`);
            const losses = pgBoss.logged("pg-boss listener");
            assert.lengthOf(losses, 2);
            assert.nestedPropertyVal(losses[0]?.[1], "reason.cause.code", "57P01");
            assert.nestedPropertyVal(losses[1]?.[1], "message", "PostgreSQL did not confirm LISTEN in time");
            yield* sql`select pg_notify(${CHANNEL}, 'after-unconfirmed')`;
            yield* eventually("the payload", Effect.sync(pgBoss.payloads), Arr.contains("after-unconfirmed"));
        }).pipe(Effect.scoped),
    );

    it.effect("closing the layer's scope during the first subscribe rejects listen", () =>
        Effect.gen(function* () {
            const proxy = yield* Proxy.make(server, "freeze");
            const scope = yield* Scope.make();
            const pgBoss = pgBossStandIn();
            const listen = yield* Scope.provide(Listener.make({ url: proxy.url }), scope);
            const pending = yield* Effect.forkChild(
                Effect.exit(Effect.tryPromise(() => listen(CHANNEL, pgBoss.onNotification, pgBoss.onReconnect))),
            );
            yield* eventually("the LISTEN to reach the proxy", Effect.sync(proxy.listens), (n) => n === 1);

            yield* Scope.close(scope, Exit.void);
            const settled = yield* Fiber.join(pending).pipe(
                Effect.timeoutOrElse({
                    duration: "10 seconds",
                    orElse: () => Effect.die(new Error("listen never settled after its scope closed")),
                }),
            );

            assert.isTrue(Exit.isFailure(settled));
            assert.strictEqual(pgBoss.reconnects(), 0);
        }).pipe(Effect.scoped),
    );
});
