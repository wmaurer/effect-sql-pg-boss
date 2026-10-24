/**
 * pg-boss's `db.listen`: the LISTEN/NOTIFY wake-up path behind `useListenNotify`.
 *
 * The listener runs on a connection of its own, opened from the app's
 * `PgClient.config` with `@effect/sql-pg`'s `PgConnection`, the same session
 * type `PgClient.listen` uses underneath. It does not go through
 * `PgClient.listen` because that call hides the session it pins: nothing can
 * run a heartbeat on it, nothing can close its socket before its `UNLISTEN`,
 * and on a `makeClient` client it pins the app's only connection.
 *
 * Mirrors pg-boss's built-in `Db.listen` (pg-boss 12.38 `db.ts:181-336`):
 *
 * - `onReconnect` runs after every successful subscribe, the first included.
 * - A heartbeat asks the listening session itself whether the channel is
 *   still registered (`pg_listening_channels()`). `@effect/sql-pg` turns a
 *   timed-out query on a dead socket into a dead session about 5 s later (its
 *   abort-drain timeout), so a silent drop is found within interval + timeout
 *   + 5 s.
 * - A lost session is logged and replaced after
 *   `min(30 s, 1 s * 2^min(attempt, 5))`; `attempt` resets once a subscribe
 *   succeeds.
 * - A failed first subscribe rejects and retries nothing. pg-boss then warns
 *   `listen_notify_unavailable`, polls, and never closes the handle it did
 *   not get.
 *
 * Not counted against the headroom budget: the connection is outside the
 * PgClient pool, like the maintenance connections.
 *
 * @internal
 */
import * as PgConnection from "@effect/sql-pg/PgConnection";
import * as Cause from "effect/Cause";
import * as Data from "effect/Data";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Scope from "effect/Scope";

import { applicationName } from "./housekeeping.ts";

import type { Db } from "./connection.ts";
import type { PgClientConfig } from "@effect/sql-pg/PgClient";
import type { SqlError } from "effect/sql/SqlError";

/** pg-boss's `IDatabase.listen`, which the layer puts on the `db` it hands pg-boss. */
export type Listen = NonNullable<Db["listen"]>;

/** How often the heartbeat runs, and how long one may take. pg-boss's own listener uses 10 s and 5 s. */
export interface Heartbeat {
    readonly interval: Duration.Input;
    readonly timeout: Duration.Input;
}

/** One pg-boss `listen` call, as its supervisor sees it. */
interface Listener {
    readonly config: PgClientConfig;
    readonly heartbeat: Heartbeat;
    readonly channel: string;
    /** `manager.notifyQueue(payload)`: wakes that queue's workers. */
    readonly onNotification: (payload: string) => void;
    /** `manager.forceFetchLnWorkers()`: every notify-queue worker fetches once, recovering what the gap missed. */
    readonly onReconnect: () => void;
    /** Succeeds once PostgreSQL confirms the first LISTEN. */
    readonly subscribed: Deferred.Deferred<void>;
}

/** One established subscription. It lives in one session scope, which closes the socket first. */
interface Subscription {
    /** The pinned view of the dedicated session: the heartbeat runs here, beside the LISTEN. */
    readonly session: PgConnection.PgConnection;
    /** Fails with the session's `SqlError` when the connection dies. */
    readonly notifications: Queue.Dequeue<PgConnection.Notification, SqlError>;
}

/** The heartbeat found the session alive but no longer listening, or got no answer in time. */
class ListenerLost extends Data.TaggedError("ListenerLost")<{ readonly reason: string }> {}

/** pg-boss's own heartbeat statement, run on the listening session. */
const HEARTBEAT_SQL = `SELECT EXISTS (
  SELECT 1 FROM pg_listening_channels() AS active(channel) WHERE channel = $1
) AS listening`;

/** Delay before reconnect attempt `attempt` (0 right after a loss): pg-boss's formula. */
export const reconnectDelay = (attempt: number): Duration.Duration =>
    Duration.millis(Math.min(30_000, 1000 * 2 ** Math.min(attempt, 5)));

/**
 * Builds pg-boss's `listen` for this layer. Opens nothing: pg-boss calls the
 * result only when `useListenNotify` is on (pg-boss 12.38 `index.ts:272`), so
 * an app that does not opt in sees no connection and no statement.
 *
 * Each call forks one supervisor into the layer's scope. pg-boss's `close()`
 * interrupts it. pg-boss calls `listen` inside `boss.start()`, so the scope's
 * backstop is registered before `boss.stop()` and runs after it.
 */
export const make = Effect.fnUntraced(function* (
    config: PgClientConfig,
    heartbeat: Heartbeat = { interval: "10 seconds", timeout: "5 seconds" },
): Effect.fn.Return<Listen, never, Scope.Scope> {
    const scope = yield* Scope.Scope;
    // pg-boss calls `listen` and `close` outside any fiber; the layer's context carries the app's logger.
    const run = Effect.runPromiseWith(yield* Effect.context<never>());
    return (channel, onNotification, onReconnect) =>
        run(
            Effect.gen(function* () {
                const subscribed = yield* Deferred.make<void>();
                const supervisor = yield* Effect.forkIn(
                    supervise({ config, heartbeat, channel, onNotification, onReconnect, subscribed }),
                    scope,
                );
                // The supervisor ends only by failing its first subscribe or by interruption, which
                // can come before it starts. Either rejects `listen`.
                yield* Effect.raceFirst(Deferred.await(subscribed), Fiber.join(supervisor));
                return { close: () => run(Fiber.interrupt(supervisor)) };
            }),
        );
});

/**
 * Owns one listener for its whole life: first subscribe, watch, and every
 * reconnect. Once the first subscribe succeeded, only interruption ends it.
 *
 * States: subscribing (first) -> watching -> backing off(attempt) ->
 * subscribing -> watching ... A failed first subscribe fails the supervisor.
 */
const supervise = Effect.fnUntraced(function* (listener: Listener): Effect.fn.Return<never, SqlError | ListenerLost> {
    let established = false;
    let attempt = 0;
    while (true) {
        // Interrupting the supervisor (`close()`, or the layer closing) ends it here and never
        // resumes the loop, so an intentional close is not logged as a loss.
        const cause = yield* Effect.scoped(
            Effect.gen(function* () {
                const subscription = yield* subscribe(listener.config, listener.channel);
                if (!established) {
                    established = true;
                    yield* Deferred.succeed(listener.subscribed, undefined);
                }
                attempt = 0;
                yield* callback(listener.onReconnect);
                return yield* watch(subscription, listener);
            }),
        ).pipe(Effect.catchCause(Effect.succeed));
        if (!established) return yield* Effect.failCause(cause);
        yield* Effect.logWarning("pg-boss listener", Cause.squash(cause));
        yield* Effect.sleep(reconnectDelay(attempt++));
    }
});

/**
 * Opens the dedicated session, pins it, and LISTENs on `channel`. Resolves
 * once PostgreSQL confirms the LISTEN.
 *
 * Invariant: when the enclosing scope closes, the socket closes before
 * listen's finalizer runs. `closeUnsafe` marks the session dead, so that
 * finalizer skips `UNLISTEN`; written to a silently dead socket, the
 * `UNLISTEN` would wait for the kernel's TCP timeout (minutes) and hang
 * `boss.stop()` with it.
 *
 * The connection gets the app's config with this library's
 * `applicationName` default, as the maintenance connections do. `onAcquire`
 * is not run: the session runs no app statements.
 */
const subscribe = Effect.fnUntraced(function* (
    config: PgClientConfig,
    channel: string,
): Effect.fn.Return<Subscription, SqlError, Scope.Scope> {
    // Forked before the connection opens, so it closes after the connection: see the invariant.
    const listenScope = yield* Scope.fork(yield* Scope.Scope);
    const connection = yield* PgConnection.make({
        ...config,
        applicationName: config.applicationName ?? applicationName,
    });
    // Listen through the pinned view: it is reentrant, so the heartbeat shares the session with
    // the LISTEN instead of queueing behind its pin.
    const session = yield* Scope.provide(connection.pin, listenScope);
    const notifications = yield* Scope.provide(session.listen(channel), listenScope);
    return { session, notifications };
});

/**
 * Forwards notifications and runs the heartbeat until the session is lost.
 * Never succeeds: it fails with the session's `SqlError` (connection error,
 * `pg_terminate_backend`, the abort-drain timeout after a heartbeat timed
 * out) or with `ListenerLost`.
 *
 * Every payload goes to pg-boss unfiltered: the channel carries queue names
 * only, and nothing else in this design sends on it.
 */
const watch = (subscription: Subscription, listener: Listener): Effect.Effect<never, SqlError | ListenerLost> =>
    Effect.raceFirst(
        Effect.forever(
            Effect.flatMap(Queue.take(subscription.notifications), ({ payload }) =>
                callback(() => listener.onNotification(payload)),
            ),
        ),
        Effect.forever(
            Effect.andThen(
                Effect.sleep(listener.heartbeat.interval),
                beat(subscription.session, listener.channel, listener.heartbeat.timeout),
            ),
        ),
    );

/** One heartbeat on the listening session. */
const beat = (
    session: PgConnection.PgConnection,
    channel: string,
    timeout: Duration.Input,
): Effect.Effect<void, SqlError | ListenerLost> =>
    session.query(HEARTBEAT_SQL, [channel]).pipe(
        Effect.timeoutOrElse({
            duration: timeout,
            orElse: () => Effect.fail(new ListenerLost({ reason: "LISTEN/NOTIFY heartbeat timed out" })),
        }),
        Effect.flatMap((result) =>
            result.rows[0]?.["listening"] === true
                ? Effect.void
                : Effect.fail(new ListenerLost({ reason: "LISTEN/NOTIFY channel registration was lost" })),
        ),
    );

/** pg-boss's callbacks run its manager synchronously. One that throws must not cost the subscription. */
const callback = (f: () => void): Effect.Effect<void> =>
    Effect.try(f).pipe(Effect.catch((error) => Effect.logWarning("pg-boss listener callback", error)));
