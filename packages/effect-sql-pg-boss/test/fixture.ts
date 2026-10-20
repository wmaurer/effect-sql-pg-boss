/**
 * The wiring `PgBoss.layer` will own, assembled from the internal modules the
 * way the design prototype ran them: the maintenance pool, a FiberSet runtime,
 * and `WorkerTransaction.begin` as pg-boss's `beginTransaction`, around a real
 * pg-boss over a database of the test file's own.
 */
import { PgClient } from "@effect/sql-pg";
import {
    Array as Arr,
    Clock,
    Context,
    Duration,
    Effect,
    FiberSet,
    Layer,
    Option,
    Record,
    Redacted,
    Ref,
    Schedule,
    Schema,
} from "effect";
import { PgBoss } from "pg-boss";
import { inject } from "vitest";

import * as Connection from "../src/internal/connection.ts";
import * as Housekeeping from "../src/internal/housekeeping.ts";
import * as WorkerTransaction from "../src/internal/workerTransaction.ts";
import { docker } from "./docker.ts";

import type * as Boss from "pg-boss";

/** What every test job carries. `tag` names the job's rows in `app` and in `pgboss.job`. */
export const JobData = Schema.Struct({ tag: Schema.String, fail: Schema.optional(Schema.Boolean) });
export type JobData = typeof JobData.Type;

/** A pg-boss job id. */
export const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

/** One `commit()` or `rollback()` pg-boss made on a worker transaction, in epoch milliseconds. */
export interface Settle {
    readonly db: Connection.Db;
    readonly kind: "commit" | "rollback";
    readonly calledAt: number;
    readonly resolvedAt: number;
}

export class Harness extends Context.Service<
    Harness,
    {
        readonly sql: PgClient.PgClient;
        readonly boss: PgBoss;
        readonly housekeeping: Housekeeping.Housekeeping;
        /** The `tx` pg-boss handed each batch, by the tag of the batch's first job. */
        readonly transactions: Ref.Ref<Record<string, Connection.Db>>;
        readonly settles: Ref.Ref<ReadonlyArray<Settle>>;
        /** What the server logged for one backend, when this run started the server. */
        readonly serverLog: Option.Option<(pid: number) => Effect.Effect<ReadonlyArray<string>>>;
    }
>()("test/Harness") {}

export interface Options {
    /** Names the test file's database. */
    readonly database: string;
    readonly queues: ReadonlyArray<readonly [name: string, options: Omit<Boss.Queue, "name">]>;
    readonly rollbackTimeout?: Duration.Input;
}

/** A database for one test file, dropped with its layer. Starts by dropping one a crashed run left. */
const database = (name: string) =>
    Layer.effectDiscard(
        Effect.gen(function* () {
            const sql = yield* PgClient.PgClient;
            const drop = sql.unsafe(`drop database if exists ${name} with (force)`);
            yield* Effect.acquireRelease(Effect.andThen(drop, sql.unsafe(`create database ${name}`)), () =>
                Effect.orDie(drop),
            );
        }),
    ).pipe(Layer.provide(PgClient.layer({ url: Redacted.make(inject("databaseUrl")) })));

const appClient = (name: string) => {
    const url = new URL(inject("databaseUrl"));
    url.pathname = `/${name}`;
    return PgClient.layer({
        url: Redacted.make(url.toString()),
        maxConnections: 6,
        transformResultNames: (column) => column.replace(/_(\w)/g, (_, letter: string) => letter.toUpperCase()),
    });
};

const serverLog = (container: string) => (pid: number) =>
    Effect.map(docker("logs", container), (logs) =>
        Arr.filter(logs.split("\n"), (line) => line.startsWith(`[${pid}] `)),
    );

const harness = (options: Options) =>
    Layer.effect(
        Harness,
        Effect.gen(function* () {
            const sql = yield* PgClient.PgClient;
            const housekeeping = yield* Housekeeping.make(sql.config, 3);
            const fork = yield* FiberSet.makeRuntime<never>();
            const runPromise = yield* FiberSet.makeRuntimePromise<never>();
            const base = yield* Effect.context<never>();
            const rollbackTimeout = options.rollbackTimeout ?? "5 seconds";
            const settles = yield* Ref.make<ReadonlyArray<Settle>>([]);

            const settle = (db: Connection.Db, kind: Settle["kind"], run: () => Promise<void>) =>
                runPromise(
                    Effect.gen(function* () {
                        const calledAt = yield* Clock.currentTimeMillis;
                        yield* Effect.promise(run);
                        const resolvedAt = yield* Clock.currentTimeMillis;
                        yield* Ref.update(settles, Arr.append({ db, kind, calledAt, resolvedAt }));
                    }),
                );

            const boss = new PgBoss({
                schema: "pgboss",
                db: {
                    executeSql: housekeeping.executeSql,
                    beginTransaction: () =>
                        WorkerTransaction.begin({ sql, fork, base, rollbackTimeout }).then((handle) => ({
                            db: handle.db,
                            commit: () => settle(handle.db, "commit", () => handle.commit()),
                            rollback: () => settle(handle.db, "rollback", () => handle.rollback()),
                        })),
                },
            });
            boss.on("error", (error) => fork(Effect.logWarning("pg-boss", error)));
            yield* Effect.acquireRelease(
                Connection.call("start", () => boss.start()),
                () => Effect.promise(() => boss.stop({ graceful: true, timeout: 5000, close: false })),
            );
            for (const [name, queue] of options.queues) {
                yield* Connection.call("createQueue", () => boss.createQueue(name, queue));
            }
            yield* sql`create table app (tag text, note text, created_on int8 default 1)`;

            const container = inject("postgresContainer");
            return Harness.of({
                sql,
                boss,
                housekeeping,
                transactions: yield* Ref.make<Record<string, Connection.Db>>({}),
                settles,
                serverLog: container === undefined ? Option.none() : Option.some(serverLog(container)),
            });
        }),
    );

/** A pg-boss started on the app's `PgClient`, over a fresh database. Its scope stops pg-boss. */
export const layer = (options: Options) =>
    harness(options).pipe(Layer.provideMerge(appClient(options.database)), Layer.provide(database(options.database)));

/**
 * Works `queue` with a transactional worker, as `JobQueue.worker` will: the
 * handler runs through `WorkerTransaction.workCallback`, once per job.
 */
export const work = <E, R>(queue: string, handler: (job: JobData) => Effect.Effect<void, E, R>) =>
    Effect.gen(function* () {
        const { boss, transactions } = yield* Harness;
        const context = yield* Effect.context<R>();
        const decode = Schema.decodeUnknownEffect(JobData);
        const id = yield* Connection.call("work", () =>
            boss.work(queue, { transactional: true, pollingIntervalSeconds: 0.5 }, (jobs, tx) =>
                WorkerTransaction.workCallback({
                    transactional: true,
                    context,
                    handler: (batch) =>
                        Effect.forEach(
                            batch,
                            (job, index) =>
                                Effect.flatMap(decode(job.data), (data) =>
                                    index === 0
                                        ? Effect.andThen(
                                              Ref.update(transactions, Record.set(data.tag, tx)),
                                              handler(data),
                                          )
                                        : handler(data),
                                ),
                            { discard: true },
                        ),
                })(jobs, tx),
            ),
        );
        yield* Effect.addFinalizer(() => Effect.promise(() => boss.offWork(queue, { id, wait: true })));
    });

/** The `tx` pg-boss handed the batch whose first job has this tag. */
export const transactionOf = (tag: string) =>
    Effect.gen(function* () {
        const { transactions } = yield* Harness;
        return Record.get(yield* Ref.get(transactions), tag);
    });

/** Polls until pg-boss's settle of the batch with this tag has resolved, and returns its settles. */
export const settled = (tag: string) =>
    Effect.gen(function* () {
        const { settles } = yield* Harness;
        const tx = yield* transactionOf(tag);
        return yield* Effect.map(Ref.get(settles), (all) =>
            Arr.filter(all, (settle) => Option.contains(tx, settle.db)),
        ).pipe(
            Effect.repeat({ schedule: Schedule.spaced("50 millis"), until: (mine) => mine.length > 0 }),
            Effect.timeoutOrElse({
                duration: "5 seconds",
                orElse: () => Effect.die(new Error(`timed out waiting for the settle of ${tag}`)),
            }),
        );
    });

/** Sends a job on the fiber's transaction, as `Queue.send` will. */
export const send = (queue: string, data: JobData) =>
    Effect.gen(function* () {
        const { boss, sql } = yield* Harness;
        return yield* Connection.withDb(sql, (db) => Connection.call("send", () => boss.send(queue, data, { db })));
    });

/** Sends a job on pg-boss's own connections, as a producer outside the app would. */
export const sendOutside = (queue: string, data: JobData) =>
    Effect.gen(function* () {
        const { boss } = yield* Harness;
        return yield* Connection.call("send", () => boss.send(queue, data));
    });

/** `name:state` for each job with this tag, read on the maintenance pool. */
export const jobStates = (tag: string) =>
    Effect.gen(function* () {
        const { housekeeping } = yield* Harness;
        const result = yield* Effect.promise(() =>
            housekeeping.executeSql("select name, state from pgboss.job where data->>'tag' = $1 order by name", [tag]),
        );
        return Arr.map(result.rows, (row) => `${row.name}:${row.state}`);
    });

/** The notes of the app rows with this tag, read through the app's `PgClient`. */
export const appNotes = (tag: string) =>
    Effect.gen(function* () {
        const { sql } = yield* Harness;
        const rows = yield* sql<{ readonly note: string }>`select note from app where tag = ${tag} order by note`;
        return Arr.map(rows, (row) => row.note);
    });

/** Polls until the job `name` with this tag reaches `state`. */
export const waitFor = (tag: string, name: string, state: string) =>
    jobStates(tag).pipe(
        Effect.repeat({
            schedule: Schedule.spaced("100 millis"),
            until: (states) => Arr.contains(states, `${name}:${state}`),
        }),
        Effect.timeoutOrElse({
            duration: "15 seconds",
            orElse: () => Effect.die(new Error(`timed out waiting for ${name} of ${tag} to be ${state}`)),
        }),
    );

/** When a handler reached each named point, read on the live clock. */
export const timeline = Effect.map(Ref.make<Record<string, number>>({}), (marks) => ({
    mark: (event: string) => Effect.flatMap(Clock.currentTimeMillis, (at) => Ref.update(marks, Record.set(event, at))),
    at: (event: string) =>
        Effect.flatMap(Ref.get(marks), (all) =>
            Option.match(Record.get(all, event), {
                onNone: () => Effect.die(new Error(`the handler never reached "${event}"`)),
                onSome: Effect.succeed,
            }),
        ),
    reached: (event: string) => Effect.map(Ref.get(marks), (all) => Record.has(all, event)),
}));
