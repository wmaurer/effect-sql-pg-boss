/**
 * `PgBoss.layer` over a `PgClient` on a database of the test file's own, with
 * the test file's queue classes on top. Tests reach pg-boss and the internals
 * only through the layer's runtime.
 */
import { PgClient } from "@effect/sql-pg";
import {
    Array as Arr,
    Clock,
    Context,
    Effect,
    Layer,
    Logger,
    MutableRef,
    Option,
    Record,
    Redacted,
    Ref,
    Schedule,
    Schema,
} from "effect";
import { inject } from "vitest";

import { PgBoss } from "../src/index.ts";
import * as Connection from "../src/internal/connection.ts";
import { RuntimeId } from "../src/internal/runtime.ts";
import { docker } from "./docker.ts";

import type { Duration } from "effect";
import type * as Boss from "pg-boss";

/** What every test job carries. `tag` names the job's rows in `app` and in `pgboss.job`. */
export const JobData = Schema.Struct({
    tag: Schema.String,
    fail: Schema.optional(Schema.Boolean),
    note: Schema.optional(Schema.String),
});
export type JobData = typeof JobData.Type;

/** A pg-boss job id. */
export const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

export class Harness extends Context.Service<
    Harness,
    {
        readonly sql: PgClient.PgClient;
        readonly boss: Boss.PgBoss;
        /** The `tx` pg-boss handed each batch, by the tag of the batch's first job. */
        readonly transactions: Ref.Ref<Record<string, Connection.Db>>;
        /** When pg-boss called each batch's `commit()` and when it resolved, by the batch's tag. */
        readonly commits: Ref.Ref<Record<string, Commit>>;
        /** What the server logged for one backend, when this run started the server. */
        readonly serverLog: Option.Option<(pid: number) => Effect.Effect<ReadonlyArray<string>>>;
    }
>()("test/Harness") {}

/** Epoch milliseconds on the live clock. */
export interface Commit {
    readonly calledAt: number;
    readonly resolvedAt: number;
}

export interface Options {
    /** Names the test file's database. */
    readonly database: string;
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

/** The URL of the test file's database. */
export const databaseUrl = (name: string) => {
    const url = new URL(inject("databaseUrl"));
    url.pathname = `/${name}`;
    return Redacted.make(url.toString());
};

const appClient = (name: string) =>
    PgClient.layer({
        url: databaseUrl(name),
        maxConnections: 6,
        transformResultNames: (column) => column.replace(/_(\w)/g, (_, letter: string) => letter.toUpperCase()),
    });

const serverLog = (container: string) => (pid: number) =>
    Effect.map(docker("logs", container), (logs) =>
        Arr.filter(logs.split("\n"), (line) => line.startsWith(`[${pid}] `)),
    );

/**
 * Wraps `boss.work` so each transactional batch's `tx` is recorded under the
 * tag of its first job. The handler still gets the same `tx`, unchanged.
 */
const recordTransactions = (boss: Boss.PgBoss, transactions: Ref.Ref<Record<string, Connection.Db>>) => {
    const work = boss.work.bind(boss);
    const decode = Schema.decodeUnknownOption(JobData);
    type Handler = (jobs: Array<Boss.Job<unknown>>, tx?: Connection.Db) => Promise<void>;
    // SAFETY: the wrapper accepts the three-argument overload every worker in this suite uses, and
    // passes each argument through; only the handler is wrapped, with the same parameters.
    boss.work = ((name: string, options: Boss.WorkOptions, handler: Handler) =>
        work(name, options, (jobs: Array<Boss.Job<unknown>>, tx?: Connection.Db) => {
            const data = decode(jobs[0]?.data);
            if (Option.isSome(data) && tx !== undefined) {
                Effect.runSync(Ref.update(transactions, Record.set(data.value.tag, tx)));
            }
            return handler(jobs, tx);
        })) as typeof boss.work;
};

/**
 * Wraps the db pg-boss was given so each worker transaction's `commit()`
 * resolution is recorded under the tag {@link recordTransactions} recorded
 * for its `tx`. pg-boss still gets the library's handle, with the same `db`.
 */
const recordCommits = (
    boss: Boss.PgBoss,
    transactions: Ref.Ref<Record<string, Connection.Db>>,
    commits: Ref.Ref<Record<string, Commit>>,
) => {
    const db = boss.getDb();
    const begin = db.beginTransaction?.bind(db);
    if (begin === undefined) return;
    const record = (tx: Connection.Db, calledAt: number) =>
        Effect.gen(function* () {
            const resolvedAt = yield* Clock.currentTimeMillis;
            const tag = Arr.findFirst(
                Record.toEntries(yield* Ref.get(transactions)),
                ([, recorded]) => recorded === tx,
            );
            if (Option.isSome(tag)) {
                yield* Ref.update(commits, Record.set(tag.value[0], { calledAt, resolvedAt }));
            }
        });
    db.beginTransaction = () =>
        begin().then((handle) => ({
            ...handle,
            commit: () => {
                const calledAt = Effect.runSync(Clock.currentTimeMillis);
                return handle.commit().then(() => Effect.runSync(record(handle.db, calledAt)));
            },
        }));
};

const harness = Layer.effect(
    Harness,
    Effect.gen(function* () {
        const sql = yield* PgClient.PgClient;
        const { boss } = (yield* PgBoss.PgBoss)[RuntimeId];
        yield* sql`create table app (tag text, note text, created_on int8 default 1)`;

        const transactions = yield* Ref.make<Record<string, Connection.Db>>({});
        recordTransactions(boss, transactions);
        const commits = yield* Ref.make<Record<string, Commit>>({});
        recordCommits(boss, transactions, commits);
        const container = inject("postgresContainer");
        return Harness.of({
            sql,
            boss,
            transactions,
            commits,
            serverLog: container === undefined ? Option.none() : Option.some(serverLog(container)),
        });
    }),
);

/**
 * `PgBoss.layer` started on the app's `PgClient`, over a fresh database, with
 * `queues` (the test file's queue layers) built on it. Its scope stops pg-boss.
 */
export const layer = <A, E>(options: Options, queues: Layer.Layer<A, E, PgBoss.PgBoss>) =>
    Layer.provideMerge(queues, harness).pipe(
        Layer.provideMerge(
            PgBoss.layer({
                schema: "pgboss",
                maintenanceConnections: 3,
                rollbackTimeout: options.rollbackTimeout,
                stop: { timeout: 5000 },
            }),
        ),
        Layer.provideMerge(appClient(options.database)),
        Layer.provide(database(options.database)),
    );

/** The `tx` pg-boss handed the batch whose first job has this tag. */
export const transactionOf = (tag: string) =>
    Effect.gen(function* () {
        const { transactions } = yield* Harness;
        return Record.get(yield* Ref.get(transactions), tag);
    });

/** When pg-boss's `commit()` of the batch whose first job has this tag was called and resolved. */
export const commitOf = (tag: string) =>
    Effect.gen(function* () {
        const { commits } = yield* Harness;
        return Record.get(yield* Ref.get(commits), tag);
    });

/** Sends a job on pg-boss's own connections, as a producer outside the app would. */
export const sendOutside = (queue: string, data: JobData) => PgBoss.use((boss) => boss.send(queue, data));

/** Runs a statement on pg-boss's maintenance connections, apart from the app's pool. */
export const maintenanceQuery = (text: string, values: ReadonlyArray<unknown>) =>
    Effect.gen(function* () {
        const { boss } = yield* Harness;
        const result = yield* Effect.promise(() => boss.getDb().executeSql(text, [...values]));
        return result.rows;
    });

/** `name:state` for each job with this tag, read on the maintenance pool. */
export const jobStates = (tag: string) =>
    Effect.map(
        maintenanceQuery("select name, state from pgboss.job where data->>'tag' = $1 order by name", [tag]),
        Arr.map((row) => `${row.name}:${row.state}`),
    );

/**
 * The error pg-boss stored on the job `name` with this tag, and when it
 * failed the job, in epoch milliseconds. pg-boss fails a transactional batch
 * only after its `rollback()` resolved.
 */
export const failure = (tag: string, name: string) =>
    Effect.map(
        maintenanceQuery(
            "select output, (extract(epoch from completed_on) * 1000)::float8 as failed_at from pgboss.job where name = $1 and data->>'tag' = $2 and state = 'failed'",
            [name, tag],
        ),
        (rows) => ({ output: rows[0]?.output, failedAt: Number(rows[0]?.failed_at) }),
    );

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

/** A logger that keeps each message's arguments. `logged` returns the arguments of one message. */
export const captureLogs = () => {
    const logged = MutableRef.make<ReadonlyArray<ReadonlyArray<unknown>>>([]);
    const logger = Logger.make((options) => {
        MutableRef.update(logged, Arr.append(Arr.ensure(options.message)));
    });
    return {
        layer: Logger.layer([logger]),
        logged: (message: string) => Arr.filter(MutableRef.get(logged), (args) => args[0] === message),
    };
};

/** Runs `effect` every 50 ms until its result passes `until`, for up to 20 seconds. */
export const eventually = <A, E, R>(what: string, effect: Effect.Effect<A, E, R>, until: (a: A) => boolean) =>
    effect.pipe(
        Effect.repeat({ schedule: Schedule.spaced("50 millis"), until }),
        Effect.timeoutOrElse({
            duration: "20 seconds",
            orElse: () => Effect.die(new Error(`timed out waiting for ${what}`)),
        }),
    );

/**
 * The backends of this application whose last statement was the LISTEN/NOTIFY
 * listener's `LISTEN` or its heartbeat, read on the ambient `PgClient`.
 */
export const listenerPids = (applicationName: string) =>
    Effect.gen(function* () {
        const sql = yield* PgClient.PgClient;
        const rows = yield* sql<{ readonly pid: number }>`select pid from pg_stat_activity
            where application_name = ${applicationName}
            and (query like 'LISTEN %' or query like '%pg_listening_channels%') order by pid`;
        return Arr.map(rows, (row) => row.pid);
    });
