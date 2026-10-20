// Runtime check of the one risky mechanism in this candidate: pg-boss's
// beginTransaction() backed by a fiber parked inside sql.withTransaction, and
// a handler fiber joining it through the context delta.
// Run: docker run -d --name arena-c1-pg -e POSTGRES_PASSWORD=probe -p 127.0.0.1:55501:5432 postgres:17.11
//      node prototype/bridge.ts   (from the candidate-1 dir)
import { PgClient } from "@effect/sql-pg";
import { Context, Deferred, Effect, Exit, Fiber, Layer, Stream } from "effect";
import * as Reactivity from "effect/reactivity/Reactivity";
import { SqlClient } from "effect/sql";
import { SqlError, UnknownError } from "effect/sql/SqlError";
import pg from "pg";
import { PgBoss } from "pg-boss";

const URL = "postgres://postgres:probe@127.0.0.1:55501/postgres";
const pool = new pg.Pool({ connectionString: URL, max: 8 });
const log: Array<string> = [];

class Conn {
    client: pg.PoolClient;
    constructor(client: pg.PoolClient) {
        this.client = client;
    }
    q(sql: string, params: ReadonlyArray<unknown>) {
        return Effect.tryPromise({
            try: () => this.client.query(sql, params.length ? [...params] : undefined),
            catch: (cause) =>
                new SqlError({ reason: new UnknownError({ cause, message: String((cause as any)?.message) }) }),
        });
    }
    execute = (sql: string, params: ReadonlyArray<unknown>, t?: any) =>
        Effect.map(this.q(sql, params), (r) => (t ? t(r.rows) : r.rows));
    executeUnprepared = this.execute;
    executeRaw = (sql: string, params: ReadonlyArray<unknown>) => this.q(sql, params);
    executeValues = (sql: string, params: ReadonlyArray<unknown>) =>
        Effect.map(this.q(sql, params), (r) => r.rows.map((x: any) => Object.values(x)));
    executeValuesUnprepared = this.executeValues;
    executeStream = (sql: string, params: ReadonlyArray<unknown>) =>
        Stream.fromIterableEffect(this.execute(sql, params)) as any;
    // pg-boss Db
    executeSql = (text: string, values?: Array<unknown>) => {
        log.push(`tx: ${text.trim().split(/\s+/).slice(0, 3).join(" ")}`);
        return this.client.query(text, values);
    };
}

const acquirer = Effect.map(
    Effect.acquireRelease(
        Effect.promise(() => pool.connect()),
        (c) => Effect.sync(() => c.release()),
    ),
    (c) => new Conn(c),
);

const contexts = new WeakMap<object, Context.Context<never>>();
const contextDelta = (inner: Context.Context<never>, outer: Context.Context<never>) =>
    Context.makeUnsafe<never>(new Map([...inner.mapUnsafe].filter(([k, v]) => outer.mapUnsafe.get(k) !== v)));

const program = Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const base = yield* Effect.context<never>();
    const runFork = Effect.runForkWith(base);

    const db = {
        executeSql: (text: string, values?: Array<unknown>) => pool.query(text, values),
        beginTransaction: () =>
            new Promise<any>((resolve, reject) => {
                const settlement = Deferred.makeUnsafe<"commit" | "rollback">();
                let begun = false;
                const fiber = runFork(
                    sql.withTransaction(
                        Effect.gen(function* () {
                            const context = yield* Effect.context<never>();
                            const [conn] = Context.getUnsafe(context, sql.transactionService);
                            contexts.set(conn, contextDelta(context, base));
                            begun = true;
                            resolve({
                                db: conn,
                                commit: async () => {
                                    if (!Deferred.doneUnsafe(settlement, Effect.succeed("commit")))
                                        throw new Error("already settled");
                                    const exit = await Effect.runPromise(Fiber.await(fiber));
                                    log.push(`commit: ${exit._tag}`);
                                    if (Exit.isFailure(exit))
                                        throw (
                                            (exit.cause as any).reasons?.[0]?.error?.reason?.cause ??
                                            new Error("commit failed")
                                        );
                                },
                                rollback: async () => {
                                    Deferred.doneUnsafe(settlement, Effect.succeed("rollback"));
                                    const exit = await Effect.runPromise(Fiber.await(fiber));
                                    log.push(`rollback: ${exit._tag}`);
                                },
                            });
                            if ((yield* Deferred.await(settlement)) === "rollback") {
                                return yield* Effect.fail(new Error("rollback requested"));
                            }
                        }),
                    ),
                );
                fiber.addObserver((exit) => {
                    if (!begun) reject(exit);
                });
            }),
    };

    const boss = new PgBoss({ db, schema: "pgboss_c1" } as any);
    boss.on("error", (e: unknown) => log.push(`boss error: ${(e as any)?.message}`));
    yield* Effect.promise(() => boss.start());
    yield* sql`create table if not exists app_row (tag text, note text)`;
    yield* Effect.promise(() => boss.createQueue("work_q"));
    yield* Effect.promise(() => boss.createQueue("next_q"));

    const handlerContext = yield* Effect.context<SqlClient.SqlClient>();
    const runHandler = <A, E>(tx: object, eff: Effect.Effect<A, E, SqlClient.SqlClient>) =>
        Effect.runPromise(Effect.provideContext(eff, Context.merge(handlerContext, contexts.get(tx)!)));

    const done = Deferred.makeUnsafe<void>();
    let seen = 0;
    yield* Effect.promise(() =>
        boss.work("work_q", { transactional: true, pollingIntervalSeconds: 0.5 } as any, async (jobs: any, tx: any) => {
            const job = jobs[0];
            const tag = job.data.tag as string;
            try {
                await runHandler(
                    tx,
                    Effect.gen(function* () {
                        const sql = yield* SqlClient.SqlClient;
                        yield* sql`insert into app_row ${sql.insert({ tag, note: "outer" })}`;
                        // nested: must be a SAVEPOINT on the same connection
                        yield* sql.withTransaction(sql`insert into app_row ${sql.insert({ tag, note: "nested" })}`);
                        // nested that rolls back: only its row disappears
                        yield* sql
                            .withTransaction(
                                Effect.andThen(
                                    sql`insert into app_row ${sql.insert({ tag, note: "nested-rb" })}`,
                                    Effect.fail("x"),
                                ),
                            )
                            .pipe(Effect.ignore);
                        // direction 2 + 1: send on the handler's tx (same object pg-boss gave us)
                        const [level] = Context.getUnsafe(yield* Effect.context<never>(), sql.transactionService);
                        log.push(`handler conn === tx: ${level === tx}`);
                        yield* Effect.promise(() => boss.send("next_q", { tag }, { db: level } as any));
                        yield* sql.afterCommit(
                            Effect.promise(async () => {
                                const r = await pool.query(`select state from pgboss_c1.job where id = $1`, [job.id]);
                                log.push(`afterCommit [${tag}] job state seen from pool: ${r.rows[0]?.state}`);
                            }),
                        );
                        if (job.data.fail) return yield* Effect.fail(new Error("handler failed"));
                    }),
                );
            } finally {
                if (++seen === 2) Deferred.doneUnsafe(done, Effect.void);
            }
        }),
    );

    const t = Date.now();
    yield* Effect.promise(() => boss.send("work_q", { tag: `ok-${t}` }, { retryLimit: 0 }));
    yield* Effect.promise(() => boss.send("work_q", { tag: `fail-${t}`, fail: true }, { retryLimit: 0 }));
    yield* Deferred.await(done);
    yield* Effect.sleep("1500 millis");

    for (const tag of [`ok-${t}`, `fail-${t}`]) {
        const rows = yield* sql`select note from app_row where tag = ${tag} order by note`;
        const work = yield* sql`select state from pgboss_c1.job where name = 'work_q' and data->>'tag' = ${tag}`;
        const next =
            yield* sql`select count(*)::int as n from pgboss_c1.job where name = 'next_q' and data->>'tag' = ${tag}`;
        console.log(
            `[${tag.split("-")[0]}] app rows=${JSON.stringify(rows.map((r: any) => r.note))} work_q=${work[0]?.state} next_q jobs=${next[0]?.n}`,
        );
    }

    // Direction 1: send inside the app's withTransaction, then roll back.
    const d1 = `d1-${t}`;
    yield* sql
        .withTransaction(
            Effect.gen(function* () {
                yield* sql`insert into app_row ${sql.insert({ tag: d1, note: "app" })}`;
                const [conn] = Context.getUnsafe(yield* Effect.context<never>(), sql.transactionService);
                yield* Effect.promise(() =>
                    boss.flow(
                        [
                            { ref: "a", name: "next_q", data: { tag: d1 } },
                            { ref: "b", name: "next_q", data: { tag: d1 }, dependsOn: ["a"] },
                        ],
                        { db: conn } as any,
                    ),
                );
                return yield* Effect.fail("app rollback");
            }),
        )
        .pipe(Effect.ignore);
    const d1rows = yield* sql`select count(*)::int as n from app_row where tag = ${d1}`;
    const d1jobs = yield* sql`select count(*)::int as n from pgboss_c1.job where data->>'tag' = ${d1}`;
    console.log(`[d1 rollback] app rows=${d1rows[0].n} jobs=${d1jobs[0].n}`);

    yield* Effect.promise(() => boss.stop({ graceful: true, timeout: 5000 }));
    console.log(log.filter((l) => !l.startsWith("tx: ")).join("\n"));
    console.log("tx statements:", [...new Set(log.filter((l) => l.startsWith("tx: ")))].join(" | "));
});

const SqlLive = Layer.effect(SqlClient.SqlClient)(
    SqlClient.make({
        driver: "pg-node",
        acquirer,
        compiler: PgClient.makeCompiler(),
        spanAttributes: [],
        releaseSavepoint: (name: string) => `RELEASE SAVEPOINT ${name}`,
    }),
).pipe(Layer.provide(Reactivity.layer));

await Effect.runPromise(Effect.provide(Effect.scoped(program), SqlLive)).catch((e) => console.log("DEFECT", e));
await pool.end();
