// Prototype: pg-boss transactional worker whose transaction is a real
// `PgClient.withTransaction` on the native client ("parked transaction"),
// with pg-boss housekeeping on a node-postgres sidecar pool.
import { PgClient } from "@effect/sql-pg";
import { Context, Deferred, Effect, Exit, Fiber, Layer, Redacted } from "effect";
import { SqlClient } from "effect/sql";
import pg from "pg";
import { PgBoss } from "pg-boss";

const URL = "postgres://postgres:probe@127.0.0.1:55503/postgres";
const log = (...a: Array<unknown>) => console.log(...a);

type Rows = { rows: Array<any> };
const toDriverError = (e: unknown): unknown => {
    const reason = (e as any)?.reason;
    return reason?.cause instanceof Error ? reason.cause : e;
};

const SPLIT = ";\n";
const makeCallDb = (ctx: Context.Context<SqlClient.SqlClient>) => {
    const sql = Context.get(ctx, SqlClient.SqlClient);
    const run = Effect.runPromiseWith(ctx);
    return {
        executeSql: (text: string, values?: Array<unknown>): Promise<Rows> =>
            run(
                values !== undefined
                    ? Effect.map(sql.unsafe(text, values), (rows) => ({ rows: rows as Array<any> }))
                    : Effect.map(
                          sql.withTransaction(Effect.forEach(text.split(SPLIT), (part) => sql.unsafe(part))),
                          (all) => ({ rows: (all.at(-1) ?? []) as Array<any> }),
                      ),
            ).catch((e) => Promise.reject(toDriverError(e))),
    };
};

const program = Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const layerCtx = yield* Effect.context<SqlClient.SqlClient>();
    const runFork = Effect.runForkWith(layerCtx);
    const contexts = new WeakMap<object, Context.Context<SqlClient.SqlClient>>();

    const pool = new pg.Pool({ connectionString: URL, max: 3 });
    const db = {
        executeSql: async (text: string, values?: Array<unknown>) => {
            const r: any = await pool.query(text, values);
            return { rows: Array.isArray(r) ? r.flatMap((x: any) => x.rows ?? []) : r.rows };
        },
        beginTransaction: async () => {
            const ready = Deferred.makeUnsafe<Context.Context<SqlClient.SqlClient>>();
            const outcome = Deferred.makeUnsafe<void, "rollback">();
            const fiber = runFork(
                sql.withTransaction(
                    Effect.gen(function* () {
                        const ctx = yield* Effect.context<SqlClient.SqlClient>();
                        Deferred.doneUnsafe(ready, Exit.succeed(ctx));
                        yield* Deferred.await(outcome);
                    }),
                ),
            );
            fiber.addObserver((exit) => {
                if (Exit.isFailure(exit)) Deferred.doneUnsafe(ready, exit as any);
            });
            const ctx = await Effect.runPromise(Deferred.await(ready));
            const txDb = makeCallDb(ctx);
            contexts.set(txDb, ctx);
            let settled = false;
            return {
                db: txDb,
                commit: async () => {
                    settled = true;
                    Deferred.doneUnsafe(outcome, Exit.void);
                    const exit = await Effect.runPromise(Fiber.await(fiber));
                    if (Exit.isFailure(exit)) throw toDriverError(exit.cause);
                },
                rollback: async () => {
                    if (!settled) {
                        settled = true;
                        Deferred.doneUnsafe(outcome, Exit.fail("rollback"));
                    }
                    await Effect.runPromise(Fiber.await(fiber));
                },
            };
        },
    };

    const boss = new PgBoss({ db: db as any, schema: "pgboss" });
    boss.on("error", (e: unknown) => log("[boss error]", (e as any)?.message));
    yield* Effect.promise(() => boss.start());
    yield* Effect.addFinalizer(() =>
        Effect.promise(async () => {
            await boss.stop({ graceful: true, wait: true } as any);
            await pool.end();
        }),
    );
    for (const q of ["c3work", "c3follow", "c3parent", "c3child"]) {
        yield* Effect.promise(() => boss.createQueue(q, { retryLimit: 0 } as any));
    }
    yield* sql`create table if not exists c3_app (tag text primary key, n int8)`;
    const events: Array<string> = [];

    const handler = (tag: string, fail: boolean) =>
        Effect.gen(function* () {
            yield* sql`insert into c3_app ${sql.insert({ tag, n: 1n as any })}`;
            // nested withTransaction inside the worker's transaction
            yield* sql.withTransaction(sql`update c3_app set n = n + 1 where tag = ${tag}`);
            yield* sql.afterCommit(
                Effect.promise(async () => {
                    const r = await pool.query(
                        "select state from pgboss.job where data->>'tag' = $1 and name = 'c3work'",
                        [tag],
                    );
                    events.push(`afterCommit ${tag}: job state seen from another connection = ${r.rows[0]?.state}`);
                }),
            );
            // enqueue inside the worker transaction via a per-call adapter
            const ctx = yield* Effect.context<SqlClient.SqlClient>();
            yield* Effect.promise(() => boss.send("c3follow", { tag }, { db: makeCallDb(ctx) } as any));
            const [{ n }] = yield* sql<{ n: unknown }>`select n from c3_app where tag = ${tag}`;
            events.push(`handler ${tag}: n=${String(n)} (${typeof n})`);
            if (fail) return yield* Effect.fail(new Error("handler failure"));
        });

    const jobTags = new Map<string, boolean>();
    yield* Effect.promise(() =>
        boss.work("c3work", { transactional: true, pollingIntervalSeconds: 0.5 } as any, async (jobs: any, tx: any) => {
            const ctx = contexts.get(tx);
            if (!ctx) throw new Error("tx identity lost");
            for (const job of jobs) {
                await Effect.runPromiseWith(ctx)(handler(job.data.tag, jobTags.get(job.data.tag)!));
            }
        }),
    );

    const t = Date.now();
    const okTag = `ok-${t}`,
        badTag = `bad-${t}`;
    jobTags.set(okTag, false);
    jobTags.set(badTag, true);
    yield* Effect.promise(() => boss.send("c3work", { tag: okTag }));
    yield* Effect.promise(() => boss.send("c3work", { tag: badTag }));
    yield* Effect.sleep("4 seconds");

    const report = (tag: string) =>
        Effect.gen(function* () {
            const app = yield* sql`select n from c3_app where tag = ${tag}`;
            const jobs = yield* sql.unsafe(`select name, state from pgboss.job where data->>'tag' = $1 order by name`, [
                tag,
            ]);
            return { tag, app, jobs };
        });
    log(JSON.stringify(yield* report(okTag), (_, v) => (typeof v === "bigint" ? `${v}n` : v)));
    log(JSON.stringify(yield* report(badTag), (_, v) => (typeof v === "bigint" ? `${v}n` : v)));
    log(events.join("\n"));

    // Direction 1: enqueue (send + flow) inside an app withTransaction
    const enqueue = (tag: string, fail: boolean) =>
        sql
            .withTransaction(
                Effect.gen(function* () {
                    yield* sql`insert into c3_app ${sql.insert({ tag, n: 0n as any })}`;
                    const ctx = yield* Effect.context<SqlClient.SqlClient>();
                    const callDb = makeCallDb(ctx);
                    yield* Effect.promise(() => boss.send("c3follow", { tag }, { db: callDb } as any));
                    yield* Effect.promise(() =>
                        boss.flow(
                            [
                                { ref: "p", name: "c3parent", data: { tag, note: "a;\nb" } },
                                { ref: "c", name: "c3child", data: { tag }, dependsOn: ["p"] },
                            ] as any,
                            { db: callDb } as any,
                        ),
                    );
                    if (fail) return yield* Effect.fail("app failure");
                }),
            )
            .pipe(Effect.result);
    yield* enqueue(`enq-ok-${t}`, false);
    yield* enqueue(`enq-bad-${t}`, true);
    log(JSON.stringify(yield* report(`enq-ok-${t}`), (_, v) => (typeof v === "bigint" ? `${v}n` : v)));
    log(JSON.stringify(yield* report(`enq-bad-${t}`), (_, v) => (typeof v === "bigint" ? `${v}n` : v)));

    // flow outside any transaction: per-call adapter wraps the split in withTransaction
    yield* Effect.promise(() =>
        boss.flow(
            [
                { ref: "p", name: "c3parent", data: { tag: `bare-${t}` } },
                { ref: "c", name: "c3child", data: { tag: `bare-${t}` }, dependsOn: ["p"] },
            ] as any,
            { db: makeCallDb(layerCtx) } as any,
        ),
    );
    log(JSON.stringify(yield* report(`bare-${t}`)));
});

await Effect.runPromise(
    Effect.scoped(program).pipe(
        Effect.provide(PgClient.layer({ url: Redacted.make(URL) })),
        Effect.catchCause((c) => Effect.sync(() => console.log("FAILED", c))),
    ),
);
