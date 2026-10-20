import { PgClient } from "@effect/sql-pg";
import { Effect, Layer } from "effect";
import * as Reactivity from "effect/reactivity/Reactivity";
import { SqlClient } from "effect/sql";
import pg from "pg";
import { PgBoss } from "pg-boss";

import { makeBorrowedConnection } from "./borrowed.ts";
import { showErr, URL } from "./common.ts";

const pool = new pg.Pool({ connectionString: URL });
const boss = new PgBoss({ connectionString: URL, schema: "pgboss" });
boss.on("error", (e: unknown) => console.log("[boss error]", showErr(e)));
await boss.start();

const acquirer = Effect.map(
    Effect.acquireRelease(
        Effect.promise(() => pool.connect()),
        (c) => Effect.sync(() => c.release()),
    ),
    (c) => makeBorrowedConnection((text, values) => c.query(text, values)),
);
const PoolSql = Layer.effect(SqlClient.SqlClient)(
    SqlClient.make({
        driver: "pg-pool",
        acquirer,
        compiler: PgClient.makeCompiler(),
        spanAttributes: [],
        releaseSavepoint: (name: string) => `RELEASE SAVEPOINT ${name}`,
    }),
).pipe(Layer.provide(Reactivity.layer));

const scenario = (tag: string, failAfter: boolean) =>
    Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const res = yield* sql
            .withTransaction(
                Effect.gen(function* () {
                    yield* sql`insert into app_row ${sql.insert({ tag })}`;
                    const ctx = yield* Effect.context<SqlClient.SqlClient>();
                    const adapter = {
                        executeSql: (text: string, values?: Array<unknown>) =>
                            Effect.runPromiseWith(ctx)(
                                Effect.map(values?.length ? sql.unsafe(text, values) : sql.unsafe(text), (rows) => ({
                                    rows: rows as any[],
                                })),
                            ),
                    };
                    const ids = yield* Effect.tryPromise({
                        try: () =>
                            boss.flow(
                                [
                                    { ref: "p", name: "parent", data: { tag } },
                                    { ref: "c", name: "child", data: { tag }, dependsOn: ["p"] },
                                ],
                                { db: adapter },
                            ),
                        catch: (e) => e,
                    });
                    if (failAfter) return yield* Effect.fail(new Error("app failure after flow"));
                    return ids;
                }),
            )
            .pipe(Effect.result);
        const app = yield* sql`select count(*)::int as n from app_row where tag = ${tag}`;
        const jobs = yield* sql`select name, state from pgboss.job where data->>'tag' = ${tag} order by name`;
        return {
            result: res._tag === "Success" ? res.success : `FAILED: ${showErr(res.failure)}`,
            appRows: app[0].n,
            jobs,
        };
    });

const t = Date.now();
for (const [label, fail] of [
    ["E5 pool-backed commit", false],
    ["E5 pool-backed rollback", true],
] as const) {
    await Effect.runPromise(Effect.provide(scenario(`e5-${fail}-${t}`, fail), PoolSql)).then(
        (r) => console.log(`[${label}]`, JSON.stringify(r)),
        (d) => console.log(`[${label}] DEFECT`, showErr(d)),
    );
}
await boss.stop({ graceful: false, wait: true } as any);
await pool.end();
