import { Effect } from "effect";
import { SqlClient } from "effect/sql";
import { PgBoss } from "pg-boss";

import { pgLayer, showErr, URL } from "./common.ts";

const boss = new PgBoss({ connectionString: URL, schema: "pgboss" });
boss.on("error", (e: unknown) => console.log("[boss error]", showErr(e)));
await boss.start();
for (const q of ["parent", "child"]) {
    if (!(await boss.getQueue(q))) await boss.createQueue(q);
}

type Mode = "direct" | "split";
const sent: Array<string> = [];

const makeAdapter = (mode: Mode) =>
    Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const ctx = yield* Effect.context<SqlClient.SqlClient>();
        const runP = Effect.runPromiseWith(ctx);
        return {
            executeSql: (text: string, values?: Array<unknown>) => {
                sent.push(text);
                if (values?.length)
                    return runP(Effect.map(sql.unsafe(text, values), (rows) => ({ rows: rows as any[] })));
                if (mode === "direct") return runP(Effect.map(sql.unsafe(text), (rows) => ({ rows: rows as any[] })));
                const parts = text.split(";\n");
                return runP(
                    Effect.map(
                        Effect.forEach(parts, (p) => sql.unsafe(p)),
                        (all) => ({ rows: (all.at(-1) ?? []) as any[] }),
                    ),
                );
            },
        };
    });

const flowJobs = (tag: string) => [
    { ref: "p", name: "parent", data: { tag, note: "it's; a\ntest" } },
    { ref: "c", name: "child", data: { tag }, dependsOn: ["p"] },
];

const scenario = (mode: Mode, tag: string, failAfter: boolean) =>
    Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`create table if not exists app_row (tag text primary key)`;
        const res = yield* sql
            .withTransaction(
                Effect.gen(function* () {
                    yield* sql`insert into app_row ${sql.insert({ tag })}`;
                    const adapter = yield* makeAdapter(mode);
                    const ids = yield* Effect.tryPromise({
                        try: () => boss.flow(flowJobs(tag), { db: adapter }),
                        catch: (e) => e,
                    });
                    if (failAfter) return yield* Effect.fail(new Error("app failure after flow"));
                    return ids;
                }),
            )
            .pipe(Effect.result);
        const app = yield* sql`select count(*)::int as n from app_row where tag = ${tag}`;
        const jobs = yield* sql.unsafe(
            `select name, state, data->>'tag' as tag from pgboss.job where data->>'tag' = $1 order by name`,
            [tag],
        );
        return {
            result: res._tag === "Success" ? res.success : `FAILED: ${showErr(res.failure)}`,
            appRows: app[0].n,
            jobs,
        };
    });

const L = pgLayer();
const runS = (label: string, eff: Effect.Effect<any, any, SqlClient.SqlClient>) =>
    Effect.runPromise(Effect.provide(eff, L)).then(
        (r) => console.log(`[${label}]`, JSON.stringify(r, null, 1)),
        (d) => console.log(`[${label}] DEFECT`, showErr(d)),
    );

const t = Date.now();
await runS("E2 direct commit", scenario("direct", `e2-${t}`, false));
console.log("---- raw text pg-boss sent (E2) ----\n" + sent.at(-1) + "\n---- end ----");
await runS("E3a split commit", scenario("split", `e3a-${t}`, false));
await runS("E3b split rollback", scenario("split", `e3b-${t}`, true));
console.log("count of ';\\n' separators in last text:", sent.at(-1)!.split(";\n").length - 1);
await boss.stop({ graceful: false, wait: true } as any);
