import { Effect, Layer } from "effect";
import * as Reactivity from "effect/reactivity/Reactivity";
import { SqlClient } from "effect/sql";
import pg from "pg";
import { PgBoss } from "pg-boss";

import { makeBorrowedClient } from "./borrowed.ts";
import { showErr, URL } from "./common.ts";

const pool = new pg.Pool({ connectionString: URL });
await pool.query(`create table if not exists app_row (tag text primary key)`);

const boss = new PgBoss({ connectionString: URL, schema: "pgboss" });
boss.on("error", (e: unknown) => console.log("[boss error]", showErr(e)));
await boss.start();
if (!(await boss.getQueue("wq"))) await boss.createQueue("wq", { retryLimit: 0 } as any);

const t = Date.now();
const logs: Record<string, Array<string>> = {};

const appWork = (tag: string, fail: boolean) =>
    Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`insert into app_row ${sql.insert({ tag: `${tag}-outer` })}`;
        yield* sql.withTransaction(sql`insert into app_row ${sql.insert({ tag: `${tag}-nested` })}`);
        yield* sql
            .withTransaction(
                Effect.andThen(
                    sql`insert into app_row ${sql.insert({ tag: `${tag}-nested-rolledback` })}`,
                    Effect.fail("inner boom"),
                ),
            )
            .pipe(Effect.ignore);
        const seen = yield* sql<{ tag: string }>`select tag from app_row where tag like ${tag + "%"} order by tag`;
        if (fail)
            return yield* Effect.fail(new Error(`handler fails after writing ${seen.map((r) => r.tag).join(",")}`));
        return seen.map((r) => r.tag);
    });

let txKeys: unknown;
await boss.work(
    "wq",
    { transactional: true, pollingIntervalSeconds: 0.5 } as any,
    async (jobs: Array<any>, tx: any) => {
        txKeys = {
            keys: Object.keys(tx),
            protoKeys: Object.getOwnPropertyNames(Object.getPrototypeOf(tx)),
            hasClient: "client" in tx,
            type: tx?.constructor?.name,
        };
        const { tag, fail } = jobs[0].data;
        const log = (logs[tag] = []);
        const program = Effect.flatMap(
            makeBorrowedClient((text, values) => tx.executeSql(text, values), "savepoint-controls", log),
            (client) => Effect.provideService(appWork(tag, fail), SqlClient.SqlClient, client as any),
        );
        return Effect.runPromise(Effect.provide(program, Reactivity.layer));
    },
);

const okId = await boss.send("wq", { tag: `ok-${t}`, fail: false });
const failId = await boss.send("wq", { tag: `fail-${t}`, fail: true });
await new Promise((r) => setTimeout(r, 4000));

console.log("tx shape:", JSON.stringify(txKeys));
for (const [label, id, tag] of [
    ["success", okId, `ok-${t}`],
    ["failure", failId, `fail-${t}`],
] as const) {
    const job = await pool.query(`select state, output from pgboss.job where id = $1`, [id]);
    const rows = await pool.query(`select tag from app_row where tag like $1 order by tag`, [tag + "%"]);
    console.log(
        `[E4 ${label}] job:`,
        JSON.stringify(job.rows[0]),
        "app rows:",
        JSON.stringify(rows.rows.map((r) => r.tag)),
    );
    console.log(`[E4 ${label}] statements effect sent on tx:`, JSON.stringify(logs[tag]));
}

// Variant: keep default BEGIN/COMMIT and instead seed the client's transactionService with the borrowed conn.
const client = await pool.connect();
await client.query("BEGIN");
const vlog: Array<string> = [];
const r = await Effect.runPromise(
    Effect.provide(
        Effect.flatMap(
            makeBorrowedClient((t, v) => client.query(t, v), "provide-transaction-service", vlog),
            (x: any) =>
                Effect.provideService(
                    Effect.provideService(appWork(`svc-${t}`, false), SqlClient.SqlClient, x.client),
                    x.client.transactionService,
                    [x.conn, 0] as const,
                ),
        ).pipe(Effect.exit),
        Reactivity.layer,
    ),
).catch((d) => ({ defect: showErr(d) }));
await client.query("ROLLBACK");
client.release();
console.log(
    "[E4 provide-transaction-service] exit:",
    (r as any)._tag ?? "",
    showErr((r as any).cause ?? (r as any).defect ?? r),
);
console.log("[E4 provide-transaction-service] statements:", JSON.stringify(vlog));

await boss.stop({ graceful: false, wait: true } as any);
await pool.end();
