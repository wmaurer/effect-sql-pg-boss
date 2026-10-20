// Runs every pg-boss API that accepts { db } with a db adapter that forces the extended
// protocol (Parse/Bind/Execute), like @effect/sql-pg does. Records each executeSql call.
const NM = process.argv[2];
const { PgBoss } = await import(`${NM}/pg-boss/dist/index.js`);
const pg = (await import(`${NM}/pg/lib/index.js`)).default;
const conn = "postgres://postgres:pw@127.0.0.1:55511/postgres";

const boss = new PgBoss({
    connectionString: conn,
    schema: "pgboss",
    supervise: false,
    schedule: true,
    useListenNotify: false,
});
boss.on("error", (e) => console.error("boss error", e.message));
await boss.start();
for (const q of ["parent", "child", "dlq", "ev"]) await boss.createQueue(q, { notify: q === "parent" });
await boss.updateQueue("child", { deadLetter: "dlq", retryLimit: 0 });

const client = new pg.Client(conn);
await client.connect();
const log = [];
let current = "";
const extDb = {
    executeSql: async (text, values) => {
        const stmtCount = text.split(/;\s*\n/).filter((s) => s.trim()).length;
        const entry = {
            api: current,
            hasValues: values !== undefined,
            nvalues: values?.length,
            len: text.length,
            semicolonNewlineSplits: stmtCount,
        };
        try {
            const r = await client.query({ text, values: values ?? [], queryMode: "extended" });
            entry.ok = true;
            log.push(entry);
            return r;
        } catch (e) {
            entry.ok = false;
            entry.err = e.message;
            log.push(entry);
            throw e;
        }
    },
};
async function run(api, fn) {
    current = api;
    await client.query("BEGIN");
    try {
        const r = await fn();
        await client.query("COMMIT");
        return r;
    } catch (e) {
        await client.query("ROLLBACK");
        log.push({ api, threw: e.message });
    }
}
const db = extDb;
const id1 = await run("send", () => boss.send("parent", { a: "x;\ny" }, { db }));
const ids = await run("insert", () => boss.insert("parent", [{ data: { b: 1 } }, { data: { b: 2 } }], { db }));
await run("flow", () =>
    boss.flow(
        [
            { ref: "p", name: "parent", data: { s: "it's;\n" } },
            { ref: "c", name: "child", dependsOn: ["p"] },
        ],
        { db },
    ),
);
await run("update", () => boss.update("parent", { u: 1 }, { id: id1, db }));
await run("upsert", () => boss.upsert("parent", { u: 2 }, { singletonKey: "k1", db }));
await boss.subscribe("evt", "ev");
await run("publish", () => boss.publish("evt", { e: 1 }, { db }));
const fetched = await run("fetch", () => boss.fetch("parent", { batchSize: 3, db }));
const f = fetched ?? [];
if (f[0]) await run("complete", () => boss.complete("parent", f[0].id, { ok: 1 }, { db }));
if (f[1]) await run("fail", () => boss.fail("parent", f[1].id, { bad: 1 }, { db }));
if (f[2]) await run("touch", () => boss.touch("parent", f[2].id, { db }));
if (f[2]) await run("cancel", () => boss.cancel("parent", f[2].id, { db }));
if (f[2]) await run("resume", () => boss.resume("parent", f[2].id, { db }));
if (f[1]) await run("retry", () => boss.retry("parent", f[1].id, { db }));
// fail on a queue with dead letter -> reinsertFailedJobs path inside ensureTransaction
const cid = await boss.send("child", { c: 1 }, { retryLimit: 0, deadLetter: "dlq" });
const cf = await boss.fetch("child", { batchSize: 5 });
if (cf.length)
    await run("fail(deadLetter)", () =>
        boss.fail(
            "child",
            cf.map((j) => j.id),
            { bad: 2 },
            { db },
        ),
    );
await run("getJobById", () => boss.getJobById("parent", id1, { db }));
await run("findJobs", () => boss.findJobs("parent", { db }));
await run("getDependencies", () => boss.getDependencies("child", cid ?? id1, { db }));
await run("getDependents", () => boss.getDependents("parent", id1, { db }));
await run("previewRedrive", () => boss.previewRedrive("dlq", { db }));
await run("redrive", () => boss.redrive("dlq", { db }));
await run("deleteJob", () => boss.deleteJob("parent", id1, { db }));
await run("schedule", () => boss.schedule("parent", "* * * * *", { s: 1 }, { db }));
await run("unschedule", () => boss.unschedule("parent", undefined, { db }));
for (const e of log) console.log(JSON.stringify(e));
await client.end();
await boss.stop({ graceful: false, close: true });
