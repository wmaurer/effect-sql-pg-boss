// What does a transactional worker's handler receive, and what does pg-boss call on the handle?
const NM = process.argv[2];
const { PgBoss } = await import(`${NM}/pg-boss/dist/index.js`);
const Db = (await import(`${NM}/pg-boss/dist/db.js`)).default;
const conn = "postgres://postgres:pw@127.0.0.1:55511/postgres";
const log = [];
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// 1. Built-in pool: what is tx?
{
    const boss = new PgBoss({
        connectionString: conn,
        schema: "pgboss",
        supervise: false,
        schedule: false,
        pollingIntervalSeconds: 0.5,
    });
    await boss.start();
    await boss.createQueue("txq");
    let seen;
    await boss.work("txq", { transactional: true, pollingIntervalSeconds: 0.5 }, async (jobs, tx) => {
        seen = {
            keys: Object.keys(tx),
            proto: Object.getPrototypeOf(tx) === Object.prototype,
            hasClient: "client" in tx,
            ctor: tx.constructor?.name,
        };
    });
    await boss.send("txq", {});
    for (let i = 0; i < 40 && !seen; i++) await sleep(100);
    console.log("built-in tx shape:", JSON.stringify(seen));
    await boss.stop({ graceful: false, close: true });
}

// 2. Custom IDatabase: pg-boss's own Db for executeSql, app-controlled beginTransaction.
const inner = new Db({ connectionString: conn });
await inner.open();
let lastTxDb;
const adapter = {
    executeSql: (t, v, o) => inner.executeSql(t, v, o),
    listen: (...a) => inner.listen(...a),
    poolCounts: () => inner.poolCounts(),
    beginTransaction: async () => {
        log.push("beginTransaction");
        const h = await inner.beginTransaction();
        const db = {
            appMarker: Symbol("app-tx"),
            executeSql: (t, v) => {
                log.push(`tx.executeSql: ${t.trim().replace(/\s+/g, " ").slice(0, 70)} | nvalues=${v?.length}`);
                return h.db.executeSql(t, v);
            },
        };
        lastTxDb = db;
        return {
            db,
            commit: () => {
                log.push("handle.commit");
                return h.commit();
            },
            rollback: () => {
                log.push("handle.rollback");
                return h.rollback();
            },
        };
    },
};
await inner.executeSql("create table if not exists app_t (id serial primary key, note text)");
const boss = new PgBoss({
    db: adapter,
    schema: "pgboss",
    supervise: false,
    schedule: false,
    migrate: false,
    createSchema: false,
});
boss.on("error", (e) => log.push("boss error " + e.message));
await boss.start();
let done = 0;
await boss.work("txq", { transactional: true, pollingIntervalSeconds: 0.5 }, async (jobs, tx) => {
    log.push(`handler: tx === adapter db? ${tx === lastTxDb}; ok=${jobs[0].data.ok}`);
    await tx.executeSql("insert into app_t (note) values ($1)", [jobs[0].id]);
    done++;
    if (!jobs[0].data.ok) throw new Error("boom");
});
await boss.send("txq", { ok: true }, { retryLimit: 0 });
for (let i = 0; i < 40 && done < 1; i++) await sleep(100);
await sleep(300);
log.push("--- failing job");
await boss.send("txq", { ok: false }, { retryLimit: 0 });
for (let i = 0; i < 40 && done < 2; i++) await sleep(100);
await sleep(500);
// flow without options.db through the custom adapter (multi-statement BEGIN..COMMIT on inner pool)
await boss.createQueue("parent");
await boss.createQueue("child");
const refs = await boss.flow([
    { ref: "p", name: "parent" },
    { ref: "c", name: "child", dependsOn: ["p"] },
]);
log.push("flow via constructor adapter ok: " + Object.keys(refs).join(","));
const { rows } = await inner.executeSql("select count(*)::int n from app_t");
log.push("app_t rows (expect 1 from committed job): " + rows[0].n);
console.log(log.join("\n"));
await boss.stop({ graceful: false, close: true });
await inner.close();
