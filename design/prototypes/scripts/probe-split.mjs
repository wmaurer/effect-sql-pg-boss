// Captures flow's joined text, then splits on ';\n' and runs each piece on the extended protocol.
const NM = process.argv[2];
const { PgBoss } = await import(`${NM}/pg-boss/dist/index.js`);
const pg = (await import(`${NM}/pg/lib/index.js`)).default;
const conn = "postgres://postgres:pw@127.0.0.1:55511/postgres";
const boss = new PgBoss({ connectionString: conn, schema: "pgboss", supervise: false, schedule: false });
await boss.start();
const client = new pg.Client(conn);
await client.connect();
let captured;
const capture = {
    executeSql: async (text, values) => {
        captured = { text, values };
        return { rows: [] };
    },
};
const nasty = { s: "it's;\n done", t: ";\n", arr: [";", "\n"], q: "''; DROP", u: " ;\u000a" };
const refs = await boss.flow(
    [
        { ref: "p", name: "parent", data: nasty },
        { ref: "p2", name: "parent", data: { k: 2 } },
        { ref: "c", name: "child", data: nasty, dependsOn: ["p", "p2"] },
    ],
    { db: capture },
);
console.log("values passed:", captured.values);
console.log('occurrences of ";\\n":', captured.text.split(";\n").length - 1);
const parts = captured.text.split(";\n");
console.log("parts:", parts.length);
parts.forEach((p, i) =>
    console.log(
        `--- part ${i} head:`,
        JSON.stringify(p.trim().slice(0, 90)),
        "tail:",
        JSON.stringify(p.trim().slice(-60)),
    ),
);
await client.query("BEGIN");
for (const p of parts) await client.query({ text: p, values: [], queryMode: "extended" });
await client.query("COMMIT");
const { rows } = await client.query(`select name, data from pgboss.job where id = any($1::uuid[]) order by name`, [
    Object.values(refs),
]);
console.log("rows:", JSON.stringify(rows));
console.log(
    "data round-trip equal:",
    JSON.stringify(rows.find((r) => r.name === "child").data) === JSON.stringify(nasty),
);
const deps = await client.query(`select count(*) from pgboss.job_dependency where child_id = $1`, [refs.c]);
console.log("deps:", deps.rows[0].count);
// The no-db path (custom constructor IDatabase): what does the text look like?
let captured2;
const boss2 = boss; // reuse cache; call with no db but intercept this.db? Show plans.transaction shape instead
const plans = await import(`${NM}/pg-boss/dist/plans.js`);
console.log("plans.transaction shape:", JSON.stringify(plans.transaction(["SELECT 1", "SELECT 2"])));
await client.end();
await boss.stop({ graceful: false, close: true });
