import { Effect } from "effect";
import { SqlClient } from "effect/sql";

import { pgLayer, run } from "./common.ts";

const multi = "select 1 as a; select 2 as b";

for (const [name, cfg] of [
    ["prepare default", {}],
    ["prepare:false", { prepare: false }],
] as const) {
    const L = pgLayer(cfg);
    await run(
        `${name} | withTransaction(sql.unsafe(multi))`,
        Effect.gen(function* () {
            const sql = yield* SqlClient.SqlClient;
            return yield* sql.withTransaction(sql.unsafe(multi));
        }),
        L,
    );
    await run(
        `${name} | withTransaction(sql.unsafe(multi).unprepared)`,
        Effect.gen(function* () {
            const sql = yield* SqlClient.SqlClient;
            return yield* sql.withTransaction(sql.unsafe(multi).unprepared);
        }),
        L,
    );
    await run(
        `${name} | withTransaction(sql.unsafe(multi).raw)`,
        Effect.gen(function* () {
            const sql = yield* SqlClient.SqlClient;
            return yield* sql.withTransaction(sql.unsafe(multi).raw);
        }),
        L,
    );
    await run(
        `${name} | no tx sql.unsafe(multi)`,
        Effect.gen(function* () {
            const sql = yield* SqlClient.SqlClient;
            return yield* sql.unsafe(multi);
        }),
        L,
    );
}
await run(
    `multiplex:true | withTransaction(sql.unsafe(multi).unprepared)`,
    Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        return yield* sql.withTransaction(sql.unsafe(multi).unprepared);
    }),
    pgLayer({ multiplex: true }),
);
