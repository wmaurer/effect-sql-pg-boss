import { assert, layer } from "@effect/vitest";
import { Array as Arr, Deferred, Effect, Exit, Order } from "effect";

import * as Connection from "../src/internal/connection.ts";
import { appNotes, Harness, jobStates, layer as harness, send, UUID } from "./fixture.ts";

layer(
    harness({
        database: "connection",
        queues: [
            ["p1-follow", {}],
            ["flow-a", {}],
            ["flow-b", {}],
        ],
    }),
    { excludeTestServices: true, timeout: "60 seconds" },
)("pg-boss on the fiber's transaction", (it) => {
    it.effect("P5: pg-boss gets raw rows while the app's client transforms column names", () =>
        Effect.gen(function* () {
            const { sql } = yield* Harness;
            assert.deepStrictEqual(yield* sql`select 1 as created_on`, [{ createdOn: 1 }]);
            assert.deepStrictEqual(yield* sql.unsafe("select 1 as created_on"), [{ createdOn: 1 }]);
            const viaDb = yield* sql.withTransaction(
                Connection.withDb(sql, (db) => Effect.promise(() => db.executeSql("select 1 as created_on", []))),
            );
            assert.deepStrictEqual(viaDb.rows, [{ created_on: 1 }]);
        }),
    );

    it.effect("P5: send inside sql.withTransaction commits with the app's rows", () =>
        Effect.gen(function* () {
            const { sql } = yield* Harness;
            const id = yield* sql.withTransaction(
                Effect.andThen(
                    sql`insert into app ${sql.insert({ tag: "p5-send", note: "app" })}`,
                    send("p1-follow", { tag: "p5-send" }),
                ),
            );
            assert.match(id ?? "", UUID);
            assert.deepStrictEqual(yield* appNotes("p5-send"), ["app"]);
            assert.deepStrictEqual(yield* jobStates("p5-send"), ["p1-follow:created"]);
        }),
    );

    it.effect("P5: flow commits and rolls back with the transaction, and opens its own outside one", () =>
        Effect.gen(function* () {
            const { sql, boss } = yield* Harness;
            const flow = (tag: string) =>
                Connection.withDb(sql, (db) =>
                    Connection.call("flow", () =>
                        boss.flow(
                            [
                                { ref: "a", name: "flow-a", data: { tag, note: "x;\ny" } },
                                { ref: "b", name: "flow-b", data: { tag }, dependsOn: ["a"] },
                            ],
                            { db },
                        ),
                    ),
                );
            const flowIn = (tag: string, fail: boolean) =>
                sql.withTransaction(
                    Effect.gen(function* () {
                        yield* sql`insert into app ${sql.insert({ tag, note: "app" })}`;
                        const ids = yield* flow(tag);
                        if (fail) return yield* Effect.fail("app rollback");
                        return ids;
                    }),
                );

            const committed = yield* flowIn("p5-flow-ok", false);
            assert.deepStrictEqual(Arr.sort(Object.keys(committed), Order.String), ["a", "b"]);
            assert.deepStrictEqual(yield* appNotes("p5-flow-ok"), ["app"]);
            assert.deepStrictEqual(yield* jobStates("p5-flow-ok"), ["flow-a:created", "flow-b:created"]);

            const rolledBack = yield* Effect.exit(flowIn("p5-flow-bad", true));
            assert.deepStrictEqual(rolledBack, Exit.fail("app rollback"));
            assert.deepStrictEqual(yield* appNotes("p5-flow-bad"), []);
            assert.deepStrictEqual(yield* jobStates("p5-flow-bad"), []);

            const bare = yield* flow("p5-flow-bare");
            assert.deepStrictEqual(Arr.sort(Object.keys(bare), Order.String), ["a", "b"]);
            assert.deepStrictEqual(yield* jobStates("p5-flow-bare"), ["flow-a:created", "flow-b:created"]);
        }),
    );

    it.effect("P5: a unique violation reaches pg-boss with its SQLSTATE and the Effect as UniqueViolation", () =>
        Effect.gen(function* () {
            const { sql } = yield* Harness;
            yield* sql`create table synth_unique (id int primary key)`;
            yield* sql`insert into synth_unique values (1)`;
            const seenByPgBoss = yield* Deferred.make<Error>();
            const error = yield* sql
                .withTransaction(
                    Connection.withDb(sql, (db) =>
                        Connection.call("executeSql", () =>
                            db.executeSql("insert into synth_unique values ($1)", [1]).catch((rejection: Error) => {
                                Deferred.doneUnsafe(seenByPgBoss, Exit.succeed(rejection));
                                return Promise.reject(rejection);
                            }),
                        ),
                    ),
                )
                .pipe(Effect.flip);
            const driver = yield* Deferred.await(seenByPgBoss);
            assert.instanceOf(driver, Error);
            assert.propertyVal(driver, "code", "23505");
            assert.propertyVal(driver, "constraint", "synth_unique_pkey");
            assert.strictEqual(error._tag, "SqlError");
            assert.strictEqual(error._tag === "SqlError" ? error.reason._tag : undefined, "UniqueViolation");
        }),
    );
});
