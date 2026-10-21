import { assert, layer } from "@effect/vitest";
import { Array as Arr, Deferred, Effect, Exit, Layer, MutableRef, Option, Order } from "effect";

import { JobQueue } from "../src/index.ts";
import * as Connection from "../src/internal/connection.ts";
import { appNotes, Harness, JobData, jobStates, layer as harness, maintenanceQuery, UUID } from "./fixture.ts";

class P1Follow extends JobQueue.Service<P1Follow>()("p1-follow", { payload: JobData }) {}
class FlowA extends JobQueue.Service<FlowA>()("flow-a", { payload: JobData }) {}
class FlowB extends JobQueue.Service<FlowB>()("flow-b", { payload: JobData }) {}

layer(harness({ database: "connection" }, Layer.mergeAll(P1Follow.layer, FlowA.layer, FlowB.layer)), {
    excludeTestServices: true,
    timeout: "60 seconds",
})("pg-boss on the fiber's transaction", (it) => {
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
                    P1Follow.send({ tag: "p5-send" }),
                ),
            );
            assert.match(
                Option.getOrElse(id, () => ""),
                UUID,
            );
            assert.deepStrictEqual(yield* appNotes("p5-send"), ["app"]);
            assert.deepStrictEqual(yield* jobStates("p5-send"), ["p1-follow:created"]);
        }),
    );

    it.effect("P5: flow commits and rolls back with the transaction, and opens its own outside one", () =>
        Effect.gen(function* () {
            const { sql } = yield* Harness;
            const flow = (tag: string) =>
                JobQueue.flow({
                    a: FlowA.step({ tag, note: "x;\ny" }),
                    b: FlowB.step({ tag }, { dependsOn: ["a"] }),
                });
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

    it.effect("pg-boss sends a flow on a caller's db as one script of statements joined by FLOW_SEPARATOR", () =>
        Effect.gen(function* () {
            const { boss } = yield* Harness;
            const calls = MutableRef.make<
                ReadonlyArray<{ readonly text: string; readonly values: ReadonlyArray<unknown> | undefined }>
            >([]);
            const db: Connection.Db = {
                executeSql: (text, values) => {
                    MutableRef.update(calls, Arr.append({ text, values }));
                    return Promise.resolve({ rows: [] });
                },
            };
            yield* Effect.promise(() =>
                boss.flow(
                    [
                        { ref: "a", name: "flow-a", data: { tag: "guard" } },
                        { ref: "b", name: "flow-b", data: { tag: "guard" }, dependsOn: ["a"] },
                    ],
                    { db },
                ),
            );
            const sent = MutableRef.get(calls);
            assert.strictEqual(sent.length, 1, "one executeSql call for the whole flow");
            assert.strictEqual(sent[0]?.values, undefined, "a script without values");
            const statements = Arr.map((sent[0]?.text ?? "").split(Connection.FLOW_SEPARATOR), (part) =>
                `${/INSERT INTO \S+/.exec(part)?.[0]} ${/'([\w-]+)' as name/.exec(part)?.[1] ?? ""}`.trimEnd(),
            );
            assert.deepStrictEqual(statements, [
                "INSERT INTO pgboss.job_common flow-a",
                "INSERT INTO pgboss.job_common flow-b",
                "INSERT INTO pgboss.job_dependency",
            ]);
        }),
    );

    it.effect("flow keeps a caller-chosen id on a step, so the app can store it before the commit", () =>
        Effect.gen(function* () {
            const { sql } = yield* Harness;
            const parentId = "0b6e5f3c-2a4d-4c1e-9f7a-3d2b1c0a9e8f";
            const jobs = yield* sql.withTransaction(
                Effect.gen(function* () {
                    const ids = yield* JobQueue.flow({
                        parent: FlowA.step({ tag: "chosen-id" }, { job: { id: parentId } }),
                        child: FlowB.step({ tag: "chosen-id" }, { dependsOn: ["parent"] }),
                    });
                    yield* sql`insert into app ${sql.insert({ tag: "chosen-id", note: ids.parent })}`;
                    return ids;
                }),
            );
            assert.strictEqual(jobs.parent, parentId);
            assert.deepStrictEqual(yield* appNotes("chosen-id"), [parentId]);
            assert.deepStrictEqual(
                yield* maintenanceQuery("select name, state from pgboss.job where id = $1", [parentId]),
                [{ name: "flow-a", state: "created" }],
            );
            assert.deepStrictEqual(
                yield* maintenanceQuery(
                    "select parent_name, parent_id::text from pgboss.job_dependency where child_id = $1",
                    [jobs.child],
                ),
                [{ parent_name: "flow-a", parent_id: parentId }],
            );
        }),
    );

    it.effect("a flow statement that fails after the first one rolls back the app's rows and the whole flow", () =>
        Effect.gen(function* () {
            const { sql } = yield* Harness;
            const takenId = "6f1d2c3b-4a5e-4f6a-8b7c-9d0e1f2a3b4c";
            yield* FlowB.send({ tag: "flow-taken" }, { id: takenId });
            const error = yield* sql
                .withTransaction(
                    Effect.gen(function* () {
                        yield* sql`insert into app ${sql.insert({ tag: "flow-fails", note: "app" })}`;
                        return yield* JobQueue.flow({
                            a: FlowA.step({ tag: "flow-fails" }),
                            b: FlowB.step({ tag: "flow-fails" }, { dependsOn: ["a"], job: { id: takenId } }),
                        });
                    }),
                )
                .pipe(Effect.flip);
            assert.strictEqual(error._tag, "PgBossError");
            const cause = error._tag === "PgBossError" && error.cause instanceof Error ? error.cause : undefined;
            assert.strictEqual(error._tag === "PgBossError" ? error.operation : undefined, "flow");
            assert.strictEqual(cause?.message.split(". ")[0], "one or more jobs could not be created");
            assert.propertyVal(cause?.cause, "code", "22012");
            assert.deepStrictEqual(yield* appNotes("flow-fails"), []);
            assert.deepStrictEqual(yield* jobStates("flow-fails"), []);
            assert.deepStrictEqual(yield* jobStates("flow-taken"), ["flow-b:created"]);
        }),
    );
});
