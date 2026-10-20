import * as Effect from "/home/wayne/projects/effect-ts/effect/.worktrees/feat-sql-pg-pgboss-transactions/packages/effect/src/Effect.ts";
import * as Reactivity from "/home/wayne/projects/effect-ts/effect/.worktrees/feat-sql-pg-pgboss-transactions/packages/effect/src/reactivity/Reactivity.ts";
import * as SqlClient from "/home/wayne/projects/effect-ts/effect/.worktrees/feat-sql-pg-pgboss-transactions/packages/effect/src/sql/SqlClient.ts";
import * as Statement from "/home/wayne/projects/effect-ts/effect/.worktrees/feat-sql-pg-pgboss-transactions/packages/effect/src/sql/Statement.ts";
import * as Stream from "/home/wayne/projects/effect-ts/effect/.worktrees/feat-sql-pg-pgboss-transactions/packages/effect/src/Stream.ts";

import type { Connection } from "/home/wayne/projects/effect-ts/effect/.worktrees/feat-sql-pg-pgboss-transactions/packages/effect/src/sql/SqlConnection.ts";

const makeConn = (name: string, log: Array<string>): Connection => {
    const run = (sql: string, params: ReadonlyArray<unknown>) =>
        Effect.sync(() => {
            log.push(`${name}: ${sql} ${JSON.stringify(params)}`);
            return [] as Array<any>;
        });
    return {
        execute: (sql, params) => run(sql, params),
        executeRaw: (sql, params) => run(sql, params),
        executeStream: () => Stream.empty,
        executeValues: (sql, params) => run(sql, params),
        executeValuesUnprepared: (sql, params) => run(sql, params),
        executeUnprepared: (sql, params) => run(sql, params),
    };
};

const compiler = Statement.makeCompiler({
    dialect: "pg",
    placeholder: (i) => `$${i}`,
    onIdentifier: Statement.defaultEscape('"'),
    onRecordUpdate: () => ["", []],
    onCustom: () => ["", []],
});

const main = Effect.gen(function* () {
    const log: Array<string> = [];
    const pool = makeConn("pool", log);
    const borrowed = makeConn("borrowed", log);
    const sql = yield* SqlClient.make({
        driver: "fake",
        acquirer: Effect.succeed(pool),
        compiler,
        spanAttributes: [],
    });

    // A: provide the client's own transactionService with a borrowed connection
    const a = yield* Effect.exit(
        Effect.gen(function* () {
            yield* sql`select ${1}`;
            yield* sql.withTransaction(sql`insert ${2}`);
        }).pipe(Effect.provideService(sql.transactionService, [borrowed, 0] as const)),
    );
    console.log("A exit:", a._tag, a._tag === "Failure" ? String(a.cause) : "");
    console.log("A log:", log.splice(0));

    // B: separate client whose top-level transaction is a savepoint on the borrowed conn
    const borrowedClient = yield* SqlClient.make({
        driver: "fake-borrowed",
        acquirer: Effect.succeed(borrowed),
        compiler,
        spanAttributes: [],
        beginTransaction: "SAVEPOINT effect_sql_borrowed",
        commit: "RELEASE SAVEPOINT effect_sql_borrowed",
        rollback: "ROLLBACK TO SAVEPOINT effect_sql_borrowed",
        releaseSavepoint: (n) => `RELEASE SAVEPOINT ${n}`,
    });
    const b = yield* Effect.exit(
        Effect.gen(function* () {
            yield* borrowedClient`select ${1}`;
            yield* borrowedClient.withTransaction(
                Effect.gen(function* () {
                    yield* borrowedClient`insert ${2}`;
                    yield* borrowedClient.withTransaction(borrowedClient`insert ${3}`);
                    yield* borrowedClient.withTransaction(Effect.fail("boom")).pipe(Effect.ignore);
                }),
            );
        }),
    );
    console.log("B exit:", b._tag, b._tag === "Failure" ? String(b.cause) : "");
    console.log("B log:", log.splice(0));
});

Effect.runPromise(main.pipe(Effect.provide(Reactivity.layer))).catch((e) => console.error("top", e));
