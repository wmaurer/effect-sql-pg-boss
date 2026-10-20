import { PgClient } from "@effect/sql-pg";
import { Effect, Stream } from "effect";
import { SqlClient } from "effect/sql";
import { SqlError, UnknownError } from "effect/sql/SqlError";

import type { Connection } from "effect/sql/SqlConnection";

export type Exec = (text: string, values?: Array<unknown>) => Promise<{ rows: Array<any> }>;

const wrap = <A>(operation: string, f: () => Promise<A>) =>
    Effect.tryPromise({
        try: f,
        catch: (cause) =>
            new SqlError({
                reason: new UnknownError({ cause, message: `borrowed: ${String((cause as any)?.message)}`, operation }),
            }),
    });

export const makeBorrowedConnection = (exec: Exec, log?: Array<string>): Connection => {
    const q = (sql: string, params: ReadonlyArray<unknown>) => {
        log?.push(sql.trim().split(/\s+/).slice(0, 4).join(" "));
        return wrap("execute", () => exec(sql, params.length ? [...params] : undefined));
    };
    const rows = (sql: string, params: ReadonlyArray<unknown>, t?: (r: ReadonlyArray<any>) => ReadonlyArray<any>) =>
        Effect.map(q(sql, params), (r) => (t ? t(r.rows) : r.rows));
    const values = (sql: string, params: ReadonlyArray<unknown>) =>
        Effect.map(q(sql, params), (r) => r.rows.map((row: any) => Object.values(row)));
    return {
        execute: rows,
        executeUnprepared: rows,
        executeRaw: (sql, params) => q(sql, params),
        executeValues: values,
        executeValuesUnprepared: values,
        executeStream: (sql, params, t) => Stream.fromIterableEffect(rows(sql, params, t as any)) as any,
    };
};

export type Mode = "savepoint-controls" | "provide-transaction-service";

export const makeBorrowedClient = (exec: Exec, mode: Mode, log?: Array<string>) =>
    Effect.gen(function* () {
        const conn = makeBorrowedConnection(exec, log);
        const base = {
            driver: "pg-borrowed",
            acquirer: Effect.succeed(conn),
            compiler: PgClient.makeCompiler(),
            spanAttributes: [],
            releaseSavepoint: (name: string) => `RELEASE SAVEPOINT ${name}`,
        };
        if (mode === "savepoint-controls") {
            return yield* SqlClient.make({
                ...base,
                beginTransaction: "SAVEPOINT effect_borrowed",
                commit: "RELEASE SAVEPOINT effect_borrowed",
                rollback: "ROLLBACK TO SAVEPOINT effect_borrowed",
            });
        }
        const client = yield* SqlClient.make(base);
        return { client, conn };
    });
