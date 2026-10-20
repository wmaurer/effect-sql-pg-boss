/**
 * pg-boss' view of the shared pool: an `IDatabase` over `pg.Pool` that
 * supports everything pg-boss asks of its own `db` (multi-statement text,
 * migrations, maintenance, `flow` without `db`) plus `beginTransaction`, which
 * `work({ transactional: true })` requires.
 *
 * pg-boss treats a constructor `db` it did not create as caller-owned: it never
 * opens or closes it. The pool's owner is the `NodePgClient` layer.
 *
 * Invariants:
 * - Parameterless calls use the simple protocol (`values` is dropped when
 *   empty). pg-boss' own SQL needs it.
 * - Errors are the driver's, unwrapped, because pg-boss reads `code`,
 *   `constraint` and `detail` from them.
 * - The `db` of a {@link TransactionHandle} is created once per transaction and
 *   handed out unchanged. pg-boss matches settle calls to a handler's claim by
 *   the identity of this object.
 * - `rollback` never rejects, can be called twice, and gives up on a stuck
 *   connection after `rollbackTimeoutMillis` by destroying it.
 *
 * @internal
 */
import type * as Pg from "pg";
import type { Db } from "pg-boss";

/** @internal */
export const make = (_pool: Pg.Pool, _options?: { readonly rollbackTimeoutMillis?: number }): Db => {
    // executeSql(text, values) => pool.query(text, values?.length ? values : undefined)
    //
    // beginTransaction():
    //   client = await pool.connect()
    //   try { await client.query("BEGIN") } catch (e) { client.release(true); throw e }
    //   settled = false
    //   db = { executeSql: (t, v) => client.query(t, v?.length ? v : undefined) }
    //   return {
    //     db,
    //     commit: async () => { settled-check; try { await client.query("COMMIT") } catch (e) { client.release(true); throw e }; client.release() },
    //     rollback: async () => { if (settled) return; settled = true; race(client.query("ROLLBACK"), timeout) -> release(timedOut || failed) }
    //   }
    //
    // listen: omitted in this version (pg-boss polls), see BossConfig.listen in the rationale.
    throw new Error("not implemented");
};
