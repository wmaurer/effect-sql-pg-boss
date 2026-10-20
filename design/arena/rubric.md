# Rubric: `@wmaurer/effect-sql-pg-boss` API sketch

Score each criterion 1-5 per candidate, with one line of evidence (file:line in the candidate).

1. **Caller ergonomics for both directions.** Enqueue-in-transaction and write-in-worker are each one obvious call in Effect code. No manual `runPromise`, context capture, or adapter plumbing in app code.
2. **Correct by construction.** The design makes the grounding's caveats impossible or explicit in types: no COMMIT/release on a borrowed transaction, `tx` identity preserved for pg-boss settle calls, driver error `code`/`constraint`/`detail` preserved, concurrent use of one borrowed connection serialized, the early-`afterCommit` problem handled or documented, `flow` atomic without relying on undocumented behavior (or the reliance is isolated and named).
3. **Interface depth.** A small public surface that hides pg-boss's Promise API, node-postgres, and IDatabase details. No pass-through wrappers that mirror pg-boss method by method without adding value. No wire/driver types on the public API unless unavoidable.
4. **Shared database and lifecycle.** One clear story for pool sharing, `boss.start()`/`stop()` as a scoped Layer, and schema install. Failure modes (pool closed by whom, pg-boss not owning a custom pool) are handled.
5. **Upgrade path.** If upstream `@effect/sql-pg` gains a simple-query method and the `SqlClient.ts:676` semaphore fix, does the design shrink without breaking callers? Does it avoid locking users into decoding semantics they cannot later change?
6. **Fidelity and verifiability.** Uses real Effect v4 APIs (checked against packages/effect/src) and real pg-boss 12.38.0 types; the sketch typechecks or the gaps are named. Typed job payloads (Schema) without overreach.
