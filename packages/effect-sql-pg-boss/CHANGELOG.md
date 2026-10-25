# @wmaurer/effect-sql-pg-boss

## 0.1.0

The first release. It runs pg-boss 12.38 next to the app's native `@effect/sql-pg` client, so jobs and the app's rows share transactions.

- `PgBoss.layer` and `PgBoss.layerConfig` start pg-boss when the layer builds and stop it when the layer's scope closes. pg-boss runs its own statements on a separate pool of `maintenanceConnections` connections, which the library builds from the `PgClient` settings.
- `JobQueue.Service` declares a queue with a payload schema, an optional dead-letter queue, and pg-boss queue options. Its `layer` creates the queue. Its `send` sends a job, and inside `sql.withTransaction` the job commits or rolls back with the app's rows.
- `JobQueue.flow` sends a group of jobs that pg-boss inserts in one step. A job in the flow can depend on other jobs in it.
- A queue's `worker` takes `transactional: true` or `transactional: false`. With `true`, the handler's statements, its `send` and `flow` calls, and the completion of the batch commit in one transaction. With `false`, the handler's writes commit on their own, and pg-boss completes the jobs afterwards.
- `onDecodeError: "drop"` lets a worker skip a payload that does not decode. The worker logs the job at warning level, and the job completes with its batch. The default, `"fail"`, fails the batch before the handler runs.
- `useListenNotify: true` wakes the workers of `notify: true` queues when a job commits. The library opens one extra connection for `LISTEN` and replaces it when it is lost.
- `PgBoss.use` gives the pg-boss instance and the connection options of the fiber's transaction, for the pg-boss APIs that the queue classes do not cover.
- A SQL failure fails with the original `SqlError`. Any other pg-boss failure fails with `PgBoss.PgBossError`, whose `operation` field names the call.
