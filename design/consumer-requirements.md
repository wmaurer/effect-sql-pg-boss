# Requirements from a first consumer

A service plans to adopt `@wmaurer/effect-sql-pg-boss` on 2026-10-21. It keeps its own tables on the native `@effect/sql-pg` `PgClient` and owns four pg-boss queues that a separate worker process consumes or feeds:

- `work`: the service only sends to it, as the parent of a flow. The worker process consumes it. The queue has a retry policy and `deadLetter: "work-failed"`.
- `work-succeeded`: the dependent child of each flow. The service consumes it with a transactional worker.
- `work-failed`: the dead-letter queue of `work`. The service consumes it with a transactional worker.
- `work-progress`: best-effort progress messages from the worker process. The service consumes it with a non-transactional worker, `batchSize` about 50.

Three things it needs are missing from the API in `src/JobQueue.ts` today. Each section gives the need, the gap, and one possible shape. The shapes are suggestions only.

## 1. Dead-letter metadata on `Job`

**Status: met.** `work-failed` is declared with `deadLetter: true`, and its handler gets `job.source` with `id`, `queue`, `output`, `retryCount`, `rootId` and `createdOn`. `work` names it with `options: { deadLetter: WorkFailed }`, so its layer requires `WorkFailed`'s layer. Tested in `test/jobQueue.test.ts`, with the compile errors in `test/usage.ts`.

**Need.** The `work-failed` handler must know which job failed and why.

- It compares `sourceId`, the failed job's id, with the id the app stored when it enqueued the flow. A message whose `sourceId` is not the current one is a straggler and is dropped.
- It decodes `sourceOutput`, the failed job's output, which is the worker's typed failure, or pg-boss's own shape for a lost heartbeat, an expiry or an uncaught throw.

pg-boss copies the source job's `data` into the dead-letter job and sets `sourceName`, `sourceId`, `sourceOutput`, `sourceRootId`, `sourceCreatedOn` and `sourceRetryCount` (`pg-boss/dist/plans.js:3210-3240`, `types.d.ts:1184-1210`). They reach a handler only with `includeMetadata: true`.

**Gap.** `JobQueue.Job<A>` has `id`, `queue`, `data`, `retryCount` and `expireInSeconds` (`src/JobQueue.ts:60-67`). `includeMetadata` is not offered, because it "would change `Job`'s shape" (`src/JobQueue.ts:110`).

**Possible shape.** Let a queue be declared as a dead-letter queue of another, for example `JobQueue.Service<Self>()("work-failed", { payload, deadLetterOf: Work })`, or with `source: true`. Its `Job` then carries a `source: { id: JobId; queue: string; output: unknown; retryCount: number; rootId: JobId }` field. The payload schema of a dead-letter queue is the source queue's payload, because pg-boss copies `data`, so `deadLetterOf` can supply it. `output` stays `unknown`, and the handler decodes it with its own schema.

## 2. `updateQueue` on an existing queue

**Status: met.** The queue layer writes the declared options with `updateQueue` on every build, with no opt-in. An option removed from the declaration keeps its stored value. Tested in `test/jobQueue.test.ts`.

**Need.** The `work` queue's retry values (`retryLimit`, `retryDelay`, `retryDelayMax`) come from the service's environment, with defaults in shared code. When an operator changes one, the next start must apply it.

**Gap.** The planned `layer` creates the queue only when `getQueue` finds nothing (`src/JobQueue.ts:316`). pg-boss's `createQueue` leaves an existing queue alone. So a changed setting is silently ignored after the first start. This is the design record's open decision 4.

**Possible shape.** The queue `layer` calls `updateQueue(name, options)` after creating the queue, or when the queue exists and its stored options differ. Make it the default, or an opt-in `reconcile: true`. One ordering detail: `createQueue` with a `deadLetter` checks that the dead-letter queue exists, so the dead-letter queue's layer must build first. A `deadLetterOf` link from requirement 1 could express that order in the layer graph.

## 3. What happens when a payload fails to decode

**Status: met.** `work-progress` uses `onDecodeError: "drop"`. `work-succeeded` and `work-failed` keep the default, `"fail"`. `"deadletter"` is not offered. Tested in `test/jobQueue.test.ts`.

**Need.** It depends on the queue.

- `work-progress` is best effort. A malformed message should be logged and dropped (completed), not retried. One bad message must not fail its batch, so the other 49 still apply.
- `work-succeeded` and `work-failed` follow the queue's retry policy, so a decode failure there fails the job like any other handler failure.

**Gap.** The planned worker decodes the batch with `Effect.forEach(jobs, decode)` before calling the handler (`src/JobQueue.ts`, the `worker` sketch). One failed decode fails the whole batch, and pg-boss retries every job in it. This is the design record's open decision 3.

**Possible shape.** A worker option `onDecodeError: "fail" | "drop" | "deadletter"`, default `"fail"`. `"drop"` logs the job and its `SchemaError`, completes it, and passes the handler only the jobs that decoded. If none decode, the handler is not called. `"deadletter"` needs `perJobResults`, which cannot combine with `transactional`, so it may only be possible on non-transactional workers.

## Already covered

**Status: both hold.** The first rests on `PgBoss.use` joining the transaction, tested in `test/layer.test.ts`. The second is tested in `test/connection.test.ts`.

- Calling `getDependents` and `cancel` inside a transactional handler's transaction, to cancel the blocked flow child once its parent has dead-lettered. `PgBoss.use((boss, conn) => …)` does this already (`test/layer.test.ts`).
- `JobQueue.flow` inside `sql.withTransaction` with a caller-chosen id on the parent step, so the app can store the parent's id in its own row before it commits.
