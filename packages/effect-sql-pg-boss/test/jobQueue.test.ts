import { assert, layer } from "@effect/vitest";
import { Array as Arr, Deferred, Effect, Layer, Order, pipe, Ref } from "effect";

import { JobQueue, PgBoss } from "../src/index.ts";
import { captureLogs, failure, JobData, layer as harness, maintenanceQuery, sendOutside, waitFor } from "./fixture.ts";

/** The stored options of a queue that convergence writes, read on pg-boss's own connections. */
const storedOptions = (name: string) =>
    PgBoss.use((boss) => boss.getQueue(name)).pipe(
        Effect.map((queue) => ({ retryLimit: queue?.retryLimit, retryDelay: queue?.retryDelay })),
    );

class ConvergeFirst extends JobQueue.Service<ConvergeFirst>()("converge", {
    payload: JobData,
    options: { retryLimit: 1 },
}) {}
class ConvergeChanged extends JobQueue.Service<ConvergeChanged>()("converge", {
    payload: JobData,
    options: { retryLimit: 5, retryDelay: 3 },
}) {}
class ConvergeBare extends JobQueue.Service<ConvergeBare>()("converge", { payload: JobData }) {}

class DecodeDrop extends JobQueue.Service<DecodeDrop>()("decode-drop", { payload: JobData }) {}
class DecodeDropAll extends JobQueue.Service<DecodeDropAll>()("decode-drop-all", { payload: JobData }) {}
class DecodeFail extends JobQueue.Service<DecodeFail>()("decode-fail", {
    payload: JobData,
    options: { retryLimit: 0 },
}) {}

/** Sends a payload the queue's schema rejects: `fail` must be a boolean. */
const sendMalformed = (queue: string, tag: string) => PgBoss.use((boss) => boss.send(queue, { tag, fail: "yes" }));

/** `tag:state` for every job of the queue, read on the maintenance pool. */
const queueStates = (queue: string) =>
    Effect.map(
        maintenanceQuery("select data->>'tag' as tag, state from pgboss.job where name = $1 order by 1", [queue]),
        Arr.map((row) => `${row.tag}:${row.state}`),
    );

/** A handler that records the sorted tags of each batch it gets. */
const recordBatches = Effect.map(Ref.make<ReadonlyArray<ReadonlyArray<string>>>([]), (batches) => ({
    batches: Ref.get(batches),
    handler: (jobs: Arr.NonEmptyReadonlyArray<JobQueue.Job<JobData>>) =>
        Ref.update(
            batches,
            Arr.append(
                pipe(
                    jobs,
                    Arr.map((job) => job.data.tag),
                    Arr.sort(Order.String),
                ),
            ),
        ),
}));

class DeadLetters extends JobQueue.Service<DeadLetters>()("dead-letters", { payload: JobData, deadLetter: true }) {}
class Charges extends JobQueue.Service<Charges>()("charges", {
    payload: JobData,
    options: { deadLetter: DeadLetters, retryLimit: 1 },
}) {}

class SingletonPolicy extends JobQueue.Service<SingletonPolicy>()("fixed-policy", {
    payload: JobData,
    options: { policy: "singleton" },
}) {}
class StandardPolicy extends JobQueue.Service<StandardPolicy>()("fixed-policy", { payload: JobData }) {}

layer(
    harness(
        { database: "job_queue" },
        Layer.mergeAll(
            DecodeDrop.layer,
            DecodeDropAll.layer,
            DecodeFail.layer,
            Charges.layer.pipe(Layer.provideMerge(DeadLetters.layer)),
        ),
    ),
    {
        excludeTestServices: true,
        timeout: "60 seconds",
    },
)("JobQueue", (it) => {
    it.effect("a queue layer converges an existing queue to its declared options", () =>
        Effect.gen(function* () {
            yield* Effect.scoped(Layer.build(ConvergeFirst.layer));
            assert.deepStrictEqual(yield* storedOptions("converge"), { retryLimit: 1, retryDelay: 0 });

            yield* Effect.scoped(Layer.build(ConvergeChanged.layer));
            assert.deepStrictEqual(yield* storedOptions("converge"), { retryLimit: 5, retryDelay: 3 });

            yield* Effect.scoped(Layer.build(ConvergeBare.layer));
            assert.deepStrictEqual(
                yield* storedOptions("converge"),
                { retryLimit: 5, retryDelay: 3 },
                "an option removed from the declaration keeps its stored value",
            );
        }),
    );

    it.effect("a queue layer fails to build when the stored policy differs from the declared one", () =>
        Effect.gen(function* () {
            yield* Effect.scoped(Layer.build(SingletonPolicy.layer));
            const error = yield* Effect.flip(Effect.scoped(Layer.build(StandardPolicy.layer)));
            assert.strictEqual(error._tag, "PgBossError");
            assert.strictEqual(error._tag === "PgBossError" ? error.operation : undefined, "updateQueue");
            assert.strictEqual(
                error._tag === "PgBossError" && error.cause instanceof Error ? error.cause.message : undefined,
                `queue "fixed-policy" has policy singleton, but its declaration says standard; pg-boss cannot change policy after creation`,
            );
        }),
    );

    it.effect("onDecodeError drop: the handler gets the jobs that decode, and the batch completes", () =>
        Effect.gen(function* () {
            const recorded = yield* recordBatches;
            const logs = captureLogs();
            yield* sendOutside(DecodeDrop.queueName, { tag: "drop-a" });
            const badId = yield* sendMalformed(DecodeDrop.queueName, "drop-bad");
            yield* sendOutside(DecodeDrop.queueName, { tag: "drop-b" });
            yield* Layer.build(
                DecodeDrop.worker(recorded.handler, {
                    transactional: false,
                    batchSize: 3,
                    pollingIntervalSeconds: 0.5,
                    onDecodeError: "drop",
                }),
            ).pipe(Effect.provide(logs.layer));
            yield* waitFor("drop-bad", DecodeDrop.queueName, "completed");

            assert.deepStrictEqual(yield* recorded.batches, [["drop-a", "drop-b"]]);
            assert.deepStrictEqual(yield* queueStates(DecodeDrop.queueName), [
                "drop-a:completed",
                "drop-b:completed",
                "drop-bad:completed",
            ]);
            const dropped = logs.logged("dropped a job whose payload does not decode");
            assert.deepStrictEqual(
                Arr.map(dropped, (message) => message[1]),
                [{ queue: "decode-drop", id: badId }],
            );
            assert.propertyVal(dropped[0]?.[2], "_tag", "SchemaError");
        }),
    );

    it.effect("onDecodeError drop: a batch where nothing decodes completes without running the handler", () =>
        Effect.gen(function* () {
            const recorded = yield* recordBatches;
            yield* sendMalformed(DecodeDropAll.queueName, "drop-all-1");
            yield* sendMalformed(DecodeDropAll.queueName, "drop-all-2");
            yield* Layer.build(
                DecodeDropAll.worker(recorded.handler, {
                    transactional: true,
                    batchSize: 2,
                    pollingIntervalSeconds: 0.5,
                    onDecodeError: "drop",
                }),
            );
            yield* waitFor("drop-all-2", DecodeDropAll.queueName, "completed");
            yield* waitFor("drop-all-1", DecodeDropAll.queueName, "completed");

            assert.deepStrictEqual(yield* recorded.batches, []);
            assert.deepStrictEqual(yield* queueStates(DecodeDropAll.queueName), [
                "drop-all-1:completed",
                "drop-all-2:completed",
            ]);
        }),
    );

    it.effect("onDecodeError fail (the default): one payload that does not decode fails the whole batch", () =>
        Effect.gen(function* () {
            const recorded = yield* recordBatches;
            yield* sendOutside(DecodeFail.queueName, { tag: "fail-a" });
            yield* sendMalformed(DecodeFail.queueName, "fail-bad");
            yield* sendOutside(DecodeFail.queueName, { tag: "fail-b" });
            yield* Layer.build(
                DecodeFail.worker(recorded.handler, {
                    transactional: true,
                    batchSize: 3,
                    pollingIntervalSeconds: 0.5,
                }),
            );
            yield* waitFor("fail-bad", DecodeFail.queueName, "failed");

            assert.deepStrictEqual(yield* recorded.batches, []);
            assert.deepStrictEqual(yield* queueStates(DecodeFail.queueName), [
                "fail-a:failed",
                "fail-b:failed",
                "fail-bad:failed",
            ]);
            const { output } = yield* failure("fail-a", DecodeFail.queueName);
            assert.strictEqual(String(output?.message).split("\n")[0], "SchemaError: Expected boolean | null");
        }),
    );

    it.effect("a job that fails past its retries reaches the dead-letter queue with its source", () =>
        Effect.gen(function* () {
            const chargeId = "3c9e1a7b-5d2f-4e8a-9b6c-1f0e2d3c4b5a";
            const received = yield* Deferred.make<JobQueue.DeadLetterJob<JobData>>();
            yield* Charges.send({ tag: "declined" }, { id: chargeId });
            yield* Layer.build(
                Charges.worker(() => Effect.fail("card declined"), {
                    transactional: true,
                    pollingIntervalSeconds: 0.5,
                }),
            );
            yield* Layer.build(
                DeadLetters.worker((jobs) => Deferred.succeed(received, Arr.headNonEmpty(jobs)), {
                    transactional: true,
                    pollingIntervalSeconds: 0.5,
                }),
            );
            const job = yield* Deferred.await(received).pipe(Effect.timeout("15 seconds"));
            const [failed] = yield* maintenanceQuery(
                "select state, output, created_on from pgboss.job where name = 'charges' and id = $1",
                [chargeId],
            );

            assert.deepStrictEqual(job.data, { tag: "declined" });
            assert.strictEqual(job.queue, "dead-letters");
            assert.deepStrictEqual(
                {
                    id: job.source.id,
                    queue: job.source.queue,
                    retryCount: job.source.retryCount,
                    rootId: job.source.rootId,
                },
                { id: chargeId, queue: "charges", retryCount: 1, rootId: chargeId },
            );
            assert.strictEqual(failed?.state, "failed");
            assert.deepStrictEqual(job.source.output, failed?.output, "the output the failed job stored");
            assert.propertyVal(job.source.output, "name", "Error");
            assert.propertyVal(job.source.output, "message", "Error: card declined");
            assert.strictEqual(job.source.createdOn.getTime(), failed?.created_on.getTime());
        }),
    );
});
