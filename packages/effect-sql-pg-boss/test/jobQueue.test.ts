import { assert, layer } from "@effect/vitest";
import { Effect, Layer } from "effect";

import { JobQueue, PgBoss } from "../src/index.ts";
import { JobData, layer as harness } from "./fixture.ts";

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

class SingletonPolicy extends JobQueue.Service<SingletonPolicy>()("fixed-policy", {
    payload: JobData,
    options: { policy: "singleton" },
}) {}
class StandardPolicy extends JobQueue.Service<StandardPolicy>()("fixed-policy", { payload: JobData }) {}

layer(harness({ database: "job_queue" }, Layer.empty), { excludeTestServices: true, timeout: "60 seconds" })(
    "JobQueue",
    (it) => {
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
    },
);
