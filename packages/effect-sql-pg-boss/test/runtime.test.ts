import { assert, it } from "@effect/vitest";
import { Effect } from "effect";

import { connectionBudget } from "../src/internal/runtime.ts";

it.effect("a transactional worker that would eat into the reserve fails to build", () =>
    Effect.gen(function* () {
        const reserve = connectionBudget(6 - 2);
        yield* reserve("a", 3);
        const error = yield* Effect.flip(reserve("b", 2));
        assert.strictEqual(error._tag, "PgBossError");
        assert.strictEqual(error.operation, "work");
        assert.strictEqual(
            error.cause instanceof Error ? error.cause.message : undefined,
            `transactional worker "b" needs 2 connection(s); 1 of the PgClient pool's 4 left after the reserve`,
        );
    }),
);
