// Compiles the README call sites against the sketch. Not part of the package.
import { Config, Effect, Layer, Schema } from "effect";
import { SqlClient } from "effect/sql";

import { Boss, Flow, NodePgClient, Queue } from "../src/index.ts";

const OrderId = Schema.String.pipe(Schema.brand("OrderId"));

const ChargeCard = Queue.make("charge-card", Schema.Struct({ orderId: OrderId }), {
    retry: { limit: 5, delay: "10 seconds", backoff: true },
});
const SendReceipt = Queue.make("send-receipt", Schema.Struct({ orderId: OrderId, email: Schema.String }));

const AppDb = NodePgClient.layerConfig(
    Config.all({
        url: Config.Redacted("DATABASE_URL"),
        pool: Config.succeed({ max: 20 }),
    }),
);

// 1. App to queue
export const placeOrder = Effect.fn("placeOrder")(function* (input: { email: string }) {
    const sql = yield* SqlClient.SqlClient;
    return yield* sql.withTransaction(
        Effect.gen(function* () {
            const [order] = yield* sql<{
                id: string;
            }>`insert into orders ${sql.insert({ email: input.email })} returning id`;
            const orderId = OrderId.make(order.id);
            const charge = Flow.node("charge", ChargeCard, { orderId });
            const receipt = Flow.node("receipt", SendReceipt, { orderId, email: input.email }, { dependsOn: [charge] });
            const ids = yield* Boss.flow(receipt);
            const chargeId: string = ids.charge;
            const receiptId: string = ids.receipt;
            // @ts-expect-error not a ref in this graph
            ids.nope;
            return { orderId, chargeId, receiptId };
        }),
    );
});

// 2. Queue to app
export const ChargeWorker = Boss.work(
    ChargeCard,
    { concurrency: 4 },
    Effect.fn("ChargeWorker")(function* ([job]) {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`update orders set status = 'paid' where id = ${job.data.orderId}`;
        yield* Boss.send(SendReceipt, { orderId: job.data.orderId, email: "x" });
    }),
);

// Non-atomic worker keeps the caller's SqlClient requirement
export const Mailer = Boss.work(SendReceipt, { atomic: false }, (jobs) => Effect.log(jobs.length));

// Escape hatch
export const cancelAll = Boss.use((boss, db) => boss.cancel("charge-card", [], { db }));

// Wiring
export const Main = Layer.mergeAll(Layer.effectDiscard(ChargeWorker), Layer.effectDiscard(Mailer)).pipe(
    Layer.provide(Boss.layer({ queues: [ChargeCard, SendReceipt] })),
    Layer.provideMerge(AppDb),
);

// @ts-expect-error wrong payload
export const bad = Boss.send(ChargeCard, { orderId: 1 });

// Requirements: the worker needs Boss, a root NodePgClient and a Scope, never the transaction services.
export const _worker: Effect.Effect<
    void,
    Boss.BossError,
    | import("effect/Scope").Scope
    | Boss.Boss
    | NodePgClient.NodePgClient
    | Boss.Registered<"charge-card" | "send-receipt">
> = ChargeWorker;
// The whole app closes over its requirements.
export const _main: Layer.Layer<never, any, never> = Main;
// A placeOrder caller must supply Boss and NodePgClient (SqlClient comes from NodePgClient).
export const _place: Effect.Effect<
    any,
    any,
    Boss.Boss | NodePgClient.NodePgClient | SqlClient.SqlClient | Boss.Registered<"charge-card" | "send-receipt">
> = placeOrder({ email: "a" });

// A queue missing from Boss.layer({ queues }) is a compile error where it is used.
const Incomplete = Layer.effectDiscard(ChargeWorker).pipe(
    Layer.provide(Boss.layer({ queues: [ChargeCard] })),
    Layer.provideMerge(AppDb),
);
// @ts-expect-error Registered<"send-receipt"> is not provided
export const _incomplete: Layer.Layer<never, any, never> = Incomplete;
