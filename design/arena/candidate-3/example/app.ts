// The README call sites, typechecked against the sketch.
import { PgClient } from "@effect/sql-pg";
import { Context, Effect, Layer, Option, Redacted, Schema } from "effect";
import { SqlClient } from "effect/sql";

import { JobQueue, JobWorker, PgBoss } from "../src/index.ts";

export const ChargeCard = JobQueue.make("charge-card", {
    payload: Schema.Struct({ orderId: Schema.BigInt }),
});
export const ShipOrder = JobQueue.make("ship-order", {
    payload: Schema.Struct({ orderId: Schema.BigInt }),
    queue: { retryLimit: 5, retryBackoff: true },
});
export const NotifyCustomer = JobQueue.make("notify-customer", {
    payload: Schema.Struct({ orderId: Schema.BigInt, kind: Schema.Literals(["shipped", "charged"]) }),
});
export const WelcomeEmail = JobQueue.make("welcome-email", {
    payload: Schema.Struct({ userId: Schema.BigInt, email: Schema.String }),
});

class NotReady extends Schema.TaggedError<NotReady>()("NotReady", { orderId: Schema.BigInt }) {}
class Mailer extends Context.Service<
    Mailer,
    {
        readonly send: (to: string, subject: string) => Effect.Effect<void>;
    }
>()("app/Mailer") {}

interface Item {
    readonly sku: string;
    readonly qty: number;
}

const SqlLive = PgClient.layer({ url: Redacted.make("postgres://localhost/app") });
const BossLive = PgBoss.layer({ schema: "pgboss" }).pipe(Layer.provide(SqlLive));

export const placeOrder = Effect.fn("placeOrder")(function* (customerId: bigint, items: ReadonlyArray<Item>) {
    const sql = yield* SqlClient.SqlClient;
    return yield* sql.withTransaction(
        Effect.gen(function* () {
            const [order] = yield* sql<{
                id: bigint;
            }>`insert into orders ${sql.insert({ customer_id: customerId })} returning id`;
            yield* sql`insert into order_items ${sql.insert(items.map((i) => ({ order_id: order.id, ...i })))}`;
            const jobs = yield* JobQueue.flow([
                JobQueue.step("charge", ChargeCard, { orderId: order.id }),
                JobQueue.step("ship", ShipOrder, { orderId: order.id }, { dependsOn: ["charge"] }),
            ]);
            yield* sql`update orders set charge_job = ${jobs.charge} where id = ${order.id}`;
            return order.id;
        }),
    );
});

export const signUp = Effect.fn("signUp")(function* (email: string) {
    const sql = yield* SqlClient.SqlClient;
    return yield* sql.withTransaction(
        Effect.gen(function* () {
            const [user] = yield* sql<{ id: bigint }>`insert into users ${sql.insert({ email })} returning id`;
            const jobId = yield* JobQueue.send(WelcomeEmail, { userId: user.id, email }, { startAfter: 60 });
            return { user: user.id, jobId: Option.getOrUndefined(jobId) };
        }),
    );
});

export const ShipOrderWorker = JobWorker.layer(
    ShipOrder,
    Effect.fn("ShipOrder")(function* (job) {
        const sql = yield* SqlClient.SqlClient;
        const [order] = yield* sql<{
            status: string;
        }>`select status from orders where id = ${job.data.orderId} for update`;
        if (order.status !== "charged") return yield* new NotReady({ orderId: job.data.orderId });
        yield* sql`update orders set status = 'shipping' where id = ${job.data.orderId}`;
        yield* JobQueue.send(NotifyCustomer, { orderId: job.data.orderId, kind: "shipped" });
        yield* sql.afterCommit(Effect.logInfo("order shipped"));
    }),
    { localConcurrency: 4 },
);

export const WelcomeEmailWorker = JobWorker.layer(
    WelcomeEmail,
    (job) =>
        Effect.gen(function* () {
            const mailer = yield* Mailer;
            yield* mailer.send(job.data.email, "Welcome!");
        }),
    { transactional: false },
);

export const cancelShipment = (jobId: JobQueue.JobId) =>
    PgBoss.use((boss, { db }) => boss.cancel(ShipOrder.name, jobId, { db }));

export const WorkersLive: Layer.Layer<never, unknown, Mailer> = Layer.mergeAll(
    ShipOrderWorker,
    WelcomeEmailWorker,
).pipe(Layer.provide(BossLive), Layer.provide(SqlLive));

// Type-level checks on the call sites above.
type ErrorOf<T> = T extends (...args: any) => Effect.Effect<any, infer E, any> ? E : never;
type RequirementsOf<T> = T extends (...args: any) => Effect.Effect<any, any, infer R> ? R : never;
export const signUpErrors: Array<ErrorOf<typeof signUp>["_tag"]> = ["PgBossError", "SqlError", "SchemaError"];
export const signUpNeeds: Array<RequirementsOf<typeof signUp>> = [];
export const flowIds = Effect.gen(function* () {
    const ids = yield* JobQueue.flow([JobQueue.step("charge", ChargeCard, { orderId: 1n })]);
    const charge: JobQueue.JobId = ids.charge;
    // @ts-expect-error unknown ref
    ids.ship;
    // @ts-expect-error payload must match the queue's schema
    yield* JobQueue.send(ChargeCard, { orderId: 1 });
    return charge;
});
