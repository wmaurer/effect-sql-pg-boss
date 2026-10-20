// The README call sites, typechecked against the sketch. Never run.
import { Config, Context, Effect, Layer, Schema } from "effect";
import { SqlClient } from "effect/sql";

import { JobQueue, PgBoss, PgNodeClient } from "../src/index.ts";

class SendWelcomeEmail extends JobQueue.Service<SendWelcomeEmail>()("send-welcome-email", {
    payload: Schema.Struct({ userId: Schema.String, email: Schema.String }),
    options: { retryLimit: 5, retryBackoff: true },
}) {}

class ChargeCard extends JobQueue.Service<ChargeCard>()("charge-card", {
    payload: Schema.Struct({ orderId: Schema.String, amountCents: Schema.Int }),
}) {}
class ShipOrder extends JobQueue.Service<ShipOrder>()("ship-order", {
    payload: Schema.Struct({ orderId: Schema.String }),
}) {}
class SendReceipt extends JobQueue.Service<SendReceipt>()("send-receipt", {
    payload: Schema.Struct({ orderId: Schema.String }),
}) {}
class AuditLog extends JobQueue.Service<AuditLog>()("audit-log", {
    payload: Schema.Struct({ orderId: Schema.String, event: Schema.String }),
}) {}

class Payments extends Context.Service<
    Payments,
    {
        readonly charge: (
            orderId: string,
            amountCents: number,
            options: { readonly idempotencyKey: string },
        ) => Effect.Effect<{ readonly ref: string }>;
    }
>()("Payments") {}

// Direction 1: enqueue inside the app's transaction.
export const signUp = Effect.fn("signUp")(function* (email: string) {
    const sql = yield* SqlClient.SqlClient;
    return yield* sql.withTransaction(
        Effect.gen(function* () {
            const [user] = yield* sql<{ readonly id: string }>`insert into users ${sql.insert({ email })} returning id`;
            yield* SendWelcomeEmail.send({ userId: user.id, email });
            return user.id;
        }),
    );
});

export const placeOrder = Effect.fn("placeOrder")(function* (cart: {
    readonly email: string;
    readonly totalCents: number;
}) {
    const sql = yield* SqlClient.SqlClient;
    return yield* sql.withTransaction(
        Effect.gen(function* () {
            const [order] = yield* sql<{ readonly id: string }>`insert into orders ${sql.insert(cart)} returning id`;
            const jobs = yield* JobQueue.flow({
                charge: ChargeCard.step({ orderId: order.id, amountCents: cart.totalCents }),
                ship: ShipOrder.step({ orderId: order.id }, { dependsOn: ["charge"] }),
                receipt: SendReceipt.step({ orderId: order.id }, { dependsOn: ["charge"] }),
            });
            const chargeJob: JobQueue.JobId = jobs.charge;
            yield* sql`update orders set charge_job_id = ${chargeJob} where id = ${order.id}`;
            return order.id;
        }),
    );
});

// A ref that is not a sibling key does not compile.
export const badFlow = JobQueue.flow({
    charge: ChargeCard.step({ orderId: "o1", amountCents: 100 }),
    // @ts-expect-error "chrage" is not a key of this flow
    ship: ShipOrder.step({ orderId: "o1" }, { dependsOn: ["chrage"] }),
});

// A step cannot depend on itself.
export const selfFlow = JobQueue.flow({
    // @ts-expect-error a step's refs exclude its own key
    ship: ShipOrder.step({ orderId: "o1" }, { dependsOn: ["ship"] }),
});

// Direction 2: write app tables inside pg-boss's transaction.
export const ChargeCardWorker = ChargeCard.worker(
    Effect.fn("ChargeCardWorker")(function* (jobs) {
        const sql = yield* SqlClient.SqlClient;
        const payments = yield* Payments;
        for (const job of jobs) {
            const receipt = yield* payments.charge(job.data.orderId, job.data.amountCents, { idempotencyKey: job.id });
            yield* sql`update orders set status = 'charged', payment_ref = ${receipt.ref} where id = ${job.data.orderId}`;
            yield* sql.withTransaction(
                sql`insert into ledger ${sql.insert({ orderId: job.data.orderId, ref: receipt.ref })}`,
            );
            yield* AuditLog.send({ orderId: job.data.orderId, event: "charged" });
            yield* sql.afterCommit(Effect.logInfo("order charged", job.data.orderId));
        }
    }),
    { transactional: true, batchSize: 10, localConcurrency: 2 },
);

export const WelcomeEmailWorker = SendWelcomeEmail.worker(
    (jobs) =>
        Effect.gen(function* () {
            const sql = yield* SqlClient.SqlClient;
            for (const job of jobs) {
                yield* sql`insert into email_outbox ${sql.insert({ userId: job.data.userId, kind: "welcome" })}`;
            }
        }),
    { transactional: false },
);

// The expected error must sit on the line right after the directive, so this call stays on one line.
// oxfmt-ignore
// @ts-expect-error transactionTimeoutSeconds needs transactional: true
export const badOptions = SendWelcomeEmail.worker(() => Effect.void, { transactional: false, transactionTimeoutSeconds: 5 });

// Escape hatch.
export const stats = PgBoss.use((boss) => boss.getQueueStats(ChargeCard.queueName));

// Wiring.
const DatabaseLive = PgNodeClient.layerConfig({ url: Config.Redacted("DATABASE_URL") });
const BossLive = PgBoss.layer({ schema: "pgboss" });
const QueuesLive = Layer.mergeAll(
    SendWelcomeEmail.layer,
    ChargeCard.layer,
    ShipOrder.layer,
    SendReceipt.layer,
    AuditLog.layer,
);
const PaymentsLive = Layer.succeed(Payments, { charge: () => Effect.succeed({ ref: "r" }) });

export const AppLive = Layer.mergeAll(ChargeCardWorker, WelcomeEmailWorker).pipe(
    Layer.provideMerge(QueuesLive),
    Layer.provideMerge(BossLive),
    Layer.provideMerge(DatabaseLive),
    Layer.provide(PaymentsLive),
);

// Everything the app layer needs is satisfied.
export const appRequirements: Layer.Layer<any, any, never> = AppLive;

// Type-level checks that the README's claims are what the compiler infers.
type Equals<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;
const assertType = <T extends true>(_: T) => _;

type PlaceOrderR = Effect.Services<ReturnType<typeof placeOrder>>;
assertType<Equals<PlaceOrderR, SqlClient.SqlClient | ChargeCard | ShipOrder | SendReceipt>>(true);

type FlowIds = Effect.Success<
    ReturnType<
        typeof JobQueue.flow<{
            readonly charge: JobQueue.Step<ChargeCard, readonly []>;
            readonly ship: JobQueue.Step<ShipOrder, readonly ["charge"]>;
        }>
    >
>;
assertType<Equals<FlowIds, { readonly charge: JobQueue.JobId; readonly ship: JobQueue.JobId }>>(true);

type WorkerR = Layer.Services<typeof ChargeCardWorker>;
assertType<Equals<WorkerR, ChargeCard | SqlClient.SqlClient | Payments | AuditLog>>(true);

// Without Payments the app layer still has a requirement.
const AppWithoutPayments = Layer.mergeAll(ChargeCardWorker, WelcomeEmailWorker).pipe(
    Layer.provideMerge(QueuesLive),
    Layer.provideMerge(BossLive),
    Layer.provideMerge(DatabaseLive),
);
assertType<Equals<Layer.Services<typeof AppWithoutPayments>, Payments>>(true);
assertType<Equals<Layer.Services<typeof AppLive>, never>>(true);

// Sending to a queue whose layer is missing leaves the queue as a requirement.
type SendR = Effect.Services<ReturnType<typeof SendWelcomeEmail.send>>;
assertType<Equals<SendR, SendWelcomeEmail>>(true);
