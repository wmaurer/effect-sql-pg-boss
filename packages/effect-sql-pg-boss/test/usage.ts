// The README call sites, typechecked by `pnpm typecheck`. Never run: vitest only picks up `*.test.ts`.
import { PgClient } from "@effect/sql-pg";
import { Config, Context, Effect, Layer, Option, Schema, String } from "effect";
import { SqlClient } from "effect/sql";

import { JobQueue, PgBoss } from "../src/index.ts";

import type { SqlError } from "effect/sql/SqlError";

class SendWelcomeEmail extends JobQueue.Service<SendWelcomeEmail>()("send-welcome-email", {
    payload: Schema.Struct({ userId: Schema.BigInt, email: Schema.String }),
    options: { retryLimit: 5, retryBackoff: true },
}) {}

class ChargeCard extends JobQueue.Service<ChargeCard>()("charge-card", {
    payload: Schema.Struct({ orderId: Schema.BigInt, amountCents: Schema.Int }),
    options: { retryLimit: 5, retryBackoff: true },
}) {}
class ShipOrder extends JobQueue.Service<ShipOrder>()("ship-order", {
    payload: Schema.Struct({ orderId: Schema.BigInt }),
}) {}
class SendReceipt extends JobQueue.Service<SendReceipt>()("send-receipt", {
    payload: Schema.Struct({ orderId: Schema.BigInt }),
}) {}
class AuditLog extends JobQueue.Service<AuditLog>()("audit-log", {
    payload: Schema.Struct({ orderId: Schema.BigInt, event: Schema.String }),
}) {}

// Declared but never given a layer in AppLive below.
class Unregistered extends JobQueue.Service<Unregistered>()("unregistered", {
    payload: Schema.Struct({ n: Schema.Finite }),
}) {}

class Payments extends Context.Service<
    Payments,
    {
        readonly charge: (
            orderId: bigint,
            amountCents: number,
            options: { readonly idempotencyKey: string },
        ) => Effect.Effect<{ readonly ref: string }>;
    }
>()("Payments") {}

// Enqueue inside the app's transaction.
export const signUp = Effect.fn("signUp")(function* (email: string) {
    const sql = yield* SqlClient.SqlClient;
    return yield* sql.withTransaction(
        Effect.gen(function* () {
            const [user] = yield* sql<{ readonly id: bigint }>`insert into users ${sql.insert({ email })} returning id`;
            yield* SendWelcomeEmail.send({ userId: user.id, email }, { startAfter: 60 });
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
            const [order] = yield* sql<{ readonly id: bigint }>`insert into orders ${sql.insert(cart)} returning id`;
            const jobs = yield* JobQueue.flow({
                charge: ChargeCard.step({ orderId: order.id, amountCents: cart.totalCents }),
                ship: ShipOrder.step({ orderId: order.id }, { dependsOn: ["charge"] }),
                receipt: SendReceipt.step({ orderId: order.id }, { dependsOn: ["charge"], job: { startAfter: 30 } }),
            });
            const chargeJob: JobQueue.JobId = jobs.charge;
            yield* sql`update orders set charge_job_id = ${chargeJob} where id = ${order.id}`;
            return order.id;
        }),
    );
});

// A ref that is not a sibling key does not compile.
export const badFlow = JobQueue.flow({
    charge: ChargeCard.step({ orderId: 1n, amountCents: 100 }),
    // @ts-expect-error "chrage" is not a key of this flow
    ship: ShipOrder.step({ orderId: 1n }, { dependsOn: ["chrage"] }),
});

// A step cannot depend on itself.
export const selfFlow = JobQueue.flow({
    // @ts-expect-error a step's refs exclude its own key
    ship: ShipOrder.step({ orderId: 1n }, { dependsOn: ["ship"] }),
});

// The payload is the schema's type.
// @ts-expect-error orderId is a bigint, not a string
export const badPayload = ChargeCard.send({ orderId: "1", amountCents: 100 });

// A transactional worker: app writes, a nested savepoint, a follow-up job and
// an afterCommit all ride pg-boss's transaction for the batch.
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

// The quickstart worker only writes the database, so it is transactional.
export const QuickstartWorker = SendWelcomeEmail.worker(
    (jobs) =>
        Effect.gen(function* () {
            const sql = yield* SqlClient.SqlClient;
            for (const job of jobs) {
                yield* sql`insert into email_outbox ${sql.insert({ userId: job.data.userId, kind: "welcome" })}`;
            }
        }),
    { transactional: true },
);

// A non-transactional worker: nothing is held while the handler runs.
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
// @ts-expect-error transactionTimeoutSeconds needs a transactional worker
export const badOptions = SendWelcomeEmail.worker(() => Effect.void, { transactional: false, transactionTimeoutSeconds: 5 });

export const timedOptions = SendWelcomeEmail.worker(() => Effect.void, {
    transactional: true,
    transactionTimeoutSeconds: 30,
});

// Every worker states whether it is transactional.
// @ts-expect-error transactional is required
export const unstatedOptions = SendWelcomeEmail.worker(() => Effect.void, { batchSize: 5 });

// @ts-expect-error the options argument is required
export const noOptions = SendWelcomeEmail.worker(() => Effect.void);

// A best-effort worker drops a job whose payload does not decode, so the rest of its batch still runs.
export const AuditLogWorker = AuditLog.worker((jobs) => Effect.logInfo("audit events", jobs.length), {
    transactional: false,
    batchSize: 50,
    onDecodeError: "drop",
});

// A dead-letter queue is declared first. The source queue names its class, and
// its layer then requires the dead-letter queue's layer.
const WorkPayload = Schema.Struct({ workId: Schema.String });
class WorkFailed extends JobQueue.Service<WorkFailed>()("work-failed", { payload: WorkPayload, deadLetter: true }) {}
class Work extends JobQueue.Service<Work>()("work", {
    payload: WorkPayload,
    options: { deadLetter: WorkFailed, retryLimit: 3 },
}) {}

// The handler of a dead-letter queue sees where each job came from.
export const WorkFailedWorker = WorkFailed.worker(
    (jobs) =>
        Effect.forEach(jobs, (job) => Effect.logInfo(job.data.workId, job.source.id, job.source.output), {
            discard: true,
        }),
    { transactional: true },
);

// Only a dead-letter queue's jobs have a source.
export const NoSourceWorker = ShipOrder.worker(
    // @ts-expect-error ship-order is not a dead-letter queue
    (jobs) => Effect.logInfo(jobs[0].source),
    { transactional: false },
);

// The dead-letter queue's payload is the source queue's payload.
class OtherFailed extends JobQueue.Service<OtherFailed>()("other-failed", {
    payload: Schema.Struct({ n: Schema.Finite }),
    deadLetter: true,
}) {}
export class MismatchedPayload extends JobQueue.Service<MismatchedPayload>()("mismatched-payload", {
    payload: WorkPayload,
    // @ts-expect-error OtherFailed's payload is not WorkPayload
    options: { deadLetter: OtherFailed },
}) {}

// Only a queue declared with `deadLetter: true` can be one.
export class NotADeadLetterQueue extends JobQueue.Service<NotADeadLetterQueue>()("not-a-dead-letter-queue", {
    payload: Schema.Struct({ orderId: Schema.BigInt }),
    // @ts-expect-error ShipOrder is not declared with deadLetter: true
    options: { deadLetter: ShipOrder },
}) {}

// A queue whose workers wake when a job commits, on a pg-boss that listens.
export class Notified extends JobQueue.Service<Notified>()("notified", {
    payload: Schema.Struct({ orderId: Schema.BigInt }),
    options: { notify: true },
}) {}
export const ListeningBossLive = PgBoss.layer({ useListenNotify: true });

// Escape hatch.
export const stats = PgBoss.use((boss) => boss.getQueueStats(ChargeCard.queueName));
export const cancelCharge = (id: JobQueue.JobId) =>
    PgBoss.use((boss, connection) => boss.cancel(ChargeCard.queueName, id, connection));

// Wiring.
const SqlLive = PgClient.layerConfig({ url: Config.Redacted("DATABASE_URL") });
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
    Layer.provideMerge(SqlLive),
    Layer.provide(PaymentsLive),
);

// An unregistered queue is a missing requirement.
// The Effect plugin reports the same missing service; @ts-expect-error alone is the assertion.
// @effect-diagnostics missingEffectContext:off
// @ts-expect-error Unregistered's layer is not part of AppLive
export const unregistered = Effect.runPromise(Unregistered.send({ n: 1 }).pipe(Effect.provide(AppLive)));
// @effect-diagnostics missingEffectContext:error

// A source queue whose dead-letter queue's layer is missing.
// @effect-diagnostics missingEffectContext:off
export const missingDeadLetter = Effect.runPromise(
    // @ts-expect-error WorkFailed's layer is not provided
    Work.send({ workId: "w" }).pipe(
        Effect.provide(Work.layer.pipe(Layer.provideMerge(BossLive), Layer.provideMerge(SqlLive))),
    ),
);
// @effect-diagnostics missingEffectContext:error

export const withDeadLetter = Effect.runPromise(
    Work.send({ workId: "w" }).pipe(
        Effect.provide(
            Work.layer.pipe(
                Layer.provideMerge(WorkFailed.layer),
                Layer.provideMerge(BossLive),
                Layer.provideMerge(SqlLive),
            ),
        ),
    ),
);

export const registered = Effect.runPromise(
    ChargeCard.send({ orderId: 1n, amountCents: 100 }).pipe(Effect.provide(AppLive)),
);

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

type SendResult = Effect.Success<ReturnType<typeof ChargeCard.send>>;
assertType<Equals<SendResult, Option.Option<JobQueue.JobId>>>(true);
type SendErrors = Effect.Error<ReturnType<typeof ChargeCard.send>>;
assertType<Equals<SendErrors, SqlError | PgBoss.PgBossError | Schema.SchemaError>>(true);
type SendR = Effect.Services<ReturnType<typeof SendWelcomeEmail.send>>;
assertType<Equals<SendR, SendWelcomeEmail>>(true);

type WorkerR = Layer.Services<typeof ChargeCardWorker>;
assertType<Equals<WorkerR, ChargeCard | SqlClient.SqlClient | Payments | AuditLog>>(true);

const AppWithoutPayments = Layer.mergeAll(ChargeCardWorker, WelcomeEmailWorker).pipe(
    Layer.provideMerge(QueuesLive),
    Layer.provideMerge(BossLive),
    Layer.provideMerge(SqlLive),
);
assertType<Equals<Layer.Services<typeof AppWithoutPayments>, Payments>>(true);
assertType<Equals<Layer.Services<typeof AppLive>, never>>(true);

// The quickstart's app layer needs nothing more.
const QuickstartLive = QuickstartWorker.pipe(
    Layer.provideMerge(SendWelcomeEmail.layer),
    Layer.provideMerge(PgBoss.layer()),
    Layer.provideMerge(
        PgClient.layerConfig({
            url: Config.Redacted("DATABASE_URL"),
            transformQueryNames: Config.succeed(String.camelToSnake),
            transformResultNames: Config.succeed(String.snakeToCamel),
        }),
    ),
);
assertType<Equals<Layer.Services<typeof QuickstartLive>, never>>(true);

type BossLayer = typeof BossLive;
assertType<Equals<BossLayer, Layer.Layer<PgBoss.PgBoss, PgBoss.PgBossError | SqlError, PgClient.PgClient>>>(true);

type WorkLayerR = Layer.Services<typeof Work.layer>;
assertType<Equals<WorkLayerR, PgBoss.PgBoss | WorkFailed>>(true);
type WorkFailedJob = Parameters<Parameters<typeof WorkFailed.worker>[0]>[0][number];
assertType<Equals<WorkFailedJob, JobQueue.DeadLetterJob<{ readonly workId: string }>>>(true);
assertType<
    Equals<
        JobQueue.DeadLetterSource,
        {
            readonly id: JobQueue.JobId;
            readonly queue: string;
            readonly output: unknown;
            readonly retryCount: number;
            readonly rootId: JobQueue.JobId;
            readonly createdOn: Date;
        }
    >
>(true);
