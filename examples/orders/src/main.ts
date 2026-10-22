import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { JobQueue } from "@wmaurer/effect-sql-pg-boss";
import { Array as Arr, Console, Effect, Schema } from "effect";
import { Argument, Command, Flag } from "effect/cli";
import { SqlClient } from "effect/sql";

import { AppLive } from "./app.ts";
import { ChargeCard, ShipOrder } from "./queues.ts";
import { WorkersLive } from "./workers.ts";

class RollbackRequested extends Schema.TaggedError<RollbackRequested, { readonly brand: unique symbol }>()(
    "RollbackRequested",
    {
        orderId: Schema.BigInt,
        jobIds: Schema.Array(Schema.String),
    },
) {}

const worker = Command.make("worker", {}, () =>
    Effect.logInfo("worker: working charge-card, ship-order and charge-failed; press Ctrl-C to stop").pipe(
        Effect.andThen(Effect.never),
        Effect.provide(WorkersLive),
        Effect.onInterrupt(() => Effect.logInfo("worker: stopped, every layer closed")),
    ),
).pipe(Command.withDescription("Run the workers until Ctrl-C"));

const order = Command.make(
    "order",
    {
        amountCents: Argument.Int("amountCents").pipe(Argument.withDescription("Order total in cents")),
        decline: Flag.Boolean("decline").pipe(
            Flag.withDescription("Make every charge attempt fail, so the job ends in charge-failed"),
            Flag.withDefault(false),
        ),
        rollback: Flag.Boolean("rollback").pipe(
            Flag.withDescription("Fail the transaction after the jobs are enqueued"),
            Flag.withDefault(false),
        ),
    },
    Effect.fn("order")(function* ({ amountCents, decline, rollback }) {
        const sql = yield* SqlClient.SqlClient;
        yield* sql
            .withTransaction(
                Effect.gen(function* () {
                    const [placed] = yield* sql<{
                        readonly id: bigint;
                    }>`insert into orders ${sql.insert({ amountCents })} returning id`;
                    const orderId = placed.id;
                    const jobs = yield* JobQueue.flow({
                        charge: ChargeCard.step({ orderId, amountCents, decline }),
                        ship: ShipOrder.step({ orderId }, { dependsOn: ["charge"] }),
                    });
                    yield* Console.log(`order ${orderId} inserted`);
                    yield* Console.log(`  charge-card job ${jobs.charge}`);
                    yield* Console.log(`  ship-order job  ${jobs.ship} (runs after charge-card completes)`);
                    if (rollback) {
                        return yield* new RollbackRequested({ orderId, jobIds: [jobs.charge, jobs.ship] });
                    }
                    yield* Console.log("committed: the order and both jobs are now visible to the worker");
                }),
            )
            .pipe(
                Effect.catchTag("RollbackRequested", ({ orderId, jobIds }) =>
                    Effect.gen(function* () {
                        const [left] = yield* sql<{ readonly orders: number; readonly jobs: number }>`
                            select
                                (select count(*)::int from orders where id = ${orderId}) as orders,
                                (select count(*)::int from pgboss.job where id in ${sql.in(jobIds)}) as jobs
                        `;
                        yield* Console.log(
                            `rolled back: orders has ${left.orders} rows for order ${orderId}, pgboss.job has ${left.jobs} rows for its jobs`,
                        );
                    }),
                ),
            );
    }),
).pipe(Command.provide(AppLive), Command.withDescription("Place an order and enqueue its charge and ship jobs"));

const line = (columns: ReadonlyArray<string | number | bigint>) =>
    Arr.join(
        Arr.map(columns, (column) => `${column}`.padEnd(16)),
        "",
    ).trimEnd();

const status = Command.make("status", {}, () =>
    Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const orders = yield* sql<{
            readonly id: bigint;
            readonly amountCents: number;
            readonly status: string;
        }>`select id, amount_cents, status from orders order by id`;
        const jobs = yield* sql<{ readonly queue: string; readonly state: string; readonly count: number }>`
            select name as queue, case when blocked then 'blocked' else state::text end as state, count(*)::int as count
            from pgboss.job
            group by 1, 2
            order by 1, 2
        `;
        yield* Console.log(line(["order", "amount_cents", "status"]));
        yield* Effect.forEach(orders, (row) => Console.log(line([row.id, row.amountCents, row.status])), {
            discard: true,
        });
        yield* Console.log("");
        yield* Console.log(line(["queue", "state", "jobs"]));
        yield* Effect.forEach(jobs, (row) => Console.log(line([row.queue, row.state, row.count])), {
            discard: true,
        });
    }),
).pipe(Command.provide(AppLive), Command.withDescription("Print the orders table and pg-boss job counts"));

Command.make("orders").pipe(
    Command.withSubcommands([worker, order, status]),
    Command.run({ version: "0.0.0" }),
    Effect.provide(NodeServices.layer),
    NodeRuntime.runMain,
);
