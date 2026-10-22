import { Effect, Layer, Option, Schema } from "effect";
import { SqlClient } from "effect/sql";

import { AppLive } from "./app.ts";
import { ChargeCard, ChargeFailed, ShipOrder } from "./queues.ts";

export class CardDeclined extends Schema.TaggedError<CardDeclined, { readonly brand: unique symbol }>()(
    "CardDeclined",
    {
        orderId: Schema.BigInt,
    },
) {
    override get message() {
        return `card declined for order ${this.orderId}`;
    }
}

const ChargeCardWorker = ChargeCard.worker(
    Effect.fn("ChargeCardWorker")(function* (jobs) {
        const sql = yield* SqlClient.SqlClient;
        for (const job of jobs) {
            const { orderId, decline } = job.data;
            if (decline) {
                yield* Effect.logWarning(
                    `charge-card: card declined for order ${orderId} (attempt ${job.retryCount + 1})`,
                );
                return yield* new CardDeclined({ orderId });
            }
            yield* sql`update orders set status = 'charged' where id = ${orderId}`;
            yield* Effect.logInfo(`charge-card: order ${orderId} set to charged, not committed yet`);
            yield* sql.afterCommit(
                Effect.logInfo(`charge-card: committed order ${orderId} together with job ${job.id}`),
            );
        }
    }),
    { transactional: true },
);

const ShipOrderWorker = ShipOrder.worker(
    Effect.fn("ShipOrderWorker")(function* (jobs) {
        const sql = yield* SqlClient.SqlClient;
        for (const job of jobs) {
            yield* sql`update orders set status = 'shipped' where id = ${job.data.orderId}`;
            yield* Effect.logInfo(`ship-order: order ${job.data.orderId} shipped`);
        }
    }),
    { transactional: true },
);

const StoredError = Schema.Struct({ message: Schema.String });

const ChargeFailedWorker = ChargeFailed.worker(
    Effect.fn("ChargeFailedWorker")(function* (jobs) {
        const sql = yield* SqlClient.SqlClient;
        for (const job of jobs) {
            const { source } = job;
            const error = Option.match(Schema.decodeUnknownOption(StoredError)(source.output), {
                onNone: () => "unknown error",
                onSome: ({ message }) => message.split("\n")[0],
            });
            yield* sql`update orders set status = 'payment_failed' where id = ${job.data.orderId}`;
            yield* Effect.logError(
                `charge-failed: order ${job.data.orderId} set to payment_failed; ${source.queue} job ${source.id} failed after ${source.retryCount} retries with "${error}"`,
            );
        }
    }),
    { transactional: true },
);

/** The three workers on top of the app. Closing it stops the workers before pg-boss and the pool close. */
export const WorkersLive = Layer.mergeAll(ChargeCardWorker, ShipOrderWorker, ChargeFailedWorker).pipe(
    Layer.provideMerge(AppLive),
);
