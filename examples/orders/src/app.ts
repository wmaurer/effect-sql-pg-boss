import { PgClient } from "@effect/sql-pg";
import { PgBoss } from "@wmaurer/effect-sql-pg-boss";
import { Config, Effect, Layer, Redacted, String } from "effect";
import { SqlClient } from "effect/sql";

import { ChargeCard, ChargeFailed, ShipOrder } from "./queues.ts";

const SqlLive = PgClient.layerConfig({
    url: Config.Redacted("DATABASE_URL").pipe(
        Config.withDefault(Redacted.make("postgres://postgres:postgres@localhost:54320/orders")),
    ),
    transformQueryNames: Config.succeed(String.camelToSnake),
    transformResultNames: Config.succeed(String.snakeToCamel),
});

const OrdersTable = Layer.effectDiscard(
    Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`
            create table if not exists orders (
                id bigserial primary key,
                amount_cents int not null,
                status text not null default 'placed'
                    check (status in ('placed', 'charged', 'shipped', 'payment_failed')),
                created_at timestamptz not null default now()
            )
        `;
    }),
);

/** The orders table, pg-boss and the three queues, on one PgClient. */
export const AppLive = Layer.mergeAll(OrdersTable, ChargeCard.layer, ShipOrder.layer).pipe(
    Layer.provideMerge(ChargeFailed.layer),
    // pg-boss unblocks a flow's dependent jobs on this interval; its default of 5 seconds drags in a demo.
    Layer.provideMerge(PgBoss.layer({ flowIntervalSeconds: 1 })),
    Layer.provideMerge(SqlLive),
);
