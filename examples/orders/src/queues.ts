import { JobQueue } from "@wmaurer/effect-sql-pg-boss";
import { Schema } from "effect";

const ChargePayload = Schema.Struct({ orderId: Schema.BigInt, amountCents: Schema.Int, decline: Schema.Boolean });

export class ChargeFailed extends JobQueue.Service<ChargeFailed>()("charge-failed", {
    payload: ChargePayload,
    deadLetter: true,
}) {}

export class ChargeCard extends JobQueue.Service<ChargeCard>()("charge-card", {
    payload: ChargePayload,
    // Three attempts one second apart, so a declined card reaches charge-failed within seconds.
    options: { retryLimit: 2, retryDelay: 1, deadLetter: ChargeFailed },
}) {}

export class ShipOrder extends JobQueue.Service<ShipOrder>()("ship-order", {
    payload: Schema.Struct({ orderId: Schema.BigInt }),
}) {}
