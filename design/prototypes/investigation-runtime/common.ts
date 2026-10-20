import { PgClient } from "@effect/sql-pg";
import { Effect, Layer, Redacted } from "effect";

export const URL = "postgres://postgres:probe@127.0.0.1:55499/postgres";

export const pgLayer = (extra: Record<string, unknown> = {}) =>
    PgClient.layer({ url: Redacted.make(URL), ...extra } as any);

export const showErr = (e: unknown): string => {
    const anyE = e as any;
    const parts: Array<string> = [];
    let cur: any = anyE;
    for (let i = 0; i < 5 && cur; i++) {
        parts.push(`${cur?._tag ?? cur?.name ?? typeof cur}: ${cur?.message ?? String(cur)}`);
        cur = cur?.reason ?? cur?.cause;
    }
    return parts.join("  <-  ");
};

export const run = <A, E>(label: string, eff: Effect.Effect<A, E, any>, layer: Layer.Layer<any, any, any>) =>
    Effect.runPromise(
        eff.pipe(
            Effect.match({
                onSuccess: (a) => console.log(`[${label}] OK`, JSON.stringify(a)),
                onFailure: (e) => console.log(`[${label}] FAIL`, showErr(e)),
            }),
            Effect.provide(layer),
        ) as Effect.Effect<void>,
    ).catch((d) => console.log(`[${label}] DEFECT`, showErr(d)));
