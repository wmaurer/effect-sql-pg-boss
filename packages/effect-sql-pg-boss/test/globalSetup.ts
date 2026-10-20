import { Clock, Config, Effect, Option, Schedule } from "effect";
import pg from "pg";

import { docker } from "./docker.ts";

import type { TestProject } from "vitest/node";

declare module "vitest" {
    export interface ProvidedContext {
        /** A superuser URL. Each test file creates its own database through it. */
        readonly databaseUrl: string;
        /** The container this run started, whose server log the suite reads. Unset with `DATABASE_URL`. */
        readonly postgresContainer: string | undefined;
    }
}

const ready = (url: string) =>
    Effect.acquireUseRelease(
        Effect.sync(() => new pg.Client({ connectionString: url })),
        (client) => Effect.tryPromise(() => client.connect().then(() => client.query("select 1"))),
        (client) => Effect.promise(() => client.end()),
    );

/**
 * Starts `postgres:18`. Statement logging is on, with each line prefixed by
 * the backend pid, so a test can read what one backend ran. Docker picks the
 * host port, so it is always free.
 */
const start = Effect.gen(function* () {
    const name = `effect-sql-pg-boss-test-${process.pid}-${yield* Clock.currentTimeMillis}`;
    yield* docker(
        "run",
        "--detach",
        "--rm",
        "--name",
        name,
        "--env",
        "POSTGRES_PASSWORD=test",
        "--publish",
        "127.0.0.1::5432",
        "postgres:18",
        "-c",
        "log_statement=all",
        "-c",
        "log_line_prefix=[%p] ",
    );
    return yield* Effect.gen(function* () {
        const port = (yield* docker("port", name, "5432/tcp")).split(":").at(-1);
        const url = `postgres://postgres:test@127.0.0.1:${port}/postgres`;
        yield* Effect.retry(ready(url), { schedule: Schedule.spaced("200 millis"), times: 150 });
        return { url, name };
    }).pipe(Effect.onError(() => Effect.ignoreCause(docker("rm", "--force", name))));
});

/** Uses `DATABASE_URL` when it is set, and a container of its own otherwise. Returns that container. */
const setup = (project: TestProject) =>
    Effect.gen(function* () {
        const external = yield* Config.option(Config.String("DATABASE_URL"));
        const server = Option.isSome(external) ? { url: external.value, name: undefined } : yield* start;
        project.provide("databaseUrl", server.url);
        project.provide("postgresContainer", server.name);
        return Option.fromNullishOr(server.name);
    });

const teardown = (container: Option.Option<string>) =>
    Option.match(container, {
        onNone: () => Effect.void,
        onSome: (name) => Effect.asVoid(docker("rm", "--force", name)),
    });

export default (project: TestProject) =>
    Effect.runPromise(setup(project)).then((container) => () => Effect.runPromise(teardown(container)));
